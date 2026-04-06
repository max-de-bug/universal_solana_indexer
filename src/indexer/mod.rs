pub mod decoder;
pub mod fetcher;
pub mod grpc_stream;

use crate::config::{Config, IndexingMode};
use crate::db;
use crate::error::IndexerError;
use crate::idl::AnchorIdl;
use crate::indexer::decoder::{decode_fields, match_account, match_instruction};
use crate::indexer::fetcher::Fetcher;
use serde_json::json;
use futures_util::StreamExt;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{EncodedTransaction, UiInnerInstructions, UiInstruction};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Shared state passed into the indexer.
pub struct IndexerState {
    pub pool: PgPool,
    pub idl: AnchorIdl,
    pub config: Config,
    pub fetcher: Fetcher,
    pub cancel: CancellationToken,
}

/// Main entry: dispatch to the correct indexing strategy.
pub async fn run(state: Arc<IndexerState>) -> anyhow::Result<()> {
    // Spawn a parallel daemon completely dedicated to syncing Account States globally.
    let poller_state = state.clone();
    let poller_handle = tokio::spawn(async move {
        run_account_poller(poller_state).await;
    });

    let res = match &state.config.mode {
        IndexingMode::Batch {
            start_slot,
            end_slot,
        } => run_batch_slots(state.clone(), *start_slot, *end_slot).await,
        IndexingMode::BatchSignatures { signatures } => {
            let sigs = signatures.clone();
            run_batch_signatures(state.clone(), &sigs).await
        }
        IndexingMode::Realtime => run_realtime(state.clone()).await,
        IndexingMode::GrpcStream => grpc_stream::run_grpc_stream(state.clone()).await,
    };

    if !poller_handle.is_finished() {
        poller_handle.abort();
    }
    
    Ok(res?)
}

// ---------------------------------------------------------------------------
// Batch mode: process a specific slot range
// ---------------------------------------------------------------------------

async fn run_batch_slots(
    state: Arc<IndexerState>,
    start_slot: u64,
    end_slot: u64,
) -> Result<(), IndexerError> {
    info!(%start_slot, %end_slot, "Starting batch slot indexing");

    let mut before: Option<String> = None;
    let mut total = 0u64;
    let concurrency = state.config.batch_concurrency;

    loop {
        if state.cancel.is_cancelled() {
            info!("Batch interrupted by shutdown");
            break;
        }

        let sigs = state
            .fetcher
            .get_signatures(
                &state.config.program_id,
                before.as_deref(),
                None,
                state.config.batch_size,
            )
            .await?;

        if sigs.is_empty() {
            break;
        }

        let last_sig_info = sigs.last().cloned();
        let reached_start = last_sig_info.as_ref().map_or(false, |s| s.slot < start_slot);

        let futures = sigs.into_iter().filter_map(|sig_info| {
            let slot = sig_info.slot;
            if slot < start_slot || slot > end_slot {
                None
            } else {
                let state_ref = state.clone();
                let sig = sig_info.signature; // Now owned
                Some(async move {
                    let res = process_signature(&state_ref, &sig).await;
                    (sig, slot, res)
                })
            }
        });

        let mut stream = futures_util::stream::iter(futures).buffer_unordered(concurrency);

        while let Some((sig, slot, res)) = stream.next().await {
            if let Err(e) = res {
                warn!(%sig, error = %e, "Failed to process tx");
            }
            total += 1;

            if total % 100 == 0 {
                let _ = db::update_sync_state(
                    &state.pool,
                    &state.config.program_id.to_string(),
                    slot,
                    Some(&sig),
                )
                .await; // log error? ignoring is fine for intermittent sync state
            }
        }

        if reached_start {
            info!(%total, "Reached start_slot boundary, batch complete");
            return Ok(());
        }

        if let Some(oldest) = last_sig_info {
            before = Some(oldest.signature);
        }
    }

    info!(%total, "Batch slot indexing finished");
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch mode: process a list of specific signatures
// ---------------------------------------------------------------------------

async fn run_batch_signatures(
    state: Arc<IndexerState>,
    signatures: &[String],
) -> Result<(), IndexerError> {
    info!(count = signatures.len(), "Starting batch signature indexing");

    for (i, sig) in signatures.iter().enumerate() {
        if state.cancel.is_cancelled() {
            info!("Batch signatures interrupted by shutdown");
            break;
        }
        if let Err(e) = process_signature(&state, sig).await {
            warn!(%sig, error = %e, "Failed to process tx");
        }
        if (i + 1) % 50 == 0 {
            info!(progress = i + 1, total = signatures.len(), "Batch progress");
        }
    }

    info!("Batch signature indexing finished");
    Ok(())
}

// ---------------------------------------------------------------------------
// Real-time mode with cold-start backfill
// ---------------------------------------------------------------------------

async fn run_realtime(state: Arc<IndexerState>) -> Result<(), IndexerError> {
    let program_str = state.config.program_id.to_string();
    let mut reconnect_delay = std::time::Duration::from_secs(1);

    loop {
        if state.cancel.is_cancelled() {
            break;
        }

        // Backfill from last processed point on every (re)connect.
        let last = db::get_last_processed(&state.pool, &program_str).await?;
        let last_sig = last.as_ref().and_then(|(_, s)| s.clone());

        if last_sig.is_some() {
            info!("Backfilling from last processed signature");
        } else {
            info!("Fresh start: no previous state found");
        }

        backfill(&state, last_sig.as_deref()).await?;

        // Attempt WSS connection.
        info!(
            wss = %state.config.wss_url,
            "Connecting to real-time WSS stream"
        );

        let pubsub = match PubsubClient::new(&state.config.wss_url).await {
            Ok(p) => {
                reconnect_delay = std::time::Duration::from_secs(1);
                p
            }
            Err(e) => {
                warn!(error = %e, delay = ?reconnect_delay, "WSS connect failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(reconnect_delay) => {}
                    _ = state.cancel.cancelled() => break,
                }
                reconnect_delay = (reconnect_delay * 2).min(std::time::Duration::from_secs(60));
                continue;
            }
        };

        let (mut stream, _unsubscribe) = match pubsub.logs_subscribe(
            RpcTransactionLogsFilter::Mentions(vec![program_str.clone()]),
            RpcTransactionLogsConfig {
                commitment: Some(CommitmentConfig::confirmed()),
            },
        ).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, delay = ?reconnect_delay, "WSS subscribe failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(reconnect_delay) => {}
                    _ = state.cancel.cancelled() => break,
                }
                reconnect_delay = (reconnect_delay * 2).min(std::time::Duration::from_secs(60));
                continue;
            }
        };

        info!("WSS stream active");

        loop {
            tokio::select! {
                _ = state.cancel.cancelled() => {
                    info!("Real-time WSS stream stopped by shutdown");
                    return Ok(());
                }
                resp = stream.next() => {
                    match resp {
                        Some(log) => {
                            let sig = log.value.signature;
                            match process_signature(&state, &sig).await {
                                Ok(slot) => {
                                    let _ = db::update_sync_state(&state.pool, &program_str, slot, Some(&sig)).await;
                                }
                                Err(e) => warn!(%sig, error = %e, "Failed to process WSS tx"),
                            }
                        }
                        None => {
                            warn!("WSS stream closed, reconnecting...");
                            break; // breaks inner loop, outer loop reconnects
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Back-fill all signatures newer than `until` (or all available if None).
pub(crate) async fn backfill(state: &IndexerState, until: Option<&str>) -> Result<(), IndexerError> {
    let mut before: Option<String> = None;
    let mut total = 0u64;

    loop {
        if state.cancel.is_cancelled() {
            break;
        }

        let sigs = state
            .fetcher
            .get_signatures(
                &state.config.program_id,
                before.as_deref(),
                until,
                state.config.batch_size,
            )
            .await?;

        if sigs.is_empty() {
            break;
        }

        let concurrency = state.config.batch_concurrency;
        let newest_sig_info = sigs.first().cloned();
        let oldest_sig_info = sigs.last().cloned();

        let mut stream = futures_util::stream::iter(sigs.into_iter().rev())
            .map(|sig_info| {
                let state_ref = state;
                async move {
                    let sig = sig_info.signature;
                    let res = process_signature(&state_ref, &sig).await;
                    (sig, res)
                }
            })
            .buffer_unordered(concurrency);

        while let Some((sig, res)) = stream.next().await {
            if let Err(e) = res {
                warn!(%sig, error = %e, "Backfill: failed to process tx");
            }
            total += 1;
        }

        let prog_str = state.config.program_id.to_string();
        if let Some(oldest) = oldest_sig_info {
            before = Some(oldest.signature);
        }
        if let Some(newest) = newest_sig_info {
            db::update_sync_state(
                &state.pool,
                &prog_str,
                newest.slot,
                Some(&newest.signature),
            )
            .await?;
        }

        debug!(%total, "Backfill progress");
    }

    info!(%total, "Backfill complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction processing
// ---------------------------------------------------------------------------

pub(crate) async fn process_signature(
    state: &IndexerState,
    sig: &str,
) -> Result<u64, IndexerError> {
    // Dedup: skip if already indexed to save an RPC fetch.
    if db::transaction_exists(&state.pool, sig).await? {
        debug!(%sig, "Skipping already-indexed transaction");
        // We need to return a valid slot here, but we don't know it without a DB query.
        // Let's just return 0 here since it's only used for update_sync_state fallback, which is okay for skips.
        return Ok(0);
    }

    let tx = state.fetcher.get_transaction(sig).await?;

    let slot = tx.slot;
    let block_time = tx.block_time;
    let meta = tx.transaction.meta.as_ref();
    let success = meta.map_or(true, |m| m.err.is_none());
    let fee = meta.map(|m| m.fee);
    let err_msg = meta
        .and_then(|m| m.err.as_ref())
        .map(|e| format!("{e:?}"));

    // Wrap all DB writes for this transaction in a single atomic commit.
    let mut db_tx = state.pool.begin().await?;

    db::insert_transaction(
        &mut *db_tx,
        sig,
        slot,
        block_time,
        success,
        fee,
        err_msg.as_deref(),
    )
    .await?;

    let meta = tx.transaction.meta.as_ref();

    let ui_tx = &tx.transaction.transaction;
    let inner_ixs = meta.and_then(|m| {
        if let solana_transaction_status::option_serializer::OptionSerializer::Some(v) = &m.inner_instructions {
            Some(v)
        } else {
            None
        }
    });

    decode_and_store_instructions(state, &mut db_tx, sig, slot, ui_tx, inner_ixs).await?;

    db_tx.commit().await?;

    debug!(%sig, %slot, "Transaction indexed");
    Ok(slot)
}

async fn decode_and_store_instructions(
    state: &IndexerState,
    db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tx_sig: &str,
    slot: u64,
    encoded_tx: &EncodedTransaction,
    inner_instructions: Option<&Vec<UiInnerInstructions>>,
) -> Result<(), IndexerError> {
    let type_map = state.idl.type_map();
    let program_id_str = state.config.program_id.to_string();

    // Decode from base64.
    let tx_bytes = match encoded_tx {
        EncodedTransaction::Binary(blob, _encoding) => {
            base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                blob,
            )
            .unwrap_or_default()
        }
        _ => return Ok(()), // JSON-parsed txs handled differently
    };

    // Parse the raw transaction to get instructions.
    let tx: solana_sdk::transaction::VersionedTransaction =
        match bincode::deserialize(&tx_bytes) {
            Ok(t) => t,
            Err(e) => {
                warn!(%tx_sig, error = %e, "Failed to deserialize transaction bytes");
                return Ok(());
            }
        };

    let account_keys = tx.message.static_account_keys();

    // ---- Top-level instructions ---------------------------------------------
    for (ix_idx, ix) in tx.message.instructions().iter().enumerate() {
        let program_key = account_keys
            .get(ix.program_id_index as usize)
            .map(|k| k.to_string())
            .unwrap_or_default();

        if program_key == program_id_str {
            process_single_instruction(
                state, db_tx, tx_sig, slot, ix_idx as i32, &ix.data, &ix.accounts,
                account_keys, &type_map, &program_id_str,
            )
            .await?;
        }

        // ---- Inner / CPI instructions for this top-level ix -----------------
        if let Some(inners) = inner_instructions {
            if let Some(inner_set) = inners.iter().find(|ii| ii.index as usize == ix_idx) {
                for (cpi_offset, ui_ix) in inner_set.instructions.iter().enumerate() {
                    if let UiInstruction::Compiled(c) = ui_ix {
                        let inner_prog = account_keys
                            .get(c.program_id_index as usize)
                            .map(|k| k.to_string())
                            .unwrap_or_default();

                        if inner_prog == program_id_str {
                            let data = bs58::decode(&c.data).into_vec().unwrap_or_default();
                            // Label inner instructions with a distinct index.
                            let inner_ix_idx = (ix_idx as i32) * 1000 + (cpi_offset as i32);
                            process_single_instruction(
                                state, db_tx, tx_sig, slot, inner_ix_idx, &data, &c.accounts,
                                account_keys, &type_map, &program_id_str,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Decode and store a single instruction (used for both top-level and CPI).
async fn process_single_instruction(
    state: &IndexerState,
    db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tx_sig: &str,
    slot: u64,
    ix_index: i32,
    data: &[u8],
    account_indices: &[u8],
    account_keys: &[Pubkey],
    type_map: &HashMap<String, &crate::idl::IdlTypeDef>,
    program_id_str: &str,
) -> Result<(), IndexerError> {
    let accounts_json: Vec<String> = account_indices
        .iter()
        .filter_map(|&idx| account_keys.get(idx as usize).map(|k| k.to_string()))
        .collect();

    if let Some((ix_def, remaining)) = match_instruction(data, &state.idl) {
        // Decode the instruction arguments.
        let args = decode_fields(remaining, &ix_def.args, type_map)
            .unwrap_or_else(|e| {
                warn!(ix = %ix_def.name, error = %e, "Partial arg decode");
                serde_json::Value::Null
            });

        db::insert_instruction(
            &mut **db_tx,
            tx_sig,
            ix_index,
            &ix_def.name,
            program_id_str,
            &args,
            &json!(accounts_json),
            Some(data),
        )
        .await?;

        if let Err(e) = db::insert_dynamic_instruction(
            &mut **db_tx,
            &state.idl.metadata.name,
            tx_sig,
            ix_index,
            &ix_def.name,
            &args,
        )
        .await {
            warn!(ix = %ix_def.name, error = %e, "Dynamic native table insert failed");
        }

        // Try to decode writable account states (uses pool, not the DB transaction,
        // because account fetching involves RPC calls and we don't want to hold the
        // DB transaction open during network I/O).
        for (acc_idx, acc_meta) in ix_def.accounts.iter().enumerate() {
            if !acc_meta.writable {
                continue;
            }
            if let Some(&key_idx) = account_indices.get(acc_idx) {
                if let Some(pubkey) = account_keys.get(key_idx as usize) {
                    try_decode_account(state, pubkey, slot, tx_sig, type_map).await;
                }
            }
        }
    } else {
        // Unknown instruction — store raw.
        db::insert_instruction(
            &mut **db_tx,
            tx_sig,
            ix_index,
            "unknown",
            program_id_str,
            &serde_json::Value::Null,
            &json!(accounts_json),
            Some(data),
        )
        .await?;
    }

    Ok(())
}


/// Attempt to fetch and decode an on-chain account's state.
async fn try_decode_account(
    state: &IndexerState,
    pubkey: &Pubkey,
    slot: u64,
    tx_sig: &str,
    type_map: &HashMap<String, &crate::idl::IdlTypeDef>,
) {
    let data = match state.fetcher.get_account_data(pubkey).await {
        Ok(Some(d)) => d,
        Ok(None) => return,
        Err(e) => {
            debug!(pubkey = %pubkey, error = %e, "Could not fetch account data");
            return;
        }
    };

    if let Some((acc_def, remaining)) = match_account(&data, &state.idl) {
        if let Some(fields) = state.idl.account_fields(&acc_def.name) {
            match decode_fields(remaining, fields, type_map) {
                Ok(decoded) => {
                    let _ = db::insert_account_state(
                        &state.pool,
                        &state.idl.metadata.name,
                        &acc_def.name,
                        &pubkey.to_string(),
                        Some(slot),
                        Some(tx_sig),
                        &decoded,
                    )
                    .await
                    .map_err(|e| warn!(%pubkey, error = %e, "Failed to insert account state"));
                }
                Err(e) => {
                    debug!(account = %acc_def.name, error = %e, "Account decode failed");
                }
            }
        }
    }
}

/// A dedicated background daemon polling `getProgramAccounts` to ingest structural state globally.
async fn run_account_poller(state: Arc<IndexerState>) {
    // We poll GPA significantly less frequently than realtime slots to avoid burning RPC limits.
    // 30 seconds default.
    let poll_dur = std::time::Duration::from_secs(30);

    loop {
        if state.cancel.is_cancelled() {
            break;
        }

        info!("Fetching global program accounts chunk...");
        match state.fetcher.get_program_accounts(&state.config.program_id).await {
            Ok(accounts) => {
                let type_map = state.idl.type_map();
                let mut success_count = 0;
                
                for (pubkey, account) in accounts {
                    if state.cancel.is_cancelled() {
                        break;
                    }

                    if let Some((acc_def, remaining)) = match_account(&account.data, &state.idl) {
                        if let Some(fields) = state.idl.account_fields(&acc_def.name) {
                            if let Ok(decoded) = decode_fields(remaining, fields, &type_map) {
                                let _ = db::insert_account_state(
                                    &state.pool,
                                    &state.idl.metadata.name,
                                    &acc_def.name,
                                    &pubkey.to_string(),
                                    Some(0), // slot is purely historical bounds for GPA
                                    None,
                                    &decoded,
                                )
                                .await;
                                success_count += 1;
                            }
                        }
                    }
                }
                info!(count = success_count, "Successfully synced global account states");
            }
            Err(e) => {
                warn!(error = %e, "Failed to poll program accounts");
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(poll_dur) => {}
            _ = state.cancel.cancelled() => break,
        }
    }
}
