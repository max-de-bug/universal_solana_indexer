pub mod decoder;
pub mod fetcher;

use crate::config::{Config, IndexingMode};
use crate::db;
use crate::idl::AnchorIdl;
use crate::indexer::decoder::{match_account, match_instruction, decode_fields};
use crate::indexer::fetcher::Fetcher;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiMessage,
};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
    match &state.config.mode {
        IndexingMode::Batch {
            start_slot,
            end_slot,
        } => run_batch_slots(state.clone(), *start_slot, *end_slot).await,
        IndexingMode::BatchSignatures { signatures } => {
            let sigs = signatures.clone();
            run_batch_signatures(state.clone(), &sigs).await
        }
        IndexingMode::Realtime => run_realtime(state.clone()).await,
    }
}

// ---------------------------------------------------------------------------
// Batch mode: process a specific slot range
// ---------------------------------------------------------------------------

async fn run_batch_slots(
    state: Arc<IndexerState>,
    start_slot: u64,
    end_slot: u64,
) -> anyhow::Result<()> {
    info!(%start_slot, %end_slot, "Starting batch slot indexing");

    let mut before: Option<String> = None;
    let mut total = 0u64;

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

        for sig_info in &sigs {
            let slot = sig_info.slot;
            if slot < start_slot {
                info!(%total, "Reached start_slot boundary, batch complete");
                return Ok(());
            }
            if slot > end_slot {
                continue;
            }

            if let Err(e) = process_signature(&state, &sig_info.signature, slot).await {
                warn!(sig = %sig_info.signature, error = %e, "Failed to process tx");
            }
            total += 1;

            if total % 100 == 0 {
                db::update_sync_state(
                    &state.pool,
                    &state.config.program_id.to_string(),
                    slot,
                    Some(&sig_info.signature),
                )
                .await?;
            }
        }

        before = sigs.last().map(|s| s.signature.clone());
        debug!(%total, batch_len = sigs.len(), "Batch page processed");
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
) -> anyhow::Result<()> {
    info!(count = signatures.len(), "Starting batch signature indexing");

    for (i, sig) in signatures.iter().enumerate() {
        if state.cancel.is_cancelled() {
            info!("Batch signatures interrupted by shutdown");
            break;
        }
        if let Err(e) = process_signature(&state, sig, 0).await {
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

async fn run_realtime(state: Arc<IndexerState>) -> anyhow::Result<()> {
    let program_str = state.config.program_id.to_string();

    // Cold-start: backfill from last processed point.
    let last = db::get_last_processed(&state.pool, &program_str).await?;
    let last_sig = last.as_ref().and_then(|(_, s)| s.clone());

    if last_sig.is_some() {
        info!("Cold start: backfilling from last processed signature");
    } else {
        info!("Fresh start: no previous state found");
    }

    backfill(&state, last_sig.as_deref()).await?;

    // Enter polling loop.
    info!(
        interval_ms = state.config.poll_interval_ms,
        "Entering real-time polling loop"
    );
    let poll_dur = std::time::Duration::from_millis(state.config.poll_interval_ms);

    loop {
        if state.cancel.is_cancelled() {
            info!("Real-time loop stopped by shutdown");
            break;
        }

        // Determine the newest processed signature.
        let latest = db::get_last_processed(&state.pool, &program_str).await?;
        let until_sig = latest.and_then(|(_, s)| s);

        let sigs = state
            .fetcher
            .get_signatures(
                &state.config.program_id,
                None,
                until_sig.as_deref(),
                state.config.batch_size,
            )
            .await?;

        if sigs.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(poll_dur) => {}
                _ = state.cancel.cancelled() => break,
            }
            continue;
        }

        // Process in chronological order (reverse the reverse-chronological response).
        for sig_info in sigs.iter().rev() {
            if state.cancel.is_cancelled() {
                break;
            }
            if let Err(e) = process_signature(&state, &sig_info.signature, sig_info.slot).await {
                warn!(sig = %sig_info.signature, error = %e, "Failed to process tx");
            }
        }

        // Persist progress: the newest signature we just processed.
        if let Some(newest) = sigs.first() {
            db::update_sync_state(
                &state.pool,
                &program_str,
                newest.slot,
                Some(&newest.signature),
            )
            .await?;
        }

        info!(new_txs = sigs.len(), "Polled new transactions");
    }

    Ok(())
}

/// Back-fill all signatures newer than `until` (or all available if None).
async fn backfill(state: &IndexerState, until: Option<&str>) -> anyhow::Result<()> {
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

        // Process in chronological order.
        for sig_info in sigs.iter().rev() {
            if let Err(e) = process_signature(state, &sig_info.signature, sig_info.slot).await {
                warn!(sig = %sig_info.signature, error = %e, "Backfill: failed to process tx");
            }
            total += 1;
        }

        let prog_str = state.config.program_id.to_string();
        if let Some(oldest) = sigs.last() {
            before = Some(oldest.signature.clone());
        }
        if let Some(newest) = sigs.first() {
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

async fn process_signature(
    state: &IndexerState,
    sig: &str,
    hint_slot: u64,
) -> anyhow::Result<()> {
    let tx = state.fetcher.get_transaction(sig).await?;

    let slot = tx.slot;
    let block_time = tx.block_time;
    let meta = tx.transaction.meta.as_ref();
    let success = meta.map_or(true, |m| m.err.is_none());
    let fee = meta.and_then(|m| {
        // Fee is part of the meta
        Some(m.fee)
    });
    let err_msg = meta
        .and_then(|m| m.err.as_ref())
        .map(|e| format!("{e:?}"));

    // Insert the transaction record.
    db::insert_transaction(
        &state.pool,
        sig,
        slot,
        block_time,
        success,
        fee,
        err_msg.as_deref(),
    )
    .await?;

    // Decode instructions.
    if let Some(ui_tx) = &tx.transaction.transaction {
        decode_and_store_instructions(state, sig, slot, ui_tx).await?;
    }

    debug!(%sig, %slot, "Transaction indexed");
    Ok(())
}

async fn decode_and_store_instructions(
    state: &IndexerState,
    tx_sig: &str,
    slot: u64,
    encoded_tx: &EncodedTransaction,
) -> anyhow::Result<()> {
    let type_map = state.idl.type_map();
    let program_id_str = state.config.program_id.to_string();

    // Decode from base64.
    let tx_bytes = match encoded_tx {
        EncodedTransaction::Binary(blob, encoding) => {
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

    for (ix_idx, ix) in tx.message.instructions().iter().enumerate() {
        let program_key = account_keys
            .get(ix.program_id_index as usize)
            .map(|k| k.to_string())
            .unwrap_or_default();

        if program_key != program_id_str {
            continue;
        }

        let data = &ix.data;
        let accounts_json: Vec<String> = ix
            .accounts
            .iter()
            .filter_map(|&idx| account_keys.get(idx as usize).map(|k| k.to_string()))
            .collect();

        if let Some((ix_def, remaining)) = match_instruction(data, &state.idl) {
            // Decode the instruction arguments.
            let args = decode_fields(remaining, &ix_def.args, &type_map)
                .unwrap_or_else(|e| {
                    warn!(ix = %ix_def.name, error = %e, "Partial arg decode");
                    serde_json::Value::Null
                });

            db::insert_instruction(
                &state.pool,
                tx_sig,
                ix_idx as i32,
                &ix_def.name,
                &program_id_str,
                &args,
                &json!(accounts_json),
                Some(data),
            )
            .await?;

            // Try to decode writable account states.
            for (acc_idx, acc_meta) in ix_def.accounts.iter().enumerate() {
                if !acc_meta.writable {
                    continue;
                }
                if let Some(&key_idx) = ix.accounts.get(acc_idx) {
                    if let Some(pubkey) = account_keys.get(key_idx as usize) {
                        try_decode_account(state, pubkey, slot, tx_sig, &type_map).await;
                    }
                }
            }
        } else {
            // Unknown instruction — store raw.
            db::insert_instruction(
                &state.pool,
                tx_sig,
                ix_idx as i32,
                "unknown",
                &program_id_str,
                &serde_json::Value::Null,
                &json!(accounts_json),
                Some(data),
            )
            .await?;
        }
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
                        slot,
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
