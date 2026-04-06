//! Yellowstone gRPC streaming mode.
//!
//! Subscribes to the Geyser gRPC feed for confirmed transactions that
//! mention `PROGRAM_ID`, then feeds each signature into the existing
//! [`super::process_signature`] pipeline so that full Borsh decoding,
//! IDL-aware DB insertion, and account state tracking are all reused.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status, metadata::MetadataValue};
use tracing::{debug, error, info, warn};
use yellowstone_grpc_proto::geyser::geyser_client::GeyserClient;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions,
};

use crate::db;
use crate::error::IndexerError;
use crate::indexer::IndexerState;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the Yellowstone gRPC streaming pipeline.
///
/// 1.  Back-fill from the last stored checkpoint (exactly like `run_realtime`).
/// 2.  Open a Geyser gRPC subscription filtered to `PROGRAM_ID`.
/// 3.  For every incoming transaction, extract the signature and route it to
///     a bounded worker queue that calls `process_signature`.
/// 4.  On stream errors, reconnect with exponential back-off.
pub async fn run_grpc_stream(state: Arc<IndexerState>) -> Result<(), IndexerError> {
    let program_str = state.config.program_id.to_string();
    let endpoint = state
        .config
        .grpc_endpoint
        .as_deref()
        .expect("GRPC_ENDPOINT validated at config load");
    let x_token = state.config.grpc_x_token.clone();
    let queue_size = state.config.grpc_queue_size;

    // ---- Initial backfill ---------------------------------------------------
    let last = db::get_last_processed(&state.pool, &program_str).await?;
    let last_sig = last.as_ref().and_then(|(_, s)| s.clone());

    if last_sig.is_some() {
        info!("gRPC: back-filling from last processed signature");
    } else {
        info!("gRPC: fresh start, no previous state found");
    }
    super::backfill(&state, last_sig.as_deref()).await?;

    // ---- Reconnect loop -----------------------------------------------------
    let mut reconnect_delay = Duration::from_secs(1);

    loop {
        if state.cancel.is_cancelled() {
            break;
        }

        // -- Connect ----------------------------------------------------------
        info!(endpoint, "gRPC: connecting to Yellowstone endpoint");
        let channel = match connect_grpc(endpoint).await {
            Ok(ch) => {
                reconnect_delay = Duration::from_secs(1);
                ch
            }
            Err(e) => {
                warn!(error = %e, delay = ?reconnect_delay, "gRPC: connect failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(reconnect_delay) => {}
                    _ = state.cancel.cancelled() => break,
                }
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
                continue;
            }
        };

        // -- Subscribe --------------------------------------------------------
        let stream_result = subscribe(
            channel,
            &program_str,
            x_token.clone(),
        )
        .await;

        let mut stream = match stream_result {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, delay = ?reconnect_delay, "gRPC: subscribe failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(reconnect_delay) => {}
                    _ = state.cancel.cancelled() => break,
                }
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
                continue;
            }
        };

        info!("gRPC: stream active, processing transactions");

        // -- Worker queue -----------------------------------------------------
        let (tx, mut rx) = mpsc::channel::<String>(queue_size);
        let worker_state = state.clone();
        let worker_program = program_str.clone();
        let worker = tokio::spawn(async move {
            while let Some(sig) = rx.recv().await {
                match super::process_signature(&worker_state, &sig).await {
                    Ok(slot) => {
                        let _ = db::update_sync_state(
                            &worker_state.pool,
                            &worker_program,
                            slot,
                            Some(&sig),
                        )
                        .await;
                    }
                    Err(e) => warn!(%sig, error = %e, "gRPC: failed to process tx"),
                }
            }
        });

        // -- Consume stream ---------------------------------------------------
        let stream_ended = loop {
            tokio::select! {
                _ = state.cancel.cancelled() => {
                    info!("gRPC: stream stopped by shutdown");
                    break false;
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(update)) => {
                            match update.update_oneof {
                                Some(UpdateOneof::Transaction(tx_update)) => {
                                    if let Some(tx_info) = tx_update.transaction.as_ref() {
                                        let sig = bs58::encode(&tx_info.signature).into_string();
                                        debug!(%sig, slot = tx_update.slot, "gRPC: received tx");
                                        if tx.send(sig).await.is_err() {
                                            error!("gRPC: worker queue closed unexpectedly");
                                            break false;
                                        }
                                    }
                                }
                                Some(UpdateOneof::Slot(slot_update)) => {
                                    debug!(
                                        slot = slot_update.slot,
                                        "gRPC: slot update"
                                    );
                                }
                                _ => {} // ping, block, account — ignored
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "gRPC: stream error");
                            break true; // reconnect
                        }
                        None => {
                            warn!("gRPC: stream closed by server");
                            break true; // reconnect
                        }
                    }
                }
            }
        };

        // Drop sender so the worker drains remaining items and exits.
        drop(tx);
        let _ = worker.await;

        if !stream_ended {
            // Shutdown was requested.
            break;
        }

        // Re-backfill on reconnect to bridge any gap.
        info!("gRPC: re-backfilling after disconnect");
        let last = db::get_last_processed(&state.pool, &program_str).await?;
        let last_sig = last.as_ref().and_then(|(_, s)| s.clone());
        super::backfill(&state, last_sig.as_deref()).await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// gRPC helpers
// ---------------------------------------------------------------------------

/// Establish a TLS-secured gRPC channel with tuned HTTP/2 window sizes.
async fn connect_grpc(endpoint: &str) -> Result<Channel, IndexerError> {
    Channel::from_shared(endpoint.to_string())
        .map_err(|e| IndexerError::Grpc(format!("Invalid gRPC endpoint URL: {e}")))?
        .tls_config(
            ClientTlsConfig::new().with_native_roots(),
        )
        .map_err(|e| IndexerError::Grpc(format!("TLS config error: {e}")))?
        .http2_adaptive_window(true)
        .initial_connection_window_size(1 << 23) // 8 MB
        .initial_stream_window_size(1 << 23) // 8 MB
        .tcp_keepalive(Some(Duration::from_secs(10)))
        .connect_timeout(Duration::from_secs(15))
        .connect()
        .await
        .map_err(|e| IndexerError::Grpc(format!("gRPC connect failed: {e}")))
}

/// Build a Geyser subscribe stream filtered to confirmed, non-vote
/// transactions that mention the given program ID.
async fn subscribe(
    channel: Channel,
    program_id: &str,
    x_token: Option<String>,
) -> Result<
    tonic::Streaming<yellowstone_grpc_proto::geyser::SubscribeUpdate>,
    IndexerError,
> {
    // Prepare the initial subscribe request.
    let subscribe_req = SubscribeRequest {
        transactions: HashMap::from([(
            "program".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: None,
                account_include: vec![program_id.to_string()],
                ..Default::default()
            },
        )]),
        slots: HashMap::from([(
            "slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(false),
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };

    // Send the subscribe request through a one-shot channel.
    let (req_tx, req_rx) = mpsc::channel::<SubscribeRequest>(1);
    req_tx
        .send(subscribe_req)
        .await
        .map_err(|_| IndexerError::Grpc("Failed to queue subscribe request".into()))?;

    let request_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);

    // Build the gRPC client with optional x-token auth interceptor.
    let mut client = GeyserClient::with_interceptor(channel, move |mut req: Request<()>| {
        if let Some(ref token) = x_token {
            let val = MetadataValue::try_from(token.as_str())
                .map_err(|_| Status::internal("Invalid x-token value"))?;
            req.metadata_mut().insert("x-token", val);
        }
        Ok(req)
    })
    .max_decoding_message_size(64 * 1024 * 1024); // 64 MB

    let response = client
        .subscribe(request_stream)
        .await
        .map_err(|e| IndexerError::Grpc(format!("gRPC subscribe failed: {e}")))?;

    Ok(response.into_inner())
}
