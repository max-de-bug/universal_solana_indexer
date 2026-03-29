use crate::error::IndexerError;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcTransactionConfig, RpcTransactionLogsConfig, RpcTransactionLogsFilter};
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, warn};

/// RPC wrapper with automatic retries and exponential backoff.
pub struct Fetcher {
    rpc: RpcClient,
    max_retries: u32,
    initial_delay: Duration,
}

impl Fetcher {
    pub fn new(rpc_url: &str, max_retries: u32, initial_delay_ms: u64) -> Self {
        let rpc = RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        );
        Self {
            rpc,
            max_retries,
            initial_delay: Duration::from_millis(initial_delay_ms),
        }
    }

    /// Get current slot with retries.
    pub async fn get_slot(&self) -> Result<u64, IndexerError> {
        self.with_retry("get_slot", || self.rpc.get_slot()).await
    }

    /// Fetch signatures for a program address, paginated.
    /// Returns results in reverse chronological order (newest first).
    pub async fn get_signatures(
        &self,
        program: &Pubkey,
        before: Option<&str>,
        until: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>, IndexerError> {
        let before_sig = before
            .map(|s| Signature::from_str(s))
            .transpose()
            .map_err(|e| IndexerError::Rpc(format!("Invalid 'before' signature: {e}")))?;
        let until_sig = until
            .map(|s| Signature::from_str(s))
            .transpose()
            .map_err(|e| IndexerError::Rpc(format!("Invalid 'until' signature: {e}")))?;

        let config = solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
            before: before_sig,
            until: until_sig,
            limit: Some(limit),
            commitment: Some(CommitmentConfig::confirmed()),
        };

        let program = *program;
        self.with_retry("get_signatures", || {
            self.rpc
                .get_signatures_for_address_with_config(&program, config.clone())
        })
        .await
    }

    /// Fetch a full transaction by signature.
    pub async fn get_transaction(
        &self,
        sig: &str,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, IndexerError> {
        let signature = Signature::from_str(sig)
            .map_err(|e| IndexerError::Rpc(format!("Invalid signature: {e}")))?;

        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        self.with_retry("get_transaction", || {
            self.rpc
                .get_transaction_with_config(&signature, config)
        })
        .await
    }

    /// Fetch account data for a specific pubkey.
    pub async fn get_account_data(
        &self,
        pubkey: &Pubkey,
    ) -> Result<Option<Vec<u8>>, IndexerError> {
        let pk = *pubkey;
        match self
            .with_retry("get_account_info", || self.rpc.get_account_data(&pk))
            .await
        {
            Ok(data) => Ok(Some(data)),
            Err(IndexerError::Rpc(msg)) if msg.contains("AccountNotFound") => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Generic retry wrapper with exponential backoff + jitter.
    async fn with_retry<F, T>(&self, op: &str, f: F) -> Result<T, IndexerError>
    where
        F: Fn() -> Result<T, solana_client::client_error::ClientError>,
    {
        let mut delay = self.initial_delay;
        for attempt in 0..=self.max_retries {
            match f() {
                Ok(val) => return Ok(val),
                Err(e) => {
                    if attempt == self.max_retries {
                        error!(%op, attempt, error = %e, "Max retries exhausted");
                        return Err(IndexerError::Rpc(format!("{op}: {e}")));
                    }
                    warn!(%op, attempt, error = %e, retry_in = ?delay, "RPC call failed, retrying");
                    tokio::time::sleep(delay).await;
                    // Exponential backoff: double delay, cap at 30s.
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        unreachable!()
    }
}
