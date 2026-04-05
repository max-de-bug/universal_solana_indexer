use crate::error::IndexerError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::str::FromStr;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug)]
pub struct NodeMetrics {
    pub ema_latency_ms: f64,
    pub success_count: u64,
    pub total_count: u64,
    pub last_slot: u64,
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            ema_latency_ms: 1000.0,
            success_count: 1,
            total_count: 1,
            last_slot: 0,
        }
    }
}

pub struct RpcNode {
    pub client: RpcClient,
    pub url: String,
    pub metrics: RwLock<NodeMetrics>,
}

macro_rules! retry_rpc {
    ($self:expr, $op:expr, $rpc:ident, $body:expr) => {{
        let mut delay = $self.initial_delay;
        let mut attempt = 0;
        loop {
            let node = $self.best_node(attempt);
            let $rpc = &node.client;
            let start = tokio::time::Instant::now();
            tracing::debug!(op = $op, url = %node.url, attempt, "Routing to node");
            match $body.await {
                Ok(v) => {
                    let elapsed = start.elapsed().as_millis() as f64;
                    {
                        let mut lock = node.metrics.write();
                        lock.ema_latency_ms = (lock.ema_latency_ms * 0.8) + (elapsed * 0.2);
                        lock.success_count += 1;
                        lock.total_count += 1;
                    }
                    break Ok(v)
                },
                Err(e) => {
                    {
                        let mut lock = node.metrics.write();
                        lock.total_count += 1;
                    }
                    if attempt == $self.max_retries {
                        tracing::error!(op = $op, attempt, error = %e, "Max retries exhausted");
                        break Err(crate::error::IndexerError::Rpc(format!("{}: {}", $op, e)));
                    }
                    tracing::warn!(op = $op, attempt, error = %e, retry_in = ?delay, "Retrying");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = $self.cancel.cancelled() => {
                            break Err(crate::error::IndexerError::Rpc(format!("{}: cancelled during retry", $op)));
                        }
                    }
                    delay = (delay * 2).min(Duration::from_secs(30));
                    attempt += 1;
                }
            }
        }
    }};
}

/// Async RPC wrapper with intelligent cluster load balancing.
pub struct Fetcher {
    nodes: Arc<Vec<RpcNode>>,
    max_retries: u32,
    initial_delay: Duration,
    cancel: CancellationToken,
}

impl Fetcher {
    pub fn new(rpc_urls: &[String], max_retries: u32, initial_delay_ms: u64, cancel: CancellationToken) -> Self {
        let nodes: Vec<RpcNode> = rpc_urls
            .iter()
            .map(|url| {
                RpcNode {
                    client: RpcClient::new_with_commitment(url.to_string(), CommitmentConfig::confirmed()),
                    url: url.to_string(),
                    metrics: RwLock::new(NodeMetrics::default()),
                }
            })
            .collect();
            
        let nodes = Arc::new(nodes);
        
        let fetcher_cancel = cancel.clone();
        let bg_nodes = nodes.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = fetcher_cancel.cancelled() => break,
                }
                for node in bg_nodes.iter() {
                    let start = Instant::now();
                    if let Ok(slot) = node.client.get_slot().await {
                        let elapsed = start.elapsed().as_millis() as f64;
                        let mut m = node.metrics.write();
                        m.last_slot = m.last_slot.max(slot);
                        m.ema_latency_ms = (m.ema_latency_ms * 0.8) + (elapsed * 0.2);
                        m.success_count += 1;
                        m.total_count += 1;
                    } else {
                        node.metrics.write().total_count += 1;
                    }
                }
            }
        });

        Self {
            nodes,
            max_retries,
            initial_delay: Duration::from_millis(initial_delay_ms),
            cancel,
        }
    }

    fn best_node(&self, attempt: u32) -> &RpcNode {
        let max_slot = self.nodes.iter().map(|n| n.metrics.read().last_slot).max().unwrap_or(0);
        let mut scored: Vec<(&RpcNode, f64)> = self.nodes.iter().map(|node| {
            let m = node.metrics.read();
            let success_rate = m.success_count as f64 / m.total_count.max(1) as f64;
            let lag = max_slot.saturating_sub(m.last_slot);
            let lag_penalty = if lag > 10 { 0.5 } else { 1.0 };
            let score = (success_rate / m.ema_latency_ms.max(1.0)) * lag_penalty;
            (node, score)
        }).collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let idx = (attempt as usize) % scored.len();
        scored[idx].0
    }

    /// Fetch signatures for a program address, paginated.
    pub async fn get_signatures(
        &self,
        program: &Pubkey,
        before: Option<&str>,
        until: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>, IndexerError> {
        let before_sig = before.map(|s| Signature::from_str(s)).transpose().map_err(|e| IndexerError::Rpc(format!("Invalid 'before': {e}")))?;
        let until_sig = until.map(|s| Signature::from_str(s)).transpose().map_err(|e| IndexerError::Rpc(format!("Invalid 'until': {e}")))?;
        let program = *program;

        retry_rpc!(self, "get_signatures", rpc, {
            let config = GetConfirmedSignaturesForAddress2Config {
                before: before_sig,
                until: until_sig,
                limit: Some(limit),
                commitment: Some(CommitmentConfig::confirmed()),
            };
            rpc.get_signatures_for_address_with_config(&program, config)
        })
    }

    /// Fetch a full transaction by signature.
    pub async fn get_transaction(&self, sig: &str) -> Result<EncodedConfirmedTransactionWithStatusMeta, IndexerError> {
        let signature = Signature::from_str(sig).map_err(|e| IndexerError::Rpc(format!("Invalid signature: {e}")))?;
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        retry_rpc!(self, "get_transaction", rpc, {
            rpc.get_transaction_with_config(&signature, config)
        })
    }

    /// Fetch account data for a specific pubkey.
    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Option<Vec<u8>>, IndexerError> {
        let pk = *pubkey;
        match retry_rpc!(self, "get_account_info", rpc, { rpc.get_account_data(&pk) }) {
            Ok(data) => Ok(Some(data)),
            Err(IndexerError::SolanaClient(ref e)) if e.to_string().contains("AccountNotFound") => Ok(None),
            Err(IndexerError::Rpc(ref msg)) if msg.contains("AccountNotFound") => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Fetch all accounts owned by a program.
    pub async fn get_program_accounts(&self, program: &Pubkey) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>, IndexerError> {
        let pk = *program;
        retry_rpc!(self, "get_program_accounts", rpc, { rpc.get_program_accounts(&pk) })
    }
}
