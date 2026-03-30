use std::str::FromStr;

use solana_sdk::pubkey::Pubkey;
use tracing::info;

/// Top-level application configuration, loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    pub rpc_url: String,
    pub wss_url: String,
    pub database_url: String,
    /// Path to a local IDL JSON file (takes priority over `idl_account`).
    pub idl_path: Option<String>,
    /// On-chain IDL account address — fetched and decompressed at startup.
    pub idl_account: Option<String>,
    pub program_id: Pubkey,
    pub mode: IndexingMode,
    pub api_port: u16,
    pub batch_size: usize,
    pub max_retries: u32,
    pub initial_retry_delay_ms: u64,
    pub poll_interval_ms: u64,
}

/// Determines how the indexer processes data.
#[derive(Debug, Clone)]
pub enum IndexingMode {
    /// Process a specified slot range.
    Batch { start_slot: u64, end_slot: u64 },
    /// Process a list of known signatures.
    BatchSignatures { signatures: Vec<String> },
    /// Subscribe to new transactions with cold-start backfill.
    Realtime,
}

impl Config {
    /// Build configuration from environment variables.
    /// Panics early with clear messages for missing required vars.
    pub fn from_env() -> anyhow::Result<Self> {
        let rpc_url = env_or("RPC_URL", "https://api.mainnet-beta.solana.com");
        let wss_url = env_or("WSS_URL", "wss://api.mainnet-beta.solana.com");
        let database_url = required_env("DATABASE_URL")?;
        let idl_path = std::env::var("IDL_PATH").ok();
        let idl_account = std::env::var("IDL_ACCOUNT").ok();
        anyhow::ensure!(
            idl_path.is_some() || idl_account.is_some(),
            "At least one of IDL_PATH or IDL_ACCOUNT must be set"
        );
        let program_id = Pubkey::from_str(&required_env("PROGRAM_ID")?)
            .map_err(|e| anyhow::anyhow!("Invalid PROGRAM_ID: {e}"))?;

        let mode = match std::env::var("INDEXING_MODE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "batch" => {
                let start = required_env("BATCH_START_SLOT")?.parse::<u64>()?;
                let end = required_env("BATCH_END_SLOT")?.parse::<u64>()?;
                anyhow::ensure!(start <= end, "BATCH_START_SLOT must be <= BATCH_END_SLOT");
                IndexingMode::Batch {
                    start_slot: start,
                    end_slot: end,
                }
            }
            "batch_signatures" => {
                let raw = required_env("BATCH_SIGNATURES")?;
                let sigs: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).collect();
                anyhow::ensure!(!sigs.is_empty(), "BATCH_SIGNATURES must not be empty");
                IndexingMode::BatchSignatures { signatures: sigs }
            }
            _ => IndexingMode::Realtime,
        };

        let api_port = env_or("API_PORT", "3000").parse()?;
        let batch_size = env_or("BATCH_SIZE", "100").parse()?;
        let max_retries = env_or("MAX_RETRIES", "5").parse()?;
        let initial_retry_delay_ms = env_or("INITIAL_RETRY_DELAY_MS", "500").parse()?;
        let poll_interval_ms = env_or("POLL_INTERVAL_MS", "2000").parse()?;

        let cfg = Self {
            rpc_url,
            wss_url,
            database_url,
            idl_path,
            idl_account,
            program_id,
            mode,
            api_port,
            batch_size,
            max_retries,
            initial_retry_delay_ms,
            poll_interval_ms,
        };

        info!(?cfg.mode, %cfg.program_id, %cfg.api_port, "Configuration loaded");
        Ok(cfg)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn required_env(key: &str) -> anyhow::Result<String> {
    std::env::var(key).map_err(|_| anyhow::anyhow!("Missing required env var: {key}"))
}
