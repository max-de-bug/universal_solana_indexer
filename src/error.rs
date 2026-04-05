use thiserror::Error;

/// Unified error type for the indexer.
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum IndexerError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Solana client error: {0}")]
    SolanaClient(#[from] solana_client::client_error::ClientError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("IDL parsing error: {0}")]
    IdlParse(String),

    #[error("Decoding error at offset {offset}: {message}")]
    Decode { offset: usize, message: String },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Max retries ({0}) exhausted")]
    RetriesExhausted(u32),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}
