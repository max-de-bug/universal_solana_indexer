mod api;
mod config;
mod db;
mod error;
mod idl;
mod indexer;

use crate::config::Config;
use crate::idl::AnchorIdl;
use crate::indexer::fetcher::Fetcher;
use crate::indexer::IndexerState;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ---- Bootstrap ----------------------------------------------------------
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "universal_solana_indexer=info,sqlx=warn".into()),
        )
        .with_target(true)
        .json()
        .init();

    info!("Starting Universal Solana Indexer");

    let config = Config::from_env()?;
    let pool = db::create_pool(&config.database_url).await?;

    // ---- IDL loading (file → on-chain) --------------------------------------
    let idl = load_idl(&config).await?;

    // ---- Schema initialisation ----------------------------------------------
    db::initialize_schema(&pool, &idl, &idl.metadata.name).await?;

    // ---- Cancellation token for graceful shutdown ----------------------------
    let cancel = CancellationToken::new();

    // ---- API server ---------------------------------------------------------
    let account_types: Vec<String> = idl.accounts.iter().map(|a| a.name.clone()).collect();
    let api_state = Arc::new(api::ApiState {
        pool: pool.clone(),
        program_name: idl.metadata.name.clone(),
        program_id: config.program_id.to_string(),
        account_types,
    });

    let app = api::router(api_state).layer(CorsLayer::permissive());
    let api_port = config.api_port;
    let api_cancel = cancel.clone();

    let api_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{api_port}"))
            .await
            .expect("Failed to bind API port");
        info!(%api_port, "API server listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(api_cancel.cancelled_owned())
            .await
            .expect("API server error");
    });

    // ---- Indexer ------------------------------------------------------------
    let fetcher = Fetcher::new(
        &config.rpc_urls,
        config.max_retries,
        config.initial_retry_delay_ms,
        cancel.clone(),
    );

    let indexer_state = Arc::new(IndexerState {
        pool: pool.clone(),
        idl,
        config: config.clone(),
        fetcher,
        cancel: cancel.clone(),
    });

    let indexer_handle = tokio::spawn(indexer::run(indexer_state));

    // ---- Graceful shutdown --------------------------------------------------
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT – initiating graceful shutdown");
            cancel.cancel();
        }
        res = indexer_handle => {
            match res {
                Ok(Ok(())) => info!("Indexer finished"),
                Ok(Err(e)) => error!(error = %e, "Indexer error"),
                Err(e) => error!(error = %e, "Indexer task panicked"),
            }
            cancel.cancel();
        }
    }

    // Wait for API to drain requests.
    let _ = api_handle.await;
    pool.close().await;
    info!("Shutdown complete");

    Ok(())
}

/// Load IDL: prioritise local file, fall back to on-chain account.
async fn load_idl(config: &Config) -> anyhow::Result<AnchorIdl> {
    // 1. Try local file.
    if let Some(ref path) = config.idl_path {
        if std::fs::metadata(path).is_ok() {
            info!(%path, "Loading IDL from file");
            return AnchorIdl::from_file(path);
        }
    }

    // 2. Try on-chain account.
    if let Some(ref addr) = config.idl_account {
        let pubkey = Pubkey::from_str(addr)
            .map_err(|e| anyhow::anyhow!("Invalid IDL_ACCOUNT address: {e}"))?;
        let rpc = RpcClient::new_with_commitment(
            config.rpc_urls.first().cloned().unwrap_or_else(|| "https://api.mainnet-beta.solana.com".into()),
            CommitmentConfig::confirmed(),
        );
        info!(%addr, "Fetching IDL from on-chain account");
        return AnchorIdl::from_chain(&rpc, &pubkey).await;
    }

    anyhow::bail!("No IDL source available: set IDL_PATH or IDL_ACCOUNT")
}
