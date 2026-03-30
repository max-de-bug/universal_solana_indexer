use crate::idl::{idl_type_to_sql, AnchorIdl};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tracing::info;

// ---------------------------------------------------------------------------
// Pool creation
// ---------------------------------------------------------------------------

pub async fn create_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;
    info!("Connected to PostgreSQL");
    Ok(pool)
}

// ---------------------------------------------------------------------------
// Dedup helper
// ---------------------------------------------------------------------------

/// Returns true if a transaction with this signature is already indexed.
pub async fn transaction_exists(pool: &PgPool, signature: &str) -> anyhow::Result<bool> {
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM transactions WHERE signature = $1)",
    )
    .bind(signature)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

// ---------------------------------------------------------------------------
// Schema initialisation (fully dynamic from IDL)
// ---------------------------------------------------------------------------

pub async fn initialize_schema(
    pool: &PgPool,
    idl: &AnchorIdl,
    program_name: &str,
) -> anyhow::Result<()> {
    create_sync_state_table(pool).await?;
    create_transactions_table(pool).await?;
    create_instructions_table(pool).await?;

    for account_def in &idl.accounts {
        let fields = idl.account_fields(&account_def.name);
        create_account_table(pool, program_name, &account_def.name, fields).await?;
    }

    for ix_def in &idl.instructions {
        create_instruction_table(pool, program_name, &ix_def.name, &ix_def.args).await?;
    }

    info!(%program_name, "Database schema initialised");
    Ok(())
}

// ---- Core tables ----------------------------------------------------------

async fn create_sync_state_table(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS sync_state (
            id            SERIAL      PRIMARY KEY,
            program_id    TEXT        NOT NULL UNIQUE,
            last_slot     BIGINT      NOT NULL DEFAULT 0,
            last_signature TEXT,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_transactions_table(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS transactions (
            id          BIGSERIAL   PRIMARY KEY,
            signature   TEXT        NOT NULL UNIQUE,
            slot        BIGINT      NOT NULL,
            block_time  TIMESTAMPTZ,
            success     BOOLEAN     NOT NULL,
            fee         BIGINT,
            err_msg     TEXT,
            indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await?;

    for idx in &[
        "CREATE INDEX IF NOT EXISTS idx_tx_slot       ON transactions(slot)",
        "CREATE INDEX IF NOT EXISTS idx_tx_block_time ON transactions(block_time)",
        "CREATE INDEX IF NOT EXISTS idx_tx_success    ON transactions(success)",
    ] {
        sqlx::query(idx).execute(pool).await?;
    }
    Ok(())
}

async fn create_instructions_table(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS instructions (
            id                     BIGSERIAL   PRIMARY KEY,
            transaction_signature  TEXT        NOT NULL,
            instruction_index      INTEGER     NOT NULL,
            instruction_name       TEXT        NOT NULL,
            program_id             TEXT        NOT NULL,
            args                   JSONB,
            accounts               JSONB,
            raw_data               BYTEA,
            indexed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT fk_ix_tx FOREIGN KEY (transaction_signature)
                REFERENCES transactions(signature)
        )",
    )
    .execute(pool)
    .await?;

    for idx in &[
        "CREATE INDEX IF NOT EXISTS idx_ix_name   ON instructions(instruction_name)",
        "CREATE INDEX IF NOT EXISTS idx_ix_tx_sig ON instructions(transaction_signature)",
        "CREATE INDEX IF NOT EXISTS idx_ix_prog   ON instructions(program_id)",
    ] {
        sqlx::query(idx).execute(pool).await?;
    }
    Ok(())
}

// ---- Dynamic account tables -----------------------------------------------

async fn create_account_table(
    pool: &PgPool,
    program_name: &str,
    account_name: &str,
    fields: Option<&Vec<crate::idl::IdlField>>,
) -> anyhow::Result<()> {
    let table = format!("\"{}\"" , sanitize_name(&format!("{program_name}_{account_name}")));

    let mut cols = vec![
        "id                     BIGSERIAL   PRIMARY KEY".to_string(),
        "pubkey                 TEXT        NOT NULL".to_string(),
        "slot                   BIGINT      NOT NULL".to_string(),
        "transaction_signature  TEXT".to_string(),
        "data                   JSONB       NOT NULL DEFAULT '{}'::jsonb".to_string(),
    ];

    if let Some(fields) = fields {
        for f in fields {
            let col = sanitize_name(&f.name);
            let ty = idl_type_to_sql(&f.field_type);
            cols.push(format!("{col}  {ty}"));
        }
    }

    cols.push("indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()".to_string());

    let ddl = format!("CREATE TABLE IF NOT EXISTS {table} ({})", cols.join(", "));
    sqlx::query(&ddl).execute(pool).await?;

    for idx in &[
        format!("CREATE INDEX IF NOT EXISTS idx_{table}_pubkey ON {table}(pubkey)"),
        format!("CREATE INDEX IF NOT EXISTS idx_{table}_slot   ON {table}(slot)"),
    ] {
        sqlx::query(idx).execute(pool).await?;
    }

    info!(%table, "Dynamic account table ready");
    Ok(())
}

async fn create_instruction_table(
    pool: &PgPool,
    program_name: &str,
    ix_name: &str,
    fields: &[crate::idl::IdlField],
) -> anyhow::Result<()> {
    let table = format!("\"{}\"" , sanitize_name(&format!("{program_name}_ix_{ix_name}")));

    let mut cols = vec![
        "id                     BIGSERIAL   PRIMARY KEY".to_string(),
        "transaction_signature  TEXT        NOT NULL".to_string(),
        "instruction_index      INTEGER     NOT NULL".to_string(),
    ];

    for f in fields {
        let col = sanitize_name(&f.name);
        let ty = idl_type_to_sql(&f.field_type);
        cols.push(format!("{col}  {ty}"));
    }

    cols.push("indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()".to_string());

    let ddl = format!("CREATE TABLE IF NOT EXISTS {table} ({})", cols.join(", "));
    sqlx::query(&ddl).execute(pool).await?;

    let idx = format!("CREATE INDEX IF NOT EXISTS idx_{table}_tx ON {table}(transaction_signature)");
    sqlx::query(&idx).execute(pool).await?;

    Ok(())
}

/// Strip non-alphanumeric characters and lowercase for safe SQL identifiers.
pub fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Sync-state helpers
// ---------------------------------------------------------------------------

pub async fn get_last_processed(
    pool: &PgPool,
    program_id: &str,
) -> anyhow::Result<Option<(u64, Option<String>)>> {
    let row: Option<(i64, Option<String>)> = sqlx::query_as(
        "SELECT last_slot, last_signature FROM sync_state WHERE program_id = $1",
    )
    .bind(program_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(s, sig)| (s as u64, sig)))
}

pub async fn update_sync_state(
    pool: &PgPool,
    program_id: &str,
    slot: u64,
    signature: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO sync_state (program_id, last_slot, last_signature, updated_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (program_id) DO UPDATE SET
             last_slot      = EXCLUDED.last_slot,
             last_signature = EXCLUDED.last_signature,
             updated_at     = NOW()",
    )
    .bind(program_id)
    .bind(slot as i64)
    .bind(signature)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Data insertion
// ---------------------------------------------------------------------------

pub async fn insert_transaction(
    pool: &PgPool,
    signature: &str,
    slot: u64,
    block_time: Option<i64>,
    success: bool,
    fee: Option<u64>,
    err_msg: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO transactions (signature, slot, block_time, success, fee, err_msg)
         VALUES ($1, $2, to_timestamp($3), $4, $5, $6)
         ON CONFLICT (signature) DO NOTHING",
    )
    .bind(signature)
    .bind(slot as i64)
    .bind(block_time.map(|t| t as f64))
    .bind(success)
    .bind(fee.map(|f| f as i64))
    .bind(err_msg)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_instruction(
    pool: &PgPool,
    tx_sig: &str,
    ix_index: i32,
    ix_name: &str,
    program_id: &str,
    args: &serde_json::Value,
    accounts: &serde_json::Value,
    raw_data: Option<&[u8]>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO instructions
            (transaction_signature, instruction_index, instruction_name, program_id, args, accounts, raw_data)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT DO NOTHING",
    )
    .bind(tx_sig)
    .bind(ix_index)
    .bind(ix_name)
    .bind(program_id)
    .bind(args)
    .bind(accounts)
    .bind(raw_data)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_account_state(
    pool: &PgPool,
    program_name: &str,
    account_type: &str,
    pubkey: &str,
    slot: u64,
    tx_sig: Option<&str>,
    data: &serde_json::Value,
) -> anyhow::Result<()> {
    let table = sanitize_name(&format!("{program_name}_{account_type}"));
    let obj = match data.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    let mut dyn_cols = vec![];
    for k in obj.keys() {
        dyn_cols.push(sanitize_name(k));
    }

    let dyn_selector = if dyn_cols.is_empty() {
        "".to_string()
    } else {
        format!(", {}", dyn_cols.clone().join(", "))
    };

    let sql = format!(
        "INSERT INTO \"{table}\" (pubkey, slot, transaction_signature, data{})
         SELECT $1, $2, $3, $4{} 
         FROM jsonb_populate_record(NULL::\"{table}\", $4::jsonb)",
        dyn_selector, dyn_selector
    );

    sqlx::query(&sql)
        .bind(pubkey)
        .bind(slot as i64)
        .bind(tx_sig)
        .bind(data)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_dynamic_instruction(
    pool: &PgPool,
    program_name: &str,
    tx_sig: &str,
    ix_index: i32,
    ix_name: &str,
    args: &serde_json::Value,
) -> anyhow::Result<()> {
    let table = sanitize_name(&format!("{program_name}_ix_{ix_name}"));
    
    let obj = match args.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    if obj.is_empty() {
        let sql = format!(
            "INSERT INTO \"{table}\" (transaction_signature, instruction_index) VALUES ($1, $2)"
        );
        sqlx::query(&sql)
            .bind(tx_sig)
            .bind(ix_index)
            .execute(pool)
            .await?;
        return Ok(());
    }

    let mut dyn_cols = vec![];
    for k in obj.keys() {
        dyn_cols.push(sanitize_name(k));
    }

    let dyn_selector = dyn_cols.join(", ");

    let sql = format!(
        "INSERT INTO \"{table}\" (transaction_signature, instruction_index, {dyn_selector})
         SELECT $1, $2, {dyn_selector} 
         FROM jsonb_populate_record(NULL::\"{table}\", $3::jsonb)"
    );

    sqlx::query(&sql)
        .bind(tx_sig)
        .bind(ix_index)
        .bind(args)
        .execute(pool)
        .await?;
    Ok(())
}

