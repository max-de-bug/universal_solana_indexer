use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::error;

use crate::db::sanitize_name;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

pub struct ApiState {
    pub pool: PgPool,
    pub program_name: String,
    pub program_id: String,
    pub account_types: Vec<String>,
}

pub fn router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/transactions", get(list_transactions))
        .route("/api/v1/instructions", get(list_instructions))
        .route("/api/v1/accounts/:account_type", get(list_accounts))
        .route("/api/v1/stats", get(stats))
        .route("/api/v1/stats/instructions", get(instruction_aggregation))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Query parameter structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct TxQuery {
    pub signature: Option<String>,
    pub slot_from: Option<i64>,
    pub slot_to: Option<i64>,
    pub success: Option<bool>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct IxQuery {
    pub name: Option<String>,
    pub transaction_signature: Option<String>,
    pub program_id: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AccountQuery {
    pub pubkey: Option<String>,
    pub slot_from: Option<i64>,
    pub slot_to: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AggQuery {
    pub name: Option<String>,
    /// "hour" or "day"
    pub period: Option<String>,
    pub from_time: Option<String>,
    pub to_time: Option<String>,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StatsResponse {
    total_transactions: i64,
    successful_transactions: i64,
    failed_transactions: i64,
    total_instructions: i64,
    instruction_breakdown: Vec<InstructionCount>,
    latest_slot: Option<i64>,
    account_types: Vec<String>,
}

#[derive(Serialize)]
struct InstructionCount {
    name: String,
    count: i64,
}

#[derive(Serialize)]
struct AggBucket {
    period: String,
    instruction_name: String,
    count: i64,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health() -> &'static str {
    "OK"
}

async fn list_transactions(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<TxQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).min(500);
    let offset = q.offset.unwrap_or(0);

    let mut sql = String::from(
        "SELECT signature, slot, block_time, success, fee, err_msg, indexed_at
         FROM transactions WHERE 1=1",
    );
    let mut params: Vec<String> = Vec::new();
    let mut idx = 1;

    if let Some(ref sig) = q.signature {
        sql.push_str(&format!(" AND signature = ${idx}"));
        params.push(sig.clone());
        idx += 1;
    }
    if let Some(from) = q.slot_from {
        sql.push_str(&format!(" AND slot >= ${idx}"));
        params.push(from.to_string());
        idx += 1;
    }
    if let Some(to) = q.slot_to {
        sql.push_str(&format!(" AND slot <= ${idx}"));
        params.push(to.to_string());
        idx += 1;
    }
    if let Some(success) = q.success {
        sql.push_str(&format!(" AND success = ${idx}"));
        params.push(success.to_string());
        idx += 1;
    }
    sql.push_str(&format!(
        " ORDER BY slot DESC LIMIT ${idx} OFFSET ${}",
        idx + 1
    ));
    params.push(limit.to_string());
    params.push(offset.to_string());

    // Build dynamic query using raw SQL with indexed parameters.
    let mut query = sqlx::query(&sql);
    for p in &params {
        query = query.bind(p);
    }

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
            let results: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    use sqlx::Row;
                    serde_json::json!({
                        "signature": r.get::<String, _>("signature"),
                        "slot": r.get::<i64, _>("slot"),
                        "block_time": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("block_time"),
                        "success": r.get::<bool, _>("success"),
                        "fee": r.get::<Option<i64>, _>("fee"),
                        "err_msg": r.get::<Option<String>, _>("err_msg"),
                        "indexed_at": r.get::<chrono::DateTime<chrono::Utc>, _>("indexed_at"),
                    })
                })
                .collect();
            Json(serde_json::json!({ "data": results, "count": results.len() })).into_response()
        }
        Err(e) => {
            error!(error = %e, "Transaction query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn list_instructions(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<IxQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).min(500);
    let offset = q.offset.unwrap_or(0);

    let mut sql = String::from(
        "SELECT id, transaction_signature, instruction_index, instruction_name, program_id, args, accounts, indexed_at
         FROM instructions WHERE 1=1",
    );
    let mut params: Vec<String> = Vec::new();
    let mut idx = 1;

    if let Some(ref name) = q.name {
        sql.push_str(&format!(" AND instruction_name = ${idx}"));
        params.push(name.clone());
        idx += 1;
    }
    if let Some(ref tx) = q.transaction_signature {
        sql.push_str(&format!(" AND transaction_signature = ${idx}"));
        params.push(tx.clone());
        idx += 1;
    }
    if let Some(ref pid) = q.program_id {
        sql.push_str(&format!(" AND program_id = ${idx}"));
        params.push(pid.clone());
        idx += 1;
    }
    sql.push_str(&format!(
        " ORDER BY id DESC LIMIT ${idx} OFFSET ${}",
        idx + 1
    ));
    params.push(limit.to_string());
    params.push(offset.to_string());

    let mut query = sqlx::query(&sql);
    for p in &params {
        query = query.bind(p);
    }

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
            let results: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    use sqlx::Row;
                    serde_json::json!({
                        "id": r.get::<i64, _>("id"),
                        "transaction_signature": r.get::<String, _>("transaction_signature"),
                        "instruction_index": r.get::<i32, _>("instruction_index"),
                        "instruction_name": r.get::<String, _>("instruction_name"),
                        "program_id": r.get::<String, _>("program_id"),
                        "args": r.get::<Option<serde_json::Value>, _>("args"),
                        "accounts": r.get::<Option<serde_json::Value>, _>("accounts"),
                        "indexed_at": r.get::<chrono::DateTime<chrono::Utc>, _>("indexed_at"),
                    })
                })
                .collect();
            Json(serde_json::json!({ "data": results, "count": results.len() })).into_response()
        }
        Err(e) => {
            error!(error = %e, "Instruction query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn list_accounts(
    State(state): State<Arc<ApiState>>,
    Path(account_type): Path<String>,
    Query(q): Query<AccountQuery>,
) -> impl IntoResponse {
    let table = sanitize_name(&format!("{}_{}", state.program_name, account_type));
    if !state.account_types.contains(&account_type) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Unknown account type"})),
        )
            .into_response();
    }

    let limit = q.limit.unwrap_or(50).min(500);
    let offset = q.offset.unwrap_or(0);

    let mut sql = format!("SELECT * FROM {table} WHERE 1=1");
    let mut params: Vec<String> = Vec::new();
    let mut idx = 1;

    if let Some(ref pk) = q.pubkey {
        sql.push_str(&format!(" AND pubkey = ${idx}"));
        params.push(pk.clone());
        idx += 1;
    }
    if let Some(from) = q.slot_from {
        sql.push_str(&format!(" AND slot >= ${idx}"));
        params.push(from.to_string());
        idx += 1;
    }
    if let Some(to) = q.slot_to {
        sql.push_str(&format!(" AND slot <= ${idx}"));
        params.push(to.to_string());
        idx += 1;
    }
    sql.push_str(&format!(
        " ORDER BY slot DESC LIMIT ${idx} OFFSET ${}",
        idx + 1
    ));
    params.push(limit.to_string());
    params.push(offset.to_string());

    let mut query = sqlx::query(&sql);
    for p in &params {
        query = query.bind(p);
    }

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
            use sqlx::Row;
            let results: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "pubkey": r.get::<String, _>("pubkey"),
                        "slot": r.get::<i64, _>("slot"),
                        "transaction_signature": r.get::<Option<String>, _>("transaction_signature"),
                        "data": r.get::<Option<serde_json::Value>, _>("data"),
                        "indexed_at": r.get::<chrono::DateTime<chrono::Utc>, _>("indexed_at"),
                    })
                })
                .collect();
            Json(serde_json::json!({ "data": results, "count": results.len() })).into_response()
        }
        Err(e) => {
            error!(error = %e, "Account query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn stats(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM transactions")
        .fetch_one(&state.pool)
        .await
        .unwrap_or((0,));
    let success: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM transactions WHERE success = true")
            .fetch_one(&state.pool)
            .await
            .unwrap_or((0,));
    let total_ix: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM instructions")
        .fetch_one(&state.pool)
        .await
        .unwrap_or((0,));
    let latest_slot: Option<(i64,)> =
        sqlx::query_as("SELECT MAX(slot) FROM transactions")
            .fetch_optional(&state.pool)
            .await
            .unwrap_or(None);

    let breakdown: Vec<InstructionCount> = sqlx::query_as::<_, (String, i64)>(
        "SELECT instruction_name, COUNT(*) as cnt
         FROM instructions GROUP BY instruction_name ORDER BY cnt DESC",
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|(name, count)| InstructionCount { name, count })
    .collect();

    Json(StatsResponse {
        total_transactions: total.0,
        successful_transactions: success.0,
        failed_transactions: total.0 - success.0,
        total_instructions: total_ix.0,
        instruction_breakdown: breakdown,
        latest_slot: latest_slot.map(|s| s.0),
        account_types: state.account_types.clone(),
    })
}

async fn instruction_aggregation(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<AggQuery>,
) -> impl IntoResponse {
    let trunc = match q.period.as_deref() {
        Some("day") => "day",
        _ => "hour",
    };

    let mut sql = format!(
        "SELECT date_trunc('{trunc}', i.indexed_at) as period,
                i.instruction_name,
                COUNT(*) as cnt
         FROM instructions i WHERE 1=1"
    );
    let mut params: Vec<String> = Vec::new();
    let mut idx = 1;

    if let Some(ref name) = q.name {
        sql.push_str(&format!(" AND i.instruction_name = ${idx}"));
        params.push(name.clone());
        idx += 1;
    }
    if let Some(ref from) = q.from_time {
        sql.push_str(&format!(" AND i.indexed_at >= ${idx}::timestamptz"));
        params.push(from.clone());
        idx += 1;
    }
    if let Some(ref to) = q.to_time {
        sql.push_str(&format!(" AND i.indexed_at <= ${idx}::timestamptz"));
        params.push(to.clone());
        idx += 1;
    }
    sql.push_str(&format!(
        " GROUP BY period, i.instruction_name ORDER BY period DESC LIMIT 1000"
    ));

    let mut query = sqlx::query(&sql);
    for p in &params {
        query = query.bind(p);
    }

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
            use sqlx::Row;
            let results: Vec<AggBucket> = rows
                .iter()
                .map(|r| AggBucket {
                    period: r
                        .get::<chrono::DateTime<chrono::Utc>, _>("period")
                        .to_rfc3339(),
                    instruction_name: r.get::<String, _>("instruction_name"),
                    count: r.get::<i64, _>("cnt"),
                })
                .collect();
            Json(serde_json::json!({ "data": results })).into_response()
        }
        Err(e) => {
            error!(error = %e, "Aggregation query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
