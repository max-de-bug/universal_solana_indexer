use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
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
// Typed dynamic query builder — avoids the String-bind-everything anti-pattern
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum Param {
    String(String),
    I64(i64),
    Bool(bool),
}

/// Accumulates SQL fragments and heterogeneous parameter values.
struct QueryBuilder {
    sql: String,
    param_idx: u32,
    params: Vec<Param>,
}

impl QueryBuilder {
    fn new(base: &str) -> Self {
        Self {
            sql: base.to_string(),
            param_idx: 1,
            params: Vec::new(),
        }
    }

    fn push_str(&mut self, fragment: &str) {
        self.sql.push_str(fragment);
    }

    fn bind_string(&mut self, clause: &str, value: String) {
        self.sql.push_str(&clause.replace("{}", &format!("${}", self.param_idx)));
        self.param_idx += 1;
        self.params.push(Param::String(value));
    }

    fn bind_i64(&mut self, clause: &str, value: i64) {
        self.sql.push_str(&clause.replace("{}", &format!("${}", self.param_idx)));
        self.param_idx += 1;
        self.params.push(Param::I64(value));
    }

    fn bind_bool(&mut self, clause: &str, value: bool) {
        self.sql.push_str(&clause.replace("{}", &format!("${}", self.param_idx)));
        self.param_idx += 1;
        self.params.push(Param::Bool(value));
    }

    fn build_count(&self) -> (String, Vec<Param>) {
        let count_sql = if let Some(from_idx) = self.sql.find(" FROM ") {
            format!("SELECT COUNT(*) {}", &self.sql[from_idx..])
        } else {
            self.sql.clone()
        };
        (count_sql, self.params.clone())
    }

    fn finish_with_pagination(mut self, limit: i64, offset: i64, order: &str) -> (String, Vec<Param>) {
        self.sql.push_str(&format!(
            " ORDER BY {order} LIMIT ${} OFFSET ${}",
            self.param_idx,
            self.param_idx + 1
        ));
        self.params.push(Param::I64(limit));
        self.params.push(Param::I64(offset));
        (self.sql, self.params)
    }

    fn finish(self) -> (String, Vec<Param>) {
        (self.sql, self.params)
    }
}

fn apply_params<'a>(
    mut query: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: Vec<Param>,
) -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for param in params {
        query = match param {
            Param::String(s) => query.bind(s),
            Param::I64(i) => query.bind(i),
            Param::Bool(b) => query.bind(b),
        };
    }
    query
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

    let mut qb = QueryBuilder::new(
        "SELECT signature, slot, block_time, success, fee, err_msg, indexed_at
         FROM transactions WHERE 1=1",
    );

    if let Some(sig) = q.signature {
        qb.bind_string(" AND signature = {}", sig);
    }
    if let Some(from) = q.slot_from {
        qb.bind_i64(" AND slot >= {}", from);
    }
    if let Some(to) = q.slot_to {
        qb.bind_i64(" AND slot <= {}", to);
    }
    if let Some(success) = q.success {
        qb.bind_bool(" AND success = {}", success);
    }

    let (count_sql, count_params) = qb.build_count();
    let total_count: i64 = apply_params(sqlx::query_scalar(&count_sql), count_params)
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);

    let (sql, params) = qb.finish_with_pagination(limit, offset, "slot DESC");
    let query = apply_params(sqlx::query(&sql), params);

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
            let results: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
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
            Json(serde_json::json!({ 
                "data": results,
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": offset + limit < total_count
            })).into_response()
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

    let mut qb = QueryBuilder::new(
        "SELECT id, transaction_signature, instruction_index, instruction_name, program_id, args, accounts, indexed_at
         FROM instructions WHERE 1=1",
    );

    if let Some(name) = q.name {
        qb.bind_string(" AND instruction_name = {}", name);
    }
    if let Some(tx) = q.transaction_signature {
        qb.bind_string(" AND transaction_signature = {}", tx);
    }
    if let Some(pid) = q.program_id {
        qb.bind_string(" AND program_id = {}", pid);
    }

    let (count_sql, count_params) = qb.build_count();
    let total_count: i64 = apply_params(sqlx::query_scalar(&count_sql), count_params)
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);

    let (sql, params) = qb.finish_with_pagination(limit, offset, "id DESC");
    let query = apply_params(sqlx::query(&sql), params);

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
            let results: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
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
            Json(serde_json::json!({ 
                "data": results,
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": offset + limit < total_count
            })).into_response()
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
    if !state.account_types.contains(&account_type) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Unknown account type"})),
        )
            .into_response();
    }

    let table = sanitize_name(&format!("{}_{}", state.program_name, account_type));
    let limit = q.limit.unwrap_or(50).min(500);
    let offset = q.offset.unwrap_or(0);

    let mut qb = QueryBuilder::new(&format!(
        "SELECT pubkey, slot, transaction_signature, data, indexed_at FROM \"{}\" WHERE 1=1",
        table
    ));

    if let Some(pk) = q.pubkey {
        qb.bind_string(" AND pubkey = {}", pk);
    }
    if let Some(from) = q.slot_from {
        qb.bind_i64(" AND slot >= {}", from);
    }
    if let Some(to) = q.slot_to {
        qb.bind_i64(" AND slot <= {}", to);
    }

    let (count_sql, count_params) = qb.build_count();
    let total_count: i64 = apply_params(sqlx::query_scalar(&count_sql), count_params)
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);

    let (sql, params) = qb.finish_with_pagination(limit, offset, "slot DESC");
    let query = apply_params(sqlx::query(&sql), params);

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
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
            Json(serde_json::json!({ 
                "data": results,
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": offset + limit < total_count
            })).into_response()
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

    let mut qb = QueryBuilder::new(&format!(
        "SELECT date_trunc('{trunc}', i.indexed_at) as period,
                i.instruction_name,
                COUNT(*) as cnt
         FROM instructions i WHERE 1=1"
    ));

    if let Some(name) = q.name {
        qb.bind_string(" AND i.instruction_name = {}", name);
    }
    if let Some(from) = q.from_time {
        qb.bind_string(" AND i.indexed_at >= {}::timestamptz", from);
    }
    if let Some(to) = q.to_time {
        qb.bind_string(" AND i.indexed_at <= {}::timestamptz", to);
    }

    qb.push_str(" GROUP BY period, i.instruction_name ORDER BY period DESC LIMIT 1000");

    let (sql, params) = qb.finish();
    let query = apply_params(sqlx::query(&sql), params);

    match query.fetch_all(&state.pool).await {
        Ok(rows) => {
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
