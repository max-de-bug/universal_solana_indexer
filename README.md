# Universal Solana Indexer

![Universal Solana Indexer Demo](./gif_test/universal_solana_indexer-ezgif.com-video-to-gif-converter.gif)
An ultra-fast, production-grade, and entirely dynamic Solana program indexer written in Rust.

Unlike static indexers that require manual data-struct modeling for every new smart contract you interact with, the Universal Indexer reads **Anchor IDLs natively**, computes the 8-byte discriminators dynamically, and algorithmically generates and mutates strongly-typed **PostgreSQL schemas** seamlessly upon boot. 

It handles automated mapping of complex `serde_json` blobs into strict SQL types utilizing advanced Postgres `jsonb_populate_record` casting, removing all SQL boilerplate from your development pipeline entirely.

---

## ⚡️ Enterprise Features

- **Automated DDL (Data Definition Language) Schemas:** Connect any Anchor `.json` IDL. The engine reads it, maps `u64`, `u8`, `Pubkey`, and complex Structs into SQL types, and builds your `CREATE TABLE` and `CREATE INDEX` queries organically on startup.
- **WebSocket `PubsubClient` Streaming:** Outperforms traditional 500ms RPC polling architectures by utilizing non-blocking WSS `logsSubscribe` streams. Transactions are natively pushed into the engine directly from the blockchain at near-0 network latency.
- **Advanced Dynamic DML:** Arbitrary transaction instruction arguments and Account State mutations are extracted, decoded against your IDL discriminator, and recursively dumped entirely into explicit PostgreSQL columns intelligently.
- **Global Account State Memory Polling:** An independent background `tokio::spawn` daemon continuously polls `getProgramAccounts`, syncing global program account states directly into your database independently of transaction-mutations (capturing completely static or pre-indexer accounts).
- **V2 EMA Load Balancing:** An integrated `parking_lot::RwLock` cluster evaluates RPC endpoint heal-states, tracking successful API rates, Exponential Moving Average (EMA) latency ms, and slot-lags to mathematically route execution logic away from unhealthy or rate-limited nodes.
- **Prometheus Metrics:** Built-in `/api/v1/metrics` endpoint exposes transaction counts, instruction counts, latest slot, and DB pool stats in Prometheus-compatible text format.

---

## 🏗 Architectural Overview

### Data Flow

```text
                      ┌──────────────────────────────────────┐
                      │          Anchor IDL (.json)           │
                      │  (local file  OR  on-chain account)  │
                      └──────────────┬───────────────────────┘
                                     │  parse at boot
                                     ▼
┌─────────────────┐          ┌───────────────┐         ┌─────────────────────┐
│  Solana RPC(s)  │◄────────►│    Fetcher    │────────►│   PostgreSQL (DDL)  │
│  (multi-node)   │  retry   │  EMA-scored   │ schema  │  dynamic tables     │
└─────┬───────────┘  backoff │  load balancer│ init    │  per account & ix   │
      │                      └───────┬───────┘         └──────────┬──────────┘
      │                              │                            │
      │  ┌───────────────────────────┤                            │
      │  │  Three indexing modes:    │                            │
      │  │                           │                            │
      │  │  1. Realtime (WSS)        │                            │
      │  │  2. Batch (slot range)    │                            │
      │  │  3. Batch (signatures)    │                            │
      │  └───────────────────────────┤                            │
      │                              ▼                            │
      │                     ┌────────────────┐                    │
      │                     │    Decoder     │                    │
      │                     │  discriminator │                    │
      │                     │  + Borsh parse │                    │
      │                     └───────┬────────┘                    │
      │                             │ decoded JSON                │
      │                             ▼                             ▼
      │                     ┌────────────────────────────────────────┐
      │                     │        DB Writer (atomic txns)         │
      │                     │  insert_transaction + insert_instruction│
      │                     │  + insert_dynamic_instruction           │
      │                     │  + insert_account_state                 │
      │                     └────────────────────────────────────────┘
      │                                        │
      │  ┌─────────────────────┐               │
      └──┤ Account Poller      │               │
         │ (background daemon) ├───────────────┘
         │ getProgramAccounts  │
         │ every 30s           │
         └─────────────────────┘
                                       ┌────────────────────────┐
                                       │   Axum REST API        │
                                       │  /health /metrics      │
                                       │  /transactions         │
                                       │  /instructions         │
                                       │  /accounts/:type       │
                                       │  /stats                │
                                       └────────────────────────┘
```

### Module Map

```text
src/
├── main.rs            Startup, dependency injection, IDL loading, graceful shutdown
├── config.rs          Env-based configuration with three IndexingMode variants
├── db.rs              Dynamic DDL schema builder, jsonb type coercions, all INSERT logic
├── idl.rs             Anchor IDL parser (v0.29 & v0.30+), discriminator computation
├── api.rs             Axum REST API: health, metrics, paginated queries, stats
├── error.rs           Unified IndexerError enum
└── indexer/
    ├── mod.rs          Mode dispatcher, WSS processor, backfill loop, account poller
    ├── fetcher.rs      Multi-node RPC client with EMA scoring and retry macro
    └── decoder.rs      Runtime Borsh decoder, discriminator matching, BorshReader cursor
```

### Key Subsystems

| Subsystem | Responsibility | Key Types |
|---|---|---|
| **IDL Parser** | Reads Anchor IDL (file or on-chain), computes SHA-256 discriminators, resolves nested type references | `AnchorIdl`, `IdlInstruction`, `IdlType` |
| **Schema Builder** | Generates `CREATE TABLE` + `CREATE INDEX` DDL dynamically from IDL definitions at startup | `initialize_schema`, `create_account_table` |
| **Fetcher** | Multi-RPC load balancer with EMA latency tracking, success-rate scoring, and exponential backoff retries | `Fetcher`, `NodeMetrics`, `retry_rpc!` macro |
| **Decoder** | Matches 8-byte discriminators, then walks raw Borsh bytes to produce `serde_json::Value` using the IDL type tree | `BorshReader`, `decode_fields`, `decode_type` |
| **Indexer** | Orchestrates realtime/batch modes, backfill-on-reconnect, concurrent signature processing | `IndexerState`, `run_realtime`, `backfill` |
| **Account Poller** | Background daemon calling `getProgramAccounts` every 30s to capture pre-existing state | `run_account_poller` |
| **API** | Stateless Axum HTTP server with typed query builders, Prometheus metrics, health checks | `ApiState`, `QueryBuilder` |

---

## 🚀 Setup and Running Instructions

### Prerequisites
- **Rust** 1.82+ (`rustup update stable`)
- **PostgreSQL 14+** (or Docker)
- **Solana API Endpoints** (both HTTP and WebSocket)

### Option A: Docker Compose (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/your-org/universal-solana-indexer.git
cd universal-solana-indexer

# 2. Configure environment
cp .env.example .env
# Edit .env — set PROGRAM_ID, RPC_URLS, and IDL source

# 3. Place your IDL
cp path/to/my_program.json idl.json

# 4. Launch PostgreSQL + Indexer
docker compose up -d

# 5. Verify
docker compose logs -f indexer
curl http://localhost:3000/api/v1/health
```

### Option B: Native Build

```bash
# 1. Start a PostgreSQL instance (Docker one-liner)
docker run -d --name pg -e POSTGRES_USER=indexer \
  -e POSTGRES_PASSWORD=indexer -e POSTGRES_DB=solana_indexer \
  -p 5432:5432 postgres:16-alpine

# 2. Configure .env
cp .env.example .env
# Ensure DATABASE_URL=postgres://indexer:indexer@localhost:5432/solana_indexer

# 3. Build and run
cargo build --release
./target/release/universal-solana-indexer
```

### `.env` Configuration Reference

```env
# Multi-node failover pool (comma-separated)
RPC_URLS=https://api.mainnet-beta.solana.com,https://api.mainnet.rpcpool.com

# Real-time WebSocket stream
WSS_URL=wss://api.mainnet-beta.solana.com

DATABASE_URL=postgres://user:password@localhost/db

# Target smart contract
PROGRAM_ID=your_program_pubkey

# IDL source (at least one must be set)
IDL_PATH=./idl.json
# IDL_ACCOUNT=your_idl_pubkey   # Alternative: fetch from on-chain

# Indexing mode: "realtime" (default), "batch", "batch_signatures"
INDEXING_MODE=realtime

# Tuning
BATCH_SIZE=100
BATCH_CONCURRENCY=5
DB_MAX_CONNECTIONS=10
DB_MIN_CONNECTIONS=2
API_PORT=3000
MAX_RETRIES=5
INITIAL_RETRY_DELAY_MS=500
```

### Production Tuning

| Parameter | Default | Description |
|---|---|---|
| `RPC_URLS` | mainnet | Comma-separated RPC endpoints for load-balanced failover |
| `BATCH_CONCURRENCY` | 5 | Parallel transaction processing during batch/backfill |
| `DB_MAX_CONNECTIONS` | 10 | Maximum PostgreSQL connections in pool |
| `DB_MIN_CONNECTIONS` | 2 | Minimum idle connections maintained |
| `BATCH_SIZE` | 100 | Signatures fetched per RPC page |
| `MAX_RETRIES` | 5 | RPC retry attempts before failure |
| `INITIAL_RETRY_DELAY_MS` | 500 | Starting delay between retries (exponential backoff) |

---

## 📊 API Query Examples

The indexer ships with a bundled Axum-powered REST API on port 3000.

### Health Check

```bash
curl http://localhost:3000/api/v1/health
```
```json
{
  "status": "healthy",
  "database_connected": true,
  "program_id": "whir7mCuzS8B74J9YtuZ3r1G1X849mQWNDvL819T78f",
  "uptime_seconds": 3600
}
```

### Prometheus Metrics

```bash
curl http://localhost:3000/api/v1/metrics
```
```text
indexer_transactions_total 15234
indexer_instructions_total 42567
indexer_latest_slot 330012345
indexer_db_pool_size 10
indexer_db_pool_idle 8
```

### List Transactions

```bash
# Latest 20 transactions
curl "http://localhost:3000/api/v1/transactions?limit=20"

# Filter by slot range
curl "http://localhost:3000/api/v1/transactions?slot_from=330000000&slot_to=330000100&limit=50"

# Filter by success status
curl "http://localhost:3000/api/v1/transactions?success=true&limit=10"

# Lookup a specific transaction
curl "http://localhost:3000/api/v1/transactions?signature=5UfD...xyz"
```
```json
{
  "data": [
    {
      "signature": "5UfDk...",
      "slot": 330000042,
      "block_time": "2026-04-06T12:00:00Z",
      "success": true,
      "fee": 5000,
      "indexed_at": "2026-04-06T12:00:01Z"
    }
  ],
  "total": 1234,
  "limit": 20,
  "offset": 0,
  "has_next": true
}
```

### List Instructions

```bash
# All instructions for a specific instruction type
curl "http://localhost:3000/api/v1/instructions?instruction_name=initialize&limit=10"

# Paginate through results
curl "http://localhost:3000/api/v1/instructions?limit=50&offset=100"
```

### Query Account State

```bash
# Fetch all accounts of a specific type (e.g., UserProfile)
curl "http://localhost:3000/api/v1/accounts/UserProfile?limit=25"

# With slot range filter
curl "http://localhost:3000/api/v1/accounts/UserProfile?slot_from=330000000&slot_to=330001000"
```

### Aggregate Stats

```bash
# Overall statistics
curl http://localhost:3000/api/v1/stats
```
```json
{
  "total_transactions": 15234,
  "total_instructions": 42567,
  "latest_slot": 330012345,
  "instruction_counts": [
    { "name": "initialize", "count": 1200 },
    { "name": "transfer", "count": 8400 }
  ]
}
```

```bash
# Hourly time-series breakdown by instruction name
curl http://localhost:3000/api/v1/stats/instructions
```
```json
[
  {
    "bucket": "2026-04-06T12:00:00Z",
    "instruction_name": "initialize",
    "count": 42
  }
]
```

### Endpoint Summary

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/health` | GET | JSON health status with DB connectivity, uptime, and program ID |
| `/api/v1/metrics` | GET | Prometheus-compatible metrics (transactions, instructions, slot, pool) |
| `/api/v1/transactions` | GET | Filter parsed transactions by `signature`, `success`, `slot_from`, `slot_to` |
| `/api/v1/instructions` | GET | Extract instructions filtered by `instruction_name` or `program_id` |
| `/api/v1/accounts/:type` | GET | Read IDL account shapes (e.g. `/accounts/UserProfile`) with slot history |
| `/api/v1/stats` | GET | Aggregate stats: total transactions, instruction breakdown, latest slot |
| `/api/v1/stats/instructions` | GET | Time-series aggregation `date_trunc('hour')` by instruction name |

All list endpoints support `limit`, `offset`, `slot_from`, `slot_to` query parameters.

---

## ⚖️ Key Architectural Decisions and Trade-offs

### 1. Runtime IDL Parsing vs. Compile-Time Code Generation

**Decision:** Parse the Anchor IDL at runtime and derive all schemas, discriminators, and decoders dynamically — rather than using a build-step code generator.

**Why:** A single compiled binary can index *any* Anchor program without recompilation. Swapping programs is as simple as changing `PROGRAM_ID` and `IDL_PATH` in your `.env` file.

**Trade-off:** We lose compile-time type safety for decoded data. All decoded instruction args and account fields flow through `serde_json::Value` before reaching Postgres. This adds a small serialization overhead and means decoding errors are caught at runtime rather than compile time.

### 2. `JSONB` Fallback for Complex Nested Types

**Decision:** IDL fields that are simple primitives (`u64`, `bool`, `Pubkey`) get their own strongly-typed Postgres columns. Deeply nested or variable-length types (`Vec<Option<SomeStruct>>`) are stored in a `JSONB data` column.

**Why:** Generating arbitrary-depth recursive SQL DDL for every possible Anchor type composition is fragile and produces schemas that are hard to query. `JSONB` provides full data capture with flexible querying via Postgres JSON operators.

**Trade-off:** `JSONB` columns can't be indexed as efficiently as native columns. Queries filtering on deeply nested fields inside `data` will be slower than columnar lookups. For most indexing use cases (analytics, dashboards, event replay), this is acceptable.

### 3. `jsonb_populate_record` for Dynamic Inserts

**Decision:** Instead of generating SQL with N positional parameters for each dynamic table, we pass the entire decoded JSON as a single `$4::jsonb` bind and use Postgres's `jsonb_populate_record(NULL::"table", $4::jsonb)` to extract columns.

**Why:** This eliminates the need to dynamically construct parameterized SQL at runtime — the SQL template is stable regardless of how many columns a table has. Postgres handles the type casting internally.

**Trade-off:** Relies on Postgres-specific functionality (not portable to other databases). Also requires that the JSON keys exactly match the sanitized column names, which our `sanitize_name` function enforces.

### 4. EMA-Based RPC Load Balancing (not Round-Robin)

**Decision:** The `Fetcher` scores each RPC node using `(success_rate / ema_latency) × lag_penalty` and routes requests to the highest-scoring node, rather than simple round-robin or random selection.

**Why:** Public Solana RPC endpoints have heterogeneous reliability. A node might be up but 5 slots behind, or responding slowly due to rate limiting. EMA smoothing prevents a single spike from permanently penalizing a node, while slot-lag detection avoids stale data.

**Trade-off:** We use `parking_lot::RwLock` (blocking) instead of `tokio::RwLock` (async) for the metrics. This is intentional — the lock is held for nanoseconds (incrementing counters), so the blocking cost is negligible, and we avoid the overhead of async lock machinery on every single RPC call.

### 5. Backfill-on-Reconnect (not Backfill-Once)

**Decision:** Every time the WebSocket stream reconnects, the indexer runs a full backfill pass from its last checkpoint before resuming live streaming.

**Why:** WebSocket connections to Solana validators drop regularly (network instability, node restarts, rate limits). Without re-backfill, every disconnection creates a gap in the indexed data. By always backfilling from the last stored signature, we guarantee zero data loss.

**Trade-off:** This means some transactions may be fetched twice (once during backfill, once via WSS). We mitigate this with a deduplication check (`transaction_exists`) before processing, and all inserts use `ON CONFLICT DO NOTHING`.

### 6. Background Account Poller (Separate from Transaction Indexing)

**Decision:** A dedicated `tokio::spawn` daemon runs independently, calling `getProgramAccounts` every 30 seconds and upserting all account states into the database.

**Why:** Transaction-based indexing only captures accounts that are *modified* after the indexer starts. Pre-existing accounts (created before the indexer was deployed) would never appear. The poller ensures complete state capture.

**Trade-off:** `getProgramAccounts` is one of the most expensive RPC calls and can be rate-limited aggressively. The 30-second interval is a deliberate compromise between data freshness and RPC budget. For programs with thousands of accounts, this call can also be slow.

### 7. Atomic Transaction Writes

**Decision:** All database writes for a single Solana transaction (the transaction row, its instructions, dynamic instruction tables, and account state) are wrapped in a single `sqlx::Transaction` and committed atomically.

**Why:** Partial writes would leave the database in an inconsistent state — e.g., an instruction row referencing a transaction that doesn't exist. Atomic commits ensure all-or-nothing semantics.

**Trade-off:** Holding a database transaction open during instruction decoding increases lock contention under high throughput. We mitigate this by keeping the critical section narrow (decode first, then batch-insert) and using a properly sized connection pool.

### 8. Graceful Shutdown via CancellationToken

**Decision:** A single `tokio_util::CancellationToken` is shared across all async tasks (API server, indexer, account poller). `Ctrl+C` triggers cancellation, and each task checks `is_cancelled()` at its natural yield points.

**Why:** Hard kills can leave database transactions uncommitted, WebSocket subscriptions dangling, and connection pools dirty. Cooperative cancellation lets each task finish its current unit of work and clean up resources.

---

## 📈 Monitoring

### Prometheus

Scrape `http://indexer:3000/api/v1/metrics` to collect:

```
indexer_transactions_total 15234
indexer_instructions_total 42567
indexer_latest_slot 330012345
indexer_db_pool_size 10
indexer_db_pool_idle 8
```

### Health Checks

```bash
curl http://localhost:3000/api/v1/health
# {"status":"healthy","database_connected":true,"program_id":"...","uptime_seconds":3600}
```

---

> **Note on Windows MSVC Compilation:** The native bundle utilizes a locked `vcpkg` OpenSSL patch internally to gracefully compile `.rlib` linkages across Windows builds securely.
