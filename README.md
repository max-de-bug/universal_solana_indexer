# Universal Solana Indexer

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

## 🚀 Quickstart

### 1. Prerequisites
- **Rust** 1.82+ (`cargo build --release`)
- **PostgreSQL 14+** (or Docker)
- **Solana API Endpoints** (Both HTTP and WebSocket are supported natively)

### 2. Configure `.env`
```env
# Multi-node failover pool separated by commas
RPC_URLS=https://api.mainnet-beta.solana.com,https://api.mainnet.rpcpool.com

# High-velocity instantaneous stream tunnel
WSS_URL=wss://api.mainnet-beta.solana.com

DATABASE_URL=postgres://user:password@localhost/db

# The target smart contract 
PROGRAM_ID=your_program_pubkey

# Anchor smart contract interface architecture
# (Use IDL_ACCOUNT to dynamically uncompress the schema from on-chain)
IDL_PATH=./target/idl/my_program.json
# IDL_ACCOUNT=your_idl_pubkey

# Indexing mode: "realtime" (default), "batch", "batch_signatures"
INDEXING_MODE=realtime

# Tuning
BATCH_SIZE=100
BATCH_CONCURRENCY=5
DB_MAX_CONNECTIONS=10
DB_MIN_CONNECTIONS=2
```

### 3. Run the Indexer
```bash
cargo build --release
./target/release/universal-solana-indexer
```

---

## 🐳 Docker Deployment

The simplest way to deploy is with Docker Compose, which starts both PostgreSQL and the indexer:

```bash
# 1. Copy and edit .env.example
cp .env.example .env
# Edit .env with your PROGRAM_ID, RPC_URLS, IDL_PATH...

# 2. Place your IDL
cp my_program.json idl.json

# 3. Launch
docker compose up -d

# 4. Check logs
docker compose logs -f indexer

# 5. Verify health
curl http://localhost:3000/api/v1/health
```

The indexer container runs as a non-root user and includes a built-in healthcheck.

---

## 📊 REST API

The Indexer ships with a bundled Axum-powered HTTP API designed for maximum query velocity and granular data extraction.

| Endpoint | Description |
|---|---|
| `GET /api/v1/health` | JSON health status with DB connectivity, uptime, and program ID |
| `GET /api/v1/metrics` | Prometheus-compatible metrics (transactions, instructions, slot, pool) |
| `GET /api/v1/transactions` | Filter parsed transactions by `signature`, `success`, `slot_from`, `slot_to` |
| `GET /api/v1/instructions` | Extract instructions filtered by `instruction_name` or `program_id` |
| `GET /api/v1/accounts/:type` | Read IDL account shapes (e.g. `/accounts/UserProfile`) with slot history |
| `GET /api/v1/stats` | Aggregate stats: total transactions, instruction breakdown, latest slot |
| `GET /api/v1/stats/instructions` | Time-series aggregation `date_trunc('hour')` by instruction name |

All list endpoints support `limit`, `offset`, `slot_from`, `slot_to` query parameters with paginated responses:
```json
{
  "data": [...],
  "total": 1234,
  "limit": 50,
  "offset": 0,
  "has_next": true
}
```

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

## ⚙️ Production Tuning

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

## 🛠 Architecture Layout

```text
src/
├── main.rs          # Startup, Dependency Injection, and IDL Parsing Loop
├── config.rs        # Configuration matrix supporting Array ingestion
├── db.rs            # The heart of the DDL schema-builder and jsonb type coercions
├── idl.rs           # Converts raw Anchor blobs into strict Rust enums & traits
├── api.rs           # The Axum high-performance stateless Analytics engine
└── indexer/
    ├── mod.rs       # The WSS WebSocket processor and Account State background poller 
    ├── fetcher.rs   # The Enterprise Load Balancer logic and exponential backoff retry macros
    └── decoder.rs   # Mathematical computation of sha256 8-byte discriminator hashes
```

> **Note on Windows MSVC Compilation:** The native bundle utilizes a locked `vcpkg` OpenSSL patch internally to gracefully compile `.rlib` linkages across Windows builds securely.
