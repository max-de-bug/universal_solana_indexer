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

---

## 🚀 Quickstart

### 1. Prerequisites
- **Rust** (`cargo build --release`)
- **PostgreSQL Database**
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
```

### 3. Run the Indexer
```bash
cargo build --release
./target/release/universal-solana-indexer
```

---

## 📊 Advanced Analytics API

The Indexer ships with a bundled Axum-powered HTTP API designed for maximum query velocity and granular data extraction.

- **`GET /api/v1/transactions`** Filter parsed transactions by `signature`, `success`, `slot_from`, and `slot_to`.
- **`GET /api/v1/instructions`** Extract dynamic table rows securely filtered by `instruction_name` or `program_id`.
- **`GET /api/v1/accounts/:account_type`** Read explicit IDL account shapes (e.g. `/api/v1/accounts/UserProfile`) mapped sequentially against their historical block allocations.
- **`GET /api/v1/stats/instructions`** View highly-calibrated time aggregations (grouped natively by POSTGRES `date_trunc('hour')`) to monitor usage volume directly dynamically natively.

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
