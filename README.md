# Universal Solana Indexer

A production-ready, universal Solana indexer that automatically adapts to **any** Anchor IDL. Drop in your program's IDL JSON and the indexer dynamically creates database tables, decodes instructions and account states, and exposes a rich REST API — all without writing program-specific code.

---

## Architecture

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Solana RPC   │◄───►│   Indexer Core   │────►│  PostgreSQL  │
│  (+ WSS)      │     │ Fetcher/Decoder  │     │  (dynamic)   │
└──────────────┘     └────────┬────────┘     └──────┬───────┘
                              │                      │
                     ┌────────▼────────┐     ┌──────▼───────┐
                     │   IDL Parser     │     │   REST API    │
                     │ (schema gen)     │     │   (Axum)      │
                     └─────────────────┘     └──────────────┘
```

### Key Components

| Module | Responsibility |
|---|---|
| `config.rs` | Environment-based configuration with validation |
| `idl.rs` | Anchor IDL parsing, discriminator computation, SQL type mapping |
| `db.rs` | Dynamic schema generation, sync-state tracking, data insertion |
| `indexer/fetcher.rs` | Solana RPC wrapper with exponential backoff & retries |
| `indexer/decoder.rs` | Runtime Borsh decoder — walks bytes according to IDL types |
| `indexer/mod.rs` | Orchestration: batch (slots/sigs), real-time with cold-start |
| `api.rs` | REST endpoints: filtering, aggregation, statistics |

### Design Decisions

- **Dynamic Borsh decoding** — no compile-time Rust types needed; the decoder reads raw bytes guided purely by the IDL type graph.
- **JSONB-first account storage** — decoded account data is stored as JSONB for maximal flexibility; typed columns are generated in the schema for documentation and potential future indexed queries.
- **Polling over WebSocket** — `getSignaturesForAddress` polling is more reliable than `logsSubscribe` for catches missed messages. Polling naturally doubles as the cold-start backfill mechanism.
- **CancellationToken** for graceful shutdown — both the indexer loop and API server respond to `Ctrl+C` / SIGTERM and drain cleanly.

---

## Setup

### Prerequisites

- Docker & Docker Compose
- Your Anchor program's IDL (`idl.json`)
- A Solana RPC endpoint (Helius, Triton, or public RPC)

### 1. Configure

```bash
cp .env.example .env
# Edit .env with your PROGRAM_ID, RPC_URL, and IDL_PATH
```

### 2. Place your IDL

Copy your Anchor IDL JSON to `idl.json` in the project root (or set `IDL_PATH` in `.env`).

### 3. Launch

```bash
docker compose up -d
```

This starts PostgreSQL and the indexer. The indexer will:
1. Parse the IDL and auto-generate all database tables
2. Backfill historical transactions (cold start)
3. Begin real-time polling for new transactions
4. Serve the API on port `3000`

### Local Development (without Docker)

```bash
# Start a local Postgres
docker compose up postgres -d

# Run the indexer directly
cargo run --release
```

---

## API Reference

### Health Check
```
GET /api/v1/health
→ "OK"
```

### Transactions
```
GET /api/v1/transactions?signature=...&slot_from=...&slot_to=...&success=true&limit=50&offset=0
```

### Instructions
```
GET /api/v1/instructions?name=initialize&transaction_signature=...&limit=50&offset=0
```

### Account States
```
GET /api/v1/accounts/{account_type}?pubkey=...&slot_from=...&slot_to=...&limit=50
```

### Program Statistics
```
GET /api/v1/stats
```
```json
{
  "total_transactions": 12450,
  "successful_transactions": 12300,
  "failed_transactions": 150,
  "total_instructions": 24800,
  "instruction_breakdown": [
    { "name": "initialize", "count": 5000 },
    { "name": "transfer", "count": 19800 }
  ],
  "latest_slot": 285000000,
  "account_types": ["UserAccount", "VaultState"]
}
```

### Instruction Aggregation
```
GET /api/v1/stats/instructions?name=transfer&period=hour&from_time=2024-01-01T00:00:00Z
```
```json
{
  "data": [
    { "period": "2024-01-15T14:00:00Z", "instruction_name": "transfer", "count": 42 }
  ]
}
```

---

## Indexing Modes

| Mode | Env Vars | Description |
|---|---|---|
| **Real-time** | `INDEXING_MODE=realtime` (default) | Cold-start backfill + continuous polling |
| **Batch (slots)** | `INDEXING_MODE=batch`, `BATCH_START_SLOT`, `BATCH_END_SLOT` | Index a specific slot range, then exit |
| **Batch (signatures)** | `INDEXING_MODE=batch_signatures`, `BATCH_SIGNATURES=sig1,sig2` | Index specific transactions, then exit |

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `RPC_URL` | `https://api.mainnet-beta.solana.com` | Solana JSON-RPC endpoint |
| `WSS_URL` | `wss://api.mainnet-beta.solana.com` | WebSocket endpoint |
| `DATABASE_URL` | *required* | PostgreSQL connection string |
| `PROGRAM_ID` | *required* | Program public key to index |
| `IDL_PATH` | *required* | Path to Anchor IDL JSON file |
| `INDEXING_MODE` | `realtime` | `realtime`, `batch`, or `batch_signatures` |
| `API_PORT` | `3000` | REST API listen port |
| `BATCH_SIZE` | `100` | Signatures per RPC page |
| `MAX_RETRIES` | `5` | Max RPC retry attempts |
| `INITIAL_RETRY_DELAY_MS` | `500` | Starting backoff delay |
| `POLL_INTERVAL_MS` | `2000` | Polling interval in real-time mode |
| `RUST_LOG` | `info` | Log filtering |

---

## Project Structure

```
universal_solana_indexer/
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── README.md
└── src/
    ├── main.rs             # Entrypoint, orchestration, graceful shutdown
    ├── config.rs           # Env-based configuration
    ├── error.rs            # Unified error types
    ├── idl.rs              # Anchor IDL data model & SQL mapping
    ├── db.rs               # Dynamic schema generation & data access
    ├── api.rs              # REST API (Axum)
    └── indexer/
        ├── mod.rs           # Batch / real-time / cold-start orchestration
        ├── fetcher.rs       # RPC client with exponential backoff
        └── decoder.rs       # Dynamic Borsh decoder
```

---

## License

MIT
