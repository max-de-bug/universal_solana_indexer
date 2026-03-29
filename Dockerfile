# --- Build stage ---
FROM rust:1.77-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
# Create a dummy main to cache deps
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true

COPY src/ src/
RUN cargo build --release

# --- Runtime stage ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/universal-solana-indexer /usr/local/bin/indexer

ENTRYPOINT ["indexer"]
