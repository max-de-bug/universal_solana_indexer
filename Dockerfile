# --- Build stage ---
FROM rust:1.82-bookworm AS builder

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

# Create non-root user for security
RUN groupadd -r indexer && useradd -r -g indexer indexer

COPY --from=builder /app/target/release/universal-solana-indexer /usr/local/bin/indexer

USER indexer

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:3000/api/v1/health || exit 1

ENTRYPOINT ["indexer"]
