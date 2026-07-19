# Tokimeki Quant Engine - Rust Implementation

High-performance gRPC quantitative engine in Rust:

- **MonteCarloVarService** — parallel GBM portfolio VaR/CVaR (rayon)
- **OptionsPricingService** — Black-Scholes European options + Greeks
- **BenchmarkModelsService** — rolling correlation/covariance/regression, Sharpe, decomposition, PCA
- **OrderBookArenaService** — arena-based limit order book matching engine
- **PaymentAuthArenaService** — deterministic fraud-scoring rule engine
- **EventPulseService** — live firehose ingestion (Wikipedia EventStreams, Bluesky Jetstream, Stack Overflow's recent-questions feed) with streaming trending-topic aggregation. This is the only service that makes outbound network calls — it needs egress to `stream.wikimedia.org`, `jetstream2.us-east.bsky.network`, and `stackoverflow.com` to do anything useful. Stack Overflow has no push transport, so that source polls the feed every 30s instead of holding a persistent connection.

## Build & Run

**Requirements**: Rust 1.85+ (Docker image uses `rust:1.85-bookworm`)

```bash
cargo build --release
cargo run --release
```

**Port** — Railway injects `PORT` (often `8080`). The server binds `PORT`, then `GRPC_PORT`, then `50052`:

```bash
PORT=8080 cargo run --release
GRPC_PORT=50053 cargo run --release
```

## Docker / Railway

```bash
docker build -t tokimeki-quant-rust .
docker run -p 8080:8080 -e PORT=8080 tokimeki-quant-rust
```

`railway.toml` is included. Create a new Railway service from this repo; Railway builds via `Dockerfile`.

### Tokimeki env (same Railway project)

```bash
GRPC_RUST_HOST=${{tokimeki-quant-rust.RAILWAY_PRIVATE_DOMAIN}}
GRPC_RUST_PORT=${{tokimeki-quant-rust.PORT}}
```

Cross-project deploys need a TCP proxy; use the public proxy host:port instead of private domain.

## gRPC Testing

```bash
grpcurl -plaintext localhost:50052 list

grpcurl -plaintext \
  -d '{"n_paths":1000,"n_days":252,"n_stocks":5,"mu":0.08,"seed":42}' \
  localhost:50052 monte_carlo_var.MonteCarloVarService/RunVar

grpcurl -plaintext \
  -d '{"n_rows":1000,"n_assets":10,"window":252,"seed":42}' \
  localhost:50052 benchmark_models.BenchmarkModelsService/RunRollingCorrelation

grpcurl -plaintext \
  -d '{"sources":["wikipedia","bluesky"],"batch_ms":250,"trending_top_n":5}' \
  localhost:50052 event_pulse.EventPulseService/RunEventPulse
```
