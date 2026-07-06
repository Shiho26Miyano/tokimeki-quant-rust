# Tokimeki Quant Engine - Rust Implementation

High-performance gRPC-based quantitative engine in Rust with two core services:

- **MonteCarloVarService**: Parallel GBM portfolio VaR/CVaR computation via rayon
- **OptionsPricingService**: Black-Scholes European option pricing with Greeks

## Build & Run

**Requirements**: Rust 1.75+

```bash
# Build
cargo build --release

# Run (default port 50052)
cargo run --release

# Custom port
GRPC_PORT=50053 cargo run --release
```

## Docker

```bash
docker build -t tokimeki-quant-rust .
docker run -p 50052:50052 -e GRPC_PORT=50052 tokimeki-quant-rust
```

## Services

### MonteCarloVarService

```
rpc RunVar(VarRequest) returns (stream VarResult)
```

**Inputs**: n_paths, n_days, n_stocks, weights[], vols[], mu, seed, stream_every

**Output**: Streams VarResult with paths_done, var_95, var_99, cvar_95, cvar_99, elapsed_ms

Uses parallel rayon for near-linear speedup on multi-core.

### OptionsPricingService

```
rpc PriceOptions(OptionsPricingRequest) returns (stream OptionsPricingResult)
```

**Input**: List of OptionContract (spot, strike, time_to_expiry, volatility, risk_free_rate, etc.)

**Output**: Streams pricing results with all Greeks (delta, gamma, vega, theta, rho)

## Python Client

The `RustQuantEngineClient` in Tokimeki connects to this service:

```python
from app.services.rust_quant_engine_client import RustQuantEngineClient

client = RustQuantEngineClient(host="localhost", port=50052)
await client.connect()

async for result in client.run_var(n_paths=10000, n_days=252):
    print(f"VaR95: {result.var_95}")
```

## gRPC Testing

```bash
# List services
grpcurl -plaintext localhost:50052 list

# Test VaR
grpcurl -plaintext \
  -d '{"n_paths":1000,"n_days":252,"n_stocks":5,"mu":0.08,"seed":42}' \
  localhost:50052 monte_carlo_var.MonteCarloVarService/RunVar

# Test options pricing
grpcurl -plaintext \
  -d '{"contracts":[{"spot":100,"strike":100,"time_to_expiry":1.0,"volatility":0.2,"risk_free_rate":0.05}]}' \
  localhost:50052 options_pricing.OptionsPricingService/PriceOptions
```
