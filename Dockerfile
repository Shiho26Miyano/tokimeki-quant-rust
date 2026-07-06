FROM rust:1.75 as builder

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY proto ./proto
COPY src ./src
COPY build.rs .

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates netcat-openbsd && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/tokimeki-quant-server /app/

EXPOSE 50052

# Railway sets PORT (often 8080); server binds PORT then GRPC_PORT.
ENV GRPC_PORT=50052
HEALTHCHECK --interval=15s --timeout=5s --start-period=45s --retries=3 \
  CMD sh -c 'nc -z localhost ${PORT:-${GRPC_PORT:-50052}}' || exit 1

CMD ["/app/tokimeki-quant-server"]
