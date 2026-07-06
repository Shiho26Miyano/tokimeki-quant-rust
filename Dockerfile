FROM rust:1.75 as builder

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY proto ./proto
COPY src ./src
COPY build.rs .

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/tokimeki-quant-server /app/

EXPOSE 50052
ENV GRPC_PORT=50052

CMD ["/app/tokimeki-quant-server"]
