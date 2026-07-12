use tokimeki_quant_rust::{
    benchmark_models, monte_carlo_var, options_pricing, order_book_arena, payment_auth_arena,
    services,
};
use tonic::transport::Server;
use std::env;

fn listen_port() -> u16 {
    env::var("PORT")
        .or_else(|_| env::var("GRPC_PORT"))
        .unwrap_or_else(|_| "50052".to_string())
        .parse()
        .expect("PORT/GRPC_PORT must be a valid u16")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = listen_port();
    let addr = format!("0.0.0.0:{}", port).parse()?;

    println!("QuantEngine Rust gRPC server starting on port {}", port);
    println!("Services: MonteCarloVar | OptionsPricing | BenchmarkModels | OrderBookArena | PaymentAuthArena");

    let monte_carlo_var_svc = services::mc_var::MonteCarloVarServiceImpl::default();
    let options_pricing_svc = services::opt_pricing::OptionsPricingServiceImpl::default();
    let benchmark_models_svc = services::benchmark_models::BenchmarkModelsServiceImpl::default();
    let order_book_arena_svc = services::order_book_arena::OrderBookArenaServiceImpl::default();
    let payment_auth_arena_svc = services::payment_auth_arena::PaymentAuthArenaServiceImpl::default();

    Server::builder()
        .add_service(
            monte_carlo_var::monte_carlo_var_service_server::MonteCarloVarServiceServer::new(
                monte_carlo_var_svc,
            ),
        )
        .add_service(
            options_pricing::options_pricing_service_server::OptionsPricingServiceServer::new(
                options_pricing_svc,
            ),
        )
        .add_service(
            benchmark_models::benchmark_models_service_server::BenchmarkModelsServiceServer::new(
                benchmark_models_svc,
            ),
        )
        .add_service(
            order_book_arena::order_book_arena_service_server::OrderBookArenaServiceServer::new(
                order_book_arena_svc,
            ),
        )
        .add_service(
            payment_auth_arena::payment_auth_arena_service_server::PaymentAuthArenaServiceServer::new(
                payment_auth_arena_svc,
            ),
        )
        .serve(addr)
        .await?;

    Ok(())
}
