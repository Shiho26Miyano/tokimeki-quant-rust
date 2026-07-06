use tokimeki_quant_rust::{
    engines, services,
    monte_carlo_var, options_pricing,
};
use tonic::transport::Server;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50052".to_string())
        .parse::<u16>()?;

    let addr = format!("0.0.0.0:{}", port).parse()?;

    println!("QuantEngine Rust gRPC server starting on port {}", port);
    println!("Services: MonteCarloVar | OptionsPricing");

    let monte_carlo_var_svc = services::mc_var::MonteCarloVarServiceImpl::default();
    let options_pricing_svc = services::opt_pricing::OptionsPricingServiceImpl::default();

    Server::builder()
        .add_service(
            monte_carlo_var::monte_carlo_var_service_server::MonteCarloVarServiceServer::new(monte_carlo_var_svc),
        )
        .add_service(
            options_pricing::options_pricing_service_server::OptionsPricingServiceServer::new(options_pricing_svc),
        )
        .serve(addr)
        .await?;

    Ok(())
}
