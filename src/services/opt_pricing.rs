use crate::options_pricing::{OptionsPricingRequest, OptionsPricingResult, OptionResult};
use crate::engines::black_scholes::BlackScholesEngine;
use tonic::{Request, Response, Status};
use tokio::sync::mpsc;
use std::time::Instant;

#[derive(Default)]
pub struct OptionsPricingServiceImpl;

#[tonic::async_trait]
impl crate::options_pricing::options_pricing_service_server::OptionsPricingService
    for OptionsPricingServiceImpl
{
    type PriceOptionsStream = mpsc::Receiver<Result<OptionsPricingResult, Status>>;

    async fn price_options(
        &self,
        request: Request<OptionsPricingRequest>,
    ) -> Result<Response<Self::PriceOptionsStream>, Status> {
        let req = request.into_inner();
        let (mut tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            if let Err(e) = process_options(&req, &mut tx).await {
                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
            }
        });

        Ok(Response::new(rx))
    }
}

async fn process_options(
    req: &OptionsPricingRequest,
    tx: &mut mpsc::Sender<Result<OptionsPricingResult, Status>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let contracts = &req.contracts;
    let stream_every = if req.stream_every > 0 { req.stream_every as usize } else { 50 };
    let total = contracts.len();

    let mut results = Vec::new();
    let engine = BlackScholesEngine;

    for (i, contract) in contracts.iter().enumerate() {
        let greeks = engine.price(contract);
        results.push(OptionResult {
            contract_index: i as i32,
            price: greeks.price,
            delta: greeks.delta,
            gamma: greeks.gamma,
            vega: greeks.vega,
            theta: greeks.theta,
            rho: greeks.rho,
        });

        if (results.len() >= stream_every) || (i == total - 1) {
            let is_final = i == total - 1;
            let result = OptionsPricingResult {
                results: results.clone(),
                is_final,
                elapsed_ms: if is_final { start.elapsed().as_secs_f64() * 1000.0 } else { 0.0 },
                contracts_done: (i + 1) as i32,
                implementation: "black_scholes".to_string(),
            };

            if tx.send(Ok(result)).await.is_err() {
                return Err("Client disconnected".into());
            }

            results.clear();
        }
    }

    if total == 0 {
        let result = OptionsPricingResult {
            results: vec![],
            is_final: true,
            elapsed_ms: 0.0,
            contracts_done: 0,
            implementation: "black_scholes".to_string(),
        };
        let _ = tx.send(Ok(result)).await;
    }

    Ok(())
}
