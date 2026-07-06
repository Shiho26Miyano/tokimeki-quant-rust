use crate::monte_carlo_var::{VarRequest, VarResult};
use crate::engines::monte_carlo::MonteCarloEngine;
use tonic::{Request, Response, Status};
use tokio::sync::mpsc;
use std::time::Instant;

#[derive(Default)]
pub struct MonteCarloVarServiceImpl;

#[tonic::async_trait]
impl crate::monte_carlo_var::monte_carlo_var_service_server::MonteCarloVarService
    for MonteCarloVarServiceImpl
{
    type RunVarStream = mpsc::Receiver<Result<VarResult, Status>>;

    async fn run_var(
        &self,
        request: Request<VarRequest>,
    ) -> Result<Response<Self::RunVarStream>, Status> {
        let req = request.into_inner();
        let (mut tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            if let Err(e) = process_var(&req, &mut tx).await {
                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
            }
        });

        Ok(Response::new(rx))
    }
}

async fn process_var(
    req: &VarRequest,
    tx: &mut mpsc::Sender<Result<VarResult, Status>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let n_paths = if req.n_paths > 0 { req.n_paths as usize } else { 200_000 };
    let n_days = if req.n_days > 0 { req.n_days as usize } else { 10 };
    let n_stocks = if req.n_stocks > 0 { req.n_stocks as usize } else { 5 };
    let stream_every = if req.stream_every > 0 { req.stream_every as usize } else { 20_000 };
    let mu = req.mu;
    let seed = if req.seed > 0 { req.seed as u64 } else { 42 };

    let weights = if req.weights.is_empty() {
        vec![1.0 / n_stocks as f64; n_stocks]
    } else {
        req.weights.clone()
    };

    let vols = if req.vols.is_empty() {
        vec![0.15; n_stocks]
    } else {
        req.vols.clone()
    };

    let mut engine = MonteCarloEngine::new(n_paths, n_days, n_stocks, weights, vols, mu, seed);
    let mut prev_paths = 0;

    while !engine.is_complete() {
        engine.run_batch(stream_every);
        let paths_done = engine.paths_done();

        if paths_done - prev_paths >= stream_every {
            let result = VarResult {
                paths_done: paths_done as i32,
                var_95: engine.var_95(),
                var_99: engine.var_99(),
                cvar_95: engine.cvar_95(),
                cvar_99: engine.cvar_99(),
                is_final: false,
                elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
                paths_per_sec: 0.0,
                language: "rust".to_string(),
                peak_mem_mb: 0.0,
                pnl_histogram: vec![],
                histogram_bins: 0,
                implementation: "parallel_rayon".to_string(),
            };

            if tx.send(Ok(result)).await.is_err() {
                return Err("Client disconnected".into());
            }

            prev_paths = paths_done;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }

    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let paths_per_sec = n_paths as f64 / elapsed.as_secs_f64();

    let final_result = VarResult {
        paths_done: n_paths as i32,
        var_95: engine.var_95(),
        var_99: engine.var_99(),
        cvar_95: engine.cvar_95(),
        cvar_99: engine.cvar_99(),
        is_final: true,
        elapsed_ms,
        paths_per_sec,
        language: "rust".to_string(),
        peak_mem_mb: 0.0,
        pnl_histogram: vec![],
        histogram_bins: 0,
        implementation: "parallel_rayon".to_string(),
    };

    let _ = tx.send(Ok(final_result)).await;
    Ok(())
}
