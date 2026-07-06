use crate::benchmark_models::{
    BenchmarkDataRequest, PcaDecompositionResult, RollingCorrelationResult,
    RollingCovarianceResult, RollingRegressionResult, RollingSharpResult,
    TimeSeriesDecompositionResult,
};
use crate::engines::benchmark_models::{
    generate_data, pca_decomposition, rolling_correlation, rolling_covariance,
    rolling_regression, rolling_sharpe, time_series_decomposition,
};
use tonic::{Request, Response, Status};
use tokio::sync::mpsc;

#[derive(Default)]
pub struct BenchmarkModelsServiceImpl;

#[tonic::async_trait]
impl crate::benchmark_models::benchmark_models_service_server::BenchmarkModelsService
    for BenchmarkModelsServiceImpl
{
    type RunRollingCorrelationStream = mpsc::Receiver<Result<RollingCorrelationResult, Status>>;
    type RunRollingCovarianceStream = mpsc::Receiver<Result<RollingCovarianceResult, Status>>;
    type RunRollingRegressionStream = mpsc::Receiver<Result<RollingRegressionResult, Status>>;
    type RunRollingSharpStream = mpsc::Receiver<Result<RollingSharpResult, Status>>;
    type RunTimeSeriesDecompositionStream =
        mpsc::Receiver<Result<TimeSeriesDecompositionResult, Status>>;
    type RunPCADecompositionStream = mpsc::Receiver<Result<PcaDecompositionResult, Status>>;

    async fn run_rolling_correlation(
        &self,
        request: Request<BenchmarkDataRequest>,
    ) -> Result<Response<Self::RunRollingCorrelationStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let result = compute_rows(&req, rolling_correlation);
            let _ = tx.send(Ok(RollingCorrelationResult {
                output_rows: result.0 as i32,
                elapsed_ms: result.1,
                peak_mem_mb: result.2,
                is_final: true,
            })).await;
        });
        Ok(Response::new(rx))
    }

    async fn run_rolling_covariance(
        &self,
        request: Request<BenchmarkDataRequest>,
    ) -> Result<Response<Self::RunRollingCovarianceStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let result = compute_rows(&req, rolling_covariance);
            let _ = tx.send(Ok(RollingCovarianceResult {
                output_rows: result.0 as i32,
                elapsed_ms: result.1,
                peak_mem_mb: result.2,
                is_final: true,
            })).await;
        });
        Ok(Response::new(rx))
    }

    async fn run_rolling_regression(
        &self,
        request: Request<BenchmarkDataRequest>,
    ) -> Result<Response<Self::RunRollingRegressionStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let result = compute_rows(&req, rolling_regression);
            let _ = tx.send(Ok(RollingRegressionResult {
                output_rows: result.0 as i32,
                elapsed_ms: result.1,
                peak_mem_mb: result.2,
                is_final: true,
            })).await;
        });
        Ok(Response::new(rx))
    }

    async fn run_rolling_sharp(
        &self,
        request: Request<BenchmarkDataRequest>,
    ) -> Result<Response<Self::RunRollingSharpStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let result = compute_rows(&req, rolling_sharpe);
            let _ = tx.send(Ok(RollingSharpResult {
                output_length: result.0 as i32,
                elapsed_ms: result.1,
                peak_mem_mb: result.2,
                is_final: true,
            })).await;
        });
        Ok(Response::new(rx))
    }

    async fn run_time_series_decomposition(
        &self,
        request: Request<BenchmarkDataRequest>,
    ) -> Result<Response<Self::RunTimeSeriesDecompositionStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let result = compute_rows(&req, time_series_decomposition);
            let _ = tx.send(Ok(TimeSeriesDecompositionResult {
                output_length: result.0 as i32,
                elapsed_ms: result.1,
                peak_mem_mb: result.2,
                is_final: true,
            })).await;
        });
        Ok(Response::new(rx))
    }

    async fn run_pca_decomposition(
        &self,
        request: Request<BenchmarkDataRequest>,
    ) -> Result<Response<Self::RunPCADecompositionStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let n_rows = req.n_rows.max(1) as usize;
            let n_assets = req.n_assets.max(1) as usize;
            let n_components = req.window.max(1) as usize;
            let seed = if req.seed > 0 { req.seed as u64 } else { 42 };
            let data = generate_data(n_rows, n_assets, seed);
            let (output_rows, elapsed_ms, peak_mem_mb) =
                pca_decomposition(&data, n_components.min(n_assets));
            let _ = tx
                .send(Ok(PcaDecompositionResult {
                    output_rows: output_rows as i32,
                    elapsed_ms,
                    peak_mem_mb,
                    is_final: true,
                }))
                .await;
        });
        Ok(Response::new(rx))
    }
}

fn compute_rows(
    req: &BenchmarkDataRequest,
    f: fn(&[Vec<f64>], usize) -> (usize, f64, f64),
) -> (usize, f64, f64) {
    let n_rows = req.n_rows.max(1) as usize;
    let n_assets = req.n_assets.max(1) as usize;
    let window = req.window.max(1) as usize;
    let seed = if req.seed > 0 { req.seed as u64 } else { 42 };
    let data = generate_data(n_rows, n_assets, seed);
    f(&data, window)
}
