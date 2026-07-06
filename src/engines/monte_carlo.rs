use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::Normal;
use rand::Rng;
use std::sync::Arc;
use parking_lot::RwLock;

pub struct MonteCarloEngine {
    n_paths: usize,
    n_days: usize,
    n_stocks: usize,
    weights: Vec<f64>,
    vols: Vec<f64>,
    mu: f64,
    seed: u64,
    pnl_values: Arc<RwLock<Vec<f64>>>,
    paths_completed: Arc<RwLock<usize>>,
}

impl MonteCarloEngine {
    pub fn new(
        n_paths: usize,
        n_days: usize,
        n_stocks: usize,
        weights: Vec<f64>,
        vols: Vec<f64>,
        mu: f64,
        seed: u64,
    ) -> Self {
        Self {
            n_paths,
            n_days,
            n_stocks,
            weights,
            vols,
            mu,
            seed,
            pnl_values: Arc::new(RwLock::new(Vec::with_capacity(n_paths))),
            paths_completed: Arc::new(RwLock::new(0)),
        }
    }

    pub fn run_batch(&mut self, batch_size: usize) {
        let batch_size = batch_size.min(self.n_paths - self.paths_done());

        let n_paths = self.n_paths;
        let n_days = self.n_days;
        let n_stocks = self.n_stocks;
        let weights = self.weights.clone();
        let vols = self.vols.clone();
        let mu = self.mu;
        let seed = self.seed;
        let pnl_values = Arc::clone(&self.pnl_values);
        let paths_completed = Arc::clone(&self.paths_completed);

        rayon::scope(|s| {
            s.spawn(|_| {
                let results: Vec<f64> = (0..batch_size)
                    .map(|path_idx| {
                        simulate_path(
                            path_idx as u64 + seed,
                            n_days,
                            n_stocks,
                            &weights,
                            &vols,
                            mu,
                        )
                    })
                    .collect();

                let mut pnl = pnl_values.write();
                pnl.extend(results);

                let mut completed = paths_completed.write();
                *completed += batch_size;
            });
        });
    }

    pub fn paths_done(&self) -> usize {
        *self.paths_completed.read()
    }

    pub fn is_complete(&self) -> bool {
        self.paths_done() >= self.n_paths
    }

    pub fn var_95(&self) -> f64 {
        self.calculate_var(0.95)
    }

    pub fn var_99(&self) -> f64 {
        self.calculate_var(0.99)
    }

    pub fn cvar_95(&self) -> f64 {
        self.calculate_cvar(0.95)
    }

    pub fn cvar_99(&self) -> f64 {
        self.calculate_cvar(0.99)
    }

    fn calculate_var(&self, confidence: f64) -> f64 {
        let pnl = self.pnl_values.read();
        if pnl.is_empty() {
            return 0.0;
        }

        let mut sorted = pnl.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let idx = ((1.0 - confidence) * sorted.len() as f64) as usize;
        sorted.get(idx).copied().unwrap_or(0.0)
    }

    fn calculate_cvar(&self, confidence: f64) -> f64 {
        let pnl = self.pnl_values.read();
        if pnl.is_empty() {
            return 0.0;
        }

        let mut sorted = pnl.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let idx = ((1.0 - confidence) * sorted.len() as f64) as usize;
        sorted[..=idx].iter().sum::<f64>() / (idx as f64 + 1.0)
    }
}

fn simulate_path(seed: u64, n_days: usize, n_stocks: usize, weights: &[f64], vols: &[f64], mu: f64) -> f64 {
    let mut rng = StdRng::seed_from_u64(seed);
    let normal = Normal::new(0.0, 1.0).unwrap();

    let mut prices = vec![1.0; n_stocks];
    let dt = 1.0 / 252.0;

    for _ in 0..n_days {
        for stock in 0..n_stocks {
            let dW = rng.sample(&normal);
            prices[stock] *= (1.0 + mu * dt + vols[stock] * dW * dt.sqrt()).exp();
        }
    }

    let portfolio_value: f64 = prices.iter().zip(weights.iter()).map(|(p, w)| p * w).sum();
    -(1.0 - portfolio_value)
}
