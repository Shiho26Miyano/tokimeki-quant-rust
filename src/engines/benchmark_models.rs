use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

pub fn generate_data(n_rows: usize, n_assets: usize, seed: u64) -> Vec<Vec<f64>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n_rows)
        .map(|_| (0..n_assets).map(|_| rng.gen::<f64>() * 0.01).collect())
        .collect()
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn std_dev(values: &[f64], mu: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let var = values.iter().map(|v| (v - mu).powi(2)).sum::<f64>() / values.len() as f64;
    var.sqrt()
}

fn correlation_matrix(window: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n_assets = window[0].len();
    let mut cols: Vec<Vec<f64>> = vec![Vec::with_capacity(window.len()); n_assets];
    for row in window {
        for (j, val) in row.iter().enumerate() {
            cols[j].push(*val);
        }
    }

    let means: Vec<f64> = cols.iter().map(|c| mean(c)).collect();
    let stds: Vec<f64> = cols
        .iter()
        .zip(means.iter())
        .map(|(c, m)| std_dev(c, *m))
        .collect();

    let mut corr = vec![vec![0.0; n_assets]; n_assets];
    for i in 0..n_assets {
        corr[i][i] = 1.0;
        for j in (i + 1)..n_assets {
            let denom = stds[i] * stds[j];
            let c = if denom > 0.0 {
                cols[i]
                    .iter()
                    .zip(cols[j].iter())
                    .map(|(a, b)| (a - means[i]) * (b - means[j]))
                    .sum::<f64>()
                    / (cols[i].len() as f64 * denom)
            } else {
                0.0
            };
            corr[i][j] = c;
            corr[j][i] = c;
        }
    }
    corr
}

fn covariance_matrix(window: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n_assets = window[0].len();
    let n = window.len() as f64;
    let mut cols: Vec<Vec<f64>> = vec![Vec::with_capacity(window.len()); n_assets];
    for row in window {
        for (j, val) in row.iter().enumerate() {
            cols[j].push(*val);
        }
    }
    let means: Vec<f64> = cols.iter().map(|c| mean(c)).collect();
    let mut cov = vec![vec![0.0; n_assets]; n_assets];
    for i in 0..n_assets {
        for j in i..n_assets {
            let c = cols[i]
                .iter()
                .zip(cols[j].iter())
                .map(|(a, b)| (a - means[i]) * (b - means[j]))
                .sum::<f64>()
                / (n - 1.0).max(1.0);
            cov[i][j] = c;
            cov[j][i] = c;
        }
    }
    cov
}

pub fn rolling_correlation(
    data: &[Vec<f64>],
    window: usize,
) -> (usize, f64, f64) {
    let start = Instant::now();
    let n_out = data.len().saturating_sub(window).saturating_add(1);
    let mut bytes = 0usize;

    for i in 0..n_out {
        let window_slice = &data[i..i + window];
        let corr = correlation_matrix(window_slice);
        bytes += corr.len() * corr[0].len() * std::mem::size_of::<f64>();
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let peak_mem_mb = bytes as f64 / (1024.0 * 1024.0);
    (n_out, elapsed_ms, peak_mem_mb)
}

pub fn rolling_covariance(data: &[Vec<f64>], window: usize) -> (usize, f64, f64) {
    let start = Instant::now();
    let n_out = data.len().saturating_sub(window).saturating_add(1);
    let mut bytes = 0usize;

    for i in 0..n_out {
        let window_slice = &data[i..i + window];
        let cov = covariance_matrix(window_slice);
        bytes += cov.len() * cov[0].len() * std::mem::size_of::<f64>();
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let peak_mem_mb = bytes as f64 / (1024.0 * 1024.0);
    (n_out, elapsed_ms, peak_mem_mb)
}

pub fn rolling_regression(data: &[Vec<f64>], window: usize) -> (usize, f64, f64) {
    let start = Instant::now();
    let n_out = data.len().saturating_sub(window).saturating_add(1);
    let n_features = data[0].len().saturating_sub(1);
    let k = n_features + 1; // intercept
    let mut bytes = 0usize;

    for i in 0..n_out {
        let window_slice = &data[i..i + window];
        let mut xtx = vec![vec![0.0; k]; k];
        let mut xty = vec![0.0; k];
        for row in window_slice {
            let yi = row[0];
            let mut x = vec![1.0];
            x.extend_from_slice(&row[1..]);
            for (r, xi) in x.iter().enumerate() {
                for (c, xj) in x.iter().enumerate() {
                    xtx[r][c] += xi * xj;
                }
                xty[r] += xi * yi;
            }
        }
        bytes += k * std::mem::size_of::<f64>();
        let _ = solve_linear(&xtx, &xty);
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let peak_mem_mb = bytes as f64 / (1024.0 * 1024.0);
    (n_out, elapsed_ms, peak_mem_mb)
}

fn solve_linear(a: &[Vec<f64>], b: &[f64]) -> Vec<f64> {
    let n = b.len();
    let mut aug: Vec<Vec<f64>> = a
        .iter()
        .zip(b.iter())
        .map(|(row, &bi)| {
            let mut r = row.clone();
            r.push(bi);
            r
        })
        .collect();

    for col in 0..n {
        let pivot = (col..n)
            .max_by(|&i, &j| {
                aug[i][col]
                    .abs()
                    .partial_cmp(&aug[j][col].abs())
                    .unwrap()
            })
            .unwrap_or(col);
        aug.swap(col, pivot);
        let div = aug[col][col];
        if div.abs() < 1e-12 {
            return vec![f64::NAN; n];
        }
        for j in col..=n {
            aug[col][j] /= div;
        }
        for i in 0..n {
            if i == col {
                continue;
            }
            let factor = aug[i][col];
            for j in col..=n {
                aug[i][j] -= factor * aug[col][j];
            }
        }
    }
    aug.iter().map(|row| row[n]).collect()
}

pub fn rolling_sharpe(data: &[Vec<f64>], window: usize) -> (usize, f64, f64) {
    let start = Instant::now();
    let returns: Vec<f64> = data.iter().map(|row| row[0]).collect();
    let n_out = returns.len().saturating_sub(window).saturating_add(1);
    let mut bytes = n_out * std::mem::size_of::<f64>();

    for i in 0..n_out {
        let window_slice = &returns[i..i + window];
        let mu = mean(window_slice);
        let sigma = std_dev(window_slice, mu);
        let _sharpe = if sigma > 0.0 { mu / sigma } else { 0.0 };
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let peak_mem_mb = bytes as f64 / (1024.0 * 1024.0);
    (n_out, elapsed_ms, peak_mem_mb)
}

pub fn time_series_decomposition(data: &[Vec<f64>], period: usize) -> (usize, f64, f64) {
    let start = Instant::now();
    let series: Vec<f64> = data.iter().map(|row| row[0]).collect();
    let n = series.len();
    let period = period.max(1);

    // Moving average trend
    let mut trend = vec![0.0; n];
    for i in 0..n {
        let lo = i.saturating_sub(period / 2);
        let hi = (i + period / 2 + 1).min(n);
        let slice = &series[lo..hi];
        trend[i] = mean(slice);
    }

    let detrended: Vec<f64> = series.iter().zip(trend.iter()).map(|(s, t)| s - t).collect();
    let mut seasonal = vec![0.0; n];
    for p in 0..period {
        let indices: Vec<usize> = (p..n).step_by(period).collect();
        let avg = if indices.is_empty() {
            0.0
        } else {
            indices.iter().map(|&i| detrended[i]).sum::<f64>() / indices.len() as f64
        };
        for &i in &indices {
            seasonal[i] = avg;
        }
    }

    let bytes = (trend.len() + seasonal.len() + n) * std::mem::size_of::<f64>();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let peak_mem_mb = bytes as f64 / (1024.0 * 1024.0);
    (n, elapsed_ms, peak_mem_mb)
}

pub fn pca_decomposition(data: &[Vec<f64>], n_components: usize) -> (usize, f64, f64) {
    let start = Instant::now();
    let n_rows = data.len();
    let n_assets = data[0].len();
    let n_components = n_components.min(n_assets);

    let mut means = vec![0.0; n_assets];
    for row in data {
        for (j, val) in row.iter().enumerate() {
            means[j] += val;
        }
    }
    for m in &mut means {
        *m /= n_rows as f64;
    }

    let mut cov = vec![vec![0.0; n_assets]; n_assets];
    for row in data {
        for i in 0..n_assets {
            for j in i..n_assets {
                let c = (row[i] - means[i]) * (row[j] - means[j]);
                cov[i][j] += c;
                if i != j {
                    cov[j][i] += c;
                }
            }
        }
    }
    let denom = (n_rows as f64 - 1.0).max(1.0);
    for row in &mut cov {
        for v in row {
            *v /= denom;
        }
    }

    // Power iteration for top eigenvectors (sufficient for benchmark timing)
    let mut loadings = vec![vec![0.0; n_components]; n_assets];
    let mut used = cov.clone();
    for c in 0..n_components {
        let mut v = vec![1.0 / (n_assets as f64).sqrt(); n_assets];
        for _ in 0..50 {
            let mut nv = vec![0.0; n_assets];
            for i in 0..n_assets {
                for j in 0..n_assets {
                    nv[i] += used[i][j] * v[j];
                }
            }
            let norm = nv.iter().map(|x| x * x).sum::<f64>().sqrt().max(1e-12);
            for x in &mut nv {
                *x /= norm;
            }
            v = nv;
        }
        for i in 0..n_assets {
            loadings[i][c] = v[i];
        }
        let lambda: f64 = v
            .iter()
            .enumerate()
            .map(|(i, &vi)| {
                vi * (0..n_assets).map(|j| used[i][j] * v[j]).sum::<f64>()
            })
            .sum();
        for i in 0..n_assets {
            for j in 0..n_assets {
                used[i][j] -= lambda * v[i] * v[j];
            }
        }
    }

    let bytes = (loadings.len() * loadings[0].len() + n_rows * n_components) * std::mem::size_of::<f64>();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let peak_mem_mb = bytes as f64 / (1024.0 * 1024.0);
    (n_rows, elapsed_ms, peak_mem_mb)
}
