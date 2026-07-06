use crate::options_pricing::OptionContract;
use std::f64::consts::PI;

pub struct Greeks {
    pub price: f64,
    pub delta: f64,
    pub gamma: f64,
    pub vega: f64,
    pub theta: f64,
    pub rho: f64,
}

pub struct BlackScholesEngine;

impl BlackScholesEngine {
    pub fn price(&self, contract: &OptionContract) -> Greeks {
        let s = contract.spot;
        let k = contract.strike;
        let t = contract.time_to_expiry;
        let sigma = contract.volatility;
        let r = contract.risk_free_rate;
        let q = contract.dividend_yield;
        let is_call = contract.option_type == 0;

        if t <= 0.0 {
            let intrinsic = if is_call {
                (s - k).max(0.0)
            } else {
                (k - s).max(0.0)
            };
            return Greeks {
                price: intrinsic,
                delta: if is_call { 1.0 } else { 0.0 },
                gamma: 0.0,
                vega: 0.0,
                theta: 0.0,
                rho: 0.0,
            };
        }

        let d1 = (((s / k).ln() + (r - q + 0.5 * sigma * sigma) * t) / (sigma * t.sqrt()));
        let d2 = d1 - sigma * t.sqrt();

        let nd1 = normal_cdf(d1);
        let nd2 = normal_cdf(d2);
        let pdf_d1 = normal_pdf(d1);

        let price = if is_call {
            s * (-q * t).exp() * nd1 - k * (-r * t).exp() * nd2
        } else {
            k * (-r * t).exp() * (1.0 - nd2) - s * (-q * t).exp() * (1.0 - nd1)
        };

        let delta = if is_call {
            (-q * t).exp() * nd1
        } else {
            (-q * t).exp() * (nd1 - 1.0)
        };

        let gamma = pdf_d1 / (s * sigma * t.sqrt()) * (-q * t).exp();

        let vega = s * pdf_d1 * t.sqrt() * (-q * t).exp() / 100.0;

        let theta = if is_call {
            (-s * pdf_d1 * sigma / (2.0 * t.sqrt())) * (-q * t).exp()
                - r * k * (-r * t).exp() * nd2
                + q * s * nd1 * (-q * t).exp()
        } else {
            (-s * pdf_d1 * sigma / (2.0 * t.sqrt())) * (-q * t).exp()
                + r * k * (-r * t).exp() * (1.0 - nd2)
                - q * s * (1.0 - nd1) * (-q * t).exp()
        } / 365.0;

        let rho = if is_call {
            k * t * (-r * t).exp() * nd2 / 100.0
        } else {
            -k * t * (-r * t).exp() * (1.0 - nd2) / 100.0
        };

        Greeks {
            price,
            delta,
            gamma,
            vega,
            theta,
            rho,
        }
    }
}

fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

fn normal_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * PI).sqrt()
}

fn erf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}
