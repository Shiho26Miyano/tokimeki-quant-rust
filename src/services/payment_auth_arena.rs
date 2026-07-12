use crate::payment_auth_arena::{
    Decision as ProtoDecision, FraudDecision as ProtoFraudDecision, LatencySummary,
    PaymentAuthArenaEvent, PaymentAuthArenaRequest,
};
use crate::engines::payment_auth::{self, Decision, RuleLimits};
use tonic::{Request, Response, Status};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use std::time::Instant;

#[derive(Default)]
pub struct PaymentAuthArenaServiceImpl;

#[tonic::async_trait]
impl crate::payment_auth_arena::payment_auth_arena_service_server::PaymentAuthArenaService
    for PaymentAuthArenaServiceImpl
{
    type RunPaymentAuthArenaStream = ReceiverStream<Result<PaymentAuthArenaEvent, Status>>;

    async fn run_payment_auth_arena(
        &self,
        request: Request<PaymentAuthArenaRequest>,
    ) -> Result<Response<Self::RunPaymentAuthArenaStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            run_arena(&req, &tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn to_proto_decision(d: Decision) -> i32 {
    match d {
        Decision::Approve => ProtoDecision::Approve as i32,
        Decision::Review => ProtoDecision::Review as i32,
        Decision::Decline => ProtoDecision::Decline as i32,
    }
}

async fn send_error(tx: &mpsc::Sender<Result<PaymentAuthArenaEvent, Status>>, msg: String) {
    let evt = PaymentAuthArenaEvent {
        language: "rust".to_string(),
        implementation: "Rust fixed-point rule engine".to_string(),
        is_final: true,
        error: msg,
        ..Default::default()
    };
    let _ = tx.send(Ok(evt)).await;
}

async fn run_arena(
    req: &PaymentAuthArenaRequest,
    tx: &mpsc::Sender<Result<PaymentAuthArenaEvent, Status>>,
) {
    let transaction_count = req.transaction_count.max(0) as usize;
    let expected_len = transaction_count.checked_mul(payment_auth::RECORD_LEN);
    match expected_len {
        Some(len) if len == req.transaction_flow.len() => {}
        _ => {
            send_error(
                tx,
                format!(
                    "transaction_count ({}) does not match transaction_flow.len()/{} (got {} bytes)",
                    transaction_count,
                    payment_auth::RECORD_LEN,
                    req.transaction_flow.len()
                ),
            )
            .await;
            return;
        }
    }

    let records = match payment_auth::parse_records(&req.transaction_flow) {
        Ok(r) => r,
        Err(e) => {
            send_error(tx, e).await;
            return;
        }
    };

    let limits = RuleLimits {
        velocity_limit: if req.velocity_limit > 0 { req.velocity_limit } else { 5 },
        geo_delta_limit_km: if req.geo_delta_limit_km > 0 { req.geo_delta_limit_km } else { 500 },
        amount_limit_cents: if req.amount_limit_cents > 0 { req.amount_limit_cents } else { 50_000 },
    };

    let batch_size = if req.batch_size > 0 { req.batch_size as usize } else { 1000 };

    let mut all_latencies: Vec<u32> = Vec::with_capacity(records.len());
    let mut approve_count: u64 = 0;
    let mut review_count: u64 = 0;
    let mut decline_count: u64 = 0;

    let mut batch_latencies: Vec<u32> = Vec::with_capacity(batch_size);
    let mut batch_decisions: Vec<ProtoFraudDecision> = Vec::with_capacity(batch_size);
    let mut batch_start_seq: u64 = 0;

    let start = Instant::now();

    for (idx, tx_rec) in records.iter().enumerate() {
        let op_start = Instant::now();
        let decision = payment_auth::score(tx_rec, &limits);
        let elapsed_ns = op_start.elapsed().as_nanos();
        let elapsed_ns_u32 = if elapsed_ns > u32::MAX as u128 { u32::MAX } else { elapsed_ns as u32 };

        match decision.decision {
            Decision::Approve => approve_count += 1,
            Decision::Review => review_count += 1,
            Decision::Decline => decline_count += 1,
        }

        all_latencies.push(elapsed_ns_u32);
        batch_latencies.push(elapsed_ns_u32);
        batch_decisions.push(ProtoFraudDecision {
            seq: decision.seq,
            risk_score: decision.risk_score,
            decision: to_proto_decision(decision.decision),
            reason_mask: decision.reason_mask,
        });

        let processed = idx + 1;
        let is_batch_boundary = processed % batch_size == 0 || processed == records.len();
        if is_batch_boundary {
            let evt = PaymentAuthArenaEvent {
                language: "rust".to_string(),
                implementation: "Rust fixed-point rule engine".to_string(),
                batch_start_seq,
                batch_end_seq: (processed - 1) as u64,
                latency_ns: std::mem::take(&mut batch_latencies),
                decisions: std::mem::take(&mut batch_decisions),
                is_final: false,
                summary: None,
                final_transaction_count: 0,
                final_approve_count: 0,
                final_review_count: 0,
                final_decline_count: 0,
                error: String::new(),
            };

            if tx.send(Ok(evt)).await.is_err() {
                return; // client disconnected
            }
            batch_start_seq = processed as u64;
            batch_latencies = Vec::with_capacity(batch_size);
        }
    }

    let total_elapsed = start.elapsed();
    let total_elapsed_ms = total_elapsed.as_secs_f64() * 1000.0;

    let mut sorted_latencies = all_latencies.clone();
    sorted_latencies.sort_unstable();
    let sample_count = sorted_latencies.len() as u64;

    let percentile = |p: f64| -> f64 {
        if sorted_latencies.is_empty() {
            return 0.0;
        }
        let idx = ((sorted_latencies.len() as f64 - 1.0) * p).round() as usize;
        sorted_latencies[idx.min(sorted_latencies.len() - 1)] as f64
    };

    let p50_ns = percentile(0.50);
    let p99_ns = percentile(0.99);
    let p999_ns = percentile(0.999);
    let max_ns = sorted_latencies.last().copied().unwrap_or(0) as f64;
    let mean_ns = if sample_count > 0 {
        sorted_latencies.iter().map(|&v| v as f64).sum::<f64>() / sample_count as f64
    } else {
        0.0
    };
    let txns_per_sec = if total_elapsed.as_secs_f64() > 0.0 {
        records.len() as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };

    let summary = LatencySummary {
        p50_ns,
        p99_ns,
        p999_ns,
        max_ns,
        mean_ns,
        sample_count,
        total_elapsed_ms,
        txns_per_sec,
    };

    let final_evt = PaymentAuthArenaEvent {
        language: "rust".to_string(),
        implementation: "Rust fixed-point rule engine".to_string(),
        batch_start_seq: 0,
        batch_end_seq: 0,
        latency_ns: vec![],
        decisions: vec![],
        is_final: true,
        summary: Some(summary),
        final_transaction_count: records.len() as u64,
        final_approve_count: approve_count,
        final_review_count: review_count,
        final_decline_count: decline_count,
        error: String::new(),
    };

    let _ = tx.send(Ok(final_evt)).await;
}
