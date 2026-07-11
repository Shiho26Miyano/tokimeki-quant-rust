use crate::order_book_arena::{
    DepthLevel, DepthSnapshot, LatencySummary, OrderBookArenaEvent, OrderBookArenaRequest, Trade,
};
use crate::engines::order_book::{OrderBook, Side};
use tonic::{Request, Response, Status};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use std::time::Instant;

const RECORD_LEN: usize = 24;
const EMPTY_SENTINEL: u32 = 0xFFFF_FFFF;

#[derive(Default)]
pub struct OrderBookArenaServiceImpl;

#[tonic::async_trait]
impl crate::order_book_arena::order_book_arena_service_server::OrderBookArenaService
    for OrderBookArenaServiceImpl
{
    type RunOrderBookArenaStream = ReceiverStream<Result<OrderBookArenaEvent, Status>>;

    async fn run_order_book_arena(
        &self,
        request: Request<OrderBookArenaRequest>,
    ) -> Result<Response<Self::RunOrderBookArenaStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            run_arena(&req, &tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Clone, Copy)]
struct ParsedRecord {
    record_type: u8, // 0=LIMIT, 1=CANCEL, 2=MARKET
    order_id: u64,
    side: u8, // 0=Buy, 1=Sell
    price_idx: u32,
    qty: u32,
}

/// 解析 24 字节定长二进制订单流记录。
/// 偏移: 0=u8 record_type, 4=u64 LE order_id, 12=u8 side, 16=u32 LE price_idx, 20=u32 LE qty
fn parse_records(buf: &[u8]) -> Result<Vec<ParsedRecord>, String> {
    if buf.len() % RECORD_LEN != 0 {
        return Err(format!(
            "order_flow length {} is not a multiple of {}",
            buf.len(),
            RECORD_LEN
        ));
    }
    let n = buf.len() / RECORD_LEN;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let rec = &buf[i * RECORD_LEN..(i + 1) * RECORD_LEN];
        let record_type = rec[0];
        let order_id = u64::from_le_bytes(rec[4..12].try_into().unwrap());
        let side = rec[12];
        let price_idx = u32::from_le_bytes(rec[16..20].try_into().unwrap());
        let qty = u32::from_le_bytes(rec[20..24].try_into().unwrap());
        out.push(ParsedRecord {
            record_type,
            order_id,
            side,
            price_idx,
            qty,
        });
    }
    Ok(out)
}

fn side_of(raw: u8) -> Side {
    if raw == 1 {
        Side::Sell
    } else {
        Side::Buy
    }
}

async fn send_error(tx: &mpsc::Sender<Result<OrderBookArenaEvent, Status>>, msg: String) {
    let evt = OrderBookArenaEvent {
        language: "rust".to_string(),
        implementation: "Rust arena+intrusive list".to_string(),
        is_final: true,
        error: msg,
        ..Default::default()
    };
    let _ = tx.send(Ok(evt)).await;
}

fn build_snapshot(book: &OrderBook, seq: u64, depth_levels: usize) -> DepthSnapshot {
    let bids = book
        .top_levels(Side::Buy, depth_levels)
        .into_iter()
        .map(|(price_idx, qty)| DepthLevel { price_idx, qty })
        .collect();
    let asks = book
        .top_levels(Side::Sell, depth_levels)
        .into_iter()
        .map(|(price_idx, qty)| DepthLevel { price_idx, qty })
        .collect();
    DepthSnapshot { seq, bids, asks }
}

async fn run_arena(req: &OrderBookArenaRequest, tx: &mpsc::Sender<Result<OrderBookArenaEvent, Status>>) {
    let order_count = req.order_count.max(0) as usize;
    let expected_len = order_count.checked_mul(RECORD_LEN);
    match expected_len {
        Some(len) if len == req.order_flow.len() => {}
        _ => {
            send_error(
                tx,
                format!(
                    "order_count ({}) does not match order_flow.len()/{} (got {} bytes)",
                    order_count,
                    RECORD_LEN,
                    req.order_flow.len()
                ),
            )
            .await;
            return;
        }
    }

    let records = match parse_records(&req.order_flow) {
        Ok(r) => r,
        Err(e) => {
            send_error(tx, e).await;
            return;
        }
    };

    if records.len() != order_count {
        send_error(
            tx,
            format!(
                "parsed {} records but order_count was {}",
                records.len(),
                order_count
            ),
        )
        .await;
        return;
    }

    let n_levels = if req.n_levels > 0 { req.n_levels as usize } else { 4096 };
    let book_capacity = if req.book_capacity > 0 { req.book_capacity as usize } else { 100_000 };
    let snapshot_every = if req.snapshot_every > 0 { req.snapshot_every as usize } else { usize::MAX };
    let batch_size = if req.batch_size > 0 { req.batch_size as usize } else { 1000 };
    let depth_levels = if req.depth_levels > 0 { req.depth_levels as usize } else { 10 };

    let mut book = OrderBook::new(n_levels, book_capacity);

    let mut all_latencies: Vec<u32> = Vec::with_capacity(records.len());
    let mut total_trade_count: u64 = 0;
    let mut total_trade_volume: u64 = 0;

    let mut batch_latencies: Vec<u32> = Vec::with_capacity(batch_size);
    let mut batch_trades: Vec<Trade> = Vec::new();
    let mut batch_start_seq: u64 = 0;

    let mut trades_buf: Vec<crate::engines::order_book::Trade> = Vec::with_capacity(64);

    let mut next_snapshot_at: usize = snapshot_every;
    let mut pending_snapshot: Option<DepthSnapshot> = None;

    let start = Instant::now();

    for (idx, rec) in records.iter().enumerate() {
        let seq = idx as u64;
        trades_buf.clear();

        let op_start = Instant::now();
        match rec.record_type {
            0 => {
                book.limit_order(
                    rec.order_id,
                    side_of(rec.side),
                    rec.price_idx,
                    rec.qty,
                    &mut trades_buf,
                );
            }
            1 => {
                book.cancel(rec.order_id);
            }
            2 => {
                book.market_order(rec.order_id, side_of(rec.side), rec.qty, &mut trades_buf);
            }
            other => {
                send_error(tx, format!("unknown record_type {} at record {}", other, idx)).await;
                return;
            }
        }
        let elapsed_ns = op_start.elapsed().as_nanos();
        let elapsed_ns_u32 = if elapsed_ns > u32::MAX as u128 {
            u32::MAX
        } else {
            elapsed_ns as u32
        };

        all_latencies.push(elapsed_ns_u32);
        batch_latencies.push(elapsed_ns_u32);

        if !trades_buf.is_empty() {
            for t in &trades_buf {
                total_trade_count += 1;
                total_trade_volume += t.qty as u64;
                batch_trades.push(Trade {
                    seq,
                    maker_id: t.maker_id,
                    taker_id: t.taker_id,
                    price_idx: t.price_idx,
                    qty: t.qty,
                });
            }
        }

        let orders_processed = idx + 1;
        if orders_processed >= next_snapshot_at {
            pending_snapshot = Some(build_snapshot(&book, seq, depth_levels));
            next_snapshot_at += snapshot_every;
        }

        let is_batch_boundary = orders_processed % batch_size == 0 || orders_processed == records.len();
        if is_batch_boundary {
            let evt = OrderBookArenaEvent {
                language: "rust".to_string(),
                implementation: "Rust arena+intrusive list".to_string(),
                batch_start_seq,
                batch_end_seq: seq,
                latency_ns: std::mem::take(&mut batch_latencies),
                trades: std::mem::take(&mut batch_trades),
                snapshot: pending_snapshot.take(),
                is_final: false,
                summary: None,
                final_order_count: 0,
                final_best_bid: 0,
                final_best_ask: 0,
                final_trade_count: 0,
                final_trade_volume: 0,
                error: String::new(),
            };

            if tx.send(Ok(evt)).await.is_err() {
                return; // client disconnected
            }
            batch_start_seq = seq + 1;
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
    let orders_per_sec = if total_elapsed.as_secs_f64() > 0.0 {
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
        orders_per_sec,
    };

    let final_best_bid = book.best_bid().unwrap_or(EMPTY_SENTINEL);
    let final_best_ask = book.best_ask().unwrap_or(EMPTY_SENTINEL);

    let final_evt = OrderBookArenaEvent {
        language: "rust".to_string(),
        implementation: "Rust arena+intrusive list".to_string(),
        batch_start_seq: 0,
        batch_end_seq: 0,
        latency_ns: vec![],
        trades: vec![],
        snapshot: None,
        is_final: true,
        summary: Some(summary),
        final_order_count: records.len() as u64,
        final_best_bid,
        final_best_ask,
        final_trade_count: total_trade_count,
        final_trade_volume: total_trade_volume,
        error: String::new(),
    };

    let _ = tx.send(Ok(final_evt)).await;
}
