use crate::engines::event_pulse::{Aggregator, IngestedItem};
use crate::event_pulse::{EventPulseRequest, PulseBatch, PulseEvent, TrendingTopic};
use crate::sources::{self, RawEvent};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const TRENDING_WINDOW_MS: i64 = 30_000;

#[derive(Default)]
pub struct EventPulseServiceImpl;

#[tonic::async_trait]
impl crate::event_pulse::event_pulse_service_server::EventPulseService for EventPulseServiceImpl {
    type RunEventPulseStream = ReceiverStream<Result<PulseBatch, Status>>;

    async fn run_event_pulse(
        &self,
        request: Request<EventPulseRequest>,
    ) -> Result<Response<Self::RunEventPulseStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            run_pulse(&req, &tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Aborts every spawned source task when dropped, so a disconnected client (or a
/// max_events cutoff) can never leave a Wikipedia/Bluesky connection running in
/// the background forever.
struct AbortOnDrop(Vec<tokio::task::JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for h in &self.0 {
            h.abort();
        }
    }
}

async fn send_error(tx: &mpsc::Sender<Result<PulseBatch, Status>>, msg: String) {
    let evt = PulseBatch {
        is_final: true,
        error: msg,
        ..Default::default()
    };
    let _ = tx.send(Ok(evt)).await;
}

async fn run_pulse(req: &EventPulseRequest, tx: &mpsc::Sender<Result<PulseBatch, Status>>) {
    let wanted: Vec<String> = if req.sources.is_empty() {
        vec!["bluesky".to_string(), "wikipedia".to_string()]
    } else {
        req.sources.clone()
    };
    for s in &wanted {
        if s != "bluesky" && s != "wikipedia" && s != "stackoverflow" {
            send_error(
                tx,
                format!(
                    "unknown source \"{s}\" \u{2014} expected \"bluesky\", \"wikipedia\", or \"stackoverflow\""
                ),
            )
            .await;
            return;
        }
    }

    let batch_ms = if req.batch_ms > 0 { req.batch_ms as u64 } else { 250 };
    let top_n = if req.trending_top_n > 0 {
        req.trending_top_n as usize
    } else {
        5
    };
    let max_events = if req.max_events > 0 {
        req.max_events as u64
    } else {
        u64::MAX
    };

    let (raw_tx, mut raw_rx) = mpsc::channel::<RawEvent>(512);
    let mut handles = Vec::new();

    if wanted.iter().any(|s| s == "wikipedia") {
        let t = raw_tx.clone();
        handles.push(tokio::spawn(async move {
            sources::wikipedia::run(t).await;
        }));
    }
    if wanted.iter().any(|s| s == "bluesky") {
        let t = raw_tx.clone();
        handles.push(tokio::spawn(async move {
            sources::bluesky::run(t).await;
        }));
    }
    if wanted.iter().any(|s| s == "stackoverflow") {
        let t = raw_tx.clone();
        handles.push(tokio::spawn(async move {
            sources::stackoverflow::run(t).await;
        }));
    }
    let _guard = AbortOnDrop(handles);
    drop(raw_tx);

    let mut aggregator = Aggregator::new(TRENDING_WINDOW_MS);
    let mut batch_events: Vec<PulseEvent> = Vec::new();
    let mut batch_seq: u64 = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(batch_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let start = Instant::now();

    loop {
        tokio::select! {
            maybe_raw = raw_rx.recv() => {
                let raw = match maybe_raw {
                    Some(r) => r,
                    None => break, // both source tasks ended (they normally retry forever)
                };
                aggregator.ingest(&IngestedItem { text: raw.summary.clone(), ts_ms: raw.ts_ms });
                batch_events.push(PulseEvent {
                    source: raw.source.to_string(),
                    kind: raw.kind,
                    summary: raw.summary,
                    ts_ms: raw.ts_ms,
                });
                if aggregator.total_ingested() >= max_events {
                    break;
                }
                // Eager flush so the UI lights up without waiting for the next timer tick.
                if batch_events.len() >= 8 {
                    batch_seq += 1;
                    let trending = aggregator
                        .trending(top_n)
                        .into_iter()
                        .map(|(topic, count)| TrendingTopic { topic, count })
                        .collect();
                    let elapsed_s = start.elapsed().as_secs_f64().max(0.001);
                    let batch = PulseBatch {
                        batch_seq,
                        events: std::mem::take(&mut batch_events),
                        trending,
                        current_rate_per_sec: aggregator.total_ingested() as f64 / elapsed_s,
                        total_ingested: aggregator.total_ingested(),
                        is_final: false,
                        error: String::new(),
                    };
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                batch_seq += 1;
                let trending = aggregator
                    .trending(top_n)
                    .into_iter()
                    .map(|(topic, count)| TrendingTopic { topic, count })
                    .collect();
                let elapsed_s = start.elapsed().as_secs_f64().max(0.001);
                let batch = PulseBatch {
                    batch_seq,
                    events: std::mem::take(&mut batch_events),
                    trending,
                    current_rate_per_sec: aggregator.total_ingested() as f64 / elapsed_s,
                    total_ingested: aggregator.total_ingested(),
                    is_final: false,
                    error: String::new(),
                };
                if tx.send(Ok(batch)).await.is_err() {
                    return; // client disconnected — AbortOnDrop cleans up the source tasks
                }
            }
        }
    }

    let final_batch = PulseBatch {
        batch_seq: batch_seq + 1,
        events: std::mem::take(&mut batch_events),
        trending: vec![],
        current_rate_per_sec: 0.0,
        total_ingested: aggregator.total_ingested(),
        is_final: true,
        error: String::new(),
    };
    let _ = tx.send(Ok(final_batch)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_pulse::event_pulse_service_server::EventPulseService;
    use tokio_stream::StreamExt as _;

    // These exercise the tonic handler in-process — no real socket, no external
    // network. `sources::wikipedia::run`/`sources::bluesky::run` are spawned as
    // usual and will just sit in their retry loop since this sandbox can't reach
    // the real endpoints, which is fine: these tests only assert on the framing/
    // batching/error-path behavior of `run_pulse` itself.

    #[tokio::test]
    async fn unknown_source_returns_a_terminal_error_batch_immediately() {
        let svc = EventPulseServiceImpl;
        let req = Request::new(EventPulseRequest {
            sources: vec!["not-a-real-source".to_string()],
            batch_ms: 50,
            trending_top_n: 5,
            max_events: 0,
        });

        let mut stream = svc.run_event_pulse(req).await.unwrap().into_inner();
        let first = stream.next().await.expect("expected one batch").unwrap();
        assert!(first.is_final);
        assert!(first.error.contains("not-a-real-source"));
    }

    #[tokio::test]
    async fn ticker_emits_non_final_batches_even_with_zero_ingested_events() {
        let svc = EventPulseServiceImpl;
        let req = Request::new(EventPulseRequest {
            sources: vec!["wikipedia".to_string()],
            batch_ms: 20,
            trending_top_n: 5,
            max_events: 0,
        });

        let mut stream = svc.run_event_pulse(req).await.unwrap().into_inner();
        let first = stream.next().await.expect("expected a batch").unwrap();
        assert!(!first.is_final);
        assert_eq!(first.total_ingested, 0);
        assert!(first.events.is_empty());
        // Dropping `stream` here simulates a client disconnect; the AbortOnDrop
        // guard inside run_pulse's spawned task must not panic on teardown.
    }
}
