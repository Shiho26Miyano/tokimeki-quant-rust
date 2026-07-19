pub mod bluesky;
pub mod stackoverflow;
pub mod wikipedia;

/// A source-agnostic event, normalized before it reaches the aggregator/service layer.
#[derive(Clone, Debug)]
pub struct RawEvent {
    pub source: &'static str, // "bluesky" | "wikipedia" | "stackoverflow"
    pub kind: String,
    pub summary: String,
    pub ts_ms: i64,
}

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
