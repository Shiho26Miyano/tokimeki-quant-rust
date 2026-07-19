use super::RawEvent;
use futures_util::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::tungstenite::Message;

const JETSTREAM_URL: &str =
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post";
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

/// Connects to Bluesky's public Jetstream firehose (JSON over WebSocket, no auth
/// required) and forwards normalized "new post" events onto `tx`. Runs until the
/// task is aborted by the caller, reconnecting on any stream error.
pub async fn run(tx: Sender<RawEvent>) {
    loop {
        if let Err(e) = connect_and_stream(&tx).await {
            eprintln!("[bluesky] stream error: {e}, retrying in {RETRY_DELAY:?}");
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

async fn connect_and_stream(tx: &Sender<RawEvent>) -> Result<(), String> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(JETSTREAM_URL)
        .await
        .map_err(|e| e.to_string())?;
    let (_write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        let msg = msg.map_err(|e| e.to_string())?;
        if let Message::Text(text) = msg {
            if let Some(evt) = parse_message(text.as_str()) {
                if tx.send(evt).await.is_err() {
                    return Ok(()); // receiver gone, nothing left to do
                }
            }
        }
    }
    Err("stream ended".to_string())
}

fn parse_message(text: &str) -> Option<RawEvent> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    if v.get("kind").and_then(|x| x.as_str()) != Some("commit") {
        return None;
    }
    let commit = v.get("commit")?;
    if commit.get("operation").and_then(|x| x.as_str()) != Some("create") {
        return None;
    }
    let record = commit.get("record")?;
    let text_content = record.get("text").and_then(|x| x.as_str()).unwrap_or("");
    if text_content.trim().is_empty() {
        return None;
    }

    let did = v.get("did").and_then(|x| x.as_str()).unwrap_or("?");
    let handle_fragment = did.rsplit(':').next().unwrap_or(did);
    let short_id: String = handle_fragment.chars().take(10).collect();

    Some(RawEvent {
        source: "bluesky",
        kind: "post".to_string(),
        summary: format!("@{short_id}: {}", truncate(text_content, 120)),
        ts_ms: super::now_ms(),
    })
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let head: String = s.chars().take(max_chars).collect();
        format!("{head}\u{2026}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_post_json(text: &str) -> String {
        format!(
            "{{\"did\":\"did:plc:abc123xyz\",\"time_us\":1234567890,\"kind\":\"commit\",\"commit\":{{\"rev\":\"r1\",\"operation\":\"create\",\"collection\":\"app.bsky.feed.post\",\"rkey\":\"k1\",\"record\":{{\"text\":\"{text}\",\"createdAt\":\"2026-01-01T00:00:00Z\"}},\"cid\":\"c1\"}}}}"
        )
    }

    #[test]
    fn parses_a_realistic_create_post_commit() {
        let msg = create_post_json("hello world");
        let evt = parse_message(&msg).expect("should parse");
        assert_eq!(evt.source, "bluesky");
        assert_eq!(evt.kind, "post");
        assert!(evt.summary.starts_with('@'));
        assert!(evt.summary.contains("hello world"));
    }

    #[test]
    fn identity_events_are_ignored() {
        let msg = "{\"did\":\"did:plc:abc\",\"time_us\":1,\"kind\":\"identity\",\"identity\":{}}";
        assert!(parse_message(msg).is_none());
    }

    #[test]
    fn delete_operations_are_ignored() {
        let msg = "{\"did\":\"did:plc:abc\",\"time_us\":1,\"kind\":\"commit\",\"commit\":{\"rev\":\"r\",\"operation\":\"delete\",\"collection\":\"app.bsky.feed.post\",\"rkey\":\"k\"}}";
        assert!(parse_message(msg).is_none());
    }

    #[test]
    fn empty_post_text_is_ignored() {
        let msg = create_post_json("");
        assert!(parse_message(&msg).is_none());
    }

    #[test]
    fn long_text_gets_truncated_with_ellipsis() {
        let long_text = "a".repeat(200);
        let msg = create_post_json(&long_text);
        let evt = parse_message(&msg).expect("should parse");
        assert!(evt.summary.contains('\u{2026}'));
        assert!(evt.summary.chars().count() < 200);
    }

    #[test]
    fn malformed_json_is_ignored_not_panicking() {
        assert!(parse_message("{not valid json").is_none());
    }
}
