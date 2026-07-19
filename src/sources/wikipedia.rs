use super::RawEvent;
use futures_util::StreamExt;
use tokio::sync::mpsc::Sender;

const STREAM_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

/// Connects to Wikipedia's public EventStreams `recentchange` feed (Server-Sent
/// Events, no auth required) and forwards normalized edit events onto `tx`. Runs
/// until the task is aborted by the caller, reconnecting on any stream error.
pub async fn run(tx: Sender<RawEvent>) {
    loop {
        if let Err(e) = connect_and_stream(&tx).await {
            eprintln!("[wikipedia] stream error: {e}, retrying in {RETRY_DELAY:?}");
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

async fn connect_and_stream(tx: &Sender<RawEvent>) -> Result<(), String> {
    // Wikimedia EventStreams requires a descriptive User-Agent; bare clients get 403.
    let client = reqwest::Client::builder()
        .user_agent("Tokimeki-EventPulse/1.0 (https://github.com/Shiho26Miyano/tokimeki-quant-rust; educational demo)")
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .get(STREAM_URL)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?;

    let mut stream = resp.bytes_stream();
    let mut buf = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        buf.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buf.find("\n\n") {
            let frame = buf[..pos].to_string();
            buf.drain(..pos + 2);
            if let Some(evt) = parse_frame(&frame) {
                if tx.send(evt).await.is_err() {
                    return Ok(()); // receiver gone, nothing left to do
                }
            }
        }
    }
    Err("stream ended".to_string())
}

fn parse_frame(frame: &str) -> Option<RawEvent> {
    let data_line = frame.lines().find(|l| l.starts_with("data:"))?;
    let json_str = data_line.trim_start_matches("data:").trim();
    let v: serde_json::Value = serde_json::from_str(json_str).ok()?;

    let wiki = v.get("wiki").and_then(|x| x.as_str()).unwrap_or("?");
    let title = v.get("title").and_then(|x| x.as_str()).unwrap_or("(untitled)");
    let user = v.get("user").and_then(|x| x.as_str()).unwrap_or("anon");
    let comment = v
        .get("comment")
        .and_then(|x| x.as_str())
        .filter(|c| !c.is_empty())
        .unwrap_or("(no summary)");

    Some(RawEvent {
        source: "wikipedia",
        kind: "edit".to_string(),
        summary: format!("[{wiki}] {title} \u{2014} {user}: {comment}"),
        ts_ms: super::now_ms(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_realistic_recentchange_frame() {
        let frame = "event: message\nid: 123\ndata: {\"$schema\":\"/mediawiki/recentchange/1.0.0\",\"wiki\":\"enwiki\",\"title\":\"Example page\",\"user\":\"SomeUser\",\"comment\":\"Fixed typo\",\"type\":\"edit\"}";
        let evt = parse_frame(frame).expect("should parse");
        assert_eq!(evt.source, "wikipedia");
        assert_eq!(evt.kind, "edit");
        assert_eq!(evt.summary, "[enwiki] Example page \u{2014} SomeUser: Fixed typo");
    }

    #[test]
    fn missing_optional_fields_fall_back_to_placeholders() {
        let frame = "event: message\ndata: {\"wiki\":\"enwiki\"}";
        let evt = parse_frame(frame).expect("should parse even with missing fields");
        assert!(evt.summary.contains("(untitled)"));
        assert!(evt.summary.contains("anon"));
        assert!(evt.summary.contains("(no summary)"));
    }

    #[test]
    fn heartbeat_comment_frame_without_data_line_is_ignored() {
        let frame = ": heartbeat";
        assert!(parse_frame(frame).is_none());
    }

    #[test]
    fn malformed_json_is_ignored_not_panicking() {
        let frame = "data: {not valid json";
        assert!(parse_frame(frame).is_none());
    }
}
