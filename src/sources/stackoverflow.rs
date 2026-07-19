use super::RawEvent;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::{HashSet, VecDeque};
use tokio::sync::mpsc::Sender;

const FEED_URL: &str = "https://stackoverflow.com/feeds";
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(10);
const SEEN_CAPACITY: usize = 500;

/// Polls Stack Overflow's public "recent questions" feed (Atom XML, no auth
/// required) and forwards normalized "new question" events onto `tx`. Unlike
/// Wikipedia/Bluesky there is no push transport for this feed, so instead of a
/// persistent connection this source re-fetches the feed on a fixed interval and
/// tracks already-seen entry ids (the feed re-lists the same ~30 newest
/// questions on every poll) so nothing is emitted twice.
pub async fn run(tx: Sender<RawEvent>) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut seen_order: VecDeque<String> = VecDeque::new();

    loop {
        match fetch_and_parse().await {
            Ok(entries) => {
                for (id, summary) in entries {
                    if seen.contains(&id) {
                        continue;
                    }
                    if seen_order.len() >= SEEN_CAPACITY {
                        if let Some(oldest) = seen_order.pop_front() {
                            seen.remove(&oldest);
                        }
                    }
                    seen.insert(id.clone());
                    seen_order.push_back(id);

                    let evt = RawEvent {
                        source: "stackoverflow",
                        kind: "question".to_string(),
                        summary,
                        ts_ms: super::now_ms(),
                    };
                    if tx.send(evt).await.is_err() {
                        return; // receiver gone, nothing left to do
                    }
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Err(e) => {
                eprintln!("[stackoverflow] feed error: {e}, retrying in {RETRY_DELAY:?}");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

async fn fetch_and_parse() -> Result<Vec<(String, String)>, String> {
    let client = reqwest::Client::builder()
        .user_agent("Tokimeki-EventPulse/1.0 (https://github.com/Shiho26Miyano/tokimeki-quant-rust; educational demo)")
        .build()
        .map_err(|e| e.to_string())?;
    let body = client
        .get(FEED_URL)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;

    Ok(parse_feed(&body))
}

/// Parses an Atom feed body into `(entry_id, "title [tag]")` pairs. Malformed
/// XML or missing fields are skipped rather than treated as fatal, mirroring
/// how `wikipedia::parse_frame` / `bluesky::parse_message` degrade.
fn parse_feed(xml: &str) -> Vec<(String, String)> {
    let mut reader = Reader::from_str(xml);

    let mut out = Vec::new();
    let mut in_entry = false;
    let mut text_target: Option<&'static str> = None;
    let mut cur_id: Option<String> = None;
    let mut cur_title: Option<String> = None;
    let mut cur_tag: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => match e.name().as_ref() {
                b"entry" => {
                    in_entry = true;
                    cur_id = None;
                    cur_title = None;
                    cur_tag = None;
                }
                b"id" if in_entry => text_target = Some("id"),
                b"title" if in_entry => text_target = Some("title"),
                b"category" if in_entry && cur_tag.is_none() => {
                    if let Some(attr) = e
                        .attributes()
                        .filter_map(|a| a.ok())
                        .find(|a| a.key.as_ref() == b"term")
                    {
                        cur_tag = Some(String::from_utf8_lossy(&attr.value).trim().to_string());
                    }
                }
                _ => {}
            },
            Ok(Event::Text(t)) => {
                if let Some(target) = text_target {
                    let text = t.unescape().unwrap_or_default().trim().to_string();
                    match target {
                        "id" => cur_id = Some(text),
                        "title" => cur_title = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => match e.name().as_ref() {
                b"id" | b"title" => text_target = None,
                b"entry" => {
                    in_entry = false;
                    if let (Some(id), Some(title)) = (cur_id.take(), cur_title.take()) {
                        let tag_suffix = cur_tag
                            .take()
                            .map(|t| format!(" [{t}]"))
                            .unwrap_or_default();
                        out.push((id, format!("{title}{tag_suffix}")));
                    }
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_FEED: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title type="text">Recent Questions - Stack Overflow</title>
  <entry>
    <id>https://stackoverflow.com/q/111</id>
    <title type="text">How do I parse Atom feeds in Rust?</title>
    <category scheme="https://stackoverflow.com/tags" term="rust"/>
    <link rel="alternate" href="https://stackoverflow.com/questions/111/how-do-i-parse"/>
  </entry>
  <entry>
    <id>https://stackoverflow.com/q/222</id>
    <title type="text">Why is my tokio task not yielding?</title>
    <category scheme="https://stackoverflow.com/tags" term="tokio"/>
  </entry>
</feed>"#;

    #[test]
    fn parses_entries_with_id_title_and_tag() {
        let entries = parse_feed(SAMPLE_FEED);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "https://stackoverflow.com/q/111");
        assert!(entries[0].1.contains("How do I parse Atom feeds in Rust?"));
        assert!(entries[0].1.contains("[rust]"));
        assert_eq!(entries[1].0, "https://stackoverflow.com/q/222");
        assert!(entries[1].1.contains("[tokio]"));
    }

    #[test]
    fn empty_feed_yields_no_entries() {
        let entries = parse_feed("<feed xmlns=\"http://www.w3.org/2005/Atom\"></feed>");
        assert!(entries.is_empty());
    }

    #[test]
    fn entry_missing_title_is_skipped() {
        let feed = r#"<feed xmlns="http://www.w3.org/2005/Atom">
          <entry><id>https://stackoverflow.com/q/999</id></entry>
        </feed>"#;
        assert!(parse_feed(feed).is_empty());
    }

    #[test]
    fn malformed_xml_does_not_panic() {
        let entries = parse_feed("<feed><entry><id>oops");
        assert!(entries.is_empty());
    }
}
