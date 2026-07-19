use std::collections::{HashMap, HashSet, VecDeque};

/// A single ingested item: free text to tokenize for trending topics, plus the
/// millisecond timestamp it arrived. Source-agnostic — the service layer is
/// responsible for turning a Bluesky post or a Wikipedia edit into one of these.
#[derive(Clone, Debug)]
pub struct IngestedItem {
    pub text: String,
    pub ts_ms: i64,
}

const DEFAULT_STOP_WORDS: &[&str] = &[
    "the", "and", "for", "that", "with", "this", "from", "have", "are", "was", "were", "you",
    "your", "http", "https", "www", "com", "org", "net", "not", "but", "his", "her", "she", "him",
];

/// Rolling-window word-frequency + throughput aggregator. Pure, synchronous, no I/O —
/// the service layer feeds it items as they arrive off the network.
pub struct Aggregator {
    window_ms: i64,
    stop_words: HashSet<&'static str>,
    counts: HashMap<String, u64>,
    history: VecDeque<(i64, Vec<String>)>,
    total_ingested: u64,
}

impl Aggregator {
    pub fn new(window_ms: i64) -> Self {
        Aggregator {
            window_ms,
            stop_words: DEFAULT_STOP_WORDS.iter().copied().collect(),
            counts: HashMap::new(),
            history: VecDeque::new(),
            total_ingested: 0,
        }
    }

    pub fn total_ingested(&self) -> u64 {
        self.total_ingested
    }

    /// Tokenizes `item.text`, folds the tokens into the rolling window, and evicts
    /// anything older than `window_ms` relative to `item.ts_ms`.
    pub fn ingest(&mut self, item: &IngestedItem) {
        self.total_ingested += 1;
        let tokens = tokenize(&item.text, &self.stop_words);
        for t in &tokens {
            *self.counts.entry(t.clone()).or_insert(0) += 1;
        }
        self.history.push_back((item.ts_ms, tokens));
        self.evict_before(item.ts_ms - self.window_ms);
    }

    fn evict_before(&mut self, cutoff_ms: i64) {
        while let Some((ts, _)) = self.history.front() {
            if *ts >= cutoff_ms {
                break;
            }
            let (_, tokens) = self.history.pop_front().unwrap();
            for t in tokens {
                if let Some(c) = self.counts.get_mut(&t) {
                    if *c <= 1 {
                        self.counts.remove(&t);
                    } else {
                        *c -= 1;
                    }
                }
            }
        }
    }

    pub fn trending(&self, top_n: usize) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self.counts.iter().map(|(k, c)| (k.clone(), *c)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(top_n);
        v
    }

    /// Items currently inside the rolling window, expressed as an approximate rate.
    pub fn rate_per_sec(&self) -> f64 {
        if self.window_ms <= 0 {
            return 0.0;
        }
        self.history.len() as f64 / (self.window_ms as f64 / 1000.0)
    }
}

fn tokenize(text: &str, stop_words: &HashSet<&'static str>) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() >= 3)
        .map(|w| w.to_lowercase())
        .filter(|w| !stop_words.contains(w.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trending_counts_most_frequent_token() {
        let mut agg = Aggregator::new(60_000);
        agg.ingest(&IngestedItem {
            text: "Rust rocks, Rust is fast".into(),
            ts_ms: 0,
        });
        agg.ingest(&IngestedItem {
            text: "rust everywhere".into(),
            ts_ms: 10,
        });
        let top = agg.trending(3);
        assert_eq!(top[0].0, "rust");
        assert_eq!(top[0].1, 3);
    }

    #[test]
    fn eviction_drops_stale_tokens_outside_window() {
        let mut agg = Aggregator::new(1_000);
        agg.ingest(&IngestedItem {
            text: "ancient river kingdom".into(),
            ts_ms: 0,
        });
        agg.ingest(&IngestedItem {
            text: "digital protocol".into(),
            ts_ms: 5_000,
        });
        let top = agg.trending(10);
        assert!(top.iter().all(|(w, _)| w != "ancient"));
        assert!(top.iter().all(|(w, _)| w != "river"));
        assert!(top.iter().all(|(w, _)| w != "kingdom"));
    }

    #[test]
    fn total_ingested_tracks_every_item() {
        let mut agg = Aggregator::new(60_000);
        for i in 0..5 {
            agg.ingest(&IngestedItem {
                text: "x".into(),
                ts_ms: i,
            });
        }
        assert_eq!(agg.total_ingested(), 5);
    }

    #[test]
    fn stop_words_and_short_tokens_are_excluded() {
        let mut agg = Aggregator::new(60_000);
        agg.ingest(&IngestedItem {
            text: "the cat and a dog with fastcars".into(),
            ts_ms: 0,
        });
        let top = agg.trending(10);
        assert!(top.iter().all(|(w, _)| w != "the"));
        assert!(top.iter().all(|(w, _)| w != "and"));
        assert!(top.iter().all(|(w, _)| w != "a"));
        assert!(top.iter().any(|(w, _)| w == "fastcars"));
    }
}
