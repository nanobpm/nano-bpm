//! Durable cockpit conversations (§10 — the pilot ↔ droid pairing channel).
//!
//! The experiment narrative the cockpit shows was, until now, *reconstructed* on
//! every render from the Nano instance's variables + trace. That snapshot reflects
//! only the latest round and **loses the human's words entirely** — the BPMN keeps
//! the `decision` enum, never the note the pilot wrote. This module gives ProcessOS
//! its own append-only conversation log so the pairing is a real, persisted dialogue:
//! the droid writes a turn per round, the pilot writes notes/decisions, and both
//! survive a restart.
//!
//! Storage is intentionally dependency-light: one JSON-Lines file per experiment
//! under the data dir, append-only (crash-friendly), with an in-memory index in
//! front so reads don't touch disk on the hot path.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// One turn in an experiment's conversation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// Who spoke: `pilot` (the human), `droid` (the LLM), or `engine` (the system).
    pub role: String,
    /// The turn's text.
    pub text: String,
    /// Milliseconds since the epoch when the turn was recorded.
    pub ts: u64,
    /// The optimization round (iteration) this turn belongs to, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round: Option<i64>,
}

/// An append-only, file-backed conversation store keyed by experiment instance key.
pub struct ConversationStore {
    dir: PathBuf,
    mem: RwLock<BTreeMap<String, Vec<Message>>>,
}

impl ConversationStore {
    /// Open (creating if needed) a store rooted at `dir`. A failure to create the
    /// directory is logged and the store degrades to memory-only rather than taking
    /// the server down — persistence is a feature, not a hard dependency.
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        if let Err(e) = fs::create_dir_all(&dir) {
            tracing::warn!(dir = %dir.display(), error = %e, "conversation store: dir create failed; memory-only");
        }
        Self {
            dir,
            mem: RwLock::new(BTreeMap::new()),
        }
    }

    /// Append a turn to an experiment's conversation and persist it. Returns the
    /// stored message (with its assigned timestamp).
    pub fn append(&self, key: &str, role: &str, text: &str, round: Option<i64>) -> Message {
        let msg = Message {
            role: role.to_string(),
            text: text.to_string(),
            ts: now_ms(),
            round,
        };
        // Make sure the on-disk history is loaded before we extend it in memory, so a
        // process that restarts mid-experiment keeps one contiguous log.
        self.ensure_loaded(key);
        if let Ok(mut mem) = self.mem.write() {
            mem.entry(key.to_string()).or_default().push(msg.clone());
        }
        if let Err(e) = self.persist(key, &msg) {
            tracing::warn!(key = %key, error = %e, "conversation store: persist failed");
        }
        msg
    }

    /// Read an experiment's full conversation in chronological order. Empty when the
    /// experiment has no persisted turns yet (the caller falls back to reconstruction).
    pub fn read(&self, key: &str) -> Vec<Message> {
        self.ensure_loaded(key);
        self.mem
            .read()
            .ok()
            .and_then(|m| m.get(key).cloned())
            .unwrap_or_default()
    }

    /// Lazily hydrate one experiment's history from disk into the in-memory index.
    fn ensure_loaded(&self, key: &str) {
        if self
            .mem
            .read()
            .map(|m| m.contains_key(key))
            .unwrap_or(false)
        {
            return;
        }
        let loaded = self.load_from_disk(key);
        if let Ok(mut mem) = self.mem.write() {
            mem.entry(key.to_string()).or_insert(loaded);
        }
    }

    fn load_from_disk(&self, key: &str) -> Vec<Message> {
        let path = self.path_for(key);
        let Ok(body) = fs::read_to_string(&path) else {
            return Vec::new();
        };
        body.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<Message>(l).ok())
            .collect()
    }

    fn persist(&self, key: &str, msg: &Message) -> std::io::Result<()> {
        let line = serde_json::to_string(msg).map_err(std::io::Error::other)?;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path_for(key))?;
        writeln!(f, "{line}")
    }

    /// One file per experiment; the key is sanitised so it can never escape `dir`.
    fn path_for(&self, key: &str) -> PathBuf {
        let safe: String = key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.dir.join(format!("{safe}.jsonl"))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Resolve the conversation data directory from `PROCESSOS_DATA_DIR`, defaulting to
/// `./.processos-data` beside the running server.
pub fn data_dir_from_env() -> PathBuf {
    std::env::var("PROCESSOS_DATA_DIR")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(".processos-data").to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        // nextest runs each test in its own process, so a process-global counter
        // resets to 0 in every test and now_ms() alone collides when two test
        // processes land in the same millisecond. Include the PID and a
        // nanosecond clock so every call yields a distinct dir across processes.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!(
            "processos-convo-{}-{}-{}",
            std::process::id(),
            nanos,
            n
        ));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn append_then_read_round_trips_in_order() {
        let dir = tmp();
        let store = ConversationStore::open(&dir);
        store.append("42", "engine", "forked from replayProc", Some(0));
        store.append("42", "droid", "round 1: best is Parallelize", Some(1));
        store.append("42", "pilot", "focus on tail latency next round", Some(1));

        let log = store.read("42");
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].role, "engine");
        assert_eq!(log[2].role, "pilot");
        assert_eq!(log[2].text, "focus on tail latency next round");
        assert!(log[0].ts <= log[2].ts);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn history_survives_a_reopen() {
        let dir = tmp();
        {
            let store = ConversationStore::open(&dir);
            store.append("7", "pilot", "iterate, but keep summarize", Some(2));
        }
        // A fresh store (simulating a server restart) reloads the persisted log.
        let store = ConversationStore::open(&dir);
        let log = store.read("7");
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].text, "iterate, but keep summarize");
        assert_eq!(log[0].round, Some(2));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unknown_experiment_reads_empty() {
        let dir = tmp();
        let store = ConversationStore::open(&dir);
        assert!(store.read("nope").is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn keys_cannot_escape_the_data_dir() {
        let dir = tmp();
        let store = ConversationStore::open(&dir);
        store.append("../../etc/passwd", "pilot", "x", None);
        // The traversal characters are sanitised away to a flat filename under dir.
        let entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(entries.iter().all(|n| !n.contains('/')));
        assert!(entries.iter().any(|n| n.ends_with(".jsonl")));
        let _ = fs::remove_dir_all(&dir);
    }
}
