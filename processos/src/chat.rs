//! Interactive cockpit chat sessions — durable, multi-turn droid conversations
//! bound to a workspace's trace dataset.
//!
//! The one-shot investigation ([`crate::investigate::run_investigation`]) answers a
//! fixed question and forgets everything. A *cockpit chat* instead lets the operator
//! converse with the droid over many turns, where each turn can issue SQL/Python tool
//! calls and the model keeps its full working memory. To make that real we persist the
//! **entire model transcript** ([`Msg`], including tool calls and their results) per
//! `(workspace, process)` key, so a turn resumes exactly where the last one left off —
//! even across a server restart.
//!
//! Storage mirrors [`crate::conversation::ConversationStore`] in spirit (one file per
//! key, sanitised so it can't escape the data dir, degrades to memory-only on a dir
//! failure) but stores a *whole session* (overwrite-on-save) rather than appending
//! single human-readable turns, because the model transcript is rewritten in full each
//! turn.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::agent::{Msg, ToolCall};

/// Sentinel that marks an assistant message as a **Pair AI** contribution in the persisted
/// transcript. Format: `"{PAIR_MARK}{name}{PAIR_MARK}{answer}"`. The control char (SOH) never
/// occurs in model prose, so detection is unambiguous and the marker stays invisible if any
/// path renders the raw text. See [`mark_pair`] / [`render_view`].
pub const PAIR_MARK: &str = "\u{1}";

/// Wrap a Pair AI reviewer's `answer` (authored by reviewer `name`) for persistence in the
/// primary transcript, so it renders with provenance and is carried into the next turn.
pub fn mark_pair(name: &str, answer: &str) -> String {
    format!("{PAIR_MARK}{name}{PAIR_MARK}{answer}")
}

/// If `text` is a [`mark_pair`]-encoded Pair AI message, return `(name, answer)`.
fn unmark_pair(text: &str) -> Option<(String, String)> {
    let rest = text.strip_prefix(PAIR_MARK)?;
    let idx = rest.find(PAIR_MARK)?;
    Some((
        rest[..idx].to_string(),
        rest[idx + PAIR_MARK.len()..].to_string(),
    ))
}

/// Monotonic suffix so two sessions created in the same millisecond get distinct ids.
static SESSION_SEQ: AtomicU64 = AtomicU64::new(0);

/// A persisted chat session: a named, timestamped droid conversation. A `(workspace,
/// process)` dataset can carry several, so the operator can run parallel investigations.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChatSession {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub created: u64,
    #[serde(default)]
    pub updated: u64,
    #[serde(default)]
    pub messages: Vec<Msg>,
    /// Epoch-ms timestamp per rendered turn (aligned by index to [`render_view`]).
    #[serde(default)]
    pub stamps: Vec<u64>,
    /// The persona (standing system prompt) this session uses, set on its first turn and
    /// fixed thereafter (the system prompt is baked into the transcript). Empty until then.
    #[serde(default)]
    pub persona: String,
    /// The distinct LLM model labels that have driven a turn in this session (insertion order).
    /// Recorded per send so the operator can see which model(s) produced a conversation.
    #[serde(default)]
    pub models: Vec<String>,
    /// The model label that produced each rendered turn, aligned by index to [`render_view`]
    /// (empty string for operator/user turns). Lets historical droid bubbles keep the name of
    /// the model that actually answered, rather than re-labelling with the current selection.
    #[serde(default)]
    pub turn_models: Vec<String>,
    /// Provenance marker. Empty for a native investigation; `"imported"` for a session
    /// reconstructed from a shared psychological-trace zip (rendered with a special icon).
    #[serde(default)]
    pub origin: String,
    /// Human-readable provenance summary for an imported session (e.g. the original
    /// dataset/model and processos version), shown as the icon's tooltip. `None` when native.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_from: Option<String>,
}

/// Lightweight session descriptor for the tab list (no transcript).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMeta {
    pub id: String,
    pub name: String,
    pub created: u64,
    pub updated: u64,
    pub turns: usize,
    /// The persona (standing system prompt) bound to this session, if its first turn has run.
    #[serde(default)]
    pub persona: String,
    /// Distinct LLM model labels that have driven a turn in this session (insertion order).
    #[serde(default)]
    pub models: Vec<String>,
    /// Provenance marker (`""` native, `"imported"` for a shared psychological trace).
    #[serde(default)]
    pub origin: String,
    /// Human-readable provenance summary for an imported session (icon tooltip).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_from: Option<String>,
}

/// On-disk shape: all sessions for one `(workspace, process)` key in a single file.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ChatFile {
    #[serde(default)]
    sessions: Vec<ChatSession>,
}

/// The pre-multisession on-disk shape (one conversation per key). Read for migration.
#[derive(Deserialize)]
struct LegacyChatFile {
    #[serde(default)]
    messages: Vec<Msg>,
    #[serde(default)]
    updated: u64,
}

/// A file-backed multi-session chat store keyed by a sanitised `(workspace, process)` key.
/// Each key owns a list of [`ChatSession`]s persisted together (overwrite-on-save).
pub struct ChatStore {
    dir: PathBuf,
    mem: RwLock<HashMap<String, Vec<ChatSession>>>,
}

impl ChatStore {
    /// Open (creating if needed) a store rooted at `dir`. A dir-create failure degrades
    /// to memory-only rather than failing the server.
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        if let Err(e) = fs::create_dir_all(&dir) {
            tracing::warn!(dir = %dir.display(), error = %e, "chat store: dir create failed; memory-only");
        }
        Self {
            dir,
            mem: RwLock::new(HashMap::new()),
        }
    }

    /// List a key's sessions (metadata only), newest activity first.
    pub fn list(&self, key: &str) -> Vec<SessionMeta> {
        self.ensure_loaded(key);
        let mut metas: Vec<SessionMeta> = self
            .mem
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(|sessions| {
                sessions
                    .iter()
                    .map(|s| SessionMeta {
                        id: s.id.clone(),
                        name: s.name.clone(),
                        created: s.created,
                        updated: s.updated,
                        turns: render_view(&s.messages).len(),
                        persona: s.persona.clone(),
                        models: s.models.clone(),
                        origin: s.origin.clone(),
                        imported_from: s.imported_from.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        metas.sort_by_key(|b| std::cmp::Reverse(b.updated));
        metas
    }

    /// Create a new (empty) session for `key`, returning it.
    pub fn create(&self, key: &str, name: Option<String>) -> ChatSession {
        self.ensure_loaded(key);
        let now = now_ms();
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let sessions = guard.entry(key.to_string()).or_default();
        let name = name.map(|n| n.trim().to_string()).filter(|n| !n.is_empty());
        let session = ChatSession {
            id: new_session_id(),
            name: name.unwrap_or_else(|| default_name(sessions.len() + 1)),
            created: now,
            updated: now,
            messages: Vec::new(),
            stamps: Vec::new(),
            persona: String::new(),
            models: Vec::new(),
            turn_models: Vec::new(),
            origin: String::new(),
            imported_from: None,
        };
        sessions.push(session.clone());
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
        session
    }

    /// Reconstruct a shared psychological trace as a fresh, `"imported"`-origin session under
    /// `key`. Assigns a new id (avoiding collisions with the recipient's own sessions), keeps the
    /// original transcript/stamps/persona/models for faithful replay, and records a provenance
    /// summary for the icon tooltip. Returns the stored session.
    #[allow(clippy::too_many_arguments)]
    pub fn import_session(
        &self,
        key: &str,
        name: &str,
        messages: Vec<Msg>,
        stamps: Vec<u64>,
        persona: String,
        models: Vec<String>,
        turn_models: Vec<String>,
        imported_from: Option<String>,
    ) -> ChatSession {
        self.ensure_loaded(key);
        let now = now_ms();
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let sessions = guard.entry(key.to_string()).or_default();
        let name = name.trim();
        let name = if name.is_empty() {
            default_name(sessions.len() + 1)
        } else {
            name.to_string()
        };
        let session = ChatSession {
            id: new_session_id(),
            name,
            created: now,
            updated: now,
            messages,
            stamps,
            persona,
            models,
            turn_models,
            origin: "imported".to_string(),
            imported_from,
        };
        sessions.push(session.clone());
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
        session
    }

    /// Load a session by id (its full transcript + stamps).
    pub fn get(&self, key: &str, session_id: &str) -> Option<ChatSession> {
        self.ensure_loaded(key);
        self.mem
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .and_then(|s| s.iter().find(|s| s.id == session_id).cloned())
    }

    /// Replace a session's transcript + stamps, bumping `updated`. Creates the session if
    /// the id is unknown (e.g. a race) so a turn is never lost.
    pub fn save(&self, key: &str, session_id: &str, messages: Vec<Msg>, stamps: Vec<u64>) {
        self.ensure_loaded(key);
        let now = now_ms();
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let sessions = guard.entry(key.to_string()).or_default();
        if let Some(s) = sessions.iter_mut().find(|s| s.id == session_id) {
            s.messages = messages;
            s.stamps = stamps;
            s.updated = now;
        } else {
            let n = sessions.len() + 1;
            sessions.push(ChatSession {
                id: session_id.to_string(),
                name: default_name(n),
                created: now,
                updated: now,
                messages,
                stamps,
                persona: String::new(),
                models: Vec::new(),
                turn_models: Vec::new(),
                origin: String::new(),
                imported_from: None,
            });
        }
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
    }

    /// Record that `model` drove a turn in a session (deduplicated, insertion-ordered). Creates
    /// the session if the id is unknown so the first turn's model is never lost.
    pub fn record_model(&self, key: &str, session_id: &str, model: &str) {
        let model = model.trim();
        if model.is_empty() {
            return;
        }
        self.ensure_loaded(key);
        let now = now_ms();
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let sessions = guard.entry(key.to_string()).or_default();
        match sessions.iter_mut().find(|s| s.id == session_id) {
            Some(s) => {
                if s.models.iter().any(|m| m == model) {
                    return; // already recorded — nothing to persist
                }
                s.models.push(model.to_string());
            }
            None => sessions.push(ChatSession {
                id: session_id.to_string(),
                name: default_name(sessions.len() + 1),
                created: now,
                updated: now,
                messages: Vec::new(),
                stamps: Vec::new(),
                persona: String::new(),
                models: vec![model.to_string()],
                turn_models: Vec::new(),
                origin: String::new(),
                imported_from: None,
            }),
        }
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
    }

    /// Persist the per-turn model attribution for a session (aligned by index to the rendered
    /// turns). No-op if the id is unknown. Kept separate from [`save`] so the existing save call
    /// sites are untouched; callers compute it with [`extend_turn_models`] after saving.
    pub fn set_turn_models(&self, key: &str, session_id: &str, turn_models: Vec<String>) {
        self.ensure_loaded(key);
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let Some(sessions) = guard.get_mut(key) else {
            return;
        };
        let Some(s) = sessions.iter_mut().find(|s| s.id == session_id) else {
            return;
        };
        if s.turn_models == turn_models {
            return; // unchanged — skip the disk write
        }
        s.turn_models = turn_models;
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
    }

    /// Rename a session. Returns false when the id is unknown.
    pub fn rename(&self, key: &str, session_id: &str, name: &str) -> bool {
        self.ensure_loaded(key);
        let name = name.trim();
        if name.is_empty() {
            return false;
        }
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let Some(sessions) = guard.get_mut(key) else {
            return false;
        };
        let Some(s) = sessions.iter_mut().find(|s| s.id == session_id) else {
            return false;
        };
        s.name = name.to_string();
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
        true
    }

    /// Bind a session to a persona (its standing system prompt) on its first turn. No-op if the
    /// session already has a persona recorded (it is fixed once the conversation has started) or
    /// the id is unknown. Creates the session if missing so the first turn never loses it.
    pub fn set_persona(&self, key: &str, session_id: &str, persona: &str) {
        self.ensure_loaded(key);
        let persona = persona.trim();
        if persona.is_empty() {
            return;
        }
        let now = now_ms();
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let sessions = guard.entry(key.to_string()).or_default();
        match sessions.iter_mut().find(|s| s.id == session_id) {
            Some(s) => {
                if !s.persona.is_empty() {
                    return; // already bound — persona is fixed for the session
                }
                s.persona = persona.to_string();
            }
            None => sessions.push(ChatSession {
                id: session_id.to_string(),
                name: default_name(sessions.len() + 1),
                created: now,
                updated: now,
                messages: Vec::new(),
                stamps: Vec::new(),
                persona: persona.to_string(),
                models: Vec::new(),
                turn_models: Vec::new(),
                origin: String::new(),
                imported_from: None,
            }),
        }
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
    }

    /// Delete one session. Returns false when the id is unknown.
    pub fn delete(&self, key: &str, session_id: &str) -> bool {
        self.ensure_loaded(key);
        let mut guard = self.mem.write().unwrap_or_else(|e| e.into_inner());
        let Some(sessions) = guard.get_mut(key) else {
            return false;
        };
        let before = sessions.len();
        sessions.retain(|s| s.id != session_id);
        if sessions.len() == before {
            return false;
        }
        let snapshot = sessions.clone();
        drop(guard);
        self.persist(key, &snapshot);
        true
    }

    /// Clear every session under a key (used by the legacy "reset" path).
    pub fn clear(&self, key: &str) {
        self.mem
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.to_string(), Vec::new());
        let _ = fs::remove_file(self.path_for(key));
    }

    fn ensure_loaded(&self, key: &str) {
        // Recover a poisoned lock rather than skipping the load: the in-memory
        // sessions are plain data (no invariant a panic can violate), and
        // skipping here would let a later `create`/`save` start from an empty
        // vector and overwrite the persisted file, losing saved history.
        if self
            .mem
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(key)
        {
            return;
        }
        let loaded = self.load_from_disk(key).unwrap_or_default();
        self.mem
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key.to_string())
            .or_insert(loaded);
    }

    /// Load a key's sessions from disk, migrating the legacy single-conversation shape.
    fn load_from_disk(&self, key: &str) -> Option<Vec<ChatSession>> {
        let body = fs::read_to_string(self.path_for(key)).ok()?;
        if let Ok(file) = serde_json::from_str::<ChatFile>(&body) {
            if !file.sessions.is_empty() {
                return Some(file.sessions);
            }
        }
        // Migrate the legacy `{messages, updated}` file into a single named session.
        let legacy: LegacyChatFile = serde_json::from_str(&body).ok()?;
        if legacy.messages.is_empty() {
            return Some(Vec::new());
        }
        let updated = if legacy.updated > 0 {
            legacy.updated
        } else {
            now_ms()
        };
        Some(vec![ChatSession {
            id: new_session_id(),
            name: default_name(1),
            created: updated,
            updated,
            messages: legacy.messages,
            stamps: Vec::new(),
            persona: String::new(),
            models: Vec::new(),
            turn_models: Vec::new(),
            origin: String::new(),
            imported_from: None,
        }])
    }

    fn persist(&self, key: &str, sessions: &[ChatSession]) {
        let file = ChatFile {
            sessions: sessions.to_vec(),
        };
        match serde_json::to_string(&file) {
            Ok(body) => {
                if let Err(e) = fs::write(self.path_for(key), body) {
                    tracing::warn!(key = %key, error = %e, "chat store: persist failed");
                }
            }
            Err(e) => tracing::warn!(key = %key, error = %e, "chat store: serialise failed"),
        }
    }

    /// One file per `(workspace, process)`; the key is sanitised so it can never escape
    /// `dir`.
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
        self.dir.join(format!("chat-{safe}.json"))
    }
}

/// Durable store for the per-session **Debug** payloads: the exact model request bodies
/// sent during a chat session's most recent turn (one entry per round), powering the
/// cockpit's per-chat "Debug" tab and the psychological-trace export. Mirrors [`ChatStore`]
/// — one JSON file per session `cancel_key`, sanitised so it can't escape `dir`, degrading
/// to memory-only on a dir failure. Unlike the former in-memory-only buffer, these survive a
/// server restart, so the operator can still inspect what was sent after ProcessOS bounces.
pub struct DebugStore {
    dir: PathBuf,
    mem: RwLock<HashMap<String, Vec<serde_json::Value>>>,
}

#[derive(Serialize, Deserialize, Default)]
struct DebugFile {
    #[serde(default)]
    requests: Vec<serde_json::Value>,
}

impl DebugStore {
    /// Open (creating if needed) a store rooted at `dir`. A dir-create failure degrades to
    /// memory-only rather than failing the server.
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        if let Err(e) = fs::create_dir_all(&dir) {
            tracing::warn!(dir = %dir.display(), error = %e, "debug store: dir create failed; memory-only");
        }
        Self {
            dir,
            mem: RwLock::new(HashMap::new()),
        }
    }

    /// Replace the stored request payloads for a session (`cancel_key` = `key::session_id`),
    /// writing through to disk so they outlive a restart.
    pub fn save(&self, cancel_key: &str, requests: Vec<serde_json::Value>) {
        self.mem
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(cancel_key.to_string(), requests.clone());
        self.persist(cancel_key, &requests);
    }

    /// Load a session's request payloads: the in-memory copy if present, else the persisted
    /// file (cached back into memory). Empty when nothing has been captured for the session.
    pub fn load(&self, cancel_key: &str) -> Vec<serde_json::Value> {
        if let Some(v) = self
            .mem
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(cancel_key)
        {
            return v.clone();
        }
        let loaded = self.load_from_disk(cancel_key).unwrap_or_default();
        self.mem
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(cancel_key.to_string())
            .or_insert_with(|| loaded.clone());
        loaded
    }

    /// Drop a session's payloads from memory and disk (called when the session is deleted).
    pub fn remove(&self, cancel_key: &str) {
        self.mem
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(cancel_key);
        let path = self.path_for(cancel_key);
        if path.exists() {
            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!(key = %cancel_key, error = %e, "debug store: remove failed");
            }
        }
    }

    fn load_from_disk(&self, cancel_key: &str) -> Option<Vec<serde_json::Value>> {
        let body = fs::read_to_string(self.path_for(cancel_key)).ok()?;
        let file: DebugFile = serde_json::from_str(&body).ok()?;
        Some(file.requests)
    }

    fn persist(&self, cancel_key: &str, requests: &[serde_json::Value]) {
        let file = DebugFile {
            requests: requests.to_vec(),
        };
        match serde_json::to_string(&file) {
            Ok(body) => {
                if let Err(e) = fs::write(self.path_for(cancel_key), body) {
                    tracing::warn!(key = %cancel_key, error = %e, "debug store: persist failed");
                }
            }
            Err(e) => {
                tracing::warn!(key = %cancel_key, error = %e, "debug store: serialise failed")
            }
        }
    }

    /// One file per session; the `key::session_id` cancel-key is sanitised so it can never
    /// escape `dir`.
    fn path_for(&self, cancel_key: &str) -> PathBuf {
        let safe: String = cancel_key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.dir.join(format!("debug-{safe}.json"))
    }
}

/// A fresh, collision-resistant session id (`s{epoch_ms}-{seq}`).
fn new_session_id() -> String {
    let seq = SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("s{}-{}", now_ms(), seq)
}

/// Default name for the Nth session under a key.
fn default_name(n: usize) -> String {
    format!("Investigation {n}")
}

/// Build the store's per-`(workspace, process)` key.
pub fn session_key(workspace: &str, process: &str) -> String {
    let san = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    format!("{}__{}", san(workspace), san(process))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---- View projection: transcript → operator-facing turns --------------------------

/// One tool call + result the droid ran while answering (the lab notebook line).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatStepView {
    pub tool: String,
    pub arguments: serde_json::Value,
    pub result: String,
}

/// One entry in a droid turn's chain-of-thought timeline: either a block of reasoning prose
/// or a tool call, kept in the order they actually happened so the cockpit can show each tool
/// call *where it occurred* in the thinking rather than lumped at the end.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ThoughtItem {
    Reasoning {
        text: String,
    },
    Tool {
        tool: String,
        arguments: serde_json::Value,
        result: String,
    },
}

/// One operator-facing turn in the chat.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatTurnView {
    /// `user` (the operator), `droid` (the primary model), or `pair` (a Pair AI reviewer).
    pub role: String,
    /// For `pair` turns, the reviewer persona's display name (e.g. "Skeptic / Red-Team").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub text: String,
    /// For droid turns, the tool calls it ran before replying (flat, legacy view).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<ChatStepView>,
    /// For droid turns, the interleaved reasoning + tool-call timeline in chronological order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thought: Vec<ThoughtItem>,
    /// Epoch-ms timestamp of this turn, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<u64>,
    /// For droid turns, the model label that produced this answer (persisted at send time), so a
    /// historical bubble keeps the answering model's name regardless of the current selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Find `needle` (ASCII) in `hay` case-insensitively from byte offset `from`, returning a byte
/// offset into the original string. Operates on bytes; safe for UTF-8 because the ASCII tag
/// bytes never occur inside a multi-byte sequence.
fn find_ci(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from >= hay.len() || needle.len() > hay.len() {
        return None;
    }
    let end = hay.len() - needle.len();
    (from..=end).find(|&i| hay[i..i + needle.len()].eq_ignore_ascii_case(needle))
}

/// Pull `<think>`/`<thinking>` reasoning out of `text`, mirroring the cockpit's client-side
/// `splitThinking`. Returns `(thinking, answer)` where `answer` is the cleaned prose. Handles
/// multiple blocks and tolerates an unterminated trailing `<think>` (as seen mid-stream).
fn split_thinking(text: &str) -> (String, String) {
    let bytes = text.as_bytes();
    let mut think = String::new();
    let mut answer = String::new();
    let mut pos = 0usize;
    let push_think = |think: &mut String, body: &str| {
        let body = body.trim();
        if !body.is_empty() {
            if !think.is_empty() {
                think.push_str("\n\n");
            }
            think.push_str(body);
        }
    };
    while pos < bytes.len() {
        let open_a = find_ci(bytes, b"<think>", pos).map(|i| (i, 7usize));
        let open_b = find_ci(bytes, b"<thinking>", pos).map(|i| (i, 10usize));
        let next = match (open_a, open_b) {
            (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let Some((start, open_len)) = next else {
            answer.push_str(&text[pos..]);
            break;
        };
        answer.push_str(&text[pos..start]);
        let body_start = start + open_len;
        let close_a = find_ci(bytes, b"</think>", body_start).map(|i| (i, 8usize));
        let close_b = find_ci(bytes, b"</thinking>", body_start).map(|i| (i, 11usize));
        let close = match (close_a, close_b) {
            (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        match close {
            Some((cstart, clen)) => {
                push_think(&mut think, &text[body_start..cstart]);
                pos = cstart + clen;
            }
            None => {
                push_think(&mut think, &text[body_start..]);
                pos = bytes.len();
            }
        }
    }
    (think.trim().to_string(), answer.trim().to_string())
}

/// Project a model transcript into operator-facing turns: each `User` message becomes a
/// user turn; the model's tool calls are paired with their `Tool` results into steps and
/// attached to the next droid turn (an `Assistant` message carrying final text). The droid
/// turn also carries a `thought` timeline interleaving every reasoning block (extracted from
/// each round's `<think>` block) with the tool calls in chronological order, so the cockpit
/// can render each tool call exactly where it happened in the thinking. The visible `text` is
/// the final answer with its `<think>` stripped; the system message is omitted.
/// Extract the Alternate Reality Engine runs (`simulate` / `compare_variants`) from a persisted
/// transcript, pairing each tool call with its result, for the cockpit's Simulations tab. Each
/// returned run carries the raw tool `arguments` (which include the candidate BPMN XML) and the
/// parsed `result` (the fidelity scorecard / ranking), in transcript order.
pub fn extract_simulations(messages: &[Msg]) -> Vec<serde_json::Value> {
    use std::collections::HashMap;
    // Tool results, by the call_id they answer.
    let mut results: HashMap<&str, &str> = HashMap::new();
    for m in messages {
        if let Msg::Tool { call_id, content } = m {
            results.insert(call_id.as_str(), content.as_str());
        }
    }
    let mut runs = Vec::new();
    for m in messages {
        if let Msg::Assistant { tool_calls, .. } = m {
            for tc in tool_calls {
                if tc.name != "simulate" && tc.name != "compare_variants" {
                    continue;
                }
                let result = results.get(tc.id.as_str()).map(|c| {
                    serde_json::from_str::<serde_json::Value>(c)
                        .unwrap_or_else(|_| serde_json::json!({ "raw": c }))
                });
                runs.push(serde_json::json!({
                    "tool": tc.name,
                    "arguments": tc.arguments,
                    "result": result,
                }));
            }
        }
    }
    runs
}

pub fn render_view(messages: &[Msg]) -> Vec<ChatTurnView> {
    let mut turns: Vec<ChatTurnView> = Vec::new();
    // Tool calls awaiting their results / a final answer, by call_id.
    let mut pending: Vec<ToolCall> = Vec::new();
    let mut steps: Vec<ChatStepView> = Vec::new();
    let mut thought: Vec<ThoughtItem> = Vec::new();

    for msg in messages {
        match msg {
            Msg::System(_) => {}
            Msg::User(text) => {
                turns.push(ChatTurnView {
                    role: "user".into(),
                    name: None,
                    text: text.clone(),
                    steps: Vec::new(),
                    thought: Vec::new(),
                    ts: None,
                    model: None,
                });
            }
            Msg::Assistant { text, tool_calls } => {
                if !tool_calls.is_empty() {
                    // A tool-requesting turn: remember the calls so the following Tool
                    // messages can be matched, and record this round's reasoning.
                    pending.extend(tool_calls.iter().cloned());
                    if let Some(t) = text {
                        let (think, rest) = split_thinking(t);
                        let reason = if !think.is_empty() { think } else { rest };
                        if !reason.trim().is_empty() {
                            thought.push(ThoughtItem::Reasoning {
                                text: reason.trim().to_string(),
                            });
                        }
                    }
                } else if let Some((name, answer)) = text.as_deref().and_then(unmark_pair) {
                    // A Pair AI reviewer's contribution: its own attributed turn. Its tool work
                    // ran in a separate sub-conversation (so no steps here), but its own reasoning
                    // is embedded as <think> in the answer — surface it as a collapsible Thinking
                    // section, exactly like the primary droid turn.
                    let (think, clean) = split_thinking(&answer);
                    let mut pair_thought = Vec::new();
                    if !think.trim().is_empty() {
                        pair_thought.push(ThoughtItem::Reasoning {
                            text: think.trim().to_string(),
                        });
                    }
                    turns.push(ChatTurnView {
                        role: "pair".into(),
                        name: Some(name),
                        text: clean,
                        steps: Vec::new(),
                        thought: pair_thought,
                        ts: None,
                        model: None,
                    });
                } else {
                    // A final answer: split off its reasoning, then drain the timeline.
                    let (think, answer) = split_thinking(text.as_deref().unwrap_or(""));
                    if !think.trim().is_empty() {
                        thought.push(ThoughtItem::Reasoning {
                            text: think.trim().to_string(),
                        });
                    }
                    turns.push(ChatTurnView {
                        role: "droid".into(),
                        name: None,
                        text: answer,
                        steps: std::mem::take(&mut steps),
                        thought: std::mem::take(&mut thought),
                        ts: None,
                        model: None,
                    });
                    pending.clear();
                }
            }
            Msg::Tool { call_id, content } => {
                let step = if let Some(pos) = pending.iter().position(|c| c.id == *call_id) {
                    let call = pending.remove(pos);
                    ChatStepView {
                        tool: call.name,
                        arguments: call.arguments,
                        result: content.clone(),
                    }
                } else {
                    ChatStepView {
                        tool: "tool".into(),
                        arguments: serde_json::Value::Null,
                        result: content.clone(),
                    }
                };
                thought.push(ThoughtItem::Tool {
                    tool: step.tool.clone(),
                    arguments: step.arguments.clone(),
                    result: step.result.clone(),
                });
                steps.push(step);
            }
        }
    }
    turns
}

/// Project a transcript into turns and attach per-turn timestamps from `stamps` (aligned by
/// index to the rendered turns; missing/extra entries are tolerated).
pub fn render_view_stamped(messages: &[Msg], stamps: &[u64]) -> Vec<ChatTurnView> {
    let mut turns = render_view(messages);
    for (i, t) in turns.iter_mut().enumerate() {
        if let Some(&ts) = stamps.get(i) {
            if ts > 0 {
                t.ts = Some(ts);
            }
        }
    }
    turns
}

/// Like [`render_view_stamped`], but also attaches the model that produced each turn from
/// `turn_models` (aligned by index; blank entries are skipped). The cockpit uses `model` to keep
/// a historical droid bubble labelled with the model that actually answered it.
pub fn render_view_full(
    messages: &[Msg],
    stamps: &[u64],
    turn_models: &[String],
) -> Vec<ChatTurnView> {
    let mut turns = render_view_stamped(messages, stamps);
    for (i, t) in turns.iter_mut().enumerate() {
        if let Some(m) = turn_models.get(i) {
            if !m.is_empty() {
                t.model = Some(m.clone());
            }
        }
    }
    turns
}

/// Render a chat session's turns into a downloadable transcript. `markdown` chooses Markdown
/// vs plain text; `verbose` (the "debug" export) includes each turn's reasoning timeline and
/// tool calls (arguments + results), while the clean "chat" export keeps just the prose.
pub fn export_transcript(
    name: &str,
    turns: &[ChatTurnView],
    markdown: bool,
    verbose: bool,
) -> String {
    let mut out = String::new();
    let title = if name.trim().is_empty() {
        "Investigation".to_string()
    } else {
        format!("Investigation — {name}")
    };
    let kind = if verbose { "debug" } else { "chat" };
    if markdown {
        out.push_str(&format!("# {title}\n\n"));
        out.push_str(&format!(
            "_Exported from Nano ProcessOS · {kind} transcript · {} turns_\n",
            turns.len()
        ));
    } else {
        out.push_str(&format!("{title}\n"));
        out.push_str(&"=".repeat(title.chars().count().min(80)));
        out.push('\n');
        out.push_str(&format!(
            "Exported from Nano ProcessOS · {kind} transcript · {} turns\n",
            turns.len()
        ));
    }

    for (i, t) in turns.iter().enumerate() {
        let who = match t.role.as_str() {
            "user" => "You".to_string(),
            "pair" => format!("Pair · {}", t.name.as_deref().unwrap_or("reviewer")),
            _ => match t.model.as_deref() {
                Some(m) if !m.is_empty() => format!("Droid ({m})"),
                _ => "Droid".to_string(),
            },
        };
        let heading = format!("{} · {}", i + 1, who);
        if markdown {
            out.push_str(&format!("\n## {heading}\n\n"));
        } else {
            out.push_str(&format!("\n----- {heading} -----\n\n"));
        }

        // The debug export walks the interleaved reasoning + tool-call timeline before the answer.
        if verbose {
            for item in &t.thought {
                match item {
                    ThoughtItem::Reasoning { text } => {
                        let text = text.trim();
                        if text.is_empty() {
                            continue;
                        }
                        if markdown {
                            out.push_str("_Reasoning:_\n\n");
                            for line in text.lines() {
                                out.push_str(&format!("> {line}\n"));
                            }
                            out.push('\n');
                        } else {
                            out.push_str("[Reasoning]\n");
                            out.push_str(text);
                            out.push_str("\n\n");
                        }
                    }
                    ThoughtItem::Tool {
                        tool,
                        arguments,
                        result,
                    } => {
                        let args = compact_json(arguments);
                        if markdown {
                            out.push_str(&format!("**Tool · `{tool}`**\n\n"));
                            out.push_str(&format!("```json\n{args}\n```\n\n"));
                            if !result.trim().is_empty() {
                                out.push_str(&format!("```\n{}\n```\n\n", result.trim_end()));
                            }
                        } else {
                            out.push_str(&format!("[Tool: {tool}]\n"));
                            out.push_str(&format!("  args: {args}\n"));
                            if !result.trim().is_empty() {
                                out.push_str(&format!("  result: {}\n", result.trim_end()));
                            }
                            out.push('\n');
                        }
                    }
                }
            }
            // Older turns may only carry the flat `steps` list (no interleaved timeline).
            if t.thought.is_empty() {
                for s in &t.steps {
                    let args = compact_json(&s.arguments);
                    if markdown {
                        out.push_str(&format!("**Tool · `{}`**\n\n", s.tool));
                        out.push_str(&format!("```json\n{args}\n```\n\n"));
                        if !s.result.trim().is_empty() {
                            out.push_str(&format!("```\n{}\n```\n\n", s.result.trim_end()));
                        }
                    } else {
                        out.push_str(&format!("[Tool: {}]\n", s.tool));
                        out.push_str(&format!("  args: {args}\n"));
                        if !s.result.trim().is_empty() {
                            out.push_str(&format!("  result: {}\n", s.result.trim_end()));
                        }
                        out.push('\n');
                    }
                }
            }
        }

        let answer = t.text.trim();
        if !answer.is_empty() {
            out.push_str(answer);
            out.push('\n');
        }
    }

    out.push('\n');
    out
}

/// Pretty-print small JSON inline, falling back to the compact form for large blobs so a tool
/// export stays legible without ballooning.
fn compact_json(v: &serde_json::Value) -> String {
    let pretty = serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string());
    if pretty.len() <= 2000 {
        pretty
    } else {
        v.to_string()
    }
}

/// turn is stamped `user_ts` and newly-appeared droid turns `droid_ts`. Existing stamps are
/// preserved; the result is truncated to the turn count.
pub fn extend_stamps(
    messages: &[Msg],
    mut stamps: Vec<u64>,
    user_ts: u64,
    droid_ts: u64,
) -> Vec<u64> {
    let view = render_view(messages);
    while stamps.len() < view.len() {
        let i = stamps.len();
        let ts = if view[i].role == "user" {
            user_ts
        } else {
            droid_ts
        };
        stamps.push(ts);
    }
    stamps.truncate(view.len());
    stamps
}

/// Extend `turn_models` to align with the rendered turns of `messages`: each newly-appeared
/// non-user turn (droid/pair) is attributed to `model`; user turns get an empty label. Existing
/// entries are preserved (so earlier turns keep the model that actually produced them); the result
/// is truncated to the turn count.
pub fn extend_turn_models(
    messages: &[Msg],
    mut turn_models: Vec<String>,
    model: &str,
) -> Vec<String> {
    let view = render_view(messages);
    while turn_models.len() < view.len() {
        let i = turn_models.len();
        let label = if view[i].role == "user" {
            String::new()
        } else {
            model.to_string()
        };
        turn_models.push(label);
    }
    turn_models.truncate(view.len());
    turn_models
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        // nextest runs each test in its own process, so the process-local SEQ
        // resets to 0 in every test. Include the PID and a nanosecond clock so
        // two tests running in parallel never derive the same temp dir (a
        // collision lets one test's remove_dir_all wipe another's store).
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!(
            "processos-chat-{}-{}-{}",
            std::process::id(),
            nanos,
            n
        ));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn create_save_load_round_trips_and_survives_reopen() {
        let dir = tmp();
        let key = session_key("acme", "loan");
        let id;
        {
            let store = ChatStore::open(&dir);
            let s = store.create(&key, Some("My probe".into()));
            id = s.id.clone();
            assert_eq!(s.name, "My probe");
            store.save(
                &key,
                &id,
                vec![Msg::System("s".into()), Msg::User("hi".into())],
                vec![0, 123],
            );
        }
        let store = ChatStore::open(&dir);
        let loaded = store.get(&key, &id).expect("session present after reopen");
        assert_eq!(loaded.messages.len(), 2);
        assert_eq!(loaded.stamps, vec![0, 123]);
        assert_eq!(store.list(&key).len(), 1);
        assert!(store.delete(&key, &id));
        assert!(store.get(&key, &id).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn poisoned_lock_recovers_without_losing_persisted_sessions() {
        // Regression for the poison-recovery hole: a panic while holding the
        // store's lock poisons it. `ensure_loaded`/`list`/`get`/`clear` must
        // recover the guard (like the write paths) instead of skipping — a
        // skip would leave a persisted-but-uncached key out of the in-memory
        // map, so a following `create` would start from an empty vector and
        // overwrite the file with only the new session, losing saved history.
        let dir = tmp();
        let key = session_key("acme", "loan");
        let old_id;
        {
            let store = ChatStore::open(&dir);
            old_id = store.create(&key, Some("old".into())).id;
        }

        // Reopen so the key is persisted but NOT cached in memory.
        let store = ChatStore::open(&dir);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = store.mem.write().unwrap_or_else(|e| e.into_inner());
            panic!("poison the chat store lock");
        }));
        assert!(store.mem.is_poisoned(), "lock should now be poisoned");

        // Creating a session for the uncached persisted key must load the
        // existing history first, not overwrite it.
        let new_session = store.create(&key, Some("new".into()));
        let metas = store.list(&key);
        assert_eq!(metas.len(), 2, "old and new sessions must both survive");
        let old = store
            .get(&key, &old_id)
            .expect("persisted session must stay readable after poison");
        assert_eq!(old.name, "old");

        // Both sessions must be persisted: a fresh store recovers them from disk.
        let store = ChatStore::open(&dir);
        let metas = store.list(&key);
        assert_eq!(metas.len(), 2, "both sessions must persist to disk");
        assert!(store.get(&key, &old_id).is_some());
        assert!(store.get(&key, &new_session.id).is_some());

        // `clear` must also work against the poisoned lock.
        let store = ChatStore::open(&dir);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = store.mem.write().unwrap_or_else(|e| e.into_inner());
            panic!("poison the chat store lock");
        }));
        store.clear(&key);
        assert!(store.list(&key).is_empty());
        assert!(store.get(&key, &old_id).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn debug_store_persists_across_reopen_and_removes() {
        let dir = tmp();
        let key = session_key("acme", "loan");
        let cancel_key = format!("{key}::s123-0");
        let requests = vec![
            serde_json::json!({ "round": 1, "body": { "messages": ["system", "user"] } }),
            serde_json::json!({ "round": 2, "body": { "messages": ["assistant"] } }),
        ];
        {
            let store = DebugStore::open(&dir);
            // Nothing captured yet.
            assert!(store.load(&cancel_key).is_empty());
            store.save(&cancel_key, requests.clone());
            assert_eq!(store.load(&cancel_key), requests);
        }
        // Simulate a server restart: a fresh store with an empty memory cache must recover
        // the payloads from disk — the whole point of the fix.
        let store = DebugStore::open(&dir);
        assert_eq!(
            store.load(&cancel_key),
            requests,
            "debug payloads must survive a restart"
        );
        // A later turn replaces (not appends to) the buffer, matching the Debug tab contract.
        let newer = vec![serde_json::json!({ "round": 1, "body": "newer" })];
        store.save(&cancel_key, newer.clone());
        assert_eq!(store.load(&cancel_key), newer);
        // Deleting the session drops the payloads from memory and disk.
        store.remove(&cancel_key);
        assert!(store.load(&cancel_key).is_empty());
        assert!(!dir
            .join(format!("debug-{}.json", "acme__loan__s123-0"))
            .exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn debug_store_recovers_poisoned_lock() {
        // Same poison-recovery class as the ChatStore: a panic while holding the
        // lock must not wedge the debug store. `save`/`load`/`remove` recover the
        // guard instead of silently skipping the in-memory cache.
        let dir = tmp();
        let key = session_key("acme", "loan");
        let cancel_key = format!("{key}::s123-0");
        let store = DebugStore::open(&dir);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = store.mem.write().unwrap_or_else(|e| e.into_inner());
            panic!("poison the debug store lock");
        }));
        assert!(store.mem.is_poisoned(), "lock should now be poisoned");

        let requests = vec![serde_json::json!({ "round": 1, "body": "x" })];
        store.save(&cancel_key, requests.clone());
        assert_eq!(store.load(&cancel_key), requests);
        store.remove(&cancel_key);
        assert!(store.load(&cancel_key).is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_model_dedupes_preserves_order_and_persists() {
        let dir = tmp();
        let key = session_key("acme", "loan");
        let id;
        {
            let store = ChatStore::open(&dir);
            let s = store.create(&key, None);
            id = s.id.clone();
            store.record_model(&key, &id, "qwen3-8b");
            store.record_model(&key, &id, "qwen3-8b"); // dupe — ignored
            store.record_model(&key, &id, "  "); // blank — ignored
            store.record_model(&key, &id, "gemma-3-4b");
            // A transcript save must not clobber the recorded models.
            store.save(&key, &id, vec![Msg::User("hi".into())], vec![0]);
        }
        let store = ChatStore::open(&dir);
        let loaded = store.get(&key, &id).expect("session present after reopen");
        assert_eq!(loaded.models, vec!["qwen3-8b", "gemma-3-4b"]);
        assert_eq!(store.list(&key)[0].models, vec!["qwen3-8b", "gemma-3-4b"]);
        // record_model on an unknown id creates the session so the first turn's model is kept.
        store.record_model(&key, "ghost-id", "phi-4");
        assert_eq!(store.get(&key, "ghost-id").unwrap().models, vec!["phi-4"]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rename_and_multiple_sessions() {
        let dir = tmp();
        let key = session_key("acme", "loan");
        let store = ChatStore::open(&dir);
        let a = store.create(&key, None);
        let b = store.create(&key, None);
        assert_eq!(a.name, "Investigation 1");
        assert_eq!(b.name, "Investigation 2");
        assert!(store.rename(&key, &a.id, "Renamed"));
        assert_eq!(store.get(&key, &a.id).unwrap().name, "Renamed");
        assert_eq!(store.list(&key).len(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn migrates_legacy_single_session_file() {
        let dir = tmp();
        let key = session_key("acme", "loan");
        std::fs::create_dir_all(&dir).unwrap();
        // Write the pre-multisession shape directly.
        let store = ChatStore::open(&dir);
        let legacy = serde_json::json!({
            "messages": [{"User": "old question"}],
            "updated": 42,
        });
        std::fs::write(store.path_for(&key), legacy.to_string()).unwrap();
        // A fresh store should migrate it into one named session on load.
        let store = ChatStore::open(&dir);
        let metas = store.list(&key);
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].name, "Investigation 1");
        assert_eq!(metas[0].updated, 42);
        let s = store.get(&key, &metas[0].id).unwrap();
        assert_eq!(s.messages.len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn keys_cannot_escape_the_data_dir() {
        let dir = tmp();
        let store = ChatStore::open(&dir);
        store.save(
            "../../etc/passwd",
            "sid",
            vec![Msg::User("x".into())],
            vec![0],
        );
        let entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(entries.iter().all(|n| !n.contains('/')));
        assert!(entries.iter().any(|n| n.ends_with(".json")));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn extend_stamps_aligns_user_then_droid() {
        let transcript = vec![
            Msg::System("s".into()),
            Msg::User("q".into()),
            Msg::Assistant {
                text: Some("a".into()),
                tool_calls: vec![],
            },
        ];
        let stamps = extend_stamps(&transcript, vec![], 100, 200);
        assert_eq!(stamps, vec![100, 200]); // user turn, droid turn
        let view = render_view_stamped(&transcript, &stamps);
        assert_eq!(view[0].ts, Some(100));
        assert_eq!(view[1].ts, Some(200));
    }

    #[test]
    fn turn_models_persist_per_turn_across_model_switch() {
        // Turn 1: model A answers.
        let mut transcript = vec![
            Msg::System("s".into()),
            Msg::User("q1".into()),
            Msg::Assistant {
                text: Some("a1".into()),
                tool_calls: vec![],
            },
        ];
        let tm = extend_turn_models(&transcript, vec![], "model-a");
        assert_eq!(tm, vec!["".to_string(), "model-a".to_string()]);

        // Turn 2: operator switches to model B, which answers.
        transcript.push(Msg::User("q2".into()));
        transcript.push(Msg::Assistant {
            text: Some("a2".into()),
            tool_calls: vec![],
        });
        let tm = extend_turn_models(&transcript, tm, "model-b");
        // The first droid turn keeps model-a; the new one is model-b.
        assert_eq!(
            tm,
            vec![
                "".to_string(),
                "model-a".to_string(),
                "".to_string(),
                "model-b".to_string()
            ]
        );

        // render_view_full surfaces the per-turn model on droid turns and none on user turns.
        let stamps = extend_stamps(&transcript, vec![], 1, 2);
        let view = render_view_full(&transcript, &stamps, &tm);
        assert_eq!(view[0].model, None); // user
        assert_eq!(view[1].model, Some("model-a".to_string()));
        assert_eq!(view[2].model, None); // user
        assert_eq!(view[3].model, Some("model-b".to_string()));
    }

    #[test]
    fn set_turn_models_persists_and_survives_reopen() {
        let dir = tmp();
        let key = session_key("acme", "loan");
        let id;
        {
            let store = ChatStore::open(&dir);
            let s = store.create(&key, Some("probe".into()));
            id = s.id.clone();
            store.set_turn_models(&key, &id, vec!["".into(), "qwen3".into()]);
        }
        {
            let store = ChatStore::open(&dir);
            let s = store.get(&key, &id).unwrap();
            assert_eq!(s.turn_models, vec!["".to_string(), "qwen3".to_string()]);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn render_view_pairs_tool_calls_with_results() {
        let transcript = vec![
            Msg::System("sys".into()),
            Msg::User("why slow?".into()),
            Msg::Assistant {
                text: None,
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "query_traces".into(),
                    arguments: json!({"sql": "SELECT 1"}),
                }],
            },
            Msg::Tool {
                call_id: "c1".into(),
                content: "1".into(),
            },
            Msg::Assistant {
                text: Some("It's the credit-check job.".into()),
                tool_calls: vec![],
            },
        ];
        let view = render_view(&transcript);
        assert_eq!(view.len(), 2);
        assert_eq!(view[0].role, "user");
        assert_eq!(view[0].text, "why slow?");
        assert_eq!(view[1].role, "droid");
        assert_eq!(view[1].text, "It's the credit-check job.");
        assert_eq!(view[1].steps.len(), 1);
        assert_eq!(view[1].steps[0].tool, "query_traces");
        assert_eq!(view[1].steps[0].result, "1");
    }

    #[test]
    fn export_transcript_chat_vs_debug() {
        let transcript = vec![
            Msg::System("sys".into()),
            Msg::User("why slow?".into()),
            Msg::Assistant {
                text: None,
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "query_traces".into(),
                    arguments: json!({"sql": "SELECT 1"}),
                }],
            },
            Msg::Tool {
                call_id: "c1".into(),
                content: "1".into(),
            },
            Msg::Assistant {
                text: Some("It's the credit-check job.".into()),
                tool_calls: vec![],
            },
        ];
        let turns = render_view(&transcript);

        // Clean chat export: prose only, no tool dump.
        let chat_md = export_transcript("North Wind", &turns, true, false);
        assert!(chat_md.contains("# Investigation — North Wind"));
        assert!(chat_md.contains("## 1 · You"));
        assert!(chat_md.contains("why slow?"));
        assert!(chat_md.contains("It's the credit-check job."));
        assert!(!chat_md.contains("query_traces"));

        // Debug export: includes the tool call + result.
        let debug_md = export_transcript("North Wind", &turns, true, true);
        assert!(debug_md.contains("Tool · `query_traces`"));
        assert!(debug_md.contains("SELECT 1"));
        assert!(debug_md.contains("It's the credit-check job."));

        // Plain-text variant has no markdown headers.
        let chat_txt = export_transcript("North Wind", &turns, false, false);
        assert!(chat_txt.contains("----- 1 · You -----"));
        assert!(!chat_txt.contains("# Investigation"));
    }

    #[test]
    fn extract_simulations_pairs_runs_with_scorecards() {
        let transcript = vec![
            Msg::System("sys".into()),
            Msg::User("make it faster".into()),
            Msg::Assistant {
                text: None,
                tool_calls: vec![
                    ToolCall {
                        id: "q1".into(),
                        name: "query_traces".into(),
                        arguments: json!({"sql": "SELECT 1"}),
                    },
                    ToolCall {
                        id: "s1".into(),
                        name: "simulate".into(),
                        arguments: json!({"name": "parallelised", "model": "<bpmn/>"}),
                    },
                ],
            },
            Msg::Tool { call_id: "q1".into(), content: "1".into() },
            Msg::Tool {
                call_id: "s1".into(),
                content: json!({"replayable": true, "datasetSize": 42, "scorecard": {"fidelityTier": "recorded-replay"}}).to_string(),
            },
        ];
        let runs = extract_simulations(&transcript);
        // Only the simulate call is extracted (query_traces is ignored).
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["tool"], "simulate");
        assert_eq!(runs[0]["arguments"]["model"], "<bpmn/>");
        assert_eq!(runs[0]["result"]["datasetSize"], 42);
        assert_eq!(
            runs[0]["result"]["scorecard"]["fidelityTier"],
            "recorded-replay"
        );
    }

    #[test]
    fn render_view_carries_tool_round_thinking_into_droid_turn() {
        // The model thinks, calls a tool (thinking persisted as <think> on that turn), then
        // answers in a later round with no further thinking. The droid turn must still show it.
        let transcript = vec![
            Msg::System("sys".into()),
            Msg::User("why slow?".into()),
            Msg::Assistant {
                text: Some("<think>let me check durations</think>".into()),
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "query_traces".into(),
                    arguments: json!({"sql": "SELECT 1"}),
                }],
            },
            Msg::Tool {
                call_id: "c1".into(),
                content: "1".into(),
            },
            Msg::Assistant {
                text: Some("credit-check.".into()),
                tool_calls: vec![],
            },
        ];
        let view = render_view(&transcript);
        assert_eq!(view.len(), 2);
        // The visible answer is the cleaned final text (no <think>).
        assert_eq!(view[1].text, "credit-check.");
        // The reasoning from the tool round survives in the interleaved thought timeline,
        // ordered before the tool call it preceded.
        assert!(
            matches!(&view[1].thought[0], ThoughtItem::Reasoning { text } if text.contains("let me check durations"))
        );
        assert!(
            matches!(&view[1].thought[1], ThoughtItem::Tool { tool, .. } if tool == "query_traces")
        );
    }

    #[test]
    fn split_thinking_handles_blocks_and_unterminated() {
        let (think, answer) = split_thinking("<think>a</think>hello<thinking>b</thinking> world");
        assert_eq!(think, "a\n\nb");
        assert_eq!(answer, "hello world");
        let (think, answer) = split_thinking("still thinking <think>not closed");
        assert_eq!(think, "not closed");
        assert_eq!(answer, "still thinking");
    }

    #[test]
    fn render_view_decodes_pair_marked_reviewer_turn() {
        // A Pair AI reviewer's answer persists as a marked assistant message; render_view must
        // surface it as an attributed `pair` turn carrying the reviewer persona's display name.
        let transcript = vec![
            Msg::System("sys".into()),
            Msg::User("why slow?".into()),
            Msg::Assistant {
                text: Some("credit-check.".into()),
                tool_calls: vec![],
            },
            Msg::Assistant {
                text: Some(mark_pair(
                    "Skeptic / Red-Team",
                    "<think>hmm</think>Actually check identity-check too.",
                )),
                tool_calls: vec![],
            },
        ];
        let view = render_view(&transcript);
        assert_eq!(view.len(), 3);
        assert_eq!(view[1].role, "droid");
        assert_eq!(view[2].role, "pair");
        assert_eq!(view[2].name.as_deref(), Some("Skeptic / Red-Team"));
        // The reviewer's <think> is stripped from the visible answer.
        assert_eq!(view[2].text, "Actually check identity-check too.");
        // …but preserved as a collapsible Thinking item, just like the primary droid turn.
        assert_eq!(view[2].thought.len(), 1);
        match &view[2].thought[0] {
            ThoughtItem::Reasoning { text } => assert_eq!(text, "hmm"),
            other => panic!("expected reasoning, got {other:?}"),
        }
    }

    #[test]
    fn mark_unmark_pair_round_trips() {
        let marked = mark_pair("Synthesizer", "final answer");
        let (name, answer) = unmark_pair(&marked).expect("decodes");
        assert_eq!(name, "Synthesizer");
        assert_eq!(answer, "final answer");
        // Plain prose (no sentinel) is not mistaken for a pair turn.
        assert!(unmark_pair("just an answer").is_none());
    }
}
