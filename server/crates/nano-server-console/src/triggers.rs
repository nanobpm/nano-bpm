//! Trigger runtime — the durable inbox + dispatcher (ADR 0025, phase 1).
//!
//! An installed Urban App is inert until something makes it *act*. A **trigger**
//! maps a source event to an engine call — start a process, or publish a
//! `CorrelateMessage`. This module is the "one genuinely-new runtime subsystem"
//! ADR 0022 §B said to build first: a **durable inbox** (a table on the App's
//! ADR 0024 datasource) that is the crash-consistency boundary between "event
//! received" and "action applied", plus a **dispatcher** that drains it.
//!
//! ## Delivery semantics (ADR 0025 §2) — at-least-once
//! Each event flows through three durable steps:
//! 1. **persist** to the inbox (status `pending`) under a unique **idempotency
//!    key** — the dedup point (§3);
//! 2. **dispatch** — apply the trigger's `action` to the engine exactly like an
//!    SDK client would, over the local gateway (`http://127.0.0.1:<port>`), so
//!    embedded and remote are identical (ADR 0005);
//! 3. **settle** — mark the row `done`, or bump `attempts` with capped backoff
//!    and dead-letter (`failed`) past [`MAX_ATTEMPTS`].
//!
//! A crash between step 2 and step 3 re-delivers on the next drain (the row is
//! still `pending`); this is why the contract is honestly *at-least-once*, not
//! exactly-once, and why makers should design idempotent processes.
//!
//! ## Runtime portability (ADR 0038)
//! The inbox is reached through [`super::projects::run_data_op`], which is
//! Node-first (Deno optional). The dispatcher itself is pure in-process Rust; it
//! carries no Deno dependency.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Value as Json, json};
use tokio::sync::{Mutex, Notify};

use super::projects::{self, DataError};

/// Rows are dead-lettered (`status = 'failed'`) once `attempts` reaches this.
const MAX_ATTEMPTS: i64 = 5;
/// First retry waits this long; each subsequent retry doubles, capped at
/// [`BACKOFF_CAP_MS`].
const BACKOFF_BASE_MS: u64 = 1_000;
const BACKOFF_CAP_MS: u64 = 60_000;
/// Rows pulled per drain pass.
const DRAIN_BATCH: i64 = 32;
/// How often the always-on dispatcher polls the inbox for a running App.
const POLL_INTERVAL_MS: u64 = 2_000;
/// Back-off after a drain error (e.g. the App has no datasource yet).
const ERROR_BACKOFF_MS: u64 = 5_000;
/// Inbox rows surfaced by [`inbox_status`].
const RECENT_LIMIT: i64 = 20;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum TriggerError {
    /// A datasource op failed (see [`DataError`]).
    Data(DataError),
    /// The App manifest is missing/unreadable, or references an unknown trigger.
    Manifest(String),
    /// A trigger action's FEEL (`variables`/`correlationKey`) failed to evaluate.
    Feel(String),
    /// The engine rejected the action (non-2xx from the gateway, or a transport
    /// error). Transient — the row is retried.
    Apply(String),
    /// An inbound webhook presented an invalid or missing shared secret.
    Unauthorized(String),
}

impl std::fmt::Display for TriggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerError::Data(e) => write!(f, "datasource: {e:?}"),
            TriggerError::Manifest(m) => write!(f, "manifest: {m}"),
            TriggerError::Feel(m) => write!(f, "feel: {m}"),
            TriggerError::Apply(m) => write!(f, "apply: {m}"),
            TriggerError::Unauthorized(m) => write!(f, "unauthorized: {m}"),
        }
    }
}

impl From<DataError> for TriggerError {
    fn from(e: DataError) -> Self {
        TriggerError::Data(e)
    }
}

// ---------------------------------------------------------------------------
// Inbox schema + primitives
// ---------------------------------------------------------------------------

const CREATE_INBOX_SQL: &str = "CREATE TABLE IF NOT EXISTS trigger_inbox (\
    id INTEGER PRIMARY KEY AUTOINCREMENT, \
    trigger_id TEXT NOT NULL, \
    idem_key TEXT NOT NULL UNIQUE, \
    body TEXT NOT NULL, \
    status TEXT NOT NULL DEFAULT 'pending', \
    attempts INTEGER NOT NULL DEFAULT 0, \
    next_at INTEGER NOT NULL DEFAULT 0, \
    last_error TEXT, \
    created_at INTEGER NOT NULL, \
    updated_at INTEGER NOT NULL)";

/// Create the inbox table on the App's default datasource if absent.
///
/// Idempotent **and memoised**: the `CREATE TABLE IF NOT EXISTS` is cheap on
/// SQLite, but every call is a fresh `urban data` process spawn (ADR 0038), so
/// re-running it on every drain round (#1340) burns a core on small hosts. Once
/// the table has been created for a project we skip the spawn entirely; a
/// failed attempt is *not* memoised, so a project whose datasource isn't ready
/// yet retries on the next call.
///
/// The memo is keyed by name but **generation-checked** on insert: a project
/// name does not identify a stable datasource (rename, datasource reconfig, and
/// delete+recreate all swap the DB under a reused name), and the `CREATE` spawn
/// is not atomic with the memo write. [`forget_inbox`] bumps the project's
/// generation, so an in-flight `ensure_inbox` that was mid-`CREATE` when the
/// datasource was torn down cannot re-insert a now-stale marker for the fresh
/// datasource — its continuation observes the generation bump and skips the
/// write, forcing a re-`CREATE` on the next call.
pub(crate) async fn ensure_inbox(project: &str) -> Result<(), TriggerError> {
    // Fast path: hold the lock only to read the memo + generation, never across
    // the `await`. Two concurrent callers may both miss the memo and each spawn
    // a `CREATE` — `CREATE TABLE IF NOT EXISTS` makes that rare double-spawn
    // harmless, and both then insert under the same generation.
    //
    // The memo hit is validated against the datasource's current fingerprint,
    // not just the name: a re-import or a manifest edit (even an out-of-band one
    // that never calls `forget_inbox`) repoints the datasource, so a name-only
    // marker would skip the required `CREATE` on the new datasource and make
    // enqueue/drain/status fail on a missing `trigger_inbox` (#1340). The
    // fingerprint is computed before the lock (it does filesystem IO).
    let fingerprint = projects::datasource_fingerprint(project);
    let generation = {
        let memo = ensure_inbox_memo().lock().unwrap();
        if memo.hits(project, fingerprint) {
            return Ok(());
        }
        memo.generation(project)
    };
    projects::run_data_op(project, json!({ "op": "exec", "sql": CREATE_INBOX_SQL })).await?;
    // Insert only if the generation we ensured under is still current — i.e.
    // no `forget_inbox` (delete / rename / datasource reconfig) landed while
    // the `CREATE` was in flight. A bumped generation means the table we just
    // created may belong to a torn-down datasource, so we must not memo it.
    let mut memo = ensure_inbox_memo().lock().unwrap();
    memo.record_ensured(project, fingerprint, generation);
    Ok(())
}

/// Run one inbox datasource op (`body` is the `run_data_op` request), healing a
/// stale memo on the way.
///
/// The memo's fingerprint covers the project directory and manifest bytes — it
/// cannot observe the datasource's *contents*: deleting `app.db` through the
/// console's file API, or dropping `trigger_inbox` through the data exec API,
/// leaves the fingerprint unchanged, so a memo hit would skip the `CREATE` and
/// every inbox op would fail on the missing table (pre-#1340 behaviour was to
/// `CREATE` unconditionally, which self-healed). Rather than enumerate every
/// mutation path, this wrapper treats the missing-table error itself as the
/// invalidation signal: it drops the memo, re-`CREATE`s, and retries the op
/// exactly once — restoring self-healing without re-introducing the per-op
/// spawn #1340 removed. A genuine SQL error mentioning no table is unaffected
/// (the retry reproduces the original error); an op that fails again after a
/// successful re-`CREATE` propagates.
async fn run_inbox_op(project: &str, body: Json) -> Result<Json, TriggerError> {
    match projects::run_data_op(project, body.clone()).await {
        Err(DataError::Op(msg))
            if msg.contains("trigger_inbox") && msg.contains("no such table") =>
        {
            // The memo claimed the inbox existed, but the datasource says
            // otherwise. Invalidate and re-create, then retry the op once.
            forget_inbox(project);
            ensure_inbox(project).await?;
            Ok(projects::run_data_op(project, body).await?)
        }
        res => res.map_err(TriggerError::from),
    }
}

/// Drop the memoised "inbox ensured" marker for `project`, so the next
/// [`ensure_inbox`] re-creates the table. Called whenever a project's
/// datasource may have changed out from under the memo — project delete,
/// rename, or a `nano.app.json` save that selects a fresh default database —
/// and bumps the project's generation so any in-flight [`ensure_inbox`] for the
/// old datasource cannot resurrect a stale marker.
pub(crate) fn forget_inbox(project: &str) {
    let mut memo = ensure_inbox_memo().lock().unwrap();
    memo.ensured.remove(project);
    // Bump the generation so an `ensure_inbox` currently awaiting its `CREATE`
    // for the pre-teardown datasource skips its memo insert.
    let next = memo.generations.entry(project.to_string()).or_insert(0);
    *next += 1;
}

/// The process-wide "inbox ensured" memo state, shared by [`ensure_inbox`] and
/// [`forget_inbox`]. A project maps to the **datasource fingerprint** its
/// `trigger_inbox` table was created on, so later calls skip the per-op
/// `urban data` spawn (#1340) *only* while the datasource is unchanged — a
/// re-import or (out-of-band) manifest edit yields a new fingerprint and forces
/// a re-`CREATE`. `generations` is the invalidation counter that lets an
/// in-flight `ensure_inbox` detect a concurrent teardown. A plain
/// `std::sync::Mutex` (never held across an `await`) keeps [`forget_inbox`]
/// callable from synchronous teardown paths.
#[derive(Default)]
struct InboxMemo {
    ensured: HashMap<String, u64>,
    generations: HashMap<String, u64>,
}

impl InboxMemo {
    /// The current invalidation generation for `project` (0 if never torn down).
    fn generation(&self, project: &str) -> u64 {
        self.generations.get(project).copied().unwrap_or(0)
    }

    /// Record `project`'s inbox as ensured under `generation`, returning `true`
    /// when the marker was inserted. The insert is refused (`false`) when the
    /// memo's current generation has moved past `generation` — i.e. a
    /// [`forget_inbox`] (delete / rename / datasource reconfig) landed while the
    /// `CREATE` was in flight, so the table just created may belong to a
    /// torn-down datasource and must not be memoised. A `None` fingerprint
    /// (unresolvable datasource) also fails closed: nothing is memoised, so the
    /// next call re-`CREATE`s.
    fn record_ensured(&mut self, project: &str, fingerprint: Option<u64>, generation: u64) -> bool {
        if self.generation(project) != generation {
            return false;
        }
        if let Some(fp) = fingerprint {
            self.ensured.insert(project.to_string(), fp);
        }
        true
    }

    /// Whether a memoised marker may be trusted for `project` under its current
    /// datasource `fingerprint`. A hit requires a recorded marker **and** a
    /// fingerprint match: a `None` fingerprint (unresolved datasource) or a
    /// changed one (re-import / manifest edit) is always a miss, so the caller
    /// re-`CREATE`s the table rather than trusting a stale datasource.
    fn hits(&self, project: &str, fingerprint: Option<u64>) -> bool {
        matches!(fingerprint, Some(fp) if self.ensured.get(project) == Some(&fp))
    }
}

fn ensure_inbox_memo() -> &'static std::sync::Mutex<InboxMemo> {
    static ENSURED: OnceLock<std::sync::Mutex<InboxMemo>> = OnceLock::new();
    ENSURED.get_or_init(|| std::sync::Mutex::new(InboxMemo::default()))
}

/// Test-only: whether `project`'s inbox is currently memoised as created.
#[cfg(test)]
pub(crate) fn inbox_is_memoised(project: &str) -> bool {
    ensure_inbox_memo()
        .lock()
        .unwrap()
        .ensured
        .contains_key(project)
}

/// Test-only: the current invalidation generation for `project`.
#[cfg(test)]
pub(crate) fn inbox_generation(project: &str) -> u64 {
    ensure_inbox_memo().lock().unwrap().generation(project)
}

/// Test-only: mark `project`'s inbox as ensured under the current generation,
/// without spawning a `CREATE` — so lifecycle-invalidation tests can seed the
/// memo without a JS runtime.
#[cfg(test)]
pub(crate) fn ensure_inbox_memo_for_test(project: &str) {
    // Seed the fingerprint the project currently resolves to (0 when it does not
    // resolve), so the seeded marker behaves like a genuine `ensure_inbox` hit.
    let fingerprint = projects::datasource_fingerprint(project).unwrap_or(0);
    ensure_inbox_memo()
        .lock()
        .unwrap()
        .ensured
        .insert(project.to_string(), fingerprint);
}

/// The outcome of [`enqueue`]: `enqueued` is false when the idempotency key was
/// already present — a no-op that proves the §3 dedup boundary.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct EnqueueOutcome {
    pub enqueued: bool,
    pub id: Option<i64>,
}

/// Persist an event for `trigger_id` into the inbox (step 1). A repeated
/// `idem_key` is suppressed by the table's UNIQUE constraint. When `idem_key`
/// is `None`, a deterministic content hash of `(trigger_id, body)` is used so
/// identical submissions collapse to one row.
pub(crate) async fn enqueue(
    project: &str,
    trigger_id: &str,
    idem_key: Option<&str>,
    body: &Json,
) -> Result<EnqueueOutcome, TriggerError> {
    ensure_inbox(project).await?;
    let key = match idem_key {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => derive_idem_key(trigger_id, body),
    };
    let body_str = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
    let now = now_ms() as i64;
    let res = run_inbox_op(
        project,
        json!({
            "op": "exec",
            "sql": "INSERT INTO trigger_inbox \
                    (trigger_id, idem_key, body, status, attempts, next_at, created_at, updated_at) \
                    VALUES (?, ?, ?, 'pending', 0, 0, ?, ?) \
                    ON CONFLICT(idem_key) DO NOTHING",
            "params": [trigger_id, key, body_str, now, now],
        }),
    )
    .await?;
    let changed = res.get("changed").and_then(Json::as_i64).unwrap_or(0);
    if changed >= 1 {
        let id = res.get("lastInsertId").and_then(Json::as_i64);
        Ok(EnqueueOutcome { enqueued: true, id })
    } else {
        Ok(EnqueueOutcome {
            enqueued: false,
            id: None,
        })
    }
}

/// A deterministic, non-cryptographic content key. `DefaultHasher::new()` uses
/// fixed SipHash keys, so this is stable across process restarts — enough to
/// collapse re-submitted identical events within the runtime (§3).
fn derive_idem_key(trigger_id: &str, body: &Json) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    trigger_id.hash(&mut h);
    serde_json::to_string(body).unwrap_or_default().hash(&mut h);
    format!("auto:{trigger_id}:{:016x}", h.finish())
}

// ---------------------------------------------------------------------------
// Webhook ingress (ADR 0025 phase 2) — the universal external emit endpoint
// ---------------------------------------------------------------------------

/// Ingest an inbound webhook event for `trigger_id` (the gateway ingress route
/// delegates here). Validates the trigger exists and is a `webhook`, enforces
/// its optional shared-secret `auth`, then **persists before acking** (§2:
/// sources acknowledge upstream after step-1 only, so "a webhook that arrived
/// is not lost across a crash"). Any external producer — including a pack
/// source driver (§6) — uses this same path, which is why it is the universal
/// emit endpoint.
pub(crate) async fn webhook_ingest(
    project: &str,
    trigger_id: &str,
    provided_token: Option<&str>,
    idem_key: Option<&str>,
    body: &Json,
) -> Result<EnqueueOutcome, TriggerError> {
    let manifest = read_manifest(project)?;
    let t = manifest
        .get("triggers")
        .and_then(Json::as_array)
        .and_then(|ts| {
            ts.iter()
                .find(|t| t.get("id").and_then(Json::as_str) == Some(trigger_id))
        })
        .ok_or_else(|| TriggerError::Manifest(format!("no trigger '{trigger_id}' in manifest")))?;
    // The ingress (the universal emit endpoint) accepts the passive `webhook`
    // source AND any pack source (a recognised non-core kind, ADR 0025 §6):
    // pack drivers run out-of-process and POST their events here. Core loop
    // kinds (`cron`/`file`/`manual`) and unrecognised kinds are refused — they
    // are not driven by HTTP ingress.
    let kind = t.get("type").and_then(Json::as_str).unwrap_or("");
    let accepts_ingress = kind == "webhook"
        || (!super::trigger_sources::is_builtin(kind)
            && super::trigger_sources::known_kinds().contains(kind));
    if !accepts_ingress {
        return Err(TriggerError::Manifest(format!(
            "trigger '{trigger_id}' (type '{kind}') does not accept webhook ingress"
        )));
    }
    if let Some(auth) = t
        .get("auth")
        .and_then(Json::as_str)
        .filter(|s| !s.is_empty())
    {
        check_webhook_secret(auth, provided_token)?;
    }
    enqueue(project, trigger_id, idem_key, body).await
}

/// Enforce a webhook's `auth` shared secret. `auth` names an environment
/// variable (either `env:VARNAME` or a bare `VARNAME`) holding the expected
/// secret — secrets are never inlined in the bundle (ADR 0025 §1 / 0024). An
/// unset secret env **fails closed** (the hook is declared protected but the
/// deployment hasn't supplied the secret). HMAC signatures / connection-backed
/// secrets are a later refinement (§Open questions).
fn check_webhook_secret(auth: &str, provided: Option<&str>) -> Result<(), TriggerError> {
    let var = auth.strip_prefix("env:").unwrap_or(auth);
    let expected = std::env::var(var).unwrap_or_default();
    if expected.is_empty() {
        return Err(TriggerError::Apply(format!(
            "webhook secret env '{var}' is unset; refusing to accept unauthenticated event"
        )));
    }
    match provided {
        Some(tok) if constant_time_eq(tok.as_bytes(), expected.as_bytes()) => Ok(()),
        _ => Err(TriggerError::Unauthorized(
            "invalid or missing webhook token".to_string(),
        )),
    }
}

/// Length-independent-leaking constant-time byte comparison (avoids an
/// early-return timing side channel on the shared secret).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A `pending` inbox row that is due for dispatch.
#[derive(Debug, Clone)]
pub(crate) struct InboxRow {
    pub id: i64,
    pub trigger_id: String,
    pub body: Json,
    pub attempts: i64,
}

/// Read the FIFO-ordered `pending` rows whose `next_at` has arrived (step 2
/// input). Ordering is by insert order (`id`); there is no cross-source order
/// guarantee (triggers are independent Zapier-style automations).
pub(crate) async fn claim_due(
    project: &str,
    now: i64,
    limit: i64,
) -> Result<Vec<InboxRow>, TriggerError> {
    let res = run_inbox_op(
        project,
        json!({
            "op": "query",
            "sql": "SELECT id, trigger_id, body, attempts FROM trigger_inbox \
                    WHERE status = 'pending' AND next_at <= ? ORDER BY id ASC LIMIT ?",
            "params": [now, limit],
        }),
    )
    .await?;
    let rows = res
        .get("rows")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            Some(InboxRow {
                id: r.get("id")?.as_i64()?,
                trigger_id: r.get("trigger_id")?.as_str()?.to_string(),
                body: r
                    .get("body")
                    .and_then(Json::as_str)
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or(Json::Null),
                attempts: r.get("attempts").and_then(Json::as_i64).unwrap_or(0),
            })
        })
        .collect())
}

/// Settle a row as applied (step 3, success path).
pub(crate) async fn mark_done(project: &str, id: i64) -> Result<(), TriggerError> {
    run_inbox_op(
        project,
        json!({
            "op": "exec",
            "sql": "UPDATE trigger_inbox SET status = 'done', updated_at = ? WHERE id = ?",
            "params": [now_ms() as i64, id],
        }),
    )
    .await?;
    Ok(())
}

/// The result of settling a failed dispatch: whether the row was dead-lettered
/// and when it becomes eligible again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Settlement {
    pub terminal: bool,
    pub attempts: i64,
    pub next_at: i64,
}

/// Settle a row after a failed dispatch (step 3, failure path): increment
/// `attempts`; keep it `pending` with an exponential-backoff `next_at` until it
/// reaches [`MAX_ATTEMPTS`], then dead-letter it (`failed`, terminal).
pub(crate) async fn settle_failure(
    project: &str,
    id: i64,
    current_attempts: i64,
    err: &str,
    now: i64,
) -> Result<Settlement, TriggerError> {
    let attempts = current_attempts + 1;
    let terminal = attempts >= MAX_ATTEMPTS;
    let (status, next_at) = if terminal {
        ("failed", now)
    } else {
        ("pending", now + backoff_ms(attempts) as i64)
    };
    // Keep dead-letter errors readable and the column bounded.
    let err = err.chars().take(500).collect::<String>();
    run_inbox_op(
        project,
        json!({
            "op": "exec",
            "sql": "UPDATE trigger_inbox \
                    SET status = ?, attempts = ?, next_at = ?, last_error = ?, updated_at = ? \
                    WHERE id = ?",
            "params": [status, attempts, next_at, err, now, id],
        }),
    )
    .await?;
    Ok(Settlement {
        terminal,
        attempts,
        next_at,
    })
}

fn backoff_ms(attempt: i64) -> u64 {
    let shift = (attempt.max(1) - 1).min(20) as u32;
    BACKOFF_BASE_MS
        .saturating_mul(1u64 << shift)
        .min(BACKOFF_CAP_MS)
}

// ---------------------------------------------------------------------------
// Actions — resolve from the manifest, plan the engine call via FEEL
// ---------------------------------------------------------------------------

/// A trigger's engine mapping (ADR 0025 §1), parsed from the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Action {
    /// Start a process; `variables` is FEEL over the event body.
    Start {
        process: String,
        variables: Option<String>,
    },
    /// Publish a `CorrelateMessage`; `correlation_key`/`variables` are FEEL.
    Message {
        name: String,
        correlation_key: Option<String>,
        variables: Option<String>,
    },
}

/// The concrete engine call after FEEL evaluation — what the gateway receives.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EngineCall {
    Start {
        process: String,
        variables: Json,
    },
    Message {
        name: String,
        correlation_key: String,
        variables: Json,
    },
}

/// Resolve the `action` for `trigger_id` from the App manifest's `triggers[]`.
/// `Ok(None)` when the manifest declares no such trigger.
pub(crate) fn resolve_action(
    project: &str,
    trigger_id: &str,
) -> Result<Option<Action>, TriggerError> {
    let manifest = read_manifest(project)?;
    let triggers = match manifest.get("triggers").and_then(Json::as_array) {
        Some(t) => t,
        None => return Ok(None),
    };
    let Some(t) = triggers
        .iter()
        .find(|t| t.get("id").and_then(Json::as_str) == Some(trigger_id))
    else {
        return Ok(None);
    };
    let action = t
        .get("action")
        .ok_or_else(|| TriggerError::Manifest(format!("trigger '{trigger_id}' has no action")))?;
    parse_action(action, trigger_id).map(Some)
}

fn parse_action(action: &Json, trigger_id: &str) -> Result<Action, TriggerError> {
    let variables = action
        .get("variables")
        .and_then(Json::as_str)
        .map(str::to_string);
    if let Some(process) = action.get("start").and_then(Json::as_str) {
        return Ok(Action::Start {
            process: process.to_string(),
            variables,
        });
    }
    if let Some(name) = action.get("message").and_then(Json::as_str) {
        return Ok(Action::Message {
            name: name.to_string(),
            correlation_key: action
                .get("correlationKey")
                .and_then(Json::as_str)
                .map(str::to_string),
            variables,
        });
    }
    Err(TriggerError::Manifest(format!(
        "trigger '{trigger_id}' action has neither start nor message"
    )))
}

/// Evaluate a trigger `action`'s FEEL over `body` to produce the engine call.
/// The FEEL scope is `{ body: <event body> }` (ADR 0029 §5 / 0025 §1).
pub(crate) fn plan_call(action: &Action, body: &Json) -> Result<EngineCall, TriggerError> {
    let mut ctx: HashMap<String, nanobpmn_engine_core::Value> = HashMap::new();
    ctx.insert("body".to_string(), nanobpmn_read_model::json_to_value(body));
    match action {
        Action::Start { process, variables } => Ok(EngineCall::Start {
            process: process.clone(),
            variables: eval_variables(variables.as_deref(), &ctx)?,
        }),
        Action::Message {
            name,
            correlation_key,
            variables,
        } => {
            let correlation_key = match correlation_key.as_deref() {
                Some(expr) => nanobpmn_engine_core::feel::eval_string(expr, &ctx)
                    .map_err(|e| TriggerError::Feel(e.to_string()))?,
                None => String::new(),
            };
            Ok(EngineCall::Message {
                name: name.clone(),
                correlation_key,
                variables: eval_variables(variables.as_deref(), &ctx)?,
            })
        }
    }
}

/// Evaluate the optional `variables` FEEL to a JSON object. A `null` result (or
/// no expression) yields `{}`; a non-object result is a FEEL error — the started
/// instance / published message needs a variable context, not a scalar.
fn eval_variables(
    expr: Option<&str>,
    ctx: &HashMap<String, nanobpmn_engine_core::Value>,
) -> Result<Json, TriggerError> {
    let Some(expr) = expr else {
        return Ok(json!({}));
    };
    let v = nanobpmn_engine_core::feel::eval(expr, ctx)
        .map_err(|e| TriggerError::Feel(e.to_string()))?;
    match nanobpmn_read_model::value_to_json(&v) {
        Json::Null => Ok(json!({})),
        obj @ Json::Object(_) => Ok(obj),
        other => Err(TriggerError::Feel(format!(
            "variables expression must yield a context/object, got {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Applying the call over the local gateway (SDK-style)
// ---------------------------------------------------------------------------

/// Perform the engine call against the gateway at `base_url` exactly like an SDK
/// client would (ADR 0025 §4). `start` → `POST /v2/process-instances`;
/// `message` → `POST /v2/messages/publication` (Nano's unbuffered
/// `CorrelateMessage`). A non-2xx or transport error is a transient
/// [`TriggerError::Apply`], so the row is retried.
async fn perform_over_gateway(base_url: &str, call: &EngineCall) -> Result<(), TriggerError> {
    let client = reqwest::Client::new();
    let (url, payload) = match call {
        EngineCall::Start { process, variables } => (
            format!("{base_url}/v2/process-instances"),
            json!({ "processDefinitionId": process, "variables": variables }),
        ),
        EngineCall::Message {
            name,
            correlation_key,
            variables,
        } => (
            format!("{base_url}/v2/messages/publication"),
            json!({ "name": name, "correlationKey": correlation_key, "variables": variables }),
        ),
    };
    let resp = client
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&payload).unwrap_or_default())
        .send()
        .await
        .map_err(|e| TriggerError::Apply(format!("gateway request failed: {e}")))?;
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        let body = body.chars().take(200).collect::<String>();
        Err(TriggerError::Apply(format!("gateway {status}: {body}")))
    }
}

// ---------------------------------------------------------------------------
// The dispatcher — one drain pass, and the always-on supervised loop
// ---------------------------------------------------------------------------

/// The outcome of one [`drain_over_gateway`] pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DrainStats {
    pub claimed: usize,
    pub done: usize,
    pub retried: usize,
    pub dead_lettered: usize,
}

/// One full drain pass over `project`'s inbox: claim due rows, apply each row's
/// action over the gateway, and settle. This is the dispatcher's unit of work
/// (ADR 0025 §4). Restart-safe: rows left `pending` by a crash are simply
/// re-claimed on the next pass (at-least-once, §2).
pub(crate) async fn drain_over_gateway(
    project: &str,
    base_url: &str,
) -> Result<DrainStats, TriggerError> {
    ensure_inbox(project).await?;
    let now = now_ms() as i64;
    let rows = claim_due(project, now, DRAIN_BATCH).await?;
    let mut stats = DrainStats {
        claimed: rows.len(),
        ..Default::default()
    };
    for row in rows {
        match dispatch_row(project, &row, base_url).await {
            Ok(()) => {
                mark_done(project, row.id).await?;
                stats.done += 1;
            }
            Err(e) => {
                let s = settle_failure(
                    project,
                    row.id,
                    row.attempts,
                    &e.to_string(),
                    now_ms() as i64,
                )
                .await?;
                if s.terminal {
                    stats.dead_lettered += 1;
                } else {
                    stats.retried += 1;
                }
            }
        }
    }
    Ok(stats)
}

/// Resolve + plan + apply one row's action (dispatch, step 2). Any error leaves
/// the row for [`settle_failure`] to retry/dead-letter.
async fn dispatch_row(project: &str, row: &InboxRow, base_url: &str) -> Result<(), TriggerError> {
    let action = resolve_action(project, &row.trigger_id)?.ok_or_else(|| {
        TriggerError::Manifest(format!("no trigger '{}' in manifest", row.trigger_id))
    })?;
    let call = plan_call(&action, &row.body)?;
    perform_over_gateway(base_url, &call).await
}

/// Inbox status for the Triggers panel: row counts by state + the most recently
/// updated rows.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxStatus {
    pub pending: i64,
    pub done: i64,
    pub failed: i64,
    pub recent: Vec<InboxStatusRow>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxStatusRow {
    pub id: i64,
    #[serde(rename = "triggerId")]
    pub trigger_id: String,
    pub status: String,
    pub attempts: i64,
    #[serde(rename = "lastError")]
    pub last_error: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
}

/// Read the inbox counts + recent rows.
pub(crate) async fn inbox_status(project: &str) -> Result<InboxStatus, TriggerError> {
    ensure_inbox(project).await?;
    let counts = run_inbox_op(
        project,
        json!({
            "op": "query",
            "sql": "SELECT status, COUNT(*) AS n FROM trigger_inbox GROUP BY status",
        }),
    )
    .await?;
    let mut pending = 0;
    let mut done = 0;
    let mut failed = 0;
    for r in counts
        .get("rows")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let n = r.get("n").and_then(Json::as_i64).unwrap_or(0);
        match r.get("status").and_then(Json::as_str) {
            Some("pending") => pending = n,
            Some("done") => done = n,
            Some("failed") => failed = n,
            _ => {}
        }
    }
    let recent = run_inbox_op(
        project,
        json!({
            "op": "query",
            "sql": "SELECT id, trigger_id, status, attempts, last_error, created_at \
                    FROM trigger_inbox ORDER BY updated_at DESC, id DESC LIMIT ?",
            "params": [RECENT_LIMIT],
        }),
    )
    .await?;
    let recent = recent
        .get("rows")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| {
            Some(InboxStatusRow {
                id: r.get("id")?.as_i64()?,
                trigger_id: r.get("trigger_id")?.as_str()?.to_string(),
                status: r.get("status")?.as_str()?.to_string(),
                attempts: r.get("attempts").and_then(Json::as_i64).unwrap_or(0),
                last_error: r
                    .get("last_error")
                    .and_then(Json::as_str)
                    .map(str::to_string),
                created_at: r.get("created_at").and_then(Json::as_i64).unwrap_or(0),
            })
        })
        .collect();
    Ok(InboxStatus {
        pending,
        done,
        failed,
        recent,
    })
}

// ---------------------------------------------------------------------------
// Triggers overview — declared triggers resolved against the source registry
// ---------------------------------------------------------------------------

/// The manifest's declared triggers plus the recognised source registry (ADR
/// 0025 phase 2). Powers the Triggers panel and source picker: each trigger is
/// tagged `builtin`/`recognized` against [`super::trigger_sources::known_kinds`]
/// (core kinds ∪ installed-pack kinds), and any source-config error (e.g. a bad
/// cron spec) is surfaced.
pub(crate) async fn triggers_overview(project: &str) -> Result<Json, TriggerError> {
    let manifest = read_manifest(project)?;
    let known = super::trigger_sources::known_kinds();
    let (_, errors) = super::trigger_sources::parse_sources(&manifest);

    let triggers: Vec<Json> = manifest
        .get("triggers")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|t| {
            let id = t.get("id").and_then(Json::as_str)?.to_string();
            let kind = t
                .get("type")
                .and_then(Json::as_str)
                .unwrap_or("")
                .to_string();
            Some(json!({
                "id": id,
                "type": kind,
                "builtin": super::trigger_sources::is_builtin(&kind),
                "recognized": known.contains(&kind),
                "action": t.get("action").cloned(),
            }))
        })
        .collect();

    let sources: Vec<Json> = known
        .iter()
        .map(|k| {
            json!({
                "kind": k,
                "builtin": super::trigger_sources::is_builtin(k),
                "displayName": super::trigger_sources::display_name(k),
                "configFields": config_fields_json(k),
            })
        })
        .collect();

    Ok(json!({ "triggers": triggers, "sources": sources, "errors": errors }))
}

/// The config fields the console's Add-trigger form renders for a source `kind`:
/// the core kinds' known manifest keys, or a pack's declared `configFields`.
fn config_fields_json(kind: &str) -> Vec<Json> {
    let field = |key: &str, label: &str, desc: &str, required: bool| json!({ "key": key, "label": label, "description": desc, "default": Json::Null, "required": required });
    match kind {
        "cron" => vec![
            field(
                "spec",
                "Cron expression",
                "5-field crontab, e.g. */5 * * * *",
                true,
            ),
            field(
                "onMissed",
                "On missed",
                "catchup or skip (default skip)",
                false,
            ),
        ],
        "file" => vec![
            field("path", "Path to watch", "A file or directory path", true),
            field(
                "pollMs",
                "Poll interval (ms)",
                "How often to poll (default 1000)",
                false,
            ),
        ],
        // webhook and manual take no source config (webhook's ingress path is the
        // trigger id); pack kinds contribute their own declared fields.
        "webhook" | "manual" => vec![],
        _ => super::extensions::all_trigger_sources()
            .into_iter()
            .find(|s| s.kind == kind)
            .map(|s| {
                s.config_fields
                    .into_iter()
                    .map(|f| {
                        json!({
                            "key": f.key,
                            "label": f.label,
                            "description": f.description,
                            "default": f.default,
                            "required": false,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// Append a trigger to the App manifest's `triggers[]` and persist it. Places the
/// supplied flat `config` map into the shape each source kind expects (core kinds
/// read some keys at the trigger's top level; pack kinds read a nested `config`).
pub(crate) fn add_trigger(
    project: &str,
    id: &str,
    kind: &str,
    config: &std::collections::BTreeMap<String, String>,
    connection: Option<&str>,
    action: &Json,
) -> Result<(), TriggerError> {
    let id = id.trim();
    if id.is_empty() {
        return Err(TriggerError::Manifest("trigger id is required".into()));
    }
    if kind.trim().is_empty() {
        return Err(TriggerError::Manifest(
            "trigger type (source kind) is required".into(),
        ));
    }
    if !super::trigger_sources::known_kinds().contains(kind) {
        return Err(TriggerError::Manifest(format!(
            "unknown source kind '{kind}' — install its pack or pick a recognised kind"
        )));
    }
    if !action.is_object() || action.as_object().map(|o| o.is_empty()).unwrap_or(true) {
        return Err(TriggerError::Manifest(
            "an action is required (e.g. start a process or publish a message)".into(),
        ));
    }

    let mut manifest = read_manifest(project)?;
    let triggers = manifest
        .as_object_mut()
        .ok_or_else(|| TriggerError::Manifest("nano.app.json is not a JSON object".into()))?
        .entry("triggers")
        .or_insert_with(|| Json::Array(vec![]));
    let arr = triggers
        .as_array_mut()
        .ok_or_else(|| TriggerError::Manifest("manifest 'triggers' is not an array".into()))?;
    if arr
        .iter()
        .any(|t| t.get("id").and_then(Json::as_str) == Some(id))
    {
        return Err(TriggerError::Manifest(format!(
            "a trigger with id '{id}' already exists"
        )));
    }

    let mut obj = serde_json::Map::new();
    obj.insert("id".into(), json!(id));
    obj.insert("type".into(), json!(kind));

    // Field placement: core kinds read some keys at the top level; everything
    // else (incl. pack kinds) rides a nested `config` object. Empty values are
    // dropped so the manifest stays clean.
    let mut nested = serde_json::Map::new();
    for (k, v) in config {
        let v = v.trim();
        if v.is_empty() {
            continue;
        }
        let top_level = matches!(
            (kind, k.as_str()),
            ("cron", "spec") | ("cron", "onMissed") | ("file", "path")
        );
        if kind == "file" && k == "pollMs" {
            if let Ok(n) = v.parse::<u64>() {
                nested.insert("pollMs".into(), json!(n));
            }
        } else if top_level {
            obj.insert(k.clone(), json!(v));
        } else {
            nested.insert(k.clone(), json!(v));
        }
    }
    if !nested.is_empty() {
        obj.insert("config".into(), Json::Object(nested));
    }
    if let Some(c) = connection.map(str::trim).filter(|c| !c.is_empty()) {
        obj.insert("connection".into(), json!(c));
    }
    obj.insert("action".into(), action.clone());

    arr.push(Json::Object(obj));
    write_manifest(project, &manifest)?;
    Ok(())
}

/// Persist the App manifest, pretty-printed (nano.app.json is strict JSON).
pub(crate) fn write_manifest(project: &str, manifest: &Json) -> Result<(), TriggerError> {
    let dir = projects::project_dir(project)
        .ok_or_else(|| TriggerError::Manifest("invalid project name".to_string()))?;
    let text = serde_json::to_string_pretty(manifest)
        .map_err(|e| TriggerError::Manifest(format!("could not serialize manifest: {e}")))?;
    std::fs::write(dir.join("nano.app.json"), format!("{text}\n"))
        .map_err(|e| TriggerError::Apply(format!("could not write nano.app.json: {e}")))
}

// ---------------------------------------------------------------------------
// Supervision — one drain loop per running App that declares triggers
// ---------------------------------------------------------------------------

/// Shared stop signal for a project's supervised tasks — the drain loop and
/// every in-process source loop ([`super::trigger_sources`]) share one, so a
/// single [`TriggerDispatcher::stop`] tears them all down together.
pub(crate) struct LoopHandle {
    running: AtomicBool,
    stop: Notify,
}

impl LoopHandle {
    /// Whether the supervised tasks should keep running.
    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Resolves when [`TriggerDispatcher::stop`] fires, so a source loop can
    /// `select!` on it to wake immediately rather than after its next poll.
    pub(crate) async fn stopped(&self) {
        self.stop.notified().await;
    }

    /// Test-only: a fresh running handle for driving [`super::trigger_sources::
    /// spawn_sources`] directly (outside the dispatcher).
    #[cfg(test)]
    pub(crate) fn new_running() -> Arc<Self> {
        Arc::new(Self {
            running: AtomicBool::new(true),
            stop: Notify::new(),
        })
    }

    /// Test-only: stop supervised tasks (mirrors [`TriggerDispatcher::stop`]).
    #[cfg(test)]
    pub(crate) fn stop_for_test(&self) {
        self.running.store(false, Ordering::Relaxed);
        self.stop.notify_waiters();
    }
}

/// Owns a drain loop per project (ADR 0025 §4). Modeled on the worker/project
/// supervisors: [`ensure_started`](TriggerDispatcher::ensure_started) is
/// idempotent, and [`stop`](TriggerDispatcher::stop) tears the loop down.
pub(crate) struct TriggerDispatcher {
    loops: Mutex<HashMap<String, Arc<LoopHandle>>>,
}

/// The process-wide dispatcher.
pub(crate) fn dispatcher() -> &'static TriggerDispatcher {
    static D: OnceLock<TriggerDispatcher> = OnceLock::new();
    D.get_or_init(|| TriggerDispatcher {
        loops: Mutex::new(HashMap::new()),
    })
}

impl TriggerDispatcher {
    /// Start the drain loop **and the in-process source loops** for `project`
    /// if it declares `triggers[]` (inbound) or `workers[]` (outbound, ADR 0050)
    /// and isn't already running. Called from the project run path; idempotent.
    pub(crate) async fn ensure_started(&self, project: &str, base_url: String) {
        // Only running Apps that actually declare triggers pay for a loop.
        let Ok(manifest) = read_manifest(project) else {
            return;
        };
        let has_triggers = manifest
            .get("triggers")
            .and_then(Json::as_array)
            .map(|t| !t.is_empty())
            .unwrap_or(false);
        // A connector-only App (ADR 0050) enables workers[] but no triggers[];
        // it still needs the supervised loop (to keep its workers alive), so
        // gate on either edge being present.
        let has_workers = manifest
            .get("workers")
            .and_then(Json::as_array)
            .map(|w| !w.is_empty())
            .unwrap_or(false);
        if !has_triggers && !has_workers {
            return;
        }
        let mut loops = self.loops.lock().await;
        if loops.contains_key(project) {
            return;
        }
        let handle = Arc::new(LoopHandle {
            running: AtomicBool::new(true),
            stop: Notify::new(),
        });
        loops.insert(project.to_string(), handle.clone());
        // Spawn the in-process source drivers (cron/file); webhook + pack
        // sources emit via the ingress and spawn no loop. They share `handle`,
        // so `stop` tears them down with the drain loop below.
        super::trigger_sources::spawn_sources(project, &manifest, handle.clone());
        // Spawn + supervise the outbound connector workers (ADR 0050 §4) under
        // the same `handle`, so `stop` tears them down alongside the sources.
        super::trigger_sources::spawn_workers(project, &manifest, handle.clone());
        let project = project.to_string();
        // Only an App that declares inbound triggers[] needs the periodic inbox
        // drain — its inbox is the only thing that can ever hold rows. A
        // connector-only App (workers[], no triggers[]) still gets the
        // supervised source/worker loops above, but spawning a drain here would
        // poll (and, pre-#1340, re-`CREATE TABLE`) an inbox that can never
        // receive anything — two `urban data` process spawns every
        // POLL_INTERVAL_MS, pinning a core on a small host (#1340).
        if !has_triggers {
            return;
        }
        tokio::spawn(async move {
            while handle.is_running() {
                let wait = match drain_over_gateway(&project, &base_url).await {
                    Ok(_) => Duration::from_millis(POLL_INTERVAL_MS),
                    // A drain error (no datasource yet, engine not up) backs off
                    // quietly rather than spinning.
                    Err(_) => Duration::from_millis(ERROR_BACKOFF_MS),
                };
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = handle.stopped() => break,
                }
            }
        });
    }

    /// Stop and forget `project`'s drain loop. Idempotent.
    pub(crate) async fn stop(&self, project: &str) {
        if let Some(h) = self.loops.lock().await.remove(project) {
            h.running.store(false, Ordering::Relaxed);
            h.stop.notify_waiters();
        }
    }

    /// Whether a drain loop is registered for `project` (tests/introspection).
    #[cfg(test)]
    pub(crate) async fn is_running(&self, project: &str) -> bool {
        self.loops.lock().await.contains_key(project)
    }
}

// ---------------------------------------------------------------------------
// Manifest access
// ---------------------------------------------------------------------------

pub(crate) fn read_manifest(project: &str) -> Result<Json, TriggerError> {
    let dir = projects::project_dir(project)
        .ok_or_else(|| TriggerError::Manifest("invalid project name".to_string()))?;
    let path = dir.join("nano.app.json");
    let text = std::fs::read_to_string(&path)
        .map_err(|_| TriggerError::Manifest("nano.app.json not found".to_string()))?;
    serde_json::from_str(&text)
        .map_err(|e| TriggerError::Manifest(format!("nano.app.json is not valid JSON: {e}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::MutexGuard;
    use std::sync::atomic::AtomicU64;

    use super::super::workers;
    use super::*;

    /// The memo hit is gated on the datasource fingerprint, not just the name:
    /// a changed fingerprint (re-import / manifest edit) or an unresolved one
    /// must miss, so `ensure_inbox` re-`CREATE`s rather than trusting a stale
    /// datasource (#1340). Pure — needs no datasource/runtime.
    #[test]
    fn memo_hit_requires_matching_datasource_fingerprint() {
        let mut memo = InboxMemo::default();
        memo.ensured.insert("proj".to_string(), 0xA11CE);

        assert!(
            memo.hits("proj", Some(0xA11CE)),
            "same datasource fingerprint is a hit"
        );
        assert!(
            !memo.hits("proj", Some(0xB0B)),
            "a changed fingerprint (datasource repointed) must miss"
        );
        assert!(
            !memo.hits("proj", None),
            "an unresolved datasource must never hit (fail closed)"
        );
        assert!(
            !memo.hits("other", Some(0xA11CE)),
            "a different project never hits on another's fingerprint"
        );
    }

    /// The guarded memo insert itself: a `record_ensured` arriving with a
    /// generation the memo has already moved past (a `forget_inbox` landed
    /// while its `CREATE` was in flight) must be refused, so a stale
    /// continuation cannot resurrect a marker for a torn-down datasource. This
    /// is the unit the async `ensure_inbox_does_not_resurrect_a_marker_forgotten_mid_create`
    /// test cannot reach deterministically (its await completes before the
    /// forget), so it is exercised directly here. Pure — needs no
    /// datasource/runtime.
    #[test]
    fn record_ensured_rejects_a_stale_generation() {
        let mut memo = InboxMemo::default();

        // Current generation, resolvable fingerprint: the marker is recorded.
        assert!(memo.record_ensured("proj", Some(0xA11CE), 0));
        assert!(memo.hits("proj", Some(0xA11CE)));

        // A teardown bumps the generation and drops the marker…
        memo.ensured.remove("proj");
        *memo.generations.entry("proj".to_string()).or_insert(0) += 1;
        // …so the in-flight ensure from generation 0 is refused: no marker.
        assert!(
            !memo.record_ensured("proj", Some(0xA11CE), 0),
            "a stale-generation insert must be rejected"
        );
        assert!(
            !memo.hits("proj", Some(0xA11CE)),
            "no marker may be resurrected for the torn-down datasource"
        );

        // The next ensure runs under the new generation and records again.
        assert!(memo.record_ensured("proj", Some(0xA11CE), 1));
        assert!(memo.hits("proj", Some(0xA11CE)));

        // An unresolvable fingerprint never memoises (fail closed), but is not
        // treated as stale.
        assert!(memo.record_ensured("other", None, 0));
        assert!(
            !memo.hits("other", None),
            "a None fingerprint is never memoised"
        );
    }

    /// Serializes tests that mutate the process-global `NANOBPMN_PROJECTS_DIR`.
    /// This is the crate-wide guard ([`projects::env_lock`]) — a module-local
    /// mutex would not serialize against the lifecycle tests in `lib.rs` or the
    /// suites in `projects.rs` / `connectors.rs` mutating the same variable.
    fn lock() -> MutexGuard<'static, ()> {
        projects::env_lock()
    }

    fn runtime_available() -> bool {
        workers::usable_node().is_some() || workers::find_deno().is_some()
    }

    /// Grace period a withheld-pack test waits before concluding no child
    /// launched. A safety net, not a correctness signal: a launched driver/worker
    /// reaches out within ~1s locally, and the happy-path twins observe a launch
    /// well inside this budget; 8s leaves ample headroom for a slow cold start on
    /// a loaded CI runner without the per-poll cost of the (~500ms) inbox query.
    const WITHHELD_GRACE: Duration = Duration::from_secs(8);

    /// Materialise a fresh Urban App project (sqlite datasource + optional
    /// `triggers[]`) under a unique projects root, and return its name.
    fn setup_app(triggers_json: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "nano-trig-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        unsafe {
            std::env::set_var("NANOBPMN_PROJECTS_DIR", &root);
        }
        let name = "trigapp";
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("db/migrations")).unwrap();
        std::fs::write(
            dir.join("nano.app.json"),
            format!(
                r#"{{ "data": {{ "default": "app", "sources": {{
                    "app": {{ "driver": "sqlite", "url": "file:./app.db", "migrations": "db/migrations" }}
                }} }}{triggers_json} }}"#
            ),
        )
        .unwrap();
        projects::ensure_project_sdk(name).unwrap();
        // Each setup creates a *fresh* datasource under the same project name,
        // so clear the process-wide inbox memo (#1340) — otherwise a prior
        // test's "inbox ensured" marker would skip creating the table here.
        forget_inbox(name);
        name.to_string()
    }

    // --- pure FEEL planning (no datasource; always runs) ------------------

    #[test]
    fn plan_call_start_evaluates_variables_feel() {
        let action = Action::Start {
            process: "heating".to_string(),
            variables: Some("= {room: body.room, target: body.target}".to_string()),
        };
        let body = json!({ "room": "kitchen", "target": 21 });
        let call = plan_call(&action, &body).expect("plan");
        assert_eq!(
            call,
            EngineCall::Start {
                process: "heating".to_string(),
                variables: json!({ "room": "kitchen", "target": 21 }),
            }
        );
    }

    #[test]
    fn plan_call_start_without_variables_defaults_to_empty_object() {
        let action = Action::Start {
            process: "p".to_string(),
            variables: None,
        };
        let call = plan_call(&action, &json!({ "x": 1 })).expect("plan");
        assert_eq!(
            call,
            EngineCall::Start {
                process: "p".to_string(),
                variables: json!({}),
            }
        );
    }

    #[test]
    fn plan_call_message_evaluates_correlation_key_feel() {
        let action = Action::Message {
            name: "temp-reading".to_string(),
            correlation_key: Some("= body.room".to_string()),
            variables: Some("= {celsius: body.celsius}".to_string()),
        };
        let body = json!({ "room": "kitchen", "celsius": 19.5 });
        let call = plan_call(&action, &body).expect("plan");
        assert_eq!(
            call,
            EngineCall::Message {
                name: "temp-reading".to_string(),
                correlation_key: "kitchen".to_string(),
                variables: json!({ "celsius": 19.5 }),
            }
        );
    }

    #[test]
    fn plan_call_rejects_non_object_variables() {
        let action = Action::Start {
            process: "p".to_string(),
            variables: Some("= body.room".to_string()),
        };
        let err = plan_call(&action, &json!({ "room": "kitchen" }));
        assert!(matches!(err, Err(TriggerError::Feel(_))));
    }

    #[test]
    fn parse_action_reads_start_and_message() {
        let start = parse_action(&json!({ "start": "p", "variables": "= body" }), "t").unwrap();
        assert_eq!(
            start,
            Action::Start {
                process: "p".to_string(),
                variables: Some("= body".to_string())
            }
        );
        let msg = parse_action(
            &json!({ "message": "m", "correlationKey": "= body.k" }),
            "t",
        )
        .unwrap();
        assert_eq!(
            msg,
            Action::Message {
                name: "m".to_string(),
                correlation_key: Some("= body.k".to_string()),
                variables: None
            }
        );
        assert!(parse_action(&json!({ "nope": true }), "t").is_err());
    }

    #[test]
    fn resolve_action_finds_declared_trigger() {
        let _g = lock();
        let name = setup_app(
            r#", "triggers": [
                { "id": "morning", "type": "cron", "spec": "0 6 * * *",
                  "action": { "start": "heating", "variables": "= {room: body.room}" } }
            ]"#,
        );
        let action = resolve_action(&name, "morning")
            .expect("resolve")
            .expect("some");
        assert_eq!(
            action,
            Action::Start {
                process: "heating".to_string(),
                variables: Some("= {room: body.room}".to_string()),
            }
        );
        // Unknown trigger id resolves to None (not an error).
        assert!(resolve_action(&name, "nope").expect("resolve").is_none());
    }

    // --- durable inbox (datasource; runtime-gated) ------------------------

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // serialises on the shared PROJECTS_DIR env across awaits
    async fn enqueue_dedups_on_idempotency_key() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        let body = json!({ "room": "kitchen" });
        let first = enqueue(&name, "t", Some("k1"), &body)
            .await
            .expect("enqueue");
        assert!(first.enqueued, "first enqueue persists");
        let second = enqueue(&name, "t", Some("k1"), &body)
            .await
            .expect("enqueue");
        assert!(!second.enqueued, "repeat key is a no-op (dedup)");
        let due = claim_due(&name, now_ms() as i64, 10).await.expect("claim");
        assert_eq!(due.len(), 1, "only one row despite two enqueues");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn derived_key_dedups_identical_bodies() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        let body = json!({ "a": 1 });
        assert!(enqueue(&name, "t", None, &body).await.unwrap().enqueued);
        assert!(!enqueue(&name, "t", None, &body).await.unwrap().enqueued);
        // A different body is a distinct event.
        assert!(
            enqueue(&name, "t", None, &json!({ "a": 2 }))
                .await
                .unwrap()
                .enqueued
        );
        let due = claim_due(&name, now_ms() as i64, 10).await.expect("claim");
        assert_eq!(due.len(), 2);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn claim_is_fifo_and_mark_done_removes_from_due() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        enqueue(&name, "t", Some("a"), &json!({ "n": 1 }))
            .await
            .unwrap();
        enqueue(&name, "t", Some("b"), &json!({ "n": 2 }))
            .await
            .unwrap();
        let due = claim_due(&name, now_ms() as i64, 10).await.expect("claim");
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].body, json!({ "n": 1 }), "FIFO by insert order");
        mark_done(&name, due[0].id).await.expect("done");
        let due2 = claim_due(&name, now_ms() as i64, 10).await.expect("claim");
        assert_eq!(due2.len(), 1);
        assert_eq!(due2[0].body, json!({ "n": 2 }));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn at_least_once_redelivers_unsettled_row() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        enqueue(&name, "t", Some("k"), &json!({ "n": 1 }))
            .await
            .unwrap();
        // First drain: claim + "apply" but crash before settling (no mark_done).
        let due = claim_due(&name, now_ms() as i64, 10).await.expect("claim");
        assert_eq!(due.len(), 1);
        // Second drain (after restart): the unsettled row is still pending.
        let redue = claim_due(&name, now_ms() as i64, 10).await.expect("claim");
        assert_eq!(
            redue.len(),
            1,
            "unsettled row is re-delivered (at-least-once)"
        );
        assert_eq!(redue[0].id, due[0].id);
        // Now settle it.
        mark_done(&name, redue[0].id).await.expect("done");
        let after = claim_due(&name, now_ms() as i64, 10).await.expect("claim");
        assert!(after.is_empty(), "settled row is no longer due");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn retry_backoff_then_dead_letter() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        enqueue(&name, "t", Some("k"), &json!({ "n": 1 }))
            .await
            .unwrap();
        let now = now_ms() as i64;
        let row = claim_due(&name, now, 10).await.expect("claim")[0].clone();

        // First failure: stays pending but with a future next_at (backoff).
        let s1 = settle_failure(&name, row.id, row.attempts, "boom", now)
            .await
            .expect("settle");
        assert!(!s1.terminal);
        assert_eq!(s1.attempts, 1);
        assert!(s1.next_at > now, "backoff schedules a future retry");
        assert!(
            claim_due(&name, now, 10).await.unwrap().is_empty(),
            "not due yet"
        );
        let far = s1.next_at + 1;
        let due = claim_due(&name, far, 10).await.expect("claim");
        assert_eq!(due.len(), 1, "due again after backoff elapses");
        assert_eq!(due[0].attempts, 1);

        // Exhaust attempts → dead-letter (terminal 'failed', never re-claimed).
        let mut attempts = 1;
        let mut terminal = false;
        for _ in 0..MAX_ATTEMPTS {
            let s = settle_failure(&name, row.id, attempts, "boom", now_ms() as i64)
                .await
                .expect("settle");
            attempts = s.attempts;
            terminal = s.terminal;
            if terminal {
                break;
            }
        }
        assert!(terminal, "row is dead-lettered after MAX_ATTEMPTS");
        let far_future = now_ms() as i64 + 10_000_000;
        assert!(
            claim_due(&name, far_future, 10).await.unwrap().is_empty(),
            "dead-lettered rows are never re-claimed"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn inbox_status_counts_by_state() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        enqueue(&name, "t", Some("a"), &json!({})).await.unwrap();
        enqueue(&name, "t", Some("b"), &json!({})).await.unwrap();
        let due = claim_due(&name, now_ms() as i64, 10).await.unwrap();
        mark_done(&name, due[0].id).await.unwrap();
        let st = inbox_status(&name).await.expect("status");
        assert_eq!(st.done, 1);
        assert_eq!(st.pending, 1);
        assert_eq!(st.failed, 0);
        assert_eq!(st.recent.len(), 2);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn dispatcher_skips_apps_without_triggers() {
        let _g = lock();
        // A manifest with no triggers[] must not spawn a drain loop.
        let name = setup_app("");
        dispatcher()
            .ensure_started(&name, "http://127.0.0.1:1".to_string())
            .await;
        assert!(
            !dispatcher().is_running(&name).await,
            "no loop for an App without triggers"
        );
        // stop() is a harmless no-op when nothing is registered.
        dispatcher().stop(&name).await;
        assert!(!dispatcher().is_running(&name).await);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn dispatcher_supervises_workers_only_app_without_draining() {
        let _g = lock();
        // A connector-only App (workers[], no triggers[]) is still supervised so
        // its workers stay alive, but it must NOT spawn the periodic inbox drain
        // — that inbox can never receive anything, and each drain round is two
        // `urban data` process spawns that pin a core on a small host (#1340).
        let name = setup_app(r#", "workers": [ { "type": "test:job" } ]"#);
        // Serial tests share the process-wide memo + dispatcher map under the
        // same `name`; start from a clean slate so the assertion is isolated.
        forget_inbox(&name);
        dispatcher().stop(&name).await;

        dispatcher()
            .ensure_started(&name, "http://127.0.0.1:1".to_string())
            .await;
        assert!(
            dispatcher().is_running(&name).await,
            "a workers-only App stays supervised"
        );

        if runtime_available() {
            // Give a (wrongly) spawned drain loop more than one poll interval to
            // run its first pass, which would memoise the inbox via ensure_inbox.
            tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS + 500)).await;
            assert!(
                !inbox_is_memoised(&name),
                "a workers-only App must never drain (or create) its inbox"
            );
        }
        dispatcher().stop(&name).await;
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn ensure_inbox_is_memoised_until_forgotten() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        // ensure_inbox is called on every drain round, enqueue and status path;
        // it must only spawn `urban data` once per project (#1340). The memo is
        // the observable proxy: once recorded, later calls short-circuit before
        // the spawn.
        let name = setup_app("");
        forget_inbox(&name);
        assert!(!inbox_is_memoised(&name));

        ensure_inbox(&name)
            .await
            .expect("first ensure creates the table");
        assert!(
            inbox_is_memoised(&name),
            "the first successful ensure records the memo"
        );
        // A repeat is a no-op — the memo short-circuits the spawn.
        ensure_inbox(&name)
            .await
            .expect("second ensure is a cheap no-op");
        assert!(inbox_is_memoised(&name));

        // A datasource teardown (e.g. project delete) clears the memo, so the
        // table is re-created on a fresh datasource rather than trusted stale.
        forget_inbox(&name);
        assert!(!inbox_is_memoised(&name), "forget_inbox clears the memo");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn ensure_inbox_does_not_resurrect_a_marker_forgotten_mid_create() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        // The memo write is not atomic with the `CREATE` spawn: an in-flight
        // `ensure_inbox` can still be awaiting its op when `forget_inbox`
        // (project delete / rename / datasource reconfig) tears the datasource
        // down. The continuation must NOT re-insert a marker for the now-stale
        // generation, or a recreated project would skip the `CREATE` and fail on
        // a missing `trigger_inbox` table. We can't intercept the await, so we
        // drive the exact interleaving: forget *between* the create and a
        // re-ensure, and assert the memo reflects only the current generation.
        // The stale-generation rejection itself is exercised directly by
        // `record_ensured_rejects_a_stale_generation` (the await here completes
        // before the forget, so this test alone could not reach it).
        let name = setup_app("");
        forget_inbox(&name);
        let gen0 = inbox_generation(&name);

        ensure_inbox(&name).await.expect("first ensure creates");
        assert!(inbox_is_memoised(&name));
        // The ensure did not bump the generation — only a teardown does.
        assert_eq!(inbox_generation(&name), gen0);

        // Teardown mid-lifecycle: the marker is dropped AND the generation
        // bumps, so any in-flight ensure from the old generation would skip its
        // insert.
        forget_inbox(&name);
        assert!(!inbox_is_memoised(&name));
        assert_eq!(
            inbox_generation(&name),
            gen0 + 1,
            "forget_inbox bumps the invalidation generation"
        );

        // The next ensure re-creates against the fresh datasource and re-memoises
        // under the new generation.
        ensure_inbox(&name).await.expect("re-ensure after forget");
        assert!(inbox_is_memoised(&name));
        assert_eq!(inbox_generation(&name), gen0 + 1);
    }

    /// The memo's fingerprint covers the project dir + manifest bytes, not the
    /// datasource's *contents*: deleting the SQLite file (or dropping the table
    /// through the data exec API) leaves the fingerprint unchanged, so a memo
    /// hit would skip the `CREATE` and inbox ops would fail on a missing
    /// `trigger_inbox` until something else invalidated the memo. `run_inbox_op`
    /// must treat the missing-table error as the invalidation signal: drop the
    /// memo, re-`CREATE`, and retry once — restoring the pre-#1340 self-healing
    /// without the per-op spawn. Regression guard for the review finding on
    /// #1341.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn inbox_ops_self_heal_when_the_database_file_is_deleted() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        ensure_inbox(&name).await.expect("first ensure creates");
        assert!(inbox_is_memoised(&name));

        // Delete the datasource file out from under the memo (the console's
        // file-delete API can do this); the manifest is untouched, so the
        // fingerprint still matches and the memo reads as a hit.
        std::fs::remove_file(
            projects::project_dir(&name)
                .expect("project dir")
                .join("app.db"),
        )
        .expect("delete app.db");

        // The next inbox op hits the missing table, self-heals, and succeeds —
        // where a name/fingerprint-keyed memo alone would keep failing.
        let out = enqueue(&name, "t", Some("after-db-delete"), &json!({}))
            .await
            .expect("enqueue self-heals after the database file is deleted");
        assert!(out.enqueued);
        let st = inbox_status(&name).await.expect("status after heal");
        assert_eq!(st.pending, 1, "the healed inbox holds the enqueued row");
    }

    /// The DROP-TABLE twin of the file-deletion case: a `DROP TABLE
    /// trigger_inbox` issued through the data exec API commits (each op is a
    /// fresh gateway process), leaving the memo stale under an unchanged
    /// manifest. The next op must self-heal the same way.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn inbox_ops_self_heal_when_the_inbox_table_is_dropped() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        ensure_inbox(&name).await.expect("first ensure creates");
        assert!(inbox_is_memoised(&name));

        projects::run_data_op(
            &name,
            json!({ "op": "exec", "sql": "DROP TABLE trigger_inbox" }),
        )
        .await
        .expect("drop the inbox table out from under the memo");

        let out = enqueue(&name, "t", Some("after-drop"), &json!({}))
            .await
            .expect("enqueue self-heals after the inbox table is dropped");
        assert!(out.enqueued);
        let due = claim_due(&name, now_ms() as i64, 10)
            .await
            .expect("claim_due reads the healed inbox");
        assert_eq!(due.len(), 1, "the healed inbox holds the enqueued row");
    }

    /// The self-heal must not mask a genuine SQL error: an op whose statement
    /// is invalid for a reason *other* than the missing inbox table fails, and
    /// the single retry reproduces the same error rather than looping or
    /// swallowing it.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn inbox_ops_do_not_heal_unrelated_sql_errors() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app("");
        ensure_inbox(&name).await.expect("first ensure creates");

        // A bad column reference is an Op error that mentions neither
        // "no such table" nor "trigger_inbox": no heal, error propagates.
        let err = run_inbox_op(
            &name,
            json!({
                "op": "query",
                "sql": "SELECT no_such_column FROM trigger_inbox",
            }),
        )
        .await
        .expect_err("an unrelated SQL error must propagate unchanged");
        assert!(
            matches!(err, TriggerError::Data(DataError::Op(_))),
            "expected a datasource op error, got {err:?}"
        );
        // And the memo is untouched — a genuine op error is not an
        // invalidation signal.
        assert!(inbox_is_memoised(&name));
    }

    // --- pack-source driver auto-launch (ADR 0025 phase 4) -----------------

    /// A dependency-free, cross-runtime (Node/Deno) trigger driver: it reads the
    /// env contract, POSTs two distinct events to the ingress, then stays alive
    /// (so the supervisor doesn't treat its exit as a crash and respawn it).
    const TEST_DRIVER_MJS: &str = r#"
const env = globalThis.Deno ? Deno.env.toObject() : process.env;
const url = env.NANOBPMN_HOOK_URL;
const token = env.NANOBPMN_WEBHOOK_TOKEN;
const cfg = JSON.parse(env.NANOBPMN_TRIGGER_CONFIG || "{}");
for (let i = 0; i < 2; i++) {
  const headers = { "content-type": "application/json", "idempotency-key": `evt-${i}` };
  if (token) headers["x-webhook-token"] = token;
  await fetch(url, { method: "POST", headers, body: JSON.stringify({ n: i, topic: cfg.topic }) });
}
await new Promise((r) => setTimeout(r, 60000));
"#;

    /// End-to-end: an installed pack declaring a source `kind` with a `driver`
    /// is auto-launched + supervised, and its events reach the durable inbox
    /// over the real ingress. Hermetic — a throwaway pack in a temp extensions
    /// dir + the real `project_hook` handler on an ephemeral port.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pack_driver_autolaunches_and_emits_over_ingress() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }

        // Install a throwaway trigger pack under a temp extensions dir.
        static N: AtomicU64 = AtomicU64::new(0);
        let ext_root = std::env::temp_dir().join(format!(
            "nano-trig-ext-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let pack = ext_root.join("nano-ide-trigger-testpack");
        let _ = std::fs::remove_dir_all(&ext_root);
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{
                "id": "nano-ide-trigger-testpack",
                "kind": "trigger",
                "displayName": "Test trigger pack",
                "triggerSources": [
                    { "kind": "testmqtt", "displayName": "Test MQTT", "driver": "driver.mjs" }
                ]
            }"#,
        )
        .unwrap();
        std::fs::write(pack.join("driver.mjs"), TEST_DRIVER_MJS).unwrap();
        unsafe {
            std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext_root);
        }
        // Approve the pack so its driver may launch (#460 trust gate). The
        // untrusted twin below asserts the withheld path.
        super::super::extensions::save_trust(&super::super::extensions::TrustStore {
            yolo: false,
            approved: ["nano-ide-trigger-testpack".to_string()]
                .into_iter()
                .collect(),
        })
        .unwrap();

        let name = setup_app(
            r#", "triggers": [
                { "id": "sensor", "type": "testmqtt", "config": { "topic": "test/topic" }, "action": { "start": "p" } }
            ]"#,
        );

        // Stand up the real ingress on an ephemeral port; point drivers at it.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, crate::test_ingress_router())
                .await
                .ok();
        });
        workers::set_gateway_port(port);

        // Launch + supervise the pack driver via the real source layer.
        let manifest = read_manifest(&name).unwrap();
        let handle = LoopHandle::new_running();
        crate::trigger_sources::spawn_sources(&name, &manifest, handle.clone());

        // Poll until both events land in the inbox (or time out ~10s).
        let mut pending = 0;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Ok(s) = inbox_status(&name).await {
                pending = s.pending;
                if pending >= 2 {
                    break;
                }
            }
        }

        handle.stop_for_test();
        server.abort();
        unsafe {
            std::env::remove_var("NANOBPMN_EXTENSIONS_DIR");
        }
        let _ = std::fs::remove_dir_all(&ext_root);

        assert_eq!(
            pending, 2,
            "the auto-launched pack driver emitted two events over the ingress"
        );
    }

    /// Security gate (#460): a pack that is **not** trusted must have its
    /// out-of-process trigger driver *withheld* — nothing may reach the ingress.
    /// The symmetric happy path above approves the pack first; this is its
    /// negative twin, and the regression guard that an untrusted pack cannot get
    /// code execution the moment its app runs.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pack_driver_withheld_when_untrusted() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }

        static N: AtomicU64 = AtomicU64::new(0);
        let ext_root = std::env::temp_dir().join(format!(
            "nano-trig-untrusted-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let pack = ext_root.join("nano-ide-trigger-untrusted");
        let _ = std::fs::remove_dir_all(&ext_root);
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{
                "id": "nano-ide-trigger-untrusted",
                "kind": "trigger",
                "displayName": "Untrusted trigger pack",
                "triggerSources": [
                    { "kind": "testmqtt", "displayName": "Test MQTT", "driver": "driver.mjs" }
                ]
            }"#,
        )
        .unwrap();
        std::fs::write(pack.join("driver.mjs"), TEST_DRIVER_MJS).unwrap();
        unsafe {
            std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext_root);
        }
        // Deliberately do NOT approve the pack: no trust.json is written, so
        // `is_trusted("nano-ide-trigger-untrusted")` is false.

        let name = setup_app(
            r#", "triggers": [
                { "id": "sensor", "type": "testmqtt", "config": { "topic": "test/topic" }, "action": { "start": "p" } }
            ]"#,
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, crate::test_ingress_router())
                .await
                .ok();
        });
        workers::set_gateway_port(port);

        let manifest = read_manifest(&name).unwrap();
        let handle = LoopHandle::new_running();
        crate::trigger_sources::spawn_sources(&name, &manifest, handle.clone());

        // Give a would-be driver the full grace period to launch + emit (a real
        // launch posts within ~1s), then check the inbox once. A single expensive
        // inbox query instead of a tight poll keeps the test bounded (~8s) rather
        // than paying ~500ms per iteration across a long window.
        tokio::time::sleep(WITHHELD_GRACE).await;
        let pending = inbox_status(&name).await.map(|s| s.pending).unwrap_or(0);

        handle.stop_for_test();
        server.abort();
        unsafe {
            std::env::remove_var("NANOBPMN_EXTENSIONS_DIR");
        }
        let _ = std::fs::remove_dir_all(&ext_root);

        assert_eq!(
            pending, 0,
            "an untrusted pack's trigger driver must not launch or emit"
        );
    }

    /// A minimal connector worker: on startup it repeatedly reaches out to the
    /// gateway (`NANOBPMN_BASE_URL`). We don't need a real job stream — a single
    /// TCP connection to the test server is proof the child launched.
    const TEST_WORKER_MJS: &str = r#"
const env = globalThis.Deno ? Deno.env.toObject() : process.env;
const base = env.NANOBPMN_BASE_URL;
for (let i = 0; i < 100; i++) {
  try { await fetch(`${base}/hit`); } catch (_) {}
  await new Promise((r) => setTimeout(r, 100));
}
"#;

    /// Stand up a throwaway connector-worker pack (declaring `workers[]` with an
    /// `entry`) plus an App that enables its job `type`, and a TCP server the
    /// launched worker will connect to. Returns `(app name, ext_root, hit flag,
    /// server task)`. `approve` writes trust for the pack when true.
    async fn setup_worker_pack(
        approve: bool,
    ) -> (
        String,
        std::path::PathBuf,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        tokio::task::JoinHandle<()>,
    ) {
        static N: AtomicU64 = AtomicU64::new(0);
        let ext_root = std::env::temp_dir().join(format!(
            "nano-conn-ext-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let pack = ext_root.join("nano-ide-connector-testpack");
        let _ = std::fs::remove_dir_all(&ext_root);
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{
                "id": "nano-ide-connector-testpack",
                "kind": "app",
                "displayName": "Test connector pack",
                "workers": [
                    { "type": "test:job", "entry": "worker.mjs", "displayName": "Test worker" }
                ]
            }"#,
        )
        .unwrap();
        std::fs::write(pack.join("worker.mjs"), TEST_WORKER_MJS).unwrap();
        unsafe {
            std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext_root);
        }
        if approve {
            super::super::extensions::save_trust(&super::super::extensions::TrustStore {
                yolo: false,
                approved: ["nano-ide-connector-testpack".to_string()]
                    .into_iter()
                    .collect(),
            })
            .unwrap();
        }

        let name = setup_app(r#", "workers": [ { "type": "test:job" } ]"#);

        // A TCP server on the gateway port: any accepted connection = the worker
        // launched and reached out. Reply with a minimal 200 so `fetch` resolves.
        let hit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        workers::set_gateway_port(port);
        let hit_srv = hit.clone();
        let server = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                hit_srv.store(true, Ordering::Relaxed);
                use tokio::io::AsyncWriteExt;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
            }
        });
        (name, ext_root, hit, server)
    }

    /// Class-scoped twin of the trigger-driver happy path, for the connector
    /// **worker** edge (#460): an approved pack's worker child is launched.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pack_worker_launches_when_trusted() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let (name, ext_root, hit, server) = setup_worker_pack(true).await;

        let manifest = read_manifest(&name).unwrap();
        let handle = LoopHandle::new_running();
        crate::trigger_sources::spawn_workers(&name, &manifest, handle.clone());

        let mut launched = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if hit.load(Ordering::Relaxed) {
                launched = true;
                break;
            }
        }

        handle.stop_for_test();
        server.abort();
        unsafe {
            std::env::remove_var("NANOBPMN_EXTENSIONS_DIR");
        }
        let _ = std::fs::remove_dir_all(&ext_root);

        assert!(launched, "an approved pack's connector worker must launch");
    }

    /// Class-scoped twin of the trigger-driver withheld path, for the connector
    /// **worker** edge (#460): an untrusted pack's worker child must not launch.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pack_worker_withheld_when_untrusted() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let (name, ext_root, hit, server) = setup_worker_pack(false).await;

        let manifest = read_manifest(&name).unwrap();
        let handle = LoopHandle::new_running();
        crate::trigger_sources::spawn_workers(&name, &manifest, handle.clone());

        // Give a would-be worker the full grace period to launch + connect (the
        // happy-path twin observes a launch well inside this budget), then check
        // the cheap hit flag once.
        tokio::time::sleep(WITHHELD_GRACE).await;
        let launched = hit.load(Ordering::Relaxed);

        handle.stop_for_test();
        server.abort();
        unsafe {
            std::env::remove_var("NANOBPMN_EXTENSIONS_DIR");
        }
        let _ = std::fs::remove_dir_all(&ext_root);

        assert!(
            !launched,
            "an untrusted pack's connector worker must not launch"
        );
    }

    #[test]
    fn check_webhook_secret_accepts_matching_token() {
        let _g = lock();
        let var = "NANO_TEST_WH_SECRET_OK";
        unsafe {
            std::env::set_var(var, "s3cr3t");
        }
        assert!(check_webhook_secret("env:NANO_TEST_WH_SECRET_OK", Some("s3cr3t")).is_ok());
        // bare VARNAME form resolves the same env var.
        assert!(check_webhook_secret("NANO_TEST_WH_SECRET_OK", Some("s3cr3t")).is_ok());
        unsafe {
            std::env::remove_var(var);
        }
    }

    #[test]
    fn check_webhook_secret_rejects_wrong_and_missing_token() {
        let _g = lock();
        let var = "NANO_TEST_WH_SECRET_BAD";
        unsafe {
            std::env::set_var(var, "expected");
        }
        assert!(matches!(
            check_webhook_secret("env:NANO_TEST_WH_SECRET_BAD", Some("nope")),
            Err(TriggerError::Unauthorized(_))
        ));
        assert!(matches!(
            check_webhook_secret("env:NANO_TEST_WH_SECRET_BAD", None),
            Err(TriggerError::Unauthorized(_))
        ));
        unsafe {
            std::env::remove_var(var);
        }
    }

    #[test]
    fn check_webhook_secret_fails_closed_when_env_unset() {
        let _g = lock();
        unsafe {
            std::env::remove_var("NANO_TEST_WH_SECRET_UNSET");
        }
        // Declared protected but no secret supplied → refuse (not Unauthorized).
        assert!(matches!(
            check_webhook_secret("env:NANO_TEST_WH_SECRET_UNSET", Some("anything")),
            Err(TriggerError::Apply(_))
        ));
    }

    #[test]
    fn constant_time_eq_matches_bytewise() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    // --- triggers_overview (manifest read; no runtime) --------------------

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn triggers_overview_tags_builtin_and_unknown_kinds() {
        let _g = lock();
        let name = setup_app(
            r#", "triggers": [
                { "id": "nightly", "type": "cron", "spec": "0 6 * * *", "action": { "start": "p" } },
                { "id": "weird", "type": "quantum", "action": { "start": "p" } }
            ]"#,
        );
        let ov = triggers_overview(&name).await.expect("overview");
        let triggers = ov.get("triggers").and_then(Json::as_array).unwrap();
        assert_eq!(triggers.len(), 2);
        let cron = &triggers[0];
        assert_eq!(cron.get("type").and_then(Json::as_str), Some("cron"));
        assert_eq!(cron.get("builtin").and_then(Json::as_bool), Some(true));
        assert_eq!(cron.get("recognized").and_then(Json::as_bool), Some(true));
        let weird = &triggers[1];
        assert_eq!(weird.get("builtin").and_then(Json::as_bool), Some(false));
        assert_eq!(weird.get("recognized").and_then(Json::as_bool), Some(false));
        // The source registry advertises the builtin kinds.
        let sources = ov.get("sources").and_then(Json::as_array).unwrap();
        assert!(
            sources
                .iter()
                .any(|s| s.get("kind").and_then(Json::as_str) == Some("cron"))
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn triggers_overview_surfaces_bad_cron_error() {
        let _g = lock();
        let name = setup_app(
            r#", "triggers": [
                { "id": "broken", "type": "cron", "spec": "not a cron", "action": { "start": "p" } }
            ]"#,
        );
        let ov = triggers_overview(&name).await.expect("overview");
        let errors = ov.get("errors").and_then(Json::as_array).unwrap();
        assert!(!errors.is_empty(), "a malformed cron spec is reported");
    }

    // --- webhook_ingest (needs a JS runtime for the sqlite inbox) ----------

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn webhook_ingest_enqueues_and_dedups() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let name = setup_app(
            r#", "triggers": [
                { "id": "hook", "type": "webhook", "action": { "start": "p" } }
            ]"#,
        );
        let body = json!({ "n": 1 });
        let first = webhook_ingest(&name, "hook", None, Some("idem-1"), &body)
            .await
            .expect("ingest");
        assert!(first.enqueued, "new event is persisted");
        let dup = webhook_ingest(&name, "hook", None, Some("idem-1"), &body)
            .await
            .expect("ingest");
        assert!(!dup.enqueued, "same idempotency key is collapsed");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn webhook_ingest_rejects_non_webhook_trigger() {
        let _g = lock();
        let name = setup_app(
            r#", "triggers": [
                { "id": "nightly", "type": "cron", "spec": "0 6 * * *", "action": { "start": "p" } }
            ]"#,
        );
        let err = webhook_ingest(&name, "nightly", None, None, &json!({})).await;
        assert!(matches!(err, Err(TriggerError::Manifest(_))));
        // Unknown trigger id is also a manifest error.
        let missing = webhook_ingest(&name, "ghost", None, None, &json!({})).await;
        assert!(matches!(missing, Err(TriggerError::Manifest(_))));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn webhook_ingest_enforces_auth() {
        let _g = lock();
        if !runtime_available() {
            eprintln!("skipping: no JS runtime");
            return;
        }
        let var = "NANO_TEST_WH_INGEST_SECRET";
        unsafe {
            std::env::set_var(var, "letmein");
        }
        let name = setup_app(
            r#", "triggers": [
                { "id": "hook", "type": "webhook", "auth": "env:NANO_TEST_WH_INGEST_SECRET", "action": { "start": "p" } }
            ]"#,
        );
        // Wrong token → Unauthorized, nothing persisted.
        let bad = webhook_ingest(&name, "hook", Some("wrong"), None, &json!({})).await;
        assert!(matches!(bad, Err(TriggerError::Unauthorized(_))));
        // Correct token → enqueued.
        let ok = webhook_ingest(&name, "hook", Some("letmein"), Some("k"), &json!({}))
            .await
            .expect("ingest");
        assert!(ok.enqueued);
        unsafe {
            std::env::remove_var(var);
        }
    }

    // --- add_trigger (console "Add trigger" form) -------------------------

    fn cfg(pairs: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn add_trigger_places_cron_spec_top_level_and_appends() {
        let _g = lock();
        let name = setup_app("");
        add_trigger(
            &name,
            "nightly",
            "cron",
            &cfg(&[("spec", "0 0 * * *"), ("onMissed", "skip")]),
            None,
            &json!({ "start": "cleanup" }),
        )
        .expect("add");
        let manifest = read_manifest(&name).unwrap();
        let t = &manifest["triggers"].as_array().unwrap()[0];
        assert_eq!(t["id"], json!("nightly"));
        assert_eq!(t["type"], json!("cron"));
        assert_eq!(t["spec"], json!("0 0 * * *"));
        assert_eq!(t["onMissed"], json!("skip"));
        assert_eq!(t["action"], json!({ "start": "cleanup" }));
        assert!(t.get("config").is_none());
    }

    #[test]
    fn add_trigger_places_file_path_top_level_and_pollms_nested() {
        let _g = lock();
        let name = setup_app("");
        add_trigger(
            &name,
            "watch",
            "file",
            &cfg(&[("path", "/tmp/in"), ("pollMs", "500")]),
            None,
            &json!({ "message": "file-seen" }),
        )
        .expect("add");
        let manifest = read_manifest(&name).unwrap();
        let t = &manifest["triggers"].as_array().unwrap()[0];
        assert_eq!(t["path"], json!("/tmp/in"));
        assert_eq!(t["config"]["pollMs"], json!(500));
    }

    #[test]
    fn add_trigger_rejects_duplicate_id() {
        let _g = lock();
        let name = setup_app(
            r#", "triggers": [ { "id": "dup", "type": "manual", "action": { "start": "p" } } ]"#,
        );
        let err = add_trigger(
            &name,
            "dup",
            "manual",
            &cfg(&[]),
            None,
            &json!({ "start": "p" }),
        )
        .unwrap_err();
        assert!(matches!(err, TriggerError::Manifest(_)));
    }

    #[test]
    fn add_trigger_rejects_unknown_kind_and_empty_action() {
        let _g = lock();
        let name = setup_app("");
        assert!(matches!(
            add_trigger(
                &name,
                "x",
                "no-such-kind",
                &cfg(&[]),
                None,
                &json!({ "start": "p" })
            ),
            Err(TriggerError::Manifest(_))
        ));
        assert!(matches!(
            add_trigger(&name, "x", "manual", &cfg(&[]), None, &json!({})),
            Err(TriggerError::Manifest(_))
        ));
    }
}
