//! Trigger **sources** — the extensible source layer (ADR 0025 phase 2 + §6).
//!
//! Phase 1 ([`super::triggers`]) built the durable inbox + dispatcher: events
//! that reach the inbox are applied to the engine at-least-once. This module
//! is what *puts* events into that inbox — the **sources**. A source is the
//! left half of the Zapier primitive ("when X …"); the inbox/dispatcher is the
//! right half ("… do Y").
//!
//! ## The extensibility contract (the marketplace seam)
//! Every source — core or third-party — ultimately calls one primitive:
//! [`super::triggers::enqueue`] (the §2 step-1 durable persist). What differs
//! is *where the source loop runs*:
//!
//! - **Core sources** (`cron`, `file`) are in-process Rust loops supervised by
//!   the dispatcher: they compute/observe events and call `enqueue` directly.
//! - **`webhook`** is passive: it owns no loop; the gateway ingress route
//!   ([`super::mod`]'s `/console/api/projects/{name}/hooks/{triggerId}`) persists
//!   on request and acks after persist. This same ingress is the **universal
//!   emit endpoint** any external producer can POST to.
//! - **Pack sources** (`nano-ide-trigger-*`, ADR 0007 / §6) are declared by an
//!   installed extension via `nano-ide.ext.json` `triggerSources[]`. Their
//!   driver runs out-of-process (Node) and emits over the same
//!   ingress. The runtime *recognises* the kind (so validation/status treat it
//!   as first-class) and owns the inbox/dispatch/retry — the pack only produces
//!   events. A community source therefore adds a `type` without touching core,
//!   and cannot run arbitrary host code beyond its packaged, reviewed driver
//!   (sources are declared data, no `eval`, per ADR 0007).
//!
//! [`known_kinds`] unions the compiled-in core kinds with every installed pack's
//! declared kinds — the single registry the console and validation consult.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value as Json, json};

use super::triggers::{self, LoopHandle};

/// Core source kinds compiled into the binary (§1/§6).
pub(crate) const BUILTIN_KINDS: &[&str] = &["cron", "webhook", "file", "manual"];

/// How often a `file` source re-stats its path when no explicit interval is set.
const FILE_POLL_MS_DEFAULT: u64 = 2_000;
/// Floor on the file poll interval so a misconfigured trigger can't busy-spin.
const FILE_POLL_MS_MIN: u64 = 250;

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Source classification — parse a manifest trigger into a driven source
// ---------------------------------------------------------------------------

/// Cron catch-up policy for fires missed while the App was down (§Open
/// questions). Default [`OnMissed::Skip`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum OnMissed {
    /// Ignore every missed instant; fire only on the next future match.
    #[default]
    Skip,
    /// Fire exactly once to represent the whole missed span, then resume.
    Once,
    /// Enqueue one event per missed instant (dedup keys keep it idempotent).
    All,
}

impl OnMissed {
    fn parse(s: Option<&str>) -> Self {
        match s {
            Some("once") => OnMissed::Once,
            Some("all") => OnMissed::All,
            _ => OnMissed::Skip,
        }
    }
}

/// A source resolved from a manifest trigger, classified by how it is driven.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceConfig {
    /// In-process scheduler firing on a crontab `spec`.
    Cron { spec: CronSpec, on_missed: OnMissed },
    /// In-process watcher polling `path`'s mtime.
    File { path: String, poll_ms: u64 },
    /// Passive: driven by the gateway ingress route, not a loop.
    Webhook,
    /// External/pack source: recognised, driven by the ingress (its driver
    /// runs out-of-process and emits over the universal emit endpoint).
    External,
}

/// A configured source for one trigger id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TriggerSource {
    pub id: String,
    pub kind: String,
    pub config: SourceConfig,
}

/// Parse every trigger in `manifest` into a [`TriggerSource`]. A trigger whose
/// `type` is unknown-but-not-core is classified [`SourceConfig::External`] so a
/// pack source is accepted; a malformed core source (e.g. a bad cron `spec`) is
/// skipped with its error returned in the second tuple element.
pub(crate) fn parse_sources(manifest: &Json) -> (Vec<TriggerSource>, Vec<String>) {
    let mut out = Vec::new();
    let mut errors = Vec::new();
    let Some(triggers) = manifest.get("triggers").and_then(Json::as_array) else {
        return (out, errors);
    };
    for t in triggers {
        let Some(id) = t.get("id").and_then(Json::as_str) else {
            continue;
        };
        let kind = t
            .get("type")
            .and_then(Json::as_str)
            .unwrap_or("")
            .to_string();
        let config = match kind.as_str() {
            "cron" => {
                let spec = t.get("spec").and_then(Json::as_str).unwrap_or("");
                match CronSpec::parse(spec) {
                    Ok(spec) => SourceConfig::Cron {
                        spec,
                        on_missed: OnMissed::parse(t.get("onMissed").and_then(Json::as_str)),
                    },
                    Err(e) => {
                        errors.push(format!("trigger '{id}': cron spec '{spec}': {e}"));
                        continue;
                    }
                }
            }
            "file" => {
                let Some(path) = t
                    .get("path")
                    .and_then(Json::as_str)
                    .filter(|p| !p.is_empty())
                else {
                    errors.push(format!("trigger '{id}': file source needs a 'path'"));
                    continue;
                };
                let poll_ms = t
                    .get("config")
                    .and_then(|c| c.get("pollMs"))
                    .and_then(Json::as_u64)
                    .unwrap_or(FILE_POLL_MS_DEFAULT)
                    .max(FILE_POLL_MS_MIN);
                SourceConfig::File {
                    path: path.to_string(),
                    poll_ms,
                }
            }
            "webhook" => SourceConfig::Webhook,
            "manual" | "" => SourceConfig::External,
            _ => SourceConfig::External,
        };
        out.push(TriggerSource {
            id: id.to_string(),
            kind,
            config,
        });
    }
    (out, errors)
}

/// The registry of recognised source kinds: the compiled-in core kinds unioned
/// with every installed pack's `triggerSources[].kind` (§6). This is the single
/// list the console and manifest validation consult — the marketplace seam.
pub(crate) fn known_kinds() -> BTreeSet<String> {
    let mut kinds: BTreeSet<String> = BUILTIN_KINDS.iter().map(|s| s.to_string()).collect();
    for spec in super::extensions::all_trigger_sources() {
        kinds.insert(spec.kind);
    }
    kinds
}

/// Whether `kind` is a core source compiled into the binary (vs a pack source).
pub(crate) fn is_builtin(kind: &str) -> bool {
    BUILTIN_KINDS.contains(&kind)
}

/// A human label for a source `kind`: the built-in name, or a pack's declared
/// `displayName`, else `None`.
pub(crate) fn display_name(kind: &str) -> Option<String> {
    let builtin = match kind {
        "cron" => Some("Schedule (cron)"),
        "webhook" => Some("Webhook (HTTP)"),
        "file" => Some("File watch"),
        "manual" => Some("Manual / synthetic"),
        _ => None,
    };
    if let Some(b) = builtin {
        return Some(b.to_string());
    }
    super::extensions::all_trigger_sources()
        .into_iter()
        .find(|s| s.kind == kind)
        .and_then(|s| s.display_name)
}

// ---------------------------------------------------------------------------
// Cron — a small, dependency-free 5-field crontab parser
// ---------------------------------------------------------------------------

/// A parsed 5-field crontab spec (`min hour day-of-month month day-of-week`).
/// Supports `*`, `*/n`, `a`, `a-b`, `a-b/n`, and comma lists of those. Evaluated
/// in **UTC**. Day-of-week is `0..=7` (0 and 7 are Sunday). When both
/// day-of-month and day-of-week are restricted (neither `*`), a minute matches
/// if **either** matches — the standard Vixie-cron OR rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CronSpec {
    minute: Vec<bool>, // 60
    hour: Vec<bool>,   // 24
    dom: Vec<bool>,    // 32 (index 1..=31)
    month: Vec<bool>,  // 13 (index 1..=12)
    dow: Vec<bool>,    // 7  (0..=6, Sunday=0)
    dom_star: bool,
    dow_star: bool,
}

impl CronSpec {
    pub(crate) fn parse(spec: &str) -> Result<Self, String> {
        let fields: Vec<&str> = spec.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!("expected 5 fields, got {}", fields.len()));
        }
        let minute = parse_field(fields[0], 0, 59)?;
        let hour = parse_field(fields[1], 0, 23)?;
        let dom_raw = parse_field(fields[2], 1, 31)?;
        let month = parse_field(fields[3], 1, 12)?;
        let dow_raw = parse_field(fields[4], 0, 7)?;
        // Fold day-of-week 7 → 0 (both Sunday) into a 0..=6 table.
        let mut dow = vec![false; 7];
        for (i, on) in dow_raw.iter().enumerate() {
            if *on {
                dow[i % 7] = true;
            }
        }
        // Index the day-of-month table at 1..=31 (slot 0 unused).
        let mut dom = vec![false; 32];
        for (i, on) in dom_raw.iter().enumerate() {
            if *on {
                dom[i + 1] = true;
            }
        }
        // Index month at 1..=12.
        let mut month_t = vec![false; 13];
        for (i, on) in month.iter().enumerate() {
            if *on {
                month_t[i + 1] = true;
            }
        }
        Ok(CronSpec {
            minute,
            hour,
            dom,
            month: month_t,
            dow,
            dom_star: fields[2] == "*",
            dow_star: fields[4] == "*",
        })
    }

    /// Does the calendar minute at UTC epoch-second `ts` match this spec?
    fn matches(&self, ts: i64) -> bool {
        use chrono::{Datelike, TimeZone, Timelike, Utc};
        let dt = match Utc.timestamp_opt(ts, 0).single() {
            Some(dt) => dt,
            None => return false,
        };
        let min = dt.minute() as usize;
        let hour = dt.hour() as usize;
        let dom = dt.day() as usize;
        let month = dt.month() as usize;
        let dow = dt.weekday().num_days_from_sunday() as usize;
        if !self.minute.get(min).copied().unwrap_or(false) {
            return false;
        }
        if !self.hour.get(hour).copied().unwrap_or(false) {
            return false;
        }
        if !self.month.get(month).copied().unwrap_or(false) {
            return false;
        }
        let dom_ok = self.dom.get(dom).copied().unwrap_or(false);
        let dow_ok = self.dow.get(dow).copied().unwrap_or(false);
        // Vixie OR-rule: when both restricted, either satisfies.
        match (self.dom_star, self.dow_star) {
            (true, true) => true,
            (false, true) => dom_ok,
            (true, false) => dow_ok,
            (false, false) => dom_ok || dow_ok,
        }
    }

    /// The next matching UTC epoch-second **strictly after** `after`, searching
    /// minute-by-minute up to a bounded horizon (guards against an impossible
    /// spec like Feb 30). Returns `None` past the horizon.
    pub(crate) fn next_after(&self, after: i64) -> Option<i64> {
        // Advance to the start of the next whole minute after `after`.
        let mut ts = (after - after.rem_euclid(60)) + 60;
        // ~500 days of minutes is the horizon (Feb-29-only specs still resolve).
        for _ in 0..(500 * 24 * 60) {
            if self.matches(ts) {
                return Some(ts);
            }
            ts += 60;
        }
        None
    }
}

/// Parse one crontab field into a `min..=max` occupancy table (length
/// `max - min + 1`, index 0 == `min`).
fn parse_field(field: &str, min: u32, max: u32) -> Result<Vec<bool>, String> {
    let width = (max - min + 1) as usize;
    let mut table = vec![false; width];
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (
                r,
                s.parse::<u32>()
                    .map_err(|_| format!("bad step '{s}'"))
                    .and_then(|n| if n == 0 { Err("step 0".into()) } else { Ok(n) })?,
            ),
            None => (part, 1),
        };
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            let a = a
                .parse::<u32>()
                .map_err(|_| format!("bad range start '{a}'"))?;
            let b = b
                .parse::<u32>()
                .map_err(|_| format!("bad range end '{b}'"))?;
            (a, b)
        } else {
            let v = range
                .parse::<u32>()
                .map_err(|_| format!("bad value '{range}'"))?;
            (v, v)
        };
        if lo < min || hi > max || lo > hi {
            return Err(format!("value out of range {min}..={max} in '{part}'"));
        }
        let mut v = lo;
        while v <= hi {
            table[(v - min) as usize] = true;
            v += step;
        }
    }
    Ok(table)
}

// ---------------------------------------------------------------------------
// In-process source drivers (cron, file) — supervised loops that emit
// ---------------------------------------------------------------------------

/// Spawn the in-process driver loops for a project's built-in loop sources
/// (`cron`, `file`). Passive (`webhook`) and external/pack sources spawn no
/// loop — they emit via the ingress. All loops share `handle`'s stop signal so
/// [`super::triggers::TriggerDispatcher::stop`] tears them down with the drain
/// loop. Malformed source configs are logged and skipped, never fatal.
pub(crate) fn spawn_sources(project: &str, manifest: &Json, handle: Arc<LoopHandle>) {
    let (sources, errors) = parse_sources(manifest);
    for e in errors {
        tracing::warn!(project, "trigger source ignored: {e}");
    }
    for src in sources {
        match src.config {
            SourceConfig::Cron { spec, on_missed } => {
                let (project, id, handle) = (project.to_string(), src.id, handle.clone());
                tokio::spawn(run_cron(project, id, spec, on_missed, handle));
            }
            SourceConfig::File { path, poll_ms } => {
                let (project, id, handle) = (project.to_string(), src.id, handle.clone());
                tokio::spawn(run_file(project, id, path, poll_ms, handle));
            }
            SourceConfig::Webhook => {}
            SourceConfig::External => {
                // A pack source (ADR 0025 §6): if the installed pack ships a
                // `driver`, auto-launch + supervise it; otherwise it is a
                // declaration-only source driven out-of-band (still emits over
                // the ingress), so there is nothing to spawn.
                let Some(driver) = super::extensions::trigger_driver(&src.kind) else {
                    continue;
                };
                // Security gate (#460): a pack driver is out-of-process code —
                // withhold it unless the contributing pack is trusted, mirroring
                // the lang/app toolchain gate. Logged + skipped, never fatal.
                if !super::extensions::is_trusted(&driver.id) {
                    tracing::warn!(
                        project,
                        kind = %src.kind,
                        ext = %driver.id,
                        "pack trigger driver withheld: extension '{}' is not approved to run code; approve it (or enable yolo) in Extensions",
                        driver.id
                    );
                    continue;
                }
                let Some(trigger) = raw_trigger(manifest, &src.id) else {
                    continue;
                };
                let connection = resolve_connection(manifest, &trigger);
                let (project, id, kind, handle) =
                    (project.to_string(), src.id, src.kind, handle.clone());
                tokio::spawn(run_pack_driver(
                    project, id, kind, trigger, connection, driver, handle,
                ));
            }
        }
    }
}

/// Find a trigger's raw manifest object by id (the parsed [`TriggerSource`]
/// intentionally discards `config`/`connection`/`auth`; a pack driver needs
/// them, so re-read the source of truth).
fn raw_trigger(manifest: &Json, id: &str) -> Option<Json> {
    manifest
        .get("triggers")
        .and_then(Json::as_array)?
        .iter()
        .find(|t| t.get("id").and_then(Json::as_str) == Some(id))
        .cloned()
}

/// Resolve a trigger's referenced `connections[]` entry (the credentials/
/// endpoint object) into JSON for the driver, or [`Json::Null`] if the trigger
/// names none / it is missing. Secrets inside should be env templates the
/// driver expands itself (ADR 0025 §1) — we forward the object verbatim.
fn resolve_connection(manifest: &Json, trigger: &Json) -> Json {
    let Some(name) = trigger.get("connection").and_then(Json::as_str) else {
        return Json::Null;
    };
    manifest
        .get("connections")
        .and_then(|c| c.get(name))
        .cloned()
        .unwrap_or(Json::Null)
}

/// Backoff floor/ceiling for respawning a crashed pack driver.
const DRIVER_BACKOFF_MIN: Duration = Duration::from_millis(500);
const DRIVER_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A driver/worker that stays up at least this long counts as a healthy run, so
/// its next restart starts fresh from [`DRIVER_BACKOFF_MIN`]. Shorter-lived
/// exits are treated as a crash-loop and keep ramping the backoff toward the
/// cap, instead of resetting to the floor on every exit.
const DRIVER_HEALTHY_UPTIME: Duration = DRIVER_BACKOFF_MAX;

/// Supervise one pack source's out-of-process driver (ADR 0025 phase 4): launch
/// it (Node-only, ADR 0065), pipe its logs, and on crash restart with capped
/// exponential backoff — until [`LoopHandle::stopped`] fires, at which point the
/// child is killed. The driver emits its events over the trigger ingress using
/// the env contract below; this function owns only its lifecycle.
async fn run_pack_driver(
    project: String,
    id: String,
    kind: String,
    trigger: Json,
    connection: Json,
    driver: super::extensions::TriggerDriver,
    handle: Arc<LoopHandle>,
) {
    let port = super::workers::gateway_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let hook_url = format!("{base_url}/console/api/projects/{project}/hooks/{id}");
    let config_json = trigger
        .get("config")
        .cloned()
        .unwrap_or_else(|| json!({}))
        .to_string();
    let connection_json = connection.to_string();
    // `auth` names an env var (optionally `env:VAR`) holding the shared secret
    // the ingress expects; forward its value so the driver can present it.
    let token = trigger
        .get("auth")
        .and_then(Json::as_str)
        .filter(|s| !s.is_empty())
        .map(|a| a.strip_prefix("env:").unwrap_or(a))
        .and_then(|v| std::env::var(v).ok());

    let mut backoff = DRIVER_BACKOFF_MIN;
    while handle.is_running() {
        match spawn_driver_child(
            &driver,
            &base_url,
            &hook_url,
            &project,
            &id,
            &kind,
            &config_json,
            &connection_json,
            token.as_deref(),
        ) {
            Ok(mut child) => {
                let started = std::time::Instant::now();
                let pid = child.id().unwrap_or(0);
                tracing::info!(project, trigger = id, kind, pid, "trigger driver started");
                let stopped = tokio::select! {
                    _ = handle.stopped() => {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        true
                    }
                    status = child.wait() => {
                        tracing::warn!(
                            project, trigger = id, kind,
                            "trigger driver exited (status {status:?}); restarting"
                        );
                        false
                    }
                };
                if stopped {
                    break;
                }
                // A driver that stayed up a while gets a fresh backoff window;
                // a fast crash-loop keeps ramping the backoff toward the cap.
                if started.elapsed() >= DRIVER_HEALTHY_UPTIME {
                    backoff = DRIVER_BACKOFF_MIN;
                }
            }
            Err(e) => {
                tracing::error!(
                    project,
                    trigger = id,
                    kind,
                    "failed to launch trigger driver: {e}"
                );
            }
        }
        if !sleep_or_stop(&handle, backoff).await {
            break;
        }
        backoff = (backoff * 2).min(DRIVER_BACKOFF_MAX);
    }
}

/// Build + spawn the driver child on the Node runtime (ADR 0065)
/// and stripping TS types for a `.ts` entrypoint. Streams stdout/stderr to the
/// tracing log. Returns the spawned [`tokio::process::Child`].
#[allow(clippy::too_many_arguments)]
fn spawn_driver_child(
    driver: &super::extensions::TriggerDriver,
    base_url: &str,
    hook_url: &str,
    project: &str,
    id: &str,
    kind: &str,
    config_json: &str,
    connection_json: &str,
    token: Option<&str>,
) -> Result<tokio::process::Child, String> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    let entry = &driver.entry;
    let is_ts = entry.ends_with(".ts") || entry.ends_with(".mts") || entry.ends_with(".cts");

    let mut cmd;
    if let Some(node) = super::workers::usable_node() {
        cmd = Command::new(node);
        cmd.current_dir(&driver.dir);
        if is_ts {
            cmd.arg("--experimental-strip-types").arg("--no-warnings");
        }
        cmd.arg(entry);
    } else {
        return Err("no JS runtime (Node >=22.6) available".to_string());
    }

    cmd.env("NO_COLOR", "1")
        .env("NANOBPMN_BASE_URL", base_url)
        .env("NANOBPMN_HOOK_URL", hook_url)
        .env("NANOBPMN_PROJECT", project)
        .env("NANOBPMN_TRIGGER_ID", id)
        .env("NANOBPMN_TRIGGER_TYPE", kind)
        .env("NANOBPMN_TRIGGER_CONFIG", config_json)
        .env("NANOBPMN_TRIGGER_CONNECTION", connection_json)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(tok) = token {
        cmd.env("NANOBPMN_WEBHOOK_TOKEN", tok);
    }

    let mut child = cmd.spawn().map_err(|e| e.to_string())?;

    if let Some(out) = child.stdout.take() {
        let (project, id) = (project.to_string(), id.to_string());
        tokio::spawn(async move {
            let mut lines = BufReader::new(out).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(project, trigger = id, stream = "out", "{line}");
            }
        });
    }
    if let Some(err) = child.stderr.take() {
        let (project, id) = (project.to_string(), id.to_string());
        tokio::spawn(async move {
            let mut lines = BufReader::new(err).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(project, trigger = id, stream = "err", "{line}");
            }
        });
    }

    Ok(child)
}

// ── Connector workers (outbound edge, ADR 0050 §4) ──────────────────────────
//
// The outbound sibling of [`spawn_sources`]. Where a pack source's driver
// *ingests* events, a connector's worker *acts* on engine jobs (Zeebe-style,
// long-lived, keyed by job `type`). We reuse this file's process-supervision
// machinery (backoff, `LoopHandle` stop, Node-first launch) verbatim so both
// I/O edges share one lifecycle contract.

/// Extract the distinct job `type`s an App enables via its manifest `workers[]`
/// (accepting either `taskType` or `type`, first-wins dedup). The manifest is
/// raw JSON (there is no typed App-manifest struct); only the type is needed to
/// resolve the backing pack via [`super::extensions::worker_driver`].
fn parse_worker_types(manifest: &Json) -> Vec<String> {
    let Some(arr) = manifest.get("workers").and_then(Json::as_array) else {
        return vec![];
    };
    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for w in arr {
        let Some(t) = w
            .get("taskType")
            .and_then(Json::as_str)
            .or_else(|| w.get("type").and_then(Json::as_str))
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        if seen.insert(t.to_string()) {
            out.push(t.to_string());
        }
    }
    out
}

/// Launch + supervise the out-of-process workers an App enables (ADR 0050 §4),
/// the outbound-edge sibling of [`spawn_sources`]. For each `workers[]` entry
/// whose job `type` resolves to an installed pack shipping a worker `entry`,
/// auto-launch that worker under the shared `handle`; a `type` with no pack
/// worker (a project-local worker, or an `llm`) is left to its own path.
pub(crate) fn spawn_workers(project: &str, manifest: &Json, handle: Arc<LoopHandle>) {
    for task_type in parse_worker_types(manifest) {
        let Some(driver) = super::extensions::worker_driver(&task_type) else {
            continue;
        };
        // Security gate (#460): a connector worker is out-of-process code —
        // withhold it unless the contributing pack is trusted, mirroring the
        // lang/app toolchain gate. Logged + skipped, never fatal.
        if !super::extensions::is_trusted(&driver.id) {
            tracing::warn!(
                project,
                worker = %task_type,
                ext = %driver.id,
                "pack connector worker withheld: extension '{}' is not approved to run code; approve it (or enable yolo) in Extensions",
                driver.id
            );
            continue;
        }
        let (project, handle) = (project.to_string(), handle.clone());
        tokio::spawn(run_pack_worker(project, task_type, driver, handle));
    }
}

/// Supervise one connector worker's out-of-process child (ADR 0050 §4): launch
/// it (Node-only, ADR 0065), pipe its logs, and on crash restart with capped
/// exponential backoff — until [`LoopHandle::stopped`] fires, at which point the
/// child is killed. The worker subscribes to its job `type` over the base URL
/// using `@nanobpm/worker`; this function owns only its lifecycle. Mirrors
/// [`run_pack_driver`].
async fn run_pack_worker(
    project: String,
    task_type: String,
    driver: super::extensions::WorkerDriver,
    handle: Arc<LoopHandle>,
) {
    let port = super::workers::gateway_port();
    let base_url = format!("http://127.0.0.1:{port}");
    // Activation identity used by the worker for job streaming/logging.
    let worker_name = format!("{project}:{task_type}");

    let mut backoff = DRIVER_BACKOFF_MIN;
    while handle.is_running() {
        match spawn_worker_child(&driver, &base_url, &project, &task_type, &worker_name) {
            Ok(mut child) => {
                let started = std::time::Instant::now();
                let pid = child.id().unwrap_or(0);
                tracing::info!(project, worker = task_type, pid, "connector worker started");
                let stopped = tokio::select! {
                    _ = handle.stopped() => {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        true
                    }
                    status = child.wait() => {
                        tracing::warn!(
                            project, worker = task_type,
                            "connector worker exited (status {status:?}); restarting"
                        );
                        false
                    }
                };
                if stopped {
                    break;
                }
                // A worker that stayed up a while gets a fresh backoff window;
                // a fast crash-loop keeps ramping the backoff toward the cap.
                if started.elapsed() >= DRIVER_HEALTHY_UPTIME {
                    backoff = DRIVER_BACKOFF_MIN;
                }
            }
            Err(e) => {
                tracing::error!(
                    project,
                    worker = task_type,
                    "failed to launch connector worker: {e}"
                );
            }
        }
        if !sleep_or_stop(&handle, backoff).await {
            break;
        }
        backoff = (backoff * 2).min(DRIVER_BACKOFF_MAX);
    }
}

/// Build + spawn the worker child on the Node runtime (ADR 0065)
/// and stripping TS types for a `.ts` entrypoint, streaming its logs. The child
/// inherits the host env (so the connector's configured token env pointers flow
/// through) plus the base URL + worker name. Mirrors [`spawn_driver_child`].
fn spawn_worker_child(
    driver: &super::extensions::WorkerDriver,
    base_url: &str,
    project: &str,
    task_type: &str,
    worker_name: &str,
) -> Result<tokio::process::Child, String> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    let entry = &driver.entry;
    let is_ts = entry.ends_with(".ts") || entry.ends_with(".mts") || entry.ends_with(".cts");

    let mut cmd;
    if let Some(node) = super::workers::usable_node() {
        cmd = Command::new(node);
        cmd.current_dir(&driver.dir);
        if is_ts {
            cmd.arg("--experimental-strip-types").arg("--no-warnings");
        }
        cmd.arg(entry);
    } else {
        return Err("no JS runtime (Node >=22.6) available".to_string());
    }

    cmd.env("NO_COLOR", "1")
        .env("NANOBPMN_BASE_URL", base_url)
        .env("NANOBPMN_WORKER_NAME", worker_name)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|e| e.to_string())?;

    if let Some(out) = child.stdout.take() {
        let (project, task_type) = (project.to_string(), task_type.to_string());
        tokio::spawn(async move {
            let mut lines = BufReader::new(out).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(project, worker = task_type, stream = "out", "{line}");
            }
        });
    }
    if let Some(err) = child.stderr.take() {
        let (project, task_type) = (project.to_string(), task_type.to_string());
        tokio::spawn(async move {
            let mut lines = BufReader::new(err).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(project, worker = task_type, stream = "err", "{line}");
            }
        });
    }

    Ok(child)
}
/// keyed `<id>:<fireEpochSecond>` (deterministic, §3 — a given instant enqueues
/// once). On boot it applies `on_missed` to instants between the last-known and
/// now, then schedules forward.
async fn run_cron(
    project: String,
    id: String,
    spec: CronSpec,
    on_missed: OnMissed,
    handle: Arc<LoopHandle>,
) {
    // Catch-up: fires missed while the App was down (from the previous minute).
    let boot = now_secs();
    match on_missed {
        OnMissed::Skip => {}
        OnMissed::Once => {
            // Represent the whole missed span with one fire if any instant in
            // (boot-60, boot] matched (dedup collapses repeats across restarts).
            if let Some(t) = spec.next_after(boot - 61)
                && t <= boot
            {
                emit_cron(&project, &id, t).await;
            }
        }
        OnMissed::All => {
            let mut t = spec.next_after(boot - 61);
            while let Some(fire) = t {
                if fire > boot {
                    break;
                }
                emit_cron(&project, &id, fire).await;
                t = spec.next_after(fire);
            }
        }
    }
    let mut cursor = now_secs();
    while handle.is_running() {
        let Some(fire) = spec.next_after(cursor) else {
            // No further fire within the horizon — nothing to do.
            break;
        };
        let wait = (fire - now_secs()).max(0) as u64;
        if !sleep_or_stop(&handle, Duration::from_secs(wait)).await {
            break;
        }
        emit_cron(&project, &id, fire).await;
        cursor = fire;
    }
}

async fn emit_cron(project: &str, id: &str, fire: i64) {
    let key = format!("{id}:{fire}");
    let body = json!({ "firedAt": fire, "trigger": id, "source": "cron" });
    if let Err(e) = triggers::enqueue(project, id, Some(&key), &body).await {
        tracing::warn!(project, trigger = id, "cron enqueue failed: {e}");
    }
}

/// The file watcher loop: re-stat `path` on each poll; enqueue on a create/
/// modify/delete transition keyed `<path>:<mtime>:<kind>` (§3).
async fn run_file(
    project: String,
    id: String,
    path: String,
    poll_ms: u64,
    handle: Arc<LoopHandle>,
) {
    let mut last = stat_file(&path);
    while handle.is_running() {
        if !sleep_or_stop(&handle, Duration::from_millis(poll_ms)).await {
            break;
        }
        let cur = stat_file(&path);
        if let Some((kind, mtime)) = file_transition(&last, &cur) {
            let key = format!("{path}:{mtime}:{kind}");
            let body = json!({ "path": path, "kind": kind, "mtime": mtime, "trigger": id, "source": "file" });
            if let Err(e) = triggers::enqueue(&project, &id, Some(&key), &body).await {
                tracing::warn!(project, trigger = id, "file enqueue failed: {e}");
            }
        }
        last = cur;
    }
}

/// `Some(mtime)` when `path` exists (mtime as epoch-secs, 0 if unreadable),
/// `None` when absent.
fn stat_file(path: &str) -> Option<i64> {
    std::fs::metadata(path).ok().map(|m| {
        m.modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    })
}

/// Classify a stat transition into `(eventKind, mtime)`, or `None` for no change.
fn file_transition(last: &Option<i64>, cur: &Option<i64>) -> Option<(&'static str, i64)> {
    match (last, cur) {
        (None, Some(m)) => Some(("created", *m)),
        (Some(_), None) => Some(("deleted", now_secs())),
        (Some(a), Some(b)) if a != b => Some(("modified", *b)),
        _ => None,
    }
}

/// Sleep for `dur`, returning `false` if the stop signal fired first (so the
/// caller breaks its loop).
async fn sleep_or_stop(handle: &Arc<LoopHandle>, dur: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(dur) => handle.is_running(),
        _ = handle.stopped() => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_worker_types_dedups_and_accepts_both_keys() {
        let manifest = serde_json::json!({
            "workers": [
                { "taskType": "slack:send-message" },
                { "type": "http:call" },
                { "taskType": "slack:send-message" },
                { "taskType": "" },
                { "name": "no-type" }
            ]
        });
        assert_eq!(
            parse_worker_types(&manifest),
            vec!["slack:send-message".to_string(), "http:call".to_string()]
        );
        // No workers[] at all → empty, not a panic.
        assert!(parse_worker_types(&serde_json::json!({})).is_empty());
    }

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        use chrono::{TimeZone, Utc};
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .unwrap()
            .timestamp()
    }

    #[test]
    fn cron_parses_and_matches_every_minute() {
        let c = CronSpec::parse("* * * * *").unwrap();
        assert!(c.matches(ts(2026, 7, 24, 10, 0)));
        assert!(c.matches(ts(2026, 7, 24, 10, 37)));
    }

    #[test]
    fn cron_daily_at_six() {
        let c = CronSpec::parse("0 6 * * *").unwrap();
        assert!(c.matches(ts(2026, 7, 24, 6, 0)));
        assert!(!c.matches(ts(2026, 7, 24, 6, 1)));
        assert!(!c.matches(ts(2026, 7, 24, 7, 0)));
        // next fire after 07:00 is the following day 06:00.
        let next = c.next_after(ts(2026, 7, 24, 7, 0)).unwrap();
        assert_eq!(next, ts(2026, 7, 25, 6, 0));
    }

    #[test]
    fn cron_step_and_list_and_range() {
        let c = CronSpec::parse("*/15 9-17 * * 1-5").unwrap();
        // 09:15 on a Friday (2026-07-24 is a Friday) matches.
        assert!(c.matches(ts(2026, 7, 24, 9, 15)));
        // 09:07 does not (not a /15 minute).
        assert!(!c.matches(ts(2026, 7, 24, 9, 7)));
        // 18:00 out of the 9-17 hour window.
        assert!(!c.matches(ts(2026, 7, 24, 18, 0)));
        // Sunday 2026-07-26 excluded by 1-5 dow.
        assert!(!c.matches(ts(2026, 7, 26, 9, 15)));
    }

    #[test]
    fn cron_dow_sunday_is_zero_or_seven() {
        let c0 = CronSpec::parse("0 0 * * 0").unwrap();
        let c7 = CronSpec::parse("0 0 * * 7").unwrap();
        let sunday = ts(2026, 7, 26, 0, 0);
        assert!(c0.matches(sunday));
        assert!(c7.matches(sunday));
    }

    #[test]
    fn cron_dom_dow_or_rule() {
        // Fire on the 1st OR on a Monday.
        let c = CronSpec::parse("0 0 1 * 1").unwrap();
        // 2026-07-01 is a Wednesday — matches via day-of-month.
        assert!(c.matches(ts(2026, 7, 1, 0, 0)));
        // 2026-07-06 is a Monday — matches via day-of-week.
        assert!(c.matches(ts(2026, 7, 6, 0, 0)));
        // 2026-07-07 (Tue, not the 1st) — no match.
        assert!(!c.matches(ts(2026, 7, 7, 0, 0)));
    }

    #[test]
    fn cron_rejects_malformed() {
        assert!(CronSpec::parse("* * * *").is_err()); // 4 fields
        assert!(CronSpec::parse("60 * * * *").is_err()); // minute out of range
        assert!(CronSpec::parse("* 24 * * *").is_err()); // hour out of range
        assert!(CronSpec::parse("*/0 * * * *").is_err()); // zero step
        assert!(CronSpec::parse("x * * * *").is_err()); // non-numeric
    }

    #[test]
    fn cron_next_after_is_strict() {
        let c = CronSpec::parse("0 6 * * *").unwrap();
        let at_six = ts(2026, 7, 24, 6, 0);
        // Strictly after: the same instant is not returned.
        assert_eq!(c.next_after(at_six).unwrap(), ts(2026, 7, 25, 6, 0));
    }

    #[test]
    fn on_missed_parse() {
        assert_eq!(OnMissed::parse(Some("once")), OnMissed::Once);
        assert_eq!(OnMissed::parse(Some("all")), OnMissed::All);
        assert_eq!(OnMissed::parse(Some("skip")), OnMissed::Skip);
        assert_eq!(OnMissed::parse(None), OnMissed::Skip);
        assert_eq!(OnMissed::parse(Some("bogus")), OnMissed::Skip);
    }

    #[test]
    fn file_transition_kinds() {
        assert_eq!(file_transition(&None, &Some(5)), Some(("created", 5)));
        assert_eq!(file_transition(&Some(5), &Some(9)), Some(("modified", 9)));
        assert_eq!(file_transition(&Some(5), &Some(5)), None);
        assert!(matches!(
            file_transition(&Some(5), &None),
            Some(("deleted", _))
        ));
        assert_eq!(file_transition(&None, &None), None);
    }

    #[test]
    fn parse_sources_classifies_kinds() {
        let manifest = json!({
            "triggers": [
                { "id": "morning", "type": "cron", "spec": "0 6 * * *", "action": { "start": "p" } },
                { "id": "hook", "type": "webhook", "path": "/h", "action": { "message": "m" } },
                { "id": "watch", "type": "file", "path": "/tmp/x", "action": { "start": "p" } },
                { "id": "mail", "type": "imap", "connection": "mbox", "action": { "start": "p" } },
                { "id": "bad", "type": "cron", "spec": "not a cron", "action": { "start": "p" } }
            ]
        });
        let (sources, errors) = parse_sources(&manifest);
        assert_eq!(sources.len(), 4); // the malformed cron is skipped
        assert_eq!(errors.len(), 1);
        assert!(matches!(sources[0].config, SourceConfig::Cron { .. }));
        assert!(matches!(sources[1].config, SourceConfig::Webhook));
        assert!(matches!(sources[2].config, SourceConfig::File { .. }));
        // An unknown-but-declared kind (pack source) is accepted as External.
        assert_eq!(sources[3].kind, "imap");
        assert!(matches!(sources[3].config, SourceConfig::External));
    }

    #[test]
    fn known_kinds_include_builtins() {
        let k = known_kinds();
        for b in BUILTIN_KINDS {
            assert!(k.contains(*b), "missing builtin {b}");
        }
        assert!(is_builtin("cron"));
        assert!(!is_builtin("imap"));
    }
}
