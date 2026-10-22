//! **Local llama.cpp sidecar supervisor.**
//!
//! ProcessOS reaches its LLM over an OpenAI-compatible HTTP endpoint (see [`crate::settings`]).
//! Rather than make the operator download, configure and run `llama-server` separately, this
//! module lets ProcessOS *start and stop them itself* — supervised child processes, exactly like
//! [`crate::supervisor::OwnNano`] does for the Nano gateway. Up to [`MAX_SIDECARS`] run at once
//! (each on its own port), so an investigation can pair a primary model with a sparring-partner or
//! loop-monitor model. A profile flagged `sidecar:true`
//! describes which model to load ([`LlmProfile::model_file`]) and any extra launch args
//! ([`LlmProfile::sidecar_args`]); the manager builds the `llama-server` command, spawns it,
//! drains its combined stdout/stderr into a bounded ring buffer, and serves the model at the
//! profile's `base_url` port.
//!
//! It is intentionally optional and crash-isolated: starting the sidecar is a button, the model
//! weights are the real cost (downloaded on demand by `llama-server` into the shared models dir),
//! and a profile can still point at a remote endpoint instead.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use crate::settings::LlmProfile;

/// Most recent log lines kept in memory for the streaming viewer.
const LOG_CAPACITY: usize = 2000;

/// How many `llama-server` sidecars may run at once. Two lets an investigation pair a primary
/// model with a sparring-partner / loop-monitor model (each on its own port).
pub const MAX_SIDECARS: usize = 2;

/// How many background model downloads may run at once. Independent of the serving sidecar pool —
/// a download is a transient `llama-server` that fetches the GGUF into the cache and is stopped
/// before it loads into memory, so it never occupies a serving slot.
pub const MAX_DOWNLOADS: usize = 2;

/// The plan for launching `llama-server`: the resolved binary, argv, and the `LLAMA_CACHE`
/// (models) directory. Split out from the spawn so it can be unit-tested and shown to the operator
/// as the equivalent terminal command.
/// A resolved **draft model** for llama.cpp speculative decoding: the primary `llama-server`
/// loads this second GGUF via `--model-draft` and uses it to propose tokens the main model then
/// verifies. Built from a `speculator`-mode pairing ([`crate::pairings::PairMode::Speculator`])
/// after the compatibility gate (`crate::gguf::speculator_compatible`) has passed — the same
/// GGUF as the target is explicitly allowed ("the model loaded twice", self-speculation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftSpec {
    /// Profile the draft model came from (display only).
    pub profile_id: String,
    /// The resolved on-disk GGUF path of the draft model.
    pub model_path: PathBuf,
    /// `--draft-max` — most tokens drafted per step (llama-server default when `None`).
    pub draft_max: Option<u32>,
    /// `--draft-min` — fewest tokens drafted per step (llama-server default when `None`).
    pub draft_min: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchPlan {
    pub bin: String,
    pub args: Vec<String>,
    pub models_dir: PathBuf,
    /// Display fields echoed back in the status payload.
    pub profile_id: String,
    pub model: String,
    pub port: u16,
    /// True when the plan enables MTP (`--mtp`) self-speculative decoding.
    pub mtp: bool,
    /// The draft model GGUF loaded for speculative decoding (a speculator pairing), if any.
    pub draft_model: Option<String>,
}

impl LaunchPlan {
    /// Build the launch plan for a sidecar profile. `llama_bin` is the operator-configured binary
    /// path (else `llama-server` on `PATH`); `models_dir` is exported as `LLAMA_CACHE` so HF
    /// downloads and local GGUFs are shared with a separately-run llama.cpp. `draft` attaches a
    /// speculative-decoding draft model (from a speculator pairing); the caller is responsible for
    /// the MTP/compatibility gates — this only assembles argv.
    pub fn build(
        profile: &LlmProfile,
        models_dir: &Path,
        llama_bin: Option<&str>,
        port: u16,
        draft: Option<&DraftSpec>,
    ) -> Result<Self, String> {
        let model = profile
            .model_file
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or(profile.model.as_deref())
            .ok_or_else(|| {
                format!(
                    "profile '{}' has no model_file to load with the sidecar",
                    profile.id
                )
            })?
            .to_string();
        let bin = llama_bin
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("llama-server")
            .to_string();

        let mut args: Vec<String> = Vec::new();
        // A `.gguf` is a local file (absolute, or relative to the models dir); anything else is a
        // Hugging Face `repo[:quant]` spec that llama-server downloads into LLAMA_CACHE.
        if model.to_ascii_lowercase().ends_with(".gguf") {
            let p = PathBuf::from(&model);
            let resolved = if p.is_absolute() {
                p
            } else {
                models_dir.join(&p)
            };
            args.push("-m".into());
            args.push(resolved.display().to_string());
        } else {
            args.push("-hf".into());
            args.push(model.clone());
        }
        args.push("--host".into());
        args.push("127.0.0.1".into());
        args.push("--port".into());
        args.push(port.to_string());
        // MTP (multi-token prediction): llama.cpp's feature-flagged self-speculative decoding
        // using the model's built-in NextN prediction heads. Gated upstream by a GGUF scan.
        if profile.mtp {
            args.push("--mtp".into());
        }
        // Speculative decoding with a separate draft model (speculator pairing).
        if let Some(d) = draft {
            args.push("--model-draft".into());
            args.push(d.model_path.display().to_string());
            if let Some(n) = d.draft_max {
                args.push("--draft-max".into());
                args.push(n.to_string());
            }
            if let Some(n) = d.draft_min {
                args.push("--draft-min".into());
                args.push(n.to_string());
            }
        }
        // Performance defaults (GPU offload, flash attention, prompt-prefix KV reuse). Each is
        // suppressed when the operator already set an equivalent flag, so their value still wins.
        let operator_args = profile
            .sidecar_args
            .as_deref()
            .map(split_args)
            .unwrap_or_default();
        args.extend(perf_default_args(&operator_args));
        // Append the operator's extra args verbatim (e.g. "-ngl 20 -c 32768 --jinja") last, so
        // they can override every default above.
        args.extend(operator_args);

        Ok(Self {
            bin,
            args,
            models_dir: models_dir.to_path_buf(),
            profile_id: profile.id.clone(),
            model,
            port,
            mtp: profile.mtp,
            draft_model: draft.map(|d| d.model_path.display().to_string()),
        })
    }

    /// The equivalent shell command, so the operator can run the same server in a terminal to
    /// watch its output directly. Includes the `LLAMA_CACHE` export.
    pub fn command_line(&self) -> String {
        let mut s = format!(
            "LLAMA_CACHE={} {}",
            shell_quote(&self.models_dir.display().to_string()),
            shell_quote(&self.bin)
        );
        for a in &self.args {
            s.push(' ');
            s.push_str(&shell_quote(a));
        }
        s
    }
}

/// The managed port range ProcessOS draws sidecar ports from when a profile has no usable
/// preferred port (or it is taken). Chosen high enough to avoid common service ports.
const PORT_BASE: u16 = 18080;
const PORT_RANGE: u16 = 512;

/// Whether `port` can currently be bound on loopback (i.e. nothing else is listening on it).
/// Best-effort: there is a tiny TOCTOU window before `llama-server` binds it, but with at most
/// [`MAX_SIDECARS`] local sidecars and a re-check at start that is acceptable.
fn port_bindable(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Pick a free TCP port for a new sidecar, so the operator never configures one. Reuses
/// `preferred` (the profile's last-assigned port) when it is free, otherwise scans the managed
/// range. `exclude` is the set of ports already held by running sidecars.
fn pick_free_port(preferred: Option<u16>, exclude: &[u16]) -> Option<u16> {
    if let Some(p) = preferred {
        if p != 0 && !exclude.contains(&p) && port_bindable(p) {
            return Some(p);
        }
    }
    (PORT_BASE..PORT_BASE.saturating_add(PORT_RANGE))
        .find(|p| !exclude.contains(p) && port_bindable(*p))
}

/// llama.cpp performance defaults applied to every sidecar launch, each suppressed when the
/// operator's `sidecar_args` already sets an equivalent flag (matched on the listed aliases) so
/// the operator's value always wins. Conservative and fully overridable:
///  * `-ngl 99` — offload all layers to the GPU: the single biggest local-inference speedup on a
///    GPU/Metal build, and a no-op on a CPU-only build. Operators on a VRAM-limited GPU can cap it
///    with their own `-ngl N`.
///  * `-fa` — flash attention: faster attention and a smaller KV cache.
///  * `--cache-reuse 256` — reuse the KV cache across a shared prompt prefix, a large per-turn win
///    for ProcessOS's fixed investigation system prompt.
///
/// Each entry is `(canonical argv, &[aliases that suppress it])`.
const PERF_DEFAULTS: &[(&[&str], &[&str])] = &[
    (&["-ngl", "99"], &["-ngl", "--gpu-layers", "--n-gpu-layers"]),
    (&["-fa"], &["-fa", "--flash-attn"]),
    (&["--cache-reuse", "256"], &["--cache-reuse"]),
];

/// The perf defaults to insert for a launch, given the operator's already-split extra args. A
/// default is skipped when any of its alias flags is already present, so we never emit a duplicate
/// flag or clobber an operator-provided value.
fn perf_default_args(operator_args: &[String]) -> Vec<String> {
    // Match on the flag portion only, so an operator's `--flag=value` form (e.g.
    // `--cache-reuse=512`) suppresses the default just like the spaced `--flag value` form.
    let present: std::collections::HashSet<&str> = operator_args
        .iter()
        .map(|a| a.split('=').next().unwrap_or(a.as_str()))
        .collect();
    let mut out = Vec::new();
    for (argv, aliases) in PERF_DEFAULTS {
        if aliases.iter().any(|a| present.contains(a)) {
            continue;
        }
        out.extend(argv.iter().map(|s| (*s).to_string()));
    }
    out
}

/// Split a free-text args string into argv on whitespace, honouring simple single/double quotes
/// so a quoted value (e.g. a path with spaces) stays one argument.
fn split_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut has = false;
    for c in s.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                } else {
                    cur.push(c);
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                    has = true;
                } else if c.is_whitespace() {
                    if has || !cur.is_empty() {
                        out.push(std::mem::take(&mut cur));
                        has = false;
                    }
                } else {
                    cur.push(c);
                }
            }
        }
    }
    if has || !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Minimal shell quoting for the display-only command line.
fn shell_quote(s: &str) -> String {
    if !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./:=".contains(c))
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

/// A bounded, shared log buffer; lines carry a monotonic offset so the viewer can poll for new
/// output with `?since=N`.
#[derive(Clone, Default)]
struct LogBuffer {
    inner: Arc<Mutex<LogInner>>,
}

#[derive(Default)]
struct LogInner {
    lines: VecDeque<String>,
    /// Offset of the first line still retained (older lines were evicted).
    base: u64,
}

impl LogBuffer {
    fn push(&self, line: String) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.lines.push_back(line);
        while g.lines.len() > LOG_CAPACITY {
            g.lines.pop_front();
            g.base += 1;
        }
    }

    fn clear(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let end = g.base + g.lines.len() as u64;
        g.lines.clear();
        g.base = end;
    }

    /// Lines with offset >= `since`, plus the next offset to poll from.
    fn since(&self, since: u64) -> (Vec<String>, u64) {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let end = g.base + g.lines.len() as u64;
        let from = since.max(g.base);
        let start = (from - g.base) as usize;
        let lines = g.lines.iter().skip(start).cloned().collect();
        (lines, end)
    }
}

/// What's currently running (or last attempted), surfaced to the status endpoint.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LlamaStatus {
    pub running: bool,
    pub profile_id: Option<String>,
    pub model: Option<String>,
    pub port: Option<u16>,
    pub pid: Option<u32>,
    pub command: Option<String>,
    pub models_dir: Option<String>,
    pub started_at: Option<String>,
    /// True when this sidecar was launched with MTP (`--mtp`) enabled.
    pub mtp: bool,
    /// The draft-model GGUF loaded for speculative decoding, when a speculator pairing is active.
    pub draft_model: Option<String>,
    /// Set when the last start/stop attempt failed.
    pub error: Option<String>,
}

/// The aggregate sidecar state surfaced to `/api/llama/status`: every running sidecar plus the
/// pool's capacity, so the UI can decide whether another may be started.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusList {
    /// True when at least one sidecar is running (drives the rail status dot).
    pub running: bool,
    pub count: usize,
    pub max: usize,
    pub sidecars: Vec<LlamaStatus>,
    /// The most recent start/stop/exit error, if any.
    pub error: Option<String>,
}

struct Running {
    child: Child,
    profile_id: String,
    model: String,
    port: u16,
    pid: Option<u32>,
    command: String,
    models_dir: String,
    started_at: String,
    mtp: bool,
    draft_model: Option<String>,
}

/// A live background model download. The owning watcher task holds the `llama-server` child; this
/// handle stays in the manager so status can report it and the operator can cancel it. `cancel`
/// asks the watcher to kill the child; `finished` flips true once the watcher exits (complete,
/// cancelled, or the process died), at which point the handle is pruned.
struct DownloadHandle {
    profile_id: String,
    model: String,
    port: u16,
    started_at: String,
    cancel: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
}

/// A background download surfaced to `/api/llama/status` so the UI can show "Downloading…" with a
/// Logs/Cancel affordance and distinguish it from a configured-but-absent model.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadState {
    pub profile_id: String,
    pub model: String,
    pub port: u16,
    pub started_at: String,
}

#[derive(Default)]
struct ManagerInner {
    running: Vec<Running>,
    last_error: Option<String>,
}

/// The supervised-sidecar handle held in `AppState`. Cloneable (shares the inner lock + the
/// per-profile log buffers) so it can live in the axum state and be driven from handlers. Up to
/// [`MAX_SIDECARS`] `llama-server` children run concurrently, each keyed by its profile id.
#[derive(Clone, Default)]
pub struct LlamaManager {
    inner: Arc<Mutex<ManagerInner>>,
    /// One ring buffer per profile id, kept even after the sidecar stops so its final output is
    /// still viewable.
    logs: Arc<Mutex<HashMap<String, LogBuffer>>>,
    /// Active background downloads (each a transient `llama-server` fetching a GGUF into the cache).
    downloads: Arc<Mutex<Vec<DownloadHandle>>>,
}

impl LlamaManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// The log buffer for `profile_id`, created on first use.
    fn log_buf(&self, profile_id: &str) -> LogBuffer {
        self.logs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(profile_id.to_string())
            .or_default()
            .clone()
    }

    /// Start `llama-server` for sidecar `profile`, auto-assigning a free TCP port so the operator
    /// never has to configure one. Honours the profile's previously-assigned port when it is still
    /// free (keeping `base_url` stable across restarts), otherwise picks the next free port. The
    /// chosen port is returned on the status so the caller can persist it into the profile's
    /// `base_url` (the model client talks to that port). Errors if the profile is already running,
    /// the pool is full, no port is free, or the model/spawn fails.
    pub fn start_profile(
        &self,
        profile: &LlmProfile,
        models_dir: &Path,
        llama_bin: Option<&str>,
        draft: Option<&DraftSpec>,
    ) -> Result<LlamaStatus, String> {
        self.reap();
        let excluded: Vec<u16> = {
            let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if g.running.iter().any(|r| r.profile_id == profile.id) {
                return Err(format!("sidecar '{}' is already running", profile.id));
            }
            if g.running.len() >= MAX_SIDECARS {
                return Err(format!(
                    "already running {MAX_SIDECARS} sidecars (the maximum); stop one first"
                ));
            }
            g.running.iter().map(|r| r.port).collect()
        };
        let port = pick_free_port(profile.preferred_port(), &excluded).ok_or_else(|| {
            "no free TCP port available for the sidecar; stop another server and retry".to_string()
        })?;
        let plan = LaunchPlan::build(profile, models_dir, llama_bin, port, draft)?;
        self.start(plan)
    }

    /// Start `llama-server` for `plan`. Errors if that profile is already running, another sidecar
    /// already holds the port, the pool is full, or the spawn fails.
    pub fn start(&self, plan: LaunchPlan) -> Result<LlamaStatus, String> {
        self.reap();
        {
            let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if g.running.iter().any(|r| r.profile_id == plan.profile_id) {
                return Err(format!("sidecar '{}' is already running", plan.profile_id));
            }
            if let Some(other) = g.running.iter().find(|r| r.port == plan.port) {
                return Err(format!(
                    "port {} is already in use by sidecar '{}'; retry to get a different port",
                    plan.port, other.profile_id
                ));
            }
            if g.running.len() >= MAX_SIDECARS {
                return Err(format!(
                    "already running {MAX_SIDECARS} sidecars (the maximum); stop one first"
                ));
            }
        }
        // Refuse to serve a profile that is mid background-download — two llama-servers writing the
        // same cache blob would corrupt it.
        if self.is_downloading(&plan.profile_id) {
            return Err(format!(
                "profile '{}' is downloading in the background; wait for it to finish",
                plan.profile_id
            ));
        }
        let command = plan.command_line();
        let (child, pid) = self.spawn_child(&plan)?;

        let started_at = now_iso();
        let running = Running {
            child,
            profile_id: plan.profile_id.clone(),
            model: plan.model.clone(),
            port: plan.port,
            pid,
            command,
            models_dir: plan.models_dir.display().to_string(),
            started_at,
            mtp: plan.mtp,
            draft_model: plan.draft_model.clone(),
        };
        let status = running_status(&running, None);
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.last_error = None;
        g.running.push(running);
        Ok(status)
    }

    /// Spawn the `llama-server` child for `plan`: clears + seeds the profile's log buffer with the
    /// equivalent command, exports `LLAMA_CACHE`, and drains stdout+stderr into the ring buffer.
    /// Shared by [`Self::start`] (a serving sidecar) and [`Self::start_download`] (a background
    /// fetch). Returns the live child and its pid.
    fn spawn_child(&self, plan: &LaunchPlan) -> Result<(Child, Option<u32>), String> {
        let logs = self.log_buf(&plan.profile_id);
        logs.clear();
        logs.push(format!("$ {}", plan.command_line()));

        let mut cmd = Command::new(&plan.bin);
        cmd.args(&plan.args)
            .env("LLAMA_CACHE", &plan.models_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| {
            let msg = if e.kind() == std::io::ErrorKind::NotFound {
                format!(
                    "{} not found — llama.cpp is not installed (or not on PATH). \
                     Install it: https://github.com/ggml-org/llama.cpp/blob/master/docs/install.md \
                     (or set the llama binary path in Settings).",
                    plan.bin
                )
            } else {
                format!("failed to start {}: {e}", plan.bin)
            };
            self.set_error(&msg);
            logs.push(msg.clone());
            msg
        })?;
        let pid = child.id();

        // Drain BOTH stdout and stderr into the ring buffer so the pipe never blocks the child and
        // the operator sees everything (llama-server logs mostly to stderr).
        if let Some(out) = child.stdout.take() {
            self.spawn_drain(out, logs.clone());
        }
        if let Some(err) = child.stderr.take() {
            self.spawn_drain(err, logs.clone());
        }
        Ok((child, pid))
    }

    /// Start a BACKGROUND download of `profile`'s model into the cache, separate from the serving
    /// sidecar pool. Spawns a transient `llama-server` (which fetches the GGUF) and a watcher that
    /// stops it the moment the model is fully cached — before it loads the weights into memory — so
    /// the operator can pre-fetch a model without occupying a serving slot or RAM. `is_complete`
    /// reports whether the GGUF is fully present (the cache check lives in `main.rs`). Errors if the
    /// profile is already running/downloading, the download pool is full, no port is free, the model
    /// is already cached, or the spawn fails.
    pub fn start_download(
        &self,
        profile: &LlmProfile,
        models_dir: &Path,
        llama_bin: Option<&str>,
        is_complete: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<DownloadState, String> {
        self.reap();
        self.reap_downloads();
        if is_complete() {
            return Err(format!("model for '{}' is already downloaded", profile.id));
        }
        let mut excluded: Vec<u16> = {
            let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if g.running.iter().any(|r| r.profile_id == profile.id) {
                return Err(format!(
                    "profile '{}' is already running as a sidecar",
                    profile.id
                ));
            }
            g.running.iter().map(|r| r.port).collect()
        };
        {
            let d = self.downloads.lock().unwrap_or_else(|e| e.into_inner());
            if d.iter().any(|h| h.profile_id == profile.id) {
                return Err(format!("profile '{}' is already downloading", profile.id));
            }
            if d.len() >= MAX_DOWNLOADS {
                return Err(format!(
                    "already downloading {MAX_DOWNLOADS} models (the maximum); wait for one to finish"
                ));
            }
            excluded.extend(d.iter().map(|h| h.port));
        }
        let port = pick_free_port(profile.preferred_port(), &excluded)
            .ok_or_else(|| "no free TCP port available for the download".to_string())?;
        // A download never speculates — it is stopped before the model loads into memory.
        let plan = LaunchPlan::build(profile, models_dir, llama_bin, port, None)?;
        let (mut child, _pid) = self.spawn_child(&plan)?;

        let cancel = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let started_at = now_iso();
        self.downloads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(DownloadHandle {
                profile_id: profile.id.clone(),
                model: plan.model.clone(),
                port,
                started_at: started_at.clone(),
                cancel: cancel.clone(),
                finished: finished.clone(),
            });
        let state = DownloadState {
            profile_id: profile.id.clone(),
            model: plan.model.clone(),
            port,
            started_at,
        };

        // Watcher: poll until the GGUF is fully cached (or the operator cancels / the process dies),
        // then kill the downloader so it never proceeds to load the model into memory.
        let logs = self.log_buf(&profile.id);
        tokio::spawn(async move {
            let mut stable: u8 = 0;
            loop {
                if cancel.load(Ordering::Relaxed) {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    logs.push("[download cancelled]".into());
                    break;
                }
                match child.try_wait() {
                    Ok(Some(status)) => {
                        if is_complete() {
                            logs.push("[download complete — model cached]".into());
                        } else {
                            logs.push(format!("[downloader exited before completing: {status}]"));
                        }
                        break;
                    }
                    Ok(None) => {}
                    Err(_) => break,
                }
                // Require the cache to read complete for two consecutive polls (~3s) before stopping,
                // so we don't kill in a brief gap between sharded-GGUF parts.
                if is_complete() {
                    stable += 1;
                    if stable >= 2 {
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                        logs.push(
                            "[download complete — model cached; stopped the downloader before it loaded into memory]"
                                .into(),
                        );
                        break;
                    }
                } else {
                    stable = 0;
                }
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
            finished.store(true, Ordering::Relaxed);
        });

        Ok(state)
    }

    /// Ask the background download for `profile_id` to stop (the watcher kills the child on its next
    /// poll). Returns true if a matching active download was found.
    pub fn cancel_download(&self, profile_id: &str) -> bool {
        self.reap_downloads();
        let d = self.downloads.lock().unwrap_or_else(|e| e.into_inner());
        match d.iter().find(|h| h.profile_id == profile_id) {
            Some(h) => {
                h.cancel.store(true, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// Snapshot of the active background downloads, for `/api/llama/status`.
    pub fn download_states(&self) -> Vec<DownloadState> {
        self.reap_downloads();
        self.downloads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|h| DownloadState {
                profile_id: h.profile_id.clone(),
                model: h.model.clone(),
                port: h.port,
                started_at: h.started_at.clone(),
            })
            .collect()
    }

    /// Whether a background download is active for `profile_id`.
    fn is_downloading(&self, profile_id: &str) -> bool {
        self.reap_downloads();
        self.downloads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|h| h.profile_id == profile_id)
    }

    /// Drop the handles of downloads whose watcher has finished (complete / cancelled / died).
    fn reap_downloads(&self) {
        self.downloads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|h| !h.finished.load(Ordering::Relaxed));
    }

    /// Stop a sidecar by profile id, or — when `profile_id` is `None` — all of them (best-effort).
    /// Returns the resulting pool state.
    pub async fn stop(&self, profile_id: Option<&str>) -> StatusList {
        let to_kill: Vec<Running> = {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            match profile_id {
                Some(id) => match g.running.iter().position(|r| r.profile_id == id) {
                    Some(pos) => vec![g.running.remove(pos)],
                    None => Vec::new(),
                },
                None => std::mem::take(&mut g.running),
            }
        };
        for mut r in to_kill {
            let _ = r.child.start_kill();
            let _ = r.child.wait().await;
            self.log_buf(&r.profile_id).push("[server stopped]".into());
        }
        self.statuses()
    }

    /// Reap any children that exited on their own, so status reflects reality.
    pub fn reap(&self) {
        let mut exited_ids: Vec<String> = Vec::new();
        {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let mut i = 0;
            while i < g.running.len() {
                let exited = matches!(g.running[i].child.try_wait(), Ok(Some(_)));
                if exited {
                    let r = g.running.remove(i);
                    g.last_error = Some(format!("model server for '{}' exited", r.profile_id));
                    exited_ids.push(r.profile_id);
                } else {
                    i += 1;
                }
            }
        }
        for id in exited_ids {
            self.log_buf(&id).push("[server process exited]".into());
        }
    }

    /// The full pool state: every running sidecar plus capacity.
    pub fn statuses(&self) -> StatusList {
        self.reap();
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let sidecars: Vec<LlamaStatus> =
            g.running.iter().map(|r| running_status(r, None)).collect();
        StatusList {
            running: !sidecars.is_empty(),
            count: sidecars.len(),
            max: MAX_SIDECARS,
            sidecars,
            error: g.last_error.clone(),
        }
    }

    /// The status of a single profile's sidecar (running:false when it isn't up).
    pub fn status_of(&self, profile_id: &str) -> LlamaStatus {
        self.reap();
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match g.running.iter().find(|r| r.profile_id == profile_id) {
            Some(r) => running_status(r, g.last_error.clone()),
            None => empty_status(g.last_error.clone()),
        }
    }

    /// Log lines for `profile_id` at or after `since`, plus the next offset and whether that
    /// sidecar (or a background download for it) is still running.
    pub fn logs_since(&self, profile_id: &str, since: u64) -> (Vec<String>, u64, bool) {
        let (lines, next) = self.log_buf(profile_id).since(since);
        let serving = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .running
            .iter()
            .any(|r| r.profile_id == profile_id);
        let running = serving || self.is_downloading(profile_id);
        (lines, next, running)
    }

    fn spawn_drain<R>(&self, reader: R, logs: LogBuffer)
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                logs.push(line);
            }
        });
    }

    fn set_error(&self, msg: &str) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_error = Some(msg.to_string());
    }
}

fn running_status(r: &Running, last_error: Option<String>) -> LlamaStatus {
    LlamaStatus {
        running: true,
        profile_id: Some(r.profile_id.clone()),
        model: Some(r.model.clone()),
        port: Some(r.port),
        pid: r.pid,
        command: Some(r.command.clone()),
        models_dir: Some(r.models_dir.clone()),
        started_at: Some(r.started_at.clone()),
        mtp: r.mtp,
        draft_model: r.draft_model.clone(),
        error: last_error,
    }
}

fn empty_status(last_error: Option<String>) -> LlamaStatus {
    LlamaStatus {
        running: false,
        profile_id: None,
        model: None,
        port: None,
        pid: None,
        command: None,
        models_dir: None,
        started_at: None,
        mtp: false,
        draft_model: None,
        error: last_error,
    }
}

/// A coarse ISO-8601 UTC timestamp (seconds) without pulling in chrono.
fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // days-since-epoch → y/m/d (civil calendar, Howard Hinnant's algorithm).
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_buffer_recovers_from_a_poisoned_lock() {
        // A panic while holding the buffer's mutex poisons it. The lock-recovery
        // idiom (`.lock().unwrap_or_else(|e| e.into_inner())`) must keep the
        // buffer usable instead of propagating the poison as a panic — a
        // background trace-analysis plane should degrade, not crash, on poison.
        let buf = LogBuffer::default();
        buf.push("before".into());

        let poisoned = buf.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = poisoned.inner.lock().unwrap_or_else(|e| e.into_inner());
            panic!("poison the buffer lock");
        }));
        assert!(buf.inner.is_poisoned(), "lock should now be poisoned");

        // Every accessor must still work against the poisoned lock.
        buf.push("after".into());
        let (lines, _) = buf.since(0);
        assert_eq!(lines, vec!["before".to_string(), "after".to_string()]);
        buf.clear();
        let (lines, _) = buf.since(0);
        assert!(lines.is_empty(), "clear should still work after poison");
    }

    fn profile(model_file: &str, base_url: &str, args: Option<&str>) -> LlmProfile {
        LlmProfile {
            id: "p".into(),
            name: "p".into(),
            provider: Some("openai".into()),
            base_url: Some(base_url.into()),
            model: Some(model_file.into()),
            api_key: None,
            max_tokens: None,
            context_window: None,
            temperature: None,
            sidecar: true,
            model_file: Some(model_file.into()),
            sidecar_args: args.map(String::from),
            mtp: false,
            thinking_level: None,
            starred: false,
        }
    }

    #[test]
    fn builds_hf_launch_with_port() {
        let p = profile(
            "unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M",
            "http://127.0.0.1:8888/v1",
            Some("-ngl 99 -c 32768"),
        );
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8888, None).unwrap();
        assert_eq!(plan.bin, "llama-server");
        assert_eq!(plan.port, 8888);
        assert_eq!(
            plan.args,
            vec![
                "-hf",
                "unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M",
                "--host",
                "127.0.0.1",
                "--port",
                "8888",
                // Perf defaults: -ngl is suppressed (operator set it below); -fa + --cache-reuse
                // are inserted before the operator args.
                "-fa",
                "--cache-reuse",
                "256",
                "-ngl",
                "99",
                "-c",
                "32768",
            ]
        );
        let cl = plan.command_line();
        assert!(cl.contains("LLAMA_CACHE=/models"));
        assert!(cl.contains("-hf unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M"));
    }

    #[test]
    fn builds_local_gguf_launch_resolving_against_models_dir() {
        let p = profile("loan.gguf", "http://127.0.0.1:9001/v1", None);
        let plan = LaunchPlan::build(
            &p,
            Path::new("/models"),
            Some("/opt/llama-server"),
            9001,
            None,
        )
        .unwrap();
        assert_eq!(plan.bin, "/opt/llama-server");
        assert_eq!(plan.args[0], "-m");
        assert_eq!(plan.args[1], "/models/loan.gguf");
        assert_eq!(plan.port, 9001);
    }

    #[test]
    fn absolute_gguf_path_is_left_untouched() {
        let p = profile("/data/m.gguf", "http://127.0.0.1:8080/v1", None);
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8080, None).unwrap();
        assert_eq!(plan.args[1], "/data/m.gguf");
    }

    #[test]
    fn mtp_profile_gets_the_mtp_flag_before_operator_args() {
        let mut p = profile("m.gguf", "http://127.0.0.1:8080/v1", Some("-ngl 99"));
        p.mtp = true;
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8080, None).unwrap();
        let mtp_at = plan
            .args
            .iter()
            .position(|a| a == "--mtp")
            .expect("--mtp present");
        let ngl_at = plan.args.iter().position(|a| a == "-ngl").unwrap();
        assert!(
            mtp_at < ngl_at,
            "operator args must come last so they can override"
        );
        assert!(plan.mtp);
        assert!(plan.command_line().contains("--mtp"));
    }

    #[test]
    fn draft_spec_adds_model_draft_and_tuning_args() {
        let p = profile("target.gguf", "http://127.0.0.1:8080/v1", None);
        let draft = DraftSpec {
            profile_id: "draft-p".into(),
            model_path: PathBuf::from("/models/draft.gguf"),
            draft_max: Some(16),
            draft_min: Some(2),
        };
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8080, Some(&draft)).unwrap();
        let a = &plan.args;
        let md = a
            .iter()
            .position(|x| x == "--model-draft")
            .expect("--model-draft present");
        assert_eq!(a[md + 1], "/models/draft.gguf");
        assert!(a.windows(2).any(|w| w[0] == "--draft-max" && w[1] == "16"));
        assert!(a.windows(2).any(|w| w[0] == "--draft-min" && w[1] == "2"));
        assert_eq!(plan.draft_model.as_deref(), Some("/models/draft.gguf"));
        // Tuning flags are optional — omitted when unset.
        let bare = DraftSpec {
            profile_id: "draft-p".into(),
            model_path: PathBuf::from("/models/draft.gguf"),
            draft_max: None,
            draft_min: None,
        };
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8080, Some(&bare)).unwrap();
        assert!(!plan
            .args
            .iter()
            .any(|x| x == "--draft-max" || x == "--draft-min"));
    }

    #[test]
    fn perf_defaults_added_and_overridable() {
        // No operator args → all three defaults present.
        let p = profile("m.gguf", "http://127.0.0.1:8080/v1", None);
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8080, None).unwrap();
        let a = &plan.args;
        assert!(a.windows(2).any(|w| w[0] == "-ngl" && w[1] == "99"));
        assert!(a.iter().any(|x| x == "-fa"));
        assert!(a
            .windows(2)
            .any(|w| w[0] == "--cache-reuse" && w[1] == "256"));

        // Operator-set flags suppress the matching default (incl. aliases) — no duplicate, and the
        // operator's value wins.
        let p = profile(
            "m.gguf",
            "http://127.0.0.1:8080/v1",
            Some("--gpu-layers 20 -fa --cache-reuse 512"),
        );
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8080, None).unwrap();
        let a = &plan.args;
        // -ngl default suppressed by the --gpu-layers alias.
        assert!(!a.iter().any(|x| x == "-ngl"));
        assert!(a.windows(2).any(|w| w[0] == "--gpu-layers" && w[1] == "20"));
        // -fa appears exactly once (default suppressed by operator's own -fa).
        assert_eq!(a.iter().filter(|x| *x == "-fa").count(), 1);
        // --cache-reuse appears once with the operator's value, not the default 256.
        assert_eq!(a.iter().filter(|x| *x == "--cache-reuse").count(), 1);
        assert!(a
            .windows(2)
            .any(|w| w[0] == "--cache-reuse" && w[1] == "512"));

        // The `--flag=value` form also suppresses the matching default (no duplicate flag).
        let p = profile(
            "m.gguf",
            "http://127.0.0.1:8080/v1",
            Some("--gpu-layers=20 --cache-reuse=512"),
        );
        let plan = LaunchPlan::build(&p, Path::new("/models"), None, 8080, None).unwrap();
        let a = &plan.args;
        assert!(!a.iter().any(|x| x == "-ngl"));
        assert!(!a.iter().any(|x| x == "--cache-reuse"));
        assert!(a.iter().any(|x| x == "--gpu-layers=20"));
        assert!(a.iter().any(|x| x == "--cache-reuse=512"));
        // -fa was not overridden, so its default is still present.
        assert!(a.iter().any(|x| x == "-fa"));
    }

    #[test]
    fn split_args_honours_quotes() {
        assert_eq!(
            split_args("-ngl 99 -c 32768"),
            vec!["-ngl", "99", "-c", "32768"]
        );
        assert_eq!(
            split_args("--chat-template '/a b/t.jinja' -ngl 99"),
            vec!["--chat-template", "/a b/t.jinja", "-ngl", "99"]
        );
        assert!(split_args("   ").is_empty());
    }

    #[test]
    fn log_buffer_offsets_and_eviction() {
        let buf = LogBuffer::default();
        buf.push("a".into());
        buf.push("b".into());
        let (lines, next) = buf.since(0);
        assert_eq!(lines, vec!["a", "b"]);
        assert_eq!(next, 2);
        // Polling from the tip returns nothing new.
        let (lines, _) = buf.since(2);
        assert!(lines.is_empty());
    }

    #[test]
    fn now_iso_is_well_formed() {
        let s = now_iso();
        assert_eq!(s.len(), 20);
        assert!(s.ends_with('Z') && s.contains('T'));
    }

    /// A launch plan that runs a harmless long-lived child (`sleep`) standing in for `llama-server`,
    /// so the supervisor's bookkeeping/guards can be tested without a model.
    fn sleep_plan(profile_id: &str, port: u16) -> LaunchPlan {
        LaunchPlan {
            bin: "sleep".into(),
            args: vec!["30".into()],
            models_dir: PathBuf::from("/tmp"),
            profile_id: profile_id.into(),
            model: "stub".into(),
            port,
            mtp: false,
            draft_model: None,
        }
    }

    #[tokio::test]
    async fn runs_two_sidecars_then_caps_at_max() {
        let mgr = LlamaManager::new();
        let a = mgr.start(sleep_plan("a", 9001)).unwrap();
        assert!(a.running && a.profile_id.as_deref() == Some("a"));
        // A second, distinct profile on a distinct port is allowed (up to MAX_SIDECARS).
        let b = mgr.start(sleep_plan("b", 9002)).unwrap();
        assert!(b.running);
        let st = mgr.statuses();
        assert_eq!(st.count, 2);
        assert_eq!(st.max, MAX_SIDECARS);
        assert!(st.running);
        // A third exceeds the pool cap.
        let third = mgr.start(sleep_plan("c", 9003));
        assert!(third.unwrap_err().contains("maximum"));
        mgr.stop(None).await;
        assert_eq!(mgr.statuses().count, 0);
    }

    #[tokio::test]
    async fn rejects_duplicate_profile_and_port_collision() {
        let mgr = LlamaManager::new();
        mgr.start(sleep_plan("a", 9101)).unwrap();
        // Same profile id → already running.
        assert!(mgr
            .start(sleep_plan("a", 9109))
            .unwrap_err()
            .contains("already running"));
        // Different profile but the same port → collision safety-net (auto-assignment normally
        // prevents this, but the low-level start() still rejects it).
        let err = mgr.start(sleep_plan("b", 9101)).unwrap_err();
        assert!(err.contains("port 9101") && err.contains("already in use"));
        mgr.stop(None).await;
    }

    #[test]
    fn pick_free_port_reuses_preferred_else_scans_range() {
        // A reachable free port is honoured verbatim (stable base_url across restarts).
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let taken = listener.local_addr().unwrap().port();
        // When the preferred port is excluded (already held by another sidecar), allocation falls
        // back into the managed range and never reuses it.
        let chosen = pick_free_port(Some(taken), &[taken]).unwrap();
        assert_ne!(chosen, taken);
        assert!((PORT_BASE..PORT_BASE + PORT_RANGE).contains(&chosen));
        // With no preference, a port from the managed range is returned.
        let any = pick_free_port(None, &[]).unwrap();
        assert!((PORT_BASE..PORT_BASE + PORT_RANGE).contains(&any));
    }

    #[tokio::test]
    async fn stop_targets_one_sidecar_and_keeps_the_other() {
        let mgr = LlamaManager::new();
        mgr.start(sleep_plan("a", 9201)).unwrap();
        mgr.start(sleep_plan("b", 9202)).unwrap();
        let after = mgr.stop(Some("a")).await;
        assert_eq!(after.count, 1);
        assert_eq!(after.sidecars[0].profile_id.as_deref(), Some("b"));
        assert!(!mgr.status_of("a").running);
        assert!(mgr.status_of("b").running);
        mgr.stop(None).await;
    }

    #[tokio::test]
    async fn logs_are_kept_per_profile() {
        let mgr = LlamaManager::new();
        // Before anything starts, an unknown profile's buffer is empty.
        assert!(mgr.logs_since("b", 0).0.is_empty());
        mgr.start(sleep_plan("a", 9301)).unwrap();
        // 'a' has its launch line; 'b' is still untouched (separate buffer).
        let (la, _, a_running) = mgr.logs_since("a", 0);
        assert!(a_running && la.iter().any(|l| l.starts_with("$ ")));
        assert!(mgr.logs_since("b", 0).0.is_empty());
        mgr.start(sleep_plan("b", 9302)).unwrap();
        assert!(mgr.logs_since("b", 0).0.iter().any(|l| l.starts_with("$ ")));
        mgr.stop(None).await;
    }
}
