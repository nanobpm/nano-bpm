//! ProcessOS server — Stage T1.
//!
//! The optimization plane runs as its **own process with its own webserver**,
//! separate from the Nano gateway: *Nano handles production, ProcessOS handles
//! optimization.* This binary ingests Nano's public trace/metrics export and serves
//! an Insights report (`GET /api/insights`) plus a small dashboard.
//!
//! The dependency is strictly one-way — ProcessOS reads Nano over HTTP; Nano never
//! links or knows about ProcessOS. Run a Nano cluster without this binary and
//! production is unaffected.

mod advisor;
mod agent;
mod analysis;
mod annotations;
mod bpmn_model;
mod camunda_import;
mod chat;
mod chat_prompts;
mod cockpit;
mod colorize;
mod conformance;
mod contracts;
mod conversation;
mod corpus;
mod curator;
mod dataset;
mod deps;
mod experiment;
mod gguf;
mod harness;
mod investigate;
mod ir_spec;
mod layout;
mod llama;
mod model_ir;
mod monitor;
mod nano_instances;
mod pairings;
mod personas;
mod pilot;
mod pyrunner;
mod reasoning;
mod report;
mod settings;
mod supervisor;
mod tools;
mod trace;
mod workspace;

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use nanobpmn_engine_core::bpmn::parse_bpmn;
use serde::Deserialize;

use crate::contracts::NanoClient;
use crate::harness::{
    apply_calibration, build_baseline, build_cluster_summary, build_evolve_prompt,
    calibrate_from_measured, example_scenario, list_models, llm_complete, llm_complete_streaming,
    parse_structural_candidates, rank_candidates_by_replay, replay_dataset, replay_instance,
    run_hypothesis, run_scenario, staff_for_summary, summarize_dataset, CandidateModel, LlmConfig,
    LlmOverride, MeasuredJobType, Prompt, PromptLibrary, RecordedInstance, Scenario,
    DEFAULT_EVOLVE_SYSTEM_PROMPT, DEFAULT_PROMPT_ID, DEFAULT_SYSTEM_PROMPT,
};

/// Per-session steering queues: an operator instruction stack drained into the running turn.
type SteerQueues =
    std::sync::Mutex<std::collections::HashMap<String, Arc<std::sync::Mutex<Vec<String>>>>>;

/// Per-session live completion ids (`chatcmpl-…`): the in-flight turn's id, used to target it
/// with the reasoning-control endpoint (end its thinking mid-generation).
type CompletionIds =
    std::sync::Mutex<std::collections::HashMap<String, Arc<std::sync::Mutex<Option<String>>>>>;

#[derive(Clone)]
struct AppState {
    /// The client's production engine — the read-only analysis TARGET. Traces,
    /// metrics, and the baseline process definition are read from here; ProcessOS
    /// never runs its own meta-workloads on the client's engine.
    ///
    /// This is the boot-configured *fallback*. The live analysis target is resolved per
    /// request from the active [`nano_instances`] selection via [`AppState::active_target`],
    /// so the operator can switch instances from the Console without a restart.
    target: NanoClient,
    /// ProcessOS's OWN engine — where the `pilotSelfOptimize` loop runs and its
    /// workers are deployed. The cockpit creates/reads experiments and completes
    /// the human Review task here.
    own: NanoClient,
    /// The prompt library — import / select / author the system prompts that drive
    /// hypothesis generation. Shared, interior-mutable so authoring is live.
    prompts: Arc<RwLock<PromptLibrary>>,
    /// Durable cockpit conversations (§10) — the persisted pilot ↔ droid dialogue,
    /// one append-only log per experiment.
    conversations: Arc<conversation::ConversationStore>,
    /// The pilot process (§10 plastic surface **a**) — the forkable BPMN choreography
    /// that drives the optimization loop. File-backed; the supervisor deploys it on
    /// boot, and `PUT /api/pilot` re-forks it and hot-redeploys to the own engine.
    pilot: Arc<pilot::PilotStore>,
    /// The consultant's workspace tree — workspaces > processes, each bound to a live
    /// customer Nano or a loaded trace dataset. Discovered by scanning the root.
    workspaces: workspace::WorkspaceCatalog,
    /// Operator-editable settings (LLM connection + Python interpreter), persisted to the
    /// user's config dir and layered over the environment at request time.
    settings: settings::SettingsStore,
    /// The configurable Nano instances the Console manages. The active selection resolves
    /// the live analysis target (see [`AppState::active_target`]); seeded with the
    /// boot-configured URL (default `http://localhost:8080`) on first run.
    nano_instances: Arc<nano_instances::NanoInstanceStore>,
    /// Persisted interactive cockpit chat sessions — the full droid transcript per
    /// `(workspace, process)`, so a conversation resumes with memory across turns and
    /// restarts.
    chat: Arc<chat::ChatStore>,
    /// The operator's chat prompt library (reusable compose-box message templates),
    /// persisted to the user's config dir alongside `settings.json`.
    chat_prompts: Arc<chat_prompts::ChatPromptStore>,
    /// The operator's persona library (selectable standing system prompts for chat),
    /// persisted to the user's config dir alongside `settings.json`.
    personas: Arc<personas::PersonaStore>,
    /// The operator's LLM-pairing library (named, first-class "Pair AI" configs), persisted to
    /// the user's config dir alongside `settings.json`.
    pairings: Arc<pairings::PairingStore>,
    /// The operator's custom-tool library (data-driven investigation tools), persisted to the
    /// user's config dir alongside `settings.json`.
    tools: Arc<tools::ToolStore>,
    /// Wrap-up flags for in-flight chat turns, keyed by session. The cockpit's "wrap it up"
    /// control sets the flag; the agent loop checks it between rounds and reports early.
    chat_cancels: Arc<
        std::sync::Mutex<std::collections::HashMap<String, Arc<std::sync::atomic::AtomicBool>>>,
    >,
    /// Steering queues for in-flight chat turns, keyed by session. The cockpit's "Steer …"
    /// control appends an operator instruction; the agent loop drains it at the next round
    /// boundary and injects it as a user turn, redirecting an investigation without restarting it.
    chat_steers: Arc<SteerQueues>,
    /// Live OpenAI completion ids (`chatcmpl-…`) for in-flight chat turns, keyed by session. The
    /// streaming agent sets the inner cell as soon as the first chunk carries the id; the wrap-up
    /// handler and the loop monitor read it to target the live turn with the reasoning-control
    /// endpoint (end its thinking mid-generation). Cleared once the turn ends.
    chat_cmpl_ids: Arc<CompletionIds>,
    /// The exact model request bodies sent during each session's most recent turn, keyed by
    /// session (one entry per round), powering the cockpit's per-chat "Debug" tab. Durable —
    /// written through to disk (alongside the chat store) so it survives a server restart.
    chat_debug: Arc<chat::DebugStore>,
    /// Live event buffers for in-flight chat turns, keyed by session. The streaming turn appends
    /// every SSE event here as it produces it; the original request *and* any later reattach
    /// (`GET .../chat/stream/live`, used after a page reload) replay the buffer then follow along.
    /// This decouples the running investigation from the fetch that started it, so reloading the
    /// page no longer loses the live view. Removed once the turn ends.
    chat_live: Arc<std::sync::Mutex<std::collections::HashMap<String, LiveTurn>>>,
    /// The supervised local llama.cpp `llama-server` sidecar (start/stop/status/logs). Optional at
    /// runtime — nothing runs until the operator presses Start for a `sidecar:true` profile.
    llama: llama::LlamaManager,
    /// The previous `/metrics` scrape per live-instance base URL, so the worker-scaling advisor
    /// (`GET /api/advisor`) can measure backlog growth and per-job-type drain rate across polls.
    advisor_prev: Arc<std::sync::Mutex<std::collections::HashMap<String, advisor::Snapshot>>>,
}

impl AppState {
    /// Resolve the live analysis target: the active configured Nano instance if one is set,
    /// otherwise the boot-configured fallback. Constructing a [`NanoClient`] is cheap, so
    /// callers resolve it per request — letting the operator switch instances from the
    /// Console without restarting the server.
    fn active_target(&self) -> NanoClient {
        match self.nano_instances.active_base_url() {
            Some(url) if !url.trim().is_empty() => NanoClient::new(url),
            _ => self.target.clone(),
        }
    }
}

/// An append-only buffer for one in-flight chat turn's SSE events, shared between the producing
/// (blocking) agent task and any number of SSE consumers. Consumers each keep their own cursor,
/// replay `events` from 0, then wait on `notify` for more until `done` is set.
#[derive(Clone)]
struct LiveTurn {
    /// The operator message that started this turn — used to render the user bubble on reattach,
    /// since the persisted transcript doesn't yet include this in-flight turn.
    user: String,
    /// Every SSE event emitted so far this turn (including the terminal `done`/`error`).
    events: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    /// Woken whenever a new event is appended or the turn ends.
    notify: Arc<tokio::sync::Notify>,
    /// Set once a terminal (`done`/`error`) event has been appended.
    done: Arc<std::sync::atomic::AtomicBool>,
}

impl LiveTurn {
    fn new(user: String) -> Self {
        LiveTurn {
            user,
            events: Arc::new(std::sync::Mutex::new(Vec::new())),
            notify: Arc::new(tokio::sync::Notify::new()),
            done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Append one event and wake all consumers. Terminal events also flip `done`.
    fn emit(&self, v: serde_json::Value) {
        let terminal = matches!(
            v.get("type").and_then(|t| t.as_str()),
            Some("done") | Some("error")
        );
        if let Ok(mut e) = self.events.lock() {
            e.push(v);
        }
        if terminal {
            self.done.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        self.notify.notify_waiters();
    }
}

/// Build an SSE response that replays a `LiveTurn`'s buffered events from the beginning, then
/// follows along live until the turn ends. Multiple consumers (the original request and any
/// number of reattachers) can each call this independently; each keeps its own cursor.
fn sse_from_live(
    live: LiveTurn,
) -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = futures_util::stream::unfold(0usize, move |cursor| {
        let live = live.clone();
        async move {
            loop {
                // Arm the wakeup *before* inspecting the buffer so an event appended between our
                // read and our await can't be missed (lost-wakeup-safe).
                let notified = live.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let item = live.events.lock().ok().and_then(|e| e.get(cursor).cloned());
                if let Some(v) = item {
                    return Some((
                        Ok::<Event, std::convert::Infallible>(Event::default().data(v.to_string())),
                        cursor + 1,
                    ));
                }
                if live.done.load(std::sync::atomic::Ordering::Relaxed) {
                    return None;
                }
                notified.await;
            }
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Server configuration, all overridable by environment.
struct Config {
    port: u16,
    /// The client's production engine ProcessOS analyses (read-only). `NANO_TARGET_URL`,
    /// falling back to `NANO_BASE_URL`, then `http://localhost:8080`.
    target_url: String,
    /// ProcessOS's own engine, where the pilot loop + workers run. `PROCESSOS_NANO_URL`,
    /// falling back to `NANO_BASE_URL`, then `http://localhost:8080`. In a single-Nano
    /// dev setup this equals `target_url`, keeping the historical behaviour.
    own_url: String,
    /// Optional directory to import prompts from on startup (`PROCESSOS_PROMPTS_DIR`).
    prompts_dir: Option<String>,
    /// Directory for durable state — currently cockpit conversations
    /// (`PROCESSOS_DATA_DIR`, default `./.processos-data`).
    data_dir: std::path::PathBuf,
}

impl Config {
    fn from_env() -> Self {
        let port = std::env::var("PROCESSOS_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8090);
        let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
        let (target_url, own_url) = resolve_nano_urls(
            env("NANO_BASE_URL"),
            env("NANO_TARGET_URL"),
            env("PROCESSOS_NANO_URL"),
        );
        let prompts_dir = std::env::var("PROCESSOS_PROMPTS_DIR")
            .ok()
            .filter(|s| !s.is_empty());
        Self {
            port,
            target_url,
            own_url,
            prompts_dir,
            data_dir: conversation::data_dir_from_env(),
        }
    }
}

/// Resolve the (target, own) Nano URLs from the three env sources. `NANO_BASE_URL`
/// is the back-compat alias that defaults BOTH roles; `NANO_TARGET_URL` and
/// `PROCESSOS_NANO_URL` override each role independently. With none set, both fall
/// back to the local gateway. Pure so the precedence is unit-testable.
fn resolve_nano_urls(
    base: Option<String>,
    target: Option<String>,
    own: Option<String>,
) -> (String, String) {
    let default = base.unwrap_or_else(|| "http://localhost:8080".to_string());
    let target_url = target.unwrap_or_else(|| default.clone());
    let own_url = own.unwrap_or(default);
    (target_url, own_url)
}

/// Help / usage text for the CLI, reused by `--help` and the unknown-subcommand
/// error. The version is stamped at build time (see build.rs).
fn usage() -> String {
    format!(
        "processos {ver}\n\
         The Nano BPM process-optimization plane.\n\
         Advanced Research Prototype.\n\n\
         USAGE:\n  \
         processos [SUBCOMMAND]\n\n\
         With no subcommand, starts the optimization-plane server (configured via\n\
         environment variables).\n\n\
         SUBCOMMANDS:\n  \
         gen <pack.json> <out-dir>                  Generate a synthetic dataset\n  \
         infer <dataset-dir> [target-p99-wait-ms]   Infer insights from a dataset\n  \
         import-camunda <records.json|dir> <out-dir> [--no-tier2]\n  \
         {pad}Import a Camunda 8 record export\n  \
         layout <in.bpmn> <annotations.json> [--out out.bpmn] [--debug-svg out.svg] [--solver rowbias|field] [--side-by-side sxs.svg]\n  \
         {pad}Run the semantic BPMN layout (see docs/layout.md)\n  \
         emit-gbnf [--out ir.gbnf]                  Emit the reversible-IR GBNF for llama.cpp\n  \
         {pad}(see docs/adr/0001; hand to `llama-server --grammar-file`)\n\n\
         OPTIONS:\n  \
         -h, --help       Print this help\n  \
         -V, --version    Print version\n",
        ver = env!("PROCESSOS_VERSION"),
        pad = "                                           ",
    )
}

#[tokio::main]
async fn main() {
    // CLI subcommands run a one-shot task and exit before the server boots. This keeps
    // the default (no-args) behaviour — start the optimization-plane server — intact.
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "-h" | "--help" | "help" => {
                print!("{}", usage());
                return;
            }
            "-V" | "--version" | "version" => {
                println!("processos {}", env!("PROCESSOS_VERSION"));
                return;
            }
            "gen" | "generate" => {
                run_cli_generate(&args[2..]);
                return;
            }
            "infer" => {
                run_cli_infer(&args[2..]);
                return;
            }
            "import-camunda" => {
                run_cli_import_camunda(&args[2..]);
                return;
            }
            "layout" => {
                run_cli_layout(&args[2..]);
                return;
            }
            "emit-gbnf" => {
                run_cli_emit_gbnf(&args[2..]);
                return;
            }
            other => {
                eprintln!("unknown subcommand '{other}'.\n");
                eprint!("{}", usage());
                std::process::exit(2);
            }
        }
    }

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cfg = Config::from_env();

    // Seed the prompt library with the built-in default, then import any prompts the
    // operator dropped in PROCESSOS_PROMPTS_DIR.
    let mut library = PromptLibrary::seeded(DEFAULT_SYSTEM_PROMPT);
    if let Some(dir) = &cfg.prompts_dir {
        match library.import_dir(std::path::Path::new(dir)) {
            Ok(n) => tracing::info!(dir = %dir, imported = n, "imported prompts"),
            Err(e) => tracing::warn!(dir = %dir, error = %e, "prompt import failed"),
        }
    }

    // The pilot process (§10 surface a) — file-backed under the data dir, seeded with
    // the built-in default on first boot. The supervisor deploys whatever it resolves.
    let pilot = Arc::new(pilot::PilotStore::open(&cfg.data_dir));

    // If configured, spawn ProcessOS's OWN Nano engine (the client's production
    // engine is the read-only target; ProcessOS runs its pilot loop here). The
    // spawned engine's URL overrides `own_url`. Held to shutdown so we don't orphan
    // a gateway across restarts.
    let mut own_engine: Option<supervisor::OwnNano> = None;
    let own_url = match supervisor::SpawnConfig::from_env() {
        Some(spawn_cfg) => {
            tracing::info!(
                bin = %spawn_cfg.bin.display(),
                data_dir = %spawn_cfg.data_dir.display(),
                "starting ProcessOS's own Nano engine"
            );
            match supervisor::OwnNano::spawn(&spawn_cfg, &pilot.current_xml()).await {
                Ok(engine) => {
                    let url = engine.base_url.clone();
                    let src = pilot.doc().source;
                    tracing::info!(own = %url, pilot = %src, "own Nano engine ready; pilot process deployed");
                    own_engine = Some(engine);
                    url
                }
                Err(e) => panic!("failed to start own Nano engine: {e}"),
            }
        }
        None => cfg.own_url.clone(),
    };

    let state = AppState {
        target: NanoClient::new(&cfg.target_url),
        own: NanoClient::new(&own_url),
        prompts: Arc::new(RwLock::new(library)),
        conversations: Arc::new(conversation::ConversationStore::open(&cfg.data_dir)),
        pilot,
        workspaces: workspace::WorkspaceCatalog::open(workspace::root_from_env(&cfg.data_dir)),
        settings: settings::SettingsStore::open(),
        nano_instances: Arc::new(nano_instances::NanoInstanceStore::open(
            settings::config_dir().join("nano-instances.json"),
            &cfg.target_url,
        )),
        chat: Arc::new(chat::ChatStore::open(cfg.data_dir.join("chat"))),
        chat_prompts: Arc::new(chat_prompts::ChatPromptStore::open(
            settings::config_dir().join("chat-prompts.json"),
        )),
        personas: Arc::new(personas::PersonaStore::open(
            settings::config_dir().join("personas.json"),
        )),
        pairings: Arc::new(pairings::PairingStore::open(
            settings::config_dir().join("pairings.json"),
        )),
        tools: Arc::new(tools::ToolStore::open(
            settings::config_dir().join("tools.json"),
        )),
        chat_cancels: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        chat_steers: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        chat_cmpl_ids: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        chat_debug: Arc::new(chat::DebugStore::open(cfg.data_dir.join("chat"))),
        chat_live: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        llama: llama::LlamaManager::new(),
        advisor_prev: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    };

    // A handle to stop the llama sidecar on shutdown (the router takes ownership of `state`).
    let llama_for_shutdown = state.llama.clone();

    let app = Router::new()
        .route("/", get(landing))
        .route("/features", get(features))
        .route("/guide", get(guide_index))
        .route("/guide/{slug}", get(guide_section))
        .route("/console", get(dashboard))
        .route("/cockpit", get(cockpit_page))
        .route("/health", get(health))
        .route("/api/insights", get(insights))
        .route("/api/cockpit/overview", get(cockpit_overview))
        .route("/api/advisor", get(advisor_advise))
        .route(
            "/api/cockpit/experiments",
            get(cockpit_experiments).post(cockpit_create),
        )
        .route("/api/cockpit/experiments/{key}", get(cockpit_experiment))
        .route(
            "/api/cockpit/experiments/{key}/decision",
            post(cockpit_decision),
        )
        .route(
            "/api/cockpit/experiments/{key}/conversation",
            get(cockpit_conversation).post(cockpit_message),
        )
        .route("/harness", get(harness_dashboard))
        .route("/api/harness/example", get(harness_example))
        .route("/api/harness/example/run", get(harness_example_run))
        .route("/api/harness/run", post(harness_run))
        .route("/api/harness/calibrate", post(harness_calibrate))
        .route("/api/harness/hypothesize", post(harness_hypothesize))
        .route("/api/harness/replay", post(harness_replay))
        .route("/api/harness/replay-batch", post(harness_replay_batch))
        .route("/api/harness/replay-rank", post(harness_replay_rank))
        .route("/api/harness/evolve", post(harness_evolve))
        .route("/api/harness/production", get(harness_production))
        .route("/api/harness/cluster", get(harness_cluster))
        .route("/api/prompts", get(prompts_list).post(prompts_upsert))
        .route("/api/prompts/{id}", get(prompts_get).delete(prompts_delete))
        .route("/api/pilot", get(pilot_get).put(pilot_put))
        .route("/api/pilot/reset", post(pilot_reset))
        .route("/api/layout", post(layout_relayout))
        .route("/api/layout/score", post(layout_score))
        .route("/api/colorize", post(colorize_flows_handler))
        .route("/api/curator/propose", post(curator_propose))
        .route("/api/curator/propose/stream", post(curator_propose_stream))
        .route("/api/ir/grammar.gbnf", get(ir_grammar_gbnf))
        .route("/workspace", get(workspace_page))
        .route("/semantics", get(semantics_page))
        .route(
            "/api/workspaces",
            get(ws_workspaces).post(ws_create_workspace),
        )
        .route("/api/workspaces/{workspace}", get(ws_workspace))
        .route(
            "/api/workspaces/demo-datasets",
            get(ws_demo_datasets),
        )
        .route("/api/workspaces/seed-demo", post(ws_seed_demo))
        .route(
            "/api/workspaces/{workspace}/processes",
            post(ws_create_process),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}",
            get(ws_process).put(ws_update_process),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/model",
            get(ws_process_model).put(ws_set_process_model),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/annotations",
            get(ws_process_annotations).put(ws_set_process_annotations),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/insights",
            get(ws_process_insights),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/investigate",
            post(ws_process_investigate),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat",
            get(cockpit_chat_load).post(cockpit_chat_send),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions",
            get(cockpit_chat_sessions_list).post(cockpit_chat_sessions_create),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}",
            get(cockpit_chat_session_load).delete(cockpit_chat_session_delete),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/rename",
            post(cockpit_chat_session_rename),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/debug",
            get(cockpit_chat_session_debug),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/simulations",
            get(cockpit_chat_session_simulations),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/export",
            get(cockpit_chat_session_export),
        )
        // ── Editable model Workbench (ADR 0011) ──────────────────────────────────────────
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/workbench/baseline",
            get(cockpit_workbench_baseline),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/workbench/validate",
            post(cockpit_workbench_validate),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/workbench/apply-edit",
            post(cockpit_workbench_apply_edit),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/workbench/simulate",
            post(cockpit_workbench_simulate),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/workbench/compare",
            post(cockpit_workbench_compare),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/workbench/review",
            post(cockpit_workbench_review),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/sessions/{id}/trace",
            get(cockpit_chat_session_trace_export),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/traces/import",
            post(cockpit_chat_trace_import)
                .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024)),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/stream",
            post(cockpit_chat_stream),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/stream/live",
            get(cockpit_chat_stream_live),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/reset",
            post(cockpit_chat_reset),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/wrapup",
            post(cockpit_chat_wrapup),
        )
        .route(
            "/api/workspaces/{workspace}/processes/{process}/chat/steer",
            post(cockpit_chat_steer),
        )
        .route("/api/python/status", get(python_status))
        .route(
            "/api/chat-prompts",
            get(chat_prompts_list).post(chat_prompts_upsert),
        )
        .route(
            "/api/chat-prompts/{id}",
            axum::routing::delete(chat_prompts_delete),
        )
        .route("/api/personas", get(personas_list).post(personas_upsert))
        .route("/api/personas/{id}", axum::routing::delete(personas_delete))
        .route("/api/pairings", get(pairings_list).post(pairings_upsert))
        .route("/api/pairings/{id}", axum::routing::delete(pairings_delete))
        .route("/api/tools", get(tools_list).post(tools_upsert))
        .route("/api/tools/import", post(tools_import))
        .route("/api/tools/{id}", axum::routing::delete(tools_delete))
        .route(
            "/api/nano/instances",
            get(nano_instances_list).post(nano_instances_add),
        )
        .route(
            "/api/nano/instances/{id}",
            axum::routing::put(nano_instances_update).delete(nano_instances_delete),
        )
        .route(
            "/api/nano/instances/{id}/select",
            post(nano_instances_select),
        )
        .route(
            "/api/nano/instances/{id}/star",
            post(nano_instances_star),
        )
        .route(
            "/api/nano/instances/{id}/health",
            get(nano_instances_health),
        )
        .route("/assets/bpmn/{file}", get(bpmn_asset))
        .route("/assets/settings.js", get(settings_js))
        .route("/assets/theme.css", get(theme_css))
        .route("/assets/theme.js", get(theme_js))
        .route("/api/settings", get(get_settings).put(put_settings))
        .route("/api/settings/profiles", post(post_profile))
        .route(
            "/api/settings/profiles/{id}",
            axum::routing::put(put_profile).delete(delete_profile),
        )
        .route("/api/settings/models", post(post_models))
        .route("/api/llama/status", get(llama_status))
        .route("/api/llama/ready", get(llama_ready))
        .route("/api/llama/start", post(llama_start))
        .route("/api/llama/scan/{profile_id}", get(llama_scan))
        .route("/api/llama/download", post(llama_download))
        .route("/api/llama/download/cancel", post(llama_download_cancel))
        .route("/api/llama/stop", post(llama_stop))
        .route("/api/llama/logs", get(llama_logs))
        .route("/api/llama/reasoning-control", get(reasoning_control_status))
        .route("/api/system/dependencies", get(system_dependencies))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("ProcessOS could not bind {addr}: {e}"));

    tracing::info!(
        %addr,
        target = %cfg.target_url,
        own = %own_url,
        data_dir = %cfg.data_dir.display(),
        "ProcessOS listening; reading the client's production engine (target) over the public trace/metrics contract; running the pilot loop on its own engine"
    );
    println!("PROCESSOS_PORT={}", cfg.port);

    #[allow(
        clippy::expect_used,
        reason = "top-level server run: a serve error is fatal and unrecoverable — panicking aborts the process, which is the intended behaviour at the binary's entry point"
    )]
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("ProcessOS server error");

    // Tear down the spawned engine (if any) so it doesn't outlive ProcessOS.
    if let Some(engine) = own_engine {
        engine.shutdown().await;
    }
    // Stop all local model sidecars (if any) so llama-server doesn't linger.
    llama_for_shutdown.stop(None).await;
}

/// `processos gen <pack.json> <out-dir>` — generate a synthetic trace corpus.
fn run_cli_generate(args: &[String]) {
    if args.len() < 2 {
        eprintln!("usage: processos gen <pack.json> <out-dir>");
        std::process::exit(2);
    }
    let pack_path = std::path::Path::new(&args[0]);
    let out_dir = std::path::Path::new(&args[1]);
    let (pack, def) = corpus::load_pack(pack_path).unwrap_or_else(|e| {
        eprintln!("load pack failed: {e}");
        std::process::exit(1);
    });
    let summary = corpus::generate(&pack, &def, out_dir).unwrap_or_else(|e| {
        eprintln!("generate failed: {e}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
}

/// `processos infer <dataset-dir> [target-p99-wait-ms]` — run the inference + score.
fn run_cli_infer(args: &[String]) {
    if args.is_empty() {
        eprintln!("usage: processos infer <dataset-dir> [target-p99-wait-ms]");
        std::process::exit(2);
    }
    let dataset_dir = std::path::Path::new(&args[0]);
    let target = args
        .get(1)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(500);
    let inference = corpus::infer(dataset_dir, target).unwrap_or_else(|e| {
        eprintln!("infer failed: {e}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&inference).unwrap_or_default()
    );

    // Score against expected.json if it sits beside the dataset.
    let expected = dataset_dir.join("expected.json");
    if expected.is_file() {
        match corpus::score(&inference, &expected) {
            Ok(s) => println!("SCORE {}", serde_json::to_string(&s).unwrap_or_default()),
            Err(e) => eprintln!("score failed: {e}"),
        }
    }
}

/// `processos import-camunda <records.json|dir> <out-dir> [--no-tier2]` — fold a
/// Camunda 8 record export (Elasticsearch/Opensearch/debug-log JSON) into a
/// `traces.json` dataset directly loadable by `DatasetSource`.
fn run_cli_import_camunda(args: &[String]) {
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    if positional.len() < 2 {
        eprintln!("usage: processos import-camunda <records.json|dir> <out-dir> [--no-tier2]");
        std::process::exit(2);
    }
    let tier2 = !args.iter().any(|a| a == "--no-tier2");
    let input = std::path::Path::new(positional[0].as_str());
    let out_dir = std::path::Path::new(positional[1].as_str());
    let summary = camunda_import::import(input, out_dir, tier2).unwrap_or_else(|e| {
        eprintln!("import-camunda failed: {e}");
        std::process::exit(1);
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
}

/// `processos emit-gbnf [--out ir.gbnf]` — write the reversible-IR GBNF grammar (ADR 0001) to
/// stdout, or to the given path. Hand this file to a llama.cpp server via
/// `llama-server --grammar-file <ir.gbnf>` (or POST as the `grammar` field on `/completion`) to
/// constrain the sampler so a local model literally cannot emit invalid IR. The grammar is
/// generated from [`ir_spec::ELEMENT_KIND_SPECS`], which the parity test keeps in sync with the
/// engine's `ElementKind` enum — regenerate on each processos release.
fn run_cli_emit_gbnf(args: &[String]) {
    let gbnf = ir_spec::emit_gbnf();
    let out: Option<&String> = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1));
    match out {
        None => {
            print!("{gbnf}");
        }
        Some(path) => {
            if let Err(e) = std::fs::write(path, &gbnf) {
                eprintln!("failed to write GBNF to {path}: {e}");
                std::process::exit(1);
            }
            eprintln!("wrote {} bytes of GBNF to {path}", gbnf.len());
        }
    }
}

/// `processos layout <in.bpmn> <annotations.json> [--out out.bpmn] [--debug-svg out.svg]`
///
/// Runs the semantic BPMN layout: parses the input model, reads the annotations
/// (see `docs/layout.md` for the schema), lays out with the row-bias solver,
/// and writes both the re-DI'd BPMN and a debug SVG. When `--out`/`--debug-svg`
/// aren't given, sensible defaults derived from the input filename are used.
fn run_cli_layout(args: &[String]) {
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    if positional.len() < 2 {
        eprintln!(
            "usage: processos layout <in.bpmn> <annotations.json> \
             [--out out.bpmn] [--debug-svg out.svg]"
        );
        std::process::exit(2);
    }
    let in_bpmn = std::path::Path::new(positional[0].as_str());
    let in_ann = std::path::Path::new(positional[1].as_str());
    let opt = |flag: &str| -> Option<String> {
        let mut it = args.iter();
        while let Some(a) = it.next() {
            if a == flag {
                return it.next().cloned();
            }
        }
        None
    };
    let stem = in_bpmn
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("out");
    let out_bpmn = opt("--out").unwrap_or_else(|| format!("{stem}.laid-out.bpmn"));
    let out_svg = opt("--debug-svg").unwrap_or_else(|| format!("{stem}.debug.svg"));
    let side_by_side_path = opt("--side-by-side");
    let solver = opt("--solver")
        .as_deref()
        .map(layout::Solver::parse)
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        })
        .unwrap_or(layout::Solver::RowBias);

    let bpmn_xml = std::fs::read_to_string(in_bpmn).unwrap_or_else(|e| {
        eprintln!("read {}: {e}", in_bpmn.display());
        std::process::exit(1);
    });
    let ann_json = std::fs::read_to_string(in_ann).unwrap_or_else(|e| {
        eprintln!("read {}: {e}", in_ann.display());
        std::process::exit(1);
    });
    let ann: layout::SemanticAnnotations = serde_json::from_str(&ann_json).unwrap_or_else(|e| {
        eprintln!("parse annotations JSON: {e}");
        std::process::exit(1);
    });
    let out = layout::layout_with(&bpmn_xml, &ann, solver).unwrap_or_else(|e| {
        eprintln!("layout failed: {e}");
        std::process::exit(1);
    });
    std::fs::write(&out_bpmn, &out.bpmn_xml).unwrap_or_else(|e| {
        eprintln!("write {out_bpmn}: {e}");
        std::process::exit(1);
    });
    std::fs::write(&out_svg, &out.debug_svg).unwrap_or_else(|e| {
        eprintln!("write {out_svg}: {e}");
        std::process::exit(1);
    });
    println!("wrote {out_bpmn}");
    println!("wrote {out_svg}");
    if let Some(d) = &out.field_diagnostics {
        println!(
            "Fromme sim: {steps} steps, KE={ke:.2}, converged={conv}, pinned={pinned}",
            steps = d.steps,
            ke = d.final_kinetic_energy,
            conv = d.converged,
            pinned = d.pinned_by_oscillation.len(),
        );
    }
    if let Some(path) = side_by_side_path {
        let sxs = layout::layout_side_by_side(&bpmn_xml, &ann).unwrap_or_else(|e| {
            eprintln!("side-by-side render failed: {e}");
            std::process::exit(1);
        });
        std::fs::write(&path, &sxs).unwrap_or_else(|e| {
            eprintln!("write {path}: {e}");
            std::process::exit(1);
        });
        println!("wrote {path} (side-by-side)");
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Debug, Deserialize)]
struct InsightsQuery {
    /// How many recent trace summaries to scan for totals.
    limit: Option<usize>,
    /// How many of those to pull full detail for (per-element aggregation).
    sample: Option<usize>,
}

/// `GET /api/insights` — the folded performance report from the **active** Nano instance.
/// On a read failure we return 502 with the underlying error AND the instance's base URL,
/// so the console can show an informative "no instance available at …" message rather than
/// a bare error.
async fn insights(
    State(state): State<AppState>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    let target = state.active_target();
    match report::build(&target, limit, sample).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e, "baseUrl": target.base_url(), "unreachable": true })),
        )
            .into_response(),
    }
}

/// The standalone marketing landing page for Nano Process OS (self-contained:
/// inline canvas particle field, no external assets). Served at `/`.
const LANDING_HTML: &str = include_str!("landing.html");

/// `POST /api/layout` — re-lay-out a BPMN document with the semantic solver of
/// choice. Powers the chat model card's renderer drop-down so authors can
/// eyeball how a proposed model looks under the ELK client-side layout vs. the
/// server-side row-bias or Fromme (field) semantic layouts.
///
/// Slice 15 additions: the response now includes a `conformance` score
/// (weighted mean of 8 BPMN best-practice rules — see
/// [`layout::conformance`]) and `gates` block (coverage + overlap), and
/// the request accepts `polish: bool` to run the deterministic polish pass
/// (grid-snap + gateway centring + happy-path align, non-regressing).
/// The solver call goes through [`layout::guarded::run`] so a panicking
/// experimental solver falls back to RowBias instead of 500-ing.
///
/// Request body:
/// ```json
/// { "xml": "<bpmn:definitions>…</bpmn:definitions>",
///   "solver": "rowbias" | "field",
///   "annotations": { … optional SemanticAnnotations … },
///   "colorize": false,
///   "polish": false }
/// ```
/// Response body:
/// ```json
/// { "bpmn_xml": "<bpmn:definitions>… with fresh DI …</bpmn:definitions>",
///   "debug_svg": "<svg …/>",
///   "solver": "rowbias",
///   "used_solver": "rowbias",
///   "fallback": null,
///   "conformance": { "score": 0.81, "rules": [ … ] },
///   "gates": { "coverage": 1.0, "overlap_ratio": 0.0, "passed": true, … },
///   "polish": { "moves": 3, "aligns": 2, "reverted": false, "candidate": 0 } }
/// ```
/// Errors return 400 with `{ "error": "…" }`.
#[derive(Deserialize)]
struct LayoutRequest {
    xml: String,
    solver: String,
    #[serde(default)]
    annotations: Option<layout::SemanticAnnotations>,
    /// Slice 13: when true, run [`colorize::colorize_flows`] on the
    /// returned bpmn_xml so flow-membership becomes visible as light
    /// pastel fills on the shapes. Rendering-only — the stored model
    /// on disk is never touched.
    #[serde(default)]
    colorize: bool,
    /// Slice 15: when true, run [`layout::polish::polish`] on the solver
    /// output. Non-regressing — if the polish pass would lower conformance
    /// or raise overlap, the original layout is shipped and
    /// `polish.reverted = true` in the response.
    #[serde(default)]
    polish: bool,
}

async fn layout_relayout(Json(req): Json<LayoutRequest>) -> impl IntoResponse {
    let solver = match layout::Solver::parse(&req.solver) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let structural = layout::annotate::infer_from_xml(&req.xml);
    let ann = layout::annotate::merge(req.annotations, structural);
    let declared = layout::guarded::declared_shape_count(&req.xml);
    let guarded = match layout::guarded::run(&req.xml, &ann, solver, declared) {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let (polished_xml, polish_meta) = if req.polish {
        let (px, pm) = layout::polish::polish(&guarded.output.bpmn_xml, &ann);
        (px, Some(pm))
    } else {
        (guarded.output.bpmn_xml, None)
    };
    let bpmn_xml = if req.colorize {
        colorize::colorize_flows(&polished_xml, &ann)
    } else {
        polished_xml
    };
    // Score the final laid-out geometry (post-polish, pre-colorize is fine
    // — colorize only adds attributes, doesn't move things).
    let geom = layout::geom::parse(&bpmn_xml);
    let conf = layout::conformance::score(&geom, &ann);
    let gates = layout::gates::evaluate(&geom, declared);
    let used_solver_name = match guarded.used_solver {
        layout::Solver::RowBias => "rowbias",
        layout::Solver::Field => "field",
    };
    Json(serde_json::json!({
        "bpmn_xml": bpmn_xml,
        "debug_svg": guarded.output.debug_svg,
        "solver": req.solver,
        "used_solver": used_solver_name,
        "fallback": guarded.fallback,
        "annotations_used": ann,
        "colorized": req.colorize,
        "conformance": conf,
        "gates": gates,
        "polish": polish_meta,
    }))
    .into_response()
}

/// `POST /api/layout/score` — score an already-laid-out BPMN document
/// against the 8-rule BPMN best-practices conformance metric plus the
/// coverage / overlap correctness gates. Does not modify the input.
///
/// Slice 15: exposes [`layout::conformance::score`] + [`layout::gates::evaluate`]
/// so callers can objectively compare renderer outputs (RowBias vs Field vs
/// bpmn-js ELK vs the authored DI) without eyeballing them. Annotations
/// are optional — when omitted, the structural pass infers the happy path
/// so `happyPathStraight` is still meaningful.
///
/// Request body:
/// ```json
/// { "xml": "<bpmn:definitions>… with DI …</bpmn:definitions>",
///   "annotations": { … optional SemanticAnnotations … } }
/// ```
/// Response body:
/// ```json
/// { "conformance": { "score": 0.81, "rules": [
///     { "key": "flowDirection", "label": "Left-to-right flow",
///       "weight": 0.22, "score": 1.0,
///       "source": "Silver M&S; Camunda; Effinger" }, … ] },
///   "gates": { "coverage": 1.0, "overlap_ratio": 0.0, "passed": true,
///              "penalty": 1.0, "failures": [] },
///   "declared_shapes": 5, "drawn_shapes": 5 }
/// ```
#[derive(Deserialize)]
struct LayoutScoreRequest {
    xml: String,
    #[serde(default)]
    annotations: Option<layout::SemanticAnnotations>,
}

/// `GET /api/ir/grammar.gbnf` — serve the checked-in reversible-IR GBNF as `text/plain`.
///
/// This is the runtime source of truth for consumers that want to hand the grammar to a
/// llama.cpp sidecar over HTTP (rather than a local `--grammar-file` path). The body is the
/// committed [`processos/assets/ir.gbnf`] via [`ir_spec::ir_gbnf`], kept in sync with the
/// emitter by the `checked_in_grammar_matches_emitter` parity test.
async fn ir_grammar_gbnf() -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        ir_spec::ir_gbnf(),
    )
}

async fn layout_score(Json(req): Json<LayoutScoreRequest>) -> impl IntoResponse {
    if req.xml.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "xml is required" })),
        )
            .into_response();
    }
    let structural = layout::annotate::infer_from_xml(&req.xml);
    let ann = layout::annotate::merge(req.annotations, structural);
    let declared = layout::guarded::declared_shape_count(&req.xml);
    let geom = layout::geom::parse(&req.xml);
    let conf = layout::conformance::score(&geom, &ann);
    let gates = layout::gates::evaluate(&geom, declared);
    Json(serde_json::json!({
        "conformance": conf,
        "gates": gates,
        "declared_shapes": declared,
        "drawn_shapes": geom.nodes.len(),
    }))
    .into_response()
}

/// `POST /api/colorize` — apply the flow colour pass to a BPMN XML
/// document without changing its layout. Powers the "As authored
/// (coloured)" renderer option in the Semantics Workbench: the model
/// keeps its original DI (positions and edges as authored), and shapes
/// get a light pastel fill based on flow membership.
///
/// Rendering-only — the stored `model.bpmn` on disk is never mutated
/// (the workbench holds the input in memory and this endpoint returns a
/// fresh string to hand to bpmn-js).
///
/// Request body:
/// ```json
/// { "xml": "<bpmn:definitions>…</bpmn:definitions>",
///   "annotations": { "flows": [ … ] } }
/// ```
///
/// Response body:
/// ```json
/// { "bpmn_xml": "<bpmn:definitions …>… with color:background-color …</bpmn:definitions>",
///   "colorized": true }
/// ```
#[derive(Deserialize)]
struct ColorizeRequest {
    xml: String,
    #[serde(default)]
    annotations: Option<layout::SemanticAnnotations>,
}

async fn colorize_flows_handler(Json(req): Json<ColorizeRequest>) -> impl IntoResponse {
    if req.xml.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "xml is required" })),
        )
            .into_response();
    }
    // Merge caller annotations with the structural inference so a
    // caller who only passes flows still gets clusters/roles inferred
    // (parity with /api/layout).
    let structural = layout::annotate::infer_from_xml(&req.xml);
    let ann = layout::annotate::merge(req.annotations, structural);
    let bpmn_xml = colorize::colorize_flows(&req.xml, &ann);
    Json(serde_json::json!({
        "bpmn_xml": bpmn_xml,
        "colorized": true,
    }))
    .into_response()
}

/// `POST /api/curator/propose` — the Semantics Workbench's LLM assistant for
/// STRUCTURAL semantic annotations (flows, clusters, roles).
///
/// The Curator is a one-shot LLM call — no tool loop, no dataset binding —
/// that reads the BPMN XML and emits ONE JSON object for the requested axis.
/// Cost and time proposals are out of scope by design (numbers need real
/// telemetry, not LLM guesses); [`curator::parse_proposal`] strips them if
/// the LLM leaks them.
///
/// Request body:
/// ```json
/// { "xml": "<bpmn:definitions>…</bpmn:definitions>",
///   "axis": "flows" | "clusters" | "roles",
///   "currentAnnotations": { … optional SemanticAnnotations … },
///   "chatHistory": [{"role":"user"|"assistant","content":"…"}],
///   "userMessage": "make the happy flow shorter",
///   "llm": { … optional LlmOverride … } }
/// ```
///
/// Response body:
/// ```json
/// { "proposal": { "flows": [ … ] },      // exactly the requested axis subset
///   "assistantText": "{\"flows\": …}",   // raw LLM reply, for the chat log
///   "chatHistory": [ … history with new user+assistant turns appended … ] }
/// ```
/// Errors return 400 with `{ "error": "…" }`.
const CURATOR_PERSONA_ID: &str = "semantics-curator";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CuratorProposeRequest {
    xml: String,
    axis: String,
    #[serde(default)]
    current_annotations: Option<layout::SemanticAnnotations>,
    #[serde(default)]
    chat_history: Vec<curator::ChatTurn>,
    #[serde(default)]
    user_message: Option<String>,
    #[serde(default)]
    llm: Option<LlmOverride>,
}

async fn curator_propose(
    State(state): State<AppState>,
    Json(req): Json<CuratorProposeRequest>,
) -> impl IntoResponse {
    let axis = match curator::Axis::parse(&req.axis) {
        Ok(a) => a,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    if req.xml.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "xml is required" })),
        )
            .into_response();
    }

    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured (set PROCESSOS_LLM_MODEL, pick a profile, \
                          or pass llm.model in the request body)"
            })),
        )
            .into_response();
    }

    let persona = match state.personas.get(CURATOR_PERSONA_ID) {
        Some(p) => p,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!(
                        "curator persona {CURATOR_PERSONA_ID:?} missing from persona library"
                    )
                })),
            )
                .into_response();
        }
    };

    let current = req.current_annotations.unwrap_or_default();
    let user_prompt = curator::build_user_prompt(
        axis,
        &req.xml,
        &current,
        &req.chat_history,
        req.user_message.as_deref(),
    );

    let raw = match llm_complete(&cfg, &persona.system, &user_prompt).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": format!("LLM call failed: {e}") })),
            )
                .into_response();
        }
    };

    let proposal = match curator::parse_proposal(axis, &raw) {
        Ok(p) => p,
        Err(e) => {
            // Include the raw text so the operator can see what the model actually
            // said — usually enough to diagnose ("it wrote prose again", "it emitted
            // clusters instead of flows").
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": e,
                    "assistantText": raw,
                })),
            )
                .into_response();
        }
    };

    // Append this turn to the returned history so the client can persist it
    // for iterative refinement without having to reconstruct the assistant
    // turn from the parsed proposal.
    let mut history = req.chat_history;
    if let Some(msg) = req
        .user_message
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        history.push(curator::ChatTurn {
            role: "user".into(),
            content: msg.to_string(),
        });
    }
    history.push(curator::ChatTurn {
        role: "assistant".into(),
        content: raw.clone(),
    });

    Json(serde_json::json!({
        "proposal": proposal,
        "assistantText": raw,
        "chatHistory": history,
    }))
    .into_response()
}

/// `POST /api/curator/propose/stream` — streaming variant of
/// [`curator_propose`]. Same request body, but returns
/// `text/event-stream` so the workbench can render the model's thinking
/// live (mirroring the Cockpit's chat-stream feedback loop).
///
/// Event types (all payloads are JSON):
/// * `{ "type": "llm", "provider": "openai" | "anthropic", "baseUrl": …, "model": …, "profileName": …, "sidecarReady": true|false }`
///   — emitted once at the start so the UI can show which model is answering.
/// * `{ "type": "reasoning", "text": "…" }` — a chunk of the model's
///   chain-of-thought (only when the endpoint surfaces `reasoning_content`
///   deltas; older servers just produce `answer` chunks).
/// * `{ "type": "answer", "text": "…" }` — a chunk of the model's final
///   JSON reply.
/// * `{ "type": "proposal", "proposal": { … }, "chatHistory": [ … ] }` —
///   the validated per-axis proposal, emitted once the stream closes.
/// * `{ "type": "done" }` — terminal marker.
/// * `{ "type": "error", "message": "…", "assistantText": "…" }` — the
///   request failed (LLM error, unparseable proposal, misconfigured
///   endpoint). The raw model reply is included when available so the
///   operator can see what the model actually said.
async fn curator_propose_stream(
    State(state): State<AppState>,
    Json(req): Json<CuratorProposeRequest>,
) -> impl IntoResponse {
    // Validate the axis + xml up front — an early 400 is nicer than a
    // one-line SSE stream that just carries an error event.
    let axis = match curator::Axis::parse(&req.axis) {
        Ok(a) => a,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    if req.xml.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "xml is required" })),
        )
            .into_response();
    }
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured (set PROCESSOS_LLM_MODEL, pick a profile, \
                          or pass llm.model in the request body)"
            })),
        )
            .into_response();
    }
    let persona = match state.personas.get(CURATOR_PERSONA_ID) {
        Some(p) => p,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!(
                        "curator persona {CURATOR_PERSONA_ID:?} missing from persona library"
                    )
                })),
            )
                .into_response();
        }
    };

    // Look up the active profile's *name* so the UI can show "gemma-4-local
    // (llama.cpp @ …/v1)" instead of just the model file. Also check
    // whether that profile's sidecar (if any) is ready to answer — the
    // Curator was hanging silently for the operator whose sidecar had been
    // started but hadn't yet finished loading.
    let snap = state.settings.snapshot();
    let active_profile = snap.active();
    let profile_id = active_profile.map(|p| p.id.clone());
    let profile_name = active_profile.map(|p| p.name.clone());
    let profile_is_sidecar = active_profile.is_some_and(|p| p.sidecar);
    let sidecar_ready = if profile_is_sidecar {
        profile_id
            .as_deref()
            .map(|id| {
                let s = state.llama.status_of(id);
                s.running && s.port.is_some() && s.error.is_none()
            })
            .unwrap_or(false)
    } else {
        // Non-sidecar profile (external endpoint) — nothing to ready-check;
        // report `true` so the UI doesn't display a spurious warning.
        true
    };

    let live = LiveTurn::new(String::new());
    let live_tx = live.clone();

    // Emit the LLM info header event immediately so the pane can render
    // "answering with …" before the first reasoning chunk arrives (some
    // models take several seconds before their first token).
    live_tx.emit(serde_json::json!({
        "type": "llm",
        "provider": cfg.provider.as_str(),
        "baseUrl": cfg.base_url,
        "model": cfg.model,
        "profileId": profile_id,
        "profileName": profile_name,
        "sidecar": profile_is_sidecar,
        "sidecarReady": sidecar_ready,
    }));

    let current = req.current_annotations.unwrap_or_default();
    let user_prompt = curator::build_user_prompt(
        axis,
        &req.xml,
        &current,
        &req.chat_history,
        req.user_message.as_deref(),
    );
    let system = persona.system.clone();
    let history_in = req.chat_history;
    let user_message = req.user_message;
    let cfg_for_task = cfg.clone();

    tokio::spawn(async move {
        // Fall back to a single non-streaming call on Anthropic (which
        // [`llm_complete_streaming`] rejects); the header event has already
        // been sent, so the operator still sees which model was used.
        let streamed = if cfg_for_task.provider == harness::Provider::Openai {
            let tx_r = live_tx.clone();
            let tx_a = live_tx.clone();
            let on_r = move |chunk: &str| {
                tx_r.emit(serde_json::json!({ "type": "reasoning", "text": chunk }));
            };
            let on_a = move |chunk: &str| {
                tx_a.emit(serde_json::json!({ "type": "answer", "text": chunk }));
            };
            llm_complete_streaming(&cfg_for_task, &system, &user_prompt, on_r, on_a).await
        } else {
            match llm_complete(&cfg_for_task, &system, &user_prompt).await {
                Ok(text) => {
                    live_tx.emit(serde_json::json!({ "type": "answer", "text": text.clone() }));
                    Ok(harness::StreamedCompletion {
                        reasoning: String::new(),
                        content: text,
                    })
                }
                Err(e) => Err(e),
            }
        };

        let raw = match streamed {
            Ok(sc) => sc.into_folded(),
            Err(e) => {
                live_tx.emit(serde_json::json!({
                    "type": "error",
                    "message": format!("LLM call failed: {e}"),
                }));
                return;
            }
        };

        // Parse the proposal on the same side as the one-shot endpoint.
        // Include the raw text on parse failure so the operator can see
        // what the model wrote and correct with a refinement (same
        // contract as /api/curator/propose).
        let proposal = match curator::parse_proposal(axis, &raw) {
            Ok(p) => p,
            Err(e) => {
                live_tx.emit(serde_json::json!({
                    "type": "error",
                    "message": e,
                    "assistantText": raw,
                }));
                return;
            }
        };

        let mut history = history_in;
        if let Some(msg) = user_message
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            history.push(curator::ChatTurn {
                role: "user".into(),
                content: msg.to_string(),
            });
        }
        history.push(curator::ChatTurn {
            role: "assistant".into(),
            content: raw.clone(),
        });

        live_tx.emit(serde_json::json!({
            "type": "proposal",
            "proposal": proposal,
            "assistantText": raw,
            "chatHistory": history,
        }));
        live_tx.emit(serde_json::json!({ "type": "done" }));
    });

    sse_from_live(live).into_response()
}

/// The features page, leading with the two flagship capabilities. Served at
/// `/features`.
const FEATURES_HTML: &str = include_str!("features.html");

/// `GET /` — the Nano Process OS landing page.
async fn landing() -> Html<&'static str> {
    Html(LANDING_HTML)
}

/// `GET /features` — what Process OS does.
async fn features() -> Html<&'static str> {
    Html(FEATURES_HTML)
}

/// One page of the User Guide, single-sourced from `README.md` at build time.
/// See `build.rs`; the generated array below is embedded with `include!`.
pub struct GuidePage {
    pub slug: &'static str,
    pub title: &'static str,
    pub body: &'static str,
}
include!(concat!(env!("OUT_DIR"), "/userguide.rs"));

/// Sidebar nav for the guide shell — one link per generated page, the active one
/// highlighted.
fn guide_nav(active: &str) -> String {
    USERGUIDE_PAGES
        .iter()
        .map(|p| {
            let href = if p.slug == "index" {
                "/guide".to_string()
            } else {
                format!("/guide/{}", p.slug)
            };
            let cls = if p.slug == active {
                "g-link active"
            } else {
                "g-link"
            };
            format!(
                "<a class=\"{cls}\" href=\"{href}\">{}</a>",
                html_escape(p.title)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Wrap a rendered guide page in the branded ProcessOS shell (light/dark/system
/// via the `--po-*` design tokens in /assets/theme.css) with a sticky sidebar.
fn guide_shell(page: &GuidePage) -> String {
    let subtitle = if page.slug == "index" {
        "User Guide".to_string()
    } else {
        html_escape(page.title)
    };
    format!(
        r###"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Nano ProcessOS · {title}</title>
<link rel="stylesheet" href="/assets/theme.css">
<script src="/assets/theme.js"></script>
<style>
  /* All colours resolve through the --po-* design tokens (/assets/theme.css); the
     light/dark/system choice is applied by /assets/theme.js before first paint. */
  * {{ box-sizing:border-box; }}
  html {{ scroll-behavior:smooth; }}
  body {{ margin:0; background:var(--po-app); color:var(--po-text);
    font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; line-height:1.65; }}
  a {{ color:var(--po-accent-2); }}
  code {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:0.86em; }}
  .g-bar {{ display:flex; align-items:center; gap:.7rem; padding:.6rem 1.1rem; background:var(--po-panel);
    border-bottom:1px solid var(--po-edge); position:sticky; top:0; z-index:10; }}
  .g-bar a.brand {{ display:flex; align-items:center; gap:.5rem; text-decoration:none; color:var(--po-text); font-weight:600; }}
  .g-bar .dot {{ width:.6rem; height:.6rem; border-radius:9999px; background:linear-gradient(135deg,var(--po-accent),var(--po-accent-2)); }}
  .g-bar .sub {{ color:var(--po-text-muted); font-size:.85rem; }}
  .g-bar .spacer {{ flex:1; }}
  .g-bar a.x {{ color:var(--po-accent-2); text-decoration:none; font-size:.85rem; margin-left:1rem; }}
  .g-bar a.x:hover {{ text-decoration:underline; }}
  .layout {{ display:flex; align-items:flex-start; max-width:1180px; margin:0 auto; }}
  .sidebar {{ width:255px; flex:0 0 255px; position:sticky; top:50px; align-self:flex-start;
    max-height:calc(100vh - 50px); overflow-y:auto; padding:1.25rem .75rem 3rem; border-right:1px solid var(--po-edge); }}
  .nav-title {{ font-size:.72rem; text-transform:uppercase; letter-spacing:.06em; color:var(--po-text-muted);
    font-weight:700; padding:0 .6rem; margin-bottom:.5rem; }}
  .g-link {{ display:block; padding:.34rem .6rem; border-radius:6px; text-decoration:none; color:var(--po-text-muted); font-size:.9rem; }}
  .g-link:hover {{ background:var(--po-hover); color:var(--po-text); }}
  .g-link.active {{ background:var(--po-accent-wash); color:var(--po-accent-soft); font-weight:600; }}
  .content {{ min-width:0; flex:1; padding:1.6rem 2.2rem 5rem; }}
  .content h1 {{ font-size:1.9rem; margin:.2rem 0 1rem; }}
  .content h2 {{ font-size:1.4rem; margin:2rem 0 .6rem; padding-bottom:.3rem; border-bottom:1px solid var(--po-edge); }}
  .content h3 {{ font-size:1.12rem; margin:1.5rem 0 .4rem; color:var(--po-accent-soft); }}
  .content h4 {{ font-size:1rem; margin:1.1rem 0 .3rem; }}
  .content p, .content li {{ color:var(--po-text); }}
  .content a {{ text-decoration:none; }}
  .content a:hover {{ text-decoration:underline; }}
  .content ul, .content ol {{ padding-left:1.4rem; }}
  .content li {{ margin:.25rem 0; }}
  .content :not(pre) > code {{ background:var(--po-inset); border:1px solid var(--po-edge); border-radius:4px;
    padding:.05rem .32rem; color:var(--po-accent-soft); }}
  .content pre {{ background:var(--po-inset); color:var(--po-text); padding:.9rem 1rem; border-radius:8px; overflow-x:auto;
    font-size:.84rem; line-height:1.5; border:1px solid var(--po-edge); }}
  .content pre code {{ background:none; border:0; padding:0; color:inherit; }}
  .content blockquote {{ margin:1rem 0; padding:.3rem 1rem; border-left:3px solid var(--po-accent); background:var(--po-panel); color:var(--po-text-muted); }}
  .content table {{ border-collapse:collapse; width:100%; margin:1rem 0; font-size:.9rem; display:block; overflow-x:auto; }}
  .content th, .content td {{ border:1px solid var(--po-edge); padding:.4rem .6rem; text-align:left; vertical-align:top; }}
  .content th {{ background:var(--po-raised); }}
  .content img {{ max-width:100%; }}
  .content hr {{ border:0; border-top:1px solid var(--po-edge); margin:2rem 0; }}
  .source-note {{ margin-top:3rem; padding-top:1rem; border-top:1px solid var(--po-edge); color:var(--po-text-muted); font-size:.82rem; }}
  @media (max-width:800px) {{
    .layout {{ flex-direction:column; }}
    .sidebar {{ position:static; width:100%; max-height:none; border-right:0; border-bottom:1px solid var(--po-edge); }}
    .content {{ padding:1.25rem; }}
  }}
</style>
</head>
<body>
  <div class="g-bar">
    <a class="brand" href="/"><span class="dot"></span>Nano ProcessOS</a>
    <span class="sub">{subtitle}</span>
    <span class="spacer"></span>
    <a class="x" href="/features">Features</a>
    <a class="x" href="/workspace">Workspaces</a>
    <a class="x" href="/cockpit">Cockpit</a>
    <span id="gTheme" style="margin-left:1rem"></span>
  </div>
  <div class="layout">
    <nav class="sidebar">
      <div class="nav-title">User Guide</div>
      {nav}
    </nav>
    <main class="content">
      {body}
    </main>
  </div>
<script>if(window.poMountThemeToggle)poMountThemeToggle(document.getElementById('gTheme'))</script>
</body>
</html>"###,
        title = html_escape(page.title),
        subtitle = subtitle,
        nav = guide_nav(page.slug),
        body = page.body,
    )
}

/// `GET /guide` — the User Guide home (Overview), single-sourced from the README.
async fn guide_index() -> impl IntoResponse {
    serve_guide("index")
}

/// `GET /guide/{slug}` — a single User Guide section.
async fn guide_section(Path(slug): Path<String>) -> impl IntoResponse {
    serve_guide(&slug)
}

fn serve_guide(slug: &str) -> axum::response::Response {
    match USERGUIDE_PAGES.iter().find(|p| p.slug == slug) {
        Some(p) => Html(guide_shell(p)).into_response(),
        None => {
            // Unknown section — fall back to the Overview so a stale link still lands
            // somewhere useful, with a 404 status for correctness.
            match USERGUIDE_PAGES.iter().find(|p| p.slug == "index") {
                Some(overview) => {
                    (StatusCode::NOT_FOUND, Html(guide_shell(overview))).into_response()
                }
                None => (StatusCode::NOT_FOUND, Html(String::new())).into_response(),
            }
        }
    }
}

/// Minimal HTML escape for text interpolated into the guide shell (titles).
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// A dependency-free single-file dashboard that fetches `/api/insights` and renders
/// the report. Served at `/console`. Intentionally tiny — the richer UX is the console
/// "Optimization" tab that proxies to this server (see docs/processos-design.md §4).
async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

// --- The cockpit — Console → Process → Experiment (design §7.8 / §10) ------------

/// `GET /cockpit` — the single-file cockpit app (left rail, process drilldown,
/// the four-step experiment stepper, and the persistent droid-conversation pane).
async fn cockpit_page() -> impl IntoResponse {
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (axum::http::header::CACHE_CONTROL, NO_CACHE),
        ],
        COCKPIT_HTML,
    )
}

/// `GET /api/cockpit/overview` — target-process cards + the experiments list.
async fn cockpit_overview(
    State(state): State<AppState>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    let target = state.active_target();
    match cockpit::overview(&target, &state.own, limit, sample).await {
        Ok(o) => Json(o).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/advisor` — the worker-scaling advisory for the active live instance.
///
/// Scrapes the instance's raw Prometheus `/metrics`, diffs it against the previous
/// scrape (kept per base URL in [`AppState::advisor_prev`]) and returns, per job
/// type, whether adding workers would raise throughput — and refuses to recommend
/// scaling when the single-writer server is the real bottleneck. Advisory only; it
/// never actuates. Poll it ~1–2 Hz from the cockpit to build the rate window.
async fn advisor_advise(State(state): State<AppState>) -> impl IntoResponse {
    let target = state.active_target();
    let key = target.base_url().to_string();
    let text = match target.get_text("/metrics").await {
        Ok(t) => t,
        Err(e) => return bad_gateway(format!("scrape {key}/metrics: {e}")),
    };
    let cur = advisor::parse_snapshot(&text, chat_now_ms());
    // Diff against the previous scrape for this instance, then remember this one.
    let prev = {
        let mut map = state.advisor_prev.lock().unwrap_or_else(|e| e.into_inner());
        let prev = map.get(&key).cloned().unwrap_or_default();
        map.insert(key, cur.clone());
        prev
    };
    Json(advisor::advise(&prev, &cur)).into_response()
}

async fn cockpit_experiments(
    State(state): State<AppState>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    match cockpit::list_experiments(&state.own, limit).await {
        Ok(xs) => Json(xs).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/cockpit/experiments/{key}` — the full cockpit view of one experiment.
async fn cockpit_experiment(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    match cockpit::experiment_detail(&state.own, &key).await {
        Ok(mut d) => {
            // Prefer the durable dialogue: once an experiment has a persisted log
            // (engine framing at creation, a droid turn per round, the pilot's
            // notes/decisions), that log is the true running history. The
            // reconstructed snapshot remains the fallback for legacy experiments
            // created before conversations were persisted.
            let log = state.conversations.read(&key);
            if !log.is_empty() {
                d.conversation = log
                    .into_iter()
                    .map(|m| cockpit::Turn {
                        role: m.role,
                        text: m.text,
                    })
                    .collect();
            }
            Json(d).into_response()
        }
        Err(e) => bad_gateway(e),
    }
}

/// `POST /api/cockpit/experiments` — start an experiment for a target process. If
/// no `baselineModel` is supplied, the latest deployed BPMN for `processId` is used.
async fn cockpit_create(
    State(state): State<AppState>,
    Json(req): Json<CockpitCreateRequest>,
) -> impl IntoResponse {
    let baseline = match req.baseline_model {
        Some(b) if !b.trim().is_empty() => b,
        _ => match cockpit::latest_process_xml(&state.active_target(), &req.process_id).await {
            Ok(xml) => xml,
            Err(e) => return bad_gateway(e),
        },
    };
    let max_iterations = req.max_iterations.unwrap_or(2).clamp(1, 50);
    match cockpit::create_experiment(
        &state.own,
        &req.process_id,
        baseline,
        max_iterations,
        req.prompt_id.clone(),
    )
    .await
    {
        Ok(key) => {
            // Seed the durable conversation with the engine's framing turn so the log
            // is a complete dialogue from the first render, not just from round 1.
            let prompt_note = req
                .prompt_id
                .as_deref()
                .map(|p| format!(" Droid prompt: {p}."))
                .unwrap_or_default();
            state.conversations.append(
                &key,
                "engine",
                &format!(
                    "Experiment forked from “{}”. The recorded production dataset is the fitness data; the engine replays every candidate against it.{prompt_note}",
                    req.process_id
                ),
                Some(0),
            );
            Json(serde_json::json!({ "instanceKey": key })).into_response()
        }
        Err(e) => bad_gateway(e),
    }
}

/// `POST /api/cockpit/experiments/{key}/decision` — the pilot's turn at `Review`.
async fn cockpit_decision(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(req): Json<CockpitDecisionRequest>,
) -> impl IntoResponse {
    let decision = req.decision.trim();
    if !matches!(decision, "accept" | "iterate" | "stop") {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "decision must be accept | iterate | stop" })),
        )
            .into_response();
    }
    let note = req.note.as_deref().map(str::trim).filter(|s| !s.is_empty());
    // Persist the pilot's turn (decision + any free-text guidance) before acting, so
    // the dialogue is durable even if the engine call below fails.
    let pilot_text = match note {
        Some(n) => format!("[{decision}] {n}"),
        None => format!("[{decision}]"),
    };
    state.conversations.append(&key, "pilot", &pilot_text, None);
    match cockpit::submit_decision(&state.own, &key, decision, note).await {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/cockpit/experiments/{key}/conversation` — the persisted pilot ↔ droid log.
async fn cockpit_conversation(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    Json(state.conversations.read(&key))
}

/// `POST /api/cockpit/experiments/{key}/conversation` — append a turn to the
/// persisted conversation. Used by the pilot to chat with the droid out of band, and
/// by the worker to record the droid's per-round result. Role defaults to `pilot`.
async fn cockpit_message(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(req): Json<CockpitMessageRequest>,
) -> impl IntoResponse {
    let text = req.text.trim();
    if text.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "message text must not be empty" })),
        )
            .into_response();
    }
    let role = match req.role.as_deref().map(str::trim) {
        Some("droid") => "droid",
        Some("engine") => "engine",
        _ => "pilot",
    };
    let msg = state.conversations.append(&key, role, text, req.round);
    (StatusCode::CREATED, Json(msg)).into_response()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CockpitCreateRequest {
    process_id: String,
    #[serde(default)]
    baseline_model: Option<String>,
    #[serde(default)]
    max_iterations: Option<i64>,
    /// Library prompt id to drive the droid (the experimental variable, §7.8 step 2).
    #[serde(default)]
    prompt_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CockpitDecisionRequest {
    decision: String,
    /// Optional free-text guidance from the pilot for the next round — persisted to
    /// the conversation and threaded into the BPMN as `pilotNote` so the droid sees it.
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CockpitMessageRequest {
    text: String,
    /// `pilot` (default), `droid`, or `engine`.
    #[serde(default)]
    role: Option<String>,
    /// The optimization round this turn belongs to, when known.
    #[serde(default)]
    round: Option<i64>,
}

fn bad_gateway(e: String) -> axum::response::Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": e })),
    )
        .into_response()
}

fn unprocessable(e: String) -> axum::response::Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(serde_json::json!({ "error": e })),
    )
        .into_response()
}

// --- Workspaces (customer/deployment bounded contexts) > processes (the consultant's folder tree) -----------

/// The single-file workspace browser. Served at `/workspace`.
const WORKSPACE_HTML: &str = include_str!("workspace.html");
const SEMANTICS_HTML: &str = include_str!("semantics.html");

/// Vendored bpmn-js viewer assets (self-contained, served at `/assets/bpmn/*`), so
/// the workspace console can render a real BPMN diagram with no CDN/build step.
const BPMN_VIEWER_JS: &str = include_str!("../assets/bpmn/bpmn-navigated-viewer.js");
/// The full bpmn-js **Modeler** (palette, context pad, structured editing), embedded so the
/// cockpit's editable-model Workbench (ADR 0011) is self-contained like the read-only viewer.
const BPMN_MODELER_JS: &str = include_str!("../assets/bpmn/bpmn-modeler.production.min.js");
const BPMN_AUTO_LAYOUT_JS: &str = include_str!("../assets/bpmn/bpmn-auto-layout.js");
const BPMN_ELK_JS: &str = include_str!("../assets/bpmn/elk.bundled.js");
const BPMN_DIAGRAM_CSS: &str = include_str!("../assets/bpmn/diagram-js.css");
const BPMN_EMBEDDED_CSS: &str = include_str!("../assets/bpmn/bpmn-embedded.css");
/// Zeebe moddle descriptor, so the Workbench's bpmn-js Modeler binds `zeebe:` extension elements
/// (e.g. `zeebe:taskDefinition`) and preserves them across an edit instead of dropping them.
const BPMN_ZEEBE_MODDLE_JSON: &str = include_str!("../assets/bpmn/zeebe-moddle.json");
/// Nano semantic-annotation moddle descriptor (ADR 0002 slice 4). Registers
/// the `nano:*` extension namespace with the Workbench's bpmn-js Modeler so
/// `nano:cost` / `nano:time` / `nano:role` / `nano:flowClass` elements
/// round-trip across a Modeler save/load instead of being dropped.
const BPMN_NANO_MODDLE_JSON: &str = include_str!("../assets/bpmn/nano-moddle.json");
const SETTINGS_JS: &str = include_str!("../assets/settings.js");
const THEME_CSS: &str = include_str!("../assets/theme.css");
const THEME_JS: &str = include_str!("../assets/theme.js");

/// A single embedded file belonging to a demo dataset, addressed by its path
/// relative to the pack directory (so `bpmn_library` directory references and the
/// `bpmn` field resolve exactly as they do on disk).
struct DemoFile {
    rel: &'static str,
    contents: &'static str,
}

/// A bundled corpus pack, embedded so the demo datasets seed from any working
/// directory. Each becomes a selectable workspace+process scenario.
struct DemoDataset {
    /// URL-safe selector used by `POST /api/workspaces/seed-demo {dataset}`.
    id: &'static str,
    /// Default workspace display name.
    workspace: &'static str,
    /// Default process display name.
    process: &'static str,
    /// Objective stamped onto the seeded process.
    objective: &'static str,
    /// One-line scenario blurb shown in the demo picker.
    description: &'static str,
    /// The `pack.json` entry within `files`.
    pack_rel: &'static str,
    /// The primary BPMN entry within `files` (written as the process model).
    model_rel: &'static str,
    /// Every file to materialise before loading the pack.
    files: &'static [DemoFile],
}

/// The bundled demo datasets, embedded so `seed-demo` works from any working
/// directory. The first entry is the default when no `dataset` is requested.
const DEMO_DATASETS: &[DemoDataset] = &[
    DemoDataset {
        id: "loan-approval",
        workspace: "Northwind Bank",
        process: "Loan Approval",
        objective: "keep loan approvals fast and reliable as volume grows",
        description: "Loan origination with a credit-check bottleneck under weekday-morning peaks.",
        pack_rel: "pack.json",
        model_rel: "loan-approval.bpmn",
        files: &[
            DemoFile {
                rel: "pack.json",
                contents: include_str!("../corpus-packs/loan-approval/pack.json"),
            },
            DemoFile {
                rel: "loan-approval.bpmn",
                contents: include_str!("../corpus-packs/loan-approval/loan-approval.bpmn"),
            },
        ],
    },
    DemoDataset {
        id: "cdd-refresh",
        workspace: "Meridian Trust",
        process: "CDD Refresh",
        objective: "complete periodic KYC/CDD refreshes within SLA without overloading analysts",
        description:
            "Multi-phase KYC/CDD refresh orchestrator with an agentic AI investigation stage.",
        pack_rel: "pack.json",
        model_rel: "orchestrator.bpmn",
        files: &[
            DemoFile {
                rel: "pack.json",
                contents: include_str!("../corpus-packs/cdd-refresh/pack.json"),
            },
            DemoFile {
                rel: "orchestrator.bpmn",
                contents: include_str!("../corpus-packs/cdd-refresh/orchestrator.bpmn"),
            },
            DemoFile {
                rel: "phases/process-01-intake.bpmn",
                contents: include_str!("../corpus-packs/cdd-refresh/phases/process-01-intake.bpmn"),
            },
            DemoFile {
                rel: "phases/process-02-document-request.bpmn",
                contents: include_str!(
                    "../corpus-packs/cdd-refresh/phases/process-02-document-request.bpmn"
                ),
            },
            DemoFile {
                rel: "phases/process-03-reminders-and-escalation.bpmn",
                contents: include_str!(
                    "../corpus-packs/cdd-refresh/phases/process-03-reminders-and-escalation.bpmn"
                ),
            },
            DemoFile {
                rel: "phases/process-05-parallel-screening.bpmn",
                contents: include_str!(
                    "../corpus-packs/cdd-refresh/phases/process-05-parallel-screening.bpmn"
                ),
            },
            DemoFile {
                rel: "phases/process-06-sanctions-gate.bpmn",
                contents: include_str!(
                    "../corpus-packs/cdd-refresh/phases/process-06-sanctions-gate.bpmn"
                ),
            },
            DemoFile {
                rel: "phases/process-07-risk-assessment.bpmn",
                contents: include_str!(
                    "../corpus-packs/cdd-refresh/phases/process-07-risk-assessment.bpmn"
                ),
            },
            DemoFile {
                rel: "phases/process-08-approval.bpmn",
                contents: include_str!(
                    "../corpus-packs/cdd-refresh/phases/process-08-approval.bpmn"
                ),
            },
            DemoFile {
                rel: "phases/process-09-closure.bpmn",
                contents: include_str!(
                    "../corpus-packs/cdd-refresh/phases/process-09-closure.bpmn"
                ),
            },
        ],
    },
    DemoDataset {
        id: "amazing-retail",
        workspace: "Amazing Retail",
        process: "Delivery Exception Resolution",
        objective: "resolve delivery exceptions quickly while controlling AI agent cost",
        description:
            "Delivery-exception triage with an under-provisioned AI agent and duplicate-case routing.",
        pack_rel: "pack.json",
        model_rel: "delivery-exception-resolution.bpmn",
        files: &[
            DemoFile {
                rel: "pack.json",
                contents: include_str!("../corpus-packs/amazing-retail/pack.json"),
            },
            DemoFile {
                rel: "delivery-exception-resolution.bpmn",
                contents: include_str!(
                    "../corpus-packs/amazing-retail/delivery-exception-resolution.bpmn"
                ),
            },
        ],
    },
];

fn demo_dataset(id: &str) -> Option<&'static DemoDataset> {
    DEMO_DATASETS.iter().find(|d| d.id == id)
}

/// `GET /workspace` — browse workspaces, drill into a process, view its Insights.
async fn workspace_page() -> Html<&'static str> {
    Html(WORKSPACE_HTML)
}

/// `GET /semantics` — the Semantics Workbench (ADR 0002 slice 6). A standalone
/// route for authoring the annotations sidecar for a single process. The
/// workspace/process are picked either from the URL query (?workspace=…&process=…)
/// or the hash (#/c/<ws>/p/<proc>) so the same page can be linked from the
/// workspace tile as well as the cockpit's rail nav.
async fn semantics_page() -> Html<&'static str> {
    Html(SEMANTICS_HTML)
}

#[derive(Debug, Deserialize)]
struct CreateWorkspaceBody {
    #[serde(default, alias = "displayName", alias = "name")]
    display_name: String,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateProcessBody {
    #[serde(default, alias = "displayName", alias = "name")]
    display_name: String,
    #[serde(default, alias = "targetUrl")]
    target_url: Option<String>,
    #[serde(default)]
    dataset: Option<String>,
    #[serde(default)]
    objective: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

/// `GET /api/workspaces` — every workspace (scanned from disk).
async fn ws_workspaces(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "root": state.workspaces.root().to_string_lossy(),
        "workspaces": state.workspaces.list_workspaces(),
    }))
}

/// `POST /api/workspaces` — create a workspace from a display name.
async fn ws_create_workspace(
    State(state): State<AppState>,
    Json(body): Json<CreateWorkspaceBody>,
) -> impl IntoResponse {
    match state
        .workspaces
        .create_workspace(&body.display_name, body.notes)
    {
        Ok(c) => (StatusCode::CREATED, Json(c)).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/workspaces/{workspace}` — a workspace + its processes.
async fn ws_workspace(
    State(state): State<AppState>,
    Path(workspace): Path<String>,
) -> impl IntoResponse {
    match state.workspaces.get_workspace(&workspace) {
        Some(c) => Json(serde_json::json!({
            "workspace": c,
            "processes": state.workspaces.list_processes(&workspace),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such workspace: {workspace}") })),
        )
            .into_response(),
    }
}

/// `POST /api/workspaces/{workspace}/processes` — create a process.
async fn ws_create_process(
    State(state): State<AppState>,
    Path(workspace): Path<String>,
    Json(body): Json<CreateProcessBody>,
) -> impl IntoResponse {
    let config = workspace::ProcessConfig {
        display_name: body.display_name.clone(),
        target_url: body.target_url.filter(|s| !s.trim().is_empty()),
        dataset: body.dataset.filter(|s| !s.trim().is_empty()),
        objective: body.objective.filter(|s| !s.trim().is_empty()),
        notes: body.notes.filter(|s| !s.trim().is_empty()),
    };
    match state
        .workspaces
        .create_process(&workspace, &body.display_name, config)
    {
        Ok(p) => (StatusCode::CREATED, Json(p)).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/workspaces/{workspace}/processes/{process}` — one process.
async fn ws_process(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.workspaces.get_process(&workspace, &process) {
        Some(p) => Json(p).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such process: {workspace}/{process}") })),
        )
            .into_response(),
    }
}

/// `PUT /api/workspaces/{workspace}/processes/{process}` — update config.
async fn ws_update_process(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Json(config): Json<workspace::ProcessConfig>,
) -> impl IntoResponse {
    match state
        .workspaces
        .update_process(&workspace, &process, config)
    {
        Ok(p) => Json(p).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /assets/bpmn/{file}` — serve a vendored bpmn-js viewer asset.
async fn bpmn_asset(Path(file): Path<String>) -> impl IntoResponse {
    let (body, ctype): (&'static str, &'static str) = match file.as_str() {
        "bpmn-navigated-viewer.js" => (BPMN_VIEWER_JS, "application/javascript; charset=utf-8"),
        "bpmn-modeler.production.min.js" => {
            (BPMN_MODELER_JS, "application/javascript; charset=utf-8")
        }
        "bpmn-auto-layout.js" => (BPMN_AUTO_LAYOUT_JS, "application/javascript; charset=utf-8"),
        "elk.bundled.js" => (BPMN_ELK_JS, "application/javascript; charset=utf-8"),
        "diagram-js.css" => (BPMN_DIAGRAM_CSS, "text/css; charset=utf-8"),
        "bpmn-embedded.css" => (BPMN_EMBEDDED_CSS, "text/css; charset=utf-8"),
        "zeebe-moddle.json" => (BPMN_ZEEBE_MODDLE_JSON, "application/json; charset=utf-8"),
        "nano-moddle.json" => (BPMN_NANO_MODDLE_JSON, "application/json; charset=utf-8"),
        _ => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    (
        [
            (axum::http::header::CONTENT_TYPE, ctype),
            (axum::http::header::CACHE_CONTROL, NO_CACHE),
        ],
        body,
    )
        .into_response()
}

/// Cache directive for the single-file UI and its sibling JS/CSS assets. They are recompiled into
/// the binary (`include_str!`) on every build, so a heuristically-cached copy in the browser makes
/// a fresh build look stale (missing routes/buttons). `no-cache` forces a revalidation each load.
const NO_CACHE: &str = "no-cache, no-store, must-revalidate";

/// Serves the shared settings panel module (`/assets/settings.js`), included by both the
/// console and the cockpit.
async fn settings_js() -> impl IntoResponse {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, NO_CACHE),
        ],
        SETTINGS_JS,
    )
}

/// Serves the shared design tokens (`/assets/theme.css`) — the `--po-*` custom properties every
/// ProcessOS page styles through, with the dark and light palettes.
async fn theme_css() -> impl IntoResponse {
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (axum::http::header::CACHE_CONTROL, NO_CACHE),
        ],
        THEME_CSS,
    )
}

/// Serves the theme runtime (`/assets/theme.js`) — applies the persisted light/dark/system
/// choice before first paint and mounts the appearance switcher.
async fn theme_js() -> impl IntoResponse {
    (
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (axum::http::header::CACHE_CONTROL, NO_CACHE),
        ],
        THEME_JS,
    )
}

/// `GET /api/workspaces/{workspace}/processes/{process}/model` — the process's BPMN
/// XML, for the viewer. 404 when the process has no `model.bpmn`.
async fn ws_process_model(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.workspaces.read_model(&workspace, &process) {
        Some(xml) => (
            [(
                axum::http::header::CONTENT_TYPE,
                "application/xml; charset=utf-8",
            )],
            xml,
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no model.bpmn for this process" })),
        )
            .into_response(),
    }
}

/// `PUT /api/workspaces/{workspace}/processes/{process}/model` — set the BPMN model
/// (raw XML request body).
async fn ws_set_process_model(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    body: String,
) -> impl IntoResponse {
    if body.trim().is_empty() {
        return unprocessable("empty model body".into());
    }
    match state.workspaces.write_model(&workspace, &process, &body) {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/workspaces/{workspace}/processes/{process}/annotations` — the
/// semantic-annotation sidecar for a process, plus the current model's
/// revision key so callers can tell whether the entry they're looking at
/// still applies (see [`annotations`][crate::annotations] for the format
/// and rationale).
///
/// Response body (always 200, even when the sidecar is empty — the model
/// may have a valid revision key with no annotations yet):
/// ```json
/// {
///   "revision": "<hex-sha256 of the current model.bpmn, or null when no model>",
///   "sidecar": { "revisions": {…}, "current": "…" },
///   "entry":   { "annotations": {…}, "provenance": "human", "updatedAt": "…" } | null
/// }
/// ```
async fn ws_process_annotations(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
) -> impl IntoResponse {
    let revision = state
        .workspaces
        .current_model_revision(&workspace, &process);
    let sidecar = match state.workspaces.read_annotations(&workspace, &process) {
        Ok(s) => s.unwrap_or_default(),
        Err(e) => return unprocessable(e),
    };
    let entry = revision
        .as_deref()
        .and_then(|r| sidecar.revision(r).cloned());
    Json(serde_json::json!({
        "revision": revision,
        "sidecar": sidecar,
        "entry": entry,
    }))
    .into_response()
}

/// `PUT /api/workspaces/{workspace}/processes/{process}/annotations` — write
/// (or replace) the sidecar entry for the *current* model revision. The
/// revision key is computed server-side from `model.bpmn`, not accepted
/// from the client, so a stale UI can't accidentally overwrite entries for
/// the wrong bytes.
///
/// Request body:
/// ```json
/// {
///   "annotations": { … SemanticAnnotations … },
///   "provenance":  "human" | "inferred" | "mixed" | "llm"   // optional, default "human"
/// }
/// ```
/// Response mirrors the GET shape.
#[derive(Deserialize)]
struct SetAnnotationsRequest {
    annotations: layout::SemanticAnnotations,
    #[serde(default)]
    provenance: Option<String>,
}

async fn ws_set_process_annotations(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Json(req): Json<SetAnnotationsRequest>,
) -> impl IntoResponse {
    let Some(revision) = state
        .workspaces
        .current_model_revision(&workspace, &process)
    else {
        return unprocessable(format!(
            "process {workspace}/{process} has no model.bpmn to annotate"
        ));
    };
    let provenance = req.provenance.as_deref().unwrap_or("human");
    let sidecar = match state.workspaces.write_annotations(
        &workspace,
        &process,
        &revision,
        req.annotations,
        provenance,
    ) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let entry = sidecar.revision(&revision).cloned();
    Json(serde_json::json!({
        "revision": revision,
        "sidecar": sidecar,
        "entry": entry,
    }))
    .into_response()
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SeedDemoBody {
    /// Which bundled dataset to seed (default: the first registered, loan-approval).
    #[serde(default)]
    dataset: Option<String>,
    /// Workspace display name (default: the dataset's own).
    #[serde(default)]
    workspace: Option<String>,
    /// Process display name (default: the dataset's own).
    #[serde(default)]
    process: Option<String>,
}

/// `GET /api/workspaces/demo-datasets` — the bundled scenarios the operator can
/// seed, so the UI can present a picker.
async fn ws_demo_datasets() -> impl IntoResponse {
    let items: Vec<serde_json::Value> = DEMO_DATASETS
        .iter()
        .map(|d| {
            serde_json::json!({
                "id": d.id,
                "workspace": d.workspace,
                "process": d.process,
                "objective": d.objective,
                "description": d.description,
            })
        })
        .collect();
    Json(serde_json::json!({ "datasets": items }))
}

/// `POST /api/workspaces/seed-demo` — materialise a bundled demo dataset: a
/// workspace + a process bound to a freshly generated trace dataset + its BPMN
/// model. Idempotent on the derived slugs. Returns the created `{workspace,
/// process}` slugs.
async fn ws_seed_demo(
    State(state): State<AppState>,
    Json(body): Json<SeedDemoBody>,
) -> impl IntoResponse {
    let dataset_id = body
        .dataset
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEMO_DATASETS[0].id.to_string());
    let Some(dataset) = demo_dataset(&dataset_id) else {
        return unprocessable(format!("unknown demo dataset '{dataset_id}'"));
    };

    let ws_name = body
        .workspace
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| dataset.workspace.to_string());
    let proc_name = body
        .process
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| dataset.process.to_string());

    // Run the (blocking) corpus generation off the async runtime.
    let workspaces = state.workspaces.clone();
    let result =
        tokio::task::spawn_blocking(move || seed_demo(&workspaces, dataset, &ws_name, &proc_name))
            .await
            .map_err(|e| format!("seed task failed: {e}"));

    match result {
        Ok(Ok(slugs)) => (StatusCode::CREATED, Json(slugs)).into_response(),
        Ok(Err(e)) => unprocessable(e),
        Err(e) => bad_gateway(e),
    }
}

/// Create the workspace + process, generate the corpus into its `traces/` folder, and
/// drop the BPMN model beside it. Returns the slugs.
fn seed_demo(
    workspaces: &workspace::WorkspaceCatalog,
    dataset: &DemoDataset,
    ws_name: &str,
    proc_name: &str,
) -> Result<serde_json::Value, String> {
    let ws = workspaces.create_workspace(
        ws_name,
        Some(format!("Demo engagement (bundled {} corpus)", dataset.id)),
    )?;
    let proc_cfg = workspace::ProcessConfig {
        display_name: proc_name.to_string(),
        objective: Some(dataset.objective.to_string()),
        ..Default::default()
    };
    let proc = workspaces.create_process(&ws.slug, proc_name, proc_cfg)?;

    // Materialise the embedded pack files (preserving relative paths so the
    // `bpmn`/`bpmn_library` references resolve), then load + generate.
    let tmp = std::env::temp_dir().join(format!(
        "processos-seed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&tmp).map_err(|e| format!("seed tmp: {e}"))?;
    for file in dataset.files {
        let dest = tmp.join(file.rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("seed tmp dir: {e}"))?;
        }
        std::fs::write(&dest, file.contents).map_err(|e| format!("write {}: {e}", file.rel))?;
    }

    let (pack, def) = corpus::load_pack(&tmp.join(dataset.pack_rel))?;
    let traces_dir = workspaces.process_traces_dir(&ws.slug, &proc.slug)?;
    let summary = corpus::generate(&pack, &def, &traces_dir)?;
    let model_xml = dataset
        .files
        .iter()
        .find(|f| f.rel == dataset.model_rel)
        .map(|f| f.contents)
        .ok_or_else(|| format!("model file '{}' missing from dataset", dataset.model_rel))?;
    // For a multi-stage orchestrator, make the persisted model SELF-CONTAINED: merge the called
    // phase definitions into the one file so the cockpit tools (read_model expand / read_model_xml
    // / edit_model process:) and replay can see and address the inner tasks — the `Parent$Child`
    // ids the trace tables carry. Single-file models (no call activities) are written unchanged.
    let merged;
    let model_to_write: &str = if crate::bpmn_model::references_call_activities(model_xml) {
        let phases: Vec<&str> = dataset
            .files
            .iter()
            .filter(|f| f.rel != dataset.model_rel && f.rel.ends_with(".bpmn"))
            .map(|f| f.contents)
            .collect();
        merged = crate::bpmn_model::merge_definitions(model_xml, &phases);
        &merged
    } else {
        model_xml
    };
    workspaces.write_model(&ws.slug, &proc.slug, model_to_write)?;
    let _ = std::fs::remove_dir_all(&tmp);

    Ok(serde_json::json!({
        "workspace": ws.slug,
        "process": proc.slug,
        "instances": summary.instances,
    }))
}

/// folded Insights report for this process, read from whatever source it is bound
/// to (live customer Nano or a loaded trace dataset).
async fn ws_process_insights(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<InsightsQuery>,
) -> impl IntoResponse {
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    // An in-memory captured dataset costs nothing per-read, so analyze the whole
    // population (up to a sane ceiling) rather than a small sampled window — the
    // report then reflects the real dataset, not an arbitrary 200/50 slice. A
    // live gateway pays a request per detail, so it keeps the bounded defaults.
    const FULL_ANALYSIS_CAP: usize = 50_000;
    let (limit, sample) = match src.total() {
        Some(total) => {
            let n = total.clamp(1, FULL_ANALYSIS_CAP);
            (n, n)
        }
        None => (
            q.limit.unwrap_or(200).clamp(1, 1000),
            q.sample.unwrap_or(50).clamp(1, 500),
        ),
    };
    match report::build_over(&src, limit, sample).await {
        Ok(report) => Json(report).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// `GET /api/settings` — the operator's persisted settings (LLM profiles + active
/// profile + Python interpreter), redacted so API keys are never echoed back.
async fn get_settings(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.settings.view())
}

/// `PUT /api/settings` — update the globals: the active profile and/or the Python
/// interpreter. Absent fields are left unchanged.
async fn put_settings(
    State(state): State<AppState>,
    Json(patch): Json<settings::GlobalsPatch>,
) -> impl IntoResponse {
    match state.settings.update_globals(patch) {
        Ok(view) => Json(view).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/settings/profiles` — create a new LLM profile (optionally seeded from the
/// body). Returns `{ id, settings }`.
async fn post_profile(
    State(state): State<AppState>,
    Json(patch): Json<settings::ProfilePatch>,
) -> impl IntoResponse {
    match state.settings.add_profile(patch) {
        Ok((id, view)) => Json(serde_json::json!({ "id": id, "settings": view })).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `PUT /api/settings/profiles/{id}` — partial update of one profile. Absent fields are
/// left unchanged; an empty string clears a field back to the environment default.
async fn put_profile(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(patch): Json<settings::ProfilePatch>,
) -> impl IntoResponse {
    match state.settings.update_profile(&id, patch) {
        Ok(view) => Json(view).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/settings/profiles/{id}` — remove a profile.
async fn delete_profile(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.settings.delete_profile(&id) {
        Ok(view) => Json(view).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/settings/models` — query an endpoint's model list so the console can pick a
/// model. Body: `{ profileId?, provider?, baseUrl?, apiKey? }` (a saved profile supplies a
/// stored key; inline fields let the console probe an endpoint it is still editing).
async fn post_models(
    State(state): State<AppState>,
    Json(req): Json<settings::ProbeRequest>,
) -> impl IntoResponse {
    let cfg = match state.settings.probe_config(&req) {
        Ok(c) => c,
        Err(e) => return unprocessable(e),
    };
    match list_models(&cfg).await {
        Ok(models) => Json(serde_json::json!({ "models": models })).into_response(),
        Err(e) => bad_gateway(e),
    }
}

/// Bytes downloaded so far if `model`'s GGUF is mid-download in the Hugging Face cache under
/// `models_dir` (llama.cpp writes `models--org--repo/blobs/<sha>.downloadInProgress` while a
/// `-hf` model is still being fetched). Returns the largest such partial file's size, or `None`
/// when nothing is downloading. This is how we tell "the model is still downloading" apart from
/// "the server is up" — a `-hf` sidecar downloads the whole GGUF before it ever binds its port.
fn download_in_progress(models_dir: &std::path::Path, model: &str) -> Option<u64> {
    let repo = model.split(':').next().unwrap_or(model);
    let folder = format!("models--{}", repo.replace('/', "--"));
    let blobs = models_dir.join(folder).join("blobs");
    let mut best: Option<u64> = None;
    if let Ok(entries) = std::fs::read_dir(&blobs) {
        for e in entries.flatten() {
            if e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".downloadInProgress"))
            {
                if let Ok(m) = e.metadata() {
                    let sz = m.len();
                    best = Some(best.map_or(sz, |b| b.max(sz)));
                }
            }
        }
    }
    best
}

/// Resolve `model` to its on-disk GGUF, when fully present in the cache under `models_dir`.
/// Two cases:
///   * a local `*.gguf` model spec → the file itself (absolute, or relative to the models dir);
///   * a Hugging Face `repo[:quant]` spec → a usable `snapshots/<rev>/<file>.gguf` (whose backing
///     blob exists) in the llama.cpp cache folder `models--org--repo`, provided no half-finished
///     `*.downloadInProgress` blob remains. When a `:quant` is given, the snapshot filename must
///     contain it (case-insensitive) so a different quant already cached doesn't resolve.
///
/// Sharded models resolve to their lexicographically-first shard (`…-00001-of-…`), which carries
/// the full GGUF metadata a header scan needs. `None` when absent or still downloading.
fn resolve_model_gguf(models_dir: &std::path::Path, model: &str) -> Option<std::path::PathBuf> {
    let model = model.trim();
    if model.is_empty() {
        return None;
    }
    if model.to_ascii_lowercase().ends_with(".gguf") {
        let p = std::path::PathBuf::from(model);
        let resolved = if p.is_absolute() {
            p
        } else {
            models_dir.join(&p)
        };
        return resolved.exists().then_some(resolved);
    }
    // Hugging Face spec: split repo from the optional :quant tag.
    let (repo, quant) = match model.split_once(':') {
        Some((r, q)) => (r, Some(q.to_ascii_lowercase())),
        None => (model, None),
    };
    let base = models_dir.join(format!("models--{}", repo.replace('/', "--")));
    // A still-in-progress blob means the download is not complete.
    if download_in_progress(models_dir, model).is_some() {
        return None;
    }
    // Collect the usable .gguf files under any snapshot revision whose backing blob exists.
    let snaps = base.join("snapshots");
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    for rev in std::fs::read_dir(&snaps).ok()?.flatten() {
        let Ok(files) = std::fs::read_dir(rev.path()) else {
            continue;
        };
        for f in files.flatten() {
            let name = f.file_name().to_string_lossy().to_ascii_lowercase();
            if !name.ends_with(".gguf") {
                continue;
            }
            if let Some(q) = &quant {
                if !name.contains(q.as_str()) {
                    continue;
                }
            }
            // `f.path()` is usually a symlink into blobs/; `exists()` follows it, so a dangling
            // link (blob evicted) correctly reads as not-downloaded.
            if f.path().exists() {
                candidates.push(f.path());
            }
        }
    }
    // Sorting puts a sharded model's first shard (…-00001-of-…) ahead of its siblings.
    candidates.sort();
    candidates.into_iter().next()
}

/// Whether `model`'s GGUF is already fully present in the local cache under `models_dir`, so it
/// can be started without a (re)download. Distinguishes "downloaded" from "configured" and from
/// "available for use" (running).
fn model_downloaded(models_dir: &std::path::Path, model: &str) -> bool {
    resolve_model_gguf(models_dir, model).is_some()
}

/// The model a sidecar profile loads: `model_file`, else the plain `model` id.
fn profile_model_spec(p: &settings::LlmProfile) -> Option<String> {
    p.model_file
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or(p.model.as_deref())
        .map(str::to_string)
}

/// The real, operator-facing lifecycle phase of a sidecar — beyond the binary "the process is
/// running" flag, which is misleading because a freshly-spawned `llama-server` is NOT usable for
/// minutes: it first downloads the model (no port yet), then loads it into memory (`/health` 503),
/// and only then serves chat (`/health` 200). Returns `(phase, human-detail)`:
///   down · starting · downloading · loading · ready
async fn sidecar_phase(status: &llama::LlamaStatus) -> (String, Option<String>) {
    if !status.running {
        return ("down".into(), None);
    }
    if let Some(port) = status.port {
        let url = format!("http://127.0.0.1:{port}/health");
        if let Ok(client) = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
        {
            match client.get(&url).send().await {
                Ok(r) if r.status().is_success() => return ("ready".into(), None),
                // The HTTP server is up but the model is still loading into memory (llama-server
                // answers /health with 503 until the weights are mapped and warmed).
                Ok(_) => return ("loading".into(), Some("loading model into memory…".into())),
                // Connection refused / unreachable: the process is alive but hasn't bound its
                // port — almost always because it is still downloading the model.
                Err(_) => {}
            }
        }
    }
    if let (Some(dir), Some(model)) = (status.models_dir.as_deref(), status.model.as_deref()) {
        if let Some(bytes) = download_in_progress(std::path::Path::new(dir), model) {
            let gb = bytes as f64 / 1_073_741_824.0;
            return (
                "downloading".into(),
                Some(format!("downloading model… {gb:.1} GB fetched so far")),
            );
        }
    }
    ("starting".into(), Some("starting llama-server…".into()))
}

/// Probe a running sidecar for the **live context window** it was launched with. llama.cpp's
/// OpenAI-compatible `/v1/models` reports `data[0].meta.n_ctx` (the configured window, honouring
/// the sidecar's `-c`/`--ctx-size` arg), which is the most accurate "how much context this model
/// actually affords right now". Returns `None` if the endpoint is unreachable or doesn't report
/// one. A short timeout keeps `/api/llama/status` snappy.
async fn sidecar_context_window(port: u16) -> Option<u64> {
    let url = format!("http://127.0.0.1:{port}/v1/models");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .ok()?;
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    let items = v.get("data").or_else(|| v.get("models"))?.as_array()?;
    items.iter().find_map(harness::llm::extract_context_window)
}

/// `GET /api/system/dependencies` — preflight the external CLI tools ProcessOS
/// shells out to (Node for embedded Nano workers; llama.cpp's `llama-server` for
/// local LLM sidecars). Reports each tool's presence, resolved binary, version,
/// purpose, and — when missing — an actionable hint plus an OS-aware install URL,
/// so the Console can warn the operator instead of failing later with a cryptic
/// spawn error.
async fn system_dependencies(State(state): State<AppState>) -> impl IntoResponse {
    let llama_bin = state.settings.snapshot().llama_bin.clone();
    // The probes spawn short-lived child processes; run them off the async runtime.
    let deps = tokio::task::spawn_blocking(move || deps::check(llama_bin.as_deref()))
        .await
        .unwrap_or_default();
    let missing: Vec<&str> = deps.iter().filter(|d| !d.present).map(|d| d.id).collect();
    Json(serde_json::json!({
        "dependencies": deps,
        "allPresent": missing.is_empty(),
        "missing": missing,
    }))
}

/// `GET /api/llama/status` — the local sidecar pool: every running `llama-server` (model, port,
/// pid, the equivalent terminal command) plus the capacity, so the UI knows whether another may
/// be started. Each sidecar also carries its real lifecycle `phase`/`detail` (downloading vs
/// loading vs ready) so the UI never reports a still-downloading model as usable, and `anyReady`
/// is true only when at least one sidecar can actually serve a chat.
async fn llama_status(State(state): State<AppState>) -> impl IntoResponse {
    let list = state.llama.statuses();
    let mut sidecars = Vec::with_capacity(list.sidecars.len());
    let mut any_ready = false;
    for s in &list.sidecars {
        let (phase, detail) = sidecar_phase(s).await;
        if phase == "ready" {
            any_ready = true;
        }
        // For a ready sidecar, surface the live context window it was launched with so the LLM
        // tile can show how much context the model actually affords (may differ from the model's
        // trained window if started with a smaller `-c`).
        let ctx = if phase == "ready" {
            match s.port {
                Some(port) => sidecar_context_window(port).await,
                None => None,
            }
        } else {
            None
        };
        let mut v = serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(obj) = v.as_object_mut() {
            obj.insert("phase".into(), serde_json::json!(phase));
            obj.insert("detail".into(), serde_json::json!(detail));
            if let Some(ctx) = ctx {
                obj.insert("contextWindow".into(), serde_json::json!(ctx));
            }
        }
        sidecars.push(v);
    }
    // Per-sidecar-profile download state, so the UI can show "Download" vs "Start" vs running and
    // keep "configured" / "downloaded" / "available for use" distinct.
    let snap = state.settings.snapshot();
    let models_dir = snap.effective_models_dir();
    let downloads: Vec<serde_json::Value> = state
        .llama
        .download_states()
        .into_iter()
        .map(|d| {
            let bytes = download_in_progress(&models_dir, &d.model);
            serde_json::json!({
                "profileId": d.profile_id,
                "model": d.model,
                "port": d.port,
                "startedAt": d.started_at,
                "bytes": bytes,
            })
        })
        .collect();
    let models: Vec<serde_json::Value> = snap
        .profiles
        .iter()
        .filter(|p| p.sidecar)
        .map(|p| {
            let model = p
                .model_file
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .or(p.model.as_deref())
                .unwrap_or("");
            serde_json::json!({
                "profileId": p.id,
                "downloaded": model_downloaded(&models_dir, model),
            })
        })
        .collect();
    Json(serde_json::json!({
        "running": list.running,
        "anyReady": any_ready,
        "count": list.count,
        "max": list.max,
        "sidecars": sidecars,
        "downloads": downloads,
        "models": models,
        "error": list.error,
    }))
}

/// `GET /api/llama/ready?profileId=…` — whether a sidecar is not just spawned but actually
/// answering (its model has finished loading). Probes the `llama-server` `/health` endpoint
/// (200 once ready, 503 while loading). With `profileId` it checks that specific sidecar; without
/// it, any running sidecar. Returns `{ running, ready, profileId }`. The cockpit polls this after
/// a just-in-time sidecar start, before sending a queued chat message.
async fn llama_ready(
    State(state): State<AppState>,
    Query(q): Query<LlamaProfileQuery>,
) -> impl IntoResponse {
    let status = match q.profile_id.as_deref() {
        Some(id) => state.llama.status_of(id),
        None => state
            .llama
            .statuses()
            .sidecars
            .into_iter()
            .next()
            .unwrap_or_else(|| state.llama.status_of("")),
    };
    let (phase, detail) = sidecar_phase(&status).await;
    let ready = phase == "ready";
    Json(serde_json::json!({
        "running": status.running, "ready": ready,
        "phase": phase, "detail": detail,
        "profileId": status.profile_id,
    }))
}

/// Request body for starting the sidecar: which saved profile to serve, and optionally which
/// speculator-mode pairing to apply (the pairing's secondary model is loaded as the
/// speculative-decoding draft via `--model-draft`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaStartRequest {
    profile_id: String,
    #[serde(default)]
    pairing_id: Option<String>,
}

/// Scan a sidecar profile's GGUF header, resolving the model through the cache. Errors when the
/// profile has no model spec or the model is not fully downloaded (a scan needs the file).
fn scan_profile_gguf(
    models_dir: &std::path::Path,
    profile: &settings::LlmProfile,
) -> Result<gguf::GgufScan, String> {
    let model = profile_model_spec(profile)
        .ok_or_else(|| format!("profile '{}' has no model to scan", profile.id))?;
    let path = resolve_model_gguf(models_dir, &model).ok_or_else(|| {
        format!(
            "model for profile '{}' ({model}) is not downloaded yet — download it first so its \
             GGUF metadata can be verified",
            profile.id
        )
    })?;
    gguf::GgufScan::read(&path)
}

/// Resolve + validate a speculator pairing for launching `primary`, returning the draft spec to
/// attach to the launch plan. This is the **hard gate** that disallows invalid configs:
/// the pairing must be speculator-mode and name this primary (when it pins one), the draft must
/// be a local sidecar GGUF that is fully downloaded, and the two models' GGUF metadata must be
/// speculative-decoding compatible (same tokenizer family, BOS/EOS, vocab within llama.cpp's
/// tolerance). The same model on both sides — "loaded twice" — passes trivially.
fn resolve_speculator_draft(
    snap: &settings::Settings,
    pairing: &pairings::Pairing,
    primary: &settings::LlmProfile,
    models_dir: &std::path::Path,
) -> Result<llama::DraftSpec, String> {
    if pairing.mode != pairings::PairMode::Speculator {
        return Err(format!(
            "pairing '{}' is {:?}-mode, not a speculator pairing",
            pairing.id, pairing.mode
        ));
    }
    if let Some(pinned) = pairing.primary_profile_id.as_deref() {
        if pinned != primary.id {
            return Err(format!(
                "pairing '{}' pins primary profile '{pinned}', but '{}' is being started",
                pairing.id, primary.id
            ));
        }
    }
    let draft_profile = snap
        .profiles
        .iter()
        .find(|p| p.id == pairing.secondary_profile_id)
        .ok_or_else(|| {
            format!(
                "pairing '{}' names unknown draft profile '{}'",
                pairing.id, pairing.secondary_profile_id
            )
        })?;
    if !draft_profile.sidecar {
        return Err(format!(
            "draft profile '{}' is not a local sidecar — a speculator must be a local GGUF the \
             primary's llama-server can load",
            draft_profile.id
        ));
    }
    let draft_model = profile_model_spec(draft_profile)
        .ok_or_else(|| format!("draft profile '{}' has no model file", draft_profile.id))?;
    let draft_path = resolve_model_gguf(models_dir, &draft_model).ok_or_else(|| {
        format!(
            "draft model for '{}' ({draft_model}) is not downloaded yet",
            draft_profile.id
        )
    })?;
    let target_scan = scan_profile_gguf(models_dir, primary)?;
    let draft_scan = gguf::GgufScan::read(&draft_path)?;
    gguf::speculator_compatible(&target_scan, &draft_scan).map_err(|e| {
        format!(
            "'{}' cannot draft for '{}': {e}",
            draft_profile.id, primary.id
        )
    })?;
    Ok(llama::DraftSpec {
        profile_id: draft_profile.id.clone(),
        model_path: draft_path,
        draft_max: pairing.draft_max,
        draft_min: pairing.draft_min,
    })
}

/// `POST /api/llama/start` — launch `llama-server` for the named `sidecar:true` profile. ProcessOS
/// auto-assigns a free TCP port (the operator never configures one); the assigned port is written
/// back into the profile's `base_url` so the model client talks to it. Up to [`llama::MAX_SIDECARS`]
/// run at once. The models directory is exported as `LLAMA_CACHE`.
///
/// Two launch-time capability gates (both scan GGUF headers, see [`crate::gguf`]):
///   * a profile with `mtp: true` must actually carry MTP (NextN) prediction heads;
///   * a `pairingId` must name a speculator pairing whose draft model is verified compatible —
///     the draft is then loaded into the same `llama-server` via `--model-draft`.
async fn llama_start(
    State(state): State<AppState>,
    Json(req): Json<LlamaStartRequest>,
) -> impl IntoResponse {
    let snap = state.settings.snapshot();
    let profile = match snap.profiles.iter().find(|p| p.id == req.profile_id) {
        Some(p) => p.clone(),
        None => return unprocessable(format!("no such profile: {}", req.profile_id)),
    };
    if !profile.sidecar {
        return unprocessable(format!(
            "profile '{}' is not configured to use the local sidecar",
            profile.id
        ));
    }
    let models_dir = snap.effective_models_dir();
    // Header scans are blocking file I/O — run the gates off the async worker.
    let pairing = match req.pairing_id.as_deref() {
        Some(pid) => match state.pairings.list().into_iter().find(|p| p.id == pid) {
            Some(p) => Some(p),
            None => return unprocessable(format!("no such pairing: {pid}")),
        },
        None => None,
    };
    let draft = {
        let snap = snap.clone();
        let profile = profile.clone();
        let models_dir = models_dir.clone();
        let gates =
            tokio::task::spawn_blocking(move || -> Result<Option<llama::DraftSpec>, String> {
                if profile.mtp {
                    let scan = scan_profile_gguf(&models_dir, &profile)?;
                    if !scan.mtp_capable() {
                        return Err(format!(
                            "profile '{}' has MTP enabled, but its GGUF carries no NextN \
                         prediction heads (arch {}) — disable MTP or pick an MTP-capable model",
                            profile.id,
                            scan.arch.as_deref().unwrap_or("unknown")
                        ));
                    }
                }
                pairing
                    .as_ref()
                    .map(|p| resolve_speculator_draft(&snap, p, &profile, &models_dir))
                    .transpose()
            })
            .await;
        match gates {
            Ok(Ok(d)) => d,
            Ok(Err(e)) => return unprocessable(e),
            Err(e) => return unprocessable(format!("scan task failed: {e}")),
        }
    };
    let status = match state.llama.start_profile(
        &profile,
        &models_dir,
        snap.llama_bin.as_deref(),
        draft.as_ref(),
    ) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    // Persist the auto-assigned port into the profile's base URL so the model client (resolve_llm /
    // as_llm_override) and the reasoning-control probe all reach the actual running endpoint.
    if let Some(port) = status.port {
        let base_url = format!("http://127.0.0.1:{port}/v1");
        if profile.base_url.as_deref() != Some(base_url.as_str()) {
            let patch = settings::ProfilePatch {
                base_url: Some(base_url),
                ..Default::default()
            };
            let _ = state.settings.update_profile(&profile.id, patch);
        }
    }
    Json(status).into_response()
}

/// `GET /api/llama/scan/{profile_id}` — scan a sidecar profile's GGUF header and report its
/// capability card: architecture, tokenizer family, vocab size, context length, and whether the
/// model carries MTP (NextN multi-token-prediction) heads that llama.cpp's `--mtp` can drive.
/// Only the header is read (weights untouched), but the model must be fully downloaded. Powers
/// the Models view's MTP toggle and the speculator-pairing compatibility hints.
async fn llama_scan(
    State(state): State<AppState>,
    Path(profile_id): Path<String>,
) -> impl IntoResponse {
    let snap = state.settings.snapshot();
    let Some(profile) = snap.profiles.iter().find(|p| p.id == profile_id).cloned() else {
        return unprocessable(format!("no such profile: {profile_id}"));
    };
    if !profile.sidecar {
        return unprocessable(format!(
            "profile '{}' is not a local sidecar — only local GGUF models can be scanned",
            profile.id
        ));
    }
    let Some(model) = profile_model_spec(&profile) else {
        return unprocessable(format!("profile '{}' has no model to scan", profile.id));
    };
    let models_dir = snap.effective_models_dir();
    // Blocking file I/O (cache walk + header read) off the async worker.
    let scanned = tokio::task::spawn_blocking(move || {
        resolve_model_gguf(&models_dir, &model).map(|path| (gguf::GgufScan::read(&path), path))
    })
    .await;
    let body = match scanned {
        Ok(None) => serde_json::json!({
            "profileId": profile.id, "downloaded": false,
        }),
        Ok(Some((Ok(scan), path))) => serde_json::json!({
            "profileId": profile.id, "downloaded": true,
            "path": path.display().to_string(),
            "arch": scan.arch, "name": scan.name,
            "contextLength": scan.context_length,
            "vocabSize": scan.vocab_size,
            "tokenizerModel": scan.tokenizer_model,
            "tokenizerPre": scan.tokenizer_pre,
            "bosTokenId": scan.bos_token_id, "eosTokenId": scan.eos_token_id,
            "mtpCapable": scan.mtp_capable(),
            "mtpLayers": scan.nextn_predict_layers,
            "nextnTensors": scan.nextn_tensor_count,
            "tensorCount": scan.tensor_count,
        }),
        Ok(Some((Err(e), _))) => serde_json::json!({
            "profileId": profile.id, "downloaded": true, "error": e,
        }),
        Err(e) => serde_json::json!({
            "profileId": profile.id, "error": format!("scan task failed: {e}"),
        }),
    };
    Json(body).into_response()
}

/// `POST /api/llama/download` — start a BACKGROUND download of a `sidecar:true` profile's model
/// into the cache, without serving it. A transient `llama-server` fetches the GGUF and is stopped
/// the moment it is fully cached, so "downloaded" stays distinct from "available for use". Returns
/// the download state, or 200 `{ alreadyDownloaded:true }` when the model is already present.
async fn llama_download(
    State(state): State<AppState>,
    Json(req): Json<LlamaStartRequest>,
) -> impl IntoResponse {
    let snap = state.settings.snapshot();
    let profile = match snap.profiles.iter().find(|p| p.id == req.profile_id) {
        Some(p) => p.clone(),
        None => return unprocessable(format!("no such profile: {}", req.profile_id)),
    };
    if !profile.sidecar {
        return unprocessable(format!(
            "profile '{}' is not configured to use the local sidecar",
            profile.id
        ));
    }
    let models_dir = snap.effective_models_dir();
    let model = profile
        .model_file
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or(profile.model.as_deref())
        .unwrap_or("")
        .to_string();
    if model_downloaded(&models_dir, &model) {
        return Json(serde_json::json!({ "alreadyDownloaded": true, "profileId": profile.id }))
            .into_response();
    }
    // The watcher polls this to know when the GGUF is fully cached (cache scan lives in main.rs).
    let dir = models_dir.clone();
    let m = model.clone();
    let is_complete: std::sync::Arc<dyn Fn() -> bool + Send + Sync> =
        std::sync::Arc::new(move || model_downloaded(&dir, &m));
    match state.llama.start_download(
        &profile,
        &models_dir,
        snap.llama_bin.as_deref(),
        is_complete,
    ) {
        Ok(s) => Json(s).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/llama/download/cancel` — cancel a background download (`{profileId}`). Idempotent.
async fn llama_download_cancel(
    State(state): State<AppState>,
    Json(req): Json<LlamaStartRequest>,
) -> impl IntoResponse {
    let cancelled = state.llama.cancel_download(&req.profile_id);
    Json(serde_json::json!({ "cancelled": cancelled, "profileId": req.profile_id }))
}

/// Request body for stopping a sidecar: an optional profile id (omit to stop all).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaStopRequest {
    #[serde(default)]
    profile_id: Option<String>,
}

/// `POST /api/llama/stop` — stop one sidecar (`{profileId}`) or all of them (empty body).
/// Idempotent. Returns the resulting pool state.
async fn llama_stop(
    State(state): State<AppState>,
    body: Option<Json<LlamaStopRequest>>,
) -> impl IntoResponse {
    let profile_id = body.and_then(|Json(b)| b.profile_id);
    Json(state.llama.stop(profile_id.as_deref()).await)
}

/// Query for incremental log polling (`?profileId=…&since=<offset>`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaLogsQuery {
    #[serde(default)]
    since: u64,
    #[serde(default)]
    profile_id: Option<String>,
}

/// Query carrying just a profile id (`?profileId=…`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LlamaProfileQuery {
    #[serde(default)]
    profile_id: Option<String>,
}

/// `GET /api/llama/logs?profileId=…&since=N` — a sidecar's captured stdout/stderr from offset `N`
/// onward, plus the next offset to poll and whether the process is still running. Powers the
/// streaming log viewer. When `profileId` is omitted, the single running sidecar is used.
async fn llama_logs(
    State(state): State<AppState>,
    Query(q): Query<LlamaLogsQuery>,
) -> impl IntoResponse {
    let profile_id = q.profile_id.or_else(|| {
        state
            .llama
            .statuses()
            .sidecars
            .into_iter()
            .next()
            .and_then(|s| s.profile_id)
    });
    let Some(profile_id) = profile_id else {
        return Json(serde_json::json!({
            "lines": [], "text": "", "nextOffset": q.since, "running": false,
        }));
    };
    let (lines, next, running) = state.llama.logs_since(&profile_id, q.since);
    let text = lines.join("\n");
    Json(serde_json::json!({
        "lines": lines, "text": text, "nextOffset": next, "running": running, "profileId": profile_id,
    }))
}

/// `GET /api/llama/reasoning-control?profileId=…` — probe whether the target endpoint exposes the
/// optional reasoning-control surface (`/v1/chat/completions/control`). When `profileId` is given
/// the named profile's endpoint is probed, otherwise the active profile's. Lets the UI tell the
/// operator whether wrap-up/monitor can halt mid-thinking or only at a round boundary (fallback).
async fn reasoning_control_status(
    State(state): State<AppState>,
    Query(q): Query<LlamaLogsQuery>,
) -> impl IntoResponse {
    let cfg = match q.profile_id.as_deref() {
        Some(pid) => resolve_llm_for_profile(&state, Some(pid), None),
        None => Some(resolve_llm(&state, None)),
    };
    let Some(cfg) = cfg.filter(|c| c.is_ready()) else {
        return Json(serde_json::json!({ "supported": false, "ready": false }));
    };
    let supported = reasoning::supports_control(&cfg.base_url).await;
    Json(serde_json::json!({
        "supported": supported,
        "ready": true,
        "baseUrl": cfg.base_url,
    }))
}

/// Resolve the effective LLM config for a request: built-in defaults → `PROCESSOS_LLM_*`
/// env → the active operator profile → per-request override. Each later layer wins when
/// present, so the console is authoritative over the environment while a one-off request
/// body can still override everything.
fn resolve_llm(state: &AppState, req: Option<&LlmOverride>) -> LlmConfig {
    let mut cfg = LlmConfig::from_env().with_override(&state.settings.snapshot().as_llm_override());
    if let Some(o) = req {
        cfg = cfg.with_override(o);
    }
    cfg
}

/// Resolve an LLM config for a **specific named profile** (not the active one): env defaults →
/// the named profile's settings → an optional one-off override. Used for Pair AI reviewers,
/// which target their own profile independent of the operator's active primary profile. Returns
/// `None` if `profile_id` is given but no such profile exists.
fn resolve_llm_for_profile(
    state: &AppState,
    profile_id: Option<&str>,
    ovr: Option<&LlmOverride>,
) -> Option<LlmConfig> {
    let mut cfg = LlmConfig::from_env();
    if let Some(pid) = profile_id.map(str::trim).filter(|s| !s.is_empty()) {
        let snap = state.settings.snapshot();
        let p = snap.profiles.iter().find(|p| p.id == pid)?;
        cfg = cfg.with_override(&p.as_llm_override());
    }
    if let Some(o) = ovr {
        cfg = cfg.with_override(o);
    }
    Some(cfg)
}

/// Build the Pair AI reviewer pipeline from a chat request. An explicit `pairs` chain wins;
/// otherwise the single `pair` (if enabled) becomes a one-stage chain. Each enabled stage must
/// resolve to a ready LLM config and a pairing persona, else the whole turn is rejected (the
/// operator asked for a reviewer — failing loudly beats silently dropping it).
fn build_pair_stages(
    state: &AppState,
    req: &ChatSendRequest,
) -> Result<Vec<investigate::PairStage>, String> {
    let specs: Vec<&PairRequest> = if !req.pairs.is_empty() {
        req.pairs.iter().collect()
    } else {
        req.pair.iter().collect()
    };
    let mut stages = Vec::new();
    for spec in specs.into_iter().filter(|p| p.enabled) {
        let cfg = resolve_llm_for_profile(state, spec.profile_id.as_deref(), spec.llm.as_ref())
            .ok_or_else(|| {
                format!(
                    "Pair AI reviewer references unknown profile '{}'",
                    spec.profile_id.as_deref().unwrap_or("")
                )
            })?;
        if !cfg.is_ready() {
            return Err(
                "Pair AI is enabled but its reviewer has no model configured — pick a profile \
                 for the Pair AI in the composer, or disable Pair AI"
                    .to_string(),
            );
        }
        let (id, name, system) = state.personas.resolve_pair(spec.persona_id.as_deref());
        // An inline pairing prompt/name (self-contained) wins over the resolved persona.
        let system = spec
            .system
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or(system);
        let name = spec
            .name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or(name);
        stages.push(investigate::PairStage {
            id,
            name,
            cfg,
            system,
            tools: spec.tools.clone(),
        });
    }
    Ok(stages)
}

/// Build the optional subagent (delegation) from a chat request. When enabled, the primary may
/// call the `delegate` tool to hand a self-contained research task to this worker, which runs in
/// its own context with read-only tools and returns a capped digest. A disabled/absent request
/// ⇒ no subagent (the `delegate` tool is not offered). Errors only if explicitly enabled but the
/// referenced profile can't resolve to a ready model.
fn build_subagent(
    state: &AppState,
    req: &ChatSendRequest,
) -> Result<Option<investigate::SubAgent>, String> {
    let Some(spec) = req.subagent.as_ref().filter(|s| s.enabled) else {
        return Ok(None);
    };
    let cfg = resolve_llm_for_profile(state, spec.profile_id.as_deref(), spec.llm.as_ref())
        .ok_or_else(|| {
            format!(
                "Subagent references unknown profile '{}'",
                spec.profile_id.as_deref().unwrap_or("")
            )
        })?;
    if !cfg.is_ready() {
        return Err(
            "Subagent is enabled but has no model configured — pick a profile for the \
                    subagent, or disable delegation"
                .to_string(),
        );
    }
    let (_id, name, system) = state.personas.resolve_subagent(spec.persona_id.as_deref());
    let system = spec
        .system
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or(system);
    let name = spec
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or(name);
    Ok(Some(investigate::SubAgent {
        cfg,
        name,
        system,
        max_rounds: spec.max_rounds.unwrap_or(8).clamp(1, 30),
        digest_cap: spec.digest_cap.unwrap_or(4000).clamp(500, 20000),
        src: None,
        limit: 0,
        model: None,
    }))
}

/// Build the optional drafter from a chat request. When enabled, the primary may call the
/// `draft_ir` tool to commission grammar-constrained model IR from this (typically small, local)
/// sidecar. A disabled/absent request ⇒ no drafter (the `draft_ir` tool is not offered). Errors
/// only if explicitly enabled but the referenced profile can't resolve to a ready model.
fn build_drafter(
    state: &AppState,
    req: &ChatSendRequest,
) -> Result<Option<investigate::Drafter>, String> {
    let Some(spec) = req.drafter.as_ref().filter(|s| s.enabled) else {
        return Ok(None);
    };
    let cfg = resolve_llm_for_profile(state, spec.profile_id.as_deref(), spec.llm.as_ref())
        .ok_or_else(|| {
            format!(
                "Drafter references unknown profile '{}'",
                spec.profile_id.as_deref().unwrap_or("")
            )
        })?;
    if !cfg.is_ready() {
        return Err(
            "Drafter is enabled but has no model configured — pick a profile for the \
                    drafter, or disable it"
                .to_string(),
        );
    }
    let system = spec
        .system
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_default();
    let name = spec
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| "drafter".to_string());
    Ok(Some(investigate::Drafter {
        cfg,
        name,
        system,
        model: None,
    }))
}

/// Resolve the loop-monitor config from a chat request, honouring an env default. Returns the
/// `(LlmConfig, persona_system)` to run the monitor with, or `None` when monitoring is off or no
/// model can be resolved (in which case the turn simply runs without a monitor).
///
/// Enablement: the request's `monitor.enabled` wins; absent that, `PROCESSOS_MONITOR` set to a
/// truthy value (or a profile id) enables it so the monitor can be used without UI.
fn resolve_monitor(state: &AppState, mon: Option<&MonitorRequest>) -> Option<(LlmConfig, String)> {
    let env = std::env::var("PROCESSOS_MONITOR").ok();
    let (enabled, profile_id, ovr, persona_id, system_override) = match mon {
        Some(m) if m.enabled => (
            true,
            m.profile_id.clone(),
            m.llm.clone(),
            m.persona_id.clone(),
            m.system.clone(),
        ),
        _ => {
            // Env fallback: "0"/"off"/"false"/"" disable; anything else enables (and a non-bool
            // value is treated as the profile id to run the monitor on).
            let raw = env.as_deref().map(str::trim).unwrap_or("");
            let off = matches!(
                raw.to_ascii_lowercase().as_str(),
                "" | "0" | "off" | "false" | "no"
            );
            if off {
                return None;
            }
            let is_bool = matches!(
                raw.to_ascii_lowercase().as_str(),
                "1" | "on" | "true" | "yes"
            );
            let profile = if is_bool { None } else { Some(raw.to_string()) };
            (true, profile, None, None, None)
        }
    };
    if !enabled {
        return None;
    }
    let cfg = resolve_llm_for_profile(state, profile_id.as_deref(), ovr.as_ref())?;
    if !cfg.is_ready() {
        return None;
    }
    let (_id, system) = state.personas.resolve_monitor(persona_id.as_deref());
    // An inline pairing prompt (self-contained) wins over the resolved persona.
    let system = system_override
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or(system);
    Some((cfg, system))
}

/// How often the loop monitor re-reads the primary's transcript, and how many times it may steer
/// before escalating to a forced wrap-up. Deliberately conservative so the monitor is cheap and
/// can never itself spam the conversation.
const MONITOR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(12);
const MONITOR_MAX_STEERS: usize = 3;

/// The out-of-band loop-monitor watcher. Polls the (incrementally persisted) transcript on a
/// fixed cadence; when its model says the primary is circling it pushes a steer into the running
/// turn's steer queue, and after its steer budget is spent it flips the cancel flag to force a
/// graceful wrap-up. It never blocks the primary and any monitor error is swallowed (the monitor
/// failing must never take down the investigation).
#[allow(clippy::too_many_arguments)]
async fn run_loop_monitor(
    cfg: LlmConfig,
    persona_system: String,
    chat: Arc<chat::ChatStore>,
    key: String,
    sid: String,
    steer: Arc<std::sync::Mutex<Vec<String>>>,
    cancel: Arc<std::sync::atomic::AtomicBool>,
    live: LiveTurn,
    done: Arc<std::sync::atomic::AtomicBool>,
    // Endpoint of the PRIMARY model this monitor governs (for the reasoning-control call).
    primary_base_url: String,
    // The primary turn's live completion id (`chatcmpl-…`), set once it starts replying.
    primary_cmpl_id: Arc<std::sync::Mutex<Option<String>>>,
    // Shared with the primary's Debug capture: each monitor evaluation is appended here (tagged
    // `kind:"monitor"`) so the cockpit's Debug tab can show the second model's full exchange.
    dbg: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
) {
    use std::sync::atomic::Ordering;
    // Read the primary's current live completion id, if it has started replying.
    let live_cmpl = |cell: &Arc<std::sync::Mutex<Option<String>>>| -> Option<String> {
        cell.lock().ok().and_then(|c| c.clone())
    };
    let mut policy = monitor::MonitorPolicy::new(MONITOR_MAX_STEERS);
    let mut last_window = String::new();
    loop {
        tokio::time::sleep(MONITOR_INTERVAL).await;
        if done.load(Ordering::Relaxed) || cancel.load(Ordering::Relaxed) {
            return;
        }
        let messages = chat.get(&key, &sid).map(|s| s.messages).unwrap_or_default();
        let window = monitor::render_window(&messages);
        // Skip when the transcript hasn't advanced since the last evaluation — no new behaviour
        // to judge, so don't spend a monitor call (and don't risk re-flagging the same state).
        if window.is_empty() || window == last_window {
            continue;
        }
        last_window = window.clone();
        let eval = monitor::evaluate(&cfg, &persona_system, &window).await;
        let verdict = eval.verdict.clone();
        // The primary may have finished while the monitor was thinking.
        if done.load(Ordering::Relaxed) || cancel.load(Ordering::Relaxed) {
            return;
        }
        let action = policy.observe(&verdict);
        // What (if anything) the monitor injected into the PRIMARY this round, and a label.
        let (action_label, injected): (&str, Option<String>) = match &action {
            monitor::MonitorAction::None => ("none", None),
            monitor::MonitorAction::Steer(text) => {
                ("steer", Some(format!("[loop monitor] {text}")))
            }
            monitor::MonitorAction::WrapUp(reason) => ("wrapup", Some(reason.clone())),
        };
        // Record this evaluation for the Debug tab: the exact request the second model received,
        // its raw reply, the parsed verdict, and what (if anything) reached the primary. Build it
        // once (cloning the verdict fields so the match arms below can still use them) and reuse it
        // for both the persisted buffer and the live wire mirror.
        let ts = chat_now_ms();
        let record = serde_json::json!({
            "kind": "monitor",
            "ts": ts,
            "action": action_label,
            "verdict": {
                "circling": verdict.circling,
                "reason": verdict.reason.clone(),
                "steer": verdict.steer.clone(),
            },
            "injected": injected,
            "raw": eval.raw,
            "body": eval.request,
        });
        if let Ok(mut d) = dbg.lock() {
            d.push(record.clone());
        }
        // Mirror it onto the live wire so the Debug tab updates without waiting for turn-end.
        let mut live_record = record;
        live_record["type"] = serde_json::json!("monitorRequest");
        live.emit(live_record);
        match action {
            monitor::MonitorAction::None => {}
            monitor::MonitorAction::Steer(text) => {
                if let Ok(mut q) = steer.lock() {
                    q.push(format!("[loop monitor] {text}"));
                }
                // If the primary supports reasoning control, end its current (circling) thinking
                // now so the steer lands sooner instead of waiting out the whole think budget.
                if let Some(id) = live_cmpl(&primary_cmpl_id) {
                    let _ = crate::reasoning::end_reasoning(&primary_base_url, &id).await;
                }
                live.emit(serde_json::json!({
                    "type": "monitor",
                    "action": "steer",
                    "reason": verdict.reason,
                    "steer": text,
                }));
            }
            monitor::MonitorAction::WrapUp(reason) => {
                cancel.store(true, Ordering::Relaxed);
                // Prefer a mid-thinking halt via the reasoning-control surface (ends the live
                // turn's reasoning so it answers now); the cancel flag is the round-boundary
                // fallback on builds without the surface or before the turn has a completion id.
                if let Some(id) = live_cmpl(&primary_cmpl_id) {
                    let _ = crate::reasoning::end_reasoning(&primary_base_url, &id).await;
                }
                live.emit(serde_json::json!({
                    "type": "monitor",
                    "action": "wrapup",
                    "reason": reason,
                }));
                return;
            }
        }
    }
}

/// Request body for an investigation: optional LLM override + bounds.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InvestigateRequest {
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// Max instances to load into the analytic view (default 100k).
    #[serde(default)]
    limit: Option<usize>,
    /// Max model↔tool rounds before giving up (default 12).
    #[serde(default)]
    max_rounds: Option<usize>,
    /// Offer the trusted Python escape hatch (`run_python`) in addition to the SQL tool.
    /// Off by default; gated behind the SQL tool by prompt discipline. The tool runs
    /// ARBITRARY operator-trusted code (timeout + private workdir, not a sandbox).
    #[serde(default)]
    allow_python: bool,
}

/// `POST /api/workspaces/{workspace}/processes/{process}/investigate` — point
/// the configured LLM at this process's bound trace source and let it form and test its
/// own hypotheses over the data via the read-only `query_traces` SQL tool. Returns the
/// agent's conclusion plus the full query/result "lab notebook".
async fn ws_process_investigate(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Json(req): Json<InvestigateRequest>,
) -> impl IntoResponse {
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return unprocessable(
            "no LLM model configured; set it in the console settings (cog, lower-left) or \
             via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER \
             as needed), or pass an `llm` object with at least `model` in the request body"
                .to_string(),
        );
    }
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let limit = req.limit.unwrap_or(100_000).clamp(1, 1_000_000);
    let max_rounds = req.max_rounds.unwrap_or(12).clamp(1, 40);
    let allow_python = req.allow_python;
    let py = state.settings.snapshot().py_config();
    // The DuckDB connection inside the analysis is !Send, so the investigation cannot
    // cross the multi-threaded handler's await boundary. Run the whole loop on a
    // dedicated current-thread runtime where the connection never leaves its thread.
    let task = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        rt.block_on(investigate::run_investigation(
            &src,
            cfg,
            py,
            limit,
            max_rounds,
            allow_python,
        ))
    })
    .await;
    match task {
        Ok(Ok(report)) => Json(report).into_response(),
        Ok(Err(e)) => bad_gateway(e),
        Err(e) => bad_gateway(format!("investigation task failed: {e}")),
    }
}

// --- Interactive cockpit chat ----------------------------------------------------

/// Query string carrying the target chat session id (shared by load/stream/wrapup).
#[derive(Debug, Default, Deserialize)]
struct SessionQuery {
    #[serde(default)]
    session: Option<String>,
}

/// Query for the transcript export: `mode` (`chat`|`debug`) and `format` (`md`|`txt`).
#[derive(Debug, Default, Deserialize)]
struct ExportQuery {
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    format: Option<String>,
}

/// Per-session wrap-up flag key, so an in-flight turn in one session can be wrapped up
/// without touching another session of the same dataset.
fn chat_cancel_key(key: &str, session_id: &str) -> String {
    format!("{key}::{session_id}")
}

/// Fetch the current-revision semantic annotations for a process, or `None`
/// if no sidecar exists / the current pointer is missing. Used to plumb
/// annotations into `run_chat_turn` so the `read_annotations` tool and
/// `simulate`'s cost/time deltas are available in chat turns.
fn current_annotations(
    state: &AppState,
    workspace: &str,
    process: &str,
) -> Option<crate::layout::SemanticAnnotations> {
    let side = state
        .workspaces
        .read_annotations(workspace, process)
        .ok()
        .flatten()?;
    let current = side.current.as_deref()?;
    side.revisions.get(current).map(|r| r.annotations.clone())
}

/// Resolve the session to act on: the explicit `?session=` id if it exists, else the most
/// recently active session, creating a first one if the dataset has none yet.
fn resolve_session(state: &AppState, key: &str, wanted: Option<&str>) -> chat::SessionMeta {
    if let Some(id) = wanted {
        if let Some(s) = state.chat.get(key, id) {
            return chat::SessionMeta {
                id: s.id,
                name: s.name,
                created: s.created,
                updated: s.updated,
                turns: 0,
                persona: s.persona,
                models: s.models,
                origin: s.origin,
                imported_from: s.imported_from,
            };
        }
    }
    let mut list = state.chat.list(key);
    if let Some(first) = list.drain(..).next() {
        return first;
    }
    let s = state.chat.create(key, None);
    chat::SessionMeta {
        id: s.id,
        name: s.name,
        created: s.created,
        updated: s.updated,
        turns: 0,
        persona: s.persona,
        models: s.models,
        origin: s.origin,
        imported_from: s.imported_from,
    }
}

/// `GET .../chat/sessions` — list this dataset's chat sessions (newest activity first),
/// creating an initial one so the cockpit always has a session to open.
async fn cockpit_chat_sessions_list(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let mut sessions = state.chat.list(&key);
    if sessions.is_empty() {
        state.chat.create(&key, None);
        sessions = state.chat.list(&key);
    }
    Json(serde_json::json!({ "sessions": sessions })).into_response()
}

/// Body for creating / renaming a chat session.
#[derive(Debug, Default, Deserialize)]
struct ChatSessionNameRequest {
    #[serde(default)]
    name: Option<String>,
}

/// `POST .../chat/sessions` — start a new (empty) chat session.
async fn cockpit_chat_sessions_create(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    body: Option<Json<ChatSessionNameRequest>>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let name = body.and_then(|Json(b)| b.name);
    let s = state.chat.create(&key, name);
    Json(serde_json::json!({
        "id": s.id, "name": s.name, "created": s.created, "updated": s.updated, "turns": 0,
    }))
    .into_response()
}

/// `GET .../chat/sessions/{id}` — load one session's transcript as timestamped turns.
async fn cockpit_chat_session_load(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    match state.chat.get(&key, &id) {
        Some(s) => Json(serde_json::json!({
            "id": s.id,
            "name": s.name,
            "turns": chat::render_view_full(&s.messages, &s.stamps, &s.turn_models),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response(),
    }
}

/// `POST .../chat/sessions/{id}/rename` — rename a chat session.
async fn cockpit_chat_session_rename(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
    Json(req): Json<ChatSessionNameRequest>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let name = req.name.unwrap_or_default();
    if name.trim().is_empty() {
        return unprocessable("name must not be empty".to_string());
    }
    if state.chat.rename(&key, &id, &name) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response()
    }
}

/// `GET .../chat/sessions/{id}/debug` — the exact model request payloads sent during this
/// session's most recent turn (one per round): system prompt, tool specs, and the full
/// transcript. Powers the cockpit's per-chat "Debug" tab so the operator can see everything
/// the model receives, not just their latest message. Empty until a turn has run this session.
async fn cockpit_chat_session_debug(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let cancel_key = chat_cancel_key(&key, &id);
    let requests = state.chat_debug.load(&cancel_key);
    Json(serde_json::json!({ "sessionId": id, "requests": requests }))
}

/// `GET .../chat/sessions/{id}/simulations` — the Alternate Reality Engine runs
/// (`simulate` / `compare_variants`) recorded in this session's persisted transcript, each
/// pairing the candidate model(s) with their fidelity scorecard. Powers the cockpit's
/// "Simulations" tab so the operator can watch the droid's exploration — the variants it forked
/// and how each scored. Reads the persisted transcript, so it survives restarts (unlike Debug).
async fn cockpit_chat_session_simulations(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let runs = state
        .chat
        .get(&key, &id)
        .map(|s| chat::extract_simulations(&s.messages))
        .unwrap_or_default();
    Json(serde_json::json!({ "sessionId": id, "runs": runs }))
}

// ── Editable model Workbench (ADR 0011) ──────────────────────────────────────────────
//
// The investigation already owns the validate → simulate/compare → ranked-scorecard loop, but
// only the LLM could drive it (those tools live behind the agent's tool surface). The Workbench
// exposes the SAME deterministic machinery to a human-edited bpmn-js Modeler canvas over plain
// HTTP, so an operator-authored variant is scored by the identical harness as an agent variant
// and lands in the same Simulations tab — tagged by provenance.

/// A request body that carries only a candidate BPMN document.
#[derive(Debug, Deserialize)]
struct WorkbenchXmlRequest {
    xml: String,
}

/// `POST .../workbench/validate` — the instant, token-free lint-before-simulate gate. Runs the
/// same deterministic checks the agent's `validate_model` tool uses (`validate_model`, which folds
/// in `normalize_authoring` + `lint_task_definition_attribute`) and also returns the normalized
/// XML so the canvas can offer to apply the heal. The cockpit blocks Simulate until `valid` is
/// true with no error-severity finding.
async fn cockpit_workbench_validate(Json(req): Json<WorkbenchXmlRequest>) -> impl IntoResponse {
    let report = match crate::bpmn_model::validate_model(&req.xml) {
        Ok(v) => v,
        Err(e) => return unprocessable(e),
    };
    let (normalized, fixes) = crate::bpmn_model::normalize_authoring(&req.xml);
    // Deployable iff it parses AND carries no error-severity finding.
    let has_error = report
        .get("findings")
        .and_then(|f| f.as_array())
        .map(|a| {
            a.iter()
                .any(|f| f.get("severity").and_then(|s| s.as_str()) == Some("error"))
        })
        .unwrap_or(false);
    let deployable = report
        .get("valid")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        && !has_error;
    Json(serde_json::json!({
        "deployable": deployable,
        "report": report,
        "normalizedXml": normalized,
        "autoFixes": fixes,
    }))
    .into_response()
}

/// `GET .../workbench/baseline` — the model to fork onto a blank canvas: the process's
/// recorded/deployed BPMN. The frontend also forks suggested-model cards directly (they carry
/// their own XML), so this is the "start from the real model" entry point.
async fn cockpit_workbench_baseline(
    State(state): State<AppState>,
    Path((workspace, process, _id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    match state.workspaces.read_model(&workspace, &process) {
        Some(xml) => Json(serde_json::json!({ "xml": xml, "name": process })).into_response(),
        None => unprocessable(format!(
            "process '{process}' has no recorded BPMN model to fork; draw a new model on the canvas \
             instead"
        )),
    }
}

/// Apply-an-edit request: a base document plus structured `edit_model` operations.
#[derive(Debug, Deserialize)]
struct WorkbenchApplyRequest {
    xml: String,
    ops: Vec<serde_json::Value>,
}

/// `POST .../workbench/apply-edit` — deterministically apply structured `edit_model` ops to the
/// canvas XML (used by "Apply suggestion" when the agent returns counter-edits, and reusable for
/// any structured authoring). Returns the engine-validated resulting XML.
async fn cockpit_workbench_apply_edit(Json(req): Json<WorkbenchApplyRequest>) -> impl IntoResponse {
    match crate::bpmn_model::edit_model(&req.xml, &req.ops) {
        Ok(v) => Json(v).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// Simulate request: a candidate document plus optional name, staged `limit`, and `mockWorkers`
/// for job types with no recorded history (`requiresNewWorkers`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkbenchSimulateRequest {
    xml: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    mock_workers: Option<serde_json::Value>,
}

/// `POST .../workbench/simulate` — replay a human-authored variant against THIS investigation's
/// recorded dataset, via the exact `experiment::simulate` path the agent's tool uses. The run is
/// appended to the session transcript (so it surfaces in the Simulations tab and persists across
/// restarts) tagged `authoredBy: "user"`. Staged with `limit` (1 → 25 → full) like the agent.
async fn cockpit_workbench_simulate(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
    Json(req): Json<WorkbenchSimulateRequest>,
) -> impl IntoResponse {
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let model = state.workspaces.read_model(&workspace, &process);
    let name = req
        .name
        .clone()
        .unwrap_or_else(|| "workbench variant".to_string());
    let mut args = serde_json::json!({ "model": req.xml, "name": name });
    if let Some(l) = req.limit {
        args["limit"] = serde_json::json!(l);
    }
    if let Some(m) = &req.mock_workers {
        args["mockWorkers"] = m.clone();
    }
    let args_for_record = args.clone();
    let ann = state
        .workspaces
        .read_annotations(&workspace, &process)
        .ok()
        .flatten()
        .and_then(|s| {
            s.current
                .clone()
                .and_then(|k| s.revisions.get(&k).map(|r| r.annotations.clone()))
        });
    let result = run_dataset_op(src, move |ds| {
        crate::experiment::simulate(model.as_deref(), &ds, &args, ann.as_ref())
    })
    .await;
    match result {
        Ok(v) => {
            append_workbench_run(
                &state,
                &workspace,
                &process,
                &id,
                "simulate",
                args_for_record,
                &v,
                &format!("Workbench: simulated user-authored variant “{name}”."),
            );
            Json(v).into_response()
        }
        Err(e) => bad_gateway(e),
    }
}

/// Compare request: the human variant, plus an optional best agent candidate to rank against.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkbenchCompareRequest {
    xml: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
    /// The current best agent-authored candidate's XML, so the human edit is ranked head-to-head
    /// against it as well as the baseline. Optional.
    #[serde(default)]
    agent_best: Option<String>,
    #[serde(default)]
    agent_best_name: Option<String>,
}

/// `POST .../workbench/compare` — rank the human variant against the baseline (and, when supplied,
/// the best agent candidate) on the same recorded dataset via `experiment::compare_variants`. The
/// ranked run is appended to the transcript tagged `authoredBy: "user"`.
async fn cockpit_workbench_compare(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
    Json(req): Json<WorkbenchCompareRequest>,
) -> impl IntoResponse {
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let model = state.workspaces.read_model(&workspace, &process);
    let name = req
        .name
        .clone()
        .unwrap_or_else(|| "workbench variant".to_string());
    let mut candidates = vec![serde_json::json!({ "name": name, "model": req.xml })];
    if let Some(base) = &model {
        candidates.push(serde_json::json!({ "name": "baseline (recorded)", "model": base }));
    }
    if let Some(best) = &req.agent_best {
        let bn = req
            .agent_best_name
            .clone()
            .unwrap_or_else(|| "agent best".to_string());
        candidates.push(serde_json::json!({ "name": bn, "model": best }));
    }
    let mut args = serde_json::json!({ "candidates": candidates });
    if let Some(l) = req.limit {
        args["limit"] = serde_json::json!(l);
    }
    let args_for_record = args.clone();
    let model_cloned = model.clone();
    let ann = state
        .workspaces
        .read_annotations(&workspace, &process)
        .ok()
        .flatten()
        .and_then(|s| {
            s.current
                .clone()
                .and_then(|k| s.revisions.get(&k).map(|r| r.annotations.clone()))
        });
    let result = run_dataset_op(src, move |ds| {
        crate::experiment::compare_variants(model_cloned.as_deref(), &ds, &args, ann.as_ref())
    })
    .await;
    match result {
        Ok(v) => {
            append_workbench_run(
                &state,
                &workspace,
                &process,
                &id,
                "compare_variants",
                args_for_record,
                &v,
                &format!("Workbench: compared user-authored variant “{name}”."),
            );
            Json(v).into_response()
        }
        Err(e) => bad_gateway(e),
    }
}

/// Agent-review request: the current canvas plus an optional persona/LLM and instruction.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkbenchReviewRequest {
    xml: String,
    #[serde(default)]
    persona_id: Option<String>,
    #[serde(default)]
    llm: Option<LlmOverride>,
    #[serde(default)]
    instruction: Option<String>,
}

/// `POST .../workbench/review` — ask an LLM (investigator or a chosen persona) to review the
/// canvas. The turn runs through the normal `run_chat_turn` machinery (so it persists in the
/// conversation and can itself simulate), and if the agent proposes structured `edit_model` ops we
/// surface them as an applyable diff: `{ answer, ops, resultingXml }`. Counter-edits flow back to
/// the canvas via `apply-edit`.
async fn cockpit_workbench_review(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
    Json(req): Json<WorkbenchReviewRequest>,
) -> impl IntoResponse {
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return unprocessable("no LLM model configured for the review".to_string());
    }
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let py = state.settings.snapshot().py_config();
    let objective = state
        .workspaces
        .get_process(&workspace, &process)
        .and_then(|p| p.config.objective);
    let model = state.workspaces.read_model(&workspace, &process);
    let annotations = current_annotations(&state, &workspace, &process);
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, Some(&id)).id;
    let (persona_id, persona_system) = state.personas.resolve(req.persona_id.as_deref());
    let session = state.chat.get(&key, &sid).unwrap_or_default();
    if session.messages.is_empty() {
        state.chat.set_persona(&key, &sid, &persona_id);
    }
    state.chat.record_model(&key, &sid, &cfg.model);
    let prior = session.messages;
    let prior_stamps = session.stamps;
    let prior_turn_models = session.turn_models;
    let model_label = cfg.model.clone();
    let user_ts = chat_now_ms();
    let instruction = req.instruction.clone().unwrap_or_else(|| {
        "Review the BPMN model below that I drew on the workbench canvas. Assess it for structural \
         defects and whether it improves on the recorded baseline against this investigation's \
         dataset. If you can improve it, call edit_model with `base` set to exactly this XML and \
         return concrete ops. Then briefly summarise your assessment."
            .to_string()
    });
    let message = format!("{instruction}\n\n```xml\n{}\n```", req.xml);
    let task = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        let mut sink = |_ev: agent::AgentEvent| {};
        rt.block_on(investigate::run_chat_turn(
            &src,
            cfg,
            py,
            100_000,
            12,
            false,
            objective.as_deref(),
            Some(&persona_system),
            model,
            annotations,
            None,
            None,
            &mut sink,
            &[],
            None,
            None,
            Vec::new(),
            None,
            prior,
            &message,
            None,
        ))
    })
    .await;
    match task {
        Ok(Ok(result)) => {
            let stamps =
                chat::extend_stamps(&result.messages, prior_stamps, user_ts, chat_now_ms());
            let turn_models =
                chat::extend_turn_models(&result.messages, prior_turn_models, &model_label);
            // Pull out the agent's last edit_model proposal, if any, as an applyable diff.
            let (ops, resulting_xml) = last_edit_model(&result.messages);
            state
                .chat
                .save(&key, &sid, result.messages.clone(), stamps.clone());
            state.chat.set_turn_models(&key, &sid, turn_models.clone());
            Json(serde_json::json!({
                "sessionId": sid,
                "answer": result.answer,
                "ops": ops,
                "resultingXml": resulting_xml,
                "turns": chat::render_view_full(&result.messages, &stamps, &turn_models),
            }))
            .into_response()
        }
        Ok(Err(e)) => bad_gateway(e),
        Err(e) => bad_gateway(format!("review task failed: {e}")),
    }
}

/// Scan a finished transcript for the agent's most recent `edit_model` tool call and its result,
/// returning the ops it applied and the resulting XML (so the canvas can offer "Apply suggestion").
fn last_edit_model(messages: &[agent::Msg]) -> (serde_json::Value, Option<String>) {
    use std::collections::HashMap;
    let mut results: HashMap<&str, &str> = HashMap::new();
    for m in messages {
        if let agent::Msg::Tool { call_id, content } = m {
            results.insert(call_id.as_str(), content.as_str());
        }
    }
    let mut found = (serde_json::Value::Null, None);
    for m in messages {
        if let agent::Msg::Assistant { tool_calls, .. } = m {
            for tc in tool_calls {
                if tc.name != "edit_model" {
                    continue;
                }
                let ops = tc
                    .arguments
                    .get("ops")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let xml = results.get(tc.id.as_str()).and_then(|c| {
                    serde_json::from_str::<serde_json::Value>(c)
                        .ok()
                        .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(String::from))
                });
                if xml.is_some() {
                    found = (ops, xml);
                }
            }
        }
    }
    found
}

/// Run a closure that needs the recorded dataset on a dedicated current-thread runtime. The
/// dataset is distilled from the (Send) `TraceSource`; the closure (`simulate`/`compare_variants`)
/// is synchronous and CPU-bound, so it runs off the async executor like `run_investigation`.
async fn run_dataset_op<F>(src: dataset::TraceSource, f: F) -> Result<serde_json::Value, String>
where
    F: FnOnce(crate::experiment::RecordedDataset) -> Result<serde_json::Value, String>
        + Send
        + 'static,
{
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        let cap = crate::experiment::recorded_cap(&src);
        let ds = rt.block_on(crate::experiment::build_recorded_dataset(&src, cap));
        f(ds)
    })
    .await
    .map_err(|e| format!("dataset op failed: {e}"))?
}

/// Append a Workbench simulate/compare run to a session transcript so it renders in the
/// Simulations tab beside agent runs. We write a self-contained mini-turn — an assistant tool
/// call, its result, and a short note that flushes the timeline — so `extract_simulations` picks
/// it up and the leftover timeline never leaks into the next real turn. The `authoredBy` field in
/// the recorded arguments carries provenance.
#[allow(clippy::too_many_arguments)]
fn append_workbench_run(
    state: &AppState,
    workspace: &str,
    process: &str,
    sid: &str,
    tool: &str,
    mut arguments: serde_json::Value,
    result: &serde_json::Value,
    note: &str,
) {
    let key = chat::session_key(workspace, process);
    let mut session = state.chat.get(&key, sid).unwrap_or_default();
    if let Some(obj) = arguments.as_object_mut() {
        obj.insert("authoredBy".into(), serde_json::json!("user"));
    }
    let call_id = format!("wb-{}-{}", tool, chat_now_ms());
    let result_str = serde_json::to_string(result).unwrap_or_else(|_| "{}".to_string());
    session.messages.push(agent::Msg::Assistant {
        text: None,
        tool_calls: vec![agent::ToolCall {
            id: call_id.clone(),
            name: tool.to_string(),
            arguments,
        }],
    });
    session.messages.push(agent::Msg::Tool {
        call_id,
        content: result_str,
    });
    session.messages.push(agent::Msg::Assistant {
        text: Some(note.to_string()),
        tool_calls: Vec::new(),
    });
    let now = chat_now_ms();
    let stamps = chat::extend_stamps(&session.messages, session.stamps.clone(), now, now);
    let turn_models =
        chat::extend_turn_models(&session.messages, session.turn_models.clone(), "workbench");
    state.chat.save(&key, sid, session.messages.clone(), stamps);
    state.chat.set_turn_models(&key, sid, turn_models);
}

/// `GET .../chat/sessions/{id}/export` — download this session's transcript as Markdown or
/// plain text. `mode=chat` (default) is the clean conversation; `mode=debug` adds each turn's
/// reasoning timeline and tool calls (arguments + results). `format=md` (default) or `txt`.
async fn cockpit_chat_session_export(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
    Query(q): Query<ExportQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let Some(s) = state.chat.get(&key, &id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response();
    };
    let verbose = q.mode.as_deref() == Some("debug");
    let markdown = !matches!(
        q.format.as_deref(),
        Some("txt") | Some("text") | Some("plain")
    );
    let turns = chat::render_view_full(&s.messages, &s.stamps, &s.turn_models);
    let body = chat::export_transcript(&s.name, &turns, markdown, verbose);
    let ext = if markdown { "md" } else { "txt" };
    let ctype = if markdown {
        "text/markdown; charset=utf-8"
    } else {
        "text/plain; charset=utf-8"
    };
    let kind = if verbose { "debug" } else { "chat" };
    let slug = slugify(&s.name, &id);
    let base = slug.strip_prefix("investigation-").unwrap_or(&slug);
    let filename = format!("investigation-{base}-{kind}.{ext}");
    (
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_TYPE, ctype.to_string()),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (axum::http::header::CACHE_CONTROL, NO_CACHE.to_string()),
        ],
        body,
    )
        .into_response()
}

/// A filesystem-safe slug for an export filename, falling back to the session id.
fn slugify(name: &str, fallback_id: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let s = s.trim_matches('-').to_string();
    let collapsed = s
        .split('-')
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if collapsed.is_empty() {
        fallback_id.to_string()
    } else {
        collapsed.chars().take(60).collect()
    }
}

/// `GET .../chat/sessions/{id}/trace` — export a **psychological trace**: a single shareable zip
/// bundling the investigation transcript, the LLM profiles that drove it (API keys redacted), the
/// persona, and a manifest naming the dataset + model. A Forward-Deployed Engineer can download
/// this and Slack it back for remote replay/debugging (see [`crate::trace`]).
async fn cockpit_chat_session_trace_export(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let Some(s) = state.chat.get(&key, &id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response();
    };

    // Resolve (and redact) the LLM profiles that drove this session — matched by the model label
    // recorded per turn against each profile's model / name / id.
    let snapshot = state.settings.snapshot();
    let llm_profiles: Vec<serde_json::Value> = snapshot
        .profiles
        .iter()
        .filter(|p| {
            let model = p.model.as_deref();
            s.models
                .iter()
                .any(|lbl| Some(lbl.as_str()) == model || lbl == &p.name || lbl == &p.id)
        })
        .map(trace::redact_profile)
        .collect();

    let persona_prompt = state
        .personas
        .get(&s.persona)
        .map(|p| p.system)
        .unwrap_or_default();
    let turns = chat::render_view_full(&s.messages, &s.stamps, &s.turn_models);
    let transcript = chat::export_transcript(&s.name, &turns, true, true);
    let debug_requests = state.chat_debug.load(&chat_cancel_key(&key, &id));

    let manifest = trace::TraceManifest {
        schema: trace::SCHEMA.to_string(),
        processos_version: env!("PROCESSOS_VERSION").to_string(),
        exported_at: chat_now_ms(),
        workspace: workspace.clone(),
        process: process.clone(),
        session_id: s.id.clone(),
        session_name: s.name.clone(),
        persona: s.persona.clone(),
        models: s.models.clone(),
        turns: turns.len(),
        debug_rounds: debug_requests.len(),
        llm_profiles,
    };

    let zip = match trace::build_zip(
        &manifest,
        &s,
        &transcript,
        &persona_prompt,
        &serde_json::Value::Array(debug_requests),
    ) {
        Ok(bytes) => bytes,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("build trace: {e}") })),
            )
                .into_response();
        }
    };

    let slug = slugify(&s.name, &id);
    let base = slug.strip_prefix("investigation-").unwrap_or(&slug);
    let filename = format!("psych-trace-{base}.zip");
    (
        StatusCode::OK,
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/zip".to_string(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (axum::http::header::CACHE_CONTROL, NO_CACHE.to_string()),
        ],
        zip,
    )
        .into_response()
}

/// `POST .../traces/import` — import a shared psychological-trace zip (raw request body) as a new
/// `"imported"`-origin investigation under the **currently viewed** `(workspace, process)`, so it
/// surfaces immediately in the recipient's session list (with the imported icon). The original
/// dataset/model/version from the manifest are kept as a provenance tooltip.
async fn cockpit_chat_trace_import(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let (manifest, session) = match trace::parse_zip(body.to_vec()) {
        Ok(parsed) => parsed,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid trace: {e}") })),
            )
                .into_response();
        }
    };

    let provenance = {
        let mut s = format!("{} / {}", manifest.workspace, manifest.process);
        if !manifest.models.is_empty() {
            s.push_str(" · ");
            s.push_str(&manifest.models.join(", "));
        }
        s.push_str(&format!(" · ProcessOS {}", manifest.processos_version));
        s
    };
    let name = if manifest.session_name.trim().is_empty() {
        "Imported investigation".to_string()
    } else {
        manifest.session_name.clone()
    };

    let key = chat::session_key(&workspace, &process);
    let stored = state.chat.import_session(
        &key,
        &name,
        session.messages,
        session.stamps,
        session.persona,
        session.models,
        session.turn_models,
        Some(provenance.clone()),
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "sessionId": stored.id,
            "name": stored.name,
            "workspace": workspace,
            "process": process,
            "source": provenance,
            "origin": manifest,
        })),
    )
        .into_response()
}

/// `DELETE .../chat/sessions/{id}` — delete a chat session.
async fn cockpit_chat_session_delete(
    State(state): State<AppState>,
    Path((workspace, process, id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    if state.chat.delete(&key, &id) {
        state.chat_debug.remove(&chat_cancel_key(&key, &id));
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no such chat session" })),
        )
            .into_response()
    }
}

/// `GET /api/workspaces/{workspace}/processes/{process}/chat` — load a chat session's
/// transcript as operator-facing turns (the `?session=` one, or the most recent).
async fn cockpit_chat_load(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let meta = resolve_session(&state, &key, q.session.as_deref());
    let session = state.chat.get(&key, &meta.id).unwrap_or_default();
    // If a turn is mid-flight for this session (e.g. the page was reloaded), surface its user
    // message so the cockpit can redraw the in-flight user bubble and reattach to the live stream.
    let cancel_key = chat_cancel_key(&key, &meta.id);
    let inflight = state.chat_live.lock().ok().and_then(|m| {
        m.get(&cancel_key)
            .map(|lt| serde_json::json!({ "user": lt.user }))
    });
    Json(serde_json::json!({
        "sessionId": meta.id,
        "turns": chat::render_view_full(&session.messages, &session.stamps, &session.turn_models),
        "inflight": inflight,
    }))
    .into_response()
}

/// `GET .../chat/stream/live` — reattach to an in-flight chat turn's event stream (e.g. after a
/// page reload). Replays the buffered events from the start, then follows along until the turn
/// ends. Returns 404 when no turn is running for the session, so the cockpit can fall back to the
/// persisted transcript.
async fn cockpit_chat_stream_live(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let meta = resolve_session(&state, &key, q.session.as_deref());
    let cancel_key = chat_cancel_key(&key, &meta.id);
    let live = state
        .chat_live
        .lock()
        .ok()
        .and_then(|m| m.get(&cancel_key).cloned());
    match live {
        Some(live) => sse_from_live(live).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no investigation is running for this session" })),
        )
            .into_response(),
    }
}

/// Request body for an interactive chat turn.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChatSendRequest {
    /// The operator's message to the droid.
    message: String,
    #[serde(default)]
    llm: Option<LlmOverride>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    max_rounds: Option<usize>,
    /// Offer the trusted Python escape hatch in addition to the SQL tool (off by default).
    #[serde(default)]
    allow_python: bool,
    /// The persona (standing system prompt) for this conversation. Only takes effect on the
    /// first turn of a session — afterwards the persona is baked into the persisted transcript.
    #[serde(default)]
    persona_id: Option<String>,
    /// Pair AI: a single reviewer agent that runs after the primary each turn (off unless
    /// `enabled`). Convenience for the cockpit's one-reviewer UI.
    #[serde(default)]
    pair: Option<PairRequest>,
    /// Pair AI (N-tier): an explicit chain of reviewer stages run in sequence after the primary,
    /// each handed the previous stage's answer. Takes precedence over `pair` when non-empty.
    #[serde(default)]
    pairs: Vec<PairRequest>,
    /// Loop monitor: a second model that watches this turn's live transcript and steers the
    /// primary when it goes in circles (off unless `enabled`).
    #[serde(default)]
    monitor: Option<MonitorRequest>,
    /// Subagent: a worker the primary can DELEGATE self-contained research tasks to, sparing its
    /// context window. Off unless `enabled`; when on, the primary gets a `delegate` tool.
    #[serde(default)]
    subagent: Option<SubagentRequest>,
    /// Drafter: a small local sidecar the primary can commission grammar-constrained model IR
    /// from. Off unless `enabled`; when on, the primary gets a `draft_ir` tool.
    #[serde(default)]
    drafter: Option<DrafterRequest>,
    /// Optional allowlist of tool names the PRIMARY model may use this turn (the per-investigation
    /// "Configure tools" selection). `None`/empty = the full available surface.
    #[serde(default)]
    tools: Option<Vec<String>>,
}

/// Subagent (delegation) configuration for a chat turn. When `enabled`, the primary is offered a
/// `delegate` tool; each call runs a read-only subagent in its own context and returns a capped
/// digest. `maxRounds`/`digestCap` are advanced tuning surfaces (sensible defaults otherwise).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubagentRequest {
    #[serde(default)]
    enabled: bool,
    /// The saved LLM profile the subagent uses (ideally a small/fast model). Env default if absent.
    #[serde(default)]
    profile_id: Option<String>,
    /// A one-off LLM override layered on top of the profile (rarely needed).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// The subagent persona (its system prompt); defaults to the built-in researcher.
    #[serde(default)]
    persona_id: Option<String>,
    /// An inline system-prompt override (from a saved pairing). Wins over `persona_id` when a
    /// non-empty string is provided, so a pairing's self-contained prompt takes effect.
    #[serde(default)]
    system: Option<String>,
    /// An inline display name (from a saved pairing) shown on the subagent's outputs.
    #[serde(default)]
    name: Option<String>,
    /// Advanced: tool-loop budget per delegated task (default 8, clamped 1..30).
    #[serde(default)]
    max_rounds: Option<usize>,
    /// Advanced: max chars of digest fed back to the primary (default 4000, clamped 500..20000).
    #[serde(default)]
    digest_cap: Option<usize>,
}

/// Drafter configuration for a chat turn. When `enabled`, the primary is offered a `draft_ir`
/// tool; each call runs ONE grammar-constrained completion on the secondary (a small local model)
/// that returns well-formed model IR. No tool loop / digest / round budget applies — the grammar
/// carries the syntactic weight, so even a tiny model emits valid IR.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrafterRequest {
    #[serde(default)]
    enabled: bool,
    /// The saved LLM profile the drafter uses (ideally a small/fast local model). Env default if
    /// absent.
    #[serde(default)]
    profile_id: Option<String>,
    /// A one-off LLM override layered on top of the profile (rarely needed).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// An inline system-prompt override (from a saved pairing). Empty ⇒ the built-in IR-writer
    /// prompt is used.
    #[serde(default)]
    system: Option<String>,
    /// An inline display name (from a saved pairing) shown on the drafter's outputs.
    #[serde(default)]
    name: Option<String>,
}

/// One configured Pair AI reviewer in a chat request.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairRequest {
    /// Whether this reviewer is active. The cockpit sends `enabled:false` (or omits the object)
    /// when Pair AI is switched off, so the turn runs exactly as a single-agent investigation.
    #[serde(default)]
    enabled: bool,
    /// The saved LLM profile the reviewer uses (its own model — ideally a different family from
    /// the primary). Falls back to the env default when absent.
    #[serde(default)]
    profile_id: Option<String>,
    /// A one-off LLM override layered on top of the profile (rarely needed).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// The pairing persona (reviewer system prompt); defaults to the built-in skeptic.
    #[serde(default)]
    persona_id: Option<String>,
    /// An inline system-prompt override (from a saved pairing). Wins over `persona_id` when a
    /// non-empty string is provided.
    #[serde(default)]
    system: Option<String>,
    /// An inline display name (from a saved pairing) shown on the reviewer's bubble.
    #[serde(default)]
    name: Option<String>,
    /// Optional allowlist of tool names this reviewer (secondary model) may use. `None`/empty =
    /// the full available surface.
    #[serde(default)]
    tools: Option<Vec<String>>,
}

/// Loop-monitor configuration for a chat turn. When `enabled`, a second model watches the
/// primary's live transcript at a fixed cadence and steers it (or, after its budget, forces a
/// wrap-up) when it goes in circles. Off unless `enabled` — existing turns are unaffected.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MonitorRequest {
    #[serde(default)]
    enabled: bool,
    /// The saved LLM profile the monitor uses (ideally a small/cheap model). Falls back to the
    /// env default when absent.
    #[serde(default)]
    profile_id: Option<String>,
    /// A one-off LLM override layered on top of the profile (rarely needed).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// The monitor persona (its system prompt); defaults to the built-in loop breaker.
    #[serde(default)]
    persona_id: Option<String>,
    /// An inline system-prompt override (from a saved pairing). Wins over `persona_id` when a
    /// non-empty string is provided.
    #[serde(default)]
    system: Option<String>,
}

/// `POST .../chat` — send one operator message; the droid replies (running SQL/Python
/// tool calls as needed), resuming from the persisted transcript. The updated transcript
/// is saved so the next turn keeps full context.
async fn cockpit_chat_send(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
    Json(req): Json<ChatSendRequest>,
) -> impl IntoResponse {
    if req.message.trim().is_empty() {
        return unprocessable("message must not be empty".to_string());
    }
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return unprocessable(
            "no LLM model configured; set it in the console settings (cog, lower-left) or \
             via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER \
             as needed), or pass an `llm` object with at least `model` in the request body"
                .to_string(),
        );
    }
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let limit = req.limit.unwrap_or(100_000).clamp(1, 1_000_000);
    let max_rounds = req.max_rounds.unwrap_or(12).clamp(1, 40);
    let allow_python = req.allow_python;
    let py = state.settings.snapshot().py_config();
    // The operator's stated objective for this process (if any) frames the conversation.
    let objective = state
        .workspaces
        .get_process(&workspace, &process)
        .and_then(|p| p.config.objective);
    // The process's BPMN model (if any) unlocks the structural read_model/analyze_model tools.
    let model = state.workspaces.read_model(&workspace, &process);
    let annotations = current_annotations(&state, &workspace, &process);
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    // Load the prior transcript BEFORE the spawn_blocking (Vec<Msg> is Send); the DuckDB
    // connection inside the analysis is !Send, so the loop runs on a current-thread runtime.
    let session = state.chat.get(&key, &sid).unwrap_or_default();
    let prior = session.messages;
    let prior_stamps = session.stamps;
    let prior_turn_models = session.turn_models;
    // The model label that will answer this turn — persisted per turn so a historical bubble keeps
    // the answering model's name regardless of what's selected later.
    let model_label = cfg.model.clone();
    // Resolve the persona (standing system prompt). It only takes effect on the first turn;
    // record the resolved id on the session so the cockpit can show/lock it thereafter.
    let (persona_id, persona_system) = state.personas.resolve(req.persona_id.as_deref());
    if prior.is_empty() {
        state.chat.set_persona(&key, &sid, &persona_id);
    }
    state.chat.record_model(&key, &sid, &cfg.model);
    let user_ts = chat_now_ms();
    let pairs = match build_pair_stages(&state, &req) {
        Ok(p) => p,
        Err(e) => return unprocessable(e),
    };
    let subagent = match build_subagent(&state, &req) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let drafter = match build_drafter(&state, &req) {
        Ok(d) => d,
        Err(e) => return unprocessable(e),
    };
    let custom_tools = state.tools.enabled();
    let primary_tools = req.tools.clone();
    let message = req.message;
    // Register a wrap-up flag for this in-flight turn so `POST .../chat/wrapup` can ask the
    // agent to report early. Cleared in all exit paths below.
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if let Ok(mut m) = state.chat_cancels.lock() {
        m.insert(cancel_key.clone(), cancel.clone());
    }
    let task = {
        let cancel = cancel.clone();
        let dbg = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
        let dbg_capture = dbg.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("runtime: {e}"))?;
            let mut sink = move |ev: agent::AgentEvent| {
                if let agent::AgentEvent::Request { round, body } = ev {
                    if let Ok(mut d) = dbg_capture.lock() {
                        let ts = chat_now_ms();
                        d.push(serde_json::json!({ "round": round, "ts": ts, "body": body }));
                    }
                }
            };
            rt.block_on(investigate::run_chat_turn(
                &src,
                cfg,
                py,
                limit,
                max_rounds,
                allow_python,
                objective.as_deref(),
                Some(&persona_system),
                model,
                annotations,
                Some(&cancel),
                None,
                &mut sink,
                &pairs,
                subagent,
                drafter,
                custom_tools,
                primary_tools,
                prior,
                &message,
                None,
            ))
        })
        .await;
        // Stash this turn's exact request payloads for the session's Debug tab (durable).
        let bodies = dbg.lock().map(|d| d.clone()).unwrap_or_default();
        state.chat_debug.save(&cancel_key, bodies);
        handle
    };
    if let Ok(mut m) = state.chat_cancels.lock() {
        m.remove(&cancel_key);
    }
    match task {
        Ok(Ok(result)) => {
            let stamps =
                chat::extend_stamps(&result.messages, prior_stamps, user_ts, chat_now_ms());
            let turn_models =
                chat::extend_turn_models(&result.messages, prior_turn_models, &model_label);
            state
                .chat
                .save(&key, &sid, result.messages.clone(), stamps.clone());
            state.chat.set_turn_models(&key, &sid, turn_models.clone());
            Json(serde_json::json!({
                "sessionId": sid,
                "answer": result.answer,
                "rounds": result.rounds,
                "dataset": result.dataset,
                "turns": chat::render_view_full(&result.messages, &stamps, &turn_models),
            }))
            .into_response()
        }
        Ok(Err(e)) => bad_gateway(e),
        Err(e) => bad_gateway(format!("chat task failed: {e}")),
    }
}

/// `POST .../chat/stream` — like [`cockpit_chat_send`], but streams the turn as Server-Sent
/// Events so the cockpit can show the droid's thinking, tool calls, and answer live as they are
/// produced. Event payloads are JSON: `{type:"round",n}`, `{type:"reasoning",text}`,
/// `{type:"answer",text}`, `{type:"tool",tool,arguments}`, `{type:"toolResult",tool,result}`,
/// and a terminal `{type:"done",answer,rounds,dataset,turns}` or `{type:"error",message}`.
async fn cockpit_chat_stream(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
    Json(req): Json<ChatSendRequest>,
) -> impl IntoResponse {
    if req.message.trim().is_empty() {
        return unprocessable("message must not be empty".to_string());
    }
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return unprocessable(
            "no LLM model configured; set it in the console settings (cog, lower-left) or \
             via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL / PROCESSOS_LLM_PROVIDER \
             as needed), or pass an `llm` object with at least `model` in the request body"
                .to_string(),
        );
    }
    let src = match state.workspaces.resolve_source(&workspace, &process) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let limit = req.limit.unwrap_or(100_000).clamp(1, 1_000_000);
    let max_rounds = req.max_rounds.unwrap_or(12).clamp(1, 40);
    let allow_python = req.allow_python;
    let py = state.settings.snapshot().py_config();
    let objective = state
        .workspaces
        .get_process(&workspace, &process)
        .and_then(|p| p.config.objective);
    let model = state.workspaces.read_model(&workspace, &process);
    let annotations = current_annotations(&state, &workspace, &process);
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    let session = state.chat.get(&key, &sid).unwrap_or_default();
    let prior = session.messages;
    let prior_stamps = session.stamps;
    let prior_turn_models = session.turn_models;
    // The model label answering this turn — persisted per turn so historical bubbles keep it.
    let model_label = cfg.model.clone();
    // Resolve the persona; it only takes effect on the first turn. Record it on the session.
    let (persona_id, persona_system) = state.personas.resolve(req.persona_id.as_deref());
    if prior.is_empty() {
        state.chat.set_persona(&key, &sid, &persona_id);
    }
    state.chat.record_model(&key, &sid, &cfg.model);
    let user_ts = chat_now_ms();
    let pairs = match build_pair_stages(&state, &req) {
        Ok(p) => p,
        Err(e) => return unprocessable(e),
    };
    let subagent = match build_subagent(&state, &req) {
        Ok(s) => s,
        Err(e) => return unprocessable(e),
    };
    let drafter = match build_drafter(&state, &req) {
        Ok(d) => d,
        Err(e) => return unprocessable(e),
    };
    let custom_tools = state.tools.enabled();
    let primary_tools = req.tools.clone();
    let message = req.message;
    // Register a wrap-up flag so `POST .../chat/wrapup` can ask this in-flight turn to report early.
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if let Ok(mut m) = state.chat_cancels.lock() {
        m.insert(cancel_key.clone(), cancel.clone());
    }
    // Register a steering queue so `POST .../chat/steer` can redirect this in-flight turn.
    let steer = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    if let Ok(mut m) = state.chat_steers.lock() {
        m.insert(cancel_key.clone(), steer.clone());
    }
    // Register a cell for this turn's live completion id (`chatcmpl-…`), set by the streaming sink
    // once the model starts replying, so wrap-up / the monitor can end its thinking mid-generation.
    let cmpl_id = Arc::new(std::sync::Mutex::new(None::<String>));
    if let Ok(mut m) = state.chat_cmpl_ids.lock() {
        m.insert(cancel_key.clone(), cmpl_id.clone());
    }

    // Register a live event buffer so the streaming response — and any later reattach after a
    // page reload — can replay this turn's events. The agent task pushes here regardless of
    // whether the original fetch is still connected.
    let live = LiveTurn::new(message.clone());
    if let Ok(mut m) = state.chat_live.lock() {
        m.insert(cancel_key.clone(), live.clone());
    }

    // Loop monitor (off by default): a second model that watches this turn's transcript and
    // steers the primary when it circles. It runs out-of-band on the main runtime and writes into
    // the same steer/cancel channels as the operator's manual controls; `monitor_done` lets the
    // primary signal completion so the watcher stops. Resolved before the blocking task so a
    // misconfiguration just means "no monitor", never a failed turn.
    let monitor_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // This turn's Debug capture, created before the monitor spawns so BOTH the primary agent loop
    // and the out-of-band loop monitor append their request payloads to the same buffer (the
    // monitor's entries are tagged `kind:"monitor"`). Persisted to `chat_debug` at turn end.
    let dbg = std::sync::Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
    if let Some((mon_cfg, mon_persona)) = resolve_monitor(&state, req.monitor.as_ref()) {
        tokio::spawn(run_loop_monitor(
            mon_cfg,
            mon_persona,
            state.chat.clone(),
            key.clone(),
            sid.clone(),
            steer.clone(),
            cancel.clone(),
            live.clone(),
            monitor_done.clone(),
            // The monitor controls the PRIMARY turn: it must target the primary endpoint + the
            // primary's live completion id for the reasoning-control "end thinking" call.
            cfg.base_url.clone(),
            cmpl_id.clone(),
            dbg.clone(),
        ));
    }

    // The agent loop runs on a blocking thread (DuckDB is !Send) and pushes events into the live
    // buffer; the SSE response(s) follow that buffer on the main runtime.
    let task_state = state.clone();
    let task_key = key.clone();
    let task_sid = sid.clone();
    let task_cancel_key = cancel_key.clone();
    let task_live = live.clone();
    let task_monitor_done = monitor_done.clone();
    let task_cmpl_id = cmpl_id.clone();
    let task_dbg = dbg.clone();
    tokio::task::spawn_blocking(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                task_live.emit(
                    serde_json::json!({ "type": "error", "message": format!("runtime: {e}") }),
                );
                if let Ok(mut m) = task_state.chat_live.lock() {
                    m.remove(&task_cancel_key);
                }
                return;
            }
        };
        let live_for_sink = task_live.clone();
        // Incrementally PERSIST the running transcript (throttled to ~2s) so a turn that times
        // out, errors, or is interrupted still leaves a debuggable transcript on disk — instead
        // of the old behaviour where nothing was saved unless the whole turn completed cleanly.
        let cp_chat = task_state.chat.clone();
        let cp_key = task_key.clone();
        let cp_sid = task_sid.clone();
        let cp_prior_stamps = prior_stamps.clone();
        let cp_prior_turn_models = prior_turn_models.clone();
        let cp_model = model_label.clone();
        let cp_user_ts = user_ts;
        // Start "stale" so the first checkpoint (which carries the operator's message) saves at once.
        let mut last_save = std::time::Instant::now() - std::time::Duration::from_secs(3600);
        let mut checkpoint = move |msgs: &[agent::Msg]| {
            let now = std::time::Instant::now();
            if now.duration_since(last_save) < std::time::Duration::from_secs(2) {
                return;
            }
            last_save = now;
            let stamps =
                chat::extend_stamps(msgs, cp_prior_stamps.clone(), cp_user_ts, chat_now_ms());
            let turn_models =
                chat::extend_turn_models(msgs, cp_prior_turn_models.clone(), &cp_model);
            cp_chat.save(&cp_key, &cp_sid, msgs.to_vec(), stamps);
            cp_chat.set_turn_models(&cp_key, &cp_sid, turn_models);
        };
        // Accumulate this turn's exact request payloads for the session's Debug tab while also
        // forwarding each over the wire so the tab can update live. Shared with the loop monitor,
        // whose evaluations are appended to the same buffer (tagged `kind:"monitor"`).
        let dbg = task_dbg;
        let dbg_for_sink = dbg.clone();
        let cmpl_for_sink = task_cmpl_id.clone();
        let mut sink = move |ev: agent::AgentEvent| {
            let v = match ev {
                agent::AgentEvent::Round(n) => serde_json::json!({ "type": "round", "n": n }),
                agent::AgentEvent::Completion { id } => {
                    // Stash the live completion id so wrap-up / the monitor can target this turn
                    // with the reasoning-control endpoint. Not forwarded to the cockpit (internal).
                    if let Ok(mut c) = cmpl_for_sink.lock() {
                        *c = Some(id);
                    }
                    return;
                }
                agent::AgentEvent::Request { round, body } => {
                    let ts = chat_now_ms();
                    if let Ok(mut d) = dbg_for_sink.lock() {
                        d.push(
                            serde_json::json!({ "round": round, "ts": ts, "body": body.clone() }),
                        );
                    }
                    serde_json::json!({ "type": "request", "round": round, "ts": ts, "body": body })
                }
                agent::AgentEvent::Reasoning(t) => {
                    serde_json::json!({ "type": "reasoning", "text": t })
                }
                agent::AgentEvent::Answer(t) => serde_json::json!({ "type": "answer", "text": t }),
                agent::AgentEvent::ToolCall { tool, arguments } => {
                    serde_json::json!({ "type": "tool", "tool": tool, "arguments": arguments })
                }
                agent::AgentEvent::ToolResult { tool, result } => {
                    serde_json::json!({ "type": "toolResult", "tool": tool, "result": result })
                }
                agent::AgentEvent::Agent { id, name, role } => {
                    serde_json::json!({ "type": "agent", "id": id, "name": name, "role": role })
                }
            };
            live_for_sink.emit(v);
        };
        let result = rt.block_on(investigate::run_chat_turn(
            &src,
            cfg,
            py,
            limit,
            max_rounds,
            allow_python,
            objective.as_deref(),
            Some(&persona_system),
            model,
            annotations,
            Some(&cancel),
            Some(&steer),
            &mut sink,
            &pairs,
            subagent,
            drafter,
            custom_tools,
            primary_tools,
            prior,
            &message,
            Some(&mut checkpoint),
        ));
        // Signal the loop monitor (if any) that the primary has finished, so it stops polling.
        task_monitor_done.store(true, std::sync::atomic::Ordering::Relaxed);
        // Persist the captured payloads regardless of outcome (a failed turn still sent a request).
        let bodies = dbg.lock().map(|d| d.clone()).unwrap_or_default();
        task_state.chat_debug.save(&task_cancel_key, bodies);
        match result {
            Ok(r) => {
                let stamps = chat::extend_stamps(&r.messages, prior_stamps, user_ts, chat_now_ms());
                let turn_models =
                    chat::extend_turn_models(&r.messages, prior_turn_models, &model_label);
                task_state
                    .chat
                    .save(&task_key, &task_sid, r.messages.clone(), stamps.clone());
                task_state
                    .chat
                    .set_turn_models(&task_key, &task_sid, turn_models.clone());
                task_live.emit(serde_json::json!({
                    "type": "done",
                    "sessionId": task_sid,
                    "answer": r.answer,
                    "rounds": r.rounds,
                    "dataset": r.dataset,
                    "turns": chat::render_view_full(&r.messages, &stamps, &turn_models),
                }));
            }
            Err(e) => {
                task_live.emit(serde_json::json!({ "type": "error", "message": e }));
            }
        }
        if let Ok(mut m) = task_state.chat_cancels.lock() {
            m.remove(&task_cancel_key);
        }
        if let Ok(mut m) = task_state.chat_steers.lock() {
            m.remove(&task_cancel_key);
        }
        if let Ok(mut m) = task_state.chat_cmpl_ids.lock() {
            m.remove(&task_cancel_key);
        }
        // Drop the live buffer last: any consumers still attached hold their own Arc clones and
        // already saw the terminal event, so this only stops *new* reattachers (which then fall
        // back to the now-complete persisted transcript).
        if let Ok(mut m) = task_state.chat_live.lock() {
            m.remove(&task_cancel_key);
        }
    });

    sse_from_live(live).into_response()
}

/// `POST .../chat/reset` — forget a conversation. With `?session=`, clears just that
/// session's transcript (keeping its name); without, forgets every session for the dataset.
async fn cockpit_chat_reset(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    match q.session {
        Some(sid) if state.chat.get(&key, &sid).is_some() => {
            state.chat.save(&key, &sid, Vec::new(), Vec::new());
        }
        _ => state.chat.clear(&key),
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST .../chat/wrapup` — ask an in-flight chat turn to stop investigating and report its
/// findings so far. No-op (404) when no turn is running for this session.
async fn cockpit_chat_wrapup(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
) -> impl IntoResponse {
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    let flagged = state
        .chat_cancels
        .lock()
        .ok()
        .and_then(|m| m.get(&cancel_key).cloned());
    match flagged {
        Some(flag) => {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
            // Prefer a mid-thinking halt via the reasoning-control surface: end the live turn's
            // reasoning now (it answers immediately) rather than only stopping at the next round
            // boundary. Falls back to the cancel flag above when there's no live completion id yet
            // or the endpoint is an older build without the surface.
            let cmpl = state
                .chat_cmpl_ids
                .lock()
                .ok()
                .and_then(|m| m.get(&cancel_key).cloned())
                .and_then(|c| c.lock().ok().and_then(|v| v.clone()));
            if let Some(id) = cmpl {
                let cfg = resolve_llm(&state, None);
                if cfg.is_ready() {
                    let _ = reasoning::end_reasoning(&cfg.base_url, &id).await;
                }
            }
            StatusCode::ACCEPTED.into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no investigation is running for this dataset" })),
        )
            .into_response(),
    }
}

/// Body for `POST .../chat/steer` — the operator's mid-investigation steering instruction.
#[derive(Debug, Deserialize)]
struct SteerRequest {
    message: String,
}

/// `POST .../chat/steer` — send a steering instruction to an in-flight chat turn. The message
/// is queued and injected as a user turn at the agent loop's next round boundary, redirecting
/// the investigation without restarting it. No-op (404) when no turn is running for this session.
async fn cockpit_chat_steer(
    State(state): State<AppState>,
    Path((workspace, process)): Path<(String, String)>,
    Query(q): Query<SessionQuery>,
    Json(req): Json<SteerRequest>,
) -> impl IntoResponse {
    if req.message.trim().is_empty() {
        return unprocessable("message must not be empty".to_string());
    }
    let key = chat::session_key(&workspace, &process);
    let sid = resolve_session(&state, &key, q.session.as_deref()).id;
    let cancel_key = chat_cancel_key(&key, &sid);
    let queue = state
        .chat_steers
        .lock()
        .ok()
        .and_then(|m| m.get(&cancel_key).cloned());
    match queue {
        Some(q) => {
            if let Ok(mut v) = q.lock() {
                v.push(req.message);
            }
            StatusCode::ACCEPTED.into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no investigation is running for this dataset" })),
        )
            .into_response(),
    }
}
fn chat_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `GET /api/python/status` — whether a Python interpreter is configured and whether the
/// optional data-science stack (pandas / numpy / duckdb / scipy) is importable in it, so the
/// console can label the Python escape-hatch control appropriately.
async fn python_status(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.settings.snapshot();
    // "Configured" = the operator set an interpreter explicitly (settings or env), as opposed
    // to falling back to the bare `python3` default.
    let configured = snap
        .python_bin
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty())
        || std::env::var_os("PROCESSOS_PYTHON").is_some();
    let py = snap.py_config();
    let interpreter = py.python_bin.clone();
    let probe = tokio::task::spawn_blocking(move || pyrunner::probe_data_science(&py))
        .await
        .unwrap_or_default();
    Json(serde_json::json!({
        "configured": configured,
        "interpreter": interpreter,
        "interpreterRuns": probe.runs,
        "dataScience": probe.data_science,
        "missing": probe.missing,
    }))
    .into_response()
}

/// `GET /api/chat-prompts` — list reusable compose-box message templates.
async fn chat_prompts_list(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.chat_prompts.list()).into_response()
}

/// `POST /api/chat-prompts` — author or update a chat prompt (persisted to the config dir).
async fn chat_prompts_upsert(
    State(state): State<AppState>,
    Json(prompt): Json<chat_prompts::ChatPrompt>,
) -> impl IntoResponse {
    match state.chat_prompts.upsert(prompt) {
        Ok(p) => Json(p).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/chat-prompts/{id}` — delete a non-built-in chat prompt.
async fn chat_prompts_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.chat_prompts.delete(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/personas` — list the selectable chat personas (standing system prompts).
async fn personas_list(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.personas.list()).into_response()
}

/// `POST /api/personas` — author or update a persona (persisted to the config dir).
async fn personas_upsert(
    State(state): State<AppState>,
    Json(persona): Json<personas::Persona>,
) -> impl IntoResponse {
    match state.personas.upsert(persona) {
        Ok(p) => Json(p).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/personas/{id}` — delete a non-built-in persona.
async fn personas_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.personas.delete(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/pairings` — list the saved Pair AI configurations.
async fn pairings_list(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.pairings.list()).into_response()
}

/// `POST /api/pairings` — author or update a pairing (persisted to the config dir).
///
/// Speculator-mode pairings get semantic validation on top of the store's syntactic checks:
/// the draft (secondary) profile must be a local sidecar with a model file, a pinned primary must
/// also be a local sidecar, and — when both models are already downloaded — their GGUF headers
/// must be speculative-decoding compatible. Verification that needs a not-yet-downloaded model is
/// deferred to launch time ([`llama_start`]'s hard gate), so authoring before downloading works.
async fn pairings_upsert(
    State(state): State<AppState>,
    Json(pairing): Json<pairings::Pairing>,
) -> impl IntoResponse {
    if pairing.mode == pairings::PairMode::Speculator {
        let snap = state.settings.snapshot();
        let p = pairing.clone();
        let verdict = tokio::task::spawn_blocking(move || validate_speculator_pairing(&snap, &p))
            .await
            .unwrap_or_else(|e| Err(format!("validation task failed: {e}")));
        if let Err(e) = verdict {
            return unprocessable(e);
        }
    }
    match state.pairings.upsert(pairing) {
        Ok(p) => Json(p).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// Author-time validation for a speculator pairing (see [`pairings_upsert`]). Rejects what is
/// provably invalid now; anything unverifiable because a model isn't downloaded yet is allowed
/// through and re-checked by the launch-time gate.
fn validate_speculator_pairing(
    snap: &settings::Settings,
    pairing: &pairings::Pairing,
) -> Result<(), String> {
    let draft = snap
        .profiles
        .iter()
        .find(|p| p.id == pairing.secondary_profile_id)
        .ok_or_else(|| {
            format!(
                "speculator pairing names unknown draft profile '{}'",
                pairing.secondary_profile_id
            )
        })?;
    if !draft.sidecar {
        return Err(format!(
            "draft profile '{}' is not a local sidecar — a speculator must be a local GGUF the \
             primary's llama-server can load via --model-draft",
            draft.id
        ));
    }
    if profile_model_spec(draft).is_none() {
        return Err(format!("draft profile '{}' has no model file", draft.id));
    }
    let models_dir = snap.effective_models_dir();
    // When the pairing pins a primary, it must be a local sidecar too; and when both GGUFs are
    // already on disk, prove compatibility now instead of surprising the operator at launch.
    if let Some(pid) = pairing.primary_profile_id.as_deref() {
        let primary =
            snap.profiles.iter().find(|p| p.id == pid).ok_or_else(|| {
                format!("speculator pairing pins unknown primary profile '{pid}'")
            })?;
        if !primary.sidecar {
            return Err(format!(
                "primary profile '{pid}' is not a local sidecar — speculative decoding only \
                 applies to models ProcessOS launches itself"
            ));
        }
        let scans = (
            profile_model_spec(primary).and_then(|m| resolve_model_gguf(&models_dir, &m)),
            profile_model_spec(draft).and_then(|m| resolve_model_gguf(&models_dir, &m)),
        );
        if let (Some(tp), Some(dp)) = scans {
            let target = gguf::GgufScan::read(&tp)?;
            let draft_scan = gguf::GgufScan::read(&dp)?;
            gguf::speculator_compatible(&target, &draft_scan)
                .map_err(|e| format!("'{}' cannot draft for '{pid}': {e}", draft.id))?;
        }
    }
    Ok(())
}

/// `DELETE /api/pairings/{id}` — delete a non-built-in pairing.
async fn pairings_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.pairings.delete(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/tools` — the full tool surface: the read-only built-in catalog followed by the
/// operator's custom tools.
async fn tools_list(State(state): State<AppState>) -> impl IntoResponse {
    let mut all = tools::builtin_catalog();
    all.extend(state.tools.list());
    Json(all).into_response()
}

/// `POST /api/tools` — author or update a custom tool (persisted to the config dir).
async fn tools_upsert(
    State(state): State<AppState>,
    Json(def): Json<tools::ToolDef>,
) -> impl IntoResponse {
    match state.tools.upsert(def) {
        Ok(d) => Json(d).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/tools/{id}` — delete a custom tool.
async fn tools_delete(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.tools.delete(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/tools/import` — import a bundle of custom tool definitions (shared by another
/// operator). Returns how many were imported and any per-entry errors.
async fn tools_import(
    State(state): State<AppState>,
    Json(defs): Json<Vec<tools::ToolDef>>,
) -> impl IntoResponse {
    let (imported, errors) = state.tools.import_many(defs);
    Json(serde_json::json!({ "imported": imported, "errors": errors })).into_response()
}

// --- Nano instances (Console-managed connection targets) ------------------------

/// Request body to add or update a Nano instance.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NanoInstanceBody {
    #[serde(default)]
    name: String,
    #[serde(default, alias = "url")]
    base_url: String,
}

/// Request body to star/unstar a Nano instance.
#[derive(Debug, Deserialize)]
struct NanoStarBody {
    #[serde(default)]
    starred: bool,
}

/// `GET /api/nano/instances` — every configured Nano instance plus the active id.
async fn nano_instances_list(State(state): State<AppState>) -> impl IntoResponse {
    let (instances, active) = state.nano_instances.list();
    Json(serde_json::json!({ "instances": instances, "active": active })).into_response()
}

/// `POST /api/nano/instances` — add a new instance (`{name, baseUrl}`).
async fn nano_instances_add(
    State(state): State<AppState>,
    Json(body): Json<NanoInstanceBody>,
) -> impl IntoResponse {
    match state.nano_instances.add(&body.name, &body.base_url) {
        Ok(inst) => (StatusCode::CREATED, Json(inst)).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `PUT /api/nano/instances/{id}` — edit an instance's name/base URL.
async fn nano_instances_update(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<NanoInstanceBody>,
) -> impl IntoResponse {
    match state.nano_instances.update(&id, &body.name, &body.base_url) {
        Ok(inst) => Json(inst).into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `DELETE /api/nano/instances/{id}` — remove an instance.
async fn nano_instances_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.nano_instances.remove(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/nano/instances/{id}/select` — make an instance the active analysis target.
async fn nano_instances_select(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.nano_instances.select(&id) {
        Ok(()) => {
            let (instances, active) = state.nano_instances.list();
            Json(serde_json::json!({ "instances": instances, "active": active })).into_response()
        }
        Err(e) => unprocessable(e),
    }
}

/// `POST /api/nano/instances/{id}/star` — star/unstar an instance (`{starred: bool}`), so the
/// Console floats it to the top of the card layout. Returns the refreshed list.
async fn nano_instances_star(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<NanoStarBody>,
) -> impl IntoResponse {
    match state.nano_instances.set_star(&id, body.starred) {
        Ok(_) => {
            let (instances, active) = state.nano_instances.list();
            Json(serde_json::json!({ "instances": instances, "active": active })).into_response()
        }
        Err(e) => unprocessable(e),
    }
}

/// `GET /api/nano/instances/{id}/health` — probe whether an instance is reachable, so the
/// console can show a live reachability light. 404 for an unknown id.
async fn nano_instances_health(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let Some(inst) = state.nano_instances.get(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such instance: {id}") })),
        )
            .into_response();
    };
    let ok = NanoClient::new(&inst.base_url).health_ok().await;
    Json(serde_json::json!({ "id": id, "baseUrl": inst.base_url, "ok": ok })).into_response()
}

/// `GET /api/harness/example` — the bundled example scenario JSON, so callers have
/// a ready template to copy and adapt.
async fn harness_example() -> impl IntoResponse {
    Json(example_scenario())
}

/// `GET /api/harness/example/run` — run the bundled example and return its ranking.
async fn harness_example_run() -> impl IntoResponse {
    run_scenario_blocking(example_scenario()).await
}

/// `POST /api/harness/run` — run a caller-supplied scenario and return its ranking.
async fn harness_run(Json(scenario): Json<Scenario>) -> impl IntoResponse {
    run_scenario_blocking(scenario).await
}

/// Request body for the calibration path: a scenario plus the measured per-job-type
/// distributions to ground its baseline workers in.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CalibrateRequest {
    scenario: Scenario,
    /// Measured per-job-type behaviour (e.g. the `byJobType` array from
    /// `GET /api/harness/cluster`, or any equivalent observation).
    #[serde(default)]
    measured: Vec<MeasuredJobType>,
}

/// `POST /api/harness/calibrate` — ground a scenario's baseline workers in measured
/// production, then rank over the calibrated model. Returns the calibration (which
/// job types were covered, and the resulting worker pool) alongside the ranking, so
/// the same loop's verdicts reflect the service times and failure rates the cluster
/// actually observed rather than hand-authored numbers. No LLM or live cluster
/// required — the caller supplies the measured distributions.
async fn harness_calibrate(Json(req): Json<CalibrateRequest>) -> impl IntoResponse {
    let calibration = calibrate_from_measured(&req.scenario, &req.measured);
    let calibrated_scenario = apply_calibration(&req.scenario, &calibration);
    let scenario = calibrated_scenario.clone();
    let ranked = tokio::task::spawn_blocking(move || run_scenario(&scenario)).await;
    match ranked {
        Ok(Ok(report)) => Json(serde_json::json!({
            "calibration": calibration,
            "report": report,
        }))
        .into_response(),
        Ok(Err(e)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("join error: {e}") })),
        )
            .into_response(),
    }
}

/// Request body for the LLM hypothesis path: a scenario plus optional LLM
/// overrides and a flag to also evaluate the baked grid for comparison.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HypothesizeRequest {
    scenario: Scenario,
    #[serde(default)]
    llm: Option<LlmOverride>,
    #[serde(default)]
    include_baked: bool,
    /// Optional measured per-job-type distributions. When present, the scenario's
    /// baseline workers are calibrated from production before the LLM reasons over
    /// it and before any candidate is scored — so the bar the hypotheses must beat
    /// reflects measured reality, not hand-authored numbers.
    #[serde(default)]
    measured: Vec<MeasuredJobType>,
    /// Select a system prompt from the library by id. Defaults to the built-in
    /// `default` when absent.
    #[serde(default)]
    prompt_id: Option<String>,
    /// Inline system-prompt override (highest precedence) — author/experiment with a
    /// prompt for this single run without storing it.
    #[serde(default)]
    prompt: Option<String>,
}

/// `POST /api/harness/hypothesize` — ask the configured LLM to propose candidates,
/// evaluate + rank them with the SimRunner, and return the report. The model is
/// reached via the pluggable client (local llama.cpp / Ollama / vLLM, or Anthropic).
async fn harness_hypothesize(
    State(state): State<AppState>,
    Json(req): Json<HypothesizeRequest>,
) -> impl IntoResponse {
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured; set it in the console settings (cog, \
                          lower-left) or via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL \
                          / PROCESSOS_LLM_PROVIDER as needed), or pass an `llm` object with \
                          at least `model` in the request body"
            })),
        )
            .into_response();
    }

    // Resolve the system prompt: an inline override wins; otherwise a selected
    // library prompt; otherwise the built-in default.
    let system_prompt = match (&req.prompt, &req.prompt_id) {
        (Some(inline), _) => inline.clone(),
        (None, Some(id)) => match state
            .prompts
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .system_of(id)
        {
            Some(s) => s,
            None => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(serde_json::json!({ "error": format!("unknown prompt id: {id}") })),
                )
                    .into_response();
            }
        },
        (None, None) => state
            .prompts
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .system_of(DEFAULT_PROMPT_ID)
            .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.to_string()),
    };

    // Ground the baseline in production when measured distributions are supplied,
    // so the LLM reasons over — and every candidate is scored against — the service
    // times and failure rates the cluster actually observed.
    let scenario = if req.measured.is_empty() {
        req.scenario.clone()
    } else {
        let calibration = calibrate_from_measured(&req.scenario, &req.measured);
        apply_calibration(&req.scenario, &calibration)
    };
    match run_hypothesis(
        &scenario,
        &cfg,
        req.include_baked,
        &req.measured,
        &system_prompt,
    )
    .await
    {
        Ok(report) => Json(report).into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// Request body for the recorded-input replay verifier.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayRequest {
    /// The recorded instance to replay. Its trace must carry the Tier-2 stimulus
    /// log — capture the source cluster with `c8 nano --capture`.
    instance_key: String,
    /// BPMN XML of the candidate model to evaluate against the recorded inputs.
    candidate_model: String,
    /// Process id to start; defaults to the recorded trace's own process id.
    #[serde(default)]
    process_id: Option<String>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
}

/// `POST /api/harness/replay` — the **Level-2 verifier** (§7.7/§7.9). Fetch one
/// recorded instance's trace, replay its creation inputs + recorded job outputs
/// against the supplied candidate model on the real engine, and return the
/// **gradient**: validity, completion, per-job-type coverage (`uncoveredJobTypes`
/// ⇒ requires new workers) and boundary-conservation `divergences`. This is the
/// idempotent, directly-callable eval an agentic loop iterates against.
async fn harness_replay(
    State(state): State<AppState>,
    Json(req): Json<ReplayRequest>,
) -> impl IntoResponse {
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.active_target(),
    };
    let trace = match client.trace(&req.instance_key).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let rec = match RecordedInstance::from_trace(&trace) {
        Ok(r) => r,
        Err(why) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": why.to_string(),
                    "instanceKey": req.instance_key,
                })),
            )
                .into_response();
        }
    };
    let defs = match parse_bpmn(&req.candidate_model) {
        Ok(d) if !d.is_empty() => d,
        Ok(_) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": "candidate model contained no process definitions"
                })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": format!("candidate model failed to parse: {e:?}")
                })),
            )
                .into_response();
        }
    };
    let process_id = req
        .process_id
        .clone()
        .unwrap_or_else(|| rec.process_id.clone());
    let result = replay_instance(&defs, &process_id, &rec);
    Json(result).into_response()
}

/// Request body for the batch replay verifier.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayBatchRequest {
    /// BPMN XML of the candidate model to score against the recorded dataset.
    candidate_model: String,
    /// Process whose recorded instances form the dataset. Defaults to the
    /// candidate's first process id.
    #[serde(default)]
    process_id: Option<String>,
    /// How many recent trace summaries to scan for matching instances.
    #[serde(default)]
    limit: Option<usize>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
}

/// `POST /api/harness/replay-batch` — score one candidate against a **recorded
/// dataset** (§7.7 Level-2 / §7.9). Fetch the recent recorded instances of a
/// process, replay each against the candidate, and fold the per-instance gradients
/// into a single scorecard (`conservedRate`, `uncoveredJobTypes`, `divergentKeys`,
/// latency). This is the candidate scorer the hypothesis loop ranks with.
async fn harness_replay_batch(
    State(state): State<AppState>,
    Json(req): Json<ReplayBatchRequest>,
) -> impl IntoResponse {
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.active_target(),
    };
    let defs = match parse_bpmn(&req.candidate_model) {
        Ok(d) if !d.is_empty() => d,
        Ok(_) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": "candidate model contained no process definitions"
                })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": format!("candidate model failed to parse: {e:?}")
                })),
            )
                .into_response();
        }
    };
    let process_id = req.process_id.clone().unwrap_or_else(|| defs[0].id.clone());

    let limit = req.limit.unwrap_or(200);
    let summaries = match client.list_traces(limit).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };

    // Distil the recorded instances of this process, tracking why any are skipped
    // (capture off, truncated log/snapshot) so the operator can fix the source.
    let (dataset, matched, skipped, skip_reasons) =
        distil_recorded_dataset(&client, &process_id, &summaries).await;

    let report = replay_dataset(&defs, &process_id, &dataset);
    Json(serde_json::json!({
        "processId": process_id,
        "matchedInstances": matched,
        "skipped": skipped,
        "skipReasons": skip_reasons,
        "report": report,
    }))
    .into_response()
}

/// Distil the replayable recorded instances of a process from a list of trace
/// summaries (already fetched from Nano's read contract). Returns the dataset
/// plus skip accounting (matched, skipped, reasons) so callers can tell the
/// operator *why* instances were dropped (capture off, truncated log/snapshot,
/// fetch failure). Shared by replay-batch and replay-rank.
async fn distil_recorded_dataset(
    client: &NanoClient,
    process_id: &str,
    summaries: &[crate::contracts::TraceSummary],
) -> (
    Vec<RecordedInstance>,
    u32,
    u32,
    std::collections::HashMap<String, u32>,
) {
    let mut dataset: Vec<RecordedInstance> = Vec::new();
    let mut skipped = 0u32;
    let mut skip_reasons: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut matched = 0u32;
    for s in summaries.iter().filter(|s| s.process_id == process_id) {
        matched += 1;
        let trace = match client.trace(&s.instance_key).await {
            Ok(t) => t,
            Err(_) => {
                skipped += 1;
                *skip_reasons
                    .entry("trace fetch failed".to_string())
                    .or_insert(0) += 1;
                continue;
            }
        };
        match RecordedInstance::from_trace(&trace) {
            Ok(r) => dataset.push(r),
            Err(why) => {
                skipped += 1;
                *skip_reasons.entry(why.to_string()).or_insert(0) += 1;
            }
        }
    }
    (dataset, matched, skipped, skip_reasons)
}

/// One candidate model in a replay-rank request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RankCandidateBody {
    /// Display name for the candidate.
    name: String,
    /// Why it was proposed (carried through to the scorecard).
    #[serde(default)]
    rationale: Option<String>,
    /// BPMN XML of the candidate model.
    model: String,
    /// Generative mocks for new workers this candidate introduces. Each entry is
    /// either a static output object (deterministic worker) or a `{ "outcomes":
    /// [{ "weight", "output" }] }` distribution (non-deterministic worker), so a
    /// variant adding a worker — including one that drives a downstream split —
    /// can still be scored. Parsed via `harness::parse_mock_workers`.
    #[serde(default)]
    mock_workers: serde_json::Value,
}

/// Request body for the replay-rank population scorer.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplayRankRequest {
    /// The candidate population to score against the recorded dataset.
    candidates: Vec<RankCandidateBody>,
    /// Process whose recorded instances form the dataset. Required (the dataset is
    /// process-scoped and candidates may rename their process id).
    process_id: String,
    /// How many recent trace summaries to scan for matching instances.
    #[serde(default)]
    limit: Option<usize>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
}

/// `POST /api/harness/replay-rank` — score a **population** of candidate models
/// against the same recorded dataset and rank them by the fidelity gradient
/// (§7.9). This is the gradient-driven loop's evaluation step: fan a set of
/// proposed redesigns over real production traces, rank fidelity-first
/// (`conservedRate`, then latency, then fewer required new workers), and hand the
/// operator a scorecard per candidate to decide on. The harness ranks; the human
/// (or the pilot process) decides.
async fn harness_replay_rank(
    State(state): State<AppState>,
    Json(req): Json<ReplayRankRequest>,
) -> impl IntoResponse {
    if req.candidates.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": "no candidates supplied" })),
        )
            .into_response();
    }
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.active_target(),
    };
    let limit = req.limit.unwrap_or(200);
    let summaries = match client.list_traces(limit).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let (dataset, matched, skipped, skip_reasons) =
        distil_recorded_dataset(&client, &req.process_id, &summaries).await;

    if dataset.is_empty() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "error": "no replayable recorded instances for this process \
                          (was the gateway run with --capture?)",
                "processId": req.process_id,
                "matchedInstances": matched,
                "skipped": skipped,
                "skipReasons": skip_reasons,
            })),
        )
            .into_response();
    }

    let candidates: Vec<CandidateModel> = req
        .candidates
        .into_iter()
        .map(|c| CandidateModel {
            name: c.name,
            rationale: c.rationale,
            model: c.model,
            mock_workers: crate::harness::parse_mock_workers(&c.mock_workers),
        })
        .collect();

    let ranking = rank_candidates_by_replay(&candidates, &dataset, Some(&req.process_id));
    Json(serde_json::json!({
        "processId": req.process_id,
        "matchedInstances": matched,
        "skipped": skipped,
        "skipReasons": skip_reasons,
        "ranking": ranking,
    }))
    .into_response()
}

/// Request body for the LLM-propose → replay-rank loop.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvolveRequest {
    /// BPMN XML of the current/baseline model the LLM redesigns from.
    baseline_model: String,
    /// Process whose recorded instances form the dataset + signal. Defaults to the
    /// baseline's first process id.
    #[serde(default)]
    process_id: Option<String>,
    /// How many recent trace summaries to scan for matching instances.
    #[serde(default)]
    limit: Option<usize>,
    /// Override the Nano gateway base url; defaults to the server's configured one.
    #[serde(default)]
    base_url: Option<String>,
    /// Per-request LLM overrides (model, base_url, provider, …).
    #[serde(default)]
    llm: Option<LlmOverride>,
    /// Inline system prompt override (wins over `promptId`).
    #[serde(default)]
    prompt: Option<String>,
    /// Select a library prompt by id; falls back to the built-in evolve prompt.
    #[serde(default)]
    prompt_id: Option<String>,
    /// The pilot's free-text steer for this round (§10) — threaded into the user
    /// prompt so the droid conditions its redesigns on the human's intent.
    #[serde(default)]
    pilot_note: Option<String>,
}

/// `POST /api/harness/evolve` — the gradient-driven loop end-to-end (§7.9): distil
/// the recorded production signal, ask the LLM to propose structural candidate
/// models, and rank them by replaying against the *real* recorded traces. The
/// droid proposes; the engine proves. Single-shot for now (the iterative loop is
/// authored as a Nano process in a later brick).
async fn harness_evolve(
    State(state): State<AppState>,
    Json(req): Json<EvolveRequest>,
) -> impl IntoResponse {
    // Resolve LLM config (env + persisted settings + per-request override); a model
    // name is required.
    let cfg = resolve_llm(&state, req.llm.as_ref());
    if !cfg.is_ready() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no LLM model configured; set it in the console settings (cog, \
                          lower-left) or via PROCESSOS_LLM_MODEL (and PROCESSOS_LLM_BASE_URL \
                          / PROCESSOS_LLM_PROVIDER as needed), or pass an `llm` object with \
                          at least `model`"
            })),
        )
            .into_response();
    }

    // The baseline must parse so we can derive the default process id and ground
    // the prompt; a broken baseline is the caller's error.
    let defs = match parse_bpmn(&req.baseline_model) {
        Ok(d) if !d.is_empty() => d,
        _ => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": "baseline model failed to parse" })),
            )
                .into_response();
        }
    };
    let process_id = req.process_id.clone().unwrap_or_else(|| defs[0].id.clone());

    // Resolve the system prompt: inline override, else library prompt, else default.
    let system_prompt = match (&req.prompt, &req.prompt_id) {
        (Some(inline), _) => inline.clone(),
        (None, Some(id)) => match state
            .prompts
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .system_of(id)
        {
            Some(s) => s,
            None => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(serde_json::json!({ "error": format!("unknown prompt id: {id}") })),
                )
                    .into_response();
            }
        },
        (None, None) => DEFAULT_EVOLVE_SYSTEM_PROMPT.to_string(),
    };

    // Fetch + distil the recorded dataset (this is the fitness data).
    let client = match &req.base_url {
        Some(url) if !url.is_empty() => NanoClient::new(url),
        _ => state.active_target(),
    };
    let limit = req.limit.unwrap_or(200);
    let summaries = match client.list_traces(limit).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let (dataset, matched, skipped, skip_reasons) =
        distil_recorded_dataset(&client, &process_id, &summaries).await;
    if dataset.is_empty() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "error": "no replayable recorded instances for this process \
                          (was the gateway run with --capture?)",
                "processId": process_id,
                "matchedInstances": matched,
                "skipped": skipped,
                "skipReasons": skip_reasons,
            })),
        )
            .into_response();
    }

    // Distil the production signal and ask the model to propose redesigns.
    let signal = summarize_dataset(&process_id, &dataset);
    let user_prompt = build_evolve_prompt(&signal, &req.baseline_model, req.pilot_note.as_deref());
    let raw = match llm_complete(&cfg, &system_prompt, &user_prompt).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": e, "signal": signal })),
            )
                .into_response();
        }
    };
    let candidates: Vec<CandidateModel> = match parse_structural_candidates(&raw) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": e, "signal": signal, "llmRaw": raw })),
            )
                .into_response();
        }
    };

    // Prove each proposal against the recorded history.
    let ranking = rank_candidates_by_replay(&candidates, &dataset, Some(&process_id));
    Json(serde_json::json!({
        "processId": process_id,
        "matchedInstances": matched,
        "skipped": skipped,
        "skipReasons": skip_reasons,
        "signal": signal,
        "proposed": candidates.len(),
        "ranking": ranking,
    }))
    .into_response()
}
/// `GET /api/prompts` — list every prompt in the library (built-in + authored).
async fn prompts_list(State(state): State<AppState>) -> impl IntoResponse {
    let list = state
        .prompts
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .list();
    Json(list)
}

/// `GET /api/prompts/{id}` — one prompt, or 404.
async fn prompts_get(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state
        .prompts
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&id)
    {
        Some(p) => Json(p).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such prompt: {id}") })),
        )
            .into_response(),
    }
}

/// `POST /api/prompts` — author or import a prompt (create/update by id). Returns the
/// stored prompt. 422 on a validation failure (empty id or system text).
async fn prompts_upsert(
    State(state): State<AppState>,
    Json(prompt): Json<Prompt>,
) -> impl IntoResponse {
    match state
        .prompts
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .upsert(prompt)
    {
        Ok(stored) => (StatusCode::OK, Json(stored)).into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// `DELETE /api/prompts/{id}` — remove an authored prompt. 422 when it is built-in or
/// unknown (the message distinguishes the two).
async fn prompts_delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state
        .prompts
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&id)
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// A pilot fork submitted by the operator.
#[derive(Debug, Deserialize)]
struct PilotUpdate {
    /// The forked pilot BPMN XML.
    xml: String,
}

/// `GET /api/pilot` — the current pilot process (§10 surface a): its source
/// (`default`/`forked`), declared process ids, and XML.
async fn pilot_get(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.pilot.doc())
}

/// `PUT /api/pilot` — author a fork of the pilot process. Validates the BPMN, persists
/// it durably, then **hot-redeploys it to the own engine** so the next experiment runs
/// the operator's choreography. 422 on invalid BPMN (nothing changes); 502 if the
/// engine deploy fails after a successful save (the fork is kept). The response carries
/// the new doc plus a `warning` when the cockpit's expected process id is absent.
async fn pilot_put(
    State(state): State<AppState>,
    Json(body): Json<PilotUpdate>,
) -> impl IntoResponse {
    let doc = match state.pilot.save(&body.xml) {
        Ok(doc) => doc,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    redeploy_pilot(&state, doc).await
}

/// `POST /api/pilot/reset` — restore the built-in default pilot and hot-redeploy it.
async fn pilot_reset(State(state): State<AppState>) -> impl IntoResponse {
    let doc = state.pilot.reset();
    redeploy_pilot(&state, doc).await
}

/// Deploy the (already-persisted) pilot doc to the own engine and shape the response.
async fn redeploy_pilot(state: &AppState, doc: pilot::PilotDoc) -> axum::response::Response {
    let warning = (!doc.process_ids.iter().any(|id| id == cockpit::PILOT_PROCESS_ID)).then(|| {
        format!(
            "pilot declares {:?}, not '{}' — the cockpit creates experiments on '{}', so they will fail until the process id matches",
            doc.process_ids, cockpit::PILOT_PROCESS_ID, cockpit::PILOT_PROCESS_ID
        )
    });
    match state.own.deploy_bpmn(doc.deploy_filename, &doc.xml).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "pilot": doc,
                "deployed": true,
                "warning": warning,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "pilot": doc,
                "deployed": false,
                "error": format!("saved, but redeploy to own engine failed: {e}"),
                "warning": warning,
            })),
        )
            .into_response(),
    }
}

/// Query for the production live-source baseline path.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProductionQuery {
    /// Which process to baseline. When absent, the busiest sampled process is used.
    process_id: Option<String>,
    /// How many recent trace summaries to scan.
    limit: Option<usize>,
    /// How many of those to pull full detail for (per-element aggregation).
    sample: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterQuery {
    /// Which process to summarize. When absent, the busiest sampled process is used.
    process_id: Option<String>,
    /// How many recent trace summaries to scan (per node).
    limit: Option<usize>,
    /// How many of those to pull full detail for (the queue/service split).
    sample: Option<usize>,
    /// When set, attach the regime-(b) queueing model's per-job-type staffing
    /// recommendation that holds this p99 queue-wait (ms).
    target_p99_ms: Option<u64>,
}

/// `GET /api/harness/production` — the **generation-skipped** production path:
/// fold the live cluster's traces (T1 contract) into the baseline a candidate
/// search must beat for one process. On a Nano read failure we return 502; on an
/// empty/unknown process we return 422.
async fn harness_production(
    State(state): State<AppState>,
    Query(q): Query<ProductionQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    match build_baseline(
        &state.active_target(),
        q.process_id.as_deref(),
        limit,
        sample,
    )
    .await
    {
        Ok(baseline) => Json(baseline).into_response(),
        // A read-contract failure surfaces the underlying GET error; an empty or
        // unknown-process result is a request the caller can fix, so 422.
        Err(e) if e.starts_with("GET ") || e.starts_with("decode ") => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// `GET /api/harness/cluster` — the **at-scale measurement** half of the M3
/// ClusterRunner: summarize a live run (throughput, e2e tail p50/p95/p99, the
/// queue/service split under load, per-job-type breakdown, and per-node backlog
/// sensing) for one process, unioned across every cluster node. When
/// `targetP99Ms` is supplied, it also attaches `staffing`: the regime-(b) queueing
/// model's per-job-type worker-count recommendation to hold that p99 queue-wait.
/// Point it at a cluster the perf-matrix is driving. 502 on a Nano read failure, 422
/// on an empty/unknown process.
async fn harness_cluster(
    State(state): State<AppState>,
    Query(q): Query<ClusterQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(500).clamp(1, 1000);
    let sample = q.sample.unwrap_or(50).clamp(1, 500);
    match build_cluster_summary(
        &state.active_target(),
        q.process_id.as_deref(),
        limit,
        sample,
    )
    .await
    {
        Ok(summary) => match q.target_p99_ms {
            // Attach the staffing recommendation derived from the measured run.
            Some(target) => {
                let staffing = staff_for_summary(&summary, target);
                let mut body = serde_json::to_value(&summary).unwrap_or_default();
                if let Some(obj) = body.as_object_mut() {
                    obj.insert(
                        "staffing".to_string(),
                        serde_json::to_value(staffing).unwrap_or_default(),
                    );
                }
                Json(body).into_response()
            }
            None => Json(summary).into_response(),
        },
        Err(e) if e.starts_with("GET ") || e.starts_with("decode ") => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// Run a scenario off the async runtime (the SimRunner is synchronous CPU work).
async fn run_scenario_blocking(scenario: Scenario) -> axum::response::Response {
    let result = tokio::task::spawn_blocking(move || run_scenario(&scenario)).await;
    match result {
        Ok(Ok(report)) => Json(report).into_response(),
        Ok(Err(e)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("harness task failed: {e}") })),
        )
            .into_response(),
    }
}

/// `GET /harness` — a tiny dashboard that runs the example and renders the ranking.
async fn harness_dashboard() -> Html<&'static str> {
    Html(HARNESS_HTML)
}

/// The cockpit single-file app (design §7.8 / §10), served at `/cockpit`.
const COCKPIT_HTML: &str = include_str!("cockpit.html");

const DASHBOARD_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Nano ProcessOS — Live instance</title>
<link rel="stylesheet" href="/assets/theme.css">
<script src="/assets/theme.js"></script>
<style>
  /* All colours resolve through the --po-* design tokens (/assets/theme.css); the
     light/dark/system choice is applied by /assets/theme.js before first paint. */
  body { margin: 0; font: 14px/1.5 system-ui, sans-serif; background: var(--po-app); color: var(--po-text); }
  a { color: inherit; text-decoration: none; }
  .app { display: grid; grid-template-columns: 220px 1fr; min-height: 100vh; }
  .rail { border-right: 1px solid var(--po-edge); padding: 18px 14px; background: var(--po-panel); }
  .rail .brand { display: block; font-weight: 700; font-size: 16px; letter-spacing: .02em; margin-bottom: 2px; }
  .rail .brand .dot { color: var(--po-accent-strong); }
  .rail .tag { color: var(--po-text-faint); font-size: 11px; margin-bottom: 20px; }
  .rail nav a { display: block; padding: 7px 10px; border-radius: 7px; color: var(--po-text-muted); font-weight: 500; margin-bottom: 2px; }
  .rail nav a:hover { background: var(--po-hover); color: var(--po-text); }
  .rail nav a.active { background: var(--po-hover); color: var(--po-accent-soft); }
  .content { min-width: 0; }
  header { padding: 16px 24px; border-bottom: 1px solid var(--po-edge); display: flex; align-items: baseline; gap: 12px; }
  h1 { font-size: 18px; margin: 0; }
  .sub { color: var(--po-text-faint); font-size: 12px; }
  main { padding: 24px; max-width: 1100px; }
  .cards { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 24px; }
  .card { background: var(--po-raised); border: 1px solid var(--po-edge); border-radius: 8px; padding: 12px 16px; min-width: 120px; }
  .card .n { font-size: 22px; font-weight: 600; }
  .card .l { color: var(--po-text-muted); font-size: 12px; }
  h2 { font-size: 14px; color: var(--po-text-muted); margin: 24px 0 8px; text-transform: uppercase; letter-spacing: .04em; }
  table { width: 100%; border-collapse: collapse; margin-bottom: 16px; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid var(--po-edge); }
  th { color: var(--po-text-muted); font-weight: 500; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .pid { font-family: ui-monospace, monospace; color: var(--po-accent-strong); }
  .bottleneck { color: var(--po-danger-soft); }
  .err { color: var(--po-danger-soft); background: var(--po-danger-wash); border: 1px solid var(--po-danger-edge); padding: 12px 16px; border-radius: 8px; }
  button { background: var(--po-raised); color: var(--po-text); border: 1px solid var(--po-edge-strong); border-radius: 6px; padding: 4px 10px; cursor: pointer; }
  button:hover { background: var(--po-hover); }
  button.primary { background: var(--po-accent); border-color: var(--po-accent); color: var(--po-on-accent); }
  button.primary:hover { background: var(--po-accent-strong); }
  button.danger { color: var(--po-danger-soft); }
  /* Nano instance manager */
  .inst-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 12px; margin-bottom: 12px; }
  .inst { background: var(--po-raised); border: 1px solid var(--po-edge); border-radius: 10px; padding: 14px; display: flex; gap: 12px; align-items: flex-start; }
  .inst.active { border-color: var(--po-accent); box-shadow: 0 0 0 1px var(--po-accent) inset; }
  .inst.starred { border-color: var(--po-warn-edge); }
  .inst.active.starred { border-color: var(--po-accent); }
  .inst .logo { flex: 0 0 auto; }
  .inst .meta { min-width: 0; flex: 1; }
  .inst .nm { font-weight: 600; font-size: 14px; display: flex; align-items: center; gap: 8px; }
  .star { background: none; border: none; padding: 0 0 0 2px; margin-left: auto; cursor: pointer; font-size: 16px; line-height: 1; color: var(--po-text-faint); }
  .star:hover { color: var(--po-warn); }
  .star.on { color: var(--po-warn); }
  .inst .url { color: var(--po-text-muted); font-family: ui-monospace, monospace; font-size: 12px; word-break: break-all; }
  .inst .acts { margin-top: 8px; display: flex; gap: 6px; flex-wrap: wrap; }
  .inst .acts button { padding: 3px 9px; font-size: 12px; }
  .badge { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: .04em; color: var(--po-accent-soft); background: var(--po-accent-wash); border: 1px solid var(--po-accent-edge); padding: 1px 6px; border-radius: 999px; }
  .rdot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; background: var(--po-text-faint); flex: 0 0 auto; }
  .rdot.up { background: var(--po-ok); box-shadow: 0 0 6px color-mix(in srgb, var(--po-ok) 53%, transparent); }
  .rdot.down { background: var(--po-danger); }
  .rdot.probing { background: var(--po-warn); animation: pulse 1s infinite; }
  @keyframes pulse { 50% { opacity: .35; } }
  .empty { background: var(--po-panel); border: 1px dashed var(--po-edge-strong); border-radius: 10px; padding: 22px; color: var(--po-text-muted); }
  .empty .big { color: var(--po-text); font-size: 15px; font-weight: 600; margin-bottom: 6px; }
  .inst-form { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin: 6px 0 4px; }
  .inst-form input { background: var(--po-inset); border: 1px solid var(--po-edge-strong); color: var(--po-text); border-radius: 6px; padding: 6px 9px; font-size: 13px; }
  .inst-form input.nm { width: 160px; }
  .inst-form input.url { width: 240px; font-family: ui-monospace, monospace; }
</style>
</head>
<body>
<div class="app">
  <aside class="rail">
    <a class="brand" href="/">Nano Process<span class="dot">OS</span></a>
    <div class="tag">live instance</div>
    <nav>
      <a href="/console" class="active">Console</a>
      <a href="/workspace">Workspaces</a>
      <a href="/cockpit">Cockpit</a>
      <a href="/harness">Harness</a>
      <a href="/features">Features</a>
    </nav>
    <div id="railTheme" style="margin-top:18px"></div>
  </aside>
  <div class="content">
    <header>
      <h1>Live instance</h1>
      <span class="sub">&middot; <span id="nano"></span></span>
      <button onclick="refreshAll()" style="margin-left:auto">Refresh</button>
    </header>
    <main>
      <div id="instances">Loading instances…</div>
      <div id="out"></div>
    </main>
  </div>
</div>
<script>
function ms(v){ return v==null ? '—' : (v>=1000 ? (v/1000).toFixed(2)+'s' : v+'ms'); }
function esc(s){ return String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

// A self-contained "Nano node" mark: a violet hexagonal nucleus with an orbiting electron.
function nanoLogo(size){
  const s = size||34;
  return '<svg class="logo" width="'+s+'" height="'+s+'" viewBox="0 0 40 40" fill="none" aria-hidden="true">'
    + '<defs><radialGradient id="ng" cx="50%" cy="40%" r="65%">'
    + '<stop offset="0%" stop-color="var(--po-accent-soft)"/><stop offset="60%" stop-color="var(--po-accent-strong)"/><stop offset="100%" stop-color="var(--po-accent)"/>'
    + '</radialGradient></defs>'
    + '<ellipse cx="20" cy="20" rx="17" ry="7" stroke="var(--po-accent)" stroke-width="1.4" opacity=".55" transform="rotate(35 20 20)"/>'
    + '<ellipse cx="20" cy="20" rx="17" ry="7" stroke="var(--po-accent-strong)" stroke-width="1.4" opacity=".55" transform="rotate(-35 20 20)"/>'
    + '<path d="M20 9 L29 14.5 L29 25.5 L20 31 L11 25.5 L11 14.5 Z" fill="url(#ng)"/>'
    + '<circle cx="34" cy="13" r="2.4" fill="var(--po-accent-strong)"/>'
    + '</svg>';
}

let INSTANCES = [];
let ACTIVE = null;

async function loadInstances(){
  const wrap = document.getElementById('instances');
  let r;
  try { r = await fetch('/api/nano/instances'); }
  catch(e){ wrap.innerHTML = '<div class="err">Cannot reach ProcessOS API: '+esc(e)+'</div>'; return; }
  const d = await r.json();
  INSTANCES = d.instances || [];
  ACTIVE = d.active;
  renderInstances();
  for(const i of INSTANCES) probe(i.id);
}

function renderInstances(){
  const wrap = document.getElementById('instances');
  let h = '<h2>Nano instances</h2><div class="inst-grid">';
  for(const i of INSTANCES){
    const act = i.id===ACTIVE;
    h += '<div class="inst'+(act?' active':'')+(i.starred?' starred':'')+'" data-id="'+esc(i.id)+'">'
       + nanoLogo(34)
       + '<div class="meta">'
       + '<div class="nm"><span class="rdot probing" id="rdot-'+esc(i.id)+'"></span>'+esc(i.name)
       + (act?' <span class="badge">active</span>':'')
       + '<button class="star'+(i.starred?' on':'')+'" title="'+(i.starred?'Unstar':'Star (pin to top)')+'" onclick="starInst(\''+esc(i.id)+'\','+(i.starred?'false':'true')+')">'+(i.starred?'★':'☆')+'</button>'
       + '</div>'
       + '<div class="url">'+esc(i.baseUrl)+'</div>'
       + '<div class="acts">'
       + (act?'':'<button class="primary" onclick="selectInst(\''+esc(i.id)+'\')">Select</button>')
       + '<button onclick="editInst(\''+esc(i.id)+'\')">Edit</button>'
       + (INSTANCES.length>1?'<button class="danger" onclick="removeInst(\''+esc(i.id)+'\')">Delete</button>':'')
       + '</div></div></div>';
  }
  h += '</div>';
  h += '<div class="inst-form">'
     + '<input class="nm" id="newName" placeholder="Name (e.g. Staging)" />'
     + '<input class="url" id="newUrl" placeholder="http://localhost:8080" />'
     + '<button class="primary" onclick="addInst()">Add instance</button>'
     + '<span class="sub" id="instMsg"></span>'
     + '</div>';
  wrap.innerHTML = h;
}

async function probe(id){
  const dot = document.getElementById('rdot-'+id);
  if(dot) dot.className = 'rdot probing';
  try {
    const r = await fetch('/api/nano/instances/'+encodeURIComponent(id)+'/health');
    const d = await r.json();
    if(dot) dot.className = 'rdot ' + (d.ok ? 'up' : 'down');
    if(dot) dot.title = d.ok ? 'reachable' : 'unreachable';
  } catch(e){ if(dot){ dot.className = 'rdot down'; dot.title='unreachable'; } }
}

async function selectInst(id){
  await fetch('/api/nano/instances/'+encodeURIComponent(id)+'/select', {method:'POST'});
  await refreshAll();
}
async function starInst(id, starred){
  await fetch('/api/nano/instances/'+encodeURIComponent(id)+'/star', {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({starred})});
  await loadInstances();
}
async function removeInst(id){
  if(!confirm('Delete this instance?')) return;
  await fetch('/api/nano/instances/'+encodeURIComponent(id), {method:'DELETE'});
  await refreshAll();
}
async function addInst(){
  const name = document.getElementById('newName').value.trim();
  const url = document.getElementById('newUrl').value.trim();
  const msg = document.getElementById('instMsg');
  if(!url){ if(msg) msg.textContent = 'A base URL is required.'; return; }
  const r = await fetch('/api/nano/instances', {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({name, baseUrl: url})});
  if(!r.ok){ const e = await r.json().catch(()=>({})); if(msg) msg.textContent = e.error || ('Failed ('+r.status+')'); return; }
  await refreshAll();
}
async function editInst(id){
  const inst = INSTANCES.find(x=>x.id===id);
  if(!inst) return;
  const name = prompt('Instance name:', inst.name);
  if(name===null) return;
  const url = prompt('Base URL:', inst.baseUrl);
  if(url===null) return;
  const r = await fetch('/api/nano/instances/'+encodeURIComponent(id), {method:'PUT', headers:{'content-type':'application/json'}, body: JSON.stringify({name, baseUrl: url})});
  if(!r.ok){ const e = await r.json().catch(()=>({})); alert(e.error || ('Failed ('+r.status+')')); return; }
  await refreshAll();
}

async function load(){
  const out = document.getElementById('out');
  out.innerHTML = '<p class="sub">Loading insights…</p>';
  let r;
  try { r = await fetch('/api/insights'); }
  catch(e){ renderEmpty(null, 'Cannot reach the ProcessOS API: '+esc(e)); return; }
  const d = await r.json();
  if(!r.ok){
    if(d.unreachable){ renderEmpty(d.baseUrl, null); }
    else { out.innerHTML = '<div class="err">Nano read failed: '+esc(d.error||r.status)+'</div>'; }
    return;
  }
  document.getElementById('nano').textContent = d.nanoBaseUrl;
  const t = d.totals, live = d.live || {};
  let h = '<h2>Insights</h2><div class="cards">'
    + card(t.instances, 'instances') + card(t.completed, 'completed')
    + card(t.active, 'active') + card(t.incidents, 'incidents')
    + card(d.sampledInstances, 'sampled')
    + (live.activeInstances!=null ? card(live.activeInstances, 'live active') : '')
    + '</div>';

  h += '<h2>Processes</h2>';
  if(!d.processes.length){ h += '<p class="sub">No sampled traces yet. Start some instances on the cluster.</p>'; }
  for(const p of d.processes){
    h += '<h3 class="pid">'+esc(p.processId)+'</h3>';
    h += '<div class="sub">'+p.instances+' sampled · '+p.completed+' completed · '
       + p.incidentInstances+' with incidents · avg '+ms(p.avgDurationMs)+' · p95 '+ms(p.p95DurationMs)
       + (p.bottleneck ? ' · bottleneck <span class="bottleneck">'+esc(p.bottleneck.elementId)+'</span> '+ms(p.bottleneck.avgDurationMs) : '')
       + '</div>';
    h += '<table><thead><tr><th>Element</th><th class="num">count</th><th class="num">avg</th>'
       + '<th class="num">queue</th><th class="num">service</th><th class="num">incidents</th><th class="num">job fails</th></tr></thead><tbody>';
    for(const e of p.elements){
      const bn = p.bottleneck && e.elementId===p.bottleneck.elementId;
      h += '<tr'+(bn?' class="bottleneck"':'')+'><td>'+esc(e.elementId)+'</td>'
         + '<td class="num">'+e.count+'</td><td class="num">'+ms(e.avgDurationMs)+'</td>'
         + '<td class="num">'+ms(e.avgQueueMs)+'</td><td class="num">'+ms(e.avgServiceMs)+'</td>'
         + '<td class="num">'+e.incidents+'</td><td class="num">'+e.jobFailures+'</td></tr>';
    }
    h += '</tbody></table>';
  }

  if(d.incidentClusters.length){
    h += '<h2>Incident clusters</h2><table><thead><tr><th>Element</th><th>Kind</th><th class="num">count</th><th>Top reason</th></tr></thead><tbody>';
    for(const c of d.incidentClusters){
      h += '<tr><td>'+esc(c.elementId)+'</td><td>'+esc(c.kind)+'</td><td class="num">'+c.count+'</td><td>'+esc(c.topReason)+'</td></tr>';
    }
    h += '</tbody></table>';
  }
  out.innerHTML = h;
}

function renderEmpty(baseUrl, detail){
  document.getElementById('nano').textContent = baseUrl || '—';
  const where = baseUrl ? (' at <span class="pid">'+esc(baseUrl)+'</span>') : '';
  document.getElementById('out').innerHTML = '<div class="empty">'
    + nanoLogo(40)
    + '<div class="big" style="margin-top:10px">No Nano instance available'+where+'</div>'
    + '<div>Start your Nano server, or select / add another instance above. '
    + 'The active instance is where ProcessOS reads traces and metrics from.</div>'
    + (detail ? '<div class="sub" style="margin-top:8px">'+detail+'</div>' : '')
    + '</div>';
}

async function refreshAll(){ await loadInstances(); await load(); }
function card(n,l){ return '<div class="card"><div class="n">'+n+'</div><div class="l">'+l+'</div></div>'; }
refreshAll();
if(window.poMountThemeToggle) poMountThemeToggle(document.getElementById('railTheme'));
</script>
<script src="/assets/settings.js"></script>
</body>
</html>
"##;

/// The harness dashboard: runs the bundled example and renders the ranked
/// candidates. Dependency-free; the richer UX is the console "Optimization" tab.
const HARNESS_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Nano ProcessOS — Optimization Harness</title>
<link rel="stylesheet" href="/assets/theme.css">
<script src="/assets/theme.js"></script>
<style>
  /* All colours resolve through the --po-* design tokens (/assets/theme.css); the
     light/dark/system choice is applied by /assets/theme.js before first paint. */
  body { margin: 0; font: 14px/1.5 system-ui, sans-serif; background: var(--po-app); color: var(--po-text); }
  header { padding: 20px 24px; border-bottom: 1px solid var(--po-edge); }
  h1 { margin: 0; font-size: 18px; }
  .sub { color: var(--po-text-muted); margin-top: 4px; }
  main { padding: 24px; max-width: 1000px; }
  .card { background: var(--po-raised); border: 1px solid var(--po-edge); border-radius: 10px; padding: 16px 18px; margin-bottom: 18px; }
  .notes { color: var(--po-text-muted); font-size: 13px; }
  .notes li { margin: 2px 0; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--po-edge); }
  th { color: var(--po-text-muted); font-weight: 600; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .ok { color: var(--po-ok); }
  .bad { color: var(--po-danger); }
  .best { background: var(--po-ok-wash); }
  .pill { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 12px; border: 1px solid var(--po-edge); }
  .pill.win { color: var(--po-ok); border-color: var(--po-ok-edge); }
  code { color: var(--po-warn); }
</style>
</head>
<body>
<header>
  <div id="hTheme" style="float:right;margin-left:12px"></div>
  <h1><a href="/" style="color:inherit;text-decoration:none">Nano ProcessOS</a> — Optimization Harness</h1>
  <div class="sub">SimRunner over the bundled example scenario (worker-swap transform space). The same loop runs in production against live Nano traces.</div>
  <div class="sub" style="margin-top:8px"><a href="/" style="color:var(--po-accent-strong);text-decoration:none">Home</a> · <a href="/workspace" style="color:var(--po-accent-strong);text-decoration:none">Workspaces</a> · <a href="/features" style="color:var(--po-accent-strong);text-decoration:none">Features</a> · <a href="/console" style="color:var(--po-accent-strong);text-decoration:none">Console</a></div>
</header>
<main id="root">Running the example scenario…</main>
<script>
function pct(x) { return (x * 100).toFixed(0) + "%"; }
function num(x, d) { return Number(x).toFixed(d === undefined ? 2 : d); }
async function load() {
  const root = document.getElementById("root");
  try {
    const res = await fetch("/api/harness/example/run");
    if (!res.ok) { root.textContent = "Harness error: " + res.status; return; }
    const r = await res.json();
    const golden = r.golden ? r.golden.name : "—";
    const rows = r.variants.map((v, i) => {
      const cls = i === 0 ? "best" : "";
      const feas = v.feasible ? '<span class="ok">yes</span>' : '<span class="bad">no</span>';
      return `<tr class="${cls}">
        <td>${i + 1}</td>
        <td>${v.name}${i === 0 ? ' <span class="pill win">best</span>' : ''}</td>
        <td>${v.source}</td>
        <td class="num">${num(v.avgCost)}</td>
        <td class="num">${num(v.avgLatencyMs, 0)}</td>
        <td class="num">${pct(v.correctnessRate)}</td>
        <td class="num">${pct(v.incidentRate)}</td>
        <td class="num">${pct(v.completionRate)}</td>
        <td class="num">${feas}</td>
      </tr>`;
    }).join("");
    root.innerHTML = `
      <div class="card">
        <div><strong>${r.scenario}</strong> &middot; process <code>${r.processId}</code> &middot; ${r.inputs} inputs &middot; minimize <code>${r.objective.minimize}</code></div>
        <div class="sub" style="margin-top:6px">
          Golden: <code>${golden}</code> &middot;
          recovered: ${r.recoveredGolden ? '<span class="ok">yes</span>' : '<span class="bad">no</span>'}
          ${r.goldenDistance != null ? ` (distance ${r.goldenDistance})` : ''}
          &middot; best: <code>${r.best}</code>
        </div>
      </div>
      <div class="card">
        <table>
          <thead><tr>
            <th>#</th><th>candidate</th><th>source</th><th class="num">avg cost</th><th class="num">avg latency (ms)</th>
            <th class="num">correct</th><th class="num">incidents</th><th class="num">completed</th><th class="num">feasible</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
      <div class="card notes">
        <strong>Notes</strong>
        <ul>${r.notes.map(n => `<li>${n}</li>`).join("")}</ul>
      </div>`;
  } catch (e) {
    root.textContent = "Harness error: " + e;
  }
}
load();
if (window.poMountThemeToggle) poMountThemeToggle(document.getElementById("hTheme"));
</script>
</body>
</html>
"##;

#[cfg(test)]
mod config_tests {
    use super::resolve_nano_urls;

    #[test]
    fn defaults_both_roles_to_the_local_gateway() {
        let (t, o) = resolve_nano_urls(None, None, None);
        assert_eq!(t, "http://localhost:8080");
        assert_eq!(o, "http://localhost:8080");
    }

    #[test]
    fn nano_base_url_aliases_both_roles() {
        let (t, o) = resolve_nano_urls(Some("http://shared:9".into()), None, None);
        assert_eq!(t, "http://shared:9");
        assert_eq!(o, "http://shared:9");
    }

    #[test]
    fn role_specific_urls_override_the_alias_independently() {
        let (t, o) = resolve_nano_urls(
            Some("http://base:8080".into()),
            Some("http://client-prod:8080".into()),
            Some("http://processos-own:8081".into()),
        );
        assert_eq!(t, "http://client-prod:8080", "target = client's production");
        assert_eq!(o, "http://processos-own:8081", "own = ProcessOS's engine");
    }

    #[test]
    fn a_single_role_override_leaves_the_other_on_the_alias() {
        let (t, o) = resolve_nano_urls(
            Some("http://base:8080".into()),
            None,
            Some("http://own:8081".into()),
        );
        assert_eq!(t, "http://base:8080");
        assert_eq!(o, "http://own:8081");
    }
}

#[cfg(test)]
mod sidecar_phase_tests {
    use std::fs;

    use super::{download_in_progress, model_downloaded};

    #[test]
    fn detects_an_in_progress_hf_download_and_reports_its_size() {
        let dir = std::env::temp_dir().join(format!("se-dl-{}", std::process::id()));
        let blobs = dir
            .join("models--unsloth--Qwen3-Coder-30B-A3B-Instruct-GGUF")
            .join("blobs");
        fs::create_dir_all(&blobs).unwrap();
        fs::write(blobs.join("abc123.downloadInProgress"), vec![0u8; 4096]).unwrap();
        // A completed blob (no suffix) must NOT be mistaken for a download.
        fs::write(blobs.join("def456"), vec![0u8; 8192]).unwrap();

        let model = "unsloth/Qwen3-Coder-30B-A3B-Instruct-GGUF:UD-Q4_K_XL";
        assert_eq!(download_in_progress(&dir, model), Some(4096));

        // Once the partial file is gone, nothing is downloading.
        fs::remove_file(blobs.join("abc123.downloadInProgress")).unwrap();
        assert_eq!(download_in_progress(&dir, model), None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn no_download_for_an_unknown_model_dir() {
        let dir = std::env::temp_dir().join("se-dl-absent");
        assert_eq!(download_in_progress(&dir, "org/repo:tag"), None);
    }

    #[test]
    fn model_downloaded_tracks_the_hf_cache_lifecycle() {
        let dir = std::env::temp_dir().join(format!("se-dlc-{}", std::process::id()));
        let repo = dir.join("models--unsloth--Qwen3-8B-GGUF");
        let blobs = repo.join("blobs");
        let snap = repo.join("snapshots").join("rev0");
        fs::create_dir_all(&blobs).unwrap();
        fs::create_dir_all(&snap).unwrap();
        let model = "unsloth/Qwen3-8B-GGUF:UD-Q4_K_XL";

        // Mid-download: only a .downloadInProgress blob, no snapshot file yet.
        fs::write(blobs.join("sha1.downloadInProgress"), vec![0u8; 4096]).unwrap();
        assert!(
            !model_downloaded(&dir, model),
            "partial download is not downloaded"
        );

        // Complete: a snapshot .gguf (whose name carries the quant) backed by a real blob, and the
        // partial file gone.
        fs::remove_file(blobs.join("sha1.downloadInProgress")).unwrap();
        let blob = blobs.join("sha1");
        fs::write(&blob, vec![0u8; 8192]).unwrap();
        let gguf = snap.join("Qwen3-8B-UD-Q4_K_XL.gguf");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&blob, &gguf).unwrap();
        #[cfg(not(unix))]
        fs::write(&gguf, vec![0u8; 8192]).unwrap();
        assert!(
            model_downloaded(&dir, model),
            "complete cache reads as downloaded"
        );

        // A different quant of the same repo must not read as downloaded.
        assert!(
            !model_downloaded(&dir, "unsloth/Qwen3-8B-GGUF:Q8_0"),
            "a different quant is not satisfied by an existing one"
        );

        // A still-in-progress blob alongside the complete snapshot still reads as not-ready.
        fs::write(blobs.join("sha2.downloadInProgress"), vec![0u8; 16]).unwrap();
        assert!(
            !model_downloaded(&dir, model),
            "an active partial blocks downloaded"
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn model_downloaded_handles_a_local_gguf_path() {
        let dir = std::env::temp_dir().join(format!("se-dll-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        assert!(!model_downloaded(&dir, "local-model.gguf"));
        fs::write(dir.join("local-model.gguf"), vec![0u8; 16]).unwrap();
        assert!(model_downloaded(&dir, "local-model.gguf"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sharded_model_resolves_to_its_first_shard() {
        let dir = std::env::temp_dir().join(format!("se-shard-{}", std::process::id()));
        let snap = dir
            .join("models--org--Big-GGUF")
            .join("snapshots")
            .join("rev0");
        fs::create_dir_all(&snap).unwrap();
        // Write shards out of order — resolution must still pick the first (which carries the
        // full GGUF metadata a header scan needs).
        fs::write(snap.join("big-q4-00002-of-00002.gguf"), vec![0u8; 8]).unwrap();
        fs::write(snap.join("big-q4-00001-of-00002.gguf"), vec![0u8; 8]).unwrap();
        let path = super::resolve_model_gguf(&dir, "org/Big-GGUF:q4").expect("resolves");
        assert!(
            path.to_string_lossy().contains("00001-of-00002"),
            "expected first shard, got {}",
            path.display()
        );
        fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod speculator_gate_tests {
    use super::{profile_model_spec, validate_speculator_pairing};
    use crate::pairings::{PairMode, Pairing};
    use crate::settings::{LlmProfile, Settings};

    fn profile(id: &str, sidecar: bool, model_file: Option<&str>) -> LlmProfile {
        LlmProfile {
            id: id.into(),
            name: id.into(),
            provider: Some("openai".into()),
            base_url: None,
            model: None,
            api_key: None,
            max_tokens: None,
            context_window: None,
            temperature: None,
            sidecar,
            model_file: model_file.map(String::from),
            sidecar_args: None,
            mtp: false,
            thinking_level: None,
            starred: false,
        }
    }

    fn pairing(secondary: &str, primary: Option<&str>) -> Pairing {
        Pairing {
            id: "spec".into(),
            name: "Speculator".into(),
            mode: PairMode::Speculator,
            secondary_profile_id: secondary.into(),
            primary_profile_id: primary.map(String::from),
            system: String::new(),
            max_rounds: None,
            digest_cap: None,
            draft_max: None,
            draft_min: None,
            primary_tools: None,
            secondary_tools: None,
            builtin: false,
        }
    }

    fn settings(profiles: Vec<LlmProfile>) -> Settings {
        Settings {
            profiles,
            ..Default::default()
        }
    }

    #[test]
    fn rejects_unknown_or_remote_or_modelless_draft_profiles() {
        let snap = settings(vec![
            profile("target", true, Some("t.gguf")),
            profile("remote", false, None),
            profile("empty", true, None),
        ]);
        let err = validate_speculator_pairing(&snap, &pairing("nope", None)).unwrap_err();
        assert!(err.contains("unknown draft profile"), "{err}");
        let err = validate_speculator_pairing(&snap, &pairing("remote", None)).unwrap_err();
        assert!(err.contains("not a local sidecar"), "{err}");
        let err = validate_speculator_pairing(&snap, &pairing("empty", None)).unwrap_err();
        assert!(err.contains("no model file"), "{err}");
    }

    #[test]
    fn rejects_a_pinned_primary_that_is_not_a_sidecar() {
        let snap = settings(vec![
            profile("draft", true, Some("d.gguf")),
            profile("cloud", false, None),
        ]);
        let err = validate_speculator_pairing(&snap, &pairing("draft", Some("cloud"))).unwrap_err();
        assert!(err.contains("not a local sidecar"), "{err}");
        let err = validate_speculator_pairing(&snap, &pairing("draft", Some("ghost"))).unwrap_err();
        assert!(err.contains("unknown primary"), "{err}");
    }

    #[test]
    fn allows_authoring_before_the_models_are_downloaded() {
        // Neither GGUF exists on disk — compatibility cannot be proven yet, so the save is
        // allowed; the launch-time gate re-checks with the real files.
        let snap = settings(vec![
            profile("target", true, Some("/nonexistent/t.gguf")),
            profile("draft", true, Some("/nonexistent/d.gguf")),
        ]);
        assert!(validate_speculator_pairing(&snap, &pairing("draft", Some("target"))).is_ok());
        // The same model on both sides ("loaded twice") is a legal speculator config.
        assert!(validate_speculator_pairing(&snap, &pairing("target", Some("target"))).is_ok());
    }

    #[test]
    fn profile_model_spec_prefers_model_file_over_model_id() {
        let mut p = profile("x", true, Some("file.gguf"));
        p.model = Some("ignored".into());
        assert_eq!(profile_model_spec(&p).as_deref(), Some("file.gguf"));
        p.model_file = None;
        assert_eq!(profile_model_spec(&p).as_deref(), Some("ignored"));
    }
}
