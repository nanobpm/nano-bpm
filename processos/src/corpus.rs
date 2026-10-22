//! The **synthetic trace corpus** generator + inference.
//!
//! ProcessOS analyses *traces*. To demo and regression-test that analysis we need
//! believable traces with a *known planted pathology* — a labelled benchmark. This
//! module turns a **scenario pack** (a real "before" BPMN + a thin infra sidecar +
//! a ground-truth label) into a folder of `InstanceTrace` JSON byte-compatible with
//! a real Nano capture (`GET /console/api/traces/{key}`), so it drops straight into
//! a workspace [`crate::dataset::DatasetSource`].
//!
//! ## Why a separate timing overlay
//! The BPMN logic (which branch, which jobs) is run on the **real `engine-core`** so
//! the path is honest. Timing is *overlaid* analytically: per-job **service time**
//! is a lognormal draw, and per-job **queue wait** is sampled from the M/M/c
//! waiting-time law (`P(Wq>t)=C·e^{-(c-a)/s·t}`, the same model `harness::queueing`
//! recommends staffing against) using the **instantaneous offered load** at the
//! instance's arrival time. So a weekday-morning arrival spike against an
//! under-provisioned pool produces a real, time-localised queue *tail* — the signal
//! under test — without needing a contended multi-instance engine run.
//!
//! ## What it emits
//! `<out>/traces/inst-*.json` (+ `metrics.json`, `expected.json`). Point a workspace
//! process at `<out>` (or `<out>/traces`) and the Insights fold renders it.

use std::collections::HashMap;
use std::path::Path;

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    Command, ElementKind, Engine, JobState, ProcessDefinition, ProcessInstanceState, UserTaskState,
    Value,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::harness::{draw, min_workers_for_p99};

/// Epoch base for generated wall-clock timestamps: **Mon 2024-01-01 00:00:00 UTC**
/// (a Monday, so `day % 7 < 5` is a weekday). Milliseconds.
const EPOCH_BASE_MS: u64 = 1_704_067_200_000;
const MS_PER_HOUR: u64 = 3_600_000;
const MS_PER_DAY: u64 = 86_400_000;

// ---------------------------------------------------------------------------
// Scenario pack (the generator input, on disk as `pack.json`).
// ---------------------------------------------------------------------------

/// A scenario pack: a "before" process plus the infra + arrival model that plants a
/// known pathology, plus the ground-truth label the inference is scored against.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pack {
    /// The executable process id inside the BPMN (`bpmn:process id=`).
    pub process_id: String,
    /// Path to the BPMN file, absolute or relative to the pack file's directory.
    pub bpmn: String,
    /// Additional BPMN files or directories holding the processes invoked by
    /// `bpmn`'s call activities (a multi-stage orchestrator references its phase
    /// processes by id). Each entry is a `.bpmn` file or a directory scanned for
    /// `*.bpmn`, resolved relative to the pack file. Every `<process>` found
    /// (plus those in `bpmn` itself) forms the call-activity resolution library;
    /// the selected `process_id` is then expanded inline. Empty for single-file
    /// packs.
    #[serde(default)]
    pub bpmn_library: Vec<String>,
    /// Domain label (ground truth for classification), e.g. `"loan-origination"`.
    pub domain: String,
    /// Master seed — the whole corpus is deterministic in it.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// How many days of traffic to synthesise.
    #[serde(default = "default_horizon")]
    pub horizon_days: u32,
    /// Off-peak instance arrival rate (instances per hour).
    #[serde(default = "default_base_arrivals")]
    pub base_arrivals_per_hour: f64,
    /// The arrival spike that drives the pathology.
    pub peak: Peak,
    /// Per-job-type infra model, keyed by `zeebe:taskDefinition type`.
    pub jobs: HashMap<String, JobInfra>,
    /// The input mix (categorical; weights need not sum to 1).
    pub inputs: Vec<InputCase>,
    /// The planted fault — what a correct inference must recover.
    pub planted_fault: PlantedFault,
}

/// A named, time-windowed arrival multiplier.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Peak {
    /// `"weekday-morning"` | `"daily"` | `"always"` (others fall back to always-off).
    pub pattern: String,
    /// Arrival-rate multiplier inside the window.
    pub multiplier: f64,
    /// Window start hour (inclusive, 0..24, local == UTC here).
    #[serde(default)]
    pub start_hour: u32,
    /// Window end hour (exclusive, 0..24).
    #[serde(default = "default_end_hour")]
    pub end_hour: u32,
}

impl Peak {
    /// The arrival multiplier active at `(day_index, hour)`.
    fn multiplier_at(&self, day_index: u32, hour: u32) -> f64 {
        let in_window = hour >= self.start_hour && hour < self.end_hour;
        let active = match self.pattern.as_str() {
            "weekday-morning" => in_window && (day_index % 7) < 5,
            "daily" => in_window,
            "always" => true,
            _ => false,
        };
        if active {
            self.multiplier
        } else {
            1.0
        }
    }
}

/// The cost/latency/failure/capacity of one job type's worker pool.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobInfra {
    /// Mean service time (ms) — the worker's busy time per job.
    pub service_ms_mean: u64,
    /// Lognormal shape (sigma of the underlying normal). `0.0` => deterministic.
    #[serde(default = "default_sigma")]
    pub service_ms_sigma: f64,
    /// Additive cost charged per job. Part of the on-disk schema; reserved for the
    /// cost-axis fold (not yet surfaced by the inference).
    #[serde(default)]
    #[allow(dead_code)]
    pub cost: f64,
    /// Probability `[0,1]` a job fails (raises an incident, parks the instance).
    #[serde(default)]
    pub failure_rate: f64,
    /// Worker-pool size `c` for the M/M/c queue. The lever the pathology pulls.
    #[serde(default = "default_pool")]
    pub pool_capacity: u32,
}

/// One weighted input case. `vars` seed the instance; `expectedOutcome` documents
/// the intended terminal (not asserted by the generator).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputCase {
    #[serde(default = "default_weight")]
    pub weight: f64,
    #[serde(default)]
    pub vars: HashMap<String, Json>,
    #[serde(default)]
    #[allow(dead_code)] // documents the intended terminal; not asserted by the generator
    pub expected_outcome: Option<String>,
}

/// The ground-truth planted pathology.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlantedFault {
    /// The under-provisioned job type whose queue tail is the signal.
    pub job_type: String,
    /// e.g. `"under-provisioned"`.
    pub kind: String,
    /// The temporal window label, e.g. `"weekday-morning"`.
    pub window: String,
}

fn default_seed() -> u64 {
    42
}
fn default_horizon() -> u32 {
    14
}
fn default_base_arrivals() -> f64 {
    20.0
}
fn default_end_hour() -> u32 {
    12
}
fn default_sigma() -> f64 {
    0.35
}
fn default_pool() -> u32 {
    4
}
fn default_weight() -> f64 {
    1.0
}

// ---------------------------------------------------------------------------
// Emission shapes — byte-compatible with contracts::InstanceTrace (camelCase).
// Defined locally because contracts:: types are Deserialize-only.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TraceOut {
    instance_key: String,
    process_id: String,
    version: i32,
    outcome: String,
    started_at: u64,
    duration_ms: u64,
    elements: Vec<ElementOut>,
    incidents: Vec<IncidentOut>,
    /// Tier-1 capture: the instance creation inputs. Emitted so the corpus is
    /// **recorded-input replayable** (drives the Alternate Reality Engine).
    #[serde(skip_serializing_if = "Option::is_none")]
    creation_variables: Option<VariablesOut>,
    /// Tier-2 capture: the ordered `jobCompleted` stimulus log (one per completed
    /// job, in execution order) that replay feeds back into a candidate model.
    #[serde(skip_serializing_if = "Option::is_none")]
    stimuli: Option<Vec<StimulusOut>>,
    /// The synthetic corpus never truncates the stimulus log.
    stimuli_truncated: bool,
}

/// A captured variable map (mirrors `contracts::Variables`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct VariablesOut {
    truncated: bool,
    bytes: usize,
    values: Json,
}

/// One recorded external input (mirrors `contracts::Stimulus`). The corpus only
/// emits `jobCompleted` stimuli, keyed by job *type* (replay matches by type).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StimulusOut {
    seq: u32,
    at: u64,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<VariablesOut>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ElementOut {
    element_id: String,
    duration_ms: u64,
    incidents: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<JobOut>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobOut {
    #[serde(rename = "type")]
    job_type: String,
    queue_ms: u64,
    service_ms: u64,
    failures: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct IncidentOut {
    element_id: String,
    kind: String,
    reason: String,
}

/// Summary returned by [`generate`].
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateSummary {
    pub instances: usize,
    pub completed: usize,
    pub terminated: usize,
    pub out_dir: String,
    pub traces_dir: String,
}

// ---------------------------------------------------------------------------
// Generation.
// ---------------------------------------------------------------------------

/// Load a pack from `pack.json` (or any JSON file), resolving `bpmn` (and any
/// `bpmn_library` entries) relative to it. Every `<process>` found across the
/// main BPMN and the library forms the call-activity resolution library; the
/// selected `process_id` is returned with its call activities expanded inline.
pub fn load_pack(pack_path: &Path) -> Result<(Pack, ProcessDefinition), String> {
    let bytes =
        std::fs::read(pack_path).map_err(|e| format!("read {}: {e}", pack_path.display()))?;
    let pack: Pack = serde_json::from_slice(&bytes).map_err(|e| format!("parse pack: {e}"))?;

    // A pack with no inputs cannot drive the generator (the weighted input
    // sampler has nothing to pick), so reject it here with a typed error rather
    // than letting it surface as a panic deep inside generation.
    if pack.inputs.is_empty() {
        return Err(format!(
            "parse pack {}: a pack must declare at least one input",
            pack_path.display()
        ));
    }

    let pack_dir = pack_path.parent().unwrap_or_else(|| Path::new("."));
    let resolve = |raw: &str| -> std::path::PathBuf {
        let p = Path::new(raw);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            pack_dir.join(p)
        }
    };

    // Build the resolution library from the main BPMN plus every library entry
    // (a file, or a directory scanned for `*.bpmn`). A single BPMN file may
    // itself declare several `<process>` elements.
    let mut library: HashMap<String, ProcessDefinition> = HashMap::new();
    let add_file = |path: &Path,
                    library: &mut HashMap<String, ProcessDefinition>|
     -> Result<(), String> {
        let xml = std::fs::read_to_string(path)
            .map_err(|e| format!("read bpmn {}: {e}", path.display()))?;
        let defs = parse_bpmn(&xml).map_err(|e| format!("parse bpmn {}: {e}", path.display()))?;
        for d in defs {
            library.insert(d.id.clone(), d);
        }
        Ok(())
    };

    let bpmn_path = resolve(&pack.bpmn);
    add_file(&bpmn_path, &mut library)?;
    for entry in &pack.bpmn_library {
        let path = resolve(entry);
        if path.is_dir() {
            let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(&path)
                .map_err(|e| format!("read dir {}: {e}", path.display()))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().map(|x| x == "bpmn").unwrap_or(false))
                .collect();
            files.sort();
            for f in files {
                add_file(&f, &mut library)?;
            }
        } else {
            add_file(&path, &mut library)?;
        }
    }

    let def = library.get(&pack.process_id).cloned().ok_or_else(|| {
        format!(
            "process id '{}' not found in {} or its bpmn_library",
            pack.process_id,
            bpmn_path.display()
        )
    })?;

    // Expand call activities inline so the generator's real-engine walk runs the
    // multi-stage flow on the existing sub-process machinery.
    let has_calls = def
        .elements
        .values()
        .any(|e| matches!(e.kind, ElementKind::CallActivity { .. }));
    let def = if has_calls {
        def.inline_call_activities(&library)?
    } else {
        def
    };
    Ok((pack, def))
}

/// Generate the corpus into `out_dir`: a single `traces.json` array (the shape a
/// `DatasetSource` reads) + `metrics.json` + `expected.json`. Deterministic in
/// `pack.seed`.
pub fn generate(
    pack: &Pack,
    def: &ProcessDefinition,
    out_dir: &Path,
) -> Result<GenerateSummary, String> {
    std::fs::create_dir_all(out_dir).map_err(|e| format!("mkdir {}: {e}", out_dir.display()))?;

    // 1. Build the arrival timeline: per hour draw a Poisson instance count from the
    //    time-varying rate, then place each instance at a uniform offset.
    let mut arrivals: Vec<u64> = Vec::new();
    for day in 0..pack.horizon_days {
        for hour in 0u32..24 {
            let rate = pack.base_arrivals_per_hour * pack.peak.multiplier_at(day, hour);
            let salt = (day as u64) << 8 | hour as u64;
            let count = poisson(rate, pack.seed ^ 0xA11C, salt);
            let hour_start = EPOCH_BASE_MS + day as u64 * MS_PER_DAY + hour as u64 * MS_PER_HOUR;
            for k in 0..count {
                let off = (draw(pack.seed ^ 0xB22D, salt.wrapping_mul(1009).wrapping_add(k))
                    * MS_PER_HOUR as f64) as u64;
                arrivals.push(hour_start + off);
            }
        }
    }
    arrivals.sort_unstable();

    // 2. Per instance: run the engine for the path, overlay timing + queue waits.
    let total_weight: f64 = pack
        .inputs
        .iter()
        .map(|i| i.weight.max(0.0))
        .sum::<f64>()
        .max(1e-9);
    let mut completed = 0usize;
    let mut terminated = 0usize;
    let mut traces: Vec<TraceOut> = Vec::with_capacity(arrivals.len());

    for (idx, &arrival) in arrivals.iter().enumerate() {
        let input = pick_input(&pack.inputs, total_weight, pack.seed ^ 0xC33E, idx as u64);
        let path = run_path(
            def,
            &pack.process_id,
            &input.vars,
            pack,
            pack.seed,
            idx as u64,
        );

        let day = ((arrival - EPOCH_BASE_MS) / MS_PER_DAY) as u32;
        let hour = (((arrival - EPOCH_BASE_MS) % MS_PER_DAY) / MS_PER_HOUR) as u32;
        let inst_rate_per_sec =
            (pack.base_arrivals_per_hour * pack.peak.multiplier_at(day, hour)) / 3600.0;

        let mut elements: Vec<ElementOut> = Vec::new();
        let mut incidents: Vec<IncidentOut> = Vec::new();
        let mut stimuli: Vec<StimulusOut> = Vec::new();
        let mut cursor = arrival;

        for (step, jstep) in path.steps.iter().enumerate() {
            let infra = pack.jobs.get(&jstep.job_type);
            let mean = infra.map(|i| i.service_ms_mean).unwrap_or(100);
            let sigma = infra.map(|i| i.service_ms_sigma).unwrap_or(0.0);
            let cap = infra.map(|i| i.pool_capacity).unwrap_or(default_pool());

            let svc_seed = mix(pack.seed ^ 0xD44F, (idx as u64) << 8 | step as u64);
            let service_ms = lognormal_ms(mean, sigma, svc_seed);
            let service_s = (service_ms as f64 / 1000.0).max(1e-6);

            // Offered load for THIS job type in THIS hour: every reached instance hits
            // each job on its path once, so the job's arrival rate is the instance rate.
            let queue_ms =
                sample_queue_ms(cap, inst_rate_per_sec, service_s, mix(svc_seed, 0x5151));

            let failures = if jstep.failed { 1 } else { 0 };
            if jstep.failed {
                incidents.push(IncidentOut {
                    element_id: jstep.element_id.clone(),
                    kind: "job-failure".into(),
                    reason: format!("worker for '{}' exhausted retries", jstep.job_type),
                });
            }
            elements.push(ElementOut {
                element_id: jstep.element_id.clone(),
                duration_ms: queue_ms + service_ms,
                incidents: failures,
                job: Some(JobOut {
                    job_type: jstep.job_type.clone(),
                    queue_ms,
                    service_ms,
                    failures,
                }),
            });
            cursor += queue_ms + service_ms;
            // A completed job is one recorded-input stimulus (jobCompleted), keyed by
            // type and timestamped at its completion. A failed job that exhausts
            // retries terminates the instance and produces no completion input.
            if !jstep.failed {
                stimuli.push(StimulusOut {
                    seq: (stimuli.len() + 1) as u32,
                    at: cursor,
                    kind: "jobCompleted".into(),
                    reference: Some(jstep.job_type.clone()),
                    variables: None,
                });
            }
        }

        let outcome = if path.completed {
            "completed"
        } else {
            "terminated"
        };
        if path.completed {
            completed += 1;
        } else {
            terminated += 1;
        }

        let creation_values = serde_json::to_value(&input.vars).unwrap_or(Json::Null);
        let creation_bytes = serde_json::to_vec(&input.vars)
            .map(|v| v.len())
            .unwrap_or(0);

        traces.push(TraceOut {
            instance_key: format!("{}", 100_000 + idx),
            process_id: pack.process_id.clone(),
            version: 1,
            outcome: outcome.into(),
            started_at: arrival,
            duration_ms: cursor - arrival,
            elements,
            incidents,
            creation_variables: Some(VariablesOut {
                truncated: false,
                bytes: creation_bytes,
                values: creation_values,
            }),
            stimuli: Some(stimuli),
            stimuli_truncated: false,
        });
    }

    let traces_file = out_dir.join("traces.json");
    let bytes = serde_json::to_vec(&traces).map_err(|e| format!("serialize traces: {e}"))?;
    std::fs::write(&traces_file, bytes)
        .map_err(|e| format!("write {}: {e}", traces_file.display()))?;

    // 3. Sidecars: the ground-truth label + a tiny metrics gauge.
    let expected = serde_json::json!({
        "domain": pack.domain,
        "plantedFault": pack.planted_fault,
        "instances": arrivals.len(),
    });
    std::fs::write(
        out_dir.join("expected.json"),
        serde_json::to_vec_pretty(&expected).map_err(|e| format!("serialize expected: {e}"))?,
    )
    .map_err(|e| format!("write expected.json: {e}"))?;

    let metrics = serde_json::json!({
        "processInstancesActive": 0,
        "processInstancesCompleted": completed,
    });
    std::fs::write(
        out_dir.join("metrics.json"),
        serde_json::to_vec_pretty(&metrics).map_err(|e| format!("serialize metrics: {e}"))?,
    )
    .map_err(|e| format!("write metrics.json: {e}"))?;

    Ok(GenerateSummary {
        instances: arrivals.len(),
        completed,
        terminated,
        out_dir: out_dir.to_string_lossy().into_owned(),
        traces_dir: traces_file.to_string_lossy().into_owned(),
    })
}

/// One executed service-task step.
struct JobStep {
    element_id: String,
    job_type: String,
    failed: bool,
}

struct InstancePath {
    steps: Vec<JobStep>,
    completed: bool,
}

/// Drive `engine-core` over one instance to discover the taken path + which jobs ran
/// (and which failed). Timing is *not* taken from here — only the logical path is.
fn run_path(
    def: &ProcessDefinition,
    process_id: &str,
    input_vars: &HashMap<String, Json>,
    pack: &Pack,
    seed: u64,
    inst_index: u64,
) -> InstancePath {
    let mut engine = Engine::new();
    let clock = 0u64;
    let _ = engine.apply_command_at(Command::DeployResources(vec![def.clone()]), clock);
    let vars: HashMap<String, Value> = input_vars
        .iter()
        .map(|(k, v)| (k.clone(), json_to_value(v)))
        .collect();
    let _ = engine.apply_command_at(
        Command::CreateInstance {
            process_id: process_id.to_string(),
            variables: vars,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: None,
            version: None,
        },
        clock,
    );

    let mut steps: Vec<JobStep> = Vec::new();
    let max_steps = 10_000usize;
    let mut guard = 0usize;
    loop {
        guard += 1;
        if guard > max_steps {
            break;
        }
        let pending: Vec<(u64, String, String, bool)> = engine
            .state()
            .jobs
            .values()
            .filter(|j| matches!(j.state, JobState::Created | JobState::Activated))
            .map(|j| {
                (
                    j.key,
                    j.job_type.clone(),
                    j.element_id.clone(),
                    j.state == JobState::Created,
                )
            })
            .collect();
        // Human steps: a userTask on the taken path parks the token until a human
        // completes it. The corpus auto-completes them (the "user" always acts) so
        // multi-stage flows reach their end, but does NOT model them as worker-pool
        // work — they carry no queue/service infra and so are not recorded as job
        // steps (only service-task jobs are the levers the inference pulls).
        let pending_user_tasks: Vec<u64> = engine
            .state()
            .user_tasks
            .values()
            .filter(|t| t.state == UserTaskState::Created)
            .map(|t| t.key)
            .collect();
        if pending.is_empty() && pending_user_tasks.is_empty() {
            break;
        }
        for ut_key in pending_user_tasks {
            let _ = engine.apply_command_at(Command::complete_user_task(ut_key), clock);
        }
        for (job_key, job_type, element_id, needs_activation) in pending {
            if needs_activation {
                let _ = engine.apply_command_at(
                    Command::activate_jobs(
                        job_type.clone(),
                        "corpus".to_string(),
                        100_000,
                        u64::MAX / 4,
                        clock,
                    ),
                    clock,
                );
            }
            let fr = pack
                .jobs
                .get(&job_type)
                .map(|i| i.failure_rate)
                .unwrap_or(0.0);
            let failed = draw(seed ^ 0xE55A, mix(inst_index, job_key)) < fr;
            steps.push(JobStep {
                element_id: element_id.clone(),
                job_type: job_type.clone(),
                failed,
            });
            if failed {
                let _ = engine.apply_command_at(
                    Command::fail_job(job_key, 0, format!("mock worker '{}' failed", job_type)),
                    clock,
                );
            } else {
                let _ = engine
                    .apply_command_at(Command::complete_job_with(job_key, HashMap::new()), clock);
            }
        }
    }

    let completed = engine
        .state()
        .instances
        .values()
        .next()
        .map(|i| i.state == ProcessInstanceState::Completed)
        .unwrap_or(false);
    InstancePath { steps, completed }
}

// ---------------------------------------------------------------------------
// Sampling helpers (all deterministic in the master seed).
// ---------------------------------------------------------------------------

fn mix(seed: u64, salt: u64) -> u64 {
    let mut z = seed.wrapping_add(salt).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A standard normal via Box-Muller from two uniforms.
fn std_normal(seed: u64) -> f64 {
    let u1 = draw(seed, 1).max(1e-12);
    let u2 = draw(seed, 2);
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// Lognormal service time in ms with the given mean and shape sigma. With `sigma=0`
/// it is exactly `mean`. The lognormal is parameterised so its *median* is `mean`.
fn lognormal_ms(mean: u64, sigma: f64, seed: u64) -> u64 {
    if sigma <= 0.0 {
        return mean.max(1);
    }
    let z = std_normal(seed);
    let v = (mean as f64) * (sigma * z).exp();
    v.round().max(1.0) as u64
}

/// Small-count Poisson draw via Knuth's algorithm, deterministic in `(seed, salt)`.
fn poisson(rate: f64, seed: u64, salt: u64) -> u64 {
    if rate <= 0.0 {
        return 0;
    }
    let l = (-rate).exp();
    let mut k = 0u64;
    let mut p = 1.0f64;
    loop {
        let u = draw(seed, mix(salt, k));
        p *= u;
        if p <= l {
            return k;
        }
        k += 1;
        if k > 100_000 {
            return k; // guard
        }
    }
}

/// Sample a queue wait (ms) from the M/M/c FCFS waiting-time law: with probability
/// `C` (Erlang-C) the job waits, then the wait is `Exp((c-a)/s)`; otherwise 0. When
/// the pool is unstable (`a >= c`) the queue is saturated — return a degraded wait
/// that grows with the overload so the tail is visibly pathological.
fn sample_queue_ms(c: u32, lambda_per_sec: f64, service_s: f64, seed: u64) -> u64 {
    let a = lambda_per_sec * service_s; // offered load (Erlangs)
    if c == 0 {
        return 0;
    }
    if a >= c as f64 {
        // Unstable: model a backlog that worsens with the overload ratio, with jitter.
        let overload = a / c as f64; // >= 1
        let base = service_s * 1000.0 * (1.0 + (overload - 1.0) * 5.0);
        let jitter = 0.5 + draw(seed, 7);
        return (base * jitter).round() as u64;
    }
    let cc = crate::harness::erlang_c(c, a);
    if draw(seed, 3) >= cc {
        return 0; // did not have to wait
    }
    let decay = (c as f64 - a) / service_s; // per second
    let u = draw(seed, 4).max(1e-12);
    let wait_s = -(u.ln()) / decay; // Exp(decay)
    (wait_s * 1000.0).round() as u64
}

fn pick_input(inputs: &[InputCase], total_weight: f64, seed: u64, idx: u64) -> &InputCase {
    let mut r = draw(seed, idx) * total_weight;
    for c in inputs {
        r -= c.weight.max(0.0);
        if r <= 0.0 {
            return c;
        }
    }
    // The weighted walk above returns on the first input whose cumulative weight
    // covers `r`; this final line only runs when floating-point residue leaves
    // `r` fractionally positive past the last input. `inputs` is non-empty by
    // construction — `load_pack` rejects an empty-input pack with a typed error,
    // and every in-repo `Pack` literal declares at least one input.
    #[allow(
        clippy::expect_used,
        reason = "inputs is non-empty by construction: load_pack rejects an empty-input pack with a typed error before any pick_input call"
    )]
    inputs.last().expect("pack has at least one input")
}

fn json_to_value(v: &Json) -> Value {
    match v {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::Int(u as i64)
            } else {
                Value::Double(n.as_f64().unwrap_or(0.0))
            }
        }
        Json::String(s) => Value::Str(s.clone()),
        Json::Array(items) => Value::List(items.iter().map(json_to_value).collect()),
        Json::Object(map) => Value::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Inference — the consumer under test. Reads a generated (or real) dataset and
// (a) localises the worst queue tail in time, (b) recommends staffing, and
// (c) classifies the domain. Scored against `expected.json`.
// ---------------------------------------------------------------------------

/// A single read trace, the minimal shape the inference needs.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadTrace {
    #[serde(default)]
    started_at: u64,
    #[serde(default)]
    elements: Vec<ReadElement>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadElement {
    #[serde(default)]
    job: Option<ReadJob>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadJob {
    #[serde(rename = "type")]
    job_type: String,
    #[serde(default)]
    queue_ms: u64,
    #[serde(default)]
    service_ms: u64,
}

/// The inference verdict.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Inference {
    pub domain: String,
    pub bottleneck_job: Option<String>,
    pub window: Option<String>,
    /// Peak-window mean queue wait (ms) for the bottleneck job.
    pub peak_queue_ms: u64,
    /// Off-peak mean queue wait (ms) for the bottleneck job.
    pub offpeak_queue_ms: u64,
    /// Recommended worker count to hold a p99 wait target during the peak.
    pub recommended_workers: Option<u32>,
    pub evidence: Vec<String>,
}

/// Bucket a trace's hour into a coarse temporal class.
fn window_of(started_at: u64) -> &'static str {
    if started_at < EPOCH_BASE_MS {
        return "other";
    }
    let day = ((started_at - EPOCH_BASE_MS) / MS_PER_DAY) as u32;
    let hour = (((started_at - EPOCH_BASE_MS) % MS_PER_DAY) / MS_PER_HOUR) as u32;
    let weekday = (day % 7) < 5;
    match (weekday, hour) {
        (true, 6..=11) => "weekday-morning",
        (true, 12..=17) => "weekday-afternoon",
        (true, _) => "weekday-offhours",
        (false, _) => "weekend",
    }
}

/// Per (job, window) queue-wait accumulator.
#[derive(Default, Clone)]
struct Acc {
    queue_sum: u64,
    service_sum: u64,
    n: u64,
}

/// Run the inference over a dataset directory (the folder containing `traces.json`
/// or `traces/`).
pub fn infer(dataset_dir: &Path, target_p99_wait_ms: u64) -> Result<Inference, String> {
    let mut per: HashMap<(String, String), Acc> = HashMap::new();
    let mut job_types: Vec<String> = Vec::new();

    let mut ingest = |trace: &ReadTrace| {
        let win = window_of(trace.started_at).to_string();
        for el in &trace.elements {
            if let Some(job) = &el.job {
                if !job_types.iter().any(|j| j == &job.job_type) {
                    job_types.push(job.job_type.clone());
                }
                let acc = per.entry((job.job_type.clone(), win.clone())).or_default();
                acc.queue_sum += job.queue_ms;
                acc.service_sum += job.service_ms;
                acc.n += 1;
            }
        }
    };

    // Prefer a single `traces.json` array; else scan per-instance files under the dir
    // and a `traces/` subdir.
    let bundle = dataset_dir.join("traces.json");
    if bundle.is_file() {
        let bytes =
            std::fs::read(&bundle).map_err(|e| format!("read {}: {e}", bundle.display()))?;
        let arr: Vec<ReadTrace> =
            serde_json::from_slice(&bytes).map_err(|e| format!("parse traces.json: {e}"))?;
        for t in &arr {
            ingest(t);
        }
    } else {
        for dir in [dataset_dir.to_path_buf(), dataset_dir.join("traces")] {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name == "metrics.json" || name == "expected.json" {
                    continue;
                }
                if let Ok(bytes) = std::fs::read(&path) {
                    if let Ok(trace) = serde_json::from_slice::<ReadTrace>(&bytes) {
                        ingest(&trace);
                    }
                }
            }
        }
    }

    if job_types.is_empty() {
        return Err("no jobs found in dataset".into());
    }

    // For each job: peak (highest-mean) window vs the rest. The bottleneck is the job
    // with the biggest peak/offpeak queue-wait inflation.
    let mut best: Option<(String, String, u64, u64, u64)> = None; // job, window, peak, offpeak, peak_service
    for jt in &job_types {
        let mut windows: Vec<(String, u64, u64, u64)> = Vec::new(); // window, mean_q, mean_s, n
        for ((j, w), acc) in &per {
            if j == jt && acc.n > 0 {
                windows.push((
                    w.clone(),
                    acc.queue_sum / acc.n,
                    acc.service_sum / acc.n,
                    acc.n,
                ));
            }
        }
        if windows.is_empty() {
            continue;
        }
        windows.sort_by_key(|x| std::cmp::Reverse(x.1));
        let (peak_w, peak_q, peak_s, _) = windows[0].clone();
        // Off-peak = weighted mean of the remaining windows.
        let (mut osum, mut on) = (0u64, 0u64);
        for (w, q, _, n) in &windows {
            if w != &peak_w {
                osum += q * n;
                on += n;
            }
        }
        let offpeak_q = osum.checked_div(on).unwrap_or(0);
        let inflation = peak_q.saturating_sub(offpeak_q);
        let better = match &best {
            Some((_, _, bp, bo, _)) => inflation > bp.saturating_sub(*bo),
            None => true,
        };
        if better {
            best = Some((jt.clone(), peak_w, peak_q, offpeak_q, peak_s));
        }
    }

    let domain = classify_domain(&job_types);
    let mut evidence = Vec::new();
    let (bottleneck_job, window, peak_q, offpeak_q, recommended_workers) = match best {
        Some((jt, win, pq, oq, ps)) => {
            evidence.push(format!(
                "job '{jt}' queue wait {pq}ms in {win} vs {oq}ms off-peak ({}x)",
                if oq > 0 { pq / oq.max(1) } else { pq }
            ));
            // Fit peak arrival rate from the offered load implied by the queue inflation
            // is hard; instead recommend from the measured peak service time + the peak
            // throughput. We approximate arrival rate as (#peak jobs / peak seconds).
            let plan = if ps > 0 {
                // Estimate peak arrival rate from the worst window's sample density.
                let n_peak = per
                    .iter()
                    .filter(|((j, w), _)| j == &jt && w == &win)
                    .map(|(_, a)| a.n)
                    .sum::<u64>();
                // Peak windows here span ~6 weekday hours/day across the horizon; use a
                // conservative single-peak-hour rate = n_peak / (peak_hours_seconds).
                let peak_seconds = 6.0 * 3600.0; // per the window definition span
                let lambda = (n_peak as f64 / peak_seconds).max(1e-6);
                let plan = min_workers_for_p99(lambda, ps, target_p99_wait_ms);
                evidence.push(format!(
                    "fitted peak λ≈{:.3}/s, service≈{ps}ms → {} workers hold p99 wait ≤ {}ms",
                    lambda, plan.workers, target_p99_wait_ms
                ));
                Some(plan.workers)
            } else {
                None
            };
            (Some(jt), Some(win), pq, oq, plan)
        }
        None => (None, None, 0, 0, None),
    };
    evidence.push(format!(
        "domain classified as '{domain}' from job types {job_types:?}"
    ));

    Ok(Inference {
        domain,
        bottleneck_job,
        window,
        peak_queue_ms: peak_q,
        offpeak_queue_ms: offpeak_q,
        recommended_workers,
        evidence,
    })
}

/// A small keyword heuristic mapping the observed job-type vocabulary to a domain.
fn classify_domain(job_types: &[String]) -> String {
    let hay = job_types.join(" ").to_lowercase();
    let has = |kw: &str| hay.contains(kw);
    if has("credit") || has("loan") || has("approve") {
        "loan-origination".into()
    } else if has("cdd")
        || has("sanction")
        || has("screen")
        || has("kyc")
        || has("edd")
        || has("ubo")
        || has("mlro")
        || has("jurisdiction")
        || has("adverse-media")
    {
        // Corporate CDD / AML refresh: the screening/sanctions/EDD vocabulary is
        // checked before the generic "case" heuristic below because tasks like
        // `close-cdd-case` would otherwise be misread as case management.
        "kyc-cdd-refresh".into()
    } else if has("salesforce") || has("case") || has("tracking") || has("delivery") {
        "delivery-exception-resolution".into()
    } else if has("document") {
        "kyc-cdd-refresh".into()
    } else if has("classify") || has("summarize") {
        "document-processing".into()
    } else {
        "unknown".into()
    }
}

/// How well an inference recovered the planted ground truth.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Score {
    pub domain_ok: bool,
    pub job_ok: bool,
    pub window_ok: bool,
    pub points: u32,
    pub max_points: u32,
}

/// Score an inference against an `expected.json` (the pack's planted label).
pub fn score(inference: &Inference, expected_path: &Path) -> Result<Score, String> {
    let bytes = std::fs::read(expected_path)
        .map_err(|e| format!("read {}: {e}", expected_path.display()))?;
    let exp: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse expected: {e}"))?;
    let exp_domain = exp.get("domain").and_then(|v| v.as_str()).unwrap_or("");
    let fault = exp.get("plantedFault");
    let exp_job = fault
        .and_then(|f| f.get("jobType"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let exp_window = fault
        .and_then(|f| f.get("window"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let domain_ok = inference.domain == exp_domain;
    let job_ok = inference.bottleneck_job.as_deref() == Some(exp_job);
    let window_ok = inference.window.as_deref() == Some(exp_window);
    let points = domain_ok as u32 + job_ok as u32 + window_ok as u32;
    Ok(Score {
        domain_ok,
        job_ok,
        window_ok,
        points,
        max_points: 3,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const LOAN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  id="Definitions_loan" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="loan-approval" name="Loan" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f0" sourceRef="Start" targetRef="Task_CreditCheck"/>
    <bpmn:serviceTask id="Task_CreditCheck" name="Credit Check">
      <bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements>
      <bpmn:incoming>f0</bpmn:incoming><bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f1" sourceRef="Task_CreditCheck" targetRef="Gateway"/>
    <bpmn:exclusiveGateway id="Gateway"><bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing><bpmn:outgoing>f3</bpmn:outgoing></bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="f2" sourceRef="Gateway" targetRef="Task_Approve">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">=creditScore &gt;= 700</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f3" sourceRef="Gateway" targetRef="Task_Reject">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">=creditScore &lt; 700</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:serviceTask id="Task_Approve" name="Approve">
      <bpmn:extensionElements><zeebe:taskDefinition type="approve-loan"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Task_Reject" name="Reject">
      <bpmn:extensionElements><zeebe:taskDefinition type="reject-application"/></bpmn:extensionElements>
      <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f5</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f4" sourceRef="Task_Approve" targetRef="End"/>
    <bpmn:sequenceFlow id="f5" sourceRef="Task_Reject" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f4</bpmn:incoming><bpmn:incoming>f5</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

    pub(crate) fn loan_def() -> ProcessDefinition {
        parse_bpmn(LOAN_BPMN)
            .expect("loan bpmn parses")
            .into_iter()
            .find(|d| d.id == "loan-approval")
            .expect("loan process present")
    }

    pub(crate) fn loan_pack() -> Pack {
        Pack {
            process_id: "loan-approval".into(),
            bpmn: "loan.bpmn".into(),
            bpmn_library: Vec::new(),
            domain: "loan-origination".into(),
            seed: 7,
            horizon_days: 2,
            // ~15 instances/hour off-peak; an 8x weekday-morning spike. With a 2-minute
            // credit check on a 2-worker bureau pool, off-peak load is light (ρ≈0.25)
            // but the morning spike drives the offered load past the pool → a real,
            // time-localised queue tail on credit-check.
            base_arrivals_per_hour: 15.0,
            peak: Peak {
                pattern: "weekday-morning".into(),
                multiplier: 8.0,
                start_hour: 9,
                end_hour: 12,
            },
            jobs: HashMap::from([
                (
                    "credit-check".to_string(),
                    JobInfra {
                        service_ms_mean: 120_000,
                        service_ms_sigma: 0.3,
                        cost: 0.0,
                        failure_rate: 0.02,
                        pool_capacity: 2,
                    },
                ),
                (
                    "approve-loan".to_string(),
                    JobInfra {
                        service_ms_mean: 30_000,
                        service_ms_sigma: 0.2,
                        cost: 0.0,
                        failure_rate: 0.0,
                        pool_capacity: 30,
                    },
                ),
                (
                    "reject-application".to_string(),
                    JobInfra {
                        service_ms_mean: 20_000,
                        service_ms_sigma: 0.2,
                        cost: 0.0,
                        failure_rate: 0.0,
                        pool_capacity: 30,
                    },
                ),
            ]),
            inputs: vec![
                InputCase {
                    weight: 7.0,
                    vars: HashMap::from([("creditScore".to_string(), serde_json::json!(750))]),
                    expected_outcome: Some("approved".into()),
                },
                InputCase {
                    weight: 3.0,
                    vars: HashMap::from([("creditScore".to_string(), serde_json::json!(650))]),
                    expected_outcome: Some("rejected".into()),
                },
            ],
            planted_fault: PlantedFault {
                job_type: "credit-check".into(),
                kind: "under-provisioned".into(),
                window: "weekday-morning".into(),
            },
        }
    }

    #[test]
    fn run_path_takes_the_approve_branch_for_high_score() {
        let def = loan_def();
        let pack = loan_pack();
        let vars = HashMap::from([("creditScore".to_string(), serde_json::json!(750))]);
        let path = run_path(&def, "loan-approval", &vars, &pack, 1, 0);
        assert!(path.completed);
        let jobs: Vec<&str> = path.steps.iter().map(|s| s.job_type.as_str()).collect();
        assert_eq!(jobs, vec!["credit-check", "approve-loan"]);
    }

    #[test]
    fn run_path_takes_the_reject_branch_for_low_score() {
        let def = loan_def();
        let pack = loan_pack();
        let vars = HashMap::from([("creditScore".to_string(), serde_json::json!(650))]);
        let path = run_path(&def, "loan-approval", &vars, &pack, 1, 0);
        assert!(path.completed);
        let jobs: Vec<&str> = path.steps.iter().map(|s| s.job_type.as_str()).collect();
        assert_eq!(jobs, vec!["credit-check", "reject-application"]);
    }

    #[test]
    fn load_pack_rejects_a_pack_with_no_inputs() {
        // A malformed external pack that declares an empty `inputs` array must
        // yield a typed parse error, never a panic deep inside generation (the
        // weighted input sampler would otherwise have nothing to pick).
        let tmp = std::env::temp_dir().join(format!("corpus-empty-inputs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).expect("create tmp dir");
        std::fs::write(tmp.join("loan.bpmn"), LOAN_BPMN).expect("write bpmn");
        let pack_json = r#"{
            "processId": "loan-approval",
            "bpmn": "loan.bpmn",
            "domain": "loan-origination",
            "peak": { "pattern": "weekday-morning", "multiplier": 8.0, "startHour": 9, "endHour": 12 },
            "jobs": {},
            "inputs": [],
            "plantedFault": { "jobType": "credit-check", "kind": "under-provisioned", "window": "weekday-morning" }
        }"#;
        let pack_path = tmp.join("pack.json");
        std::fs::write(&pack_path, pack_json).expect("write pack");

        let err = load_pack(&pack_path).expect_err("empty-inputs pack must be rejected");
        assert!(
            err.contains("at least one input"),
            "expected a typed empty-inputs error, got: {err}"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn unstable_pool_produces_a_large_queue_wait() {
        // c=1, λ=10/s, service=0.8s → a=8 >> 1: saturated, should be large.
        let q = sample_queue_ms(1, 10.0, 0.8, 123);
        assert!(
            q > 800,
            "saturated queue should dwarf service time, got {q}"
        );
        // A well-provisioned pool barely waits.
        let q2 = sample_queue_ms(20, 1.0, 0.8, 123);
        assert!(q2 < 800, "idle pool should rarely wait, got {q2}");
    }

    #[test]
    fn generate_then_infer_recovers_the_planted_fault() {
        let def = loan_def();
        let pack = loan_pack();
        let tmp = std::env::temp_dir().join(format!("corpus-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let summary = generate(&pack, &def, &tmp).expect("generate");
        assert!(summary.instances > 100, "expected a populated corpus");

        let inference = infer(&tmp, 500).expect("infer");
        assert_eq!(inference.bottleneck_job.as_deref(), Some("credit-check"));
        assert_eq!(inference.window.as_deref(), Some("weekday-morning"));
        assert!(
            inference.peak_queue_ms > inference.offpeak_queue_ms,
            "peak queue {} should exceed off-peak {}",
            inference.peak_queue_ms,
            inference.offpeak_queue_ms
        );

        let score = score(&inference, &tmp.join("expected.json")).expect("score");
        assert_eq!(
            score.points, 3,
            "should recover domain+job+window: {score:?}"
        );

        // The generated corpus must load through the workspace DatasetSource and fold
        // an Insights report — i.e. it is byte-compatible with a real Nano capture.
        let src = crate::dataset::TraceSource::Dataset(std::sync::Arc::new(
            crate::dataset::DatasetSource::open(&tmp).expect("open dataset"),
        ));
        let insights = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(crate::report::build_over(&src, 5000, 1000))
            .expect("build insights");
        assert!(
            insights
                .processes
                .iter()
                .any(|p| p.process_id == "loan-approval"),
            "Insights should surface the loan-approval process"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn classify_domain_recognises_the_cdd_refresh_vocabulary() {
        // The CDD/AML refresh job vocabulary classifies as kyc-cdd-refresh even
        // though `close-cdd-case` contains "case" (which the generic
        // case-management heuristic would otherwise claim).
        let cdd = vec![
            "io.camunda.agenticai:aiagent:1".to_string(),
            "decision-aggregate-screening-result".to_string(),
            "set-edd-required".to_string(),
            "close-cdd-case".to_string(),
        ];
        assert_eq!(classify_domain(&cdd), "kyc-cdd-refresh");

        // Sanctions/UBO signals also land in the CDD domain.
        let sanctions = vec![
            "lift-client-comms-freeze".to_string(),
            "decision-apply-ubo-threshold".to_string(),
        ];
        assert_eq!(classify_domain(&sanctions), "kyc-cdd-refresh");

        // The loan vocabulary is unaffected by the reordering.
        let loan = vec!["credit-check".to_string(), "approve-loan".to_string()];
        assert_eq!(classify_domain(&loan), "loan-origination");
    }
}
