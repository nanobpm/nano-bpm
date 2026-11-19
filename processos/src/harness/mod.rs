//! The MVP optimization harness (ProcessOS design §7, build stages **M0+M1**).
//!
//! This is the *same loop as production* with the data source swapped: a
//! [`Scenario`] supplies a test model, a pool of seeded **mock workers** (each
//! with a cost / latency / failure model), a set of **latent worker-swap
//! options** (the MVP transform space), and **inputs carrying expected outputs**.
//! The [`sim::SimRunner`] embeds the *real* `engine-core` driven by a virtual
//! clock — so the numbers are trustworthy, exactly as Nano runs the process —
//! and collects a base dataset of `(output, latency, cost, incidents)`.
//!
//! M1 ([`rank`]) enumerates the worker-swap candidate space, evaluates every
//! candidate over the same inputs and seeds, and ranks them across
//! latency / cost / incident-rate / correctness, including the optional
//! **golden** variant as a candidate and reporting whether exploration
//! recovered it. No LLM yet — this validates the measurement + ranking rig.
//!
//! The harness is deterministic: the same scenario + seed always yields the same
//! ranking. `engine-core` is consumed read-only over a `path` dependency and is
//! never modified.

mod calibrate;
mod cluster;
mod evolve;
mod example;
mod hypothesize;
pub(crate) mod llm;
mod production;
mod prompts;
pub(crate) mod queueing;
mod rank;
mod replay;
mod replay_rank;
mod sim;

use std::collections::BTreeMap;

pub use calibrate::{apply as apply_calibration, calibrate_from_measured};
#[allow(unused_imports)]
pub use calibrate::{calibrate_from_cluster, Calibration, MeasuredJobType};
pub use cluster::build_cluster_summary;
#[allow(unused_imports)]
pub use cluster::ClusterRunSummary;
#[allow(unused_imports)]
pub use evolve::{
    build_evolve_prompt, parse_structural_candidates, summarize_dataset, DatasetSignal,
    DEFAULT_EVOLVE_SYSTEM_PROMPT,
};
pub use example::example_scenario;
pub use hypothesize::run_hypothesis;
pub use hypothesize::DEFAULT_SYSTEM_PROMPT;
pub use llm::{
    complete as llm_complete, complete_streaming as llm_complete_streaming, list_models, LlmConfig,
    LlmOverride, Provider, StreamedCompletion, ThinkingLevel,
};
pub use production::build_baseline;
#[allow(unused_imports)]
pub use production::ProductionBaseline;
pub use prompts::{Prompt, PromptLibrary, DEFAULT_ID as DEFAULT_PROMPT_ID};
pub use queueing::staff_for_summary;
#[allow(unused_imports)]
pub use queueing::WorkerStaffing;
#[allow(unused_imports)]
pub(crate) use queueing::{erlang_c, min_workers_for_p99, StaffingPlan};
#[allow(unused_imports)]
pub use rank::{run_scenario, HarnessReport, VariantResult};
#[allow(unused_imports)]
pub use replay::{
    parse_mock_workers, replay_dataset, replay_dataset_with_mocks, replay_instance,
    replay_instance_with_mocks, JobCoverage, KeyCount, MockOutcome, MockWorker, MockWorkers,
    RecordedInstance, RecordedStimulus, ReplayReport, ReplayResult, ReplayUnavailable,
    VarDivergence,
};
#[allow(unused_imports)]
pub use replay_rank::{
    rank_candidates_by_replay, CandidateModel, FidelityTier, RankedCandidate, ReplayRanking,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

/// A seeded mock worker: a deterministic `(vars, seed) -> (vars', cost, latency,
/// outcome)` model standing in for a real job worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerModel {
    /// Stable identifier, referenced by [`Scenario::task_workers`] /
    /// [`Scenario::latent_options`] / [`Variant::assignment`].
    pub id: String,
    /// Monetary (or otherwise additive) cost charged each time this worker runs a job.
    #[serde(default)]
    pub cost: f64,
    /// Service time the worker takes, advancing the virtual clock.
    #[serde(default)]
    pub latency_ms: u64,
    /// Probability in `[0,1]` that a given job ends in failure (raising an
    /// incident). Drawn deterministically from the scenario seed + job identity,
    /// so the same worker fails on the same logical jobs across every candidate.
    #[serde(default)]
    pub failure_rate: f64,
    /// Variables this worker merges into the instance on successful completion
    /// (its "answer"). Cheaper/weaker workers can encode a different answer to
    /// model lower correctness.
    #[serde(default)]
    pub output: BTreeMap<String, Json>,
}

/// One scenario input: the variables a process instance starts with, plus the
/// expected output variables used to score correctness.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioInput {
    #[serde(default)]
    pub vars: BTreeMap<String, Json>,
    /// Expected output variables. A run is *correct* when every expected
    /// key/value is present and equal in the final instance variables.
    #[serde(default)]
    pub expected: BTreeMap<String, Json>,
}

/// A candidate process configuration. In the MVP transform space the BPMN is the
/// test model and the variation is which worker runs each task: `assignment`
/// overrides the default [`Scenario::task_workers`] for some job types.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Variant {
    pub name: String,
    /// `job_type -> worker_id` overrides over the default task assignment.
    #[serde(default)]
    pub assignment: BTreeMap<String, String>,
}

/// How candidates are ranked. Feasible candidates (meeting the correctness and
/// incident gates) are ordered ahead of infeasible ones; within feasible ones the
/// `minimize` axis decides, then latency, then cost.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Objective {
    /// `"cost"` | `"latency"` | `"incidents"` — the axis minimized among feasible
    /// candidates.
    #[serde(default = "default_minimize")]
    pub minimize: String,
    /// Feasibility gate: a candidate's correctness rate must be `>=` this.
    #[serde(default = "default_min_correctness")]
    pub min_correctness: f64,
    /// Feasibility gate: a candidate's incident rate must be `<=` this.
    #[serde(default)]
    pub max_incident_rate: f64,
}

fn default_minimize() -> String {
    "cost".to_string()
}
fn default_min_correctness() -> f64 {
    1.0
}

impl Default for Objective {
    fn default() -> Self {
        Self {
            minimize: default_minimize(),
            min_correctness: default_min_correctness(),
            max_incident_rate: 0.0,
        }
    }
}

/// A full optimization scenario: the test model, the worker pool + default
/// assignment, the latent swap options that define the candidate space, the
/// inputs to evaluate over, and an optional golden variant to aim for.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scenario {
    pub name: String,
    /// BPMN XML of the test model (the same format real models deploy with).
    pub test_model: String,
    /// Process id to start. Inferred from the first parsed definition when absent.
    #[serde(default)]
    pub process_id: Option<String>,
    /// Worker pool, keyed by worker id.
    pub workers: BTreeMap<String, WorkerModel>,
    /// Default assignment: `job_type -> worker_id` for the test model.
    pub task_workers: BTreeMap<String, String>,
    /// Latent swappable options per job type — the MVP transform space.
    #[serde(default)]
    pub latent_options: BTreeMap<String, Vec<String>>,
    /// Inputs with expected outputs (for correctness scoring).
    pub inputs: Vec<ScenarioInput>,
    /// Optional golden variant we hope exploration recovers or beats.
    #[serde(default)]
    pub golden: Option<Variant>,
    /// Ranking objective.
    #[serde(default)]
    pub objective: Objective,
    /// Master seed for deterministic mock-worker failure draws.
    #[serde(default)]
    pub seed: u64,
}

/// A deterministic uniform draw in `[0,1)` from a seed and a salt (splitmix64).
/// Used for mock-worker failure decisions so a candidate's outcome depends only
/// on the scenario seed + job identity, never on wall-clock or iteration order.
pub(crate) fn draw(seed: u64, salt: u64) -> f64 {
    let mut z = seed.wrapping_add(salt).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // Top 53 bits -> [0,1).
    (z >> 11) as f64 / (1u64 << 53) as f64
}

/// Mix an index into a master seed (for per-input determinism).
pub(crate) fn mix_seed(seed: u64, index: usize) -> u64 {
    let mut z = seed
        .wrapping_add((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    z = (z ^ (z >> 33)).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    z ^= z >> 33;
    z
}
