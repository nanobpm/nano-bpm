//! The SimRunner: drives the **real** `engine-core` with a virtual clock to run
//! one scenario input through one candidate, collecting `(output, latency, cost,
//! incidents, correctness)`. This is the M0 base-dataset collector — the same
//! engine production runs, so its measurements transfer.
//!
//! Each input runs on a *fresh* [`Engine`] so the virtual clock measures that
//! single instance's end-to-end latency cleanly and deterministically (the MVP
//! studies per-instance output/latency/cost, not cross-instance throughput —
//! that is the ClusterRunner's job, M3).

use std::collections::BTreeMap;

use nanobpmn_engine_core::{
    Command, Engine, Event, IncidentState, JobState, ProcessDefinition, ProcessInstanceState,
    TimerState, Value,
};
use serde_json::Value as Json;

use super::{draw, Scenario, WorkerModel};

/// Outcome of running ONE input through ONE candidate on a fresh engine.
#[derive(Clone, Debug)]
pub struct InstanceRun {
    /// Whether the instance reached the `Completed` terminal state.
    pub completed: bool,
    /// Virtual end-to-end latency (sum of the service times along the taken path).
    pub e2e_latency_ms: u64,
    /// Total cost charged by the workers that ran.
    pub cost: f64,
    /// Active incidents left on the instance (e.g. an exhausted-retry job failure).
    pub incidents: u32,
    /// Whether every `expected` key/value is present and equal in the output.
    pub correct: bool,
    /// Final instance variables (the process output) as natural JSON. Retained
    /// for inspection/debugging; aggregation reads correctness/latency/cost.
    #[allow(dead_code)]
    pub output: BTreeMap<String, Json>,
}

/// Run a single input through the model under the given effective assignment
/// (`job_type -> worker_id`, already merged with the defaults). Pure and
/// deterministic for a given `seed`.
pub fn run_instance(
    defs: &[ProcessDefinition],
    process_id: &str,
    input_vars: &BTreeMap<String, Json>,
    expected: &BTreeMap<String, Json>,
    assignment: &BTreeMap<String, String>,
    scenario: &Scenario,
    seed: u64,
) -> InstanceRun {
    let mut engine = Engine::new();
    let mut clock: u64 = 0;
    let mut cost = 0.0_f64;

    // Reconstruct the instance's terminal variables by folding every
    // `VariablesUpdated` delta the engine emits (last-writer-wins), seeded with
    // the creation inputs. The engine drops a terminal instance's variable
    // payload on completion (ADR 0012), so correctness must be checked against
    // the event-projected output rather than cleared hot state.
    let mut output: BTreeMap<String, Json> = input_vars.clone();

    // Deploy + create. Errors here would indicate a malformed model; the caller
    // validates the BPMN up front, so we surface failure as a non-completing run.
    apply_and_fold(
        &mut engine,
        &mut output,
        Command::DeployResources(defs.to_vec()),
        clock,
    );
    let vars: BTreeMap<String, Value> = input_vars
        .iter()
        .map(|(k, v)| (k.clone(), json_to_value(v)))
        .collect();
    apply_and_fold(
        &mut engine,
        &mut output,
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

    // Drive to a terminal/quiescent state. The guard bounds pathological loops.
    let max_steps = 100_000usize;
    let mut steps = 0usize;
    loop {
        steps += 1;
        if steps > max_steps {
            break;
        }

        let pending: Vec<(u64, String, bool)> = engine
            .state()
            .jobs
            .values()
            .filter(|j| matches!(j.state, JobState::Created | JobState::Activated))
            .map(|j| (j.key, j.job_type.clone(), j.state == JobState::Created))
            .collect();

        if pending.is_empty() {
            // No runnable jobs: advance to the earliest armed timer, if any.
            let next_due = engine
                .state()
                .timers
                .values()
                .filter(|t| t.state == TimerState::Created)
                .map(|t| t.due_at)
                .min();
            match next_due {
                Some(due) => {
                    clock = clock.max(due);
                    apply_and_fold(
                        &mut engine,
                        &mut output,
                        Command::TriggerTimers { now: clock },
                        clock,
                    );
                    apply_and_fold(
                        &mut engine,
                        &mut output,
                        Command::ExpireJobs { now: clock },
                        clock,
                    );
                    continue;
                }
                None => break, // settled: completed, terminated, or parked on an incident
            }
        }

        for (job_key, job_type, needs_activation) in pending {
            let worker = resolve_worker(scenario, assignment, &job_type);
            // The mock worker takes `latency_ms` and charges `cost`.
            clock = clock.saturating_add(worker.latency_ms);
            cost += worker.cost;

            if needs_activation {
                apply_and_fold(
                    &mut engine,
                    &mut output,
                    Command::activate_jobs(
                        job_type.clone(),
                        "sim".to_string(),
                        100_000,
                        u64::MAX / 4,
                        clock,
                    ),
                    clock,
                );
            }

            // Seeded outcome: same worker fails on the same logical job across
            // every candidate, so comparisons are apples-to-apples.
            if draw(seed, job_key) < worker.failure_rate {
                apply_and_fold(
                    &mut engine,
                    &mut output,
                    Command::fail_job(job_key, 0, format!("mock worker '{}' failed", worker.id)),
                    clock,
                );
            } else {
                let out: BTreeMap<String, Value> = worker
                    .output
                    .iter()
                    .map(|(k, v)| (k.clone(), json_to_value(v)))
                    .collect();
                apply_and_fold(
                    &mut engine,
                    &mut output,
                    Command::complete_job_with(job_key, out),
                    clock,
                );
            }
        }
    }

    let state = engine.state();
    let inst = state.instances.values().next();
    let completed = inst
        .map(|i| i.state == ProcessInstanceState::Completed)
        .unwrap_or(false);
    // `output` was reconstructed from the emitted `VariablesUpdated` events; the
    // completed instance's hot-state variables have been dropped (ADR 0012).
    let incidents = state
        .incidents
        .values()
        .filter(|i| i.state == IncidentState::Active)
        .count() as u32;
    let correct = completed && expected.iter().all(|(k, v)| output.get(k) == Some(v));

    InstanceRun {
        completed,
        e2e_latency_ms: clock,
        cost,
        incidents,
        correct,
        output,
    }
}

/// Resolve which worker runs a job of `job_type` under `assignment`, falling back
/// to the scenario default, then to a free no-op so unmapped tasks never stall.
fn resolve_worker(
    scenario: &Scenario,
    assignment: &BTreeMap<String, String>,
    job_type: &str,
) -> WorkerModel {
    let worker_id = assignment
        .get(job_type)
        .or_else(|| scenario.task_workers.get(job_type));
    if let Some(id) = worker_id {
        if let Some(w) = scenario.workers.get(id) {
            return w.clone();
        }
    }
    // Unmapped job type: instant, free, no output — keeps the token moving.
    WorkerModel {
        id: format!("noop:{job_type}"),
        cost: 0.0,
        latency_ms: 0,
        failure_rate: 0.0,
        output: BTreeMap::new(),
    }
}

// --- Value <-> JSON converters (mirrors engine-wasm/src/lib.rs) -----------------

/// Applies a command to the sim engine and folds any `VariablesUpdated` deltas
/// it emits into `output` (last-writer-wins), reconstructing the instance's
/// terminal variables from the event stream since the engine drops them on
/// completion (ADR 0012). Command errors are swallowed (no-op), matching the
/// prior `let _ =` behaviour.
fn apply_and_fold(
    engine: &mut Engine,
    output: &mut BTreeMap<String, Json>,
    cmd: Command,
    clock: u64,
) {
    if let Ok(events) = engine.apply_command_at(cmd, clock) {
        for e in &events {
            if let Event::VariablesUpdated { variables, .. } = e {
                for (k, v) in variables {
                    output.insert(k.clone(), value_to_json(v));
                }
            }
        }
    }
}

fn value_to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => Json::Number((*i).into()),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Str(s) => Json::String(s.clone()),
        Value::List(items) => Json::Array(items.iter().map(value_to_json).collect()),
        Value::Map(entries) => {
            let mut m = serde_json::Map::new();
            for (k, val) in entries {
                m.insert(k.clone(), value_to_json(val));
            }
            Json::Object(m)
        }
    }
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
