//! Reusable, spec-agnostic TLC-behaviour → `Engine` replay harness (#1226,
//! Deliverable B).
//!
//! The formal-verification epic (#1224) requires every TLA+ spec to be tied to
//! the Rust implementation: TLC-generated behaviours replayed against the real
//! engine, with any divergence failing a test. This module is the shared engine
//! side of that anchor. It is deliberately **not** hardwired to TokenFlow:
//!
//!   * [`Fixture`] parses the committed `formal/tla/traces/<Spec>/<Model>.json`
//!     artifacts (produced by `formal/tla/gen-traces.sh`) — a spec **graph**
//!     (nodes + kinds + flows) and the **milestone multiset** TLC observed on
//!     the shortest completing behaviour.
//!   * A spec family supplies a [`TraceMapping`]: how to build its process from
//!     the spec graph, and how to project the engine's `Event` stream onto the
//!     same milestone vocabulary.
//!   * [`validate`] runs the process to quiescence and asserts the engine's
//!     observed milestone multiset equals the spec's.
//!
//! Sibling specs (#1227 job-lease/activation, #1240 Zeebe parity) anchor their
//! own models by implementing [`TraceMapping`] and calling [`validate`] — they
//! reuse this driver rather than copying the deploy/drive/compare loop.
//!
//! Reuse entry point (documented in `formal/README.md`):
//!
//! ```ignore
//! #[path = "trace_validation/harness.rs"]
//! mod harness;
//! use harness::{Fixture, Milestone, TraceMapping, validate};
//! ```
//!
//! The comparison is a **multiset** (sorted `Vec`) equality, which is what makes
//! the anchor sound for interleaving-nondeterministic parallel behaviour: the
//! trace-anchored TokenFlow corpus is restricted to routing-deterministic,
//! parallel-only models whose observable milestone multiset is invariant under
//! interleaving, so multiset equality is an exact check with no tolerated
//! mismatch and no retries.

#![allow(dead_code)]

use std::collections::BTreeMap;

use nanobpmn_engine_core::{Command, Engine, Event, ProcessDefinition};
use serde_json::Value;

/// One observable milestone in the shared spec/engine vocabulary. Ordering is
/// derived so a `Vec<Milestone>` can be sorted into a canonical multiset for
/// comparison.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Milestone {
    /// A token moved along a sequence flow (spec `pending[flow]` increment /
    /// engine `SequenceFlowTaken`). The `<create>` pseudo-flow into the start
    /// event is not a milestone. Only the endpoints `(from, to)` are observable:
    /// the engine's `SequenceFlowTaken` carries no per-flow identity, so a model
    /// with two distinct flows sharing endpoints is model-checked but not
    /// trace-anchored (its milestone multiset could not tell the two apart).
    Flow { from: String, to: String },
    /// A task element reached its wait state (spec `waiting[node]` increment /
    /// engine `JobCreated`).
    Task { node: String },
    /// A join fired, consuming one token per incoming flow (spec `fireCount`
    /// ghost increment / engine `ParallelJoinFired`).
    JoinFired { node: String },
    /// The process instance completed.
    Completed,
}

/// The process graph TLC evaluated for a model: node kinds and sequence flows.
/// The spec vocabulary (`start`/`end`/`task`/`and`/`or`/`xor`) is intentionally
/// engine-agnostic; a [`TraceMapping`] decides how each maps onto engine
/// elements.
#[derive(Debug, Clone)]
pub struct Graph {
    pub start: String,
    /// node id → spec kind (`start`, `end`, `task`, `and`, `or`, `xor`).
    pub nodes: BTreeMap<String, String>,
    /// sequence flows as `(id, from, to)`, in declaration order (duplicates
    /// preserved — a model may declare two flows with the same endpoints).
    pub flows: Vec<(String, String, String)>,
}

/// A parsed committed trace fixture.
#[derive(Debug, Clone)]
pub struct Fixture {
    pub spec: String,
    pub model: String,
    pub graph: Graph,
    /// The milestone multiset TLC observed on the witnessed behaviour.
    pub milestones: Vec<Milestone>,
}

impl Fixture {
    /// Parse a fixture from its committed JSON (`serde_json::Value`, so no
    /// `#[derive(Deserialize)]` and thus no dependency on the optional `serde`
    /// feature — the harness compiles with or without `--features serde`).
    pub fn from_json(text: &str) -> Result<Self, String> {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("fixture JSON: {e}"))?;
        let spec = str_field(&v, "spec")?;
        let model = str_field(&v, "model")?;
        let g = v.get("graph").ok_or("fixture missing `graph`")?;
        let start = str_field(g, "start")?;
        let mut nodes = BTreeMap::new();
        for (k, kind) in g
            .get("nodes")
            .and_then(Value::as_object)
            .ok_or("graph.nodes must be an object")?
        {
            nodes.insert(
                k.clone(),
                kind.as_str()
                    .ok_or("node kind must be a string")?
                    .to_string(),
            );
        }
        let mut flows = Vec::new();
        for f in g
            .get("flows")
            .and_then(Value::as_array)
            .ok_or("graph.flows must be an array")?
        {
            flows.push((
                str_field(f, "id")?,
                str_field(f, "from")?,
                str_field(f, "to")?,
            ));
        }
        let mut milestones = Vec::new();
        for m in v
            .get("milestones")
            .and_then(Value::as_array)
            .ok_or("fixture missing `milestones`")?
        {
            milestones.push(parse_milestone(m)?);
        }
        Ok(Fixture {
            spec,
            model,
            graph: Graph {
                start,
                nodes,
                flows,
            },
            milestones,
        })
    }
}

fn str_field(v: &Value, key: &str) -> Result<String, String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("missing/!string field `{key}`"))
}

fn parse_milestone(v: &Value) -> Result<Milestone, String> {
    match v.get("kind").and_then(Value::as_str) {
        Some("flow") => Ok(Milestone::Flow {
            from: str_field(v, "from")?,
            to: str_field(v, "to")?,
        }),
        Some("task") => Ok(Milestone::Task {
            node: str_field(v, "node")?,
        }),
        Some("joinFired") => Ok(Milestone::JoinFired {
            node: str_field(v, "node")?,
        }),
        Some("completed") => Ok(Milestone::Completed),
        other => Err(format!("unknown milestone kind {other:?}")),
    }
}

/// A spec family's binding to the engine: build its process from the spec graph,
/// and project the engine event stream onto [`Milestone`]s.
pub trait TraceMapping {
    /// The process id used for deploy/create.
    fn process_id(&self, fixture: &Fixture) -> String;
    /// Build the engine process from the spec graph.
    fn build_process(&self, fixture: &Fixture) -> ProcessDefinition;
    /// Project the engine's observable events onto the shared milestone
    /// vocabulary (the counterpart to what TLC recorded).
    fn engine_milestones(&self, events: &[Event]) -> Vec<Milestone>;
}

/// The canonical engine-side construction for the shared spec graph vocabulary.
///
/// Every spec family whose graph uses the `start`/`end`/`task`/`and`/`or`/`xor`
/// node kinds (TokenFlow, ZeebeTokenFlow, and any sibling) builds its engine
/// process identically:
///   * `start` → start event, `end` → end event
///   * `task`  → **service task** (a wait state that creates a job; the harness
///     drives it to completion), job type = the node id
///   * `and`   → parallel gateway, `or` → inclusive gateway, `xor` → exclusive
///     gateway
///
/// This is the single source of truth for that mapping (#1259): spec families
/// delegate to it from their `TraceMapping::build_process` so a correction to
/// the graph→engine binding lands in one place instead of drifting across
/// per-spec copies.
pub fn build_spec_process(fixture: &Fixture) -> ProcessDefinition {
    let g = &fixture.graph;
    let mut b = nanobpmn_engine_core::ProcessBuilder::new(fixture.model.clone());
    for (id, kind) in &g.nodes {
        b = match kind.as_str() {
            "start" => b.start_event(id.clone()),
            "end" => b.end_event(id.clone()),
            "task" => b.service_task(id.clone(), id.clone()),
            "and" => b.parallel_gateway(id.clone()),
            "or" => b.inclusive_gateway(id.clone()),
            "xor" => b.exclusive_gateway(id.clone()),
            other => panic!("unmapped spec node kind {other:?}"),
        };
    }
    for (_id, from, to) in &g.flows {
        b = b.connect(from.clone(), to.clone());
    }
    b.build().expect("spec graph builds a valid process")
}

/// The canonical engine-side projection onto the shared milestone vocabulary.
///
/// The engine's `Event` stream carries the observable facts every token-flow
/// spec family reasons about (`SequenceFlowTaken`, `JobCreated`,
/// `ParallelJoinFired`, `ProcessInstanceCompleted`), so the projection is
/// identical across families. This is the single source of truth for it
/// (#1259): spec families delegate to it from their
/// `TraceMapping::engine_milestones` so a correction to the event→milestone
/// binding lands in one place instead of drifting across per-spec copies.
pub fn spec_engine_milestones(events: &[Event]) -> Vec<Milestone> {
    let mut out = Vec::new();
    for e in events {
        match e {
            Event::SequenceFlowTaken { from, to, .. } => out.push(Milestone::Flow {
                from: from.to_string(),
                to: to.to_string(),
            }),
            Event::JobCreated { element_id, .. } => out.push(Milestone::Task {
                node: element_id.to_string(),
            }),
            Event::ParallelJoinFired { element_id, .. } => out.push(Milestone::JoinFired {
                node: element_id.to_string(),
            }),
            Event::ProcessInstanceCompleted { .. } => out.push(Milestone::Completed),
            _ => {}
        }
    }
    out
}

/// Deploy the mapped process, create one instance, drive every created job to
/// completion until the instance quiesces, and return the full event stream.
/// Spec-agnostic: the only spec input is the [`TraceMapping`]-built process.
pub fn run_to_quiescence(
    mapping: &dyn TraceMapping,
    fixture: &Fixture,
) -> Result<Vec<Event>, String> {
    let def = mapping.build_process(fixture);
    let pid = mapping.process_id(fixture);
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(def))
        .map_err(|e| format!("deploy: {e:?}"))?;

    let mut events = engine
        .apply_command(Command::create_instance(pid))
        .map_err(|e| format!("create: {e:?}"))?;

    let mut completed: std::collections::BTreeSet<u64> = Default::default();
    // Drive every created job to completion until the instance quiesces. A job
    // must be activated before it can be completed (the engine rejects
    // completing an unactivated job); activation itself emits no token-flow
    // milestone, so it does not perturb the observed multiset. Bounded: a
    // completing model has finitely many jobs, each key completed at most once.
    const MAX_ITERS: usize = 10_000;
    let mut quiesced = false;
    for _ in 0..MAX_ITERS {
        let pending: Option<(u64, String)> = events.iter().find_map(|e| match e {
            Event::JobCreated {
                job_key, job_type, ..
            } if !completed.contains(job_key) => Some((*job_key, job_type.clone())),
            _ => None,
        });
        let Some((_, job_type)) = pending else {
            quiesced = true;
            break;
        };
        let activated =
            engine.activate_jobs(job_type.clone(), "trace-validation", usize::MAX, 60_000, 0);
        if activated.is_empty() {
            return Err(format!("no activatable job for type {job_type:?}"));
        }
        for job in activated {
            if !completed.insert(job.key) {
                continue;
            }
            let more = engine
                .apply_command(Command::complete_job(job.key))
                .map_err(|e| format!("complete job {}: {e:?}", job.key))?;
            events.extend(more);
        }
    }
    if !quiesced {
        return Err(format!(
            "run_to_quiescence exhausted its {MAX_ITERS}-iteration bound without the \
             instance quiescing (a job mapping that never completes, or a model with \
             more than {MAX_ITERS} job completions); the replay is partial and cannot \
             be compared against the spec"
        ));
    }
    Ok(events)
}

/// Sort a milestone list into its canonical multiset form.
fn multiset(mut v: Vec<Milestone>) -> Vec<Milestone> {
    v.sort();
    v
}

/// Replay a fixture's behaviour against the engine and assert the engine's
/// observed milestone multiset equals the spec's. Returns `Err` (never panics)
/// on divergence so callers — and the harness's own detector test — can inspect
/// the mismatch.
pub fn validate(mapping: &dyn TraceMapping, fixture: &Fixture) -> Result<(), String> {
    let events = run_to_quiescence(mapping, fixture)?;
    let engine = multiset(mapping.engine_milestones(&events));
    let spec = multiset(fixture.milestones.clone());
    if engine == spec {
        return Ok(());
    }
    Err(format!(
        "milestone divergence for {}/{}\n  spec  : {:?}\n  engine: {:?}\n  only in spec  : {:?}\n  only in engine: {:?}",
        fixture.spec,
        fixture.model,
        spec,
        engine,
        difference(&spec, &engine),
        difference(&engine, &spec),
    ))
}

/// Multiset difference `a \ b` (respecting multiplicity), for readable error
/// messages.
fn difference(a: &[Milestone], b: &[Milestone]) -> Vec<Milestone> {
    let mut remaining: Vec<Milestone> = b.to_vec();
    let mut out = Vec::new();
    for m in a {
        if let Some(pos) = remaining.iter().position(|x| x == m) {
            remaining.remove(pos);
        } else {
            out.push(m.clone());
        }
    }
    out
}
