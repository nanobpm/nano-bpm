//! Trace-validation conformance for the **JobLease** spec (#1227, epic #1224):
//! nano's job lease / activation semantics (ADR 0002; ADR 0001/0017) anchored to
//! the real engine, so the TLA+ model in `formal/tla/joblease/JobLease.tla`
//! cannot silently drift from `engine-core`.
//!
//! This reuses the spec-agnostic #1226 replay harness
//! (`engine-core/tests/trace_validation/harness.rs`) rather than copying its
//! deploy/drive/compare loop:
//!
//!   * The **activation lifecycle** (create -> activate -> complete) is anchored
//!     through the harness `TraceMapping` + `validate` entry point, exactly as
//!     `token_flow.rs` does: a one-job JobLease model is a single-service-task
//!     process, and the engine's observable milestone multiset must equal the
//!     spec's.
//!   * The **lease safety** the token-flow milestone vocabulary cannot express
//!     — exclusivity (at-most-one live holder), reclaim after expiry, and
//!     done-is-terminal — is anchored by replaying committed JobLease behaviours
//!     (`trace_validation/joblease/*.json`) against `Engine::apply_command`,
//!     driving the engine's `ActivateJobs` / `ExpireJobs` / `CompleteJob`
//!     surface and asserting its job-lease `Event`s and the two spec invariants
//!     at every step. A divergence fails the test — no tolerated mismatch, no
//!     retries.
//!
//! Each committed behaviour is a **two-sided** anchor: the `spec_model`
//! validator asserts it is an *admitted* `JobLease.tla` behaviour (every step is
//! an enabled spec action — or, for an engine sweep that reclaims nothing, a
//! spec *stutter* step — with invariants holding throughout), and the engine
//! replay
//! asserts the same fixture matches `engine-core`. Because both checks run
//! against the *same* fixture, the engine is tied to the spec through it and
//! neither can silently drift (finding #1257). The `spec_model` module is a
//! small, commented mirror of the `.tla` transition relation — the
//! reviewer-sanctioned "equivalent spec-side validator" alternative to
//! generating fixtures from TLC.
//!
//! The lease pipeline deliberately does **not** go through the TokenFlow
//! `gen-traces.sh` / `parse.mjs` fixtures (whose `pending`/`waiting`/`fireCount`
//! observable vocabulary is token-flow specific and owned by #1240): a
//! lease/expiry behaviour is a per-state safety trace, not an
//! interleaving-invariant milestone multiset, so the JobLease descriptor
//! declares no `SPEC_TRACE_MODELS` and this test carries its own behaviours.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
#[path = "trace_validation/harness.rs"]
mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use harness::{Fixture, Graph, Milestone, TraceMapping};
use nanobpmn_engine_core::{Command, Engine, Event, ProcessBuilder, ProcessDefinition};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Spec-side admissibility validator: a faithful mirror of the JobLease.tla
// transition relation.
//
// Findings #1257: the committed JSON fixtures must not be *hand-written
// expectations* that can silently drift from `formal/tla/joblease/JobLease.tla`.
// The engine replay below proves each fixture matches the engine; this module
// proves the *same fixture* is an admitted `JobLease.tla` behaviour — every step
// is an ENABLED spec action with the fixture's exact expectation (or, for an
// engine `ExpireJobs` sweep that reclaims nothing, an admitted spec STUTTER
// step — see the `expire` arm), the spec invariants hold after each step, and
// the clock only advances (the `Tick`
// guard). A fixture is therefore a two-sided anchor: engine <-> shared fixture
// <-> spec. If either side drifts, one of the two checks fails.
//
// This is the reviewer-sanctioned "equivalent spec-side validator" alternative
// to generating fixtures from TLC. It mirrors the transition relation directly
// (guards + effects of Activate/Expire/Complete/Tick), so it stays a small,
// commented reflection of the `.tla` rather than a second TLC toolchain.
mod spec_model {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::Value;

    use super::{str_at, u64_at};

    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct Lock {
        deadline: u64,
        worker: String,
    }

    struct State {
        timeout: u64,
        locks: BTreeMap<String, BTreeSet<Lock>>,
        done: BTreeSet<String>,
        /// The persistent `activated` latch (`JobLease.tla`'s `activated`): set by
        /// `Activate`, never cleared by `Expire`, and the `Complete` precondition
        /// (finding #1257). Mirrors the engine's `Job::activated`, which the
        /// `JobLockExpired` reducer leaves set.
        activated: BTreeSet<String>,
        clock: u64,
    }

    impl State {
        /// `LiveLocks(j)` — locks whose deadline is strictly in the future.
        fn live_locks(&self, j: &str) -> BTreeSet<Lock> {
            self.locks
                .get(j)
                .into_iter()
                .flatten()
                .filter(|l| l.deadline > self.clock)
                .cloned()
                .collect()
        }

        fn has_live_lock(&self, j: &str) -> bool {
            !self.live_locks(j).is_empty()
        }

        /// Advance the logical clock to `now` via `Tick`s. `Tick` only ever moves
        /// the clock forward, so a fixture whose timed steps go backwards is not
        /// an admitted behaviour. This enforces the `Tick` *semantic* (monotone
        /// advance), which is the real engine/spec constraint, but deliberately
        /// does not enforce `JobLease.tla`'s `clock \in 0..MaxClock` bound:
        /// `MaxClock` is only a TLC state-enumeration bound (the checked MC models
        /// use a tiny value, 2), not a semantic limit, and the fixtures
        /// intentionally run at a far larger clock (1000). Carrying it here would
        /// reject every real fixture, so admissibility is checked against the
        /// unbounded transition relation, not the finite model-checking cap.
        fn tick_to(&mut self, now: u64, ctx: &dyn Fn(String) -> String) -> Result<(), String> {
            if now < self.clock {
                return Err(ctx(format!(
                    "clock cannot move backwards (Tick only advances): now {now} < clock {}",
                    self.clock
                )));
            }
            self.clock = now;
            Ok(())
        }

        /// `AtMostOneLiveHolder`, `DoneIsTerminal`, and `CompletedWasActivated` —
        /// the three *state* invariants checked after every step, matching those
        /// `.tla` invariants. `JobLease.tla`'s fourth registered invariant,
        /// `ExpiredIsReclaimable`, is not a post-step state predicate but an
        /// *enabledness* property (`~done[j] /\ ~HasLiveLock(j) => \E w :
        /// ENABLED Activate(j, w)`); this mirror holds it *by construction*,
        /// because its `Activate` guard is exactly `~done /\ ~has_live_lock`, so a
        /// non-done job with no live lock is always re-activatable. It is
        /// therefore guaranteed structurally rather than asserted here.
        fn assert_invariants(&self, ctx: &dyn Fn(String) -> String) -> Result<(), String> {
            for j in self.locks.keys() {
                if self.live_locks(j).len() > 1 {
                    return Err(ctx(format!(
                        "AtMostOneLiveHolder violated: {j} has >1 live lock"
                    )));
                }
            }
            for j in &self.done {
                if !self.locks.get(j).is_none_or(BTreeSet::is_empty) {
                    return Err(ctx(format!(
                        "DoneIsTerminal violated: completed {j} still holds a lock"
                    )));
                }
                // CompletedWasActivated: a completed job was activated at least
                // once (the completion latch, finding #1257).
                if !self.activated.contains(j) {
                    return Err(ctx(format!(
                        "CompletedWasActivated violated: completed {j} was never activated"
                    )));
                }
            }
            Ok(())
        }
    }

    /// Assert `fixture` is an admitted `JobLease.tla` behaviour: every step is an
    /// enabled spec action carrying the fixture's exact expectation, and the spec
    /// invariants hold throughout. Returns `Err` (never panics) on inadmissibility
    /// so the detector test can prove the validator is not vacuous.
    pub fn assert_admitted(fixture: &Value) -> Result<(), String> {
        let model = str_at(fixture, "model")?;
        // Enforce this directory's spec-registration boundary: a fixture under
        // `trace_validation/joblease` is only a `JobLease.tla` behaviour if it
        // declares `"spec": "JobLease"`. A JSON copied in with a different or
        // absent spec must be rejected rather than silently reported as JobLease
        // conformance (finding #1257).
        let spec = str_at(fixture, "spec")?;
        if spec != "JobLease" {
            return Err(format!(
                "spec-model {model}: fixture spec {spec:?} is not \"JobLease\""
            ));
        }
        let timeout = u64_at(fixture, "timeout")?;
        // `JobLease.tla`: `ASSUME Timeout \in Nat /\ Timeout > 0`. A `timeout` of
        // 0 names a constant no JobLease model can instantiate, so a fixture using
        // it is not a behaviour of any admitted model — reject it here rather than
        // let it pass a vacuous spec-admissibility check (finding #1257).
        if timeout == 0 {
            return Err(format!(
                "spec-model {model}: timeout must be > 0 (JobLease.tla ASSUME Timeout > 0)"
            ));
        }
        let jobs: Vec<String> = fixture
            .get("jobs")
            .and_then(Value::as_array)
            .ok_or("fixture missing `jobs`")?
            .iter()
            .map(|j| {
                j.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| "job id must be a string".to_string())
            })
            .collect::<Result<_, _>>()?;

        // Init: every job available (no locks), nothing done or activated, clock 0.
        let mut st = State {
            timeout,
            locks: jobs.iter().map(|j| (j.clone(), BTreeSet::new())).collect(),
            done: BTreeSet::new(),
            activated: BTreeSet::new(),
            clock: 0,
        };

        let steps = fixture
            .get("steps")
            .and_then(Value::as_array)
            .ok_or("fixture missing `steps`")?;

        for (i, step) in steps.iter().enumerate() {
            let action = str_at(step, "action")?;
            let ctx = |msg: String| format!("spec-model {model} step {i} ({action}): {msg}");
            match action.as_str() {
                "activate" => {
                    let job = str_at(step, "job")?;
                    let worker = str_at(step, "worker")?;
                    let now = u64_at(step, "now")?;
                    let expect_leased = step
                        .get("expect_leased")
                        .and_then(Value::as_bool)
                        .ok_or_else(|| ctx("missing/!bool `expect_leased`".into()))?;
                    if !st.locks.contains_key(&job) {
                        return Err(ctx(format!("unknown job {job}")));
                    }
                    st.tick_to(now, &ctx)?;
                    // `Activate(j, w)` guard: `~done[j] /\ ~HasLiveLock(j)`.
                    let enabled = !st.done.contains(&job) && !st.has_live_lock(&job);
                    if enabled != expect_leased {
                        return Err(ctx(format!(
                            "Activate({job},{worker}) enabled={enabled} but fixture expects leased={expect_leased}"
                        )));
                    }
                    if enabled {
                        // Effect: `locks[j] = LiveLocks(j) \cup {new}` — drop the
                        // stale expired lock, keep any live one (there is none
                        // under the guard), add the new lease. Set the persistent
                        // `activated` latch (`activated' = [.. EXCEPT ![j] = TRUE]`).
                        // `JobLease.tla` computes the deadline as `clock +
                        // Timeout` in unbounded `Nat` arithmetic, so it never
                        // saturates. Clamping a u64 `now + timeout` at `u64::MAX`
                        // would silently diverge from the spec at the boundary: a
                        // saturated `u64::MAX` deadline admits an `Expire` at an
                        // instant the spec's strictly larger deadline forbids.
                        // Refuse to anchor such a fixture rather than validate the
                        // engine against a deadline this validator cannot
                        // faithfully represent (finding #1257).
                        let deadline = now.checked_add(st.timeout).ok_or_else(|| {
                            ctx(format!(
                                "deadline now ({now}) + timeout ({}) overflows u64; \
                                 JobLease.tla uses unbounded Nat arithmetic, so this \
                                 validator cannot faithfully anchor the boundary",
                                st.timeout
                            ))
                        })?;
                        let mut next = st.live_locks(&job);
                        next.insert(Lock { deadline, worker });
                        st.locks.insert(job.clone(), next);
                        st.activated.insert(job);
                    }
                    st.assert_invariants(&ctx)?;
                }
                "expire" => {
                    let now = u64_at(step, "now")?;
                    let expect: BTreeSet<String> = step
                        .get("expect_expired")
                        .and_then(Value::as_array)
                        .ok_or_else(|| ctx("missing `expect_expired` array".into()))?
                        .iter()
                        .map(|j| {
                            j.as_str()
                                .map(str::to_string)
                                .ok_or_else(|| ctx("job id must be a string".into()))
                        })
                        .collect::<Result<_, _>>()?;
                    st.tick_to(now, &ctx)?;
                    // `Expire(j)` is enabled per job with a lock at/past deadline;
                    // the engine's sweep reclaims every such job at once.
                    let reclaimed: BTreeSet<String> = st
                        .locks
                        .iter()
                        .filter(|(_, ls)| ls.iter().any(|l| l.deadline <= st.clock))
                        .map(|(j, _)| j.clone())
                        .collect();
                    if reclaimed != expect {
                        return Err(ctx(format!(
                            "Expire reclaims {reclaimed:?} but fixture expects {expect:?}"
                        )));
                    }
                    if reclaimed.is_empty() {
                        // Engine `ExpireJobs` sweep that reclaims nothing: no
                        // `Expire(j)` spec action is enabled (its guard
                        // `\E l \in locks[j] : l.deadline <= clock` holds for no
                        // job — the `reclaimed == expect == {}` check above
                        // already proves that for every job), so this engine-API
                        // no-op maps to a spec STUTTER step: an admitted
                        // `[Next]_vars` behaviour that changes no state. It is
                        // modeled explicitly as a stutter here rather than
                        // masqueraded as an enabled `Expire` action (advisory
                        // #1257), keeping the two-sided anchor honest.
                    } else {
                        // Effect: `locks[j] = LiveLocks(j)` for every reclaimed job.
                        for j in &reclaimed {
                            let live = st.live_locks(j);
                            st.locks.insert(j.clone(), live);
                        }
                    }
                    st.assert_invariants(&ctx)?;
                }
                "complete" => {
                    let job = str_at(step, "job")?;
                    if !st.locks.contains_key(&job) {
                        return Err(ctx(format!("unknown job {job}")));
                    }
                    // `Complete(j)` guard: `~done[j] /\ activated[j]` — the
                    // persistent activation latch, NOT a live or recorded lock. A
                    // job reclaimed by `Expire` (its lock swept) keeps the latch,
                    // so it still completes without re-activation (finding #1257).
                    let enabled = !st.done.contains(&job) && st.activated.contains(&job);
                    if !enabled {
                        return Err(ctx(format!(
                            "Complete({job}) is disabled in the spec (job is done or was never activated)"
                        )));
                    }
                    st.done.insert(job.clone());
                    st.locks.insert(job, BTreeSet::new());
                    // `activated` is a latch: unchanged by `Complete`.
                    st.assert_invariants(&ctx)?;
                }
                other => return Err(ctx(format!("unknown action {other:?}"))),
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Activation-lifecycle anchor: reuse the #1226 harness driver.

/// A one-job JobLease model as a single-service-task process: the job's
/// create -> activate -> complete lifecycle is the engine's job lifecycle. This
/// is the JobLease binding of the shared `TraceMapping` seam.
struct JobLeaseLifecycleMapping;

impl TraceMapping for JobLeaseLifecycleMapping {
    fn process_id(&self, fixture: &Fixture) -> String {
        fixture.model.clone()
    }

    fn build_process(&self, fixture: &Fixture) -> ProcessDefinition {
        let g = &fixture.graph;
        let mut b = ProcessBuilder::new(fixture.model.clone());
        for (id, kind) in &g.nodes {
            b = match kind.as_str() {
                "start" => b.start_event(id.clone()),
                "end" => b.end_event(id.clone()),
                "task" => b.service_task(id.clone(), id.clone()),
                other => panic!("unmapped JobLease node kind {other:?}"),
            };
        }
        for (_id, from, to) in &g.flows {
            b = b.connect(from.clone(), to.clone());
        }
        b.build()
            .expect("JobLease lifecycle graph builds a valid process")
    }

    fn engine_milestones(&self, events: &[Event]) -> Vec<Milestone> {
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
                Event::ProcessInstanceCompleted { .. } => out.push(Milestone::Completed),
                _ => {}
            }
        }
        out
    }
}

/// The activation lifecycle of a single leased job conforms to the engine: a
/// created job is activated to exactly one holder and driven to completion,
/// producing the spec's milestone multiset (via `harness::validate`).
///
/// Finding #1257: the milestone list is token-flow vocabulary, so on its own it
/// would only prove a hand-written process matches a hand-written list. To make
/// this a genuine *JobLease* anchor, the create -> activate -> complete lifecycle
/// it exercises is first asserted to be an admitted `JobLease.tla` behaviour
/// through the spec-side validator (the same one guarding the committed
/// fixtures), tying the milestone claim to the spec rather than to a bare
/// expectation.
#[test]
fn job_lease_lifecycle_conforms_to_engine() {
    // The lifecycle as a JobLease behaviour: activate the single job to one
    // holder at now=0, then complete it. Assert it is spec-admitted before
    // anchoring the engine's milestone multiset against it.
    let lifecycle_behaviour = serde_json::json!({
        "spec": "JobLease",
        "model": "lease-lifecycle",
        "timeout": 1000,
        "jobs": ["work"],
        "steps": [
            { "action": "activate", "job": "work", "worker": "w1", "now": 0, "expect_leased": true },
            { "action": "complete", "job": "work" }
        ]
    });
    spec_model::assert_admitted(&lifecycle_behaviour)
        .expect("the lifecycle is an admitted JobLease.tla behaviour");

    let mut nodes = BTreeMap::new();
    nodes.insert("start".to_string(), "start".to_string());
    nodes.insert("work".to_string(), "task".to_string());
    nodes.insert("end".to_string(), "end".to_string());
    let fixture = Fixture {
        spec: "JobLease".to_string(),
        model: "lease-lifecycle".to_string(),
        graph: Graph {
            start: "start".to_string(),
            nodes,
            flows: vec![
                ("f1".to_string(), "start".to_string(), "work".to_string()),
                ("f2".to_string(), "work".to_string(), "end".to_string()),
            ],
        },
        milestones: vec![
            Milestone::Flow {
                from: "start".to_string(),
                to: "work".to_string(),
            },
            Milestone::Task {
                node: "work".to_string(),
            },
            Milestone::Flow {
                from: "work".to_string(),
                to: "end".to_string(),
            },
            Milestone::Completed,
        ],
    };
    harness::validate(&JobLeaseLifecycleMapping, &fixture)
        .unwrap_or_else(|e| panic!("JobLease lifecycle anchor: {e}"));
}

// ---------------------------------------------------------------------------
// Lease-safety anchor: replay committed JobLease behaviours.

fn joblease_traces_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/trace_validation/joblease")
}

fn load_lease_fixtures() -> Vec<(String, Value)> {
    let dir = joblease_traces_dir();
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|r| r.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    entries.sort();
    let mut out = Vec::new();
    for path in entries {
        let text = std::fs::read_to_string(&path).expect("read fixture");
        let v: Value =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        out.push((path.display().to_string(), v));
    }
    assert!(
        !out.is_empty(),
        "no committed JobLease trace fixtures under {}",
        dir.display()
    );
    out
}

/// Deploy one single-service-task process per abstract job (job_type == job id)
/// and return the engine plus the stable `job id -> job key` map.
fn setup_jobs(jobs: &[String]) -> Result<(Engine, BTreeMap<String, u64>), String> {
    let mut engine = Engine::new();
    let mut keys = BTreeMap::new();
    for job in jobs {
        let pid = format!("lease-{job}");
        let def = ProcessBuilder::new(pid.clone())
            .start_event("start")
            .service_task("work", job.clone())
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .map_err(|e| format!("build process for {job}: {e:?}"))?;
        engine
            .apply_command(Command::DeployProcess(def))
            .map_err(|e| format!("deploy {job}: {e:?}"))?;
        let events = engine
            .apply_command(Command::create_instance(pid))
            .map_err(|e| format!("create instance {job}: {e:?}"))?;
        let key = events
            .iter()
            .find_map(|e| match e {
                Event::JobCreated {
                    job_key, job_type, ..
                } if job_type == job => Some(*job_key),
                _ => None,
            })
            .ok_or_else(|| format!("no JobCreated for job {job}"))?;
        keys.insert(job.clone(), key);
    }
    Ok((engine, keys))
}
/// One lease grant observed during replay: which job (`key`) was handed to which
/// `worker`, and the half-open `[grant, deadline)` window it was live for.
#[derive(Clone, Debug)]
struct LiveWindow {
    key: u64,
    worker: String,
    grant: u64,
    deadline: u64,
}

/// The engine-observable `AtMostOneLiveHolder` invariant.
///
/// Checked against the *activation history* — every lease grant the engine
/// actually made during replay — rather than a snapshot of `activated_leases()`.
/// The latter is keyed by job (one `(job_key, deadline)` tuple per job record),
/// so deduplicating it can never observe the same key twice and is vacuous: it
/// is structurally incapable of witnessing two-holder behaviour. Two grants of
/// the *same* job whose live `[grant, deadline)` windows overlap mean two workers
/// held the job concurrently — the real violation — so we assert directly on the
/// grant transitions instead.
fn assert_at_most_one_live_holder(history: &[LiveWindow]) -> Result<(), String> {
    for (i, a) in history.iter().enumerate() {
        for b in &history[i + 1..] {
            // Half-open windows: a grant at exactly the previous deadline (the
            // lease expired, then was re-leased) does not overlap.
            if a.key == b.key && a.grant < b.deadline && b.grant < a.deadline {
                return Err(format!(
                    "AtMostOneLiveHolder violated: job {} held by {} over [{}, {}) and by {} over [{}, {}) concurrently",
                    a.key, a.worker, a.grant, a.deadline, b.worker, b.grant, b.deadline
                ));
            }
        }
    }
    Ok(())
}

fn str_at(v: &Value, key: &str) -> Result<String, String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("step missing/!string `{key}`"))
}

fn u64_at(v: &Value, key: &str) -> Result<u64, String> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("step missing/!u64 `{key}`"))
}

/// Replay one committed JobLease behaviour against the engine, asserting the
/// job-lease `Event`s and the spec invariants at every step. Returns `Err`
/// (never panics) on divergence so the detector test can prove the replay is not
/// vacuous.
fn replay_lease_behaviour(fixture: &Value) -> Result<(), String> {
    let model = str_at(fixture, "model")?;
    let timeout = u64_at(fixture, "timeout")?;
    let jobs: Vec<String> = fixture
        .get("jobs")
        .and_then(Value::as_array)
        .ok_or("fixture missing `jobs`")?
        .iter()
        .map(|j| {
            j.as_str()
                .map(str::to_string)
                .ok_or("job id must be a string".to_string())
        })
        .collect::<Result<_, _>>()?;

    let (mut engine, keys) = setup_jobs(&jobs)?;
    let by_key: BTreeMap<u64, String> = keys.iter().map(|(j, k)| (*k, j.clone())).collect();
    let mut done: BTreeSet<String> = BTreeSet::new();
    // Accumulated lease-grant history: the observable substrate for the
    // `AtMostOneLiveHolder` invariant (see `assert_at_most_one_live_holder`).
    let mut history: Vec<LiveWindow> = Vec::new();

    let steps = fixture
        .get("steps")
        .and_then(Value::as_array)
        .ok_or("fixture missing `steps`")?;

    for (i, step) in steps.iter().enumerate() {
        let ctx = |msg: String| {
            format!(
                "{model} step {i} ({}): {msg}",
                str_at(step, "action").unwrap_or_default()
            )
        };
        match str_at(step, "action")?.as_str() {
            "activate" => {
                let job = str_at(step, "job")?;
                let worker = str_at(step, "worker")?;
                let now = u64_at(step, "now")?;
                let expect_leased = step
                    .get("expect_leased")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| ctx("missing/!bool `expect_leased`".into()))?;
                let activated = engine.activate_jobs(job.clone(), worker.clone(), 1, timeout, now);
                if expect_leased {
                    if activated.len() != 1 {
                        return Err(ctx(format!(
                            "expected {worker} to lease {job}, but {} jobs activated",
                            activated.len()
                        )));
                    }
                    let a = &activated[0];
                    let want_key = *keys.get(&job).ok_or_else(|| ctx("unknown job".into()))?;
                    if a.key != want_key {
                        return Err(ctx(format!(
                            "activated key {} != job key {want_key}",
                            a.key
                        )));
                    }
                    if a.worker != worker {
                        return Err(ctx(format!("holder {} != expected {worker}", a.worker)));
                    }
                    if a.deadline != now.saturating_add(timeout) {
                        return Err(ctx(format!(
                            "deadline {} != now+timeout {}",
                            a.deadline,
                            now.saturating_add(timeout)
                        )));
                    }
                    history.push(LiveWindow {
                        key: a.key,
                        worker: worker.clone(),
                        grant: now,
                        deadline: a.deadline,
                    });
                } else if !activated.is_empty() {
                    return Err(ctx(format!(
                        "expected {job} to be unavailable to {worker} (live lock held), but it activated"
                    )));
                }
                assert_at_most_one_live_holder(&history).map_err(ctx)?;
            }
            "expire" => {
                let now = u64_at(step, "now")?;
                let expect: BTreeSet<String> = step
                    .get("expect_expired")
                    .and_then(Value::as_array)
                    .ok_or_else(|| ctx("missing `expect_expired` array".into()))?
                    .iter()
                    .map(|j| {
                        j.as_str()
                            .map(str::to_string)
                            .ok_or_else(|| ctx("job id must be a string".into()))
                    })
                    .collect::<Result<_, _>>()?;
                let events = engine.expire_jobs(now);
                let got: BTreeSet<String> = events
                    .iter()
                    .filter_map(|e| match e {
                        Event::JobLockExpired { job_key, .. } => by_key.get(job_key).cloned(),
                        _ => None,
                    })
                    .collect();
                if got != expect {
                    return Err(ctx(format!("reclaimed {got:?} but expected {expect:?}")));
                }
                assert_at_most_one_live_holder(&history).map_err(ctx)?;
            }
            "complete" => {
                let job = str_at(step, "job")?;
                let key = *keys.get(&job).ok_or_else(|| ctx("unknown job".into()))?;
                let events = engine
                    .apply_command(Command::complete_job(key))
                    .map_err(|e| ctx(format!("complete {job}: {e:?}")))?;
                if !events
                    .iter()
                    .any(|e| matches!(e, Event::JobCompleted { job_key, .. } if *job_key == key))
                {
                    return Err(ctx(format!("no JobCompleted for {job}")));
                }
                done.insert(job.clone());
                // DoneIsTerminal: a completed job is never redelivered — reclaim
                // can never resurrect it, so it is not activatable again.
                let re = engine.activate_jobs(job.clone(), "done-check", 1, timeout, 0);
                if !re.is_empty() {
                    return Err(ctx(format!(
                        "DoneIsTerminal violated: completed job {job} was re-activated"
                    )));
                }
                assert_at_most_one_live_holder(&history).map_err(ctx)?;
            }
            other => return Err(ctx(format!("unknown action {other:?}"))),
        }
    }
    Ok(())
}

/// Every committed JobLease behaviour is a two-sided anchor: it is an admitted
/// `JobLease.tla` behaviour (the spec-side validator) **and** it replays against
/// the engine with matching lease events and no invariant violation. Checking
/// both against the *same* fixture ties the engine to the spec through it, so
/// neither the fixture nor the engine can silently drift from `JobLease.tla`.
#[test]
fn job_lease_traces_conform_to_engine() {
    for (path, fixture) in load_lease_fixtures() {
        spec_model::assert_admitted(&fixture)
            .unwrap_or_else(|e| panic!("{path}: not a JobLease.tla-admitted behaviour: {e}"));
        replay_lease_behaviour(&fixture).unwrap_or_else(|e| panic!("{path}: {e}"));
    }
}

/// The spec-side validator must actually *reject* an inadmissible behaviour, or
/// the admissibility half of the anchor above would be vacuous. Tamper with the
/// `exclusive-lease` behaviour so it claims the second, concurrent activation is
/// leased — which `JobLease.tla`'s `~HasLiveLock` guard forbids — and assert the
/// validator reports the divergence (Red/Green: the validator is proven to fire).
#[test]
fn spec_model_detects_inadmissible_behaviour() {
    let (_, mut fixture) = load_lease_fixtures()
        .into_iter()
        .find(|(_, v)| v.get("model").and_then(Value::as_str) == Some("exclusive-lease"))
        .expect("the exclusive-lease behaviour is committed");
    fixture["steps"][1]["expect_leased"] = Value::Bool(true);
    let result = spec_model::assert_admitted(&fixture);
    assert!(
        result.is_err(),
        "the spec-side validator must reject a behaviour that violates the exclusive-lease guard"
    );
    assert!(
        result.unwrap_err().contains("Activate"),
        "the inadmissibility report should name the disabled Activate action"
    );
}

/// The spec-side validator must reject a `timeout` of 0: `JobLease.tla` assumes
/// `Timeout > 0`, so no JobLease model can instantiate it and a fixture using it
/// is not an admitted behaviour. Without this guard the two-sided anchor would
/// silently accept an un-modellable fixture (finding #1257). Red/Green: prove the
/// timeout-assumption check actually fires.
#[test]
fn spec_model_rejects_zero_timeout() {
    let (_, mut fixture) = load_lease_fixtures()
        .into_iter()
        .find(|(_, v)| v.get("model").and_then(Value::as_str) == Some("activate-complete"))
        .expect("the activate-complete behaviour is committed");
    fixture["timeout"] = Value::from(0u64);
    let result = spec_model::assert_admitted(&fixture);
    assert!(
        result.is_err(),
        "the spec-side validator must reject a timeout of 0 (JobLease.tla ASSUME Timeout > 0)"
    );
    assert!(
        result.unwrap_err().contains("timeout must be > 0"),
        "the report should name the violated Timeout assumption"
    );
}

/// The spec-side validator must reject a fixture whose `spec` is not `JobLease`:
/// the `trace_validation/joblease` directory is a spec-registration boundary, so
/// a JSON copied in with a different or unknown spec must not be silently
/// reported as JobLease conformance (finding #1257). Red/Green: prove the
/// spec-field check actually fires.
#[test]
fn spec_model_rejects_foreign_spec() {
    let (_, mut fixture) = load_lease_fixtures()
        .into_iter()
        .find(|(_, v)| v.get("model").and_then(Value::as_str) == Some("activate-complete"))
        .expect("the activate-complete behaviour is committed");
    fixture["spec"] = Value::from("SomeOtherSpec");
    let result = spec_model::assert_admitted(&fixture);
    assert!(
        result.is_err(),
        "the spec-side validator must reject a fixture whose spec is not JobLease"
    );
    assert!(
        result.unwrap_err().contains("is not \"JobLease\""),
        "the report should name the rejected foreign spec"
    );
}

/// The `AtMostOneLiveHolder` invariant helper must actually *fire* on a
/// two-holder history, or asserting it throughout replay would be vacuous — the
/// exact defect that deduplicating the job-keyed `activated_leases()` snapshot
/// had (finding #1257). Feed the helper a hand-built activation history and prove
/// it (a) rejects two grants of one job whose live windows overlap, (b) accepts
/// non-overlapping (sequential) re-leases, and (c) treats the windows as
/// half-open, so a re-lease at exactly the previous deadline is not an overlap.
#[test]
fn at_most_one_live_holder_detects_concurrent_holders() {
    // (a) Two workers hold job 1 over overlapping windows -> violation.
    let overlapping = vec![
        LiveWindow {
            key: 1,
            worker: "w-a".into(),
            grant: 0,
            deadline: 10,
        },
        LiveWindow {
            key: 1,
            worker: "w-b".into(),
            grant: 5,
            deadline: 15,
        },
    ];
    let err = assert_at_most_one_live_holder(&overlapping)
        .expect_err("overlapping live windows on one job must violate AtMostOneLiveHolder");
    assert!(
        err.contains("AtMostOneLiveHolder violated"),
        "the report should name the violated invariant, got: {err}"
    );

    // (b) Sequential re-lease of the same job (no overlap) is admissible, and
    // (c) a re-lease starting exactly at the prior deadline is not an overlap
    // (windows are half-open). A concurrent lease on a *different* job is fine.
    let sequential = vec![
        LiveWindow {
            key: 1,
            worker: "w-a".into(),
            grant: 0,
            deadline: 10,
        },
        LiveWindow {
            key: 1,
            worker: "w-b".into(),
            grant: 10,
            deadline: 20,
        },
        LiveWindow {
            key: 2,
            worker: "w-c".into(),
            grant: 5,
            deadline: 25,
        },
    ];
    assert!(
        assert_at_most_one_live_holder(&sequential).is_ok(),
        "sequential re-leases and cross-job concurrency must not trip the invariant"
    );
}

/// The replay must actually *catch* a divergence, or the conformance test above
/// would be vacuous. Tamper with the `exclusive-lease` behaviour so it expects
/// the second, concurrent activation to succeed, and assert the replay reports
/// the divergence (Red/Green: the detector is proven to fire).
#[test]
fn replay_detects_lease_divergence() {
    let (_, mut fixture) = load_lease_fixtures()
        .into_iter()
        .find(|(_, v)| v.get("model").and_then(Value::as_str) == Some("exclusive-lease"))
        .expect("the exclusive-lease behaviour is committed");
    // Flip the exclusivity expectation: claim the second worker CAN lease the
    // already-live job. The engine refuses, so the replay must diverge.
    fixture["steps"][1]["expect_leased"] = Value::Bool(true);
    let result = replay_lease_behaviour(&fixture);
    assert!(
        result.is_err(),
        "replay must fail when the engine diverges from the (tampered) behaviour"
    );
    assert!(
        result.unwrap_err().contains("to lease"),
        "the divergence report should name the exclusivity mismatch"
    );
}

/// The spec-side validator must reject a fixture whose deadline arithmetic
/// overflows u64: `JobLease.tla` evaluates `clock + Timeout` in unbounded `Nat`
/// arithmetic, so saturating (or wrapping) a u64 `now + timeout` near the top of
/// the domain would silently diverge from the spec at the boundary. Rather than
/// anchor the engine against a deadline it cannot faithfully represent, the
/// validator must refuse the fixture (finding #1257). Red/Green: prove the
/// overflow check actually fires.
#[test]
fn spec_model_rejects_deadline_overflow() {
    let (_, mut fixture) = load_lease_fixtures()
        .into_iter()
        .find(|(_, v)| v.get("model").and_then(Value::as_str) == Some("activate-complete"))
        .expect("the activate-complete behaviour is committed");
    // `now = u64::MAX` with a positive timeout makes `now + timeout` overflow at
    // the first (enabled) Activate step.
    fixture["steps"][0]["now"] = Value::from(u64::MAX);
    let result = spec_model::assert_admitted(&fixture);
    assert!(
        result.is_err(),
        "the spec-side validator must reject a fixture whose now + timeout overflows u64"
    );
    assert!(
        result.unwrap_err().contains("overflows u64"),
        "the report should name the deadline overflow"
    );
}
