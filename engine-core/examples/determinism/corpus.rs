//! The determinism-CI **command corpus generator** (ADR 0066 slice 6, #1665).
//!
//! Builds a representative, fully deterministic command corpus covering every
//! [`Command`] variant — deployments (process, DMN, forms, generic resources,
//! bundles), instance lifecycle, jobs (activate, complete, fail, error, retry,
//! timeout, **expiry** — soft and durable locks), user tasks, **timers**,
//! messages and signals (including the **cross-partition routed subscription
//! protocol** against a real harvested `Opening` subscription), conditional
//! starts, **event sub-processes**, **migration**, **modification**, ad-hoc
//! activities, agents, orphan quarantine (**local and replicated-stamped**),
//! **transcendental FEEL floats** (ADR 0066 audit #7), **cluster variables**
//! and the **config commands** — plus a seeded randomized workload (a
//! fixed-seed xorshift, never host entropy) over the same processes.
//!
//! Keys are harvested by applying each command to a live engine while the
//! corpus is built, so later commands address real instances, jobs and user
//! tasks. A command the engine rejects is still a valid corpus entry: its
//! typed rejection is part of the compared output.
//!
//! [`covered_kinds`] reports the [`Command::kind`] of every emitted entry;
//! `tests/determinism_corpus.rs` asserts it equals the full variant set parsed
//! from `Command::kind`'s compiler-exhaustive match, so a new variant cannot
//! silently escape the gate.
//!
//! Ordering note: the variants newest to the persisted log (cluster variables
//! and config commands, ADR 0066 slices 1–2) are emitted **last**, so an older
//! release — which stops at the first command it cannot decode — still
//! replays the longest possible compatible prefix.

use std::collections::{BTreeMap, BTreeSet};

use nanobpmn_engine_core::{
    Command, Engine, EventSubProcessTrigger, OrphanKind, ProcessBuilder, Value,
};
use serde_json::{json, Value as Json};

/// Epoch the corpus clock starts at (fixed: the clock is corpus data).
const T0: u64 = 1_700_000_000_000;

/// Cluster width the corpus engine is configured with. The engine mints every
/// key in partition 0's namespace (single-writer: the corpus IS the log), but
/// a `> 1` width makes `hash(correlation_key) % N != 0` keys route their
/// message subscriptions "off-partition", so the corpus exercises the
/// cross-partition subscription protocol (`MessageSubscriptionOpening` →
/// `OpenMessageSubscription` → `CorrelateMessageSubscription`) against real
/// harvested keys instead of only its hand-issued, no-op form.
const NUM_PARTITIONS: u64 = 3;

/// A built corpus: `(now, command)` entries in order.
pub type Corpus = Vec<(u64, Command)>;

/// A corpus under construction, applied to a live engine as it grows.
pub struct Gen {
    engine: Engine,
    now: u64,
    entries: Vec<(u64, Command)>,
    rng: u64,
    /// Corpus positions to leave out (a previous release cannot decode them).
    /// Excluded commands are neither recorded nor applied, so every LATER key
    /// is harvested from the engine the filtered corpus actually builds.
    exclude: BTreeSet<u64>,
    /// Position in the UNFILTERED corpus of the next push.
    position: u64,
    /// Per unfiltered position: `Some(accepted?)`, or `None` when excluded.
    outcomes: Vec<Option<bool>>,
    /// The scenario subsequent pushes belong to (see [`Gen::scenario`]).
    scenario: &'static str,
    /// Per recorded entry (parallel to `entries`): its scenario label.
    labels: Vec<&'static str>,
}

impl Default for Gen {
    fn default() -> Self {
        Self::new()
    }
}

impl Gen {
    pub fn new() -> Self {
        Self {
            engine: Engine::new(),
            now: T0,
            entries: Vec::new(),
            rng: 0x9E37_79B9_7F4A_7C15,
            exclude: BTreeSet::new(),
            position: 0,
            outcomes: Vec::new(),
            scenario: "setup",
            labels: Vec::new(),
        }
    }

    /// Labels every subsequent entry with `name`. The previous-release gate's
    /// behavioural-divergence allowlist (`divergence-allowlist.json`, #1698)
    /// ratifies divergences **per scenario**, so a ratified change in one
    /// scenario never masks an undeclared one elsewhere.
    fn scenario(&mut self, name: &'static str) {
        self.scenario = name;
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64: fixed seed, platform-independent.
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn tick(&mut self, millis: u64) {
        self.now += millis;
    }

    /// Records `command` at the current clock and returns its events as JSON.
    fn push(&mut self, command: Command) -> Vec<Json> {
        self.try_push(command).unwrap_or_default()
    }

    /// Records `command` at the current clock; `Err` carries its rejection.
    fn try_push(&mut self, command: Command) -> Result<Vec<Json>, String> {
        self.tick(1);
        let position = self.position;
        self.position += 1;
        if self.exclude.contains(&position) {
            self.outcomes.push(None);
            return Err(EXCLUDED.into());
        }
        self.entries.push((self.now, command.clone()));
        self.labels.push(self.scenario);
        let result = match self.engine.apply_command_at(command, self.now) {
            Ok(events) => Ok(events
                .iter()
                .map(|e| serde_json::to_value(e).unwrap_or(Json::Null))
                .collect()),
            Err(e) => Err(e.to_string()),
        };
        self.outcomes.push(Some(result.is_ok()));
        result
    }

    fn push_json(&mut self, command: Json) -> Vec<Json> {
        self.push(decode(command))
    }

    /// Like [`Self::push_json`], but the command is **meant to succeed**: a
    /// rejection panics while the corpus is built, so a success-path entry can
    /// never silently degrade into exercising only a rejection (e.g. a stale
    /// or wrong key harvested from an earlier entry).
    fn push_ok_json(&mut self, command: Json) -> Vec<Json> {
        let command = decode(command);
        let kind = command.kind();
        match self.try_push(command) {
            Ok(events) => events,
            // A filtered build is checked against the unfiltered one instead
            // (`build_excluding`): an outcome change is reported, not panicked.
            Err(_) if !self.exclude.is_empty() => Vec::new(),
            Err(e) => panic!("corpus success-path {kind} was rejected: {e}"),
        }
    }

    /// The finished corpus.
    pub fn into_entries(self) -> Vec<(u64, Command)> {
        self.entries
    }
}

fn decode(command: Json) -> Command {
    match serde_json::from_value::<Command>(command.clone()) {
        Ok(c) => c,
        Err(e) => panic!("corpus command does not decode: {e}: {command}"),
    }
}

/// The `element_instance_key` of the `ElementActivated` event for
/// `element_id` in `events`.
fn activated(events: &[Json], element_id: &str) -> Option<u64> {
    events
        .iter()
        .filter_map(|e| e.get("ElementActivated"))
        .find(|v| v.get("element_id").and_then(Json::as_str) == Some(element_id))
        .and_then(|v| v.get("element_instance_key").and_then(Json::as_u64))
}

/// The first `field` of an event of `variant` in `events`.
fn find(events: &[Json], variant: &str, field: &str) -> Option<u64> {
    events
        .iter()
        .filter_map(|e| e.get(variant))
        .find_map(|v| v.get(field).and_then(Json::as_u64))
}

fn all(events: &[Json], variant: &str, field: &str) -> Vec<u64> {
    events
        .iter()
        .filter_map(|e| e.get(variant))
        .filter_map(|v| v.get(field).and_then(Json::as_u64))
        .collect()
}

fn vars(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn create(process_id: &str, variables: BTreeMap<String, Value>) -> Command {
    Command::CreateInstance {
        process_id: process_id.into(),
        variables,
        tags: Vec::new(),
        business_id: None,
        process_definition_key: None,
        version: None,
    }
}

fn activate(job_type: &str, max_jobs: usize, now: u64) -> Command {
    Command::ActivateJobs {
        job_type: job_type.into(),
        worker: "det-worker".into(),
        max_jobs,
        timeout: 30_000,
        now,
        fetch_variables: Vec::new(),
        with_lease: false,
    }
}

const DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="det" name="det" namespace="http://camunda.org/schema/1.0/dmn">
  <decision id="tier" name="Tier">
    <decisionTable id="dt" hitPolicy="FIRST">
      <input id="i1" label="amount"><inputExpression id="ie1" typeRef="number"><text>amount</text></inputExpression></input>
      <output id="o1" name="tier" typeRef="string" />
      <rule id="r1"><inputEntry id="e1"><text>&gt; 100</text></inputEntry><outputEntry id="oe1"><text>"gold"</text></outputEntry></rule>
      <rule id="r2"><inputEntry id="e2"><text>-</text></inputEntry><outputEntry id="oe2"><text>"basic"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>"#;

#[allow(
    clippy::too_many_lines,
    reason = "one linear scenario script; splitting it would scatter the corpus"
)]
/// An external (job-backed) agent task and a job-worker ad-hoc container, in
/// TWO processes, both left running for the ad-hoc / agent commands. Not one
/// process behind a parallel fork: the corpus harvests the element-instance
/// keys the commands address from the head engine, and the order a fork mints
/// its branches' keys is release-specific (#1244 changed it), so a forked
/// model would address the WRONG branch on a previous release — a corpus
/// artefact, not an engine divergence (#1698).
const AGENTS_BPMN: &str = r#"
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="agents" isExecutable="true">
      <bpmn:startEvent id="s" />
      <bpmn:serviceTask id="agent">
        <bpmn:extensionElements>
          <zeebe:agentDefinition agentType="external" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="e1" />
      <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
      <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e1" />
    </bpmn:process>
    <bpmn:process id="adhocs" isExecutable="true">
      <bpmn:startEvent id="s" />
      <bpmn:adHocSubProcess id="adhoc">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="adhoc-worker" />
        </bpmn:extensionElements>
        <bpmn:serviceTask id="toolA">
          <bpmn:extensionElements>
            <zeebe:taskDefinition type="tool" />
          </bpmn:extensionElements>
        </bpmn:serviceTask>
      </bpmn:adHocSubProcess>
      <bpmn:endEvent id="e2" />
      <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="adhoc" />
      <bpmn:sequenceFlow id="f2" sourceRef="adhoc" targetRef="e2" />
    </bpmn:process>
  </bpmn:definitions>"#;

const EXCLUDED: &str = "excluded from this corpus";

/// The kept-coverage floor of a filtered (previous-release) corpus: at least
/// this share of the unfiltered entries must survive the exclusions.
pub const MIN_KEPT_PERCENT: usize = 90;

/// Builds the full corpus. `random_instances` sizes the randomized workload.
pub fn build(random_instances: usize) -> Vec<(u64, Command)> {
    generate(random_instances, BTreeSet::new()).0
}

/// [`build`], plus each entry's scenario label (parallel to the entries).
pub fn build_labeled(random_instances: usize) -> (Corpus, Vec<&'static str>) {
    let (entries, _, labels) = generate(random_instances, BTreeSet::new());
    (entries, labels)
}

/// Builds the corpus WITHOUT the commands at the unfiltered positions in
/// `exclude` (those a previous release cannot decode). Keys are harvested
/// from the engine the filtered corpus builds, so later commands still
/// address real entities. A kept command whose outcome (accepted vs
/// rejected) would change depends on an excluded one, so it is excluded too
/// (returned in the set) rather than silently compared as a degraded
/// execution both builds agree on; fails if less than [`MIN_KEPT_PERCENT`]
/// percent of the corpus survives. Also returns each kept entry's scenario label.
pub fn build_excluding(
    random_instances: usize,
    exclude: BTreeSet<u64>,
) -> Result<(Corpus, Vec<&'static str>, BTreeSet<u64>), String> {
    let (_, full, _) = generate(random_instances, BTreeSet::new());
    let mut excluded = exclude;
    // Fixpoint: a kept command whose outcome flips depends on an excluded one
    // (e.g. a create of a process whose deployment the old release cannot
    // decode) — that scenario is unreachable for the old release, so it is
    // excluded too, and the corpus regenerated, until nothing flips.
    for _ in 0..64 {
        let (entries, filtered, labels) = generate(random_instances, excluded.clone());
        if full.len() != filtered.len() {
            return Err(format!(
                "excluding entries changed the corpus shape: {} vs {} positions",
                full.len(),
                filtered.len()
            ));
        }
        let flipped: Vec<u64> = full
            .iter()
            .zip(&filtered)
            .enumerate()
            .filter(|(_, (f, k))| matches!((f, k), (Some(f), Some(k)) if f != k))
            .map(|(p, _)| p as u64)
            .collect();
        if flipped.is_empty() {
            // Bound the silent loss: the compatible corpus must keep the bulk
            // of the scenarios, or the cross-release gate proves little.
            if entries.len() * 100 < full.len() * MIN_KEPT_PERCENT {
                return Err(format!(
                    "only {} of {} corpus entries survive the exclusions ({:?}); \
                     isolate newer variants so they do not take core scenarios with them",
                    entries.len(),
                    full.len(),
                    excluded
                ));
            }
            return Ok((entries, labels, excluded));
        }
        excluded.extend(flipped);
    }
    Err("exclusions did not reach a fixpoint".into())
}

/// A generated corpus: its entries, each entry's head outcome (`None` for an
/// excluded position), and each entry's scenario label.
type Generated = (Vec<(u64, Command)>, Vec<Option<bool>>, Vec<&'static str>);

fn generate(random_instances: usize, exclude: BTreeSet<u64>) -> Generated {
    let mut g = Gen::new();
    g.exclude = exclude;

    // ---- Deployments -------------------------------------------------------
    g.scenario("deployments");
    let order = ProcessBuilder::new("order")
        .start_event("s")
        .service_task("pay", "payment")
        .exclusive_gateway("gw")
        .user_task("review")
        .script_task("calc", "=amount * 2", "doubled")
        .end_event("e")
        .connect("s", "pay")
        .connect("pay", "gw")
        .connect_when("gw", "review", "=amount > 100")
        .connect_default("gw", "calc")
        .connect("review", "calc")
        .connect("calc", "e")
        .build()
        .expect("order");
    g.push(Command::DeployProcess(order.clone()));

    // Transcendental floats (ADR 0066 audit #7): `log`, `exp` and `**` are the
    // platform-libm-sensitive surface, routed through the pinned pure-Rust
    // `libm` so every platform/arch cell must agree on the persisted bits.
    // The script results persist into instance variables, so they reach the
    // compared records AND the durable-state checkpoints.
    let floats = ProcessBuilder::new("floats")
        .start_event("s")
        .script_task("calc", "=\"ln:\" + string(log(base)) + \",exp:\" + string(exp(base)) + \",pow:\" + string(base ** 0.5)", "transcendental")
        .end_event("e")
        .connect("s", "calc")
        .connect("calc", "e")
        .build()
        .expect("floats");
    g.push(Command::DeployProcess(floats));

    let timers = ProcessBuilder::new("timers")
        .start_event("s")
        .service_task("slow", "slow-work")
        .timer_boundary_event("tb", "slow", 5_000)
        .timer_intermediate_catch_event("wait", 2_000)
        .end_event("e")
        .end_event("timed-out")
        .connect("s", "wait")
        .connect("wait", "slow")
        .connect("slow", "e")
        .connect("tb", "timed-out")
        .build()
        .expect("timers");
    let messaging = ProcessBuilder::new("messaging")
        .start_event("s")
        .parallel_gateway("fork")
        .message_intermediate_catch_event("m", "paid", "=orderId")
        .signal_intermediate_catch_event("sig", "go")
        .parallel_gateway("join")
        .service_task("ship", "shipping")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "m")
        .connect("fork", "sig")
        .connect("m", "join")
        .connect("sig", "join")
        .connect("join", "ship")
        .connect("ship", "e")
        .build()
        .expect("messaging");
    let starters = ProcessBuilder::new("starters")
        .message_start_event("ms", "kickoff")
        .conditional_start_event("cs", "=go = true")
        .service_task("w", "starter-work")
        .end_event("e")
        .connect("ms", "w")
        .connect("cs", "w")
        .connect("w", "e")
        .build()
        .expect("starters");
    // Newer model shapes (conditional start, event sub-process) each get their
    // OWN entry, apart from the core processes: a previous release that cannot
    // decode them excludes only that deployment (and its dependents), never a
    // core process the rest of the corpus runs on.
    let escalation = ProcessBuilder::new("escalation")
        .start_event("s")
        .service_task("hold", "hold-work")
        .end_event("e")
        .connect("s", "hold")
        .connect("hold", "e")
        .event_sub_process(
            "esp",
            "esp-s",
            EventSubProcessTrigger::Message {
                message_name: "cancel-request".into(),
                correlation_key: "orderId".into(),
            },
            false,
        )
        .start_event("esp-s")
        .end_event("esp-e")
        .contained_in("esp-s", "esp")
        .contained_in("esp-e", "esp")
        .connect("esp-s", "esp-e")
        .build()
        .expect("escalation");
    g.push(Command::DeployResources(vec![timers, messaging]));
    g.push(Command::DeployProcess(starters));
    g.push(Command::DeployProcess(escalation));

    let order_v2 = ProcessBuilder::new("order")
        .start_event("s")
        .service_task("pay", "payment")
        .service_task("audit", "audit")
        .end_event("e")
        .connect("s", "pay")
        .connect("pay", "audit")
        .connect("audit", "e")
        .build()
        .expect("order v2");

    let drg = nanobpmn_engine_core::dmn::parse_dmn(DMN).expect("dmn");
    g.push(Command::DeployDecisionRequirements(vec![drg.clone()]));
    g.push_json(json!({"DeployForms": [{
        "id": "det-form", "resource_name": "det.form",
        "schema": "{\"id\":\"det-form\",\"components\":[]}"
    }]}));
    g.push_json(json!({"DeployGenericResources": [{
        "resource_id": "det-rpa", "resource_name": "det.rpa", "content": "{}"
    }]}));
    let bundled = ProcessBuilder::new("bundled")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .build()
        .expect("bundled");
    g.push_json(json!({"DeployBundle": {
        "processes": [serde_json::to_value(&bundled).expect("bundled")],
        "decisions": [], "forms": [], "resources": []
    }}));

    // ---- Decisions ---------------------------------------------------------
    g.scenario("decisions");
    let eval = g.push_json(json!({"EvaluateDecision": {
        "decision": {"Id": "tier"},
        "variables": {"amount": {"Int": 250}}
    }}));
    let eval_key = eval
        .iter()
        .find_map(|e| e.as_object().and_then(|o| o.values().next().cloned()))
        .and_then(|v| v.get("decision_evaluation_key").and_then(Json::as_u64))
        .unwrap_or(0);
    g.push(Command::DeleteDecisionInstance {
        instance_key: eval_key,
        decision_evaluation_key: eval_key,
    });

    // ---- Jobs and user tasks ----------------------------------------------
    g.scenario("jobs-and-user-tasks");
    // Transcendental-float instances (ADR 0066 audit #7): the results persist
    // into instance variables, so the platform matrix compares their bits.
    let fl = g.push(create("floats", vars(&[("base", Value::Int(2))])));
    let _fl_instance = find(&fl, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let fl2 = g.push(create("floats", vars(&[("base", Value::Double(0.1))])));
    let fl2_instance = find(&fl2, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let big = g.push(create("order", vars(&[("amount", Value::Int(500))])));
    let big_instance = find(&big, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let small = g.push(create("order", vars(&[("amount", Value::Int(5))])));
    let _ = small;
    let failing = g.push(create("order", vars(&[("amount", Value::Int(7))])));
    let failing_instance = find(&failing, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let _ = failing_instance;

    let now = g.now;
    let act = g.push(activate("payment", 10, now));
    let jobs = all(&act, "JobActivated", "job_key");
    let jobs = if jobs.is_empty() {
        all(&act, "JobsActivated", "job_key")
    } else {
        jobs
    };
    let job_keys: Vec<u64> = {
        // Fall back to the creation events when activation shapes differ.
        let mut k = jobs;
        if k.is_empty() {
            k.extend(all(&big, "JobCreated", "job_key"));
            k.extend(all(&small, "JobCreated", "job_key"));
            k.extend(all(&failing, "JobCreated", "job_key"));
        }
        k
    };
    let j = |i: usize| job_keys.get(i).copied().unwrap_or(0);

    let done = g.push(Command::complete_job_with(
        j(0),
        vars(&[("paid", Value::Bool(true))]),
    ));
    let done2 = g.push(Command::complete_job(j(1)));
    let user_task = find(&done, "UserTaskCreated", "user_task_key")
        .or_else(|| find(&done2, "UserTaskCreated", "user_task_key"))
        .unwrap_or(0);
    g.push(Command::update_job_retries(j(2), 3));
    g.push(Command::update_job_timeout(j(2), 60_000));
    g.push_json(json!({"UpdateJob": {"job_key": j(2), "retries": 2, "timeout": 45_000}}));
    let failed = g.push(Command::fail_job(j(2), 0, "boom"));
    let incident = find(&failed, "IncidentRaised", "incident_key").unwrap_or(0);
    g.push(Command::update_job_retries(j(2), 1));
    g.push(Command::resolve_incident(incident));

    g.push(Command::assign_user_task(user_task, "alice"));
    g.push(Command::unassign_user_task(user_task));
    g.push_json(
        json!({"UpdateUserTask": {"user_task_key": user_task, "changeset": {
            "candidate_groups": ["ops"], "priority": 80
        }}}),
    );
    g.push(Command::set_variables(
        big_instance,
        vars(&[("note", Value::Str("reviewed".into()))]),
    ));
    g.push(Command::complete_user_task_with(
        user_task,
        vars(&[("approved", Value::Bool(true))]),
    ));

    // A job thrown as a BPMN error, then a job activated by key.
    let e1 = g.push(create("order", vars(&[("amount", Value::Int(1))])));
    let e1_job = find(&e1, "JobCreated", "job_key").unwrap_or(0);
    let now = g.now;
    g.push_json(json!({"ActivateJobsByKey": {
        "job_keys": [e1_job], "worker": "det-worker", "timeout": 1_000, "now": now
    }}));
    g.push(Command::throw_job_error(e1_job, "E42", "business error"));

    // ---- Timers and job expiry --------------------------------------------
    g.scenario("timers-and-job-expiry");
    for _ in 0..3 {
        g.push(create("timers", BTreeMap::new()));
    }
    g.tick(2_500);
    let now = g.now;
    g.push(Command::TriggerTimers { now });
    let now = g.now;
    g.push(activate("slow-work", 3, now));
    // Selective expiry must have locks of BOTH durabilities live at once, or
    // it degenerates into a no-op behind the unfiltered `ExpireJobs`. The
    // `activate` above took soft locks (`with_lease: false`); take DURABLE
    // locks (a lease token) on two fresh `slow-work` jobs by key, so
    // `ExpireJobsByDurability { durable: true }` releases exactly them and
    // `durable: false` exactly the soft ones — each emitting `JobLockExpired`.
    // The fresh instances park on the `wait` timer first, so their `slow`
    // jobs exist only after a timer tick — harvest them from the TICK's
    // events, not the creates' (the earlier instances' jobs were created by
    // the first tick, so this tick's `JobCreated`s are exactly the two fresh
    // ones).
    let _fresh = g.push(create("timers", BTreeMap::new()));
    let _fresh2 = g.push(create("timers", BTreeMap::new()));
    g.tick(2_500);
    let now = g.now;
    let ticked = g.push(Command::TriggerTimers { now });
    let durable_jobs: Vec<u64> = all(&ticked, "JobCreated", "job_key");
    let now = g.now;
    g.push_json(json!({"ActivateJobsByKey": {
        "job_keys": durable_jobs, "worker": "det-worker", "timeout": 30_000,
        "now": now, "with_lease": true
    }}));
    g.tick(31_000);
    let now = g.now;
    let expired_durable = g.push(Command::ExpireJobsByDurability { now, durable: true });
    let now = g.now;
    let expired_soft = g.push(Command::ExpireJobsByDurability {
        now,
        durable: false,
    });
    let now = g.now;
    g.push(Command::ExpireJobs { now });
    if g.exclude.is_empty() {
        // The selective expiries must each release exactly their own domain —
        // an empty batch here is the silent no-op this scenario exists to
        // prevent (a later engine change that stops `ActivateJobsByKey` from
        // taking durable locks must fail the corpus build, not pass it
        // degraded).
        assert_eq!(
            all(&expired_durable, "JobLockExpired", "job_key"),
            {
                let mut d = durable_jobs.clone();
                d.sort_unstable();
                d
            },
            "durable selective expiry released the wrong locks"
        );
        assert!(
            all(&expired_soft, "JobLockExpired", "job_key").len() >= 3,
            "soft selective expiry released nothing (the soft activations were due)"
        );
    }
    g.tick(6_000);
    let now = g.now;
    g.push(Command::TriggerTimers { now });

    // ---- Messages, signals, event sub-processes, conditional starts -------
    g.scenario("messages-signals-conditional");
    let m1 = g.push(create(
        "messaging",
        vars(&[("orderId", Value::Str("o-1".into()))]),
    ));
    let m1_instance = find(&m1, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    g.push(create(
        "escalation",
        vars(&[("orderId", Value::Str("o-1".into()))]),
    ));
    g.push_json(json!({"CorrelateMessage": {
        "message_name": "cancel-request", "correlation_key": "o-1", "variables": {}
    }}));
    g.push_json(json!({"CorrelateMessage": {
        "message_name": "paid", "correlation_key": "o-1",
        "variables": {"receipt": {"Str": "r-1"}}
    }}));
    g.push_json(json!({"BroadcastSignal": {"signal_name": "go", "variables": {}}}));
    g.push_json(json!({"CorrelateMessage": {
        "message_name": "kickoff", "correlation_key": "", "variables": {}
    }}));
    g.push_json(json!({"EvaluateConditionalStartEvents": {
        "variables": {"go": {"Bool": true}}
    }}));
    // Partition-routed subscription/start commands (the cross-partition
    // protocol). The MAIN corpus engine stays single-partition (so the bulk
    // of the corpus keeps exercising the local paths); this self-contained
    // segment stands up a two-partition pair — an instance partition and a
    // message partition — and plays the host's routing between them. The
    // ROUTED commands (`OpenMessageSubscription`, `CorrelateMessageSubscription`,
    // `CloseMessageSubscription`, `DispatchStartInstance`) are recorded as
    // corpus entries, applied to the partition that owns them, so the
    // compared records cover both partitions' halves of the protocol against
    // real harvested keys. The intra-partition triggers (deploy, create,
    // publish) are NOT corpus entries: they belong to the stand-in engines'
    // own logs, and recording them would make the runner re-apply them to the
    // wrong (single-partition) engine.
    let routed = routed_protocol_entries();
    for command in routed {
        g.push(command);
    }
    g.push_json(json!({"DispatchStartInstance": {
        "process_id": "starters", "start_element_id": "ms", "variables": {},
        "tags": [], "business_id": null
    }}));

    // ---- Scalar-list routing (#1755) ---------------------------------------
    // `list contains` coerces a non-null scalar to a singleton list (DMN: a
    // scalar in a list context), so a gateway condition on a scalar route head
    // ROUTES instead of raising an incident. The previous release rejects the
    // scalar with an evaluation error, so the same command journals different
    // events across the two builds — a ratified upgrade-boundary divergence
    // (`divergence-allowlist.json`), never a rolling upgrade.
    g.scenario("scalar-list-routing");
    let scalar_route = ProcessBuilder::new("scalar-route")
        .start_event("s")
        .parallel_gateway("fork")
        .exclusive_gateway("merge")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "scalar-m1", "=key")
        .message_intermediate_catch_event("onM2", "scalar-m2", "=key")
        .inclusive_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "merge")
        .connect("fork", "merge")
        .connect("merge", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "join")
        .connect("onM2", "join")
        // The corpus shape (#1755): the condition reads the route schedule's
        // head `route[1]` — the activation's single-entry choice, a bare
        // scalar once dereferenced.
        .connect_when("join", "e", "=list contains(route[1], \"f8\")")
        .connect_when("join", "e", "=list contains(route[1], \"f9\")")
        .build()
        .expect("scalar-route");
    g.push(Command::DeployProcess(scalar_route));
    let sr = g.push(create(
        "scalar-route",
        vars(&[
            ("key", Value::Str("k".into())),
            ("route", Value::List(vec![Value::Str("f8".into())])),
        ]),
    ));
    let sr_instance = find(&sr, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    g.push(Command::correlate_message("scalar-m1", "k"));
    g.push(Command::correlate_message("scalar-m2", "k"));
    if g.exclude.is_empty() {
        assert!(
            g.engine.is_completed(sr_instance),
            "the scalar head choice routes the fired inclusive join"
        );
    }

    // ---- Suspend / resume / cancel ----------------------------------------
    g.scenario("suspend-resume-cancel");
    g.push(Command::SuspendInstance {
        instance_key: m1_instance,
    });
    g.push(Command::ResumeInstance {
        instance_key: m1_instance,
    });
    let to_cancel = g.push(create(
        "messaging",
        vars(&[("orderId", Value::Str("o-2".into()))]),
    ));
    let to_cancel_instance =
        find(&to_cancel, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    g.push(Command::cancel_instance(to_cancel_instance));

    // ---- Migration and modification ---------------------------------------
    g.scenario("migration-and-modification");
    let mig = g.push(create("order", vars(&[("amount", Value::Int(3))])));
    let mig_instance = find(&mig, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let v2 = g.push(Command::DeployProcess(order_v2));
    let order_v2_key = find(&v2, "ProcessDeployed", "process_definition_key");
    g.push_json(json!({"MigrateInstance": {
        "instance_key": mig_instance,
        "target_process_definition_key": order_v2_key.unwrap_or(0),
        "mapping_instructions": [["pay", "pay"]]
    }}));
    let modi = g.push(create("order", vars(&[("amount", Value::Int(4))])));
    let modi_instance = find(&modi, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    g.push_json(json!({"ModifyInstance": {
        "instance_key": modi_instance,
        "activate_instructions": [{"element_id": "audit", "variables": {}}],
        "terminate_instructions": [],
        "terminate_element_ids": ["pay"]
    }}));

    // ---- Ad-hoc, agents, orphan quarantine --------------------------------
    g.scenario("adhoc-agents-quarantine");
    // A running ad-hoc container and an active external agent task, so both
    // command families exercise their SUCCESS paths (push_ok_json), not only
    // rejections against an unrelated key.
    for model in nanobpmn_engine_core::bpmn::parse_bpmn(AGENTS_BPMN).expect("agents model") {
        g.push(Command::DeployProcess(model));
    }
    let ag = g.push(create("agents", BTreeMap::new()));
    let ah = g.push(create("adhocs", BTreeMap::new()));
    // `unwrap_or(0)`: unfiltered, a 0 key makes push_ok_json panic below.
    let ag_instance = find(&ag, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let container = activated(&ah, "adhoc").unwrap_or(0);
    let agent_ei = activated(&ag, "agent").unwrap_or(0);
    g.push_ok_json(json!({"ActivateAdHocActivities": {
        "ad_hoc_instance_key": container,
        "activate_elements": [{"element_id": "toolA"}],
        "cancel_remaining": false
    }}));
    let agent = g.push_ok_json(json!({"CreateAgentInstance": {
        "element_instance_key": agent_ei, "definition": {"model": "det-model"}
    }}));
    let agent_key = agent
        .iter()
        .find_map(|e| e.get("AgentInstanceCreated"))
        .and_then(|v| v.get("agent_instance"))
        .and_then(|v| v.get("agent_instance_key"))
        .and_then(Json::as_u64)
        .unwrap_or(0);
    g.push_ok_json(json!({"UpdateAgentInstance": {
        "agent_instance_key": agent_key, "element_instance_key": agent_ei,
        "element_id": "agent", "process_instance_key": ag_instance,
        "status": "Thinking"
    }}));
    g.push_ok_json(json!({"CompleteAgentInstance": {"agent_instance_key": agent_key}}));
    // Orphan quarantine, BOTH forms, against LIVE harvested instances (a
    // never-created key makes the local form a no-op, and the replicated form
    // would go entirely unexercised). The agent instance is still running
    // (its scope is open until the ad-hoc container finishes), so the local
    // form (`stamp: None`) raises the resident-orphan `VariablesLost`
    // incident. The replicated form (`stamp: Some`, #1447) is stamped the way
    // a leader stamps it — `orphan_incident_stamp` against a second live
    // instance parked on a job — so its episode matches the allocator and it
    // emits its `IncidentRaised` plus the `VariablesLostQuarantined` dedup
    // tombstone.
    g.push(Command::QuarantineOffheapOrphan {
        instance_key: ag_instance,
        stamp: None,
    });
    g.push(Command::QuarantineColdOrphan {
        instance_key: 9_100_001,
    });
    let held = g.push(create(
        "escalation",
        vars(&[("orderId", Value::Str("o-q".into()))]),
    ));
    let held_instance = find(&held, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let stamp = g
        .engine
        .orphan_incident_stamp(held_instance, OrphanKind::Resident)
        .expect("a live instance stamps a quarantine report");
    g.push(Command::QuarantineOffheapOrphan {
        instance_key: held_instance,
        stamp: Some(stamp),
    });
    g.push(Command::QuarantineColdOrphan {
        instance_key: fl2_instance,
    });

    // ---- Seeded randomized workload ---------------------------------------
    g.scenario("random-workload");
    for n in 0..random_instances {
        match g.next_rand() % 4 {
            0 | 1 => {
                let amount = (g.next_rand() % 300) as i64;
                g.push(create(
                    "order",
                    vars(&[
                        ("amount", Value::Int(amount)),
                        ("seq", Value::Int(n as i64)),
                    ]),
                ));
            }
            2 => {
                g.push(create(
                    "messaging",
                    vars(&[
                        ("orderId", Value::Str(format!("r-{n}"))),
                        ("seq", Value::Int(n as i64)),
                    ]),
                ));
            }
            _ => {
                let id = g.next_rand() % (n as u64 + 1);
                g.push_json(json!({"CorrelateMessage": {
                    "message_name": "paid", "correlation_key": format!("r-{id}"),
                    "variables": {}
                }}));
            }
        }
        if n % 8 == 7 {
            let now = g.now;
            let act = g.push(activate("payment", 4, now));
            for key in all(&act, "JobActivated", "job_key") {
                if g.next_rand().is_multiple_of(3) {
                    g.push(Command::fail_job(key, 1, "random failure"));
                } else {
                    g.push(Command::complete_job(key));
                }
            }
            let pause = g.next_rand() % 10_000;
            g.tick(pause);
            let now = g.now;
            g.push(Command::TriggerTimers { now });
        }
    }

    // ---- Multi-instance cancel-remaining (#1528 review) -------------------
    // A satisfied completion condition cancels the loop's still-running
    // children. They now TERMINATE (ElementTerminating/ElementTerminated,
    // depth-first: a sub-process child's TERMINATING precedes its inner
    // teardown) where the previous release COMPLETED them — an
    // upgrade-boundary behavioural change ratified in
    // `divergence-allowlist.json`. Both a leaf (service task) child and an
    // embedded sub-process child are driven. Placed BEFORE the #1528 user-task
    // scenario, whose extra per-child keys would otherwise offset the job keys
    // harvested here, and with a FIXED command count (`build_excluding`).
    g.scenario("multi-instance-cancel-remaining");
    let cancel_mi = || nanobpmn_engine_core::MultiInstance {
        input_collection: "=items".to_string(),
        input_element: Some("item".to_string()),
        output_collection: None,
        output_element: None,
        completion_condition: Some("=true".to_string()),
        cancel_remaining_instances: true,
        sequential: false,
    };
    let mi_leaf = ProcessBuilder::new("mi_cancel_leaf")
        .start_event("start")
        .service_task("each", "mi-cancel-leaf")
        .with_multi_instance("each", cancel_mi())
        .end_event("end")
        .connect("start", "each")
        .connect("each", "end")
        .build()
        .expect("mi_cancel_leaf");
    let mi_sub = ProcessBuilder::new("mi_cancel_sub")
        .start_event("start")
        .sub_process("each", "sub_start")
        .with_multi_instance("each", cancel_mi())
        .start_event("sub_start")
        .contained_in("sub_start", "each")
        .service_task("inner", "mi-cancel-inner")
        .contained_in("inner", "each")
        .end_event("sub_end")
        .contained_in("sub_end", "each")
        .end_event("end")
        .connect("start", "each")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("each", "end")
        .build()
        .expect("mi_cancel_sub");
    for (model, job_type) in [(mi_leaf, "mi-cancel-leaf"), (mi_sub, "mi-cancel-inner")] {
        let process_id = model.id.clone();
        g.push(Command::DeployProcess(model));
        g.push(create(
            &process_id,
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ));
        let now = g.now;
        let act = g.push(activate(job_type, 10, now));
        // Completing ONE child satisfies the condition and cancels the rest.
        // Complete the LAST-minted job: it is a job key under both the
        // current lazily-chained parallel scheduler (#1539) and the previous
        // release's eager one, so the previous-release gate compares the same
        // accepted command (the minimum key is a job only under the old order).
        let last = all(&act, "JobActivated", "job_key")
            .into_iter()
            .max()
            .unwrap_or(0);
        g.push(Command::complete_job(last));
    }
    // ---- Multi-instance user tasks (#1528) --------------------------------
    // A multi-instance on a DIRECT user task creates one user task per child
    // (the previous release completed each child at once — an upgrade-boundary
    // behavioural change, ratified in `divergence-allowlist.json`). The
    // scenario emits a FIXED number of commands so the filtered
    // (previous-release) corpus keeps the unfiltered shape (`build_excluding`
    // rejects a shape change): the completion below targets a never-created
    // task key so BOTH builds reject it (`err` == `err`, a valid compared
    // entry), keeping the behavioural divergence entirely on the CREATE entry.
    g.scenario("multi-instance-user-tasks");
    let mi_review = ProcessBuilder::new("mi_review")
        .start_event("start")
        .user_task("review")
        .with_multi_instance(
            "review",
            nanobpmn_engine_core::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                // No output aggregation: an `output_element` seeds a
                // child-local `decision = null` on the head build that the
                // previous release never seeds, a `local_variables` payload
                // difference no event-pattern normalization reconciles (and
                // the field is not golden-recorded as added, so it is not
                // erased). The divergence this scenario exists to ratify is
                // the per-child user task, not output collection.
                output_collection: None,
                output_element: None,
                completion_condition: None,
                // `true` (the BPMN default): with no completion condition the
                // flag never fires, and the default keeps the deploy's
                // `cancel_remaining_instances` field at the value the previous
                // release's lossy decode treats as benign — so the scenario's
                // deployment is KEPT in the filtered corpus and the divergence
                // is actually exercised, not excluded as a dependent.
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .expect("mi_review");
    g.push(Command::DeployProcess(mi_review));
    let mi = g.push(create(
        "mi_review",
        vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
    ));
    let mi_tasks = all(&mi, "UserTaskCreated", "user_task_key");
    if g.exclude.is_empty() {
        assert_eq!(
            mi_tasks.len(),
            2,
            "one user task per MI child (the head behaviour #1528 ratifies)"
        );
    }
    // Complete a NEVER-CREATED task key: BOTH builds reject it (`err` ==
    // `err`, a valid compared entry), so the scenario keeps a
    // release-independent shape (`build_excluding` rejects a shape change)
    // while the behavioural divergence stays entirely on the CREATE entry —
    // head's per-child `UserTaskCreated` events against prev's immediate
    // child completions, which the allowlist's unbalanced declaration
    // ratifies. Completing a real harvested task would emit a
    // `UserTaskCompleted` lifecycle on head that prev (which minted no task)
    // rejects — a per-entry ok/err mismatch no event-pattern normalization
    // reconciles.
    let never_created = mi_tasks.iter().max().copied().unwrap_or(0) + 100_000;
    g.push(Command::complete_user_task_with(
        never_created,
        vars(&[("decision", Value::Int(10))]),
    ));

    // ---- Newest log variants last (see module doc) ------------------------
    g.scenario("newest-variants");
    g.push_json(json!({"CreateClusterVariable": {
        "tenant_id": null, "name": "region", "value": {"Str": "eu"},
        "kind": "JSON", "metadata": {}, "distributed": false
    }}));
    g.push_json(json!({"UpdateClusterVariable": {
        "tenant_id": null, "name": "region", "value": {"Str": "us"}, "metadata": {}
    }}));
    g.push_json(json!({"DeleteClusterVariable": {
        "tenant_id": null, "name": "region", "distributed": false
    }}));
    g.push(Command::SetLenientCompletion { lenient: true });
    g.push(create("order", vars(&[("amount", Value::Int(42))])));

    // ---- Suspend then cancel (#1863) ---------------------------------------
    // A SUSPENDED root: the legacy cancel (the gate-closed, mixed-version
    // behaviour) is rejected and leaves it SUSPENDED; the capability-gated
    // `terminate_suspended` cancel (every member advertises
    // `cancel-suspended-instance-v1`) terminates it.
    g.scenario("suspend-then-cancel");
    let sc = g.push(create(
        "messaging",
        vars(&[("orderId", Value::Str("o-sc".into()))]),
    ));
    let sc_instance = find(&sc, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    g.push(Command::suspend_instance(sc_instance));
    g.push(Command::cancel_instance(sc_instance));
    g.push(Command::cancel_instance_including_suspended(sc_instance));

    let outcomes = std::mem::take(&mut g.outcomes);
    let labels = std::mem::take(&mut g.labels);
    (g.into_entries(), outcomes, labels)
}

/// Plays the cross-partition message-subscription protocol across a real
/// two-partition engine pair and returns the ROUTED commands, in order, for
/// the corpus to record. Self-contained: the pair deploys its own process and
/// parks its own instance, so no key collides with the main corpus engine's
/// namespace (partition-encoded), and the segment cannot regress the main
/// corpus's local-path coverage by widening the main engine's cluster.
///
/// The protocol, exactly as a host drives it (#1497):
/// 1. instance partition (0): deploy + create parks the token on an
///    `Opening` record (the correlation key hashes to partition 1);
/// 2. host routes `OpenMessageSubscription` to the message partition (1),
///    which opens the canonical record — RETURNED (corpus entry);
/// 3. message partition publishes the message, settling the canonical record
///    and emitting a `RemoteMessageCorrelation`;
/// 4. host routes the `CorrelateMessageSubscription` continuation back to the
///    instance partition, advancing the parked token — RETURNED;
/// 5. host routes `CloseMessageSubscription` to the message partition —
///    RETURNED (a no-op there: the canonical record already settled, which is
///    the idempotent-close path).
fn routed_protocol_entries() -> Vec<Command> {
    let process = ProcessBuilder::new("routed")
        .start_event("s")
        .message_intermediate_catch_event("m", "paid", "=orderId")
        .service_task("ship", "shipping")
        .end_event("e")
        .connect("s", "m")
        .connect("m", "ship")
        .connect("ship", "e")
        .build()
        .expect("routed");
    let mut instance_partition = Engine::with_partition(0);
    instance_partition.set_num_partitions(NUM_PARTITIONS);
    let mut message_partition = Engine::with_partition(1);
    message_partition.set_num_partitions(NUM_PARTITIONS);
    instance_partition
        .apply_command_at(Command::DeployProcess(process), T0)
        .expect("deploy on the instance partition");

    // A correlation key that hashes off partition 0, so the subscription's
    // canonical record lives on partition 1. Derived, not hard-coded, so a
    // stable-hash change cannot silently re-local it.
    let order_id = (0u64..)
        .map(|n| format!("routed-{n}"))
        .find(|k| nanobpmn_engine_core::subscription_partition(k, NUM_PARTITIONS) == 1)
        .expect("some key hashes to partition 1");
    let created = instance_partition
        .apply_command_at(
            create("routed", vars(&[("orderId", Value::Str(order_id.clone()))])),
            T0 + 1,
        )
        .expect("create on the instance partition");
    let opening = created
        .iter()
        .find_map(|e| match e {
            nanobpmn_engine_core::Event::MessageSubscriptionOpening {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
                delivery_key,
            } => Some(Command::OpenMessageSubscription {
                subscription_key: *subscription_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                message_name: message_name.clone(),
                correlation_key: correlation_key.clone(),
                kind: kind.clone(),
                delivery_key: *delivery_key,
            }),
            _ => None,
        })
        .expect("an off-partition catch emits a routable Opening");

    // (2): the routed open lands on the message partition.
    message_partition
        .apply_command_at(opening.clone(), T0 + 2)
        .expect("the routed open applies on the message partition");

    // (3): publishing on the message partition routes a continuation back.
    let published = message_partition
        .apply_command_at(
            Command::correlate_message_with("paid", order_id, BTreeMap::new()),
            T0 + 3,
        )
        .expect("publish on the message partition");
    let continuation = published
        .iter()
        .find_map(|e| match e {
            nanobpmn_engine_core::Event::RemoteMessageCorrelation {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id,
                kind,
                variables,
                delivery_key,
                ack_partition,
            } => Some(Command::CorrelateMessageSubscription {
                subscription_key: *subscription_key,
                message_key: *message_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                kind: kind.clone(),
                variables: variables.clone(),
                delivery_key: *delivery_key,
                ack_partition: *ack_partition,
            }),
            _ => None,
        })
        .expect("publishing on the message partition routes a continuation back");

    // (4): the continuation lands on the instance partition and advances the
    // token. Assert it here (build-time), so a regression in the routed
    // continuation fails the corpus build, not only the digest comparison.
    let advanced = instance_partition
        .apply_command_at(continuation.clone(), T0 + 4)
        .expect("the continuation applies on the instance partition");
    assert!(
        advanced
            .iter()
            .any(|e| matches!(e, nanobpmn_engine_core::Event::MessageCorrelated { .. })),
        "the routed continuation advanced the parked token"
    );

    // (5): the routed close lands on the message partition.
    let Command::CorrelateMessageSubscription {
        subscription_key,
        message_key,
        instance_key,
        element_instance_key,
        element_id,
        ..
    } = continuation.clone()
    else {
        unreachable!("continuation is a CorrelateMessageSubscription");
    };
    let close = Command::CloseMessageSubscription {
        subscription_key,
        instance_key,
        element_instance_key,
        element_id,
        delivery_key: 0,
    };
    message_partition
        .apply_command_at(close.clone(), T0 + 5)
        .expect("the routed close applies on the message partition");

    // (6): a stale ack (#1874) — the catch's subscription is not awaiting an
    // ack, so the message partition ignores it (the idempotent no-op path).
    let ack = Command::AcknowledgeMessageCorrelation {
        subscription_key,
        message_key,
        accepted: true,
        consumed: true,
        deferred: false,
    };
    let acked = message_partition
        .apply_command_at(ack.clone(), T0 + 6)
        .expect("a stale ack applies on the message partition");
    assert!(acked.is_empty(), "a stale ack is a no-op: {acked:?}");

    // (7): a resume handshake (#1949) for the now-closed subscription is a
    // no-op too (only an open, non-awaiting subscription is re-offered).
    let resume = Command::ResumeMessageSubscription { subscription_key };
    let resumed = message_partition
        .apply_command_at(resume.clone(), T0 + 7)
        .expect("a resume handshake applies on the message partition");
    assert!(resumed.is_empty(), "a stale resume is a no-op: {resumed:?}");

    vec![opening, continuation, close, ack, resume]
}

/// The [`Command::kind`] of every corpus entry (coverage evidence).
pub fn covered_kinds(entries: &[(u64, Command)]) -> std::collections::BTreeSet<&'static str> {
    entries.iter().map(|(_, c)| c.kind()).collect()
}

/// Serializes the corpus as the runner's JSON-lines input. Each line carries
/// its `scenario` label (`labels` is parallel to `entries`); the runner
/// ignores it (serde skips unknown fields, so a previous release's runner
/// reads the same file), the cross-release comparison keys its
/// behavioural-divergence allowlist on it.
pub fn to_jsonl(entries: &[(u64, Command)], labels: &[&str]) -> String {
    assert_eq!(entries.len(), labels.len(), "one scenario label per entry");
    let mut out = String::new();
    for ((now, command), scenario) in entries.iter().zip(labels) {
        out.push_str(&json!({ "now": now, "command": command, "scenario": scenario }).to_string());
        out.push('\n');
    }
    out
}
