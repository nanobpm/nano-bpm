//! The determinism-CI **command corpus generator** (ADR 0066 slice 6, #1665).
//!
//! Builds a representative, fully deterministic command corpus covering every
//! [`Command`] variant — deployments (process, DMN, forms, generic resources,
//! bundles), instance lifecycle, jobs (activate, complete, fail, error, retry,
//! timeout, **expiry**), user tasks, **timers**, messages and signals,
//! conditional starts, **event sub-processes**, **migration**,
//! **modification**, ad-hoc activities, agents, orphan quarantine, **cluster
//! variables** and the **config commands** — plus a seeded randomized workload
//! (a fixed-seed xorshift, never host entropy) over the same processes.
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

use nanobpmn_engine_core::{Command, Engine, EventSubProcessTrigger, ProcessBuilder, Value};
use serde_json::{json, Value as Json};

/// Epoch the corpus clock starts at (fixed: the clock is corpus data).
const T0: u64 = 1_700_000_000_000;

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
        }
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
/// An external (job-backed) agent task plus a job-worker ad-hoc container,
/// side by side, both left running for the ad-hoc / agent commands.
const AGENTS_BPMN: &str = r#"
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="agents" isExecutable="true">
      <bpmn:startEvent id="s" />
      <bpmn:parallelGateway id="fork" />
      <bpmn:serviceTask id="agent">
        <bpmn:extensionElements>
          <zeebe:agentDefinition agentType="external" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
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
      <bpmn:endEvent id="e1" />
      <bpmn:endEvent id="e2" />
      <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="fork" />
      <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="agent" />
      <bpmn:sequenceFlow id="f3" sourceRef="fork" targetRef="adhoc" />
      <bpmn:sequenceFlow id="f4" sourceRef="agent" targetRef="e1" />
      <bpmn:sequenceFlow id="f5" sourceRef="adhoc" targetRef="e2" />
    </bpmn:process>
  </bpmn:definitions>"#;

const EXCLUDED: &str = "excluded from this corpus";

/// Builds the full corpus. `random_instances` sizes the randomized workload.
pub fn build(random_instances: usize) -> Vec<(u64, Command)> {
    generate(random_instances, BTreeSet::new()).0
}

/// Builds the corpus WITHOUT the commands at the unfiltered positions in
/// `exclude` (those a previous release cannot decode). Keys are harvested
/// from the engine the filtered corpus builds, so later commands still
/// address real entities. A kept command whose outcome (accepted vs
/// rejected) would change depends on an excluded one, so it is excluded too
/// (returned in the set) rather than silently compared as a degraded
/// execution both builds agree on; fails if more than 10% of the corpus is
/// lost that way.
pub fn build_excluding(
    random_instances: usize,
    exclude: BTreeSet<u64>,
) -> Result<(Corpus, BTreeSet<u64>), String> {
    let (_, full) = generate(random_instances, BTreeSet::new());
    let mut excluded = exclude;
    // Fixpoint: a kept command whose outcome flips depends on an excluded one
    // (e.g. a create of a process whose deployment the old release cannot
    // decode) — that scenario is unreachable for the old release, so it is
    // excluded too, and the corpus regenerated, until nothing flips.
    for _ in 0..64 {
        let (entries, filtered) = generate(random_instances, excluded.clone());
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
            if entries.len() * 10 < full.len() * 9 {
                return Err(format!(
                    "only {} of {} corpus entries survive the exclusions ({:?}); \
                     isolate newer variants so they do not take core scenarios with them",
                    entries.len(),
                    full.len(),
                    excluded
                ));
            }
            return Ok((entries, excluded));
        }
        excluded.extend(flipped);
    }
    Err("exclusions did not reach a fixpoint".into())
}

fn generate(
    random_instances: usize,
    exclude: BTreeSet<u64>,
) -> (Vec<(u64, Command)>, Vec<Option<bool>>) {
    let mut g = Gen::new();
    g.exclude = exclude;

    // ---- Deployments -------------------------------------------------------
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
    for _ in 0..3 {
        g.push(create("timers", BTreeMap::new()));
    }
    g.tick(2_500);
    let now = g.now;
    g.push(Command::TriggerTimers { now });
    let now = g.now;
    g.push(activate("slow-work", 3, now));
    g.tick(31_000);
    let now = g.now;
    g.push(Command::ExpireJobs { now });
    let now = g.now;
    g.push(Command::ExpireJobsByDurability { now, durable: true });
    g.tick(6_000);
    let now = g.now;
    g.push(Command::TriggerTimers { now });

    // ---- Messages, signals, event sub-processes, conditional starts -------
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
    // protocol), addressed at real keys where they exist.
    let m2 = g.push(create(
        "messaging",
        vars(&[("orderId", Value::Str("o-2".into()))]),
    ));
    let m2_instance = find(&m2, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let m2_ei = m2
        .iter()
        .filter_map(|e| e.get("ElementActivated"))
        .find(|v| v.get("element_id").and_then(Json::as_str) == Some("m"))
        .and_then(|v| v.get("element_instance_key").and_then(Json::as_u64))
        .unwrap_or(0);
    g.push_json(json!({"OpenMessageSubscription": {
        "subscription_key": 9_000_001u64, "instance_key": m2_instance,
        "element_instance_key": m2_ei, "element_id": "m", "message_name": "paid",
        "correlation_key": "o-2", "kind": "IntermediateCatch"
    }}));
    g.push_json(json!({"CorrelateMessageSubscription": {
        "subscription_key": 9_000_001u64, "message_key": 9_000_002u64,
        "instance_key": m2_instance, "element_instance_key": m2_ei, "element_id": "m",
        "kind": "IntermediateCatch", "variables": {}
    }}));
    g.push_json(json!({"CloseMessageSubscription": {
        "subscription_key": 9_000_001u64, "instance_key": m2_instance,
        "element_instance_key": m2_ei, "element_id": "m"
    }}));
    g.push_json(json!({"DispatchStartInstance": {
        "process_id": "starters", "start_element_id": "ms", "variables": {},
        "tags": [], "business_id": null
    }}));

    // ---- Suspend / resume / cancel ----------------------------------------
    g.push(Command::SuspendInstance {
        instance_key: m1_instance,
    });
    g.push(Command::ResumeInstance {
        instance_key: m1_instance,
    });
    g.push(Command::CancelInstance {
        instance_key: m2_instance,
    });

    // ---- Migration and modification ---------------------------------------
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
    // A running ad-hoc container and an active external agent task, so both
    // command families exercise their SUCCESS paths (push_ok_json), not only
    // rejections against an unrelated key.
    let agents = nanobpmn_engine_core::bpmn::parse_bpmn(AGENTS_BPMN)
        .expect("agents model")
        .remove(0);
    g.push(Command::DeployProcess(agents));
    let ag = g.push(create("agents", BTreeMap::new()));
    // `unwrap_or(0)`: unfiltered, a 0 key makes push_ok_json panic below.
    let ag_instance = find(&ag, "ProcessInstanceCreated", "instance_key").unwrap_or(0);
    let container = activated(&ag, "adhoc").unwrap_or(0);
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
    g.push(Command::QuarantineOffheapOrphan {
        instance_key: 9_100_000,
        stamp: None,
    });
    g.push(Command::QuarantineColdOrphan {
        instance_key: 9_100_001,
    });

    // ---- Seeded randomized workload ---------------------------------------
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

    // ---- Newest log variants last (see module doc) ------------------------
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

    let outcomes = std::mem::take(&mut g.outcomes);
    (g.into_entries(), outcomes)
}

/// The [`Command::kind`] of every corpus entry (coverage evidence).
pub fn covered_kinds(entries: &[(u64, Command)]) -> std::collections::BTreeSet<&'static str> {
    entries.iter().map(|(_, c)| c.kind()).collect()
}

/// Serializes the corpus as the runner's JSON-lines input.
pub fn to_jsonl(entries: &[(u64, Command)]) -> String {
    let mut out = String::new();
    for (now, command) in entries {
        out.push_str(&json!({ "now": now, "command": command }).to_string());
        out.push('\n');
    }
    out
}
