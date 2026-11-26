//! Replica agreement for FEEL's nondeterministic builtins (#1642).
//!
//! Raft replicas apply the same command journal independently. Every
//! engine-side FEEL evaluation (gateway conditions, output mappings, …) must
//! therefore see the command's replicated timestamp and a replicated RNG
//! stream — never the process-local wall clock or entropy — or replicas take
//! different branches and allocate different keys.
use super::*;

const XML: &str = r#"
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="p">
      <bpmn:startEvent id="s" />
      <bpmn:exclusiveGateway id="gw" default="toB" />
      <bpmn:serviceTask id="A">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="a" />
          <zeebe:ioMapping>
            <zeebe:output source="=random number()" target="r" />
            <zeebe:output source="=string(now())" target="t" />
            <zeebe:output source="=string(today())" target="d" />
            <zeebe:output source="=uuid()" target="u" />
          </zeebe:ioMapping>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:serviceTask id="B">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="b" />
          <zeebe:ioMapping>
            <zeebe:output source="=random number()" target="r" />
            <zeebe:output source="=string(now())" target="t" />
            <zeebe:output source="=string(today())" target="d" />
            <zeebe:output source="=uuid()" target="u" />
          </zeebe:ioMapping>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="e" />
      <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="gw" />
      <bpmn:sequenceFlow id="toA" sourceRef="gw" targetRef="A">
        <bpmn:conditionExpression>=random number() &lt; 0.5</bpmn:conditionExpression>
      </bpmn:sequenceFlow>
      <bpmn:sequenceFlow id="toB" sourceRef="gw" targetRef="B" />
      <bpmn:sequenceFlow id="f2" sourceRef="A" targetRef="e" />
      <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="e" />
    </bpmn:process>
  </bpmn:definitions>"#;

/// 2023-11-14T22:13:20Z in epoch milliseconds.
const T0: u64 = 1_700_000_000_000;

/// Replays a fixed command journal (deploy, N creates, complete every job) on a
/// fresh engine, returning every emitted event.
fn replay_journal() -> Vec<Event> {
    let def = crate::bpmn::parse_bpmn(XML).unwrap().remove(0);
    let mut engine = Engine::new();
    let mut all = engine
        .apply_command_at(Command::DeployProcess(def), T0)
        .unwrap();
    let mut jobs = Vec::new();
    for i in 0..32u64 {
        let evs = engine
            .apply_command_at(Command::create_instance("p"), T0 + i)
            .unwrap();
        jobs.extend(evs.iter().filter_map(|e| match e {
            Event::JobCreated { job_key, .. } => Some(*job_key),
            _ => None,
        }));
        all.extend(evs);
        // Separate the two replicas' wall clocks / process entropy further.
        std::thread::sleep(std::time::Duration::from_micros(50));
    }
    for ty in ["a", "b"] {
        engine.activate_jobs(ty, "w", 64, 60_000, T0 + 500);
    }
    for (i, job) in jobs.into_iter().enumerate() {
        all.extend(
            engine
                .apply_command_at(Command::complete_job(job), T0 + 1_000 + i as u64)
                .unwrap(),
        );
    }
    all
}

#[test]
fn replicas_replaying_the_same_journal_agree_on_feel_now_and_random() {
    let replica_a = replay_journal();
    let replica_b = replay_journal();
    assert_eq!(
        replica_a.len(),
        replica_b.len(),
        "replicas emitted different event counts"
    );
    if let Some(i) = (0..replica_a.len()).find(|&i| replica_a[i] != replica_b[i]) {
        panic!(
            "replicas diverged at event {i}:\n  a: {:?}\n  b: {:?}",
            replica_a[i], replica_b[i]
        );
    }
}

/// A process whose single service task has NO outgoing flows: completing its
/// job mints no keys, so the allocator position (`next_local`) is unchanged by
/// the completion — the exact shape in which two same-timestamp completions
/// would reseed the random stream identically if the seed were a bare
/// `(now, next_local)` pair (#1649 review).
const SINK_XML: &str = r#"
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="sink">
      <bpmn:startEvent id="s" />
      <bpmn:serviceTask id="A">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="a" />
          <zeebe:ioMapping>
            <zeebe:output source="=uuid()" target="u" />
            <zeebe:output source="=random number()" target="r" />
          </zeebe:ioMapping>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="A" />
    </bpmn:process>
  </bpmn:definitions>"#;

/// Deploys `SINK_XML` on `engine` at `T0`, then creates one instance and
/// activates its job, returning the activated job keys.
fn deploy_and_start_sink(engine: &mut Engine) -> Vec<u64> {
    let def = crate::bpmn::parse_bpmn(SINK_XML).unwrap().remove(0);
    engine
        .apply_command_at(Command::DeployProcess(def), T0)
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("sink"), T0)
        .unwrap();
    activate_sink_jobs(engine)
}

/// Activates the pending `a` job(s) of already-created sink instances.
fn activate_sink_jobs(engine: &mut Engine) -> Vec<u64> {
    engine
        .activate_jobs("a", "w", 64, 60_000, T0)
        .into_iter()
        .map(|j| j.key)
        .collect()
}

/// The `u` (uuid) and `r` (random) outputs of every completed sink task, in
/// event order.
fn sink_outputs(events: &[Event]) -> Vec<(Value, Value)> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::VariablesUpdated { variables, .. }
            | Event::ScopedVariablesUpdated { variables, .. }
                if variables.contains_key("u") =>
            {
                Some((variables["u"].clone(), variables["r"].clone()))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn same_timestamp_completions_without_key_allocation_do_not_repeat_the_stream() {
    let mut engine = Engine::new();
    let mut jobs = deploy_and_start_sink(&mut engine);
    // A second instance at the SAME timestamp: the create mints keys, so the
    // two completions below run at the same `now` with an allocator position
    // that no completion advances (the task has no outgoing flows).
    engine
        .apply_command_at(Command::create_instance("sink"), T0)
        .unwrap();
    jobs.extend(activate_sink_jobs(&mut engine));
    assert_eq!(jobs.len(), 2);

    let mut all = Vec::new();
    for job in jobs {
        all.extend(
            engine
                .apply_command_at(Command::complete_job(job), T0)
                .unwrap(),
        );
    }
    let outputs = sink_outputs(&all);
    assert_eq!(outputs.len(), 2, "both instances ran their output mapping");
    assert_ne!(
        outputs[0].0, outputs[1].0,
        "same-timestamp completions produced the same uuid"
    );
    assert_ne!(
        outputs[0].1, outputs[1].1,
        "same-timestamp completions produced the same random number"
    );
}

#[test]
fn distinct_partitions_with_matching_clock_and_allocator_do_not_share_streams() {
    // Two partitions with identical `(now, next_local)` at completion time:
    // both deploy and create at T0 (same local counter values), then complete
    // at T0. A partition-blind seed would give both the same stream.
    let mut p1 = Engine::with_partition(1);
    let mut p2 = Engine::with_partition(2);
    let j1 = deploy_and_start_sink(&mut p1);
    let j2 = deploy_and_start_sink(&mut p2);
    let out1 = sink_outputs(
        &p1.apply_command_at(Command::complete_job(j1[0]), T0)
            .unwrap(),
    );
    let out2 = sink_outputs(
        &p2.apply_command_at(Command::complete_job(j2[0]), T0)
            .unwrap(),
    );
    assert_eq!(out1.len(), 1);
    assert_eq!(out2.len(), 1);
    assert_ne!(
        out1[0].0, out2[0].0,
        "partitions 1 and 2 produced the same uuid"
    );
    assert_ne!(
        out1[0].1, out2[0].1,
        "partitions 1 and 2 produced the same random number"
    );
}

#[cfg(feature = "serde")]
#[test]
fn snapshot_recovery_continues_the_seed_sequence() {
    // Drive two same-timestamp completions, snapshot, restore, then complete a
    // third at the same timestamp: the restored engine's seed must continue the
    // sequence, not restart it — a restart would reissue the FIRST completion's
    // stream here (both ran at `T0` with the allocator back at the same
    // position relative to the snapshot).
    let mut engine = Engine::new();
    let mut jobs = deploy_and_start_sink(&mut engine);
    engine
        .apply_command_at(Command::create_instance("sink"), T0)
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("sink"), T0)
        .unwrap();
    jobs.extend(activate_sink_jobs(&mut engine));
    assert_eq!(jobs.len(), 3);

    let mut outputs = Vec::new();
    for job in jobs.iter().take(2) {
        outputs.extend(sink_outputs(
            &engine
                .apply_command_at(Command::complete_job(*job), T0)
                .unwrap(),
        ));
    }
    let mut restored = Engine::from_snapshot(engine.snapshot());
    outputs.extend(sink_outputs(
        &restored
            .apply_command_at(Command::complete_job(jobs[2]), T0)
            .unwrap(),
    ));
    assert_eq!(outputs.len(), 3);
    for i in 0..outputs.len() {
        for j in (i + 1)..outputs.len() {
            assert_ne!(
                outputs[i], outputs[j],
                "completions {i} and {j} (across a snapshot restore) produced the same (uuid, random)"
            );
        }
    }
}

#[test]
fn feel_now_and_today_observe_the_command_timestamp() {
    let events = replay_journal();
    let outputs: Vec<&BTreeMap<String, Value>> = events
        .iter()
        .filter_map(|e| match e {
            Event::VariablesUpdated { variables, .. }
            | Event::ScopedVariablesUpdated { variables, .. }
                if variables.contains_key("t") =>
            {
                Some(variables)
            }
            _ => None,
        })
        .collect();
    assert_eq!(outputs.len(), 32, "every instance ran its output mapping");
    for vars in &outputs {
        assert_eq!(vars["d"], Value::Str("2023-11-14".into()));
        match &vars["t"] {
            Value::Str(t) => assert!(t.starts_with("2023-11-14T22:13:21"), "now() = {t}"),
            other => panic!("now() rendered {other:?}"),
        }
    }
    // The seeded stream still behaves like randomness: both branches are taken
    // and the uuids are distinct.
    let mut uuids: Vec<_> = outputs.iter().map(|v| v["u"].clone()).collect();
    uuids.sort_by_key(|v| format!("{v:?}"));
    uuids.dedup();
    assert_eq!(uuids.len(), 32);
    let created_a = events
        .iter()
        .filter(|e| matches!(e, Event::JobCreated { job_type, .. } if job_type == "a"))
        .count();
    assert!(
        created_a > 0 && created_a < 32,
        "both gateway branches taken"
    );
}

/// Live execution and every recovery path (event-only replay, snapshot plus
/// journal tail) must seed the NEXT command identically (#1649 review): the
/// seed is a pure function of the replicated command and replicated state, so
/// there is no hidden counter for recovery to lose or a batch to skew.
#[test]
fn recovered_engines_seed_the_next_command_like_the_live_engine() {
    let mut live = Engine::new();
    let mut journal = Vec::new();
    let def = crate::bpmn::parse_bpmn(SINK_XML).unwrap().remove(0);
    journal.extend(
        live.apply_command_at(Command::DeployProcess(def), T0)
            .unwrap(),
    );
    for _ in 0..3 {
        journal.extend(
            live.apply_command_at(Command::create_instance("sink"), T0)
                .unwrap(),
        );
    }
    let jobs = activate_sink_jobs(&mut live);
    assert_eq!(jobs.len(), 3);
    journal.extend(
        live.apply_command_at(Command::complete_job(jobs[0]), T0)
            .unwrap(),
    );

    // Event-only replay of the whole journal (no `RaftEntryApplied` records).
    let mut replayed = Engine::replay(journal.clone());
    replayed.activate_jobs("a", "w", 64, 60_000, T0);

    let expected = sink_outputs(
        &live
            .apply_command_at(Command::complete_job(jobs[1]), T0)
            .unwrap(),
    );
    let got = sink_outputs(
        &replayed
            .apply_command_at(Command::complete_job(jobs[1]), T0)
            .unwrap(),
    );
    assert_eq!(expected.len(), 1);
    assert_eq!(got, expected, "event-only replay seeded differently");
}

/// Snapshot taken mid-journal plus the journal tail must also agree with the
/// live engine on the next command's seed (#1649 review).
#[cfg(feature = "serde")]
#[test]
fn snapshot_plus_tail_seeds_the_next_command_like_the_live_engine() {
    let mut live = Engine::new();
    let def = crate::bpmn::parse_bpmn(SINK_XML).unwrap().remove(0);
    live.apply_command_at(Command::DeployProcess(def), T0)
        .unwrap();
    live.apply_command_at(Command::create_instance("sink"), T0)
        .unwrap();
    live.apply_command_at(Command::create_instance("sink"), T0)
        .unwrap();
    let snap = live.snapshot();
    let mut tail = Vec::new();
    tail.extend(
        live.apply_command_at(Command::create_instance("sink"), T0)
            .unwrap(),
    );
    let jobs = activate_sink_jobs(&mut live);
    tail.extend(
        live.apply_command_at(Command::complete_job(jobs[0]), T0)
            .unwrap(),
    );

    let mut recovered = Engine::from_snapshot(snap);
    recovered.apply_replayed_events(tail);
    recovered.activate_jobs("a", "w", 64, 60_000, T0);

    let expected = sink_outputs(
        &live
            .apply_command_at(Command::complete_job(jobs[1]), T0)
            .unwrap(),
    );
    let got = sink_outputs(
        &recovered
            .apply_command_at(Command::complete_job(jobs[1]), T0)
            .unwrap(),
    );
    assert_eq!(expected.len(), 1);
    assert_eq!(got, expected, "snapshot+tail recovery seeded differently");
}

/// An exclusive gateway whose LAST-declared flow (evaluated first, in
/// execution order) holds, and whose other flow's condition is `skipped`. Task
/// `A`'s input mapping then draws from the command's random stream.
fn lazy_routing_xml(skipped: &str) -> String {
    format!(
        r#"
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="lazy">
      <bpmn:startEvent id="s" />
      <bpmn:exclusiveGateway id="gw" />
      <bpmn:serviceTask id="A">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="a" />
          <zeebe:ioMapping>
            <zeebe:input source="=random number()" target="r" />
          </zeebe:ioMapping>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="C" />
      <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="gw" />
      <bpmn:sequenceFlow id="toC" sourceRef="gw" targetRef="C">
        <bpmn:conditionExpression>{skipped}</bpmn:conditionExpression>
      </bpmn:sequenceFlow>
      <bpmn:sequenceFlow id="toA" sourceRef="gw" targetRef="A">
        <bpmn:conditionExpression>=true</bpmn:conditionExpression>
      </bpmn:sequenceFlow>
    </bpmn:process>
  </bpmn:definitions>"#
    )
}

/// Task `A`'s drawn `r`, for a fresh engine running `lazy_routing_xml(skipped)`.
fn lazy_routing_draw(skipped: &str) -> Value {
    let def = crate::bpmn::parse_bpmn(&lazy_routing_xml(skipped))
        .unwrap()
        .remove(0);
    let mut engine = Engine::new();
    engine
        .apply_command_at(Command::DeployProcess(def), T0)
        .unwrap();
    let events = engine
        .apply_command_at(Command::create_instance("lazy"), T0)
        .unwrap();
    events
        .iter()
        .find_map(|e| match e {
            Event::ScopedVariablesUpdated { variables, .. } => variables.get("r").cloned(),
            _ => None,
        })
        .expect("the gateway routed to A, whose input mapping drew r")
}

/// #2051 review: split-gateway routing must keep the lazy scan's stopping
/// points (exclusive: the first holding condition or error; inclusive: the
/// first error). A condition the scan never reaches must not draw from the
/// shared per-command random stream, or it perturbs every later draw.
#[test]
fn exclusive_routing_does_not_evaluate_conditions_past_the_first_match() {
    assert_eq!(
        lazy_routing_draw("=random number() &lt; 2"),
        lazy_routing_draw("=false"),
        "a condition after the first holding one advanced the random stream"
    );
}
