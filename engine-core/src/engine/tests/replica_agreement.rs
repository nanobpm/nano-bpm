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
    assert_eq!(replica_a.len(), replica_b.len(), "replicas emitted different event counts");
    if let Some(i) = (0..replica_a.len()).find(|&i| replica_a[i] != replica_b[i]) {
        panic!(
            "replicas diverged at event {i}:\n  a: {:?}\n  b: {:?}",
            replica_a[i], replica_b[i]
        );
    }
}

#[test]
fn feel_now_and_today_observe_the_command_timestamp() {
    let events = replay_journal();
    let outputs: Vec<&HashMap<String, Value>> = events
        .iter()
        .filter_map(|e| match e {
            Event::VariablesUpdated { variables, .. } | Event::ScopedVariablesUpdated { variables, .. } if variables.contains_key("t") => {
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
    assert!(created_a > 0 && created_a < 32, "both gateway branches taken");
}
