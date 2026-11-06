//! `listeners` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn end_listener_fires_on_a_multi_incoming_inclusive_join() {
    // #1168 regression: an inclusive gateway acting as a JOIN fires via its
    // activation guard (`activate_join`, #1241), NOT the split-completion
    // path (`complete_inclusive_gateway`). That path must still honour the
    // element's `end` execution-listener gate (ADR 0037): the join rests in
    // COMPLETING while the listener runs, and its outgoing flow is only taken
    // once the chain drains. Without the gate a multi-incoming inclusive join
    // silently skips its listener job (and any variable rewrite it performs).
    let def = ProcessBuilder::new("inc-join-listener")
        .start_event("s")
        .inclusive_gateway("isplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .inclusive_gateway("join")
        .end_event("e")
        .connect("s", "isplit")
        .connect("isplit", "a")
        .connect("isplit", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "e")
        .with_listeners(
            "join",
            Vec::new(),
            vec![el(ListenerEventType::End, "join-audit")],
        )
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("inc-join-listener"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Drive both split branches to the join; the second arrival takes the last
    // incoming flow and makes the join ready.
    complete_one(&mut engine, "ja");
    let arrive = complete_one(&mut engine, "jb");

    // The ready join parks on its `end` listener: no outgoing flow taken yet, and
    // the instance is not complete.
    assert!(
        !arrive.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, .. } if from == "join"
        )),
        "join must not route until its end listener completes"
    );
    assert!(
        !engine.is_completed(instance_key),
        "instance parked on the inclusive join's end listener"
    );

    // Completing the listener drains the chain → the join routes its outgoing
    // flow exactly once and the instance completes.
    let audit = engine.activate_jobs("join-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one inclusive-join end-listener job");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    let routed = done
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "join" && to == "e"
            )
        })
        .count();
    assert_eq!(
        routed, 1,
        "join routes its outgoing flow exactly once after the end listener"
    );
    assert!(engine.is_completed(instance_key));
}

/// Issue #1170 regression — when an interrupting boundary fires while the MI body
/// is parked on its own `start` execution-listener job (the boundary is armed
/// BEFORE the start-listener gate), the body-level listener job must be
/// cancelled, not left live on a removed body.
#[test]
fn interrupting_boundary_cancels_the_multi_instance_body_start_listener_job() {
    let def = ProcessBuilder::new("mi")
        .start_event("start")
        .service_task("each", "handle")
        .with_multi_instance(
            "each",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        )
        .timer_boundary_event("timeout", "each", 5_000)
        .end_event("done")
        .end_event("escalated")
        .connect("start", "each")
        .connect("each", "done")
        .connect("timeout", "escalated")
        .with_listeners(
            "each",
            vec![el(ListenerEventType::Start, "mi-start")],
            Vec::new(),
        )
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The body parks on its start-listener job; no children have spawned yet.
    let listener = engine.activate_jobs("mi-start", "W", 10, 1_000, 0);
    assert_eq!(
        listener.len(),
        1,
        "the body is parked on a start-listener job"
    );
    let listener_key = listener[0].key;
    assert!(
        engine.activate_jobs("handle", "W", 10, 1_000, 0).is_empty(),
        "no child jobs while the body start listener is live"
    );

    // The boundary fires while the body is parked on that listener.
    let fired = engine.trigger_timers(5_000);
    assert_eq!(
        engine.state().jobs[&listener_key].state,
        state::JobState::Canceled,
        "the body's own start-listener job is cancelled, not left live on a removed body (#1170)"
    );
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "timeout" && to == "escalated"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        engine.instance(key).is_none() || engine.instance(key).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared"
    );
    assert!(engine.is_completed(key), "the instance completes (#1170)");
}

/// Defect-class guard (dead-scope guard, `AdvanceTaskListener` branch):
/// `UserTaskCanceled` intentionally *retains* the task record (state → `Canceled`,
/// `pending` cleared). A bare existence check would let an `AdvanceTaskListener`
/// queued before a terminate teardown mint another listener job against the dead
/// task, so the guard must require the task to still be `Created`.
#[test]
fn dead_scope_guard_rejects_task_listener_advance_for_a_canceled_task() {
    //  sub: sub_start -> split =< ut(user task), trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .user_task("ut")
        .contained_in("ut", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "ut")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let ut_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");

    let advance = Step::AdvanceTaskListener {
        user_task_key: ut_key,
        event_type: crate::model::TaskListenerEventType::Completing,
        index: 0,
    };

    // While the task is `Created` the guard admits the listener advance.
    assert!(!engine.step_targets_dead_scope(&advance));

    // The sibling terminate cancels the resting task (record retained, Canceled).
    complete_one(&mut engine, "trigger-job");
    assert_eq!(
        engine.state().user_tasks[&ut_key].state,
        crate::state::UserTaskState::Canceled
    );
    assert_eq!(
        engine.instance(key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );

    // A listener advance queued before the cancel must now be dropped rather than
    // mint another listener job against the cancelled task.
    assert!(
        engine.step_targets_dead_scope(&advance),
        "an AdvanceTaskListener for a cancelled task must be dropped"
    );
}

#[test]
fn start_listener_holds_the_element_in_activating_until_the_chain_drains() {
    // #1429 (Zeebe parity): an element with `start` execution listeners writes
    // `ElementActivating` + the first listener job and NOTHING else — no
    // `ElementActivated` — until the chain drains; the drained chain then writes
    // `ElementActivated` before the element's own behaviour (here: its job).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![
                el(ListenerEventType::Start, "first"),
                el(ListenerEventType::Start, "second"),
            ],
            Vec::new(),
        )))
        .unwrap();
    let create = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // The task's records: Activating, then the first listener job — no
    // Activated while the chain is open. (JobCompleted carries no element_id,
    // so listener-job completions are excluded from the per-element view.)
    let task_record = |e: &&Event| {
        matches!(
            e,
            Event::ElementActivating { element_id, .. }
            | Event::ElementActivated { element_id, .. }
            | Event::ExecutionListenerJobCreated { element_id, .. }
            | Event::JobCreated { element_id, .. }
            if element_id == "charge"
        )
    };
    let task_kinds = |events: &[Event]| {
        kinds(
            &events
                .iter()
                .filter(task_record)
                .cloned()
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(
        task_kinds(&create),
        vec!["Activating", "ListenerJobCreated"],
        "element stays ACTIVATING behind its start-listener chain (#1429)"
    );

    // Completing the first listener mints the second — still no Activated.
    let first = engine.activate_jobs("first", "W", 10, 1_000, 0);
    let mid = engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();
    assert_eq!(
        task_kinds(&mid),
        vec!["ListenerJobCreated"],
        "mid-chain: no ElementActivated yet"
    );

    // Draining the chain writes Activated, then the element's own job.
    let second = engine.activate_jobs("second", "W", 10, 1_000, 0);
    let drained = engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();
    assert_eq!(
        task_kinds(&drained),
        vec!["Activated", "JobCreated"],
        "ElementActivated only after the last start listener completes"
    );
}

#[test]
fn start_listener_on_a_pass_through_element_writes_activated_then_completing_on_drain() {
    // #1429 (Zeebe parity): a pass-through element (a terminate end event)
    // with a `start` listener stays ACTIVATING until the chain drains; the
    // drain then writes Activated → Completing → Completed in one run.
    let def = ProcessBuilder::new("kill-switch")
        .start_event("s")
        .terminate_end_event("kill")
        .connect("s", "kill")
        .with_listeners(
            "kill",
            vec![el(ListenerEventType::Start, "killStart")],
            Vec::new(),
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let create = engine
        .apply_command(Command::create_instance("kill-switch"))
        .unwrap();
    let kill_record = |e: &&Event| {
        matches!(
            e,
            Event::ElementActivating { element_id, .. }
            | Event::ElementActivated { element_id, .. }
            | Event::ElementCompleting { element_id, .. }
            | Event::ElementCompleted { element_id, .. }
            | Event::ExecutionListenerJobCreated { element_id, .. }
            if element_id == "kill"
        )
    };
    let kill_kinds = |events: &[Event]| {
        kinds(
            &events
                .iter()
                .filter(kill_record)
                .cloned()
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(
        kill_kinds(&create),
        vec!["Activating", "ListenerJobCreated"],
        "terminate end stays ACTIVATING behind its start listener (#1429)"
    );

    let listener = engine.activate_jobs("killStart", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "one start-listener job");
    let done = engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    assert_eq!(
        kill_kinds(&done),
        vec!["Activated", "Completing", "Completed"],
        "drain writes Activated, then Completing for the pass-through element"
    );
}

#[test]
fn start_listener_runs_before_the_service_job_is_created() {
    // A `start` execution listener defers the element's own behaviour (job
    // creation): the listener job is minted at ACTIVATED time; the service job
    // only appears once the listener chain drains.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![el(ListenerEventType::Start, "audit")],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // No service `payment` job yet — the element rests in ACTIVATING.
    assert!(
        engine
            .activate_jobs("payment", "W", 10, 1_000, 0)
            .is_empty(),
        "service job must not exist until the start listener completes"
    );
    // The start-listener job is activatable.
    let listener = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "one start-listener job");

    // Completing it drains the chain → the service job is created.
    engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(payment.len(), 1, "service job created after start listener");
}

#[test]
fn end_listener_runs_before_the_element_completes() {
    // An `end` execution listener defers ElementCompleted + the outgoing flow:
    // the element rests in COMPLETING while the listener runs, so the instance
    // is not yet finished.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            Vec::new(),
            vec![el(ListenerEventType::End, "audit")],
        )))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    let completing = engine
        .apply_command(Command::complete_job(payment[0].key))
        .unwrap();
    // Completing the service job emits Completing + the end-listener job, but
    // NOT ElementCompleted, and the instance is still running.
    assert!(kinds(&completing).contains(&"Completing"));
    assert!(!kinds(&completing).contains(&"Completed"));
    assert!(kinds(&completing).contains(&"ListenerJobCreated"));
    assert!(
        !engine.is_completed(inst),
        "instance parked on end listener"
    );

    // Completing the end listener finalises completion → instance done.
    let listener = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    assert_eq!(listener.len(), 1, "one end-listener job");
    let done = engine
        .apply_command(Command::complete_job(listener[0].key))
        .unwrap();
    assert!(kinds(&done).contains(&"Completed"));
    assert!(
        engine.is_completed(inst),
        "instance finished after end listener"
    );
}

#[test]
fn multiple_start_listeners_run_sequentially_in_declaration_order() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![
                el(ListenerEventType::Start, "first"),
                el(ListenerEventType::Start, "second"),
            ],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();

    // Only the first listener is activatable; the second does not exist yet.
    assert!(engine.activate_jobs("second", "W", 10, 1_000, 0).is_empty());
    let first = engine.activate_jobs("first", "W", 10, 1_000, 0);
    assert_eq!(first.len(), 1);
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    // Now the second listener exists; the service job still does not.
    assert!(engine
        .activate_jobs("payment", "W", 10, 1_000, 0)
        .is_empty());
    let second = engine.activate_jobs("second", "W", 10, 1_000, 0);
    assert_eq!(second.len(), 1);
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();

    // Chain drained → service job created.
    assert_eq!(engine.activate_jobs("payment", "W", 10, 1_000, 0).len(), 1);
}

#[test]
fn listener_variables_merge_into_the_element_scope() {
    // A start listener that returns a variable makes it visible to the service
    // job that follows (Zeebe parity: listener completions merge forward).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![el(ListenerEventType::Start, "audit")],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let listener = engine.activate_jobs("audit", "W", 10, 1_000, 0);
    let mut vars = HashMap::new();
    vars.insert("approved".to_string(), Value::Bool(true));
    engine
        .apply_command(Command::complete_job_with(listener[0].key, vars))
        .unwrap();
    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(
        payment[0].variables.get("approved"),
        Some(&Value::Bool(true)),
        "start-listener variable is visible to the service job"
    );
}

#[test]
fn listener_free_model_journal_is_byte_identical() {
    // The critical invariant: a model with NO listeners must emit exactly the
    // same events as before execution-listener support existed. We assert the
    // full lifecycle event shape of a plain service task.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            Vec::new(),
            Vec::new(),
        )))
        .unwrap();
    let create = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    // Activation of the service task: Activating, Activated, JobCreated — no
    // listener events interleaved.
    assert_eq!(
        kinds(&create),
        vec![
            "_",
            "Activating",
            "Activated",
            "Completing",
            "Completed",
            "FlowTaken",
            "Activating",
            "Activated",
            "JobCreated"
        ],
        "listener-free activation journal unchanged"
    );
    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    let done = engine
        .apply_command(Command::complete_job(payment[0].key))
        .unwrap();
    assert_eq!(
        kinds(&done),
        vec![
            "JobCompleted",
            "Completing",
            "Completed",
            "FlowTaken",
            "Activating",
            "Activated",
            "Completing",
            "Completed",
            "InstanceCompleted"
        ],
        "listener-free completion journal unchanged"
    );
}

#[test]
fn start_and_end_listeners_bracket_the_service_task() {
    // Full lifecycle: start listener → service job → end listener.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![el(ListenerEventType::Start, "before")],
            vec![el(ListenerEventType::End, "after")],
        )))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let before = engine.activate_jobs("before", "W", 10, 1_000, 0);
    assert_eq!(before.len(), 1, "start listener first");
    engine
        .apply_command(Command::complete_job(before[0].key))
        .unwrap();

    let payment = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    assert_eq!(payment.len(), 1, "then the service job");
    engine
        .apply_command(Command::complete_job(payment[0].key))
        .unwrap();
    assert!(!engine.is_completed(inst));

    let after = engine.activate_jobs("after", "W", 10, 1_000, 0);
    assert_eq!(after.len(), 1, "then the end listener");
    engine
        .apply_command(Command::complete_job(after[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance finished after end listener"
    );
}

#[test]
fn end_listener_fires_on_an_exclusive_gateway() {
    // An `end` execution listener on an exclusive gateway defers the routing
    // decision: the gateway rests in COMPLETING while the listener runs, and the
    // outgoing flow is only taken once the chain drains (ADR 0037). This closes a
    // real gap — the gateway previously short-circuited before the end gate.
    let process = ProcessBuilder::new("route")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes")
        .end_event("no")
        .connect("s", "g")
        .connect_when("g", "yes", "=go")
        .connect_default("g", "no")
        .with_listeners(
            "g",
            Vec::new(),
            vec![el(ListenerEventType::End, "gate-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let create = engine
        .apply_command(Command::create_instance_with(
            "route",
            vars(&[("go", Value::Bool(true))]),
        ))
        .unwrap();
    let inst = create.iter().find_map(|e| e.instance_key()).unwrap();

    // The gateway parked on its end listener: no routing flow taken yet.
    assert!(
        !create.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "yes" || to == "no"
        )),
        "gateway must not route until its end listener completes"
    );
    assert!(kinds(&create).contains(&"ListenerJobCreated"));
    assert!(
        !engine.is_completed(inst),
        "instance parked on gateway end listener"
    );

    // Completing the listener drains the chain → the conditional flow is taken.
    let audit = engine.activate_jobs("gate-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one gateway end-listener job");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "yes"
        )),
        "conditional flow taken after the end listener"
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn xml_declared_gateway_start_listener_fires_end_to_end() {
    // #1197: a `start` execution listener declared in BPMN XML on an exclusive
    // gateway must be parsed AND fire at runtime — the gateway defers its routing
    // behind the listener job. Guards the whole parser→runtime path, not just the
    // programmatic ProcessBuilder wiring covered above.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="route" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:exclusiveGateway id="g" default="f3">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="gate-audit" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:exclusiveGateway>
    <bpmn:endEvent id="yes" />
    <bpmn:endEvent id="no" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="g" />
    <bpmn:sequenceFlow id="f2" sourceRef="g" targetRef="yes">
      <bpmn:conditionExpression>=go</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f3" sourceRef="g" targetRef="no" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let create = engine
        .apply_command(Command::create_instance_with(
            "route",
            vars(&[("go", Value::Bool(true))]),
        ))
        .unwrap();
    let inst = create.iter().find_map(|e| e.instance_key()).unwrap();

    // The gateway parked on its start listener: no routing flow taken yet.
    assert!(
        !create.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "yes" || to == "no"
        )),
        "gateway must not route until its start listener completes"
    );
    assert!(kinds(&create).contains(&"ListenerJobCreated"));
    assert!(!engine.is_completed(inst));

    // Completing the parsed listener drains the chain → the conditional route.
    let audit = engine.activate_jobs("gate-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one gateway start-listener job from XML");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "yes"
        )),
        "conditional flow taken after the parsed start listener"
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn xml_declared_boundary_start_listener_fires_end_to_end() {
    // #1197: a `start` execution listener declared in BPMN XML on a boundary
    // event must be parsed AND fire when the boundary triggers — the boundary
    // event activates through the shared listener gate, deferring its handler
    // flow behind the listener job. Guards the parser→runtime path for boundaries.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:signal id="sig" name="kill-switch" />
  <bpmn:process id="guarded" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="do-work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="abort" attachedToRef="work">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="abort-audit" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
      <bpmn:signalEventDefinition signalRef="sig" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="aborted" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="done" />
    <bpmn:sequenceFlow id="f3" sourceRef="abort" targetRef="aborted" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Trigger the boundary: the activity is interrupted, the boundary event
    // activates and parks on its parsed start listener rather than routing.
    let fired = engine.broadcast_signal("kill-switch", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })));
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "aborted"
        )),
        "boundary must not route until its start listener completes"
    );
    assert!(!engine.is_completed(instance_key));

    // Completing the parsed listener drains the chain → the boundary flow runs.
    let audit = engine.activate_jobs("abort-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one boundary start-listener job from XML");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(done.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "aborted"
    )));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn xml_declared_boundary_end_listener_fires_end_to_end() {
    // #1197: an `end` execution listener declared in BPMN XML on a boundary
    // event must be parsed AND gate the boundary's outgoing flow — when the
    // boundary triggers and completes, its parsed `end` listener job must fire
    // and defer the handler route until the listener completes. Complements the
    // `start`-listener e2e above so a boundary-specific *completion* regression
    // cannot pass on the parser test alone.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:signal id="sig" name="kill-switch" />
  <bpmn:process id="guarded" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="do-work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="abort" attachedToRef="work">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="abort-audit-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
      <bpmn:signalEventDefinition signalRef="sig" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="aborted" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="done" />
    <bpmn:sequenceFlow id="f3" sourceRef="abort" targetRef="aborted" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("guarded"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Trigger the boundary: the activity is interrupted and the boundary event
    // activates. With only an `end` listener, it completes its activation body
    // and parks in COMPLETING on the parsed end-listener chain rather than
    // routing to `aborted`.
    let fired = engine.broadcast_signal("kill-switch", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })));
    assert!(
        !fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "aborted"
        )),
        "boundary must not route until its end listener completes"
    );
    assert!(!engine.is_completed(instance_key));

    // Completing the parsed end listener drains the chain → the boundary flow runs.
    let audit = engine.activate_jobs("abort-audit-end", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one boundary end-listener job from XML");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(done.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "aborted"
    )));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn xml_declared_start_event_start_listener_fires_end_to_end() {
    // #1197: a `start` execution listener declared in BPMN XML on the process
    // START EVENT must be parsed AND fire when the instance is created — the
    // start event activates through the shared listener gate and defers taking
    // its outgoing flow behind the listener job. Instance creation enters through
    // the dedicated `start_instance` path (which activates the start event), so a
    // parser-only ownership assertion would NOT catch a regression in the
    // start-event activation/completion path; this drives it end to end.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="greet" isExecutable="true">
    <bpmn:startEvent id="s">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="start-audit" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let create = engine
        .apply_command(Command::create_instance("greet"))
        .unwrap();
    let inst = create.iter().find_map(|e| e.instance_key()).unwrap();

    // The start event parked on its start listener: the outgoing flow to the end
    // event is NOT taken yet, and the instance is not complete.
    assert!(
        create
            .iter()
            .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })),
        "the parsed start-event start listener must create a listener job on instance creation"
    );
    assert!(
        !create.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "e"
        )),
        "start event must not take its outgoing flow until its start listener completes"
    );
    assert!(!engine.is_completed(inst));

    // Completing the parsed listener drains the chain → the outgoing flow runs
    // and the instance completes.
    let audit = engine.activate_jobs("start-audit", "W", 10, 1_000, 0);
    assert_eq!(
        audit.len(),
        1,
        "one start-event start-listener job from XML"
    );
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "e"
        )),
        "outgoing flow taken after the parsed start-event start listener completes"
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn xml_declared_start_event_end_listener_fires_end_to_end() {
    // #1197: an `end` execution listener declared in BPMN XML on the process
    // START EVENT must be parsed AND gate the start event's outgoing flow — the
    // start event activates through the shared gate and, on completion, defers
    // taking its outgoing flow behind the parsed end-listener job. Instance
    // creation enters through the dedicated `start_instance` path, so a
    // parser-only assertion (or the `start`-listener e2e above) would NOT catch a
    // regression in the start event's COMPLETION listener path; this drives it
    // end to end.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="greet" isExecutable="true">
    <bpmn:startEvent id="s">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="start-audit-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let create = engine
        .apply_command(Command::create_instance("greet"))
        .unwrap();
    let inst = create.iter().find_map(|e| e.instance_key()).unwrap();

    // The start event parked on its END listener: the outgoing flow to the end
    // event is NOT taken yet, and the instance is not complete.
    assert!(
        create
            .iter()
            .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })),
        "the parsed start-event end listener must create a listener job on instance creation"
    );
    assert!(
        !create.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "e"
        )),
        "start event must not take its outgoing flow until its end listener completes"
    );
    assert!(!engine.is_completed(inst));

    // Completing the parsed end listener drains the chain → the outgoing flow
    // runs and the instance completes.
    let audit = engine.activate_jobs("start-audit-end", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one start-event end-listener job from XML");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "e"
        )),
        "outgoing flow taken after the parsed start-event end listener completes"
    );
    assert!(engine.is_completed(inst));
}

// zeebe-cells: element:ReceiveTask
#[test]
fn xml_declared_receive_task_end_listener_fires_end_to_end() {
    // #1197: a `receiveTask` is modelled as a pass-through, and an `end`
    // execution listener declared on it in BPMN XML must be parsed AND gate its
    // outgoing flow — proving the io_stack push makes the listener attach to the
    // receive task itself and fire through the shared pass-through gate (rather
    // than being dropped/hoisted, the mis-attachment class this PR closes).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="await" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:receiveTask id="rt">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="rt-audit-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="rt" />
    <bpmn:sequenceFlow id="f2" sourceRef="rt" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let create = engine
        .apply_command(Command::create_instance("await"))
        .unwrap();
    let inst = create.iter().find_map(|e| e.instance_key()).unwrap();

    // The receive task parked on its end listener: the outgoing flow to the end
    // event is NOT taken yet.
    assert!(
        create
            .iter()
            .any(|e| matches!(e, Event::ExecutionListenerJobCreated { .. })),
        "the parsed receive-task end listener must create a listener job"
    );
    assert!(
        !create.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "e"
        )),
        "receive task must not take its outgoing flow until its end listener completes"
    );
    assert!(!engine.is_completed(inst));

    let audit = engine.activate_jobs("rt-audit-end", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one receive-task end-listener job from XML");
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { to, .. } if to == "e"
        )),
        "outgoing flow taken after the parsed receive-task end listener completes"
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn end_listener_fires_on_an_embedded_subprocess() {
    // An `end` execution listener on an embedded sub-process defers the
    // sub-process's completion (and its outgoing flow) until the listener runs.
    let process = ProcessBuilder::new("wrap")
        .start_event("s")
        .sub_process("sub", "inner-start")
        .start_event("inner-start")
        .service_task("work", "job")
        .end_event("inner-end")
        .end_event("e")
        .contained_in("inner-start", "sub")
        .contained_in("work", "sub")
        .contained_in("inner-end", "sub")
        .connect("s", "sub")
        .connect("inner-start", "work")
        .connect("work", "inner-end")
        .connect("sub", "e")
        .with_listeners(
            "sub",
            Vec::new(),
            vec![el(ListenerEventType::End, "sub-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("wrap"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let work = engine.activate_jobs("job", "W", 10, 1_000, 0);
    assert_eq!(work.len(), 1);
    engine
        .apply_command(Command::complete_job(work[0].key))
        .unwrap();

    // Inner flow drained; the sub-process parks on its end listener.
    assert!(
        !engine.is_completed(inst),
        "sub-process parked on end listener"
    );
    let audit = engine.activate_jobs("sub-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one sub-process end-listener job");
    engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance done after the sub-process end listener"
    );
}

#[test]
fn start_listener_fires_once_on_a_multi_instance_body_before_children() {
    // A `start` execution listener on a multi-instance activity fires ONCE on the
    // body, before ANY child is instantiated (ADR 0037). It does not double-fire
    // per child.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(mi_body_with_listeners(
            vec![el(ListenerEventType::Start, "mi-start")],
            Vec::new(),
        )))
        .unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap();

    // No child jobs until the body's start listener drains.
    assert!(
        engine.activate_jobs("handle", "W", 10, 1_000, 0).is_empty(),
        "children must not spawn until the body start listener completes"
    );
    let start = engine.activate_jobs("mi-start", "W", 10, 1_000, 0);
    assert_eq!(start.len(), 1, "exactly one body start-listener job");
    engine
        .apply_command(Command::complete_job(start[0].key))
        .unwrap();

    // Now all three children spawn, and the start listener does NOT re-fire.
    assert_eq!(
        engine.activate_jobs("handle", "W", 10, 1_000, 0).len(),
        3,
        "all children spawn after the body start listener"
    );
    assert!(
        engine
            .activate_jobs("mi-start", "W", 10, 1_000, 0)
            .is_empty(),
        "start listener must not re-fire per child"
    );
}

#[test]
fn end_listener_fires_once_on_a_multi_instance_body_after_all_children() {
    // An `end` execution listener on a multi-instance activity fires ONCE on the
    // body, only after every child has completed, before the body itself
    // completes.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(mi_body_with_listeners(
            Vec::new(),
            vec![el(ListenerEventType::End, "mi-end")],
        )))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "mi",
            vars(&[(
                "items",
                Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let handle = engine.activate_jobs("handle", "W", 10, 1_000, 0);
    assert_eq!(handle.len(), 3);
    // Completing the first two children does NOT yet mint the body end listener.
    for j in &handle[..2] {
        engine.apply_command(Command::complete_job(j.key)).unwrap();
    }
    assert!(
        engine.activate_jobs("mi-end", "W", 10, 1_000, 0).is_empty(),
        "body end listener must wait for all children"
    );
    // Completing the last child parks the body on its single end listener.
    engine
        .apply_command(Command::complete_job(handle[2].key))
        .unwrap();
    assert!(
        !engine.is_completed(inst),
        "body parked on its end listener"
    );
    let end = engine.activate_jobs("mi-end", "W", 10, 1_000, 0);
    assert_eq!(end.len(), 1, "exactly one body end-listener job");
    engine
        .apply_command(Command::complete_job(end[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance done after the body end listener"
    );
}

#[test]
fn end_listener_fires_on_an_adhoc_container() {
    // An `end` execution listener on an ad-hoc sub-process container defers the
    // container's completion until the listener runs (natural, non-cancel path).
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc />
              <zeebe:executionListeners>
                <zeebe:executionListener eventType="end" type="agent-audit" />
              </zeebe:executionListeners>
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let process = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("p"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Drive the ad-hoc worker: activate one tool, then complete with no further
    // activations so the container drains naturally.
    let agent = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    assert_eq!(agent.len(), 1, "ad-hoc container job");
    engine
        .apply_command(Command::complete_job_with_result(
            agent[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool.len(), 1, "activated tool job");
    engine
        .apply_command(Command::complete_job(tool[0].key))
        .unwrap();

    // The tool drained; the agent re-emits for a final turn. Returning NO further
    // activations drives the container down its natural (non-cancel) completion
    // path, where it parks on its end listener before completing.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted");
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();

    // The ad-hoc container parks on its end listener before completing.
    assert!(
        !engine.is_completed(inst),
        "container parked on end listener"
    );
    let audit = engine.activate_jobs("agent-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1, "one ad-hoc container end-listener job");
    engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "instance done after the ad-hoc container end listener"
    );
}

#[test]
fn exclusive_gateway_end_listener_cannot_change_the_activation_selection() {
    // Since #1402 a split gateway's routing decision is finalised while it is
    // ACTIVATING (Zeebe `onActivate` → `findSequenceFlowToTake`), BEFORE the
    // `end` listener chain runs. A listener that rewrites a condition variable
    // therefore does NOT change the route: the gateway completes through the
    // selection it made at activation (journaled as `GatewayRouteSelected`),
    // exactly as Zeebe takes the flow found in `finalizeActivation` after the
    // start listeners drain.
    let process = ProcessBuilder::new("route")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("yes")
        .connect("s", "g")
        .connect_when("g", "yes", "=go")
        .with_listeners(
            "g",
            Vec::new(),
            vec![el(ListenerEventType::End, "gate-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    // `go=true` at creation → the flow to "yes" is selected at activation, so
    // the gateway parks on its end listener.
    let inst = engine
        .apply_command(Command::create_instance_with(
            "route",
            vars(&[("go", Value::Bool(true))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let audit = engine.activate_jobs("gate-audit", "W", 10, 1_000, 0);
    assert_eq!(audit.len(), 1);

    // The listener completes, flipping `go` to false — the activation-time
    // selection still routes to "yes"; no incident is raised.
    let done = engine
        .apply_command(Command::complete_job_with(
            audit[0].key,
            vars(&[("go", Value::Bool(false))]),
        ))
        .unwrap();
    assert!(
        !done
            .iter()
            .any(|e| matches!(e, Event::IncidentRaised { .. })),
        "the carried selection routes without re-evaluating: {done:?}"
    );
    assert!(
        done.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "g" && to == "yes"
        )),
        "the gateway takes the flow selected while ACTIVATING: {done:?}"
    );
    assert!(
        engine.is_completed(inst),
        "the instance completes through the carried selection"
    );
}

#[test]
fn creating_listener_gates_the_user_task_before_it_is_available() {
    // A `creating` listener runs while the element is ACTIVATING: the user task
    // is not completable until the chain drains.
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Creating,
        "onCreate",
    )]));

    // The task is not yet `Created`: assigning it is rejected.
    let key = only_user_task_key(&engine);
    assert!(
        engine
            .apply_command(Command::assign_user_task(key, "alice"))
            .is_err(),
        "task must not accept an assignment while the creating listener runs"
    );

    // The creating-listener job is activatable; completing it drains the chain
    // and the task becomes `Created`.
    let jobs = engine.activate_jobs("onCreate", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1, "one creating-listener job");
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
}

#[test]
fn assigning_listener_defers_the_assignment_until_it_completes() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Assigning,
        "onAssign",
    )]));
    let key = only_user_task_key(&engine);

    // Assign defers behind the listener: the assignee is not yet set.
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    assert_eq!(engine.state().user_tasks[&key].assignee, None);

    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1);
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );
}

#[test]
fn assigning_listener_can_deny_the_assignment() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Assigning,
        "onAssign",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("not allowed".into()),
                corrections: UserTaskCorrections::default(),
            },
        ))
        .unwrap();
    // Denied: the assignee stays unset and the task is assignable again.
    assert_eq!(engine.state().user_tasks[&key].assignee, None);
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    engine
        .apply_command(Command::assign_user_task(key, "bob"))
        .unwrap();
}

#[test]
fn assigning_listener_can_correct_the_assignee() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Assigning,
        "onAssign",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: false,
                denied_reason: None,
                corrections: UserTaskCorrections {
                    assignee: Some("carol".into()),
                    ..Default::default()
                },
            },
        ))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("carol")
    );
}

#[test]
fn updating_listener_defers_and_can_deny() {
    use crate::UserTaskChangeset;
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Updating,
        "onUpdate",
    )]));
    let key = only_user_task_key(&engine);

    let changeset = UserTaskChangeset {
        priority: Some(80),
        ..Default::default()
    };
    engine
        .apply_command(Command::update_user_task(key, changeset))
        .unwrap();
    // Deferred: priority not yet applied.
    assert_eq!(engine.state().user_tasks[&key].priority, 50);

    let jobs = engine.activate_jobs("onUpdate", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("no".into()),
                corrections: UserTaskCorrections::default(),
            },
        ))
        .unwrap();
    // Denied: priority unchanged.
    assert_eq!(engine.state().user_tasks[&key].priority, 50);
}

#[test]
fn completing_listener_defers_completion_and_can_deny() {
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);

    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    // Deferred: not completed, instance still running.
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    assert!(!engine.is_completed(inst));

    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1);
    // Deny returns the task to Created; the instance keeps running.
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("blocked".into()),
                corrections: UserTaskCorrections::default(),
            },
        ))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    assert!(!engine.is_completed(inst));

    // A second completion now succeeds through the listener.
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Completed
    );
    assert!(engine.is_completed(inst));
}

#[test]
fn completing_listeners_run_sequentially_in_declaration_order() {
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![
        tl(TaskListenerEventType::Completing, "first"),
        tl(TaskListenerEventType::Completing, "second"),
    ]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();

    // Only the first listener's job exists initially.
    assert!(engine.activate_jobs("second", "W", 10, 1_000, 0).is_empty());
    let first = engine.activate_jobs("first", "W", 10, 1_000, 0);
    assert_eq!(first.len(), 1);
    engine
        .apply_command(Command::complete_job(first[0].key))
        .unwrap();

    // Now the second appears; completing it drains the chain.
    let second = engine.activate_jobs("second", "W", 10, 1_000, 0);
    assert_eq!(second.len(), 1);
    engine
        .apply_command(Command::complete_job(second[0].key))
        .unwrap();
    assert!(engine.is_completed(inst));
}

#[test]
fn canceling_listener_runs_before_the_instance_terminates() {
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Canceling,
        "onCancel",
    )]));
    let key = only_user_task_key(&engine);

    engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    // Deferred termination: the task is not yet canceled and the instance is
    // not yet terminated.
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Created
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminating
    );

    let jobs = engine.activate_jobs("onCancel", "W", 10, 1_000, 0);
    assert_eq!(jobs.len(), 1, "one canceling-listener job");
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    // The chain drained: task canceled and the instance is gone.
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Canceled
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminated
    );
}

#[test]
fn task_listener_job_may_not_carry_variables() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    let vars = HashMap::from([("x".to_string(), Value::Bool(true))]);
    assert!(matches!(
        engine.apply_command(Command::complete_job_with(jobs[0].key, vars)),
        Err(EngineError::TaskListenerJobWithVariables { .. })
    ));
}

#[test]
fn task_listener_deny_may_not_carry_corrections() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    let jobs = engine.activate_jobs("onComplete", "W", 10, 1_000, 0);
    assert!(matches!(
        engine.apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("x".into()),
                corrections: UserTaskCorrections {
                    priority: Some(10),
                    ..Default::default()
                },
            },
        )),
        Err(EngineError::TaskListenerDenyWithCorrections { .. })
    ));
}

#[test]
fn creating_and_canceling_listeners_may_not_deny() {
    let (mut engine, _inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Creating,
        "onCreate",
    )]));
    let key = only_user_task_key(&engine);
    // The creating listener job exists while the task is ACTIVATING.
    let _ = key;
    let jobs = engine.activate_jobs("onCreate", "W", 10, 1_000, 0);
    assert!(matches!(
        engine.apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                denied: true,
                denied_reason: Some("x".into()),
                corrections: UserTaskCorrections::default(),
            },
        )),
        Err(EngineError::TaskListenerDenyNotSupported { .. })
    ));
}

#[test]
fn user_task_without_listeners_is_byte_identical() {
    // A user task carrying no listeners must produce the exact same journal as
    // the pre-task-listener engine: assign, then complete, straight through.
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = deploy_and_start(def);
    let key = only_user_task_key(&engine);

    let assign = engine
        .apply_command(Command::assign_user_task(key, "alice"))
        .unwrap();
    // No task-listener machinery leaks into the journal.
    assert!(!assign.iter().any(|e| matches!(
        e,
        Event::TaskListenerJobCreated { .. }
            | Event::UserTaskTransitionDeferred { .. }
            | Event::UserTaskTransitionResolved { .. }
    )));
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );

    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    assert!(engine.is_completed(inst));
}

#[test]
fn cancel_without_canceling_listener_terminates_synchronously() {
    // Byte-identical cancellation: a user task with no canceling listener is
    // canceled and the instance terminated in the same command.
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = deploy_and_start(def);
    let key = only_user_task_key(&engine);
    let events = engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminating { .. })));
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Canceled
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminated
    );
}

#[test]
fn initial_assignee_fires_the_assigning_listener() {
    // An initial assignee declared on the task must route through the `assigning`
    // listener (Zeebe parity): it is stripped off the CREATED record and applied
    // only once the assigning chain drains.
    use crate::model::UserTaskProps;
    let (mut engine, _inst) = deploy_and_start(user_task_with_props_and_listeners(
        UserTaskProps {
            assignee: Some("alice".into()),
            ..Default::default()
        },
        vec![tl(TaskListenerEventType::Assigning, "onAssign")],
    ));
    let key = only_user_task_key(&engine);
    // Not yet assigned: the assigning chain gates the initial assignment.
    assert_eq!(engine.state().user_tasks[&key].assignee, None);

    let jobs = engine.activate_jobs("onAssign", "W", 10, 1_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "one assigning-listener job for the initial assignee"
    );
    // The listener may correct it.
    engine
        .apply_command(Command::complete_job_with_task_result(
            jobs[0].key,
            TaskListenerJobResult {
                corrections: UserTaskCorrections {
                    assignee: Some("carol".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("carol")
    );
}

#[test]
fn initial_assignee_without_assigning_listener_stays_byte_identical() {
    // No assigning listeners: the initial assignee is applied directly on CREATED,
    // exactly as before task listeners existed.
    use crate::model::UserTaskProps;
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task_with(
            "review",
            UserTaskProps {
                assignee: Some("alice".into()),
                ..Default::default()
            },
        )
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let (engine, _inst) = deploy_and_start(def);
    let key = only_user_task_key(&engine);
    assert_eq!(
        engine.state().user_tasks[&key].assignee.as_deref(),
        Some("alice")
    );
}

/// Every `ElementActivated` / `ElementActivating` record for `element_id`.
fn activation_markers(events: &[Event], element_id: &str) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::ElementActivating { element_id: id, .. } if id == element_id => {
                Some("Activating")
            }
            Event::ElementActivated { element_id: id, .. } if id == element_id => Some("Activated"),
            _ => None,
        })
        .collect()
}

#[test]
fn start_listener_creation_incident_defers_activated_until_the_chain_drains() {
    // #1429 review: the FIRST start listener's job-type expression fails, so the
    // activation raises an incident instead of `ExecutionListenerJobCreated`.
    // The element must still stay ACTIVATING (no marker before the incident),
    // and resolving + draining must write exactly one `ElementActivated`.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![ExecutionListener {
                event_type: ListenerEventType::Start,
                job_type: "=listenerType".to_string(),
                retries: None,
            }],
            Vec::new(),
        )))
        .unwrap();
    let mut all = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let inst = all.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        activation_markers(&all, "charge"),
        vec!["Activating"],
        "a failed first start listener leaves the element ACTIVATING: {all:?}"
    );
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "{incidents:?}");
    let incident_key = incidents[0].key;
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("listenerType".to_string(), Value::Str("ls".into()))]),
        ))
        .unwrap();
    all.extend(
        engine
            .apply_command(Command::resolve_incident(incident_key))
            .unwrap(),
    );
    assert_eq!(activation_markers(&all, "charge"), vec!["Activating"]);
    all.extend(complete_one(&mut engine, "ls"));
    assert_eq!(
        activation_markers(&all, "charge"),
        vec!["Activating", "Activated"],
        "exactly one ElementActivated, written at the drain: {all:?}"
    );
    assert_eq!(engine.activate_jobs("payment", "W", 10, 1_000, 0).len(), 1);
}

#[test]
fn modification_seeded_start_listener_chain_writes_one_activated() {
    // #1429 review: a modification-seeded activation writes `ElementActivated`
    // before opening the start chain; the drain must not write a second one.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_listeners(
            vec![el(ListenerEventType::Start, "ls")],
            Vec::new(),
        )))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    // Drain the original token's chain so only the seeded one remains.
    complete_one(&mut engine, "ls");
    let mut all = engine
        .apply_command(Command::modify_instance(
            inst,
            vec![crate::command::ActivateElementInstruction {
                element_id: "charge".into(),
                variable_instructions: vec![crate::command::VariableInstruction {
                    variables: HashMap::from([("x".to_string(), Value::Int(1))]),
                    scope_id: Some("charge".into()),
                }],
                ..Default::default()
            }],
            vec![],
        ))
        .unwrap();
    let seeded = all
        .iter()
        .find_map(|e| match e {
            Event::ElementActivating {
                element_id,
                element_instance_key,
                ..
            } if element_id == "charge" => Some(*element_instance_key),
            _ => None,
        })
        .unwrap();
    all.extend(complete_one(&mut engine, "ls"));
    let seeded_activated = all
        .iter()
        .filter(|e| {
            matches!(e, Event::ElementActivated { element_instance_key, .. }
                if *element_instance_key == seeded)
        })
        .count();
    assert_eq!(
        seeded_activated, 1,
        "one marker for the seeded token: {all:?}"
    );
}

#[test]
fn compensation_throw_writes_activated_when_its_handler_has_a_start_listener() {
    // #1429 review: a compensation throw's activation carries its handler's
    // records; the handler's start-listener chain must not suppress the
    // THROW's own `ElementActivated`.
    let def = ProcessBuilder::new("comp-listener")
        .start_event("start")
        .service_task("book", "book-job")
        .compensation_boundary_event("comp", "book", "undo")
        .service_task("undo", "undo-job")
        .compensation_throw_event("throw")
        .end_event("done")
        .connect("start", "book")
        .connect("book", "throw")
        .connect("throw", "done")
        .with_listeners("undo", vec![el(ListenerEventType::Start, "ls")], Vec::new())
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("comp-listener"))
        .unwrap();
    let mut all = complete_one(&mut engine, "book-job");
    assert_eq!(
        activation_markers(&all, "throw"),
        vec!["Activating", "Activated"],
        "the throw is ACTIVATED even though its handler's chain opened: {all:?}"
    );
    assert_eq!(activation_markers(&all, "undo"), vec!["Activating"]);
    all.extend(complete_one(&mut engine, "ls"));
    assert_eq!(
        activation_markers(&all, "undo"),
        vec!["Activating", "Activated"]
    );
    assert_eq!(
        activation_markers(&all, "throw"),
        vec!["Activating", "Activated"]
    );
}

#[cfg(feature = "serde")]
#[test]
fn legacy_snapshot_restores_owed_activation_for_a_parked_routing_incident() {
    // #1429 review: a pre-#1429 snapshot carries a split gateway parked on an
    // `ActivationPending` routing incident (raised while ACTIVATING, before its
    // start listeners opened) but no `activating` set — the field did not
    // exist. Restoring must reconstruct the owed marker from that active
    // incident, or resolving + draining the listener chain completes the
    // gateway without ever writing its `ElementActivated`.
    let def = ProcessBuilder::new("split-listener")
        .start_event("s")
        .exclusive_gateway("g")
        .service_task("a", "ja")
        .end_event("e")
        .connect("s", "g")
        .connect_when("g", "a", "amount > 100")
        .connect("a", "e")
        .with_listeners("g", vec![el(ListenerEventType::Start, "ls")], Vec::new())
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(
            "split-listener",
            HashMap::from([("amount".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    let incidents = engine.active_incidents();
    assert_eq!(incidents.len(), 1, "{incidents:?}");
    assert_eq!(
        incidents[0].kind,
        state::IncidentKind::NoMatchingSequenceFlow
    );
    let incident_key = incidents[0].key;

    // Serialize, then strip the `activating` field to emulate a pre-#1429
    // snapshot: the parked gateway owes a marker the legacy bytes do not name.
    let serialized = serde_json::to_value(engine.snapshot()).expect("serializes");
    let mut legacy = serialized.clone();
    for inst in legacy["state"]["instances"]
        .as_object_mut()
        .expect("instances object")
        .values_mut()
    {
        inst.as_object_mut()
            .expect("instance object")
            .remove("activating");
    }
    assert!(
        legacy.to_string().len() < serialized.to_string().len(),
        "the field was present to strip"
    );
    let mut restored =
        Engine::from_snapshot(serde_json::from_value(legacy).expect("legacy snapshot decodes"));

    // The reconstruction under test: the parked gateway's owed marker is rebuilt
    // from its active `ActivationPending` incident even though the legacy bytes
    // carried no `activating` set.
    let inst = restored
        .state()
        .instances
        .keys()
        .next()
        .copied()
        .expect("one live instance");
    let gw = restored.state().instances[&inst]
        .active
        .iter()
        .find(|(_, id)| id.as_str() == "g")
        .map(|(k, _)| *k)
        .expect("the gateway token is still live");
    assert!(
        restored.state().instances[&inst].activating.contains(&gw),
        "restore reconstructs the owed activation from the parked incident"
    );

    // Resolve (routing now matches) and drain the start-listener chain: the
    // gateway must get exactly one `ElementActivated`, at the drain.
    restored
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("amount".to_string(), Value::Int(500))]),
        ))
        .unwrap();
    let mut all = restored
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    all.extend(complete_one(&mut restored, "ls"));
    assert_eq!(
        activation_markers(&all, "g"),
        vec!["Activated"],
        "the restored legacy gateway gets exactly one ElementActivated, at the drain: {all:?}"
    );
    all.extend(complete_one(&mut restored, "ja"));
    assert!(
        restored.is_completed(inst),
        "the token routed to the end: {all:?}"
    );
}
