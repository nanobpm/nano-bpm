//! `adhoc` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

/// Issue #1170 — regression guard, ad-hoc (JOB_WORKER) multi-instance child.
///
/// Each MI child is a JOB_WORKER `adHocSubProcess`: it mints an agent job and
/// registers its own ad-hoc runtime record, into which the agent can activate
/// tools. The interrupting boundary on the body must tear down every child's
/// ad-hoc scope (its agent job, activated tools and their inner instances),
/// clear both the ad-hoc records and the body record, and route the boundary
/// flow once.
#[test]
fn interrupting_boundary_on_a_multi_instance_adhoc_tears_down_every_container() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-agent" />
              <zeebe:adHoc outputCollection="r" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:userTask id="InnerTask">
              <bpmn:extensionElements>
                <zeebe:userTask />
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:adHocSubProcess>
          <bpmn:boundaryEvent id="Bnd" attachedToRef="Host">
            <bpmn:messageEventDefinition messageRef="M" />
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="EndNormal" />
          <bpmn:endEvent id="EndInterrupted" />
          <bpmn:sequenceFlow id="F1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="F2" sourceRef="Host" targetRef="EndNormal" />
          <bpmn:sequenceFlow id="F3" sourceRef="Bnd" targetRef="EndInterrupted" />
        </bpmn:process>
        <bpmn:message id="M" name="probe-cancel">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=customerId" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[
                ("customerId", Value::Str("C1".into())),
                ("items", Value::List(vec![Value::Int(1), Value::Int(2)])),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Each MI child is its own ad-hoc container with its own agent job.
    let agents = engine.activate_jobs("probe-agent", "W", 10, 1_000, 0);
    assert_eq!(
        agents.len(),
        2,
        "each MI ad-hoc child minted its own agent job"
    );
    assert_eq!(engine.instance(inst).unwrap().multi_instances.len(), 1);
    // One container activates an inner user-task tool into its own scope.
    let container = agents[0].element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agents[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("InnerTask")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the tool is active in its container before the boundary fires"
    );
    assert!(!engine.is_completed(inst));

    // The cancel message fires the interrupting boundary on the whole loop.
    let fired = engine.correlate_message("probe-cancel", "C1", HashMap::new(), 0);
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(
                e,
                Event::SequenceFlowTaken { from, to, .. } if from == "Bnd" && to == "EndInterrupted"
            ))
            .count(),
        1,
        "the boundary flow is taken exactly once"
    );
    assert!(
        engine.is_completed(inst),
        "the instance reaches EndInterrupted and completes (#1170)"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().multi_instances.is_empty(),
        "the multi-instance runtime record is cleared (no stale active set)"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().adhoc_instances.is_empty(),
        "every ad-hoc container record was cleared on cancel"
    );
    assert!(
        engine
            .state()
            .user_tasks
            .values()
            .all(|t| t.element_id != "InnerTask" || t.state != state::UserTaskState::Created),
        "the activated tool's open user task was cancelled, not left Created"
    );
}

/// Issue #1170 regression — an EARLY multi-instance completion (a satisfied
/// completion condition, not an interrupting boundary) must tear down a still-
/// active AD-HOC container child container-aware, exactly like the boundary
/// path. `complete_multi_instance_body` cancels the remaining active children
/// via `cancel_mi_child_events`, which is LEAF-only: for a JOB_WORKER ad-hoc MI
/// child it would cancel the agent job but orphan the child's activated tool
/// (its open user task / tool jobs) and leave the ad-hoc runtime record behind
/// with no `AdHocCompleted`. Assert the cancelled container emits
/// `AdHocCompleted { cancelled: true }`, its activated tool's user task is
/// cancelled (not left `Created`), and no ad-hoc record leaks.
#[test]
fn early_multi_instance_completion_tears_down_an_active_adhoc_tool_child() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-agent" />
              <zeebe:adHoc outputCollection="r" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
              <bpmn:completionCondition>=true</bpmn:completionCondition>
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:userTask id="InnerTask">
              <bpmn:extensionElements>
                <zeebe:userTask />
              </bpmn:extensionElements>
            </bpmn:userTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="f2" sourceRef="Host" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Each MI child is its own ad-hoc container with its own agent job.
    let agents = engine.activate_jobs("probe-agent", "W", 10, 1_000, 0);
    assert_eq!(
        agents.len(),
        2,
        "each MI ad-hoc child minted its own agent job"
    );
    // Container A activates an inner user-task tool into its own scope; it is now
    // an active tool the leaf cancel path would orphan.
    let container_a = agents[0].element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agents[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("InnerTask")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container_a)
            .unwrap()
            .active
            .len(),
        1,
        "container A holds an active tool before the early completion"
    );

    // Container B finishes its agent turn with no tools -> it completes, which
    // completes MI child B and satisfies the completion condition (`=true`),
    // triggering the early cancel of the still-active container A.
    let fired = engine
        .apply_command(Command::complete_job_with_result(
            agents[1].key,
            HashMap::new(),
            crate::model::AdHocJobResult::default(),
        ))
        .unwrap();
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::AdHocCompleted { container_key, cancelled: true, .. } if *container_key == container_a
        )),
        "the early-cancelled ad-hoc container emits AdHocCompleted{{cancelled:true}}, not a leaked record"
    );
    assert!(
        engine.is_completed(inst),
        "the loop completes early and the instance drains"
    );
    assert!(
        engine.instance(inst).is_none()
            || (engine.instance(inst).unwrap().multi_instances.is_empty()
                && engine.instance(inst).unwrap().adhoc_instances.is_empty()),
        "no stale multi-instance or ad-hoc runtime records remain after early completion"
    );
    assert!(
        engine
            .state()
            .user_tasks
            .values()
            .all(|t| t.element_id != "InnerTask" || t.state != state::UserTaskState::Created),
        "container A's activated tool user task was cancelled, not left Created"
    );
}

/// Issue #1170 regression — the NORMAL completion of a multi-instance JOB_WORKER
/// ad-hoc container must drain the loop, and each child must seed its own
/// `outputCollection`. This PR taught `run_mi_child_behaviour` to register an
/// ad-hoc runtime record per loop item; without a matching completion route the
/// container's agent turn completes via `complete_adhoc_container`, which takes
/// the outgoing flow and never emits `MultiInstanceChildCompleted` — hanging the
/// loop. It also never seeded the child's `outputCollection`, so tool results
/// would be silently dropped. Assert the seed reaches the tool job and the loop
/// drains to completion.
#[test]
fn multi_instance_adhoc_child_completes_normally_and_seeds_its_output_collection() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="f2" sourceRef="Host" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Each MI child is its own ad-hoc container with its own agent job.
    let agents = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    assert_eq!(
        agents.len(),
        2,
        "each MI ad-hoc child minted its own agent job"
    );
    assert_eq!(engine.instance(inst).unwrap().multi_instances.len(), 1);

    // Each container activates its tool. The tool job must inherit the child
    // container's seeded (empty) `outputCollection` — proving the seed is present
    // in the MI child scope, without which tool results would be dropped.
    for agent in &agents {
        engine
            .apply_command(Command::complete_job_with_result(
                agent.key,
                HashMap::new(),
                crate::model::AdHocJobResult {
                    activate_elements: vec![activate_element("toolA")],
                    ..Default::default()
                },
            ))
            .unwrap();
    }
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 2, "one tool per MI ad-hoc child");
    assert!(
        tool_jobs
            .iter()
            .all(|j| j.variables.get("results") == Some(&Value::List(Vec::new()))),
        "each MI ad-hoc child seeded its own outputCollection `results: []` (#1170)"
    );

    // Complete each tool with a `result`, then let each agent turn finish (no more
    // tools) so its container completes.
    for job in &tool_jobs {
        engine
            .apply_command(Command::complete_job_with_result(
                job.key,
                HashMap::from([("result".to_string(), Value::Int(7))]),
                crate::model::AdHocJobResult::default(),
            ))
            .unwrap();
    }
    let agents2 = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    for agent in &agents2 {
        engine
            .apply_command(Command::complete_job_with_result(
                agent.key,
                HashMap::new(),
                crate::model::AdHocJobResult::default(),
            ))
            .unwrap();
    }

    assert!(
        engine.is_completed(inst),
        "every MI ad-hoc child completes and the loop drains to completion (#1170)"
    );
    assert!(
        engine.instance(inst).is_none()
            || (engine.instance(inst).unwrap().multi_instances.is_empty()
                && engine.instance(inst).unwrap().adhoc_instances.is_empty()),
        "no stale multi-instance or ad-hoc runtime records remain"
    );
}

/// Issue #1175 (gap 1) — a DECLARATIVE (BPMN_TASK / `activeElementsCollection`)
/// ad-hoc container used as a multi-instance child must evaluate its
/// active-elements collection and activate those inner elements — NOT mint a
/// stray agent job. Because an ad-hoc sub-process flattens to
/// `ElementKind::ServiceTask`, `run_mi_child_behaviour` used to unconditionally
/// mint a job for every ad-hoc MI child, so a declarative container as an MI
/// child minted a phantom container job and never ran its collection. Assert the
/// branch activates the collection's inner tools directly (no container job) and
/// the loop drains back through `complete_mi_child`.
#[test]
fn multi_instance_declarative_adhoc_child_activates_its_collection_without_a_job() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:adHoc activeElementsCollection="=tools" outputCollection="results" outputElement="=results" />
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="f2" sourceRef="Host" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[
                ("items", Value::List(vec![Value::Int(1), Value::Int(2)])),
                ("tools", Value::List(vec![Value::Str("toolA".to_string())])),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // A declarative container mints NO agent/container job. On `main`, each MI
    // child would mint a phantom job (job type = the container element id "Host")
    // and never activate its collection.
    assert!(
        engine.activate_jobs("Host", "W", 10, 1_000, 0).is_empty(),
        "a declarative ad-hoc MI child mints no container job"
    );
    // Instead each of the two children activated its collection's `toolA`
    // directly — two real tool jobs, one per MI child.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(
        tool_jobs.len(),
        2,
        "each declarative MI child activated its active-elements collection (gap 1)"
    );
    assert_eq!(engine.instance(inst).unwrap().multi_instances.len(), 1);

    // Each tool completes with a `result`; the container aggregates it into its
    // `results` outputCollection, the container drains, and its MI outputElement
    // `=results` feeds the loop — draining the whole MI back through
    // `complete_mi_child`.
    for job in &tool_jobs {
        engine
            .apply_command(Command::complete_job_with(
                job.key,
                HashMap::from([("result".to_string(), Value::Str("x".to_string()))]),
            ))
            .unwrap();
    }

    assert!(
        engine.is_completed(inst),
        "every declarative MI ad-hoc child drains and the loop completes (gap 1)"
    );
    assert!(
        engine.instance(inst).is_none()
            || (engine.instance(inst).unwrap().multi_instances.is_empty()
                && engine.instance(inst).unwrap().adhoc_instances.is_empty()),
        "no stale multi-instance or ad-hoc runtime records remain"
    );
}

/// Issue #1175 (gap 4) — an ad-hoc MI child must seed its `outputCollection`
/// BEFORE its per-child input mappings run, mirroring the normal activation
/// path. The seed used to be emitted AFTER the mappings, so a (nonsensical but
/// possible) per-child `zeebe:input` targeting the outputCollection variable was
/// clobbered by the empty seed instead of surviving.
#[test]
fn multi_instance_adhoc_child_seeds_output_collection_before_input_mappings() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="r" outputElement="=result" />
              <zeebe:ioMapping>
                <zeebe:input source="=[item]" target="r" />
              </zeebe:ioMapping>
            </bpmn:extensionElements>
            <bpmn:multiInstanceLoopCharacteristics>
              <zeebe:loopCharacteristics inputCollection="=items" inputElement="item" />
            </bpmn:multiInstanceLoopCharacteristics>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="f2" sourceRef="Host" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("items", Value::List(vec![Value::Int(1)]))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The container's agent job carries the child scope, in which the
    // outputCollection `r` must reflect the per-child input mapping (`[item]` =
    // `[1]`) rather than the empty seed — proving the seed ran BEFORE the mapping.
    // On `main` the seed ran after the mapping and clobbered it to `[]`.
    let agents = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    assert_eq!(
        agents.len(),
        1,
        "the single MI ad-hoc child minted its agent job"
    );
    assert_eq!(
        agents[0].variables.get("r"),
        Some(&Value::List(vec![Value::Int(1)])),
        "the per-child input mapping to the outputCollection survived the seed (gap 4)"
    );
    let _ = inst;
}

#[test]
fn top_level_terminate_end_clears_an_adhoc_container() {
    // The ad-hoc analogue of the multi-instance case: a top-level terminate must
    // drop the ad-hoc container runtime record on the terminal (COMPLETED) transition.
    //
    //  s -> split =< agent(adhoc, resting) , trigger(svc) -> stop(terminate) >
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:parallelGateway id="split" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:serviceTask id="trigger">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="trigger-job" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="agent_end" />
          <bpmn:endEvent id="stop"><bpmn:terminateEventDefinition /></bpmn:endEvent>
          <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="split" />
          <bpmn:sequenceFlow id="f2" sourceRef="split" targetRef="agent" />
          <bpmn:sequenceFlow id="f3" sourceRef="split" targetRef="trigger" />
          <bpmn:sequenceFlow id="f4" sourceRef="agent" targetRef="agent_end" />
          <bpmn:sequenceFlow id="f5" sourceRef="trigger" targetRef="stop" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let proc = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine.apply_command(Command::create_instance("p")).unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    // The ad-hoc container registered a runtime record on activation and rests
    // waiting for its `agent-worker` job.
    assert_eq!(engine.instance(key).unwrap().adhoc_instances.len(), 1);

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(
        engine.instance(key).unwrap().adhoc_instances.is_empty(),
        "terminal teardown must clear the ad-hoc container runtime record"
    );
}

#[test]
fn adhoc_tool_input_mapping_failure_raises_io_mapping_and_reactivates() {
    // #946 item 1 (ad-hoc tool input): a tool's own input `zeebe:ioMapping` that
    // fails to evaluate must raise `IO_MAPPING_ERROR` on the container and NOT
    // create the tool child (no skip-to-`Complete`). Resolution re-drives the
    // tool's *activation* (`AdHocToolActivation`), re-applying the now-fixed input
    // and minting the tool job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_with_tool_input_mapping()))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("base", Value::Str("oops".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    // Activate the tool: its input mapping `=base + 1` fails (string + int).
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "expected one active incident: {active:?}");
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(
        matches!(
            active[0].redrive,
            Some(state::IoMappingRedrive::AdHocToolActivation { .. })
        ),
        "an ad-hoc tool input failure re-drives the tool activation, got {:?}",
        active[0].redrive
    );
    assert_eq!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).len(),
        0,
        "no tool job while the tool is parked on the input-mapping incident"
    );
    let incident_key = engine.incidents()[0].key;

    // Fix `base` and resolve: the tool re-activates, its input maps
    // (`weighted = 5 + 1 = 6`), and its job is minted.
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([("base".to_string(), Value::Int(5))]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    assert!(engine.instance(inst).unwrap().incidents.is_empty());
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 1, "the tool job is minted on resolution");
    assert_eq!(tool_jobs[0].variables.get("weighted"), Some(&Value::Int(6)));
}

/// Regression for nanobpm/nano-bpm#1200: a tool input `zeebe:ioMapping` authored
/// the standard Camunda agentic way — `=fromAi(toolCall.foo, "desc", "string")`
/// — must evaluate (returning the LLM-supplied value unchanged) instead of
/// raising an `IO_MAPPING_ERROR` incident. `fromAi` is a Camunda FEEL built-in
/// the engine previously lacked, so calling it produced `cannot call a null
/// value`. The activation carries `toolCall.foo`; the mapped `mapped` target
/// must receive that value and no incident must be raised.
#[test]
fn adhoc_tool_fromai_input_mapping_returns_llm_value_without_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_fromai_tool_input_mapping(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    // Activate the tool with the LLM's answer arriving as `toolCall.foo`.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element_with(
                    "toolA",
                    &[(
                        "toolCall",
                        Value::Map(std::collections::BTreeMap::from([(
                            "foo".to_string(),
                            Value::Str("VALUE-FROM-LLM".into()),
                        )])),
                    )],
                )],
                ..Default::default()
            },
        ))
        .unwrap();

    assert!(
        engine.active_incidents().is_empty(),
        "fromAi(...) must evaluate — no IO_MAPPING_ERROR incident: {:?}",
        engine.active_incidents()
    );
    assert!(engine.instance(inst).unwrap().incidents.is_empty());

    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 1, "the tool job is minted");
    assert_eq!(
        tool_jobs[0].variables.get("mapped"),
        Some(&Value::Str("VALUE-FROM-LLM".into())),
        "fromAi returns its first argument unchanged"
    );
}

/// Regression for Magikcraft/nano-bpm#605: an agent's
/// `activateElements[{ elementId, variables }]` must scope those `variables`
/// into the activated tool's OWN job — including tools that declare no
/// `ioMapping` (the `toolA`/`toolB` fixture tools have none). The reported bug
/// dropped the seed variables entirely, so a no-mapping tool saw only the
/// instance-root scope. This asserts the whole defect class: a plain tool AND a
/// second tool activated in the same turn each receive only their own seed
/// variables, with no cross-contamination.
#[test]
fn adhoc_activation_variables_reach_a_tool_job_without_io_mapping() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    // Turn 1: activate both no-ioMapping tools, each with a distinct seed var.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![
                    activate_element_with("toolA", &[("fromActivation", Value::Str("A".into()))]),
                    activate_element_with("toolB", &[("fromActivation", Value::Str("B".into()))]),
                ],
                ..Default::default()
            },
        ))
        .unwrap();

    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(
        tool_jobs.len(),
        2,
        "exactly the two activated tools emit jobs — no duplicates or extras (#605)"
    );
    let tool_a = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let tool_b = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolB")
        .expect("toolB job emitted");

    // Gaps #2 and #8 (#614) both write LOCAL container-scope variables on
    // activation — the tool catalog `adHocSubProcessElements` (gap #2) and the
    // seeded `outputCollection` `results: []` (gap #8). Activated tools are
    // children of that scope, so — like Camunda's scope inheritance — each tool
    // job legitimately inherits BOTH alongside its own activation seed. Assert
    // the FULL scope exactly (seed + inherited catalog + inherited empty
    // collection): this still guards #605's defect class, since any root
    // bleed-through or a sibling tool's seed would break equality.
    let expected_catalog = || {
        Value::List(vec![
            Value::Map(std::collections::BTreeMap::from([
                ("elementId".to_string(), Value::Str("toolA".into())),
                ("elementName".to_string(), Value::Str(String::new())),
            ])),
            Value::Map(std::collections::BTreeMap::from([
                ("elementId".to_string(), Value::Str("toolB".into())),
                ("elementName".to_string(), Value::Str(String::new())),
            ])),
        ])
    };
    assert_eq!(
        *tool_a.variables,
        HashMap::from([
            ("fromActivation".to_string(), Value::Str("A".into())),
            ("adHocSubProcessElements".to_string(), expected_catalog()),
            ("results".to_string(), Value::List(vec![])),
        ]),
        "toolA's job scope is its own activation seed PLUS the inherited \
         container catalog (gap #2) and empty outputCollection (gap #8) — \
         reaches the job with no ioMapping, and \
         carries no root bleed-through or toolB seed (#605)"
    );
    assert_eq!(
        *tool_b.variables,
        HashMap::from([
            ("fromActivation".to_string(), Value::Str("B".into())),
            ("adHocSubProcessElements".to_string(), expected_catalog()),
            ("results".to_string(), Value::List(vec![])),
        ]),
        "toolB's job scope is its own activation seed PLUS the inherited \
         container catalog (gap #2) and empty outputCollection (gap #8) — \
         no cross-contamination from toolA's \
         concurrently-activated seed (#605)"
    );

    // The exact failure mode #605 describes: a seed merged into the shared
    // instance-root scope would still satisfy the per-job assertions above while
    // reintroducing the global-namespace leak. Guard it directly — the seeds
    // must stay scoped to their tool jobs and never surface at the root.
    assert_eq!(
        io_var(&engine, inst, "fromActivation"),
        None,
        "activation seeds must stay scoped to their tool jobs, never merged \
         into the instance-root scope (#605)"
    );

    assert!(
        !engine.is_completed(inst),
        "container still parked while the seeded tools run"
    );
}

/// Regression for Magikcraft/nano-bpm#614 gap 3 (Zeebe parity): the external
/// "activate ad-hoc activities" command
/// (`AdHocSubProcessInstructionActivateProcessor` — REST
/// `POST /element-instances/ad-hoc-activities/{key}/activation`) activates named
/// tools on an already-running ad-hoc container WITHOUT completing its agent
/// job, driving the same activation machinery the agent-job path does — and
/// rejecting a bad instruction identically. This asserts the whole defect
/// class: an unknown container key is NOT_FOUND, an unknown element id is
/// NOT_FOUND (leaving the container untouched), a valid target activates its
/// tool without consuming the agent job, and `cancelRemainingInstances`
/// completes the container.
#[test]
fn adhoc_external_command_activates_tools_without_the_agent_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    // Discover the container's element-instance key via its agent job, but do
    // NOT complete the job — the external command activates independently, and
    // both seams must coexist.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // An `adHocSubProcessInstanceKey` that identifies no active container is
    // rejected NOT_FOUND.
    let missing = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: 999_999,
            activate_elements: vec![activate_element("toolA")],
            cancel_remaining: false,
        })
        .unwrap_err();
    assert!(
        matches!(missing, EngineError::AdHocSubProcessNotFound { .. }),
        "an unknown container key must be rejected (NOT_FOUND parity), got {missing:?}"
    );

    // An unknown element id is rejected NOT_FOUND — identically to the agent-job
    // path — and leaves the container untouched (no phantom child).
    let ghost = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: vec![activate_element("ghost")],
            cancel_remaining: false,
        })
        .unwrap_err();
    assert!(
        matches!(&ghost, EngineError::AdHocUnknownElement { element_id, .. } if element_id == "ghost"),
        "an unknown element id must be rejected (NOT_FOUND parity), got {ghost:?}"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        0,
        "a rejected external activation leaves the container untouched"
    );

    // Happy path: activating a real tool mints its job and marks it active,
    // WITHOUT consuming the agent job (the container is still running).
    let events = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: vec![activate_element("toolA")],
            cancel_remaining: false,
        })
        .expect("external activation of a real tool succeeds");
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the external command activated `toolA`"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::JobCreated { element_id, .. } if element_id == "toolA")),
        "the activated tool's own job was created"
    );
    assert!(
        !engine.is_completed(inst),
        "the container is still running — the external command did not complete the agent job"
    );

    // `cancelRemainingInstances` cancels the in-flight tool and completes the
    // container (which then flows to the end event, completing the instance).
    engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .expect("cancel-remaining completes the container");
    assert!(
        engine.is_completed(inst),
        "cancelRemainingInstances completed the container and the instance"
    );
}

/// Regression for Magikcraft/nano-bpm#614 gap 3: the external "activate ad-hoc
/// activities" command must NOT let an empty request (`elements: []` with
/// `cancelRemainingInstances = false`) implicitly complete a parked container.
/// The REST schema permits an empty `elements` array, so without this guard a
/// client could accidentally finish a running instance by POSTing `{}`. The
/// command rejects it as INVALID_ARGUMENT (mapped to HTTP 400) and leaves the
/// container running; completion is only expressible via
/// `cancelRemainingInstances`. (The agent-job completion seam — gap 4 — is
/// unaffected: activating nothing there still ends the turn.)
#[test]
fn adhoc_external_command_rejects_empty_activation() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Empty elements with no cancel is rejected as INVALID_ARGUMENT — it must
    // not complete the parked container.
    let empty = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: Vec::new(),
            cancel_remaining: false,
        })
        .unwrap_err();
    assert!(
        matches!(&empty, EngineError::AdHocNoActivationTargets { ad_hoc_instance_key } if *ad_hoc_instance_key == container),
        "an empty activation with no cancel must be rejected (INVALID_ARGUMENT), got {empty:?}"
    );
    assert!(
        !engine.is_completed(inst),
        "a rejected empty activation must leave the container running, not complete it"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        0,
        "a rejected empty activation activates nothing"
    );
}

/// Gap #2 (issue #614): a JOB_WORKER ad-hoc container must advertise its tool
/// catalog to the agent by writing a local variable `adHocSubProcessElements`
/// (Camunda `AdHocSubProcessProcessor.onActivate` →
/// `AD_HOC_SUB_PROCESS_ELEMENTS`) so the agent can discover which tools it may
/// activate — a list of `{ elementId, elementName }` entries, one per catalog
/// tool, in document order. On `main` the agent job carries no such variable.
/// Class-scoped: asserts the variable is present on the agent job, is a list of
/// exactly the container's tools, and carries each tool's id and human name.
#[test]
fn adhoc_container_advertises_its_tool_catalog_on_the_agent_job() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_named_tools_process()))
        .unwrap();
    let _inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    let catalog = agent
        .variables
        .get("adHocSubProcessElements")
        .expect("the agent job advertises its tool catalog (#614 gap 2)");
    let entries = match catalog {
        Value::List(items) => items,
        other => panic!("expected a list of tool metadata, got {other:?}"),
    };
    assert_eq!(
        entries.len(),
        2,
        "one catalog entry per activatable tool, in document order"
    );

    let entry = |elem_id: &str, name: &str| {
        Value::Map(
            [
                ("elementId".to_string(), Value::Str(elem_id.to_string())),
                ("elementName".to_string(), Value::Str(name.to_string())),
            ]
            .into_iter()
            .collect(),
        )
    };
    assert_eq!(
        entries,
        &vec![
            entry("toolA", "Search the web"),
            entry("toolB", "Send an email"),
        ],
        "each entry carries the tool's id and human name"
    );
}

/// Regression for the tool-catalog filter: the parser captures known
/// non-activatable inner nodes (e.g. gateways) in `def.tools` as
/// `AdHocToolKind::Other`, but the advertised `adHocSubProcessElements` catalog
/// must only surface activatable tools (Zeebe parity). The inner exclusive
/// gateway below must NOT appear in the advertised list.
#[test]
fn adhoc_container_advertised_catalog_excludes_non_activatable_nodes() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="agent-worker" />
              <zeebe:adHoc outputCollection="results" outputElement="=result" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="toolA" name="Search the web">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:exclusiveGateway id="gw" name="Not a tool" />
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
    let _inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    let catalog = agent
        .variables
        .get("adHocSubProcessElements")
        .expect("the agent job advertises its tool catalog (#614 gap 2)");
    let entries = match catalog {
        Value::List(items) => items,
        other => panic!("expected a list of tool metadata, got {other:?}"),
    };

    let entry = |elem_id: &str, name: &str| {
        Value::Map(
            [
                ("elementId".to_string(), Value::Str(elem_id.to_string())),
                ("elementName".to_string(), Value::Str(name.to_string())),
            ]
            .into_iter()
            .collect(),
        )
    };
    assert_eq!(
        entries,
        &vec![entry("toolA", "Search the web")],
        "only the activatable service task is advertised; the gateway (Other) is excluded"
    );
}

#[test]
fn adhoc_agent_activates_tools_loops_and_completes_with_output_collection() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // The container activated and emitted its agent job on the container element
    // instance; its runtime state is registered but no tools are active yet.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        0,
        "no tools active before the first agent turn"
    );

    // Turn 1: the agent activates both tools. The engine instantiates them as
    // real element instances (each a `tool` job) inside the container scope.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        2,
        "both tools active after the agent's activate-element instruction"
    );
    assert!(
        !engine.is_completed(inst),
        "container parks while tools run"
    );

    // Drain the two tool jobs, each producing a `result`. The container captures
    // each via `outputElement`; when the last drains, the agent job re-emits.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 2, "both tools produced jobs");
    for (job, val) in tool_jobs.iter().zip(["A", "B"]) {
        let mut vars = HashMap::new();
        vars.insert("result".to_string(), Value::Str(val.to_string()));
        engine
            .apply_command(Command::complete_job_with(job.key, vars))
            .unwrap();
    }
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(adhoc.active.len(), 0, "all tools drained");
    assert_eq!(
        adhoc.iterations, 1,
        "agent job re-emitted for the next turn"
    );
    assert_eq!(
        container_output_collection(&engine, inst, container, "results").map(|v| match v {
            Value::List(l) => l.len(),
            _ => usize::MAX,
        }),
        Some(2),
        "two tool outputs accumulated in the live outputCollection",
    );
    assert!(
        !engine.is_completed(inst),
        "container still awaiting the agent"
    );

    // Turn 2: the agent returns no activations (it is done) → the container
    // completes, writes its `outputCollection`, and takes its outgoing flow to
    // the end event, completing the instance.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted");
    // The container writes its `outputCollection` as it completes; a terminal
    // instance drops its variable payload (ADR 0012), so we read the collection
    // off the emitted `VariablesUpdated` rather than hot state.
    let final_events = engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();

    assert!(
        engine.is_completed(inst),
        "container completed → instance done"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    let mut got = match results {
        Some(Value::List(v)) => v,
        other => panic!("expected outputCollection list, got {other:?}"),
    };
    got.sort_by_key(|v| match v {
        Value::Str(s) => s.clone(),
        _ => String::new(),
    });
    assert_eq!(
        got,
        vec![Value::Str("A".to_string()), Value::Str("B".to_string())],
        "outputCollection holds both tool results"
    );
}

/// Gap #9 (issue #614): each activated ad-hoc tool must run beneath a dedicated
/// `AD_HOC_SUB_PROCESS_INNER_INSTANCE` element (id `<container>#innerInstance`),
/// exactly as Zeebe's `BpmnAdHocSubProcessBehavior.createInnerInstance` nests
/// `container → innerInstance → tool`. On `main` nano activates the tool child
/// directly under the container (`scopes[child] == container`), so the
/// read-model element-instance tree is one level too shallow and cannot be
/// migrated/queried like a Zeebe process. This asserts the whole defect class:
/// (a) two tools activated in one turn each get their OWN inner instance whose
/// scope is the container and whose element id is the inner-instance id;
/// (b) the tool child's scope is its inner instance (not the container); and
/// (c) completing a tool tears its inner instance down too, leaving no dangling
/// element instance in the read model.
#[test]
fn adhoc_tools_run_under_a_dedicated_inner_instance_element() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: activate both tools.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();

    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 2, "both tools produced jobs");

    for job in &tool_jobs {
        let child = job.element_instance_key;
        // (b) the tool child hangs off an inner instance, NOT the container.
        let inner = engine.instance(inst).unwrap().scopes.get(&child).copied();
        let inner = inner.expect("tool child has a read-model scope");
        assert_ne!(
            inner, container,
            "tool child's scope is a dedicated inner instance, not the container"
        );
        // (a) the inner instance is scoped to the container and carries the
        // `<container>#innerInstance` element id.
        assert_eq!(
            engine.instance(inst).unwrap().scopes.get(&inner).copied(),
            Some(container),
            "inner instance is scoped to the container"
        );
        assert_eq!(
            engine.instance(inst).unwrap().active.get(&inner).cloned(),
            Some("agent#innerInstance".to_string()),
            "inner instance carries the ad-hoc inner-instance element id"
        );
    }

    // Each tool got its OWN inner instance (no sharing).
    let inners: Vec<Key> = tool_jobs
        .iter()
        .map(|j| {
            engine
                .instance(inst)
                .unwrap()
                .scopes
                .get(&j.element_instance_key)
                .copied()
                .unwrap()
        })
        .collect();
    assert_ne!(
        inners[0], inners[1],
        "each tool gets its own inner instance"
    );

    // (c) completing a tool tears its inner instance down — no dangling
    // element instance survives in the read model.
    let first = tool_jobs[0].element_instance_key;
    let first_inner = inners[0];
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("A".to_string()));
    engine
        .apply_command(Command::complete_job_with(tool_jobs[0].key, vars))
        .unwrap();
    let live = engine.instance(inst).unwrap();
    assert!(
        !live.active.contains_key(&first_inner),
        "the completed tool's inner instance is torn down"
    );
    assert!(
        !live.active.contains_key(&first),
        "the completed tool child is torn down"
    );
    assert!(
        !live.scopes.contains_key(&first_inner),
        "the inner instance leaves no read-model scope entry behind"
    );
}

/// Gap #1 (issue #614): a native **user-task** tool must execute, not silently
/// pass through. Activating one has to create a real user task and PARK the
/// child inside the container scope until it is completed via `CompleteUserTask`
/// — mirroring an ordinary user-task activation. On `main` the non-service-task
/// kinds fall into `activate_adhoc_tool`'s `None` arm and are short-circuited to
/// immediate completion, so no user task is ever created and the child never
/// parks. This asserts the whole defect class: the tool child stays active with
/// a real user task, the container keeps looping, and the human's completion
/// output flows through the container's `outputElement` exactly like a
/// service-task tool's job output.
// zeebe-cells: element:AdHocSubProcess
#[test]
fn adhoc_agent_activates_a_user_task_tool_and_parks_until_completed() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_with_user_task_tool()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: the agent activates the user-task tool. It must create a user task
    // and keep the child ACTIVE — not auto-complete it.
    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("ask")],
                ..Default::default()
            },
        ))
        .unwrap();

    let user_task_key = activated
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                assignee,
                ..
            } if element_id == "ask" => {
                assert_eq!(
                    assignee.as_deref(),
                    Some("alice"),
                    "the tool's static assignee is resolved on activation"
                );
                Some(*user_task_key)
            }
            _ => None,
        })
        .expect("activating a user-task tool creates a real user task (#614 gap 1)");

    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the user-task tool child stays active — it must not auto-complete"
    );
    assert!(
        !engine.is_completed(inst),
        "container parks while the human task is open"
    );

    // Complete the human task with an output; it flows through the container's
    // `outputElement` and, as the last active tool, re-emits the agent job.
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("approved".to_string()));
    engine
        .apply_command(Command::complete_user_task_with(user_task_key, vars))
        .unwrap();

    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the user-task tool drained on completion"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "agent job re-emitted for the next turn after the human finished"
    );
    assert!(
        matches!(
            container_output_collection(&engine, inst, container, "results"),
            Some(Value::List(ref v)) if v.len() == 1
        ),
        "the human's `result` was appended to the container's outputCollection \
         (gap #8 lifecycle: accumulated into the local scope variable, visible \
         mid-run), got {:?}",
        container_output_collection(&engine, inst, container, "results")
    );

    // Turn 2: agent is done → container completes and writes its collection.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted");
    let final_events = engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "container completed → instance done"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("approved".to_string())])),
        "the user-task tool's output reached the container's outputCollection"
    );
}

#[test]
fn interrupting_boundary_on_adhoc_cancels_an_activated_tool() {
    // Issue #1155: an interrupting boundary event on an `adHocSubProcess` must
    // terminate the attached activity AND everything inside its scope —
    // including an activated tool and its open user task — then take its own
    // outgoing flow. Before the fix the boundary fired but left the activated
    // tool + its `#innerInstance` active, so the instance hung `Active` forever.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_interrupting_message_boundary(),
        ))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("customerId", Value::Str("C1".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The agent activates the inner user-task tool: it stays active, the
    // boundary subscription is open, and the instance parks.
    let agent = engine
        .activate_jobs("probe-agent", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "Host")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("InnerTask")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the user-task tool is active before the boundary fires"
    );
    assert!(!engine.is_completed(inst));

    // The cancel message arrives: the interrupting boundary fires and must tear
    // down the whole ad-hoc scope, then route its own outgoing flow to
    // `EndInterrupted`.
    let fired = engine.correlate_message("probe-cancel", "C1", HashMap::new(), 0);
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "Bnd" && to == "EndInterrupted"
        )),
        "the boundary event fires and takes its outgoing flow"
    );

    assert!(
        engine.is_completed(inst),
        "the instance reaches EndInterrupted and completes"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine
                .instance(inst)
                .unwrap()
                .active
                .values()
                .all(|id| id != "InnerTask"
                    && !id.ends_with(crate::engine::ADHOC_INNER_INSTANCE_ID_POSTFIX)),
        "the activated tool and its #innerInstance were torn down, not orphaned"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().adhoc_instances.is_empty(),
        "the ad-hoc container's runtime record was cleared on cancel"
    );
    assert!(
        engine
            .state()
            .user_tasks
            .values()
            .all(|t| t.element_id != "InnerTask" || t.state != state::UserTaskState::Created),
        "the open user task was cancelled, not left Created"
    );
}

#[test]
fn interrupting_boundary_on_adhoc_cancels_an_in_flight_agent_job() {
    // Issue #1155: the interrupting-boundary teardown must also cancel the
    // container's OWN agent job when it is still in flight — i.e. the boundary
    // fires while the ad-hoc worker is mid-turn (its job activated but not yet
    // completed). This exercises the `active_job_on(container)` /
    // `Event::JobCanceled` branch in `interrupt_activity_via_boundary`, which
    // the tool-teardown test above never reaches because it completes the agent
    // job before correlating (so `active_job_on` returns `None` there).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_interrupting_message_boundary(),
        ))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("customerId", Value::Str("C1".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The agent job is activated (a worker picked it up) but NOT completed — the
    // container is mid-investigation with an in-flight job.
    let agent = engine
        .activate_jobs("probe-agent", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "Host")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;
    assert!(!engine.is_completed(inst));

    // The cancel message arrives while the agent job is still in flight: the
    // interrupting boundary must cancel that job (not leave it activated) and
    // route its own outgoing flow to `EndInterrupted`.
    let fired = engine.correlate_message("probe-cancel", "C1", HashMap::new(), 0);
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::JobCanceled { job_key, .. } if *job_key == agent.key
        )),
        "the in-flight agent job is canceled when the boundary fires"
    );
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "Bnd" && to == "EndInterrupted"
        )),
        "the boundary event fires and takes its outgoing flow"
    );

    assert!(
        engine.is_completed(inst),
        "the instance reaches EndInterrupted and completes"
    );
    // The agent job must be gone (canceled), not left activated on a dead
    // container.
    assert!(
        engine.active_job_on(container).is_none(),
        "no agent job survives on the torn-down container"
    );
    assert!(
        engine.instance(inst).is_none()
            || engine.instance(inst).unwrap().adhoc_instances.is_empty(),
        "the ad-hoc container's runtime record was cleared on cancel"
    );
}

/// Regression for Magikcraft/nano-bpm#1154: a `bpmn:sequenceFlow` between two
/// DIRECT children of an ad-hoc container must execute as a chain. Before the
/// fix the activated element ran but its outgoing flow was silently dropped —
/// the downstream element never activated and the container completed as if the
/// activated element were a leaf (the `parsed-not-executed` class #1009, for a
/// sequence flow inside an ad-hoc scope). This drives the reproduction: activate
/// `toolA` alone, and assert `toolA -> toolB` is taken, `toolB` runs, the agent
/// job re-emits only once the chain drains, and the leaf's output is the single
/// entry appended to the container's `outputCollection`.
#[test]
fn adhoc_inner_sequence_flow_chains_to_the_follow_up_tool() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_chained_tools_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: the agent activates ONLY `toolA` (the head of the chain).
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();

    // `toolB` is NOT active yet — it only runs once `toolA` completes and its
    // outgoing flow is taken.
    assert!(
        engine
            .activate_jobs("toolB-type", "W", 10, 1_000, 0)
            .is_empty(),
        "toolB must not run before toolA completes and its flow is taken"
    );

    // Complete `toolA`. This is where the defect surfaced: the flow was dropped.
    let tool_a = engine
        .activate_jobs("toolA-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let chain_events = engine
        .apply_command(Command::complete_job_with(
            tool_a.key,
            HashMap::from([("result".to_string(), Value::Str("A".into()))]),
        ))
        .unwrap();
    assert!(
        chain_events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "toolA" && to == "toolB"
        )),
        "toolA's outgoing inner flow to toolB must be taken (issue #1154), \
         got {chain_events:?}"
    );

    // The agent job must NOT re-emit while the chain is still running: the
    // container is a single active path (now at toolB), not drained.
    assert!(
        engine
            .activate_jobs("agent-worker", "W", 10, 1_000, 0)
            .is_empty(),
        "the agent job must not re-emit mid-chain — the path is still running"
    );

    // `toolB` chained into existence and produces a job.
    let tool_b = engine
        .activate_jobs("toolB-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolB")
        .expect("toolB chained from toolA's completed flow (issue #1154)");
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "exactly one tool active during the chain (toolB, after toolA handed off)"
    );

    // Complete `toolB` (the leaf). Only NOW does the path drain and the agent
    // job re-emit for the next turn.
    engine
        .apply_command(Command::complete_job_with(
            tool_b.key,
            HashMap::from([("result".to_string(), Value::Str("B".into()))]),
        ))
        .unwrap();
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the chain drained after the leaf toolB"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "the agent job re-emits exactly once — only after the whole chain drained"
    );
    // `outputElement` is a per-execution-path result: the chain toolA -> toolB is
    // ONE path, so the collection has exactly ONE entry — the leaf's result.
    assert!(
        matches!(
            container_output_collection(&engine, inst, container, "results"),
            Some(Value::List(ref v)) if v.as_slice() == [Value::Str("B".into())]
        ),
        "the chain contributes ONE outputCollection entry — the leaf toolB's \
         result — not one per node, got {:?}",
        container_output_collection(&engine, inst, container, "results")
    );

    // Turn 2: the agent signals completion → the container completes and writes
    // its aggregated collection outward.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted for turn 2");
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "the container completes once the agent is done"
    );
}

/// Regression for the empty-`element_id` teardown failure mode in the mid-chain
/// hand-off (`continue_adhoc_inner_flow`). A completing tool tears down its
/// dedicated inner-instance wrapper; if that wrapper has already left `active`
/// (but still resolves via `scopes`), the old `unwrap_or_default()` emitted
/// `ElementCompleting`/`ElementCompleted` with an empty `element_id`, which
/// corrupts downstream element aggregates — the same class guarded on the cancel
/// path by `nested_adhoc_cancel_child_skips_already_completed_inner_instance`.
/// The fix routes both tool paths through `adhoc_inner_instance_teardown`, which
/// skips the teardown when the id is unresolvable.
#[test]
fn adhoc_mid_chain_handoff_skips_already_completed_inner_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_chained_tools_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    // Turn 1: activate only `toolA` (the head of the chain).
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool_a = engine
        .activate_jobs("toolA-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let tool_a_eik = tool_a.element_instance_key;

    // The dedicated inner wrapper `toolA` hangs off.
    let inner = engine.scope_of(inst, tool_a_eik);
    assert_ne!(inner, 0, "toolA hangs off a dedicated inner instance");

    // Simulate that inner wrapper having ALREADY been torn down: drop it from
    // `active` (so its element id no longer resolves) while its `scopes` mapping
    // still resolves it — the exact state the old `unwrap_or_default()` mishandled.
    engine
        .state
        .instances
        .get_mut(&inst)
        .unwrap()
        .active
        .remove(&inner);
    assert!(
        engine.element_id_of_instance(inst, inner).is_none(),
        "inner wrapper is no longer active"
    );
    assert_eq!(
        engine.scope_of(inst, tool_a_eik),
        inner,
        "but its scopes mapping still resolves it"
    );

    // Completing `toolA` drives the mid-chain hand-off; its events must not carry
    // an empty element_id nor fabricate a completion for the gone inner instance.
    let events = engine
        .apply_command(Command::complete_job_with(
            tool_a.key,
            HashMap::from([("result".to_string(), Value::Str("A".into()))]),
        ))
        .unwrap();
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleting { element_id, .. } | Event::ElementCompleted { element_id, .. }
                if element_id.is_empty()
        )),
        "no element-completion event carries an empty element_id; events: {events:?}"
    );
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, .. } if *element_instance_key == inner
        )),
        "the already-completed inner instance is not torn down again; events: {events:?}"
    );
    // The chain still hands off — toolB activates despite the gone inner wrapper.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "toolA" && to == "toolB"
        )),
        "toolA's outgoing inner flow to toolB is still taken; events: {events:?}"
    );
}

/// Regression for the PR #1164 review: a fulfilled `<completionCondition>` must
/// be honoured on the MID-CHAIN hand-off, not just on a leaf tool. Before the
/// fix `complete_adhoc_tool` early-returned into `continue_adhoc_inner_flow`
/// whenever a tool had an outgoing inner flow, skipping ALL completion-condition
/// handling — so a container whose condition became true after `toolA` would
/// still take the flow and activate `toolB` instead of completing. This drives
/// the reproduction: activate `toolA` alone, complete it with `done = true`, and
/// assert the container completes at once, the `toolA -> toolB` flow is NOT
/// taken, and `toolB` never runs.
#[test]
fn adhoc_completion_condition_fires_on_the_mid_chain_handoff() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_chained_tools_completion_condition_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    // Turn 1: the agent activates ONLY `toolA` (the head of the chain).
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();

    // Complete `toolA` returning `done = true`: the completion condition fires on
    // the hand-off, so the flow to `toolB` must NOT be taken.
    let tool_a = engine
        .activate_jobs("toolA-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job emitted");
    let events = engine
        .apply_command(Command::complete_job_with(
            tool_a.key,
            HashMap::from([
                ("done".to_string(), Value::Bool(true)),
                ("result".to_string(), Value::Str("A".into())),
            ]),
        ))
        .unwrap();

    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "toolA" && to == "toolB"
        )),
        "a fulfilled completion condition must short-circuit the chain — \
         toolA's outgoing flow to toolB must NOT be taken, got {events:?}"
    );
    assert!(
        engine.is_completed(inst),
        "the container completes at once when its completion condition fires \
         on the mid-chain hand-off"
    );
    assert!(
        engine
            .activate_jobs("toolB-type", "W", 10, 1_000, 0)
            .is_empty(),
        "toolB must never run — the chain was cut short by completion"
    );
    // The mid-chain hand-off must NOT append `toolA`'s output to the
    // `outputCollection`: with the default `cancelRemainingInstances=true`, a
    // fulfilled completion condition CANCELS the in-flight execution path, which
    // therefore drains no leaf and contributes no entry. `outputElement` is a
    // per-execution-path result appended only at a leaf (see
    // `adhoc_inner_sequence_flow_chains_to_the_follow_up_tool`); a truncated,
    // cancelled path is not a leaf, so evaluating the container-level
    // `outputElement` against `toolA`'s non-final scope would be wrong. Lock that
    // `continue_adhoc_inner_flow` hands off with `output: None` even when the
    // condition short-circuits — guarding against a future refactor silently
    // collecting the cancelled tool's partial result ("A").
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::AdHocToolCompleted {
                output: Some(_),
                ..
            }
        )),
        "the mid-chain short-circuit must drop the tool with output: None — a \
         cancelled path appends nothing to the outputCollection, got {events:?}"
    );
    assert!(
        !matches!(
            engine.instance(inst).unwrap().variables.get("results"),
            Some(Value::List(v)) if v.contains(&Value::Str("A".into()))
        ),
        "toolA's result must never reach the outputCollection when the completion \
         condition short-circuits the chain, got {:?}",
        engine.instance(inst).unwrap().variables.get("results")
    );
}

/// Acceptance guard for Magikcraft/nano-bpm#872 (the deferred remainder of #631,
/// which PR #863 closed after delivering only the nested-ad-hoc / agent-of-agents
/// half). Activating an embedded `subProcess` tool must run its multi-element
/// body by token flow — creating the inner `ask` user task and parking the tool
/// until the body reaches its end event — NOT complete the tool immediately.
///
/// Today the tool is classified `AdHocToolKind::Other` and `activate_adhoc_tool`
/// passes it straight through (`Step::Complete`), so no user task is created and
/// this test's `expect` fails. It is `#[ignore]`d so it never reports a failure
/// on `main` while the capability is unimplemented; the engineer who lands #872
/// removes the `#[ignore]` to flip it green. This makes #872's done-state
/// test-defined rather than issue-state-defined (see nano-workforce#313).
#[test]
fn adhoc_agent_runs_an_embedded_subprocess_tool_body_by_token_flow() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_embedded_subprocess_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: the agent activates the embedded subProcess tool. Its body must
    // run — creating the inner `ask` user task — and the tool must stay ACTIVE
    // until the body completes, not auto-complete on activation.
    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
                ..Default::default()
            },
        ))
        .unwrap();

    let user_task_key = activated
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                ..
            } if element_id == "ask" => Some(*user_task_key),
            _ => None,
        })
        .expect(
            "activating an embedded subProcess tool runs its body: the inner \
             `ask` user task is created (#872)",
        );

    assert!(
        !engine.is_completed(inst),
        "container parks while the tool's inner human task is open (#872)"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the subProcess tool child stays active while its body runs (#872)"
    );

    // Completing the inner user task drains the tool body to its end event,
    // which completes the tool — feeding the container's outputCollection and
    // re-emitting the agent job for the next turn.
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("approved".to_string()));
    engine
        .apply_command(Command::complete_user_task_with(user_task_key, vars))
        .unwrap();
    let results = container_output_collection(&engine, inst, container, "results");
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("approved".to_string())])),
        "the subProcess tool body's output reaches the container's \
         outputCollection once the body completes (#872)"
    );

    // The tool completed cleanly: its body ran to the end event, so (1) the
    // container's active set is empty again — the child actually drained,
    // rather than the outputCollection being fed while it dangles — and (2) the
    // container iteration advanced, re-emitting the agent job for the next turn.
    // Read both off the recorded container state (side-effect free — do not
    // `activate_jobs` here, which would lock/mutate the re-emitted job), the
    // same way the sibling drain guards in this file do.
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the subProcess tool child drains once its body completes (#872)"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "completing the subProcess tool re-emits the agent job for the next \
         turn (#872)"
    );
}

#[test]
fn adhoc_subprocess_tool_body_routes_through_a_gateway_before_completing() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_loan_decision_review_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    let container = agent.element_instance_key;

    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
                ..Default::default()
            },
        ))
        .unwrap();
    let officer_task = activated
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                ..
            } if element_id == "officer" => Some(*user_task_key),
            _ => None,
        })
        .expect("the review body's human `officer` task is created (#872)");

    // The reviewer approves: the body must route the gateway to the offer branch
    // and drain to that end event, completing the tool.
    let mut vars = HashMap::new();
    vars.insert("decision".to_string(), Value::Str("approve".to_string()));
    engine
        .apply_command(Command::complete_user_task_with(officer_task, vars))
        .unwrap();

    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::List(vec![Value::Str("approve".to_string())])),
        "the routed body's decision reaches the container's outputCollection (#872)"
    );
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the tool drains once the routed branch reaches its end event (#872)"
    );
    assert_eq!(
        adhoc.iterations, 1,
        "completing the routed tool body re-emits the agent job (#872)"
    );
}

#[test]
fn adhoc_cancel_remaining_tears_down_an_open_subprocess_tool_body() {
    // Defect-class guard (#872): cancelling the container while an embedded
    // subProcess tool's body is mid-flight (an open human task inside it) must
    // tear the body down — the inner user-task element instance is completed, not
    // orphaned in the read-model element-instance tree — mirroring the nested
    // ad-hoc teardown #863 added.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_embedded_subprocess_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    let container = agent.element_instance_key;
    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
                ..Default::default()
            },
        ))
        .unwrap();
    let ask = activated
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "ask" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the body's `ask` user task element is active");
    let ask_task = activated
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated {
                user_task_key,
                element_id,
                ..
            } if element_id == "ask" => Some(*user_task_key),
            _ => None,
        })
        .expect("the body's `ask` user task was created");

    // Cancel the container's remaining instances while the body's human task is
    // still open.
    let events = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .expect("cancel-remaining completes the container");

    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, element_id, .. }
                if *element_instance_key == ask && element_id == "ask"
        )),
        "the open body user-task element instance is torn down, not orphaned; events: {events:?}"
    );
    // The parked human task inside the body must be explicitly cancelled, not
    // just its element instance completed — otherwise the `user_tasks` entry
    // stays `Created`, surfacing as an orphaned/open user task after the
    // container is cancelled (#872 cancel defect class).
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::UserTaskCanceled { user_task_key, .. } if *user_task_key == ask_task
        )),
        "the open body user task is cancelled, not left orphaned in Created; events: {events:?}"
    );
    assert_eq!(
        engine.state().user_tasks[&ask_task].state,
        crate::state::UserTaskState::Canceled,
        "the body's user task ends Canceled after the container is cancelled (#872)"
    );
    assert!(engine.is_completed(inst), "the whole instance completes");
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "no ad-hoc runtime state leaks after cancelling an open subProcess tool"
    );
}

#[test]
fn adhoc_subprocess_tool_read_model_tree_nests_container_inner_tool_and_body() {
    // #872 read-model nesting: outer container -> inner instance -> subProcess
    // tool -> its body's leaves. The body's `ask` task hangs off the subProcess
    // tool, which hangs off the `#innerInstance`, which is scoped to the
    // container.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_embedded_subprocess_tool(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("review")],
                ..Default::default()
            },
        ))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    let eik_of = |element_id: &str| -> Key {
        *instance
            .active
            .iter()
            .find(|(_, id)| id.as_str() == element_id)
            .map(|(k, _)| k)
            .unwrap_or_else(|| panic!("no active element instance for {element_id}"))
    };
    let scope_of = |eik: Key| -> Key { instance.scopes.get(&eik).copied().unwrap_or(0) };

    let ask = eik_of("ask");
    let review = eik_of("review");
    let inner = eik_of("agent#innerInstance");
    assert_eq!(
        scope_of(ask),
        review,
        "the body task hangs off the subProcess tool"
    );
    assert_eq!(
        scope_of(review),
        inner,
        "the subProcess tool hangs off its dedicated inner instance"
    );
    assert_eq!(
        scope_of(inner),
        container,
        "the inner instance is scoped to the ad-hoc container"
    );
}

#[test]
fn adhoc_output_collection_is_initialised_empty_on_activation() {
    // Zeebe parity (AdHocSubProcessProcessor.onActivate): a declared
    // `outputCollection` is seeded to an empty array as a *local* container
    // variable when the container activates — so the agent (and any FEEL that
    // reads it) sees `results = []` before a single tool has run. nano used to
    // materialise the collection only at completion, so mid-run it did not exist.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let container = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted")
        .element_instance_key;

    assert_eq!(
        container_output_collection(&engine, inst, container, "results"),
        Some(Value::List(vec![])),
        "outputCollection seeded to an empty array on activation, visible in the container scope",
    );
}

#[test]
fn adhoc_output_collection_grows_and_is_visible_during_the_run() {
    // Zeebe appends each completed element's `outputElement` to the local
    // `outputCollection` as it goes (beforeExecutionPathCompleted), so a
    // mid-run agent turn can inspect the accumulated results. nano used to keep
    // them in a hidden accumulator invisible until the container completed.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    for (job, val) in tool_jobs.iter().zip(["A", "B"]) {
        let mut vars = HashMap::new();
        vars.insert("result".to_string(), Value::Str(val.to_string()));
        engine
            .apply_command(Command::complete_job_with(job.key, vars))
            .unwrap();
    }

    // The container has NOT completed yet (it is awaiting the agent's next turn),
    // but the collection is already visible and holds both tool outputs.
    assert!(
        !engine.is_completed(inst),
        "container still awaiting the agent"
    );
    let mut got = match container_output_collection(&engine, inst, container, "results") {
        Some(Value::List(v)) => v,
        other => panic!("expected a live outputCollection list mid-run, got {other:?}"),
    };
    got.sort_by_key(|v| match v {
        Value::Str(s) => s.clone(),
        _ => String::new(),
    });
    assert_eq!(
        got,
        vec![Value::Str("A".to_string()), Value::Str("B".to_string())],
        "outputCollection accumulates each tool's output as the run progresses",
    );
}

#[test]
fn adhoc_agent_cancel_remaining_instances_completes_container() {
    // An agent that immediately asks to cancel/stop completes the container
    // (v1: at a turn boundary there are no in-flight tools to cancel) and lets
    // the instance flow on to its end event.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                cancel_remaining_instances: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(engine.is_completed(inst), "cancel completes the container");
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "ad-hoc runtime state torn down on completion"
    );
}

#[test]
fn plain_job_completion_ignores_absent_adhoc_result() {
    // A completion of an ordinary (non-ad-hoc) service-task job carries no
    // ad-hoc result and resumes the token normally — the ad-hoc branch must not
    // intercept it.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "order");
    let job = engine
        .activate_jobs("payment", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.instance_key == inst)
        .unwrap();
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();
    assert!(engine.is_completed(inst));
    assert!(engine
        .instance(inst)
        .map(|i| i.adhoc_instances.is_empty())
        .unwrap_or(true));
}

#[test]
fn adhoc_completion_condition_ends_the_loop_and_cancels_remaining_tools() {
    // The declared `<completionCondition>` is evaluated after each tool completes:
    // the first tool sets `done = true`, so the container completes at once —
    // without another agent turn — and cancels the still-running second tool.
    assert_eq!(
        adhoc_completion_condition_process().adhoc[0]
            .completion_condition
            .as_deref(),
        Some("=done = true"),
        "the ad-hoc container's completionCondition is parsed onto the catalog"
    );
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_completion_condition_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;

    // Turn 1: activate both tools.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        2,
        "both tools active"
    );

    // Complete only toolA, returning `done = true`; the completion condition then
    // fires and the container completes, cancelling toolB.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    let job_a = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job");
    let mut vars = HashMap::new();
    vars.insert("done".to_string(), Value::Bool(true));
    vars.insert("result".to_string(), Value::Str("A".to_string()));
    let final_events = engine
        .apply_command(Command::complete_job_with(job_a.key, vars))
        .unwrap();

    assert!(
        engine.is_completed(inst),
        "completion condition ends the container after the first tool"
    );
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "ad-hoc runtime state torn down"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("A".to_string())])),
        "only the completed tool's output is collected; toolB was cancelled"
    );
}

#[test]
fn adhoc_completion_condition_cancel_remaining_defaults_true() {
    // The BPMN `cancelRemainingInstances` attribute defaults to `true`, so a
    // container that omits it cancels remaining tools on a fulfilled condition.
    assert!(
        adhoc_completion_condition_process().adhoc[0].cancel_remaining_instances,
        "cancelRemainingInstances defaults to true when the attribute is absent"
    );
    // And an explicit `false` is parsed onto the catalog.
    assert!(
        !adhoc_completion_condition_defer_process().adhoc[0].cancel_remaining_instances,
        "cancelRemainingInstances=\"false\" is parsed onto the catalog"
    );
}

#[test]
fn adhoc_completion_condition_defers_when_cancel_remaining_is_false() {
    // The declared `<completionCondition>` fires after toolA sets `done = true`,
    // but `cancelRemainingInstances="false"` defers container completion: toolB
    // keeps running, and the container completes only once toolB drains — with
    // BOTH tools' outputs collected (nothing cancelled).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_completion_condition_defer_process(),
        ))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;

    // Turn 1: activate both tools.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ))
        .unwrap();

    // Complete toolA with `done = true`: the completion condition fires, but with
    // cancelRemainingInstances=false the container defers — it stays active with
    // toolB still running and the fulfilment latched.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 60_000, 0);
    let job_a = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolA")
        .expect("toolA job")
        .clone();
    let job_b = tool_jobs
        .iter()
        .find(|j| j.element_id == "toolB")
        .expect("toolB job")
        .clone();
    let mut vars_a = HashMap::new();
    vars_a.insert("done".to_string(), Value::Bool(true));
    vars_a.insert("result".to_string(), Value::Str("A".to_string()));
    engine
        .apply_command(Command::complete_job_with(job_a.key, vars_a))
        .unwrap();

    assert!(
        !engine.is_completed(inst),
        "the container defers completion while toolB is still running"
    );
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .expect("ad-hoc container still active (deferred)")
        .clone();
    assert_eq!(
        adhoc.active.len(),
        1,
        "toolB is still active (not cancelled)"
    );
    assert!(
        adhoc.completion_condition_fulfilled,
        "the fulfilled completion condition is latched while deferring"
    );

    // Complete toolB: no active children remain, so the deferred container now
    // completes — collecting toolB's output as well.
    let mut vars_b = HashMap::new();
    vars_b.insert("result".to_string(), Value::Str("B".to_string()));
    let final_events = engine
        .apply_command(Command::complete_job_with(job_b.key, vars_b))
        .unwrap();

    assert!(
        engine.is_completed(inst),
        "the container completes once its last outstanding tool drains"
    );
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "ad-hoc runtime state torn down after deferred completion"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![
            Value::Str("A".to_string()),
            Value::Str("B".to_string()),
        ])),
        "both tools' outputs are collected; the deferred toolB was not cancelled"
    );
}

#[test]
fn adhoc_tool_io_mapping_applies_inputs_on_activation_and_outputs_on_completion() {
    let proc = adhoc_tool_io_process();
    let tool = proc.adhoc[0]
        .tools
        .iter()
        .find(|t| t.element_id == "toolA")
        .unwrap();
    assert_eq!(
        tool.io.inputs.len(),
        1,
        "tool input mapping retained on catalog"
    );
    assert_eq!(
        tool.io.outputs.len(),
        1,
        "tool output mapping retained on catalog"
    );

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            HashMap::from([("base".to_string(), Value::Int(1))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;

    // Turn 1: activate toolA. Its input mapping `=base + 1` should resolve `n = 2`
    // local to the tool's own scope.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ))
        .unwrap();
    let child = *engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap()
        .active
        .iter()
        .next()
        .expect("toolA active");
    let local = engine
        .instance(inst)
        .unwrap()
        .scope_variables
        .get(&child)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        local.get("n"),
        Some(&Value::Int(2)),
        "input mapping evaluated into the tool's local scope"
    );

    // Complete toolA returning `result = "ok"`; the output mapping projects
    // `status = "ok"` into the container scope, which satisfies the completion
    // condition and ends the container.
    let job = engine
        .activate_jobs("tool", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolA")
        .unwrap();
    engine
        .apply_command(Command::complete_job_with(
            job.key,
            HashMap::from([("result".to_string(), Value::Str("ok".to_string()))]),
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "output-mapped `status` satisfies the completion condition"
    );
}

#[test]
fn adhoc_agent_completion_flag_with_activations_is_rejected() {
    // Zeebe parity (#614 gap 4): asserting `isCompletionConditionFulfilled`
    // while ALSO returning activate-element instructions is contradictory. The
    // JOB_WORKER completion path rejects it with INVALID_ARGUMENT
    // (JobCompleteProcessor.checkAdHocSubProcessCompletionConditionNotFulfilled
    // ForElementActivation). nano previously silently superseded the
    // activations and completed the container; it now rejects the command so
    // the agent must resubmit a coherent turn.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    let err = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::AdHocActivateWithCompletion { .. }),
        "completion flag + activations is rejected (INVALID_ARGUMENT parity), got {err:?}"
    );
    assert!(
        !engine.is_completed(inst),
        "the rejected command leaves the container running"
    );
    assert!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).is_empty(),
        "no tool was activated by the rejected turn"
    );
    let _ = container;
}

#[test]
fn adhoc_agent_completion_flag_alone_completes_and_cancels() {
    // The `isCompletionConditionFulfilled` flag on its own (no activations) ends
    // the loop: the container completes at once and any in-flight tool is
    // cancelled. This is the non-contradictory sibling of the rejection test
    // above.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .unwrap();
    let container = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "completion flag ends the container"
    );
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "runtime state torn down; no tool left active"
    );
    assert!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).is_empty(),
        "no tool job was created"
    );
    let _ = container;
}

#[test]
fn adhoc_declarative_activates_collection_elements_without_a_job() {
    // The defect class: a declarative (BPMN_TASK) ad-hoc container is parsed but
    // never executed — on `main` it is minted as a plain job (job type = its id)
    // and its `activeElementsCollection` is ignored, so no inner element runs.
    // On the branch it evaluates the collection and activates those elements
    // directly, minting no container job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_declarative_process()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "p",
        HashMap::from([(
            "tools".to_string(),
            Value::List(vec![
                Value::Str("toolA".to_string()),
                Value::Str("toolB".to_string()),
            ]),
        )]),
    );

    // No job is minted for the container itself (it is declarative, not a job
    // worker). On `main` a "sub" job would exist here.
    assert!(
        engine.activate_jobs("sub", "W", 10, 1_000, 0).is_empty(),
        "a declarative ad-hoc container mints no job"
    );

    // Both collection elements activated immediately as real element instances
    // inside the container scope, each parked on its own `tool` job.
    let container = *engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .keys()
        .next()
        .expect("container runtime state registered");
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        2,
        "both named elements active with no agent turn"
    );
    assert!(
        !engine.is_completed(inst),
        "container parks while tools run"
    );

    // Drain the two tool jobs, each producing a `result`. When the last drains,
    // the declarative container completes (no agent job re-emitted), writes its
    // `outputCollection`, and takes its outgoing flow to the end event.
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 2, "both named tools produced jobs");
    let mut final_events = Vec::new();
    for (job, val) in tool_jobs.iter().zip(["A", "B"]) {
        let mut vars = HashMap::new();
        vars.insert("result".to_string(), Value::Str(val.to_string()));
        final_events = engine
            .apply_command(Command::complete_job_with(job.key, vars))
            .unwrap();
    }
    assert!(
        engine.is_completed(inst),
        "container completes once its collection drains → instance done"
    );
    // No agent job is ever re-emitted for a declarative container.
    assert!(
        engine.activate_jobs("sub", "W", 10, 1_000, 0).is_empty(),
        "declarative container never re-emits a container job"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    let mut got = match results {
        Some(Value::List(v)) => v,
        other => panic!("expected outputCollection list, got {other:?}"),
    };
    got.sort_by_key(|v| match v {
        Value::Str(s) => s.clone(),
        _ => String::new(),
    });
    assert_eq!(
        got,
        vec![Value::Str("A".to_string()), Value::Str("B".to_string())],
        "outputCollection holds both tool results"
    );
}

#[test]
fn adhoc_declarative_empty_collection_completes_immediately() {
    // A declarative container whose collection evaluates to an empty list has
    // nothing to run and completes at once, flowing on to the end event — it must
    // not park forever waiting on a non-existent job.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_declarative_process()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "p",
        HashMap::from([("tools".to_string(), Value::List(vec![]))]),
    );
    assert!(
        engine.is_completed(inst),
        "an empty active-elements collection completes the container immediately"
    );
    assert!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).is_empty(),
        "no tool was activated"
    );
    assert!(
        engine.activate_jobs("sub", "W", 10, 1_000, 0).is_empty(),
        "no container job was minted"
    );
}

#[test]
fn adhoc_declarative_unknown_collection_ids_raise_an_incident() {
    // Zeebe `AdHocSubProcessProcessor` (BPMN_TASK variant): an id in the
    // `activeElementsCollection` that is not one of the container's inner
    // elements is an EXTRACT_VALUE_ERROR on the container — "... the given
    // elements [ghost] do not exist." — never silently dropped (#1398). Fixing
    // the collection and resolving activates the real element.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(adhoc_declarative_process()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "p",
        HashMap::from([(
            "tools".to_string(),
            Value::List(vec![
                Value::Str("toolA".to_string()),
                Value::Str("ghost".to_string()),
            ]),
        )]),
    );
    assert!(
        engine.activate_jobs("tool", "W", 10, 1_000, 0).is_empty(),
        "no tool runs while the collection names an unknown element"
    );
    let incident = engine.active_incidents()[0].clone();
    assert_eq!(incident.kind, state::IncidentKind::ExtractValueError);
    assert!(
        incident
            .reason
            .contains("but the given elements [ghost] do not exist."),
        "{}",
        incident.reason
    );
    engine
        .apply_command(Command::set_variables(
            inst,
            HashMap::from([(
                "tools".to_string(),
                Value::List(vec![Value::Str("toolA".to_string())]),
            )]),
        ))
        .unwrap();
    engine
        .apply_command(Command::resolve_incident(incident.key))
        .unwrap();
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 1, "exactly one tool ran");
    engine
        .apply_command(Command::complete_job(tool_jobs[0].key))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "container completes after its one tool"
    );
}

/// Red for #631: activating a nested `adHocSubProcess` tool must stand up a
/// real second-level container — its own agent job (`sub-agent-worker`), its own
/// registered ad-hoc scope, and its own seeded `outputCollection` — not a plain
/// opaque job. The nested container's completion then feeds the outer
/// container's `outputElement`/loop across the nesting boundary, and the outer
/// container completes normally.
#[test]
fn nested_adhoc_tool_stands_up_a_second_level_container_and_propagates_completion() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // Outer container's agent job.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job emitted");
    let outer = agent.element_instance_key;

    // Turn 1: outer agent activates the nested `subagent` tool.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();

    // The nested container is now an active tool of the outer container.
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&outer)
            .unwrap()
            .active
            .len(),
        1,
        "the nested container is active in the outer container"
    );

    // It must have minted its OWN agent job (second-level worker), and
    // registered its own ad-hoc scope with a seeded outputCollection.
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested container minted its own agent job (second level)");
    let nested = sub_agent.element_instance_key;
    assert!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .contains_key(&nested),
        "the nested container registered its own ad-hoc runtime scope"
    );
    assert_eq!(
        container_output_collection(&engine, inst, nested, "subResults"),
        Some(Value::List(vec![])),
        "the nested container seeded its own empty outputCollection"
    );

    // Second-level turn: the nested agent activates its `leaf` tool.
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf")],
                ..Default::default()
            },
        ))
        .unwrap();
    let leaf = engine
        .activate_jobs("leaf-tool", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "leaf")
        .expect("leaf tool job emitted inside the nested container");
    let mut leaf_vars = HashMap::new();
    leaf_vars.insert("leafOut".to_string(), Value::Str("deep".into()));
    engine
        .apply_command(Command::complete_job_with(leaf.key, leaf_vars))
        .unwrap();

    // The leaf output accumulated in the nested container's outputCollection.
    assert_eq!(
        container_output_collection(&engine, inst, nested, "subResults"),
        Some(Value::List(vec![Value::Str("deep".into())])),
        "leaf output accumulated in the nested container's collection"
    );

    // Second-level agent job re-emitted for the next turn; it signals done,
    // completing the nested container, whose completion feeds the OUTER
    // container's outputElement (=result) and loop.
    let sub_agent2 = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job re-emitted");
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent2.key,
            {
                let mut m = HashMap::new();
                m.insert("result".to_string(), Value::Str("nested-done".into()));
                m
            },
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();

    // The nested container drained out of the outer container's active set.
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&outer)
            .unwrap()
            .active
            .len(),
        0,
        "the nested container completed and left the outer active set"
    );
    // The outer agent job re-emitted for its next turn.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job re-emitted after nested tool completed");
    let final_events = engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(
        engine.is_completed(inst),
        "outer container completed → instance done"
    );
    let results = final_events.iter().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => variables.get("results").cloned(),
        _ => None,
    });
    assert_eq!(
        results,
        Some(Value::List(vec![Value::Str("nested-done".into())])),
        "the nested container's result fed the outer outputCollection across the boundary"
    );
}

/// #631 (read-model nesting): while a second-level tool runs, the element-
/// instance tree must nest correctly — leaf tool → its inner instance → nested
/// container → the nested container's inner instance → outer container. A flat
/// or mis-parented tree would break the console trace and Operate-parity audit
/// trail the ADR requires.
#[test]
fn nested_adhoc_read_model_element_tree_nests_correctly() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf")],
                ..Default::default()
            },
        ))
        .unwrap();

    let instance = engine.instance(inst).unwrap();
    let eik_of = |element_id: &str| -> Key {
        *instance
            .active
            .iter()
            .find(|(_, id)| id.as_str() == element_id)
            .map(|(k, _)| k)
            .unwrap_or_else(|| panic!("no active element instance for {element_id}"))
    };
    let scope_of = |eik: Key| -> Key { instance.scopes.get(&eik).copied().unwrap_or(0) };

    let leaf = eik_of("leaf");
    let nested_inner = eik_of("subagent#innerInstance");
    let outer_inner = eik_of("agent#innerInstance");

    assert_eq!(
        scope_of(leaf),
        nested_inner,
        "leaf hangs off its inner instance"
    );
    assert_eq!(
        scope_of(nested_inner),
        nested,
        "the leaf's inner instance is scoped to the nested container"
    );
    assert_eq!(
        scope_of(nested),
        outer_inner,
        "the nested container hangs off its own inner instance in the outer container"
    );
    assert_eq!(
        scope_of(outer_inner),
        outer,
        "the nested container's inner instance is scoped to the outer container"
    );
}

/// #631 (cancel propagation across the boundary): a nested container whose
/// `<completionCondition>` fires cancels its remaining second-level tools, then
/// completes and feeds the outer container's loop — its still-running tool is
/// not orphaned and the outer container advances.
#[test]
fn nested_adhoc_completion_condition_cancels_remaining_and_feeds_parent() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_cancel_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;

    // Nested agent activates BOTH leaves in one turn.
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf1"), activate_element("leaf2")],
                ..Default::default()
            },
        ))
        .unwrap();
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&nested)
            .unwrap()
            .active
            .len(),
        2,
        "both nested leaves active"
    );

    // Drain only leaf1: its output makes the nested completionCondition true, so
    // the nested container cancels leaf2 and completes — crossing the boundary
    // into the outer container.
    let leaf_jobs = engine.activate_jobs("leaf-tool", "W", 10, 1_000, 0);
    let leaf1 = leaf_jobs
        .iter()
        .find(|j| j.element_id == "leaf1")
        .expect("leaf1 job");
    let mut vars = HashMap::new();
    vars.insert("leafOut".to_string(), Value::Str("x".into()));
    vars.insert("done".to_string(), Value::Bool(true));
    engine
        .apply_command(Command::complete_job_with(leaf1.key, vars))
        .unwrap();

    // The nested container is gone (completed) and left the outer active set.
    assert!(
        !engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .contains_key(&nested),
        "the nested container completed and dropped its runtime state"
    );
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&outer)
            .unwrap()
            .active
            .len(),
        0,
        "the nested container left the outer active set (cancel crossed the boundary)"
    );
    assert_eq!(
        container_output_collection(&engine, inst, outer, "results"),
        Some(Value::List(vec![Value::Bool(true)])),
        "the cancelled nested container still fed the outer outputElement"
    );
    // The nested container's OWN `outputCollection` (`subResults`) is an internal
    // detail of the nested scope — it must NOT leak across the nesting boundary
    // into the enclosing scope. Its result crosses only via the parent's
    // outputElement (`results`, asserted above). Before the fix, completing the
    // nested container propagated `subResults` out of its scope, landing it in the
    // root instance variables (no ancestor scope defines it).
    assert!(
        !engine
            .instance(inst)
            .unwrap()
            .variables
            .contains_key("subResults"),
        "the nested container's internal outputCollection did not leak into the root scope"
    );
    assert_eq!(
        container_output_collection(&engine, inst, outer, "subResults"),
        None,
        "the nested container's internal outputCollection did not leak into the outer scope"
    );

    // The outer agent job re-emitted; complete the run.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job re-emitted");
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(engine.is_completed(inst), "instance completes");
}

/// #631 (parent cancels a nested tool): when the OUTER container completes with
/// `cancelRemainingInstances=true` while a NESTED ad-hoc container tool is still
/// active, the nested container must be torn down RECURSIVELY — its own active
/// descendants (second-level tools + their jobs) cancelled, its element instance
/// completed, and its `adhoc_instances` runtime state dropped (`AdHocCompleted`).
/// Before the fix the outer cancel loop treated the nested container as a leaf
/// tool (`cancel_mi_child_events`), orphaning the nested container's leaf job +
/// element instance and leaking its ad-hoc state (no `AdHocCompleted`).
#[test]
fn nested_adhoc_parent_cancel_recursively_tears_down_nested_container() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // Outer agent activates the nested `subagent` tool.
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();

    // Nested agent activates its `leaf` tool, which mints a leaf job left
    // in-flight (never completed).
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            sub_agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("leaf")],
                ..Default::default()
            },
        ))
        .unwrap();
    let leaf_job = engine
        .activate_jobs("leaf-tool", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "leaf")
        .expect("leaf tool job in-flight inside the nested container");
    let leaf = leaf_job.element_instance_key;
    assert!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .contains_key(&nested),
        "nested container active before the parent cancels"
    );

    // Cancel the OUTER container's remaining instances while the nested container
    // (and its leaf) are still running.
    let events = engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: outer,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .expect("cancel-remaining completes the outer container");

    // The nested container's ad-hoc runtime state was dropped (recursive
    // teardown), not left dangling.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::AdHocCompleted { container_key, cancelled, .. }
                if *container_key == nested && *cancelled
        )),
        "the nested container emitted AdHocCompleted (its ad-hoc state was dropped); events: {events:?}"
    );
    // The nested container's leaf descendant's job was cancelled — not orphaned.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::JobCanceled { job_key, .. } if *job_key == leaf_job.key
        )),
        "the nested container's in-flight leaf job was cancelled; events: {events:?}"
    );
    // The leaf descendant's element instance was completed — not left orphaned in
    // the read-model element-instance tree.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, element_id, .. }
                if *element_instance_key == leaf && element_id == "leaf"
        )),
        "the nested container's leaf element instance was completed; events: {events:?}"
    );
    assert!(engine.is_completed(inst), "the whole instance completes");
    assert!(
        engine
            .instance(inst)
            .map(|i| i.adhoc_instances.is_empty())
            .unwrap_or(true),
        "no ad-hoc runtime state leaks after the recursive cancel"
    );
}

/// #631 robustness (Copilot review, PR #863): the recursive cancel helper
/// `cancel_adhoc_active_child` also tears down the dedicated inner wrapper
/// instance each tool hangs off. That inner instance's `scopes` mapping can
/// linger after it has already left `active` (been completed), in which case its
/// element id no longer resolves. Emitting `ElementCompleting`/`ElementCompleted`
/// with an EMPTY element_id in that case corrupts downstream element aggregates —
/// so the teardown must be skipped when the id can't be resolved (mirroring the
/// defensive skip on the `ModifyInstance` termination path). Before the fix the
/// inner teardown used `element_id_of_instance(..).unwrap_or_default()`, emitting
/// a completion for an id of `""`.
#[test]
fn nested_adhoc_cancel_child_skips_already_completed_inner_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(nested_adhoc_agent_process()))
        .unwrap();
    let inst = create_instance_key(&mut engine, "p");

    // Outer agent activates the nested `subagent` tool (which hangs off a
    // dedicated `agent#innerInstance` wrapper instance).
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("outer agent job");
    let outer = agent.element_instance_key;
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("subagent")],
                ..Default::default()
            },
        ))
        .unwrap();
    let sub_agent = engine
        .activate_jobs("sub-agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "subagent")
        .expect("nested agent job");
    let nested = sub_agent.element_instance_key;

    // The inner wrapper the nested tool hangs off.
    let inner = engine.scope_of(inst, nested);
    assert_ne!(inner, 0, "nested tool hangs off a dedicated inner instance");
    assert_ne!(
        inner, outer,
        "the inner wrapper is distinct from the container"
    );

    // Simulate that inner wrapper having ALREADY been torn down: drop it from
    // `active` (so its element id no longer resolves) while its `scopes` mapping
    // still resolves it — the exact state the old `unwrap_or_default()` mishandled.
    engine
        .state
        .instances
        .get_mut(&inst)
        .unwrap()
        .active
        .remove(&inner);
    assert!(
        engine.element_id_of_instance(inst, inner).is_none(),
        "inner wrapper is no longer active"
    );
    assert_eq!(
        engine.scope_of(inst, nested),
        inner,
        "but its scopes mapping still resolves it"
    );

    let events = engine.cancel_adhoc_active_child(inst, outer, nested);

    // No element-completion event may carry an empty element_id.
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleting { element_id, .. } | Event::ElementCompleted { element_id, .. }
                if element_id.is_empty()
        )),
        "no element-completion event carries an empty element_id; events: {events:?}"
    );
    // And it must not fabricate a completion for the already-gone inner instance.
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_instance_key, .. } if *element_instance_key == inner
        )),
        "the already-completed inner instance is not torn down again; events: {events:?}"
    );
}

/// Issue #1159 (red/green): activating a `callActivity` as an ad-hoc tool must
/// INSTANTIATE its called process (propagating the mapped input), wait for it,
/// and apply the tool's output mapping to what the child ACTUALLY produced —
/// instead of the old bug where the call activity was activated but its child
/// process was never started, so no child instance/job/incident was created and
/// the output mapping manufactured an all-null `{status: null, summary: null}`.
#[test]
fn adhoc_call_activity_tool_spawns_child_and_maps_its_real_output() {
    let mut engine = Engine::new();
    for def in adhoc_agent_with_call_activity_tool() {
        engine.apply_command(Command::DeployProcess(def)).unwrap();
    }
    let inst = engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[("askedAbout", Value::Str("a mortgage".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    // Turn 1: the agent activates the call-activity tool. This must SPAWN the
    // child process (its `probe-child` job appears) rather than pass straight
    // through — and the tool child must stay ACTIVE while the child runs.
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();

    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .active
            .len(),
        1,
        "the call-activity tool child stays active while its spawned child runs"
    );

    let child_jobs = engine.activate_jobs("probe-child", "W", 10, 1_000, 0);
    assert_eq!(
        child_jobs.len(),
        1,
        "activating a call-activity tool instantiates the called process \
         (a `probe-child` job is minted) — the #1159 bug minted zero"
    );
    let child_job = &child_jobs[0];
    assert_eq!(
        child_job.variables.get("customerRequest"),
        Some(&Value::Str("a mortgage".into())),
        "the tool's input mapping (askedAbout -> customerRequest) crossed into \
         the spawned child"
    );

    // The container must NOT have re-emitted the agent job yet — it parks on the
    // in-flight child, exactly like it parks on an open user-task tool.
    assert!(
        engine
            .activate_jobs("agent-worker", "W", 10, 1_000, 0)
            .is_empty(),
        "the container waits for the child; no premature agent re-emit"
    );

    // The specialist child answers.
    engine
        .apply_command(Command::complete_job_with(
            child_job.key,
            vars(&[
                ("status", Value::Str("resolved".into())),
                ("summary", Value::Str("Monthly payment is $1,516.".into())),
            ]),
        ))
        .unwrap();

    // The tool child drained, and the child's REAL output flowed through the
    // tool's output mapping into `toolCallResult` and the container's
    // `outputCollection` — no all-null result.
    let adhoc = engine
        .instance(inst)
        .unwrap()
        .adhoc_instances
        .get(&container)
        .unwrap();
    assert_eq!(
        adhoc.active.len(),
        0,
        "the call-activity tool drained once its child completed"
    );

    let expected = Value::Map(std::collections::BTreeMap::from([
        ("status".to_string(), Value::Str("resolved".into())),
        (
            "summary".to_string(),
            Value::Str("Monthly payment is $1,516.".into()),
        ),
    ]));
    assert_eq!(
        container_output_collection(&engine, inst, container, "toolCallResults"),
        Some(Value::List(vec![expected.clone()])),
        "the container's outputCollection carries the child's real result, not \
         a manufactured {{status: null, summary: null}}"
    );

    // Turn 2: the agent's re-emitted job sees the tool's REAL result in its
    // working memory (`toolCallResult`, the io-output projection into the
    // container scope) — the "parent's toolCallResult" the issue tracks — instead
    // of a manufactured `{status: null, summary: null}`. The agent then signals
    // completion and the container (and parent instance) complete.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted after the tool drained");
    assert_eq!(
        agent2.variables.get("toolCallResult"),
        Some(&expected),
        "the tool's output mapping projected the child's real result for the agent"
    );
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(engine.is_completed(inst), "the parent instance completed");
}

/// Issue #1159 (Copilot review round 2): the call-activity tool's output mapping
/// must be evaluated EXACTLY ONCE, against the child's real produced variables —
/// not once in the bridge and again in the shared completion leaf. A double pass
/// double-applies chained mappings, so the container's `outputElement`-collected
/// result and its projected `toolCallResult` disagree. Both must equal the
/// single-pass projection `{status: "resolved", summary: null}` (the second
/// mapping's `summaryCopy` is a sibling target, absent from the child view).
#[test]
fn adhoc_call_activity_tool_output_mapping_evaluated_once_for_chained_mappings() {
    let mut engine = Engine::new();
    for def in adhoc_agent_with_chained_output_call_activity_tool() {
        engine.apply_command(Command::DeployProcess(def)).unwrap();
    }
    let inst = engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[("askedAbout", Value::Str("a mortgage".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();

    let child_jobs = engine.activate_jobs("probe-child", "W", 10, 1_000, 0);
    assert_eq!(child_jobs.len(), 1, "the called process was instantiated");
    engine
        .apply_command(Command::complete_job_with(
            child_jobs[0].key,
            vars(&[
                ("status", Value::Str("resolved".into())),
                ("summary", Value::Str("Monthly payment is $1,516.".into())),
            ]),
        ))
        .unwrap();

    // Single-pass projection: `summaryCopy` is a sibling output target, NOT a
    // child variable, so the second mapping's `summary: summaryCopy` resolves to
    // null. A double evaluation would instead see the seeded `summaryCopy` and
    // manufacture the real summary here.
    let single_pass = Value::Map(std::collections::BTreeMap::from([
        ("status".to_string(), Value::Str("resolved".into())),
        ("summary".to_string(), Value::Null),
    ]));

    // The container's `outputElement` (`=toolCallResult`) collected exactly the
    // single-pass projection...
    assert_eq!(
        container_output_collection(&engine, inst, container, "toolCallResults"),
        Some(Value::List(vec![single_pass.clone()])),
        "outputElement collected the single-pass projection"
    );

    // ...and the `toolCallResult` projected into the container (the agent's next
    // working memory) AGREES with it — the double-eval bug made these diverge.
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted after the tool drained");
    assert_eq!(
        agent2.variables.get("toolCallResult"),
        Some(&single_pass),
        "the projected toolCallResult agrees with the collected outputElement \
         (single evaluation, no double-applied chained mapping)"
    );
    engine
        .apply_command(Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ))
        .unwrap();
    assert!(engine.is_completed(inst), "the parent instance completed");
}

/// Issue #1159 (Copilot review round 3): the single-pass output projection must
/// be preserved through the CHAINED-FLOW hand-off too, not only the leaf path. A
/// `callActivity` tool that chains into a follow-up sibling early-returns from
/// `complete_adhoc_tool` into `continue_adhoc_inner_flow` before the leaf's
/// output-mapping match — so if the precomputed projection is not threaded
/// through, the hand-off re-evaluates the SAME chained mappings against the
/// seeded tool scope and double-applies them. The follow-up `toolB` reads the
/// container-scoped `toolCallResult`; it must equal the single-pass projection
/// `{status: "resolved", summary: null}` (its `summaryCopy` is a sibling target,
/// absent from the child view), not the double-applied `summary: "Monthly…"`.
#[test]
fn adhoc_call_activity_tool_output_mapping_single_pass_through_chained_flow() {
    let mut engine = Engine::new();
    for def in adhoc_agent_with_chained_flow_call_activity_tool() {
        engine.apply_command(Command::DeployProcess(def)).unwrap();
    }
    let _inst = engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[("askedAbout", Value::Str("a mortgage".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");

    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();

    let child_jobs = engine.activate_jobs("probe-child", "W", 10, 1_000, 0);
    assert_eq!(child_jobs.len(), 1, "the called process was instantiated");
    engine
        .apply_command(Command::complete_job_with(
            child_jobs[0].key,
            vars(&[
                ("status", Value::Str("resolved".into())),
                ("summary", Value::Str("Monthly payment is $1,516.".into())),
            ]),
        ))
        .unwrap();

    let single_pass = Value::Map(std::collections::BTreeMap::from([
        ("status".to_string(), Value::Str("resolved".into())),
        ("summary".to_string(), Value::Null),
    ]));

    // The follow-up `toolB` chained into existence and reads the container-scoped
    // projection the hand-off seeded. It must be the single-pass value — a
    // re-evaluation in the hand-off would have double-applied `summaryCopy` and
    // manufactured a non-null `summary` here.
    let tool_b = engine
        .activate_jobs("toolB-type", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "toolB")
        .expect("toolB chained from CallSpecialist's completed flow");
    assert_eq!(
        tool_b.variables.get("toolCallResult"),
        Some(&single_pass),
        "the chained-flow hand-off projected the single-pass toolCallResult \
         (no double-applied chained mapping across the hand-off)"
    );
}

/// Issue #1159 (coverage for the propagate-`true` branches): with
/// `propagateAllParentVariables=true` the child sees a parent variable that no
/// input mapping named (`sharedContext`), and with `propagateAllChildVariables=true`
/// a raw variable the child produced (`childOnly`) crosses back into the
/// container scope — while the tool's output mapping still projects the real
/// result. The existing spawn test exercises both flags OFF, so this guards the
/// separate default/`true` seed + merge behaviour of the new `activate_adhoc_tool`
/// arm.
#[test]
fn adhoc_call_activity_tool_propagates_parent_and_child_variables() {
    let mut engine = Engine::new();
    for def in adhoc_agent_with_propagating_call_activity_tool() {
        engine.apply_command(Command::DeployProcess(def)).unwrap();
    }
    let inst = engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[
                ("askedAbout", Value::Str("a mortgage".into())),
                ("sharedContext", Value::Str("branch-42".into())),
            ]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();

    let child_jobs = engine.activate_jobs("probe-child", "W", 10, 1_000, 0);
    assert_eq!(child_jobs.len(), 1, "the called process is instantiated");
    let child_job = &child_jobs[0];
    assert_eq!(
        child_job.variables.get("customerRequest"),
        Some(&Value::Str("a mortgage".into())),
        "the tool's input mapping crossed into the spawned child"
    );
    assert_eq!(
        child_job.variables.get("sharedContext"),
        Some(&Value::Str("branch-42".into())),
        "propagateAllParentVariables=true crossed the un-mapped parent variable \
         into the child"
    );

    engine
        .apply_command(Command::complete_job_with(
            child_job.key,
            vars(&[
                ("status", Value::Str("resolved".into())),
                ("summary", Value::Str("Monthly payment is $1,516.".into())),
                ("childOnly", Value::Str("scratch".into())),
            ]),
        ))
        .unwrap();

    let expected = Value::Map(std::collections::BTreeMap::from([
        ("status".to_string(), Value::Str("resolved".into())),
        (
            "summary".to_string(),
            Value::Str("Monthly payment is $1,516.".into()),
        ),
    ]));
    assert_eq!(
        container_output_collection(&engine, inst, container, "toolCallResults"),
        Some(Value::List(vec![expected.clone()])),
        "the output mapping still projects the child's real result"
    );

    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job re-emitted after the tool drained");
    assert_eq!(
        agent2.variables.get("toolCallResult"),
        Some(&expected),
        "the tool's output projection is visible to the re-emitted agent"
    );
    assert_eq!(
        agent2.variables.get("childOnly"),
        Some(&Value::Str("scratch".into())),
        "propagateAllChildVariables=true merged the child's raw variable back \
         into the container scope"
    );
}

/// Issue #1159 (cancellation defect class): a `callActivity` tool drives a
/// distinct CHILD PROCESS INSTANCE that hangs off the tool element, not off the
/// parent instance's active tokens. Cancelling the ad-hoc container while that
/// child is still running must TERMINATE the linked child (and cancel its jobs),
/// not leave the callee running after the ad-hoc token is gone — the generic
/// post-command `cascade_cancel_children` sweep only reaps children of a
/// *terminated* instance, and here the parent instance stays ALIVE (a parallel
/// `hold` branch keeps it running), so the tool-local teardown must reap it.
#[test]
fn adhoc_cancel_remaining_terminates_a_running_call_activity_tool_child() {
    let mut engine = Engine::new();
    for def in adhoc_agent_with_call_activity_tool_and_keepalive() {
        engine.apply_command(Command::DeployProcess(def)).unwrap();
    }
    let inst = engine
        .apply_command(Command::create_instance_with(
            "parent",
            vars(&[("askedAbout", Value::Str("a mortgage".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job emitted for the ad-hoc container");
    let container = agent.element_instance_key;

    let activated = engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();
    let child_key = activated
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                ..
            } if process_id == "child" => Some(*instance_key),
            _ => None,
        })
        .expect("the call-activity tool spawned a child process instance");
    let child_job = engine
        .activate_jobs("probe-child", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("the spawned child minted its job");

    // Cancel the container's remaining instances while the child is still live.
    engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: container,
            activate_elements: Vec::new(),
            cancel_remaining: true,
        })
        .expect("cancel-remaining completes the container");

    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated,
        "the linked call-activity child is terminated, not left running after the \
         ad-hoc token is cancelled"
    );
    assert_eq!(
        engine.state().jobs[&child_job.key].state,
        crate::state::JobState::Canceled,
        "the child's job is cancelled, not left orphaned/activatable"
    );
    assert!(
        engine
            .activate_jobs("probe-child", "W", 10, 1_000, 0)
            .is_empty(),
        "no child job survives the cancellation"
    );
    assert!(
        !engine.is_completed(inst),
        "the parent instance stays ALIVE (its `hold` branch is still open) — the \
         leak is only observable because `cascade_cancel_children` cannot reap the \
         child of a still-live parent"
    );
}

#[test]
fn adhoc_call_activity_tool_output_failure_preserves_child_result_for_redrive() {
    let mut engine = Engine::new();
    for def in adhoc_agent_with_failing_output_call_activity_tool() {
        engine.apply_command(Command::DeployProcess(def)).unwrap();
    }
    let inst = engine
        .apply_command(Command::create_instance("parent"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let agent = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent")
        .expect("agent job");
    engine
        .apply_command(Command::complete_job_with_result(
            agent.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("CallSpecialist")],
                ..Default::default()
            },
        ))
        .unwrap();
    let child_job = engine
        .activate_jobs("probe-child", "W", 10, 1_000, 0)
        .into_iter()
        .next()
        .expect("the called process is instantiated");
    engine
        .apply_command(Command::complete_job_with(
            child_job.key,
            vars(&[("status", Value::Str("resolved".into()))]),
        ))
        .unwrap();

    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the failing output mapping parks one incident"
    );
    assert_eq!(
        active[0].kind,
        state::IncidentKind::IoMapping,
        "a call-activity tool output-mapping failure raises IO_MAPPING_ERROR"
    );
    match &active[0].redrive {
        Some(state::IoMappingRedrive::CallActivityCompletion { child_variables }) => {
            assert_eq!(
                child_variables.get("status"),
                Some(&Value::Str("resolved".into())),
                "the gone child's real result is captured on the incident redrive, \
                 not lost to a generic Completion redrive"
            );
        }
        other => panic!("expected CallActivityCompletion redrive, got {other:?}"),
    }
    assert!(
        !engine.is_completed(inst),
        "the tool must not complete while its output mapping is unresolved"
    );

    // Resolving an unfixable mapping re-projects the captured child result — which
    // fails again — so it stays parked, rather than routing `Step::Complete` into
    // `complete_adhoc_tool` and silently completing the tool with a wrong answer.
    let incident_key = engine.incidents()[0].key;
    engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();
    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the unfixable output mapping re-raises (context-preserving redrive), not \
         a silent wrong completion"
    );
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(!engine.is_completed(inst), "still parked after resolve");
}

#[test]
fn adhoc_call_activity_tool_preserves_chained_input_projection_across_spawn_retry() {
    // The clean first spawn projects the chain single-pass: `y = <x>`, and
    // `z = <y>` reads the ORIGINAL (empty) view, so `z` is null.
    let clean = chained_input_child_seed(true);
    assert_eq!(
        clean.get("y"),
        Some(&Value::Str("seed".into())),
        "clean spawn: y = <x>",
    );
    assert_eq!(
        clean.get("z"),
        Some(&Value::Null),
        "clean spawn: z reads the original (empty) view, so it is exactly null \
         (asserted explicitly so a stray non-null value can't become a \
         non-diagnostic baseline for the retry comparison below)",
    );

    // The respawn after a recoverable spawn incident must reproduce the SAME seed
    // — not re-project `z` against the mutated child scope (which now carries the
    // first pass's applied `y`, making `z = <y>` non-null). This is the #1176
    // defect: without preserving the single-pass projection, `z` would become
    // `"seed"` on the retry.
    let respawned = chained_input_child_seed(false);
    assert_eq!(
        respawned.get("z"),
        clean.get("z"),
        "the respawn seed's z must match the clean first-spawn seed \
         (single-pass projection preserved, not re-evaluated against the mutated \
         child scope)",
    );
    assert_eq!(
        respawned.get("y"),
        clean.get("y"),
        "the respawn seed's y must match the clean first-spawn seed",
    );
}

/// Issue #1508 regression — the ad-hoc-container branch: an interrupting
/// boundary firing while the container's OWN agent job is parked on a
/// `JobNoRetries` incident must RESOLVE that incident on the container root,
/// not leave it ACTIVE on the completed container. `terminate_subprocess_scope`
/// resolves only *descendant* incidents, never the scope root's.
#[test]
fn interrupting_boundary_on_adhoc_resolves_the_container_root_incident() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            adhoc_agent_with_interrupting_message_boundary(),
        ))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("customerId", Value::Str("C1".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Activate the container's agent job, then fail it with no retries so the
    // container ROOT carries a JobNoRetries incident.
    let agent = engine
        .activate_jobs("probe-agent", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "Host")
        .expect("agent job emitted for the ad-hoc container");
    engine
        .apply_command(Command::fail_job(agent.key, 0, "boom"))
        .unwrap();
    assert_eq!(
        engine.active_incidents().len(),
        1,
        "JobNoRetries parked on the ad-hoc container root"
    );

    // The interrupting message boundary fires while the container carries that
    // incident: the failed agent job is cancelled and the root incident is
    // resolved — not left ACTIVE on the removed container.
    let fired = engine.correlate_message("probe-cancel", "C1", HashMap::new(), 0);
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::IncidentResolved { .. })),
        "the boundary resolves the incident on the container root (#1508)"
    );
    assert!(
        engine.active_incidents().is_empty(),
        "no ACTIVE incident remains on the completed container (#1508)"
    );
    assert!(
        engine.is_completed(inst),
        "the instance reaches EndInterrupted and completes"
    );
}

/// Issue #1508 follow-up — the error-catch branch must not RESURRECT the
/// caught container's own FAILED agent job. The agent job fails with zero
/// retries (a `JobNoRetries` incident on the container root), then an
/// externally activated tool throws a BPMN error caught by the container's
/// error boundary. Resolving the root incident used to reset the still-`Failed`
/// agent job to `Created`, leaving it activatable against a completed
/// container; it must be cancelled first.
#[test]
fn error_caught_at_adhoc_cancels_the_failed_agent_job_before_resolving() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:adHocSubProcess id="Host">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-agent" />
              <zeebe:adHoc outputCollection="r" outputElement="=toolCallResult" />
            </bpmn:extensionElements>
            <bpmn:serviceTask id="Tool">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="probe-tool" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
          </bpmn:adHocSubProcess>
          <bpmn:boundaryEvent id="Bnd" attachedToRef="Host">
            <bpmn:errorEventDefinition errorRef="E" />
          </bpmn:boundaryEvent>
          <bpmn:endEvent id="EndNormal" />
          <bpmn:endEvent id="EndCaught" />
          <bpmn:sequenceFlow id="F1" sourceRef="s" targetRef="Host" />
          <bpmn:sequenceFlow id="F2" sourceRef="Host" targetRef="EndNormal" />
          <bpmn:sequenceFlow id="F3" sourceRef="Bnd" targetRef="EndCaught" />
        </bpmn:process>
        <bpmn:error id="E" errorCode="TOOL_ERR" />
      </bpmn:definitions>"#;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            crate::bpmn::parse_bpmn(xml).unwrap().remove(0),
        ))
        .unwrap();
    let inst = engine
        .apply_command(Command::create_instance("p"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let agent = engine
        .activate_jobs("probe-agent", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "Host")
        .expect("agent job emitted for the ad-hoc container");
    engine
        .apply_command(Command::fail_job(agent.key, 0, "boom"))
        .unwrap();
    assert_eq!(engine.active_incidents().len(), 1);

    engine
        .apply_command(Command::ActivateAdHocActivities {
            ad_hoc_instance_key: agent.element_instance_key,
            activate_elements: vec![activate_element("Tool")],
            cancel_remaining: false,
        })
        .unwrap();
    let tool = engine
        .activate_jobs("probe-tool", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "Tool")
        .expect("tool job emitted");
    let events = engine
        .apply_command(Command::throw_job_error(tool.key, "TOOL_ERR", "bad"))
        .unwrap();

    let cancel_pos = events
        .iter()
        .position(|e| matches!(e, Event::JobCanceled { job_key, .. } if *job_key == agent.key))
        .expect("the failed agent job is cancelled when the container is caught");
    let resolve_pos = events
        .iter()
        .position(|e| matches!(e, Event::IncidentResolved { .. }))
        .expect("the root incident is resolved");
    assert!(cancel_pos < resolve_pos, "cancel precedes resolve");
    assert!(
        matches!(
            engine.job(agent.key).map(|j| &j.state),
            Some(crate::state::JobState::Canceled)
        ),
        "the agent job must stay Canceled, not be resurrected to Created"
    );
    assert!(
        engine
            .activate_jobs("probe-agent", "W", 10, 1_000, 0)
            .is_empty(),
        "no agent job is activatable against the completed container"
    );
    assert!(engine.active_incidents().is_empty());
    assert!(engine.is_completed(inst));
}
