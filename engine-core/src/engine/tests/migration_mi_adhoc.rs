//! #1630 — migration of active multi-instance bodies and ad-hoc sub-processes
//! (Zeebe `MigrateParallelMultiInstanceBodyTest`,
//! `MigrateSequentialMultiInstanceBodyTest`, `MigrateAdHocSubProcessTest`
//! parity), plus the MI <-> non-MI / loop-kind rejections
//! (`ProcessInstanceMigrationPreconditions.requireSameElementType` /
//! `requireSameMultiInstanceLoopCharacteristics`).
use super::*;

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(s, t)| (s.to_string(), t.to_string()))
        .collect()
}

/// `start -> task (MI over [1,2,3], item -> item * 10 into results) -> end`.
/// `loop_kind`: `None` = plain task, `Some(sequential)` = multi-instance.
fn mi_task(process: &str, task: &str, loop_kind: Option<bool>, output: &str) -> ProcessDefinition {
    let mi = match loop_kind {
        None => String::new(),
        Some(sequential) => format!(
            r#"<bpmn:multiInstanceLoopCharacteristics isSequential="{sequential}">
                <bpmn:extensionElements>
                  <zeebe:loopCharacteristics inputCollection="=[1,2,3]" inputElement="item"
                    outputCollection="{output}" outputElement="=item * 10" />
                </bpmn:extensionElements>
              </bpmn:multiInstanceLoopCharacteristics>"#
        ),
    };
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="{process}">
            <bpmn:startEvent id="s" />
            <bpmn:serviceTask id="{task}">
              <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
              {mi}
            </bpmn:serviceTask>
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="{task}" />
            <bpmn:sequenceFlow id="f2" sourceRef="{task}" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

/// A JOB_WORKER ad-hoc container `container` with service-task tools `tool_a` /
/// `tool_b` (`tool` jobs), collecting each tool's `result` into `results`.
fn adhoc(process: &str, container: &str, tool_a: &str, tool_b: &str) -> ProcessDefinition {
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="{process}">
            <bpmn:startEvent id="s" />
            <bpmn:adHocSubProcess id="{container}">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="agent-worker" />
                <zeebe:adHoc outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="{tool_a}">
                <bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="{tool_b}">
                <bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="{container}" />
            <bpmn:sequenceFlow id="f2" sourceRef="{container}" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

fn deploy(engine: &mut Engine, log: &mut Vec<Event>, def: ProcessDefinition) -> Key {
    let events = engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap();
    log.extend(events);
    key
}

fn run(engine: &mut Engine, log: &mut Vec<Event>, cmd: Command) -> Vec<Event> {
    let events = engine.apply_command(cmd).unwrap();
    log.extend(events.iter().cloned());
    events
}

fn last_list_value(log: &[Event], name: &str) -> Option<Vec<Value>> {
    log.iter().rev().find_map(|e| match e {
        Event::VariablesUpdated { variables, .. } => match variables.get(name) {
            Some(Value::List(l)) => Some(l.clone()),
            _ => None,
        },
        _ => None,
    })
}

/// Replaying the journal rebuilds the same instance (the migration applier is
/// replay-safe). Compared per instance: job activation is not journaled here.
fn assert_replays(log: &[Event], engine: &Engine, inst: Key) {
    let recovered = Engine::replay(log.to_vec());
    assert_eq!(recovered.instance(inst), engine.instance(inst));
}

fn ints(v: &[i64]) -> Vec<Value> {
    v.iter().map(|n| Value::Int(*n)).collect()
}

/// Drive a MI service task mid-flight (one child completed), migrate it onto a
/// renamed target, then finish the loop on the target.
fn assert_mi_migrates_mid_flight(sequential: bool) {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        mi_task("src", "a", Some(sequential), "results"),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        mi_task("tgt", "b", Some(sequential), "results"),
    );
    let created = run(&mut engine, &mut log, Command::create_instance("src"));
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Complete the first child before migrating.
    let first = engine.activate_jobs("work", "W", 1, 1_000, 0);
    run(&mut engine, &mut log, Command::complete_job(first[0].key));

    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(inst, target_key, pairs(&[("a", "b")])),
    );

    let live = engine.instance(inst).unwrap();
    assert_eq!(live.process_id, "tgt");
    assert!(
        live.active.values().all(|id| id == "b"),
        "body and inner instances re-pointed: {:?}",
        live.active
    );
    let bodies: Vec<&state::MultiInstanceState> = live.multi_instances.values().collect();
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0].element_id, "b", "MI bookkeeping re-pointed");
    assert_eq!(bodies[0].spawned, if sequential { 2 } else { 3 });

    // The recovered engine agrees (the applier is replay-safe).
    assert_replays(&log, &engine, inst);

    // Finish the loop on the target definition.
    loop {
        let jobs = engine.activate_jobs("work", "W", 10, 1_000, 0);
        if jobs.is_empty() {
            break;
        }
        for job in jobs {
            assert_eq!(job.element_id, "b", "jobs run under the target element");
            run(&mut engine, &mut log, Command::complete_job(job.key));
        }
    }
    assert!(engine.is_completed(inst), "migrated MI body completes");
    assert_eq!(
        last_list_value(&log, "results"),
        Some(ints(&[10, 20, 30])),
        "output collection keeps the pre-migration child and collects the rest"
    );
    assert_replays(&log, &engine, inst);
}

#[test]
fn migration_of_parallel_multi_instance_mid_flight() {
    assert_mi_migrates_mid_flight(false);
}

#[test]
fn migration_of_sequential_multi_instance_mid_flight() {
    assert_mi_migrates_mid_flight(true);
}

#[test]
fn migrated_multi_instance_adopts_target_output_collection() {
    // Zeebe reads the loop characteristics from the target after migration
    // (`shouldRaiseIncidentOnCompletionAfterChangingOutputCollection`).
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        mi_task("src", "a", Some(false), "results"),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        mi_task("tgt", "b", Some(false), "results2"),
    );
    let created = run(&mut engine, &mut log, Command::create_instance("src"));
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(inst, target_key, pairs(&[("a", "b")])),
    );
    let body = engine
        .instance(inst)
        .unwrap()
        .multi_instances
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(body.output_collection.as_deref(), Some("results2"));
    assert_replays(&log, &engine, inst);
}

#[test]
fn migration_of_adhoc_sub_process_with_active_inner_instances() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, adhoc("p", "agent", "toolA", "toolB"));
    let target_key = deploy(
        &mut engine,
        &mut log,
        adhoc("p2", "agent2", "toolA2", "toolB2"),
    );
    let created = run(&mut engine, &mut log, Command::create_instance("p"));
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    let agent = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    let container = agent[0].element_instance_key;
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with_result(
            agent[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA"), activate_element("toolB")],
                ..Default::default()
            },
        ),
    );
    // Complete one tool before migrating, leaving one inner instance active.
    let tools = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tools.len(), 2);
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("A".to_string()));
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with(tools[0].key, vars),
    );

    // Only the container and its tools are mapped: the `#innerInstance`
    // mapping is derived (Zeebe adds it implicitly).
    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(
            inst,
            target_key,
            pairs(&[
                ("agent", "agent2"),
                ("toolA", "toolA2"),
                ("toolB", "toolB2"),
            ]),
        ),
    );
    let live = engine.instance(inst).unwrap();
    assert_eq!(
        live.active.get(&container).map(String::as_str),
        Some("agent2")
    );
    assert!(
        live.active.values().any(|id| id == "agent2#innerInstance"),
        "inner instance re-pointed: {:?}",
        live.active
    );
    assert!(
        !live
            .active
            .values()
            .any(|id| id.starts_with("agent#") || id == "agent" || id == "toolA" || id == "toolB"),
        "no source ids remain: {:?}",
        live.active
    );
    assert_eq!(
        live.adhoc_instances
            .get(&container)
            .map(|a| a.element_id.as_str()),
        Some("agent2"),
        "ad-hoc bookkeeping re-pointed"
    );
    assert_replays(&log, &engine, inst);

    // Finish the remaining tool; the agent re-runs on the target container.
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("B".to_string()));
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with(tools[1].key, vars),
    );
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent2")
        .expect("agent job re-emitted on the target container");
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                completion_condition_fulfilled: true,
                ..Default::default()
            },
        ),
    );
    assert!(engine.is_completed(inst), "migrated container completes");
    let mut results = last_list_value(&log, "results").expect("outputCollection written");
    results.sort_by_key(|v| format!("{v:?}"));
    assert_eq!(
        results,
        vec![Value::Str("A".to_string()), Value::Str("B".to_string())]
    );
    assert_replays(&log, &engine, inst);
}

#[test]
fn migration_rejects_explicit_inner_instance_mapping() {
    // Zeebe `shouldRejectMigrationWhenInnerInstanceInSourceButNotInTarget`: the
    // `#innerInstance` id is not a definition element.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, adhoc("p", "agent", "toolA", "toolB"));
    let target_key = deploy(
        &mut engine,
        &mut log,
        adhoc("p2", "agent2", "toolA2", "toolB2"),
    );
    let inst = create_instance_key(&mut engine, "p");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[
                ("agent", "agent2"),
                ("agent#innerInstance", "agent2#innerInstance"),
            ]),
        ))
        .unwrap_err();
    assert!(
        matches!(
            err,
            EngineError::MappingSourceElementNotFound { ref element_id, .. }
                if element_id == "agent#innerInstance"
        ),
        "{err:?}"
    );
}

fn mi_mismatch(source: Option<bool>, target: Option<bool>) -> EngineError {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        mi_task("src", "a", source, "results"),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        mi_task("tgt", "b", target, "results"),
    );
    let inst = create_instance_key(&mut engine, "src");
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("a", "b")]),
        ))
        .unwrap_err()
}

#[test]
fn migration_rejects_multi_instance_to_plain_with_zeebe_message() {
    let err = mi_mismatch(Some(false), None);
    let EngineError::MigrationMultiInstanceMismatch { instance_key, .. } = err else {
        panic!("{err:?}");
    };
    assert_eq!(
        err.to_string(),
        format!(
            "Expected to migrate process instance '{instance_key}' but active element with id 'a' and type 'MULTI_INSTANCE_BODY' is mapped to an element with id 'b' and different type 'SERVICE_TASK'. Elements must be mapped to elements of the same type."
        )
    );
}

#[test]
fn migration_rejects_plain_to_multi_instance_with_zeebe_message() {
    let err = mi_mismatch(None, Some(true));
    let EngineError::MigrationMultiInstanceMismatch { instance_key, .. } = err else {
        panic!("{err:?}");
    };
    assert_eq!(
        err.to_string(),
        format!(
            "Expected to migrate process instance '{instance_key}' but active element with id 'a' and type 'SERVICE_TASK' is mapped to an element with id 'b' and different type 'MULTI_INSTANCE_BODY'. Elements must be mapped to elements of the same type."
        )
    );
}

#[test]
fn migration_rejects_loop_characteristics_change_with_zeebe_message() {
    for (s, t) in [(false, true), (true, false)] {
        let err = mi_mismatch(Some(s), Some(t));
        let EngineError::MigrationLoopCharacteristicsChanged { instance_key, .. } = err else {
            panic!("{err:?}");
        };
        assert_eq!(
            err.to_string(),
            format!(
                "Expected to migrate process instance '{instance_key}' but active element with id 'a' has a different loop characteristics than the target element with id 'b'. Both elements must have either sequential or parallel loop characteristics."
            )
        );
    }
}

#[test]
fn migration_rejects_adhoc_container_mapped_to_plain_task() {
    // The container's `#innerInstance`s are derived from the container: a
    // target that is not an ad-hoc container could not host them.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, adhoc("p", "agent", "toolA", "toolB"));
    let target_key = deploy(&mut engine, &mut log, mi_task("tgt", "b", None, "results"));
    let inst = create_instance_key(&mut engine, "p");
    let err = engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("agent", "b")]),
        ))
        .unwrap_err();
    assert!(
        matches!(
            err,
            EngineError::MappedElementTypeChanged { ref source_element_id, .. }
                if source_element_id == "agent"
        ),
        "{err:?}"
    );
}

/// An outer JOB_WORKER ad-hoc container `agent` whose single tool `sub` is
/// either a NESTED ad-hoc sub-process (`nested = true`, agent-of-agents #631) or
/// a PLAIN service task (`nested = false`). Both catalogue `sub` as
/// `AdHocToolKind::ServiceTask`, so only the container-status check tells them
/// apart.
fn adhoc_with_tool(process: &str, nested: bool) -> ProcessDefinition {
    let tool = if nested {
        r#"<bpmn:adHocSubProcess id="sub">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="sub-agent-worker" />
                <zeebe:adHoc outputCollection="subResults" outputElement="=leafOut" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="leaf">
                <bpmn:extensionElements><zeebe:taskDefinition type="leaf-tool" /></bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>"#
            .to_string()
    } else {
        r#"<bpmn:serviceTask id="sub">
              <bpmn:extensionElements><zeebe:taskDefinition type="sub-agent-worker" /></bpmn:extensionElements>
            </bpmn:serviceTask>"#
            .to_string()
    };
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="{process}">
            <bpmn:startEvent id="s" />
            <bpmn:adHocSubProcess id="agent">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="agent-worker" />
                <zeebe:adHoc outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
              {tool}
            </bpmn:adHocSubProcess>
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
            <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

/// Migrate a freshly-created `adhoc_with_tool` instance, mapping the container
/// `agent -> agent` and its tool `sub -> sub`, returning the rejection.
fn migrate_tool(source_nested: bool, target_nested: bool) -> EngineError {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(&mut engine, &mut log, adhoc_with_tool("src", source_nested));
    let target_key = deploy(&mut engine, &mut log, adhoc_with_tool("tgt", target_nested));
    let inst = create_instance_key(&mut engine, "src");
    engine
        .apply_command(Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("agent", "agent"), ("sub", "sub")]),
        ))
        .unwrap_err()
}

#[test]
fn migration_rejects_nested_container_tool_mapped_to_plain_service_task_tool() {
    // Both catalogue `sub` as `AdHocToolKind::ServiceTask`, so the discriminant
    // check alone accepts the mapping; but the source `sub` is an ad-hoc
    // container whose derived `#innerInstance` and ad-hoc bookkeeping have no
    // container definition on the plain-service-task target.
    let err = migrate_tool(true, false);
    assert!(
        matches!(
            err,
            EngineError::MappedElementTypeChanged { ref source_element_id, .. }
                if source_element_id == "sub"
        ),
        "{err:?}"
    );
}

#[test]
fn migration_rejects_plain_service_task_tool_mapped_to_nested_container_tool() {
    // The reverse direction must reject too: a plain service-task tool migrated
    // onto a nested ad-hoc container would gain a container definition (and
    // derived inner instances) it never had.
    let err = migrate_tool(false, true);
    assert!(
        matches!(
            err,
            EngineError::MappedElementTypeChanged { ref source_element_id, .. }
                if source_element_id == "sub"
        ),
        "{err:?}"
    );
}

/// `mi_task` plus a non-interrupting timer boundary `timer` on the MI activity
/// when `with_boundary` — the target side of the boundary-reconciliation test.
fn mi_task_with_boundary(
    process: &str,
    task: &str,
    output: &str,
    with_boundary: bool,
) -> ProcessDefinition {
    let boundary = if with_boundary {
        format!(
            r#"<bpmn:boundaryEvent id="timer" attachedToRef="{task}" cancelActivity="false">
                <bpmn:timerEventDefinition><bpmn:timeDuration>PT60S</bpmn:timeDuration></bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="caught" />
              <bpmn:sequenceFlow id="f3" sourceRef="timer" targetRef="caught" />"#
        )
    } else {
        String::new()
    };
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="{process}">
            <bpmn:startEvent id="s" />
            <bpmn:serviceTask id="{task}">
              <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
              <bpmn:multiInstanceLoopCharacteristics isSequential="false">
                <bpmn:extensionElements>
                  <zeebe:loopCharacteristics inputCollection="=[1,2,3]" inputElement="item"
                    outputCollection="{output}" outputElement="=item * 10" />
                </bpmn:extensionElements>
              </bpmn:multiInstanceLoopCharacteristics>
            </bpmn:serviceTask>
            {boundary}
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="{task}" />
            <bpmn:sequenceFlow id="f2" sourceRef="{task}" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

#[test]
fn migration_arms_a_newly_added_boundary_on_the_mi_body_only() {
    // The target adds a boundary to a live parallel multi-instance activity. The
    // boundary belongs to the BODY (it interrupts / spawns beside the whole
    // loop), so it must be armed exactly once — not once per active child, even
    // though the children share the body's element id.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        mi_task_with_boundary("src", "a", "results", false),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        mi_task_with_boundary("tgt", "b", "results", true),
    );
    let created = run(&mut engine, &mut log, Command::create_instance("src"));
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The parallel body fans out three children before the migration.
    let body = engine
        .instance(inst)
        .unwrap()
        .multi_instances
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(body.active.len(), 3, "three children in flight");

    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(inst, target_key, pairs(&[("a", "b")])),
    );

    // Exactly one boundary timer is armed, on the body — never on a child.
    let armed: Vec<Key> = engine
        .state()
        .timers
        .values()
        .filter(|t| t.state == state::TimerState::Created && t.element_id == "b")
        .map(|t| t.element_instance_key)
        .collect();
    let body_key = engine
        .instance(inst)
        .unwrap()
        .multi_instances
        .keys()
        .next()
        .copied()
        .unwrap();
    assert_eq!(
        armed,
        vec![body_key],
        "the boundary is armed once, on the body (children: {:?})",
        body.active
    );
    assert_replays(&log, &engine, inst);
}

/// A JOB_WORKER ad-hoc container `agent` whose single tool `tool` carries a
/// failing input `zeebe:ioMapping` (`weighted = base + 1`, with `base` a
/// string), so activating it parks an `AdHocToolActivation` incident.
fn adhoc_tool_with_failing_input(process: &str, container: &str, tool: &str) -> ProcessDefinition {
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="{process}">
            <bpmn:startEvent id="s" />
            <bpmn:adHocSubProcess id="{container}">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="agent-worker" />
                <zeebe:adHoc outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="{tool}">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="tool" />
                  <zeebe:ioMapping>
                    <zeebe:input source="=assert(base, base instance of number) + 1" target="weighted" />
                  </zeebe:ioMapping>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="{container}" />
            <bpmn:sequenceFlow id="f2" sourceRef="{container}" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

#[test]
fn migration_remaps_a_pending_adhoc_tool_activation_incident() {
    // A tool parked on an `AdHocToolActivation` incident carries the SOURCE tool
    // id in its redrive payload. Migration renames the tool; resolving the
    // incident afterwards must re-activate the TARGET tool, not fall through on
    // the unknown source id.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        adhoc_tool_with_failing_input("src", "agent", "toolA"),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        adhoc_tool_with_failing_input("tgt", "agent2", "toolA2"),
    );
    let created = run(
        &mut engine,
        &mut log,
        Command::create_instance_with("src", vars(&[("base", Value::Str("oops".into()))])),
    );
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Activate the tool: its input mapping `=base + 1` fails (string + int),
    // parking an AdHocToolActivation incident on the container.
    let agent = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with_result(
            agent[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ),
    );
    let incident = engine.incidents()[0].key;
    assert!(
        matches!(
            engine.incident(incident).unwrap().redrive,
            Some(state::IoMappingRedrive::AdHocToolActivation { .. })
        ),
        "parked on an AdHocToolActivation redrive"
    );

    // Migrate, renaming the tool. The pending redrive payload must follow.
    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("agent", "agent2"), ("toolA", "toolA2")]),
        ),
    );
    match &engine.incident(incident).unwrap().redrive {
        Some(state::IoMappingRedrive::AdHocToolActivation { element_id, .. }) => {
            assert_eq!(element_id, "toolA2", "the redrive names the target tool");
        }
        other => panic!("expected an AdHocToolActivation redrive, got {other:?}"),
    }
    assert_replays(&log, &engine, inst);

    // Fix `base` and resolve: the TARGET tool re-activates and mints its job.
    run(
        &mut engine,
        &mut log,
        Command::set_variables(inst, HashMap::from([("base".to_string(), Value::Int(5))])),
    );
    run(&mut engine, &mut log, Command::resolve_incident(incident));
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(
        tool_jobs.len(),
        1,
        "the target tool job is minted on resolution"
    );
    assert_eq!(tool_jobs[0].element_id, "toolA2");
    assert_eq!(tool_jobs[0].variables.get("weighted"), Some(&Value::Int(6)));
    assert_replays(&log, &engine, inst);
}

/// A JOB_WORKER ad-hoc container `agent` whose single tool `tool` declares its
/// job type as the FEEL expression `=toolType`, so activating it while
/// `toolType` is unbound parks an `AdHocToolJobType` incident on the container.
fn adhoc_tool_with_failing_job_type(
    process: &str,
    container: &str,
    tool: &str,
) -> ProcessDefinition {
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="{process}">
            <bpmn:startEvent id="s" />
            <bpmn:adHocSubProcess id="{container}">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="agent-worker" />
                <zeebe:adHoc outputCollection="results" outputElement="=result" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="{tool}">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="=toolType" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="{container}" />
            <bpmn:sequenceFlow id="f2" sourceRef="{container}" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

#[test]
fn migration_remaps_a_pending_adhoc_tool_job_type_incident() {
    // Same class as the activation redrive, on the job-type failure path: the
    // `AdHocToolJobType` redrive carries the SOURCE tool id, so a tool rename
    // must re-point it or resolution retries an unknown id.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        adhoc_tool_with_failing_job_type("src", "agent", "toolA"),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        adhoc_tool_with_failing_job_type("tgt", "agent2", "toolA2"),
    );
    let created = run(&mut engine, &mut log, Command::create_instance("src"));
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Activate the tool while `toolType` is unbound: the job-type expression
    // fails before the tool child exists, parking an AdHocToolJobType incident.
    let agent = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with_result(
            agent[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ),
    );
    let incident = engine.incidents()[0].key;
    assert!(
        matches!(
            engine.incident(incident).unwrap().redrive,
            Some(state::IoMappingRedrive::AdHocToolJobType { .. })
        ),
        "parked on an AdHocToolJobType redrive"
    );

    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("agent", "agent2"), ("toolA", "toolA2")]),
        ),
    );
    match &engine.incident(incident).unwrap().redrive {
        Some(state::IoMappingRedrive::AdHocToolJobType { element_id, .. }) => {
            assert_eq!(element_id, "toolA2", "the redrive names the target tool");
        }
        other => panic!("expected an AdHocToolJobType redrive, got {other:?}"),
    }
    assert_replays(&log, &engine, inst);

    // Fix `toolType` and resolve: the TARGET tool re-activates and mints its job.
    run(
        &mut engine,
        &mut log,
        Command::set_variables(
            inst,
            HashMap::from([("toolType".to_string(), Value::Str("tool".into()))]),
        ),
    );
    run(&mut engine, &mut log, Command::resolve_incident(incident));
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(
        tool_jobs.len(),
        1,
        "the target tool job is minted on resolution"
    );
    assert_eq!(tool_jobs[0].element_id, "toolA2");
    assert_replays(&log, &engine, inst);
}

/// A JOB_WORKER ad-hoc container `container` with one service-task tool `tool`,
/// collecting each tool's `result` into the named `output` collection.
fn adhoc_collecting(process: &str, container: &str, tool: &str, output: &str) -> ProcessDefinition {
    let xml = format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
          <bpmn:process id="{process}">
            <bpmn:startEvent id="s" />
            <bpmn:adHocSubProcess id="{container}">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="agent-worker" />
                <zeebe:adHoc outputCollection="{output}" outputElement="=result" />
              </bpmn:extensionElements>
              <bpmn:serviceTask id="{tool}">
                <bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements>
              </bpmn:serviceTask>
            </bpmn:adHocSubProcess>
            <bpmn:endEvent id="e" />
            <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="{container}" />
            <bpmn:sequenceFlow id="f2" sourceRef="{container}" targetRef="e" />
          </bpmn:process>
        </bpmn:definitions>"#
    );
    crate::bpmn::parse_bpmn(&xml).unwrap().remove(0)
}

#[test]
fn migration_raises_a_recoverable_incident_when_the_renamed_output_collection_is_missing() {
    // Zeebe `MigrateAdHocSubProcessTest.shouldRaiseIncidentOnCompletionAfter
    // ChangingOutputCollection`: migration renames the container's
    // `outputCollection`, but the accumulated list stays under its SOURCE name in
    // the container scope. Completing a tool against the missing TARGET binding
    // must raise a recoverable completion incident — never silently discard the
    // output and publish an empty collection.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        adhoc_collecting("src", "agent", "toolA", "results"),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        adhoc_collecting("tgt", "agent2", "toolA2", "results2"),
    );
    let created = run(&mut engine, &mut log, Command::create_instance("src"));
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Activate the tool, then migrate (renaming the output collection) BEFORE
    // the tool completes.
    let agent = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    let container = agent[0].element_instance_key;
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with_result(
            agent[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ),
    );
    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(
            inst,
            target_key,
            pairs(&[("agent", "agent2"), ("toolA", "toolA2")]),
        ),
    );
    // The container's ad-hoc state now names the TARGET collection, but the
    // container scope still holds only the SOURCE binding.
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .adhoc_instances
            .get(&container)
            .unwrap()
            .output_collection
            .as_deref(),
        Some("results2")
    );

    // Completing the tool must NOT silently discard its output: a recoverable
    // incident parks on the tool child instead of the append.
    let tool = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool.len(), 1);
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("A".to_string()));
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with(tool[0].key, vars),
    );
    let incident = engine
        .active_incidents()
        .into_iter()
        .find(|i| i.instance_key == inst)
        .unwrap_or_else(|| panic!("a recoverable incident is raised: {:?}", engine.incidents()))
        .key;
    assert_replays(&log, &engine, inst);

    // Repair: seed the renamed collection LOCAL to the container scope (where
    // the append reads it), then resolve — the append lands.
    run(
        &mut engine,
        &mut log,
        Command::set_variables_scoped(
            container,
            HashMap::from([("results2".to_string(), Value::List(Vec::new()))]),
            true,
        ),
    );
    run(&mut engine, &mut log, Command::resolve_incident(incident));
    assert!(
        engine.active_incidents().is_empty(),
        "the incident resolves after the collection is repaired"
    );
    let collected = engine
        .instance(inst)
        .unwrap()
        .scope_variables
        .get(&container)
        .and_then(|m| m.get("results2"))
        .cloned();
    assert_eq!(
        collected,
        Some(Value::List(vec![Value::Str("A".to_string())])),
        "the repaired collection receives the tool output"
    );
    assert_replays(&log, &engine, inst);
}

/// The `elementId`s advertised in a container scope's `adHocSubProcessElements`
/// catalog variable, in order.
fn advertised_tool_ids(engine: &Engine, inst: Key, container: Key) -> Vec<String> {
    engine
        .instance(inst)
        .unwrap()
        .scope_variables
        .get(&container)
        .and_then(|m| m.get("adHocSubProcessElements"))
        .and_then(|v| match v {
            Value::List(items) => Some(
                items
                    .iter()
                    .filter_map(|e| match e {
                        Value::Map(m) => m.get("elementId").cloned(),
                        _ => None,
                    })
                    .map(|id| match id {
                        Value::Str(s) => s,
                        other => panic!("elementId is a string, got {other:?}"),
                    })
                    .collect(),
            ),
            _ => None,
        })
        .expect("the container advertises its tool catalog")
}

#[test]
fn migration_refreshes_the_advertised_adhoc_tool_catalog() {
    // After migration renames the tools, the container's engine-owned
    // `adHocSubProcessElements` variable must advertise the TARGET ids — the
    // next agent turn reads it to choose tools, and a stale source id would be
    // rejected by the target catalog.
    let mut engine = Engine::new();
    let mut log = Vec::new();
    deploy(
        &mut engine,
        &mut log,
        adhoc("src", "agent", "toolA", "toolB"),
    );
    let target_key = deploy(
        &mut engine,
        &mut log,
        adhoc("tgt", "agent2", "toolA2", "toolB2"),
    );
    let created = run(&mut engine, &mut log, Command::create_instance("src"));
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    let agent = engine.activate_jobs("agent-worker", "W", 10, 1_000, 0);
    let container = agent[0].element_instance_key;
    assert_eq!(
        advertised_tool_ids(&engine, inst, container),
        vec!["toolA".to_string(), "toolB".to_string()],
        "the source catalog is advertised before migration"
    );

    // Activate toolA so the container parks with a tool in flight; the agent job
    // is re-emitted only once that tool drains.
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with_result(
            agent[0].key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolA")],
                ..Default::default()
            },
        ),
    );

    run(
        &mut engine,
        &mut log,
        Command::migrate_instance(
            inst,
            target_key,
            pairs(&[
                ("agent", "agent2"),
                ("toolA", "toolA2"),
                ("toolB", "toolB2"),
            ]),
        ),
    );

    assert_eq!(
        advertised_tool_ids(&engine, inst, container),
        vec!["toolA2".to_string(), "toolB2".to_string()],
        "the catalog is refreshed to the target tool ids"
    );
    assert_replays(&log, &engine, inst);

    // Drain the in-flight tool; the container re-emits its agent job for the
    // next turn. That job advertises the refreshed (target) catalog, and the
    // agent can activate a TARGET tool by its advertised id.
    let tool = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool.len(), 1, "the migrated tool is still in flight");
    assert_eq!(tool[0].element_id, "toolA2");
    let mut vars = HashMap::new();
    vars.insert("result".to_string(), Value::Str("A".to_string()));
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with(tool[0].key, vars),
    );
    let agent2 = engine
        .activate_jobs("agent-worker", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.element_id == "agent2")
        .expect("the agent job is re-emitted on the target container");
    let advertised = agent2
        .variables
        .get("adHocSubProcessElements")
        .and_then(|v| match v {
            Value::List(items) => Some(items),
            _ => None,
        })
        .expect("the re-emitted agent job advertises a catalog");
    let ids: Vec<&str> = advertised
        .iter()
        .filter_map(|e| match e {
            Value::Map(m) => m.get("elementId"),
            _ => None,
        })
        .map(|id| match id {
            Value::Str(s) => s.as_str(),
            other => panic!("elementId is a string, got {other:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec!["toolA2", "toolB2"],
        "the next agent turn discovers the target tools"
    );
    run(
        &mut engine,
        &mut log,
        Command::complete_job_with_result(
            agent2.key,
            HashMap::new(),
            crate::model::AdHocJobResult {
                activate_elements: vec![activate_element("toolB2")],
                ..Default::default()
            },
        ),
    );
    let tool_jobs = engine.activate_jobs("tool", "W", 10, 1_000, 0);
    assert_eq!(tool_jobs.len(), 1, "the advertised target tool activates");
    assert_eq!(tool_jobs[0].element_id, "toolB2");
    assert_replays(&log, &engine, inst);
}
