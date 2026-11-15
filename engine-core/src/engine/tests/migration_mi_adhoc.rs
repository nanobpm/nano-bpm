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
