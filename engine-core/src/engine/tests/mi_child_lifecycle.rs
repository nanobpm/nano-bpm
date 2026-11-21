//! Multi-instance child lifecycle gaps (#1724): the activation marker, the
//! completion-condition cancellation of user-task children, and the per-child
//! output mappings.
use super::*;
use crate::model::MultiInstance;

fn mi(sequential: bool) -> MultiInstance {
    MultiInstance {
        input_collection: "=items".to_string(),
        input_element: Some("item".to_string()),
        output_collection: None,
        output_element: None,
        completion_condition: None,
        cancel_remaining_instances: true,
        sequential,
    }
}

fn start(
    def: ProcessDefinition,
    proc_id: &str,
    vars_in: BTreeMap<String, Value>,
) -> (Engine, Key, Vec<Event>) {
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(proc_id, vars_in))
        .unwrap();
    let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
    (engine, inst, events)
}

/// The `ElementActivated` events for `eik` in `events`.
fn activated_markers(events: &[Event], eik: Key) -> usize {
    events
        .iter()
        .filter(|e| {
            matches!(e, Event::ElementActivated { element_instance_key, .. }
                if *element_instance_key == eik)
        })
        .count()
}

/// The single active incident's element instance (the MI child).
fn incident_child(engine: &Engine) -> (Key, Key) {
    let active = engine.active_incidents();
    assert_eq!(active.len(), 1, "exactly one incident: {active:?}");
    (active[0].key, active[0].element_instance_key)
}

/// Asserts the child is parked in ACTIVATING with no marker, then fixes the
/// failure (`fix`), resolves, and asserts exactly one marker is emitted, and
/// a second (no-op) path never adds another.
fn assert_marker_withheld_then_emitted_once(
    mut engine: Engine,
    inst: Key,
    created: Vec<Event>,
    fix: BTreeMap<String, Value>,
) -> Engine {
    let (incident, child) = incident_child(&engine);
    assert_eq!(
        activated_markers(&created, child),
        0,
        "a child whose activation failed must not be reported ACTIVATED"
    );
    assert!(
        engine.instance(inst).unwrap().activating.contains(&child),
        "the child is parked in ACTIVATING"
    );
    engine
        .apply_command(Command::set_variables(inst, fix))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident))
        .unwrap();
    assert!(engine.active_incidents().is_empty(), "redrive succeeded");
    assert_eq!(
        activated_markers(&resolved, child),
        1,
        "the successful redrive emits ElementActivated exactly once"
    );
    assert!(!engine.instance(inst).unwrap().activating.contains(&child));
    engine
}

#[test]
fn mi_child_job_property_failure_withholds_activated_until_redrive() {
    let def = ProcessBuilder::new("mi-jt")
        .start_event("start")
        .service_task("work", "=jobType")
        .with_multi_instance("work", mi(true))
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let (engine, inst, created) = start(
        def,
        "mi-jt",
        vars(&[("items", Value::List(vec![Value::Int(1)]))]),
    );
    let mut engine = assert_marker_withheld_then_emitted_once(
        engine,
        inst,
        created,
        vars(&[("jobType", Value::Str("w".into()))]),
    );
    complete_one(&mut engine, "w");
    assert!(engine.is_completed(inst));
}

#[test]
fn mi_child_input_mapping_failure_withholds_activated_until_redrive() {
    let def = ProcessBuilder::new("mi-io")
        .start_event("start")
        .service_task("work", "w")
        .with_multi_instance("work", mi(false))
        .with_io(
            "work",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=assert(factor, factor instance of number) + item".to_string(),
                    target: "weighted".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let (engine, inst, created) = start(
        def,
        "mi-io",
        vars(&[
            ("items", Value::List(vec![Value::Int(1)])),
            ("factor", Value::Str("x".into())),
        ]),
    );
    let mut engine = assert_marker_withheld_then_emitted_once(
        engine,
        inst,
        created,
        vars(&[("factor", Value::Int(2))]),
    );
    complete_one(&mut engine, "w");
    assert!(engine.is_completed(inst));
}

#[test]
fn mi_user_task_expression_failure_withholds_activated_until_redrive() {
    let def = ProcessBuilder::new("mi-ut")
        .start_event("start")
        .user_task_with(
            "review",
            crate::model::UserTaskProps {
                due_date: Some("=due".to_string()),
                ..Default::default()
            },
        )
        .with_multi_instance("review", mi(false))
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let (engine, inst, created) = start(
        def,
        "mi-ut",
        vars(&[
            ("items", Value::List(vec![Value::Int(1)])),
            ("due", Value::Int(5)),
        ]),
    );
    assert!(engine.state().user_tasks.is_empty(), "no task while parked");
    let engine = assert_marker_withheld_then_emitted_once(
        engine,
        inst,
        created,
        vars(&[("due", Value::Str("2026-01-01T00:00:00Z".into()))]),
    );
    assert_eq!(
        engine.state().user_tasks.len(),
        1,
        "task created on redrive"
    );
}

/// A successful activation keeps the pre-#1724 journal order:
/// `ElementActivating`, `ElementActivated`, `MultiInstanceChildActivated`.
#[test]
fn mi_child_success_keeps_activation_marker_order() {
    let def = ProcessBuilder::new("mi-ok")
        .start_event("start")
        .service_task("work", "w")
        .with_multi_instance("work", mi(false))
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let (_, _, created) = start(
        def,
        "mi-ok",
        vars(&[("items", Value::List(vec![Value::Int(1), Value::Int(2)]))]),
    );
    let children: Vec<Key> = created
        .iter()
        .filter_map(|e| match e {
            Event::MultiInstanceChildActivated { child_key, .. } => Some(*child_key),
            _ => None,
        })
        .collect();
    assert_eq!(children.len(), 2);
    for child in children {
        let pos = |f: &dyn Fn(&Event) -> bool| created.iter().position(f).unwrap();
        let activating = pos(
            &|e| matches!(e, Event::ElementActivating { element_instance_key, .. } if *element_instance_key == child),
        );
        let activated = pos(
            &|e| matches!(e, Event::ElementActivated { element_instance_key, .. } if *element_instance_key == child),
        );
        let mi_child = pos(
            &|e| matches!(e, Event::MultiInstanceChildActivated { child_key, .. } if *child_key == child),
        );
        assert_eq!(activated, activating + 1);
        assert_eq!(mi_child, activated + 1);
    }
}

/// #1724 item 3: a child's output mapping over the child-local `item` that
/// fails must raise an incident on the completing child even when the body
/// has no `outputElement` and no completion condition (nothing aggregates).
#[test]
fn mi_child_failing_output_mapping_raises_incident_without_aggregation() {
    let def = ProcessBuilder::new("mi-out")
        .start_event("start")
        .service_task("work", "w")
        .with_multi_instance("work", mi(false))
        .with_io(
            "work",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=assert(item, item > 5)".to_string(),
                    target: "checked".to_string(),
                }],
            },
        )
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let (mut engine, inst, _) = start(
        def,
        "mi-out",
        vars(&[("items", Value::List(vec![Value::Int(1)]))]),
    );
    complete_one(&mut engine, "w");
    let active = engine.active_incidents();
    assert_eq!(
        active.len(),
        1,
        "the failing output mapping raises: {active:?}"
    );
    assert_eq!(active[0].kind, state::IncidentKind::IoMapping);
    assert!(matches!(
        active[0].redrive,
        Some(state::IoMappingRedrive::MiChildCompletion { .. })
    ));
    assert!(!engine.is_completed(inst), "the child stays parked");
}

/// #1724 item 2: an early (completion-condition) body completion cancels the
/// still-running user-task children through their `canceling` task listeners,
/// and the body completes only once every canceling chain has drained.
#[test]
fn completion_condition_cancellation_runs_canceling_task_listeners() {
    let def = ProcessBuilder::new("mi-cancel")
        .start_event("start")
        .user_task("review")
        .with_multi_instance(
            "review",
            MultiInstance {
                completion_condition: Some("=true".to_string()),
                ..mi(false)
            },
        )
        .with_task_listeners(
            "review",
            vec![crate::model::TaskListener {
                event_type: crate::model::TaskListenerEventType::Canceling,
                job_type: "on-cancel".to_string(),
                retries: None,
            }],
        )
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "sink")
        .connect("sink", "end")
        .build()
        .unwrap();
    let (mut engine, inst, created) = start(
        def,
        "mi-cancel",
        vars(&[(
            "items",
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        )]),
    );
    let tasks: Vec<Key> = created
        .iter()
        .filter_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .collect();
    assert_eq!(tasks.len(), 3);
    let sink_created = |engine: &Engine| {
        engine
            .state()
            .jobs
            .values()
            .any(|j| j.job_type == "sink-work")
    };
    let canceled = |events: &[Event]| -> Vec<Key> {
        events
            .iter()
            .filter_map(|e| match e {
                Event::UserTaskCanceled { user_task_key, .. } => Some(*user_task_key),
                _ => None,
            })
            .collect()
    };
    let mut all = engine
        .apply_command(Command::complete_user_task(tasks[0]))
        .unwrap();
    assert!(
        canceled(&all).is_empty(),
        "remaining tasks are not cancelled before their canceling listeners ran"
    );
    assert!(
        !sink_created(&engine),
        "the body waits for the canceling chains"
    );
    let jobs = engine.activate_jobs("on-cancel", "W", 10, 1_000, 0);
    assert_eq!(
        jobs.len(),
        2,
        "one canceling listener job per remaining child"
    );

    let first = engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert_eq!(canceled(&first).len(), 1);
    assert!(!sink_created(&engine), "still waiting on the second chain");
    all.extend(first);

    let second = engine
        .apply_command(Command::complete_job(jobs[1].key))
        .unwrap();
    assert_eq!(canceled(&second).len(), 1);
    all.extend(second);
    assert!(
        sink_created(&engine),
        "the body completed after the last chain"
    );

    let mut got = canceled(&all);
    got.sort();
    assert_eq!(
        got,
        vec![tasks[1], tasks[2]],
        "each remaining task cancelled once"
    );
    let terminated = all
        .iter()
        .filter(
            |e| matches!(e, Event::ElementTerminated { element_id, .. } if element_id == "review"),
        )
        .count();
    assert_eq!(terminated, 2, "both remaining children are TERMINATED");
    assert!(engine.instance(inst).unwrap().multi_instances.is_empty());
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(inst));
}
