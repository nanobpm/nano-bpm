//! Root-level conditional start events (#932): a conditional evaluation
//! creates an instance for every deployed definition whose conditional start
//! condition holds for the supplied variables.
use super::*;

fn conditional_process(id: &str, condition: &str) -> ProcessDefinition {
    ProcessBuilder::new(id)
        .conditional_start_event("cs", condition)
        .end_event("end")
        .connect("cs", "end")
        .build()
        .unwrap()
}

fn evaluate(
    engine: &mut Engine,
    variables: BTreeMap<String, Value>,
    process_definition_key: Option<Key>,
) -> Result<Vec<Event>, EngineError> {
    engine.apply_command(Command::EvaluateConditionalStartEvents {
        variables,
        process_definition_key,
    })
}

/// `(process_id, instance_key, process_definition_key)` of every instance the
/// evaluation created, in log order.
fn created(events: &[Event]) -> Vec<(String, Key, Key)> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                process_definition_key,
                ..
            } => Some((process_id.clone(), *instance_key, *process_definition_key)),
            _ => None,
        })
        .collect()
}

#[test]
fn conditional_start_creates_an_instance_when_the_condition_holds() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(conditional_process(
            "big-order",
            "=orderAmount > 100",
        )))
        .unwrap();

    // A false condition creates nothing.
    let none = evaluate(&mut engine, vars(&[("orderAmount", Value::Int(10))]), None).unwrap();
    assert!(created(&none).is_empty());

    // A true condition creates one instance, seeded with the variables, that
    // runs from the conditional start along its outgoing flow to completion.
    let events = evaluate(
        &mut engine,
        vars(&[("orderAmount", Value::Int(1000))]),
        None,
    )
    .unwrap();
    let instances = created(&events);
    assert_eq!(instances.len(), 1);
    let (process_id, key, _) = &instances[0];
    assert_eq!(process_id, "big-order");
    assert!(engine.is_completed(*key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceCreated { variables, .. }
            if variables.get("orderAmount") == Some(&Value::Int(1000))
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ElementActivated { element_id, .. } if element_id == "cs"
    )));
}

#[test]
fn conditional_start_errors_and_non_booleans_are_not_satisfied() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(conditional_process(
            "p",
            "=orderAmount > 100",
        )))
        .unwrap();
    // The referenced variable is absent: the condition does not hold and no
    // incident or instance results.
    let events = evaluate(&mut engine, BTreeMap::new(), None).unwrap();
    assert!(created(&events).is_empty());
    assert!(engine.incidents().is_empty());
}

#[test]
fn conditional_evaluation_spans_every_latest_definition_deterministically() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployResources(vec![
            conditional_process("b-proc", "=x > 1"),
            conditional_process("a-proc", "=x > 1"),
            conditional_process("c-proc", "=x > 100"),
            // A none-start-only process is never triggered.
            ProcessBuilder::new("plain")
                .start_event("s")
                .end_event("e")
                .connect("s", "e")
                .build()
                .unwrap(),
        ]))
        .unwrap();
    let events = evaluate(&mut engine, vars(&[("x", Value::Int(5))]), None).unwrap();
    let ids: Vec<String> = created(&events).into_iter().map(|c| c.0).collect();
    assert_eq!(ids, vec!["a-proc".to_string(), "b-proc".to_string()]);

    // Only the LATEST version is evaluated: v2 of a-proc changes its condition.
    engine
        .apply_command(Command::DeployProcess(conditional_process(
            "a-proc",
            "=x > 1000",
        )))
        .unwrap();
    let events = evaluate(&mut engine, vars(&[("x", Value::Int(5))]), None).unwrap();
    let ids: Vec<String> = created(&events).into_iter().map(|c| c.0).collect();
    assert_eq!(ids, vec!["b-proc".to_string()]);
}

#[test]
fn conditional_evaluation_scoped_to_a_definition_key() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployResources(vec![
            conditional_process("a", "=go"),
            conditional_process("b", "=go"),
        ]))
        .unwrap();
    let a_v1 = engine.state().processes["a"].key;
    // A newer version of `a` exists, but the key pins the older one.
    engine
        .apply_command(Command::DeployProcess(conditional_process(
            "a",
            "=go and false",
        )))
        .unwrap();

    let events = evaluate(&mut engine, vars(&[("go", Value::Bool(true))]), Some(a_v1)).unwrap();
    let instances = created(&events);
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].0, "a");
    assert_eq!(instances[0].2, a_v1, "the instance runs the pinned version");

    // An unknown key is rejected, with nothing emitted.
    let err = evaluate(&mut engine, BTreeMap::new(), Some(987_654_321)).unwrap_err();
    assert!(matches!(err, EngineError::ProcessNotFound { .. }));

    // Key 0 is an explicit (never-minted) key, not "unscoped": it must fail
    // the lookup rather than broaden the evaluation to every definition.
    let err = evaluate(&mut engine, vars(&[("go", Value::Bool(true))]), Some(0)).unwrap_err();
    assert!(matches!(err, EngineError::ProcessNotFound { .. }));
}

#[test]
fn conditional_start_process_still_creates_instances_by_create_instance() {
    // A conditional-start-only process designates its conditional start as the
    // process entry, like a message-start-only process.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(conditional_process("p", "=false")))
        .unwrap();
    let events = engine.apply_command(Command::create_instance("p")).unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(key));
}

#[test]
fn conditional_evaluation_replays_to_the_same_state() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(conditional_process("p", "=x = 1")))
            .unwrap(),
    );
    log.extend(evaluate(&mut engine, vars(&[("x", Value::Int(1))]), None).unwrap());
    let key = created(&log)[0].1;
    let replayed = Engine::replay(log);
    assert!(replayed.is_completed(key));
    assert_eq!(
        replayed.state().processes["p"].key,
        engine.state().processes["p"].key
    );
    assert_eq!(replayed.variables(key), engine.variables(key));
}
