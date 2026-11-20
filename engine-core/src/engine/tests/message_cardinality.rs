//! Message cardinality (#1676, Zeebe `MessageCorrelator`): one published
//! message correlates to at most ONE subscription per process instance. Two
//! tokens resting on separate armed copies of the same catch (here: two
//! activations of one event-based gateway, each arming `M1` and `M2` under the
//! same correlation key) must be resumed one per message, not both by the
//! first message — and the winner of one race withdraws only ITS OWN losing
//! sibling, never the other race's.
use super::*;

/// start -> fork(and) => merge(xor) -> gw(event-based) -> { onM1 | onM2 } -> end
///
/// Both fork branches pass the merge, so `gw` is activated twice in the root
/// scope: two independent races, each arming its own `onM1` and `onM2`
/// (the shape of the fuzz reproducer `Gen_8208a6f661`).
fn twin_gateways() -> ProcessDefinition {
    ProcessBuilder::new("twins")
        .start_event("start")
        .parallel_gateway("fork")
        .exclusive_gateway("merge")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end")
        .connect("start", "fork")
        .connect("fork", "merge")
        .connect("fork", "merge")
        .connect("merge", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end")
        .connect("onM2", "end")
        .build()
        .unwrap()
}

fn completions(events: &[Event], id: &str) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, Event::ElementCompleted { element_id, .. } if element_id.as_str() == id))
        .count()
}

#[test]
fn one_message_correlates_once_per_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(twin_gateways()))
        .unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        "twins",
        vars(&[("key", Value::Str("k".into()))]),
    );

    let first = engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    // One onM1 wins (resumed by M1) and only its race's onM2 is withdrawn.
    assert_eq!(
        completions(&first, "onM1"),
        1,
        "M1 must resume exactly one armed gateway"
    );
    assert_eq!(
        completions(&first, "onM2"),
        1,
        "only the winning race's onM2 is withdrawn"
    );

    let second = engine
        .apply_command(Command::correlate_message("M2", "k"))
        .unwrap();
    assert_eq!(
        completions(&second, "onM2"),
        1,
        "M2 must resume the other armed gateway"
    );
    assert_eq!(
        completions(&second, "onM1"),
        1,
        "the second race's onM1 loses to M2"
    );
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn one_message_still_correlates_to_every_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(twin_gateways()))
        .unwrap();
    let a = create_instance_with_vars(
        &mut engine,
        "twins",
        vars(&[("key", Value::Str("k".into()))]),
    );
    let b = create_instance_with_vars(
        &mut engine,
        "twins",
        vars(&[("key", Value::Str("k".into()))]),
    );
    let events = engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    for inst in [a, b] {
        let n = events
            .iter()
            .filter(|e| matches!(e, Event::ElementCompleted { instance_key, element_id, .. } if *instance_key == inst && element_id.as_str() == "onM1"))
            .count();
        assert_eq!(n, 1, "instance {inst} must correlate M1 once");
    }
}
