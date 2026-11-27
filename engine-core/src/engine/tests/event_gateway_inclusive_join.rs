//! #1990 (fuzz #1789/#1790/#1895/#1896): an inclusive join downstream of an
//! event-based gateway must fire once every path that can still reach it has
//! been taken. These tests refute the suspected engine defect (a phantom live
//! source left by the triggered gateway): the engine fires the join. The fuzz
//! divergence was a scenario-generator defect: the join's route variable was
//! never delivered before the tick (`formal/corpus/enumerate.mjs`), so the
//! join's outgoing conditions read an unset variable and raised an incident. Zeebe's `BpmnInclusiveGatewayBehavior.hasActivePathToTheGateway`
//! searches only from the flow scope's live element instances; a triggered
//! event-based gateway is completed (`EventBasedGatewayProcessor`), so neither
//! it nor its untriggered catches is a live source.
use super::*;

/// #1895: `and,evt,or|0>2d,1>3b,1>3d,2>1m,2>4m,3>4d,3>4d`. `s -> gw(evt)`;
/// `gw -> m1 -> fork(and)`, `gw -> m2 -> end2`; `fork -> t1 -> join`
/// (interrupting timer `b1 -> join`), `fork -> join`; `join(or) -> end`.
fn reproducer_1895() -> ProcessDefinition {
    ProcessBuilder::new("r1895")
        .start_event("s")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("m1", "M1", "=key")
        .message_intermediate_catch_event("m2", "M2", "=key")
        .parallel_gateway("fork")
        .service_task("t1", "jt1")
        .timer_boundary_event("b1", "t1", 60_000)
        .inclusive_gateway("join")
        .end_event("end")
        .end_event("end2")
        .connect("s", "gw")
        .connect("gw", "m1")
        .connect("gw", "m2")
        .connect("m1", "fork")
        .connect("m2", "end2")
        .connect("fork", "t1")
        .connect("fork", "join")
        .connect("t1", "join")
        .connect("b1", "join")
        .connect("join", "end")
        .build()
        .unwrap()
}

fn start(engine: &mut Engine) -> Key {
    engine
        .apply_command(Command::DeployProcess(reproducer_1895()))
        .unwrap();
    let created = engine
        .apply_command_at(
            Command::create_instance_with("r1895", vars(&[("key", Value::Str("k".into()))])),
            0,
        )
        .unwrap();
    created.iter().find_map(|e| e.instance_key()).unwrap()
}

fn active_ids(engine: &Engine, instance_key: Key) -> Vec<String> {
    engine.state.instances[&instance_key]
        .active
        .values()
        .cloned()
        .collect()
}

#[test]
fn inclusive_join_after_event_gateway_fires_once_the_boundary_interrupts() {
    let mut engine = Engine::new();
    let instance_key = start(&mut engine);
    engine
        .apply_command_at(Command::correlate_message("M1", "k"), 0)
        .unwrap();
    let fired = engine.trigger_timers(60_000);
    assert!(
        fired
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == "join")),
        "the join must fire once `b1` interrupts `t1`: {fired:#?}"
    );
    assert!(
        engine.is_completed(instance_key),
        "active: {:?}",
        active_ids(&engine, instance_key)
    );
}

/// Guard for the defect class: once the gateway triggers, neither it nor its
/// untriggered catch stays live (a phantom source for the active-path search).
#[test]
fn triggered_event_gateway_leaves_no_live_source_behind() {
    let mut engine = Engine::new();
    let instance_key = start(&mut engine);
    engine
        .apply_command_at(Command::correlate_message("M1", "k"), 0)
        .unwrap();
    let active = active_ids(&engine, instance_key);
    for stale in ["gw", "m1", "m2"] {
        assert!(
            !active.iter().any(|id| id == stale),
            "`{stale}` is still live after the trigger: {active:?}"
        );
    }
}
