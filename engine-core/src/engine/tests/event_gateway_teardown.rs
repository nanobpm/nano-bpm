//! Event-based gateway parity slice iii (#2005, design #1963): cancellation,
//! process-instance modification and teardown on the gateway-owned model.
//! Terminating the gateway — directly, through its scope, or with the whole
//! instance — unsubscribes everything it owns (message, timer, signal and
//! conditional), and a gateway activated by `ModifyInstance` opens its owned
//! subscriptions exactly as one reached by token flow. Migration of a waiting
//! gateway (incl. the #1554 race checks) lives in `migration_event_gateway.rs`.
use super::*;

/// `start -> [hold] -> gw` with one catch of every kind behind the gateway.
/// `hold` (a user task in a parallel branch) keeps the instance alive while
/// the gateway is terminated.
fn four_kind_race(id: &str) -> ProcessBuilder {
    ProcessBuilder::new(id)
        .start_event("start")
        .parallel_gateway("fork")
        .user_task("hold")
        .end_event("endHold")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onMsg", "M1", "=key")
        .timer_intermediate_catch_event("onTimer", 5_000)
        .signal_intermediate_catch_event("onSig", "go")
        .conditional_intermediate_catch_event("onCond", "=flag")
        .end_event("endMsg")
        .end_event("endTimer")
        .end_event("endSig")
        .end_event("endCond")
        .connect("start", "fork")
        .connect("fork", "hold")
        .connect("hold", "endHold")
        .connect("fork", "gw")
        .connect("gw", "onMsg")
        .connect("gw", "onTimer")
        .connect("gw", "onSig")
        .connect("gw", "onCond")
        .connect("onMsg", "endMsg")
        .connect("onTimer", "endTimer")
        .connect("onSig", "endSig")
        .connect("onCond", "endCond")
}

fn start(def: ProcessDefinition) -> (Engine, Key) {
    let mut engine = Engine::new();
    let id = def.id.clone();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = create_instance_with_vars(
        &mut engine,
        &id,
        vars(&[
            ("key", Value::Str("k".into())),
            ("flag", Value::Bool(false)),
        ]),
    );
    (engine, inst)
}

fn eik_of(engine: &Engine, inst: Key, id: &str) -> Option<Key> {
    engine
        .state()
        .instances
        .get(&inst)?
        .active
        .iter()
        .find_map(|(k, e)| (e == id).then_some(*k))
}

/// Every live subscription (of any kind) resting on `eik`.
fn owned(engine: &Engine, eik: Key) -> usize {
    use crate::state::{MessageSubscriptionState as Sub, TimerState};
    let s = engine.state();
    let live = |st: &Sub| matches!(st, Sub::Open | Sub::Opening);
    s.message_subscriptions
        .values()
        .filter(|x| x.element_instance_key == eik && live(&x.state))
        .count()
        + s.timers
            .values()
            .filter(|x| x.element_instance_key == eik && x.state == TimerState::Created)
            .count()
        + s.signal_subscriptions
            .values()
            .filter(|x| x.element_instance_key == eik && live(&x.state))
            .count()
        + s.conditional_subscriptions
            .values()
            .filter(|x| x.element_instance_key == eik && live(&x.state))
            .count()
}

/// Fires every kind of event the gateway subscribed to; none may activate
/// anything once the gateway is gone.
fn assert_nothing_fires(engine: &mut Engine, inst: Key) {
    let mut events = engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap_or_default();
    events.extend(engine.trigger_timers(60_000));
    events.extend(engine.broadcast_signal("go", BTreeMap::new(), 0));
    if let Some(hold) = eik_of(engine, inst, "hold") {
        // Make the condition true while the instance is still alive.
        events.extend(
            engine
                .apply_command(Command::SetVariables {
                    scope_key: inst,
                    variables: vars(&[("flag", Value::Bool(true))]),
                    local: false,
                })
                .unwrap_or_default(),
        );
        assert!(eik_of(engine, inst, "hold") == Some(hold));
    }
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementActivated { element_id, .. }
                if ["gw", "onMsg", "onTimer", "onSig", "onCond"].contains(&element_id.as_str())
        ) || matches!(e, Event::EventGatewayTriggered { .. })),
        "a torn-down gateway's subscriptions must never fire: {events:?}"
    );
}

fn assert_gateway_torn_down(engine: &mut Engine, inst: Key, gw: Key) {
    assert_eq!(owned(engine, gw), 0, "every owned subscription deleted");
    if let Some(i) = engine.state().instances.get(&inst) {
        assert!(!i.gateway_subscriptions.contains_key(&gw));
        assert!(!i.gateway_triggers.contains_key(&gw));
        assert!(!i.active.contains_key(&gw));
    }
    assert_nothing_fires(engine, inst);
}

#[test]
fn waiting_gateway_owns_one_subscription_per_catch() {
    let (engine, inst) = start(four_kind_race("evt").build().unwrap());
    let gw = eik_of(&engine, inst, "gw").expect("gateway waits");
    assert_eq!(owned(&engine, gw), 4);
    for catch in ["onMsg", "onTimer", "onSig", "onCond"] {
        assert!(eik_of(&engine, inst, catch).is_none(), "{catch} not armed");
    }
}

#[test]
fn modify_terminating_the_gateway_unsubscribes_everything_it_owns() {
    let (mut engine, inst) = start(four_kind_race("evt").build().unwrap());
    let gw = eik_of(&engine, inst, "gw").unwrap();
    let events = engine
        .apply_command(Command::modify_instance(inst, vec![], vec![gw]))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ElementTerminated { element_instance_key, .. } if *element_instance_key == gw
    )));
    assert_gateway_torn_down(&mut engine, inst, gw);
}

#[test]
fn modify_terminating_the_gateway_by_element_id_unsubscribes_everything() {
    let (mut engine, inst) = start(four_kind_race("evt").build().unwrap());
    let gw = eik_of(&engine, inst, "gw").unwrap();
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec!["gw".into()],
            move_instructions: vec![],
        })
        .unwrap();
    assert_gateway_torn_down(&mut engine, inst, gw);
}

#[test]
fn modify_moving_off_the_gateway_unsubscribes_it_and_arms_the_target() {
    let (mut engine, inst) = start(four_kind_race("evt").build().unwrap());
    let gw = eik_of(&engine, inst, "gw").unwrap();
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: inst,
            activate_instructions: vec![],
            terminate_instructions: vec![],
            terminate_element_ids: vec![],
            move_instructions: vec![crate::event::MoveInstruction {
                source: crate::event::MoveSource::ById("gw".into()),
                target_element_id: "onMsg".into(),
                ..Default::default()
            }],
        })
        .unwrap();
    assert_eq!(owned(&engine, gw), 0);
    assert!(!engine.state().instances[&inst]
        .gateway_subscriptions
        .contains_key(&gw));
    // The moved-to catch subscribes on its own element instance.
    let catch = eik_of(&engine, inst, "onMsg").expect("catch activated");
    assert_eq!(owned(&engine, catch), 1);
    engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    assert!(eik_of(&engine, inst, "onMsg").is_none());
    // Only the gateway's other subscriptions could have fired; none remain.
    let events = engine.trigger_timers(60_000);
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { .. })));
}

#[test]
fn modify_activating_a_gateway_opens_its_owned_subscriptions() {
    let (mut engine, inst) = start(four_kind_race("evt").build().unwrap());
    let old = eik_of(&engine, inst, "gw").unwrap();
    engine
        .apply_command(Command::modify_instance(
            inst,
            vec![crate::command::ActivateElementInstruction {
                element_id: "gw".into(),
                ..Default::default()
            }],
            vec![old],
        ))
        .unwrap();
    assert_eq!(owned(&engine, old), 0);
    let gw = eik_of(&engine, inst, "gw").expect("re-activated gateway waits");
    assert_ne!(gw, old);
    assert_eq!(owned(&engine, gw), 4);
    assert_eq!(
        engine.state().instances[&inst].gateway_subscriptions[&gw],
        vec!["onMsg", "onTimer", "onSig", "onCond"]
    );
    // It is a fully live gateway: the timer triggers it, the rest are deleted.
    let events = engine.trigger_timers(6_000);
    assert!(events.iter().any(|e| matches!(
        e,
        Event::EventGatewayTriggered { gateway_element_instance_key, catch_element_id, .. }
            if *gateway_element_instance_key == gw && catch_element_id == "onTimer"
    )));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::ElementCompleted { element_id, .. } if element_id == "endTimer"
    )));
    assert_eq!(owned(&engine, gw), 0);
    assert_nothing_fires(&mut engine, inst);
}

#[test]
fn cancelling_the_instance_unsubscribes_the_waiting_gateway() {
    let (mut engine, inst) = start(four_kind_race("evt").build().unwrap());
    let gw = eik_of(&engine, inst, "gw").unwrap();
    engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    assert_gateway_torn_down(&mut engine, inst, gw);
}

#[test]
fn terminate_end_event_unsubscribes_the_waiting_gateway() {
    // A parallel branch's terminate end kills the waiting gateway.
    let def = ProcessBuilder::new("evtKill")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("work", "work")
        .terminate_end_event("kill")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onMsg", "M1", "=key")
        .timer_intermediate_catch_event("onTimer", 5_000)
        .signal_intermediate_catch_event("onSig", "go")
        .conditional_intermediate_catch_event("onCond", "=flag")
        .end_event("end")
        .connect("start", "fork")
        .connect("fork", "work")
        .connect("work", "kill")
        .connect("fork", "gw")
        .connect("gw", "onMsg")
        .connect("gw", "onTimer")
        .connect("gw", "onSig")
        .connect("gw", "onCond")
        .connect("onMsg", "end")
        .connect("onTimer", "end")
        .connect("onSig", "end")
        .connect("onCond", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = start(def);
    let gw = eik_of(&engine, inst, "gw").unwrap();
    assert_eq!(owned(&engine, gw), 4);
    complete_one(&mut engine, "work");
    assert!(eik_of(&engine, inst, "gw").is_none());
    assert_gateway_torn_down(&mut engine, inst, gw);
}

/// The gateway's enclosing sub-process is interrupted by a boundary event:
/// the scope teardown unsubscribes everything the gateway owns.
#[test]
fn interrupting_the_gateway_scope_unsubscribes_the_gateway() {
    let def = ProcessBuilder::new("scoped")
        .start_event("start")
        .sub_process("sub", "subStart")
        .start_event("subStart")
        .contained_in("subStart", "sub")
        .event_based_gateway("gw")
        .contained_in("gw", "sub")
        .message_intermediate_catch_event("onMsg", "M1", "=key")
        .contained_in("onMsg", "sub")
        .timer_intermediate_catch_event("onTimer", 5_000)
        .contained_in("onTimer", "sub")
        .signal_intermediate_catch_event("onSig", "go")
        .contained_in("onSig", "sub")
        .conditional_intermediate_catch_event("onCond", "=flag")
        .contained_in("onCond", "sub")
        .end_event("subEnd")
        .contained_in("subEnd", "sub")
        .message_boundary_event("cancelSub", "sub", "STOP", "=key")
        .user_task("hold")
        .end_event("end")
        .connect("start", "sub")
        .connect("subStart", "gw")
        .connect("gw", "onMsg")
        .connect("gw", "onTimer")
        .connect("gw", "onSig")
        .connect("gw", "onCond")
        .connect("onMsg", "subEnd")
        .connect("onTimer", "subEnd")
        .connect("onSig", "subEnd")
        .connect("onCond", "subEnd")
        .connect("sub", "end")
        .connect("cancelSub", "hold")
        .connect("hold", "end")
        .build()
        .unwrap();
    let (mut engine, inst) = start(def);
    let gw = eik_of(&engine, inst, "gw").expect("gateway waits in the sub-process");
    assert_eq!(owned(&engine, gw), 4);
    engine
        .apply_command(Command::correlate_message("STOP", "k"))
        .unwrap();
    assert!(eik_of(&engine, inst, "sub").is_none(), "scope interrupted");
    assert!(eik_of(&engine, inst, "hold").is_some());
    assert_gateway_torn_down(&mut engine, inst, gw);
}
