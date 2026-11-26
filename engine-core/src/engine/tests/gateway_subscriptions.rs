//! Gateway-owned event-based gateway subscription state (#1963 slice i,
//! #2003): the additive `ProcessInstance::gateway_subscriptions` projection
//! and the legacy-race detector the v4 snapshot loader uses to refuse
//! in-flight #1676 races.
use super::*;

fn gateway_race() -> ProcessDefinition {
    ProcessBuilder::new("evt")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .end_event("end")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end")
        .connect("onM2", "end")
        .build()
        .unwrap()
}

#[test]
fn gateway_subscriptions_open_then_clear_on_trigger_and_on_gateway_end() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(gateway_race()))
        .unwrap();
    let inst =
        create_instance_with_vars(&mut engine, "evt", vars(&[("key", Value::Str("k".into()))]));
    let mut state = engine.state().clone();
    let opened = |gw: Key| Event::EventGatewaySubscriptionsOpened {
        instance_key: inst,
        gateway_element_instance_key: gw,
        gateway_element_id: "gw".to_string(),
        catch_element_ids: vec!["onM1".to_string(), "onM2".to_string()],
    };

    crate::state::apply(&mut state, &opened(9001));
    assert_eq!(
        state.instances[&inst].gateway_subscriptions.get(&9001),
        Some(&vec!["onM1".to_string(), "onM2".to_string()]),
        "subscriptions are owned by the gateway EIK, tagged with catch ids"
    );

    // First trigger wins: every subscription the gateway owns is deleted.
    crate::state::apply(
        &mut state,
        &Event::EventGatewayTriggered {
            instance_key: inst,
            gateway_element_instance_key: 9001,
            catch_element_id: "onM1".to_string(),
        },
    );
    assert!(state.instances[&inst].gateway_subscriptions.is_empty());

    // Terminating the gateway unsubscribes everything it owns.
    crate::state::apply(&mut state, &opened(9002));
    crate::state::apply(
        &mut state,
        &Event::ElementTerminated {
            instance_key: inst,
            element_instance_key: 9002,
            element_id: "gw".to_string(),
        },
    );
    assert!(state.instances[&inst].gateway_subscriptions.is_empty());
}

#[test]
fn legacy_race_detector_names_instances_with_armed_races_only() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(gateway_race()))
        .unwrap();
    // The legacy runtime (still active in this slice) arms a #1676 race.
    let inst =
        create_instance_with_vars(&mut engine, "evt", vars(&[("key", Value::Str("k".into()))]));
    assert_eq!(
        engine.state().legacy_event_gateway_race_instances(),
        vec![inst]
    );

    // Once the race settles, nothing is in flight.
    engine
        .apply_command(Command::correlate_message("M1", "k"))
        .unwrap();
    assert!(engine
        .state()
        .legacy_event_gateway_race_instances()
        .is_empty());
}

/// Whole-instance teardown (a top-level completion — e.g. a terminate end — or
/// a cancellation) bypasses per-gateway terminal events, so it must clear the
/// gateway-owned subscriptions itself or they strand on the terminal shell.
#[test]
fn gateway_subscriptions_clear_on_whole_instance_teardown() {
    for terminal in [
        (|k| Event::ProcessInstanceCompleted { instance_key: k }) as fn(Key) -> Event,
        |k| Event::ProcessInstanceTerminated { instance_key: k },
    ] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(gateway_race()))
            .unwrap();
        let inst =
            create_instance_with_vars(&mut engine, "evt", vars(&[("key", Value::Str("k".into()))]));
        let mut state = engine.state().clone();
        crate::state::apply(
            &mut state,
            &Event::EventGatewaySubscriptionsOpened {
                instance_key: inst,
                gateway_element_instance_key: 9001,
                gateway_element_id: "gw".to_string(),
                catch_element_ids: vec!["onM1".to_string()],
            },
        );
        assert!(!state.instances[&inst].gateway_subscriptions.is_empty());
        crate::state::apply(&mut state, &terminal(inst));
        assert!(
            state.instances[&inst].gateway_subscriptions.is_empty(),
            "teardown {:?} must clear gateway subscriptions",
            terminal(inst)
        );
    }
}
