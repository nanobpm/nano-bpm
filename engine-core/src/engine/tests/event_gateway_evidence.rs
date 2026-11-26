//! Evidence for the gateway-owned event-based gateway runtime (#2004):
//! close triggers arbitrate to exactly one winner, a late trigger is never
//! silently consumed, the winning message's payload reaches the scope, and
//! snapshot / restore / replay agree with and without a pending trigger,
//! before and after the runtime latch.
use super::*;

/// A gateway racing a timer (due 5s after activation) and a message.
fn timer_and_message() -> ProcessDefinition {
    ProcessBuilder::new("tm")
        .start_event("start")
        .event_based_gateway("gw")
        .timer_intermediate_catch_event("onTimer", 5_000)
        .message_intermediate_catch_event("onMsg", "reply", "=key")
        .end_event("endTimer")
        .end_event("endMsg")
        .connect("start", "gw")
        .connect("gw", "onTimer")
        .connect("gw", "onMsg")
        .connect("onTimer", "endTimer")
        .connect("onMsg", "endMsg")
        .build()
        .unwrap()
}

/// A gateway racing two messages, both leading to the `hold` service task
/// (which keeps the instance, and its variables, alive); `end_listener` parks
/// the gateway on an `end` execution listener after its trigger is recorded.
fn two_messages(end_listener: bool) -> ProcessDefinition {
    let builder = ProcessBuilder::new("mm")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .service_task("hold", "hold")
        .end_event("end")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "hold")
        .connect("onM2", "hold")
        .connect("hold", "end");
    let builder = if end_listener {
        builder.with_listeners("gw", Vec::new(), vec![el(ListenerEventType::End, "audit")])
    } else {
        builder
    };
    builder.build().unwrap()
}

/// A publish that buffers for `ttl` when nothing is waiting for it.
fn publish(name: &str, key: &str, payload: i64, ttl: u64) -> Command {
    Command::CorrelateMessage {
        message_name: name.into(),
        correlation_key: key.into(),
        variables: vars(&[("payload", Value::Int(payload))]),
        business_id: None,
        message_key: None,
        time_to_live: Some(ttl),
        message_id: None,
    }
}

/// An engine with `def` deployed, recording every event it emits so the run
/// can be replayed.
struct Journaled {
    engine: Engine,
    journal: Vec<Event>,
}

impl Journaled {
    fn new(def: ProcessDefinition, latch: bool) -> Self {
        let mut engine = Engine::new();
        engine.set_event_gateway_runtime(latch);
        let journal = engine.apply_command(Command::DeployProcess(def)).unwrap();
        Self { engine, journal }
    }

    fn apply(&mut self, command: Command, now: u64) -> Vec<Event> {
        let events = self.engine.apply_command_at(command, now).unwrap();
        self.journal.extend(events.iter().cloned());
        events
    }

    fn timers(&mut self, now: u64) -> Vec<Event> {
        let events = self.engine.trigger_timers(now);
        self.journal.extend(events.iter().cloned());
        events
    }

    fn create(&mut self, process: &str, now: u64) -> Key {
        self.apply(
            Command::create_instance_with(process, vars(&[("key", Value::Str("k".into()))])),
            now,
        )
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap()
    }
}

fn triggers(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::EventGatewayTriggered {
                catch_element_id, ..
            } => Some(catch_element_id.clone()),
            _ => None,
        })
        .collect()
}

/// The catch elements (`on*`) activated in `events`.
fn catch_activations(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::ElementActivated { element_id, .. } if element_id.starts_with("on") => {
                Some(element_id.to_string())
            }
            _ => None,
        })
        .collect()
}

/// Whether the instance passed the gateway and waits on `hold`.
fn holding(engine: &Engine, inst: Key) -> bool {
    engine
        .instance(inst)
        .unwrap()
        .active
        .values()
        .any(|id| id == "hold")
}

fn correlations(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, Event::MessageCorrelated { .. }))
        .count()
}

/// The buffered messages no instance has consumed.
fn unconsumed(engine: &Engine) -> usize {
    engine
        .state()
        .buffered_messages
        .values()
        .filter(|m| m.correlated_instances.is_empty())
        .count()
}

/// A message and the timer fall due in the same batch (same instant, applied
/// back to back, in either order): exactly one trigger, one catch activation;
/// the message is never consumed by the timer-won gateway (it stays
/// buffered), and the timer never fires for the message-won one.
#[test]
fn message_and_timer_due_in_one_batch_trigger_once() {
    for message_first in [true, false] {
        let mut run = Journaled::new(timer_and_message(), true);
        let inst = run.create("tm", 1_000);
        let mut batch = Vec::new();
        if message_first {
            batch.extend(run.apply(publish("reply", "k", 1, 60_000), 6_000));
            batch.extend(run.timers(6_000));
        } else {
            batch.extend(run.timers(6_000));
            batch.extend(run.apply(publish("reply", "k", 1, 60_000), 6_000));
        }
        let winner = if message_first { "onMsg" } else { "onTimer" };
        assert_eq!(triggers(&batch), [winner], "message_first={message_first}");
        assert_eq!(catch_activations(&batch), [winner]);
        assert!(run.engine.is_completed(inst));
        if message_first {
            assert_eq!(correlations(&batch), 1);
            assert!(!batch
                .iter()
                .any(|e| matches!(e, Event::TimerTriggered { .. })));
        } else {
            assert_eq!(correlations(&batch), 0, "the late message is not consumed");
            assert_eq!(
                unconsumed(&run.engine),
                1,
                "the late message stays buffered"
            );
        }
    }
}

/// Two messages for the same gateway in one batch: the first in log order
/// wins; the second is buffered, never consumed by the completed gateway.
#[test]
fn two_messages_in_one_batch_trigger_once() {
    let mut run = Journaled::new(two_messages(false), true);
    let inst = run.create("mm", 1_000);
    let mut batch = run.apply(publish("M2", "k", 2, 60_000), 2_000);
    batch.extend(run.apply(publish("M1", "k", 1, 60_000), 2_000));
    assert_eq!(triggers(&batch), ["onM2"]);
    assert_eq!(catch_activations(&batch), ["onM2"]);
    assert_eq!(correlations(&batch), 1);
    assert!(holding(&run.engine, inst));
    assert_eq!(unconsumed(&run.engine), 1, "M1 stays buffered");
    assert_eq!(
        run.engine.instance(inst).unwrap().variables.get("payload"),
        Some(&Value::Int(2)),
        "only the winner's payload reached the scope"
    );
}

/// A message arriving after the gateway was triggered — whether the gateway
/// already completed, or is still parked on its `end` listener with the
/// trigger pending — is buffered (with a TTL) or dropped (TTL 0), never
/// silently consumed, and never activates a second catch.
#[test]
fn late_message_after_the_trigger_is_never_consumed() {
    for end_listener in [false, true] {
        let mut run = Journaled::new(two_messages(end_listener), true);
        let inst = run.create("mm", 1_000);
        let first = run.apply(publish("M1", "k", 1, 0), 2_000);
        assert_eq!(triggers(&first), ["onM1"]);
        assert_eq!(
            holding(&run.engine, inst),
            !end_listener,
            "end_listener={end_listener}"
        );
        for (name, ttl) in [("M2", 60_000), ("M1", 60_000), ("M2", 0)] {
            let late = run.apply(publish(name, "k", 9, ttl), 3_000);
            assert!(triggers(&late).is_empty(), "{late:?}");
            assert!(catch_activations(&late).is_empty(), "{late:?}");
            assert_eq!(correlations(&late), 0, "{late:?}");
            assert_eq!(
                late.iter()
                    .filter(|e| matches!(e, Event::MessageBuffered { .. }))
                    .count(),
                usize::from(ttl > 0),
                "buffered iff it has a TTL: {late:?}"
            );
        }
        assert_eq!(unconsumed(&run.engine), 2);
        if end_listener {
            let job = run.engine.activate_jobs("audit", "W", 10, 1_000, 3_000);
            assert_eq!(job.len(), 1);
            let done = run.apply(Command::complete_job(job[0].key), 4_000);
            assert_eq!(
                catch_activations(&done),
                ["onM1"],
                "only the recorded winner"
            );
            assert_eq!(correlations(&done), 0);
            assert!(holding(&run.engine, inst));
            assert_eq!(unconsumed(&run.engine), 2, "late messages still buffered");
        }
        assert_eq!(
            run.engine.instance(inst).unwrap().variables.get("payload"),
            Some(&Value::Int(1))
        );
    }
}

/// The winning message's variables and the catch's output mappings reach
/// the scope after the gateway completes — exactly as for the same catch
/// reached without a gateway.
#[test]
fn winning_message_payload_and_output_mappings_reach_the_scope() {
    let io = || crate::model::IoMapping {
        inputs: Vec::new(),
        outputs: vec![crate::model::Mapping {
            source: "=payload * 2".to_string(),
            target: "doubled".to_string(),
        }],
    };
    let gated = ProcessBuilder::new("p")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .with_io("onM1", io())
        .service_task("hold", "hold")
        .end_event("end")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "hold")
        .connect("onM2", "hold")
        .connect("hold", "end")
        .build()
        .unwrap();
    let plain = ProcessBuilder::new("p")
        .start_event("start")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .with_io("onM1", io())
        .service_task("hold", "hold")
        .end_event("end")
        .connect("start", "onM1")
        .connect("onM1", "hold")
        .connect("hold", "end")
        .build()
        .unwrap();
    let scope_after = |def: ProcessDefinition, publish_first: bool| {
        let mut run = Journaled::new(def, true);
        if publish_first {
            run.apply(publish("M1", "k", 21, 60_000), 500);
        }
        let inst = run.create("p", 1_000);
        if !publish_first {
            run.apply(publish("M1", "k", 21, 60_000), 2_000);
        }
        assert!(holding(&run.engine, inst));
        (*run.engine.instance(inst).unwrap().variables).clone()
    };
    for publish_first in [false, true] {
        let gated_scope = scope_after(gated.clone(), publish_first);
        assert_eq!(gated_scope.get("doubled"), Some(&Value::Int(42)));
        assert_eq!(gated_scope, scope_after(plain.clone(), publish_first));
    }
}

/// Snapshot -> `from_snapshot` and journal replay rebuild the identical
/// state, and both continue identically: before the latch (a legacy race),
/// and after it with a gateway waiting (no trigger) and with a trigger
/// pending on the gateway's `end` listener.
#[cfg(feature = "serde")]
#[test]
fn snapshot_restore_and_replay_agree_around_the_latch() {
    for (latch, end_listener, trigger) in [
        (false, false, false),
        (false, true, true),
        (true, false, false),
        (true, true, false),
        (true, true, true),
    ] {
        let label = format!("latch={latch} end_listener={end_listener} trigger={trigger}");
        let mut run = Journaled::new(two_messages(end_listener), latch);
        let inst = run.create("mm", 1_000);
        if trigger {
            run.apply(publish("M1", "k", 1, 0), 2_000);
        }
        let pending = run.engine.instance(inst).unwrap().gateway_triggers.len();
        assert_eq!(pending, usize::from(latch && trigger), "{label}");
        assert_eq!(run.engine.event_gateway_runtime_latched(), latch, "{label}");

        let json = serde_json::to_string(&run.engine.snapshot()).unwrap();
        let mut restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
        let mut replayed = Engine::replay(run.journal.clone());
        assert_eq!(restored.state(), run.engine.state(), "restore: {label}");
        assert_eq!(replayed.state(), run.engine.state(), "replay: {label}");
        assert_eq!(
            serde_json::to_string(&restored.snapshot()).unwrap(),
            json,
            "restore round-trips byte-identically: {label}"
        );

        // All three continue identically.
        let next: Vec<Command> = if trigger && end_listener {
            let job = run.engine.activate_jobs("audit", "W", 10, 1_000, 3_000);
            restored.activate_jobs("audit", "W", 10, 1_000, 3_000);
            replayed.activate_jobs("audit", "W", 10, 1_000, 3_000);
            vec![Command::complete_job(job[0].key)]
        } else {
            vec![publish("M2", "k", 2, 0)]
        };
        for command in next {
            let a = run.apply(command.clone(), 4_000);
            let b = restored.apply_command_at(command.clone(), 4_000).unwrap();
            let c = replayed.apply_command_at(command, 4_000).unwrap();
            assert_eq!(a, b, "restored continues identically: {label}");
            assert_eq!(a, c, "replayed continues identically: {label}");
        }
        assert_eq!(restored.state(), run.engine.state(), "{label}");
        assert_eq!(replayed.state(), run.engine.state(), "{label}");
    }
}

/// A latched gateway whose two catches are `a` and `b`, built by `catch`,
/// both leading to `hold`.
fn two_catches(catch: fn(ProcessBuilder, &str) -> ProcessBuilder) -> ProcessDefinition {
    let builder = ProcessBuilder::new("cc")
        .start_event("start")
        .event_based_gateway("gw");
    let builder = catch(catch(builder, "onA"), "onB");
    builder
        .service_task("hold", "hold")
        .end_event("end")
        .connect("start", "gw")
        .connect("gw", "onA")
        .connect("gw", "onB")
        .connect("onA", "hold")
        .connect("onB", "hold")
        .connect("hold", "end")
        .build()
        .unwrap()
}

/// Asserts one trigger, one catch activation, and no open subscription left
/// on the gateway, and returns the winner.
fn assert_single_winner(run: &Journaled, inst: Key, events: &[Event]) -> String {
    let won = triggers(events);
    assert_eq!(won.len(), 1, "exactly one trigger: {events:#?}");
    assert_eq!(catch_activations(events), won, "only the winner activates");
    assert!(
        !run.engine
            .state()
            .signal_subscriptions
            .values()
            .any(|s| s.instance_key == inst
                && s.state == crate::state::MessageSubscriptionState::Open),
        "the loser's signal subscription is cancelled"
    );
    won[0].clone()
}

/// One signal broadcast matching BOTH catches of one gateway (#2047 review)
/// records exactly one `SignalCorrelated`; the loser's subscription is
/// cancelled, never recorded as a successful correlation.
#[test]
fn one_broadcast_matching_two_gateway_catches_correlates_once() {
    let mut run = Journaled::new(
        two_catches(|b, id| b.signal_intermediate_catch_event(id, "go")),
        true,
    );
    let inst = run.create("cc", 1_000);
    let events = run.apply(
        Command::BroadcastSignal {
            signal_name: "go".into(),
            variables: BTreeMap::new(),
        },
        2_000,
    );
    let correlated = events
        .iter()
        .filter(|e| matches!(e, Event::SignalCorrelated { .. }))
        .count();
    assert_eq!(correlated, 1, "one winning correlation: {events:#?}");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::SignalSubscriptionCanceled { .. })));
    assert_eq!(assert_single_winner(&run, inst, &events), "onA");
    assert!(holding(&run.engine, inst));
    assert_eq!(
        Engine::replay(run.journal.clone()).state(),
        run.engine.state()
    );
}

/// Two gateway catch conditions becoming true together (#2047 review)
/// record exactly one `ConditionalTriggered`; the loser is cancelled.
#[test]
fn two_gateway_conditions_true_together_trigger_once() {
    let mut run = Journaled::new(
        two_catches(|b, id| b.conditional_intermediate_catch_event(id, "=x > 0")),
        true,
    );
    let inst = run.create("cc", 1_000);
    let events = run.apply(
        Command::set_variables(inst, BTreeMap::from([("x".to_string(), Value::Int(1))])),
        2_000,
    );
    let fired = events
        .iter()
        .filter(|e| matches!(e, Event::ConditionalTriggered { .. }))
        .count();
    assert_eq!(fired, 1, "one winning trigger: {events:#?}");
    let won = triggers(&events);
    assert_eq!(won, vec!["onA".to_string()]);
    assert_eq!(catch_activations(&events), won);
    assert!(
        !run.engine
            .state()
            .conditional_subscriptions
            .values()
            .any(|s| s.instance_key == inst
                && s.state == crate::state::MessageSubscriptionState::Open)
    );
    assert!(holding(&run.engine, inst));
    assert_eq!(
        Engine::replay(run.journal.clone()).state(),
        run.engine.state()
    );
}

/// The winning payload is snapshotted at the trigger (#2047 review): a
/// variable update while the gateway waits on its `end` listener does not
/// change what the catch's output mapping reads — live, after snapshot
/// restore, and after journal replay.
#[cfg(feature = "serde")]
#[test]
fn winning_payload_is_snapshotted_across_an_intervening_update() {
    let def = ProcessBuilder::new("mm")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "=key")
        .message_intermediate_catch_event("onM2", "M2", "=key")
        .with_io(
            "onM1",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=payload * 2".to_string(),
                    target: "doubled".to_string(),
                }],
            },
        )
        .service_task("hold", "hold")
        .end_event("end")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "hold")
        .connect("onM2", "hold")
        .connect("hold", "end")
        .with_listeners("gw", Vec::new(), vec![el(ListenerEventType::End, "audit")])
        .build()
        .unwrap();
    let mut run = Journaled::new(def, true);
    let inst = run.create("mm", 1_000);
    run.apply(publish("M1", "k", 21, 0), 2_000);
    assert_eq!(run.engine.instance(inst).unwrap().gateway_triggers.len(), 1);
    run.apply(
        Command::set_variables(
            inst,
            BTreeMap::from([("payload".to_string(), Value::Int(99))]),
        ),
        2_500,
    );
    let json = serde_json::to_string(&run.engine.snapshot()).unwrap();
    let mut restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
    let mut replayed = Engine::replay(run.journal.clone());
    let job = run.engine.activate_jobs("audit", "W", 10, 1_000, 3_000);
    restored.activate_jobs("audit", "W", 10, 1_000, 3_000);
    replayed.activate_jobs("audit", "W", 10, 1_000, 3_000);
    let done = Command::complete_job(job[0].key);
    run.apply(done.clone(), 4_000);
    restored.apply_command_at(done.clone(), 4_000).unwrap();
    replayed.apply_command_at(done, 4_000).unwrap();
    for (label, engine) in [
        ("live", &run.engine),
        ("restored", &restored),
        ("replayed", &replayed),
    ] {
        assert!(holding(engine, inst), "{label}");
        assert_eq!(
            engine.instance(inst).unwrap().variables.get("doubled"),
            Some(&Value::Int(42)),
            "{label}: the output reads the payload recorded at the trigger"
        );
    }
}
