//! Message buffering with a time-to-live (#1874, Zeebe parity): a publish that
//! finds no open subscription stays buffered until `now + ttl`; a subscription
//! opened before the deadline correlates it; TTL 0 never buffers; an expired
//! message is never correlated; a `messageId` still buffered for the same name
//! and correlation key rejects a duplicate publish.
use super::*;

fn publish(name: &str, key: &str, ttl: Option<u64>, id: Option<&str>) -> Command {
    Command::CorrelateMessage {
        message_name: name.into(),
        correlation_key: key.into(),
        variables: vars(&[("paid", Value::Bool(true))]),
        business_id: None,
        message_key: None,
        time_to_live: ttl,
        message_id: id.map(str::to_string),
    }
}

fn deployed() -> Engine {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    engine
}

fn create(engine: &mut Engine, order: &str, now: u64) -> (Key, Vec<Event>) {
    let events = engine
        .apply_command_at(
            Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str(order.into()))]),
            ),
            now,
        )
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    (key, events)
}

#[test]
fn publish_then_create_correlates_within_ttl() {
    let mut engine = deployed();
    let published = engine
        .apply_command_at(
            publish("payment-received", "o-1", Some(10_000), None),
            1_000,
        )
        .unwrap();
    assert!(published.iter().any(|e| matches!(
        e,
        Event::MessageBuffered {
            deadline: 11_000,
            ..
        }
    )));
    assert_eq!(engine.state().buffered_messages.len(), 1);

    let (inst, events) = create(&mut engine, "o-1", 5_000);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::BufferedMessageCorrelated { instance_key, .. } if *instance_key == inst)),
        "the opened subscription takes the buffered message: {events:?}"
    );
    assert_eq!(completions(&events, "await"), 1);
    let instance = engine.instance(inst).unwrap();
    assert_eq!(instance.state, ProcessInstanceState::Completed);
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::MessageCorrelated { variables, .. }
                if variables.get("paid") == Some(&Value::Bool(true))
        )),
        "the buffered payload is merged on correlation"
    );

    // The message stays buffered but never re-correlates to the same
    // instance; a second instance still takes it (one per instance).
    let (second, events) = create(&mut engine, "o-1", 6_000);
    assert_eq!(completions(&events, "await"), 1);
    assert_eq!(
        engine.instance(second).unwrap().state,
        ProcessInstanceState::Completed
    );
}

#[test]
fn ttl_zero_never_buffers() {
    for ttl in [None, Some(0)] {
        let mut engine = deployed();
        let published = engine
            .apply_command_at(publish("payment-received", "o-1", ttl, None), 1_000)
            .unwrap();
        assert!(!published
            .iter()
            .any(|e| matches!(e, Event::MessageBuffered { .. })));
        assert!(engine.state().buffered_messages.is_empty());
        let (inst, _) = create(&mut engine, "o-1", 1_001);
        assert_eq!(
            engine.instance(inst).unwrap().state,
            ProcessInstanceState::Active
        );
    }
}

#[test]
fn expired_message_is_not_correlated() {
    // Deadline passed but not yet swept: never correlated.
    let mut engine = deployed();
    engine
        .apply_command_at(publish("payment-received", "o-1", Some(1_000), None), 1_000)
        .unwrap();
    let (inst, events) = create(&mut engine, "o-1", 2_000);
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::BufferedMessageCorrelated { .. })));
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );

    // The TTL checker (TriggerTimers) removes it.
    let ticked = engine
        .apply_command_at(Command::TriggerTimers { now: 2_000 }, 2_000)
        .unwrap();
    assert!(ticked
        .iter()
        .any(|e| matches!(e, Event::MessageExpired { .. })));
    assert!(engine.state().buffered_messages.is_empty());
}

#[test]
fn ticks_before_the_deadline_keep_the_message() {
    let mut engine = deployed();
    engine
        .apply_command_at(publish("payment-received", "o-1", Some(1_000), None), 1_000)
        .unwrap();
    let ticked = engine
        .apply_command_at(Command::TriggerTimers { now: 1_999 }, 1_999)
        .unwrap();
    assert!(ticked.is_empty(), "nothing expires early: {ticked:?}");
    assert_eq!(engine.state().buffered_messages.len(), 1);
}

#[test]
fn duplicate_message_id_is_rejected_while_buffered() {
    let mut engine = deployed();
    engine
        .apply_command_at(
            publish("payment-received", "o-1", Some(1_000), Some("m-1")),
            1_000,
        )
        .unwrap();
    let err = engine
        .apply_command_at(
            publish("payment-received", "o-1", Some(1_000), Some("m-1")),
            1_500,
        )
        .unwrap_err();
    assert!(
        matches!(err, EngineError::MessageAlreadyExists { .. }),
        "{err:?}"
    );
    // A different correlation key or name is a different message.
    engine
        .apply_command_at(
            publish("payment-received", "o-2", Some(1_000), Some("m-1")),
            1_500,
        )
        .unwrap();
    engine
        .apply_command_at(publish("other", "o-1", Some(1_000), Some("m-1")), 1_500)
        .unwrap();
    // Once expired the id is free again.
    engine
        .apply_command_at(
            publish("payment-received", "o-1", Some(1_000), Some("m-1")),
            2_000,
        )
        .unwrap();
}

#[test]
fn suspended_instance_keeps_the_message_buffered() {
    let mut engine = deployed();
    let (inst, _) = create(&mut engine, "o-1", 0);
    engine
        .apply_command(Command::SuspendInstance { instance_key: inst })
        .unwrap();
    engine
        .apply_command_at(
            publish("payment-received", "o-1", Some(10_000), None),
            1_000,
        )
        .unwrap();
    assert_eq!(engine.state().buffered_messages.len(), 1);
    let buffered = engine.state().buffered_messages.values().next().unwrap();
    assert!(buffered.correlated_instances.is_empty());
}

/// The cross-partition path is LOCAL-ONLY for buffered messages (#1874): a
/// subscription opened on the message partition by a routed
/// `OpenMessageSubscription` for an instance on ANOTHER partition takes no
/// buffered message — no `BufferedMessageCorrelated`, no
/// `RemoteMessageCorrelation` — and the message stays buffered, untouched,
/// until its TTL expires. Remote delivery is tracked in
/// https://github.com/nanobpm/nano-bpm/issues/1949.
#[test]
fn routed_open_leaves_buffered_messages_untouched_until_ttl() {
    const N: u64 = 2;
    let order = ('a'..='z')
        .map(|c| c.to_string())
        .find(|k| state::subscription_partition(k, N) == 1)
        .expect("some key hashes to partition 1");
    let mut instance_engine = Engine::with_partition(0);
    instance_engine.set_num_partitions(N);
    instance_engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let mut message_engine = Engine::with_partition(1);
    message_engine.set_num_partitions(N);

    message_engine
        .apply_command_at(
            publish("payment-received", &order, Some(10_000), None),
            1_000,
        )
        .unwrap();
    assert_eq!(message_engine.state().buffered_messages.len(), 1);

    let (instance_key, created) = create(&mut instance_engine, &order, 2_000);
    let open = created
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionOpening {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
                delivery_key,
            } => Some(Command::OpenMessageSubscription {
                subscription_key: *subscription_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                message_name: message_name.clone(),
                correlation_key: correlation_key.clone(),
                kind: kind.clone(),
                delivery_key: *delivery_key,
            }),
            _ => None,
        })
        .expect("a cross-partition Opening");
    let opened = message_engine.apply_command_at(open, 3_000).unwrap();
    assert!(
        !opened.iter().any(|e| matches!(
            e,
            Event::RemoteMessageCorrelation { .. } | Event::BufferedMessageCorrelated { .. }
        )),
        "a remote subscription must not take a buffered message: {opened:?}"
    );
    let buffered = message_engine
        .state()
        .buffered_messages
        .values()
        .next()
        .unwrap();
    assert!(buffered.correlated_instances.is_empty());
    assert_eq!(
        instance_engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Active
    );

    // Still buffered just before the deadline (published at 1_000, TTL 10_000)...
    message_engine
        .apply_command_at(Command::TriggerTimers { now: 10_999 }, 10_999)
        .unwrap();
    assert_eq!(message_engine.state().buffered_messages.len(), 1);
    // ...and expired by the TTL checker at the deadline.
    let ticked = message_engine
        .apply_command_at(Command::TriggerTimers { now: 11_000 }, 11_000)
        .unwrap();
    assert!(ticked
        .iter()
        .any(|e| matches!(e, Event::MessageExpired { .. })));
    assert!(message_engine.state().buffered_messages.is_empty());
}

/// Only the correlation-key partition buffers: the partition-0 copy of a
/// multi-partition publish (message starts) never does, so it never dedups.
#[test]
fn only_the_message_partition_buffers() {
    const N: u64 = 2;
    let order = ('a'..='z')
        .map(|c| c.to_string())
        .find(|k| state::subscription_partition(k, N) == 1)
        .expect("some key hashes to partition 1");
    let mut start_engine = Engine::with_partition(0);
    start_engine.set_num_partitions(N);
    for _ in 0..2 {
        start_engine
            .apply_command(publish("payment-received", &order, Some(10_000), Some("x")))
            .unwrap();
    }
    assert!(start_engine.state().buffered_messages.is_empty());
}

/// Replay parity: replaying the journal of a buffering run reconstructs the
/// same state (buffer included) and key high-water.
#[test]
fn buffered_run_replays_to_identical_state() {
    let mut engine = Engine::new();
    let mut journal: Vec<Event> = Vec::new();
    let mut apply = |engine: &mut Engine, command: Command, now: u64| {
        journal.extend(engine.apply_command_at(command, now).unwrap());
    };
    apply(
        &mut engine,
        Command::DeployProcess(process_with_message_catch()),
        0,
    );
    apply(
        &mut engine,
        publish("payment-received", "o-1", Some(5_000), Some("a")),
        1_000,
    );
    apply(
        &mut engine,
        publish("payment-received", "o-2", Some(1_000), None),
        1_000,
    );
    apply(
        &mut engine,
        Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("o-1".into()))]),
        ),
        2_000,
    );
    apply(&mut engine, Command::TriggerTimers { now: 2_500 }, 2_500);
    assert_eq!(engine.state().buffered_messages.len(), 1);
    assert!(!engine
        .state()
        .buffered_messages
        .values()
        .next()
        .unwrap()
        .correlated_instances
        .is_empty());

    let replayed = Engine::replay_partition(0, journal);
    assert_eq!(replayed.state(), engine.state());
    assert_eq!(replayed.snapshot().next_local, engine.snapshot().next_local);
}

fn publish_with(
    name: &str,
    key: &str,
    variables: BTreeMap<String, Value>,
    message_key: Option<Key>,
) -> Command {
    Command::CorrelateMessage {
        message_name: name.into(),
        correlation_key: key.into(),
        variables,
        business_id: None,
        message_key,
        time_to_live: Some(10_000),
        message_id: None,
    }
}

fn buffered_correlations(events: &[Event]) -> Vec<(Key, Key)> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::BufferedMessageCorrelated {
                message_key,
                instance_key,
                ..
            } => Some((*message_key, *instance_key)),
            _ => None,
        })
        .collect()
}

/// r4692161066: a publication that STARTS an instance (message start) is in
/// that instance's ledger, so a catch for the same name + key inside it waits
/// for another message instead of consuming the one that created it.
#[test]
fn message_start_instance_does_not_reconsume_its_starting_message() {
    let mut engine = Engine::new();
    let def = ProcessBuilder::new("start-then-catch")
        .message_start_event("ms", "payment-received")
        .message_intermediate_catch_event("await", "payment-received", "orderId")
        .end_event("end")
        .connect("ms", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command_at(
            publish_with(
                "payment-received",
                "o-1",
                vars(&[("orderId", Value::Str("o-1".into()))]),
                None,
            ),
            1_000,
        )
        .unwrap();
    let inst = events
        .iter()
        .find_map(|e| match e {
            Event::MessageStartEventCorrelated { instance_key, .. } => Some(*instance_key),
            _ => None,
        })
        .expect("the publish starts an instance");
    assert!(
        buffered_correlations(&events).is_empty(),
        "the starting message must not also resume the catch: {events:?}"
    );
    assert_eq!(
        engine.instance(inst).unwrap().state,
        ProcessInstanceState::Active
    );
    let buffered = engine.state().buffered_messages.values().next().unwrap();
    assert!(buffered.correlated_instances.contains(&inst));
}

/// r4692161117: a message event sub-process armed with a scope whose main
/// flow drains at once still takes a buffered message — the buffered scan
/// runs before the completion sweeps disarm it (root and embedded scope).
#[test]
fn drained_scope_event_subprocess_takes_buffered_message() {
    use crate::model::EventSubProcessTrigger as Trig;
    for embedded in [false, true] {
        let mut b = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .event_sub_process(
                "esp",
                "esp_start",
                Trig::Message {
                    message_name: "m".into(),
                    correlation_key: "orderId".into(),
                },
                true,
            )
            .start_event("esp_start")
            .service_task("esp_work", "esp")
            .end_event("esp_end")
            .connect("esp_start", "esp_work")
            .connect("esp_work", "esp_end")
            .contained_in("esp_start", "esp")
            .contained_in("esp_work", "esp")
            .contained_in("esp_end", "esp");
        if embedded {
            b = b
                .sub_process("sub", "sub_s")
                .start_event("sub_s")
                .end_event("sub_e")
                .connect("s", "sub")
                .connect("sub_s", "sub_e")
                .connect("sub", "e")
                .contained_in("sub_s", "sub")
                .contained_in("sub_e", "sub")
                .contained_in("esp", "sub");
        } else {
            b = b.connect("s", "e");
        }
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(b.build().unwrap()))
            .unwrap();
        engine
            .apply_command_at(publish_with("m", "o-1", BTreeMap::new(), None), 1_000)
            .unwrap();
        let events = engine
            .apply_command_at(
                Command::create_instance_with("p", vars(&[("orderId", Value::Str("o-1".into()))])),
                2_000,
            )
            .unwrap();
        assert_eq!(
            buffered_correlations(&events).len(),
            1,
            "embedded={embedded}: the event sub-process takes the message: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::ElementActivated { element_id, .. } if element_id.as_str() == "esp_work")),
            "embedded={embedded}: the event sub-process runs"
        );
    }
}

/// r4692161163: two event-based gateway alternatives with distinct buffered
/// messages — only the winner correlates; the withdrawn loser takes nothing,
/// so its payload is never merged and its message stays available.
#[test]
fn event_based_gateway_takes_only_one_buffered_message() {
    let def = ProcessBuilder::new("race")
        .start_event("start")
        .event_based_gateway("gw")
        .message_intermediate_catch_event("onM1", "M1", "orderId")
        .message_intermediate_catch_event("onM2", "M2", "orderId")
        .end_event("end")
        .connect("start", "gw")
        .connect("gw", "onM1")
        .connect("gw", "onM2")
        .connect("onM1", "end")
        .connect("onM2", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command_at(
            publish_with(
                "M1",
                "o-1",
                vars(&[("winner", Value::Str("M1".into()))]),
                None,
            ),
            1_000,
        )
        .unwrap();
    engine
        .apply_command_at(
            publish_with(
                "M2",
                "o-1",
                vars(&[("winner", Value::Str("M2".into()))]),
                None,
            ),
            1_000,
        )
        .unwrap();
    let events = engine
        .apply_command_at(
            Command::create_instance_with("race", vars(&[("orderId", Value::Str("o-1".into()))])),
            2_000,
        )
        .unwrap();
    assert_eq!(
        buffered_correlations(&events).len(),
        1,
        "only the winning alternative takes a message: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::MessageCorrelated { .. }))
            .count(),
        1
    );
    let untouched = engine
        .state()
        .buffered_messages
        .values()
        .filter(|m| m.correlated_instances.is_empty())
        .count();
    assert_eq!(untouched, 1, "the loser's message stays available");
}

/// r4692161207: buffered correlation takes the OLDEST buffered message by
/// enqueue order, not by key — a key pre-minted by another partition sorts
/// after this partition's newer keys. Holds across replay too.
#[test]
fn oldest_buffered_message_wins_regardless_of_key_origin() {
    let foreign = (1u64 << state::LOCAL_BITS) | 7; // partition-1 pre-minted key
    let mut journal = Vec::new();
    let mut engine = Engine::new();
    journal.extend(
        engine
            .apply_command_at(Command::DeployProcess(process_with_message_catch()), 0)
            .unwrap(),
    );
    journal.extend(
        engine
            .apply_command_at(
                publish_with("payment-received", "o-1", BTreeMap::new(), Some(foreign)),
                1_000,
            )
            .unwrap(),
    );
    journal.extend(
        engine
            .apply_command_at(
                publish_with("payment-received", "o-1", BTreeMap::new(), None),
                1_100,
            )
            .unwrap(),
    );
    let mut replayed = Engine::replay_partition(0, journal);
    assert_eq!(replayed.state(), engine.state());
    for e in [&mut engine, &mut replayed] {
        let (_, events) = create(e, "o-1", 2_000);
        let got = buffered_correlations(&events);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, foreign, "the first-published message wins");
    }
}

/// A main flow that parks on a service task, plus a NON-interrupting message
/// event sub-process: a persistent subscription that stays open after firing.
fn non_interrupting_esp_process() -> ProcessDefinition {
    use crate::model::EventSubProcessTrigger as Trig;
    ProcessBuilder::new("ni")
        .start_event("s")
        .service_task("work", "work")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
        .event_sub_process(
            "esp",
            "esp_start",
            Trig::Message {
                message_name: "m".into(),
                correlation_key: "orderId".into(),
            },
            false,
        )
        .start_event("esp_start")
        .end_event("esp_end")
        .connect("esp_start", "esp_end")
        .contained_in("esp_start", "esp")
        .contained_in("esp_end", "esp")
        .build()
        .unwrap()
}

/// r4692383366 (previously missed): a still-armed non-interrupting
/// subscription takes EVERY matching buffered message, oldest first — not
/// just the first one.
#[test]
fn non_interrupting_subscription_drains_the_whole_backlog_locally() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(non_interrupting_esp_process()))
        .unwrap();
    let mut published = Vec::new();
    for t in [1_000, 1_001, 1_002] {
        let events = engine
            .apply_command_at(publish_with("m", "o-1", BTreeMap::new(), None), t)
            .unwrap();
        published.push(
            events
                .iter()
                .find_map(|e| match e {
                    Event::MessageBuffered { message_key, .. } => Some(*message_key),
                    _ => None,
                })
                .expect("buffered"),
        );
    }
    engine.buffer_scan_count.set(0);
    let events = engine
        .apply_command_at(
            Command::create_instance_with("ni", vars(&[("orderId", Value::Str("o-1".into()))])),
            2_000,
        )
        .unwrap();
    let taken: Vec<Key> = buffered_correlations(&events)
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    assert_eq!(
        taken, published,
        "every buffered message, in order: {events:?}"
    );
    // r4692584915: the backlog is drained from one ordered read of the
    // matching index set — each message visited at most twice (found, then
    // stepped past on resume), never once per message.
    assert!(engine.buffer_scan_count.get() <= 2 * published.len());
    // r4693206193: the per-command candidate cache does not outlive the
    // command (no idle retention).
    assert!(engine.buffer_candidates.is_empty());
}

/// The routed twin: a still-open non-interrupting subscription whose instance
/// is on another partition takes NONE of the backlog (local-only buffered
/// correlation, #1874; remote delivery is
/// https://github.com/nanobpm/nano-bpm/issues/1949) — never a partial drain.
#[test]
fn non_interrupting_remote_subscription_takes_none_of_the_backlog() {
    const N: u64 = 2;
    let order = ('a'..='z')
        .map(|c| c.to_string())
        .find(|k| state::subscription_partition(k, N) == 1)
        .expect("some key hashes to partition 1");
    let mut instance_engine = Engine::with_partition(0);
    instance_engine.set_num_partitions(N);
    instance_engine
        .apply_command(Command::DeployProcess(non_interrupting_esp_process()))
        .unwrap();
    let mut message_engine = Engine::with_partition(1);
    message_engine.set_num_partitions(N);
    for t in [1_000, 1_001] {
        message_engine
            .apply_command_at(publish_with("m", &order, BTreeMap::new(), None), t)
            .unwrap();
    }
    let created = instance_engine
        .apply_command_at(
            Command::create_instance_with("ni", vars(&[("orderId", Value::Str(order.clone()))])),
            2_000,
        )
        .unwrap();
    let open = created
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionOpening {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
                delivery_key,
            } => Some(Command::OpenMessageSubscription {
                subscription_key: *subscription_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                message_name: message_name.clone(),
                correlation_key: correlation_key.clone(),
                kind: kind.clone(),
                delivery_key: *delivery_key,
            }),
            _ => None,
        })
        .expect("a cross-partition Opening");
    let opened = message_engine.apply_command_at(open, 3_000).unwrap();
    let remote = opened
        .iter()
        .filter(|e| matches!(e, Event::RemoteMessageCorrelation { .. }))
        .count();
    assert!(buffered_correlations(&opened).is_empty(), "{opened:?}");
    assert_eq!(remote, 0, "{opened:?}");
    assert!(message_engine
        .state()
        .buffered_messages
        .values()
        .all(|m| m.correlated_instances.is_empty()));
}

/// A parent that forks into a call activity and a top-level terminate end must
/// terminate the child before a buffered message can complete it (#1874): the
/// child's freshly opened catch must not correlate while its parent has ended.
#[test]
fn buffered_message_never_reaches_a_child_of_an_ended_parent() {
    let mut engine = deployed();
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .parallel_gateway("split")
        .call_activity("c1", "await-payment")
        .end_event("c1_end")
        .terminate_end_event("stop")
        .connect("start", "split")
        .connect("split", "c1")
        .connect("split", "stop")
        .connect("c1", "c1_end")
        .build()
        .unwrap();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    engine
        .apply_command_at(
            publish("payment-received", "o-1", Some(10_000), None),
            1_000,
        )
        .unwrap();
    let events = engine
        .apply_command_at(
            Command::create_instance_with("orch", vars(&[("orderId", Value::Str("o-1".into()))])),
            2_000,
        )
        .unwrap();
    let parent = events.iter().find_map(|e| e.instance_key()).unwrap();
    let child = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } if *instance_key != parent => {
                Some(*instance_key)
            }
            _ => None,
        })
        .expect("call activity spawned a child");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::BufferedMessageCorrelated { .. })),
        "no buffered correlation into a child of an ended parent: {events:?}"
    );
    assert_eq!(
        engine.instance(child).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert_eq!(engine.state().buffered_messages.len(), 1);
}

/// Publish-time messageId dedup is an INDEX lookup (#1874, Zeebe's messageId
/// column family), not a buffer scan: the index tracks every buffered id,
/// frees an expired id (even before the sweep removes the message), drops it
/// on expiry, and is re-derived on replay and on snapshot restore.
#[test]
fn message_id_dedup_uses_an_index_that_survives_replay_and_restore() {
    let mut engine = Engine::new();
    let mut journal: Vec<Event> = Vec::new();
    journal.extend(
        engine
            .apply_command_at(Command::DeployProcess(process_with_message_catch()), 0)
            .unwrap(),
    );
    for (i, id) in ["m-1", "m-2", "m-3"].iter().enumerate() {
        journal.extend(
            engine
                .apply_command_at(
                    publish("payment-received", "o-1", Some(1_000 + i as u64), Some(id)),
                    1_000,
                )
                .unwrap(),
        );
    }
    let index_key = |id: &str| {
        (
            "payment-received".to_string(),
            "o-1".to_string(),
            id.to_string(),
        )
    };
    assert_eq!(engine.state().buffered_message_ids.len(), 3);
    assert!(engine
        .state()
        .buffered_message_ids
        .contains_key(&index_key("m-2")));

    let is_dup = |e: &mut Engine, id: &str, now: u64| {
        matches!(
            e.apply_command_at(
                publish("payment-received", "o-1", Some(1_000), Some(id)),
                now
            ),
            Err(EngineError::MessageAlreadyExists { .. })
        )
    };

    // Replay re-derives the index; a duplicate is still rejected there.
    let mut replayed = Engine::replay_partition(0, journal);
    assert_eq!(replayed.state(), engine.state());
    assert_eq!(
        replayed.state().buffered_message_ids,
        engine.state().buffered_message_ids
    );
    assert!(is_dup(&mut replayed, "m-2", 1_500));

    // Snapshot restore re-derives the serde-skipped index.
    #[cfg(feature = "serde")]
    {
        let json = serde_json::to_string(&engine.snapshot()).unwrap();
        let mut restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
        assert_eq!(
            restored.state().buffered_message_ids,
            engine.state().buffered_message_ids
        );
        assert!(is_dup(&mut restored, "m-3", 1_500));
    }

    assert!(is_dup(&mut engine, "m-1", 1_500));
    // m-1 (deadline 2_000) has expired but is not yet swept: still indexed,
    // yet its id is free again.
    assert_eq!(engine.state().buffered_messages.len(), 3);
    engine
        .apply_command_at(
            publish("payment-received", "o-1", Some(10_000), Some("m-1")),
            2_000,
        )
        .unwrap();
    assert!(is_dup(&mut engine, "m-1", 2_100));

    // The sweep removes the expired messages; their index entries go too,
    // except m-1, which now points at the fresh re-publish.
    engine
        .apply_command_at(Command::TriggerTimers { now: 5_000 }, 5_000)
        .unwrap();
    assert_eq!(engine.state().buffered_messages.len(), 1);
    assert_eq!(engine.state().buffered_message_ids.len(), 1);
    assert!(is_dup(&mut engine, "m-1", 5_000));
}

fn fan_out_catches(n: usize) -> ProcessDefinition {
    let mut b = ProcessBuilder::new("fan")
        .start_event("start")
        .parallel_gateway("fork");
    b = b.connect("start", "fork");
    for i in 0..n {
        let (c, e) = (format!("c{i}"), format!("e{i}"));
        b = b
            .message_intermediate_catch_event(&c, "m", "orderId")
            .end_event(&e)
            .connect("fork", &c)
            .connect(&c, &e);
    }
    b.build().unwrap()
}

/// Copilot (PR 1944): k subscriptions opening in one command must not each
/// filter the whole buffer (O(k*N)). Candidates come from the
/// (name, correlation key) index, so a backlog under OTHER keys is never
/// visited, and the index stays consistent across expiry and replay.
#[test]
fn opening_subscriptions_never_scan_unrelated_buffered_messages() {
    let mut engine = Engine::new();
    let mut journal: Vec<Event> = Vec::new();
    journal.extend(
        engine
            .apply_command_at(Command::DeployProcess(fan_out_catches(50)), 0)
            .unwrap(),
    );
    for i in 0..1_000u64 {
        let ttl = if i % 2 == 0 { 1_000 } else { 100_000 };
        journal.extend(
            engine
                .apply_command_at(publish("m", &format!("other-{i}"), Some(ttl), None), 1_000)
                .unwrap(),
        );
    }
    journal.extend(
        engine
            .apply_command_at(publish("m", "o-1", Some(100_000), None), 1_000)
            .unwrap(),
    );
    engine.buffer_scan_count.set(0);
    let events = engine
        .apply_command_at(
            Command::create_instance_with("fan", vars(&[("orderId", Value::Str("none".into()))])),
            1_500,
        )
        .unwrap();
    journal.extend(events.iter().cloned());
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::MessageSubscriptionCreated { .. }))
            .count(),
        50
    );
    assert_eq!(
        engine.buffer_scan_count.get(),
        0,
        "no buffered message matches, so none may be visited"
    );

    // With one match, each of the 50 catches visits only that message.
    engine.buffer_scan_count.set(0);
    let events = engine
        .apply_command_at(
            Command::create_instance_with("fan", vars(&[("orderId", Value::Str("o-1".into()))])),
            1_600,
        )
        .unwrap();
    journal.extend(events.iter().cloned());
    assert!(engine.buffer_scan_count.get() <= 50);
    assert_eq!(buffered_correlations(&events).len(), 1);

    // Expiry sweep drops entries from the index too.
    journal.extend(
        engine
            .apply_command_at(Command::TriggerTimers { now: 5_000 }, 5_000)
            .unwrap(),
    );
    assert_eq!(engine.state().buffered_messages.len(), 501);
    let indexed: usize = engine
        .state()
        .buffered_by_correlation
        .values()
        .map(BTreeSet::len)
        .sum();
    assert_eq!(indexed, 501);
    engine.state().assert_buffered_message_id_index_consistent();

    let replayed = Engine::replay_partition(0, journal);
    assert_eq!(
        replayed.state().buffered_by_correlation,
        engine.state().buffered_by_correlation
    );
    #[cfg(feature = "serde")]
    {
        let json = serde_json::to_string(&engine.snapshot()).unwrap();
        let restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
        assert_eq!(
            restored.state().buffered_by_correlation,
            engine.state().buffered_by_correlation
        );
    }
}

/// Adversarial review (PR 1944): k one-shot catches of ONE instance on the
/// same (name, correlation key) against M matching buffered messages must
/// cost O(k + M) index visits, not O(k*M) — each catch resumes from the
/// instance's shared cursor instead of re-reading the whole matching set.
#[test]
fn consecutive_catches_over_a_matching_backlog_visit_linearly() {
    let mut engine = Engine::new();
    engine
        .apply_command_at(Command::DeployProcess(fan_out_catches(50)), 0)
        .unwrap();
    for _ in 0..1_000u64 {
        engine
            .apply_command_at(publish("m", "o-1", Some(100_000), None), 1_000)
            .unwrap();
    }
    engine.buffer_scan_count.set(0);
    let events = engine
        .apply_command_at(
            Command::create_instance_with("fan", vars(&[("orderId", Value::Str("o-1".into()))])),
            1_500,
        )
        .unwrap();
    assert_eq!(buffered_correlations(&events).len(), 50);
    assert!(
        engine.buffer_scan_count.get() <= 2 * (50 + 50),
        "visits must be O(k + taken), got {}",
        engine.buffer_scan_count.get()
    );
    assert!(engine.buffer_candidates.is_empty());
}
