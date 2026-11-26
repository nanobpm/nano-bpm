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

/// Rolling-upgrade gate (#1949): with remote buffered delivery disabled (a
/// group member cannot apply ack-paced delivery), a routed open takes nothing
/// from the buffer — byte-for-byte the pre-#1949 local-only behaviour, so a
/// legacy follower applying the same command stays in step.
#[test]
fn routed_open_with_remote_delivery_disabled_stays_local_only() {
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
    message_engine.set_remote_buffered_delivery(false);
    message_engine
        .apply_command_at(
            publish("payment-received", &order, Some(10_000), None),
            1_000,
        )
        .unwrap();
    let (_, created) = create(&mut instance_engine, &order, 2_000);
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
        "gate closed: no remote buffered delivery, got {opened:?}"
    );
    assert!(message_engine
        .state()
        .message_subscriptions
        .values()
        .all(|s| s.awaiting_ack.is_none()));
    let buffered = message_engine
        .state()
        .buffered_messages
        .values()
        .next()
        .unwrap();
    assert!(buffered.correlated_instances.is_empty());
    assert!(!message_engine.message_correlation_ack_ever_used());
}

/// The cross-partition path: the canonical subscription is opened on the
/// message partition by a routed `OpenMessageSubscription`; a buffered message
/// there is settled as a `RemoteMessageCorrelation` for the host to route.
#[test]
fn routed_open_correlates_a_buffered_message_remotely() {
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
    assert!(!message_engine.message_correlation_ack_ever_used());
    let opened = message_engine.apply_command_at(open, 3_000).unwrap();
    // Sticky capability requirement (#1949): once sent, the partition's
    // retained state depends on `message-correlation-ack-v1`.
    assert!(message_engine.message_correlation_ack_ever_used());
    let continuation = opened
        .iter()
        .find_map(|e| match e {
            Event::RemoteMessageCorrelation {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id,
                kind,
                variables,
                delivery_key,
                ack_partition,
            } => Some(Command::CorrelateMessageSubscription {
                subscription_key: *subscription_key,
                message_key: *message_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                kind: kind.clone(),
                variables: variables.clone(),
                delivery_key: *delivery_key,
                ack_partition: *ack_partition,
            }),
            _ => None,
        })
        .unwrap_or_else(|| panic!("the buffered message settles remotely: {opened:?}"));
    assert!(opened
        .iter()
        .any(|e| matches!(e, Event::BufferedMessageCorrelated { .. })));
    instance_engine
        .apply_command_at(continuation, 3_001)
        .unwrap();
    assert_eq!(
        instance_engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Completed
    );
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

/// The cross-partition command a routed follow-up event becomes (the host's
/// `route_event`, test twin): `(true, cmd)` targets the message partition,
/// `(false, cmd)` the instance partition.
fn routed(event: &Event) -> Option<(bool, Command)> {
    Some(match event.clone() {
        Event::MessageSubscriptionOpening {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            message_name,
            correlation_key,
            kind,
            delivery_key,
        } => (
            true,
            Command::OpenMessageSubscription {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                message_name,
                correlation_key,
                kind,
                delivery_key,
            },
        ),
        Event::MessageSubscriptionClosing {
            subscription_key,
            instance_key,
            element_instance_key,
            element_id,
            delivery_key,
            ..
        } => (
            true,
            Command::CloseMessageSubscription {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                delivery_key,
            },
        ),
        Event::RemoteMessageCorrelation {
            subscription_key,
            message_key,
            instance_key,
            element_instance_key,
            element_id,
            kind,
            variables,
            delivery_key,
            ack_partition,
        } => (
            false,
            Command::CorrelateMessageSubscription {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                kind,
                variables,
                delivery_key,
                ack_partition,
            },
        ),
        Event::MessageSubscriptionResuming {
            subscription_key,
            generation,
            ..
        } => (
            true,
            Command::ResumeMessageSubscription {
                subscription_key,
                generation,
            },
        ),
        Event::MessageSubscriptionResumed {
            subscription_key,
            generation,
            ..
        } => (
            false,
            Command::ConfirmMessageSubscriptionResume {
                subscription_key,
                generation,
            },
        ),
        Event::MessageCorrelationSettled {
            subscription_key,
            delivery_key,
            ..
        } => (
            false,
            Command::SettleMessageCorrelationReceipt {
                subscription_key,
                delivery_key,
            },
        ),
        Event::MessageCorrelationAcknowledging {
            subscription_key,
            message_key,
            accepted,
            consumed,
            deferred,
            delivery_key,
            element_id,
            ..
        } => (
            true,
            Command::AcknowledgeMessageCorrelation {
                subscription_key,
                message_key,
                delivery_key,
                accepted,
                consumed,
                deferred,
                element_id,
            },
        ),
        _ => return None,
    })
}

/// A two-partition rig (instance partition 0, message partition 1) with a FIFO
/// routing pump and a journal of the message partition, for replay checks.
struct Rig {
    instance: Engine,
    message: Engine,
    message_journal: Vec<Event>,
    /// The instance partition's journal, for restart checks on that side.
    instance_journal: Vec<Event>,
    /// Every event either partition emitted, in routing order.
    trace: Vec<Event>,
}

impl Rig {
    const N: u64 = 2;

    fn new(definition: ProcessDefinition) -> (Self, String) {
        let order = ('a'..='z')
            .map(|c| c.to_string())
            .find(|k| state::subscription_partition(k, Self::N) == 1)
            .expect("some key hashes to partition 1");
        let mut instance = Engine::with_partition(0);
        instance.set_num_partitions(Self::N);
        let instance_journal = instance
            .apply_command(Command::DeployProcess(definition))
            .unwrap();
        let mut message = Engine::with_partition(1);
        message.set_num_partitions(Self::N);
        let rig = Self {
            instance,
            message,
            message_journal: Vec::new(),
            instance_journal,
            trace: Vec::new(),
        };
        (rig, order)
    }

    fn on_instance(&mut self, command: Command, now: u64) -> Vec<Event> {
        let events = self.instance.apply_command_at(command, now).unwrap();
        self.instance_journal.extend(events.iter().cloned());
        events
    }

    fn on_message(&mut self, command: Command, now: u64) -> Vec<Event> {
        let events = self.message.apply_command_at(command, now).unwrap();
        self.message_journal.extend(events.iter().cloned());
        events
    }

    /// Applies `command` on `to_message`'s partition, then routes every
    /// cross-partition follow-up FIFO until quiescent.
    fn run(&mut self, to_message: bool, command: Command, now: u64) {
        let mut pending = std::collections::VecDeque::from([(to_message, command)]);
        while let Some((to_message, command)) = pending.pop_front() {
            let events = if to_message {
                self.on_message(command, now)
            } else {
                self.on_instance(command, now)
            };
            pending.extend(events.iter().filter_map(routed));
            self.trace.extend(events);
        }
    }

    fn create(&mut self, process: &str, order: &str, now: u64) -> Key {
        let before = self.trace.len();
        self.run(
            false,
            Command::create_instance_with(process, vars(&[("orderId", Value::Str(order.into()))])),
            now,
        );
        self.trace[before..]
            .iter()
            .find_map(|e| match e {
                Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
                _ => None,
            })
            .expect("an instance was created")
    }

    fn publish_backlog(&mut self, order: &str, count: u64) -> Vec<Key> {
        self.publish_named_backlog("m", order, count)
    }

    fn publish_named_backlog(&mut self, name: &str, order: &str, count: u64) -> Vec<Key> {
        (0..count)
            .map(|i| {
                self.on_message(publish_with(name, order, BTreeMap::new(), None), 1_000 + i)
                    .iter()
                    .find_map(|e| match e {
                        Event::MessageBuffered { message_key, .. } => Some(*message_key),
                        _ => None,
                    })
                    .expect("buffered")
            })
            .collect()
    }

    /// The message keys delivered remotely, in delivery order, per instance.
    fn delivered_to(&self, instance_key: Key) -> Vec<Key> {
        self.trace
            .iter()
            .filter_map(|e| match e {
                Event::RemoteMessageCorrelation {
                    message_key,
                    instance_key: i,
                    ..
                } if *i == instance_key => Some(*message_key),
                _ => None,
            })
            .collect()
    }
}

/// #1874 (Zeebe `CORRELATING`): a remote non-interrupting subscription drains
/// a buffered backlog ONE message per ack round-trip, in publish order. The
/// open command itself sends exactly one; each accepted ack sends the next.
#[test]
fn remote_non_interrupting_subscription_drains_backlog_via_acks() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 3);
    let instance_key = rig.create("ni", &order, 2_000);
    assert_eq!(rig.delivered_to(instance_key), published, "{:?}", rig.trace);
    // Each delivery is ack-paced and acknowledged before the next is sent.
    let mut in_flight = false;
    for event in &rig.trace {
        match event {
            Event::RemoteMessageCorrelation { ack_partition, .. } => {
                assert_eq!(*ack_partition, Some(1));
                assert!(
                    !in_flight,
                    "a second delivery before the ack: {:?}",
                    rig.trace
                );
                in_flight = true;
            }
            Event::MessageCorrelationAcknowledged { accepted, .. } => {
                assert!(*accepted);
                in_flight = false;
            }
            _ => {}
        }
    }
    let subscription = rig
        .message
        .state()
        .message_subscriptions
        .values()
        .next()
        .unwrap();
    assert_eq!(subscription.state, state::MessageSubscriptionState::Open);
    assert_eq!(subscription.awaiting_ack, None);
    // The instance partition spawned one handler token per message.
    let handled = rig
        .trace
        .iter()
        .filter(|e| matches!(e, Event::MessageCorrelated { .. }))
        .count();
    assert_eq!(handled, 3);
}

/// A non-interrupting message boundary whose side path terminates the whole
/// instance: the first delivery ends the subscription's scope.
fn terminating_non_interrupting_boundary_process() -> ProcessDefinition {
    ProcessBuilder::new("nt")
        .start_event("s")
        .service_task("work", "work")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
        .non_interrupting_message_boundary_event("b", "work", "m", "orderId")
        .terminate_end_event("stop")
        .connect("b", "stop")
        .build()
        .unwrap()
}

/// #1874: when the first ack-paced delivery ends the subscription's scope, the
/// message partition stops — the remaining backlog stays buffered, and a later
/// subscription takes it.
#[test]
fn remote_delivery_that_ends_the_scope_leaves_the_rest_buffered() {
    let (mut rig, order) = Rig::new(terminating_non_interrupting_boundary_process());
    let published = rig.publish_backlog(&order, 3);
    let first = rig.create("nt", &order, 2_000);
    assert_eq!(rig.delivered_to(first), published[..1], "{:?}", rig.trace);
    // A top-level terminate end completes the instance.
    assert!(rig.instance.is_completed(first));
    assert!(rig.trace.iter().any(|e| matches!(
        e,
        Event::MessageCorrelationAcknowledged {
            accepted: false,
            ..
        }
    )));
    assert_eq!(rig.message.state().buffered_messages.len(), 3);
    assert!(rig
        .message
        .state()
        .message_subscriptions
        .values()
        .all(|s| s.state != state::MessageSubscriptionState::Open && s.awaiting_ack.is_none()));
    // A later subscription (a new instance) takes the untaken backlog, oldest
    // first — one message, since its handler ends its scope too.
    let second = rig.create("nt", &order, 3_000);
    assert_eq!(rig.delivered_to(second), published[..1]);
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published_ni = rig.publish_backlog(&order, 3);
    let third = rig.create("ni", &order, 2_000);
    assert_eq!(rig.delivered_to(third), published_ni);
}

/// #1949: a remote INTERRUPTING (one-shot) subscription takes exactly one
/// buffered message. Its delivery is ack-paced too, so the message partition
/// learns the token consumed it before the reservation is final.
#[test]
fn remote_interrupting_subscription_takes_exactly_one() {
    let (mut rig, order) = Rig::new(process_with_message_catch());
    let published = rig.publish_named_backlog("payment-received", &order, 3);
    let instance_key = rig.create("await-payment", &order, 2_000);
    assert_eq!(rig.delivered_to(instance_key), published[..1]);
    assert!(rig.instance.is_completed(instance_key));
    let acks: Vec<_> = rig
        .trace
        .iter()
        .filter_map(|e| match e {
            Event::MessageCorrelationAcknowledged {
                accepted, consumed, ..
            } => Some((*accepted, *consumed)),
            _ => None,
        })
        .collect();
    assert_eq!(acks, vec![(false, true)], "{:?}", rig.trace);
    let subscription = rig
        .message
        .state()
        .message_subscriptions
        .values()
        .next()
        .unwrap();
    assert_eq!(
        subscription.state,
        state::MessageSubscriptionState::Correlated
    );
    assert_eq!(subscription.awaiting_ack, None);
    let buffered = &rig.message.state().buffered_messages;
    assert!(buffered[&published[0]]
        .correlated_instances
        .contains(&instance_key));
    assert!(published[1..]
        .iter()
        .all(|k| buffered[k].correlated_instances.is_empty()));
}

/// #1874: the ack-paced state is event-sourced — replaying the message
/// partition's journal, both mid-flight (awaiting an ack) and after the drain,
/// reproduces the live state exactly.
#[test]
fn ack_paced_drain_replays_deterministically() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.publish_backlog(&order, 3);
    // Mid-flight: open the subscription on the message partition without
    // routing the reply, so it is left awaiting the first ack.
    let created = rig
        .instance
        .apply_command_at(
            Command::create_instance_with("ni", vars(&[("orderId", Value::Str(order.clone()))])),
            2_000,
        )
        .unwrap();
    let open = created
        .iter()
        .filter_map(routed)
        .map(|(_, c)| c)
        .next()
        .expect("a cross-partition open");
    let opened = rig.on_message(open, 2_001);
    let awaiting = rig
        .message
        .state()
        .message_subscriptions
        .values()
        .next()
        .unwrap();
    assert!(awaiting.awaiting_ack.is_some());
    let replayed = Engine::replay_partition(1, rig.message_journal.clone());
    assert_eq!(replayed.state(), rig.message.state());
    // A redelivered/stale ack for another message is a no-op.
    let sub = awaiting.key;
    assert!(rig
        .on_message(
            Command::AcknowledgeMessageCorrelation {
                subscription_key: sub,
                message_key: 1,
                delivery_key: 0,
                accepted: true,
                consumed: true,
                deferred: false,
                element_id: None,
            },
            2_002,
        )
        .is_empty());
    // Finish the drain through the pump, then replay the full journal.
    for (to_message, command) in opened.iter().filter_map(routed).collect::<Vec<_>>() {
        rig.run(to_message, command, 2_003);
    }
    assert_eq!(
        rig.trace
            .iter()
            .filter(|e| matches!(e, Event::MessageCorrelationAcknowledged { .. }))
            .count(),
        3
    );
    let replayed = Engine::replay_partition(1, rig.message_journal.clone());
    assert_eq!(replayed.state(), rig.message.state());
    assert_eq!(
        replayed.snapshot().next_local,
        rig.message.snapshot().next_local
    );
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

/// #1874 (adversarial review): an ack-paced buffered delivery that reaches a
/// SUSPENDED instance must not close the canonical persistent subscription.
/// The token takes nothing while suspended (the message stays buffered for the
/// instance, matching the local path), and after `ResumeInstance` the still
/// waiting non-interrupting event sub-process keeps receiving messages.
#[test]
fn remote_delivery_to_suspended_instance_keeps_subscription_open() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 2);
    // Create the instance, but suspend it before routing its open.
    let created = rig
        .instance
        .apply_command_at(
            Command::create_instance_with("ni", vars(&[("orderId", Value::Str(order.clone()))])),
            2_000,
        )
        .unwrap();
    let instance_key = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        })
        .expect("created");
    let opens: Vec<_> = created.iter().filter_map(routed).collect();
    rig.trace.extend(created);
    rig.run(false, Command::SuspendInstance { instance_key }, 2_001);
    for (to_message, command) in opens {
        rig.run(to_message, command, 2_002);
    }
    // The suspended token took nothing.
    assert!(!rig
        .trace
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    // The canonical subscription is still open, not awaiting, and the offered
    // message is still buffered for this instance.
    let subs: Vec<_> = rig.message.state().message_subscriptions.values().collect();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);
    assert!(subs[0].awaiting_ack.is_none());
    for key in &published {
        let m = &rig.message.state().buffered_messages[key];
        assert!(!m.correlated_instances.contains(&instance_key));
    }
    // After resume, a live publish still reaches the waiting sub-process.
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    rig.run(
        true,
        publish_with("m", &order, BTreeMap::new(), None),
        3_001,
    );
    assert!(
        rig.trace.iter().any(
            |e| matches!(e, Event::MessageCorrelated { instance_key: i, .. } if *i == instance_key)
        ),
        "{:?}",
        rig.trace
    );
    let replayed = Engine::replay_partition(1, rig.message_journal.clone());
    assert_eq!(replayed.state(), rig.message.state());
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

impl Rig {
    /// Like [`Rig::run`], but every routed command `hold` selects is NOT
    /// delivered (a lost or delayed send); the held ones are returned.
    fn run_holding(
        &mut self,
        to_message: bool,
        command: Command,
        now: u64,
        hold: &dyn Fn(&Command) -> bool,
    ) -> Vec<(bool, Command)> {
        let mut held = Vec::new();
        let mut pending = std::collections::VecDeque::from([(to_message, command)]);
        while let Some((to_message, command)) = pending.pop_front() {
            let events = if to_message {
                self.on_message(command, now)
            } else {
                self.on_instance(command, now)
            };
            for routed in events.iter().filter_map(routed) {
                if hold(&routed.1) {
                    held.push(routed);
                } else {
                    pending.push_back(routed);
                }
            }
            self.trace.extend(events);
        }
        held
    }

    fn create_holding(
        &mut self,
        process: &str,
        order: &str,
        now: u64,
        hold: &dyn Fn(&Command) -> bool,
    ) -> (Key, Vec<(bool, Command)>) {
        let before = self.trace.len();
        let held = self.run_holding(
            false,
            Command::create_instance_with(process, vars(&[("orderId", Value::Str(order.into()))])),
            now,
            hold,
        );
        let instance_key = self.trace[before..]
            .iter()
            .find_map(|e| match e {
                Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
                _ => None,
            })
            .expect("an instance was created");
        (instance_key, held)
    }

    fn acks(&self) -> Vec<(bool, bool)> {
        self.trace
            .iter()
            .filter_map(|e| match e {
                Event::MessageCorrelationAcknowledged {
                    accepted, consumed, ..
                } => Some((*accepted, *consumed)),
                _ => None,
            })
            .collect()
    }
}

/// A duplicate / stale ack changes nothing on the message partition: it only
/// (re-)confirms the delivery's settlement to the instance partition (#1949).
fn assert_only_settles(events: &[Event]) {
    assert!(
        !events.is_empty()
            && events
                .iter()
                .all(|e| matches!(e, Event::MessageCorrelationSettled { .. })),
        "{events:?}"
    );
}

fn is_delivery(command: &Command) -> bool {
    matches!(command, Command::CorrelateMessageSubscription { .. })
}

fn is_ack(command: &Command) -> bool {
    matches!(command, Command::AcknowledgeMessageCorrelation { .. })
}

/// The routed command a re-driven [`Engine::pending_buffered_deliveries`]
/// event becomes.
fn redrive(rig: &Rig) -> Vec<(bool, Command)> {
    rig.message
        .pending_buffered_deliveries()
        .iter()
        .filter_map(routed)
        .collect()
}

/// An interrupting message boundary on a task, then a catch for the SAME
/// message after it: a buffered delivery meant for the boundary that arrives
/// after the task completed must not use the message up for the catch.
fn boundary_then_catch_process() -> ProcessDefinition {
    ProcessBuilder::new("bc")
        .start_event("s")
        .service_task("work", "work")
        .message_boundary_event("b", "work", "m", "orderId")
        .end_event("b_end")
        .message_intermediate_catch_event("c", "m", "orderId")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "c")
        .connect("c", "e")
        .connect("b", "b_end")
        .build()
        .unwrap()
}

/// Copilot r4693719539 (#1949): consumption is confirmed by the instance
/// partition, never assumed at send time. A boundary cancelled before its
/// delayed delivery arrives answers `consumed: false`, the reservation is
/// released, and a later catch in the SAME instance takes the message.
#[test]
fn delivery_to_a_cancelled_boundary_releases_the_message_for_a_later_catch() {
    let (mut rig, order) = Rig::new(boundary_then_catch_process());
    let published = rig.publish_backlog(&order, 1);
    let (instance_key, held) = rig.create_holding("bc", &order, 2_000, &is_delivery);
    assert_eq!(held.len(), 1, "the boundary's delivery is delayed");
    // The task completes first: the boundary closes, the catch opens — and
    // skips the message, which is still reserved for this instance.
    let completed = complete_one(&mut rig.instance, "work");
    for routed in completed.iter().filter_map(routed) {
        rig.run(routed.0, routed.1, 2_100);
    }
    rig.trace.extend(completed);
    assert!(!rig.instance.is_completed(instance_key));
    // The delayed delivery lands on the cancelled boundary: not consumed.
    for (to_message, command) in held {
        rig.run(to_message, command, 2_200);
    }
    assert_eq!(rig.acks()[0], (false, false), "{:?}", rig.trace);
    // Released, re-offered to the catch, consumed there.
    assert_eq!(rig.acks()[1], (false, true), "{:?}", rig.trace);
    assert!(rig.instance.is_completed(instance_key));
    assert_eq!(
        rig.delivered_to(instance_key),
        vec![published[0], published[0]]
    );
    assert!(rig
        .message
        .state()
        .message_subscriptions
        .values()
        .all(|s| s.awaiting_ack.is_none()));
}

fn event_gateway_two_messages() -> ProcessDefinition {
    ProcessBuilder::new("gw")
        .start_event("s")
        .event_based_gateway("g")
        .message_intermediate_catch_event("c1", "m1", "orderId")
        .end_event("e1")
        .message_intermediate_catch_event("c2", "m2", "orderId")
        .end_event("e2")
        .connect("s", "g")
        .connect("g", "c1")
        .connect("g", "c2")
        .connect("c1", "e1")
        .connect("c2", "e2")
        .build()
        .unwrap()
}

/// Copilot r4693719539 (#1949): a LOSING remote event-based-gateway catch
/// (one-shot) is acked too, answers `consumed: false`, and its message is
/// released — never left marked consumed by an instance that did not take it.
#[test]
fn losing_remote_event_gateway_catch_releases_its_message() {
    let (mut rig, order) = Rig::new(event_gateway_two_messages());
    let m1 = rig.publish_named_backlog("m1", &order, 1)[0];
    let m2 = rig.publish_named_backlog("m2", &order, 1)[0];
    let instance_key = rig.create("gw", &order, 2_000);
    assert!(rig.instance.is_completed(instance_key));
    // Both catches got a delivery; the first to land wins the gateway.
    let delivered = rig.delivered_to(instance_key);
    let mut both = delivered.clone();
    both.sort_unstable();
    assert_eq!(both, vec![m1, m2]);
    let (winner, loser) = (delivered[0], delivered[1]);
    assert_eq!(
        rig.acks(),
        vec![(false, true), (false, false)],
        "{:?}",
        rig.trace
    );
    let buffered = &rig.message.state().buffered_messages;
    assert!(buffered[&winner]
        .correlated_instances
        .contains(&instance_key));
    assert!(
        buffered[&loser].correlated_instances.is_empty(),
        "the losing catch's message is released"
    );
}

/// Copilot r4693719423 (#1949): a delivery and an ack lost to exhausted
/// routing retries never leave the subscription stuck. The in-flight delivery
/// is durable state: it survives a restart / leader change (replay and
/// snapshot), the host re-drives it from `pending_buffered_deliveries` with
/// the SAME delivery key, the instance partition answers a duplicate with a
/// re-ack (never a second token), and a duplicate ack is ignored.
#[test]
fn lost_delivery_and_lost_ack_are_redriven_across_a_restart() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 3);
    // The first delivery is lost.
    let (instance_key, lost) = rig.create_holding("ni", &order, 2_000, &is_delivery);
    assert_eq!(lost.len(), 1);
    assert_eq!(redrive(&rig), lost, "the re-drive rebuilds the exact send");

    // Restart / leader change mid-flight: a replica rebuilt from the journal
    // (and one restored from a snapshot) holds the same in-flight delivery.
    let mut replayed = Engine::replay_partition(1, rig.message_journal.clone());
    replayed.set_num_partitions(Rig::N);
    assert_eq!(replayed.state(), rig.message.state());
    assert_eq!(
        replayed.pending_buffered_deliveries(),
        rig.message.pending_buffered_deliveries()
    );
    #[cfg(feature = "serde")]
    {
        let json = serde_json::to_string(&rig.message.snapshot()).unwrap();
        let restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
        assert_eq!(
            restored.pending_buffered_deliveries(),
            rig.message.pending_buffered_deliveries()
        );
    }
    rig.message = replayed;

    // Re-driven, it is delivered — but now its ACK is lost.
    let mut lost_acks = Vec::new();
    for (to_message, command) in redrive(&rig) {
        lost_acks.extend(rig.run_holding(to_message, command, 3_000, &is_ack));
    }
    assert_eq!(lost_acks.len(), 1);
    let tokens = |rig: &Rig| {
        rig.trace
            .iter()
            .filter(|e| matches!(e, Event::MessageCorrelated { .. }))
            .count()
    };
    assert_eq!(tokens(&rig), 1, "the re-driven delivery was applied");
    // Still awaiting: the re-drive sends the same delivery again; the
    // instance partition dedups it (no second token) and only re-acks.
    let again = redrive(&rig);
    assert_eq!(again.len(), 1);
    for (to_message, command) in again {
        rig.run(to_message, command, 3_100);
    }
    // A duplicate of the (lost, now superseded) ack is a no-op.
    for (_, command) in lost_acks {
        assert_only_settles(&rig.on_message(command, 3_200));
    }
    assert!(redrive(&rig).is_empty());
    assert_eq!(tokens(&rig), 3, "each message spawns exactly one token");
    let taken: Vec<Key> = rig
        .trace
        .iter()
        .filter_map(|e| match e {
            Event::MessageCorrelated { message_key, .. } => Some(*message_key),
            _ => None,
        })
        .collect();
    assert_eq!(taken, published, "publish order: {:?}", rig.trace);
    assert_eq!(rig.acks(), vec![(true, true); 3]);
    assert!(rig.instance.state().instances.contains_key(&instance_key));
}

/// #1949: a reserved (in-flight) message outlives its TTL until the ack
/// settles it — the instance may already have consumed it, and a re-drive
/// needs its variables — and then expires on the next sweep.
#[test]
fn a_reserved_message_expires_only_after_its_ack() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 1);
    let deadline = rig.message.state().buffered_messages[&published[0]].deadline;
    let (_, held) = rig.create_holding("ni", &order, 2_000, &is_delivery);
    let swept = rig.on_message(Command::TriggerTimers { now: deadline + 1 }, deadline + 1);
    assert!(!swept
        .iter()
        .any(|e| matches!(e, Event::MessageExpired { .. })));
    assert_eq!(redrive(&rig).len(), 1);
    for (to_message, command) in held {
        rig.run(to_message, command, deadline + 2);
    }
    assert_eq!(rig.acks(), vec![(true, true)]);
    let swept = rig.on_message(Command::TriggerTimers { now: deadline + 3 }, deadline + 3);
    assert!(swept
        .iter()
        .any(|e| matches!(e, Event::MessageExpired { .. })));
    assert!(rig.message.state().buffered_messages.is_empty());
}

/// Copilot r4693719494 (#1949): draining N buffered messages to a remote
/// subscription through N ack commands visits O(N) buffer entries in total —
/// each accepted ack resumes the scan right after the message it settled,
/// instead of rescanning the already-delivered prefix.
#[test]
fn remote_ack_paced_drain_scans_linearly() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    const N: usize = 200;
    let published = rig.publish_backlog(&order, N as u64);
    rig.message.buffer_scan_count.set(0);
    let instance_key = rig.create("ni", &order, 2_000);
    assert_eq!(rig.delivered_to(instance_key), published);
    let visits = rig.message.buffer_scan_count.get();
    assert!(
        visits <= 2 * N + 2,
        "visits must be O(N), got {visits} for N = {N}"
    );
}

/// The `(accepted, consumed, deferred)` answers the instance partition
/// emitted, in order (#1949).
fn receipts(trace: &[Event]) -> Vec<(Key, bool, bool, bool)> {
    trace
        .iter()
        .filter_map(|e| match e {
            Event::MessageCorrelationAcknowledging {
                delivery_key,
                accepted,
                consumed,
                deferred,
                ..
            } => Some((*delivery_key, *accepted, *consumed, *deferred)),
            _ => None,
        })
        .collect()
}

/// Creates a `ni` instance whose subscription open is routed only AFTER the
/// instance is suspended, so the buffered delivery is answered `deferred`.
/// Holds whatever `hold` matches.
fn create_suspended(
    rig: &mut Rig,
    order: &str,
    hold: &dyn Fn(&Command) -> bool,
) -> (Key, Vec<(bool, Command)>) {
    create_suspended_in(rig, "ni", order, hold)
}

/// [`create_suspended`] for an arbitrary deployed `process`.
fn create_suspended_in(
    rig: &mut Rig,
    process: &str,
    order: &str,
    hold: &dyn Fn(&Command) -> bool,
) -> (Key, Vec<(bool, Command)>) {
    let created = rig.on_instance(
        Command::create_instance_with(process, vars(&[("orderId", Value::Str(order.into()))])),
        2_000,
    );
    let instance_key = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        })
        .expect("created");
    let opens: Vec<_> = created.iter().filter_map(routed).collect();
    rig.trace.extend(created);
    rig.run(false, Command::SuspendInstance { instance_key }, 2_001);
    let mut held = Vec::new();
    for (to_message, command) in opens {
        held.extend(rig.run_holding(to_message, command, 2_002, hold));
    }
    (instance_key, held)
}

fn correlated(rig: &Rig, instance_key: Key) -> Vec<Key> {
    rig.trace
        .iter()
        .filter_map(|e| match e {
            Event::MessageCorrelated {
                instance_key: i,
                message_key,
                ..
            } if *i == instance_key => Some(*message_key),
            _ => None,
        })
        .collect()
}

/// #1949 resume handshake: a delivery DEFERRED while the instance was
/// suspended is re-offered when it resumes — without waiting for a new
/// publication — and the handshake survives a restart of the message
/// partition between the deferral and the resume.
#[test]
fn resume_handshake_redelivers_a_deferred_backlog_across_a_restart() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 2);
    let (instance_key, _) = create_suspended(&mut rig, &order, &|_| false);
    assert!(correlated(&rig, instance_key).is_empty());
    assert_eq!(
        receipts(&rig.trace)
            .iter()
            .map(|r| (r.1, r.2, r.3))
            .collect::<Vec<_>>(),
        vec![(false, false, true)],
        "deferred while suspended"
    );
    // Restart (leader change) of the message partition before the resume.
    let mut replayed = Engine::replay_partition(1, rig.message_journal.clone());
    replayed.set_num_partitions(Rig::N);
    assert_eq!(replayed.state(), rig.message.state());
    rig.message = replayed;
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert!(
        rig.trace
            .iter()
            .any(|e| matches!(e, Event::MessageSubscriptionResuming { .. })),
        "the resume announces itself to the message partition"
    );
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
    // A repeated handshake re-offers nothing (idempotent); it only
    // re-confirms, which the instance partition ignores once settled.
    let sub = *rig
        .message
        .state()
        .message_subscriptions
        .keys()
        .next()
        .unwrap();
    let again = rig.on_message(
        Command::ResumeMessageSubscription {
            subscription_key: sub,
            generation: 1,
        },
        3_100,
    );
    assert!(
        again
            .iter()
            .all(|e| matches!(e, Event::MessageSubscriptionResumed { .. })),
        "{again:?}"
    );
}

/// #1949 one-way gate: once ack-paced delivery is enabled, a transient peer
/// outage (the host's cluster-wide capability probe briefly reports `false`)
/// must NOT turn it back off — otherwise a delivery deferred while the
/// instance was suspended would never be re-offered on resume.
#[test]
fn remote_buffered_delivery_gate_is_latched_across_a_peer_outage() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.instance.set_remote_buffered_delivery(true);
    rig.message.set_remote_buffered_delivery(true);
    let published = rig.publish_backlog(&order, 1);
    let (instance_key, _) = create_suspended(&mut rig, &order, &|_| false);
    assert!(correlated(&rig, instance_key).is_empty(), "deferred");
    // A peer becomes unreachable: the probe now reports `false`.
    rig.instance.set_remote_buffered_delivery(false);
    rig.message.set_remote_buffered_delivery(false);
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert!(
        rig.trace
            .iter()
            .any(|e| matches!(e, Event::MessageSubscriptionResuming { .. })),
        "the resume handshake is still emitted during the outage"
    );
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
}

/// #1949 durable receipt + duplicate ack: the deferral's ack is LOST, the
/// instance resumes (its handshake finds the delivery still in flight, a
/// no-op), then the re-driven duplicate delivery is answered with the STORED
/// receipt (`deferred`, never recomputed from the now-resumed state) plus a
/// fresh handshake, so the message is still taken exactly once. A duplicate
/// of the lost ack is a no-op. The instance partition is restarted between
/// the lost ack and the duplicate: the receipt is durable.
#[test]
fn duplicate_delivery_is_answered_with_its_stored_receipt_across_a_restart() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 1);
    let (instance_key, lost_acks) = create_suspended(&mut rig, &order, &is_ack);
    assert_eq!(lost_acks.len(), 1);
    let first = receipts(&rig.trace);
    assert_eq!(first.len(), 1);
    assert_ne!(first[0].0, 0, "the receipt carries its delivery key");
    assert!(first[0].3, "deferred");
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert!(correlated(&rig, instance_key).is_empty(), "still in flight");

    // Restart of the instance partition: the receipt is rebuilt from the log.
    let mut replayed = Engine::replay_partition(0, rig.instance_journal.clone());
    replayed.set_num_partitions(Rig::N);
    assert_eq!(replayed.state(), rig.instance.state());
    assert_eq!(
        replayed
            .state()
            .routed_deliveries
            .receipt(first[0].0)
            .map(|r| (r.accepted, r.consumed, r.deferred)),
        Some((false, false, true))
    );
    rig.instance = replayed;

    let before = rig.trace.len();
    for (to_message, command) in redrive(&rig) {
        rig.run(to_message, command, 3_100);
    }
    let answered = receipts(&rig.trace[before..]);
    assert_eq!(answered[0], first[0], "the stored receipt, verbatim");
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
    assert_eq!(correlated(&rig, instance_key).len(), 1, "exactly once");
    // The lost original ack, arriving late, changes nothing.
    for (_, command) in lost_acks {
        assert_only_settles(&rig.on_message(command, 3_200));
    }
    assert!(redrive(&rig).is_empty());
}

/// #1949 mixed-version cluster: with remote buffered delivery gated OFF (a
/// member lacks `message-correlation-ack-v1`), a resume emits no handshake —
/// a legacy message partition would not know the command — and the routed
/// open takes nothing from the buffer (the 1944 behaviour).
#[test]
fn resume_handshake_is_gated_off_in_a_mixed_version_cluster() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.instance.set_remote_buffered_delivery(false);
    rig.message.set_remote_buffered_delivery(false);
    rig.publish_backlog(&order, 1);
    let (instance_key, _) = create_suspended(&mut rig, &order, &|_| false);
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert!(!rig.trace.iter().any(|e| matches!(
        e,
        Event::MessageSubscriptionResuming { .. }
            | Event::RemoteMessageCorrelation { .. }
            | Event::MessageCorrelationAcknowledging { .. }
    )));
    assert_eq!(rig.message.state().buffered_messages.len(), 1);
}

/// #1949 (Copilot r4702895398): the SOURCE partition enabled paced delivery
/// but the destination never applied an enabling envelope. The destination
/// answers (defers) a paced delivery, so it has used the capability: its
/// resume gate must honour that durably (live and after replay), or the
/// resume handshake is suppressed and the backlog strands.
#[test]
fn a_never_enabled_destination_that_deferred_still_resumes() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.instance.set_remote_buffered_delivery(false);
    rig.message.set_remote_buffered_delivery(true);
    rig.publish_backlog(&order, 1);
    let (instance_key, _) = create_suspended(&mut rig, &order, &|_| false);
    assert!(rig.trace.iter().any(|e| matches!(
        e,
        Event::MessageCorrelationAcknowledging { deferred: true, .. }
    )));
    assert!(
        rig.instance.message_correlation_ack_required(),
        "answering a paced delivery is capability usage"
    );
    let mut replayed = state::State::default();
    for e in &rig.instance_journal {
        state::apply(&mut replayed, e);
    }
    assert!(
        replayed.message_correlation_ack_used,
        "durable across replay"
    );
    let before = rig.trace.len();
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert!(rig.trace[before..]
        .iter()
        .any(|e| matches!(e, Event::MessageSubscriptionResuming { .. })));
}

/// #1949 reservation release on subscription close: the instance is
/// cancelled while a delivery is in flight; the late delivery is answered
/// unconsumed, the reservation is released, and the message is never
/// stranded — a later instance takes it.
#[test]
fn reservation_is_released_when_the_subscription_closes_mid_flight() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 1);
    let (first, held) = rig.create_holding("ni", &order, 2_000, &is_delivery);
    assert_eq!(held.len(), 1);
    assert!(rig.message.state().buffered_messages[&published[0]]
        .correlated_instances
        .contains(&first));
    rig.run(
        false,
        Command::CancelInstance {
            instance_key: first,
            terminate_suspended: false,
        },
        2_100,
    );
    for (to_message, command) in held {
        rig.run(to_message, command, 2_200);
    }
    assert!(correlated(&rig, first).is_empty());
    assert!(
        rig.message.state().buffered_messages[&published[0]]
            .correlated_instances
            .is_empty(),
        "released: {:?}",
        rig.trace
    );
    assert!(redrive(&rig).is_empty(), "nothing left in flight");
    let second = rig.create("ni", &order, 3_000);
    assert_eq!(correlated(&rig, second), published);
}

fn two_non_interrupting_esps() -> ProcessDefinition {
    use crate::model::EventSubProcessTrigger as Trig;
    let trig = || Trig::Message {
        message_name: "m".into(),
        correlation_key: "orderId".into(),
    };
    ProcessBuilder::new("two")
        .start_event("s")
        .service_task("work", "work")
        .end_event("e")
        .connect("s", "work")
        .connect("work", "e")
        .event_sub_process("esp1", "s1", trig(), false)
        .start_event("s1")
        .end_event("e1")
        .connect("s1", "e1")
        .contained_in("s1", "esp1")
        .contained_in("e1", "esp1")
        .event_sub_process("esp2", "s2", trig(), false)
        .start_event("s2")
        .end_event("e2")
        .connect("s2", "e2")
        .contained_in("s2", "esp2")
        .contained_in("e2", "esp2")
        .build()
        .unwrap()
}

/// Copilot e8o (#1949) reservation release floor: A reserves M1, B (same
/// instance, name and key) skips it and reserves M2. A's delivery is
/// answered unconsumed (deferred while suspended), releasing M1 while B
/// still awaits its ack. When B's consumed ack arrives, its scan must not
/// resume past M1: B is offered M1 next (oldest first), not M3 — and every
/// message is still taken exactly once by the instance.
#[test]
fn a_release_during_a_sibling_ack_is_not_skipped_by_its_cursor() {
    let (mut rig, order) = Rig::new(two_non_interrupting_esps());
    let published = rig.publish_backlog(&order, 3);
    let (instance_key, held) = rig.create_holding("two", &order, 2_000, &is_delivery);
    assert_eq!(held.len(), 2, "one delivery per subscription");
    let b_sub = match &held[1].1 {
        Command::CorrelateMessageSubscription {
            subscription_key, ..
        } => *subscription_key,
        other => panic!("{other:?}"),
    };
    rig.run(false, Command::SuspendInstance { instance_key }, 2_001);
    let mut held = held.into_iter();
    let (a_to, a) = held.next().unwrap();
    let (b_to, b) = held.next().unwrap();
    // A's delivery is deferred (unconsumed): M1 released while B awaits.
    let mut later = rig.run_holding(a_to, a, 2_002, &is_delivery);
    assert!(rig
        .message
        .state()
        .message_subscriptions
        .values()
        .any(|s| s.rescan_from.is_some()));
    // Resume, holding its handshake so A does not retake M1 first.
    later.extend(rig.run_holding(
        false,
        Command::ResumeInstance { instance_key },
        2_003,
        &|c| matches!(c, Command::ResumeMessageSubscription { .. }),
    ));
    // B consumes M2; its next offer is the released M1.
    let before = rig.trace.len();
    rig.run(b_to, b, 2_004);
    let next_for_b = rig.trace[before..].iter().find_map(|e| match e {
        Event::RemoteMessageCorrelation {
            subscription_key,
            message_key,
            ..
        } if *subscription_key == b_sub => Some(*message_key),
        _ => None,
    });
    assert_eq!(next_for_b, Some(published[0]), "{:?}", rig.trace);
    for (to, command) in later {
        rig.run(to, command, 2_005);
    }
    let mut taken = correlated(&rig, instance_key);
    taken.sort_unstable();
    assert_eq!(taken, published, "each message exactly once");
}

/// #1949 (Copilot r4698796509): the one-way delivery latch is snapshotted
/// state, so a replica restored from a snapshot taken after the enabling
/// envelope applies a later DISABLED envelope exactly like the replica that
/// applied the enabling one live.
#[cfg(feature = "serde")]
#[test]
fn remote_buffered_delivery_latch_survives_snapshot_restore() {
    let mut live = Engine::with_partition(1);
    live.set_num_partitions(Rig::N);
    live.set_remote_buffered_delivery(true);
    // The enabling envelope rides a command; applying it records the latch.
    live.apply_command_at(Command::TriggerTimers { now: 1 }, 1)
        .unwrap();
    assert!(!live.message_correlation_ack_ever_used());
    let json = serde_json::to_string(&live.snapshot()).unwrap();
    let mut restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
    restored.set_num_partitions(Rig::N);
    live.set_remote_buffered_delivery(false);
    restored.set_remote_buffered_delivery(false);
    assert!(live.remote_buffered_delivery);
    assert_eq!(
        restored.remote_buffered_delivery,
        live.remote_buffered_delivery
    );
}

/// #1949 (Copilot r4698796595): a delayed duplicate ack of an earlier
/// (deferred) delivery of the SAME message must not settle the current,
/// re-reserved delivery of it — acks match on the delivery key too.
#[test]
fn a_stale_ack_of_an_earlier_delivery_of_the_same_message_is_ignored() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 1);
    let (instance_key, held_acks) = create_suspended(&mut rig, &order, &is_ack);
    assert_eq!(held_acks.len(), 1);
    let (_, stale_ack) = held_acks[0].clone();
    // The deferral's ack lands; a duplicate copy of it is still in transit.
    rig.run(true, stale_ack.clone(), 2_500);
    // Resume re-reserves the same message as a NEW delivery, held in flight.
    let held = rig.run_holding(
        false,
        Command::ResumeInstance { instance_key },
        3_000,
        &is_delivery,
    );
    assert_eq!(held.len(), 1, "{:?}", rig.trace);
    let current = rig
        .message
        .state()
        .message_subscriptions
        .values()
        .find_map(|s| s.awaiting_ack)
        .expect("the resumed delivery is in flight");
    assert_eq!(current.message_key, published[0]);
    // The delayed duplicate of the OLD delivery's ack arrives: a no-op.
    assert_only_settles(&rig.on_message(stale_ack, 3_100));
    assert_eq!(
        rig.message
            .state()
            .message_subscriptions
            .values()
            .find_map(|s| s.awaiting_ack),
        Some(current),
        "the current reservation is still awaited"
    );
    for (to_message, command) in held {
        rig.run(to_message, command, 3_200);
    }
    assert_eq!(correlated(&rig, instance_key), published);
    assert!(redrive(&rig).is_empty());
}

/// #1949 (Copilot r4698796640): two persistent subscriptions of one
/// SUSPENDED instance and one buffered message: a deferral must not requeue
/// the sibling (it would bounce off the same suspension, ping-ponging
/// deliveries without progress). After resume the message is taken once.
#[test]
fn a_deferral_does_not_requeue_a_sibling_of_a_suspended_instance() {
    let (mut rig, order) = Rig::new(two_non_interrupting_esps());
    let published = rig.publish_backlog(&order, 1);
    // Both subscriptions are open; A's delivery is in flight (B skipped the
    // reserved message) when the instance is suspended.
    let (instance_key, held) = rig.create_holding("two", &order, 2_000, &is_delivery);
    assert_eq!(held.len(), 1);
    rig.run(false, Command::SuspendInstance { instance_key }, 2_001);
    let before = rig.trace.len();
    for (to_message, command) in held {
        rig.run(to_message, command, 2_002);
    }
    let deliveries = rig.trace[before..]
        .iter()
        .filter(|e| matches!(e, Event::RemoteMessageCorrelation { .. }))
        .count();
    assert_eq!(deliveries, 0, "no sibling ping-pong: {:?}", rig.trace);
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
}

/// #1949 (Copilot r4698796783): a remote subscription that opened while the
/// capability gate was CLOSED gets its buffered backlog as soon as the gate
/// opens — on the next applied command — without reopening it or publishing
/// another message.
#[test]
fn opening_the_gate_drains_subscriptions_opened_while_it_was_closed() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.instance.set_remote_buffered_delivery(false);
    rig.message.set_remote_buffered_delivery(false);
    let published = rig.publish_backlog(&order, 2);
    let instance_key = rig.create("ni", &order, 2_000);
    assert!(rig.delivered_to(instance_key).is_empty(), "gated off");
    rig.instance.set_remote_buffered_delivery(true);
    rig.message.set_remote_buffered_delivery(true);
    rig.run(true, Command::TriggerTimers { now: 2_500 }, 2_500);
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
}

/// #1949 (Copilot r4700605219): a follower that installs a PRE-activation
/// snapshot (an open remote subscription + its buffered backlog, gate never
/// enabled) must scan that backlog on the first enabling envelope exactly like
/// the live replica. The scan edge is derived from the durable latch, not
/// from the in-memory gate flag a restore defaults.
#[cfg(feature = "serde")]
#[test]
fn pre_activation_snapshot_scans_the_backlog_like_the_live_replica() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.instance.set_remote_buffered_delivery(false);
    rig.message.set_remote_buffered_delivery(false);
    rig.publish_backlog(&order, 2);
    let instance_key = rig.create("ni", &order, 2_000);
    assert!(rig.delivered_to(instance_key).is_empty(), "gated off");
    let json = serde_json::to_string(&rig.message.snapshot()).unwrap();
    let mut restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
    restored.set_num_partitions(Rig::N);
    rig.message.set_remote_buffered_delivery(true);
    restored.set_remote_buffered_delivery(true);
    let command = Command::TriggerTimers { now: 2_500 };
    let live = rig
        .message
        .apply_command_at(command.clone(), 2_500)
        .unwrap();
    let follower = restored.apply_command_at(command, 2_500).unwrap();
    assert!(
        live.iter()
            .any(|e| matches!(e, Event::RemoteMessageCorrelation { .. })),
        "the live replica delivers the backlog: {live:?}"
    );
    assert_eq!(
        follower, live,
        "the follower diverged from the live replica"
    );
    assert_eq!(restored.state(), rig.message.state());
}

/// #1949 (Copilot r4698796872): the resume handshake is lost (routing
/// exhausted, or the node stopped after `ResumeInstance` committed). The
/// instance partition still owes it — rebuilt from durable state across a
/// restart — and the re-driven handshake delivers the deferred backlog; once
/// a delivery is answered without deferral nothing is owed any more.
#[test]
fn a_lost_resume_handshake_is_redriven_from_durable_state() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 1);
    let (instance_key, _) = create_suspended(&mut rig, &order, &|_| false);
    assert!(
        rig.instance.pending_resume_handshakes().is_empty(),
        "nothing owed while suspended"
    );
    let lost = rig.run_holding(
        false,
        Command::ResumeInstance { instance_key },
        3_000,
        &|c| matches!(c, Command::ResumeMessageSubscription { .. }),
    );
    assert_eq!(lost.len(), 1);
    assert!(correlated(&rig, instance_key).is_empty());
    let mut replayed = Engine::replay_partition(0, rig.instance_journal.clone());
    replayed.set_num_partitions(Rig::N);
    rig.instance = replayed;
    let owed: Vec<_> = rig
        .instance
        .pending_resume_handshakes()
        .iter()
        .filter_map(routed)
        .collect();
    assert_eq!(owed, lost, "re-derived verbatim");
    for (to_message, command) in owed {
        rig.run(to_message, command, 3_100);
    }
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
    assert!(rig.instance.pending_resume_handshakes().is_empty());
}

/// #1949 (adversarial review): the deferred message EXPIRES while the
/// instance is suspended, so the resume finds an empty backlog and no later
/// delivery ever clears `resume_pending`. The message partition must confirm
/// the handshake itself, or the host re-drives it forever.
#[test]
fn a_resume_with_an_empty_backlog_is_confirmed_and_not_redriven() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 1);
    let deadline = rig.message.state().buffered_messages[&published[0]].deadline;
    let (instance_key, held) = create_suspended(&mut rig, &order, &is_ack);
    for (to_message, command) in held {
        rig.run(to_message, command, 2_500);
    }
    let swept = rig.on_message(Command::TriggerTimers { now: deadline + 1 }, deadline + 1);
    assert!(swept
        .iter()
        .any(|e| matches!(e, Event::MessageExpired { .. })));
    rig.trace.extend(swept);
    rig.run(
        false,
        Command::ResumeInstance { instance_key },
        deadline + 2,
    );
    assert!(correlated(&rig, instance_key).is_empty());
    assert!(
        rig.instance.pending_resume_handshakes().is_empty(),
        "the confirmed handshake is not owed any more: {:?}",
        rig.trace
    );
    // Durable: a replayed instance partition owes nothing either.
    let replayed = Engine::replay_partition(0, rig.instance_journal.clone());
    assert!(!replayed
        .state()
        .message_subscriptions
        .values()
        .any(|s| s.resume_pending));
    // A duplicate confirmation is a no-op.
    let sub = *rig
        .instance
        .state()
        .message_subscriptions
        .keys()
        .next()
        .unwrap();
    assert!(rig
        .on_instance(
            Command::ConfirmMessageSubscriptionResume {
                subscription_key: sub,
                generation: rig.instance.state().message_subscriptions[&sub].resume_generation,
            },
            deadline + 3
        )
        .is_empty());
}

/// The instance partition's `Opening` record for the rig's single remote
/// subscription.
fn opening_record(rig: &Rig) -> state::MessageSubscription {
    rig.instance
        .state()
        .message_subscriptions
        .values()
        .find(|s| s.state == state::MessageSubscriptionState::Opening)
        .cloned()
        .expect("an Opening record")
}

/// #1949 (Copilot r4699223004): the activation latch is recorded in the
/// EVENT journal, so a replica recovered by journal replay (no snapshot) —
/// with the enabling envelope already applied and before any paced send —
/// still ignores a later disabled envelope, like the live replica.
#[test]
fn activation_latch_survives_journal_replay_before_any_paced_send() {
    let (mut rig, _order) = Rig::new(non_interrupting_esp_process());
    rig.message.set_remote_buffered_delivery(true);
    rig.on_message(Command::TriggerTimers { now: 1_000 }, 1_000);
    assert!(rig.message_journal.iter().any(
        |e| matches!(e, Event::RemoteBufferedDeliveryActivated { partition: 1 }
            if e.owning_partition() == 1)
    ));
    // Recorded once only.
    let again = rig.on_message(Command::TriggerTimers { now: 1_001 }, 1_001);
    assert!(!again
        .iter()
        .any(|e| matches!(e, Event::RemoteBufferedDeliveryActivated { .. })));
    assert!(
        !rig.message.message_correlation_ack_ever_used(),
        "no paced send"
    );
    let mut replayed = Engine::replay_partition(1, rig.message_journal.clone());
    replayed.set_num_partitions(Rig::N);
    replayed.set_remote_buffered_delivery(false);
    assert!(
        replayed.message_correlation_ack_required(),
        "the replayed replica keeps the latch (and requires the capability)"
    );
    rig.message.set_remote_buffered_delivery(false);
    assert!(rig.message.message_correlation_ack_required());
    assert_eq!(replayed.state(), rig.message.state());
}

/// #1949 (Copilot r4699223106): a recorded latch alone (no paced send yet)
/// requires `message-correlation-ack-v1`; a partition never enabled does not.
#[test]
fn a_recorded_latch_requires_the_capability() {
    let (mut rig, _order) = Rig::new(non_interrupting_esp_process());
    rig.message.set_remote_buffered_delivery(false);
    rig.on_message(Command::TriggerTimers { now: 1_000 }, 1_000);
    assert!(!rig.message.message_correlation_ack_required());
    rig.message.set_remote_buffered_delivery(true);
    rig.on_message(Command::TriggerTimers { now: 1_001 }, 1_001);
    rig.message.set_remote_buffered_delivery(false);
    assert!(rig.message.message_correlation_ack_required());
}

/// #1949 (Copilot r4699223177): a resume handshake that arrives while the
/// ORIGINAL deferred delivery still awaits its ack must NOT confirm — the
/// delayed ack clears `awaiting_ack` without re-offering anything, so the
/// instance partition has to keep owing (and re-driving) the handshake.
#[test]
fn a_resume_while_the_deferred_ack_is_in_flight_is_not_confirmed() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 1);
    let (instance_key, held_acks) = create_suspended(&mut rig, &order, &is_ack);
    assert_eq!(held_acks.len(), 1, "the deferral's ack is delayed");
    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert!(
        !rig.trace
            .iter()
            .any(|e| matches!(e, Event::MessageSubscriptionResumed { .. })),
        "no confirmation while the ack is in flight: {:?}",
        rig.trace
    );
    // The delayed original ack lands: nothing re-offered yet, still owed.
    for (to_message, command) in held_acks {
        rig.run(to_message, command, 3_100);
    }
    let owed: Vec<_> = rig
        .instance
        .pending_resume_handshakes()
        .iter()
        .filter_map(routed)
        .collect();
    assert_eq!(owed.len(), 1, "still owed: {:?}", rig.trace);
    for (to_message, command) in owed {
        rig.run(to_message, command, 3_200);
    }
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
    assert!(rig.instance.pending_resume_handshakes().is_empty());
}

/// #1949 (Copilot r4699223241): only a confirmation of the CURRENT resume
/// generation clears the obligation; a delayed confirmation of an older
/// resume (a stale generation) is a no-op.
#[test]
fn a_stale_generation_confirmation_does_not_clear_a_newer_resume() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.publish_backlog(&order, 1);
    let (instance_key, _) = create_suspended(&mut rig, &order, &|_| false);
    let record = opening_record(&rig);
    assert!(record.resume_pending);
    let current = record.resume_generation;
    assert!(current > 0, "a deferral bumps the generation");
    rig.on_instance(Command::ResumeInstance { instance_key }, 3_000);
    let stale = rig.on_instance(
        Command::ConfirmMessageSubscriptionResume {
            subscription_key: record.key,
            generation: current - 1,
        },
        3_001,
    );
    assert!(stale.is_empty(), "{stale:?}");
    assert!(opening_record(&rig).resume_pending, "still owed");
    let fresh = rig.on_instance(
        Command::ConfirmMessageSubscriptionResume {
            subscription_key: record.key,
            generation: current,
        },
        3_002,
    );
    assert!(fresh
        .iter()
        .any(|e| matches!(e, Event::MessageSubscriptionResumeConfirmed { .. })));
    assert!(!opening_record(&rig).resume_pending);
}

/// #1949 (Copilot r4699223301): an instance that still owes a resume
/// handshake is never cold-spilled, so the resident re-drive scan keeps
/// seeing the obligation; once confirmed it may spill again.
#[test]
fn an_instance_owing_a_resume_handshake_is_not_cold_spilled() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.publish_backlog(&order, 1);
    let (instance_key, _) = create_suspended(&mut rig, &order, &|_| false);
    let lost = rig.run_holding(
        false,
        Command::ResumeInstance { instance_key },
        3_000,
        &|c| matches!(c, Command::ResumeMessageSubscription { .. }),
    );
    assert_eq!(lost.len(), 1);
    assert!(rig.instance.snapshot_instance(instance_key).is_none());
    assert_eq!(rig.instance.pending_resume_handshakes().len(), 1);
}

/// Floods `rig`'s instance partition ledger with `n` unrelated routed
/// deliveries (closes of unknown subscriptions, each recording a tombstone).
fn flood_instance_ledger(rig: &mut Rig, n: u64) {
    for i in 0..n {
        let k = state::compose_key(1, 900_000 + i);
        rig.on_instance(
            Command::CloseMessageSubscription {
                subscription_key: k,
                instance_key: k,
                element_instance_key: k,
                element_id: "x".into(),
                delivery_key: k,
            },
            2_500,
        );
    }
}

/// #1949 D1 (operator review): a receipt whose ack is LOST must survive any
/// number of other routed deliveries — it is pinned outside the ledger cap
/// until the message partition settles it. Otherwise the re-driven delivery
/// of a completed one-shot instance is answered from current state
/// (`consumed: false`), the reservation is released and the message can
/// correlate a second time. Also across a snapshot restore.
#[test]
fn an_unsettled_receipt_outlives_ledger_eviction_until_settled() {
    const CAP: usize = 4;
    let (mut rig, order) = Rig::new(process_with_message_catch());
    rig.instance.state_mut().routed_deliveries.set_cap(CAP);
    let published = rig.publish_named_backlog("payment-received", &order, 1);
    let (instance_key, lost_acks) = rig.create_holding("await-payment", &order, 2_000, &is_ack);
    assert_eq!(lost_acks.len(), 1, "the ack is lost");
    assert!(rig.instance.is_completed(instance_key));
    let delivery_key = receipts(&rig.trace)[0].0;

    flood_instance_ledger(&mut rig, 3 * CAP as u64);
    let ledger = &rig.instance.state().routed_deliveries;
    assert!(ledger.contains(delivery_key), "pinned, never evicted");
    assert_eq!(ledger.len(), CAP + 1, "settled entries stay bounded");
    assert_eq!(
        rig.instance.pending_unsettled_receipts().len(),
        1,
        "the unsettled answer is re-drivable"
    );

    #[cfg(feature = "serde")]
    {
        let json = serde_json::to_string(&rig.instance.snapshot()).unwrap();
        let mut restored = Engine::from_snapshot(serde_json::from_str(&json).unwrap());
        restored.set_num_partitions(Rig::N);
        assert_eq!(restored.state(), rig.instance.state());
        restored.state_mut().routed_deliveries.set_cap(CAP);
        assert!(restored
            .state()
            .routed_deliveries
            .receipt(delivery_key)
            .is_some());
        rig.instance = restored;
    }

    // The message partition re-drives the delivery: answered with the
    // ORIGINAL receipt, so the reservation is kept.
    let before = rig.trace.len();
    for (to_message, command) in redrive(&rig) {
        rig.run(to_message, command, 3_000);
    }
    assert_eq!(
        receipts(&rig.trace[before..]),
        receipts(&rig.trace[..before])
    );
    assert_eq!(rig.acks(), vec![(false, true)], "{:?}", rig.trace);
    assert!(rig.message.state().buffered_messages[&published[0]]
        .correlated_instances
        .contains(&instance_key));
    assert!(redrive(&rig).is_empty());
    // The settlement round-tripped: the receipt is unpinned and bounded again.
    let ledger = &rig.instance.state().routed_deliveries;
    assert!(ledger.receipt(delivery_key).is_none());
    assert!(ledger.len() <= CAP);
    assert!(rig.instance.pending_unsettled_receipts().is_empty());
    // The lost original ack, arriving late, only re-confirms the settlement.
    for (_, command) in lost_acks {
        assert_only_settles(&rig.on_message(command, 3_100));
    }
    assert_eq!(rig.acks().len(), 1);
}

/// #1949 D1: a lost SETTLEMENT never pins a receipt forever: the instance
/// partition re-drives its answer, which the message partition (no longer
/// awaiting it) answers with another settlement.
#[test]
fn a_lost_settlement_is_redriven_from_the_pinned_receipt() {
    let (mut rig, order) = Rig::new(process_with_message_catch());
    rig.publish_named_backlog("payment-received", &order, 1);
    let is_settle = |c: &Command| matches!(c, Command::SettleMessageCorrelationReceipt { .. });
    let (_, lost) = rig.create_holding("await-payment", &order, 2_000, &is_settle);
    assert_eq!(lost.len(), 1, "the settlement is lost");
    assert_eq!(rig.acks(), vec![(false, true)]);
    let pending = rig.instance.pending_unsettled_receipts();
    assert_eq!(pending.len(), 1);
    for (to_message, command) in pending.iter().filter_map(routed) {
        rig.run(to_message, command, 3_000);
    }
    assert!(rig.instance.pending_unsettled_receipts().is_empty());
    assert_eq!(rig.acks().len(), 1, "the re-driven answer applied nothing");
    // A late duplicate settlement is a no-op.
    for (to_message, command) in lost {
        rig.run(to_message, command, 3_100);
    }
    assert!(rig.instance.pending_unsettled_receipts().is_empty());
}

/// #1949 D2 (operator review): a subscription awaiting the ack of a reserved
/// message that is GONE (removed by any path) must not stall: the re-drive
/// releases the reservation with an explicit self-routed ack, and the
/// subscription is offered its remaining backlog.
#[test]
fn an_orphaned_reservation_is_released_not_stalled() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    let published = rig.publish_backlog(&order, 2);
    let (instance_key, held) = rig.create_holding("ni", &order, 2_000, &is_delivery);
    assert_eq!(held.len(), 1);
    // The reserved message disappears (simulates any removal path).
    rig.message
        .state_mut()
        .remove_buffered_message(published[0]);
    let release = redrive(&rig);
    assert_eq!(release.len(), 1);
    assert!(
        matches!(
            release[0].1,
            Command::AcknowledgeMessageCorrelation {
                consumed: false,
                ..
            }
        ),
        "{release:?}"
    );
    for (to_message, command) in release {
        rig.run(to_message, command, 3_000);
    }
    assert_eq!(
        correlated(&rig, instance_key),
        published[1..],
        "the rest of the backlog flows: {:?}",
        rig.trace
    );
    assert!(redrive(&rig).is_empty(), "nothing left awaiting");
}

/// #1949 D2: an orphaned ONE-SHOT reservation re-opens the subscription, so
/// a later message still reaches the waiting catch.
#[test]
fn an_orphaned_one_shot_reservation_reopens_the_subscription() {
    let (mut rig, order) = Rig::new(process_with_message_catch());
    let published = rig.publish_named_backlog("payment-received", &order, 1);
    let (instance_key, held) = rig.create_holding("await-payment", &order, 2_000, &is_delivery);
    assert_eq!(held.len(), 1);
    rig.message
        .state_mut()
        .remove_buffered_message(published[0]);
    for (to_message, command) in redrive(&rig) {
        rig.run(to_message, command, 3_000);
    }
    assert!(redrive(&rig).is_empty());
    // A later publication reaches the re-opened subscription.
    rig.run(
        true,
        publish_with("payment-received", &order, BTreeMap::new(), None),
        3_100,
    );
    assert!(rig.instance.is_completed(instance_key), "{:?}", rig.trace);
    assert!(redrive(&rig).is_empty());
}

/// #1949 (Copilot r4701729161): only a delivery's FIRST acknowledgement moves
/// the subscription's resume state. A re-emitted duplicate — the original
/// receipt replayed, or an answer recomputed after the receipt settled — must
/// neither clear a pending resume, bump its generation, nor re-pin a receipt.
#[test]
fn a_duplicate_acknowledgement_does_not_rewrite_resume_state() {
    let (mut rig, order) = Rig::new(non_interrupting_esp_process());
    rig.publish_backlog(&order, 1);
    let _ = create_suspended(&mut rig, &order, &|_| false);
    let record = opening_record(&rig);
    assert!(record.resume_pending);
    let generation = record.resume_generation;
    let original = rig
        .trace
        .iter()
        .find(|e| {
            matches!(e, Event::MessageCorrelationAcknowledging { deferred: true, delivery_key, .. } if *delivery_key != 0)
        })
        .cloned()
        .expect("a deferred receipt");
    let Event::MessageCorrelationAcknowledging {
        subscription_key,
        delivery_key,
        ..
    } = original.clone()
    else {
        unreachable!()
    };
    let resume_state = |s: &state::State| {
        let r = &s.message_subscriptions[&subscription_key];
        (r.resume_pending, r.resume_generation)
    };

    // Variant 1: the original deferred receipt re-emitted while pinned.
    let mut st = rig.instance.state().clone();
    state::apply(&mut st, &original);
    assert_eq!(resume_state(&st), (true, generation), "replayed receipt");

    // Variant 2: a non-deferred answer recomputed for a duplicate (pinned).
    let mut recomputed = original.clone();
    if let Event::MessageCorrelationAcknowledging { deferred, .. } = &mut recomputed {
        *deferred = false;
    }
    let mut st = rig.instance.state().clone();
    state::apply(&mut st, &recomputed);
    assert_eq!(resume_state(&st), (true, generation), "recomputed, pinned");

    // Variant 3: the same recomputed duplicate AFTER the receipt settled.
    let mut st = rig.instance.state().clone();
    state::apply(
        &mut st,
        &Event::MessageCorrelationReceiptSettled {
            subscription_key,
            delivery_key,
        },
    );
    assert!(st.routed_deliveries.receipt(delivery_key).is_none());
    state::apply(&mut st, &recomputed);
    assert_eq!(resume_state(&st), (true, generation), "recomputed, settled");
    assert!(
        st.routed_deliveries.receipt(delivery_key).is_none(),
        "a settled receipt is never re-pinned"
    );

    // Variant 4: settle → evict → newer deferral → lost resume → late
    // duplicate. The evicted duplicate re-records as a fresh ledger entry,
    // but it answers an OLDER delivery, so the newer obligation stands.
    let mut st = rig.instance.state().clone();
    st.routed_deliveries.set_cap(2);
    state::apply(
        &mut st,
        &Event::MessageCorrelationReceiptSettled {
            subscription_key,
            delivery_key,
        },
    );
    let record = |st: &mut state::State, key: Key| {
        state::apply(
            st,
            &Event::RoutedDeliveryRecorded {
                delivery_key: key,
                recipient_partition: 0,
                subscription_key: Some(subscription_key),
                close_tombstone: false,
            },
        );
    };
    for i in 0..4 {
        record(&mut st, state::compose_key(1, 900_000 + i));
    }
    assert!(!st.routed_deliveries.contains(delivery_key), "evicted");
    let newer = state::compose_key(1, 950_000);
    assert!(newer > delivery_key);
    record(&mut st, newer);
    let mut newer_deferral = original.clone();
    if let Event::MessageCorrelationAcknowledging { delivery_key, .. } = &mut newer_deferral {
        *delivery_key = newer;
    }
    state::apply(&mut st, &newer_deferral);
    assert_eq!(resume_state(&st), (true, generation + 1), "newer deferral");
    // The resume handshake is lost; the late duplicate lands.
    record(&mut st, delivery_key);
    state::apply(&mut st, &recomputed);
    assert_eq!(
        resume_state(&st),
        (true, generation + 1),
        "late duplicate after eviction"
    );
}

/// The rig's canonical (message-partition) record for its single remote
/// subscription.
fn canonical_record(rig: &Rig) -> state::MessageSubscription {
    rig.message
        .state()
        .message_subscriptions
        .values()
        .next()
        .cloned()
        .expect("a canonical record")
}

/// #1949 (Copilot r4702442021): a suspended remote ONE-SHOT catch defers a
/// buffered delivery, which re-opens its canonical record. A live publish
/// before the resume must not settle it through an unpaced delivery (the
/// suspended instance drops it, stranding the backlog): the live message
/// waits, and the resume delivers the buffered one exactly once.
#[test]
fn a_live_publish_never_settles_a_deferred_one_shot_before_resume() {
    let (mut rig, order) = Rig::new(process_with_message_catch());
    let published = rig.publish_named_backlog("payment-received", &order, 1);
    let (instance_key, _) = create_suspended_in(&mut rig, "await-payment", &order, &|_| false);
    let canonical = canonical_record(&rig);
    assert_eq!(canonical.state, state::MessageSubscriptionState::Open);
    assert!(canonical.deferred, "awaits the resume handshake");

    let before = rig.trace.len();
    rig.run(
        true,
        publish_with("payment-received", &order, BTreeMap::new(), None),
        2_500,
    );
    let live = rig.trace[before..]
        .iter()
        .find_map(|e| match e {
            Event::MessageBuffered { message_key, .. } => Some(*message_key),
            _ => None,
        })
        .expect("the live message waits, buffered");
    assert!(
        !rig.trace[before..]
            .iter()
            .any(|e| matches!(e, Event::RemoteMessageCorrelation { .. })),
        "{:?}",
        &rig.trace[before..]
    );
    assert_eq!(
        canonical_record(&rig).state,
        state::MessageSubscriptionState::Open
    );
    // Durable: a replayed message partition holds the same deferral.
    let replayed = Engine::replay_partition(1, rig.message_journal.clone());
    assert_eq!(replayed.state(), rig.message.state());

    rig.run(false, Command::ResumeInstance { instance_key }, 3_000);
    assert_eq!(correlated(&rig, instance_key), published, "{:?}", rig.trace);
    assert!(rig.instance.is_completed(instance_key));
    assert!(!canonical_record(&rig).deferred);
    assert!(
        rig.message.state().buffered_messages[&live]
            .correlated_instances
            .is_empty(),
        "the live message still waits"
    );
    assert!(redrive(&rig).is_empty());
}

/// #1949 (Copilot r4702442076): the suspended one-shot catch is CANCELLED
/// after its deferred answer but before that ack reaches the message
/// partition. The close cancels the reserved record; the late ack settles
/// the delivery and releases the message without re-opening a phantom wait,
/// so a later instance takes it.
#[test]
fn a_close_before_the_deferred_ack_leaves_no_phantom_wait() {
    let (mut rig, order) = Rig::new(process_with_message_catch());
    let published = rig.publish_named_backlog("payment-received", &order, 1);
    let (first, held_acks) = create_suspended_in(&mut rig, "await-payment", &order, &is_ack);
    assert_eq!(held_acks.len(), 1, "the deferral's ack is delayed");
    assert_eq!(
        canonical_record(&rig).state,
        state::MessageSubscriptionState::Correlated,
        "reserved"
    );
    rig.run(
        false,
        Command::CancelInstance {
            instance_key: first,
            terminate_suspended: true,
        },
        2_600,
    );
    let canceled = canonical_record(&rig);
    assert_eq!(canceled.state, state::MessageSubscriptionState::Canceled);
    assert!(
        canceled.awaiting_ack.is_some(),
        "the late ack still settles"
    );
    for (to_message, command) in held_acks {
        rig.run(to_message, command, 2_700);
    }
    let settled = canonical_record(&rig);
    assert_eq!(settled.state, state::MessageSubscriptionState::Canceled);
    assert!(settled.awaiting_ack.is_none());
    assert!(!settled.deferred, "no phantom wait");
    assert!(rig.message.state().buffered_messages[&published[0]]
        .correlated_instances
        .is_empty());
    assert!(redrive(&rig).is_empty());
    assert!(rig.instance.pending_resume_handshakes().is_empty());
    let second = rig.create("await-payment", &order, 3_000);
    assert_eq!(correlated(&rig, second), published, "{:?}", rig.trace);
}

/// #2037: an instance migrated BETWEEN the message partition's reservation of
/// a buffered message (the in-flight ack-paced delivery) and the instance
/// partition's consumption of it. The delivery itself still names the
/// pre-migration catch (it was routed before the migration), but the
/// durable receipt, the ack and the message partition's acknowledgement must
/// carry the RESOLVED post-migration consumption metadata — the catch's new
/// element id and the target process definition — exactly as Zeebe's
/// message-partition subscription is re-pointed by `MessageSubscription
/// MIGRATE` before it is `CORRELATED` (see the PR for the source citation).
/// A re-driven duplicate is answered with the same metadata, and the message
/// partition's canonical subscription is re-pointed at the migrated element.
#[test]
fn migration_between_reservation_and_consumption_acks_post_migration_metadata() {
    let source = ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await", "payment-received", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let target = ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await-v2", "payment-received", "orderId")
        .end_event("end")
        .connect("start", "await-v2")
        .connect("await-v2", "end")
        .build()
        .unwrap();
    let (mut rig, order) = Rig::new(source);
    let published = rig.publish_named_backlog("payment-received", &order, 1);
    // Reserve: the buffered message is delivered to the (remote) catch, and
    // the delivery is held in flight.
    let (instance_key, held) = rig.create_holding("await-payment", &order, 2_000, &|c| {
        matches!(c, Command::CorrelateMessageSubscription { .. })
    });
    assert_eq!(held.len(), 1, "exactly one delivery in flight: {held:?}");
    let Command::CorrelateMessageSubscription { element_id, .. } = &held[0].1 else {
        unreachable!()
    };
    assert_eq!(element_id, "await", "routed before the migration");
    // Migrate while the reservation is in flight.
    let deployed = rig.on_instance(Command::DeployProcess(target), 2_001);
    let target_key = deployed
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .expect("target deployed");
    rig.run(
        false,
        Command::MigrateInstance {
            instance_key,
            target_process_definition_key: target_key,
            mapping_instructions: vec![("await".into(), "await-v2".into())],
        },
        2_002,
    );
    // Consume: the in-flight delivery arrives after the migration.
    for (to_message, command) in held {
        rig.run(to_message, command, 2_003);
    }
    let expected = Some("await-v2".to_string());
    let acking: Vec<_> = rig
        .trace
        .iter()
        .filter_map(|e| match e {
            Event::MessageCorrelationAcknowledging {
                element_id,
                consumed,
                ..
            } => Some((*consumed, element_id.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(acking, vec![(true, expected.clone())], "{:?}", rig.trace);
    let acked: Vec<_> = rig
        .trace
        .iter()
        .filter_map(|e| match e {
            Event::MessageCorrelationAcknowledged { element_id, .. } => Some(element_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(acked, vec![expected.clone()]);
    // The message partition's canonical subscription is re-pointed.
    let canonical: Vec<_> = rig
        .message
        .state()
        .message_subscriptions
        .values()
        .map(|s| s.element_id.clone())
        .collect();
    assert_eq!(canonical, vec!["await-v2".to_string()]);
    // The consumed message stays taken by this instance.
    assert!(rig
        .message
        .state()
        .buffered_messages
        .get(&published[0])
        .is_none_or(|m| m.correlated_instances.contains(&instance_key)));
    // Replay of both journals reproduces the live state.
    let replayed = Engine::replay_partition(1, rig.message_journal.clone());
    assert_eq!(replayed.state(), rig.message.state());
    let replayed = Engine::replay_partition(0, rig.instance_journal.clone());
    assert_eq!(replayed.state(), rig.instance.state());
}

/// #2037: a pinned receipt re-driven (lost ack) carries the same
/// post-migration metadata as the original answer.
#[test]
fn a_redriven_receipt_carries_the_post_migration_metadata() {
    let source = ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await", "payment-received", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let target = ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await-v2", "payment-received", "orderId")
        .end_event("end")
        .connect("start", "await-v2")
        .connect("await-v2", "end")
        .build()
        .unwrap();
    let (mut rig, order) = Rig::new(source);
    rig.publish_named_backlog("payment-received", &order, 1);
    let (instance_key, held) = rig.create_holding("await-payment", &order, 2_000, &|c| {
        matches!(c, Command::CorrelateMessageSubscription { .. })
    });
    let deployed = rig.on_instance(Command::DeployProcess(target), 2_001);
    let target_key = deployed
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .expect("target deployed");
    rig.run(
        false,
        Command::MigrateInstance {
            instance_key,
            target_process_definition_key: target_key,
            mapping_instructions: vec![("await".into(), "await-v2".into())],
        },
        2_002,
    );
    // Deliver, but drop the ack: the receipt stays pinned.
    for (to_message, command) in held {
        rig.run_holding(to_message, command, 2_003, &is_ack);
    }
    let redriven = rig.instance.pending_unsettled_receipts();
    assert_eq!(redriven.len(), 1, "{redriven:?}");
    let Event::MessageCorrelationAcknowledging { element_id, .. } = &redriven[0] else {
        unreachable!()
    };
    assert_eq!(element_id.as_deref(), Some("await-v2"));
}
