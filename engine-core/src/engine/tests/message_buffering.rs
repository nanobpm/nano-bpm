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
    let opened = message_engine.apply_command_at(open, 3_000).unwrap();
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
            } => Some(Command::CorrelateMessageSubscription {
                subscription_key: *subscription_key,
                message_key: *message_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                kind: kind.clone(),
                variables: variables.clone(),
                delivery_key: *delivery_key,
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
