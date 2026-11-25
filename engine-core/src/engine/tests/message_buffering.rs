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
    // r4692584915: the backlog is drained from one ordered scan of the
    // buffer, not one full scan per message.
    assert_eq!(engine.buffer_scan_count.get(), 1);
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
        Event::MessageCorrelationAcknowledging {
            subscription_key,
            message_key,
            accepted,
            deferred,
            ..
        } => (
            true,
            Command::AcknowledgeMessageCorrelation {
                subscription_key,
                message_key,
                accepted,
                deferred,
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
        instance
            .apply_command(Command::DeployProcess(definition))
            .unwrap();
        let mut message = Engine::with_partition(1);
        message.set_num_partitions(Self::N);
        let rig = Self {
            instance,
            message,
            message_journal: Vec::new(),
            trace: Vec::new(),
        };
        (rig, order)
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
                self.instance.apply_command_at(command, now).unwrap()
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

/// #1874: a remote INTERRUPTING subscription takes exactly one buffered
/// message (no ack pacing: a one-shot subscription settles on delivery).
#[test]
fn remote_interrupting_subscription_takes_exactly_one() {
    let (mut rig, order) = Rig::new(process_with_message_catch());
    let published = rig.publish_named_backlog("payment-received", &order, 3);
    let instance_key = rig.create("await-payment", &order, 2_000);
    assert_eq!(rig.delivered_to(instance_key), published[..1]);
    assert!(rig.trace.iter().all(|e| !matches!(
        e,
        Event::RemoteMessageCorrelation {
            ack_partition: Some(_),
            ..
        } | Event::MessageCorrelationAcknowledging { .. }
    )));
    assert!(rig.instance.is_completed(instance_key));
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
                accepted: true,
                deferred: false,
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
