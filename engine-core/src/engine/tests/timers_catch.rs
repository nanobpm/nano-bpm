//! `timers_catch` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn suspend_resume_lifecycle_gates_jobs_and_timers() {
    use crate::state::ProcessInstanceState;
    // start -> charge (service task, job `payment`) -> wait (timer PT5S) -> end
    let def = ProcessBuilder::new("delayed")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_intermediate_catch_event("wait", 5_000)
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command_at(Command::create_instance("delayed"), 1_000)
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Active
    );

    // Suspend: the instance stops making progress. Its created `payment` job is
    // no longer activatable while suspended (Camunda parity).
    let suspend = engine
        .apply_command_at(Command::suspend_instance(key), 2_000)
        .expect("suspend");
    assert!(suspend.contains(&Event::ProcessInstanceSuspended {
        instance_key: key,
        at: 2_000,
    }));
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Suspended
    );
    assert_eq!(engine.instance(key).unwrap().suspended_at, Some(2_000));
    assert!(
        engine
            .activate_jobs("payment", "w", 1, 60_000, 2_500)
            .is_empty(),
        "a suspended instance's jobs are not activatable"
    );

    // Resume: back to Active with its exact prior running state; the job is live
    // again and `suspended_at` clears.
    let resume = engine
        .apply_command_at(Command::resume_instance(key), 3_000)
        .expect("resume");
    assert!(resume.contains(&Event::ProcessInstanceResumed { instance_key: key }));
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Active
    );
    assert_eq!(engine.instance(key).unwrap().suspended_at, None);

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 3_000)
        .into_iter()
        .next()
        .expect("job activatable again after resume");
    engine
        .apply_command_at(Command::complete_job(job.key), 3_000)
        .unwrap();

    // Token now parked on the timer armed for due_at = 3000 + 5000 = 8000.
    assert!(!engine.is_completed(key));
    assert_eq!(engine.timers()[0].due_at, 8_000);

    // Suspend again: a due timer must NOT fire while the instance is suspended.
    engine
        .apply_command_at(Command::suspend_instance(key), 8_500)
        .expect("re-suspend");
    let fired = engine.trigger_timers(9_000);
    assert!(
        fired.is_empty(),
        "a suspended instance's timers do not fire"
    );
    assert!(!engine.is_completed(key));

    // Resume and tick again: the timer now fires and the instance completes.
    engine
        .apply_command_at(Command::resume_instance(key), 9_500)
        .expect("resume 2");
    let fired = engine.trigger_timers(10_000);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(engine.is_completed(key));
}

// zeebe-cells: element:IntermediateCatchEvent event:intermediate-catch:timer
#[test]
fn should_park_on_timer_then_fire_when_due() {
    // start -> charge (service task) -> wait (timer PT5S) -> end
    let def = ProcessBuilder::new("delayed")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_intermediate_catch_event("wait", 5_000)
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // create instance at t=1000, then run the job so the token reaches the timer.
    let events = engine
        .apply_command_at(Command::create_instance("delayed"), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 1_000)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command_at(Command::complete_job(job.key), 1_000)
        .unwrap();

    // Token now parked on the timer, armed for due_at = 1000 + 5000 = 6000.
    assert!(!engine.is_completed(instance_key));
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].state, state::TimerState::Created);
    assert_eq!(timers[0].due_at, 6_000);

    // A tick before the due instant fires nothing.
    let fired = engine.trigger_timers(5_999);
    assert!(fired.is_empty());
    assert!(!engine.is_completed(instance_key));

    // A tick at/after the due instant fires the timer and completes the instance.
    let fired = engine.trigger_timers(6_000);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(engine.is_completed(instance_key));
    assert!(fired.contains(&Event::ProcessInstanceCompleted { instance_key }));

    // The timer is retained as Triggered so a later tick never re-fires it.
    assert_eq!(engine.timers()[0].state, state::TimerState::Triggered);
    assert!(engine.trigger_timers(10_000).is_empty());
}

#[test]
fn should_recover_parked_timer_via_replay() {
    let def = ProcessBuilder::new("delayed")
        .start_event("start")
        .timer_intermediate_catch_event("wait", 5_000)
        .end_event("end")
        .connect("start", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(engine.apply_command(Command::DeployProcess(def)).unwrap());
    log.extend(
        engine
            .apply_command_at(Command::create_instance("delayed"), 1_000)
            .unwrap(),
    );

    // Replay the durable log into a fresh engine; the parked timer survives.
    let mut recovered = Engine::replay(log);
    let timers = recovered.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].state, state::TimerState::Created);
    let instance_key = timers[0].instance_key;
    assert!(!recovered.is_completed(instance_key));

    // The recovered engine fires the timer on the next due tick.
    recovered.trigger_timers(6_000);
    assert!(recovered.is_completed(instance_key));
}

// zeebe-cells: event:intermediate-catch:signal
#[test]
fn should_park_on_signal_catch_then_broadcast() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_signal_catch()))
        .unwrap();

    // Two instances both park on the signal catch.
    let a = engine
        .apply_command(Command::create_instance("await-signal"))
        .unwrap();
    let a_key = a.iter().find_map(|e| e.instance_key()).unwrap();
    let b = engine
        .apply_command(Command::create_instance("await-signal"))
        .unwrap();
    let b_key = b.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(!engine.is_completed(a_key));
    assert!(!engine.is_completed(b_key));
    assert_eq!(engine.signal_subscriptions().len(), 2);

    // A non-matching signal correlates nothing.
    let fired = engine.broadcast_signal("other", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(!engine.is_completed(a_key));

    // The matching broadcast fans out to BOTH instances, completing them.
    let fired = engine.broadcast_signal("all-clear", HashMap::new(), 0);
    assert_eq!(
        fired
            .iter()
            .filter(|e| matches!(e, Event::SignalCorrelated { .. }))
            .count(),
        2
    );
    assert!(engine.is_completed(a_key));
    assert!(engine.is_completed(b_key));

    // A repeat broadcast never re-correlates a settled subscription.
    let fired = engine.broadcast_signal("all-clear", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
}

// zeebe-cells: event:intermediate-catch:message
#[test]
fn should_park_on_message_catch_then_correlate() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();

    // Create an instance whose orderId resolves the correlation value "A".
    let events = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The token parks on the catch event, opening one open subscription.
    assert!(!engine.is_completed(instance_key));
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);
    assert_eq!(subs[0].message_name, "payment-received");
    assert_eq!(subs[0].correlation_key, "A");

    // A non-matching correlation key correlates nothing.
    let fired = engine.correlate_message("payment-received", "B", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(!engine.is_completed(instance_key));

    // The matching message releases the token and completes the instance.
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );

    // A repeat message never correlates the now-settled subscription twice.
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
}

#[test]
fn feel_signal_name_resolves_on_activation() {
    // The signal name is a FEEL expression referencing an instance variable,
    // evaluated when the signal subscription opens (on activation).
    let def = ProcessBuilder::new("await-signal")
        .start_event("start")
        .signal_intermediate_catch_event("await", "=\"clear-\" + zone")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance_with(
            "await-signal",
            vars(&[("zone", Value::Str("north".into()))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.signal_subscriptions().len(), 1);

    // A broadcast for a different zone does not correlate.
    let fired = engine.broadcast_signal("clear-south", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(!engine.is_completed(key));

    // The resolved name completes the instance.
    let fired = engine.broadcast_signal("clear-north", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::SignalCorrelated { .. })));
    assert!(engine.is_completed(key));
}

/// The cross-partition (Zeebe-style) placement protocol for an intermediate
/// catch: the instance partition parks on an `Opening` record, the host routes
/// `OpenMessageSubscription` to the message partition (`hash(correlation_key)`),
/// a published message correlates there and yields a `RemoteMessageCorrelation`,
/// and the routed `CorrelateMessageSubscription` continuation advances the token
/// back on the instance partition. Two engines, commands hand-routed.
#[test]
fn cross_partition_catch_opens_remote_then_correlates_back() {
    const N: u64 = 2;
    // A correlation value that hashes onto the *other* partition (1), so the
    // subscription is placed off the instance partition (0).
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

    // Create the instance on partition 0; its token parks on an Opening record
    // (the subscription's canonical home is partition 1).
    let created = instance_engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str(order.clone()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(crate::partition_of(instance_key), 0);
    assert!(!instance_engine.is_completed(instance_key));

    // Exactly one Opening event, no local Open subscription.
    let opening = created
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
            } => Some((
                *subscription_key,
                *instance_key,
                *element_instance_key,
                element_id.clone(),
                message_name.clone(),
                correlation_key.clone(),
                kind.clone(),
            )),
            _ => None,
        })
        .expect("an Opening event was emitted");
    assert!(!created
        .iter()
        .any(|e| matches!(e, Event::MessageSubscriptionCreated { .. })));
    assert_eq!(
        instance_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Opening
    );

    // Host routes the open to the message partition: it records the canonical
    // Open subscription.
    let (sub_key, inst_key, eik, eid, name, ckey, kind) = opening;
    assert_eq!(ckey, order);
    message_engine
        .apply_command(Command::OpenMessageSubscription {
            subscription_key: sub_key,
            instance_key: inst_key,
            element_instance_key: eik,
            element_id: eid,
            message_name: name,
            correlation_key: ckey,
            kind,
        })
        .unwrap();
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Re-routing the same open is idempotent (no second subscription).
    message_engine
        .apply_command(Command::OpenMessageSubscription {
            subscription_key: sub_key,
            instance_key: inst_key,
            element_instance_key: eik,
            element_id: "await".into(),
            message_name: "payment-received".into(),
            correlation_key: order.clone(),
            kind: state::MessageSubscriptionKind::IntermediateCatch,
        })
        .unwrap();
    assert_eq!(message_engine.message_subscriptions().len(), 1);

    // Publish lands on the message partition. It settles the canonical sub and
    // emits a RemoteMessageCorrelation (no local token to advance there).
    let published = message_engine
        .apply_command(Command::correlate_message_with(
            "payment-received",
            order.clone(),
            vars(&[("paid", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(!published
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    let remote = published
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
            } => Some((
                *subscription_key,
                *message_key,
                *instance_key,
                *element_instance_key,
                element_id.clone(),
                kind.clone(),
                variables.clone(),
            )),
            _ => None,
        })
        .expect("a RemoteMessageCorrelation was emitted");
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );

    // Host routes the continuation back to the instance partition: the token
    // advances and the instance completes, merging the message variables.
    let (r_sub, r_msg, r_inst, r_eik, r_eid, r_kind, r_vars) = remote;
    assert_eq!(r_sub, sub_key);
    assert_eq!(crate::partition_of(r_inst), 0);
    let advanced = instance_engine
        .apply_command(Command::CorrelateMessageSubscription {
            subscription_key: r_sub,
            message_key: r_msg,
            instance_key: r_inst,
            element_instance_key: r_eik,
            element_id: r_eid,
            kind: r_kind,
            variables: r_vars,
        })
        .unwrap();
    assert!(advanced
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(instance_engine.is_completed(instance_key));
    assert_eq!(
        instance_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );

    // Re-delivering the continuation is a no-op (at-least-once safe).
    let again = instance_engine
        .apply_command(Command::CorrelateMessageSubscription {
            subscription_key: sub_key,
            message_key: r_msg,
            instance_key: r_inst,
            element_instance_key: r_eik,
            element_id: "await".into(),
            kind: state::MessageSubscriptionKind::IntermediateCatch,
            variables: HashMap::new(),
        })
        .unwrap();
    assert!(!again
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
}

#[test]
fn catch_event_output_mapping_applies_on_correlation() {
    // A message catch event carrying a `zeebe:ioMapping` output that increments a
    // loop counter (`=round + 1 -> round`) — the shape the urban-pr-review
    // convergence loop uses to advance its round on each `review-ready`. The
    // mapping must apply when the event is triggered; before the parser attached
    // catch-event ioMappings it was silently dropped and `round` stayed at 1.
    let xml = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:intermediateCatchEvent id="await">
            <bpmn:extensionElements>
              <zeebe:ioMapping>
                <zeebe:output source="=round + 1" target="round" />
              </zeebe:ioMapping>
            </bpmn:extensionElements>
            <bpmn:messageEventDefinition messageRef="Message_1" />
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
          <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="Message_1" name="review-ready">
          <bpmn:extensionElements>
            <zeebe:subscription correlationKey="=prKey" />
          </bpmn:extensionElements>
        </bpmn:message>
      </bpmn:definitions>"#;
    let process = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance_with(
            "p",
            vars(&[("prKey", Value::Str("A".into())), ("round", Value::Int(1))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Triggering the catch event runs its output mapping. The instance then
    // completes and drops hot state (ADR 0012), so assert the incremented value
    // on the durable `VariablesUpdated` event rather than hot state.
    let fired = engine.correlate_message("review-ready", "A", HashMap::new(), 0);
    let bumped = fired
        .iter()
        .find_map(|e| match e {
            Event::VariablesUpdated {
                instance_key: k,
                variables,
            } if *k == instance_key => variables.get("round").cloned(),
            _ => None,
        })
        .expect("the catch event's output mapping emits a VariablesUpdated for round");
    assert_eq!(bumped, Value::Int(2));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn should_run_inclusive_split_and_join_across_unconditional_flows() {
    // An inclusive gateway with only unconditional outgoing flows behaves like a
    // parallel split: every flow is taken, and the join synchronises them.
    let def = ProcessBuilder::new("inc-all")
        .start_event("s")
        .inclusive_gateway("isplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .inclusive_gateway("join")
        .end_event("e")
        .connect("s", "isplit")
        .connect("isplit", "a")
        .connect("isplit", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("inc-all"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    assert_eq!(engine.pending_jobs().len(), 2);
    complete_one(&mut engine, "ja");
    assert!(!engine.is_completed(instance_key));
    complete_one(&mut engine, "jb");
    assert!(engine.is_completed(instance_key));
}

#[test]
fn subprocess_terminate_end_cancels_a_signal_subscription_in_its_scope() {
    // A terminate end must cancel an open SIGNAL subscription in the torn-down
    // scope — before, scope teardown cancelled only message subscriptions, so a
    // later broadcast could fire a token in a dead scope.
    //
    //  sub: sub_start -> split =< await(signal catch) -> await_end,
    //                             trigger(svc) -> stop(terminate) >
    let orch = ProcessBuilder::new("orch")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .signal_intermediate_catch_event("await", "all-clear")
        .contained_in("await", "sub")
        .end_event("await_end")
        .contained_in("await_end", "sub")
        .service_task("trigger", "trigger-job")
        .contained_in("trigger", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "await")
        .connect("await", "await_end")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .connect("sub", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(orch)).unwrap();
    engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    // `signal_subscriptions()` retains cancelled records, so count only OPEN ones.
    let open_subs = |e: &Engine| {
        e.signal_subscriptions()
            .iter()
            .filter(|s| s.state == crate::state::MessageSubscriptionState::Open)
            .count()
    };
    assert_eq!(open_subs(&engine), 1);

    let events = complete_one(&mut engine, "trigger-job");
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::SignalSubscriptionCanceled { .. })));
    assert_eq!(open_subs(&engine), 0);
}

#[test]
fn should_take_default_flow_even_when_listed_before_the_conditional() {
    // Regression: an exclusive gateway's explicit default flow must be a
    // fallback only — never selected by document order. Here the default
    // (g -> rejected) is connected BEFORE the conditional (g -> approved),
    // the order Camunda often serialises.
    let process = ProcessBuilder::new("approval")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("approved")
        .end_event("rejected")
        .connect("s", "g")
        .connect_default("g", "rejected")
        .connect_when("g", "approved", r#"decision = "yes""#)
        .build()
        .unwrap();

    // decision == yes -> the conditional flow wins despite appearing last.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process.clone()))
        .unwrap();
    let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "approved"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));

    // decision != yes -> the default flow is the fallback.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process))
        .unwrap();
    let vars = HashMap::from([("decision".to_string(), Value::Str("no".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

/// #592 follow-up #2: `JobErrorRequest.variables` must be instantiated at the
/// local scope of the error catch event, so the error-handling path downstream
/// can read them — not silently dropped.
#[test]
fn should_seed_thrown_error_variables_at_the_catch_scope() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary_to_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("payment-recover"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when the worker throws the caught error WITH variables
    let vars = HashMap::from([("reason".to_string(), Value::Str("declined".to_string()))]);
    engine
        .apply_command(Command::throw_job_error_with(
            job_key,
            "CARD_DECLINED",
            "card was declined",
            vars,
        ))
        .unwrap();

    // then the downstream recovery job (on the error-handling path) sees them
    let recover = engine.activate_jobs("recovery", "w", 1, 60_000, 0);
    assert_eq!(recover.len(), 1);
    assert_eq!(
        recover[0].variables.get("reason"),
        Some(&Value::Str("declined".to_string()))
    );
}

#[test]
fn should_open_a_message_start_subscription_at_deploy() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // Deploy opens a process-level subscription but creates no instance.
    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    assert!(engine.state().instances.is_empty());
    let sub = &engine.state().message_start_subscriptions["order-placed"];
    assert_eq!(sub.process_id, "order-flow");
    assert_eq!(sub.start_element_id, "start");
}

#[test]
fn should_create_an_instance_when_a_message_start_correlates() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // A non-matching message creates nothing.
    engine.correlate_message("other", "", HashMap::new(), 0);
    assert!(engine.state().instances.is_empty());

    // The matching message creates and runs a fresh instance to completion,
    // seeding it with the message's variables.
    let fired = engine.correlate_message("order-placed", "", vars(&[("amount", Value::Int(7))]), 0);
    let (instance_key, seeded) = fired
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                variables,
                ..
            } => Some((*instance_key, variables.get("amount").cloned())),
            _ => None,
        })
        .unwrap();
    // The instance is seeded with the message's variables (carried on the
    // durable ProcessInstanceCreated event) and runs to completion, which drops
    // its hot-state variables (ADR 0012).
    assert_eq!(seeded, Some(Value::Int(7)));
    assert!(engine.is_completed(instance_key));
    assert!(engine.state().instances[&instance_key].variables.is_empty());
}

/// Camunda 8.10 `businessId` on publish/correlate: it is stamped on the
/// instance a message start event creates (carried on the durable
/// `ProcessInstanceCreated`), and has no effect on a catch correlation.
#[test]
fn message_start_stamps_the_message_business_id_on_the_new_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();
    let fired = engine
        .apply_command_at(
            Command::CorrelateMessage {
                message_name: "order-placed".into(),
                correlation_key: String::new(),
                variables: HashMap::new(),
                business_id: Some("order-42".into()),
            },
            0,
        )
        .unwrap();
    let stamped = fired.iter().find_map(|e| match e {
        Event::ProcessInstanceCreated { business_id, .. } => Some(business_id.clone()),
        _ => None,
    });
    assert_eq!(stamped, Some(Some("order-42".to_string())));

    // A catch correlation leaves the waiting instance's business id untouched.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = created
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        })
        .unwrap();
    let fired = engine
        .apply_command_at(
            Command::CorrelateMessage {
                message_name: "payment-received".into(),
                correlation_key: "A".into(),
                variables: HashMap::new(),
                business_id: Some("ignored".into()),
            },
            0,
        )
        .unwrap();
    assert!(
        fired.iter().any(
            |e| matches!(e, Event::MessageCorrelated { instance_key: k, .. } if *k == instance_key)
        ),
        "the catch correlates"
    );
    assert!(
        !fired
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })),
        "a catch correlation starts nothing"
    );
    assert_eq!(engine.state().instances[&instance_key].business_id, None);
}

#[test]
fn feel_message_start_name_resolves_at_deploy() {
    // A message-start-event name expression is evaluated at deploy time against
    // an empty context (Zeebe parity); the resolved value keys the subscription.
    let def = ProcessBuilder::new("order-flow")
        .message_start_event("start", "=\"order-\" + \"placed\"")
        .end_event("end")
        .connect("start", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    // The subscription is keyed by the resolved name, not the raw expression.
    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    assert!(engine
        .state()
        .message_start_subscriptions
        .contains_key("order-placed"));

    // A message under the resolved name creates an instance.
    let fired = engine.correlate_message("order-placed", "", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })));
}

#[test]
fn should_create_one_instance_per_matching_message_start() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // Each matching message creates a distinct instance.
    engine.correlate_message("order-placed", "", HashMap::new(), 0);
    engine.correlate_message("order-placed", "", HashMap::new(), 0);
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn message_start_distributes_created_instances_across_partitions() {
    // On a multi-partition deploy owner, message-start correlations must NOT
    // pile every created instance onto the deploy partition. The round-robin
    // dispatcher keeps the first inline (target == self) and emits a routable
    // `StartInstanceDispatched` (carrying the chosen target) for the rest.
    const N: u64 = 4;
    let mut engine = Engine::with_partition(0);
    engine.set_num_partitions(N);
    engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    let mut dispatched_targets = Vec::new();
    let mut inline_instances = 0;
    for _ in 0..N {
        let fired = engine.correlate_message("order-placed", "", HashMap::new(), 0);
        for e in &fired {
            match e {
                Event::ProcessInstanceCreated { .. } => inline_instances += 1,
                Event::StartInstanceDispatched {
                    process_id,
                    target_partition,
                    ..
                } => {
                    assert_eq!(process_id, "order-flow");
                    dispatched_targets.push(*target_partition);
                }
                _ => {}
            }
        }
    }

    // Exactly one lands inline (rr=0 -> target 0 == self); the other three
    // dispatch to partitions 1, 2, 3 in round-robin order.
    assert_eq!(inline_instances, 1, "the first correlation creates inline");
    assert_eq!(
        dispatched_targets,
        vec![1, 2, 3],
        "subsequent correlations dispatch round-robin to the other partitions"
    );
    assert_eq!(
        engine.state().instances.len(),
        1,
        "only the inline instance lives on the deploy partition"
    );
}

#[test]
fn should_recover_a_message_start_subscription_via_replay() {
    let mut engine = Engine::new();
    let log = engine
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();

    // Replay: the process-level subscription survives and still fires.
    let mut recovered = Engine::replay(log);
    assert_eq!(recovered.state().message_start_subscriptions.len(), 1);
    let fired = recovered.correlate_message("order-placed", "", HashMap::new(), 0);
    let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(recovered.is_completed(instance_key));
}

#[test]
fn should_arm_a_start_timer_at_deploy() {
    let mut engine = Engine::new();
    // Deploy at t=1000: the start timer is armed for 1000 + 10000 = 11000.
    engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_once()),
            1_000,
        )
        .unwrap();
    assert_eq!(engine.state().start_timers.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, Some(11_000));
    assert!(engine.state().instances.is_empty());
}

#[test]
fn should_fire_a_one_shot_start_timer_exactly_once() {
    let mut engine = Engine::new();
    engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_once()),
            1_000,
        )
        .unwrap();

    // A tick before the due instant creates nothing.
    assert!(engine.trigger_timers(10_999).is_empty());
    assert!(engine.state().instances.is_empty());

    // At the due instant the timer fires and creates one instance; the timer
    // is retained but has no due time, so it never fires again.
    let fired = engine.trigger_timers(11_000);
    let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.state().instances.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, None);

    // A later tick fires nothing more.
    assert!(engine.trigger_timers(100_000).is_empty());
    assert_eq!(engine.state().instances.len(), 1);
}

#[test]
fn should_re_arm_a_cycle_start_timer_after_each_fire() {
    let mut engine = Engine::new();
    engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_cycle()),
            1_000,
        )
        .unwrap();

    // First fire at 11000 creates an instance and re-arms for 21000.
    engine.trigger_timers(11_000);
    assert_eq!(engine.state().instances.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, Some(21_000));

    // Second fire at 21000 creates another and re-arms for 31000.
    engine.trigger_timers(21_000);
    assert_eq!(engine.state().instances.len(), 2);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.due_at, Some(31_000));
}

#[test]
fn should_recover_an_armed_start_timer_via_replay() {
    let mut engine = Engine::new();
    let log = engine
        .apply_command_at(
            Command::DeployProcess(process_with_timer_start_once()),
            1_000,
        )
        .unwrap();

    // Replay: the armed start timer survives and still fires on the next tick.
    let mut recovered = Engine::replay(log);
    assert_eq!(recovered.state().start_timers.len(), 1);
    assert_eq!(
        recovered
            .state()
            .start_timers
            .values()
            .next()
            .unwrap()
            .due_at,
        Some(11_000)
    );
    let fired = recovered.trigger_timers(11_000);
    let instance_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(recovered.is_completed(instance_key));
}

#[test]
fn none_plus_message_start_both_function() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_none_plus_message()))
        .unwrap();

    // Deploy wires the message start's subscription (the none start opens none),
    // and creates no instance yet.
    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    let sub = &engine.state().message_start_subscriptions["order-placed"];
    assert_eq!(sub.process_id, "dual-start");
    assert_eq!(sub.start_element_id, "b_msg");
    assert!(engine.state().instances.is_empty());

    // The none start accepts a CreateInstance and runs to completion.
    let created = engine
        .apply_command(Command::create_instance("dual-start"))
        .unwrap();
    let none_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(none_key));

    // The message start ALSO functions: a matching message creates a distinct
    // instance (seeded with the message variables) that runs to completion.
    let fired = engine.correlate_message("order-placed", "", vars(&[("amount", Value::Int(9))]), 0);
    let (msg_key, seeded) = fired
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated {
                instance_key,
                variables,
                ..
            } => Some((*instance_key, variables.get("amount").cloned())),
            _ => None,
        })
        .unwrap();
    assert_ne!(msg_key, none_key);
    assert_eq!(seeded, Some(Value::Int(9)));
    assert!(engine.is_completed(msg_key));
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn none_plus_timer_start_both_function() {
    let mut engine = Engine::new();
    // Deploy at t=1000: the timer start is armed for 11000.
    engine
        .apply_command_at(Command::DeployProcess(process_none_plus_timer()), 1_000)
        .unwrap();
    assert_eq!(engine.state().start_timers.len(), 1);
    let timer = engine.state().start_timers.values().next().unwrap();
    assert_eq!(timer.start_element_id, "b_timer");
    assert_eq!(timer.due_at, Some(11_000));
    assert!(engine.state().instances.is_empty());

    // The none start accepts a CreateInstance.
    let created = engine
        .apply_command_at(Command::create_instance("dual-timer"), 2_000)
        .unwrap();
    let none_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(none_key));

    // The timer start ALSO fires when due, creating a distinct instance.
    let fired = engine.trigger_timers(11_000);
    let timer_key = fired.iter().find_map(|e| e.instance_key()).unwrap();
    assert_ne!(timer_key, none_key);
    assert!(engine.is_completed(timer_key));
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn two_message_starts_open_two_subscriptions() {
    // Two message starts (distinct names) each open their own subscription and
    // fire at their own start element — no none start required.
    let def = ProcessBuilder::new("two-msg")
        .message_start_event("s_a", "msg-a")
        .message_start_event("s_b", "msg-b")
        .end_event("e_a")
        .end_event("e_b")
        .connect("s_a", "e_a")
        .connect("s_b", "e_b")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    assert_eq!(engine.state().message_start_subscriptions.len(), 2);
    assert_eq!(
        engine.state().message_start_subscriptions["msg-a"].start_element_id,
        "s_a"
    );
    assert_eq!(
        engine.state().message_start_subscriptions["msg-b"].start_element_id,
        "s_b"
    );

    // Each name fires its own start; a non-matching name fires nothing.
    engine.correlate_message("msg-a", "", HashMap::new(), 0);
    engine.correlate_message("msg-b", "", HashMap::new(), 0);
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn message_and_timer_starts_wire_both_triggers() {
    // A message start and a timer start (no none start) each get wired at deploy.
    let def = ProcessBuilder::new("msg-and-timer")
        .message_start_event("m", "kick")
        .timer_start_event_once("t", 5_000)
        .end_event("me")
        .end_event("te")
        .connect("m", "me")
        .connect("t", "te")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command_at(Command::DeployProcess(def), 1_000)
        .unwrap();

    assert_eq!(engine.state().message_start_subscriptions.len(), 1);
    assert_eq!(engine.state().start_timers.len(), 1);

    engine.correlate_message("kick", "", HashMap::new(), 2_000);
    engine.trigger_timers(6_000);
    assert_eq!(engine.state().instances.len(), 2);
}

#[test]
fn cancel_disarms_a_parked_timer() {
    let def = ProcessBuilder::new("delayed")
        .start_event("start")
        .timer_intermediate_catch_event("wait", 5_000)
        .end_event("end")
        .connect("start", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let instance_key = engine
        .apply_command_at(Command::create_instance("delayed"), 1_000)
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(engine.timers()[0].state, state::TimerState::Created);

    engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    // The armed timer is cancelled, so a later due tick fires nothing.
    assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);
    assert!(engine.trigger_timers(6_000).is_empty());
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
}

// --- FEEL timer expressions -------------------------------------------------
#[test]
fn feel_duration_timer_evaluates_variable() {
    // A timer intermediate catch whose timeDuration is a FEEL expression
    // (`=waitFor`) resolves against the instance variables at timer creation.
    let def = ProcessBuilder::new("feel-timer")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_intermediate_catch_event("wait", 0)
        .with_timer(
            "wait",
            crate::model::TimerDef {
                kind: crate::model::TimerDefKind::Duration,
                expr: "=waitFor".to_string(),
            },
        )
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let mut vars = HashMap::new();
    vars.insert("waitFor".to_string(), Value::Str("PT5S".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("feel-timer", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 1_000)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command_at(Command::complete_job(job.key), 1_000)
        .unwrap();

    // due_at = now(1000) + FEEL("PT5S")=5000 = 6000.
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].due_at, 6_000);
    assert!(!engine.is_completed(instance_key));
    assert!(engine.trigger_timers(5_999).is_empty());
    assert!(engine
        .trigger_timers(6_000)
        .iter()
        .any(|e| matches!(e, Event::TimerTriggered { .. })));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn feel_date_timer_fires_at_absolute_instant() {
    // A timeDate timer resolves to an absolute epoch instant, independent of the
    // engine's current clock.
    let def = ProcessBuilder::new("feel-date")
        .start_event("start")
        .service_task("charge", "payment")
        .timer_intermediate_catch_event("wait", 0)
        .with_timer(
            "wait",
            crate::model::TimerDef {
                kind: crate::model::TimerDefKind::Date,
                expr: "=dueAt".to_string(),
            },
        )
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "wait")
        .connect("wait", "end")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let mut vars = HashMap::new();
    vars.insert(
        "dueAt".to_string(),
        Value::Str("2030-01-01T00:00:00Z".to_string()),
    );
    engine
        .apply_command_at(Command::create_instance_with("feel-date", vars), 1_000)
        .unwrap();

    let job = engine
        .activate_jobs("payment", "w", 1, 60_000, 1_000)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command_at(Command::complete_job(job.key), 1_000)
        .unwrap();

    // 2030-01-01T00:00:00Z = 1_893_456_000_000 ms since the Unix epoch.
    let timers = engine.timers();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].due_at, 1_893_456_000_000);
}

// zeebe-cells: event:intermediate-catch:conditional
#[test]
fn should_park_on_a_conditional_catch_until_a_variable_change_satisfies_it() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_catch()))
        .unwrap();

    // The condition is false at arrival (approved unset), so the token parks on
    // an open conditional subscription.
    let created = engine
        .apply_command(Command::create_instance("await-approval"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(!engine.is_completed(key));
    assert_eq!(engine.conditional_subscriptions().len(), 1);
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );
    // The subscription records the variable its condition depends on.
    assert_eq!(
        engine.conditional_subscriptions()[0].referenced_vars,
        vec!["approved".to_string()]
    );

    // Setting an UNRELATED variable does not trigger it.
    engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("other", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(!engine.is_completed(key));

    // Setting `approved = true` flips the condition and fires the catch, which
    // completes the instance.
    let fired = engine
        .apply_command(Command::set_variables(
            key,
            vars(&[("approved", Value::Bool(true))]),
        ))
        .unwrap();
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::ConditionalTriggered { .. })));
    assert!(engine.is_completed(key));
    assert_eq!(
        engine.conditional_subscriptions()[0].state,
        state::MessageSubscriptionState::Correlated
    );
}

#[test]
fn should_pass_a_conditional_catch_immediately_when_already_true_on_arrival() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_conditional_catch()))
        .unwrap();

    // The condition already holds at creation, so the token passes straight
    // through without ever opening a subscription.
    let created = engine
        .apply_command(Command::create_instance_with(
            "await-approval",
            vars(&[("approved", Value::Bool(true))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(key));
    assert!(engine.conditional_subscriptions().is_empty());
}

// zeebe-cells: element:EventBasedGateway
#[test]
fn event_based_gateway_arms_all_catch_events_then_timer_wins() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(event_gateway_race()))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("race", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The gateway split armed both catch events at once: a timer (due 6000) and
    // an open message subscription.
    assert_eq!(engine.timers().len(), 1);
    assert_eq!(engine.timers()[0].state, state::TimerState::Created);
    assert_eq!(engine.timers()[0].due_at, 6_000);
    assert_eq!(engine.message_subscriptions().len(), 1);
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );
    assert!(!engine.is_completed(instance_key));

    // The timer fires first: its branch completes the instance and the losing
    // message sibling is withdrawn (its subscription cancelled).
    let fired = engine.trigger_timers(6_000);
    assert!(fired.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );

    // A message arriving after the race is over correlates nothing.
    let late = engine
        .apply_command(Command::correlate_message("reply", "A"))
        .unwrap();
    assert!(!late
        .iter()
        .any(|e| matches!(e, Event::ElementActivated { .. })));
}

#[test]
fn event_based_gateway_message_wins_and_withdraws_the_timer_sibling() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(event_gateway_race()))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("race", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The message correlates before the timer is due: its branch completes the
    // instance and the losing timer sibling is cancelled.
    let correlated = engine
        .apply_command_at(Command::correlate_message("reply", "A"), 2_000)
        .unwrap();
    assert!(correlated.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
    assert_eq!(engine.timers()[0].state, state::TimerState::Canceled);

    // The cancelled timer never fires, even past its original due instant.
    assert!(engine.trigger_timers(6_000).is_empty());
}

#[test]
fn event_based_gateway_ambiguous_owner_withdraws_nothing() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(ambiguous_event_gateway_race()))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("ambig", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Only gw1's race is live (gw2 is never reached): its timer `onTimer1` and
    // the shared message subscription are armed.
    assert_eq!(engine.timers().len(), 1);
    assert_eq!(engine.timers()[0].state, state::TimerState::Created);
    assert_eq!(engine.message_subscriptions().len(), 1);

    // The shared catch event wins its message. Because two gateways statically
    // route into it, the owning race is ambiguous, so the guard withdraws
    // nothing: the timer sibling is left armed (not cancelled), and its lingering
    // token keeps the instance live rather than completing it.
    let correlated = engine
        .apply_command_at(Command::correlate_message("reply", "A"), 2_000)
        .unwrap();
    assert!(!correlated
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.timers()[0].state,
        state::TimerState::Created,
        "ambiguous owner must not cancel the sibling timer"
    );
}

#[test]
fn event_based_gateway_never_force_completes_non_catch_sibling() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            malformed_event_gateway_with_service_task_sibling(),
        ))
        .unwrap();

    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command_at(Command::create_instance_with("malformed", vars), 1_000)
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The gateway armed both the message catch and the service task: a job exists.
    let job_before = engine
        .state()
        .jobs
        .values()
        .find(|j| j.job_type == "do-work")
        .expect("a do-work job was created");
    let job_state_before = job_before.state;

    // The message wins. The service-task sibling is a non-catch node, so it is
    // left untouched: its job survives in the same state (never force-completed),
    // and the lingering token keeps the instance live.
    let correlated = engine
        .apply_command_at(Command::correlate_message("reply", "A"), 2_000)
        .unwrap();
    assert!(!correlated
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    assert!(!engine.is_completed(instance_key));
    let job_after = engine
        .state()
        .jobs
        .values()
        .find(|j| j.job_type == "do-work")
        .expect("the do-work job must still exist");
    assert_eq!(
        job_after.state, job_state_before,
        "a non-catch sibling must not be force-completed (its job must survive)"
    );
}

/// Regression (#1157): a link throw hands its token to the matching link catch;
/// the second half of the model (everything downstream of the catch) must
/// actually run, not silently vanish while the instance reports success.
// zeebe-cells: event:intermediate-catch:link
#[test]
fn link_events_hand_the_token_from_throw_to_catch() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="links" isExecutable="true">
          <bpmn:startEvent id="LStart"><bpmn:outgoing>LF1</bpmn:outgoing></bpmn:startEvent>
          <bpmn:sequenceFlow id="LF1" sourceRef="LStart" targetRef="Throw" />
          <bpmn:intermediateThrowEvent id="Throw">
            <bpmn:incoming>LF1</bpmn:incoming>
            <bpmn:linkEventDefinition id="LD1" name="hop" />
          </bpmn:intermediateThrowEvent>
          <bpmn:intermediateCatchEvent id="Catch">
            <bpmn:outgoing>LF2</bpmn:outgoing>
            <bpmn:linkEventDefinition id="LD2" name="hop" />
          </bpmn:intermediateCatchEvent>
          <bpmn:sequenceFlow id="LF2" sourceRef="Catch" targetRef="AfterLink" />
          <bpmn:serviceTask id="AfterLink">
            <bpmn:incoming>LF2</bpmn:incoming>
            <bpmn:outgoing>LF3</bpmn:outgoing>
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="probe-after-link" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:sequenceFlow id="LF3" sourceRef="AfterLink" targetRef="LEnd" />
          <bpmn:endEvent id="LEnd"><bpmn:incoming>LF3</bpmn:incoming></bpmn:endEvent>
        </bpmn:process>
      </bpmn:definitions>"#;

    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    assert_eq!(
        def.element("Throw").unwrap().kind,
        ElementKind::LinkIntermediateThrowEvent {
            link_name: "hop".to_string()
        }
    );
    assert_eq!(
        def.element("Catch").unwrap().kind,
        ElementKind::LinkIntermediateCatchEvent {
            link_name: "hop".to_string()
        }
    );

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = engine
        .apply_command(Command::create_instance("links"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The token must reach `AfterLink` (a job appears) rather than vanishing at
    // the throw — and the instance must NOT be reported completed while that
    // second-half work is still pending (the #1157 false success).
    assert!(
        !engine.is_completed(inst),
        "instance must not complete while AfterLink is still pending"
    );
    let jobs = engine.activate_jobs("probe-after-link", "w", 5, 1_000, 0);
    assert_eq!(
        jobs.len(),
        1,
        "the link catch's downstream service task must run (got {} jobs)",
        jobs.len()
    );

    // Completing the second half drives the instance to a genuine completion.
    engine
        .apply_command(Command::complete_job(jobs[0].key))
        .unwrap();
    assert!(engine.is_completed(inst));
}
