//! `lifecycle` engine tests (slice 7 of #1201), extracted verbatim.
use super::*;

#[test]
fn subprocess_input_mapping_is_local_to_the_subprocess_scope() {
    // A sub-process is its own variable scope: its input mapping creates a
    // variable LOCAL to the sub-process, visible to a job running inside it but
    // NOT at the root scope.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // The inner job sees the sub-process-local `scoped` (5 = seed + 1)...
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("scoped"), Some(&Value::Int(5)));
    // ...but it never leaked to the root scope.
    assert_eq!(io_var(&engine, inst, "scoped"), None);
}

#[test]
fn inner_element_input_mapping_resolves_against_the_enclosing_subprocess_scope() {
    // Regression: an input mapping on an element nested inside a sub-process must
    // evaluate its source against the sub-process's scoped view (which carries the
    // sub-process-local `scoped`), NOT the root variables. Evaluating against root
    // would leave `scoped` unresolved and silently drop `derived`.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            subprocess_with_nested_input_mapping(),
        ))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    engine
        .apply_command(Command::create_instance_with("nested-scope", vars))
        .unwrap();

    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    // scoped = seed + 1 = 5 (sub-process local); derived = scoped * 10 = 50,
    // which is only correct when the inner input mapping sees the enclosing scope.
    assert_eq!(job.variables.get("scoped"), Some(&Value::Int(5)));
    assert_eq!(job.variables.get("derived"), Some(&Value::Int(50)));
}

#[test]
fn subprocess_scope_local_variable_is_dropped_when_the_subprocess_completes() {
    // Once the sub-process drains and completes, its local scope (and the
    // input-mapped `scoped`) is destroyed — never surfacing at the root.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "work");
    assert!(engine.is_completed(inst));
    let instance = engine.instance(inst).unwrap();
    assert_eq!(instance.variables.get("scoped"), None);
    assert!(instance.scope_variables.is_empty());
    assert!(instance.scope_parents.is_empty());
}

#[test]
fn subprocess_output_mapping_reads_local_scope_and_propagates_to_root() {
    // A sub-process output mapping can read the sub-process-local `scoped` and
    // its projected result propagates OUT to the (root) parent scope, surviving
    // the sub-process scope teardown.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(vec![
            crate::model::Mapping {
                source: "=scoped".to_string(),
                target: "exported".to_string(),
            },
        ])))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let events = complete_one(&mut engine, "work");
    assert!(engine.is_completed(inst));
    // The process completes (clearing root variables), so assert the sub-process
    // output mapping surfaced `exported = 5` propagated to the root scope as a
    // flat `VariablesUpdated`, and that the local `scoped` never leaked there.
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == inst
                    && variables.get("exported") == Some(&Value::Int(5))
        )),
        "sub-process output should export scoped=5 to root; events: {events:?}"
    );
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::VariablesUpdated { variables, .. } if variables.contains_key("scoped")
        )),
        "the sub-process-local `scoped` must not surface at root; events: {events:?}"
    );
}

#[test]
fn set_variables_local_writes_only_the_target_scope() {
    // `SetVariables` with local=true writes strictly into the addressed scope.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("only_here".to_string(), Value::Int(7))]),
            true,
        ))
        .unwrap();
    // The local write is in the sub-process scope, not the root.
    assert_eq!(io_var(&engine, inst, "only_here"), None);
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .scope_variables
            .get(&sub_scope)
            .and_then(|m| m.get("only_here")),
        Some(&Value::Int(7))
    );
}

#[test]
fn set_variables_non_local_propagates_to_the_root_scope() {
    // `SetVariables` with local=false against a nested scope, for a name defined
    // nowhere, creates the variable at the ROOT scope (Zeebe default).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("bubbled".to_string(), Value::Int(3))]),
            false,
        ))
        .unwrap();
    // Propagated to root since no ancestor scope defines `bubbled`.
    assert_eq!(io_var(&engine, inst, "bubbled"), Some(Value::Int(3)));
}

#[test]
fn set_variables_non_local_updates_the_defining_scope_not_root() {
    // `SetVariables` with local=false for a name the target scope already defines
    // updates THAT scope, not the root.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let inner_eik = engine.pending_jobs()[0].element_instance_key;
    let sub_scope = *engine
        .instance(inst)
        .unwrap()
        .scopes
        .get(&inner_eik)
        .unwrap();

    // `scoped` is owned by the sub-process scope (from its input mapping).
    engine
        .apply_command(Command::set_variables_scoped(
            sub_scope,
            HashMap::from([("scoped".to_string(), Value::Int(42))]),
            false,
        ))
        .unwrap();
    assert_eq!(io_var(&engine, inst, "scoped"), None);
    assert_eq!(
        engine
            .instance(inst)
            .unwrap()
            .scope_variables
            .get(&sub_scope)
            .and_then(|m| m.get("scoped")),
        Some(&Value::Int(42))
    );
}

#[test]
fn abstract_task_passes_through_to_completion() {
    // An abstract `task` has no execution semantics: it must behave as a
    // pass-through, so a `start -> task -> end` process runs to completion on
    // instance creation, with no job ever created (Zeebe/C8 parity). Before
    // support was added, the flow into the task dangled and deploy failed with
    // "unknown target element".
    let def = ProcessBuilder::new("passthrough")
        .start_event("start")
        .task("do-something")
        .end_event("end")
        .connect("start", "do-something")
        .connect("do-something", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance("passthrough"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(
        events.contains(&Event::ProcessInstanceCompleted { instance_key }),
        "an abstract task must pass through so the instance completes immediately"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "do-something"
        )),
        "the abstract task element should complete as a pass-through"
    );
    // No job is ever emitted for an abstract task.
    assert!(
        engine
            .activate_jobs("do-something", "W", 10, 1_000, 0)
            .is_empty(),
        "an abstract task must not create a job"
    );
}

#[test]
fn inflight_by_process_tracks_create_and_terminal_transitions() {
    // ADR-0020 Tier-2 signal L_P: per-definition in-flight instance count,
    // maintained at the logical lifecycle (create +1, terminal −1), with a
    // monotonic created counter feeding λ_P.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(task_with_priority("orders", "10")))
        .unwrap();
    assert!(
        engine.backlog_by_process().is_empty(),
        "no definitions with live instances yet"
    );

    let a = create_instance_key(&mut engine, "orders");
    let _b = create_instance_key(&mut engine, "orders");
    let snap = engine.backlog_by_process();
    let (_, inflight, created) = snap.iter().find(|(p, _, _)| p == "orders").unwrap();
    assert_eq!(*inflight, 2, "two live instances");
    assert_eq!(*created, 2, "two cumulative creates");

    // Complete instance a's single job → a reaches its end event → Completed,
    // so its in-flight count drops to 1 while `created` stays monotonic.
    let job = engine
        .activate_jobs("work", "W", 10, 1_000, 0)
        .into_iter()
        .find(|j| j.instance_key == a)
        .unwrap();
    engine
        .apply_command(Command::complete_job(job.key))
        .unwrap();
    let snap = engine.backlog_by_process();
    let (_, inflight, created) = snap.iter().find(|(p, _, _)| p == "orders").unwrap();
    assert_eq!(*inflight, 1, "one instance completed");
    assert_eq!(*created, 2, "created is monotonic across completion");
}

#[test]
fn redeploying_an_identical_definition_is_idempotent() {
    // A byte-for-byte identical redeploy of the latest version reuses its
    // identity: no ProcessDeployed event, no new key, no version bump.
    let mut engine = Engine::new();
    let first = engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let deployed: Vec<_> = first
        .iter()
        .filter(|e| matches!(e, Event::ProcessDeployed { .. }))
        .collect();
    assert_eq!(deployed.len(), 1, "first deploy registers the definition");
    let (first_key, first_version) = match deployed[0] {
        Event::ProcessDeployed {
            process_definition_key,
            version,
            ..
        } => (*process_definition_key, *version),
        _ => unreachable!(),
    };
    assert_eq!(first_version, 1);

    // Redeploy the exact same definition twice more.
    let second = engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let third = engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    // Since issue #47 (Option B) every deploy emits a single DeploymentCreated
    // to guarantee the response envelope carries a valid LongKey — the
    // *idempotent* invariant is now specifically that no ProcessDeployed is
    // emitted (no new version, no new state).
    for events in [&second, &third] {
        assert_eq!(
            events.len(),
            1,
            "identical redeploy emits only the DeploymentCreated envelope event",
        );
        assert!(
            matches!(events[0], Event::DeploymentCreated { .. }),
            "the sole event on an identical redeploy is DeploymentCreated",
        );
    }

    // State still holds exactly the original version and key.
    let current = engine.state().processes.get("order").unwrap();
    assert_eq!(current.version, 1, "version is not bumped");
    assert_eq!(current.key, first_key, "key is reused");
}

#[test]
fn redeploying_a_changed_definition_bumps_the_version() {
    // A different model under the same id is a new version (not idempotent).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let changed = ProcessBuilder::new("order")
        .start_event("start")
        .service_task("charge", "payment")
        .service_task("ship", "shipping")
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "ship")
        .connect("ship", "end")
        .build()
        .unwrap();
    let events = engine
        .apply_command(Command::DeployProcess(changed))
        .unwrap();
    let version = events
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed { version, .. } => Some(*version),
            _ => None,
        })
        .expect("a changed definition is deployed as a new version");
    assert_eq!(version, 2);
    assert_eq!(engine.state().processes.get("order").unwrap().version, 2);
}

#[test]
fn resident_variable_bytes_counts_scope_local_maps() {
    // The resident-footprint gauge must include non-root scope-local maps, which
    // stay resident through a variable spill. A nested-scope instance therefore
    // reports MORE than the same root payload alone, and spilling the root leaves
    // the scope-local bytes still attributed as resident.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert!(!engine.instance(inst).unwrap().scope_variables.is_empty());

    let with_scope = engine.resident_variable_bytes();
    assert!(with_scope > 0);

    // After shedding the root, the scope-local bytes are still counted resident.
    let _ = engine.spill_variables(inst).expect("spillable");
    let scope_only = engine.resident_variable_bytes();
    assert!(
        scope_only > 0,
        "scope-local maps stay resident and keep contributing bytes after a root spill"
    );
    assert!(
        scope_only < with_scope,
        "shedding the root payload reduces the resident footprint"
    );
}

#[test]
fn completing_an_instance_drops_its_variables_from_hot_state() {
    // ADR 0012: a terminal instance's variables are never read from hot state
    // again, so completion drops the payload immediately (heap reclaimed without
    // waiting for exporter-driven eviction). The instance shell remains resident
    // (queryable) until eviction, but carries no variables.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(!engine.instance(key).unwrap().variables.is_empty());

    // Run the single service task to completion.
    complete_one(&mut engine, "payment");

    let instance = engine.instance(key).expect("shell still resident");
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed,
        "instance is terminal"
    );
    assert!(
        instance.variables.is_empty(),
        "a completed instance holds no variables in hot state"
    );
    assert_eq!(
        engine.resident_variable_bytes(),
        0,
        "terminal instance contributes no resident variable bytes"
    );
}

#[test]
fn terminating_an_instance_drops_its_variables_from_hot_state() {
    // ADR 0012: cancellation (→ Terminated) also drops the payload.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("k".to_string(), Value::Int(7));
    let events = engine
        .apply_command(Command::create_instance_with("order", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(!engine.instance(key).unwrap().variables.is_empty());

    engine
        .apply_command(Command::cancel_instance(key))
        .expect("cancel");

    let instance = engine.instance(key).expect("shell still resident");
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(
        instance.variables.is_empty(),
        "a terminated instance holds no variables in hot state"
    );
}

#[test]
fn suspend_drops_message_correlation_and_does_not_rebuffer() {
    use crate::state::ProcessInstanceState;
    // A suspended instance makes no progress, and the message model is
    // unbuffered — so a message correlated to a suspended instance is DROPPED
    // (not buffered) and does not re-correlate on resume. This is the
    // documented drop-on-suspend suspension semantics for messages, mirroring
    // the gate jobs and timers already have.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();

    let events = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Parked on the message catch with one open subscription.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Suspend, then publish the matching message: it correlates NOTHING while
    // suspended and the subscription stays open (the message is dropped).
    engine
        .apply_command_at(Command::suspend_instance(instance_key), 1_000)
        .expect("suspend");
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Suspended
    );
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 1_500);
    assert!(
        !fired
            .iter()
            .any(|e| matches!(e, Event::MessageCorrelated { .. })),
        "a suspended instance does not correlate messages"
    );
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open,
        "the subscription stays open; the dropped message is not buffered"
    );

    // Resume: the previously dropped message does NOT re-correlate — the token
    // is still parked on the catch.
    engine
        .apply_command_at(Command::resume_instance(instance_key), 2_000)
        .expect("resume");
    assert!(
        !engine.is_completed(instance_key),
        "the message dropped during suspension does not re-correlate on resume"
    );
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // A fresh matching message after resume correlates normally and completes.
    let fired = engine.correlate_message("payment-received", "A", HashMap::new(), 2_500);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn suspend_resume_reject_illegal_transitions() {
    use crate::state::ProcessInstanceState;
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Unknown instance: both suspend and resume are a clean not-found.
    assert!(matches!(
        engine.apply_command(Command::suspend_instance(999)),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    ));
    assert!(matches!(
        engine.apply_command(Command::resume_instance(999)),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    ));

    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Resuming an Active instance is an idempotent no-op (no error, no event).
    let noop = engine.apply_command(Command::resume_instance(key)).unwrap();
    assert!(noop.is_empty());
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Active
    );

    // Suspend, then suspending again is an idempotent no-op.
    engine
        .apply_command(Command::suspend_instance(key))
        .unwrap();
    let noop = engine
        .apply_command(Command::suspend_instance(key))
        .unwrap();
    assert!(noop.is_empty());

    engine.apply_command(Command::resume_instance(key)).unwrap();

    // Cancel to a terminal state, then neither transition is valid.
    engine.apply_command(Command::cancel_instance(key)).unwrap();
    assert_eq!(
        engine.instance(key).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(matches!(
        engine.apply_command(Command::suspend_instance(key)),
        Err(EngineError::InstanceTransitionInvalid {
            instance_key,
            to: "SUSPENDED",
            ..
        }) if instance_key == key
    ));
    assert!(matches!(
        engine.apply_command(Command::resume_instance(key)),
        Err(EngineError::InstanceTransitionInvalid {
            instance_key,
            to: "ACTIVE",
            ..
        }) if instance_key == key
    ));
}

#[cfg(feature = "serde")]
#[test]
fn engine_snapshot_round_trips_state_and_key_generator() {
    // A state snapshot must reproduce the materialized state exactly and
    // resume the key generator where it left off, so a node rebuilt from a
    // snapshot serves identical state and never mints a colliding key — the
    // correctness contract for bounded, state-based Raft snapshots.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    // One instance driven to completion (terminal; pruned from hot state but
    // retained for audit)...
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    complete_one(&mut engine, "payment");
    // ...and one left parked on its job (live working state).
    let e2 = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let parked = e2.iter().find_map(|e| e.instance_key()).unwrap();

    let snapshot = engine.snapshot();
    let serialized = serde_json::to_vec(&snapshot).expect("snapshot serializes");
    let decoded: EngineSnapshot =
        serde_json::from_slice(&serialized).expect("snapshot deserializes");
    let mut restored = Engine::from_snapshot(decoded);

    assert_eq!(
        restored.state(),
        engine.state(),
        "restored state equals the source state byte-for-byte"
    );

    // The parked instance and its job survive the round-trip and remain
    // completable on the restored engine.
    assert!(restored.instance(parked).is_some());
    let done = complete_one(&mut restored, "payment");
    assert!(done.iter().any(|e| matches!(
        e,
        Event::ProcessInstanceCompleted { instance_key } if *instance_key == parked
    )));

    // The restored engine resumes minting keys without colliding with any key
    // the source engine already assigned.
    let e3 = restored
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let k3 = e3.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(
        !engine.state().instances.contains_key(&k3),
        "next minted key {k3} must not collide with a pre-snapshot key"
    );
    assert_ne!(k3, parked);
}

#[cfg(feature = "serde")]
#[test]
fn engine_snapshot_round_trips_scope_variables_and_parents() {
    // Part C snapshot compatibility: a nested-scope instance's scope tree
    // (`scope_variables` + `scope_parents`) must survive a serialized snapshot
    // round-trip so a node rebuilt from a snapshot serves the same merged view.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(subprocess_with_input_mapping(
            Vec::new(),
        )))
        .unwrap();
    let mut vars = HashMap::new();
    vars.insert("seed".to_string(), Value::Int(4));
    let inst = engine
        .apply_command(Command::create_instance_with("sub-scope", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let eik = engine.pending_jobs()[0].element_instance_key;
    // The sub-process scope is populated (input mapping created `scoped`).
    assert!(!engine.instance(inst).unwrap().scope_variables.is_empty());
    assert!(!engine.instance(inst).unwrap().scope_parents.is_empty());
    let merged_before = engine.element_variables(inst, eik);

    let serialized = serde_json::to_vec(&engine.snapshot()).expect("serializes");
    let decoded: EngineSnapshot = serde_json::from_slice(&serialized).expect("deserializes");
    let restored = Engine::from_snapshot(decoded);

    assert_eq!(
        restored.state(),
        engine.state(),
        "restored state (scope tree included) equals the source byte-for-byte"
    );
    // The merged scoped view is reproduced exactly on the restored engine.
    assert_eq!(restored.element_variables(inst, eik), merged_before);
    assert_eq!(
        restored.element_variables(inst, eik).get("scoped"),
        Some(&Value::Int(5))
    );
}

#[test]
fn cold_spill_round_trips_a_message_parked_instance() {
    // The long-lived case variable spill deliberately skips: an instance
    // parked on a message intermediate catch (no job at all). Cold spill lifts
    // it out wholesale and rehydration restores the subscription so a later
    // correlated message still resumes and completes it.
    let def = ProcessBuilder::new("wait")
        .start_event("start")
        .message_intermediate_catch_event("await", "approve", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("orderId".to_string(), Value::Str("A".to_string()));
    let events = engine
        .apply_command(Command::create_instance_with("wait", vars))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // Parked on a message, no job: still a cold-spill candidate.
    assert_eq!(engine.cold_spillable_instances(10), vec![key]);
    let snapshot = engine.snapshot_instance(key).expect("snapshottable");
    assert_eq!(snapshot.message_subscriptions.len(), 1);
    assert!(engine.instance(key).is_none());
    // While cold the subscription is out of hot state entirely.
    assert!(engine.state().message_subscriptions.is_empty());

    engine.rehydrate_instance(snapshot);
    // Now the subscription is back: correlation resumes and completes it.
    let correlated = engine
        .apply_command(Command::correlate_message("approve", "A"))
        .unwrap();
    assert!(correlated.iter().any(
        |e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)
    ));
}

#[test]
fn feel_message_name_resolves_on_activation() {
    // The message name is a FEEL expression referencing an instance variable;
    // Zeebe evaluates it when the subscription opens (on activation).
    let def = ProcessBuilder::new("await-payment")
        .start_event("start")
        .message_intermediate_catch_event("await", "=\"payment-\" + region", "orderId")
        .end_event("end")
        .connect("start", "await")
        .connect("await", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[
                ("orderId", Value::Str("A".into())),
                ("region", Value::Str("eu".into())),
            ]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The subscription opened under the resolved name, not the raw expression.
    let subs = engine.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].message_name, "payment-eu");
    assert_eq!(subs[0].correlation_key, "A");

    // A message for a different region does not correlate.
    let fired = engine.correlate_message("payment-us", "A", HashMap::new(), 0);
    assert!(!fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(!engine.is_completed(instance_key));

    // The resolved name correlates and completes the instance.
    let fired = engine.correlate_message("payment-eu", "A", HashMap::new(), 0);
    assert!(fired
        .iter()
        .any(|e| matches!(e, Event::MessageCorrelated { .. })));
    assert!(engine.is_completed(instance_key));
}

#[test]
fn bpmn_rejects_linked_resource_missing_required_attributes() {
    // Zeebe's design-time validator requires `resourceId`, `bindingType` and
    // `resourceType` on every linkedResource and rejects the deployment
    // (INVALID_ARGUMENT -> HTTP 400) when any is absent. Nano must match that
    // parity: a link missing any required attribute is a hard parse error, not
    // a silent drop that leaves the task's `linkedResources` header empty at
    // activation. Guards the whole defect *class* (each required attribute).
    let bpmn_with = |linked: &str| {
        format!(
            r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="p">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="agent">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="run-agent" />
              <zeebe:linkedResources>
                {linked}
              </zeebe:linkedResources>
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="agent" />
          <bpmn:sequenceFlow id="b" sourceRef="agent" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#
        )
    };

    // Each of the three Zeebe-required attributes, when omitted, is rejected
    // with an `InvalidLinkedResource` error naming the offending task and
    // attribute — never silently dropped.
    for (attribute, link) in [
        (
            "resourceType",
            r#"<zeebe:linkedResource resourceId="x.md" bindingType="latest" linkName="prompt" />"#,
        ),
        (
            "resourceId",
            r#"<zeebe:linkedResource bindingType="latest" resourceType="GenericScript" linkName="prompt" />"#,
        ),
        (
            "bindingType",
            r#"<zeebe:linkedResource resourceId="x.md" resourceType="GenericScript" linkName="prompt" />"#,
        ),
    ] {
        let err = crate::bpmn::parse_bpmn(&bpmn_with(link))
            .expect_err(&format!("missing {attribute} must be rejected"));
        match err {
            crate::bpmn::ParseError::InvalidLinkedResource {
                ref task_id,
                attribute: ref attr,
            } => {
                assert_eq!(task_id, "agent");
                assert_eq!(attr, attribute, "error names the missing attribute");
            }
            other => panic!("expected InvalidLinkedResource for {attribute}, got {other:?}"),
        }
        // The actionable message matches the issue's requested wording.
        assert_eq!(
            err.to_string(),
            format!("linkedResource on 'agent' is missing required attribute '{attribute}'")
        );
    }

    // An empty attribute value is treated as absent (Zeebe's `hasNonEmptyAttribute`).
    let err = crate::bpmn::parse_bpmn(&bpmn_with(
        r#"<zeebe:linkedResource resourceId="x.md" bindingType="latest" resourceType="" linkName="prompt" />"#,
    ))
    .expect_err("empty resourceType must be rejected");
    assert!(matches!(
        err,
        crate::bpmn::ParseError::InvalidLinkedResource { .. }
    ));

    // A fully-specified linkedResource (Zeebe's required set present) parses,
    // and `linkName` is optional per Zeebe's validator: its absence resolves to
    // an empty link name rather than a rejection.
    let def = crate::bpmn::parse_bpmn(&bpmn_with(
        r#"<zeebe:linkedResource resourceId="ok.md" bindingType="latest" resourceType="GenericScript" />"#,
    ))
    .unwrap()
    .pop()
    .unwrap();
    let ElementKind::ServiceTask {
        linked_resources, ..
    } = &def.element("agent").unwrap().kind
    else {
        panic!("agent is a service task");
    };
    assert_eq!(linked_resources.len(), 1);
    assert_eq!(linked_resources[0].resource_id, "ok.md");
    assert_eq!(linked_resources[0].resource_type, "GenericScript");
    assert_eq!(linked_resources[0].link_name, "");
}

// zeebe-cells: element:ScriptTask
#[test]
fn inline_script_task_evaluates_feel_and_writes_result_variable() {
    // A zeebe:script script task evaluates its FEEL expression on activation,
    // stores the result under resultVariable, and passes straight through with
    // no job. A downstream service task parks the token so the result is still
    // observable in hot state (ADR 0012 clears variables only on completion).
    let def = ProcessBuilder::new("scripted")
        .start_event("start")
        .script_task("calc", "=a + b", "sum")
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance_with(
            "scripted",
            vars(&[("a", Value::Int(2)), ("b", Value::Int(3))]),
        ))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // The script did not create a job; only the downstream service task did.
    assert_eq!(engine.state().jobs.len(), 1);
    let job = engine.state().jobs.values().next().unwrap();
    assert_eq!(job.job_type, "do-work");

    // The token advanced past the script (parked at the service task) and the
    // computed result is present under the declared resultVariable.
    assert!(!engine.is_completed(key));
    assert_eq!(
        engine.instance(key).unwrap().variables.get("sum"),
        Some(&Value::Int(5))
    );
}

#[test]
fn inline_script_task_completes_the_instance_when_terminal() {
    // A script task with no downstream work completes the instance immediately
    // (pass-through), like an intermediate throw event.
    let def = ProcessBuilder::new("scripted-end")
        .start_event("start")
        .script_task("calc", "=x * 2", "doubled")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let events = engine
        .apply_command(Command::create_instance_with(
            "scripted-end",
            vars(&[("x", Value::Int(21))]),
        ))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.state().jobs.len(), 0);
    assert!(engine.is_completed(key));
}

#[test]
fn inline_script_task_result_is_visible_to_output_mappings() {
    // Zeebe merges resultVariable first, then applies output mappings; a
    // zeebe:output on the script task can therefore reference the result.
    let def = ProcessBuilder::new("scripted-out")
        .start_event("start")
        .script_task("calc", "=a + b", "sum")
        .with_io(
            "calc",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "=sum".to_string(),
                    target: "total".to_string(),
                }],
            },
        )
        .service_task("work", "do-work")
        .end_event("end")
        .connect("start", "calc")
        .connect("calc", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let created = engine
        .apply_command(Command::create_instance_with(
            "scripted-out",
            vars(&[("a", Value::Int(4)), ("b", Value::Int(5))]),
        ))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine.instance(key).unwrap().variables.get("total"),
        Some(&Value::Int(9))
    );
}

/// Cancelling an instance whose catch subscription is canonically placed on
/// another partition emits a routable `MessageSubscriptionClosing` (carrying
/// the message name + correlation key the host needs to address the message
/// partition), and the local placeholder transitions to `Canceled`. The
/// canonical record is then disarmed by routing a `CloseMessageSubscription`
/// to the message partition, which settles its own copy.
#[test]
fn cross_partition_cancel_emits_a_routable_closing() {
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

    // Park the instance on partition 0 with a cross-partition Opening, then
    // open the canonical subscription on partition 1.
    let created = instance_engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str(order.clone()))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
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
            } => Some(Command::OpenMessageSubscription {
                subscription_key: *subscription_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: element_id.clone(),
                message_name: message_name.clone(),
                correlation_key: correlation_key.clone(),
                kind: kind.clone(),
            }),
            _ => None,
        })
        .expect("an Opening was emitted");
    message_engine.apply_command(opening).unwrap();
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    // Cancel on the instance partition: a routable Closing is emitted for the
    // off-partition placeholder, carrying the routing payload.
    let canceled = instance_engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();
    let closing = canceled
        .iter()
        .find_map(|e| match e {
            Event::MessageSubscriptionClosing {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
            } => Some(Command::CloseMessageSubscription {
                subscription_key: *subscription_key,
                instance_key: *instance_key,
                element_instance_key: *element_instance_key,
                element_id: {
                    assert_eq!(message_name, "payment-received");
                    assert_eq!(correlation_key, &order);
                    element_id.clone()
                },
            }),
            _ => None,
        })
        .expect("a routable Closing was emitted for the cross-partition sub");
    assert!(
        !canceled
            .iter()
            .any(|e| matches!(e, Event::MessageSubscriptionCanceled { .. })),
        "the off-partition placeholder routes a Closing, not a local Canceled"
    );
    assert_eq!(
        instance_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );

    // Host routes the Close to the message partition, disarming the canonical
    // record so a later publish correlates nothing.
    message_engine.apply_command(closing).unwrap();
    assert_eq!(
        message_engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
    let published = message_engine
        .apply_command(Command::correlate_message_with(
            "payment-received",
            order.clone(),
            HashMap::new(),
        ))
        .unwrap();
    assert!(
        !published
            .iter()
            .any(|e| matches!(e, Event::RemoteMessageCorrelation { .. })),
        "the disarmed canonical sub correlates nothing"
    );
}

#[test]
fn should_publish_a_message_with_no_subscription() {
    let mut engine = Engine::new();
    // With nothing subscribed, a published message is minted and dropped.
    let fired = engine.correlate_message("nobody-home", "X", HashMap::new(), 0);
    assert_eq!(fired.len(), 1);
    assert!(matches!(fired[0], Event::MessagePublished { .. }));
    assert!(engine.message_subscriptions().is_empty());
}

#[test]
fn should_merge_message_variables_on_correlation() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let fired = engine.correlate_message(
        "payment-received",
        "A",
        vars(&[("amount", Value::Int(42))]),
        0,
    );

    // The message payload is merged into the instance — the merge is carried on
    // the durable `VariablesUpdated` event (the exporter's source of truth). The
    // instance then runs to completion, which drops its hot-state variables
    // (ADR 0012), so the merged value is asserted on the event, not hot state.
    let merged = fired
        .iter()
        .find_map(|e| match e {
            Event::VariablesUpdated {
                instance_key: k,
                variables,
            } if *k == instance_key => variables.get("amount").cloned(),
            _ => None,
        })
        .expect("correlation emits a VariablesUpdated carrying the payload");
    assert_eq!(merged, Value::Int(42));
    assert!(engine.is_completed(instance_key));
    assert!(
        engine.state().instances[&instance_key].variables.is_empty(),
        "a completed instance holds no variables in hot state (ADR 0012)"
    );
}

#[test]
fn should_correlate_only_the_instance_with_the_matching_key() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let a = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("A".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let b = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("B".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Correlating "A" releases only instance A; B stays parked.
    engine.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(engine.is_completed(a));
    assert!(!engine.is_completed(b));
}

#[test]
fn should_recover_an_open_message_subscription_via_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(process_with_message_catch()))
            .unwrap(),
    );
    log.extend(
        engine
            .apply_command(Command::create_instance_with(
                "await-payment",
                vars(&[("orderId", Value::Str("A".into()))]),
            ))
            .unwrap(),
    );

    // Replay: the open subscription and its parked token survive.
    let mut recovered = Engine::replay(log);
    let subs = recovered.message_subscriptions();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].state, state::MessageSubscriptionState::Open);
    let instance_key = subs[0].instance_key;
    assert!(!recovered.is_completed(instance_key));

    // The recovered engine correlates the message and completes the instance.
    recovered.correlate_message("payment-received", "A", HashMap::new(), 0);
    assert!(recovered.is_completed(instance_key));
}

// zeebe-cells: element:EndEvent element:StartEvent event:end:none
#[test]
fn should_complete_immediately_when_no_task() {
    let def = ProcessBuilder::new("noop")
        .start_event("s")
        .end_event("e")
        .connect("s", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("noop"))
        .unwrap();

    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(engine.is_completed(instance_key));
    assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
}

// zeebe-cells: element:ParallelGateway
#[test]
fn should_run_parallel_split_and_join() {
    // s -> split =< a, b >= join -> e   (a and b are service tasks)
    let def = ProcessBuilder::new("par")
        .start_event("s")
        .parallel_gateway("split")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "split")
        .connect("split", "a")
        .connect("split", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("par"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    // given both branches forked and both tasks are waiting
    assert_eq!(engine.pending_jobs().len(), 2);
    assert!(!engine.is_completed(instance_key));

    // when the first branch's job completes, the join must still wait
    complete_one(&mut engine, "ja");
    assert!(!engine.is_completed(instance_key));

    // when the second branch completes, the join fires and the instance ends
    let final_events = complete_one(&mut engine, "jb");
    assert!(engine.is_completed(instance_key));
    assert!(final_events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    // exactly one ProcessInstanceCompleted across the whole run
    assert_eq!(
        final_events
            .iter()
            .filter(|e| matches!(e, Event::ProcessInstanceCompleted { .. }))
            .count(),
        1
    );
}

#[test]
fn should_run_inclusive_split_taking_every_matching_flow() {
    // s -> isplit =< a (when x), b (when y), c (when z) >= join -> e
    // With x=true, y=true, z=false the split takes exactly a and b (an inclusive
    // OR: every flow whose condition holds), not c.
    let def = ProcessBuilder::new("inc")
        .start_event("s")
        .inclusive_gateway("isplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .service_task("c", "jc")
        .inclusive_gateway("join")
        .end_event("e")
        .connect("s", "isplit")
        .connect_when("isplit", "a", "x")
        .connect_when("isplit", "b", "y")
        .connect_when("isplit", "c", "z")
        .connect("a", "join")
        .connect("b", "join")
        .connect("c", "join")
        .connect("join", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([
        ("x".to_string(), Value::Bool(true)),
        ("y".to_string(), Value::Bool(true)),
        ("z".to_string(), Value::Bool(false)),
    ]);
    let created = engine
        .apply_command(Command::create_instance_with("inc", vars))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Exactly the two matching branches forked; `c` never activated.
    assert!(created
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "a")));
    assert!(created
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "b")));
    assert!(!created
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "c")));
    assert_eq!(engine.pending_jobs().len(), 2);
    assert!(!engine.is_completed(instance_key));

    // The join must wait until BOTH taken branches arrive — not fire on the
    // first, and not wait for the untaken `c` branch that has no token.
    complete_one(&mut engine, "ja");
    assert!(!engine.is_completed(instance_key));

    let final_events = complete_one(&mut engine, "jb");
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        final_events
            .iter()
            .filter(|e| matches!(e, Event::ProcessInstanceCompleted { .. }))
            .count(),
        1
    );
}

#[test]
fn should_take_inclusive_split_default_when_no_condition_matches() {
    // No non-default flow's condition holds, so the explicit default is taken.
    let def = ProcessBuilder::new("inc-def")
        .start_event("s")
        .inclusive_gateway("isplit")
        .end_event("hot")
        .end_event("cold")
        .connect("s", "isplit")
        .connect_when("isplit", "hot", "temp > 100")
        .connect_default("isplit", "cold")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = HashMap::from([("temp".to_string(), Value::Int(20))]);
    let events = engine
        .apply_command(Command::create_instance_with("inc-def", vars))
        .unwrap();

    assert!(events
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "cold")));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "hot")));
}

#[test]
fn chained_inclusive_joins_do_not_fire_downstream_join_prematurely() {
    // #1168 regression (queued-activation reachability). Two inclusive joins in a
    // chain, `j1 -> j2`, where `j2` also has an independent incoming branch `c`.
    // Firing `j1` only *queues* its token toward `j2` (state.active is not yet
    // updated). If `j2`'s guard ignored queued activations it would see no live
    // token reaching it and fire prematurely on an incomplete set (just `c`),
    // then reopen when the queued `j1` token arrives. Zeebe counts in-transit
    // flows as active paths (`activeSequenceFlowIds`); `has_active_path_to`
    // counts queued `Step::Activate`s the same way (#1241). The instance must
    // complete cleanly, once.
    let def = ProcessBuilder::new("inc-chain")
        .start_event("s")
        .parallel_gateway("psplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .service_task("c", "jc")
        .inclusive_gateway("j1")
        .inclusive_gateway("j2")
        .end_event("e")
        .connect("s", "psplit")
        .connect("psplit", "a")
        .connect("psplit", "b")
        .connect("psplit", "c")
        .connect("a", "j1")
        .connect("b", "j1")
        .connect("j1", "j2")
        .connect("c", "j2")
        .connect("j2", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("inc-chain"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.pending_jobs().len(), 3);

    // Complete all three tasks; the last one drives both joins to fire.
    let mut all_events = Vec::new();
    all_events.extend(complete_one(&mut engine, "ja"));
    all_events.extend(complete_one(&mut engine, "jb"));
    all_events.extend(complete_one(&mut engine, "jc"));

    assert!(
        engine.is_completed(instance_key),
        "the chained joins must synchronise and complete the instance"
    );
    assert!(engine.active_incidents().is_empty());
    assert_eq!(
        all_events
            .iter()
            .filter(|e| matches!(e, Event::ProcessInstanceCompleted { .. }))
            .count(),
        1,
        "the instance must complete exactly once"
    );
    assert_eq!(
        all_events
            .iter()
            .filter(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "e"))
            .count(),
        1,
        "the downstream join must route to the end exactly once (no premature/duplicate fire)"
    );
}

#[test]
fn inclusive_join_waits_while_a_link_throw_that_reaches_it_is_pending() {
    // #1168 regression (link-event reachability). An inclusive join `j` is
    // reachable from branch `a` (normal flow `a -> j`) AND from branch `b` via a
    // link hop: `b -> thr` (link *throw*) hands its token directly to the
    // matching same-scope link *catch* `cat` (no sequence flow), and `cat -> j`.
    // Because the throw→catch transition is invisible to the sequence-flow graph,
    // a token parked on `b` (upstream of the throw) is NOT a sequence-flow
    // predecessor of `j` — only the link edge makes `b` reach `j`. When `a`
    // arrives at the open join while `b` is still parked on its job, the
    // join's guard must NOT fire `j`: the pending link hop could yet route a
    // token in, and firing early would then create a second, late arrival at `j`.
    let def = ProcessBuilder::new("inc-link")
        .start_event("s")
        .inclusive_gateway("isplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .link_intermediate_throw_event("thr", "L")
        .link_intermediate_catch_event("cat", "L")
        .inclusive_gateway("j")
        .end_event("e")
        .connect("s", "isplit")
        .connect("isplit", "a")
        .connect("isplit", "b")
        .connect("a", "j")
        .connect("b", "thr")
        .connect("cat", "j")
        .connect("j", "e")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("inc-link"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(engine.pending_jobs().len(), 2);

    // Complete branch `a`; its token reaches the open inclusive join `j`. Branch
    // `b` is still parked on its job, and `b` reaches `j` through the pending link
    // hop, so `j` must not fire yet.
    let after_a = complete_one(&mut engine, "ja");
    assert!(
        !after_a
            .iter()
            .any(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "e")),
        "the inclusive join must not fire while `b` can still reach it via the link throw"
    );
    assert!(
        !engine.is_completed(instance_key),
        "the instance must still be running with `b` parked and the join held open"
    );

    // Complete branch `b`; its token flows `b -> thr -> (link) -> cat -> j`. Now no
    // live token can reach `j` other than the two that arrived — it fires once and
    // routes to the end exactly once (no premature fire, no duplicate).
    let after_b = complete_one(&mut engine, "jb");
    assert!(
        engine.is_completed(instance_key),
        "once `b` arrives through the link hop, the join fires and the instance completes"
    );
    assert_eq!(
        after_b
            .iter()
            .filter(|e| matches!(e, Event::SequenceFlowTaken { to, .. } if to == "e"))
            .count(),
        1,
        "the join routes to the end exactly once (no premature fire, no duplicate)"
    );
    assert!(engine.active_incidents().is_empty());
}

#[test]
fn terminate_end_kills_sibling_branch_and_completes_the_instance() {
    // s -> split =< work (service task), trigger (service task) -> stop (terminate end) >
    //
    // A parallel split forks two branches: `work` parks on a job while
    // `trigger` reaches a terminate end. Completing `trigger` must kill the
    // still-active `work` token (cancelling its job) and COMPLETE the whole
    // top-level instance — Zeebe parity (#1085): the terminate end kills every
    // inner token, but the process instance's own terminal record is
    // `ProcessInstanceCompleted` (`PROCESS -> ELEMENT_COMPLETED`), not
    // `ProcessInstanceTerminated`. It must still not degrade to a plain end that
    // leaves `work` running.
    let def = ProcessBuilder::new("term")
        .start_event("s")
        .parallel_gateway("split")
        .service_task("work", "work-job")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("s", "split")
        .connect("split", "work")
        .connect("split", "trigger")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("term"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Both branches forked; both tasks are parked on jobs.
    assert_eq!(engine.pending_jobs().len(), 2);
    assert!(!engine.is_completed(instance_key));
    let work_job = engine.activate_jobs("work-job", "w", 1, 60_000, 0)[0].key;

    // Completing the trigger job drives its token into the terminate end.
    let events = complete_one(&mut engine, "trigger-job");

    // The terminate end kills the sibling `work` token (its job cancelled) and
    // completes the whole instance — the instance's terminal record is
    // COMPLETED (Zeebe parity), not TERMINATED.
    assert!(events.contains(&Event::JobCanceled {
        job_key: work_job,
        instance_key,
    }));
    assert!(events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
    assert!(engine.is_completed(instance_key));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Completed
    );
    // No token survives: the instance holds no active elements after completion.
    assert!(engine.instance(instance_key).unwrap().active.is_empty());
    // The cancelled sibling job can no longer be completed.
    let err = engine
        .apply_command(Command::complete_job(work_job))
        .unwrap_err();
    assert_eq!(err, EngineError::JobNotActive { job_key: work_job });
}

#[test]
fn subprocess_terminate_end_confines_to_the_subprocess_scope() {
    // A terminate end inside an embedded sub-process kills only that
    // sub-process's tokens and lets the parent instance continue on the
    // sub-process's outgoing flow — it does NOT terminate the whole instance.
    //
    //  start -> sub -> after (service task) -> done
    //  sub: sub_start -> split =< inner_work (svc), inner_trigger (svc) -> inner_stop (terminate end) >
    let def = ProcessBuilder::new("sub-term")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .service_task("inner_work", "inner-work-job")
        .contained_in("inner_work", "sub")
        .service_task("inner_trigger", "inner-trigger-job")
        .contained_in("inner_trigger", "sub")
        .terminate_end_event("inner_stop")
        .contained_in("inner_stop", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split")
        .connect("split", "inner_work")
        .connect("split", "inner_trigger")
        .connect("inner_trigger", "inner_stop")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-term"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Inside the sub-process both inner branches are parked on jobs.
    assert_eq!(engine.pending_jobs().len(), 2);
    let inner_work_job = engine.activate_jobs("inner-work-job", "w", 1, 60_000, 0)[0].key;

    // Completing the inner trigger drives its token into the terminate end.
    let events = complete_one(&mut engine, "inner-trigger-job");

    // The inner sibling token is killed (its job cancelled)...
    assert!(events.contains(&Event::JobCanceled {
        job_key: inner_work_job,
        instance_key,
    }));
    // ...and the sub-process completes and continues on its outgoing flow —
    // the parent instance is NOT terminated.
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "after"
    )));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })));
    assert!(!engine.is_completed(instance_key));
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        crate::state::ProcessInstanceState::Active
    );

    // The parent proceeds normally: completing the `after` task ends the
    // instance by ordinary completion, not termination.
    let final_events = complete_one(&mut engine, "after-job");
    assert!(final_events.contains(&Event::ProcessInstanceCompleted { instance_key }));
    assert!(engine.is_completed(instance_key));
}

/// Defect-class guard (dead-scope guard, `Step::Activate` branch): a scoped
/// terminate tears down every descendant token but deliberately leaves the
/// enclosing sub-process token active until the post-drain completion sweep. A
/// sibling branch's `Step::Activate` still queued in the same drain must NOT be
/// allowed to recreate a token inside that already-terminated scope (it would
/// keep the sub-process from ever draining — a wedge). The sub-process key is
/// still in `active`, so the guard needs the per-drain `torn_down_scopes` marker,
/// not just the `active`/MI/ad-hoc maps.
#[test]
fn dead_scope_guard_rejects_activation_into_a_torn_down_subprocess_scope() {
    // start -> sub -> after, with an inner task so `sub` is a live active scope.
    let def = ProcessBuilder::new("sub-live")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "inner-job")
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .service_task("after", "after-job")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-live"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // `sub` is active (its inner task is parked on a job): find its element
    // instance key — the scope the inner tokens run in.
    let sub_eik = *engine
        .instance(key)
        .unwrap()
        .active
        .iter()
        .find(|(_, element_id)| *element_id == "sub")
        .map(|(eik, _)| eik)
        .expect("sub scope is active");

    let sibling_activation = Step::Activate {
        instance_key: key,
        element_id: "inner".to_string(),
        scope: sub_eik,
        via: None,
    };

    // While the scope is live the guard admits the activation.
    assert!(
        !engine.step_targets_dead_scope(&sibling_activation),
        "a live sub-process scope must admit a queued activation"
    );

    // Once the scope is marked torn down for this drain, the guard rejects the
    // still-queued sibling activation even though the sub-process token is still
    // in `active` (left for the drain sweep to complete).
    engine.torn_down_scopes.insert(sub_eik);
    assert!(
        engine.step_targets_dead_scope(&sibling_activation),
        "a queued activation into a torn-down scope must be dropped, not recreate a token that wedges the drain"
    );

    // A root-scoped activation (scope 0) is never gated by this marker.
    assert!(!engine.step_targets_dead_scope(&Step::Activate {
        instance_key: key,
        element_id: "after".to_string(),
        scope: 0,
        via: None,
    }));
}

/// How many times the parallel join `join` fired (routed its outgoing flow).
fn join_fire_count(events: &[Event], join: &str) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, Event::SequenceFlowTaken { from, .. } if from == join))
        .count()
}

/// `s -> fork =< inner, t >;  inner =< x, y >;  x, y -> merge (xor) -> join;
/// t -> join;  join -> e`. Two tokens reach `join` over the SAME incoming flow
/// (`merge -> join`) while `t` is still pending. Found by the TLA+ token-flow
/// model (`formal/tla/MCParallelJoinMultiArrival`, #1233).
fn multi_arrival_process() -> ProcessDefinition {
    ProcessBuilder::new("multi_arrival")
        .start_event("s")
        .parallel_gateway("fork")
        .parallel_gateway("inner")
        .service_task("x", "jx")
        .service_task("y", "jy")
        .exclusive_gateway("merge")
        .service_task("t", "jt")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "inner")
        .connect("fork", "t")
        .connect("inner", "x")
        .connect("inner", "y")
        .connect("x", "merge")
        .connect("y", "merge")
        .connect("merge", "join")
        .connect("t", "join")
        .connect("join", "e")
        .build()
        .unwrap()
}

#[test]
fn parallel_join_waits_for_every_incoming_flow_not_just_enough_arrivals() {
    // A BPMN parallel join activates only once every incoming flow has been
    // taken; Zeebe counts distinct taken flows
    // (`ProcessInstanceStateTransitionGuard.canActivateParallelGateway`), so two
    // arrivals over one flow must not fire it while `t` is still pending (#1233).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_arrival_process()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("multi_arrival"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let first = complete_one(&mut engine, "jx");
    let second = complete_one(&mut engine, "jy");
    assert_eq!(
        join_fire_count(&first, "join") + join_fire_count(&second, "join"),
        0,
        "parallel join fired on two arrivals over one incoming flow while `t` is still pending"
    );
    assert!(!engine.is_completed(instance_key));

    let third = complete_one(&mut engine, "jt");
    assert_eq!(
        join_fire_count(&third, "join"),
        1,
        "the join fires once `t` arrives"
    );
}

#[test]
fn parallel_join_keeps_surplus_token_waiting_like_zeebe() {
    // Zeebe consumes ONE taken-flow count per incoming flow on activation and
    // keeps the remainder for the next activation (the "Tetris principle",
    // `ProcessInstanceElementActivatingV3Applier`). The surplus `merge -> join`
    // token is still an active sequence flow, so the instance does not complete:
    // it waits at the join for another `t` token (#1233).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(multi_arrival_process()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("multi_arrival"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    complete_one(&mut engine, "jx");
    complete_one(&mut engine, "jy");
    complete_one(&mut engine, "jt");

    assert!(
        !engine.is_completed(instance_key),
        "the surplus token keeps the instance alive, as in Zeebe"
    );
    let instance = engine.instance(instance_key).unwrap();
    assert_eq!(instance.join_flows_taken("join"), 1);
    let open = instance
        .join_instances
        .get("join")
        .copied()
        .expect("the join is re-opened to hold the surplus token");
    assert_eq!(
        instance.active.get(&open).map(String::as_str),
        Some("join"),
        "the re-opened join is a live token"
    );
}

#[test]
fn parallel_join_surplus_fires_again_once_every_flow_is_retaken() {
    // s -> fork =< pa, pb >;  pa =< a1, a2 > -> xa (xor) -> join;
    //                         pb =< b1, b2 > -> xb (xor) -> join;  join -> e
    // Each incoming flow of `join` is taken twice. Zeebe fires the join twice:
    // once per complete set, keeping `xa -> join`'s second token for the second
    // activation. Dropping the surplus would strand the instance (#1233).
    let def = ProcessBuilder::new("surplus")
        .start_event("s")
        .parallel_gateway("fork")
        .parallel_gateway("pa")
        .parallel_gateway("pb")
        .service_task("a1", "ja1")
        .service_task("a2", "ja2")
        .service_task("b1", "jb1")
        .service_task("b2", "jb2")
        .exclusive_gateway("xa")
        .exclusive_gateway("xb")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "pa")
        .connect("fork", "pb")
        .connect("pa", "a1")
        .connect("pa", "a2")
        .connect("pb", "b1")
        .connect("pb", "b2")
        .connect("a1", "xa")
        .connect("a2", "xa")
        .connect("b1", "xb")
        .connect("b2", "xb")
        .connect("xa", "join")
        .connect("xb", "join")
        .connect("join", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("surplus"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let mut fired = 0;
    for job in ["ja1", "ja2"] {
        fired += join_fire_count(&complete_one(&mut engine, job), "join");
    }
    assert_eq!(
        fired, 0,
        "two tokens on `xa -> join` alone must not fire it"
    );
    fired += join_fire_count(&complete_one(&mut engine, "jb1"), "join");
    assert_eq!(fired, 1);
    assert!(!engine.is_completed(instance_key));
    let last = complete_one(&mut engine, "jb2");
    fired += join_fire_count(&last, "join");
    assert_eq!(fired, 2, "the kept surplus completes the second activation");
    assert!(engine.is_completed(instance_key));
}

#[test]
fn parallel_join_counts_duplicate_flows_between_the_same_elements_separately() {
    // Two distinct sequence flows `fork -> join`. They are different incoming
    // flows (Zeebe keys by flow id), so one token on each fires the join.
    let def = ProcessBuilder::new("dup")
        .start_event("s")
        .parallel_gateway("fork")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "join")
        .connect("fork", "join")
        .connect("join", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("dup"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(join_fire_count(&events, "join"), 1);
    assert!(engine.is_completed(instance_key));
}

#[test]
fn terminate_clears_open_parallel_join_bookkeeping() {
    // A terminate that fires while a parallel join is half-open must drop the
    // join's runtime bookkeeping (`join_counts`/`join_instances`) on the terminal
    // instance — the same forced-teardown cleanup as MI/ad-hoc state. Otherwise a
    // mid-join terminate strands this bookkeeping on the terminal instance shell
    // until eviction, leaving its snapshot inconsistent.
    //
    //  start -> split =< a(svc) -> join, b(svc) -> join,
    //                    trigger(svc) -> stop(terminate) >
    //  join(parallel) -> done
    let proc = ProcessBuilder::new("join-term")
        .start_event("start")
        .parallel_gateway("split")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .parallel_gateway("join")
        .end_event("done")
        .service_task("trigger", "trigger-job")
        .terminate_end_event("stop")
        .connect("start", "split")
        .connect("split", "a")
        .connect("split", "b")
        .connect("split", "trigger")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "done")
        .connect("trigger", "stop")
        .build()
        .unwrap();

    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(proc)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("join-term"))
        .unwrap();
    let key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Drive branch `a` into the join so it half-opens and waits for branch `b`.
    complete_one(&mut engine, "ja");
    let instance = engine.instance(key).unwrap();
    assert_eq!(
        instance.join_flows_taken("join"),
        1,
        "the join is half-open before the terminate"
    );
    assert!(instance.join_instances.contains_key("join"));

    // The sibling terminate fires when `trigger` completes, ending the instance.
    complete_one(&mut engine, "trigger-job");
    let instance = engine.instance(key).unwrap();
    assert_eq!(
        instance.state,
        crate::state::ProcessInstanceState::Completed
    );
    assert!(
        instance.join_counts.is_empty()
            && instance.join_flow_arrivals.is_empty()
            && instance.join_instances.is_empty(),
        "the terminal instance must not retain open-join bookkeeping"
    );
}

// zeebe-cells: element:ExclusiveGateway element:SequenceFlow
#[test]
fn should_route_exclusive_gateway_by_variable() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(approval_process()))
        .unwrap();

    let vars = HashMap::from([("decision".to_string(), Value::Str("yes".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "approved"
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

#[test]
fn should_take_default_flow_when_no_condition_matches() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(approval_process()))
        .unwrap();

    let vars = HashMap::from([("decision".to_string(), Value::Str("no".into()))]);
    let events = engine
        .apply_command(Command::create_instance_with("approval", vars))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    assert!(engine.is_completed(instance_key));
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "rejected"
    )));
}

#[test]
fn should_route_exclusive_gateway_on_a_numeric_feel_comparison() {
    // A richer FEEL condition than equality: amount > 100 -> big ; else small.
    let def = ProcessBuilder::new("amounts")
        .start_event("s")
        .exclusive_gateway("g")
        .end_event("big")
        .end_event("small")
        .connect("s", "g")
        .connect_when("g", "big", "amount > 100")
        .connect("g", "small")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();

    let vars = HashMap::from([("amount".to_string(), Value::Int(250))]);
    let events = engine
        .apply_command(Command::create_instance_with("amounts", vars))
        .unwrap();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { to, .. } if to == "big"
    )));
}

#[test]
fn should_leave_form_key_none_when_the_form_is_not_deployed() {
    // A user task whose formId references a form that was never deployed: the
    // task is still created, but with no resolved form key.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="noform-proc">
          <bpmn:startEvent id="s" />
          <bpmn:userTask id="review">
            <bpmn:extensionElements>
              <zeebe:userTask />
              <zeebe:formDefinition formId="missing-form" />
            </bpmn:extensionElements>
          </bpmn:userTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
          <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("noform-proc"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .expect("user task created");
    assert_eq!(engine.state().user_tasks[&user_task_key].form_key, None);
}

#[test]
fn should_reject_reassigning_an_assigned_task_without_override() {
    let def = ProcessBuilder::new("approval")
        .start_event("start")
        .user_task("review")
        .end_event("end")
        .connect("start", "review")
        .connect("review", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("approval"))
        .unwrap();
    let user_task_key = created
        .iter()
        .find_map(|e| match e {
            Event::UserTaskCreated { user_task_key, .. } => Some(*user_task_key),
            _ => None,
        })
        .unwrap();

    // First assignment succeeds.
    engine
        .apply_command(Command::assign_user_task(user_task_key, "alice"))
        .unwrap();
    // A non-override reassignment is rejected while assigned.
    assert!(matches!(
        engine.apply_command(Command::AssignUserTask {
            user_task_key,
            assignee: "bob".to_string(),
            allow_override: false,
        }),
        Err(EngineError::UserTaskAlreadyAssigned { .. })
    ));
    // Unassigning then assigning again works.
    engine
        .apply_command(Command::unassign_user_task(user_task_key))
        .unwrap();
    assert_eq!(engine.state().user_tasks[&user_task_key].assignee, None);
    engine
        .apply_command(Command::AssignUserTask {
            user_task_key,
            assignee: "bob".to_string(),
            allow_override: false,
        })
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&user_task_key]
            .assignee
            .as_deref(),
        Some("bob")
    );
}

#[test]
fn should_set_variables_via_an_element_instance_scope_key() {
    // given a service task parked with a known element instance key
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let element_instance_key = engine.pending_jobs()[0].element_instance_key;
    let instance_key = engine.pending_jobs()[0].instance_key;

    // when variables are set against the element instance key (not the
    // process instance key)
    engine
        .apply_command(Command::set_variables(
            element_instance_key,
            HashMap::from([("x".to_string(), Value::Int(42))]),
        ))
        .unwrap();

    // then they land in the owning instance's single variable scope
    assert_eq!(
        engine.instance(instance_key).unwrap().variables.get("x"),
        Some(&Value::Int(42))
    );
}

#[test]
fn should_reject_setting_variables_on_an_unknown_scope() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::set_variables(
            404,
            HashMap::from([("x".to_string(), Value::Int(1))]),
        ))
        .unwrap_err();
    assert_eq!(err, EngineError::ScopeNotFound { scope_key: 404 });
}

#[test]
fn should_run_an_embedded_subprocess_to_completion_on_the_happy_path() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            process_with_subprocess_error_boundary(),
        ))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("sub-error"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // The token enters the sub-process and parks on its inner service task.
    assert!(!engine.is_completed(instance_key));
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].job_type, "work");

    // Completing the inner job drains the sub-process scope, which then
    // routes out its normal outgoing flow to the outer end event.
    let events = complete_one(&mut engine, "work");
    assert!(events.iter().any(|e| matches!(
        e,
        Event::SequenceFlowTaken { from, to, .. } if from == "sub" && to == "done"
    )));
    assert!(engine.is_completed(instance_key));
    assert!(engine.instance(instance_key).unwrap().incidents.is_empty());
}

#[test]
fn should_reject_unknown_process() {
    let mut engine = Engine::new();
    let err = engine
        .apply_command(Command::create_instance("missing"))
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::ProcessNotFound {
            process_id: "missing".into()
        }
    );
}

#[test]
fn recover_lease_restores_a_soft_lease_and_holds_redelivery_until_the_deadline() {
    // given a newly-promoted leader that has the job in Created state (it
    // replicated the create but, under leader-local activation, never saw the
    // previous leader's activation) and a digested lease (key, deadline=1000)
    let mut engine = Engine::new();
    engine.set_lenient_completion(true);
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // when it recovers the lease from the digest at t=0
    assert!(engine.recover_lease(job_key, 1_000, 0));

    // then the job is no longer activatable (held until the deadline), so a
    // worker activation before the deadline gets nothing
    assert!(engine.pending_jobs().is_empty());
    assert!(engine
        .activate_jobs("payment", "W", 10, 1_000, 500)
        .is_empty());

    // and once the original deadline passes, the leader-local expiry tick
    // reclaims it and it is redelivered (at-least-once, honouring the deadline)
    engine.expire_jobs(1_500);
    let reactivated = engine.activate_jobs("payment", "W", 10, 1_000, 1_500);
    assert_eq!(reactivated.len(), 1);
    assert_eq!(reactivated[0].key, job_key);
}

#[test]
fn recover_lease_is_idempotent_and_respects_an_expired_deadline() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;

    // a lease whose deadline has already passed is not recovered
    assert!(!engine.recover_lease(job_key, 1_000, 1_000));
    assert_eq!(engine.pending_jobs().len(), 1);

    // a live lease is recovered and surfaces in activated_leases
    assert!(engine.recover_lease(job_key, 2_000, 1_000));
    assert_eq!(engine.activated_leases(), vec![(job_key, 2_000)]);

    // recovering again on an already-activated job is a no-op
    assert!(!engine.recover_lease(job_key, 3_000, 1_000));
    assert_eq!(engine.activated_leases(), vec![(job_key, 2_000)]);

    // an unknown key is a no-op
    assert!(!engine.recover_lease(999_999, 5_000, 1_000));
}

#[test]
fn should_reject_a_lock_extension_once_the_lock_has_expired() {
    // given a job whose activation lock has expired (back in the pool, Created)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    engine.expire_jobs(1_500);

    // when its (now non-existent) lock is extended
    let err = engine
        .apply_command_at(Command::update_job_timeout(job_key, 5_000), 1_600)
        .unwrap_err();

    // Camunda validates the requested property after its live-job gate.
    assert_eq!(
        err,
        EngineError::JobUpdateInvalid {
            job_key,
            reason: "timeout requires an active job deadline".into(),
        }
    );
}

/// `Command` is persisted through the Raft log (`ReplicatedCommand` serializes
/// `Command`). New fields on persisted variants must (a) deserialize from logs
/// written before the field existed (`serde(default)`) and (b) stay off the wire
/// in their empty/absent form (`skip_serializing_if`) so the documented
/// "byte-unchanged" claim holds. This guards the whole defect class for the
/// three fields added in #592: `ThrowJobError.variables`,
/// `UpdateJobRetries.operation_reference`, `UpdateJobTimeout.operation_reference`.
#[cfg(feature = "serde")]
#[test]
fn new_persisted_command_fields_are_backward_compatible_and_omit_when_empty() {
    use std::collections::HashMap;

    // (a) Old log entries lack the new fields — they must still deserialize.
    let throw: Command = serde_json::from_str(
        r#"{"ThrowJobError":{"job_key":7,"error_code":"E","error_message":"m"}}"#,
    )
    .expect("legacy ThrowJobError without variables must deserialize");
    assert!(matches!(
        throw,
        Command::ThrowJobError { ref variables, .. } if variables.is_empty()
    ));

    let retries: Command =
        serde_json::from_str(r#"{"UpdateJobRetries":{"job_key":7,"retries":3}}"#)
            .expect("legacy UpdateJobRetries without operation_reference must deserialize");
    assert!(matches!(
        retries,
        Command::UpdateJobRetries {
            operation_reference: None,
            ..
        }
    ));

    let timeout: Command =
        serde_json::from_str(r#"{"UpdateJobTimeout":{"job_key":7,"timeout":5000}}"#)
            .expect("legacy UpdateJobTimeout without operation_reference must deserialize");
    assert!(matches!(
        timeout,
        Command::UpdateJobTimeout {
            operation_reference: None,
            ..
        }
    ));

    // (b) The empty/absent form must not appear on the wire (byte-unchanged).
    let throw_empty = Command::ThrowJobError {
        lease_token: None,
        job_key: 7,
        error_code: "E".into(),
        error_message: "m".into(),
        variables: HashMap::new(),
    };
    let s = serde_json::to_string(&throw_empty).unwrap();
    assert!(
        !s.contains("variables"),
        "empty variables must be skipped: {s}"
    );

    let s = serde_json::to_string(&Command::update_job_retries(7, 3)).unwrap();
    assert!(
        !s.contains("operation_reference"),
        "absent operation_reference must be skipped: {s}"
    );

    let s = serde_json::to_string(&Command::update_job_timeout(7, 5000)).unwrap();
    assert!(
        !s.contains("operation_reference"),
        "absent operation_reference must be skipped: {s}"
    );

    // Present values still round-trip.
    let s = serde_json::to_string(&Command::update_job_retries_with_ref(7, 3, Some(42))).unwrap();
    assert!(s.contains("operation_reference"));
    let back: Command = serde_json::from_str(&s).unwrap();
    assert!(matches!(
        back,
        Command::UpdateJobRetries {
            operation_reference: Some(42),
            ..
        }
    ));
}

/// Class-scoped companion to the above: variables on an UNHANDLED thrown error
/// (one that raises an incident rather than being caught) must NOT be seeded —
/// there is no catch scope to instantiate them at.
#[test]
fn should_not_seed_variables_when_a_thrown_error_is_unhandled() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_error_boundary_to_task()))
        .unwrap();
    let created = engine
        .apply_command(Command::create_instance("payment-recover"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    let job_key = engine.activate_jobs("payment", "w", 1, 60_000, 0)[0].key;

    // when an error no boundary catches is thrown with variables
    let vars = HashMap::from([("reason".to_string(), Value::Str("declined".to_string()))]);
    let events = engine
        .apply_command(Command::throw_job_error_with(
            job_key, "UNKNOWN", "boom", vars,
        ))
        .unwrap();

    // then an incident is raised and no seed variable write was emitted
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::IncidentRaised { .. })));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::VariablesUpdated { variables, .. } if variables.contains_key("reason")
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::ScopedVariablesUpdated { variables, .. } if variables.contains_key("reason")
    )));
    // the instance did not acquire the dropped variable either
    assert_eq!(
        engine
            .instance(instance_key)
            .unwrap()
            .variables
            .get("reason"),
        None
    );
}

#[test]
fn should_expire_locks_on_tick() {
    // given an activated (locked) job
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.activate_jobs("payment", "A", 10, 1_000, 0)[0].key;
    assert!(engine.pending_jobs().is_empty());

    // when a tick runs after the deadline
    engine.expire_jobs(2_000);

    // then the job is activatable again
    assert_eq!(engine.pending_jobs().len(), 1);
    assert_eq!(engine.pending_jobs()[0].key, job_key);
}

#[test]
fn should_be_deterministic_and_replayable() {
    let run = || {
        let mut engine = Engine::new();
        let mut all = Vec::new();
        all.extend(
            engine
                .apply_command(Command::DeployProcess(linear_with_task()))
                .unwrap(),
        );
        all.extend(
            engine
                .apply_command(Command::create_instance("order"))
                .unwrap(),
        );
        let job_key = engine.pending_jobs()[0].key;
        all.extend(
            engine
                .apply_command(Command::activate_jobs("payment", "worker-1", 1, 60_000, 0))
                .unwrap(),
        );
        all.extend(
            engine
                .apply_command(Command::complete_job(job_key))
                .unwrap(),
        );
        (engine, all)
    };

    let (engine_a, log_a) = run();
    let (_engine_b, log_b) = run();
    assert_eq!(log_a, log_b);

    // Replaying the log over a fresh State reconstructs engine state exactly.
    let mut replayed = State::new();
    for event in &log_a {
        state::apply(&mut replayed, event);
    }
    assert_eq!(&replayed, engine_a.state());
}

#[test]
fn should_recover_state_and_key_generator_via_replay() {
    // given a run that deploys, starts an instance, and raises an incident
    let (engine_a, log) = {
        let mut engine = Engine::new();
        let mut log = Vec::new();
        log.extend(
            engine
                .apply_command(Command::DeployProcess(linear_with_task()))
                .unwrap(),
        );
        log.extend(
            engine
                .apply_command(Command::create_instance("order"))
                .unwrap(),
        );
        let job_key = engine.pending_jobs()[0].key;
        engine
            .apply_command(Command::activate_jobs("payment", "w", 1, 60_000, 0))
            .unwrap();
        // fail with no retries -> parks the job and raises an incident
        log.extend(
            engine
                .apply_command_at(Command::fail_job(job_key, 0, "boom"), 1_234)
                .unwrap(),
        );
        (engine, log)
    };

    // when the durable log is replayed into a fresh engine
    // (activation events are volatile and intentionally not part of `log`)
    let mut recovered = Engine::replay(log);

    // then state matches (modulo the volatile activation: the replayed job
    // is parked Failed, identical to the original after fail)
    let orig_incident = engine_a.active_incidents()[0];
    let rec_incident = recovered.active_incidents()[0];
    assert_eq!(rec_incident.key, orig_incident.key);
    assert_eq!(rec_incident.created_at, 1_234);
    assert_eq!(rec_incident.kind, state::IncidentKind::JobNoRetries);

    // and the key generator resumes past every replayed key: a new instance
    // mints a strictly higher key than anything in the recovered log
    let max_existing = recovered
        .state()
        .instances
        .keys()
        .chain(recovered.state().jobs.keys())
        .chain(recovered.state().incidents.keys())
        .copied()
        .max()
        .unwrap();
    let events = recovered
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let new_instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert!(
        new_instance_key > max_existing,
        "new key {new_instance_key} must exceed replayed max {max_existing}"
    );
}

#[test]
fn message_prefers_open_subscription_over_starting_a_new_instance() {
    // Issue #1156: when a message name is subscribed by BOTH a message start
    // event and an open boundary subscription on a running instance, the open
    // subscription takes precedence — the same publish must NOT also start a
    // duplicate instance.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_message_start_and_boundary()))
        .unwrap();

    // First publish: no subscription is open yet, so the message start event
    // creates a fresh instance. It parks on the service task and opens the
    // boundary subscription (correlating on the seeded customerId).
    engine.correlate_message(
        "probe-alert",
        "C1",
        vars(&[("customerId", Value::Str("C1".into()))]),
        0,
    );
    assert_eq!(engine.state().instances.len(), 1);
    let open: Vec<_> = engine
        .message_subscriptions()
        .into_iter()
        .filter(|s| s.state == state::MessageSubscriptionState::Open)
        .collect();
    assert_eq!(open.len(), 1);
    let instance_key = open[0].instance_key;

    // Second publish: the open boundary subscription on the running instance
    // claims the message. It fires the boundary (interrupting the instance) and
    // does NOT start a second instance from the message start event.
    let fired = engine.correlate_message(
        "probe-alert",
        "C1",
        vars(&[("customerId", Value::Str("C1".into()))]),
        0,
    );
    assert_eq!(
        engine.state().instances.len(),
        1,
        "the open subscription wins; no duplicate instance is created"
    );
    assert!(
        fired.iter().any(|e| matches!(
            e,
            Event::SequenceFlowTaken { from, to, .. } if from == "bnd" && to == "interrupted"
        )),
        "the boundary event fires on the running instance"
    );
    assert!(
        !fired
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })),
        "no new instance is created by the same publish"
    );
    assert!(engine.is_completed(instance_key));
}

#[test]
fn dispatch_start_instance_mints_the_instance_locally() {
    // The command a routed `StartInstanceDispatched` becomes on the target
    // partition: it mints the start-triggered instance in that partition's
    // own key namespace and runs it.
    const N: u64 = 4;
    let mut target = Engine::with_partition(2);
    target.set_num_partitions(N);
    target
        .apply_command(Command::DeployProcess(process_with_message_start()))
        .unwrap();
    assert!(target.state().instances.is_empty());

    let fired = target
        .apply_command(Command::DispatchStartInstance {
            process_id: "order-flow".into(),
            start_element_id: "start".into(),
            variables: vars(&[("amount", Value::Int(9))]),
            tags: Vec::new(),
            business_id: None,
        })
        .unwrap();

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
        .expect("the dispatch mints an instance");
    assert_eq!(
        crate::state::partition_of(instance_key),
        2,
        "the instance is minted in the target partition's namespace"
    );
    // The dispatched variables seed the instance (carried on the durable
    // ProcessInstanceCreated event); the instance then completes and drops its
    // hot-state variables (ADR 0012), so the seed is asserted on the event.
    assert_eq!(
        seeded,
        Some(Value::Int(9)),
        "the dispatched variables seed the instance"
    );
}

#[test]
fn evicts_only_completed_instances_and_what_they_own() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // One instance that we drive to completion, and one left in-flight
    // (parked on its service-task job).
    let done = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    assert!(engine.is_completed(done));

    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert!(!engine.is_completed(live));
    // The live instance still owns an activatable job.
    assert!(engine.state().jobs.values().any(|j| j.instance_key == live));

    // Evicting an active instance is a no-op.
    assert!(!engine.evict_instance(live));
    assert!(engine.instance(live).is_some());

    // Evicting the completed one removes it and its jobs.
    assert!(engine.evict_instance(done));
    assert!(engine.instance(done).is_none());
    assert!(!engine.state().jobs.values().any(|j| j.instance_key == done));

    // The in-flight instance and its job are untouched, and the deployed
    // definition (not instance-scoped) is retained.
    assert!(engine.instance(live).is_some());
    assert!(engine.state().jobs.values().any(|j| j.instance_key == live));
    assert_eq!(engine.state().processes.len(), 1);
}

#[test]
fn evict_instances_batches_in_one_pass() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Two completed instances and one left in-flight.
    let done1 = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    let done2 = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Batch includes both completed keys, the live key (ignored, not
    // terminal) and an unknown key (ignored).
    let evicted = engine.evict_instances(&[done1, done2, live, 9_999_999]);
    assert_eq!(evicted, 2);
    assert!(engine.instance(done1).is_none());
    assert!(engine.instance(done2).is_none());
    assert!(engine.instance(live).is_some());
    assert!(!engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == done1 || j.instance_key == done2));
    assert!(engine.state().jobs.values().any(|j| j.instance_key == live));
}

/// `retire_instances` is the follower-side counterpart to `evict_instances`: it
/// drops the named instances from hot state regardless of their local lifecycle
/// state (a follower replica never sees the leader-local completion, so the
/// instance is still `Active` here), while ignoring keys that are absent.
#[test]
fn retire_instances_drops_active_shells_and_ignores_absent() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Two in-flight (Active, with a Created job) instances — this models a
    // follower replica that applied the CreateInstance but never the completion.
    let a = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let b = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let keep = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Both `a` and `b` are Active, not terminal — `evict_instances` must NOT
    // touch them, proving the leak they cause.
    assert_eq!(engine.evict_instances(&[a, b]), 0);
    assert!(engine.instance(a).is_some());

    // `retire_instances` drops the two named Active shells (and their jobs),
    // ignores the unknown key, and leaves the untouched instance intact.
    let retired = engine.retire_instances(&[a, b, 9_999_999]);
    assert_eq!(retired, 2);
    assert!(engine.instance(a).is_none());
    assert!(engine.instance(b).is_none());
    assert!(engine.instance(keep).is_some());
    assert!(!engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == a || j.instance_key == b));
    assert!(engine.state().jobs.values().any(|j| j.instance_key == keep));
}

/// The retire-before-create race: under leader-durable the leader completes and
/// broadcasts a retirement digest before the async learner has applied the
/// instance's `CreateInstance`. `retire_instances` then finds the key absent and
/// must tombstone it so the instance is reaped the moment its create materializes
/// — otherwise it leaks as a never-retired `Active` shell.
#[test]
fn retire_before_create_tombstones_and_reaps_on_arrival() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Minting is deterministic for an identical command sequence, but a create
    // consumes several local keys (instance + job + tokens), so the instance key
    // is not simply `prev + 1`. Learn the key the create WILL mint from a twin
    // engine driven with the identical sequence.
    let future_key = {
        let mut twin = Engine::new();
        twin.apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
        twin.apply_command(Command::create_instance("order"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap()
    };

    // Retire `future_key` BEFORE it exists on this engine, exactly as a digest
    // that raced ahead of the replicated create.
    assert_eq!(
        engine.retire_instances(&[future_key]),
        0,
        "nothing present yet"
    );
    assert!(engine.instance(future_key).is_none());

    // Now the create arrives and mints exactly that key. It must be reaped
    // immediately, not left Active.
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(created, future_key, "the create mints the tombstoned key");
    assert!(
        engine.instance(created).is_none(),
        "the tombstoned instance is reaped on arrival (no leak)"
    );
    assert!(!engine
        .state()
        .jobs
        .values()
        .any(|j| j.instance_key == created));

    // A subsequent, untombstoned create is unaffected.
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert!(engine.instance(live).is_some());
}

/// The loss-tolerant reconciliation backstop: `retire_below` drops every resident
/// replica instance below the owner's low-water mark, and `retirement_low_water`
/// ignores the owner's own resident terminal shells so the mark keeps advancing.
#[test]
fn retire_below_reconciles_follower_to_owner_low_water() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Model a follower replica that applied five creates but no retirements.
    let mut keys = Vec::new();
    for _ in 0..5 {
        keys.push(
            engine
                .apply_command(Command::create_instance("order"))
                .unwrap()
                .iter()
                .find_map(|e| e.instance_key())
                .unwrap(),
        );
    }
    keys.sort_unstable();
    assert!(keys.iter().all(|k| engine.instance(*k).is_some()));

    // The owner's low-water mark sits just above the 3rd instance: everything below
    // it has terminated on the owner. The sweep reaps exactly those (bounded).
    let low_water = keys[3];
    let reaped = engine.retire_below(low_water, 100);
    assert_eq!(
        reaped.len(),
        3,
        "reaps every resident instance below the mark"
    );
    assert!(engine.instance(keys[0]).is_none());
    assert!(engine.instance(keys[2]).is_none());
    assert!(
        engine.instance(keys[3]).is_some(),
        "the mark itself is kept"
    );
    assert!(engine.instance(keys[4]).is_some());

    // Idempotent: re-running with the same mark reaps nothing more.
    assert_eq!(engine.retire_below(low_water, 100).len(), 0);

    // The bound caps a single sweep so a huge backlog drains across ticks.
    let capped = engine.retire_below(keys[4] + 1, 1);
    assert_eq!(capped.len(), 1);
}

/// `retirement_low_water` is the smallest still-`Active` key (so a follower can
/// drop everything below it), and the next mintable key once nothing is active
/// (so a quiescent owner tells followers to drop their whole backlog).
#[test]
fn retirement_low_water_tracks_min_active_then_next_key() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let first = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let second = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    // Two active instances -> the mark is the smaller (oldest) key.
    assert_eq!(engine.retirement_low_water(), first.min(second));
    let older = first.min(second);
    let newer = first.max(second);

    // Activate both jobs in one pass (a second pass would find nothing left).
    let jobs = engine.activate_jobs("payment", "W", 10, 1_000, 0);
    let job_older = jobs.iter().find(|j| j.instance_key == older).unwrap().key;
    let job_newer = jobs.iter().find(|j| j.instance_key == newer).unwrap().key;

    // Complete the older instance; the mark advances past it to the newer.
    engine
        .apply_command(Command::complete_job(job_older))
        .unwrap();
    assert_eq!(engine.retirement_low_water(), newer);

    // Once nothing is active the mark is above every key ever minted, so a
    // follower drops its entire resident backlog.
    let mark_before = engine.retirement_low_water();
    engine
        .apply_command(Command::complete_job(job_newer))
        .unwrap();
    let mark_empty = engine.retirement_low_water();
    assert!(
        mark_empty > mark_before,
        "a quiescent owner's mark exceeds every active key it ever held"
    );
    assert!(mark_empty > newer);
}

#[test]
fn evict_completed_sweeps_every_finished_instance() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    for _ in 0..3 {
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        complete_one(&mut engine, "payment");
    }
    // A fourth instance left in-flight.
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    assert_eq!(engine.state().instances.len(), 4);
    let evicted = engine.evict_completed();
    assert_eq!(evicted, 3);
    assert_eq!(engine.state().instances.len(), 1);
    assert!(engine.instance(live).is_some());
}

#[test]
fn cancel_disarms_an_open_message_subscription() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(process_with_message_catch()))
        .unwrap();
    let instance_key = engine
        .apply_command(Command::create_instance_with(
            "await-payment",
            vars(&[("orderId", Value::Str("o-1".into()))]),
        ))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Open
    );

    engine
        .apply_command(Command::cancel_instance(instance_key))
        .unwrap();

    assert_eq!(
        engine.message_subscriptions()[0].state,
        state::MessageSubscriptionState::Canceled
    );
    assert_eq!(
        engine.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
}

#[test]
fn cancel_rejects_unknown_or_finished_instances() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    // Unknown key.
    assert_eq!(
        engine.apply_command(Command::cancel_instance(999)),
        Err(EngineError::InstanceNotFound { instance_key: 999 })
    );

    // A completed instance can no longer be cancelled.
    let done = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    complete_one(&mut engine, "payment");
    assert!(engine.is_completed(done));
    assert_eq!(
        engine.apply_command(Command::cancel_instance(done)),
        Err(EngineError::InstanceNotFound { instance_key: done })
    );

    // And cancelling twice fails the second time.
    let live = engine
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    engine
        .apply_command(Command::cancel_instance(live))
        .unwrap();
    assert_eq!(
        engine.apply_command(Command::cancel_instance(live)),
        Err(EngineError::InstanceNotFound { instance_key: live })
    );
}

#[test]
fn cancel_survives_replay() {
    let mut engine = Engine::new();
    let mut log = Vec::new();
    log.extend(
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap(),
    );
    let instance_key = {
        let events = engine
            .apply_command(Command::create_instance("order"))
            .unwrap();
        let k = events.iter().find_map(|e| e.instance_key()).unwrap();
        log.extend(events);
        k
    };
    log.extend(
        engine
            .apply_command(Command::cancel_instance(instance_key))
            .unwrap(),
    );

    let recovered = Engine::replay(log);
    assert_eq!(
        recovered.instance(instance_key).unwrap().state,
        ProcessInstanceState::Terminated
    );
    assert!(recovered.instance(instance_key).unwrap().active.is_empty());
    assert!(recovered
        .state()
        .jobs
        .values()
        .all(|j| j.state == state::JobState::Canceled));
}

#[test]
fn default_engine_mints_unpartitioned_keys() {
    // partition 0 keeps the historical 1,2,3,… sequence (zero regression).
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(crate::partition_of(key), 0);
    assert!(key < (1 << 51), "partition-0 keys carry no high bits");
}

#[test]
fn partitioned_engine_embeds_partition_id_in_every_key() {
    let mut engine = Engine::with_partition(3);
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    for key in events.iter().filter_map(|e| {
        // every minted key in these events belongs to partition 3
        let k = e.max_key();
        if k != 0 {
            Some(k)
        } else {
            None
        }
    }) {
        assert_eq!(
            crate::partition_of(key),
            3,
            "key {key} routes to partition 3"
        );
    }
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(crate::partition_of(instance_key), 3);
    assert!(crate::local_of(instance_key) > 0);
}

#[test]
fn keys_from_different_partitions_never_collide() {
    let mut p1 = Engine::with_partition(1);
    let mut p2 = Engine::with_partition(2);
    for e in [&mut p1, &mut p2] {
        e.apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap();
    }
    let k1 = p1
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let k2 = p2
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_ne!(k1, k2);
    assert_eq!(crate::partition_of(k1), 1);
    assert_eq!(crate::partition_of(k2), 2);
}

#[test]
fn replay_partition_recovers_local_counter_ignoring_foreign_keys() {
    // Build a partition-2 log, then replay it prefixed with a foreign
    // (partition-0) deployment event. The foreign key must NOT advance
    // partition 2's local counter, so the next minted key stays in
    // partition 2 and does not collide with the replayed instance.
    let mut p2 = Engine::with_partition(2);
    let deploy_events = p2
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let create_events = p2.apply_command(Command::create_instance("order")).unwrap();
    let replayed_key = create_events.iter().find_map(|e| e.instance_key()).unwrap();

    // A deployment minted on partition 0 (low keys) that is replicated in.
    let mut p0 = Engine::new();
    let foreign_deploy = p0
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();

    let mut log: Vec<Event> = foreign_deploy.to_vec();
    log.extend(deploy_events.iter().cloned());
    log.extend(create_events.iter().cloned());

    let mut recovered = Engine::replay_partition(2, log);
    let next_key = recovered
        .apply_command(Command::create_instance("order"))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    assert_eq!(crate::partition_of(next_key), 2);
    assert!(
        crate::local_of(next_key) > crate::local_of(replayed_key),
        "counter advanced past the replayed partition-2 key"
    );
}

#[test]
fn install_deployment_registers_definition_without_minting() {
    // Mint a deployment on partition 0, then install it on partition 5.
    // Partition 5 can create instances of it, the definition key is shared,
    // and partition 5's own key counter is untouched by the install.
    let mut p0 = Engine::new();
    let deploy_events = p0
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let def_key = deploy_events
        .iter()
        .find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        })
        .unwrap();

    let mut p5 = Engine::with_partition(5);
    p5.install_deployment(&deploy_events);
    // Definition is registered under the same shared key (partition 0).
    assert_eq!(crate::partition_of(def_key), 0);

    let events = p5.apply_command(Command::create_instance("order")).unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    // The instance is minted in partition 5, not partition 0.
    assert_eq!(crate::partition_of(instance_key), 5);
    assert_eq!(
        crate::local_of(instance_key),
        1,
        "install did not consume a local key"
    );
}

#[test]
fn cancelling_a_parent_reaps_a_child_already_mid_termination() {
    // A cascade cancel is not interruptible, so it must also sweep a child that
    // is already `Terminating` — e.g. a child whose own cancel deferred on a
    // user-task canceling listener. If the parent terminates while the child is
    // mid-drain, sweeping only `Active` children would leave the child stuck in
    // `Terminating` forever (orphaned).
    let child_def = ProcessBuilder::new("phase")
        .start_event("pstart")
        .user_task("review")
        .end_event("pend")
        .connect("pstart", "review")
        .connect("review", "pend")
        .with_task_listeners(
            "review",
            vec![crate::model::TaskListener {
                event_type: crate::model::TaskListenerEventType::Canceling,
                job_type: "onCancel".to_string(),
                retries: None,
            }],
        )
        .build()
        .unwrap();
    let mut engine = deploy_native_call(Default::default(), child_def);
    let created = engine
        .apply_command(Command::create_instance("orch"))
        .unwrap();
    let parent_key = parent_key_of(&created, "orch");
    let child_key = engine
        .state()
        .instances
        .values()
        .find(|i| i.parent_process_instance_key == Some(parent_key))
        .map(|i| i.key)
        .expect("a child instance was spawned");

    // Cancel the child directly: its canceling listener defers termination, so
    // it parks in `Terminating` waiting for the listener job to drain.
    engine
        .apply_command(Command::cancel_instance(child_key))
        .unwrap();
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminating
    );

    // Now cancel the parent. The cascade must force-complete the child's
    // deferred drain rather than leave it orphaned in `Terminating`.
    let cancel = engine
        .apply_command(Command::cancel_instance(parent_key))
        .unwrap();
    assert!(
        cancel.iter().any(|e| matches!(
            e,
            Event::ProcessInstanceTerminated { instance_key } if *instance_key == child_key
        )),
        "the cascade reaps the still-Terminating child"
    );
    assert_eq!(
        engine.instance(child_key).unwrap().state,
        crate::state::ProcessInstanceState::Terminated
    );
    assert!(engine.pending_jobs().is_empty());
}

#[cfg(feature = "serde")]
#[test]
fn dirty_var_tracking_drains_upserts_and_forgets_for_lean_snapshot() {
    use crate::model::Value;

    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine.set_track_dirty_vars(true);

    // Create two instances with variables -> both dirty.
    let mut v = std::collections::HashMap::new();
    v.insert("amount".to_string(), Value::Int(50));
    let a = engine
        .apply_command(Command::create_instance_with("order", v.clone()))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let b = engine
        .apply_command(Command::create_instance_with("order", v.clone()))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();

    let (upserts, forgets) = engine.drain_dirty_vars();
    let keys: std::collections::HashSet<Key> = upserts.iter().map(|(k, _)| *k).collect();
    assert_eq!(keys, [a, b].into_iter().collect());
    assert!(forgets.is_empty());
    // Draining clears the set.
    let (again, _) = engine.drain_dirty_vars();
    assert!(again.is_empty());

    // A spilled instance is skipped in the upserts (spill write-through owns it).
    let mut v2 = std::collections::HashMap::new();
    v2.insert("k".to_string(), Value::Int(1));
    engine
        .apply_command(Command::SetVariables {
            scope_key: a,
            variables: v2,
            local: false,
        })
        .unwrap();
    let _ = engine.spill_variables(a); // a is now spilled
    let (upserts, _) = engine.drain_dirty_vars();
    assert!(
        !upserts.iter().any(|(k, _)| *k == a),
        "spilled instance must not appear in checkpoint upserts"
    );

    // Lean snapshot carries empty variable maps; a full one carries payloads.
    let lean = engine.snapshot_control_only();
    assert!(lean
        .state
        .instances
        .values()
        .all(|i| i.variables.is_empty()));
    let full = engine.snapshot();
    assert!(full
        .state
        .instances
        .get(&b)
        .is_some_and(|i| !i.variables.is_empty()));

    // Restoring variables from the store onto a lean-recovered engine.
    let mut recovered = Engine::from_snapshot(lean);
    let mut restored = std::collections::HashMap::new();
    restored.insert("amount".to_string(), Value::Int(99));
    recovered.install_variables(b, restored);
    assert_eq!(
        recovered
            .instance(b)
            .and_then(|i| i.variables.get("amount")),
        Some(&Value::Int(99))
    );
}

#[test]
fn static_input_mapping_source_is_passed_through_as_literal_not_feel() {
    // #1160: a `zeebe:input` `source` WITHOUT a leading `=` is a STATIC literal
    // string (Zeebe parity), not a FEEL expression. Before the fix, engine-wasm
    // 0.9.0 evaluated every source as FEEL, so `in-process` parsed as the FEEL
    // subtraction `in - process` (incident "- not defined for null and null")
    // and `{{secrets.FOO}}` as a malformed context ("expected a context key").
    // Each must now merge verbatim, no incident, and the job must be offered.
    let def = ProcessBuilder::new("io-static")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![
                    crate::model::Mapping {
                        source: "in-process".to_string(),
                        target: "mode".to_string(),
                    },
                    crate::model::Mapping {
                        source: "{{secrets.CAMUNDA_PROVIDED_LLM_API_ENDPOINT}}".to_string(),
                        target: "endpoint".to_string(),
                    },
                    crate::model::Mapping {
                        source: "openaiCompatible".to_string(),
                        target: "provider".to_string(),
                    },
                    // A static literal is passed through VERBATIM — significant
                    // leading/trailing whitespace must be preserved, not trimmed.
                    crate::model::Mapping {
                        source: "  spaced value  ".to_string(),
                        target: "padded".to_string(),
                    },
                    // A leading `=` still selects FEEL evaluation.
                    crate::model::Mapping {
                        source: "=1 + 1".to_string(),
                        target: "sum".to_string(),
                    },
                ],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let key = create_instance_key(&mut engine, "io-static");

    assert!(
        engine.active_incidents().is_empty(),
        "static ioMapping sources must not raise incidents: {:?}",
        engine.active_incidents()
    );
    assert!(!engine.is_completed(key));

    // The activated job sees each mapped value: literals verbatim, FEEL evaluated.
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(
        job.variables.get("mode"),
        Some(&Value::Str("in-process".to_string()))
    );
    assert_eq!(
        job.variables.get("endpoint"),
        Some(&Value::Str(
            "{{secrets.CAMUNDA_PROVIDED_LLM_API_ENDPOINT}}".to_string()
        ))
    );
    assert_eq!(
        job.variables.get("provider"),
        Some(&Value::Str("openaiCompatible".to_string()))
    );
    assert_eq!(
        job.variables.get("padded"),
        Some(&Value::Str("  spaced value  ".to_string())),
        "a static literal source must be passed through verbatim, whitespace intact"
    );
    assert_eq!(job.variables.get("sum"), Some(&Value::Int(2)));
}

#[test]
fn static_output_mapping_source_is_passed_through_as_literal_not_feel() {
    // #1160 (output side): a `zeebe:output` `source` without a leading `=` is a
    // static literal string too, projected verbatim at completion rather than
    // evaluated as FEEL.
    let def = ProcessBuilder::new("io-static-out")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: Vec::new(),
                outputs: vec![crate::model::Mapping {
                    source: "in-process".to_string(),
                    target: "mode".to_string(),
                }],
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let inst = create_instance_key(&mut engine, "io-static-out");
    let job_key = engine.activate_jobs("work", "w", 1, 60_000, 0)[0].key;
    let events = engine
        .apply_command(Command::complete_job_with(job_key, HashMap::new()))
        .unwrap();
    assert!(
        engine.active_incidents().is_empty(),
        "static output mapping must not raise an incident"
    );
    let mapped = events.iter().any(|e| {
        matches!(
            e,
            Event::VariablesUpdated { instance_key, variables }
                if *instance_key == inst
                    && variables.get("mode") == Some(&Value::Str("in-process".to_string()))
        )
    });
    assert!(
        mapped,
        "output mapping should set mode='in-process'; {events:?}"
    );
}

#[test]
fn input_mapping_with_dotted_target_builds_nested_context() {
    // A dotted target `order.total` merges into a nested context, preserving the
    // other members of an existing `order`.
    let def = ProcessBuilder::new("io-nested")
        .start_event("s")
        .service_task("t", "work")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=price * qty".to_string(),
                    target: "order.total".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut existing_order = std::collections::BTreeMap::new();
    existing_order.insert("id".to_string(), Value::Str("A1".to_string()));
    let mut vars = HashMap::new();
    vars.insert("price".to_string(), Value::Int(3));
    vars.insert("qty".to_string(), Value::Int(4));
    vars.insert("order".to_string(), Value::Map(existing_order));
    let inst = engine
        .apply_command(Command::create_instance_with("io-nested", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("id".to_string(), Value::Str("A1".to_string()));
    expected.insert("total".to_string(), Value::Int(12));
    // The dotted input mapping builds its nested context LOCAL to the activity
    // scope, seeded from the enclosing `order`, so the activated job sees the
    // merged value while the root `order` keeps only its original members.
    let job = &engine.activate_jobs("work", "w", 1, 60_000, 0)[0];
    assert_eq!(job.variables.get("order"), Some(&Value::Map(expected)));
    let mut root_order = std::collections::BTreeMap::new();
    root_order.insert("id".to_string(), Value::Str("A1".to_string()));
    assert_eq!(io_var(&engine, inst, "order"), Some(Value::Map(root_order)));
}

#[test]
fn input_mapped_local_variable_is_not_visible_to_a_later_activity() {
    // `y = x + 1` is an input mapping on task `t`. Input mappings are LOCAL to the
    // activity scope (Zeebe semantics), so `y` is dropped when `t` completes and
    // is never visible at the root scope nor to the downstream task `u`'s job.
    let def = ProcessBuilder::new("io-local")
        .start_event("s")
        .service_task("t", "work-t")
        .with_io(
            "t",
            crate::model::IoMapping {
                inputs: vec![crate::model::Mapping {
                    source: "=x + 1".to_string(),
                    target: "y".to_string(),
                }],
                outputs: Vec::new(),
            },
        )
        .service_task("u", "work-u")
        .end_event("e")
        .connect("s", "t")
        .connect("t", "u")
        .connect("u", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let mut vars = HashMap::new();
    vars.insert("x".to_string(), Value::Int(1));
    let inst = engine
        .apply_command(Command::create_instance_with("io-local", vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap();
    // `t`'s job sees the local `y`; complete it.
    let job_t = engine.activate_jobs("work-t", "w", 1, 60_000, 0)[0].key;
    engine.apply_command(Command::complete_job(job_t)).unwrap();
    // `u` activates; its job must NOT see `y` (it was local to `t`).
    let job_u = &engine.activate_jobs("work-u", "w", 1, 60_000, 0)[0];
    assert_eq!(job_u.variables.get("y"), None);
    assert_eq!(io_var(&engine, inst, "y"), None);
}

#[test]
fn scoped_variable_resolution_walks_child_scope_to_root() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1)), ("b", Value::Int(2))]),
        },
    );
    let child: Key = 900_001;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("b", Value::Int(20)), ("c", Value::Int(3))]),
        },
    );

    let inst = engine.state.instances.get(&key).unwrap();
    // The root scope sees only its own variables.
    let root_view = engine.visible_variables(inst, key);
    assert_eq!(root_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(root_view.get("b"), Some(&Value::Int(2)));
    assert_eq!(root_view.get("c"), None);
    // The child scope shadows `b`, adds local `c`, and inherits `a` from root.
    let child_view = engine.visible_variables(inst, child);
    assert_eq!(child_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(child_view.get("b"), Some(&Value::Int(20)));
    assert_eq!(child_view.get("c"), Some(&Value::Int(3)));
}

#[test]
fn variable_propagation_updates_nearest_defining_scope_else_root() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1))]),
        },
    );
    let child: Key = 900_002;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("b", Value::Int(2))]),
        },
    );

    // Non-local merge into the child: `a` is defined only at root -> root; `b`
    // is defined locally -> child; `c` is new anywhere -> created at root.
    let writes = engine.propagate_variables(
        key,
        child,
        vars(&[
            ("a", Value::Int(9)),
            ("b", Value::Int(9)),
            ("c", Value::Int(9)),
        ]),
        false,
    );
    let mut by_scope: std::collections::HashMap<Key, Vec<String>> =
        std::collections::HashMap::new();
    for (scope, map) in writes {
        by_scope
            .entry(scope)
            .or_default()
            .extend(map.keys().cloned());
    }
    for names in by_scope.values_mut() {
        names.sort();
    }
    assert_eq!(
        by_scope.get(&key),
        Some(&vec!["a".to_string(), "c".to_string()])
    );
    assert_eq!(by_scope.get(&child), Some(&vec!["b".to_string()]));
}

#[test]
fn local_variable_write_stays_in_the_target_scope() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1))]),
        },
    );
    let child: Key = 900_003;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );

    // A local write pins every value to the child scope even though `a` is
    // defined at root (Zeebe `local=true` / input-mapping semantics).
    let writes = engine.propagate_variables(key, child, vars(&[("a", Value::Int(9))]), true);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].0, child);
    assert!(writes[0].1.contains_key("a"));
}

#[test]
fn destroying_a_scope_drops_its_local_variables() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    let child: Key = 900_004;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("c", Value::Int(3))]),
        },
    );
    assert!(engine
        .state
        .instances
        .get(&key)
        .unwrap()
        .scope_variables
        .contains_key(&child));
    apply_raw(
        &mut engine,
        Event::VariableScopeDestroyed {
            instance_key: key,
            scope_key: child,
        },
    );
    let inst = engine.state.instances.get(&key).unwrap();
    assert!(!inst.scope_variables.contains_key(&child));
    assert!(!inst.scope_parents.contains_key(&child));
}

#[test]
fn deep_nesting_resolves_each_level_through_its_nearest_shadow_to_root() {
    // root:   a=1, b=2, c=3
    // middle: shadows b=20, adds d=40   (parent = root)
    // leaf:   shadows c=300, adds e=500 (parent = middle)
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[
                ("a", Value::Int(1)),
                ("b", Value::Int(2)),
                ("c", Value::Int(3)),
            ]),
        },
    );
    let middle: Key = 910_001;
    let leaf: Key = 910_002;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: middle,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: leaf,
            parent_scope_key: middle,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: middle,
            variables: vars(&[("b", Value::Int(20)), ("d", Value::Int(40))]),
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: leaf,
            variables: vars(&[("c", Value::Int(300)), ("e", Value::Int(500))]),
        },
    );

    let inst = engine.state.instances.get(&key).unwrap();

    // The leaf sees: a from root, b from the middle shadow, c from its own
    // shadow, d inherited from middle, e local.
    let leaf_view = engine.visible_variables(inst, leaf);
    assert_eq!(leaf_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(leaf_view.get("b"), Some(&Value::Int(20)));
    assert_eq!(leaf_view.get("c"), Some(&Value::Int(300)));
    assert_eq!(leaf_view.get("d"), Some(&Value::Int(40)));
    assert_eq!(leaf_view.get("e"), Some(&Value::Int(500)));

    // The middle sees its own shadow of b, root's c (NOT the leaf's), and never
    // the leaf-local e.
    let middle_view = engine.visible_variables(inst, middle);
    assert_eq!(middle_view.get("b"), Some(&Value::Int(20)));
    assert_eq!(middle_view.get("c"), Some(&Value::Int(3)));
    assert_eq!(middle_view.get("e"), None);

    // The root is untouched by any descendant shadow.
    let root_view = engine.visible_variables(inst, key);
    assert_eq!(root_view.get("b"), Some(&Value::Int(2)));
    assert_eq!(root_view.get("c"), Some(&Value::Int(3)));
    assert_eq!(root_view.get("d"), None);
}

#[test]
fn parallel_sibling_scopes_are_isolated() {
    // Two sibling scopes under the same root each shadow root `x` with a
    // different value and add a private local. Neither sees the other's
    // binding; the root is unchanged.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("x", Value::Int(1))]),
        },
    );
    let left: Key = 920_001;
    let right: Key = 920_002;
    for sib in [left, right] {
        apply_raw(
            &mut engine,
            Event::VariableScopeCreated {
                instance_key: key,
                scope_key: sib,
                parent_scope_key: key,
            },
        );
    }
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: left,
            variables: vars(&[("x", Value::Int(10)), ("only_left", Value::Int(7))]),
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: right,
            variables: vars(&[("x", Value::Int(20)), ("only_right", Value::Int(9))]),
        },
    );

    let inst = engine.state.instances.get(&key).unwrap();
    let left_view = engine.visible_variables(inst, left);
    let right_view = engine.visible_variables(inst, right);

    assert_eq!(left_view.get("x"), Some(&Value::Int(10)));
    assert_eq!(left_view.get("only_left"), Some(&Value::Int(7)));
    assert_eq!(
        left_view.get("only_right"),
        None,
        "left cannot see the sibling's local"
    );

    assert_eq!(right_view.get("x"), Some(&Value::Int(20)));
    assert_eq!(right_view.get("only_right"), Some(&Value::Int(9)));
    assert_eq!(
        right_view.get("only_left"),
        None,
        "right cannot see the sibling's local"
    );

    // Root's `x` is unchanged by either sibling's shadow.
    assert_eq!(
        engine.visible_variables(inst, key).get("x"),
        Some(&Value::Int(1))
    );
}

#[test]
fn local_write_shadows_an_inherited_root_variable_until_the_scope_is_destroyed() {
    // Zeebe input-mapping / `local=true` semantics: a local write of a name that
    // exists only at root creates a child-scope SHADOW, leaving the root value
    // intact; destroying the scope resurfaces the root value.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("x", Value::Int(1))]),
        },
    );
    let child: Key = 930_001;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );

    // local=true pins the write to the child scope even though `x` is a root var.
    let writes = engine.propagate_variables(key, child, vars(&[("x", Value::Int(99))]), true);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].0, child);
    for event in engine.propagated_updates(key, child, vars(&[("x", Value::Int(99))]), true) {
        apply_raw(&mut engine, event);
    }

    let inst = engine.state.instances.get(&key).unwrap();
    // Child shadows x=99; root still reads x=1.
    assert_eq!(
        engine.visible_variables(inst, child).get("x"),
        Some(&Value::Int(99))
    );
    assert_eq!(
        engine.visible_variables(inst, key).get("x"),
        Some(&Value::Int(1))
    );

    apply_raw(
        &mut engine,
        Event::VariableScopeDestroyed {
            instance_key: key,
            scope_key: child,
        },
    );
    // The shadow is gone; the child key now resolves to root and reads x=1.
    let inst = engine.state.instances.get(&key).unwrap();
    assert_eq!(
        engine.visible_variables(inst, key).get("x"),
        Some(&Value::Int(1))
    );
    assert!(!inst.scope_variables.contains_key(&child));
}

// ---------------------------------------------------------------------------
// Perf guard (Part C phase: perf)
//
// The scoping model must keep the common case — a flat, root-only instance —
// free. `element_variables` (the activation-path variable resolver) must return
// the instance's *shared* root `Arc` by pointer for a root-only instance: no
// clone, no merge, no allocation, byte-identical to the pre-scoping engine. A
// nested-scope instance instead allocates a freshly merged view. Pinning the
// pointer identity is a non-flaky structural guard against a future change
// accidentally putting an allocation on the hot flat-activation path.
// ---------------------------------------------------------------------------
#[test]
fn flat_instance_activation_returns_the_shared_root_arc_without_copying() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    let key = create_instance_key(&mut engine, "order");
    apply_raw(
        &mut engine,
        Event::VariablesUpdated {
            instance_key: key,
            variables: vars(&[("a", Value::Int(1)), ("b", Value::Int(2))]),
        },
    );

    let root_arc = Arc::clone(&engine.state.instances.get(&key).unwrap().variables);

    // Flat instance: the element view is the very same allocation (zero-copy).
    let flat_view = engine.element_variables(key, key);
    assert!(
        Arc::ptr_eq(&flat_view, &root_arc),
        "flat activation must reuse the shared root Arc, not allocate a merged copy",
    );

    // Add a nested scope with a local shadow: the merged view is now a fresh
    // allocation (correctly no longer pointer-equal to root).
    let child: Key = 940_001;
    apply_raw(
        &mut engine,
        Event::VariableScopeCreated {
            instance_key: key,
            scope_key: child,
            parent_scope_key: key,
        },
    );
    apply_raw(
        &mut engine,
        Event::ScopedVariablesUpdated {
            instance_key: key,
            scope_key: child,
            variables: vars(&[("b", Value::Int(20))]),
        },
    );
    let scoped_view = engine.element_variables(key, child);
    assert!(
        !Arc::ptr_eq(&scoped_view, &root_arc),
        "a nested scope must produce a distinct merged view",
    );
    assert_eq!(scoped_view.get("a"), Some(&Value::Int(1)));
    assert_eq!(scoped_view.get("b"), Some(&Value::Int(20)));

    // The flat resolution for the root scope still returns the shared Arc even
    // after a sibling scope exists — only elements *in* a scope pay the merge.
    let still_flat = engine.element_variables(key, key);
    assert!(
        Arc::ptr_eq(&still_flat, &root_arc),
        "root-scope activation stays zero-copy even when other scopes exist",
    );
}

#[test]
fn deploy_forms_mints_a_form_key_versions_per_id_and_is_idempotent() {
    use crate::command::FormResource;
    let mut engine = Engine::new();
    let form = |schema: &str| FormResource {
        id: "greeting-form".to_string(),
        resource_name: "greeting.form".to_string(),
        schema: schema.to_string(),
    };
    let v1 = r#"{"id":"greeting-form","type":"default","components":[]}"#;

    let events = engine
        .apply_command(Command::DeployForms(vec![form(v1)]))
        .unwrap();
    let deployed = events
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed {
                form_key,
                version,
                form_id,
                schema,
                ..
            } => Some((*form_key, *version, form_id.clone(), schema.clone())),
            _ => None,
        })
        .expect("a FormDeployed event is emitted");
    assert_eq!(deployed.1, 1, "the first deploy is version 1");
    assert_eq!(deployed.2, "greeting-form");
    assert_eq!(deployed.3, v1, "the raw schema is carried on the event");
    let stored = &engine.state().forms["greeting-form"];
    assert_eq!(stored.key, deployed.0);
    assert_eq!(stored.version, 1);

    // Redeploying the identical form is a no-op (only DeploymentCreated).
    let again = engine
        .apply_command(Command::DeployForms(vec![form(v1)]))
        .unwrap();
    assert!(
        !again
            .iter()
            .any(|e| matches!(e, Event::FormDeployed { .. })),
        "an identical redeploy emits no FormDeployed"
    );
    assert_eq!(engine.state().forms["greeting-form"].version, 1);

    // A changed schema bumps the version and re-mints a key.
    let v2 = r#"{"id":"greeting-form","type":"default","components":[{"type":"textfield","key":"who"}]}"#;
    let changed = engine
        .apply_command(Command::DeployForms(vec![form(v2)]))
        .unwrap();
    let bumped = changed
        .iter()
        .find_map(|e| match e {
            Event::FormDeployed {
                form_key, version, ..
            } => Some((*form_key, *version)),
            _ => None,
        })
        .expect("a changed form redeploys");
    assert_eq!(bumped.1, 2, "a changed schema is version 2");
    assert_ne!(bumped.0, deployed.0, "a new form key is minted");
    assert_eq!(engine.state().forms["greeting-form"].version, 2);
}

#[test]
fn deploy_generic_resources_mints_a_resource_key_versions_per_id_and_is_idempotent() {
    use crate::command::GenericResource;
    let mut engine = Engine::new();
    let resource = |content: &str| GenericResource {
        resource_id: "agent-prompt.md".to_string(),
        resource_name: "agent-prompt.md".to_string(),
        content: content.to_string(),
    };
    let v1 = "# Reviewer\n\nReview the PR.";

    let events = engine
        .apply_command(Command::DeployGenericResources(vec![resource(v1)]))
        .unwrap();
    let deployed = events
        .iter()
        .find_map(|e| match e {
            Event::GenericResourceDeployed {
                resource_key,
                version,
                resource_id,
                content,
                ..
            } => Some((
                *resource_key,
                *version,
                resource_id.clone(),
                content.clone(),
            )),
            _ => None,
        })
        .expect("a GenericResourceDeployed event is emitted");
    assert_eq!(deployed.1, 1, "the first deploy is version 1");
    assert_eq!(deployed.2, "agent-prompt.md", "resource_id is the filename");
    assert_eq!(deployed.3, v1, "the raw content is carried on the event");
    let stored = &engine.state().resources["agent-prompt.md"];
    assert_eq!(stored.key, deployed.0);
    assert_eq!(stored.version, 1);

    // Redeploying the identical resource (same name + content) is a no-op.
    let again = engine
        .apply_command(Command::DeployGenericResources(vec![resource(v1)]))
        .unwrap();
    assert!(
        !again
            .iter()
            .any(|e| matches!(e, Event::GenericResourceDeployed { .. })),
        "an identical redeploy emits no GenericResourceDeployed"
    );
    assert_eq!(engine.state().resources["agent-prompt.md"].version, 1);

    // Changed content under the same filename bumps the version and re-mints a key.
    let v2 = "# Reviewer\n\nReview the PR carefully and cite lines.";
    let changed = engine
        .apply_command(Command::DeployGenericResources(vec![resource(v2)]))
        .unwrap();
    let bumped = changed
        .iter()
        .find_map(|e| match e {
            Event::GenericResourceDeployed {
                resource_key,
                version,
                ..
            } => Some((*resource_key, *version)),
            _ => None,
        })
        .expect("changed content redeploys");
    assert_eq!(bumped.1, 2, "changed content is version 2");
    assert_ne!(bumped.0, deployed.0, "a new resource key is minted");
    assert_eq!(engine.state().resources["agent-prompt.md"].version, 2);
    assert_eq!(
        engine.state().resources["agent-prompt.md"].content,
        v2,
        "the latest content is stored"
    );

    // A different filename is an independent resource (its own version 1).
    let other = engine
        .apply_command(Command::DeployGenericResources(vec![GenericResource {
            resource_id: "other-prompt.md".to_string(),
            resource_name: "other-prompt.md".to_string(),
            content: "# Other".to_string(),
        }]))
        .unwrap();
    let other_v = other
        .iter()
        .find_map(|e| match e {
            Event::GenericResourceDeployed { version, .. } => Some(*version),
            _ => None,
        })
        .expect("a new filename deploys");
    assert_eq!(other_v, 1, "a different resource_id restarts at version 1");
    assert_eq!(engine.state().resources.len(), 2);
}

#[test]
fn deploying_multiple_form_versions_retains_every_version_by_key() {
    // Every deployed form version is retained under its own key in
    // `form_versions`; the latest-by-id index tracks the highest version, and
    // `form_by_key` / `form_version` resolve an OLD version.
    use crate::command::FormResource;
    let mut engine = Engine::new();
    let form = |schema: &str| FormResource {
        id: "f".to_string(),
        resource_name: "f.form".to_string(),
        schema: schema.to_string(),
    };
    let deploy = |engine: &mut Engine, schema: &str| -> (Key, i32) {
        engine
            .apply_command(Command::DeployForms(vec![form(schema)]))
            .unwrap()
            .iter()
            .find_map(|e| match e {
                Event::FormDeployed {
                    form_key, version, ..
                } => Some((*form_key, *version)),
                _ => None,
            })
            .expect("a changed form emits FormDeployed")
    };
    let (k1, v1) = deploy(&mut engine, r#"{"id":"f","components":[]}"#);
    let (k2, v2) = deploy(&mut engine, r#"{"id":"f","components":[{"key":"a"}]}"#);
    let (k3, v3) = deploy(&mut engine, r#"{"id":"f","components":[{"key":"b"}]}"#);
    assert_eq!((v1, v2, v3), (1, 2, 3));

    let st = engine.state();
    assert_eq!(st.form_versions.len(), 3, "all three versions are retained");
    assert_eq!(st.form_by_key(k1).unwrap().version, 1, "old version by key");
    assert_eq!(st.form_by_key(k2).unwrap().version, 2);
    assert_eq!(
        st.form_version("f", 1).unwrap().key,
        k1,
        "old by id+version"
    );
    assert_eq!(st.forms["f"].version, 3, "latest index tracks the newest");
    assert_eq!(st.forms["f"].key, k3);
}

#[test]
fn deploying_multiple_resource_versions_retains_every_version_by_key() {
    // Every deployed generic-resource version is retained under its own key in
    // `resource_versions`, resolvable via `resource_by_key` / `resource_version`
    // even though the latest-by-id index only holds the newest.
    use crate::command::GenericResource;
    let mut engine = Engine::new();
    let deploy = |engine: &mut Engine, content: &str| -> (Key, i32) {
        engine
            .apply_command(Command::DeployGenericResources(vec![GenericResource {
                resource_id: "p.md".to_string(),
                resource_name: "p.md".to_string(),
                content: content.to_string(),
            }]))
            .unwrap()
            .iter()
            .find_map(|e| match e {
                Event::GenericResourceDeployed {
                    resource_key,
                    version,
                    ..
                } => Some((*resource_key, *version)),
                _ => None,
            })
            .expect("changed content emits GenericResourceDeployed")
    };
    let (k1, _) = deploy(&mut engine, "v1");
    let (k2, _) = deploy(&mut engine, "v2");
    let (k3, _) = deploy(&mut engine, "v3");

    let st = engine.state();
    assert_eq!(st.resource_versions.len(), 3);
    assert_eq!(st.resource_by_key(k1).unwrap().content, "v1", "old by key");
    assert_eq!(st.resource_by_key(k2).unwrap().content, "v2");
    assert_eq!(st.resource_version("p.md", 1).unwrap().key, k1);
    assert_eq!(st.resources["p.md"].version, 3, "latest tracks the newest");
    assert_eq!(st.resources["p.md"].key, k3);
}

#[test]
fn deploying_multiple_drg_versions_retains_every_version_and_evaluates_old_by_key() {
    // Every DRG/decision version is retained by key; an EvaluateDecision pinned
    // to an OLD decision key resolves and evaluates that exact version — the
    // concrete regression the latest-only index could not serve.
    let mut engine = Engine::new();
    let deploy = |engine: &mut Engine| -> (Key, Key, i32) {
        // (decision_requirements_key, decision_key, version)
        let events = engine
            .apply_command(Command::DeployDecisionRequirements(vec![greeting_dmn()]))
            .unwrap();
        let drg_key = events
            .iter()
            .find_map(|e| match e {
                Event::DecisionRequirementsDeployed {
                    decision_requirements_key,
                    ..
                } => Some(*decision_requirements_key),
                _ => None,
            })
            .expect("DRG deployed");
        let (dkey, ver) = events
            .iter()
            .find_map(|e| match e {
                Event::DecisionDeployed {
                    decision_key,
                    version,
                    ..
                } => Some((*decision_key, *version)),
                _ => None,
            })
            .expect("decision deployed");
        (drg_key, dkey, ver)
    };
    // Deploy v1, then force a v2 by deploying a *changed* DRG (same id).
    let (_drg1, decision_k1, v1) = deploy(&mut engine);
    assert_eq!(v1, 1);
    // A changed DRG: swap an output value so the content differs and versions.
    let changed_xml = r##"<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="drg" name="drg">
      <decision id="greeting" name="Greeting">
        <decisionTable hitPolicy="UNIQUE">
          <input id="i1"><inputExpression id="e1" typeRef="string"><text>lang</text></inputExpression></input>
          <output id="o1" name="result" typeRef="string" />
          <rule id="r1"><inputEntry id="ie1"><text>"en"</text></inputEntry>
            <outputEntry id="oe1"><text>"hi"</text></outputEntry></rule>
        </decisionTable>
      </decision>
    </definitions>"##;
    let changed = engine
        .apply_command(Command::DeployDecisionRequirements(vec![
            crate::dmn::parse_dmn(changed_xml).unwrap(),
        ]))
        .unwrap();
    let (decision_k2, v2) = changed
        .iter()
        .find_map(|e| match e {
            Event::DecisionDeployed {
                decision_key,
                version,
                ..
            } => Some((*decision_key, *version)),
            _ => None,
        })
        .expect("changed DRG redeploys the decision");
    assert_eq!(v2, 2);
    assert_ne!(decision_k1, decision_k2);

    let st = engine.state();
    assert_eq!(st.decision_versions.len(), 2, "both versions retained");
    assert_eq!(st.decision_requirements_versions.len(), 2);
    assert_eq!(st.decision_by_key(decision_k1).unwrap().version, 1);
    assert_eq!(st.decisions["greeting"].version, 2, "latest tracks newest");

    // Evaluate the OLD version by its key: it returns "hello" (v1), whereas the
    // latest (v2) would return "hi". This is impossible without retention.
    let inputs = vars(&[("lang", Value::Str("en".to_string()))]);
    let mut evaluate = |decision: crate::command::DecisionReference| {
        let events = engine
            .apply_command(Command::EvaluateDecision {
                decision,
                variables: inputs.clone(),
            })
            .expect("deployed decision evaluates");
        match events.as_slice() {
            [Event::DecisionEvaluated {
                decision_key,
                decision_output,
                decision_evaluation_key,
                evaluated_decisions,
                ..
            }] => (
                *decision_key,
                decision_output.clone(),
                *decision_evaluation_key,
                evaluated_decisions[0].decision_version,
            ),
            other => panic!("expected one DecisionEvaluated, got {other:?}"),
        }
    };
    let (old_key, old_output, old_eval, old_version) =
        evaluate(crate::command::DecisionReference::Key(decision_k1));
    assert_eq!((old_key, old_version), (decision_k1, 1));
    assert_eq!(old_output, Value::Str("hello".to_string()));

    let (latest_key, latest_output, latest_eval, latest_version) = evaluate(
        crate::command::DecisionReference::Id("greeting".to_string()),
    );
    assert_eq!((latest_key, latest_version), (decision_k2, 2));
    assert_eq!(latest_output, Value::Str("hi".to_string()));
    assert_ne!(old_eval, latest_eval, "each evaluation mints its own key");
}

#[test]
#[cfg(feature = "serde")]
fn version_retention_survives_a_snapshot_round_trip() {
    // The `*_versions` maps are part of State, so a serde snapshot round-trip
    // preserves every retained version.
    use crate::command::{FormResource, GenericResource};
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployForms(vec![FormResource {
            id: "f".to_string(),
            resource_name: "f.form".to_string(),
            schema: r#"{"id":"f","components":[]}"#.to_string(),
        }]))
        .unwrap();
    engine
        .apply_command(Command::DeployForms(vec![FormResource {
            id: "f".to_string(),
            resource_name: "f.form".to_string(),
            schema: r#"{"id":"f","components":[{"key":"a"}]}"#.to_string(),
        }]))
        .unwrap();
    engine
        .apply_command(Command::DeployGenericResources(vec![GenericResource {
            resource_id: "p.md".to_string(),
            resource_name: "p.md".to_string(),
            content: "v1".to_string(),
        }]))
        .unwrap();
    engine
        .apply_command(Command::DeployGenericResources(vec![GenericResource {
            resource_id: "p.md".to_string(),
            resource_name: "p.md".to_string(),
            content: "v2".to_string(),
        }]))
        .unwrap();

    let serialized = serde_json::to_vec(&engine.snapshot()).unwrap();
    let restored = Engine::from_snapshot(serde_json::from_slice(&serialized).unwrap());
    let restored = restored.state();
    assert_eq!(restored.form_versions.len(), 2, "form versions round-trip");
    assert_eq!(restored.resource_versions.len(), 2, "resource versions too");
    assert_eq!(restored.forms["f"].version, 2);
    assert_eq!(restored.resources["p.md"].version, 2);
}

#[test]
fn canceling_a_task_mid_transition_clears_its_pending_state() {
    // A task deferring a `completing` transition that is force-cancelled by
    // instance termination must not be left with an unresolvable pending
    // transition (its listener job is cancelled with the instance's jobs).
    let (mut engine, inst) = deploy_and_start(user_task_with_listeners(vec![tl(
        TaskListenerEventType::Completing,
        "onComplete",
    )]));
    let key = only_user_task_key(&engine);
    engine
        .apply_command(Command::complete_user_task(key))
        .unwrap();
    // The completing chain is in flight (pending set).
    assert!(engine.state().user_tasks[&key].pending.is_some());

    // Cancelling the instance (this task has no `canceling` listener) cancels it
    // immediately and terminates.
    engine
        .apply_command(Command::cancel_instance(inst))
        .unwrap();
    assert_eq!(
        engine.state().user_tasks[&key].state,
        state::UserTaskState::Canceled
    );
    assert!(
        engine.state().user_tasks[&key].pending.is_none(),
        "a cancelled task must not retain a pending transition"
    );
    assert_eq!(
        engine.state().instances[&inst].state,
        state::ProcessInstanceState::Terminated
    );
}

#[test]
fn deploying_multiple_versions_retains_every_version_by_key() {
    // v1, v2, v3 of the same id must all be retained in `process_versions`
    // (keyed by definition key), with monotonically increasing versions and a
    // latest-index that tracks the highest version.
    let mut engine = Engine::new();
    let (k1, v1) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (k2, v2) = deploy_returning_key(&mut engine, order_with_extra_tasks(1));
    let (k3, v3) = deploy_returning_key(&mut engine, order_with_extra_tasks(2));

    assert_eq!((v1, v2, v3), (1, 2, 3), "versions increment monotonically");
    assert!(
        k1 != k2 && k2 != k3 && k1 != k3,
        "each version has a distinct definition key"
    );

    let versions = &engine.state().process_versions;
    for (k, v) in [(k1, 1), (k2, 2), (k3, 3)] {
        let d = versions
            .get(&k)
            .unwrap_or_else(|| panic!("version {v} (key {k}) is retained"));
        assert_eq!(d.version, v, "retained definition reports its own version");
    }

    // The latest-by-id index points at the highest version.
    let latest = engine.state().processes.get("order").unwrap();
    assert_eq!(latest.version, 3, "latest index tracks the newest version");
    assert_eq!(latest.key, k3);
}

#[test]
fn create_by_key_pins_instance_to_that_exact_version() {
    // Creating by the *key* of an older version must pin the instance to that
    // version even though a newer one is the latest — the key already
    // identifies the version (Zeebe by-key semantics), so any version selector
    // is irrelevant.
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (_k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1)); // v2 is latest

    let events = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            Some(k1),
            // A version selector is ignored on a by-key create; prove it does
            // not override the key's own version.
            Some(2),
        ))
        .unwrap();
    let inst_key = events.iter().find_map(|e| e.instance_key()).unwrap();

    let instance = engine.state().instances.get(&inst_key).unwrap();
    assert_eq!(
        instance.process_definition_key, k1,
        "the instance is pinned to the requested (older) version's key"
    );
    let def = engine
        .state()
        .definition_for(instance)
        .expect("pinned definition resolves");
    assert_eq!(
        def.version, 1,
        "execution resolves the pinned v1, not latest"
    );

    // The job it emits reports the pinned version, not the latest.
    let job = &engine.activate_jobs("payment", "w", 1, 60_000, 0)[0];
    assert_eq!(job.process_definition_version, 1);
    assert_eq!(job.process_definition_key, k1);
}

#[test]
fn create_by_id_and_version_selects_that_version_else_latest_else_errors() {
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1)); // v2 latest

    // Explicit version 1 → v1.
    let by_v1 = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            None,
            Some(1),
        ))
        .unwrap();
    let i1 = by_v1.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine
            .state()
            .instances
            .get(&i1)
            .unwrap()
            .process_definition_key,
        k1
    );

    // No version → latest (v2).
    let by_latest = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            None,
            None,
        ))
        .unwrap();
    let i2 = by_latest.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(
        engine
            .state()
            .instances
            .get(&i2)
            .unwrap()
            .process_definition_key,
        k2
    );

    // Unknown version → error, no instance created.
    let err = engine.apply_command(Command::create_instance_versioned(
        "order",
        HashMap::new(),
        Vec::new(),
        None,
        None,
        Some(99),
    ));
    assert!(
        matches!(err, Err(EngineError::ProcessNotFound { .. })),
        "an unknown version is rejected, not silently coerced to latest"
    );
}

#[test]
fn instance_without_a_pinned_key_resolves_to_latest() {
    // Back-compat: an instance materialized from an old snapshot (no
    // `process_definition_key`, i.e. key 0) must resolve its definition via the
    // latest-by-id index rather than failing to resolve.
    let mut engine = Engine::new();
    deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let inst_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // Simulate an old-format instance by clearing its pinned key.
    // (Field is `#[serde(default)]` → 0 on legacy snapshots.)
    let mut state = engine.state().clone();
    state
        .instances
        .get_mut(&inst_key)
        .unwrap()
        .process_definition_key = 0;
    let instance = state.instances.get(&inst_key).unwrap();
    let def = state
        .definition_for(instance)
        .expect("a key-0 instance falls back to the latest-by-id definition");
    assert_eq!(def.definition.id, "order");
}

#[test]
fn every_active_instance_pins_its_own_definition_key() {
    // Class-scoped guard: no live instance may rely on the latest-by-id index
    // for execution. Even after a newer version is deployed, previously-created
    // instances keep their original pinned key and resolve their original
    // version.
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let created = engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let inst_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    // A newer version lands *after* the instance was created.
    let (_k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1));

    for (key, instance) in &engine.state().instances {
        assert_ne!(
            instance.process_definition_key, 0,
            "instance {key} must pin a concrete definition key, not fall through to latest"
        );
    }
    let instance = engine.state().instances.get(&inst_key).unwrap();
    assert_eq!(instance.process_definition_key, k1);
    let job = &engine.activate_jobs("payment", "w", 1, 60_000, 0)[0];
    assert_eq!(
        job.process_definition_version, 1,
        "the pre-existing instance still executes v1 after v2 is deployed"
    );
}

#[cfg(feature = "serde")]
#[test]
fn snapshot_round_trip_retains_all_versions_and_instance_pins() {
    // A serialized snapshot must preserve every retained version *and* each
    // instance's pinned definition key, so a node rebuilt from a snapshot
    // resolves execution against the same versions.
    let mut engine = Engine::new();
    let (k1, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(0));
    let (k2, _) = deploy_returning_key(&mut engine, order_with_extra_tasks(1));
    let created = engine
        .apply_command(Command::create_instance_versioned(
            "order",
            HashMap::new(),
            Vec::new(),
            None,
            Some(k1),
            None,
        ))
        .unwrap();
    let inst_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let snapshot = engine.snapshot();
    let serialized = serde_json::to_vec(&snapshot).expect("snapshot serializes");
    let decoded: EngineSnapshot =
        serde_json::from_slice(&serialized).expect("snapshot deserializes");
    let restored = Engine::from_snapshot(decoded);

    assert_eq!(
        restored.state(),
        engine.state(),
        "restored state equals the source state, versions and pins included"
    );
    assert!(restored.state().process_versions.contains_key(&k1));
    assert!(restored.state().process_versions.contains_key(&k2));
    assert_eq!(
        restored
            .state()
            .instances
            .get(&inst_key)
            .unwrap()
            .process_definition_key,
        k1,
        "the instance's pinned key survives the round-trip"
    );
}

#[test]
fn subprocess_end_event_output_propagates_reached_branch_not_last_defined() {
    // Regression: several end events inside one sub-process, each carrying a
    // `zeebe:output` for the SAME target, must each attach to their own end
    // event — so when a token reaches ONE of them, only that branch's output
    // propagates to the parent scope. Previously the parser hoisted every
    // end-event mapping onto the enclosing sub-process, so the last-parsed one
    // ("escalate") clobbered the rest at sub-process completion, and a "fixed"
    // outcome routed as "escalate" (nano-workforce merge-loop #466). Driven
    // through a job so the token completes across a drain boundary, as the real
    // model does.
    let xml = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="sp-ends" isExecutable="true">
          <bpmn:startEvent id="s" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
          <bpmn:subProcess id="sub">
            <bpmn:startEvent id="ss" />
            <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="work" />
            <bpmn:serviceTask id="work">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="work" />
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="g" />
            <bpmn:exclusiveGateway id="g" default="fB" />
            <bpmn:sequenceFlow id="fA" sourceRef="g" targetRef="endA">
              <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=pick = "A"</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fB" sourceRef="g" targetRef="endB" />
            <bpmn:endEvent id="endA">
              <bpmn:extensionElements>
                <zeebe:ioMapping><zeebe:output source="=&#34;A&#34;" target="outcome" /></zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:endEvent id="endB">
              <bpmn:extensionElements>
                <zeebe:ioMapping><zeebe:output source="=&#34;B&#34;" target="outcome" /></zeebe:ioMapping>
              </bpmn:extensionElements>
            </bpmn:endEvent>
          </bpmn:subProcess>
          <bpmn:sequenceFlow id="f3" sourceRef="sub" targetRef="done" />
          <bpmn:endEvent id="done" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    // Parser-level guard: each end event owns its mapping; the sub-process owns none.
    assert!(
        def.element("sub").unwrap().io.outputs.is_empty(),
        "mapping must NOT hoist onto the sub-process"
    );
    assert_eq!(def.element("endA").unwrap().io.outputs.len(), 1);
    assert_eq!(def.element("endB").unwrap().io.outputs.len(), 1);

    for (pick, want, unwanted) in [("A", "A", "B"), ("B", "B", "A")] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(def.clone()))
            .unwrap();
        let mut vars = HashMap::new();
        vars.insert("pick".to_string(), Value::Str(pick.to_string()));
        engine
            .apply_command(Command::create_instance_with("sp-ends", vars))
            .unwrap();
        let events = complete_one(&mut engine, "work");
        assert!(
            events.iter().any(|e| matches!(e,
                Event::VariablesUpdated { variables, .. }
                    if variables.get("outcome") == Some(&Value::Str(want.to_string())))),
            "pick={pick}: reached end event must propagate outcome={want}; events: {events:?}"
        );
        assert!(
            !events.iter().any(|e| matches!(e,
                Event::VariablesUpdated { variables, .. }
                    if variables.get("outcome") == Some(&Value::Str(unwanted.to_string())))),
            "pick={pick}: unreached end event must NOT propagate outcome={unwanted}; events: {events:?}"
        );
    }
}

#[test]
fn external_agent_create_with_a_stale_lease_token_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");

    // Right job, right element, but a lease token (deadline) that does not match.
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: "stale-lease".to_string(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobLeaseMismatch { .. }),
        "a mismatched lease token must be rejected, got {err:?}"
    );
}

#[test]
fn external_agent_create_with_a_foreign_element_instance_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");

    // The job's element instance must match the CREATE's element_instance_key.
    // Assert with a different (inactive) element instance key → rejected as
    // inactive before it can borrow this job's lease.
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik + 777,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            EngineError::AgentInstanceElementInstanceInactive { .. }
                | EngineError::AgentInstanceJobElementMismatch { .. }
        ),
        "a foreign element instance must be rejected, got {err:?}"
    );
    let _ = eik;
}

#[test]
fn external_agent_repeat_create_preserves_the_existing_agent_instance() {
    use crate::agent::AgentDefinition;
    let (mut engine, pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");

    // First CREATE mints the AgentInstance.
    let first = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = first
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("first CREATE mints");

    // A second CREATE conflicts without changing the existing registration.
    let second = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition {
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(
        matches!(second, EngineError::AgentInstanceAlreadyExists { agent_instance_key, .. } if agent_instance_key == aik)
    );
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).definition.model,
        None
    );
    assert_eq!(
        engine
            .state
            .instances
            .get(&pi)
            .map(|pi| pi.agent_instances.len())
            .unwrap_or(0),
        1,
        "exactly one AgentInstance for the element"
    );
}

#[test]
fn external_agent_history_bearing_update_is_lease_gated() {
    use crate::agent::{AgentDefinition, AgentHistoryRole};
    let (mut engine, pi, eik) = external_agent_instance();
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let created = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap();
    let aik = created
        .iter()
        .find_map(|e| match e {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .expect("CREATE mints");

    // A history-bearing UPDATE with a stale lease token is rejected.
    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job.key,
            job_lease: "stale-lease".to_string(),
            status: None,
            metrics: Default::default(),
            tools: None,
            history: vec![history_turn(0, 200, AgentHistoryRole::Assistant)],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceJobLeaseMismatch { .. }),
        "a history-bearing UPDATE with a bad lease is rejected, got {err:?}"
    );

    // A history-free UPDATE (pure status advance) is NOT gated.
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(crate::agent::AgentInstanceStatus::Thinking),
            metrics: Default::default(),
            tools: None,
            history: vec![],
        })
        .expect("a history-free UPDATE is not lease-gated");

    // A history-bearing UPDATE with the valid lease is accepted and appends.
    let ok = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: job.key,
            job_lease: job
                .lease_token
                .clone()
                .expect("external agent job carries a lease"),
            status: None,
            metrics: Default::default(),
            tools: None,
            history: vec![worker_history_turn(1, 300, AgentHistoryRole::Assistant)],
        })
        .expect("a history-bearing UPDATE with a valid lease is accepted");
    assert!(
        ok.iter()
            .any(|e| matches!(e, Event::AgentHistoryCreated { .. })),
        "the valid-lease UPDATE appends a history record"
    );
}

/// #1106 divergence 1 — the lease token is a distinct **opaque per-activation**
/// value, NOT the job's `deadline`. The pre-#1106 engine reused `deadline` as
/// the token; a distinct token is what lets a lease survive a `deadline`-moving
/// lock extension (see the next test).
#[test]
fn external_agent_lease_token_is_distinct_from_the_deadline() {
    let (mut engine, _pi, _eik) = external_agent_instance();
    // Use a deliberately large deadline (`now + timeout`) so that the monotonic
    // lease token (minted from `mint_key()`, i.e. small keys) cannot accidentally
    // equal it — the assertion below checks semantic distinctness, not a value
    // collision.
    let job = engine
        .activate_jobs_with_options("agent", "W", 1, 1_000_000_000, 100, lease_options())
        .pop()
        .expect("activatable");
    let token = job
        .lease_token
        .expect("an external agent job activates with a lease token");
    assert_ne!(
        token,
        job.deadline.to_string(),
        "the opaque lease token must not be the activation deadline"
    );
}

#[test]
fn agent_history_append_batch_materialises_one_pending_record_per_turn_in_order() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // A batch handed to the engine deliberately out of (loop_iteration,
    // produced_at) order — the store must re-order it deterministically.
    let turns = vec![
        history_turn(1, 200, AgentHistoryRole::Assistant),
        history_turn(0, 100, AgentHistoryRole::User),
        history_turn(0, 50, AgentHistoryRole::Configuration),
        history_turn(1, 150, AgentHistoryRole::ToolResult),
    ];
    let events = engine.append_agent_history(aik, turns);

    // One AGENT_HISTORY record per item, each PENDING and keyed by the agent
    // instance, all under the owning process instance.
    assert_eq!(events.len(), 4, "one record per appended turn");
    for event in &events {
        match event {
            Event::AgentHistoryCreated {
                instance_key: ik,
                record,
            } => {
                assert_eq!(*ik, instance_key);
                assert_eq!(record.agent_instance_key, aik);
                assert_eq!(record.process_instance_key, instance_key);
                assert_eq!(record.commit_status, AgentHistoryCommitStatus::Pending);
                assert_ne!(record.agent_history_key, 0);
            }
            other => panic!("expected AgentHistoryCreated, got {other:?}"),
        }
    }

    // Stored deterministically by (loop_iteration, produced_at).
    let stored = stored_history(&engine, instance_key, aik);
    let order: Vec<(i32, u64)> = stored
        .iter()
        .map(|r| (r.loop_iteration, r.produced_at))
        .collect();
    assert_eq!(order, vec![(0, 50), (0, 100), (1, 150), (1, 200)]);

    // Every turn got a distinct, monotonic history key.
    let mut keys: Vec<Key> = stored.iter().map(|r| r.agent_history_key).collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), 4, "history keys are unique");
}

#[test]
fn agent_history_append_ties_break_by_mint_order() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // Two turns share an identical (loop_iteration, produced_at); the earlier
    // appended (smaller minted key) must keep the earlier slot.
    let events = engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 100, AgentHistoryRole::User),
            history_turn(0, 100, AgentHistoryRole::Assistant),
        ],
    );
    let first_key = match &events[0] {
        Event::AgentHistoryCreated { record, .. } => record.agent_history_key,
        other => panic!("expected AgentHistoryCreated, got {other:?}"),
    };

    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 2);
    assert_eq!(
        stored[0].agent_history_key, first_key,
        "the first-appended turn stays first on a (loop_iteration, produced_at) tie"
    );
    assert_eq!(stored[0].role, AgentHistoryRole::User);
    assert_eq!(stored[1].role, AgentHistoryRole::Assistant);
}

#[test]
fn agent_history_commit_moves_pending_turns_to_committed() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();
    engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 10, AgentHistoryRole::User),
            history_turn(0, 20, AgentHistoryRole::Assistant),
        ],
    );

    let events = engine.commit_agent_history(aik);
    assert_eq!(events.len(), 1, "a single commit event names the batch");
    match &events[0] {
        Event::AgentHistoryCommitted {
            instance_key: ik,
            agent_instance_key,
            agent_history_keys,
        } => {
            assert_eq!(*ik, instance_key);
            assert_eq!(*agent_instance_key, aik);
            assert_eq!(agent_history_keys.len(), 2);
        }
        other => panic!("expected AgentHistoryCommitted, got {other:?}"),
    }

    let stored = stored_history(&engine, instance_key, aik);
    assert!(
        stored
            .iter()
            .all(|r| r.commit_status == AgentHistoryCommitStatus::Committed),
        "every pending turn is now COMMITTED"
    );

    // Nothing is pending, so a second commit is a no-op (committed turns are
    // immutable — the log never re-touches them).
    assert!(
        engine.commit_agent_history(aik).is_empty(),
        "re-committing with no pending turns emits nothing"
    );
}

#[test]
fn agent_history_discard_moves_pending_turns_to_discarded() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();
    engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 10, AgentHistoryRole::User),
            history_turn(1, 20, AgentHistoryRole::Assistant),
        ],
    );

    let events = engine.discard_agent_history(aik);
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::AgentHistoryDiscarded {
            agent_instance_key,
            agent_history_keys,
            ..
        } => {
            assert_eq!(*agent_instance_key, aik);
            assert_eq!(agent_history_keys.len(), 2);
        }
        other => panic!("expected AgentHistoryDiscarded, got {other:?}"),
    }

    let stored = stored_history(&engine, instance_key, aik);
    assert!(
        stored
            .iter()
            .all(|r| r.commit_status == AgentHistoryCommitStatus::Discarded),
        "every pending turn is now DISCARDED"
    );
    assert!(
        engine.discard_agent_history(aik).is_empty(),
        "re-discarding with no pending turns emits nothing"
    );
}

#[test]
fn agent_history_committed_turns_are_immutable_across_later_batches() {
    use crate::agent::{AgentHistoryCommitStatus, AgentHistoryRole};
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // First batch committed.
    engine.append_agent_history(aik, vec![history_turn(0, 10, AgentHistoryRole::User)]);
    engine.commit_agent_history(aik);
    let committed_key = stored_history(&engine, instance_key, aik)[0].agent_history_key;

    // A second batch, then discard — must touch only the pending (second) turns.
    engine.append_agent_history(aik, vec![history_turn(1, 20, AgentHistoryRole::Assistant)]);
    let discard = engine.discard_agent_history(aik);
    match &discard[0] {
        Event::AgentHistoryDiscarded {
            agent_history_keys, ..
        } => assert!(
            !agent_history_keys.contains(&committed_key),
            "an already-committed turn is never named by a later discard"
        ),
        other => panic!("expected AgentHistoryDiscarded, got {other:?}"),
    }

    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 2);
    // The first turn is still COMMITTED; only the second flipped to DISCARDED.
    let by_key: std::collections::HashMap<Key, AgentHistoryCommitStatus> = stored
        .iter()
        .map(|r| (r.agent_history_key, r.commit_status))
        .collect();
    assert_eq!(by_key[&committed_key], AgentHistoryCommitStatus::Committed);
    let discarded = stored
        .iter()
        .filter(|r| r.commit_status == AgentHistoryCommitStatus::Discarded)
        .count();
    assert_eq!(discarded, 1, "only the second batch was discarded");
}

#[test]
fn agent_history_append_to_unknown_instance_is_a_noop() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, _instance_key, _aik) = agent_instance_for_history();
    let bogus = 999_999_999;
    assert!(
        engine
            .append_agent_history(bogus, vec![history_turn(0, 1, AgentHistoryRole::User)])
            .is_empty(),
        "appending to an unknown agent instance emits nothing"
    );
    assert!(engine.commit_agent_history(bogus).is_empty());
    assert!(engine.discard_agent_history(bogus).is_empty());
}

#[test]
fn agent_history_dedups_repeated_history_item_id_within_a_batch() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // Two turns share `historyItemId` "h1"; the second is an idempotent retry
    // of the first and must NOT materialise a second record.
    let events = engine.append_agent_history(
        aik,
        vec![
            history_turn_with_id(0, 10, AgentHistoryRole::User, "h1"),
            history_turn_with_id(0, 20, AgentHistoryRole::Assistant, "h1"),
        ],
    );

    let created_key = match &events[0] {
        Event::AgentHistoryCreated { record, .. } => {
            assert_eq!(record.history_item_id.as_deref(), Some("h1"));
            record.agent_history_key
        }
        other => panic!("expected AgentHistoryCreated first, got {other:?}"),
    };
    match &events[1] {
        Event::AgentHistoryDeduplicated {
            instance_key: ik,
            agent_instance_key,
            history_item_id,
            original_agent_history_key,
        } => {
            assert_eq!(*ik, instance_key);
            assert_eq!(*agent_instance_key, aik);
            assert_eq!(history_item_id, "h1");
            assert_eq!(
                *original_agent_history_key, created_key,
                "the duplicate resolves to the original turn's key"
            );
        }
        other => panic!("expected AgentHistoryDeduplicated second, got {other:?}"),
    }
    assert_eq!(events.len(), 2);

    // Only one record is materialised in the append-only log.
    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 1, "the duplicate created no new record");
    assert_eq!(stored[0].agent_history_key, created_key);
}

#[test]
fn agent_history_dedups_history_item_id_against_a_prior_committed_batch() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // First batch records "h1" and commits it.
    engine.append_agent_history(
        aik,
        vec![history_turn_with_id(0, 10, AgentHistoryRole::User, "h1")],
    );
    engine.commit_agent_history(aik);
    let original_key = stored_history(&engine, instance_key, aik)[0].agent_history_key;

    // A retry re-submits "h1": no new record, dedup resolves to the original.
    let events = engine.append_agent_history(
        aik,
        vec![history_turn_with_id(1, 20, AgentHistoryRole::User, "h1")],
    );
    assert_eq!(events.len(), 1);
    match &events[0] {
        Event::AgentHistoryDeduplicated {
            history_item_id,
            original_agent_history_key,
            ..
        } => {
            assert_eq!(history_item_id, "h1");
            assert_eq!(*original_agent_history_key, original_key);
        }
        other => panic!("expected AgentHistoryDeduplicated, got {other:?}"),
    }
    let stored = stored_history(&engine, instance_key, aik);
    assert_eq!(stored.len(), 1, "the retry created no new record");
}

#[test]
fn agent_history_absent_history_item_id_never_dedups() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // Two id-less turns are indistinguishable for correlation, so both must
    // materialise fresh records — dedup applies only to carried ids.
    let events = engine.append_agent_history(
        aik,
        vec![
            history_turn(0, 10, AgentHistoryRole::User),
            history_turn(0, 20, AgentHistoryRole::Assistant),
        ],
    );
    assert!(
        events
            .iter()
            .all(|e| matches!(e, Event::AgentHistoryCreated { .. })),
        "id-less turns never dedup"
    );
    assert_eq!(stored_history(&engine, instance_key, aik).len(), 2);
}

#[test]
fn agent_history_discarded_history_item_id_is_re_recordable() {
    use crate::agent::AgentHistoryRole;
    let (mut engine, instance_key, aik) = agent_instance_for_history();

    // "h1" is appended then discarded (rejected) — it is not "already
    // recorded", so re-submitting it must create a fresh record, not dedup.
    engine.append_agent_history(
        aik,
        vec![history_turn_with_id(0, 10, AgentHistoryRole::User, "h1")],
    );
    engine.discard_agent_history(aik);

    let events = engine.append_agent_history(
        aik,
        vec![history_turn_with_id(1, 20, AgentHistoryRole::User, "h1")],
    );
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], Event::AgentHistoryCreated { .. }),
        "a discarded id is re-recordable, not deduped"
    );
    // One discarded + one fresh pending record.
    assert_eq!(stored_history(&engine, instance_key, aik).len(), 2);
}

#[test]
fn agent_instance_create_from_active_agent_element_rejects_re_registration() {
    use crate::agent::{
        AgentDefinition, AgentHistoryRole, AgentInstanceLimits, AgentInstanceStatus,
    };
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    let registered = stored_agent_instance(&engine, pi, aik);

    let limits = AgentInstanceLimits {
        max_tokens: 1_000,
        max_model_calls: 10,
        max_tool_calls: 5,
    };
    let before = stored_agent_instance(&engine, pi, aik);
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: registered.job_key,
            job_lease: registered.job_lease,
            definition: AgentDefinition {
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
            limits: Some(limits),
            history: vec![history_turn(0, 10, AgentHistoryRole::Configuration)],
        })
        .unwrap_err();
    assert!(
        matches!(err, EngineError::AgentInstanceAlreadyExists { agent_instance_key, .. } if agent_instance_key == aik)
    );
    assert_eq!(stored_agent_instance(&engine, pi, aik), before);
    assert_eq!(before.status, AgentInstanceStatus::Initializing);
    assert!(stored_history(&engine, pi, aik).is_empty());
}

#[test]
fn agent_instance_create_without_explicit_limits_defaults_to_unlimited() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};
    let (engine, pi, aik) = agent_with_initial_history(vec![worker_history_turn(
        1,
        1,
        AgentHistoryRole::Configuration,
    )]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        AgentInstanceLimits::default(),
        "absent limits default to -1/-1/-1"
    );
}

#[test]
fn agent_instance_create_takes_limits_from_configuration_history_item() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};

    let cfg_limits = AgentInstanceLimits {
        max_tokens: 42,
        max_model_calls: -1,
        max_tool_calls: 7,
    };
    let mut cfg_turn = worker_history_turn(1, 5, AgentHistoryRole::Configuration);
    cfg_turn.limits = Some(cfg_limits);
    let (engine, pi, aik) = agent_with_initial_history(vec![cfg_turn]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        cfg_limits,
        "limits fall back to the CONFIGURATION history item"
    );
}

#[test]
fn agent_instance_create_ignores_limits_from_non_configuration_history_item() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};

    // An ASSISTANT turn carrying `limits` must NOT seed the record's limits:
    // only a CONFIGURATION turn may. With no CONFIGURATION turn and no explicit
    // limits, the record must fall back to the unlimited default.
    let mut assistant_turn = worker_history_turn(2, 5, AgentHistoryRole::Assistant);
    assistant_turn.limits = Some(AgentInstanceLimits {
        max_tokens: 42,
        max_model_calls: 3,
        max_tool_calls: 7,
    });
    let (engine, pi, aik) = agent_with_initial_history(vec![
        worker_history_turn(1, 1, AgentHistoryRole::Configuration),
        assistant_turn,
    ]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        AgentInstanceLimits::default(),
        "a non-CONFIGURATION turn must not seed limits; default to unlimited"
    );
}

#[test]
fn agent_instance_create_takes_limits_from_last_configuration_not_later_turn() {
    use crate::agent::{AgentHistoryRole, AgentInstanceLimits};

    // CONFIGURATION seeds limits; a LATER ASSISTANT turn carrying different
    // limits must not override the CONFIGURATION-supplied value.
    let cfg_limits = AgentInstanceLimits {
        max_tokens: 100,
        max_model_calls: 5,
        max_tool_calls: 9,
    };
    let mut cfg_turn = worker_history_turn(1, 5, AgentHistoryRole::Configuration);
    cfg_turn.limits = Some(cfg_limits);
    let mut later_assistant = worker_history_turn(2, 10, AgentHistoryRole::Assistant);
    later_assistant.limits = Some(AgentInstanceLimits {
        max_tokens: 1,
        max_model_calls: 1,
        max_tool_calls: 1,
    });
    let (engine, pi, aik) = agent_with_initial_history(vec![cfg_turn, later_assistant]);
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).limits,
        cfg_limits,
        "the CONFIGURATION turn wins; a later non-CONFIGURATION turn cannot override it"
    );
}

#[test]
fn agent_instance_create_on_inactive_element_instance_is_rejected() {
    use crate::agent::AgentDefinition;
    let (mut engine, _pi, _aik) = agent_instance_for_history();
    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: 999_999_999,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceElementInstanceInactive { .. }
    ));
}

#[test]
fn agent_instance_create_on_non_eligible_element_is_rejected() {
    use crate::agent::AgentDefinition;
    // A timer intermediate catch event is active-but-not-eligible.
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="catch-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:intermediateCatchEvent id="await">
            <bpmn:timerEventDefinition>
              <bpmn:timeDuration>PT1H</bpmn:timeDuration>
            </bpmn:timerEventDefinition>
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="await" />
          <bpmn:sequenceFlow id="f2" sourceRef="await" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("catch-proc"))
        .unwrap();
    let eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "await" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the catch event should activate and wait");

    let err = engine
        .apply_command(Command::CreateAgentInstance {
            element_instance_key: eik,
            job_key: 0,
            job_lease: String::new(),
            definition: AgentDefinition::default(),
            limits: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceElementNotEligible { .. }
    ));
}

#[test]
fn agent_instance_update_advances_status_appends_history_and_accumulates_metrics() {
    use crate::agent::{AgentHistoryRole, AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);

    // Advance through each active state, appending one turn per update.
    let steps = [
        AgentInstanceStatus::ToolDiscovery,
        AgentInstanceStatus::Thinking,
        AgentInstanceStatus::ToolCalling,
        AgentInstanceStatus::Idle,
    ];
    for (i, status) in steps.iter().enumerate() {
        let mut turn =
            worker_history_turn(i as i32 + 1, (i as u64) * 10, AgentHistoryRole::Assistant);
        turn.metrics = Some(crate::agent::AgentHistoryMetrics {
            input_tokens: Some(100),
            output_tokens: Some(20),
            ..Default::default()
        });
        turn.tool_calls.push(crate::agent::AgentHistoryToolCall {
            tool_call_id: format!("call-{i}"),
            tool_name: "tool".into(),
            element_id: None,
            arguments: None,
        });
        let events = engine
            .apply_command(update_agent(
                &engine,
                aik,
                eik,
                pi,
                *status,
                AgentInstanceMetricsDelta::default(),
                vec![turn],
            ))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::AgentInstanceUpdated { .. })),
            "each UPDATE emits AgentInstanceUpdated"
        );
        assert_eq!(stored_agent_instance(&engine, pi, aik).status, *status);
    }

    // Metrics accumulate immediately; history remains pending until job completion.
    let stored = stored_agent_instance(&engine, pi, aik);
    assert_eq!(stored.metrics.input_tokens, 400);
    assert_eq!(stored.metrics.output_tokens, 80);
    assert_eq!(stored.metrics.model_calls, 4);
    assert_eq!(stored.metrics.tool_calls, 4);
    let hist = stored_history(&engine, pi, aik);
    assert_eq!(hist.len(), 4);
    assert!(hist
        .iter()
        .all(|r| r.commit_status == crate::agent::AgentHistoryCommitStatus::Pending));
    engine
        .apply_command(Command::complete_job(stored.job_key).with_job_lease(stored.job_lease))
        .unwrap();
    assert!(stored_history(&engine, pi, aik)
        .iter()
        .all(|r| r.commit_status == crate::agent::AgentHistoryCommitStatus::Committed));
}

#[test]
fn agent_instance_update_replaces_tools() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus, AgentTool};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);

    let tool = AgentTool {
        name: "search".to_string(),
        description: None,
        element_id: None,
    };
    engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::ToolDiscovery),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: Some(vec![tool.clone()]),
            history: vec![],
        })
        .unwrap();
    assert_eq!(stored_agent_instance(&engine, pi, aik).tools, vec![tool]);
}

#[test]
fn agent_instance_update_to_completed_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    let err = engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Completed,
            AgentInstanceMetricsDelta::default(),
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceStatusNotSettable { .. }
    ));
}

#[test]
fn agent_instance_update_with_wrong_element_id_or_process_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);

    let wrong_element = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "not-agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        wrong_element,
        EngineError::AgentInstanceOwnershipMismatch { .. }
    ));

    let wrong_pi = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: eik,
            element_id: "agent".to_string(),
            process_instance_key: 424_242,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        wrong_pi,
        EngineError::AgentInstanceOwnershipMismatch { .. }
    ));
}

#[test]
fn agent_instance_update_on_inactive_element_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let err = engine
        .apply_command(update_agent(
            &engine,
            aik,
            888_888_888,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta::default(),
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceElementInstanceInactive { .. }
    ));
}

#[test]
fn agent_instance_update_with_conflicting_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    // Two activations of the same element in one process each own an agent.
    // Referencing the second activation from the first agent conflicts.
    let (mut engine, pi_a, aik_a) = agent_instance_for_history();
    let events_b = engine
        .apply_command(Command::ModifyInstance {
            instance_key: pi_a,
            activate_instructions: vec![crate::command::ActivateElementInstruction {
                element_id: "agent".into(),
                variables: HashMap::new(),
            }],
            terminate_instructions: vec![],
        })
        .unwrap();
    let pi_b = events_b.iter().find_map(|e| e.instance_key()).unwrap();
    let aik_b = register_job_backed_agent(&mut engine, "agent").agent_instance_key;
    let eik_b = agent_element_instance_key(&engine, pi_b, aik_b);

    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik_a,
            element_instance_key: eik_b,
            element_id: "agent".to_string(),
            process_instance_key: pi_a,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    match err {
        EngineError::AgentInstanceConflict {
            conflicting_agent_instance_key,
            ..
        } => assert_eq!(conflicting_agent_instance_key, aik_b),
        other => panic!("expected AgentInstanceConflict, got {other:?}"),
    }
}

#[test]
fn agent_instance_update_with_foreign_process_element_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    // A *different* process whose active element merely shares the id "agent"
    // but is not agent-eligible (a userTask, so no owning AgentInstance is
    // minted). Referencing that foreign, unowned element instance from an UPDATE
    // must be rejected as an ownership mismatch — it would otherwise be linked as
    // a re-entry key and corrupt ownership/re-entry tracking across process
    // instances.
    let (mut engine, pi, aik) = agent_instance_for_history();
    let foreign_xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="foreign-proc" isExecutable="true">
          <bpmn:startEvent id="start" />
          <bpmn:userTask id="agent" />
          <bpmn:endEvent id="end" />
          <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="agent" />
          <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="end" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let def = crate::bpmn::parse_bpmn(foreign_xml).unwrap().remove(0);
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("foreign-proc"))
        .unwrap();
    let foreign_eik = events
        .iter()
        .find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id,
                ..
            } if element_id == "agent" => Some(*element_instance_key),
            _ => None,
        })
        .expect("the foreign userTask should activate and wait");

    let err = engine
        .apply_command(Command::UpdateAgentInstance {
            agent_instance_key: aik,
            element_instance_key: foreign_eik,
            element_id: "agent".to_string(),
            process_instance_key: pi,
            job_key: 0,
            job_lease: String::new(),
            status: Some(AgentInstanceStatus::Thinking),
            metrics: AgentInstanceMetricsDelta::default(),
            tools: None,
            history: vec![],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceOwnershipMismatch { .. }
    ));
}

#[test]
fn agent_instance_update_rejects_negative_metric_deltas() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    // First accumulate some real usage.
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta {
                input_tokens: 100,
                output_tokens: 40,
                model_calls: 3,
                tool_calls: 2,
                ..Default::default()
            },
            vec![],
        ))
        .unwrap();

    // Invalid negative deltas reject atomically rather than refund prior usage.
    let err = engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta {
                input_tokens: -1_000,
                output_tokens: -1_000,
                model_calls: -10,
                tool_calls: -10,
                ..Default::default()
            },
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentHistoryInvalid { .. }));
    let stored = stored_agent_instance(&engine, pi, aik);
    assert_eq!(stored.metrics.input_tokens, 100);
    assert_eq!(stored.metrics.output_tokens, 40);
    assert_eq!(stored.metrics.model_calls, 3);
    assert_eq!(stored.metrics.tool_calls, 2);
}

#[test]
fn agent_instance_update_on_unknown_instance_is_rejected() {
    use crate::agent::{AgentInstanceMetricsDelta, AgentInstanceStatus};
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    let err = engine
        .apply_command(update_agent(
            &engine,
            123_456_789,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta::default(),
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentInstanceNotFound { .. }));
}

#[test]
fn agent_instance_update_records_usage_even_when_it_exceeds_configured_limits() {
    use crate::agent::{
        AgentHistoryRole, AgentInstanceLimits, AgentInstanceMetricsDelta, AgentInstanceStatus,
    };
    // Unlimited (-1) instance: a large batch is accepted.
    let (mut engine, pi, aik) = agent_instance_for_history();
    let eik = agent_element_instance_key(&engine, pi, aik);
    engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta {
                input_tokens: 10_000,
                model_calls: 999,
                tool_calls: 999,
                ..Default::default()
            },
            vec![],
        ))
        .unwrap();
    assert_eq!(
        stored_agent_instance(&engine, pi, aik).metrics.input_tokens,
        10_000
    );

    // The worker enforces execution limits; recording observed usage must remain possible.
    let mut configuration = worker_history_turn(1, 1, AgentHistoryRole::Configuration);
    configuration.limits = Some(AgentInstanceLimits {
        max_tokens: 50,
        max_model_calls: -1,
        max_tool_calls: -1,
    });
    let (mut engine, pi, aik) = agent_with_initial_history(vec![configuration]);
    let eik = agent_element_instance_key(&engine, pi, aik);
    let mut turn = worker_history_turn(2, 2, AgentHistoryRole::Assistant);
    turn.metrics = Some(crate::agent::AgentHistoryMetrics {
        input_tokens: Some(100),
        ..Default::default()
    });
    engine
        .apply_command(update_agent(
            &engine,
            aik,
            eik,
            pi,
            AgentInstanceStatus::Thinking,
            AgentInstanceMetricsDelta::default(),
            vec![turn],
        ))
        .unwrap();
    let after = stored_agent_instance(&engine, pi, aik);
    assert_eq!(after.metrics.input_tokens, 100);
    assert_eq!(after.limits.max_tokens, 50);
    assert_eq!(after.status, AgentInstanceStatus::Thinking);
    assert_eq!(stored_history(&engine, pi, aik).len(), 2);
}

#[test]
fn agent_instance_complete_drives_to_completed_and_drains_remaining() {
    use crate::agent::AgentInstanceStatus;
    let (mut engine, pi, aiks) = two_agent_instances();
    assert_eq!(
        aiks.len(),
        2,
        "workers register two parallel agent instances"
    );

    let active_count = |engine: &Engine| -> usize {
        engine
            .state
            .instances
            .get(&pi)
            .map(|p| {
                p.agent_instances
                    .values()
                    .filter(|ai| ai.status.is_active())
                    .count()
            })
            .unwrap_or(0)
    };
    assert_eq!(active_count(&engine), 2);

    // Complete each by key; the active set drains one at a time.
    let events = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[0],
        })
        .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::AgentInstanceCompleted { .. })));
    assert_eq!(
        stored_agent_instance(&engine, pi, aiks[0]).status,
        AgentInstanceStatus::Completed
    );
    assert_eq!(active_count(&engine), 1);

    engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[1],
        })
        .unwrap();
    assert_eq!(
        stored_agent_instance(&engine, pi, aiks[1]).status,
        AgentInstanceStatus::Completed
    );
    assert_eq!(active_count(&engine), 0, "no active agent instances remain");
}

#[test]
fn agent_instance_complete_is_the_only_path_to_completed_and_rejects_re_completion() {
    let (mut engine, _pi, aiks) = two_agent_instances();
    engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[0],
        })
        .unwrap();
    // Re-completing a terminal instance is rejected.
    let err = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: aiks[0],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::AgentInstanceAlreadyCompleted { .. }
    ));
}

#[test]
fn agent_instance_complete_on_unknown_instance_is_rejected() {
    let (mut engine, _pi, _aiks) = agent_instance_for_history();
    let err = engine
        .apply_command(Command::CompleteAgentInstance {
            agent_instance_key: 777_777_777,
        })
        .unwrap_err();
    assert!(matches!(err, EngineError::AgentInstanceNotFound { .. }));
}

/// A link throw hands its token to the matching catch *by name*, so multiple
/// distinct link pairs in one model each route to their own catch.
// zeebe-cells: element:IntermediateThrowEvent event:intermediate-throw:link
#[test]
fn link_events_route_by_matching_name() {
    let xml = r#"
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                        xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="two-links" isExecutable="true">
          <bpmn:startEvent id="s"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="ThrowB" />
          <bpmn:intermediateThrowEvent id="ThrowB">
            <bpmn:incoming>f0</bpmn:incoming>
            <bpmn:linkEventDefinition name="B" />
          </bpmn:intermediateThrowEvent>
          <bpmn:intermediateCatchEvent id="CatchA">
            <bpmn:outgoing>fa</bpmn:outgoing>
            <bpmn:linkEventDefinition name="A" />
          </bpmn:intermediateCatchEvent>
          <bpmn:sequenceFlow id="fa" sourceRef="CatchA" targetRef="TaskA" />
          <bpmn:serviceTask id="TaskA">
            <bpmn:incoming>fa</bpmn:incoming>
            <bpmn:extensionElements><zeebe:taskDefinition type="job-a" /></bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:intermediateCatchEvent id="CatchB">
            <bpmn:outgoing>fb</bpmn:outgoing>
            <bpmn:linkEventDefinition name="B" />
          </bpmn:intermediateCatchEvent>
          <bpmn:sequenceFlow id="fb" sourceRef="CatchB" targetRef="TaskB" />
          <bpmn:serviceTask id="TaskB">
            <bpmn:incoming>fb</bpmn:incoming>
            <bpmn:extensionElements><zeebe:taskDefinition type="job-b" /></bpmn:extensionElements>
          </bpmn:serviceTask>
        </bpmn:process>
      </bpmn:definitions>"#;

    let def = crate::bpmn::parse_bpmn(xml).unwrap().pop().unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance("two-links"))
        .unwrap();

    // The throw named "B" must reach CatchB's task, not CatchA's.
    assert_eq!(
        engine.activate_jobs("job-a", "w", 5, 1_000, 0).len(),
        0,
        "the unrelated link 'A' catch must not fire"
    );
    assert_eq!(
        engine.activate_jobs("job-b", "w", 5, 1_000, 0).len(),
        1,
        "the throw hands off to the same-named catch 'B'"
    );
}

/// `multi_arrival_process` with an INCLUSIVE join: `s -> fork =< inner, t >;
/// inner =< x, y > -> merge (xor) -> join (or);  t -> join;  join -> e`. Two
/// tokens reach `join` over `merge -> join`, one over `t -> join` (#1237).
fn inclusive_multi_arrival_process() -> ProcessDefinition {
    inclusive_multi_arrival_builder()
        .connect("join", "e")
        .build()
        .unwrap()
}

/// `inclusive_multi_arrival_process` without the join's outgoing flow, so a
/// test can make it conditional or gate it behind a listener.
fn inclusive_multi_arrival_builder() -> ProcessBuilder {
    ProcessBuilder::new("inclusive_multi_arrival")
        .start_event("s")
        .parallel_gateway("fork")
        .parallel_gateway("inner")
        .service_task("x", "jx")
        .service_task("y", "jy")
        .exclusive_gateway("merge")
        .service_task("t", "jt")
        .inclusive_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "inner")
        .connect("fork", "t")
        .connect("inner", "x")
        .connect("inner", "y")
        .connect("x", "merge")
        .connect("y", "merge")
        .connect("merge", "join")
        .connect("t", "join")
}

/// Drive `inclusive_multi_arrival_process`, completing its tasks in `order`,
/// and return every join firing per step plus the instance key.
fn run_inclusive_multi_arrival(order: [&str; 3]) -> (Engine, Key, Vec<usize>) {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(inclusive_multi_arrival_process()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("inclusive_multi_arrival"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    let fires = order
        .iter()
        .map(|job| join_fire_count(&complete_one(&mut engine, job), "join"))
        .collect();
    (engine, instance_key, fires)
}

/// The `merge -> join` incoming flow of `inclusive_multi_arrival_process`.
fn merge_to_join() -> IncomingFlow {
    IncomingFlow {
        from: "merge".into(),
        ordinal: 0,
    }
}

#[test]
fn inclusive_join_strands_a_surplus_that_arrives_first_like_zeebe() {
    // Zeebe evaluates an inclusive join only when a token arrives
    // (`ProcessInstanceStateTransitionGuard.canActivateInclusiveGateway`), and
    // its path search skips paths that end on an already-taken incoming flow
    // (`BpmnInclusiveGatewayBehavior.visitElement`). Both `merge -> join` tokens
    // arrive while `t` can still reach the join, so both are rejected. `t`
    // then completes the set: the join fires once and consumes one token per
    // flow (Tetris). Nothing arrives afterwards, so the surplus `merge -> join`
    // token is stranded and the instance never completes (#1241).
    let (engine, instance_key, fires) = run_inclusive_multi_arrival(["jx", "jy", "jt"]);
    assert_eq!(fires, vec![0, 0, 1], "fires once, when `t` arrives");
    assert!(
        !engine.is_completed(instance_key),
        "the surplus is stranded"
    );
    let instance = engine.instance(instance_key).unwrap();
    assert_eq!(
        instance.join_flow_arrivals["join"].count(&merge_to_join()),
        1
    );
    assert!(instance.join_instances.contains_key("join"));
}

#[test]
fn inclusive_join_fires_on_arrival_when_the_remaining_path_ends_on_a_taken_flow() {
    // `t` arrives after the first `merge -> join` token. `y` is still live, but
    // its only path to the join ends on `merge -> join`, which is already
    // taken, so Zeebe's path search prunes it and the join fires on `t`'s
    // arrival. `y`'s token then arrives with no live path left and fires the
    // join again (#1241).
    let (engine, instance_key, fires) = run_inclusive_multi_arrival(["jx", "jt", "jy"]);
    assert_eq!(fires, vec![0, 1, 1]);
    assert!(engine.is_completed(instance_key));
}

#[test]
fn inclusive_join_waits_for_a_live_path_over_an_untaken_flow() {
    // `t` arrives first, while `x` and `y` can still reach the join over the
    // untaken `merge -> join` flow, so it waits. The first `merge -> join`
    // token completes the set; the second finds no live path and fires alone.
    let (engine, instance_key, fires) = run_inclusive_multi_arrival(["jt", "jx", "jy"]);
    assert_eq!(fires, vec![0, 1, 1]);
    assert!(engine.is_completed(instance_key));
}

#[test]
fn inclusive_join_is_not_reevaluated_when_a_competing_path_diverges_like_zeebe() {
    // `a` arrives while `b` can still reach the join through `g`, so Zeebe
    // rejects the activation. `b` then routes to `eb` instead. Zeebe only
    // re-evaluates the join when another token arrives at it, and none does,
    // so the join never fires and the instance stays active (#1241).
    let def = ProcessBuilder::new("inc-diverge")
        .start_event("s")
        .inclusive_gateway("isplit")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .exclusive_gateway("g")
        .inclusive_gateway("join")
        .end_event("e")
        .end_event("eb")
        .connect("s", "isplit")
        .connect("isplit", "a")
        .connect("isplit", "b")
        .connect("a", "join")
        .connect("b", "g")
        .connect_when("g", "join", "go")
        .connect_default("g", "eb")
        .connect("join", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "inc-diverge",
            HashMap::from([("go".to_string(), Value::Bool(false))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();

    let after_a = complete_one(&mut engine, "ja");
    let after_b = complete_one(&mut engine, "jb");

    assert_eq!(join_fire_count(&after_a, "join"), 0);
    assert_eq!(join_fire_count(&after_b, "join"), 0);
    assert!(!engine.is_completed(instance_key));
    let a_to_join = IncomingFlow {
        from: "a".into(),
        ordinal: 0,
    };
    let instance = engine.instance(instance_key).unwrap();
    assert_eq!(instance.join_flow_arrivals["join"].count(&a_to_join), 1);
    assert!(instance.join_instances.contains_key("join"));
}

#[test]
fn joins_count_a_flow_when_it_is_taken_like_zeebe() {
    // Zeebe counts a join's incoming flow when the flow is taken
    // (`ProcessInstanceSequenceFlowTakenApplier`), before the join's ACTIVATE
    // command is processed. Both flows into each join are taken before either
    // activation runs, so the first activation already sees the full set and
    // fires; no placeholder join instance is ever opened (#1241).
    for (id, join) in [("par-take", "parallel"), ("inc-take", "inclusive")] {
        let builder = ProcessBuilder::new(id)
            .start_event("s")
            .parallel_gateway("fork")
            .exclusive_gateway("p")
            .exclusive_gateway("q");
        let builder = if join == "parallel" {
            builder.parallel_gateway("join")
        } else {
            builder.inclusive_gateway("join")
        };
        let def = builder
            .end_event("e")
            .connect("s", "fork")
            .connect("fork", "p")
            .connect("fork", "q")
            .connect("p", "join")
            .connect("q", "join")
            .connect("join", "e")
            .build()
            .unwrap();
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let events = engine.apply_command(Command::create_instance(id)).unwrap();
        let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();

        assert!(engine.is_completed(instance_key), "{join} join completes");
        assert_eq!(
            join_fire_count(&events, "join"),
            1,
            "{join} join fires once"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, Event::ParallelJoinOpened { .. })),
            "{join} join fires on its first activation"
        );
    }
}

#[test]
fn inclusive_join_records_the_flow_each_token_arrived_over() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(inclusive_multi_arrival_process()))
        .unwrap();
    let events = engine
        .apply_command(Command::create_instance("inclusive_multi_arrival"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    complete_one(&mut engine, "jx");
    complete_one(&mut engine, "jy");
    let instance = engine.instance(instance_key).unwrap();
    assert_eq!(
        instance.join_flow_arrivals["join"].count(&merge_to_join()),
        2
    );
    assert!(!instance.join_counts.contains_key("join"));
}

/// Count the `join -> e` routings in `events`.
fn join_routed_to_end(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, Event::SequenceFlowTaken { from, to, .. } if from == "join" && to == "e"))
        .count()
}

#[test]
fn inclusive_join_consumes_before_its_incident_like_zeebe() {
    // Zeebe consumes the join's tokens when the activation is accepted
    // (`ProcessInstanceElementActivatingV3Applier`), before
    // `InclusiveGatewayProcessor.finalizeActivation` evaluates the outgoing
    // conditions. With go=false no flow matches, so the accepted join parks
    // on an incident after consuming one token per flow. Resolving it routes
    // once. The surplus `merge -> join` token stays stranded (#1241).
    let def = inclusive_multi_arrival_builder()
        .connect_when("join", "e", "go")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "inclusive_multi_arrival",
            HashMap::from([("go".to_string(), Value::Bool(false))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    for job in ["jx", "jy", "jt"] {
        complete_one(&mut engine, job);
    }
    let incidents = engine.active_incidents();
    assert_eq!(
        incidents.len(),
        1,
        "the accepted join parks on one incident"
    );
    let incident_key = incidents[0].key;
    let instance = engine.instance(instance_key).unwrap();
    assert_eq!(
        instance.join_flow_arrivals["join"].count(&merge_to_join()),
        1,
        "the incident parks after the consumption"
    );

    engine
        .apply_command(Command::set_variables(
            instance_key,
            HashMap::from([("go".to_string(), Value::Bool(true))]),
        ))
        .unwrap();
    let resolved = engine
        .apply_command(Command::resolve_incident(incident_key))
        .unwrap();

    assert_eq!(join_routed_to_end(&resolved), 1, "the redrive routes once");
    assert!(engine.active_incidents().is_empty());
    assert!(
        !engine.is_completed(instance_key),
        "the surplus is stranded"
    );
    let instance = engine.instance(instance_key).unwrap();
    assert_eq!(
        instance.join_flow_arrivals["join"].count(&merge_to_join()),
        1
    );
}

#[test]
fn inclusive_join_late_arrival_while_an_incident_is_pending_activates_again_like_zeebe() {
    // The join is accepted when `t` arrives and parks on an incident after
    // consuming. `y`'s later token is a fresh arrival: Zeebe's guard excludes
    // the gateway's own instance from the live-path sources, so it accepts a
    // second gateway instance, which raises its own incident. Confirmed on
    // c8run 8.8.8 (order x, t, y): two ACTIVE join instances, two
    // CONDITION_ERROR incidents (#1241).
    let def = inclusive_multi_arrival_builder()
        .connect_when("join", "e", "go")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance_with(
            "inclusive_multi_arrival",
            HashMap::from([("go".to_string(), Value::Bool(false))]),
        ))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    complete_one(&mut engine, "jx");
    complete_one(&mut engine, "jt");
    assert_eq!(engine.active_incidents().len(), 1);
    complete_one(&mut engine, "jy");

    assert_eq!(
        engine.active_incidents().len(),
        2,
        "the late arrival activates the join again"
    );
    let instance = engine.instance(instance_key).unwrap();
    assert_eq!(
        instance
            .active
            .values()
            .filter(|id| id.as_str() == "join")
            .count(),
        2,
        "both accepted join instances wait on their incidents"
    );
    assert!(!instance.join_instances.contains_key("join"));
    assert!(!instance.join_flow_arrivals.contains_key("join"));
}

#[test]
fn inclusive_join_end_listener_runs_once_per_accepted_activation() {
    // The join is accepted once, when `t` arrives, and rests in COMPLETING
    // behind its `end` listener. Draining the chain routes once. The surplus
    // `merge -> join` token is stranded, so no second listener job is created
    // (#1241).
    let def = inclusive_multi_arrival_builder()
        .connect("join", "e")
        .with_listeners(
            "join",
            Vec::new(),
            vec![el(ListenerEventType::End, "join-audit")],
        )
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let created = engine
        .apply_command(Command::create_instance("inclusive_multi_arrival"))
        .unwrap();
    let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
    for job in ["jx", "jy", "jt"] {
        complete_one(&mut engine, job);
    }

    let audit = engine.activate_jobs("join-audit", "W", 10, 60_000, 0);
    assert_eq!(
        audit.len(),
        1,
        "one end-listener job for the one activation"
    );
    let done = engine
        .apply_command(Command::complete_job(audit[0].key))
        .unwrap();
    assert_eq!(join_routed_to_end(&done), 1);
    assert!(engine
        .activate_jobs("join-audit", "W", 10, 60_000, 0)
        .is_empty());
    assert!(
        !engine.is_completed(instance_key),
        "the surplus is stranded"
    );
}

/// Two tokens head for an inclusive join in one command: `i` routes one
/// straight to `join` and one through the exclusive gateway `x`. The first
/// arrival must count the token still queued through `x` as a live path, like
/// Zeebe's `activeSequenceFlowIds`, so `join` fires once, on both tokens (#1241).
/// Formal counterpart: `formal/tla/MCInclusiveInTransit.tla`.
#[test]
fn inclusive_join_waits_for_a_sibling_token_still_in_transit() {
    let def = ProcessBuilder::new("inc-in-transit")
        .start_event("s")
        .inclusive_gateway("i")
        .exclusive_gateway("x")
        .service_task("t", "jt")
        .inclusive_gateway("join")
        .end_event("e")
        .connect("s", "i")
        .connect("i", "join")
        .connect("i", "x")
        .connect_when("i", "t", "=false")
        .connect("x", "join")
        .connect("t", "join")
        .connect("join", "e")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance("inc-in-transit"))
        .unwrap();
    let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
    assert_eq!(join_fire_count(&events, "join"), 1, "the join fires once");
    assert_eq!(join_routed_to_end(&events), 1, "the join routes once");
    assert!(engine.is_completed(instance_key));
}

/// A sub-process whose inner split takes a flow into the parallel join `pj`
/// in the same drain as a terminate end that tears the sub-process down, so
/// the join's activation is still queued when its scope dies. `via_gateway`
/// routes the join's flow through a pass-through gateway, which shifts the
/// take one step later in the queue.
fn scoped_terminate_with_queued_join_arrival(via_gateway: bool) -> ProcessDefinition {
    let mut b = ProcessBuilder::new("term-queued-join")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .parallel_gateway("split")
        .contained_in("split", "sub")
        .terminate_end_event("stop")
        .contained_in("stop", "sub")
        .service_task("b", "jb")
        .contained_in("b", "sub")
        .parallel_gateway("pj")
        .contained_in("pj", "sub")
        .end_event("inner_end")
        .contained_in("inner_end", "sub")
        .service_task("after", "jafter")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "split");
    b = if via_gateway {
        b.exclusive_gateway("g")
            .contained_in("g", "sub")
            .connect("split", "g")
            .connect("split", "stop")
            .connect("g", "pj")
    } else {
        b.connect("split", "stop").connect("split", "pj")
    };
    b.connect("split", "b")
        .connect("b", "pj")
        .connect("pj", "inner_end")
        .connect("sub", "after")
        .connect("after", "done")
        .build()
        .unwrap()
}

#[test]
fn scoped_terminate_drops_a_join_arrival_whose_activation_was_still_queued() {
    // A flow into a join is counted when it is taken (#1241), before the
    // join's activation runs. If a scoped teardown kills the scope in between,
    // the queued activation is dropped, so its counted arrival must go too:
    // otherwise it survives as a phantom token that a re-entered scope would
    // fire against. In Zeebe the count lives on the flow-scope instance and
    // dies with it.
    for via_gateway in [false, true] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(
                scoped_terminate_with_queued_join_arrival(via_gateway),
            ))
            .unwrap();
        let created = engine
            .apply_command(Command::create_instance("term-queued-join"))
            .unwrap();
        let instance_key = created.iter().find_map(|e| e.instance_key()).unwrap();
        let instance = engine.instance(instance_key).unwrap();
        assert!(
            instance.active.values().any(|id| id == "after"),
            "the terminate ends the sub-process only (via_gateway={via_gateway})"
        );
        assert!(
            instance.join_flow_arrivals.is_empty()
                && instance.join_counts.is_empty()
                && instance.join_instances.is_empty(),
            "no join bookkeeping survives the scope (via_gateway={via_gateway}): {:?} {:?} {:?}",
            instance.join_flow_arrivals,
            instance.join_counts,
            instance.join_instances
        );
    }
}
