//! Join bookkeeping is keyed by **flow-scope instance** (#1580, Zeebe parity:
//! a parallel/inclusive join counts taken flows per flow-scope instance).
//!
//! Any scope that can have more than one live instance at once — a
//! multi-instance body's children, an embedded sub-process entered by two
//! concurrent tokens, concurrent non-interrupting event sub-process
//! executions — must not let one execution's arrival satisfy another
//! execution's join. These tests pin that for every inner join kind.
use super::*;

/// The inner pattern every test shares: `inner_start -> fork(split) ->
/// {a, b} -> join -> inner_end`, all contained in `container`. `join` is a
/// parallel or inclusive gateway.
fn with_inner_fork_join(b: ProcessBuilder, container: &str, inclusive: bool) -> ProcessBuilder {
    let b = b
        .start_event("inner_start")
        .contained_in("inner_start", container)
        .parallel_gateway("fork")
        .contained_in("fork", container)
        .service_task("a", "a-work")
        .contained_in("a", container)
        .service_task("b", "b-work")
        .contained_in("b", container);
    let b = if inclusive {
        b.inclusive_gateway("join")
    } else {
        b.parallel_gateway("join")
    };
    b.contained_in("join", container)
        .end_event("inner_end")
        .contained_in("inner_end", container)
        .connect("inner_start", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "inner_end")
}

/// `start -> wave (parallel MI over `items`, inner fork/join) -> sink -> end`.
fn mi_body_with_inner_join(inclusive: bool) -> ProcessDefinition {
    let b = ProcessBuilder::new("mi-join")
        .start_event("start")
        .sub_process("wave", "inner_start")
        .with_multi_instance(
            "wave",
            crate::model::MultiInstance {
                input_collection: "=items".to_string(),
                input_element: Some("item".to_string()),
                output_collection: None,
                output_element: None,
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: false,
            },
        );
    with_inner_fork_join(b, "wave", inclusive)
        .service_task("sink", "sink-work")
        .end_event("end")
        .connect("start", "wave")
        .connect("wave", "sink")
        .connect("sink", "end")
        .build()
        .unwrap()
}

/// `start -> split -> {sub, sub}` : one embedded sub-process entered twice,
/// concurrently, by a parallel split; each completion reaches `end`.
fn reentered_subprocess_with_inner_join(inclusive: bool) -> ProcessDefinition {
    let b = ProcessBuilder::new("reenter-join")
        .start_event("start")
        .parallel_gateway("split")
        .sub_process("sub", "inner_start");
    with_inner_fork_join(b, "sub", inclusive)
        .end_event("end")
        .connect("start", "split")
        .connect("split", "sub")
        .connect("split", "sub")
        .connect("sub", "end")
        .build()
        .unwrap()
}

/// The flow scope (enclosing sub-process instance) of a job's element.
fn scope_of_job(engine: &Engine, job: &ActivatedJob) -> Key {
    engine.instance(job.instance_key).unwrap().scopes[&job.element_instance_key]
}

/// Complete `a` in one scope instance and `b` in ANOTHER: no join may fire,
/// because neither scope instance has seen both of its incoming flows. Then
/// complete the remaining two jobs: each scope's join fires exactly once.
fn assert_joins_count_per_scope_instance(engine: &mut Engine, key: Key) {
    let a_jobs = engine.activate_jobs("a-work", "w", 10, 60_000, 0);
    let b_jobs = engine.activate_jobs("b-work", "w", 10, 60_000, 0);
    assert_eq!(a_jobs.len(), 2, "one `a` per scope instance");
    assert_eq!(b_jobs.len(), 2, "one `b` per scope instance");
    let first_scope = scope_of_job(engine, &a_jobs[0]);
    let (b_other, b_same): (Vec<_>, Vec<_>) = b_jobs
        .iter()
        .partition(|j| scope_of_job(engine, j) != first_scope);
    assert_eq!((b_other.len(), b_same.len()), (1, 1));

    let mut events = engine
        .apply_command(Command::complete_job(a_jobs[0].key))
        .unwrap();
    events.extend(
        engine
            .apply_command(Command::complete_job(b_other[0].key))
            .unwrap(),
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::ParallelJoinFired { .. })),
        "a join fired on arrivals from two different scope instances: {events:#?}"
    );
    assert!(
        !events.iter().any(|e| matches!(
            e,
            Event::ElementCompleted { element_id, .. } if element_id == "join"
        )),
        "no scope instance's join may complete yet"
    );

    let mut events = engine
        .apply_command(Command::complete_job(a_jobs[1].key))
        .unwrap();
    events.extend(
        engine
            .apply_command(Command::complete_job(b_same[0].key))
            .unwrap(),
    );
    let fired = events
        .iter()
        .filter(|e| matches!(e, Event::ParallelJoinFired { .. }))
        .count();
    assert_eq!(
        fired, 2,
        "each scope instance's join fires once: {events:#?}"
    );
    let instance = engine.instance(key).unwrap();
    assert!(
        instance.join_flow_arrivals.is_empty()
            && instance.join_counts.is_empty()
            && instance.join_instances.is_empty(),
        "no join bookkeeping survives both joins firing"
    );
}

fn start(engine: &mut Engine, def: ProcessDefinition, vars: HashMap<String, Value>) -> Key {
    let id = def.id.clone();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    engine
        .apply_command(Command::create_instance_with(id, vars))
        .unwrap()
        .iter()
        .find_map(|e| e.instance_key())
        .unwrap()
}

fn two_items() -> HashMap<String, Value> {
    let mut vars = HashMap::new();
    vars.insert(
        "items".to_string(),
        Value::List(vec![Value::Int(1), Value::Int(2)]),
    );
    vars
}

#[test]
fn parallel_join_in_multi_instance_body_counts_per_child_scope() {
    let mut engine = Engine::new();
    let key = start(&mut engine, mi_body_with_inner_join(false), two_items());
    assert_joins_count_per_scope_instance(&mut engine, key);
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(key));
}

#[test]
fn inclusive_join_in_multi_instance_body_counts_per_child_scope() {
    let mut engine = Engine::new();
    let key = start(&mut engine, mi_body_with_inner_join(true), two_items());
    assert_joins_count_per_scope_instance(&mut engine, key);
    complete_one(&mut engine, "sink-work");
    assert!(engine.is_completed(key));
}

#[test]
fn parallel_join_in_concurrently_reentered_subprocess_counts_per_instance() {
    let mut engine = Engine::new();
    let key = start(
        &mut engine,
        reentered_subprocess_with_inner_join(false),
        HashMap::new(),
    );
    assert_joins_count_per_scope_instance(&mut engine, key);
    assert!(engine.is_completed(key));
}

#[test]
fn inclusive_join_in_concurrently_reentered_subprocess_counts_per_instance() {
    let mut engine = Engine::new();
    let key = start(
        &mut engine,
        reentered_subprocess_with_inner_join(true),
        HashMap::new(),
    );
    assert_joins_count_per_scope_instance(&mut engine, key);
    assert!(engine.is_completed(key));
}

/// A journal written before #1580 carries no `scope` on its join events.
/// Replaying it must still key the join by its flow-scope instance: legacy
/// arrivals counted before the join opened are re-keyed by
/// `ParallelJoinOpened`, and a legacy firing/reset addresses the element's
/// single slot. Replaying the scope-stripped journal reconstructs exactly the
/// state the scoped journal does, after every command (a command's events are
/// journaled atomically, so mid-command prefixes are never a resting state).
#[test]
fn legacy_scopeless_join_events_replay_into_scoped_bookkeeping() {
    for inclusive in [false, true] {
        let mut engine = Engine::new();
        let mut one = HashMap::new();
        one.insert("items".to_string(), Value::List(vec![Value::Int(1)]));
        let mut journal = Vec::new();
        let mut boundaries = Vec::new();
        let def = mi_body_with_inner_join(inclusive);
        let id = def.id.clone();
        journal.extend(engine.apply_command(Command::DeployProcess(def)).unwrap());
        boundaries.push(journal.len());
        journal.extend(
            engine
                .apply_command(Command::create_instance_with(id, one))
                .unwrap(),
        );
        boundaries.push(journal.len());
        for job_type in ["a-work", "b-work", "sink-work"] {
            journal.extend(complete_one(&mut engine, job_type));
            boundaries.push(journal.len());
        }
        let strip = |e: &Event| {
            let mut e = e.clone();
            match &mut e {
                Event::ParallelJoinTokenArrived { scope, .. }
                | Event::ParallelJoinReset { scope, .. }
                | Event::ParallelJoinFired { scope, .. } => *scope = None,
                _ => {}
            }
            e
        };
        assert!(
            journal
                .iter()
                .any(|e| matches!(e, Event::ParallelJoinOpened { .. })),
            "the scenario opens (parks) a join inside the MI child scope"
        );
        for prefix in boundaries {
            let scoped = Engine::replay_partition(0, journal[..prefix].to_vec());
            let legacy = Engine::replay_partition(
                0,
                journal[..prefix].iter().map(strip).collect::<Vec<_>>(),
            );
            assert_eq!(
                scoped.state(),
                legacy.state(),
                "inclusive={inclusive}: prefix {prefix} diverges"
            );
        }
    }
}

/// A snapshot written before #1580 serialized the join maps as JSON objects
/// keyed by element id. It must still decode, and loading it keys each entry
/// by the flow scope of the open join instance holding it (root when the join
/// instance has no recorded scope). The new form round-trips losslessly.
#[cfg(feature = "serde")]
#[test]
fn legacy_element_keyed_join_maps_load_into_scoped_bookkeeping() {
    let mut engine = Engine::new();
    let key = start(&mut engine, mi_body_with_inner_join(false), {
        let mut one = HashMap::new();
        one.insert("items".to_string(), Value::List(vec![Value::Int(1)]));
        one
    });
    complete_one(&mut engine, "a-work");
    let instance = engine.instance(key).unwrap().clone();
    let (join, eik) = instance
        .join_instances
        .iter()
        .next()
        .map(|(k, v)| (k.clone(), *v))
        .expect("the MI child's join is open");
    assert_ne!(join.scope, 0, "the join waits inside the MI child scope");

    // Round-trip of the current form is lossless.
    let mut json = serde_json::to_value(&instance).unwrap();
    let back: crate::state::ProcessInstance = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(back, instance);

    // Rewrite the three join maps into the legacy element-keyed object form.
    let arrivals = serde_json::to_value(&instance.join_flow_arrivals[&join]).unwrap();
    json["join_flow_arrivals"] = serde_json::json!({ "join": arrivals });
    json["join_instances"] = serde_json::json!({ "join": eik });
    json["join_counts"] = serde_json::json!({});
    let mut legacy: crate::state::ProcessInstance = serde_json::from_value(json).unwrap();
    assert!(
        legacy
            .join_instances
            .keys()
            .all(|k| k.scope == crate::state::UNRESOLVED_JOIN_SCOPE),
        "a legacy entry decodes unresolved until load"
    );
    legacy.resolve_legacy_join_scopes();
    assert_eq!(
        legacy, instance,
        "load keys the legacy join by its flow scope"
    );
}

/// Legacy (pre-#1580) scope-less join records address the element's single
/// slot, whichever flow-scope instance it lives in. These drive the state
/// applier directly with hand-built legacy sequences that a scoped engine can
/// no longer emit, asserting the outcome a legacy snapshot of the same history
/// loads into (one slot per join element).
mod legacy_element_wide {
    use super::*;
    use crate::state::{JoinKey, State, UNRESOLVED_JOIN_SCOPE};

    const INSTANCE: Key = 1;
    const SCOPE_A: Key = 100;
    const SCOPE_B: Key = 200;
    const JOIN_A: Key = 10;
    const JOIN_B: Key = 20;

    fn state() -> State {
        let mut engine = Engine::new();
        let key = start(&mut engine, mi_body_with_inner_join(false), two_items());
        let mut state = engine.state().clone();
        let mut instance = state.instances.remove(&key).unwrap();
        instance.key = INSTANCE;
        instance.join_counts.clear();
        instance.join_flow_arrivals.clear();
        instance.join_instances.clear();
        instance.parked_joins.clear();
        state.instances.clear();
        state.instances.insert(INSTANCE, instance);
        state
    }

    fn flow(from: &str) -> IncomingFlow {
        IncomingFlow {
            from: from.into(),
            ordinal: 0,
        }
    }

    fn opened(eik: Key, element_id: &str, scope: Key) -> Event {
        Event::ParallelJoinOpened {
            instance_key: INSTANCE,
            element_instance_key: eik,
            element_id: element_id.into(),
            scope: Some(scope),
        }
    }

    fn arrived(element_id: &str, from: &str) -> Event {
        Event::ParallelJoinTokenArrived {
            instance_key: INSTANCE,
            element_id: element_id.into(),
            flow: Some(flow(from)),
            scope: None,
        }
    }

    fn instance(state: &State) -> &crate::state::ProcessInstance {
        &state.instances[&INSTANCE]
    }

    /// A legacy firing in scope A keeps its surplus in the element's single
    /// slot, which the join re-opened in scope B adopts.
    #[test]
    fn legacy_fire_then_reopen_in_another_scope_carries_the_surplus() {
        let mut state = state();
        for e in [
            opened(JOIN_A, "join", SCOPE_A),
            arrived("join", "a"),
            arrived("join", "a"),
            arrived("join", "b"),
            Event::ParallelJoinFired {
                instance_key: INSTANCE,
                element_id: "join".into(),
                scope: None,
            },
            opened(JOIN_B, "join", SCOPE_B),
        ] {
            crate::state::apply(&mut state, &e);
        }
        let i = instance(&state);
        let slot = JoinKey::new(SCOPE_B, "join");
        assert_eq!(i.join_keys(), vec![slot.clone()]);
        assert_eq!(i.join_flow_arrivals[&slot].count(&flow("a")), 1);
        assert_eq!(i.join_instances[&slot], JOIN_B);
    }

    /// A legacy reset is element-wide: it drops every parked key of the
    /// element even when no join-map entry names their scope any more.
    #[test]
    fn legacy_reset_after_fire_drops_every_parked_key() {
        let mut state = state();
        for e in [
            opened(JOIN_A, "join", SCOPE_A),
            opened(JOIN_B, "join", SCOPE_B),
            Event::ParallelJoinFired {
                instance_key: INSTANCE,
                element_id: "join".into(),
                scope: None,
            },
            Event::ParallelJoinReset {
                instance_key: INSTANCE,
                element_id: "join".into(),
                scope: None,
            },
        ] {
            crate::state::apply(&mut state, &e);
        }
        let i = instance(&state);
        assert!(i.parked_joins.is_empty(), "no phantom parked join remains");
        assert!(!i.active.contains_key(&JOIN_A) && !i.active.contains_key(&JOIN_B));
        assert!(i.join_keys().is_empty());
    }

    /// A legacy migration merged joins element-wide; a scoped one keeps each
    /// flow-scope instance's join separate.
    #[test]
    fn legacy_migration_merges_joins_across_scopes() {
        for join_scoped in [false, true] {
            let mut state = state();
            for e in [
                opened(JOIN_A, "join-a", SCOPE_A),
                arrived("join-a", "a"),
                opened(JOIN_B, "join-b", SCOPE_B),
                arrived("join-b", "b"),
            ] {
                crate::state::apply(&mut state, &e);
            }
            let process_id = instance(&state).process_id.clone();
            let pdk = instance(&state).process_definition_key;
            crate::state::apply(
                &mut state,
                &Event::ProcessInstanceMigrated {
                    instance_key: INSTANCE,
                    target_process_id: process_id,
                    target_process_definition_key: pdk,
                    element_mappings: vec![
                        ("join-a".into(), "join".into()),
                        ("join-b".into(), "join".into()),
                    ],
                    migrated_flow_scopes: vec![],
                    subscriptions: vec![],
                    child_instances: vec![],
                    join_scoped,
                },
            );
            let i = instance(&state);
            if join_scoped {
                assert_eq!(
                    i.join_keys(),
                    vec![JoinKey::new(SCOPE_A, "join"), JoinKey::new(SCOPE_B, "join")]
                );
            } else {
                let slot = JoinKey::new(SCOPE_A, "join");
                assert_eq!(i.join_keys(), vec![slot.clone()]);
                assert_eq!(i.join_instances[&slot], JOIN_A);
                assert_eq!(i.join_flow_arrivals[&slot].distinct_flows(), 2);
            }
            assert!(i
                .join_keys()
                .iter()
                .all(|k| k.scope != UNRESOLVED_JOIN_SCOPE));
        }
    }
}
