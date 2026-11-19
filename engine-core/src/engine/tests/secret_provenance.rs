//! #1640: mapping-time secret-reference provenance journaled on `JobCreated`.
//!
//! Trust is captured when a service task's input mappings run — against the
//! exact view they evaluated — never re-derived later. Covers: a trusted
//! reference is recorded; an untrusted same-name reference is not; process /
//! parent-scope shadowing of a trusted cluster root; overwrite by a later
//! mapping; per-path (sibling) isolation; mutation of the cluster variables
//! after the mapping ran; and that no plaintext ever reaches the journal or
//! snapshot.
use super::*;
use crate::cluster_vars::{ClusterVariableKind, DEFAULT_TENANT};
use crate::model::{IoMapping, Mapping};
use crate::state::SecretProvenance;

const REF: &str = "camunda.secrets.TOKEN";

fn m(source: &str, target: &str) -> Mapping {
    Mapping {
        source: source.to_string(),
        target: target.to_string(),
    }
}

/// An engine whose cluster variables hold a SECRET_REFERENCE `apiCfg` and a
/// JSON-kind `jsonCfg`, both carrying `camunda.secrets.TOKEN`.
fn engine_with_cluster_vars() -> Engine {
    let mut engine = Engine::new();
    put_cluster_var(
        &mut engine,
        None,
        "apiCfg",
        ClusterVariableKind::SecretReference,
    );
    put_cluster_var(&mut engine, None, "jsonCfg", ClusterVariableKind::Json);
    engine
}

/// Creates a cluster variable holding [`REF`] through the replicated command
/// (#1657) — the only way cluster-variable state changes.
fn put_cluster_var(
    engine: &mut Engine,
    tenant: Option<&str>,
    name: &str,
    kind: ClusterVariableKind,
) {
    engine
        .apply_command(Command::CreateClusterVariable {
            tenant_id: tenant.map(str::to_string),
            name: name.to_string(),
            value: Value::Str(REF.to_string()),
            kind,
            metadata: Default::default(),
            distributed: false,
        })
        .unwrap();
}

fn task(inputs: Vec<Mapping>) -> ProcessDefinition {
    ProcessBuilder::new("p")
        .start_event("s")
        .service_task("call", "call-api")
        .with_io(
            "call",
            IoMapping {
                inputs,
                outputs: Vec::new(),
            },
        )
        .end_event("e")
        .connect("s", "call")
        .connect("call", "e")
        .build()
        .unwrap()
}

/// Deploys `def`, creates an instance with `vars`, and returns the
/// provenance journaled on the instance's `JobCreated` plus every event.
fn run(
    engine: &mut Engine,
    def: ProcessDefinition,
    vars: BTreeMap<String, Value>,
) -> (Vec<SecretProvenance>, Vec<Event>) {
    let proc_id = def.id.clone();
    let mut log = engine.apply_command(Command::DeployProcess(def)).unwrap();
    log.extend(
        engine
            .apply_command(Command::create_instance_with(proc_id, vars))
            .unwrap(),
    );
    let provenance = log
        .iter()
        .find_map(|e| match e {
            Event::JobCreated {
                secret_provenance, ..
            } => Some(secret_provenance.clone()),
            _ => None,
        })
        .expect("a JobCreated");
    (provenance, log)
}

fn site(path: &str) -> SecretProvenance {
    SecretProvenance {
        path: path.to_string(),
        secrets: vec!["TOKEN".to_string()],
    }
}

#[test]
fn trusted_sources_are_recorded_untrusted_are_not() {
    let mut engine = engine_with_cluster_vars();
    let (prov, _) = run(
        &mut engine,
        task(vec![
            m("=apiCfg", "token"),
            m("=camunda.secrets.TOKEN", "direct.inner"),
            m("=jsonCfg", "fromJson"),
            m("=bodyRef", "fromBody"),
            m("camunda.secrets.TOKEN", "static"),
            m("=if true then apiCfg else jsonCfg", "computed"),
        ]),
        BTreeMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
    );
    assert_eq!(prov, vec![site("token"), site("direct.inner")]);
    // The job state carries the same provenance (what activation reads).
    let job = engine.state.jobs.values().next().unwrap();
    assert_eq!(job.secret_provenance, prov);
}

#[test]
fn a_process_variable_shadowing_a_trusted_root_is_untrusted() {
    let mut engine = engine_with_cluster_vars();
    // Same name as the trusted root AND the same reference text: still
    // untrusted, because the mapping read the process variable.
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        BTreeMap::from([("apiCfg".to_string(), Value::Str(REF.to_string()))]),
    );
    assert!(prov.is_empty(), "{prov:?}");
}

#[test]
fn a_parent_scope_variable_shadowing_a_trusted_root_is_untrusted() {
    let mut engine = engine_with_cluster_vars();
    let def = ProcessBuilder::new("nested")
        .start_event("start")
        .sub_process("sub", "sub_start")
        .with_io(
            "sub",
            IoMapping {
                inputs: vec![m("=bodyRef", "apiCfg")],
                outputs: Vec::new(),
            },
        )
        .start_event("sub_start")
        .contained_in("sub_start", "sub")
        .service_task("inner", "work")
        .with_io(
            "inner",
            IoMapping {
                inputs: vec![m("=apiCfg", "token")],
                outputs: Vec::new(),
            },
        )
        .contained_in("inner", "sub")
        .end_event("sub_end")
        .contained_in("sub_end", "sub")
        .end_event("done")
        .connect("start", "sub")
        .connect("sub_start", "inner")
        .connect("inner", "sub_end")
        .connect("sub", "done")
        .build()
        .unwrap();
    let (prov, _) = run(
        &mut engine,
        def,
        BTreeMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
    );
    assert!(prov.is_empty(), "{prov:?}");
}

#[test]
fn a_later_untrusted_mapping_overwrites_provenance() {
    let mut engine = engine_with_cluster_vars();
    let (prov, _) = run(
        &mut engine,
        task(vec![
            m("=apiCfg", "same"),
            m("=bodyRef", "same"),
            m("=apiCfg", "parent.child"),
            m("=bodyRef", "parent"),
            m("=apiCfg", "whole"),
            m("=bodyRef", "whole.child"),
            m("=bodyRef", "retrusted"),
            m("=apiCfg", "retrusted"),
        ]),
        BTreeMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
    );
    assert_eq!(
        prov,
        vec![site("retrusted")],
        "an overwritten path (or its ancestor / descendant) loses trust; a path \
         re-written by an attributable mapping regains it"
    );
}

#[test]
fn a_jsonkind_tenant_variable_shadowing_a_trusted_global_is_untrusted() {
    let mut engine = engine_with_cluster_vars();
    put_cluster_var(
        &mut engine,
        Some(DEFAULT_TENANT),
        "apiCfg",
        ClusterVariableKind::Json,
    );
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        BTreeMap::new(),
    );
    assert!(prov.is_empty(), "{prov:?}");
}

#[test]
fn provenance_is_fixed_at_mapping_time() {
    let mut engine = engine_with_cluster_vars();
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        BTreeMap::new(),
    );
    assert_eq!(prov, vec![site("token")]);
    // Deleting / retyping the cluster variable afterwards changes nothing
    // recorded: activation never re-derives trust.
    engine
        .apply_command(Command::DeleteClusterVariable {
            tenant_id: None,
            name: "apiCfg".to_string(),
            distributed: false,
        })
        .unwrap();
    let job = engine.state.jobs.values().next().unwrap();
    assert_eq!(job.secret_provenance, vec![site("token")]);
}

/// The journal and the snapshot carry only reference text, paths and names —
/// never a secret's plaintext (the engine never sees one; scan the bytes).
#[cfg(feature = "serde")]
#[test]
fn journal_and_snapshot_hold_no_plaintext() {
    let mut engine = engine_with_cluster_vars();
    let (_, log) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        BTreeMap::new(),
    );
    let journal = serde_json::to_vec(&log).unwrap();
    let snapshot = serde_json::to_vec(&engine.snapshot()).unwrap();
    for bytes in [&journal, &snapshot] {
        let text = String::from_utf8_lossy(bytes);
        assert!(text.contains("secret_provenance"));
        assert!(text.contains(REF));
        assert!(!text.contains("s3cr3t"), "plaintext leaked: {text}");
    }
    // The provenance survives a snapshot round-trip.
    let restored =
        Engine::from_snapshot(serde_json::from_slice::<crate::EngineSnapshot>(&snapshot).unwrap());
    assert_eq!(
        restored
            .state
            .jobs
            .values()
            .next()
            .unwrap()
            .secret_provenance,
        vec![site("token")]
    );
}

/// Aliased spellings of the same field (`payload..auth`, `.payload.auth`)
/// write what `payload.auth` writes, so they must overwrite its provenance.
#[test]
fn an_aliased_target_overwrites_provenance() {
    for alias in ["payload..auth", ".payload.auth", "payload.auth.", "payload"] {
        let mut engine = engine_with_cluster_vars();
        let (prov, _) = run(
            &mut engine,
            task(vec![m("=apiCfg", "payload.auth"), m("=bodyRef", alias)]),
            BTreeMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
        );
        assert!(prov.is_empty(), "alias {alias}: {prov:?}");
    }
    // A trusted write through an aliased spelling is recorded canonically.
    let mut engine = engine_with_cluster_vars();
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "payload..auth")]),
        BTreeMap::new(),
    );
    assert_eq!(prov, vec![site("payload.auth")]);
}

/// Any variable write to the instance after `JobCreated` may replace the
/// trusted value (even with equal reference text), so it revokes the job's
/// provenance — local element-scope and propagating root writes alike.
#[test]
fn a_post_creation_variable_write_revokes_provenance() {
    for local in [true, false] {
        let mut engine = engine_with_cluster_vars();
        let (prov, _) = run(
            &mut engine,
            task(vec![m("=apiCfg", "payload.auth")]),
            BTreeMap::new(),
        );
        assert_eq!(prov, vec![site("payload.auth")]);
        let job = engine.state.jobs.values().next().unwrap().clone();
        let scope = if local {
            job.element_instance_key
        } else {
            job.instance_key
        };
        let mut other = std::collections::BTreeMap::new();
        other.insert("other".to_string(), Value::Str(REF.to_string()));
        engine
            .apply_command(Command::set_variables_scoped(
                scope,
                BTreeMap::from([("payload".to_string(), Value::Map(other))]),
                local,
            ))
            .unwrap();
        let job = engine.state.jobs.values().next().unwrap();
        assert!(job.secret_provenance.is_empty(), "local={local}");
    }
}

/// A write to a scope the job cannot see (a sibling branch, e.g. a parallel
/// task's own input-mapping scope) cannot replace the job's inputs, so it must
/// keep the job's provenance; a write to an ancestor scope still revokes it.
#[test]
fn a_sibling_scope_write_preserves_provenance() {
    let mut engine = engine_with_cluster_vars();
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "payload.auth")]),
        BTreeMap::new(),
    );
    assert_eq!(prov, vec![site("payload.auth")]);
    let job = engine.state.jobs.values().next().unwrap().clone();
    let sibling = job.element_instance_key + 1_000_000;
    let write = |scope_key| crate::event::Event::ScopedVariablesUpdated {
        instance_key: job.instance_key,
        scope_key,
        variables: BTreeMap::from([("payload".to_string(), Value::Str(REF.to_string()))]),
    };
    crate::state::apply(&mut engine.state, &write(sibling));
    assert_eq!(
        engine.state.jobs[&job.key].secret_provenance,
        vec![site("payload.auth")]
    );
    crate::state::apply(&mut engine.state, &write(job.element_instance_key));
    assert!(engine.state.jobs[&job.key].secret_provenance.is_empty());
}

/// #1640 check-then-use race, closed structurally by #1657: cluster-variable
/// values and kinds are engine state changed only by their own commands, so
/// there is no second read that a concurrent retype could land between. A
/// retype is itself a replicated command, ordered against the evaluation.
#[test]
fn a_retype_is_a_command_ordered_against_evaluation() {
    let mut engine = engine_with_cluster_vars();
    // Retype `jsonCfg` -> SECRET_REFERENCE through the (distributed upsert)
    // command: only evaluations AFTER it in the log observe the new kind.
    let (before, _) = run(&mut engine, task(vec![m("=jsonCfg", "t")]), BTreeMap::new());
    assert!(before.is_empty(), "{before:?}");
    engine
        .apply_command(Command::CreateClusterVariable {
            tenant_id: None,
            name: "jsonCfg".to_string(),
            value: Value::Str(REF.to_string()),
            kind: ClusterVariableKind::SecretReference,
            metadata: Default::default(),
            distributed: true,
        })
        .unwrap();
    let after = engine
        .apply_command(Command::create_instance_with("p", BTreeMap::new()))
        .unwrap()
        .into_iter()
        .find_map(|e| match e {
            Event::JobCreated {
                secret_provenance, ..
            } => Some(secret_provenance),
            _ => None,
        })
        .expect("a JobCreated");
    assert_eq!(after, vec![site("t")]);
}

/// A trusted root is shadowed only by a binding on the mapping's OWN visible
/// scope chain. A same-named local in a SIBLING parallel branch's scope is
/// invisible to this mapping's evaluation, so it must NOT revoke the trust:
/// the sibling's `=apiCfg` still reads the trusted cluster variable and its
/// job must keep its provenance (and its missing-secret incident handling).
/// The pre-fix check scanned EVERY scope of the instance, so one branch's
/// local `apiCfg` stripped the sibling's trust (#1652 round-5 review).
#[test]
fn a_sibling_branch_local_does_not_shadow_a_trusted_root() {
    let mut engine = engine_with_cluster_vars();
    // fork -> (a, b) -> join. `a` maps an untrusted bodyRef onto a LOCAL
    // `apiCfg`; `b` reads the trusted cluster `apiCfg`.
    let def = ProcessBuilder::new("par")
        .start_event("s")
        .parallel_gateway("fork")
        .service_task("a", "task-a")
        .with_io(
            "a",
            IoMapping {
                inputs: vec![m("=bodyRef", "apiCfg")],
                outputs: Vec::new(),
            },
        )
        .service_task("b", "task-b")
        .with_io(
            "b",
            IoMapping {
                inputs: vec![m("=apiCfg", "token")],
                outputs: Vec::new(),
            },
        )
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "e")
        .build()
        .unwrap();
    let proc_id = def.id.clone();
    let mut log = engine.apply_command(Command::DeployProcess(def)).unwrap();
    log.extend(
        engine
            .apply_command(Command::create_instance_with(
                proc_id,
                BTreeMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
            ))
            .unwrap(),
    );
    // b's job must be trusted (its `=apiCfg` reads the cluster variable); a's
    // own local `apiCfg` must not have revoked it. Find b's job by type.
    let b_job = engine
        .state
        .jobs
        .values()
        .find(|j| j.job_type == "task-b")
        .expect("task-b job");
    assert_eq!(
        b_job.secret_provenance,
        vec![site("token")],
        "a sibling branch's local `apiCfg` must not shadow the trusted cluster root"
    );
    // a's job wrote an UNTRUSTED local apiCfg (from bodyRef): no provenance.
    let a_job = engine
        .state
        .jobs
        .values()
        .find(|j| j.job_type == "task-a")
        .expect("task-a job");
    assert!(
        a_job.secret_provenance.is_empty(),
        "{:?}",
        a_job.secret_provenance
    );
}

/// Value and provenance come from ONE evaluation of each mapping. A
/// nondeterministic source (`random number()`) re-evaluated for provenance
/// after the value was computed could take a different branch, so the
/// journaled value and its recorded trust would describe different runs — an
/// untrusted field the value-evaluation kept could then be resolved to
/// plaintext at activation. The fused evaluator never re-evaluates, so the
/// provenance always describes the journaled value (#1652 round-5 review).
///
/// Seeded: the engine's deterministic FEEL environment (#1642) is installed
/// so `random number()` is reproducible. Across seeds the single evaluation
/// yields EITHER `other = bodyRef` (untrusted text, withheld) OR `other =
/// null`; in BOTH the recorded provenance must match the journaled value —
/// never trust a target whose journaled value carries the untrusted text.
#[test]
fn provenance_matches_the_single_evaluated_value_under_nondeterminism() {
    let mut engine = engine_with_cluster_vars();
    let inst = {
        let def = task(vec![m(
            "={auth: camunda.secrets.TOKEN, other: if random number() < 0.5 then bodyRef else null}",
            "payload",
        )]);
        let proc_id = def.id.clone();
        let mut log = engine.apply_command(Command::DeployProcess(def)).unwrap();
        log.extend(
            engine
                .apply_command(Command::create_instance_with(
                    proc_id,
                    BTreeMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
                ))
                .unwrap(),
        );
        log.iter()
            .find_map(|e| match e {
                Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
                _ => None,
            })
            .unwrap()
    };
    let vars = engine.variables_for_element(inst, 0);
    let mappings = vec![m(
        "={auth: camunda.secrets.TOKEN, other: if random number() < 0.5 then bodyRef else null}",
        "payload",
    )];

    // Across several seeded environments the single evaluation must keep
    // value and provenance consistent: whenever the journaled `payload.other`
    // carries the untrusted reference text, `payload` must NOT be trusted.
    let mut saw_body_ref = false;
    let mut saw_null = false;
    for seed in 0..40u64 {
        let mut env = crate::feel::EvalEnv::new(1_700_000_000_000, seed);
        let (updates, prov) = crate::feel::with_env(&mut env, || {
            match engine.eval_input_mappings_with_provenance(inst, 0, &vars, &mappings) {
                Ok(evaluated) => evaluated,
                Err(f) => panic!("seed {seed}: mapping failed: {}", f.reason),
            }
        });
        let other = updates
            .get("payload")
            .and_then(|p| match p {
                Value::Map(map) => map.get("other"),
                _ => None,
            })
            .cloned();
        let trusted = prov.iter().any(|p| p.path == "payload");
        match other {
            Some(Value::Str(s)) if s == REF => {
                saw_body_ref = true;
                assert!(
                    !trusted,
                    "seed {seed}: journaled value carries the untrusted bodyRef text \
                     but `payload` was trusted — value and provenance diverged"
                );
            }
            Some(Value::Null) => {
                saw_null = true;
                // `other` is null; only the trusted literal `auth` remains, so
                // `payload` may be trusted. Either way it must not contradict
                // the value (there is no untrusted text to leak).
            }
            other => panic!("seed {seed}: unexpected payload.other = {other:?}"),
        }
    }
    assert!(
        saw_body_ref && saw_null,
        "expected both branches across the seeds (bodyRef={saw_body_ref}, null={saw_null})"
    );
}
