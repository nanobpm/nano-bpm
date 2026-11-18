//! #1640: mapping-time secret-reference provenance journaled on `JobCreated`.
//!
//! Trust is captured when a service task's input mappings run — against the
//! exact view they evaluated — never re-derived later. Covers: a trusted
//! reference is recorded; an untrusted same-name reference is not; process /
//! parent-scope shadowing of a trusted cluster root; overwrite by a later
//! mapping; per-path (sibling) isolation; mutation of the cluster variables
//! after the mapping ran; and that no plaintext ever reaches the journal or
//! snapshot.
use std::sync::{Arc as StdArc, RwLock};

use super::*;
use crate::cluster_vars::{ClusterVariableSnapshot, DEFAULT_TENANT};
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
fn engine_with_cluster_vars() -> (Engine, crate::cluster_vars::ClusterVariables) {
    let mut snap = ClusterVariableSnapshot::default();
    snap.global
        .insert("apiCfg".to_string(), Value::Str(REF.to_string()));
    snap.global
        .insert("jsonCfg".to_string(), Value::Str(REF.to_string()));
    snap.global_secret_references.insert("apiCfg".to_string());
    let handle = StdArc::new(RwLock::new(snap));
    let mut engine = Engine::new();
    engine.set_cluster_variables(StdArc::clone(&handle));
    (engine, handle)
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
    vars: HashMap<String, Value>,
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
    let (mut engine, _) = engine_with_cluster_vars();
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
        HashMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
    );
    assert_eq!(prov, vec![site("token"), site("direct.inner")]);
    // The job state carries the same provenance (what activation reads).
    let job = engine.state.jobs.values().next().unwrap();
    assert_eq!(job.secret_provenance, prov);
}

#[test]
fn a_process_variable_shadowing_a_trusted_root_is_untrusted() {
    let (mut engine, _) = engine_with_cluster_vars();
    // Same name as the trusted root AND the same reference text: still
    // untrusted, because the mapping read the process variable.
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        HashMap::from([("apiCfg".to_string(), Value::Str(REF.to_string()))]),
    );
    assert!(prov.is_empty(), "{prov:?}");
}

#[test]
fn a_parent_scope_variable_shadowing_a_trusted_root_is_untrusted() {
    let (mut engine, _) = engine_with_cluster_vars();
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
        HashMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
    );
    assert!(prov.is_empty(), "{prov:?}");
}

#[test]
fn a_later_untrusted_mapping_overwrites_provenance() {
    let (mut engine, _) = engine_with_cluster_vars();
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
        HashMap::from([("bodyRef".to_string(), Value::Str(REF.to_string()))]),
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
    let (mut engine, handle) = engine_with_cluster_vars();
    handle
        .write()
        .unwrap()
        .tenants
        .entry(DEFAULT_TENANT.to_string())
        .or_default()
        .insert("apiCfg".to_string(), Value::Str(REF.to_string()));
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        HashMap::new(),
    );
    assert!(prov.is_empty(), "{prov:?}");
}

#[test]
fn provenance_is_fixed_at_mapping_time() {
    let (mut engine, handle) = engine_with_cluster_vars();
    let (prov, _) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        HashMap::new(),
    );
    assert_eq!(prov, vec![site("token")]);
    // Deleting / retyping the cluster variable afterwards changes nothing
    // recorded: activation never re-derives trust.
    *handle.write().unwrap() = ClusterVariableSnapshot::default();
    let job = engine.state.jobs.values().next().unwrap();
    assert_eq!(job.secret_provenance, vec![site("token")]);
}

/// The journal and the snapshot carry only reference text, paths and names —
/// never a secret's plaintext (the engine never sees one; scan the bytes).
#[cfg(feature = "serde")]
#[test]
fn journal_and_snapshot_hold_no_plaintext() {
    let (mut engine, _) = engine_with_cluster_vars();
    let (_, log) = run(
        &mut engine,
        task(vec![m("=apiCfg", "token")]),
        HashMap::new(),
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
