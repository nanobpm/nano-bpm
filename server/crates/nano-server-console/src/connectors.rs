//! Connectors — the **outbound** I/O edge (ADR 0050), the mirror of
//! [`super::triggers`]'s inbound edge.
//!
//! A *connector* is a pack that ships both an element-template **component**
//! (the design-time face, ADR 0033 §4) and a long-lived **worker** keyed by the
//! component's `zeebe:taskDefinition:type` (the runtime, ADR 0022 §E). Where a
//! trigger is enabled into a project by appending to `triggers[]`, a connector
//! is enabled by appending to `workers[]` a pack-backed entry
//! (`{ taskType, connector, connection? }`, ADR 0050 §Enablement) — the host
//! then resolves + supervises the pack's worker by `taskType`
//! ([`super::trigger_sources::spawn_workers`]).
//!
//! This module is the console seam for that enablement:
//! - [`connectors_overview`] powers the Connectors panel (enabled connectors +
//!   the installable registry + coherence errors) — the mirror of
//!   [`super::triggers::triggers_overview`].
//! - [`add_connector`] is the "Add connector" form — the mirror of
//!   [`super::triggers::add_trigger`].
//! - [`validate_connector_seam`] is the ADR 0027 §4 boot gate: every enabled
//!   connector must still resolve to an installed, launchable, component-backed
//!   pack, or the App fails closed (ADR 0050 §2 seam invariant).

use serde_json::json;

use super::extensions;
use super::triggers::{TriggerError, read_manifest, write_manifest};

type Json = serde_json::Value;

/// One installable connector the registry offers — a pack's declared worker
/// `type`, joined to whether the same pack backs it with a seam component.
struct ConnectorKind {
    /// The worker's job type (the design→runtime seam, ADR 0050 §2).
    task_type: String,
    /// Id of the pack contributing the worker (written to `workers[].connector`).
    connector: String,
    display_name: Option<String>,
    /// Whether the pack also contributes an element-template component whose
    /// `zeebe:taskDefinition:type` equals `task_type`.
    has_component: bool,
    config_fields: Vec<extensions::ConfigField>,
}

/// The `zeebe:taskDefinition:type` an element-template `template` binds, if any
/// (its `properties[]` entry whose `binding.type` is `zeebe:taskDefinition:type`).
/// This is the value a service task created from the component enqueues as its
/// job `type` — the seam a worker subscribes to.
fn component_task_type(template: &Json) -> Option<String> {
    template
        .get("properties")
        .and_then(Json::as_array)?
        .iter()
        .find_map(|p| {
            let is_task_def = p
                .get("binding")
                .and_then(|b| b.get("type"))
                .and_then(Json::as_str)
                == Some("zeebe:taskDefinition:type");
            if !is_task_def {
                return None;
            }
            p.get("value")
                .and_then(Json::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
}

/// The connector registry: every installed pack's declared `workers[]`, joined
/// to that pack's seam components. First pack wins on a duplicate `type`
/// (mirrors [`extensions::worker_driver`]'s first-wins resolution).
fn connector_registry() -> Vec<ConnectorKind> {
    let mut out: Vec<ConnectorKind> = Vec::new();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for ext in extensions::all_extensions() {
        if ext.workers.is_empty() {
            continue;
        }
        let component_types: std::collections::BTreeSet<String> =
            extensions::pack_component_templates(&ext.id)
                .iter()
                .filter_map(component_task_type)
                .collect();
        for w in &ext.workers {
            if seen.insert(w.worker_type.clone()) {
                out.push(ConnectorKind {
                    task_type: w.worker_type.clone(),
                    connector: ext.id.clone(),
                    display_name: w.display_name.clone(),
                    has_component: component_types.contains(&w.worker_type),
                    config_fields: w.config_fields.clone(),
                });
            }
        }
    }
    out
}

fn config_fields_json(fields: &[extensions::ConfigField]) -> Vec<Json> {
    fields
        .iter()
        .map(|f| {
            json!({
                "key": f.key,
                "label": f.label,
                "description": f.description,
                "default": f.default,
                "required": false,
            })
        })
        .collect()
}

/// The App's enabled connectors resolved against the registry (ADR 0050) —
/// powers the Connectors panel and the "Add connector" picker. Each enabled
/// connector is tagged `backed` (the pinned pack still ships a launchable
/// worker) and `recognized` (the type is in the registry at all); incoherent
/// enablements are surfaced as `errors`. The outbound mirror of
/// [`super::triggers::triggers_overview`].
pub(crate) fn connectors_overview(project: &str) -> Result<Json, TriggerError> {
    let manifest = read_manifest(project)?;
    let registry = connector_registry();
    let mut errors: Vec<String> = Vec::new();

    let connectors: Vec<Json> = manifest
        .get("workers")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        // Only pack-backed workers are connectors; project-local `handler`/`llm`
        // workers are authored in the project, not enabled from a pack.
        .filter_map(|w| {
            let connector = w.get("connector").and_then(Json::as_str)?.to_string();
            let task_type = w
                .get("taskType")
                .and_then(Json::as_str)
                .unwrap_or("")
                .to_string();
            let kind = registry.iter().find(|k| k.task_type == task_type);
            let recognized = kind.is_some();
            let backed = extensions::worker_driver_for(&connector, &task_type).is_some();
            if !recognized {
                errors.push(format!(
                    "connector '{task_type}' (pack '{connector}') is enabled but not installed"
                ));
            } else if !backed {
                errors.push(format!(
                    "connector '{task_type}' (pack '{connector}') has no launchable worker \
                     (uninstalled or declaration-only)"
                ));
            }
            Some(json!({
                "taskType": task_type,
                "connector": connector,
                "displayName": kind.and_then(|k| k.display_name.clone()),
                "connection": w.get("connection").and_then(Json::as_str),
                "backed": backed,
                "recognized": recognized,
            }))
        })
        .collect();

    let available: Vec<Json> = registry
        .iter()
        .map(|k| {
            json!({
                "taskType": k.task_type,
                "connector": k.connector,
                "displayName": k.display_name,
                "hasComponent": k.has_component,
                "configFields": config_fields_json(&k.config_fields),
            })
        })
        .collect();

    Ok(json!({ "connectors": connectors, "available": available, "errors": errors }))
}

/// Enable a connector by appending a pack-backed worker to the App manifest's
/// `workers[]` and persisting it (ADR 0050 §Enablement). Optionally writes a
/// named `connections[]` entry (env-pointer config, never inline secrets — ADR
/// 0027 §5) the worker references. The outbound mirror of
/// [`super::triggers::add_trigger`].
pub(crate) fn add_connector(
    project: &str,
    task_type: &str,
    connection: Option<&str>,
    config: &std::collections::BTreeMap<String, String>,
) -> Result<(), TriggerError> {
    let task_type = task_type.trim();
    if task_type.is_empty() {
        return Err(TriggerError::Manifest("connector type is required".into()));
    }
    let registry = connector_registry();
    let Some(kind) = registry.iter().find(|k| k.task_type == task_type) else {
        return Err(TriggerError::Manifest(format!(
            "unknown connector '{task_type}' — install its pack or pick a recognised connector"
        )));
    };
    let connector_id = kind.connector.clone();
    let connection = connection.map(str::trim).filter(|c| !c.is_empty());

    let mut manifest = read_manifest(project)?;
    let obj = manifest
        .as_object_mut()
        .ok_or_else(|| TriggerError::Manifest("nano.app.json is not a JSON object".into()))?;

    {
        let workers = obj.entry("workers").or_insert_with(|| Json::Array(vec![]));
        let arr = workers
            .as_array_mut()
            .ok_or_else(|| TriggerError::Manifest("manifest 'workers' is not an array".into()))?;
        if arr
            .iter()
            .any(|w| w.get("taskType").and_then(Json::as_str) == Some(task_type))
        {
            return Err(TriggerError::Manifest(format!(
                "a worker for '{task_type}' already exists"
            )));
        }
        let mut entry = serde_json::Map::new();
        entry.insert("taskType".into(), json!(task_type));
        entry.insert("connector".into(), json!(connector_id));
        if let Some(c) = connection {
            entry.insert("connection".into(), json!(c));
        }
        arr.push(Json::Object(entry));
    }

    // Persist any supplied config as env-pointer values on the named connection
    // (secrets stay env templates — ADR 0027 §5). Requires a connection name so
    // the worker can reference it; without one, config is surfaced only as the
    // env pointers the maker sets in the environment.
    let cfg: Vec<(&String, &str)> = config
        .iter()
        .map(|(k, v)| (k, v.trim()))
        .filter(|(_, v)| !v.is_empty())
        .collect();
    if let (Some(name), false) = (connection, cfg.is_empty()) {
        let connections = obj
            .entry("connections")
            .or_insert_with(|| Json::Object(serde_json::Map::new()));
        let cobj = connections.as_object_mut().ok_or_else(|| {
            TriggerError::Manifest("manifest 'connections' is not an object".into())
        })?;
        let conn_entry = cobj
            .entry(name.to_string())
            .or_insert_with(|| Json::Object(serde_json::Map::new()));
        let ce = conn_entry.as_object_mut().ok_or_else(|| {
            TriggerError::Manifest(format!("connection '{name}' is not an object"))
        })?;
        // `type` is the connection schema's only required field (ADR 0025 §1).
        ce.entry("type")
            .or_insert_with(|| json!(connector_id.clone()));
        for (k, v) in cfg {
            // Never let a pack's config field clobber the reserved `type`
            // discriminator — that would corrupt the connection and break
            // resolution. `type` is owned by the enablement seam, not config.
            if k == "type" {
                continue;
            }
            ce.insert(k.clone(), json!(v));
        }
    }

    write_manifest(project, &manifest)?;
    Ok(())
}

/// The ADR 0027 §4 boot gate for the ADR 0050 §2 seam: every enabled connector
/// (`workers[]` entry with a `connector`) must still resolve to an installed,
/// launchable, component-backed pack, or the App fails closed. Catches drift a
/// hand-edited manifest or an uninstalled pack introduces — a component with no
/// backing worker would otherwise parse but hang at runtime (ADR 0033 §1). A
/// missing manifest / no connectors is vacuously valid.
pub(crate) fn validate_connector_seam(project: &str) -> Result<(), String> {
    let Ok(manifest) = read_manifest(project) else {
        return Ok(());
    };
    let Some(workers) = manifest.get("workers").and_then(Json::as_array) else {
        return Ok(());
    };
    for w in workers {
        let Some(connector) = w.get("connector").and_then(Json::as_str) else {
            continue; // a project-local handler/llm worker — not a connector.
        };
        let task_type = w.get("taskType").and_then(Json::as_str).unwrap_or("");
        if task_type.is_empty() {
            return Err(format!(
                "connector from pack '{connector}' is missing a taskType in nano.app.json"
            ));
        }
        if extensions::worker_driver_for(connector, task_type).is_none() {
            return Err(format!(
                "connector '{task_type}' (pack '{connector}') enabled in nano.app.json has no \
                 launchable worker — install the pack or remove the worker (ADR 0050)"
            ));
        }
        // The design→runtime seam: the pack must also ship the element-template
        // component whose zeebe:taskDefinition:type is this taskType (ADR 0050 §2).
        let has_component = extensions::pack_component_templates(connector)
            .iter()
            .filter_map(component_task_type)
            .any(|t| t == task_type);
        if !has_component {
            return Err(format!(
                "connector '{task_type}' (pack '{connector}') has no backing element-template \
                 component — the design→runtime seam is broken (ADR 0050 §2)"
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::super::projects;
    use super::*;

    /// Tests here mutate the process-global `NANOBPMN_PROJECTS_DIR` and
    /// `NANOBPMN_EXTENSIONS_DIR`; serialize on the crate-wide guard
    /// ([`projects::env_lock`]) so cargo's parallel runner can't let two of
    /// them read each other's temp dirs. A mutex local to this module would
    /// not serialize against the suites in `projects.rs` / `triggers.rs` /
    /// `lib.rs` mutating the same variables.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        projects::env_lock()
    }

    fn cfg(entries: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// A minimal element template binding `zeebe:taskDefinition:type = task_type`.
    fn component_template(task_type: &str) -> String {
        format!(
            r#"{{
                "id": "com.example.{task_type}",
                "name": "Example {task_type}",
                "appliesTo": ["bpmn:Task"],
                "properties": [
                    {{ "type": "Hidden", "value": "{task_type}",
                      "binding": {{ "type": "zeebe:taskDefinition:type" }} }}
                ]
            }}"#
        )
    }

    /// Materialise a temp projects root with one project holding `manifest`, and
    /// a temp extensions root with a single connector pack. `with_entry` controls
    /// whether the pack's worker resolves to a launchable driver on disk;
    /// `with_component` whether it also ships the seam element template.
    fn setup(manifest: &str, with_entry: bool, with_component: bool) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!("nano-conn-{}-{n}", std::process::id()));

        // Projects root + one project.
        let proj_root = base.join("projects");
        let name = "connapp";
        let dir = proj_root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("nano.app.json"), manifest).unwrap();

        // Extensions root + one connector pack.
        let ext_root = base.join("ext");
        let pack = ext_root.join("nano-ide-connector-testpack");
        std::fs::create_dir_all(&pack).unwrap();
        let entry_decl = if with_entry {
            std::fs::write(pack.join("worker.mjs"), "export default {}\n").unwrap();
            r#", "entry": "worker.mjs""#
        } else {
            ""
        };
        let (components_decl, ship_component) = if with_component {
            (r#", "components": ["tpl.json"]"#, true)
        } else {
            ("", false)
        };
        if ship_component {
            std::fs::write(pack.join("tpl.json"), component_template("test.slack")).unwrap();
        }
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            format!(
                r#"{{
                    "id": "nano-ide-connector-testpack",
                    "kind": "app",
                    "displayName": "Test connector pack"{components_decl},
                    "workers": [
                        {{ "type": "test.slack", "displayName": "Test Slack"{entry_decl},
                          "configFields": [
                            {{ "key": "token", "label": "API token", "env": "SLACK_TOKEN" }}
                          ] }}
                    ]
                }}"#
            ),
        )
        .unwrap();

        unsafe {
            std::env::set_var("NANOBPMN_PROJECTS_DIR", &proj_root);
            std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext_root);
        }
        name.to_string()
    }

    const BARE_APP: &str = r#"{ "data": { "default": "app", "sources": {
        "app": { "driver": "sqlite", "url": "file:./app.db", "migrations": "db/migrations" } } } }"#;

    #[test]
    fn overview_lists_installable_registry_when_none_enabled() {
        let _g = env_lock();
        let name = setup(BARE_APP, true, true);
        let ov = connectors_overview(&name).unwrap();
        assert_eq!(ov["connectors"].as_array().unwrap().len(), 0);
        let available = ov["available"].as_array().unwrap();
        assert_eq!(available.len(), 1);
        assert_eq!(available[0]["taskType"], "test.slack");
        assert_eq!(available[0]["hasComponent"], true);
        assert_eq!(available[0]["configFields"][0]["key"], "token");
        assert!(ov["errors"].as_array().unwrap().is_empty());
    }

    #[test]
    fn add_connector_appends_worker_and_connection() {
        let _g = env_lock();
        let name = setup(BARE_APP, true, true);
        add_connector(
            &name,
            "test.slack",
            Some("slack"),
            &cfg(&[("token", "env:SLACK_TOKEN")]),
        )
        .unwrap();

        let manifest = read_manifest(&name).unwrap();
        let worker = &manifest["workers"][0];
        assert_eq!(worker["taskType"], "test.slack");
        assert_eq!(worker["connector"], "nano-ide-connector-testpack");
        assert_eq!(worker["connection"], "slack");
        // Config persisted as env pointers on the named connection (never inline).
        assert_eq!(
            manifest["connections"]["slack"]["type"],
            "nano-ide-connector-testpack"
        );
        assert_eq!(manifest["connections"]["slack"]["token"], "env:SLACK_TOKEN");

        // Now the overview reflects the enabled, backed connector.
        let ov = connectors_overview(&name).unwrap();
        let enabled = ov["connectors"].as_array().unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0]["backed"], true);
        assert_eq!(enabled[0]["recognized"], true);
        assert!(ov["errors"].as_array().unwrap().is_empty());
    }

    #[test]
    fn add_connector_rejects_unknown_and_duplicate() {
        let _g = env_lock();
        let name = setup(BARE_APP, true, true);
        assert!(add_connector(&name, "no.such", None, &cfg(&[])).is_err());
        add_connector(&name, "test.slack", None, &cfg(&[])).unwrap();
        // A second enablement of the same taskType is rejected.
        assert!(add_connector(&name, "test.slack", None, &cfg(&[])).is_err());
    }

    #[test]
    fn validate_seam_passes_for_backed_component_connector() {
        let _g = env_lock();
        let name = setup(BARE_APP, true, true);
        add_connector(&name, "test.slack", None, &cfg(&[])).unwrap();
        validate_connector_seam(&name).expect("seam is coherent");
    }

    #[test]
    fn validate_seam_fails_when_worker_not_launchable() {
        let _g = env_lock();
        // Enable first (with a launchable pack so add_connector accepts it)...
        let name = setup(BARE_APP, true, true);
        add_connector(&name, "test.slack", None, &cfg(&[])).unwrap();
        // ...then re-point the extensions root at a declaration-only pack (no entry).
        setup_declaration_only_pack();
        let err = validate_connector_seam(&name).unwrap_err();
        assert!(err.contains("launchable"), "got: {err}");
    }

    #[test]
    fn validate_seam_fails_when_component_missing() {
        let _g = env_lock();
        // Pack ships a launchable worker but no seam component template.
        let name = setup(BARE_APP, true, false);
        // add_connector still accepts (registry match); the seam gate catches it.
        add_connector(&name, "test.slack", None, &cfg(&[])).unwrap();
        let err = validate_connector_seam(&name).unwrap_err();
        assert!(
            err.contains("element-template") || err.contains("seam"),
            "got: {err}"
        );
    }

    /// Re-point `NANOBPMN_EXTENSIONS_DIR` at a pack that declares the same worker
    /// `type` but ships no `entry` (declaration-only), so `worker_driver` returns
    /// `None` — simulating an uninstalled/partial pack after enablement.
    fn setup_declaration_only_pack() {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let ext_root =
            std::env::temp_dir().join(format!("nano-conn-decl-{}-{n}", std::process::id()));
        let pack = ext_root.join("nano-ide-connector-testpack");
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(pack.join("tpl.json"), component_template("test.slack")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{
                "id": "nano-ide-connector-testpack",
                "kind": "app",
                "components": ["tpl.json"],
                "workers": [ { "type": "test.slack", "displayName": "Test Slack" } ]
            }"#,
        )
        .unwrap();
        unsafe {
            std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext_root);
        }
    }

    /// A pack's config field must never overwrite the reserved `type`
    /// discriminator on the connection object (ADR 0025 §1) — otherwise a pack
    /// declaring a `type` config field could corrupt connection resolution.
    #[test]
    fn add_connector_config_cannot_clobber_reserved_type() {
        let _g = env_lock();
        let name = setup(BARE_APP, true, true);
        add_connector(
            &name,
            "test.slack",
            Some("slack"),
            &cfg(&[("type", "attacker-owned"), ("token", "env:SLACK_TOKEN")]),
        )
        .unwrap();

        let manifest = read_manifest(&name).unwrap();
        // `type` stays the seam-owned connector id, not the config value.
        assert_eq!(
            manifest["connections"]["slack"]["type"],
            "nano-ide-connector-testpack"
        );
        // The non-reserved config field is still persisted.
        assert_eq!(manifest["connections"]["slack"]["token"], "env:SLACK_TOKEN");
    }

    /// Materialise a project whose manifest pins its connector to pack `packb`,
    /// while the only installed pack is `packa` — which independently declares the
    /// same worker `type` (launchable) and ships its seam component. This models
    /// the drift the seam gate exists to catch: the *pinned* pack is uninstalled,
    /// but a *different* pack coincidentally backs the same type. Deterministic
    /// (single pack ⇒ no `read_dir`-order dependence).
    fn setup_pinned_pack_missing() -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!("nano-conn-pin-{}-{n}", std::process::id()));

        let proj_root = base.join("projects");
        let name = "pinapp";
        let dir = proj_root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        // Manifest pins connector = "packb", which is not installed.
        std::fs::write(
            dir.join("nano.app.json"),
            r#"{ "workers": [ { "taskType": "test.slack", "connector": "packb" } ] }"#,
        )
        .unwrap();

        // The only installed pack is `packa`: launchable worker + seam component.
        let ext_root = base.join("ext");
        let packa = ext_root.join("packa");
        std::fs::create_dir_all(&packa).unwrap();
        std::fs::write(packa.join("worker.mjs"), "export default {}\n").unwrap();
        std::fs::write(packa.join("tpl.json"), component_template("test.slack")).unwrap();
        std::fs::write(
            packa.join("nano-ide.ext.json"),
            r#"{ "id": "packa", "kind": "app", "displayName": "Pack A", "components": ["tpl.json"],
                "workers": [ { "type": "test.slack", "entry": "worker.mjs" } ] }"#,
        )
        .unwrap();

        unsafe {
            std::env::set_var("NANOBPMN_PROJECTS_DIR", &proj_root);
            std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext_root);
        }
        name.to_string()
    }

    /// The seam gate must interrogate the *pinned* `connector` pack, not whichever
    /// pack first-wins the type. `packb` is pinned but not installed; a type-scoped
    /// check would see `packa` back `test.slack` and wrongly report the connector
    /// as launchable/backed (it would instead trip on the pack-scoped *component*
    /// check — the wrong error, and `backed` would read `true`).
    #[test]
    fn validate_seam_is_pack_scoped_not_type_scoped() {
        let _g = env_lock();
        let name = setup_pinned_pack_missing();
        let err = validate_connector_seam(&name).unwrap_err();
        assert!(err.contains("launchable"), "got: {err}");

        // The overview agrees: the pinned connector is not backed.
        let ov = connectors_overview(&name).unwrap();
        let enabled = ov["connectors"].as_array().unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0]["connector"], "packb");
        assert_eq!(enabled[0]["backed"], false);
    }
}
