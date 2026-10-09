//! Request-field coverage guard (#1291).
//!
//! Every re-sync of the vendored Camunda spec (`spec/`) can add request fields
//! to operations Nano already serves. The generator happily deserializes them,
//! This guard makes every such field an explicit
//! decision: it derives the set of `Schema.property` pairs reachable from the
//! request body of every served operation (the `OVERRIDES` table in
//! `scripts/gen-stub-server.py` - the single source of which operations are
//! wired to the engine) and requires it to equal the checked-in manifest
//! `spec-patches/request-fields.txt`.
//!
//! The derivation reads the spec exactly as the gateway *serves* it: the raw
//! `spec/` files **with `spec-patches/patches.yaml` applied** (mirroring
//! `scripts/preprocess-spec.py`). A nano extension added only by a patch - e.g.
//! the `lastUpdateTime` job sort value, or the deprecated `leaseToken` /
//! `jobLease` aliases - is request surface the generated server accepts, so it
//! must be triaged here too; reading the unpatched spec would blind the guard to
//! exactly those fields (a drift surface between the guarded spec and the served
//! one).
//!
//! Manifest lines are `Schema.property`, optionally followed by
//! `unhonoured #<issue>` for a field Nano accepts but does not act on yet.
//! A new upstream field fails the guard until it is triaged into the manifest
//! (implemented, or recorded as unhonoured with a tracking issue); a field
//! upstream removed fails it as stale. Regenerate the field list (preserving
//! annotations) with:
//!
//! ```text
//! UPDATE_REQUEST_FIELDS=1 cargo nextest run -p nanobpm-gateway-rest-server request_field
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde_yaml::{Mapping, Value};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("server/ has a parent")
        .to_path_buf()
}

/// `camelCase` / `PascalCase` -> `snake_case`, matching the generator's
/// operation method names (`completeJob` -> `complete_job`,
/// `getProcessDefinitionXML` -> `get_process_definition_xml`).
fn snake(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_ascii_lowercase());
            if prev.is_ascii_lowercase()
                || prev.is_ascii_digit()
                || (prev.is_ascii_uppercase() && next_lower)
            {
                out.push('_');
            }
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// Served operation method names, parsed from the `OVERRIDES` keys
/// `("module", "method"):` in `scripts/gen-stub-server.py`.
fn served_operations() -> BTreeSet<String> {
    let src = std::fs::read_to_string(repo_root().join("scripts/gen-stub-server.py"))
        .expect("read gen-stub-server.py");
    let mut ops = BTreeSet::new();
    for line in src.lines() {
        let t = line.trim_start();
        if !t.starts_with("(\"") {
            continue;
        }
        let parts: Vec<&str> = t.split('"').collect();
        // ( "module" , "method" ): ...
        if parts.len() > 4 && parts[4].starts_with("):") {
            ops.insert(parts[3].to_string());
        }
    }
    assert!(ops.len() > 50, "parsed too few OVERRIDES ({})", ops.len());
    ops
}

/// Lazily-loaded multi-file spec with cross-file `$ref` resolution. Files are
/// loaded with `spec-patches/patches.yaml` applied, so the derived field set
/// matches the spec the gateway actually serves (see module docs).
struct Spec {
    dir: PathBuf,
    files: HashMap<String, Value>,
    /// `patches.yaml` actions grouped by their target `file` (relative to `dir`).
    patches: HashMap<String, Vec<Value>>,
}

/// Loads `spec-patches/patches.yaml`, grouping each action by its `file` so a
/// file's overlays can be applied the moment it is first read. Missing file =>
/// no patches (the guard still works against the raw spec).
fn load_patches(root: &Path) -> HashMap<String, Vec<Value>> {
    let path = root.join("spec-patches/patches.yaml");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return HashMap::new();
    };
    let Value::Sequence(entries) =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
    else {
        panic!("{}: expected a top-level list of patches", path.display());
    };
    let mut by_file: HashMap<String, Vec<Value>> = HashMap::new();
    for entry in entries {
        let file = entry
            .get("file")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("patch missing a string 'file': {entry:?}"))
            .to_string();
        by_file.entry(file).or_default().push(entry);
    }
    by_file
}

/// Deep-merges `addition` into `target` additively (mirrors
/// `preprocess-spec.py::_deep_merge`): where both hold a mapping the merge
/// recurses, otherwise the addition's value overwrites.
fn deep_merge(target: &mut Value, addition: &Value) {
    let (Some(t), Some(a)) = (target.as_mapping_mut(), addition.as_mapping()) else {
        return;
    };
    for (key, value) in a {
        match t.get_mut(key) {
            Some(existing) if existing.is_mapping() && value.is_mapping() => {
                deep_merge(existing, value)
            }
            _ => {
                t.insert(key.clone(), value.clone());
            }
        }
    }
}

/// Walks the dotted `target` (e.g. `components.schemas.Foo.required`), creating
/// intermediate mappings on demand, and returns `(parent_container, last_key)` -
/// mirrors `preprocess-spec.py::_resolve_parent` (naive split on `.`).
fn resolve_parent<'a>(doc: &'a mut Value, dotted: &str) -> (&'a mut Value, String) {
    let parts: Vec<&str> = dotted.split('.').collect();
    let (last, parents) = parts.split_last().expect("patch target is non-empty");
    let mut node = doc;
    for part in parents {
        let map = node
            .as_mapping_mut()
            .unwrap_or_else(|| panic!("cannot descend into non-mapping at '{part}' in '{dotted}'"));
        if !map.contains_key(*part) || map.get(*part).is_some_and(Value::is_null) {
            map.insert(
                Value::String((*part).to_string()),
                Value::Mapping(Mapping::new()),
            );
        }
        node = map.get_mut(*part).expect("intermediate node just ensured");
    }
    (node, (*last).to_string())
}

/// Applies a single `patches.yaml` action to a parsed file document, mirroring
/// `preprocess-spec.py::_apply_patch` (`merge` / `append` / `remove`).
fn apply_patch(doc: &mut Value, patch: &Value) {
    let target = patch
        .get("target")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("patch missing a string 'target': {patch:?}"));
    let (parent, last) = resolve_parent(doc, target);
    let parent = parent
        .as_mapping_mut()
        .unwrap_or_else(|| panic!("target parent of '{target}' is not a mapping"));

    if let Some(addition) = patch.get("merge") {
        // Mirror `preprocess-spec.py::_apply_patch` exactly: a non-mapping
        // `merge` payload is rejected, and an existing non-mapping target is
        // rejected (only an absent/null target is created). Silently returning
        // from `deep_merge` in either case would let this guard derive the
        // unpatched request surface and pass even though the real generator
        // fails — a drift surface between the guarded spec and the served one.
        if !addition.is_mapping() {
            panic!("'merge' for target '{target}' must be a mapping");
        }
        if !parent.contains_key(last.as_str())
            || parent.get(last.as_str()).is_some_and(Value::is_null)
        {
            parent.insert(Value::String(last.clone()), Value::Mapping(Mapping::new()));
        }
        let node = parent.get_mut(last.as_str()).expect("merge node ensured");
        if !node.is_mapping() {
            panic!("cannot merge into non-mapping at target '{target}'");
        }
        deep_merge(node, addition);
    } else if let Some(items) = patch.get("append").and_then(Value::as_sequence) {
        if !parent.contains_key(last.as_str()) {
            parent.insert(Value::String(last.clone()), Value::Sequence(Vec::new()));
        }
        let node = parent
            .get_mut(last.as_str())
            .and_then(Value::as_sequence_mut)
            .unwrap_or_else(|| panic!("cannot append to non-list at target '{target}'"));
        for item in items {
            if !node.contains(item) {
                node.push(item.clone());
            }
        }
    } else if let Some(items) = patch.get("remove").and_then(Value::as_sequence) {
        let node = parent
            .get_mut(last.as_str())
            .and_then(Value::as_sequence_mut)
            .unwrap_or_else(|| panic!("cannot remove from non-list at target '{target}'"));
        for item in items {
            // Fail loud on a stale removal, mirroring `preprocess-spec.py`: after
            // an upstream re-sync a `remove` that no longer matches is a stale
            // patch that must be revisited, not silently ignored — otherwise this
            // guard would pass a patch the real generator rejects.
            let pos = node.iter().position(|e| e == item).unwrap_or_else(|| {
                panic!("stale patch: {item:?} not present at target '{target}'")
            });
            node.remove(pos);
        }
    } else {
        panic!("patch for target '{target}' has none of 'merge', 'append', 'remove'");
    }
}

impl Spec {
    fn file(&mut self, name: &str) -> &Value {
        if !self.files.contains_key(name) {
            let text = std::fs::read_to_string(self.dir.join(name))
                .unwrap_or_else(|e| panic!("read spec/{name}: {e}"));
            let mut doc: Value =
                serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("parse spec/{name}: {e}"));
            if let Some(patches) = self.patches.get(name) {
                for patch in patches.clone() {
                    apply_patch(&mut doc, &patch);
                }
            }
            self.files.insert(name.to_string(), doc);
        }
        self.files.get(name).expect("file just inserted")
    }

    /// Resolves `file.yaml#/a/b` (or `#/a/b` relative to `cur`) to
    /// `(value, file, last path segment)`.
    fn resolve(&mut self, reference: &str, cur: &str) -> (Value, String, String) {
        let (file, frag) = reference.split_once('#').expect("$ref has a fragment");
        let file = if file.is_empty() {
            cur.to_string()
        } else {
            file.to_string()
        };
        let mut node = self.file(&file).clone();
        let mut last = String::new();
        for seg in frag.trim_start_matches('/').split('/') {
            let seg = seg.replace("~1", "/").replace("~0", "~");
            node = node
                .get(seg.as_str())
                .unwrap_or_else(|| panic!("dangling $ref {reference} from {cur}"))
                .clone();
            last = seg;
        }
        (node, file, last)
    }
}

/// Records every `Schema.property` reachable from `node`. Named (`$ref`)
/// schemas are visited once, so a shared schema is listed under its own name;
/// inline nested objects are named by their dotted parent path.
fn walk(
    spec: &mut Spec,
    node: &Value,
    cur: &str,
    name: &str,
    fields: &mut BTreeSet<String>,
    visited: &mut BTreeSet<(String, String)>,
) {
    if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        let (target, file, schema) = spec.resolve(reference, cur);
        if visited.insert((file.clone(), schema.clone())) {
            walk(spec, &target, &file, &schema, fields, visited);
        }
        return;
    }
    if !node.is_mapping() {
        return;
    }
    for key in ["allOf", "oneOf", "anyOf"] {
        if let Some(list) = node.get(key).and_then(Value::as_sequence) {
            for sub in list.clone() {
                walk(spec, &sub, cur, name, fields, visited);
            }
        }
    }
    for key in ["items", "additionalProperties"] {
        if let Some(sub) = node.get(key).filter(|v| v.is_mapping()).cloned() {
            walk(spec, &sub, cur, name, fields, visited);
        }
    }
    if let Some(props) = node.get("properties").and_then(Value::as_mapping) {
        for (prop, sub) in props.clone() {
            let prop = prop
                .as_str()
                .expect("property names are strings")
                .to_string();
            let path = format!("{name}.{prop}");
            fields.insert(path.clone());
            // A sort request's `field` values are request surface too: each is
            // listed as `Schema.field=value`, so an upstream-added sort value
            // cannot silently fall through a dispatcher's wildcard arm.
            if name.ends_with("SortRequest") && prop == "field" {
                let target = match sub.get("$ref").and_then(Value::as_str) {
                    Some(r) => spec.resolve(r, cur).0,
                    None => sub.clone(),
                };
                for value in target
                    .get("enum")
                    .and_then(Value::as_sequence)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                {
                    fields.insert(format!("{path}={value}"));
                }
            }
            walk(spec, &sub, cur, &path, fields, visited);
        }
    }
}

/// `(served operations missing from the spec, request fields of the served
/// operations present in it)`.
fn served_request_fields() -> (BTreeSet<String>, BTreeSet<String>) {
    let served = served_operations();
    let mut spec = Spec {
        dir: repo_root().join("spec"),
        files: HashMap::new(),
        patches: load_patches(&repo_root()),
    };
    let paths = spec
        .file("rest-api.yaml")
        .get("paths")
        .and_then(Value::as_mapping)
        .expect("rest-api.yaml has paths")
        .clone();
    let mut found = BTreeSet::new();
    let mut fields = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for (_, item) in paths {
        let (item, file) = match item.get("$ref").and_then(Value::as_str) {
            Some(r) => {
                let (v, f, _) = spec.resolve(r, "rest-api.yaml");
                (v, f)
            }
            None => (item.clone(), "rest-api.yaml".to_string()),
        };
        let Some(ops) = item.as_mapping() else {
            continue;
        };
        for (_, op) in ops.clone() {
            let Some(op_id) = op.get("operationId").and_then(Value::as_str) else {
                continue;
            };
            if !served.contains(&snake(op_id)) {
                continue;
            }
            found.insert(snake(op_id));
            let mut body = op.get("requestBody").cloned();
            let mut body_file = file.clone();
            if let Some(r) = body
                .as_ref()
                .and_then(|b| b.get("$ref"))
                .and_then(Value::as_str)
            {
                let (v, f, _) = spec.resolve(r, &file);
                body = Some(v);
                body_file = f;
            }
            let Some(content) = body
                .as_ref()
                .and_then(|b| b.get("content"))
                .and_then(Value::as_mapping)
            else {
                continue;
            };
            for (_, media) in content.clone() {
                if let Some(schema) = media.get("schema") {
                    let name = format!("{op_id}Body");
                    walk(
                        &mut spec,
                        schema,
                        &body_file,
                        &name,
                        &mut fields,
                        &mut visited,
                    );
                }
            }
        }
    }
    let missing = served.difference(&found).cloned().collect();
    (missing, fields)
}

const MANIFEST: &str = "spec-patches/request-fields.txt";

const MANIFEST_HEADER: &str = "\
# Request fields of every served operation (derived from spec/ + the OVERRIDES
# table in scripts/gen-stub-server.py). Guarded by
# server/src/request_field_guard.rs - see its module docs. One `Schema.property`
# per line; append `unhonoured #<issue>` to a field Nano accepts but ignores.
";

/// Manifest entries: field -> optional annotation (`unhonoured #NNNN`).
fn read_manifest() -> BTreeMap<String, Option<String>> {
    let text = std::fs::read_to_string(repo_root().join(MANIFEST)).unwrap_or_default();
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| match l.split_once(char::is_whitespace) {
            Some((field, note)) => (field.to_string(), Some(note.trim().to_string())),
            None => (l.to_string(), None),
        })
        .collect()
}

#[test]
fn request_fields_of_served_operations_are_all_triaged() {
    let (missing, fields) = served_request_fields();
    let mut manifest = read_manifest();

    if std::env::var_os("UPDATE_REQUEST_FIELDS").is_some() {
        manifest.retain(|f, _| fields.contains(f));
        let mut out = MANIFEST_HEADER.to_string();
        for field in &fields {
            out.push_str(field);
            if let Some(Some(note)) = manifest.get(field) {
                out.push(' ');
                out.push_str(note);
            }
            out.push('\n');
        }
        std::fs::write(repo_root().join(MANIFEST), out).expect("write manifest");
        return;
    }

    // Every served operation must resolve to a spec operation, so a typo'd or
    // renamed OVERRIDES key cannot silently hide an operation from the guard.
    assert_eq!(
        missing,
        BTreeSet::new(),
        "served operations not found in spec/ - fix OVERRIDES or the guard"
    );

    let listed: BTreeSet<String> = manifest.keys().cloned().collect();
    let untriaged: Vec<_> = fields.difference(&listed).collect();
    let stale: Vec<_> = listed.difference(&fields).collect();
    assert!(
        untriaged.is_empty() && stale.is_empty(),
        "{MANIFEST} is out of date with spec/.\n\
         New request fields (implement them, or record `unhonoured #<issue>`): {untriaged:#?}\n\
         Fields no longer in the spec: {stale:#?}\n\
         Regenerate with UPDATE_REQUEST_FIELDS=1, then triage every new line."
    );
    for (field, note) in &manifest {
        if let Some(note) = note {
            let ok = note
                .strip_prefix("unhonoured #")
                .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()));
            assert!(
                ok,
                "{field}: annotation must be `unhonoured #<issue>`, got `{note}`"
            );
        }
    }
}

/// Defect-class guard (#1346 review): the guard's `remove` must mirror
/// `preprocess-spec.py::_apply_patch` and FAIL LOUD on a stale removal (an item
/// that is no longer present at the target). A silent skip would let a stale
/// patch pass this guard even though the real generator rejects it — a drift
/// surface between the guarded spec and the served one.
#[test]
fn apply_patch_remove_fails_loud_on_a_stale_removal() {
    let mut doc = serde_yaml::from_str::<Value>("required:\n  - keep\n").unwrap();
    let patch =
        serde_yaml::from_str::<Value>("target: required\nremove:\n  - already-gone-upstream\n")
            .unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        apply_patch(&mut doc, &patch);
    }));
    let payload = outcome.expect_err("a stale removal must panic, not be silently ignored");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("stale patch") && message.contains("already-gone-upstream"),
        "the panic must name the stale item and target, got: {message}"
    );
}

/// The happy-path counterpart: a `remove` whose items ARE present applies
/// cleanly (and stays a no-panic), so tightening stale-removal handling does not
/// break the two live `remove` patches in `spec-patches/patches.yaml`.
#[test]
fn apply_patch_remove_removes_present_items() {
    let mut doc = serde_yaml::from_str::<Value>("required:\n  - keep\n  - drop\n").unwrap();
    let patch = serde_yaml::from_str::<Value>("target: required\nremove:\n  - drop\n").unwrap();
    apply_patch(&mut doc, &patch);
    let remaining: Vec<&str> = doc
        .get("required")
        .and_then(Value::as_sequence)
        .unwrap()
        .iter()
        .map(Value::as_str)
        .collect::<Option<Vec<_>>>()
        .unwrap();
    assert_eq!(remaining, ["keep"], "only the requested item is removed");
}

/// Defect-class guard (#1346 review): the guard's `merge` must mirror
/// `preprocess-spec.py::_apply_patch` and FAIL LOUD on a non-mapping `merge`
/// payload. Silently returning (the pre-fix `deep_merge` behaviour) would let a
/// malformed patch pass this guard even though the real generator rejects it —
/// the same drift surface as a stale `remove`.
#[test]
fn apply_patch_merge_fails_loud_on_a_non_mapping_payload() {
    let mut doc = serde_yaml::from_str::<Value>("properties: {}\n").unwrap();
    let patch =
        serde_yaml::from_str::<Value>("target: properties\nmerge:\n  - not-a-mapping\n").unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        apply_patch(&mut doc, &patch);
    }));
    let payload = outcome.expect_err("a non-mapping merge payload must panic, not be skipped");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("must be a mapping") && message.contains("properties"),
        "the panic must name the target, got: {message}"
    );
}

/// The sibling failure mode: an EXISTING non-mapping target (a scalar/sequence
/// already at the target key) must also fail loud, not be silently skipped by
/// `deep_merge`. Only an absent/null target is created fresh (mirroring
/// `preprocess-spec.py`).
#[test]
fn apply_patch_merge_fails_loud_on_a_non_mapping_target() {
    let mut doc = serde_yaml::from_str::<Value>("properties: not-a-mapping\n").unwrap();
    let patch =
        serde_yaml::from_str::<Value>("target: properties\nmerge:\n  new-field: {}\n").unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        apply_patch(&mut doc, &patch);
    }));
    let payload = outcome.expect_err("merging into a non-mapping target must panic");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("cannot merge into non-mapping") && message.contains("properties"),
        "the panic must name the target, got: {message}"
    );
}
