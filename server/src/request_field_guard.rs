//! Request-field coverage guard (#1291).
//!
//! Every re-sync of the vendored Camunda spec (`spec/`) can add request fields
//! to operations Nano already serves. The generator happily deserializes them.
//! This guard makes every such field an explicit
//! decision: it derives the set of `Schema.property` pairs reachable from the
//! request body of every served operation (the `OVERRIDES` table in
//! `scripts/gen-stub-server.py` - the single source of which operations are
//! wired to the engine) and requires it to equal the checked-in manifest
//! `spec-patches/request-fields.txt`.
//!
//! The derivation reads the spec exactly as the gateway *serves* it by reading
//! the **preprocessed** spec tree under `build/spec/` - the output of
//! `scripts/preprocess-spec.py` (sanitized, with `spec-patches/patches.yaml`
//! applied) that the `rust-axum` generator consumes to produce the served
//! `generated/` crate. That preprocessor is the *single source of truth* for
//! patch application: the guard reads its output and does **not** re-implement
//! the patch language. (An earlier version mirrored `preprocess-spec.py`'s
//! `merge`/`append`/`remove` in Rust; the two implementations were a duplicate
//! source of truth that drifted repeatedly - exactly the drift surface this
//! guard exists to prevent elsewhere.) A nano extension added only by a patch -
//! e.g. the `lastUpdateTime` job sort value, or the deprecated `leaseToken` /
//! `jobLease` aliases - is already baked into `build/spec/`, so it is triaged
//! here automatically; a bare, unpatched `spec/` would blind the guard to
//! exactly those fields.
//!
//! `build/spec/` is a git-ignored codegen artifact produced by `make generate`
//! alongside the `generated/` crate the server compiles against. Because this
//! test lives in a crate that compiles against `generated/`, whenever it can
//! build, `build/spec/` exists and is exactly as fresh as the served code. Run
//! `make generate` first if it is absent.
//!
//! Manifest lines are `Schema.property`, optionally followed by
//! `unhonoured #<issue>` for a field Nano accepts but does not act on yet.
//! A new upstream field fails the guard until it is triaged into the manifest
//! (implemented, or recorded as unhonoured with a tracking issue); a field
//! upstream removed fails it as stale. Regenerate the field list (preserving
//! annotations) with - first `make generate` so `build/spec/` reflects the
//! current `spec/` + `spec-patches/`, then:
//!
//! ```text
//! make generate
//! UPDATE_REQUEST_FIELDS=1 cargo nextest run -p nanobpm-gateway-rest-server request_field
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde_yaml::Value;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("server/ has a parent")
        .to_path_buf()
}

/// The preprocessed spec tree the generator consumes (`build/spec/`, the output
/// of `scripts/preprocess-spec.py` - sanitized, with `spec-patches/patches.yaml`
/// applied). This is a git-ignored codegen artifact produced by `make generate`
/// alongside the `generated/` crate the server compiles against, so it is
/// present (and as fresh as the served code) whenever this test can build. Fail
/// loud with an actionable message if it is missing rather than silently falling
/// back to the unpatched `spec/` (which would blind the guard to patch-added
/// request surface - a drift surface between the guarded spec and the served
/// one).
fn build_spec_dir() -> PathBuf {
    let dir = repo_root().join("build/spec");
    assert!(
        dir.join("rest-api.yaml").is_file(),
        "preprocessed spec not found at {} - run `make generate` first (it writes \
         build/spec/ via scripts/preprocess-spec.py, the single source of truth \
         for patch application)",
        dir.display()
    );
    dir
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
/// read from the preprocessed `build/spec/` tree (patches already applied by
/// `scripts/preprocess-spec.py`), so the derived field set matches the spec the
/// gateway actually serves (see module docs).
struct Spec {
    dir: PathBuf,
    files: HashMap<String, Value>,
}

impl Spec {
    fn file(&mut self, name: &str) -> &Value {
        if !self.files.contains_key(name) {
            let path = self.dir.join(name);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            let doc: Value = serde_yaml::from_str(&text)
                .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
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
        dir: build_spec_dir(),
        files: HashMap::new(),
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
