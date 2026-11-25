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

use std::collections::{BTreeMap, BTreeSet};
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
    files: BTreeMap<String, Value>,
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
    let (missing, by_op) = served_request_fields_by_operation();
    (missing, by_op.into_values().flatten().collect())
}

/// `(served operations missing from the spec, served operation method name ->
/// the request fields reachable from its body)`. Each operation is walked with
/// its own `visited` set, so a schema shared by several operations is attributed
/// to every one of them.
fn served_request_fields_by_operation() -> (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>) {
    let served = served_operations();
    let mut spec = Spec {
        dir: build_spec_dir(),
        files: BTreeMap::new(),
    };
    let paths = spec
        .file("rest-api.yaml")
        .get("paths")
        .and_then(Value::as_mapping)
        .expect("rest-api.yaml has paths")
        .clone();
    let mut found = BTreeSet::new();
    let mut by_op: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
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
            let fields = by_op.entry(snake(op_id)).or_default();
            let mut visited = BTreeSet::new();
            for (_, media) in content.clone() {
                if let Some(schema) = media.get("schema") {
                    let name = format!("{op_id}Body");
                    walk(&mut spec, schema, &body_file, &name, fields, &mut visited);
                }
            }
        }
    }
    let missing = served.difference(&found).cloned().collect();
    (missing, by_op)
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

/// The source text of every sort dispatcher for `schema` in `main.rs`: from
/// each `models::<schema>| (r.field…` extractor up to the following
/// `query::paginate(` call, plus the body of any named accessor fn the window
/// delegates to (e.g. `job_sort_val`).
fn sort_dispatcher_sources(main_rs: &str, schema: &str) -> Vec<String> {
    let needle = format!("models::{schema}|");
    main_rs
        .match_indices(&needle)
        .map(|(at, _)| {
            let rest = &main_rs[at..];
            let end = rest.find("query::paginate(").unwrap_or(rest.len());
            let mut window = rest[..end].to_string();
            // Follow named `*_sort_val(` accessors the closure delegates to.
            let accessors: BTreeSet<String> = window
                .match_indices("_sort_val(")
                .map(|(i, _)| {
                    let head = &window[..i];
                    let start = head
                        .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .map_or(0, |p| p + 1);
                    format!("{}_sort_val", &head[start..])
                })
                .collect();
            for accessor in accessors {
                if let Some(start) = main_rs.find(&format!("fn {accessor}(")) {
                    let body = &main_rs[start..];
                    let stop = body.find("\n}\n").unwrap_or(body.len());
                    window.push_str(&body[..stop]);
                }
            }
            window
        })
        .collect()
}

/// Defect-class guard (#1298): a sort `field` value that the manifest marks as
/// honoured (no `unhonoured #N` note) must have an explicit arm in its
/// endpoint's sort dispatcher. The dispatchers are stringly
/// `match field { … _ => key }` closures, so an unhandled value silently falls
/// through to entity-key order with a 200 — exactly the bug #1298 fixed for 60+
/// values. This makes a forgotten arm (or a dropped `unhonoured` marker without
/// an implementation) fail CI instead.
#[test]
fn honoured_sort_field_values_have_an_explicit_dispatcher_arm() {
    // Sort dispatchers live in `main.rs` and, for operations shared with the
    // WASM read surface (#1883, #1898), in the `nano-search` crate - scan both,
    // every module of the shared crate (listed from disk, so a newly migrated
    // operation's module is scanned without editing this guard).
    let search_dir = repo_root().join("server/crates/nano-search/src");
    let mut sources: Vec<std::path::PathBuf> = std::fs::read_dir(&search_dir)
        .expect("read nano-search sources")
        .map(|e| e.expect("nano-search entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    sources.sort();
    sources.insert(0, repo_root().join("server/src/main.rs"));
    let main_rs = sources
        .iter()
        .map(|p| std::fs::read_to_string(p).expect("read sort dispatcher source"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut unhandled = Vec::new();
    for (entry, note) in read_manifest() {
        if note.is_some() {
            continue;
        }
        let Some((schema, value)) = entry
            .split_once(".field=")
            .filter(|(s, _)| s.ends_with("SortRequest"))
        else {
            continue;
        };
        let sources = sort_dispatcher_sources(&main_rs, schema);
        let quoted = format!("\"{value}\"");
        if sources.is_empty() || sources.iter().any(|src| !src.contains(&quoted)) {
            unhandled.push(entry);
        }
    }
    assert!(
        unhandled.is_empty(),
        "sort values with no explicit dispatcher arm (they would silently sort \
         by entity key) - implement them, or mark `unhonoured #<issue>` in \
         spec-patches/request-fields.txt: {unhandled:#?}"
    );
}

/// `OVERRIDES` method name -> (the `self.<impl>(` handler it delegates to,
/// the delegation's argument list), parsed from `scripts/gen-stub-server.py`
/// (the single source of which operations are wired to which handler).
fn override_handlers() -> BTreeMap<String, (String, Vec<String>)> {
    let src = std::fs::read_to_string(repo_root().join("scripts/gen-stub-server.py"))
        .expect("read gen-stub-server.py");
    let mut out = BTreeMap::new();
    for line in src.lines() {
        let parts: Vec<&str> = line.trim_start().split('"').collect();
        if parts.len() > 6
            && parts[0] == "("
            && parts[4].starts_with("):")
            && let Some(call) = parts[5].strip_prefix("self.")
        {
            let (name, rest) = call.split_once('(').unwrap_or((call, ""));
            let args = rest
                .rsplit_once(')')
                .map_or("", |(a, _)| a)
                .split(',')
                .map(|a| a.trim().to_string())
                .filter(|a| !a.is_empty())
                .collect();
            out.insert(parts[3].to_string(), (name.to_string(), args));
        }
    }
    out
}

/// Whether an `OVERRIDES` delegation hands the request body to its handler:
/// one argument must be exactly `body` (no `None`/`&None` substitute, which
/// would produce the same handler mapping while dropping every REST field).
fn delegation_forwards_body(args: &[String]) -> bool {
    args.iter().any(|a| a == "body")
}

#[test]
fn delegation_forwards_body_rejects_dropped_bodies() {
    let args = |s: &[&str]| s.iter().map(|a| a.to_string()).collect::<Vec<_>>();
    assert!(delegation_forwards_body(&args(&["path_params", "body"])));
    assert!(!delegation_forwards_body(&args(&["path_params", "&None"])));
    assert!(!delegation_forwards_body(&args(&["path_params", "None"])));
    assert!(!delegation_forwards_body(&args(&["path_params"])));
}

/// The source of `fn <name>(` (through its closing `}` at the definition's
/// indentation) in any `server/src/*.rs` file.
fn fn_source(name: &str) -> Option<String> {
    let dir = repo_root().join("server/src");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("read server/src")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.sort();
    let needle = format!("fn {name}(");
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read source");
        let Some(at) = text.find(&needle) else {
            continue;
        };
        let line_start = text[..at].rfind('\n').map_or(0, |p| p + 1);
        let indent: String = text[line_start..]
            .chars()
            .take_while(|c| *c == ' ')
            .collect();
        let rest = &text[at..];
        let close = format!("\n{indent}}}\n");
        let end = rest.find(&close).map_or(rest.len(), |e| e + close.len());
        return Some(rest[..end].to_string());
    }
    None
}

/// Whether a handler body (Rust source) really reads the request's
/// `operationReference` and hands it on: comments are stripped, then a
/// `.operation_reference` field read must be followed by a later bare use of
/// the bound value *as a forwarding position*: a non-macro call argument
/// (`f(x, operation_reference)`) or a struct-literal field
/// (`Cmd { operation_reference }` / `operation_reference: operation_reference`).
/// A mention only in a comment, a binding that is never used, or a use that
/// merely logs (`tracing::info!(.., operation_reference)`), discards
/// (`drop(operation_reference)`, `let _ = operation_reference`) or otherwise
/// consumes it without passing it on does not count.
fn forwards_operation_reference(src: &str) -> bool {
    let code = strip_rust_comments(src);
    const FIELD: &str = "operation_reference";
    let Some(read) = code.find(&format!(".{FIELD}")) else {
        return false;
    };
    let after = read + 1 + FIELD.len();
    let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    code[after..].match_indices(FIELD).any(|(i, _)| {
        let at = after + i;
        let before = code[..at].trim_end();
        let tail = code[at + FIELD.len()..].trim_start();
        let prev = before.chars().next_back();
        let next = tail.chars().next();
        let bare = code[..at]
            .chars()
            .next_back()
            .is_some_and(|c| !is_ident(c) && c != '.')
            && !code[at + FIELD.len()..]
                .chars()
                .next()
                .is_some_and(is_ident);
        // Positioned as a list element / field value, ending the element.
        let positioned = matches!(prev, Some('(' | ',' | '{' | ':'))
            && !before.ends_with("::")
            && matches!(next, Some(')' | ',' | '}'));
        bare && positioned && enclosing_is_forwarding(&code[..at])
    })
}

/// `src` with every Rust comment and every literal's contents blanked out:
/// `//` line comments, (nested) `/* */` block comments, and the interiors of
/// string, raw-string and char literals (delimiters kept). Literals are lexed
/// so a `//` or `/*` inside one does not start a comment, and their contents
/// are masked so text in a string (e.g. a log message spelling out
/// `cancel_core(k, operation_reference)`) can never pass for code. Removed
/// text is replaced by spaces (newlines kept) so code on either side never
/// fuses.
fn strip_rust_comments(src: &str) -> String {
    let blank = |ch: char| if ch == '\n' { '\n' } else { ' ' };
    let b: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let n = b.get(i + 1).copied();
        if c == '/' && n == Some('/') {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && n == Some('*') {
            let mut depth = 0usize;
            while i < b.len() {
                if b[i] == '/' && b.get(i + 1) == Some(&'*') {
                    depth += 1;
                    out.push_str("  ");
                    i += 2;
                } else if b[i] == '*' && b.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    out.push_str("  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    out.push(if b[i] == '\n' { '\n' } else { ' ' });
                    i += 1;
                }
            }
        } else if c == 'r'
            && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == '_'))
            && raw_string_hashes(&b[i + 1..]).is_some()
        {
            // Raw string `r#"..."#`: no escapes; ends at `"` + same hashes.
            let hashes = raw_string_hashes(&b[i + 1..]).unwrap_or(0);
            let open = 2 + hashes;
            out.extend(&b[i..i + open]);
            i += open;
            while i < b.len() {
                if b[i] == '"' && (1..=hashes).all(|k| b.get(i + k) == Some(&'#')) {
                    out.extend(&b[i..i + 1 + hashes]);
                    i += 1 + hashes;
                    break;
                }
                out.push(blank(b[i]));
                i += 1;
            }
        } else if c == '\'' {
            // Char literal ('x', '\'', '\u{..}') vs. lifetime ('a): a
            // literal closes with `'` within a short escape-aware span.
            let end = if n == Some('\\') {
                (i + 3..b.len().min(i + 12)).find(|&j| b[j] == '\'')
            } else if b.get(i + 2) == Some(&'\'') {
                Some(i + 2)
            } else {
                None
            };
            match end {
                Some(e) => {
                    out.push('\'');
                    out.extend(b[i + 1..e].iter().map(|&ch| blank(ch)));
                    out.push('\'');
                    i = e + 1;
                }
                None => {
                    out.push(c);
                    i += 1;
                }
            }
        } else if c == '"' {
            out.push(c);
            i += 1;
            while i < b.len() {
                if b[i] == '\\' {
                    out.push(' ');
                    if let Some(&e) = b.get(i + 1) {
                        out.push(blank(e));
                    }
                    i += 2;
                    continue;
                }
                if b[i] == '"' {
                    out.push('"');
                    i += 1;
                    break;
                }
                out.push(blank(b[i]));
                i += 1;
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

/// If `rest` (the text after an `r`) opens a raw string (`#`* then `"`),
/// the number of `#`s.
fn raw_string_hashes(rest: &[char]) -> Option<usize> {
    let h = rest.iter().take_while(|c| **c == '#').count();
    (rest.get(h) == Some(&'"')).then_some(h)
}

#[test]
fn strip_rust_comments_handles_char_literals_and_raw_strings() {
    // A `'"'` char literal must not open a phantom string that hides a
    // later comment (fail open): the commented-out forward is stripped.
    let s = strip_rust_comments("let q = '\"'; // forward(x)\nb");
    assert!(!s.contains("forward"), "{s}");
    let s = strip_rust_comments("let q = '\\''; // forward(x)\nb");
    assert!(!s.contains("forward"), "{s}");
    let s = strip_rust_comments("let q = r#\"a\"b\"#; /* forward(x) */ c");
    assert!(!s.contains("forward"), "{s}");
    assert!(s.contains("r#\"   \"#"), "{s}");
    // Literal contents are masked, delimiters kept.
    assert_eq!(strip_rust_comments("r\"//keep\" x"), "r\"      \" x");
    let s = strip_rust_comments("fn f<'a>(x: &'a str) {} // forward(x)");
    assert!(!s.contains("forward"), "{s}");
}

#[test]
fn strip_rust_comments_removes_line_and_nested_block_comments() {
    assert_eq!(strip_rust_comments("a // x\nb"), "a \nb");
    let s = strip_rust_comments("a /* x /* y */ z */ b");
    assert_eq!(s.split_whitespace().collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(strip_rust_comments("\"//no\" c"), "\"    \" c");
    assert_eq!(strip_rust_comments("\"/*no*/\\\"\" c"), "\"        \" c");
}

/// Whether the innermost open `(`/`{` before the end of `head` is a
/// forwarding context: a non-macro call (not `drop(`, not `name!(`), or a
/// struct literal (`Name {`) rather than a block or a macro body.
fn enclosing_is_forwarding(head: &str) -> bool {
    let mut depth = 0i32;
    for (i, c) in head.char_indices().rev() {
        match c {
            ')' | '}' | ']' => depth += 1,
            '(' | '{' | '[' if depth > 0 => depth -= 1,
            '(' | '{' => {
                let pre = head[..i].trim_end();
                if pre.ends_with('!') {
                    return false;
                }
                let ident: String = pre
                    .chars()
                    .rev()
                    .take_while(|c| c.is_ascii_alphanumeric() || c == &'_')
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                if c == '(' {
                    return !ident.is_empty() && ident != "drop";
                }
                // `{`: a struct literal names a type (UpperCamel) directly.
                return ident.chars().next().is_some_and(|c| c.is_ascii_uppercase());
            }
            '[' => return false,
            _ => {}
        }
    }
    false
}

#[test]
fn forwards_operation_reference_rejects_dropping_handlers() {
    let good = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    self.cancel_core(k, operation_reference)\n}";
    assert!(forwards_operation_reference(good));
    let good_struct = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    send(Req { key, operation_reference })\n}";
    assert!(forwards_operation_reference(good_struct));
    let good_await = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    self.cancel_core(\n        k,\n        operation_reference,\n    )\n    .await\n}";
    assert!(forwards_operation_reference(good_await));
    let comment_only = "fn h(body: &Option<B>) {\n    // reads body.operation_reference (not really)\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(comment_only));
    let bound_dropped = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(bound_dropped));
    let never_read =
        "fn h(body: &Option<B>) {\n    let x = body.is_some();\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(never_read));
    let logged_only = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    tracing::info!(?k, operation_reference);\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(logged_only));
    let println_only = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    println!(\"{:?}\", operation_reference);\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(println_only));
    let dropped = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    drop(operation_reference);\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(dropped));
    let discarded = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    let _ = operation_reference;\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(discarded));
    let in_block = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    if x { operation_reference }\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(in_block));
    let block_comment = "fn h(body: &Option<B>) {\n    /* let operation_reference = body.as_ref().and_then(|b| b.operation_reference); self.cancel_core(k, operation_reference); */\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(block_comment));
    let nested_comment = "fn h(body: &Option<B>) {\n    /* outer /* inner */ let operation_reference = body.as_ref().and_then(|b| b.operation_reference); self.cancel_core(k, operation_reference); */\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(nested_comment));
    // Text inside string literals must never pass for code (#1606 review).
    let in_string = "fn h(body: &Option<B>) {\n    tracing::info!(\"body.operation_reference -> cancel_core(k, operation_reference)\");\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(in_string));
    let in_raw_string = "fn h(body: &Option<B>) {\n    let m = r#\"body.operation_reference; f(k, operation_reference)\"#;\n    self.cancel_core(k, None)\n}";
    assert!(!forwards_operation_reference(in_raw_string));
    let read_live_use_commented = "fn h(body: &Option<B>) {\n    let operation_reference = body.as_ref().and_then(|b| b.operation_reference);\n    self.cancel_core(k, /* operation_reference */ None)\n}";
    assert!(!forwards_operation_reference(read_live_use_commented));
}

/// Defect-class guard (#1606): an `operationReference` request field the
/// manifest marks as honoured (no `unhonoured #N` note) must actually be read
/// by the served operation's handler and handed on toward the `_ref` engine
/// funnel. Before #1606, `CancelProcessInstanceRequest.operationReference` and
/// its suspend/resume/set-variables siblings were listed as honoured while the
/// `OVERRIDES` delegation passed only `path_params`, silently dropping the
/// value. The guard derives, from the spec, every served operation whose body
/// reaches an honoured `*.operationReference`, and requires that operation's
/// handler (resolved through `OVERRIDES`) to read `.operation_reference` and
/// pass the bound value onward (see `forwards_operation_reference`).
#[test]
fn honoured_operation_references_are_forwarded_by_their_handler() {
    let manifest = read_manifest();
    let honoured: BTreeSet<&String> = manifest
        .iter()
        .filter(|(f, note)| note.is_none() && f.ends_with(".operationReference"))
        .map(|(f, _)| f)
        .collect();
    assert!(
        !honoured.is_empty(),
        "no honoured operationReference fields parsed - guard is blind"
    );
    let (_, by_op) = served_request_fields_by_operation();
    let handlers = override_handlers();
    let mut dropped = Vec::new();
    let mut checked = 0;
    for (op, fields) in &by_op {
        let reaches: Vec<_> = fields.iter().filter(|f| honoured.contains(f)).collect();
        if reaches.is_empty() {
            continue;
        }
        checked += 1;
        let Some((handler, args)) = handlers.get(op) else {
            dropped.push(format!("{op}: no OVERRIDES handler ({reaches:?})"));
            continue;
        };
        if !delegation_forwards_body(args) {
            dropped.push(format!(
                "{op} -> {handler}({}): OVERRIDES delegation drops the request body",
                args.join(", ")
            ));
            continue;
        }
        let forwards = fn_source(handler).is_some_and(|src| forwards_operation_reference(&src));
        if !forwards {
            dropped.push(format!("{op} -> {handler}: drops {reaches:?}"));
        }
    }
    assert!(
        checked > 0,
        "no served operation reaches an honoured operationReference"
    );
    assert!(
        dropped.is_empty(),
        "honoured operationReference fields the handler never reads - thread the \
         reference through to the `_ref` funnel, or mark the field \
         `unhonoured #<issue>` in spec-patches/request-fields.txt: {dropped:#?}"
    );
}

/// Response-side `operationReference` fields Nano projects into its read model
/// (#1601, step 5 of #1556). Each entry is a `Schema.property` path as derived by
/// [`walk`] from a 2xx response body of a spec operation.
///
/// Camunda's read model may surface the write-side `operationReference` on read
/// results (audit log entries, batch operation items). As of the synced spec no
/// response schema carries it - neither `AuditLogResult` nor the batch operation
/// item result - so there is nothing to project and this list is empty. The
/// guard below derives the set from the spec on every run: when a re-sync adds
/// `operationReference` to any response, it fails until the field is projected
/// (and listed here, with its read-model column wired) - instead of the spec
/// silently advertising a read field Nano always omits.
const PROJECTED_RESPONSE_OPERATION_REFERENCES: &[&str] = &[];

/// Every `Schema.property` reachable from a 2xx response body of **any** spec
/// operation (served or not: an unserved read surface that gains the field is
/// still a read-side exposure Nano must decide on before serving it).
fn response_fields() -> BTreeSet<String> {
    let mut spec = Spec {
        dir: build_spec_dir(),
        files: BTreeMap::new(),
    };
    let paths = spec
        .file("rest-api.yaml")
        .get("paths")
        .and_then(Value::as_mapping)
        .expect("rest-api.yaml has paths")
        .clone();
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
            let Some(responses) = op.get("responses").and_then(Value::as_mapping) else {
                continue;
            };
            for (status, resp) in responses.clone() {
                let status = match &status {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    _ => continue,
                };
                if !status.starts_with('2') {
                    continue;
                }
                let (resp, resp_file) = match resp.get("$ref").and_then(Value::as_str) {
                    Some(r) => {
                        let (v, f, _) = spec.resolve(r, &file);
                        (v, f)
                    }
                    None => (resp, file.clone()),
                };
                let Some(content) = resp.get("content").and_then(Value::as_mapping) else {
                    continue;
                };
                for (_, media) in content.clone() {
                    if let Some(schema) = media.get("schema") {
                        let name = format!("{op_id}Response");
                        walk(
                            &mut spec,
                            schema,
                            &resp_file,
                            &name,
                            &mut fields,
                            &mut visited,
                        );
                    }
                }
            }
        }
    }
    assert!(
        fields.len() > 100,
        "derived too few response fields ({}) - is the walk broken?",
        fields.len()
    );
    fields
}

#[test]
fn response_operation_references_are_all_projected() {
    let exposed: BTreeSet<String> = response_fields()
        .into_iter()
        .filter(|f| f.ends_with(".operationReference"))
        .collect();
    let projected: BTreeSet<String> = PROJECTED_RESPONSE_OPERATION_REFERENCES
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        exposed, projected,
        "the spec's read-side `operationReference` fields (left) differ from the \
         ones Nano projects (right, PROJECTED_RESPONSE_OPERATION_REFERENCES in \
         server/src/request_field_guard.rs). Project a newly exposed field into \
         the read model (#1601) before listing it; drop entries upstream removed."
    );
}

#[test]
fn response_field_walk_detects_a_known_read_field() {
    // Liveness of the derivation above: a walk that silently found nothing
    // would make the projection guard vacuous.
    let fields = response_fields();
    assert!(
        fields.iter().any(|f| f.ends_with(".batchOperationKey")),
        "expected batch operation read results to be walked"
    );
}
