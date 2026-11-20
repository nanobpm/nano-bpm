//! Record-file comparisons for the determinism CI (ADR 0066 slice 6, #1665),
//! shared by the `determinism_corpus` CLI and `tests/determinism_corpus.rs`.
//!
//! * [`cross_release_checked`] — head vs previous-release records over the
//!   same (filtered) corpus: events re-decoded through the head `Event` type,
//!   rejections compared by presence, and **durable state** checkpoints
//!   compared through the head `EngineSnapshot` type whenever both builds
//!   share a `SNAPSHOT_FORMAT_VERSION` (a bumped format is the explicit,
//!   drift-guarded shape-change decision; the replay-migrator rebuilds such a
//!   snapshot from the event journal, which the event comparison covers);
//!   golden-recorded additive fields the old build never emits are erased,
//!   ratified observational variants filtered, and per-scenario ratified
//!   divergences admitted only as declared (`divergence-allowlist.json`);
//! * [`explain_drops`] — every entry the previous release cannot decode must
//!   be explained by a shape the golden serde corpus records as added since
//!   that release (#1698).

use std::collections::{BTreeMap, BTreeSet};

use nanobpmn_engine_core::EngineSnapshot;
use serde_json::Value as Json;

/// Parses a record file's lines.
pub fn parse_records(text: &str) -> Result<Vec<Json>, String> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).map_err(|e| format!("bad record line: {e}: {l}")))
        .collect()
}

fn is_state(record: &Json) -> bool {
    record.get("state").is_some()
}

/// The corpus indices a probe run marked undecodable.
pub fn undecodable_indices(probe: &[Json]) -> BTreeSet<u64> {
    probe
        .iter()
        .filter(|r| r.get("undecodable").is_some())
        .filter_map(|r| r.get("i").and_then(Json::as_u64))
        .collect()
}

/// An entry record's outcome: `"ok"` or `"err"`.
fn outcome(record: &Json) -> Result<&'static str, String> {
    if let Some(e) = record.get("undecodable") {
        return Err(format!("undecodable entry in a filtered corpus: {e}"));
    }
    if record.get("err").is_some() {
        return Ok("err");
    }
    if record.get("events").is_some() {
        return Ok("ok");
    }
    Err(format!("malformed record {record}"))
}

/// A state record's snapshot, re-encoded through the head type.
fn canonical_state(record: &Json, side: &str, erasable: &Vocab) -> Result<Json, String> {
    let state = record.get("state").cloned().unwrap_or(Json::Null);
    let snapshot: EngineSnapshot = serde_json::from_value(state)
        .map_err(|e| format!("{side} state does not decode under head: {e}"))?;
    let mut v = serde_json::to_value(&snapshot).map_err(|e| e.to_string())?;
    erase_fields(&mut v, "$", erasable);
    Ok(v)
}

/// The scope of a ratified upgrade-boundary `unbalanced` divergence for the
/// same-format state comparison: the first divergent entry, and each side's
/// process instances that the divergent entries touch.
#[derive(Debug, Default)]
struct RatifiedBoundary {
    from: Option<u64>,
    head_instances: BTreeSet<u64>,
    prev_instances: BTreeSet<u64>,
}

/// Whether a field name holds an entity key (or a list of them).
fn is_key_field(name: &str) -> bool {
    name == "key" || name == "scope" || name.ends_with("_key") || name.ends_with("_keys")
}

/// The key-insensitive projection of a canonical snapshot past a ratified
/// upgrade-boundary divergence (#1528 review). The divergence shifts every
/// later minted key and changes ITS OWN instances' state by declaration, so
/// the projection (a) drops every record owned by a ratified instance
/// (`excluded`: a keyed-map entry under such a key, or an object naming it as
/// `instance_key` / `process_instance_key`), (b) replaces every key value with
/// a placeholder and every key-indexed map with the sorted multiset of its
/// projected values, and (c) drops the key allocator (`next_local`). Every
/// other field — flags, variables, counts of jobs/tasks/timers of every other
/// instance — is still compared, so an unrelated post-boundary state
/// regression fails.
fn boundary_projection(value: &Json, excluded: &BTreeSet<u64>) -> Option<Json> {
    match value {
        Json::Object(map) => {
            for owner in ["instance_key", "process_instance_key"] {
                if map
                    .get(owner)
                    .and_then(Json::as_u64)
                    .is_some_and(|k| excluded.contains(&k))
                {
                    return None;
                }
            }
            let keyed = !map.is_empty() && map.keys().all(|k| k.parse::<u64>().is_ok());
            if keyed {
                let mut items: Vec<Json> = map
                    .iter()
                    .filter(|(k, _)| k.parse::<u64>().is_ok_and(|k| !excluded.contains(&k)))
                    .filter_map(|(_, v)| boundary_projection(v, excluded))
                    .collect();
                items.sort_by_key(|v| v.to_string());
                return Some(Json::Array(items));
            }
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if k == "next_local" {
                    continue;
                }
                let projected = if is_key_field(k) {
                    key_placeholder(v)
                } else {
                    match boundary_projection(v, excluded) {
                        Some(p) => p,
                        None => continue,
                    }
                };
                out.insert(k.clone(), projected);
            }
            Some(Json::Object(out))
        }
        Json::Array(items) => Some(Json::Array(
            items
                .iter()
                .filter_map(|v| boundary_projection(v, excluded))
                .collect(),
        )),
        other => Some(other.clone()),
    }
}

/// A key value (or list of keys) with each raw key replaced by a placeholder.
fn key_placeholder(v: &Json) -> Json {
    match v {
        Json::Number(n) if n.is_u64() => Json::String("#key".into()),
        Json::Array(items) => Json::Array(items.iter().map(key_placeholder).collect()),
        other => other.clone(),
    }
}

/// Compares state checkpoints (through the head `EngineSnapshot` type) when
/// both builds share a `SNAPSHOT_FORMAT_VERSION`, erasing `erasable` fields.
fn compare_states(
    head_states: &[&Json],
    prev_states: &[&Json],
    entries: usize,
    erasable: &Vocab,
    boundary: &RatifiedBoundary,
) -> Result<CrossReleaseReport, String> {
    if head_states.len() != prev_states.len() {
        return Err(format!(
            "state checkpoint counts differ: head {} vs prev {}",
            head_states.len(),
            prev_states.len()
        ));
    }
    if head_states.is_empty() {
        return Err("no state checkpoints to compare (run with --state-every)".into());
    }
    let (mut compared, mut skipped, mut projected) = (0, 0, 0);
    for (h, p) in head_states.iter().zip(prev_states.iter()) {
        let at = h.get("i").cloned().unwrap_or(Json::Null);
        if p.get("i") != h.get("i") {
            return Err(format!("state checkpoint positions differ at head i={at}"));
        }
        let format = |r: &Json| r.get("format").and_then(Json::as_u64);
        let (hf, pf) = (format(h), format(p));
        if hf.is_none() || pf.is_none() {
            return Err(format!("state checkpoint i={at} lacks its format version"));
        }
        if hf != pf {
            skipped += 1;
            continue;
        }
        // Always decode both checkpoints: a snapshot that no longer decodes
        // fails whatever the comparison mode (#1528 review).
        let (hs, ps) = (
            canonical_state(h, "head", erasable)?,
            canonical_state(p, "prev", erasable)?,
        );
        // Past a ratified upgrade-boundary `unbalanced` divergence the raw
        // snapshots cannot agree (its declared state change, and every later
        // minted key shifts), so they are compared through the
        // key-insensitive projection that excludes only the ratified
        // instances — every other state is still checked (#1528 review).
        if let (Some(from), Some(i)) = (boundary.from, at.as_u64()) {
            if i >= from {
                if boundary_projection(&hs, &boundary.head_instances)
                    != boundary_projection(&ps, &boundary.prev_instances)
                {
                    return Err(format!(
                        "durable state diverges at entry {at} outside the ratified \
                         upgrade-boundary instances (same snapshot format {hf:?})"
                    ));
                }
                projected += 1;
                continue;
            }
        }
        if hs != ps {
            return Err(format!(
                "durable state diverges at entry {at} (same snapshot format {hf:?})"
            ));
        }
        compared += 1;
    }
    Ok(CrossReleaseReport {
        entries,
        states_compared: compared,
        states_skipped_format_bump: skipped,
        states_compared_outside_ratified_boundary: projected,
    })
}

/// What the state/entry comparison covered.
#[derive(Debug, PartialEq, Eq)]
pub struct CrossReleaseReport {
    pub entries: usize,
    pub states_compared: usize,
    pub states_skipped_format_bump: usize,
    /// Checkpoints at or after the first entry of a ratified upgrade-boundary
    /// `unbalanced` divergence, compared key-insensitively with the ratified
    /// instances excluded (see `boundary_projection`).
    pub states_compared_outside_ratified_boundary: usize,
}

// ---- Previous-release gate: golden-derived additive set (#1698) ----------
//
// The previous-release gate drops every corpus entry the old build cannot
// decode. Such a drop is only legitimate when it is explained by a shape the
// **golden serde corpus** (`tests/fixtures/golden/`, the canonical
// additive-vs-breaking record pinned by `tests/golden_serde_drift.rs`)
// records as added since the previous release: the additive set is DERIVED
// as `vocabulary(golden @ HEAD) − vocabulary(golden @ prev tag)`, and a drop
// (or a new event variant / field in the compared stream) it does not
// explain fails the gate — never a silent coverage shrink (#1065 class).

/// A golden-corpus shape vocabulary: externally-tagged variant names
/// (`"JobCompleted"`) and variant-qualified field names
/// (`"JobCompleted.completed_at"`; `"$.field"` outside any variant).
pub type Vocab = BTreeSet<String>;

/// Golden files whose content IS a token list (see [`shape_tokens`]) rather
/// than a serialized value: `command_shapes.v<N>.json` /
/// `event_shapes.v<N>.json` (`<kind>_shapes.v<N>.json`).
pub const SHAPES_MARKER: &str = "_shapes.v";

fn is_variant_tag(key: &str) -> bool {
    key.starts_with(|c: char| c.is_ascii_uppercase())
}

/// A Rust field name (`snake_case`). Any other non-variant key is map DATA
/// (an instance key, a variable name) — never a shape token, so never erased.
fn is_field_name(key: &str) -> bool {
    key.starts_with(|c: char| c.is_ascii_lowercase())
        && key
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// The engine source files whose types reach the serialized `Command` /
/// `Event` / `EngineSnapshot` shapes the gate compares. The map fields and
/// serialized unit variants they declare are DERIVED from this source (never
/// hand-listed), so a new one is covered the moment it is declared;
/// `data_map_fields_cover_every_engine_map_field` and
/// `unit_variants_cover_every_engine_unit_variant` (tests) fail if one is
/// declared in a file missing from this list.
pub const ENGINE_TYPE_SOURCES: &[&str] = &[
    include_str!("../../src/command.rs"),
    include_str!("../../src/event.rs"),
    include_str!("../../src/model.rs"),
    include_str!("../../src/state/types.rs"),
    include_str!("../../src/engine/mod.rs"),
    include_str!("../../src/cluster_vars.rs"),
    include_str!("../../src/engine/call_activity.rs"),
    include_str!("../../src/bpmn/build.rs"),
    include_str!("../../src/validate/mod.rs"),
    include_str!("../../src/agent.rs"),
    include_str!("../../src/dmn/model.rs"),
];

/// The map field names (`name: BTreeMap<K, _>`, also `HashMap`/`IndexMap`,
/// optionally wrapped in `Option<…>`, for ANY key type `K` — aliases such as
/// `ElementId` included) declared in `source`. A superset is safe: a name wrongly treated as a map only stops
/// erasure under it, so a real difference still surfaces (fail closed).
pub fn map_fields_in(source: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in source.lines() {
        let line = line.trim_start();
        let line = line
            .strip_prefix("pub(crate) ")
            .or_else(|| line.strip_prefix("pub(super) "))
            .or_else(|| line.strip_prefix("pub "))
            .unwrap_or(line);
        let Some((name, ty)) = line.split_once(':') else {
            continue;
        };
        if name.is_empty() || !is_field_name(name) {
            continue;
        }
        let ty = ty.trim_start();
        let ty = ty.strip_prefix("Option<").unwrap_or(ty);
        let ty = ty.strip_prefix("std::collections::").unwrap_or(ty);
        // ANY key type, not just a literal `String`: a key-type alias
        // (`BTreeMap<ElementId, Element>`, `ElementId = String`) or a
        // numeric key still serializes as a JSON object whose keys are DATA.
        if ["BTreeMap<", "HashMap<", "IndexMap<"]
            .iter()
            .any(|p| ty.starts_with(p))
        {
            out.insert(name.to_owned());
        }
    }
    out
}

/// Every map field of the compared engine types (see
/// [`ENGINE_TYPE_SOURCES`]). Under such a field the object's keys are DATA
/// (variable names, header names, process ids) — never struct fields — so
/// they are neither shape tokens nor erasable, whatever their values hold
/// (`Value`s, plain strings like `ServiceTask.custom_headers`, or structs).
pub fn data_map_fields() -> &'static BTreeSet<String> {
    static FIELDS: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
    FIELDS.get_or_init(|| {
        ENGINE_TYPE_SOURCES
            .iter()
            .flat_map(|s| map_fields_in(s))
            .collect()
    })
}

/// The unit variant names (`Name,` lines inside an `enum … {` body) of the
/// serde-`Serialize`d enums declared in `source`. A superset is safe: it only lets a matching bare string add
/// a (known) token.
pub fn unit_variants_in(source: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut depth: Option<usize> = None; // brace depth inside an enum body
                                         // Whether the attributes since the last item derive `Serialize` (plain
                                         // or `cfg_attr(feature = "serde", …)`): only serialized enums reach the
                                         // compared shapes.
    let mut serialized = false;
    for line in source.lines() {
        let t = line.trim();
        match depth {
            None => {
                if t.starts_with("#[") {
                    serialized |= t.contains("Serialize");
                    continue;
                }
                if t.starts_with("//") || t.is_empty() {
                    continue;
                }
                let derives = std::mem::take(&mut serialized);
                let decl = t
                    .strip_prefix("pub(crate) ")
                    .or_else(|| t.strip_prefix("pub(super) "))
                    .or_else(|| t.strip_prefix("pub "))
                    .unwrap_or(t);
                if derives && decl.starts_with("enum ") && t.ends_with('{') {
                    depth = Some(1);
                }
            }
            Some(d) => {
                if d == 1 {
                    let name = t.strip_suffix(',').unwrap_or(t);
                    let name = name.split_once(" =").map_or(name, |(n, _)| n);
                    if is_variant_tag(name) && name.chars().all(|c| c.is_ascii_alphanumeric()) {
                        out.insert(name.to_owned());
                    }
                }
                let opens = t.matches('{').count();
                let closes = t.matches('}').count();
                let nd = (d + opens).saturating_sub(closes);
                depth = (nd > 0).then_some(nd);
            }
        }
    }
    out
}

/// Every unit variant of the compared engine types (see
/// [`ENGINE_TYPE_SOURCES`]). A unit variant serializes as a bare string at an
/// enum-valued location at ANY depth (`{"kind": "StartEvent"}` inside
/// `DeployProcess`), so it is recorded wherever it appears — but only a
/// string naming a DECLARED unit variant, never arbitrary application text.
pub fn unit_variants() -> &'static BTreeSet<String> {
    static VARIANTS: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
    VARIANTS.get_or_init(|| {
        ENGINE_TYPE_SOURCES
            .iter()
            .flat_map(|s| unit_variants_in(s))
            .collect()
    })
}

/// Whether `map` is a `BTreeMap<String, Value>` of variable/metadata entries
/// rather than a struct. Every such map in `Command`/`Event`/`EngineSnapshot`
/// holds a typed [`Value`] per key, and a `Value` serializes as an
/// externally-tagged object (`{"Bool": …}`, `{"Str": …}`, `{"Map": {…}}`) or
/// the bare string `"Null"` — a shape no flattened struct field has. This is
/// the discriminator that keeps a snake_case VARIABLE name (e.g. a
/// `variables.receipt` key) from being mistaken for a same-named struct field
/// under serde flattening: inside a variable map, keys are DATA, never
/// erasable fields.
fn is_value_map(map: &serde_json::Map<String, Json>) -> bool {
    !map.is_empty()
        && map.values().all(|v| match v {
            // `Value::Null` serializes as the bare string "Null".
            Json::String(s) => s == "Null",
            // Every other `Value` variant is a single-key externally-tagged
            // object (`{"Bool": …}`, `{"List": […]}`, …).
            Json::Object(inner) => inner.len() == 1 && is_variant_tag(inner.keys().next().unwrap()),
            _ => false,
        })
}

/// Adds `value`'s shape tokens to `out`. An object key starting with an
/// upper-case letter is an externally-tagged variant (token = the name, and
/// it becomes the context of everything below it); any other key is a field,
/// qualified by its nearest enclosing variant; any other key (map data: a
/// numeric key, a camelCase variable name) contributes no token.
pub fn shape_tokens(value: &Json, out: &mut Vocab) {
    walk_tokens(value, "$", out);
}

fn walk_tokens(value: &Json, ctx: &str, out: &mut Vocab) {
    match value {
        Json::Object(map) => {
            // A `BTreeMap<String, Value>` of variables/metadata is DATA: its
            // keys are variable names, not fields, so it contributes no tokens
            // and is not descended into (a snake_case variable name would
            // otherwise be misread as a struct field).
            if is_value_map(map) {
                return;
            }
            for (k, v) in map {
                if is_variant_tag(k) {
                    out.insert(k.clone());
                    walk_tokens(v, k, out);
                } else {
                    if is_field_name(k) {
                        out.insert(format!("{ctx}.{k}"));
                    }
                    if is_field_name(k) && data_map_fields().contains(k) {
                        // A map field: its keys are DATA. Only
                        // its values (which may be structs) carry shapes.
                        walk_map_values(v, ctx, out);
                        continue;
                    }
                    // Propagate the context through a snake_case struct field
                    // (serde flattens nested structs, e.g. `multi_instance`'s
                    // fields stay under the variant). Map DATA keys — a
                    // variable name like `receipt`, a camelCase key — are not
                    // `is_field_name`, so they contribute no token; a
                    // snake_case map key is guarded by the `is_value_map`
                    // boundary above (see [`erase_fields`]).
                    walk_tokens(v, ctx, out);
                }
            }
        }
        Json::Array(items) => items.iter().for_each(|v| walk_tokens(v, ctx, out)),
        Json::String(s)
            if is_variant_tag(s) && (ctx == "$" || unit_variants().contains(s.as_str())) =>
        {
            // A unit variant serializes as a bare string (`"Ping"` at the
            // root, `"StartEvent"` nested under `DeployProcess`).
            out.insert(s.clone());
        }
        _ => {}
    }
}

/// Walks the VALUES of a data map (never its keys), keeping `ctx`.
fn walk_map_values(value: &Json, ctx: &str, out: &mut Vocab) {
    match value {
        Json::Object(map) if !is_value_map(map) => {
            map.values().for_each(|v| walk_tokens(v, ctx, out));
        }
        Json::Object(_) => {}
        other => walk_tokens(other, ctx, out),
    }
}

/// The vocabulary of every `*.json` golden file in `dir` (all format
/// versions: a shape recorded at any version is known to that release).
pub fn golden_vocab(dir: &std::path::Path) -> Result<Vocab, String> {
    let mut out = Vocab::new();
    let entries = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let value: Json =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if name.contains(SHAPES_MARKER) {
            let tokens = value
                .as_array()
                .ok_or_else(|| format!("{name}: expected a token array"))?;
            out.extend(tokens.iter().filter_map(Json::as_str).map(str::to_owned));
        } else {
            shape_tokens(&value, &mut out);
        }
    }
    if out.is_empty() {
        return Err(format!("no golden shapes under {}", dir.display()));
    }
    Ok(out)
}

/// The sorted shape-token list a `<kind>_shapes.v<N>.json` golden records:
/// the vocabulary of the determinism corpus's commands
/// (`command_shapes`) or of the events they emit (`event_shapes`).
pub fn shape_list<'a>(values: impl IntoIterator<Item = &'a Json>) -> Vec<String> {
    let mut out = Vocab::new();
    values.into_iter().for_each(|c| shape_tokens(c, &mut out));
    out.into_iter().collect()
}

/// The `Variant.field` tokens of a runner `lossy decode` report (a field the
/// previous release's `Command` type silently ignores), or `None` for any
/// other undecodable report.
pub fn lossy_tokens(error: &str) -> Option<Vec<String>> {
    if !error.starts_with("lossy decode:") {
        return None;
    }
    Some(
        error
            .split("drops field `")
            .skip(1)
            .filter_map(|part| part.split_once('`').map(|(t, _)| t.to_owned()))
            .collect(),
    )
}

/// Whether the fields a previous release ignores in `command` (`tokens`, from
/// [`lossy_tokens`]) all hold the HEAD build's default. Compared as the
/// DECODED command, not raw JSON: the raw line omits every field the
/// serializer defaults, so a raw comparison cannot tell an omitted field
/// from an explicit one. The corpus line decodes to the typed command (the
/// ignored fields simply absent), and it is benign iff that typed value
/// equals the typed value with the fields removed — i.e. the head build's
/// defaults for those fields reproduce the line's explicit values. An
/// explicit value against a non-empty default (e.g.
/// `cancel_remaining_instances: false`, default `true`) is NOT benign.
pub fn lossy_is_benign(command: &Json, tokens: &[String]) -> bool {
    // Decode the full line first; a line the head build cannot decode is not
    // a benign lossy case at all.
    let Ok(full) = serde_json::from_value::<nanobpmn_engine_core::Command>(command.clone()) else {
        return false;
    };
    // Re-encode, drop the named fields from the canonical (fully-defaulted)
    // form, and decode again: the result is the command with those fields at
    // their head defaults. Benign iff that equals the full decode.
    let Ok(mut canonical) = serde_json::to_value(&full) else {
        return false;
    };
    let erasable: Vocab = tokens.iter().cloned().collect();
    erase_fields(&mut canonical, "$", &erasable);
    serde_json::from_value::<nanobpmn_engine_core::Command>(canonical)
        .ok()
        .is_some_and(|stripped| stripped == full)
}

/// Why the previous release could not decode an entry, if the golden-derived
/// additive set explains it.
fn explain(error: &str, added: &Vocab) -> Option<String> {
    let quoted = |marker: &str| {
        let rest = error.split_once(marker)?.1;
        rest.split_once('`').map(|(name, _)| name.to_owned())
    };
    if let Some(tokens) = lossy_tokens(error) {
        // Every dropped field must be golden-recorded as added.
        if tokens.is_empty() || tokens.iter().any(|t| !added.contains(t)) {
            return None;
        }
        let fields: Vec<String> = tokens.iter().map(|t| format!("`{t}`")).collect();
        return Some(format!("field {} (lossy decode)", fields.join(", ")));
    }
    if let Some(variant) = quoted("unknown variant `") {
        return added
            .contains(&variant)
            .then(|| format!("variant `{variant}`"));
    }
    if let Some(field) = quoted("unknown field `") {
        let suffix = format!(".{field}");
        return added
            .iter()
            .find(|t| t.ends_with(&suffix))
            .map(|t| format!("field `{t}`"));
    }
    None
}

/// The undecodable entries of a previous-release probe run, each **explained**
/// by a golden-recorded addition. Fails — listing every offender — when any
/// drop is not explained: an undeclared new variant, an undecodable change
/// the golden corpus does not record as additive (a breaking change), or any
/// other decode failure. Returns `(index, explanation)` per drop.
pub fn explain_drops(probe: &[Json], added: &Vocab) -> Result<Vec<(u64, String)>, String> {
    let mut explained = Vec::new();
    let mut unexplained = Vec::new();
    for r in probe {
        let Some(err) = r.get("undecodable") else {
            continue;
        };
        let i = r
            .get("i")
            .and_then(Json::as_u64)
            .ok_or_else(|| format!("undecodable record without an index: {r}"))?;
        let err = err.as_str().unwrap_or_default();
        match explain(err, added) {
            Some(why) => explained.push((i, why)),
            None => unexplained.push(format!("  entry {i}: {err}")),
        }
    }
    if !unexplained.is_empty() {
        return Err(format!(
            "{} of {} dropped entries are NOT explained by a shape the golden serde corpus \
             records as added since the previous release (an undeclared new variant/field, or \
             a breaking change):\n{}",
            unexplained.len(),
            unexplained.len() + explained.len(),
            unexplained.join("\n")
        ));
    }
    Ok(explained)
}

// ---- Behavioural-divergence allowlist (#1698) -----------------------------

/// The ratified previous-release divergences (`divergence-allowlist.json`).
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Allowlist {
    /// New event variants a head build emits and the previous release never
    /// does, ratified as safe for a rolling upgrade; filtered from the head
    /// stream before comparison. Each must be golden-recorded as added.
    #[serde(default)]
    pub observational_variants: Vec<ObservationalVariant>,
    /// Ratified per-scenario behavioural changes.
    #[serde(default)]
    pub divergences: Vec<Divergence>,
}

/// How a release containing a ratified divergence may be upgraded to. Raft
/// members RE-EXECUTE committed commands (ADR 0066, "Rolling upgrade",
/// option C), so any allowlisted divergence makes mixed-version members
/// disagree unless the new behaviour is gated. Every entry must say which of
/// exactly two cases applies; there is no default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpgradeMode {
    /// The new behaviour runs only once every voter AND learner advertises
    /// the capability (#1661 cluster-join feature advertisement); the old
    /// behaviour runs until then, so mixed-version clusters stay consistent.
    FeatureGated,
    /// The release requires an all-nodes-same-version (quiesced) upgrade: no
    /// rolling upgrade across it. Must be called out in the release notes.
    UpgradeBoundary,
}

impl UpgradeMode {
    /// The literal the allowlist (and its rationale) uses.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FeatureGated => "feature-gated",
            Self::UpgradeBoundary => "upgrade-boundary",
        }
    }
}

/// A ratified new event variant.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationalVariant {
    pub variant: String,
    /// The first release tag that emits it. Expires once the previous
    /// release is at or past it (both builds emit it; nothing to declare).
    pub introduced_in: String,
    pub issue: String,
    /// How a cluster may upgrade across it (required, no default).
    pub upgrade: UpgradeMode,
    /// Why the declared [`UpgradeMode`] applies; must name it.
    pub rationale: String,
}

/// What a ratified scenario divergence is allowed to change — and nothing
/// else: per entry, after these normalizations the two event streams must be
/// identical, so any undeclared extra divergence still fails. The
/// `only_in_prev` / `only_in_head` patterns are the one relaxation of "per
/// entry": a ratified `reorder` divergence may MOVE an event across entries
/// (#1244 fires a parallel join's activation in a later entry than the first
/// arriving token's), so the matched events reconcile at SCENARIO level — the
/// multiset matched on one side must equal the multiset matched on the other
/// (same scenario, same element, same instance after the ratified remint). An
/// unbalanced pattern (removed but never re-added, or the reverse) fails —
/// unless the divergence carries an explicit [`Unbalanced`] declaration
/// (#1528), the scoped ratification for an upgrade-boundary behavioural
/// change whose new events have NO counterpart on the previous release.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expected {
    /// The events may come in a different order (compared as a multiset).
    #[serde(default)]
    pub reorder: bool,
    /// The engine keys the events mint (`key` / `*_key` fields) may differ —
    /// the footprint of minting in a different order (#1244 fork order), a
    /// bijection on keys. Normalization rewrites every key reference to the
    /// identity of the entity it references (from that side's own creation
    /// events), so a clean re-mint reconciles while a reference to the wrong
    /// or an un-minted entity (a completion/job/subscription naming a key its
    /// entry never created) canonicalizes to a distinct orphan token and still
    /// diverges; and an entry whose minting is not a clean one-key-per-entity
    /// (a collision, or two keys for one element) fails closed rather than
    /// reconcile.
    #[serde(default)]
    pub remint_keys: bool,
    /// Events only the previous release emits (dropped from its side). With
    /// `reorder`, matched events reconcile at SCENARIO level against
    /// `only_in_head` (see the type doc): a cross-entry move balances, a real
    /// loss fails.
    #[serde(default)]
    pub only_in_prev: Vec<EventPattern>,
    /// Events only the head build emits (dropped from its side).
    #[serde(default)]
    pub only_in_head: Vec<EventPattern>,
    /// A field whose value head newly populates where the previous release's
    /// journal decodes to the serde default — a behavioural refinement on a
    /// golden-added field, declared per (variant, field) with the value the
    /// field is normalized to on BOTH sides. Normalization rewrites the
    /// field to `value` wherever the variant carries it, so a head that
    /// records the real value and a prev that decodes to the default
    /// reconcile. This is the ONLY sanctioned relaxation of value equality:
    /// it is scoped to one variant's one field, so any other value
    /// difference still fails closed. (#1632: `ElementActivating.scope` for
    /// an ad-hoc tool's `#innerInstance` and its child is the real container
    /// scope on head, `0` on the previous release.)
    #[serde(default)]
    pub field_overrides: Vec<FieldOverride>,
    /// A lifecycle-kind substitution: head emits `head` where the previous
    /// release emitted `prev` for the SAME element instance (#1535 terminates
    /// an interrupted activity — `ElementTerminating`/`ElementTerminated` —
    /// where the previous release completed it —
    /// `ElementCompleting`/`ElementCompleted`). During scenario-level
    /// reconciliation the matched events' variant tag is rewritten through
    /// this mapping on BOTH sides, so a substituted event balances against
    /// its counterpart (same entity, same scope, same keys). An event whose
    /// variant is not mapped keeps its tag, so an undeclared substitution
    /// still fails the balance. Only `only_in_prev`/`only_in_head`-matched
    /// events are rewritten — the per-entry strict comparison is unaffected.
    #[serde(default)]
    pub variant_substitutions: Vec<VariantSubstitution>,
    /// The explicit ratification for a scenario whose declared
    /// `only_in_prev` / `only_in_head` patterns do NOT balance (#1528): the
    /// behavioural change emits (or stops emitting) events that have no
    /// counterpart on the other build at all — e.g. an MI user task's
    /// `UserTaskCreated` per child, which the previous release never emits
    /// because it completed each child at once. `None` (the default) keeps
    /// the strict balance requirement. Because an unbalanced declaration
    /// weakens the lost-event guard, it is valid only on a divergence that
    /// also names its issue and a non-empty reason (see
    /// [`Unbalanced::valid`]).
    #[serde(default)]
    pub unbalanced: Option<Unbalanced>,
}

/// The scoped ratification of an UNBALANCED scenario divergence (#1528):
/// after the declared `only_in_prev` / `only_in_head` events are dropped, the
/// two sides' matched multisets may differ — the change adds or removes
/// events outright rather than moving them. Everything outside the declared
/// patterns is still compared strictly per entry, and the declaration is
/// scenario-scoped (the `Divergence` carries it), so it cannot mask a
/// divergence anywhere else.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Unbalanced {
    /// Why the imbalance is the intended new behaviour — must name the
    /// behavioural change (e.g. "Zeebe parity: an MI userTask child creates a
    /// real user task"), never a blanket "events differ".
    pub reason: String,
}

impl Unbalanced {
    /// Whether this declaration may ratify an unbalanced scenario: it must
    /// carry a non-empty reason AND the divergence must name its tracking
    /// issue, so the declaration is always traceable to a reviewed decision.
    fn valid(&self, issue: &str) -> bool {
        !self.reason.trim().is_empty() && !issue.trim().is_empty()
    }
}

/// Renames one event variant to another for scenario-level reconciliation
/// (see [`Expected::variant_substitutions`]).
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariantSubstitution {
    /// The variant the previous release emitted.
    pub prev: String,
    /// The variant head emits in its place.
    pub head: String,
}

/// Normalizes one variant's field to a fixed value on both sides (see
/// [`Expected::field_overrides`]).
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldOverride {
    pub variant: String,
    pub field: String,
    /// The value the field is rewritten to on both sides before comparison.
    pub value: Json,
}

impl FieldOverride {
    /// Rewrites `event`'s `field` to `value` when `event` is `variant`.
    fn apply(&self, event: &mut Json) {
        if let Some(body) = event.get_mut(&self.variant).and_then(Json::as_object_mut) {
            if body.contains_key(&self.field) {
                body.insert(self.field.clone(), self.value.clone());
            }
        }
    }
}

/// Matches an event by variant and, optionally, its `element_id` and/or (for a
/// `SequenceFlowTaken`) its `from` source element.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventPattern {
    pub variant: String,
    #[serde(default)]
    pub element_id: Option<String>,
    /// A `SequenceFlowTaken` carries no `element_id`; its source element is
    /// `from`. Scoping on it lets a divergence name ONE flow (the body's
    /// advance, `review → end`) without also matching a flow both sides take
    /// (the entry into the body, `start → review`) — #1528.
    #[serde(default)]
    pub from: Option<String>,
}

impl EventPattern {
    fn matches(&self, event: &Json) -> bool {
        let Some(body) = event.get(&self.variant) else {
            return false;
        };
        self.element_id
            .as_deref()
            .is_none_or(|id| body.get("element_id").and_then(Json::as_str) == Some(id))
            && self
                .from
                .as_deref()
                .is_none_or(|f| body.get("from").and_then(Json::as_str) == Some(f))
    }
}

/// The entity a minted key refers to. `remint_keys` declares the keys may be
/// re-minted in a different order (#1244 fork order) — a bijection on keys —
/// so the comparison must normalize each key to *which entity it references*,
/// not erase it. Erasing (zeroing) hides an inconsistent reference: a
/// completion/job/subscription pointing at the wrong entity zeroes to the
/// same shape as the correct one.
///
/// Identities are ONE-TO-ONE per side: each minted key gets
/// `<kind>:<owner>#<n>`, where `<owner>` is the process instance plus element
/// id (element instances) or the identity of the owning element instance
/// (jobs, subscriptions), and `<n>` is the ordinal of that mint for its owner
/// over the whole corpus. Two `pay` jobs in different entries (or a retried
/// job) therefore never share an identity, so a reference to *another*
/// already-minted same-element key cannot canonicalize to the right one.
/// Mints:
/// * `ElementActivating`/`ElementActivated` → `element:<instance>:<element_id>#n`;
/// * `ParallelJoinOpened` → the same element identity: since #1244 a not-ready
///   join is PARKED — it mints its element-instance key here, without
///   activating (no `ElementActivating` until it fires), so this event is the
///   key's only mint sighting on a head build while a previous release
///   activates the join visibly. Registering it keeps the parked join's key
///   from canonicalizing to an orphan;
/// * `JobCreated` / `ExecutionListenerJobCreated` / `TaskListenerJobCreated`
///   → `job:<owning element identity>#n`;
/// * `TimerCreated` → `timer:<owning element identity>#n`;
/// * `MessageSubscriptionCreated` / `SignalSubscriptionCreated` /
///   `ConditionalSubscriptionCreated` → `subscription:<owning element identity>#n`.
#[derive(Debug, Default, Clone)]
struct KeyIds {
    ids: std::collections::BTreeMap<u64, String>,
    ordinals: std::collections::BTreeMap<String, u64>,
}

impl KeyIds {
    fn new() -> Self {
        Self::default()
    }

    fn get(&self, key: &u64) -> Option<&String> {
        self.ids.get(key)
    }

    /// The identity of an owning element instance key (or a distinct orphan
    /// token when it was never minted on this side).
    fn owner(&self, key: u64) -> String {
        self.ids
            .get(&key)
            .cloned()
            .unwrap_or_else(|| format!("?orphan:{key}"))
    }

    /// Records the mint of `key` for `base`. Re-sighting the same key (an
    /// `ElementActivating` followed by its `ElementActivated`) is idempotent
    /// only for the same base; a key minted for two different entities fails
    /// closed.
    fn mint(&mut self, key: u64, base: String) -> Result<(), String> {
        if let Some(prev) = self.ids.get(&key) {
            let prev_base = prev.rsplit_once('#').map_or(prev.as_str(), |(b, _)| b);
            if prev_base == base {
                return Ok(());
            }
            return Err(format!(
                "key {key} minted for two entities across the corpus ({prev} vs {base})"
            ));
        }
        let n = self.ordinals.entry(base.clone()).or_insert(0);
        *n += 1;
        self.ids.insert(key, format!("{base}#{n}"));
        Ok(())
    }

    /// Re-sights `key` for `base`, the identity of an entity `key` may already
    /// carry. Unlike [`KeyIds::mint`] this never assigns a new ordinal: an
    /// unseen key canonicalizes to the base itself (no `#n`), so the result
    /// does not depend on which sighting order the entry's events arrived in
    /// (#1528 review: an MI child's generic `ElementActivating` and its
    /// `MultiInstanceChildActivated` must yield ONE identity either way).
    /// A key already carrying a DIFFERENT base is a collision and fails
    /// closed — a `UserTaskCreated` reusing its child's key, or a child
    /// reusing a job key, is an inconsistent mint, not a re-sighting.
    fn resight(&mut self, key: u64, base: &str) -> Result<(), String> {
        match self.ids.get(&key) {
            None => {
                self.ids.insert(key, base.to_owned());
                Ok(())
            }
            Some(existing) => {
                let existing_base = existing
                    .rsplit_once('#')
                    .map_or(existing.as_str(), |(b, _)| b);
                if existing_base == base {
                    Ok(())
                } else {
                    Err(format!(
                        "key {key} minted for two entities across the corpus ({existing} vs {base})"
                    ))
                }
            }
        }
    }
}

/// Folds `events`' minted keys into `ids` (one side, in corpus order).
///
/// Three passes keep every identity independent of the entry's event order
/// (a ratified `reorder` may permute it): first the `ProcessInstanceCreated`
/// mints (an element activation references its owning `instance_key`, which
/// the reorder may move after the reference), then the
/// `MultiInstanceChildActivated` mints (a child's identity must not depend on
/// whether its generic `ElementActivating` or its child activation is seen
/// first — #1528 review), then every other mint.
fn extend_identities(events: &[String], ids: &mut KeyIds) -> Result<(), String> {
    let parsed: Vec<(String, Json)> = events
        .iter()
        .filter_map(|e| serde_json::from_str::<Json>(e).ok())
        .filter_map(|v| {
            // Every event is a single-key externally-tagged object; a bare
            // unit-variant string carries no keys to canonicalize.
            variant_body(&v).map(|(variant, body)| (variant.to_owned(), body.clone()))
        })
        .collect();
    let u64_of = |body: &Json, f: &str| body.get(f).and_then(Json::as_u64);
    fn str_of<'a>(body: &'a Json, f: &str) -> Option<&'a str> {
        body.get(f).and_then(Json::as_str)
    }
    // Pass 1: the process-instance key itself. A release that mints extra
    // keys mid-corpus (#1528's per-child user-task keys) shifts every LATER
    // instance key, so the instance must canonicalize to WHICH instance it
    // is, not its raw key. The base is the process id; `mint` appends the
    // per-process creation ordinal, so two instances of one process stay
    // distinct while the same instance agrees across the offset.
    for (_, body) in parsed
        .iter()
        .filter(|(variant, _)| *variant == "ProcessInstanceCreated")
    {
        if let (Some(k), Some(pid)) = (u64_of(body, "instance_key"), str_of(body, "process_id")) {
            ids.mint(k, format!("instance:{pid}"))?;
        }
    }
    // Pass 2: a multi-instance child mints its element-instance key at
    // activation. The child's `ElementActivating` re-sights the same key but
    // under a base that cannot tell the children apart (same element id, same
    // instance), so the child's identity is registered HERE — keyed by the
    // body element's CANONICAL identity plus the child ordinal, so equivalent
    // bodies with different raw keys across releases still agree (#1528
    // review) and the identity is the same whichever sighting comes first
    // (a `resight`, never an ordinal mint). This matters when a release
    // changes what a child mints: #1528 adds a per-child user-task key, so
    // the head build's child keys interleave with the task keys while the
    // previous release's stay contiguous — a clean per-child remint that
    // only a distinct, order-independent child identity reconciles.
    // Pass 2a: register every MI BODY's element identity first — a body is
    // usually activated in the same entry as its children, and a child's
    // identity embeds its body's CANONICAL identity, so the body must be
    // known before any child is derived (#1528 review). Nested bodies are
    // element activations too, so they are covered here as well; a later
    // pass-3 sighting of the same key re-mints idempotently (same base).
    let body_keys: BTreeSet<u64> = parsed
        .iter()
        .filter(|(variant, _)| *variant == "MultiInstanceChildActivated")
        .filter_map(|(_, body)| u64_of(body, "body_key"))
        .collect();
    for (_, body) in parsed
        .iter()
        .filter(|(variant, _)| *variant == "ElementActivating" || *variant == "ElementActivated")
    {
        if let (Some(k), Some(id)) = (
            u64_of(body, "element_instance_key"),
            str_of(body, "element_id"),
        ) {
            if body_keys.contains(&k) {
                let instance = u64_of(body, "instance_key")
                    .map_or_else(|| "?".to_owned(), |ik| ids.owner(ik).clone());
                ids.mint(k, format!("element:{instance}:{id}"))?;
            }
        }
    }
    // Pass 2b: the children.
    for (_, body) in parsed
        .iter()
        .filter(|(variant, _)| *variant == "MultiInstanceChildActivated")
    {
        if let (Some(k), Some(index)) = (u64_of(body, "child_key"), u64_of(body, "index")) {
            let body_id =
                u64_of(body, "body_key").map_or_else(|| "?".to_owned(), |b| ids.owner(b).clone());
            let instance = u64_of(body, "instance_key")
                .map_or_else(|| "?".to_owned(), |ik| ids.owner(ik).clone());
            ids.resight(k, &format!("mi-child:{instance}:{body_id}#{index}"))?;
        }
    }
    // Pass 3: every other mint, in corpus order.
    for (variant, body) in &parsed {
        let minted = match variant.as_str() {
            "ElementActivating" | "ElementActivated" | "ParallelJoinOpened" => {
                // `ParallelJoinOpened` mints the (parked, #1244) join's
                // element-instance key without activating it; the firing
                // entry's `ElementActivating` re-sights the same key for the
                // same base (idempotent). The owning instance is its CANONICAL
                // identity (the `ProcessInstanceCreated` mint), not its raw
                // key, so a corpus-wide instance-key offset (#1528) does not
                // leak into every element identity.
                u64_of(body, "element_instance_key")
                    .zip(str_of(body, "element_id"))
                    .map(|(k, id)| {
                        let instance = u64_of(body, "instance_key")
                            .map_or_else(|| "?".to_owned(), |ik| ids.owner(ik).clone());
                        (k, format!("element:{instance}:{id}"))
                    })
            }
            "ProcessInstanceCreated" | "MultiInstanceChildActivated" => None, // passes 1 and 2
            "JobCreated" | "ExecutionListenerJobCreated" | "TaskListenerJobCreated" => {
                u64_of(body, "job_key").map(|k| {
                    let owner = u64_of(body, "element_instance_key")
                        .map_or_else(|| "?".to_owned(), |o| ids.owner(o));
                    (k, format!("job:{variant}:{owner}"))
                })
            }
            "TimerCreated" => u64_of(body, "timer_key").map(|k| {
                let owner = u64_of(body, "element_instance_key")
                    .map_or_else(|| "?".to_owned(), |o| ids.owner(o));
                (k, format!("timer:{owner}"))
            }),
            "UserTaskCreated" => u64_of(body, "user_task_key").map(|k| {
                let owner = u64_of(body, "element_instance_key")
                    .map_or_else(|| "?".to_owned(), |o| ids.owner(o));
                (k, format!("user-task:{owner}"))
            }),
            "MessageSubscriptionCreated"
            | "SignalSubscriptionCreated"
            | "ConditionalSubscriptionCreated" => u64_of(body, "subscription_key").map(|k| {
                let owner = u64_of(body, "element_instance_key")
                    .map_or_else(|| "?".to_owned(), |o| ids.owner(o));
                (k, format!("subscription:{owner}"))
            }),
            _ => None,
        };
        if let Some((k, base)) = minted {
            // A multi-instance child's key was already registered by pass 2
            // under its distinct `mi-child:…` identity; its generic
            // `ElementActivating`/`ElementActivated` is a lifecycle
            // re-sighting of THAT identity (the generic base cannot tell the
            // children apart), so it is skipped — but only when the key
            // already carries a `mi-child:…` identity. Every other sighting
            // goes through `mint`, which rejects a key reused for an
            // unrelated entity (a `UserTaskCreated` reusing its child's key,
            // a child reusing a job key) as the collision it is (#1528
            // review).
            let generic_child_resight = base.starts_with("element:")
                && ids
                    .get(&k)
                    .is_some_and(|existing| existing.starts_with("mi-child:"));
            if !generic_child_resight {
                ids.mint(k, base)?;
            }
        }
    }
    Ok(())
}

/// Rewrites every `key`/`*_key` reference of `value` to the identity of the
/// entity it references, preserving the event's structure. A clean remint is
/// a bijection, so both sides canonicalize every reference to the same
/// per-entity token and reconcile; a reference to a key never minted (an
/// inconsistent or colliding reference) rewrites to a distinct `?orphan`
/// token, so it diverges unless the raw key is identical on both sides
/// (`instance_key`/`scope_key`: the process instance is not re-minted by
/// fork order).
///
/// The bare `scope` field is a key too: `ElementActivating`/`ElementActivated`
/// (and `ExecutionListenerJobCreated`, the `ParallelJoin*` events) name their
/// enclosing element instance as `scope` — an element-instance key, not
/// application data (a `scope` key inside a `variables`/`custom_headers` map
/// is never reached: those maps are `BTreeMap<String, Value>`/string data,
/// and a numeric `scope` value only ever appears at an event-body key).
fn canonicalize_keys(value: &mut Json, ids: &KeyIds) {
    match value {
        Json::Object(map) => {
            for (k, v) in map.iter_mut() {
                if (k == "key" || k == "scope" || k.ends_with("_key")) && v.is_u64() {
                    let raw = v.as_u64().unwrap_or(0);
                    let token = match ids.get(&raw) {
                        Some(identity) => Json::String(identity.clone()),
                        None => Json::String(format!("?orphan:{raw}")),
                    };
                    *v = token;
                } else {
                    canonicalize_keys(v, ids);
                }
            }
        }
        Json::Array(items) => items.iter_mut().for_each(|v| canonicalize_keys(v, ids)),
        _ => {}
    }
}

/// `events` with every key reference rewritten to its corpus-wide identity.
fn canonical_by_identity(events: &[String], ids: &KeyIds) -> Vec<String> {
    events
        .iter()
        .map(|e| match serde_json::from_str::<Json>(e) {
            Ok(mut v) => {
                canonicalize_keys(&mut v, ids);
                v.to_string()
            }
            Err(_) => e.clone(),
        })
        .collect()
}

/// Whether `head` and `prev` (one entry's canonical events) differ only as
/// `expected` allows. Keys are canonicalized through the corpus-wide
/// identity maps (`head_ids` / `prev_ids`), so a reference to a key minted in
/// an EARLIER entry resolves to its entity rather than an orphan token.
fn diverges_only_as(
    expected: &Expected,
    head: &[String],
    prev: &[String],
    head_ids: &KeyIds,
    prev_ids: &KeyIds,
) -> bool {
    let normalize =
        |events: &[String], drop: &[EventPattern], corpus_ids: &KeyIds| -> Option<Vec<String>> {
            // A structurally inconsistent mint (one key for two entities)
            // already failed closed in `extend_identities`.
            let ids = if expected.remint_keys {
                Some(corpus_ids)
            } else {
                None
            };
            let mut out = Vec::with_capacity(events.len());
            for e in events {
                let mut v: Json = serde_json::from_str(e).ok()?;
                if drop.iter().any(|p| p.matches(&v)) {
                    continue;
                }
                for o in &expected.field_overrides {
                    o.apply(&mut v);
                }
                if let Some(ids) = ids {
                    canonicalize_keys(&mut v, ids);
                }
                out.push(v.to_string());
            }
            if expected.reorder {
                out.sort();
            }
            Some(out)
        };
    match (
        normalize(head, &expected.only_in_head, head_ids),
        normalize(prev, &expected.only_in_prev, prev_ids),
    ) {
        (Some(h), Some(p)) => h == p,
        _ => false,
    }
}

// ---- Scenario-level reconciliation of ratified patterns (#1700) -----------
//
// A ratified `reorder` divergence may MOVE an event across entries: #1244
// activates a parallel join's element instance when the join FIRES, so its
// `ElementActivating`/`ElementActivated` leave the first arriving token's
// entry and appear in the firing entry. Per-entry `only_in_prev` /
// `only_in_head` matching cannot reconcile that — the events are absent from
// one entry on one side and present in a LATER entry on the other. The
// patterns therefore reconcile at SCENARIO level: over the whole scenario,
// the multiset of events matching `only_in_prev` on the prev side must equal
// the multiset matching `only_in_head` on the head side (each pattern removed
// from prev appears in head, same scenario, same element, same instance after
// the ratified remint), while everything else stays strict per entry. An
// unbalanced pattern — removed but never re-added, or the reverse — still
// FAILS: that is the guard against hiding a real loss.

/// The comparison identity of a matched event: its JSON with every key
/// reference canonicalized through `ids` when `remint` is declared (so a
/// re-minted join activation matches across sides), the variant tag rewritten
/// through `subs` (so a ratified lifecycle-kind substitution balances), raw
/// otherwise. Lifecycle kind (absent a ratified substitution), entity and
/// scope are all preserved.
fn pattern_projection(
    event: &Json,
    ids: &KeyIds,
    remint: bool,
    subs: &[VariantSubstitution],
) -> Json {
    let mut v = event.clone();
    if remint {
        canonicalize_keys(&mut v, ids);
    }
    // Rewrite the externally-tagged variant through the ratified
    // substitutions, mapping BOTH `prev` and `head` to `head` so a
    // substituted event's identity is its head form.
    if let Json::Object(map) = &mut v {
        if map.len() == 1 {
            let tag = map.keys().next().cloned().unwrap();
            if let Some(s) = subs.iter().find(|s| s.prev == tag || s.head == tag) {
                if let Some(body) = map.remove(&tag) {
                    map.insert(s.head.clone(), body);
                }
            }
        }
    }
    v
}

/// Adds every event of `events` matching one of `patterns` to the multiset
/// `out`, projected by [`pattern_projection`].
fn collect_matched(
    events: &[String],
    patterns: &[EventPattern],
    ids: &KeyIds,
    remint: bool,
    subs: &[VariantSubstitution],
    out: &mut BTreeMap<String, usize>,
) {
    for e in events {
        let Ok(v) = serde_json::from_str::<Json>(e) else {
            continue;
        };
        if patterns.iter().any(|p| p.matches(&v)) {
            *out.entry(pattern_projection(&v, ids, remint, subs).to_string())
                .or_insert(0) += 1;
        }
    }
}

/// The single-variant body of an externally tagged event.
fn variant_body(event: &Json) -> Option<(&str, &Json)> {
    let map = event.as_object()?;
    if map.len() != 1 {
        return None;
    }
    map.iter().next().map(|(k, v)| (k.as_str(), v))
}

/// The prev-side activations of PARKED joins (#1244), to be excepted from
/// the scenario balance. An entity qualifies only when prev carries EXACTLY
/// one `ElementActivating` and one `ElementActivated` for it, head carries
/// NO activation of it, and head opens it (`ParallelJoinOpened` with the
/// same canonical `element_instance_key`). Anything else is left in the
/// balance, so it fails there.
fn parked_join_activations(
    prev: &BTreeMap<String, usize>,
    head: &BTreeMap<String, usize>,
    head_all: &[Json],
) -> BTreeMap<String, usize> {
    let entity_of = |s: &str| -> Option<(String, String)> {
        let v: Json = serde_json::from_str(s).ok()?;
        let (kind, body) = variant_body(&v)?;
        if !matches!(kind, "ElementActivating" | "ElementActivated") {
            return None;
        }
        Some((
            kind.to_owned(),
            body.get("element_instance_key")?.to_string(),
        ))
    };
    let opened: BTreeSet<String> = head_all
        .iter()
        .filter_map(|v| match variant_body(v) {
            Some(("ParallelJoinOpened", b)) => b.get("element_instance_key").map(Json::to_string),
            _ => None,
        })
        .collect();
    let head_activated: BTreeSet<String> = head
        .keys()
        .filter_map(|s| entity_of(s).map(|(_, e)| e))
        .collect();
    // entity -> (kind -> [(event, count)])
    let mut by_entity: BTreeMap<String, Vec<(String, String, usize)>> = BTreeMap::new();
    for (s, n) in prev {
        if let Some((kind, e)) = entity_of(s) {
            by_entity.entry(e).or_default().push((kind, s.clone(), *n));
        }
    }
    let mut out = BTreeMap::new();
    for (entity, events) in by_entity {
        if !opened.contains(&entity) || head_activated.contains(&entity) {
            continue;
        }
        let count = |k: &str| {
            events
                .iter()
                .filter(|(kind, _, _)| kind == k)
                .map(|(_, _, n)| n)
                .sum::<usize>()
        };
        if events.len() == 2 && count("ElementActivating") == 1 && count("ElementActivated") == 1 {
            for (_, s, n) in events {
                out.insert(s, n);
            }
        }
    }
    out
}

/// One side's matched events and key identities, for
/// [`validate_unbalanced_surplus`].
struct MatchedSide<'a> {
    /// The canonicalized matched multiset (keys are identity strings).
    canonical: &'a BTreeMap<String, usize>,
    /// The same matched events RAW (decodable under the head `Event` type).
    raw: &'a BTreeMap<String, usize>,
    /// This side's corpus-wide key identities.
    ids: &'a KeyIds,
    /// Whether the declaration remints keys — the `canonical` multiset was
    /// projected with this setting, so the surplus lookup must be too.
    remint: bool,
    /// Every raw event of this side's divergent entries.
    entries: &'a [&'a String],
}

/// Validates the one-sided surplus an `unbalanced` declaration ratifies
/// (#1528 review): the events only one side emits (the asymmetric remainder
/// of the two matched multisets). The declaration says the change ADDS or
/// REMOVES events outright — it must not ratify a WRONG event. So:
///
/// * every surplus event must still decode under the head `Event` type (a
///   corrupted or truncated one fails); and
/// * regardless of snapshot format (when formats differ the durable-state
///   comparison is skipped, so this is the ONLY payload check), the surplus
///   must be internally consistent: a `UserTaskCreated` whose owning element instance
///   its own side never minted points at a wrong or missing child — not a
///   clean one-sided emission — and fails.
///
/// Counts are otherwise the declaration's own contract (it ratifies exactly
/// the imbalance it reviewed); this check guards the surplus's SHAPE, not its
/// size. The `raw` multisets carry each side's matched events RAW (the
/// `canonical` multisets hold identity strings in the key positions, which no
/// longer decode); the owner check resolves the raw `element_instance_key`
/// against the side's own mints.
fn validate_unbalanced_surplus(
    scenario: &str,
    head: MatchedSide<'_>,
    prev: MatchedSide<'_>,
) -> Result<(), String> {
    // The asymmetric remainder of a side's canonical multiset, as raw events:
    // the raw events whose canonical projection is in the surplus.
    let surplus_raw = |side: &MatchedSide, other: &MatchedSide| {
        let mut out: Vec<(String, usize)> = Vec::new();
        for (re, n) in side.raw {
            let v: Json = match serde_json::from_str(re) {
                Ok(v) => v,
                Err(_) => {
                    out.push((re.clone(), *n));
                    continue;
                }
            };
            let proj = pattern_projection(&v, side.ids, side.remint).to_string();
            let extra = side
                .canonical
                .get(&proj)
                .copied()
                .unwrap_or(0)
                .saturating_sub(other.canonical.get(&proj).copied().unwrap_or(0));
            if extra > 0 {
                out.push((re.clone(), extra));
            }
        }
        out
    };
    for (side_name, side, other) in [("head", &head, &prev), ("prev", &prev, &head)] {
        // The owners of this side's surplus `UserTaskCreated`, by element id.
        let mut task_owners: BTreeMap<String, BTreeMap<u64, usize>> = BTreeMap::new();
        for (event, extra) in surplus_raw(side, other) {
            // Decode under the head type: a corrupted surplus event fails.
            let decoded = nanobpmn_engine_core::decode_event_json(&event).map_err(|e| {
                format!(
                    "scenario {scenario}: a {side_name}-side surplus event does not decode under head: {e}: {event}"
                )
            })?;
            // The owning element instance of a one-sided `UserTaskCreated`
            // must be an entity its own side minted — otherwise the task
            // hangs off a wrong or missing child.
            let v = serde_json::to_value(&decoded).map_err(|e| e.to_string())?;
            if let Some(("UserTaskCreated", body)) = variant_body(&v) {
                if let Some(owner) = body.get("element_instance_key").and_then(Json::as_u64) {
                    if side.ids.get(&owner).is_none() {
                        return Err(format!(
                            "scenario {scenario}: a {side_name}-side surplus `UserTaskCreated` \
                             (x{extra}) names an owning element_instance_key {owner} its own \
                             side never minted — a wrong-child/wrong-owner event, not a clean \
                             one-sided emission: {event}"
                        ));
                    }
                    let element = body
                        .get("element_id")
                        .and_then(Json::as_str)
                        .unwrap_or("?")
                        .to_owned();
                    *task_owners
                        .entry(element)
                        .or_default()
                        .entry(owner)
                        .or_insert(0) += extra;
                }
            }
        }
        // One task per activated multi-instance child of that element: a
        // duplicate owner, a task on a non-child (another valid but wrong
        // element instance) or a child left without its task all fail
        // (#1528 review).
        let children = mi_children_by_element(side.entries);
        for (element, owners) in &task_owners {
            if let Some((owner, n)) = owners.iter().find(|(_, n)| **n > 1) {
                return Err(format!(
                    "scenario {scenario}: {n} {side_name}-side surplus `UserTaskCreated` for \
                     element {element} share the owner {owner} — one task per child"
                ));
            }
            let got: BTreeSet<u64> = owners.keys().copied().collect();
            let want = children.get(element).cloned().unwrap_or_default();
            if got != want {
                return Err(format!(
                    "scenario {scenario}: the {side_name}-side surplus `UserTaskCreated` for \
                     element {element} are owned by {got:?}, but the activated multi-instance \
                     children are {want:?} — one task per activated child"
                ));
            }
        }
    }
    Ok(())
}

/// The process instances `events` touch (every event body's `instance_key`).
fn instance_keys_of(events: &[&String]) -> BTreeSet<u64> {
    events
        .iter()
        .filter_map(|e| serde_json::from_str::<Json>(e).ok())
        .filter_map(|v| {
            variant_body(&v).and_then(|(_, body)| body.get("instance_key").and_then(Json::as_u64))
        })
        .collect()
}

/// The multi-instance children `events` activate, by element id: the child
/// keys of `MultiInstanceChildActivated`, labelled with the element id of
/// their own `ElementActivating`/`ElementActivated`.
fn mi_children_by_element(events: &[&String]) -> BTreeMap<String, BTreeSet<u64>> {
    let parsed: Vec<Json> = events
        .iter()
        .filter_map(|e| serde_json::from_str::<Json>(e).ok())
        .collect();
    let children: BTreeSet<u64> = parsed
        .iter()
        .filter_map(|v| match variant_body(v) {
            Some(("MultiInstanceChildActivated", b)) => b.get("child_key").and_then(Json::as_u64),
            _ => None,
        })
        .collect();
    let mut out: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
    for v in &parsed {
        if let Some(("ElementActivating" | "ElementActivated", b)) = variant_body(v) {
            if let (Some(k), Some(id)) = (
                b.get("element_instance_key").and_then(Json::as_u64),
                b.get("element_id").and_then(Json::as_str),
            ) {
                if children.contains(&k) {
                    out.entry(id.to_owned()).or_default().insert(k);
                }
            }
        }
    }
    out
}

/// A ratified behavioural change in one corpus scenario.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Divergence {
    pub scenario: String,
    pub expected: Expected,
    /// The first release tag with the new behaviour. Expires once the
    /// previous release is at or past it.
    pub introduced_in: String,
    pub issue: String,
    /// How a cluster may upgrade across it (required, no default).
    pub upgrade: UpgradeMode,
    /// Why the declared [`UpgradeMode`] applies; must name it.
    pub rationale: String,
}

impl Allowlist {
    /// Every entry that makes its release an `upgrade-boundary` (no rolling
    /// upgrade across it) — what the release notes must call out.
    pub fn upgrade_boundaries(&self) -> Vec<String> {
        self.observational_variants
            .iter()
            .filter(|o| o.upgrade == UpgradeMode::UpgradeBoundary)
            .map(|o| format!("observational variant {} ({})", o.variant, o.issue))
            .chain(
                self.divergences
                    .iter()
                    .filter(|d| d.upgrade == UpgradeMode::UpgradeBoundary)
                    .map(|d| format!("scenario {} divergence ({})", d.scenario, d.issue)),
            )
            .collect()
    }
}

/// Parses the allowlist file.
pub fn parse_allowlist(text: &str) -> Result<Allowlist, String> {
    serde_json::from_str(text).map_err(|e| format!("divergence allowlist: {e}"))
}

/// `vMAJOR.MINOR.PATCH` (leading `v` optional) as a comparable triple.
pub fn parse_release(tag: &str) -> Result<(u64, u64, u64), String> {
    let core = tag.trim_start_matches('v');
    let mut parts = core.split('.').map(str::parse::<u64>);
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(Ok(a)), Some(Ok(b)), Some(Ok(c)), None) => Ok((a, b, c)),
        _ => Err(format!("not a release tag: {tag:?}")),
    }
}

/// Inputs of the previous-release comparison beyond the two record files.
pub struct CrossReleaseContext<'a> {
    /// The scenario label of each compared corpus entry (by entry order).
    pub scenarios: &'a [String],
    pub allowlist: &'a Allowlist,
    /// The previous release's tag (allowlist expiry).
    pub prev_tag: &'a str,
    /// `vocabulary(golden @ HEAD) − vocabulary(golden @ prev tag)`.
    pub added: &'a Vocab,
}

/// Fails on any allowlist entry that has expired (the previous release is at
/// or past its introducing release, so both sides of the gate run the new
/// behaviour) or is malformed.
fn check_expiry(ctx: &CrossReleaseContext<'_>) -> Result<(), String> {
    let prev = parse_release(ctx.prev_tag)?;
    let entries = ctx
        .allowlist
        .observational_variants
        .iter()
        .map(|o| {
            (
                format!("observational variant {}", o.variant),
                &o.introduced_in,
                o.upgrade,
                &o.rationale,
            )
        })
        .chain(ctx.allowlist.divergences.iter().map(|d| {
            (
                format!("scenario {} divergence", d.scenario),
                &d.introduced_in,
                d.upgrade,
                &d.rationale,
            )
        }));
    for (what, introduced, upgrade, rationale) in entries {
        if rationale.trim().is_empty() {
            return Err(format!("allowlist {what} has no upgrade rationale"));
        }
        if !rationale.contains(upgrade.as_str()) {
            return Err(format!(
                "allowlist {what} declares upgrade `{}` but its rationale does not say so — \
                 the rationale must state which upgrade mode applies and why",
                upgrade.as_str()
            ));
        }
        if parse_release(introduced)? <= prev {
            return Err(format!(
                "STALE allowlist entry: {what} was introduced in {introduced}, and the previous \
                 release {} is at or past it — remove the entry",
                ctx.prev_tag
            ));
        }
    }
    for o in &ctx.allowlist.observational_variants {
        if !ctx.added.contains(&o.variant) {
            return Err(format!(
                "allowlist observational variant {} is not recorded by the golden serde corpus \
                 as added since {} — only a golden-recorded additive variant can be ratified",
                o.variant, ctx.prev_tag
            ));
        }
    }
    Ok(())
}

fn event_tag(event: &Json) -> Option<&str> {
    match event {
        Json::Object(map) if map.len() == 1 => map.keys().next().map(String::as_str),
        Json::String(s) => Some(s),
        _ => None,
    }
}

/// Erases `erasable` fields. A field token is variant-qualified
/// (`Variant.field`) for the externally-tagged `Command`/`Event` shapes, or
/// `$`-qualified (`$.field`) for the untagged `EngineSnapshot`/`State`
/// structs, whose fields keep the root context. Erasure therefore happens at
/// every context — the root included, but only for a token explicitly
/// listing that `$`-qualified field (an event token never starts with `$`,
/// so root erasure can never touch an event field, and a `$` token never
/// names a variant-qualified one) — and propagates through a snake_case
/// struct field (serde flattens nested structs, e.g. `multi_instance`'s
/// fields stay erasable under the variant).
/// A `BTreeMap<String, Value>` of variables/metadata is a DATA boundary
/// ([`is_value_map`]): its keys are variable names, never struct fields, so
/// erasure never removes one — a variable the previous stream omits stays a
/// divergence even when the golden records a same-named field as added.
pub fn erase_fields(value: &mut Json, ctx: &str, erasable: &Vocab) {
    match value {
        Json::Object(map) => {
            if is_value_map(map) {
                return; // variable/metadata map: keys are data, never fields
            }
            map.retain(|k, _| !is_field_name(k) || !erasable.contains(&format!("{ctx}.{k}")));
            for (k, v) in map.iter_mut() {
                // A variant tag becomes the context for everything under it —
                // including the VALUES of a map field, whose entries are
                // structs of that variant's shape (a `ProcessDeployed`
                // process's `elements` map holds element structs whose fields
                // are `ProcessDeployed.*`). Passing the stale outer `ctx`
                // (`$`) instead would leave a golden-added field under a map
                // unerased — the divergence the previous-release gate then
                // reports on every deploy (#1528 review).
                let next = if is_variant_tag(k) { k.as_str() } else { ctx };
                if is_field_name(k) && data_map_fields().contains(k) {
                    // A map field: its keys are DATA, never
                    // erased; only its values (which may be structs) are
                    // descended into — under the CURRENT context (`next`).
                    if let Json::Object(entries) = v {
                        if !is_value_map(entries) {
                            entries
                                .values_mut()
                                .for_each(|e| erase_fields(e, next, erasable));
                        }
                    }
                    continue;
                }
                erase_fields(v, next, erasable);
            }
        }
        Json::Array(items) => items
            .iter_mut()
            .for_each(|v| erase_fields(v, ctx, erasable)),
        _ => {}
    }
}

/// An entry's events in cross-release canonical form, one string per event:
/// re-decoded through the head `Event` type and with `erasable` fields
/// removed. `None` for a rejection.
fn canonical_events(record: &Json, erasable: &Vocab) -> Result<Option<Vec<String>>, String> {
    if outcome(record)? == "err" {
        return Ok(None);
    }
    let events = record
        .get("events")
        .and_then(Json::as_array)
        .ok_or_else(|| format!("malformed record {record}"))?;
    let mut out = Vec::with_capacity(events.len());
    for e in events {
        let event = nanobpmn_engine_core::decode_event_json(&e.to_string())
            .map_err(|err| format!("event does not decode under head: {err}: {e}"))?;
        let mut v = serde_json::to_value(&event).map_err(|err| err.to_string())?;
        erase_fields(&mut v, "$", erasable);
        out.push(v.to_string());
    }
    Ok(Some(out))
}

/// What [`cross_release_checked`] compared, beyond [`CrossReleaseReport`].
#[derive(Debug, PartialEq, Eq)]
pub struct CheckedReport {
    pub base: CrossReleaseReport,
    /// Golden-added fields the previous release never emits, erased from both
    /// sides (an additive `#[serde(default)]` field).
    pub erased_fields: Vec<String>,
    /// Head events filtered as ratified observational variants.
    pub observational_filtered: usize,
    /// Entries that diverged exactly as their scenario's allowlist declares.
    pub ratified_divergent_entries: usize,
}

/// Head vs previous-release records: golden-recorded additive
/// fields the old build never emits are erased, ratified observational
/// variants are filtered from the head stream, an undeclared new variant
/// fails, and a divergence passes only in an allowlisted scenario and only
/// as the declared kind. Every allowlist entry must be live (unexpired) and
/// used, or it is stale and fails.
pub fn cross_release_checked(
    head: &[Json],
    prev: &[Json],
    ctx: &CrossReleaseContext<'_>,
) -> Result<CheckedReport, String> {
    check_expiry(ctx)?;
    let head_entries: Vec<&Json> = head.iter().filter(|r| !is_state(r)).collect();
    let prev_entries: Vec<&Json> = prev.iter().filter(|r| !is_state(r)).collect();
    if head_entries.len() != prev_entries.len() {
        return Err(format!(
            "record counts differ: head {} vs prev {}",
            head_entries.len(),
            prev_entries.len()
        ));
    }
    if ctx.scenarios.len() != head_entries.len() {
        return Err(format!(
            "{} scenario labels for {} compared entries",
            ctx.scenarios.len(),
            head_entries.len()
        ));
    }
    // Shapes the previous release actually emits anywhere in this corpus.
    let mut prev_emitted = Vocab::new();
    for r in &prev_entries {
        if let Some(events) = r.get("events") {
            shape_tokens(events, &mut prev_emitted);
        }
    }
    // Variants a ratified divergence declares in `only_in_head` keep their
    // fields: erasure is for an additive field on a variant BOTH builds emit,
    // but a variant only head emits would erase to an empty object — and the
    // `EventPattern` match (by variant + optional `element_id`) in the
    // divergence reconciliation needs the fields intact.
    let only_in_head_variants: BTreeSet<&str> = ctx
        .allowlist
        .divergences
        .iter()
        .flat_map(|d| d.expected.only_in_head.iter().map(|p| p.variant.as_str()))
        .collect();
    let erasable: Vocab = ctx
        .added
        .iter()
        .filter(|t| {
            t.contains('.')
                && !prev_emitted.contains(*t)
                && t.split_once('.')
                    .is_none_or(|(variant, _)| !only_in_head_variants.contains(variant))
        })
        .cloned()
        .collect();
    let observational: BTreeSet<&str> = ctx
        .allowlist
        .observational_variants
        .iter()
        .map(|o| o.variant.as_str())
        .collect();
    let mut observed: BTreeSet<String> = BTreeSet::new();
    let (mut filtered, mut ratified_entries) = (0, 0);
    let (mut head_ids, mut prev_ids) = (KeyIds::new(), KeyIds::new());
    // Per-scenario accumulator for the scenario-level reconciliation: the
    // canonical events of every divergent entry, plus the scenario's ratified
    // divergence. Reconciled after the loop, once both sides' corpus-wide key
    // identities are complete (an event a ratified pattern moves ACROSS
    // entries reconciles against the whole scenario, not its own entry).
    struct Pending<'a> {
        divergence: &'a Divergence,
        /// `(entry index, head events, prev events)` of each divergent entry.
        entries: Vec<PendingEntry>,
    }
    type PendingEntry = (usize, Option<Vec<String>>, Option<Vec<String>>);
    let mut pending: std::collections::BTreeMap<&str, Pending> = Default::default();
    for (i, (h, p)) in head_entries.iter().zip(&prev_entries).enumerate() {
        let scenario = ctx.scenarios[i].as_str();
        // Ratified observational variants leave the head stream; any OTHER
        // variant the old build never emits but the golden records as added
        // is an undeclared new variant.
        let mut h = (*h).clone();
        if let Some(Json::Array(events)) = h.get_mut("events") {
            events.retain(|e| match event_tag(e) {
                Some(tag) if observational.contains(tag) => {
                    observed.insert(tag.to_owned());
                    filtered += 1;
                    false
                }
                _ => true,
            });
            // A variant a ratified divergence of THIS scenario declares in
            // `only_in_head` is declared, not undeclared: the scenario-level
            // reconciliation below is exactly where a behavioural change that
            // REPLACES one event with another (#1535 terminates an
            // interrupted activity where the previous release completed it)
            // is ratified. Filtering such a variant observationally would
            // drop it from EVERY scenario's stream, including ones whose
            // comparison must carry it.
            let declared_here: BTreeSet<&str> = ctx
                .allowlist
                .divergences
                .iter()
                .filter(|d| d.scenario == scenario)
                .flat_map(|d| d.expected.only_in_head.iter().map(|p| p.variant.as_str()))
                .collect();
            for e in events.iter() {
                if let Some(tag) = event_tag(e) {
                    if ctx.added.contains(tag)
                        && !prev_emitted.contains(tag)
                        && !declared_here.contains(tag)
                    {
                        return Err(format!(
                            "entry {i} (scenario {scenario}): UNDECLARED new event variant `{tag}` \
                             (golden-recorded as added since {}, never emitted by it) — ratify it \
                             in divergence-allowlist.json `observational_variants` with a \
                             rolling-upgrade rationale, or fix the change",
                            ctx.prev_tag
                        ));
                    }
                }
            }
        }
        let (hc, pc) = (
            canonical_events(&h, &erasable)?,
            canonical_events(p, &erasable)?,
        );
        for (side, events, ids) in [("head", &hc, &mut head_ids), ("prev", &pc, &mut prev_ids)] {
            if let Some(events) = events {
                extend_identities(events, ids)
                    .map_err(|e| format!("entry {i} (scenario {scenario}), {side}: {e}"))?;
            }
        }
        // Equal raw events are equal only if every key also names the SAME
        // entity on both sides (a ratified remint earlier in the corpus can
        // make the same raw key mean different entities).
        let same_identity = |a: &Option<Vec<String>>, b: &Option<Vec<String>>| match (a, b) {
            (Some(a), Some(b)) => {
                canonical_by_identity(a, &head_ids) == canonical_by_identity(b, &prev_ids)
            }
            (None, None) => true,
            _ => false,
        };
        if hc == pc && same_identity(&hc, &pc) {
            continue;
        }
        let declared = ctx
            .allowlist
            .divergences
            .iter()
            .find(|d| d.scenario == scenario);
        if let (Some(d), Some(_), Some(_)) = (declared, &hc, &pc) {
            // A divergent entry of an allowlisted scenario reconciles after
            // the loop, at scenario level: a ratified `reorder` divergence
            // may move an event ACROSS entries (#1244 fires a parallel join's
            // activation in a later entry than the first token's), which no
            // per-entry normalization can see.
            let entry = pending.entry(scenario).or_insert(Pending {
                divergence: d,
                entries: Vec::new(),
            });
            entry.entries.push((i, hc, pc));
            continue;
        }
        let render = |c: &Option<Vec<String>>| c.as_ref().map_or("err".into(), |v| v.join("\n"));
        let why = match declared {
            Some(d) => format!(
                "an UNDECLARED extra divergence in allowlisted scenario {scenario} (ratified \
                 only as {:?}, {}) — the declared normalizations do not reconcile it",
                d.expected, d.issue
            ),
            None => format!("scenario {scenario}"),
        };
        return Err(format!(
            "entry {i}, {why}:\n  head: {}\n  prev: {}",
            render(&hc),
            render(&pc)
        ));
    }
    // Reconcile each scenario's divergent entries. Per entry the declared
    // normalizations apply STRICTLY (an undeclared extra difference in one
    // entry fails, wherever it sits); the `only_in_prev` / `only_in_head`
    // patterns then reconcile at SCENARIO level. A ratified `reorder`
    // divergence may MOVE an event across entries (#1244 fires a parallel
    // join's activation in a later entry than the first token's) and may
    // change WHICH event mints the entity (a parked join mints its key at
    // `ParallelJoinOpened`, where the previous release minted it at the
    // visible `ElementActivating`). So the matched events of each side are
    // balanced as a MULTISET of full canonical events (lifecycle kind,
    // entity and scope preserved, occurrences counted); the parked-join
    // shape is the only exception and is validated exactly, per entity. A
    // pattern removed but never re-added (a real loss) still fails.
    let mut ratified_scenarios: BTreeSet<&str> = BTreeSet::new();
    let mut boundary = RatifiedBoundary::default();
    for (scenario, pend) in &pending {
        let d = pend.divergence;
        let expected = &d.expected;
        let remint = expected.remint_keys;
        // Accumulate, over the scenario's divergent entries, each side's
        // declared-pattern events as a MULTISET of full canonical events:
        // lifecycle kind, canonical entity identity and scope all preserved,
        // and occurrences counted — so a deleted, duplicated or re-scoped
        // lifecycle event never balances.
        let mut head_matched: BTreeMap<String, usize> = BTreeMap::new();
        let mut prev_matched: BTreeMap<String, usize> = BTreeMap::new();
        // The same matched events RAW (pre-canonicalization), so the
        // `unbalanced` surplus validation can decode them under the head
        // `Event` type (the canonical multisets hold identity strings in the
        // key positions, which no longer decode).
        let mut head_matched_raw: BTreeMap<String, usize> = BTreeMap::new();
        let mut prev_matched_raw: BTreeMap<String, usize> = BTreeMap::new();
        // Every head event of the divergent entries, canonical — the parked
        // join exception below looks up head's `ParallelJoinOpened` here.
        let mut head_all: Vec<Json> = Vec::new();
        // Every RAW event of the divergent entries, per side — the surplus
        // owner check derives the activated user-task children from these.
        let (mut head_entry_events, mut prev_entry_events): (Vec<&String>, Vec<&String>) =
            (Vec::new(), Vec::new());
        for (i, hc, pc) in &pend.entries {
            let (Some(hs), Some(ps)) = (hc, pc) else {
                return Err(format!(
                    "entry {i} (scenario {scenario}): a divergent entry without events on \
                     both sides cannot reconcile"
                ));
            };
            if !diverges_only_as(expected, hs, ps, &head_ids, &prev_ids) {
                let render = |v: &[String]| v.join("\n");
                return Err(format!(
                    "entry {i}, an UNDECLARED extra divergence in allowlisted scenario \
                     {scenario} (ratified only as {expected:?}, {}) — the declared \
                     normalizations do not reconcile it:\n  head: {}\n  prev: {}",
                    d.issue,
                    render(hs),
                    render(ps)
                ));
            }
            head_entry_events.extend(hs.iter());
            prev_entry_events.extend(ps.iter());
            for e in hs {
                if let Ok(v) = serde_json::from_str::<Json>(e) {
                    head_all.push(pattern_projection(
                        &v,
                        &head_ids,
                        remint,
                        &expected.variant_substitutions,
                    ));
                }
            }
            collect_matched(
                hs,
                &expected.only_in_head,
                &head_ids,
                remint,
                &expected.variant_substitutions,
                &mut head_matched,
            );
            collect_matched(
                ps,
                &expected.only_in_prev,
                &prev_ids,
                remint,
                &expected.variant_substitutions,
                &mut prev_matched,
            );
            // The raw counterparts, for the `unbalanced` surplus validation.
            collect_matched(
                hs,
                &expected.only_in_head,
                &head_ids,
                false,
                &mut head_matched_raw,
            );
            collect_matched(
                ps,
                &expected.only_in_prev,
                &prev_ids,
                false,
                &mut prev_matched_raw,
            );
        }
        // The one ratified exception (#1244): a PARKED join. The previous
        // release activated it visibly (exactly one `ElementActivating` +
        // one `ElementActivated`); head mints the same entity only by
        // `ParallelJoinOpened` and never activates it. Validate that shape
        // exactly, per entity, and only then drop prev's pair.
        let parked = parked_join_activations(&prev_matched, &head_matched, &head_all);
        for (event, n) in &parked {
            if let Some(c) = prev_matched.get_mut(event) {
                *c -= n;
                if *c == 0 {
                    prev_matched.remove(event);
                }
            }
        }
        // An `unbalanced` marker is validated whether or not the matched
        // events balance (#1528 review): an untraceable marker fails even on
        // a reorder-only scenario, never silently enabling the boundary
        // state projection below.
        if let Some(u) = &expected.unbalanced {
            if !u.valid(&d.issue) {
                return Err(format!(
                    "scenario {scenario}: an unbalanced divergence declaration is valid \
                     only with a non-empty reason AND a named issue ({}) — the \
                     declaration must be traceable to a reviewed decision",
                    d.issue
                ));
            }
        }
        let unbalanced_surplus = head_matched != prev_matched;
        if unbalanced_surplus {
            // An UNBALANCED declaration (#1528) ratifies the mismatch — the
            // change adds or removes events outright, so the declared
            // patterns can never reconcile as a move — but only when it is
            // traceable: a non-empty reason on the declaration and the
            // divergence's issue named. Anything else fails closed.
            match &expected.unbalanced {
                Some(_) => {
                    // The declaration ratifies that the two sides' matched
                    // events have NO counterpart on the other build — but it
                    // must not ratify a WRONG event. Validate the one-sided
                    // surplus (the events only one side emits) rather than
                    // accepting it blindly (#1528 review): every surplus event
                    // must still decode under the head `Event` type (a
                    // corrupted one fails), and — whatever the snapshot
                    // formats — the surplus must be internally consistent (a `UserTaskCreated`
                    // whose owning `element_instance_key` its own side never
                    // minted is a wrong-child/wrong-owner event, not a clean
                    // one-sided emission).
                    validate_unbalanced_surplus(
                        scenario,
                        MatchedSide {
                            canonical: &head_matched,
                            raw: &head_matched_raw,
                            ids: &head_ids,
                            remint,
                            entries: &head_entry_events,
                        },
                        MatchedSide {
                            canonical: &prev_matched,
                            raw: &prev_matched_raw,
                            ids: &prev_ids,
                            remint,
                            entries: &prev_entry_events,
                        },
                    )?;
                }
                None => {
                    let show = |m: &BTreeMap<String, usize>| {
                        m.iter()
                            .map(|(k, n)| format!("{n} x {k}"))
                            .collect::<Vec<_>>()
                            .join("\n")
                    };
                    return Err(format!(
                        "scenario {scenario}: unbalanced ratified only_in_prev/only_in_head patterns \
                         ({}, {:?}) — an event removed on one side never re-appears on the other \
                         (a real loss, not a move):\n  head-side patterns matched:\n{}\n  \
                         prev-side patterns matched (parked joins excepted):\n{}",
                        d.issue,
                        expected,
                        show(&head_matched),
                        show(&prev_matched)
                    ));
                }
            }
        }
        ratified_entries += pend.entries.len();
        ratified_scenarios.insert(scenario);
        // Only an ACCEPTED one-sided surplus (an unbalanced divergence that
        // really is unbalanced, and whose surplus validated above) scopes the
        // same-format boundary state projection.
        if unbalanced_surplus && d.upgrade == UpgradeMode::UpgradeBoundary {
            let first = pend
                .entries
                .iter()
                .filter_map(|(i, _, _)| head_entries[*i].get("i").and_then(Json::as_u64))
                .min();
            boundary.from = match (boundary.from, first) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            // Exclude the ratified instances by IDENTITY (the union over
            // both sides), so an instance only one side's divergent events
            // name is excluded on both, at its own raw key.
            let identities: BTreeSet<String> = instance_keys_of(&head_entry_events)
                .into_iter()
                .map(|k| head_ids.owner(k))
                .chain(
                    instance_keys_of(&prev_entry_events)
                        .into_iter()
                        .map(|k| prev_ids.owner(k)),
                )
                .collect();
            for (ids, out) in [
                (&head_ids, &mut boundary.head_instances),
                (&prev_ids, &mut boundary.prev_instances),
            ] {
                out.extend(
                    ids.ids
                        .iter()
                        .filter(|(_, id)| identities.contains(*id))
                        .map(|(k, _)| *k),
                );
                // A key never minted on either side (`?orphan:<raw>`) is
                // excluded at its raw value on both.
                out.extend(identities.iter().filter_map(|id| {
                    id.strip_prefix("?orphan:")
                        .and_then(|r| r.parse::<u64>().ok())
                }));
            }
        }
    }
    for o in &ctx.allowlist.observational_variants {
        if !observed.contains(&o.variant) {
            return Err(format!(
                "STALE allowlist entry: observational variant {} ({}) never appears in the \
                 head stream — remove it",
                o.variant, o.issue
            ));
        }
    }
    for d in &ctx.allowlist.divergences {
        if !ratified_scenarios.contains(d.scenario.as_str()) {
            return Err(format!(
                "STALE allowlist entry: scenario {} no longer diverges from {} — remove it",
                d.scenario, ctx.prev_tag
            ));
        }
    }
    // State checkpoints: golden-added fields the old build never persists
    // are erased, exactly as for events.
    let head_states: Vec<&Json> = head.iter().filter(|r| is_state(r)).collect();
    let prev_states: Vec<&Json> = prev.iter().filter(|r| is_state(r)).collect();
    let mut prev_state_shapes = Vocab::new();
    for r in &prev_states {
        if let Some(state) = r.get("state") {
            shape_tokens(state, &mut prev_state_shapes);
        }
    }
    let state_erasable: Vocab = ctx
        .added
        .iter()
        .filter(|t| t.contains('.') && !prev_state_shapes.contains(*t))
        .cloned()
        .collect();
    let base = compare_states(
        &head_states,
        &prev_states,
        prev_entries.len(),
        &state_erasable,
        &boundary,
    )?;
    // Report the fields actually erased (present on the head side of their
    // own stream), not the whole candidate set.
    let (mut head_event_shapes, mut head_state_shapes) = (Vocab::new(), Vocab::new());
    for r in head.iter() {
        if let Some(v) = r.get("events") {
            shape_tokens(v, &mut head_event_shapes);
        }
        if let Some(v) = r.get("state") {
            shape_tokens(v, &mut head_state_shapes);
        }
    }
    let erased_fields = erasable
        .intersection(&head_event_shapes)
        .chain(state_erasable.intersection(&head_state_shapes))
        .cloned()
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();
    Ok(CheckedReport {
        base,
        erased_fields,
        observational_filtered: filtered,
        ratified_divergent_entries: ratified_entries,
    })
}
