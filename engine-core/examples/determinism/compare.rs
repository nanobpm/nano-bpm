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

use std::collections::BTreeSet;

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

/// Compares state checkpoints (through the head `EngineSnapshot` type) when
/// both builds share a `SNAPSHOT_FORMAT_VERSION`, erasing `erasable` fields.
fn compare_states(
    head_states: &[&Json],
    prev_states: &[&Json],
    entries: usize,
    erasable: &Vocab,
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
    let (mut compared, mut skipped) = (0, 0);
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
        if canonical_state(h, "head", erasable)? != canonical_state(p, "prev", erasable)? {
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
    })
}

/// What the state/entry comparison covered.
#[derive(Debug, PartialEq, Eq)]
pub struct CrossReleaseReport {
    pub entries: usize,
    pub states_compared: usize,
    pub states_skipped_format_bump: usize,
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
/// identical, so any undeclared extra divergence still fails.
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
    /// Events only the previous release emits (dropped from its side).
    #[serde(default)]
    pub only_in_prev: Vec<EventPattern>,
    /// Events only the head build emits (dropped from its side).
    #[serde(default)]
    pub only_in_head: Vec<EventPattern>,
}

/// Matches an event by variant and, optionally, its `element_id`.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventPattern {
    pub variant: String,
    #[serde(default)]
    pub element_id: Option<String>,
}

impl EventPattern {
    fn matches(&self, event: &Json) -> bool {
        let Some(body) = event.get(&self.variant) else {
            return false;
        };
        self.element_id
            .as_deref()
            .is_none_or(|id| body.get("element_id").and_then(Json::as_str) == Some(id))
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
/// * `JobCreated` / `ExecutionListenerJobCreated` / `TaskListenerJobCreated`
///   → `job:<owning element identity>#n`;
/// * `MessageSubscriptionCreated`/`SignalSubscriptionCreated`
///   → `subscription:<owning element identity>#n`.
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
}

/// Folds `events`' minted keys into `ids` (one side, in corpus order).
fn extend_identities(events: &[String], ids: &mut KeyIds) -> Result<(), String> {
    for e in events {
        let Ok(v) = serde_json::from_str::<Json>(e) else {
            continue;
        };
        // Every event is a single-key externally-tagged object; a bare
        // unit-variant string carries no keys to canonicalize.
        let Some((variant, body)) = v.as_object().and_then(|o| o.iter().next()) else {
            continue;
        };
        let u64_of = |f: &str| body.get(f).and_then(Json::as_u64);
        let str_of = |f: &str| body.get(f).and_then(Json::as_str);
        let minted = match variant.as_str() {
            "ElementActivating" | "ElementActivated" => {
                u64_of("element_instance_key").zip(str_of("element_id").map(|id| {
                    let instance =
                        u64_of("instance_key").map_or_else(|| "?".to_owned(), |k| k.to_string());
                    format!("element:{instance}:{id}")
                }))
            }
            "JobCreated" | "ExecutionListenerJobCreated" | "TaskListenerJobCreated" => {
                u64_of("job_key").map(|k| {
                    let owner = u64_of("element_instance_key")
                        .map_or_else(|| "?".to_owned(), |o| ids.owner(o));
                    (k, format!("job:{variant}:{owner}"))
                })
            }
            "MessageSubscriptionCreated" | "SignalSubscriptionCreated" => {
                u64_of("subscription_key").map(|k| {
                    let owner = u64_of("element_instance_key")
                        .map_or_else(|| "?".to_owned(), |o| ids.owner(o));
                    (k, format!("subscription:{owner}"))
                })
            }
            _ => None,
        };
        if let Some((k, base)) = minted {
            ids.mint(k, base)?;
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
                if is_field_name(k) && data_map_fields().contains(k) {
                    // A map field: its keys are DATA, never
                    // erased; only its values (which may be structs) are
                    // descended into.
                    if let Json::Object(entries) = v {
                        if !is_value_map(entries) {
                            entries
                                .values_mut()
                                .for_each(|e| erase_fields(e, ctx, erasable));
                        }
                    }
                    continue;
                }
                let next = if is_variant_tag(k) {
                    // An externally-tagged variant: it becomes the context.
                    k.as_str()
                } else {
                    ctx
                };
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
    let erasable: Vocab = ctx
        .added
        .iter()
        .filter(|t| t.contains('.') && !prev_emitted.contains(*t))
        .cloned()
        .collect();
    let observational: BTreeSet<&str> = ctx
        .allowlist
        .observational_variants
        .iter()
        .map(|o| o.variant.as_str())
        .collect();
    let mut observed: BTreeSet<String> = BTreeSet::new();
    let mut ratified_scenarios: BTreeSet<&str> = BTreeSet::new();
    let (mut filtered, mut ratified_entries) = (0, 0);
    let (mut head_ids, mut prev_ids) = (KeyIds::new(), KeyIds::new());
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
            for e in events.iter() {
                if let Some(tag) = event_tag(e) {
                    if ctx.added.contains(tag) && !prev_emitted.contains(tag) {
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
        let explained = match (declared, &hc, &pc) {
            (Some(d), Some(hs), Some(ps)) => {
                diverges_only_as(&d.expected, hs, ps, &head_ids, &prev_ids)
            }
            _ => false,
        };
        if explained {
            ratified_scenarios.insert(scenario);
            ratified_entries += 1;
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
