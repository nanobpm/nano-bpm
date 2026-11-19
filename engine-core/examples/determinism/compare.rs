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
            for (k, v) in map {
                if is_variant_tag(k) {
                    out.insert(k.clone());
                    walk_tokens(v, k, out);
                } else {
                    if is_field_name(k) {
                        out.insert(format!("{ctx}.{k}"));
                    }
                    // Propagate the context through a snake_case struct field
                    // (serde flattens nested structs, e.g. `multi_instance`'s
                    // fields stay under the variant). Map DATA keys — a
                    // variable name like `receipt`, a camelCase key — are not
                    // `is_field_name`, so they contribute no token; only a
                    // snake_case map key is indistinguishable from a field,
                    // and erasure guards those (see [`erase_fields`]).
                    walk_tokens(v, ctx, out);
                }
            }
        }
        Json::Array(items) => items.iter().for_each(|v| walk_tokens(v, ctx, out)),
        Json::String(s) if ctx == "$" && is_variant_tag(s) => {
            // A unit variant serializes as a bare string (`"Ping"`).
            out.insert(s.clone());
        }
        _ => {}
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

/// A ratified new event variant.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationalVariant {
    pub variant: String,
    /// The first release tag that emits it. Expires once the previous
    /// release is at or past it (both builds emit it; nothing to declare).
    pub introduced_in: String,
    pub issue: String,
    /// Why a mixed old/new cluster stays correct while only new replicas
    /// emit it.
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
    /// the footprint of minting in a different order (#1244 fork order).
    /// Normalization zeroes the keys but keeps each event's structure
    /// (`element_id`, kind, …), so a reference inconsistent with its entity's
    /// creation event still differs; and an entry whose minting is not a
    /// clean one-key-per-element (two keys for one element, or one key for
    /// two) fails closed rather than reconcile.
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

/// Zeroes every `key`/`*_key` field of `value`, preserving the event's
/// structure (its `element_id`, its other fields). Two events that reference
/// the same entity zero to the same shape only when their OTHER fields
/// (element_id, kind, …) also agree — so a reference inconsistent with the
/// entity's creation event still differs.
fn mask_keys(value: &mut Json) {
    match value {
        Json::Object(map) => {
            for (k, v) in map.iter_mut() {
                if (k == "key" || k.ends_with("_key")) && v.is_u64() {
                    *v = Json::from(0);
                } else {
                    mask_keys(v);
                }
            }
        }
        Json::Array(items) => items.iter_mut().for_each(mask_keys),
        _ => {}
    }
}

/// Fails (returns `false`) when an entry's key usage is structurally
/// inconsistent — the failure mode zeroing would hide: two *distinct*
/// element-instance keys minted for the same `element_id` by the entry's
/// creation events (`ElementActivating`/`ElementActivated`), or the same key
/// minted for two different `element_id`s (a collision). Both mean the
/// entry's key↔entity relationship is not a clean per-entity remint, so the
/// declared `remint_keys` normalization must not reconcile it. Returns
/// `true` when the entry's minting is one-key-per-element.
fn consistent_mint(events: &[String]) -> bool {
    let mut by_element: std::collections::BTreeMap<String, u64> = Default::default();
    let mut by_key: std::collections::BTreeMap<u64, String> = Default::default();
    for e in events {
        let Ok(v) = serde_json::from_str::<Json>(e) else {
            return false;
        };
        let body = v
            .get("ElementActivating")
            .or_else(|| v.get("ElementActivated"));
        let Some(body) = body else { continue };
        let (Some(id), Some(key)) = (
            body.get("element_id").and_then(Json::as_str),
            body.get("element_instance_key").and_then(Json::as_u64),
        ) else {
            continue;
        };
        if let Some(prev_key) = by_element.insert(id.to_owned(), key) {
            if prev_key != key {
                return false; // one element minted under two distinct keys
            }
        }
        if let Some(prev_id) = by_key.insert(key, id.to_owned()) {
            if prev_id != id {
                return false; // one key minted for two elements (collision)
            }
        }
    }
    true
}

/// Whether `head` and `prev` (one entry's canonical events) differ only as
/// `expected` allows.
fn diverges_only_as(expected: &Expected, head: &[String], prev: &[String]) -> bool {
    let normalize = |events: &[String], drop: &[EventPattern]| -> Option<Vec<String>> {
        // Fail closed: a structurally inconsistent mint is not the declared
        // remint, whatever the allowlist says.
        if expected.remint_keys && !consistent_mint(events) {
            return None;
        }
        let mut out = Vec::with_capacity(events.len());
        for e in events {
            let mut v: Json = serde_json::from_str(e).ok()?;
            if drop.iter().any(|p| p.matches(&v)) {
                continue;
            }
            if expected.remint_keys {
                mask_keys(&mut v);
            }
            out.push(v.to_string());
        }
        if expected.reorder {
            out.sort();
        }
        Some(out)
    };
    match (
        normalize(head, &expected.only_in_head),
        normalize(prev, &expected.only_in_prev),
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
    /// Why a rolling upgrade (old and new replicas side by side) is safe.
    pub rationale: String,
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
                &o.rationale,
            )
        })
        .chain(ctx.allowlist.divergences.iter().map(|d| {
            (
                format!("scenario {} divergence", d.scenario),
                &d.introduced_in,
                &d.rationale,
            )
        }));
    for (what, introduced, rationale) in entries {
        if rationale.trim().is_empty() {
            return Err(format!("allowlist {what} has no rolling-upgrade rationale"));
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

/// Erases `erasable` fields. A field token is always variant-qualified
/// (`Variant.field`), and a snake_case key is only a struct FIELD under an
/// externally-tagged variant — so erasure happens at every non-root context
/// and propagates through a snake_case struct field (serde flattens nested
/// structs, e.g. `multi_instance`'s fields stay erasable under the variant).
/// A map's data keys are only erased if the (mis-)recorded token names them,
/// which the golden vocabulary avoids for non-`is_field_name` data (a
/// variable named `receipt`); a snake_case variable name is
/// indistinguishable from a struct field under serde flattening and is the
/// documented residual risk of a value-only shape vocabulary.
fn erase_fields(value: &mut Json, ctx: &str, erasable: &Vocab) {
    match value {
        Json::Object(map) => {
            if ctx != "$" {
                map.retain(|k, _| !is_field_name(k) || !erasable.contains(&format!("{ctx}.{k}")));
            }
            for (k, v) in map.iter_mut() {
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
        if hc == pc {
            continue;
        }
        let declared = ctx
            .allowlist
            .divergences
            .iter()
            .find(|d| d.scenario == scenario);
        let explained = match (declared, &hc, &pc) {
            (Some(d), Some(hs), Some(ps)) => diverges_only_as(&d.expected, hs, ps),
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
