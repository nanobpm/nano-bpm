//! Server/WASM search parity guard (#1883).
//!
//! The in-browser engine and the gateway server both answer the agent searches
//! through the shared `nanobpmn-search` pipeline. What can still drift is the
//! glue each surface wraps around it: how the request body is parsed, which
//! read-model rows are handed to the pipeline (the server concatenates sharded
//! `nano-server-storage::ReadModel` rows, the WASM engine queries its single
//! in-memory `ReadStore`) and how the generated result is serialised.
//!
//! This guard drives one engine to a fixture state, projects the SAME event log
//! into a server `ReadModel`, and runs every request of a matrix DERIVED FROM THE
//! SPEC through both surfaces, failing on any difference (accept/reject or the
//! exact JSON result):
//!
//! * every property of `AgentInstanceFilter` / `AgentInstanceHistoryFilter` ×
//!   every advanced filter operator the spec defines (`$eq`, `$gte`, `$like`,
//!   …, collected from `spec/filters.yaml` + `spec/keys.yaml`), plus the
//!   implicit-equality shorthand. An operator a property does not support must
//!   be rejected by BOTH surfaces;
//! * every sort field of the `*SearchQuerySortRequest` enum, ascending and
//!   descending, walked page by page with `limit` + `after` cursors, plus a
//!   `before` cursor and offset (`from`) paging.
//!
//! The server side reproduces `search_agent_instances_impl` /
//! `search_agent_instance_history_impl` (`server/src/main.rs`): the generated
//! router deserialises the body with serde and runs the generated validation,
//! then the handler calls the shared pipeline over `ReadModel`. Extending the
//! guard to an operation migrated later (#1898) is one more `Op` entry.
//!
//! A second guard (#1900, bottom of this file) enumerates filters from the
//! published `@nanobpm/engine-wasm/readmodel-types` (`types.gen.ts`) instead of
//! the spec, and asserts the WASM engine accepts every advertised filter field
//! × operator.

use std::sync::Arc;

use nano_server_storage::readstore::ReadModel;
use nanobpmn_read_model::ReadStore;
use nanobpmn_search::models;
use serde_json::{json, Value as J};
use validator::Validate;

use super::*;

const OPERATOR_SPECS: [&str; 2] = [
    include_str!("../../spec/filters.yaml"),
    include_str!("../../spec/keys.yaml"),
];

fn yaml(src: &str) -> serde_yaml::Value {
    serde_yaml::from_str(src).expect("spec parses")
}

/// Every vendored spec document (`spec/*.yaml`), parsed. Read from disk (not a
/// hand-kept `include_str!` list) so a newly vendored spec file is enumerated
/// without touching this guard.
fn spec_documents() -> Vec<(String, serde_yaml::Value)> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../spec");
    let mut docs: Vec<(String, serde_yaml::Value)> = std::fs::read_dir(dir)
        .expect("spec dir")
        .map(|e| e.expect("spec entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .map(|p| {
            let src = std::fs::read_to_string(&p).expect("spec file");
            (p.display().to_string(), yaml(&src))
        })
        .collect();
    docs.sort_by(|a, b| a.0.cmp(&b.0));
    docs
}

/// The named component schema, from whichever spec document defines it.
fn schema(name: &str) -> serde_yaml::Value {
    spec_documents()
        .into_iter()
        .map(|(_, doc)| doc["components"]["schemas"][name].clone())
        .find(|s| !s.is_null())
        .unwrap_or_else(|| panic!("spec defines schema {name}"))
}

/// The property names of a spec object schema (e.g. a `*Filter`), following
/// `allOf` composition and local `$ref`s (`UserTaskFilter` = `allOf` of
/// `UserTaskFilterFields` + `$or`).
fn spec_properties(name: &str) -> Vec<String> {
    fn collect(s: &serde_yaml::Value, out: &mut Vec<String>) {
        if let Some(r) = s["$ref"].as_str() {
            let target = r.rsplit('/').next().expect("ref target");
            collect(&schema(target), out);
        }
        if let Some(props) = s["properties"].as_mapping() {
            out.extend(
                props
                    .keys()
                    .map(|k| k.as_str().expect("string key").to_string()),
            );
        }
        for part in s["allOf"].as_sequence().into_iter().flatten() {
            collect(part, out);
        }
    }
    let mut out = Vec::new();
    collect(&schema(name), &mut out);
    assert!(!out.is_empty(), "{name} has properties");
    out
}

/// The inline `type` of property `prop` of the spec object schema `name`
/// (following `allOf` / local `$ref`s like [`spec_properties`]); `None` when
/// the property is declared by composition rather than an inline type.
fn spec_property_type(name: &str, prop: &str) -> Option<String> {
    fn find(s: &serde_yaml::Value, prop: &str) -> Option<String> {
        if let Some(t) = s["properties"][prop]["type"].as_str() {
            return Some(t.to_string());
        }
        if let Some(r) = s["$ref"].as_str() {
            let target = r.rsplit('/').next().expect("ref target");
            if let Some(t) = find(&schema(target), prop) {
                return Some(t);
            }
        }
        s["allOf"]
            .as_sequence()
            .into_iter()
            .flatten()
            .find_map(|part| find(part, prop))
    }
    find(&schema(name), prop)
}

/// A well-formed scalar operand for the spec schema `s` (a filter property
/// schema), derived from the spec alone: the first `enum` value for an enum,
/// an integer for numeric types, a date-time for `format: date-time`, the
/// schema's `example` (keys, ids) or `"1"` for other strings, `true` for
/// booleans. Follows local/cross-file `$ref`s, `allOf`, and picks the
/// exact-match (non-object) branch of a `oneOf` advanced-filter property.
/// `None` for object/array properties (`$or`, variable joins), which have no
/// scalar shorthand.
fn spec_scalar_operand(s: &serde_yaml::Value) -> Option<J> {
    if let Some(first) = s["enum"].as_sequence().and_then(|e| e.first()) {
        return Some(serde_json::to_value(first).expect("enum value"));
    }
    if let Some(r) = s["$ref"].as_str() {
        let target = r.rsplit('/').next().expect("ref target");
        return spec_scalar_operand(&schema(target));
    }
    if let Some(v) = s["allOf"]
        .as_sequence()
        .into_iter()
        .flatten()
        .find_map(spec_scalar_operand)
    {
        return Some(v);
    }
    if let Some(v) = s["oneOf"]
        .as_sequence()
        .into_iter()
        .flatten()
        .find_map(spec_scalar_operand)
    {
        return Some(v);
    }
    match s["type"].as_str()? {
        "boolean" => Some(json!(true)),
        "integer" | "number" => Some(json!(1)),
        "string" if s["format"].as_str() == Some("date-time") => {
            Some(json!("2026-01-01T00:00:00Z"))
        }
        "string" => Some(
            s["example"]
                .as_str()
                .map_or_else(|| json!("1"), |e| json!(e)),
        ),
        _ => None,
    }
}

/// The `items` schema of an array-typed filter property schema `s`
/// (following `$ref` / `allOf`); `None` for a non-array schema.
fn spec_array_items(s: &serde_yaml::Value) -> Option<serde_yaml::Value> {
    if s["type"].as_str() == Some("array") {
        return Some(s["items"].clone());
    }
    if let Some(r) = s["$ref"].as_str() {
        let target = r.rsplit('/').next().expect("ref target");
        return spec_array_items(&schema(target));
    }
    s["allOf"]
        .as_sequence()
        .into_iter()
        .flatten()
        .find_map(spec_array_items)
}

/// Object-array filter properties with no scalar shorthand, each driven by
/// [`check_array_filters`]. Any other non-scalar filter fails the guard.
const ARRAY_FILTERS: &[&str] = &[
    "$or",
    "variables",
    "processInstanceVariables",
    "localVariables",
];

/// The schema of property `prop` of spec object schema `name` (following
/// `allOf` / `$ref` like [`spec_properties`]).
fn spec_property_schema(name: &str, prop: &str) -> Option<serde_yaml::Value> {
    fn find(s: &serde_yaml::Value, prop: &str) -> Option<serde_yaml::Value> {
        let p = &s["properties"][prop];
        if !p.is_null() {
            return Some(p.clone());
        }
        if let Some(r) = s["$ref"].as_str() {
            let target = r.rsplit('/').next().expect("ref target");
            if let Some(t) = find(&schema(target), prop) {
                return Some(t);
            }
        }
        s["allOf"]
            .as_sequence()
            .into_iter()
            .flatten()
            .find_map(|part| find(part, prop))
    }
    find(&schema(name), prop)
}

/// Whether the filter property schema `s` admits advanced-filter operator
/// objects (`{"$eq": …}`): some `$ref`/`allOf`/`oneOf` branch declares
/// `$`-prefixed properties. A plain scalar property (e.g. a bare `string`
/// key) takes only the implicit-equality shorthand.
fn spec_accepts_operators(s: &serde_yaml::Value) -> bool {
    if s["properties"].as_mapping().is_some_and(|m| {
        m.keys()
            .filter_map(|k| k.as_str())
            .any(|k| k.starts_with('$'))
    }) {
        return true;
    }
    if let Some(r) = s["$ref"].as_str() {
        let target = r.rsplit('/').next().expect("ref target");
        if spec_accepts_operators(&schema(target)) {
            return true;
        }
    }
    ["allOf", "oneOf", "anyOf"].iter().any(|k| {
        s[*k]
            .as_sequence()
            .into_iter()
            .flatten()
            .any(spec_accepts_operators)
    })
}

/// The advanced-filter operators (`$eq`, `$gt`, …) the filter property schema
/// `s` declares, following `$ref` / `allOf` / `oneOf` / `anyOf` like
/// [`spec_accepts_operators`]. Each must have an accepted probe.
fn spec_supported_operators(s: &serde_yaml::Value) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    if let Some(m) = s["properties"].as_mapping() {
        out.extend(
            m.keys()
                .filter_map(|k| k.as_str())
                .filter(|k| k.starts_with('$'))
                .map(str::to_string),
        );
    }
    if let Some(r) = s["$ref"].as_str() {
        let target = r.rsplit('/').next().expect("ref target");
        out.extend(spec_supported_operators(&schema(target)));
    }
    for k in ["allOf", "oneOf", "anyOf"] {
        for part in s[k].as_sequence().into_iter().flatten() {
            out.extend(spec_supported_operators(part));
        }
    }
    out
}

/// Scalar filter properties whose spec-declared operators cannot all take
/// an accepted well-formed operand, each with the reason. Keep this empty unless genuinely needed.
const UNEXERCISABLE_FILTERS: &[(&str, &str)] = &[];

/// The `field` enum of a spec `*SearchQuerySortRequest`.
fn spec_sort_fields(name: &str) -> Vec<String> {
    schema(name)["properties"]["field"]["enum"]
        .as_sequence()
        .unwrap_or_else(|| panic!("{name} has a field enum"))
        .iter()
        .map(|v| v.as_str().expect("string enum").to_string())
        .collect()
}

/// Every advanced filter operator the spec defines (`$eq`, `$in`, `$gte`, …).
fn spec_operators() -> Vec<String> {
    let mut ops = std::collections::BTreeSet::new();
    for src in OPERATOR_SPECS {
        let spec = yaml(src);
        let Some(schemas) = spec["components"]["schemas"].as_mapping() else {
            continue;
        };
        // Operators are declared inline OR inside composition parts
        // (`AdvancedStringFilter` = `allOf` [BasicStringFilter, {$like}]), so
        // walk `allOf`/`oneOf`/`anyOf` too — a top-level-only scan missed
        // `$like` entirely (#1946).
        fn walk(s: &serde_yaml::Value, ops: &mut std::collections::BTreeSet<String>) {
            if let Some(props) = s["properties"].as_mapping() {
                ops.extend(
                    props
                        .keys()
                        .filter_map(|k| k.as_str())
                        .filter(|k| k.starts_with('$'))
                        .map(str::to_string),
                );
            }
            for k in ["allOf", "oneOf", "anyOf"] {
                for part in s[k].as_sequence().into_iter().flatten() {
                    walk(part, ops);
                }
            }
        }
        for s in schemas.values() {
            walk(s, &mut ops);
        }
    }
    let ops: Vec<String> = ops.into_iter().collect();
    for required in [
        "$eq", "$neq", "$exists", "$in", "$notIn", "$like", "$gt", "$gte", "$lt", "$lte",
    ] {
        assert!(
            ops.iter().any(|o| o == required),
            "spec operator {required} not found"
        );
    }
    ops
}

/// The operand variants tried for `op` around the fixture value `v`.
fn operands(op: &str, v: &J) -> Vec<J> {
    match op {
        "$exists" => vec![json!(true), json!(false)],
        "$in" | "$notIn" => vec![json!([v]), json!([])],
        "$like" => {
            let s = v
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| v.to_string());
            let prefix: String = s.chars().take(2).collect();
            vec![json!(s), json!(format!("{prefix}*")), json!("no-match*")]
        }
        _ => vec![v.clone()],
    }
}

/// One surface's answer to a request body: the result JSON, or a rejection.
type Surface<'a, T> = Box<dyn Fn(&str) -> Result<T, String> + 'a>;

/// One search operation, answered by both surfaces.
struct Op<'a> {
    name: &'static str,
    filter_schema: &'static str,
    sort_schema: &'static str,
    wasm: Surface<'a, String>,
    server: Surface<'a, J>,
    /// A fixture item whose values seed the filter operands.
    sample: J,
    /// A filter every paging request carries so the walk covers several rows
    /// (history turns are PENDING; the default `commitStatus` hides them).
    page_filter: J,
}

/// The gateway router's body handling: the generated `StrictOptionalJson`
/// extractor (`strict::decode_strict`), then the generated validation rules.
fn server_body<T>(body: &str) -> Result<Option<T>, String>
where
    T: serde::de::DeserializeOwned + serde::Serialize + Validate,
{
    let query: Option<T> = nanobpmn_search::strict::decode_strict(body.as_bytes())?;
    if let Some(q) = &query {
        q.validate().map_err(|e| e.to_string())?;
    }
    Ok(query)
}

fn compare(op: &Op, body: &str) -> Option<J> {
    let wasm = (op.wasm)(body).map(|s| serde_json::from_str::<J>(&s).expect("wasm JSON"));
    let server = (op.server)(body);
    match (&wasm, &server) {
        (Ok(w), Ok(s)) => {
            assert_eq!(w, s, "{}: result differs for {body}", op.name);
            Some(s.clone())
        }
        (Err(_), Err(_)) => None,
        _ => panic!(
            "{}: accept/reject differs for {body}\n  wasm:   {wasm:?}\n  server: {server:?}",
            op.name
        ),
    }
}

fn check_filters(op: &Op) -> usize {
    let ops = spec_operators();
    let mut accepted = 0;
    let mut unexercised = Vec::new();
    for prop in spec_properties(op.filter_schema) {
        let is_bool = spec_property_type(op.filter_schema, &prop).as_deref() == Some("boolean");
        // The spec-derived well-formed operand; `None` = no scalar shorthand
        // (`$or`, variable joins — covered by `check_array_filters`).
        let prop_schema = spec_property_schema(op.filter_schema, &prop);
        // An array-typed property (`elementInstanceKeys`, `tags`) carries a
        // LIST of scalar filter values: derive operand + operator admission
        // from its `items` schema.
        let item_schema = prop_schema.as_ref().and_then(spec_array_items);
        let is_array = item_schema.is_some();
        let elem_schema = item_schema.or(prop_schema);
        let Some(spec_operand) = elem_schema.as_ref().and_then(spec_scalar_operand) else {
            // No scalar shorthand: must be one of the object-array filters
            // `check_array_filters` drives, never silently skipped.
            assert!(
                ARRAY_FILTERS.contains(&prop.as_str()),
                "{}: filter {prop} is neither scalar nor covered by check_array_filters",
                op.name
            );
            continue;
        };
        let takes_operators = elem_schema.as_ref().is_some_and(spec_accepts_operators);
        // Whether the fixture carries a real value for the property (an empty
        // list, e.g. no `tags`, is as good as absent).
        let present = match &op.sample[&prop] {
            J::Null => false,
            J::Array(a) => !a.is_empty(),
            _ => true,
        };
        let v = match &op.sample[&prop] {
            J::Array(a) => a.first().cloned().unwrap_or(spec_operand),
            J::Null => spec_operand,
            v => v.clone(),
        };
        let wrap = |f: J| if is_array { json!([f]) } else { f };
        // Every request carries the op's baseline filter (e.g. history's
        // `commitStatus: PENDING`, without which every history query matches
        // nothing); the tested property overrides it.
        let with_base = |cond: J| {
            let mut f = op.page_filter.clone();
            f.as_object_mut()
                .expect("page_filter is an object")
                .insert(prop.clone(), cond);
            json!({ "filter": f })
        };
        // The implicit-equality shorthand with a well-formed operand must be
        // ACCEPTED by both surfaces, whether or not the fixture carries a
        // value — agreeing rejections would let the matrix pass vacuously.
        let shorthand = with_base(wrap(v.clone())).to_string();
        let hit =
            compare(op, &shorthand).unwrap_or_else(|| panic!("{}: {shorthand} rejected", op.name));
        // A real fixture value must also match rows, so the comparison is
        // never two empty results.
        if present {
            assert!(
                !hit["items"].as_array().expect("items").is_empty(),
                "{}: {shorthand} matched no rows",
                op.name
            );
        }
        // A boolean filter must be accepted for BOTH schema-valid operands.
        if is_bool {
            for b in [true, false] {
                let body = with_base(wrap(json!(b))).to_string();
                accepted += usize::from(
                    compare(op, &body)
                        .unwrap_or_else(|| panic!("{}: {body} rejected", op.name))
                        .is_object(),
                );
            }
        }
        // Per-operator probes: EVERY operator the property's spec schema
        // declares must have at least one accepted probe (one accepted `$eq`
        // must not mask a rejected `$gt`); unsupported operators only need
        // the surfaces to agree.
        let supported = elem_schema
            .as_ref()
            .map(spec_supported_operators)
            .unwrap_or_default();
        let exempt = UNEXERCISABLE_FILTERS.iter().any(|(p, _)| *p == prop);
        // Every operand variant `operands` builds is well-formed for its
        // operator (`$in` an array of the scalar operand, `$exists` a boolean,
        // `$like` a string pattern), so for a DECLARED operator each one must
        // be accepted: an accepted `$in: []` must not mask a rejected
        // `$in: [v]`.
        // Probe the spec-wide operator set plus anything this property
        // declares, so a declared operator can never escape the probe set.
        let probe_ops: std::collections::BTreeSet<&String> = ops.iter().chain(&supported).collect();
        for o in probe_ops {
            let declared = takes_operators && supported.contains(o) && !exempt;
            for operand in operands(o, &v) {
                let body = with_base(wrap(json!({ o.as_str(): operand }))).to_string();
                if compare(op, &body).is_some() {
                    accepted += 1;
                } else if declared {
                    unexercised.push(format!("{prop}.{o} = {operand}"));
                }
            }
        }
    }
    assert!(
        unexercised.is_empty(),
        "{}: spec-declared operator probes rejected: {unexercised:?}",
        op.name
    );
    accepted
}

/// Schema-correct probes for the array-valued filters (`$or`, the variable
/// joins). `check_filters` seeds operands from a fixture item, which carries
/// no value for these, so its probes are malformed and both surfaces reject
/// them — agreement without exercising the filter. Here each such property is
/// driven with a well-formed matching AND non-matching clause; both must be
/// accepted, the match must return rows and the miss must return fewer.
fn check_array_filters(op: &Op) {
    let props = spec_properties(op.filter_schema);
    // A scalar property the fixture item carries, for the `$or` clause.
    let key = ["userTaskKey", "processInstanceKey", "variableKey"]
        .into_iter()
        .find(|k| props.iter().any(|p| p == k) && op.sample[*k].is_string())
        .unwrap_or_else(|| panic!("{}: no key property for $or", op.name));
    let hit_key = op.sample[key].clone();
    let mut probes: Vec<(String, J, J)> = Vec::new();
    if props.iter().any(|p| p == "$or") {
        probes.push((
            "$or".into(),
            json!([{ key: hit_key }]),
            json!([{ key: "1" }]),
        ));
    }
    // Every fixture instance carries the root variable `label`; the value
    // filter compares the variable's JSON-serialised value.
    for p in ["variables", "processInstanceVariables", "localVariables"] {
        if props.iter().any(|q| q == p) {
            probes.push((
                p.into(),
                json!([{ "name": "label", "value": "\"item-1\"" }]),
                json!([{ "name": "label", "value": "\"no-such-item\"" }]),
            ));
        }
    }
    for (prop, hit, miss) in probes {
        let all = compare(op, &json!({ "filter": op.page_filter }).to_string())
            .unwrap_or_else(|| panic!("{}: baseline rejected", op.name));
        let total = all["items"].as_array().expect("items").len();
        let run = |cond: &J| {
            let mut f = op.page_filter.clone();
            f.as_object_mut()
                .expect("page_filter is an object")
                .insert(prop.clone(), cond.clone());
            let body = json!({ "filter": f }).to_string();
            let r = compare(op, &body).unwrap_or_else(|| panic!("{}: {body} rejected", op.name));
            r["items"].as_array().expect("items").len()
        };
        let (h, m) = (run(&hit), run(&miss));
        // `localVariables` matches element-scope variables only; the fixture's
        // variables are root-scoped, so the probe must still be ACCEPTED and
        // agree, but may match nothing.
        if prop != "localVariables" {
            assert!(h > 0, "{}: {prop} {hit} matched no rows", op.name);
        }
        assert!(m < total, "{}: {prop} {miss} did not filter", op.name);
        assert!(
            m <= h || prop == "localVariables",
            "{}: {prop} miss > hit",
            op.name
        );
    }
}

fn check_paging(op: &Op) {
    for field in spec_sort_fields(op.sort_schema) {
        for order in ["ASC", "DESC"] {
            let sort = json!([{ "field": field, "order": order }]);
            let f = &op.page_filter;
            let all = compare(op, &json!({ "filter": f, "sort": sort }).to_string())
                .unwrap_or_else(|| panic!("{}: sort {field} {order} accepted", op.name));
            let total = all["items"].as_array().expect("items").len();
            // A walk over fewer than two rows never follows a cursor.
            assert!(total >= 2, "{}: paging fixture has {total} rows", op.name);
            // Forward walk with limit 1 + `after` cursors.
            let mut body = json!({ "filter": f, "sort": sort, "page": { "limit": 1 } });
            let mut followed = 0;
            let mut seen = 0;
            loop {
                let page = compare(op, &body.to_string()).expect("page accepted");
                let n = page["items"].as_array().expect("items").len();
                seen += n;
                let Some(cursor) = page["page"]["endCursor"].as_str().filter(|_| n > 0) else {
                    break;
                };
                // One backward step from every page's start, too.
                if let Some(start) = page["page"]["startCursor"].as_str() {
                    let back = json!({ "filter": f, "sort": sort, "page": { "limit": 1, "before": start } });
                    compare(op, &back.to_string()).expect("before page accepted");
                }
                body =
                    json!({ "filter": f, "sort": sort, "page": { "limit": 1, "after": cursor } });
                followed += 1;
                assert!(seen <= total, "{}: cursor walk overran", op.name);
            }
            assert_eq!(seen, total, "{}: cursor walk {field} {order}", op.name);
            assert!(
                followed >= total,
                "{}: cursor walk followed no cursors",
                op.name
            );
            for from in [0, 1, total] {
                let b = json!({ "filter": f, "sort": sort, "page": { "from": from, "limit": 1 } });
                compare(op, &b.to_string()).expect("offset page accepted");
            }
        }
    }
}

/// Three agent instances created at different instants, one with committed and
/// pending history across loop iterations and roles.
fn fixture() -> (TestEngine, String, Vec<std::ops::Range<usize>>) {
    let mut eng = TestEngine::new();
    let xml = include_str!("../../engine-core/tests/fixtures/external-agent-job-type.bpmn")
        .replace("agentType=\"external\"", "agentType=\"aiAgentTask\"");
    eng.deploy(&xml).unwrap();
    let mut first_key = None;
    let mut spans = Vec::new();
    spans.push(0..eng.log.len());
    for i in 0..3 {
        let start = eng.log.len();
        eng.advance_time(60_000.0).unwrap();
        eng.create_instance(
            "external-agent-routing",
            r#"{"route":"senior:rebase"}"#,
            None,
        )
        .unwrap();
        let jobs: J = serde_json::from_str(
            &eng.activate_jobs("senior:rebase", 1, 600_000.0, "W", Some(true))
                .unwrap(),
        )
        .unwrap();
        let job = &jobs[0];
        let created: J = serde_json::from_str(
            &eng.create_agent_instance(
                &json!({
                    "elementInstanceKey": job["elementInstanceKey"],
                    "jobKey": job["key"],
                    "jobLease": job["jobLeaseToken"],
                    "history": [{
                        "historyItemId": format!("config-{i}"), "loopIteration": 1,
                        "producedAt": format!("2026-01-0{}T03:04:05Z", i + 1),
                        "role": "CONFIGURATION", "content": [],
                        "model": "gpt", "provider": "openai",
                        "systemPrompt": [{"contentType":"TEXT", "text":"Be helpful"}],
                    }],
                })
                .to_string(),
            )
            .unwrap(),
        )
        .unwrap();
        let key = created["agentInstanceKey"].as_str().unwrap().to_string();
        if i == 1 {
            let turns: Vec<J> = (0..4)
                .map(|t| {
                    json!({
                        "historyItemId": format!("turn-{t}"),
                        "loopIteration": 1 + t / 2,
                        "producedAt": format!("2026-02-0{}T00:00:0{t}.250Z", 1 + t),
                        "role": if t % 2 == 0 { "USER" } else { "ASSISTANT" },
                        "content": [{ "contentType": "TEXT", "text": format!("t{t}") }],
                    })
                })
                .collect();
            eng.update_agent_instance(
                &key,
                &json!({
                    "elementInstanceKey": job["elementInstanceKey"],
                    "jobKey": job["key"],
                    "jobLease": job["jobLeaseToken"],
                    "status": "THINKING",
                    "history": turns,
                })
                .to_string(),
            )
            .unwrap();
        }
        if i == 1 {
            first_key = Some(key);
        }
        spans.push(start..eng.log.len());
    }
    (eng, first_key.expect("history fixture key"), spans)
}

/// A server `ReadModel` of two partitions fed the engine's event log, with the
/// three agent instances split across them (0 and 2 on shard 0, 1 on shard 1)
/// so their creation times — and every other sort key — interleave across the
/// shard boundary. A server that sorted or paged each shard before
/// concatenating would then diverge from the single-store WASM result.
fn server_read_model(eng: &TestEngine, spans: &[std::ops::Range<usize>]) -> ReadModel {
    let shard = |parts: &[usize]| {
        let events: Vec<&Event> = parts
            .iter()
            .flat_map(|&p| eng.log[spans[p].clone()].iter().map(|e| &e.event))
            .collect();
        let store = Arc::new(ReadStore::open(None).unwrap());
        store.export(&events).unwrap();
        store
    };
    // spans[0] is the deployment; each instance span follows it.
    ReadModel::from_shards(vec![(0, shard(&[0, 1, 3])), (1, shard(&[0, 2]))])
}

#[test]
fn agent_searches_match_the_server_for_every_spec_operator_sort_and_page() {
    let (eng, history_key, spans) = fixture();
    let rm = server_read_model(&eng, &spans);
    let key: u64 = history_key.parse().unwrap();

    let instances = Op {
        name: "searchAgentInstances",
        filter_schema: "AgentInstanceFilter",
        sort_schema: "AgentInstanceSearchQuerySortRequest",
        wasm: Box::new(|b| eng.search_agent_instances_json(b)),
        server: Box::new(|b| {
            let body = server_body::<models::AgentInstanceSearchQuery>(b)?;
            let r = nanobpmn_search::agent::search_agent_instances(body.as_ref(), || {
                rm.try_agent_instances()
            })
            .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(r).unwrap())
        }),
        sample: serde_json::from_str::<J>(&eng.search_agent_instances_json("{}").unwrap()).unwrap()
            ["items"][1]
            .clone(),
        page_filter: json!({}),
    };
    let history_sample = serde_json::from_str::<J>(
        &eng.search_agent_instance_history_json(
            &history_key,
            r#"{"filter":{"commitStatus":"PENDING"}}"#,
        )
        .unwrap(),
    )
    .unwrap()["items"][0]
        .clone();
    let history = Op {
        name: "searchAgentInstanceHistory",
        filter_schema: "AgentInstanceHistoryFilter",
        sort_schema: "AgentInstanceHistorySearchQuerySortRequest",
        wasm: Box::new(|b| eng.search_agent_instance_history_json(&history_key, b)),
        server: Box::new(|b| {
            let body = server_body::<models::AgentInstanceHistorySearchQuery>(b)?;
            let r =
                nanobpmn_search::agent::search_agent_instance_history(key, body.as_ref(), |f| {
                    rm.try_agent_history(f)
                })
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(r).unwrap())
        }),
        sample: history_sample,
        page_filter: json!({ "commitStatus": "PENDING" }),
    };

    for op in [&instances, &history] {
        assert!(!op.sample.is_null(), "{}: fixture has items", op.name);
        let accepted = check_filters(op);
        assert!(accepted > 0, "{}: no filter request accepted", op.name);
        check_paging(op);
    }
}

/// The server's sharded [`ReadModel`] as the shared layer's row source — the
/// same delegation the gateway's `ServerReadSource` declares (`server/src/main.rs`).
struct ServerSource<'a>(&'a ReadModel);

nanobpmn_search::impl_read_source!(ServerSource<'_>, |s| s.0);

const TASK_XML: &str = r#"
  <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
    <bpmn:process id="review-flow">
      <bpmn:startEvent id="s" />
      <bpmn:userTask id="review">
        <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
      </bpmn:userTask>
      <bpmn:endEvent id="e" />
      <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
      <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
    </bpmn:process>
  </bpmn:definitions>"#;

/// Three user-task instances created at different instants (one completed),
/// each with variables (one over the truncation preview length), plus a
/// deployed form and generic resource. Returns the engine and the log span of
/// the deployments (0) and of each instance.
fn core_fixture() -> (TestEngine, Vec<std::ops::Range<usize>>) {
    let mut eng = TestEngine::new();
    eng.deploy(TASK_XML).unwrap();
    eng.deploy_form(r#"{"id":"greeting","components":[]}"#)
        .unwrap();
    eng.deploy_resource("agent-prompt.md", "# Prompt").unwrap();
    let mut spans = Vec::new();
    spans.push(0..eng.log.len());
    let long = "x".repeat(nanobpmn_read_model::VARIABLE_VALUE_PREVIEW_LEN + 10);
    for i in 0..3 {
        let start = eng.log.len();
        eng.advance_time(60_000.0).unwrap();
        eng.create_instance(
            "review-flow",
            &json!({ "n": i, "label": format!("item-{i}"), "big": long }).to_string(),
            None,
        )
        .unwrap();
        if i == 1 {
            let tasks: J = serde_json::from_str(
                &eng.search_user_tasks_json(r#"{"filter":{"state":"CREATED"}}"#)
                    .unwrap(),
            )
            .unwrap();
            let key = tasks["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["userTaskKey"].as_str().unwrap().to_string())
                .max_by_key(|k| k.parse::<u64>().unwrap())
                .unwrap();
            eng.complete_user_task(&key, r#"{"approved":true}"#)
                .unwrap();
        }
        spans.push(start..eng.log.len());
    }
    (eng, spans)
}

#[test]
fn core_searches_match_the_server_for_every_spec_operator_sort_and_page() {
    let (eng, spans) = core_fixture();
    let rm = server_read_model(&eng, &spans);
    let src = ServerSource(&rm);
    // Variables are compared against a ONE-shard server model: the read model
    // mints `variableKey` per shard (SQLite AUTOINCREMENT), so keys collide
    // across partitions — a known gateway defect tracked in #1939. Drop this
    // carve-out (use `src`) once #1939 lands.
    let single_shard = ReadModel::from_shards(vec![(0, {
        let events: Vec<&Event> = eng.log.iter().map(|e| &e.event).collect();
        let store = Arc::new(ReadStore::open(None).unwrap());
        store.export(&events).unwrap();
        store
    })]);
    let var_src = ServerSource(&single_shard);
    let (eng, src, var_src) = (&eng, &src, &var_src);
    let sample = |json: String| serde_json::from_str::<J>(&json).unwrap()["items"][1].clone();

    let user_tasks = Op {
        name: "searchUserTasks",
        filter_schema: "UserTaskFilter",
        sort_schema: "UserTaskSearchQuerySortRequest",
        wasm: Box::new(|b| eng.search_user_tasks_json(b)),
        server: Box::new(|b| {
            let body = server_body::<models::UserTaskSearchQuery>(b)?;
            let r = nanobpmn_search::user_task::search_user_tasks(src, body.as_ref());
            Ok(serde_json::to_value(r).unwrap())
        }),
        sample: sample(eng.search_user_tasks_json("{}").unwrap()),
        page_filter: json!({}),
    };
    let instances = Op {
        name: "searchProcessInstances",
        filter_schema: "ProcessInstanceFilter",
        sort_schema: "ProcessInstanceSearchQuerySortRequest",
        wasm: Box::new(|b| eng.search_process_instances_json(b)),
        server: Box::new(|b| {
            let body = server_body::<models::ProcessInstanceSearchQuery>(b)?;
            let r = nanobpmn_search::process_instance::search_process_instances(src, body.as_ref());
            Ok(serde_json::to_value(r).unwrap())
        }),
        sample: sample(eng.search_process_instances_json("{}").unwrap()),
        page_filter: json!({}),
    };
    let variables = |truncate: bool| Op {
        name: if truncate {
            "searchVariables"
        } else {
            "searchVariables(truncateValues=false)"
        },
        filter_schema: "VariableFilter",
        sort_schema: "VariableSearchQuerySortRequest",
        wasm: Box::new(move |b| eng.search_variables_json(b, Some(truncate))),
        server: Box::new(move |b| {
            let body = server_body::<models::VariableSearchQuery>(b)?;
            let r = nanobpmn_search::variable::search_variables(var_src, truncate, body.as_ref());
            Ok(serde_json::to_value(r).unwrap())
        }),
        sample: sample(eng.search_variables_json("{}", Some(truncate)).unwrap()),
        page_filter: json!({}),
    };

    for op in [&user_tasks, &instances, &variables(true), &variables(false)] {
        assert!(!op.sample.is_null(), "{}: fixture has items", op.name);
        check_array_filters(op);
        let accepted = check_filters(op);
        assert!(accepted > 0, "{}: no filter request accepted", op.name);
        check_paging(op);
    }
}

/// The gateway's mapping of a shared [`Lookup`](nanobpmn_search::get::Lookup)
/// onto HTTP, rendered in the WASM transport's terms: 200 ⇒ the body, 404 ⇒
/// JSON `null`, 400 ⇒ a rejection.
fn server_lookup<T: serde::Serialize>(
    lookup: nanobpmn_search::get::Lookup<T>,
) -> Result<J, String> {
    use nanobpmn_search::get::Lookup;
    match lookup {
        Lookup::Found(v) => Ok(serde_json::to_value(v).unwrap()),
        l @ Lookup::NotFound { .. } => {
            assert_eq!(l.status(), 404);
            Ok(J::Null)
        }
        l @ Lookup::Invalid { .. } => {
            assert_eq!(l.status(), 400);
            Err(format!("{}", l.status()))
        }
    }
}

#[test]
fn get_operations_match_the_server_for_found_missing_and_malformed_keys() {
    let (mut eng, spans) = core_fixture();
    let (agent_eng, agent_key, agent_spans) = fixture();
    let rm = server_read_model(&eng, &spans);
    let src = ServerSource(&rm);
    let agent_rm = server_read_model(&agent_eng, &agent_spans);
    let first = |json: String, field: &str| {
        serde_json::from_str::<J>(&json).unwrap()[field]
            .as_str()
            .unwrap()
            .to_string()
    };
    // Redeploying the identical form/resource is idempotent: it returns the
    // fixture's (already projected) key without emitting events.
    let form_key = first(
        eng.deploy_form(r#"{"id":"greeting","components":[]}"#)
            .unwrap(),
        "formKey",
    );
    let resource_key = first(
        eng.deploy_resource("agent-prompt.md", "# Prompt").unwrap(),
        "resourceKey",
    );
    let probes = |real: &str| {
        vec![
            real.to_string(),
            "999999".to_string(),
            "0".to_string(),
            "-1".to_string(),
            "x".to_string(),
            String::new(),
            format!(" {real}"),
        ]
    };
    let mut found = 0;
    let mut check = |name: &str, wasm: Result<String, String>, server: Result<J, String>| {
        let wasm = wasm.map(|s| serde_json::from_str::<J>(&s).expect("wasm JSON"));
        match (&wasm, &server) {
            (Ok(w), Ok(s)) => {
                assert_eq!(w, s, "{name}: result differs");
                found += usize::from(!s.is_null());
            }
            (Err(_), Err(_)) => {}
            _ => panic!("{name}: accept/reject differs\n  wasm: {wasm:?}\n  server: {server:?}"),
        }
    };
    for k in probes(&form_key) {
        check(
            &format!("getFormByKey({k:?})"),
            eng.get_form_by_key_json(&k),
            server_lookup(nanobpmn_search::get::get_form_by_key(&src, &k)),
        );
    }
    for k in probes(&resource_key) {
        check(
            &format!("getResource({k:?})"),
            eng.get_resource_by_key_json(&k),
            server_lookup(nanobpmn_search::get::get_resource(&src, &k)),
        );
    }
    for k in probes(&agent_key) {
        check(
            &format!("getAgentInstance({k:?})"),
            agent_eng.get_agent_instance_json(&k),
            nanobpmn_search::get::get_agent_instance(&k, |key| agent_rm.try_agent_instance(key))
                .map_err(|e| e.to_string())
                .and_then(server_lookup),
        );
    }
    assert_eq!(found, 3, "each real key resolves on both surfaces");
}

/// The WASM read methods (`js_name`s) that answer a spec operation, derived
/// from the `#[wasm_bindgen(js_name = …)]` attributes in `lib.rs` itself. The
/// only rename: the WASM `getResourceByKey` answers the spec's `getResource`.
fn wasm_read_operations() -> std::collections::BTreeSet<String> {
    let src = include_str!("lib.rs");
    src.split("js_name = ")
        .skip(1)
        .map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect::<String>()
        })
        .filter(|n| n.starts_with("search") || n.starts_with("get"))
        .map(|n| match n.as_str() {
            "getResourceByKey" => "getResource".to_string(),
            _ => n,
        })
        .collect()
}

/// Every `search*` / `get*` `operationId` in the vendored spec.
fn spec_read_operations() -> std::collections::BTreeSet<String> {
    let mut ops = std::collections::BTreeSet::new();
    for (_, doc) in spec_documents() {
        for item in doc["paths"]
            .as_mapping()
            .into_iter()
            .flat_map(|m| m.values())
        {
            for op in item.as_mapping().into_iter().flat_map(|m| m.values()) {
                if let Some(id) = op["operationId"].as_str() {
                    if id.starts_with("search") || id.starts_with("get") {
                        ops.insert(id.to_string());
                    }
                }
            }
        }
    }
    ops
}

/// The spec operations the parity tests in this module drive through BOTH
/// surfaces. Extending the WASM read surface means adding the op's parity
/// check above AND its name here; the guard below fails until both happen.
const PARITY_CHECKED: [&str; 8] = [
    "searchAgentInstances",
    "searchAgentInstanceHistory",
    "searchUserTasks",
    "searchProcessInstances",
    "searchVariables",
    "getFormByKey",
    "getResource",
    "getAgentInstance",
];

#[test]
fn parity_guard_enumerates_every_spec_search_and_get_operation() {
    let spec = spec_read_operations();
    let wasm = wasm_read_operations();
    let checked: std::collections::BTreeSet<String> =
        PARITY_CHECKED.iter().map(|s| s.to_string()).collect();
    assert!(
        spec.len() > PARITY_CHECKED.len(),
        "spec enumerates read ops"
    );
    // Every WASM read method answers a real spec operation…
    let unknown: Vec<_> = wasm.difference(&spec).collect();
    assert!(
        unknown.is_empty(),
        "WASM read methods with no spec operation: {unknown:?}"
    );
    // …and every spec search/get operation is classified: the ones the WASM
    // engine exposes are parity-checked against the server; the rest are not
    // on the WASM surface. A newly exposed WASM read without a parity check
    // (or a stale parity entry) fails here.
    for op in &spec {
        assert_eq!(
            wasm.contains(op),
            checked.contains(op),
            "{op}: exposed by WASM = {}, parity-checked = {}",
            wasm.contains(op),
            checked.contains(op)
        );
    }
    let stale: Vec<_> = checked.difference(&spec).collect();
    assert!(
        stale.is_empty(),
        "parity entries naming no spec operation: {stale:?}"
    );
}

/// A stub surface pair that accepts every body except those containing
/// `reject`, answering one empty-ish row.
fn stub_op<'a>(reject: &'a str) -> Op<'a> {
    let accept = move |b: &str| {
        if !reject.is_empty() && b.contains(reject) {
            Err(format!("rejected {b}"))
        } else {
            Ok(json!({ "items": [{}] }))
        }
    };
    Op {
        name: "stub",
        filter_schema: "UserTaskFilter",
        sort_schema: "UserTaskSearchQuerySortRequest",
        wasm: Box::new(move |b| accept(b).map(|j| j.to_string())),
        server: Box::new(move |b| accept(b)),
        sample: json!({}),
        page_filter: json!({}),
    }
}

#[test]
fn spec_operands_are_well_formed_per_property_type() {
    let op = |p: &str| {
        spec_property_schema("UserTaskFilter", p)
            .as_ref()
            .and_then(spec_scalar_operand)
    };
    assert_eq!(op("state"), Some(json!("CREATING")));
    assert_eq!(op("priority"), Some(json!(1)));
    assert_eq!(op("creationDate"), Some(json!("2026-01-01T00:00:00Z")));
    let key = op("userTaskKey").expect("key operand");
    assert!(
        key.as_str().is_some_and(|k| k.parse::<i64>().is_ok()),
        "{key}"
    );
    assert_eq!(op("$or"), None);
    // Scalar-array filters resolve their operand through `items`.
    let item = |f: &str, p: &str| {
        spec_property_schema(f, p)
            .as_ref()
            .and_then(spec_array_items)
            .as_ref()
            .and_then(spec_scalar_operand)
    };
    assert_eq!(
        item("ProcessInstanceFilter", "tags"),
        Some(json!("business_key:1234"))
    );
    assert_eq!(
        item("UserTaskFilter", "tags"),
        Some(json!("business_key:1234"))
    );
    assert!(item("AgentInstanceFilter", "elementInstanceKeys").is_some());
    let ops = |p: &str| {
        spec_property_schema("UserTaskFilter", p)
            .as_ref()
            .is_some_and(spec_accepts_operators)
    };
    assert!(ops("state"), "state is an advanced filter");
    assert!(!ops("userTaskKey"), "userTaskKey is a plain scalar");
}

#[test]
fn filter_guard_fails_when_both_surfaces_reject_a_well_formed_enum_operand() {
    // Control: surfaces accepting everything pass the guard.
    check_filters(&stub_op(""));
    // Both surfaces rejecting the well-formed enum value AGREE — the old guard
    // counted that as parity; it must now fail.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_filters(&stub_op("\"CREATING\""))
    }));
    assert!(r.is_err(), "guard passed despite rejected enum operand");
}

#[test]
fn filter_guard_fails_when_both_surfaces_reject_one_supported_operator() {
    // `priority` supports `$gt`; rejecting only `$gt` (while `$eq` etc. are
    // accepted) must still fail the guard.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_filters(&stub_op("\"$gt\""))
    }));
    assert!(r.is_err(), "guard passed despite rejected $gt operator");
    assert!(
        spec_supported_operators(&spec_property_schema("UserTaskFilter", "priority").unwrap())
            .contains("$gt")
    );
}

#[test]
fn filter_guard_fails_when_both_surfaces_reject_a_tag_array_operand() {
    // `tags` is a scalar-array filter: its shorthand must be probed (and
    // accepted), not skipped as a non-scalar property.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_filters(&stub_op("business_key:1234"))
    }));
    assert!(r.is_err(), "guard passed despite rejected tags operand");
}

/// Runs the guard over surfaces rejecting bodies containing `needle`.
fn guard_rejects(needle: &str) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_filters(&stub_op(needle))
    }))
    .is_err()
}

#[test]
fn filter_guard_fails_when_both_surfaces_reject_any_declared_operator() {
    // Every operator some UserTaskFilter property declares: rejecting it
    // everywhere (both surfaces agree) must fail the guard.
    let declared: std::collections::BTreeSet<String> = spec_properties("UserTaskFilter")
        .iter()
        .filter_map(|p| spec_property_schema("UserTaskFilter", p))
        .flat_map(|s| spec_supported_operators(&s))
        .collect();
    for o in ["$eq", "$neq", "$exists", "$in", "$like", "$gte", "$lt"] {
        assert!(declared.contains(o), "UserTaskFilter declares {o}");
    }
    for o in &declared {
        assert!(
            guard_rejects(&format!("\"{o}\"")),
            "guard passed with {o} rejected"
        );
    }
}

#[test]
fn filter_guard_fails_when_one_well_formed_operand_variant_is_rejected() {
    // `$in: []` accepted must not mask a rejected non-empty `$in` list.
    assert!(guard_rejects("\"$in\":[\""), "non-empty $in rejected");
    // A rejected `$exists: false` is not masked by an accepted `true`.
    assert!(guard_rejects("\"$exists\":false"), "$exists false rejected");
    // A rejected `$like` wildcard pattern is not masked by an exact pattern.
    assert!(guard_rejects("no-match*"), "$like pattern rejected");
}

// ---------------------------------------------------------------------------
// Published TS `readmodel-types` ⇄ WASM acceptance guard (#1900).
//
// `@nanobpm/engine-wasm/readmodel-types` ships `types.gen.ts`, generated from
// the spec. The parity tests above enumerate filters from the SPEC; this guard
// enumerates them from the PUBLISHED TYPES, so the types package can never
// advertise a filter field or operator the WASM engine rejects (#1883 item 5):
// every filter field of every WASM search request type × every advanced
// operator its TS type admits (plus the implicit-equality shorthand) must be
// ACCEPTED by the WASM engine.
// ---------------------------------------------------------------------------

/// The published TS declarations (`export type Name = <expr>;`), comments
/// stripped, keyed by name.
fn ts_decls() -> std::collections::BTreeMap<String, String> {
    let src = include_str!("../readmodel-types/types.gen.ts");
    // Strip block comments (doc comments contain braces and `$or` examples).
    let mut text = String::with_capacity(src.len());
    let mut rest = src;
    while let Some(i) = rest.find("/*") {
        text.push_str(&rest[..i]);
        let end = rest[i..].find("*/").expect("unterminated comment");
        rest = &rest[i + end + 2..];
    }
    text.push_str(rest);
    let text: String = text
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut decls = std::collections::BTreeMap::new();
    for chunk in text.split("export type ").skip(1) {
        let (name, expr) = chunk.split_once(" = ").expect("type alias");
        let mut depth = 0i32;
        let mut quote = None;
        let mut end = expr.len();
        for (i, c) in expr.char_indices() {
            match (quote, c) {
                (Some(q), c) if c == q => quote = None,
                (Some(_), _) => {}
                (None, '\'' | '"') => quote = Some(c),
                (None, '{' | '<' | '(' | '[') => depth += 1,
                (None, '}' | '>' | ')' | ']') => depth -= 1,
                (None, ';') if depth == 0 => {
                    end = i;
                    break;
                }
                _ => {}
            }
        }
        decls.insert(name.trim().to_string(), expr[..end].trim().to_string());
    }
    decls
}

/// Split a TS type expression on `sep` at nesting depth 0.
fn ts_split(expr: &str, sep: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut quote = None;
    let mut cur = String::new();
    for c in expr.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"') => quote = Some(c),
            (None, '{' | '<' | '(' | '[') => depth += 1,
            (None, '}' | '>' | ')' | ']') => depth -= 1,
            (None, c) if c == sep && depth == 0 => {
                parts.push(std::mem::take(&mut cur).trim().to_string());
                continue;
            }
            _ => {}
        }
        cur.push(c);
    }
    let last = cur.trim().to_string();
    if !last.is_empty() {
        parts.push(last);
    }
    parts.retain(|p| !p.is_empty());
    parts
}

/// The `(field, type)` members of a TS object-literal type `{ a?: T; … }`.
fn ts_object_fields(lit: &str) -> Vec<(String, String)> {
    let inner = lit
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .expect("object literal");
    ts_split(inner, ';')
        .into_iter()
        .map(|m| {
            let (k, t) = m.split_once(':').expect("member `name: type`");
            let k = k.trim().trim_end_matches('?').trim_matches('\'');
            (k.to_string(), t.trim().to_string())
        })
        .collect()
}

/// The fields of a TS object type expression, following `&` intersections
/// and named aliases (`UserTaskFilter = UserTaskFilterFields & { $or… }`).
fn ts_fields(
    decls: &std::collections::BTreeMap<String, String>,
    expr: &str,
) -> Vec<(String, String)> {
    ts_split(expr, '&')
        .into_iter()
        .flat_map(|part| {
            if part.starts_with('{') {
                ts_object_fields(&part)
            } else {
                let d = decls
                    .get(&part)
                    .unwrap_or_else(|| panic!("TS type {part} declared"));
                ts_fields(decls, d)
            }
        })
        .collect()
}

/// The element type of `Array<T>` / `T[]`, if `expr` is an array type.
fn ts_array_elem(expr: &str) -> Option<&str> {
    expr.strip_prefix("Array<")
        .and_then(|s| s.strip_suffix('>'))
        .or_else(|| expr.strip_suffix("[]"))
}

/// The advanced-filter operators (`$`-prefixed members) a TS filter-property
/// type admits, following `|` / `&` composition and named aliases. An array
/// property (`elementInstanceKeys`) admits its element type's operators.
fn ts_operators(
    decls: &std::collections::BTreeMap<String, String>,
    expr: &str,
) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    for alt in ts_split(expr, '|') {
        for part in ts_split(&alt, '&') {
            if let Some(elem) = ts_array_elem(&part) {
                out.extend(ts_operators(decls, elem));
            } else if part.starts_with('{') {
                out.extend(
                    ts_object_fields(&part)
                        .into_iter()
                        .map(|(k, _)| k)
                        .filter(|k| k.starts_with('$')),
                );
            } else if let Some(d) = decls.get(&part) {
                out.extend(ts_operators(decls, d));
            }
        }
    }
    out
}

/// The WASM search methods and the published TS request type each accepts.
/// Must cover exactly the WASM `search*` read methods (checked below).
const TS_SEARCH_REQUESTS: [(&str, &str); 5] = [
    ("searchAgentInstances", "AgentInstanceSearchQuery"),
    (
        "searchAgentInstanceHistory",
        "AgentInstanceHistorySearchQuery",
    ),
    ("searchUserTasks", "UserTaskSearchQuery"),
    ("searchProcessInstances", "ProcessInstanceSearchQuery"),
    ("searchVariables", "VariableSearchQuery"),
];

/// Every TS-advertised filter probe for the request type `query`, as WASM
/// request bodies. The probe operand per field is derived from the spec
/// schema the TS type is generated from (a TS field the spec lacks fails).
fn ts_filter_probes(query: &str) -> Vec<String> {
    let decls = ts_decls();
    let q = decls
        .get(query)
        .unwrap_or_else(|| panic!("TS declares {query}"));
    let filter_ty = ts_fields(&decls, q)
        .into_iter()
        .find(|(k, _)| k == "filter")
        .unwrap_or_else(|| panic!("{query} has a filter"))
        .1;
    let fields = ts_fields(&decls, &filter_ty);
    assert!(!fields.is_empty(), "{filter_ty} advertises fields");
    let mut bodies = Vec::new();
    for (field, ty) in fields {
        // Resolve named aliases (`tags?: TagSet`, `TagSet = Array<Tag>`).
        let mut resolved = ty.as_str();
        while let Some(d) = decls.get(resolved).filter(|d| !d.contains('|')) {
            resolved = d;
        }
        let is_array = ts_array_elem(resolved).is_some();
        let wrap = |f: J| if is_array { json!([f]) } else { f };
        if ARRAY_FILTERS.contains(&field.as_str()) {
            let v = if field == "$or" {
                json!([{}])
            } else {
                json!([{ "name": "label", "value": "\"item-1\"" }])
            };
            bodies.push(json!({ "filter": { field.as_str(): v } }).to_string());
            continue;
        }
        let prop_schema = spec_property_schema(&filter_ty, &field).unwrap_or_else(|| {
            panic!("{filter_ty}.{field}: advertised by readmodel-types but not in the spec")
        });
        let elem_schema = spec_array_items(&prop_schema).unwrap_or(prop_schema);
        let v = spec_scalar_operand(&elem_schema)
            .unwrap_or_else(|| panic!("{filter_ty}.{field}: no scalar operand"));
        bodies.push(json!({ "filter": { field.as_str(): wrap(v.clone()) } }).to_string());
        for o in ts_operators(&decls, &ty) {
            for operand in operands(&o, &v) {
                bodies.push(
                    json!({ "filter": { field.as_str(): wrap(json!({ o.as_str(): operand })) } })
                        .to_string(),
                );
            }
        }
    }
    bodies
}

#[test]
fn ts_parser_reads_fields_and_operators_through_composition() {
    let decls = ts_decls();
    let fields: Vec<String> = ts_fields(&decls, &decls["UserTaskFilter"])
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(fields.iter().any(|f| f == "$or"), "{fields:?}");
    assert!(fields.iter().any(|f| f == "assignee"), "{fields:?}");
    // `$like` lives in `AdvancedStringFilter = BasicStringFilter & { $like }`.
    let ops = ts_operators(&decls, "StringFilterProperty");
    for o in ["$eq", "$neq", "$exists", "$in", "$notIn", "$like"] {
        assert!(ops.contains(o), "{o} in {ops:?}");
    }
    assert!(ts_operators(&decls, "DateTimeFilterProperty").contains("$gte"));
    assert!(ts_operators(&decls, "Array<IntegerFilterProperty>").contains("$lt"));
}

#[test]
fn ts_search_requests_cover_every_wasm_search_method() {
    let wasm: std::collections::BTreeSet<String> = wasm_read_operations()
        .into_iter()
        .filter(|n| n.starts_with("search"))
        .collect();
    let covered: std::collections::BTreeSet<String> = TS_SEARCH_REQUESTS
        .iter()
        .map(|(n, _)| n.to_string())
        .collect();
    assert_eq!(
        wasm, covered,
        "every WASM search method maps to its TS request type"
    );
}

#[test]
fn every_readmodel_types_filter_field_and_operator_is_accepted_by_wasm() {
    let (agent_eng, history_key, _) = fixture();
    let (core_eng, _) = core_fixture();
    let mut rejected = Vec::new();
    let mut probes = 0;
    for (method, query) in TS_SEARCH_REQUESTS {
        for body in ts_filter_probes(query) {
            let r = match method {
                "searchAgentInstances" => agent_eng.search_agent_instances_json(&body),
                "searchAgentInstanceHistory" => {
                    agent_eng.search_agent_instance_history_json(&history_key, &body)
                }
                "searchUserTasks" => core_eng.search_user_tasks_json(&body),
                "searchProcessInstances" => core_eng.search_process_instances_json(&body),
                "searchVariables" => core_eng.search_variables_json(&body, None),
                other => panic!("no WASM dispatch for {other}"),
            };
            probes += 1;
            if let Err(e) = r {
                rejected.push(format!("{method} {body}: {e}"));
            }
        }
    }
    assert!(probes > 100, "probed {probes} TS filters");
    assert!(
        rejected.is_empty(),
        "readmodel-types advertises filters the WASM engine rejects:\n{}",
        rejected.join("\n")
    );
}

#[test]
fn ts_probes_include_the_1883_date_range_operator() {
    // The #1883 symptom: `creationDate: {$gte: …}` was advertised by the
    // types and rejected by WASM. The guard must probe exactly that request.
    let probes = ts_filter_probes("AgentInstanceSearchQuery");
    assert!(
        probes
            .iter()
            .any(|b| b.contains(r#""creationDate":{"$gte""#)),
        "{probes:?}"
    );
    assert!(probes.iter().any(|b| b.contains(r#""tenantId":{"$like""#)));
}
