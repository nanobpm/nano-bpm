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
        for s in schemas.values() {
            if let Some(props) = s["properties"].as_mapping() {
                for k in props.keys().filter_map(|k| k.as_str()) {
                    if k.starts_with('$') {
                        ops.insert(k.to_string());
                    }
                }
            }
        }
    }
    let ops: Vec<String> = ops.into_iter().collect();
    for required in [
        "$eq", "$neq", "$exists", "$in", "$gt", "$gte", "$lt", "$lte",
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
    for prop in spec_properties(op.filter_schema) {
        let v = match &op.sample[&prop] {
            // No fixture value (e.g. a never-completed `completionDate`):
            // a well-formed operand of the property's type.
            J::Null if prop.ends_with("Date") || prop.ends_with("At") => {
                json!("2026-01-01T00:00:00Z")
            }
            J::Null => json!("1"),
            J::Array(a) => a.first().cloned().unwrap_or(json!("1")),
            v => v.clone(),
        };
        // The implicit-equality shorthand on a real fixture value must be
        // accepted, so the matrix cannot pass vacuously by both surfaces
        // rejecting everything.
        // An array-typed property (`elementInstanceKeys`) carries a LIST of
        // per-element filter properties.
        let is_array = op.sample[&prop].is_array();
        // Whether the fixture carries a real value for the property (an empty
        // list, e.g. no `tags`, is as good as absent).
        let present = match &op.sample[&prop] {
            J::Null => false,
            J::Array(a) => !a.is_empty(),
            _ => true,
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
        let shorthand = with_base(wrap(v.clone())).to_string();
        let hit = compare(op, &shorthand);
        // A real fixture value must be accepted AND match rows, so the
        // comparison is never two empty results. (A property the fixture has no
        // value for — `$or`, variable filters, … — only needs both surfaces to
        // agree.)
        if present {
            let hit = hit.unwrap_or_else(|| panic!("{}: {shorthand} rejected", op.name));
            assert!(
                !hit["items"].as_array().expect("items").is_empty(),
                "{}: {shorthand} matched no rows",
                op.name
            );
        }
        let mut bodies = Vec::new();
        for o in &ops {
            for operand in operands(o, &v) {
                bodies.push(with_base(wrap(json!({ o.as_str(): operand }))));
            }
        }
        for body in bodies {
            accepted += usize::from(compare(op, &body.to_string()).is_some());
        }
    }
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
