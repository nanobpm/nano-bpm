//! Trace-validation conformance for the **VariableScopes** spec (#1522, epic
//! #1224): variable-scope isolation of a parallel multi-instance sub-process,
//! anchored to the real engine so `formal/tla/varscopes/VariableScopes.tla`
//! cannot silently drift from `engine-core`.
//!
//! `formal/tla/gen-traces.sh` dumps, for every `SPEC_TRACE_MODELS` entry of
//! `formal/tla/specs/VariableScopes.spec`, the TLC witness of the shortest
//! completing behaviour in which sibling children interleave (the
//! `overlapped` ghost — the #1518 shape) into
//! `formal/tla/traces/VariableScopes/<Model>.json`. This test rebuilds each
//! model as a real BPMN process, drives the engine through the **same** step
//! sequence (`Write(i)` / `Finish(i)` complete child `i`'s first / second job)
//! and asserts after every step that the engine's variable tree — the root
//! scope, the MI body scope, every live child scope, the collected
//! `outputCollection` slots and the root's propagated collection — equals the
//! spec's. A divergence fails the test: no tolerated mismatch, no retries.
//!
//! The spec's `Activate` (body + all parallel children activated, locals
//! seeded) and `Complete` (body completes once every child has) are internal
//! steps the engine performs atomically within the command that enables them
//! (instance creation / the last child's completion), so they are fused with
//! the preceding driven step: the engine is compared against the spec state at
//! the end of each such fused run.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{Command, Engine, Key, Value};
use serde_json::{json, Value as Json};

/// The variable names the spec tracks (its `Names` constant). Engine-only
/// bookkeeping (`items`, the body-local `results` collection) is outside the
/// spec's vocabulary and projected away.
const NAMES: [&str; 4] = ["item", "loopCounter", "result", "seen"];
const OUTPUT_COLLECTION: &str = "results";

fn traces_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../formal/tla/traces/VariableScopes")
}

/// The process for a spec config: a parallel MI sub-process over `items`
/// (`inputElement="item"`, `outputElement="=<outputElement>"`) whose body runs
/// `first` then `second`, followed by a `sink` task that parks the token so
/// the root scope stays inspectable after the body completes.
fn process_xml(output_element: &str) -> String {
    format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="vs" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Each">
      <bpmn:multiInstanceLoopCharacteristics>
        <bpmn:extensionElements>
          <zeebe:loopCharacteristics inputCollection="=items" inputElement="item"
            outputCollection="{OUTPUT_COLLECTION}" outputElement="={output_element}" />
        </bpmn:extensionElements>
      </bpmn:multiInstanceLoopCharacteristics>
      <bpmn:startEvent id="ss" />
      <bpmn:serviceTask id="t1">
        <bpmn:extensionElements><zeebe:taskDefinition type="first" /></bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:serviceTask id="t2">
        <bpmn:extensionElements><zeebe:taskDefinition type="second" /></bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="i1" sourceRef="ss" targetRef="t1" />
      <bpmn:sequenceFlow id="i2" sourceRef="t1" targetRef="t2" />
      <bpmn:sequenceFlow id="i3" sourceRef="t2" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:serviceTask id="sink">
      <bpmn:extensionElements><zeebe:taskDefinition type="sink" /></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Each" />
    <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="sink" />
    <bpmn:sequenceFlow id="f3" sourceRef="sink" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#
    )
}

/// Engine value -> the spec's JSON projection (items are integers, the nil
/// seed is `null`).
fn to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Int(i) => json!(i),
        Value::List(xs) => Json::Array(xs.iter().map(to_json).collect()),
        other => panic!("value outside the spec's vocabulary: {other:?}"),
    }
}

/// One scope's map projected onto the spec's `Names`.
fn project_scope(map: &HashMap<String, Value>) -> Json {
    let m: serde_json::Map<String, Json> = NAMES
        .iter()
        .filter_map(|n| map.get(*n).map(|v| ((*n).to_string(), to_json(v))))
        .collect();
    Json::Object(m)
}

/// The engine's observable state in the fixture's `state` shape.
fn engine_state(engine: &Engine, key: Key, n: usize) -> Json {
    let inst = engine
        .instance(key)
        .expect("instance present (parked at sink)");
    // Teardown guard: every non-root variable scope must belong to a live
    // element instance, so a child/body scope leaked past completion fails the
    // replay instead of projecting as null/empty.
    for k in inst.scope_variables.keys().chain(inst.scope_parents.keys()) {
        assert!(
            inst.active.contains_key(k),
            "stale variable scope {k} outlived its element instance"
        );
    }
    let mi = inst.multi_instances.values().next();
    let mut children = serde_json::Map::new();
    let mut collected = vec![json!("undef"); n];
    let body = match mi {
        Some(mi) => {
            assert_eq!(inst.multi_instances.len(), 1, "exactly one MI body");
            for i in 1..=n {
                let live = mi
                    .child_indices
                    .iter()
                    .find(|(k, idx)| **idx == i - 1 && mi.active.contains(k))
                    .map(|(k, _)| *k);
                let scope = live.map(|k| {
                    project_scope(inst.scope_variables.get(&k).unwrap_or(&HashMap::new()))
                });
                children.insert(i.to_string(), scope.unwrap_or(Json::Null));
            }
            for (slot, v) in collected.iter_mut().zip(&mi.output_values) {
                if let Some(v) = v {
                    *slot = to_json(v);
                }
            }
            let body_key = inst
                .multi_instances
                .keys()
                .next()
                .copied()
                .expect("body key");
            project_scope(
                inst.scope_variables
                    .get(&body_key)
                    .unwrap_or(&HashMap::new()),
            )
        }
        None => {
            for i in 1..=n {
                children.insert(i.to_string(), Json::Null);
            }
            json!({})
        }
    };
    let root_results = match inst.variables.get(OUTPUT_COLLECTION) {
        Some(Value::List(xs)) => xs.iter().map(to_json).collect(),
        Some(other) => panic!("root {OUTPUT_COLLECTION} is not a list: {other:?}"),
        None => vec![json!("undef"); n],
    };
    if mi.is_none() {
        // The body completed: its collection is the root's (the spec keeps
        // `collected` after Complete).
        collected.clone_from(&root_results);
    }
    json!({
        "phase": if mi.is_some() { "running" } else { "done" },
        "root": project_scope(&inst.variables),
        "body": body,
        "children": Json::Object(children),
        "collected": collected,
        "rootResults": root_results,
    })
}

struct Replay {
    engine: Engine,
    key: Key,
    n: usize,
    /// Activated-but-uncompleted jobs per (job type, child item).
    jobs: BTreeMap<(&'static str, i64), Key>,
}

impl Replay {
    fn start(output_element: &str, n: usize) -> Self {
        let def = parse_bpmn(&process_xml(output_element))
            .expect("parse")
            .remove(0);
        let mut engine = Engine::new();
        engine.apply_command(Command::DeployProcess(def)).unwrap();
        let items = Value::List((1..=n as i64).map(Value::Int).collect());
        let key = engine
            .apply_command(Command::create_instance_with(
                "vs",
                HashMap::from([("items".to_string(), items)]),
            ))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .expect("instance created");
        let mut r = Replay {
            engine,
            key,
            n,
            jobs: BTreeMap::new(),
        };
        r.collect_jobs("first");
        r
    }

    /// Activate every available job of `ty`, keyed by its child's `item`.
    fn collect_jobs(&mut self, ty: &'static str) {
        for job in self.engine.activate_jobs(ty, "trace", 100, 60_000, 0) {
            let Some(Value::Int(item)) = job.variables.get("item") else {
                panic!("{ty} job without an integer item: {:?}", job.variables);
            };
            assert!(
                self.jobs.insert((ty, *item), job.key).is_none(),
                "duplicate {ty} job for child {item}"
            );
        }
    }

    fn complete(&mut self, ty: &'static str, child: i64, vars: HashMap<String, Value>) {
        let job = self
            .jobs
            .remove(&(ty, child))
            .unwrap_or_else(|| panic!("no activated {ty} job for child {child}"));
        self.engine
            .apply_command(Command::complete_job_with(job, vars))
            .unwrap_or_else(|e| panic!("complete {ty} for child {child}: {e:?}"));
    }

    fn drive(&mut self, action: &str, child: Option<i64>) {
        match action {
            // Engine-internal: fused with the command that enables it.
            "Activate" | "Complete" => {}
            "Write" => {
                let i = child.expect("Write names a child");
                let vars = HashMap::from([
                    ("result".to_string(), Value::Int(i)),
                    ("seen".to_string(), Value::Int(i)),
                ]);
                self.complete("first", i, vars);
                self.collect_jobs("second");
            }
            "Finish" => {
                let i = child.expect("Finish names a child");
                self.complete("second", i, HashMap::new());
            }
            other => panic!("unknown VariableScopes action {other}"),
        }
    }
}

fn replay(fixture: &Json) -> Result<(), String> {
    let model = fixture["model"].as_str().ok_or("fixture model")?;
    let config = &fixture["config"];
    let n = config["n"].as_u64().ok_or("config.n")? as usize;
    let output_element = config["outputElement"]
        .as_str()
        .ok_or("config.outputElement")?;
    if config["seedOutput"] != json!(true) {
        return Err(format!(
            "{model}: only seeded (Zeebe-parity) models are trace-anchored"
        ));
    }
    let steps = fixture["steps"].as_array().ok_or("fixture steps")?;
    if steps.is_empty() {
        return Err(format!("{model}: empty behaviour"));
    }
    let mut r = Replay::start(output_element, n);
    for (k, step) in steps.iter().enumerate() {
        let action = step["action"].as_str().ok_or("step action")?;
        r.drive(action, step["child"].as_i64());
        // Compare at the end of a fused run: skip a driven step that is
        // immediately followed by an engine-internal one.
        let next_internal = steps
            .get(k + 1)
            .and_then(|s| s["action"].as_str())
            .is_some_and(|a| a == "Activate" || a == "Complete");
        if next_internal {
            continue;
        }
        let got = engine_state(&r.engine, r.key, r.n);
        if got != step["state"] {
            return Err(format!(
                "{model}: step {k} ({action} {:?}) diverges\n  spec:   {}\n  engine: {got}",
                step.get("child"),
                step["state"]
            ));
        }
    }
    Ok(())
}

fn load_fixtures() -> Vec<(String, Json)> {
    let dir = traces_dir();
    let mut out: Vec<(String, Json)> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .map(|p| {
            let text = std::fs::read_to_string(&p).expect("read fixture");
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                serde_json::from_str(&text).expect("fixture is JSON"),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Every committed VariableScopes behaviour replays against the engine with
/// an identical variable tree at every step.
#[test]
fn variable_scopes_traces_conform_to_engine() {
    let fixtures = load_fixtures();
    assert!(
        fixtures.len() >= 3,
        "expected the committed VariableScopes trace fixtures, found {}",
        fixtures.len()
    );
    let failures: Vec<String> = fixtures
        .iter()
        .filter(|(_, f)| f["spec"] == json!("VariableScopes"))
        .filter_map(|(name, f)| replay(f).err().map(|e| format!("{name}: {e}")))
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The fixtures must exercise the #1518 shape: a sibling's write lands between
/// a child's write and its completion. Otherwise the replay would pass on an
/// engine that shares the output variable across children.
#[test]
fn variable_scopes_traces_interleave_siblings() {
    for (name, f) in load_fixtures() {
        let steps = f["steps"].as_array().unwrap();
        let mut pending: Vec<i64> = Vec::new();
        let mut overlapped = false;
        for s in steps {
            match (s["action"].as_str().unwrap(), s["child"].as_i64()) {
                ("Write", Some(i)) => {
                    overlapped |= !pending.is_empty();
                    pending.push(i);
                }
                ("Finish", Some(i)) => pending.retain(|x| *x != i),
                _ => {}
            }
        }
        assert!(overlapped, "{name}: no interleaved sibling writes");
    }
}

/// A replay must FAIL on a divergent fixture (the anchor has teeth): tamper
/// with the seeded model's collected slot and expect a mismatch.
#[test]
fn variable_scopes_replay_rejects_a_divergent_fixture() {
    let (_, mut f) = load_fixtures()
        .into_iter()
        .find(|(n, _)| n == "VS_MISeeded.json")
        .expect("VS_MISeeded fixture");
    let last = f["steps"].as_array_mut().unwrap().last_mut().unwrap();
    last["state"]["rootResults"] = json!([2, 2]);
    assert!(replay(&f).is_err(), "a tampered fixture must not replay");
}
