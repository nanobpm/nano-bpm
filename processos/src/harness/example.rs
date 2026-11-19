//! A self-contained example scenario used by the `/api/harness/example*`
//! endpoints and the harness tests. It encodes a real optimization tradeoff so
//! the rig has something meaningful to discover:
//!
//! Process: **Classify → Summarize** (two service tasks).
//!
//! * `classify` — `accurate-llm` (default: dear, slow, correct) vs `cheap-llm`
//!   (cheap, fast, *equally correct here*). Swapping to `cheap-llm` is a pure win.
//! * `summarize` — `premium-llm` (default: dear, slow, reliable) vs `budget-llm`
//!   (cheap, fast, but fails ~half the time → incidents). Swapping it is a trap.
//!
//! The golden variant is therefore `{classify: cheap-llm, summarize: premium-llm}`:
//! the cheapest fully-correct, incident-free configuration. A correct ranking
//! recovers exactly that.

use std::collections::BTreeMap;

use serde_json::json;

use super::{Objective, Scenario, ScenarioInput, Variant, WorkerModel};

/// The Classify → Summarize test model (no DI; `engine-core` parses this directly).
const EXAMPLE_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  id="Definitions_processos_demo"
                  targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="ClassifySummarize" name="Classify and Summarize" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Classify" />
    <bpmn:serviceTask id="Classify" name="Classify">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="classify" />
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Classify" targetRef="Summarize" />
    <bpmn:serviceTask id="Summarize" name="Summarize">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="summarize" />
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Summarize" targetRef="End" />
    <bpmn:endEvent id="End">
      <bpmn:incoming>f3</bpmn:incoming>
    </bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

fn worker(
    id: &str,
    cost: f64,
    latency_ms: u64,
    failure_rate: f64,
    output: BTreeMap<String, serde_json::Value>,
) -> WorkerModel {
    WorkerModel {
        id: id.to_string(),
        cost,
        latency_ms,
        failure_rate,
        output,
    }
}

/// Build the example scenario.
pub fn example_scenario() -> Scenario {
    let mut workers: BTreeMap<String, WorkerModel> = BTreeMap::new();
    // classify options — both correct; cheap is strictly better.
    workers.insert(
        "accurate-llm".into(),
        worker(
            "accurate-llm",
            10.0,
            1200,
            0.0,
            BTreeMap::from([("category".to_string(), json!("A"))]),
        ),
    );
    workers.insert(
        "cheap-llm".into(),
        worker(
            "cheap-llm",
            1.0,
            200,
            0.0,
            BTreeMap::from([("category".to_string(), json!("A"))]),
        ),
    );
    // summarize options — premium reliable; budget fails half the time.
    workers.insert(
        "premium-llm".into(),
        worker(
            "premium-llm",
            8.0,
            900,
            0.0,
            BTreeMap::from([("summary".to_string(), json!("ok"))]),
        ),
    );
    workers.insert(
        "budget-llm".into(),
        worker(
            "budget-llm",
            2.0,
            300,
            0.5,
            BTreeMap::from([("summary".to_string(), json!("ok"))]),
        ),
    );

    let task_workers: BTreeMap<String, String> = BTreeMap::from([
        ("classify".to_string(), "accurate-llm".to_string()),
        ("summarize".to_string(), "premium-llm".to_string()),
    ]);

    let latent_options: BTreeMap<String, Vec<String>> = BTreeMap::from([
        ("classify".to_string(), vec!["cheap-llm".to_string()]),
        ("summarize".to_string(), vec!["budget-llm".to_string()]),
    ]);

    // A handful of inputs; expected output is the same correct answer each time.
    let inputs: Vec<ScenarioInput> = (0..6)
        .map(|i| ScenarioInput {
            vars: BTreeMap::from([("docId".to_string(), json!(format!("doc-{i}")))]),
            expected: BTreeMap::from([
                ("category".to_string(), json!("A")),
                ("summary".to_string(), json!("ok")),
            ]),
        })
        .collect();

    let golden = Some(Variant {
        name: "golden (cheap classify, premium summarize)".to_string(),
        assignment: BTreeMap::from([
            ("classify".to_string(), "cheap-llm".to_string()),
            ("summarize".to_string(), "premium-llm".to_string()),
        ]),
    });

    Scenario {
        name: "Classify+Summarize worker-swap demo".to_string(),
        test_model: EXAMPLE_BPMN.to_string(),
        process_id: Some("ClassifySummarize".to_string()),
        workers,
        task_workers,
        latent_options,
        inputs,
        golden,
        objective: Objective::default(),
        seed: 42,
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::bpmn::parse_bpmn;

    use super::*;

    #[test]
    fn example_bpmn_parses() {
        let defs = parse_bpmn(EXAMPLE_BPMN).expect("example BPMN parses");
        assert_eq!(defs[0].id, "ClassifySummarize");
    }
}
