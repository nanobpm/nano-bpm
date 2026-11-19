//! **Conformance mining** — the intersection of the two surfaces ProcessOS already
//! owns: the *designed* BPMN model and the *actual* execution traces.
//!
//! - [`discover_flow`] mines the directly-follows graph straight from the `jobs` table
//!   (the model the data *implies*), with no reference to the BPMN at all.
//! - [`conformance_check`] replays that mined behaviour against the model's permitted
//!   task-to-task transitions ([`crate::bpmn_model::model_task_graph`]) and reports where
//!   reality and design diverge: nonconformant transitions, undocumented tasks, model
//!   paths never taken, and start/end deviations — plus a simple fitness score.
//!
//! Both tools work at the granularity of **service/user tasks**, because those are the
//! only model nodes that produce a `jobs` row. Gateways and events are collapsed away:
//! a model transition `A -> B` is "permitted" when `B` is reachable from `A` through a
//! path of only non-task nodes. Concurrency caveat: parallel branches are linearised in
//! capture (a single recorded `seq` order per instance), so a directly-follows edge
//! between two genuinely parallel tasks is an artefact of that linearisation, not a real
//! ordering — treat cross-branch transitions with care.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

use crate::analysis::Analysis;

/// Cap on how many rows of any one list we hand back to the model.
const LIST_CAP: usize = 60;

/// A mined directly-follows edge with its observed count.
struct Edge {
    from: String,
    to: String,
    count: i64,
}

fn cell(row: &[String], c: usize) -> &str {
    row.get(c).map(|s| s.as_str()).unwrap_or("")
}

fn as_i64(s: &str) -> i64 {
    s.parse::<i64>().unwrap_or(0)
}

/// Run an aggregating query and return its rows (column order as written in the SQL).
fn rows(analysis: &Analysis, sql: &str) -> Result<(Vec<Vec<String>>, bool), String> {
    let r = analysis.query(sql)?;
    Ok((r.rows, r.truncated))
}

/// SQL that mines the directly-follows pairs (`from`, `to`, `count`) over consecutive
/// task executions within each instance, ordered by the captured `seq`.
const DF_SQL: &str = r#"
WITH seq AS (
  SELECT instance_key, element_id,
         LEAD(element_id) OVER (PARTITION BY instance_key ORDER BY seq) AS next_el
  FROM jobs
)
SELECT element_id AS from_el, next_el AS to_el, COUNT(*) AS cnt
FROM seq WHERE next_el IS NOT NULL
GROUP BY 1, 2 ORDER BY cnt DESC"#;

const NODES_SQL: &str = r#"
SELECT element_id, COUNT(*) AS execs, COUNT(DISTINCT instance_key) AS insts
FROM jobs GROUP BY 1 ORDER BY execs DESC"#;

/// First / last observed task per instance ('start' / 'end' position).
const ENDS_SQL: &str = r#"
WITH r AS (
  SELECT instance_key, element_id,
         ROW_NUMBER() OVER (PARTITION BY instance_key ORDER BY seq) AS rn,
         COUNT(*) OVER (PARTITION BY instance_key) AS tot
  FROM jobs
)
SELECT 'start' AS pos, element_id, COUNT(*) AS cnt FROM r WHERE rn = 1 GROUP BY element_id
 UNION ALL
SELECT 'end' AS pos, element_id, COUNT(*) AS cnt FROM r WHERE rn = tot GROUP BY element_id
ORDER BY cnt DESC"#;

/// Mine the directly-follows graph implied by the trace, independent of any model.
pub fn discover_flow(analysis: &Analysis) -> Result<Value, String> {
    let edges = mine_edges(analysis)?;
    let (node_rows, node_trunc) = rows(analysis, NODES_SQL)?;
    let (end_rows, _) = rows(analysis, ENDS_SQL)?;

    let total_occurrences: i64 = edges.iter().map(|e| e.count).sum();
    let nodes: Vec<Value> = node_rows
        .iter()
        .map(|row| {
            json!({
                "elementId": cell(row, 0),
                "executions": as_i64(cell(row, 1)),
                "instances": as_i64(cell(row, 2)),
            })
        })
        .collect();

    let edge_json: Vec<Value> = edges
        .iter()
        .take(LIST_CAP)
        .map(|e| {
            json!({
                "from": e.from,
                "to": e.to,
                "count": e.count,
                "pctOfTransitions": pct(e.count, total_occurrences),
            })
        })
        .collect();

    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for row in &end_rows {
        let pos = cell(row, 0);
        let entry = json!({
            "elementId": cell(row, 1),
            "count": as_i64(cell(row, 2)),
        });
        if pos == "start" {
            starts.push(entry);
        } else {
            ends.push(entry);
        }
    }

    Ok(json!({
        "kind": "directly-follows-graph",
        "note": "Mined purely from the trace (no model). Nodes are service/user tasks; an \
                 edge A->B means B's job ran immediately after A's within an instance, ordered \
                 by capture seq. Parallel branches are linearised, so cross-branch edges are \
                 artefacts.",
        "totals": {
            "distinctTasks": node_rows.len(),
            "distinctTransitions": edges.len(),
            "transitionOccurrences": total_occurrences,
        },
        "nodes": nodes,
        "nodesTruncated": node_trunc,
        "edges": edge_json,
        "edgesTruncated": edges.len() > LIST_CAP,
        "observedStarts": starts,
        "observedEnds": ends,
    }))
}

/// Replay the mined behaviour against the model and report divergences.
pub fn conformance_check(analysis: &Analysis, model_xml: &str) -> Result<Value, String> {
    let mtg = crate::bpmn_model::model_task_graph(model_xml)?;
    let edges = mine_edges(analysis)?;

    // Observed task -> total executions (for undocumented-task reporting).
    let (node_rows, _) = rows(analysis, NODES_SQL)?;
    let mut observed_execs: BTreeMap<String, i64> = BTreeMap::new();
    for row in &node_rows {
        observed_execs.insert(cell(row, 0).to_string(), as_i64(cell(row, 1)));
    }

    let total_occurrences: i64 = edges.iter().map(|e| e.count).sum();
    let mut conformant_occurrences = 0i64;
    let mut conformant_edges = 0usize;
    let mut nonconformant: Vec<Value> = Vec::new();
    let observed_pairs: BTreeSet<(String, String)> = edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();

    for e in &edges {
        let reason = if !mtg.tasks.contains(&e.from) {
            Some("source task is not in the model")
        } else if !mtg.tasks.contains(&e.to) {
            Some("target task is not in the model")
        } else if !mtg
            .allowed
            .get(&e.from)
            .map(|s| s.contains(&e.to))
            .unwrap_or(false)
        {
            Some("the model permits no task-path from source to target")
        } else {
            None
        };
        match reason {
            None => {
                conformant_occurrences += e.count;
                conformant_edges += 1;
            }
            Some(r) => nonconformant.push(json!({
                "from": e.from,
                "to": e.to,
                "count": e.count,
                "pctOfTransitions": pct(e.count, total_occurrences),
                "reason": r,
            })),
        }
    }
    // Worst offenders first.
    nonconformant.sort_by(|a, b| b["count"].as_i64().cmp(&a["count"].as_i64()));

    // Tasks executed at runtime that the model never declares.
    let mut undocumented: Vec<Value> = observed_execs
        .iter()
        .filter(|(id, _)| !mtg.tasks.contains(*id))
        .map(|(id, execs)| json!({ "elementId": id, "executions": execs }))
        .collect();
    undocumented.sort_by(|a, b| b["executions"].as_i64().cmp(&a["executions"].as_i64()));

    // Model transitions that the trace never exercised (dead model paths).
    let mut unused: Vec<Value> = Vec::new();
    let mut allowed_from: Vec<(&String, &BTreeSet<String>)> = mtg.allowed.iter().collect();
    allowed_from.sort_by(|a, b| a.0.cmp(b.0));
    for (from, tos) in allowed_from {
        let mut tos_sorted: Vec<&String> = tos.iter().collect();
        tos_sorted.sort();
        for to in tos_sorted {
            if !observed_pairs.contains(&(from.clone(), to.clone())) {
                unused.push(json!({ "from": from, "to": to }));
            }
        }
    }

    // Start / end deviations: instances that opened or closed on a task the model
    // does not list as a legal opening / closing task.
    let (end_rows, _) = rows(analysis, ENDS_SQL)?;
    let mut start_dev: Vec<Value> = Vec::new();
    let mut end_dev: Vec<Value> = Vec::new();
    for row in &end_rows {
        let pos = cell(row, 0);
        let el = cell(row, 1).to_string();
        let cnt = as_i64(cell(row, 2));
        if pos == "start" && mtg.tasks.contains(&el) && !mtg.start_tasks.contains(&el) {
            start_dev.push(json!({ "elementId": el, "count": cnt }));
        } else if pos == "end" && mtg.tasks.contains(&el) && !mtg.end_tasks.contains(&el) {
            end_dev.push(json!({ "elementId": el, "count": cnt }));
        }
    }

    let fitness = if total_occurrences > 0 {
        conformant_occurrences as f64 / total_occurrences as f64
    } else {
        1.0
    };

    Ok(json!({
        "kind": "conformance-report",
        "processId": mtg.process_id,
        "note": "Fitness is the share of observed task-to-task transitions the model permits. \
                 Nonconformant transitions, undocumented tasks and start/end deviations are \
                 where reality diverges from the design; unused model transitions are designed \
                 paths the trace never took. Tasks = service/user tasks (the only nodes traced).",
        "fitness": {
            "transitionFitness": (fitness * 1000.0).round() / 1000.0,
            "transitionOccurrences": total_occurrences,
            "conformantOccurrences": conformant_occurrences,
            "distinctTransitions": edges.len(),
            "conformantTransitions": conformant_edges,
            "nonconformantTransitions": nonconformant.len(),
            "modelTaskCount": mtg.tasks.len(),
            "observedTaskCount": observed_execs.len(),
        },
        "nonconformantTransitions": capped(nonconformant),
        "undocumentedTasks": capped(undocumented),
        "unusedModelTransitions": capped(unused),
        "startDeviations": start_dev,
        "endDeviations": end_dev,
    }))
}

/// Run [`DF_SQL`] and collect the typed edges.
fn mine_edges(analysis: &Analysis) -> Result<Vec<Edge>, String> {
    let (edge_rows, _) = rows(analysis, DF_SQL)?;
    Ok(edge_rows
        .iter()
        .map(|row| Edge {
            from: cell(row, 0).to_string(),
            to: cell(row, 1).to_string(),
            count: as_i64(cell(row, 2)),
        })
        .collect())
}

fn pct(part: i64, whole: i64) -> f64 {
    if whole <= 0 {
        0.0
    } else {
        ((part as f64 / whole as f64) * 1000.0).round() / 10.0
    }
}

/// Cap a list, appending a sentinel object when truncated so the model knows there's more.
fn capped(mut v: Vec<Value>) -> Value {
    if v.len() > LIST_CAP {
        let extra = v.len() - LIST_CAP;
        v.truncate(LIST_CAP);
        v.push(json!({ "_truncated": format!("{extra} more omitted") }));
    }
    Value::Array(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{Element, InstanceTrace, Job};

    // start -> CreditCheck -> XOR -> {Approve | Reject} -> end. Three service tasks.
    const LOAN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="defs">
  <bpmn:process id="loan-approval" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="CreditCheck">
      <bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements>
      <bpmn:incoming>f0</bpmn:incoming><bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="Decision" default="f3">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:serviceTask id="Approve">
      <bpmn:extensionElements><zeebe:taskDefinition type="approve-loan"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Reject">
      <bpmn:extensionElements><zeebe:taskDefinition type="reject-application"/></bpmn:extensionElements>
      <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f5</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="EndApproved"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="EndRejected"><bpmn:incoming>f5</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f0" sourceRef="Start" targetRef="CreditCheck"/>
    <bpmn:sequenceFlow id="f1" sourceRef="CreditCheck" targetRef="Decision"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Decision" targetRef="Approve">
      <bpmn:conditionExpression>= creditScore &gt;= 700</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f3" sourceRef="Decision" targetRef="Reject"/>
    <bpmn:sequenceFlow id="f4" sourceRef="Approve" targetRef="EndApproved"/>
    <bpmn:sequenceFlow id="f5" sourceRef="Reject" targetRef="EndRejected"/>
  </bpmn:process>
</bpmn:definitions>"#;

    /// One instance whose `elements` carry a job per given (element_id, job_type), in order.
    fn inst(key: &str, steps: &[(&str, &str)]) -> InstanceTrace {
        InstanceTrace {
            instance_key: key.into(),
            process_id: "loan-approval".into(),
            version: Some(1),
            outcome: "COMPLETED".into(),
            started_at: 1_000,
            duration_ms: Some(10),
            elements: steps
                .iter()
                .map(|(el, jt)| Element {
                    element_id: (*el).into(),
                    duration_ms: Some(1),
                    incidents: 0,
                    job: Some(Job {
                        job_type: (*jt).into(),
                        queue_ms: Some(0),
                        service_ms: Some(1),
                        failures: 0,
                    }),
                })
                .collect(),
            incidents: vec![],
            creation_variables: None,
            stimuli: None,
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        }
    }

    fn fixture() -> Analysis {
        let mut traces = Vec::new();
        // 8 conformant CreditCheck -> Approve
        for i in 0..8 {
            traces.push(inst(
                &format!("a{i}"),
                &[("CreditCheck", "credit-check"), ("Approve", "approve-loan")],
            ));
        }
        // 2 conformant CreditCheck -> Reject
        for i in 0..2 {
            traces.push(inst(
                &format!("r{i}"),
                &[
                    ("CreditCheck", "credit-check"),
                    ("Reject", "reject-application"),
                ],
            ));
        }
        // 1 nonconformant: Approve runs before CreditCheck (model permits no such path)
        traces.push(inst(
            "bad",
            &[("Approve", "approve-loan"), ("CreditCheck", "credit-check")],
        ));
        // 1 undocumented task 'Fraud' not in the model
        traces.push(inst(
            "u0",
            &[("CreditCheck", "credit-check"), ("Fraud", "fraud-screen")],
        ));
        Analysis::build(&traces).expect("build analysis")
    }

    #[test]
    fn discover_flow_mines_transitions_in_capture_order() {
        let a = fixture();
        let v = discover_flow(&a).unwrap();
        let edges = v["edges"].as_array().unwrap();
        let cc_approve = edges
            .iter()
            .find(|e| e["from"] == "CreditCheck" && e["to"] == "Approve")
            .expect("CreditCheck->Approve mined");
        assert_eq!(cc_approve["count"], 8);
        // The reversed instance produced an Approve->CreditCheck edge (order respected).
        assert!(edges
            .iter()
            .any(|e| e["from"] == "Approve" && e["to"] == "CreditCheck"));
    }

    #[test]
    fn conformance_check_flags_divergences_and_scores_fitness() {
        let a = fixture();
        let v = conformance_check(&a, LOAN_BPMN).unwrap();
        // 12 instances, each one transition = 12 occurrences; 2 of them nonconformant
        // (Approve->CreditCheck and CreditCheck->Fraud) => fitness 10/12 ~= 0.833.
        assert_eq!(v["fitness"]["transitionOccurrences"], 12);
        assert_eq!(v["fitness"]["conformantOccurrences"], 10);
        let nc = v["nonconformantTransitions"].as_array().unwrap();
        assert!(nc
            .iter()
            .any(|e| e["from"] == "Approve" && e["to"] == "CreditCheck"));
        // Fraud is undocumented; the CreditCheck->Fraud edge is nonconformant (target absent).
        let undoc = v["undocumentedTasks"].as_array().unwrap();
        assert!(undoc_contains(undoc, "Fraud"));
    }

    fn undoc_contains(arr: &[Value], id: &str) -> bool {
        arr.iter().any(|e| e["elementId"] == id)
    }
}
