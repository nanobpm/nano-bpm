//! The ZeebeTokenFlow spec family's [`TraceMapping`]: how the shared
//! Zeebe-exporter record vocabulary binds to the spec's observable milestones
//! (#1259, epic #1240).
//!
//! This is the **spec-level normaliser**: it maps the *record-level* normalised
//! stream (the output of `formal/traces/normaliser.json`, applied by both
//! `Normaliser.java` on the Zeebe capture side and `normalise()` in
//! `engine-core/tests/zeebe_traces.rs` on the nano side) onto the
//! `ZeebeTokenFlow.tla` milestone vocabulary. It reuses the #1226 `TraceMapping`
//! trait shape and the `Milestone` vocabulary, but its input is the normalised
//! record stream — not the engine's `Event` stream — so the SAME projection
//! validates a captured Zeebe golden and a nano replay against the spec.
//!
//! The mapping is grounded in `ZeebeTokenFlow.tla`'s vocabulary-rule header:
//!
//! | Spec variable | Exporter record | Milestone |
//! |---|---|---|
//! | `taken` (per sequence flow) | `SEQUENCE_FLOW_TAKEN` | `Flow { from, to }` |
//! | `active` (task wait states) | `ELEMENT_ACTIVATED` of a task | `Task { node }` |
//! | `fired` (gateway activations) | `ELEMENT_ACTIVATED` of a gateway | `JoinFired { node }` |
//! | `done` (instance completion) | `ELEMENT_COMPLETED` of the process root | `Completed` |
//!
//! The `from`/`to` endpoints of a `Flow` milestone are resolved through the
//! scenario's sequence-flow map (`(source, target) -> flow id`), which the
//! loader derives from the BPMN XML. A `SEQUENCE_FLOW_TAKEN` record names only
//! the flow's `elementId`; the endpoints are the graph identity the spec
//! reasons about.
//!
//! This module is included (via `#[path]`) by several integration-test targets
//! (`zeebe_traces.rs`, `zeebe_token_flow_trace_validation.rs`). Each target uses
//! a different subset of its surface: `zeebe_traces.rs` uses only the
//! record-projection functions and never constructs `ZeebeTokenFlowMapping`,
//! while `zeebe_token_flow_trace_validation.rs` uses the mapping and not the
//! projection. Allow `dead_code` so a target exercising only one half does not
//! trip `dead_code` under `-D warnings` (the same pattern `harness.rs` uses for
//! this shared-module shape).
#![allow(dead_code)]

use std::collections::HashMap;

use nanobpmn_engine_core::{Event, ProcessDefinition};
use serde_json::Value as J;

use super::harness::{Fixture, Milestone, TraceMapping};

/// The Zeebe `BpmnElementType` names that activate as a task wait state (a job
/// the harness drives to completion). Everything else that activates is a
/// pass-through (start/end event, gateway) or the process root.
const TASK_ELEMENT_TYPES: &[&str] = &[
    "SERVICE_TASK",
    "SEND_TASK",
    "RECEIVE_TASK",
    "USER_TASK",
    "BUSINESS_RULE_TASK",
    "SCRIPT_TASK",
    "MANUAL_TASK",
    "TASK",
    "CALL_ACTIVITY",
];

/// The Zeebe `BpmnElementType` names whose `ELEMENT_ACTIVATED` can be the
/// spec's `fired` ghost increment — i.e. the gateway kinds `ZeebeTokenFlow.tla`
/// treats as joins. `IsJoin(n)` is `Kind[n] ∈ {"and","or"} ∧ |In(n)| > 1`, so
/// only PARALLEL (`and`) and INCLUSIVE (`or`) gateways ever fire a join. An
/// EXCLUSIVE (`xor`) or EVENT_BASED merge passes each arriving token straight
/// through without incrementing `fired`, so it is NOT join-eligible even when
/// it has multiple incoming flows — keeping those types out of this set is what
/// stops a multi-input exclusive merge from emitting a spurious `JoinFired`.
const JOIN_GATEWAY_ELEMENT_TYPES: &[&str] = &["PARALLEL_GATEWAY", "INCLUSIVE_GATEWAY"];

/// Project one normalised record onto the ZeebeTokenFlow milestone vocabulary.
/// Returns `None` for a record that carries no milestone (commands, rejections,
/// lifecycle records of pass-through elements, non-lane records).
///
/// `flow_endpoints` maps a sequence-flow id to its `(source, target)` element
/// ids — the reverse of the scenario loader's `flows` map. A `SEQUENCE_FLOW_TAKEN`
/// record names only the flow id; the endpoints are what the spec observes.
///
/// `join_elements` is the set of element ids that are joins (gateways with
/// multiple incoming flows). Only a join's `ELEMENT_ACTIVATED` is a `JoinFired`
/// milestone; a split gateway's activation is not a token-flow milestone (it
/// is a pass-through that takes its outgoing flows).
pub fn record_milestone(
    record: &J,
    flow_endpoints: &HashMap<String, (String, String)>,
    join_elements: &std::collections::HashSet<String>,
) -> Option<Milestone> {
    let value_type = record.get("valueType")?.as_str()?;
    let intent = record.get("intent")?.as_str()?;

    match (value_type, intent) {
        ("PROCESS_INSTANCE", "SEQUENCE_FLOW_TAKEN") => {
            let flow_id = record.get("elementId")?.as_str()?;
            let (from, to) = flow_endpoints.get(flow_id)?.clone();
            Some(Milestone::Flow { from, to })
        }
        ("PROCESS_INSTANCE", "ELEMENT_ACTIVATED") => {
            let element_id = record.get("elementId")?.as_str()?;
            let element_type = record.get("bpmnElementType")?.as_str()?;
            if TASK_ELEMENT_TYPES.contains(&element_type) {
                Some(Milestone::Task {
                    node: element_id.to_string(),
                })
            } else if JOIN_GATEWAY_ELEMENT_TYPES.contains(&element_type)
                && join_elements.contains(element_id)
            {
                Some(Milestone::JoinFired {
                    node: element_id.to_string(),
                })
            } else {
                None
            }
        }
        ("PROCESS_INSTANCE", "ELEMENT_COMPLETED") => {
            let element_type = record.get("bpmnElementType")?.as_str()?;
            if element_type == "PROCESS" {
                Some(Milestone::Completed)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Project a normalised record stream (the output of `normalise()` applied to
/// one scenario's steps) onto the ZeebeTokenFlow milestone multiset. The
/// records of every step are concatenated in order; the result is the
/// unordered multiset the spec's shortest completing behaviour must equal.
///
/// `flow_endpoints` maps a sequence-flow id to its `(source, target)` element
/// ids, derived from the scenario's BPMN XML. `join_elements` is the set of
/// element ids that are joins (gateways with multiple incoming flows).
pub fn normalised_records_to_milestones(
    steps: &[J],
    flow_endpoints: &HashMap<String, (String, String)>,
    join_elements: &std::collections::HashSet<String>,
) -> Vec<Milestone> {
    let mut out = Vec::new();
    for step in steps {
        let Some(records) = step.get("records").and_then(J::as_array) else {
            continue;
        };
        for record in records {
            if let Some(m) = record_milestone(record, flow_endpoints, join_elements) {
                out.push(m);
            }
        }
    }
    out
}

/// The ZeebeTokenFlow spec family's [`TraceMapping`] binding.
///
/// Unlike `TokenFlowMapping` (which projects the engine's `Event` stream), this
/// mapping is used in two directions:
///
/// 1. **Engine replay**: `build_process` + `engine_milestones` drive the real
///    engine and project its events onto the milestone vocabulary, exactly as
///    `TokenFlowMapping` does. This anchors the spec to nano.
/// 2. **Spec-level normalisation**: `normalised_records_to_milestones` projects
///    a *captured Zeebe golden* (or a nano replay rendered as normalised
///    records) onto the same vocabulary. This anchors the spec to Zeebe.
///
/// Both directions share the `Milestone` vocabulary and the `TraceMapping`
/// trait shape, so the harness's `validate` driver is reused unchanged.
pub struct ZeebeTokenFlowMapping;

impl TraceMapping for ZeebeTokenFlowMapping {
    fn process_id(&self, fixture: &Fixture) -> String {
        fixture.model.clone()
    }

    fn build_process(&self, fixture: &Fixture) -> ProcessDefinition {
        // The ZeebeTokenFlow graph vocabulary is the same as TokenFlow's
        // (`start`/`end`/`task`/`and`/`or`/`xor`), so the process construction
        // is the canonical shared one (`harness::build_spec_process`). The
        // difference is the *oracle*: ZeebeTokenFlow is the reference
        // semantics, not nano's.
        super::harness::build_spec_process(fixture)
    }

    fn engine_milestones(&self, events: &[Event]) -> Vec<Milestone> {
        // The engine-side projection is the canonical shared one
        // (`harness::spec_engine_milestones`): the engine's `Event` stream
        // carries the same observable facts (SequenceFlowTaken, JobCreated,
        // ParallelJoinFired, ProcessInstanceCompleted). The ZeebeTokenFlow spec
        // is the *reference* for these facts, not a different vocabulary.
        super::harness::spec_engine_milestones(events)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn flow_map() -> HashMap<String, (String, String)> {
        [
            ("f1".to_string(), ("S".to_string(), "P1".to_string())),
            ("f2".to_string(), ("P1".to_string(), "A".to_string())),
            ("f3".to_string(), ("P1".to_string(), "B".to_string())),
            ("f4".to_string(), ("A".to_string(), "P2".to_string())),
            ("f5".to_string(), ("B".to_string(), "P2".to_string())),
            ("f6".to_string(), ("P2".to_string(), "E".to_string())),
        ]
        .into_iter()
        .collect()
    }

    fn join_elements() -> std::collections::HashSet<String> {
        ["P2".to_string()].into_iter().collect()
    }

    #[test]
    fn sequence_flow_taken_maps_to_flow_milestone() {
        let record = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "SEQUENCE_FLOW_TAKEN",
            "elementId": "f1",
            "bpmnElementType": "SEQUENCE_FLOW",
            "processInstanceKey": 1,
            "flowScopeKey": 1
        });
        let m = record_milestone(&record, &flow_map(), &join_elements()).expect("a flow milestone");
        assert_eq!(
            m,
            Milestone::Flow {
                from: "S".to_string(),
                to: "P1".to_string()
            }
        );
    }

    #[test]
    fn task_activation_maps_to_task_milestone() {
        let record = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "ELEMENT_ACTIVATED",
            "elementId": "A",
            "bpmnElementType": "SERVICE_TASK",
            "processInstanceKey": 1,
            "flowScopeKey": 1
        });
        let m = record_milestone(&record, &flow_map(), &join_elements()).expect("a task milestone");
        assert_eq!(
            m,
            Milestone::Task {
                node: "A".to_string()
            }
        );
    }

    #[test]
    fn gateway_activation_maps_to_join_fired_milestone() {
        let record = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "ELEMENT_ACTIVATED",
            "elementId": "P2",
            "bpmnElementType": "PARALLEL_GATEWAY",
            "processInstanceKey": 1,
            "flowScopeKey": 1
        });
        let m = record_milestone(&record, &flow_map(), &join_elements())
            .expect("a join-fired milestone");
        assert_eq!(
            m,
            Milestone::JoinFired {
                node: "P2".to_string()
            }
        );
    }

    #[test]
    fn split_gateway_activation_carries_no_milestone() {
        // P1 is a parallel SPLIT (one incoming flow, two outgoing), not a join.
        let record = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "ELEMENT_ACTIVATED",
            "elementId": "P1",
            "bpmnElementType": "PARALLEL_GATEWAY",
            "processInstanceKey": 1,
            "flowScopeKey": 1
        });
        assert!(record_milestone(&record, &flow_map(), &join_elements()).is_none());
    }

    #[test]
    fn exclusive_merge_activation_carries_no_milestone() {
        // A multi-input EXCLUSIVE merge is never a join: `ZeebeTokenFlow.tla`'s
        // `IsJoin(n)` is `Kind[n] ∈ {"and","or"} ∧ |In(n)| > 1`, so `xor` is
        // excluded — each arriving token passes straight through without
        // incrementing `fired`. Even when the incoming-flow-count rule flags the
        // element as a "join", its EXCLUSIVE_GATEWAY type must suppress the
        // spurious `JoinFired` (regression for the exclusive-merge false
        // positive: the type guard in the caller's `join_elements` keeps `X`
        // out, so `record_milestone` returns `None`).
        let record = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "ELEMENT_ACTIVATED",
            "elementId": "X",
            "bpmnElementType": "EXCLUSIVE_GATEWAY",
            "processInstanceKey": 1,
            "flowScopeKey": 1
        });
        // `X` is NOT in `join_elements()` (the caller's type guard excludes
        // exclusive gateways), so no JoinFired is emitted.
        assert!(record_milestone(&record, &flow_map(), &join_elements()).is_none());

        // Defence in depth: even if a buggy caller DID admit an exclusive
        // gateway into `join_elements`, the projection must still refuse to
        // fire a join for it — `record_milestone` only fires for the gateway
        // types the spec treats as joins (`JOIN_GATEWAY_ELEMENT_TYPES`:
        // parallel/inclusive), and an exclusive gateway is not one.
        let mut with_xor = join_elements();
        with_xor.insert("X".to_string());
        assert!(record_milestone(&record, &flow_map(), &with_xor).is_none());
    }

    #[test]
    fn process_completion_maps_to_completed_milestone() {
        let record = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "ELEMENT_COMPLETED",
            "elementId": "ParallelDiamond",
            "bpmnElementType": "PROCESS",
            "processInstanceKey": 1,
            "flowScopeKey": -1
        });
        let m = record_milestone(&record, &flow_map(), &join_elements())
            .expect("a completed milestone");
        assert_eq!(m, Milestone::Completed);
    }

    #[test]
    fn pass_through_lifecycle_records_carry_no_milestone() {
        // A start event's ACTIVATED is not a task wait state.
        let start_activated = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "ELEMENT_ACTIVATED",
            "elementId": "S",
            "bpmnElementType": "START_EVENT",
            "processInstanceKey": 1,
            "flowScopeKey": 1
        });
        assert!(record_milestone(&start_activated, &flow_map(), &join_elements()).is_none());

        // An end event's COMPLETED is not the process completion.
        let end_completed = json!({
            "valueType": "PROCESS_INSTANCE",
            "intent": "ELEMENT_COMPLETED",
            "elementId": "E",
            "bpmnElementType": "END_EVENT",
            "processInstanceKey": 1,
            "flowScopeKey": 1
        });
        assert!(record_milestone(&end_completed, &flow_map(), &join_elements()).is_none());

        // A JOB record is not a token-flow milestone.
        let job_created = json!({
            "valueType": "JOB",
            "intent": "CREATED",
            "elementId": "A",
            "type": "A",
            "retries": 3,
            "processInstanceKey": 1,
            "elementInstanceKey": 2
        });
        assert!(record_milestone(&job_created, &flow_map(), &join_elements()).is_none());

        // A command rejection is not a token-flow milestone.
        let rejection = json!({
            "recordType": "COMMAND_REJECTION",
            "valueType": "PROCESS_INSTANCE",
            "intent": "ACTIVATE_ELEMENT",
            "rejectionType": "INVALID_STATE"
        });
        assert!(record_milestone(&rejection, &flow_map(), &join_elements()).is_none());
    }

    #[test]
    fn full_stream_projects_to_milestone_multiset() {
        let steps = vec![
            json!({
                "step": 0,
                "op": "createInstance",
                "records": [
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATING", "elementId": "ParallelDiamond", "bpmnElementType": "PROCESS", "processInstanceKey": 1, "flowScopeKey": -1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "ParallelDiamond", "bpmnElementType": "PROCESS", "processInstanceKey": 1, "flowScopeKey": -1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "SEQUENCE_FLOW_TAKEN", "elementId": "f1", "bpmnElementType": "SEQUENCE_FLOW", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "P1", "bpmnElementType": "PARALLEL_GATEWAY", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "SEQUENCE_FLOW_TAKEN", "elementId": "f2", "bpmnElementType": "SEQUENCE_FLOW", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "SEQUENCE_FLOW_TAKEN", "elementId": "f3", "bpmnElementType": "SEQUENCE_FLOW", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "A", "bpmnElementType": "SERVICE_TASK", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "B", "bpmnElementType": "SERVICE_TASK", "processInstanceKey": 1, "flowScopeKey": 1},
                ]
            }),
            json!({
                "step": 1,
                "op": "completeJob",
                "records": [
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_COMPLETED", "elementId": "A", "bpmnElementType": "SERVICE_TASK", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "SEQUENCE_FLOW_TAKEN", "elementId": "f4", "bpmnElementType": "SEQUENCE_FLOW", "processInstanceKey": 1, "flowScopeKey": 1},
                ]
            }),
            json!({
                "step": 2,
                "op": "completeJob",
                "records": [
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_COMPLETED", "elementId": "B", "bpmnElementType": "SERVICE_TASK", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "SEQUENCE_FLOW_TAKEN", "elementId": "f5", "bpmnElementType": "SEQUENCE_FLOW", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_ACTIVATED", "elementId": "P2", "bpmnElementType": "PARALLEL_GATEWAY", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "SEQUENCE_FLOW_TAKEN", "elementId": "f6", "bpmnElementType": "SEQUENCE_FLOW", "processInstanceKey": 1, "flowScopeKey": 1},
                    {"valueType": "PROCESS_INSTANCE", "intent": "ELEMENT_COMPLETED", "elementId": "ParallelDiamond", "bpmnElementType": "PROCESS", "processInstanceKey": 1, "flowScopeKey": -1},
                ]
            }),
        ];
        let mut milestones =
            normalised_records_to_milestones(&steps, &flow_map(), &join_elements());
        milestones.sort();
        let mut expected = vec![
            Milestone::Completed,
            Milestone::Flow {
                from: "A".to_string(),
                to: "P2".to_string(),
            },
            Milestone::Flow {
                from: "B".to_string(),
                to: "P2".to_string(),
            },
            Milestone::Flow {
                from: "P1".to_string(),
                to: "A".to_string(),
            },
            Milestone::Flow {
                from: "P1".to_string(),
                to: "B".to_string(),
            },
            Milestone::Flow {
                from: "P2".to_string(),
                to: "E".to_string(),
            },
            Milestone::Flow {
                from: "S".to_string(),
                to: "P1".to_string(),
            },
            Milestone::JoinFired {
                node: "P2".to_string(),
            },
            Milestone::Task {
                node: "A".to_string(),
            },
            Milestone::Task {
                node: "B".to_string(),
            },
        ];
        expected.sort();
        assert_eq!(milestones, expected);
    }
}
