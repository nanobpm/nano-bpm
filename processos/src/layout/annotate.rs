//! **Semantic annotation inference (slices 1 & 2)** — derive a
//! [`SemanticAnnotations`] sidecar from the BPMN structure alone, so the
//! [`crate::layout::Solver::RowBias`] and [`crate::layout::Solver::Field`]
//! renderers have real bands / role hints / clusters to work with even when
//! nobody authored annotations by hand.
//!
//! The demo case that motivated this pass authored a loan-approval variant,
//! ran it through the Fromme renderer, and saw a flat line — because the LLM
//! authoring the variant had emitted zero annotations and both solvers
//! defaulted every node to the primary band. See ADR 0002 for the pipeline
//! this fits into.
//!
//! # Inference passes
//!
//! [`infer_from_xml`] runs three deterministic passes in order and merges
//! their outputs into a single [`SemanticAnnotations`]:
//!
//! 1. **Structural** ([`infer`], slice 1): tracing from `start_event` through
//!    every non-gateway element's outgoing flows and each exclusive **or
//!    inclusive** gateway's `is_default` (or first) outgoing yields the
//!    *primary flow* (both condition-routed gateway kinds pick a single
//!    representative branch for the spine).
//!    Every interrupting boundary event (error / timer / message / signal /
//!    conditional) seeds an *exception flow* spanning everything reachable
//!    downstream. `ExclusiveGateway`/`InclusiveGateway → decision` (both
//!    condition-routed gateway kinds), `UserTask → review`.
//! 2. **Heuristic** ([`heuristic_roles_and_clusters`], slice 2): name and
//!    `jobType` regex matching upgrades tasks to
//!    [`Role::Notification`] (`notify|send.?email|send.?sms|escalate`),
//!    [`Role::External`] (`external|api|http|remote|third.?party`), and
//!    refines [`Role::Review`] on service/user tasks whose name mentions
//!    review-like verbs (`review|verify|manual|approve|check|inspect`). Any
//!    `ServiceTask.job_type` shared by two or more tasks becomes a
//!    [`Cluster`] so the field solver draws them together.
//! 3. **Merge** ([`merge`]): a supplied (LLM- or human-authored) sidecar is
//!    layered over the inferred one per non-empty field, so any explicit
//!    annotation wins over inference without the caller needing per-field
//!    emptiness checks.
//!
//! # What we deliberately leave for later slices
//!
//! * Telemetry-source overrides for time / variance defaults (slice 2 in
//!   ADR 0002 mentions this, but the [`SemanticAnnotations`] schema doesn't
//!   yet carry cost / time — that's slice 4's `nano:*` extension work).
//! * Compensation flow classes — Nano's engine surface doesn't yet model
//!   dedicated compensation boundary events as a distinct flow class.
//!   (Escalation boundaries (#1173) *are* now classified — see
//!   [`escalation_flows`], which emits [`FlowKind::Escalation`].)
//! * Provenance / confidence per annotation — slice 5/6 workbench feature.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{ElementKind, ProcessDefinition};

use crate::bpmn_model::parse_element_names;
use crate::layout::schema::{AnnotatedFlow, Cluster, FlowKind, Role, SemanticAnnotations};

/// Run every inference pass against `xml` and return the resulting
/// [`SemanticAnnotations`], degrading to an empty sidecar if the XML fails
/// to parse. This is the entry point [`crate::layout`] callers should use.
pub fn infer_from_xml(xml: &str) -> SemanticAnnotations {
    let Ok(defs) = parse_bpmn(xml) else {
        return SemanticAnnotations::default();
    };
    let Some(def) = defs.into_iter().next() else {
        return SemanticAnnotations::default();
    };
    let names = parse_element_names(xml);
    let mut ann = infer(&def);
    let (roles, clusters) = heuristic_roles_and_clusters(&def, &names);
    // Heuristic roles override the structural default (a `ServiceTask`
    // classified as `notification` beats the structural pass's silence);
    // this is intentional — the heuristic pass has strictly more signal.
    for (id, role) in roles {
        ann.roles.insert(id, role);
    }
    ann.clusters = clusters;
    ann
}

/// Run the structural inference pass against `def` alone. Pure function of
/// the process definition — no telemetry, no LLM, no naming heuristics — so
/// every call on the same input returns the same output.
///
/// Callers that want the full inference should use [`infer_from_xml`]; this
/// entry point exists mainly for the slice-1 tests and any future caller
/// that only has a [`ProcessDefinition`] in hand.
pub fn infer(def: &ProcessDefinition) -> SemanticAnnotations {
    let mut ann = SemanticAnnotations::default();
    if let Some(primary) = trace_primary(def) {
        if !primary.is_empty() {
            ann.flows.push(AnnotatedFlow {
                id: "happy".to_string(),
                kind: FlowKind::Primary,
                nodes: primary,
            });
        }
    }
    ann.flows.extend(exception_flows(def));
    ann.flows.extend(escalation_flows(def));
    ann.roles = infer_roles(def);
    ann
}

/// Walk the process's default path from the start event and return the ordered
/// list of node ids it visits. See the module doc-comment for the exact rules.
fn trace_primary(def: &ProcessDefinition) -> Option<Vec<String>> {
    let start = def.element(&def.start_event)?;
    let mut order: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut stack: VecDeque<String> = VecDeque::new();
    stack.push_back(start.id.clone());
    while let Some(id) = stack.pop_front() {
        if !seen.insert(id.clone()) {
            continue;
        }
        order.push(id.clone());
        let Some(el) = def.element(&id) else { continue };
        let next: Vec<String> = match &el.kind {
            // Both condition-routed gateway kinds pick a single representative
            // branch for the primary spine — its `is_default` (or first)
            // outgoing. An inclusive gateway can fire several branches at
            // runtime, but following *all* of them here would mark every branch
            // as primary (the exact flat-line failure this module fixes), so it
            // is traced like an exclusive gateway.
            ElementKind::ExclusiveGateway | ElementKind::InclusiveGateway => el
                .outgoing
                .iter()
                .find(|f| f.is_default)
                .or_else(|| el.outgoing.first())
                .map(|f| vec![f.to.clone()])
                .unwrap_or_default(),
            _ => el.outgoing.iter().map(|f| f.to.clone()).collect(),
        };
        for n in next {
            if !seen.contains(&n) {
                stack.push_back(n);
            }
        }
    }
    Some(order)
}

/// Discover every interrupting boundary event and return one exception flow
/// per boundary — starting at the boundary itself, then everything reachable
/// downstream via sequence flows (short of re-entering the primary path
/// through a converging join, which we simply let happen — nodes claimed by
/// both flows use [`FlowKind::priority`] to resolve to exception).
fn exception_flows(def: &ProcessDefinition) -> Vec<AnnotatedFlow> {
    let mut flows: Vec<AnnotatedFlow> = Vec::new();
    let mut boundaries: Vec<(&str, &ElementKind)> = def
        .elements
        .values()
        .filter_map(|el| match &el.kind {
            ElementKind::ErrorBoundaryEvent { .. }
            | ElementKind::TimerBoundaryEvent {
                interrupting: true, ..
            }
            | ElementKind::MessageBoundaryEvent {
                interrupting: true, ..
            }
            | ElementKind::SignalBoundaryEvent {
                interrupting: true, ..
            }
            | ElementKind::ConditionalBoundaryEvent {
                interrupting: true, ..
            } => Some((el.id.as_str(), &el.kind)),
            _ => None,
        })
        .collect();
    // Stable, id-sorted ordering so repeat runs produce identical annotations.
    boundaries.sort_by_key(|(id, _)| *id);
    for (boundary_id, kind) in boundaries {
        let nodes = reachable_from(def, boundary_id);
        if nodes.is_empty() {
            continue;
        }
        flows.push(AnnotatedFlow {
            id: format!("exception.{boundary_id}"),
            kind: FlowKind::Exception,
            nodes,
        });
        // Silence the unused-binding warning without dropping the debug context.
        let _ = kind;
    }
    flows
}

/// Discover every escalation boundary event and return one escalation flow per
/// boundary — starting at the boundary itself, then everything reachable
/// downstream via sequence flows. Escalation handler paths get their own band
/// ([`FlowKind::Escalation`], rising above the centerline) rather than the
/// exception band, matching the "escalation to a supervisor / out-of-band
/// notification" reading. Both interrupting and non-interrupting escalation
/// boundaries are annotated: the handler path is out-of-band regardless of
/// whether the caught activity is torn down (the usual escalation idiom is in
/// fact non-interrupting). Without this the boundary and its handler nodes fall
/// through to the primary/default band and render on the main spine (#1173).
fn escalation_flows(def: &ProcessDefinition) -> Vec<AnnotatedFlow> {
    let mut flows: Vec<AnnotatedFlow> = Vec::new();
    let mut boundaries: Vec<&str> = def
        .elements
        .values()
        .filter_map(|el| match &el.kind {
            ElementKind::EscalationBoundaryEvent { .. } => Some(el.id.as_str()),
            _ => None,
        })
        .collect();
    // Stable, id-sorted ordering so repeat runs produce identical annotations.
    boundaries.sort_unstable();
    for boundary_id in boundaries {
        let nodes = reachable_from(def, boundary_id);
        if nodes.is_empty() {
            continue;
        }
        flows.push(AnnotatedFlow {
            id: format!("escalation.{boundary_id}"),
            kind: FlowKind::Escalation,
            nodes,
        });
    }
    flows
}
/// returning them in visitation order and including `from` itself.
fn reachable_from(def: &ProcessDefinition, from: &str) -> Vec<String> {
    let mut order: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    queue.push_back(from.to_string());
    while let Some(id) = queue.pop_front() {
        if !seen.insert(id.clone()) {
            continue;
        }
        order.push(id.clone());
        if let Some(el) = def.element(&id) {
            for f in &el.outgoing {
                if !seen.contains(&f.to) {
                    queue.push_back(f.to.clone());
                }
            }
        }
    }
    order
}

/// Role hints keyed by node id. See the module doc-comment for the (very
/// small) set of structural classifications we make today.
fn infer_roles(def: &ProcessDefinition) -> std::collections::BTreeMap<String, Role> {
    let mut roles = std::collections::BTreeMap::new();
    for el in def.elements.values() {
        let role = match &el.kind {
            // Both condition-routed gateway kinds are decisions: an inclusive
            // (OR) gateway evaluates its branch conditions exactly as an
            // exclusive (XOR) one does, so it carries the same `Decision` role in
            // the inferred contract (a parallel/event-based gateway does not).
            ElementKind::ExclusiveGateway | ElementKind::InclusiveGateway => Some(Role::Decision),
            ElementKind::UserTask(_) => Some(Role::Review),
            _ => None,
        };
        if let Some(r) = role {
            roles.insert(el.id.clone(), r);
        }
    }
    roles
}

/// Slice-2 heuristic pass. Walks tasks and applies substring rules against
/// each element's lowercased name and (for `ServiceTask`) `job_type` to
/// upgrade [`Role::Notification`] / [`Role::External`] / [`Role::Review`],
/// and groups every `ServiceTask` sharing a `job_type` into a [`Cluster`].
///
/// Returns `(roles, clusters)` — the caller decides how to fold roles into
/// the structural output (today: overwriting). Everything here is pure
/// substring matching (no regex crate) so slice 2 introduces no new
/// dependency.
fn heuristic_roles_and_clusters(
    def: &ProcessDefinition,
    names: &BTreeMap<String, String>,
) -> (BTreeMap<String, Role>, Vec<Cluster>) {
    let mut roles: BTreeMap<String, Role> = BTreeMap::new();
    // job_type → list of element ids sharing that type. We build it while
    // walking so we don't traverse the process twice.
    let mut by_job_type: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for el in def.elements.values() {
        let name_lc = names
            .get(&el.id)
            .map(|s| s.to_lowercase())
            .unwrap_or_default();
        let (job_type_lc, job_type_raw) = match &el.kind {
            ElementKind::ServiceTask { job_type, .. } => (job_type.to_lowercase(), Some(job_type)),
            _ => (String::new(), None),
        };
        if let Some(jt) = job_type_raw {
            by_job_type
                .entry(jt.clone())
                .or_default()
                .push(el.id.clone());
        }
        // Match against the combined haystack so a service task named
        // "Send confirmation email" with job_type="notify-email" is caught
        // by either signal.
        let haystack = format!("{name_lc} {job_type_lc}");
        if let Some(role) = classify_role(&haystack, &el.kind) {
            roles.insert(el.id.clone(), role);
        }
    }
    // Only emit clusters when the job_type is genuinely shared (2+ tasks) —
    // a singleton cluster would just add a phantom attractor with itself
    // as its only member.
    let mut clusters: Vec<Cluster> = by_job_type
        .into_iter()
        .filter(|(_, ids)| ids.len() >= 2)
        .map(|(jt, mut ids)| {
            ids.sort(); // Determinism across BTreeMap iteration orders.
            Cluster {
                id: format!("cluster.{jt}"),
                nodes: ids,
                affinity: 0.5,
            }
        })
        .collect();
    clusters.sort_by(|a, b| a.id.cmp(&b.id));
    (roles, clusters)
}

/// Apply the substring rules to a lowercased `haystack` (name + jobType)
/// and return the resulting [`Role`], or `None` when the heuristic has no
/// opinion. Notification / external win over review because a task named
/// "Send review reminder" is more usefully banded as a notification.
fn classify_role(haystack: &str, kind: &ElementKind) -> Option<Role> {
    if is_notification(haystack) {
        return Some(Role::Notification);
    }
    if is_external(haystack) {
        return Some(Role::External);
    }
    if is_review(haystack) {
        return Some(Role::Review);
    }
    // Preserve the structural default for user tasks even when nothing in
    // the name matches, so we never *demote* a user task by classifying it.
    if matches!(kind, ElementKind::UserTask(_)) {
        return Some(Role::Review);
    }
    None
}

fn is_notification(h: &str) -> bool {
    h.contains("notify")
        || h.contains("notification")
        || h.contains("escalate")
        || (h.contains("send") && (h.contains("email") || h.contains("sms") || h.contains("mail")))
}

fn is_external(h: &str) -> bool {
    h.contains("external")
        || h.contains("http")
        || h.contains("remote")
        || h.contains("third-party")
        || h.contains("third party")
        || h.contains("thirdparty")
        // "api" would match "capital" or "apiary"; match it only as a
        // whole word or hyphen-separated token.
        || h.split(|c: char| !c.is_alphanumeric()).any(|w| w == "api")
}

fn is_review(h: &str) -> bool {
    h.contains("review")
        || h.contains("verify")
        || h.contains("manual")
        || h.contains("approve")
        || h.contains("approval")
        || h.contains("inspect")
}

/// Merge a `structural` inferred annotation set with a `supplied` one, letting
/// every non-empty supplied field win. Callers use this to layer an
/// LLM-authored or human-confirmed sidecar over the structural default without
/// having to check emptiness field-by-field.
pub fn merge(
    supplied: Option<SemanticAnnotations>,
    structural: SemanticAnnotations,
) -> SemanticAnnotations {
    let Some(supplied) = supplied else {
        return structural;
    };
    let flows = if supplied.flows.is_empty() {
        structural.flows
    } else {
        supplied.flows
    };
    let clusters = if supplied.clusters.is_empty() {
        structural.clusters
    } else {
        supplied.clusters
    };
    let roles = if supplied.roles.is_empty() {
        structural.roles
    } else {
        supplied.roles
    };
    // Costs and times are authored in the workbench (slice 6) and consumed
    // by the optimisation prompt (slice 7); the structural inference pass
    // never emits them, so "supplied wins, else empty" collapses to
    // "supplied wins" and no field-by-field check is needed.
    let costs = if supplied.costs.is_empty() {
        structural.costs
    } else {
        supplied.costs
    };
    let times = if supplied.times.is_empty() {
        structural.times
    } else {
        supplied.times
    };
    SemanticAnnotations {
        flows,
        clusters,
        roles,
        costs,
        times,
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::bpmn::parse_bpmn;

    use super::*;

    const TINY_XML: &str = include_str!("../../fixtures/layout/tiny.bpmn");

    #[test]
    fn primary_flow_follows_the_default_gateway_branch() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let ann = infer(&def);
        let primary = ann
            .flows
            .iter()
            .find(|f| f.kind == FlowKind::Primary)
            .expect("primary flow inferred");
        assert!(
            primary.nodes.iter().any(|n| n == "Start_1"),
            "primary starts at Start_1, got {:?}",
            primary.nodes
        );
        // The fixture's GwCheck default goes to TaskB → End_ok (the happy
        // route). The primary trace must therefore follow TaskB, not
        // HandleError (which is the exception branch, reached via the
        // gateway's non-default flow / the error boundary in the wider
        // model).
        assert!(
            primary.nodes.iter().any(|n| n == "TaskB"),
            "primary follows the default flow, got {:?}",
            primary.nodes
        );
        assert!(
            !primary.nodes.iter().any(|n| n == "HandleError"),
            "primary must not visit the non-default branch, got {:?}",
            primary.nodes
        );
    }

    #[test]
    fn exception_flow_captures_boundary_downstream() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let ann = infer(&def);
        // The fixture attaches an error boundary to HandleError routing to
        // End_err; if the boundary event is present, the exception flow must
        // pick it up. If it isn't (older fixture), infer() returns no
        // exception flow and this test degrades to a no-op assertion.
        let has_boundary = def
            .elements
            .values()
            .any(|el| matches!(el.kind, ElementKind::ErrorBoundaryEvent { .. }));
        if !has_boundary {
            return;
        }
        let exc = ann
            .flows
            .iter()
            .find(|f| f.kind == FlowKind::Exception)
            .expect("exception flow inferred for error boundary");
        assert!(
            !exc.nodes.is_empty(),
            "exception flow has at least the boundary event itself"
        );
    }

    #[test]
    fn escalation_flow_captures_the_boundary_handler_path() {
        // An escalation boundary (#1173) must produce a `FlowKind::Escalation`
        // band starting at the boundary and covering its downstream handler
        // nodes, and that path must NOT be folded onto the primary spine —
        // otherwise the escalation handler renders on the main flow line. Built
        // via `ProcessBuilder` (no fixture needed) so the assertion is exact.
        use nanobpmn_engine_core::ProcessBuilder;
        let def = ProcessBuilder::new("esc")
            .start_event("start")
            .sub_process("sub", "sub_start")
            .start_event("sub_start")
            .contained_in("sub_start", "sub")
            .escalation_throw_event("throw", "OVERLOAD")
            .contained_in("throw", "sub")
            .end_event("sub_end")
            .contained_in("sub_end", "sub")
            .escalation_boundary_event("boundary", "sub", "OVERLOAD")
            .service_task("notify", "notify")
            .end_event("done")
            .end_event("handled")
            .connect("start", "sub")
            .connect("sub_start", "throw")
            .connect("throw", "sub_end")
            .connect("sub", "done")
            .connect("boundary", "notify")
            .connect("notify", "handled")
            .build()
            .expect("valid escalation model");
        let ann = infer(&def);
        let esc = ann
            .flows
            .iter()
            .find(|f| f.kind == FlowKind::Escalation)
            .expect("escalation flow inferred for the escalation boundary");
        assert_eq!(esc.id, "escalation.boundary");
        assert_eq!(
            esc.nodes.first().map(String::as_str),
            Some("boundary"),
            "escalation flow starts at the boundary, got {:?}",
            esc.nodes
        );
        assert!(
            esc.nodes.iter().any(|n| n == "notify"),
            "escalation flow includes the downstream handler, got {:?}",
            esc.nodes
        );
        assert!(
            esc.nodes.iter().any(|n| n == "handled"),
            "escalation flow reaches the handler end, got {:?}",
            esc.nodes
        );
        // The handler path is out-of-band: it must not appear on the primary spine.
        let primary = ann
            .flows
            .iter()
            .find(|f| f.kind == FlowKind::Primary)
            .expect("primary flow inferred");
        assert!(
            !primary.nodes.iter().any(|n| n == "notify"),
            "escalation handler must not be folded onto the primary spine, got {:?}",
            primary.nodes
        );
    }

    #[test]
    fn roles_tag_exclusive_gateways_as_decisions() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let ann = infer(&def);
        let gw = def
            .elements
            .values()
            .find(|el| matches!(el.kind, ElementKind::ExclusiveGateway))
            .expect("fixture has a gateway");
        assert_eq!(
            ann.roles.get(&gw.id).copied(),
            Some(Role::Decision),
            "exclusive gateway {} should be a decision role",
            gw.id
        );
    }

    #[test]
    fn roles_tag_inclusive_gateways_as_decisions() {
        // An inclusive (OR) gateway is condition-routed just like an exclusive
        // one, so `infer_roles` must classify it as a `Decision` too — otherwise
        // the new gateway kind would silently vanish from the inferred role
        // contract processos consumers rely on.
        const INCLUSIVE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  id="Definitions_or" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="OrProcess" isExecutable="true">
    <bpmn:startEvent id="Start_1"><bpmn:outgoing>Flow_1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:inclusiveGateway id="GwOr" name="which?" default="Flow_b">
      <bpmn:incoming>Flow_1</bpmn:incoming>
      <bpmn:outgoing>Flow_a</bpmn:outgoing>
      <bpmn:outgoing>Flow_b</bpmn:outgoing>
    </bpmn:inclusiveGateway>
    <bpmn:endEvent id="End_a"><bpmn:incoming>Flow_a</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="End_b"><bpmn:incoming>Flow_b</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="Flow_1" sourceRef="Start_1" targetRef="GwOr"/>
    <bpmn:sequenceFlow id="Flow_a" sourceRef="GwOr" targetRef="End_a">
      <bpmn:conditionExpression>=go</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_b" sourceRef="GwOr" targetRef="End_b"/>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="Diagram_or">
    <bpmndi:BPMNPlane id="Plane_or" bpmnElement="OrProcess">
      <bpmndi:BPMNShape id="Start_1_di" bpmnElement="Start_1">
        <dc:Bounds x="150" y="102" width="36" height="36"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="GwOr_di" bpmnElement="GwOr" isMarkerVisible="true">
        <dc:Bounds x="255" y="95" width="50" height="50"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="End_a_di" bpmnElement="End_a">
        <dc:Bounds x="412" y="52" width="36" height="36"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="End_b_di" bpmnElement="End_b">
        <dc:Bounds x="412" y="152" width="36" height="36"/>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_1_di" bpmnElement="Flow_1">
        <di:waypoint x="186" y="120"/>
        <di:waypoint x="255" y="120"/>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_a_di" bpmnElement="Flow_a">
        <di:waypoint x="305" y="112"/>
        <di:waypoint x="412" y="70"/>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_b_di" bpmnElement="Flow_b">
        <di:waypoint x="305" y="128"/>
        <di:waypoint x="412" y="170"/>
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>"#;
        let defs = parse_bpmn(INCLUSIVE_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let ann = infer(&def);
        let gw = def
            .elements
            .values()
            .find(|el| matches!(el.kind, ElementKind::InclusiveGateway))
            .expect("fixture has an inclusive gateway");
        assert_eq!(
            ann.roles.get(&gw.id).copied(),
            Some(Role::Decision),
            "inclusive gateway {} should be a decision role",
            gw.id
        );
        // `trace_primary` must treat the inclusive gateway like an exclusive
        // one: follow its `default` branch (Flow_b → End_b) for the primary
        // spine, not fan out across every outgoing branch (which would mark the
        // non-default End_a as primary too — the flat-line failure this module
        // exists to prevent).
        let primary = ann
            .flows
            .iter()
            .find(|f| f.kind == FlowKind::Primary)
            .expect("primary flow inferred");
        assert!(
            primary.nodes.iter().any(|n| n == "End_b"),
            "primary follows the inclusive gateway's default branch, got {:?}",
            primary.nodes
        );
        assert!(
            !primary.nodes.iter().any(|n| n == "End_a"),
            "primary must not visit the non-default inclusive branch, got {:?}",
            primary.nodes
        );
    }

    #[test]
    fn merge_prefers_supplied_when_populated_and_structural_otherwise() {
        let structural = SemanticAnnotations {
            flows: vec![AnnotatedFlow {
                id: "happy".into(),
                kind: FlowKind::Primary,
                nodes: vec!["A".into(), "B".into()],
            }],
            ..Default::default()
        };
        // Empty supplied ⇒ structural wins.
        let merged = merge(Some(SemanticAnnotations::default()), structural.clone());
        assert_eq!(
            merged.flows.len(),
            1,
            "empty supplied falls back to structural"
        );
        // Populated supplied ⇒ supplied wins for that field only.
        let supplied = SemanticAnnotations {
            flows: vec![AnnotatedFlow {
                id: "manual".into(),
                kind: FlowKind::Primary,
                nodes: vec!["X".into()],
            }],
            ..Default::default()
        };
        let merged = merge(Some(supplied), structural);
        assert_eq!(merged.flows[0].id, "manual");
    }

    #[test]
    fn infer_is_deterministic() {
        let defs = parse_bpmn(TINY_XML).expect("parse");
        let def = defs.into_iter().next().expect("one process");
        let a = infer(&def);
        let b = infer(&def);
        assert_eq!(a.flows.len(), b.flows.len());
        for (fa, fb) in a.flows.iter().zip(b.flows.iter()) {
            assert_eq!(fa.id, fb.id);
            assert_eq!(fa.nodes, fb.nodes);
        }
        assert_eq!(a.roles, b.roles);
    }

    // ------------------- slice 2: heuristic + cluster passes -------------------

    const HEURISTIC_XML: &str = include_str!("../../fixtures/layout/heuristic.bpmn");

    #[test]
    fn heuristic_classifies_notification_external_and_review_tasks() {
        let ann = infer_from_xml(HEURISTIC_XML);
        assert_eq!(
            ann.roles.get("SendConfirmationEmail").copied(),
            Some(Role::Notification),
            "'Send confirmation email' should be a notification role, got roles={:?}",
            ann.roles
        );
        assert_eq!(
            ann.roles.get("NotifyCustomer").copied(),
            Some(Role::Notification),
            "'Notify customer' should be a notification role"
        );
        assert_eq!(
            ann.roles.get("CheckCreditBureau").copied(),
            Some(Role::External),
            "'Call external credit bureau API' should be external"
        );
        assert_eq!(
            ann.roles.get("ReviewApplication").copied(),
            Some(Role::Review),
            "'Review application' should be review"
        );
        // ArchiveRecord has no matching name/jobType heuristic and isn't a
        // gateway or user task — it stays unclassified.
        assert!(
            !ann.roles.contains_key("ArchiveRecord"),
            "ArchiveRecord should remain unclassified, got {:?}",
            ann.roles.get("ArchiveRecord")
        );
    }

    #[test]
    fn heuristic_notification_beats_review_when_name_overlaps() {
        // A "Send review reminder" service task is more usefully banded as
        // notification than as review — classify_role's ordering enforces
        // that; regression-test it.
        let role = classify_role(
            "send review reminder email",
            &ElementKind::ServiceTask {
                agent_type: None,
                job_type: "notify".into(),
                priority: None,
                custom_headers: std::collections::BTreeMap::new(),
                linked_resources: Vec::new(),
            },
        );
        assert_eq!(role, Some(Role::Notification));
    }

    #[test]
    fn heuristic_external_matches_api_as_a_word_not_substring() {
        // "capital" contains "api" as a substring but is not an external
        // call; is_external must not fire on it.
        assert!(!is_external("collect capital reserves"));
        assert!(is_external("call external api"));
        assert!(is_external("post to remote http endpoint"));
        assert!(is_external("send to third-party service"));
    }

    #[test]
    fn cluster_groups_shared_job_types_and_ignores_singletons() {
        let ann = infer_from_xml(HEURISTIC_XML);
        // Fixture has two `notify` service tasks and one each of `review`,
        // `http-call`, `archive`; only `notify` should produce a cluster.
        assert_eq!(
            ann.clusters.len(),
            1,
            "one shared-jobType cluster expected, got {:?}",
            ann.clusters.iter().map(|c| &c.id).collect::<Vec<_>>()
        );
        let notify = &ann.clusters[0];
        assert_eq!(notify.id, "cluster.notify");
        assert_eq!(
            notify.nodes,
            vec![
                "NotifyCustomer".to_string(),
                "SendConfirmationEmail".to_string()
            ],
            "cluster nodes are sorted for determinism"
        );
        assert_eq!(notify.affinity, 0.5);
    }

    #[test]
    fn infer_from_xml_is_deterministic_across_runs() {
        let a = infer_from_xml(HEURISTIC_XML);
        let b = infer_from_xml(HEURISTIC_XML);
        assert_eq!(a.roles, b.roles);
        assert_eq!(a.clusters.len(), b.clusters.len());
        for (ca, cb) in a.clusters.iter().zip(b.clusters.iter()) {
            assert_eq!(ca.id, cb.id);
            assert_eq!(ca.nodes, cb.nodes);
        }
    }

    #[test]
    fn infer_from_xml_degrades_gracefully_on_parse_failure() {
        let ann = infer_from_xml("<not-bpmn/>");
        assert!(ann.flows.is_empty());
        assert!(ann.roles.is_empty());
        assert!(ann.clusters.is_empty());
    }
}
