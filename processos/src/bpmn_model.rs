//! **BPMN model analysis** — a structural lens on a process *definition*, independent
//! of runtime traces.
//!
//! The cockpit droid reasons about *what happened* from the trace tables (see
//! [`crate::analysis`]). This module is the complementary *what is modelled* surface: it
//! distils a `model.bpmn` into a compact structural graph ([`read_model`]) and runs a
//! battery of deterministic static checks ([`analyze_model`]) — unreachable nodes, missing
//! ends, gateway split/join hazards, unguarded service tasks, rework loops.
//!
//! Crucially the node `id`s and service-task `jobType`s reported here are the **same keys**
//! the trace tables carry (`jobs.element_id` / `jobs.job_type`, `incidents.element_id`), so
//! a persona can join structure to runtime: find a structural risk here, then measure how
//! often it bites with `query_traces`.
//!
//! Both functions take raw BPMN XML and parse it with the engine's own
//! [`parse_bpmn`](nanobpmn_engine_core::bpmn::parse_bpmn) — exactly the parser production
//! uses — so the structural view never drifts from executable semantics.

use std::collections::{BTreeMap, HashMap, HashSet};

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    BindingType, Condition, Element, ElementKind, ProcessDefinition, SequenceFlow,
};
use serde_json::{json, Value};

/// Parse `xml` and return its first process definition, or an error message.
fn first_def(xml: &str) -> Result<(ProcessDefinition, usize), String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("model failed to parse: {e:?}"))?;
    let count = defs.len();
    let def = defs
        .into_iter()
        .next()
        .ok_or_else(|| "model contained no process definitions".to_string())?;
    Ok((def, count))
}

/// The short label and structural extras for an element kind.
fn kind_label(kind: &ElementKind) -> &'static str {
    match kind {
        ElementKind::StartEvent => "startEvent",
        ElementKind::EndEvent => "endEvent",
        ElementKind::TerminateEndEvent => "terminateEndEvent",
        ElementKind::ServiceTask { .. } => "serviceTask",
        ElementKind::BusinessRuleTask { .. } => "businessRuleTask",
        ElementKind::UserTask(_) => "userTask",
        ElementKind::ExclusiveGateway => "exclusiveGateway",
        ElementKind::ParallelGateway => "parallelGateway",
        ElementKind::InclusiveGateway => "inclusiveGateway",
        ElementKind::EventBasedGateway => "eventBasedGateway",
        ElementKind::ErrorBoundaryEvent { .. } => "errorBoundaryEvent",
        ElementKind::TimerIntermediateCatchEvent { .. } => "timerIntermediateCatchEvent",
        ElementKind::TimerBoundaryEvent { .. } => "timerBoundaryEvent",
        ElementKind::MessageIntermediateCatchEvent { .. } => "messageIntermediateCatchEvent",
        ElementKind::MessageBoundaryEvent { .. } => "messageBoundaryEvent",
        ElementKind::MessageStartEvent { .. } => "messageStartEvent",
        ElementKind::TimerStartEvent { .. } => "timerStartEvent",
        ElementKind::SubProcess { .. } => "subProcess",
        ElementKind::IntermediateThrowEvent => "intermediateThrowEvent",
        ElementKind::EscalationThrowEvent { .. } => "escalationThrowEvent",
        ElementKind::EscalationBoundaryEvent { .. } => "escalationBoundaryEvent",
        ElementKind::LinkIntermediateThrowEvent { .. } => "linkIntermediateThrowEvent",
        ElementKind::LinkIntermediateCatchEvent { .. } => "linkIntermediateCatchEvent",
        ElementKind::Task => "task",
        ElementKind::ScriptTask { .. } => "scriptTask",
        ElementKind::CallActivity { .. } => "callActivity",
        ElementKind::SignalIntermediateCatchEvent { .. } => "signalIntermediateCatchEvent",
        ElementKind::SignalBoundaryEvent { .. } => "signalBoundaryEvent",
        ElementKind::ConditionalIntermediateCatchEvent { .. } => {
            "conditionalIntermediateCatchEvent"
        }
        ElementKind::ConditionalBoundaryEvent { .. } => "conditionalBoundaryEvent",
        ElementKind::CompensationBoundaryEvent { .. } => "compensationBoundaryEvent",
        ElementKind::CompensationThrowEvent => "compensationThrowEvent",
        ElementKind::AgentTask { .. } => "serviceTask",
    }
}

fn is_boundary(kind: &ElementKind) -> bool {
    kind.attached_to().is_some()
}

fn is_gateway(kind: &ElementKind) -> bool {
    matches!(
        kind,
        ElementKind::ExclusiveGateway
            | ElementKind::ParallelGateway
            | ElementKind::InclusiveGateway
            | ElementKind::EventBasedGateway
    )
}

/// A node that produces a `jobs` row at runtime (one row per executed service/user task).
/// These are the only model nodes observable in the trace tables, so conformance checking
/// works at the granularity of task-to-task transitions.
fn is_task(kind: &ElementKind) -> bool {
    matches!(
        kind,
        ElementKind::ServiceTask { .. } | ElementKind::UserTask(_) | ElementKind::Task
    )
}

/// Kind-specific extra attributes for the structural view (job type, attachment, etc.).
fn kind_extras(kind: &ElementKind) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    match kind {
        ElementKind::ServiceTask {
            job_type, priority, ..
        } => {
            m.insert("jobType".into(), json!(job_type));
            if let Some(p) = priority {
                m.insert("priority".into(), json!(p));
            }
        }
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => {
            m.insert("attachedTo".into(), json!(attached_to));
            m.insert("errorCode".into(), json!(error_code));
        }
        ElementKind::TimerIntermediateCatchEvent { duration_millis } => {
            m.insert("durationMs".into(), json!(duration_millis));
        }
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => {
            m.insert("attachedTo".into(), json!(attached_to));
            m.insert("durationMs".into(), json!(duration_millis));
            m.insert("interrupting".into(), json!(interrupting));
            m.insert("repeating".into(), json!(repeating));
        }
        ElementKind::MessageIntermediateCatchEvent {
            message_name,
            correlation_key,
        } => {
            m.insert("messageName".into(), json!(message_name));
            m.insert("correlationKey".into(), json!(correlation_key));
        }
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => {
            m.insert("attachedTo".into(), json!(attached_to));
            m.insert("messageName".into(), json!(message_name));
            m.insert("correlationKey".into(), json!(correlation_key));
            m.insert("interrupting".into(), json!(interrupting));
        }
        ElementKind::MessageStartEvent { message_name } => {
            m.insert("messageName".into(), json!(message_name));
        }
        ElementKind::TimerStartEvent {
            interval_millis,
            repeating,
        } => {
            m.insert("intervalMs".into(), json!(interval_millis));
            m.insert("repeating".into(), json!(repeating));
        }
        ElementKind::SubProcess { start_event } => {
            m.insert("innerStartEvent".into(), json!(start_event));
        }
        ElementKind::EscalationThrowEvent { escalation_code } => {
            m.insert("escalationCode".into(), json!(escalation_code));
        }
        ElementKind::EscalationBoundaryEvent {
            attached_to,
            escalation_code,
            interrupting,
        } => {
            m.insert("attachedTo".into(), json!(attached_to));
            m.insert("escalationCode".into(), json!(escalation_code));
            m.insert("interrupting".into(), json!(interrupting));
        }
        ElementKind::CallActivity {
            called_process_id,
            propagate_all_parent_variables,
            propagate_all_child_variables,
        } => {
            m.insert("calledElement".into(), json!(called_process_id));
            m.insert(
                "propagateAllParentVariables".into(),
                json!(propagate_all_parent_variables),
            );
            m.insert(
                "propagateAllChildVariables".into(),
                json!(propagate_all_child_variables),
            );
        }
        _ => {}
    }
    m
}

/// Parse every `<bpmn:process>` in `xml` and index them by id — the call-activity
/// resolution library a multi-stage orchestrator needs to inline its phases.
fn definition_library(
    xml: &str,
) -> Result<(Vec<ProcessDefinition>, HashMap<String, ProcessDefinition>), String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("model failed to parse: {e:?}"))?;
    if defs.is_empty() {
        return Err("model contained no process definitions".to_string());
    }
    let library: HashMap<String, ProcessDefinition> =
        defs.iter().map(|d| (d.id.clone(), d.clone())).collect();
    Ok((defs, library))
}

/// Inline a multi-definition model: every `<process>` forms the call-activity
/// library and `process_id` (default: the first/orchestrator definition) is
/// returned with its call activities expanded inline. Inlined node ids are the
/// `Parent$Child` keys the trace tables carry, so the structural view lines up
/// with `jobs.element_id` / `incidents.element_id`.
pub fn inline_definition(xml: &str, process_id: Option<&str>) -> Result<ProcessDefinition, String> {
    let (defs, library) = definition_library(xml)?;
    let root = match process_id {
        Some(pid) => library
            .get(pid)
            .cloned()
            .ok_or_else(|| format!("no process '{pid}' in the model"))?,
        None => defs[0].clone(),
    };
    root.inline_call_activities(&library)
}

/// True when any definition in the model uses a call activity (i.e. the model is
/// a multi-stage orchestrator whose phases can be inlined / read individually).
fn has_call_activities(defs: &[ProcessDefinition]) -> bool {
    defs.iter()
        .flat_map(|d| d.elements.values())
        .any(|e| matches!(e.kind, ElementKind::CallActivity { .. }))
}

/// True when the raw model XML uses any call activity — a cheap textual probe for
/// callers (e.g. seeding) that don't want to pull in the engine element types.
pub fn references_call_activities(xml: &str) -> bool {
    match parse_bpmn(xml) {
        Ok(defs) => has_call_activities(&defs),
        Err(_) => false,
    }
}

// ---------------------------------------------------------------------------
// Lightweight, dependency-free XML element splicing.
//
// These primitives let the model tools work with MULTI-DEFINITION BPMN (a
// multi-stage orchestrator whose called phases travel in the same file): read a
// single phase's raw XML, edit one definition while preserving the others (and
// the orchestrator's authored diagram), and merge phase files into one
// self-contained model at seed time. BPMN root elements (`process`, `message`,
// `error`, `definitions`) never nest within themselves, so a non-recursive
// open→matching-close scan is faithful.
// ---------------------------------------------------------------------------

/// The full tag name (including any namespace prefix) of the start tag at `lt`.
fn tag_name_at(xml: &str, lt: usize) -> &str {
    let rest = &xml[lt + 1..];
    let end = rest
        .find([' ', '\t', '\n', '\r', '>', '/'])
        .unwrap_or(rest.len());
    &rest[..end]
}

/// Byte offset of the first `<[ns:]local` *start* tag at or after `from`, where
/// the local name matches exactly (so `error` never matches `errorEventDefinition`).
fn find_tag_open(xml: &str, local: &str, from: usize) -> Option<usize> {
    let mut i = from.min(xml.len());
    while let Some(rel) = xml[i..].find('<') {
        let lt = i + rel;
        let after = &xml[lt + 1..];
        match after.chars().next() {
            Some('/') | Some('!') | Some('?') => {
                i = lt + 1;
                continue;
            }
            _ => {}
        }
        let name = tag_name_at(xml, lt);
        let local_name = name.rsplit(':').next().unwrap_or(name);
        if local_name == local {
            return Some(lt);
        }
        i = lt + 1;
    }
    None
}

/// The byte span `[start, end)` of the first `<[ns:]local …>…</[ns:]local>` (or
/// self-closing) element at or after `from`.
fn element_span(xml: &str, local: &str, from: usize) -> Option<(usize, usize)> {
    let start = find_tag_open(xml, local, from)?;
    let name = tag_name_at(xml, start);
    let open_gt = start + xml[start..].find('>')?;
    if xml[start..=open_gt].trim_end().ends_with("/>") {
        return Some((start, open_gt + 1));
    }
    let close = format!("</{name}>");
    let close_at = xml[open_gt..].find(&close)? + open_gt + close.len();
    Some((start, close_at))
}

/// The value of attribute `attr` in `s` (the `attr` must be preceded by whitespace,
/// so `id` doesn't match `processId`).
fn attr_value_in(s: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let mut from = 0;
    while let Some(rel) = s[from..].find(&needle) {
        let at = from + rel;
        let preceded_by_ws = s[..at].chars().last().is_some_and(|c| c.is_whitespace());
        if preceded_by_ws {
            let vstart = at + needle.len();
            let vend = s[vstart..].find('"')? + vstart;
            return Some(s[vstart..vend].to_string());
        }
        from = at + needle.len();
    }
    None
}

/// Every value of an `id` attribute in `s`, in document order. Used to reserve the
/// ids inside a preserved `<bpmndi:BPMNDiagram>` block so a generated declaration
/// id cannot collide with one (#1173). Tolerant of the legal XML spellings the DI
/// serializer (or a hand-edited model) may use — `id="…"`, `id = "…"` and
/// `id='…'` — so a DI id in any of them is still reserved.
fn all_id_attrs_in(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = s[from..].find("id") {
        let at = from + rel;
        from = at + 2;
        // `id` must be a standalone attribute name: preceded by whitespace so it
        // is not the tail of another name (`bpmndi`, `validId`, …) ...
        if !s[..at].chars().last().is_some_and(|c| c.is_whitespace()) {
            continue;
        }
        // ... and followed by optional whitespace, `=`, optional whitespace, then a
        // single- or double-quote opening the value.
        let after = s[from..].trim_start();
        let Some(after_eq) = after.strip_prefix('=') else {
            continue;
        };
        let val = after_eq.trim_start();
        let Some(quote) = val.chars().next() else {
            continue;
        };
        if quote != '"' && quote != '\'' {
            continue;
        }
        let body = &val[quote.len_utf8()..];
        if let Some(off) = body.find(quote) {
            out.push(body[..off].to_string());
        }
    }
    out
}

/// The raw `<process id="process_id">…</process>` block from a (multi-definition)
/// document, or `None` if no such process exists.
fn process_block(xml: &str, process_id: &str) -> Option<String> {
    let mut from = 0;
    while let Some((s, e)) = element_span(xml, "process", from) {
        if attr_value_in(&xml[s..e], "id").as_deref() == Some(process_id) {
            return Some(xml[s..e].to_string());
        }
        from = e;
    }
    None
}

/// The inner body of the first `<definitions>` element, with any `<bpmndi:BPMNDiagram>`
/// blocks stripped — i.e. the root declarations (messages/errors) and process(es),
/// verbatim, ready to splice into another document. Diagrams are dropped so a merged
/// file keeps a single (the primary's) diagram.
fn definitions_inner_no_di(xml: &str) -> Option<String> {
    let start = find_tag_open(xml, "definitions", 0)?;
    let name = tag_name_at(xml, start);
    let open_gt = start + xml[start..].find('>')? + 1;
    let close = format!("</{name}>");
    let close_at = xml[open_gt..].find(&close)? + open_gt;
    let mut inner = xml[open_gt..close_at].to_string();
    while let Some((ds, de)) = element_span(&inner, "BPMNDiagram", 0) {
        inner.replace_range(ds..de, "");
    }
    Some(inner.trim_matches(['\n', ' ', '\t']).to_string())
}

/// Splice the body of each `extra` definitions document into `primary`, just
/// before `primary`'s diagram (so all root elements precede the BPMNDI, keeping
/// the document schema-ordered) — or before its closing tag when it has no
/// diagram. The extras' own diagrams are dropped; `primary`'s is preserved.
pub fn merge_definitions(primary: &str, extras: &[&str]) -> String {
    let mut injected = String::new();
    for x in extras {
        if let Some(inner) = definitions_inner_no_di(x) {
            if !inner.is_empty() {
                injected.push_str("  ");
                injected.push_str(inner.trim());
                injected.push('\n');
            }
        }
    }
    if injected.is_empty() {
        return primary.to_string();
    }
    let insert_at = find_tag_open(primary, "BPMNDiagram", 0)
        .map(|di| primary[..di].rfind('\n').map(|n| n + 1).unwrap_or(di))
        .or_else(|| {
            let start = find_tag_open(primary, "definitions", 0)?;
            let name = tag_name_at(primary, start);
            primary.rfind(&format!("</{name}>"))
        });
    match insert_at {
        Some(at) => {
            let mut out = String::with_capacity(primary.len() + injected.len());
            out.push_str(&primary[..at]);
            out.push_str(&injected);
            out.push_str(&primary[at..]);
            out
        }
        None => primary.to_string(),
    }
}

/// Return the raw BPMN XML of the model, or of one named phase within it. `target`
/// may be a `<process>` id OR a callActivity node id (resolved to its calledElement),
/// letting the droid inspect the exact XML behind an opaque orchestrator phase.
pub fn read_model_xml(xml: &str, target: Option<&str>) -> Result<Value, String> {
    let (defs, _library) = definition_library(xml)?;
    match target {
        None => Ok(json!({
            "scope": "full",
            "definitionCount": defs.len(),
            "processIds": defs.iter().map(|d| d.id.clone()).collect::<Vec<_>>(),
            "xml": xml,
            "note": "The full BPMN document (orchestrator + any called phase processes). Pass \
                     process:\"<id>\" — a process id or a callActivity node id — to isolate one \
                     phase's raw XML, then edit it with edit_model process:\"<id>\".",
        })),
        Some(t) => {
            // Resolve a callActivity node id to the process it calls; else treat t as a process id.
            let mut process_id = t.to_string();
            for d in &defs {
                if let Some(ElementKind::CallActivity {
                    called_process_id, ..
                }) = d.elements.get(t).map(|e| &e.kind)
                {
                    process_id = called_process_id.clone();
                    break;
                }
            }
            let block = process_block(xml, &process_id).ok_or_else(|| {
                format!(
                    "no <process id=\"{process_id}\"> in the model (and '{t}' is not a callActivity \
                     node). Known process ids: {}",
                    defs.iter()
                        .map(|d| d.id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;
            Ok(json!({
                "scope": "process",
                "processId": process_id,
                "xml": block,
                "note": "Raw BPMN for this phase. Reason over it, then patch it with edit_model \
                         process:\"<id>\" (NOT by hand) so XML correctness is guaranteed.",
            }))
        }
    }
}

/// Structural edges used for reachability and loop detection: outgoing sequence flows,
/// sub-process entry (container -> inner start), and boundary attachment (activity ->
/// boundary event). The returned map is keyed by element id.
fn adjacency(def: &ProcessDefinition) -> HashMap<String, Vec<String>> {
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for (id, el) in &def.elements {
        let entry = adj.entry(id.clone()).or_default();
        for flow in &el.outgoing {
            entry.push(flow.to.clone());
        }
        if let ElementKind::SubProcess { start_event } = &el.kind {
            entry.push(start_event.clone());
        }
    }
    // Boundary events have no incoming flow; they become reachable through their host.
    for (id, el) in &def.elements {
        if let Some(host) = el.kind.attached_to() {
            adj.entry(host.to_string()).or_default().push(id.clone());
        }
    }
    adj
}

/// The set of element ids reachable from the start event over [`adjacency`].
fn reachable_from_start(
    def: &ProcessDefinition,
    adj: &HashMap<String, Vec<String>>,
) -> HashSet<String> {
    let mut seen = HashSet::new();
    let mut stack = vec![def.start_event.clone()];
    while let Some(id) = stack.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(next) = adj.get(&id) {
            for n in next {
                if !seen.contains(n) {
                    stack.push(n.clone());
                }
            }
        }
    }
    seen
}

/// Reverse adjacency (predecessors) for ancestor queries.
fn predecessors(adj: &HashMap<String, Vec<String>>) -> HashMap<String, Vec<String>> {
    let mut rev: HashMap<String, Vec<String>> = HashMap::new();
    for (from, tos) in adj {
        for to in tos {
            rev.entry(to.clone()).or_default().push(from.clone());
        }
    }
    rev
}

/// All ancestors of `node` over the reverse graph (nodes that can reach it).
fn ancestors(node: &str, rev: &HashMap<String, Vec<String>>) -> HashSet<String> {
    let mut seen = HashSet::new();
    let mut stack: Vec<String> = rev.get(node).cloned().unwrap_or_default();
    while let Some(id) = stack.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(preds) = rev.get(&id) {
            for p in preds {
                if !seen.contains(p) {
                    stack.push(p.clone());
                }
            }
        }
    }
    seen
}

/// Tarjan strongly-connected components over the structural graph. Returns the components
/// that constitute a loop (size > 1, or a single self-looping node).
fn loop_components(
    def: &ProcessDefinition,
    adj: &HashMap<String, Vec<String>>,
) -> Vec<Vec<String>> {
    // Index the nodes deterministically.
    let ids: Vec<String> = {
        let mut v: Vec<String> = def.elements.keys().cloned().collect();
        v.sort();
        v
    };
    let index_of: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    let n = ids.len();
    let neighbours: Vec<Vec<usize>> = ids
        .iter()
        .map(|id| {
            adj.get(id)
                .map(|tos| {
                    tos.iter()
                        .filter_map(|t| index_of.get(t.as_str()).copied())
                        .collect()
                })
                .unwrap_or_default()
        })
        .collect();

    #[derive(Default)]
    struct Tarjan {
        idx: usize,
        indices: Vec<Option<usize>>,
        low: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        sccs: Vec<Vec<usize>>,
    }
    impl Tarjan {
        fn strong(&mut self, v: usize, neighbours: &[Vec<usize>]) {
            let v_index = self.idx;
            self.indices[v] = Some(v_index);
            self.low[v] = self.idx;
            self.idx += 1;
            self.stack.push(v);
            self.on_stack[v] = true;
            for &w in &neighbours[v] {
                if self.indices[w].is_none() {
                    self.strong(w, neighbours);
                    self.low[v] = self.low[v].min(self.low[w]);
                } else if self.on_stack[w] {
                    if let Some(iw) = self.indices[w] {
                        self.low[v] = self.low[v].min(iw);
                    }
                }
            }
            if self.low[v] == v_index {
                let mut comp = Vec::new();
                while let Some(w) = self.stack.pop() {
                    self.on_stack[w] = false;
                    comp.push(w);
                    if w == v {
                        break;
                    }
                }
                self.sccs.push(comp);
            }
        }
    }

    let mut t = Tarjan {
        indices: vec![None; n],
        low: vec![0; n],
        on_stack: vec![false; n],
        ..Default::default()
    };
    for v in 0..n {
        if t.indices[v].is_none() {
            t.strong(v, &neighbours);
        }
    }

    let mut loops = Vec::new();
    for comp in t.sccs {
        let self_loop = comp.len() == 1 && neighbours[comp[0]].contains(&comp[0]);
        if comp.len() > 1 || self_loop {
            let mut names: Vec<String> = comp.iter().map(|&i| ids[i].clone()).collect();
            names.sort();
            loops.push(names);
        }
    }
    loops
}

/// The model projected onto its *observable* nodes (service/user tasks). This is the
/// reference against which mined trace behaviour is checked for conformance: the trace only
/// records task executions, so the model's allowed behaviour is expressed as task-to-task
/// transitions, collapsing the gateways and events that sit between tasks.
pub(crate) struct ModelTaskGraph {
    pub process_id: String,
    /// Every service/user task id in the model.
    pub tasks: HashSet<String>,
    /// For each task, the set of tasks reachable next via paths of only non-task nodes
    /// (gateways, events, subprocess/boundary edges) — i.e. the transitions the model permits.
    pub allowed: HashMap<String, HashSet<String>>,
    /// Tasks the model can reach first from the start event (legal opening tasks).
    pub start_tasks: HashSet<String>,
    /// Tasks from which an end event is reachable without an intervening task (legal closing tasks).
    pub end_tasks: HashSet<String>,
}

/// Build the [`ModelTaskGraph`] by collapsing the structural graph onto its task nodes.
pub(crate) fn model_task_graph(xml: &str) -> Result<ModelTaskGraph, String> {
    let (def, _) = first_def(xml)?;
    let adj = adjacency(&def);
    let is_task_id = |id: &str| def.element(id).map(|e| is_task(&e.kind)).unwrap_or(false);
    let is_end_id = |id: &str| {
        def.element(id).is_some_and(|e| match &e.kind {
            ElementKind::EndEvent | ElementKind::TerminateEndEvent => true,
            // An escalation throw with no outgoing flow is an escalation *end*
            // event (terminal); one with outgoing is an intermediate throw and
            // is not a closing node.
            ElementKind::EscalationThrowEvent { .. } => e.outgoing.is_empty(),
            _ => false,
        })
    };
    let tasks: HashSet<String> = def
        .elements
        .iter()
        .filter(|(_, e)| is_task(&e.kind))
        .map(|(id, _)| id.clone())
        .collect();

    // From a starting frontier, walk forward through only non-task nodes, recording the
    // first task reached on each path (and whether any path reaches an end event).
    let next_tasks = |frontier: Vec<String>| -> (HashSet<String>, bool) {
        let mut found = HashSet::new();
        let mut reached_end = false;
        let mut visited = HashSet::new();
        let mut stack = frontier;
        while let Some(n) = stack.pop() {
            if !visited.insert(n.clone()) {
                continue;
            }
            if is_end_id(&n) {
                reached_end = true;
                continue;
            }
            if is_task_id(&n) {
                found.insert(n.clone()); // stop: don't expand past a task
                continue;
            }
            if let Some(nexts) = adj.get(&n) {
                for s in nexts {
                    stack.push(s.clone());
                }
            }
        }
        (found, reached_end)
    };

    let mut allowed = HashMap::new();
    let mut end_tasks = HashSet::new();
    for t in &tasks {
        let frontier = adj.get(t).cloned().unwrap_or_default();
        let (succ, reached_end) = next_tasks(frontier);
        if reached_end {
            end_tasks.insert(t.clone());
        }
        allowed.insert(t.clone(), succ);
    }
    let (start_tasks, _) = next_tasks(vec![def.start_event.clone()]);

    Ok(ModelTaskGraph {
        process_id: def.id.clone(),
        tasks,
        allowed,
        start_tasks,
        end_tasks,
    })
}

/// `read_model` — a compact, deterministic structural view of the process: the start
/// event, per-kind counts, and every node with its kind, key attributes (service-task
/// `jobType`, boundary attachment, timer/message details), incoming count, outgoing
/// targets (flagged conditional), reachability, and gateway split/join role.
pub fn read_model(xml: &str) -> Result<Value, String> {
    let (def, def_count) = first_def(xml)?;
    let (counts, nodes) = node_view(&def);
    let defs = parse_bpmn(xml).map_err(|e| format!("model failed to parse: {e:?}"))?;
    let expandable = has_call_activities(&defs);
    let note = if expandable {
        "Node ids and serviceTask jobTypes are the same keys the trace tables use \
         (jobs.element_id / jobs.job_type, incidents.element_id) — join structure to runtime \
         with query_traces. This model is a multi-stage ORCHESTRATOR: callActivity nodes are \
         opaque here (see each node's calledElement). The trace tables address the inner tasks \
         with Parent$Child ids (e.g. Phase2_DocumentRequest$Task_SendRefreshRequest). Call \
         read_model with expand:true to inline every phase into one Parent$Child graph that \
         matches those ids, or read_model_xml {process} to see a phase's raw BPMN."
    } else {
        "Node ids and serviceTask jobTypes are the same keys the trace tables use \
         (jobs.element_id / jobs.job_type, incidents.element_id) — join structure to runtime \
         with query_traces."
    };
    Ok(json!({
        "processId": def.id,
        "startEvent": def.start_event,
        "definitionCount": def_count,
        "expandable": expandable,
        "counts": counts,
        "nodes": nodes,
        "note": note,
    }))
}

/// The inlined (expanded) structural view: every call activity is spliced in so
/// the node ids are the `Parent$Child` keys the trace tables carry — letting the
/// droid join a slow/incident-prone `element_id` straight to a node it can see.
pub fn read_model_expanded(xml: &str) -> Result<Value, String> {
    let def_count = parse_bpmn(xml)
        .map_err(|e| format!("model failed to parse: {e:?}"))?
        .len();
    let def = inline_definition(xml, None)?;
    let (counts, nodes) = node_view(&def);
    Ok(json!({
        "processId": def.id,
        "startEvent": def.start_event,
        "definitionCount": def_count,
        "expanded": true,
        "counts": counts,
        "nodes": nodes,
        "note": "EXPANDED view: call activities are inlined, so node ids are the Parent$Child \
                 keys the trace tables use (jobs.element_id / incidents.element_id). Join any \
                 slow or incident-prone element_id directly to a node here, then fix it with \
                 edit_model (use process:\"<calledElement>\" to edit inside a phase).",
    }))
}

/// Build the per-kind counts and the node list for a (possibly inlined) definition.
fn node_view(def: &ProcessDefinition) -> (BTreeMap<&'static str, usize>, Vec<Value>) {
    let adj = adjacency(def);
    let reachable = reachable_from_start(def, &adj);

    let mut ids: Vec<&String> = def.elements.keys().collect();
    ids.sort();

    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut nodes = Vec::with_capacity(ids.len());
    for id in &ids {
        let el: &Element = &def.elements[*id];
        let label = kind_label(&el.kind);
        *counts.entry(label).or_insert(0) += 1;
        let incoming = def.incoming_count(id);
        let outgoing: Vec<Value> = el
            .outgoing
            .iter()
            .map(|f| json!({ "to": f.to, "conditional": f.condition.is_some() }))
            .collect();
        let mut node = json!({
            "id": id,
            "kind": label,
            "incoming": incoming,
            "outgoing": outgoing,
            "reachable": reachable.contains(*id),
        });
        if let Some(obj) = node.as_object_mut() {
            if is_gateway(&el.kind) {
                let role = match (incoming > 1, el.outgoing.len() > 1) {
                    (true, true) => "join+split",
                    (true, false) => "join",
                    (false, true) => "split",
                    (false, false) => "pass",
                };
                obj.insert("gatewayRole".into(), json!(role));
            }
            for (k, v) in kind_extras(&el.kind) {
                obj.insert(k, v);
            }
        }
        nodes.push(node);
    }
    (counts, nodes)
}

fn finding(severity: &str, code: &str, element: Option<&str>, message: String) -> Value {
    json!({
        "severity": severity,
        "code": code,
        "element": element,
        "message": message,
    })
}

/// `analyze_model` — deterministic static checks over the model structure. Findings are
/// **advisory** (heuristic where noted) and reference element ids that join to the trace
/// data, so the persona can quantify each structural risk against runtime.
pub fn analyze_model(xml: &str) -> Result<Value, String> {
    let (def, _) = first_def(xml)?;
    let adj = adjacency(&def);
    let rev = predecessors(&adj);
    let reachable = reachable_from_start(&def, &adj);
    let loops = loop_components(&def, &adj);

    let mut ids: Vec<&String> = def.elements.keys().collect();
    ids.sort();

    let mut findings = Vec::new();

    // No explicit end event. An escalation throw with no outgoing flow is an
    // escalation *end* event, so it counts as a terminal node here — otherwise a
    // process whose only terminal is an escalation end would draw a false
    // `no-end-event` warning (#1173).
    let end_events = ids
        .iter()
        .filter(|id| {
            let el = &def.elements[**id];
            matches!(
                el.kind,
                ElementKind::EndEvent | ElementKind::TerminateEndEvent
            ) || (matches!(el.kind, ElementKind::EscalationThrowEvent { .. })
                && el.outgoing.is_empty())
        })
        .count();
    if end_events == 0 {
        findings.push(finding(
            "warn",
            "no-end-event",
            None,
            "The process has no end event; instances complete implicitly when their last \
             token is consumed, which is easy to misread."
                .into(),
        ));
    }

    for id in &ids {
        let el = &def.elements[*id];
        let kind = &el.kind;

        // Unreachable from the start event (start event itself always counts as reached).
        if !reachable.contains(*id) {
            findings.push(finding(
                "warn",
                "unreachable-node",
                Some(id),
                format!(
                    "'{id}' ({}) is not reachable from the start event — it can never execute.",
                    kind_label(kind)
                ),
            ));
        }

        // A non-end flow node with no outgoing flow silently drops its token.
        // An escalation throw with no outgoing flow is an escalation *end* event
        // (terminal), so — like a plain end event — it must not be flagged as a
        // dead end (#1173).
        let is_event_end = matches!(kind, ElementKind::EndEvent | ElementKind::TerminateEndEvent)
            || (matches!(kind, ElementKind::EscalationThrowEvent { .. }) && el.outgoing.is_empty());
        if !is_event_end && el.outgoing.is_empty() && !is_boundary(kind) {
            // Sub-process inner ends and the like aside, a task/gateway with no exit is a
            // dead end.
            if !matches!(kind, ElementKind::SubProcess { .. }) {
                findings.push(finding(
                    "warn",
                    "dead-end",
                    Some(id),
                    format!(
                        "'{id}' ({}) has no outgoing flow and is not an end event — its token \
                         vanishes (implicit end).",
                        kind_label(kind)
                    ),
                ));
            }
        }

        // Conditional sequence flow leaving a node that is NOT a condition-routed
        // gateway: this engine only honours flow conditions on an exclusive (XOR)
        // or inclusive (OR) split. A condition on a service task's / event's /
        // parallel split's outgoing flow is silently ignored — a common authoring
        // corruption where branch conditions get moved off the gateway onto a
        // downstream task (the routing then breaks, but no gateway-no-default
        // warning fires). Flag it so the model fixes the topology.
        if !matches!(
            kind,
            ElementKind::ExclusiveGateway | ElementKind::InclusiveGateway
        ) && el.outgoing.iter().any(|f| f.condition.is_some())
        {
            let conds = el.outgoing.iter().filter(|f| f.condition.is_some()).count();
            findings.push(finding(
                "warn",
                "condition-on-non-gateway",
                Some(id),
                format!(
                    "'{id}' ({}) has {conds} conditional outgoing flow(s), but only an \
                     exclusive (XOR) or inclusive (OR) gateway evaluates flow conditions here — \
                     these conditions are ignored and routing is wrong. Put the branch conditions \
                     on an exclusive (XOR) or inclusive (OR) gateway, not on this node.",
                    kind_label(kind)
                ),
            ));
        }

        match kind {
            // A condition-routed split (XOR or OR) with every branch guarded: if no
            // condition matches and there is no default flow, the token has nowhere
            // to go (an exclusive gateway gets stuck; an inclusive gateway raises a
            // `NoMatchingSequenceFlow` incident). Same defect, so one
            // gateway-neutral advisory covers both condition-routed kinds.
            ElementKind::ExclusiveGateway | ElementKind::InclusiveGateway
                if el.outgoing.len() > 1 =>
            {
                let has_default = el.outgoing.iter().any(|f| f.condition.is_none());
                if !has_default {
                    findings.push(finding(
                        "warn",
                        "gateway-no-default",
                        Some(id),
                        format!(
                            "{} '{id}' splits {} ways but every flow is \
                             conditional (no default) — if no condition holds the token gets \
                             stuck.",
                            kind_label(kind),
                            el.outgoing.len()
                        ),
                    ));
                }
            }
            // Service task with no error/timeout boundary: a failing or hung job raises an
            // incident with no modelled recovery path.
            ElementKind::ServiceTask { job_type, .. } => {
                let guarded = def.elements.values().any(|b| {
                    b.kind.attached_to() == Some(id.as_str())
                        && matches!(
                            b.kind,
                            ElementKind::ErrorBoundaryEvent { .. }
                                | ElementKind::TimerBoundaryEvent { .. }
                        )
                });
                if !guarded {
                    findings.push(finding(
                        "info",
                        "service-task-unguarded",
                        Some(id),
                        format!(
                            "Service task '{id}' (job type '{job_type}') has no error or timer \
                             boundary event — a failing or stuck job has no modelled recovery; \
                             check incidents for this element in the traces."
                        ),
                    ));
                }
            }
            _ => {}
        }

        // Join hazards (heuristic, via ancestry).
        if matches!(kind, ElementKind::ParallelGateway) && def.incoming_count(id) > 1 {
            let anc = ancestors(id, &rev);
            let conditional_split_upstream = anc.iter().any(|a| {
                def.elements
                    .get(a)
                    .map(|e| {
                        // A condition-routed split (XOR or OR) may activate only
                        // some of its outgoing branches, so a downstream parallel
                        // (AND) join that waits for all of them can deadlock.
                        matches!(
                            e.kind,
                            ElementKind::ExclusiveGateway | ElementKind::InclusiveGateway
                        ) && e.outgoing.len() > 1
                    })
                    .unwrap_or(false)
            });
            if conditional_split_upstream {
                findings.push(finding(
                    "info",
                    "parallel-join-may-deadlock",
                    Some(id),
                    format!(
                        "Parallel (AND) join '{id}' waits for all incoming branches, but a \
                         conditional (XOR/OR) split upstream may activate only some of them — \
                         risk of a token waiting forever. Verify branch arrival in the traces."
                    ),
                ));
            }
        }
        if matches!(kind, ElementKind::ExclusiveGateway) && def.incoming_count(id) > 1 {
            let anc = ancestors(id, &rev);
            let parallel_split_upstream = anc.iter().any(|a| {
                def.elements
                    .get(a)
                    .map(|e| matches!(e.kind, ElementKind::ParallelGateway) && e.outgoing.len() > 1)
                    .unwrap_or(false)
            });
            if parallel_split_upstream {
                findings.push(finding(
                    "info",
                    "exclusive-join-of-parallel",
                    Some(id),
                    format!(
                        "Exclusive (XOR) join '{id}' merges branches a parallel split fanned \
                         out — it does NOT synchronise, so each arriving token passes through \
                         and the downstream may run more than once."
                    ),
                ));
            }
        }
    }

    // Rework / retry loops.
    for comp in &loops {
        findings.push(finding(
            "info",
            "rework-loop",
            comp.first().map(|s| s.as_str()),
            format!(
                "Elements {comp:?} form a loop (rework/retry cycle). Measure how often it \
                 re-executes per instance in the traces — a hot loop inflates latency and cost."
            ),
        ));
    }

    // Severity tallies for a quick read.
    let warns = findings.iter().filter(|f| f["severity"] == "warn").count();
    let infos = findings.iter().filter(|f| f["severity"] == "info").count();

    Ok(json!({
        "processId": def.id,
        "metrics": {
            "nodes": def.elements.len(),
            "endEvents": end_events,
            "loops": loops.len(),
        },
        "findingCount": findings.len(),
        "warnings": warns,
        "infos": infos,
        "findings": findings,
        "note": "Findings are advisory; some join hazards are heuristic. Element ids join to \
                 the trace tables — quantify each risk with query_traces before advising.",
    }))
}

/// Does this XML carry a `<…:definitions>` root element? LLM-authored variants are sometimes
/// a bare `<bpmn:process>` fragment (xmlns decls hoisted onto the process) with no definitions
/// wrapper — the engine parser and bpmn-js both reject those.
fn has_definitions_root(xml: &str) -> bool {
    for part in xml.split('<') {
        let name = part
            .trim_start_matches('/')
            .trim_start()
            .split(|c: char| c.is_whitespace() || c == '>' || c == '/')
            .next()
            .unwrap_or("");
        let local = name.rsplit(':').next().unwrap_or(name);
        if local.eq_ignore_ascii_case("definitions") {
            return true;
        }
    }
    false
}

/// Wrap a bare `<…:process>` fragment in a synthesized `<bpmn:definitions>` envelope so it can be
/// parsed and analysed. Returns `(xml, wrapped)`; a document that already has a definitions root is
/// returned unchanged. Mirrors the cockpit's `ensureBpmnDefinitions` render-side normalization.
fn ensure_definitions(xml: &str) -> (String, bool) {
    let trimmed = xml.trim();
    if trimmed.is_empty() || has_definitions_root(trimmed) {
        return (xml.to_string(), false);
    }
    let body = match trimmed.strip_prefix("<?xml") {
        Some(rest) => rest
            .find("?>")
            .map(|i| rest[i + 2..].trim_start())
            .unwrap_or(trimmed),
        None => trimmed,
    };
    let wrapped = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <bpmn:definitions xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" \
         xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" \
         xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\" \
         xmlns:di=\"http://www.omg.org/spec/DD/20100524/DI\" \
         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
         xmlns:zeebe=\"http://camunda.org/schema/zeebe\" \
         targetNamespace=\"http://bpmn.io/schema/bpmn\">\n{body}\n</bpmn:definitions>"
    );
    (wrapped, true)
}

/// Map a known BPMN deploy/parse error to a concrete, actionable fix, so the model stops repeating
/// the same authoring mistake. Shared by `validate_model` and the experiment replay scorecard.
pub fn deploy_fix_hint(err: &str) -> Option<String> {
    if err.contains("InvalidBoundaryEvent") && err.contains("unknown error") {
        return Some(
            "The error boundary event has an empty or unknown errorRef. A BPMN error \
             boundary needs BOTH a top-level `<bpmn:error id=\"E_X\" errorCode=\"...\"/>` \
             definition AND `<bpmn:errorEventDefinition errorRef=\"E_X\"/>` on the boundary \
             event referencing that id (errorRef points at the error's `id`, not its \
             `errorCode`). To model a RETRY, prefer a timer boundary event \
             (interrupting=false) that loops back to the task, or reuse the model's existing \
             error definition — do not leave errorRef empty."
                .into(),
        );
    }
    // A sequence flow points at a node id the process never declares. The most common cause is an
    // error boundary authored as `<bpmn:errorBoundaryEvent>` (NOT a real BPMN element) — the engine
    // never creates the node, so a flow whose sourceRef/targetRef names it dangles.
    if err.contains("unknown source element") || err.contains("unknown target element") {
        return Some(
            "A sequence flow references a node id that the process never declares. Most common \
             cause: an error boundary written as `<bpmn:errorBoundaryEvent …>` — that element does \
             NOT exist in BPMN, so the node is never created and the flow referencing it dangles. \
             Use `<bpmn:boundaryEvent id=\"X\" attachedToRef=\"Task\"><bpmn:errorEventDefinition \
             errorRef=\"E\"/></bpmn:boundaryEvent>`. Otherwise check for a typo: every sequenceFlow \
             sourceRef/targetRef must match a declared node id exactly."
                .into(),
        );
    }
    None
}

/// Auto-heal the highest-frequency LLM BPMN authoring mistake **before** the model is parsed, so
/// the experimenter iterates on process *semantics* instead of thrashing on syntax. The runtime
/// error is pulled forward (and fixed) at the authoring boundary, IDE-red-squiggle style.
///
/// `<…:errorBoundaryEvent>` is not a real BPMN element — the engine models an error boundary as a
/// `boundaryEvent` carrying a nested `errorEventDefinition`. When a model uses it, the boundary node
/// is never created and every sequence flow referencing it fails with "unknown source element". We
/// rename it in place and report what changed. (`errorBoundaryEvent` never legitimately occurs as a
/// substring, so a plain rename is safe.) Returns `(healed_xml, notes)`; `notes` is empty when
/// nothing was touched.
pub fn normalize_authoring(xml: &str) -> (String, Vec<String>) {
    let mut notes = Vec::new();
    let mut out = xml.to_string();
    if out.contains("errorBoundaryEvent") {
        out = out.replace("errorBoundaryEvent", "boundaryEvent");
        notes.push(
            "Renamed <errorBoundaryEvent> to <boundaryEvent>: errorBoundaryEvent is not a valid \
             BPMN element. An error boundary is a <bpmn:boundaryEvent attachedToRef=\"Task\"> with \
             a nested <bpmn:errorEventDefinition errorRef=\"…\"/>."
                .to_string(),
        );
    }
    (out, notes)
}

/// Lint for the silent `zeebe:taskDefinition` ATTRIBUTE mistake. Written as an attribute on a
/// serviceTask (`<…:serviceTask … zeebe:taskDefinition="x">`), the engine **ignores it** and the
/// job type silently defaults to the task **id** — so the variant's worker never binds to the
/// recorded job types (it surfaces as an uncovered / brand-new worker, derailing the scorecard).
/// The engine only reads the *child element* form `<zeebe:taskDefinition type="x"/>`. The element
/// form writes `taskDefinition ` followed by ` type=`, so a literal `taskDefinition=` (no space)
/// uniquely identifies the attribute misuse. Returns one advisory finding when present.
pub(crate) fn lint_task_definition_attribute(xml: &str) -> Vec<Value> {
    let count = xml.matches("taskDefinition=").count();
    if count == 0 {
        return Vec::new();
    }
    vec![finding(
        "warn",
        "task-definition-as-attribute",
        None,
        format!(
            "{count} service task(s) declare zeebe:taskDefinition as an ATTRIBUTE \
             (zeebe:taskDefinition=\"…\"). The engine IGNORES this — the job type silently \
             defaults to the task id, so the worker will not bind to recorded job types (it shows \
             up as uncovered / a new worker). Move it to the child element form: \
             <bpmn:extensionElements><zeebe:taskDefinition type=\"your-job-type\"/></bpmn:extensionElements>."
        ),
    )]
}

/// Explain the **"the simulate engine uses the element id as the job type"** symptom.
///
/// When a candidate's serviceTask carries no parseable `zeebe:taskDefinition` *child*
/// element, the engine has nothing to bind and the job type silently defaults to the
/// task **id**. Simulate then reports that id under `uncoveredJobTypes` /
/// `requiresNewWorkers`, which reads like an engine bug ("it matched on element_id!")
/// — but it is the taskDefinition-binding mistake surfacing one step downstream.
///
/// Given the (healed) candidate XML and the job types simulate flagged as uncovered,
/// this returns an actionable hint for every uncovered job type that exactly equals a
/// serviceTask id in the model — pulling the runtime confusion forward into a crisp,
/// fixable signal. Returns an empty vec on a parse failure or when no uncovered job
/// type matches a task id (the normal, correctly-bound case).
pub(crate) fn job_type_binding_hints(xml: &str, uncovered: &[String]) -> Vec<Value> {
    if uncovered.is_empty() {
        return Vec::new();
    }
    let Ok((def, _)) = first_def(xml) else {
        return Vec::new();
    };
    let mut hints = Vec::new();
    for (id, el) in &def.elements {
        if let ElementKind::ServiceTask { job_type, .. } = &el.kind {
            // The job type defaulted to the task id *and* simulate flagged it as a
            // worker history never recorded — the binding-mistake signature.
            if job_type == id && uncovered.iter().any(|u| u == id) {
                hints.push(finding(
                    "warn",
                    "job-type-defaulted-to-task-id",
                    Some(id),
                    format!(
                        "Job type '{id}' equals the serviceTask id — this is NOT an engine bug. \
                         The engine fell back to the element id because '{id}' has no parseable \
                         <zeebe:taskDefinition> CHILD element, so its worker never bound to the \
                         recorded job types (it shows up as uncovered / requires-new-worker). Fix \
                         the binding: either set it with edit_model \
                         {{\"op\":\"set_task_job_type\",\"task\":\"{id}\",\"jobType\":\"<recorded-job-type>\"}} \
                         or write the child-element form \
                         <bpmn:extensionElements><zeebe:taskDefinition type=\"<recorded-job-type>\"/></bpmn:extensionElements> \
                         (a taskDefinition ATTRIBUTE is ignored). Use a job type that matches the \
                         recorded dataset so the existing worker output is replayed."
                    ),
                ));
            }
        }
    }
    hints.sort_by(|a, b| a["element"].as_str().cmp(&b["element"].as_str()));
    hints
}

/// `validate_model` — a cheap, dataset-independent **lint/validate** pass over a candidate BPMN
/// model. It parses the XML with the engine's own parser (the same one production deploys with),
/// surfacing structural mistakes the LLM commonly makes — a dangling `errorRef`, a missing
/// `<bpmn:definitions>` root, unparseable XML — before the model is ever simulated or deployed. A
/// bare `<bpmn:process>` fragment is wrapped for analysis (with a warning to emit a full document).
/// On a clean parse it folds in the full [`analyze_model`] structural findings.
pub fn validate_model(xml: &str) -> Result<Value, String> {
    let (healed, fixes) = normalize_authoring(xml);
    let lint = lint_task_definition_attribute(&healed);
    let (doc, wrapped) = ensure_definitions(&healed);
    match analyze_model(&doc) {
        Ok(mut v) => {
            if let Some(obj) = v.as_object_mut() {
                obj.insert("valid".into(), Value::Bool(true));
                if let Some(arr) = obj.get_mut("findings").and_then(|f| f.as_array_mut()) {
                    // Surface the silent taskDefinition-attribute mistake (advisory) and report any
                    // syntax we auto-healed, so the model learns rather than re-submitting it.
                    for l in lint.into_iter().rev() {
                        arr.insert(0, l);
                    }
                    for note in fixes.iter().rev() {
                        arr.insert(0, finding("info", "auto-fixed", None, note.clone()));
                    }
                }
                if !fixes.is_empty() {
                    obj.insert("autoFixed".into(), json!(fixes));
                }
                if wrapped {
                    if let Some(arr) = obj.get_mut("findings").and_then(|f| f.as_array_mut()) {
                        arr.insert(
                            0,
                            finding(
                                "warn",
                                "missing-definitions-root",
                                None,
                                "Model was a bare <bpmn:process> fragment with no \
                                 <bpmn:definitions> root; it was wrapped for analysis. Emit a \
                                 full <bpmn:definitions …> document so it deploys and renders \
                                 without normalization."
                                    .into(),
                            ),
                        );
                    }
                    obj.insert("wrapped".into(), Value::Bool(true));
                }
                // Recompute the headline counts so they reflect the findings we just inserted.
                if let Some(arr) = obj.get("findings").and_then(|f| f.as_array()) {
                    let (mut warns, mut infos) = (0u64, 0u64);
                    for f in arr {
                        match f.get("severity").and_then(|s| s.as_str()) {
                            Some("warn") => warns += 1,
                            Some("info") => infos += 1,
                            _ => {}
                        }
                    }
                    obj.insert("findingCount".into(), json!(arr.len()));
                    obj.insert("warnings".into(), json!(warns));
                    obj.insert("infos".into(), json!(infos));
                }
            }
            Ok(v)
        }
        Err(parse_error) => {
            let fix = deploy_fix_hint(&parse_error);
            let mut message = parse_error.clone();
            if let Some(h) = &fix {
                message.push_str("\nFix: ");
                message.push_str(h);
            }
            let mut findings = Vec::new();
            for note in &fixes {
                findings.push(finding("info", "auto-fixed", None, note.clone()));
            }
            findings.push(finding("error", "parse-error", None, message));
            Ok(json!({
                "valid": false,
                "parseError": parse_error,
                "fix": fix,
                "autoFixed": fixes,
                "findingCount": findings.len(),
                "warnings": 0,
                "infos": fixes.len(),
                "findings": findings,
                "note": "The model does not parse and cannot be deployed or simulated. Fix the \
                         error above and re-run validate_model.",
            }))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Structured authoring: serialize → edit → re-validate
//
// The Experiment Designer is good at process *ideas* but poor at one-shotting whole BPMN
// documents by hand (Investigation 1 thrashed on `<errorBoundaryEvent>` and a
// `zeebe:taskDefinition` attribute it could not see was wrong). `edit_model` flips the
// authoring contract: the model proposes high-level, *validated* operations against the
// CURRENT model and OUR code owns XML correctness — emitting engine-parseable XML and
// re-parsing it before returning. The LLM never types raw whole-document XML again; it
// composes a variant from patches that cannot produce the syntax mistakes it falls into.
// ─────────────────────────────────────────────────────────────────────────────

/// Escape the five predefined XML entities for safe emission into attribute values and text
/// (the inverse of the engine tokenizer's `unescape`). `&` first so we don't double-escape.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// A safe id fragment: keep alphanumerics and `_`/`-`, map everything else to `_`.
fn id_fragment(s: &str) -> String {
    let frag: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if frag.is_empty() {
        "x".to_string()
    } else {
        frag
    }
}

/// Minimal XML entity un-escaping for the five predefined entities, so a `name="…&amp;…"` read
/// from a base model round-trips cleanly when re-escaped on emit.
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Read one attribute value (`key="…"` or `key='…'`) from the inside of a start tag, honouring
/// word boundaries so `name=` doesn't match e.g. `messageName=`. Returns the un-escaped value.
fn tag_attr(tag: &str, key: &str) -> Option<String> {
    let mut from = 0usize;
    while let Some(rel) = tag[from..].find(key) {
        let pos = from + rel;
        let before_ok = pos == 0
            || tag.as_bytes()[pos - 1].is_ascii_whitespace()
            || tag.as_bytes()[pos - 1] == b'<';
        let after = tag[pos + key.len()..].trim_start();
        if before_ok && after.starts_with('=') {
            let rest = after[1..].trim_start();
            let q = rest.chars().next()?;
            if q == '"' || q == '\'' {
                let body = &rest[1..];
                let end = body.find(q)?;
                return Some(xml_unescape(&body[..end]));
            }
        }
        from = pos + key.len();
    }
    None
}

/// Harvest a map of `element id -> human label` from a BPMN document by scanning every start tag
/// that carries both `id=` and a non-empty `name=`. The engine model drops names (it is purely
/// semantic), so `edit_model` re-reads them here to preserve operator-facing labels across an edit.
pub(crate) fn parse_element_names(xml: &str) -> HashMap<String, String> {
    let mut names = HashMap::new();
    let mut i = 0usize;
    while let Some(rel) = xml[i..].find('<') {
        let start = i + rel;
        let Some(grel) = xml[start..].find('>') else {
            break;
        };
        let end = start + grel;
        let tag = &xml[start + 1..end];
        i = end + 1;
        if tag.starts_with('/') || tag.starts_with('!') || tag.starts_with('?') {
            continue;
        }
        if let (Some(id), Some(name)) = (tag_attr(tag, "id"), tag_attr(tag, "name")) {
            if !name.trim().is_empty() {
                names.entry(id).or_insert(name);
            }
        }
    }
    names
}

// ── Slice 4 (ADR 0002): nano:* extension preservation across re-emission ────────
//
// The serialiser reconstructs BPMN from the parsed `ProcessDefinition`, and
// engine-core deliberately keeps no state for extension elements outside a
// small hand-picked set (zeebe:taskDefinition, zeebe:ioMapping, …). Anything
// under `<bpmn:extensionElements>` from an unknown namespace — the whole
// `nano:*` semantic-annotation namespace this ADR introduces, and anything
// else authored by a tool we don't know about — silently disappears on the
// next re-serialisation. That's the loss we're closing here.
//
// Approach: a pair of pure-string helpers over the retained source XML
// (`ProcessDefinition::xml` is exactly this — the original bytes as parsed,
// kept for round-trip purposes). We scan the source for every
// `<bpmn:extensionElements>` block and record, per element id, the raw XML
// of every `nano:*` (or configurably any unknown-namespace) child element.
// After the serialiser has emitted its fresh XML we splice those fragments
// back into the emitted `<extensionElements>` blocks — creating one if the
// element didn't previously have any known extensions — and ensure the
// definitions root declares the `nano` namespace.
//
// Being a post-pass on emitted text has two important properties: it never
// touches engine-core (this slice is scoped to `processos`) and it degrades
// safely to a no-op when the source is empty (fresh models authored
// programmatically) or contains no `nano:*` content.

/// The canonical namespace URI for `nano:*` semantic-annotation extensions
/// per ADR 0002.
pub const NANO_NAMESPACE_URI: &str = "http://nano.camunda.io/schema/semantic/1.0";

/// Parse `xml` for `nano:*` extension children of `<bpmn:extensionElements>`
/// and return a map from *owning-element id* to the concatenated raw XML of
/// those children (verbatim, indentation-agnostic — we re-indent on inject).
///
/// The owning element is the nearest ancestor with an `id=` attribute at the
/// same nesting position as the `<extensionElements>` block (that is, its
/// parent). This is a lightweight XML scan — no full parser — because the
/// serialiser we're pairing with is also lightweight and BPMN documents in
/// this codebase are small.
pub(crate) fn extract_nano_extensions(xml: &str) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    // A stack of open elements: (id, whether we already recorded a nano
    // block for this element). We only ever need the id of the immediate
    // parent of a `<bpmn:extensionElements>` block.
    let mut open_ids: Vec<Option<String>> = Vec::new();
    let bytes = xml.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        // Skip past any content that isn't a tag start.
        let Some(rel) = xml[i..].find('<') else { break };
        let start = i + rel;
        // Comments / declarations / CDATA — skip whole span.
        if xml[start..].starts_with("<!--") {
            match xml[start..].find("-->") {
                Some(k) => {
                    i = start + k + 3;
                    continue;
                }
                None => break,
            }
        }
        if xml[start..].starts_with("<![CDATA[") {
            match xml[start..].find("]]>") {
                Some(k) => {
                    i = start + k + 3;
                    continue;
                }
                None => break,
            }
        }
        if xml[start..].starts_with("<?") || xml[start..].starts_with("<!") {
            match xml[start..].find('>') {
                Some(k) => {
                    i = start + k + 1;
                    continue;
                }
                None => break,
            }
        }
        let Some(grel) = xml[start..].find('>') else {
            break;
        };
        let end = start + grel;
        let inner = &xml[start + 1..end];
        i = end + 1;

        if let Some(name) = inner.strip_prefix('/') {
            // Closing tag: pop the matching open. We only track ids so it's
            // fine if names aren't validated — a mismatched document would
            // fail to parse anyway.
            let _ = name;
            open_ids.pop();
            continue;
        }
        let self_closing = inner.ends_with('/');
        let inner_trim = if self_closing {
            inner.trim_end_matches('/').trim_end()
        } else {
            inner
        };
        let local_name = inner_trim
            .split_ascii_whitespace()
            .next()
            .unwrap_or(inner_trim);

        // If this is an <extensionElements> open tag, look at its content
        // (from `end + 1` to the matching close) and pull out every
        // top-level `nano:*` child.
        if local_name.ends_with(":extensionElements") || local_name == "extensionElements" {
            if self_closing {
                continue;
            }
            let owner = open_ids.iter().rev().find_map(|o| o.clone());
            if let Some(owner_id) = owner {
                let close_tag_needle = format!("</{local_name}>");
                if let Some(k) = xml[i..].find(&close_tag_needle) {
                    let block = &xml[i..i + k];
                    let mut buf = String::new();
                    collect_nano_children(block, &mut buf);
                    if !buf.is_empty() {
                        out.entry(owner_id).or_default().push_str(&buf);
                    }
                    // Consume everything up to (but not including) the
                    // closing tag; the outer loop will handle the close.
                    i += k;
                    continue;
                }
            }
            // No id in scope, or no close tag found — treat as opaque open
            // and let the normal scan continue (an id will not be needed
            // for its content).
            open_ids.push(None);
            continue;
        }

        if !self_closing {
            let id = tag_attr(inner_trim, "id");
            open_ids.push(id);
        }
    }
    out
}

/// Walk the interior of an `<extensionElements>` block and append every
/// top-level `nano:*` child element (in source form) to `buf`.
fn collect_nano_children(block: &str, buf: &mut String) {
    let mut i = 0usize;
    while i < block.len() {
        let Some(rel) = block[i..].find('<') else {
            break;
        };
        let start = i + rel;
        if block[start..].starts_with("<!--") {
            match block[start..].find("-->") {
                Some(k) => {
                    i = start + k + 3;
                    continue;
                }
                None => break,
            }
        }
        let Some(grel) = block[start..].find('>') else {
            break;
        };
        let end = start + grel;
        let inner = &block[start + 1..end];
        i = end + 1;
        if inner.starts_with('/') {
            continue;
        }
        let local_name = inner
            .trim_end_matches('/')
            .trim_end()
            .split_ascii_whitespace()
            .next()
            .unwrap_or("");
        if !local_name.starts_with("nano:") {
            // Skip past a non-nano element and its close (if any).
            if !inner.ends_with('/') {
                let close_needle = format!("</{local_name}>");
                if let Some(k) = block[i..].find(&close_needle) {
                    i += k + close_needle.len();
                }
            }
            continue;
        }
        // Capture the nano element and its content up to the matching close.
        if inner.ends_with('/') {
            buf.push_str(&block[start..end + 1]);
            buf.push('\n');
        } else {
            let close_needle = format!("</{local_name}>");
            if let Some(k) = block[i..].find(&close_needle) {
                let element_end = i + k + close_needle.len();
                buf.push_str(&block[start..element_end]);
                buf.push('\n');
                i = element_end;
            } else {
                // Malformed — bail on this element.
                break;
            }
        }
    }
}

/// Post-process `emitted` XML to splice preserved `nano:*` extensions back
/// into the corresponding `<bpmn:extensionElements>` blocks, creating one
/// per element that has extensions but no emitted `<extensionElements>`,
/// and ensuring the definitions root declares `xmlns:nano`.
///
/// Returns `emitted` unchanged when `extensions` is empty.
pub(crate) fn preserve_nano_extensions_in(
    emitted: String,
    extensions: &HashMap<String, String>,
) -> String {
    if extensions.is_empty() {
        return emitted;
    }
    let mut out = ensure_nano_namespace(emitted);
    // Splice per element id. We do a simple find-and-replace against the
    // opening tag with `id="<id>"`, which is unambiguous because BPMN ids
    // are unique within a document.
    for (id, fragment) in extensions {
        let needle_id = format!("id=\"{}\"", xml_escape_attr(id));
        let Some(open_start) = out.find(&needle_id) else {
            continue;
        };
        // Walk backwards from the id attribute to find the '<' that opens
        // the element; forwards to find its '>'.
        let Some(lt_offset) = out[..open_start].rfind('<') else {
            continue;
        };
        let Some(gt_offset_rel) = out[open_start..].find('>') else {
            continue;
        };
        let open_end = open_start + gt_offset_rel;
        let open_tag = &out[lt_offset..=open_end];
        let self_closing = open_tag.ends_with("/>");
        let local_name = open_tag[1..]
            .trim_end_matches("/>")
            .trim_end_matches('>')
            .trim_end()
            .split_ascii_whitespace()
            .next()
            .unwrap_or("")
            .to_string();
        if local_name.is_empty() {
            continue;
        }

        let indent = "        "; // Inside an <extensionElements> block.
        let indented_fragment = fragment
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| format!("{indent}{}\n", l.trim()))
            .collect::<String>();

        if self_closing {
            // Convert self-closing to open+close and insert a fresh
            // <extensionElements> with just the nano block.
            let replacement = format!(
                "{}>\n      <bpmn:extensionElements>\n{}      </bpmn:extensionElements>\n    </{}>",
                open_tag[..open_tag.len() - 2].trim_end(),
                indented_fragment,
                local_name
            );
            out.replace_range(lt_offset..=open_end, &replacement);
            continue;
        }

        // Non-self-closing: find the matching close.
        let close_needle = format!("</{local_name}>");
        let search_from = open_end + 1;
        let Some(close_rel) = out[search_from..].find(&close_needle) else {
            continue;
        };
        let close_start = search_from + close_rel;
        let interior = &out[search_from..close_start];

        // If there's already an <extensionElements> block, splice the nano
        // fragment before its closing tag. Otherwise create a fresh one
        // right after the element's opening tag.
        if let Some(ext_rel) = interior.find("<bpmn:extensionElements") {
            let ext_open_start = search_from + ext_rel;
            let Some(ext_open_gt_rel) = out[ext_open_start..].find('>') else {
                continue;
            };
            let ext_open_end = ext_open_start + ext_open_gt_rel;
            // Self-closing <bpmn:extensionElements/> -> expand.
            if out[ext_open_start..=ext_open_end].ends_with("/>") {
                let replacement = format!(
                    "<bpmn:extensionElements>\n{}      </bpmn:extensionElements>",
                    indented_fragment
                );
                out.replace_range(ext_open_start..=ext_open_end, &replacement);
                continue;
            }
            let ext_close_needle = "</bpmn:extensionElements>";
            let Some(ext_close_rel) = out[ext_open_end + 1..].find(ext_close_needle) else {
                continue;
            };
            let ext_close_start = ext_open_end + 1 + ext_close_rel;
            out.insert_str(ext_close_start, &indented_fragment);
        } else {
            let insertion = format!(
                "\n      <bpmn:extensionElements>\n{}      </bpmn:extensionElements>",
                indented_fragment
            );
            out.insert_str(open_end + 1, &insertion);
        }
    }
    out
}

/// Add `xmlns:nano="…"` to the `<bpmn:definitions>` open tag if it isn't
/// already declared. Idempotent — repeated invocations don't accumulate.
fn ensure_nano_namespace(mut xml: String) -> String {
    if xml.contains("xmlns:nano=") {
        return xml;
    }
    let Some(open_start) = xml.find("<bpmn:definitions") else {
        return xml;
    };
    let Some(gt_rel) = xml[open_start..].find('>') else {
        return xml;
    };
    let insertion_point = open_start + "<bpmn:definitions".len();
    xml.insert_str(
        insertion_point,
        &format!(" xmlns:nano=\"{NANO_NAMESPACE_URI}\""),
    );
    // Silence unused warning if the compiler can't prove the tag was closed.
    let _ = gt_rel;
    xml
}

/// Escape a string for use inside an XML attribute value delimited by `"`.
/// Kept separate from [`xml_escape`] because attribute values need `"`
/// escaped as well.
fn xml_escape_attr(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('"', "&quot;")
}

/// Turn a machine id into a readable label: strip a leading kind prefix (`Task_`, `Gateway_`,
/// `Event_`, `Activity_`, `Flow_`…), then split camelCase / snake_case / kebab-case / digit runs
/// into Title-Cased words. `CreditCheck` -> "Credit Check", `verify_kyc` -> "Verify Kyc",
/// `Task_FraudScreen` -> "Fraud Screen". Returns the original id if nothing readable remains.
fn humanize_id(id: &str) -> String {
    let mut s = id;
    for p in [
        "Task_",
        "Activity_",
        "Gateway_",
        "Event_",
        "StartEvent_",
        "EndEvent_",
        "Flow_",
        "BoundaryEvent_",
    ] {
        if let Some(rest) = s.strip_prefix(p) {
            if !rest.is_empty() {
                s = rest;
            }
            break;
        }
    }
    let mut words: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut prev: Option<char> = None;
    for c in s.chars() {
        if c == '_' || c == '-' || c == '.' || c == ' ' {
            if !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
            prev = None;
            continue;
        }
        // Boundary on a lower/digit -> upper transition (camelCase).
        if let Some(p) = prev {
            let split = (p.is_lowercase() || p.is_ascii_digit()) && c.is_uppercase();
            if split && !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
        }
        cur.push(c);
        prev = Some(c);
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    let titled: Vec<String> = words
        .into_iter()
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut ch = w.chars();
            match ch.next() {
                Some(f) => f.to_uppercase().collect::<String>() + ch.as_str(),
                None => w,
            }
        })
        .collect();
    if titled.is_empty() {
        id.to_string()
    } else {
        titled.join(" ")
    }
}

/// Render a duration in milliseconds as an ISO-8601 duration the engine parser accepts
/// (`P[nD]T[nH][nM][nS]`). Sub-second precision is not representable in that grammar, so the
/// value is floored to whole seconds; authoring timers at second granularity round-trips.
fn millis_to_iso8601(ms: u64) -> String {
    let mut rem = ms / 1000;
    let days = rem / 86_400;
    rem %= 86_400;
    let hours = rem / 3_600;
    rem %= 3_600;
    let mins = rem / 60;
    let secs = rem % 60;
    let mut date = String::from("P");
    if days > 0 {
        date.push_str(&format!("{days}D"));
    }
    let mut time = String::new();
    if hours > 0 {
        time.push_str(&format!("{hours}H"));
    }
    if mins > 0 {
        time.push_str(&format!("{mins}M"));
    }
    if secs > 0 || (days == 0 && hours == 0 && mins == 0) {
        time.push_str(&format!("{secs}S"));
    }
    if !time.is_empty() {
        date.push('T');
        date.push_str(&time);
    }
    date
}

/// Collect the definitions-level `<bpmn:error>` declarations a model needs: one per distinct
/// error code carried by an error boundary event. Returns a code → synthesized-error-id map.
fn collect_error_ids(def: &ProcessDefinition) -> BTreeMap<String, String> {
    let mut codes: BTreeMap<String, String> = BTreeMap::new();
    for el in def.elements.values() {
        if let ElementKind::ErrorBoundaryEvent { error_code, .. } = &el.kind {
            codes
                .entry(error_code.clone())
                .or_insert_with(|| format!("Error_{}", id_fragment(error_code)));
        }
    }
    codes
}

/// A definitions-level `<bpmn:message>` declaration needed by a message start/catch/boundary.
struct MessageDecl {
    id: String,
    name: String,
    correlation_key: Option<String>,
}

/// The message declarations a model needs, paired with a lookup from `(name, correlation_key)`
/// to the declaration id (so events differing only in correlation get distinct declarations).
type MessageCollection = (Vec<MessageDecl>, HashMap<(String, Option<String>), String>);

/// Collect the `<bpmn:message>` declarations a model needs, keyed for lookup by
/// `(name, correlation_key)` so events that differ in correlation get distinct declarations.
fn collect_messages(def: &ProcessDefinition) -> MessageCollection {
    let mut decls: Vec<MessageDecl> = Vec::new();
    let mut lookup: HashMap<(String, Option<String>), String> = HashMap::new();
    let mut want: Vec<(String, Option<String>)> = Vec::new();
    let mut ids: Vec<&String> = def.elements.keys().collect();
    ids.sort();
    for id in ids {
        match &def.elements[id].kind {
            ElementKind::MessageStartEvent { message_name } => {
                want.push((message_name.clone(), None));
            }
            ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            }
            | ElementKind::MessageBoundaryEvent {
                message_name,
                correlation_key,
                ..
            } => {
                want.push((message_name.clone(), Some(correlation_key.clone())));
            }
            _ => {}
        }
    }
    for (name, key) in want {
        let entry = (name.clone(), key.clone());
        if lookup.contains_key(&entry) {
            continue;
        }
        let mid = format!("Message_{}_{}", id_fragment(&name), decls.len());
        lookup.insert(entry, mid.clone());
        decls.push(MessageDecl {
            id: mid,
            name,
            correlation_key: key,
        });
    }
    (decls, lookup)
}

/// Collect the `<bpmn:signal>` declarations a model needs, as a lookup from signal name to a
/// synthesized declaration id. Signals correlate by name only, so one declaration per name.
fn collect_signals(def: &ProcessDefinition) -> BTreeMap<String, String> {
    let mut names: BTreeMap<String, String> = BTreeMap::new();
    for el in def.elements.values() {
        let name = match &el.kind {
            ElementKind::SignalIntermediateCatchEvent { signal_name }
            | ElementKind::SignalBoundaryEvent { signal_name, .. } => Some(signal_name),
            _ => None,
        };
        if let Some(name) = name {
            names
                .entry(name.clone())
                .or_insert_with(|| format!("Signal_{}", id_fragment(name)));
        }
    }
    names
}

/// Collect the `<bpmn:escalation>` declarations a model needs, as a lookup from escalation code
/// to a synthesized declaration id. Escalations correlate by code only, so one declaration per
/// code. Empty codes (an unnamed throw or a catch-all boundary) carry no `escalationRef`, so they
/// need no declaration and are skipped.
///
/// Generated ids are allocated **collision-free**: `id_fragment` is not injective (distinct legal
/// codes such as `A/B` and `A_B` both fragment to `A_B`), and a generated `Escalation_…` id could
/// also clash with any id the document already emits — a model element id, the `<bpmn:process>` id,
/// or a (preserved or synthesized) sequence-flow id — either would emit duplicate BPMN ids and
/// ambiguous `escalationRef`s. `reserved` carries that full set of already-emitted ids. Codes are
/// assigned in sorted order (deterministic regardless of the element map's iteration order), each
/// getting the base id or the first free `…_2`, `…_3`, … suffix not already taken by a reserved id
/// or an earlier escalation id.
fn collect_escalations(
    def: &ProcessDefinition,
    reserved: &HashSet<String>,
) -> BTreeMap<String, String> {
    let mut codes: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for el in def.elements.values() {
        match &el.kind {
            ElementKind::EscalationThrowEvent { escalation_code }
            | ElementKind::EscalationBoundaryEvent {
                escalation_code, ..
            } if !escalation_code.is_empty() => {
                codes.insert(escalation_code.clone());
            }
            _ => {}
        }
    }
    // Reserve every id this document will emit — model element ids, the
    // `<bpmn:process>` id, and every (preserved or synthesized) sequence-flow id
    // — so a generated `Escalation_…` declaration id can never shadow one of
    // them (a process/flow literally named `Escalation_OVERLOAD` would otherwise
    // duplicate a `<bpmn:escalation id>`; #1173).
    let mut used: HashSet<String> = reserved.clone();
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for code in codes {
        let id = alloc_unique_id(format!("Escalation_{}", id_fragment(&code)), &mut used);
        out.insert(code, id);
    }
    out
}

/// Allocate `base` — or the first free `base_2`, `base_3`, … — as an id absent from `used`,
/// inserting the chosen id into `used`. Keeps synthesized declaration ids collision-free against
/// each other and against reserved (model element) ids, since the id-deriving `id_fragment` is not
/// injective.
fn alloc_unique_id(base: String, used: &mut HashSet<String>) -> String {
    if used.insert(base.clone()) {
        return base;
    }
    let mut n = 2u32;
    loop {
        let candidate = format!("{base}_{n}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        n += 1;
    }
}
/// `zeebe:loopCharacteristics` extension and optional `completionCondition`) as a
/// child of the activity element. No-op when the element carries no MI. The
/// output round-trips through the engine parser (`parse_bpmn`).
fn emit_multi_instance(el: &Element, out: &mut String) {
    let Some(mi) = &el.multi_instance else {
        return;
    };
    let seq = if mi.sequential {
        " isSequential=\"true\""
    } else {
        ""
    };
    out.push_str(&format!(
        "      <bpmn:multiInstanceLoopCharacteristics{seq}>\n"
    ));
    out.push_str("        <bpmn:extensionElements>\n");
    out.push_str(&format!(
        "          <zeebe:loopCharacteristics inputCollection=\"{}\"",
        xml_escape(&mi.input_collection)
    ));
    if let Some(v) = &mi.input_element {
        out.push_str(&format!(" inputElement=\"{}\"", xml_escape(v)));
    }
    if let Some(v) = &mi.output_collection {
        out.push_str(&format!(" outputCollection=\"{}\"", xml_escape(v)));
    }
    if let Some(v) = &mi.output_element {
        out.push_str(&format!(" outputElement=\"{}\"", xml_escape(v)));
    }
    out.push_str("/>\n");
    out.push_str("        </bpmn:extensionElements>\n");
    if let Some(cc) = &mi.completion_condition {
        out.push_str(&format!(
            "        <bpmn:completionCondition>{}</bpmn:completionCondition>\n",
            xml_escape(cc)
        ));
    }
    out.push_str("      </bpmn:multiInstanceLoopCharacteristics>\n");
}

/// Emit a `zeebe:ioMapping` block (with its nested `zeebe:input`/`zeebe:output`) if the element
/// carries any variable mappings. Emitted INSIDE the owner's `<bpmn:extensionElements>` so the
/// engine parser (which only reads input/output within an open ioMapping on an activity) round-trips
/// them. A no-op when both directions are empty, so nodes without mappings stay terse.
fn emit_io_mapping(el: &Element, out: &mut String) {
    if el.io.inputs.is_empty() && el.io.outputs.is_empty() {
        return;
    }
    out.push_str("        <zeebe:ioMapping>\n");
    for m in &el.io.inputs {
        out.push_str(&format!(
            "          <zeebe:input source=\"{}\" target=\"{}\"/>\n",
            xml_escape(&m.source),
            xml_escape(&m.target)
        ));
    }
    for m in &el.io.outputs {
        out.push_str(&format!(
            "          <zeebe:output source=\"{}\" target=\"{}\"/>\n",
            xml_escape(&m.source),
            xml_escape(&m.target)
        ));
    }
    out.push_str("        </zeebe:ioMapping>\n");
}

/// Serialize one element (and, for a sub-process, its contained children) as BPMN XML. Sequence
/// flows are emitted separately and flat, so this only renders the node and its event/extension
/// definitions. `errors`/`messages`/`signals` provide the synthesized declaration ids to reference.
/// `default_flows` maps a gateway id to the synthesized id of its default outgoing flow.
#[allow(clippy::too_many_arguments)]
fn emit_element(
    def: &ProcessDefinition,
    id: &str,
    errors: &BTreeMap<String, String>,
    messages: &HashMap<(String, Option<String>), String>,
    signals: &BTreeMap<String, String>,
    escalations: &BTreeMap<String, String>,
    children_by_parent: &HashMap<String, Vec<String>>,
    labels: &HashMap<String, String>,
    default_flows: &HashMap<String, String>,
    out: &mut String,
) {
    let el = &def.elements[id];
    let eid = xml_escape(id);
    // Every node carries a human label (operator-set name preserved across the edit, else a
    // readable label derived from the id) so the rendered diagram and the downloaded .bpmn are
    // legible rather than a wall of machine ids.
    let na = format!(
        " name=\"{}\"",
        xml_escape(labels.get(id).map(String::as_str).unwrap_or(id))
    );
    match &el.kind {
        ElementKind::StartEvent => {
            out.push_str(&format!("    <bpmn:startEvent id=\"{eid}\"{na}/>\n"));
        }
        ElementKind::EndEvent => {
            // Round-trip any `zeebe:ioMapping` the model carries (the parser
            // pushes an end event onto the io_stack, so mappings attach to it):
            // a self-closing tag would silently drop them, changing the model on
            // re-parse. Terse self-closing form only when there is nothing to nest.
            if el.io.inputs.is_empty() && el.io.outputs.is_empty() {
                out.push_str(&format!("    <bpmn:endEvent id=\"{eid}\"{na}/>\n"));
            } else {
                out.push_str(&format!("    <bpmn:endEvent id=\"{eid}\"{na}>\n"));
                out.push_str("      <bpmn:extensionElements>\n");
                emit_io_mapping(el, out);
                out.push_str("      </bpmn:extensionElements>\n");
                out.push_str("    </bpmn:endEvent>\n");
            }
        }
        ElementKind::TerminateEndEvent => {
            out.push_str(&format!("    <bpmn:endEvent id=\"{eid}\"{na}>\n"));
            // Preserve any `zeebe:ioMapping` on the terminate end (same io_stack
            // attachment as a plain end event) so the model round-trips; without
            // it, re-parsing the emitted BPMN would drop the mapping. Nano — like
            // Zeebe — does not *apply* io mappings on a terminate end at runtime,
            // but the serializer must not silently mutate the authored model.
            if !el.io.inputs.is_empty() || !el.io.outputs.is_empty() {
                out.push_str("      <bpmn:extensionElements>\n");
                emit_io_mapping(el, out);
                out.push_str("      </bpmn:extensionElements>\n");
            }
            out.push_str("      <bpmn:terminateEventDefinition />\n");
            out.push_str("    </bpmn:endEvent>\n");
        }
        ElementKind::IntermediateThrowEvent => {
            out.push_str(&format!(
                "    <bpmn:intermediateThrowEvent id=\"{eid}\"{na}/>\n"
            ));
        }
        ElementKind::LinkIntermediateThrowEvent { link_name } => {
            out.push_str(&format!(
                "    <bpmn:intermediateThrowEvent id=\"{eid}\"{na}>\n"
            ));
            out.push_str(&format!(
                "      <bpmn:linkEventDefinition name=\"{}\"/>\n",
                xml_escape(link_name)
            ));
            out.push_str("    </bpmn:intermediateThrowEvent>\n");
        }
        ElementKind::LinkIntermediateCatchEvent { link_name } => {
            out.push_str(&format!(
                "    <bpmn:intermediateCatchEvent id=\"{eid}\"{na}>\n"
            ));
            out.push_str(&format!(
                "      <bpmn:linkEventDefinition name=\"{}\"/>\n",
                xml_escape(link_name)
            ));
            out.push_str("    </bpmn:intermediateCatchEvent>\n");
        }
        ElementKind::Task => {
            // An abstract task is a pass-through, but — like the typed tasks — it
            // still round-trips a `zeebe:ioMapping` and/or
            // `multiInstanceLoopCharacteristics` when the model carries them (the
            // parser pushes it onto the io_stack, so both attach to the element).
            // Emit an open tag only when there is something to nest; otherwise a
            // self-closing tag keeps the common case terse.
            let has_io = !el.io.inputs.is_empty() || !el.io.outputs.is_empty();
            if !has_io && el.multi_instance.is_none() {
                out.push_str(&format!("    <bpmn:task id=\"{eid}\"{na}/>\n"));
            } else {
                out.push_str(&format!("    <bpmn:task id=\"{eid}\"{na}>\n"));
                if has_io {
                    out.push_str("      <bpmn:extensionElements>\n");
                    emit_io_mapping(el, out);
                    out.push_str("      </bpmn:extensionElements>\n");
                }
                emit_multi_instance(el, out);
                out.push_str("    </bpmn:task>\n");
            }
        }
        ElementKind::ScriptTask {
            expression,
            result_variable,
        } => {
            out.push_str(&format!("    <bpmn:scriptTask id=\"{eid}\"{na}>\n"));
            out.push_str("      <bpmn:extensionElements>\n");
            out.push_str(&format!(
                "        <zeebe:script expression=\"{}\" resultVariable=\"{}\"/>\n",
                xml_escape(expression),
                xml_escape(result_variable)
            ));
            emit_io_mapping(el, out);
            out.push_str("      </bpmn:extensionElements>\n");
            out.push_str("    </bpmn:scriptTask>\n");
        }
        ElementKind::CallActivity {
            called_process_id,
            propagate_all_parent_variables,
            propagate_all_child_variables,
        } => {
            // Emit the Zeebe-native `zeebe:calledElement` child so the variable
            // propagation flags round-trip through the engine parser (which reads
            // them from that element, not from a `calledElement` attribute). The
            // engine parser pushes the callActivity onto its io_stack, so any
            // authored `zeebe:ioMapping` and/or `multiInstanceLoopCharacteristics`
            // attach to this element too — round-trip them alongside the
            // calledElement so re-parsing the emitted BPMN does not silently drop
            // mappings/MI (the ioMapping nests in the same extensionElements block;
            // the MI block is a direct child of the callActivity).
            out.push_str(&format!("    <bpmn:callActivity id=\"{eid}\"{na}>\n"));
            out.push_str("      <bpmn:extensionElements>\n");
            let unbound_tool = def.adhoc.iter().flat_map(|a| &a.tools).any(|tool| {
                tool.element_id == id
                    && matches!(
                        tool.kind,
                        nanobpmn_engine_core::AdHocToolKind::CallActivity {
                            process_id: None,
                            ..
                        }
                    )
            });
            if !unbound_tool {
                out.push_str(&format!(
                    "        <zeebe:calledElement processId=\"{}\" \
propagateAllParentVariables=\"{propagate_all_parent_variables}\" \
propagateAllChildVariables=\"{propagate_all_child_variables}\"/>\n",
                    xml_escape(called_process_id)
                ));
            }
            emit_io_mapping(el, out);
            out.push_str("      </bpmn:extensionElements>\n");
            emit_multi_instance(el, out);
            out.push_str("    </bpmn:callActivity>\n");
        }
        ElementKind::ExclusiveGateway => {
            let da = default_flows
                .get(id)
                .map(|f| format!(" default=\"{}\"", xml_escape(f)))
                .unwrap_or_default();
            out.push_str(&format!(
                "    <bpmn:exclusiveGateway id=\"{eid}\"{na}{da}/>\n"
            ));
        }
        ElementKind::ParallelGateway => {
            out.push_str(&format!("    <bpmn:parallelGateway id=\"{eid}\"{na}/>\n"));
        }
        ElementKind::InclusiveGateway => {
            let da = default_flows
                .get(id)
                .map(|f| format!(" default=\"{}\"", xml_escape(f)))
                .unwrap_or_default();
            out.push_str(&format!(
                "    <bpmn:inclusiveGateway id=\"{eid}\"{na}{da}/>\n"
            ));
        }
        ElementKind::EventBasedGateway => {
            out.push_str(&format!("    <bpmn:eventBasedGateway id=\"{eid}\"{na}/>\n"));
        }
        ElementKind::ServiceTask {
            job_type,
            priority,
            custom_headers,
            linked_resources,
            agent_type,
        } => {
            let adhoc = def.adhoc.iter().find(|a| a.container_id == id);
            let tag = if adhoc.is_some() {
                "adHocSubProcess"
            } else {
                "serviceTask"
            };
            let cancel_attr = adhoc
                .map(|a| {
                    format!(
                        " cancelRemainingInstances=\"{}\"",
                        a.cancel_remaining_instances
                    )
                })
                .unwrap_or_default();
            out.push_str(&format!("    <bpmn:{tag} id=\"{eid}\"{na}{cancel_attr}>\n"));
            out.push_str("      <bpmn:extensionElements>\n");
            if let Some(adhoc) = adhoc {
                out.push_str("        <zeebe:adHoc");
                for (key, value) in [
                    (
                        "activeElementsCollection",
                        &adhoc.active_elements_collection,
                    ),
                    ("outputCollection", &adhoc.output_collection),
                    ("outputElement", &adhoc.output_element),
                ] {
                    if let Some(value) = value {
                        out.push_str(&format!(" {key}=\"{}\"", xml_escape(value)));
                    }
                }
                out.push_str("/>\n");
            }
            if let Some(agent_type) = agent_type {
                out.push_str(&format!(
                    "        <zeebe:agentDefinition agentType=\"{}\"/>\n",
                    xml_escape(agent_type.as_str())
                ));
            }
            if adhoc.is_none_or(|a| {
                a.impl_type == nanobpmn_engine_core::AdHocImplementationType::JobWorker
            }) {
                let retries_attr = el
                    .retries
                    .as_deref()
                    .map(|r| format!(" retries=\"{}\"", xml_escape(r)))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "        <zeebe:taskDefinition type=\"{}\"{retries_attr}/>\n",
                    xml_escape(job_type)
                ));
            }
            if let Some(p) = priority {
                out.push_str(&format!(
                    "        <zeebe:priorityDefinition priority=\"{}\"/>\n",
                    xml_escape(p)
                ));
            }
            if !custom_headers.is_empty() {
                out.push_str("        <zeebe:taskHeaders>\n");
                for (k, v) in custom_headers {
                    out.push_str(&format!(
                        "          <zeebe:header key=\"{}\" value=\"{}\"/>\n",
                        xml_escape(k),
                        xml_escape(v)
                    ));
                }
                out.push_str("        </zeebe:taskHeaders>\n");
            }
            if !linked_resources.is_empty() {
                out.push_str("        <zeebe:linkedResources>\n");
                for lr in linked_resources {
                    let binding = match lr.binding_type {
                        BindingType::Deployment => "deployment",
                        BindingType::Latest => "latest",
                        BindingType::VersionTag => "versionTag",
                    };
                    let version_tag_attr = lr
                        .version_tag
                        .as_deref()
                        .map(|t| format!(" versionTag=\"{}\"", xml_escape(t)))
                        .unwrap_or_default();
                    out.push_str(&format!(
                        "          <zeebe:linkedResource linkName=\"{}\" resourceId=\"{}\" \
resourceType=\"{}\" bindingType=\"{}\"{version_tag_attr}/>\n",
                        xml_escape(&lr.link_name),
                        xml_escape(&lr.resource_id),
                        xml_escape(&lr.resource_type),
                        binding,
                    ));
                }
                out.push_str("        </zeebe:linkedResources>\n");
            }
            emit_io_mapping(el, out);
            out.push_str("      </bpmn:extensionElements>\n");
            emit_multi_instance(el, out);
            if let Some(adhoc) = adhoc {
                for tool in &adhoc.tools {
                    emit_element(
                        def,
                        &tool.element_id,
                        errors,
                        messages,
                        signals,
                        escalations,
                        children_by_parent,
                        labels,
                        default_flows,
                        out,
                    );
                }
                if let Some(condition) = &adhoc.completion_condition {
                    out.push_str(&format!(
                        "      <bpmn:completionCondition>{}</bpmn:completionCondition>\n",
                        xml_escape(condition)
                    ));
                }
            }
            out.push_str(&format!("    </bpmn:{tag}>\n"));
        }
        ElementKind::BusinessRuleTask {
            decision_id,
            result_variable,
        } => {
            out.push_str(&format!("    <bpmn:businessRuleTask id=\"{eid}\"{na}>\n"));
            out.push_str("      <bpmn:extensionElements>\n");
            let result_attr = result_variable
                .as_deref()
                .map(|r| format!(" resultVariable=\"{}\"", xml_escape(r)))
                .unwrap_or_default();
            out.push_str(&format!(
                "        <zeebe:calledDecision decisionId=\"{}\"{result_attr}/>\n",
                xml_escape(decision_id)
            ));
            emit_io_mapping(el, out);
            out.push_str("      </bpmn:extensionElements>\n");
            emit_multi_instance(el, out);
            out.push_str("    </bpmn:businessRuleTask>\n");
        }
        ElementKind::UserTask(props) => {
            out.push_str(&format!("    <bpmn:userTask id=\"{eid}\"{na}>\n"));
            out.push_str("      <bpmn:extensionElements>\n");
            if props.assignee.is_some()
                || props.candidate_groups.is_some()
                || props.candidate_users.is_some()
            {
                out.push_str("        <zeebe:assignmentDefinition");
                if let Some(a) = &props.assignee {
                    out.push_str(&format!(" assignee=\"{}\"", xml_escape(a)));
                }
                if let Some(g) = &props.candidate_groups {
                    out.push_str(&format!(" candidateGroups=\"{}\"", xml_escape(g)));
                }
                if let Some(u) = &props.candidate_users {
                    out.push_str(&format!(" candidateUsers=\"{}\"", xml_escape(u)));
                }
                out.push_str("/>\n");
            }
            if props.due_date.is_some() || props.follow_up_date.is_some() {
                out.push_str("        <zeebe:taskSchedule");
                if let Some(d) = &props.due_date {
                    out.push_str(&format!(" dueDate=\"{}\"", xml_escape(d)));
                }
                if let Some(f) = &props.follow_up_date {
                    out.push_str(&format!(" followUpDate=\"{}\"", xml_escape(f)));
                }
                out.push_str("/>\n");
            }
            if let Some(p) = &props.priority {
                out.push_str(&format!(
                    "        <zeebe:priorityDefinition priority=\"{}\"/>\n",
                    xml_escape(p)
                ));
            }
            if props.form_id.is_some() || props.external_form_reference.is_some() {
                out.push_str("        <zeebe:formDefinition");
                // Zeebe treats `formId` and `externalReference` as mutually
                // exclusive; an external reference wins so a round-tripped model
                // never carries both.
                if let Some(r) = &props.external_form_reference {
                    out.push_str(&format!(" externalReference=\"{}\"", xml_escape(r)));
                } else if let Some(f) = &props.form_id {
                    out.push_str(&format!(" formId=\"{}\"", xml_escape(f)));
                }
                out.push_str("/>\n");
            }
            out.push_str("      </bpmn:extensionElements>\n");
            out.push_str("    </bpmn:userTask>\n");
        }
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => {
            let err_id = errors
                .get(error_code)
                .cloned()
                .unwrap_or_else(|| format!("Error_{}", id_fragment(error_code)));
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\"{na} attachedToRef=\"{}\">\n",
                xml_escape(attached_to)
            ));
            out.push_str(&format!(
                "      <bpmn:errorEventDefinition errorRef=\"{}\"/>\n",
                xml_escape(&err_id)
            ));
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => {
            let cancel = if *interrupting {
                ""
            } else {
                " cancelActivity=\"false\""
            };
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\"{na} attachedToRef=\"{}\"{cancel}>\n",
                xml_escape(attached_to)
            ));
            out.push_str("      <bpmn:timerEventDefinition>\n");
            if *repeating && !*interrupting {
                out.push_str(&format!(
                    "        <bpmn:timeCycle>R/{}</bpmn:timeCycle>\n",
                    millis_to_iso8601(*duration_millis)
                ));
            } else {
                out.push_str(&format!(
                    "        <bpmn:timeDuration>{}</bpmn:timeDuration>\n",
                    millis_to_iso8601(*duration_millis)
                ));
            }
            out.push_str("      </bpmn:timerEventDefinition>\n");
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => {
            let mref = messages
                .get(&(message_name.clone(), Some(correlation_key.clone())))
                .cloned()
                .unwrap_or_default();
            let cancel = if *interrupting {
                ""
            } else {
                " cancelActivity=\"false\""
            };
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\"{na} attachedToRef=\"{}\"{cancel}>\n",
                xml_escape(attached_to)
            ));
            out.push_str(&format!(
                "      <bpmn:messageEventDefinition messageRef=\"{}\"/>\n",
                xml_escape(&mref)
            ));
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::TimerIntermediateCatchEvent { duration_millis } => {
            out.push_str(&format!(
                "    <bpmn:intermediateCatchEvent id=\"{eid}\"{na}>\n"
            ));
            out.push_str("      <bpmn:timerEventDefinition>\n");
            out.push_str(&format!(
                "        <bpmn:timeDuration>{}</bpmn:timeDuration>\n",
                millis_to_iso8601(*duration_millis)
            ));
            out.push_str("      </bpmn:timerEventDefinition>\n");
            out.push_str("    </bpmn:intermediateCatchEvent>\n");
        }
        ElementKind::MessageIntermediateCatchEvent {
            message_name,
            correlation_key,
        } => {
            let mref = messages
                .get(&(message_name.clone(), Some(correlation_key.clone())))
                .cloned()
                .unwrap_or_default();
            out.push_str(&format!(
                "    <bpmn:intermediateCatchEvent id=\"{eid}\"{na}>\n"
            ));
            out.push_str(&format!(
                "      <bpmn:messageEventDefinition messageRef=\"{}\"/>\n",
                xml_escape(&mref)
            ));
            out.push_str("    </bpmn:intermediateCatchEvent>\n");
        }
        ElementKind::MessageStartEvent { message_name } => {
            let mref = messages
                .get(&(message_name.clone(), None))
                .cloned()
                .unwrap_or_default();
            out.push_str(&format!("    <bpmn:startEvent id=\"{eid}\"{na}>\n"));
            out.push_str(&format!(
                "      <bpmn:messageEventDefinition messageRef=\"{}\"/>\n",
                xml_escape(&mref)
            ));
            out.push_str("    </bpmn:startEvent>\n");
        }
        ElementKind::TimerStartEvent {
            interval_millis,
            repeating,
        } => {
            out.push_str(&format!("    <bpmn:startEvent id=\"{eid}\"{na}>\n"));
            out.push_str("      <bpmn:timerEventDefinition>\n");
            if *repeating {
                out.push_str(&format!(
                    "        <bpmn:timeCycle>R/{}</bpmn:timeCycle>\n",
                    millis_to_iso8601(*interval_millis)
                ));
            } else {
                out.push_str(&format!(
                    "        <bpmn:timeDuration>{}</bpmn:timeDuration>\n",
                    millis_to_iso8601(*interval_millis)
                ));
            }
            out.push_str("      </bpmn:timerEventDefinition>\n");
            out.push_str("    </bpmn:startEvent>\n");
        }
        ElementKind::SignalIntermediateCatchEvent { signal_name } => {
            let sref = signals.get(signal_name).cloned().unwrap_or_default();
            out.push_str(&format!(
                "    <bpmn:intermediateCatchEvent id=\"{eid}\"{na}>\n"
            ));
            out.push_str(&format!(
                "      <bpmn:signalEventDefinition signalRef=\"{}\"/>\n",
                xml_escape(&sref)
            ));
            out.push_str("    </bpmn:intermediateCatchEvent>\n");
        }
        ElementKind::SignalBoundaryEvent {
            attached_to,
            signal_name,
            interrupting,
        } => {
            let sref = signals.get(signal_name).cloned().unwrap_or_default();
            let cancel = if *interrupting {
                ""
            } else {
                " cancelActivity=\"false\""
            };
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\"{na} attachedToRef=\"{}\"{cancel}>\n",
                xml_escape(attached_to)
            ));
            out.push_str(&format!(
                "      <bpmn:signalEventDefinition signalRef=\"{}\"/>\n",
                xml_escape(&sref)
            ));
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::ConditionalIntermediateCatchEvent { condition } => {
            out.push_str(&format!(
                "    <bpmn:intermediateCatchEvent id=\"{eid}\"{na}>\n"
            ));
            out.push_str("      <bpmn:conditionalEventDefinition>\n");
            out.push_str(&format!(
                "        <bpmn:condition xsi:type=\"bpmn:tFormalExpression\">{}</bpmn:condition>\n",
                xml_escape(condition)
            ));
            out.push_str("      </bpmn:conditionalEventDefinition>\n");
            out.push_str("    </bpmn:intermediateCatchEvent>\n");
        }
        ElementKind::ConditionalBoundaryEvent {
            attached_to,
            condition,
            interrupting,
        } => {
            let cancel = if *interrupting {
                ""
            } else {
                " cancelActivity=\"false\""
            };
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\"{na} attachedToRef=\"{}\"{cancel}>\n",
                xml_escape(attached_to)
            ));
            out.push_str("      <bpmn:conditionalEventDefinition>\n");
            out.push_str(&format!(
                "        <bpmn:condition xsi:type=\"bpmn:tFormalExpression\">{}</bpmn:condition>\n",
                xml_escape(condition)
            ));
            out.push_str("      </bpmn:conditionalEventDefinition>\n");
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::EscalationThrowEvent { escalation_code } => {
            // An escalation throw parsed from an `endEvent` carries no outgoing
            // sequence flow; one parsed from an `intermediateThrowEvent` does.
            // Preserve that flavour on round-trip (mirrors the compensation
            // throw). An empty code emits no `escalationRef` (an unnamed throw).
            let tag = if el.outgoing.is_empty() {
                "endEvent"
            } else {
                "intermediateThrowEvent"
            };
            out.push_str(&format!("    <bpmn:{tag} id=\"{eid}\"{na}>\n"));
            // Round-trip any `zeebe:ioMapping` the model carries: the parser
            // attaches io mappings on BOTH the `intermediateThrowEvent` and the
            // `endEvent` form (same io_stack push as a plain/terminate end), so
            // omitting them here would silently drop inputs/outputs on re-parse.
            // Mirror the plain/terminate end branches — emit the block before the
            // event definition. A no-op when there are no mappings.
            if !el.io.inputs.is_empty() || !el.io.outputs.is_empty() {
                out.push_str("      <bpmn:extensionElements>\n");
                emit_io_mapping(el, out);
                out.push_str("      </bpmn:extensionElements>\n");
            }
            if escalation_code.is_empty() {
                out.push_str("      <bpmn:escalationEventDefinition/>\n");
            } else {
                let eref = escalations
                    .get(escalation_code)
                    .cloned()
                    .unwrap_or_default();
                out.push_str(&format!(
                    "      <bpmn:escalationEventDefinition escalationRef=\"{}\"/>\n",
                    xml_escape(&eref)
                ));
            }
            out.push_str(&format!("    </bpmn:{tag}>\n"));
        }
        ElementKind::EscalationBoundaryEvent {
            attached_to,
            escalation_code,
            interrupting,
        } => {
            let cancel = if *interrupting {
                ""
            } else {
                " cancelActivity=\"false\""
            };
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\"{na} attachedToRef=\"{}\"{cancel}>\n",
                xml_escape(attached_to)
            ));
            if escalation_code.is_empty() {
                out.push_str("      <bpmn:escalationEventDefinition/>\n");
            } else {
                let eref = escalations
                    .get(escalation_code)
                    .cloned()
                    .unwrap_or_default();
                out.push_str(&format!(
                    "      <bpmn:escalationEventDefinition escalationRef=\"{}\"/>\n",
                    xml_escape(&eref)
                ));
            }
            out.push_str("    </bpmn:boundaryEvent>\n");
        }
        ElementKind::CompensationThrowEvent => {
            // A compensation throw parsed from an `endEvent` carries no outgoing
            // sequence flow; one parsed from an `intermediateThrowEvent` does.
            // Preserve that flavour on round-trip by emitting the matching tag —
            // otherwise an end-event compensation throw would re-serialize as an
            // intermediate throw event.
            let tag = if el.outgoing.is_empty() {
                "endEvent"
            } else {
                "intermediateThrowEvent"
            };
            out.push_str(&format!("    <bpmn:{tag} id=\"{eid}\"{na}>\n"));
            out.push_str("      <bpmn:compensateEventDefinition/>\n");
            out.push_str(&format!("    </bpmn:{tag}>\n"));
        }
        ElementKind::CompensationBoundaryEvent {
            attached_to,
            handler,
        } => {
            out.push_str(&format!(
                "    <bpmn:boundaryEvent id=\"{eid}\"{na} attachedToRef=\"{}\">\n",
                xml_escape(attached_to)
            ));
            out.push_str("      <bpmn:compensateEventDefinition/>\n");
            out.push_str("    </bpmn:boundaryEvent>\n");
            // The compensation handler is wired to this boundary by an
            // `<association>`; emit it so the document round-trips back to the
            // same CompensationBoundaryEvent on re-parse. The `id` attribute is
            // deliberately omitted: BPMN ids share one global namespace, so a
            // synthesized `Association_{eid}` could collide with a user-defined
            // id and produce invalid XML — and the parser wires compensation
            // purely from `sourceRef`/`targetRef`, never the association id.
            out.push_str(&format!(
                "    <bpmn:association associationDirection=\"One\" sourceRef=\"{eid}\" targetRef=\"{}\"/>\n",
                xml_escape(handler)
            ));
        }
        ElementKind::SubProcess { .. } => {
            out.push_str(&format!("    <bpmn:subProcess id=\"{eid}\"{na}>\n"));
            if let Some(kids) = children_by_parent.get(id) {
                for child in kids {
                    // Children are emitted at the same indentation; the engine parser keys
                    // containment off the scope stack, not indentation, so this is faithful.
                    emit_element(
                        def,
                        child,
                        errors,
                        messages,
                        signals,
                        escalations,
                        children_by_parent,
                        labels,
                        default_flows,
                        out,
                    );
                }
            }
            out.push_str("    </bpmn:subProcess>\n");
        }
        ElementKind::AgentTask { .. } => unreachable!("legacy agent tasks are normalized"),
    }
}

/// A hand-authored diagram-interchange sidecar extracted from an original BPMN document, so a pure
/// attribute/condition/default edit can preserve the customer's layout byte-for-byte (ADR 0001
/// Phase 5). The executable core (nodes + flows) is a bijection; layout is the opaque complement —
/// kept keyed by element id, re-attached when the topology is unchanged and regenerated otherwise.
struct PreservedDi {
    /// The verbatim `<bpmndi:BPMNDiagram>…</bpmndi:BPMNDiagram>` block.
    diagram_block: String,
    /// Node ids that carry a `<bpmndi:BPMNShape>`.
    shape_ids: HashSet<String>,
    /// `(sourceRef, targetRef)` pairs that carry a `<bpmndi:BPMNEdge>` (via their sequence flow).
    edge_keys: HashSet<(String, String)>,
    /// `(sourceRef, targetRef) -> original sequence-flow id`, so the reused body can adopt the same
    /// ids the preserved edges already reference.
    flow_ids: HashMap<(String, String), String>,
    /// Every `id="…"` attribute declared INSIDE the verbatim diagram block — the
    /// `<bpmndi:BPMNDiagram>`/`<bpmndi:BPMNPlane>`/`<bpmndi:BPMNShape>`/`<bpmndi:BPMNEdge>`
    /// own ids (NOT their `bpmnElement` refs). The block is re-attached
    /// byte-for-byte, so a generated declaration id (`Escalation_…`, `Signal_…`)
    /// must be reserved against these too, or a legal DI id like
    /// `Escalation_OVERLOAD` could duplicate a `<bpmn:escalation id>` (#1173).
    di_ids: HashSet<String>,
}

impl PreservedDi {
    /// The original id of the flow between `src` and `to`, if the preserved diagram had one.
    fn flow_id(&self, src: &str, to: &str) -> Option<String> {
        self.flow_ids
            .get(&(src.to_string(), to.to_string()))
            .cloned()
    }

    /// True iff the preserved diagram covers *exactly* `def`'s node and flow topology, so it can be
    /// re-attached verbatim without dangling or missing DI. Any node/flow added or removed — or a
    /// duplicate parallel edge the `(src,to)` key can't disambiguate — fails the check and forces a
    /// clean auto-layout instead.
    fn covers(&self, def: &ProcessDefinition) -> bool {
        let def_nodes: HashSet<&str> = def.elements.keys().map(String::as_str).collect();
        if def_nodes.len() != self.shape_ids.len()
            || !self
                .shape_ids
                .iter()
                .all(|id| def_nodes.contains(id.as_str()))
        {
            return false;
        }
        let mut def_flows: Vec<(String, String)> = Vec::new();
        for (src, el) in &def.elements {
            for f in &el.outgoing {
                def_flows.push((src.clone(), f.to.clone()));
            }
        }
        let def_set: HashSet<(String, String)> = def_flows.iter().cloned().collect();
        if def_set.len() != def_flows.len() {
            return false; // duplicate parallel edge — can't key layout by (src,to)
        }
        def_set.len() == self.edge_keys.len() && def_set.iter().all(|k| self.edge_keys.contains(k))
    }
}

/// Extract a [`PreservedDi`] from an original BPMN document, or `None` if it has no diagram (or no
/// shapes). Edges are re-keyed by `(sourceRef, targetRef)` because the executable serializer
/// regenerates flow ids; shapes stay keyed by the stable node id.
fn extract_preserved_di(xml: &str) -> Option<PreservedDi> {
    let (ds, de) = element_span(xml, "BPMNDiagram", 0)?;
    let diagram_block = xml[ds..de].to_string();

    // Map every sequence flow's id to its endpoints (flows live in the process body, not the DI).
    let mut flow_endpoints: HashMap<String, (String, String)> = HashMap::new();
    let mut from = 0;
    while let Some((s, e)) = element_span(xml, "sequenceFlow", from) {
        let span = &xml[s..e];
        if let (Some(id), Some(src), Some(to)) = (
            attr_value_in(span, "id"),
            attr_value_in(span, "sourceRef"),
            attr_value_in(span, "targetRef"),
        ) {
            flow_endpoints.insert(id, (src, to));
        }
        from = e;
    }

    let mut shape_ids: HashSet<String> = HashSet::new();
    let mut from = 0;
    while let Some((s, e)) = element_span(&diagram_block, "BPMNShape", from) {
        if let Some(be) = attr_value_in(&diagram_block[s..e], "bpmnElement") {
            shape_ids.insert(be);
        }
        from = e;
    }

    let mut edge_keys: HashSet<(String, String)> = HashSet::new();
    let mut flow_ids: HashMap<(String, String), String> = HashMap::new();
    let mut from = 0;
    while let Some((s, e)) = element_span(&diagram_block, "BPMNEdge", from) {
        if let Some(be) = attr_value_in(&diagram_block[s..e], "bpmnElement") {
            if let Some((src, to)) = flow_endpoints.get(&be) {
                edge_keys.insert((src.clone(), to.clone()));
                flow_ids.insert((src.clone(), to.clone()), be);
            }
        }
        from = e;
    }

    if shape_ids.is_empty() {
        return None;
    }
    let di_ids: HashSet<String> = all_id_attrs_in(&diagram_block).into_iter().collect();
    Some(PreservedDi {
        diagram_block,
        shape_ids,
        edge_keys,
        flow_ids,
        di_ids,
    })
}

/// Serialize a [`ProcessDefinition`] back to BPMN XML that the engine's own parser round-trips
/// (start event, every node kind, sequence flows with conditions, and the definitions-level
/// `<bpmn:error>` / `<bpmn:message>` declarations boundary and message events reference).
///
/// Each node is emitted with a human `name=` and the document carries a generated, left-to-right
/// `<bpmndi:BPMNDiagram>` so the model renders and downloads legibly. `overrides` supplies
/// operator-set labels (`id -> name`) to preserve across an edit; any id not in `overrides` falls
/// back to a humanized form of the id. `parse_bpmn(definition_to_xml_labeled(def, …))` still
/// reproduces `def`'s structure (the engine ignores both `name=` and the DI).
pub fn definition_to_xml_labeled(
    def: &ProcessDefinition,
    overrides: &HashMap<String, String>,
) -> String {
    let emitted = serialize_definition(def, overrides, None, None);
    let extensions = extract_nano_extensions(&def.xml);
    preserve_nano_extensions_in(emitted, &extensions)
}

/// Like [`definition_to_xml_labeled`], but when `di_source` (the original XML the model was read
/// from) carries a hand-authored `<bpmndi:BPMNDiagram>` whose shapes and edges cover *exactly* the
/// current node + flow topology, that diagram is re-attached verbatim and the original sequence-flow
/// ids are reused — so a pure attribute/condition/default edit preserves the customer's hand layout
/// byte-for-byte. Any topology change (a node or flow added/removed) falls back to auto-layout,
/// identical to `definition_to_xml_labeled`. This is the DI-preservation lens of ADR 0001 Phase 5.
pub fn definition_to_xml_preserving_di(
    def: &ProcessDefinition,
    overrides: &HashMap<String, String>,
    di_source: Option<&str>,
) -> String {
    let di = di_source.and_then(extract_preserved_di);
    let emitted = serialize_definition(def, overrides, di.as_ref(), None);
    // Prefer the DI source for nano:* preservation when the caller supplied
    // one (that's the "the customer's authored file" input the DI lens is
    // designed around); fall back to the retained ProcessDefinition.xml.
    let source_for_ext = di_source.unwrap_or(&def.xml);
    let extensions = extract_nano_extensions(source_for_ext);
    preserve_nano_extensions_in(emitted, &extensions)
}

/// Auto-layout serializer that additionally accepts a `row_bias` map from the
/// [`layout`](crate::layout) module — the semantic solver's preferred y-row per node id, honoured
/// by [`desired_row`] to pull annotated nodes into their band. Any node not in the map falls back
/// to the predecessor-mean heuristic. Never re-attaches a preserved diagram (semantic layout is
/// always regenerated — the point of running it is to relay out to the new bands).
pub fn definition_to_xml_with_row_bias(
    def: &ProcessDefinition,
    overrides: &HashMap<String, String>,
    row_bias: &HashMap<String, f64>,
) -> String {
    let emitted = serialize_definition(def, overrides, None, Some(row_bias));
    let extensions = extract_nano_extensions(&def.xml);
    preserve_nano_extensions_in(emitted, &extensions)
}

/// Restore pruned catalog entries only in the emission copy, so XML and DI share the
/// same authored tool nodes. Retained sub-process tools already carry their executable body.
fn restore_adhoc_catalog_elements(def: &mut ProcessDefinition) {
    use nanobpmn_engine_core::AdHocToolKind;

    for adhoc in &def.adhoc {
        for tool in &adhoc.tools {
            if def.elements.contains_key(&tool.element_id) {
                continue;
            }
            let kind = match &tool.kind {
                AdHocToolKind::ServiceTask { job_type } => ElementKind::ServiceTask {
                    job_type: job_type.clone(),
                    priority: None,
                    agent_type: None,
                    custom_headers: BTreeMap::new(),
                    linked_resources: Vec::new(),
                },
                AdHocToolKind::UserTask(props) => ElementKind::UserTask(props.clone()),
                AdHocToolKind::CallActivity {
                    process_id,
                    propagate_all_parent_variables,
                    propagate_all_child_variables,
                } => ElementKind::CallActivity {
                    called_process_id: process_id.clone().unwrap_or_default(),
                    propagate_all_parent_variables: *propagate_all_parent_variables,
                    propagate_all_child_variables: *propagate_all_child_variables,
                },
                AdHocToolKind::SubProcess { start_event } => ElementKind::SubProcess {
                    start_event: start_event.clone(),
                },
                AdHocToolKind::Other => ElementKind::Task,
            };
            def.elements.insert(
                tool.element_id.clone(),
                Element {
                    id: tool.element_id.clone(),
                    kind,
                    name: Some(tool.name.clone()),
                    parent: Some(adhoc.container_id.clone()),
                    outgoing: Vec::new(),
                    io: tool.io.clone(),
                    timer: None,
                    retries: None,
                    multi_instance: None,
                    start_listeners: Vec::new(),
                    end_listeners: Vec::new(),
                    task_listeners: Vec::new(),
                },
            );
        }
    }
}

/// Resolve a stable id for every sequence flow once, in the canonical order the
/// process body and the diagram-interchange both use (sorted sources, outgoing in
/// declaration order), plus the per-source `default` flow map. When a preserved
/// diagram covers the topology, the original flow ids are reused so its
/// `<bpmndi:BPMNEdge>`s still reference live flows; otherwise a `Flow_{n}` id
/// (n starting at 1) is synthesized. This is the single source of truth for flow
/// ids: [`serialize_definition`] both reserves them against generated declaration
/// ids and emits them, so the two can never drift.
fn flow_id_resolution(
    def: &ProcessDefinition,
    preserve: Option<&PreservedDi>,
) -> (Vec<String>, HashMap<String, String>) {
    let mut flow_ids: Vec<String> = Vec::new();
    let mut default_flows: HashMap<String, String> = HashMap::new();
    let mut sources: Vec<&String> = def.elements.keys().collect();
    sources.sort();
    let mut k = 0usize;
    for src in sources {
        for flow in &def.elements[src].outgoing {
            k += 1;
            let id = preserve
                .and_then(|d| d.flow_id(src, &flow.to))
                .unwrap_or_else(|| format!("Flow_{k}"));
            if flow.is_default {
                default_flows.insert(src.clone(), id.clone());
            }
            flow_ids.push(id);
        }
    }
    (flow_ids, default_flows)
}

fn serialize_definition(
    def: &ProcessDefinition,
    overrides: &HashMap<String, String>,
    di: Option<&PreservedDi>,
    row_bias: Option<&HashMap<String, f64>>,
) -> String {
    let mut normalized = def.clone();
    normalized.normalize_legacy_agent_tasks();
    restore_adhoc_catalog_elements(&mut normalized);
    let def = &normalized;
    // Resolve the preserved hand-layout (if it still covers the topology) and the
    // canonical sequence-flow ids up front, so declaration-id allocation can
    // reserve *every* id this document will emit. A generated declaration id
    // (`Escalation_…`, `Signal_…`, …) must not collide with the `<bpmn:process>`
    // id or a preserved/synthesized `<bpmn:sequenceFlow>` id — e.g. a process or
    // preserved flow literally named `Escalation_OVERLOAD` would otherwise
    // duplicate a `<bpmn:escalation id>` and emit an ambiguous `escalationRef`
    // (#1173). `flow_id_resolution` is the single source of truth for flow ids,
    // reused verbatim when the flows are emitted below.
    let preserve = di.filter(|d| d.covers(def));
    let (flow_ids, default_flows) = flow_id_resolution(def, preserve);
    let mut reserved_ids: HashSet<String> = def.elements.keys().cloned().collect();
    reserved_ids.insert(def.id.clone());
    reserved_ids.extend(flow_ids.iter().cloned());
    // Reserve every id declared inside a preserved `<bpmndi:BPMNDiagram>` block:
    // it is re-attached verbatim by `definition_to_xml_preserving_di`, so a
    // generated declaration id (`Escalation_…`, `Signal_…`, …) that duplicated a
    // DI plane/shape/edge/diagram id would emit an invalid document with two
    // elements sharing one XML id (#1173).
    if let Some(d) = preserve {
        reserved_ids.extend(d.di_ids.iter().cloned());
    }
    let errors = collect_error_ids(def);
    let (messages, msg_lookup) = collect_messages(def);
    let signals = collect_signals(def);
    let escalations = collect_escalations(def, &reserved_ids);

    // Resolve a human label for every element: an operator-set name wins, else a readable label
    // derived from the id (so an authored node like `FraudScreen` shows as "Fraud Screen").
    let mut labels: HashMap<String, String> = HashMap::new();
    for id in def.elements.keys() {
        let label = overrides
            .get(id)
            .cloned()
            .or_else(|| {
                def.adhoc
                    .iter()
                    .flat_map(|a| &a.tools)
                    .find(|tool| &tool.element_id == id)
                    .map(|tool| tool.name.clone())
            })
            .unwrap_or_else(|| humanize_id(id));
        labels.insert(id.clone(), label);
    }

    // Group sub-process children by parent so a container emits its contents inline.
    let mut children_by_parent: HashMap<String, Vec<String>> = HashMap::new();
    for (id, el) in &def.elements {
        if let Some(parent) = &el.parent {
            children_by_parent
                .entry(parent.clone())
                .or_default()
                .push(id.clone());
        }
    }
    for kids in children_by_parent.values_mut() {
        kids.sort();
    }

    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(
        "<bpmn:definitions xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" \
         xmlns:zeebe=\"http://camunda.org/schema/zeebe/1.0\" \
         xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" \
         xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\" \
         xmlns:di=\"http://www.omg.org/spec/DD/20100524/DI\" \
         targetNamespace=\"http://bpmn.io/schema/bpmn\">\n",
    );
    for (code, eid) in &errors {
        out.push_str(&format!(
            "  <bpmn:error id=\"{}\" errorCode=\"{}\"/>\n",
            xml_escape(eid),
            xml_escape(code)
        ));
    }
    for m in &messages {
        if let Some(key) = &m.correlation_key {
            out.push_str(&format!(
                "  <bpmn:message id=\"{}\" name=\"{}\">\n",
                xml_escape(&m.id),
                xml_escape(&m.name)
            ));
            out.push_str("    <bpmn:extensionElements>\n");
            out.push_str(&format!(
                "      <zeebe:subscription correlationKey=\"={}\"/>\n",
                xml_escape(key)
            ));
            out.push_str("    </bpmn:extensionElements>\n");
            out.push_str("  </bpmn:message>\n");
        } else {
            out.push_str(&format!(
                "  <bpmn:message id=\"{}\" name=\"{}\"/>\n",
                xml_escape(&m.id),
                xml_escape(&m.name)
            ));
        }
    }
    for (name, sid) in &signals {
        out.push_str(&format!(
            "  <bpmn:signal id=\"{}\" name=\"{}\"/>\n",
            xml_escape(sid),
            xml_escape(name)
        ));
    }
    for (code, eid) in &escalations {
        out.push_str(&format!(
            "  <bpmn:escalation id=\"{}\" escalationCode=\"{}\"/>\n",
            xml_escape(eid),
            xml_escape(code)
        ));
    }
    out.push_str(&format!(
        "  <bpmn:process id=\"{}\" isExecutable=\"true\">\n",
        xml_escape(&def.id)
    ));
    // `preserve`, `flow_ids` and `default_flows` were resolved once at the top of
    // this function (so declaration-id allocation could reserve every emitted id);
    // reuse them here for the flow body/DI emission.

    // Emit top-level nodes (parent == None) in a stable order; sub-processes recurse.
    let mut top: Vec<&String> = def
        .elements
        .iter()
        .filter(|(_, e)| e.parent.is_none())
        .map(|(id, _)| id)
        .collect();
    top.sort();
    for id in top {
        emit_element(
            def,
            id,
            &errors,
            &msg_lookup,
            &signals,
            &escalations,
            &children_by_parent,
            &labels,
            &default_flows,
            &mut out,
        );
    }

    // Emit each sequence flow using the id assigned above, so the process body and the DI edges
    // reference the same ids. The engine parser builds flows purely from sourceRef/targetRef,
    // so flat emission (scope-independent) is faithful.
    let mut sources: Vec<&String> = def.elements.keys().collect();
    sources.sort();
    let mut flows: Vec<FlowEdge> = Vec::new();
    let mut idx = 0usize;
    for src in sources {
        for flow in &def.elements[src].outgoing {
            let fid = flow_ids[idx].clone();
            idx += 1;
            // Label a guarded branch with its condition so a person can read WHY each branch is
            // taken — bpmn.js renders a flow's `name`, not its conditionExpression, so without this
            // the diagram shows bare arrows and the guards look "lost".
            let label = flow.condition.as_ref().map(|c| flow_label(&c.expression));
            flows.push(FlowEdge {
                id: fid.clone(),
                src: src.clone(),
                to: flow.to.clone(),
                label: label.clone(),
            });
            let name_attr = label
                .as_deref()
                .map(|l| format!(" name=\"{}\"", xml_escape(l)))
                .unwrap_or_default();
            match &flow.condition {
                Some(cond) => {
                    out.push_str(&format!(
                        "    <bpmn:sequenceFlow id=\"{}\"{name_attr} sourceRef=\"{}\" targetRef=\"{}\">\n",
                        xml_escape(&fid),
                        xml_escape(src),
                        xml_escape(&flow.to)
                    ));
                    out.push_str(&format!(
                        "      <bpmn:conditionExpression>{}</bpmn:conditionExpression>\n",
                        xml_escape(&cond.expression)
                    ));
                    out.push_str("    </bpmn:sequenceFlow>\n");
                }
                None => {
                    out.push_str(&format!(
                        "    <bpmn:sequenceFlow id=\"{}\"{name_attr} sourceRef=\"{}\" targetRef=\"{}\"/>\n",
                        xml_escape(&fid),
                        xml_escape(src),
                        xml_escape(&flow.to)
                    ));
                }
            }
        }
    }

    out.push_str("  </bpmn:process>\n");
    match preserve {
        // Re-attach the customer's hand layout verbatim (shapes + edges keyed by the reused ids).
        Some(d) => {
            out.push_str(d.diagram_block.trim_end());
            out.push('\n');
        }
        None => append_diagram(def, &flows, &mut out, row_bias),
    }
    out.push_str("</bpmn:definitions>\n");
    out
}

/// A resolved sequence flow: a synthesized id plus its endpoints, shared between the process body
/// (`<bpmn:sequenceFlow>`) and the diagram-interchange (`<bpmndi:BPMNEdge>`) so both agree on ids.
/// `label` is the human-readable edge caption (a guarded branch's condition), rendered as a
/// `<bpmndi:BPMNLabel>` so the guard is visible on the diagram.
struct FlowEdge {
    id: String,
    src: String,
    to: String,
    label: Option<String>,
}

/// Turn a flow's FEEL guard into a short edge caption: drop a leading `=`, collapse whitespace,
/// and cap the length so a long expression doesn't dominate the diagram.
fn flow_label(expr: &str) -> String {
    let s = expr.trim().strip_prefix('=').unwrap_or(expr).trim();
    let collapsed = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() > 40 {
        let mut out: String = collapsed.chars().take(39).collect();
        out.push('…');
        out
    } else {
        collapsed
    }
}

/// A laid-out shape rectangle (top-left origin), in diagram coordinates.
#[derive(Clone, Copy)]
struct Rect {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}
impl Rect {
    fn cx(&self) -> f64 {
        self.x + self.w / 2.0
    }
    fn cy(&self) -> f64 {
        self.y + self.h / 2.0
    }
}

/// Diagram footprint (width, height) for a node kind: tasks/sub-processes are boxes, gateways are
/// diamonds, everything else (events) is a small circle.
fn node_dims(kind: &ElementKind) -> (f64, f64) {
    match kind {
        ElementKind::ServiceTask { .. }
        | ElementKind::UserTask(_)
        | ElementKind::Task
        | ElementKind::CallActivity { .. }
        | ElementKind::SubProcess { .. } => (110.0, 80.0),
        ElementKind::ExclusiveGateway
        | ElementKind::ParallelGateway
        | ElementKind::InclusiveGateway
        | ElementKind::EventBasedGateway => (50.0, 50.0),
        _ => (36.0, 36.0),
    }
}

/// Mean assigned row of a node's already-placed (main-flow) predecessors, or 0 when it has none —
/// the target row the node "wants" so a flow tends to run straight left-to-right.
///
/// When `bias.get(id)` is `Some`, the caller-supplied target row wins outright — this is the seam
/// the [`layout`](crate::layout) module uses to pull semantically-annotated nodes into their band
/// (primary→0, exception→+H, escalation→−H, …) instead of drifting toward their predecessors.
fn desired_row(
    id: &str,
    preds: &HashMap<String, Vec<String>>,
    row_of: &HashMap<String, f64>,
    bias: Option<&HashMap<String, f64>>,
) -> f64 {
    if let Some(b) = bias.and_then(|m| m.get(id).copied()) {
        return b;
    }
    let mut sum = 0.0;
    let mut cnt = 0.0;
    if let Some(ps) = preds.get(id) {
        for p in ps {
            if let Some(&r) = row_of.get(p) {
                sum += r;
                cnt += 1.0;
            }
        }
    }
    if cnt > 0.0 {
        sum / cnt
    } else {
        0.0
    }
}

/// True if the axis-aligned segment a→b passes through `r` (inflated by `pad`). Only horizontal and
/// vertical segments are produced by the router, so a diagonal is treated as a miss.
fn seg_hits_rect(a: (f64, f64), b: (f64, f64), r: &Rect, pad: f64) -> bool {
    let (rx0, ry0, rx1, ry1) = (r.x - pad, r.y - pad, r.x + r.w + pad, r.y + r.h + pad);
    if (a.1 - b.1).abs() < 0.5 {
        let y = a.1;
        if y <= ry0 || y >= ry1 {
            return false;
        }
        let (xa, xb) = if a.0 < b.0 { (a.0, b.0) } else { (b.0, a.0) };
        xa < rx1 && xb > rx0
    } else if (a.0 - b.0).abs() < 0.5 {
        let x = a.0;
        if x <= rx0 || x >= rx1 {
            return false;
        }
        let (ya, yb) = if a.1 < b.1 { (a.1, b.1) } else { (b.1, a.1) };
        ya < ry1 && yb > ry0
    } else {
        false
    }
}

/// Whether any segment of `path` clips any obstacle (inflated by `pad`).
fn path_hits(path: &[(f64, f64)], obstacles: &[Rect], pad: f64) -> bool {
    path.windows(2)
        .any(|w| obstacles.iter().any(|r| seg_hits_rect(w[0], w[1], r, pad)))
}

/// The point to anchor an edge label: the middle of the path's longest horizontal run (so the
/// caption sits on a clear stretch), falling back to the geometric midpoint.
fn edge_label_anchor(path: &[(f64, f64)]) -> (f64, f64) {
    let mut best: Option<((f64, f64), f64)> = None;
    for w in path.windows(2) {
        if (w[0].1 - w[1].1).abs() < 0.5 {
            let len = (w[0].0 - w[1].0).abs();
            let mid = ((w[0].0 + w[1].0) / 2.0, w[0].1);
            if best.as_ref().is_none_or(|(_, l)| len > *l) {
                best = Some((mid, len));
            }
        }
    }
    if let Some((p, _)) = best {
        return p;
    }
    let mid = path.len() / 2;
    path.get(mid).copied().unwrap_or((0.0, 0.0))
}

/// Orthogonal waypoints from `s` to `t` that try to avoid every node box in `obstacles`. Candidate
/// Manhattan routes are tried in order of visual preference (straight, then a single elbow tucked
/// against the source or target, then a detour through a clear lane below the diagram); the first
/// that clips nothing wins. A boundary-event edge always drops into the bottom lane first so it
/// can't cut back across its own host. `idx` staggers the bottom-lane detours so parallel edges
/// don't overlap. The last candidate is returned even if it still clips (better a drawn edge than
/// none).
fn route_avoiding(
    s: &Rect,
    t: &Rect,
    from_boundary: bool,
    obstacles: &[Rect],
    bottom_lane: f64,
    idx: usize,
) -> Vec<(f64, f64)> {
    const PAD: f64 = 6.0;
    const G: f64 = 18.0;
    let stagger = (idx % 5) as f64 * 14.0;

    if from_boundary {
        // Leave the host's bottom and reach the target without cutting back across the host or the
        // nodes stacked directly below it. The boundary sits in a thin clear band just under its
        // host, so escape horizontally along that band to a gutter beside the target, then run
        // vertically in the (clear) gutter into the target. Fall back to the lane beneath the whole
        // diagram if that band is blocked.
        let sx = s.cx();
        let sy = s.y + s.h;
        let tx = t.x;
        let ty = t.cy();
        let tb = t.y + t.h;
        let ly = bottom_lane + stagger;
        let candidates: Vec<Vec<(f64, f64)>> = vec![
            // Horizontal along the sub-host band to the target's left gutter, then vertical in.
            vec![(sx, sy), (tx - G, sy), (tx - G, ty), (tx, ty)],
            // Drop into the bottom lane, across, up into the target's left.
            vec![(sx, sy), (sx, ly), (tx - G, ly), (tx - G, ty), (tx, ty)],
            // Drop into the bottom lane, across, up into the target's bottom.
            vec![(sx, sy), (sx, ly), (t.cx(), ly), (t.cx(), tb)],
        ];
        for c in &candidates {
            if !path_hits(c, obstacles, PAD) {
                return c.clone();
            }
        }
        return candidates.into_iter().next_back().unwrap_or_default();
    }

    let sx = s.x + s.w;
    let sy = s.cy();
    let tx = t.x;
    let ty = t.cy();

    let mut candidates: Vec<Vec<(f64, f64)>> = Vec::new();

    // If the target is significantly above or below the source (more than the
    // source's own height, i.e. clearly on a different row band — as happens
    // when a semantic-layout exception drops below the happy path), prefer
    // leaving the source from the top / bottom face rather than the right.
    // This keeps a gateway's happy-path and error-path edges from stacking on
    // the same right-edge exit point.
    let dy = ty - sy;
    let vertical_dominant = dy.abs() > s.h;
    if vertical_dominant {
        let (sy_face, ty_side) = if dy > 0.0 {
            (s.y + s.h, ty) // leave bottom, run down to target row
        } else {
            (s.y, ty) // leave top, run up to target row
        };
        let sxc = s.cx();
        // Drop / rise straight into the target row, then across to the target's left gutter.
        candidates.push(vec![
            (sxc, sy_face),
            (sxc, ty_side),
            (tx - G, ty_side),
            (tx, ty),
        ]);
        // Same, but hit the target's top/bottom face directly (useful when the
        // target sits directly beneath / above the source column).
        let tface = if dy > 0.0 { t.y } else { t.y + t.h };
        if (t.cx() - sxc).abs() < G * 0.5 {
            candidates.push(vec![(sxc, sy_face), (t.cx(), tface)]);
        }
    }

    // 1) Straight shot when the two share a row.
    if (sy - ty).abs() < 0.5 {
        candidates.push(vec![(sx, sy), (tx, ty)]);
    }
    // 2) Elbow tucked just left of the target (vertical leg in the gutter before the target column).
    candidates.push(vec![(sx, sy), (tx - G, sy), (tx - G, ty), (tx, ty)]);
    // 3) Elbow just right of the source (vertical leg in the gutter after the source column).
    candidates.push(vec![(sx, sy), (sx + G, sy), (sx + G, ty), (tx, ty)]);
    // 4) Mid-gutter elbow.
    let mx = (sx + tx) / 2.0;
    candidates.push(vec![(sx, sy), (mx, sy), (mx, ty), (tx, ty)]);
    // 5) Detour through the clear lane below everything (last resort, but collision-free).
    let ly = bottom_lane + stagger;
    candidates.push(vec![
        (sx, sy),
        (sx + G, sy),
        (sx + G, ly),
        (tx - G, ly),
        (tx - G, ty),
        (tx, ty),
    ]);

    for c in &candidates {
        if !path_hits(c, obstacles, PAD) {
            return c.clone();
        }
    }
    candidates.pop().unwrap_or_default()
}

/// Append a generated, left-to-right `<bpmndi:BPMNDiagram>` for `def` so the model renders and
/// downloads with a real (horizontal) layout instead of relying on client-side auto-layout.
///
/// Layout is a simple layered ("Sugiyama-lite") pass: each node's column is its longest-path
/// distance from a source (a back-edge in a loop just caps the rank); within a column, rows are
/// assigned to follow predecessors and resolve collisions downward. Boundary events ride on their
/// host's bottom edge. It is not optimal, but it reads as a normal horizontal BPMN flow.
fn append_diagram(
    def: &ProcessDefinition,
    flows: &[FlowEdge],
    out: &mut String,
    bias: Option<&HashMap<String, f64>>,
) {
    let mut ids: Vec<String> = def.elements.keys().cloned().collect();
    ids.sort();
    let n = ids.len();

    // --- Column (rank) per node: longest path from any source, boundary pinned to its host. ---
    let mut rank: HashMap<String, usize> = ids.iter().map(|s| (s.clone(), 0usize)).collect();
    for _ in 0..(n + 2) {
        for id in &ids {
            if let Some(host) = def.elements[id].kind.attached_to() {
                if let Some(&hr) = rank.get(host) {
                    rank.insert(id.clone(), hr);
                }
            }
        }
        for f in flows {
            let sr = *rank.get(&f.src).unwrap_or(&0);
            let nr = (sr + 1).min(n);
            if rank.get(&f.to).is_none_or(|&r| r < nr) {
                rank.insert(f.to.clone(), nr);
            }
        }
    }

    // --- Row per (non-boundary) node, biased toward the mean of its main-flow predecessors. ---
    let mut preds: HashMap<String, Vec<String>> = HashMap::new();
    for f in flows {
        if !is_boundary(&def.elements[&f.src].kind) {
            preds.entry(f.to.clone()).or_default().push(f.src.clone());
        }
    }
    let grid_ids: Vec<&String> = ids
        .iter()
        .filter(|id| !is_boundary(&def.elements[*id].kind))
        .collect();
    let max_rank = grid_ids.iter().map(|id| rank[*id]).max().unwrap_or(0);
    let mut row_of: HashMap<String, f64> = HashMap::new();
    for r in 0..=max_rank {
        let mut here: Vec<String> = grid_ids
            .iter()
            .filter(|id| rank[**id] == r)
            .map(|id| (*id).clone())
            .collect();
        here.sort_by(|a, b| {
            let da = desired_row(a, &preds, &row_of, bias);
            let db = desired_row(b, &preds, &row_of, bias);
            da.partial_cmp(&db)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.cmp(b))
        });
        let mut used: Vec<i64> = Vec::new();
        for id in &here {
            let mut row = desired_row(id, &preds, &row_of, bias).round() as i64;
            if row < 0 {
                row = 0;
            }
            while used.contains(&row) {
                row += 1;
            }
            used.push(row);
            row_of.insert(id.clone(), row as f64);
        }
    }

    // --- Coordinates. ---
    const OX: f64 = 160.0;
    const OY: f64 = 100.0;
    const COL: f64 = 190.0;
    const ROW: f64 = 110.0;
    let mut rects: HashMap<String, Rect> = HashMap::new();
    for id in &ids {
        let kind = &def.elements[id].kind;
        if is_boundary(kind) {
            continue;
        }
        let (w, h) = node_dims(kind);
        let cx = OX + rank[id] as f64 * COL;
        let cy = OY + row_of.get(id).copied().unwrap_or(0.0) * ROW;
        rects.insert(
            id.clone(),
            Rect {
                x: cx - w / 2.0,
                y: cy - h / 2.0,
                w,
                h,
            },
        );
    }
    // Boundary events straddle the bottom edge of their host (3/4 along its width).
    for id in &ids {
        let kind = &def.elements[id].kind;
        if let Some(host) = kind.attached_to() {
            if let Some(hb) = rects.get(host).copied() {
                let (w, h) = node_dims(kind);
                let cx = hb.x + hb.w * 0.75;
                let cy = hb.y + hb.h;
                rects.insert(
                    id.clone(),
                    Rect {
                        x: cx - w / 2.0,
                        y: cy - h / 2.0,
                        w,
                        h,
                    },
                );
            }
        }
    }

    let fmt = |v: f64| (v.round() as i64).to_string();

    // Diagram extent, so obstacle-avoiding routes can escape into a clear lane below every node.
    let bottom_lane = rects.values().map(|r| r.y + r.h).fold(0.0_f64, f64::max) + 40.0;

    out.push_str("  <bpmndi:BPMNDiagram id=\"BPMNDiagram_1\">\n");
    out.push_str(&format!(
        "    <bpmndi:BPMNPlane id=\"BPMNPlane_1\" bpmnElement=\"{}\">\n",
        xml_escape(&def.id)
    ));
    for id in &ids {
        if let Some(b) = rects.get(id) {
            // A gateway only shows its distinguishing marker when the shape
            // opts in via `isMarkerVisible`; without it an exclusive (X) or
            // inclusive (O) gateway renders as an empty diamond
            // indistinguishable from a parallel gateway.
            let marker = if matches!(
                def.elements[id].kind,
                ElementKind::ExclusiveGateway | ElementKind::InclusiveGateway
            ) {
                " isMarkerVisible=\"true\""
            } else {
                ""
            };
            out.push_str(&format!(
                "      <bpmndi:BPMNShape id=\"{0}_di\" bpmnElement=\"{0}\"{5}>\n\
                 \x20       <dc:Bounds x=\"{1}\" y=\"{2}\" width=\"{3}\" height=\"{4}\"/>\n\
                 \x20     </bpmndi:BPMNShape>\n",
                xml_escape(id),
                fmt(b.x),
                fmt(b.y),
                fmt(b.w),
                fmt(b.h),
                marker
            ));
        }
    }
    for (idx, f) in flows.iter().enumerate() {
        let (Some(sb), Some(tb)) = (rects.get(&f.src), rects.get(&f.to)) else {
            continue;
        };
        let from_boundary = is_boundary(&def.elements[&f.src].kind);
        // Obstacles: every node box except this edge's own endpoints.
        let obstacles: Vec<Rect> = ids
            .iter()
            .filter(|id| id.as_str() != f.src && id.as_str() != f.to)
            .filter_map(|id| rects.get(id).copied())
            .collect();
        let path = route_avoiding(sb, tb, from_boundary, &obstacles, bottom_lane, idx);
        out.push_str(&format!(
            "      <bpmndi:BPMNEdge id=\"{0}_di\" bpmnElement=\"{0}\">\n",
            xml_escape(&f.id)
        ));
        for (x, y) in &path {
            out.push_str(&format!(
                "        <di:waypoint x=\"{}\" y=\"{}\"/>\n",
                fmt(*x),
                fmt(*y)
            ));
        }
        // Caption a guarded branch at the edge's midpoint so the condition is legible.
        if let Some(label) = &f.label {
            let (lx, ly) = edge_label_anchor(&path);
            let w = (label.chars().count() as f64 * 6.0 + 12.0).min(160.0);
            out.push_str(&format!(
                "        <bpmndi:BPMNLabel>\n\
                 \x20         <dc:Bounds x=\"{}\" y=\"{}\" width=\"{}\" height=\"14\"/>\n\
                 \x20       </bpmndi:BPMNLabel>\n",
                fmt(lx - w / 2.0),
                fmt(ly - 16.0),
                fmt(w)
            ));
        }
        out.push_str("      </bpmndi:BPMNEdge>\n");
    }
    out.push_str("    </bpmndi:BPMNPlane>\n  </bpmndi:BPMNDiagram>\n");
}

/// Build a sequence-flow condition from an optional expression string. An empty/whitespace
/// expression clears the condition (an unconditional flow).
fn condition_from(expr: Option<&str>) -> Option<Condition> {
    match expr.map(str::trim) {
        Some(e) if !e.is_empty() => Some(Condition::new(e.to_string())),
        _ => None,
    }
}

/// Require a string field on an edit op, or return a clear error naming the op and field.
fn req_str<'a>(op: &'a Value, field: &str, op_name: &str) -> Result<&'a str, String> {
    op.get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("op '{op_name}' requires a non-empty string '{field}'"))
}

/// Apply a single structured edit operation to `def` in place, returning a human-readable note
/// describing what changed. Each op fully owns the XML-level correctness of its change; an op that
/// references a missing node or would duplicate an id fails with an actionable message so the model
/// can correct the *operation* rather than re-typing raw XML.
fn apply_edit_op(
    def: &mut ProcessDefinition,
    names: &mut HashMap<String, String>,
    op: &Value,
) -> Result<String, String> {
    let op_name = op
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or("each edit op needs a string 'op' field")?;
    // Optional human label carried by an authoring op (insert ops, or the dedicated set_name op).
    let op_name_label = op.get("name").and_then(|v| v.as_str()).map(str::to_string);
    match op_name {
        "set_name" => {
            let node = req_str(op, "node", op_name)?;
            let label = req_str(op, "name", op_name)?;
            if !def.elements.contains_key(node) {
                return Err(format!("set_name: no node '{node}' in the model"));
            }
            names.insert(node.to_string(), label.to_string());
            Ok(format!("Set label of '{node}' to \"{label}\"."))
        }
        "set_task_job_type" => {
            let task = req_str(op, "task", op_name)?;
            let job_type = req_str(op, "jobType", op_name)?;
            let el = def
                .elements
                .get_mut(task)
                .ok_or_else(|| format!("set_task_job_type: no node '{task}' in the model"))?;
            match &mut el.kind {
                ElementKind::ServiceTask { job_type: jt, .. } => {
                    *jt = job_type.to_string();
                    Ok(format!("Set serviceTask '{task}' jobType to '{job_type}'."))
                }
                _ => Err(format!("set_task_job_type: '{task}' is not a serviceTask")),
            }
        }
        "set_flow_condition" => {
            let from = req_str(op, "from", op_name)?;
            let to = req_str(op, "to", op_name)?;
            let cond = condition_from(op.get("condition").and_then(|v| v.as_str()));
            let el = def
                .elements
                .get_mut(from)
                .ok_or_else(|| format!("set_flow_condition: no node '{from}' in the model"))?;
            let flow = el
                .outgoing
                .iter_mut()
                .find(|f| f.to == to)
                .ok_or_else(|| format!("set_flow_condition: no flow '{from}' -> '{to}'"))?;
            flow.condition = cond.clone();
            Ok(match cond {
                Some(c) => format!("Set condition on '{from}' -> '{to}' to `{}`.", c.expression),
                None => format!("Cleared the condition on '{from}' -> '{to}'."),
            })
        }
        "insert_service_task_after" => {
            let after = req_str(op, "after", op_name)?;
            let id = req_str(op, "id", op_name)?;
            let job_type = req_str(op, "jobType", op_name)?;
            if def.elements.contains_key(id) {
                return Err(format!(
                    "insert_service_task_after: node id '{id}' already exists"
                ));
            }
            let anchor = def
                .elements
                .get_mut(after)
                .ok_or_else(|| format!("insert_service_task_after: no node '{after}'"))?;
            // The new task inherits the anchor's outgoing flows; the anchor flows unconditionally
            // into it. So: after -> NEW -> (original targets, conditions preserved).
            let moved: Vec<SequenceFlow> = std::mem::take(&mut anchor.outgoing);
            anchor.outgoing.push(SequenceFlow {
                to: id.to_string(),
                condition: None,
                is_default: false,
            });
            let parent = anchor.parent.clone();
            def.elements.insert(
                id.to_string(),
                Element {
                    id: id.to_string(),
                    kind: ElementKind::ServiceTask {
                        agent_type: None,
                        job_type: job_type.to_string(),
                        priority: None,
                        custom_headers: std::collections::BTreeMap::new(),
                        linked_resources: Vec::new(),
                    },
                    name: None,
                    outgoing: moved,
                    parent,
                    io: Default::default(),
                    timer: None,
                    retries: None,
                    multi_instance: None,
                    start_listeners: Vec::new(),
                    end_listeners: Vec::new(),
                    task_listeners: Vec::new(),
                },
            );
            if let Some(label) = &op_name_label {
                names.insert(id.to_string(), label.clone());
            }
            Ok(format!(
                "Inserted serviceTask '{id}' (jobType '{job_type}') immediately after '{after}'."
            ))
        }
        "add_error_boundary" => {
            let task = req_str(op, "task", op_name)?;
            let error_code = req_str(op, "errorCode", op_name)?;
            let target = req_str(op, "target", op_name)?;
            let id = op
                .get("id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    format!("Boundary_{}_{}", id_fragment(task), id_fragment(error_code))
                });
            if def.elements.contains_key(&id) {
                return Err(format!("add_error_boundary: node id '{id}' already exists"));
            }
            match def.elements.get(task).map(|e| &e.kind) {
                Some(ElementKind::ServiceTask { .. }) => {}
                Some(_) => {
                    return Err(format!("add_error_boundary: '{task}' is not a serviceTask"))
                }
                None => return Err(format!("add_error_boundary: no node '{task}'")),
            }
            if !def.elements.contains_key(target) {
                return Err(format!(
                    "add_error_boundary: target node '{target}' does not exist"
                ));
            }
            def.elements.insert(
                id.clone(),
                Element {
                    id: id.clone(),
                    kind: ElementKind::ErrorBoundaryEvent {
                        attached_to: task.to_string(),
                        error_code: error_code.to_string(),
                    },
                    name: None,
                    outgoing: vec![SequenceFlow {
                        to: target.to_string(),
                        condition: None,
                        is_default: false,
                    }],
                    parent: None,
                    io: Default::default(),
                    timer: None,
                    retries: None,
                    multi_instance: None,
                    start_listeners: Vec::new(),
                    end_listeners: Vec::new(),
                    task_listeners: Vec::new(),
                },
            );
            if let Some(label) = &op_name_label {
                names.insert(id.clone(), label.clone());
            }
            Ok(format!(
                "Added an error boundary '{id}' (errorCode '{error_code}') on '{task}', routing to '{target}'."
            ))
        }
        "reroute_flow" => {
            let from = req_str(op, "from", op_name)?;
            let to = req_str(op, "to", op_name)?;
            let new_to = req_str(op, "newTo", op_name)?;
            if !def.elements.contains_key(new_to) {
                return Err(format!(
                    "reroute_flow: newTo node '{new_to}' does not exist"
                ));
            }
            let el = def
                .elements
                .get_mut(from)
                .ok_or_else(|| format!("reroute_flow: no node '{from}'"))?;
            let flow = el
                .outgoing
                .iter_mut()
                .find(|f| f.to == to)
                .ok_or_else(|| format!("reroute_flow: no flow '{from}' -> '{to}'"))?;
            flow.to = new_to.to_string();
            Ok(format!(
                "Rerouted flow '{from}' -> '{to}' to target '{new_to}'."
            ))
        }
        "remove_node" => {
            let id = req_str(op, "id", op_name)?;
            if id == def.start_event {
                return Err("remove_node: refusing to remove the start event".to_string());
            }
            let removed = def
                .elements
                .remove(id)
                .ok_or_else(|| format!("remove_node: no node '{id}'"))?;
            // Heal flows: every predecessor that pointed at the removed node now points at each of
            // the removed node's successors (carrying the predecessor's own condition).
            let successors: Vec<SequenceFlow> = removed.outgoing;
            for el in def.elements.values_mut() {
                let mut rewired: Vec<SequenceFlow> = Vec::new();
                for flow in std::mem::take(&mut el.outgoing) {
                    if flow.to == id {
                        for succ in &successors {
                            rewired.push(SequenceFlow {
                                to: succ.to.clone(),
                                condition: flow.condition.clone(),
                                is_default: flow.is_default,
                            });
                        }
                    } else {
                        rewired.push(flow);
                    }
                }
                el.outgoing = rewired;
            }
            // Drop any boundary events that were attached to the removed node (they would dangle).
            // Derived from the shared `attached_to` helper so every boundary kind
            // (error/timer/message/signal/conditional/compensation/escalation) is
            // covered — enumerating a subset here silently leaks an orphan boundary
            // of any omitted kind, emitting an invalid `attachedToRef` on the next serialize.
            let orphaned: Vec<String> = def
                .elements
                .iter()
                .filter(|(_, e)| e.kind.attached_to() == Some(id))
                .map(|(bid, _)| bid.clone())
                .collect();
            for bid in &orphaned {
                def.elements.remove(bid);
                names.remove(bid);
            }
            names.remove(id);
            Ok(format!(
                "Removed node '{id}', reconnecting its predecessors to its successors{}.",
                if orphaned.is_empty() {
                    String::new()
                } else {
                    format!(
                        " (also dropped {} attached boundary event(s))",
                        orphaned.len()
                    )
                }
            ))
        }
        "add_exclusive_gateway" => {
            let id = req_str(op, "id", op_name)?;
            let after = req_str(op, "after", op_name)?;
            if def.elements.contains_key(id) {
                return Err(format!(
                    "add_exclusive_gateway: node id '{id}' already exists"
                ));
            }
            let branches = op
                .get("branches")
                .and_then(|v| v.as_array())
                .ok_or("add_exclusive_gateway requires a 'branches' array")?;
            if branches.is_empty() {
                return Err(
                    "add_exclusive_gateway: 'branches' must list at least one target".into(),
                );
            }
            let mut outgoing: Vec<SequenceFlow> = Vec::new();
            for (i, b) in branches.iter().enumerate() {
                let to = b
                    .get("to")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        format!("add_exclusive_gateway: branch {i} needs a 'to' node")
                    })?;
                if !def.elements.contains_key(to) {
                    return Err(format!(
                        "add_exclusive_gateway: branch target '{to}' does not exist"
                    ));
                }
                outgoing.push(SequenceFlow {
                    to: to.to_string(),
                    condition: condition_from(b.get("condition").and_then(|v| v.as_str())),
                    is_default: false,
                });
            }
            // Splice the gateway onto the anchor's single outgoing edge: after -> G, and G fans out
            // to the requested branches. The anchor's original targets are preserved as the LAST,
            // unconditional (default) branch so no path is silently dropped.
            let anchor = def
                .elements
                .get_mut(after)
                .ok_or_else(|| format!("add_exclusive_gateway: no node '{after}'"))?;
            let original: Vec<SequenceFlow> = std::mem::take(&mut anchor.outgoing);
            anchor.outgoing.push(SequenceFlow {
                to: id.to_string(),
                condition: None,
                is_default: false,
            });
            let parent = anchor.parent.clone();
            for (i, f) in original.into_iter().enumerate() {
                // Carried as the default branch (conditions dropped: the gateway now decides). A
                // diverging exclusive gateway may name only one default flow, so the first preserved
                // path becomes the default; any others fall back to conditionless branches, which the
                // gateway validation will reject — the anchor is spliced on its single outgoing edge,
                // so in practice there is exactly one preserved path.
                outgoing.push(SequenceFlow {
                    to: f.to,
                    condition: None,
                    is_default: i == 0,
                });
            }
            def.elements.insert(
                id.to_string(),
                Element {
                    id: id.to_string(),
                    kind: ElementKind::ExclusiveGateway,
                    name: None,
                    outgoing,
                    parent,
                    io: Default::default(),
                    timer: None,
                    retries: None,
                    multi_instance: None,
                    start_listeners: Vec::new(),
                    end_listeners: Vec::new(),
                    task_listeners: Vec::new(),
                },
            );
            Ok(format!(
                "Inserted exclusiveGateway '{id}' after '{after}' with {} branch(es).",
                branches.len()
            ))
        }
        other => Err(format!(
            "unknown edit op '{other}'. Supported: set_task_job_type, set_flow_condition, \
             insert_service_task_after, add_error_boundary, reroute_flow, remove_node, \
             add_exclusive_gateway, set_name."
        )),
    }
}

/// `edit_model` — compose a candidate BPMN variant from a list of **validated structured
/// operations** applied to a base model, instead of one-shotting raw whole-document XML.
///
/// The base is auto-healed (`normalize_authoring`) and parsed with the engine's own parser; each
/// op mutates the parsed model and OUR serializer re-emits engine-parseable XML, which is re-parsed
/// to guarantee the result deploys. Returns the new XML, the per-op change notes, and the
/// post-edit [`analyze_model`] structural findings so the model sees the consequences immediately.
pub fn edit_model(base_xml: &str, ops: &[Value]) -> Result<Value, String> {
    edit_model_in(base_xml, ops, None)
}

/// Serialize one or more process definitions back into a single self-contained BPMN document.
/// A single definition round-trips exactly; a multi-stage model re-emits the orchestrator (index
/// 0, with a fresh diagram) and splices the remaining phase definitions back in, so the file stays
/// self-contained and the authored overview survives. Shared by `edit_model_in` and the IR
/// `write_model_ir` path so both assemble multi-phase models identically.
pub(crate) fn assemble_model(
    defs: &[ProcessDefinition],
    names: &HashMap<String, String>,
) -> String {
    if defs.len() == 1 {
        definition_to_xml_labeled(&defs[0], names)
    } else {
        let primary = definition_to_xml_labeled(&defs[0], names);
        let extras: Vec<String> = defs[1..]
            .iter()
            .map(|d| definition_to_xml_labeled(d, names))
            .collect();
        let extra_refs: Vec<&str> = extras.iter().map(String::as_str).collect();
        merge_definitions(&primary, &extra_refs)
    }
}

/// Like [`edit_model`], but `process` selects WHICH definition to edit in a
/// multi-stage model: the orchestrator (default / first) or a named called phase.
/// The other definitions and the orchestrator's authored diagram are preserved, so
/// editing inside a phase (where the bottleneck usually lives) doesn't flatten the
/// overview. Node ids are LOCAL to the selected process.
pub fn edit_model_in(
    base_xml: &str,
    ops: &[Value],
    process: Option<&str>,
) -> Result<Value, String> {
    if ops.is_empty() {
        return Err("edit_model needs at least one operation in 'ops'".to_string());
    }
    let (healed, heal_notes) = normalize_authoring(base_xml);
    let (wrapped, _) = ensure_definitions(&healed);
    // Preserve the operator-facing labels the base already carries (the engine model drops them),
    // so an edit doesn't strip every node's name; ops may add/override entries.
    let mut names = parse_element_names(&wrapped);
    let mut defs = parse_bpmn(&wrapped).map_err(|e| {
        let err = format!("{e:?}");
        match deploy_fix_hint(&err) {
            Some(h) => format!("base model failed to parse: {err}\nFix: {h}"),
            None => format!("base model failed to parse: {err}"),
        }
    })?;
    if defs.is_empty() {
        return Err("base model contained no process definitions".to_string());
    }

    let target_idx = match process {
        Some(pid) => defs.iter().position(|d| d.id == pid).ok_or_else(|| {
            format!(
                "no process '{pid}' to edit. Known process ids: {}. Omit `process` to edit the \
                 orchestrator; pass a callActivity's calledElement to edit a phase.",
                defs.iter()
                    .map(|d| d.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?,
        None => 0,
    };

    let mut applied: Vec<String> = heal_notes;
    {
        let def = &mut defs[target_idx];
        for (i, op) in ops.iter().enumerate() {
            let note = apply_edit_op(def, &mut names, op)
                .map_err(|e| format!("op {} failed: {e}", i + 1))?;
            applied.push(note);
        }
    }

    // Serialize. Single-definition models round-trip exactly as before. For a multi-stage model
    // we re-emit the orchestrator (with a fresh diagram) and splice the remaining phase
    // definitions back in, so the file stays self-contained and the overview survives.
    let xml = assemble_model(&defs, &names);

    // The serializer owns correctness, but re-parse defensively so we never hand back XML that the
    // engine would reject at deploy time — surfacing any logical inconsistency the ops introduced.
    if let Err(e) = parse_bpmn(&xml) {
        let err = format!("{e:?}");
        let hint = deploy_fix_hint(&err);
        return Err(format!(
            "the edited model does not parse ({err}){}. This usually means an operation left a \
             dangling reference (e.g. a flow to a removed node).",
            hint.map(|h| format!("\nFix: {h}")).unwrap_or_default()
        ));
    }
    // Findings are reported for the definition we actually edited (the orchestrator by default,
    // or the selected phase), not just the first process.
    let edited_xml = definition_to_xml_labeled(&defs[target_idx], &names);
    let analysis = analyze_model(&edited_xml).unwrap_or_else(|_| json!({}));

    Ok(json!({
        "ok": true,
        "editedProcess": defs[target_idx].id,
        "model": xml,
        "appliedOps": applied,
        "findings": analysis.get("findings").cloned().unwrap_or(json!([])),
        "metrics": analysis.get("metrics").cloned().unwrap_or(json!({})),
        "note": "This XML is engine-validated and ready to simulate. Pass it to simulate (start \
                 with limit:1) or compare_variants. Do NOT hand-edit it — apply further changes \
                 with another edit_model call.",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A small loan-approval-shaped model: start -> credit-check (service) -> XOR gateway
    // -> approve / reject -> end. The service task has an error boundary to a reject end.
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

    #[test]
    fn read_model_distils_nodes_kinds_and_join_keys() {
        let v = read_model(LOAN_BPMN).expect("read");
        assert_eq!(v["processId"], "loan-approval");
        assert_eq!(v["startEvent"], "Start");
        let nodes = v["nodes"].as_array().unwrap();
        // The credit-check service task surfaces its job type (the trace join key).
        let cc = nodes.iter().find(|n| n["id"] == "CreditCheck").unwrap();
        assert_eq!(cc["kind"], "serviceTask");
        assert_eq!(cc["jobType"], "credit-check");
        assert_eq!(cc["reachable"], true);
        // The gateway reports a split role.
        let gw = nodes.iter().find(|n| n["id"] == "Decision").unwrap();
        assert_eq!(gw["gatewayRole"], "split");
        // Two end events counted.
        assert_eq!(v["counts"]["endEvent"], 2);
    }

    #[test]
    fn analyze_model_flags_unguarded_service_tasks() {
        let v = analyze_model(LOAN_BPMN).expect("analyze");
        let findings = v["findings"].as_array().unwrap();
        // Default flow f3 exists, so NO gateway-no-default finding.
        assert!(!findings.iter().any(|f| f["code"] == "gateway-no-default"));
        // All three service tasks lack error/timer boundaries -> unguarded info.
        let unguarded: Vec<&str> = findings
            .iter()
            .filter(|f| f["code"] == "service-task-unguarded")
            .map(|f| f["element"].as_str().unwrap())
            .collect();
        assert!(unguarded.contains(&"CreditCheck"));
        assert!(unguarded.contains(&"Approve"));
        assert!(unguarded.contains(&"Reject"));
    }

    #[test]
    fn analyze_model_flags_unreachable_and_dead_end_nodes() {
        // An orphan service task with no incoming flow and no outgoing flow.
        let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="T">
      <bpmn:extensionElements><zeebe:taskDefinition type="work"/></bpmn:extensionElements>
      <bpmn:incoming>a</bpmn:incoming>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Orphan">
      <bpmn:extensionElements><zeebe:taskDefinition type="orphan"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="T"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let v = analyze_model(bpmn).expect("analyze");
        let findings = v["findings"].as_array().unwrap();
        assert!(findings
            .iter()
            .any(|f| f["code"] == "unreachable-node" && f["element"] == "Orphan"));
        // T has no outgoing flow -> dead-end; no end event -> warn.
        assert!(findings
            .iter()
            .any(|f| f["code"] == "dead-end" && f["element"] == "T"));
        assert!(findings.iter().any(|f| f["code"] == "no-end-event"));
    }

    #[test]
    fn analyze_model_flags_conditions_on_a_non_gateway_source() {
        // A service task (not a gateway) carrying conditional outgoing flows — the
        // Investigation-9 corruption where branch conditions are moved off the gateway.
        let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="Handler">
      <bpmn:extensionElements><zeebe:taskDefinition type="handler"/></bpmn:extensionElements>
      <bpmn:incoming>a</bpmn:incoming>
      <bpmn:outgoing>hi</bpmn:outgoing>
      <bpmn:outgoing>lo</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="EHi"><bpmn:incoming>hi</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="ELo"><bpmn:incoming>lo</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="Handler"/>
    <bpmn:sequenceFlow id="hi" sourceRef="Handler" targetRef="EHi">
      <bpmn:conditionExpression>= creditScore &gt;= 700</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="lo" sourceRef="Handler" targetRef="ELo">
      <bpmn:conditionExpression>= creditScore &lt; 700</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
  </bpmn:process>
</bpmn:definitions>"#;
        let v = analyze_model(bpmn).expect("analyze");
        let findings = v["findings"].as_array().unwrap();
        assert!(findings
            .iter()
            .any(|f| f["code"] == "condition-on-non-gateway"
                && f["element"] == "Handler"
                && f["severity"] == "warn"));
    }

    #[test]
    fn analyze_model_flags_gateways_with_no_default_flow() {
        // Both a condition-routed exclusive (XOR) and inclusive (OR) split with
        // every outgoing flow guarded and no default flow trip the gateway-neutral
        // `gateway-no-default` advisory: if no condition holds the token is stuck
        // (XOR) or the OR gateway raises a no-matching-flow incident.
        for (kw, gw_id) in [("exclusiveGateway", "X"), ("inclusiveGateway", "O")] {
            let bpmn = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
    <bpmn:{kw} id="{gw_id}">
      <bpmn:incoming>a</bpmn:incoming>
      <bpmn:outgoing>hi</bpmn:outgoing>
      <bpmn:outgoing>lo</bpmn:outgoing>
    </bpmn:{kw}>
    <bpmn:endEvent id="EHi"><bpmn:incoming>hi</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="ELo"><bpmn:incoming>lo</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="{gw_id}"/>
    <bpmn:sequenceFlow id="hi" sourceRef="{gw_id}" targetRef="EHi">
      <bpmn:conditionExpression>= x &gt;= 1</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="lo" sourceRef="{gw_id}" targetRef="ELo">
      <bpmn:conditionExpression>= x &lt; 1</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
  </bpmn:process>
</bpmn:definitions>"#
            );
            let v = analyze_model(&bpmn).expect("analyze");
            let findings = v["findings"].as_array().unwrap();
            assert!(
                findings.iter().any(|f| f["code"] == "gateway-no-default"
                    && f["element"] == gw_id
                    && f["severity"] == "warn"),
                "expected gateway-no-default for {kw} '{gw_id}', got: {v}"
            );
        }
    }

    #[test]
    fn analyze_model_flags_a_parallel_join_fed_by_a_conditional_split() {
        // A parallel (AND) join waits for ALL its incoming branches, but a
        // condition-routed split upstream — exclusive (XOR) OR inclusive (OR) —
        // may activate only some of them, so the join can wait forever. The
        // `parallel-join-may-deadlock` heuristic must fire for both split kinds
        // (regression: it originally recognised only the exclusive split).
        for kw in ["exclusiveGateway", "inclusiveGateway"] {
            let bpmn = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="d">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
    <bpmn:{kw} id="Split">
      <bpmn:incoming>a</bpmn:incoming>
      <bpmn:outgoing>hi</bpmn:outgoing>
      <bpmn:outgoing>lo</bpmn:outgoing>
    </bpmn:{kw}>
    <bpmn:task id="Ta"><bpmn:incoming>hi</bpmn:incoming><bpmn:outgoing>ja</bpmn:outgoing></bpmn:task>
    <bpmn:task id="Tb"><bpmn:incoming>lo</bpmn:incoming><bpmn:outgoing>jb</bpmn:outgoing></bpmn:task>
    <bpmn:parallelGateway id="Join">
      <bpmn:incoming>ja</bpmn:incoming>
      <bpmn:incoming>jb</bpmn:incoming>
      <bpmn:outgoing>done</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:endEvent id="E"><bpmn:incoming>done</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="Split"/>
    <bpmn:sequenceFlow id="hi" sourceRef="Split" targetRef="Ta">
      <bpmn:conditionExpression>= x &gt;= 1</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="lo" sourceRef="Split" targetRef="Tb">
      <bpmn:conditionExpression>= x &lt; 1</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="ja" sourceRef="Ta" targetRef="Join"/>
    <bpmn:sequenceFlow id="jb" sourceRef="Tb" targetRef="Join"/>
    <bpmn:sequenceFlow id="done" sourceRef="Join" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
            );
            let v = analyze_model(&bpmn).expect("analyze");
            let findings = v["findings"].as_array().unwrap();
            assert!(
                findings
                    .iter()
                    .any(|f| f["code"] == "parallel-join-may-deadlock" && f["element"] == "Join"),
                "expected parallel-join-may-deadlock for {kw} split feeding 'Join', got: {v}"
            );
        }
    }

    #[test]
    fn analyze_model_does_not_flag_conditions_on_an_exclusive_gateway() {
        // Conditions on an exclusive gateway's outgoing flows are correct — no warning.
        let v = analyze_model(LOAN_BPMN).expect("analyze");
        let findings = v["findings"].as_array().unwrap();
        assert!(!findings
            .iter()
            .any(|f| f["code"] == "condition-on-non-gateway"));
    }

    #[test]
    fn analyze_model_detects_a_rework_loop() {
        // start -> A -> gateway -> (retry back to A) | -> end
        let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="d">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="A">
      <bpmn:extensionElements><zeebe:taskDefinition type="work"/></bpmn:extensionElements>
      <bpmn:incoming>a</bpmn:incoming><bpmn:incoming>retry</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="G" default="done">
      <bpmn:incoming>b</bpmn:incoming><bpmn:outgoing>retry</bpmn:outgoing><bpmn:outgoing>done</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:endEvent id="E"><bpmn:incoming>done</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="A"/>
    <bpmn:sequenceFlow id="b" sourceRef="A" targetRef="G"/>
    <bpmn:sequenceFlow id="retry" sourceRef="G" targetRef="A">
      <bpmn:conditionExpression>= retry = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="done" sourceRef="G" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let v = analyze_model(bpmn).expect("analyze");
        assert_eq!(v["metrics"]["loops"], 1);
        let findings = v["findings"].as_array().unwrap();
        let loop_finding = findings
            .iter()
            .find(|f| f["code"] == "rework-loop")
            .unwrap();
        let msg = loop_finding["message"].as_str().unwrap();
        assert!(msg.contains("\"A\""));
        assert!(msg.contains("\"G\""));
    }

    #[test]
    fn rejects_unparseable_xml() {
        assert!(read_model("not bpmn").is_err());
        assert!(analyze_model("<bpmn/>").is_err());
    }

    // The exact authoring mistake from the loan-approval variant: an error boundary whose
    // errorRef points at no <bpmn:error> definition AND no <bpmn:definitions> root.
    const BARE_DANGLING_ERRORREF: &str = r#"<bpmn:process id="loan-approval" isExecutable="true" xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe"><bpmn:startEvent id="StartEvent_1"/><bpmn:sequenceFlow id="f1" sourceRef="StartEvent_1" targetRef="Task_CreditCheck"/><bpmn:serviceTask id="Task_CreditCheck" name="Credit Check"><bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements></bpmn:serviceTask><bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="EndEvent_Done"/><bpmn:endEvent id="EndEvent_Done"/><bpmn:boundaryEvent id="BoundaryEvent_CreditError" attachedToRef="Task_CreditCheck"><bpmn:errorEventDefinition errorRef="CREDIT_BUREAU_ERROR"/></bpmn:boundaryEvent><bpmn:sequenceFlow id="f3" sourceRef="BoundaryEvent_CreditError" targetRef="EndEvent_Err"/><bpmn:endEvent id="EndEvent_Err"/></bpmn:process>"#;

    #[test]
    fn validate_model_flags_a_dangling_error_ref_with_a_fix() {
        let v = validate_model(BARE_DANGLING_ERRORREF).expect("validate returns Ok");
        assert_eq!(v["valid"], false);
        let pe = v["parseError"].as_str().unwrap();
        assert!(pe.contains("unknown error"), "got: {pe}");
        // The fix names the real cause: errorRef must point at an <bpmn:error id=…>.
        let fix = v["fix"].as_str().unwrap();
        assert!(fix.contains("errorRef"));
        assert!(fix.contains("bpmn:error"));
        assert_eq!(v["findings"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn validate_model_wraps_a_bare_process_fragment() {
        // A bare <bpmn:process> with a VALID error definition is still missing a definitions root.
        let bare = r#"<bpmn:process id="p" isExecutable="true" xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"><bpmn:startEvent id="S"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent><bpmn:endEvent id="E"><bpmn:incoming>f0</bpmn:incoming></bpmn:endEvent><bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="E"/></bpmn:process>"#;
        let v = validate_model(bare).expect("validate");
        assert_eq!(v["valid"], true);
        assert_eq!(v["wrapped"], true);
        let findings = v["findings"].as_array().unwrap();
        assert!(findings
            .iter()
            .any(|f| f["code"] == "missing-definitions-root"));
    }

    #[test]
    fn validate_model_passes_a_clean_document() {
        let v = validate_model(LOAN_BPMN).expect("validate");
        assert_eq!(v["valid"], true);
        assert!(v.get("wrapped").is_none() || v["wrapped"] == false);
        assert_eq!(v["processId"], "loan-approval");
    }

    // The exact loop from Investigation 1: an error boundary authored as the NON-EXISTENT element
    // `<bpmn:errorBoundaryEvent>`, so the node is never created and the flow off it dangles.
    const ERROR_BOUNDARY_MISSPELLED: &str = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:error id="E_CREDIT" errorCode="CREDIT_BUREAU_ERROR"/>
  <bpmn:process id="loan-approval" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Task_CreditCheck"/>
    <bpmn:serviceTask id="Task_CreditCheck" name="Credit Check"><bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="EndDone"/>
    <bpmn:endEvent id="EndDone"/>
    <bpmn:errorBoundaryEvent id="BoundaryEvent_CreditError" attachedToRef="Task_CreditCheck"><bpmn:errorEventDefinition errorRef="E_CREDIT"/></bpmn:errorBoundaryEvent>
    <bpmn:sequenceFlow id="f3" sourceRef="BoundaryEvent_CreditError" targetRef="EndErr"/>
    <bpmn:endEvent id="EndErr"/>
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn normalize_authoring_renames_error_boundary_event() {
        // Before: errorBoundaryEvent makes the model unparseable (dangling flow source).
        assert!(analyze_model(ERROR_BOUNDARY_MISSPELLED).is_err());
        let (healed, notes) = normalize_authoring(ERROR_BOUNDARY_MISSPELLED);
        assert!(!healed.contains("errorBoundaryEvent"));
        assert!(healed.contains("boundaryEvent"));
        assert_eq!(notes.len(), 1);
        assert!(notes[0].contains("errorBoundaryEvent"));
        // After healing it parses cleanly.
        analyze_model(&healed).expect("healed model parses");
    }

    #[test]
    fn validate_model_auto_heals_a_misspelled_error_boundary() {
        let v = validate_model(ERROR_BOUNDARY_MISSPELLED).expect("validate returns Ok");
        // The model that thrashed Investigation 1 now passes, with a note on what was fixed.
        assert_eq!(v["valid"], true);
        assert!(v["autoFixed"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false));
        let findings = v["findings"].as_array().unwrap();
        assert!(findings.iter().any(|f| f["code"] == "auto-fixed"));
    }

    #[test]
    fn deploy_fix_hint_explains_an_unknown_source_element() {
        let err = "InvalidProcess { process_id: \"loan-approval\", reason: \"sequence flow \
                   BoundaryEvent_CreditError->EndEvent_CreditError has unknown source element \
                   BoundaryEvent_CreditError\" }";
        let hint = deploy_fix_hint(err).expect("hint");
        assert!(hint.contains("errorBoundaryEvent"));
        assert!(hint.contains("boundaryEvent"));
    }

    #[test]
    fn validate_model_flags_task_definition_used_as_an_attribute() {
        // The silent mistake: zeebe:taskDefinition as a serviceTask attribute (engine ignores it,
        // job type defaults to the task id) — parses fine, so it must be surfaced as a warning.
        let bad = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:serviceTask id="T" name="Pre Screen" zeebe:taskDefinition="pre-screen"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
    <bpmn:endEvent id="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let v = validate_model(bad).expect("validate");
        assert_eq!(v["valid"], true);
        let findings = v["findings"].as_array().unwrap();
        assert!(findings
            .iter()
            .any(|f| f["code"] == "task-definition-as-attribute"));
    }

    #[test]
    fn lint_task_definition_attribute_ignores_the_correct_element_form() {
        // The proper child-element form must NOT trip the lint.
        assert!(lint_task_definition_attribute(LOAN_BPMN).is_empty());
    }

    #[test]
    fn model_task_graph_collapses_gateways_onto_task_transitions() {
        let g = model_task_graph(LOAN_BPMN).expect("graph");
        assert_eq!(g.process_id, "loan-approval");
        // Only the three service tasks are observable (start/end/gateway collapse away).
        assert_eq!(g.tasks.len(), 3);
        assert!(g.tasks.contains("CreditCheck"));
        // CreditCheck -> Decision(XOR) -> {Approve, Reject}: both are permitted next tasks.
        let from_cc = &g.allowed["CreditCheck"];
        assert!(from_cc.contains("Approve"));
        assert!(from_cc.contains("Reject"));
        // Approve / Reject lead only to end events — no further task.
        assert!(g.allowed["Approve"].is_empty());
        // The model opens on CreditCheck and closes on Approve or Reject.
        assert!(g.start_tasks.contains("CreditCheck"));
        assert!(g.end_tasks.contains("Approve") && g.end_tasks.contains("Reject"));
        assert!(!g.end_tasks.contains("CreditCheck"));
    }

    // ── Structured authoring: serializer round-trip + edit_model operations ──────────────────

    /// Structural equivalence ignoring the verbatim `xml` field (which the serializer rewrites).
    fn assert_same_structure(a: &ProcessDefinition, b: &ProcessDefinition) {
        assert_eq!(a.id, b.id, "process id");
        assert_eq!(a.start_event, b.start_event, "start event");
        // The serializer synthesizes humanized `name=` diagram labels that are
        // not part of the structural model, so compare elements ignoring the
        // (display-only) `name` field.
        let strip = |els: &std::collections::HashMap<String, Element>| {
            let mut m = els.clone();
            for el in m.values_mut() {
                el.name = None;
            }
            m
        };
        assert_eq!(strip(&a.elements), strip(&b.elements), "elements");
    }

    /// Serialize with no operator label overrides (humanized fallbacks only).
    fn definition_to_xml(def: &ProcessDefinition) -> String {
        definition_to_xml_labeled(def, &HashMap::new())
    }

    #[test]
    fn external_agent_job_type_survives_xml_and_ir_round_trips() {
        let model = include_str!("../../engine-core/tests/fixtures/external-agent-job-type.bpmn");
        for declaration in [
            "<zeebe:taskDefinition type=\"senior:rebase\"/>",
            "<zeebe:taskDefinition type=\"= localRoute\"/>",
            "",
        ] {
            let expected_declaration = if declaration.is_empty() {
                "<zeebe:taskDefinition type=\"agent\"/>"
            } else {
                declaration
            };
            let model = model.replace(
                "<zeebe:taskDefinition type=\"senior:rebase\"/>",
                declaration,
            );
            let (def, _) = first_def(&model).unwrap();
            let xml = definition_to_xml(&def);
            assert!(xml.contains("<zeebe:taskDefinition"));
            assert!(xml.contains(expected_declaration), "{xml}");
            let ir = crate::model_ir::definition_to_ir(&def, &HashMap::new());
            assert!(ir.contains("jobType"), "{ir}");
            let restored = crate::model_ir::ir_to_definition(&ir).unwrap();
            let xml = definition_to_xml(&restored.definition);
            assert!(xml.contains(expected_declaration), "{xml}");
            assert_same_structure(&def, &parse_bpmn(&xml).unwrap()[0]);
        }
    }

    #[test]
    fn agent_metadata_xml_preserves_all_job_configuration() {
        use nanobpmn_engine_core::{AgentType, IoMapping, Mapping, MultiInstance};

        for marker in [AgentType::AiAgentTask, AgentType::External] {
            let mut def = nanobpmn_engine_core::ProcessBuilder::new("AgentConfig")
                .start_event("Start")
                .agent_task("Agent", "= workerType", marker)
                .end_event("End")
                .connect("Start", "Agent")
                .connect("Agent", "End")
                .build()
                .unwrap();
            let el = def.elements.get_mut("Agent").unwrap();
            el.retries = Some("= maxRetries".into());
            el.io = IoMapping {
                inputs: vec![Mapping {
                    source: "= item".into(),
                    target: "in".into(),
                }],
                outputs: vec![Mapping {
                    source: "= result".into(),
                    target: "out".into(),
                }],
            };
            el.multi_instance = Some(MultiInstance {
                input_collection: "= items".into(),
                input_element: Some("item".into()),
                output_collection: Some("results".into()),
                output_element: Some("= result".into()),
                sequential: true,
                completion_condition: None,
            });
            let ElementKind::ServiceTask {
                priority,
                custom_headers,
                linked_resources,
                ..
            } = &mut el.kind
            else {
                panic!("agent marker must be ordinary job metadata");
            };
            *priority = Some("= urgency".into());
            custom_headers.insert("model".into(), "a&b".into());
            linked_resources.push(nanobpmn_engine_core::LinkedResource {
                resource_id: "prompt.md".into(),
                binding_type: BindingType::VersionTag,
                resource_type: "GenericScript".into(),
                version_tag: Some("v3".into()),
                link_name: "agentPrompt".into(),
            });
            let xml = definition_to_xml(&def);
            assert!(xml.contains("<bpmndi:BPMNDiagram"));
            assert!(xml.contains(&format!("agentType=\"{}\"", marker.as_str())));
            assert!(xml.contains("<bpmn:serviceTask id=\"Agent\""));
            let mut parsed = parse_bpmn(&xml).unwrap();
            parsed[0].elements.get_mut("Agent").unwrap().name = None;
            assert_eq!(parsed[0].elements["Agent"], def.elements["Agent"]);
        }
    }

    fn authored_adhoc_catalog_xml() -> String {
        let source = include_str!("../../engine-core/tests/fixtures/external-agent-job-type.bpmn");
        let tools = r#"
      <bpmn:serviceTask id="Lookup" name="Look up account">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="lookup"/>
          <zeebe:ioMapping><zeebe:input source="= account" target="id"/><zeebe:output source="= result" target="accountResult"/></zeebe:ioMapping>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:userTask id="Review" name="Review account">
        <bpmn:extensionElements><zeebe:userTask/><zeebe:assignmentDefinition assignee="alice"/></bpmn:extensionElements>
      </bpmn:userTask>
      <bpmn:callActivity id="Escalate"><bpmn:extensionElements><zeebe:calledElement processId="escalation"/></bpmn:extensionElements></bpmn:callActivity>
      <bpmn:callActivity id="UnboundCall"/>
      <bpmn:task id="Note"/>
      <bpmn:subProcess id="Investigate" name="Investigate">
        <bpmn:startEvent id="ToolStart"/><bpmn:serviceTask id="ToolWork"><bpmn:extensionElements><zeebe:taskDefinition type="investigate"/></bpmn:extensionElements></bpmn:serviceTask><bpmn:endEvent id="ToolEnd"/>
        <bpmn:sequenceFlow id="tool_f1" sourceRef="ToolStart" targetRef="ToolWork"/><bpmn:sequenceFlow id="tool_f2" sourceRef="ToolWork" targetRef="ToolEnd"/>
      </bpmn:subProcess>
      <bpmn:completionCondition>= done</bpmn:completionCondition>
    </bpmn:adHocSubProcess>"#;
        let mut source = source
            .replace("<bpmn:serviceTask id=\"agent\">", "<bpmn:adHocSubProcess id=\"agent\" cancelRemainingInstances=\"false\">")
            .replace("</bpmn:serviceTask>", tools)
            .replace("<dc:Bounds x=\"200\" y=\"78\" width=\"100\" height=\"80\"/>", "<dc:Bounds x=\"200\" y=\"78\" width=\"900\" height=\"400\"/>")
            .replace("bpmnElement=\"agent\">", "bpmnElement=\"agent\" isExpanded=\"true\">")
            .replace("x=\"300\"", "x=\"1100\"")
            .replace("x=\"364\"", "x=\"1164\"")
            .replace("<zeebe:taskDefinition type=\"senior:rebase\"/>", "<zeebe:taskDefinition type=\"senior:rebase\" retries=\"7\"/><zeebe:adHoc outputCollection=\"results\" outputElement=\"= result\"/><zeebe:priorityDefinition priority=\"= urgency\"/><zeebe:taskHeaders><zeebe:header key=\"model\" value=\"gpt\"/></zeebe:taskHeaders><zeebe:linkedResources><zeebe:linkedResource linkName=\"prompt\" resourceId=\"prompt.md\" resourceType=\"GenericScript\" bindingType=\"latest\"/></zeebe:linkedResources>");
        let mut shapes = String::new();
        for (id, x, y, width, height) in [
            ("Lookup", 220, 140, 100, 80),
            ("Review", 350, 140, 100, 80),
            ("Escalate", 480, 140, 100, 80),
            ("UnboundCall", 610, 140, 100, 80),
            ("Note", 740, 140, 100, 80),
            ("Investigate", 220, 260, 500, 160),
            ("ToolStart", 250, 320, 36, 36),
            ("ToolWork", 350, 298, 100, 80),
            ("ToolEnd", 500, 320, 36, 36),
        ] {
            let expanded = if id == "Investigate" {
                " isExpanded=\"true\""
            } else {
                ""
            };
            shapes.push_str(&format!(
                "<bpmndi:BPMNShape id=\"{id}_di\" bpmnElement=\"{id}\"{expanded}><dc:Bounds x=\"{x}\" y=\"{y}\" width=\"{width}\" height=\"{height}\"/></bpmndi:BPMNShape>"
            ));
        }
        for (id, from, to) in [("tool_f1", 286, 350), ("tool_f2", 450, 500)] {
            shapes.push_str(&format!(
                "<bpmndi:BPMNEdge id=\"{id}_di\" bpmnElement=\"{id}\"><di:waypoint x=\"{from}\" y=\"338\"/><di:waypoint x=\"{to}\" y=\"338\"/></bpmndi:BPMNEdge>"
            ));
        }
        source = source.replace(
            "</bpmndi:BPMNPlane>",
            &format!("{shapes}</bpmndi:BPMNPlane>"),
        );
        source
    }

    #[test]
    fn agent_adhoc_xml_preserves_authored_catalog_and_di() {
        let source = authored_adhoc_catalog_xml();
        for marker in ["external", "aiAgentSubProcess"] {
            let source =
                source.replace("agentType=\"external\"", &format!("agentType=\"{marker}\""));
            let original = parse_bpmn(&source)
                .expect("valid authored adHoc model")
                .remove(0);
            assert_eq!(original.adhoc[0].tools.len(), 6);
            let emitted = definition_to_xml(&original);
            assert!(
                emitted.contains("<bpmn:adHocSubProcess id=\"agent\""),
                "{emitted}"
            );
            let restored = parse_bpmn(&emitted)
                .expect("emitted catalog is valid")
                .remove(0);
            assert_eq!(restored.adhoc, original.adhoc);
            assert_same_structure(&restored, &original);
            for tool in &original.adhoc[0].tools {
                assert!(emitted.contains(&format!("bpmnElement=\"{}\"", tool.element_id)));
            }
            assert!(emitted.contains("bpmnElement=\"ToolWork\""));
            let preserved =
                definition_to_xml_preserving_di(&original, &HashMap::new(), Some(&source));
            let authored_di = extract_preserved_di(&source).unwrap();
            assert!(preserved.contains(&authored_di.diagram_block));
            assert_eq!(parse_bpmn(&preserved).unwrap()[0].adhoc, original.adhoc);
        }
    }

    #[test]
    fn adhoc_catalog_call_activity_tool_round_trips_false_propagation_flags() {
        // Issue #1159 (Copilot review round 4): the PRUNED ad-hoc catalog
        // serializer (`AdHocToolKind::CallActivity`, not the normal
        // `ElementKind::CallActivity` element path) must preserve a tool's
        // `propagateAllParentVariables` / `propagateAllChildVariables` when set to
        // `false`. Both default to `true`, so only an explicit-false round trip
        // proves the pruned-element serializer cannot silently revert tool
        // propagation semantics — the authored-catalog fixture only exercises the
        // default-true `Escalate` and the unbound `UnboundCall`.
        let source = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:adHocSubProcess id="agent">
      <bpmn:extensionElements>
        <zeebe:agentDefinition agentType="external"/>
        <zeebe:taskDefinition type="agent-worker"/>
        <zeebe:adHoc outputCollection="results" outputElement="= result"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:callActivity id="CallSpecialist">
        <bpmn:extensionElements>
          <zeebe:calledElement processId="child" propagateAllParentVariables="false" propagateAllChildVariables="false"/>
        </bpmn:extensionElements>
      </bpmn:callActivity>
    </bpmn:adHocSubProcess>
    <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent"/>
    <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let original = parse_bpmn(source).expect("valid ad-hoc model").remove(0);
        // Sanity: the authored tool parsed with BOTH flags false (not the default).
        let tool = original.adhoc[0]
            .tools
            .iter()
            .find(|t| t.element_id == "CallSpecialist")
            .expect("CallSpecialist tool in the catalog");
        assert!(
            matches!(
                tool.kind,
                nanobpmn_engine_core::AdHocToolKind::CallActivity {
                    propagate_all_parent_variables: false,
                    propagate_all_child_variables: false,
                    ..
                }
            ),
            "authored tool parsed with both propagation flags false, got {:?}",
            tool.kind
        );

        let emitted = definition_to_xml(&original);
        assert!(
            emitted.contains("propagateAllParentVariables=\"false\""),
            "the pruned catalog serializer must emit the false parent flag:\n{emitted}"
        );
        assert!(
            emitted.contains("propagateAllChildVariables=\"false\""),
            "the pruned catalog serializer must emit the false child flag:\n{emitted}"
        );

        let restored = parse_bpmn(&emitted)
            .expect("emitted catalog is valid")
            .remove(0);
        assert_eq!(
            restored.adhoc, original.adhoc,
            "the ad-hoc catalog (including the tool's false propagation flags) \
             survives the round trip"
        );
    }

    #[test]
    fn declarative_adhoc_xml_does_not_gain_a_job_worker() {
        use nanobpmn_engine_core::AdHocImplementationType;

        let source = authored_adhoc_catalog_xml()
            .replace(
                "<zeebe:taskDefinition type=\"senior:rebase\" retries=\"7\"/>",
                "",
            )
            .replace("<zeebe:agentDefinition agentType=\"external\"/>", "")
            .replace(
                "<zeebe:adHoc ",
                "<zeebe:adHoc activeElementsCollection=\"=[&quot;Lookup&quot;]\" ",
            );
        let original = parse_bpmn(&source)
            .expect("valid declarative adHoc")
            .remove(0);
        assert_eq!(
            original.adhoc[0].impl_type,
            AdHocImplementationType::BpmnTask
        );
        for emitted in [
            definition_to_xml(&original),
            definition_to_xml_preserving_di(&original, &HashMap::new(), Some(&source)),
        ] {
            let container_extensions = emitted
                .split("<bpmn:adHocSubProcess")
                .nth(1)
                .unwrap()
                .split("</bpmn:extensionElements>")
                .next()
                .unwrap();
            assert!(!container_extensions.contains("<zeebe:taskDefinition"));
            let restored = parse_bpmn(&emitted)
                .expect("declarative XML remains valid")
                .remove(0);
            assert_eq!(restored.adhoc, original.adhoc);
            assert_same_structure(&restored, &original);
        }
    }

    #[test]
    fn agent_task_emission_round_trips_io_mapping_and_multi_instance() {
        // An `agentTask` emits as a `bpmn:serviceTask`; like every other activity
        // kind it must carry its `zeebe:ioMapping` and multi-instance loop
        // characteristics through emission, or they are silently dropped on a
        // round trip.
        const AGENT_MI_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="agent-mi" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Agent">
      <bpmn:extensionElements>
        <zeebe:agentDefinition agentType="aiAgentTask"/>
        <zeebe:taskDefinition type="agent-worker"/>
        <zeebe:ioMapping>
          <zeebe:input source="= item" target="in"/>
          <zeebe:output source="= result" target="out"/>
        </zeebe:ioMapping>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:multiInstanceLoopCharacteristics>
        <bpmn:extensionElements>
          <zeebe:loopCharacteristics inputCollection="= items" inputElement="item" outputCollection="results" outputElement="= result"/>
        </bpmn:extensionElements>
      </bpmn:multiInstanceLoopCharacteristics>
    </bpmn:serviceTask>
    <bpmn:endEvent id="End">
      <bpmn:incoming>f2</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Agent"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Agent" targetRef="End"/>
  </bpmn:process>
</bpmn:definitions>"#;

        let (orig, _) = first_def(AGENT_MI_BPMN).expect("parse agent multi-instance model");
        assert!(matches!(
            orig.elements["Agent"].kind,
            ElementKind::ServiceTask {
                agent_type: Some(_),
                ..
            }
        ));
        let xml = definition_to_xml(&orig);
        assert!(
            xml.contains("<zeebe:agentDefinition agentType=\"aiAgentTask\"/>"),
            "agent marker must survive, got:\n{xml}"
        );
        assert!(
            xml.contains("<zeebe:ioMapping>")
                && xml.contains("<zeebe:input source=\"= item\" target=\"in\"/>")
                && xml.contains("<zeebe:output source=\"= result\" target=\"out\"/>"),
            "ioMapping must survive agentTask emission, got:\n{xml}"
        );
        assert!(
            xml.contains("<bpmn:multiInstanceLoopCharacteristics>")
                && xml.contains("inputCollection=\"= items\""),
            "multi-instance loop characteristics must survive agentTask emission, got:\n{xml}"
        );
        // And the emitted model re-parses to the same structure.
        let reparsed = parse_bpmn(&xml).expect("emitted agent model re-parses");
        assert_same_structure(&orig, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_round_trips_link_events() {
        // A link throw/catch pair must survive serialize -> re-parse: the throw
        // emits an `<intermediateThrowEvent>` and the catch an
        // `<intermediateCatchEvent>`, each carrying a `<linkEventDefinition
        // name=…>`, and re-parse to the same link kinds (#1157).
        use nanobpmn_engine_core::ProcessBuilder;

        let orig = ProcessBuilder::new("links")
            .start_event("s")
            .link_intermediate_throw_event("throw", "hop")
            .link_intermediate_catch_event("catch", "hop")
            .service_task("after", "probe-after-link")
            .end_event("done")
            .connect("s", "throw")
            .connect("catch", "after")
            .connect("after", "done")
            .build()
            .unwrap();
        let xml = definition_to_xml(&orig);
        assert!(
            xml.contains("<bpmn:intermediateThrowEvent id=\"throw\"")
                && xml.contains("<bpmn:linkEventDefinition name=\"hop\"/>"),
            "the link throw must emit a linkEventDefinition, got:\n{xml}"
        );
        assert!(
            xml.contains("<bpmn:intermediateCatchEvent id=\"catch\""),
            "the link catch must emit an intermediateCatchEvent, got:\n{xml}"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized link model re-parses");
        assert_same_structure(&orig, &reparsed[0]);
        assert_eq!(
            reparsed[0].elements["throw"].kind,
            ElementKind::LinkIntermediateThrowEvent {
                link_name: "hop".to_string()
            }
        );
        assert_eq!(
            reparsed[0].elements["catch"].kind,
            ElementKind::LinkIntermediateCatchEvent {
                link_name: "hop".to_string()
            }
        );
    }

    #[test]
    fn definition_to_xml_round_trips_the_loan_model() {
        // serialize -> re-parse must reproduce the exact structure (nodes, kinds, job types,
        // gateway split with a guarded branch + a default, error declarations).
        let (orig, _) = first_def(LOAN_BPMN).expect("parse loan");
        let xml = definition_to_xml(&orig);
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&orig, &reparsed[0]);
        // The guarded branch's FEEL condition (with an escaped '>=') survives the round trip.
        let decision = &reparsed[0].elements["Decision"];
        assert!(decision
            .outgoing
            .iter()
            .any(|f| f.to == "Approve" && f.condition.is_some()));
    }

    #[test]
    fn definition_to_xml_round_trips_compensation_throw_flavour_by_outgoing_flow() {
        // A `CompensationThrowEvent` is parsed from either an
        // `<intermediateThrowEvent>` (has an outgoing flow) or an `<endEvent>`
        // (no outgoing flow). The serializer must preserve that flavour off the
        // presence of an outgoing flow — otherwise an end-event compensation
        // throw would re-serialize as an intermediate throw event and drift.
        use nanobpmn_engine_core::ProcessBuilder;

        // Intermediate flavour: the throw has an outgoing flow.
        let mid = ProcessBuilder::new("mid")
            .start_event("s")
            .compensation_throw_event("throw")
            .end_event("done")
            .connect("s", "throw")
            .connect("throw", "done")
            .build()
            .unwrap();
        let mid_xml = definition_to_xml(&mid);
        assert!(
            mid_xml.contains("<bpmn:intermediateThrowEvent id=\"throw\""),
            "a throw with an outgoing flow must emit intermediateThrowEvent, got:\n{mid_xml}"
        );
        assert_eq!(
            parse_bpmn(&mid_xml).unwrap()[0].elements["throw"].kind,
            ElementKind::CompensationThrowEvent
        );

        // End flavour: the throw is terminal (no outgoing flow).
        let end = ProcessBuilder::new("end")
            .start_event("s")
            .compensation_throw_event("throw")
            .connect("s", "throw")
            .build()
            .unwrap();
        let end_xml = definition_to_xml(&end);
        assert!(
            end_xml.contains("<bpmn:endEvent id=\"throw\"")
                && !end_xml.contains("<bpmn:intermediateThrowEvent id=\"throw\""),
            "a terminal compensation throw must emit endEvent, got:\n{end_xml}"
        );
        let reparsed = parse_bpmn(&end_xml).unwrap();
        assert_eq!(
            reparsed[0].elements["throw"].kind,
            ElementKind::CompensationThrowEvent,
            "an end-event compensation throw must round-trip back to CompensationThrowEvent"
        );
        assert!(
            reparsed[0].elements["throw"].outgoing.is_empty(),
            "the end-flavour throw must stay terminal on round-trip"
        );
    }

    #[test]
    fn definition_to_xml_round_trips_an_abstract_task_with_io_and_multi_instance() {
        // An abstract `<bpmn:task>` is a pass-through, but it still carries a
        // `zeebe:ioMapping` and `multiInstanceLoopCharacteristics` when modelled
        // (the parser pushes it onto the io_stack, so both attach to the
        // element). The serializer must emit an OPEN tag nesting those children —
        // a self-closing `<bpmn:task/>` would silently drop them on a round-trip.
        use nanobpmn_engine_core::{Mapping, MultiInstance};
        let mut def = nanobpmn_engine_core::ProcessBuilder::new("Passthrough")
            .start_event("Start")
            .task("Work")
            .end_event("Done")
            .connect("Start", "Work")
            .connect("Work", "Done")
            .build()
            .unwrap();
        {
            let work = def.elements.get_mut("Work").unwrap();
            work.io.inputs.push(Mapping {
                source: "= order.id".to_string(),
                target: "orderId".to_string(),
            });
            work.io.outputs.push(Mapping {
                source: "= result".to_string(),
                target: "outcome".to_string(),
            });
            work.multi_instance = Some(MultiInstance {
                input_collection: "= items".to_string(),
                input_element: Some("item".to_string()),
                ..Default::default()
            });
        }

        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<bpmn:task id=\"Work\"") && xml.contains("</bpmn:task>"),
            "abstract task with io/MI must emit an OPEN tag, got:\n{xml}"
        );
        assert!(
            xml.contains("<zeebe:ioMapping>"),
            "must emit ioMapping:\n{xml}"
        );
        assert!(
            xml.contains("multiInstanceLoopCharacteristics"),
            "must emit multi-instance:\n{xml}"
        );

        let reparsed = parse_bpmn(&xml).expect("serialized abstract task re-parses");
        assert_same_structure(&def, &reparsed[0]);
        let work = &reparsed[0].elements["Work"];
        assert_eq!(work.kind, ElementKind::Task);
        assert_eq!(work.io.inputs.len(), 1, "input mapping preserved");
        assert_eq!(work.io.outputs.len(), 1, "output mapping preserved");
        assert!(work.multi_instance.is_some(), "multi-instance preserved");
    }

    #[test]
    fn definition_to_xml_round_trips_a_call_activity_with_io_and_multi_instance() {
        // A `<bpmn:callActivity>` emits an OPEN tag for its `zeebe:calledElement`,
        // and the engine parser pushes it onto the io_stack — so an authored
        // `zeebe:ioMapping` and/or `multiInstanceLoopCharacteristics` attach to it
        // too. The serializer must round-trip those alongside the calledElement; a
        // callActivity emit that only wrote the calledElement would silently drop
        // mappings/MI when the emitted BPMN is parsed again.
        use nanobpmn_engine_core::{Mapping, MultiInstance};
        let mut def = nanobpmn_engine_core::ProcessBuilder::new("Orchestrator")
            .start_event("Start")
            .call_activity_with_propagation("Call", "Child", false, true)
            .end_event("Done")
            .connect("Start", "Call")
            .connect("Call", "Done")
            .build()
            .unwrap();
        {
            let call = def.elements.get_mut("Call").unwrap();
            call.io.inputs.push(Mapping {
                source: "= order.id".to_string(),
                target: "orderId".to_string(),
            });
            call.io.outputs.push(Mapping {
                source: "= result".to_string(),
                target: "outcome".to_string(),
            });
            call.multi_instance = Some(MultiInstance {
                input_collection: "= items".to_string(),
                input_element: Some("item".to_string()),
                ..Default::default()
            });
        }

        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<bpmn:callActivity id=\"Call\"") && xml.contains("</bpmn:callActivity>"),
            "call activity with io/MI must emit an OPEN tag, got:\n{xml}"
        );
        assert!(
            xml.contains("<zeebe:ioMapping>"),
            "must emit ioMapping:\n{xml}"
        );
        assert!(
            xml.contains("multiInstanceLoopCharacteristics"),
            "must emit multi-instance:\n{xml}"
        );

        let reparsed = parse_bpmn(&xml).expect("serialized call activity re-parses");
        assert_same_structure(&def, &reparsed[0]);
        let call = &reparsed[0].elements["Call"];
        assert!(
            matches!(
                call.kind,
                ElementKind::CallActivity {
                    propagate_all_parent_variables: false,
                    propagate_all_child_variables: true,
                    ..
                }
            ),
            "propagation flags preserved, got {:?}",
            call.kind
        );
        assert_eq!(call.io.inputs.len(), 1, "input mapping preserved");
        assert_eq!(call.io.outputs.len(), 1, "output mapping preserved");
        assert!(call.multi_instance.is_some(), "multi-instance preserved");
    }

    #[test]
    fn definition_to_xml_round_trips_a_terminate_end_event_with_io() {
        // A terminate end is an `<endEvent>` with a `<terminateEventDefinition>`;
        // the parser pushes it onto the io_stack, so a `zeebe:ioMapping` attaches
        // to it. The serializer must nest that mapping inside the open endEvent —
        // a bare `<endEvent><terminateEventDefinition/></endEvent>` would silently
        // drop it, changing the model on re-parse. (Nano, like Zeebe, does not
        // *apply* io on a terminate end at runtime, but must not mutate the model.)
        use nanobpmn_engine_core::Mapping;
        let mut def = nanobpmn_engine_core::ProcessBuilder::new("Halting")
            .start_event("Start")
            .terminate_end_event("Stop")
            .connect("Start", "Stop")
            .build()
            .unwrap();
        {
            let stop = def.elements.get_mut("Stop").unwrap();
            stop.io.outputs.push(Mapping {
                source: "= reason".to_string(),
                target: "haltReason".to_string(),
            });
        }

        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<zeebe:ioMapping>"),
            "terminate end with io must emit ioMapping:\n{xml}"
        );
        assert!(
            xml.contains("terminateEventDefinition"),
            "terminate definition must survive:\n{xml}"
        );

        let reparsed = parse_bpmn(&xml).expect("serialized terminate end re-parses");
        assert_same_structure(&def, &reparsed[0]);
        let stop = &reparsed[0].elements["Stop"];
        assert_eq!(stop.kind, ElementKind::TerminateEndEvent);
        assert_eq!(
            stop.io.outputs.len(),
            1,
            "terminate-end output mapping preserved on round-trip"
        );
    }

    #[test]
    fn definition_to_xml_round_trips_a_gateway_default_flow() {
        // The original motivating bug: a gateway `default` fallback was DROPPED by the serializer
        // (no `default="…"` attribute emitted), so an authored default flow silently degraded to a
        // plain unconditioned branch on deploy. The serializer must now emit `default="Flow_N"` and
        // the re-parse must re-flag exactly that branch as the default.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Routed")
            .start_event("Start")
            .exclusive_gateway("Gate")
            .service_task("Approve", "approve")
            .service_task("Review", "review")
            .end_event("DoneA")
            .end_event("DoneR")
            .connect("Start", "Gate")
            .connect_when("Gate", "Approve", "= score >= 700")
            .connect_default("Gate", "Review")
            .connect("Approve", "DoneA")
            .connect("Review", "DoneR")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<bpmn:exclusiveGateway id=\"Gate\"") && xml.contains(" default=\"Flow_"),
            "gateway must emit a default=\"Flow_N\" attribute, got:\n{xml}"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
        let gate = &reparsed[0].elements["Gate"];
        let default_branch = gate
            .outgoing
            .iter()
            .find(|f| f.is_default)
            .expect("a default branch survives the round trip");
        assert_eq!(
            default_branch.to, "Review",
            "the Review branch is the default"
        );
        assert!(
            gate.outgoing
                .iter()
                .find(|f| f.to == "Approve")
                .is_some_and(|f| !f.is_default && f.condition.is_some()),
            "the guarded branch stays conditioned and non-default"
        );
    }

    #[test]
    fn definition_to_xml_round_trips_task_retries_and_io_mappings() {
        // A service task carrying a `retries` expression plus `zeebe:ioMapping` input/output
        // variable mappings must round-trip: all three were previously dropped by the serializer,
        // which would silently change retry + variable-flow behavior (i.e. simulate() outcomes).
        let mut def = nanobpmn_engine_core::ProcessBuilder::new("Mapped")
            .start_event("Start")
            .service_task("Call", "io.camunda:http-json:1")
            .end_event("Done")
            .connect("Start", "Call")
            .connect("Call", "Done")
            .with_retries("Call", "=maxRetries")
            .build()
            .unwrap();
        {
            let el = def.elements.get_mut("Call").unwrap();
            el.io.inputs.push(nanobpmn_engine_core::Mapping {
                source: "=orderId".into(),
                target: "id".into(),
            });
            el.io.outputs.push(nanobpmn_engine_core::Mapping {
                source: "=response.body".into(),
                target: "result".into(),
            });
        }
        let xml = definition_to_xml(&def);
        assert!(xml.contains("retries=\"=maxRetries\""), "emits retries");
        assert!(xml.contains("<zeebe:ioMapping>"), "emits ioMapping");
        assert!(
            xml.contains("<zeebe:input source=\"=orderId\" target=\"id\"/>"),
            "emits the input mapping"
        );
        assert!(
            xml.contains("<zeebe:output source=\"=response.body\" target=\"result\"/>"),
            "emits the output mapping"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
        let call = &reparsed[0].elements["Call"];
        assert_eq!(call.retries.as_deref(), Some("=maxRetries"));
        assert_eq!(call.io.inputs.len(), 1);
        assert_eq!(call.io.outputs.len(), 1);
    }

    #[test]
    fn definition_to_xml_round_trips_linked_resources() {
        // A serviceTask carrying zeebe:linkedResources must round-trip through the XML
        // emitter losslessly — otherwise processos silently drops the declarative resource
        // links when it re-serializes a model that engine-core parsed. Guards the drift
        // surface flagged in review when ServiceTask gained a `linked_resources` field.
        let mut def = nanobpmn_engine_core::ProcessBuilder::new("Linked")
            .start_event("Start")
            .service_task("Call", "agent")
            .end_event("Done")
            .connect("Start", "Call")
            .connect("Call", "Done")
            .build()
            .unwrap();
        if let ElementKind::ServiceTask {
            linked_resources, ..
        } = &mut def.elements.get_mut("Call").unwrap().kind
        {
            linked_resources.push(nanobpmn_engine_core::LinkedResource {
                resource_id: "prompt.md".into(),
                binding_type: BindingType::VersionTag,
                resource_type: "GenericScript".into(),
                version_tag: Some("v3".into()),
                link_name: "agentPrompt".into(),
            });
        } else {
            panic!("Call is a service task");
        }
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<zeebe:linkedResources>"),
            "emits the linkedResources container, got:\n{xml}"
        );
        assert!(
            xml.contains(
                "<zeebe:linkedResource linkName=\"agentPrompt\" resourceId=\"prompt.md\" \
resourceType=\"GenericScript\" bindingType=\"versionTag\" versionTag=\"v3\"/>"
            ),
            "emits the linkedResource with all attributes, got:\n{xml}"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
        let ElementKind::ServiceTask {
            linked_resources, ..
        } = &reparsed[0].elements["Call"].kind
        else {
            panic!("Call re-parses as a service task");
        };
        assert_eq!(
            linked_resources.len(),
            1,
            "the link survives the round trip"
        );
        let lr = &linked_resources[0];
        assert_eq!(lr.resource_id, "prompt.md");
        assert_eq!(lr.resource_type, "GenericScript");
        assert_eq!(lr.link_name, "agentPrompt");
        assert_eq!(lr.version_tag.as_deref(), Some("v3"));
        assert!(matches!(lr.binding_type, BindingType::VersionTag));
    }

    #[test]
    fn definition_to_xml_round_trips_an_error_boundary() {
        // The healed Investigation-1 shape: a serviceTask with an error boundary routing to an
        // error end. Round-tripping must re-synthesize the <bpmn:error> declaration + errorRef.
        let healed = normalize_authoring(ERROR_BOUNDARY_MISSPELLED).0;
        let (orig, _) = first_def(&healed).expect("parse healed");
        let xml = definition_to_xml(&orig);
        assert!(xml.contains("<bpmn:error "), "emits an error declaration");
        let reparsed = parse_bpmn(&xml).expect("re-parses");
        assert_same_structure(&orig, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_round_trips_signal_events() {
        // A signal intermediate catch and an interrupting signal boundary on a task must
        // round-trip: the serializer synthesizes the <bpmn:signal> declaration + signalRef,
        // and the re-parse reproduces the exact structure.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Sig")
            .start_event("Start")
            .signal_intermediate_catch_event("Await", "all-clear")
            .service_task("Work", "do-work")
            .signal_boundary_event("Abort", "Work", "kill-switch")
            .end_event("Done")
            .end_event("Aborted")
            .connect("Start", "Await")
            .connect("Await", "Work")
            .connect("Work", "Done")
            .connect("Abort", "Aborted")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        assert!(xml.contains("<bpmn:signal "), "emits a signal declaration");
        assert!(
            xml.contains("signalEventDefinition"),
            "emits signalRef defs"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_round_trips_escalation_events() {
        // An escalation throw inside a sub-process and both an interrupting and a
        // non-interrupting escalation boundary must round-trip: the serializer
        // synthesizes the <bpmn:escalation> declaration + escalationRef, and the
        // re-parse reproduces the exact structure.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Esc")
            .start_event("Start")
            .sub_process("Sub", "SubStart")
            .start_event("SubStart")
            .contained_in("SubStart", "Sub")
            .escalation_throw_event("Throw", "OVERLOAD")
            .contained_in("Throw", "Sub")
            .end_event("SubEnd")
            .contained_in("SubEnd", "Sub")
            .escalation_boundary_event("Bnd", "Sub", "OVERLOAD")
            .non_interrupting_escalation_boundary_event("Watch", "Sub", "")
            .service_task("Handle", "handle")
            .end_event("Done")
            .end_event("Handled")
            .end_event("Watched")
            .connect("Start", "Sub")
            .connect("SubStart", "Throw")
            .connect("Throw", "SubEnd")
            .connect("Sub", "Done")
            .connect("Bnd", "Handle")
            .connect("Handle", "Handled")
            .connect("Watch", "Watched")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<bpmn:escalation "),
            "emits an escalation declaration"
        );
        assert!(
            xml.contains("escalationEventDefinition"),
            "emits escalationRef defs"
        );
        assert!(
            xml.contains("cancelActivity=\"false\""),
            "emits the non-interrupting boundary"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn collect_escalations_assigns_distinct_ids_to_colliding_codes() {
        // `id_fragment` is not injective: distinct escalation codes can normalize
        // to the same fragment (e.g. `A/B` and `A_B` both sanitize to `A_B`).
        // `collect_escalations` must still mint a UNIQUE declaration id per code,
        // or two `<bpmn:escalation>`s collide and one `escalationRef` dangles.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Collide")
            .start_event("Start")
            .sub_process("Sub", "SubStart")
            .start_event("SubStart")
            .contained_in("SubStart", "Sub")
            .escalation_throw_event("Throw1", "A/B")
            .contained_in("Throw1", "Sub")
            .escalation_throw_event("Throw2", "A_B")
            .contained_in("Throw2", "Sub")
            .end_event("SubEnd")
            .contained_in("SubEnd", "Sub")
            .end_event("Done")
            .connect("Start", "Sub")
            .connect("SubStart", "Throw1")
            .connect("Throw1", "Throw2")
            .connect("Throw2", "SubEnd")
            .connect("Sub", "Done")
            .build()
            .unwrap();
        let reserved: std::collections::HashSet<String> = def.elements.keys().cloned().collect();
        let escalations = collect_escalations(&def, &reserved);
        assert_eq!(escalations.len(), 2, "both codes get a declaration");
        let ids: std::collections::HashSet<&String> = escalations.values().collect();
        assert_eq!(
            ids.len(),
            2,
            "colliding codes must receive distinct escalation ids, got {:?}",
            escalations
        );
        // And every code still round-trips through serialize -> parse.
        let xml = definition_to_xml(&def);
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn collect_escalations_reserves_ids_emitted_outside_the_element_map() {
        // Regression (#1173): a generated `Escalation_…` declaration id must not
        // collide with an id the document emits that is NOT a model element id —
        // the `<bpmn:process>` id or a preserved `<bpmn:sequenceFlow>` id. Here a
        // reserved id (e.g. a preserved flow literally named `Escalation_OVERLOAD`)
        // is passed alongside the element ids; the mint must skip it.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Collide")
            .start_event("Start")
            .sub_process("Sub", "SubStart")
            .start_event("SubStart")
            .contained_in("SubStart", "Sub")
            .escalation_throw_event("Throw", "OVERLOAD")
            .contained_in("Throw", "Sub")
            .end_event("SubEnd")
            .contained_in("SubEnd", "Sub")
            .end_event("Done")
            .connect("Start", "Sub")
            .connect("SubStart", "Throw")
            .connect("Throw", "SubEnd")
            .connect("Sub", "Done")
            .build()
            .unwrap();
        let mut reserved: std::collections::HashSet<String> =
            def.elements.keys().cloned().collect();
        // A preserved sequence-flow id (or the process id) that happens to equal
        // the escalation base id.
        reserved.insert("Escalation_OVERLOAD".to_string());
        let escalations = collect_escalations(&def, &reserved);
        assert_eq!(
            escalations.get("OVERLOAD").map(String::as_str),
            Some("Escalation_OVERLOAD_2"),
            "the mint must skip the reserved id, got {escalations:?}"
        );
    }

    #[test]
    fn definition_to_xml_avoids_an_escalation_id_colliding_with_the_process_id() {
        // End-to-end (#1173): a process literally named `Escalation_OVERLOAD` and
        // an escalation code `OVERLOAD` (which fragments to the same base id) must
        // not emit two elements with id `Escalation_OVERLOAD`. The escalation
        // declaration is bumped to a free suffix and the document still re-parses.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Escalation_OVERLOAD")
            .start_event("Start")
            .sub_process("Sub", "SubStart")
            .start_event("SubStart")
            .contained_in("SubStart", "Sub")
            .escalation_throw_event("Throw", "OVERLOAD")
            .contained_in("Throw", "Sub")
            .end_event("SubEnd")
            .contained_in("SubEnd", "Sub")
            .end_event("Done")
            .connect("Start", "Sub")
            .connect("SubStart", "Throw")
            .connect("Throw", "SubEnd")
            .connect("Sub", "Done")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<bpmn:process id=\"Escalation_OVERLOAD\""),
            "the process keeps its id"
        );
        assert!(
            xml.contains("<bpmn:escalation id=\"Escalation_OVERLOAD_2\""),
            "the escalation declaration avoids the process id, got:\n{xml}"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_preserving_di_avoids_an_escalation_id_colliding_with_a_di_id() {
        // Regression (#1173): a preserved `<bpmndi:BPMNDiagram>` is re-attached
        // byte-for-byte by `definition_to_xml_preserving_di`, so its own
        // plane/shape/edge/diagram ids must be reserved against a generated
        // `<bpmn:escalation>` declaration id. A hand-authored file whose DI plane
        // is literally named `Escalation_OVERLOAD` (a legal DI id) would otherwise
        // duplicate the declaration minted for code `OVERLOAD`, emitting two
        // elements that share one XML id.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Esc")
            .start_event("Start")
            .sub_process("Sub", "SubStart")
            .start_event("SubStart")
            .contained_in("SubStart", "Sub")
            .escalation_throw_event("Throw", "OVERLOAD")
            .contained_in("Throw", "Sub")
            .end_event("SubEnd")
            .contained_in("SubEnd", "Sub")
            .end_event("Done")
            .connect("Start", "Sub")
            .connect("SubStart", "Throw")
            .connect("Throw", "SubEnd")
            .connect("Sub", "Done")
            .build()
            .unwrap();
        // Auto-serialize once to obtain a DI sidecar that covers the topology,
        // then rename its plane id to the value the escalation declaration wants.
        let di_source =
            definition_to_xml(&def).replace("id=\"BPMNPlane_1\"", "id=\"Escalation_OVERLOAD\"");
        assert!(
            di_source.contains("id=\"Escalation_OVERLOAD\""),
            "the DI source carries the colliding plane id"
        );
        let xml = definition_to_xml_preserving_di(&def, &HashMap::new(), Some(&di_source));
        assert!(
            xml.contains("id=\"Escalation_OVERLOAD\""),
            "the preserved DI plane id survives verbatim, got:\n{xml}"
        );
        assert!(
            xml.contains("<bpmn:escalation id=\"Escalation_OVERLOAD_2\""),
            "the escalation declaration must avoid the preserved DI id, got:\n{xml}"
        );
        assert_eq!(
            xml.matches("\"Escalation_OVERLOAD\"").count(),
            1,
            "exactly one element (the DI plane) may carry id=\"Escalation_OVERLOAD\", got:\n{xml}"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_round_trips_an_escalation_throw_with_io() {
        // Regression (#1173): the escalation throw/end serializer must nest a
        // `zeebe:ioMapping` the model carries. The parser attaches io on both the
        // `intermediateThrowEvent` and `endEvent` forms (same io_stack push as a
        // plain/terminate end), so a bare
        // `<intermediateThrowEvent><escalationEventDefinition/></…>` would silently
        // drop the mappings, mutating the model on re-parse.
        use nanobpmn_engine_core::Mapping;
        let mut def = nanobpmn_engine_core::ProcessBuilder::new("EscIo")
            .start_event("Start")
            .sub_process("Sub", "SubStart")
            .start_event("SubStart")
            .contained_in("SubStart", "Sub")
            .escalation_throw_event("Throw", "OVERLOAD")
            .contained_in("Throw", "Sub")
            .end_event("SubEnd")
            .contained_in("SubEnd", "Sub")
            .end_event("Done")
            .connect("Start", "Sub")
            .connect("SubStart", "Throw")
            .connect("Throw", "SubEnd")
            .connect("Sub", "Done")
            .build()
            .unwrap();
        {
            let throw = def.elements.get_mut("Throw").unwrap();
            throw.io.inputs.push(Mapping {
                source: "= payload".to_string(),
                target: "escPayload".to_string(),
            });
            throw.io.outputs.push(Mapping {
                source: "= code".to_string(),
                target: "escCode".to_string(),
            });
        }

        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<zeebe:ioMapping>"),
            "an escalation throw with io must emit ioMapping:\n{xml}"
        );
        assert!(
            xml.contains("intermediateThrowEvent") && xml.contains("escalationEventDefinition"),
            "the throw flavour and escalation definition must survive:\n{xml}"
        );

        let reparsed = parse_bpmn(&xml).expect("serialized escalation throw re-parses");
        assert_same_structure(&def, &reparsed[0]);
        let throw = &reparsed[0].elements["Throw"];
        assert_eq!(
            throw.io.inputs.len(),
            1,
            "escalation-throw input mapping preserved on round-trip"
        );
        assert_eq!(
            throw.io.outputs.len(),
            1,
            "escalation-throw output mapping preserved on round-trip"
        );
    }

    #[test]
    fn definition_to_xml_preserving_di_reserves_a_di_id_in_an_alternate_spelling() {
        // Regression (#1173, suppressed advisory): the DI-id reservation scanned
        // for the literal `id="…"`, missing legal XML spellings like `id = "…"`
        // and `id='…'`. A preserved DI element named `Escalation_OVERLOAD` in such
        // a form was omitted from `reserved_ids`, so the escalation declaration
        // minted for code `OVERLOAD` collided with it (two elements sharing one
        // XML id). The scanner is now quote/whitespace tolerant.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Esc")
            .start_event("Start")
            .sub_process("Sub", "SubStart")
            .start_event("SubStart")
            .contained_in("SubStart", "Sub")
            .escalation_throw_event("Throw", "OVERLOAD")
            .contained_in("Throw", "Sub")
            .end_event("SubEnd")
            .contained_in("SubEnd", "Sub")
            .end_event("Done")
            .connect("Start", "Sub")
            .connect("SubStart", "Throw")
            .connect("Throw", "SubEnd")
            .connect("Sub", "Done")
            .build()
            .unwrap();
        // Rename the plane id using an alternate-but-legal spelling (`id = '…'`).
        let di_source =
            definition_to_xml(&def).replace("id=\"BPMNPlane_1\"", "id = 'Escalation_OVERLOAD'");
        assert!(
            di_source.contains("id = 'Escalation_OVERLOAD'"),
            "the DI source carries the alternate-spelling colliding plane id"
        );
        let xml = definition_to_xml_preserving_di(&def, &HashMap::new(), Some(&di_source));
        assert!(
            xml.contains("id = 'Escalation_OVERLOAD'"),
            "the preserved DI plane id survives verbatim, got:\n{xml}"
        );
        assert!(
            xml.contains("<bpmn:escalation id=\"Escalation_OVERLOAD_2\""),
            "the escalation declaration must avoid the alternate-spelling DI id, got:\n{xml}"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn analyze_model_treats_an_escalation_end_event_as_terminal() {
        // Regression (#1173, suppressed advisory): a task → escalation *end* event
        // (an `EscalationThrowEvent` with no outgoing flow) is a legitimate
        // terminal. The end-event and dead-end checks only recognised
        // `EndEvent`/`TerminateEndEvent`, so such a model drew a false
        // `no-end-event` warning and a false `dead-end` on the escalation end.
        let def = nanobpmn_engine_core::ProcessBuilder::new("EscTerminal")
            .start_event("Start")
            .service_task("Work", "worker")
            .escalation_throw_event("EscEnd", "OVERLOAD")
            .connect("Start", "Work")
            .connect("Work", "EscEnd")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        let v = analyze_model(&xml).expect("analyze");
        let findings = v["findings"].as_array().unwrap();
        assert!(
            !findings.iter().any(|f| f["code"] == "no-end-event"),
            "an escalation end event is a terminal, so no `no-end-event` warning is due:\n{findings:#?}"
        );
        assert!(
            !findings
                .iter()
                .any(|f| f["code"] == "dead-end" && f["element"] == "EscEnd"),
            "an escalation end event must not be flagged as a dead end:\n{findings:#?}"
        );
        // The observable-task projection must also see it as a closing task.
        let graph = model_task_graph(&xml).expect("task graph");
        assert!(
            graph.end_tasks.contains("Work"),
            "the task reaching the escalation end event is a legal closing task, got {:?}",
            graph.end_tasks
        );
    }

    #[test]
    fn definition_to_xml_round_trips_an_inline_script_task() {
        // An inline-FEEL script task must round-trip: the serializer emits a
        // <bpmn:scriptTask> with a <zeebe:script expression=.. resultVariable=..>,
        // and the re-parse reproduces the exact ScriptTask structure.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Scripted")
            .start_event("Start")
            .script_task("Calc", "=a + b", "sum")
            .end_event("Done")
            .connect("Start", "Calc")
            .connect("Calc", "Done")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        assert!(xml.contains("<bpmn:scriptTask "), "emits a script task");
        assert!(xml.contains("<zeebe:script "), "emits the zeebe:script");
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_round_trips_conditional_events() {
        // A conditional intermediate catch and a (non-interrupting) conditional
        // boundary must round-trip: the serializer emits
        // <bpmn:conditionalEventDefinition><bpmn:condition>=..</> and the re-parse
        // reproduces the exact conditional structure (condition text + attach + kind).
        let def = nanobpmn_engine_core::ProcessBuilder::new("Conditioned")
            .start_event("Start")
            .conditional_intermediate_catch_event("Gate", "=approved = true")
            .service_task("Work", "do-work")
            .non_interrupting_conditional_boundary_event("Ping", "Work", "=ping = true")
            .end_event("Done")
            .end_event("Pinged")
            .connect("Start", "Gate")
            .connect("Gate", "Work")
            .connect("Work", "Done")
            .connect("Ping", "Pinged")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<bpmn:conditionalEventDefinition"),
            "emits a conditional event definition"
        );
        assert!(
            xml.contains("<bpmn:condition"),
            "emits the nested condition element"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_round_trips_a_multi_instance_activity() {
        // A multi-instance service task must round-trip: the serializer emits a
        // <bpmn:multiInstanceLoopCharacteristics> with a nested
        // <zeebe:loopCharacteristics> and <bpmn:completionCondition>, and the
        // re-parse reproduces the exact MultiInstance structure.
        let def = nanobpmn_engine_core::ProcessBuilder::new("Batched")
            .start_event("Start")
            .service_task("Each", "handle")
            .with_multi_instance(
                "Each",
                nanobpmn_engine_core::MultiInstance {
                    input_collection: "=items".to_string(),
                    input_element: Some("item".to_string()),
                    output_collection: Some("results".to_string()),
                    output_element: Some("=item * 2".to_string()),
                    completion_condition: Some("=count(results) >= 2".to_string()),
                    sequential: true,
                },
            )
            .end_event("Done")
            .connect("Start", "Each")
            .connect("Each", "Done")
            .build()
            .unwrap();
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("<bpmn:multiInstanceLoopCharacteristics"),
            "emits the multi-instance characteristics"
        );
        assert!(
            xml.contains("<zeebe:loopCharacteristics "),
            "emits the zeebe:loopCharacteristics extension"
        );
        assert!(
            xml.contains("<bpmn:completionCondition>"),
            "emits the completion condition"
        );
        let reparsed = parse_bpmn(&xml).expect("serialized model re-parses");
        assert_same_structure(&def, &reparsed[0]);
        // assert_same_structure ignores the MI field; verify it explicitly.
        let mi = reparsed[0]
            .element("Each")
            .unwrap()
            .multi_instance
            .as_ref()
            .expect("multi-instance survives the round-trip");
        assert_eq!(mi.input_collection, "=items");
        assert_eq!(mi.input_element.as_deref(), Some("item"));
        assert_eq!(mi.output_collection.as_deref(), Some("results"));
        assert_eq!(mi.output_element.as_deref(), Some("=item * 2"));
        assert_eq!(
            mi.completion_condition.as_deref(),
            Some("=count(results) >= 2")
        );
        assert!(mi.sequential);
    }

    #[test]
    fn job_type_binding_hints_flags_task_id_as_job_type() {
        // A serviceTask with NO parseable taskDefinition child: the engine defaults its
        // job type to the element id. When simulate reports that id as uncovered, the
        // hint must fire and name the task.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task_CreditCheck" />
    <bpmn:serviceTask id="Task_CreditCheck" name="Credit Check">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;
        let hints = job_type_binding_hints(xml, &["Task_CreditCheck".to_string()]);
        assert_eq!(hints.len(), 1, "exactly one binding hint expected");
        assert_eq!(hints[0]["code"], "job-type-defaulted-to-task-id");
        assert_eq!(hints[0]["element"], "Task_CreditCheck");
    }

    #[test]
    fn job_type_binding_hints_silent_when_job_type_is_bound() {
        // A correctly-bound serviceTask whose job type differs from its id must not
        // trip the hint, even if its (real) job type is uncovered for other reasons.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Defs">
  <bpmn:process id="P" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task_CreditCheck" />
    <bpmn:serviceTask id="Task_CreditCheck" name="Credit Check">
      <bpmn:extensionElements><zeebe:taskDefinition type="credit-check" /></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task_CreditCheck" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;
        let hints = job_type_binding_hints(xml, &["credit-check".to_string()]);
        assert!(hints.is_empty(), "bound job type must not trip the hint");
    }

    #[test]
    fn edit_model_sets_a_task_job_type() {
        let ops =
            vec![json!({"op":"set_task_job_type","task":"CreditCheck","jobType":"bureau-pull"})];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        assert_eq!(v["ok"], true);
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        match &def.elements["CreditCheck"].kind {
            ElementKind::ServiceTask { job_type, .. } => assert_eq!(job_type, "bureau-pull"),
            _ => panic!("CreditCheck should still be a serviceTask"),
        }
    }

    #[test]
    fn edit_model_inserts_a_service_task_after() {
        let ops = vec![json!({
            "op":"insert_service_task_after","after":"CreditCheck","id":"FraudCheck","jobType":"fraud-check"
        })];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        // CreditCheck now flows into the new task, which flows on to the original target (Decision).
        let cc = &def.elements["CreditCheck"];
        assert!(cc.outgoing.iter().any(|f| f.to == "FraudCheck"));
        let fc = &def.elements["FraudCheck"];
        assert!(fc.outgoing.iter().any(|f| f.to == "Decision"));
    }

    #[test]
    fn edit_model_adds_an_error_boundary_that_parses() {
        // Investigation 1's intent, expressed as a validated op instead of hand-written XML.
        let base = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" targetNamespace="t">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Credit"><bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:endEvent id="Done"/>
    <bpmn:endEvent id="Err"/>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="Credit"/>
    <bpmn:sequenceFlow id="b" sourceRef="Credit" targetRef="Done"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let ops = vec![json!({
            "op":"add_error_boundary","task":"Credit","errorCode":"CREDIT_BUREAU_ERROR","target":"Err"
        })];
        let v = edit_model(base, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        let boundary = def
            .elements
            .values()
            .find(|e| matches!(&e.kind, ElementKind::ErrorBoundaryEvent { .. }))
            .expect("an error boundary was added");
        match &boundary.kind {
            ElementKind::ErrorBoundaryEvent {
                attached_to,
                error_code,
            } => {
                assert_eq!(attached_to, "Credit");
                assert_eq!(error_code, "CREDIT_BUREAU_ERROR");
            }
            _ => unreachable!(),
        }
        assert!(boundary.outgoing.iter().any(|f| f.to == "Err"));
    }

    #[test]
    fn edit_model_removes_a_node_and_heals_flows() {
        // Remove the gateway: its predecessor (CreditCheck) should reconnect to the gateway's
        // successors (Approve, Reject), so no dangling reference remains.
        let ops = vec![json!({"op":"remove_node","id":"Decision"})];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        assert!(!def.elements.contains_key("Decision"));
        let cc = &def.elements["CreditCheck"];
        assert!(cc.outgoing.iter().any(|f| f.to == "Approve"));
        assert!(cc.outgoing.iter().any(|f| f.to == "Reject"));
    }

    #[test]
    fn edit_model_removes_a_boundary_orphan_beyond_the_error_timer_message_trio() {
        // Regression (#1173): removing an activity must drop EVERY boundary event
        // attached to it, not just the error/timer/message trio the orphan filter
        // used to enumerate. Signal/conditional/compensation/escalation boundaries
        // were silently leaked, emitting an invalid `attachedToRef` on the next
        // serialize. The filter now derives from the shared `attached_to` helper,
        // so this uses a SIGNAL boundary (previously missed) on a plain task to
        // pin the whole defect class; the same code path covers escalation.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:signal id="Sig" name="Cancelled" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="task">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="task">
      <bpmn:signalEventDefinition signalRef="Sig" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="task" />
    <bpmn:sequenceFlow id="f2" sourceRef="task" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;
        let ops = vec![json!({"op":"remove_node","id":"task"})];
        let v = edit_model(xml, &ops).expect("edit");
        let out = v["model"].as_str().unwrap();
        let def = first_def(out).unwrap().0;
        assert!(
            !def.elements.contains_key("task"),
            "the removed task is gone"
        );
        assert!(
            !def.elements.contains_key("Bnd"),
            "the signal boundary orphaned by removing its host must be dropped, got {:?}",
            def.elements.get("Bnd").map(|e| &e.kind)
        );
    }

    #[test]
    fn edit_model_adds_an_exclusive_gateway() {
        // Splice a decision gateway after CreditCheck, branching to Approve (guarded) with the
        // original path (to Decision) preserved as the default branch.
        let ops = vec![json!({
            "op":"add_exclusive_gateway","id":"Triage","after":"CreditCheck",
            "branches":[{"to":"Approve","condition":"= score > 800"}]
        })];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        let def = first_def(xml).unwrap().0;
        assert!(matches!(
            def.elements["Triage"].kind,
            ElementKind::ExclusiveGateway
        ));
        let cc = &def.elements["CreditCheck"];
        assert!(cc.outgoing.iter().any(|f| f.to == "Triage"));
        let triage = &def.elements["Triage"];
        assert!(triage
            .outgoing
            .iter()
            .any(|f| f.to == "Approve" && f.condition.is_some()));
        // The original CreditCheck -> Decision path is preserved through the gateway as default.
        assert!(triage
            .outgoing
            .iter()
            .any(|f| f.to == "Decision" && f.condition.is_none()));
    }

    #[test]
    fn edit_model_auto_heals_the_base_then_applies_ops() {
        // The base uses the bogus <errorBoundaryEvent>; edit_model heals it before editing, so a
        // job-type tweak still succeeds (and the heal is reported in appliedOps).
        let ops =
            vec![json!({"op":"set_task_job_type","task":"Task_CreditCheck","jobType":"bureau"})];
        let v = edit_model(ERROR_BOUNDARY_MISSPELLED, &ops).expect("edit heals + applies");
        let applied = v["appliedOps"].as_array().unwrap();
        assert!(applied
            .iter()
            .any(|n| n.as_str().unwrap().contains("errorBoundaryEvent")));
        let xml = v["model"].as_str().unwrap();
        assert!(!xml.contains("errorBoundaryEvent"));
        parse_bpmn(xml).expect("edited model parses");
    }

    #[test]
    fn edit_model_rejects_an_unknown_node_with_a_clear_error() {
        let ops = vec![json!({"op":"set_task_job_type","task":"Nope","jobType":"x"})];
        let err = edit_model(LOAN_BPMN, &ops).expect_err("should fail");
        assert!(err.contains("op 1 failed"), "got: {err}");
        assert!(err.contains("Nope"), "names the missing node: {err}");
    }

    // ── Human-readable output: labels + diagram interchange ──────────────────────────────────

    /// Pull the `x` coordinate of a node's `<bpmndi:BPMNShape>` Bounds from serialized XML.
    fn shape_x(xml: &str, id: &str) -> i64 {
        let marker = format!("bpmnElement=\"{id}\"");
        let after = xml
            .split(&marker)
            .nth(1)
            .unwrap_or_else(|| panic!("no shape for {id}"));
        let x_attr = after.split("x=\"").nth(1).expect("bounds x");
        x_attr
            .split('"')
            .next()
            .unwrap()
            .parse()
            .expect("x is an int")
    }

    #[test]
    fn definition_to_xml_emits_di_and_humanized_labels() {
        // Serializing a label-less model gives every node a readable name (humanized from its id)
        // and a generated diagram, so the authored variant renders and downloads legibly.
        let (orig, _) = first_def(LOAN_BPMN).expect("parse loan");
        let xml = definition_to_xml(&orig);
        assert!(xml.contains("<bpmndi:BPMNDiagram"), "carries a diagram");
        assert!(xml.contains("<bpmndi:BPMNShape"), "carries shapes");
        assert!(
            xml.contains("name=\"Credit Check\""),
            "humanizes CreditCheck -> 'Credit Check': {xml}"
        );
        // The DI must round-trip back through the engine parser without disturbing structure.
        let reparsed = parse_bpmn(&xml).expect("DI-bearing model re-parses");
        assert_same_structure(&orig, &reparsed[0]);
    }

    #[test]
    fn definition_to_xml_lays_the_model_out_left_to_right() {
        // A horizontal layout: each node sits to the right of its predecessor, so the start is
        // left of the tasks which are left of the end events.
        let (orig, _) = first_def(LOAN_BPMN).expect("parse loan");
        let xml = definition_to_xml(&orig);
        let start = shape_x(&xml, "Start");
        let credit = shape_x(&xml, "CreditCheck");
        let decision = shape_x(&xml, "Decision");
        let end = shape_x(&xml, "EndApproved");
        assert!(start < credit, "Start left of CreditCheck");
        assert!(credit < decision, "CreditCheck left of Decision");
        assert!(decision < end, "Decision left of the end event");
    }

    #[test]
    fn edit_model_preserves_existing_human_names() {
        // A base model that already carries operator names keeps them through an unrelated edit.
        let base = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" targetNamespace="t">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="S" name="Application received"/>
    <bpmn:serviceTask id="Credit" name="Pull credit bureau"><bpmn:extensionElements><zeebe:taskDefinition type="credit-check"/></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:endEvent id="Done" name="Decision made"/>
    <bpmn:sequenceFlow id="a" sourceRef="S" targetRef="Credit"/>
    <bpmn:sequenceFlow id="b" sourceRef="Credit" targetRef="Done"/>
  </bpmn:process>
</bpmn:definitions>"#;
        let ops = vec![json!({"op":"set_task_job_type","task":"Credit","jobType":"bureau-pull"})];
        let v = edit_model(base, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        assert!(
            xml.contains("name=\"Application received\""),
            "keeps the start label: {xml}"
        );
        assert!(
            xml.contains("name=\"Pull credit bureau\""),
            "keeps the task label: {xml}"
        );
        assert!(
            xml.contains("name=\"Decision made\""),
            "keeps the end label: {xml}"
        );
    }

    #[test]
    fn edit_model_set_name_relabels_a_node() {
        let ops = vec![json!({"op":"set_name","node":"CreditCheck","name":"Run fraud screen"})];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        assert_eq!(v["ok"], true);
        let xml = v["model"].as_str().unwrap();
        assert!(
            xml.contains("name=\"Run fraud screen\""),
            "applies the new label: {xml}"
        );
    }

    #[test]
    fn edit_model_insert_honors_a_supplied_name() {
        let ops = vec![json!({
            "op":"insert_service_task_after","after":"CreditCheck","id":"FraudCheck",
            "jobType":"fraud-check","name":"Screen for fraud"
        })];
        let v = edit_model(LOAN_BPMN, &ops).expect("edit");
        let xml = v["model"].as_str().unwrap();
        assert!(
            xml.contains("name=\"Screen for fraud\""),
            "labels the inserted task: {xml}"
        );
    }

    #[test]
    fn definition_to_xml_marks_inclusive_gateways() {
        // An inclusive (OR) gateway must also opt into `isMarkerVisible` so its
        // circle marker renders; without it the generated diagram shows an empty
        // diamond indistinguishable from a parallel gateway (#1168).
        const INCLUSIVE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:inclusiveGateway id="Split" default="toB" />
    <bpmn:task id="a" />
    <bpmn:task id="b" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="Split" />
    <bpmn:sequenceFlow id="toA" sourceRef="Split" targetRef="a">
      <bpmn:conditionExpression>=go</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="toB" sourceRef="Split" targetRef="b" />
    <bpmn:sequenceFlow id="fa" sourceRef="a" targetRef="e" />
    <bpmn:sequenceFlow id="fb" sourceRef="b" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
        let (def, _) = first_def(INCLUSIVE).expect("parse inclusive");
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("bpmnElement=\"Split\" isMarkerVisible=\"true\""),
            "inclusive gateway opts into the marker: {xml}"
        );
        // The marker is not the only routing metadata the emitter adds for an
        // inclusive gateway: it also serializes the `default` flow and the guarded
        // branch's `conditionExpression`. Asserting only `isMarkerVisible` would
        // still pass if either were silently dropped, so protect the round-trip.
        // Flow ids are synthesized on emit, so `default` references a synthesized
        // id (not `toB`); re-parse the emitted XML and assert the semantics survive.
        assert!(
            xml.contains("<bpmn:conditionExpression>=go</bpmn:conditionExpression>"),
            "guarded branch's condition is emitted: {xml}"
        );
        let (round, _) = first_def(&xml).expect("re-parse emitted inclusive");
        let split = &round.elements["Split"];
        assert!(
            split.outgoing.iter().any(|f| f.is_default),
            "default flow survives the round-trip: {xml}"
        );
        assert!(
            split.outgoing.iter().any(|f| f.condition.is_some()),
            "guarded branch survives the round-trip: {xml}"
        );
    }

    #[test]
    fn edit_model_set_name_rejects_a_missing_node() {
        let ops = vec![json!({"op":"set_name","node":"Nope","name":"x"})];
        let err = edit_model(LOAN_BPMN, &ops).expect_err("should fail");
        assert!(err.contains("Nope"), "names the missing node: {err}");
    }

    #[test]
    fn definition_to_xml_marks_exclusive_gateways_and_labels_guards() {
        // The Investigation-4 shape: a guarded XOR split. The serializer must (a) flag the gateway
        // shape isMarkerVisible so its X renders, and (b) name each guarded branch with its
        // condition so the guard is legible (bpmn.js draws the name, not the conditionExpression).
        let (def, _) = first_def(LOAN_BPMN).expect("parse loan");
        let xml = definition_to_xml(&def);
        assert!(
            xml.contains("bpmnElement=\"Decision\" isMarkerVisible=\"true\""),
            "exclusive gateway opts into the X marker: {xml}"
        );
        // The LOAN model guards Decision->Approve with `creditScore >= 700`.
        assert!(
            xml.contains("<bpmn:sequenceFlow") && xml.contains("name=\"creditScore &gt;= 700\""),
            "guarded branch carries its condition as a name: {xml}"
        );
        assert!(
            xml.contains("<bpmndi:BPMNLabel>"),
            "guarded edge carries a label shape: {xml}"
        );
    }

    // ---- Multi-stage orchestrator: merge / expand / raw-XML / nested edit ----

    const CDD_ORCH: &str = include_str!("../corpus-packs/cdd-refresh/orchestrator.bpmn");
    const CDD_PHASE2: &str =
        include_str!("../corpus-packs/cdd-refresh/phases/process-02-document-request.bpmn");
    const CDD_PHASE5: &str =
        include_str!("../corpus-packs/cdd-refresh/phases/process-05-parallel-screening.bpmn");
    const CDD_PHASES: [&str; 8] = [
        include_str!("../corpus-packs/cdd-refresh/phases/process-01-intake.bpmn"),
        CDD_PHASE2,
        include_str!("../corpus-packs/cdd-refresh/phases/process-03-reminders-and-escalation.bpmn"),
        CDD_PHASE5,
        include_str!("../corpus-packs/cdd-refresh/phases/process-06-sanctions-gate.bpmn"),
        include_str!("../corpus-packs/cdd-refresh/phases/process-07-risk-assessment.bpmn"),
        include_str!("../corpus-packs/cdd-refresh/phases/process-08-approval.bpmn"),
        include_str!("../corpus-packs/cdd-refresh/phases/process-09-closure.bpmn"),
    ];

    fn merged_cdd() -> String {
        merge_definitions(CDD_ORCH, &CDD_PHASES)
    }

    #[test]
    fn merge_definitions_produces_one_self_contained_parseable_document() {
        let merged = merged_cdd();
        let defs = parse_bpmn(&merged).expect("merged model parses");
        // Orchestrator + the two phases we merged.
        let ids: Vec<&str> = defs.iter().map(|d| d.id.as_str()).collect();
        assert!(
            ids.contains(&"Process02DocumentRequest"),
            "phase 2 present: {ids:?}"
        );
        assert!(
            ids.contains(&"Process05ParallelScreeningProcess"),
            "phase 5 present: {ids:?}"
        );
        // Exactly one diagram survives (the orchestrator's).
        assert_eq!(
            merged.matches("<bpmndi:BPMNDiagram").count(),
            1,
            "single diagram"
        );
    }

    #[test]
    fn read_model_overview_flags_expandable_and_exposes_called_element() {
        let v = read_model(&merged_cdd()).expect("read");
        assert_eq!(v["expandable"], json!(true));
        let nodes = v["nodes"].as_array().unwrap();
        let call = nodes
            .iter()
            .find(|n| n["id"] == "Phase2_DocumentRequest")
            .expect("phase 2 call activity present");
        assert_eq!(call["calledElement"], json!("Process02DocumentRequest"));
    }

    #[test]
    fn read_model_expanded_uses_parent_child_trace_ids() {
        let v = read_model_expanded(&merged_cdd()).expect("expand");
        assert_eq!(v["expanded"], json!(true));
        let ids: Vec<String> = v["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap_or("").to_string())
            .collect();
        // The inner task the trace tables address as Parent$Child is now a visible node.
        assert!(
            ids.iter()
                .any(|i| i == "Phase2_DocumentRequest$Task_SendRefreshRequest"),
            "expanded view exposes the inlined trace id: {ids:?}"
        );
    }

    #[test]
    fn read_model_xml_resolves_a_call_activity_to_its_phase() {
        let merged = merged_cdd();
        // By callActivity node id (resolved to calledElement).
        let v = read_model_xml(&merged, Some("Phase2_DocumentRequest")).expect("xml by node");
        assert_eq!(v["processId"], json!("Process02DocumentRequest"));
        let block = v["xml"].as_str().unwrap();
        assert!(
            block.contains("Task_SendRefreshRequest"),
            "phase 2 body: {block}"
        );
        assert!(
            !block.contains("Process05"),
            "only phase 2, not other phases"
        );
        // By process id directly.
        let v2 = read_model_xml(&merged, Some("Process05ParallelScreeningProcess")).expect("xml");
        assert_eq!(v2["processId"], json!("Process05ParallelScreeningProcess"));
        // Whole document.
        let full = read_model_xml(&merged, None).expect("full");
        assert_eq!(full["scope"], json!("full"));
        assert!(full["processIds"].as_array().unwrap().len() >= 3);
    }

    #[test]
    fn edit_model_in_edits_a_phase_and_preserves_the_other_definitions() {
        let merged = merged_cdd();
        // Rename a task INSIDE phase 2; the orchestrator and phase 5 must survive.
        let ops = vec![json!({
            "op": "set_name",
            "node": "Task_SendRefreshRequest",
            "name": "Send the refresh request packet"
        })];
        let v = edit_model_in(&merged, &ops, Some("Process02DocumentRequest")).expect("edit phase");
        assert_eq!(v["editedProcess"], json!("Process02DocumentRequest"));
        let model = v["model"].as_str().unwrap();
        let defs = parse_bpmn(model).expect("edited model still parses");
        let ids: Vec<&str> = defs.iter().map(|d| d.id.as_str()).collect();
        assert!(
            ids.contains(&"Process05ParallelScreeningProcess"),
            "phase 5 preserved: {ids:?}"
        );
        assert!(
            ids.contains(&"CddRefreshOrchestrator"),
            "orchestrator preserved: {ids:?}"
        );
        // The edit can still be inlined for replay.
        let inlined = inline_definition(model, None).expect("inline edited model");
        assert!(
            inlined.elements.keys().any(|k| k.contains('$')),
            "still inlines to Parent$Child"
        );
    }

    #[test]
    fn edit_model_in_rejects_an_unknown_phase_with_the_known_ids() {
        let ops = vec![json!({"op":"set_name","node":"X","name":"Y"})];
        let err = edit_model_in(&merged_cdd(), &ops, Some("NoSuchPhase")).expect_err("should fail");
        assert!(
            err.contains("Process02DocumentRequest"),
            "lists known ids: {err}"
        );
    }

    // ── Slice 4 (ADR 0002): nano:* extension round-trip ─────────────────────────

    const NANO_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  xmlns:nano="http://nano.camunda.io/schema/semantic/1.0">
  <bpmn:process id="Proc" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>F1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="Credit" name="Credit check">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="credit"/>
        <nano:cost value="0.50" currency="USD" per="invocation"/>
        <nano:time p50="2s" p99="8s"/>
        <nano:role>external</nano:role>
      </bpmn:extensionElements>
      <bpmn:incoming>F1</bpmn:incoming>
      <bpmn:outgoing>F2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="End"><bpmn:incoming>F2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Credit"/>
    <bpmn:sequenceFlow id="F2" sourceRef="Credit" targetRef="End"/>
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn extract_nano_extensions_captures_all_nano_children() {
        let ext = extract_nano_extensions(NANO_BPMN);
        let block = ext.get("Credit").expect("Credit task has nano extensions");
        assert!(block.contains("<nano:cost"), "captured nano:cost: {block}");
        assert!(block.contains("<nano:time"), "captured nano:time: {block}");
        assert!(
            block.contains("<nano:role>external</nano:role>"),
            "captured nano:role with content: {block}"
        );
        // The zeebe:taskDefinition sibling must NOT be captured — that's the
        // engine parser's territory.
        assert!(
            !block.contains("taskDefinition"),
            "did not capture zeebe children: {block}"
        );
    }

    #[test]
    fn extract_nano_extensions_returns_empty_for_a_model_without_nano() {
        let ext = extract_nano_extensions(LOAN_BPMN);
        assert!(
            ext.is_empty(),
            "no nano:* content in loan fixture, got {:?}",
            ext.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn definition_to_xml_labeled_preserves_nano_extensions_across_round_trip() {
        let def = parse_bpmn(NANO_BPMN)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one process");
        let emitted = definition_to_xml_labeled(&def, &HashMap::new());
        assert!(
            emitted.contains("xmlns:nano="),
            "definitions root re-declares nano namespace: {emitted}"
        );
        assert!(
            emitted.contains("<nano:cost value=\"0.50\""),
            "nano:cost survives the round trip: {emitted}"
        );
        assert!(
            emitted.contains("<nano:time p50=\"2s\""),
            "nano:time survives: {emitted}"
        );
        assert!(
            emitted.contains("<nano:role>external</nano:role>"),
            "nano:role with text content survives: {emitted}"
        );
        // The re-emitted document must still parse as valid BPMN.
        let redef = parse_bpmn(&emitted).expect("re-parse");
        assert_eq!(redef.len(), 1);
    }

    #[test]
    fn definition_to_xml_with_row_bias_preserves_nano_extensions() {
        let def = parse_bpmn(NANO_BPMN)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one process");
        let out = definition_to_xml_with_row_bias(&def, &HashMap::new(), &HashMap::new());
        assert!(
            out.contains("<nano:cost"),
            "row-bias emit preserves nano:cost"
        );
        assert!(out.contains("<nano:role>external</nano:role>"));
    }

    #[test]
    fn preserve_nano_extensions_in_is_a_no_op_when_map_is_empty() {
        let same = preserve_nano_extensions_in("<bpmn:definitions/>".to_string(), &HashMap::new());
        assert_eq!(same, "<bpmn:definitions/>");
    }

    #[test]
    fn form_definition_emission_lets_external_reference_win_over_form_id() {
        // Zeebe treats formId and externalReference as mutually exclusive. When a
        // UserTaskProps carries both (e.g. built programmatically), the emitter
        // must surface only externalReference, so a re-parsed model never carries
        // both a numeric formKey and an externalFormReference.
        use nanobpmn_engine_core::{ProcessBuilder, UserTaskProps};
        let def = ProcessBuilder::new("p")
            .start_event("s")
            .user_task_with(
                "both",
                UserTaskProps {
                    form_id: Some("feature-escalation".into()),
                    external_form_reference: Some("https://forms.example/x".into()),
                    ..Default::default()
                },
            )
            .end_event("e")
            .connect("s", "both")
            .connect("both", "e")
            .build()
            .expect("build");

        let xml = definition_to_xml_labeled(&def, &HashMap::new());
        assert!(
            xml.contains(r#"externalReference="https://forms.example/x""#),
            "external reference is emitted:\n{xml}"
        );
        assert!(
            !xml.contains("formId="),
            "formId is suppressed when an external reference is present:\n{xml}"
        );

        // And it round-trips to the mutually-exclusive shape.
        let round = &parse_bpmn(&xml).unwrap()[0];
        let ElementKind::UserTask(props) = &round.element("both").unwrap().kind else {
            panic!("expected a user task");
        };
        assert_eq!(props.form_id, None);
        assert_eq!(
            props.external_form_reference.as_deref(),
            Some("https://forms.example/x")
        );
    }
}
