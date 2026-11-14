//! Phase 1 of the reversible semantic IR (ADR 0001): the **canonical pretty-printer**
//! `ProcessDefinition (+ names) → IR text`.
//!
//! The IR is the surface syntax of the engine's own executable model, so it cannot drift from
//! execution semantics. This module only *emits* the IR; the validating parser (IR → model) and
//! the engine-derived grammar/GBNF are later phases. The output is a **normal form**: elements are
//! rendered in a deterministic order (sorted by id) and sequence flows as a separate block grouped
//! by source id — but WITHIN each source the model's declared outgoing order is preserved verbatim,
//! because an exclusive gateway takes the *first* matching outgoing flow, so that order is
//! load-bearing. `definition_to_ir` is thus a pure function of the model and two runs always agree
//! (a prerequisite for the future `emit(parse(ir)) == ir` idempotence guarantee).
//!
//! ## Concrete syntax
//!
//! ```text
//! process "<id>" {
//!   start <startEventId>
//!
//!   <node statements, sorted by id>
//!
//!   <flow statements, grouped by source id; per-source declaration order preserved>
//! }
//! ```
//!
//! A **node statement** is `<kindKeyword> <id> ["<name>"] [{ <attrs> }]` where the keyword is the
//! BPMN element kind (`serviceTask`, `exclusiveGateway`, …), the optional quoted name is the
//! preserved human label, and the optional brace block carries the load-bearing attributes for that
//! kind plus any element-level annotations (io mappings, retries, timer expression, multi-instance,
//! sub-process parent).
//!
//! A **flow statement** is `<from> -> <to> [when "<expr>"] [default]`: `when` carries a guard
//! condition, `default` marks a condition-routed gateway's fallback flow (an exclusive or
//! inclusive gateway) — the exact construct the
//! imperative `edit_model` API could not express.
//!
//! The kind dispatch (`render_kind_attrs`) is an **exhaustive match** over `ElementKind`, so adding
//! a new engine element kind fails to compile until the IR learns to render it — the
//! coverage guarantee ADR 0001 relies on.

// The pretty-printer (Phase 1), the inverse parser (Phase 2), and the `read_model_ir` /
// `write_model_ir` LLM tools (Phase 4) are all wired. `analyze_ir` remains an exercised-in-tests
// convenience with no binary caller yet (write_model_ir already returns analyze_model findings), so
// the module keeps a dead-code allowance for that residue.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    Condition, Element, ElementKind, IoMapping, Mapping, MultiInstance, ProcessDefinition,
    SequenceFlow, TimerDef, TimerDefKind,
};

/// Render a process definition to canonical IR text. `names` maps element id → human label (as
/// recovered from the source BPMN, which the engine model itself drops); ids absent from the map
/// render without a quoted name.
pub fn definition_to_ir(def: &ProcessDefinition, names: &HashMap<String, String>) -> String {
    let mut normalized = def.clone();
    normalized.normalize_legacy_agent_tasks();
    let def = &normalized;
    let mut out = String::new();
    out.push_str(&format!("process {} {{\n", quote(&def.id)));
    out.push_str(&format!("  start {}\n", def.start_event));

    // Nodes in a deterministic order (sorted by id) so the emission is a normal form.
    let ordered: BTreeMap<&str, &Element> =
        def.elements.iter().map(|(k, v)| (k.as_str(), v)).collect();

    if !ordered.is_empty() {
        out.push('\n');
    }
    for (id, el) in &ordered {
        render_node(id, el, names.get(*id).map(String::as_str), &mut out);
    }

    // Sequence flows as one block after the nodes. Grouped by source id for a deterministic order,
    // but WITHIN a source the model's declared outgoing order is preserved verbatim: for an
    // exclusive gateway the engine takes the first matching outgoing flow (see the declaration-order
    // scan in engine/mod.rs), so that order is load-bearing and must not be re-sorted by target.
    // `ordered` is a BTreeMap (iterates by source id) and each element's `outgoing` is kept in
    // declaration order, so the collection is already grouped-by-id in declaration order; a *stable*
    // sort by source id only cements the grouping without disturbing the per-source order.
    let mut flows: Vec<(&str, &SequenceFlow)> = Vec::new();
    for (id, el) in &ordered {
        for f in &el.outgoing {
            flows.push((id, f));
        }
    }
    flows.sort_by(|a, b| a.0.cmp(b.0));
    if !flows.is_empty() {
        out.push('\n');
    }
    for (from, f) in &flows {
        render_flow(from, f, &mut out);
    }

    out.push_str("}\n");
    out
}

/// Parse BPMN and render it to IR in one step, recovering element names from the XML. A convenience
/// for callers (and the future `read_model_ir` tool) that hold raw BPMN rather than a parsed model.
pub fn xml_to_ir(xml: &str) -> Result<String, String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("model failed to parse: {e:?}"))?;
    let def = defs
        .into_iter()
        .next()
        .ok_or_else(|| "no process definition in the document".to_string())?;
    let names = crate::bpmn_model::parse_element_names(xml);
    Ok(definition_to_ir(&def, &names))
}

fn render_node(id: &str, el: &Element, name: Option<&str>, out: &mut String) {
    out.push_str("  ");
    out.push_str(kind_keyword(&el.kind));
    out.push(' ');
    out.push_str(id);
    if let Some(n) = name {
        if !n.trim().is_empty() {
            out.push(' ');
            out.push_str(&quote(n));
        }
    }

    let mut attrs: Vec<String> = Vec::new();
    render_kind_attrs(&el.kind, &mut attrs);
    render_element_attrs(el, &mut attrs);

    if attrs.is_empty() {
        out.push('\n');
    } else {
        out.push_str(" {\n");
        for a in &attrs {
            out.push_str("    ");
            out.push_str(a);
            out.push('\n');
        }
        out.push_str("  }\n");
    }
}

/// The IR keyword for an element kind — the surface-syntax half of the notation table. Mirrors the
/// BPMN element names so a modeler recognises them.
fn kind_keyword(kind: &ElementKind) -> &'static str {
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

/// Emit the kind-specific attribute lines. Exhaustive over `ElementKind`: a new engine element kind
/// will not compile until its IR notation is added here (ADR 0001 coverage guarantee).
fn render_kind_attrs(kind: &ElementKind, attrs: &mut Vec<String>) {
    match kind {
        ElementKind::StartEvent
        | ElementKind::EndEvent
        | ElementKind::TerminateEndEvent
        | ElementKind::ExclusiveGateway
        | ElementKind::ParallelGateway
        | ElementKind::InclusiveGateway
        | ElementKind::EventBasedGateway
        | ElementKind::IntermediateThrowEvent
        | ElementKind::CompensationThrowEvent
        | ElementKind::Task => {}
        ElementKind::ServiceTask {
            job_type,
            priority,
            custom_headers,
            // Linked resources (zeebe:linkedResources) are not carried by the
            // compact IR text format; only the lossless BPMN XML emitter
            // round-trips them. Ignore here so the IR stays stable.
            linked_resources: _,
            agent_type,
        } => {
            attrs.push(format!("jobType {}", quote(job_type)));
            if let Some(p) = priority {
                attrs.push(format!("priority {}", quote(p)));
            }
            if let Some(agent_type) = agent_type {
                attrs.push(format!("agentType {}", quote(agent_type.as_str())));
            }
            for (k, v) in custom_headers {
                attrs.push(format!("header {} <- {}", quote(k), quote(v)));
            }
        }
        ElementKind::BusinessRuleTask {
            decision_id,
            result_variable,
        } => {
            attrs.push(format!("decisionId {}", quote(decision_id)));
            if let Some(v) = result_variable {
                attrs.push(format!("resultVariable {}", quote(v)));
            }
        }
        ElementKind::AgentTask { .. } => unreachable!("legacy agent tasks are normalized"),
        ElementKind::UserTask(props) => {
            if let Some(v) = &props.assignee {
                attrs.push(format!("assignee {}", quote(v)));
            }
            if let Some(v) = &props.candidate_groups {
                attrs.push(format!("candidateGroups {}", quote(v)));
            }
            if let Some(v) = &props.candidate_users {
                attrs.push(format!("candidateUsers {}", quote(v)));
            }
            if let Some(v) = &props.due_date {
                attrs.push(format!("dueDate {}", quote(v)));
            }
            if let Some(v) = &props.follow_up_date {
                attrs.push(format!("followUpDate {}", quote(v)));
            }
            if let Some(v) = &props.priority {
                attrs.push(format!("priority {}", quote(v)));
            }
            if let Some(v) = &props.form_id {
                attrs.push(format!("formId {}", quote(v)));
            }
            if let Some(v) = &props.external_form_reference {
                attrs.push(format!("externalFormReference {}", quote(v)));
            }
        }
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("errorCode {}", quote(error_code)));
        }
        ElementKind::TimerIntermediateCatchEvent { duration_millis } => {
            attrs.push(format!("duration {duration_millis}ms"));
        }
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("duration {duration_millis}ms"));
            attrs.push(format!("interrupting {interrupting}"));
            attrs.push(format!("repeating {repeating}"));
        }
        ElementKind::MessageIntermediateCatchEvent {
            message_name,
            correlation_key,
        } => {
            attrs.push(format!("message {}", quote(message_name)));
            attrs.push(format!("correlationKey {}", quote(correlation_key)));
        }
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("message {}", quote(message_name)));
            attrs.push(format!("correlationKey {}", quote(correlation_key)));
            attrs.push(format!("interrupting {interrupting}"));
        }
        ElementKind::MessageStartEvent { message_name } => {
            attrs.push(format!("message {}", quote(message_name)));
        }
        ElementKind::TimerStartEvent {
            interval_millis,
            repeating,
        } => {
            attrs.push(format!("interval {interval_millis}ms"));
            attrs.push(format!("repeating {repeating}"));
        }
        ElementKind::SubProcess { start_event } => {
            attrs.push(format!("startEvent {start_event}"));
        }
        ElementKind::ScriptTask {
            expression,
            result_variable,
        } => {
            attrs.push(format!("expression {}", quote(expression)));
            attrs.push(format!("resultVariable {}", quote(result_variable)));
        }
        ElementKind::CallActivity {
            called_process_id,
            propagate_all_parent_variables,
            propagate_all_child_variables,
        } => {
            attrs.push(format!("calledElement {}", quote(called_process_id)));
            attrs.push(format!(
                "propagateAllParentVariables {propagate_all_parent_variables}"
            ));
            attrs.push(format!(
                "propagateAllChildVariables {propagate_all_child_variables}"
            ));
        }
        ElementKind::SignalIntermediateCatchEvent { signal_name } => {
            attrs.push(format!("signal {}", quote(signal_name)));
        }
        ElementKind::LinkIntermediateThrowEvent { link_name }
        | ElementKind::LinkIntermediateCatchEvent { link_name } => {
            attrs.push(format!("link {}", quote(link_name)));
        }
        ElementKind::SignalBoundaryEvent {
            attached_to,
            signal_name,
            interrupting,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("signal {}", quote(signal_name)));
            attrs.push(format!("interrupting {interrupting}"));
        }
        ElementKind::ConditionalIntermediateCatchEvent { condition } => {
            attrs.push(format!("condition {}", quote(condition)));
        }
        ElementKind::ConditionalBoundaryEvent {
            attached_to,
            condition,
            interrupting,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("condition {}", quote(condition)));
            attrs.push(format!("interrupting {interrupting}"));
        }
        ElementKind::CompensationBoundaryEvent {
            attached_to,
            handler,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("handler {handler}"));
        }
        ElementKind::EscalationThrowEvent { escalation_code } => {
            if !escalation_code.is_empty() {
                attrs.push(format!("escalationCode {}", quote(escalation_code)));
            }
        }
        ElementKind::EscalationBoundaryEvent {
            attached_to,
            escalation_code,
            interrupting,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            if !escalation_code.is_empty() {
                attrs.push(format!("escalationCode {}", quote(escalation_code)));
            }
            attrs.push(format!("interrupting {interrupting}"));
        }
    }
}

/// Emit element-level annotations shared across kinds (sub-process parent, job retries, timer FEEL
/// expression, io mappings, multi-instance). These are the engine-ignored-but-load-bearing extras
/// the IR preserves.
fn render_element_attrs(el: &Element, attrs: &mut Vec<String>) {
    if let Some(parent) = &el.parent {
        attrs.push(format!("parent {parent}"));
    }
    if let Some(retries) = &el.retries {
        attrs.push(format!("retries {}", quote(retries)));
    }
    if let Some(timer) = &el.timer {
        attrs.push(format!("timer {}", render_timer(timer)));
    }
    for m in &el.io.inputs {
        attrs.push(format!("input {} <- {}", m.target, quote(&m.source)));
    }
    for m in &el.io.outputs {
        attrs.push(format!("output {} <- {}", m.target, quote(&m.source)));
    }
    // `zeebe:properties` extension properties (e.g. the `io.camunda.tool:*` MCP
    // tool metadata, #1499) round-trip through the IR so an IR edit → BPMN
    // re-emit never silently strips them. BTreeMap order keeps the emission
    // deterministic.
    for (name, value) in &el.properties {
        attrs.push(format!("property {} {}", quote(name), quote(value)));
    }
    if let Some(mi) = &el.multi_instance {
        let mut parts: Vec<String> = vec![format!("collection {}", quote(&mi.input_collection))];
        if let Some(v) = &mi.input_element {
            parts.push(format!("inputElement {}", quote(v)));
        }
        if let Some(v) = &mi.output_collection {
            parts.push(format!("outputCollection {}", quote(v)));
        }
        if let Some(v) = &mi.output_element {
            parts.push(format!("outputElement {}", quote(v)));
        }
        if let Some(v) = &mi.completion_condition {
            parts.push(format!("completionCondition {}", quote(v)));
        }
        if !mi.cancel_remaining_instances {
            // Only the non-default value is rendered, keeping existing IR stable.
            parts.push("cancelRemainingInstances false".to_string());
        }
        parts.push(format!("sequential {}", mi.sequential));
        attrs.push(format!("multiInstance {{ {} }}", parts.join(", ")));
    }
}

fn render_timer(timer: &TimerDef) -> String {
    let kind = match timer.kind {
        TimerDefKind::Duration => "duration",
        TimerDefKind::Cycle => "cycle",
        TimerDefKind::Date => "date",
    };
    format!("{kind} {}", quote(&timer.expr))
}

fn render_flow(from: &str, f: &SequenceFlow, out: &mut String) {
    out.push_str(&format!("  {from} -> {}", f.to));
    if let Some(c) = &f.condition {
        out.push_str(&format!(" when {}", quote(&c.expression)));
    }
    if f.is_default {
        out.push_str(" default");
    }
    out.push('\n');
}

/// Quote a free-text value (name, FEEL expression, job type, …) as a double-quoted string with `\`
/// and `"` escaped, so values containing spaces or IR punctuation round-trip unambiguously.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

// ===========================================================================================
// Phase 2 (ADR 0001): the total, validating parser `IR text → ProcessDefinition`.
//
// This is the inverse of `definition_to_ir`. It shares the same concrete notation, so the pair is a
// bijection over the executable core: `emit(parse(ir)) == ir` for any IR this module emits (checked
// at unit level here; ADR 0001 phase 3 extends it to a corpus replay property). The parser is
// *total* — every input yields a `Result`, never a panic — and *validating*: it enforces structural
// invariants itself and reuses `analyze_model` (via `analyze_ir`) for semantic advisories.
// ===========================================================================================

/// A parsed IR document: the executable model plus the element id → human name map the IR carried
/// (the names the engine model itself drops).
#[derive(Debug, Clone)]
pub struct ParsedIr {
    pub definition: ProcessDefinition,
    pub names: HashMap<String, String>,
}

/// Parse canonical IR text into a [`ProcessDefinition`] and its element-name map, validating
/// structure along the way. Returns a human-readable error (never panics) on malformed input,
/// unknown element kinds, missing required attributes, or dangling references.
pub fn ir_to_definition(ir: &str) -> Result<ParsedIr, String> {
    let tokens = tokenize(ir)?;
    let mut p = Parser::new(&tokens);
    p.parse_document()
}

/// Parse IR and run the existing semantic analyzer over it, reusing `analyze_model`. The IR is
/// lowered to BPMN via `definition_to_xml_labeled` (which `parse_bpmn` round-trips) and handed to
/// [`crate::bpmn_model::analyze_model`], so IR authoring gets the same advisories (unguarded tasks,
/// gateways without a default, unreachable nodes, …) as XML authoring.
pub fn analyze_ir(ir: &str) -> Result<serde_json::Value, String> {
    let parsed = ir_to_definition(ir)?;
    let xml = crate::bpmn_model::definition_to_xml_labeled(&parsed.definition, &parsed.names);
    crate::bpmn_model::analyze_model(&xml)
}

/// `read_model_ir` LLM tool (ADR 0001 Phase 4): emit the executable IR for a model. With no
/// `target`, returns the IR for the first (orchestrator) definition. Pass a process id — or a
/// callActivity node id, resolved to its `calledElement` — to isolate a called phase's IR (the
/// same targeting `read_model_xml` uses), so an operator can read and edit one phase of a
/// multi-stage model. The returned IR round-trips through `write_model_ir` (ADR Phase 3 proves the
/// executable core survives), so it is the surface an LLM edits instead of hand-writing XML.
pub fn read_model_ir(xml: &str, target: Option<&str>) -> Result<serde_json::Value, String> {
    match target {
        None => {
            let ir = xml_to_ir(xml)?;
            Ok(serde_json::json!({
                "scope": "full",
                "ir": ir,
                "note": "The executable IR for this process (the orchestrator, for a multi-stage \
                         model). Edit it and deploy the result with write_model_ir. Pass \
                         process:\"<id>\" (a process id or a callActivity node id) to read a \
                         called phase's IR instead.",
            }))
        }
        Some(_) => {
            let block = crate::bpmn_model::read_model_xml(xml, target)?;
            let phase_xml = block
                .get("xml")
                .and_then(|v| v.as_str())
                .ok_or("could not isolate the phase XML for that target")?;
            let ir = xml_to_ir(phase_xml)?;
            let process_id = block
                .get("processId")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            Ok(serde_json::json!({
                "scope": "process",
                "processId": process_id,
                "ir": ir,
                "note": "The executable IR for this phase. Edit it, then deploy with \
                         write_model_ir passing base:<full model xml> and process:\"<id>\" so it \
                         is spliced back into the document, preserving the other phases and the \
                         overview diagram. Keep the IR's `process \"<id>\"` header matching the \
                         phase you read.",
            }))
        }
    }
}

/// `write_model_ir` LLM tool (ADR 0001 Phase 4): parse + validate + compile IR to a deployable,
/// engine-validated BPMN model — the inverse of `read_model_ir` and the write side that retires
/// hand-written XML. `ir_to_definition` validates structure (unknown kinds, missing attributes,
/// dangling flow targets) with a human-readable error before anything is emitted.
///
/// With no `base`, the IR *is* the whole model and compiles standalone; when `di_source` (the XML
/// the model was read from) carries a hand-authored diagram and the edit left the node + flow
/// topology unchanged, that layout is preserved verbatim (ADR 0001 Phase 5), else it is auto-laid.
/// Pass `base` (the full document) and `process` (the phase id) to splice the IR back into a
/// multi-stage model in place, preserving the other phase definitions and the authored overview —
/// the same capability as `edit_model process:"<id>"`, but IR-driven (the spliced phase is
/// auto-laid). The result mirrors `edit_model`: a ready-to-simulate `model` plus the post-write
/// `analyze_model` findings/metrics.
pub fn write_model_ir(
    ir: &str,
    base: Option<&str>,
    process: Option<&str>,
    di_source: Option<&str>,
) -> Result<serde_json::Value, String> {
    let parsed = ir_to_definition(ir)?;
    let (xml, analysis_xml, edited_process) = match base {
        None => {
            // Without a base the IR *is* the whole model. A `process` arg here is meaningless and
            // usually signals the caller meant to splice into a multi-stage doc — fail fast rather
            // than silently write a standalone model that ignores it.
            if let Some(pid) = process {
                if pid != parsed.definition.id {
                    return Err(format!(
                        "`process` was '{pid}' but no `base` model was given, so the IR is written \
                         as a standalone model with id '{}'. To splice a phase into a multi-stage \
                         model, pass the full document as `base`. To write a standalone model, omit \
                         `process` (or match it to the IR's process id).",
                        parsed.definition.id
                    ));
                }
            }
            // Single-process write: re-attach the original hand layout when the edit left the node
            // and flow topology unchanged (ADR 0001 Phase 5), else auto-layout.
            let xml = crate::bpmn_model::definition_to_xml_preserving_di(
                &parsed.definition,
                &parsed.names,
                di_source,
            );
            let id = parsed.definition.id.clone();
            // The whole model is the edited process, so it is also what we analyze.
            (xml.clone(), xml, id)
        }
        Some(base_xml) => {
            let mut defs =
                parse_bpmn(base_xml).map_err(|e| format!("base model failed to parse: {e:?}"))?;
            if defs.is_empty() {
                return Err("base model contained no process definitions".to_string());
            }
            // Preserve the base document's labels; the IR's names override for the replaced phase.
            let mut names = crate::bpmn_model::parse_element_names(base_xml);
            for (k, v) in &parsed.names {
                names.insert(k.clone(), v.clone());
            }
            let target_idx = match process {
                Some(pid) => defs.iter().position(|d| d.id == pid).ok_or_else(|| {
                    format!(
                        "no process '{pid}' in the base model. Known process ids: {}. Omit \
                         `process` to replace the orchestrator (first) definition.",
                        defs.iter()
                            .map(|d| d.id.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })?,
                None => 0,
            };
            // The IR must keep the phase's id: the splice replaces the target definition in place,
            // and a renamed process would orphan the callActivity `calledElement` that invokes it.
            if parsed.definition.id != defs[target_idx].id {
                return Err(format!(
                    "the IR's `process \"{}\"` header does not match the phase being replaced \
                     ('{}'). Splicing keeps the phase id stable so the orchestrator's callActivity \
                     still resolves — read the phase with read_model_ir and keep its header id.",
                    parsed.definition.id, defs[target_idx].id
                ));
            }
            // The findings/metrics must describe the edited phase, not the first (orchestrator)
            // definition analyze_model would otherwise pick — so analyze the phase in isolation.
            let analysis_xml =
                crate::bpmn_model::definition_to_xml_labeled(&parsed.definition, &parsed.names);
            let id = parsed.definition.id.clone();
            defs[target_idx] = parsed.definition;
            let xml = crate::bpmn_model::assemble_model(&defs, &names);
            (xml, analysis_xml, id)
        }
    };

    // Defensive re-parse: never hand back XML the engine would reject at deploy time.
    if let Err(e) = parse_bpmn(&xml) {
        return Err(format!(
            "the IR compiled to XML that does not parse ({e:?}). This usually means a dangling \
             reference — a flow to a node that isn't declared."
        ));
    }
    let analysis =
        crate::bpmn_model::analyze_model(&analysis_xml).unwrap_or_else(|_| serde_json::json!({}));
    Ok(serde_json::json!({
        "ok": true,
        "process": edited_process,
        "model": xml,
        "findings": analysis.get("findings").cloned().unwrap_or(serde_json::json!([])),
        "metrics": analysis.get("metrics").cloned().unwrap_or(serde_json::json!({})),
        "note": "This XML is engine-validated and ready to simulate (start with limit:1) or \
                 compare_variants. Do NOT hand-edit it — author further changes in IR and call \
                 write_model_ir again.",
    }))
}

// --- tokenizer ------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A bareword: an id, keyword, boolean, or a number-with-unit like `5000ms`.
    Word(String),
    /// A decoded (unescaped) double-quoted string.
    Str(String),
    Arrow,  // ->
    LArrow, // <-
    LBrace, // {
    RBrace, // }
    Comma,  // ,
}

/// Whether `c` may appear in a bareword (element ids can contain `-`, `.`, `:`, `_`).
fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '.' | ':' | '-')
}

fn tokenize(src: &str) -> Result<Vec<Tok>, String> {
    let mut toks = Vec::new();
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '"' => {
                let (s, next) = read_string(&chars, i)?;
                toks.push(Tok::Str(s));
                i = next;
            }
            '{' => {
                toks.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                toks.push(Tok::RBrace);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            '-' if chars.get(i + 1) == Some(&'>') => {
                toks.push(Tok::Arrow);
                i += 2;
            }
            '<' if chars.get(i + 1) == Some(&'-') => {
                toks.push(Tok::LArrow);
                i += 2;
            }
            c if is_word_char(c) => {
                let start = i;
                while i < chars.len() && is_word_char(chars[i]) {
                    // A `-` immediately followed by `>` begins an arrow, not part of the word.
                    if chars[i] == '-' && chars.get(i + 1) == Some(&'>') {
                        break;
                    }
                    i += 1;
                }
                toks.push(Tok::Word(chars[start..i].iter().collect()));
            }
            other => return Err(format!("unexpected character '{other}' in IR")),
        }
    }
    Ok(toks)
}

/// Read a double-quoted string starting at `start` (the opening quote). Returns the decoded value
/// and the index just past the closing quote.
fn read_string(chars: &[char], start: usize) -> Result<(String, usize), String> {
    let mut s = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        match chars[i] {
            '"' => return Ok((s, i + 1)),
            '\\' => {
                i += 1;
                match chars.get(i) {
                    Some('"') => s.push('"'),
                    Some('\\') => s.push('\\'),
                    Some('n') => s.push('\n'),
                    Some(other) => return Err(format!("invalid escape '\\{other}' in IR string")),
                    None => break,
                }
                i += 1;
            }
            c => {
                s.push(c);
                i += 1;
            }
        }
    }
    Err("unterminated string literal in IR".to_string())
}

// --- parser ---------------------------------------------------------------------------------

/// Attributes gathered from a node's `{ … }` block before they are lowered into an `ElementKind`
/// and element-level fields.
#[derive(Default)]
struct NodeAttrs {
    /// Scalar `key value` attributes (value already unquoted); keyed by attribute name.
    scalars: HashMap<String, String>,
    inputs: Vec<Mapping>,
    outputs: Vec<Mapping>,
    /// `zeebe:taskHeaders` entries (serviceTask only), keyed by header name.
    headers: BTreeMap<String, String>,
    /// `zeebe:properties` entries (`property "name" "value"` lines), keyed by
    /// property name.
    properties: BTreeMap<String, String>,
    timer: Option<TimerDef>,
    multi_instance: Option<MultiInstance>,
}

impl NodeAttrs {
    fn take(&mut self, key: &str) -> Option<String> {
        self.scalars.remove(key)
    }
    fn require(&mut self, key: &str, node: &str) -> Result<String, String> {
        self.take(key)
            .ok_or_else(|| format!("element '{node}' is missing required attribute '{key}'"))
    }
    fn bool_or(&mut self, key: &str, default: bool) -> Result<bool, String> {
        match self.take(key) {
            None => Ok(default),
            Some(v) => parse_bool(&v, key),
        }
    }
}

fn parse_bool(v: &str, key: &str) -> Result<bool, String> {
    match v {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!(
            "attribute '{key}' expects true/false, got '{other}'"
        )),
    }
}

/// Parse a `<n>ms` duration word into milliseconds.
fn parse_millis(v: &str, key: &str) -> Result<u64, String> {
    let digits = v
        .strip_suffix("ms")
        .ok_or_else(|| format!("attribute '{key}' expects a '<n>ms' duration, got '{v}'"))?;
    digits
        .parse::<u64>()
        .map_err(|_| format!("attribute '{key}' has a non-numeric duration '{v}'"))
}

struct Parser<'a> {
    toks: &'a [Tok],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(toks: &'a [Tok]) -> Self {
        Parser { toks, pos: 0 }
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<&Tok> {
        let t = self.toks.get(self.pos);
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect_word(&mut self, ctx: &str) -> Result<String, String> {
        match self.next() {
            Some(Tok::Word(w)) => Ok(w.clone()),
            other => Err(format!("expected an identifier {ctx}, found {other:?}")),
        }
    }

    fn expect_str(&mut self, ctx: &str) -> Result<String, String> {
        match self.next() {
            Some(Tok::Str(s)) => Ok(s.clone()),
            other => Err(format!("expected a quoted string {ctx}, found {other:?}")),
        }
    }

    fn expect_tok(&mut self, want: &Tok, ctx: &str) -> Result<(), String> {
        match self.next() {
            Some(t) if t == want => Ok(()),
            other => Err(format!("expected {want:?} {ctx}, found {other:?}")),
        }
    }

    fn parse_document(&mut self) -> Result<ParsedIr, String> {
        // Header: process "<id>" {
        let kw = self.expect_word("at the start of the document")?;
        if kw != "process" {
            return Err(format!(
                "expected the document to start with `process`, found `{kw}`"
            ));
        }
        let process_id = self.expect_str("for the process id")?;
        self.expect_tok(&Tok::LBrace, "after the process id")?;

        // start <id>
        let start_kw = self.expect_word("for the `start` declaration")?;
        if start_kw != "start" {
            return Err(format!("expected `start <id>` first, found `{start_kw}`"));
        }
        let start_event = self.expect_word("for the start event id")?;

        let mut elements: HashMap<String, Element> = HashMap::new();
        let mut names: HashMap<String, String> = HashMap::new();
        // Collect flows until every node is known, then attach (so forward references resolve).
        let mut pending_flows: Vec<(String, SequenceFlow)> = Vec::new();

        loop {
            match self.peek() {
                Some(Tok::RBrace) => {
                    self.next();
                    break;
                }
                None => return Err("unexpected end of input: missing closing `}`".to_string()),
                // A statement is a flow iff its second token is `->`; otherwise it is a node
                // statement (so an unrecognised leading word is reported as an unknown kind rather
                // than a malformed flow).
                Some(Tok::Word(_)) if self.toks.get(self.pos + 1) == Some(&Tok::Arrow) => {
                    let (from, flow) = self.parse_flow()?;
                    pending_flows.push((from, flow));
                }
                Some(Tok::Word(_)) => {
                    let (id, name, el) = self.parse_node()?;
                    if elements.contains_key(&id) {
                        return Err(format!("duplicate element id '{id}'"));
                    }
                    if let Some(n) = name {
                        names.insert(id.clone(), n);
                    }
                    elements.insert(id, el);
                }
                other => return Err(format!("unexpected token {other:?} at statement start")),
            }
        }

        // Attach flows to their source elements, validating both endpoints exist.
        for (from, flow) in pending_flows {
            if !elements.contains_key(&flow.to) {
                return Err(format!(
                    "flow from '{from}' targets unknown element '{}'",
                    flow.to
                ));
            }
            let src = elements
                .get_mut(&from)
                .ok_or_else(|| format!("flow references unknown source element '{from}'"))?;
            src.outgoing.push(flow);
        }

        // Structural validation: start event and every `attachedTo`/`parent` reference must exist.
        if !elements.contains_key(&start_event) {
            return Err(format!(
                "start event '{start_event}' is not a declared element"
            ));
        }
        for el in elements.values() {
            if let Some(target) = el.kind.attached_to() {
                if !elements.contains_key(target) {
                    return Err(format!(
                        "boundary event '{}' is attached to unknown element '{target}'",
                        el.id
                    ));
                }
            }
            if let Some(parent) = &el.parent {
                if !elements.contains_key(parent) {
                    return Err(format!(
                        "element '{}' names unknown parent '{parent}'",
                        el.id
                    ));
                }
            }
        }

        Ok(ParsedIr {
            definition: ProcessDefinition {
                id: process_id,
                name: None,
                elements,
                start_event,
                xml: String::new(),
                adhoc: Vec::new(),
                start_form_id: None,
            },
            names,
        })
    }

    /// Parse one node statement: `<keyword> <id> ["<name>"] [{ <attrs> }]`.
    fn parse_node(&mut self) -> Result<(String, Option<String>, Element), String> {
        let keyword = self.expect_word("for a node kind")?;
        let id = self.expect_word("for a node id")?;
        let mut name = None;
        if let Some(Tok::Str(_)) = self.peek() {
            name = Some(self.expect_str("for a node name")?);
        }
        let mut attrs = NodeAttrs::default();
        if let Some(Tok::LBrace) = self.peek() {
            self.parse_attr_block(&mut attrs)?;
        }

        let kind = build_kind(&keyword, &id, &mut attrs)?;
        let parent = attrs.take("parent");
        let retries = attrs.take("retries");
        let element = Element {
            id: id.clone(),
            kind,
            // processos tracks display names in its own `names` map (returned
            // alongside), not on `Element.name`, so leave it unset to keep the
            // XML round-trip identity stable.
            name: None,
            outgoing: Vec::new(),
            parent,
            io: IoMapping {
                inputs: std::mem::take(&mut attrs.inputs),
                outputs: std::mem::take(&mut attrs.outputs),
            },
            timer: attrs.timer.take(),
            retries,
            multi_instance: attrs.multi_instance.take(),
            start_listeners: Vec::new(),
            end_listeners: Vec::new(),
            task_listeners: Vec::new(),
            // `zeebe:properties` extension properties round-trip through the IR
            // as `property "name" "value"` lines (MCP tool metadata lives here,
            // #1499), so an IR parse → BPMN re-emit preserves them.
            properties: std::mem::take(&mut attrs.properties),
        };
        if let Some((leftover, _)) = attrs.scalars.iter().next() {
            return Err(format!(
                "element '{id}' has an unknown attribute '{leftover}'"
            ));
        }
        // `header` statements only feed `attrs.headers`, which is consumed by the
        // `serviceTask` arm. For any other kind the headers are left behind here;
        // surface that as an error instead of silently dropping them (mirrors the
        // unknown-scalar-attribute check above).
        if let Some((leftover, _)) = attrs.headers.iter().next() {
            return Err(format!(
                "element '{id}' declares task header '{leftover}' but only a serviceTask may have headers"
            ));
        }
        Ok((id, name, element))
    }

    /// Parse a `{ … }` attribute block into `attrs`.
    fn parse_attr_block(&mut self, attrs: &mut NodeAttrs) -> Result<(), String> {
        self.expect_tok(&Tok::LBrace, "to open an attribute block")?;
        loop {
            match self.peek() {
                Some(Tok::RBrace) => {
                    self.next();
                    return Ok(());
                }
                None => return Err("unterminated attribute block".to_string()),
                _ => self.parse_attr(attrs)?,
            }
        }
    }

    /// Parse a single attribute line inside a `{ … }` block.
    fn parse_attr(&mut self, attrs: &mut NodeAttrs) -> Result<(), String> {
        let key = self.expect_word("for an attribute name")?;
        match key.as_str() {
            "input" | "output" => {
                let target = self.expect_word("for an io mapping target")?;
                self.expect_tok(&Tok::LArrow, "in an io mapping")?;
                let source = self.expect_str("for an io mapping source")?;
                let mapping = Mapping { source, target };
                if key == "input" {
                    attrs.inputs.push(mapping);
                } else {
                    attrs.outputs.push(mapping);
                }
            }
            "header" => {
                let key = self.expect_str("for a task header key")?;
                self.expect_tok(&Tok::LArrow, "in a task header")?;
                let value = self.expect_str("for a task header value")?;
                attrs.headers.insert(key, value);
            }
            "property" => {
                let name = self.expect_str("for a property name")?;
                let value = self.expect_str("for a property value")?;
                attrs.properties.insert(name, value);
            }
            "timer" => {
                let kind_word = self.expect_word("for a timer kind")?;
                let kind = match kind_word.as_str() {
                    "duration" => TimerDefKind::Duration,
                    "cycle" => TimerDefKind::Cycle,
                    "date" => TimerDefKind::Date,
                    other => return Err(format!("unknown timer kind '{other}'")),
                };
                let expr = self.expect_str("for a timer expression")?;
                attrs.timer = Some(TimerDef { kind, expr });
            }
            "multiInstance" => {
                attrs.multi_instance = Some(self.parse_multi_instance()?);
            }
            // Scalar attributes: `<key> <value>` where the value is a quoted string or a bareword
            // (id, boolean, or `<n>ms` duration).
            _ => {
                let value = match self.next() {
                    Some(Tok::Str(s)) => s.clone(),
                    Some(Tok::Word(w)) => w.clone(),
                    other => {
                        return Err(format!(
                            "attribute '{key}' expects a value, found {other:?}"
                        ))
                    }
                };
                attrs.scalars.insert(key, value);
            }
        }
        Ok(())
    }

    /// Parse `multiInstance { collection "…", inputElement "…", …, sequential <bool> }`.
    fn parse_multi_instance(&mut self) -> Result<MultiInstance, String> {
        self.expect_tok(&Tok::LBrace, "to open a multiInstance block")?;
        let mut mi = MultiInstance::default();
        let mut have_collection = false;
        loop {
            match self.next() {
                Some(Tok::RBrace) => break,
                Some(Tok::Comma) => continue,
                Some(Tok::Word(key)) => {
                    let key = key.clone();
                    match key.as_str() {
                        "sequential" => {
                            let v = self.expect_word("for multiInstance sequential")?;
                            mi.sequential = parse_bool(&v, "sequential")?;
                        }
                        "cancelRemainingInstances" => {
                            let v =
                                self.expect_word("for multiInstance cancelRemainingInstances")?;
                            mi.cancel_remaining_instances =
                                parse_bool(&v, "cancelRemainingInstances")?;
                        }
                        "collection" => {
                            mi.input_collection =
                                self.expect_str("for multiInstance collection")?;
                            have_collection = true;
                        }
                        "inputElement" => {
                            mi.input_element =
                                Some(self.expect_str("for multiInstance inputElement")?);
                        }
                        "outputCollection" => {
                            mi.output_collection =
                                Some(self.expect_str("for multiInstance outputCollection")?);
                        }
                        "outputElement" => {
                            mi.output_element =
                                Some(self.expect_str("for multiInstance outputElement")?);
                        }
                        "completionCondition" => {
                            mi.completion_condition =
                                Some(self.expect_str("for multiInstance completionCondition")?);
                        }
                        other => return Err(format!("unknown multiInstance attribute '{other}'")),
                    }
                }
                other => return Err(format!("unexpected token {other:?} in multiInstance block")),
            }
        }
        if !have_collection {
            return Err("multiInstance is missing required attribute 'collection'".to_string());
        }
        Ok(mi)
    }

    /// Parse one flow statement: `<from> -> <to> [when "<expr>"] [default]`.
    fn parse_flow(&mut self) -> Result<(String, SequenceFlow), String> {
        let from = self.expect_word("for a flow source")?;
        self.expect_tok(&Tok::Arrow, "in a flow statement")?;
        let to = self.expect_word("for a flow target")?;
        let mut condition = None;
        let mut is_default = false;
        loop {
            match self.peek() {
                Some(Tok::Word(w)) if w == "when" => {
                    self.next();
                    let expr = self.expect_str("for a flow condition")?;
                    condition = Some(Condition::new(expr));
                }
                Some(Tok::Word(w)) if w == "default" => {
                    self.next();
                    is_default = true;
                }
                _ => break,
            }
        }
        Ok((
            from,
            SequenceFlow {
                to,
                condition,
                is_default,
            },
        ))
    }
}

/// Lower a node keyword + attribute block into an [`ElementKind`]. Exhaustive over the kind keywords
/// `kind_keyword` emits, so the printer and parser stay in lockstep.
fn build_kind(keyword: &str, id: &str, attrs: &mut NodeAttrs) -> Result<ElementKind, String> {
    let kind = match keyword {
        "startEvent" => ElementKind::StartEvent,
        "endEvent" => ElementKind::EndEvent,
        "terminateEndEvent" => ElementKind::TerminateEndEvent,
        "exclusiveGateway" => ElementKind::ExclusiveGateway,
        "parallelGateway" => ElementKind::ParallelGateway,
        "inclusiveGateway" => ElementKind::InclusiveGateway,
        "eventBasedGateway" => ElementKind::EventBasedGateway,
        "intermediateThrowEvent" => ElementKind::IntermediateThrowEvent,
        "linkIntermediateThrowEvent" => ElementKind::LinkIntermediateThrowEvent {
            link_name: attrs.require("link", id)?,
        },
        "linkIntermediateCatchEvent" => ElementKind::LinkIntermediateCatchEvent {
            link_name: attrs.require("link", id)?,
        },
        "task" => ElementKind::Task,
        "serviceTask" | "agentTask" => {
            let raw = if keyword == "agentTask" {
                Some(attrs.require("agentType", id)?)
            } else {
                attrs.take("agentType")
            };
            let agent_type = raw
                .map(|raw| {
                    nanobpmn_engine_core::AgentType::parse(&raw)
                        .ok_or_else(|| format!("element '{id}': unknown agentType '{raw}'"))
                })
                .transpose()?;
            let job_type = if keyword == "agentTask" {
                attrs.take("jobType").unwrap_or_else(|| id.to_owned())
            } else {
                attrs.require("jobType", id)?
            };
            ElementKind::ServiceTask {
                agent_type,
                job_type,
                priority: attrs.take("priority"),
                custom_headers: std::mem::take(&mut attrs.headers),
                linked_resources: Vec::new(),
            }
        }
        "businessRuleTask" => ElementKind::BusinessRuleTask {
            decision_id: attrs.require("decisionId", id)?,
            result_variable: attrs.take("resultVariable"),
        },
        "userTask" => ElementKind::UserTask(nanobpmn_engine_core::UserTaskProps {
            assignee: attrs.take("assignee"),
            candidate_groups: attrs.take("candidateGroups"),
            candidate_users: attrs.take("candidateUsers"),
            due_date: attrs.take("dueDate"),
            follow_up_date: attrs.take("followUpDate"),
            priority: attrs.take("priority"),
            form_id: attrs.take("formId"),
            external_form_reference: attrs.take("externalFormReference"),
        }),
        "errorBoundaryEvent" => ElementKind::ErrorBoundaryEvent {
            attached_to: attrs.require("attachedTo", id)?,
            error_code: attrs.require("errorCode", id)?,
        },
        "timerIntermediateCatchEvent" => {
            let d = attrs.require("duration", id)?;
            ElementKind::TimerIntermediateCatchEvent {
                duration_millis: parse_millis(&d, "duration")?,
            }
        }
        "timerBoundaryEvent" => {
            let d = attrs.require("duration", id)?;
            ElementKind::TimerBoundaryEvent {
                attached_to: attrs.require("attachedTo", id)?,
                duration_millis: parse_millis(&d, "duration")?,
                interrupting: attrs.bool_or("interrupting", true)?,
                repeating: attrs.bool_or("repeating", false)?,
            }
        }
        "messageIntermediateCatchEvent" => ElementKind::MessageIntermediateCatchEvent {
            message_name: attrs.require("message", id)?,
            correlation_key: attrs.require("correlationKey", id)?,
        },
        "messageBoundaryEvent" => ElementKind::MessageBoundaryEvent {
            attached_to: attrs.require("attachedTo", id)?,
            message_name: attrs.require("message", id)?,
            correlation_key: attrs.require("correlationKey", id)?,
            interrupting: attrs.bool_or("interrupting", true)?,
        },
        "messageStartEvent" => ElementKind::MessageStartEvent {
            message_name: attrs.require("message", id)?,
        },
        "timerStartEvent" => {
            let interval = attrs.require("interval", id)?;
            ElementKind::TimerStartEvent {
                interval_millis: parse_millis(&interval, "interval")?,
                repeating: attrs.bool_or("repeating", false)?,
            }
        }
        "subProcess" => ElementKind::SubProcess {
            start_event: attrs.require("startEvent", id)?,
        },
        "scriptTask" => ElementKind::ScriptTask {
            expression: attrs.require("expression", id)?,
            result_variable: attrs.require("resultVariable", id)?,
        },
        "callActivity" => ElementKind::CallActivity {
            called_process_id: attrs.require("calledElement", id)?,
            propagate_all_parent_variables: attrs.bool_or("propagateAllParentVariables", true)?,
            propagate_all_child_variables: attrs.bool_or("propagateAllChildVariables", true)?,
        },
        "signalIntermediateCatchEvent" => ElementKind::SignalIntermediateCatchEvent {
            signal_name: attrs.require("signal", id)?,
        },
        "signalBoundaryEvent" => ElementKind::SignalBoundaryEvent {
            attached_to: attrs.require("attachedTo", id)?,
            signal_name: attrs.require("signal", id)?,
            interrupting: attrs.bool_or("interrupting", true)?,
        },
        "conditionalIntermediateCatchEvent" => ElementKind::ConditionalIntermediateCatchEvent {
            condition: attrs.require("condition", id)?,
        },
        "conditionalBoundaryEvent" => ElementKind::ConditionalBoundaryEvent {
            attached_to: attrs.require("attachedTo", id)?,
            condition: attrs.require("condition", id)?,
            interrupting: attrs.bool_or("interrupting", true)?,
        },
        "compensationBoundaryEvent" => ElementKind::CompensationBoundaryEvent {
            attached_to: attrs.require("attachedTo", id)?,
            handler: attrs.require("handler", id)?,
        },
        "compensationThrowEvent" => ElementKind::CompensationThrowEvent,
        "escalationThrowEvent" => ElementKind::EscalationThrowEvent {
            escalation_code: attrs.take("escalationCode").unwrap_or_default(),
        },
        "escalationBoundaryEvent" => ElementKind::EscalationBoundaryEvent {
            attached_to: attrs.require("attachedTo", id)?,
            escalation_code: attrs.take("escalationCode").unwrap_or_default(),
            interrupting: attrs.bool_or("interrupting", true)?,
        },
        other => return Err(format!("unknown element kind '{other}'")),
    };
    Ok(kind)
}

// -------------------------------------------------------------------------------------------------
// Test hooks for `crate::ir_spec` parity harness — the private renderers exposed at pub(crate)
// visibility (test-only) so the notation-table parity tests can drive exactly the same code paths
// the real pretty-printer uses. Not part of the public API.
// -------------------------------------------------------------------------------------------------

#[cfg(test)]
pub(crate) fn kind_keyword_for_test(kind: &ElementKind) -> &'static str {
    kind_keyword(kind)
}

#[cfg(test)]
pub(crate) fn render_kind_attrs_for_test(kind: &ElementKind, attrs: &mut Vec<String>) {
    render_kind_attrs(kind, attrs);
}

#[cfg(test)]
pub(crate) fn render_element_attrs_for_test(el: &Element, attrs: &mut Vec<String>) {
    render_element_attrs(el, attrs);
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::ProcessBuilder;

    use super::*;

    const LOAN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="loan-approval" isExecutable="true">
    <bpmn:startEvent id="Start" name="Application received">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="CheckCredit" name="Check credit">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="check-credit" />
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="Decision" name="Route by amount" default="f4">
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
      <bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:endEvent id="Approve" name="Approved">
      <bpmn:incoming>f3</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:endEvent id="Manual" name="Manual review">
      <bpmn:incoming>f4</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="CheckCredit" />
    <bpmn:sequenceFlow id="f2" sourceRef="CheckCredit" targetRef="Decision" />
    <bpmn:sequenceFlow id="f3" sourceRef="Decision" targetRef="Approve">
      <bpmn:conditionExpression>= amount &gt;= 1000</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f4" sourceRef="Decision" targetRef="Manual" />
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn renders_the_loan_model_with_names_default_and_condition() {
        let ir = xml_to_ir(LOAN_BPMN).expect("render loan model");
        // Header + start.
        assert!(ir.contains("process \"loan-approval\" {"), "\n{ir}");
        assert!(ir.contains("  start Start\n"), "\n{ir}");
        // Node statements: kind keyword + id + preserved name + attr block.
        assert!(
            ir.contains("startEvent Start \"Application received\"\n"),
            "\n{ir}"
        );
        assert!(
            ir.contains(
                "serviceTask CheckCredit \"Check credit\" {\n    jobType \"check-credit\"\n  }\n"
            ),
            "\n{ir}"
        );
        assert!(
            ir.contains("exclusiveGateway Decision \"Route by amount\"\n"),
            "\n{ir}"
        );
        // The two constructs the imperative API could not express, now first-class in the IR.
        assert!(
            ir.contains("Decision -> Approve when \"= amount >= 1000\"\n"),
            "\n{ir}"
        );
        assert!(ir.contains("Decision -> Manual default\n"), "\n{ir}");
    }

    #[test]
    fn is_deterministic_across_runs() {
        let a = xml_to_ir(LOAN_BPMN).expect("render");
        let b = xml_to_ir(LOAN_BPMN).expect("render");
        assert_eq!(a, b, "the pretty-printer must be a normal form");
    }

    #[test]
    fn preserves_gateway_outgoing_declaration_order() {
        // An exclusive gateway takes the FIRST matching outgoing flow, so declaration order is
        // load-bearing. Here the conditional flow ("Zeta") is declared before the default ("Alpha")
        // even though sorting by target id would put "Alpha" first — the IR must keep Zeta first.
        let def = ProcessBuilder::new("Priority")
            .start_event("Start")
            .exclusive_gateway("Gate")
            .end_event("Zeta")
            .end_event("Alpha")
            .connect("Start", "Gate")
            .connect_when("Gate", "Zeta", "= amount > 100")
            .connect_default("Gate", "Alpha")
            .build()
            .unwrap();
        let ir = definition_to_ir(&def, &HashMap::new());
        let zeta = ir.find("Gate -> Zeta").expect("zeta flow present");
        let alpha = ir.find("Gate -> Alpha").expect("alpha flow present");
        assert!(
            zeta < alpha,
            "declared order (conditional before default) must survive; got:\n{ir}"
        );
        assert!(
            ir.contains("Gate -> Zeta when \"= amount > 100\"\n"),
            "\n{ir}"
        );
        assert!(ir.contains("Gate -> Alpha default\n"), "\n{ir}");
    }

    #[test]
    fn agent_metadata_ir_round_trips_as_canonical_service_task() {
        for marker in ["aiAgentTask", "external", "aiAgentSubProcess"] {
            let ir = format!(
                "process \"p\" {{\n start s\n startEvent s\n serviceTask a {{\n\
                 jobType \"= workerType\"\n agentType \"{marker}\"\n priority \"= urgency\"\n\
                 header \"model\" <- \"gpt\"\n retries \"7\"\n }}\n endEvent e\n s -> a\n a -> e\n}}\n"
            );
            let parsed = ir_to_definition(&ir).expect("agent metadata parses");
            let canonical = definition_to_ir(&parsed.definition, &parsed.names);
            assert!(canonical.contains("serviceTask a"));
            assert!(!canonical.contains("agentTask a"));
            assert!(canonical.contains(&format!("agentType \"{marker}\"")));
            assert!(canonical.contains("header \"model\" <- \"gpt\""));
            let roundtrip = ir_to_definition(&canonical).unwrap();
            assert_eq!(roundtrip.definition.elements, parsed.definition.elements);
            if marker == "aiAgentSubProcess" {
                // The compact IR retains the marker, not an ad-hoc container's tool catalog.
                continue;
            }
            let xml =
                crate::bpmn_model::definition_to_xml_labeled(&parsed.definition, &parsed.names);
            assert!(xml.contains("<bpmndi:BPMNDiagram"));
            let reparsed = parse_bpmn(&xml).unwrap();
            assert_eq!(definition_to_ir(&reparsed[0], &HashMap::new()), canonical);
        }
    }

    #[test]
    fn legacy_agent_task_ir_lowers_to_service_task() {
        for job_type in [Some("worker"), None] {
            let job_attr = job_type
                .map(|v| format!("jobType \"{v}\""))
                .unwrap_or_default();
            let ir = format!(
                "process \"p\" {{\n start s\n startEvent s\n agentTask a {{\n\
                 agentType \"external\"\n {job_attr}\n }}\n endEvent e\n s -> a\n a -> e\n}}\n"
            );
            let parsed = ir_to_definition(&ir).expect("legacy alias parses");
            assert!(matches!(
                &parsed.definition.elements["a"].kind,
                ElementKind::ServiceTask { job_type: actual, agent_type: Some(_), .. }
                    if actual == job_type.unwrap_or("a")
            ));
            let canonical = definition_to_ir(&parsed.definition, &parsed.names);
            assert!(canonical.contains("serviceTask a"));
            assert!(!canonical.contains("agentTask a"));
        }
    }

    #[test]
    fn agent_metadata_ir_rejects_unknown_marker() {
        let ir = "process \"p\" {\n start s\n startEvent s\n serviceTask a {\n\
                  jobType \"worker\"\n agentType \"unknown\"\n }\n s -> a\n}\n";
        assert!(ir_to_definition(ir)
            .unwrap_err()
            .contains("unknown agentType"));
    }

    #[test]
    fn legacy_agent_model_emits_canonical_ir_and_xml() {
        use nanobpmn_engine_core::{AgentDefinition, AgentType};

        for marker in [
            AgentType::AiAgentTask,
            AgentType::External,
            AgentType::AiAgentSubProcess,
        ] {
            for job_type in [Some("= workerType".to_owned()), None] {
                let mut def = ProcessBuilder::new("Legacy")
                    .start_event("Start")
                    .service_task("Agent", "unused")
                    .end_event("End")
                    .connect("Start", "Agent")
                    .connect("Agent", "End")
                    .build()
                    .unwrap();
                def.elements.get_mut("Agent").unwrap().kind = ElementKind::AgentTask {
                    agent_type: marker,
                    job_type,
                    definition: AgentDefinition::default(),
                    limits: None,
                };
                let ir = definition_to_ir(&def, &HashMap::new());
                let xml = crate::bpmn_model::definition_to_xml_labeled(&def, &HashMap::new());
                assert!(ir.contains("serviceTask Agent"));
                assert!(!ir.contains("agentTask Agent"));
                assert!(xml.contains("<zeebe:taskDefinition"));
                let parsed = ir_to_definition(&ir).unwrap();
                def.normalize_legacy_agent_tasks();
                assert_eq!(parsed.definition.elements, def.elements);
            }
        }
    }

    #[test]
    fn nodes_sorted_by_id_flows_after_nodes() {
        let ir = xml_to_ir(LOAN_BPMN).expect("render");
        // Node ids appear in sorted order.
        let pos = |needle: &str| ir.find(needle).unwrap_or(usize::MAX);
        assert!(pos("\n  serviceTask CheckCredit") < pos("\n  exclusiveGateway Decision"));
        assert!(pos("\n  exclusiveGateway Decision") < pos("\n  startEvent Start"));
        // All flow statements come after all node statements.
        assert!(pos("startEvent Start") < pos("  Decision -> Approve"));
    }

    #[test]
    fn renders_io_mappings_and_retries() {
        // A service task carrying io mappings + a retries expression exercises the element-level
        // annotation channel.
        let def = ProcessBuilder::new("Mapped")
            .start_event("Start")
            .service_task("Work", "do-work")
            .end_event("Done")
            .connect("Start", "Work")
            .connect("Work", "Done")
            .build()
            .unwrap();
        let mut def = def;
        let work = def.elements.get_mut("Work").unwrap();
        work.io.inputs.push(nanobpmn_engine_core::Mapping {
            source: "=order.amount".into(),
            target: "amount".into(),
        });
        work.io.outputs.push(nanobpmn_engine_core::Mapping {
            source: "=result.ok".into(),
            target: "approved".into(),
        });
        work.retries = Some("5".into());
        let ir = definition_to_ir(&def, &HashMap::new());
        assert!(ir.contains("input amount <- \"=order.amount\""), "\n{ir}");
        assert!(ir.contains("output approved <- \"=result.ok\""), "\n{ir}");
        assert!(ir.contains("retries \"5\""), "\n{ir}");
    }

    #[test]
    fn renders_an_error_boundary_attached_to_a_task() {
        let def = ProcessBuilder::new("Boundaried")
            .start_event("Start")
            .service_task("Charge", "charge-card")
            .error_boundary_event("OnDecline", "Charge", "CARD_DECLINED")
            .end_event("Done")
            .end_event("Declined")
            .connect("Start", "Charge")
            .connect("Charge", "Done")
            .connect("OnDecline", "Declined")
            .build()
            .unwrap();
        let ir = definition_to_ir(&def, &HashMap::new());
        assert!(
            ir.contains("errorBoundaryEvent OnDecline {\n    attachedTo Charge\n    errorCode \"CARD_DECLINED\"\n  }\n"),
            "\n{ir}"
        );
        // A boundary event has no incoming flow but does have an outgoing one.
        assert!(ir.contains("OnDecline -> Declined\n"), "\n{ir}");
    }

    #[test]
    fn escapes_quotes_in_names() {
        let def = ProcessBuilder::new("Q")
            .start_event("Start")
            .end_event("Done")
            .connect("Start", "Done")
            .build()
            .unwrap();
        let mut names = HashMap::new();
        names.insert("Start".to_string(), "the \"real\" start".to_string());
        let ir = definition_to_ir(&def, &names);
        assert!(
            ir.contains("startEvent Start \"the \\\"real\\\" start\"\n"),
            "\n{ir}"
        );
    }

    // --- Phase 2: parser + round-trip -----------------------------------------------------

    #[test]
    fn round_trips_the_loan_model_through_emit_parse_emit() {
        // The reversibility guarantee at unit level: emit(parse(emit(model))) == emit(model).
        let ir1 = xml_to_ir(LOAN_BPMN).expect("emit");
        let parsed = ir_to_definition(&ir1).expect("parse");
        let ir2 = definition_to_ir(&parsed.definition, &parsed.names);
        assert_eq!(ir1, ir2, "IR must survive a parse/emit round-trip");
    }

    #[test]
    fn parses_structure_default_condition_and_names() {
        let ir = xml_to_ir(LOAN_BPMN).expect("emit");
        let parsed = ir_to_definition(&ir).expect("parse");
        let def = &parsed.definition;
        assert_eq!(def.id, "loan-approval");
        assert_eq!(def.start_event, "Start");
        assert_eq!(
            parsed.names.get("Start").map(String::as_str),
            Some("Application received")
        );
        // The service task's job type survived.
        match &def.elements["CheckCredit"].kind {
            ElementKind::ServiceTask { job_type, .. } => assert_eq!(job_type, "check-credit"),
            other => panic!("expected serviceTask, got {other:?}"),
        }
        // The gateway's default + conditional flows parsed with the right semantics.
        let gate = &def.elements["Decision"];
        let approve = gate.outgoing.iter().find(|f| f.to == "Approve").unwrap();
        let manual = gate.outgoing.iter().find(|f| f.to == "Manual").unwrap();
        assert_eq!(
            approve.condition.as_ref().map(|c| c.expression.as_str()),
            Some("= amount >= 1000")
        );
        assert!(!approve.is_default);
        assert!(manual.is_default);
        assert!(manual.condition.is_none());
    }

    #[test]
    fn round_trips_a_synthetic_model_with_boundary_io_and_mi() {
        // A model exercising element-level annotations (io, retries, multi-instance) and a boundary
        // event, built directly and round-tripped emit -> parse -> emit.
        let mut def = ProcessBuilder::new("Rich")
            .start_event("Start")
            .service_task("Work", "do-work")
            .error_boundary_event("OnFail", "Work", "BOOM")
            .end_event("Done")
            .end_event("Failed")
            .connect("Start", "Work")
            .connect("Work", "Done")
            .connect("OnFail", "Failed")
            .build()
            .unwrap();
        {
            let work = def.elements.get_mut("Work").unwrap();
            work.retries = Some("5".into());
            work.io.inputs.push(Mapping {
                source: "=order.items".into(),
                target: "items".into(),
            });
            work.multi_instance = Some(MultiInstance {
                input_collection: "=items".into(),
                input_element: Some("item".into()),
                output_collection: Some("results".into()),
                output_element: Some("=out".into()),
                completion_condition: None,
                cancel_remaining_instances: true,
                sequential: true,
            });
        }
        let ir1 = definition_to_ir(&def, &HashMap::new());
        let parsed = ir_to_definition(&ir1).expect("parse");
        let ir2 = definition_to_ir(&parsed.definition, &parsed.names);
        assert_eq!(ir1, ir2, "annotations must survive the round-trip:\n{ir1}");
        // Spot-check the reconstructed model rather than only its text.
        let work = &parsed.definition.elements["Work"];
        assert_eq!(work.retries.as_deref(), Some("5"));
        assert_eq!(work.io.inputs.len(), 1);
        let mi = work.multi_instance.as_ref().unwrap();
        assert_eq!(mi.input_collection, "=items");
        assert!(mi.sequential);
        assert!(mi.cancel_remaining_instances);
        match &parsed.definition.elements["OnFail"].kind {
            ElementKind::ErrorBoundaryEvent {
                attached_to,
                error_code,
            } => {
                assert_eq!(attached_to, "Work");
                assert_eq!(error_code, "BOOM");
            }
            other => panic!("expected error boundary, got {other:?}"),
        }
    }

    #[test]
    fn preserves_gateway_flow_order_through_parse() {
        let def = ProcessBuilder::new("Priority")
            .start_event("Start")
            .exclusive_gateway("Gate")
            .end_event("Zeta")
            .end_event("Alpha")
            .connect("Start", "Gate")
            .connect_when("Gate", "Zeta", "= amount > 100")
            .connect_default("Gate", "Alpha")
            .build()
            .unwrap();
        let ir = definition_to_ir(&def, &HashMap::new());
        let parsed = ir_to_definition(&ir).expect("parse");
        let gate = &parsed.definition.elements["Gate"];
        // First declared flow (the conditional) must remain first after parsing.
        assert_eq!(gate.outgoing[0].to, "Zeta");
        assert_eq!(gate.outgoing[1].to, "Alpha");
        assert!(gate.outgoing[1].is_default);
    }

    #[test]
    fn parser_is_total_and_reports_clear_errors() {
        // Unknown kind.
        let e = ir_to_definition("process \"p\" {\n start S\n frobnicate S\n}").unwrap_err();
        assert!(e.contains("unknown element kind 'frobnicate'"), "{e}");
        // Missing required attribute.
        let e = ir_to_definition("process \"p\" {\n start S\n startEvent S\n serviceTask T\n}")
            .unwrap_err();
        assert!(e.contains("missing required attribute 'jobType'"), "{e}");
        // Dangling flow target.
        let e = ir_to_definition("process \"p\" {\n start S\n startEvent S\n S -> Ghost\n}")
            .unwrap_err();
        assert!(e.contains("unknown element 'Ghost'"), "{e}");
        // Start event not declared.
        let e = ir_to_definition("process \"p\" {\n start Missing\n endEvent E\n}").unwrap_err();
        assert!(e.contains("start event 'Missing'"), "{e}");
        // Unterminated string.
        let e = ir_to_definition("process \"p").unwrap_err();
        assert!(e.contains("unterminated string"), "{e}");
        // Task headers on a non-serviceTask kind are rejected, not silently dropped.
        let e = ir_to_definition(
            "process \"p\" {\n start S\n startEvent S { header \"k\" <- \"v\" }\n}",
        )
        .unwrap_err();
        assert!(e.contains("only a serviceTask may have headers"), "{e}");
    }

    #[test]
    fn analyze_ir_surfaces_semantic_warnings() {
        // A gateway with two conditional flows and no default should trip the existing
        // `gateway-no-default` advisory, proving analyze_model is reused over the parsed IR.
        let ir = "process \"p\" {\n  start S\n  startEvent S\n  exclusiveGateway G\n  \
                  endEvent A\n  endEvent B\n  S -> G\n  G -> A when \"= x > 1\"\n  \
                  G -> B when \"= x <= 1\"\n}";
        let analysis = analyze_ir(ir).expect("analyze");
        let text = analysis.to_string();
        assert!(
            text.contains("default"),
            "expected a no-default advisory, got: {text}"
        );
    }

    /// Phase 3 (ADR 0001): corpus replay property test. For every model in the recorded customer
    /// corpus, the reversible pipeline `fromXml → emit → parse → toXml` must preserve the executable
    /// `ProcessDefinition`. Structural equality of the executable core (id, start event, and every
    /// element with its outgoing-flow order) is strictly stronger than "`simulate()` outcomes match":
    /// an identical model simulates identically for all inputs. We assert two closures:
    ///   1. IR round-trip: `def → definition_to_ir → ir_to_definition` reproduces `def`.
    ///   2. Full pipeline: additionally `→ definition_to_xml_labeled → parse_bpmn` reproduces `def`.
    ///
    /// A corpus-size floor guards against the test silently passing if the corpus dir moves/empties.
    #[test]
    fn corpus_models_survive_the_ir_roundtrip() {
        use std::path::PathBuf;
        // processos tracks display names in a side `names` map, not on
        // `Element.name` (which engine-core's parser now populates from `name=`);
        // compare the structural model with that display-only field cleared.
        fn strip_names(
            els: &std::collections::HashMap<String, Element>,
        ) -> std::collections::HashMap<String, Element> {
            let mut m = els.clone();
            for el in m.values_mut() {
                el.name = None;
            }
            m
        }
        fn collect(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
            for e in std::fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    collect(&p, out);
                } else if p.extension().map(|x| x == "bpmn").unwrap_or(false) {
                    out.push(p);
                }
            }
        }
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus-packs");
        let mut files = Vec::new();
        collect(&root, &mut files);
        files.sort();

        let mut checked = 0usize;
        for f in &files {
            let name = f.strip_prefix(&root).unwrap_or(f).display().to_string();
            let xml = std::fs::read_to_string(f)
                .unwrap_or_else(|e| panic!("corpus model {name} failed to read: {e}"));
            let defs = parse_bpmn(&xml)
                .unwrap_or_else(|e| panic!("corpus model {name} failed to parse: {e:?}"));
            let names = crate::bpmn_model::parse_element_names(&xml);
            for def in &defs {
                // 1. IR round-trip closure.
                let ir = definition_to_ir(def, &names);
                let parsed = ir_to_definition(&ir).unwrap_or_else(|e| {
                    panic!(
                        "IR for {name} [{}] failed to parse back:\n{e}\n--- IR ---\n{ir}",
                        def.id
                    )
                });
                assert_eq!(
                    parsed.definition.id, def.id,
                    "IR round-trip changed process id for {name}"
                );
                assert_eq!(
                    parsed.definition.start_event, def.start_event,
                    "IR round-trip changed start event for {name} [{}]",
                    def.id
                );
                assert_eq!(
                    strip_names(&parsed.definition.elements),
                    strip_names(&def.elements),
                    "IR round-trip changed elements for {name} [{}]",
                    def.id
                );

                // 2. Full ADR pipeline closure: fromXml -> emit -> parse -> toXml -> fromXml.
                let xml2 =
                    crate::bpmn_model::definition_to_xml_labeled(&parsed.definition, &parsed.names);
                let round = parse_bpmn(&xml2).unwrap_or_else(|e| {
                    panic!(
                        "re-emitted XML for {name} [{}] failed to parse: {e:?}",
                        def.id
                    )
                });
                let r = round.first().unwrap_or_else(|| {
                    panic!("re-emitted XML for {name} [{}] had no process", def.id)
                });
                assert_eq!(r.id, def.id, "full pipeline changed process id for {name}");
                assert_eq!(
                    r.start_event, def.start_event,
                    "full pipeline changed start event for {name} [{}]",
                    def.id
                );
                assert_eq!(
                    strip_names(&r.elements),
                    strip_names(&def.elements),
                    "full pipeline changed elements for {name} [{}]",
                    def.id
                );
                checked += 1;
            }
        }
        assert!(
            checked >= 11,
            "expected at least 11 corpus models, only checked {checked} — has corpus-packs moved?"
        );
    }

    #[test]
    fn read_model_ir_emits_the_current_model_ir() {
        let v = read_model_ir(LOAN_BPMN, None).expect("read ir");
        assert_eq!(v["scope"], "full");
        let ir = v["ir"].as_str().expect("ir string");
        assert!(ir.contains("process \"loan-approval\" {"), "\n{ir}");
        assert!(ir.contains("Decision -> Manual default\n"), "\n{ir}");
    }

    #[test]
    fn write_model_ir_compiles_ir_to_a_deployable_model() {
        // Round-trip through the tools: read -> write reproduces a deployable, engine-valid model.
        let ir = xml_to_ir(LOAN_BPMN).expect("emit ir");
        let v = write_model_ir(&ir, None, None, None).expect("write ir");
        assert_eq!(v["ok"], true);
        assert_eq!(v["process"], "loan-approval");
        let model = v["model"].as_str().expect("model xml");
        let reparsed = parse_bpmn(model).expect("written model parses");
        let orig = parse_bpmn(LOAN_BPMN).expect("orig parses");
        assert_eq!(
            reparsed[0].elements, orig[0].elements,
            "write_model_ir preserves the executable model"
        );
        // The finding surface is present (analyze_model ran on the written model).
        assert!(v["findings"].is_array());
    }

    #[test]
    fn write_model_ir_can_add_a_gateway_default_flow() {
        // THE motivating case: the imperative edit_model verbs cannot switch a gateway branch to the
        // `default` fallback, but the IR can. Start from a gateway whose second branch is a plain
        // conditional flow, replace its guard with `default` in the IR text, write it back, and
        // confirm the compiled model flags that branch as the exclusive gateway's default.
        let ir = "process \"router\" {\n  \
             start Start\n  \
             endEvent Hit\n  \
             endEvent Miss\n  \
             exclusiveGateway Gate\n  \
             startEvent Start\n  \
             Start -> Gate\n  \
             Gate -> Hit when \"= score >= 700\"\n  \
             Gate -> Miss when \"= score < 700\"\n}";
        // Sanity: with a guard rather than `default`, the Miss branch is a plain conditional flow.
        let before = write_model_ir(ir, None, None, None).expect("write baseline");
        let before_model = before["model"].as_str().unwrap();
        let before_def = parse_bpmn(before_model).expect("parse baseline");
        assert!(
            !before_def[0].elements["Gate"]
                .outgoing
                .iter()
                .any(|f| f.is_default),
            "baseline has no default flow"
        );
        // Now the edit an LLM would make: switch the Miss branch's guard to ` default`.
        let edited = ir.replace(
            "Gate -> Miss when \"= score < 700\"\n",
            "Gate -> Miss default\n",
        );
        let after = write_model_ir(&edited, None, None, None).expect("write with default");
        let after_model = after["model"].as_str().unwrap();
        let after_def = parse_bpmn(after_model).expect("parse edited");
        let gate = &after_def[0].elements["Gate"];
        let default = gate
            .outgoing
            .iter()
            .find(|f| f.is_default)
            .expect("the edited model now has a default flow");
        assert_eq!(default.to, "Miss", "Miss is the default branch");
    }

    #[test]
    fn write_model_ir_reports_a_clear_error_on_a_dangling_flow() {
        // A flow to an undeclared node must fail validation with a readable error, not a panic.
        let ir = "process \"p\" {\n  start S\n  startEvent S\n  S -> Nope\n}";
        let err = write_model_ir(ir, None, None, None).unwrap_err();
        assert!(
            err.to_lowercase().contains("nope") || err.to_lowercase().contains("declared"),
            "error should name the dangling target: {err}"
        );
    }

    #[test]
    fn write_model_ir_splices_a_phase_into_a_multi_stage_model() {
        // A multi-stage model: an orchestrator whose callActivity invokes a phase process.
        // read_model_ir(process) reads the phase's IR; editing it and writing back with base +
        // process must splice it in place while preserving the orchestrator definition.
        let base = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="Orchestrator" isExecutable="true">
    <bpmn:startEvent id="OStart"><bpmn:outgoing>o1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:callActivity id="RunPhase" name="Run phase">
      <bpmn:extensionElements><zeebe:calledElement processId="Phase" /></bpmn:extensionElements>
      <bpmn:incoming>o1</bpmn:incoming><bpmn:outgoing>o2</bpmn:outgoing>
    </bpmn:callActivity>
    <bpmn:endEvent id="ODone"><bpmn:incoming>o2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="o1" sourceRef="OStart" targetRef="RunPhase" />
    <bpmn:sequenceFlow id="o2" sourceRef="RunPhase" targetRef="ODone" />
  </bpmn:process>
  <bpmn:process id="Phase" isExecutable="true">
    <bpmn:startEvent id="PStart"><bpmn:outgoing>p1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="Work">
      <bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements>
      <bpmn:incoming>p1</bpmn:incoming><bpmn:outgoing>p2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="PDone"><bpmn:incoming>p2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="p1" sourceRef="PStart" targetRef="Work" />
    <bpmn:sequenceFlow id="p2" sourceRef="Work" targetRef="PDone" />
  </bpmn:process>
</bpmn:definitions>"#;
        // Read the phase's IR (targeting by the callActivity node id resolves to its calledElement).
        let read = read_model_ir(base, Some("RunPhase")).expect("read phase ir");
        assert_eq!(read["processId"], "Phase");
        let phase_ir = read["ir"].as_str().expect("phase ir");
        assert!(phase_ir.contains("process \"Phase\" {"), "\n{phase_ir}");
        // Edit the phase: change the job type, then splice it back.
        let edited = phase_ir.replace("\"work\"", "\"work-v2\"");
        let v =
            write_model_ir(&edited, Some(base), Some("Phase"), Some(base)).expect("splice write");
        assert_eq!(v["process"], "Phase");
        let model = v["model"].as_str().expect("model");
        let defs = parse_bpmn(model).expect("spliced model parses");
        // Both definitions survive, and the phase's job type is updated.
        assert!(
            defs.iter().any(|d| d.id == "Orchestrator"),
            "orchestrator preserved"
        );
        let phase = defs
            .iter()
            .find(|d| d.id == "Phase")
            .expect("phase present");
        match &phase.elements["Work"].kind {
            ElementKind::ServiceTask { job_type, .. } => assert_eq!(job_type, "work-v2"),
            other => panic!("Work should stay a service task, got {other:?}"),
        }
    }

    #[test]
    fn write_model_ir_analyzes_the_edited_phase_not_the_orchestrator() {
        // In a splice, findings/metrics must describe the edited phase, not the first (orchestrator)
        // definition analyze_model would otherwise pick. Give the phase an unguarded-service-task
        // smell the orchestrator lacks, and confirm the finding surfaces against the phase's node.
        let base = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="Orchestrator" isExecutable="true">
    <bpmn:startEvent id="OStart"><bpmn:outgoing>o1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:callActivity id="RunPhase"><bpmn:extensionElements><zeebe:calledElement processId="Phase" /></bpmn:extensionElements><bpmn:incoming>o1</bpmn:incoming><bpmn:outgoing>o2</bpmn:outgoing></bpmn:callActivity>
    <bpmn:endEvent id="ODone"><bpmn:incoming>o2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="o1" sourceRef="OStart" targetRef="RunPhase" />
    <bpmn:sequenceFlow id="o2" sourceRef="RunPhase" targetRef="ODone" />
  </bpmn:process>
  <bpmn:process id="Phase" isExecutable="true">
    <bpmn:startEvent id="PStart"><bpmn:outgoing>p1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="Work"><bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements><bpmn:incoming>p1</bpmn:incoming><bpmn:outgoing>p2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:endEvent id="PDone"><bpmn:incoming>p2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="p1" sourceRef="PStart" targetRef="Work" />
    <bpmn:sequenceFlow id="p2" sourceRef="Work" targetRef="PDone" />
  </bpmn:process>
</bpmn:definitions>"#;
        let phase_ir = read_model_ir(base, Some("Phase")).expect("read phase")["ir"]
            .as_str()
            .expect("ir")
            .to_string();
        // Add an unguarded exclusive gateway inside the phase (a node the orchestrator lacks).
        let edited = phase_ir
            .replace(
                "serviceTask Work {\n    jobType \"work\"\n  }\n",
                "serviceTask Work {\n    jobType \"work\"\n  }\n  exclusiveGateway Fork\n",
            )
            .replace("Work -> PDone\n", "Work -> Fork\n  Fork -> PDone\n");
        let v = write_model_ir(&edited, Some(base), Some("Phase"), Some(base)).expect("write");
        let findings = v["findings"].as_array().expect("findings array");
        // The finding references a phase node id (Fork/Work/PStart…), never an orchestrator id.
        let mentions_orchestrator = findings.iter().any(|f| {
            matches!(f.get("element").and_then(|e| e.as_str()), Some(id) if
                id == "RunPhase" || id == "OStart" || id == "ODone")
        });
        assert!(
            !mentions_orchestrator,
            "findings must describe the edited phase, not the orchestrator: {findings:#?}"
        );
    }

    #[test]
    fn write_model_ir_rejects_a_phase_id_rename_during_splice() {
        // The IR header id must match the phase being replaced; a rename would orphan the
        // orchestrator's callActivity calledElement. It must fail fast, not silently mis-splice.
        let base = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="Orchestrator" isExecutable="true">
    <bpmn:startEvent id="OStart"><bpmn:outgoing>o1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:endEvent id="ODone"><bpmn:incoming>o1</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="o1" sourceRef="OStart" targetRef="ODone" />
  </bpmn:process>
  <bpmn:process id="Phase" isExecutable="true">
    <bpmn:startEvent id="PStart"><bpmn:outgoing>p1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:endEvent id="PDone"><bpmn:incoming>p1</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="p1" sourceRef="PStart" targetRef="PDone" />
  </bpmn:process>
</bpmn:definitions>"#;
        let renamed = "process \"Phase-RENAMED\" {\n  start PStart\n  startEvent PStart\n  \
                       endEvent PDone\n  PStart -> PDone\n}";
        let err = write_model_ir(renamed, Some(base), Some("Phase"), Some(base)).unwrap_err();
        assert!(
            err.contains("does not match") && err.contains("Phase"),
            "error should explain the id-rename mismatch: {err}"
        );
    }

    #[test]
    fn write_model_ir_rejects_a_process_arg_without_a_base() {
        // `process` is meaningless without a base to splice into; a mismatched value must fail fast
        // rather than silently write a standalone model that ignores the caller's intent.
        let ir = "process \"router\" {\n  start S\n  startEvent S\n  endEvent E\n  S -> E\n}";
        let err = write_model_ir(ir, None, Some("some-other-phase"), None).unwrap_err();
        assert!(
            err.contains("no `base`") || err.contains("standalone"),
            "error should explain base is required for splicing: {err}"
        );
        // Matching the IR's own id is harmless and still writes standalone.
        write_model_ir(ir, None, Some("router"), None).expect("matching id writes standalone");
    }

    /// A hand-laid diagram with deliberately non-generatable coordinates and custom flow ids, so a
    /// preserved round-trip is observable (auto-layout would never reproduce these exact bounds).
    const HAND_LAID_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI">
  <bpmn:process id="router" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f_sg</bpmn:outgoing></bpmn:startEvent>
    <bpmn:exclusiveGateway id="Gate"><bpmn:incoming>f_sg</bpmn:incoming><bpmn:outgoing>f_hit</bpmn:outgoing><bpmn:outgoing>f_miss</bpmn:outgoing></bpmn:exclusiveGateway>
    <bpmn:endEvent id="Hit"><bpmn:incoming>f_hit</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="Miss"><bpmn:incoming>f_miss</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f_sg" sourceRef="Start" targetRef="Gate" />
    <bpmn:sequenceFlow id="f_hit" sourceRef="Gate" targetRef="Hit"><bpmn:conditionExpression>= score &gt;= 700</bpmn:conditionExpression></bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f_miss" sourceRef="Gate" targetRef="Miss"><bpmn:conditionExpression>= score &lt; 700</bpmn:conditionExpression></bpmn:sequenceFlow>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="Diag">
    <bpmndi:BPMNPlane id="Plane" bpmnElement="router">
      <bpmndi:BPMNShape id="Start_di" bpmnElement="Start"><dc:Bounds x="152" y="82" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gate_di" bpmnElement="Gate" isMarkerVisible="true"><dc:Bounds x="251" y="73" width="50" height="50" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Hit_di" bpmnElement="Hit"><dc:Bounds x="403" y="41" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Miss_di" bpmnElement="Miss"><dc:Bounds x="403" y="121" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f_sg_di" bpmnElement="f_sg"><di:waypoint x="188" y="100" /><di:waypoint x="251" y="98" /></bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f_hit_di" bpmnElement="f_hit"><di:waypoint x="276" y="73" /><di:waypoint x="276" y="59" /><di:waypoint x="403" y="59" /></bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f_miss_di" bpmnElement="f_miss"><di:waypoint x="276" y="123" /><di:waypoint x="276" y="139" /><di:waypoint x="403" y="139" /></bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>"#;

    #[test]
    fn write_model_ir_preserves_hand_layout_on_an_unchanged_topology_edit() {
        // ADR 0001 Phase 5: an attribute-only edit (switch the gateway's fallback to a default flow)
        // must NOT re-lay the customer's diagram. Read -> edit the IR -> write with the original as
        // the DI source, and confirm the exact hand-laid bounds AND the original flow ids survive
        // verbatim.
        let ir = read_model_ir(HAND_LAID_BPMN, None).expect("read ir")["ir"]
            .as_str()
            .expect("ir string")
            .to_string();
        let edited = ir.replace(
            "Gate -> Miss when \"= score < 700\"\n",
            "Gate -> Miss default\n",
        );
        assert_ne!(edited, ir, "the edit must actually change the IR");
        let v = write_model_ir(&edited, None, None, Some(HAND_LAID_BPMN)).expect("write");
        let model = v["model"].as_str().expect("model xml");

        // The exact hand-laid coordinates and the marker flag survive byte-for-byte.
        assert!(
            model.contains(r#"bpmnElement="Gate" isMarkerVisible="true""#),
            "gateway shape preserved verbatim:\n{model}"
        );
        assert!(
            model.contains(r#"<dc:Bounds x="251" y="73" width="50" height="50" />"#),
            "hand-laid gateway bounds preserved:\n{model}"
        );
        assert!(
            model.contains(r#"<dc:Bounds x="403" y="121" width="36" height="36" />"#),
            "hand-laid Miss bounds preserved:\n{model}"
        );
        // Original flow ids are reused (not re-synthesized as Flow_N), so the preserved DI edges
        // still resolve and the gateway's default references the original id.
        assert!(
            model.contains(r#"id="f_sg""#),
            "original flow id reused:\n{model}"
        );
        assert!(
            model.contains(r#"default="f_miss""#),
            "gateway default references the reused flow id:\n{model}"
        );
        assert!(
            !model.contains("Flow_1"),
            "no synthesized flow ids on the preserved path:\n{model}"
        );
        // The edit still took: the compiled model flags Miss as the gateway default.
        let def = parse_bpmn(model).expect("written model parses");
        let default = def[0].elements["Gate"]
            .outgoing
            .iter()
            .find(|f| f.is_default)
            .expect("Miss is now the default");
        assert_eq!(default.to, "Miss");
    }

    #[test]
    fn write_model_ir_auto_lays_out_when_the_topology_changes() {
        // Adding a node changes the topology, so the hand layout can no longer be re-attached
        // verbatim (it would dangle / miss shapes). The serializer falls back to a clean auto-layout
        // — synthesized flow ids and a freshly generated diagram — and the model still parses.
        let ir = read_model_ir(HAND_LAID_BPMN, None).expect("read ir")["ir"]
            .as_str()
            .expect("ir string")
            .to_string();
        // Reroute Miss through a new task, adding a node + flow. The new gateway fallback (to the
        // task) becomes the default so the diverging gateway stays valid.
        let edited = ir
            .replace(
                "Gate -> Miss when \"= score < 700\"\n",
                "Gate -> Extra default\n  Extra -> Miss\n",
            )
            .replace(
                "exclusiveGateway Gate\n",
                "exclusiveGateway Gate\n  serviceTask Extra {\n    jobType \"extra\"\n  }\n",
            );
        let v = write_model_ir(&edited, None, None, Some(HAND_LAID_BPMN)).expect("write");
        let model = v["model"].as_str().expect("model xml");
        // Fell back to auto-layout: the original hand-laid ids and bounds are gone.
        assert!(
            !model.contains(r#"id="f_sg""#),
            "topology change must not reuse original flow ids:\n{model}"
        );
        assert!(
            model.contains("Flow_1"),
            "auto-layout synthesizes Flow_N ids:\n{model}"
        );
        // The new node is present and the model deploys.
        let def = parse_bpmn(model).expect("auto-laid model parses");
        assert!(def[0].elements.contains_key("Extra"), "new node present");
    }
}

#[cfg(test)]
mod cancel_remaining_instances_ir_tests {
    use std::collections::HashMap;

    use nanobpmn_engine_core::{MultiInstance, ProcessBuilder};

    use super::{definition_to_ir, ir_to_definition};

    #[test]
    fn ir_round_trips_cancel_remaining_instances_both_values() {
        for cancel in [true, false] {
            let mut def = ProcessBuilder::new("P")
                .start_event("Start")
                .service_task("Work", "work")
                .end_event("Done")
                .connect("Start", "Work")
                .connect("Work", "Done")
                .build()
                .unwrap();
            def.elements.get_mut("Work").unwrap().multi_instance = Some(MultiInstance {
                input_collection: "=items".into(),
                cancel_remaining_instances: cancel,
                ..Default::default()
            });
            let ir1 = definition_to_ir(&def, &HashMap::new());
            let parsed = ir_to_definition(&ir1).expect("parse");
            let mi = parsed.definition.elements["Work"]
                .multi_instance
                .as_ref()
                .expect("mi");
            assert_eq!(mi.cancel_remaining_instances, cancel, "{ir1}");
            assert_eq!(ir1, definition_to_ir(&parsed.definition, &parsed.names));
        }
    }
}
