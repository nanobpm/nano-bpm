//! Rule #856 — remaining cheap validators.
//!
//! Three small, independent Zeebe deploy checks with no prior Nano equivalent,
//! each restoring parity with a Camunda 8 validator:
//!
//!   1. **An end event must have no outgoing sequence flow.** Zeebe rejects an
//!      end event that declares an outgoing `<sequenceFlow>`
//!      (`EndEventValidator`: `!element.getOutgoing().isEmpty()`). Nano silently
//!      accepted it. Read off the built
//!      [`ProcessDefinition`](crate::model::ProcessDefinition): an
//!      [`ElementKind::EndEvent`] whose [`Element::outgoing`](crate::model::Element::outgoing)
//!      is non-empty is rejected with
//!      [`ParseError::InvalidEndEvent`](crate::bpmn::ParseError::InvalidEndEvent).
//!
//!   2. **No two start events may correlate on the same message or signal.**
//!      Zeebe's `ProcessValidator` runs `ModelUtil.verifyNoDuplicateMessage`/
//!      `SignalStartEvents`. Message starts survive on the built definition as
//!      [`ElementKind::MessageStartEvent`] (keyed by resolved message name).
//!      Signal starts have no dedicated element kind in Nano — a surviving
//!      signal start builds to a plain [`ElementKind::StartEvent`] and any
//!      surplus one is demoted to an inert throw (see the `start_events` slice,
//!      #855) — so their `signalRef`s are read from the raw
//!      [`ProcessCapture`](crate::validate::ProcessCapture), which the parser
//!      snapshots *before* that demotion. A `signalRef` reference site whose
//!      declaring node is a process-level flow node with no incoming flow (and
//!      is not a boundary / intermediate catch event) is a signal start event.
//!      Signals correlate by *name*, so each `signalRef` id is resolved to its
//!      declared `<signal>` name (via the captured id→name map) and deduped by
//!      name — matching the message rule and catching two distinct signal ids
//!      that share one name. A repeated message name / signal name is rejected
//!      with
//!      [`ParseError::DuplicateStartEvent`](crate::bpmn::ParseError::DuplicateStartEvent).
//!
//!   3. **Required `zeebe:taskDefinition` attributes must be non-empty.** Zeebe's
//!      `ZeebeElementValidator.verifyThat(ZeebeTaskDefinition.class)
//!      .hasNonEmptyAttribute(type, retries)` rejects a *declared* attribute that
//!      is empty. The raw attribute strings (before the builder defaults an
//!      absent `type` to the task id) are in `capture.task_definitions`: a
//!      present-but-empty `type` — or a present-but-empty `retries` — is rejected
//!      with
//!      [`ParseError::InvalidTaskDefinition`](crate::bpmn::ParseError::InvalidTaskDefinition).
//!      An *absent* attribute is left to Nano's defaulting (an absent `type`
//!      defaults to the id), matching Zeebe's "when present" semantics.
//!
//! Keep this a **cheap post-parse validator**: work from the built
//! [`ProcessDefinition`](crate::model::ProcessDefinition) and the pre-captured
//! [`ProcessCapture`](crate::validate::ProcessCapture), and avoid widening the
//! parser / capture / [`ParseError`] surface unless a
//! rule genuinely needs a new input — as the signal rule did, adding the
//! captured signal id→name map (`ProcessCapture::signal_names`). This validator
//! is already registered in `validate/mod.rs`, and every `ParseError` reason it
//! raises is already declared.

use std::collections::BTreeSet;

use super::ParseError;
use super::ValidationInput;
use crate::model::ElementKind;

pub(crate) fn validate(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    end_events_have_no_outgoing(input)?;
    no_duplicate_typed_starts(input)?;
    task_definition_attributes_non_empty(input)?;
    Ok(())
}

/// Rule 1: an end event must not declare an outgoing sequence flow.
fn end_events_have_no_outgoing(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    let def = input.def;
    for element in def.elements.values() {
        if matches!(
            element.kind,
            ElementKind::EndEvent | ElementKind::TerminateEndEvent
        ) && !element.outgoing.is_empty()
        {
            return Err(ParseError::InvalidEndEvent {
                process_id: def.id.clone(),
                element_id: element.id.clone(),
                reason: "an end event must have no outgoing sequence flow".to_string(),
            });
        }
    }
    Ok(())
}

/// Rule 2: no two start events correlate on the same message or on the same
/// signal.
fn no_duplicate_typed_starts(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    let def = input.def;

    // Duplicate message start events: message starts survive as
    // `MessageStartEvent` on the built definition, keyed by the resolved
    // message name. Only process-level (top-level) starts count.
    let mut seen_messages: BTreeSet<&str> = BTreeSet::new();
    for element in def.elements.values() {
        if element.parent.is_some() {
            continue;
        }
        if let ElementKind::MessageStartEvent { message_name } = &element.kind {
            if !seen_messages.insert(message_name.as_str()) {
                return Err(ParseError::DuplicateStartEvent {
                    process_id: def.id.clone(),
                    correlation_kind: "message".to_string(),
                    reference: message_name.clone(),
                    reason: "multiple message start events with the same message are not allowed"
                        .to_string(),
                });
            }
        }
    }

    // Duplicate signal start events: signal starts have no dedicated element
    // kind (a surplus one is demoted to an inert throw), so read their
    // `signalRef`s from the pre-demotion capture. A `signalRef` site is a signal
    // start when its declaring node is a process-level flow node that has no
    // incoming flow and is not a boundary / intermediate catch event — i.e. it
    // builds to a `StartEvent` or the demotion target `IntermediateThrowEvent`.
    let flow_targets: BTreeSet<&str> = def
        .elements
        .values()
        .flat_map(|e| e.outgoing.iter().map(|f| f.to.as_str()))
        .collect();
    let mut seen_signals: BTreeSet<&str> = BTreeSet::new();
    for site in &input.capture.references {
        if site.kind != "signalRef" {
            continue;
        }
        let Some(element) = def.elements.get(&site.from_node) else {
            continue;
        };
        let is_process_level_start = element.parent.is_none()
            && !flow_targets.contains(element.id.as_str())
            && matches!(
                element.kind,
                ElementKind::StartEvent | ElementKind::IntermediateThrowEvent
            );
        if !is_process_level_start {
            continue;
        }
        // Signals correlate by *name*, not by `signalRef` id: two distinct
        // `<bpmn:signal>` ids sharing one `name` still collide at correlation
        // time. Resolve the id to its declared name and dedupe by that (falling
        // back to the raw id if the name is somehow absent), so this matches the
        // name-keyed message rule above rather than drifting from it.
        let signal_name = input
            .capture
            .signal_names
            .get(&site.id)
            .map(String::as_str)
            .unwrap_or(site.id.as_str());
        if !seen_signals.insert(signal_name) {
            return Err(ParseError::DuplicateStartEvent {
                process_id: def.id.clone(),
                correlation_kind: "signal".to_string(),
                reference: signal_name.to_string(),
                reason: "multiple signal start events with the same signal are not allowed"
                    .to_string(),
            });
        }
    }

    Ok(())
}

/// Rule 3: a *declared* `zeebe:taskDefinition` attribute must not be empty.
fn task_definition_attributes_non_empty(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    let def = input.def;
    for task in &input.capture.task_definitions {
        // `type`: only validated when present (an absent `type` defaults to the
        // task id, per Nano's parser and Zeebe's "when present" semantics).
        if let Some(job_type) = &task.job_type {
            if job_type.trim().is_empty() {
                return Err(ParseError::InvalidTaskDefinition {
                    process_id: def.id.clone(),
                    task_id: task.task_id.clone(),
                    attribute: "type".to_string(),
                    reason: "must not be empty when declared".to_string(),
                });
            }
        }
        // `retries`: declared optionally; when declared it must be non-empty.
        if let Some(retries) = &task.retries {
            if retries.trim().is_empty() {
                return Err(ParseError::InvalidTaskDefinition {
                    process_id: def.id.clone(),
                    task_id: task.task_id.clone(),
                    attribute: "retries".to_string(),
                    reason: "must not be empty when declared".to_string(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ParseError;

    /// `validate` is exercised through the real streaming parser. The parser is
    /// a *downstream* consumer of `validate`, so this is a test-only fixture edge,
    /// not a production dependency (the #1201 layering lint ignores test bodies).
    fn parse_bpmn(xml: &str) -> Result<Vec<crate::model::ProcessDefinition>, ParseError> {
        crate::bpmn::parse_bpmn(xml)
    }

    /// Wraps process-body `inner` in a `<definitions>` declaring reusable
    /// messages and signals so typed events resolve.
    fn wrap(inner: &str) -> String {
        format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
                 <bpmn:message id="MsgA" name="msgA"/>
                 <bpmn:message id="MsgB" name="msgB"/>
                 <bpmn:signal id="SigA" name="sigA"/>
                 <bpmn:signal id="SigB" name="sigB"/>
                 <bpmn:process id="p" isExecutable="true">{inner}</bpmn:process>
               </bpmn:definitions>"#
        )
    }

    // ---- Rule 1: end event must have no outgoing sequence flow ----

    fn is_invalid_end_event(err: &ParseError) -> bool {
        matches!(err, ParseError::InvalidEndEvent { .. })
    }

    #[test]
    fn end_event_with_outgoing_flow_is_rejected() {
        // RED on `main`: an end event that declares an outgoing flow was
        // silently accepted.
        let inner = r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:endEvent>
                       <bpmn:task id="t"><bpmn:incoming>f2</bpmn:incoming></bpmn:task>
                       <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e"/>
                       <bpmn:sequenceFlow id="f2" sourceRef="e" targetRef="t"/>"#;
        let err = parse_bpmn(&wrap(inner)).expect_err("end event with outgoing rejected");
        assert!(is_invalid_end_event(&err), "got {err:?}");
    }

    #[test]
    fn end_event_without_outgoing_flow_is_accepted() {
        let inner = r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                       <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e"/>"#;
        parse_bpmn(&wrap(inner)).expect("end event without outgoing accepted");
    }

    // ---- Rule 2: no duplicate message / signal start events ----

    /// A process with two typed start events `s1`/`s2`, each with the given
    /// nested event definition, each flowing to its own end event.
    fn two_typed_starts(def1: &str, def2: &str) -> String {
        wrap(&format!(
            r#"<bpmn:startEvent id="s1">{def1}<bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:startEvent id="s2">{def2}<bpmn:outgoing>f2</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="e1"/>
               <bpmn:sequenceFlow id="f2" sourceRef="s2" targetRef="e2"/>"#
        ))
    }

    const MSG_A: &str = r#"<bpmn:messageEventDefinition messageRef="MsgA"/>"#;
    const MSG_B: &str = r#"<bpmn:messageEventDefinition messageRef="MsgB"/>"#;
    const SIG_A: &str = r#"<bpmn:signalEventDefinition signalRef="SigA"/>"#;
    const SIG_B: &str = r#"<bpmn:signalEventDefinition signalRef="SigB"/>"#;

    fn is_duplicate_start(err: &ParseError, kind: &str) -> bool {
        matches!(
            err,
            ParseError::DuplicateStartEvent { correlation_kind, .. } if correlation_kind == kind
        )
    }

    #[test]
    fn duplicate_message_start_events_are_rejected() {
        let err = parse_bpmn(&two_typed_starts(MSG_A, MSG_A))
            .expect_err("duplicate message start rejected");
        assert!(is_duplicate_start(&err, "message"), "got {err:?}");
    }

    #[test]
    fn duplicate_signal_start_events_are_rejected() {
        let err = parse_bpmn(&two_typed_starts(SIG_A, SIG_A))
            .expect_err("duplicate signal start rejected");
        assert!(is_duplicate_start(&err, "signal"), "got {err:?}");
    }

    #[test]
    fn distinct_typed_start_events_are_accepted() {
        // The positive class: two starts correlating on DIFFERENT messages /
        // signals are permitted (and, per #855, multiple typed starts build).
        parse_bpmn(&two_typed_starts(MSG_A, MSG_B)).expect("distinct message starts accepted");
        parse_bpmn(&two_typed_starts(SIG_A, SIG_B)).expect("distinct signal starts accepted");
    }

    #[test]
    fn signal_starts_correlating_on_the_same_name_via_distinct_ids_are_rejected() {
        // RED before the name-keyed fix: signals correlate by NAME, so two
        // signal starts whose `signalRef`s point at *distinct* `<bpmn:signal>`
        // ids that share one `name` still collide at correlation time and must
        // be rejected. Id-keyed dedup silently accepted this. The error reports
        // the resolved name, not either raw id.
        let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                                       xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
             <bpmn:signal id="Sig1" name="shared"/>
             <bpmn:signal id="Sig2" name="shared"/>
             <bpmn:process id="p" isExecutable="true">
               <bpmn:startEvent id="s1"><bpmn:signalEventDefinition signalRef="Sig1"/><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:startEvent id="s2"><bpmn:signalEventDefinition signalRef="Sig2"/><bpmn:outgoing>f2</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="e1"/>
               <bpmn:sequenceFlow id="f2" sourceRef="s2" targetRef="e2"/>
             </bpmn:process>
           </bpmn:definitions>"#;
        let err = parse_bpmn(xml).expect_err("duplicate signal-by-name start rejected");
        assert!(
            matches!(
                &err,
                ParseError::DuplicateStartEvent { correlation_kind, reference, .. }
                    if correlation_kind == "signal" && reference == "shared"
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn a_signal_catch_event_is_not_a_signal_start() {
        // A signal INTERMEDIATE CATCH event shares the `signalRef` capture but has
        // an incoming flow, so it must not be miscounted as a start event: a lone
        // signal start plus a same-signal catch is accepted.
        let inner = r#"<bpmn:startEvent id="s1"><bpmn:signalEventDefinition signalRef="SigA"/><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:intermediateCatchEvent id="c"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing><bpmn:signalEventDefinition signalRef="SigA"/></bpmn:intermediateCatchEvent>
                       <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
                       <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="c"/>
                       <bpmn:sequenceFlow id="f2" sourceRef="c" targetRef="e"/>"#;
        parse_bpmn(&wrap(inner)).expect("signal start + same-signal catch accepted");
    }

    // ---- Rule 3: required zeebe:taskDefinition attributes non-empty ----

    /// A single service task carrying the given raw `zeebe:taskDefinition`
    /// attribute string.
    fn service_task_with_task_def(task_def_attrs: &str) -> String {
        wrap(&format!(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t">
                 <bpmn:extensionElements><zeebe:taskDefinition {task_def_attrs}/></bpmn:extensionElements>
                 <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
               </bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t"/>
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e"/>"#
        ))
    }

    fn is_invalid_task_def(err: &ParseError, attribute: &str) -> bool {
        matches!(
            err,
            ParseError::InvalidTaskDefinition { attribute: a, .. } if a == attribute
        )
    }

    #[test]
    fn empty_task_definition_type_is_rejected() {
        let err = parse_bpmn(&service_task_with_task_def(r#"type="""#))
            .expect_err("empty taskDefinition type rejected");
        assert!(is_invalid_task_def(&err, "type"), "got {err:?}");
    }

    #[test]
    fn empty_task_definition_retries_is_rejected() {
        let err = parse_bpmn(&service_task_with_task_def(r#"type="worker" retries="""#))
            .expect_err("empty taskDefinition retries rejected");
        assert!(is_invalid_task_def(&err, "retries"), "got {err:?}");
    }

    #[test]
    fn non_empty_task_definition_attributes_are_accepted() {
        parse_bpmn(&service_task_with_task_def(r#"type="worker""#))
            .expect("non-empty type accepted");
        parse_bpmn(&service_task_with_task_def(r#"type="worker" retries="5""#))
            .expect("non-empty type + retries accepted");
    }

    #[test]
    fn absent_task_definition_type_is_accepted() {
        // An absent `type` is not the empty-attribute defect — Nano defaults it to
        // the task id, matching Zeebe's "when present" semantics.
        let inner = r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:serviceTask id="t">
                         <bpmn:extensionElements><zeebe:taskDefinition retries="3"/></bpmn:extensionElements>
                         <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                       </bpmn:serviceTask>
                       <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
                       <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t"/>
                       <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e"/>"#;
        parse_bpmn(&wrap(inner)).expect("absent type accepted");
    }
}
