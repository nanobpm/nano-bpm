//! Rule #851 — generic reference-integrity pass.
//!
//! Every QName/id reference site the streaming parser recorded onto
//! [`ProcessCapture`](super::ProcessCapture) must resolve to a declared target;
//! a dangling one is rejected with the single shared
//! [`ParseError::UnresolvedReference`](crate::bpmn::ParseError::UnresolvedReference)
//! variant (no per-ref bespoke variants). This mirrors camunda-xml-model, which
//! resolves these references eagerly at deploy and fails an unresolved one with
//! `INVALID_ARGUMENT`, plus Zeebe's `ModelUtil.verifyLinkIntermediateEvents`
//! link pairing.
//!
//! Reference sites and the declaration each resolves against:
//!
//! | `kind`          | resolves against                         |
//! |-----------------|------------------------------------------|
//! | `messageRef`    | declared `<message>` ids                 |
//! | `signalRef`     | declared `<signal>` ids                  |
//! | `errorRef`      | declared `<error>` ids                   |
//! | `escalationRef` | declared `<escalation>` ids              |
//! | `default`       | declared `<sequenceFlow>` ids            |
//! | `attachedToRef` | declared activity ids (incl. ad-hoc)     |
//! | `linkThrow`     | a matching intermediate catch link name  |
//!
//! Beyond reference integrity, this pass also enforces the **structural** link
//! rules that keep runtime execution faithful to the model: link names must be
//! non-empty and pair within one scope, and — because a link *throw* hands its
//! token straight to the matching catch (its outgoing flows are ignored) and a
//! link *catch* is activated directly (never routed into) — a throw with any
//! outgoing sequence flow, or a catch with any incoming one, is rejected at
//! deploy rather than deployed and silently mis-executed.
//!
//! Reference kinds that [`crate::bpmn`]'s builder already resolves eagerly —
//! `messageRef`/`signalRef` on start, intermediate-catch and boundary events,
//! and boundary `errorRef` — are rejected by `build` *before* this validator
//! runs (with `InvalidMessageEvent`/`InvalidBoundaryEvent`/`InvalidProcess`), so
//! those models are already rejected; this pass is the single place that also
//! rejects the remaining sites `build` leaves unresolved (non-boundary
//! `errorRef` on error end/throw events, `escalationRef`, a gateway/activity
//! `default` pointing at no declared flow, a boundary `attachedToRef` pointing
//! at no declared activity, and an unpaired throw link). Together they close the
//! whole "silently-accepted dangling reference" class.
//!
//! Flow `sourceRef`/`targetRef`, call-activity `calledElement` and sub-process
//! containment are already resolved in `build` and are intentionally left there.
//!
//! This validator only *reads* pre-captured data; it does not edit
//! [`crate::bpmn`]'s streaming parser, `validate/mod.rs`, or the `ParseError`
//! enum.

use std::collections::{BTreeMap, BTreeSet};

use super::ParseError;
use super::ValidationInput;
use crate::model::ElementKind;

pub(crate) fn validate(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    let capture = input.capture;

    // Every declared activity id an `attachedToRef` may resolve against. A
    // boundary event legally attaches only to an *activity*
    // (task/sub-process/call activity), so the graph elements are filtered to
    // those (via `ElementKind::is_activity`) — a boundary pointing at a
    // gateway/event id is a dangling `attachedToRef`, not "resolved". Unioned
    // with the ad-hoc catalog so a boundary attached to an ad-hoc inner element
    // (pruned from the graph but genuinely declared) is not mistaken for a
    // dangling reference.
    let mut activity_ids: BTreeSet<&str> = input
        .def
        .elements
        .iter()
        .filter(|(_, element)| element.kind.is_activity())
        .map(|(id, _)| id.as_str())
        .collect();
    for adhoc in &input.def.adhoc {
        activity_ids.insert(adhoc.container_id.as_str());
        for tool in &adhoc.tools {
            activity_ids.insert(tool.element_id.as_str());
        }
    }

    for site in &capture.references {
        let resolved = match site.kind {
            "messageRef" => capture.declared_messages.contains(&site.id),
            "signalRef" => capture.declared_signals.contains(&site.id),
            "errorRef" => capture.declared_errors.contains(&site.id),
            "escalationRef" => capture.declared_escalations.contains(&site.id),
            "default" => capture.flow_ids.contains(&site.id),
            "attachedToRef" => activity_ids.contains(site.id.as_str()),
            // An unrecognised reference kind is not something this pass claims to
            // resolve; leave it for whichever stage owns it rather than reject.
            _ => true,
        };
        if !resolved {
            return Err(ParseError::UnresolvedReference {
                kind: site.kind.to_string(),
                id: site.id.clone(),
                process_id: capture.process_id.clone(),
                from_node: site.from_node.clone(),
            });
        }
    }

    // Link pairing: every intermediate *throw* link must have a matching
    // intermediate *catch* link of the same name **in the same scope**, and no
    // two catch links may share a name — an ambiguous target is rejected at
    // deploy (Zeebe `ModelUtil.verifyLinkIntermediateEvents`). An empty link
    // name is malformed and rejected outright (Zeebe requires a non-empty
    // `name`), so a missing/blank name can never route silently under "".
    let mut catch_scopes: BTreeMap<&str, Option<&str>> = BTreeMap::new();
    for (catch_name, catch_scope) in &capture.link_catches {
        if catch_name.trim().is_empty() {
            return Err(ParseError::InvalidProcess {
                process_id: capture.process_id.clone(),
                reason: "intermediate catch link event with an empty link name is not allowed"
                    .to_string(),
            });
        }
        if catch_scopes
            .insert(catch_name.as_str(), catch_scope.as_deref())
            .is_some()
        {
            return Err(ParseError::InvalidProcess {
                process_id: capture.process_id.clone(),
                reason: format!(
                    "multiple intermediate catch link events with the same link name '{catch_name}' are not allowed"
                ),
            });
        }
    }
    for (throw_name, from_node, throw_scope) in &capture.link_throws {
        if throw_name.trim().is_empty() {
            return Err(ParseError::InvalidProcess {
                process_id: capture.process_id.clone(),
                reason: "intermediate throw link event with an empty link name is not allowed"
                    .to_string(),
            });
        }
        match catch_scopes.get(throw_name.as_str()) {
            None => {
                return Err(ParseError::UnresolvedReference {
                    kind: "linkThrow".to_string(),
                    id: throw_name.clone(),
                    process_id: capture.process_id.clone(),
                    from_node: from_node.clone(),
                });
            }
            // A throw and its catch must live in the same scope: link events do
            // not cross a (sub)process boundary. A cross-scope pair would let the
            // runtime activate the catch in the throw's scope, corrupting
            // variable scoping — reject it here so that can never happen.
            Some(catch_scope) if *catch_scope != throw_scope.as_deref() => {
                return Err(ParseError::InvalidProcess {
                    process_id: capture.process_id.clone(),
                    reason: format!(
                        "intermediate throw link event '{throw_name}' and its matching catch link event are in different scopes; link events must be paired within the same scope"
                    ),
                });
            }
            Some(_) => {}
        }
    }

    // Link events do not participate in ordinary sequence flow: a link *throw*
    // has **no** outgoing flow (it hands its token directly to the matching
    // catch) and a link *catch* has **no** incoming flow (it is activated
    // directly by the throw, not routed into). The runtime honours this by
    // ignoring a throw's outgoing flows and never routing a token *into* a
    // catch, so a model that declares such a flow would deploy but silently
    // mis-execute (a throw's downstream would be dropped, a catch could be
    // double-triggered). Reject the structural mistake at deploy, matching the
    // documented link-event semantics.
    let mut flow_targets: BTreeSet<&str> = BTreeSet::new();
    for element in input.def.elements.values() {
        for flow in &element.outgoing {
            flow_targets.insert(flow.to.as_str());
        }
    }
    for (id, element) in &input.def.elements {
        match &element.kind {
            ElementKind::LinkIntermediateThrowEvent { .. } if !element.outgoing.is_empty() => {
                return Err(ParseError::InvalidProcess {
                    process_id: capture.process_id.clone(),
                    reason: format!(
                        "intermediate throw link event '{id}' must not have an outgoing sequence flow"
                    ),
                });
            }
            ElementKind::LinkIntermediateCatchEvent { .. }
                if flow_targets.contains(id.as_str()) =>
            {
                return Err(ParseError::InvalidProcess {
                    process_id: capture.process_id.clone(),
                    reason: format!(
                        "intermediate catch link event '{id}' must not have an incoming sequence flow"
                    ),
                });
            }
            _ => {}
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

    /// Wraps a process body (and optional definitions-level declarations) into a
    /// full `<definitions>` document so a test can focus on one reference site.
    fn model(defs: &str, body: &str) -> String {
        format!(
            r#"<bpmn:definitions
                 xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                 xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
                 {defs}
                 <bpmn:process id="p" isExecutable="true">{body}</bpmn:process>
               </bpmn:definitions>"#
        )
    }

    fn assert_unresolved(xml: &str, kind: &str, id: &str, from_node: &str) {
        match parse_bpmn(xml) {
            Err(ParseError::UnresolvedReference {
                kind: k,
                id: i,
                process_id,
                from_node: f,
            }) => {
                assert_eq!(
                    (k.as_str(), i.as_str(), process_id.as_str(), f.as_str()),
                    (kind, id, "p", from_node),
                    "expected an UnresolvedReference for {kind} '{id}' on '{from_node}'"
                );
            }
            other => panic!("expected UnresolvedReference {kind} '{id}', got {other:?}"),
        }
    }

    #[test]
    fn should_reject_a_dangling_error_ref_on_a_non_boundary_error_event() {
        // `build` resolves boundary `errorRef`s but silently accepts an
        // `errorRef` on an error end/throw event. This pass generalises the
        // resolution to *every* error event: a dangling one is rejected.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:errorEventDefinition errorRef="Missing"/>
               </bpmn:endEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e"/>"#,
        );
        assert_unresolved(&xml, "errorRef", "Missing", "e");
    }

    #[test]
    fn should_reject_a_dangling_escalation_ref() {
        // Escalation references are not modelled for execution and were silently
        // dropped; a dangling `escalationRef` is now rejected.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:escalationEventDefinition escalationRef="Missing"/>
               </bpmn:endEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e"/>"#,
        );
        assert_unresolved(&xml, "escalationRef", "Missing", "e");
    }

    #[test]
    fn should_reject_a_dangling_default_flow_reference() {
        // A gateway `default="…"` naming a sequence flow that is not declared is
        // an unresolved IDREF; `build` would silently ignore it.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:exclusiveGateway id="g" default="Missing">
                 <bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing>
               </bpmn:exclusiveGateway>
               <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="g"/>
               <bpmn:sequenceFlow id="b" sourceRef="g" targetRef="e"/>"#,
        );
        assert_unresolved(&xml, "default", "Missing", "g");
    }

    #[test]
    fn should_reject_a_dangling_attached_to_ref() {
        // A boundary event whose `attachedToRef` names no declared activity is
        // an unresolved reference (the `errorRef` here is declared so the
        // boundary itself is otherwise valid).
        let xml = model(
            r#"<bpmn:error id="Err" errorCode="E1"/>"#,
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t"><bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing></bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
               <bpmn:boundaryEvent id="bnd" attachedToRef="NoSuchTask">
                 <bpmn:errorEventDefinition errorRef="Err"/>
               </bpmn:boundaryEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="t"/>
               <bpmn:sequenceFlow id="b" sourceRef="t" targetRef="e"/>"#,
        );
        assert_unresolved(&xml, "attachedToRef", "NoSuchTask", "bnd");
    }

    #[test]
    fn should_reject_an_unpaired_throw_link() {
        // An intermediate throw link with no matching catch link name is
        // rejected (Zeebe `ModelUtil.verifyLinkIntermediateEvents`).
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:intermediateThrowEvent id="thr">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateThrowEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="thr"/>"#,
        );
        assert_unresolved(&xml, "linkThrow", "L1", "thr");
    }

    #[test]
    fn should_reject_duplicate_catch_link_names() {
        // Two intermediate catch links sharing a name make a throw's target
        // ambiguous; Zeebe's `verifyLinkIntermediateEvents` rejects it at deploy.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:intermediateThrowEvent id="thr">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateThrowEvent>
               <bpmn:intermediateCatchEvent id="c1">
                 <bpmn:outgoing>b</bpmn:outgoing>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateCatchEvent>
               <bpmn:intermediateCatchEvent id="c2">
                 <bpmn:outgoing>c</bpmn:outgoing>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateCatchEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>c</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="thr"/>
               <bpmn:sequenceFlow id="b" sourceRef="c1" targetRef="e1"/>
               <bpmn:sequenceFlow id="c" sourceRef="c2" targetRef="e2"/>"#,
        );
        match parse_bpmn(&xml) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("same link name 'L1'"),
                "unexpected reason: {reason}"
            ),
            other => panic!("expected InvalidProcess for duplicate catch link, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_an_empty_catch_link_name() {
        // A catch link with a missing/blank `name` (parsed as "") is malformed:
        // it would route under an empty link name. Zeebe requires a non-empty
        // name, so reject it at deploy rather than deploy a silently-broken model.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:intermediateCatchEvent id="cat">
                 <bpmn:outgoing>b</bpmn:outgoing>
                 <bpmn:linkEventDefinition/>
               </bpmn:intermediateCatchEvent>
               <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="cat"/>
               <bpmn:sequenceFlow id="b" sourceRef="cat" targetRef="e"/>"#,
        );
        match parse_bpmn(&xml) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("catch link event with an empty link name"),
                "unexpected reason: {reason}"
            ),
            other => panic!("expected InvalidProcess for empty catch link name, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_an_empty_throw_link_name() {
        // A throw link with a missing/blank `name` (parsed as "") is likewise
        // malformed and rejected at deploy.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:intermediateThrowEvent id="thr">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:linkEventDefinition/>
               </bpmn:intermediateThrowEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="thr"/>"#,
        );
        match parse_bpmn(&xml) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("throw link event with an empty link name"),
                "unexpected reason: {reason}"
            ),
            other => panic!("expected InvalidProcess for empty throw link name, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_a_cross_scope_link_pairing() {
        // A throw at the process root and its only same-named catch nested inside
        // an embedded subprocess are in *different* scopes. Link events do not
        // cross a (sub)process boundary, so pairing them is rejected at deploy —
        // this prevents the runtime from ever activating a different-scope catch
        // in the throw's scope (which would corrupt variable scoping).
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:intermediateThrowEvent id="thr">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateThrowEvent>
               <bpmn:subProcess id="Sub">
                 <bpmn:startEvent id="innerStart"><bpmn:outgoing>i1</bpmn:outgoing></bpmn:startEvent>
                 <bpmn:intermediateCatchEvent id="cat">
                   <bpmn:incoming>i1</bpmn:incoming>
                   <bpmn:outgoing>i2</bpmn:outgoing>
                   <bpmn:linkEventDefinition name="L1"/>
                 </bpmn:intermediateCatchEvent>
                 <bpmn:endEvent id="innerEnd"><bpmn:incoming>i2</bpmn:incoming></bpmn:endEvent>
                 <bpmn:sequenceFlow id="i1" sourceRef="innerStart" targetRef="cat"/>
                 <bpmn:sequenceFlow id="i2" sourceRef="cat" targetRef="innerEnd"/>
               </bpmn:subProcess>
               <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="thr"/>
               <bpmn:sequenceFlow id="b" sourceRef="Sub" targetRef="e"/>"#,
        );
        match parse_bpmn(&xml) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("different scopes"),
                "unexpected reason: {reason}"
            ),
            other => panic!("expected InvalidProcess for cross-scope link pairing, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_a_link_throw_with_an_outgoing_flow() {
        // A link *throw* hands its token directly to the matching catch and the
        // runtime ignores its outgoing flows, so a declared outgoing flow would
        // deploy but silently drop that downstream path. Reject it at deploy.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:intermediateThrowEvent id="thr">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:outgoing>x</bpmn:outgoing>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateThrowEvent>
               <bpmn:intermediateCatchEvent id="cat">
                 <bpmn:outgoing>b</bpmn:outgoing>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateCatchEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>x</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="thr"/>
               <bpmn:sequenceFlow id="x" sourceRef="thr" targetRef="e1"/>
               <bpmn:sequenceFlow id="b" sourceRef="cat" targetRef="e2"/>"#,
        );
        match parse_bpmn(&xml) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("throw link event 'thr' must not have an outgoing sequence flow"),
                "unexpected reason: {reason}"
            ),
            other => panic!(
                "expected InvalidProcess for a link throw with an outgoing flow, got {other:?}"
            ),
        }
    }

    #[test]
    fn should_reject_a_link_catch_with_an_incoming_flow() {
        // A link *catch* is activated directly by its throw, never routed into,
        // and the runtime honours this, so a declared incoming flow would deploy
        // but could double-trigger the catch. Reject it at deploy.
        let xml = model(
            "",
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:intermediateCatchEvent id="cat">
                 <bpmn:incoming>a</bpmn:incoming>
                 <bpmn:outgoing>b</bpmn:outgoing>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateCatchEvent>
               <bpmn:intermediateThrowEvent id="thr">
                 <bpmn:incoming>b</bpmn:incoming>
                 <bpmn:linkEventDefinition name="L1"/>
               </bpmn:intermediateThrowEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="cat"/>
               <bpmn:sequenceFlow id="b" sourceRef="cat" targetRef="thr"/>"#,
        );
        match parse_bpmn(&xml) {
            Err(ParseError::InvalidProcess { reason, .. }) => assert!(
                reason.contains("catch link event 'cat' must not have an incoming sequence flow"),
                "unexpected reason: {reason}"
            ),
            other => panic!(
                "expected InvalidProcess for a link catch with an incoming flow, got {other:?}"
            ),
        }
    }

    #[test]
    fn should_reject_an_attached_to_ref_pointing_at_a_non_activity() {
        // A boundary event may attach only to an *activity*; an `attachedToRef`
        // naming a gateway (or any non-activity element) is a dangling
        // reference even though the id is declared. The `errorRef` here is
        // declared so the boundary is otherwise valid.
        let xml = model(
            r#"<bpmn:error id="Err" errorCode="E1"/>"#,
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
               <bpmn:exclusiveGateway id="g"><bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing></bpmn:exclusiveGateway>
               <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
               <bpmn:boundaryEvent id="bnd" attachedToRef="g">
                 <bpmn:errorEventDefinition errorRef="Err"/>
               </bpmn:boundaryEvent>
               <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="g"/>
               <bpmn:sequenceFlow id="b" sourceRef="g" targetRef="e"/>"#,
        );
        assert_unresolved(&xml, "attachedToRef", "g", "bnd");
    }

    #[test]
    fn should_reject_the_whole_unresolved_reference_class() {
        // Class-scoped guard (red on `main`, green here): for *every* reference
        // kind a dangling reference is rejected and a resolving one accepted.
        //
        // `messageRef`/`signalRef` are resolved eagerly by `build` and so are
        // rejected there (a different variant) *before* this pass runs; they are
        // asserted only as "rejected" to prove the class leaves no dangling
        // reference silently accepted. The remaining kinds this pass owns are
        // asserted to be the shared `UnresolvedReference` variant.
        enum Expect {
            /// Rejected specifically as `UnresolvedReference { kind, id, from }`.
            Unresolved(&'static str, &'static str, &'static str),
            /// Rejected by some earlier stage (still not silently accepted).
            Rejected,
            /// Accepted (all references resolve).
            Accepted,
        }
        struct Case {
            name: &'static str,
            defs: &'static str,
            body: &'static str,
            expect: Expect,
        }

        let cases = [
            Case {
                name: "dangling errorRef on an error end event",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>a</bpmn:incoming><bpmn:errorEventDefinition errorRef="M"/></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e"/>"#,
                expect: Expect::Unresolved("errorRef", "M", "e"),
            },
            Case {
                name: "resolved errorRef on an error end event",
                defs: r#"<bpmn:error id="M" errorCode="E1"/>"#,
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>a</bpmn:incoming><bpmn:errorEventDefinition errorRef="M"/></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e"/>"#,
                expect: Expect::Accepted,
            },
            Case {
                name: "dangling escalationRef",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>a</bpmn:incoming><bpmn:escalationEventDefinition escalationRef="M"/></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e"/>"#,
                expect: Expect::Unresolved("escalationRef", "M", "e"),
            },
            Case {
                // The escalation *reference* resolves and escalation is now
                // modelled for execution (#1173): an escalation end event
                // deploys as an escalation throw carrier, so the definition is
                // accepted end-to-end.
                name: "resolved escalationRef on an escalation end event",
                defs: r#"<bpmn:escalation id="M" escalationCode="C1"/>"#,
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>a</bpmn:incoming><bpmn:escalationEventDefinition escalationRef="M"/></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e"/>"#,
                expect: Expect::Accepted,
            },
            Case {
                name: "dangling default flow",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:exclusiveGateway id="g" default="M"><bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing></bpmn:exclusiveGateway>
                         <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="g"/>
                         <bpmn:sequenceFlow id="b" sourceRef="g" targetRef="e"/>"#,
                expect: Expect::Unresolved("default", "M", "g"),
            },
            Case {
                name: "resolved default flow",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:exclusiveGateway id="g" default="b"><bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing></bpmn:exclusiveGateway>
                         <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="g"/>
                         <bpmn:sequenceFlow id="b" sourceRef="g" targetRef="e"/>"#,
                expect: Expect::Accepted,
            },
            Case {
                name: "dangling attachedToRef",
                defs: r#"<bpmn:error id="Err" errorCode="E1"/>"#,
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:serviceTask id="t"><bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing></bpmn:serviceTask>
                         <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
                         <bpmn:boundaryEvent id="bnd" attachedToRef="M"><bpmn:errorEventDefinition errorRef="Err"/></bpmn:boundaryEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="t"/>
                         <bpmn:sequenceFlow id="b" sourceRef="t" targetRef="e"/>"#,
                expect: Expect::Unresolved("attachedToRef", "M", "bnd"),
            },
            Case {
                name: "resolved attachedToRef",
                defs: r#"<bpmn:error id="Err" errorCode="E1"/>"#,
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:serviceTask id="t"><bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing></bpmn:serviceTask>
                         <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
                         <bpmn:boundaryEvent id="bnd" attachedToRef="t"><bpmn:errorEventDefinition errorRef="Err"/></bpmn:boundaryEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="t"/>
                         <bpmn:sequenceFlow id="b" sourceRef="t" targetRef="e"/>"#,
                expect: Expect::Accepted,
            },
            Case {
                name: "attachedToRef pointing at a non-activity (gateway)",
                defs: r#"<bpmn:error id="Err" errorCode="E1"/>"#,
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:exclusiveGateway id="g"><bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>b</bpmn:outgoing></bpmn:exclusiveGateway>
                         <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
                         <bpmn:boundaryEvent id="bnd" attachedToRef="g"><bpmn:errorEventDefinition errorRef="Err"/></bpmn:boundaryEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="g"/>
                         <bpmn:sequenceFlow id="b" sourceRef="g" targetRef="e"/>"#,
                expect: Expect::Unresolved("attachedToRef", "g", "bnd"),
            },
            Case {
                name: "unpaired throw link",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:intermediateThrowEvent id="thr"><bpmn:incoming>a</bpmn:incoming><bpmn:linkEventDefinition name="L1"/></bpmn:intermediateThrowEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="thr"/>"#,
                expect: Expect::Unresolved("linkThrow", "L1", "thr"),
            },
            Case {
                name: "paired throw/catch links",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:intermediateThrowEvent id="thr"><bpmn:incoming>a</bpmn:incoming><bpmn:linkEventDefinition name="L1"/></bpmn:intermediateThrowEvent>
                         <bpmn:intermediateCatchEvent id="cat"><bpmn:outgoing>b</bpmn:outgoing><bpmn:linkEventDefinition name="L1"/></bpmn:intermediateCatchEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>b</bpmn:incoming></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="thr"/>
                         <bpmn:sequenceFlow id="b" sourceRef="cat" targetRef="e"/>"#,
                expect: Expect::Accepted,
            },
            Case {
                name: "dangling messageRef (rejected earlier by build)",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:intermediateCatchEvent id="c"><bpmn:incoming>a</bpmn:incoming><bpmn:messageEventDefinition messageRef="M"/></bpmn:intermediateCatchEvent>
                         <bpmn:endEvent id="e"/>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="c"/>"#,
                expect: Expect::Rejected,
            },
            Case {
                name: "dangling signalRef (rejected earlier by build)",
                defs: "",
                body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:intermediateCatchEvent id="c"><bpmn:incoming>a</bpmn:incoming><bpmn:signalEventDefinition signalRef="M"/></bpmn:intermediateCatchEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="c"/>"#,
                expect: Expect::Rejected,
            },
        ];

        for case in cases {
            let xml = model(case.defs, case.body);
            let result = parse_bpmn(&xml);
            match case.expect {
                Expect::Unresolved(kind, id, from_node) => match result {
                    Err(ParseError::UnresolvedReference {
                        kind: k,
                        id: i,
                        process_id,
                        from_node: f,
                    }) => assert_eq!(
                        (k.as_str(), i.as_str(), process_id.as_str(), f.as_str()),
                        (kind, id, "p", from_node),
                        "case '{}': wrong UnresolvedReference payload",
                        case.name
                    ),
                    other => panic!(
                        "case '{}': expected UnresolvedReference {kind} '{id}', got {other:?}",
                        case.name
                    ),
                },
                Expect::Rejected => assert!(
                    result.is_err(),
                    "case '{}': a dangling reference must not be silently accepted",
                    case.name
                ),
                Expect::Accepted => assert!(
                    result.is_ok(),
                    "case '{}': all references resolve, must be accepted, got {:?}",
                    case.name,
                    result.err()
                ),
            }
        }
    }
}
