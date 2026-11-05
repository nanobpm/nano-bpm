//! Rule #853 — unsupported flow elements / event definitions (STUB).
//!
//! Sibling slice #853 implements this rule by editing **only** this file. The
//! streaming parser already records every unmodelled flow-element tag / event
//! definition it does not model as `{ tag, element_id }` in
//! `capture.unmodelled` (an explicit ignore-list keeps out non-flow noise).
//! #853 must reject each recorded entry that is genuinely unsupported —
//! deriving the *supported* set from the canonical registry (`processos`
//! `ELEMENT_KIND_SPECS` / engine-core `ElementKind`) so it cannot drift — with
//! [`ParseError::UnsupportedElement`](crate::bpmn::ParseError::UnsupportedElement).
//!
//! Do **not** edit `crate::bpmn`'s streaming parser / catch-all (the tags are
//! already recorded), `validate/mod.rs` (this validator is already registered),
//! or the `ParseError` enum.
//!
//! ## Why this cannot maintain (or drift from) a parallel allowlist
//!
//! The *supported* set is not written here — it is the parser's own modelled
//! `match` arms in [`crate::bpmn`], which correspond one-to-one to the canonical
//! [`crate::model::ElementKind`] registry (mirrored by `processos`'s
//! `ELEMENT_KIND_SPECS`). A tag reaches `capture.unmodelled` **only** when the
//! parser has no arm that models it *and* it is not genuinely-ignorable non-flow
//! noise (`is_ignorable_tag`). So the candidate list handed to this validator is
//! already exactly `all tags − supported − noise`; this rule simply rejects the
//! residual. Because it reads that pre-derived list rather than duplicating a
//! keyword allowlist, the drift surface is closed: teaching the engine a new
//! `ElementKind` (a new parser arm) makes its tag stop appearing in
//! `capture.unmodelled`, so it is accepted here with **no change to this file**.
//!
//! ## Foreign extension-element children are filtered at the parser, not here
//!
//! `<extensionElements>` carries foreign-namespace vendor metadata — Zeebe's
//! `zeebe:*`, Nano's semantic `nano:*`, or any other namespace — not BPMN flow
//! elements or event definitions. The children Nano executes are consumed by the
//! parser's explicit extension arms; every other child (a stray `<zeebe:header>`,
//! a `<zeebe:properties>` container, an open-ended `<nano:cost>`) is metadata
//! Zeebe ignores rather than failing the deploy, so it must not be reported as an
//! unsupported element. The parser recognises this *by position* — its
//! `extension_depth` guard suppresses the flow-element catch-all for anything
//! nested inside `<extensionElements>` — so such children never reach
//! `capture.unmodelled` at all. This validator therefore needs no tag allowlist
//! of its own: it rejects the residual list verbatim, and because the
//! discriminator is "inside `extensionElements`?" (which the parser knows) rather
//! than a curated set of local names (which cannot cover the open-ended `nano:*`
//! vocabulary), there is no second source of truth to drift.

use super::ParseError;
use super::ValidationInput;

/// Rejects the first recorded unmodelled flow element / event definition.
///
/// Every entry in `capture.unmodelled` is a flow-element tag or event definition
/// the engine does not model — the parser's `is_ignorable_tag` noise filter has
/// already excluded diagram interchange, documentation, structural/data BPMN,
/// and the event definitions the engine *does* model, and its `extension_depth`
/// guard has already excluded foreign extension-element children (`zeebe:*`,
/// `nano:*`, …) that live inside `<extensionElements>`. Historically such a
/// flow-element tag was silently dropped: a flow into it then failed deploy with
/// a misleading "unknown target element" error at the flow, and an element with
/// no inbound flow deployed clean and mis-executed. Zeebe instead only
/// transforms known element types and rejects the rest at deploy; matching that,
/// we reject with an actionable [`ParseError::UnsupportedElement`] naming the
/// offending tag and element id.
pub(crate) fn validate(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    reject_unmodelled(input.capture)
}

/// The capture-only core of [`validate`], also consulted by the parser when a
/// process's model assembly fails (#985). An unmodelled
/// construct is the root cause of any downstream build failure it induces —
/// e.g. a boundary event whose only definition is unmodelled is not built, so
/// its outgoing flow would otherwise fail first with a misleading "unknown
/// source element" error. Preferring this over the build error guarantees the
/// actionable [`ParseError::UnsupportedElement`] always wins, for every
/// unmodelled placement, from one implementation.
pub(crate) fn reject_unmodelled(capture: &super::ProcessCapture) -> Result<(), ParseError> {
    if let Some(unsupported) = capture.unmodelled.first() {
        return Err(ParseError::UnsupportedElement {
            tag: unsupported.tag.clone(),
            element_id: unsupported.element_id.clone(),
        });
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

    /// Wraps `body` (the children of a `<process>`) in a minimal, otherwise
    /// deploy-valid definition: a none start event flowing into `body`'s first
    /// declared element is *not* assumed — callers provide whatever they need.
    fn defs_xml(body: &str) -> String {
        format!(
            r#"<bpmn:definitions
                 xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                 xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
                 <bpmn:process id="p" isExecutable="true">
                   {body}
                 </bpmn:process>
               </bpmn:definitions>"#
        )
    }

    fn parse_err(xml: &str) -> Option<ParseError> {
        parse_bpmn(xml).err()
    }

    // ── The defect CLASS: every unmodelled flow element / event definition is
    //    rejected with `UnsupportedElement{tag, element_id}` naming the culprit,
    //    instead of being silently dropped (green here, RED on `main`). ──

    #[test]
    fn unsupported_flow_elements_and_event_definitions_are_rejected() {
        // (fragment, expected offending tag, expected element id). Each fragment
        // is embedded in an otherwise deploy-valid process (a real start + end +
        // flows) so the *only* reason to reject is the unsupported construct —
        // proving the rejection is this rule's, not a start/flow rule's.
        let cases: &[(&str, &str, &str)] = &[
            // Unsupported flow-element tags. These are *detached* (no sequence
            // flow references them) — the exact "deploys clean and silently
            // mis-executes" bug: on `main` the element is dropped and the
            // process deploys; here it must reject. (A flow *into* an unmodelled
            // element is a different symptom the build already rejects, with a
            // misleading message, before this validator runs.)
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
                   <bpmn:complexGateway id="cg" />"#,
                "complexGateway",
                "cg",
            ),
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
                   <bpmn:transaction id="tx" />"#,
                "transaction",
                "tx",
            ),
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
                   <bpmn:callChoreographyTask id="cct" />"#,
                "callChoreographyTask",
                "cct",
            ),
            // Unsupported event definitions: an event the engine does not model
            // must reject, not silently degrade to a bare none event. The owning
            // event *is* modelled, so the build succeeds and the definition is
            // recorded as unmodelled. It carries no id of its own, so it is
            // attributed to the owning element.
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="ce"><bpmn:incoming>f1</bpmn:incoming>
                     <bpmn:cancelEventDefinition />
                   </bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="ce" />"#,
                "cancelEventDefinition",
                "ce",
            ),
        ];

        for (fragment, want_tag, want_id) in cases {
            let xml = defs_xml(fragment);
            match parse_err(&xml) {
                Some(ParseError::UnsupportedElement { tag, element_id }) => {
                    assert_eq!(
                        &tag, want_tag,
                        "wrong tag reported for fragment:\n{fragment}"
                    );
                    assert_eq!(
                        &element_id, want_id,
                        "wrong element id reported for tag `{want_tag}`",
                    );
                }
                other => panic!(
                    "expected UnsupportedElement{{tag: {want_tag:?}, ..}} for fragment:\n{fragment}\ngot {other:?}"
                ),
            }
        }
    }

    // ── Positive: modelled elements (the whole executable surface) and
    //    genuinely-ignorable non-flow noise are accepted. This is also the
    //    DRIFT guard — the validator holds no keyword list, so the supported set
    //    is exactly what the parser (≡ `ElementKind`) models. An element that
    //    looks "exotic" but IS modelled (conditional event, business-rule task,
    //    ad-hoc sub-process) is accepted with no entry in this file. ──

    #[test]
    fn modelled_elements_and_ignorable_noise_are_accepted() {
        let xml = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="st" name="do">
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="work" />
                 </bpmn:extensionElements>
                 <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
               </bpmn:serviceTask>
               <bpmn:exclusiveGateway id="gw" default="f4">
                 <bpmn:incoming>f2</bpmn:incoming>
                 <bpmn:outgoing>f3</bpmn:outgoing><bpmn:outgoing>f4</bpmn:outgoing>
               </bpmn:exclusiveGateway>
               <bpmn:intermediateCatchEvent id="cond">
                 <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f5</bpmn:outgoing>
                 <bpmn:conditionalEventDefinition>
                   <bpmn:condition>=x &gt; 1</bpmn:condition>
                 </bpmn:conditionalEventDefinition>
               </bpmn:intermediateCatchEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>f5</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="st" />
               <bpmn:sequenceFlow id="f2" sourceRef="st" targetRef="gw" />
               <bpmn:sequenceFlow id="f3" sourceRef="gw" targetRef="cond">
                 <bpmn:conditionExpression>=x &gt; 0</bpmn:conditionExpression>
               </bpmn:sequenceFlow>
               <bpmn:sequenceFlow id="f4" sourceRef="gw" targetRef="e1" />
               <bpmn:sequenceFlow id="f5" sourceRef="cond" targetRef="e2" />
               <bpmn:textAnnotation id="note"><bpmn:text>hi</bpmn:text></bpmn:textAnnotation>
               <bpmn:association id="a1" sourceRef="st" targetRef="note" />"#,
        );
        assert!(
            parse_bpmn(&xml).is_ok(),
            "a process built only from modelled elements + ignorable noise must deploy: {:?}",
            parse_bpmn(&xml).err()
        );
    }

    /// The drift guard, made explicit: the *same* tag that is rejected as
    /// unsupported when the engine does not model it is accepted the moment the
    /// engine models it — and this file never mentions the tag. `receiveTask`
    /// (a modelled `ElementKind`) stands in for "a synthetic 'known' kind":
    /// swapping the unsupported `complexGateway` for the modelled `receiveTask`
    /// flips reject → accept with no edit here, because the decision is derived
    /// from the parser's registry, not a list maintained in this validator.
    #[test]
    fn supported_set_is_derived_from_the_parser_not_a_local_list() {
        let unsupported = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
               <bpmn:complexGateway id="t" />"#,
        );
        assert!(
            matches!(
                parse_err(&unsupported),
                Some(ParseError::UnsupportedElement { .. })
            ),
            "an unmodelled `complexGateway` must reject",
        );

        let supported = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:receiveTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:receiveTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />"#,
        );
        assert!(
            parse_bpmn(&supported).is_ok(),
            "a modelled `receiveTask` must deploy without any change to this validator: {:?}",
            parse_bpmn(&supported).err(),
        );
    }

    /// Foreign extension-element children — a `<zeebe:header>` misplaced outside
    /// `<zeebe:taskHeaders>`, a `<zeebe:properties>` container the parser models
    /// nowhere — are extension metadata Zeebe ignores, not unsupported flow
    /// elements. The parser's `extension_depth` guard suppresses the flow-element
    /// catch-all for anything nested inside `<extensionElements>`, so they never
    /// reach `capture.unmodelled`. Guards that against regressing to rejecting
    /// valid deploys.
    #[test]
    fn misplaced_extension_children_are_not_unsupported_elements() {
        let stray_header = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="w" />
                   <zeebe:header key="stray" value="nope" />
                 </bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />"#,
        );
        assert!(
            parse_bpmn(&stray_header).is_ok(),
            "a misplaced <zeebe:header> is extension noise Zeebe ignores, not an \
             unsupported element: {:?}",
            parse_bpmn(&stray_header).err(),
        );

        // The `zeebe:properties` container is a distinct sub-case: the parser
        // models it *nowhere*, so absent the `extension_depth` guard it would
        // always reach the flow-element catch-all (not only when misplaced) and
        // be over-recorded. Its `zeebe:property` children are absorbed by
        // `is_ignorable_tag` upstream. A service task carrying element
        // properties must still deploy clean.
        let element_properties = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="w" />
                   <zeebe:properties>
                     <zeebe:property name="k" value="v" />
                   </zeebe:properties>
                 </bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />"#,
        );
        assert!(
            parse_bpmn(&element_properties).is_ok(),
            "a <zeebe:properties> container is extension metadata Zeebe ignores, not \
             an unsupported element: {:?}",
            parse_bpmn(&element_properties).err(),
        );

        // Open-ended `nano:*` semantic extensions (`processos` round-trips these
        // as `nano:cost` / `nano:time` / `nano:role`, and captures *every* nano
        // child — so no tag allowlist could cover them) live inside
        // `<extensionElements>` like any other vendor metadata. They must deploy
        // clean: this is the exact defect that regressed the `processos`
        // `definition_to_xml_labeled_preserves_nano_extensions_across_round_trip`
        // round-trip, and `nano:sla` stands in for a nano tag no list knows.
        let nano_extensions = r#"<bpmn:definitions
                 xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                 xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                 xmlns:nano="http://nano.camunda.io/schema/semantic/1.0">
                 <bpmn:process id="p" isExecutable="true">
                   <bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:serviceTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                     <bpmn:extensionElements>
                       <zeebe:taskDefinition type="w" />
                       <nano:cost value="0.50" currency="USD" per="invocation" />
                       <nano:time p50="2s" p99="8s" />
                       <nano:role>external</nano:role>
                       <nano:sla>99.9</nano:sla>
                     </bpmn:extensionElements>
                   </bpmn:serviceTask>
                   <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
                   <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
                 </bpmn:process>
               </bpmn:definitions>"#;
        assert!(
            parse_bpmn(nano_extensions).is_ok(),
            "open-ended nano:* semantic extensions are metadata, not unsupported \
             elements: {:?}",
            parse_bpmn(nano_extensions).err(),
        );

        // A genuine unsupported element sitting alongside extension noise must
        // still reject — the `extension_depth` guard only suppresses children
        // *inside* `<extensionElements>`, never a real flow element beside it.
        let noise_plus_unsupported = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="w" />
                   <zeebe:header key="stray" value="nope" />
                 </bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
               <bpmn:transaction id="tx" />"#,
        );
        assert!(
            matches!(
                parse_err(&noise_plus_unsupported),
                Some(ParseError::UnsupportedElement { tag, .. }) if tag == "transaction"
            ),
            "a real unsupported `transaction` must reject even when extension noise \
             was also over-recorded: {:?}",
            parse_err(&noise_plus_unsupported),
        );
    }

    /// A `<terminateEventDefinition>` on an `<endEvent>` is an **executed**
    /// terminate end event (`docs/camunda-compatibility.md`): it parses to
    /// [`ElementKind::TerminateEndEvent`](crate::model::ElementKind::TerminateEndEvent)
    /// via an explicit parse arm and drives real "kill the enclosing scope's
    /// remaining tokens" semantics at runtime. It must therefore deploy clean —
    /// it is a recognised, supported construct, not the silent
    /// accept-and-mis-execute of an *unknown* element that this rule guards
    /// against. (Real corpus models — e.g. the `cdd-refresh` sanctions gate —
    /// rely on this.)
    #[test]
    fn a_terminate_end_event_deploys_clean_and_is_not_rejected_as_unsupported() {
        let terminate_end = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e">
                 <bpmn:incoming>f1</bpmn:incoming>
                 <bpmn:terminateEventDefinition id="TerminateEventDefinition_1" />
               </bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />"#,
        );
        assert!(
            parse_bpmn(&terminate_end).is_ok(),
            "a terminate end event is an executed, supported element, not an \
             unsupported one: {:?}",
            parse_bpmn(&terminate_end).err(),
        );
    }
}
