//! Parser tests for `bpmn.rs`, extracted verbatim from the former inline
//! `#[cfg(test)]` modules (`tests`, `io_mapping_tests`, `feel_timer_tests`).
use super::*;
use crate::model::Condition;
use crate::model::ElementId;
use crate::model::ElementKind;
use crate::model::TimerDefKind;

const ORDER_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="order" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="charge">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="payment" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;

#[test]
fn should_parse_the_process_name_attribute() {
    // The `<bpmn:process>` `name` attribute (the modeller label) is captured
    // as `ProcessDefinition::name`, distinct from the executable `id`. This
    // is what the process-definition search `name` filter matches against and
    // what read models surface as the definition `name`.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="main-process" name="Main Process" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(def.id, "main-process");
    assert_eq!(def.name.as_deref(), Some("Main Process"));
}

#[test]
fn should_leave_process_name_none_when_absent() {
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="simple-process" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(def.id, "simple-process");
    assert_eq!(def.name, None);
}

#[test]
fn should_reject_a_dangling_outgoing_flow_reference() {
    // A flow node declaring an `<outgoing>` reference to a sequenceFlow that
    // is not declared anywhere is an unresolved QName reference. Zeebe
    // rejects such a model at deploy with `INVALID_ARGUMENT`; Nano must too,
    // rather than silently accepting it because it builds the graph only
    // from `<sequenceFlow>` elements. Regression guard for issue #849.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="invalid-process" isExecutable="true">
              <bpmn:startEvent id="Start">
                <bpmn:outgoing>Flow_missing</bpmn:outgoing>
              </bpmn:startEvent>
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert_eq!(
        err,
        ParseError::UnresolvedReference {
            kind: "outgoing".to_string(),
            id: "Flow_missing".to_string(),
            process_id: "invalid-process".to_string(),
            from_node: "Start".to_string(),
        }
    );
}

#[test]
fn should_reject_a_dangling_incoming_flow_reference() {
    // The `<incoming>` direction is validated the same way as `<outgoing>`.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="invalid-in" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e">
                <bpmn:incoming>Flow_missing</bpmn:incoming>
              </bpmn:endEvent>
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert_eq!(
        err,
        ParseError::UnresolvedReference {
            kind: "incoming".to_string(),
            id: "Flow_missing".to_string(),
            process_id: "invalid-in".to_string(),
            from_node: "e".to_string(),
        }
    );
}

#[test]
fn should_accept_resolved_incoming_outgoing_flow_references() {
    // A well-formed model — the modeller-exported `<incoming>`/`<outgoing>`
    // references all resolve to declared `<sequenceFlow>` ids — is
    // unaffected: it parses cleanly and the graph is built as before.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="valid-process" isExecutable="true">
              <bpmn:startEvent id="s">
                <bpmn:outgoing>a</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e">
                <bpmn:incoming>a</bpmn:incoming>
              </bpmn:endEvent>
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(def.id, "valid-process");
    assert_eq!(def.element("s").unwrap().outgoing[0].to, "e");
}

#[test]
fn should_attribute_container_flow_refs_to_the_container_not_a_nested_child() {
    // A container flow node (`subProcess`/`adHocSubProcess`) whose
    // `<incoming>`/`<outgoing>` follow its nested elements must still have
    // its reference captured and validated against the container, not the
    // most recently *added* (now closed) child. Regression guard for the
    // review finding on issue #849: a single "last added node" pointer would
    // misattribute the reference to `InnerStart` and could mask it. The
    // `<outgoing>` is placed after the nested (non-self-closing) child, and
    // the dangling reference must still be rejected **and attributed to the
    // container `Sub`** (not the nested `InnerStart`) — the `from_node`
    // assertion below enforces that attribution claim directly.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="container-attr" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>toSub</bpmn:outgoing></bpmn:startEvent>
              <bpmn:subProcess id="Sub">
                <bpmn:startEvent id="InnerStart"><bpmn:outgoing>i1</bpmn:outgoing></bpmn:startEvent>
                <bpmn:endEvent id="InnerEnd"><bpmn:incoming>i1</bpmn:incoming></bpmn:endEvent>
                <bpmn:sequenceFlow id="i1" sourceRef="InnerStart" targetRef="InnerEnd" />
                <bpmn:outgoing>Flow_missing</bpmn:outgoing>
              </bpmn:subProcess>
              <bpmn:endEvent id="End"><bpmn:incoming>fromSub</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="toSub" sourceRef="Start" targetRef="Sub" />
              <bpmn:sequenceFlow id="fromSub" sourceRef="Sub" targetRef="End" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert_eq!(
        err,
        ParseError::UnresolvedReference {
            kind: "outgoing".to_string(),
            id: "Flow_missing".to_string(),
            process_id: "container-attr".to_string(),
            from_node: "Sub".to_string(),
        }
    );
}

#[test]
fn should_attribute_anonymous_unmodelled_event_def_under_a_boundary_to_the_boundary() {
    // Regression guard for the suppressed review finding on PR #860: an
    // unsupported event definition that carries no `id` and is nested under a
    // `<boundaryEvent>` must be attributed to the boundary event, not to a
    // containing flow node or an empty id. Boundary events are buffered in
    // `cur_boundary` and are *never* pushed onto `flow_node_stack`, so a
    // fallback that consulted only the stack would misattribute the element —
    // making the future `UnsupportedElement { element_id }` report point at
    // the wrong (or no) element. `<cancelEventDefinition>` is an
    // unmodelled event def (Nano models none/error/timer/message/signal/
    // compensation boundaries, not cancel), and it carries no `id`. We
    // inspect the raw capture directly (the consuming #853 validator is
    // still a stub), which is exactly the data that validator will see.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="boundary-attr" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:serviceTask id="Task"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
              <bpmn:boundaryEvent id="Boundary" attachedToRef="Task">
                <bpmn:cancelEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let capture = &parse_with_captures(xml).unwrap()[0].0;
    let compensate = capture
        .unmodelled
        .iter()
        .find(|u| u.tag == "cancelEventDefinition")
        .expect("the anonymous cancelEventDefinition must be captured as unmodelled");
    assert_eq!(
        compensate.element_id, "Boundary",
        "an anonymous unmodelled event def under a boundary event must attribute \
             to the boundary event, not a containing flow node or an empty id"
    );
}

#[test]
fn should_parse_compensation_throw_and_boundary_with_its_handler() {
    // A `compensateEventDefinition` on an intermediateThrowEvent becomes a
    // CompensationThrowEvent; on a boundary event it becomes a
    // CompensationBoundaryEvent whose handler is resolved from the
    // `<association>` wiring the boundary to the `isForCompensation` activity.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="comp" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
              <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                <bpmn:compensateEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:serviceTask id="CancelBook" isForCompensation="true" />
              <bpmn:association id="a1" sourceRef="BookComp" targetRef="CancelBook" />
              <bpmn:intermediateThrowEvent id="Throw"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
                <bpmn:compensateEventDefinition />
              </bpmn:intermediateThrowEvent>
              <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />
              <bpmn:sequenceFlow id="f2" sourceRef="Book" targetRef="Throw" />
              <bpmn:sequenceFlow id="f3" sourceRef="Throw" targetRef="End" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let defs = parse_bpmn(xml).unwrap();
    let def = &defs[0];
    let throw = def
        .elements
        .get(&ElementId::from("Throw"))
        .expect("throw element");
    assert!(
        matches!(throw.kind, ElementKind::CompensationThrowEvent),
        "compensateEventDefinition on a throw event must be a CompensationThrowEvent, got {:?}",
        throw.kind
    );
    let boundary = def
        .elements
        .get(&ElementId::from("BookComp"))
        .expect("boundary element");
    match &boundary.kind {
        ElementKind::CompensationBoundaryEvent {
            attached_to,
            handler,
        } => {
            assert_eq!(attached_to.as_str(), "Book");
            assert_eq!(handler.as_str(), "CancelBook");
        }
        other => panic!("expected CompensationBoundaryEvent, got {other:?}"),
    }
}

#[test]
fn should_reject_compensation_boundary_associations_that_do_not_resolve_to_a_single_handler() {
    // Failure-mode guard: a compensation boundary's handler is resolved from
    // its `<association>` wiring, but only `isForCompensation` activities are
    // valid handlers. An association to a non-handler node (e.g. a
    // `textAnnotation`) must NOT mis-bind, and multiple candidate handlers
    // must be rejected rather than nondeterministically picking one.
    let boundary = r#"
              <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
              <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                <bpmn:compensateEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />"#;
    let wrap = |body: &str| {
        format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                    <bpmn:process id="comp" isExecutable="true">{boundary}{body}</bpmn:process>
                   </bpmn:definitions>"#
        )
    };

    // (a) The only association points at a `textAnnotation`, not a handler.
    let annotation = wrap(
        r#"<bpmn:textAnnotation id="Note"><bpmn:text>hi</bpmn:text></bpmn:textAnnotation>
               <bpmn:association id="a1" sourceRef="BookComp" targetRef="Note" />"#,
    );
    match parse_bpmn(&annotation) {
        Err(ParseError::InvalidBoundaryEvent { reason, .. }) => {
            assert!(
                reason.contains("isForCompensation"),
                "expected a missing-handler error, got: {reason}"
            );
        }
        other => {
            panic!("expected InvalidBoundaryEvent for a non-handler association, got {other:?}")
        }
    }

    // (b) Two distinct `isForCompensation` handlers are associated — ambiguous.
    let ambiguous = wrap(
        r#"<bpmn:serviceTask id="CancelA" isForCompensation="true" />
               <bpmn:serviceTask id="CancelB" isForCompensation="true" />
               <bpmn:association id="a1" sourceRef="BookComp" targetRef="CancelA" />
               <bpmn:association id="a2" sourceRef="BookComp" targetRef="CancelB" />"#,
    );
    match parse_bpmn(&ambiguous) {
        Err(ParseError::InvalidBoundaryEvent { reason, .. }) => {
            assert!(
                reason.contains("multiple handler activities"),
                "expected an ambiguous-handler error, got: {reason}"
            );
        }
        other => {
            panic!("expected InvalidBoundaryEvent for ambiguous associations, got {other:?}")
        }
    }

    // (c) A handler association plus a harmless annotation association still
    // resolves to the single real handler (the annotation is ignored).
    let mixed = wrap(
        r#"<bpmn:serviceTask id="CancelBook" isForCompensation="true" />
               <bpmn:textAnnotation id="Note"><bpmn:text>hi</bpmn:text></bpmn:textAnnotation>
               <bpmn:association id="a1" sourceRef="BookComp" targetRef="Note" />
               <bpmn:association id="a2" sourceRef="BookComp" targetRef="CancelBook" />"#,
    );
    let defs = parse_bpmn(&mixed).expect("a single real handler must resolve");
    match &defs[0]
        .elements
        .get(&ElementId::from("BookComp"))
        .expect("boundary element")
        .kind
    {
        ElementKind::CompensationBoundaryEvent { handler, .. } => {
            assert_eq!(handler.as_str(), "CancelBook");
        }
        other => panic!("expected CompensationBoundaryEvent, got {other:?}"),
    }
}

#[test]
fn should_not_treat_a_non_activity_as_a_compensation_handler() {
    // Failure-mode guard (advisory): `isForCompensation="true"` is only
    // meaningful on an *activity*. A non-activity node (e.g. a gateway) that
    // stray-carries the attribute must NOT become an eligible handler, so an
    // association pointing at it fails to resolve exactly as if no handler
    // existed — rather than silently binding the boundary to a gateway.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="comp" isExecutable="true">
              <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
              <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                <bpmn:compensateEventDefinition />
              </bpmn:boundaryEvent>
              <bpmn:exclusiveGateway id="NotAHandler" isForCompensation="true" />
              <bpmn:association id="a1" sourceRef="BookComp" targetRef="NotAHandler" />
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />
            </bpmn:process>
          </bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::InvalidBoundaryEvent { reason, .. }) => {
            assert!(
                reason.contains("isForCompensation"),
                "expected a missing-handler error, got: {reason}"
            );
        }
        other => panic!(
            "a gateway carrying isForCompensation must not resolve as a handler, got {other:?}"
        ),
    }
}

#[test]
fn should_reject_sequence_flows_touching_a_compensation_boundary_or_handler() {
    // Failure-mode guard: compensation boundary events and their
    // `isForCompensation` handlers are structural markers outside ordinary
    // token flow. A model that wires a sequenceFlow to/from either would let
    // the engine route tokens through an element it never arms, so deploy
    // must reject it rather than mis-execute.
    let wrap = |body: &str| {
        format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                    <bpmn:process id="comp" isExecutable="true">
                      <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                      <bpmn:serviceTask id="Book"><bpmn:incoming>f1</bpmn:incoming></bpmn:serviceTask>
                      <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
                        <bpmn:compensateEventDefinition />
                      </bpmn:boundaryEvent>
                      <bpmn:serviceTask id="CancelBook" isForCompensation="true" />
                      <bpmn:association id="a1" sourceRef="BookComp" targetRef="CancelBook" />
                      <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Book" />
                      {body}
                    </bpmn:process>
                   </bpmn:definitions>"#
        )
    };

    // (a) A sequenceFlow whose target is the compensation boundary.
    let into_boundary = wrap(
        r#"<bpmn:endEvent id="E" /><bpmn:sequenceFlow id="bad" sourceRef="Book" targetRef="BookComp" />"#,
    );
    match parse_bpmn(&into_boundary) {
        Err(ParseError::InvalidProcess { reason, .. }) => assert!(
            reason.contains("compensation boundary event") && reason.contains("sequenceFlow"),
            "expected a boundary flow rejection, got: {reason}"
        ),
        other => {
            panic!("expected InvalidProcess for a flow into a compensation boundary, got {other:?}")
        }
    }

    // (b) A sequenceFlow whose source is the compensation boundary.
    let out_of_boundary = wrap(
        r#"<bpmn:endEvent id="E"><bpmn:incoming>bad</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="bad" sourceRef="BookComp" targetRef="E" />"#,
    );
    match parse_bpmn(&out_of_boundary) {
        Err(ParseError::InvalidProcess { reason, .. }) => assert!(
            reason.contains("compensation boundary event"),
            "expected a boundary flow rejection, got: {reason}"
        ),
        other => panic!(
            "expected InvalidProcess for a flow out of a compensation boundary, got {other:?}"
        ),
    }

    // (c) A sequenceFlow into the `isForCompensation` handler.
    let into_handler =
        wrap(r#"<bpmn:sequenceFlow id="bad" sourceRef="Book" targetRef="CancelBook" />"#);
    match parse_bpmn(&into_handler) {
        Err(ParseError::InvalidProcess { reason, .. }) => assert!(
            reason.contains("compensation handler activity")
                && reason.contains("isForCompensation"),
            "expected a handler flow rejection, got: {reason}"
        ),
        other => {
            panic!("expected InvalidProcess for a flow into a compensation handler, got {other:?}")
        }
    }

    // (d) A sequenceFlow out of the handler.
    let out_of_handler = wrap(
        r#"<bpmn:endEvent id="E"><bpmn:incoming>bad</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="bad" sourceRef="CancelBook" targetRef="E" />"#,
    );
    match parse_bpmn(&out_of_handler) {
        Err(ParseError::InvalidProcess { reason, .. }) => assert!(
            reason.contains("compensation handler activity"),
            "expected a handler flow rejection, got: {reason}"
        ),
        other => panic!(
            "expected InvalidProcess for a flow out of a compensation handler, got {other:?}"
        ),
    }

    // (e) The well-formed model (no stray flows) still parses.
    let ok = wrap("");
    parse_bpmn(&ok).expect("a compensation model with no stray flows must parse");
}

#[test]
fn should_record_compensate_event_definition_on_an_unsupported_node_as_unmodelled() {
    // Failure-mode guard (advisory): a `compensateEventDefinition` is only
    // modelled as a compensation throw on an intermediateThrowEvent or an
    // endEvent (the two kinds the build step interprets). On any other node
    // kind the flag would be silently dropped at build, so instead the
    // placement is recorded as an unmodelled element attributed to that node
    // — exactly the data the #853 unsupported-elements validator (still a
    // stub) will reject at deploy. We inspect the raw capture directly.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="comp" isExecutable="true">
              <bpmn:startEvent id="Start">
                <bpmn:outgoing>f1</bpmn:outgoing>
                <bpmn:compensateEventDefinition />
              </bpmn:startEvent>
              <bpmn:endEvent id="End"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End" />
            </bpmn:process>
          </bpmn:definitions>"#;
    let capture = &parse_with_captures(xml).unwrap()[0].0;
    let unmodelled = capture
        .unmodelled
        .iter()
        .find(|u| u.tag == "compensateEventDefinition")
        .expect("a compensateEventDefinition on a startEvent must be captured as unmodelled");
    assert_eq!(
        unmodelled.element_id, "Start",
        "the invalid compensateEventDefinition placement must attribute to the startEvent"
    );
}

#[test]
fn should_reject_the_whole_dangling_incoming_outgoing_reference_class() {
    // Class-scoped guard (red on `main`, green here): assert the whole
    // defect class — a dangling `<incoming>` or `<outgoing>` reference on any
    // flow node is rejected as an `UnresolvedReference`, while a model whose
    // references all resolve is accepted. Parametrised over direction and
    // owning-node kind so a future regression on any single site fails here.
    struct Case {
        name: &'static str,
        body: &'static str,
        expect: Option<(&'static str, &'static str, &'static str)>, // (kind, id, from_node) on reject
    }
    let cases = [
        Case {
            name: "dangling outgoing on a start event",
            body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>missing</bpmn:outgoing></bpmn:startEvent>"#,
            expect: Some(("outgoing", "missing", "s")),
        },
        Case {
            name: "dangling incoming on an end event",
            body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>missing</bpmn:incoming></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />"#,
            expect: Some(("incoming", "missing", "e")),
        },
        Case {
            name: "dangling outgoing on a task",
            body: r#"<bpmn:startEvent id="s" />
                         <bpmn:task id="t"><bpmn:outgoing>missing</bpmn:outgoing></bpmn:task>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="t" />"#,
            expect: Some(("outgoing", "missing", "t")),
        },
        Case {
            name: "all references resolve",
            body: r#"<bpmn:startEvent id="s"><bpmn:outgoing>a</bpmn:outgoing></bpmn:startEvent>
                         <bpmn:endEvent id="e"><bpmn:incoming>a</bpmn:incoming></bpmn:endEvent>
                         <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="e" />"#,
            expect: None,
        },
    ];
    for case in cases {
        let xml = format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                     <bpmn:process id="p" isExecutable="true">{}</bpmn:process>
                   </bpmn:definitions>"#,
            case.body
        );
        match (parse_bpmn(&xml), case.expect) {
            (
                Err(ParseError::UnresolvedReference {
                    kind,
                    id,
                    process_id,
                    from_node,
                }),
                Some((ek, eid, efrom)),
            ) => {
                assert_eq!(
                    (
                        kind.as_str(),
                        id.as_str(),
                        process_id.as_str(),
                        from_node.as_str()
                    ),
                    (ek, eid, "p", efrom),
                    "case: {}",
                    case.name
                );
            }
            (Ok(_), None) => {}
            (other, _) => panic!("case {}: unexpected result {other:?}", case.name),
        }
    }
}

#[test]
fn should_parse_a_timer_intermediate_catch_event() {
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="delayed">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="wait">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT1M30S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:intermediateCatchEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="wait" />
              <bpmn:sequenceFlow id="b" sourceRef="wait" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    assert_eq!(
        def.element("wait").unwrap().kind,
        ElementKind::TimerIntermediateCatchEvent {
            duration_millis: 90_000
        }
    );
    assert_eq!(def.element("wait").unwrap().outgoing[0].to, "e");
}

#[test]
fn should_parse_iso8601_durations() {
    assert_eq!(parse_iso8601_duration("PT5S"), Some(5_000));
    assert_eq!(parse_iso8601_duration("PT1M"), Some(60_000));
    assert_eq!(parse_iso8601_duration("PT2H"), Some(7_200_000));
    assert_eq!(parse_iso8601_duration("P1D"), Some(86_400_000));
    assert_eq!(parse_iso8601_duration("P1W"), Some(604_800_000));
    assert_eq!(parse_iso8601_duration("P1DT6H30M"), Some(109_800_000));
    assert_eq!(parse_iso8601_duration(" PT10S "), Some(10_000));
    // invalid / unsupported
    assert_eq!(parse_iso8601_duration("5S"), None);
    assert_eq!(parse_iso8601_duration("P"), None);
    assert_eq!(parse_iso8601_duration("PT"), None);
    assert_eq!(parse_iso8601_duration("P1Y"), None);
    assert_eq!(parse_iso8601_duration("PT5"), None);
}

// zeebe-cells: element:ManualTask element:Task
#[test]
fn should_parse_abstract_task_and_manual_task_as_pass_through() {
    // An abstract `bpmn:task` (and `manualTask`) has no execution semantics;
    // Zeebe/C8 accept it as a pass-through. Nano must parse it (not drop it,
    // which would dangle the inbound sequence flow with an
    // "unknown target element" deploy error).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="abstract" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:task id="do-something" name="Do Something" />
    <bpmn:manualTask id="do-manual" />
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="do-something" />
    <bpmn:sequenceFlow id="f2" sourceRef="do-something" targetRef="do-manual" />
    <bpmn:sequenceFlow id="f3" sourceRef="do-manual" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;

    let defs = parse_bpmn(xml).expect("an abstract task must parse (Zeebe parity)");
    let def = &defs[0];
    assert_eq!(def.element("do-something").unwrap().kind, ElementKind::Task);
    assert_eq!(def.element("do-manual").unwrap().kind, ElementKind::Task);
    // The `name` attribute is captured like any other element.
    assert_eq!(
        def.element("do-something").unwrap().name.as_deref(),
        Some("Do Something")
    );
    // The inbound flow resolves to the task (no dangling target).
    assert_eq!(def.element("start").unwrap().outgoing[0].to, "do-something");
    assert_eq!(
        def.element("do-something").unwrap().outgoing[0].to,
        "do-manual"
    );
}

#[test]
fn should_parse_a_linear_process_with_a_service_task() {
    // given / when
    let defs = parse_bpmn(ORDER_BPMN).unwrap();

    // then
    assert_eq!(defs.len(), 1);
    let def = &defs[0];
    assert_eq!(def.id, "order");
    assert_eq!(def.start_event, "start");
    assert_eq!(
        def.element("charge").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "payment".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
    assert_eq!(def.element("start").unwrap().outgoing[0].to, "charge");
}

// zeebe-cells: element:SendTask
#[test]
fn should_parse_a_send_task_as_a_job_based_service_task() {
    // A `sendTask` with a `zeebe:taskDefinition` is executed by a job worker
    // exactly like a service task (its throwing cousin of `receiveTask`,
    // #1168). A flow into it must resolve to a modelled element rather than
    // failing deploy with the misleading "unknown target element" error.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="notify" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:sendTask id="send" name="Send Notification">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="notifier" retries="4" />
      </bpmn:extensionElements>
    </bpmn:sendTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="send" />
    <bpmn:sequenceFlow id="f2" sourceRef="send" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("send").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "notifier".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
    // The job type falls back to the element id when no taskDefinition type
    // is given, mirroring a serviceTask.
    assert_eq!(def.element("s").unwrap().outgoing[0].to, "send");
    assert_eq!(def.element("send").unwrap().outgoing[0].to, "e");
}

#[test]
fn should_parse_a_bare_send_task_defaulting_job_type_to_its_id() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="notify" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:sendTask id="send" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="send" />
    <bpmn:sequenceFlow id="f2" sourceRef="send" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("send").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "send".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn should_parse_a_non_interrupting_escalation_boundary_and_its_throw() {
    // The #1168 probe, now modelled (#1173): a non-interrupting escalation
    // boundary on a sub-process catching an escalation thrown from inside it.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:intermediateThrowEvent id="Thr">
        <bpmn:escalationEventDefinition escalationRef="Esc" />
      </bpmn:intermediateThrowEvent>
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if1" sourceRef="ss" targetRef="Thr" />
      <bpmn:sequenceFlow id="if2" sourceRef="Thr" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub" cancelActivity="false">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("Bnd").unwrap().kind,
        ElementKind::EscalationBoundaryEvent {
            attached_to: "Sub".to_string(),
            escalation_code: "OVERLOAD".to_string(),
            interrupting: false,
        }
    );
    assert_eq!(
        def.element("Thr").unwrap().kind,
        ElementKind::EscalationThrowEvent {
            escalation_code: "OVERLOAD".to_string(),
        }
    );
}

#[test]
fn should_parse_an_interrupting_escalation_boundary_by_default() {
    // Absent `cancelActivity` (or `="true"`) is interrupting.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if1" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("Bnd").unwrap().kind,
        ElementKind::EscalationBoundaryEvent {
            attached_to: "Sub".to_string(),
            escalation_code: "OVERLOAD".to_string(),
            interrupting: true,
        }
    );
}

#[test]
fn should_parse_a_catch_all_escalation_boundary_without_a_ref() {
    // A boundary escalation carrier with no `escalationRef` is a catch-all
    // (empty escalation code), catching any escalation.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if1" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub" cancelActivity="false">
      <bpmn:escalationEventDefinition />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("Bnd").unwrap().kind,
        ElementKind::EscalationBoundaryEvent {
            attached_to: "Sub".to_string(),
            escalation_code: String::new(),
            interrupting: false,
        }
    );
}

#[test]
fn should_parse_an_escalation_throw_event() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateThrowEvent id="Thr">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:intermediateThrowEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Thr" />
    <bpmn:sequenceFlow id="f2" sourceRef="Thr" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("Thr").unwrap().kind,
        ElementKind::EscalationThrowEvent {
            escalation_code: "OVERLOAD".to_string(),
        }
    );
}

#[test]
fn should_parse_a_ref_less_escalation_throw_as_a_codeless_escalation() {
    // A ref-less escalation carrier (no `escalationRef`) parses to an
    // escalation throw with an empty code (no dangling reference to reject).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateThrowEvent id="Thr">
      <bpmn:escalationEventDefinition />
    </bpmn:intermediateThrowEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Thr" />
    <bpmn:sequenceFlow id="f2" sourceRef="Thr" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("Thr").unwrap().kind,
        ElementKind::EscalationThrowEvent {
            escalation_code: String::new(),
        }
    );
}

#[test]
fn should_parse_an_escalation_end_event() {
    // An escalation end event is an escalation throw carrier on an end event:
    // it raises the escalation and drains (no outgoing flow).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="EscEnd">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="EscEnd" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("EscEnd").unwrap().kind,
        ElementKind::EscalationThrowEvent {
            escalation_code: "OVERLOAD".to_string(),
        }
    );
}

#[test]
fn should_reject_an_escalation_end_event_with_an_outgoing_flow() {
    // Regression (#1173): an escalation `<endEvent>` is remapped to an
    // `EscalationThrowEvent`, so the model-level `end_events_have_no_outgoing`
    // rule (which only matches `EndEvent`/`TerminateEndEvent`) cannot see it.
    // A malformed escalation end event that declares an outgoing flow would
    // otherwise deploy as a *routing* intermediate throw and continue past the
    // end event. Reject it at parse time like any other end-event flavour.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="EscEnd">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:endEvent>
    <bpmn:endEvent id="after" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="EscEnd" />
    <bpmn:sequenceFlow id="f2" sourceRef="EscEnd" targetRef="after" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml).expect_err("escalation end event with outgoing flow is rejected");
    assert!(
        matches!(
            &err,
            ParseError::InvalidEndEvent { element_id, .. } if element_id == "EscEnd"
        ),
        "expected InvalidEndEvent for EscEnd, got {err:?}"
    );
}

#[test]
fn should_reject_a_compensation_end_event_with_an_outgoing_flow() {
    // Same failure class as the escalation end event: a compensation
    // `<endEvent>` is remapped to a `CompensationThrowEvent`, bypassing the
    // end-event rule. Guard every end-event flavour (#1173).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="CompEnd">
      <bpmn:compensateEventDefinition />
    </bpmn:endEvent>
    <bpmn:endEvent id="after" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="CompEnd" />
    <bpmn:sequenceFlow id="f2" sourceRef="CompEnd" targetRef="after" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml).expect_err("compensation end event with outgoing flow is rejected");
    assert!(
        matches!(
            &err,
            ParseError::InvalidEndEvent { element_id, .. } if element_id == "CompEnd"
        ),
        "expected InvalidEndEvent for CompEnd, got {err:?}"
    );
}

#[test]
fn should_reject_an_escalation_boundary_on_a_non_container_activity() {
    // Regression (#1173): an escalation propagates out of the inner scope it
    // is raised in, so an escalation boundary attached to a plain activity
    // (here a service task) — which opens no such scope — is a dead boundary
    // `find_catching_escalation_boundary` can never reach. Reject it at deploy
    // rather than silently accepting an inert boundary.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="task">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="task">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="task" />
    <bpmn:sequenceFlow id="f2" sourceRef="task" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml).expect_err("escalation boundary on a service task is rejected");
    assert!(
        matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. } if reason.contains("Bnd")),
        "expected InvalidBoundaryEvent for Bnd, got {err:?}"
    );
}

#[test]
fn should_accept_an_escalation_boundary_on_an_adhoc_sub_process() {
    // The ad-hoc sub-process is a supported escalation container (#1173): it
    // opens an inner scope, so a boundary attached to it is reachable. It
    // flattens to a job-backed service element, so this guards that the
    // container check keys off `is_adhoc`, not only `NodeKind::SubProcess`.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="Agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="tool">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="tool" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
    </bpmn:adHocSubProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Agent">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="Agent" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).expect("escalation boundary on an ad-hoc sub-process is valid")[0];
    assert_eq!(
        def.element("Bnd").unwrap().kind,
        ElementKind::EscalationBoundaryEvent {
            attached_to: "Agent".to_string(),
            escalation_code: "OVERLOAD".to_string(),
            interrupting: true,
        }
    );
}

#[test]
fn should_reject_a_sequence_flow_targeting_an_escalation_boundary() {
    // Regression (#1173): a reactive boundary event has an outgoing flow to
    // its handler but is itself reached ONLY when its trigger fires — it must
    // never be the TARGET of a sequenceFlow. Otherwise the generic
    // pass-through would complete the boundary and route its handler with no
    // escalation ever raised or matched. Reject an incoming flow at deploy.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
    <bpmn:sequenceFlow id="f4" sourceRef="s" targetRef="Bnd" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err =
        parse_bpmn(xml).expect_err("a sequenceFlow targeting an escalation boundary is rejected");
    assert!(
        matches!(&err, ParseError::InvalidProcess { reason, .. }
                if reason.contains("Bnd") && reason.contains("must not be the target")),
        "expected InvalidProcess for the incoming flow to Bnd, got {err:?}"
    );
}

#[test]
fn should_reject_a_multi_definition_escalation_boundary() {
    // Regression (#1173): a `boundaryEvent` carrying an
    // `escalationEventDefinition` PLUS another trigger (here a timer) would be
    // silently modelled as escalation, discarding the declared timer. There is
    // no OR-trigger boundary in the supported subset, so reject the ambiguous
    // multi-definition boundary at deploy rather than drop a declared event.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
      <bpmn:timerEventDefinition>
        <bpmn:timeDuration>PT5M</bpmn:timeDuration>
      </bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml).expect_err("a multi-definition escalation boundary is rejected");
    assert!(
        matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. }
                if reason.contains("Bnd") && reason.contains("more than one event")),
        "expected InvalidBoundaryEvent for the multi-definition Bnd, got {err:?}"
    );
}

#[test]
fn should_reject_an_escalation_boundary_on_an_adhoc_tool() {
    // Regression (#1173): an embedded-subProcess ad-hoc TOOL opens an inner
    // scope (so the container check passes), and the runtime arms boundary
    // events on it — but an interrupting boundary firing on such a tool cannot
    // release it back to its parent container's active set, hanging the
    // instance. Reject the attachment at deploy rather than mis-execute.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="Agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent" />
      </bpmn:extensionElements>
      <bpmn:subProcess id="tool">
        <bpmn:startEvent id="ts" />
        <bpmn:endEvent id="te" />
        <bpmn:sequenceFlow id="tf" sourceRef="ts" targetRef="te" />
      </bpmn:subProcess>
    </bpmn:adHocSubProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="tool">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="Agent" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml).expect_err("an escalation boundary on an ad-hoc tool is rejected");
    assert!(
        matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. }
                if reason.contains("Bnd") && reason.contains("ad-hoc tool")),
        "expected InvalidBoundaryEvent for the ad-hoc-tool Bnd, got {err:?}"
    );
}

#[test]
fn should_reject_a_duplicate_escalation_definition_on_a_boundary() {
    // Regression (#1173, suppressed advisory): a SECOND
    // `escalationEventDefinition` on one boundary silently overwrites
    // `escalation_ref` (last-wins), so an exact code could be swapped for a
    // catch-all with no diagnostic. Reject the ambiguous multi-definition
    // boundary at deploy.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="EscA" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:escalation id="EscB" name="Any" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="Sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="if" sourceRef="ss" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Sub">
      <bpmn:escalationEventDefinition escalationRef="EscA" />
      <bpmn:escalationEventDefinition escalationRef="EscB" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="Sub" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml).expect_err("a boundary with two escalation definitions is rejected");
    assert!(
        matches!(&err, ParseError::InvalidBoundaryEvent { reason, .. }
                if reason.contains("Bnd") && reason.contains("more than one event")),
        "expected InvalidBoundaryEvent for the duplicate-escalation Bnd, got {err:?}"
    );
}

#[test]
fn should_reject_an_end_event_with_escalation_and_terminate_definitions() {
    // Regression (#1173): an `endEvent` carrying BOTH an escalation and a
    // terminate definition sets both node flags, and the build dispatch picks
    // escalation first — silently discarding the terminate semantics. There is
    // no combined-semantics end event in the supported subset, so reject the
    // ambiguous multi-definition end event at deploy.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="Bad">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
      <bpmn:terminateEventDefinition />
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Bad" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml).expect_err("an end event with escalation + terminate is rejected");
    assert!(
        matches!(
            &err,
            ParseError::InvalidEndEvent { element_id, reason, .. }
                if element_id == "Bad" && reason.contains("more than one event definition")
        ),
        "expected InvalidEndEvent for the multi-definition Bad, got {err:?}"
    );
}

#[test]
fn should_reject_an_intermediate_throw_with_competing_definitions() {
    // Regression (#1173): the throw form of the same class — an
    // `intermediateThrowEvent` carrying BOTH an escalation and a compensation
    // definition. The build dispatch checks escalation first, discarding the
    // compensation throw. Reject it (the throw-side `UnsupportedElement`
    // rejection, matching how a wrong-placement throw is already refused).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateThrowEvent id="Bad">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
      <bpmn:compensateEventDefinition />
    </bpmn:intermediateThrowEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Bad" />
    <bpmn:sequenceFlow id="f2" sourceRef="Bad" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err = parse_bpmn(xml)
        .expect_err("an intermediate throw with escalation + compensation is rejected");
    assert!(
        matches!(
            &err,
            ParseError::UnsupportedElement { tag, element_id }
                if tag == "intermediateThrowEvent" && element_id == "Bad"
        ),
        "expected UnsupportedElement for the multi-definition throw Bad, got {err:?}"
    );
}

#[test]
fn should_reject_an_escalation_throw_inside_an_adhoc_container() {
    // Regression (#1173, critical): an escalation *throw* placed directly in
    // an ad-hoc container is pruned to `AdHocToolKind::Other`; activating that
    // tool completes it directly, so the runtime escalation hook (which only
    // runs for `ElementKind::EscalationThrowEvent`) never fires and the
    // container's escalation boundary handler is silently skipped. nano cannot
    // execute a throw event as an ad-hoc tool, so reject the placement at
    // deploy naming the construct rather than let the model misbehave.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:escalation id="Esc" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="Agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="tool">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="tool" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:intermediateThrowEvent id="ThrowEsc">
        <bpmn:escalationEventDefinition escalationRef="Esc" />
      </bpmn:intermediateThrowEvent>
    </bpmn:adHocSubProcess>
    <bpmn:boundaryEvent id="Bnd" attachedToRef="Agent">
      <bpmn:escalationEventDefinition escalationRef="Esc" />
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="Handler" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="Agent" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="Bnd" targetRef="Handler" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err =
        parse_bpmn(xml).expect_err("an escalation throw inside an ad-hoc container is rejected");
    assert!(
        matches!(&err, ParseError::InvalidProcess { reason, .. }
                if reason.contains("ThrowEsc") && reason.contains("escalation throw event")),
        "expected InvalidProcess naming the escalation throw ThrowEsc, got {err:?}"
    );
}

#[test]
fn should_reject_a_duplicate_escalation_definition_on_a_throw() {
    // Regression (#1173, suppressed advisory): a SECOND
    // `escalationEventDefinition` on one intermediate throw / end event
    // silently overwrites `escalation_ref` (last-wins), swapping the raised
    // code with no diagnostic — the throw/end analogue of the boundary
    // duplicate guard. Reject the ambiguous multi-definition throw at deploy.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:escalation id="EscA" name="Overload" escalationCode="OVERLOAD" />
  <bpmn:escalation id="EscB" name="Any" />
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="Bad">
      <bpmn:escalationEventDefinition escalationRef="EscA" />
      <bpmn:escalationEventDefinition escalationRef="EscB" />
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Bad" />
  </bpmn:process>
</bpmn:definitions>"#;

    let err =
        parse_bpmn(xml).expect_err("an end event with two escalation definitions is rejected");
    assert!(
        matches!(
            &err,
            ParseError::InvalidEndEvent { element_id, reason, .. }
                if element_id == "Bad" && reason.contains("more than one escalationEventDefinition")
        ),
        "expected InvalidEndEvent for the duplicate-escalation end event Bad, got {err:?}"
    );
}

#[test]
fn should_capture_the_element_name_attribute() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="named" isExecutable="true">
    <bpmn:startEvent id="s" name="Start Here" />
    <bpmn:serviceTask id="charge" name="Charge Card">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="payment" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("s").unwrap().name.as_deref(),
        Some("Start Here")
    );
    assert_eq!(
        def.element("charge").unwrap().name.as_deref(),
        Some("Charge Card")
    );
    // An element with no `name` attribute leaves it unset.
    assert_eq!(def.element("e").unwrap().name, None);
}

#[test]
fn should_parse_zeebe_task_definition_retries() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="retryable" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="do-work" retries="=maxRetries" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
    let defs = parse_bpmn(xml).unwrap();
    let def = &defs[0];
    // The retries expression is captured verbatim (with its `=` prefix) for
    // evaluation at job creation; the type still drives the job type.
    assert_eq!(
        def.element("work").unwrap().retries.as_deref(),
        Some("=maxRetries")
    );
    assert_eq!(
        def.element("work").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "do-work".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn should_parse_called_decision_as_a_business_rule_task() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="rules" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:businessRuleTask id="decide">
      <bpmn:extensionElements>
        <zeebe:calledDecision decisionId="rating" resultVariable="score" />
      </bpmn:extensionElements>
    </bpmn:businessRuleTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="decide" />
    <bpmn:sequenceFlow id="f2" sourceRef="decide" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
    let defs = parse_bpmn(xml).unwrap();
    let def = &defs[0];
    // A businessRuleTask carrying a zeebe:calledDecision becomes a native
    // BusinessRuleTask (evaluated in-engine), not a job-based service task.
    assert_eq!(
        def.element("decide").unwrap().kind,
        ElementKind::BusinessRuleTask {
            decision_id: "rating".to_string(),
            result_variable: Some("score".to_string()),
        }
    );
}

#[test]
fn should_parse_business_rule_task_with_task_definition_as_a_service_task() {
    // A businessRuleTask that instead declares a job worker (zeebe:taskDefinition)
    // stays a job-based service task — only calledDecision makes it native.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="rules2" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:businessRuleTask id="decide">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="ruler" />
      </bpmn:extensionElements>
    </bpmn:businessRuleTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="decide" />
    <bpmn:sequenceFlow id="f2" sourceRef="decide" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
    let defs = parse_bpmn(xml).unwrap();
    assert_eq!(
        defs[0].element("decide").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "ruler".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn should_parse_zeebe_script_as_an_inline_script_task() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="scripted" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:scriptTask id="calc">
      <bpmn:extensionElements>
        <zeebe:script expression="=a + b" resultVariable="sum" />
      </bpmn:extensionElements>
    </bpmn:scriptTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="calc" />
    <bpmn:sequenceFlow id="f2" sourceRef="calc" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
    let defs = parse_bpmn(xml).unwrap();
    let def = &defs[0];
    // A scriptTask carrying a zeebe:script becomes an inline ScriptTask, not
    // a job-based service task; the expression is captured verbatim.
    assert_eq!(
        def.element("calc").unwrap().kind,
        ElementKind::ScriptTask {
            expression: "=a + b".to_string(),
            result_variable: "sum".to_string(),
        }
    );
}

#[test]
fn should_parse_a_script_task_with_task_definition_as_a_job() {
    // A scriptTask that declares a zeebe:taskDefinition (no zeebe:script) is
    // job-based, exactly like a service task.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="scripted-job" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:scriptTask id="calc">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="run-script" />
      </bpmn:extensionElements>
    </bpmn:scriptTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="calc" />
    <bpmn:sequenceFlow id="f2" sourceRef="calc" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;
    let defs = parse_bpmn(xml).unwrap();
    let def = &defs[0];
    assert_eq!(
        def.element("calc").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "run-script".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn should_parse_an_adhoc_subprocess_as_a_single_job_and_prune_its_tools() {
    // given: the Camunda 8 agentic AI-agent shape — an <adHocSubProcess> that
    // carries its own zeebe:taskDefinition and contains "tool" activities the
    // worker invokes out-of-band (not by token flow).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent" name="Agentic investigation">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="io.camunda.agenticai:aiagent-job-worker:1" />
                  <zeebe:adHoc outputCollection="toolCallResults"
                               outputElement="={ id: toolCall._meta.id }" />
                </bpmn:extensionElements>
                <bpmn:serviceTask id="tool_issue_credit">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="io.camunda:http-json:1" />
                  </bpmn:extensionElements>
                </bpmn:serviceTask>
                <bpmn:userTask id="tool_review" />
                <bpmn:exclusiveGateway id="tool_gw" />
                <bpmn:sequenceFlow id="t1" sourceRef="tool_review" targetRef="tool_gw" />
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the ad-hoc sub-process is one Service job (type from its own
    // taskDefinition), wired into the parent flow s -> agent -> e.
    assert_eq!(
        def.element("agent").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "io.camunda.agenticai:aiagent-job-worker:1".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
    assert_eq!(def.element("s").unwrap().outgoing[0].to, "agent");
    assert_eq!(def.element("agent").unwrap().outgoing[0].to, "e");
    // and: the contained tool activities (and their internal flow) are pruned
    // from the executable graph.
    assert!(def.element("tool_issue_credit").is_none());
    assert!(def.element("tool_review").is_none());
    assert!(def.element("tool_gw").is_none());

    // and: the ad-hoc tool catalog + zeebe:adHoc wiring is retained as
    // metadata (ADR 0023 Tier-1 substrate).
    assert_eq!(def.adhoc.len(), 1);
    let cat = &def.adhoc[0];
    assert_eq!(cat.container_id, "agent");
    assert_eq!(
        cat.impl_type,
        crate::model::AdHocImplementationType::JobWorker
    );
    assert_eq!(cat.output_collection.as_deref(), Some("toolCallResults"));
    assert_eq!(
        cat.output_element.as_deref(),
        Some("={ id: toolCall._meta.id }")
    );
    // Tools are captured in document order with their kinds; the inner
    // gateway is not an activatable tool and is captured as `Other`.
    let ids: Vec<&str> = cat.tools.iter().map(|t| t.element_id.as_str()).collect();
    assert_eq!(ids, vec!["tool_issue_credit", "tool_review", "tool_gw"]);
    assert_eq!(
        cat.tools[0].kind,
        crate::model::AdHocToolKind::ServiceTask {
            job_type: "io.camunda:http-json:1".to_string()
        }
    );
    assert_eq!(
        cat.tools[1].kind,
        crate::model::AdHocToolKind::UserTask(crate::model::UserTaskProps::default())
    );
    assert_eq!(cat.tools[2].kind, crate::model::AdHocToolKind::Other);
    // and: the `bpmn:sequenceFlow` between the container's own children is
    // captured as an inner flow (issue #1154) — NOT silently dropped — so the
    // runtime can drive the "structured sequence" tool_review -> tool_gw.
    assert_eq!(cat.inner_flows.len(), 1);
    assert_eq!(cat.inner_flows[0].from, "tool_review");
    assert_eq!(cat.inner_flows[0].to, "tool_gw");
    assert_eq!(cat.inner_flows[0].condition, None);
}

// ---- Deploy-time ad-hoc validation (gap #6, Zeebe AdHocSubProcessValidator) ----
// Each case is a full model that is REJECTED at parse/deploy on the branch and
// (was) silently accepted on `main`. Together they assert the whole defect class.

/// A minimal ad-hoc container XML with the given inner body / attributes /
/// extension wiring, wired s -> agent -> e. Callers vary one facet to isolate
/// a single validation rule.
fn adhoc_model(container_attrs: &str, ext: &str, inner: &str) -> String {
    format!(
        r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent"{container_attrs}>
                <bpmn:extensionElements>{ext}</bpmn:extensionElements>
                {inner}
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#
    )
}

fn assert_rejected(xml: &str, needle: &str) {
    match parse_bpmn(xml) {
        Err(ParseError::InvalidProcess { reason, .. }) => assert!(
            reason.contains(needle),
            "expected rejection mentioning {needle:?}, got: {reason}"
        ),
        other => panic!("expected InvalidProcess mentioning {needle:?}, got {other:?}"),
    }
}

#[test]
fn rejects_adhoc_subprocess_with_no_activity() {
    let td = r#"<zeebe:taskDefinition type="agent" />"#;
    let xml = adhoc_model("", td, "");
    assert_rejected(&xml, "at least one activity");
}

#[test]
fn rejects_adhoc_subprocess_with_only_non_activity_children() {
    // Regression guard: a container with direct children that are NOT
    // activities (here a lone gateway) must still be rejected. A bare
    // `inner.is_empty()` check would wrongly accept this, so the validator
    // must require at least one task/sub-process/call activity.
    let td = r#"<zeebe:taskDefinition type="agent" />"#;
    let inner = r#"<bpmn:exclusiveGateway id="g" />"#;
    let xml = adhoc_model("", td, inner);
    assert_rejected(&xml, "at least one activity");
}

#[test]
fn rejects_adhoc_subprocess_containing_a_start_event() {
    let td = r#"<zeebe:taskDefinition type="agent" />"#;
    let inner = r#"<bpmn:serviceTask id="tool" /><bpmn:startEvent id="inner_start" />"#;
    let xml = adhoc_model("", td, inner);
    assert_rejected(&xml, "must not contain a start event");
}

#[test]
fn rejects_adhoc_subprocess_containing_an_end_event() {
    let td = r#"<zeebe:taskDefinition type="agent" />"#;
    let inner = r#"<bpmn:serviceTask id="tool" /><bpmn:endEvent id="inner_end" />"#;
    let xml = adhoc_model("", td, inner);
    assert_rejected(&xml, "must not contain an end event");
}

// ---- Embedded subProcess tool start-event validation (#872) ----
// An embedded `subProcess` tool is driven by injecting a token at its inner
// start event, so it must have exactly one. Zero or many is rejected at
// parse/deploy time rather than silently defaulting to an empty/ambiguous
// start id (which would `Step::Activate` a non-existent element at runtime).

#[test]
fn rejects_embedded_subprocess_tool_with_no_start_event() {
    let td = r#"<zeebe:taskDefinition type="agent" />"#;
    let inner = r#"<bpmn:subProcess id="review"><bpmn:userTask id="ask" /></bpmn:subProcess>"#;
    let xml = adhoc_model("", td, inner);
    assert_rejected(&xml, "must have exactly one start event");
}

#[test]
fn rejects_embedded_subprocess_tool_with_multiple_start_events() {
    let td = r#"<zeebe:taskDefinition type="agent" />"#;
    let inner = r#"<bpmn:subProcess id="review">
            <bpmn:startEvent id="r_s1" />
            <bpmn:startEvent id="r_s2" />
            <bpmn:userTask id="ask" />
        </bpmn:subProcess>"#;
    let xml = adhoc_model("", td, inner);
    assert_rejected(&xml, "must have exactly one start event");
}

#[test]
fn accepts_embedded_subprocess_tool_with_one_start_event() {
    let td = r#"<zeebe:taskDefinition type="agent" />"#;
    let inner = r#"<bpmn:subProcess id="review">
            <bpmn:startEvent id="r_s" />
            <bpmn:userTask id="ask" />
            <bpmn:sequenceFlow id="rf1" sourceRef="r_s" targetRef="ask" />
        </bpmn:subProcess>"#;
    let xml = adhoc_model("", td, inner);
    let def = &parse_bpmn(&xml).unwrap()[0];
    let tool = def.adhoc[0]
        .tools
        .iter()
        .find(|t| t.element_id == "review")
        .expect("embedded subProcess tool is catalogued");
    assert_eq!(
        tool.kind,
        crate::model::AdHocToolKind::SubProcess {
            start_event: "r_s".to_string(),
        }
    );
}

#[test]
fn rejects_taskdefinition_with_active_elements_collection() {
    let ext =
        r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc activeElementsCollection="=elems" />"#;
    let inner = r#"<bpmn:serviceTask id="tool" />"#;
    let xml = adhoc_model("", ext, inner);
    assert_rejected(&xml, "activeElementsCollection");
}

#[test]
fn rejects_output_element_without_output_collection() {
    let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputElement="={ id: 1 }" />"#;
    let inner = r#"<bpmn:serviceTask id="tool" />"#;
    let xml = adhoc_model("", ext, inner);
    assert_rejected(&xml, "outputElement and outputCollection");
}

#[test]
fn rejects_output_collection_without_output_element() {
    let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" />"#;
    let inner = r#"<bpmn:serviceTask id="tool" />"#;
    let xml = adhoc_model("", ext, inner);
    assert_rejected(&xml, "outputElement and outputCollection");
}

#[test]
fn accepts_a_valid_job_worker_adhoc_subprocess() {
    // A well-formed JOB_WORKER container: one activity, both output fields set,
    // cancelRemainingInstances left at its default — must parse cleanly.
    let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" outputElement="={ id: 1 }" />"#;
    let inner = r#"<bpmn:serviceTask id="tool"><bpmn:extensionElements><zeebe:taskDefinition type="http" /></bpmn:extensionElements></bpmn:serviceTask>"#;
    let xml = adhoc_model("", ext, inner);
    let def = &parse_bpmn(&xml).unwrap()[0];
    assert_eq!(def.adhoc.len(), 1);
    assert_eq!(def.adhoc[0].container_id, "agent");
}

#[test]
fn accepts_job_worker_adhoc_with_completion_condition() {
    // Deliberate divergence from Zeebe (issue #614 gap 6): nano supports an
    // engine-side `<completionCondition>` on a JOB_WORKER (taskDefinition)
    // ad-hoc container, so the deploy validator must NOT reject it.
    let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" outputElement="=result" />"#;
    let inner = r#"<bpmn:serviceTask id="tool"><bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements></bpmn:serviceTask><bpmn:completionCondition>=done = true</bpmn:completionCondition>"#;
    let xml = adhoc_model("", ext, inner);
    let def = &parse_bpmn(&xml).unwrap()[0];
    assert_eq!(def.adhoc.len(), 1);
}

#[test]
fn accepts_job_worker_adhoc_with_cancel_remaining_instances_false() {
    // Deliberate divergence from Zeebe (issue #614 gap 6): Zeebe's
    // `AdHocSubProcessValidator` forbids `cancelRemainingInstances="false"`
    // alongside a `zeebe:taskDefinition` (its JOB_WORKER path carries cancel
    // purely on the job result), but nano defers `cancelRemainingInstances`
    // separately (gap 7) and so must NOT reject a JOB_WORKER container that
    // carries it. This is the positive twin of
    // `accepts_job_worker_adhoc_with_completion_condition`, guarding against a
    // future refactor accidentally reintroducing Zeebe's stricter rule.
    let ext = r#"<zeebe:taskDefinition type="agent" /><zeebe:adHoc outputCollection="results" outputElement="=result" />"#;
    let inner = r#"<bpmn:serviceTask id="tool"><bpmn:extensionElements><zeebe:taskDefinition type="tool" /></bpmn:extensionElements></bpmn:serviceTask>"#;
    let xml = adhoc_model(r#" cancelRemainingInstances="false""#, ext, inner);
    let def = &parse_bpmn(&xml).unwrap()[0];
    assert_eq!(def.adhoc.len(), 1);
    assert_eq!(def.adhoc[0].container_id, "agent");
}

#[test]
fn accepts_declarative_active_elements_collection_without_taskdefinition() {
    // The declarative BPMN_TASK variant legitimately declares
    // activeElementsCollection WITHOUT a taskDefinition — it must NOT be
    // rejected by the taskDefinition-combination rule.
    let ext = r#"<zeebe:adHoc activeElementsCollection="=elems" />"#;
    let inner = r#"<bpmn:serviceTask id="tool" />"#;
    let xml = adhoc_model("", ext, inner);
    let def = &parse_bpmn(&xml).unwrap()[0];
    assert_eq!(
        def.adhoc[0].impl_type,
        crate::model::AdHocImplementationType::BpmnTask
    );
}

#[test]
fn should_retain_the_tool_catalog_from_the_camunda_golden_fixture() {
    // The unmodified Camunda AI Agent ad-hoc example (see
    // engine-core/tests/fixtures/adhoc-agent/README.md). It must parse and
    // expose its JOB_WORKER ad-hoc container + tool catalog.
    let xml = include_str!(
        "../../tests/fixtures/adhoc-agent/ai-agent-chat-with-tools/ai-agent-chat-with-tools.bpmn"
    );
    let defs = parse_bpmn(xml).unwrap();
    let def = defs
        .iter()
        .find(|d| d.adhoc.iter().any(|a| a.container_id == "AI_Agent"))
        .expect("the AI_Agent ad-hoc container should be catalogued");
    let cat = def
        .adhoc
        .iter()
        .find(|a| a.container_id == "AI_Agent")
        .unwrap();
    assert_eq!(
        cat.impl_type,
        crate::model::AdHocImplementationType::JobWorker
    );
    assert_eq!(cat.output_collection.as_deref(), Some("toolCallResults"));
    // The fixture's tool set (see the fixtures README table). These are the
    // activities nested inside the AI_Agent ad-hoc container.
    for tool in [
        "LoadUserByID",
        "ListUsers",
        "Search_Recipe",
        "GetDateAndTime",
        "SuperfluxProduct",
        "SendEmail",
        "Jokes_API",
        "Fetch_URL",
        "AskHumanToSendEmail",
        "Handle_Message",
    ] {
        assert!(
            cat.tools.iter().any(|t| t.element_id == tool),
            "tool {tool} should be in the catalog"
        );
    }
    // The agent container is still one job in the executable graph, and the
    // tools are pruned from it.
    assert!(matches!(
        def.element("AI_Agent").unwrap().kind,
        ElementKind::ServiceTask { .. }
    ));
    assert!(def.element("Search_Recipe").is_none());
}

#[test]
fn should_mark_the_gateway_default_flow_regardless_of_document_order() {
    // given: an exclusive gateway whose `default` flow is listed FIRST, with
    // the conditional flow second — the order Camunda often serialises.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:exclusiveGateway id="gw" default="to_default" />
              <bpmn:endEvent id="default_task" />
              <bpmn:endEvent id="cond_task" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gw" />
              <bpmn:sequenceFlow id="to_default" sourceRef="gw" targetRef="default_task" />
              <bpmn:sequenceFlow id="to_cond" sourceRef="gw" targetRef="cond_task">
                <bpmn:conditionExpression>=isDuplicate</bpmn:conditionExpression>
              </bpmn:sequenceFlow>
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the gateway's outgoing flows carry the right is_default markers —
    // the default flow is flagged even though it appears first in the document.
    let gw = def.element("gw").unwrap();
    let default = gw.outgoing.iter().find(|f| f.to == "default_task").unwrap();
    let cond = gw.outgoing.iter().find(|f| f.to == "cond_task").unwrap();
    assert!(default.is_default, "default flow should be flagged");
    assert!(default.condition.is_none());
    assert!(!cond.is_default, "conditional flow should not be default");
    assert!(cond.condition.is_some());
}

#[test]
fn should_parse_a_user_task_and_link_its_flows() {
    // given: start -> review (userTask) -> end, the shape the Camunda SDK's
    // user-task fixture deploys (a <bpmn:userTask> with <zeebe:userTask/>).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the user task is recognised and its sequence flows resolve (a
    // regression guard against the "unknown target element" parse error).
    assert!(matches!(
        def.element("review").unwrap().kind,
        ElementKind::UserTask(_)
    ));
    assert_eq!(def.element("s").unwrap().outgoing[0].to, "review");
    assert_eq!(def.element("review").unwrap().outgoing[0].to, "e");
}

#[test]
fn should_parse_user_task_assignment_schedule_and_priority() {
    // given: a user task carrying assignment/schedule/priority extension
    // elements, as the Camunda modeler emits them.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:assignmentDefinition assignee="=requester"
                      candidateGroups="ops,finance" candidateUsers="=reviewers" />
                  <zeebe:taskSchedule dueDate="2025-01-01T00:00:00Z"
                      followUpDate="=followUp" />
                  <zeebe:priorityDefinition priority="80" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the raw expressions are captured on the user-task props.
    let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
        panic!("expected a user task");
    };
    assert_eq!(props.assignee.as_deref(), Some("=requester"));
    assert_eq!(props.candidate_groups.as_deref(), Some("ops,finance"));
    assert_eq!(props.candidate_users.as_deref(), Some("=reviewers"));
    assert_eq!(props.due_date.as_deref(), Some("2025-01-01T00:00:00Z"));
    assert_eq!(props.follow_up_date.as_deref(), Some("=followUp"));
    assert_eq!(props.priority.as_deref(), Some("80"));
}

#[test]
fn should_parse_form_definitions_on_user_task_and_start_event() {
    // given: a start event carrying a start form and a user task carrying its
    // own form, both via zeebe:formDefinition (as the Camunda modeler emits).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:extensionElements>
                  <zeebe:formDefinition formId="start-form" />
                </bpmn:extensionElements>
              </bpmn:startEvent>
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the start form rides on the definition, and the user-task form on
    // its props.
    assert_eq!(def.start_form_id.as_deref(), Some("start-form"));
    let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
        panic!("expected a user task");
    };
    assert_eq!(props.form_id.as_deref(), Some("review-form"));
    assert_eq!(props.external_form_reference, None);
}

#[test]
fn should_parse_external_form_reference_on_user_task() {
    // given: a user task referencing an external form (no deployed formId).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition externalReference="https://forms.example/x" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
        panic!("expected a user task");
    };
    assert_eq!(props.form_id, None);
    assert_eq!(
        props.external_form_reference.as_deref(),
        Some("https://forms.example/x")
    );
    assert_eq!(def.start_form_id, None);
}

#[test]
fn should_let_external_reference_win_when_form_definition_declares_both() {
    // given: a (malformed but tolerated) zeebe:formDefinition that declares
    // BOTH formId and externalReference. Zeebe treats these as mutually
    // exclusive, so the parser must keep them so downstream — an external
    // reference wins and suppresses the form id, ensuring a task never
    // surfaces both a numeric formKey and an externalFormReference.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="both">
                <bpmn:extensionElements>
                  <zeebe:formDefinition formId="feature-escalation"
                                        externalReference="https://forms.example/x" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="both" />
              <bpmn:sequenceFlow id="b" sourceRef="both" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the external reference wins; the form id is suppressed.
    let ElementKind::UserTask(both) = &def.element("both").unwrap().kind else {
        panic!("expected a user task");
    };
    assert_eq!(both.form_id, None);
    assert_eq!(
        both.external_form_reference.as_deref(),
        Some("https://forms.example/x")
    );
}

#[test]
fn should_accept_explicit_latest_binding_on_a_user_task_form() {
    // given: a user-task formDefinition that explicitly declares the default
    // `bindingType="latest"`. This is the binding the engine implements
    // (resolve `formId` to the latest deployed form version at task
    // creation), so it parses exactly like an omitted bindingType.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" bindingType="latest" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: no regression — the form id binds latest-at-creation as before.
    let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
        panic!("expected a user task");
    };
    assert_eq!(props.form_id.as_deref(), Some("review-form"));
}

#[test]
fn should_reject_deployment_binding_on_a_user_task_form() {
    // given: a user-task formDefinition declaring `bindingType="deployment"`.
    // The engine does not implement the `deployment` binding; degrading it to
    // `latest` would silently mis-bind the form (#1190), so the deploy must
    // be rejected loudly rather than parsed.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" bindingType="deployment" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when / then
    let err = parse_bpmn(xml).expect_err("a non-latest form binding is rejected");
    assert_eq!(
        err,
        ParseError::UnsupportedUserTaskFormBinding {
            task_id: "review".to_string(),
            binding_type: "deployment".to_string(),
        }
    );
}

#[test]
fn should_reject_version_tag_binding_on_a_user_task_form() {
    // given: a user-task formDefinition declaring `bindingType="versionTag"`.
    // Like `deployment`, this binding is unimplemented and must fail the
    // deploy loudly rather than degrade to `latest` (#1190).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form"
                                        bindingType="versionTag" versionTag="v2" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when / then
    let err = parse_bpmn(xml).expect_err("a versionTag form binding is rejected");
    assert_eq!(
        err,
        ParseError::UnsupportedUserTaskFormBinding {
            task_id: "review".to_string(),
            binding_type: "versionTag".to_string(),
        }
    );
}

#[test]
fn should_ignore_non_latest_binding_on_an_external_reference_form() {
    // given: an externalReference form that also (redundantly) carries a
    // non-latest bindingType. The externalReference suppresses `formId` and
    // no form version is resolved, so the unimplemented binding is moot and
    // must not trip the interim guard.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition externalReference="https://forms.example/x"
                                        bindingType="deployment" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).expect("an external-reference form is not version-bound")[0];

    // then
    let ElementKind::UserTask(props) = &def.element("review").unwrap().kind else {
        panic!("expected a user task");
    };
    assert_eq!(props.form_id, None);
    assert_eq!(
        props.external_form_reference.as_deref(),
        Some("https://forms.example/x")
    );
}

#[test]
fn should_reject_explicit_empty_binding_on_a_user_task_form() {
    // given: a user-task formDefinition declaring an explicitly *empty*
    // `bindingType=""`. An empty attribute is a present, explicit value — not
    // an absent one — so it must not be silently treated as the `latest`
    // default (which would reopen the silent-degradation path #1190 closes).
    // Only an omitted `bindingType` defaults.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition formId="review-form" bindingType="" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when / then
    let err = parse_bpmn(xml).expect_err("an explicit empty form binding is rejected");
    assert_eq!(
        err,
        ParseError::UnsupportedUserTaskFormBinding {
            task_id: "review".to_string(),
            binding_type: String::new(),
        }
    );
}

#[test]
fn should_reject_non_latest_binding_when_form_id_is_absent() {
    // given: a user-task formDefinition with an unimplemented
    // `bindingType="deployment"` but no `formId` (and no `externalReference`).
    // Keying the guard off the raw `externalReference` — not off a resolved
    // `formId` — closes this path: a deployed (non-external) form must carry
    // only the default binding regardless of whether a `formId` happens to be
    // present, so this declaration is rejected rather than silently accepted.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:userTask />
                  <zeebe:formDefinition bindingType="deployment" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="b" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when / then
    let err = parse_bpmn(xml).expect_err("a non-latest binding without a formId is rejected");
    assert_eq!(
        err,
        ParseError::UnsupportedUserTaskFormBinding {
            task_id: "review".to_string(),
            binding_type: "deployment".to_string(),
        }
    );
}

#[test]
fn should_parse_service_task_task_headers_into_custom_headers() {
    // given: a service task carrying static zeebe:taskHeaders alongside its
    // taskDefinition, as Camunda 8 emits custom headers. Two headers, given
    // out of key order, to prove the parser captures every entry (order is
    // normalised by the BTreeMap).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="payment" />
                  <zeebe:taskHeaders>
                    <zeebe:header key="retryBackoff" value="PT5S" />
                    <zeebe:header key="channel" value="card" />
                  </zeebe:taskHeaders>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: both headers ride on the service task element verbatim.
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("channel".to_string(), "card".to_string());
    expected.insert("retryBackoff".to_string(), "PT5S".to_string());
    assert_eq!(
        def.element("charge").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "payment".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: expected,
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn self_closing_task_headers_do_not_leak_into_later_headers() {
    // given: a first service task with a self-closing (empty)
    // `<zeebe:taskHeaders />`, then a second service task whose own
    // `zeebe:header` sits *outside* any taskHeaders container. A
    // self-closing start tag emits no matching end tag, so the
    // `in_task_headers` gate must not stay stuck open across elements.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="first">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="a" />
                  <zeebe:taskHeaders />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="second">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="b" />
                  <zeebe:header key="stray" value="nope" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="first" />
              <bpmn:sequenceFlow id="f2" sourceRef="first" targetRef="second" />
              <bpmn:sequenceFlow id="f3" sourceRef="second" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the empty container yields no headers, and the stray header
    // that follows is *not* captured onto the second task.
    for id in ["first", "second"] {
        match &def.element(id).unwrap().kind {
            ElementKind::ServiceTask { custom_headers, .. } => {
                assert!(
                    custom_headers.is_empty(),
                    "{id} unexpectedly captured headers: {custom_headers:?}"
                );
            }
            other => panic!("{id} should be a service task, got {other:?}"),
        }
    }
}

#[test]
fn self_closing_execution_listeners_do_not_leak_into_later_listeners() {
    // given: a first service task with a self-closing (empty)
    // `<zeebe:executionListeners />`, then a second service task whose own
    // stray `zeebe:executionListener` sits *outside* any executionListeners
    // container. A self-closing start tag emits no matching end tag, so the
    // `in_execution_listeners` gate must not stay stuck open across elements
    // (mirrors the `zeebe:taskHeaders` gate) — otherwise the stray listener is
    // wrongly captured onto whatever element is open on the io_stack.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="first">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="a" />
                  <zeebe:executionListeners />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="second">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="b" />
                  <zeebe:executionListener eventType="start" type="stray" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="first" />
              <bpmn:sequenceFlow id="f2" sourceRef="first" targetRef="second" />
              <bpmn:sequenceFlow id="f3" sourceRef="second" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the empty container yields no listeners, and the stray listener
    // that follows is *not* captured onto the second task.
    for id in ["first", "second"] {
        let el = def.element(id).unwrap();
        assert!(
            el.start_listeners.is_empty() && el.end_listeners.is_empty(),
            "{id} unexpectedly captured listeners: start={:?} end={:?}",
            el.start_listeners,
            el.end_listeners
        );
    }
}

#[test]
fn stray_linked_resource_outside_a_container_is_not_captured() {
    // given: a first service task with a self-closing (empty)
    // `<zeebe:linkedResources />`, then a second service task whose own
    // `zeebe:linkedResource` sits *outside* any linkedResources container.
    // The `in_linked_resources` gate must reject the stray link and must
    // not stay stuck open across elements (a self-closing start tag emits
    // no matching end tag). Mirrors the `zeebe:taskHeaders` gate.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="first">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="a" />
                  <zeebe:linkedResources />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:serviceTask id="second">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="b" />
                  <zeebe:linkedResource resourceId="stray.md" bindingType="latest"
                                        resourceType="GenericScript" linkName="stray" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="first" />
              <bpmn:sequenceFlow id="f2" sourceRef="first" targetRef="second" />
              <bpmn:sequenceFlow id="f3" sourceRef="second" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the empty container yields no links, and the stray
    // linkedResource that follows is *not* captured onto the second task.
    for id in ["first", "second"] {
        match &def.element(id).unwrap().kind {
            ElementKind::ServiceTask {
                linked_resources, ..
            } => {
                assert!(
                    linked_resources.is_empty(),
                    "{id} unexpectedly captured linked resources: {linked_resources:?}"
                );
            }
            other => panic!("{id} should be a service task, got {other:?}"),
        }
    }
}

#[test]
fn should_parse_service_task_job_priority() {
    // given: a service task carrying a zeebe:priorityDefinition (job priority),
    // alongside its taskDefinition, as Camunda 8.10 emits it.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="payment" />
                  <zeebe:priorityDefinition priority="=urgency" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the raw priority expression rides on the service task element.
    assert_eq!(
        def.element("charge").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "payment".to_string(),
            priority: Some("=urgency".to_string()),
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn should_default_service_task_priority_to_none_when_absent() {
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="payment" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="b" sourceRef="charge" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("charge").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "payment".to_string(),
            priority: None,
            custom_headers: std::collections::BTreeMap::new(),
            agent_type: None,
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn should_default_service_task_job_type_to_its_id() {
    // given
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="work" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="work" />
              <bpmn:sequenceFlow id="b" sourceRef="work" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("work").unwrap().kind,
        ElementKind::ServiceTask {
            job_type: "work".to_string(),
            priority: None,
            agent_type: None,
            custom_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
        }
    );
}

#[test]
fn should_parse_exclusive_gateway_with_conditions() {
    // given
    let xml = r#"
          <definitions>
            <process id="route">
              <startEvent id="s" />
              <exclusiveGateway id="gw" default="f2" />
              <endEvent id="yes" />
              <endEvent id="no" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="gw" />
              <sequenceFlow id="f1" sourceRef="gw" targetRef="yes">
                <conditionExpression xsi:type="tFormalExpression">= decision = "yes"</conditionExpression>
              </sequenceFlow>
              <sequenceFlow id="f2" sourceRef="gw" targetRef="no" />
            </process>
          </definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    let gw = def.element("gw").unwrap();
    let to_yes = gw.outgoing.iter().find(|f| f.to == "yes").unwrap();
    assert_eq!(
        to_yes.condition,
        Some(Condition::new(r#"= decision = "yes""#))
    );
    let to_no = gw.outgoing.iter().find(|f| f.to == "no").unwrap();
    assert_eq!(to_no.condition, None);
}

#[test]
fn should_parse_event_based_gateway_and_its_catch_events() {
    // given: an event-based gateway routing to a timer and a message
    // intermediate catch event (a classic timer-vs-message race).
    let xml = r#"
          <definitions>
            <message id="Msg_reply" name="reply">
              <extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </extensionElements>
            </message>
            <process id="race">
              <startEvent id="s" />
              <eventBasedGateway id="gw" />
              <intermediateCatchEvent id="onTimer">
                <timerEventDefinition><timeDuration>PT1H</timeDuration></timerEventDefinition>
              </intermediateCatchEvent>
              <intermediateCatchEvent id="onReply">
                <messageEventDefinition messageRef="Msg_reply" />
              </intermediateCatchEvent>
              <endEvent id="timedOut" />
              <endEvent id="replied" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="gw" />
              <sequenceFlow id="f1" sourceRef="gw" targetRef="onTimer" />
              <sequenceFlow id="f2" sourceRef="gw" targetRef="onReply" />
              <sequenceFlow id="f3" sourceRef="onTimer" targetRef="timedOut" />
              <sequenceFlow id="f4" sourceRef="onReply" targetRef="replied" />
            </process>
          </definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the gateway parsed as an event-based gateway with both targets.
    let gw = def.element("gw").unwrap();
    assert_eq!(gw.kind, crate::model::ElementKind::EventBasedGateway);
    let targets: std::collections::BTreeSet<&str> =
        gw.outgoing.iter().map(|f| f.to.as_str()).collect();
    assert_eq!(targets, ["onReply", "onTimer"].into_iter().collect());
}

#[test]
fn should_parse_terminate_end_event() {
    // given: a plain end event and an end event carrying a
    // `terminateEventDefinition` (a terminate end).
    let xml = r#"
          <definitions>
            <process id="term">
              <startEvent id="s" />
              <parallelGateway id="split" />
              <endEvent id="plain" />
              <endEvent id="stop">
                <terminateEventDefinition />
              </endEvent>
              <sequenceFlow id="f0" sourceRef="s" targetRef="split" />
              <sequenceFlow id="f1" sourceRef="split" targetRef="plain" />
              <sequenceFlow id="f2" sourceRef="split" targetRef="stop" />
            </process>
          </definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: the terminate end parses to the terminate kind; the plain end
    // stays a none end event.
    assert_eq!(
        def.element("stop").unwrap().kind,
        crate::model::ElementKind::TerminateEndEvent
    );
    assert_eq!(
        def.element("plain").unwrap().kind,
        crate::model::ElementKind::EndEvent
    );
}

#[test]
fn should_parse_multiple_processes_in_one_file() {
    // given
    let xml = r#"
          <definitions>
            <process id="a"><startEvent id="s" /><endEvent id="e" />
              <sequenceFlow id="f" sourceRef="s" targetRef="e" /></process>
            <process id="b"><startEvent id="s" /><endEvent id="e" />
              <sequenceFlow id="f" sourceRef="s" targetRef="e" /></process>
          </definitions>"#;

    // when
    let defs = parse_bpmn(xml).unwrap();

    // then
    assert_eq!(
        defs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
}

#[test]
fn should_reject_a_file_without_a_process() {
    // given
    let xml = r#"<definitions xmlns="x"></definitions>"#;

    // when / then
    assert_eq!(parse_bpmn(xml), Err(ParseError::NoProcess));
}

#[test]
fn should_reject_a_process_without_a_start_event() {
    // given
    let xml = r#"<definitions><process id="p"><endEvent id="e" /></process></definitions>"#;

    // when
    let err = parse_bpmn(xml).unwrap_err();

    // then
    assert!(matches!(err, ParseError::InvalidProcess { .. }));
}

#[test]
fn should_parse_an_error_boundary_event() {
    // given
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="charge-card" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="declined" attachedToRef="charge">
                <bpmn:errorEventDefinition errorRef="Error_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="refunded" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="declined" targetRef="refunded" />
            </bpmn:process>
            <bpmn:error id="Error_1" name="Declined" errorCode="CARD_DECLINED" />
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("declined").unwrap().kind,
        ElementKind::ErrorBoundaryEvent {
            attached_to: "charge".to_string(),
            error_code: "CARD_DECLINED".to_string(),
        }
    );
    let boundary = def.element("declined").unwrap();
    assert!(boundary.outgoing.iter().any(|f| f.to == "refunded"));
}

#[test]
fn should_parse_a_timer_boundary_event() {
    // given
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="charge-card" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="timeout" attachedToRef="charge">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT5S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="escalated" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="timeout" targetRef="escalated" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("timeout").unwrap().kind,
        ElementKind::TimerBoundaryEvent {
            attached_to: "charge".to_string(),
            duration_millis: 5_000,
            interrupting: true,
            repeating: false,
        }
    );
    let boundary = def.element("timeout").unwrap();
    assert!(boundary.outgoing.iter().any(|f| f.to == "escalated"));
}

#[test]
fn should_reject_a_boundary_event_referencing_an_unknown_error() {
    // given
    let xml = r#"
          <definitions>
            <process id="p">
              <startEvent id="s" />
              <serviceTask id="t" />
              <endEvent id="e" />
              <boundaryEvent id="b" attachedToRef="t">
                <errorEventDefinition errorRef="missing" />
              </boundaryEvent>
              <endEvent id="caught" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="t" />
              <sequenceFlow id="f1" sourceRef="t" targetRef="e" />
              <sequenceFlow id="f2" sourceRef="b" targetRef="caught" />
            </process>
          </definitions>"#;

    // when
    let err = parse_bpmn(xml).unwrap_err();

    // then
    assert!(matches!(err, ParseError::InvalidBoundaryEvent { .. }));
}

#[test]
fn should_parse_signal_intermediate_catch_and_boundary_events() {
    // given: a signal intermediate catch and a signal boundary on a task,
    // both referencing a definitions-level <signal>.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="await">
                <bpmn:signalEventDefinition signalRef="Signal_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:serviceTask id="work" />
              <bpmn:endEvent id="e" />
              <bpmn:boundaryEvent id="abort" attachedToRef="work">
                <bpmn:signalEventDefinition signalRef="Signal_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="aborted" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="work" />
              <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
              <bpmn:sequenceFlow id="f3" sourceRef="abort" targetRef="aborted" />
            </bpmn:process>
            <bpmn:signal id="Signal_1" name="all-clear" />
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("await").unwrap().kind,
        ElementKind::SignalIntermediateCatchEvent {
            signal_name: "all-clear".to_string(),
        }
    );
    assert_eq!(
        def.element("abort").unwrap().kind,
        ElementKind::SignalBoundaryEvent {
            attached_to: "work".to_string(),
            signal_name: "all-clear".to_string(),
            interrupting: true,
        }
    );
    assert!(def
        .element("abort")
        .unwrap()
        .outgoing
        .iter()
        .any(|f| f.to == "aborted"));
}

#[test]
fn should_parse_multi_instance_loop_characteristics() {
    // given: a service task carrying parallel multi-instance characteristics
    // with a zeebe:loopCharacteristics extension (input/output collection and
    // element) plus a completionCondition in the standard BPMN element text.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="each">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="handle" />
                </bpmn:extensionElements>
                <bpmn:multiInstanceLoopCharacteristics isSequential="true">
                  <bpmn:extensionElements>
                    <zeebe:loopCharacteristics
                        inputCollection="=items"
                        inputElement="item"
                        outputCollection="results"
                        outputElement="=item * 2" />
                  </bpmn:extensionElements>
                  <bpmn:completionCondition>=count(results) &gt;= 2</bpmn:completionCondition>
                </bpmn:multiInstanceLoopCharacteristics>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="each" />
              <bpmn:sequenceFlow id="f1" sourceRef="each" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    let mi = def
        .element("each")
        .unwrap()
        .multi_instance
        .as_ref()
        .expect("multi-instance characteristics parsed");
    assert_eq!(mi.input_collection, "=items");
    assert_eq!(mi.input_element.as_deref(), Some("item"));
    assert_eq!(mi.output_collection.as_deref(), Some("results"));
    assert_eq!(mi.output_element.as_deref(), Some("=item * 2"));
    assert_eq!(
        mi.completion_condition.as_deref(),
        Some("=count(results) >= 2")
    );
    assert!(mi.sequential, "isSequential=true parsed");
    // The task itself still routes to a job (taskDefinition preserved).
    assert!(matches!(
        def.element("each").unwrap().kind,
        ElementKind::ServiceTask { .. }
    ));
}

#[test]
fn should_parse_conditional_intermediate_catch_and_boundary_events() {
    // given: a conditional intermediate catch and a conditional boundary
    // (one interrupting, one not) whose FEEL condition lives in the nested
    // <condition> element text of a <conditionalEventDefinition>.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="gate">
                <bpmn:conditionalEventDefinition>
                  <bpmn:condition xsi:type="bpmn:tFormalExpression">=approved = true</bpmn:condition>
                </bpmn:conditionalEventDefinition>
              </bpmn:intermediateCatchEvent>
              <bpmn:serviceTask id="work" />
              <bpmn:endEvent id="e" />
              <bpmn:boundaryEvent id="bnd" attachedToRef="work" cancelActivity="false">
                <bpmn:conditionalEventDefinition>
                  <bpmn:condition>=ping = true</bpmn:condition>
                </bpmn:conditionalEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="pinged" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gate" />
              <bpmn:sequenceFlow id="f1" sourceRef="gate" targetRef="work" />
              <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="e" />
              <bpmn:sequenceFlow id="f3" sourceRef="bnd" targetRef="pinged" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("gate").unwrap().kind,
        ElementKind::ConditionalIntermediateCatchEvent {
            condition: "=approved = true".to_string(),
        }
    );
    assert_eq!(
        def.element("bnd").unwrap().kind,
        ElementKind::ConditionalBoundaryEvent {
            attached_to: "work".to_string(),
            condition: "=ping = true".to_string(),
            interrupting: false,
        }
    );
    assert!(def
        .element("bnd")
        .unwrap()
        .outgoing
        .iter()
        .any(|f| f.to == "pinged"));
}

#[test]
fn should_decode_numeric_character_references_in_a_correlation_key() {
    // Camunda Modeler emits `&#34;` (decimal) / `&#x22;` (hex) for the
    // double-quotes of a FEEL string literal placed in an attribute value.
    // The shared attribute-value decoder must expand those numeric character
    // references to `"` before the value reaches FEEL — identical to
    // `&quot;`. Regression guard for issue #885: previously the numeric forms
    // round-tripped undecoded, so the correlation FEEL reached FEEL as the raw
    // `=&#34;k&#34;` and either evaluated to `""` or raised `unexpected
    // character '&'`.
    for reference in ["&#34;", "&#x22;", "&#X22;", "&quot;"] {
        let xml = format!(
            r#"
              <bpmn:definitions
                  xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
                <bpmn:process id="p">
                  <bpmn:startEvent id="s" />
                  <bpmn:intermediateCatchEvent id="await">
                    <bpmn:messageEventDefinition messageRef="Message_1" />
                  </bpmn:intermediateCatchEvent>
                  <bpmn:endEvent id="e" />
                  <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
                  <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
                </bpmn:process>
                <bpmn:message id="Message_1" name="payment-received">
                  <bpmn:extensionElements>
                    <zeebe:subscription correlationKey="={ref}k{ref}" />
                  </bpmn:extensionElements>
                </bpmn:message>
              </bpmn:definitions>"#,
            ref = reference
        );

        let def = &parse_bpmn(&xml).unwrap()[0];

        assert_eq!(
            def.element("await").unwrap().kind,
            ElementKind::MessageIntermediateCatchEvent {
                message_name: "payment-received".to_string(),
                correlation_key: "\"k\"".to_string(),
            },
            "correlation key FEEL for reference {reference} should decode to \"k\""
        );
    }
}

#[test]
fn should_parse_a_message_intermediate_catch_event() {
    // given
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="await">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="payment-received">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("await").unwrap().kind,
        ElementKind::MessageIntermediateCatchEvent {
            message_name: "payment-received".to_string(),
            correlation_key: "orderId".to_string(),
        }
    );
}

#[test]
fn should_parse_io_mapping_on_an_intermediate_catch_event() {
    // given — a message catch event carrying a `zeebe:ioMapping` whose output
    // increments a loop counter when the event is triggered (the
    // urban-pr-review convergence loop's `=round + 1 -> round`).
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="await">
                <bpmn:extensionElements>
                  <zeebe:ioMapping>
                    <zeebe:input source="=round" target="prevRound" />
                    <zeebe:output source="=round + 1" target="round" />
                  </zeebe:ioMapping>
                </bpmn:extensionElements>
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <bpmn:sequenceFlow id="f1" sourceRef="await" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="review-ready">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=prKey" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then — the mapping attaches to the catch event (previously it was
    // dropped because the event was never pushed onto the io_stack).
    let io = &def.element("await").unwrap().io;
    assert_eq!(io.inputs.len(), 1);
    assert_eq!(io.inputs[0].source, "=round");
    assert_eq!(io.inputs[0].target, "prevRound");
    assert_eq!(io.outputs.len(), 1);
    assert_eq!(io.outputs[0].source, "=round + 1");
    assert_eq!(io.outputs[0].target, "round");
}

#[test]
fn end_event_io_mapping_attaches_to_the_end_event_not_the_enclosing_subprocess() {
    // given — a sub-process with two end events, each carrying a
    // `zeebe:output` mapping for the same target. Each mapping must attach to
    // its OWN end event; none may hoist onto the enclosing sub-process.
    // Previously end events were never pushed onto the io_stack, so every
    // nested end-event mapping fell through to the innermost open activity
    // (the sub-process). With several end events mapping the same target, the
    // sub-process then collected them all and the last-parsed one clobbered
    // the rest at completion — a "fixed" outcome routed as "escalate" in the
    // nano-workforce merge-loop (#466).
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
              <bpmn:subProcess id="sub">
                <bpmn:startEvent id="ss" />
                <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="endA" />
                <bpmn:endEvent id="endA">
                  <bpmn:extensionElements>
                    <zeebe:ioMapping>
                      <zeebe:output source="=&#34;A&#34;" target="outcome" />
                    </zeebe:ioMapping>
                  </bpmn:extensionElements>
                </bpmn:endEvent>
                <bpmn:endEvent id="endB">
                  <bpmn:extensionElements>
                    <zeebe:ioMapping>
                      <zeebe:output source="=&#34;B&#34;" target="outcome" />
                    </zeebe:ioMapping>
                  </bpmn:extensionElements>
                </bpmn:endEvent>
              </bpmn:subProcess>
              <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="done" />
              <bpmn:endEvent id="done" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then — each end event owns exactly its own mapping, and the sub-process
    // (and the mapping-free `done` end event) own none.
    assert!(
        def.element("sub").unwrap().io.outputs.is_empty(),
        "end-event mappings must not hoist onto the enclosing sub-process"
    );
    let end_a = &def.element("endA").unwrap().io.outputs;
    assert_eq!(end_a.len(), 1);
    assert_eq!(end_a[0].source, "=\"A\"");
    assert_eq!(end_a[0].target, "outcome");
    let end_b = &def.element("endB").unwrap().io.outputs;
    assert_eq!(end_b.len(), 1);
    assert_eq!(end_b[0].source, "=\"B\"");
    assert!(def.element("done").unwrap().io.outputs.is_empty());
}

#[test]
fn throw_event_io_mapping_attaches_to_the_throw_event_not_the_enclosing_subprocess() {
    // Same defect class as end events: a `zeebe:ioMapping` on a message
    // intermediate throw event nested in a sub-process must attach to the
    // throw event, not hoist onto the enclosing sub-process.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
              <bpmn:subProcess id="sub">
                <bpmn:startEvent id="ss" />
                <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="thr" />
                <bpmn:intermediateThrowEvent id="thr">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="notify" />
                    <zeebe:ioMapping>
                      <zeebe:output source="=&#34;sent&#34;" target="state" />
                    </zeebe:ioMapping>
                  </bpmn:extensionElements>
                  <bpmn:messageEventDefinition id="m" />
                </bpmn:intermediateThrowEvent>
                <bpmn:sequenceFlow id="f2" sourceRef="thr" targetRef="e" />
                <bpmn:endEvent id="e" />
              </bpmn:subProcess>
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    assert!(
        def.element("sub").unwrap().io.outputs.is_empty(),
        "throw-event mapping must not hoist onto the enclosing sub-process"
    );
    let thr = &def.element("thr").unwrap().io.outputs;
    assert_eq!(thr.len(), 1);
    assert_eq!(thr[0].source, "=\"sent\"");
    assert_eq!(thr[0].target, "state");
}

#[test]
fn should_not_misattribute_io_mapping_after_an_id_less_catch_event() {
    // given — an intermediate catch event with no `id` (so it is never
    // pushed onto the io_stack) followed by a service task carrying a
    // `zeebe:ioMapping`. A previously unconditional pop on
    // `</intermediateCatchEvent>` would underflow/detach the stack and
    // cause the service task's mapping to attach to the wrong node.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent>
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:intermediateCatchEvent>
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:ioMapping>
                    <zeebe:input source="=amount" target="chargeAmount" />
                    <zeebe:output source="=result" target="chargeResult" />
                  </zeebe:ioMapping>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="review-ready">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=prKey" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then — the mapping attaches to the service task, not a stray node.
    let io = &def.element("charge").unwrap().io;
    assert_eq!(io.inputs.len(), 1);
    assert_eq!(io.inputs[0].source, "=amount");
    assert_eq!(io.inputs[0].target, "chargeAmount");
    assert_eq!(io.outputs.len(), 1);
    assert_eq!(io.outputs[0].source, "=result");
    assert_eq!(io.outputs[0].target, "chargeResult");
}

#[test]
fn should_not_misattribute_io_mapping_after_an_id_less_activity() {
    // given — a sub-process (pushed onto the io_stack) whose first child is an
    // id-less `sendTask` (so `add_node` returns `None` and the task is never
    // pushed), followed by the sub-process's own `zeebe:ioMapping`. A
    // previously unconditional pop on `</sendTask>` would remove the enclosing
    // sub-process from the io_stack, so the sub-process's own output mapping
    // would fall onto a stray node (or be dropped). Same defect class as the
    // id-less event handlers — every leaf-activity close path must guard its
    // pop on having actually pushed.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="sub" />
              <bpmn:subProcess id="sub">
                <bpmn:sendTask></bpmn:sendTask>
                <bpmn:extensionElements>
                  <zeebe:ioMapping>
                    <zeebe:output source="=&#34;done&#34;" target="subOut" />
                  </zeebe:ioMapping>
                </bpmn:extensionElements>
                <bpmn:startEvent id="ss" />
                <bpmn:sequenceFlow id="f1" sourceRef="ss" targetRef="se" />
                <bpmn:endEvent id="se" />
              </bpmn:subProcess>
              <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
              <bpmn:endEvent id="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then — the mapping still attaches to the enclosing sub-process, because
    // the id-less send task never popped it off the io_stack.
    let io = &def.element("sub").unwrap().io;
    assert_eq!(
        io.outputs.len(),
        1,
        "sub-process must keep its own output mapping after an id-less child activity"
    );
    assert_eq!(io.outputs[0].source, "=\"done\"");
    assert_eq!(io.outputs[0].target, "subOut");
}

#[test]
fn should_parse_a_message_boundary_event() {
    // given
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge" />
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="cancel" attachedToRef="charge">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="aborted" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="cancel" targetRef="aborted" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="order-cancelled">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("cancel").unwrap().kind,
        ElementKind::MessageBoundaryEvent {
            attached_to: "charge".to_string(),
            message_name: "order-cancelled".to_string(),
            correlation_key: "orderId".to_string(),
            interrupting: true,
        }
    );
    let boundary = def.element("cancel").unwrap();
    assert!(boundary.outgoing.iter().any(|f| f.to == "aborted"));
}

#[test]
fn should_parse_non_interrupting_timer_and_message_boundary_events() {
    // given: a service task with a non-interrupting timer boundary and a
    // non-interrupting message boundary (both cancelActivity="false").
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge" />
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="remind" attachedToRef="charge" cancelActivity="false">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT5S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:boundaryEvent id="notify" attachedToRef="charge" cancelActivity="false">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="reminded" />
              <bpmn:endEvent id="notified" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="remind" targetRef="reminded" />
              <bpmn:sequenceFlow id="f3" sourceRef="notify" targetRef="notified" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="reminder">
              <bpmn:extensionElements>
                <zeebe:subscription correlationKey="=orderId" />
              </bpmn:extensionElements>
            </bpmn:message>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: both boundaries parse as non-interrupting.
    assert_eq!(
        def.element("remind").unwrap().kind,
        ElementKind::TimerBoundaryEvent {
            attached_to: "charge".to_string(),
            duration_millis: 5_000,
            interrupting: false,
            repeating: false,
        }
    );
    assert_eq!(
        def.element("notify").unwrap().kind,
        ElementKind::MessageBoundaryEvent {
            attached_to: "charge".to_string(),
            message_name: "reminder".to_string(),
            correlation_key: "orderId".to_string(),
            interrupting: false,
        }
    );
}

#[test]
fn should_parse_a_non_interrupting_cycle_timer_boundary_event() {
    // given: a service task with a non-interrupting timer boundary whose
    // timerEventDefinition carries a timeCycle (a repeating interval).
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge" />
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="tick" attachedToRef="charge" cancelActivity="false">
                <bpmn:timerEventDefinition>
                  <bpmn:timeCycle>R/PT5S</bpmn:timeCycle>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="ticked" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="tick" targetRef="ticked" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then: it is a non-interrupting, repeating timer boundary at 5s.
    assert_eq!(
        def.element("tick").unwrap().kind,
        ElementKind::TimerBoundaryEvent {
            attached_to: "charge".to_string(),
            duration_millis: 5_000,
            interrupting: false,
            repeating: true,
        }
    );
}

#[test]
fn should_reject_a_message_event_referencing_an_unknown_message() {
    // given
    let xml = r#"
          <definitions>
            <process id="p">
              <startEvent id="s" />
              <intermediateCatchEvent id="await">
                <messageEventDefinition messageRef="missing" />
              </intermediateCatchEvent>
              <endEvent id="e" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="await" />
              <sequenceFlow id="f1" sourceRef="await" targetRef="e" />
            </process>
          </definitions>"#;

    // when
    let err = parse_bpmn(xml).unwrap_err();

    // then
    assert!(matches!(err, ParseError::InvalidMessageEvent { .. }));
}

#[test]
fn should_parse_iso8601_cycles() {
    assert_eq!(parse_iso8601_cycle("R/PT10S"), Some(10_000));
    assert_eq!(parse_iso8601_cycle("R5/PT1H"), Some(3_600_000));
    assert_eq!(parse_iso8601_cycle(" R/PT1M30S "), Some(90_000));
    assert_eq!(parse_iso8601_cycle("PT10S"), Some(10_000));
    assert_eq!(parse_iso8601_cycle("PT10S/R"), None);
    assert_eq!(parse_iso8601_cycle("R/bogus"), None);
}

#[test]
fn should_parse_a_message_start_event() {
    // given
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="order-placed" />
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("s").unwrap().kind,
        ElementKind::MessageStartEvent {
            message_name: "order-placed".to_string(),
        }
    );
}

#[test]
fn should_parse_zeebe_properties_on_a_message_start_event() {
    // Camunda marks a BPMN process as an MCP tool via `zeebe:properties` /
    // `zeebe:property` extension elements on the message start event
    // (`io.camunda.tool:name`, `io.camunda.tool:purpose`,
    // `io.camunda.tool:input_N_*`, …). The parser must carry them onto the
    // element (`Element::properties`) rather than dropping them, so the engine
    // can project the tool metadata onto the message start subscription (#1499).
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:extensionElements>
                  <zeebe:properties>
                    <zeebe:property name="io.camunda.tool:name" value="searchFlights" />
                    <zeebe:property name="io.camunda.tool:purpose" value="Search for flights" />
                    <zeebe:property name="io.camunda.tool:input_1_name" value="destination" />
                    <zeebe:property name="io.camunda.tool:input_1_type" value="string" />
                    <zeebe:property name="io.camunda.tool:input_1_required" value="true" />
                  </zeebe:properties>
                </bpmn:extensionElements>
                <bpmn:messageEventDefinition messageRef="Message_1" />
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="search-flights" />
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    let start = def.element("s").unwrap();
    assert_eq!(
        start.kind,
        ElementKind::MessageStartEvent {
            message_name: "search-flights".to_string(),
        }
    );
    assert_eq!(
        start
            .properties
            .get("io.camunda.tool:name")
            .map(String::as_str),
        Some("searchFlights")
    );
    assert_eq!(
        start
            .properties
            .get("io.camunda.tool:purpose")
            .map(String::as_str),
        Some("Search for flights")
    );
    assert_eq!(
        start
            .properties
            .get("io.camunda.tool:input_1_name")
            .map(String::as_str),
        Some("destination")
    );
    assert_eq!(
        start
            .properties
            .get("io.camunda.tool:input_1_required")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(start.properties.len(), 5);
    // An element without extension properties carries an empty map.
    assert!(def.element("e").unwrap().properties.is_empty());
}

#[test]
fn should_parse_zeebe_properties_on_a_service_task() {
    // Extension properties are not start-event-specific: any element that
    // carries `zeebe:properties` keeps them (the IR must not be lossy).
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="payment" />
                  <zeebe:properties>
                    <zeebe:property name="inbound.type" value="webhook" />
                    <zeebe:property name="custom" value="" />
                  </zeebe:properties>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    let task = def.element("charge").unwrap();
    assert!(matches!(task.kind, ElementKind::ServiceTask { .. }));
    assert_eq!(
        task.properties.get("inbound.type").map(String::as_str),
        Some("webhook")
    );
    // A property with an empty value is retained (empty string, not dropped).
    assert_eq!(task.properties.get("custom").map(String::as_str), Some(""));
    assert_eq!(task.properties.len(), 2);
}

#[test]
fn should_attach_zeebe_properties_to_a_top_level_boundary_event() {
    // A boundary event is buffered outside the io_stack: its properties must
    // land on the boundary itself, not be dropped (#1499).
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="t">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="work" />
                  <zeebe:properties>
                    <zeebe:property name="owner" value="task" />
                  </zeebe:properties>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:boundaryEvent id="b" attachedToRef="t">
                <bpmn:extensionElements>
                  <zeebe:properties>
                    <zeebe:property name="owner" value="boundary" />
                  </zeebe:properties>
                </bpmn:extensionElements>
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT1M</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="e" />
              <bpmn:endEvent id="e2" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
              <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
              <bpmn:sequenceFlow id="f3" sourceRef="b" targetRef="e2" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    let boundary = def.element("b").unwrap();
    assert_eq!(
        boundary.properties.get("owner").map(String::as_str),
        Some("boundary")
    );
    let task = def.element("t").unwrap();
    assert_eq!(
        task.properties.get("owner").map(String::as_str),
        Some("task")
    );
}

#[test]
fn should_not_attach_nested_boundary_event_properties_to_the_enclosing_subprocess() {
    // A boundary nested in a subprocess: with the subprocess on the io_stack,
    // its properties must not be attached to (and overwrite) the subprocess's.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:subProcess id="sub">
                <bpmn:extensionElements>
                  <zeebe:properties>
                    <zeebe:property name="owner" value="sub" />
                  </zeebe:properties>
                </bpmn:extensionElements>
                <bpmn:startEvent id="ss" />
                <bpmn:serviceTask id="t">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="work" />
                  </bpmn:extensionElements>
                </bpmn:serviceTask>
                <bpmn:boundaryEvent id="b" attachedToRef="t">
                  <bpmn:extensionElements>
                    <zeebe:properties>
                      <zeebe:property name="owner" value="boundary" />
                    </zeebe:properties>
                  </bpmn:extensionElements>
                  <bpmn:timerEventDefinition>
                    <bpmn:timeDuration>PT1M</bpmn:timeDuration>
                  </bpmn:timerEventDefinition>
                </bpmn:boundaryEvent>
                <bpmn:endEvent id="se" />
                <bpmn:endEvent id="se2" />
                <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="t" />
                <bpmn:sequenceFlow id="sf2" sourceRef="t" targetRef="se" />
                <bpmn:sequenceFlow id="sf3" sourceRef="b" targetRef="se2" />
              </bpmn:subProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
              <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    let sub = def.element("sub").unwrap();
    assert_eq!(sub.properties.get("owner").map(String::as_str), Some("sub"));
    let boundary = def.element("b").unwrap();
    assert_eq!(
        boundary.properties.get("owner").map(String::as_str),
        Some("boundary")
    );
}

#[test]
fn should_parse_the_camunda_mcp_fixture_tool_markers() {
    // Round-trip the vendored Camunda e2e fixture
    // (`camunda/camunda` qa/c8-orchestration-cluster-e2e-test-suite): its tool
    // markers must survive parsing onto the message start event.
    let xml = include_str!("../../tests/fixtures/mcp-process-with-inputs.bpmn");

    let def = &parse_bpmn(xml).unwrap()[0];

    assert_eq!(def.id, "mcpProcessWithInputs");
    let start = def.element("StartEvent_1").unwrap();
    assert_eq!(
        start.kind,
        ElementKind::MessageStartEvent {
            message_name: "mcp-process-with-inputs".to_string(),
        }
    );
    let expected: [(&str, &str); 9] = [
        ("io.camunda.tool:name", "mcp-process-with-inputs"),
        (
            "io.camunda.tool:purpose",
            "Tool for executing a process with typed inputs triggered by MCP.",
        ),
        ("io.camunda.tool:input_1_name", "firstName"),
        ("io.camunda.tool:input_1_type", "string"),
        (
            "io.camunda.tool:input_1_description",
            "The first name of the person.",
        ),
        ("io.camunda.tool:input_2_name", "amount"),
        ("io.camunda.tool:input_2_type", "integer"),
        (
            "io.camunda.tool:input_2_description",
            "The amount to process.",
        ),
        ("io.camunda.tool:input_2_required", "true"),
    ];
    assert_eq!(start.properties.len(), expected.len());
    for (key, value) in expected {
        assert_eq!(
            start.properties.get(key).map(String::as_str),
            Some(value),
            "tool property {key}"
        );
    }
}

#[test]
fn should_parse_a_one_shot_timer_start_event() {
    // given
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT10S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("s").unwrap().kind,
        ElementKind::TimerStartEvent {
            interval_millis: 10_000,
            repeating: false,
        }
    );
}

#[test]
fn should_parse_a_recurring_timer_start_event() {
    // given
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s">
                <bpmn:timerEventDefinition>
                  <bpmn:timeCycle>R/PT1H</bpmn:timeCycle>
                </bpmn:timerEventDefinition>
              </bpmn:startEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then
    assert_eq!(
        def.element("s").unwrap().kind,
        ElementKind::TimerStartEvent {
            interval_millis: 3_600_000,
            repeating: true,
        }
    );
}

#[test]
fn none_plus_message_start_keeps_the_message_start_typed() {
    // Regression guard (#855): a none start alongside a message start must
    // keep the message start as a MessageStartEvent — NOT demote it to an
    // inert throw event — so deploy can open its subscription. The none start
    // remains the process-entry start.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="none_s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:startEvent id="msg_s">
                <bpmn:messageEventDefinition messageRef="Message_1" />
                <bpmn:outgoing>f2</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="none_s" targetRef="e1" />
              <bpmn:sequenceFlow id="f2" sourceRef="msg_s" targetRef="e2" />
            </bpmn:process>
            <bpmn:message id="Message_1" name="order-placed" />
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(def.start_event, "none_s", "none start is the process entry");
    assert_eq!(
        def.element("msg_s").unwrap().kind,
        ElementKind::MessageStartEvent {
            message_name: "order-placed".to_string(),
        },
        "surplus message start must survive un-demoted"
    );
}

#[test]
fn none_plus_timer_start_keeps_the_timer_start_typed() {
    // Regression guard (#855): a surplus timer start must survive as a
    // TimerStartEvent so deploy can arm its process-level timer.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="none_s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:startEvent id="timer_s">
                <bpmn:timerEventDefinition><bpmn:timeDuration>PT10S</bpmn:timeDuration></bpmn:timerEventDefinition>
                <bpmn:outgoing>f2</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="none_s" targetRef="e1" />
              <bpmn:sequenceFlow id="f2" sourceRef="timer_s" targetRef="e2" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(def.start_event, "none_s");
    assert_eq!(
        def.element("timer_s").unwrap().kind,
        ElementKind::TimerStartEvent {
            interval_millis: 10_000,
            repeating: false,
        },
        "surplus timer start must survive un-demoted"
    );
}

#[test]
fn none_plus_signal_start_still_demotes_the_signal_start() {
    // Nano has no dedicated signal-start element kind, so a surplus signal
    // start is still demoted to an inert throw event: keeping it would make
    // it a second `ElementKind::StartEvent`, wrongly counted as a second
    // *none* start by the start-events validator (#855).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="none_s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:startEvent id="signal_s">
                <bpmn:signalEventDefinition signalRef="Signal_1" />
                <bpmn:outgoing>f2</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="none_s" targetRef="e1" />
              <bpmn:sequenceFlow id="f2" sourceRef="signal_s" targetRef="e2" />
            </bpmn:process>
            <bpmn:signal id="Signal_1" name="go" />
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(def.start_event, "none_s");
    assert_eq!(
        def.element("signal_s").unwrap().kind,
        ElementKind::IntermediateThrowEvent,
        "surplus signal start is demoted (no dedicated signal-start kind)"
    );
}

#[test]
fn execution_listener_on_demoted_signal_start_is_rejected() {
    // A surplus signal start is demoted to an inert `IntermediateThrowEvent`
    // (no incoming flow, never activated/completed), so an execution listener
    // on it could never fire. Reject it at deploy rather than silently drop the
    // dead listener during demotion — the reject-don't-drop contract (#1197).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="none_s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
              <bpmn:startEvent id="signal_s">
                <bpmn:extensionElements>
                  <zeebe:executionListeners>
                    <zeebe:executionListener eventType="start" type="ping" />
                  </zeebe:executionListeners>
                </bpmn:extensionElements>
                <bpmn:signalEventDefinition signalRef="Signal_1" />
                <bpmn:outgoing>f2</bpmn:outgoing>
              </bpmn:startEvent>
              <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
              <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="none_s" targetRef="e1" />
              <bpmn:sequenceFlow id="f2" sourceRef="signal_s" targetRef="e2" />
            </bpmn:process>
            <bpmn:signal id="Signal_1" name="go" />
          </bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedExecutionListener { element_id, .. }) => {
            assert_eq!(element_id, "signal_s");
        }
        other => panic!(
            "expected UnsupportedExecutionListener for a demoted signal start, got {other:?}"
        ),
    }
}

#[test]
fn should_parse_link_throw_and_catch_events() {
    // A linkEventDefinition on an intermediateThrowEvent / intermediateCatchEvent
    // parses to the dedicated link kinds (#1157), preserving the link name —
    // not to a plain throw or a timer catch.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s"><bpmn:outgoing>f0</bpmn:outgoing></bpmn:startEvent>
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="throw" />
              <bpmn:intermediateThrowEvent id="throw">
                <bpmn:incoming>f0</bpmn:incoming>
                <bpmn:linkEventDefinition name="hop" />
              </bpmn:intermediateThrowEvent>
              <bpmn:intermediateCatchEvent id="catch">
                <bpmn:outgoing>f1</bpmn:outgoing>
                <bpmn:linkEventDefinition name="hop" />
              </bpmn:intermediateCatchEvent>
              <bpmn:sequenceFlow id="f1" sourceRef="catch" targetRef="e" />
              <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("throw").unwrap().kind,
        ElementKind::LinkIntermediateThrowEvent {
            link_name: "hop".to_string()
        }
    );
    assert_eq!(
        def.element("catch").unwrap().kind,
        ElementKind::LinkIntermediateCatchEvent {
            link_name: "hop".to_string()
        }
    );
    // The throw has no outgoing flow; the catch has no incoming flow.
    assert!(def.element("throw").unwrap().outgoing.is_empty());
    assert_eq!(def.incoming_count("catch"), 0);
}

#[test]
fn should_parse_an_embedded_subprocess_with_an_error_boundary() {
    // given a process whose embedded sub-process has its own start/task/end
    // and an interrupting error boundary attached to the sub-process.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="start" />
              <bpmn:subProcess id="sub">
                <bpmn:startEvent id="sub_start" />
                <bpmn:serviceTask id="inner">
                  <bpmn:extensionElements>
                    <zeebe:taskDefinition type="work" />
                  </bpmn:extensionElements>
                </bpmn:serviceTask>
                <bpmn:endEvent id="sub_end" />
                <bpmn:sequenceFlow id="i0" sourceRef="sub_start" targetRef="inner" />
                <bpmn:sequenceFlow id="i1" sourceRef="inner" targetRef="sub_end" />
              </bpmn:subProcess>
              <bpmn:boundaryEvent id="boundary" attachedToRef="sub">
                <bpmn:errorEventDefinition errorRef="Error_1" />
              </bpmn:boundaryEvent>
              <bpmn:serviceTask id="sad">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="sad-flow" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:endEvent id="sad_end" />
              <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="sub" />
              <bpmn:sequenceFlow id="f1" sourceRef="sub" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="boundary" targetRef="sad" />
              <bpmn:sequenceFlow id="f3" sourceRef="sad" targetRef="sad_end" />
            </bpmn:process>
            <bpmn:error id="Error_1" name="Business" errorCode="BUSINESS_ERROR" />
          </bpmn:definitions>"#;

    // when
    let def = &parse_bpmn(xml).unwrap()[0];

    // then the process-level start event is the outer one, and the
    // sub-process points at its inner start.
    assert_eq!(def.start_event, "start");
    assert_eq!(
        def.element("sub").unwrap().kind,
        ElementKind::SubProcess {
            start_event: "sub_start".to_string(),
        }
    );
    // The inner nodes are tagged as contained in the sub-process; the outer
    // ones are not.
    assert_eq!(def.element("inner").unwrap().parent.as_deref(), Some("sub"));
    assert_eq!(
        def.element("sub_start").unwrap().parent.as_deref(),
        Some("sub")
    );
    assert_eq!(def.element("sub").unwrap().parent, None);
    assert_eq!(def.element("start").unwrap().parent, None);
    // The error boundary is attached to the sub-process and routes to sad-flow.
    assert_eq!(
        def.element("boundary").unwrap().kind,
        ElementKind::ErrorBoundaryEvent {
            attached_to: "sub".to_string(),
            error_code: "BUSINESS_ERROR".to_string(),
        }
    );
    assert!(def
        .element("sub")
        .unwrap()
        .outgoing
        .iter()
        .any(|f| f.to == "done"));
}

#[test]
fn should_parse_call_activities_in_both_camunda_7_and_zeebe_forms() {
    // Two call activities: one with a `calledElement` attribute (Camunda 7),
    // one with a nested `zeebe:calledElement processId` (Camunda 8/Zeebe).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="orch">
              <bpmn:startEvent id="start" />
              <bpmn:callActivity id="c1" calledElement="Phase01" />
              <bpmn:callActivity id="c2">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="Phase02" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:endEvent id="end" />
              <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="c1" />
              <bpmn:sequenceFlow id="f1" sourceRef="c1" targetRef="c2" />
              <bpmn:sequenceFlow id="f2" sourceRef="c2" targetRef="end" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(
        def.element("c1").unwrap().kind,
        ElementKind::CallActivity {
            called_process_id: "Phase01".to_string(),
            propagate_all_parent_variables: true,
            propagate_all_child_variables: true,
        }
    );
    assert_eq!(
        def.element("c2").unwrap().kind,
        ElementKind::CallActivity {
            called_process_id: "Phase02".to_string(),
            propagate_all_parent_variables: true,
            propagate_all_child_variables: true,
        }
    );
    // The call activity's outgoing flow is preserved for inline expansion.
    assert!(def
        .element("c1")
        .unwrap()
        .outgoing
        .iter()
        .any(|f| f.to == "c2"));
}

#[test]
fn should_parse_call_activity_variable_propagation_flags() {
    // Guard the silent-drop failure mode: the Zeebe
    // propagateAllParentVariables / propagateAllChildVariables attributes on
    // `zeebe:calledElement` must be captured (absent ⇒ true, explicit
    // `="false"` honored), not dropped on the floor.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="orch">
              <bpmn:startEvent id="start" />
              <bpmn:callActivity id="c_default">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:callActivity id="c_no_parent">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P" propagateAllParentVariables="false" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:callActivity id="c_no_child">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P" propagateAllChildVariables="false" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:callActivity id="c_both_false">
                <bpmn:extensionElements>
                  <zeebe:calledElement processId="P"
                                       propagateAllParentVariables="false"
                                       propagateAllChildVariables="false" />
                </bpmn:extensionElements>
              </bpmn:callActivity>
              <bpmn:endEvent id="end" />
            </bpmn:process>
          </bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let flags = |id: &str| match &def.element(id).unwrap().kind {
        ElementKind::CallActivity {
            propagate_all_parent_variables,
            propagate_all_child_variables,
            ..
        } => (
            *propagate_all_parent_variables,
            *propagate_all_child_variables,
        ),
        other => panic!("{id} should be a call activity, got {other:?}"),
    };
    assert_eq!(flags("c_default"), (true, true), "absent ⇒ both true");
    assert_eq!(flags("c_no_parent"), (false, true));
    assert_eq!(flags("c_no_child"), (true, false));
    assert_eq!(flags("c_both_false"), (false, false));
}

#[test]
fn a_call_activity_without_a_callee_is_a_parse_error() {
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="orch">
              <bpmn:startEvent id="start" />
              <bpmn:callActivity id="c1" />
              <bpmn:endEvent id="end" />
              <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="c1" />
              <bpmn:sequenceFlow id="f1" sourceRef="c1" targetRef="end" />
            </bpmn:process>
          </bpmn:definitions>"#;
    assert!(parse_bpmn(xml).is_err());
}

#[test]
fn parses_zeebe_io_mapping_inputs_and_outputs() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
        <zeebe:ioMapping>
          <zeebe:input source="=x + 1" target="y" />
          <zeebe:input source="=a" target="order.id" />
          <zeebe:output source="=result" target="approved" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let io = &def.element("t").unwrap().io;
    assert_eq!(io.inputs.len(), 2);
    assert_eq!(io.inputs[0].source, "=x + 1");
    assert_eq!(io.inputs[0].target, "y");
    assert_eq!(io.inputs[1].target, "order.id");
    assert_eq!(io.outputs.len(), 1);
    assert_eq!(io.outputs[0].source, "=result");
    assert_eq!(io.outputs[0].target, "approved");
}

#[test]
fn io_mapping_is_scoped_to_its_own_activity() {
    // Two service tasks each with their own ioMapping; the mappings must not
    // bleed across activities.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t1">
      <bpmn:extensionElements>
        <zeebe:ioMapping><zeebe:input source="=1" target="one" /></zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="t2">
      <bpmn:extensionElements>
        <zeebe:ioMapping><zeebe:output source="=2" target="two" /></zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t1" />
    <bpmn:sequenceFlow id="f2" sourceRef="t1" targetRef="t2" />
    <bpmn:sequenceFlow id="f3" sourceRef="t2" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let io1 = &def.element("t1").unwrap().io;
    assert_eq!(io1.inputs.len(), 1);
    assert!(io1.outputs.is_empty());
    let io2 = &def.element("t2").unwrap().io;
    assert!(io2.inputs.is_empty());
    assert_eq!(io2.outputs.len(), 1);
}

#[test]
fn parses_zeebe_execution_listeners() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="start-1" />
          <zeebe:executionListener eventType="start" type="start-2" retries="5" />
          <zeebe:executionListener eventType="end" type="end-1" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let el = def.element("t").unwrap();
    assert_eq!(el.start_listeners.len(), 2);
    assert_eq!(el.start_listeners[0].job_type, "start-1");
    assert_eq!(el.start_listeners[0].retries, None);
    assert_eq!(el.start_listeners[1].job_type, "start-2");
    assert_eq!(el.start_listeners[1].retries.as_deref(), Some("5"));
    assert_eq!(el.end_listeners.len(), 1);
    assert_eq!(el.end_listeners[0].job_type, "end-1");
    // A listener-free element carries empty lists.
    assert!(def.element("s").unwrap().start_listeners.is_empty());
    assert!(def.element("s").unwrap().end_listeners.is_empty());
}

#[test]
fn execution_listener_event_type_defaults_to_start() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
        <zeebe:executionListeners>
          <zeebe:executionListener type="only" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let el = def.element("t").unwrap();
    assert_eq!(el.start_listeners.len(), 1);
    assert!(el.end_listeners.is_empty());
}

#[test]
fn parses_execution_listeners_on_gateways() {
    // #1197: start/end execution listeners on every gateway flavour must
    // attach to the gateway itself, not be dropped.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:exclusiveGateway id="xor">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="xor-start" />
          <zeebe:executionListener eventType="end" type="xor-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:exclusiveGateway>
    <bpmn:parallelGateway id="and">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="and-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:inclusiveGateway id="or">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="or-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:inclusiveGateway>
    <bpmn:eventBasedGateway id="evt">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener type="evt-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:eventBasedGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="xor" />
    <bpmn:sequenceFlow id="f2" sourceRef="xor" targetRef="and" />
    <bpmn:sequenceFlow id="f3" sourceRef="and" targetRef="or" />
    <bpmn:sequenceFlow id="f4" sourceRef="or" targetRef="evt" />
    <bpmn:sequenceFlow id="f5" sourceRef="evt" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let xor = def.element("xor").unwrap();
    assert_eq!(xor.start_listeners.len(), 1);
    assert_eq!(xor.start_listeners[0].job_type, "xor-start");
    assert_eq!(xor.end_listeners.len(), 1);
    assert_eq!(xor.end_listeners[0].job_type, "xor-end");
    let and = def.element("and").unwrap();
    assert_eq!(and.start_listeners.len(), 1);
    assert_eq!(and.start_listeners[0].job_type, "and-start");
    assert!(and.end_listeners.is_empty());
    let or = def.element("or").unwrap();
    assert!(or.start_listeners.is_empty());
    assert_eq!(or.end_listeners.len(), 1);
    assert_eq!(or.end_listeners[0].job_type, "or-end");
    let evt = def.element("evt").unwrap();
    assert_eq!(evt.start_listeners.len(), 1);
    assert_eq!(evt.start_listeners[0].job_type, "evt-start");
}

#[test]
fn parses_execution_listeners_on_start_event() {
    // #1197: a start event's execution listeners must attach to the start
    // event, not fall through to the enclosing scope.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="s-start" />
          <zeebe:executionListener eventType="end" type="s-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let s = def.element("s").unwrap();
    assert_eq!(s.start_listeners.len(), 1);
    assert_eq!(s.start_listeners[0].job_type, "s-start");
    assert_eq!(s.end_listeners.len(), 1);
    assert_eq!(s.end_listeners[0].job_type, "s-end");
}

#[test]
fn parses_execution_listeners_on_boundary_event() {
    // #1197: a boundary event's execution listeners must attach to the
    // boundary event, not to the activity it is attached to.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="b" attachedToRef="t">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="b-start" />
          <zeebe:executionListener eventType="end" type="b-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1M</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="e" />
    <bpmn:endEvent id="eb" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
    <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
    <bpmn:sequenceFlow id="f3" sourceRef="b" targetRef="eb" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    let b = def.element("b").unwrap();
    assert_eq!(b.start_listeners.len(), 1);
    assert_eq!(b.start_listeners[0].job_type, "b-start");
    assert_eq!(b.end_listeners.len(), 1);
    assert_eq!(b.end_listeners[0].job_type, "b-end");
    // The host activity must NOT have inherited the boundary's listeners.
    let t = def.element("t").unwrap();
    assert!(t.start_listeners.is_empty());
    assert!(t.end_listeners.is_empty());
}

#[test]
fn self_closing_boundary_does_not_capture_next_siblings_listener() {
    // #1197 regression: a self-closing `<boundaryEvent/>` emits no matching
    // `Token::End`, so it must NOT open the pending-boundary buffer — otherwise
    // that stale buffer stays live and swallows the NEXT sibling's
    // `zeebe:executionListener` (mis-attaching it, and letting a following
    // sequence-flow listener bypass its intended rejection). Here a
    // self-closing boundary precedes a listener-bearing service task: the task
    // must own its listener and the task's parse must succeed.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="host">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="host-work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="b" attachedToRef="host" />
    <bpmn:serviceTask id="next">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="next-work" />
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="next-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="host" />
    <bpmn:sequenceFlow id="f2" sourceRef="host" targetRef="next" />
    <bpmn:sequenceFlow id="f3" sourceRef="next" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).expect("self-closing boundary must not break parse")[0];
    let next = def.element("next").expect("next task present");
    assert_eq!(
        next.start_listeners.len(),
        1,
        "the sibling task must own its own listener"
    );
    assert_eq!(next.start_listeners[0].job_type, "next-start");
}

#[test]
fn receive_task_execution_listener_attaches_to_itself() {
    // #1197 regression: a `receiveTask` is modelled as a pass-through, but it
    // must push onto the io_stack like the other plain tasks so a
    // `zeebe:executionListener` declared on it lands on the RECEIVE TASK —
    // not fall through to `io_stack.last()` and get dropped at process root
    // or hoisted onto the enclosing sub-process (the silent mis-attachment
    // class this PR closes). Here the listener is declared inside a receive
    // task nested in a sub-process: it must own its listener, and the
    // enclosing sub-process must NOT.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="ss" />
      <bpmn:receiveTask id="rt">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="rt-start" />
            <zeebe:executionListener eventType="end" type="rt-end" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:receiveTask>
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="a" sourceRef="ss" targetRef="rt" />
      <bpmn:sequenceFlow id="b" sourceRef="rt" targetRef="se" />
    </bpmn:subProcess>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).expect("receive-task listener must parse")[0];
    let rt = def.element("rt").expect("receive task present");
    assert_eq!(
        rt.start_listeners.len(),
        1,
        "start listener on the receive task"
    );
    assert_eq!(rt.start_listeners[0].job_type, "rt-start");
    assert_eq!(
        rt.end_listeners.len(),
        1,
        "end listener on the receive task"
    );
    assert_eq!(rt.end_listeners[0].job_type, "rt-end");
    let sub = def.element("sub").expect("sub-process present");
    assert!(
        sub.start_listeners.is_empty() && sub.end_listeners.is_empty(),
        "the receive task's listeners must NOT hoist onto the enclosing sub-process"
    );
}

#[test]
fn task_listener_on_non_user_task_is_rejected() {
    // #1197 reject-don't-drop: a `zeebe:taskListeners` declaration only runs
    // on a **user task** — task-listener jobs are created solely on the
    // user-task runtime path. Since a `receiveTask` now rides the `io_stack`
    // (so its *execution* listeners attach to itself), a `zeebe:taskListeners`
    // declared on it would attach to the pass-through receive task and could
    // never create a job. Accepting one would deploy a dead task listener, so
    // the deploy is rejected instead — naming the offending element.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:receiveTask id="rt">
      <bpmn:extensionElements>
        <zeebe:taskListeners>
          <zeebe:taskListener eventType="creating" type="rt-creating" />
        </zeebe:taskListeners>
      </bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="rt" />
    <bpmn:sequenceFlow id="f2" sourceRef="rt" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedTaskListener { element_id, .. }) => {
            assert_eq!(element_id, "rt");
        }
        other => {
            panic!("expected UnsupportedTaskListener for a receive task, got {other:?}")
        }
    }
}

#[test]
fn task_listener_on_user_task_is_accepted() {
    // Companion to `task_listener_on_non_user_task_is_rejected`: the supported
    // placement (a `zeebe:taskListeners` on a real `userTask`) must still be
    // accepted and attach to that user task.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:userTask id="ut">
      <bpmn:extensionElements>
        <zeebe:userTask />
        <zeebe:taskListeners>
          <zeebe:taskListener eventType="creating" type="ut-creating" />
        </zeebe:taskListeners>
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="ut" />
    <bpmn:sequenceFlow id="f2" sourceRef="ut" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).expect("user-task task listener must parse")[0];
    let ut = def.element("ut").expect("user task present");
    assert_eq!(ut.task_listeners.len(), 1, "task listener on the user task");
    assert_eq!(ut.task_listeners[0].job_type, "ut-creating");
}

#[test]
fn task_listener_on_boundary_event_is_rejected() {
    // #1197: a task listener only runs on a user task, and a boundary event is
    // never a user task. A boundary event is buffered in `cur_boundary` and
    // never enters the `io_stack`, so routing a task listener declared on it
    // through `io_stack.last()` would drop it (top-level host) or hoist it onto
    // the enclosing sub-process (mis-reporting the owner). It must be rejected
    // at deploy, naming the boundary event itself — mirroring the
    // execution-listener boundary handling.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="svc">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="bnd" attachedToRef="svc">
      <bpmn:extensionElements>
        <zeebe:taskListeners>
          <zeebe:taskListener eventType="creating" type="bnd-creating" />
        </zeebe:taskListeners>
      </bpmn:extensionElements>
      <bpmn:timerEventDefinition>
        <bpmn:timeDuration>PT1M</bpmn:timeDuration>
      </bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="svc" />
    <bpmn:sequenceFlow id="f2" sourceRef="svc" targetRef="end" />
    <bpmn:sequenceFlow id="f3" sourceRef="bnd" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedTaskListener { element_id, .. }) => {
            assert_eq!(
                element_id, "bnd",
                "the boundary event is named as the owner"
            );
        }
        other => panic!("expected UnsupportedTaskListener for a boundary event, got {other:?}"),
    }
}

#[test]
fn task_listener_on_sequence_flow_is_rejected() {
    // #1197: a sequence flow is an edge, not a user task, and never enters the
    // `io_stack`. A task listener declared on it must be rejected at deploy
    // (naming the flow) rather than dropped or hoisted onto a surrounding
    // element — mirroring the execution-listener sequence-flow handling.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="end">
      <bpmn:extensionElements>
        <zeebe:taskListeners>
          <zeebe:taskListener eventType="creating" type="f1-creating" />
        </zeebe:taskListeners>
      </bpmn:extensionElements>
    </bpmn:sequenceFlow>
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedTaskListener { element_id, .. }) => {
            assert_eq!(element_id, "f1", "the sequence flow is named as the owner");
        }
        other => {
            panic!("expected UnsupportedTaskListener for a sequence flow, got {other:?}")
        }
    }
}

#[test]
fn task_listener_on_adhoc_user_task_tool_is_rejected() {
    // #1197: a user-task *tool* of an ad-hoc sub-process is `NodeKind::User`,
    // so it slips past the node-based non-user-task scan — but the tool is
    // flattened into the non-executable ad-hoc catalog and activated through a
    // direct path that emits `UserTaskCreated` without running task listeners.
    // The task listener could never fire, so the deploy is rejected — the
    // task-listener analogue of `execution_listener_on_adhoc_tool_is_rejected`.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent-worker" />
        <zeebe:adHoc outputCollection="results" outputElement="=result" />
      </bpmn:extensionElements>
      <bpmn:userTask id="toolU">
        <bpmn:extensionElements>
          <zeebe:userTask />
          <zeebe:taskListeners>
            <zeebe:taskListener eventType="creating" type="toolU-creating" />
          </zeebe:taskListeners>
        </bpmn:extensionElements>
      </bpmn:userTask>
    </bpmn:adHocSubProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedTaskListener { element_id, .. }) => {
            assert_eq!(element_id, "toolU", "the ad-hoc user-task tool is named");
        }
        other => {
            panic!("expected UnsupportedTaskListener for an ad-hoc user-task tool, got {other:?}")
        }
    }
}

#[test]
fn self_closing_task_listeners_does_not_leak_to_later_element() {
    // #1197 mirror of the execution-listener self-closing guard: a self-closing
    // `<zeebe:taskListeners />` emits no end tag, so the `in_task_listeners`
    // flag must NOT stay stuck `true` and capture a later stray
    // `zeebe:taskListener` onto a subsequent element. Here the stray listener
    // sits *outside* any container on a service task — if the flag leaked it
    // would attach (or wrongly reject); with the guard it is ignored and the
    // model deploys clean.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:userTask id="ut">
      <bpmn:extensionElements>
        <zeebe:userTask />
        <zeebe:taskListeners />
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="svc">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="work" />
        <zeebe:taskListener eventType="creating" type="stray" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="ut" />
    <bpmn:sequenceFlow id="f2" sourceRef="ut" targetRef="svc" />
    <bpmn:sequenceFlow id="f3" sourceRef="svc" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).expect("self-closing taskListeners must deploy clean")[0];
    assert!(
        def.element("ut")
            .expect("user task present")
            .task_listeners
            .is_empty(),
        "the self-closing container declares no listeners"
    );
    assert!(
        def.element("svc")
            .expect("service task present")
            .task_listeners
            .is_empty(),
        "the stray taskListener must not leak onto the later service task"
    );
}

#[test]
fn execution_listener_at_process_level_is_rejected() {
    // #1197 reject-don't-drop: a `zeebe:executionListener` declared under the
    // `<process>`'s own extension elements has no open flow-node owner on the
    // `io_stack` (the process itself never rides it). Before the fix the
    // fallback `io_stack.last()` yielded `None` and the listener was silently
    // discarded, so the deploy succeeded with a dead listener. It must now be
    // rejected, naming the process as the owner.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="proc" isExecutable="true">
    <bpmn:extensionElements>
      <zeebe:executionListeners>
        <zeebe:executionListener eventType="start" type="proc-start" />
      </zeebe:executionListeners>
    </bpmn:extensionElements>
    <bpmn:startEvent id="start" />
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedExecutionListener {
            element_id,
            process_id,
            ..
        }) => {
            assert_eq!(element_id, "proc", "the process is named as the owner");
            assert_eq!(process_id, "proc");
        }
        other => panic!(
            "expected UnsupportedExecutionListener for a process-level listener, got {other:?}"
        ),
    }
}

#[test]
fn task_listener_at_process_level_is_rejected() {
    // #1197 reject-don't-drop, task-listener analogue: a `zeebe:taskListener`
    // declared under the `<process>`'s own extension elements has no open
    // flow-node owner on the `io_stack`. Before the fix the `io_stack.last()`
    // guard was `None`, so the listener was silently dropped and the deploy
    // succeeded. A process is not a user task, so it must be rejected, naming
    // the process as the owner — mirroring the execution-listener guard.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="proc" isExecutable="true">
    <bpmn:extensionElements>
      <zeebe:taskListeners>
        <zeebe:taskListener eventType="creating" type="proc-creating" />
      </zeebe:taskListeners>
    </bpmn:extensionElements>
    <bpmn:startEvent id="start" />
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedTaskListener {
            element_id,
            process_id,
            ..
        }) => {
            assert_eq!(element_id, "proc", "the process is named as the owner");
            assert_eq!(process_id, "proc");
        }
        other => {
            panic!("expected UnsupportedTaskListener for a process-level listener, got {other:?}")
        }
    }
}

#[test]
fn execution_listener_on_adhoc_tool_is_rejected() {
    // #1197 reject-don't-drop: an execution listener on a *tool* of an ad-hoc
    // sub-process cannot fire — a leaf tool is pruned into the non-executable
    // catalog and a retained embedded tool is activated/completed with direct
    // lifecycle events, both bypassing the listener gate. Accepting one would
    // deploy a dead listener, so the deploy is rejected instead.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent-worker" />
        <zeebe:adHoc outputCollection="results" outputElement="=result" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="toolA">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="tool" />
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="toolA-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:serviceTask>
    </bpmn:adHocSubProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedExecutionListener { element_id, .. }) => {
            assert_eq!(element_id, "toolA");
        }
        other => {
            panic!("expected UnsupportedExecutionListener for an ad-hoc tool, got {other:?}")
        }
    }
}

#[test]
fn execution_listener_on_boundary_of_adhoc_tool_is_rejected() {
    // #1197 reject-don't-drop, boundary analogue: a listener on a boundary
    // event attached to a *tool* of an ad-hoc sub-process cannot fire — a leaf
    // tool's boundary is pruned (its `attached_to` is removed) before the
    // boundary listeners are collected, so the listener would silently vanish.
    // The deploy is rejected instead of accepting a dead listener.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:adHocSubProcess id="agent">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="agent-worker" />
        <zeebe:adHoc outputCollection="results" outputElement="=result" />
      </bpmn:extensionElements>
      <bpmn:serviceTask id="toolA">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="tool" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:boundaryEvent id="tb" attachedToRef="toolA">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="end" type="tb-end" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
        <bpmn:timerEventDefinition><bpmn:timeDuration>PT1M</bpmn:timeDuration></bpmn:timerEventDefinition>
      </bpmn:boundaryEvent>
    </bpmn:adHocSubProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
    <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    match parse_bpmn(xml) {
        Err(ParseError::UnsupportedExecutionListener { element_id, .. }) => {
            assert_eq!(element_id, "tb");
        }
        other => panic!(
            "expected UnsupportedExecutionListener for a boundary on an ad-hoc tool, got {other:?}"
        ),
    }
}

#[test]
fn nested_non_activity_listener_does_not_misattach_to_enclosing_subprocess() {
    // #1197 guard against the silent io_stack mis-attachment class: a
    // listener declared on a gateway / start event / boundary event nested
    // inside a sub-process must land on that node, NOT hoist onto the
    // enclosing sub-process (the same defect class as the ioMapping bug in
    // PR #565 / #971).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="ss">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="ss-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:startEvent>
      <bpmn:serviceTask id="st" />
      <bpmn:boundaryEvent id="sb" attachedToRef="st">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="sb-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
        <bpmn:timerEventDefinition><bpmn:timeDuration>PT1M</bpmn:timeDuration></bpmn:timerEventDefinition>
      </bpmn:boundaryEvent>
      <bpmn:exclusiveGateway id="sg">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="sg-start" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:exclusiveGateway>
      <bpmn:endEvent id="se" />
      <bpmn:endEvent id="sbe" />
      <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="st" />
      <bpmn:sequenceFlow id="sf2" sourceRef="st" targetRef="sg" />
      <bpmn:sequenceFlow id="sf3" sourceRef="sg" targetRef="se" />
      <bpmn:sequenceFlow id="sf4" sourceRef="sb" targetRef="sbe" />
    </bpmn:subProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    // The nested start event and gateway carry their own listeners …
    assert_eq!(def.element("ss").unwrap().start_listeners.len(), 1);
    assert_eq!(
        def.element("ss").unwrap().start_listeners[0].job_type,
        "ss-start"
    );
    assert_eq!(def.element("sg").unwrap().start_listeners.len(), 1);
    assert_eq!(
        def.element("sg").unwrap().start_listeners[0].job_type,
        "sg-start"
    );
    // … the nested boundary owns its listener — the specific nested-boundary
    // failure mode of #1197, where `io_stack.last()` is the enclosing
    // sub-process, so a mis-routed boundary listener would land on `sub`
    // rather than the boundary. It must sit on `sb`, and neither its host
    // activity `st` nor the enclosing `sub` may absorb it.
    assert_eq!(def.element("sb").unwrap().start_listeners.len(), 1);
    assert_eq!(
        def.element("sb").unwrap().start_listeners[0].job_type,
        "sb-start"
    );
    assert!(def.element("st").unwrap().start_listeners.is_empty());
    assert!(def.element("st").unwrap().end_listeners.is_empty());
    // … and the enclosing sub-process must NOT have absorbed any of them.
    let sub = def.element("sub").unwrap();
    assert!(
        sub.start_listeners.is_empty(),
        "sub-process must not inherit nested nodes' listeners: {:?}",
        sub.start_listeners
    );
    assert!(sub.end_listeners.is_empty());
}

#[test]
fn self_closing_gateway_with_no_children_balances_io_stack() {
    // A self-closing `<exclusiveGateway/>` emits no end tag; the following
    // activity's ioMapping must still attach to that activity (no io_stack
    // imbalance from the gateway) — guards the #1197 push/pop balance.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:exclusiveGateway id="g" />
    <bpmn:serviceTask id="t">
      <bpmn:extensionElements>
        <zeebe:ioMapping><zeebe:input source="=1" target="one" /></zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="g" />
    <bpmn:sequenceFlow id="f2" sourceRef="g" targetRef="t" />
    <bpmn:sequenceFlow id="f3" sourceRef="t" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    assert!(def.element("g").unwrap().start_listeners.is_empty());
    let io = &def.element("t").unwrap().io;
    assert_eq!(io.inputs.len(), 1);
    assert_eq!(io.inputs[0].target, "one");
}

#[test]
fn listener_on_parallel_join_is_rejected_at_deploy() {
    // #1197: a multi-incoming parallel gateway is a *join* — its lifecycle
    // short-circuits inside `Engine::activate` and never runs the shared
    // listener-aware activation body, so a listener on it would be parsed but
    // never fire. Reject the deploy rather than silently store a dead listener.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:parallelGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:parallelGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="join-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let err = parse_bpmn(xml).unwrap_err();
    match err {
        ParseError::UnsupportedExecutionListener { element_id, .. } => {
            assert_eq!(element_id, "join");
        }
        other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
    }
}

#[test]
fn end_listener_on_parallel_join_is_rejected_at_deploy() {
    // #1197: unlike an inclusive join, a parallel join completes without
    // running the end-listener chain (`activate_join` emits
    // `ElementCompleting` → `ElementCompleted` directly), so even an `end`
    // listener on it can never fire — reject both phases.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:parallelGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:parallelGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="join-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let err = parse_bpmn(xml).unwrap_err();
    match err {
        ParseError::UnsupportedExecutionListener { element_id, .. } => {
            assert_eq!(element_id, "join");
        }
        other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
    }
}

#[test]
fn listener_on_inclusive_join_is_rejected_at_deploy() {
    // #1197: a multi-incoming inclusive gateway is synchronised by its
    // activation guard and short-circuits the listener-aware activation body, so its `start`
    // listener can never fire — reject at deploy. (Its `end` listener IS
    // supported and must NOT be rejected — see
    // `end_listener_on_inclusive_join_is_supported`.)
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:inclusiveGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:inclusiveGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="join-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:inclusiveGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let err = parse_bpmn(xml).unwrap_err();
    match err {
        ParseError::UnsupportedExecutionListener { element_id, .. } => {
            assert_eq!(element_id, "join");
        }
        other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
    }
}

#[test]
fn end_listener_on_inclusive_join_is_supported() {
    // #1197: an accepted inclusive join (`route_inclusive_gateway`)
    // DOES defer its routing behind its end-listener chain
    // (`begin_end_listener_chain`), so an `end` listener on a multi-incoming
    // inclusive join fires and must be ACCEPTED at deploy — the same model
    // built through `ProcessBuilder` deploys and runs
    // (`end_listener_fires_on_a_multi_incoming_inclusive_join`). Guards against
    // the over-broad rejection that also refused this supported placement.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:inclusiveGateway id="fork" />
    <bpmn:serviceTask id="a" />
    <bpmn:serviceTask id="b" />
    <bpmn:inclusiveGateway id="join">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="end" type="join-end" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:inclusiveGateway>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a">
      <bpmn:conditionExpression>=true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b">
      <bpmn:conditionExpression>=true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f3" sourceRef="a" targetRef="join" />
    <bpmn:sequenceFlow id="f4" sourceRef="b" targetRef="join" />
    <bpmn:sequenceFlow id="f5" sourceRef="join" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = parse_bpmn(xml).expect("inclusive-join end listener must deploy");
    let join = def[0].element("join").expect("join element present");
    assert_eq!(
        join.end_listeners.len(),
        1,
        "the inclusive-join end listener must be stored"
    );
    assert!(
        join.start_listeners.is_empty(),
        "no start listener was declared"
    );
}

#[test]
fn listener_on_single_incoming_gateway_split_is_supported() {
    // A single-incoming parallel/inclusive gateway (a *split*) runs the
    // ordinary activation body, so its listeners DO fire — only the join is
    // rejected. Guard that the join rejection does not over-reach to splits.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:parallelGateway id="fork">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="fork-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
    </bpmn:parallelGateway>
    <bpmn:endEvent id="a" />
    <bpmn:endEvent id="b" />
    <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="fork" />
    <bpmn:sequenceFlow id="f1" sourceRef="fork" targetRef="a" />
    <bpmn:sequenceFlow id="f2" sourceRef="fork" targetRef="b" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    assert_eq!(def.element("fork").unwrap().start_listeners.len(), 1);
    assert_eq!(
        def.element("fork").unwrap().start_listeners[0].job_type,
        "fork-start"
    );
}

#[test]
fn listener_on_compensation_boundary_is_rejected_at_deploy() {
    // #1197: a compensation boundary event is a passive structural marker,
    // armed implicitly when its host completes and never entered by token
    // flow, so it has no lifecycle to hang a listener on. Reject the deploy
    // rather than silently store a listener that can never create a job.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:serviceTask id="book" />
    <bpmn:boundaryEvent id="book-comp" attachedToRef="book">
      <bpmn:extensionElements>
        <zeebe:executionListeners>
          <zeebe:executionListener eventType="start" type="comp-start" />
        </zeebe:executionListeners>
      </bpmn:extensionElements>
      <bpmn:compensateEventDefinition />
    </bpmn:boundaryEvent>
    <bpmn:serviceTask id="undo-book" isForCompensation="true" />
    <bpmn:association associationDirection="One" sourceRef="book-comp" targetRef="undo-book" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="book" />
    <bpmn:sequenceFlow id="f2" sourceRef="book" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let err = parse_bpmn(xml).unwrap_err();
    match err {
        ParseError::UnsupportedExecutionListener { element_id, .. } => {
            assert_eq!(element_id, "book-comp");
        }
        other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
    }
}

#[test]
fn listener_nested_in_sequence_flow_is_rejected_not_hoisted() {
    // #1198 deferral guard: a sequence flow is an edge, not an `Element`, so
    // a `zeebe:executionListener` nested inside a `<sequenceFlow>` has no
    // lifecycle to run on. It must be REJECTED at deploy — the same
    // dead-listener failure mode this change rejects for joins and
    // compensation boundaries — NOT silently dropped (which would let a
    // declared take listener deploy yet never fire) and NOT fall through to
    // `io_stack.last()` and hoist onto the enclosing sub-process (the #1197
    // mis-attachment class). Here the flow sits inside `sub`.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="ss" />
      <bpmn:endEvent id="se" />
      <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="se">
        <bpmn:extensionElements>
          <zeebe:executionListeners>
            <zeebe:executionListener eventType="start" type="take-listener" />
          </zeebe:executionListeners>
        </bpmn:extensionElements>
      </bpmn:sequenceFlow>
    </bpmn:subProcess>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
    <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let err = parse_bpmn(xml).expect_err("sequence-flow listener must be rejected at deploy");
    match err {
        ParseError::UnsupportedExecutionListener { element_id, .. } => {
            assert_eq!(
                element_id, "sf1",
                "the rejection must name the offending sequence flow"
            );
        }
        other => panic!("expected UnsupportedExecutionListener, got {other:?}"),
    }
}

#[test]
fn parses_feel_timer_expressions() {
    // A `=`-prefixed timeDuration/timeCycle and any timeDate are captured as
    // FEEL timer expressions; a static ISO literal is not.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:intermediateCatchEvent id="wait">
      <bpmn:timerEventDefinition><bpmn:timeDuration>=waitFor</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="until">
      <bpmn:timerEventDefinition><bpmn:timeDate>=dueAt</bpmn:timeDate></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="fixed">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT30S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="wait" />
    <bpmn:sequenceFlow id="f2" sourceRef="wait" targetRef="until" />
    <bpmn:sequenceFlow id="f3" sourceRef="until" targetRef="fixed" />
    <bpmn:sequenceFlow id="f4" sourceRef="fixed" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];

    let wait = def.element("wait").unwrap().timer.as_ref().unwrap();
    assert_eq!(wait.kind, TimerDefKind::Duration);
    assert_eq!(wait.expr, "=waitFor");

    let until = def.element("until").unwrap().timer.as_ref().unwrap();
    assert_eq!(until.kind, TimerDefKind::Date);
    assert_eq!(until.expr, "=dueAt");

    // A static ISO literal is parsed at deploy, not carried as a FEEL expr.
    assert!(def.element("fixed").unwrap().timer.is_none());
}

#[test]
fn should_parse_an_ai_agent_task_service_task() {
    // A `serviceTask` bearing a `zeebe:agentDefinition agentType="aiAgentTask"`
    // marker classifies the ordinary job-based service task.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="agent-proc" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentTask" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    let kind = &def.element("agent").unwrap().kind;
    match kind {
        crate::model::ElementKind::ServiceTask { agent_type, .. } => {
            assert_eq!(*agent_type, Some(crate::agent::AgentType::AiAgentTask));
        }
        other => panic!("expected marked ServiceTask, got {other:?}"),
    }
}

#[test]
fn should_reject_ai_agent_task_on_a_non_service_task() {
    // Placement rule (Camunda AgentDefinitionValidator): `aiAgentTask` is only
    // valid on a `serviceTask`. On an `adHocSubProcess` it must be rejected.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentTask" />
                </bpmn:extensionElements>
                <bpmn:task id="inner" />
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert!(
        matches!(err, ParseError::InvalidAgentDefinition { ref element_id, .. } if element_id == "agent"),
        "expected InvalidAgentDefinition, got {err:?}"
    );
}

#[test]
fn should_reject_ai_agent_subprocess_on_a_service_task() {
    // Placement rule: `aiAgentSubProcess` is only valid on an
    // `adHocSubProcess`. On a plain `serviceTask` it must be rejected.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent2" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentSubProcess" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert!(
        matches!(err, ParseError::InvalidAgentDefinition { ref element_id, .. } if element_id == "agent"),
        "expected InvalidAgentDefinition, got {err:?}"
    );
}

#[test]
fn should_accept_ai_agent_subprocess_on_an_ad_hoc_sub_process() {
    // The valid placement: `agentType="aiAgentSubProcess"` on a
    // `bpmn:adHocSubProcess`. The ad-hoc variant reuses the existing
    // ad-hoc container machinery — so the container parses into a
    // single job-bearing `ServiceTask` at the parent token-flow level, and
    // its contained "tool" activities are pruned from the executable graph
    // (invoked out-of-band by the worker, not by token flow).
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="agent-adhoc" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:adHocSubProcess id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentSubProcess" />
                </bpmn:extensionElements>
                <bpmn:serviceTask id="tool" />
              </bpmn:adHocSubProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];
    // The ad-hoc container is retained as a single job-bearing ServiceTask,
    // NOT an engine-native AgentTask.
    let kind = &def.element("agent").unwrap().kind;
    assert!(
        matches!(
            kind,
            crate::model::ElementKind::ServiceTask {
                agent_type: Some(crate::agent::AgentType::AiAgentSubProcess),
                ..
            }
        ),
        "expected the ad-hoc agent container to be a ServiceTask, got {kind:?}"
    );
    // The contained tool activity is pruned from the executable graph.
    assert!(
        def.element("tool").is_none(),
        "expected the ad-hoc tool `tool` to be pruned from the executable graph"
    );
}

#[test]
fn should_reject_an_unknown_agent_type() {
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent3" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="wat" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert!(
        matches!(err, ParseError::InvalidAgentDefinition { ref reason, .. } if reason.contains("unknown agentType 'wat'")),
        "expected InvalidAgentDefinition naming the unknown value, got {err:?}"
    );
}

#[test]
fn should_reject_a_missing_agent_type_with_a_clear_reason() {
    // An empty/absent `agentType` must not be reported as `unknown agentType ''`;
    // the reason should say the attribute is missing.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent4" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="agent">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="agent" />
              <bpmn:sequenceFlow id="f2" sourceRef="agent" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert!(
        matches!(err, ParseError::InvalidAgentDefinition { ref reason, .. } if reason.contains("missing agentType attribute")),
        "expected InvalidAgentDefinition citing a missing attribute, got {err:?}"
    );
}

#[test]
fn misplaced_agent_definition_names_the_enclosing_flow_node() {
    // A `zeebe:agentDefinition` on neither a serviceTask nor an adHocSubProcess
    // (here a userTask) is rejected. The error must attribute the fault to the
    // nearest enclosing flow node so it is diagnosable, not an empty id.
    let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="bad-agent4" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:userTask id="review">
                <bpmn:extensionElements>
                  <zeebe:agentDefinition agentType="aiAgentTask" />
                </bpmn:extensionElements>
              </bpmn:userTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="review" />
              <bpmn:sequenceFlow id="f2" sourceRef="review" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let err = parse_bpmn(xml).unwrap_err();
    assert!(
        matches!(err, ParseError::InvalidAgentDefinition { ref element_id, .. } if element_id == "review"),
        "expected InvalidAgentDefinition attributed to 'review', got {err:?}"
    );
}

/// A message end / intermediate throw event with a `zeebe:taskDefinition` is a
/// Zeebe job worker (`JobWorkerElementIncidentTest[END_EVENT]` /
/// `[INTERMEDIATE_THROW_EVENT]`): it must run a job of the declared type, not
/// pass straight through.
#[test]
fn should_parse_message_throw_and_end_events_with_task_definition_as_job_workers() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <process id="p" isExecutable="true">
    <startEvent id="s"/>
    <intermediateThrowEvent id="throw">
      <extensionElements><zeebe:taskDefinition type="notify" retries="5"/></extensionElements>
      <messageEventDefinition/>
    </intermediateThrowEvent>
    <endEvent id="end">
      <extensionElements><zeebe:taskDefinition type="=finalType"/></extensionElements>
      <messageEventDefinition/>
    </endEvent>
    <sequenceFlow id="f1" sourceRef="s" targetRef="throw"/>
    <sequenceFlow id="f2" sourceRef="throw" targetRef="end"/>
  </process>
</definitions>"#;
    let def = &parse_bpmn(xml).unwrap()[0];
    for (id, ty) in [("throw", "notify"), ("end", "=finalType")] {
        match &def.element(id).unwrap().kind {
            ElementKind::ServiceTask { job_type, .. } => assert_eq!(job_type, ty),
            other => panic!("{id} must be a job worker, got {other:?}"),
        }
    }
}

/// A job-worker message END event is still an end event: an outgoing flow is
/// rejected before it is rebuilt as a job task (which no end-event rule sees).
#[test]
fn job_worker_message_end_event_with_outgoing_flow_is_rejected() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <process id="p" isExecutable="true">
    <startEvent id="s"/>
    <endEvent id="end">
      <extensionElements><zeebe:taskDefinition type="t"/></extensionElements>
      <messageEventDefinition/>
    </endEvent>
    <endEvent id="e2"/>
    <sequenceFlow id="f1" sourceRef="s" targetRef="end"/>
    <sequenceFlow id="f2" sourceRef="end" targetRef="e2"/>
  </process>
</definitions>"#;
    let err = parse_bpmn(xml).unwrap_err();
    assert!(
        matches!(&err, ParseError::InvalidEndEvent { element_id, .. } if element_id == "end"),
        "got {err:?}"
    );
}

/// Job-worker message throw/end events get the same task-definition attribute
/// validation as any job task: an empty `type` / `retries` fails deploy.
#[test]
fn job_worker_message_events_validate_task_definition_attributes() {
    for (tag, attrs) in [
        ("intermediateThrowEvent", r#"type="""#),
        ("intermediateThrowEvent", r#"type="t" retries="""#),
        ("endEvent", r#"type="""#),
        ("endEvent", r#"type="t" retries="""#),
    ] {
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <process id="p" isExecutable="true">
    <startEvent id="s"/>
    <{tag} id="w">
      <extensionElements><zeebe:taskDefinition {attrs}/></extensionElements>
      <messageEventDefinition/>
    </{tag}>
    <sequenceFlow id="f1" sourceRef="s" targetRef="w"/>
  </process>
</definitions>"#
        );
        let err = parse_bpmn(&xml).unwrap_err();
        assert!(
            format!("{err:?}").contains("InvalidTaskDefinition"),
            "{tag} {attrs}: got {err:?}"
        );
    }
}

/// One process body hosting `def` on a node of `placement` — the probe shape
/// for the #985 event-definition placement matrix below.
fn event_definition_placement_xml(placement: &str, def: &str) -> String {
    let body = match placement {
        "intermediateCatchEvent" | "intermediateThrowEvent" => format!(
            r#"<bpmn:startEvent id="s" />
               <bpmn:{placement} id="Probe">{def}</bpmn:{placement}>
               <bpmn:endEvent id="e" />
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Probe" />
               <bpmn:sequenceFlow id="f2" sourceRef="Probe" targetRef="e" />"#
        ),
        "boundaryEvent" => format!(
            r#"<bpmn:startEvent id="s" />
               <bpmn:serviceTask id="t">
                 <bpmn:extensionElements><zeebe:taskDefinition type="t" /></bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:boundaryEvent id="Probe" attachedToRef="t">{def}</bpmn:boundaryEvent>
               <bpmn:endEvent id="e" />
               <bpmn:endEvent id="e2" />
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
               <bpmn:sequenceFlow id="f3" sourceRef="Probe" targetRef="e2" />"#
        ),
        "startEvent" => format!(
            r#"<bpmn:startEvent id="Probe">{def}</bpmn:startEvent>
               <bpmn:endEvent id="e" />
               <bpmn:sequenceFlow id="f1" sourceRef="Probe" targetRef="e" />"#
        ),
        other => panic!("unknown placement {other}"),
    };
    format!(
        r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
             xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
             <bpmn:process id="p" isExecutable="true">{body}</bpmn:process>
           </bpmn:definitions>"#
    )
}

// zeebe-cells: event:end:terminate
#[test]
fn terminate_event_definition_placement_matches_zeebe_deploy_validation() {
    // #985 — the defect class: a `terminateEventDefinition` outside an
    // `endEvent` used to be silently ignored on EVERY placement. The pinned
    // Zeebe (probed against `EngineRule` deploy) answers per placement:
    //
    // - intermediateCatchEvent → REJECTED ("Event definition must be one of:
    //   message, timer, signal, link or conditional")
    // - boundaryEvent → REJECTED ("Boundary events must be one of: timer,
    //   message, error, signal, escalation, conditional")
    // - intermediateThrowEvent / startEvent → ACCEPTED, and the event runs as
    //   an inert none event (pinned at runtime by the
    //   `terminate-definition-on-{throw,start}-event` Zeebe traces).
    //
    // A silent drop on a placement Zeebe rejects deploys a model Zeebe would
    // refuse; it must reject naming the element instead (reject-don't-drop).
    let terminate = "<bpmn:terminateEventDefinition />";
    for placement in ["intermediateCatchEvent", "boundaryEvent"] {
        let xml = event_definition_placement_xml(placement, terminate);
        let err = parse_bpmn(&xml).expect_err(&format!(
            "a terminateEventDefinition on a {placement} must be rejected at deploy (Zeebe rejects it)"
        ));
        assert!(
            format!("{err:?}").contains("terminateEventDefinition")
                && format!("{err:?}").contains("Probe"),
            "{placement}: the rejection must name the definition and the element, got {err:?}"
        );
    }
    for placement in ["intermediateThrowEvent", "startEvent"] {
        let xml = event_definition_placement_xml(placement, terminate);
        let defs = parse_bpmn(&xml).unwrap_or_else(|e| {
            panic!("a terminateEventDefinition on a {placement} deploys in Zeebe: {e:?}")
        });
        let probe = defs[0]
            .elements
            .values()
            .find(|el| el.id.as_str() == "Probe")
            .expect("the probe element is built");
        assert!(
            !matches!(probe.kind, ElementKind::TerminateEndEvent),
            "{placement}: the stray definition must NOT make it a terminate end, got {:?}",
            probe.kind
        );
    }
}

#[test]
fn terminate_end_event_accepts_end_execution_listeners() {
    // #985 — Zeebe's `TerminateEndEventBehavior` completes the terminate end
    // through the normal completion path (its `end` execution listener runs)
    // BEFORE terminating the siblings, and `ExecutionListenersValidator`
    // accepts both phases on it. Rejecting the `end` listener at deploy
    // refused a model Zeebe deploys and runs.
    let xml = r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
             xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
      <bpmn:process id="p" isExecutable="true">
        <bpmn:startEvent id="s" />
        <bpmn:endEvent id="Kill">
          <bpmn:extensionElements>
            <zeebe:executionListeners>
              <zeebe:executionListener eventType="start" type="killStart" />
              <zeebe:executionListener eventType="end" type="killEnd" />
            </zeebe:executionListeners>
          </bpmn:extensionElements>
          <bpmn:terminateEventDefinition />
        </bpmn:endEvent>
        <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="Kill" />
      </bpmn:process>
    </bpmn:definitions>"#;
    let defs = parse_bpmn(xml).expect("a terminate end with start+end listeners deploys");
    let kill = defs[0]
        .elements
        .values()
        .find(|el| el.id.as_str() == "Kill")
        .expect("Kill is built");
    assert!(matches!(kill.kind, ElementKind::TerminateEndEvent));
    assert_eq!(kill.start_listeners.len(), 1, "{:?}", kill.start_listeners);
    assert_eq!(kill.end_listeners.len(), 1, "{:?}", kill.end_listeners);
}

#[test]
fn should_not_attach_nested_sequence_flow_properties_to_the_enclosing_subprocess() {
    // A sequenceFlow never enters the io_stack, so properties declared on a
    // flow inside a subprocess must not fall through to (and overwrite) the
    // subprocess's own metadata. Flows carry no properties slot: ignore them.
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:subProcess id="sub">
                <bpmn:extensionElements>
                  <zeebe:properties>
                    <zeebe:property name="owner" value="sub" />
                  </zeebe:properties>
                </bpmn:extensionElements>
                <bpmn:startEvent id="ss" />
                <bpmn:endEvent id="se" />
                <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="se">
                  <bpmn:extensionElements>
                    <zeebe:properties>
                      <zeebe:property name="owner" value="edge" />
                      <zeebe:property name="extra" value="edge" />
                    </zeebe:properties>
                  </bpmn:extensionElements>
                </bpmn:sequenceFlow>
              </bpmn:subProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
              <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    let sub = def.element("sub").unwrap();
    assert_eq!(sub.properties.get("owner").map(String::as_str), Some("sub"));
    assert!(!sub.properties.contains_key("extra"));
}

#[test]
fn should_not_attach_nested_data_object_properties_to_the_enclosing_subprocess() {
    let xml = r#"
          <bpmn:definitions
              xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:subProcess id="sub">
                <bpmn:extensionElements>
                  <zeebe:properties>
                    <zeebe:property name="owner" value="sub" />
                  </zeebe:properties>
                </bpmn:extensionElements>
                <bpmn:dataObject id="d">
                  <bpmn:extensionElements>
                    <zeebe:properties>
                      <zeebe:property name="owner" value="data" />
                      <zeebe:property name="extra" value="data" />
                    </zeebe:properties>
                  </bpmn:extensionElements>
                </bpmn:dataObject>
                <bpmn:startEvent id="ss" />
                <bpmn:endEvent id="se" />
                <bpmn:sequenceFlow id="sf1" sourceRef="ss" targetRef="se" />
              </bpmn:subProcess>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="sub" />
              <bpmn:sequenceFlow id="f2" sourceRef="sub" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

    let def = &parse_bpmn(xml).unwrap()[0];

    let sub = def.element("sub").unwrap();
    assert_eq!(sub.properties.get("owner").map(String::as_str), Some("sub"));
    assert!(!sub.properties.contains_key("extra"));
}
