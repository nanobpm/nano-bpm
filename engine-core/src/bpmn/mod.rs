//! A minimal BPMN 2.0 XML parser.
//!
//! This turns the slice of BPMN that the engine understands into
//! [`ProcessDefinition`]s, so processes can be deployed from `.bpmn` files
//! (e.g. exported by the Camunda Modeler) instead of only being built
//! programmatically with [`crate::ProcessBuilder`].
//!
//! It is deliberately tiny and dependency-free: a hand-rolled, namespace-prefix
//! agnostic XML scanner plus a single pass that recognises the supported flow
//! nodes. It is **not** a conformant BPMN/XML implementation — it understands
//! exactly the constructs the engine can execute and ignores the rest
//! (diagram interchange, documentation, lanes, …).
//!
//! ## Supported subset
//!
//! * `process` (one or more per file) with its `id`.
//! * Flow nodes: `startEvent`, `endEvent` (plain none end, or — with a nested
//!   `terminateEventDefinition` — a terminate end that kills the remaining
//!   tokens in its enclosing scope), `task`/`manualTask` (abstract
//!   pass-through), `serviceTask`, `sendTask` (a job-based service task — its
//!   throwing cousin of `receiveTask`; a message `intermediateThrowEvent` /
//!   `endEvent` carrying a `zeebe:taskDefinition` is the same job worker),
//!   `userTask`, `exclusiveGateway`,
//!   `parallelGateway`, `inclusiveGateway`, `eventBasedGateway`.
//! * `subProcess` (embedded): its nested flow nodes/flows are scoped to it, and
//!   a `boundaryEvent` with an `errorEventDefinition` attached to it becomes an
//!   interrupting error boundary on the sub-process.
//! * `intermediateCatchEvent` with a nested `timerEventDefinition`/`timeDuration`
//!   (timer catch) or a nested `messageEventDefinition` (message catch).
//! * Link events: an `intermediateThrowEvent` with a `linkEventDefinition name`
//!   (a link *throw*) and an `intermediateCatchEvent` with a matching
//!   `linkEventDefinition name` (a link *catch*). The throw has no outgoing flow
//!   and the catch no incoming flow; on completion the throw hands its token to
//!   the matching catch in the same scope, which then routes onward — the
//!   standard "page-break connector" idiom. Reference integrity (every throw has
//!   a matching, unique catch) is enforced at deploy
//!   (Zeebe `ModelUtil.verifyLinkIntermediateEvents`).
//! * `boundaryEvent` with `attachedToRef` and a nested `errorEventDefinition`
//!   `errorRef`, resolved against definitions-level `error` elements
//!   (`<error id="…" errorCode="…">`) into an error boundary event; or a nested
//!   `timerEventDefinition` (timer boundary); or a nested `messageEventDefinition`
//!   (message boundary); or a nested `compensateEventDefinition` (compensation
//!   boundary — see below). `cancelActivity="false"` makes a timer or message
//!   boundary **non-interrupting** (the activity keeps running and a parallel
//!   token is spawned on each fire); the default is interrupting.
//! * Compensation: a `boundaryEvent` with a `compensateEventDefinition`, wired
//!   via an `<association sourceRef targetRef>` to an `isForCompensation` handler
//!   activity, marks its attached activity compensable. A
//!   `compensateEventDefinition` on an `intermediateThrowEvent`/`endEvent`
//!   triggers compensation: the completed compensable activities in scope have
//!   their handlers run (reverse completion order), and the throw event rests
//!   until they finish before routing onward (single-activity path).
//! * Escalation (#1173): an `escalationEventDefinition` on an
//!   `intermediateThrowEvent`/`endEvent` raises the referenced escalation code;
//!   an `escalationEventDefinition` on a `boundaryEvent` (attached to an
//!   embedded sub-process) catches a matching code raised inside that
//!   activity's scope, propagating up the enclosing scopes (an exact code beats
//!   a catch-all with no `escalationRef`). `cancelActivity="false"` (the
//!   escalation default idiom) makes the boundary non-interrupting — the
//!   activity keeps running and a parallel token is spawned; interrupting tears
//!   the activity's scope down. An uncaught escalation is ignored (no incident),
//!   matching Zeebe. Escalation is non-critical, so the throw always continues.
//! * Definitions-level `message` elements (`<message id="…" name="…">`) with a
//!   nested `zeebe:subscription correlationKey="=var"`, referenced by message
//!   catch/boundary events via `messageRef`.
//! * `zeebe:properties` / nested `zeebe:property name="…" value="…"` on any flow
//!   node (#1499): generic vendor extension metadata, carried verbatim onto the
//!   element's [`Element::properties`](crate::model::Element::properties). This
//!   is the channel Camunda uses to mark a process as an MCP tool
//!   (`io.camunda.tool:name` / `:purpose` / `:input_N_*` on a message start
//!   event) or an inbound connector (`inbound.type`); the engine projects the
//!   tool markers onto the message start subscription (`toolName` /
//!   `toolProperties` on `searchMessageSubscriptions`).
//! * A service task's job type is taken from a nested
//!   `zeebe:taskDefinition type="…"`; if absent it defaults to the task id. A
//!   FEEL expression (`type="=jobType"`, `type='="worker-" + region'`) is
//!   evaluated against the instance variables at job-creation time via
//!   [`crate::feel`]; an expression that cannot be evaluated falls back to the
//!   literal text (no incident is raised).
//! * A user task's assignment/scheduling/priority attributes come from nested
//!   `zeebe:assignmentDefinition` (`assignee`, `candidateGroups`,
//!   `candidateUsers`), `zeebe:taskSchedule` (`dueDate`, `followUpDate`) and
//!   `zeebe:priorityDefinition` (`priority`). Each may be a literal or a FEEL
//!   expression resolved against the instance variables when the task is created.
//!   Its form linkage comes from nested `zeebe:formDefinition` (`formId` for an
//!   embedded/deployment form, resolved to a numeric form key at task creation
//!   under the default `latest` binding — a `deployment`/`versionTag`
//!   `bindingType` is rejected at deploy rather than silently degraded, #1190;
//!   `externalReference` for an external form).
//! * `sequenceFlow` with `sourceRef`/`targetRef`, and an optional
//!   `conditionExpression` whose FEEL body is stored verbatim and evaluated by
//!   [`crate::feel`] at a condition-routed gateway — exclusive (XOR) or
//!   inclusive (OR) (comparisons, arithmetic, boolean logic, member access — not
//!   just equality). A condition that fails to
//!   evaluate to a boolean raises an `ExpressionEvaluation` incident.
//! * A `serviceTask` bearing a `zeebe:agentDefinition agentType="aiAgentTask"`
//!   (or `"external"`) extension marker remains an ordinary
//!   [`ServiceTask`](crate::model::ElementKind::ServiceTask), with agent metadata.
//!   It creates a normal job; the worker explicitly registers its AgentInstance
//!   via `CreateAgentInstance`. Supplied job attribution is lease-validated,
//!   and history-bearing requests require it. Its job type comes from a co-located
//!   `zeebe:taskDefinition type` (literal or FEEL), or the element id when absent.
//!   Placement mirrors Camunda's
//!   `AgentDefinitionValidator`: `aiAgentTask` is only valid on a `serviceTask`
//!   and `aiAgentSubProcess` only on an `adHocSubProcess`; the wrong placement
//!   (or an unknown `agentType`) is rejected at parse time.
//! * `zeebe:executionListeners` / nested `zeebe:executionListener`
//!   (`eventType` `start`/`end`, ADR 0037): parsed on activities (including
//!   `receiveTask`, which rides the `io_stack` like the other plain tasks),
//!   sub-processes, ad-hoc/call containers, **gateways** (exclusive/parallel/
//!   inclusive/event-based), **start events**, and **boundary events** (#1197).
//!   Each
//!   listener attaches to its own element (activities/gateways/start & end events
//!   ride the `io_stack`; a boundary event carries its listeners on the buffered
//!   boundary and re-attaches them by id at build) and fires through the shared
//!   activation/completion listener gate. A listener that could never fire is
//!   rejected at deploy rather than dropped (`UnsupportedExecutionListener`):
//!   a multi-incoming parallel gateway (a *join* — both phases), the `start`
//!   listener of a multi-incoming inclusive gateway (its `end` listener IS
//!   supported), a compensation boundary event, a listener on a
//!   surplus signal start event (demoted to an inert throw), a **tool of an
//!   ad-hoc sub-process** (pruned leaf tools and directly-activated embedded
//!   tools both bypass the listener gate), and a sequence-flow ("take") listener
//!   (edges, not elements — deferred, #1198). A listener on the `<process>`
//!   itself is likewise rejected — the process element has no activation/
//!   completion lifecycle and is never ridden by a token. A `zeebe:taskListener`
//!   declared on
//!   a non-user-task element is likewise rejected (`UnsupportedTaskListener`),
//!   since task-listener jobs only run on the user-task path.

use std::collections::HashMap;

use crate::model::{ProcessBuilder, ProcessDefinition};
/// The BPMN parse-error type, owned by [`crate::validate::error`] (the
/// validation seam that raises it) and re-exported here so every downstream
/// `bpmn::ParseError` path keeps resolving (#1203).
pub use crate::validate::error::ParseError;
use crate::xml::{attr, local_name, tokenize, Token};

mod build;
mod walk;
/// Re-exported from [`walk`] so [`parse_bpmn`] and the parser tests keep
/// calling the streaming parse entry unqualified after the split (#1207).
pub(crate) use walk::parse_with_captures;

/// Whether a tag reaching the in-process catch-all is genuinely ignorable
/// non-flow noise (diagram interchange, documentation, extension-element
/// children Nano reads elsewhere, structural/data/collaboration BPMN, and the
/// event definitions Nano *does* model), as opposed to an unmodelled flow
/// element / event definition that must be recorded for the unsupported-element
/// validator (#853).
///
/// This is deliberately an *exclusion* list of noise: anything not listed here
/// is recorded as a candidate unmodelled element, and #853 decides — from the
/// canonical supported-element registry — which recorded candidates are
/// genuinely unsupported. Over-recording a rare non-flow tag is harmless (the
/// validator filters it); silently dropping a real flow element is the bug this
/// closes.
fn is_ignorable_tag(tag: &str) -> bool {
    matches!(
        tag,
        // Diagram interchange (BPMNDI / OMGDI / DC).
        "BPMNDiagram"
            | "BPMNPlane"
            | "BPMNShape"
            | "BPMNLabel"
            | "BPMNLabelStyle"
            | "BPMNEdge"
            | "DiagramElement"
            | "Bounds"
            | "waypoint"
            | "Label"
            | "Font"
            // Documentation / extension containers (their meaningful children are
            // matched explicitly above, or intentionally ignored).
            | "documentation"
            | "extensionElements"
            | "text"
            | "modelerTemplate"
            | "userTaskForm"
            // Event definitions Nano *does* model (timer via <timeDuration>,
            // message/signal/error/escalation/link via their refs, conditional
            // via <condition>) — not "unsupported", so never recorded.
            | "timerEventDefinition"
            | "messageEventDefinition"
            | "signalEventDefinition"
            | "errorEventDefinition"
            | "escalationEventDefinition"
            | "linkEventDefinition"
            | "conditionalEventDefinition"
            // `terminateEventDefinition` is handled by an explicit parse arm (it
            // marks its owning `<endEvent>` as a terminate end event); it never
            // reaches the fallback, so it is not listed here.
            // Nested value/config children of already-modelled constructs.
            | "condition"
            | "conditionExpression"
            | "timeDuration"
            | "timeCycle"
            | "timeDate"
            | "completionCondition"
            | "loopCardinality"
            | "activationCondition"
            | "transitionCondition"
            | "incoming"
            | "outgoing"
            // Structural / data / collaboration / resourcing elements that carry
            // no executable flow semantics for Nano.
            | "laneSet"
            | "lane"
            | "flowNodeRef"
            | "childLaneSet"
            | "dataObject"
            | "dataObjectReference"
            | "dataStore"
            | "dataStoreReference"
            | "property"
            | "dataInput"
            | "dataOutput"
            | "dataInputAssociation"
            | "dataOutputAssociation"
            | "inputSet"
            | "outputSet"
            | "ioSpecification"
            | "sourceRef"
            | "targetRef"
            | "association"
            | "group"
            | "textAnnotation"
            | "participant"
            | "collaboration"
            | "messageFlow"
            | "category"
            | "categoryValue"
            | "categoryValueRef"
            | "auditing"
            | "monitoring"
            | "relationship"
            | "resource"
            | "resourceRef"
            | "resourceAssignmentExpression"
            | "potentialOwner"
            | "humanPerformer"
            | "performer"
            | "rendering"
    )
}

/// Parses BPMN 2.0 XML into the executable [`ProcessDefinition`]s it contains.
///
/// Returns one definition per `<process>` element, in document order.
///
/// ```
/// use nanobpmn_engine_core::bpmn::parse_bpmn;
/// let xml = r#"
///   <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
///     <bpmn:process id="p">
///       <bpmn:startEvent id="s" />
///       <bpmn:endEvent id="e" />
///       <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
///     </bpmn:process>
///   </bpmn:definitions>"#;
/// let defs = parse_bpmn(xml).unwrap();
/// assert_eq!(defs.len(), 1);
/// assert_eq!(defs[0].id, "p");
/// assert_eq!(defs[0].start_event, "s");
/// ```
pub fn parse_bpmn(xml: &str) -> Result<Vec<ProcessDefinition>, ParseError> {
    parse_with_captures(xml)?
        .into_iter()
        .map(|(capture, def)| {
            // Run the post-parse validators (deploy-validation parity, #850).
            crate::validate::run(&crate::validate::ValidationInput {
                def: &def,
                capture: &capture,
            })?;
            Ok(def)
        })
        .collect()
}

/// Returns a FEEL [`crate::model::TimerDef`] of `kind` when `trimmed` is a FEEL
/// expression (a leading `=`), otherwise `None` (the caller treats it as a
/// static ISO-8601 literal).
fn feel_timer_expr(
    trimmed: &str,
    kind: crate::model::TimerDefKind,
) -> Option<crate::model::TimerDef> {
    if trimmed.starts_with('=') {
        Some(crate::model::TimerDef {
            kind,
            expr: trimmed.to_string(),
        })
    } else {
        None
    }
}

/// Parses an ISO-8601 duration (e.g. `PT5S`, `PT1M30S`, `PT2H`, `P1DT6H`,
/// `P1W`) into milliseconds. Thin BPMN-timer wrapper over the canonical
/// [`crate::temporal`] leaf ([`crate::temporal::DurationForm::BPMN_TIMER`]):
/// weeks, days, hours, minutes and seconds are supported; the date-portion
/// years/months are ambiguous in length and not supported. Returns `None` if
/// the string is not a recognisable duration.
pub(crate) fn parse_iso8601_duration(raw: &str) -> Option<u64> {
    crate::temporal::parse_duration_millis(raw)
}

/// Parses an ISO-8601 repeating interval (a BPMN `timeCycle`, e.g. `R/PT1H` or
/// `R5/PT1H`) into the interval in milliseconds. Thin wrapper over the canonical
/// [`crate::temporal`] leaf. The `Rn` repetition-count prefix is accepted but
/// ignored (the engine repeats unboundedly). A bare duration without the
/// `R[n]/` prefix is also accepted. Returns `None` if the interval portion is
/// not a recognisable duration.
pub(crate) fn parse_iso8601_cycle(raw: &str) -> Option<u64> {
    crate::temporal::parse_cycle_millis(raw)
}

/// Parses a `zeebe:subscription correlationKey` expression into the name of an
/// instance variable: strips the leading FEEL `=` marker and trims. Returns
/// `None` if the result is empty.
fn parse_correlation_key(raw: &str) -> Option<String> {
    let expr = raw.trim();
    let expr = expr.strip_prefix('=').unwrap_or(expr).trim();
    if expr.is_empty() {
        None
    } else {
        Some(expr.to_string())
    }
}

#[cfg(test)]
mod tests;
