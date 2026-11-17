//! The deploy-validation error type.
//!
//! [`ParseError`] is owned here — by the validation seam that raises it — rather
//! than by [`crate::bpmn`]'s streaming parser, so that `validate` no longer
//! imports from `bpmn` (breaking the former `bpmn ↔ validate` cycle, #1203).
//! `bpmn` re-exports it (`pub use crate::validate::error::ParseError`), keeping
//! every downstream `bpmn::ParseError` path working.

/// An error encountered while parsing BPMN XML.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// The XML was malformed (unterminated tag, bad quoting, …).
    MalformedXml(String),
    /// A `process` element had no `id` attribute.
    ProcessWithoutId,
    /// A `sequenceFlow` was missing `sourceRef`/`targetRef`.
    IncompleteSequenceFlow { process_id: String },
    /// The XML contained no `process` elements.
    NoProcess,
    /// A parsed process failed validation (e.g. no/many start events, dangling
    /// flow). Carries the underlying [`crate::BuildError`] message.
    InvalidProcess { process_id: String, reason: String },
    /// A `boundaryEvent` was missing `attachedToRef`, or its
    /// `errorEventDefinition` referenced an `error` that was not declared.
    InvalidBoundaryEvent { process_id: String, reason: String },
    /// A message event (`intermediateCatchEvent`/`boundaryEvent` with a
    /// `messageEventDefinition`) referenced a `message` that was not declared, or
    /// the declared message had no `zeebe:subscription correlationKey`.
    InvalidMessageEvent { process_id: String, reason: String },
    /// A `zeebe:linkedResource` omitted a Zeebe-required attribute
    /// (`resourceId`, `bindingType` or `resourceType`). Zeebe rejects such a
    /// deployment (`INVALID_ARGUMENT` → HTTP 400) rather than silently dropping
    /// the link, so Nano surfaces it as a hard parse error for parity.
    InvalidLinkedResource { task_id: String, attribute: String },
    /// A user task's `zeebe:formDefinition` declared a `bindingType` value that
    /// is not one of Camunda's three bindings (`latest`, `deployment`,
    /// `versionTag`) — including an explicitly empty `bindingType=""`, which is
    /// a present, explicit value and must not be silently treated as the
    /// `latest` default (the #1190 fail-loud rule). The three real bindings are
    /// all implemented (#1193); only an *unrecognised* value is rejected.
    /// `binding_type` carries the offending attribute value.
    UnsupportedUserTaskFormBinding {
        task_id: String,
        binding_type: String,
    },
    /// A dangling QName/id reference: a reference site (`kind`) points at an
    /// `id` that is not declared anywhere the reference can resolve against.
    ///
    /// This is the **single** unresolved-reference variant shared across the
    /// whole deploy-validation-parity epic (#850). `kind` names the reference
    /// site — one of `"incoming"`, `"outgoing"`, `"messageRef"`, `"errorRef"`,
    /// `"signalRef"`, `"escalationRef"`, `"linkThrow"`, `"default"`,
    /// `"attachedToRef"` — and `id` is the dangling id. Zeebe resolves these
    /// QName references eagerly and rejects an unresolved one at deploy with
    /// `INVALID_ARGUMENT`, so Nano does the same rather than silently accepting.
    /// Sibling validators reuse this variant; do **not** add per-ref bespoke
    /// variants. `process_id` names the owning `<process>` and `from_node` the
    /// id of the element that *declared* the dangling reference, so a deploy
    /// failure and its regression tests can attribute the reference to its
    /// declaring element rather than losing that context.
    UnresolvedReference {
        kind: String,
        id: String,
        process_id: String,
        from_node: String,
    },
    /// An unmodelled flow-element tag or event definition that Nano's parser
    /// does not recognise (e.g. `complexGateway`, `transaction`,
    /// `cancelEventDefinition`). Zeebe only transforms known element types
    /// and rejects the rest at deploy; Nano surfaces the offending `tag` and
    /// `element_id` rather than silently dropping it. Raised by the
    /// `unsupported_elements` validator (#853).
    UnsupportedElement { tag: String, element_id: String },
    /// A gateway with more than one outgoing flow declared a non-default branch
    /// with no condition (an exclusive/inclusive gateway must gate every
    /// non-default outgoing branch with a condition, or make it the `default`).
    /// Raised by the `gateway_conditions` validator (#854).
    InvalidGateway {
        process_id: String,
        gateway_id: String,
        reason: String,
    },
    /// A `zeebe:executionListener` was declared on an element where it can never
    /// fire, so accepting it would silently store a listener that creates no job
    /// (#1197). Rather than mis-advertise support, Nano rejects the deploy. The
    /// unsupported placements are:
    /// * a **multi-incoming parallel gateway** (a join): it synchronises tokens
    ///   and completes without running the listener-aware activation body or the
    ///   end-listener chain, so neither a `start` nor an `end` listener fires.
    /// * the **`start` listener of a multi-incoming inclusive gateway** (a join):
    ///   the join is synchronised on activation and short-circuits the activation
    ///   body, so its `start` listener never fires. An inclusive-join **`end`**
    ///   listener IS supported (an accepted join runs the end-listener chain
    ///   before routing) and is not rejected.
    /// * Single-incoming parallel/inclusive gateways (splits) and exclusive merges
    ///   run the normal activation body, so their listeners fire and are fine.
    /// * a **compensation boundary event**: a passive structural marker, armed
    ///   implicitly and never entered by token flow, so it has no lifecycle to
    ///   hang a listener on.
    /// * a **sequence flow**: an edge, not an `Element`, so it has no lifecycle
    ///   to run a "take" listener on. Sequence-flow ("take") listeners are a
    ///   deferred subset (#1198); until then a declared one is rejected rather
    ///   than dropped or hoisted onto the enclosing sub-process.
    /// * a **tool of an ad-hoc sub-process**: a leaf tool is pruned and a
    ///   retained embedded-sub-process tool is activated/completed with direct
    ///   lifecycle events, both bypassing the shared listener gate, so a listener
    ///   on it could never create a job. Rejected rather than accepted dead.
    /// * the **`end` listener of a terminate end event**: completing a terminate
    ///   end drives a scope-wide teardown that emits completion directly,
    ///   bypassing the end-listener chain. Its **`start`** listener IS supported
    ///   (it fires through the activation start-listener gate), so only the dead
    ///   `end` phase is rejected.
    /// * a listener on a **surplus signal start event**: Nano has no dedicated
    ///   signal-start kind, so a second signal start is demoted to an inert throw
    ///   event with no incoming flow that is never activated or completed — a
    ///   listener captured on it could never fire.
    /// * a **process-level listener**: a `zeebe:executionListener` declared under
    ///   the `<process>`'s own extension elements has no open flow-node owner on
    ///   the `io_stack` (the process itself never rides it), so it is rejected
    ///   naming the process as owner rather than dropped — a process has no
    ///   activate/complete lifecycle to run it.
    UnsupportedExecutionListener {
        process_id: String,
        element_id: String,
        reason: String,
    },
    /// A `zeebe:taskListener` was declared on an element that is not a user task
    /// (#1197). Task-listener jobs are created only on the user-task runtime
    /// path, so a task listener anywhere else is accepted by the lenient parser
    /// yet could never create a job. Rather than store (or silently drop) a dead
    /// task listener, Nano rejects the deploy and names the offending element.
    /// Rejected placements include:
    /// * a non-user-task flow node (e.g. a `receiveTask`, which now rides the
    ///   `io_stack` for its *execution* listeners) — caught by the build-time
    ///   node scan;
    /// * a **boundary event** or a **sequence flow** — neither enters the
    ///   `io_stack`, so the listener is rejected at parse with the real owner
    ///   named, rather than dropped (top-level) or hoisted onto the enclosing
    ///   sub-process (nested);
    /// * a **user-task tool of an ad-hoc sub-process** — the tool is flattened
    ///   into the non-executable ad-hoc catalog and activated through a direct
    ///   path that never runs task listeners.
    /// * a **process-level task listener** — a `zeebe:taskListener` declared
    ///   under the `<process>`'s own extension elements has no open flow-node
    ///   owner on the `io_stack`, so it is rejected naming the process as owner
    ///   rather than dropped; a process is not a user task.
    UnsupportedTaskListener {
        process_id: String,
        element_id: String,
        reason: String,
    },
    /// A process's start events violate Zeebe's start-event rules: it has no
    /// start event ("must have at least one start event"), or it declares more
    /// than one *none* start event ("multiple none start events are not
    /// allowed"). Raised by the `start_events` validator (#855).
    InvalidStartEvents { process_id: String, reason: String },
    /// An end event declares an outgoing sequence flow (an end event must have
    /// no outgoing flow). Raised by the `cheap_rules` validator (#856).
    InvalidEndEvent {
        process_id: String,
        element_id: String,
        reason: String,
    },
    /// Two start events in one process correlate on the same message or signal
    /// (no duplicate message/signal start events). `correlation_kind` is
    /// `"message"` or `"signal"` and `reference` is the shared id/name. Raised
    /// by the `cheap_rules` validator (#856).
    DuplicateStartEvent {
        process_id: String,
        correlation_kind: String,
        reference: String,
        reason: String,
    },
    /// A required `zeebe:taskDefinition` attribute is present but empty (its
    /// `type` — and `retries` when declared — must be non-empty). Raised by the
    /// `cheap_rules` validator (#856).
    InvalidTaskDefinition {
        process_id: String,
        task_id: String,
        attribute: String,
        reason: String,
    },
    /// A `zeebe:agentDefinition` extension marker was malformed or misplaced: its
    /// `agentType` was missing/unknown, or it violated the placement rules from
    /// Camunda's `AgentDefinitionValidator` — `agentType="aiAgentTask"` is only
    /// valid on a `serviceTask` and `agentType="aiAgentSubProcess"` is only valid
    /// on an `adHocSubProcess`. Zeebe rejects such a deployment at transform time
    /// (`INVALID_ARGUMENT`), so Nano surfaces it as a hard parse error for parity.
    InvalidAgentDefinition {
        process_id: String,
        element_id: String,
        reason: String,
    },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::MalformedXml(detail) => write!(f, "malformed BPMN XML: {detail}"),
            ParseError::ProcessWithoutId => write!(f, "<process> element has no id"),
            ParseError::IncompleteSequenceFlow { process_id } => write!(
                f,
                "process {process_id} has a sequenceFlow without sourceRef/targetRef"
            ),
            ParseError::NoProcess => write!(f, "no <process> element found"),
            ParseError::InvalidProcess { process_id, reason } => {
                write!(f, "invalid process {process_id}: {reason}")
            }
            ParseError::InvalidBoundaryEvent { process_id, reason } => {
                write!(
                    f,
                    "invalid boundary event in process {process_id}: {reason}"
                )
            }
            ParseError::InvalidMessageEvent { process_id, reason } => {
                write!(f, "invalid message event in process {process_id}: {reason}")
            }
            ParseError::InvalidLinkedResource { task_id, attribute } => {
                write!(
                    f,
                    "linkedResource on '{task_id}' is missing required attribute '{attribute}'"
                )
            }
            ParseError::UnsupportedUserTaskFormBinding {
                task_id,
                binding_type,
            } => write!(
                f,
                "user task '{task_id}' declares an unknown formDefinition bindingType \
'{binding_type}': expected one of 'latest', 'deployment' or 'versionTag'"
            ),
            ParseError::UnresolvedReference {
                kind,
                id,
                process_id,
                from_node,
            } => write!(
                f,
                "process {process_id}: unresolved {kind} reference to '{id}' on element '{from_node}': the referenced element is not declared"
            ),
            ParseError::UnsupportedElement { tag, element_id } => write!(
                f,
                "unsupported element <{tag}> (id '{element_id}'): Nano does not model this construct"
            ),
            ParseError::InvalidGateway {
                process_id,
                gateway_id,
                reason,
            } => write!(
                f,
                "process {process_id}: gateway '{gateway_id}' is invalid: {reason}"
            ),
            ParseError::UnsupportedExecutionListener {
                process_id,
                element_id,
                reason,
            } => write!(
                f,
                "process {process_id}: execution listener on '{element_id}' is not supported: {reason}"
            ),
            ParseError::UnsupportedTaskListener {
                process_id,
                element_id,
                reason,
            } => write!(
                f,
                "process {process_id}: task listener on '{element_id}' is not supported: {reason}"
            ),
            ParseError::InvalidStartEvents { process_id, reason } => {
                write!(f, "process {process_id}: {reason}")
            }
            ParseError::InvalidEndEvent {
                process_id,
                element_id,
                reason,
            } => write!(
                f,
                "process {process_id}: end event '{element_id}' is invalid: {reason}"
            ),
            ParseError::DuplicateStartEvent {
                process_id,
                correlation_kind,
                reference,
                reason,
            } => write!(
                f,
                "process {process_id}: duplicate {correlation_kind} start event on '{reference}': {reason}"
            ),
            ParseError::InvalidTaskDefinition {
                process_id,
                task_id,
                attribute,
                reason,
            } => write!(
                f,
                "process {process_id}: task '{task_id}' zeebe:taskDefinition attribute '{attribute}' is invalid: {reason}"
            ),
            ParseError::InvalidAgentDefinition {
                process_id,
                element_id,
                reason,
            } => write!(
                f,
                "process {process_id}: zeebe:agentDefinition on '{element_id}' is invalid: {reason}"
            ),
        }
    }
}

impl std::error::Error for ParseError {}
