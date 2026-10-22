//! The streaming BPMN walk: a single hand-rolled pass over the token stream
//! that dispatches each recognised tag into the accumulating [`ProcessAcc`]
//! (node accumulation and attribute capture live in [`super::build`]). This is
//! the parser's tag-dispatch core, split out of the former monolithic
//! `bpmn.rs` (#1207); [`super::parse_bpmn`] wraps it with post-parse validation.

use super::build::*;
use super::*;

/// Parses `xml` into `(raw capture, built definition)` pairs, one per
/// `<process>`, *without* running the post-parse validators. This is the single
/// source of truth for the streaming parse; [`parse_bpmn`] is a thin wrapper
/// that runs validation over the pairs. Kept separate so tests can inspect the
/// raw [`ProcessCapture`](crate::validate::ProcessCapture) a validator would see
/// (e.g. unmodelled-element attribution) directly, before any validator either
/// consumes it or rejects the definition.
pub(crate) fn parse_with_captures(
    xml: &str,
) -> Result<Vec<(crate::validate::ProcessCapture, ProcessDefinition)>, ParseError> {
    let tokens = tokenize(xml).map_err(|e| ParseError::MalformedXml(e.0))?;

    let mut processes: Vec<ProcessAcc> = Vec::new();
    let mut current: Option<ProcessAcc> = None;
    // Index of the service task currently being read (to attach its job type).
    let mut cur_service_task: Option<usize> = None;
    // Index of the plain `task`/`manualTask` currently being read. Tracked purely
    // so its close handler can pop the io_stack only when the open handler
    // actually pushed (an id-less task is never pushed) — mirroring the
    // service/user/call trackers.
    let mut cur_plain_task: Option<usize> = None;
    // Index of the user task currently being read (to attach assignment,
    // scheduling and priority expressions from its Zeebe extension elements).
    let mut cur_user_task: Option<usize> = None;
    // Index of the sequence flow currently being read (to attach a condition).
    let mut cur_flow: Option<usize> = None;
    let mut condition_text: Option<String> = None;
    // A separate buffer for a conditional-event's nested `<condition>` FEEL text
    // (distinct from a sequence flow's `conditionExpression`).
    let mut event_condition_text: Option<String> = None;
    // Buffer for the text content of a flow node's `<incoming>`/`<outgoing>`
    // child (a QName reference to a sequenceFlow), plus the owning node id and
    // reference direction so it can be resolved against declared flows later.
    let mut flow_ref_text: Option<String> = None;
    let mut flow_ref_owner: Option<String> = None;
    let mut flow_ref_dir: &'static str = "outgoing";
    // A buffer for a multi-instance `<completionCondition>` FEEL text, and the
    // index of the activity whose `multiInstanceLoopCharacteristics` is open.
    let mut completion_condition_text: Option<String> = None;
    let mut cur_multi_instance: Option<usize> = None;
    // The boundary event currently being read (to attach its errorEventDefinition).
    let mut cur_boundary: Option<PendingBoundary> = None;
    // Index of the intermediate catch event currently being read (timer or
    // message), and a buffer for its nested `timeDuration` text while inside it.
    let mut cur_intermediate: Option<usize> = None;
    // Index of the intermediate *throw* event currently being read, so a nested
    // `linkEventDefinition` (a throw link) is captured with its source node.
    // Throw events are pass-throughs for execution, but the reference-integrity
    // validator (#851) needs the link name to check throw↔catch pairing.
    let mut cur_throw: Option<usize> = None;
    // Index of the end event currently being read, so a nested `zeebe:ioMapping`
    // (an output mapping projecting a variable when the token reaches this end
    // event, e.g. a per-branch outcome inside a sub-process) attaches to the end
    // event itself rather than falling through to the innermost enclosing
    // activity on the `io_stack` (its sub-process). Without this, several end
    // events in one sub-process — each mapping the same target — all hoist onto
    // the sub-process, and the last-parsed one clobbers the rest at sub-process
    // completion (they never attach to the reached end event that should apply).
    let mut cur_end: Option<usize> = None;
    // Index of the call activity currently being read, so a nested
    // `zeebe:calledElement processId="…"` child can record its callee.
    let mut cur_call: Option<usize> = None;
    // Index of the gateway currently being read (any of exclusive/parallel/
    // inclusive/event-based). A gateway is a pass-through element that can carry
    // `start`/`end` execution listeners (ADR 0037 — listeners apply uniformly to
    // any element); it is pushed onto the `io_stack` while open so a nested
    // `zeebe:executionListener` attaches to the gateway itself rather than being
    // dropped (top-level gateway) or mis-attached to the enclosing sub-process
    // (nested gateway) — issue #1197. Gateways never nest another flow node, so a
    // single pointer suffices to guard the balancing `io_stack` pop on close.
    let mut cur_gateway: Option<usize> = None;
    let mut duration_text: Option<String> = None;
    // Index of the start event currently being read (to attach a nested
    // messageEventDefinition or timerEventDefinition), and a buffer for a timer
    // start event's nested `timeCycle` text while inside it.
    let mut cur_start: Option<usize> = None;
    let mut cycle_text: Option<String> = None;
    // Buffer for a timer event's nested `timeDate` (an absolute FEEL/ISO instant).
    let mut date_text: Option<String> = None;
    // Definitions-level `<error id=… errorCode=…>` declarations: id -> code.
    let mut errors: HashMap<String, String> = HashMap::new();
    // Definitions-level `<message id=… name=…>` declarations, with the
    // correlation-key variable from a nested `zeebe:subscription`: id -> decl.
    let mut messages: HashMap<String, MessageDecl> = HashMap::new();
    // Id of the `<message>` currently being read (to attach its subscription).
    let mut cur_message: Option<String> = None;
    // Definitions-level `<signal id=… name=…>` declarations: id -> name.
    let mut signals: HashMap<String, String> = HashMap::new();
    // Definitions-level `<escalation id=… name=…>` declarations: id -> name.
    // Captured so the reference-integrity validator (#851) can resolve an
    // `escalationRef` against a declared escalation. The parser does not model
    // escalation execution today; this only records the declarations.
    let mut escalations: HashMap<String, String> = HashMap::new();
    // Stack of activity node indices that can carry a `zeebe:ioMapping`, so a
    // nested `zeebe:input`/`zeebe:output` attaches to the innermost open
    // activity. `in_io_mapping` gates input/output reads to a real ioMapping.
    let mut io_stack: Vec<usize> = Vec::new();
    // Stack of currently-open elements, each recording the index of the flow
    // node it opened (`Some`) or `None` for a non-flow-node element. A nested
    // `<incoming>`/`<outgoing>` is attributed to the nearest enclosing flow node
    // (the top-most `Some`), not merely the most recently *added* node: for a
    // container flow node (`subProcess`/`adHocSubProcess`) whose
    // `<incoming>`/`<outgoing>` follow its nested elements, the last-added node
    // is an already-closed child, so a single pointer would misattribute the
    // reference. One entry is pushed per non-self-closing start tag and popped
    // per end tag (the tokenizer guarantees these balance), keeping the top in
    // lockstep with the open element the reference is a direct child of.
    let mut flow_node_stack: Vec<Option<usize>> = Vec::new();
    let mut in_io_mapping = false;
    // Gates `zeebe:executionListener` reads to a real `zeebe:executionListeners`
    // container; each listener attaches to the innermost open flow node on the
    // io_stack (ADR 0037) — activities, gateways, start events, and end events
    // are all pushed there, so listeners land on the correct element (#1197). A
    // boundary event is buffered in `cur_boundary` rather than on the io_stack,
    // so its listeners are routed to the pending boundary and re-attached by id
    // at build; sequence-flow ("take") listeners remain unmodelled.
    let mut in_execution_listeners = false;
    // Gates `zeebe:taskListener` reads to a real `zeebe:taskListeners` container
    // (ADR 0037 §6). Task listeners attach to the innermost open user task.
    let mut in_task_listeners = false;
    // Gates `zeebe:header` reads to a real `zeebe:taskHeaders` container; each
    // header attaches to the innermost open activity on the io_stack.
    let mut in_task_headers = false;
    // Gates `zeebe:linkedResource` reads to a real `zeebe:linkedResources`
    // container; each link attaches to the innermost open activity on the
    // io_stack. Without this a stray `linkedResource` tag elsewhere in
    // `extensionElements` (or from another namespace) would be treated as a link.
    let mut in_linked_resources = false;
    // Depth of the currently-open `<extensionElements>` subtree(s). Every child
    // of `extensionElements` is foreign-namespace vendor metadata (Zeebe's
    // `zeebe:*`, Nano's semantic `nano:*`, or any other), *not* a BPMN flow
    // element or event definition — the children Nano reads are consumed by the
    // explicit extension arms above; everything else is noise Zeebe likewise
    // ignores rather than failing the deploy. So while this is `> 0` the
    // flow-element catch-all must **not** record a tag as unmodelled, or a
    // `<nano:cost>` / stray `<zeebe:header>` would be mis-reported as an
    // unsupported element. A self-closing `<extensionElements/>` has no children
    // and emits no end tag, so it never increments this.
    let mut extension_depth: u32 = 0;

    for token in &tokens {
        // An end tag closes an element: balance the open-element stack pushed on
        // each non-self-closing start tag (see `flow_node_stack`). Done here so
        // the per-tag `Token::End` arm below stays a plain match on the tag name.
        if matches!(token, Token::End { .. }) {
            flow_node_stack.pop();
        }
        match token {
            Token::Start {
                name,
                attrs,
                self_closing,
            } => {
                // Number of flow nodes before dispatching this start tag, so we
                // can tell whether it opened a new one (see `flow_node_stack`).
                let nodes_before = current.as_ref().map(|acc| acc.nodes.len());
                match local_name(name) {
                    "process" => {
                        let id = attr(attrs, "id").ok_or(ParseError::ProcessWithoutId)?;
                        let mut acc = ProcessAcc::new(id.to_string());
                        acc.name = attr(attrs, "name").map(str::to_string);
                        current = Some(acc);
                    }
                    // Definitions-level error declarations live outside <process>.
                    "error" => {
                        if let (Some(id), Some(code)) =
                            (attr(attrs, "id"), attr(attrs, "errorCode"))
                        {
                            errors.insert(id.to_string(), code.to_string());
                        }
                    }
                    // Definitions-level message declarations also live outside
                    // <process>; the correlation key arrives via a nested
                    // zeebe:subscription.
                    "message" => {
                        if let Some(id) = attr(attrs, "id") {
                            messages.insert(
                                id.to_string(),
                                MessageDecl {
                                    name: attr(attrs, "name").unwrap_or(id).to_string(),
                                    correlation_key: None,
                                },
                            );
                            if !self_closing {
                                cur_message = Some(id.to_string());
                            }
                        }
                    }
                    // Definitions-level signal declarations live outside
                    // <process>; signals correlate by name only.
                    "signal" => {
                        if let Some(id) = attr(attrs, "id") {
                            signals.insert(
                                id.to_string(),
                                attr(attrs, "name").unwrap_or(id).to_string(),
                            );
                        }
                    }
                    // Definitions-level escalation declarations live outside
                    // <process>; referenced by an `escalationEventDefinition
                    // escalationRef`. The value stored is the `escalationCode`
                    // (empty when absent) — the code an escalation throw raises
                    // and an escalation boundary catches (Zeebe matches by code,
                    // not name). The map key (id) drives reference-integrity
                    // resolution (#851).
                    "escalation" => {
                        if let Some(id) = attr(attrs, "id") {
                            escalations.insert(
                                id.to_string(),
                                attr(attrs, "escalationCode").unwrap_or("").to_string(),
                            );
                        }
                    }
                    // zeebe:subscription correlationKey, nested in the current
                    // message's extensionElements.
                    "subscription" => {
                        if let (Some(mid), Some(expr)) =
                            (cur_message.as_ref(), attr(attrs, "correlationKey"))
                        {
                            if let Some(decl) = messages.get_mut(mid) {
                                decl.correlation_key = parse_correlation_key(expr);
                            }
                        }
                    }
                    tag if current.is_some() => {
                        // The `current.is_some()` guard proves this is `Some`.
                        #[allow(
                            clippy::expect_used,
                            reason = "the `current.is_some()` match guard proves this is Some"
                        )]
                        let acc = current.as_mut().expect("current process set");
                        match tag {
                            "startEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::Start);
                                // Push onto io_stack so a nested execution listener
                                // attaches to the start event, not the enclosing
                                // scope (#1197). A self-closing `<startEvent/>` has
                                // no children and no end tag, so only a real open
                                // element enters the io_stack (popped on
                                // `</startEvent>` when `cur_start` is set).
                                if !self_closing {
                                    cur_start = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "endEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::End);
                                // An end event can carry a `zeebe:ioMapping`
                                // (output) or execution listeners; push it so they
                                // attach to the end event, not the enclosing
                                // sub-process. A self-closing `<endEvent/>` has no
                                // children and no end tag, so only a real open
                                // element enters the io_stack (popped on
                                // `</endEvent>` when `cur_end` is set).
                                if !self_closing {
                                    cur_end = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "exclusiveGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::Exclusive);
                                // Capture the `default` flow id so it is selected
                                // only as a fallback (not by document order).
                                if let (Some(i), Some(d)) = (idx, attr(attrs, "default")) {
                                    acc.nodes[i].default_flow = Some(d.to_string());
                                }
                                // Push onto io_stack so a nested execution listener
                                // attaches to the gateway, not the enclosing scope
                                // (#1197). Self-closing gateways carry no children.
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "parallelGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::Parallel);
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // An inclusive (OR) gateway: a conditional split that
                            // may take several outgoing flows, and a synchronising
                            // join. Capture its `default` flow so it is only taken
                            // when no conditional flow matches (mirroring the
                            // exclusive gateway).
                            "inclusiveGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::Inclusive);
                                if let (Some(i), Some(d)) = (idx, attr(attrs, "default")) {
                                    acc.nodes[i].default_flow = Some(d.to_string());
                                }
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "eventBasedGateway" => {
                                let idx = acc.add_node(attrs, NodeKind::EventBased);
                                if !self_closing {
                                    cur_gateway = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "serviceTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if !self_closing {
                                    cur_service_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // A send task performs work via a job worker exactly
                            // like a service task: Zeebe/C8 execute it against its
                            // `zeebe:taskDefinition` (job type from that, else the
                            // element id). It is the throwing cousin of
                            // `receiveTask` (#1009); model it as a job-based
                            // service task so a flow into it resolves and it
                            // activates a single job rather than being dropped as
                            // an unmodelled element (the misleading
                            // "unknown target element" deploy error, #1168).
                            "sendTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if !self_closing {
                                    cur_service_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // A business-rule task (DMN) and a script task are
                            // both treated as service-task-like work steps: they
                            // run a job (its type taken from a nested
                            // zeebe:taskDefinition / zeebe:calledDecision, else
                            // defaulting to the element id). Nano has no DMN/script
                            // evaluator, but for trace generation and replay the
                            // step is faithfully a single job activation.
                            //
                            // When a native DMN decision engine is built, it is to
                            // be signed for Sebastian Menski, creator of Camunda's
                            // engine-dmn (see docs: engineer-signature naming).
                            "businessRuleTask" | "scriptTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if !self_closing {
                                    cur_service_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // zeebe:calledDecision decisionId="…" resultVariable="…"
                            // on a business-rule task — bind the node to a native
                            // DMN decision (evaluated in-engine, no job).
                            "calledDecision" => {
                                if let (Some(idx), Some(d)) =
                                    (cur_service_task, attr(attrs, "decisionId"))
                                {
                                    acc.nodes[idx].decision_id = Some(d.to_string());
                                    acc.nodes[idx].decision_result_variable =
                                        attr(attrs, "resultVariable").map(str::to_string);
                                }
                            }
                            "userTask" => {
                                let idx = acc.add_node(attrs, NodeKind::User);
                                if !self_closing {
                                    cur_user_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "callActivity" => {
                                // A call activity invokes another process. The
                                // callee id may be a `calledElement` attribute
                                // (Camunda 7) or a nested `zeebe:calledElement
                                // processId="…"` child (Camunda 8/Zeebe), captured
                                // below. Nano executes call activities natively —
                                // on activation the engine spawns a child process
                                // instance of the callee and completes the token
                                // when the child finishes (see the `CallActivity`
                                // arm of `Engine::run_activation_body`). Inline
                                // expansion (ProcessDefinition::inline_call_activities)
                                // is a legacy opt-in used by the processos harness,
                                // not the default execution path.
                                let idx = acc.add_node(attrs, NodeKind::Call);
                                if let (Some(i), Some(c)) = (idx, attr(attrs, "calledElement")) {
                                    acc.nodes[i].called_process_id = Some(c.to_string());
                                }
                                if !self_closing {
                                    cur_call = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // zeebe:calledElement processId="…" — the Camunda 8
                            // form of a call activity's callee reference. Its
                            // optional propagateAllParentVariables /
                            // propagateAllChildVariables attributes control Zeebe
                            // variable propagation across the call boundary; each
                            // defaults to `true` when absent (captured here as
                            // `Some(false)` only for an explicit `="false"`, so the
                            // builder can apply the Zeebe default).
                            "calledElement" => {
                                if let Some(idx) = cur_call {
                                    if let Some(p) = attr(attrs, "processId") {
                                        acc.nodes[idx].called_process_id = Some(p.to_string());
                                    }
                                    // Only record the explicit `="false"` case;
                                    // an explicit `="true"` is treated the same as
                                    // absent (left `None`) so the builder applies
                                    // the Zeebe `true` default, avoiding redundant
                                    // `Some(true)` values.
                                    if attr(attrs, "propagateAllParentVariables") == Some("false") {
                                        acc.nodes[idx].propagate_all_parent_variables = Some(false);
                                    }
                                    if attr(attrs, "propagateAllChildVariables") == Some("false") {
                                        acc.nodes[idx].propagate_all_child_variables = Some(false);
                                    }
                                }
                            }
                            "subProcess" => {
                                // An embedded sub-process: register it, then push
                                // its scope so nested nodes are tagged as
                                // contained in it until its end tag.
                                let idx = acc.add_node(attrs, NodeKind::SubProcess);
                                if let Some(i) = idx {
                                    acc.nodes[i].is_event_subprocess =
                                        attr(attrs, "triggeredByEvent") == Some("true");
                                }
                                if let Some(id) = attr(attrs, "id") {
                                    if !self_closing {
                                        acc.scope_stack.push(id.to_string());
                                    }
                                }
                                if !self_closing {
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // An ad-hoc sub-process — notably the Camunda 8 agentic
                            // AI agent (`io.camunda.agenticai:aiagent-job-worker`),
                            // which carries its own zeebe:taskDefinition. At the
                            // parent token-flow level it behaves as a single
                            // job-bearing activity: the ad-hoc worker activates, runs
                            // and its outgoing flow fires on completion. The
                            // contained "tool" activities are invoked out-of-band by
                            // the worker (dynamically, not by token flow), so they are
                            // parsed into the ad-hoc scope and pruned at build time —
                            // leaving the ad-hoc itself as one Service job (type from
                            // its taskDefinition, else its id). Push its scope so any
                            // contained elements are tagged for pruning.
                            "adHocSubProcess" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if let Some(i) = idx {
                                    acc.nodes[i].is_adhoc = true;
                                    // BPMN `cancelRemainingInstances` defaults to
                                    // `true`; only an explicit "false" defers.
                                    acc.nodes[i].adhoc_cancel_remaining_instances =
                                        attr(attrs, "cancelRemainingInstances") != Some("false");
                                }
                                if !self_closing {
                                    cur_service_task = idx;
                                    if let Some(id) = attr(attrs, "id") {
                                        acc.scope_stack.push(id.to_string());
                                    }
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "boundaryEvent" => {
                                // Buffered until end: kept only if it carries an
                                // errorEventDefinition (error boundary), a
                                // timerEventDefinition (timer boundary) or a
                                // messageEventDefinition (message boundary).
                                // `cancelActivity="false"` marks it
                                // non-interrupting (default interrupting).
                                //
                                // Only open the pending buffer for a
                                // non-self-closing tag (#1197): a self-closing
                                // `<boundaryEvent/>` emits no matching `Token::End`
                                // to run the `cur_boundary.take()` flush, so if it
                                // seeded `cur_boundary` the buffer would stay live
                                // and capture the *next* sibling's
                                // `zeebe:executionListener` (mis-attaching it, and
                                // letting a following sequence-flow listener bypass
                                // its intended rejection). A self-closing boundary
                                // carries no event-definition child anyway, so it is
                                // never a real boundary — not buffering it is safe.
                                if !self_closing {
                                    if let Some(id) = attr(attrs, "id") {
                                        cur_boundary = Some(PendingBoundary {
                                            id: id.to_string(),
                                            attached_to: attr(attrs, "attachedToRef")
                                                .map(str::to_string),
                                            error_ref: None,
                                            timer_duration_millis: None,
                                            timer_repeating: false,
                                            timer_expr: None,
                                            message_ref: None,
                                            interrupting: attr(attrs, "cancelActivity")
                                                != Some("false"),
                                            signal_ref: None,
                                            condition: None,
                                            compensation: false,
                                            escalation: false,
                                            escalation_ref: None,
                                            escalation_dup: false,
                                            start_listeners: Vec::new(),
                                            end_listeners: Vec::new(),
                                        });
                                    }
                                }
                            }
                            "errorEventDefinition" => {
                                let error_ref = attr(attrs, "errorRef").unwrap_or("").to_string();
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.error_ref = Some(error_ref);
                                } else if let Some(idx) =
                                    flow_node_stack.iter().rev().find_map(|e| *e)
                                {
                                    // An `errorRef` on a non-boundary error event
                                    // (an error end / throw event). Recorded so the
                                    // reference-integrity validator (#851) can
                                    // resolve it against declared `<error>`s.
                                    let node_id = acc.nodes[idx].id.clone();
                                    acc.error_refs_extra.push((node_id, error_ref));
                                }
                            }
                            // `escalationEventDefinition escalationRef="…"` on an
                            // escalation throw / end / boundary event. Escalation
                            // is modelled for execution (#1173): a throw/end raises
                            // the named escalation and a boundary catches it. The
                            // `escalationRef` resolves against a definitions-level
                            // `<escalation escalationCode="…">`; a *dangling* ref is
                            // still rejected by the reference-integrity validator
                            // (#851), which runs first. An escalation carrier on any
                            // *other* placement (an escalation intermediate catch or
                            // an event-subprocess escalation start — not modelled)
                            // is recorded as unmodelled so it is cleanly rejected.
                            "escalationEventDefinition" => {
                                let escalation_ref = attr(attrs, "escalationRef");
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    // A boundary carrier → an escalation boundary
                                    // event, built in `build` from the resolved
                                    // escalation code. A SECOND escalation
                                    // definition on the same boundary is a
                                    // last-wins overwrite of `escalation_ref`;
                                    // flag it so `build` rejects the ambiguous
                                    // multi-definition boundary (#1173).
                                    if boundary.escalation {
                                        boundary.escalation_dup = true;
                                    }
                                    boundary.escalation = true;
                                    boundary.escalation_ref = escalation_ref.map(str::to_string);
                                    if let Some(escalation_ref) = escalation_ref {
                                        acc.escalation_refs.push((
                                            boundary.id.clone(),
                                            escalation_ref.to_string(),
                                        ));
                                    }
                                } else if let Some(idx) =
                                    flow_node_stack.iter().rev().find_map(|e| *e)
                                {
                                    let node_id = acc.nodes[idx].id.clone();
                                    if let Some(escalation_ref) = escalation_ref {
                                        acc.escalation_refs
                                            .push((node_id.clone(), escalation_ref.to_string()));
                                    }
                                    // Only an intermediate throw or an end event
                                    // carrier is an escalation *throw*; any other
                                    // placement stays unmodelled (rejected at
                                    // deploy naming the construct).
                                    if matches!(
                                        acc.nodes[idx].kind,
                                        NodeKind::IntermediateThrow | NodeKind::End
                                    ) {
                                        // A SECOND escalation definition on the
                                        // same throw/end is a last-wins overwrite
                                        // of `escalation_ref`; flag it so `build`
                                        // rejects the ambiguous multi-definition
                                        // throw (#1173).
                                        if acc.nodes[idx].is_escalation_throw {
                                            acc.nodes[idx].escalation_throw_dup = true;
                                        }
                                        acc.nodes[idx].is_escalation_throw = true;
                                        acc.nodes[idx].escalation_ref =
                                            escalation_ref.map(str::to_string);
                                    } else {
                                        acc.unmodelled.push((
                                            "escalationEventDefinition".to_string(),
                                            node_id,
                                        ));
                                    }
                                }
                            }
                            // `linkEventDefinition name="…"` on an intermediate
                            // link throw or catch event. Recorded by direction so
                            // #851 can verify each throw link has a matching catch
                            // (Zeebe `ModelUtil.verifyLinkIntermediateEvents`).
                            // `compensateEventDefinition` on a boundary event
                            // (a compensation boundary marking its activity
                            // compensable) or on an intermediate throw / end
                            // event (a compensation throw). Modelled for
                            // execution; the boundary's handler is resolved from
                            // the `<association>` wiring at build.
                            "compensateEventDefinition" => {
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.compensation = true;
                                } else if let Some(idx) =
                                    flow_node_stack.iter().rev().find_map(|e| *e)
                                {
                                    // A compensation *throw* is modelled only on an
                                    // intermediateThrowEvent or endEvent — the two
                                    // node kinds the build step interprets
                                    // `is_compensation_throw` for. On any other node
                                    // kind (e.g. a start/catch event) the flag would
                                    // be silently dropped at build and never
                                    // rejected, so record the placement as unmodelled
                                    // and let the unsupported-elements validator
                                    // (#853) reject it at deploy instead.
                                    if matches!(
                                        acc.nodes[idx].kind,
                                        NodeKind::IntermediateThrow | NodeKind::End
                                    ) {
                                        acc.nodes[idx].is_compensation_throw = true;
                                    } else {
                                        acc.unmodelled.push((
                                            "compensateEventDefinition".to_string(),
                                            acc.nodes[idx].id.clone(),
                                        ));
                                    }
                                }
                            }
                            // `terminateEventDefinition` on an `endEvent` makes it
                            // a *terminate* end event: reaching it kills every
                            // other active token in the enclosing scope and
                            // completes that scope (Zeebe/Camunda terminate
                            // semantics). Recorded on the owning end-event node so
                            // the build step maps it to
                            // `ElementKind::TerminateEndEvent`. It is only
                            // meaningful on an `endEvent`; anywhere else it is
                            // ignored (deploy-parity leniency).
                            "terminateEventDefinition" => {
                                if let Some(idx) = cur_end {
                                    acc.nodes[idx].is_terminate = true;
                                }
                            }
                            // `<association sourceRef=… targetRef=…>` — wires a
                            // compensation boundary event to its
                            // `isForCompensation` handler activity. Recorded so
                            // the boundary's handler resolves at build; ignored
                            // for any other association.
                            "association" => {
                                if let (Some(source), Some(target)) =
                                    (attr(attrs, "sourceRef"), attr(attrs, "targetRef"))
                                {
                                    acc.associations
                                        .push((source.to_string(), target.to_string()));
                                }
                            }
                            "linkEventDefinition" => {
                                let link_name = attr(attrs, "name").unwrap_or("").to_string();
                                if let Some(idx) = cur_throw {
                                    let from_node = acc.nodes[idx].id.clone();
                                    let scope = acc.nodes[idx].parent.clone();
                                    acc.nodes[idx].link_name = Some(link_name.clone());
                                    acc.link_throws.push((link_name, from_node, scope));
                                } else if let Some(idx) = cur_intermediate {
                                    let scope = acc.nodes[idx].parent.clone();
                                    acc.nodes[idx].link_name = Some(link_name.clone());
                                    acc.link_catches.push((link_name, scope));
                                }
                            }
                            "messageEventDefinition" => {
                                // On an intermediate catch event or a boundary
                                // event, marks it a message event referencing a
                                // definitions-level <message>.
                                let message_ref =
                                    attr(attrs, "messageRef").unwrap_or("").to_string();
                                if let Some(idx) = cur_intermediate {
                                    acc.nodes[idx].message_ref = Some(message_ref);
                                } else if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.message_ref = Some(message_ref);
                                } else if let Some(idx) = cur_start {
                                    acc.nodes[idx].message_ref = Some(message_ref);
                                }
                            }
                            "signalEventDefinition" => {
                                // On an intermediate catch event or a boundary
                                // event, marks it a signal event referencing a
                                // definitions-level <signal>.
                                let signal_ref = attr(attrs, "signalRef").unwrap_or("").to_string();
                                if let Some(idx) = cur_intermediate {
                                    acc.nodes[idx].signal_ref = Some(signal_ref);
                                } else if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.signal_ref = Some(signal_ref);
                                } else if let Some(idx) = cur_start {
                                    // On a start event, marks it a *signal* start
                                    // (a typed start, distinct from a none start).
                                    // Recorded so the start-event count rule (#855)
                                    // does not mistake it for a second none start,
                                    // and so #851 can resolve its `signalRef`.
                                    acc.nodes[idx].signal_ref = Some(signal_ref);
                                }
                            }
                            "agentDefinition" => {
                                // zeebe:agentDefinition agentType="…" — the
                                // engine-native AgentInstance marker (Camunda
                                // stable/8.10). It attaches to the enclosing
                                // activity (a serviceTask or adHocSubProcess, both
                                // tracked as cur_service_task). Placement is
                                // validated at build: aiAgentTask only on a
                                // serviceTask, aiAgentSubProcess only on an
                                // adHocSubProcess (mirrors Camunda's
                                // AgentDefinitionValidator). A missing/unknown
                                // agentType, or the marker on any other element, is
                                // a hard parse error.
                                let raw = attr(attrs, "agentType").unwrap_or("");
                                let agent_type = crate::agent::AgentType::parse(raw);
                                if let Some(idx) = cur_service_task {
                                    let element_id = acc.nodes[idx].id.clone();
                                    let is_adhoc = acc.nodes[idx].is_adhoc;
                                    match agent_type {
                                        None => {
                                            let reason = if raw.is_empty() {
                                                "missing agentType attribute (expected aiAgentTask, aiAgentSubProcess or external)".to_string()
                                            } else {
                                                format!(
                                                    "unknown agentType '{raw}' (expected aiAgentTask, aiAgentSubProcess or external)"
                                                )
                                            };
                                            return Err(ParseError::InvalidAgentDefinition {
                                                process_id: acc.id.clone(),
                                                element_id,
                                                reason,
                                            });
                                        }
                                        Some(crate::agent::AgentType::AiAgentTask) if is_adhoc => {
                                            return Err(ParseError::InvalidAgentDefinition {
                                                process_id: acc.id.clone(),
                                                element_id,
                                                reason: "agentType 'aiAgentTask' is only valid on a serviceTask, not an adHocSubProcess".to_string(),
                                            });
                                        }
                                        Some(crate::agent::AgentType::AiAgentSubProcess)
                                            if !is_adhoc =>
                                        {
                                            return Err(ParseError::InvalidAgentDefinition {
                                                process_id: acc.id.clone(),
                                                element_id,
                                                reason: "agentType 'aiAgentSubProcess' is only valid on an adHocSubProcess, not a serviceTask".to_string(),
                                            });
                                        }
                                        Some(t) => acc.nodes[idx].agent_type = Some(t),
                                    }
                                } else {
                                    // Attribute the error to the nearest enclosing
                                    // flow node so it is diagnosable, rather than an
                                    // empty id (there is no owning task here).
                                    let element_id = flow_node_stack
                                        .iter()
                                        .rev()
                                        .find_map(|e| *e)
                                        .map(|idx| acc.nodes[idx].id.clone())
                                        .unwrap_or_default();
                                    return Err(ParseError::InvalidAgentDefinition {
                                        process_id: acc.id.clone(),
                                        element_id,
                                        reason: "zeebe:agentDefinition is only valid on a serviceTask or adHocSubProcess".to_string(),
                                    });
                                }
                            }
                            "taskDefinition" => {
                                // zeebe:taskDefinition type="…" retries="…" inside
                                // a service task.
                                if let Some(idx) = cur_service_task {
                                    if let Some(t) = attr(attrs, "type") {
                                        acc.nodes[idx].job_type = Some(t.to_string());
                                    }
                                    if let Some(r) = attr(attrs, "retries") {
                                        acc.nodes[idx].retries = Some(r.to_string());
                                    }
                                }
                            }
                            "script" => {
                                // zeebe:script expression="…" resultVariable="…"
                                // inside a script task: an inline FEEL script the
                                // engine evaluates on activation (no job). A
                                // scriptTask carrying BOTH attributes becomes an
                                // inline ScriptTask; one that instead declares a
                                // zeebe:taskDefinition stays job-based (Service).
                                if let Some(idx) = cur_service_task {
                                    if let (Some(expr), Some(rv)) =
                                        (attr(attrs, "expression"), attr(attrs, "resultVariable"))
                                    {
                                        acc.nodes[idx].script_expression = Some(expr.to_string());
                                        acc.nodes[idx].script_result_variable =
                                            Some(rv.to_string());
                                    }
                                }
                            }
                            "assignmentDefinition" => {
                                // zeebe:assignmentDefinition inside a user task.
                                if let Some(idx) = cur_user_task {
                                    let props = &mut acc.nodes[idx].user_task;
                                    props.assignee = attr(attrs, "assignee").map(str::to_string);
                                    props.candidate_groups =
                                        attr(attrs, "candidateGroups").map(str::to_string);
                                    props.candidate_users =
                                        attr(attrs, "candidateUsers").map(str::to_string);
                                }
                            }
                            "taskSchedule" => {
                                // zeebe:taskSchedule inside a user task.
                                if let Some(idx) = cur_user_task {
                                    let props = &mut acc.nodes[idx].user_task;
                                    props.due_date = attr(attrs, "dueDate").map(str::to_string);
                                    props.follow_up_date =
                                        attr(attrs, "followUpDate").map(str::to_string);
                                }
                            }
                            "priorityDefinition" => {
                                // zeebe:priorityDefinition inside a user task
                                // (task scheduling) or a service task (job
                                // activation priority).
                                if let Some(idx) = cur_user_task {
                                    acc.nodes[idx].user_task.priority =
                                        attr(attrs, "priority").map(str::to_string);
                                } else if let Some(idx) = cur_service_task {
                                    acc.nodes[idx].job_priority =
                                        attr(attrs, "priority").map(str::to_string);
                                }
                            }
                            "formDefinition" => {
                                // zeebe:formDefinition inside a user task (its
                                // form) or a start event (the process start form).
                                // `formId` names an embedded / deployment form
                                // (resolved to a numeric form_key against the
                                // deployed forms at task creation);
                                // `externalReference` names an external form
                                // (surfaced verbatim). Zeebe declares exactly one
                                // of the two, so an `externalReference` wins and
                                // suppresses `formId` to keep them mutually
                                // exclusive downstream.
                                let form_id = attr(attrs, "formId")
                                    .filter(|s| !s.is_empty())
                                    .map(str::to_string);
                                let external_reference = attr(attrs, "externalReference")
                                    .filter(|s| !s.is_empty())
                                    .map(str::to_string);
                                if let Some(idx) = cur_user_task {
                                    let props = &mut acc.nodes[idx].user_task;
                                    props.external_form_reference = external_reference;
                                    props.form_id = if props.external_form_reference.is_some() {
                                        None
                                    } else {
                                        form_id
                                    };
                                    // Interim guard (#1190): the engine resolves
                                    // a user-task deployed form to the *latest*
                                    // deployed form version at task creation. The
                                    // other two Camunda bindings (`deployment`,
                                    // `versionTag`) are not implemented; rather
                                    // than silently degrading them to `latest` (a
                                    // latent mis-binding), reject the deploy
                                    // loudly. The guard keys off the *raw*
                                    // `externalReference`: an external form
                                    // resolves no deployed version, so its binding
                                    // is moot and unaffected; every *other*
                                    // (deployed) form must carry only the default
                                    // binding. Only an **absent** `bindingType`
                                    // defaults — any present value other than the
                                    // literal `latest`, including an explicitly
                                    // empty `bindingType=""`, is an explicit
                                    // unimplemented binding and is rejected rather
                                    // than silently treated as the default.
                                    if acc.nodes[idx].user_task.external_form_reference.is_none() {
                                        if let Some(binding) =
                                            attr(attrs, "bindingType").filter(|s| *s != "latest")
                                        {
                                            return Err(
                                                ParseError::UnsupportedUserTaskFormBinding {
                                                    task_id: acc.nodes[idx].id.clone(),
                                                    binding_type: binding.to_string(),
                                                },
                                            );
                                        }
                                    }
                                } else if let Some(idx) = cur_start {
                                    // Start forms are formId-only by design: the
                                    // `GetStartProcessForm` contract resolves a
                                    // *deployed* form, which an external reference
                                    // (an externally-hosted form) is not. We
                                    // intentionally do not record
                                    // `externalReference` for start events;
                                    // external start forms are out of scope until
                                    // there is a read-model/API surface for them.
                                    acc.nodes[idx].start_form_id = form_id;
                                    let _ = external_reference;
                                }
                            }
                            "adHoc" => {
                                // zeebe:adHoc inside an adHocSubProcess: the
                                // agentic wiring — outputCollection/outputElement
                                // gather each activated tool's result, and
                                // activeElementsCollection (declarative variant)
                                // names elements to activate via FEEL. The ad-hoc
                                // container is tracked as cur_service_task.
                                if let Some(idx) = cur_service_task {
                                    let n = &mut acc.nodes[idx];
                                    n.adhoc_output_collection =
                                        attr(attrs, "outputCollection").map(str::to_string);
                                    n.adhoc_output_element =
                                        attr(attrs, "outputElement").map(str::to_string);
                                    n.adhoc_active_elements =
                                        attr(attrs, "activeElementsCollection").map(str::to_string);
                                }
                            }
                            "sequenceFlow" => {
                                let idx = acc.add_flow(attrs);
                                if !self_closing {
                                    cur_flow = idx;
                                }
                            }
                            "intermediateCatchEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::IntermediateCatch);
                                if !self_closing {
                                    cur_intermediate = idx;
                                    // A catch event can carry a `zeebe:ioMapping`:
                                    // its inputs apply when the event activates and
                                    // its outputs when it is triggered (e.g. a
                                    // loop-back counter `=round + 1 -> round`). Push
                                    // it so a nested `zeebe:input`/`zeebe:output`
                                    // attaches to the event, not a dropped `None`.
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // A throw event (none/escalation/signal/message throw)
                            // is a pure pass-through for path purposes: it routes
                            // straight to its outgoing flow. Any nested event
                            // definition is ignored.
                            "intermediateThrowEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::IntermediateThrow);
                                if !self_closing {
                                    cur_throw = idx;
                                    // A throw event can carry a `zeebe:ioMapping`
                                    // (e.g. a message throw). Push it onto the
                                    // io_stack — same defect class as end events —
                                    // so nested mappings attach to the throw event
                                    // rather than the enclosing activity. `cur_throw`
                                    // is Some iff the node had an id, matching the
                                    // guarded pop on `</intermediateThrowEvent>`.
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // An abstract `task` (or `manualTask`) has no
                            // execution semantics — Zeebe/C8 accept it and treat
                            // it as a pass-through. Model it as such (token
                            // completes on activation and takes its outgoing
                            // flow). Push it onto the io_stack so any
                            // `zeebe:ioMapping` on it still applies, mirroring the
                            // typed tasks.
                            "task" | "manualTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Task);
                                if !self_closing {
                                    cur_plain_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            // A receive task waits for a message. Nano's trace
                            // generator has no inbound correlation, so model it as
                            // a pass-through (the awaited event is assumed to
                            // arrive) rather than a perpetual block. Push it onto
                            // the io_stack like the other plain tasks so a
                            // `zeebe:ioMapping` or `zeebe:executionListeners`
                            // declared on it attaches to the receive task itself
                            // and fires through the shared pass-through listener
                            // gate — otherwise its listeners would fall through to
                            // `io_stack.last()` and be dropped at process root or
                            // hoisted onto the enclosing sub-process (#1197).
                            "receiveTask" => {
                                let idx = acc.add_node(attrs, NodeKind::IntermediateThrow);
                                if !self_closing {
                                    cur_plain_task = idx;
                                    if let Some(i) = idx {
                                        io_stack.push(i);
                                    }
                                }
                            }
                            "timeDuration"
                                if cur_intermediate.is_some()
                                    || cur_boundary.is_some()
                                    || cur_start.is_some() =>
                            {
                                duration_text = Some(String::new());
                            }
                            "timeCycle" if cur_start.is_some() || cur_boundary.is_some() => {
                                cycle_text = Some(String::new());
                            }
                            "timeDate"
                                if cur_intermediate.is_some()
                                    || cur_boundary.is_some()
                                    || cur_start.is_some() =>
                            {
                                date_text = Some(String::new());
                            }
                            "conditionExpression" if cur_flow.is_some() => {
                                condition_text = Some(String::new());
                            }
                            // A flow node's `<incoming>`/`<outgoing>` QName
                            // reference to a sequenceFlow. Its text content is the
                            // referenced flow id; captured against the owning
                            // (nearest enclosing) flow node so it can be resolved
                            // against declared flows at build time (Zeebe rejects
                            // an unresolved reference at deploy). The owner is the
                            // top-most `Some` on `flow_node_stack` — the flow node
                            // this element is a direct child of — which stays
                            // correct even when a container's `<incoming>`/
                            // `<outgoing>` follow its nested elements.
                            "incoming" | "outgoing" if !self_closing => {
                                if let Some(idx) = flow_node_stack.iter().rev().find_map(|e| *e) {
                                    flow_ref_owner = Some(acc.nodes[idx].id.clone());
                                    flow_ref_dir = if tag == "incoming" {
                                        "incoming"
                                    } else {
                                        "outgoing"
                                    };
                                    flow_ref_text = Some(String::new());
                                }
                            }
                            // A conditional event's FEEL `<condition>` (nested in
                            // a `conditionalEventDefinition` on a catch or boundary
                            // event). Distinct from a sequence flow's
                            // `conditionExpression` above.
                            "condition" if cur_intermediate.is_some() || cur_boundary.is_some() => {
                                event_condition_text = Some(String::new());
                            }
                            // A `multiInstanceLoopCharacteristics` child of an
                            // activity: mark that activity multi-instance. The
                            // FEEL-bearing details (inputCollection/inputElement/
                            // outputCollection/outputElement) live in a nested
                            // `zeebe:loopCharacteristics`; the standard BPMN
                            // `<completionCondition>` FEEL text is captured below.
                            "multiInstanceLoopCharacteristics" => {
                                if let Some(&idx) = io_stack.last() {
                                    let sequential = attr(attrs, "isSequential")
                                        .map(|v| v == "true")
                                        .unwrap_or(false);
                                    acc.nodes[idx].multi_instance =
                                        Some(crate::model::MultiInstance {
                                            sequential,
                                            ..Default::default()
                                        });
                                    if !self_closing {
                                        cur_multi_instance = Some(idx);
                                    }
                                }
                            }
                            // zeebe:loopCharacteristics inside a multi-instance
                            // activity: the input/output collection FEEL details.
                            "loopCharacteristics" => {
                                if let Some(idx) = cur_multi_instance {
                                    if let Some(mi) = acc.nodes[idx].multi_instance.as_mut() {
                                        if let Some(v) = attr(attrs, "inputCollection") {
                                            mi.input_collection = v.to_string();
                                        }
                                        mi.input_element =
                                            attr(attrs, "inputElement").map(str::to_string);
                                        mi.output_collection =
                                            attr(attrs, "outputCollection").map(str::to_string);
                                        mi.output_element =
                                            attr(attrs, "outputElement").map(str::to_string);
                                    }
                                }
                            }
                            // The standard BPMN `<completionCondition>` FEEL text
                            // nested in `multiInstanceLoopCharacteristics`, or a
                            // direct child of an ad-hoc container (ADR 0023 seam 4).
                            "completionCondition" => {
                                completion_condition_text = Some(String::new());
                            }
                            // A `<bpmn:extensionElements>` container. Its
                            // children are foreign vendor metadata (see
                            // `extension_depth`): the ones Nano reads are matched
                            // by the explicit arms above; the rest must not reach
                            // the flow-element catch-all as unmodelled elements.
                            // A self-closing container has no children, so only a
                            // real open element enters the subtree.
                            "extensionElements" => {
                                if !self_closing {
                                    extension_depth += 1;
                                }
                            }
                            // zeebe:ioMapping and its nested zeebe:input/output.
                            // input/output are only read inside an ioMapping that
                            // belongs to an open activity (the innermost on the
                            // io_stack).
                            "ioMapping" => {
                                in_io_mapping = true;
                            }
                            "taskHeaders" => {
                                // A self-closing `<zeebe:taskHeaders />` has no
                                // nested headers and emits no matching end tag, so
                                // only enter the container state for a real open
                                // element — otherwise the flag would stay stuck
                                // `true` and wrongly capture later `zeebe:header`s.
                                if !self_closing {
                                    in_task_headers = true;
                                }
                            }
                            // zeebe:header key="…" value="…" inside a
                            // zeebe:taskHeaders container: a static custom header
                            // attached to the innermost open activity. Surfaced
                            // verbatim on the activated job (Zeebe customHeaders).
                            "header" if in_task_headers => {
                                if let (Some(&idx), Some(key)) =
                                    (io_stack.last(), attr(attrs, "key"))
                                {
                                    let value = attr(attrs, "value").unwrap_or("").to_string();
                                    acc.nodes[idx].task_headers.insert(key.to_string(), value);
                                }
                            }
                            // zeebe:linkedResource inside zeebe:linkedResources: a
                            // declarative link from the innermost open job task to a
                            // deployed resource by id. Resolved to a concrete
                            // resourceKey at job activation and delivered in the
                            // `linkedResources` custom header (Zeebe parity). Gated
                            // on a real open `zeebe:linkedResources` container so a
                            // stray `linkedResource` tag elsewhere is not treated as
                            // a link (mirrors the `zeebe:taskHeaders` gate). A
                            // self-closing `<zeebe:linkedResources />` carries no
                            // children and emits no end tag, so only a real open
                            // element enters the container state.
                            "linkedResources" => {
                                if !self_closing {
                                    in_linked_resources = true;
                                }
                            }
                            "linkedResource" if in_linked_resources => {
                                // Zeebe's design-time validator requires
                                // `resourceId`, `bindingType` and `resourceType`
                                // (each non-empty) on every linkedResource. A
                                // link missing any of them is a hard deploy
                                // error: Zeebe rejects it with
                                // `INVALID_ARGUMENT` (HTTP 400) rather than
                                // silently dropping the link and leaving the
                                // task's `linkedResources` header empty at
                                // activation. Nano matches that parity here.
                                // `linkName` is intentionally *not* required
                                // (Zeebe's required set omits it); when absent it
                                // resolves to an empty link name. Like the other
                                // zeebe extension elements it attaches to the
                                // innermost open activity and is retained only
                                // where the built model keeps it (service tasks).
                                if let Some(&idx) = io_stack.last() {
                                    let nonempty =
                                        |a: &str| attr(attrs, a).filter(|v| !v.is_empty());
                                    for required in ["resourceId", "bindingType", "resourceType"] {
                                        if nonempty(required).is_none() {
                                            return Err(ParseError::InvalidLinkedResource {
                                                task_id: acc.nodes[idx].id.clone(),
                                                attribute: required.to_string(),
                                            });
                                        }
                                    }
                                    // The required-attribute loop above already
                                    // guaranteed each of these is present and
                                    // non-empty, so unwrap the invariant rather
                                    // than falling back to "" (a dead branch that
                                    // would only mask a future validation bug).
                                    #[allow(
                                        clippy::expect_used,
                                        reason = "required-attribute loop above validated these non-empty"
                                    )]
                                    let resource_id = nonempty("resourceId")
                                        .expect("resourceId validated non-empty above");
                                    #[allow(
                                        clippy::expect_used,
                                        reason = "required-attribute loop above validated these non-empty"
                                    )]
                                    let resource_type = nonempty("resourceType")
                                        .expect("resourceType validated non-empty above");
                                    #[allow(
                                        clippy::expect_used,
                                        reason = "required-attribute loop above validated these non-empty"
                                    )]
                                    let binding_type = match nonempty("bindingType")
                                        .expect("bindingType validated non-empty above")
                                    {
                                        "deployment" => crate::model::BindingType::Deployment,
                                        "versionTag" => crate::model::BindingType::VersionTag,
                                        // `latest` and any other non-empty value
                                        // default to latest (Zeebe's default).
                                        _ => crate::model::BindingType::Latest,
                                    };
                                    acc.nodes[idx].linked_resources.push(
                                        crate::model::LinkedResource {
                                            resource_id: resource_id.to_string(),
                                            binding_type,
                                            resource_type: resource_type.to_string(),
                                            version_tag: attr(attrs, "versionTag")
                                                .map(str::to_string),
                                            link_name: attr(attrs, "linkName")
                                                .unwrap_or("")
                                                .to_string(),
                                        },
                                    );
                                }
                            }
                            "input" | "output" if in_io_mapping => {
                                if let (Some(&idx), Some(source), Some(target)) = (
                                    io_stack.last(),
                                    attr(attrs, "source"),
                                    attr(attrs, "target"),
                                ) {
                                    let mapping = crate::model::Mapping {
                                        source: source.to_string(),
                                        target: target.to_string(),
                                    };
                                    if local_name(name) == "input" {
                                        acc.nodes[idx].io.inputs.push(mapping);
                                    } else {
                                        acc.nodes[idx].io.outputs.push(mapping);
                                    }
                                }
                            }
                            // zeebe:executionListeners and its nested
                            // zeebe:executionListener entries (ADR 0037). Each
                            // listener attaches to the innermost open flow node
                            // (an activity, gateway, start or end event) on the
                            // io_stack — not just activities.
                            "executionListeners" => {
                                // A self-closing `<zeebe:executionListeners />` has
                                // no nested listeners and emits no matching end tag,
                                // so only enter the container state for a real open
                                // element — otherwise the flag would stay stuck
                                // `true` and wrongly capture a later stray
                                // `zeebe:executionListener` onto whatever element is
                                // open on the io_stack (mirrors the `taskHeaders`
                                // gate).
                                if !self_closing {
                                    in_execution_listeners = true;
                                }
                            }
                            "executionListener" if in_execution_listeners => {
                                if let Some(job_type) = attr(attrs, "type") {
                                    // `eventType` defaults to "start" in Zeebe when
                                    // omitted.
                                    let event_type = match attr(attrs, "eventType") {
                                        Some("end") => crate::model::ListenerEventType::End,
                                        _ => crate::model::ListenerEventType::Start,
                                    };
                                    let retries = attr(attrs, "retries").map(str::to_string);
                                    let listener = crate::model::ExecutionListener {
                                        event_type,
                                        job_type: job_type.to_string(),
                                        retries,
                                    };
                                    // A boundary event is buffered in `cur_boundary`
                                    // and never enters the `io_stack` (it is a
                                    // sibling of its host, not a nested element), so
                                    // route its listeners to the pending boundary —
                                    // otherwise `io_stack.last()` would drop them
                                    // (top-level host) or mis-attach them to the
                                    // enclosing sub-process (#1197). A boundary's
                                    // children cannot open another listener owner, so
                                    // checking `cur_boundary` first is unambiguous.
                                    let sink = if let Some(boundary) = cur_boundary.as_mut() {
                                        Some((
                                            &mut boundary.start_listeners,
                                            &mut boundary.end_listeners,
                                        ))
                                    } else if let Some(flow_idx) = cur_flow {
                                        // A `<sequenceFlow>` is an edge, not an
                                        // `Element`, so it has no lifecycle to run
                                        // an execution listener on. Rather than
                                        // silently DROP a declared take listener —
                                        // the same dead-listener failure mode this
                                        // change rejects for joins and compensation
                                        // boundaries (deploy would succeed yet the
                                        // listener could never create a job) —
                                        // reject it at deploy until #1198 adds a
                                        // real sequence-flow ("take") listener slot.
                                        // Rejecting here also forecloses the #1197
                                        // mis-attachment class: a sequence flow does
                                        // NOT push onto the `io_stack`, so a dropped
                                        // listener would otherwise fall through to
                                        // `io_stack.last()` and hoist onto the
                                        // enclosing sub-process.
                                        return Err(ParseError::UnsupportedExecutionListener {
                                            process_id: acc.id.clone(),
                                            element_id: acc.flows[flow_idx]
                                                .id
                                                .clone()
                                                .unwrap_or_else(|| "<sequenceFlow>".to_string()),
                                            reason: "a sequence flow is an edge, not an \
                                                     element, so it has no lifecycle to run \
                                                     an execution listener; sequence-flow \
                                                     (\"take\") listeners are a deferred \
                                                     subset (#1198)"
                                                .to_string(),
                                        });
                                    } else if let Some(&idx) = io_stack.last() {
                                        let node = &mut acc.nodes[idx];
                                        Some((&mut node.start_listeners, &mut node.end_listeners))
                                    } else {
                                        // No open flow-node owner on the io_stack:
                                        // the listener is declared under
                                        // process-level extension elements (the
                                        // `<process>` itself never rides the
                                        // io_stack, only its flow nodes do). A
                                        // process has no activate/complete
                                        // lifecycle to run an execution listener,
                                        // so reject it here naming the process as
                                        // owner rather than let `io_stack.last()`
                                        // yield `None` and silently drop the
                                        // declaration — the same reject-don't-drop
                                        // contract this change enforces for
                                        // boundaries and sequence flows (#1197).
                                        return Err(ParseError::UnsupportedExecutionListener {
                                            process_id: acc.id.clone(),
                                            element_id: acc.id.clone(),
                                            reason: "an execution listener must attach to a flow \
                                                     node; it was declared at process level, \
                                                     which has no activate/complete lifecycle to \
                                                     run it"
                                                .to_string(),
                                        });
                                    };
                                    if let Some((start, end)) = sink {
                                        match event_type {
                                            crate::model::ListenerEventType::Start => {
                                                start.push(listener)
                                            }
                                            crate::model::ListenerEventType::End => {
                                                end.push(listener)
                                            }
                                        }
                                    }
                                }
                            }
                            // zeebe:taskListeners and its nested
                            // zeebe:taskListener entries (ADR 0037 §6). Task
                            // listeners run only on user tasks; the entry is
                            // attached to the innermost open element here, and a
                            // declaration on a non-user-task element is rejected
                            // at build (`UnsupportedTaskListener`, #1197) rather
                            // than stored dead.
                            "taskListeners" => {
                                // Mirror the `executionListeners` self-closing
                                // guard: a self-closing `<zeebe:taskListeners />`
                                // has no nested listeners and emits no matching end
                                // tag, so only enter the container state for a real
                                // open element — otherwise the flag would stay stuck
                                // `true` and wrongly capture a later stray
                                // `zeebe:taskListener` onto a subsequent element.
                                if !self_closing {
                                    in_task_listeners = true;
                                }
                            }
                            "taskListener" if in_task_listeners => {
                                if let Some(job_type) = attr(attrs, "type") {
                                    // `eventType` defaults to "creating" in Zeebe
                                    // when omitted.
                                    let event_type = match attr(attrs, "eventType") {
                                        Some("assigning") => {
                                            crate::model::TaskListenerEventType::Assigning
                                        }
                                        Some("updating") => {
                                            crate::model::TaskListenerEventType::Updating
                                        }
                                        Some("completing") => {
                                            crate::model::TaskListenerEventType::Completing
                                        }
                                        Some("canceling") => {
                                            crate::model::TaskListenerEventType::Canceling
                                        }
                                        _ => crate::model::TaskListenerEventType::Creating,
                                    };
                                    let retries = attr(attrs, "retries").map(str::to_string);
                                    let listener = crate::model::TaskListener {
                                        event_type,
                                        job_type: job_type.to_string(),
                                        retries,
                                    };
                                    // A task listener only runs on a user task, and
                                    // a user task is never a boundary event or a
                                    // sequence flow — neither of which enters the
                                    // `io_stack`. Mirror the execution-listener
                                    // boundary/flow handling and reject them here
                                    // explicitly, naming the real owner: routing them
                                    // through `io_stack.last()` would otherwise
                                    // silently DROP a top-level one (empty stack) or
                                    // HOIST a nested one onto the enclosing
                                    // sub-process and mis-report that as the owner
                                    // (#1197). The remaining `io_stack` case attaches
                                    // to the innermost open element; a non-user-task
                                    // placement there is caught by the build-time
                                    // `UnsupportedTaskListener` scan.
                                    if let Some(boundary) = cur_boundary.as_ref() {
                                        return Err(ParseError::UnsupportedTaskListener {
                                            process_id: acc.id.clone(),
                                            element_id: boundary.id.clone(),
                                            reason: "a task listener only runs on a user task; a \
                                                     boundary event is never a user task, so it \
                                                     could never create a task-listener job"
                                                .to_string(),
                                        });
                                    }
                                    if let Some(flow_idx) = cur_flow {
                                        return Err(ParseError::UnsupportedTaskListener {
                                            process_id: acc.id.clone(),
                                            element_id: acc.flows[flow_idx]
                                                .id
                                                .clone()
                                                .unwrap_or_else(|| "<sequenceFlow>".to_string()),
                                            reason: "a task listener only runs on a user task; a \
                                                     sequence flow is an edge, not a user task, so \
                                                     it could never create a task-listener job"
                                                .to_string(),
                                        });
                                    }
                                    if let Some(&idx) = io_stack.last() {
                                        acc.nodes[idx].task_listeners.push(listener);
                                    } else {
                                        // No open flow-node owner on the io_stack:
                                        // the task listener is declared under
                                        // process-level extension elements (the
                                        // `<process>` itself never rides the
                                        // io_stack). A process is not a user task,
                                        // so reject it naming the process as owner
                                        // rather than silently drop the
                                        // declaration — the reject-don't-drop
                                        // contract (#1197), mirroring the
                                        // execution-listener process-level guard.
                                        return Err(ParseError::UnsupportedTaskListener {
                                            process_id: acc.id.clone(),
                                            element_id: acc.id.clone(),
                                            reason: "a task listener only runs on a user task; it \
                                                     was declared at process level, which is not \
                                                     a user task"
                                                .to_string(),
                                        });
                                    }
                                }
                            }
                            // Any other tag inside a <process> the parser does
                            // not model. Instead of silently dropping it (which
                            // makes a flow into it fail deploy with a misleading
                            // "unknown target element" error, or an unattached
                            // element deploy clean and mis-execute), record
                            // genuinely-unmodelled flow elements / event
                            // definitions as `(tag, element_id)`. The
                            // unsupported-elements validator (#853) consumes this
                            // list; the `is_ignorable_tag` ignore-list keeps out
                            // non-flow noise (DI, documentation, structural BPMN,
                            // and the event definitions Nano does model), while the
                            // `extension_depth` guard keeps out foreign
                            // extension-element children (`zeebe:*`, `nano:*`, …)
                            // that Nano reads elsewhere or ignores — a BPMN flow
                            // element / event definition never appears inside
                            // `<extensionElements>`.
                            other => {
                                if extension_depth == 0 && !is_ignorable_tag(other) {
                                    // Prefer the tag's own `id`, but many
                                    // unmodelled constructs (notably event
                                    // definitions) carry no `id`; attribute those
                                    // to the owning element so the
                                    // unsupported-elements error stays actionable
                                    // instead of recording an empty id. Mirrors the
                                    // `errorEventDefinition`/`escalationEventDefinition`
                                    // owner-attribution above: a boundary event is
                                    // buffered in `cur_boundary` and never pushed
                                    // onto `flow_node_stack`, so an anonymous event
                                    // definition nested under one (e.g. an
                                    // unsupported `<cancelEventDefinition>` on a
                                    // `<boundaryEvent>`) must attribute to
                                    // `cur_boundary` before falling back to the open
                                    // flow-node stack — otherwise it would record a
                                    // containing flow node or an empty id.
                                    let element_id = match attr(attrs, "id") {
                                        Some(id) => id.to_string(),
                                        None => cur_boundary
                                            .as_ref()
                                            .map(|b| b.id.clone())
                                            .or_else(|| {
                                                flow_node_stack
                                                    .iter()
                                                    .rev()
                                                    .find_map(|e| *e)
                                                    .map(|i| acc.nodes[i].id.clone())
                                            })
                                            .unwrap_or_default(),
                                    };
                                    acc.unmodelled.push((other.to_string(), element_id));
                                }
                            }
                        }
                    }
                    _ => {}
                }
                // Track the open-element stack for `<incoming>`/`<outgoing>`
                // attribution. A self-closing tag never contains children, so it
                // is not pushed (and the tokenizer emits no matching end tag).
                if !self_closing {
                    let opened = match (nodes_before, current.as_ref()) {
                        (Some(before), Some(acc)) if acc.nodes.len() > before => {
                            Some(acc.nodes.len() - 1)
                        }
                        _ => None,
                    };
                    flow_node_stack.push(opened);
                }
            }
            Token::Text(text) => {
                if let Some(buf) = condition_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = event_condition_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = duration_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = cycle_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = date_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = completion_condition_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = flow_ref_text.as_mut() {
                    buf.push_str(text);
                }
            }
            Token::End { name } => match local_name(name) {
                "process" => {
                    if let Some(acc) = current.take() {
                        processes.push(acc);
                    }
                    cur_service_task = None;
                    cur_plain_task = None;
                    cur_flow = None;
                    cur_boundary = None;
                    cur_intermediate = None;
                    cur_throw = None;
                    duration_text = None;
                    cur_start = None;
                    cycle_text = None;
                    date_text = None;
                    event_condition_text = None;
                    cur_user_task = None;
                    cur_call = None;
                    cur_gateway = None;
                    completion_condition_text = None;
                    flow_ref_text = None;
                    flow_ref_owner = None;
                    cur_multi_instance = None;
                    io_stack.clear();
                    in_io_mapping = false;
                    in_execution_listeners = false;
                    in_task_listeners = false;
                    extension_depth = 0;
                }
                "serviceTask" => {
                    // Only pop when the open handler actually pushed. The push is
                    // conditional on `add_node` returning `Some` (the task carries
                    // an `id`); an id-less task is never pushed, so an
                    // unconditional pop would detach the enclosing activity's
                    // mapping owner and misattribute later `zeebe:ioMapping` data
                    // — the same defect class the id-less event handlers guard
                    // against. Safe because a task cannot nest another activity, so
                    // `cur_service_task` here is still this task's own index.
                    if cur_service_task.is_some() {
                        io_stack.pop();
                    }
                    cur_service_task = None;
                }
                "sendTask" => {
                    if cur_service_task.is_some() {
                        io_stack.pop();
                    }
                    cur_service_task = None;
                }
                "businessRuleTask" | "scriptTask" => {
                    if cur_service_task.is_some() {
                        io_stack.pop();
                    }
                    cur_service_task = None;
                }
                "userTask" => {
                    if cur_user_task.is_some() {
                        io_stack.pop();
                    }
                    cur_user_task = None;
                }
                "task" | "manualTask" | "receiveTask" => {
                    if cur_plain_task.is_some() {
                        io_stack.pop();
                    }
                    cur_plain_task = None;
                }
                "callActivity" => {
                    if cur_call.is_some() {
                        io_stack.pop();
                    }
                    cur_call = None;
                }
                // A gateway (any flavour) balances the io_stack push done for a
                // non-self-closing open with an `id` (#1197). Guard on
                // `cur_gateway` so an id-less gateway — never pushed — does not
                // detach the enclosing activity's mapping owner.
                "exclusiveGateway" | "parallelGateway" | "inclusiveGateway"
                | "eventBasedGateway" => {
                    if cur_gateway.is_some() {
                        io_stack.pop();
                    }
                    cur_gateway = None;
                }
                "subProcess" => {
                    if let Some(acc) = current.as_mut() {
                        acc.scope_stack.pop();
                    }
                    io_stack.pop();
                }
                "adHocSubProcess" => {
                    if let Some(acc) = current.as_mut() {
                        acc.scope_stack.pop();
                    }
                    cur_service_task = None;
                    io_stack.pop();
                }
                "ioMapping" => in_io_mapping = false,
                "extensionElements" => {
                    extension_depth = extension_depth.saturating_sub(1);
                }
                "taskHeaders" => in_task_headers = false,
                "linkedResources" => in_linked_resources = false,
                "executionListeners" => in_execution_listeners = false,
                "taskListeners" => in_task_listeners = false,
                "startEvent" => {
                    // Balance the io_stack push done for a non-self-closing start
                    // event (id-less start events are never pushed, so guard on
                    // `cur_start`) — #1197.
                    if cur_start.is_some() {
                        io_stack.pop();
                    }
                    cur_start = None;
                }
                "message" => cur_message = None,
                "boundaryEvent" => {
                    // Keep error boundaries (errorEventDefinition), timer
                    // boundaries (timerEventDefinition), message boundaries
                    // (messageEventDefinition), escalation boundaries
                    // (escalationEventDefinition, #1173) and the rest listed
                    // below; ignore boundaries carrying no recognised definition.
                    if let (Some(acc), Some(boundary)) = (current.as_mut(), cur_boundary.take()) {
                        if boundary.error_ref.is_some()
                            || boundary.timer_duration_millis.is_some()
                            || boundary.timer_expr.is_some()
                            || boundary.message_ref.is_some()
                            || boundary.signal_ref.is_some()
                            || boundary.condition.is_some()
                            || boundary.compensation
                            || boundary.escalation
                        {
                            acc.boundaries.push(boundary);
                        }
                    }
                }
                "conditionExpression" => {
                    if let (Some(acc), Some(idx), Some(text)) =
                        (current.as_mut(), cur_flow, condition_text.take())
                    {
                        let trimmed = text.trim();
                        acc.flows[idx].condition = if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        };
                    }
                }
                "incoming" | "outgoing" => {
                    if let (Some(acc), Some(owner), Some(text)) = (
                        current.as_mut(),
                        flow_ref_owner.take(),
                        flow_ref_text.take(),
                    ) {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            acc.flow_refs.push(FlowRef {
                                node_id: owner,
                                direction: flow_ref_dir,
                                flow_id: trimmed.to_string(),
                            });
                        }
                    }
                }
                "condition" => {
                    if let Some(text) = event_condition_text.take() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                acc.nodes[idx].event_condition = Some(trimmed.to_string());
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.condition = Some(trimmed.to_string());
                            }
                        }
                    }
                }
                "intermediateCatchEvent" => {
                    // Only pop when we actually pushed. The push is conditional
                    // on `add_node` returning `Some` (i.e. the event carries an
                    // `id`); popping unconditionally would detach the parent
                    // element from `io_stack` for an id-less catch event and
                    // misattach subsequent `zeebe:ioMapping` entries.
                    if cur_intermediate.is_some() {
                        io_stack.pop();
                    }
                    cur_intermediate = None;
                }
                "intermediateThrowEvent" => {
                    // Balance the io_stack push done for a non-self-closing throw
                    // event (id-less throw events are never pushed, so guard on
                    // `cur_throw`).
                    if cur_throw.is_some() {
                        io_stack.pop();
                    }
                    cur_throw = None;
                }
                "endEvent" => {
                    // Balance the io_stack push done for a non-self-closing end
                    // event (id-less end events are never pushed, so guard on
                    // `cur_end` — mirrors the id-less catch-event handling).
                    if cur_end.is_some() {
                        io_stack.pop();
                    }
                    cur_end = None;
                }
                "multiInstanceLoopCharacteristics" => cur_multi_instance = None,
                "completionCondition" => {
                    if let Some(text) = completion_condition_text.take() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            if let Some(acc) = current.as_mut() {
                                if let Some(idx) = cur_multi_instance {
                                    if let Some(mi) = acc.nodes[idx].multi_instance.as_mut() {
                                        mi.completion_condition = Some(trimmed.to_string());
                                    }
                                } else if let Some(id) = acc.scope_stack.last().cloned() {
                                    // A direct child of an ad-hoc container: attach
                                    // to the open container node (ADR 0023 seam 4).
                                    if let Some(node) =
                                        acc.nodes.iter_mut().find(|n| n.id == id && n.is_adhoc)
                                    {
                                        node.adhoc_completion_condition = Some(trimmed.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
                "timeDuration" => {
                    if let Some(text) = duration_text.take() {
                        let trimmed = text.trim();
                        // A `=`-prefixed timeDuration is a FEEL expression
                        // evaluated at timer creation; a bare value is a static
                        // ISO-8601 literal parsed now.
                        if let Some(feel) =
                            feel_timer_expr(trimmed, crate::model::TimerDefKind::Duration)
                        {
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                acc.nodes[idx].timer_expr = Some(feel);
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.timer_expr = Some(feel);
                            } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                acc.nodes[idx].timer_expr = Some(feel);
                                acc.nodes[idx].timer_repeating = Some(false);
                            }
                        } else {
                            let millis = parse_iso8601_duration(trimmed);
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                // An intermediate catch event's duration.
                                acc.nodes[idx].duration_millis = millis;
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                // A timer boundary event's duration.
                                boundary.timer_duration_millis = millis;
                            } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                // A one-shot timer start event's duration.
                                acc.nodes[idx].duration_millis = millis;
                                acc.nodes[idx].timer_repeating = Some(false);
                            }
                        }
                    }
                }
                "timeCycle" => {
                    if let Some(text) = cycle_text.take() {
                        let trimmed = text.trim();
                        if let Some(feel) =
                            feel_timer_expr(trimmed, crate::model::TimerDefKind::Cycle)
                        {
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                acc.nodes[idx].timer_expr = Some(feel);
                                acc.nodes[idx].timer_repeating = Some(true);
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.timer_expr = Some(feel);
                                boundary.timer_repeating = true;
                            }
                        } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                            // A recurring timer start event's cycle.
                            acc.nodes[idx].duration_millis = parse_iso8601_cycle(trimmed);
                            acc.nodes[idx].timer_repeating = Some(true);
                        } else if let Some(boundary) = cur_boundary.as_mut() {
                            // A recurring (cycle) timer boundary event.
                            boundary.timer_duration_millis = parse_iso8601_cycle(trimmed);
                            boundary.timer_repeating = true;
                        }
                    }
                }
                "timeDate" => {
                    if let Some(text) = date_text.take() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            // A timeDate is always an absolute instant, resolved
                            // at timer creation (FEEL when `=`-prefixed, else a
                            // literal ISO-8601 date-time).
                            let def = crate::model::TimerDef {
                                kind: crate::model::TimerDefKind::Date,
                                expr: trimmed.to_string(),
                            };
                            if let (Some(acc), Some(idx)) = (current.as_mut(), cur_intermediate) {
                                acc.nodes[idx].timer_expr = Some(def);
                            } else if let Some(boundary) = cur_boundary.as_mut() {
                                boundary.timer_expr = Some(def);
                            } else if let (Some(acc), Some(idx)) = (current.as_mut(), cur_start) {
                                acc.nodes[idx].timer_expr = Some(def);
                                acc.nodes[idx].timer_repeating = Some(false);
                            }
                        }
                    }
                }
                "sequenceFlow" => cur_flow = None,
                _ => {}
            },
        }
    }

    if processes.is_empty() {
        return Err(ParseError::NoProcess);
    }

    processes
        .into_iter()
        .map(|acc| {
            // Snapshot the raw parse data for post-parse validation before the
            // builder consumes the accumulator.
            let capture = acc.capture(&errors, &messages, &signals, &escalations);
            let def = acc.build(&errors, &messages, &signals, &escalations)?;
            // Retain the verbatim source XML on each parsed definition so it can
            // be served back (getProcessDefinitionXML / console diagram). Every
            // process in one resource shares that resource's XML.
            let def = ProcessDefinition {
                xml: xml.to_string(),
                ..def
            };
            Ok((capture, def))
        })
        .collect()
}
