//! The parser accumulators: the intermediate [`ProcessAcc`]/[`NodeAcc`] model
//! the streaming walk ([`super::walk`]) fills in, and the `build` pass that
//! turns a scanned process into a [`crate::model::ProcessDefinition`]. Split
//! out of the former monolithic `bpmn.rs` (#1207). Items are `pub(super)` so
//! the sibling walk can drive them; nothing here is part of the public API.

use super::*;

/// A flow node collected while scanning, before it becomes an [`crate::Element`].
pub(super) struct NodeAcc {
    pub(super) id: String,
    pub(super) kind: NodeKind,
    /// The element's BPMN `name` attribute, if present. Purely descriptive;
    /// surfaced by read models (the element-instance search API's `elementName`).
    pub(super) name: Option<String>,
    /// For service tasks: the resolved job type (defaults to the id at build).
    pub(super) job_type: Option<String>,
    /// For call activities: the `calledElement` / `zeebe:calledElement processId`
    /// of the invoked process. Executed natively as a child process instance;
    /// inline expansion at assembly time is a legacy opt-in (used by the
    /// processos harness).
    pub(super) called_process_id: Option<String>,
    /// For call activities: the `zeebe:calledElement propagateAllParentVariables`
    /// flag. `None` when the attribute is absent (defaults to `true` at build,
    /// matching Zeebe).
    pub(super) propagate_all_parent_variables: Option<bool>,
    /// For call activities: the `zeebe:calledElement propagateAllChildVariables`
    /// flag. `None` when the attribute is absent (defaults to `true` at build,
    /// matching Zeebe).
    pub(super) propagate_all_child_variables: Option<bool>,
    /// For service tasks: the raw `zeebe:priorityDefinition` job-priority
    /// expression (literal or FEEL), resolved at job creation. Controls
    /// activation order; `None` means no declaration (default priority).
    pub(super) job_priority: Option<String>,
    /// For timer intermediate catch events: the parsed timer duration in
    /// milliseconds (from a nested `timerEventDefinition`/`timeDuration`).
    pub(super) duration_millis: Option<u64>,
    /// For message intermediate catch events: the `messageRef` of a nested
    /// `messageEventDefinition`, resolved to a name/correlation key at build.
    pub(super) message_ref: Option<String>,
    /// For signal intermediate catch events: the `signalRef` of a nested
    /// `signalEventDefinition`, resolved to a signal name at build.
    pub(super) signal_ref: Option<String>,
    /// For timer start events: whether the timer recurs (a `timeCycle`) or is
    /// one-shot (a `timeDuration`). `None` on a plain none start event.
    pub(super) timer_repeating: Option<bool>,
    /// Id of the embedded sub-process containing this node, or `None` at the
    /// process level. Set from the scope stack as the node is scanned.
    pub(super) parent: Option<String>,
    /// For user tasks: the raw assignment/scheduling/priority expressions parsed
    /// from the Zeebe extension elements.
    pub(super) user_task: crate::model::UserTaskProps,
    /// True for an `adHocSubProcess`: kept as a single Service job activity while
    /// its contained elements are pruned at build (see `build`).
    pub(super) is_adhoc: bool,
    /// True for a `subProcess triggeredByEvent="true"` (an event sub-process). An
    /// event sub-process is triggered by its start event, not activated by token
    /// flow, so — even as a direct child of an ad-hoc container — it is NOT an
    /// activatable embedded-subProcess tool (#872) and stays on the pruned-catalog
    /// path with its inner activities.
    pub(super) is_event_subprocess: bool,
    /// `zeebe:adHoc outputCollection` on an ad-hoc container, if declared.
    pub(super) adhoc_output_collection: Option<String>,
    /// `zeebe:adHoc outputElement` FEEL expression on an ad-hoc container.
    pub(super) adhoc_output_element: Option<String>,
    /// `zeebe:adHoc activeElementsCollection` FEEL expression (declarative
    /// `BpmnTask` variant) on an ad-hoc container.
    pub(super) adhoc_active_elements: Option<String>,
    /// An ad-hoc container's `<completionCondition>` FEEL text, if declared.
    pub(super) adhoc_completion_condition: Option<String>,
    /// An ad-hoc container's `cancelRemainingInstances` attribute (BPMN default
    /// `true`): whether a fulfilled completion condition cancels still-running
    /// tools or defers until they drain.
    pub(super) adhoc_cancel_remaining_instances: bool,
    /// For an exclusive gateway: the id of its `default="..."` sequence flow, if
    /// declared. That flow becomes the gateway's fallback (taken only when no
    /// other outgoing condition matches), regardless of document order.
    pub(super) default_flow: Option<String>,
    /// The element's `zeebe:ioMapping` (input mappings applied on activation,
    /// output mappings applied on completion), populated from nested
    /// `zeebe:input`/`zeebe:output` children.
    pub(super) io: crate::model::IoMapping,
    /// A FEEL timer expression (a `=`-prefixed `timeDuration`/`timeCycle` or any
    /// `timeDate`) evaluated at timer creation; `None` for a static ISO-8601
    /// literal (which populates `duration_millis` at deploy instead).
    pub(super) timer_expr: Option<crate::model::TimerDef>,
    /// The raw `zeebe:taskDefinition` `retries` expression (literal or FEEL),
    /// resolved to a number at job creation; `None` for the default of 3.
    pub(super) retries: Option<String>,
    /// For a script task with an inline `zeebe:script`: the FEEL expression
    /// evaluated on activation. Both this and `script_result_variable` set
    /// makes the node an inline [`ScriptTask`](crate::model::ElementKind::ScriptTask)
    /// instead of a job-based service task.
    pub(super) script_expression: Option<String>,
    /// For a script task with an inline `zeebe:script`: the `resultVariable`
    /// the expression's result is stored under.
    pub(super) script_result_variable: Option<String>,
    /// For a business rule task with a `zeebe:calledDecision`: the decision id
    /// (literal or FEEL expression) to evaluate natively. Its presence makes the
    /// node a [`BusinessRuleTask`](crate::model::ElementKind::BusinessRuleTask)
    /// instead of a job-based service task.
    pub(super) decision_id: Option<String>,
    /// For a business rule task with a `zeebe:calledDecision`: the
    /// `resultVariable` the decision output is stored under (`None` spreads a map
    /// output into the scope).
    pub(super) decision_result_variable: Option<String>,
    /// For a conditional intermediate catch event: the FEEL `condition` (from a
    /// nested `conditionalEventDefinition`/`condition`) that must become `true`
    /// for the event to fire. Makes the node a
    /// [`ConditionalIntermediateCatchEvent`](crate::model::ElementKind::ConditionalIntermediateCatchEvent).
    pub(super) event_condition: Option<String>,
    /// Multi-instance loop characteristics collected from a
    /// `multiInstanceLoopCharacteristics` child plus its
    /// `zeebe:loopCharacteristics` extension; `None` for a single-instance node.
    pub(super) multi_instance: Option<crate::model::MultiInstance>,
    /// Execution listeners (`zeebe:executionListener`) declared on this node,
    /// split by `eventType` into start (fire on activation) and end (fire on
    /// completion) lists, in declaration order (ADR 0037).
    pub(super) start_listeners: Vec<crate::model::ExecutionListener>,
    pub(super) end_listeners: Vec<crate::model::ExecutionListener>,
    /// Task listeners (`zeebe:taskListener`) declared on this user task, all
    /// event types in one list in declaration order (ADR 0037 §6).
    pub(super) task_listeners: Vec<crate::model::TaskListener>,
    /// Static `zeebe:taskHeaders` (`<zeebe:header key value/>`) declared on a
    /// job-based task, in a deterministic map. Surfaced verbatim on the
    /// activated job (Zeebe `ActivatedJob.customHeaders`). Empty when none.
    pub(super) task_headers: std::collections::BTreeMap<String, String>,
    /// `zeebe:linkedResource`s declared on a job-based task, in declaration
    /// order. Resolved to concrete resource keys at job activation and delivered
    /// in the `linkedResources` custom header. Empty when none.
    pub(super) linked_resources: Vec<crate::model::LinkedResource>,
    /// For a start event: the `zeebe:formDefinition formId` declared on it — the
    /// process's start form. `None` on other nodes and start events with no form.
    pub(super) start_form_id: Option<String>,
    /// True when this `intermediateThrowEvent`/`endEvent` carries a
    /// `compensateEventDefinition`, making it a
    /// [`CompensationThrowEvent`](crate::model::ElementKind::CompensationThrowEvent).
    pub(super) is_compensation_throw: bool,
    /// True when this `intermediateThrowEvent`/`endEvent` carries an
    /// `escalationEventDefinition`, making it an
    /// [`EscalationThrowEvent`](crate::model::ElementKind::EscalationThrowEvent).
    /// Its raised escalation code is resolved from `escalation_ref` at build.
    pub(super) is_escalation_throw: bool,
    /// True when a SECOND `escalationEventDefinition` was seen on this
    /// throw/end event. `escalation_ref`/`is_escalation_throw` are last-wins, so
    /// a second definition would silently drop the first; `build` rejects the
    /// ambiguous multi-definition throw instead (#1173), mirroring the boundary
    /// `escalation_dup` guard.
    pub(super) escalation_throw_dup: bool,
    /// The `escalationRef` on this escalation throw/end (or boundary-less) event,
    /// resolved to an `escalationCode` at build. `None` when absent.
    pub(super) escalation_ref: Option<String>,
    /// The `linkEventDefinition name` on an `intermediateThrowEvent`
    /// (link *throw*) or `intermediateCatchEvent` (link *catch*), making the node
    /// a [`LinkIntermediateThrowEvent`](crate::model::ElementKind::LinkIntermediateThrowEvent)
    /// / [`LinkIntermediateCatchEvent`](crate::model::ElementKind::LinkIntermediateCatchEvent)
    /// at build. `None` on nodes without a `linkEventDefinition`.
    pub(super) link_name: Option<String>,
    /// True when this `endEvent` carries a `terminateEventDefinition`, making it
    /// a [`TerminateEndEvent`](crate::model::ElementKind::TerminateEndEvent)
    /// rather than a plain none end event.
    pub(super) is_terminate: bool,
    /// True when this activity is marked `isForCompensation="true"` — i.e. it is
    /// a compensation *handler*, reachable only via a compensation boundary's
    /// `<association>`, never by ordinary token flow. Used to resolve (and
    /// disambiguate) which association endpoint is the real handler.
    pub(super) is_for_compensation: bool,
    /// The `agentType` from a `zeebe:agentDefinition` extension marker on this
    /// activity, if present. Classifies the ordinary job-worker element.
    /// Placement is validated
    /// at build (`aiAgentTask` only on a `serviceTask`, `aiAgentSubProcess` only
    /// on an `adHocSubProcess`), mirroring Camunda's `AgentDefinitionValidator`.
    pub(super) agent_type: Option<crate::agent::AgentType>,
}

#[derive(Clone, Copy)]
pub(super) enum NodeKind {
    Start,
    End,
    Service,
    User,
    Exclusive,
    Parallel,
    Inclusive,
    EventBased,
    IntermediateCatch,
    IntermediateThrow,
    /// An abstract `task`/`manualTask` — a pass-through (see [`ElementKind::Task`]).
    Task,
    SubProcess,
    /// A call activity (callee in `NodeAcc::called_process_id`); expanded inline.
    Call,
}

impl NodeKind {
    /// True for node kinds that represent a BPMN *activity* (a task-like node, a
    /// subprocess, or a call activity) — the only kinds a compensation handler
    /// (`isForCompensation="true"`) may legitimately be. Keeps boundary→handler
    /// resolution from binding a compensation `<association>` to a non-activity
    /// (e.g. a gateway or event) that stray-carries the attribute.
    pub(super) fn is_activity(&self) -> bool {
        matches!(
            self,
            NodeKind::Service
                | NodeKind::User
                | NodeKind::Task
                | NodeKind::SubProcess
                | NodeKind::Call
        )
    }
}

/// A sequence flow collected while scanning.
pub(super) struct FlowAcc {
    pub(super) id: Option<String>,
    pub(super) source: Option<String>,
    pub(super) target: Option<String>,
    pub(super) condition: Option<String>,
}

/// A boundary event collected while scanning. An `error_ref` (resolved to an
/// error code at build time) makes it an error boundary; a `timer_duration_millis`
/// makes it a timer boundary; a `message_ref` (resolved to a message
/// name/correlation key) makes it a message boundary. `interrupting` reflects the
/// BPMN `cancelActivity` attribute (default `true`); error boundaries are always
/// interrupting. A boundary with none of these is ignored.
#[derive(Clone)]
pub(super) struct PendingBoundary {
    pub(super) id: String,
    pub(super) attached_to: Option<String>,
    pub(super) error_ref: Option<String>,
    pub(super) timer_duration_millis: Option<u64>,
    pub(super) timer_repeating: bool,
    /// A FEEL timer expression on this boundary timer (see [`NodeAcc::timer_expr`]).
    pub(super) timer_expr: Option<crate::model::TimerDef>,
    pub(super) message_ref: Option<String>,
    pub(super) signal_ref: Option<String>,
    /// A FEEL `condition` (from a nested `conditionalEventDefinition`/`condition`)
    /// that must become `true` for this boundary to fire. Makes it a
    /// [`ConditionalBoundaryEvent`](crate::model::ElementKind::ConditionalBoundaryEvent).
    pub(super) condition: Option<String>,
    pub(super) interrupting: bool,
    /// True when this boundary event carries a `compensateEventDefinition`,
    /// making it a
    /// [`CompensationBoundaryEvent`](crate::model::ElementKind::CompensationBoundaryEvent).
    /// Its handler activity is resolved from the `<association>` wiring it to the
    /// `isForCompensation` handler at build.
    pub(super) compensation: bool,
    /// True when this boundary event carries an `escalationEventDefinition`,
    /// making it an
    /// [`EscalationBoundaryEvent`](crate::model::ElementKind::EscalationBoundaryEvent)
    /// (#1173). `interrupting` reflects `cancelActivity` (default `true`); the
    /// caught code is resolved from `escalation_ref` at build.
    pub(super) escalation: bool,
    /// The boundary's `escalationRef`, resolved to an `escalationCode` (empty =
    /// catch-all) at build. `None` when the escalation carrier declares no ref.
    pub(super) escalation_ref: Option<String>,
    /// True when a *second* `escalationEventDefinition` was seen on this same
    /// boundary. A duplicate silently overwrites `escalation_ref` (last-wins),
    /// so an exact code could be replaced by a catch-all (or vice-versa) with no
    /// diagnostic. Rejected at build as an unsupported multi-definition boundary
    /// (#1173).
    pub(super) escalation_dup: bool,
    /// Execution listeners (`zeebe:executionListener`) declared on this boundary
    /// event, split into `start` (fire on activation) and `end` (fire on
    /// completion) lists (ADR 0037). A boundary event is buffered here rather
    /// than as a live `io_stack` node, so its listeners are captured on the
    /// pending boundary and re-attached to the built element in `build` — never
    /// mis-attached to the enclosing activity on the `io_stack`.
    pub(super) start_listeners: Vec<crate::model::ExecutionListener>,
    pub(super) end_listeners: Vec<crate::model::ExecutionListener>,
}

/// A definitions-level `<message>` declaration: its `name` and the instance
/// variable named by a nested `zeebe:subscription correlationKey`.
#[derive(Clone)]
pub(super) struct MessageDecl {
    pub(super) name: String,
    pub(super) correlation_key: Option<String>,
}

/// A `<incoming>`/`<outgoing>` QName reference from a flow node to a
/// `sequenceFlow`, collected while scanning so it can be resolved against the
/// declared flow ids at build time (parity with Zeebe's reference resolution).
pub(super) struct FlowRef {
    pub(super) node_id: String,
    pub(super) direction: &'static str,
    pub(super) flow_id: String,
}

/// Accumulates the nodes and flows of one `<process>` as it is scanned.
pub(super) struct ProcessAcc {
    pub(super) id: String,
    /// The `<bpmn:process>` `name` attribute (modeller label), if present.
    pub(super) name: Option<String>,
    pub(super) nodes: Vec<NodeAcc>,
    pub(super) flows: Vec<FlowAcc>,
    pub(super) boundaries: Vec<PendingBoundary>,
    /// `<incoming>`/`<outgoing>` references declared on flow nodes, resolved
    /// against `flows` at build time.
    pub(super) flow_refs: Vec<FlowRef>,
    /// `escalationRef`s declared on `escalationEventDefinition`s, as
    /// `(node_id, escalation_ref)`. Consumed by the reference-integrity
    /// validator (#851); escalation is not modelled for execution.
    pub(super) escalation_refs: Vec<(String, String)>,
    /// `errorRef`s declared on `errorEventDefinition`s that are **not** on a
    /// boundary event (i.e. on end/throw error events), as `(node_id,
    /// error_ref)`. Boundary `errorRef`s are resolved in `build`; these are the
    /// extra sites the reference-integrity validator (#851) generalises over.
    pub(super) error_refs_extra: Vec<(String, String)>,
    /// `(link name, throwing element id, enclosing scope)` for each
    /// `linkEventDefinition` on an intermediate *throw* event (consumed by #851's
    /// throw↔catch pairing check; the element id is the `from_node` on a rejected
    /// unpaired throw, and the scope enforces same-scope pairing at deploy).
    pub(super) link_throws: Vec<(String, String, Option<String>)>,
    /// Link names declared on `linkEventDefinition`s of intermediate *catch*
    /// events, paired with their enclosing scope (consumed by #851's throw↔catch
    /// pairing check).
    pub(super) link_catches: Vec<(String, Option<String>)>,
    /// Flow-element tags / event definitions the streaming parser does not
    /// model, recorded as `(tag, element_id)` instead of being silently
    /// dropped. `element_id` is the tag's own `id`, or — when the tag is
    /// anonymous (event definitions commonly are) — the id of its owning open
    /// flow node, so the unsupported-elements error stays actionable. Consumed
    /// by the unsupported-elements validator (#853).
    pub(super) unmodelled: Vec<(String, String)>,
    /// `<association>` `(sourceRef, targetRef)` pairs, used to wire a
    /// compensation boundary event to its `isForCompensation` handler activity.
    pub(super) associations: Vec<(String, String)>,
    /// Stack of open embedded sub-process ids, used to scope nested nodes.
    pub(super) scope_stack: Vec<String>,
}

impl ProcessAcc {
    pub(super) fn new(id: String) -> Self {
        Self {
            id,
            name: None,
            nodes: Vec::new(),
            flows: Vec::new(),
            boundaries: Vec::new(),
            flow_refs: Vec::new(),
            escalation_refs: Vec::new(),
            error_refs_extra: Vec::new(),
            link_throws: Vec::new(),
            link_catches: Vec::new(),
            unmodelled: Vec::new(),
            associations: Vec::new(),
            scope_stack: Vec::new(),
        }
    }

    /// Adds a flow node; returns its index, or `None` if it had no `id`.
    pub(super) fn add_node(&mut self, attrs: &[(String, String)], kind: NodeKind) -> Option<usize> {
        let id = attr(attrs, "id")?;
        self.nodes.push(NodeAcc {
            id: id.to_string(),
            kind,
            name: attr(attrs, "name").map(str::to_string),
            job_type: None,
            called_process_id: None,
            propagate_all_parent_variables: None,
            propagate_all_child_variables: None,
            job_priority: None,
            duration_millis: None,
            message_ref: None,
            signal_ref: None,
            timer_repeating: None,
            parent: self.scope_stack.last().cloned(),
            user_task: crate::model::UserTaskProps::default(),
            is_adhoc: false,
            is_event_subprocess: false,
            adhoc_output_collection: None,
            adhoc_output_element: None,
            adhoc_active_elements: None,
            adhoc_completion_condition: None,
            adhoc_cancel_remaining_instances: true,
            default_flow: None,
            io: crate::model::IoMapping::default(),
            timer_expr: None,
            retries: None,
            script_expression: None,
            script_result_variable: None,
            decision_id: None,
            decision_result_variable: None,
            event_condition: None,
            multi_instance: None,
            start_listeners: Vec::new(),
            end_listeners: Vec::new(),
            task_listeners: Vec::new(),
            task_headers: std::collections::BTreeMap::new(),
            linked_resources: Vec::new(),
            start_form_id: None,
            is_compensation_throw: false,
            is_escalation_throw: false,
            escalation_throw_dup: false,
            escalation_ref: None,
            link_name: None,
            is_terminate: false,
            is_for_compensation: attr(attrs, "isForCompensation") == Some("true"),
            agent_type: None,
        });
        let idx = self.nodes.len() - 1;
        Some(idx)
    }

    /// Adds a sequence flow; returns its index.
    pub(super) fn add_flow(&mut self, attrs: &[(String, String)]) -> Option<usize> {
        self.flows.push(FlowAcc {
            id: attr(attrs, "id").map(str::to_string),
            source: attr(attrs, "sourceRef").map(str::to_string),
            target: attr(attrs, "targetRef").map(str::to_string),
            condition: None,
        });
        Some(self.flows.len() - 1)
    }

    /// Snapshots the raw parse data a post-parse validator may need into a
    /// [`ProcessCapture`](crate::validate::ProcessCapture), owning all of it so
    /// the accumulator can then be consumed by [`build`](Self::build). Called
    /// *before* `build` so reference sites are seen against the raw parsed flows
    /// (before ad-hoc pruning), mirroring what Zeebe validates at deploy.
    pub(super) fn capture(
        &self,
        errors: &HashMap<String, String>,
        messages: &HashMap<String, MessageDecl>,
        signals: &HashMap<String, String>,
        escalations: &HashMap<String, String>,
    ) -> crate::validate::ProcessCapture {
        use crate::validate::{FlowRefCapture, RefSite, TaskDefCapture, UnmodelledElement};

        let flow_ids = self
            .flows
            .iter()
            .filter_map(|f| f.id.clone())
            .collect::<std::collections::HashSet<String>>();

        let flow_refs = self
            .flow_refs
            .iter()
            .map(|r| FlowRefCapture {
                node_id: r.node_id.clone(),
                direction: r.direction,
                flow_id: r.flow_id.clone(),
            })
            .collect();

        // Gather every non-`<incoming>`/`<outgoing>` reference site the parser
        // recorded onto nodes, boundaries and the dedicated escalation/error
        // vecs, keyed by the reference kind #851 resolves against.
        let mut references: Vec<RefSite> = Vec::new();
        for node in &self.nodes {
            if let Some(id) = &node.message_ref {
                references.push(RefSite {
                    kind: "messageRef",
                    id: id.clone(),
                    from_node: node.id.clone(),
                });
            }
            if let Some(id) = &node.signal_ref {
                references.push(RefSite {
                    kind: "signalRef",
                    id: id.clone(),
                    from_node: node.id.clone(),
                });
            }
            if let Some(id) = &node.default_flow {
                references.push(RefSite {
                    kind: "default",
                    id: id.clone(),
                    from_node: node.id.clone(),
                });
            }
        }
        for boundary in &self.boundaries {
            if let Some(id) = &boundary.attached_to {
                references.push(RefSite {
                    kind: "attachedToRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
            if let Some(id) = &boundary.error_ref {
                references.push(RefSite {
                    kind: "errorRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
            if let Some(id) = &boundary.message_ref {
                references.push(RefSite {
                    kind: "messageRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
            if let Some(id) = &boundary.signal_ref {
                references.push(RefSite {
                    kind: "signalRef",
                    id: id.clone(),
                    from_node: boundary.id.clone(),
                });
            }
        }
        for (node_id, escalation_ref) in &self.escalation_refs {
            references.push(RefSite {
                kind: "escalationRef",
                id: escalation_ref.clone(),
                from_node: node_id.clone(),
            });
        }
        for (node_id, error_ref) in &self.error_refs_extra {
            references.push(RefSite {
                kind: "errorRef",
                id: error_ref.clone(),
                from_node: node_id.clone(),
            });
        }

        let task_definitions = self
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::Service))
            .map(|n| TaskDefCapture {
                task_id: n.id.clone(),
                job_type: n.job_type.clone(),
                retries: n.retries.clone(),
            })
            .collect();

        crate::validate::ProcessCapture {
            process_id: self.id.clone(),
            flow_ids,
            flow_refs,
            references,
            link_throws: self.link_throws.clone(),
            link_catches: self.link_catches.clone(),
            unmodelled: self
                .unmodelled
                .iter()
                .map(|(tag, element_id)| UnmodelledElement {
                    tag: tag.clone(),
                    element_id: element_id.clone(),
                })
                .collect(),
            declared_messages: messages.keys().cloned().collect(),
            declared_errors: errors.keys().cloned().collect(),
            declared_signals: signals.keys().cloned().collect(),
            signal_names: signals.clone(),
            declared_escalations: escalations.keys().cloned().collect(),
            task_definitions,
        }
    }

    /// Assembles the [`ProcessDefinition`] via [`ProcessBuilder`].
    ///
    /// `errors` maps definitions-level `<error>` ids to their codes, used to
    /// resolve each boundary event's `errorRef`. `messages` maps `<message>` ids
    /// to their name and correlation-key variable, used to resolve message
    /// intermediate catch and boundary events.
    pub(super) fn build(
        mut self,
        errors: &HashMap<String, String>,
        messages: &HashMap<String, MessageDecl>,
        signals: &HashMap<String, String>,
        escalations: &HashMap<String, String>,
    ) -> Result<ProcessDefinition, ParseError> {
        // Reject `zeebe:executionListener`s declared where they can never fire
        // (#1197), rather than silently storing a listener that creates no job.
        //
        // Scope: this is the **parsed-XML deploy path** contract. All real
        // deployments arrive as BPMN XML (`parse_bpmn` → `NodeAcc::build` →
        // `ProcessDefinition` → `Engine::deploy`), so this scan gates every
        // listener a user can actually deploy. The lower-level programmatic
        // `ProcessBuilder::with_listeners` is an internal construction API (used
        // by tests and callers that assemble a `ProcessDefinition` by hand) that
        // deliberately trusts its caller and does not re-run this semantic check;
        // it is not a user-facing deploy surface.
        //
        // * A **multi-incoming parallel gateway** is a *join*:
        //   `activate_join` synchronises tokens and, once every incoming flow
        //   has been taken, emits `ElementCompleting` → `ElementCompleted` directly —
        //   running neither the listener-aware activation body (`start`) nor the
        //   end-listener chain (`end`), so *neither* phase of a listener on it can
        //   fire.
        // * A **multi-incoming inclusive gateway** is a *join* too, but an
        //   accepted join routes via `route_inclusive_gateway`, which DOES defer
        //   the routing behind its end-listener chain (`begin_end_listener_chain`) —
        //   so an `end` listener on an inclusive join fires and is supported. Only
        //   its `start` listener never fires (the join short-circuits the
        //   activation body), so we reject `start` alone.
        // * Single-incoming parallel/inclusive gateways (splits) and exclusive
        //   merges run the normal activation body, so their listeners fire — only
        //   the join placements above are unsupported.
        // * A **compensation boundary** is a passive structural marker, armed
        //   implicitly when its host completes and never entered by token flow,
        //   so it has no lifecycle to hang a listener on.
        {
            let mut incoming: HashMap<&str, usize> = HashMap::new();
            for flow in &self.flows {
                if let Some(target) = flow.target.as_deref() {
                    *incoming.entry(target).or_default() += 1;
                }
            }
            for node in &self.nodes {
                let multi_incoming = incoming.get(node.id.as_str()).copied().unwrap_or(0) > 1;
                if !multi_incoming {
                    continue;
                }
                match node.kind {
                    NodeKind::Parallel
                        if !node.start_listeners.is_empty() || !node.end_listeners.is_empty() =>
                    {
                        return Err(ParseError::UnsupportedExecutionListener {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "a multi-incoming parallel gateway (a join) synchronises \
                                     tokens and completes without running the listener-aware \
                                     activation body or the end-listener chain, so neither a \
                                     `start` nor an `end` execution listener on it would fire \
                                     (a listener on a single-incoming split is supported)"
                                .to_string(),
                        });
                    }
                    NodeKind::Inclusive if !node.start_listeners.is_empty() => {
                        return Err(ParseError::UnsupportedExecutionListener {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "a multi-incoming inclusive gateway (a join) is \
                                     synchronised on activation and never runs the listener-aware \
                                     activation body, so a `start` execution listener on it \
                                     would never fire (its `end` listener IS supported — an \
                                     accepted join runs the end-listener chain before \
                                     routing; a listener on a single-incoming split is supported)"
                                .to_string(),
                        });
                    }
                    _ => {}
                }
            }
            for boundary in &self.boundaries {
                if boundary.compensation
                    && (!boundary.start_listeners.is_empty() || !boundary.end_listeners.is_empty())
                {
                    return Err(ParseError::UnsupportedExecutionListener {
                        process_id: self.id.clone(),
                        element_id: boundary.id.clone(),
                        reason: "a compensation boundary event is a passive structural \
                                 marker that is never activated by token flow, so its \
                                 execution listeners would never fire"
                            .to_string(),
                    });
                }
            }
            // A **terminate end event** completes off the normal end-event path:
            // `complete_terminate_end` drives a scope-wide teardown and emits
            // `ElementCompleting`/`ElementCompleted` directly, never running the
            // end-listener chain — so an `end` listener on it could never create a
            // job. Reject `end` alone (reject-don't-drop, #1197). Its `start`
            // listener IS supported: it fires through the generic activation
            // start-listener gate before the terminate behaviour runs, exactly like
            // the inclusive-join case where only the dead phase is rejected.
            for node in &self.nodes {
                if node.is_terminate && !node.end_listeners.is_empty() {
                    return Err(ParseError::UnsupportedExecutionListener {
                        process_id: self.id.clone(),
                        element_id: node.id.clone(),
                        reason: "an `end` execution listener on a terminate end event would \
                                 never fire: completing a terminate end drives a scope-wide \
                                 teardown that emits completion directly, bypassing the \
                                 end-listener chain (its `start` listener IS supported — it \
                                 fires through the activation start-listener gate)"
                            .to_string(),
                    });
                }
            }
            // A **`zeebe:taskListener`** (ADR 0037 §6) only runs on a user task —
            // task-listener jobs are created solely on the user-task runtime path.
            // The lenient parser attaches a `zeebe:taskListeners` declaration to
            // whatever element is innermost on the `io_stack`, so now that a
            // `receiveTask` (a pass-through) rides the io_stack for its *execution*
            // listeners, a task listener declared on it (or any non-user-task) is
            // stored yet could never create a job. Reject-don't-drop it (#1197).
            for node in &self.nodes {
                if !node.task_listeners.is_empty() && !matches!(node.kind, NodeKind::User) {
                    return Err(ParseError::UnsupportedTaskListener {
                        process_id: self.id.clone(),
                        element_id: node.id.clone(),
                        reason: "a `zeebe:taskListener` only runs on a user task — task-listener \
                                 jobs are created only on the user-task runtime path, so a task \
                                 listener declared on any other element could never fire"
                            .to_string(),
                    });
                }
            }
        }

        // Ad-hoc sub-processes are kept as a single Service job activity; the
        // elements they contain (agent "tools", invoked out-of-band rather than by
        // token flow) are pruned from the executable graph, along with any
        // sequence flows or boundary events that reference them. A node is pruned
        // when its parent chain reaches an ad-hoc node, so nesting at any depth is
        // handled. Before pruning, the tools + `zeebe:adHoc` wiring are captured
        // into `adhoc_catalog` as non-executable metadata so the Camunda agentic
        // activate-element contract can be honoured later (ADR 0023) without
        // re-parsing.
        let mut adhoc_catalog: Vec<crate::model::AdHocSubProcessDef> = Vec::new();
        let adhoc_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| n.is_adhoc)
            .map(|n| n.id.clone())
            .collect();
        // Reject an execution listener declared on a *tool* of an ad-hoc
        // sub-process — a node whose direct parent is an ad-hoc container (#1197).
        // A leaf tool is pruned below (flattened into the non-executable
        // `AdHocTool` catalog and never activated through the lifecycle) and a
        // retained embedded-sub-process tool is activated/completed with direct
        // lifecycle events, both bypassing `advance_listener`, so a `start`/`end`
        // listener on the tool could never create a job. Refuse the model at
        // deploy rather than accept a dead listener that silently disappears —
        // the same reject-don't-drop contract applied to sequence flows and
        // joins. Checked pre-pruning (leaf tools are removed just below) and only
        // for the tool ITSELF: listeners on deeper elements inside a retained
        // sub-process tool's body run by ordinary token flow and DO fire, so they
        // are left alone. Document order keeps the error deterministic.
        if let Some(bad) = self.nodes.iter().find(|n| {
            (!n.start_listeners.is_empty() || !n.end_listeners.is_empty())
                && n.parent.as_deref().is_some_and(|p| adhoc_ids.contains(p))
        }) {
            return Err(ParseError::UnsupportedExecutionListener {
                process_id: self.id.clone(),
                element_id: bad.id.clone(),
                reason: "an execution listener on a tool of an ad-hoc sub-process is not \
                         supported: a leaf tool is pruned and a retained embedded tool is \
                         activated/completed with direct lifecycle events, both bypassing the \
                         listener gate, so the listener could never fire"
                    .to_string(),
            });
        }
        // Reject a **task listener** declared on a *tool* of an ad-hoc sub-process
        // (#1197). A user-task tool is `NodeKind::User`, so it slips past the
        // node-based `UnsupportedTaskListener` scan above — but the tool is
        // flattened into the non-executable ad-hoc catalog and activated through a
        // direct path that emits `UserTaskCreated` without running task listeners
        // (`engine/mod.rs`), so the listener could never create a job. This is the
        // task-listener analogue of the execution-listener ad-hoc-tool rejection
        // above: refuse the model at deploy rather than accept a silently-pruned
        // dead listener. Checked pre-pruning and only for the tool ITSELF; task
        // listeners deeper inside a retained sub-process tool's body would already
        // have been rejected/attached by the node scan. Document order keeps the
        // error deterministic.
        if let Some(bad) = self.nodes.iter().find(|n| {
            !n.task_listeners.is_empty()
                && n.parent.as_deref().is_some_and(|p| adhoc_ids.contains(p))
        }) {
            return Err(ParseError::UnsupportedTaskListener {
                process_id: self.id.clone(),
                element_id: bad.id.clone(),
                reason: "a task listener on a user-task tool of an ad-hoc sub-process is not \
                         supported: the tool is flattened into the non-executable ad-hoc \
                         catalog and activated through a direct path that emits UserTaskCreated \
                         without running task listeners, so the listener could never fire"
                    .to_string(),
            });
        }
        if !adhoc_ids.is_empty() {
            let parent_of: HashMap<&str, &str> = self
                .nodes
                .iter()
                .filter_map(|n| n.parent.as_deref().map(|p| (n.id.as_str(), p)))
                .collect();
            // Reject an execution listener on a *boundary event attached to a tool*
            // of an ad-hoc sub-process (#1197). This is the boundary analogue of the
            // tool-node listener rejection above: the check there only looks at
            // listeners stored on the tool node itself, but a listener-bearing
            // boundary is just as dead. For a leaf tool the boundary is dropped by
            // `self.boundaries.retain` below (its `attached_to` is pruned) *before*
            // `boundary_listeners` is collected, so the listener silently vanishes;
            // for a retained embedded-sub-process tool the boundary reaches
            // `interrupt_activity_via_boundary` with no tool-release path and never
            // runs its listener gate. Both are the reject-don't-drop class, so refuse
            // the model at deploy rather than accept a dead listener. A "tool" is a
            // direct child of an ad-hoc container (both leaf and embedded-sub-process
            // tools are); a boundary attached to a deeper element inside a retained
            // sub-process tool's body runs by ordinary token flow and IS fine, so it
            // is left alone. Checked pre-pruning, in document order for a
            // deterministic error.
            if let Some(bad) = self.boundaries.iter().find(|b| {
                (!b.start_listeners.is_empty() || !b.end_listeners.is_empty())
                    && b.attached_to
                        .as_deref()
                        .and_then(|a| parent_of.get(a).copied())
                        .is_some_and(|p| adhoc_ids.contains(p))
            }) {
                return Err(ParseError::UnsupportedExecutionListener {
                    process_id: self.id.clone(),
                    element_id: bad.id.clone(),
                    reason: "an execution listener on a boundary event attached to a tool of \
                             an ad-hoc sub-process is not supported: the tool is invoked \
                             out-of-band, so a leaf tool's boundary is pruned before its \
                             listener is collected and a retained embedded tool's boundary \
                             has no activation path, both bypassing the listener gate, so the \
                             listener could never fire"
                        .to_string(),
                });
            }
            let inside_adhoc = |id: &str| -> bool {
                let mut cur = parent_of.get(id).copied();
                while let Some(p) = cur {
                    if adhoc_ids.contains(p) {
                        return true;
                    }
                    cur = parent_of.get(p).copied();
                }
                false
            };
            // The nearest enclosing ad-hoc container of `id`, if any.
            let nearest_adhoc = |id: &str| -> Option<String> {
                let mut cur = parent_of.get(id).copied();
                while let Some(p) = cur {
                    if adhoc_ids.contains(p) {
                        return Some(p.to_string());
                    }
                    cur = parent_of.get(p).copied();
                }
                None
            };
            // Embedded `subProcess` tools (issue #872): a plain `bpmn:subProcess`
            // that is a DIRECT child of an ad-hoc container is a multi-element
            // token-flow tool — its body must run by ordinary token flow, so
            // (unlike a leaf service/user-task tool) neither the subProcess
            // element nor its body is pruned; both are retained in the executable
            // graph and reached only when the tool is activated. A nested
            // `adHocSubProcess` (agent-of-agents, #631) is NOT such a tool — it is
            // `is_adhoc` and keeps its own pruned-catalog handling — so exclude it
            // here.
            let subprocess_tool_ids: std::collections::HashSet<String> = self
                .nodes
                .iter()
                .filter(|n| {
                    matches!(n.kind, NodeKind::SubProcess)
                        && !n.is_adhoc
                        && !n.is_event_subprocess
                        && n.parent
                            .as_deref()
                            .map(|p| adhoc_ids.contains(p))
                            .unwrap_or(false)
                })
                .map(|n| n.id.clone())
                .collect();
            // Is `id` the subProcess tool itself or one of its body descendants?
            // Walk ancestor-or-self: a subProcess-tool ancestor means retained; an
            // ad-hoc ancestor reached first (e.g. a nested ad-hoc container's own
            // tool) means it is pruned by the ad-hoc rule, not retained.
            let in_subprocess_tool = |id: &str| -> bool {
                let mut cur = Some(id);
                while let Some(c) = cur {
                    if subprocess_tool_ids.contains(c) {
                        return true;
                    }
                    if adhoc_ids.contains(c) {
                        return false;
                    }
                    cur = parent_of.get(c).copied();
                }
                false
            };
            let pruned: std::collections::HashSet<String> = self
                .nodes
                .iter()
                .filter(|n| inside_adhoc(&n.id) && !in_subprocess_tool(&n.id))
                .map(|n| n.id.clone())
                .collect();

            // A pruned inner element is flattened into an `AdHocTool` whose
            // `AdHocToolKind` is `Other` for any node kind the catalog doesn't
            // model as an executable tool (only Service/User/Call/SubProcess are).
            // Activating an `Other` tool completes it directly, which is harmless
            // for a plain none-throw but SILENTLY DROPS the runtime side-effect of
            // an escalation/compensation *throw*: the escalation is never raised
            // (its boundary handler never fires) and the compensation is never
            // triggered. Executing these as ad-hoc tools is a larger runtime
            // feature nano does not yet support, so — rather than let the model
            // misbehave at runtime — reject the placement at deploy naming the
            // construct (mirrors the start/end-event rejections below and the
            // "reject, don't silently drop" contract). Iterate `self.nodes` in
            // document order so the error is deterministic.
            if let Some(bad) = self.nodes.iter().find(|n| {
                pruned.contains(&n.id) && (n.is_escalation_throw || n.is_compensation_throw)
            }) {
                let construct = if bad.is_escalation_throw {
                    "escalation throw event"
                } else {
                    "compensation throw event"
                };
                return Err(ParseError::InvalidProcess {
                    process_id: self.id.clone(),
                    reason: format!(
                        "ad-hoc sub-process must not contain a {construct} ({}); \
                         nano cannot execute a throw event as an ad-hoc tool, so its \
                         escalation/compensation would be silently dropped",
                        bad.id
                    ),
                });
            }

            // Deploy-time ad-hoc validation (Zeebe `AdHocSubProcessValidator`):
            // reject structurally invalid containers before they can run, so a bad
            // model fails at parse/deploy rather than misbehaving at runtime. The
            // inner elements are pruned just below, so this must inspect
            // `self.nodes` (pre-pruning). Rules mirror the oracle where nano shares
            // its semantics:
            //   * at least one inner activity;
            //   * no start/end events inside the container;
            //   * a `zeebe:taskDefinition` (job-worker variant) forbids
            //     `activeElementsCollection` (the declarative BPMN_TASK selector,
            //     which nano treats as mutually exclusive with a job — see the
            //     `impl_type` split below);
            //   * `outputElement` and `outputCollection` are both-or-neither.
            //
            // Deliberate divergence from Zeebe: nano does NOT forbid a
            // `<completionCondition>` or `cancelRemainingInstances=false` alongside
            // a `taskDefinition`. Zeebe rejects those because its JOB_WORKER path
            // carries completion/cancel purely on the job result. nano instead
            // supports an engine-side `<completionCondition>` on the JOB_WORKER
            // container (an intentional parity extension exercised by the engine
            // tests), and `cancelRemainingInstances` deferral is tracked separately
            // (issue #614 gap 7), so enforcing those two Zeebe rules here would
            // reject valid nano models. See issue #614 gap 6.
            for n in self.nodes.iter().filter(|n| n.is_adhoc) {
                let invalid = |reason: String| ParseError::InvalidProcess {
                    process_id: self.id.clone(),
                    reason,
                };
                // Zeebe validates the container's DIRECT flow elements
                // (`getFlowElements()`), not elements nested inside an inner
                // sub-process, so match on the immediate parent only.
                let inner: Vec<&NodeAcc> = self
                    .nodes
                    .iter()
                    .filter(|c| c.parent.as_deref() == Some(n.id.as_str()))
                    .collect();
                // Zeebe requires at least one *activity* (task / sub-process /
                // call activity), not merely any flow node: a container holding
                // only gateways or events is still structurally invalid. Checking
                // `inner.is_empty()` alone would wrongly accept such a model, so
                // match on the activity node kinds explicitly.
                let has_activity = inner.iter().any(|c| {
                    matches!(
                        c.kind,
                        NodeKind::Service | NodeKind::User | NodeKind::SubProcess | NodeKind::Call
                    )
                });
                if !has_activity {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must have at least one activity",
                        n.id
                    )));
                }
                if inner.iter().any(|c| matches!(c.kind, NodeKind::Start)) {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must not contain a start event",
                        n.id
                    )));
                }
                if inner.iter().any(|c| matches!(c.kind, NodeKind::End)) {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must not contain an end event",
                        n.id
                    )));
                }
                // `taskDefinition` presence is authoritative here as `job_type`
                // (set only by `zeebe:taskDefinition`; not yet defaulted to the id).
                // A job-backed container ignores `activeElementsCollection` (the
                // declarative selector), so declaring both is a contradictory model.
                if n.job_type.is_some()
                    && n.adhoc_active_elements
                        .as_deref()
                        .is_some_and(|s| !s.is_empty())
                {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must not define activeElementsCollection in combination with zeebe:taskDefinition",
                        n.id
                    )));
                }
                let output_element_empty =
                    n.adhoc_output_element.as_deref().unwrap_or("").is_empty();
                let output_collection_empty = n
                    .adhoc_output_collection
                    .as_deref()
                    .unwrap_or("")
                    .is_empty();
                if output_element_empty != output_collection_empty {
                    return Err(invalid(format!(
                        "ad-hoc sub-process {} must set outputElement and outputCollection both or neither",
                        n.id
                    )));
                }
            }

            // Embedded `subProcess` tools (#872) are activated by injecting a
            // token at their inner start event (`activate_adhoc_tool` does a
            // `Step::Activate` on it), so each MUST have exactly one direct-child
            // start event. Zero would leave the tool with no entry token — and
            // the catalog build below would silently default the start id to ""
            // (`unwrap_or_default`), so activation would `Step::Activate` a
            // non-existent element and hard-fail at runtime. More than one makes
            // the injection target ambiguous. Reject both at parse/deploy time
            // with a clear error rather than defaulting. Iterate in document
            // order (over `self.nodes`, not the `HashSet`) so the error is
            // deterministic when several tools are malformed.
            for n in self
                .nodes
                .iter()
                .filter(|n| subprocess_tool_ids.contains(&n.id))
            {
                let start_count = self
                    .nodes
                    .iter()
                    .filter(|c| {
                        matches!(c.kind, NodeKind::Start)
                            && c.parent.as_deref() == Some(n.id.as_str())
                    })
                    .count();
                if start_count != 1 {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "embedded subProcess tool {} must have exactly one start event, found {}",
                            n.id, start_count
                        ),
                    });
                }
            }

            // One catalog entry per ad-hoc container, in document order.
            let mut index: HashMap<String, usize> = HashMap::new();
            for n in self.nodes.iter().filter(|n| n.is_adhoc) {
                index.insert(n.id.clone(), adhoc_catalog.len());
                // A container backed by a job (taskDefinition) is the agentic
                // JOB_WORKER variant; one that only declares an
                // activeElementsCollection is the declarative BPMN_TASK variant.
                let impl_type = if n.job_type.is_none() && n.adhoc_active_elements.is_some() {
                    crate::model::AdHocImplementationType::BpmnTask
                } else {
                    crate::model::AdHocImplementationType::JobWorker
                };
                adhoc_catalog.push(crate::model::AdHocSubProcessDef {
                    container_id: n.id.clone(),
                    impl_type,
                    completion_condition: n.adhoc_completion_condition.clone(),
                    active_elements_collection: n.adhoc_active_elements.clone(),
                    output_collection: n.adhoc_output_collection.clone(),
                    output_element: n.adhoc_output_element.clone(),
                    cancel_remaining_instances: n.adhoc_cancel_remaining_instances,
                    tools: Vec::new(),
                    inner_flows: Vec::new(),
                });
            }
            // Assign each pruned tool — and each retained embedded-subProcess
            // tool (#872) — to its nearest ad-hoc container, preserving document
            // order. A subProcess tool stays in the executable graph, so it is
            // NOT in `pruned`; it is added to the catalog here so the agent can
            // activate it by id and `activate_adhoc_tool` can inject a token at
            // its body's start event.
            for n in self
                .nodes
                .iter()
                .filter(|n| pruned.contains(&n.id) || subprocess_tool_ids.contains(&n.id))
            {
                let kind = if subprocess_tool_ids.contains(&n.id) {
                    // The inner start event (a Start node whose parent is this
                    // subProcess tool) — where a token is injected on activation.
                    // The validation loop above guarantees exactly one such node,
                    // so `find` always succeeds; the `unwrap_or_default` is only a
                    // belt-and-braces fallback that the parse-time check prevents.
                    let start_event = self
                        .nodes
                        .iter()
                        .find(|c| {
                            matches!(c.kind, NodeKind::Start)
                                && c.parent.as_deref() == Some(n.id.as_str())
                        })
                        .map(|c| c.id.clone())
                        .unwrap_or_default();
                    crate::model::AdHocToolKind::SubProcess { start_event }
                } else {
                    match n.kind {
                        NodeKind::Service => crate::model::AdHocToolKind::ServiceTask {
                            job_type: n.job_type.clone().unwrap_or_else(|| n.id.clone()),
                        },
                        NodeKind::User => {
                            crate::model::AdHocToolKind::UserTask(n.user_task.clone())
                        }
                        NodeKind::Call => crate::model::AdHocToolKind::CallActivity {
                            process_id: n.called_process_id.clone(),
                            propagate_all_parent_variables: n
                                .propagate_all_parent_variables
                                .unwrap_or(true),
                            propagate_all_child_variables: n
                                .propagate_all_child_variables
                                .unwrap_or(true),
                        },
                        _ => crate::model::AdHocToolKind::Other,
                    }
                };
                if let Some(pos) = nearest_adhoc(&n.id).and_then(|c| index.get(&c).copied()) {
                    adhoc_catalog[pos].tools.push(crate::model::AdHocTool {
                        element_id: n.id.clone(),
                        name: n.name.clone().unwrap_or_default(),
                        kind,
                        io: n.io.clone(),
                    });
                }
            }

            // Capture the `bpmn:sequenceFlow`s between the container's DIRECT
            // children (issue #1154) BEFORE the pruning below drops them. Camunda
            // lets an ad-hoc container's inner elements be "connected by a
            // sequence flow to build a structured sequence": on the source's
            // completion the flow is taken and the target runs. These flows
            // reference pruned (or retained-subprocess-tool) elements, so — like
            // the tool catalog — they are captured here and driven by the ad-hoc
            // runtime seam rather than by token flow. Only flows whose source AND
            // target are both DIRECT children of the SAME ad-hoc container are
            // captured; a flow inside an embedded-`subProcess` tool's body (parent
            // is the subProcess, not the container) is left to run by ordinary
            // token flow.
            for f in &self.flows {
                let (Some(src), Some(tgt)) = (f.source.as_deref(), f.target.as_deref()) else {
                    continue;
                };
                let src_parent = parent_of.get(src).copied();
                let tgt_parent = parent_of.get(tgt).copied();
                if let (Some(sp), Some(tp)) = (src_parent, tgt_parent) {
                    if sp == tp && adhoc_ids.contains(sp) {
                        if let Some(pos) = index.get(sp).copied() {
                            adhoc_catalog[pos]
                                .inner_flows
                                .push(crate::model::AdHocInnerFlow {
                                    from: src.to_string(),
                                    to: tgt.to_string(),
                                    condition: f
                                        .condition
                                        .clone()
                                        .map(crate::model::Condition::new),
                                });
                        }
                    }
                }
            }

            // ADR 0023 seam 2 (runtime): the inner LEAF "tool" activities
            // (service / user tasks, call activities, gateways, events) are pruned
            // from the executable graph — an ad-hoc container is flattened to a
            // single job-bearing activity, so those tools are never reached by
            // ordinary token flow. They are NOT lost: the catalog above captures
            // each tool's id + kind (job type), which is all the runtime needs to
            // activate one when the agent's job result requests it (the container
            // scope + activate-element seeding drive execution, not the flat
            // element graph). An embedded-`subProcess` tool and its body are the
            // exception (#872): they are NOT in `pruned`, so they stay in
            // `self.nodes`/`flows` and run by token flow when the tool is
            // activated. Pruning the leaf tools keeps `ProcessDefinition.elements`
            // otherwise minimal, avoiding a modeler cascade over arbitrary inner
            // leaf activities.
            if !pruned.is_empty() {
                self.nodes.retain(|n| !pruned.contains(&n.id));
                self.flows.retain(|f| {
                    let keep = |o: &Option<String>| {
                        o.as_ref().map(|s| !pruned.contains(s)).unwrap_or(true)
                    };
                    keep(&f.source) && keep(&f.target)
                });
                self.boundaries.retain(|b| {
                    b.attached_to
                        .as_ref()
                        .map(|a| !pruned.contains(a))
                        .unwrap_or(true)
                });
            }
        }

        // A process may declare more than one start event (e.g. a "refresh batch"
        // and a "manual intake" start that merge downstream). Zeebe permits a none
        // start alongside any number of *typed* (message/timer/signal) starts, and
        // wires EVERY typed start independently: a message start opens a
        // subscription and a timer start arms a process-level timer at deploy, each
        // firing its own instance at its own start element (issue #855). So message
        // and timer process-level starts are ALL kept here as their typed element
        // kinds — `Engine::deploy` scans them and registers a trigger for each; the
        // single process-entry start (used only for CreateInstance) is designated
        // downstream in `ProcessBuilder::build`.
        //
        // Only surplus *signal* starts are demoted to inert throw events (they keep
        // their outgoing flow, so it still has a valid source, but lacking any
        // incoming flow are never activated). Nano has no dedicated signal-start
        // element kind — a surviving signal start builds to a plain
        // `ElementKind::StartEvent`, indistinguishable from a none start, so a
        // second one would be miscounted as a second *none* start and wrongly
        // rejected by the `start_events` validator (#855); signal-start runtime is
        // out of scope. Surplus *none* starts are deliberately NOT demoted —
        // multiple none starts are illegal, and keeping them lets that validator
        // see and reject them (`InvalidStartEvents`) rather than silently
        // collapsing them.
        let proc_starts: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| matches!(n.kind, NodeKind::Start) && n.parent.is_none())
            .map(|(i, _)| i)
            .collect();
        if proc_starts.len() > 1 {
            // A none start carries no message/timer/signal event definition.
            let is_none_start = |n: &NodeAcc| {
                n.message_ref.is_none() && n.timer_repeating.is_none() && n.signal_ref.is_none()
            };
            // A signal start carries a signalRef but no message/timer definition.
            let is_signal_start = |n: &NodeAcc| {
                n.signal_ref.is_some() && n.message_ref.is_none() && n.timer_repeating.is_none()
            };
            // Keep a single none/signal start as the process-entry `StartEvent`
            // (prefer a none start); any additional *signal* start is surplus.
            // `proc_starts.len() > 1` above guarantees the fallback min_by sees a
            // non-empty iterator.
            #[allow(
                clippy::expect_used,
                reason = "guarded by `proc_starts.len() > 1` above, so the iterator is non-empty"
            )]
            let designated = proc_starts
                .iter()
                .copied()
                .filter(|&i| is_none_start(&self.nodes[i]))
                .min_by(|&a, &b| self.nodes[a].id.cmp(&self.nodes[b].id))
                .unwrap_or_else(|| {
                    proc_starts
                        .iter()
                        .copied()
                        .min_by(|&a, &b| self.nodes[a].id.cmp(&self.nodes[b].id))
                        .expect("non-empty")
                });
            for &i in &proc_starts {
                // Demote only surplus *signal* starts; keep none/message/timer (see
                // the note above — none starts are kept for the validator, and
                // message/timer starts are kept so deploy can wire their triggers).
                if i != designated && is_signal_start(&self.nodes[i]) {
                    // A demoted signal start becomes an inert `IntermediateThrow`
                    // with no incoming flow — it is never activated or completed,
                    // so any execution listener captured on it (#1197) could never
                    // fire. Refuse the model at deploy rather than silently drop the
                    // dead listener into the demotion, the same reject-don't-drop
                    // contract applied to joins, compensation boundaries, ad-hoc
                    // tools and sequence flows above.
                    if !self.nodes[i].start_listeners.is_empty()
                        || !self.nodes[i].end_listeners.is_empty()
                    {
                        return Err(ParseError::UnsupportedExecutionListener {
                            process_id: self.id.clone(),
                            element_id: self.nodes[i].id.clone(),
                            reason: "an execution listener on a surplus signal start event is \
                                     not supported: Nano has no dedicated signal-start kind, so \
                                     a second signal start is demoted to an inert throw event \
                                     with no incoming flow that is never activated or completed, \
                                     so the listener could never fire"
                                .to_string(),
                        });
                    }
                    self.nodes[i].kind = NodeKind::IntermediateThrow;
                    self.nodes[i].message_ref = None;
                    self.nodes[i].timer_repeating = None;
                    self.nodes[i].duration_millis = None;
                    self.nodes[i].signal_ref = None;
                }
            }
        }

        let mut builder = ProcessBuilder::new(self.id.clone());
        if let Some(name) = &self.name {
            builder = builder.name(name.clone());
        }
        // Map each sub-process to its inner start event (a start node whose
        // parent is the sub-process).
        let sub_starts: HashMap<String, String> = self
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::Start))
            .filter_map(|n| n.parent.clone().map(|p| (p, n.id.clone())))
            .collect();
        // Ids of sequence flows declared as a gateway `default` flow.
        let mut default_flow_ids: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        // The process-level start event's form id (`zeebe:formDefinition formId`),
        // captured as nodes are consumed and set on the built definition below.
        let mut start_form_id: Option<String> = None;
        // Ids of activities marked `isForCompensation="true"` — the only valid
        // targets of a compensation boundary's `<association>`. Collected before
        // `self.nodes` is consumed below so the boundary-handler resolution can
        // reject associations that point at a non-handler node (e.g. a
        // `textAnnotation`) and detect ambiguity.
        let compensation_handler_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| n.is_for_compensation && n.kind.is_activity())
            .map(|n| n.id.clone())
            .collect();
        // Ids of every node that is the *source* of at least one sequence flow —
        // i.e. it declares an outgoing flow. Used to enforce, at the one site
        // that still knows an element came from an `<endEvent>`, that an end
        // event has no outgoing flow *regardless of its event-definition
        // flavour*. An escalation/compensation end event is remapped to a
        // *throw* element below, which the model-level
        // `cheap_rules::end_events_have_no_outgoing` rule cannot police (an
        // *intermediate* escalation/compensation throw legitimately has an
        // outgoing flow), so a malformed `<endEvent>…<escalationEventDefinition/>`
        // with an outgoing flow would otherwise silently deploy as a routing
        // intermediate throw (#1173).
        let flow_source_ids: std::collections::HashSet<String> =
            self.flows.iter().filter_map(|f| f.source.clone()).collect();
        // Ids of every node an escalation boundary event may legally attach to.
        // An escalation propagates *out* of the inner token scope it is raised
        // in, so only a container that opens such a scope — an embedded
        // sub-process or an ad-hoc sub-process — can catch it. A boundary on any
        // other activity (plain service/user task, call activity) is a dead
        // boundary that `find_catching_escalation_boundary` can never reach
        // (#1173).
        let escalation_container_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::SubProcess) || n.is_adhoc)
            .map(|n| n.id.clone())
            .collect();
        // Ids of ad-hoc sub-process containers, and the parent of every node.
        // Used to reject an escalation boundary attached to an activity that is
        // itself a *tool* of an ad-hoc sub-process (its parent is an ad-hoc
        // container). The runtime arms boundary events on an embedded-subProcess
        // ad-hoc tool, but an interrupting boundary firing on such a tool cannot
        // release it back to its parent container's active set (the tool →
        // agent-re-drive completion path is not wired for boundary interruption),
        // so the container would hang `Active` forever. Refuse the model at deploy
        // rather than mis-execute it (#1173); full runtime support is a follow-up.
        let adhoc_container_ids: std::collections::HashSet<String> = self
            .nodes
            .iter()
            .filter(|n| n.is_adhoc)
            .map(|n| n.id.clone())
            .collect();
        let node_parent: HashMap<String, String> = self
            .nodes
            .iter()
            .filter_map(|n| n.parent.clone().map(|p| (n.id.clone(), p)))
            .collect();
        // Node ids carrying a non-boundary `errorEventDefinition` (an error
        // throw / end). Recorded separately from the node flags (in
        // `error_refs_extra`), so surfaced here as a set for the competing
        // event-definition guard below (#1173).
        let error_def_node_ids: std::collections::HashSet<String> = self
            .error_refs_extra
            .iter()
            .map(|(node_id, _)| node_id.clone())
            .collect();
        for node in self.nodes {
            // Reject an `endEvent` / `intermediateThrowEvent` that carries more
            // than one throw/end event definition (#1173). Multiple definitions
            // set multiple independent node flags, and the build dispatch below
            // picks the FIRST it checks (escalation → compensation → terminate →
            // link → …), silently discarding the others — e.g. an end event with
            // both an escalation and a terminate definition would model as a bare
            // escalation throw, dropping the terminate semantics. There is no
            // combined-semantics element in the supported subset, so refuse the
            // ambiguous model at deploy rather than pick one arm.
            if matches!(node.kind, NodeKind::End | NodeKind::IntermediateThrow) {
                let defs = node.is_escalation_throw as u32
                    + node.is_compensation_throw as u32
                    + node.is_terminate as u32
                    + node.link_name.is_some() as u32
                    + error_def_node_ids.contains(&node.id) as u32;
                // `defs > 1`: competing definitions of different kinds.
                // `escalation_throw_dup`: two `escalationEventDefinition`s on the
                // SAME throw/end — a last-wins overwrite of `escalation_ref` that
                // would silently drop the first (#1173), the throw/end analogue of
                // the boundary `escalation_dup` guard.
                if defs > 1 || node.escalation_throw_dup {
                    return Err(match node.kind {
                        // The `endEvent` flavour has a reason-bearing error; an
                        // `intermediateThrowEvent` reuses the throw-side
                        // `UnsupportedElement` rejection (as the wrong-placement
                        // escalation/compensation throws already do).
                        NodeKind::End if node.escalation_throw_dup => ParseError::InvalidEndEvent {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "end event declares more than one \
                                         escalationEventDefinition; a multi-definition \
                                         escalation end event is not supported (declare \
                                         exactly one)"
                                .to_string(),
                        },
                        NodeKind::End => ParseError::InvalidEndEvent {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "end event declares more than one event definition \
                                     (escalation/compensation/terminate/link/error); a \
                                     multi-definition end event is not supported (declare \
                                     exactly one)"
                                .to_string(),
                        },
                        _ => ParseError::UnsupportedElement {
                            tag: "intermediateThrowEvent".to_string(),
                            element_id: node.id.clone(),
                        },
                    });
                }
            }
            let node_id = node.id.clone();
            let io_id = node.id.clone();
            let node_io = node.io.clone();
            let timer_id = node.id.clone();
            let node_timer = node.timer_expr.clone();
            let retries_id = node.id.clone();
            let node_retries = node.retries.clone();
            let mi_id = node.id.clone();
            let listeners_id = node.id.clone();
            let task_listeners_id = node.id.clone();
            let name_id = node.id.clone();
            let node_name = node.name.clone();
            let node_start_listeners = node.start_listeners.clone();
            let node_end_listeners = node.end_listeners.clone();
            let node_task_listeners = node.task_listeners.clone();
            // Only treat as multi-instance when an input collection was actually
            // declared; a bare `multiInstanceLoopCharacteristics` with no
            // `zeebe:loopCharacteristics inputCollection` degenerates to an
            // ordinary single-instance activity.
            let node_mi = node
                .multi_instance
                .clone()
                .filter(|mi| !mi.input_collection.trim().is_empty());
            let parent = node.parent.clone();
            if matches!(node.kind, NodeKind::Start) && parent.is_none() {
                start_form_id = node.start_form_id.clone();
            }
            if let Some(d) = node.default_flow.clone() {
                default_flow_ids.insert(d);
            }
            builder = match node.kind {
                NodeKind::Start => {
                    // A messageRef makes it a message start; a timer_repeating
                    // flag makes it a timer start (cycle or one-shot); otherwise
                    // a plain none start event.
                    if let Some(message_ref) = node.message_ref {
                        let decl = messages.get(&message_ref).ok_or_else(|| {
                            ParseError::InvalidMessageEvent {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "start event {} references unknown message '{message_ref}'",
                                    node.id
                                ),
                            }
                        })?;
                        builder.message_start_event(node.id, decl.name.clone())
                    } else if let Some(repeating) = node.timer_repeating {
                        let interval_millis = node.duration_millis.unwrap_or(0);
                        if repeating {
                            builder.timer_start_event_cycle(node.id, interval_millis)
                        } else {
                            builder.timer_start_event_once(node.id, interval_millis)
                        }
                    } else {
                        builder.start_event(node.id)
                    }
                }
                NodeKind::End => {
                    // An `<endEvent>` must not declare an outgoing sequence flow,
                    // whatever its event-definition flavour. The escalation and
                    // compensation flavours are remapped to *throw* elements just
                    // below, so the model-level `end_events_have_no_outgoing` rule
                    // (which only sees `EndEvent`/`TerminateEndEvent`) cannot catch
                    // them — guard the end-event flavour here, at the one site that
                    // still knows it came from an `<endEvent>` (#1173).
                    if flow_source_ids.contains(&node.id) {
                        return Err(ParseError::InvalidEndEvent {
                            process_id: self.id.clone(),
                            element_id: node.id.clone(),
                            reason: "an end event must have no outgoing sequence flow".to_string(),
                        });
                    }
                    if node.is_escalation_throw {
                        let code = node
                            .escalation_ref
                            .as_ref()
                            .and_then(|r| escalations.get(r).cloned())
                            .unwrap_or_default();
                        builder.escalation_throw_event(node.id, code)
                    } else if node.is_compensation_throw {
                        builder.compensation_throw_event(node.id)
                    } else if node.is_terminate {
                        builder.terminate_end_event(node.id)
                    } else {
                        builder.end_event(node.id)
                    }
                }
                NodeKind::IntermediateThrow => {
                    if node.is_escalation_throw {
                        let code = node
                            .escalation_ref
                            .as_ref()
                            .and_then(|r| escalations.get(r).cloned())
                            .unwrap_or_default();
                        builder.escalation_throw_event(node.id, code)
                    } else if node.is_compensation_throw {
                        builder.compensation_throw_event(node.id)
                    } else if let Some(link_name) = node.link_name.clone() {
                        builder.link_intermediate_throw_event(node.id, link_name)
                    } else {
                        builder.intermediate_throw_event(node.id)
                    }
                }
                NodeKind::Task => builder.task(node.id),
                NodeKind::Exclusive => builder.exclusive_gateway(node.id),
                NodeKind::Parallel => builder.parallel_gateway(node.id),
                NodeKind::Inclusive => builder.inclusive_gateway(node.id),
                NodeKind::EventBased => builder.event_based_gateway(node.id),
                NodeKind::Service => {
                    // Agent classification is metadata on the job worker. Placement rules
                    // (from AgentDefinitionValidator): aiAgentTask only on a
                    // serviceTask, aiAgentSubProcess only on an adHocSubProcess.
                    // Reject the wrong placement here. An `external` agent is
                    // accepted on either. When present it wins over the
                    // script/decision/job-based interpretations below.
                    if let Some(agent_type) = node.agent_type {
                        use crate::agent::AgentType;
                        match agent_type {
                            AgentType::AiAgentTask if node.is_adhoc => {
                                return Err(ParseError::InvalidAgentDefinition {
                                    process_id: self.id.clone(),
                                    element_id: node.id.clone(),
                                    reason: "agentType 'aiAgentTask' is only valid on a serviceTask, not an adHocSubProcess".to_string(),
                                });
                            }
                            AgentType::AiAgentSubProcess if !node.is_adhoc => {
                                return Err(ParseError::InvalidAgentDefinition {
                                    process_id: self.id.clone(),
                                    element_id: node.id.clone(),
                                    reason: "agentType 'aiAgentSubProcess' is only valid on an adHocSubProcess, not a serviceTask".to_string(),
                                });
                            }
                            _ => {}
                        }
                        let job_type = node.job_type.unwrap_or_else(|| node.id.clone());
                        builder.service_task_with_links_and_agent(
                            node.id,
                            job_type,
                            node.job_priority,
                            node.task_headers,
                            node.linked_resources,
                            Some(agent_type),
                        )
                    } else if let (Some(expr), Some(rv)) = (
                        node.script_expression.clone(),
                        node.script_result_variable.clone(),
                    ) {
                        builder.script_task(node.id, expr, rv)
                    } else if let Some(decision_id) = node.decision_id.clone() {
                        builder.business_rule_task(
                            node.id,
                            decision_id,
                            node.decision_result_variable.clone(),
                        )
                    } else {
                        let job_type = node.job_type.unwrap_or_else(|| node.id.clone());
                        builder.service_task_with_links(
                            node.id,
                            job_type,
                            node.job_priority,
                            node.task_headers,
                            node.linked_resources,
                        )
                    }
                }
                NodeKind::User => builder.user_task_with(node.id, node.user_task),
                NodeKind::IntermediateCatch => {
                    // Ordering: a link catch (linkEventDefinition) is a
                    // pass-through activated by its throw and carries no
                    // message/signal/timer/condition ref; a conditional catch
                    // (event_condition) has no message/signal/timer ref; a
                    // messageRef makes it a message catch; a signalRef a signal
                    // catch; otherwise a timer catch carrying a (possibly zero)
                    // duration.
                    if let Some(link_name) = node.link_name.clone() {
                        builder.link_intermediate_catch_event(node.id, link_name)
                    } else if let Some(condition) = node.event_condition {
                        builder.conditional_intermediate_catch_event(node.id, condition)
                    } else if let Some(message_ref) = node.message_ref {
                        let decl = messages.get(&message_ref).ok_or_else(|| {
                            ParseError::InvalidMessageEvent {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "intermediate catch event {} references unknown message '{message_ref}'",
                                    node.id
                                ),
                            }
                        })?;
                        let correlation_key = decl.correlation_key.clone().ok_or_else(|| {
                            ParseError::InvalidMessageEvent {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "message '{message_ref}' has no zeebe:subscription correlationKey"
                                ),
                            }
                        })?;
                        builder.message_intermediate_catch_event(
                            node.id,
                            decl.name.clone(),
                            correlation_key,
                        )
                    } else if let Some(signal_ref) = node.signal_ref {
                        let name = signals.get(&signal_ref).cloned().ok_or_else(|| {
                            ParseError::InvalidProcess {
                                process_id: self.id.clone(),
                                reason: format!(
                                    "intermediate catch event {} references unknown signal '{signal_ref}'",
                                    node.id
                                ),
                            }
                        })?;
                        builder.signal_intermediate_catch_event(node.id, name)
                    } else {
                        let duration_millis = node.duration_millis.unwrap_or(0);
                        builder.timer_intermediate_catch_event(node.id, duration_millis)
                    }
                }
                NodeKind::SubProcess => {
                    let start = sub_starts.get(&node.id).cloned().ok_or_else(|| {
                        ParseError::InvalidProcess {
                            process_id: self.id.clone(),
                            reason: format!("sub-process {} has no start event", node.id),
                        }
                    })?;
                    builder.sub_process(node.id, start)
                }
                NodeKind::Call => {
                    // The callee is required; an unresolved call activity is a
                    // parse error so it never silently becomes an inert step.
                    let called = node.called_process_id.clone().ok_or_else(|| {
                        ParseError::InvalidProcess {
                            process_id: self.id.clone(),
                            reason: format!("call activity {} has no calledElement", node.id),
                        }
                    })?;
                    builder.call_activity_with_propagation(
                        node.id,
                        called,
                        node.propagate_all_parent_variables.unwrap_or(true),
                        node.propagate_all_child_variables.unwrap_or(true),
                    )
                }
            };
            if let Some(parent) = parent {
                builder = builder.contained_in(node_id, parent);
            }
            if !node_io.is_empty() {
                builder = builder.with_io(io_id, node_io);
            }
            if let Some(timer) = node_timer {
                builder = builder.with_timer(timer_id, timer);
            }
            if let Some(retries) = node_retries {
                builder = builder.with_retries(retries_id, retries);
            }
            if let Some(mi) = node_mi {
                builder = builder.with_multi_instance(mi_id, mi);
            }
            if !node_start_listeners.is_empty() || !node_end_listeners.is_empty() {
                builder =
                    builder.with_listeners(listeners_id, node_start_listeners, node_end_listeners);
            }
            if !node_task_listeners.is_empty() {
                builder = builder.with_task_listeners(task_listeners_id, node_task_listeners);
            }
            if let Some(name) = node_name {
                builder = builder.with_name(name_id, name);
            }
        }
        let associations = self.associations;
        // Ids of the compensation boundary events. A compensation boundary is a
        // structural marker armed implicitly when its activity completes; it is
        // never reached by ordinary token flow (see the flow validation below).
        let compensation_boundary_ids: std::collections::HashSet<String> = self
            .boundaries
            .iter()
            .filter(|b| b.compensation)
            .map(|b| b.id.clone())
            .collect();
        // Ids of every REACTIVE boundary event (error/timer/message/signal/
        // conditional/escalation — i.e. every boundary except the compensation
        // marker handled separately above). A reactive boundary is armed by its
        // own trigger and reached ONLY when that trigger fires; it legitimately
        // has an OUTGOING flow to its handler but must NEVER be the TARGET of a
        // sequenceFlow. If a model wires a token into one, `run_activation_body`
        // would treat it as an ordinary pass-through and complete it (routing its
        // handler) without any event ever being raised or matched — most acutely
        // the new escalation boundary (#1173), whose handler would fire with no
        // escalation. Reject such models at deploy (see the flow validation below)
        // rather than mis-executing them.
        let reactive_boundary_ids: std::collections::HashSet<String> = self
            .boundaries
            .iter()
            .filter(|b| !b.compensation)
            .map(|b| b.id.clone())
            .collect();
        // Execution listeners declared on boundary events (#1197). Collected
        // before the consuming loop below (which moves each `PendingBoundary`)
        // and re-attached by element id after the boundaries are built, so a
        // boundary event's `start`/`end` listeners fire on its own lifecycle
        // rather than being dropped or mis-attached during parsing.
        let boundary_listeners: Vec<(
            String,
            Vec<crate::model::ExecutionListener>,
            Vec<crate::model::ExecutionListener>,
        )> = self
            .boundaries
            .iter()
            .filter(|b| !b.start_listeners.is_empty() || !b.end_listeners.is_empty())
            .map(|b| {
                (
                    b.id.clone(),
                    b.start_listeners.clone(),
                    b.end_listeners.clone(),
                )
            })
            .collect();
        for boundary in self.boundaries {
            let attached_to =
                boundary
                    .attached_to
                    .ok_or_else(|| ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!("boundary event {} has no attachedToRef", boundary.id),
                    })?;
            // An escalation boundary (#1173): resolve its caught code from the
            // `escalationRef` (empty / missing = catch-all) and build it,
            // interrupting per `cancelActivity`. Built before the flavour cascade
            // below since it carries no error/timer/message/signal ref.
            if boundary.escalation {
                // Reject an AMBIGUOUS multi-definition boundary (#1173): a
                // `boundaryEvent` carrying an `escalationEventDefinition` *plus*
                // another trigger (error/timer/message/signal/conditional/
                // compensation) would be silently modelled as escalation here (the
                // escalation branch wins and `continue`s), discarding the declared
                // second trigger and changing the model's behaviour. There is no
                // OR-trigger boundary in the supported subset, so refuse the model
                // at deploy rather than drop a declared event.
                let other_trigger = boundary.error_ref.is_some()
                    || boundary.timer_duration_millis.is_some()
                    || boundary.timer_expr.is_some()
                    || boundary.message_ref.is_some()
                    || boundary.signal_ref.is_some()
                    || boundary.condition.is_some()
                    || boundary.compensation;
                if other_trigger || boundary.escalation_dup {
                    return Err(ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "escalation boundary event {} declares more than one event \
                             definition; a multi-trigger boundary is not supported (declare \
                             exactly one of escalation/error/timer/message/signal/conditional/\
                             compensation)",
                            boundary.id
                        ),
                    });
                }
                // An escalation boundary can only catch an escalation raised in
                // the inner scope of the container it is attached to. Reject an
                // attachment to anything that opens no such scope (plain task,
                // call activity, …) so a dead boundary can never deploy (#1173).
                if !escalation_container_ids.contains(&attached_to) {
                    return Err(ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "escalation boundary event {} must be attached to an embedded \
                             sub-process or ad-hoc sub-process (attachedToRef '{attached_to}' \
                             is not a container that can raise an escalation)",
                            boundary.id
                        ),
                    });
                }
                // Reject an escalation boundary attached to a *tool* of an ad-hoc
                // sub-process (its parent is an ad-hoc container). The runtime arms
                // boundary events on an embedded-subProcess ad-hoc tool, but an
                // interrupting boundary firing on such a tool has no wired path to
                // release the tool back to its parent container's active set, so
                // the container would remain `Active` forever after the handler
                // completes. Refuse it at deploy rather than mis-execute (#1173).
                if node_parent
                    .get(&attached_to)
                    .is_some_and(|p| adhoc_container_ids.contains(p))
                {
                    return Err(ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "escalation boundary event {} is attached to '{attached_to}', a tool \
                             of an ad-hoc sub-process; an escalation boundary on an ad-hoc tool \
                             is not supported (attach it to a top-level embedded or ad-hoc \
                             sub-process instead)",
                            boundary.id
                        ),
                    });
                }
                let escalation_code = boundary
                    .escalation_ref
                    .as_ref()
                    .and_then(|r| escalations.get(r).cloned())
                    .unwrap_or_default();
                builder = if boundary.interrupting {
                    builder.escalation_boundary_event(boundary.id, attached_to, escalation_code)
                } else {
                    builder.non_interrupting_escalation_boundary_event(
                        boundary.id,
                        attached_to,
                        escalation_code,
                    )
                };
                continue;
            }
            // A compensation boundary marks its activity compensable; its
            // handler activity is the other end of the `<association>` wiring it.
            // Resolve it by scanning every association touching the boundary and
            // keeping only endpoints that are real `isForCompensation` handler
            // activities — this rejects a stray association to a `textAnnotation`
            // and rejects (rather than silently picking one of) an ambiguous set
            // of multiple candidate handlers.
            if boundary.compensation {
                let mut candidates: Vec<String> = associations
                    .iter()
                    .filter_map(|(source, target)| {
                        let other = if source == &boundary.id {
                            target
                        } else if target == &boundary.id {
                            source
                        } else {
                            return None;
                        };
                        compensation_handler_ids
                            .contains(other)
                            .then(|| other.clone())
                    })
                    .collect();
                candidates.sort();
                candidates.dedup();
                let handler = match candidates.as_slice() {
                    [single] => single.clone(),
                    [] => {
                        return Err(ParseError::InvalidBoundaryEvent {
                            process_id: self.id.clone(),
                            reason: format!(
                                "compensation boundary event {} has no associated `isForCompensation` handler activity (missing or non-handler <association>)",
                                boundary.id
                            ),
                        })
                    }
                    _ => {
                        return Err(ParseError::InvalidBoundaryEvent {
                            process_id: self.id.clone(),
                            reason: format!(
                                "compensation boundary event {} is associated with multiple handler activities ({}); exactly one is required",
                                boundary.id,
                                candidates.join(", ")
                            ),
                        })
                    }
                };
                builder = builder.compensation_boundary_event(boundary.id, attached_to, handler);
                continue;
            }
            // Resolve the boundary's flavour: conditional (condition), timer
            // (duration), message (messageRef), signal (signalRef), or error
            // (errorRef -> declared error).
            if let Some(condition) = boundary.condition {
                builder = if boundary.interrupting {
                    builder.conditional_boundary_event(boundary.id, attached_to, condition)
                } else {
                    builder.non_interrupting_conditional_boundary_event(
                        boundary.id,
                        attached_to,
                        condition,
                    )
                };
            } else if boundary.timer_duration_millis.is_some() || boundary.timer_expr.is_some() {
                // A FEEL timer boundary carries no static duration; build the
                // element with a zero fallback and attach the expression below.
                let duration_millis = boundary.timer_duration_millis.unwrap_or(0);
                let boundary_id = boundary.id.clone();
                let timer_expr = boundary.timer_expr.clone();
                builder = if boundary.interrupting {
                    // An interrupting timer fires once and cancels its activity,
                    // so a cycle is treated as a one-shot at the interval.
                    builder.timer_boundary_event(boundary.id, attached_to, duration_millis)
                } else if boundary.timer_repeating {
                    builder.non_interrupting_timer_cycle_boundary_event(
                        boundary.id,
                        attached_to,
                        duration_millis,
                    )
                } else {
                    builder.non_interrupting_timer_boundary_event(
                        boundary.id,
                        attached_to,
                        duration_millis,
                    )
                };
                if let Some(timer) = timer_expr {
                    builder = builder.with_timer(boundary_id, timer);
                }
            } else if let Some(message_ref) = boundary.message_ref {
                let decl =
                    messages
                        .get(&message_ref)
                        .ok_or_else(|| ParseError::InvalidMessageEvent {
                            process_id: self.id.clone(),
                            reason: format!(
                                "boundary event {} references unknown message '{message_ref}'",
                                boundary.id
                            ),
                        })?;
                let correlation_key = decl.correlation_key.clone().ok_or_else(|| {
                    ParseError::InvalidMessageEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "message '{message_ref}' has no zeebe:subscription correlationKey"
                        ),
                    }
                })?;
                builder = if boundary.interrupting {
                    builder.message_boundary_event(
                        boundary.id,
                        attached_to,
                        decl.name.clone(),
                        correlation_key,
                    )
                } else {
                    builder.non_interrupting_message_boundary_event(
                        boundary.id,
                        attached_to,
                        decl.name.clone(),
                        correlation_key,
                    )
                };
            } else if let Some(signal_ref) = boundary.signal_ref {
                let name = signals.get(&signal_ref).cloned().ok_or_else(|| {
                    ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "boundary event {} references unknown signal '{signal_ref}'",
                            boundary.id
                        ),
                    }
                })?;
                builder = if boundary.interrupting {
                    builder.signal_boundary_event(boundary.id, attached_to, name)
                } else {
                    builder.non_interrupting_signal_boundary_event(boundary.id, attached_to, name)
                };
            } else {
                let error_ref = boundary.error_ref.unwrap_or_default();
                let error_code = errors.get(&error_ref).cloned().ok_or_else(|| {
                    ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "boundary event {} references unknown error '{error_ref}'",
                            boundary.id
                        ),
                    }
                })?;
                builder = builder.error_boundary_event(boundary.id, attached_to, error_code);
            }
        }
        // Re-attach the boundary events' execution listeners now that every
        // boundary element exists (#1197). `with_listeners` resolves by id, so a
        // boundary listener lands on its own element rather than the enclosing
        // activity it parsed adjacent to.
        for (id, start_listeners, end_listeners) in boundary_listeners {
            builder = builder.with_listeners(id, start_listeners, end_listeners);
        }
        // Compensation boundary events and their `isForCompensation` handlers are
        // structural markers, NOT part of ordinary token flow: a compensation
        // boundary is armed implicitly when its activity completes (never reached
        // by a sequenceFlow), and a handler runs only when a compensation throw
        // triggers it (never entered by an incoming token, and its completion is
        // routed back to the waiting throw rather than onward). If a model wires a
        // sequenceFlow to or from either, `run_activation_body` would treat the
        // element as an ordinary pass-through and route tokens through something
        // the engine never arms — corrupting execution. Reject such models at
        // deploy with a clear error rather than mis-executing them. (Handlers are
        // checked against every `isForCompensation` activity, not just the
        // resolved one, so an orphaned handler dragged into normal flow is caught
        // too.)
        for flow in self.flows.iter() {
            for endpoint in [flow.source.as_deref(), flow.target.as_deref()]
                .into_iter()
                .flatten()
            {
                if compensation_boundary_ids.contains(endpoint) {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "compensation boundary event {endpoint} must not be the source or target of a sequenceFlow"
                        ),
                    });
                }
                if compensation_handler_ids.contains(endpoint) {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "compensation handler activity {endpoint} (isForCompensation) must not be the source or target of a sequenceFlow"
                        ),
                    });
                }
            }
            // A reactive boundary event has an outgoing flow to its handler but is
            // itself only reached when its trigger fires — so it may be a flow
            // SOURCE but never a flow TARGET. Reject an incoming sequenceFlow so a
            // token can never drive the handler without the event being raised.
            if let Some(target) = flow.target.as_deref() {
                if reactive_boundary_ids.contains(target) {
                    return Err(ParseError::InvalidProcess {
                        process_id: self.id.clone(),
                        reason: format!(
                            "boundary event {target} must not be the target of a sequenceFlow (a boundary event is reached only when its trigger fires)"
                        ),
                    });
                }
            }
        }
        for flow in self.flows {
            let (source, target) = match (flow.source, flow.target) {
                (Some(s), Some(t)) => (s, t),
                _ => {
                    return Err(ParseError::IncompleteSequenceFlow {
                        process_id: self.id,
                    })
                }
            };
            let is_default = flow
                .id
                .as_deref()
                .is_some_and(|id| default_flow_ids.contains(id));
            builder = match (is_default, flow.condition) {
                (true, _) => builder.connect_default(source, target),
                (false, Some(expression)) => builder.connect_when(source, target, expression),
                (false, None) => builder.connect(source, target),
            };
        }
        let mut def = builder.build().map_err(|e| ParseError::InvalidProcess {
            process_id: self.id,
            reason: e.to_string(),
        })?;
        def.adhoc = adhoc_catalog;
        def.start_form_id = start_form_id;
        Ok(def)
    }
}
