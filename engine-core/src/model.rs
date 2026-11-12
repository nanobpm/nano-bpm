//! The BPMN process model.
//!
//! This is a deliberately tiny subset of BPMN — enough to demonstrate the
//! execution architecture end to end. Parsing real BPMN 2.0 XML into this model
//! is intentionally out of scope for the core (a future `bpmn-parser` crate can
//! produce [`ProcessDefinition`]s); here processes are built programmatically via
//! [`ProcessBuilder`].

use std::collections::{BTreeMap, HashMap};

/// Serde default for boundary-event `interrupting` flags: older serialized
/// definitions (before non-interrupting boundaries existed) carried only
/// interrupting boundaries, so a missing field deserializes as `true`.
#[cfg(feature = "serde")]
fn default_true() -> bool {
    true
}

/// Identifier of a BPMN element (the BPMN `id` attribute), e.g. `"start"`.
pub type ElementId = String;

/// A process variable value.
///
/// A JSON-like value type spanning what FEEL operates over: `null`, booleans,
/// numbers (integers kept distinct from decimals so integral values round-trip
/// exactly), strings, lists and contexts (maps). Numbers compare numerically
/// across `Int`/`Double` inside the FEEL evaluator; the derived `PartialEq` here
/// is structural (used by event/state equality), so `Int(3) != Double(3.0)`
/// structurally — callers needing FEEL semantics go through [`crate::feel`].
///
/// `Eq` is implemented by hand (rather than derived) so the many `#[derive(Eq)]`
/// log/state types that embed a `Value` keep compiling despite the `f64` payload.
/// This is sound because the evaluator never produces `NaN` (FEEL maps invalid
/// arithmetic to `Null`), so reflexivity holds for every value we actually store.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    /// The FEEL `null` (and the result of an unresolved variable or a failed
    /// computation).
    Null,
    Bool(bool),
    /// An integral number. Kept distinct from [`Value::Double`] so whole numbers
    /// round-trip without acquiring a fractional rendering.
    Int(i64),
    /// A non-integral number.
    Double(f64),
    Str(String),
    /// An ordered list (FEEL list).
    List(Vec<Value>),
    /// A context (FEEL map): an ordered set of named entries.
    Map(BTreeMap<String, Value>),
}

impl Eq for Value {}

impl Value {
    /// The number this value holds, if it is numeric (`Int` or `Double`).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Double(d) => Some(*d),
            _ => None,
        }
    }

    /// A cheap O(n) estimate of this value's heap payload in bytes. Counts string
    /// and key bytes plus a small fixed per-node overhead; the exact allocator
    /// footprint is not needed — this is only a size proxy proportional to the
    /// payload, used to meter in-flight create payloads for byte-aware admission
    /// control. Scalars fold to a small constant; strings/lists/maps recurse.
    pub fn approx_bytes(&self) -> u64 {
        match self {
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Double(_) => 8,
            Value::Str(s) => 16 + s.len() as u64,
            Value::List(items) => 16 + items.iter().map(Value::approx_bytes).sum::<u64>(),
            Value::Map(entries) => {
                16 + entries
                    .iter()
                    .map(|(k, v)| k.len() as u64 + v.approx_bytes())
                    .sum::<u64>()
            }
        }
    }

    /// Builds a numeric value, narrowing to [`Value::Int`] when the number is a
    /// finite integer and to [`Value::Null`] when it is not finite (FEEL has no
    /// infinity/NaN). This keeps integral arithmetic results rendering cleanly.
    pub fn number(n: f64) -> Value {
        if !n.is_finite() {
            return Value::Null;
        }
        if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
            Value::Int(n as i64)
        } else {
            Value::Double(n)
        }
    }
}

/// Renders an `f64` the FEEL way: a finite integral value drops the fractional
/// part (`2.0` -> `"2"`), everything else uses the shortest round-trip form.
pub(crate) fn format_double(d: f64) -> String {
    if d.is_finite() && d.fract() == 0.0 && d.abs() < i64::MAX as f64 {
        (d as i64).to_string()
    } else {
        d.to_string()
    }
}

/// A boolean guard on a sequence flow: a FEEL expression evaluated against the
/// instance variables (the conditionExpression body, e.g. `= amount > 10`).
///
/// The full FEEL grammar supported by [`crate::feel`] applies — comparisons,
/// arithmetic, boolean logic, member access — not just equality.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Condition {
    /// The raw FEEL expression. A single leading `=` marker is optional (it is
    /// stripped by the evaluator).
    pub expression: String,
}

impl Condition {
    /// Wraps a raw FEEL expression as a sequence-flow condition.
    pub fn new(expression: impl Into<String>) -> Self {
        Condition {
            expression: expression.into(),
        }
    }

    /// Evaluates the condition against a set of variables, returning the FEEL
    /// error on a parse/type failure or a non-boolean result. A condition-routed
    /// gateway (exclusive or inclusive) turns such an error into an
    /// `ExpressionEvaluation` incident.
    pub fn eval(&self, variables: &HashMap<String, Value>) -> Result<bool, crate::feel::FeelError> {
        crate::feel::eval_bool(&self.expression, variables)
    }
}

/// Identifies one sequence flow into an element by its source element and its
/// ordinal among the source's flows to that same target (`0` for the usual
/// single flow; a second, parallel flow between the same two elements is `1`).
///
/// Nano's [`SequenceFlow`]s carry no BPMN id, so this is the stable identity a
/// parallel join counts arrivals by. Zeebe keys the same bookkeeping by the
/// sequence-flow id (#1233). The ordinal is taken over flows to the same target
/// only, so adding an unrelated outgoing flow to the source does not shift it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IncomingFlow {
    pub from: ElementId,
    #[cfg_attr(feature = "serde", serde(default))]
    pub ordinal: usize,
}

/// An outgoing sequence flow: a target element and an optional guard condition.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SequenceFlow {
    pub to: ElementId,
    /// `None` means an unconditional flow (its absent guard always holds). The
    /// two condition-routed gateways treat that differently: an **exclusive**
    /// gateway takes only the *first* matching flow in document order, so an
    /// unconditional flow placed last acts as a de-facto default; an
    /// **inclusive** gateway takes *every* matching flow, so an unconditional
    /// non-default flow is **always** selected (never a fallback). For an
    /// inclusive fallback use the explicit [`is_default`](Self::is_default) flow,
    /// which is chosen only when no other flow matches (see `select_inclusive_flows`).
    pub condition: Option<Condition>,
    /// True when this is the condition-routed gateway's explicit **default** flow
    /// (the gateway's `default="..."` attribute). A default flow is selected only
    /// as a fallback — after every non-default flow's condition has evaluated
    /// false — regardless of its document order among the outgoing flows.
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_default: bool,
}

/// A single `zeebe:input` / `zeebe:output` variable mapping: a FEEL `source`
/// expression whose result is assigned to the `target` variable path.
///
/// `source` is a FEEL expression (an optional leading `=` is tolerated) and
/// `target` is a variable name or a dotted path (`order.total`) naming a nested
/// context entry. Nano keeps a single flat instance-level variable scope, so
/// both input and output mappings resolve their source against — and merge
/// their result into — the instance variables (Zeebe's local element scope is
/// collapsed to the instance scope).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Mapping {
    /// The FEEL source expression (a leading `=` marker is optional).
    pub source: String,
    /// The target variable name or dotted path the source result is written to.
    pub target: String,
}

/// The `zeebe:ioMapping` of a BPMN activity: input mappings applied when the
/// element **activates** and output mappings applied when it **completes**.
///
/// Each mapping's `source` FEEL expression is evaluated against the instance
/// variables and merged into them under its `target` path. Input mappings run
/// on activation (so an activity's job/subscription sees the mapped values);
/// output mappings run on completion (so a service task's job result, already
/// merged into the instance variables, can be projected/renamed). An empty
/// mapping list (the default) is a no-op, so pre-existing definitions are
/// unaffected.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IoMapping {
    /// Input mappings, applied on element activation.
    pub inputs: Vec<Mapping>,
    /// Output mappings, applied on element completion.
    pub outputs: Vec<Mapping>,
}

impl IoMapping {
    /// Whether there are no mappings at all (the common case).
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty() && self.outputs.is_empty()
    }
}

/// How a [`LinkedResource`]'s `resource_id` is resolved to a concrete deployed
/// resource version at job activation (Zeebe `bindingType`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum BindingType {
    /// Bind to the version deployed alongside the process definition. Nano does
    /// not yet track per-resource deployment membership, so this currently
    /// resolves to the latest version (documented simplification).
    Deployment,
    /// Bind to the latest deployed version of the resource id (the default).
    #[default]
    Latest,
    /// Bind to the version carrying a matching `version_tag`, falling back to
    /// the latest when no resource carries the tag.
    VersionTag,
}

/// A `zeebe:linkedResource`: a service task's declarative link to a deployed
/// resource (e.g. a generic Markdown agent prompt) by its `resource_id`. At job
/// activation the engine resolves the id to a concrete `resourceKey` per
/// `binding_type` and delivers the resolved set to the worker in the
/// `linkedResources` custom header, so the worker can fetch the content via the
/// resource API. Mirrors Zeebe's linked-resource extension element.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LinkedResource {
    /// The linked resource's id (its filename, for a generic resource).
    pub resource_id: String,
    /// How `resource_id` is resolved to a version at activation.
    pub binding_type: BindingType,
    /// The worker-facing resource type label (opaque to the engine, e.g. `RPA`
    /// or a custom `GenericScript`). Echoed verbatim into the resolved header.
    pub resource_type: String,
    /// The version tag matched when `binding_type` is [`BindingType::VersionTag`].
    pub version_tag: Option<String>,
    /// The key under which the worker looks up the resolved resource in the
    /// `linkedResources` header (Zeebe `linkName`).
    pub link_name: String,
}

/// The lifecycle transition an execution listener fires on (Zeebe
/// `zeebe:executionListener eventType`). `Start` listeners run between an
/// element's `ACTIVATING` and `ACTIVATED` events; `End` listeners run between
/// its `COMPLETING` and `COMPLETED` events (ADR 0037).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ListenerEventType {
    /// Runs on the `ACTIVATING -> ACTIVATED` transition, before the element's
    /// own behaviour (input mappings, job creation, flow routing).
    Start,
    /// Runs on the `COMPLETING -> COMPLETED` transition, before output mappings
    /// and outgoing flows are taken.
    End,
}

/// A single BPMN execution listener declared on a flow node
/// (`zeebe:executionListener`). Each listener is realised as a job of
/// `job_type` that must be completed by a worker before the element's lifecycle
/// transition proceeds; listeners of the same event type run sequentially in
/// declaration order and their returned variables merge into the element's
/// scope (Zeebe parity, ADR 0037). Execution listeners cannot deny the
/// transition (that is a task-listener capability, deferred).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExecutionListener {
    /// Which transition this listener fires on.
    pub event_type: ListenerEventType,
    /// The job type workers subscribe to for this listener.
    pub job_type: String,
    /// The raw `retries` expression (literal or FEEL), evaluated at listener-job
    /// creation. `None` (the default) starts the job with
    /// [`crate::state::DEFAULT_JOB_RETRIES`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub retries: Option<String>,
}

/// The lifecycle transition a user-task listener fires on
/// (`zeebe:taskListener eventType`). Unlike execution listeners, task listeners
/// exist only on user tasks and the `assigning`, `updating` and `completing`
/// events may *deny* the transition and return *corrections* to task data
/// (ADR 0037 §6, Zeebe parity).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TaskListenerEventType {
    /// Fires while the user task is being made available (element
    /// `ACTIVATING`), before it becomes `Created`. Cannot deny.
    Creating,
    /// Fires when an assignee is being set (assign, or an initial assignee on
    /// creation). May deny and correct.
    Assigning,
    /// Fires when task data is being updated (`UpdateUserTask`). May deny and
    /// correct.
    Updating,
    /// Fires when the task is being completed, before it becomes `Completed`.
    /// May deny and correct.
    Completing,
    /// Fires when the task is being canceled by process termination, before it
    /// becomes `Canceled`. Cannot deny (cancellation must proceed).
    Canceling,
}

/// A single BPMN task listener declared on a user task (`zeebe:taskListener`).
/// Realised as a job of `job_type` that a worker must complete before the
/// user-task lifecycle transition proceeds; listeners of the same event type
/// run sequentially in declaration order (ADR 0037 §6).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TaskListener {
    /// Which user-task transition this listener fires on.
    pub event_type: TaskListenerEventType,
    /// The job type workers subscribe to for this listener.
    pub job_type: String,
    /// The raw `retries` expression (literal or FEEL). `None` (the default)
    /// starts the job with [`crate::state::DEFAULT_JOB_RETRIES`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub retries: Option<String>,
}

/// The flavour of a FEEL timer expression, mirroring the BPMN
/// `timerEventDefinition` children.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimerDefKind {
    /// A `timeDuration`: a relative delay (the timer is due `now + duration`).
    Duration,
    /// A `timeCycle`: a repeating interval (`R{n}/{duration}` or a bare
    /// duration); the cycle count is ignored — Nano cycles are unbounded.
    Cycle,
    /// A `timeDate`: an absolute point in time (the timer is due at that instant).
    Date,
}

/// A FEEL timer expression declared on a timer event, evaluated against the
/// instance variables (or an empty context for a process-level start timer)
/// when the timer is created, rather than parsed as a static ISO-8601 literal
/// at deploy time. This is how a timer references variables
/// (`= "PT" + hours + "H"`, `= dueDate`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimerDef {
    /// Whether the expression yields a duration, a cycle, or an absolute date.
    pub kind: TimerDefKind,
    /// The raw FEEL expression text (a leading `=` marker is optional).
    pub expr: String,
}

/// Multi-instance loop characteristics declared on an activity (a service task
/// in nano's supported subset). Mirrors BPMN `multiInstanceLoopCharacteristics`
/// plus Zeebe's `zeebe:loopCharacteristics` extension: on activation the FEEL
/// `input_collection` is evaluated to a list, and one child instance of the
/// activity is created per item (all at once when `sequential == false`, one
/// after another when `sequential == true`).
///
/// Each child runs in a local variable scope layered over the instance
/// variables: `input_element` (when set) binds that item, and `loopCounter`
/// binds the 1-based index. On each child's completion the FEEL `output_element`
/// (when set) is evaluated in the child scope and collected, by index, into the
/// list variable named by `output_collection`, which is written to the instance
/// scope when the multi-instance body completes. After each child completes the
/// FEEL `completion_condition` (when set) is evaluated; a `true` result cancels
/// any remaining children and completes the body early.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MultiInstance {
    /// FEEL expression (leading `=` optional) evaluated on activation to the
    /// collection of items driving the loop.
    pub input_collection: String,
    /// Local variable name each item is bound to for the child instance. `None`
    /// binds no item variable (only `loopCounter` is available).
    #[cfg_attr(feature = "serde", serde(default))]
    pub input_element: Option<String>,
    /// Name of the instance-level list variable the per-child `output_element`
    /// results are collected into. `None` collects nothing.
    #[cfg_attr(feature = "serde", serde(default))]
    pub output_collection: Option<String>,
    /// FEEL expression (leading `=` optional) evaluated in each child scope at
    /// child completion; its result is stored at the child's index in
    /// `output_collection`. `None` collects nothing.
    #[cfg_attr(feature = "serde", serde(default))]
    pub output_element: Option<String>,
    /// FEEL boolean expression evaluated after each child completes; `true`
    /// cancels remaining children and completes the body early. `None` waits for
    /// every child.
    #[cfg_attr(feature = "serde", serde(default))]
    pub completion_condition: Option<String>,
    /// The BPMN `cancelRemainingInstances` attribute (default `true`). When the
    /// `<completionCondition>` fires: `true` cancels the children still running
    /// and completes the body at once; `false` latches the fulfilment and
    /// completes the body once the outstanding children drain (Zeebe
    /// `MultiInstanceBodyProcessor` / `isCompletionConditionFulfilled`).
    #[cfg_attr(
        feature = "serde",
        serde(default = "crate::state::types::default_true")
    )]
    pub cancel_remaining_instances: bool,
    /// `true` runs children one at a time (each starts only after the previous
    /// completes); `false` (the default) runs them all in parallel.
    #[cfg_attr(feature = "serde", serde(default))]
    pub sequential: bool,
}

impl Default for MultiInstance {
    /// Explicit so programmatic construction (`..Default::default()`) agrees
    /// with deserialization: `cancel_remaining_instances` defaults to `true`
    /// (the BPMN default), not the `bool` zero value.
    fn default() -> Self {
        Self {
            input_collection: String::new(),
            input_element: None,
            output_collection: None,
            output_element: None,
            completion_condition: None,
            cancel_remaining_instances: true,
            sequential: false,
        }
    }
}

/// The (raw, un-evaluated) assignment, scheduling and priority expressions
/// declared on a `userTask` BPMN element via its Zeebe extension elements
/// (`zeebe:assignmentDefinition`, `zeebe:taskSchedule`, `zeebe:priorityDefinition`),
/// together with its form linkage (`zeebe:formDefinition`).
///
/// Each value may be a literal or a FEEL expression (a string beginning with
/// `=`). They are resolved against the instance variables when the user task is
/// created. `candidate_groups`/`candidate_users` are either a static
/// comma-separated list or a FEEL expression yielding a list (or comma string).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserTaskProps {
    /// Raw assignee expression (`zeebe:assignmentDefinition assignee`).
    pub assignee: Option<String>,
    /// Raw candidate-groups expression (`zeebe:assignmentDefinition candidateGroups`).
    pub candidate_groups: Option<String>,
    /// Raw candidate-users expression (`zeebe:assignmentDefinition candidateUsers`).
    pub candidate_users: Option<String>,
    /// Raw due-date expression (`zeebe:taskSchedule dueDate`).
    pub due_date: Option<String>,
    /// Raw follow-up-date expression (`zeebe:taskSchedule followUpDate`).
    pub follow_up_date: Option<String>,
    /// Raw priority expression (`zeebe:priorityDefinition priority`); defaults to
    /// `50` when absent or unresolvable.
    pub priority: Option<String>,
    /// The embedded/deployment form id declared via `<zeebe:formDefinition
    /// formId="…"/>`. Resolved against the currently-deployed forms (latest
    /// version) to a numeric `form_key` when the user task is created, so the v2
    /// user-task search can surface a `formKey` and downstream `GetFormByKey`
    /// can serve the schema. `None` when the task declares no `formId` (or an
    /// external form instead).
    ///
    /// Only Camunda's default `latest` binding is implemented. A
    /// `<zeebe:formDefinition bindingType="deployment"|"versionTag">` is
    /// rejected at deploy time (`ParseError::UnsupportedUserTaskFormBinding`)
    /// rather than silently degraded to `latest` (#1190).
    #[cfg_attr(feature = "serde", serde(default))]
    pub form_id: Option<String>,
    /// The external form reference declared via `<zeebe:formDefinition
    /// externalReference="…"/>`. Surfaced verbatim as the task's
    /// `externalFormReference`; the engine does not resolve it to a key. `None`
    /// when the task declares no external reference.
    #[cfg_attr(feature = "serde", serde(default))]
    pub external_form_reference: Option<String>,
}

/// serde default for the `CallActivity` propagation flags: `true`, matching the
/// Zeebe default and the BPMN parser's default for an absent
/// `propagateAll{Parent,Child}Variables` attribute. Used so a pre-#1057 snapshot
/// (whose `CallActivity` predates these fields) deserializes with the same
/// semantics a fresh redeploy would produce.
#[cfg(feature = "serde")]
fn default_propagate() -> bool {
    true
}

/// The start-event trigger of an event sub-process
/// ([`ElementKind::EventSubProcess`]) — which event, raised inside (or observed
/// by) the enclosing scope, activates the sub-process.
///
/// This mirrors the six event definitions Zeebe permits on an event
/// sub-process's start event. Every field carries the *resolved* reference
/// (a message/signal/error/escalation name or code, not a BPMN `…Ref` id),
/// exactly like the corresponding boundary/catch element kinds.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EventSubProcessTrigger {
    /// A `messageEventDefinition`: the sub-process starts when a message named
    /// `message_name` correlates to the enclosing scope. `correlation_key` is
    /// the raw (un-evaluated) correlation-key expression declared via
    /// `zeebe:subscription correlationKey` (typically `=`-prefixed FEEL),
    /// resolved against the scope's variables when the subscription is opened —
    /// same shape as [`ElementKind::MessageIntermediateCatchEvent`].
    Message {
        /// The BPMN message name this trigger subscribes to.
        message_name: String,
        /// The raw correlation-key expression (the variable/expression whose
        /// value identifies the scope to correlate to).
        correlation_key: String,
    },
    /// A `timerEventDefinition`: the sub-process starts when the timer becomes
    /// due after the enclosing scope activates. The definition is always
    /// carried as a [`TimerDef`] — a static ISO-8601 literal keeps its literal
    /// text (unlike a process-level timer start, an event sub-process timer is
    /// armed per *scope activation*, so it is never pre-resolved to millis at
    /// deploy time); a `=`-prefixed FEEL expression is evaluated at arming.
    /// `TimerDefKind::Cycle` re-arms after each fire while the scope lives.
    Timer(TimerDef),
    /// A `signalEventDefinition`: the sub-process starts when a signal named
    /// `signal_name` is broadcast while the enclosing scope is active. Signals
    /// correlate by name only (no correlation key).
    Signal {
        /// The BPMN signal name this trigger subscribes to.
        signal_name: String,
    },
    /// An `errorEventDefinition`: the sub-process starts when an error whose
    /// code matches `error_code` is thrown inside the enclosing scope (an error
    /// end event, or a job error). `None` (no `errorRef`) is a **catch-all**
    /// that matches any error not claimed by a more specific code at the same
    /// scope. Always interrupting — Zeebe rejects a non-interrupting error
    /// event sub-process at deploy.
    Error {
        /// The BPMN error code this trigger catches; `None` is a catch-all.
        error_code: Option<String>,
    },
    /// An `escalationEventDefinition`: the sub-process starts when an
    /// escalation whose code matches `escalation_code` is raised inside the
    /// enclosing scope. `None` (no `escalationRef`) is a **catch-all**, same
    /// convention as [`ElementKind::EscalationBoundaryEvent`]'s empty code.
    Escalation {
        /// The BPMN escalation code this trigger catches; `None` is a
        /// catch-all.
        escalation_code: Option<String>,
    },
    /// A `conditionalEventDefinition`: the sub-process starts when the FEEL
    /// boolean `condition` becomes `true` in the enclosing scope — evaluated
    /// when the scope activates and again whenever a referenced variable
    /// changes while the scope is active.
    Conditional {
        /// The FEEL boolean condition (as authored, typically `=`-prefixed).
        condition: String,
    },
}

/// The kind of a BPMN flow node.
///
/// The set is intentionally small. New element types plug in here and gain
/// behaviour in `engine::process_step` — the rest of the architecture is
/// unaffected.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ElementKind {
    /// A none start event. Pass-through: activates then immediately completes.
    StartEvent,
    /// A none end event. Pass-through; consuming the last token completes the
    /// process instance.
    EndEvent,
    /// A terminate end event (`<endEvent>` with a `<terminateEventDefinition>`).
    ///
    /// Unlike a none end event, reaching it does not merely consume its own
    /// token: it **kills every other still-active token in its enclosing scope**
    /// (parallel-split sibling branches, pending timers, open jobs and
    /// subscriptions) and then completes that scope — the Zeebe/Camunda
    /// terminate semantics. A terminate end in the top-level process terminates
    /// the whole instance (`ProcessInstanceTerminated`); a terminate end inside
    /// an embedded sub-process terminates only that sub-process scope and lets
    /// the parent instance continue on the sub-process's outgoing flow.
    TerminateEndEvent,
    /// A service task. On activation it creates a job of `job_type` and the token
    /// rests until the job is completed. `priority` is the *raw* (un-evaluated)
    /// job-priority expression declared via `zeebe:priorityDefinition` (a literal
    /// or a FEEL expression); it is resolved against the instance variables when
    /// the job is created and controls activation order (higher first). `None`
    /// means no declaration, which resolves to the default priority.
    ServiceTask {
        job_type: String,
        priority: Option<String>,
        /// Optional agent classification. The marker does not change the job
        /// lifecycle or discard any service-task configuration.
        #[cfg_attr(
            feature = "serde",
            serde(default, skip_serializing_if = "Option::is_none")
        )]
        agent_type: Option<crate::agent::AgentType>,
        /// Static custom headers declared on the task via `zeebe:taskHeaders`
        /// (`<zeebe:header key="…" value="…"/>`). Immutable model metadata
        /// surfaced verbatim on the activated job (Zeebe `ActivatedJob.customHeaders`).
        /// A `BTreeMap` so the serialized order is deterministic. Empty when the
        /// task declares no headers and when deserializing definitions written
        /// before headers were parsed.
        #[cfg_attr(feature = "serde", serde(default))]
        custom_headers: BTreeMap<String, String>,
        /// Linked resources declared via `zeebe:linkedResources`
        /// (`<zeebe:linkedResource resourceId=… bindingType=… resourceType=…
        /// linkName=… versionTag=…/>`). Each links the task to a deployed
        /// resource (e.g. a generic Markdown agent prompt) by id; at job
        /// activation the engine resolves each entry's id to a concrete
        /// `resourceKey` (per `binding_type`) and delivers the resolved set to
        /// the worker in the `linkedResources` custom header (Zeebe parity).
        /// Empty when the task declares none, and when deserializing definitions
        /// written before linked resources were parsed.
        #[cfg_attr(feature = "serde", serde(default))]
        linked_resources: Vec<LinkedResource>,
    },
    /// A business rule task bound to a DMN decision via `zeebe:calledDecision`.
    /// On activation the engine evaluates the referenced decision natively
    /// against the element's variables and, on success, merges the decision
    /// output into the instance under `result_variable` (or, if `None`, spreads a
    /// map output's entries). Unlike a service task there is no job and no
    /// worker — evaluation is synchronous. `decision_id` is the *raw*
    /// (un-evaluated) decision-id expression (a literal id, or a FEEL expression
    /// when prefixed with `=`), resolved against the instance variables at
    /// activation. A business rule task that instead carries a `zeebe:taskDefinition`
    /// (job-worker style) is modelled as a [`ElementKind::ServiceTask`].
    BusinessRuleTask {
        decision_id: String,
        result_variable: Option<String>,
    },
    /// A (native/Zeebe) user task. On activation it creates a user task that a
    /// human claims and completes; the token rests until the user task is
    /// completed ([`crate::Command::CompleteUserTask`]). Unlike a service task it
    /// is not activated by a job worker — it is assigned and completed directly
    /// through the user-task API. The fields carry the *raw* (un-evaluated)
    /// assignment, scheduling and priority expressions declared on the BPMN
    /// element; they are resolved against the instance variables (FEEL or literal)
    /// when the task is created.
    UserTask(UserTaskProps),
    /// An exclusive (XOR) gateway: takes exactly one outgoing flow, chosen by
    /// evaluating flow conditions in order (first match wins; an unconditional
    /// flow is the default). Tokens pass through independently — there is no
    /// join synchronisation.
    ExclusiveGateway,
    /// A parallel (AND) gateway. As a split it takes *all* outgoing flows; as a
    /// join (more than one incoming flow) it waits for a token on every incoming
    /// flow before producing one outgoing token.
    ParallelGateway,
    /// An inclusive (OR) gateway. As a split it takes *every* outgoing flow whose
    /// condition holds — an unconditional (non-default) flow is always taken —
    /// falling back to the explicit `default` flow only when no conditional flow
    /// matches (a no-matching-flow incident when there is no default). As a join
    /// (more than one incoming flow) it synchronises exactly as Zeebe does
    /// (#1241): each arriving token re-evaluates readiness, and the join fires
    /// once every incoming flow has been taken, or once no active element or
    /// in-transit token of its scope can still reach it over an untaken flow. It
    /// is never re-evaluated otherwise. A gateway that is both a join and a split
    /// first synchronises, then evaluates its outgoing conditions.
    InclusiveGateway,
    /// An event-based gateway: a *deferred choice* over the intermediate catch
    /// events (timer/message/signal/conditional) that immediately follow it.
    /// On arrival it takes *all* outgoing flows — arming every downstream catch
    /// event at once — and the first event to occur wins: its token continues
    /// while the losing siblings are withdrawn (their timers/subscriptions
    /// cancelled, their tokens consumed). It is a pure routing element with no
    /// join behaviour and no synchronisation.
    EventBasedGateway,
    /// An error boundary event attached to an activity (here, a service task).
    /// It has no incoming sequence flow; instead it is triggered when a job
    /// throws a business error whose code matches `error_code`, interrupting the
    /// activity and routing the token along the boundary event's outgoing flows.
    ErrorBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// The BPMN error code this boundary event catches.
        error_code: String,
    },
    /// A timer intermediate catch event. On activation it arms a timer due
    /// `duration_millis` after the current clock and the token rests on it; the
    /// token resumes along the event's outgoing flow once a clock tick
    /// ([`crate::Command::TriggerTimers`]) finds the timer due. `duration_millis`
    /// is in the same units the host feeds the engine as `now`.
    TimerIntermediateCatchEvent { duration_millis: u64 },
    /// A timer boundary event attached to an activity (here, a service task). It
    /// has no incoming sequence flow; instead a timer is armed when the activity
    /// activates, and when the timer becomes due
    /// ([`crate::Command::TriggerTimers`]) it fires once. An `interrupting` timer
    /// interrupts the activity (its token and any job cancelled) and routes the
    /// token along this event's outgoing flow; a non-interrupting one
    /// (`interrupting == false`) leaves the activity running and spawns a new
    /// parallel token along the outgoing flow instead. `duration_millis` is in
    /// the same units the host feeds the engine as `now`.
    TimerBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// How long after the activity activates the timer fires; for a
        /// `repeating` (cycle) timer this is also the period between fires.
        duration_millis: u64,
        /// Whether firing interrupts the activity (`true`, the default for older
        /// definitions) or spawns a parallel token and leaves it running.
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        interrupting: bool,
        /// Whether the timer re-arms after firing (a BPMN `timeCycle`) or fires
        /// once (a `timeDuration`). Only meaningful for non-interrupting timers —
        /// an interrupting timer cancels its activity on the first fire, so it
        /// never re-arms.
        #[cfg_attr(feature = "serde", serde(default))]
        repeating: bool,
    },
    /// A message intermediate catch event. On activation it opens a message
    /// subscription keyed by `message_name` and a correlation value (the
    /// stringified value of the instance variable named `correlation_key`), and
    /// the token rests on it; the token resumes along the event's outgoing flow
    /// once a [`crate::Command::CorrelateMessage`] with a matching name and
    /// correlation value arrives.
    MessageIntermediateCatchEvent {
        /// The BPMN message name this event subscribes to.
        message_name: String,
        /// Name of the instance variable whose value identifies the instance to
        /// correlate to (the subscription's correlation key).
        correlation_key: String,
    },
    /// A message boundary event attached to an activity (here, a service task).
    /// It has no incoming sequence flow; instead a message subscription is opened
    /// when the activity activates. An `interrupting` boundary, when a matching
    /// message is correlated, interrupts the activity (its token and any job
    /// cancelled) and runs the token along this event's outgoing flow; a
    /// non-interrupting one (`interrupting == false`) leaves the activity running
    /// and spawns a new parallel token along the outgoing flow for every matching
    /// message (its subscription stays open).
    MessageBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// The BPMN message name this event subscribes to.
        message_name: String,
        /// Name of the instance variable whose value identifies the instance to
        /// correlate to (the subscription's correlation key).
        correlation_key: String,
        /// Whether firing interrupts the activity (`true`, the default for older
        /// definitions) or spawns a parallel token and leaves it running.
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        interrupting: bool,
    },
    /// A message start event. It has no incoming sequence flow; instead the
    /// engine opens a process-level subscription at deploy time, and a
    /// [`crate::Command::CorrelateMessage`] with a matching `message_name`
    /// **creates a new instance** (the message's variables become the instance's
    /// variables), which then runs along this event's outgoing flow. Behaves as a
    /// pass-through once an instance starts.
    MessageStartEvent {
        /// The BPMN message name that triggers a new instance.
        message_name: String,
    },
    /// A timer start event. It has no incoming sequence flow; instead the engine
    /// arms a process-level timer at deploy time (first due `interval_millis`
    /// after deployment). When a clock tick ([`crate::Command::TriggerTimers`])
    /// finds it due, a **new instance** is created and runs along this event's
    /// outgoing flow. A `repeating` timer (a BPMN cycle) re-arms for another
    /// `interval_millis`; a one-shot (a BPMN duration) fires exactly once.
    TimerStartEvent {
        /// The delay from deployment to the first fire, and — when `repeating` —
        /// the period between subsequent fires, in the host's clock units.
        interval_millis: u64,
        /// Whether the timer re-arms after firing (a cycle) or fires once.
        repeating: bool,
    },
    /// An embedded sub-process: a container holding its own flow (its inner
    /// elements carry [`Element::parent`] equal to this element's id). On
    /// activation it opens a token scope and activates its inner start event
    /// ([`start_event`]); the sub-process element instance rests while the inner
    /// flow runs. When the inner flow drains (its last inner token is consumed)
    /// the sub-process completes and routes along its outgoing flow. An
    /// interrupting error boundary event attached to it terminates the whole
    /// inner scope and routes along the boundary's outgoing flow instead.
    ///
    /// [`start_event`]: ElementKind::SubProcess::start_event
    SubProcess {
        /// Id of the sub-process's inner (none) start event, where its token
        /// scope begins.
        start_event: ElementId,
    },
    /// An event sub-process (`<subProcess triggeredByEvent="true">`): a
    /// container, like [`SubProcess`], holding its own inner flow — but one
    /// that is **never entered by token flow**. It has no incoming sequence
    /// flow; instead, while its *enclosing* scope (the process instance, or an
    /// embedded sub-process) is active, its start event's [`trigger`] is armed,
    /// and the event's occurrence activates the sub-process.
    ///
    /// Zeebe runtime semantics (the contract the runtime slices implement; this
    /// wave-0 model slice only *represents* the element — deploying a model
    /// containing one succeeds, and until the runtime lands the sub-process is
    /// simply never triggered):
    ///
    /// - **Arming**: when the enclosing scope activates (the process instance
    ///   starts, or the containing embedded sub-process is entered), the start
    ///   event's subscription is opened (message/signal/conditional) or its
    ///   timer armed. It is disarmed when the scope completes or terminates.
    /// - **Interrupting** (`interrupting == true`, the BPMN default): on
    ///   trigger, every other active child of the scope is terminated, the
    ///   scope's other event sub-process subscriptions are disarmed, and the
    ///   event sub-process activates; the scope completes when it completes.
    /// - **Non-interrupting** (`interrupting == false`): the event sub-process
    ///   activates concurrently, repeatedly (a cycle timer re-arms; a
    ///   message/signal subscription stays open), and the scope waits for the
    ///   spawned instances before completing.
    /// - An **error** trigger is always interrupting (Zeebe rejects a
    ///   non-interrupting error event sub-process at deploy).
    ///
    /// The inner flow is wired exactly like an embedded sub-process's: inner
    /// elements carry [`Element::parent`] equal to this element's id and the
    /// inner start event is `start_event`. Use
    /// [`ProcessDefinition::event_subprocesses_in_scope`] to enumerate the
    /// event sub-processes directly contained in a given scope.
    ///
    /// [`trigger`]: ElementKind::EventSubProcess::trigger
    EventSubProcess {
        /// Id of the sub-process's inner (typed) start event, where its token
        /// scope begins when the trigger fires.
        start_event: ElementId,
        /// The start event's trigger — which event activates this sub-process.
        trigger: EventSubProcessTrigger,
        /// Whether firing interrupts the enclosing scope's other work (`true`,
        /// the BPMN default) or spawns a concurrent instance and leaves it
        /// running (`false`). Always `true` for an
        /// [`EventSubProcessTrigger::Error`] trigger.
        interrupting: bool,
    },
    /// An escalation throw event: an `intermediateThrowEvent` or an `endEvent`
    /// carrying an `escalationEventDefinition`. On activation (completion) it
    /// raises the named escalation — walking up the enclosing **embedded /
    /// ad-hoc sub-process** scopes of the *current* process instance for an
    /// [`EscalationBoundaryEvent`](ElementKind::EscalationBoundaryEvent)
    /// whose `escalation_code` matches (an exact code beats a catch-all). A
    /// boundary on a **call activity** is rejected at deploy and catching never
    /// crosses into a called (separate) instance — cross-instance escalation
    /// propagation is out of scope for #1173 — and
    /// then always continues: escalation is **non-critical**, so the throw's own
    /// token routes along its outgoing flow (an `intermediateThrowEvent`) or
    /// drains (an `endEvent`) whether or not the escalation was caught. A
    /// non-interrupting catch spawns a parallel token beside the still-running
    /// activity; an interrupting catch tears the caught activity's scope down.
    /// An uncaught escalation is simply ignored (no incident), matching Zeebe.
    EscalationThrowEvent {
        /// The BPMN escalation code this event raises (resolved from the
        /// `escalationRef`'s `<escalation … escalationCode="…">`). Empty when the
        /// referenced escalation declares no code.
        escalation_code: String,
    },
    /// An escalation boundary event attached to an embedded sub-process. It has
    /// no incoming sequence flow; instead it fires when an
    /// [`EscalationThrowEvent`](ElementKind::EscalationThrowEvent) inside the
    /// attached activity's scope raises an escalation whose code matches
    /// `escalation_code` (an empty `escalation_code` is a **catch-all** that
    /// matches any escalation not claimed by a more specific code at the same
    /// scope). A non-interrupting boundary (`interrupting == false` — the usual
    /// escalation idiom) leaves the activity running and spawns a new parallel
    /// token along its outgoing flow; an interrupting one tears the activity's
    /// scope down and routes the token along its outgoing flow instead.
    ///
    /// Only container activities that open an inner token scope are supported:
    /// an **embedded sub-process** or an **ad-hoc sub-process** container.
    /// Catching resolves by walking the scope tree of the *current* process
    /// instance (see `find_catching_escalation_boundary`), so a boundary on a
    /// **call activity** can never catch a throw raised in the called (separate)
    /// instance. Cross-instance escalation propagation is out of scope for #1173.
    EscalationBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// The BPMN escalation code this boundary event catches; empty is a
        /// catch-all.
        escalation_code: String,
        /// Whether firing interrupts the activity (`true`) or spawns a parallel
        /// token and leaves it running (`false`, the escalation default). BPMN's
        /// `cancelActivity` defaults to interrupting (`true`), and the
        /// parser/builders encode an omitted value as `true`, so decode must
        /// default to `true` as well — a serialized record that omits the field
        /// (e.g. a future `skip_serializing_if`) must restore as interrupting,
        /// not silently flip the control flow to non-interrupting.
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        interrupting: bool,
    },
    /// A none intermediate throw event. A pure pass-through: on activation it
    /// completes immediately and routes the token along its outgoing flow,
    /// exactly like a gateway with a single unconditional outgoing flow. Used
    /// for BPMN `intermediateThrowEvent`s (escalation/signal/none throws that
    /// don't block a forward path) and as the inert demotion target for the
    /// surplus start events of a process that declares more than one.
    IntermediateThrowEvent,
    /// A link intermediate *throw* event: an `intermediateThrowEvent` carrying a
    /// `linkEventDefinition`. Unlike an [`IntermediateThrowEvent`] it has **no**
    /// outgoing sequence flow — instead of routing along a flow, on completion it
    /// hands the token to the matching link *catch* event (a
    /// [`LinkIntermediateCatchEvent`](ElementKind::LinkIntermediateCatchEvent)
    /// with the same `link_name` in the same scope), activating it directly.
    /// Link events are the standard "page-break connector" idiom for keeping a
    /// wide model readable; multiple throws may target one catch. Reference
    /// integrity (every throw has a matching, unique catch) is enforced at deploy
    /// (Zeebe `ModelUtil.verifyLinkIntermediateEvents`).
    LinkIntermediateThrowEvent {
        /// The `linkEventDefinition name` this throw hands its token to.
        link_name: String,
    },
    /// A link intermediate *catch* event: an `intermediateCatchEvent` carrying a
    /// `linkEventDefinition`. It has **no** incoming sequence flow; it is
    /// activated directly by the matching link *throw* event
    /// ([`LinkIntermediateThrowEvent`](ElementKind::LinkIntermediateThrowEvent)
    /// with the same `link_name`). Once activated it is a pure pass-through: it
    /// completes immediately and routes the token along its outgoing flow.
    LinkIntermediateCatchEvent {
        /// The `linkEventDefinition name` a matching throw hands its token to.
        link_name: String,
    },
    /// An abstract BPMN `task` (also `manualTask`): a task with no execution
    /// semantics. Zeebe/C8 accept it and treat it as a **pass-through** — on
    /// activation the token completes immediately and routes along the
    /// element's outgoing flow, exactly like an [`IntermediateThrowEvent`]. No
    /// job is created and no external work is performed; attaching behaviour
    /// requires modelling a concrete task type (service/user/script/…). Kept as
    /// a distinct kind (rather than demoted to `IntermediateThrowEvent`) so the
    /// reversible IR round-trips it back to `<bpmn:task>`.
    Task,
    /// A script task with an inline `zeebe:script` FEEL expression. On
    /// activation the engine evaluates `expression` against the instance
    /// variables (after any input mappings are applied), stores the result
    /// under `result_variable`, and completes immediately — no job is created,
    /// so the token passes straight through like an
    /// [`IntermediateThrowEvent`]. A `scriptTask` that instead declares a
    /// `zeebe:taskDefinition` is job-based and parses to a [`ServiceTask`].
    ScriptTask {
        /// The inline FEEL expression evaluated on activation (as authored,
        /// typically with a leading `=`).
        expression: String,
        /// The variable name the expression's result is stored under.
        result_variable: String,
    },
    /// A call activity: invokes another process (`called_process_id`) as a
    /// distinct **child process instance** and parks its own token until the
    /// child completes (Zeebe/C8 parity). The engine executes this natively — on
    /// activation it spawns a child instance of `called_process_id`, links it back
    /// via `parentProcessInstanceKey`/`parentElementInstanceKey`, and completes
    /// the call-activity token when the child finishes (see the `CallActivity` arm
    /// of `Engine::run_activation_body` and `Engine::complete_call_activity`).
    /// Variables cross the instance boundary through the call activity's input and
    /// output mappings (isolated scopes). A legacy **inline-expansion** mode is
    /// still available for callers that opt in — [`ProcessDefinition::inline_call_activities`]
    /// rewrites each call activity into an embedded [`SubProcess`] holding a copy
    /// of the callee's flow (embedded-subprocess semantics, no child instance).
    CallActivity {
        /// The `calledElement` / `zeebe:calledElement processId` of the invoked
        /// process definition.
        called_process_id: String,
        /// Zeebe `propagateAllParentVariables` (`zeebe:calledElement`). When
        /// `true` (the Zeebe default when the attribute is absent), **all**
        /// variables visible in the call activity's scope are copied into the
        /// child instance at spawn; when `false`, only the call activity's local
        /// variables — the results of its input mappings — cross the boundary.
        ///
        /// Defaults to `true` when absent from a persisted snapshot: this field
        /// was added after the CallActivity variant first shipped (#1057), so a
        /// pre-#1057 snapshot lacks it. Deserializing to `true` matches both the
        /// Zeebe default and what a fresh redeploy of the same BPMN produces
        /// (the parser defaults the absent attribute to `true`), keeping the
        /// change additive-safe for migration-by-replay.
        #[cfg_attr(feature = "serde", serde(default = "default_propagate"))]
        propagate_all_parent_variables: bool,
        /// Zeebe `propagateAllChildVariables` (`zeebe:calledElement`). When
        /// `true` (the Zeebe default when the attribute is absent), the child's
        /// final variables are merged back into the parent scope on completion
        /// (the child's value wins on a name collision); when `false`, only the
        /// call activity's output mappings cross back (nothing crosses if there
        /// are none).
        ///
        /// Defaults to `true` when absent from a persisted snapshot — same
        /// forward-compat rationale as `propagate_all_parent_variables`.
        #[cfg_attr(feature = "serde", serde(default = "default_propagate"))]
        propagate_all_child_variables: bool,
    },
    /// A signal intermediate catch event. On activation it opens a signal
    /// subscription keyed by `signal_name` and the token rests on it; the token
    /// resumes along the event's outgoing flow once a
    /// [`crate::Command::BroadcastSignal`] with a matching name is broadcast.
    /// Signals correlate by **name only** (there is no correlation key), and a
    /// single broadcast fans out to every matching open subscription.
    SignalIntermediateCatchEvent {
        /// The BPMN signal name this event subscribes to.
        signal_name: String,
    },
    /// A signal boundary event attached to an activity. It has no incoming
    /// sequence flow; instead a signal subscription is opened when the activity
    /// activates. An `interrupting` boundary, when a matching signal is
    /// broadcast, interrupts the activity (its token and any job cancelled) and
    /// runs the token along this event's outgoing flow; a non-interrupting one
    /// (`interrupting == false`) leaves the activity running and spawns a new
    /// parallel token along the outgoing flow for every matching signal.
    SignalBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// The BPMN signal name this event subscribes to.
        signal_name: String,
        /// Whether firing interrupts the activity (`true`, the default) or spawns
        /// a parallel token and leaves it running.
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        interrupting: bool,
    },
    /// A conditional intermediate catch event (`conditionalEventDefinition`). On
    /// activation it evaluates its FEEL `condition`; if already `true` the token
    /// continues immediately, otherwise a conditional subscription is opened and
    /// the token rests until a change to a referenced variable makes the
    /// condition `true`. It is always interrupting (a pure wait-until point).
    ConditionalIntermediateCatchEvent {
        /// The FEEL boolean condition (as authored, typically `=`-prefixed).
        condition: String,
    },
    /// A conditional boundary event attached to an activity. It has no incoming
    /// sequence flow; a conditional subscription is opened when the activity
    /// activates and its `condition` is evaluated immediately (and again whenever
    /// a referenced variable changes while the activity is active). An
    /// `interrupting` boundary interrupts the activity and runs the token along
    /// its outgoing flow when the condition becomes `true`; a non-interrupting
    /// one leaves the activity running and spawns a new parallel token each time
    /// the condition becomes `true`.
    ConditionalBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// The FEEL boolean condition (as authored, typically `=`-prefixed).
        condition: String,
        /// Whether firing interrupts the activity (`true`, the default) or spawns
        /// a parallel token and leaves it running.
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        interrupting: bool,
    },
    /// A compensation boundary event attached to an activity. Unlike the other
    /// boundary events it is **not** armed and never fires reactively during the
    /// activity's execution: it is a structural marker recording that, once
    /// `attached_to` completes successfully, the activity becomes *compensable*
    /// and its compensation is performed by running the `handler` activity (a
    /// task flagged `isForCompensation="true"`, wired to this boundary event via
    /// a BPMN `<association>`). Compensation is triggered later by a
    /// [`CompensationThrowEvent`](ElementKind::CompensationThrowEvent). It has no
    /// incoming or outgoing sequence flow.
    CompensationBoundaryEvent {
        /// Id of the activity this boundary event is attached to.
        attached_to: ElementId,
        /// Id of the compensation handler activity run to compensate
        /// `attached_to` (resolved from the `<association>` linking this boundary
        /// event to the `isForCompensation` handler).
        handler: ElementId,
    },
    /// A compensation throw event: a `compensateEventDefinition` on an
    /// `intermediateThrowEvent` or an `endEvent`. On activation it triggers
    /// compensation of the already-completed compensable activities in its scope
    /// (those carrying a [`CompensationBoundaryEvent`](ElementKind::CompensationBoundaryEvent)),
    /// running each one's compensation handler, in reverse order of completion.
    /// The throw's token rests until every triggered handler completes, then it
    /// completes and routes along its outgoing flow(s) (an `intermediateThrowEvent`)
    /// or drains (an `endEvent`). With no compensable activity in scope it is an
    /// immediate pass-through. (Single-activity path first: broader semantics —
    /// compensating a whole sub-process, `cancelRemainingInstances`, strictly
    /// ordered nested multi-activity compensation — are follow-ups.)
    CompensationThrowEvent,
    /// Legacy persisted model shape, retained so old deployment events and
    /// snapshots remain readable. New models use `ServiceTask.agent_type`.
    /// [`ProcessDefinition::normalize_legacy_agent_tasks`] lowers this shape to
    /// the canonical job-worker model before execution. Existing AgentInstance
    /// records retain their configuration; new registrations supply it at CREATE.
    AgentTask {
        /// The `agentType` marker this element was built from.
        agent_type: crate::agent::AgentType,
        /// Optional `zeebe:taskDefinition` type (literal or FEEL expression).
        /// Normalization retains this route, defaulting to the element id when
        /// absent from a historical deployment.
        #[cfg_attr(feature = "serde", serde(default))]
        job_type: Option<String>,
        /// The static agent definition (model/provider/systemPrompt).
        #[cfg_attr(feature = "serde", serde(default))]
        definition: crate::agent::AgentDefinition,
        /// The configured limits, if declared (`None` = all unlimited).
        #[cfg_attr(feature = "serde", serde(default))]
        limits: Option<crate::agent::AgentInstanceLimits>,
    },
}

impl ElementKind {
    /// Whether this is a start-event kind (none, message or timer start). A
    /// process has exactly one process-entry start ([`ProcessDefinition::start_event`],
    /// where a CreateInstance begins) but may declare several process-level starts
    /// — a none start alongside any number of message/timer starts, each wired to
    /// its own deploy-time trigger (#855).
    pub fn is_start_event(&self) -> bool {
        matches!(
            self,
            ElementKind::StartEvent
                | ElementKind::MessageStartEvent { .. }
                | ElementKind::TimerStartEvent { .. }
        )
    }

    /// The activity a boundary event is attached to, or `None` if this kind is
    /// not a boundary event. Exhaustive so a new boundary kind is caught at
    /// compile time.
    pub fn attached_to(&self) -> Option<&str> {
        match self {
            ElementKind::ErrorBoundaryEvent { attached_to, .. }
            | ElementKind::TimerBoundaryEvent { attached_to, .. }
            | ElementKind::MessageBoundaryEvent { attached_to, .. }
            | ElementKind::SignalBoundaryEvent { attached_to, .. }
            | ElementKind::ConditionalBoundaryEvent { attached_to, .. }
            | ElementKind::EscalationBoundaryEvent { attached_to, .. }
            | ElementKind::CompensationBoundaryEvent { attached_to, .. } => Some(attached_to),
            ElementKind::StartEvent
            | ElementKind::EndEvent
            | ElementKind::TerminateEndEvent
            | ElementKind::ServiceTask { .. }
            | ElementKind::BusinessRuleTask { .. }
            | ElementKind::UserTask(_)
            | ElementKind::ExclusiveGateway
            | ElementKind::ParallelGateway
            | ElementKind::InclusiveGateway
            | ElementKind::EventBasedGateway
            | ElementKind::TimerIntermediateCatchEvent { .. }
            | ElementKind::MessageIntermediateCatchEvent { .. }
            | ElementKind::MessageStartEvent { .. }
            | ElementKind::TimerStartEvent { .. }
            | ElementKind::SubProcess { .. }
            | ElementKind::EventSubProcess { .. }
            | ElementKind::IntermediateThrowEvent
            | ElementKind::EscalationThrowEvent { .. }
            | ElementKind::CompensationThrowEvent
            | ElementKind::Task
            | ElementKind::ScriptTask { .. }
            | ElementKind::CallActivity { .. }
            | ElementKind::SignalIntermediateCatchEvent { .. }
            | ElementKind::LinkIntermediateThrowEvent { .. }
            | ElementKind::LinkIntermediateCatchEvent { .. }
            | ElementKind::ConditionalIntermediateCatchEvent { .. }
            | ElementKind::AgentTask { .. } => None,
        }
    }

    /// Whether this element is an *activity* (task / sub-process / call
    /// activity) — the only category a BPMN boundary event's `attachedToRef`
    /// may legally resolve against. Gateways and events are not activities.
    /// Kept exhaustive so a new [`ElementKind`] must be classified here rather
    /// than silently defaulting into (or out of) the activity set.
    pub fn is_activity(&self) -> bool {
        match self {
            ElementKind::ServiceTask { .. }
            | ElementKind::BusinessRuleTask { .. }
            | ElementKind::UserTask(_)
            | ElementKind::ScriptTask { .. }
            | ElementKind::Task
            | ElementKind::AgentTask { .. }
            | ElementKind::SubProcess { .. }
            | ElementKind::EventSubProcess { .. }
            | ElementKind::CallActivity { .. } => true,
            ElementKind::StartEvent
            | ElementKind::EndEvent
            | ElementKind::TerminateEndEvent
            | ElementKind::MessageStartEvent { .. }
            | ElementKind::TimerStartEvent { .. }
            | ElementKind::IntermediateThrowEvent
            | ElementKind::LinkIntermediateThrowEvent { .. }
            | ElementKind::LinkIntermediateCatchEvent { .. }
            | ElementKind::TimerIntermediateCatchEvent { .. }
            | ElementKind::MessageIntermediateCatchEvent { .. }
            | ElementKind::SignalIntermediateCatchEvent { .. }
            | ElementKind::ConditionalIntermediateCatchEvent { .. }
            | ElementKind::ErrorBoundaryEvent { .. }
            | ElementKind::EscalationBoundaryEvent { .. }
            | ElementKind::TimerBoundaryEvent { .. }
            | ElementKind::MessageBoundaryEvent { .. }
            | ElementKind::SignalBoundaryEvent { .. }
            | ElementKind::ConditionalBoundaryEvent { .. }
            | ElementKind::CompensationBoundaryEvent { .. }
            | ElementKind::CompensationThrowEvent
            | ElementKind::EscalationThrowEvent { .. }
            | ElementKind::ExclusiveGateway
            | ElementKind::ParallelGateway
            | ElementKind::InclusiveGateway
            | ElementKind::EventBasedGateway => false,
        }
    }

    /// Maps this element kind to the Camunda 8 element-instance `type` enum value
    /// (`ElementInstanceResult.type` / the search filter's `type`). Boundary and
    /// intermediate-catch variants collapse to the single BPMN category the REST
    /// API exposes; message/timer *start* events are `START_EVENT`. Kept exhaustive
    /// so a new [`ElementKind`] must be classified here rather than silently
    /// defaulting.
    pub fn type_name(&self) -> &'static str {
        match self {
            ElementKind::StartEvent
            | ElementKind::MessageStartEvent { .. }
            | ElementKind::TimerStartEvent { .. } => "START_EVENT",
            ElementKind::EndEvent => "END_EVENT",
            ElementKind::TerminateEndEvent => "END_EVENT",
            ElementKind::IntermediateThrowEvent => "INTERMEDIATE_THROW_EVENT",
            ElementKind::LinkIntermediateThrowEvent { .. } => "INTERMEDIATE_THROW_EVENT",
            ElementKind::CompensationThrowEvent => "INTERMEDIATE_THROW_EVENT",
            ElementKind::EscalationThrowEvent { .. } => "INTERMEDIATE_THROW_EVENT",
            ElementKind::Task => "TASK",
            ElementKind::TimerIntermediateCatchEvent { .. }
            | ElementKind::MessageIntermediateCatchEvent { .. }
            | ElementKind::SignalIntermediateCatchEvent { .. }
            | ElementKind::LinkIntermediateCatchEvent { .. }
            | ElementKind::ConditionalIntermediateCatchEvent { .. } => "INTERMEDIATE_CATCH_EVENT",
            ElementKind::ErrorBoundaryEvent { .. }
            | ElementKind::TimerBoundaryEvent { .. }
            | ElementKind::MessageBoundaryEvent { .. }
            | ElementKind::SignalBoundaryEvent { .. }
            | ElementKind::ConditionalBoundaryEvent { .. } => "BOUNDARY_EVENT",
            ElementKind::EscalationBoundaryEvent { .. } => "BOUNDARY_EVENT",
            ElementKind::CompensationBoundaryEvent { .. } => "BOUNDARY_EVENT",
            ElementKind::ServiceTask { .. } => "SERVICE_TASK",
            ElementKind::AgentTask { .. } => "SERVICE_TASK",
            ElementKind::BusinessRuleTask { .. } => "BUSINESS_RULE_TASK",
            ElementKind::ScriptTask { .. } => "SCRIPT_TASK",
            ElementKind::UserTask(_) => "USER_TASK",
            ElementKind::ExclusiveGateway => "EXCLUSIVE_GATEWAY",
            ElementKind::ParallelGateway => "PARALLEL_GATEWAY",
            ElementKind::InclusiveGateway => "INCLUSIVE_GATEWAY",
            ElementKind::EventBasedGateway => "EVENT_BASED_GATEWAY",
            ElementKind::SubProcess { .. } => "SUB_PROCESS",
            ElementKind::EventSubProcess { .. } => "SUB_PROCESS",
            ElementKind::CallActivity { .. } => "CALL_ACTIVITY",
        }
    }
}

/// A single BPMN flow node and its outgoing sequence flows.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Element {
    pub id: ElementId,
    pub kind: ElementKind,
    /// The element's BPMN `name` attribute, if present. Purely descriptive — the
    /// engine never dispatches on it — but carried so read models (the
    /// element-instance search API's `elementName`) can surface the modeller's
    /// label. `None` (the default) for elements with no `name` and for
    /// definitions serialized before this field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub name: Option<String>,
    /// Outgoing sequence flows, in declaration (document) order. That order
    /// fixes each flow's index and its [`IncomingFlow::ordinal`], which journals
    /// persist, so it must never change. The engine **executes** the flows in
    /// [`Element::outgoing_in_execution_order`], never by iterating this field.
    pub outgoing: Vec<SequenceFlow>,
    /// Id of the embedded sub-process that contains this element, or `None` for
    /// elements at the process level. Used to scope tokens and to pick the
    /// process-level start event. Defaulted to `None` so models serialized
    /// before sub-processes existed still load.
    #[cfg_attr(feature = "serde", serde(default))]
    pub parent: Option<ElementId>,
    /// The element's `zeebe:ioMapping` (input mappings applied on activation,
    /// output mappings applied on completion). Empty (the default) for elements
    /// without mappings and when deserializing definitions written before this
    /// field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub io: IoMapping,
    /// A FEEL timer expression on a timer event, evaluated at timer creation
    /// against the instance variables (or an empty context for a start timer).
    /// `None` (the default) for non-timer elements and for timers whose
    /// definition is a static ISO-8601 literal parsed at deploy time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub timer: Option<TimerDef>,
    /// The raw `zeebe:taskDefinition` `retries` expression on a job-based task
    /// (e.g. a service task), evaluated to a number at job creation against the
    /// instance variables. A literal (`"5"`) is used directly; a FEEL expression
    /// (`"=maxRetries"`) is evaluated. `None` (the default) means no declaration
    /// and the job starts with [`crate::state::DEFAULT_JOB_RETRIES`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub retries: Option<String>,
    /// Multi-instance loop characteristics on this activity, or `None` (the
    /// default) for an ordinary single-instance element and for definitions
    /// serialized before multi-instance support existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub multi_instance: Option<MultiInstance>,
    /// Execution listeners that fire on this element's `ACTIVATING -> ACTIVATED`
    /// transition, in declaration order. Empty (the default) for elements
    /// without listeners and for definitions serialized before listeners
    /// existed, so listener-free models run on the unchanged hot path (ADR 0037).
    #[cfg_attr(feature = "serde", serde(default))]
    pub start_listeners: Vec<ExecutionListener>,
    /// Execution listeners that fire on this element's `COMPLETING -> COMPLETED`
    /// transition, in declaration order. Empty (the default) for elements
    /// without listeners.
    #[cfg_attr(feature = "serde", serde(default))]
    pub end_listeners: Vec<ExecutionListener>,
    /// Task listeners on this user task, in declaration order (all event types
    /// share one vector; `event_type` discriminates). Empty (the default) for
    /// non-user-task elements, user tasks without listeners, and definitions
    /// serialized before task listeners existed, so listener-free user tasks run
    /// on the unchanged hot path (ADR 0037 §6).
    #[cfg_attr(feature = "serde", serde(default))]
    pub task_listeners: Vec<TaskListener>,
}

impl Element {
    /// This element's outgoing flows as `(declaration index, flow)` in the order
    /// the engine takes and evaluates them, which is **reverse document order**.
    ///
    /// Zeebe builds `ExecutableFlowNode.getOutgoing()` while its `ModelWalker`
    /// visits the model depth-first, pushing each element's children with
    /// `addFirst`. Sibling `sequenceFlow`s are therefore transformed, and
    /// appended to their source's outgoing list, last-declared first. Every
    /// consumer iterates that list:
    /// - a parallel or inclusive fork takes its flows in this order
    ///   (`BpmnStateTransitionBehavior.takeOutgoingSequenceFlows`);
    /// - an exclusive gateway takes the *first* matching flow in this order,
    ///   so of two matching conditions the one declared **last** wins
    ///   (`ExclusiveGatewayProcessor.findSequenceFlowToTake`).
    ///
    /// This is the single source of that order (#1244). The
    /// `formal/traces` scenarios `parallel-fork-join-tasks` and
    /// `exclusive-gateway-first-match` pin it against Zeebe.
    pub fn outgoing_in_execution_order(
        &self,
    ) -> impl DoubleEndedIterator<Item = (usize, &SequenceFlow)> + ExactSizeIterator {
        self.outgoing.iter().enumerate().rev()
    }
}

/// The BPMN ad-hoc sub-process implementation type (Camunda `zeebe:adHoc`).
///
/// `JobWorker` is the agentic path: the container is backed by a job worker (the
/// AI Agent connector) that decides which inner elements to activate and returns
/// them in the job result. `BpmnTask` is the declarative path: the elements to
/// activate come from a FEEL `activeElementsCollection`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AdHocImplementationType {
    /// Agentic: a job worker returns the elements to activate (default).
    #[default]
    JobWorker,
    /// Declarative: a FEEL `activeElementsCollection` names the elements.
    BpmnTask,
}

/// The kind of an ad-hoc "tool" (an inner activatable element of an
/// [`AdHocSubProcessDef`]), captured so the container's tool catalog is
/// self-describing without re-reading the pruned elements.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AdHocToolKind {
    /// A job-based task; `job_type` is its resolved `zeebe:taskDefinition` type.
    ServiceTask { job_type: String },
    /// A native user task (human-in-the-loop tool). Carries the tool's
    /// `zeebe:assignmentDefinition`/`taskSchedule`/`priorityDefinition`
    /// expressions so activating it can create a real user task even though the
    /// tool element is pruned from the executable graph (ADR 0023 seam 4, mirror
    /// of the retained `io` mapping).
    UserTask(UserTaskProps),
    /// A call activity tool (issue #1159): activating it spawns a distinct child
    /// process instance of `process_id` — exactly like a call activity on an
    /// ordinary sequence flow — rather than passing straight through to
    /// completion. `process_id` is the invoked process id (a literal or a FEEL
    /// `=` expression), if declared. `propagate_all_parent_variables` /
    /// `propagate_all_child_variables` carry the tool's
    /// `zeebe:calledElement propagateAllParentVariables` /
    /// `propagateAllChildVariables` flags (Zeebe default `true`) so the
    /// parent→child seed and child→parent merge honour the model even though the
    /// tool element is pruned from the executable graph.
    CallActivity {
        process_id: Option<String>,
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        propagate_all_parent_variables: bool,
        #[cfg_attr(feature = "serde", serde(default = "default_true"))]
        propagate_all_child_variables: bool,
    },
    /// An embedded `bpmn:subProcess` tool with a multi-element token-flow body
    /// (ADR 0023 §Subset, issue #872). `start_event` is the id of the body's
    /// inner start event, where a token is injected when the tool is activated;
    /// the tool completes only when its body reaches its end event, feeding the
    /// agent loop like any other tool. Unlike the other kinds, a subProcess
    /// tool's body is *retained* in the executable graph (not pruned to a
    /// catalog entry), so it runs by ordinary token flow within the tool's
    /// scope (tasks, gateways, boundary events, nested scopes).
    SubProcess { start_event: ElementId },
    /// Any other element kind usable as a tool.
    Other,
}

/// A `bpmn:sequenceFlow` between two DIRECT children of an ad-hoc container
/// (issue #1154). Camunda documents that inner elements of an ad-hoc
/// sub-process "can be connected by a sequence flow to build a structured
/// sequence": when the source element completes, the flow is taken and the
/// target element runs, and only once that chain drains does the container
/// re-emit its agent job. These flows reference elements that are pruned from
/// the executable graph, so — like the tool catalog — they are captured here at
/// parse time and driven out-of-band by the ad-hoc runtime seam rather than by
/// ordinary token flow. Flows *inside* an embedded-`subProcess` tool's body are
/// NOT captured here (they run by ordinary token flow); only flows whose source
/// and target are both direct children of the container are.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdHocInnerFlow {
    /// The source element id (a direct child of the container).
    pub from: ElementId,
    /// The target element id (a direct child of the container).
    pub to: ElementId,
    /// The flow's optional `conditionExpression` guard, evaluated in the source
    /// tool's completed scope. `None` is an unconditional flow.
    #[cfg_attr(feature = "serde", serde(default))]
    pub condition: Option<Condition>,
}

/// A single activatable inner element ("tool") of an ad-hoc sub-process.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdHocTool {
    pub element_id: ElementId,
    /// The tool's BPMN `name` attribute (empty if none). Retained on the catalog
    /// (like `io`) because the tool element is pruned from the executable graph;
    /// surfaced to the agent in the advertised `adHocSubProcessElements` catalog
    /// as `elementName` so the agent can present a human-readable tool list.
    #[cfg_attr(feature = "serde", serde(default))]
    pub name: String,
    pub kind: AdHocToolKind,
    /// The tool's own `zeebe:ioMapping` (empty if none). Retained on the catalog
    /// because the tool element is pruned from the executable graph, so the
    /// runtime cannot read it back via `element(id)` (ADR 0023 seam 4): input
    /// mappings apply on activation, output mappings on completion.
    #[cfg_attr(feature = "serde", serde(default))]
    pub io: IoMapping,
}

/// The retained metadata + tool catalog of one `adHocSubProcess`.
///
/// Nano keeps the container itself as a single job-bearing activity in the
/// executable graph. Leaf tools (service / user tasks, call activities) are
/// pruned to this catalog and activated out-of-band by the agent loop; an
/// embedded-`subProcess` tool is instead RETAINED in the executable graph and
/// runs its body by ordinary token flow when activated (#872), but is still
/// listed here (kind [`AdHocToolKind::SubProcess`]) so the agent can activate it
/// by id. The catalog preserves what the inner elements were, plus the
/// `zeebe:adHoc` wiring, so the Camunda agentic contract (ADR 0023:
/// activate-element execution) is honoured without re-parsing.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdHocSubProcessDef {
    /// Id of the `adHocSubProcess` element these tools belong to.
    pub container_id: ElementId,
    #[cfg_attr(feature = "serde", serde(default))]
    pub impl_type: AdHocImplementationType,
    /// Raw (un-evaluated) `<completionCondition>` FEEL text, if declared.
    #[cfg_attr(feature = "serde", serde(default))]
    pub completion_condition: Option<String>,
    /// Raw `zeebe:adHoc activeElementsCollection` FEEL expression (declarative
    /// `BpmnTask` variant), if declared.
    #[cfg_attr(feature = "serde", serde(default))]
    pub active_elements_collection: Option<String>,
    /// Raw `zeebe:adHoc outputCollection` result-variable name, if declared.
    #[cfg_attr(feature = "serde", serde(default))]
    pub output_collection: Option<String>,
    /// Raw `zeebe:adHoc outputElement` FEEL expression mapping each tool result
    /// into the output collection, if declared.
    #[cfg_attr(feature = "serde", serde(default))]
    pub output_element: Option<String>,
    /// The BPMN `cancelRemainingInstances` attribute (default `true`). When the
    /// declared `<completionCondition>` is fulfilled: `true` cancels any tools
    /// still running and completes the container immediately; `false` defers
    /// completion until no active children/flows remain (Zeebe
    /// `BpmnAdHocSubProcessBehavior#completionConditionFulfilled`).
    #[cfg_attr(feature = "serde", serde(default = "default_true"))]
    pub cancel_remaining_instances: bool,
    /// The inner activatable elements, in document order.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tools: Vec<AdHocTool>,
    /// The `bpmn:sequenceFlow`s between the container's DIRECT children (issue
    /// #1154), in document order. Captured before the inner elements are pruned
    /// so the runtime can drive a "structured sequence" (Camunda) — take a
    /// completed tool's outgoing flow and run its follow-up — instead of
    /// silently dropping it. Empty when the container's tools are unconnected.
    #[cfg_attr(feature = "serde", serde(default))]
    pub inner_flows: Vec<AdHocInnerFlow>,
}

/// The result a JOB_WORKER ad-hoc sub-process agent returns when it completes
/// the container job (Camunda's `JobResult` for `adHocSubProcess`,
/// `JobResult.java`). All fields are optional/empty for an ordinary
/// (non-agentic) job completion, so plain completions are byte-unchanged.
///
/// This is transport-plumbed and acted on by the engine (ADR 0023 seam 3): its
/// `activate_elements` drive the ad-hoc container's tool activations in the
/// runtime seam.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdHocJobResult {
    /// Inner elements the agent asks the engine to activate this turn
    /// (Camunda `activateElements[]`), in the order the agent returned them.
    #[cfg_attr(feature = "serde", serde(default))]
    pub activate_elements: Vec<AdHocActivateElement>,
    /// The agent's assertion that the container's `<completionCondition>` is
    /// satisfied (Camunda `isCompletionConditionFulfilled`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub completion_condition_fulfilled: bool,
    /// Ask the engine to cancel any still-active inner elements (Camunda
    /// `isCancelRemainingInstances`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub cancel_remaining_instances: bool,
}

impl AdHocJobResult {
    /// True when the result carries no agentic instruction — i.e. an ordinary
    /// job completion that happens to have been decoded through the ad-hoc
    /// result shape. Used to keep non-agentic paths free of behaviour changes.
    pub fn is_empty(&self) -> bool {
        self.activate_elements.is_empty()
            && !self.completion_condition_fulfilled
            && !self.cancel_remaining_instances
    }
}

/// One element-activation instruction inside an [`AdHocJobResult`] (Camunda
/// `AdHocSubProcessActivateElementInstruction`: an `elementId` plus local
/// variables to seed into the activated element's scope).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdHocActivateElement {
    /// Id of the inner element (tool) to activate.
    pub element_id: ElementId,
    /// Local variables to seed into the activated element's scope.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variables: HashMap<String, Value>,
}

/// Corrections a task listener may return to user-task data when it completes
/// its job (ADR 0037 §6, Zeebe `JobResult` corrections). Each field is `Some`
/// only when that attribute was corrected; corrections from successive
/// listeners in a chain merge (a later listener overrides an earlier one) and
/// are applied to the user task when the chain drains and the transition
/// commits. An empty list or empty-string date *clears* the attribute.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserTaskCorrections {
    /// Corrected assignee (empty string clears it).
    #[cfg_attr(feature = "serde", serde(default))]
    pub assignee: Option<String>,
    /// Corrected candidate groups.
    #[cfg_attr(feature = "serde", serde(default))]
    pub candidate_groups: Option<Vec<String>>,
    /// Corrected candidate users.
    #[cfg_attr(feature = "serde", serde(default))]
    pub candidate_users: Option<Vec<String>>,
    /// Corrected due date (empty string clears it).
    #[cfg_attr(feature = "serde", serde(default))]
    pub due_date: Option<String>,
    /// Corrected follow-up date (empty string clears it).
    #[cfg_attr(feature = "serde", serde(default))]
    pub follow_up_date: Option<String>,
    /// Corrected priority (0..=100).
    #[cfg_attr(feature = "serde", serde(default))]
    pub priority: Option<i32>,
}

impl UserTaskCorrections {
    /// Returns `true` when no attribute is corrected.
    pub fn is_empty(&self) -> bool {
        self.assignee.is_none()
            && self.candidate_groups.is_none()
            && self.candidate_users.is_none()
            && self.due_date.is_none()
            && self.follow_up_date.is_none()
            && self.priority.is_none()
    }

    /// Merges `other` into `self`, letting each `Some` field in `other` override.
    pub fn merge(&mut self, other: &UserTaskCorrections) {
        if other.assignee.is_some() {
            self.assignee = other.assignee.clone();
        }
        if other.candidate_groups.is_some() {
            self.candidate_groups = other.candidate_groups.clone();
        }
        if other.candidate_users.is_some() {
            self.candidate_users = other.candidate_users.clone();
        }
        if other.due_date.is_some() {
            self.due_date = other.due_date.clone();
        }
        if other.follow_up_date.is_some() {
            self.follow_up_date = other.follow_up_date.clone();
        }
        if other.priority.is_some() {
            self.priority = other.priority;
        }
    }
}

/// The result a worker returns when completing a *task-listener* job (ADR 0037
/// §6, Zeebe `JobResult` for user-task listeners). All fields default to a
/// non-denying, correction-free result so an ordinary completion is
/// byte-unchanged. `denied` is honoured only for the `assigning`, `updating`
/// and `completing` events (Zeebe parity); `corrections` are mutually exclusive
/// with `denied`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TaskListenerJobResult {
    /// The listener denies the transition (it must not proceed).
    #[cfg_attr(feature = "serde", serde(default))]
    pub denied: bool,
    /// Human-readable reason surfaced when `denied` is true.
    #[cfg_attr(feature = "serde", serde(default))]
    pub denied_reason: Option<String>,
    /// Corrections to user-task data (assignee, candidates, dates, priority).
    #[cfg_attr(feature = "serde", serde(default))]
    pub corrections: UserTaskCorrections,
}

impl TaskListenerJobResult {
    /// True when the result carries no denial and no corrections — an ordinary
    /// task-listener job completion.
    pub fn is_empty(&self) -> bool {
        !self.denied && self.denied_reason.is_none() && self.corrections.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessDefinition {
    pub id: String,
    /// The process's BPMN `name` attribute (the modeller's human-readable
    /// label, distinct from the executable `id`), if present. Non-executable
    /// metadata: read models surface it as the process definition `name`
    /// (Camunda/Zeebe semantics, where `name` and `processDefinitionId` are
    /// independent), and the search API's `name` filter matches against it.
    /// `None` when the `<bpmn:process>` carries no `name`, and defaulted `None`
    /// when deserializing journals/snapshots written before this field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub name: Option<String>,
    pub elements: HashMap<ElementId, Element>,
    pub start_event: ElementId,
    /// The original BPMN XML this definition was parsed from, retained verbatim
    /// so it can be served back (e.g. Camunda's `getProcessDefinitionXML`, the
    /// console's diagram view). Empty for definitions built programmatically via
    /// [`ProcessBuilder`] rather than parsed from XML, and defaulted empty when
    /// deserializing journals/snapshots written before this field existed. It is
    /// non-executable metadata: it travels with the deployment (journaled and
    /// snapshotted) but no engine logic reads it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub xml: String,
    /// Retained ad-hoc sub-process tool catalogs, one per `adHocSubProcess`.
    /// Non-executable metadata today (see [`AdHocSubProcessDef`]); defaulted
    /// empty when absent or when deserializing definitions written before this
    /// field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub adhoc: Vec<AdHocSubProcessDef>,
    /// The `zeebe:formDefinition formId` declared on the process's start event,
    /// referencing a deployed form by id — the process's *start form* (Camunda
    /// `GetStartProcessForm`). Resolved to the latest deployed form's key at
    /// query time. `None` when the start event declares no form. Defaulted
    /// absent for definitions written before start forms were parsed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub start_form_id: Option<String>,
}

impl ProcessDefinition {
    /// Validates every [`ElementKind::EventSubProcess`] structurally (no
    /// sequence flow in or out, `start_event` is a start event directly
    /// contained in it, error trigger interrupting). [`ProcessBuilder::build`]
    /// runs this; callers that assemble a `ProcessDefinition` directly (e.g.
    /// an IR parser) must run it too, so every path shares one check.
    pub fn validate_event_subprocesses(&self) -> Result<(), BuildError> {
        validate_event_subprocesses(&self.elements)
    }

    /// Enumerates the event sub-processes ([`ElementKind::EventSubProcess`])
    /// **directly contained** in `scope`: pass `None` for the process-level
    /// scope, or `Some(id)` for the embedded sub-process whose id is `id`.
    ///
    /// This is the seam the event sub-process *runtime* consumes (#1555): when
    /// a scope activates, the engine arms the start-event [`trigger`] of every
    /// event sub-process this iterator yields for that scope (and disarms them
    /// when the scope completes or terminates). Yields
    /// `(element id, start event id, trigger, interrupting)` tuples in
    /// deterministic (element-id) order so key minting and replay stay in
    /// lock-step. Only *direct* containment is reported — an event sub-process
    /// nested inside an embedded sub-process belongs to that sub-process's
    /// scope, not the process's, and is enumerated under
    /// `Some(containing_sub_process_id)`.
    ///
    /// [`trigger`]: ElementKind::EventSubProcess::trigger
    pub fn event_subprocesses_in_scope(
        &self,
        scope: Option<&str>,
    ) -> impl Iterator<Item = (&ElementId, &ElementId, &EventSubProcessTrigger, bool)> {
        let mut matched: Vec<(&ElementId, &ElementId, &EventSubProcessTrigger, bool)> = self
            .elements
            .values()
            .filter(|e| e.parent.as_deref() == scope)
            .filter_map(|e| match &e.kind {
                ElementKind::EventSubProcess {
                    start_event,
                    trigger,
                    interrupting,
                } => Some((&e.id, start_event, trigger, *interrupting)),
                _ => None,
            })
            .collect();
        matched.sort_by(|a, b| a.0.cmp(b.0));
        matched.into_iter()
    }

    /// Upgrades the historical agent-only model shape without changing event
    /// frames or minting runtime state. Called for legacy deployments and snapshots.
    pub fn normalize_legacy_agent_tasks(&mut self) {
        for element in self.elements.values_mut() {
            if let ElementKind::AgentTask {
                agent_type,
                job_type,
                ..
            } = &element.kind
            {
                element.kind = ElementKind::ServiceTask {
                    job_type: job_type.clone().unwrap_or_else(|| element.id.clone()),
                    priority: None,
                    agent_type: Some(*agent_type),
                    custom_headers: BTreeMap::new(),
                    linked_resources: Vec::new(),
                };
            }
        }
    }

    /// Looks up an element by id.
    pub fn element(&self, id: &str) -> Option<&Element> {
        self.elements.get(id)
    }

    /// Number of sequence flows across the whole process that target `id`. Used
    /// to detect parallel-gateway joins.
    pub fn incoming_count(&self, id: &str) -> usize {
        self.elements
            .values()
            .flat_map(|e| e.outgoing.iter())
            .filter(|f| f.to == id)
            .count()
    }

    /// The identity of `from`'s `index`-th outgoing flow as seen by its target:
    /// the target element id and the flow's [`IncomingFlow`]. `None` when `from`
    /// has no such flow.
    pub fn incoming_flow(&self, from: &str, index: usize) -> Option<(ElementId, IncomingFlow)> {
        let outgoing = &self.element(from)?.outgoing;
        let to = &outgoing.get(index)?.to;
        let ordinal = outgoing[..index].iter().filter(|f| &f.to == to).count();
        Some((
            to.clone(),
            IncomingFlow {
                from: from.to_string(),
                ordinal,
            },
        ))
    }

    /// Whether `flow` names an existing sequence flow into `to`.
    pub fn has_incoming_flow(&self, to: &str, flow: &IncomingFlow) -> bool {
        self.element(&flow.from)
            .is_some_and(|e| e.outgoing.iter().filter(|f| f.to == to).count() > flow.ordinal)
    }

    /// Rewrites every [`ElementKind::CallActivity`] in this definition into an
    /// embedded [`ElementKind::SubProcess`] holding an inlined, id-prefixed copy
    /// of the called process's flow, resolving callees by id from `library`.
    ///
    /// Nano's corpus generator walks paths through the **real** engine, which has
    /// no first-class call-activity executor; inline expansion lets a multi-stage
    /// orchestrator (e.g. a CDD/AML refresh that calls nine phase processes) run
    /// on the existing, well-tested sub-process token-scope machinery without any
    /// new runtime state, event or snapshot variants.
    ///
    /// Expansion is recursive (a called process may itself call others) with a
    /// cycle guard. Inlined ids are prefixed `"<callId>$"` (recursively nested,
    /// so collisions across phases are impossible); `attached_to` /
    /// `start_event` references inside a callee are remapped to the prefixed ids.
    /// The call activity's own outgoing flow and parent scope are preserved, so
    /// the surrounding flow is untouched. Returns an error if a `calledElement`
    /// is missing from `library` or a call cycle is detected.
    pub fn inline_call_activities(
        &self,
        library: &HashMap<String, ProcessDefinition>,
    ) -> Result<ProcessDefinition, String> {
        let mut elements = HashMap::new();
        let mut stack = vec![self.id.clone()];
        splice_call_activities(&mut elements, &self.elements, "", None, library, &mut stack)?;
        Ok(ProcessDefinition {
            id: self.id.clone(),
            name: self.name.clone(),
            elements,
            start_event: self.start_event.clone(),
            xml: self.xml.clone(),
            adhoc: self.adhoc.clone(),
            start_form_id: self.start_form_id.clone(),
        })
    }
}

/// Recursively copies `src` elements into `out`, prefixing every id (and the
/// targets of outgoing flows and embedded id references) with `prefix`, and
/// expanding each [`ElementKind::CallActivity`] into a [`ElementKind::SubProcess`]
/// whose inner flow is a further-prefixed copy of the callee. `parent_override`
/// is the new parent id imposed on `src`'s process-level (parentless) elements —
/// `None` at the orchestrator's own level, `Some(callId)` when splicing a callee
/// underneath the sub-process that replaced its call activity. `stack` holds the
/// process ids on the current call path for cycle detection.
fn splice_call_activities(
    out: &mut HashMap<ElementId, Element>,
    src: &HashMap<ElementId, Element>,
    prefix: &str,
    parent_override: Option<&str>,
    library: &HashMap<String, ProcessDefinition>,
    stack: &mut Vec<String>,
) -> Result<(), String> {
    let pfx = |id: &str| format!("{prefix}{id}");
    for el in src.values() {
        let new_id = pfx(&el.id);
        let new_parent = match &el.parent {
            Some(p) => Some(pfx(p)),
            None => parent_override.map(|s| s.to_string()),
        };
        let outgoing: Vec<SequenceFlow> = el
            .outgoing
            .iter()
            .map(|f| SequenceFlow {
                to: pfx(&f.to),
                condition: f.condition.clone(),
                is_default: f.is_default,
            })
            .collect();
        if let ElementKind::CallActivity {
            called_process_id, ..
        } = &el.kind
        {
            let called = library.get(called_process_id).ok_or_else(|| {
                format!(
                    "call activity '{}' references unknown process '{}'",
                    el.id, called_process_id
                )
            })?;
            if stack.iter().any(|p| p == called_process_id) {
                return Err(format!(
                    "call activity cycle detected at process '{called_process_id}'"
                ));
            }
            let inner_prefix = format!("{new_id}$");
            let inner_start = format!("{inner_prefix}{}", called.start_event);
            out.insert(
                new_id.clone(),
                Element {
                    id: new_id.clone(),
                    kind: ElementKind::SubProcess {
                        start_event: inner_start,
                    },
                    name: el.name.clone(),
                    outgoing,
                    parent: new_parent,
                    io: el.io.clone(),
                    timer: el.timer.clone(),
                    retries: el.retries.clone(),
                    multi_instance: el.multi_instance.clone(),
                    start_listeners: el.start_listeners.clone(),
                    end_listeners: el.end_listeners.clone(),
                    task_listeners: el.task_listeners.clone(),
                },
            );
            stack.push(called_process_id.clone());
            splice_call_activities(
                out,
                &called.elements,
                &inner_prefix,
                Some(&new_id),
                library,
                stack,
            )?;
            stack.pop();
        } else {
            out.insert(
                new_id.clone(),
                Element {
                    id: new_id,
                    kind: remap_kind_ids(&el.kind, &pfx),
                    name: el.name.clone(),
                    outgoing,
                    parent: new_parent,
                    io: el.io.clone(),
                    timer: el.timer.clone(),
                    retries: el.retries.clone(),
                    multi_instance: el.multi_instance.clone(),
                    start_listeners: el.start_listeners.clone(),
                    end_listeners: el.end_listeners.clone(),
                    task_listeners: el.task_listeners.clone(),
                },
            );
        }
    }
    Ok(())
}

/// Returns a clone of `kind` with every embedded element-id reference (a
/// sub-process inner start, a boundary event's `attached_to`) rewritten through
/// `pfx`. Kinds without id references are cloned unchanged.
fn remap_kind_ids(kind: &ElementKind, pfx: &impl Fn(&str) -> String) -> ElementKind {
    match kind {
        ElementKind::SubProcess { start_event } => ElementKind::SubProcess {
            start_event: pfx(start_event),
        },
        ElementKind::EventSubProcess {
            start_event,
            trigger,
            interrupting,
        } => ElementKind::EventSubProcess {
            start_event: pfx(start_event),
            trigger: trigger.clone(),
            interrupting: *interrupting,
        },
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => ElementKind::ErrorBoundaryEvent {
            attached_to: pfx(attached_to),
            error_code: error_code.clone(),
        },
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => ElementKind::TimerBoundaryEvent {
            attached_to: pfx(attached_to),
            duration_millis: *duration_millis,
            interrupting: *interrupting,
            repeating: *repeating,
        },
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => ElementKind::MessageBoundaryEvent {
            attached_to: pfx(attached_to),
            message_name: message_name.clone(),
            correlation_key: correlation_key.clone(),
            interrupting: *interrupting,
        },
        ElementKind::SignalBoundaryEvent {
            attached_to,
            signal_name,
            interrupting,
        } => ElementKind::SignalBoundaryEvent {
            attached_to: pfx(attached_to),
            signal_name: signal_name.clone(),
            interrupting: *interrupting,
        },
        ElementKind::ConditionalBoundaryEvent {
            attached_to,
            condition,
            interrupting,
        } => ElementKind::ConditionalBoundaryEvent {
            attached_to: pfx(attached_to),
            condition: condition.clone(),
            interrupting: *interrupting,
        },
        ElementKind::CompensationBoundaryEvent {
            attached_to,
            handler,
        } => ElementKind::CompensationBoundaryEvent {
            attached_to: pfx(attached_to),
            handler: pfx(handler),
        },
        ElementKind::EscalationBoundaryEvent {
            attached_to,
            escalation_code,
            interrupting,
        } => ElementKind::EscalationBoundaryEvent {
            attached_to: pfx(attached_to),
            escalation_code: escalation_code.clone(),
            interrupting: *interrupting,
        },
        other => other.clone(),
    }
}

/// Ergonomic builder for [`ProcessDefinition`]s.
///
/// ```
/// use nanobpmn_engine_core::ProcessBuilder;
/// let def = ProcessBuilder::new("p")
///     .start_event("s")
///     .end_event("e")
///     .connect("s", "e")
///     .build()
///     .unwrap();
/// assert_eq!(def.start_event, "s");
/// ```
#[derive(Debug, Default)]
pub struct ProcessBuilder {
    id: String,
    /// The process's BPMN `name` attribute (modeller label), applied verbatim to
    /// the built [`ProcessDefinition::name`]. Set via [`ProcessBuilder::name`].
    name: Option<String>,
    elements: Vec<Element>,
    edges: Vec<(ElementId, SequenceFlow)>,
    /// Recorded `(child, parent sub-process)` containment, applied in [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    parents: Vec<(ElementId, ElementId)>,
    /// Recorded `(element id, ioMapping)` declarations, applied in [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    ios: Vec<(ElementId, IoMapping)>,
    /// Recorded `(element id, timer expression)` declarations, applied in
    /// [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    timers: Vec<(ElementId, TimerDef)>,
    retries: Vec<(ElementId, String)>,
    /// Recorded `(element id, multi-instance characteristics)` declarations,
    /// applied in [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    multi_instances: Vec<(ElementId, MultiInstance)>,
    /// Recorded `(element id, start listeners, end listeners)` declarations,
    /// applied in [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    listeners: Vec<(ElementId, Vec<ExecutionListener>, Vec<ExecutionListener>)>,
    /// Recorded `(element id, task listeners)` declarations, applied in
    /// [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    task_listeners: Vec<(ElementId, Vec<TaskListener>)>,
    /// Recorded `(element id, BPMN name)` declarations, applied in [`build`].
    ///
    /// [`build`]: ProcessBuilder::build
    names: Vec<(ElementId, String)>,
}

impl ProcessBuilder {
    /// Starts building a process with the given BPMN process id.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            elements: Vec::new(),
            edges: Vec::new(),
            parents: Vec::new(),
            ios: Vec::new(),
            timers: Vec::new(),
            retries: Vec::new(),
            multi_instances: Vec::new(),
            listeners: Vec::new(),
            task_listeners: Vec::new(),
            names: Vec::new(),
        }
    }

    /// Sets the process's BPMN `name` attribute (the modeller label surfaced as
    /// the process definition `name`). Distinct from the executable `id`.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    fn add(mut self, id: impl Into<String>, kind: ElementKind) -> Self {
        self.elements.push(Element {
            id: id.into(),
            kind,
            name: None,
            outgoing: Vec::new(),
            parent: None,
            io: IoMapping::default(),
            timer: None,
            retries: None,
            multi_instance: None,
            start_listeners: Vec::new(),
            end_listeners: Vec::new(),
            task_listeners: Vec::new(),
        });
        self
    }

    /// Adds a none start event.
    pub fn start_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::StartEvent)
    }

    /// Adds a message start event: a [`crate::Command::CorrelateMessage`] with a
    /// matching `message_name` creates a new instance starting here.
    pub fn message_start_event(
        self,
        id: impl Into<String>,
        message_name: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageStartEvent {
                message_name: message_name.into(),
            },
        )
    }

    /// Adds a one-shot timer start event: a single instance is created
    /// `duration_millis` after the process deploys.
    pub fn timer_start_event_once(self, id: impl Into<String>, duration_millis: u64) -> Self {
        self.add(
            id,
            ElementKind::TimerStartEvent {
                interval_millis: duration_millis,
                repeating: false,
            },
        )
    }

    /// Adds a recurring timer start event (a BPMN cycle): a new instance is
    /// created every `interval_millis`, the first one that long after deploy.
    pub fn timer_start_event_cycle(self, id: impl Into<String>, interval_millis: u64) -> Self {
        self.add(
            id,
            ElementKind::TimerStartEvent {
                interval_millis,
                repeating: true,
            },
        )
    }

    /// Adds a none end event.
    pub fn end_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::EndEvent)
    }

    /// Adds a terminate end event (`<endEvent>` with a
    /// `<terminateEventDefinition>`; see [`ElementKind::TerminateEndEvent`]).
    /// Reaching it kills every other active token in its enclosing scope and
    /// then completes that scope.
    pub fn terminate_end_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::TerminateEndEvent)
    }

    /// Adds a none intermediate throw event (a pass-through; see
    /// [`ElementKind::IntermediateThrowEvent`]).
    pub fn intermediate_throw_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::IntermediateThrowEvent)
    }

    /// Adds a link intermediate *throw* event (`intermediateThrowEvent` with a
    /// `linkEventDefinition`; see [`ElementKind::LinkIntermediateThrowEvent`]).
    /// On completion it hands its token to the matching link *catch* event with
    /// the same `link_name` rather than routing along an outgoing flow.
    pub fn link_intermediate_throw_event(
        self,
        id: impl Into<String>,
        link_name: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::LinkIntermediateThrowEvent {
                link_name: link_name.into(),
            },
        )
    }

    /// Adds a link intermediate *catch* event (`intermediateCatchEvent` with a
    /// `linkEventDefinition`; see [`ElementKind::LinkIntermediateCatchEvent`]).
    /// It has no incoming flow — the matching link throw activates it directly —
    /// and is a pass-through once activated, routing along its outgoing flow.
    pub fn link_intermediate_catch_event(
        self,
        id: impl Into<String>,
        link_name: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::LinkIntermediateCatchEvent {
                link_name: link_name.into(),
            },
        )
    }

    /// Adds a compensation throw event (`compensateEventDefinition` on an
    /// `intermediateThrowEvent`/`endEvent`; see
    /// [`ElementKind::CompensationThrowEvent`]).
    pub fn compensation_throw_event(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::CompensationThrowEvent)
    }

    /// Adds an abstract `task` (a pass-through; see [`ElementKind::Task`]).
    pub fn task(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::Task)
    }

    /// Adds an inline-FEEL script task (see [`ElementKind::ScriptTask`]): on
    /// activation the engine evaluates `expression` against the instance
    /// variables and stores the result under `result_variable`, then completes
    /// immediately with no job.
    pub fn script_task(
        self,
        id: impl Into<String>,
        expression: impl Into<String>,
        result_variable: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ScriptTask {
                expression: expression.into(),
                result_variable: result_variable.into(),
            },
        )
    }

    /// Adds a call activity invoking `called_process_id` (see
    /// [`ElementKind::CallActivity`]). Executed natively by the engine as a child
    /// process instance; the legacy inline-expansion mode
    /// ([`ProcessDefinition::inline_call_activities`]) remains available as an
    /// opt-in. Both Zeebe variable-propagation flags default to `true` (the Zeebe
    /// default when `zeebe:calledElement` omits them); use
    /// [`Self::call_activity_with_propagation`] to override them.
    pub fn call_activity(
        self,
        id: impl Into<String>,
        called_process_id: impl Into<String>,
    ) -> Self {
        self.call_activity_with_propagation(id, called_process_id, true, true)
    }

    /// Adds a call activity, explicitly setting the Zeebe
    /// `propagateAllParentVariables` / `propagateAllChildVariables` flags (see
    /// [`ElementKind::CallActivity`]).
    pub fn call_activity_with_propagation(
        self,
        id: impl Into<String>,
        called_process_id: impl Into<String>,
        propagate_all_parent_variables: bool,
        propagate_all_child_variables: bool,
    ) -> Self {
        self.add(
            id,
            ElementKind::CallActivity {
                called_process_id: called_process_id.into(),
                propagate_all_parent_variables,
                propagate_all_child_variables,
            },
        )
    }

    /// Adds a service task that creates jobs of the given `job_type`.
    pub fn service_task(self, id: impl Into<String>, job_type: impl Into<String>) -> Self {
        self.service_task_with_links(id, job_type, None, BTreeMap::new(), Vec::new())
    }

    /// Adds a service task whose jobs carry the given (raw) `zeebe:priorityDefinition`
    /// expression — a literal or FEEL expression resolved at job creation that
    /// controls activation order (higher priority is activated first).
    pub fn service_task_with_priority(
        self,
        id: impl Into<String>,
        job_type: impl Into<String>,
        priority: Option<String>,
    ) -> Self {
        self.service_task_with(id, job_type, priority, BTreeMap::new())
    }

    /// Adds a service task with an optional (raw) `zeebe:priorityDefinition`
    /// expression and static `zeebe:taskHeaders` — the full job-based service
    /// task the BPMN parser materialises. The headers ride onto the activated
    /// job verbatim (Zeebe `ActivatedJob.customHeaders`).
    pub fn service_task_with(
        self,
        id: impl Into<String>,
        job_type: impl Into<String>,
        priority: Option<String>,
        custom_headers: BTreeMap<String, String>,
    ) -> Self {
        self.service_task_with_links(id, job_type, priority, custom_headers, Vec::new())
    }

    /// Adds a service task with static headers plus `zeebe:linkedResources`. At
    /// job activation each linked resource's id is resolved to a concrete
    /// `resourceKey` and the resolved set is delivered in the `linkedResources`
    /// custom header (Zeebe parity).
    pub fn service_task_with_links(
        self,
        id: impl Into<String>,
        job_type: impl Into<String>,
        priority: Option<String>,
        custom_headers: BTreeMap<String, String>,
        linked_resources: Vec<LinkedResource>,
    ) -> Self {
        self.service_task_with_links_and_agent(
            id,
            job_type,
            priority,
            custom_headers,
            linked_resources,
            None,
        )
    }

    /// Adds an ordinary job-worker task, optionally classified as an agent.
    /// Agent markers share all job properties and the normal worker lifecycle.
    pub fn service_task_with_links_and_agent(
        self,
        id: impl Into<String>,
        job_type: impl Into<String>,
        priority: Option<String>,
        custom_headers: BTreeMap<String, String>,
        linked_resources: Vec<LinkedResource>,
        agent_type: Option<crate::agent::AgentType>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ServiceTask {
                job_type: job_type.into(),
                priority,
                agent_type,
                custom_headers,
                linked_resources,
            },
        )
    }

    /// Adds a service task bearing an agent marker. Workers create the
    /// AgentInstance and supply its definition/limits via `CreateAgentInstance`.
    pub fn agent_task(
        self,
        id: impl Into<String>,
        job_type: impl Into<String>,
        agent_type: crate::agent::AgentType,
    ) -> Self {
        self.service_task_with_links_and_agent(
            id,
            job_type,
            None,
            BTreeMap::new(),
            Vec::new(),
            Some(agent_type),
        )
    }

    /// Adds an exclusive (XOR) gateway.
    pub fn exclusive_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::ExclusiveGateway)
    }

    /// Adds a business rule task bound to the DMN decision `decision_id`
    /// (a literal id, or a `=`-prefixed FEEL expression resolved at activation).
    /// On success the decision output is merged into the instance under
    /// `result_variable`, or — when `None` — a map output's entries are spread
    /// into the instance scope.
    pub fn business_rule_task(
        self,
        id: impl Into<String>,
        decision_id: impl Into<String>,
        result_variable: Option<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::BusinessRuleTask {
                decision_id: decision_id.into(),
                result_variable,
            },
        )
    }

    /// Adds a (native) user task: on activation it creates a user task that a
    /// human claims and completes through the user-task API.
    pub fn user_task(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::UserTask(UserTaskProps::default()))
    }

    /// Adds a (native) user task with the given assignment/scheduling/priority
    /// expressions (as declared on the BPMN element's Zeebe extension elements).
    pub fn user_task_with(self, id: impl Into<String>, props: UserTaskProps) -> Self {
        self.add(id, ElementKind::UserTask(props))
    }

    /// Adds a parallel (AND) gateway.
    pub fn parallel_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::ParallelGateway)
    }

    /// Adds an inclusive (OR) gateway. As a split it takes every outgoing flow
    /// whose condition holds (an unconditional non-default flow is always taken),
    /// falling back to the `default` flow when none match. As a join it waits
    /// until no token still in the instance could reach it before producing its
    /// outgoing token(s).
    pub fn inclusive_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::InclusiveGateway)
    }

    /// Adds an event-based gateway: a deferred choice over the intermediate
    /// catch events that immediately follow it. On arrival it takes every
    /// outgoing flow (arming each downstream catch event); the first to fire
    /// wins and the losing siblings are withdrawn.
    pub fn event_based_gateway(self, id: impl Into<String>) -> Self {
        self.add(id, ElementKind::EventBasedGateway)
    }

    /// Adds an embedded sub-process whose inner token scope begins at
    /// `start_event` (the id of its inner none start event). Add the inner flow
    /// nodes/flows normally and mark each as [`contained_in`] this sub-process;
    /// connect the sub-process's own outgoing flow with [`connect`].
    ///
    /// [`contained_in`]: ProcessBuilder::contained_in
    /// [`connect`]: ProcessBuilder::connect
    pub fn sub_process(self, id: impl Into<String>, start_event: impl Into<String>) -> Self {
        self.add(
            id,
            ElementKind::SubProcess {
                start_event: start_event.into(),
            },
        )
    }

    /// Adds an event sub-process (`<subProcess triggeredByEvent="true">`): a
    /// container never entered by token flow, activated when its start event's
    /// `trigger` fires while the enclosing scope is active. `start_event` is
    /// the id of its inner (typed) start event; `interrupting` selects
    /// Zeebe's interrupting (terminate the scope's other work) vs
    /// non-interrupting (run concurrently) semantics. Contain it in an
    /// embedded sub-process with [`contained_in`](ProcessBuilder::contained_in)
    /// (as for any inner element); leave it parentless for the process scope.
    /// Enumerate the event sub-processes of a scope with
    /// [`ProcessDefinition::event_subprocesses_in_scope`].
    pub fn event_sub_process(
        self,
        id: impl Into<String>,
        start_event: impl Into<String>,
        trigger: EventSubProcessTrigger,
        interrupting: bool,
    ) -> Self {
        self.add(
            id,
            ElementKind::EventSubProcess {
                start_event: start_event.into(),
                trigger,
                interrupting,
            },
        )
    }

    /// Records that `child` is contained in the embedded sub-process `parent`.
    /// Inner elements must be declared this way so their tokens are scoped to the
    /// sub-process and so the process-level start event can be identified.
    pub fn contained_in(mut self, child: impl Into<String>, parent: impl Into<String>) -> Self {
        self.parents.push((child.into(), parent.into()));
        self
    }

    /// Attaches a BPMN `name` (the modeller's label) to a previously-added
    /// element. Purely descriptive — surfaced by read models such as the
    /// element-instance search API. Applied in [`build`](ProcessBuilder::build).
    pub fn with_name(mut self, id: impl Into<String>, name: impl Into<String>) -> Self {
        self.names.push((id.into(), name.into()));
        self
    }

    /// Attaches a `zeebe:ioMapping` to a previously-added element. Input mappings
    /// are applied on activation and output mappings on completion (see
    /// [`IoMapping`]). Applied in [`build`](ProcessBuilder::build).
    pub fn with_io(mut self, id: impl Into<String>, io: IoMapping) -> Self {
        self.ios.push((id.into(), io));
        self
    }

    /// Declares a FEEL timer expression on a timer element (see [`TimerDef`]).
    /// Applied in [`build`](ProcessBuilder::build).
    pub fn with_timer(mut self, id: impl Into<String>, timer: TimerDef) -> Self {
        self.timers.push((id.into(), timer));
        self
    }

    /// Declares a `zeebe:taskDefinition` `retries` expression (literal or FEEL)
    /// on a job-based task, evaluated to a number at job creation. Applied in
    /// [`build`](ProcessBuilder::build).
    pub fn with_retries(mut self, id: impl Into<String>, retries: impl Into<String>) -> Self {
        self.retries.push((id.into(), retries.into()));
        self
    }

    /// Declares multi-instance loop characteristics (see [`MultiInstance`]) on an
    /// activity. Applied in [`build`](ProcessBuilder::build).
    pub fn with_multi_instance(mut self, id: impl Into<String>, mi: MultiInstance) -> Self {
        self.multi_instances.push((id.into(), mi));
        self
    }

    /// Declares execution listeners (see [`ExecutionListener`]) on an element,
    /// split into start (fire on activation) and end (fire on completion) lists.
    /// Applied in [`build`](ProcessBuilder::build).
    pub fn with_listeners(
        mut self,
        id: impl Into<String>,
        start_listeners: Vec<ExecutionListener>,
        end_listeners: Vec<ExecutionListener>,
    ) -> Self {
        self.listeners
            .push((id.into(), start_listeners, end_listeners));
        self
    }

    /// Declares task listeners (see [`TaskListener`]) on a user task. Applied in
    /// [`build`](ProcessBuilder::build).
    pub fn with_task_listeners(
        mut self,
        id: impl Into<String>,
        task_listeners: Vec<TaskListener>,
    ) -> Self {
        self.task_listeners.push((id.into(), task_listeners));
        self
    }

    /// Adds an error boundary event attached to `attached_to`, catching the BPMN
    /// error `error_code`. Connect its outgoing flow(s) with [`connect`] to route
    /// the error-handling path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn error_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        error_code: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ErrorBoundaryEvent {
                attached_to: attached_to.into(),
                error_code: error_code.into(),
            },
        )
    }

    /// Adds an escalation throw event (an `intermediateThrowEvent` or `endEvent`
    /// carrying an `escalationEventDefinition`) that raises `escalation_code`.
    /// See [`ElementKind::EscalationThrowEvent`].
    pub fn escalation_throw_event(
        self,
        id: impl Into<String>,
        escalation_code: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::EscalationThrowEvent {
                escalation_code: escalation_code.into(),
            },
        )
    }

    /// Adds an **interrupting** escalation boundary event attached to
    /// `attached_to`, catching `escalation_code` (empty is a catch-all). Connect
    /// its outgoing flow(s) with [`connect`](ProcessBuilder::connect) to route
    /// the escalation-handling path. See [`ElementKind::EscalationBoundaryEvent`].
    pub fn escalation_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        escalation_code: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::EscalationBoundaryEvent {
                attached_to: attached_to.into(),
                escalation_code: escalation_code.into(),
                interrupting: true,
            },
        )
    }

    /// Adds a **non-interrupting** escalation boundary event attached to
    /// `attached_to`, catching `escalation_code` (empty is a catch-all). The
    /// attached activity keeps running and a parallel token is spawned on each
    /// catch. See [`ElementKind::EscalationBoundaryEvent`].
    pub fn non_interrupting_escalation_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        escalation_code: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::EscalationBoundaryEvent {
                attached_to: attached_to.into(),
                escalation_code: escalation_code.into(),
                interrupting: false,
            },
        )
    }

    /// Adds a compensation boundary event attached to `attached_to`, whose
    /// compensation `handler` activity (resolved from the BPMN `<association>`)
    /// is run when compensation is triggered for the completed activity. See
    /// [`ElementKind::CompensationBoundaryEvent`].
    pub fn compensation_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        handler: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::CompensationBoundaryEvent {
                attached_to: attached_to.into(),
                handler: handler.into(),
            },
        )
    }

    /// Adds a timer intermediate catch event that holds the token for
    /// `duration_millis` (in the host's clock units) before releasing it along
    /// its outgoing flow. The token resumes when a clock tick
    /// ([`crate::Command::TriggerTimers`]) finds the timer due.
    pub fn timer_intermediate_catch_event(
        self,
        id: impl Into<String>,
        duration_millis: u64,
    ) -> Self {
        self.add(
            id,
            ElementKind::TimerIntermediateCatchEvent { duration_millis },
        )
    }

    /// Adds an interrupting timer boundary event attached to `attached_to`. A
    /// timer is armed for `duration_millis` (in the host's clock units) when the
    /// activity activates; when it fires the activity is interrupted and the
    /// token runs along this event's outgoing flow. Connect its outgoing flow(s)
    /// with [`connect`] to route the timeout-handling path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn timer_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        duration_millis: u64,
    ) -> Self {
        self.add(
            id,
            ElementKind::TimerBoundaryEvent {
                attached_to: attached_to.into(),
                duration_millis,
                interrupting: true,
                repeating: false,
            },
        )
    }

    /// Adds a non-interrupting timer boundary event attached to `attached_to`. A
    /// timer is armed for `duration_millis` (in the host's clock units) when the
    /// activity activates; when it fires the activity keeps running and a new
    /// parallel token is spawned along this event's outgoing flow. Connect its
    /// outgoing flow(s) with [`connect`] to route the side path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn non_interrupting_timer_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        duration_millis: u64,
    ) -> Self {
        self.add(
            id,
            ElementKind::TimerBoundaryEvent {
                attached_to: attached_to.into(),
                duration_millis,
                interrupting: false,
                repeating: false,
            },
        )
    }

    /// Adds a non-interrupting **cycle** timer boundary event attached to
    /// `attached_to`. Like [`non_interrupting_timer_boundary_event`], but the
    /// timer re-arms for another `interval_millis` after each fire, so it spawns a
    /// parallel token every interval while the activity runs. Connect its outgoing
    /// flow(s) with [`connect`] to route the side path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    /// [`non_interrupting_timer_boundary_event`]: ProcessBuilder::non_interrupting_timer_boundary_event
    pub fn non_interrupting_timer_cycle_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        interval_millis: u64,
    ) -> Self {
        self.add(
            id,
            ElementKind::TimerBoundaryEvent {
                attached_to: attached_to.into(),
                duration_millis: interval_millis,
                interrupting: false,
                repeating: true,
            },
        )
    }

    /// Adds a message intermediate catch event named `message_name`, correlating
    /// on the instance variable named `correlation_key`. The token rests on it
    /// until a [`crate::Command::CorrelateMessage`] with a matching name and
    /// correlation value arrives, then resumes along its outgoing flow.
    pub fn message_intermediate_catch_event(
        self,
        id: impl Into<String>,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageIntermediateCatchEvent {
                message_name: message_name.into(),
                correlation_key: correlation_key.into(),
            },
        )
    }

    /// Adds an interrupting message boundary event attached to `attached_to`,
    /// subscribing to `message_name` and correlating on the instance variable
    /// named `correlation_key`. A subscription is opened when the activity
    /// activates; when a matching message is correlated the activity is
    /// interrupted and the token runs along this event's outgoing flow. Connect
    /// its outgoing flow(s) with [`connect`] to route the handling path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn message_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageBoundaryEvent {
                attached_to: attached_to.into(),
                message_name: message_name.into(),
                correlation_key: correlation_key.into(),
                interrupting: true,
            },
        )
    }

    /// Adds a non-interrupting message boundary event attached to `attached_to`,
    /// subscribing to `message_name` and correlating on the instance variable
    /// named `correlation_key`. A subscription is opened when the activity
    /// activates; for every matching message correlated the activity keeps
    /// running and a new parallel token is spawned along this event's outgoing
    /// flow (the subscription stays open). Connect its outgoing flow(s) with
    /// [`connect`] to route the side path.
    ///
    /// [`connect`]: ProcessBuilder::connect
    pub fn non_interrupting_message_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::MessageBoundaryEvent {
                attached_to: attached_to.into(),
                message_name: message_name.into(),
                correlation_key: correlation_key.into(),
                interrupting: false,
            },
        )
    }

    /// Adds a signal intermediate catch event named `signal_name`. The token
    /// rests on it until a [`crate::Command::BroadcastSignal`] with a matching
    /// name is broadcast, then resumes along its outgoing flow.
    pub fn signal_intermediate_catch_event(
        self,
        id: impl Into<String>,
        signal_name: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::SignalIntermediateCatchEvent {
                signal_name: signal_name.into(),
            },
        )
    }

    /// Adds an interrupting signal boundary event attached to `attached_to`,
    /// subscribing to `signal_name`. A subscription is opened when the activity
    /// activates; when a matching signal is broadcast the activity is interrupted
    /// and the token runs along this event's outgoing flow.
    pub fn signal_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        signal_name: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::SignalBoundaryEvent {
                attached_to: attached_to.into(),
                signal_name: signal_name.into(),
                interrupting: true,
            },
        )
    }

    /// Adds a non-interrupting signal boundary event attached to `attached_to`,
    /// subscribing to `signal_name`. A subscription is opened when the activity
    /// activates; for every matching signal broadcast the activity keeps running
    /// and a new parallel token is spawned along this event's outgoing flow.
    pub fn non_interrupting_signal_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        signal_name: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::SignalBoundaryEvent {
                attached_to: attached_to.into(),
                signal_name: signal_name.into(),
                interrupting: false,
            },
        )
    }

    /// Adds a conditional intermediate catch event (`conditionalEventDefinition`)
    /// that waits until its FEEL `condition` becomes `true`.
    pub fn conditional_intermediate_catch_event(
        self,
        id: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ConditionalIntermediateCatchEvent {
                condition: condition.into(),
            },
        )
    }

    /// Adds an interrupting conditional boundary event attached to `attached_to`,
    /// firing when its FEEL `condition` becomes `true` while the activity is
    /// active (interrupting the activity and taking this event's outgoing flow).
    pub fn conditional_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ConditionalBoundaryEvent {
                attached_to: attached_to.into(),
                condition: condition.into(),
                interrupting: true,
            },
        )
    }

    /// Adds a non-interrupting conditional boundary event attached to
    /// `attached_to`: each time its FEEL `condition` becomes `true` while the
    /// activity is active, the activity keeps running and a new parallel token is
    /// spawned along this event's outgoing flow.
    pub fn non_interrupting_conditional_boundary_event(
        self,
        id: impl Into<String>,
        attached_to: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        self.add(
            id,
            ElementKind::ConditionalBoundaryEvent {
                attached_to: attached_to.into(),
                condition: condition.into(),
                interrupting: false,
            },
        )
    }

    /// Adds an unconditional sequence flow from `from` to `to`.
    pub fn connect(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push((
            from.into(),
            SequenceFlow {
                to: to.into(),
                condition: None,
                is_default: false,
            },
        ));
        self
    }

    /// Adds a condition-routed gateway's (exclusive or inclusive) explicit
    /// **default** sequence flow from `from` to `to`. It carries no condition and
    /// is taken only as a fallback (when no non-default flow's condition is
    /// satisfied), irrespective of its position in the outgoing-flow order.
    pub fn connect_default(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push((
            from.into(),
            SequenceFlow {
                to: to.into(),
                condition: None,
                is_default: true,
            },
        ));
        self
    }

    /// Adds a conditional sequence flow from `from` to `to`, taken (on a
    /// condition-routed gateway — exclusive or inclusive) only when the FEEL
    /// `expression` evaluates to `true`.
    pub fn connect_when(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        expression: impl Into<String>,
    ) -> Self {
        self.edges.push((
            from.into(),
            SequenceFlow {
                to: to.into(),
                condition: Some(Condition::new(expression)),
                is_default: false,
            },
        ));
        self
    }

    /// Validates and assembles the [`ProcessDefinition`].
    ///
    /// Fails if there is not exactly one start event, or if a sequence flow
    /// references an unknown element.
    pub fn build(self) -> Result<ProcessDefinition, BuildError> {
        let mut elements: HashMap<ElementId, Element> = HashMap::new();
        for e in self.elements {
            if elements.contains_key(&e.id) {
                return Err(BuildError::DuplicateElement(e.id));
            }
            elements.insert(e.id.clone(), e);
        }

        for (from, flow) in &self.edges {
            if !elements.contains_key(&flow.to) {
                return Err(BuildError::UnknownFlowTarget {
                    from: from.clone(),
                    to: flow.to.clone(),
                });
            }
            let source = elements
                .get_mut(from)
                .ok_or_else(|| BuildError::UnknownFlowSource {
                    from: from.clone(),
                    to: flow.to.clone(),
                })?;
            source.outgoing.push(flow.clone());
        }

        // Apply sub-process containment.
        for (child, parent) in &self.parents {
            if !elements.contains_key(parent) {
                return Err(BuildError::UnknownParent {
                    child: child.clone(),
                    parent: parent.clone(),
                });
            }
            match elements.get_mut(child) {
                Some(element) => element.parent = Some(parent.clone()),
                None => {
                    return Err(BuildError::UnknownChild {
                        child: child.clone(),
                        parent: parent.clone(),
                    })
                }
            }
        }

        // Attach ioMapping declarations to their elements.
        for (id, io) in &self.ios {
            match elements.get_mut(id) {
                Some(element) => element.io = io.clone(),
                None => return Err(BuildError::UnknownIoMappingElement(id.clone())),
            }
        }

        // Attach FEEL timer expressions to their timer elements.
        for (id, timer) in &self.timers {
            match elements.get_mut(id) {
                Some(element) => element.timer = Some(timer.clone()),
                None => return Err(BuildError::UnknownTimerElement(id.clone())),
            }
        }

        // Attach retries expressions to their job-based task elements.
        for (id, retries) in &self.retries {
            match elements.get_mut(id) {
                Some(element) => element.retries = Some(retries.clone()),
                None => return Err(BuildError::UnknownRetriesElement(id.clone())),
            }
        }

        // Attach multi-instance loop characteristics to their activity elements.
        for (id, mi) in &self.multi_instances {
            match elements.get_mut(id) {
                Some(element) => element.multi_instance = Some(mi.clone()),
                None => return Err(BuildError::UnknownMultiInstanceElement(id.clone())),
            }
        }

        // Attach execution listeners to their elements.
        for (id, start_listeners, end_listeners) in &self.listeners {
            match elements.get_mut(id) {
                Some(element) => {
                    element.start_listeners = start_listeners.clone();
                    element.end_listeners = end_listeners.clone();
                }
                None => return Err(BuildError::UnknownListenerElement(id.clone())),
            }
        }

        // Attach task listeners to their user tasks.
        for (id, task_listeners) in &self.task_listeners {
            match elements.get_mut(id) {
                Some(element) => {
                    element.task_listeners = task_listeners.clone();
                }
                None => return Err(BuildError::UnknownListenerElement(id.clone())),
            }
        }

        // Attach BPMN names (modeller labels) to their elements.
        for (id, name) in &self.names {
            match elements.get_mut(id) {
                Some(element) => element.name = Some(name.clone()),
                None => return Err(BuildError::UnknownNameElement(id.clone())),
            }
        }

        validate_event_subprocesses(&elements)?;

        // The process-level start event is a start event that is not contained in
        // any sub-process (sub-process inner start events have a parent and start
        // their own scope, not the instance).
        //
        // Zeebe permits a process to declare more than one start event — a none
        // start alongside any number of *typed* (message/timer/signal) starts —
        // and forbids only *multiple none* starts (rejected by the post-parse
        // `start_events` validator, #855). Every message and timer start is wired
        // to its own deploy-time trigger by `crate::engine::Engine::deploy`; this
        // designation picks only the single process-entry start a CreateInstance
        // begins at, so choose one deterministically: prefer the none start, else
        // fall back to a typed start, tie-broken by id.
        //
        // Message and timer starts survive here as distinct typed kinds
        // (`ElementKind::is_start_event`). A signal start carries no dedicated
        // element kind: a surviving signal start is modelled as a plain
        // `ElementKind::StartEvent`, so the `find(StartEvent)` preference below
        // selects it as though it were the none start.
        let mut starts: Vec<&Element> = elements
            .values()
            .filter(|e| e.kind.is_start_event() && e.parent.is_none())
            .collect();
        if starts.is_empty() {
            return Err(BuildError::NoStartEvent);
        }
        starts.sort_by(|a, b| a.id.cmp(&b.id));
        let start_event = starts
            .iter()
            .find(|e| matches!(e.kind, ElementKind::StartEvent))
            .unwrap_or(&starts[0])
            .id
            .clone();

        Ok(ProcessDefinition {
            id: self.id,
            name: self.name,
            elements,
            start_event,
            // Programmatically built definitions have no source XML; parse_bpmn
            // overwrites this with the verbatim resource for parsed deployments.
            xml: String::new(),
            adhoc: Vec::new(),
            start_form_id: None,
        })
    }
}

/// Structural validation of every [`ElementKind::EventSubProcess`] — the single
/// canonical check shared by the BPMN parser, the programmatic builder and the
/// processos IR (all of which assemble through [`ProcessBuilder::build`]).
///
/// An event sub-process is entered only by its trigger, so it must have no
/// incoming or outgoing sequence flow; its `start_event` must name a start
/// event directly contained in it; and an error trigger must be interrupting.
pub(crate) fn validate_event_subprocesses(
    elements: &HashMap<ElementId, Element>,
) -> Result<(), BuildError> {
    let mut esps: Vec<&Element> = elements
        .values()
        .filter(|e| matches!(e.kind, ElementKind::EventSubProcess { .. }))
        .collect();
    esps.sort_by(|a, b| a.id.cmp(&b.id));
    for esp in esps {
        let ElementKind::EventSubProcess {
            start_event,
            trigger,
            interrupting,
        } = &esp.kind
        else {
            continue;
        };
        let invalid = |reason: String| BuildError::InvalidEventSubProcess {
            id: esp.id.clone(),
            reason,
        };
        if let Some(flow) = esp.outgoing.first() {
            return Err(invalid(format!(
                "has an outgoing sequence flow to {}; an event sub-process takes no sequence flows",
                flow.to
            )));
        }
        let mut sources: Vec<&ElementId> = elements
            .values()
            .filter(|e| e.outgoing.iter().any(|f| f.to == esp.id))
            .map(|e| &e.id)
            .collect();
        sources.sort();
        if let Some(src) = sources.first() {
            return Err(invalid(format!(
                "has an incoming sequence flow from {src}; an event sub-process takes no sequence flows"
            )));
        }
        match elements.get(start_event) {
            None => {
                return Err(invalid(format!(
                    "references unknown start event {start_event}"
                )))
            }
            Some(start)
                if !matches!(start.kind, ElementKind::StartEvent)
                    || start.parent.as_deref() != Some(esp.id.as_str()) =>
            {
                return Err(invalid(format!(
                    "start event {start_event} must be a plain start event directly contained in the event sub-process (the trigger lives on the container)"
                )))
            }
            Some(_) => {}
        }
        let mut extra: Vec<&ElementId> = elements
            .values()
            .filter(|e| {
                e.kind.is_start_event()
                    && e.parent.as_deref() == Some(esp.id.as_str())
                    && &e.id != start_event
            })
            .map(|e| &e.id)
            .collect();
        extra.sort();
        if let Some(other) = extra.first() {
            return Err(invalid(format!(
                "must have exactly one start event, found additional start event {other}"
            )));
        }
        // No sequence flow may cross the containment boundary (in either
        // direction, including from/to nested descendants): the inner work is
        // entered only through the trigger.
        let inside = |id: &str| -> bool {
            let mut cur = elements.get(id).and_then(|e| e.parent.as_deref());
            let mut hops = 0usize;
            while let Some(p) = cur {
                if p == esp.id.as_str() {
                    return true;
                }
                hops += 1;
                if hops > elements.len() {
                    return false;
                }
                cur = elements.get(p).and_then(|e| e.parent.as_deref());
            }
            false
        };
        let mut crossing: Vec<(&ElementId, &ElementId)> = elements
            .values()
            .flat_map(|e| e.outgoing.iter().map(move |f| (&e.id, &f.to)))
            .filter(|(from, to)| inside(from) != inside(to))
            .collect();
        crossing.sort();
        if let Some((from, to)) = crossing.first() {
            return Err(invalid(format!(
                "sequence flow {from} -> {to} crosses the event sub-process boundary"
            )));
        }
        if matches!(trigger, EventSubProcessTrigger::Error { .. }) && !interrupting {
            return Err(invalid(
                "an error event sub-process must be interrupting".to_string(),
            ));
        }
    }
    Ok(())
}

/// Errors produced while assembling a [`ProcessDefinition`] with [`ProcessBuilder::build`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildError {
    DuplicateElement(ElementId),
    UnknownFlowSource {
        from: ElementId,
        to: ElementId,
    },
    UnknownFlowTarget {
        from: ElementId,
        to: ElementId,
    },
    NoStartEvent,
    /// A `contained_in` referenced a sub-process element that does not exist.
    UnknownParent {
        child: ElementId,
        parent: ElementId,
    },
    /// A `contained_in` referenced a child element that does not exist.
    UnknownChild {
        child: ElementId,
        parent: ElementId,
    },
    /// A `with_io` referenced an element that does not exist.
    UnknownIoMappingElement(ElementId),
    /// A `with_timer` referenced an element that does not exist.
    UnknownTimerElement(ElementId),
    /// A `with_retries` referenced an element that does not exist.
    UnknownRetriesElement(ElementId),
    /// A `with_multi_instance` referenced an element that does not exist.
    UnknownMultiInstanceElement(ElementId),
    /// A `with_listeners` referenced an element that does not exist.
    UnknownListenerElement(ElementId),
    /// A `with_name` referenced an element that does not exist.
    UnknownNameElement(ElementId),
    /// An [`ElementKind::EventSubProcess`] violates a structural constraint:
    /// it has an incoming/outgoing sequence flow, its `start_event` is not a
    /// start event directly contained in it, or it carries a non-interrupting
    /// error trigger.
    InvalidEventSubProcess {
        /// The offending event sub-process.
        id: ElementId,
        /// Human-readable reason.
        reason: String,
    },
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::DuplicateElement(id) => write!(f, "duplicate element id: {id}"),
            BuildError::UnknownFlowSource { from, to } => {
                write!(
                    f,
                    "sequence flow {from}->{to} has unknown source element {from}"
                )
            }
            BuildError::UnknownFlowTarget { from, to } => {
                write!(
                    f,
                    "sequence flow {from}->{to} has unknown target element {to}"
                )
            }
            BuildError::NoStartEvent => write!(f, "process has no start event"),
            BuildError::UnknownParent { child, parent } => {
                write!(
                    f,
                    "element {child} is contained in unknown sub-process {parent}"
                )
            }
            BuildError::UnknownChild { child, parent } => {
                write!(
                    f,
                    "unknown element {child} declared as contained in sub-process {parent}"
                )
            }
            BuildError::UnknownIoMappingElement(id) => {
                write!(f, "ioMapping declared on unknown element {id}")
            }
            BuildError::UnknownTimerElement(id) => {
                write!(f, "timer expression declared on unknown element {id}")
            }
            BuildError::UnknownRetriesElement(id) => {
                write!(f, "retries expression declared on unknown element {id}")
            }
            BuildError::UnknownMultiInstanceElement(id) => {
                write!(
                    f,
                    "multi-instance characteristics declared on unknown element {id}"
                )
            }
            BuildError::UnknownListenerElement(id) => {
                write!(f, "execution listeners declared on unknown element {id}")
            }
            BuildError::InvalidEventSubProcess { id, reason } => {
                write!(f, "event sub-process {id}: {reason}")
            }
            BuildError::UnknownNameElement(id) => {
                write!(f, "name declared on unknown element {id}")
            }
        }
    }
}

impl std::error::Error for BuildError {}

#[cfg(test)]
mod multi_instance_default_tests {
    use super::MultiInstance;

    #[test]
    fn default_cancels_remaining_instances() {
        let mi = MultiInstance {
            input_collection: "=items".into(),
            ..Default::default()
        };
        assert!(mi.cancel_remaining_instances);
        assert!(!mi.sequential);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn deserialize_defaults_cancel_remaining_instances() {
        let mi: MultiInstance =
            serde_json::from_str(r#"{"input_collection":"=items"}"#).expect("decode");
        assert!(mi.cancel_remaining_instances);
        assert_eq!(
            mi,
            MultiInstance {
                input_collection: "=items".into(),
                ..Default::default()
            }
        );
    }
}

#[cfg(test)]
mod start_event_build_tests {
    use super::{BuildError, ElementKind, ProcessBuilder};

    fn end(builder: ProcessBuilder, start: &str) -> ProcessBuilder {
        // Give a start event a valid outgoing flow to an end event so the graph
        // is connected; each start gets its own private end.
        let e = format!("{start}_end");
        builder.end_event(e.clone()).connect(start, e)
    }

    #[test]
    fn permits_none_plus_typed_starts_and_designates_the_none_entry() {
        // A none start alongside a message start: Zeebe permits this, and the
        // engine must begin a CreateInstance at the none start.
        let builder = ProcessBuilder::new("p")
            .start_event("noneStart")
            .message_start_event("msgStart", "orderPlaced");
        let builder = end(builder, "noneStart");
        let def = end(builder, "msgStart")
            .build()
            .expect("multi-typed start permitted");
        assert_eq!(def.start_event, "noneStart");
        // Both start events are retained on the definition.
        assert!(matches!(
            def.element("noneStart").unwrap().kind,
            ElementKind::StartEvent
        ));
        assert!(matches!(
            def.element("msgStart").unwrap().kind,
            ElementKind::MessageStartEvent { .. }
        ));
    }

    #[test]
    fn designates_a_typed_entry_when_there_is_no_none_start() {
        // No none start: the entry falls back to a typed start, deterministically
        // by id (so the choice is stable across builds).
        let builder = ProcessBuilder::new("p")
            .message_start_event("bMsg", "b")
            .timer_start_event_once("aTimer", 1000);
        let builder = end(builder, "bMsg");
        let def = end(builder, "aTimer")
            .build()
            .expect("multiple typed starts permitted");
        assert_eq!(def.start_event, "aTimer", "min-id typed start is the entry");
    }

    #[test]
    fn zero_start_events_still_rejected() {
        let def = ProcessBuilder::new("p").end_event("e").build();
        assert_eq!(def.unwrap_err(), BuildError::NoStartEvent);
    }

    #[test]
    fn build_no_longer_rejects_multiple_starts_outright() {
        // The over-strict "more than one start event" rejection is gone; the
        // multiple-none rule is enforced by the post-parse start_events validator,
        // so the builder itself accepts two none starts.
        let builder = ProcessBuilder::new("p").start_event("s1").start_event("s2");
        let builder = end(builder, "s1");
        let def = end(builder, "s2").build();
        assert!(def.is_ok(), "builder permits multiple starts: {def:?}");
    }
}

#[cfg(test)]
mod approx_bytes_tests {
    use std::collections::BTreeMap;

    use super::Value;

    #[test]
    fn approx_bytes_scales_with_string_payload() {
        // Scalars fold to a small constant.
        assert_eq!(Value::Null.approx_bytes(), 8);
        assert_eq!(Value::Int(42).approx_bytes(), 8);
        assert_eq!(Value::Bool(true).approx_bytes(), 8);

        // A string's estimate tracks its byte length (plus fixed overhead).
        let blob = "x".repeat(50_000);
        let v = Value::Str(blob.clone());
        assert_eq!(v.approx_bytes(), 16 + blob.len() as u64);

        // Nested map/list sum their members' bytes, so a large payload dominates.
        let mut m = BTreeMap::new();
        m.insert("payload".to_string(), Value::Str(blob.clone()));
        m.insert("n".to_string(), Value::Int(1));
        let map = Value::Map(m);
        let got = map.approx_bytes();
        assert!(
            got >= blob.len() as u64,
            "map estimate {got} covers the blob"
        );
        assert!(got < blob.len() as u64 + 200, "no runaway overhead: {got}");
    }
}
