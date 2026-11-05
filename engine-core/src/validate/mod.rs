//! Post-parse validation seam (deploy-validation parity, epic #850).
//!
//! After [`crate::bpmn::parse_bpmn`] assembles a [`ProcessDefinition`], every
//! validator registered in [`VALIDATORS`] runs in sequence over a
//! [`ValidationInput`] — the built definition plus the raw [`ProcessCapture`]
//! the streaming parser recorded (reference sites, unmodelled tags, declared
//! definition ids, …). The first validator to reject returns its [`ParseError`].
//!
//! ## How to add a rule (for the wave-1 sibling slices of #850)
//!
//! Each sibling implements its rule by editing **only** the body of its own
//! pre-created stub module below. A sibling **must not** edit this file
//! (`validate/mod.rs`), the [`ParseError`](crate::bpmn::ParseError) enum, or
//! [`crate::bpmn`]'s streaming parser: the validator is already registered here,
//! every [`ParseError`] variant it needs is already declared, and every
//! reference site / unmodelled tag it must inspect is already captured on
//! [`ProcessCapture`]. The rule only *reads* pre-captured data and returns
//! `Err(ParseError::…)` on a violation.
//!
//! * `incoming_outgoing` — dangling `<incoming>`/`<outgoing>` references (#849).
//! * `references`        — generic reference integrity (#851).
//! * `unsupported_elements` — unmodelled flow elements / event defs (#853).
//! * `gateway_conditions` — gateway condition-or-default (#854).
//! * `start_events`      — start-event count parity (#855).
//! * `cheap_rules`       — end-event outgoing / duplicate starts / taskDef (#856).

use std::collections::{HashMap, HashSet};

pub(crate) use error::ParseError;

use crate::model::ProcessDefinition;

mod cheap_rules;
pub(crate) mod error;
mod gateway_conditions;
mod incoming_outgoing;
mod references;
mod start_events;
pub(crate) mod unsupported_elements;

/// A captured `<incoming>`/`<outgoing>` QName reference from a flow node to a
/// `<sequenceFlow>`.
pub(crate) struct FlowRefCapture {
    /// Id of the flow node that declared the reference.
    pub node_id: String,
    /// `"incoming"` or `"outgoing"`.
    pub direction: &'static str,
    /// The referenced `<sequenceFlow>` id.
    pub flow_id: String,
}

/// A captured QName/id reference site other than `<incoming>`/`<outgoing>`
/// (which have their own [`FlowRefCapture`]). `kind` is one of `"messageRef"`,
/// `"errorRef"`, `"signalRef"`, `"escalationRef"`, `"default"`,
/// `"attachedToRef"`.
pub(crate) struct RefSite {
    pub kind: &'static str,
    /// The referenced id.
    pub id: String,
    /// Id of the element that declared the reference.
    pub from_node: String,
}

/// A flow-element tag / event definition the streaming parser does not model,
/// recorded rather than silently dropped.
pub(crate) struct UnmodelledElement {
    pub tag: String,
    pub element_id: String,
}

/// A captured `zeebe:taskDefinition` on a job-based task, retaining the raw
/// attribute strings (before `build` defaults an absent `type` to the id) so
/// the empty-attribute rule (#856) can see an explicitly-empty value.
pub(crate) struct TaskDefCapture {
    pub task_id: String,
    pub job_type: Option<String>,
    pub retries: Option<String>,
}

/// Everything the streaming parser recorded for one `<process>` that a
/// post-parse validator may need but that is not (or not losslessly) present on
/// the built [`ProcessDefinition`]. Snapshotted from the raw parse accumulator
/// before the builder consumes it, so validators inspect what Zeebe sees at
/// deploy rather than Nano's transformed executable graph.
///
/// Many fields are consumed only by the pre-registered wave-1 sibling
/// validators (#851/#853/#854/#855/#856); the scaffold populates them now so
/// those slices become pure add-a-file changes.
pub(crate) struct ProcessCapture {
    pub process_id: String,
    /// Every declared `<sequenceFlow>` id, before any ad-hoc pruning.
    pub flow_ids: HashSet<String>,
    /// `<incoming>`/`<outgoing>` references declared on flow nodes.
    pub flow_refs: Vec<FlowRefCapture>,
    /// `messageRef`/`errorRef`/`signalRef`/`escalationRef`/`default`/
    /// `attachedToRef` reference sites.
    pub references: Vec<RefSite>,
    /// `(link name, throwing element id, enclosing scope)` for each intermediate
    /// *throw* link event. The element id is used as the `from_node` when a throw
    /// link is rejected as unpaired; the scope (enclosing subprocess id, or `None`
    /// at the process root) enforces same-scope throw↔catch pairing at deploy.
    pub link_throws: Vec<(String, String, Option<String>)>,
    /// `(link name, enclosing scope)` for each intermediate *catch* link event.
    pub link_catches: Vec<(String, Option<String>)>,
    /// Unmodelled flow elements / event definitions.
    pub unmodelled: Vec<UnmodelledElement>,
    /// Declared definitions-level `<message>` ids.
    pub declared_messages: HashSet<String>,
    /// Declared definitions-level `<error>` ids.
    pub declared_errors: HashSet<String>,
    /// Declared definitions-level `<signal>` ids.
    pub declared_signals: HashSet<String>,
    /// Declared definitions-level `<signal>` id → `name` map. Signals correlate
    /// by *name*, so the duplicate-signal-start rule (#856) resolves each
    /// `signalRef` id through this map and dedupes by the resolved name —
    /// mirroring the name-keyed message-start rule.
    pub signal_names: HashMap<String, String>,
    /// Declared definitions-level `<escalation>` ids.
    pub declared_escalations: HashSet<String>,
    /// `zeebe:taskDefinition`s on job-based tasks, with raw attribute strings.
    pub task_definitions: Vec<TaskDefCapture>,
}

/// The input handed to every validator: the built definition plus the raw parse
/// capture.
pub(crate) struct ValidationInput<'a> {
    pub def: &'a ProcessDefinition,
    pub capture: &'a ProcessCapture,
}

/// A post-parse validator: rejects a definition (with a [`ParseError`]) or
/// passes it through.
type Validator = fn(&ValidationInput<'_>) -> Result<(), ParseError>;

/// The ordered validator registry. Every wave-1 sibling rule is pre-registered;
/// a sibling only fills in its own stub module's body.
const VALIDATORS: &[Validator] = &[
    incoming_outgoing::validate,
    references::validate,
    unsupported_elements::validate,
    gateway_conditions::validate,
    start_events::validate,
    cheap_rules::validate,
];

/// Runs every registered validator in order, short-circuiting on the first
/// rejection.
pub(crate) fn run(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    for validator in VALIDATORS {
        validator(input)?;
    }
    Ok(())
}
