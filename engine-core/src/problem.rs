//! The single, shared `EngineError` → ProblemDetail mapping (#1937).
//!
//! Every surface that reports an engine rejection — the server's REST
//! handlers and Raft write path, and the in-browser wasm `TestEngine` — calls
//! [`EngineError::problem`], so the HTTP status, ProblemDetail `title` and
//! retryability of a rejection cannot drift between them (the #1931 class: a
//! transient error marked non-retryable on one surface).
//!
//! The mapping is keyed on a Zeebe rejection class ([`ProblemKind`]); status,
//! title and retryability are all *derived* from that class, so the only
//! per-variant decision is which class a variant belongs to. The match in
//! [`EngineError::problem_kind`] is exhaustive with no catch-all: a new
//! `EngineError` variant does not compile until it is classified.

use crate::EngineError;

/// The Zeebe/Camunda rejection class of an engine error. Determines the HTTP
/// status, the ProblemDetail title and whether a client may retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProblemKind {
    /// Malformed or unresolvable request data (Zeebe `INVALID_ARGUMENT`, 400).
    InvalidArgument,
    /// The referenced resource does not exist (Zeebe `NOT_FOUND`, 404).
    NotFound,
    /// The resource exists but its state conflicts with the request (Zeebe
    /// `INVALID_STATE`, 409).
    InvalidState,
    /// The resource being created already exists (Zeebe `ALREADY_EXISTS`, 409).
    AlreadyExists,
    /// A transient, environmental failure; retrying may succeed (Zeebe
    /// `UNAVAILABLE`, 503).
    Unavailable,
}

impl ProblemKind {
    /// Every class, for table tests.
    pub const ALL: [ProblemKind; 5] = [
        ProblemKind::InvalidArgument,
        ProblemKind::NotFound,
        ProblemKind::InvalidState,
        ProblemKind::AlreadyExists,
        ProblemKind::Unavailable,
    ];

    /// HTTP status of this class.
    pub fn status(self) -> u16 {
        match self {
            ProblemKind::InvalidArgument => 400,
            ProblemKind::NotFound => 404,
            ProblemKind::InvalidState | ProblemKind::AlreadyExists => 409,
            ProblemKind::Unavailable => 503,
        }
    }

    /// ProblemDetail `title` (the Zeebe rejection-type spelling Camunda's REST
    /// API uses).
    pub fn title(self) -> &'static str {
        match self {
            ProblemKind::InvalidArgument => "INVALID_ARGUMENT",
            ProblemKind::NotFound => "NOT_FOUND",
            ProblemKind::InvalidState => "INVALID_STATE",
            ProblemKind::AlreadyExists => "ALREADY_EXISTS",
            ProblemKind::Unavailable => "UNAVAILABLE",
        }
    }

    /// Whether a client may retry the same request (Zeebe: only transient
    /// failures are retryable; every rejection is final).
    pub fn retryable(self) -> bool {
        match self {
            ProblemKind::Unavailable => true,
            ProblemKind::InvalidArgument
            | ProblemKind::NotFound
            | ProblemKind::InvalidState
            | ProblemKind::AlreadyExists => false,
        }
    }
}

/// A ProblemDetail-shaped description of an engine rejection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineProblem {
    /// The rejection class everything else is derived from.
    pub kind: ProblemKind,
    /// HTTP status.
    pub status: u16,
    /// ProblemDetail `title`.
    pub title: &'static str,
    /// ProblemDetail `detail` (human-readable message).
    pub detail: String,
    /// Whether the client may retry.
    pub retryable: bool,
}

impl EngineError {
    /// The rejection class of this error. Exhaustive, no catch-all (#1937).
    pub fn problem_kind(&self) -> ProblemKind {
        use EngineError as E;
        use ProblemKind as K;
        match self {
            // Off-heap state transiently unreadable (#1388): retryable, never 404.
            E::InstanceUnavailable { .. } => K::Unavailable,

            E::DecisionNotFound { .. }
            | E::JobNotFound { .. }
            | E::ClusterVariableNotFound { .. }
            | E::InstanceNotFound { .. }
            | E::IncidentNotFound { .. }
            | E::UserTaskNotFound { .. }
            | E::AdHocSubProcessNotFound { .. }
            | E::AdHocUnknownElement { .. }
            | E::TargetProcessDefinitionNotFound { .. }
            | E::AgentInstanceNotFound { .. }
            | E::AgentInstanceElementInstanceInactive { .. }
            | E::AgentInstanceJobNotActive { .. }
            | E::AgentInstanceJobLeaseMismatch { .. } => K::NotFound,

            E::ClusterVariableAlreadyExists { .. }
            | E::MessageAlreadyExists { .. }
            | E::AgentInstanceAlreadyExists { .. } => K::AlreadyExists,

            E::JobNotActive { .. }
            | E::JobNotActivated { .. }
            | E::JobLeaseMismatch { .. }
            | E::AgentInstanceActiveWriter { .. }
            | E::AgentInstanceConflict { .. }
            | E::BusinessIdOnChildInstance { .. }
            | E::BusinessIdAlreadyAssigned { .. }
            | E::InstanceTransitionInvalid { .. }
            | E::IncidentNotResolvable { .. }
            | E::UserTaskNotActive { .. }
            | E::UserTaskAlreadyAssigned { .. }
            | E::UnmappedActiveElement { .. }
            | E::MappedElementTypeChanged { .. }
            | E::MigratedParallelJoinArityChanged { .. }
            | E::MigratedJoinFlowMissing { .. }
            | E::MigratedGatewayRouteMissing { .. }
            | E::UnsupportedMigration { .. }
            | E::InstanceNotActive { .. }
            | E::MigrationFlowScopeChanged { .. }
            | E::MigrationMultiInstanceMismatch { .. }
            | E::MigrationLoopCharacteristicsChanged { .. }
            | E::MigrationCatchEventDetached { .. }
            | E::MigrationCatchEventsMerged { .. }
            | E::MigrationCompensationFlowScopeChanged { .. }
            | E::MigrationCatchEventSubscriptionFailed { .. } => K::InvalidState,

            // Process-id / scope references in the request body are invalid
            // data, not a missing addressed resource (Camunda returns 400).
            E::ProcessNotFound { .. }
            | E::ScopeNotFound { .. }
            | E::NoStartEvent { .. }
            | E::StartEventExpressionInvalid { .. }
            | E::InvalidProcessModel { .. }
            | E::JobUpdateInvalid { .. }
            | E::AgentHistoryInvalid { .. }
            | E::AgentInstanceElementNotEligible { .. }
            | E::AgentInstanceMissingAgentDefinition { .. }
            | E::AgentInstanceJobRequiredForHistory { .. }
            | E::AgentInstanceJobElementMismatch { .. }
            | E::AgentInstanceOwnershipMismatch { .. }
            | E::AgentInstanceStatusNotSettable { .. }
            | E::AgentInstanceAlreadyCompleted { .. }
            | E::AgentInstanceLimitExceeded { .. }
            | E::BusinessIdInvalid { .. }
            | E::TaskListenerJobWithVariables { .. }
            | E::TaskListenerDenyWithCorrections { .. }
            | E::TaskListenerDenyNotSupported { .. }
            | E::TaskListenerAssigneeCorrectionOnCreating { .. }
            | E::AdHocActivateWithCompletion { .. }
            | E::AdHocNoActivationTargets { .. }
            | E::DuplicateMappingSourceElement { .. }
            | E::MappingSourceElementNotFound { .. }
            | E::MappingTargetElementNotFound { .. }
            | E::MigrationCompensationBoundaryTypeChanged { .. }
            | E::ElementNotFound { .. }
            | E::ElementInstanceNotFound { .. }
            | E::ModificationInstructionNotYetSupported { .. }
            | E::ModificationInstructionInvalid { .. }
            | E::AmbiguousFlowScope { .. }
            | E::AncestorScopeNotFound { .. }
            | E::AncestorNotAnAncestor { .. }
            | E::ElementNotActivatable { .. } => K::InvalidArgument,
        }
    }

    /// The canonical ProblemDetail for this rejection — the ONE mapping the
    /// server and the wasm `TestEngine` share (#1937).
    pub fn problem(&self) -> EngineProblem {
        let kind = self.problem_kind();
        let detail = match self {
            EngineError::ProcessNotFound { process_id } => {
                format!("No deployed process with id '{process_id}'.")
            }
            EngineError::JobNotFound { job_key } => format!("No job with key {job_key}."),
            EngineError::JobNotActive { job_key } => format!("Job {job_key} is not active."),
            EngineError::JobNotActivated { job_key } => {
                format!("Job {job_key} has not been activated.")
            }
            other => other.to_string(),
        };
        EngineProblem {
            kind,
            status: kind.status(),
            title: kind.title(),
            detail,
            retryable: kind.retryable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The class table: status, title and retryability (Zeebe: only the
    /// transient `UNAVAILABLE` class is retryable).
    #[test]
    fn problem_kind_table() {
        let table = [
            (ProblemKind::InvalidArgument, 400, "INVALID_ARGUMENT", false),
            (ProblemKind::NotFound, 404, "NOT_FOUND", false),
            (ProblemKind::InvalidState, 409, "INVALID_STATE", false),
            (ProblemKind::AlreadyExists, 409, "ALREADY_EXISTS", false),
            (ProblemKind::Unavailable, 503, "UNAVAILABLE", true),
        ];
        assert_eq!(table.len(), ProblemKind::ALL.len());
        for (kind, status, title, retryable) in table {
            assert_eq!(kind.status(), status, "{kind:?}");
            assert_eq!(kind.title(), title, "{kind:?}");
            assert_eq!(kind.retryable(), retryable, "{kind:?}");
        }
    }

    /// Representative variants per class, including every retryability edge.
    #[test]
    fn engine_error_problem_table() {
        let cases: Vec<(EngineError, u16, &str, bool)> = vec![
            (
                EngineError::InstanceUnavailable { instance_key: 7 },
                503,
                "UNAVAILABLE",
                true,
            ),
            (
                EngineError::JobNotFound { job_key: 1 },
                404,
                "NOT_FOUND",
                false,
            ),
            (
                EngineError::JobNotActive { job_key: 1 },
                409,
                "INVALID_STATE",
                false,
            ),
            (
                EngineError::ProcessNotFound {
                    process_id: "p".into(),
                },
                400,
                "INVALID_ARGUMENT",
                false,
            ),
            (
                EngineError::ClusterVariableAlreadyExists {
                    tenant_id: None,
                    name: "x".into(),
                },
                409,
                "ALREADY_EXISTS",
                false,
            ),
        ];
        for (err, status, title, retryable) in cases {
            let p = err.problem();
            assert_eq!(p.status, status, "{err:?}");
            assert_eq!(p.title, title, "{err:?}");
            assert_eq!(p.retryable, retryable, "{err:?}");
            assert_eq!(p.status, p.kind.status());
            // Retryability is the server's environmental classification:
            // the two cannot drift (#1931).
            assert_eq!(p.retryable, err.is_environmental(), "{err:?}");
            assert!(!p.detail.is_empty());
        }
    }

    /// Every `EngineError` variant is classified explicitly in
    /// `problem_kind` (one `E::Variant` arm entry each) — guards against a
    /// catch-all creeping in and silently classifying new variants.
    #[test]
    fn every_variant_is_classified_explicitly() {
        let engine_src = include_str!("engine/mod.rs");
        let start = engine_src
            .find("pub enum EngineError {")
            .expect("EngineError enum");
        let body = &engine_src[start..];
        let body = &body[..body.find("\n}\n").expect("enum end")];
        let mut variants: Vec<&str> = body
            .lines()
            .skip(1)
            .filter_map(|l| {
                let t = l.strip_prefix("    ")?;
                if t.starts_with(' ') || t.starts_with("//") || t.starts_with('#') {
                    return None;
                }
                let name: &str = t.split(|c: char| !c.is_alphanumeric() && c != '_').next()?;
                (!name.is_empty()).then_some(name)
            })
            .collect();
        variants.sort_unstable();
        variants.dedup();

        let src = include_str!("problem.rs");
        let f = &src[src.find("fn problem_kind").expect("fn")..];
        let f = &f[..f.find("\n    }\n").expect("fn end")];
        assert!(!f.contains("_ =>"), "problem_kind must have no catch-all");
        let classified = f.matches("E::").count();
        assert_eq!(
            classified,
            variants.len(),
            "problem_kind arms vs EngineError variants: {variants:?}"
        );
        for v in &variants {
            assert!(f.contains(&format!("E::{v} ")), "{v} not classified");
        }
    }

    /// Variants per arm of a `match` whose arms are `E::A { .. } | … => RHS`.
    fn arm_sets<'a>(body: &'a str, prefix: &str) -> Vec<(String, Vec<&'a str>)> {
        let mut out = Vec::new();
        let mut cur: Vec<&str> = Vec::new();
        for line in body.lines() {
            let t = line.trim().trim_start_matches("| ");
            if let Some(rest) = t.strip_prefix(prefix) {
                let name = rest
                    .split(|c: char| !c.is_alphanumeric() && c != '_')
                    .next()
                    .unwrap_or_default();
                cur.push(name);
                if let Some((_, rhs)) = rest.split_once("=>") {
                    out.push((
                        rhs.trim().trim_end_matches(',').to_string(),
                        std::mem::take(&mut cur),
                    ));
                }
            }
        }
        out
    }

    /// Behavioural guard on the retryable class: the `Unavailable` arm is
    /// exactly the set `is_environmental()` reports `true` for, and is pinned
    /// to the one known transient variant. Moving any other variant into
    /// `Unavailable` (making it retryable) — or making it environmental
    /// without classifying it `Unavailable` — fails here (#1931 class).
    #[test]
    fn unavailable_class_is_exactly_the_environmental_set() {
        let src = include_str!("problem.rs");
        let f = &src[src.find("fn problem_kind").expect("fn")..];
        let f = &f[..f.find("\n    }\n").expect("fn end")];
        let mut unavailable: Vec<&str> = arm_sets(f, "E::")
            .into_iter()
            .filter(|(rhs, _)| rhs == "K::Unavailable")
            .flat_map(|(_, v)| v)
            .collect();
        unavailable.sort_unstable();

        let engine_src = include_str!("engine/mod.rs");
        let g = &engine_src[engine_src.find("pub fn is_environmental").expect("fn")..];
        let g = &g[..g.find("\n    }\n").expect("fn end")];
        let mut environmental: Vec<&str> = arm_sets(g, "EngineError::")
            .into_iter()
            .filter(|(rhs, _)| rhs == "true")
            .flat_map(|(_, v)| v)
            .collect();
        environmental.sort_unstable();

        assert_eq!(unavailable, environmental);
        assert_eq!(unavailable, ["InstanceUnavailable"]);
    }
}
