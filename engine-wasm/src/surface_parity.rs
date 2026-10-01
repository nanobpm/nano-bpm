//! Compile-time parity gate between `engine-core`'s command surface and the
//! in-browser [`crate::TestEngine`] wrapper.
//!
//! # Why this exists
//!
//! `TestEngine` is a hand-written wasm-bindgen facade: each modeler operation
//! (`deploy`, `completeJob`, `activateJobs`, …) is a bespoke method that applies
//! one [`Command`] to the embedded engine. Because the mapping is manual, a new
//! engine capability — a new [`Command`] variant — can land in `engine-core`
//! and the wasm build will keep compiling and publishing *without* the modeler
//! ever gaining the feature. That is a silent surface gap, and a package version
//! bump can't catch it (the code compiles either way).
//!
//! [`classify`] closes that gap with the one lever the compiler can't ignore: an
//! **exhaustive `match` with no wildcard arm** over every [`Command`] variant.
//! When someone adds a variant to `engine-core`, this match stops being
//! exhaustive and the crate fails to compile (`E0004`) on the `wasm32` type-check
//! (`make engine-wasm-check`, run in CI). The author is then forced to make a
//! *conscious* decision at the exact PR that adds the capability: surface it in
//! `TestEngine` and mark it [`Surface::Surfaced`], or record *why* it is left out
//! with [`Surface::NotSurfaced`].
//!
//! This is deliberately a build-time sentinel: `classify` is never called at
//! runtime (hence `#![allow(dead_code)]`). Its whole value is that the match body
//! is *type-checked*, so the exhaustiveness error fires. `NotSurfaced` is not a
//! verdict of "never" — it means a human decided the test engine doesn't need it
//! *yet*; revisit when the modeler does.
//!
//! Note on *field*-level drift (a new field on an already-surfaced command, e.g.
//! `customHeaders` on an activated job): variant exhaustiveness does not catch
//! that. Where the wrapper serialises engine types straight to JSON
//! (snapshots/events) new fields flow through serde automatically; where it maps
//! fields by hand in `lib.rs`, destructure without `..` so a new field also
//! breaks the build.
//!
//! # The read surface
//!
//! [`classify`] guards the **write** surface ([`Command`]). Its twin
//! [`classify_read`] guards the **read** surface: an exhaustive, wildcard-free
//! `match` over [`ReadQuery`] — the gateway's read-model-backed REST reads,
//! defined in `engine-core` alongside `Command`. Now that `TestEngine` answers
//! reads from an embedded read model (`getFormByKey`, `searchUserTasks`,
//! `searchProcessInstances`, `getResourceByKey`, `searchVariables`), a newly
//! served read must be classified here too, or the `wasm32` type-check fails —
//! the same conscious-decision gate `Command` has always had.
#![allow(dead_code)]

use nanobpmn_engine_core::{Command, ReadQuery};

/// How a given [`Command`] variant relates to the `TestEngine` wasm surface.
pub(crate) enum Surface {
    /// Exposed to the modeler via the named `TestEngine` method(s) — the JS
    /// name(s). Usually a single method; when one command is surfaced through
    /// more than one JS entry point this is their names joined with `/`
    /// (e.g. `advanceTime/tickNow`, which both fan out to the same command).
    Surfaced { js_method: &'static str },
    /// Deliberately not exposed, with the rationale. Revisit if the modeler ever
    /// needs it — this is a "not yet / not here", never a hard "never".
    NotSurfaced { reason: &'static str },
}

/// Exhaustive, wildcard-free classification of every `engine-core` command.
///
/// Adding a `Command` variant to `engine-core` without extending this match is a
/// hard compile error (`E0004: non-exhaustive patterns`) — that is the guard.
pub(crate) fn classify(cmd: &Command) -> Surface {
    match cmd {
        // ---- Predominantly surfaced (arms in Command-enum declaration order):
        // most have a #[wasm_bindgen] method on TestEngine. Arms are grouped by
        // enum order — not strictly partitioned by verdict — so each arm's own
        // Surfaced/NotSurfaced below is the single authoritative source. ----
        Command::DeployResources(..) => Surface::Surfaced {
            js_method: "deploy",
        },
        // Forms and generic resources each have their own JS deploy entry point
        // (mirroring the native deploy decomposition, which splits one deployment
        // into DeployResources + DeployForms + DeployGenericResources). deployForm
        // populates the read model so getFormByKey resolves in-browser; deployResource
        // does the same for getResourceByKey (#815).
        Command::DeployForms(..) => Surface::Surfaced {
            js_method: "deployForm/getFormByKey",
        },
        Command::DeployGenericResources(..) => Surface::Surfaced {
            js_method: "deployResource/getResourceByKey",
        },
        Command::CreateInstance { .. } => Surface::Surfaced {
            js_method: "createInstance",
        },
        Command::CompleteJob { .. } => Surface::Surfaced {
            // Also the substrate for completeAgentJob (agentic JobResult).
            js_method: "completeJob",
        },
        Command::FailJob { .. } => Surface::Surfaced {
            js_method: "failJob",
        },
        Command::ThrowJobError { .. } => Surface::Surfaced {
            js_method: "throwError",
        },
        Command::UpdateJobRetries { .. } => Surface::Surfaced {
            js_method: "updateRetries",
        },
        Command::ResolveIncident { .. } => Surface::Surfaced {
            js_method: "resolveIncident",
        },
        Command::ActivateJobs { .. } => Surface::Surfaced {
            js_method: "activateJobs",
        },
        Command::ActivateJobsByKey { .. } => Surface::NotSurfaced {
            reason: "authoritative activation plans are host-internal rather than a public worker API",
        },
        Command::CorrelateMessage { .. } => Surface::Surfaced {
            js_method: "correlateMessage",
        },
        Command::BroadcastSignal { .. } => Surface::Surfaced {
            js_method: "broadcastSignal",
        },
        Command::SetVariables { .. } => Surface::Surfaced {
            js_method: "setVariables",
        },
        Command::CancelInstance { .. } => Surface::Surfaced {
            js_method: "cancelInstance",
        },
        Command::ModifyInstance { .. } => Surface::Surfaced {
            js_method: "modify",
        },
        Command::MigrateInstance { .. } => Surface::Surfaced {
            js_method: "migrate",
        },
        Command::AssignUserTask { .. } => Surface::Surfaced {
            js_method: "assignUserTask",
        },
        Command::UnassignUserTask { .. } => Surface::Surfaced {
            js_method: "unassignUserTask",
        },
        Command::UpdateUserTask { .. } => Surface::Surfaced {
            js_method: "updateUserTask",
        },
        Command::CompleteUserTask { .. } => Surface::Surfaced {
            js_method: "completeUserTask",
        },
        // The virtual clock drives both timer + job-deadline expiry; advanceTime
        // and tickNow fan out to TriggerTimers + ExpireJobs.
        Command::TriggerTimers { .. } => Surface::Surfaced {
            js_method: "advanceTime/tickNow",
        },
        Command::ExpireJobs { .. } => Surface::Surfaced {
            js_method: "advanceTime/tickNow",
        },

        // ---- Predominantly not surfaced (arms continue in Command-enum
        // declaration order): mostly conscious exclusions from the modeler test
        // engine, with a few surfaced commands (e.g. DeployDecisionRequirements,
        // UpdateJobTimeout) interleaved to preserve enum order. As above, each
        // arm's own Surfaced/NotSurfaced verdict is authoritative. ----
        Command::DeployProcess(..) => Surface::NotSurfaced {
            reason: "single-process deploy; the wrapper deploys via the DeployResources superset",
        },
        Command::DeployDecisionRequirements(..) => Surface::Surfaced {
            js_method: "deploy/deployDecision",
        },
        Command::DeleteDecisionInstance { .. } => Surface::NotSurfaced {
            reason: "audit-only read-model deletion; no core engine state, irrelevant in-browser",
        },
        Command::EvaluateDecision { .. } => Surface::Surfaced {
            js_method: "evaluateDecision",
        },
        Command::UpdateJobTimeout { .. } => Surface::Surfaced {
            js_method: "updateTimeout",
        },
        Command::UpdateJob { .. } => Surface::NotSurfaced {
            reason: "combined REST property update; the simulation exposes updateRetries and updateTimeout separately",
        },
        Command::ExpireJobsByDurability { .. } => Surface::NotSurfaced {
            reason: "worker-specific expiration scan; the simulation clock expires both lease modes via ExpireJobs",
        },
        Command::OpenMessageSubscription { .. } => Surface::NotSurfaced {
            reason: "internal subscription lifecycle the engine drives itself; not a user op",
        },
        Command::CorrelateMessageSubscription { .. } => Surface::NotSurfaced {
            reason: "internal subscription correlation; user-facing entry is correlateMessage",
        },
        Command::CloseMessageSubscription { .. } => Surface::NotSurfaced {
            reason: "internal subscription lifecycle the engine drives itself; not a user op",
        },
        Command::DispatchStartInstance { .. } => Surface::NotSurfaced {
            reason: "engine-internal message/timer start dispatch; instances start via createInstance",
        },
        Command::ActivateAdHocActivities { .. } => Surface::NotSurfaced {
            reason: "external ad-hoc activity activation is a server/REST seam; the studio wasm engine \
                 drives ad-hoc tools through the agent-job path, not this direct command",
        },
        // Process-instance suspend/resume is a server/REST seam (the gateway's
        // /process-instances/{key}/suspension + /resumption operations). The
        // in-browser studio engine has no suspend/resume driver — a future
        // console/Bojtos slice would add the `#[wasm_bindgen]` methods alongside
        // its own regenerated `pkg/` — so both stay NotSurfaced here.
        Command::SuspendInstance { .. } => Surface::NotSurfaced {
            reason: "process-instance suspend is a server/REST seam (POST /process-instances/{key}/suspension); \
                 the in-browser test engine exposes no suspend driver",
        },
        Command::ResumeInstance { .. } => Surface::NotSurfaced {
            reason: "process-instance resume is a server/REST seam (POST /process-instances/{key}/resumption); \
                 the in-browser test engine exposes no resume driver",
        },
        // AgentInstance write commands (engine-native AgentInstance parity). The
        // CREATE/UPDATE/COMPLETE lifecycle processors landed in S3; their
        // `#[wasm_bindgen]` TestEngine drivers land here in S6 alongside the
        // regenerated `pkg/` artifact, so each is now `Surfaced`.
        Command::CreateAgentInstance { .. } => Surface::Surfaced {
            js_method: "createAgentInstance",
        },
        Command::UpdateAgentInstance { .. } => Surface::Surfaced {
            js_method: "updateAgentInstance",
        },
        Command::CompleteAgentInstance { .. } => Surface::Surfaced {
            js_method: "completeAgentInstance",
        },
    }
}

/// Verdict for a read-model-backed read whose `#[wasm_bindgen]` getter lives
/// behind the off-by-default `read-model` feature.
///
/// It is [`Surface::Surfaced`] only in the `read-model` build — the build that
/// actually compiles the getter onto `TestEngine`; in the lean feature-off build
/// the getter is absent, so the read is *not* on the exported surface and the
/// verdict is [`Surface::NotSurfaced`]. This keeps [`classify_read`] honest to
/// what each build ships while leaving its exhaustiveness guard firing in both.
#[cfg(feature = "read-model")]
fn read_model_read(js_method: &'static str) -> Surface {
    Surface::Surfaced { js_method }
}

#[cfg(not(feature = "read-model"))]
fn read_model_read(_js_method: &'static str) -> Surface {
    Surface::NotSurfaced {
        reason: "read-model-backed getter compiled only under the `read-model` feature; \
                 absent from the lean feature-off build's exported surface",
    }
}

/// Exhaustive, wildcard-free classification of every gateway REST **read** the
/// read model serves ([`ReadQuery`]).
///
/// This is the read counterpart of [`classify`]. Adding a [`ReadQuery`] variant
/// in `engine-core` (i.e. the gateway begins serving a new read) without
/// extending this match is a hard compile error (`E0004: non-exhaustive
/// patterns`) — that is the guard. The author is then forced to decide, at the
/// exact PR that serves the read, whether `TestEngine` surfaces it (mapping it to
/// a `#[wasm_bindgen]` method, [`Surface::Surfaced`]) or records *why* it stays
/// out ([`Surface::NotSurfaced`]).
///
/// Like [`classify`], this is a build-time sentinel: never called at runtime, its
/// only value is that the match body is type-checked so the exhaustiveness error
/// fires. It references no `read-model` types, so it type-checks — and its
/// exhaustiveness guard fires — under *both* the lean feature-off and the
/// `read-model` `wasm32` builds, linking nothing new.
///
/// The `Surfaced` verdict is kept honest to each build: the five read-model reads
/// resolve through [`read_model_read`], which reports [`Surface::Surfaced`] only
/// in the `read-model` build (where the `#[wasm_bindgen]` getter is actually
/// compiled) and [`Surface::NotSurfaced`] in the lean build (where it is absent).
pub(crate) fn classify_read(query: &ReadQuery) -> Surface {
    match query {
        // ---- Surfaced only in the `read-model` build: each has a
        // #[wasm_bindgen] read method on TestEngine compiled behind the
        // `read-model` feature (see `read_model_read`). ----
        ReadQuery::GetFormByKey => read_model_read("getFormByKey"),
        ReadQuery::GetResourceByKey => read_model_read("getResourceByKey"),
        ReadQuery::SearchProcessInstances => read_model_read("searchProcessInstances"),
        ReadQuery::SearchUserTasks => read_model_read("searchUserTasks"),
        ReadQuery::SearchVariables => read_model_read("searchVariables"),

        // ---- Not surfaced: conscious exclusions from the modeler test engine.
        // Revisit each if the modeler grows a need for it; the read model already
        // holds the data, so surfacing is a mechanical addition of a
        // #[wasm_bindgen] getter, not new engine work. ----
        ReadQuery::SearchResources => Surface::NotSurfaced {
            reason: "resource-metadata listing; the modeler resolves resources by key \
                     (getResourceByKey), not by browsing the deployed catalogue",
        },
        ReadQuery::GetProcessInstance => Surface::NotSurfaced {
            reason: "single-instance lookup by key; the modeler drives one simulated instance it \
                     already holds the key for and reads it via the debug snapshot",
        },
        ReadQuery::GetUserTask => Surface::NotSurfaced {
            reason: "single user-task lookup by key; the modeler enumerates via searchUserTasks",
        },
        ReadQuery::GetVariable => Surface::NotSurfaced {
            reason: "single-variable lookup by key; the modeler enumerates via searchVariables",
        },
        ReadQuery::SearchJobs => Surface::NotSurfaced {
            reason: "job inventory listing; the modeler activates/completes jobs directly \
                     (activateJobs/completeJob) rather than browsing the job table",
        },
        ReadQuery::SearchIncidents => Surface::NotSurfaced {
            reason: "incident listing; incidents surface inline in the simulation's debug state, \
                     and are resolved via resolveIncident",
        },
        ReadQuery::GetIncident => Surface::NotSurfaced {
            reason: "single-incident lookup by key; not needed by the in-browser test engine",
        },
        ReadQuery::SearchElementInstances => Surface::NotSurfaced {
            reason: "flow-node instance listing; the modeler reads element activation from the \
                     debug snapshot/event stream, not this read-model query",
        },
        ReadQuery::GetElementInstance => Surface::NotSurfaced {
            reason: "single element-instance lookup by key; covered by the debug snapshot",
        },
        ReadQuery::SearchElementInstanceIncidents => Surface::NotSurfaced {
            reason: "element-instance-scoped incident subtree query; incidents surface inline in \
                     the simulation's debug state (and via searchIncidents), so the modeler has no \
                     need to scope them by element-instance subtree",
        },
        ReadQuery::SearchElementInstanceWaitStates => Surface::NotSurfaced {
            reason: "element-instance wait-state listing; the modeler reads wait/activation state \
                     from the debug snapshot/event stream, not this composite read-model query",
        },
        ReadQuery::SearchMessageSubscriptions => Surface::NotSurfaced {
            reason:
                "subscription inventory; the modeler correlates via correlateMessage and reads \
                     subscription state from the debug snapshot",
        },
        ReadQuery::SearchCorrelatedMessageSubscriptions => Surface::NotSurfaced {
            reason: "audit-oriented correlated-subscription listing; not needed in-browser",
        },
        ReadQuery::SearchProcessDefinitions => Surface::NotSurfaced {
            reason: "deployment catalogue listing; the modeler deploys and drives a single known \
                     definition rather than browsing the catalogue",
        },
        ReadQuery::GetProcessDefinitionXml => Surface::NotSurfaced {
            reason: "the modeler already holds the BPMN XML it deployed; no need to read it back",
        },
        ReadQuery::SearchDecisionInstances => Surface::NotSurfaced {
            reason: "DMN evaluation history; the in-browser test engine exercises BPMN execution \
                     only",
        },
        ReadQuery::GetDecisionInstance => Surface::NotSurfaced {
            reason: "single DMN evaluation lookup; the in-browser test engine exercises BPMN only",
        },
        ReadQuery::SearchDecisionDefinitions => Surface::NotSurfaced {
            reason: "DMN definition catalogue; the in-browser test engine exercises BPMN only",
        },
        ReadQuery::GetDecisionDefinitionXml => Surface::NotSurfaced {
            reason: "DMN definition XML; the in-browser test engine exercises BPMN only",
        },
        ReadQuery::SearchDecisionRequirements => Surface::NotSurfaced {
            reason: "DMN requirements-graph catalogue; the in-browser test engine exercises BPMN \
                     only",
        },
        ReadQuery::GetDecisionRequirementsXml => Surface::NotSurfaced {
            reason: "DMN requirements-graph XML; the in-browser test engine exercises BPMN only",
        },

        // ---- AgentInstance / AgentHistory reads (Camunda 8.10 parity). The two
        // search surfaces are surfaced through the read-model build so the wasm
        // read-model TestEngine can query the projected agent state; the
        // single-key get mirrors the other single-lookup gets (GetProcessInstance)
        // and stays out — the modeler enumerates via searchAgentInstances. ----
        ReadQuery::SearchAgentInstances => read_model_read("searchAgentInstances"),
        ReadQuery::SearchAgentHistory => read_model_read("searchAgentInstanceHistory"),
        ReadQuery::GetAgentInstance => Surface::NotSurfaced {
            reason: "single AgentInstance lookup by key; the modeler enumerates via \
                     searchAgentInstances (mirrors GetProcessInstance/GetUserTask)",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A representative surfaced + not-surfaced pair, to exercise `classify` and
    // pin the intent. The real guard is the exhaustiveness of the match above,
    // enforced by the wasm32 type-check in CI — this test just validates that the
    // classification wiring is sound.
    #[test]
    fn classifies_known_commands() {
        assert!(matches!(
            classify(&Command::ExpireJobs { now: 0 }),
            Surface::Surfaced { .. }
        ));
        assert!(matches!(
            classify(&Command::UpdateJobTimeout {
                lease_token: None,
                job_key: 0,
                timeout: 0,
                operation_reference: None,
            }),
            Surface::Surfaced { .. }
        ));
    }

    // Forms and generic resources are surfaced via their own JS deploy entry
    // points (deployForm/deployResource), mirroring the native deploy
    // decomposition, so getFormByKey/getResourceByKey resolve in-browser (#815).
    #[test]
    fn deploy_forms_and_resources_are_surfaced() {
        assert!(matches!(
            classify(&Command::DeployForms(Vec::new())),
            Surface::Surfaced { .. }
        ));
        assert!(matches!(
            classify(&Command::DeployGenericResources(Vec::new())),
            Surface::Surfaced { .. }
        ));
    }

    // Read counterpart: a representative surfaced + not-surfaced pair over the
    // REST read surface. The real guard is the exhaustiveness of `classify_read`.
    // A read-model-backed read is only `Surfaced` in the `read-model` build,
    // where its getter is compiled; in the lean build it is `NotSurfaced`.
    #[test]
    fn classifies_known_reads() {
        #[cfg(feature = "read-model")]
        assert!(matches!(
            classify_read(&ReadQuery::GetFormByKey),
            Surface::Surfaced { .. }
        ));
        #[cfg(not(feature = "read-model"))]
        assert!(matches!(
            classify_read(&ReadQuery::GetFormByKey),
            Surface::NotSurfaced { .. }
        ));
        assert!(matches!(
            classify_read(&ReadQuery::SearchDecisionInstances),
            Surface::NotSurfaced { .. }
        ));
    }
}
