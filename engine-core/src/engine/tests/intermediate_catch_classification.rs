//! #2076: one shared intermediate-catch classification. Gateway subscription
//! opening (`ProcessDefinition::event_gateway_targets`) and legacy-race
//! detection (`engine::is_intermediate_catch`, used by
//! `event_gateway_race_index`) must agree on every `ElementKind`.
use super::*;

/// Exhaustive oracle (no wildcard): a new `ElementKind` variant fails to
/// compile here until its expected classification is decided. Returns the
/// variant's expected `is_intermediate_catch` value.
fn expected_catch(kind: &ElementKind) -> bool {
    match kind {
        ElementKind::TimerIntermediateCatchEvent { .. }
        | ElementKind::MessageIntermediateCatchEvent { .. }
        | ElementKind::SignalIntermediateCatchEvent { .. }
        | ElementKind::ConditionalIntermediateCatchEvent { .. } => true,
        ElementKind::StartEvent
        | ElementKind::EndEvent
        | ElementKind::TerminateEndEvent
        | ElementKind::ErrorEndEvent { .. }
        | ElementKind::ServiceTask { .. }
        | ElementKind::BusinessRuleTask { .. }
        | ElementKind::UserTask(_)
        | ElementKind::ExclusiveGateway
        | ElementKind::ParallelGateway
        | ElementKind::InclusiveGateway
        | ElementKind::EventBasedGateway
        | ElementKind::ErrorBoundaryEvent { .. }
        | ElementKind::TimerBoundaryEvent { .. }
        | ElementKind::MessageBoundaryEvent { .. }
        | ElementKind::MessageStartEvent { .. }
        | ElementKind::TimerStartEvent { .. }
        | ElementKind::ConditionalStartEvent { .. }
        | ElementKind::SubProcess { .. }
        | ElementKind::EventSubProcess { .. }
        | ElementKind::EscalationThrowEvent { .. }
        | ElementKind::EscalationBoundaryEvent { .. }
        | ElementKind::IntermediateThrowEvent
        | ElementKind::LinkIntermediateThrowEvent { .. }
        | ElementKind::LinkIntermediateCatchEvent { .. }
        | ElementKind::Task
        | ElementKind::ScriptTask { .. }
        | ElementKind::CallActivity { .. }
        | ElementKind::SignalBoundaryEvent { .. }
        | ElementKind::ConditionalBoundaryEvent { .. }
        | ElementKind::CompensationBoundaryEvent { .. }
        | ElementKind::CompensationThrowEvent
        | ElementKind::AgentTask { .. } => false,
    }
}

/// Stable per-variant index so the test can prove every variant was sampled.
fn variant_index(kind: &ElementKind) -> usize {
    match kind {
        ElementKind::StartEvent => 0,
        ElementKind::EndEvent => 1,
        ElementKind::TerminateEndEvent => 2,
        ElementKind::ErrorEndEvent { .. } => 3,
        ElementKind::ServiceTask { .. } => 4,
        ElementKind::BusinessRuleTask { .. } => 5,
        ElementKind::UserTask(_) => 6,
        ElementKind::ExclusiveGateway => 7,
        ElementKind::ParallelGateway => 8,
        ElementKind::InclusiveGateway => 9,
        ElementKind::EventBasedGateway => 10,
        ElementKind::ErrorBoundaryEvent { .. } => 11,
        ElementKind::TimerIntermediateCatchEvent { .. } => 12,
        ElementKind::TimerBoundaryEvent { .. } => 13,
        ElementKind::MessageIntermediateCatchEvent { .. } => 14,
        ElementKind::MessageBoundaryEvent { .. } => 15,
        ElementKind::MessageStartEvent { .. } => 16,
        ElementKind::TimerStartEvent { .. } => 17,
        ElementKind::ConditionalStartEvent { .. } => 18,
        ElementKind::SubProcess { .. } => 19,
        ElementKind::EventSubProcess { .. } => 20,
        ElementKind::EscalationThrowEvent { .. } => 21,
        ElementKind::EscalationBoundaryEvent { .. } => 22,
        ElementKind::IntermediateThrowEvent => 23,
        ElementKind::LinkIntermediateThrowEvent { .. } => 24,
        ElementKind::LinkIntermediateCatchEvent { .. } => 25,
        ElementKind::Task => 26,
        ElementKind::ScriptTask { .. } => 27,
        ElementKind::CallActivity { .. } => 28,
        ElementKind::SignalIntermediateCatchEvent { .. } => 29,
        ElementKind::SignalBoundaryEvent { .. } => 30,
        ElementKind::ConditionalIntermediateCatchEvent { .. } => 31,
        ElementKind::ConditionalBoundaryEvent { .. } => 32,
        ElementKind::CompensationBoundaryEvent { .. } => 33,
        ElementKind::CompensationThrowEvent => 34,
        ElementKind::AgentTask { .. } => 35,
    }
}
const VARIANT_COUNT: usize = 36;

/// A definition holding one element of every `ElementKind` variant, with an
/// event-based gateway `gw` flowing into each of them.
fn every_kind() -> ProcessDefinition {
    let b = ProcessBuilder::new("kinds")
        .start_event("start")
        .end_event("end")
        .terminate_end_event("term")
        .error_end_event("errEnd", "E")
        .service_task("svc", "job")
        .business_rule_task("brt", "dec", None)
        .user_task("ut")
        .exclusive_gateway("xor")
        .parallel_gateway("and")
        .inclusive_gateway("or")
        .event_based_gateway("gw")
        .error_boundary_event("errB", "svc", "E")
        .timer_intermediate_catch_event("timerC", 1000)
        .timer_boundary_event("timerB", "svc", 1000)
        .message_intermediate_catch_event("msgC", "M", "=k")
        .message_boundary_event("msgB", "svc", "M", "=k")
        .message_start_event("msgS", "M")
        .timer_start_event_once("timerS", 1000)
        .conditional_start_event("condS", "=x")
        .sub_process("sub", "subStart")
        .start_event("subStart")
        .contained_in("subStart", "sub")
        .event_sub_process(
            "esp",
            "espStart",
            crate::EventSubProcessTrigger::Signal {
                signal_name: "S".into(),
            },
            true,
        )
        .start_event("espStart")
        .contained_in("espStart", "esp")
        .escalation_throw_event("escT", "X")
        .escalation_boundary_event("escB", "svc", "X")
        .intermediate_throw_event("throw")
        .link_intermediate_throw_event("linkT", "L")
        .link_intermediate_catch_event("linkC", "L")
        .task("task")
        .script_task("script", "=1", "r")
        .call_activity("call", "other")
        .signal_intermediate_catch_event("sigC", "S")
        .signal_boundary_event("sigB", "svc", "S")
        .conditional_intermediate_catch_event("condC", "=x")
        .conditional_boundary_event("condB", "svc", "=x")
        .compensation_boundary_event("compB", "svc", "task")
        .compensation_throw_event("compT")
        .agent_task("agent", "job", crate::agent::AgentType::AiAgentTask)
        .connect("start", "gw");
    let mut def = b.build().unwrap();
    // `agent_task` builds the normalized service-task shape; the legacy
    // `AgentTask` variant is only reachable from old snapshots, so set it
    // directly to sample it.
    def.elements.get_mut("agent").unwrap().kind = ElementKind::AgentTask {
        agent_type: crate::agent::AgentType::AiAgentTask,
        job_type: Some("job".into()),
        definition: Default::default(),
        limits: None,
    };
    let ids: Vec<String> = def
        .elements
        .keys()
        .filter(|id| id.as_str() != "gw")
        .cloned()
        .collect();
    let gw = def.elements.get_mut("gw").unwrap();
    for to in ids {
        gw.outgoing.push(crate::model::SequenceFlow {
            to,
            condition: None,
            is_default: false,
        });
    }
    def
}

#[test]
fn intermediate_catch_classification_is_shared_by_subscriptions_and_race_detection() {
    let def = every_kind();
    let mut seen = [false; VARIANT_COUNT];
    let targets: Vec<&str> = def
        .event_gateway_targets("gw")
        .iter()
        .map(|e| e.id.as_str())
        .collect();
    for element in def.elements.values() {
        let expected = expected_catch(&element.kind);
        seen[variant_index(&element.kind)] = true;
        assert_eq!(
            element.kind.is_intermediate_catch(),
            expected,
            "{}",
            element.id
        );
        assert_eq!(
            crate::engine::is_intermediate_catch(&def, &element.id),
            expected,
            "race detection disagrees on {}",
            element.id
        );
        if element.id != "gw" {
            assert_eq!(
                targets.contains(&element.id.as_str()),
                expected,
                "gateway subscriptions disagree on {}",
                element.id
            );
        }
    }
    assert!(seen.iter().all(|s| *s), "unsampled variants: {seen:?}");
}
