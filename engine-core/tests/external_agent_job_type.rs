#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::HashMap;

use nanobpmn_engine_core::{
    bpmn::parse_bpmn, AgentDefinition, AgentType, Command, ElementKind, Engine, EngineError, Event,
    Value,
};

const MODEL: &str = include_str!("fixtures/external-agent-job-type.bpmn");

#[test]
fn agent_markers_are_metadata_on_ordinary_service_tasks() {
    for marker in ["external", "aiAgentTask"] {
        let xml = MODEL.replace("agentType=\"external\"", &format!("agentType=\"{marker}\""));
        let def = parse_bpmn(&xml).unwrap().remove(0);
        assert!(
            matches!(def.elements["agent"].kind, ElementKind::ServiceTask { .. }),
            "{marker} must not replace the ordinary job-worker element"
        );
    }
}

#[test]
fn agent_markers_preserve_the_entire_worker_job_contract() {
    use nanobpmn_engine_core::GenericResource;
    for marker in ["", "external", "aiAgentTask"] {
        let marker_xml = if marker.is_empty() {
            String::new()
        } else {
            format!("<zeebe:agentDefinition agentType=\"{marker}\"/>")
        };
        let xml = MODEL
            .replace(
                "<zeebe:agentDefinition agentType=\"external\"/>",
                &marker_xml,
            )
            .replace(
                "<zeebe:taskDefinition type=\"senior:rebase\"/>",
                r#"<zeebe:taskDefinition type="senior:rebase" retries="= retries"/>
                <zeebe:priorityDefinition priority="= priority"/>
                <zeebe:taskHeaders><zeebe:header key="channel" value="agent"/></zeebe:taskHeaders>
                <zeebe:linkedResources>
                  <zeebe:linkedResource resourceId="prompt.md" bindingType="latest"
                    resourceType="GenericScript" linkName="prompt"/>
                </zeebe:linkedResources>"#,
            );
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(parse_bpmn(&xml).unwrap().remove(0)))
            .unwrap();
        for content in ["first", "latest"] {
            engine
                .apply_command(Command::DeployGenericResources(vec![GenericResource {
                    resource_id: "prompt.md".into(),
                    resource_name: "prompt.md".into(),
                    content: content.into(),
                }]))
                .unwrap();
        }
        let latest_key = engine.state().resources["prompt.md"].key;
        let events = engine
            .apply_command(Command::create_instance_with(
                "external-agent-routing",
                HashMap::from([
                    ("route".into(), Value::Str("senior:rebase".into())),
                    ("retries".into(), Value::Int(7)),
                    ("priority".into(), Value::Int(42)),
                ]),
            ))
            .unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Event::AgentInstanceCreated { .. })),
            "{marker}: agent registration belongs to the worker"
        );
        let job = engine
            .activate_jobs("senior:rebase", "W", 1, 1_000, 0)
            .pop()
            .unwrap_or_else(|| panic!("{marker}: a normal worker job must be created"));
        assert_eq!(job.retries, 7, "{marker}");
        assert_eq!(job.priority, 42, "{marker}");
        assert_eq!(
            job.custom_headers.get("channel").map(String::as_str),
            Some("agent"),
            "{marker}"
        );
        let resources: serde_json::Value = serde_json::from_str(
            job.custom_headers
                .get("linkedResources")
                .expect("prompt resource header"),
        )
        .unwrap();
        assert_eq!(
            resources[0]["resourceKey"],
            latest_key.to_string(),
            "{marker}"
        );
        assert_eq!(resources[0]["linkName"], "prompt");
        assert_eq!(
            job.lease_token, None,
            "{marker}: leasing is an activation option, not classification"
        );
    }
}

#[test]
fn parser_preserves_agent_job_type_independently_of_extension_order() {
    for job_type in [Some("senior:rebase"), Some("= localRoute"), None] {
        let declaration = job_type
            .map(|value| format!("<zeebe:taskDefinition type=\"{value}\"/>"))
            .unwrap_or_default();
        let xml = MODEL.replace("<zeebe:taskDefinition type=\"senior:rebase\"/>", "");
        for marker in [
            format!("{declaration}<zeebe:agentDefinition agentType=\"external\"/>"),
            format!("<zeebe:agentDefinition agentType=\"external\"/>{declaration}"),
        ] {
            let xml = xml.replace("<zeebe:agentDefinition agentType=\"external\"/>", &marker);
            let def = parse_bpmn(&xml).unwrap().remove(0);
            assert!(matches!(
                &def.elements["agent"].kind,
                ElementKind::ServiceTask { agent_type: Some(AgentType::External), job_type: actual, .. }
                    if actual == job_type.unwrap_or("agent")
            ));
        }
    }
}

#[test]
fn ai_agent_task_uses_the_same_worker_registered_lifecycle() {
    let xml = MODEL.replace("agentType=\"external\"", "agentType=\"aiAgentTask\"");
    assert_external_job_lifecycle(&xml, "senior:rebase");
}

#[test]
fn ad_hoc_agent_markers_use_the_same_worker_registered_lifecycle() {
    for marker in ["external", "aiAgentSubProcess"] {
        let xml = MODEL
            .replace("bpmn:serviceTask", "bpmn:adHocSubProcess")
            .replace(
                "</bpmn:adHocSubProcess>",
                "<bpmn:serviceTask id=\"tool\"/></bpmn:adHocSubProcess>",
            )
            .replace(
                "</bpmndi:BPMNPlane>",
                r#"<bpmndi:BPMNShape id="tool_di" bpmnElement="tool">
                <dc:Bounds x="210" y="88" width="80" height="60"/>
              </bpmndi:BPMNShape></bpmndi:BPMNPlane>"#,
            )
            .replace("agentType=\"external\"", &format!("agentType=\"{marker}\""));
        assert_external_job_lifecycle(&xml, "senior:rebase");
    }
}

#[test]
fn multi_instance_agent_tasks_keep_per_child_jobs_and_leases() {
    for marker in ["external", "aiAgentTask"] {
        let xml = MODEL
            .replace("agentType=\"external\"", &format!("agentType=\"{marker}\""))
            .replace(
                "</bpmn:serviceTask>",
                r#"<bpmn:multiInstanceLoopCharacteristics>
                <bpmn:extensionElements>
                  <zeebe:loopCharacteristics inputCollection="= [1,2]" inputElement="item"/>
                </bpmn:extensionElements>
              </bpmn:multiInstanceLoopCharacteristics></bpmn:serviceTask>"#,
            );
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(parse_bpmn(&xml).unwrap().remove(0)))
            .unwrap();
        engine
            .apply_command(Command::create_instance_with(
                "external-agent-routing",
                HashMap::from([("route".into(), Value::Str("senior:rebase".into()))]),
            ))
            .unwrap();
        let jobs = engine.activate_jobs_with_options(
            "senior:rebase",
            "W",
            10,
            1_000,
            0,
            nanobpmn_engine_core::JobActivationOptions {
                with_lease: true,
                ..Default::default()
            },
        );
        assert_eq!(jobs.len(), 2, "{marker}");
        assert_ne!(jobs[0].element_instance_key, jobs[1].element_instance_key);
        assert_ne!(jobs[0].lease_token, jobs[1].lease_token);
        for job in &jobs {
            engine
                .apply_command(Command::CreateAgentInstance {
                    element_instance_key: job.element_instance_key,
                    job_key: job.key,
                    job_lease: job.lease_token.clone().unwrap(),
                    definition: AgentDefinition::default(),
                    limits: None,
                    history: vec![],
                })
                .unwrap();
        }
        let mut events = Vec::new();
        for job in jobs {
            events.extend(
                engine
                    .apply_command(
                        Command::complete_job(job.key).with_job_lease(job.lease_token.unwrap()),
                    )
                    .unwrap(),
            );
        }
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCompleted { .. })));
    }
}

fn assert_external_job_lifecycle(xml: &str, expected_type: &str) {
    let def = parse_bpmn(xml).unwrap().remove(0);
    let ElementKind::ServiceTask {
        agent_type: Some(expected_agent_type),
        ..
    } = def.elements["agent"].kind
    else {
        panic!("expected marked service task")
    };
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "external-agent-routing",
            HashMap::from([("route".into(), Value::Str("senior:rebase".into()))]),
        ))
        .unwrap();
    let (job_key, element_instance_key) = events
        .iter()
        .find_map(|event| match event {
            Event::JobCreated {
                job_key,
                element_instance_key,
                job_type,
                ..
            } => {
                assert_eq!(job_type, expected_type);
                Some((*job_key, *element_instance_key))
            }
            _ => None,
        })
        .expect("external agent creates a normal job");
    assert!(!events
        .iter()
        .any(|event| matches!(event, Event::AgentInstanceCreated { .. })));

    let create_agent = |job_lease| Command::CreateAgentInstance {
        element_instance_key,
        job_key,
        job_lease,
        definition: AgentDefinition::default(),
        limits: None,
        history: vec![],
    };
    assert!(matches!(
        engine
            .apply_command(create_agent(String::new()))
            .unwrap_err(),
        EngineError::AgentInstanceJobNotActive { .. }
    ));
    if expected_type != "agent" {
        assert!(engine.activate_jobs("agent", "W", 1, 1_000, 0).is_empty());
    }
    let job = engine
        .activate_jobs_with_options(
            expected_type,
            "W",
            1,
            1_000,
            0,
            nanobpmn_engine_core::JobActivationOptions {
                with_lease: true,
                ..Default::default()
            },
        )
        .pop()
        .expect("worker subscribed by job type activates the agent");
    assert_eq!(job.key, job_key);
    let lease = job.lease_token.expect("external job is lease-gated");
    assert!(matches!(
        engine
            .apply_command(create_agent("stale-lease".to_string()))
            .unwrap_err(),
        EngineError::AgentInstanceJobLeaseMismatch { .. }
    ));
    let events = engine.apply_command(create_agent(lease.clone())).unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        Event::AgentInstanceCreated { agent_instance, .. }
            if agent_instance.agent_type == expected_agent_type
                && agent_instance.job_key == job_key
                && agent_instance.job_lease == lease
    )));
    let events = engine
        .apply_command(Command::complete_job(job_key).with_job_lease(lease))
        .unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event, Event::ProcessInstanceCompleted { .. })));
}

#[test]
fn external_agent_uses_declared_job_type() {
    assert_external_job_lifecycle(MODEL, "senior:rebase");
}

#[test]
fn external_agent_without_task_definition_uses_element_id() {
    let xml = MODEL.replace("<zeebe:taskDefinition type=\"senior:rebase\"/>", "");
    assert_external_job_lifecycle(&xml, "agent");
}

#[test]
fn external_agent_job_type_expression_uses_input_mapping_scope() {
    let xml = MODEL.replace("type=\"senior:rebase\"", "type=\"= localRoute\"");
    assert_external_job_lifecycle(&xml, "senior:rebase");
}

#[test]
fn job_type_expressions_see_task_inputs_with_and_without_start_listeners() {
    for external in [false, true] {
        for listener in [false, true] {
            let mut xml = MODEL.replace("type=\"senior:rebase\"", "type=\"= localRoute\"");
            if !external {
                xml = xml.replace("<zeebe:agentDefinition agentType=\"external\"/>", "");
            }
            if listener {
                xml = xml.replace(
                    "<zeebe:ioMapping>",
                    r#"<zeebe:executionListeners>
                         <zeebe:executionListener eventType="start" type="before"/>
                       </zeebe:executionListeners><zeebe:ioMapping>"#,
                );
            }
            let def = parse_bpmn(&xml).unwrap().remove(0);
            let mut engine = Engine::new();
            engine.apply_command(Command::DeployProcess(def)).unwrap();
            engine
                .apply_command(Command::create_instance_with(
                    "external-agent-routing",
                    HashMap::from([
                        ("route".into(), Value::Str("senior:rebase".into())),
                        ("localRoute".into(), Value::Str("wrong-parent-route".into())),
                    ]),
                ))
                .unwrap();
            if listener {
                let job = engine
                    .activate_jobs("before", "W", 1, 1_000, 0)
                    .pop()
                    .unwrap();
                assert!(
                    job.lease_token.is_none(),
                    "execution listeners are not agent jobs"
                );
                engine
                    .apply_command(Command::complete_job(job.key))
                    .unwrap();
            }
            assert_eq!(
                engine
                    .activate_jobs("senior:rebase", "W", 1, 1_000, 0)
                    .len(),
                1,
                "external={external}, start_listener={listener}"
            );
        }
    }
}
