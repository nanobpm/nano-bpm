#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use nanobpmn_engine_core::{
    AgentType, Command, Engine, Event, JobActivationOptions, ProcessBuilder,
};
use serde_json::{json, Value};

fn base_engine(marker: AgentType) -> Engine {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(
            ProcessBuilder::new("agent-current")
                .start_event("start")
                .agent_task("agent", "worker", marker)
                .end_event("end")
                .connect("start", "agent")
                .connect("agent", "end")
                .build()
                .unwrap(),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance("agent-current"))
        .unwrap();
    engine
}

fn fixture() -> (Engine, u64, u64, String) {
    let mut engine = base_engine(AgentType::AiAgentTask);
    let job = engine
        .activate_jobs_with_options(
            "worker",
            "worker-a",
            1,
            1_000,
            0,
            JobActivationOptions {
                with_lease: true,
                ..Default::default()
            },
        )
        .pop()
        .unwrap();
    (
        engine,
        job.element_instance_key,
        job.key,
        job.lease_token.unwrap(),
    )
}

fn turn(id: &str, role: &str) -> Value {
    json!({"history_item_id":id,"loop_iteration":1,"produced_at":1,"role":role})
}

fn text_prompt(text: &str) -> Value {
    json!([{"content_type":"Text","text":text,"document_reference":null,"object":null}])
}

fn configuration() -> Vec<Value> {
    let mut model = turn("config-model", "Configuration");
    model["model"] = json!("test-model");
    let mut provider = turn("config-provider", "Configuration");
    provider["provider"] = json!("test-provider");
    let mut prompt = turn("config-prompt", "Configuration");
    prompt["system_prompt"] = text_prompt("test-prompt");
    vec![model, provider, prompt]
}

fn create(eik: u64, job: u64, lease: &str, history: Vec<Value>) -> Command {
    serde_json::from_value(json!({"CreateAgentInstance":{
        "element_instance_key":eik,"job_key":job,"job_lease":lease,
        "definition":{},"history":history
    }}))
    .unwrap()
}

fn agent(events: &[Event]) -> u64 {
    events
        .iter()
        .find_map(|event| match event {
            Event::AgentInstanceCreated { agent_instance, .. } => {
                Some(agent_instance.agent_instance_key)
            }
            _ => None,
        })
        .unwrap()
}

fn update(aik: u64, eik: u64, pi: u64, job: u64, lease: &str, history: Vec<Value>) -> Command {
    serde_json::from_value(json!({"UpdateAgentInstance":{
        "agent_instance_key":aik,"element_instance_key":eik,"element_id":"agent",
        "process_instance_key":pi,"job_key":job,"job_lease":lease,
        "status":null,"metrics":{},"tools":null,"history":history
    }}))
    .unwrap()
}

#[test]
fn replicated_agent_history_needs_durable_activation_even_without_a_lease() {
    for marker in [
        AgentType::AiAgentTask,
        AgentType::External,
        AgentType::AiAgentSubProcess,
    ] {
        for with_lease in [false, true] {
            let mut leader = base_engine(marker);
            let mut follower = Engine::from_snapshot(leader.snapshot());
            let activation = Command::activate_jobs_with_options(
                "worker",
                "worker-a",
                1,
                1_000,
                0,
                JobActivationOptions {
                    with_lease,
                    ..Default::default()
                },
            );
            let activated = leader.apply_command(activation.clone()).unwrap();
            let job = leader
                .activated_job(
                    activated
                        .iter()
                        .find_map(|event| match event {
                            Event::JobActivated { job_key, .. } => Some(*job_key),
                            _ => None,
                        })
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(job.lease_token.is_some(), with_lease);
            assert!(leader.job_supports_agent_instance(job.key));
            let attribution = job.lease_token.as_deref().unwrap_or("unleased-attribution");
            let registration = create(
                job.element_instance_key,
                job.key,
                attribution,
                configuration(),
            );
            let before = serde_json::to_value(follower.snapshot()).unwrap();
            assert!(matches!(
                follower.apply_command(registration.clone()),
                Err(nanobpmn_engine_core::EngineError::AgentInstanceJobNotActive { .. })
            ));
            assert_eq!(serde_json::to_value(follower.snapshot()).unwrap(), before);
            assert_eq!(follower.apply_command(activation).unwrap(), activated);
            let registered = leader.apply_command(registration.clone()).unwrap();
            assert_eq!(follower.apply_command(registration).unwrap(), registered);
            assert_eq!(leader.state(), follower.state());
            let aik = agent(&registered);
            let pi = leader.job(job.key).unwrap().instance_key;
            let mut restored = Engine::from_snapshot(follower.snapshot());
            let history = update(
                aik,
                job.element_instance_key,
                pi,
                job.key,
                attribution,
                vec![turn("after-failover", "Assistant")],
            );
            assert_eq!(
                leader.apply_command(history.clone()).unwrap(),
                restored.apply_command(history).unwrap()
            );
            let completion =
                Command::complete_job(job.key).with_lease_token(job.lease_token.clone());
            assert_eq!(
                leader.apply_command(completion.clone()).unwrap(),
                restored.apply_command(completion).unwrap()
            );
            assert_eq!(leader.state(), restored.state());
        }
    }
}

#[test]
fn history_preserves_positive_int32_loop_iterations_and_rejects_overflow() {
    let (mut engine, eik, job, lease) = fixture();
    let mut history = configuration();
    for turn in &mut history {
        turn["loop_iteration"] = json!(i32::MAX);
    }
    let events = engine
        .apply_command(create(eik, job, &lease, history))
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::AgentHistoryCreated { .. }))
            .count(),
        3
    );
    for event in events
        .iter()
        .filter(|event| matches!(event, Event::AgentHistoryCreated { .. }))
    {
        let Event::AgentHistoryCreated { record, .. } = event else {
            unreachable!()
        };
        assert_eq!(
            serde_json::to_value(record.loop_iteration).unwrap(),
            json!(i32::MAX)
        );
        let decoded =
            nanobpmn_engine_core::decode_event_json(&serde_json::to_string(event).unwrap())
                .unwrap();
        assert_eq!(&decoded, event);
    }
    let snapshot = serde_json::to_value(engine.snapshot()).unwrap();
    let restored = Engine::from_snapshot(serde_json::from_value(snapshot).unwrap());
    assert_eq!(restored.state(), engine.state());
    let mut oversized = turn("oversized", "User");
    oversized["loop_iteration"] = json!(i64::from(i32::MAX) + 1);
    assert!(serde_json::from_value::<nanobpmn_engine_core::AgentHistoryTurn>(oversized).is_err());
}

#[test]
fn history_metrics_preserve_object_and_counter_presence() {
    for metrics in [
        None,
        Some(Value::Null),
        Some(json!({"input_tokens":2,"output_tokens":null,"duration_ms":7})),
        Some(json!({"input_tokens":null,"output_tokens":null,"duration_ms":null})),
        Some(json!({"input_tokens":-1,"output_tokens":-1,"duration_ms":-1})),
        Some(json!({"input_tokens":0,"output_tokens":0,"duration_ms":0})),
        Some(json!({"input_tokens":8,"output_tokens":3,"duration_ms":7,
            "reasoning_token_count":2,"cache_creation_token_count":1,"cache_read_token_count":4})),
    ] {
        let mut payload = turn("observed", "Assistant");
        let expected = if let Some(Value::Object(fields)) = &metrics {
            let mut value = json!({"input_tokens":null,"output_tokens":null,"duration_ms":null,
                "reasoning_token_count":null,"cache_creation_token_count":null,"cache_read_token_count":null});
            value.as_object_mut().unwrap().extend(fields.clone());
            value
        } else {
            Value::Null
        };
        if let Some(metrics) = metrics {
            payload["metrics"] = metrics;
        }
        let history: nanobpmn_engine_core::AgentHistoryTurn =
            serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(serde_json::to_value(history.metrics).unwrap(), expected);
        let (mut engine, eik, job, lease) = fixture();
        let mut batch = configuration();
        batch.push(payload);
        let events = engine
            .apply_command(create(eik, job, &lease, batch))
            .unwrap();
        let stored = events
            .iter()
            .find(|event| {
                matches!(event, Event::AgentHistoryCreated { record, .. }
            if record.history_item_id.as_deref() == Some("observed"))
            })
            .unwrap();
        assert_eq!(
            serde_json::to_value(stored).unwrap()["AgentHistoryCreated"]["record"]["metrics"],
            expected
        );
        assert_eq!(
            &nanobpmn_engine_core::decode_event_json(&serde_json::to_string(stored).unwrap())
                .unwrap(),
            stored
        );
        let snapshot = serde_json::to_value(engine.snapshot()).unwrap();
        let restored = Engine::from_snapshot(serde_json::from_value(snapshot).unwrap());
        assert_eq!(restored.state(), engine.state());
    }
}

#[test]
fn typed_prompt_arrays_roundtrip_without_string_encoding() {
    let (mut engine, eik, job, lease) = fixture();
    let mut history = configuration();
    let prompt = text_prompt("typed prompt");
    history[2]["system_prompt"] = prompt.clone();
    let events = engine
        .apply_command(create(eik, job, &lease, history))
        .unwrap();
    let aik = agent(&events);
    let pi = engine.job(job).unwrap().instance_key;
    let snapshot = serde_json::to_value(engine.snapshot()).unwrap();
    assert_eq!(
        snapshot["state"]["instances"][pi.to_string()]["agent_instances"][aik.to_string()]
            ["definition"]["system_prompt"],
        prompt
    );
    let restored = Engine::from_snapshot(serde_json::from_value(snapshot).unwrap());
    assert_eq!(restored.state(), engine.state());
    let mut public_turn = turn("string-prompt", "Configuration");
    public_turn["system_prompt"] = json!(r#"[{"content_type":"Text","text":"not an array"}]"#);
    assert!(serde_json::from_value::<nanobpmn_engine_core::AgentHistoryTurn>(public_turn).is_err());
}

#[test]
fn legacy_prompt_strings_are_always_one_text_block_even_when_they_look_like_json() {
    let (mut engine, eik, job, lease) = fixture();
    let events = engine
        .apply_command(create(eik, job, &lease, configuration()))
        .unwrap();
    let aik = agent(&events);
    let pi = engine.job(job).unwrap().instance_key;
    for legacy in [
        "plain prompt",
        r#"[{"content_type":"Text","text":"looks typed"}]"#,
        "{}",
        "null",
        "",
    ] {
        let expected = text_prompt(legacy);
        let mut snapshot = serde_json::to_value(engine.snapshot()).unwrap();
        snapshot["format_version"] = json!(1);
        let instance = &mut snapshot["state"]["instances"][pi.to_string()];
        instance["agent_instances"][aik.to_string()]["definition"]["system_prompt"] = json!(legacy);
        for record in instance["agent_history"][aik.to_string()]
            .as_array_mut()
            .unwrap()
        {
            record["system_prompt"] = json!(legacy);
        }
        let restored = Engine::from_snapshot(serde_json::from_value(snapshot).unwrap());
        let normalized = serde_json::to_value(restored.snapshot()).unwrap();
        let instance = &normalized["state"]["instances"][pi.to_string()];
        assert_eq!(
            instance["agent_instances"][aik.to_string()]["definition"]["system_prompt"],
            expected
        );
        for record in instance["agent_history"][aik.to_string()]
            .as_array()
            .unwrap()
        {
            assert_eq!(record["system_prompt"], expected);
        }
        for event in &events {
            if let Event::AgentHistoryCreated { .. } = event {
                let mut frame = serde_json::to_value(event).unwrap();
                frame["AgentHistoryCreated"]["record"]["system_prompt"] = json!(legacy);
                let decoded = nanobpmn_engine_core::decode_event_json(&frame.to_string()).unwrap();
                assert_eq!(
                    serde_json::to_value(decoded).unwrap()["AgentHistoryCreated"]["record"]
                        ["system_prompt"],
                    expected
                );
            }
        }
        let mut corrupt = serde_json::to_value(engine.snapshot()).unwrap();
        corrupt["state"]["instances"][pi.to_string()]["agent_instances"][aik.to_string()]
            ["definition"]["system_prompt"] = json!([{"content_type":"UNKNOWN_NEW_TYPE"}]);
        assert!(serde_json::from_value::<nanobpmn_engine_core::EngineSnapshot>(corrupt).is_err());
    }
}

#[test]
fn create_derives_configuration_collectively_and_rejects_duplicate_registration() {
    let (mut engine, eik, job, lease) = fixture();
    let command = create(eik, job, &lease, configuration());
    let events = engine.apply_command(command.clone()).unwrap();
    let created = events
        .iter()
        .find_map(|event| match event {
            Event::AgentInstanceCreated { agent_instance, .. } => Some(agent_instance),
            _ => None,
        })
        .unwrap();
    assert_eq!(created.definition.model.as_deref(), Some("test-model"));
    assert_eq!(
        created.definition.provider.as_deref(),
        Some("test-provider")
    );
    assert_eq!(
        serde_json::to_value(&created.definition.system_prompt).unwrap(),
        text_prompt("test-prompt")
    );
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::AgentHistoryCommitted { .. })));
    assert!(
        engine.apply_command(command).is_err(),
        "repeat CREATE is a conflict, not upsert"
    );
}

#[test]
fn history_shape_is_validated_atomically() {
    for field in ["loop_iteration", "history_item_id"] {
        let mut history = configuration();
        let (mut engine, eik, job, lease) = fixture();
        history.push(turn("invalid", "Assistant"));
        let invalid = history.last_mut().unwrap();
        invalid[field] = if field == "loop_iteration" {
            json!(0)
        } else {
            json!("")
        };
        let before = serde_json::to_value(engine.snapshot()).unwrap();
        assert!(engine
            .apply_command(create(eik, job, &lease, history))
            .is_err());
        assert_eq!(serde_json::to_value(engine.snapshot()).unwrap(), before);
    }
    let (mut engine, eik, job, lease) = fixture();
    let mut history = configuration();
    history.push(history[0].clone());
    assert!(
        engine
            .apply_command(create(eik, job, &lease, history))
            .is_err(),
        "duplicate IDs in one batch must reject"
    );
    assert!(engine
        .apply_command(create(
            eik,
            job,
            &lease,
            vec![turn("no-config", "Assistant")]
        ))
        .is_err());
}

#[test]
fn retry_history_is_pending_until_winner_completes_and_configuration_is_deferred() {
    let (mut engine, eik, job, lease) = fixture();
    let events = engine
        .apply_command(create(eik, job, &lease, configuration()))
        .unwrap();
    let aik = agent(&events);
    let pi = engine.job(job).unwrap().instance_key;
    let mut assistant = turn("answer", "Assistant");
    assistant["metrics"] = json!({"input_tokens":7,"output_tokens":3,"reasoning_token_count":0,
        "cache_creation_token_count":0,"cache_read_token_count":0,"duration_ms":0});
    engine
        .apply_command(update(aik, eik, pi, job, &lease, vec![assistant.clone()]))
        .unwrap();
    engine
        .apply_command(Command::fail_job(job, 1, "retry").with_job_lease(lease))
        .unwrap();
    let next = engine
        .activate_jobs_with_options(
            "worker",
            "worker-b",
            1,
            1_000,
            1,
            JobActivationOptions {
                with_lease: true,
                ..Default::default()
            },
        )
        .pop()
        .unwrap();
    let next_lease = next.lease_token.unwrap();
    let mut config = turn("new-model", "Configuration");
    config["model"] = json!("deferred-model");
    let events = engine
        .apply_command(update(
            aik,
            eik,
            pi,
            job,
            &next_lease,
            vec![assistant.clone(), config],
        ))
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Event::AgentHistoryCreated { .. }))
            .count(),
        2
    );
    let duplicate = engine
        .apply_command(update(aik, eik, pi, job, &next_lease, vec![assistant]))
        .unwrap();
    assert!(!duplicate
        .iter()
        .any(|event| matches!(event, Event::AgentHistoryCreated { .. })));
    let snapshot = serde_json::to_value(engine.snapshot()).unwrap();
    let record =
        &snapshot["state"]["instances"][pi.to_string()]["agent_instances"][aik.to_string()];
    assert_eq!(record["metrics"]["model_calls"], 2);
    assert_eq!(record["metrics"]["input_tokens"], 14);
    assert_eq!(
        record["definition"]["model"], "test-model",
        "UPDATE configuration is deferred"
    );
    let events = engine
        .apply_command(Command::complete_job(job).with_job_lease(next_lease))
        .unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event, Event::AgentHistoryCommitted { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, Event::AgentHistoryDiscarded { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, Event::AgentInstanceCompleted { .. })));
    assert!(events.iter().any(
        |event| matches!(event,Event::AgentInstanceCompleted{agent_instance,..}
        if agent_instance.definition.model.as_deref() == Some("deferred-model"))
    ));
}

#[test]
fn active_writer_blocks_reassociation_before_job_context_validation() {
    use nanobpmn_engine_core::{ActivateElementInstruction, EngineError};
    let (mut engine, eik, job, lease) = fixture();
    let pi = engine.job(job).unwrap().instance_key;
    let aik = agent(
        &engine
            .apply_command(create(eik, job, &lease, configuration()))
            .unwrap(),
    );
    engine
        .apply_command(Command::ModifyInstance {
            instance_key: pi,
            activate_instructions: vec![ActivateElementInstruction {
                element_id: "agent".into(),
                variables: Default::default(),
            }],
            terminate_instructions: vec![],
        })
        .unwrap();
    let other = engine
        .activate_jobs_with_options(
            "worker",
            "worker-b",
            1,
            1_000,
            0,
            JobActivationOptions {
                with_lease: true,
                ..Default::default()
            },
        )
        .pop()
        .unwrap();
    let error = engine
        .apply_command(update(
            aik,
            other.element_instance_key,
            pi,
            999,
            "bad",
            vec![],
        ))
        .unwrap_err();
    assert!(matches!(
        error,
        EngineError::AgentInstanceActiveWriter { .. }
    ));
    engine
        .apply_command(Command::fail_job(job, 1, "release").with_job_lease(lease))
        .unwrap();
    engine
        .apply_command(update(
            aik,
            other.element_instance_key,
            pi,
            other.key,
            other.lease_token.as_deref().unwrap(),
            vec![],
        ))
        .unwrap();
    assert_eq!(
        engine.agent_instance_ownership(aik).unwrap(),
        ("agent".into(), pi)
    );
}

#[test]
fn historyless_unleased_internal_calls_and_leased_history_have_distinct_requirements() {
    let (mut engine, eik, job, lease) = fixture();
    let aik = agent(
        &engine
            .apply_command(create(eik, job, &lease, configuration()))
            .unwrap(),
    );
    let pi = engine.job(job).unwrap().instance_key;
    engine
        .apply_command(update(aik, eik, pi, 0, "", vec![]))
        .unwrap();
    assert!(engine
        .apply_command(update(
            aik,
            eik,
            pi,
            0,
            "",
            vec![turn("missing-job", "Assistant")]
        ))
        .is_err());
    assert!(engine
        .apply_command(update(
            aik,
            eik,
            pi,
            job,
            "",
            vec![turn("missing-lease", "Assistant")]
        ))
        .is_err());
}

#[test]
fn old_numeric_agent_snapshot_and_event_fields_remain_decodable() {
    let (mut engine, eik, job, lease) = fixture();
    let events = engine
        .apply_command(create(eik, job, &lease, configuration()))
        .unwrap();
    let aik = agent(&events);
    let pi = engine.job(job).unwrap().instance_key;
    for event in events {
        let mut frame = serde_json::to_value(event).unwrap();
        if let Some(created) = frame.get_mut("AgentInstanceCreated") {
            created["agent_instance"]["job_lease"] = json!(123);
        }
        if let Some(created) = frame.get_mut("AgentHistoryCreated") {
            created["record"]["job_lease"] = json!(123);
        }
        nanobpmn_engine_core::decode_event_json(&frame.to_string()).unwrap();
    }
    let mut snapshot = serde_json::to_value(engine.snapshot()).unwrap();
    snapshot["format_version"] = json!(1);
    snapshot["state"]["jobs"][job.to_string()]["lease_token"] = json!(123);
    snapshot["state"]["instances"][pi.to_string()]["agent_instances"][aik.to_string()]
        ["job_lease"] = json!(123);
    for record in snapshot["state"]["instances"][pi.to_string()]["agent_history"][aik.to_string()]
        .as_array_mut()
        .unwrap()
    {
        record["job_lease"] = json!(123);
    }
    let recovered = Engine::from_snapshot(serde_json::from_value(snapshot).unwrap());
    assert!(recovered
        .job(job)
        .unwrap()
        .lease_token
        .as_ref()
        .is_some_and(|token| !token.is_empty()));
}
