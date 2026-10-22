#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use nanobpmn_engine_core::{AgentType, Command, Engine, Event, ProcessBuilder};

fn instance(marker: Option<AgentType>) -> Engine {
    let builder = ProcessBuilder::new("lease-contract").start_event("start");
    let builder = match marker {
        Some(marker) => builder.agent_task("work", "worker", marker),
        None => builder.service_task("work", "worker"),
    };
    let definition = builder
        .end_event("end")
        .connect("start", "work")
        .connect("work", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(definition))
        .unwrap();
    engine
        .apply_command(Command::create_instance("lease-contract"))
        .unwrap();
    engine
}

fn activate(engine: &mut Engine, leased: bool, now: u64) -> Vec<Event> {
    engine
        .apply_command(
            serde_json::from_value(serde_json::json!({
                "ActivateJobs": {
                    "job_type": "worker", "worker": "worker-a", "max_jobs": 1,
                    "timeout": 1_000, "now": now, "with_lease": leased
                }
            }))
            .unwrap(),
        )
        .unwrap()
}

fn activated_key(events: &[Event]) -> u64 {
    events
        .iter()
        .find_map(|event| match event {
            Event::JobActivated { job_key, .. } => Some(*job_key),
            _ => None,
        })
        .unwrap()
}

#[test]
fn soft_digest_excludes_durable_tokens_and_cannot_restore_them() {
    let mut engine = instance(None);
    let soft_key = activated_key(&activate(&mut engine, false, 0));
    engine
        .apply_command(Command::create_instance("lease-contract"))
        .unwrap();
    let durable_key = activated_key(&activate(&mut engine, true, 0));
    assert_eq!(engine.activated_leases(), vec![(soft_key, 1_000)]);
    engine.expire_jobs(1_000);
    assert!(!engine.recover_lease(durable_key, 2_000, 1_000));
    assert!(engine.recover_lease(soft_key, 2_000, 1_000));
}

#[test]
fn authoritative_unleased_activations_never_enter_soft_recovery() {
    let mut engine = instance(None);
    let soft_key = activated_key(&activate(&mut engine, false, 0));
    engine
        .apply_command(Command::create_instance("lease-contract"))
        .unwrap();
    engine
        .apply_command(Command::DeployProcess(
            ProcessBuilder::new("agent")
                .start_event("start")
                .agent_task("work", "worker", AgentType::External)
                .end_event("end")
                .connect("start", "work")
                .connect("work", "end")
                .build()
                .unwrap(),
        ))
        .unwrap();
    engine
        .apply_command(Command::create_instance("agent"))
        .unwrap();
    let keys = engine.select_activatable_job_keys("worker", 10, 0, false);
    assert_eq!(keys.len(), 2);
    engine
        .apply_command(Command::ActivateJobsByKey {
            job_keys: keys.clone(),
            worker: "durable-worker".into(),
            timeout: 1_000,
            now: 0,
            fetch_variables: Vec::new(),
            with_lease: false,
        })
        .unwrap();
    assert_eq!(engine.activated_leases(), vec![(soft_key, 1_000)]);
    for durable_first in [false, true] {
        let snapshot =
            serde_json::from_slice(&serde_json::to_vec(&engine.snapshot()).unwrap()).unwrap();
        let mut restored = Engine::from_snapshot(snapshot);
        assert!(
            restored
                .apply_command(Command::activate_jobs_by_key(
                    keys.clone(),
                    "replacement",
                    5_000,
                    1,
                    Default::default(),
                ))
                .unwrap()
                .is_empty(),
            "an authoritative plan must not overwrite durable locks"
        );
        for durable in [durable_first, !durable_first] {
            let expired = restored.expire_jobs_by_durability(1_000, durable);
            let expired_keys: Vec<_> = expired
                .iter()
                .map(|event| match event {
                    Event::JobLockExpired { job_key, .. } => *job_key,
                    other => panic!("unexpected expiry event {other:?}"),
                })
                .collect();
            assert_eq!(
                expired_keys,
                if durable {
                    keys.clone()
                } else {
                    vec![soft_key]
                }
            );
        }
        for key in &keys {
            assert!(restored.job_requires_durable_activation(*key));
            assert!(!restored.recover_lease(*key, 2_000, 1_000));
        }
        assert!(restored.recover_lease(soft_key, 2_000, 1_000));
    }
}

#[test]
fn selective_expiry_keeps_durable_and_soft_jobs_in_separate_domains() {
    for durable_first in [false, true] {
        let mut engine = instance(None);
        let soft_key = activated_key(&activate(&mut engine, false, 0));
        engine
            .apply_command(Command::create_instance("lease-contract"))
            .unwrap();
        let durable_key = activated_key(&activate(&mut engine, true, 0));
        for leased in [durable_first, !durable_first] {
            let command = serde_json::from_value(serde_json::json!({
                "ExpireJobsByDurability": { "now": 1_000, "durable": leased }
            }))
            .unwrap();
            let events = engine.apply_command(command).unwrap();
            assert_eq!(events.len(), 1);
            assert!(matches!(events[0], Event::JobLockExpired { job_key, .. }
                if job_key == if leased { durable_key } else { soft_key }));
        }
        assert!(engine.job(durable_key).unwrap().lease_token.is_some());
    }
}

#[test]
fn combined_job_updates_fence_empty_changes_and_validate_atomically() {
    let mut engine = instance(None);
    let key = activated_key(&activate(&mut engine, true, 0));
    let lease = engine.job(key).unwrap().lease_token.clone().unwrap();
    let update = |retries: Option<i32>, timeout: Option<u64>, token: Option<&str>| {
        serde_json::from_value(serde_json::json!({
            "UpdateJob": { "job_key": key, "retries": retries, "timeout": timeout,
                "lease_token": token, "operation_reference": 42 }
        }))
        .unwrap()
    };
    for (retries, timeout, token) in [
        (None, None, Some("stale")),
        (Some(9), Some(2_000), Some("stale")),
        (Some(0), Some(2_000), Some(lease.as_str())),
    ] {
        let before = serde_json::to_value(engine.snapshot()).unwrap();
        assert!(engine
            .apply_command(update(retries, timeout, token))
            .is_err());
        assert_eq!(serde_json::to_value(engine.snapshot()).unwrap(), before);
    }
    assert!(engine
        .apply_command(update(None, None, Some(&lease)))
        .unwrap()
        .is_empty());
    let events = engine
        .apply_command(update(Some(9), Some(2_000), None))
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(engine.job(key).unwrap().retries, 9);
    assert_eq!(engine.job(key).unwrap().deadline, Some(2_000));
    engine.expire_jobs(2_000);
    let before = serde_json::to_value(engine.snapshot()).unwrap();
    assert!(engine
        .apply_command(update(Some(11), Some(2_000), None))
        .is_err());
    assert_eq!(serde_json::to_value(engine.snapshot()).unwrap(), before);
}

#[test]
fn optional_lease_helper_covers_and_clears_every_job_mutation() {
    for command in [
        Command::complete_job(1),
        Command::fail_job(1, 1, "failure"),
        Command::throw_job_error(1, "error", "business error"),
        Command::update_job_retries(1, 3),
        Command::update_job_timeout(1, 100),
        Command::UpdateJob {
            job_key: 1,
            retries: None,
            timeout: None,
            operation_reference: None,
            lease_token: None,
        },
    ] {
        let leased = command
            .clone()
            .with_lease_token(Some("opaque-token".into()));
        let value = serde_json::to_value(&leased).unwrap();
        assert_eq!(
            value.as_object().unwrap().values().next().unwrap()["lease_token"],
            "opaque-token"
        );
        assert_eq!(leased.with_lease_token(None), command);
    }
}

#[test]
fn immutable_activation_plans_override_only_replica_local_soft_locks() {
    for with_lease in [false, true] {
        let mut leader = instance(None);
        for _ in 0..2 {
            leader
                .apply_command(Command::create_instance("lease-contract"))
                .unwrap();
        }
        let mut follower = Engine::from_snapshot(leader.snapshot());
        assert_eq!(
            activate(&mut leader, false, 0),
            activate(&mut follower, false, 0)
        );
        let selected = activated_key(&activate(&mut follower, false, 0));
        assert!(leader.pending_jobs().iter().any(|job| job.key == selected));
        let plan: Command = serde_json::from_value(serde_json::json!({
            "ActivateJobsByKey": { "job_keys":[selected,selected], "worker":"replicated",
                "timeout":2_000,"now":0,"fetch_variables":["value"],"with_lease":with_lease }
        }))
        .unwrap();
        let before = leader.snapshot().next_local;
        let events = leader.apply_command(plan.clone()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(follower.apply_command(plan).unwrap(), events);
        assert_eq!(leader.state(), follower.state());
        assert_eq!(leader.snapshot().next_local, follower.snapshot().next_local);
        assert_eq!(leader.snapshot().next_local > before, with_lease);
    }
}

#[test]
fn immutable_activation_plans_never_fallback_or_override_durable_locks() {
    let mut engine = instance(None);
    let durable = activated_key(&activate(&mut engine, true, 0));
    engine
        .apply_command(Command::create_instance("lease-contract"))
        .unwrap();
    let plan = |with_lease: bool| {
        serde_json::from_value(serde_json::json!({
            "ActivateJobsByKey":{"job_keys":[durable,u64::MAX],"worker":"other",
                "timeout":2_000,"now":0,"with_lease":with_lease}
        }))
        .unwrap()
    };
    for with_lease in [false, true] {
        let before = serde_json::to_value(engine.snapshot()).unwrap();
        assert!(engine.apply_command(plan(with_lease)).unwrap().is_empty());
        assert_eq!(serde_json::to_value(engine.snapshot()).unwrap(), before);
    }
    engine.expire_jobs(1_000);
    assert!(engine.apply_command(plan(false)).unwrap().is_empty());
}

#[test]
fn activation_planning_is_pure_and_preserves_priority_and_key_order() {
    let mut engine = instance(None);
    let low = engine.pending_jobs()[0].key;
    engine
        .apply_command(Command::DeployProcess(
            ProcessBuilder::new("high-priority")
                .start_event("start")
                .service_task_with_priority("work", "worker", Some("99".into()))
                .end_event("end")
                .connect("start", "work")
                .connect("work", "end")
                .build()
                .unwrap(),
        ))
        .unwrap();
    let mut high = Vec::new();
    for _ in 0..2 {
        let events = engine
            .apply_command(Command::create_instance("high-priority"))
            .unwrap();
        high.push(
            events
                .iter()
                .find_map(|event| match event {
                    Event::JobCreated { job_key, .. } => Some(*job_key),
                    _ => None,
                })
                .unwrap(),
        );
    }
    let before = serde_json::to_value(engine.snapshot()).unwrap();
    for with_lease in [false, true] {
        assert_eq!(
            engine.select_activatable_job_keys("worker", 3, 0, with_lease),
            vec![high[0], high[1], low]
        );
        let selected = engine.select_activatable_job_keys("worker", 2, 0, with_lease);
        assert_eq!(selected, high);
        assert_eq!(serde_json::to_value(engine.snapshot()).unwrap(), before);
        let options = nanobpmn_engine_core::JobActivationOptions {
            with_lease,
            fetch_variables: vec!["declared".into()],
        };
        let mut planned = Engine::from_snapshot(engine.snapshot());
        let mut ordinary = Engine::from_snapshot(engine.snapshot());
        let planned_events = planned
            .apply_command(Command::activate_jobs_by_key(
                selected.clone(),
                "same",
                100,
                0,
                options.clone(),
            ))
            .unwrap();
        let mut ordinary_events = ordinary
            .apply_command(Command::activate_jobs_with_options(
                "worker", "same", 2, 100, 0, options,
            ))
            .unwrap();
        for event in &mut ordinary_events {
            if let Event::JobActivated { durable, .. } = event {
                *durable = true;
            }
        }
        assert_eq!(planned_events, ordinary_events);
        let mut expected_state = ordinary.state().clone();
        for key in selected {
            expected_state
                .jobs
                .get_mut(&key)
                .unwrap()
                .durable_activation = true;
        }
        assert_eq!(planned.state(), &expected_state);
    }
}

#[test]
fn timeout_updates_accept_signed_durations_and_zero() {
    for variant in ["UpdateJob", "UpdateJobTimeout"] {
        for timeout in [-5_i64, 0, 5] {
            let mut engine = instance(None);
            let key = activated_key(&activate(&mut engine, true, 0));
            let lease = engine.job(key).unwrap().lease_token.clone();
            let command: Command = serde_json::from_value(serde_json::json!({
                variant: { "job_key":key, "timeout":timeout, "lease_token":lease }
            }))
            .unwrap();
            engine.apply_command_at(command, 100).unwrap();
            assert_eq!(
                engine.job(key).unwrap().deadline,
                Some((100 + timeout) as u64)
            );
            assert_eq!(engine.expire_jobs(100).len(), usize::from(timeout <= 0));
            assert_eq!(engine.job(key).unwrap().lease_token, lease);
        }
    }
}

#[test]
fn agent_classification_never_implicitly_requests_a_job_lease() {
    for marker in [
        None,
        Some(AgentType::External),
        Some(AgentType::AiAgentTask),
    ] {
        let mut engine = instance(marker);
        let jobs = engine.activate_jobs("worker", "worker-a", 1, 1_000, 0);
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            engine.job_supports_agent_instance(jobs[0].key),
            marker.is_some()
        );
        assert!(!engine.job_supports_agent_instance(u64::MAX));
        assert_eq!(
            jobs[0].lease_token, None,
            "withLease defaults to false for {marker:?}"
        );
    }
}

#[test]
fn with_lease_opts_in_ordinary_jobs_and_fences_completion() {
    let mut engine = instance(None);
    let job_key = activated_key(&activate(&mut engine, true, 0));
    let lease = serde_json::to_value(engine.job(job_key).unwrap()).unwrap()["lease_token"].clone();
    assert!(lease.is_string(), "withLease must issue an opaque string");
    assert!(engine
        .apply_command(Command::complete_job(job_key))
        .is_err());
    assert!(
        engine.job(job_key).is_some(),
        "rejected completion must not consume the job"
    );
    let completion: Command = serde_json::from_value(serde_json::json!({
        "CompleteJob": {"job_key": job_key, "variables": {}, "lease_token": lease}
    }))
    .unwrap();
    let events = engine.apply_command(completion).unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event, Event::ProcessInstanceCompleted { .. })));
}

#[test]
fn leases_are_sticky_and_recovery_does_not_reuse_activation_identity() {
    let mut engine = instance(None);
    let job_key = activated_key(&activate(&mut engine, true, 0));
    let token = serde_json::to_value(engine.job(job_key).unwrap()).unwrap()["lease_token"].clone();
    assert!(token.is_string(), "lease tokens are opaque strings");
    engine
        .apply_command(Command::ExpireJobs { now: 1_000 })
        .unwrap();
    assert!(
        activate(&mut engine, false, 1_000).is_empty(),
        "leased jobs cannot become unleased"
    );
    let mut recovered = Engine::from_snapshot(engine.snapshot());
    activate(&mut engine, true, 1_000);
    activate(&mut recovered, true, 1_000);
    let current =
        serde_json::to_value(engine.job(job_key).unwrap()).unwrap()["lease_token"].clone();
    assert_ne!(current, token);
    assert_eq!(
        current,
        serde_json::to_value(recovered.job(job_key).unwrap()).unwrap()["lease_token"]
    );
}

#[test]
fn all_lifecycle_commands_fence_but_property_updates_allow_missing_tokens() {
    for name in [
        "CompleteJob",
        "FailJob",
        "ThrowJobError",
        "UpdateJobRetries",
        "UpdateJobTimeout",
    ] {
        let mut engine = instance(None);
        let job_key = activated_key(&activate(&mut engine, true, 0));
        let mut payload = serde_json::json!({
            "job_key": job_key, "variables": {}, "retries": 1, "timeout": 2_000,
            "error_code": "error", "error_message": "failure"
        });
        payload["lease_token"] = serde_json::json!("not-the-current-lease");
        let before = serde_json::to_value(engine.snapshot()).unwrap();
        let command = serde_json::from_value(serde_json::json!({name: payload})).unwrap();
        assert!(
            engine.apply_command(command).is_err(),
            "{name} accepted a stale token"
        );
        assert_eq!(
            serde_json::to_value(engine.snapshot()).unwrap(),
            before,
            "{name} changed state before rejection"
        );
        payload.as_object_mut().unwrap().remove("lease_token");
        let command = serde_json::from_value(serde_json::json!({name: payload})).unwrap();
        let result = engine.apply_command(command);
        assert_eq!(
            result.is_ok(),
            name.starts_with("Update"),
            "{name}: {result:?}"
        );
    }
}

#[test]
fn expired_and_failed_leases_allow_late_completion_but_not_unleased_reactivation() {
    for fail in [false, true] {
        let mut engine = instance(None);
        let job_key = activated_key(&activate(&mut engine, true, 0));
        let lease = engine.job(job_key).unwrap().lease_token.clone().unwrap();
        if fail {
            engine
                .apply_command(Command::fail_job(job_key, 1, "retry").with_job_lease(lease.clone()))
                .unwrap();
        } else {
            engine
                .apply_command(Command::ExpireJobs { now: 1_000 })
                .unwrap();
        }
        assert_eq!(
            engine.job(job_key).unwrap().lease_token.as_deref(),
            Some(lease.as_str())
        );
        assert!(activate(&mut engine, false, 1_000).is_empty());
        engine
            .apply_command(Command::complete_job(job_key).with_job_lease(lease))
            .unwrap();
    }
}

#[test]
fn numeric_historical_events_decode_but_numeric_public_tokens_do_not() {
    let mut engine = instance(None);
    let mut journal: Vec<Event> = Vec::new();
    let snapshot = engine.snapshot();
    let activation = activate(&mut engine, true, 0);
    let key = activated_key(&activation);
    for event in activation {
        let mut json = serde_json::to_value(event).unwrap();
        json["JobActivated"]["lease_token"] = serde_json::json!(123_u64);
        journal.push(
            nanobpmn_engine_core::decode_event_json(&serde_json::to_string(&json).unwrap())
                .unwrap(),
        );
    }
    let mut recovered = Engine::from_snapshot(snapshot);
    recovered.apply_replayed_events(journal);
    let legacy = recovered.job(key).unwrap().lease_token.clone().unwrap();
    recovered
        .apply_command(Command::ExpireJobs { now: 1_000 })
        .unwrap();
    let next = activate(&mut recovered, true, 1_000);
    let current = recovered.job(key).unwrap().lease_token.clone().unwrap();
    assert_ne!(current, legacy);
    assert!(
        next[0].max_key() > 123,
        "replay must reserve historical lease allocator keys"
    );
    let malformed =
        serde_json::json!({"CompleteJob":{"job_key":key,"variables":{},"lease_token":123}});
    assert!(serde_json::from_value::<Command>(malformed).is_err());
}

#[test]
fn full_event_replay_and_snapshot_recovery_issue_identical_fresh_tokens() {
    let mut engine = Engine::new();
    let mut events = engine
        .apply_command(Command::DeployProcess(
            ProcessBuilder::new("lease-contract")
                .start_event("start")
                .service_task("work", "worker")
                .end_event("end")
                .connect("start", "work")
                .connect("work", "end")
                .build()
                .unwrap(),
        ))
        .unwrap();
    events.extend(
        engine
            .apply_command(Command::create_instance("lease-contract"))
            .unwrap(),
    );
    events.extend(activate(&mut engine, true, 0));
    let mut replayed = Engine::replay(events);
    let mut restored = Engine::from_snapshot(engine.snapshot());
    for copy in [&mut engine, &mut replayed, &mut restored] {
        copy.apply_command(Command::ExpireJobs { now: 1_000 })
            .unwrap();
    }
    let expected = activate(&mut engine, true, 1_000);
    assert_eq!(activate(&mut replayed, true, 1_000), expected);
    assert_eq!(activate(&mut restored, true, 1_000), expected);
}

#[test]
fn execution_and_task_listener_jobs_use_the_same_lease_fence() {
    use nanobpmn_engine_core::{
        ExecutionListener, JobActivationOptions, ListenerEventType, TaskListener,
        TaskListenerEventType,
    };
    for task_listener in [false, true] {
        let builder = ProcessBuilder::new("listeners")
            .start_event("start")
            .user_task("task")
            .end_event("end")
            .connect("start", "task")
            .connect("task", "end");
        let definition = if task_listener {
            builder.with_task_listeners(
                "task",
                vec![TaskListener {
                    event_type: TaskListenerEventType::Creating,
                    job_type: "listener".into(),
                    retries: None,
                }],
            )
        } else {
            builder.with_listeners(
                "task",
                vec![ExecutionListener {
                    event_type: ListenerEventType::Start,
                    job_type: "listener".into(),
                    retries: None,
                }],
                vec![],
            )
        }
        .build()
        .unwrap();
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(definition))
            .unwrap();
        engine
            .apply_command(Command::create_instance("listeners"))
            .unwrap();
        let job = engine
            .activate_jobs_with_options(
                "listener",
                "worker",
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
        assert!(engine
            .apply_command(Command::complete_job(job.key))
            .is_err());
        engine
            .apply_command(Command::complete_job(job.key).with_job_lease(job.lease_token.unwrap()))
            .unwrap();
    }
}
