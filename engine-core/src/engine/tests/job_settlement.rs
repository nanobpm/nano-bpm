//! Idempotent, fenced job settlement (#1181).
//!
//! A worker that loses its activation (crash, network partition, host
//! migration) may re-issue a settlement it already performed. The engine keeps
//! a replicated **settlement ledger** keyed by job, retaining every recorded
//! settlement — the lease token (the per-activation fence) and the outcome it
//! produced — plus a per-job fence on the live job itself. A replay bearing a
//! recorded token and the same outcome is a no-op success, from *any* replica;
//! a recorded token asserting a *different* outcome is rejected outright (one
//! token settles one activation exactly once), and a settlement bearing a
//! superseded token is still fenced by the newer activation.
use super::*;

/// Deploy `linear_with_task`, start one instance, and lease its job.
fn leased(engine: &mut Engine, worker: &str, now: u64) -> (Key, String) {
    let jobs = engine.activate_jobs_with_options(
        "payment",
        worker,
        1,
        60_000,
        now,
        crate::JobActivationOptions {
            fetch_variables: Vec::new(),
            with_lease: true,
        },
    );
    assert_eq!(jobs.len(), 1, "one job leased");
    let token = jobs[0].lease_token.clone().expect("leased activation");
    (jobs[0].key, token)
}

/// A fresh engine with one instance parked on a `payment` job, plus the journal.
fn parked() -> (Engine, Vec<Event>) {
    let mut engine = Engine::new();
    let mut journal = Vec::new();
    journal.extend(
        engine
            .apply_command(Command::DeployProcess(linear_with_task()))
            .unwrap(),
    );
    journal.extend(
        engine
            .apply_command(Command::create_instance("order"))
            .unwrap(),
    );
    (engine, journal)
}

fn complete(job_key: Key, token: &str) -> Command {
    Command::complete_job(job_key).with_lease_token(Some(token.to_string()))
}

fn fail(job_key: Key, token: &str, retries: i32) -> Command {
    Command::fail_job(job_key, retries, "boom").with_lease_token(Some(token.to_string()))
}

#[test]
fn replayed_completion_with_the_recorded_lease_is_an_idempotent_no_op() {
    let (mut engine, _) = parked();
    let (job_key, token) = leased(&mut engine, "w1", 0);
    let first = engine.apply_command(complete(job_key, &token)).unwrap();
    assert!(first
        .iter()
        .any(|e| matches!(e, Event::JobCompleted { .. })));

    let replay = engine.apply_command(complete(job_key, &token));
    assert_eq!(
        replay.unwrap(),
        Vec::<Event>::new(),
        "replay is a success that changes nothing"
    );
}

#[test]
fn replayed_failure_with_the_recorded_lease_does_not_double_apply() {
    let (mut engine, _) = parked();
    let (job_key, token) = leased(&mut engine, "w1", 0);
    engine.apply_command(fail(job_key, &token, 2)).unwrap();

    // The worker crashed before it saw the ack and re-issues the same fail
    // (the decremented retry count it computed). It must not fail twice.
    assert_eq!(
        engine.apply_command(fail(job_key, &token, 2)).unwrap(),
        Vec::<Event>::new()
    );
    let job = engine.job(job_key).unwrap();
    assert_eq!(job.retries, 2);
    assert_eq!(job.state, state::JobState::Created);
}

#[test]
fn a_settlement_with_a_different_outcome_under_the_recorded_lease_is_not_masked() {
    let (mut engine, _) = parked();
    let (job_key, token) = leased(&mut engine, "w1", 0);
    engine.apply_command(complete(job_key, &token)).unwrap();
    // Same key, different outcome: not a replay — an explicit conflict
    // rejection (a token settles one activation exactly once).
    assert_eq!(
        engine.apply_command(fail(job_key, &token, 0)),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
}

#[test]
fn a_stale_fence_is_rejected_once_a_newer_activation_exists() {
    let (mut engine, _) = parked();
    let (job_key, stale) = leased(&mut engine, "w1", 0);
    // The lock lapses and another worker takes the job with a newer fence.
    engine
        .apply_command(Command::ExpireJobs { now: 60_001 })
        .ok();
    engine
        .apply_command_at(Command::ExpireJobs { now: 60_001 }, 60_001)
        .ok();
    let (_, fresh) = leased(&mut engine, "w2", 60_002);
    assert_ne!(stale, fresh);
    assert!(
        crate::lease::issued_key(&fresh) > crate::lease::issued_key(&stale),
        "fences are monotonic"
    );

    // The late original worker cannot clobber the newer activation.
    assert_eq!(
        engine.apply_command(complete(job_key, &stale)),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
    assert!(engine
        .apply_command(complete(job_key, &fresh))
        .unwrap()
        .iter()
        .any(|e| matches!(e, Event::JobCompleted { .. })));
}

#[test]
fn a_failure_recorded_under_a_superseded_fence_still_replays_idempotently() {
    let (mut engine, _) = parked();
    let (job_key, first) = leased(&mut engine, "w1", 0);
    engine.apply_command(fail(job_key, &first, 2)).unwrap();
    let (_, second) = leased(&mut engine, "w2", 1);

    // The failure really happened: re-sending it is a truthful no-op…
    assert_eq!(
        engine.apply_command(fail(job_key, &first, 2)).unwrap(),
        Vec::<Event>::new()
    );
    // …but the superseded fence cannot settle the *new* activation.
    assert_eq!(
        engine.apply_command(complete(job_key, &first)),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
    assert_eq!(engine.job(job_key).unwrap().lease_token, Some(second));
}

/// The cross-host case: the settlement is recorded by one replica, the worker
/// reconnects to another (a follower promoted after failover, a node restored
/// from snapshot, or one that never saw the leader-local activation).
#[test]
fn replay_is_idempotent_on_every_replica_that_rebuilt_from_the_journal() {
    let (mut leader, mut journal) = parked();
    let activation = leader
        .apply_command_at(
            Command::activate_jobs_with_options(
                "payment",
                "w1",
                1,
                60_000,
                0,
                crate::JobActivationOptions {
                    fetch_variables: Vec::new(),
                    with_lease: true,
                },
            ),
            0,
        )
        .unwrap();
    let (job_key, token) = activation
        .iter()
        .find_map(|e| match e {
            Event::JobActivated {
                job_key,
                lease_token: Some(t),
                ..
            } => Some((*job_key, t.clone())),
            _ => None,
        })
        .unwrap();
    let settled = leader.apply_command(complete(job_key, &token)).unwrap();

    // (a) A follower that replicated the activation.
    let mut with_activation = journal.clone();
    with_activation.extend(activation.iter().cloned());
    with_activation.extend(settled.iter().cloned());
    // (b) A follower under leader-local activation: the lease never reached it.
    journal.extend(settled.iter().cloned());

    for (name, log) in [("replicated", with_activation), ("leader-local", journal)] {
        let mut follower = Engine::replay(log);
        assert_eq!(
            follower.apply_command(complete(job_key, &token)).unwrap(),
            Vec::<Event>::new(),
            "{name}: promoted follower accepts the replay idempotently"
        );
        // Snapshot + restore (another host booting from the snapshot). The
        // snapshot/`from_snapshot` pair exists only under the `serde` feature;
        // the feature-off build exercises the journal-replay and eviction
        // checks below instead.
        #[cfg(feature = "serde")]
        {
            let mut restored = Engine::from_snapshot(follower.snapshot());
            assert_eq!(
                restored.apply_command(complete(job_key, &token)).unwrap(),
                Vec::<Event>::new(),
                "{name}: restored replica accepts the replay idempotently"
            );
            follower = restored;
        }
        // The host evicted the finished instance from hot state: the ledger
        // must outlive it, or the replay degrades to 404.
        let instance_key = follower.job(job_key).unwrap().instance_key;
        assert!(follower.evict_instance(instance_key));
        assert_eq!(
            follower.apply_command(complete(job_key, &token)).unwrap(),
            Vec::<Event>::new(),
            "{name}: replay survives instance eviction"
        );
        // A forged/unknown token is still refused.
        assert!(follower
            .apply_command(complete(job_key, "nano-lease:00000000deadbeef"))
            .is_err());
    }
}

#[cfg(feature = "serde")]
#[test]
fn the_settlement_ledger_survives_a_serialized_snapshot() {
    let (mut engine, _) = parked();
    let (job_key, token) = leased(&mut engine, "w1", 0);
    engine.apply_command(complete(job_key, &token)).unwrap();
    let json = serde_json::to_string(engine.state()).unwrap();
    let state: State = serde_json::from_str(&json).unwrap();
    assert_eq!(
        state
            .settled_jobs
            .get(&job_key)
            .and_then(|known| known.first())
            .map(|s| s.lease_token.as_str()),
        Some(token.as_str())
    );
    // An older snapshot without the ledger still loads (additive field).
    let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
    value.as_object_mut().unwrap().remove("settled_jobs");
    let old: State = serde_json::from_value(value).unwrap();
    assert!(old.settled_jobs.is_empty());
}

#[test]
fn lease_less_settlements_record_nothing() {
    let (mut engine, _) = parked();
    let job_key = engine.activate_jobs("payment", "w1", 1, 60_000, 0)[0].key;
    let events = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::JobSettlementRecorded { .. })));
    assert!(engine.state().settled_jobs.is_empty());
    // Without a key there is nothing to dedupe against: still 409-class.
    assert_eq!(
        engine.apply_command(Command::complete_job(job_key)),
        Err(EngineError::JobNotActive { job_key })
    );
}

#[test]
fn the_settlement_ledger_is_bounded_and_evicts_oldest_deterministically() {
    let mut a = State::new();
    let mut b = State::new();
    for key in (1..=5u64).rev() {
        for s in [&mut a, &mut b] {
            state::record_job_settlement(
                s,
                key,
                state::JobSettlement {
                    lease_token: format!("t{key}"),
                    outcome: state::JobSettlementOutcome::Completed,
                },
                3,
            );
        }
    }
    let keys: Vec<Key> = a.settled_jobs.keys().copied().collect();
    assert_eq!(keys, vec![3, 4, 5], "oldest job keys are evicted first");
    assert_eq!(a.settled_jobs, b.settled_jobs, "replicas agree");
}

#[test]
fn the_settlement_ledger_bounds_tokens_per_job() {
    let mut st = State::new();
    let total = state::SETTLED_TOKENS_PER_JOB_CAP + 5;
    for i in 0..total {
        state::record_job_settlement(
            &mut st,
            7,
            state::JobSettlement {
                lease_token: format!("t{i}"),
                outcome: state::JobSettlementOutcome::Failed,
            },
            state::SETTLED_JOB_LEDGER_CAP,
        );
    }
    let kept: Vec<String> = st.settled_jobs[&7]
        .iter()
        .map(|s| s.lease_token.clone())
        .collect();
    let expected: Vec<String> = (total - state::SETTLED_TOKENS_PER_JOB_CAP..total)
        .map(|i| format!("t{i}"))
        .collect();
    assert_eq!(
        kept, expected,
        "one job's history is capped, oldest dropped"
    );
}

/// A token settles one activation exactly once: after a retryable failure
/// (which returns the job to `Created` with its lease token and activation
/// latch intact), the same token asserting a DIFFERENT outcome must be
/// rejected — never allowed to settle the same activation a second time.
#[test]
fn a_conflicting_outcome_under_the_recorded_lease_is_rejected() {
    let (mut engine, _) = parked();
    let (job_key, token) = leased(&mut engine, "w1", 0);
    engine.apply_command(fail(job_key, &token, 2)).unwrap();
    assert_eq!(engine.job(job_key).unwrap().state, state::JobState::Created);

    // Complete under the failed token: not a replay, not a second settlement.
    assert_eq!(
        engine.apply_command(complete(job_key, &token)),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
    // Throw-error under the failed token: likewise.
    let throw =
        Command::throw_job_error_with(job_key, "E".to_string(), "m".to_string(), HashMap::new())
            .with_lease_token(Some(token.clone()));
    assert_eq!(
        engine.apply_command(throw),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
    // The job is untouched: still retryable, still fenced to the same token.
    let job = engine.job(job_key).unwrap();
    assert_eq!(job.state, state::JobState::Created);
    assert_eq!(job.retries, 2);
    assert_eq!(job.lease_token.as_deref(), Some(token.as_str()));

    // And the mirror image: a completion recorded under the token rejects a
    // later failure under it (already covered pre-#1181-fix by the terminal
    // state check, kept explicit here so the conflict rule is symmetric).
    let (mut engine, _) = parked();
    let (job_key, token) = leased(&mut engine, "w1", 0);
    engine.apply_command(complete(job_key, &token)).unwrap();
    assert_eq!(
        engine.apply_command(fail(job_key, &token, 0)),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
}

/// A retryable failure's replay fence must survive eviction of the job's
/// bounded-ledger entry: the live job itself retains the consumed token, so a
/// replayed failure after the ledger aged the entry out is still an idempotent
/// no-op — never a second `JobFailed` decrementing retries again.
#[test]
fn a_replayed_failure_after_ledger_eviction_is_still_fenced_by_the_live_job() {
    let (mut engine, _) = parked();
    let (job_key, token) = leased(&mut engine, "w1", 0);
    engine.apply_command(fail(job_key, &token, 2)).unwrap();

    // Age the job's ledger entry out (the live job's own fence is what must
    // keep answering; the bounded ledger is only the cross-eviction record).
    engine.state_mut().settled_jobs.clear();
    assert!(engine.state().settled_jobs.is_empty());

    assert_eq!(
        engine.apply_command(fail(job_key, &token, 2)).unwrap(),
        Vec::<Event>::new(),
        "replay after ledger eviction emits no second settlement"
    );
    let job = engine.job(job_key).unwrap();
    assert_eq!(job.retries, 2, "retries are not decremented twice");
    assert_eq!(job.state, state::JobState::Created);

    // The conflicting-outcome fence survives eviction too.
    assert_eq!(
        engine.apply_command(complete(job_key, &token)),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
}

/// A later settlement under a NEWER activation's token must not erase the
/// replay key of an earlier one: worker A's exact replay under T1 is still an
/// idempotent no-op after worker B settled the next activation under T2, even
/// when the ledger is far below its cap.
#[test]
fn a_superseded_tokens_replay_survives_a_later_settlement() {
    let (mut engine, _) = parked();
    let (job_key, first) = leased(&mut engine, "w1", 0);
    engine.apply_command(fail(job_key, &first, 2)).unwrap();
    let (_, second) = leased(&mut engine, "w2", 1);
    engine.apply_command(complete(job_key, &second)).unwrap();

    // Both settlements are retained in the ledger, keyed by token.
    let known = engine.state().settled_jobs.get(&job_key).unwrap();
    assert_eq!(known.len(), 2, "T1's entry is not replaced by T2's");

    // A's lost-response replay under T1 is still answered idempotently.
    assert_eq!(
        engine.apply_command(fail(job_key, &first, 2)).unwrap(),
        Vec::<Event>::new(),
        "the superseded token's exact replay is still a no-op success"
    );
    // And a conflicting outcome under T1 is still a conflict, not a replay.
    assert_eq!(
        engine.apply_command(complete(job_key, &first)),
        Err(EngineError::JobLeaseMismatch { job_key })
    );
}

/// Under leader-local activation the `JobActivated` event is never journaled,
/// so a replayed log's only record of the minted lease key is the settlement
/// frame itself. Replay must advance the key allocator past it, or the next
/// activation re-mints the SAME token — and the job's next legitimate
/// settlement is then swallowed as a replay of the old one.
#[test]
fn replaying_a_settlement_frame_advances_the_lease_allocator() {
    let (mut leader, mut journal) = parked();
    // Leader-local activation: its events (including the JobActivated carrying
    // the minted lease key) are NOT journaled — only the settlement is.
    let activation = leader
        .apply_command_at(
            Command::activate_jobs_with_options(
                "payment",
                "w1",
                1,
                60_000,
                0,
                crate::JobActivationOptions {
                    fetch_variables: Vec::new(),
                    with_lease: true,
                },
            ),
            0,
        )
        .unwrap();
    let (job_key, token) = activation
        .iter()
        .find_map(|e| match e {
            Event::JobActivated {
                job_key,
                lease_token: Some(t),
                ..
            } => Some((*job_key, t.clone())),
            _ => None,
        })
        .unwrap();
    journal.extend(leader.apply_command(fail(job_key, &token, 2)).unwrap());

    // The follower rebuilds from a log whose only record of the lease key is
    // the settlement frame.
    let mut follower = Engine::replay(journal);
    let (_, fresh) = leased(&mut follower, "w2", 1);
    assert_ne!(
        fresh, token,
        "reactivation must not re-mint the settled activation's token"
    );
    assert!(
        crate::lease::issued_key(&fresh) > crate::lease::issued_key(&token),
        "the allocator advanced past the replayed settlement's token"
    );

    // The new activation's own legitimate settlement is NOT mistaken for a
    // replay of the old one: it really applies.
    let events = follower.apply_command(fail(job_key, &fresh, 1)).unwrap();
    assert!(
        events.iter().any(|e| matches!(e, Event::JobFailed { .. })),
        "the fresh token's first settlement applies"
    );
    assert_eq!(follower.job(job_key).unwrap().retries, 1);
}

fn throw(job_key: Key, token: &str, code: &str) -> Command {
    Command::throw_job_error_with(job_key, code.to_string(), "m".to_string(), HashMap::new())
        .with_lease_token(Some(token.to_string()))
}

/// A leased `ThrowJobError` replayed under its recorded token must be a no-op
/// for both the caught (boundary) and uncaught (incident) paths — never a
/// second `JobErrorThrown`, boundary activation, or incident — including after
/// the finished instance is evicted from hot state.
#[test]
fn replayed_thrown_error_with_the_recorded_lease_is_an_idempotent_no_op() {
    for (code, caught) in [("CARD_DECLINED", true), ("UNHANDLED", false)] {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(process_with_error_boundary()))
            .unwrap();
        engine
            .apply_command(Command::create_instance("payment"))
            .unwrap();
        let (job_key, token) = leased(&mut engine, "w1", 0);
        let first = engine.apply_command(throw(job_key, &token, code)).unwrap();
        assert!(first
            .iter()
            .any(|e| matches!(e, Event::JobErrorThrown { .. })));
        assert!(first
            .iter()
            .any(|e| matches!(e, Event::JobSettlementRecorded { .. })));
        assert_eq!(
            first
                .iter()
                .any(|e| matches!(e, Event::IncidentRaised { .. })),
            !caught,
            "{code}: incident only when uncaught"
        );

        assert_eq!(
            engine.apply_command(throw(job_key, &token, code)).unwrap(),
            Vec::<Event>::new(),
            "{code}: replay emits no second error/boundary/incident"
        );
        // A different outcome under the same token is a conflict.
        assert_eq!(
            engine.apply_command(complete(job_key, &token)),
            Err(EngineError::JobLeaseMismatch { job_key })
        );

        if caught {
            // The instance finished via the boundary; replay survives eviction.
            let instance_key = engine.job(job_key).unwrap().instance_key;
            assert!(engine.evict_instance(instance_key));
            assert_eq!(
                engine.apply_command(throw(job_key, &token, code)).unwrap(),
                Vec::<Event>::new(),
                "{code}: replay survives instance eviction"
            );
        }
    }
}
