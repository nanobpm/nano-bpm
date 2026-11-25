//! Replica-deterministic worker attribution on terminal jobs (#963).
//!
//! Under leader-local activation the `JobActivated` lease is RAM-only on the
//! leader, so a follower re-running a replicated terminal command has no
//! activating worker of its own. Attribution must therefore come from the
//! replicated command (stamped by the proposer), never from replica-local
//! soft state — otherwise leader and follower persist different terminal
//! events for the same log entry.
use super::*;
use crate::JobAttribution;

/// A leader and a lenient follower that both applied the same replicated
/// deploy + create, with the job soft-activated by `worker` ONLY on the leader
/// (leader-local activation is never replicated).
fn leader_and_follower(worker: &str) -> (Engine, Engine, Key) {
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command(Command::DeployProcess(linear_with_task()))
                .unwrap();
            e.apply_command(Command::create_instance("order")).unwrap();
            e
        })
        .collect();
    let follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let job_key = leader.pending_jobs()[0].key;
    assert_eq!(follower.pending_jobs()[0].key, job_key);
    let activated = leader.activate_jobs("payment", worker, 1, 1_000, 0);
    assert_eq!(activated.len(), 1, "leader soft-activates the job");
    (leader, follower, job_key)
}

fn terminal_worker(events: &[Event]) -> Option<Option<String>> {
    events.iter().find_map(|e| match e {
        Event::JobCompleted { worker, .. }
        | Event::JobFailed { worker, .. }
        | Event::JobErrorThrown { worker, .. } => Some(worker.clone()),
        _ => None,
    })
}

/// Proposes `command` the way the replicated write path does: the proposer
/// (leader) stamps the authoritative attribution, then every replica applies
/// the identical stamped command.
fn replicate(
    leader: &mut Engine,
    follower: &mut Engine,
    mut command: Command,
) -> [Option<String>; 2] {
    leader.stamp_terminal_attribution(&mut command);
    let on_leader = leader.apply_command(command.clone()).unwrap();
    let on_follower = follower.apply_command(command).unwrap();
    [
        terminal_worker(&on_leader).expect("leader emits a terminal job event"),
        terminal_worker(&on_follower).expect("follower emits a terminal job event"),
    ]
}

fn every_terminal_command(job_key: Key) -> Vec<(&'static str, Command)> {
    vec![
        ("complete", Command::complete_job(job_key)),
        ("fail with retries", Command::fail_job(job_key, 2, "boom")),
        ("fail terminally", Command::fail_job(job_key, 0, "boom")),
        ("throw", Command::throw_job_error(job_key, "E", "boom")),
    ]
}

#[test]
fn a_replicated_terminal_command_attributes_the_same_worker_on_every_replica() {
    for (label, _) in every_terminal_command(0) {
        let (mut leader, mut follower, job_key) = leader_and_follower("W");
        let command = every_terminal_command(job_key)
            .into_iter()
            .find(|(l, _)| *l == label)
            .unwrap()
            .1;
        let [on_leader, on_follower] = replicate(&mut leader, &mut follower, command);
        assert_eq!(
            on_leader.as_deref(),
            Some("W"),
            "{label}: leader attribution"
        );
        assert_eq!(
            on_follower, on_leader,
            "{label}: follower must persist the leader's attribution"
        );
    }
}

#[test]
fn a_stamped_resolved_absent_attribution_overrides_replica_local_state() {
    // The leader's soft lock expired before the proposal: it resolves the
    // attribution as ABSENT. A follower that (hypothetically) still held a
    // local worker must not resurrect it — the stamp is authoritative.
    let (mut follower, mut leader, job_key) = leader_and_follower("stale");
    let [on_leader, on_follower] = replicate(
        &mut leader,
        &mut follower,
        Command::fail_job(job_key, 0, "boom"),
    );
    assert_eq!(on_leader, None);
    assert_eq!(
        on_follower, None,
        "resolved-absent stamp wins over local state"
    );
}

#[test]
fn an_unstamped_legacy_command_still_attributes_from_live_state() {
    // A command journaled before the stamp existed carries no attribution:
    // the engine falls back to the live job (single-node replay unchanged).
    let (mut leader, _, job_key) = leader_and_follower("W");
    let events = leader
        .apply_command(Command::fail_job(job_key, 0, "boom"))
        .unwrap();
    assert_eq!(terminal_worker(&events), Some(Some("W".into())));
}

#[test]
fn a_digest_recovered_lease_attributes_the_real_worker_never_the_sentinel() {
    // Failover: the promoted leader recovers the in-flight lease from the
    // previous leader's digest, which carries the real worker.
    let (_, mut promoted, job_key) = leader_and_follower("W");
    assert!(promoted.recover_lease(job_key, 1_000, Some("W"), 0));
    let mut command = Command::fail_job(job_key, 0, "boom");
    promoted.stamp_terminal_attribution(&mut command);
    let events = promoted.apply_command(command).unwrap();
    assert_eq!(terminal_worker(&events), Some(Some("W".into())));

    // A digest from an older leader carries no worker: the lease is held, but
    // the terminal event is attributed to nobody — never the sentinel.
    let (_, mut promoted, job_key) = leader_and_follower("W");
    assert!(promoted.recover_lease(job_key, 1_000, None, 0));
    let mut command = Command::throw_job_error(job_key, "E", "boom");
    promoted.stamp_terminal_attribution(&mut command);
    let events = promoted.apply_command(command).unwrap();
    assert_eq!(terminal_worker(&events), Some(None));
    // Even unstamped (live-state capture), the sentinel never leaks.
    let (_, mut promoted, job_key) = leader_and_follower("W");
    assert!(promoted.recover_lease(job_key, 1_000, None, 0));
    let events = promoted
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert_eq!(terminal_worker(&events), Some(None));
}

#[test]
fn a_job_not_resident_on_the_proposer_is_stamped_resolved_absent() {
    // A job unknown/cold on the proposer at stamp time may be rehydrated (and
    // activated) before apply: it must still carry an authoritative stamp, or
    // replicas would each fall back to their own divergent live state.
    let leader = Engine::new();
    for (label, mut command) in every_terminal_command(424_242) {
        leader.stamp_terminal_attribution(&mut command);
        let attribution = match &command {
            Command::CompleteJob { attribution, .. }
            | Command::FailJob { attribution, .. }
            | Command::ThrowJobError { attribution, .. } => attribution.clone(),
            _ => unreachable!(),
        };
        assert_eq!(
            attribution.map(|a| a.worker),
            Some(None),
            "{label}: resolved absent, not unstamped"
        );
    }
}

#[test]
fn a_worker_less_digest_lease_never_retains_the_sentinel_in_state() {
    // fail (terminal) and throw retain the worker in state for attribution;
    // a resolved-absent event must not leave the replica-local digest
    // sentinel behind (it would diverge the leader's snapshot).
    for command in [
        |k| Command::fail_job(k, 0, "boom"),
        |k| Command::throw_job_error(k, "E", "boom"),
    ] {
        let (_, mut promoted, job_key) = leader_and_follower("W");
        assert!(promoted.recover_lease(job_key, 1_000, None, 0));
        let mut command: Command = command(job_key);
        promoted.stamp_terminal_attribution(&mut command);
        promoted.apply_command(command).unwrap();
        let job = promoted.state().jobs.get(&job_key).expect("job retained");
        assert_eq!(job.worker, None, "sentinel scrubbed from retained state");
    }
}

/// The stamp survives the replicated wire (the Raft log carries serialized
/// commands): "resolved absent" stays distinct from "unstamped", and an
/// unstamped command is byte-identical to the pre-#963 shape, so journals
/// written before the field existed replay unchanged.
#[cfg(feature = "serde")]
#[test]
fn the_attribution_stamp_round_trips_and_unstamped_stays_byte_identical() {
    let unstamped = Command::fail_job(7, 0, "boom");
    let json = serde_json::to_string(&unstamped).unwrap();
    assert!(
        !json.contains("attribution"),
        "unstamped is byte-unchanged: {json}"
    );
    assert_eq!(serde_json::from_str::<Command>(&json).unwrap(), unstamped);

    for worker in [None, Some("W".to_string())] {
        let mut stamped = Command::fail_job(7, 0, "boom");
        if let Command::FailJob { attribution, .. } = &mut stamped {
            *attribution = Some(JobAttribution {
                worker: worker.clone(),
                deadline: None,
            });
        }
        let json = serde_json::to_string(&stamped).unwrap();
        let back: Command = serde_json::from_str(&json).unwrap();
        assert_eq!(back, stamped, "stamp {worker:?} round-trips");
    }
}

fn terminal_deadline(events: &[Event], job_key: Key) -> Option<Option<u64>> {
    events.iter().find_map(|e| match e {
        Event::JobCompleted {
            job_key: k,
            deadline,
            ..
        }
        | Event::JobCanceled {
            job_key: k,
            deadline,
            ..
        } if *k == job_key => Some(*deadline),
        _ => None,
    })
}

/// #1494: a SOFT (leader-local) job's terminal deadline is stamped by the
/// proposer, so `JobCompleted.deadline` and the retained `Job::terminal_end`
/// agree on every replica — and carry the real lock deadline, not `None`.
///
/// The whole flow runs at a NONZERO instant: at `0` `stamp_terminal_end`
/// leaves `Job::terminal_end` unset, which would let the agreement check pass
/// on the per-command scratch alone and never exercise the retained record.
#[test]
fn a_soft_jobs_stamped_terminal_deadline_agrees_on_every_replica() {
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command_at(Command::DeployProcess(linear_with_task()), 1)
                .unwrap();
            e.apply_command_at(Command::create_instance("order"), 1)
                .unwrap();
            e
        })
        .collect();
    let mut follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let job_key = leader.pending_jobs()[0].key;
    let activated = leader.activate_jobs("payment", "W", 1, 1_000, 1);
    assert_eq!(activated.len(), 1, "leader soft-activates the job");
    let lock = leader.state().jobs[&job_key].deadline;
    assert_eq!(lock, Some(1_001), "leader holds the soft lock");
    assert_eq!(follower.state().jobs[&job_key].deadline, None);

    let mut command = Command::complete_job(job_key);
    leader.stamp_terminal_attribution(&mut command);
    let on_leader = leader.apply_command_at(command.clone(), 1).unwrap();
    let on_follower = follower.apply_command_at(command, 1).unwrap();
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(lock));
    assert_eq!(
        terminal_deadline(&on_follower, job_key),
        Some(lock),
        "follower persists the leader-stamped deadline"
    );
    // The RETAINED terminal record — not the per-command scratch — is what a
    // snapshot or cascade restates, so assert it directly on both replicas.
    let expected = Some(crate::state::JobTerminalEnd {
        at: 1,
        deadline: lock,
    });
    assert_eq!(leader.state().jobs[&job_key].terminal_end, expected);
    assert_eq!(
        follower.state().jobs[&job_key].terminal_end,
        expected,
        "the retained terminal deadline persists identically on the follower"
    );
}

/// #1494 review: the proposer stamps at PROPOSAL time, so a journaled
/// `UpdateJobTimeout` landing between stamping and completion advances the
/// replicated durable lease past the stamp. The retained terminal record must
/// then prefer the CURRENT replicated lease over the proposal-time stamp —
/// otherwise the completed job retains a deadline it no longer held.
#[test]
fn a_timeout_update_between_stamping_and_completion_wins_over_the_stamp() {
    let (mut leader, _follower, job_key) = leader_and_follower("W");
    // Make the activation DURABLE (journaled): the replicated lease, not the
    // replica-local soft lock, is what a later `UpdateJobTimeout` advances.
    leader
        .state_mut()
        .jobs
        .get_mut(&job_key)
        .unwrap()
        .lease_token = Some("lease-1".to_string());
    // The proposer stamps the completion against the current lock…
    let mut command = Command::complete_job(job_key);
    leader.stamp_terminal_attribution(&mut command);
    // …but a replicated timeout update lands BEFORE the completion applies.
    leader
        .apply_command(Command::UpdateJobTimeout {
            job_key,
            lease_token: Some("lease-1".to_string()),
            timeout: 99_000,
        })
        .unwrap();
    let updated = leader.state().jobs[&job_key].deadline;
    assert_eq!(
        updated,
        Some(99_000),
        "the journaled update moved the lease"
    );
    // The completion command itself was stamped BEFORE the update, so its
    // `completed_at` (the engine's `now`) must sit at/after the updated
    // deadline for the retained terminal record to be well-formed.
    if let Command::CompleteJob { lease_token, .. } = &mut command {
        *lease_token = Some("lease-1".to_string());
    }
    let events = leader.apply_command_at(command, 100_000).unwrap();
    assert_eq!(
        terminal_deadline(&events, job_key),
        Some(updated),
        "the retained terminal deadline is the last replicated lease, not the stamp"
    );
    assert_eq!(
        leader.state().jobs[&job_key]
            .terminal_end
            .and_then(|t| t.deadline),
        updated
    );
}

/// #1494 review: the same stamp-vs-update interleaving for a durable
/// activation WITHOUT a fencing token — `ActivateJobsByKey` with
/// `with_lease: false` still journals the activation (`durable: true`), so its
/// deadline is replicated and a journaled `UpdateJobTimeout` advances it on
/// every replica. The retained terminal record must restate that replicated
/// deadline even though `lease_token` is `None`: durability, not the fencing
/// token, is what makes the deadline authoritative.
#[test]
fn an_unleased_durable_activation_restates_its_replicated_deadline() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    // Expire the leader-local soft lock, then activate durably by key on BOTH
    // replicas (the replicated write path) — durable, but lease-less. Every
    // command runs at an explicit instant: `apply_command` reuses the engine's
    // sticky `now`, which would silently mis-stamp the journaled deadline.
    leader.expire_jobs(1_000);
    let activate = Command::activate_jobs_by_key(
        vec![job_key],
        "W",
        1_000,
        0,
        crate::JobActivationOptions {
            fetch_variables: vec![],
            with_lease: false,
        },
    );
    leader.apply_command_at(activate.clone(), 0).unwrap();
    follower.apply_command_at(activate, 0).unwrap();
    for replica in [&leader, &follower] {
        let job = &replica.state().jobs[&job_key];
        assert_eq!(job.deadline, Some(1_000), "activation deadline journaled");
        assert_eq!(job.lease_token, None, "no fencing token requested");
        assert!(job.durable_activation, "durable despite no lease token");
    }
    // The proposer stamps the completion against the journaled deadline…
    let mut command = Command::complete_job(job_key);
    leader.stamp_terminal_attribution(&mut command);
    // …but a replicated timeout update (lease-less, like the activation)
    // lands BEFORE the completion applies.
    let update = Command::UpdateJobTimeout {
        job_key,
        lease_token: None,
        timeout: 99_000,
    };
    leader.apply_command_at(update.clone(), 0).unwrap();
    follower.apply_command_at(update, 0).unwrap();
    assert_eq!(leader.state().jobs[&job_key].deadline, Some(99_000));
    // Complete at/after the updated deadline so the retained terminal record
    // is well-formed; the stamped command is identical on both replicas.
    let on_leader = leader.apply_command_at(command.clone(), 100_000).unwrap();
    let on_follower = follower.apply_command_at(command, 100_000).unwrap();
    for (name, events) in [("leader", &on_leader), ("follower", &on_follower)] {
        assert_eq!(
            terminal_deadline(events, job_key),
            Some(Some(99_000)),
            "{name}: the terminal deadline is the replicated update, not the stamp"
        );
    }
    for (name, replica) in [("leader", &leader), ("follower", &follower)] {
        assert_eq!(
            replica.state().jobs[&job_key]
                .terminal_end
                .and_then(|t| t.deadline),
            Some(99_000),
            "{name}: the retained terminal record restates the replicated deadline"
        );
    }
}

/// An UNSTAMPED (legacy) command never reads replica-local soft state for the
/// deadline: leader and follower both record `None`, exactly as before.
#[test]
fn an_unstamped_completion_records_no_soft_deadline_on_any_replica() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    let command = Command::complete_job(job_key);
    let on_leader = leader.apply_command(command.clone()).unwrap();
    let on_follower = follower.apply_command(command).unwrap();
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(None));
    assert_eq!(terminal_deadline(&on_follower, job_key), Some(None));
}

/// A stale stamp from a REJECTED command must not leak into a later command
/// that terminates the same job (the stamp is per-command scratch).
#[test]
fn a_rejected_stamped_command_does_not_leak_its_deadline() {
    // A strict engine (no lenient completion) rejects completing a job that
    // was never activated — after the stamp was installed for this command.
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;
    let mut bad = Command::complete_job(job_key);
    if let Command::CompleteJob { attribution, .. } = &mut bad {
        *attribution = Some(JobAttribution {
            worker: None,
            deadline: Some(999),
        });
    }
    assert!(engine.apply_command(bad).is_err());
    let instance_key = engine.state().jobs[&job_key].instance_key;
    let events = engine
        .apply_command(Command::CancelInstance { instance_key })
        .unwrap();
    assert_eq!(terminal_deadline(&events, job_key), Some(None));
}

/// Defect-class guard (#1494 item 2): a cancel cascade (`CancelInstance`)
/// terminating a SOFT job derives its metadata from replicated state only, so
/// leader (holding the soft lock) and follower (not) persist identical events.
#[test]
fn a_cancel_cascade_of_a_soft_job_is_replica_identical() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    let instance_key = leader.state().jobs[&job_key].instance_key;
    let cancel = Command::CancelInstance { instance_key };
    let on_leader = leader.apply_command(cancel.clone()).unwrap();
    let on_follower = follower.apply_command(cancel).unwrap();
    assert_eq!(on_leader, on_follower);
}

#[cfg(feature = "serde")]
#[test]
fn the_deadline_stamp_round_trips_and_a_pre_deadline_stamp_still_decodes() {
    let mut stamped = Command::complete_job(7);
    if let Command::CompleteJob { attribution, .. } = &mut stamped {
        *attribution = Some(JobAttribution {
            worker: Some("W".into()),
            deadline: Some(1_000),
        });
    }
    let json = serde_json::to_string(&stamped).unwrap();
    assert_eq!(serde_json::from_str::<Command>(&json).unwrap(), stamped);

    let legacy: JobAttribution = serde_json::from_str(r#"{"worker":"W"}"#).unwrap();
    assert_eq!(legacy.deadline, None, "pre-#1494 stamp decodes additively");
    let json = serde_json::to_string(&JobAttribution::default()).unwrap();
    assert!(
        !json.contains("deadline"),
        "absent deadline is byte-unchanged"
    );
}

/// Proposes `command` through the #1570 leader-stamping seam and applies the
/// identical stamped envelope on both `leader` and `follower`.
fn replicate_stamped(
    leader: &mut Engine,
    follower: &mut Engine,
    mut command: Command,
    now: u64,
) -> (Vec<Event>, Vec<Event>) {
    leader.stamp_terminal_attribution(&mut command);
    let deadlines = leader.resolve_cascade_terminal_deadlines(&command);
    let on_leader: Vec<Event> = leader
        .apply_command_records_stamped_at(command.clone(), now, None, &deadlines)
        .unwrap()
        .into_iter()
        .map(|r| r.event)
        .collect();
    let on_follower: Vec<Event> = follower
        .apply_command_records_stamped_at(command, now, None, &deadlines)
        .unwrap()
        .into_iter()
        .map(|r| r.event)
        .collect();
    assert_eq!(
        on_leader, on_follower,
        "leader and follower must persist byte-identical events"
    );
    (on_leader, on_follower)
}

/// #1570: `CancelInstance` carries the leader's soft-job deadline so both
/// leader and follower record `Some(1_000)` on `Event::JobCanceled`.
#[test]
fn cancel_instance_stamps_soft_job_deadline_on_every_replica() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    let instance_key = leader.state().jobs[&job_key].instance_key;
    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::CancelInstance { instance_key },
        200,
    );
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(Some(1_000)));
    assert_eq!(terminal_deadline(&on_follower, job_key), Some(Some(1_000)));
}

/// #1570: `ModifyInstance` terminating an element with a soft-activated job
/// preserves its deadline on `Event::JobCanceled` across replicas.
#[test]
fn modify_instance_stamps_soft_job_deadline_on_every_replica() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    let instance_key = leader.state().jobs[&job_key].instance_key;
    let element_instance_key = leader.state().jobs[&job_key].element_instance_key;
    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::ModifyInstance {
            instance_key,
            terminate_instructions: vec![element_instance_key],
            terminate_element_ids: vec![],
            activate_instructions: vec![],
            move_instructions: vec![],
        },
        200,
    );
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(Some(1_000)));
    assert_eq!(terminal_deadline(&on_follower, job_key), Some(Some(1_000)));
}

/// #1570: completing a sibling job that reaches a Terminate End Event cancels
/// the other branch's soft-activated job with its authoritative deadline on
/// every replica.
#[test]
fn terminate_end_event_cascade_stamps_sibling_soft_job_deadline() {
    let def = ProcessBuilder::new("term_cascade")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("branch_a", "work_a")
        .service_task("branch_b", "work_b")
        .terminate_end_event("term_end")
        .end_event("end_b")
        .connect("start", "fork")
        .connect("fork", "branch_a")
        .connect("fork", "branch_b")
        .connect("branch_a", "term_end")
        .connect("branch_b", "end_b")
        .build()
        .unwrap();
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command(Command::DeployProcess(def.clone()))
                .unwrap();
            e.apply_command(Command::create_instance("term_cascade"))
                .unwrap();
            e
        })
        .collect();
    let mut follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let a = leader.activate_jobs("work_a", "wa", 1, 5_000, 100);
    let b = leader.activate_jobs("work_b", "wb", 1, 9_000, 100);
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    let job_a = a[0].key;
    let job_b = b[0].key;

    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::complete_job(job_a),
        200,
    );
    assert_eq!(terminal_deadline(&on_leader, job_a), Some(Some(5_100)));
    assert_eq!(terminal_deadline(&on_leader, job_b), Some(Some(9_100)));
    assert_eq!(terminal_deadline(&on_follower, job_b), Some(Some(9_100)));
}

/// #1570: an interrupting timer boundary fired by `TriggerTimers` cancels the
/// attached soft-activated job with its deadline on every replica.
#[test]
fn interrupting_timer_boundary_stamps_soft_job_deadline_on_every_replica() {
    let def = ProcessBuilder::new("timer_cancel")
        .start_event("start")
        .service_task("task", "work")
        .timer_boundary_event("timeout", "task", 5_000)
        .end_event("end_ok")
        .end_event("end_timeout")
        .connect("start", "task")
        .connect("task", "end_ok")
        .connect("timeout", "end_timeout")
        .build()
        .unwrap();
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command(Command::DeployProcess(def.clone()))
                .unwrap();
            e.apply_command_at(Command::create_instance("timer_cancel"), 1_000)
                .unwrap();
            e
        })
        .collect();
    let mut follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let activated = leader.activate_jobs("work", "w", 1, 30_000, 1_500);
    assert_eq!(activated.len(), 1);
    let job_key = activated[0].key;

    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::TriggerTimers { now: 6_000 },
        6_000,
    );
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(Some(31_500)));
    assert_eq!(terminal_deadline(&on_follower, job_key), Some(Some(31_500)));
}

/// #1570: an interrupting message boundary fired by `CorrelateMessage` cancels
/// the attached soft-activated job with its deadline on every replica.
#[test]
fn interrupting_message_boundary_stamps_soft_job_deadline_on_every_replica() {
    let def = ProcessBuilder::new("msg_cancel")
        .start_event("start")
        .service_task("task", "work")
        .message_boundary_event("msg_b", "task", "cancel_msg", "orderId")
        .end_event("end_ok")
        .end_event("end_msg")
        .connect("start", "task")
        .connect("task", "end_ok")
        .connect("msg_b", "end_msg")
        .build()
        .unwrap();
    let mut vars = BTreeMap::new();
    vars.insert("orderId".to_string(), Value::Str("ord-1".to_string()));
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command(Command::DeployProcess(def.clone()))
                .unwrap();
            e.apply_command(Command::create_instance_with("msg_cancel", vars.clone()))
                .unwrap();
            e
        })
        .collect();
    let mut follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let activated = leader.activate_jobs("work", "w", 1, 12_000, 500);
    assert_eq!(activated.len(), 1);
    let job_key = activated[0].key;

    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::CorrelateMessage {
            message_name: "cancel_msg".into(),
            correlation_key: "ord-1".into(),
            variables: BTreeMap::new(),
            business_id: None,
            message_key: None,
            time_to_live: None,
            message_id: None,
        },
        600,
    );
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(Some(12_500)));
    assert_eq!(terminal_deadline(&on_follower, job_key), Some(Some(12_500)));
}

/// #1570: `BroadcastSignal` interrupting soft-activated jobs across multiple
/// instances stamps every affected job's deadline on every replica.
#[test]
fn broadcast_signal_cross_instance_cascade_stamps_all_soft_job_deadlines() {
    let def = ProcessBuilder::new("sig_cancel")
        .start_event("start")
        .service_task("task", "work")
        .signal_boundary_event("sig_b", "task", "abort_all")
        .end_event("end_ok")
        .end_event("end_sig")
        .connect("start", "task")
        .connect("task", "end_ok")
        .connect("sig_b", "end_sig")
        .build()
        .unwrap();
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command(Command::DeployProcess(def.clone()))
                .unwrap();
            e.apply_command(Command::create_instance("sig_cancel"))
                .unwrap();
            e.apply_command(Command::create_instance("sig_cancel"))
                .unwrap();
            e
        })
        .collect();
    let mut follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let j1 = leader.activate_jobs("work", "w1", 1, 7_000, 100)[0].key;
    let j2 = leader.activate_jobs("work", "w2", 1, 8_000, 200)[0].key;

    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::BroadcastSignal {
            signal_name: "abort_all".into(),
            variables: BTreeMap::new(),
        },
        300,
    );
    assert_eq!(terminal_deadline(&on_leader, j1), Some(Some(7_100)));
    assert_eq!(terminal_deadline(&on_leader, j2), Some(Some(8_200)));
    assert_eq!(terminal_deadline(&on_follower, j1), Some(Some(7_100)));
    assert_eq!(terminal_deadline(&on_follower, j2), Some(Some(8_200)));
}

/// #1570: cancelling a parent process cascades to a call-activity child's
/// soft-activated job and preserves its deadline on every replica.
#[test]
fn cancel_parent_stamps_call_activity_child_soft_job_deadline() {
    let child = ProcessBuilder::new("child_proc")
        .start_event("cs")
        .service_task("ct", "child_work")
        .end_event("ce")
        .connect("cs", "ct")
        .connect("ct", "ce")
        .build()
        .unwrap();
    let parent = ProcessBuilder::new("parent_proc")
        .start_event("ps")
        .call_activity("call", "child_proc")
        .end_event("pe")
        .connect("ps", "call")
        .connect("call", "pe")
        .build()
        .unwrap();
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command(Command::DeployProcess(child.clone()))
                .unwrap();
            e.apply_command(Command::DeployProcess(parent.clone()))
                .unwrap();
            e.apply_command(Command::create_instance("parent_proc"))
                .unwrap();
            e
        })
        .collect();
    let mut follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let parent_key = leader
        .state()
        .instances
        .values()
        .find(|i| i.parent_process_instance_key.is_none())
        .unwrap()
        .key;
    let child_job = leader.activate_jobs("child_work", "wc", 1, 15_000, 400)[0].key;

    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::CancelInstance {
            instance_key: parent_key,
        },
        500,
    );
    assert_eq!(terminal_deadline(&on_leader, child_job), Some(Some(15_400)));
    assert_eq!(
        terminal_deadline(&on_follower, child_job),
        Some(Some(15_400))
    );
}

/// #1570: a cascade propagates UP across a call activity — completing the
/// child's job completes the child, the parent then reaches a Terminate End
/// Event and cancels a soft-activated job on its OTHER branch. That job must
/// carry its deadline on every replica.
#[test]
fn child_completion_cascading_to_parent_terminate_stamps_parent_soft_job_deadline() {
    let child = ProcessBuilder::new("up_child")
        .start_event("cs")
        .service_task("ct", "up_child_work")
        .end_event("ce")
        .connect("cs", "ct")
        .connect("ct", "ce")
        .build()
        .unwrap();
    let parent = ProcessBuilder::new("up_parent")
        .start_event("ps")
        .parallel_gateway("fork")
        .call_activity("call", "up_child")
        .service_task("p_task", "up_parent_work")
        .terminate_end_event("term")
        .end_event("pe")
        .connect("ps", "fork")
        .connect("fork", "call")
        .connect("fork", "p_task")
        .connect("call", "term")
        .connect("p_task", "pe")
        .build()
        .unwrap();
    let mut replicas: Vec<Engine> = (0..2)
        .map(|_| {
            let mut e = Engine::new();
            e.apply_command(Command::SetLenientCompletion { lenient: true })
                .unwrap();
            e.apply_command(Command::DeployProcess(child.clone()))
                .unwrap();
            e.apply_command(Command::DeployProcess(parent.clone()))
                .unwrap();
            e.apply_command(Command::create_instance("up_parent"))
                .unwrap();
            e
        })
        .collect();
    let mut follower = replicas.pop().unwrap();
    let mut leader = replicas.pop().unwrap();
    let parent_job = leader.activate_jobs("up_parent_work", "wp", 1, 9_000, 100)[0].key;
    let child_job = leader.activate_jobs("up_child_work", "wc", 1, 5_000, 100)[0].key;

    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::complete_job(child_job),
        200,
    );
    assert_eq!(terminal_deadline(&on_leader, parent_job), Some(Some(9_100)));
    assert_eq!(
        terminal_deadline(&on_follower, parent_job),
        Some(Some(9_100))
    );
}

/// #1570: envelope stamps on a REJECTED command must not leak into a subsequent
/// unstamped command.
#[test]
fn a_rejected_envelope_stamped_command_does_not_leak_its_deadlines() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    let instance_key = leader.state().jobs[&job_key].instance_key;
    let mut bogus_stamps = BTreeMap::new();
    bogus_stamps.insert(job_key, 99_999);
    // Cancelling a non-existent instance rejects, leaving the real instance alive.
    assert!(leader
        .apply_command_records_stamped_at(
            Command::CancelInstance {
                instance_key: 999_999,
            },
            100,
            None,
            &bogus_stamps,
        )
        .is_err());
    // An unstamped cancel of the real instance must NOT pick up the leaked 99_999.
    let on_leader = leader
        .apply_command_at(Command::CancelInstance { instance_key }, 200)
        .unwrap();
    let on_follower = follower
        .apply_command_at(Command::CancelInstance { instance_key }, 200)
        .unwrap();
    assert_eq!(on_leader, on_follower);
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(None));
}

/// #1856 review: the debug entrypoint shares the planner with the production
/// path, so a REJECTED stamped debug command must not leave its deadline stamp
/// behind for a later unstamped debug cancel (the debug path used to bypass
/// the per-command scratch cleanup in `apply_command_events`).
#[test]
fn a_rejected_stamped_debug_command_does_not_leak_its_deadline() {
    let mut engine = Engine::new();
    engine
        .apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    engine
        .apply_command(Command::create_instance("order"))
        .unwrap();
    let job_key = engine.pending_jobs()[0].key;
    let mut bad = Command::complete_job(job_key);
    if let Command::CompleteJob { attribution, .. } = &mut bad {
        *attribution = Some(JobAttribution {
            worker: None,
            deadline: Some(999),
        });
    }
    // Strict engine: completing a never-activated job rejects after stamping.
    assert!(engine.debug_command_at(bad, 0, Vec::new()).is_err());
    let instance_key = engine.state().jobs[&job_key].instance_key;
    let session = engine
        .debug_command_at(Command::CancelInstance { instance_key }, 0, Vec::new())
        .unwrap();
    assert_eq!(terminal_deadline(session.log(), job_key), Some(None));
}

/// #1856 review: a debug run that runs to completion clears its per-command
/// stamps, so nothing can leak into a later command.
#[test]
fn a_finished_debug_run_clears_its_deadline_stamps() {
    let (mut leader, _follower, job_key) = leader_and_follower("W");
    let mut stamped = Command::complete_job(job_key);
    leader.stamp_terminal_attribution(&mut stamped);
    let session = leader.debug_command_at(stamped, 0, Vec::new()).unwrap();
    assert!(!session.is_paused());
    assert!(leader.terminal_deadline_stamps.is_empty());
}

/// A three-level call-activity tree (`top` → `mid` → `leaf`) with a soft job on
/// `top`'s other branch and one in `leaf`, then `mid` spilled cold exactly as a
/// host's cold tier does (`snapshot_instance`).
fn tree_with_cold_intermediate() -> (Engine, Key, Key, Key, Key) {
    let leaf = ProcessBuilder::new("cold_leaf")
        .start_event("ls")
        .service_task("lt", "leaf_work")
        .end_event("le")
        .connect("ls", "lt")
        .connect("lt", "le")
        .build()
        .unwrap();
    let mid = ProcessBuilder::new("cold_mid")
        .start_event("ms")
        .call_activity("mcall", "cold_leaf")
        .end_event("me")
        .connect("ms", "mcall")
        .connect("mcall", "me")
        .build()
        .unwrap();
    let top = ProcessBuilder::new("cold_top")
        .start_event("ts")
        .parallel_gateway("fork")
        .call_activity("tcall", "cold_mid")
        .service_task("tt", "top_work")
        .terminate_end_event("term")
        .end_event("te")
        .connect("ts", "fork")
        .connect("fork", "tcall")
        .connect("fork", "tt")
        .connect("tcall", "term")
        .connect("tt", "te")
        .build()
        .unwrap();
    let mut e = Engine::new();
    e.apply_command(Command::SetLenientCompletion { lenient: true })
        .unwrap();
    for p in [leaf, mid, top] {
        e.apply_command(Command::DeployProcess(p)).unwrap();
    }
    e.apply_command(Command::create_instance("cold_top"))
        .unwrap();
    let key_of = |e: &Engine, id: &str| {
        e.state()
            .instances
            .values()
            .find(|i| i.process_id == id)
            .unwrap()
            .key
    };
    let (top_key, mid_key) = (key_of(&e, "cold_top"), key_of(&e, "cold_mid"));
    let top_job = e.activate_jobs("top_work", "w", 1, 7_000, 100)[0].key;
    let leaf_job = e.activate_jobs("leaf_work", "w", 1, 3_000, 100)[0].key;
    assert!(e.snapshot_instance(mid_key).is_some(), "mid spills cold");
    assert!(!e.state().instances.contains_key(&mid_key));
    (e, top_key, mid_key, top_job, leaf_job)
}

/// The host's cold tier for [`tree_with_cold_intermediate`]: `mid` is cold.
struct OneColdLink {
    parent: Key,
    child: Key,
}

impl ColdTopology for OneColdLink {
    fn parent_of(&self, key: Key) -> Option<Key> {
        (key == self.child).then_some(self.parent)
    }

    fn children_of(&self, parent: Key) -> Vec<Key> {
        if parent == self.parent {
            vec![self.child]
        } else {
            Vec::new()
        }
    }
}

/// #1856 review: a cold call-activity intermediate must not hide the soft
/// jobs below it (descent) or above it (ascent) from the deadline resolver.
#[test]
fn cascade_deadlines_walk_through_a_cold_intermediate() {
    let (e, top_key, mid_key, top_job, leaf_job) = tree_with_cold_intermediate();
    let cold = OneColdLink {
        parent: top_key,
        child: mid_key,
    };
    // Descent: cancelling `top` reaches `leaf` only through cold `mid`.
    let down = e.resolve_cascade_terminal_deadlines_with(
        &Command::CancelInstance {
            instance_key: top_key,
        },
        &cold,
    );
    assert_eq!(down.get(&leaf_job), Some(&3_100));
    assert_eq!(down.get(&top_job), Some(&7_100));
    // Ascent: completing `leaf`'s job reaches `top` only through cold `mid`.
    let up = e.resolve_cascade_terminal_deadlines_with(&Command::complete_job(leaf_job), &cold);
    assert_eq!(up.get(&top_job), Some(&7_100));
    // Without the cold topology the resident-only walk misses them — the gap
    // the `_with` variant closes for hosts with a cold tier.
    let resident_only = e.resolve_cascade_terminal_deadlines(&Command::CancelInstance {
        instance_key: top_key,
    });
    assert_eq!(resident_only.get(&leaf_job), None);
}

/// #1856 review: the cascade deadline pass must select only jobs in reached
/// instances whichever side it iterates (the reached instances' job lists
/// when few, the live activation set otherwise) — unrelated activated jobs
/// in other instances never leak in, and both branches agree.
#[test]
fn cascade_deadlines_ignore_unrelated_activated_jobs_on_both_iteration_sides() {
    let mut e = Engine::new();
    e.apply_command(Command::SetLenientCompletion { lenient: true })
        .unwrap();
    e.apply_command(Command::DeployProcess(linear_with_task()))
        .unwrap();
    for _ in 0..5 {
        e.apply_command(Command::create_instance("order")).unwrap();
    }
    let activated = e.activate_jobs("payment", "W", 5, 1_000, 0);
    assert_eq!(activated.len(), 5);
    let target = activated[0].key;
    let instance_key = e.state().jobs[&target].instance_key;
    // Few reached jobs vs many activations: per-instance side.
    let deadlines = e.resolve_cascade_terminal_deadlines(&Command::CancelInstance { instance_key });
    assert_eq!(
        deadlines.into_iter().collect::<Vec<_>>(),
        vec![(target, 1_000)]
    );
    // Activation-set side: a sequential three-job process where the target
    // instance accumulates retained job history (two completed jobs) while an
    // unrelated instance keeps a live activation, so reached jobs (3) exceed
    // live activations (2) and the unrelated job must be filtered out.
    let seq = ProcessBuilder::new("seq3")
        .start_event("s")
        .service_task("t1", "seq_a")
        .service_task("t2", "seq_b")
        .service_task("t3", "seq_c")
        .end_event("e")
        .connect("s", "t1")
        .connect("t1", "t2")
        .connect("t2", "t3")
        .connect("t3", "e")
        .build()
        .unwrap();
    let mut e = Engine::new();
    e.apply_command(Command::SetLenientCompletion { lenient: true })
        .unwrap();
    e.apply_command(Command::DeployProcess(seq)).unwrap();
    e.apply_command(Command::create_instance("seq3")).unwrap();
    e.apply_command(Command::create_instance("seq3")).unwrap();
    let firsts = e.activate_jobs("seq_a", "W", 2, 1_000, 0);
    assert_eq!(firsts.len(), 2);
    let (mine, other) = (firsts[0].key, firsts[1].key);
    let instance_key = e.state().jobs[&mine].instance_key;
    e.apply_command(Command::complete_job(mine)).unwrap();
    let second = e.activate_jobs("seq_b", "W", 1, 2_000, 0)[0].key;
    e.apply_command(Command::complete_job(second)).unwrap();
    let target = e.activate_jobs("seq_c", "W", 1, 3_000, 0)[0].key;
    assert_eq!(e.state().jobs[&target].instance_key, instance_key);
    let reached_jobs = e.state().jobs_by_instance[&instance_key].len();
    assert!(
        reached_jobs > e.state().activated_jobs.len(),
        "precondition: activation-set branch ({reached_jobs} reached jobs)"
    );
    assert!(e.state().activated_jobs.contains(&other));
    let deadlines = e.resolve_cascade_terminal_deadlines(&Command::CancelInstance { instance_key });
    assert_eq!(
        deadlines.into_iter().collect::<Vec<_>>(),
        vec![(target, 3_000)]
    );
}

/// #1856 review: a soft-activated job migrated onto an agent-marked target is
/// still a soft (unjournaled) activation, so the cascade stamp must cover it —
/// stamp eligibility and `job_terminal_deadline`'s fallback must use the SAME
/// soft-activation predicate (live lock state, not model-derived durability).
#[test]
fn a_soft_job_migrated_to_an_agent_marked_target_keeps_its_cascade_deadline() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    let agent_marked = ProcessBuilder::new("order")
        .start_event("start")
        .agent_task("charge", "payment", crate::agent::AgentType::External)
        .end_event("end")
        .connect("start", "charge")
        .connect("charge", "end")
        .build()
        .unwrap();
    let mut target = None;
    for replica in [&mut leader, &mut follower] {
        let events = replica
            .apply_command(Command::DeployProcess(agent_marked.clone()))
            .unwrap();
        target = events.iter().find_map(|e| match e {
            Event::ProcessDeployed {
                process_definition_key,
                ..
            } => Some(*process_definition_key),
            _ => None,
        });
    }
    let instance_key = leader.state().jobs[&job_key].instance_key;
    replicate_stamped(
        &mut leader,
        &mut follower,
        Command::MigrateInstance {
            instance_key,
            target_process_definition_key: target.unwrap(),
            mapping_instructions: vec![("charge".into(), "charge".into())],
        },
        100,
    );
    assert!(
        leader.state().jobs[&job_key].lease_token.is_none()
            && !leader.state().jobs[&job_key].durable_activation,
        "the migrated job is still a soft activation"
    );
    let (on_leader, on_follower) = replicate_stamped(
        &mut leader,
        &mut follower,
        Command::CancelInstance { instance_key },
        200,
    );
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(Some(1_000)));
    assert_eq!(terminal_deadline(&on_follower, job_key), Some(Some(1_000)));
}
