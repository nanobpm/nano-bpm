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
            e.set_lenient_completion(true);
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
#[test]
fn a_soft_jobs_stamped_terminal_deadline_agrees_on_every_replica() {
    let (mut leader, mut follower, job_key) = leader_and_follower("W");
    let lock = leader.state().jobs[&job_key].deadline;
    assert_eq!(lock, Some(1_000), "leader holds the soft lock");
    assert_eq!(follower.state().jobs[&job_key].deadline, None);

    let mut command = Command::complete_job(job_key);
    leader.stamp_terminal_attribution(&mut command);
    let on_leader = leader.apply_command(command.clone()).unwrap();
    let on_follower = follower.apply_command(command).unwrap();
    assert_eq!(terminal_deadline(&on_leader, job_key), Some(lock));
    assert_eq!(
        terminal_deadline(&on_follower, job_key),
        Some(lock),
        "follower persists the leader-stamped deadline"
    );
    assert_eq!(
        leader.job_terminal_deadline(job_key),
        follower.job_terminal_deadline(job_key)
    );
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
