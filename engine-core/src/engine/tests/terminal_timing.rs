//! Engine-retained terminal job timing (#1440).
//!
//! The engine keeps a terminal (COMPLETED/CANCELED) job until its owning
//! instance is evicted, so a snapshot can carry a terminal job of a still-active
//! instance. A read model reseeded from that snapshot has no events to derive
//! the job's `endTime`/`lastUpdateTime`/retained deadline from, so the engine
//! must retain them (`Job::terminal_end`) — and must retain them identically
//! under every durability path: live apply, journal replay (crash/restart and
//! follower apply), and snapshot + recovery.
use super::*;
use crate::state::{JobState, JobTerminalEnd};

/// start -> fork -> {a(ja), b(jb), c(jc)} -> join -> done
fn three_branches() -> ProcessDefinition {
    ProcessBuilder::new("three")
        .start_event("start")
        .parallel_gateway("fork")
        .service_task("a", "ja")
        .service_task("b", "jb")
        .service_task("c", "jc")
        .parallel_gateway("join")
        .end_event("done")
        .connect("start", "fork")
        .connect("fork", "a")
        .connect("fork", "b")
        .connect("fork", "c")
        .connect("a", "join")
        .connect("b", "join")
        .connect("c", "join")
        .connect("join", "done")
        .build()
        .unwrap()
}

fn job_of(engine: &Engine, job_type: &str) -> Key {
    engine
        .state()
        .jobs
        .values()
        .find(|j| j.job_type == job_type)
        .unwrap()
        .key
}

/// Drives a parallel instance to one COMPLETED and one CANCELED job while a
/// third branch keeps the instance active, returning the engine, the full
/// event journal, and the two terminal job keys.
fn leader_with_terminal_jobs() -> (Engine, Vec<Event>, Key, Key) {
    let mut leader = Engine::new();
    let mut journal = Vec::new();
    journal.extend(
        leader
            .apply_command_at(Command::DeployProcess(three_branches()), 1_000)
            .unwrap(),
    );
    let created = leader
        .apply_command_at(Command::create_instance("three"), 2_000)
        .unwrap();
    let inst = created.iter().find_map(|e| e.instance_key()).unwrap();
    journal.extend(created);
    journal.extend(
        leader
            .apply_command_at(Command::activate_jobs("ja", "w", 1, 60_000, 3_000), 3_000)
            .unwrap(),
    );
    journal.extend(
        leader
            .apply_command_at(Command::activate_jobs("jb", "w", 1, 40_000, 3_500), 3_500)
            .unwrap(),
    );
    let ja = job_of(&leader, "ja");
    let jb = job_of(&leader, "jb");
    journal.extend(
        leader
            .apply_command_at(Command::complete_job(ja), 5_000)
            .unwrap(),
    );
    let b_eik = active_eik_of(&leader, inst, "b");
    journal.extend(
        leader
            .apply_command_at(Command::modify_instance(inst, vec![], vec![b_eik]), 7_000)
            .unwrap(),
    );
    assert_eq!(
        leader.instance(inst).unwrap().state,
        ProcessInstanceState::Active,
        "branch c keeps the instance (and so its terminal jobs) alive"
    );
    (leader, journal, ja, jb)
}

fn expected() -> [(JobState, JobTerminalEnd); 2] {
    [
        (
            JobState::Completed,
            JobTerminalEnd {
                at: 5_000,
                deadline: Some(63_000),
            },
        ),
        (
            JobState::Canceled,
            JobTerminalEnd {
                at: 7_000,
                deadline: Some(43_500),
            },
        ),
    ]
}

fn assert_terminal_timing(engine: &Engine, ja: Key, jb: Key, path: &str) {
    for (key, (state, end)) in [ja, jb].into_iter().zip(expected()) {
        let job = engine.job(key).unwrap();
        assert_eq!(job.state, state, "{path}: job {key} state");
        assert_eq!(job.deadline, None, "{path}: the live lock is released");
        assert_eq!(
            job.terminal_end,
            Some(end),
            "{path}: job {key} retains its terminal instant and last deadline"
        );
    }
}

#[test]
fn terminal_job_timing_is_retained_on_the_live_path() {
    let (leader, _, ja, jb) = leader_with_terminal_jobs();
    assert_terminal_timing(&leader, ja, jb, "live");
}

#[test]
fn terminal_job_timing_survives_journal_replay() {
    // Crash/restart and follower apply both rebuild state by replaying the
    // journal. The instant comes from the EVENT (`completed_at`/`canceled_at`),
    // never from the replaying node's clock (replay resets it to 0), so every
    // replica reconstructs the identical value.
    let (leader, journal, ja, jb) = leader_with_terminal_jobs();
    let replayed = Engine::replay(journal);
    assert_terminal_timing(&replayed, ja, jb, "replay");
    assert_eq!(
        replayed.job(ja).unwrap().terminal_end,
        leader.job(ja).unwrap().terminal_end
    );
}

#[cfg(feature = "serde")]
#[test]
fn terminal_job_timing_survives_snapshot_recovery() {
    let (leader, journal, ja, jb) = leader_with_terminal_jobs();
    for (path, source) in [("leader", leader), ("replayed", Engine::replay(journal))] {
        let bytes = serde_json::to_vec(&source.snapshot()).expect("snapshot serializes");
        let decoded: EngineSnapshot =
            serde_json::from_slice(&bytes).expect("snapshot deserializes");
        let restored = Engine::from_snapshot(decoded);
        assert_terminal_timing(&restored, ja, jb, path);
    }
}

#[test]
fn the_first_terminal_transition_wins_and_a_pre_field_event_records_nothing() {
    let (mut leader, _, ja, _) = leader_with_terminal_jobs();
    let inst = leader.job(ja).unwrap().instance_key;
    // A re-delivered / later terminal event must not move the retained
    // instant (mirrors the read model's `end_ms IS NULL` guard).
    crate::state::apply(
        &mut leader.state,
        &Event::JobCanceled {
            job_key: ja,
            instance_key: inst,
            canceled_at: 99_000,
            deadline: None,
        },
    );
    assert_eq!(
        leader.job(ja).unwrap().terminal_end,
        Some(expected()[0].1),
        "first terminal transition wins"
    );

    // A journal written before the field existed decodes with `0` (unknown):
    // the job goes terminal but no fabricated epoch-zero instant is recorded.
    let jc = job_of(&leader, "jc");
    crate::state::apply(
        &mut leader.state,
        &Event::JobCompleted {
            job_key: jc,
            instance_key: inst,
            created_at: 0,
            job_type: "jc".into(),
            worker: None,
            completed_at: 0,
            deadline: None,
        },
    );
    let job = leader.job(jc).unwrap();
    assert_eq!(job.state, JobState::Completed);
    assert_eq!(job.terminal_end, None);
}

#[cfg(feature = "serde")]
#[test]
fn a_pre_field_journal_record_decodes_without_an_instant() {
    // Replay-compatibility (AGENTS.md): an old record without the new field
    // must still decode, with the documented `0` default.
    let old = r#"{"JobCanceled":{"job_key":7,"instance_key":3}}"#;
    match crate::decode_event_json(old).expect("legacy JobCanceled decodes") {
        Event::JobCanceled { canceled_at, .. } => assert_eq!(canceled_at, 0),
        other => panic!("unexpected {other:?}"),
    }
    let old = r#"{"JobCompleted":{"job_key":7,"instance_key":3}}"#;
    match crate::decode_event_json(old).expect("legacy JobCompleted decodes") {
        Event::JobCompleted { completed_at, .. } => assert_eq!(completed_at, 0),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn a_leader_local_activation_omitted_from_the_journal_still_replays_the_same_terminal_deadline() {
    // Leader-local (soft) activation is never journaled: a follower's journal
    // omits every `JobActivated`, so the follower's live `job.deadline` is
    // `None` at the terminal transition while the leader's is set. The terminal
    // deadline must come from the replicated terminal EVENT, so every replica
    // (and a snapshot reseed off any of them) agrees with the leader.
    let (leader, journal, ja, jb) = leader_with_terminal_jobs();
    let follower_journal: Vec<Event> = journal
        .into_iter()
        .filter(|e| !matches!(e, Event::JobActivated { .. }))
        .collect();
    let follower = Engine::replay(follower_journal);
    for key in [ja, jb] {
        assert_eq!(
            follower.job(key).unwrap().terminal_end,
            leader.job(key).unwrap().terminal_end,
            "job {key}: follower diverged from leader"
        );
    }
    assert_terminal_timing(&follower, ja, jb, "soft-activation follower");
}

#[test]
fn a_follower_applying_a_replicated_completion_stamps_the_leader_resolved_deadline() {
    // The REAL Raft path: replicas do not replay the leader's events — each
    // re-runs the replicated COMMAND on its own engine and journals the events
    // it produces. A follower's engine never saw the leader-local (soft)
    // activation, so its job has no lock; without the command-carried
    // `terminal_deadline` its terminal event (and snapshot) would lose the
    // deadline the leader persisted. This drives both sides through
    // `apply_command_at` on the SAME replicated command and requires the two
    // journals — and the two retained terminal records — to be identical.
    let mut leader = Engine::new();
    leader.set_lenient_completion(true);
    leader
        .apply_command_at(Command::DeployProcess(three_branches()), 1_000)
        .unwrap();
    leader
        .apply_command_at(Command::create_instance("three"), 2_000)
        .unwrap();
    // Soft activation on the leader ONLY: it sets the lock in the leader's
    // state but (being leader-local) is never replicated.
    leader
        .apply_command_at(Command::activate_jobs("ja", "w", 1, 60_000, 3_000), 3_000)
        .unwrap();
    let ja = job_of(&leader, "ja");
    let leader_deadline = leader.job(ja).unwrap().deadline;
    assert_eq!(leader_deadline, Some(63_000));

    // The follower: same replicated prefix MINUS the soft activation.
    let mut follower = Engine::new();
    follower.set_lenient_completion(true);
    follower
        .apply_command_at(Command::DeployProcess(three_branches()), 1_000)
        .unwrap();
    follower
        .apply_command_at(Command::create_instance("three"), 2_000)
        .unwrap();
    assert_eq!(follower.job(ja).unwrap().deadline, None);

    // The leader stamps its resolved deadline onto the command before
    // proposing it (server/crates/nano-server-raft `propose_partition_command`).
    let command =
        Command::complete_job(ja).with_terminal_deadline(leader.job_terminal_deadline(ja));
    let leader_events = leader.apply_command_at(command.clone(), 5_000).unwrap();
    let follower_events = follower.apply_command_at(command, 5_000).unwrap();

    // The terminal event each replica journals carries the SAME
    // leader-resolved deadline. (The events' `worker` attribution is a
    // separate, pre-existing leader-local divergence class — a soft
    // activation's worker is as unjournaled as its lock; only the deadline is
    // terminal-retained state, so it is the field this fix replicates.)
    let terminal = |events: &[Event]| {
        events
            .iter()
            .find_map(|e| match e {
                Event::JobCompleted { deadline, .. } => Some(*deadline),
                _ => None,
            })
            .expect("a JobCompleted event")
    };
    assert_eq!(terminal(&leader_events), Some(63_000));
    assert_eq!(
        terminal(&follower_events),
        terminal(&leader_events),
        "the follower's terminal event must carry the leader's deadline"
    );
    assert_eq!(
        follower.job(ja).unwrap().terminal_end,
        leader.job(ja).unwrap().terminal_end,
    );
    assert_eq!(
        follower.job(ja).unwrap().terminal_end,
        Some(JobTerminalEnd {
            at: 5_000,
            deadline: Some(63_000),
        }),
        "the command-carried deadline reaches the follower's retained terminal record"
    );
}

#[test]
fn a_replicated_completion_without_the_leader_stamp_falls_back_to_local_state() {
    // The direct (single-node / test) path proposes no `terminal_deadline`:
    // the engine reads its own lock — unchanged pre-#1440 behaviour.
    let mut engine = Engine::new();
    engine
        .apply_command_at(Command::DeployProcess(three_branches()), 1_000)
        .unwrap();
    engine
        .apply_command_at(Command::create_instance("three"), 2_000)
        .unwrap();
    engine
        .apply_command_at(Command::activate_jobs("ja", "w", 1, 60_000, 3_000), 3_000)
        .unwrap();
    let ja = job_of(&engine, "ja");
    engine
        .apply_command_at(Command::complete_job(ja), 5_000)
        .unwrap();
    assert_eq!(
        engine.job(ja).unwrap().terminal_end,
        Some(JobTerminalEnd {
            at: 5_000,
            deadline: Some(63_000),
        }),
    );
}

#[test]
fn a_cascade_recancel_of_a_terminal_job_restates_its_first_terminal_deadline() {
    // First-terminal-wins across a cascade: a job that went terminal with a
    // retained deadline (its live lock long cleared) can be swept again by a
    // later instance-discard cascade. The re-cancel must restate the RETAINED
    // terminal deadline, not the cleared live lock (`None`), or a follower
    // applying the cascade would persist a different terminal record.
    let (mut leader, _, ja, jb) = leader_with_terminal_jobs();
    let inst = leader.job(ja).unwrap().instance_key;
    // Both jobs are terminal; their live locks are cleared. Terminating the
    // instance cascades a discard over every in-play token — and any job it
    // still sees as cancellable.
    assert_eq!(leader.job(ja).unwrap().deadline, None);
    assert_eq!(leader.job(jb).unwrap().deadline, None);
    let events = leader
        .apply_command_at(Command::cancel_instance(inst), 9_000)
        .unwrap();
    // The terminal jobs were already terminal, so the cascade does not
    // re-cancel them (only Created/Activated/Failed jobs are swept); the
    // retained records are untouched.
    assert_eq!(
        leader.job(ja).unwrap().terminal_end,
        Some(expected()[0].1),
        "terminal record survives the cascade"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceTerminated { .. })),
        "the instance itself terminated"
    );
}
