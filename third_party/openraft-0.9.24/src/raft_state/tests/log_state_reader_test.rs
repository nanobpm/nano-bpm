use crate::engine::LogIdList;
use crate::raft_state::LogStateReader;
use crate::CommittedLeaderId;
use crate::LogId;
use crate::RaftState;
use crate::TokioInstant;

fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::<u64> {
        leader_id: CommittedLeaderId::new(term, 0),
        index,
    }
}

#[test]
fn test_raft_state_prev_log_id() -> anyhow::Result<()> {
    // There is log id at 0
    {
        let rs = RaftState::<u64, (), TokioInstant> {
            log_ids: LogIdList::new(vec![log_id(0, 0), log_id(1, 1), log_id(3, 4)]),
            ..Default::default()
        };

        assert_eq!(None, rs.prev_log_id(0));
        assert_eq!(Some(log_id(0, 0)), rs.prev_log_id(1));
        assert_eq!(Some(log_id(1, 3)), rs.prev_log_id(4));
        assert_eq!(Some(log_id(3, 4)), rs.prev_log_id(5));
    }

    // There is no log id at 0
    {
        let rs = RaftState::<u64, (), TokioInstant> {
            log_ids: LogIdList::new(vec![log_id(1, 1), log_id(3, 4)]),
            ..Default::default()
        };

        assert_eq!(None, rs.prev_log_id(0));
        assert_eq!(None, rs.prev_log_id(1));
        assert_eq!(Some(log_id(1, 1)), rs.prev_log_id(2));
        assert_eq!(Some(log_id(1, 3)), rs.prev_log_id(4));
        assert_eq!(Some(log_id(3, 4)), rs.prev_log_id(5));
    }
    Ok(())
}

#[test]
fn test_raft_state_has_log_id_empty() -> anyhow::Result<()> {
    let rs = RaftState::<u64, (), TokioInstant>::default();

    assert!(!rs.has_log_id(&log_id(0, 0)));

    Ok(())
}

#[test]
fn test_raft_state_has_log_id_committed_gets_true() -> anyhow::Result<()> {
    let rs = RaftState::<u64, (), TokioInstant> {
        committed: Some(log_id(2, 1)),
        ..Default::default()
    };

    assert!(rs.has_log_id(&log_id(0, 0)));
    assert!(rs.has_log_id(&log_id(2, 1)));
    assert!(!rs.has_log_id(&log_id(2, 2)));

    Ok(())
}

/// The committed fast-path must not panic on a same-term divergent lineage.
///
/// During a two-lineage hand-off a follower can receive an AppendEntries whose
/// `prev_log_id` sits below its committed index but on a DIFFERENT (same-term
/// divergent) lineage: the entry at that index was committed under another
/// leader id. `has_log_id`'s committed fast-path asserted
/// `Some(log_id) <= committed`, which is false for such a log id, and panicked
/// (the CI failure on
/// `recovery_tick_hands_off_a_boot_deferred_partition_from_the_epoch_map`).
/// The fast-path must instead fall through to the log-id comparison, which
/// correctly reports the divergent entry as not-present (a conflict to
/// resolve), never a panic.
#[test]
fn test_raft_state_has_log_id_divergent_committed_lineage_does_not_panic() -> anyhow::Result<()> {
    // Committed at index 1 under leader term 2; the local log at index 1 is
    // (term 2). An AppendEntries arrives whose prev_log_id is index 1 but on a
    // divergent lineage (term 3 — same index, different committed leader id).
    let rs = RaftState::<u64, (), TokioInstant> {
        committed: Some(log_id(2, 1)),
        log_ids: LogIdList::new(vec![log_id(2, 1)]),
        ..Default::default()
    };

    // Same index as a committed entry, but a different (divergent) leader id:
    // must return false (a conflict to resolve), NOT panic.
    assert!(!rs.has_log_id(&log_id(3, 1)));
    // The genuinely-committed log id still returns true.
    assert!(rs.has_log_id(&log_id(2, 1)));

    Ok(())
}

#[test]
fn test_raft_state_has_log_id_in_log_id_list() -> anyhow::Result<()> {
    let rs = RaftState::<u64, (), TokioInstant> {
        committed: Some(log_id(2, 1)),
        log_ids: LogIdList::new(vec![log_id(1, 2), log_id(3, 4)]),
        ..Default::default()
    };

    assert!(rs.has_log_id(&log_id(0, 0)));
    assert!(rs.has_log_id(&log_id(2, 1)));
    assert!(rs.has_log_id(&log_id(1, 3)));
    assert!(rs.has_log_id(&log_id(3, 4)));

    assert!(!rs.has_log_id(&log_id(2, 3)));
    assert!(!rs.has_log_id(&log_id(2, 4)));
    assert!(!rs.has_log_id(&log_id(3, 5)));

    Ok(())
}

#[test]
fn test_raft_state_last_log_id() -> anyhow::Result<()> {
    let rs = RaftState::<u64, (), TokioInstant> {
        log_ids: LogIdList::new(vec![]),
        ..Default::default()
    };

    assert_eq!(None, rs.last_log_id());

    let rs = RaftState::<u64, (), TokioInstant> {
        log_ids: LogIdList::new(vec![log_id(1, 2)]),
        ..Default::default()
    };
    assert_eq!(Some(&log_id(1, 2)), rs.last_log_id());

    let rs = RaftState::<u64, (), TokioInstant> {
        log_ids: LogIdList::new(vec![log_id(1, 2), log_id(3, 4)]),
        ..Default::default()
    };
    assert_eq!(Some(&log_id(3, 4)), rs.last_log_id());

    Ok(())
}

#[test]
fn test_raft_state_purge_upto() -> anyhow::Result<()> {
    let rs = RaftState::<u64, (), TokioInstant> {
        purge_upto: Some(log_id(1, 2)),
        ..Default::default()
    };

    assert_eq!(Some(&log_id(1, 2)), rs.purge_upto());

    Ok(())
}

#[test]
fn test_raft_state_last_purged_log_id() -> anyhow::Result<()> {
    let rs = RaftState::<u64, (), TokioInstant> {
        log_ids: LogIdList::new(vec![]),
        ..Default::default()
    };

    assert_eq!(None, rs.last_purged_log_id());

    let rs = RaftState::<u64, (), TokioInstant> {
        log_ids: LogIdList::new(vec![log_id(1, 2)]),
        purged_next: 3,
        ..Default::default()
    };
    assert_eq!(Some(log_id(1, 2)), rs.last_purged_log_id().copied());

    let rs = RaftState::<u64, (), TokioInstant> {
        log_ids: LogIdList::new(vec![log_id(1, 2), log_id(3, 4)]),
        purged_next: 3,
        ..Default::default()
    };
    assert_eq!(Some(log_id(1, 2)), rs.last_purged_log_id().copied());

    Ok(())
}
