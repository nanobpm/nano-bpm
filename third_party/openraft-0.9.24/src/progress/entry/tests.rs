use std::borrow::Borrow;

use crate::progress::entry::ProgressEntry;
use crate::progress::inflight::Inflight;
use crate::raft_state::LogStateReader;
use crate::CommittedLeaderId;
use crate::LogId;

fn log_id(index: u64) -> LogId<u64> {
    LogId {
        leader_id: CommittedLeaderId::new(1, 1),
        index,
    }
}

fn inflight_logs(prev_index: u64, last_index: u64) -> Inflight<u64> {
    Inflight::logs(Some(log_id(prev_index)), Some(log_id(last_index)))
}

#[test]
fn test_is_log_range_inflight() -> anyhow::Result<()> {
    let mut pe = ProgressEntry::empty(20);
    assert_eq!(false, pe.is_log_range_inflight(&log_id(2)));

    pe.inflight = inflight_logs(2, 4);
    assert_eq!(false, pe.is_log_range_inflight(&log_id(1)));
    assert_eq!(false, pe.is_log_range_inflight(&log_id(2)));
    assert_eq!(true, pe.is_log_range_inflight(&log_id(3)));
    assert_eq!(true, pe.is_log_range_inflight(&log_id(4)));
    assert_eq!(true, pe.is_log_range_inflight(&log_id(5)));

    pe.inflight = Inflight::snapshot(Some(log_id(5)));
    assert_eq!(false, pe.is_log_range_inflight(&log_id(5)));

    Ok(())
}

#[test]
fn test_update_matching() -> anyhow::Result<()> {
    // Update matching and inflight
    {
        let mut pe = ProgressEntry::empty(20);
        pe.inflight = inflight_logs(5, 10);
        pe.update_matching(pe.inflight.id(), Some(log_id(6)))?;
        assert_eq!(inflight_logs(6, 10), pe.inflight);
        assert_eq!(Some(log_id(6)), pe.matching);
        assert_eq!(20, pe.searching_end);

        pe.update_matching(pe.inflight.id(), Some(log_id(10)))?;
        assert_eq!(Inflight::None, pe.inflight);
        assert_eq!(Some(log_id(10)), pe.matching);
        assert_eq!(20, pe.searching_end);
    }

    // `searching_end` should be updated
    {
        let mut pe = ProgressEntry::empty(20);
        pe.matching = Some(log_id(6));
        pe.inflight = inflight_logs(5, 20);

        pe.update_matching(pe.inflight.id(), Some(log_id(20)))?;
        assert_eq!(21, pe.searching_end);
    }

    Ok(())
}

#[test]
fn test_update_conflicting() -> anyhow::Result<()> {
    let mut pe = ProgressEntry::empty(20);
    pe.matching = Some(log_id(3));
    pe.inflight = inflight_logs(5, 10);
    pe.update_conflicting(Some(pe.inflight.id()), 5)?;
    assert_eq!(Inflight::None, pe.inflight);
    assert_eq!(&Some(log_id(3)), pe.borrow());
    assert_eq!(5, pe.searching_end);

    Ok(())
}

/// A replication response must be applied only if it answers the CURRENT
/// in-flight request. A success/conflict whose inflight_id does not match the
/// leader's current inflight for that target is STALE (a superseded request,
/// e.g. after a walk-down reset the inflight and a fresh send bumped
/// `curr_inflight_id`) and must be dropped — never applied to the new request.
///
/// Red: a stale success regresses `matching` / a stale conflict shrinks the
/// search window for a request it does not answer.
#[test]
fn test_stale_inflight_success_is_dropped() -> anyhow::Result<()> {
    // The leader walked the follower down to matching=3, then sent a fresh
    // AppendEntries (inflight id 2) for (3..8]. A STALE success for the
    // superseded inflight id 1 (which covered (5..10]) arrives. It must be
    // dropped: applying it would ack the wrong request and jump `matching`
    // past the current search window.
    let mut pe = ProgressEntry::empty(20);
    pe.matching = Some(log_id(3));
    pe.curr_inflight_id = 2;
    pe.inflight = inflight_logs(3, 8).with_id(2);

    // Stale success for the OLD inflight id 1, matching=10 (past the window).
    pe.update_matching(1, Some(log_id(10)))?;

    // The stale response is dropped: matching and inflight are unchanged.
    assert_eq!(Some(log_id(3)), pe.matching);
    assert_eq!(inflight_logs(3, 8).with_id(2), pe.inflight);
    assert_eq!(20, pe.searching_end);

    Ok(())
}

#[test]
fn test_stale_inflight_conflict_is_dropped() -> anyhow::Result<()> {
    // The leader is mid-search with a fresh inflight (id 2) for (3..8]. A STALE
    // conflict for the superseded inflight id 1 arrives, claiming conflict at
    // index 4. It must be dropped: applying it would shrink `searching_end` for
    // a request that is no longer in flight.
    let mut pe = ProgressEntry::empty(20);
    pe.matching = Some(log_id(3));
    pe.curr_inflight_id = 2;
    pe.inflight = inflight_logs(3, 8).with_id(2);

    pe.update_conflicting(Some(1), 4)?;

    // The stale conflict is dropped: searching_end and inflight are unchanged.
    assert_eq!(20, pe.searching_end);
    assert_eq!(inflight_logs(3, 8).with_id(2), pe.inflight);
    assert_eq!(Some(log_id(3)), pe.matching);

    Ok(())
}

/// The matching response (current inflight id) is still applied.
#[test]
fn test_current_inflight_response_is_applied() -> anyhow::Result<()> {
    let mut pe = ProgressEntry::empty(20);
    pe.matching = Some(log_id(3));
    pe.curr_inflight_id = 2;
    pe.inflight = inflight_logs(3, 8).with_id(2);

    pe.update_matching(2, Some(log_id(8)))?;
    assert_eq!(Some(log_id(8)), pe.matching);
    assert_eq!(Inflight::None, pe.inflight);

    Ok(())
}

/// A conflict reported at the follower's REAL last-log frontier (the conflict
/// hint) shrinks the search window straight to that frontier, so the next probe
/// starts there instead of walking down one RPC per step.
#[test]
fn test_conflict_hint_shrinks_search_to_follower_frontier() -> anyhow::Result<()> {
    // The leader believes the follower may match up to index 20 and is probing
    // with inflight (10..15]. The follower actually only has up to index 4 (a
    // divergent/rebuilt receiver): it reports a conflict at its real frontier,
    // index 5 (= follower's last index 4, + 1).
    let mut pe = ProgressEntry::empty(20);
    pe.matching = None;
    pe.curr_inflight_id = 1;
    pe.inflight = inflight_logs(10, 15).with_id(1);

    pe.update_conflicting(Some(1), 5)?;

    // The search window shrinks to the follower's frontier: searching_end = 5,
    // not merely "one step down" from the probe.
    assert_eq!(5, pe.searching_end);
    assert_eq!(Inflight::None, pe.inflight);

    Ok(())
}

/// LogStateReader impl for testing
struct LogState {
    last: Option<LogId<u64>>,
    snap_last: Option<LogId<u64>>,
    purge_upto: Option<LogId<u64>>,
    purged: Option<LogId<u64>>,
}

impl LogState {
    fn new(purge_upto: u64, snap_last: u64, last: u64) -> Self {
        Self {
            last: Some(log_id(last)),
            snap_last: Some(log_id(snap_last)),
            // `next_send()` only checks purge_upto, but not purged,
            // We just fake a purged
            purge_upto: Some(log_id(purge_upto)),
            purged: Some(log_id(purge_upto - 1)),
        }
    }
}

impl LogStateReader<u64> for LogState {
    fn get_log_id(&self, index: u64) -> Option<LogId<u64>> {
        let x = Some(log_id(index));
        if x >= self.purged && x <= self.last {
            x
        } else {
            None
        }
    }

    fn last_log_id(&self) -> Option<&LogId<u64>> {
        self.last.as_ref()
    }

    fn committed(&self) -> Option<&LogId<u64>> {
        unimplemented!("testing")
    }

    fn io_applied(&self) -> Option<&LogId<u64>> {
        todo!()
    }

    fn io_snapshot_last_log_id(&self) -> Option<&LogId<u64>> {
        todo!()
    }

    fn io_purged(&self) -> Option<&LogId<u64>> {
        todo!()
    }

    fn snapshot_last_log_id(&self) -> Option<&LogId<u64>> {
        self.snap_last.as_ref()
    }

    fn purge_upto(&self) -> Option<&LogId<u64>> {
        self.purge_upto.as_ref()
    }

    fn last_purged_log_id(&self) -> Option<&LogId<u64>> {
        self.purged.as_ref()
    }
}

#[test]
fn test_next_send() -> anyhow::Result<()> {
    // There is already inflight data, return it in an Err
    {
        let mut pe = ProgressEntry::empty(20);
        pe.inflight = inflight_logs(10, 11);
        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Err(&inflight_logs(10, 11)), res);
    }

    {
        //    matching,end
        //    4,5
        //    v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(4);
        pe.matching = Some(log_id(4));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&Inflight::snapshot(Some(log_id(10))).with_id(1)), res);
    }
    {
        //    matching,end
        //    4 6
        //    v-v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(6);
        pe.matching = Some(log_id(4));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&Inflight::snapshot(Some(log_id(10))).with_id(1)), res);
    }

    {
        //    matching,end
        //    4  7
        //    v--v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(7);
        pe.matching = Some(log_id(4));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&inflight_logs(6, 20).with_id(1)), res);
    }

    {
        //    matching,end
        //    4              20
        //    v--------------v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(20);
        pe.matching = Some(log_id(4));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&inflight_logs(6, 20).with_id(1)), res);
    }

    //-----------

    {
        //      matching,end
        //      6,7
        //      v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(7);
        pe.matching = Some(log_id(6));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&inflight_logs(6, 20).with_id(1)), res);
    }

    {
        //      matching,end
        //      6, 8
        //      v--v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(8);
        pe.matching = Some(log_id(6));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&inflight_logs(6, 20).with_id(1)), res);
    }

    {
        //      matching,end
        //      6,           20
        //      v------------v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(20);
        pe.matching = Some(log_id(6));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&inflight_logs(6, 20).with_id(1)), res);
    }

    {
        //       matching,end
        //       7,          20
        //       v-----------v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(20);
        pe.matching = Some(log_id(7));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&inflight_logs(7, 20).with_id(1)), res);
    }

    {
        //          matching,end
        //          7,8
        //          v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(8);
        pe.matching = Some(log_id(7));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Ok(&inflight_logs(7, 20).with_id(1)), res);
    }

    {
        //                   matching,end
        //                   20,21
        //                   v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(21);
        pe.matching = Some(log_id(20));

        let res = pe.next_send(&LogState::new(6, 10, 20), 100);
        assert_eq!(Err(&Inflight::None), res, "nothing to send");
    }

    // Test max_entries
    {
        //       matching,end
        //       7,          20
        //       v-----------v
        // -----+------+-----+--->
        //      purged snap  last
        //      6      10    20

        let mut pe = ProgressEntry::empty(20);
        pe.matching = Some(log_id(7));

        let res = pe.next_send(&LogState::new(6, 10, 20), 5);
        assert_eq!(Ok(&inflight_logs(7, 12).with_id(1)), res);
    }
    Ok(())
}
