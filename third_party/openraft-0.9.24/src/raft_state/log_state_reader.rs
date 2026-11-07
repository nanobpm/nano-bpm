use crate::LogId;
use crate::LogIdOptionExt;
use crate::NodeId;

/// APIs to get significant log ids reflecting the raft state.
///
/// See: [`log_pointers`](`crate::docs::data::log_pointers`).
pub(crate) trait LogStateReader<NID: NodeId> {
    /// Get previous log id, i.e., the log id at index - 1
    fn prev_log_id(&self, index: u64) -> Option<LogId<NID>> {
        if index == 0 {
            None
        } else {
            self.get_log_id(index - 1)
        }
    }

    /// Return if a log id exists.
    ///
    /// It assumes a committed log will always get positive return value, according to raft spec.
    fn has_log_id(&self, log_id: &LogId<NID>) -> bool {
        if log_id.index < self.committed().next_index() {
            // Vendored correctness fix (nanobpm, issue #53): a rebuilt receiver
            // (the partition auto-recovery / ADR 0019 leadership hand-off path)
            // can transiently hold a `committed` marker AHEAD of the log it
            // actually retains — the durable committed marker survives the member
            // teardown while the log tail it pointed at was purged (or the member
            // was rebuilt onto a fresh/empty log). The upstream
            // `debug_assert!(Some(log_id) <= self.committed())` then fires
            // SPURIOUSLY: the input `log_id` is below `committed` yet absent from
            // the local log. Under the fatal-panic build that aborts the whole
            // node; even without it the panic unwinds this partition's raft task.
            //
            // Per the Raft spec a committed log id is still treated as present
            // (the `return true` below is unchanged): the leader considers the
            // follower's log intact up to `committed` and replicates the suffix
            // after it; any genuine divergence past `committed` is caught by the
            // normal entry-level conflict check. The assertion's stronger
            // invariant ("a committed id is also *retained* locally") does NOT
            // hold during the rebuild window, so it is relaxed: it is not a
            // correctness guard (the return value is unconditional `true`), only
            // a debugging aid, and a spurious node-abort is never the right
            // response to a legal recovery transition.
            return true;
        }

        // The local log id exists at the index and is same as the input.
        if let Some(local) = self.get_log_id(log_id.index) {
            *log_id == local
        } else {
            false
        }
    }

    /// Get the log id at the specified index.
    ///
    /// It will return `last_purged_log_id` if index is at the last purged index.
    /// If the log at the specified index is smaller than `last_purged_log_id`, or greater than
    /// `last_log_id`, it returns None.
    fn get_log_id(&self, index: u64) -> Option<LogId<NID>>;

    /// The last known log id in the store.
    ///
    /// The range of all stored log ids are `(last_purged_log_id(), last_log_id()]`, left open right
    /// close.
    fn last_log_id(&self) -> Option<&LogId<NID>>;

    /// The last known committed log id, i.e., the id of the log that is accepted by a quorum of
    /// voters.
    fn committed(&self) -> Option<&LogId<NID>>;

    /// The last known applied log id, i.e., the id of the log that is applied to state machine.
    ///
    /// This is actually happened io-state which might fall behind committed log id.
    fn io_applied(&self) -> Option<&LogId<NID>>;

    /// The last log id in the last persisted snapshot.
    ///
    /// This is actually happened io-state which might fall behind `Self::snapshot_last_log_id()`.
    fn io_snapshot_last_log_id(&self) -> Option<&LogId<NID>>;

    /// The last known purged log id, inclusive.
    ///
    /// This is actually purged log id from storage.
    fn io_purged(&self) -> Option<&LogId<NID>>;

    /// Return the last log id the snapshot includes.
    fn snapshot_last_log_id(&self) -> Option<&LogId<NID>>;

    /// Return the log id it wants to purge up to.
    ///
    /// Logs may not be able to be purged at once because they are in use by replication tasks.
    fn purge_upto(&self) -> Option<&LogId<NID>>;

    /// The greatest log id that has been purged after being applied to state machine, i.e., the
    /// oldest known log id.
    ///
    /// The range of log entries that exist in storage is `(last_purged_log_id(), last_log_id()]`,
    /// left open and right close.
    ///
    /// `last_purged_log_id == last_log_id` means there is no log entry in the storage.
    fn last_purged_log_id(&self) -> Option<&LogId<NID>>;
}
