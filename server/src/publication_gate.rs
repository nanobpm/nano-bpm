//! Publication gating on pending cross-partition subscription opens (#1604).
//!
//! A create answers its client at commit, while its detached owner routes the
//! cross-partition `MessageSubscriptionOpening`s afterwards. Without a gate, a
//! publish issued the moment the create returns could reach the subscription
//! partition before the open does, correlate against nothing and be discarded
//! as unmatched — the instance would stay parked forever.
//!
//! The gate restores the create-then-publish guarantee without making the
//! create wait for routing: before answering, the create owner
//! [`register`](PublicationGate::register)s every in-flight open (by message
//! name + correlation key) and holds the returned [`PendingOpens`] guard until
//! its routing pump has finished. A publish first
//! [`wait`](PublicationGate::wait)s — bounded by a deadline — until no open is
//! pending for its `(name, correlation key)`, so it neither overtakes the open
//! nor blocks forever on a dead target. The guard releases on drop, so a
//! panicking or cancelled routing owner can never wedge publications.
//!
//! **Outcome (#1866).** Releasing is not the same as succeeding: a pump that
//! ends without applying every open (a panicked pump task, an undelivered
//! re-drive, a cancelled owner) must not let the waiting publish proceed into
//! a target whose subscription was never opened. The owner therefore settles
//! the guard with [`PendingOpens::complete`] only when routing applied every
//! open; any other end — [`PendingOpens::fail`] or a bare drop — releases the
//! keys as **failed**, and a publish waiting on a failed key observes
//! [`GateWait::Failed`] instead of `Released`. The publish then proceeds as
//! today (a publish with no open subscription is dropped; a correlate returns
//! not-found) while the owner records the undelivered opens in
//! [`FailedOpens`], which a background task re-drives Zeebe-style
//! (`PendingMessageSubscriptionChecker`): every
//! [`FAILED_OPEN_REDRIVE_INTERVAL`], each open pending longer than
//! [`FAILED_OPEN_REDRIVE_AGE`] is routed again, unbounded, until delivered.
//! Buffering the message itself (TTL) is the follow-up parity gap.
//!
//! **Cluster-wide (#1845).** The registry itself is node-local: the create
//! owner registers its opens on the node that answered the create. A publish
//! entering through ANY node therefore consults the whole cluster before it
//! applies: the gateway waits on its own gate and, in parallel, asks every peer
//! (`AwaitPendingOpens`) to wait on theirs, all bounded by the same deadline,
//! and [`GateWait::combine`]s the outcomes. Creates answer early on a non-Raft
//! node only while every peer advertises
//! `FEATURE_CLUSTER_PUBLICATION_GATE` (so every peer a publish could enter
//! through asks this node); otherwise — and always under Raft — they keep
//! route-before-answer (see `ServerImpl::answer_and_route_create`).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nanobpmn_engine_core::Event;

/// `(message name, correlation key)` of a pending subscription open.
type OpenKey = (String, String);

/// Per-key pending state: the number of in-flight opens and the notifier
/// its waiters park on. Waiters of one key are woken only when THAT key's
/// count reaches zero, never by releases of unrelated keys.
struct Pending {
    count: usize,
    released: Arc<Release>,
}

/// The notifier a key's waiters park on, plus whether any registration of the
/// key ended without routing its open. Set before the final release notifies,
/// so a woken waiter always reads the settled outcome.
#[derive(Default)]
struct Release {
    notify: tokio::sync::Notify,
    failed: std::sync::atomic::AtomicBool,
}

impl Release {
    fn failed(&self) -> bool {
        self.failed.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[derive(Default)]
struct Inner {
    pending: Mutex<HashMap<OpenKey, Pending>>,
}

/// The node-wide registry of pending subscription opens. Cheap to clone.
#[derive(Clone, Default)]
pub(crate) struct PublicationGate {
    inner: Arc<Inner>,
}

/// Outcome of [`PublicationGate::wait`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateWait {
    /// No open was pending; the publish proceeded immediately.
    Clear,
    /// The publish waited until every pending open was routed.
    Released,
    /// A pending open was released WITHOUT being routed (#1866): the pump
    /// failed, so the subscription may not exist yet. The caller proceeds as
    /// for an unmatched publication; the open itself is re-driven in the
    /// background ([`FailedOpens`]).
    Failed,
    /// The deadline elapsed with an open still pending; the caller rejects the
    /// publish as retryable rather than applying it against a missing
    /// subscription.
    TimedOut,
}

impl GateWait {
    /// The wire spelling of this outcome on an `AwaitPendingOpens` reply
    /// (#1845) — the single source both the peer handler and the gateway use.
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            GateWait::Clear => "clear",
            GateWait::Released => "released",
            GateWait::Failed => "failed",
            GateWait::TimedOut => "timedOut",
        }
    }

    /// Parses [`as_wire`](Self::as_wire); `None` for an unknown spelling.
    pub(crate) fn from_wire(s: &str) -> Option<Self> {
        [
            GateWait::Clear,
            GateWait::Released,
            GateWait::Failed,
            GateWait::TimedOut,
        ]
        .into_iter()
        .find(|w| w.as_wire() == s)
    }

    /// Folds the outcomes of several gates (this node's and every peer's) into
    /// the one the publish acts on: any open still pending wins (`TimedOut`,
    /// fail closed), then any failed open (`Failed`), then any waited-on open
    /// (`Released`); `Clear` only when every gate was clear.
    pub(crate) fn combine(self, other: Self) -> Self {
        fn rank(w: GateWait) -> u8 {
            match w {
                GateWait::Clear => 0,
                GateWait::Released => 1,
                GateWait::Failed => 2,
                GateWait::TimedOut => 3,
            }
        }
        if rank(other) > rank(self) {
            other
        } else {
            self
        }
    }
}

/// Registration of a create's in-flight subscription opens. Settle it with
/// [`complete`](Self::complete) once routing applied every open; any other end
/// ([`fail`](Self::fail), or a drop from a panicked/cancelled owner) releases
/// the keys as failed — never wedging the gate, never claiming success.
pub(crate) struct PendingOpens {
    gate: PublicationGate,
    keys: Vec<OpenKey>,
    routed: bool,
}

impl PendingOpens {
    /// Routing applied every registered open: release the keys as routed.
    pub(crate) fn complete(mut self) {
        self.routed = true;
    }

    /// Routing ended without applying every open: release the keys as failed.
    pub(crate) fn fail(self) {}

    /// Settles by the pump's outcome (`true` = every open applied).
    pub(crate) fn settle(self, routed: bool) {
        if routed {
            self.complete();
        } else {
            self.fail();
        }
    }
}

impl PublicationGate {
    /// Registers every `MessageSubscriptionOpening` in `events` as pending.
    /// Returns `None` (nothing to hold) when there are none.
    pub(crate) fn register(&self, events: &[Event]) -> Option<PendingOpens> {
        let keys: Vec<OpenKey> = events
            .iter()
            .filter_map(|e| match e {
                Event::MessageSubscriptionOpening {
                    message_name,
                    correlation_key,
                    ..
                } => Some((message_name.clone(), correlation_key.clone())),
                _ => None,
            })
            .collect();
        if keys.is_empty() {
            return None;
        }
        let mut pending = self.lock();
        for key in &keys {
            pending
                .entry(key.clone())
                .or_insert_with(|| Pending {
                    count: 0,
                    released: Arc::new(Release::default()),
                })
                .count += 1;
        }
        drop(pending);
        Some(PendingOpens {
            gate: self.clone(),
            keys,
            routed: false,
        })
    }

    /// Whether an open for `(name, correlation_key)` is pending.
    #[cfg(test)]
    pub(crate) fn is_pending(&self, name: &str, correlation_key: &str) -> bool {
        self.lock()
            .contains_key(&(name.to_string(), correlation_key.to_string()))
    }

    /// Waits until no open for `(name, correlation_key)` is pending, at most
    /// `deadline`.
    ///
    /// The outcome is sticky over every notifier the waiter observed: if ANY
    /// registration it waited on ended failed — even one replaced by a fresh
    /// same-key registration before the waiter resumed — the wait reports
    /// [`GateWait::Failed`] (#1866).
    pub(crate) async fn wait(
        &self,
        name: &str,
        correlation_key: &str,
        deadline: Duration,
    ) -> GateWait {
        let until = tokio::time::Instant::now() + deadline;
        let key = (name.to_string(), correlation_key.to_string());
        // Every notifier the waiter has observed. The pending check and the
        // first capture happen under ONE lock (the first loop lookup), so an
        // owner failing in between can never make a waiter see "cleared"
        // without having captured the failed notifier.
        let mut observed: Vec<Arc<Release>> = Vec::new();
        let cleared = |observed: &[Arc<Release>]| {
            if observed.is_empty() {
                GateWait::Clear
            } else if observed.iter().any(|r| r.failed()) {
                GateWait::Failed
            } else {
                GateWait::Released
            }
        };
        loop {
            // Look up this key's notifier AND arm it while holding the lock.
            // Releases (and the re-registration of a cleared key, which
            // installs a fresh notifier) also take the lock, so the armed
            // notifier is always the one the current entry's final release
            // will notify.
            let released = {
                let guard = self.lock();
                match guard.get(&key) {
                    Some(p) => Arc::clone(&p.released),
                    None => return cleared(&observed),
                }
            };
            if !observed.iter().any(|r| Arc::ptr_eq(r, &released)) {
                observed.push(Arc::clone(&released));
            }
            let notified = released.notify.notified();
            tokio::pin!(notified);
            {
                let guard = self.lock();
                match guard.get(&key) {
                    // Arm under the lock, then re-validate identity: if the
                    // entry was released and re-registered meanwhile, the
                    // waiter must park on the NEW notifier, so restart.
                    Some(p) if Arc::ptr_eq(&p.released, &released) => {
                        notified.as_mut().enable();
                    }
                    Some(_) => continue,
                    None => return cleared(&observed),
                }
            }
            if tokio::time::timeout_at(until, notified).await.is_err() {
                // An open still pending at the deadline is a timeout even if
                // an overlapping registration of the key failed: the pending
                // open may yet apply, so the caller refuses retryably.
                let guard = self.lock();
                return match guard.get(&key) {
                    Some(_) => GateWait::TimedOut,
                    None => cleared(&observed),
                };
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<OpenKey, Pending>> {
        self.inner.pending.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// How long an undelivered open must have been pending before the background
/// re-drive retries it (Zeebe's `PendingMessageSubscriptionChecker` uses a
/// similar minimum age so an in-flight delivery is not duplicated).
pub(crate) const FAILED_OPEN_REDRIVE_AGE: Duration = Duration::from_secs(10);
/// How often the background re-drive scans for due undelivered opens.
pub(crate) const FAILED_OPEN_REDRIVE_INTERVAL: Duration = Duration::from_secs(30);

/// One undelivered subscription open/close awaiting background re-drive.
struct FailedOpen {
    since: tokio::time::Instant,
    event: Event,
    operation_reference: Option<i64>,
}

/// The subscription a keyed (`delivery_key != 0`) open/close addresses, and
/// whether it is a close. `None` for any other (or an unkeyed) event: an
/// unkeyed delivery has no recipient dedup, so re-driving it could apply it
/// twice.
fn routed_subscription(event: &Event) -> Option<(u64, bool)> {
    match event {
        Event::MessageSubscriptionOpening {
            subscription_key,
            delivery_key,
            ..
        } if *delivery_key != 0 => Some((*subscription_key, false)),
        Event::MessageSubscriptionClosing {
            subscription_key,
            delivery_key,
            ..
        } if *delivery_key != 0 => Some((*subscription_key, true)),
        _ => None,
    }
}

#[derive(Default)]
struct FailedOpensInner {
    /// The latest undelivered intent per subscription key: O(1) to record,
    /// replace or supersede (#1866).
    queue: HashMap<u64, FailedOpen>,
    /// Subscriptions whose open some routing pump currently owns (an
    /// original pump, a mid-cascade follow-up, or a re-drive), refcounted.
    in_flight: HashMap<u64, usize>,
    /// Owned opens a close superseded meanwhile: should such an open fail it
    /// is dropped, never queued (#1866).
    superseded: HashSet<u64>,
}

/// Node-wide queue of cross-partition subscription opens (and the closes that
/// supersede them) whose routing failed (#1866). Unbounded in attempts: an
/// entry leaves only when re-driven (a re-drive that fails again records it
/// afresh). It holds the **latest intent** per subscription: a close routed
/// (or queued) for a subscription drops its queued open, and marks any open
/// a pump still owns as superseded, so a cancelled instance's open is never
/// queued or re-driven into a live subscription — whether the close was
/// delivered (its recipient tombstone may later be evicted) or itself
/// exhausted during the same outage (then the close is re-driven instead).
/// Cheap to clone.
#[derive(Clone, Default)]
pub(crate) struct FailedOpens {
    inner: Arc<Mutex<FailedOpensInner>>,
}

impl FailedOpens {
    /// Records every keyed subscription open/close in `events` as undelivered
    /// now. Other events are ignored. Returns how many were recorded. Each
    /// event costs O(1).
    pub(crate) fn record(&self, events: &[Event], operation_reference: Option<i64>) -> usize {
        let now = tokio::time::Instant::now();
        let mut g = self.lock();
        let mut recorded = 0;
        for event in events {
            let Some((key, is_close)) = routed_subscription(event) else {
                continue;
            };
            if is_close {
                g.supersede(key);
            } else if g.superseded.contains(&key)
                || g.queue
                    .get(&key)
                    .is_some_and(|f| matches!(routed_subscription(&f.event), Some((_, true))))
            {
                // A close already superseded this open.
                continue;
            }
            g.queue.insert(
                key,
                FailedOpen {
                    since: now,
                    event: event.clone(),
                    operation_reference,
                },
            );
            recorded += 1;
        }
        recorded
    }

    /// Marks every keyed open in `events` as owned by a routing pump until
    /// [`Self::release`]: a close routed meanwhile supersedes it, so a later
    /// [`Self::record`] of it is dropped. Returns the claimed keys.
    pub(crate) fn claim(&self, events: &[Event]) -> Vec<u64> {
        let mut g = self.lock();
        let mut claimed = Vec::new();
        for event in events {
            if let Some((key, false)) = routed_subscription(event) {
                *g.in_flight.entry(key).or_default() += 1;
                claimed.push(key);
            }
        }
        claimed
    }

    /// Gives up claims taken by [`Self::claim`]; a key's supersession marker
    /// is dropped once no pump owns it any more.
    pub(crate) fn release(&self, keys: &[u64]) {
        let mut g = self.lock();
        for key in keys {
            if let Some(n) = g.in_flight.get_mut(key) {
                *n -= 1;
                if *n == 0 {
                    g.in_flight.remove(key);
                    g.superseded.remove(key);
                }
            }
        }
    }

    /// Notes that a close for `event`'s subscription is being routed: any
    /// queued open of it is dropped and an owned one will not be queued. A
    /// no-op for any event that is not a keyed close.
    pub(crate) fn note_routing(&self, event: &Event) {
        if let Some((key, true)) = routed_subscription(event) {
            self.lock().supersede(key);
        }
    }

    /// Removes and returns every entry pending at least
    /// [`FAILED_OPEN_REDRIVE_AGE`] (oldest first), claiming each open until
    /// [`Self::finish`].
    pub(crate) fn take_due(&self) -> Vec<(Event, Option<i64>)> {
        let now = tokio::time::Instant::now();
        let mut g = self.lock();
        let keys: Vec<u64> = g
            .queue
            .iter()
            .filter(|(_, f)| now.saturating_duration_since(f.since) >= FAILED_OPEN_REDRIVE_AGE)
            .map(|(k, _)| *k)
            .collect();
        let mut due: Vec<FailedOpen> = keys.iter().filter_map(|k| g.queue.remove(k)).collect();
        due.sort_by_key(|f| f.since);
        for f in &due {
            if let Some((key, false)) = routed_subscription(&f.event) {
                *g.in_flight.entry(key).or_default() += 1;
            }
        }
        due.into_iter()
            .map(|f| (f.event, f.operation_reference))
            .collect()
    }

    /// Ends the claim of a re-driven entry taken by [`Self::take_due`].
    pub(crate) fn finish(&self, event: &Event) {
        if let Some((key, false)) = routed_subscription(event) {
            self.release(&[key]);
        }
    }

    /// Number of entries awaiting re-drive.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().queue.len()
    }

    /// Whether no entry awaits re-drive.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.lock().queue.is_empty()
    }

    /// The queued events, oldest first.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> Vec<Event> {
        let g = self.lock();
        let mut v: Vec<&FailedOpen> = g.queue.values().collect();
        v.sort_by_key(|f| f.since);
        v.into_iter().map(|f| f.event.clone()).collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FailedOpensInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl FailedOpensInner {
    /// A close for subscription `key` supersedes its open: drop a queued
    /// open, and keep an owned one from being queued.
    fn supersede(&mut self, key: u64) {
        if self
            .queue
            .get(&key)
            .is_some_and(|f| matches!(routed_subscription(&f.event), Some((_, false))))
        {
            self.queue.remove(&key);
        }
        if self.in_flight.contains_key(&key) {
            self.superseded.insert(key);
        }
    }
}

/// The ack-paced buffered deliveries (#1949) a node's message partitions
/// still await acks for, carried across background re-drive passes. The
/// in-flight set itself is durable engine state
/// (`Engine::pending_buffered_deliveries`), so it survives exhausted routing
/// retries, a restart and a leader change; this only paces the re-drive: a
/// delivery is re-sent once it was ALREADY in flight on the previous pass
/// (at least one [`FAILED_OPEN_REDRIVE_INTERVAL`] old), so a delivery the
/// original pump is still routing is not duplicated. A re-send is harmless
/// anyway — it carries the original delivery key, the instance partition
/// answers a duplicate with a re-ack, and a duplicate ack is a no-op.
#[derive(Clone, Default)]
pub(crate) struct StalledDeliveries {
    previous: Arc<Mutex<HashSet<(u64, u64)>>>,
}

impl StalledDeliveries {
    /// Of the deliveries in flight now, the ones also in flight on the
    /// previous pass (by `(subscription_key, delivery_key)`); remembers `now`
    /// for the next pass. A pending resume handshake (#1949,
    /// `Engine::pending_resume_handshakes`) is paced the same way, keyed by
    /// `(subscription_key, 0)` (a real delivery key is never `0`). Any other
    /// event is ignored.
    pub(crate) fn due(&self, now: Vec<Event>) -> Vec<Event> {
        let identity = |event: &Event| match event {
            Event::RemoteMessageCorrelation {
                subscription_key,
                delivery_key,
                ack_partition: Some(_),
                ..
            } => Some((*subscription_key, *delivery_key)),
            Event::MessageSubscriptionResuming {
                subscription_key, ..
            } => Some((*subscription_key, 0)),
            _ => None,
        };
        let mut previous = self.previous.lock().unwrap_or_else(|e| e.into_inner());
        let current: HashSet<(u64, u64)> = now.iter().filter_map(identity).collect();
        let due = now
            .into_iter()
            .filter(|e| identity(e).is_some_and(|id| previous.contains(&id)))
            .collect();
        *previous = current;
        due
    }
}

impl Drop for PendingOpens {
    fn drop(&mut self) {
        let mut pending = self.gate.lock();
        for key in &self.keys {
            if let Some(p) = pending.get_mut(key) {
                if !self.routed {
                    p.released
                        .failed
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                p.count -= 1;
                if p.count == 0 {
                    // Only the key that became clear wakes its own waiters.
                    if let Some(p) = pending.remove(key) {
                        p.released.notify.notify_waiters();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1949: a pending resume handshake is re-driven like a stalled
    /// delivery — only once it was already pending on the previous pass.
    #[test]
    fn stalled_resume_handshake_is_due_on_the_second_pass() {
        let resuming = Event::MessageSubscriptionResuming {
            subscription_key: 7,
            instance_key: 8,
            correlation_key: "k".into(),
        };
        let stalled = StalledDeliveries::default();
        assert!(stalled.due(vec![resuming.clone()]).is_empty());
        assert_eq!(stalled.due(vec![resuming.clone()]), vec![resuming]);
        assert!(stalled.due(Vec::new()).is_empty());
    }

    fn opening(name: &str, key: &str) -> Event {
        Event::MessageSubscriptionOpening {
            subscription_key: 1,
            instance_key: 2,
            element_instance_key: 3,
            element_id: "catch".into(),
            message_name: name.into(),
            correlation_key: key.into(),
            kind: nanobpmn_engine_core::MessageSubscriptionKind::IntermediateCatch,
            delivery_key: 4,
        }
    }

    fn closing(name: &str, key: &str) -> Event {
        Event::MessageSubscriptionClosing {
            subscription_key: 1,
            instance_key: 2,
            element_instance_key: 3,
            element_id: "catch".into(),
            message_name: name.into(),
            correlation_key: key.into(),
            delivery_key: 5,
        }
    }

    /// #1866: a close routed for a subscription (delivered, so only its
    /// bounded recipient tombstone protects) drops the queued open, so the
    /// re-drive can never reopen it after the tombstone is evicted.
    #[tokio::test]
    async fn a_routed_close_drops_the_queued_open() {
        let q = FailedOpens::default();
        assert_eq!(q.record(&[opening("m", "k")], None), 1);
        q.note_routing(&closing("m", "k"));
        assert!(q.is_empty(), "the open is superseded by the close");
    }

    /// #1866: a close that also exhausted during the outage replaces the
    /// queued open (the close is re-driven, never the open), and a later
    /// re-record of the open cannot resurrect it.
    #[tokio::test]
    async fn an_exhausted_close_replaces_the_queued_open() {
        let q = FailedOpens::default();
        q.record(&[opening("m", "k")], None);
        assert_eq!(q.record(&[closing("m", "k")], None), 1);
        assert_eq!(q.queued(), vec![closing("m", "k")]);
        assert_eq!(q.record(&[opening("m", "k")], None), 0);
        assert_eq!(q.queued(), vec![closing("m", "k")]);
    }

    /// #1866: an open taken for re-drive that a close supersedes while in
    /// flight is not re-queued when that re-drive fails again.
    #[tokio::test]
    async fn an_in_flight_open_superseded_by_a_close_is_not_requeued() {
        tokio::time::pause();
        let q = FailedOpens::default();
        q.record(&[opening("m", "k")], None);
        tokio::time::advance(FAILED_OPEN_REDRIVE_AGE).await;
        let due = q.take_due();
        assert_eq!(due.len(), 1);
        q.note_routing(&closing("m", "k"));
        assert_eq!(q.record(&[opening("m", "k")], None), 0, "superseded");
        q.finish(&opening("m", "k"));
        assert!(q.is_empty());
        // After the window closes a fresh open of the key is queued again.
        assert_eq!(q.record(&[opening("m", "k")], None), 1);
    }

    /// #1866: a close delivered while the ORIGINAL pump (not a re-drive)
    /// still owns the open supersedes it, so the supervisor recording the
    /// open's failure afterwards drops it instead of queuing a reopen.
    #[tokio::test]
    async fn an_original_pump_open_superseded_by_a_close_is_not_queued() {
        let q = FailedOpens::default();
        let claimed = q.claim(&[opening("m", "k")]);
        q.note_routing(&closing("m", "k"));
        assert_eq!(q.record(&[opening("m", "k")], None), 0, "superseded");
        q.release(&claimed);
        assert!(q.is_empty());
    }

    /// #1866: overlapping owners of one open keep the supersession marker
    /// until the last releases.
    #[tokio::test]
    async fn supersession_outlives_all_but_the_last_owner() {
        let q = FailedOpens::default();
        let a = q.claim(&[opening("m", "k")]);
        let b = q.claim(&[opening("m", "k")]);
        q.note_routing(&closing("m", "k"));
        q.release(&a);
        assert_eq!(q.record(&[opening("m", "k")], None), 0, "still owned");
        q.release(&b);
        assert_eq!(q.record(&[opening("m", "k")], None), 1);
    }

    /// #1866: the queue keeps one latest intent per subscription, so
    /// re-recording replaces rather than accumulates.
    #[tokio::test]
    async fn re_recording_replaces_the_entry() {
        let q = FailedOpens::default();
        for _ in 0..3 {
            q.record(&[opening("m", "k")], None);
        }
        assert_eq!(q.len(), 1);
    }

    /// A fails and wakes the parked waiter, but B re-registers the same key
    /// before the waiter resumes; B completing must not mask A's failure.
    #[tokio::test]
    async fn failure_survives_a_same_key_reregistration_before_resume() {
        let gate = PublicationGate::default();
        let a = gate.register(&[opening("m", "k")]).expect("a");
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        a.fail();
        let b = gate.register(&[opening("m", "k")]).expect("b");
        tokio::task::yield_now().await;
        b.complete();
        assert_eq!(waiter.await.expect("join"), GateWait::Failed);
    }

    /// Same, but the waiter's deadline elapses while B is still pending: B may
    /// yet apply, so the wait is a timeout (the caller refuses retryably).
    #[tokio::test]
    async fn still_pending_after_a_replaced_failure_times_out() {
        let gate = PublicationGate::default();
        let a = gate.register(&[opening("m", "k")]).expect("a");
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_millis(50)).await })
        };
        tokio::task::yield_now().await;
        a.fail();
        let _b = gate.register(&[opening("m", "k")]).expect("b");
        assert_eq!(waiter.await.expect("join"), GateWait::TimedOut);
    }

    /// #1866: only keyed opens/closes are recorded, and an entry becomes due only once
    /// it has been pending [`FAILED_OPEN_REDRIVE_AGE`] (fake time).
    #[tokio::test(start_paused = true)]
    async fn failed_opens_become_due_after_the_minimum_age() {
        let q = FailedOpens::default();
        let other = Event::MessageSubscriptionClosing {
            subscription_key: 1,
            instance_key: 2,
            element_instance_key: 3,
            element_id: "catch".into(),
            message_name: "m".into(),
            correlation_key: "k".into(),
            // Unkeyed: no recipient dedup, so never re-driven.
            delivery_key: 0,
        };
        let mut unkeyed = opening("m", "k");
        if let Event::MessageSubscriptionOpening { delivery_key, .. } = &mut unkeyed {
            *delivery_key = 0;
        }
        assert_eq!(q.record(&[opening("m", "k"), other, unkeyed], Some(7)), 1);
        assert!(q.take_due().is_empty(), "too young to re-drive");
        tokio::time::advance(FAILED_OPEN_REDRIVE_AGE - Duration::from_millis(1)).await;
        assert!(q.take_due().is_empty());
        tokio::time::advance(Duration::from_millis(1)).await;
        let due = q.take_due();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, Some(7));
        assert_eq!(q.len(), 0, "a taken entry leaves the queue");
    }

    #[tokio::test]
    async fn no_opening_registers_nothing_and_never_waits() {
        let gate = PublicationGate::default();
        assert!(gate.register(&[]).is_none());
        assert_eq!(gate.wait("m", "k", Duration::ZERO).await, GateWait::Clear);
    }

    #[tokio::test]
    async fn publish_waits_until_the_open_is_released() {
        let gate = PublicationGate::default();
        let held = gate.register(&[opening("m", "k")]).expect("registered");
        // An unrelated key / name is never gated.
        assert_eq!(
            gate.wait("m", "other", Duration::ZERO).await,
            GateWait::Clear
        );
        assert_eq!(gate.wait("n", "k", Duration::ZERO).await, GateWait::Clear);
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "publish must wait for the pending open"
        );
        held.complete();
        assert_eq!(waiter.await.expect("join"), GateWait::Released);
        assert!(!gate.is_pending("m", "k"));
    }

    #[tokio::test]
    async fn a_never_released_open_is_bounded_by_the_deadline() {
        let gate = PublicationGate::default();
        let _held = gate.register(&[opening("m", "k")]).expect("registered");
        assert_eq!(
            gate.wait("m", "k", Duration::from_millis(20)).await,
            GateWait::TimedOut
        );
    }

    #[tokio::test]
    async fn overlapping_registrations_release_only_when_all_drop() {
        let gate = PublicationGate::default();
        let a = gate.register(&[opening("m", "k")]).expect("a");
        let b = gate.register(&[opening("m", "k")]).expect("b");
        drop(a);
        assert!(gate.is_pending("m", "k"));
        drop(b);
        assert!(!gate.is_pending("m", "k"));
    }

    /// A release of an unrelated key, or a partial release of the waited key,
    /// must not wake the waiter: only its own key reaching zero does.
    #[tokio::test]
    async fn waiters_wake_only_when_their_own_key_clears() {
        let gate = PublicationGate::default();
        let a = gate.register(&[opening("m", "k")]).expect("a");
        let b = gate.register(&[opening("m", "k")]).expect("b");
        let other = gate.register(&[opening("m", "x")]).expect("other");
        let released = gate
            .lock()
            .get(&("m".to_string(), "k".to_string()))
            .map(|p| Arc::clone(&p.released))
            .expect("pending");
        let notified = released.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        drop(other);
        drop(a);
        assert!(
            futures_util::FutureExt::now_or_never(notified.as_mut()).is_none(),
            "unrelated or partial releases must not notify"
        );
        drop(b);
        assert!(futures_util::FutureExt::now_or_never(notified.as_mut()).is_some());
    }
    /// A release followed by a same-key re-registration installs a fresh
    /// notifier; a waiter must park on the current one, so releasing the new
    /// registration wakes it long before the deadline.
    #[tokio::test]
    async fn same_key_reregistration_wakes_on_the_current_notifier() {
        let gate = PublicationGate::default();
        let first = gate.register(&[opening("m", "k")]).expect("first");
        let stale = gate
            .lock()
            .get(&("m".to_string(), "k".to_string()))
            .map(|p| Arc::clone(&p.released))
            .expect("pending");
        drop(first);
        let second = gate.register(&[opening("m", "k")]).expect("second");
        let current = gate
            .lock()
            .get(&("m".to_string(), "k".to_string()))
            .map(|p| Arc::clone(&p.released))
            .expect("pending");
        assert!(!Arc::ptr_eq(&stale, &current));
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        second.complete();
        let out = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("woken by the current notifier, not the deadline");
        assert_eq!(out.expect("join"), GateWait::Released);
    }

    /// #1866: a pump that ends without routing its open releases the waiter
    /// as `Failed`, never `Released` — the publish must be refused, not
    /// applied against a subscription that was never opened.
    #[tokio::test]
    async fn a_failed_pump_releases_waiters_as_failed() {
        let gate = PublicationGate::default();
        let held = gate.register(&[opening("m", "k")]).expect("registered");
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        held.fail();
        assert_eq!(waiter.await.expect("join"), GateWait::Failed);
        assert!(
            !gate.is_pending("m", "k"),
            "a failure never wedges the gate"
        );
    }

    /// A bare drop (panicked / cancelled owner) still releases — the gate
    /// never wedges — but as failed, since routing was never confirmed.
    #[tokio::test]
    async fn a_dropped_guard_releases_as_failed() {
        let gate = PublicationGate::default();
        let held = gate.register(&[opening("m", "k")]).expect("registered");
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        drop(held);
        assert_eq!(waiter.await.expect("join"), GateWait::Failed);
    }

    /// Deadline path: one overlapping registration failed while another is
    /// still pending at the deadline — the pending open wins: a timeout.
    #[tokio::test]
    async fn a_deadline_on_a_still_pending_failed_key_times_out() {
        let gate = PublicationGate::default();
        let failed = gate.register(&[opening("m", "k")]).expect("failed");
        let _stuck = gate.register(&[opening("m", "k")]).expect("stuck");
        failed.fail();
        assert_eq!(
            gate.wait("m", "k", Duration::from_millis(20)).await,
            GateWait::TimedOut
        );
    }

    /// A failure on one registration taints the key until it clears, even if
    /// a later overlapping registration routed successfully.
    #[tokio::test]
    async fn overlapping_failure_survives_a_later_success() {
        let gate = PublicationGate::default();
        let a = gate.register(&[opening("m", "k")]).expect("a");
        let b = gate.register(&[opening("m", "k")]).expect("b");
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_secs(30)).await })
        };
        tokio::task::yield_now().await;
        a.fail();
        b.complete();
        assert_eq!(waiter.await.expect("join"), GateWait::Failed);
    }

    /// #1845: every outcome round-trips through its wire spelling, and an
    /// unknown spelling is rejected rather than guessed.
    #[test]
    fn gate_wait_wire_spelling_round_trips() {
        for w in [
            GateWait::Clear,
            GateWait::Released,
            GateWait::Failed,
            GateWait::TimedOut,
        ] {
            assert_eq!(GateWait::from_wire(w.as_wire()), Some(w));
        }
        assert_eq!(GateWait::from_wire("bogus"), None);
    }

    /// #1845: a cluster-wide wait fails closed — any gate still pending makes
    /// the whole publish time out, a failed open beats a routed one, and only
    /// all-clear gates are clear. Order-independent.
    #[test]
    fn gate_wait_combine_prefers_the_most_severe_outcome() {
        use GateWait::*;
        let all = [Clear, Released, Failed, TimedOut];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                assert_eq!(a.combine(*b), all[i.max(j)]);
            }
        }
    }
}
