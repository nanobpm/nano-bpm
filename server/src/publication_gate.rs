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
//! [`GateWait::Failed`] (a retryable refusal) instead of `Released`.
//!
//! The registry is node-local: it orders only publishes that apply on the node
//! whose create owner routes the opens. Creates answer early only on a single
//! non-Raft node. With peers or under Raft they keep route-before-answer (see
//! `ServerImpl::answer_and_route_create`).

use std::collections::HashMap;
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
    /// failed, so the subscription may not exist. The caller rejects the
    /// publish as retryable rather than discarding it as unmatched.
    Failed,
    /// The deadline elapsed with an open still pending; the caller rejects the
    /// publish as retryable rather than applying it against a missing
    /// subscription.
    TimedOut,
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
        let any_failed = |observed: &[Arc<Release>]| observed.iter().any(|r| r.failed());
        let cleared = |observed: &[Arc<Release>]| {
            if observed.is_empty() {
                GateWait::Clear
            } else if any_failed(observed) {
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
                // A failure already observed (on this or a replaced notifier)
                // reports the failure, not just the timeout.
                let guard = self.lock();
                return match guard.get(&key) {
                    Some(p) if p.released.failed() || any_failed(&observed) => GateWait::Failed,
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

    /// Same, but the waiter's deadline elapses while B is still pending.
    #[tokio::test]
    async fn replaced_failure_is_reported_on_the_deadline_path() {
        let gate = PublicationGate::default();
        let a = gate.register(&[opening("m", "k")]).expect("a");
        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait("m", "k", Duration::from_millis(50)).await })
        };
        tokio::task::yield_now().await;
        a.fail();
        let _b = gate.register(&[opening("m", "k")]).expect("b");
        assert_eq!(waiter.await.expect("join"), GateWait::Failed);
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
    /// still pending at the deadline — the waiter reports the failure.
    #[tokio::test]
    async fn a_deadline_on_a_failed_key_reports_failed() {
        let gate = PublicationGate::default();
        let failed = gate.register(&[opening("m", "k")]).expect("failed");
        let _stuck = gate.register(&[opening("m", "k")]).expect("stuck");
        failed.fail();
        assert_eq!(
            gate.wait("m", "k", Duration::from_millis(20)).await,
            GateWait::Failed
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
}
