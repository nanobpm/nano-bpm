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
//! The registry is node-local: it orders only publishes that apply on the node
//! whose create owner routes the opens. Creates answer early only on a single
//! node. With peers they keep route-before-answer, because a publish can apply
//! on a node whose gate never saw the open (see
//! `ServerImpl::answer_and_route_create`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nanobpmn_engine_core::Event;

/// `(message name, correlation key)` of a pending subscription open.
type OpenKey = (String, String);

#[derive(Default)]
struct Inner {
    pending: Mutex<HashMap<OpenKey, usize>>,
    released: tokio::sync::Notify,
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
    /// The deadline elapsed with an open still pending; the publish proceeds.
    TimedOut,
}

/// Registration of a create's in-flight subscription opens; releases them on
/// drop (routing finished, failed, panicked or was cancelled).
pub(crate) struct PendingOpens {
    gate: PublicationGate,
    keys: Vec<OpenKey>,
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
            *pending.entry(key.clone()).or_insert(0) += 1;
        }
        drop(pending);
        Some(PendingOpens {
            gate: self.clone(),
            keys,
        })
    }

    /// Whether an open for `(name, correlation_key)` is pending.
    pub(crate) fn is_pending(&self, name: &str, correlation_key: &str) -> bool {
        self.lock()
            .contains_key(&(name.to_string(), correlation_key.to_string()))
    }

    /// Waits until no open for `(name, correlation_key)` is pending, at most
    /// `deadline`.
    pub(crate) async fn wait(
        &self,
        name: &str,
        correlation_key: &str,
        deadline: Duration,
    ) -> GateWait {
        if !self.is_pending(name, correlation_key) {
            return GateWait::Clear;
        }
        let until = tokio::time::Instant::now() + deadline;
        loop {
            // Arm the notification BEFORE re-checking, so a release between
            // the check and the await is not missed.
            let notified = self.inner.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.is_pending(name, correlation_key) {
                return GateWait::Released;
            }
            if tokio::time::timeout_at(until, notified).await.is_err() {
                return if self.is_pending(name, correlation_key) {
                    GateWait::TimedOut
                } else {
                    GateWait::Released
                };
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<OpenKey, usize>> {
        self.inner.pending.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Drop for PendingOpens {
    fn drop(&mut self) {
        let mut pending = self.gate.lock();
        for key in &self.keys {
            if let Some(count) = pending.get_mut(key) {
                *count -= 1;
                if *count == 0 {
                    pending.remove(key);
                }
            }
        }
        drop(pending);
        self.gate.inner.released.notify_waiters();
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
        drop(held);
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
}
