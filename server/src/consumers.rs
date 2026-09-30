//! "Who is polling what" consumer registry — the engine-side truth behind the
//! console's live consumers panel (issue #404).
//!
//! The console's Workers view lists console-*authored* worker directories
//! (`createWorker` + Monaco). It cannot see a **hired agent** that connects over
//! the SDK and polls a job type, because that agent never touches the console —
//! it talks to the engine. This module records those live consumers so the
//! console can show, e.g., "your agent is polling `convergence-loop:review-round`".
//!
//! Consumers arrive over two transports with fundamentally different liveness
//! models, so the panel is split by transport:
//!
//! * **Falcon** (the `/falcon` command-stream): a persistent WebSocket. The
//!   [`crate::falcon::Registry`] already tracks each connection's `last_seen_ms`
//!   (updated on every frame, including heartbeats) and a reaper evicts a
//!   connection silent past [`crate::falcon::falcon_liveness_timeout_ms`]. We
//!   reuse that same deadline so the panel's notion of "stale" matches the
//!   engine's own — no new timers.
//! * **REST** (`activateJobs` long-poll): stateless — there is no connection to
//!   observe. A healthy worker simply re-issues `activateJobs` in a loop, so we
//!   record the wall-clock of each poll per `(jobType, worker)` and infer
//!   liveness from its age against the windows below.
//!
//! This is best-effort *observability only*. Recording a poll is a single map
//! insert off the activation result path; it never affects job-activation
//! semantics.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::falcon::{self, Registry};

/// Grace window (millis) after a REST poll's long-poll window closes, during
/// which the consumer is still shown **live**. A healthy worker re-issues
/// `activateJobs` immediately when the previous long poll returns, so a small
/// grace past the effective window (see [`RestPoll::live_until_ms`]) absorbs the
/// re-issue gap before the row greys to "idle". 2× the default 5s long-poll
/// window ([`crate::DEFAULT_REQUEST_TIMEOUT_MS`]) tolerates one fully missed
/// cycle. Overridable via `NANOBPMN_CONSUMER_REST_STALE_MS`.
const REST_STALE_MS: u64 = 10_000;

/// A REST consumer idle this long past its live window is dropped from the panel
/// entirely (rather than merely greyed at [`REST_STALE_MS`]) — the worker is
/// gone, not idle. Chosen well above the stale window so a briefly-paused worker
/// flickers to "idle" but does not vanish and re-appear. Overridable via
/// `NANOBPMN_CONSUMER_REST_EVICT_MS`.
const REST_EVICT_MS: u64 = 60_000;

/// Hard cap on distinct REST `(jobType, worker)` consumers retained. `activateJobs`
/// is unauthenticated in the surface this observes and the key is caller-supplied
/// (`type` + `worker`), so an adversary could otherwise pump unbounded unique keys
/// and OOM the process through a best-effort observability map. At the cap we
/// prune dead entries and, failing that, refuse to grow (updates to existing
/// consumers always proceed). Overridable via `NANOBPMN_CONSUMER_REST_MAX`.
const MAX_REST_CONSUMERS: usize = 10_000;

/// A single REST consumer's liveness state. `activateJobs` may long-poll for a
/// caller-chosen window (`requestTimeout`), so a worker mid-poll is legitimately
/// silent for that whole window — we record when the poll's window closes
/// (`live_until_ms`) and treat the consumer as live until then (plus the
/// [`REST_STALE_MS`] grace), independent of the fixed stale window. Without this,
/// a 60s long poll would show "idle" 10s in while the request is still in flight.
#[derive(Debug, Clone, Copy)]
struct RestPoll {
    /// Wall-clock (epoch millis) of the most recent `activateJobs` call.
    last_seen_ms: u64,
    /// Wall-clock (epoch millis) at which this poll's long-poll window closes —
    /// i.e. the worker is expected to re-issue by. Live until this + grace.
    live_until_ms: u64,
}

/// Per-`(jobType, worker)` REST consumer state. A process-global best-effort map
/// fed by [`record_rest_poll`] on every `activateJobs`; pruned on read in
/// [`snapshot`] and, under cap pressure, on write.
static REST_POLLS: LazyLock<Mutex<HashMap<(String, String), RestPoll>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Wall-clock millis since the Unix epoch. Liveness only — never journaled.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// REST liveness window (millis), overridable via `NANOBPMN_CONSUMER_REST_STALE_MS`.
fn rest_stale_ms() -> u64 {
    env_u64("NANOBPMN_CONSUMER_REST_STALE_MS").unwrap_or(REST_STALE_MS)
}

/// REST eviction window (millis), overridable via `NANOBPMN_CONSUMER_REST_EVICT_MS`.
fn rest_evict_ms() -> u64 {
    env_u64("NANOBPMN_CONSUMER_REST_EVICT_MS").unwrap_or(REST_EVICT_MS)
}

/// Cap on distinct retained REST consumers, overridable via `NANOBPMN_CONSUMER_REST_MAX`.
fn rest_max() -> usize {
    std::env::var("NANOBPMN_CONSUMER_REST_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(MAX_REST_CONSUMERS)
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
}

/// Whether a REST consumer counts as **live** at `now`: from the poll's start
/// through its long-poll window close (`live_until_ms`) plus a `stale` grace for
/// the worker to re-issue. Measuring the grace from the window close (not the
/// poll start) keeps a long poll live for the full grace *after* it returns,
/// rather than greying the instant a >`stale` window closes.
fn rest_live(p: &RestPoll, now: u64, stale: u64) -> bool {
    now < p.live_until_ms.saturating_add(stale)
}

/// Whether a poll should still be retained (live or merely idle) at `now`: kept
/// until `evict` past its long-poll window close. Anchored on `live_until_ms`
/// (not `last_seen_ms`) for the same reason as [`rest_live`] — a worker that
/// just finished a long poll longer than `evict` still gets the eviction grace
/// to re-issue before its row vanishes.
fn rest_retained(p: &RestPoll, now: u64, evict: u64) -> bool {
    now < p.live_until_ms.saturating_add(evict)
}

/// Records that `worker` polled `job_type` over REST (an `activateJobs` call)
/// whose long-poll window is `long_poll_ms` wide (0 for a non-blocking poll).
/// Best-effort: a single map upsert, called off the activation-result path.
///
/// New keys are refused once [`rest_max`] distinct consumers are retained (after
/// first pruning dead entries) so a caller pumping unique `(type, worker)` pairs
/// cannot grow this map without bound; upserts to existing consumers always
/// proceed, so live workers are never dropped by the cap.
pub fn record_rest_poll(job_type: &str, worker: &str, long_poll_ms: u64) {
    let now = now_ms();
    let key = (job_type.to_string(), worker.to_string());
    let entry = RestPoll {
        last_seen_ms: now,
        live_until_ms: now.saturating_add(long_poll_ms),
    };
    let mut polls = REST_POLLS.lock().expect("consumer rest-polls poisoned");
    upsert_rest_poll(&mut polls, key, entry, now, rest_evict_ms(), rest_max());
}

/// Insert or refresh a REST consumer, enforcing the cardinality cap. Existing
/// keys always update (a live worker is never dropped); a new key is admitted
/// only if the map is under `max` after pruning dead entries — otherwise it is
/// refused so best-effort observability can't become an OOM vector. Pure over
/// its inputs so the cap logic is unit-testable without the process-global map
/// or env overrides.
fn upsert_rest_poll(
    polls: &mut HashMap<(String, String), RestPoll>,
    key: (String, String),
    entry: RestPoll,
    now: u64,
    evict: u64,
    max: usize,
) {
    if !polls.contains_key(&key) && polls.len() >= max {
        polls.retain(|_, p| rest_retained(p, now, evict));
        if polls.len() >= max {
            return;
        }
    }
    polls.insert(key, entry);
}

/// One live job consumer surfaced to the console panel.
///
/// Only the console `/console/api/consumers` route (and tests) constructs or
/// serializes this; the always-built REST tracker above never does, so on a
/// non-console build it is legitimately dead — allowed, not gated, so the
/// payload definition cannot drift from the always-built tracker.
#[cfg_attr(not(feature = "console"), allow(dead_code))]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Consumer {
    /// The job type being polled, e.g. `convergence-loop:review-round`.
    pub job_type: String,
    /// The worker / lease-owner name the consumer registered under.
    pub worker: String,
    /// Transport the consumer arrived on: `"rest"` or `"falcon"`.
    pub transport: &'static str,
    /// Wall-clock (epoch millis) of the consumer's most recent activity.
    pub last_seen_ms: u64,
    /// Age of `last_seen_ms` relative to the snapshot's `now_ms`.
    pub age_ms: u64,
    /// `"live"` while within the transport's liveness window, else `"idle"`.
    pub status: &'static str,
}

/// The consumers panel payload: the live consumer rows plus the windows used to
/// compute their status, so the console can label the thresholds it is showing.
/// Console-route-only on a non-console build — see [`Consumer`].
#[cfg_attr(not(feature = "console"), allow(dead_code))]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumersResponse {
    /// Snapshot wall-clock (epoch millis) — the reference for every `age_ms`.
    pub now_ms: u64,
    /// REST liveness window in effect (millis).
    pub rest_stale_ms: u64,
    /// Falcon liveness deadline in effect (millis) — the engine's reaper timeout.
    pub falcon_liveness_ms: u64,
    /// Live consumers, sorted by job type, then worker, then transport.
    pub consumers: Vec<Consumer>,
}

/// Live subscribed-worker count per job type contributed by the **REST** transport
/// (`activateJobs` long-pollers), the transport `falcon::Registry::workers_per_type`
/// cannot see.
///
/// Why this exists (issue #1294): the worker-provisioning monitor flags a job type
/// as *starved* when jobs are waiting but the subscribed-worker count is zero. That
/// count came only from the Falcon registry (persistent WebSocket subscribers), so a
/// fleet that serves a job type purely over REST long-polling — which holds no
/// subscription the registry can enumerate — read as **zero workers** and was
/// falsely reported as starved even while actively draining. Folding the live REST
/// consumers into the count closes that false-positive.
///
/// A worker is counted while it is **live** (its long-poll window is open, or it
/// re-polled within the stale grace) — the same liveness notion the consumers panel
/// shows — so a worker that has actually stopped polling stops counting and a
/// genuinely unserved type can still surface as starved. Distinct `worker` names are
/// counted per job type (a single worker polling twice counts once), mirroring the
/// Falcon roster width. Pure over `REST_POLLS`; a cheap map fold off the hot path.
pub fn rest_workers_per_type() -> HashMap<String, usize> {
    let now = now_ms();
    let stale = rest_stale_ms();
    let polls = REST_POLLS.lock().expect("consumer rest-polls poisoned");
    let mut by_type: HashMap<String, std::collections::HashSet<&str>> = HashMap::new();
    for ((job_type, worker), p) in polls.iter() {
        if rest_live(p, now, stale) {
            by_type
                .entry(job_type.clone())
                .or_default()
                .insert(worker.as_str());
        }
    }
    by_type
        .into_iter()
        .map(|(jt, workers)| (jt, workers.len()))
        .collect()
}

/// Builds the consumers snapshot: pruned REST polls + live Falcon subscriptions,
/// each tagged with a transport-appropriate live/idle status. Prunes evicted
/// REST entries as a side effect (read is the natural sweep point). Only the
/// console route calls this — see [`Consumer`].
#[cfg_attr(not(feature = "console"), allow(dead_code))]
pub fn snapshot(registry: &Registry) -> ConsumersResponse {
    let now = now_ms();
    let stale = rest_stale_ms();
    let evict = rest_evict_ms();
    let falcon_liveness = falcon::falcon_liveness_timeout_ms();
    let mut consumers = Vec::new();

    // REST: drop consumers gone past the eviction window, then emit the rest.
    // A consumer is live while its long-poll window is open (the request is
    // legitimately in flight) or it re-polled within the stale grace.
    {
        let mut polls = REST_POLLS.lock().expect("consumer rest-polls poisoned");
        polls.retain(|_, p| rest_retained(p, now, evict));
        for ((job_type, worker), p) in polls.iter() {
            let age = now.saturating_sub(p.last_seen_ms);
            consumers.push(Consumer {
                job_type: job_type.clone(),
                worker: worker.clone(),
                transport: "rest",
                last_seen_ms: p.last_seen_ms,
                age_ms: age,
                status: if rest_live(p, now, stale) {
                    "live"
                } else {
                    "idle"
                },
            });
        }
    }

    // Falcon: one row per (connection, job type) subscription. The reaper evicts
    // connections past `falcon_liveness`, so these are effectively always live;
    // the status is still computed for the brief window before a reap.
    for c in registry.consumers() {
        let age = now.saturating_sub(c.last_seen_ms);
        consumers.push(Consumer {
            job_type: c.job_type,
            worker: c.worker,
            transport: "falcon",
            last_seen_ms: c.last_seen_ms,
            age_ms: age,
            status: if age < falcon_liveness {
                "live"
            } else {
                "idle"
            },
        });
    }

    consumers.sort_by(|a, b| {
        a.job_type
            .cmp(&b.job_type)
            .then_with(|| a.worker.cmp(&b.worker))
            .then_with(|| a.transport.cmp(b.transport))
    });

    ConsumersResponse {
        now_ms: now,
        rest_stale_ms: stale,
        falcon_liveness_ms: falcon_liveness,
        consumers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each test owns a private key space (unique job type) and clears it after,
    /// so the process-global `REST_POLLS` can't leak between parallel tests.
    fn clear(job_type_prefix: &str) {
        REST_POLLS
            .lock()
            .unwrap()
            .retain(|(jt, _), _| !jt.starts_with(job_type_prefix));
    }

    fn rest_rows(resp: &ConsumersResponse, jt_prefix: &str) -> Vec<Consumer> {
        resp.consumers
            .iter()
            .filter(|c| c.transport == "rest" && c.job_type.starts_with(jt_prefix))
            .cloned()
            .collect()
    }

    /// Seed a REST consumer directly (bypassing the cap/`now_ms` of the public
    /// path) so a test can pin its `last_seen`/`live_until` timestamps.
    fn insert_poll(jt: &str, worker: &str, last_seen_ms: u64, live_until_ms: u64) {
        REST_POLLS.lock().unwrap().insert(
            (jt.to_string(), worker.to_string()),
            RestPoll {
                last_seen_ms,
                live_until_ms,
            },
        );
    }

    #[test]
    fn grace_applies_after_a_long_poll_closes_not_from_its_start() {
        // A long poll (window ≫ stale) that closed within the grace stays live;
        // the same poll closed past the grace greys to idle but is retained. This
        // guards that the grace is measured from window close, not poll start.
        let jt = "t-grace:review";
        clear("t-grace:");
        let now = now_ms();
        let stale = rest_stale_ms();
        // Closed 1s ago after a 30s poll ⇒ within grace ⇒ live.
        insert_poll(
            jt,
            "agent-g",
            now.saturating_sub(31_000),
            now.saturating_sub(1_000),
        );
        assert_eq!(
            rest_rows(&snapshot(&falcon::Registry::new()), "t-grace:")[0].status,
            "live",
            "just-closed long poll is live during the post-close grace"
        );
        // Closed past the grace ⇒ idle, still listed.
        insert_poll(
            jt,
            "agent-g",
            now.saturating_sub(31_000),
            now.saturating_sub(stale + 1_000),
        );
        let rows = rest_rows(&snapshot(&falcon::Registry::new()), "t-grace:");
        assert_eq!(rows.len(), 1, "still retained");
        assert_eq!(rows[0].status, "idle", "past the post-close grace ⇒ idle");
        clear("t-grace:");
    }

    #[test]
    fn records_a_rest_poll_as_live() {
        let jt = "t-live:review";
        clear("t-live:");
        record_rest_poll(jt, "agent-a", 0);
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        let rows = rest_rows(&resp, "t-live:");
        assert_eq!(rows.len(), 1, "one REST consumer recorded");
        assert_eq!(rows[0].worker, "agent-a");
        assert_eq!(rows[0].transport, "rest");
        assert_eq!(rows[0].status, "live");
        clear("t-live:");
    }

    #[test]
    fn stale_rest_poll_greys_to_idle_but_is_retained() {
        let jt = "t-stale:review";
        clear("t-stale:");
        let stale = rest_stale_ms();
        // Backdate the poll past the stale window but within the evict window,
        // with its long-poll window already closed.
        let aged = now_ms().saturating_sub(stale + 1_000);
        insert_poll(jt, "agent-b", aged, aged);
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        let rows = rest_rows(&resp, "t-stale:");
        assert_eq!(rows.len(), 1, "stale consumer still listed");
        assert_eq!(rows[0].status, "idle", "past stale window ⇒ idle");
        clear("t-stale:");
    }

    #[test]
    fn open_long_poll_stays_live_past_the_stale_window() {
        let jt = "t-longpoll:review";
        clear("t-longpoll:");
        let now = now_ms();
        // Last poll is older than the stale grace, but its long-poll window is
        // still open (e.g. a 60s `requestTimeout` in flight): the worker is
        // legitimately silent, so it must read live, not idle.
        let aged = now.saturating_sub(rest_stale_ms() + 5_000);
        insert_poll(jt, "agent-lp", aged, now + 30_000);
        let rows = rest_rows(&snapshot(&falcon::Registry::new()), "t-longpoll:");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].status, "live",
            "an in-flight long poll is live despite a stale-age last_seen"
        );
        clear("t-longpoll:");
    }

    #[test]
    fn evicted_rest_poll_disappears() {
        let jt = "t-evict:review";
        clear("t-evict:");
        let evict = rest_evict_ms();
        let gone = now_ms().saturating_sub(evict + 1_000);
        insert_poll(jt, "agent-c", gone, gone);
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        let rows = rest_rows(&resp, "t-evict:");
        assert!(rows.is_empty(), "consumer past evict window is dropped");
        // And the prune actually removed it from the backing map.
        assert!(
            !REST_POLLS
                .lock()
                .unwrap()
                .contains_key(&(jt.to_string(), "agent-c".to_string())),
            "evicted key pruned from REST_POLLS"
        );
    }

    #[test]
    fn re_polling_refreshes_last_seen() {
        let jt = "t-refresh:review";
        clear("t-refresh:");
        let aged = now_ms().saturating_sub(30_000);
        insert_poll(jt, "agent-d", aged, aged);
        // A fresh poll should move it back to live.
        record_rest_poll(jt, "agent-d", 0);
        let reg = falcon::Registry::new();
        let rows = rest_rows(&snapshot(&reg), "t-refresh:");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "live", "re-poll refreshes to live");
        clear("t-refresh:");
    }

    #[test]
    fn rest_workers_per_type_counts_live_distinct_pollers() {
        // The open-question fix (issue #1294): a job type served purely over REST
        // long-polling must contribute to the worker count so it is not falsely
        // read as starved. Two distinct live workers ⇒ 2; a repeat name counts
        // once; a stale (idle) poller does not count.
        let jt = "t-rw:review";
        clear("t-rw:");
        let now = now_ms();
        // Two distinct live workers (open long-poll windows).
        insert_poll(jt, "agent-a", now, now + 30_000);
        insert_poll(jt, "agent-b", now, now + 30_000);
        // Same name as agent-a re-polling ⇒ must not double-count.
        insert_poll(jt, "agent-a", now, now + 30_000);
        // A poller gone past the stale window with a closed window ⇒ not live.
        let aged = now.saturating_sub(rest_stale_ms() + 1_000);
        insert_poll(jt, "agent-idle", aged, aged);

        let counts = rest_workers_per_type();
        assert_eq!(
            counts.get(jt).copied(),
            Some(2),
            "two distinct live REST workers, deduped, idle excluded"
        );
        clear("t-rw:");
    }

    #[test]
    fn cap_refuses_new_consumers_but_still_updates_existing() {
        // Operates on a local map so the process-global cap/env is untouched and
        // the test can't race sibling tests.
        let now = now_ms();
        let evict = rest_evict_ms();
        let live = RestPoll {
            last_seen_ms: now,
            live_until_ms: now,
        };
        let mut polls: HashMap<(String, String), RestPoll> = HashMap::new();
        upsert_rest_poll(&mut polls, ("a".into(), "w".into()), live, now, evict, 2);
        upsert_rest_poll(&mut polls, ("b".into(), "w".into()), live, now, evict, 2);
        assert_eq!(polls.len(), 2, "filled to the cap");
        // A third distinct key with nothing dead to reclaim is refused.
        upsert_rest_poll(&mut polls, ("c".into(), "w".into()), live, now, evict, 2);
        assert_eq!(polls.len(), 2, "cap refuses a new consumer when full");
        assert!(!polls.contains_key(&("c".to_string(), "w".to_string())));
        // An existing key still updates even at the cap (live workers never drop).
        let refreshed = RestPoll {
            last_seen_ms: now + 1,
            live_until_ms: now + 1,
        };
        upsert_rest_poll(
            &mut polls,
            ("a".into(), "w".into()),
            refreshed,
            now,
            evict,
            2,
        );
        assert_eq!(polls.len(), 2);
        assert_eq!(
            polls[&("a".to_string(), "w".to_string())].last_seen_ms,
            now + 1,
            "existing consumer refreshed despite the cap"
        );
    }

    #[test]
    fn cap_reclaims_dead_entries_before_refusing() {
        let now = now_ms();
        let evict = rest_evict_ms();
        let mut polls: HashMap<(String, String), RestPoll> = HashMap::new();
        polls.insert(
            ("live".into(), "w".into()),
            RestPoll {
                last_seen_ms: now,
                live_until_ms: now,
            },
        );
        let dead = now.saturating_sub(evict + 1_000);
        polls.insert(
            ("dead".into(), "w".into()),
            RestPoll {
                last_seen_ms: dead,
                live_until_ms: dead,
            },
        );
        // At the cap a new key first prunes the dead entry, then is admitted.
        let fresh = RestPoll {
            last_seen_ms: now,
            live_until_ms: now,
        };
        upsert_rest_poll(&mut polls, ("new".into(), "w".into()), fresh, now, evict, 2);
        assert!(
            polls.contains_key(&("new".to_string(), "w".to_string())),
            "new consumer admitted after reclaiming a dead one"
        );
        assert!(
            !polls.contains_key(&("dead".to_string(), "w".to_string())),
            "dead entry reclaimed to make room"
        );
        assert_eq!(polls.len(), 2);
    }

    #[test]
    fn response_reports_the_active_windows() {
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        assert_eq!(resp.rest_stale_ms, rest_stale_ms());
        assert_eq!(
            resp.falcon_liveness_ms,
            falcon::falcon_liveness_timeout_ms()
        );
        assert!(resp.now_ms > 0);
    }
}
