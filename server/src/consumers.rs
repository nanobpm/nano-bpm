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
    /// Number of `activateJobs` calls for this `(job_type, worker)` currently in
    /// flight (recorded, not yet returned). A worker may overlap polls (a slow
    /// long poll plus a fresh one), and the shared entry must keep reading live
    /// until the *last* of them returns — an older poll completing must not
    /// truncate a newer, still-open window.
    in_flight: u32,
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
/// Each call marks one more poll **in flight** (`in_flight += 1`) so a worker
/// running overlapping `activateJobs` calls keeps reading live until the last of
/// them returns (see [`complete_rest_poll`]). The shared window covers the
/// *longest* outstanding poll: `live_until_ms` is the max of the prior window and
/// this call's deadline, so an overlapping shorter/non-blocking poll never
/// shrinks an already-open longer one (which would read the worker idle while the
/// longer request is still in flight).
///
/// New keys are refused once [`rest_max`] distinct consumers are retained (after
/// first pruning dead entries) so a caller pumping unique `(type, worker)` pairs
/// cannot grow this map without bound; upserts to existing consumers always
/// proceed, so live workers are never dropped by the cap.
///
/// Returns `true` when the registration was **admitted** (recorded) and so must
/// later be balanced by [`complete_rest_poll`], `false` when a new key was
/// refused at the cardinality cap (nothing recorded — completing it would
/// decrement a *different*, genuinely-admitted poll's shared counter).
pub fn record_rest_poll(job_type: &str, worker: &str, long_poll_ms: u64) -> bool {
    let now = now_ms();
    let key = (job_type.to_string(), worker.to_string());
    let mut polls = REST_POLLS.lock().expect("consumer rest-polls poisoned");
    let prior = polls.get(&key);
    let in_flight = prior.map(|p| p.in_flight).unwrap_or(0);
    // Overlapping polls share one entry keyed by `(job_type, worker)`, so the
    // window must cover the *longest* outstanding poll, not just the newest
    // call's. Taking `max` with the prior `live_until_ms` keeps an overlapping
    // shorter/non-blocking poll (`long_poll_ms` smaller, even 0) from shrinking
    // an already-open longer window — which would read the worker idle once that
    // shorter deadline + grace passed even though the longer request is still in
    // flight (a false zero-worker / starvation signal). `complete_rest_poll`
    // still clamps the window down to the completion instant once the *last*
    // in-flight poll returns, so a drained-and-gone worker is not held live.
    let live_until_ms = prior
        .map(|p| p.live_until_ms)
        .unwrap_or(0)
        .max(now.saturating_add(long_poll_ms));
    let entry = RestPoll {
        last_seen_ms: now,
        live_until_ms,
        in_flight: in_flight.saturating_add(1),
    };
    upsert_rest_poll(&mut polls, key, entry, now, rest_evict_ms(), rest_max())
}

/// Marks one of `worker`'s in-flight `activateJobs` for `job_type` as **returned**,
/// closing its long-poll window now rather than at the requested timeout.
///
/// [`record_rest_poll`] sets `live_until_ms` from the *requested* long-poll window
/// before activation is attempted, but the request may return long before that
/// window elapses — jobs were available and it answered immediately, or it gave up
/// early. Without this call the consumer keeps reading **live** until
/// `timeout + stale` even though it has already drained and gone, so a one-shot
/// client with a long `requestTimeout` would suppress the per-type `Starved`
/// signal for a job type nothing is actually draining. Closing the window at
/// completion (the [`REST_STALE_MS`] grace still applies) makes the worker count
/// reflect consumers that are genuinely still polling.
///
/// Two details keep the close from publishing a *false* zero-worker signal:
///
/// * **Overlapping polls.** The entry is keyed by `(job_type, worker)`, so a worker
///   with two `activateJobs` in flight shares one entry. This only closes the window
///   when the *last* in-flight poll returns (`in_flight` reaches 0); an older poll
///   completing while a newer one is still open leaves the newer window intact.
/// * **Long polls.** The window is clamped to the *completion instant* (`now`),
///   never back to `last_seen_ms` (the poll *start*). A poll that ran longer than
///   the stale grace would otherwise close to a start timestamp already past the
///   grace and read idle the instant it returned — denying the worker its
///   post-return grace and letting a normal re-poll gap look like starvation.
///
/// Best-effort: a no-op when the `(job_type, worker)` key is absent (e.g. the poll
/// was never recorded because `max_jobs_to_activate <= 0`).
pub fn complete_rest_poll(job_type: &str, worker: &str) {
    let now = now_ms();
    let mut polls = REST_POLLS.lock().expect("consumer rest-polls poisoned");
    if let Some(p) = polls.get_mut(&(job_type.to_string(), worker.to_string())) {
        p.in_flight = p.in_flight.saturating_sub(1);
        if p.in_flight == 0 {
            // Close the window at the completion instant (never before the most
            // recent poll start): the worker is live through the post-close grace,
            // then idles/evicts on the normal schedule.
            p.live_until_ms = now.max(p.last_seen_ms);
        }
    }
}

/// RAII guard that balances one [`record_rest_poll`] registration, making the
/// in-flight count **cancellation-safe**.
///
/// The `activateJobs` handler awaits a long-poll window
/// (`tokio::time::timeout(wait, notified).await`). If Axum drops the handler future
/// while it is suspended at that await — the client disconnected, or the server is
/// shutting down — no explicit [`complete_rest_poll`] call runs, so `in_flight`
/// would stay elevated forever. Later polls for the same `(job_type, worker)`
/// inherit the leaked count, their own completions never reach zero, the requested
/// deadline stays in place, and a phantom worker suppresses the per-type `Starved`
/// signal. Holding this guard across the await closes the window on **every** exit
/// path — normal return *and* future cancellation — because `Drop` runs either way.
///
/// Construct with [`RestPollGuard::record`]. The guard owns an owned copy of the
/// `(job_type, worker)` key so it can release the registration without borrowing
/// the request.
#[derive(Debug)]
pub struct RestPollGuard {
    job_type: String,
    worker: String,
}

impl RestPollGuard {
    /// Records one in-flight poll (see [`record_rest_poll`]) and returns the guard
    /// that releases it on drop.
    ///
    /// Returns `None` — **no guard** — when the registration was refused at the
    /// cardinality cap (a new `(job_type, worker)` key while the map is at
    /// [`rest_max`]). Nothing was recorded in that case, so there is nothing to
    /// complete: arming a guard anyway would make its `Drop` call
    /// [`complete_rest_poll`] and decrement a *different*, genuinely-admitted
    /// in-flight poll's shared counter for the same key — a false zero-worker
    /// signal. `None` carries no `Drop`, so a refused poll is a clean no-op.
    pub fn record(job_type: &str, worker: &str, long_poll_ms: u64) -> Option<Self> {
        record_rest_poll(job_type, worker, long_poll_ms).then(|| Self {
            job_type: job_type.to_string(),
            worker: worker.to_string(),
        })
    }
}

impl Drop for RestPollGuard {
    fn drop(&mut self) {
        complete_rest_poll(&self.job_type, &self.worker);
    }
}

/// Insert or refresh a REST consumer, enforcing the cardinality cap. Existing
/// keys always update (a live worker is never dropped); a new key is admitted
/// only if the map is under `max` after pruning dead entries — otherwise it is
/// refused so best-effort observability can't become an OOM vector. Pure over
/// its inputs so the cap logic is unit-testable without the process-global map
/// or env overrides.
///
/// Returns `true` when the entry was inserted/updated (the registration is
/// **admitted** and must later be completed), `false` when a new key was refused
/// at the cap (nothing was recorded, so there is nothing to complete). Callers
/// that balance the registration with [`complete_rest_poll`] must arm that
/// completion only on `true` — completing a refused poll would decrement a
/// *different*, genuinely-admitted in-flight poll's shared counter.
fn upsert_rest_poll(
    polls: &mut HashMap<(String, String), RestPoll>,
    key: (String, String),
    entry: RestPoll,
    now: u64,
    evict: u64,
    max: usize,
) -> bool {
    if !polls.contains_key(&key) && polls.len() >= max {
        polls.retain(|_, p| rest_retained(p, now, evict));
        if polls.len() >= max {
            return false;
        }
    }
    polls.insert(key, entry);
    true
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
    /// path) so a test can pin its `last_seen`/`live_until` timestamps. The poll
    /// is seeded already returned (`in_flight: 0`) — these fixtures describe a
    /// consumer whose window is whatever `live_until_ms` says, not one mid-poll.
    fn insert_poll(jt: &str, worker: &str, last_seen_ms: u64, live_until_ms: u64) {
        REST_POLLS.lock().unwrap().insert(
            (jt.to_string(), worker.to_string()),
            RestPoll {
                last_seen_ms,
                live_until_ms,
                in_flight: 0,
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
    fn completed_rest_poll_stops_counting_once_its_grace_elapses() {
        // Advisory: a one-shot client with a long `requestTimeout` that returns
        // immediately (jobs were available) must not keep the worker counted live
        // until timeout + grace — that would suppress `Starved` for a type nothing
        // is actually draining. `complete_rest_poll` closes the window at the
        // completion instant, so once the post-close grace passes the worker drops
        // out of the provisioning count.
        let jt = "t-done:review";
        clear("t-done:");
        // Record a poll with a long window still open (as `activateJobs` does on
        // entry), then complete it (as the early-return paths now do).
        record_rest_poll(jt, "agent-oneshot", 60_000);
        complete_rest_poll(jt, "agent-oneshot");
        // The window is now closed at the completion instant, so the entry is live
        // only through the stale grace — not for the full 60s window. Backdating
        // the close past the grace (simulating "grace has now elapsed") drops it.
        let aged = now_ms().saturating_sub(rest_stale_ms() + 1_000);
        insert_poll(jt, "agent-oneshot", aged, aged);
        let counts = rest_workers_per_type();
        assert_eq!(
            counts.get(jt).copied(),
            None,
            "a completed poll past its grace no longer counts as a live worker"
        );
        clear("t-done:");
    }

    #[test]
    fn complete_rest_poll_closes_an_open_window() {
        // Directly guard the window-close semantics: an open long-poll window
        // (live_until in the future) is pulled back to the completion instant, so
        // the consumer stops being live once the grace elapses rather than riding
        // the full requested timeout.
        let jt = "t-close:review";
        clear("t-close:");
        let now = now_ms();
        // Seed an in-flight poll (in_flight: 1) whose window is still open, as
        // `record_rest_poll` leaves it on entry.
        REST_POLLS.lock().unwrap().insert(
            (jt.to_string(), "agent-lp".to_string()),
            RestPoll {
                last_seen_ms: now,
                live_until_ms: now + 60_000,
                in_flight: 1,
            },
        );
        complete_rest_poll(jt, "agent-lp");
        let p = *REST_POLLS
            .lock()
            .unwrap()
            .get(&(jt.to_string(), "agent-lp".to_string()))
            .expect("poll present");
        assert_eq!(p.in_flight, 0, "the in-flight poll is marked returned");
        assert!(
            p.live_until_ms >= p.last_seen_ms,
            "completion closes the long-poll window at the completion instant"
        );
        assert!(
            p.live_until_ms <= now_ms(),
            "the window closes now, not at the requested timeout"
        );
        clear("t-close:");
    }

    #[test]
    fn complete_of_an_older_poll_keeps_a_newer_overlapping_poll_live() {
        // Overlap regression (Copilot review): two `activateJobs` for the same
        // `(job_type, worker)` share one entry. If the older poll returns *after*
        // the newer one starts, closing the shared window must not truncate the
        // newer poll's still-open window — that would read a genuinely-polling
        // worker as gone and publish a false zero-worker (starvation) signal.
        let jt = "t-overlap:review";
        clear("t-overlap:");
        // Poll 1 starts a long poll (in_flight 1, window open 60s).
        record_rest_poll(jt, "agent-ol", 60_000);
        // Poll 2 starts while poll 1 is still in flight (in_flight 2).
        record_rest_poll(jt, "agent-ol", 60_000);
        // Poll 1 (the older) returns first.
        complete_rest_poll(jt, "agent-ol");
        let p = *REST_POLLS
            .lock()
            .unwrap()
            .get(&(jt.to_string(), "agent-ol".to_string()))
            .expect("poll present");
        assert_eq!(p.in_flight, 1, "the newer poll is still in flight");
        assert!(
            p.live_until_ms > now_ms(),
            "the newer poll's open window survives the older poll's completion"
        );
        assert!(
            rest_live(&p, now_ms(), rest_stale_ms()),
            "still live while the overlapping poll is open"
        );
        // When the newer poll also returns, the window closes.
        complete_rest_poll(jt, "agent-ol");
        let p = *REST_POLLS
            .lock()
            .unwrap()
            .get(&(jt.to_string(), "agent-ol".to_string()))
            .expect("poll present");
        assert_eq!(p.in_flight, 0);
        assert!(
            p.live_until_ms <= now_ms(),
            "the window closes once the last in-flight poll returns"
        );
        clear("t-overlap:");
    }

    #[test]
    fn overlapping_short_poll_does_not_shrink_a_long_polls_window() {
        // Deadline-replacement regression (Copilot review): a worker with a 60s
        // `activateJobs` in flight that then issues an overlapping non-blocking /
        // short poll shares one entry. Recording the short poll must not replace
        // the open 60s window with the shorter deadline — otherwise, once the
        // shorter deadline + grace passes, the worker reads idle even though the
        // 60s request is still open (a false zero-worker / starvation signal).
        // The window must cover the *longest* outstanding poll.
        let jt = "t-maxwin:review";
        clear("t-maxwin:");
        // Poll 1 starts a 60s long poll (in_flight 1, window open 60s).
        record_rest_poll(jt, "agent-mw", 60_000);
        let key = (jt.to_string(), "agent-mw".to_string());
        let long_window = REST_POLLS
            .lock()
            .unwrap()
            .get(&key)
            .expect("poll")
            .live_until_ms;
        // Poll 2 (non-blocking, long_poll_ms = 0) starts while poll 1 is open.
        record_rest_poll(jt, "agent-mw", 0);
        let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(p.in_flight, 2, "both polls are in flight");
        assert_eq!(
            p.live_until_ms, long_window,
            "the overlapping non-blocking poll must not shrink the open 60s window"
        );
        // Even after the short poll's own (zero) window + grace would have passed,
        // the worker is still live on the strength of the outstanding 60s poll.
        assert!(
            rest_live(&p, now_ms(), rest_stale_ms()),
            "live while the longer overlapping poll is still open"
        );
        // The short poll returns; the 60s poll is still in flight, so the window
        // is untouched (completion only clamps once the last poll returns).
        complete_rest_poll(jt, "agent-mw");
        let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(p.in_flight, 1, "the 60s poll is still in flight");
        assert_eq!(
            p.live_until_ms, long_window,
            "the surviving 60s window is preserved across the short poll's return"
        );
        assert!(rest_live(&p, now_ms(), rest_stale_ms()));
        clear("t-maxwin:");
    }

    #[test]
    fn an_unrecorded_zero_capacity_poll_must_not_complete_a_real_poll() {
        // Zero-capacity regression (Copilot review): a `max_jobs_to_activate <= 0`
        // request is *not* recorded (it can drain nothing, so it is not a worker),
        // but the handler's completion path used to call `complete_rest_poll`
        // unconditionally. Because the entry is keyed by `(job_type, worker)`, that
        // unmatched completion found a *different*, genuinely-recorded in-flight
        // poll and decremented its shared counter — closing a real worker's live
        // window and publishing a false starvation signal. The handler now only
        // completes a window it actually opened (`recorded`). This pins the defect
        // mechanism at the primitive level: an unmatched `complete_rest_poll` (what
        // the un-guarded handler issued for a zero-capacity request) disturbs a
        // real poll, so the `recorded` guard is load-bearing, and a matched
        // record+complete pair is balanced.
        let jt = "t-zerocap:review";
        clear("t-zerocap:");
        let key = (jt.to_string(), "agent-zc".to_string());

        // A real worker opens a 60s poll (in_flight 1, window open 60s).
        record_rest_poll(jt, "agent-zc", 60_000);
        let open = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(open.in_flight, 1);
        assert!(rest_live(&open, now_ms(), rest_stale_ms()));

        // Defect mechanism: an *unmatched* complete (the old zero-capacity path)
        // decrements the real poll's counter and closes its open window.
        complete_rest_poll(jt, "agent-zc");
        let closed = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(closed.in_flight, 0, "the unmatched complete stole the poll");
        assert!(
            closed.live_until_ms < open.live_until_ms,
            "the open 60s window was closed early — the false-starvation defect"
        );

        // The guarded handler never issues that unmatched complete. A balanced
        // record + complete pair (a real poll that returns) stays correct.
        clear("t-zerocap:");
        record_rest_poll(jt, "agent-zc", 60_000);
        complete_rest_poll(jt, "agent-zc");
        let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(p.in_flight, 0, "the matched pair balances");
        clear("t-zerocap:");
    }

    #[test]
    fn a_dropped_poll_guard_balances_the_in_flight_count() {
        // Cancellation regression (Copilot review): the `activateJobs` handler
        // awaits a long-poll window, and if Axum drops that future mid-await
        // (client disconnect / shutdown) no explicit `complete_rest_poll` runs.
        // Without a guard the registration leaks — `in_flight` stays elevated, the
        // requested deadline stays in place, and a phantom worker suppresses the
        // per-type `Starved` signal. `RestPollGuard`'s `Drop` must close the window
        // on every exit path, so a dropped (cancelled) poll balances exactly like a
        // returned one.
        let jt = "t-cancel:review";
        clear("t-cancel:");
        let key = (jt.to_string(), "agent-cx".to_string());

        // Open a 60s long poll via the guard, then drop it (the cancellation path:
        // the handler future is dropped at the await, running only `Drop`).
        let open_window = {
            let _guard = RestPollGuard::record(jt, "agent-cx", 60_000);
            let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
            assert_eq!(p.in_flight, 1, "the poll is in flight while held");
            assert!(
                p.live_until_ms > now_ms(),
                "the requested 60s window is open while the poll is in flight"
            );
            p.live_until_ms
            // `_guard` drops here — the cancellation path.
        };

        // After the drop the registration is balanced: the count returns to zero
        // and the open window is closed to the drop instant (not left at the
        // requested deadline), so no phantom worker lingers.
        let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(
            p.in_flight, 0,
            "a dropped guard balances the in-flight count (no leak)"
        );
        assert!(
            p.live_until_ms < open_window,
            "the leaked 60s window is closed at the drop, not left open"
        );
        assert!(
            p.live_until_ms <= now_ms(),
            "the window closes now, not at the requested timeout"
        );
        clear("t-cancel:");
    }

    #[test]
    fn an_aborted_long_poll_does_not_leak_into_a_later_short_poll() {
        // Abort-then-short-poll regression (Copilot review): a long poll that is
        // cancelled mid-await must not leave its registration behind for a later
        // poll of the same `(job_type, worker)` to inherit. If it leaked, the later
        // short poll's completion would only bring the count down to the leaked
        // level (never zero), so its window would never close and the phantom
        // worker would keep suppressing `Starved`.
        let jt = "t-abort:review";
        clear("t-abort:");
        let key = (jt.to_string(), "agent-ab".to_string());

        // A long poll is cancelled mid-await (its guard drops without a return).
        {
            let _guard = RestPollGuard::record(jt, "agent-ab", 60_000);
        }
        let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(p.in_flight, 0, "the aborted poll left nothing in flight");

        // A later short poll for the same worker starts from a clean slate: it is
        // the only poll in flight, and its own completion closes the window fully.
        {
            let _guard = RestPollGuard::record(jt, "agent-ab", 0);
            let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
            assert_eq!(
                p.in_flight, 1,
                "the short poll is the only one in flight — no leaked count inherited"
            );
        }
        let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert_eq!(
            p.in_flight, 0,
            "the short poll's completion reaches zero (the leaked count is gone)"
        );
        assert!(
            p.live_until_ms <= now_ms(),
            "the short poll's window closes on completion (no phantom worker)"
        );
        clear("t-abort:");
    }

    #[test]
    fn complete_after_a_long_poll_still_grants_the_post_return_grace() {
        // Long-poll regression (Copilot review): a poll that runs longer than the
        // stale grace must still get its post-return grace. Closing the window to
        // the poll *start* (`last_seen_ms`, already > stale ago) would read idle
        // the instant it returns; closing to the completion instant (`now`) keeps
        // it live for the grace so a normal re-poll gap is not false starvation.
        let jt = "t-longgrace:review";
        clear("t-longgrace:");
        // Record a poll, then simulate it staying in flight past the stale window
        // by backdating its start (the window is still open: live_until is future).
        record_rest_poll(jt, "agent-lg", 60_000);
        let key = (jt.to_string(), "agent-lg".to_string());
        let stale = rest_stale_ms();
        {
            let mut polls = REST_POLLS.lock().unwrap();
            let p = polls.get_mut(&key).expect("poll present");
            p.last_seen_ms = now_ms().saturating_sub(stale + 5_000);
        }
        // The long poll now returns. Completion must clamp to now, not the start.
        complete_rest_poll(jt, "agent-lg");
        let p = *REST_POLLS.lock().unwrap().get(&key).expect("poll present");
        assert!(
            rest_live(&p, now_ms(), stale),
            "a long poll that just returned is live through the post-return grace"
        );
        let counts = rest_workers_per_type();
        assert_eq!(
            counts.get(jt).copied(),
            Some(1),
            "a just-returned long poll still counts as a live worker (no false starve)"
        );
        clear("t-longgrace:");
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
            in_flight: 0,
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
            in_flight: 0,
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
                in_flight: 0,
            },
        );
        let dead = now.saturating_sub(evict + 1_000);
        polls.insert(
            ("dead".into(), "w".into()),
            RestPoll {
                last_seen_ms: dead,
                live_until_ms: dead,
                in_flight: 0,
            },
        );
        // At the cap a new key first prunes the dead entry, then is admitted.
        let fresh = RestPoll {
            last_seen_ms: now,
            live_until_ms: now,
            in_flight: 0,
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
    fn upsert_rest_poll_reports_admission_so_a_refused_poll_arms_no_guard() {
        // Rejected-poll regression (Copilot review): `record_rest_poll` can
        // silently refuse a new key at the cap, but `RestPollGuard::record` used to
        // return an *armed* guard regardless — whose `Drop` calls
        // `complete_rest_poll`. If capacity later freed and a fresh request for the
        // same key was admitted before the rejected one finished, the rejected
        // request's `Drop` would decrement/close the *newer* registration — a
        // false zero-worker signal. The upsert must report admission (`true` =
        // recorded, arm a guard; `false` = refused, arm nothing) so the guard is
        // only ever armed for a registration that actually exists.
        let now = now_ms();
        let evict = rest_evict_ms();
        let live = RestPoll {
            last_seen_ms: now,
            live_until_ms: now,
            in_flight: 1,
        };
        let mut polls: HashMap<(String, String), RestPoll> = HashMap::new();
        assert!(
            upsert_rest_poll(&mut polls, ("a".into(), "w".into()), live, now, evict, 2),
            "first key admitted"
        );
        assert!(
            upsert_rest_poll(&mut polls, ("b".into(), "w".into()), live, now, evict, 2),
            "second key admitted (fills the cap)"
        );
        // A third distinct key with nothing dead to reclaim is refused — and the
        // caller is told, so it arms no completion guard for it.
        assert!(
            !upsert_rest_poll(&mut polls, ("c".into(), "w".into()), live, now, evict, 2),
            "refused key reports not-admitted so no guard is armed"
        );
        assert!(!polls.contains_key(&("c".to_string(), "w".to_string())));
        // An existing key still updates (admitted) even at the cap.
        assert!(
            upsert_rest_poll(&mut polls, ("a".into(), "w".into()), live, now, evict, 2),
            "an existing key is admitted (refreshed) despite the cap"
        );
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
