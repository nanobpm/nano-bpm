//! Per-job-type worker-provisioning advice for the console fleet-sizing panel
//! (issue #1294).
//!
//! The monitor tick already publishes the raw provisioning gauges
//! (`nanobpm_job_type_activatable` / `_workers` / `_dispatched_total`, plus the
//! server-saturation signals) about once a second. Those are the single source of
//! truth. This module reads them back — via the very same `/metrics` exposition the
//! [`crate::metrics::gather`] renders — and runs the **shared** worker-scaling
//! advisor ([`nano_provisioning_advisor`]) over two consecutive scrapes to classify
//! each job type (starved / under-provisioned / server-bound / adequate) and size
//! the suggested worker delta by Little's Law.
//!
//! The classification thresholds are therefore defined exactly once, in the shared
//! advisor crate the ProcessOS cockpit also consumes — never reimplemented in the
//! console's TypeScript. The console `/console/api/provisioning` endpoint just
//! serves the [`latest`] advice as JSON.
//!
//! State is process-global (a previous-scrape [`Snapshot`] and the latest
//! [`Advice`]) to mirror the metrics module it reads from: the gauges are
//! process-global, so the advice derived from them is too. Best-effort
//! observability — recording a tick is a single scrape + parse off the ~1 Hz
//! monitor path, and it never affects engine behaviour.

use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use nano_provisioning_advisor::{Advice, JobTypeSample, Snapshot, advise};

/// The previous scrape, held so the next tick has an earlier sample to diff
/// against (the backlog-slope / drain-rate window). Empty until the first tick,
/// which yields `Warming` classifications — the advisor's honest "not enough
/// history yet" state — rather than a false call.
static PREV: LazyLock<Mutex<Snapshot>> = LazyLock::new(|| Mutex::new(Snapshot::default()));

/// The most recently computed advice, served verbatim by the console endpoint.
/// Seeded with the empty-vs-empty advice (no job types, `Warming`) so the endpoint
/// has something coherent to return before the first monitor tick lands.
static LATEST: LazyLock<Mutex<Advice>> =
    LazyLock::new(|| Mutex::new(advise(&Snapshot::default(), &Snapshot::default())));

/// Wall-clock millis since the Unix epoch. Advisory rate window only — never
/// journaled.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Pure core: diff a fresh scrape against `prev` to produce the advice, returning
/// both the scrape (to store as the next tick's `prev`) and the advice. No global
/// state, so it is unit-testable and parallel-safe off any snapshot and clock.
fn compute(prev: &Snapshot, cur: Snapshot) -> (Snapshot, Advice) {
    let advice = advise(prev, &cur);
    (cur, advice)
}

/// Reads the current provisioning signals straight off the metric handles (no
/// `/metrics` serialize + re-parse) and builds the advisor's [`Snapshot`]. Reading
/// only the series the advisor consumes keeps the ~1 Hz tick cost proportional to
/// the provisioning signals rather than the whole registry — see
/// [`nano_server_storage::metrics::provisioning_signals`].
fn current_snapshot(now: u64) -> Snapshot {
    let sig = nano_server_storage::metrics::provisioning_signals();
    Snapshot::from_signals(
        now,
        sig.per_type
            .into_iter()
            .map(|(jt, s)| {
                (
                    jt,
                    JobTypeSample {
                        activatable: s.activatable,
                        workers: s.workers,
                        dispatched_total: s.dispatched_total,
                    },
                )
            })
            .collect(),
        sig.ceiling_throughput,
        sig.writer_busy_seconds,
        sig.writer_idle_seconds,
        sig.pending_create_queue,
        sig.admission_shed_total,
    )
}

/// Recompute the advice from the current provisioning signals and the stored
/// previous scrape, then rotate. Called ~1 Hz from the monitor tick.
pub fn tick() {
    let now = now_ms();
    let cur = current_snapshot(now);
    let mut prev = PREV.lock().expect("provisioning prev poisoned");
    let (cur, advice) = compute(&prev, cur);
    *prev = cur;
    *LATEST.lock().expect("provisioning latest poisoned") = advice;
}

/// The latest per-job-type provisioning advice for the console endpoint. Clones
/// the small stored [`Advice`] so the lock is held only for the copy.
pub fn latest() -> Advice {
    LATEST.lock().expect("provisioning latest poisoned").clone()
}

#[cfg(test)]
mod tests {
    use nano_provisioning_advisor::{Class, Confidence, Recommendation, parse_snapshot};

    use super::*;

    /// A minimal `/metrics` exposition for one job type plus the global
    /// server-saturation signals the advisor reads.
    fn expo(activatable: i64, workers: i64, dispatched: u64, jt: &str) -> String {
        format!(
            "nanobpm_job_type_activatable{{job_type=\"{jt}\"}} {activatable}\n\
             nanobpm_job_type_workers{{job_type=\"{jt}\"}} {workers}\n\
             nanobpm_job_type_dispatched_total{{job_type=\"{jt}\"}} {dispatched}\n\
             nanobpm_journal_writer_busy_seconds 0\n\
             nanobpm_journal_writer_idle_seconds 1\n"
        )
    }

    /// Diff the advice between two exposition-text scrapes, parsing each into a
    /// [`Snapshot`] first. The production tick builds the snapshot from the narrow
    /// metric read instead; parsing text here keeps the advisor's verdict exercised
    /// end-to-end from the `/metrics` contract the snapshot mirrors.
    fn compute_text(prev: &Snapshot, text: &str, now: u64) -> (Snapshot, Advice) {
        compute(prev, parse_snapshot(text, now))
    }

    fn find<'a>(a: &'a Advice, jt: &str) -> &'a Recommendation {
        a.recommendations
            .iter()
            .find(|r| r.job_type == jt)
            .expect("recommendation present")
    }

    /// Two scrapes feed the advisor a rate window: jobs waiting with zero workers
    /// classify as `Starved` (high confidence). Guards that the console endpoint
    /// surfaces the shared advisor's verdict end-to-end from raw exposition text.
    #[test]
    fn two_scrapes_produce_starved_advice() {
        let jt = "prov-test-starved:t";
        let (prev, _) = compute_text(&Snapshot::default(), &expo(30, 0, 0, jt), 0);
        let (_, a) = compute_text(&prev, &expo(40, 0, 0, jt), 1000);
        let r = find(&a, jt);
        assert_eq!(r.class, Class::Starved);
        assert_eq!(r.confidence, Confidence::High);
        assert!(r.suggest_worker_delta >= 1);
    }

    /// The very first scrape has no earlier sample, so a growing-but-served type is
    /// `Warming`, never a false under-provisioned call.
    #[test]
    fn first_scrape_is_warming() {
        let jt = "prov-test-warming:t";
        let (_, a) = compute_text(&Snapshot::default(), &expo(200, 3, 500, jt), 5000);
        assert_eq!(find(&a, jt).class, Class::Warming);
    }

    /// Backlog growing while workers drain and the server has headroom sizes extra
    /// workers by Little's Law — the under-provisioned hint the panel renders.
    #[test]
    fn under_provisioned_sizes_workers() {
        let jt = "prov-test-under:t";
        // 4 workers drained 200 jobs in 1s (50/worker); backlog grew 100/s ⇒ +2.
        let (prev, _) = compute_text(&Snapshot::default(), &expo(100, 4, 1000, jt), 0);
        let (_, a) = compute_text(&prev, &expo(200, 4, 1200, jt), 1000);
        assert!(!a.server_bound);
        let r = find(&a, jt);
        assert_eq!(r.class, Class::UnderProvisioned);
        assert_eq!(r.suggest_worker_delta, 2);
    }

    /// A backlog growing under the server's throughput ceiling classifies as
    /// `ServerBound` with no worker suggestion — the panel must not advise scaling
    /// workers when it wouldn't help.
    #[test]
    fn server_bound_suppresses_worker_suggestion() {
        let jt = "prov-test-serverbound:t";
        let ceiling = "nanobpm_ceiling_active{ceiling=\"throughput\"} 1\n";
        let (prev, _) = compute_text(
            &Snapshot::default(),
            &format!("{}{ceiling}", expo(100, 4, 1000, jt)),
            0,
        );
        let (_, a) = compute_text(&prev, &format!("{}{ceiling}", expo(300, 4, 1050, jt)), 1000);
        assert!(a.server_bound);
        let r = find(&a, jt);
        assert_eq!(r.class, Class::ServerBound);
        assert_eq!(r.suggest_worker_delta, 0);
    }
}
