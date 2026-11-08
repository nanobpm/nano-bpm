//! Snapshot-health observability (#1385, follow-up to #1383).
//!
//! #1383 showed a single unreadable/orphaned off-heap row could fail-close every
//! periodic snapshot for **23 days** on a production node: compaction stopped,
//! the journal grew to 5.5 GiB, and the only symptom was an ERROR line repeated
//! every tick. Nothing an operator watches changed. This module makes that
//! failure mode loud and bounded:
//!
//! * **Gauges** — seconds since the last successful snapshot, consecutive
//!   failures, uncompacted journal segments/bytes, quarantined off-heap
//!   orphans, data-dir free space, and a `degraded` flag
//!   ([`SnapshotHealth::render_prometheus`]).
//! * **A degraded health state** naming the cause and the blocking instance
//!   keys ([`SnapshotHealth::report`]), served at `GET /health`.
//! * **Deduplicated failure logging** — [`SnapshotHealth::record_failure`]
//!   says whether a failure is worth logging (the first of a streak, a changed
//!   reason, or a periodic reminder); the rest are counted, not printed.
//! * **A disk guard** — degrade (and WARN once, edge-triggered) when the
//!   uncompacted journal exceeds a byte budget or the data-dir free space drops
//!   below a percentage.
//!
//! The tracker is pure: every time-dependent method takes `now`, so the whole
//! state machine is tested deterministically, without sleeps.

use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use nanobpmn_engine_core::Key;

/// Default number of consecutive failed snapshot ticks tolerated before the
/// node reports degraded: the state is degraded once failures **exceed** it.
pub const DEFAULT_DEGRADED_AFTER_FAILURES: u64 = 3;
/// Default uncompacted-journal byte budget (8 GiB) above which the node
/// degrades. #1383's runaway journal reached 5.5 GiB after 23 days.
pub const DEFAULT_UNCOMPACTED_BYTES_MAX: u64 = 8 * 1024 * 1024 * 1024;
/// Default minimum free-space percentage of the data-dir volume.
pub const DEFAULT_DISK_FREE_MIN_PERCENT: f64 = 10.0;
/// While a failure streak continues with an unchanged reason, re-log it at
/// most once per this many failures (a reminder that it is still happening).
pub const FAILURE_LOG_EVERY: u64 = 100;

/// Thresholds that turn the raw gauges into a degraded state.
#[derive(Clone, Debug, PartialEq)]
pub struct HealthThresholds {
    /// Degrade once consecutive snapshot failures exceed this.
    pub degraded_after_failures: u64,
    /// Degrade once uncompacted journal bytes exceed this (`0` disables).
    pub uncompacted_bytes_max: u64,
    /// Degrade once data-dir free space falls below this percentage (`0`
    /// disables).
    pub disk_free_min_percent: f64,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            degraded_after_failures: DEFAULT_DEGRADED_AFTER_FAILURES,
            uncompacted_bytes_max: DEFAULT_UNCOMPACTED_BYTES_MAX,
            disk_free_min_percent: DEFAULT_DISK_FREE_MIN_PERCENT,
        }
    }
}

impl HealthThresholds {
    /// Reads the thresholds from the environment, falling back to the defaults
    /// for anything unset or unparseable:
    /// `NANOBPM_SNAPSHOT_DEGRADED_AFTER_FAILURES`,
    /// `NANOBPM_JOURNAL_UNCOMPACTED_BYTES_MAX`, `NANOBPM_DISK_FREE_MIN_PERCENT`.
    pub fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`from_env`](Self::from_env) over an injected lookup (testable).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let d = Self::default();
        let num = |name: &str| get(name).and_then(|v| v.trim().parse::<u64>().ok());
        Self {
            degraded_after_failures: num("NANOBPM_SNAPSHOT_DEGRADED_AFTER_FAILURES")
                .unwrap_or(d.degraded_after_failures),
            uncompacted_bytes_max: num("NANOBPM_JOURNAL_UNCOMPACTED_BYTES_MAX")
                .unwrap_or(d.uncompacted_bytes_max),
            disk_free_min_percent: get("NANOBPM_DISK_FREE_MIN_PERCENT")
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|p| p.is_finite() && (0.0..=100.0).contains(p))
                .unwrap_or(d.disk_free_min_percent),
        }
    }
}

/// Why the node is degraded. One variant per independent cause, so each is
/// alerted (edge-triggered) and reported on its own.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DegradedCause {
    /// Snapshots have failed for more than the tolerated number of ticks.
    SnapshotsFailing,
    /// The uncompacted journal exceeds its byte budget.
    JournalUncompactedBytes,
    /// The data-dir volume is low on free space.
    DiskLow,
}

impl DegradedCause {
    /// Stable machine-readable code (used in the `/health` body).
    pub fn code(&self) -> &'static str {
        match self {
            Self::SnapshotsFailing => "snapshots_failing",
            Self::JournalUncompactedBytes => "journal_uncompacted_bytes",
            Self::DiskLow => "disk_low",
        }
    }
}

/// The evaluated health of the snapshot/compaction pipeline.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthReport {
    /// `"healthy"` or `"degraded"`.
    pub status: &'static str,
    /// One human-readable line per [`DegradedCause`]; empty when healthy.
    pub reasons: Vec<String>,
    /// The [`DegradedCause::code`]s, aligned with `reasons`.
    pub causes: Vec<&'static str>,
    /// Instance keys whose off-heap payload blocked the latest failed snapshot.
    pub blocking_keys: Vec<Key>,
    pub consecutive_failures: u64,
    /// Seconds since the last successful snapshot (since tracker start when
    /// none has succeeded yet). `None` until the snapshot task has started.
    pub last_success_age_seconds: Option<u64>,
    pub last_failure_reason: Option<String>,
    pub uncompacted_segments: u64,
    pub uncompacted_bytes: u64,
    pub quarantined_orphans: u64,
    pub disk_free_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
}

impl HealthReport {
    pub fn is_degraded(&self) -> bool {
        self.status == "degraded"
    }
}

#[derive(Debug, Default)]
struct State {
    /// When the snapshot task started (the age baseline before any success).
    started: Option<Instant>,
    last_success: Option<Instant>,
    consecutive_failures: u64,
    last_failure_reason: Option<String>,
    blocking_keys: Vec<Key>,
    failure_logs_suppressed: u64,
    uncompacted_segments: u64,
    uncompacted_bytes: u64,
    quarantined_orphans: u64,
    /// `(free, total)` bytes of the data-dir volume, when known.
    disk: Option<(u64, u64)>,
    /// Whether storage usage (journal backlog / disk) has been sampled at
    /// least once — gates the storage gauges independently of the snapshot
    /// task, which may be disabled (`NANOBPMN_SNAPSHOT_INTERVAL_MS=0`).
    storage_sampled: bool,
    /// Causes already alerted, so a WARN fires once on entry, not every tick.
    alerted: Vec<DegradedCause>,
}

/// Process-wide snapshot-health tracker. Cheap to update (a mutex over a few
/// integers), read on scrape and on `GET /health`.
#[derive(Debug, Default)]
pub struct SnapshotHealth {
    state: Mutex<State>,
}

/// The process-global tracker the snapshot maintenance task feeds and the
/// `/metrics` + `/health` handlers read.
pub fn global() -> &'static SnapshotHealth {
    static GLOBAL: LazyLock<SnapshotHealth> = LazyLock::new(SnapshotHealth::default);
    &GLOBAL
}

/// Free space of the volume as a percentage of its total.
fn free_percent(free: u64, total: u64) -> Option<f64> {
    (total > 0).then(|| free as f64 * 100.0 / total as f64)
}

impl SnapshotHealth {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Marks the snapshot task as started at `now` (the age baseline until the
    /// first success). Idempotent: only the first call counts.
    pub fn start(&self, now: Instant) {
        let mut s = self.lock();
        s.started.get_or_insert(now);
    }

    /// Records a successful snapshot+compaction tick. Returns `true` when this
    /// success ends a failure streak (worth an INFO "recovered" line).
    pub fn record_success(&self, now: Instant) -> bool {
        let mut s = self.lock();
        s.started.get_or_insert(now);
        let recovered = s.consecutive_failures > 0;
        s.last_success = Some(now);
        s.consecutive_failures = 0;
        s.last_failure_reason = None;
        s.blocking_keys.clear();
        recovered
    }

    /// Records a failed snapshot tick: `reason` describes it, `blocking_keys`
    /// names the instances whose off-heap payload aborted the fold (empty for a
    /// non-payload failure such as a snapshot write error).
    ///
    /// Returns whether the caller should **log** this failure: the first of a
    /// streak, a changed reason or blocker set, or every [`FAILURE_LOG_EVERY`]th
    /// repeat. Otherwise the failure is only counted
    /// ([`failure_logs_suppressed`](Self::failure_logs_suppressed)) — the gauges
    /// and `/health` carry the ongoing state, not a log line per tick.
    pub fn record_failure(&self, now: Instant, reason: &str, blocking_keys: Vec<Key>) -> bool {
        let mut s = self.lock();
        s.started.get_or_insert(now);
        let changed =
            s.last_failure_reason.as_deref() != Some(reason) || s.blocking_keys != blocking_keys;
        s.consecutive_failures += 1;
        let log = changed || s.consecutive_failures.is_multiple_of(FAILURE_LOG_EVERY);
        if !log {
            s.failure_logs_suppressed += 1;
        }
        s.last_failure_reason = Some(reason.to_string());
        s.blocking_keys = blocking_keys;
        log
    }

    /// Updates the uncompacted journal footprint (segments still on disk, and
    /// their total bytes, including the active segment).
    pub fn set_journal_usage(&self, segments: u64, bytes: u64) {
        let mut s = self.lock();
        s.uncompacted_segments = segments;
        s.uncompacted_bytes = bytes;
        s.storage_sampled = true;
    }

    /// Updates the count of instances quarantined as off-heap orphans.
    pub fn set_quarantined_orphans(&self, n: u64) {
        self.lock().quarantined_orphans = n;
    }

    /// Updates the data-dir volume's `(free, total)` bytes (`None` = unknown).
    pub fn set_disk(&self, disk: Option<(u64, u64)>) {
        let mut s = self.lock();
        s.disk = disk;
        s.storage_sampled = true;
    }

    /// Failure log lines suppressed as repeats so far.
    pub fn failure_logs_suppressed(&self) -> u64 {
        self.lock().failure_logs_suppressed
    }

    fn causes(s: &State, t: &HealthThresholds) -> Vec<(DegradedCause, String)> {
        let mut out = Vec::new();
        if s.consecutive_failures > t.degraded_after_failures {
            let mut msg = format!(
                "journal snapshots have failed {} consecutive time(s): {}",
                s.consecutive_failures,
                s.last_failure_reason.as_deref().unwrap_or("unknown"),
            );
            if !s.blocking_keys.is_empty() {
                msg.push_str(&format!(
                    "; blocking instance key(s): {:?}",
                    s.blocking_keys
                ));
            }
            out.push((DegradedCause::SnapshotsFailing, msg));
        }
        if t.uncompacted_bytes_max > 0 && s.uncompacted_bytes > t.uncompacted_bytes_max {
            out.push((
                DegradedCause::JournalUncompactedBytes,
                format!(
                    "uncompacted journal is {} bytes across {} segment(s), over the {} byte budget",
                    s.uncompacted_bytes, s.uncompacted_segments, t.uncompacted_bytes_max
                ),
            ));
        }
        if t.disk_free_min_percent > 0.0
            && let Some((free, total)) = s.disk
            && let Some(pct) = free_percent(free, total)
            && pct < t.disk_free_min_percent
        {
            out.push((
                DegradedCause::DiskLow,
                format!(
                    "data-dir volume has {pct:.1}% free ({free} of {total} bytes), below {}%",
                    t.disk_free_min_percent
                ),
            ));
        }
        out
    }

    /// Evaluates the current health at `now` against `t`.
    pub fn report(&self, now: Instant, t: &HealthThresholds) -> HealthReport {
        let s = self.lock();
        let causes = Self::causes(&s, t);
        let age = s
            .last_success
            .or(s.started)
            .map(|at| now.saturating_duration_since(at).as_secs());
        HealthReport {
            status: if causes.is_empty() {
                "healthy"
            } else {
                "degraded"
            },
            causes: causes.iter().map(|(c, _)| c.code()).collect(),
            reasons: causes.into_iter().map(|(_, r)| r).collect(),
            blocking_keys: s.blocking_keys.clone(),
            consecutive_failures: s.consecutive_failures,
            last_success_age_seconds: age,
            last_failure_reason: s.last_failure_reason.clone(),
            uncompacted_segments: s.uncompacted_segments,
            uncompacted_bytes: s.uncompacted_bytes,
            quarantined_orphans: s.quarantined_orphans,
            disk_free_bytes: s.disk.map(|d| d.0),
            disk_total_bytes: s.disk.map(|d| d.1),
        }
    }

    /// Edge-triggered alerting: returns the causes that became active and those
    /// that cleared since the previous call, so the caller WARNs once on entry
    /// and INFOs once on recovery instead of logging every tick.
    pub fn alert_transitions(
        &self,
        t: &HealthThresholds,
    ) -> (Vec<(DegradedCause, String)>, Vec<DegradedCause>) {
        let mut s = self.lock();
        let now_active = Self::causes(&s, t);
        let entered: Vec<_> = now_active
            .iter()
            .filter(|(c, _)| !s.alerted.contains(c))
            .cloned()
            .collect();
        let cleared: Vec<_> = s
            .alerted
            .iter()
            .filter(|c| !now_active.iter().any(|(a, _)| a == *c))
            .cloned()
            .collect();
        s.alerted = now_active.into_iter().map(|(c, _)| c).collect();
        (entered, cleared)
    }

    /// Prometheus text-exposition of the snapshot-health gauges. The
    /// snapshot-pipeline gauges (age, failures, suppressed logs) are emitted
    /// only once the snapshot task has started; the storage gauges (backlog,
    /// disk, orphans) and the degraded flag are emitted once either the
    /// snapshot task has started or storage has been sampled — so a node with
    /// snapshots disabled still exports what `/health` reports. Emits nothing
    /// when neither has happened (no segmented storage to report on).
    pub fn render_prometheus(&self, now: Instant, t: &HealthThresholds) -> String {
        use std::fmt::Write as _;
        let r = self.report(now, t);
        let sampled = self.lock().storage_sampled;
        let age = r.last_success_age_seconds;
        if age.is_none() && !sampled {
            return String::new();
        }
        let suppressed = self.failure_logs_suppressed();
        let mut out = String::new();
        let mut gauge = |name: &str, kind: &str, help: &str, value: String| {
            let _ = write!(
                out,
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
            );
        };
        if let Some(age) = age {
            gauge(
                "nanobpm_journal_snapshot_last_success_age_seconds",
                "gauge",
                "Seconds since the last successful journal snapshot (since start if none yet) (#1385).",
                age.to_string(),
            );
            gauge(
                "nanobpm_journal_snapshot_consecutive_failures",
                "gauge",
                "Consecutive failed journal snapshot ticks (#1385).",
                r.consecutive_failures.to_string(),
            );
            gauge(
                "nanobpm_journal_snapshot_failure_logs_suppressed_total",
                "counter",
                "Repeated snapshot-failure log lines suppressed as duplicates (#1385).",
                suppressed.to_string(),
            );
        }
        gauge(
            "nanobpm_journal_uncompacted_segments",
            "gauge",
            "Journal segments on disk not yet compacted, including the active one (#1385).",
            r.uncompacted_segments.to_string(),
        );
        gauge(
            "nanobpm_journal_uncompacted_bytes",
            "gauge",
            "Bytes of journal segments on disk not yet compacted (#1385).",
            r.uncompacted_bytes.to_string(),
        );
        gauge(
            "nanobpm_journal_quarantined_orphans",
            "gauge",
            "Instances quarantined as off-heap orphans (#1383/#1385).",
            r.quarantined_orphans.to_string(),
        );
        if let (Some(free), Some(total)) = (r.disk_free_bytes, r.disk_total_bytes) {
            gauge(
                "nanobpm_data_dir_free_bytes",
                "gauge",
                "Free bytes on the data-dir volume (#1385).",
                free.to_string(),
            );
            gauge(
                "nanobpm_data_dir_total_bytes",
                "gauge",
                "Total bytes of the data-dir volume (#1385).",
                total.to_string(),
            );
        }
        gauge(
            "nanobpm_snapshot_health_degraded",
            "gauge",
            "1 when snapshot health is degraded (see GET /health for the reasons) (#1385).",
            u8::from(r.is_degraded()).to_string(),
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn after(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    fn t() -> HealthThresholds {
        HealthThresholds {
            degraded_after_failures: 2,
            uncompacted_bytes_max: 1_000,
            disk_free_min_percent: 10.0,
        }
    }

    #[test]
    fn fresh_tracker_is_healthy_and_silent_before_start() {
        let h = SnapshotHealth::new();
        let now = Instant::now();
        let r = h.report(now, &t());
        assert_eq!(r.status, "healthy");
        assert_eq!(r.last_success_age_seconds, None);
        assert!(h.render_prometheus(now, &t()).is_empty());
    }

    // Snapshots disabled: the sampler alone populates storage usage, so the
    // storage gauges + degraded flag must still be exported (matching
    // `/health`), while the snapshot-pipeline gauges stay absent.
    #[test]
    fn storage_gauges_render_without_snapshot_task() {
        let h = SnapshotHealth::new();
        let now = Instant::now();
        h.set_journal_usage(3, 5_000);
        h.set_disk(Some((50, 100)));
        assert_eq!(h.report(now, &t()).status, "degraded");
        let prom = h.render_prometheus(now, &t());
        assert!(
            prom.contains("nanobpm_journal_uncompacted_bytes 5000"),
            "{prom}"
        );
        assert!(prom.contains("nanobpm_data_dir_free_bytes 50"), "{prom}");
        assert!(
            prom.contains("nanobpm_snapshot_health_degraded 1"),
            "{prom}"
        );
        assert!(
            !prom.contains("nanobpm_journal_snapshot_last_success_age_seconds"),
            "{prom}"
        );
        assert!(
            !prom.contains("nanobpm_journal_snapshot_consecutive_failures"),
            "{prom}"
        );
    }

    // The #1383 failure mode: snapshots fail every tick on an unreadable
    // payload. The node must go degraded after N ticks, name the blocking key,
    // and the age gauge must keep growing — then recover on the next success.
    #[test]
    fn persistent_snapshot_failure_degrades_names_blockers_and_recovers() {
        let h = SnapshotHealth::new();
        let base = Instant::now();
        h.start(base);
        assert!(h.record_failure(after(base, 10), "row unreadable", vec![42]));
        assert!(!h.record_failure(after(base, 20), "row unreadable", vec![42]));
        assert_eq!(
            h.report(after(base, 20), &t()).status,
            "healthy",
            "N=2 tolerated"
        );
        assert!(!h.record_failure(after(base, 30), "row unreadable", vec![42]));
        let r = h.report(after(base, 30), &t());
        assert!(r.is_degraded());
        assert_eq!(r.causes, vec!["snapshots_failing"]);
        assert_eq!(r.blocking_keys, vec![42]);
        assert!(r.reasons[0].contains("42"), "{:?}", r.reasons);
        assert_eq!(r.last_success_age_seconds, Some(30));
        assert_eq!(h.failure_logs_suppressed(), 2);
        let prom = h.render_prometheus(after(base, 30), &t());
        assert!(prom.contains("nanobpm_journal_snapshot_consecutive_failures 3\n"));
        assert!(prom.contains("nanobpm_journal_snapshot_last_success_age_seconds 30\n"));
        assert!(prom.contains("nanobpm_snapshot_health_degraded 1\n"));

        assert!(h.record_success(after(base, 40)));
        let r = h.report(after(base, 45), &t());
        assert_eq!(r.status, "healthy");
        assert!(r.blocking_keys.is_empty());
        assert_eq!(r.last_success_age_seconds, Some(5));
        assert!(!h.record_success(after(base, 50)), "no streak to end");
    }

    #[test]
    fn failure_logging_dedupes_but_relogs_on_change_and_periodically() {
        let h = SnapshotHealth::new();
        let now = Instant::now();
        assert!(h.record_failure(now, "a", vec![1]));
        assert!(!h.record_failure(now, "a", vec![1]));
        assert!(h.record_failure(now, "b", vec![1]), "reason changed");
        assert!(h.record_failure(now, "b", vec![2]), "blocker changed");
        let mut logged = 0;
        for _ in 0..(FAILURE_LOG_EVERY * 2) {
            logged += u64::from(h.record_failure(now, "b", vec![2]));
        }
        assert_eq!(logged, 2, "periodic reminder every {FAILURE_LOG_EVERY}");
        h.record_success(now);
        assert!(h.record_failure(now, "b", vec![2]), "new streak logs again");
    }

    #[test]
    fn disk_guard_degrades_on_uncompacted_bytes_and_low_free_space() {
        let h = SnapshotHealth::new();
        let now = Instant::now();
        h.start(now);
        h.set_journal_usage(3, 1_001);
        h.set_disk(Some((5, 100)));
        let r = h.report(now, &t());
        assert_eq!(r.causes, vec!["journal_uncompacted_bytes", "disk_low"]);
        h.set_journal_usage(1, 1_000);
        h.set_disk(Some((10, 100)));
        assert_eq!(
            h.report(now, &t()).status,
            "healthy",
            "thresholds are strict"
        );
        let off = HealthThresholds {
            uncompacted_bytes_max: 0,
            disk_free_min_percent: 0.0,
            ..t()
        };
        h.set_journal_usage(1, u64::MAX);
        h.set_disk(Some((0, 100)));
        assert_eq!(h.report(now, &off).status, "healthy", "0 disables");
        h.set_disk(None);
        let prom = h.render_prometheus(now, &t());
        assert!(!prom.contains("nanobpm_data_dir_free_bytes"));
    }

    #[test]
    fn alerts_are_edge_triggered() {
        let h = SnapshotHealth::new();
        h.start(Instant::now());
        h.set_disk(Some((1, 100)));
        let (entered, cleared) = h.alert_transitions(&t());
        assert_eq!(entered.len(), 1);
        assert!(cleared.is_empty());
        let (entered, cleared) = h.alert_transitions(&t());
        assert!(entered.is_empty() && cleared.is_empty(), "no repeat");
        h.set_disk(Some((50, 100)));
        let (entered, cleared) = h.alert_transitions(&t());
        assert!(entered.is_empty());
        assert_eq!(cleared, vec![DegradedCause::DiskLow]);
    }

    #[test]
    fn thresholds_parse_from_env_with_fallbacks() {
        let th = HealthThresholds::from_lookup(|k| match k {
            "NANOBPM_SNAPSHOT_DEGRADED_AFTER_FAILURES" => Some("7".into()),
            "NANOBPM_JOURNAL_UNCOMPACTED_BYTES_MAX" => Some("junk".into()),
            "NANOBPM_DISK_FREE_MIN_PERCENT" => Some("150".into()),
            _ => None,
        });
        assert_eq!(th.degraded_after_failures, 7);
        assert_eq!(th.uncompacted_bytes_max, DEFAULT_UNCOMPACTED_BYTES_MAX);
        assert_eq!(th.disk_free_min_percent, DEFAULT_DISK_FREE_MIN_PERCENT);
    }
}
