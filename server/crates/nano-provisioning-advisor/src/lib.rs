//! Worker-scaling advisor — the single source of truth for the per-job-type
//! provisioning classification (starved / under-provisioned / server-bound /
//! adequate) and the Little's-Law worker sizing.
//!
//! This logic began life inside `processos` (`processos/src/advisor.rs`), but the
//! console fleet-sizing hint (#1294) needs the *same* classification server-side
//! so the thresholds are not reimplemented in TypeScript. The one-way dependency
//! rule is firm — **Nano never links ProcessOS** — so the shared piece lives here,
//! on the Nano side (a `server/crates/*` leaf), and ProcessOS consumes it by re-export
//! (ProcessOS may link Nano crates; it already embeds `engine-core`). Both the
//! gateway's `/console/api/provisioning` endpoint and the ProcessOS cockpit's
//! `advisor_advise` route call into this crate, so there is one implementation of
//! the thresholds and Little's-Law sizing, tested once.
//!
//! Reads a live Nano instance's Prometheus `/metrics` and, from the per-job-type
//! backlog + drain-rate signals and the server-saturation signals, advises whether
//! adding workers for a job type would actually raise throughput — and refuses to
//! recommend it when the real bottleneck is the single-writer server.
//!
//! It is **advisory only**: it computes a recommendation, it does not actuate. The
//! logic is pure (parse text → snapshot; diff two snapshots over a window → advice)
//! so it is fully unit-testable off any live engine.
//!
//! ## The signals (all from the public `/metrics` contract)
//! Per job type: `nanobpm_job_type_activatable` (backlog level), `job_type_workers`
//! (subscribed stream workers), `job_type_dispatched_total` (cumulative jobs handed
//! to workers → the drain throughput). Global: `nanobpm_ceiling_active{ceiling="throughput"}`
//! (the clipping LED), the journal-writer busy/idle counters (duty cycle),
//! `pending_create_queue`, and `admission_shed_total`.
//!
//! ## The diagnosis (see the four classes below)
//! Because we have *both* the per-type backlog signals *and* the server-saturation
//! signals, the advisor can separate the two failure modes the operator cares about:
//! "not enough workers for a job type" (scale workers) vs. "the server itself is the
//! wall" (scaling workers is futile). The per-type drain rate `D` turns the
//! under-provisioning case from directional ("backlog is growing") into quantitative
//! ("each worker drains D/W /s, backlog grows at S/s → add ⌈S·W/D⌉ workers").

use std::collections::BTreeMap;

/// Backlog below this (jobs) is treated as noise — never an under-provisioning call.
const MIN_BACKLOG: i64 = 50;
/// Sustained backlog growth (jobs/s) above this counts as "falling behind".
const MIN_SLOPE_PER_S: f64 = 1.0;
/// Writer duty cycle (busy fraction over the window) above this ⇒ server-bound.
const WRITER_SATURATED: f64 = 0.85;

/// One job type's raw counters at a scrape instant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobTypeSample {
    pub activatable: i64,
    pub workers: i64,
    pub dispatched_total: u64,
}

/// A parsed point-in-time read of the signals the advisor needs.
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub ts_ms: u64,
    pub per_type: BTreeMap<String, JobTypeSample>,
    pub ceiling_throughput: bool,
    pub writer_busy_seconds: f64,
    pub writer_idle_seconds: f64,
    pub pending_create_queue: i64,
    pub admission_shed_total: u64,
}

/// Bottleneck classification for one job type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Class {
    /// Jobs waiting, zero workers subscribed — no drain capacity at all.
    Starved,
    /// Workers present but backlog growing while the server has headroom — adding
    /// workers should raise drain.
    UnderProvisioned,
    /// Backlog growing but the server is at its ceiling — more workers won't raise
    /// aggregate throughput; relieve the server instead.
    ServerBound,
    /// Backlog small or shrinking — provisioning is adequate.
    Adequate,
    /// Not enough history yet (first scrape / no prior sample) to judge rates.
    Warming,
}

/// Confidence in a recommendation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Confidence {
    High,
    Medium,
    Low,
}

/// One job type's recommendation.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Recommendation {
    pub job_type: String,
    pub class: Class,
    pub confidence: Confidence,
    pub backlog: i64,
    pub backlog_slope_per_s: f64,
    pub drain_per_s: f64,
    pub workers: i64,
    /// Suggested change to the worker count (0 = leave alone).
    pub suggest_worker_delta: i64,
    pub rationale: String,
}

/// The advisor's whole-instance verdict for one window.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Advice {
    /// True when the server itself is the bottleneck (throughput ceiling lit or the
    /// journal writer saturated) — scaling workers will not raise aggregate throughput.
    pub server_bound: bool,
    /// Journal-writer duty cycle over the window (busy fraction, 0..1).
    pub writer_busy_ratio: f64,
    /// The `nanobpm_ceiling_active{ceiling="throughput"}` clipping LED.
    pub ceiling_throughput: bool,
    pub pending_create_queue: i64,
    /// Admissions shed over the window (delta) — nonzero ⇒ the server is shedding load.
    pub shed_delta: u64,
    /// Seconds between the two samples this advice was computed over.
    pub window_s: f64,
    pub recommendations: Vec<Recommendation>,
}

/// Parses one field value out of a Prometheus exposition line of the form
/// `name value` (no labels). Returns the last match's value.
fn parse_scalar(text: &str, name: &str) -> Option<f64> {
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(name) {
            // The char right after the name must be whitespace, so `foo` doesn't
            // match `foo_bar`, and a labelled `foo{...}` is skipped here.
            if rest.starts_with(char::is_whitespace)
                && let Ok(v) = rest.trim().parse::<f64>()
            {
                return Some(v);
            }
        }
    }
    None
}

/// One parsed `key="value"` pair out of a Prometheus label set. The value is
/// owned because escape sequences (`\\`, `\"`, `\n`) are decoded.
#[derive(Debug, PartialEq, Eq)]
struct LabelPair {
    key: String,
    value: String,
}

/// Parses a Prometheus label-set body (the text between `{` and `}`) into its
/// pairs, honouring the exposition-format quoted-string grammar: label values
/// are double-quoted, may legally contain `,`, `=` and `{`/`}`, and support the
/// escape sequences `\\`, `\"` and `\n`. A naive `split(',')` truncates a value
/// like `job_type="a,b"` at the embedded comma — merging two distinct job types
/// into one and returning incorrect worker/backlog advice — so the pairs are
/// scanned properly. Returns `None` on malformed input (the caller skips the
/// series rather than misreading it).
fn parse_labels(labels: &str) -> Option<Vec<LabelPair>> {
    let mut pairs = Vec::new();
    let cs: Vec<char> = labels.chars().collect();
    let mut i = 0;
    loop {
        // Skip whitespace and the commas between pairs.
        while i < cs.len() && (cs[i].is_whitespace() || cs[i] == ',') {
            i += 1;
        }
        if i >= cs.len() {
            return Some(pairs);
        }
        // Bare key up to `=`.
        let mut key = String::new();
        while i < cs.len() && cs[i] != '=' {
            if cs[i] == '"' || cs[i] == '{' || cs[i] == '}' {
                return None; // malformed: quote/brace inside a bare key
            }
            key.push(cs[i]);
            i += 1;
        }
        if i >= cs.len() {
            return None; // key with no `=`
        }
        i += 1; // consume `=`
        let key = key.trim().to_string();
        if key.is_empty() {
            return None;
        }
        // Optional whitespace, then the opening quote of the value.
        while i < cs.len() && cs[i].is_whitespace() {
            i += 1;
        }
        if i >= cs.len() || cs[i] != '"' {
            return None; // unquoted value — not the exposition format
        }
        i += 1; // consume opening quote
        // Quoted value with escape decoding; runs to the closing quote, so
        // embedded commas and `=` are content, not separators.
        let mut value = String::new();
        let mut closed = false;
        while i < cs.len() {
            match cs[i] {
                '"' => {
                    closed = true;
                    i += 1;
                    break;
                }
                '\\' => {
                    i += 1;
                    if i >= cs.len() {
                        return None; // dangling escape — unterminated
                    }
                    match cs[i] {
                        '\\' => value.push('\\'),
                        '"' => value.push('"'),
                        'n' => value.push('\n'),
                        // Unknown escape: keep it verbatim rather than dropping a char.
                        other => {
                            value.push('\\');
                            value.push(other);
                        }
                    }
                    i += 1;
                }
                c => {
                    value.push(c);
                    i += 1;
                }
            }
        }
        if !closed {
            return None; // unterminated quoted value
        }
        pairs.push(LabelPair { key, value });
    }
}

/// Extracts the decoded string value of a single label from a Prometheus label
/// set body, e.g. `job_type="foo",worker="w"` → for `job_type`, `Some("foo")`.
fn label_value(labels: &str, key: &str) -> Option<String> {
    parse_labels(labels)?
        .into_iter()
        .find(|p| p.key == key)
        .map(|p| p.value)
}

/// Iterates `(labels, value)` for every series of `name` in the text.
fn each_series<'a>(text: &'a str, name: &'a str) -> impl Iterator<Item = (&'a str, f64)> {
    text.lines().filter_map(move |line| {
        let line = line.trim();
        if line.starts_with('#') {
            return None;
        }
        let rest = line.strip_prefix(name)?;
        let rest = rest.strip_prefix('{')?;
        let (labels, tail) = rest.split_once('}')?;
        let value = tail.trim().parse::<f64>().ok()?;
        Some((labels, value))
    })
}

/// Parses the raw Prometheus exposition text into the advisor's [`Snapshot`].
pub fn parse_snapshot(text: &str, ts_ms: u64) -> Snapshot {
    let mut per_type: BTreeMap<String, JobTypeSample> = BTreeMap::new();

    let mut upsert = |labels: &str, f: &dyn Fn(&mut JobTypeSample)| {
        if let Some(jt) = label_value(labels, "job_type") {
            f(per_type.entry(jt.to_string()).or_default());
        }
    };
    for (labels, v) in each_series(text, "nanobpm_job_type_activatable") {
        upsert(labels, &|s| s.activatable = v as i64);
    }
    for (labels, v) in each_series(text, "nanobpm_job_type_workers") {
        upsert(labels, &|s| s.workers = v as i64);
    }
    for (labels, v) in each_series(text, "nanobpm_job_type_dispatched_total") {
        upsert(labels, &|s| s.dispatched_total = v as u64);
    }

    let ceiling_throughput = each_series(text, "nanobpm_ceiling_active").any(|(labels, v)| {
        label_value(labels, "ceiling").as_deref() == Some("throughput") && v != 0.0
    });
    let shed_total: f64 = each_series(text, "nanobpm_admission_shed_total")
        .map(|(_, v)| v)
        .sum();

    Snapshot {
        ts_ms,
        per_type,
        ceiling_throughput,
        writer_busy_seconds: parse_scalar(text, "nanobpm_journal_writer_busy_seconds")
            .unwrap_or(0.0),
        writer_idle_seconds: parse_scalar(text, "nanobpm_journal_writer_idle_seconds")
            .unwrap_or(0.0),
        pending_create_queue: parse_scalar(text, "nanobpm_pending_create_queue").unwrap_or(0.0)
            as i64,
        admission_shed_total: shed_total as u64,
    }
}

/// Computes the advice for the window between two snapshots. `prev` is the earlier
/// sample; when the two are the same instant (or `prev` is empty) every rate is
/// unknown and types are reported as `Warming` (except unambiguous hard starvation).
pub fn advise(prev: &Snapshot, cur: &Snapshot) -> Advice {
    let window_s = (cur.ts_ms.saturating_sub(prev.ts_ms)) as f64 / 1000.0;
    // A default/empty `prev` (the first scrape for this instance) carries no series,
    // so its epoch-sized `window_s` is meaningless — treat it as no window.
    let has_window = window_s > 0.0 && !prev.per_type.is_empty();
    let window_s = if has_window { window_s } else { 0.0 };

    // Journal-writer duty cycle over the window (busy fraction).
    let busy_d = (cur.writer_busy_seconds - prev.writer_busy_seconds).max(0.0);
    let idle_d = (cur.writer_idle_seconds - prev.writer_idle_seconds).max(0.0);
    let writer_busy_ratio = if busy_d + idle_d > 0.0 {
        busy_d / (busy_d + idle_d)
    } else {
        0.0
    };
    let server_bound = cur.ceiling_throughput || writer_busy_ratio > WRITER_SATURATED;
    let shed_delta = cur
        .admission_shed_total
        .saturating_sub(prev.admission_shed_total);

    let mut recommendations = Vec::with_capacity(cur.per_type.len());
    for (jt, s) in &cur.per_type {
        let p = prev.per_type.get(jt);
        let backlog = s.activatable;
        let workers = s.workers;

        let backlog_slope_per_s = match (p, has_window) {
            (Some(p), true) => (backlog - p.activatable) as f64 / window_s,
            _ => 0.0,
        };
        let drain_per_s = match (p, has_window) {
            (Some(p), true) if s.dispatched_total >= p.dispatched_total => {
                (s.dispatched_total - p.dispatched_total) as f64 / window_s
            }
            _ => 0.0,
        };

        let (class, confidence, suggest_worker_delta, rationale) = classify(
            backlog,
            workers,
            backlog_slope_per_s,
            drain_per_s,
            server_bound,
            writer_busy_ratio,
            p.is_some() && has_window,
        );

        recommendations.push(Recommendation {
            job_type: jt.clone(),
            class,
            confidence,
            backlog,
            backlog_slope_per_s: round1(backlog_slope_per_s),
            drain_per_s: round1(drain_per_s),
            workers,
            suggest_worker_delta,
            rationale,
        });
    }

    // Most actionable first: Starved, then UnderProvisioned, then the rest; within a
    // class, the deepest backlog leads.
    recommendations.sort_by(|a, b| {
        rank(a.class)
            .cmp(&rank(b.class))
            .then(b.backlog.cmp(&a.backlog))
    });

    Advice {
        server_bound,
        writer_busy_ratio: round2(writer_busy_ratio),
        ceiling_throughput: cur.ceiling_throughput,
        pending_create_queue: cur.pending_create_queue,
        shed_delta,
        window_s: round1(window_s),
        recommendations,
    }
}

#[allow(clippy::too_many_arguments)]
fn classify(
    backlog: i64,
    workers: i64,
    slope: f64,
    drain: f64,
    server_bound: bool,
    writer_ratio: f64,
    have_rates: bool,
) -> (Class, Confidence, i64, String) {
    // 1. Hard starvation — unambiguous, no rates needed: jobs waiting, no worker.
    if backlog > 0 && workers == 0 {
        // Going from zero drain always helps *this* type, but when the server is also
        // at its ceiling the aggregate gain is uncertain — downgrade the confidence.
        let (confidence, note) = if server_bound {
            (
                Confidence::Medium,
                " (server is near its ceiling, so aggregate throughput may stay capped, \
                 but going from zero drain still helps this type)",
            )
        } else {
            (Confidence::High, "")
        };
        return (
            Class::Starved,
            confidence,
            1,
            format!(
                "{backlog} jobs waiting with 0 workers subscribed — start at least one \
                 worker, then re-measure the drain rate to size it{note}."
            ),
        );
    }

    // 2. Server-bound: this type is falling behind but the wall is the single-writer
    //    server, not the workers. Scaling workers won't raise aggregate throughput.
    if server_bound && backlog > MIN_BACKLOG && slope > MIN_SLOPE_PER_S {
        return (
            Class::ServerBound,
            Confidence::High,
            0,
            format!(
                "backlog {backlog} growing at {:.0}/s, but the server is at its throughput \
                 ceiling (writer {:.0}% busy) — more workers won't raise aggregate throughput; \
                 relieve the server (admission/shed, more partitions, or lower the create rate).",
                slope,
                writer_ratio * 100.0
            ),
        );
    }

    // 3. Under-provisioned: workers present, backlog growing, server has headroom.
    if !server_bound && workers > 0 && backlog > MIN_BACKLOG && slope > MIN_SLOPE_PER_S {
        if drain > 0.0 {
            // Little's Law: each worker drains ≈ drain/workers per second; to also
            // absorb the growth we need ⌈slope · workers / drain⌉ more workers.
            let per_worker = drain / workers as f64;
            let extra = (slope / per_worker).ceil() as i64;
            let extra = extra.max(1);
            return (
                Class::UnderProvisioned,
                Confidence::High,
                extra,
                format!(
                    "backlog {backlog} growing at {:.0}/s; {workers} workers draining {:.0}/s \
                     (≈{:.1}/worker) — add ~{extra} worker(s) to absorb the growth (server has headroom).",
                    slope, drain, per_worker
                ),
            );
        }
        // Workers subscribed but no drain observed this window — likely all busy on
        // long jobs or blocked. Directional only.
        return (
            Class::UnderProvisioned,
            Confidence::Low,
            1,
            format!(
                "backlog {backlog} growing at {:.0}/s with {workers} workers but ~0 drain this \
                 window (long-running or blocked handlers?) — add 1 and re-measure.",
                slope
            ),
        );
    }

    // 4. No prior sample yet — can't judge growth/drain.
    if !have_rates {
        return (
            Class::Warming,
            Confidence::Low,
            0,
            "collecting a second sample to measure backlog growth and drain rate.".to_string(),
        );
    }

    // 5. Adequate — backlog small or not growing.
    (
        Class::Adequate,
        Confidence::High,
        0,
        "backlog stable or shrinking — provisioning looks adequate.".to_string(),
    )
}

fn rank(c: Class) -> u8 {
    match c {
        Class::Starved => 0,
        Class::UnderProvisioned => 1,
        Class::ServerBound => 2,
        Class::Warming => 3,
        Class::Adequate => 4,
    }
}

fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}
fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(ts_ms: u64, types: &[(&str, i64, i64, u64)]) -> Snapshot {
        let mut per_type = BTreeMap::new();
        for (jt, activatable, workers, dispatched_total) in types {
            per_type.insert(
                jt.to_string(),
                JobTypeSample {
                    activatable: *activatable,
                    workers: *workers,
                    dispatched_total: *dispatched_total,
                },
            );
        }
        Snapshot {
            ts_ms,
            per_type,
            ..Default::default()
        }
    }

    fn rec<'a>(a: &'a Advice, jt: &str) -> &'a Recommendation {
        a.recommendations
            .iter()
            .find(|r| r.job_type == jt)
            .expect("recommendation present")
    }

    #[test]
    fn parses_per_type_and_global_signals() {
        let text = r#"
# HELP nanobpm_job_type_activatable x
nanobpm_job_type_activatable{job_type="a"} 120
nanobpm_job_type_workers{job_type="a"} 4
nanobpm_job_type_dispatched_total{job_type="a"} 900
nanobpm_ceiling_active{ceiling="throughput"} 1
nanobpm_ceiling_active{ceiling="memory"} 0
nanobpm_journal_writer_busy_seconds 12.5
nanobpm_journal_writer_idle_seconds 7.5
nanobpm_pending_create_queue 42
nanobpm_admission_shed_total{reason="create_queue"} 3
nanobpm_admission_shed_total{reason="mem_watermark"} 2
"#;
        let s = parse_snapshot(text, 1000);
        let a = s.per_type.get("a").unwrap();
        assert_eq!(a.activatable, 120);
        assert_eq!(a.workers, 4);
        assert_eq!(a.dispatched_total, 900);
        assert!(s.ceiling_throughput);
        assert_eq!(s.writer_busy_seconds, 12.5);
        assert_eq!(s.writer_idle_seconds, 7.5);
        assert_eq!(s.pending_create_queue, 42);
        assert_eq!(s.admission_shed_total, 5);
    }

    /// Label values are quoted and may legally contain commas, `=` and braces —
    /// a naive `split(',')` would truncate `job_type="a,b"` to `a` and merge two
    /// distinct job types into one row of advice. The quoted-string grammar must
    /// keep them apart.
    #[test]
    fn label_values_with_commas_and_escapes_parse_correctly() {
        let text = "nanobpm_job_type_workers{job_type=\"a,b\"} 2\n\
                    nanobpm_job_type_workers{job_type=\"a\"} 5\n\
                    nanobpm_job_type_workers{job_type=\"x=1\"} 3\n\
                    nanobpm_job_type_workers{job_type=\"q\\\"z\"} 7\n\
                    nanobpm_job_type_workers{job_type=\"back\\\\slash\"} 11\n";
        let s = parse_snapshot(text, 1000);
        assert_eq!(s.per_type.get("a,b").map(|j| j.workers), Some(2));
        assert_eq!(s.per_type.get("a").map(|j| j.workers), Some(5));
        assert_eq!(s.per_type.get("x=1").map(|j| j.workers), Some(3));
        assert_eq!(s.per_type.get("q\"z").map(|j| j.workers), Some(7));
        assert_eq!(s.per_type.get("back\\slash").map(|j| j.workers), Some(11));
        assert_eq!(s.per_type.len(), 5, "no truncated/merged job types");
    }

    /// A malformed series (unterminated quote) is skipped, never misparsed into
    /// a neighbouring job type.
    #[test]
    fn malformed_label_series_is_skipped_not_misparsed() {
        let text = "nanobpm_job_type_workers{job_type=\"unterminated} 9\n\
                    nanobpm_job_type_workers{job_type=\"ok\"} 4\n";
        let s = parse_snapshot(text, 1000);
        assert_eq!(s.per_type.len(), 1);
        assert_eq!(s.per_type.get("ok").map(|j| j.workers), Some(4));
    }

    #[test]
    fn scalar_parse_does_not_prefix_match() {
        // `pending_create_queue` must not be picked up by a query for a shorter name.
        let text = "nanobpm_pending_create_queue 7\nnanobpm_pending_create_queue_other 99\n";
        assert_eq!(
            parse_scalar(text, "nanobpm_pending_create_queue"),
            Some(7.0)
        );
    }

    #[test]
    fn hard_starvation_is_flagged_high_confidence() {
        let prev = snap(0, &[("pay", 30, 0, 0)]);
        let cur = snap(1000, &[("pay", 40, 0, 0)]);
        let a = advise(&prev, &cur);
        let r = rec(&a, "pay");
        assert_eq!(r.class, Class::Starved);
        assert_eq!(r.confidence, Confidence::High);
        assert!(r.suggest_worker_delta >= 1);
    }

    #[test]
    fn starvation_under_server_ceiling_is_medium_confidence() {
        let prev = snap(0, &[("pay", 30, 0, 0)]);
        let mut cur = snap(1000, &[("pay", 40, 0, 0)]);
        cur.ceiling_throughput = true;
        let a = advise(&prev, &cur);
        let r = rec(&a, "pay");
        assert_eq!(r.class, Class::Starved);
        assert_eq!(r.confidence, Confidence::Medium);
        assert!(r.suggest_worker_delta >= 1);
    }

    #[test]
    fn under_provisioned_sizes_workers_by_littles_law() {
        // 4 workers drained 200 jobs in 1s (200/s ⇒ 50/worker); backlog grew 100/s.
        // Need ⌈100 / 50⌉ = 2 more workers.
        let prev = snap(0, &[("enrich", 100, 4, 1000)]);
        let cur = snap(1000, &[("enrich", 200, 4, 1200)]);
        let a = advise(&prev, &cur);
        assert!(!a.server_bound);
        let r = rec(&a, "enrich");
        assert_eq!(r.class, Class::UnderProvisioned);
        assert_eq!(r.confidence, Confidence::High);
        assert_eq!(r.suggest_worker_delta, 2);
        assert_eq!(r.drain_per_s, 200.0);
    }

    #[test]
    fn server_bound_refuses_to_scale_workers() {
        let prev = snap(0, &[("enrich", 100, 4, 1000)]);
        let mut cur = snap(1000, &[("enrich", 300, 4, 1050)]);
        cur.ceiling_throughput = true; // throughput LED lit
        let a = advise(&prev, &cur);
        assert!(a.server_bound);
        let r = rec(&a, "enrich");
        assert_eq!(r.class, Class::ServerBound);
        assert_eq!(r.suggest_worker_delta, 0);
    }

    #[test]
    fn writer_saturation_alone_marks_server_bound() {
        let mut prev = snap(0, &[("enrich", 100, 4, 1000)]);
        prev.writer_busy_seconds = 0.0;
        prev.writer_idle_seconds = 0.0;
        let mut cur = snap(1000, &[("enrich", 300, 4, 1200)]);
        cur.writer_busy_seconds = 0.95; // 95% of the 1s window busy
        cur.writer_idle_seconds = 0.05;
        let a = advise(&prev, &cur);
        assert!(a.server_bound);
        assert_eq!(rec(&a, "enrich").class, Class::ServerBound);
    }

    #[test]
    fn adequate_when_backlog_stable() {
        let prev = snap(0, &[("ok", 10, 3, 1000)]);
        let cur = snap(1000, &[("ok", 8, 3, 1200)]);
        let a = advise(&prev, &cur);
        let r = rec(&a, "ok");
        assert_eq!(r.class, Class::Adequate);
        assert_eq!(r.suggest_worker_delta, 0);
    }

    #[test]
    fn first_sample_is_warming_not_a_false_call() {
        let cur = snap(1000, &[("x", 200, 3, 500)]);
        let a = advise(&Snapshot::default(), &cur);
        // Empty prev ⇒ no window; growing backlog but workers present ⇒ warming.
        assert_eq!(rec(&a, "x").class, Class::Warming);
    }

    #[test]
    fn starved_sorts_ahead_of_under_provisioned() {
        let prev = snap(0, &[("starved", 10, 0, 0), ("under", 100, 2, 1000)]);
        let cur = snap(1000, &[("starved", 20, 0, 0), ("under", 200, 2, 1100)]);
        let a = advise(&prev, &cur);
        assert_eq!(a.recommendations[0].job_type, "starved");
    }
}
