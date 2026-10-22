//! Backpressure for `createProcessInstance`: shed load with `503
//! RESOURCE_EXHAUSTED` when the engine is saturated, so a producer that outpaces
//! the workers converges to the drain rate instead of growing an unbounded
//! create backlog (which inflates hot state and collapses per-command latency).
//!
//! Three modes, selected by `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT`:
//! - **Adaptive** (default; unset / `auto` / `adaptive`): an AIMD concurrency
//!   limiter whose watermark self-tunes from the engine's measured per-command
//!   latency — faithful to Camunda/Zeebe's default request limiter, but the
//!   congestion threshold is derived from a *measured* baseline latency rather
//!   than a hard-coded timeout, so it adapts to the host with no magic constant.
//! - **Fixed(n)** (a positive integer): a static in-flight-instance watermark.
//! - **Disabled** (`0` / `off` / `none` / `false` / `disabled`): never shed.
//!
//! In every mode the quantity compared against the limit is the count of
//! in-flight (Active, non-terminal) process instances, tracked by the read-model
//! exporter in a lock-free [`AtomicUsize`]. Adaptive mode does not change *what*
//! is limited (instances) — it makes the *limit itself* track latency.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Live-published internal state of the [`BacklogGovernor`], so the ~1 Hz monitor
/// can surface *why* the compressor is holding its cap where it is (and a shed
/// message can explain it), and so ρ + backlog-trend are available as a monitoring
/// surface in **both** SLA modes (the cap only actuates in latency mode, but the
/// signals are always published). The governor owns the write side (updated each
/// monitor tick); the server keeps a clone of the read side. All zero until the
/// first tick folds.
#[derive(Clone, Default)]
pub struct GovernorObs {
    /// The most recent tick's **actor saturation ρ**, in per-mille (0–1000). ρ→1000
    /// means the single-writer engine actor is the wall (CPU-bound apply or
    /// disk-bound fsync). This is the compressor's primary "are we the bottleneck"
    /// signal — absolute, cold-start-proof, immune to external/worker strain.
    pub rho_permille: Arc<AtomicU64>,
    /// The ρ setpoint, in per-mille: at/above this (plus the deadband) *and* while
    /// the backlog is rising, the compressor attacks the cap.
    pub rho_target_permille: Arc<AtomicU64>,
    /// The most recent tick's runnable-backlog growth rate (jobs/second, signed):
    /// `> 0` = falling behind (intake outrunning drain), `< 0` = draining. Paired
    /// with ρ it separates "we're the bottleneck" (ρ high **and** rising) from a
    /// healthy peak (ρ high, stable) or external strain (ρ low, rising).
    pub growth_per_s: Arc<AtomicI64>,
}

/// Parsed configuration for the backpressure subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureSetting {
    /// AIMD limiter that sizes the in-flight watermark from measured latency.
    Adaptive,
    /// Static in-flight-instance watermark of `n`.
    Fixed(usize),
    /// Load shedding off; creates are never rejected.
    Disabled,
}

/// Pure resolver for `NANOBPMN_BACKPRESSURE_MAX_INFLIGHT` (split out so it is
/// unit-testable without the process environment). Backpressure is **on by
/// default**, and **adaptive** by default:
/// - `None` (unset), `auto`, `adaptive` → [`BackpressureSetting::Adaptive`];
/// - `0`, `off`, `none`, `false`, `disabled` (any case) → [`BackpressureSetting::Disabled`];
/// - a positive integer → [`BackpressureSetting::Fixed`];
/// - anything else unparseable → `Adaptive` (fail safe: never silently disable).
pub fn parse_backpressure_setting(raw: Option<&str>) -> BackpressureSetting {
    let Some(raw) = raw else {
        return BackpressureSetting::Adaptive;
    };
    let v = raw.trim().to_ascii_lowercase();
    match v.as_str() {
        "" | "auto" | "adaptive" => BackpressureSetting::Adaptive,
        "0" | "off" | "none" | "false" | "disabled" => BackpressureSetting::Disabled,
        _ => match v.parse::<usize>() {
            Ok(n) if n > 0 => BackpressureSetting::Fixed(n),
            _ => BackpressureSetting::Adaptive,
        },
    }
}

/// Which service-level objective the operator wants the engine to honour when it
/// reaches its saturation ceiling (memory / processing capacity). This selects
/// the *behavioural characteristic* at the edge of the performance envelope; it
/// does **not** relax the memory-safety rails (create-queue, in-flight payload,
/// resident-memory watermarks) that exist to keep the node from OOMing — those
/// are survival guards active in every mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SlaMode {
    /// **Preserve end-to-end latency** (default). At the ceiling, shed new
    /// `createProcessInstance` calls (`503 RESOURCE_EXHAUSTED`) so accepted
    /// instances keep completing fast. This is the "time-to-complete SLA": a
    /// client that outpaces the drain rate is told to back off rather than
    /// letting its already-running instances slow down. Enforced by the AIMD
    /// concurrency limiter *and* the proactive active-backlog governor.
    Latency,
    /// **Preserve admission** (accept latency). At the ceiling, drop the proactive
    /// active-backlog governor and run the engine at its true drain ceiling
    /// (measured ~+48% throughput vs `Latency`), letting end-to-end latency and the
    /// backlog grow with demand. This is the "start-every-process SLA": only the
    /// proactive **active-backlog governor** is suppressed. The **AIMD concurrency
    /// limiter stays armed** (see [`Self::sheds_for_latency`]) as an engine-overload
    /// guard — arming it measurably tightens the tail (~40% lower p90) at no
    /// throughput cost — but note it does **not** bound the accumulated backlog: it
    /// sheds on create-*processing* concurrency, which stays low even while the
    /// completion side falls behind, so under sustained overload the backlog grows
    /// until the **memory-safety rails** shed (they, not AIMD, are the backstop in
    /// this mode). Only `Latency`'s backlog governor gives a tight, engine-enforced
    /// bound. See ADR 0013 Addendum / `PERFORMANCE.md` 2026-07-10. Terminal state
    /// frees on completion (ADR 0012) and live variables spill to disk, so the large
    /// backlog is comparatively cheap to hold up to the rail.
    Admission,
}

impl SlaMode {
    /// Whether the **proactive active-backlog governor** should reject admission
    /// to hold a latency target. `true` in [`SlaMode::Latency`], `false` in
    /// [`SlaMode::Admission`] (which drops the proactive governor to admit more).
    /// Note this gates **only** the backlog governor: the AIMD concurrency limiter
    /// (an engine-overload guard) and the memory-safety rails stay armed in both
    /// modes. AIMD is not a backlog bound — under sustained overload only the
    /// governor (i.e. `Latency` mode) keeps the backlog tightly bounded; in
    /// `Admission` the backlog grows to the memory-safety rails.
    pub fn sheds_for_latency(&self) -> bool {
        matches!(self, SlaMode::Latency)
    }

    /// Stable machine identifier (`latency` | `admission`) for APIs/UI.
    pub fn as_str(&self) -> &'static str {
        match self {
            SlaMode::Latency => "latency",
            SlaMode::Admission => "admission",
        }
    }

    /// Human-readable description for the startup log.
    pub fn describe(&self) -> &'static str {
        match self {
            SlaMode::Latency => {
                "latency (preserve end-to-end latency; shed admission at the ceiling)"
            }
            SlaMode::Admission => {
                "admission (preserve admission; accept higher latency at the ceiling)"
            }
        }
    }

    /// Reconstruct from the atomic byte used by [`SharedSlaMode`]. Fails safe to
    /// [`SlaMode::Latency`] for any unexpected value (never silently drop latency
    /// protection).
    fn from_u8(v: u8) -> Self {
        match v {
            1 => SlaMode::Admission,
            _ => SlaMode::Latency,
        }
    }
}

/// A runtime-switchable SLA mode: a cheap, cloneable handle over an atomic so an
/// operator toggle (e.g. the console SLA knob) is visible to every in-flight
/// request handler **without a restart**. Mirrors the [`Backpressure::Adaptive`]
/// watermark, which is likewise an `Arc<Atomic…>` written by one place and read
/// lock-free on the hot path. The stored value is [`SlaMode`] encoded via
/// `#[repr(u8)]`.
#[derive(Clone)]
pub struct SharedSlaMode(Arc<AtomicU8>);

impl SharedSlaMode {
    /// Seed with the startup mode (resolved from `NANOBPMN_SLA_MODE`).
    pub fn new(mode: SlaMode) -> Self {
        Self(Arc::new(AtomicU8::new(mode as u8)))
    }

    /// The current mode. A relaxed atomic load — no coordination, safe on the
    /// admission hot path.
    pub fn get(&self) -> SlaMode {
        SlaMode::from_u8(self.0.load(Ordering::Relaxed))
    }

    /// Switch the mode at runtime. Takes effect on the next admission decision;
    /// a small race against concurrent creates is irrelevant for an approximate
    /// gate.
    pub fn set(&self, mode: SlaMode) {
        self.0.store(mode as u8, Ordering::Relaxed);
    }
}

/// Pure resolver for `NANOBPMN_SLA_MODE` (split out so it is unit-testable without
/// the process environment). Defaults to [`SlaMode::Latency`] — the historical
/// behaviour — and fails safe to it for any unrecognised value (never silently
/// drop latency protection):
/// - `None` (unset), `latency`, `preserve-latency`, `time-to-complete` → `Latency`;
/// - `admission`, `preserve-admission`, `accept-latency`, `start-every-process`,
///   `throughput` → `Admission`;
/// - anything else → `Latency`.
pub fn parse_sla_mode(raw: Option<&str>) -> SlaMode {
    let Some(raw) = raw else {
        return SlaMode::Latency;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "latency" | "preserve-latency" | "time-to-complete" | "complete" => SlaMode::Latency,
        "admission"
        | "preserve-admission"
        | "accept-latency"
        | "start-every-process"
        | "start"
        | "throughput" => SlaMode::Admission,
        _ => SlaMode::Latency,
    }
}

/// Runtime backpressure handle held by the server and queried by the create
/// handler. Cheap to clone; the adaptive limit lives behind an `Arc<AtomicUsize>`
/// the engine thread writes and request handlers read.
#[derive(Clone)]
pub enum Backpressure {
    Disabled,
    Fixed(usize),
    /// The current watermark, maintained by the engine thread's [`AdaptiveController`].
    Adaptive(Arc<AtomicUsize>),
}

impl Backpressure {
    /// The watermark to compare the in-flight gauge against, or `None` when load
    /// shedding is disabled (the create handler then never rejects).
    pub fn current_limit(&self) -> Option<usize> {
        match self {
            Backpressure::Disabled => None,
            Backpressure::Fixed(n) => Some(*n),
            Backpressure::Adaptive(limit) => Some(limit.load(Ordering::Relaxed)),
        }
    }

    /// Whether a create carrying the engine's current request-processing
    /// concurrency (`in_flight`) should be shed. `false` whenever load shedding is
    /// disabled. The comparison is `>=` so a watermark of `n` admits at most `n`
    /// concurrent creates.
    pub fn should_shed(&self, in_flight: usize) -> bool {
        self.current_limit().is_some_and(|limit| in_flight >= limit)
    }

    /// Human-readable description for the startup log.
    pub fn describe(&self) -> String {
        match self {
            Backpressure::Disabled => "disabled".to_string(),
            Backpressure::Fixed(n) => {
                format!("fixed watermark of {n} concurrent creates in processing")
            }
            Backpressure::Adaptive(limit) => format!(
                "adaptive (AIMD, latency-driven), starting watermark {}",
                limit.load(Ordering::Relaxed)
            ),
        }
    }
}

// --- AIMD controller tuning ------------------------------------------------

/// Lowest watermark the adaptive limiter will shrink to. Floors load shedding so
/// a latency spike can't starve the workers; the empirical create-flood knee
/// (~250 in-flight saturates 10 workers) sits right here.
const MIN_LIMIT: usize = 256;
/// Highest watermark the limiter will grow to — a hard memory rail (~1 GB at the
/// 50 KB worst-case payload; negligible for typical small payloads). The
/// controller normally settles well below this once latency rises.
const MAX_LIMIT: usize = 20_000;
/// Starting watermark before any latency has been observed.
const INITIAL_LIMIT: usize = MIN_LIMIT;
/// A window's average latency above `baseline * CONGESTION_RATIO` is treated as
/// queueing (congestion) and triggers a multiplicative decrease.
pub const CONGESTION_RATIO: f64 = 2.0;
/// Multiplicative-decrease factor applied on congestion (AIMD's "MD").
const BACKOFF: f64 = 0.9;
/// Per-window upward drift of the latency baseline when no faster sample is seen,
/// so a permanently raised floor (e.g. larger payloads) doesn't over-throttle.
const BASELINE_CREEP: f64 = 0.05;
/// Minimum samples before a window is evaluated (avoids reacting to noise).
const WINDOW_MIN_SAMPLES: u64 = 64;
/// Minimum wall time before a window is evaluated.
const WINDOW_MIN_INTERVAL: Duration = Duration::from_millis(20);

/// AIMD limit calculator with a self-calibrated latency baseline. Pure and
/// `Instant`-free so its dynamics are unit-testable; the windowing/IO lives in
/// [`AdaptiveController`].
#[derive(Debug, Clone)]
pub struct AimdLimit {
    limit: f64,
    min_limit: usize,
    max_limit: usize,
    baseline_us: f64,
    /// Slow-start probes the ceiling fast (multiplicative growth) until the first
    /// congestion signal, then switches to additive-increase probing.
    slow_start: bool,
}

impl AimdLimit {
    pub fn new(initial: usize, min_limit: usize, max_limit: usize) -> Self {
        Self {
            limit: initial.clamp(min_limit, max_limit) as f64,
            min_limit,
            max_limit,
            baseline_us: 0.0,
            slow_start: true,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit as usize
    }

    /// The current latency reference (µs) the congestion threshold is derived
    /// from: the self-calibrated baseline in ratio mode, or the fixed target in
    /// The current self-calibrated latency reference (µs) the congestion threshold
    /// is derived from (`baseline × `[`CONGESTION_RATIO`]).
    pub fn baseline_us(&self) -> f64 {
        self.baseline_us
    }

    /// The latency (µs) at/above which a window is treated as congested:
    /// `baseline × `[`CONGESTION_RATIO`].
    pub fn congestion_threshold_us(&self) -> f64 {
        self.baseline_us * CONGESTION_RATIO
    }

    /// Fold one window of stats into the limit and return the new value.
    /// `avg_us` is the window's mean per-command latency; `inflight` is the
    /// current in-flight count (the limit only grows while it is being used, so
    /// an idle engine doesn't inflate the watermark).
    ///
    /// Congestion is judged against a *baseline of the lowest average seen* ×
    /// [`CONGESTION_RATIO`] — like-for-like, since the command mix (cheap
    /// completes vs heavy 50 KB creates) makes a single-command minimum a poor
    /// reference. When the average inflates past the threshold the engine is
    /// queueing / under heap pressure, so the limit is cut.
    pub fn on_window(&mut self, avg_us: f64, inflight: usize) -> usize {
        let loaded = (inflight as f64) * 2.0 >= self.limit;
        // Establish the baseline on the first ever window.
        if self.baseline_us == 0.0 {
            self.baseline_us = avg_us;
        }
        let congested = avg_us > self.congestion_threshold_us();

        if congested {
            // Congested: multiplicative decrease, and leave slow-start for good.
            // The baseline is *frozen* here — never adapt it toward a congested
            // latency, or sustained queueing would be accepted as the new normal
            // and shedding would stop (the exact unbounded-backlog pathology we
            // exist to prevent).
            self.slow_start = false;
            self.limit = (self.limit * BACKOFF).max(self.min_limit as f64);
        } else {
            // Healthy window. Refine the baseline toward the uncongested floor
            // (snap down to a new minimum, else creep up slowly so a genuinely
            // slower host isn't throttled forever). Then probe the limit upward
            // while it is used.
            if avg_us < self.baseline_us {
                self.baseline_us = avg_us;
            } else {
                self.baseline_us += (avg_us - self.baseline_us) * BASELINE_CREEP;
            }
            if loaded {
                if self.slow_start {
                    self.limit = (self.limit * 2.0).min(self.max_limit as f64);
                } else {
                    self.limit = (self.limit + 1.0).min(self.max_limit as f64);
                }
            }
        }
        self.limit()
    }
}

/// One AIMD limiter publishing to a shared atomic, stepped once per latency
/// window by the [`AdaptiveController`]. `signal` is the live quantity whose
/// pressure gates *growth* (the limit only grows while it is being approached, so
/// an idle engine doesn't inflate the watermark); `shared` is where the resolved
/// limit is published for the hot path to read.
struct LatencyLimiter {
    aimd: AimdLimit,
    shared: Arc<AtomicUsize>,
    signal: Arc<AtomicUsize>,
    /// Stable tag for the verbose convergence log (`adaptive`, `worker-governor`).
    label: &'static str,
}

impl LatencyLimiter {
    /// Fold one window average into the limit and publish it.
    fn step(&mut self, avg_us: f64, verbose: bool) {
        let signal = self.signal.load(Ordering::Relaxed);
        let prev = self.aimd.limit();
        let new_limit = self.aimd.on_window(avg_us, signal);
        self.shared.store(new_limit, Ordering::Relaxed);
        if verbose && new_limit != prev {
            tracing::info!(
                "backpressure({}): limit {prev} -> {new_limit} (avg {avg_us:.0}us, \
                 baseline {:.0}us, signal {signal})",
                self.label,
                self.aimd.baseline_us(),
            );
        }
    }
}

/// One command-class latency window (creates vs completion-side). Accumulates
/// per-command latencies of a single class and yields the window average once it
/// has enough samples and has run long enough — keeping each class's baseline
/// like-for-like. Windowing per class (rather than one mixed window) is what
/// stops a cheap-completion window from pinning the create-side baseline near
/// zero, which made `threshold = baseline * CONGESTION_RATIO` fire on every
/// normal window and pinned the governor at its floor.
struct ClassWindow {
    count: u64,
    sum_us: f64,
    start: Instant,
}

impl ClassWindow {
    fn new() -> Self {
        Self {
            count: 0,
            sum_us: 0.0,
            start: Instant::now(),
        }
    }

    /// Fold one same-class sample in; return the window average (and reset) once
    /// the window has enough samples and has run long enough, else `None`.
    fn offer(&mut self, us: f64) -> Option<f64> {
        self.count += 1;
        self.sum_us += us;
        if self.count >= WINDOW_MIN_SAMPLES && self.start.elapsed() >= WINDOW_MIN_INTERVAL {
            let avg = self.sum_us / self.count as f64;
            self.count = 0;
            self.sum_us = 0.0;
            self.start = Instant::now();
            Some(avg)
        } else {
            None
        }
    }
}

/// Engine-thread side of the adaptive limiters: accumulates per-command latency
/// into **per-class** windows and, once a class window has enough samples and has
/// run long enough, steps the limiter that keys off that class from its window
/// average. Owned by the single (partition-0) engine thread, so its window state
/// needs no synchronization. The command classes are creates (`Priority::Low`)
/// and completion-side/reads (`Priority::High`); splitting the window keeps each
/// class's latency baseline like-for-like:
/// - the **create limiter** (present in adaptive backpressure mode) sizes the
///   create-processing concurrency watermark, gated on the `processing` gauge,
///   and steps off the **create** window;
/// - the **worker governor** steps off the **completion** window — the drain-side
///   knee.
///
/// The admission-backlog cap is *not* driven here: it is owned by the standalone
/// monitor-stepped [`BacklogGovernor`] compressor, which keys off **engine-actor
/// saturation ρ** + backlog trend (the monitor aggregates ρ from the actor stats) —
/// "are we falling behind AND is it our fault" — deliberately *not* end-to-end
/// sojourn (which is dominated by external/worker service time and would
/// mis-throttle on a downstream outage).
pub struct AdaptiveController {
    create: Option<LatencyLimiter>,
    workers: Option<LatencyLimiter>,
    /// Create-class (`Priority::Low`) latency window — drives the create limiter.
    lo: ClassWindow,
    /// Completion-class (`Priority::High`) latency window — drives the workers.
    hi: ClassWindow,
    /// Log every limit change at INFO (gated by `NANOBPM_ACTOR_PROFILE`) so the
    /// limiters' convergence is observable during tuning.
    verbose: bool,
}

impl Default for AdaptiveController {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveController {
    /// An empty controller with no limiters installed. Install limiters with
    /// [`with_create_limiter`](Self::with_create_limiter) and/or
    /// [`with_worker_governor`](Self::with_worker_governor); check
    /// [`is_active`](Self::is_active) to decide whether it needs driving at all.
    pub fn new() -> Self {
        Self {
            create: None,
            workers: None,
            lo: ClassWindow::new(),
            hi: ClassWindow::new(),
            verbose: std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some(),
        }
    }

    /// Install the create-processing concurrency limiter (the historical adaptive
    /// backpressure watermark). Growth is gated on the `inflight` processing
    /// gauge. Returns the shared limit handle to install in
    /// [`Backpressure::Adaptive`].
    pub fn with_create_limiter(&mut self, inflight: Arc<AtomicUsize>) -> Arc<AtomicUsize> {
        let shared = Arc::new(AtomicUsize::new(INITIAL_LIMIT));
        self.create = Some(LatencyLimiter {
            aimd: AimdLimit::new(INITIAL_LIMIT, MIN_LIMIT, MAX_LIMIT),
            shared: shared.clone(),
            signal: inflight,
            label: "adaptive",
        });
        shared
    }

    /// Install the self-optimizing worker-concurrency governor: it tunes the
    /// number of subscribers the push dispatcher fans a job type out to per pass
    /// between `floor` and `ceiling`, from the same per-command latency signal,
    /// gated on the current `backlog` (runnable task-jobs waiting to drain). The
    /// push dispatcher is the throughput ceiling — activation is High-priority in
    /// the engine mailbox, so fanning a fixed job supply across too many
    /// subscribers swamps completions and inflates per-command latency. Starting
    /// at `floor`, the governor slow-starts the active-subscriber width upward
    /// while there is backlog to drain and latency is healthy, and backs off
    /// multiplicatively the moment latency inflates past its self-calibrated
    /// baseline — converging on the worker concurrency that maximizes completion
    /// throughput. Excess subscribers are parked (rotated round-robin, never
    /// starved). Returns the shared width handle the dispatcher reads.
    pub fn with_worker_governor(
        &mut self,
        floor: usize,
        ceiling: usize,
        backlog: Arc<AtomicUsize>,
    ) -> Arc<AtomicUsize> {
        let shared = Arc::new(AtomicUsize::new(floor));
        self.workers = Some(LatencyLimiter {
            aimd: AimdLimit::new(floor, floor, ceiling),
            shared: shared.clone(),
            signal: backlog,
            label: "worker-governor",
        });
        shared
    }
    pub fn is_active(&self) -> bool {
        self.create.is_some() || self.workers.is_some()
    }

    /// Record one command's processing latency, tagged with its class
    /// (`is_create` = the create-side `Priority::Low` queue). Folds the sample
    /// into that class's window and steps the limiter keyed off it once the
    /// window is ready: the create limiter off the **create** window, the worker
    /// governor off the **completion** window. Splitting the windows keeps each
    /// limiter's AIMD baseline like-for-like.
    pub fn record(&mut self, latency: Duration, is_create: bool) {
        let us = latency.as_micros() as f64;
        if is_create {
            if let Some(avg) = self.lo.offer(us)
                && let Some(c) = self.create.as_mut()
            {
                c.step(avg, self.verbose);
            }
            return;
        }
        if let Some(avg) = self.hi.offer(us)
            && let Some(w) = self.workers.as_mut()
        {
            w.step(avg, self.verbose);
        }
    }
}

/// The standalone admission-backlog **compressor**, stepped once per ~1 Hz monitor
/// tick (not on the engine actor loop). It bounds the admission-backlog cap by
/// reacting to **engine-actor saturation ρ** (the busy fraction of the single-writer
/// engine actor) paired with the **runnable-backlog growth rate** — the two signals
/// that together answer the operator's real question: *are we falling behind
/// (backlog rising) AND is it our fault (the engine actor is saturated)?*
///
/// ## Why ρ + backlog-trend, not latency-over-baseline
/// A latency-over-learned-baseline compressor (the prior design) needs a
/// *healthy-busy* reference it cannot learn from a cold start or a busy-storm
/// wake-up, and its baseline pinned to the idle floor, clamping perpetually. ρ has
/// none of those problems:
/// - **absolute** — ρ ∈ [0,1] needs no learning and no magic latency number, so it
///   is correct on the very first window (cold-start-proof);
/// - **captures disk *and* CPU** — the fsync is inside the timed `job(journal)`
///   region, so a disk-bound actor reads ρ≈1 just like a CPU-bound one;
/// - **immune to external strain** — a slow downstream/worker makes the actor *park*
///   waiting for work (ρ low), so it never mistakes someone else's outage for our
///   own saturation and never throttles admission for it.
///
/// ρ alone cannot tell a healthy peak (a work-conserving server saturates at ρ≈1
/// serving a load it *is* keeping up with) from genuine overload, so it is paired
/// with backlog growth: **we are the bottleneck only when ρ is high AND the backlog
/// is rising.** (Sojourn is retained purely as a per-job-type *reporting* surface.)
///
/// ## Compressor control law (three-zone, crisp + one proportional touch)
/// Each tick, given actor saturation `rho` ∈ [0,1] and runnable-backlog growth
/// `growth_per_s` (jobs/s, signed):
/// - **attack** — `rho > target + deadband` **and** `growth_per_s > 0` (saturated
///   *and* falling behind): cut the cap multiplicatively toward the floor. The
///   attack step is the **only proportional term**: it steepens as the backlog rises
///   faster (`f = attack^(1 + attack_gain·g)`, `g = clamp(growth/cap, 0, 1)`), so a
///   cold-start storm slams the cap to the floor in a few ticks while a mild
///   overshoot is nudged gently.
/// - **release** — `growth_per_s < 0` (draining) **or** `rho < target − deadband`
///   (idle headroom): grow the cap slowly toward the ceiling (`cap · release`).
///   Release is deliberately **slow and un-proportional** — this asymmetry is the
///   anti-oscillation guarantee (the compressor is the *outer* loop of a cascade
///   over the drain-guard servo; it must move slower than the inner loop or they
///   fight, which is what caused the historical sawtooth).
/// - **hold** — otherwise (healthy saturated peak: ρ high but backlog stable; or the
///   ambiguous mid-zone). Holding at a healthy peak is exactly the compressor knee.
///
/// Only the dimensionless `rho_target` matters as a tuning knob, and it is portable
/// across hardware — there is no absolute latency number to calibrate per box.
#[derive(Clone)]
pub struct BacklogGovernor {
    /// Published admission-backlog cap the gate reads — and the compressor's own
    /// state (`step` loads the current cap, folds one tick, stores the new cap).
    cap: Arc<AtomicUsize>,
    floor: usize,
    ceiling: usize,
    /// Actor-saturation setpoint ρ* ∈ (0,1): attack fires only above `target +
    /// deadband` (while the backlog is rising).
    rho_target: f64,
    /// Half-width of the ρ dead-band around the setpoint (the hold zone).
    deadband: f64,
    /// Base attack coefficient (< 1): the per-tick multiplicative clamp-down when
    /// saturated-and-rising, before the growth-rate steepening is applied.
    attack: f64,
    /// Release coefficient (> 1): the slow multiplicative relax toward the ceiling.
    release: f64,
    /// Proportional gain on the attack exponent: how much faster backlog growth
    /// steepens the clamp (`f = attack^(1 + attack_gain·g)`).
    attack_gain: f64,
    obs: GovernorObs,
    verbose: bool,
}

impl BacklogGovernor {
    /// Build a compressor bounded by `[floor, ceiling]` that attacks when actor
    /// saturation exceeds `rho_target + deadband` while the backlog is rising
    /// (clamping with `attack` < 1, steepened by `attack_gain` on the growth rate)
    /// and releases slowly with `release` (> 1) when draining or idle. Returns the
    /// governor plus the shared cap handle the admission gate reads (seeded at the
    /// ceiling — inert until the compressor clamps) and a [`GovernorObs`] read handle
    /// the monitor surfaces to explain the live cap.
    pub fn new(
        floor: usize,
        ceiling: usize,
        rho_target: f64,
        deadband: f64,
        attack: f64,
        release: f64,
        attack_gain: f64,
    ) -> (Self, Arc<AtomicUsize>, GovernorObs) {
        let cap = Arc::new(AtomicUsize::new(ceiling));
        let obs = GovernorObs::default();
        obs.rho_target_permille
            .store((rho_target * 1000.0) as u64, Ordering::Relaxed);
        let gov = Self {
            cap: cap.clone(),
            floor,
            ceiling,
            rho_target,
            deadband,
            attack,
            release,
            attack_gain,
            obs: obs.clone(),
            verbose: std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some(),
        };
        (gov, cap, obs)
    }

    pub fn floor(&self) -> usize {
        self.floor
    }

    pub fn ceiling(&self) -> usize {
        self.ceiling
    }

    pub fn obs(&self) -> &GovernorObs {
        &self.obs
    }

    /// Fold one monitor tick's actor saturation `rho` ∈ [0,1] and runnable-backlog
    /// growth `growth_per_s` (jobs/s, signed) into the cap, publish the observability,
    /// and return the new cap. Call once per monitor tick in **both** SLA modes (the
    /// signals stay populated for monitoring); the cap only *actuates* admission in
    /// latency mode, but the governor itself is mode-agnostic.
    pub fn step(&self, rho: f64, growth_per_s: f64) -> usize {
        let prev = self.cap.load(Ordering::Relaxed);
        let mut limit = prev as f64;
        let sat_hi = rho > self.rho_target + self.deadband;
        let sat_lo = rho < self.rho_target - self.deadband;
        let rising = growth_per_s > 0.0;
        let draining = growth_per_s < 0.0;
        if sat_hi && rising {
            // Saturated AND falling behind → attack. The step steepens with the
            // backlog growth rate (the only signal with dynamic range, since ρ
            // saturates at 1): a cold-start storm slams to the floor fast; a mild
            // overshoot is nudged gently.
            let g = (growth_per_s / (prev.max(1) as f64)).clamp(0.0, 1.0);
            let f = self.attack.powf(1.0 + self.attack_gain * g);
            limit = (limit * f).max(self.floor as f64);
        } else if draining || sat_lo {
            // Draining, or idle headroom → release slowly toward the ceiling.
            limit = (limit * self.release).min(self.ceiling as f64);
        }
        // else: hold (healthy saturated peak, or ambiguous mid-zone).
        let new = limit as usize;
        self.cap.store(new, Ordering::Relaxed);
        self.obs
            .rho_permille
            .store((rho * 1000.0) as u64, Ordering::Relaxed);
        self.obs
            .growth_per_s
            .store(growth_per_s as i64, Ordering::Relaxed);
        if self.verbose && new != prev {
            tracing::info!(
                "backlog-compressor: cap {prev} -> {new} (rho {:.3}, growth {growth_per_s:.0}/s, \
                 target {:.3})",
                rho,
                self.rho_target,
            );
        }
        new
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ADR-0020 Tier-2: per-process-definition admission compressors.
//
// Tier-1 (the ρ / raft-fsync guard above) protects OUR shared write path — a
// global "are we the bottleneck" signal. Tier-2 protects each USER WORKLOAD's
// end-to-end latency independently: it keys on the in-flight instance backlog
// L_P of each BPMN process definition P (the authoritative engine counter added
// in ADR-0020), and by Little's law W_P = L_P / λ_P bounding L_P bounds the
// definition's e2e instance sojourn. Because the detection unit *is* the
// actuation unit (the definition), a definition that accumulates backlog is
// throttled without touching a healthy sibling that merely shares a congested
// job type — strictly more precise than per-job-type detection with per-process
// fan-in.
//
// The controller is an audio-compressor-shaped delay-gradient law on each
// definition's own pressure p_P ∈ [0,1]: a fast ATTACK term drives the backlog
// growth dL_P/dt → 0 (scale-free — it needs no absolute setpoint), and a slow
// RELEASE term biases the operating point toward the Little's-law band
// L*_P = W_target · λ_P (the one portable knob: the target e2e sojourn). The
// deadband is expressed as a fraction of λ_P so the same tuning holds across
// definitions of wildly different throughput.
//
// Actuation: the ~1 Hz monitor `step`s every definition from the cross-partition
// backlog snapshot and publishes each p_P as per-mille. Admission reads p_P for
// the target definition and sheds a p_P fraction of that definition's creates
// via a per-definition accumulator (deterministic, even, cheap) — so a partially
// pressured definition is *paced*, not bang-banged on the tick boundary.

/// Parsed configuration for the [`ProcessGovernors`] Tier-2 compressors. All
/// fields are env-tunable (see [`ProcGovConfig::from_env`]) but default to a
/// conservative, largely-inert operating point so healthy workloads are never
/// throttled — the growth term does the real work and only a definition whose
/// in-flight backlog is both *above its Little's-law band and still rising* is
/// squeezed.
#[derive(Clone, Copy, Debug)]
pub struct ProcGovConfig {
    /// `W_target`: the target end-to-end instance sojourn (seconds). The band is
    /// `L*_P = W_target · λ_P`; a definition is only a candidate for attack once
    /// its in-flight backlog exceeds this. Generous by default — the scale-free
    /// growth term catches runaway accumulation long before the absolute band.
    pub w_target_s: f64,
    /// Per-tick pressure rise (fast attack) when a definition is above-band and
    /// rising, before the growth-rate steepening.
    pub attack: f64,
    /// Proportional gain on the attack: steepens the rise with the (normalised)
    /// backlog growth rate, so a runaway definition clamps in a few ticks while a
    /// mild overshoot is nudged gently.
    pub attack_gain: f64,
    /// Per-tick pressure fall (slow release) when a definition is draining or
    /// below its band. The attack/release asymmetry is the anti-oscillation term.
    pub release: f64,
    /// Deadband half-width as a fraction of λ_P (min 1 instance): the hold zone
    /// around the band + the growth-rate significance threshold.
    pub deadband_frac: f64,
    /// EWMA weight (0–1, higher = smoother) applied to the measured backlog growth
    /// rate dL_P/dt before it feeds the attack decision.
    pub ewma: f64,
    /// Below this per-definition create rate (instances/s) the definition is
    /// treated as idle and its pressure is released — never throttle a workload
    /// that is barely creating (avoids dividing by a noise-level λ_P).
    pub min_lambda: f64,
}

impl Default for ProcGovConfig {
    fn default() -> Self {
        Self {
            w_target_s: 30.0,
            attack: 0.34,
            attack_gain: 3.0,
            release: 0.05,
            deadband_frac: 0.1,
            ewma: 0.5,
            min_lambda: 5.0,
        }
    }
}

impl ProcGovConfig {
    /// Resolve the config from the `NANOBPMN_TIER2_*` environment, falling back to
    /// [`Default`] for any unset/unparseable knob.
    pub fn from_env() -> Self {
        let d = Self::default();
        let f = |key: &str, def: f64| -> f64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|x| x.is_finite())
                .unwrap_or(def)
        };
        Self {
            w_target_s: f("NANOBPMN_TIER2_W_TARGET_MS", d.w_target_s * 1000.0) / 1000.0,
            attack: f("NANOBPMN_TIER2_ATTACK", d.attack),
            attack_gain: f("NANOBPMN_TIER2_ATTACK_GAIN", d.attack_gain),
            release: f("NANOBPMN_TIER2_RELEASE", d.release),
            deadband_frac: f("NANOBPMN_TIER2_DEADBAND_FRAC", d.deadband_frac),
            ewma: f("NANOBPMN_TIER2_EWMA", d.ewma).clamp(0.0, 1.0),
            min_lambda: f("NANOBPMN_TIER2_MIN_LAMBDA", d.min_lambda),
        }
    }
}

/// Per-definition compressor state, owned and stepped by the monitor loop only.
struct DefGov {
    prev_inflight: u64,
    prev_created: u64,
    /// EWMA of dL_P/dt (instances/s), signed.
    ewma_growth: f64,
    /// The control output p_P ∈ [0,1].
    pressure: f64,
}

/// Published, admission-readable Tier-2 pressure for one definition: the shed
/// fraction in per-mille plus a running accumulator so `should_shed` paces the
/// shed evenly across the definition's creates without an RNG.
#[derive(Default)]
struct PubPressure {
    permille: u32,
    acc: u64,
}

/// The ADR-0020 Tier-2 registry: one delay-gradient compressor per BPMN process
/// definition. The monitor calls [`ProcessGovernors::step`] once per tick with
/// the cross-partition `(process_id, in_flight L_P, cumulative_created)` snapshot;
/// admission calls [`ProcessGovernors::should_shed`] per `createProcessInstance`.
pub struct ProcessGovernors {
    cfg: ProcGovConfig,
    /// Compressor state, mutated only by the monitor `step`.
    state: std::sync::Mutex<std::collections::HashMap<String, DefGov>>,
    /// Published pressure, read (and accumulator-advanced) by admission.
    published: std::sync::Mutex<std::collections::HashMap<String, PubPressure>>,
    verbose: bool,
}

impl ProcessGovernors {
    pub fn new(cfg: ProcGovConfig) -> Self {
        Self {
            cfg,
            state: std::sync::Mutex::new(std::collections::HashMap::new()),
            published: std::sync::Mutex::new(std::collections::HashMap::new()),
            verbose: std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some(),
        }
    }

    /// Fold one monitor tick. `dt_s` is the wall time since the previous `step`;
    /// `backlog` is the aggregated per-definition `(process_id, in_flight,
    /// cumulative_created)` view (summed across the node's partitions). Updates
    /// each definition's pressure and republishes the per-mille shed fractions.
    pub fn step(&self, dt_s: f64, backlog: &[(String, u64, u64)]) {
        if dt_s <= 0.0 {
            return;
        }
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut pubm = self.published.lock().unwrap_or_else(|e| e.into_inner());
        for (pid, inflight, created) in backlog {
            let e = st.entry(pid.clone()).or_insert_with(|| DefGov {
                prev_inflight: *inflight,
                prev_created: *created,
                ewma_growth: 0.0,
                pressure: 0.0,
            });
            let lambda = (created.saturating_sub(e.prev_created) as f64 / dt_s).max(0.0);
            let growth = (*inflight as f64 - e.prev_inflight as f64) / dt_s;
            e.ewma_growth = self.cfg.ewma * e.ewma_growth + (1.0 - self.cfg.ewma) * growth;
            e.prev_inflight = *inflight;
            e.prev_created = *created;

            let l = *inflight as f64;
            if lambda < self.cfg.min_lambda {
                // Idle / barely-creating definition: never throttle; let it relax.
                e.pressure = (e.pressure - self.cfg.release).max(0.0);
            } else {
                let band = self.cfg.w_target_s * lambda; // L*_P = W_target · λ_P
                let deadband = (self.cfg.deadband_frac * lambda).max(1.0);
                let above_band = l > band + deadband;
                let rising = e.ewma_growth > deadband;
                let draining = e.ewma_growth < -deadband;
                if above_band && rising {
                    // Above the latency band AND still accumulating → attack. The
                    // step steepens with the (normalised) growth rate.
                    let g = (e.ewma_growth / l.max(1.0)).clamp(0.0, 1.0);
                    let step = self.cfg.attack * (1.0 + self.cfg.attack_gain * g);
                    e.pressure = (e.pressure + step).min(1.0);
                } else if draining || l < band {
                    // Draining, or comfortably under the band → release slowly.
                    e.pressure = (e.pressure - self.cfg.release).max(0.0);
                }
                // else: within band / holding → hold pressure.
            }

            let permille = (e.pressure * 1000.0).round().clamp(0.0, 1000.0) as u32;
            let slot = pubm.entry(pid.clone()).or_default();
            slot.permille = permille;
            if self.verbose && permille > 0 {
                tracing::info!(
                    "tier2: proc={pid} L={inflight} lambda={lambda:.0}/s growth={:.0}/s p={permille}permille",
                    e.ewma_growth,
                );
            }
        }
    }

    /// Admission hook: return `true` if this `createProcessInstance` for `pid`
    /// should be shed under the definition's current Tier-2 pressure. Sheds a
    /// `permille/1000` fraction of the definition's creates, spread evenly by a
    /// per-definition accumulator (no RNG, deterministic). Zero-pressure (the
    /// common case) is a cheap map lookup with no shed.
    pub fn should_shed(&self, pid: &str) -> bool {
        let mut pubm = self.published.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(slot) = pubm.get_mut(pid) {
            if slot.permille == 0 {
                return false;
            }
            slot.acc += slot.permille as u64;
            if slot.acc >= 1000 {
                slot.acc -= 1000;
                return true;
            }
        }
        false
    }

    /// Snapshot of the currently-pressured definitions (`process_id` → per-mille),
    /// for metrics / observability. Only definitions with non-zero pressure.
    pub fn pressures(&self) -> Vec<(String, u32)> {
        self.published
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, s)| s.permille > 0)
            .map(|(k, s)| (k.clone(), s.permille))
            .collect()
    }
}

/// Parsed configuration for the ADR-0020 **Tier-1** global engine-saturation
/// guard ([`GlobalGuard`]). All fields are env-tunable (`NANOBPMN_TIER1_*`) but
/// default to a conservative operating point: the guard is a *backstop* that only
/// engages when the engine's own shared write path (the raft-log fsync) crosses
/// its latency knee, and it compresses all intake gently (well below Tier-2's
/// per-definition attack) so it never becomes the primary actuator.
#[derive(Clone, Copy, Debug)]
pub struct Tier1Config {
    /// Master switch (`NANOBPMN_TIER1`, default **on**) for the *fsync-latency*
    /// guard. When off the fsync term never sheds; the monitor still publishes the
    /// export-queue pressure (memory protection is independent of this switch and
    /// of SLA mode — see [`Self::exporter_knee`]).
    pub enabled: bool,
    /// Smoothed fsync latency above `max(baseline, floor) · congestion_ratio`
    /// counts as the shared write path saturating and drives an attack.
    pub congestion_ratio: f64,
    /// Per-tick pressure rise (fast attack) when saturating, before the severity
    /// steepening.
    pub attack: f64,
    /// Proportional gain on the attack: steepens the rise with the fractional
    /// overshoot past the knee, so a hard wall clamps in a few ticks while a mild
    /// overshoot is nudged gently.
    pub attack_gain: f64,
    /// Per-tick pressure fall (slow release) when the write path has headroom. The
    /// attack/release asymmetry is the anti-oscillation term.
    pub release: f64,
    /// EWMA weight on the newest fsync-latency sample (0–1, smaller = smoother).
    pub ewma: f64,
    /// Upward drift of the healthy baseline when no faster sample is seen (so a
    /// permanently slower disk isn't throttled forever). Snap-down is immediate.
    pub baseline_creep: f64,
    /// Floor (µs) applied to the calibrated baseline when computing the knee, so a
    /// near-zero idle baseline can't make the threshold trivially crossable — the
    /// idle raft fsync is sub-ms; the load knee is ~2.4–3 ms.
    pub baseline_floor_us: f64,
    /// Relax Tier-1 during a recovery window (`NANOBPMN_TIER1_RECOVERY_RELAX`,
    /// default **on**). When a node is a failover incumbent or a returning owner,
    /// the recovery admission throttle ([`crate::recovery_throttle`]) is the primary
    /// disk-protection actuator — it already paces intake under the fsync knee. A
    /// second soft-knee shed here would only shed the recovering owner's *own*
    /// creates for no aggregate-latency benefit (it just displaces them onto the
    /// already-loaded survivors), so during recovery the guard defers to a wider
    /// hard-ceiling backstop instead of the soft knee.
    pub recovery_relax: bool,
    /// Hard-ceiling knee multiple applied *during recovery* in place of
    /// `congestion_ratio`. Wider than the soft knee (so the recovery throttle owns
    /// the soft band) but still sheds a genuinely drowning node as a backstop.
    pub recovery_ratio: f64,
    /// Read-model **export-queue** fill fraction (0–1, of the per-shard budget) at
    /// which the guard *starts* shedding, ramping linearly to a full shed at 1.0.
    /// This is the second shared-write-path saturation input, fused into the same
    /// graded even-spread shedder as the raft-fsync knee (replacing the old binary
    /// "all shards at budget → shed every create" gate that bang-banged the queue).
    /// Unlike the fsync term it is a *normalised* signal (already a fraction of a
    /// budget), so it needs no baseline learning and actuates independent of SLA
    /// mode and the fsync `enabled` switch — it is memory protection. `1.0` reverts
    /// to the near-binary "only shed when fully over budget" behaviour; the exporter
    /// term is inert whenever export-queue backpressure is unconfigured (fill = 0).
    ///
    /// Default `0.3` (was `0.5`, orig `0.8`). Two coupled control-loop fixes make
    /// the export shed *smooth* under a hard downstream (ES) drain bottleneck: this
    /// knee, and a right-sized export queue (see `EXPORTER_QUEUE_LIMIT_FRACTION_PCT`).
    /// The exporter term is a *proportional* controller on queue fill — its gain is
    /// `1000/(1−knee)` per-mille per unit-fill, so a *lower* knee is *lower* gain
    /// (0.3 → 1429; 0.5 → 2000; 0.8 → 5000) and a wider proportional band, both of
    /// which damp the loop. A low knee also shrinks the empty-queue "honeymoon" (the
    /// full-admit window before fill first reaches the knee) that otherwise
    /// overshoots and forces a deep corrective shed. It keeps a small dead-band
    /// (zero shed below 30 % fill) so a queue that is comfortably draining never
    /// sheds. `1.0` reverts to the near-binary "only shed when fully over budget"
    /// behaviour; the exporter term is inert whenever export-queue backpressure is
    /// unconfigured (fill = 0). A soak of knee 0.5 with a 13 GB queue still showed a
    /// slow (~60–130 s) relaxation oscillation (admit cycling ~800↔5300/bucket);
    /// knee 0.3 + a ~4 GB queue removes the big slow integrator and the honeymoon.
    pub exporter_knee: f64,
    /// **Latency-causal gate** target sojourn (seconds) for the fsync knee (ADR-0021
    /// "shedding must be latency-causal"). The raft-fsync knee alone only means the
    /// shared write path is *busy* — under a disk-bound workload the healthy loaded
    /// fsync (several ms) permanently sits above an idle-seeded knee, so an ungated
    /// knee sheds ~all intake while the backlog is flat/zero, punishing admission for
    /// *zero* latency gain. The fsync term may therefore only attack when the global
    /// in-flight backlog is above its Little's-law band `L* = W_target · λ` (realized
    /// global sojourn past target) and still rising — i.e. the write path is actually
    /// backing work up, so pacing intake buys latency. Below band / draining, the
    /// elevated fsync is the normal loaded operating point and the knee holds/releases
    /// (and *learns* that loaded level as its baseline, breaking the idle-seed latch).
    /// Default 30 s (mirrors the Tier-2 `W_target`); `NANOBPMN_TIER1_W_TARGET_MS`.
    /// The exporter term is memory protection and stays *ungated*.
    pub w_target_s: f64,
    /// Deadband half-width for the latency-causal gate as a fraction of λ (min 1
    /// instance): the hold zone around the band so a backlog hovering at target does
    /// not chatter the gate. Default 0.1; `NANOBPMN_TIER1_DEADBAND_FRAC`.
    pub deadband_frac: f64,
}

impl Default for Tier1Config {
    fn default() -> Self {
        Self {
            enabled: true,
            congestion_ratio: 2.0,
            attack: 0.15,
            attack_gain: 2.0,
            release: 0.05,
            ewma: 0.3,
            baseline_creep: 0.05,
            baseline_floor_us: 500.0,
            recovery_relax: true,
            recovery_ratio: 4.0,
            exporter_knee: 0.3,
            w_target_s: 30.0,
            deadband_frac: 0.1,
        }
    }
}

impl Tier1Config {
    /// Resolve the config from the `NANOBPMN_TIER1_*` environment, falling back to
    /// [`Default`] for any unset/unparseable knob.
    pub fn from_env() -> Self {
        let d = Self::default();
        let enabled = match std::env::var("NANOBPMN_TIER1") {
            Ok(v) => {
                let v = v.trim();
                !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
            }
            Err(_) => d.enabled,
        };
        let recovery_relax = match std::env::var("NANOBPMN_TIER1_RECOVERY_RELAX") {
            Ok(v) => {
                let v = v.trim();
                !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
            }
            Err(_) => d.recovery_relax,
        };
        let f = |key: &str, def: f64| -> f64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|x| x.is_finite())
                .unwrap_or(def)
        };
        Self {
            enabled,
            congestion_ratio: f("NANOBPMN_TIER1_RATIO", d.congestion_ratio),
            attack: f("NANOBPMN_TIER1_ATTACK", d.attack),
            attack_gain: f("NANOBPMN_TIER1_ATTACK_GAIN", d.attack_gain),
            release: f("NANOBPMN_TIER1_RELEASE", d.release),
            ewma: f("NANOBPMN_TIER1_EWMA", d.ewma).clamp(0.0, 1.0),
            baseline_creep: f("NANOBPMN_TIER1_CREEP", d.baseline_creep),
            baseline_floor_us: f("NANOBPMN_TIER1_FLOOR_US", d.baseline_floor_us),
            recovery_relax,
            recovery_ratio: f("NANOBPMN_TIER1_RECOVERY_RATIO", d.recovery_ratio),
            exporter_knee: f("NANOBPMN_TIER1_EXPORTER_KNEE", d.exporter_knee).clamp(0.0, 1.0),
            w_target_s: f("NANOBPMN_TIER1_W_TARGET_MS", d.w_target_s * 1000.0) / 1000.0,
            deadband_frac: f("NANOBPMN_TIER1_DEADBAND_FRAC", d.deadband_frac),
        }
    }
}

/// Interior controller state, mutated only by the monitor `step`.
#[derive(Default)]
struct GuardState {
    /// Low-pass EWMA of the window-mean raft-log fsync latency (µs). `None` until
    /// the first non-empty window seeds it.
    ewma_us: Option<f64>,
    /// Calibrated healthy-window fsync baseline (µs). `None` until seeded.
    baseline_us: Option<f64>,
    /// The control output p ∈ [0,1] (0 = healthy, 1 = fully shed).
    pressure: f64,
}

/// The ADR-0020 **Tier-1** global engine-saturation guard: a single node-level
/// compressor on the shared write path's saturation signal (raft-log fsync
/// latency). It throttles *all* intake when the engine's own commit machinery is
/// the bottleneck — the class the apply-loop ρ signal mis-located — independent
/// of any worker pool. The monitor calls [`GlobalGuard::step`] once per tick with
/// the window-mean fsync latency; admission calls [`GlobalGuard::should_shed`]
/// per `createProcessInstance`. The final admission decision is the intersection
/// with Tier-2: shed if *either* tier sheds.
pub struct GlobalGuard {
    cfg: Tier1Config,
    state: std::sync::Mutex<GuardState>,
    /// Published shed fraction (per-mille) + accumulator, read (and advanced) by
    /// admission — separated from `state` so the hot path takes only this brief,
    /// uncontended lock.
    published: std::sync::Mutex<PubPressure>,
    verbose: bool,
}

impl GlobalGuard {
    pub fn new(cfg: Tier1Config) -> Self {
        Self {
            cfg,
            state: std::sync::Mutex::new(GuardState::default()),
            published: std::sync::Mutex::new(PubPressure::default()),
            verbose: std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some(),
        }
    }

    /// Fold one monitor tick. `fsync_avg_us` is the window-mean raft-log fsync
    /// latency this tick (µs), from the delta of `nanobpm_raft_fsync_seconds`
    /// sum/count (`0` = no fsyncs this window → treated as idle/healthy, releasing
    /// gently without disturbing the baseline). `active` is whether Tier-1 should
    /// actuate at all (latency SLA mode + feature enabled); when false the guard
    /// releases to zero. `recovering` is whether this node is in a recovery window
    /// (failover incumbent or returning owner, from `recovery_fsync_load_active`):
    /// while it holds the guard defers to the recovery admission throttle and only
    /// sheds past a wider hard-ceiling backstop (see [`Tier1Config::recovery_relax`]).
    ///
    /// `latency_causal` is the ADR-0021 gate on the fsync knee: `true` only when the
    /// global in-flight backlog is above its Little's-law band and rising (see
    /// [`GlobalGuard::latency_causal`]), i.e. the busy write path is actually backing
    /// work up so pacing intake buys latency. When `false` the fsync term must not
    /// attack — the elevated fsync is the normal loaded operating point, and shedding
    /// there punishes admission for no latency gain. The gate applies only to the
    /// fsync term; the exporter term (memory protection) is always evaluated.
    ///
    /// Returns the published shed fraction in per-mille (for the
    /// `nanobpm_tier1_pressure` gauge).
    pub fn step(
        &self,
        fsync_avg_us: f64,
        active: bool,
        recovering: bool,
        exporter_fill: f64,
        latency_causal: bool,
    ) -> u32 {
        // Second shared-write-path saturation input: the read-model export queue.
        // Already a normalised fill fraction, so it needs no baseline learning and
        // is computed unconditionally (memory protection, independent of SLA mode
        // and the fsync `enabled` switch; inert at fill = 0 when export-queue
        // backpressure is unconfigured).
        let exporter_permille = Self::exporter_permille(self.cfg.exporter_knee, exporter_fill);

        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());

        if !self.cfg.enabled || !active {
            st.pressure = 0.0;
            return self.finish(0, exporter_permille);
        }

        // Empty window (idle, no writes): not saturation. Release gently and leave
        // the smoothed signal / baseline untouched.
        if fsync_avg_us <= 0.0 {
            st.pressure = (st.pressure - self.cfg.release).max(0.0);
            let fsync_permille = (st.pressure * 1000.0).round().clamp(0.0, 1000.0) as u32;
            return self.finish(fsync_permille, exporter_permille);
        }

        // Low-pass the signal.
        let signal = {
            let s = match st.ewma_us {
                Some(prev) => self.cfg.ewma * fsync_avg_us + (1.0 - self.cfg.ewma) * prev,
                None => fsync_avg_us,
            };
            st.ewma_us = Some(s);
            s
        };

        // Recovery window: the recovery admission throttle is the primary disk-
        // protection actuator (it paces intake under the fsync knee via the backlog
        // cap). A second soft-knee shed here would only shed the recovering owner's
        // *own* creates for no aggregate-latency benefit, so defer to a wider
        // hard-ceiling backstop and never calibrate the baseline from the congested
        // recovery latency (it would inflate the knee for steady state afterward).
        if recovering && self.cfg.recovery_relax {
            match st.baseline_us {
                // A healthy baseline learned outside recovery: shed only past the
                // wide hard ceiling (a genuinely drowning node still gets clamped),
                // otherwise release. Baseline stays frozen throughout.
                Some(b) => {
                    let baseline = b.max(self.cfg.baseline_floor_us);
                    let threshold = baseline * self.cfg.recovery_ratio;
                    if signal > threshold {
                        let severity = (signal / threshold - 1.0).clamp(0.0, 1.0);
                        let step = self.cfg.attack * (1.0 + self.cfg.attack_gain * severity);
                        st.pressure = (st.pressure + step).min(1.0);
                    } else {
                        st.pressure = (st.pressure - self.cfg.release).max(0.0);
                    }
                }
                // No healthy baseline yet (fresh restart straight into recovery): do
                // not seed it from congested recovery latency and do not actuate —
                // the recovery throttle owns disk protection here. Release.
                None => {
                    st.pressure = (st.pressure - self.cfg.release).max(0.0);
                }
            }
            let fsync_permille = (st.pressure * 1000.0).round().clamp(0.0, 1000.0) as u32;
            return self.finish(fsync_permille, exporter_permille);
        }

        if st.baseline_us.is_none() {
            st.baseline_us = Some(signal);
        }
        let baseline = st
            .baseline_us
            .unwrap_or(signal)
            .max(self.cfg.baseline_floor_us);
        let threshold = baseline * self.cfg.congestion_ratio;

        if signal > threshold && latency_causal {
            // Shared write path saturating *and* the backlog is above its Little's-law
            // band and rising (ADR-0021 latency-causal gate): the busy write path is
            // actually backing work up, so pacing intake buys latency. Freeze the
            // baseline (never adapt it toward the congested latency) and attack,
            // steepening with the fractional overshoot past the knee.
            let severity = (signal / threshold - 1.0).clamp(0.0, 1.0);
            let step = self.cfg.attack * (1.0 + self.cfg.attack_gain * severity);
            st.pressure = (st.pressure + step).min(1.0);
            if self.verbose {
                tracing::info!(
                    "tier1: fsync_avg={signal:.0}us knee={threshold:.0}us severity={severity:.2} \
                     p={:.0}permille",
                    st.pressure * 1000.0,
                );
            }
        } else {
            // One of two headroom cases, both of which learn the baseline and release:
            //   (a) `signal <= threshold`: genuine headroom below the knee.
            //   (b) `signal > threshold` but NOT latency-causal (backlog flat/zero or
            //       draining): the knee is crossed but shedding buys no latency — the
            //       elevated fsync is the *normal loaded operating point*, not a
            //       create-driven saturation. Per ADR-0021 do not shed; and, crucially,
            //       *learn* this loaded level (creep the baseline up toward it) so the
            //       knee reflects steady-state load. This breaks the idle-seed latch
            //       where the baseline froze at the sub-ms warmup floor and the knee
            //       then sat permanently below the multi-ms loaded fsync.
            st.baseline_us = Some(match st.baseline_us {
                Some(b) if signal < b => signal,
                Some(b) => b + (signal - b) * self.cfg.baseline_creep,
                None => signal,
            });
            st.pressure = (st.pressure - self.cfg.release).max(0.0);
        }

        let fsync_permille = (st.pressure * 1000.0).round().clamp(0.0, 1000.0) as u32;
        self.finish(fsync_permille, exporter_permille)
    }

    /// The ADR-0021 **latency-causal gate** for the fsync knee: `true` only when the
    /// global in-flight `backlog` is above its Little's-law band `L* = W_target · λ`
    /// (with a `deadband_frac · λ` hold zone) *and* still rising (`growth_per_s >= 0`).
    /// That is exactly "realized global sojourn is past target and accumulating", so
    /// the busy write path is backing work up and pacing intake buys latency. Below
    /// band or draining it returns `false`, and [`step`](Self::step) then holds/releases
    /// the fsync term instead of shedding a healthy-but-loaded write path. `lambda` is
    /// the throughput proxy (completions/s); when it is ~0 the band collapses to the
    /// `deadband` floor so a stalled node with a real backlog still gates open.
    pub fn latency_causal(&self, backlog: f64, lambda: f64, growth_per_s: f64) -> bool {
        let lambda = lambda.max(0.0);
        let band = self.cfg.w_target_s * lambda; // L* = W_target · λ
        let deadband = (self.cfg.deadband_frac * lambda).max(1.0);
        backlog > band + deadband && growth_per_s >= 0.0
    }

    /// Graded even-spread shed fraction (per-mille) for the read-model export
    /// queue, from its fill fraction (`0` = empty … `1.0` = at budget, `>1.0` =
    /// over). Ramps linearly from `knee` to a full shed (1000) at/above budget;
    /// `0` below the knee. Stateless — the fill is already a normalised saturation,
    /// so (unlike the fsync latency knee) it needs no baseline learning.
    fn exporter_permille(knee: f64, fill: f64) -> u32 {
        if fill <= knee {
            return 0;
        }
        let span = (1.0 - knee).max(1e-6);
        (((fill - knee) / span).clamp(0.0, 1.0) * 1000.0).round() as u32
    }

    /// The graded export-queue shed fraction (per-mille) this guard *would* apply
    /// for a given export-queue fill fraction — `0` below the knee, ramping to
    /// `1000` at/above budget (see [`exporter_permille`](Self::exporter_permille)).
    /// Read by the capacity-ceiling LED so the "exporter" meter lights exactly when
    /// export lag has crossed the knee and is actively compressing create intake.
    pub fn exporter_shed_permille(&self, fill: f64) -> u32 {
        Self::exporter_permille(self.cfg.exporter_knee, fill)
    }

    /// Publish (and return) the combined shed fraction: the max of the fsync-knee
    /// pressure and the export-queue pressure. One published per-mille drives the
    /// single even-spread [`should_shed`](Self::should_shed) actuator for both
    /// saturation inputs, so there is no second shed implementation.
    fn finish(&self, fsync_permille: u32, exporter_permille: u32) -> u32 {
        let p = fsync_permille.max(exporter_permille);
        self.publish(p);
        p
    }

    fn publish(&self, permille: u32) {
        self.published
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .permille = permille;
    }

    /// Admission hook: return `true` if this `createProcessInstance` should be shed
    /// under the current global guard pressure. Sheds a `permille/1000` fraction of
    /// *all* creates, spread evenly by an accumulator (no RNG, deterministic).
    /// Zero-pressure (the common case) is a single cheap lock with no shed.
    pub fn should_shed(&self) -> bool {
        let mut pubm = self.published.lock().unwrap_or_else(|e| e.into_inner());
        if pubm.permille == 0 {
            return false;
        }
        pubm.acc += pubm.permille as u64;
        if pubm.acc >= 1000 {
            pubm.acc -= 1000;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_to_adaptive() {
        assert_eq!(
            parse_backpressure_setting(None),
            BackpressureSetting::Adaptive
        );
        for raw in ["auto", "adaptive", "ADAPTIVE", " Auto "] {
            assert_eq!(
                parse_backpressure_setting(Some(raw)),
                BackpressureSetting::Adaptive,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn parse_off_aliases_disable() {
        for raw in ["0", "off", "none", "false", "disabled", "OFF", " Off "] {
            assert_eq!(
                parse_backpressure_setting(Some(raw)),
                BackpressureSetting::Disabled,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn parse_positive_integer_is_fixed() {
        assert_eq!(
            parse_backpressure_setting(Some("6000")),
            BackpressureSetting::Fixed(6000)
        );
        assert_eq!(
            parse_backpressure_setting(Some(" 1 ")),
            BackpressureSetting::Fixed(1)
        );
    }

    #[test]
    fn parse_unparseable_falls_back_to_adaptive() {
        for raw in ["garbage", "-5", "1.5"] {
            assert_eq!(
                parse_backpressure_setting(Some(raw)),
                BackpressureSetting::Adaptive,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn sla_mode_defaults_to_latency() {
        assert_eq!(parse_sla_mode(None), SlaMode::Latency);
        for raw in [
            "latency",
            "preserve-latency",
            "time-to-complete",
            " Latency ",
            "",
        ] {
            assert_eq!(parse_sla_mode(Some(raw)), SlaMode::Latency, "{raw:?}");
        }
    }

    #[test]
    fn sla_mode_admission_aliases() {
        for raw in [
            "admission",
            "preserve-admission",
            "accept-latency",
            "start-every-process",
            "throughput",
            " Admission ",
        ] {
            assert_eq!(parse_sla_mode(Some(raw)), SlaMode::Admission, "{raw:?}");
        }
    }

    #[test]
    fn sla_mode_unrecognised_fails_safe_to_latency() {
        for raw in ["garbage", "fast", "0"] {
            assert_eq!(parse_sla_mode(Some(raw)), SlaMode::Latency, "{raw:?}");
        }
    }

    #[test]
    fn sla_mode_sheds_for_latency_only_in_latency_mode() {
        assert!(SlaMode::Latency.sheds_for_latency());
        assert!(!SlaMode::Admission.sheds_for_latency());
    }

    #[test]
    fn shared_sla_mode_is_switchable_and_shared_across_clones() {
        let a = SharedSlaMode::new(SlaMode::Latency);
        let b = a.clone();
        assert_eq!(a.get(), SlaMode::Latency);
        assert_eq!(b.get(), SlaMode::Latency);
        // A switch on one handle is visible through the other (shared Arc<atomic>).
        b.set(SlaMode::Admission);
        assert_eq!(a.get(), SlaMode::Admission);
        assert_eq!(b.get(), SlaMode::Admission);
        a.set(SlaMode::Latency);
        assert_eq!(b.get(), SlaMode::Latency);
    }

    #[test]
    fn sla_mode_from_u8_fails_safe_to_latency() {
        assert_eq!(SlaMode::from_u8(SlaMode::Latency as u8), SlaMode::Latency);
        assert_eq!(
            SlaMode::from_u8(SlaMode::Admission as u8),
            SlaMode::Admission
        );
        // Any unexpected byte never silently drops latency protection.
        assert_eq!(SlaMode::from_u8(200), SlaMode::Latency);
    }

    #[test]
    fn current_limit_reflects_mode() {
        assert_eq!(Backpressure::Disabled.current_limit(), None);
        assert_eq!(Backpressure::Fixed(7).current_limit(), Some(7));
        let l = Arc::new(AtomicUsize::new(123));
        assert_eq!(Backpressure::Adaptive(l).current_limit(), Some(123));
    }

    #[test]
    fn should_shed_compares_concurrency_against_the_watermark() {
        // Disabled never sheds, whatever the gauge.
        assert!(!Backpressure::Disabled.should_shed(0));
        assert!(!Backpressure::Disabled.should_shed(1_000_000));

        // Fixed watermark sheds at or above the limit, admits below it.
        let bp = Backpressure::Fixed(2);
        assert!(
            !bp.should_shed(0),
            "0 concurrent creates is below the limit"
        );
        assert!(!bp.should_shed(1), "1 concurrent create is below the limit");
        assert!(
            bp.should_shed(2),
            "at the limit sheds (admits at most `limit`)"
        );
        assert!(bp.should_shed(3), "above the limit sheds");

        // Adaptive tracks its shared atomic.
        let l = Arc::new(AtomicUsize::new(5));
        let bp = Backpressure::Adaptive(l.clone());
        assert!(!bp.should_shed(4));
        assert!(bp.should_shed(5));
        l.store(10, Ordering::Relaxed);
        assert!(!bp.should_shed(5), "raising the watermark re-admits");
    }

    #[test]
    fn slow_start_grows_multiplicatively_while_healthy_and_loaded() {
        let mut a = AimdLimit::new(256, 256, 20_000);
        // baseline established at 100us; avg stays healthy (<2x); inflight saturates.
        let l1 = a.on_window(100.0, 10_000); // first window sets baseline
        let l2 = a.on_window(120.0, 10_000);
        assert!(l2 > l1, "slow-start should grow: {l1} -> {l2}");
        assert_eq!(l2, l1 * 2);
    }

    #[test]
    fn congestion_triggers_multiplicative_decrease_and_ends_slow_start() {
        let mut a = AimdLimit::new(1000, 256, 20_000);
        a.on_window(100.0, 10_000); // baseline 100us
        let before = a.limit();
        // avg 500us = 5x baseline > 2x threshold => congested
        let after = a.on_window(500.0, 10_000);
        assert!(
            after < before,
            "congestion must shrink the limit: {before} -> {after}"
        );
        // Now healthy + loaded but slow-start is over => additive (+1), not doubling.
        let add = a.on_window(120.0, 10_000);
        assert_eq!(add, after + 1, "post-congestion growth is additive");
    }

    #[test]
    fn never_grows_below_load_or_outside_bounds() {
        let mut a = AimdLimit::new(1000, 256, 20_000);
        a.on_window(100.0, 0); // baseline; inflight 0 => not loaded
        let l = a.on_window(120.0, 0); // healthy but idle => no growth
        assert_eq!(l, 1000);
        // Decrease floors at min_limit.
        for _ in 0..100 {
            a.on_window(10_000.0, 10_000);
        }
        assert_eq!(a.limit(), 256);
    }

    #[test]
    fn sustained_congestion_keeps_shedding_and_does_not_drift_up() {
        // The unbounded-backlog pathology: latency stays high for a long time.
        // The limiter must keep the limit at the floor, never "learn" the
        // congested latency as normal and reopen the gates.
        let mut a = AimdLimit::new(8000, 256, 20_000);
        a.on_window(100.0, 10_000); // establish a healthy ~100us baseline
        for _ in 0..500 {
            // avg pinned high (queueing / heap pressure), inflight saturated.
            a.on_window(5_000.0, 10_000);
        }
        assert_eq!(
            a.limit(),
            256,
            "must stay shedding at the floor under sustained congestion"
        );
    }

    // --- self-optimizing backlog governor (AdaptiveController) ---------------

    /// Feed `n` synthetic commands of `us` microseconds each of the given class
    /// (`is_create`) so tests can drive the create vs completion window
    /// independently — the whole point of the per-class split. Enough to satisfy
    /// `WINDOW_MIN_SAMPLES`, with a sleep so `WINDOW_MIN_INTERVAL` also elapses.
    fn drive_window_class(c: &mut AdaptiveController, us: u64, n: u64, is_create: bool) {
        std::thread::sleep(WINDOW_MIN_INTERVAL + Duration::from_millis(1));
        for _ in 0..n.max(WINDOW_MIN_SAMPLES) {
            c.record(Duration::from_micros(us), is_create);
        }
    }

    // --- standalone internal-latency backlog compressor ---------------------

    /// Compressor params for the tests: ρ setpoint 0.9 ± 0.05 dead-band, fast attack
    /// (0.7 base), slow release (1.1), attack-gain 3 on the growth rate.
    const C_TARGET: f64 = 0.9;
    const C_DEADBAND: f64 = 0.05;
    const C_ATTACK: f64 = 0.7;
    const C_RELEASE: f64 = 1.1;
    const C_GAIN: f64 = 3.0;

    fn test_compressor() -> (BacklogGovernor, Arc<AtomicUsize>, GovernorObs) {
        BacklogGovernor::new(
            2_000, 200_000, C_TARGET, C_DEADBAND, C_ATTACK, C_RELEASE, C_GAIN,
        )
    }

    #[test]
    fn backlog_compressor_starts_inert_at_the_ceiling() {
        let (_gov, cap, _obs) = test_compressor();
        assert_eq!(
            cap.load(Ordering::Relaxed),
            200_000,
            "the compressor is inert (cap at ceiling) until ρ+backlog-trend clamps it"
        );
    }

    #[test]
    fn backlog_compressor_attacks_toward_floor_when_saturated_and_rising() {
        let (gov, cap, _obs) = test_compressor();
        let before = cap.load(Ordering::Relaxed);
        // Actor saturated (ρ above target+deadband) AND backlog rising => attack.
        gov.step(0.99, 5_000.0);
        assert!(
            cap.load(Ordering::Relaxed) < before,
            "saturated-and-rising must cut the cap: {before} -> {}",
            cap.load(Ordering::Relaxed)
        );
        // Sustained saturation-and-rising drives it all the way to the floor.
        for _ in 0..200 {
            gov.step(0.99, 5_000.0);
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            2_000,
            "sustained saturated-and-rising clamps to the floor"
        );
    }

    #[test]
    fn backlog_compressor_releases_toward_ceiling_when_draining() {
        let (gov, cap, _obs) = test_compressor();
        // Clamp it down first.
        for _ in 0..200 {
            gov.step(0.99, 5_000.0);
        }
        assert_eq!(cap.load(Ordering::Relaxed), 2_000);
        // Backlog now draining (growth < 0) => slow release back toward the ceiling.
        let after_clamp = cap.load(Ordering::Relaxed);
        gov.step(0.99, -100.0);
        assert!(
            cap.load(Ordering::Relaxed) > after_clamp,
            "draining must grow the cap: {after_clamp} -> {}",
            cap.load(Ordering::Relaxed)
        );
        for _ in 0..500 {
            gov.step(0.99, -100.0);
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            200_000,
            "sustained draining releases to the ceiling"
        );
    }

    #[test]
    fn backlog_compressor_releases_when_saturation_falls_below_target() {
        let (gov, cap, _obs) = test_compressor();
        for _ in 0..200 {
            gov.step(0.99, 5_000.0);
        }
        assert_eq!(cap.load(Ordering::Relaxed), 2_000);
        // ρ well below the target-deadband band (idle headroom) => release even
        // though the backlog is not draining (external strain: workers are the wall).
        for _ in 0..500 {
            gov.step(0.10, 5_000.0);
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            200_000,
            "low saturation (external strain) releases to the ceiling — we do not \
             throttle when the engine actor is not the bottleneck"
        );
    }

    #[test]
    fn backlog_compressor_holds_at_a_healthy_saturated_peak() {
        let (gov, cap, _obs) = test_compressor();
        // Clamp down to a mid value first.
        gov.step(0.99, 200_000.0);
        let settled = cap.load(Ordering::Relaxed);
        assert!(settled < 200_000 && settled > 2_000);
        // Saturated (ρ high) but the backlog is STABLE (growth 0) — a healthy peak,
        // the compressor knee. Must hold, neither attack nor release.
        for _ in 0..20 {
            gov.step(0.99, 0.0);
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            settled,
            "a healthy saturated peak (ρ high, backlog stable) must hold the cap"
        );
    }

    #[test]
    fn backlog_compressor_holds_in_the_ambiguous_midzone() {
        // ρ inside the dead-band (at the setpoint) and the backlog rising: neither
        // clearly saturated nor draining => hold, don't thrash.
        let (gov, cap, _obs) = test_compressor();
        let before = cap.load(Ordering::Relaxed);
        for _ in 0..50 {
            gov.step(C_TARGET, 5_000.0);
        }
        assert_eq!(
            cap.load(Ordering::Relaxed),
            before,
            "ρ within the dead-band of the setpoint must hold the cap"
        );
    }

    #[test]
    fn backlog_compressor_attack_steepens_with_backlog_growth_rate() {
        // The one proportional touch: a faster-rising backlog cuts more per tick than
        // a slow overshoot (attack^(1+gain·g), g = growth/cap).
        let (fast, fast_cap, _) = test_compressor();
        let (slow, slow_cap, _) = test_compressor();
        // Cold-start storm: growth ≈ the whole cap per second => g≈1.
        fast.step(0.99, 200_000.0);
        // Mild overshoot: a trickle of growth => g≈0.
        slow.step(0.99, 10.0);
        assert!(
            fast_cap.load(Ordering::Relaxed) < slow_cap.load(Ordering::Relaxed),
            "a faster-rising backlog must attack the cap harder: fast {} vs slow {}",
            fast_cap.load(Ordering::Relaxed),
            slow_cap.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn backlog_compressor_publishes_observability_rho_and_growth() {
        let (gov, _cap, obs) = test_compressor();
        // The setpoint is published at construction; the live signals start at zero.
        assert_eq!(
            obs.rho_target_permille.load(Ordering::Relaxed),
            (C_TARGET * 1000.0) as u64
        );
        assert_eq!(obs.rho_permille.load(Ordering::Relaxed), 0);
        assert_eq!(obs.growth_per_s.load(Ordering::Relaxed), 0);

        gov.step(0.95, -300.0);
        assert_eq!(obs.rho_permille.load(Ordering::Relaxed), 950);
        assert_eq!(obs.growth_per_s.load(Ordering::Relaxed), -300);
    }

    #[test]
    fn controller_is_active_only_with_a_limiter_installed() {
        assert!(!AdaptiveController::new().is_active());
        let mut c = AdaptiveController::new();
        c.with_create_limiter(Arc::new(AtomicUsize::new(0)));
        assert!(c.is_active());
        let mut c = AdaptiveController::new();
        c.with_worker_governor(50, 4_096, Arc::new(AtomicUsize::new(0)));
        assert!(c.is_active());
    }

    // --- self-optimizing worker-concurrency governor -------------------------

    #[test]
    fn worker_governor_grows_active_width_while_healthy_and_loaded() {
        // Backlog well above the width => there is work to fan out across more
        // subscribers, and latency stays healthy, so the governor slow-starts the
        // active dispatch width upward from the floor.
        let backlog = Arc::new(AtomicUsize::new(100_000));
        let mut c = AdaptiveController::new();
        let width = c.with_worker_governor(50, 4_096, backlog.clone());
        assert_eq!(width.load(Ordering::Relaxed), 50, "starts at the floor");

        drive_window_class(&mut c, 100, WINDOW_MIN_SAMPLES, false); // establish baseline
        let after_baseline = width.load(Ordering::Relaxed);
        drive_window_class(&mut c, 110, WINDOW_MIN_SAMPLES, false); // healthy + loaded => grow
        assert!(
            width.load(Ordering::Relaxed) > after_baseline,
            "healthy loaded windows must widen the fan-out: {after_baseline} -> {}",
            width.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn worker_governor_backs_off_to_floor_under_congestion() {
        // Over-provisioning past the knee inflates per-command latency; the
        // governor must narrow the active width back to the floor so excess
        // subscribers stop swamping the push dispatcher.
        let backlog = Arc::new(AtomicUsize::new(100_000));
        let mut c = AdaptiveController::new();
        let width = c.with_worker_governor(50, 4_096, backlog.clone());
        drive_window_class(&mut c, 100, WINDOW_MIN_SAMPLES, false); // baseline 100us
        for _ in 0..5 {
            drive_window_class(&mut c, 110, WINDOW_MIN_SAMPLES, false); // grow a bit first
        }
        assert!(width.load(Ordering::Relaxed) > 50);
        for _ in 0..200 {
            drive_window_class(&mut c, 5_000, WINDOW_MIN_SAMPLES, false); // sustained congestion
        }
        assert_eq!(
            width.load(Ordering::Relaxed),
            50,
            "congestion must hold the active width at the knee floor"
        );
    }

    #[test]
    fn worker_governor_does_not_widen_without_backlog() {
        // No runnable backlog (workers idle / nothing to drain): widening the
        // fan-out would only add dispatcher overhead, so growth is gated on the
        // backlog signal and the width must stay pinned at the floor.
        let backlog = Arc::new(AtomicUsize::new(0));
        let mut c = AdaptiveController::new();
        let width = c.with_worker_governor(50, 4_096, backlog.clone());
        drive_window_class(&mut c, 100, WINDOW_MIN_SAMPLES, false); // baseline
        for _ in 0..20 {
            drive_window_class(&mut c, 110, WINDOW_MIN_SAMPLES, false); // healthy but unloaded
        }
        assert_eq!(
            width.load(Ordering::Relaxed),
            50,
            "no backlog to drain must never widen the active fan-out"
        );
    }

    // ── ADR-0020 Tier-2 per-definition governor ─────────────────────────────

    fn tight_procgov() -> ProcessGovernors {
        // Tight, deterministic knobs for the unit tests: a small band so the
        // scenarios cross it quickly, brisk attack, slow release.
        ProcessGovernors::new(ProcGovConfig {
            w_target_s: 1.0,
            attack: 0.34,
            attack_gain: 3.0,
            release: 0.05,
            deadband_frac: 0.1,
            ewma: 0.5,
            min_lambda: 5.0,
        })
    }

    fn pressure_of(g: &ProcessGovernors, pid: &str) -> u32 {
        g.pressures()
            .into_iter()
            .find(|(k, _)| k == pid)
            .map(|(_, p)| p)
            .unwrap_or(0)
    }

    #[test]
    fn tier2_attacks_a_definition_whose_backlog_grows_above_band() {
        let g = tight_procgov();
        // A definition creating ~1000/s (λ well above min_lambda) with its
        // in-flight backlog climbing far past the band L* = W_target·λ = 1000.
        let mut inflight = 1000u64;
        let mut created = 1000u64;
        // Seed the previous-tick baseline.
        g.step(1.0, &[("orders-slow".into(), inflight, created)]);
        for _ in 0..8 {
            created += 1000; // λ ≈ 1000/s
            inflight += 800; // backlog rising fast, above band
            g.step(1.0, &[("orders-slow".into(), inflight, created)]);
        }
        assert!(
            pressure_of(&g, "orders-slow") > 500,
            "a runaway, above-band definition must be attacked to high pressure"
        );
    }

    #[test]
    fn tier2_leaves_a_healthy_sibling_untouched() {
        let g = tight_procgov();
        // Two definitions sharing nothing at the governor level: SLOW accumulates,
        // FAST stays flat (create==complete, in-flight ~ constant, within band).
        let mut sc = 1000u64;
        let mut si = 1000u64;
        let mut fc = 1000u64;
        let fi = 500u64; // flat, comfortably under band (1000)
        g.step(
            1.0,
            &[
                ("orders-slow".into(), si, sc),
                ("orders-fast".into(), fi, fc),
            ],
        );
        for _ in 0..8 {
            sc += 1000;
            si += 800; // slow: rising above band
            fc += 1000; // fast: same create rate…
            // …but in-flight stays flat (drains as fast as it creates).
            g.step(
                1.0,
                &[
                    ("orders-slow".into(), si, sc),
                    ("orders-fast".into(), fi, fc),
                ],
            );
        }
        assert!(
            pressure_of(&g, "orders-slow") > 500,
            "the accumulating definition is throttled"
        );
        assert_eq!(
            pressure_of(&g, "orders-fast"),
            0,
            "a healthy sibling with a flat in-flight backlog must NOT be throttled"
        );
    }

    #[test]
    fn tier2_releases_when_a_definition_drains() {
        let g = tight_procgov();
        // Drive it to high pressure first.
        let mut inflight = 1000u64;
        let mut created = 1000u64;
        g.step(1.0, &[("p".into(), inflight, created)]);
        for _ in 0..8 {
            created += 1000;
            inflight += 800;
            g.step(1.0, &[("p".into(), inflight, created)]);
        }
        assert!(pressure_of(&g, "p") > 500);
        // Now it drains: in-flight falls each tick, still creating.
        for _ in 0..40 {
            created += 1000;
            inflight = inflight.saturating_sub(600);
            g.step(1.0, &[("p".into(), inflight, created)]);
        }
        assert_eq!(
            pressure_of(&g, "p"),
            0,
            "a draining definition must release its pressure back to zero"
        );
    }

    #[test]
    fn tier2_does_not_throttle_a_barely_creating_definition() {
        let g = tight_procgov();
        // Below min_lambda (5/s): even a deep in-flight backlog must not throttle,
        // because dividing by a noise-level λ would fabricate a tiny band.
        let mut inflight = 10_000u64;
        let mut created = 100u64;
        g.step(1.0, &[("idle".into(), inflight, created)]);
        for _ in 0..8 {
            created += 1; // λ ≈ 1/s < min_lambda
            inflight += 1;
            g.step(1.0, &[("idle".into(), inflight, created)]);
        }
        assert_eq!(
            pressure_of(&g, "idle"),
            0,
            "a definition creating below min_lambda is never throttled"
        );
    }

    #[test]
    fn tier2_should_shed_paces_the_configured_fraction() {
        let g = tight_procgov();
        // Force a definition to a known pressure, then confirm should_shed sheds
        // ~that fraction evenly (deterministic accumulator, no RNG).
        let mut inflight = 1000u64;
        let mut created = 1000u64;
        g.step(1.0, &[("p".into(), inflight, created)]);
        for _ in 0..8 {
            created += 1000;
            inflight += 800;
            g.step(1.0, &[("p".into(), inflight, created)]);
        }
        let permille = pressure_of(&g, "p");
        assert!(permille > 0);
        let n = 10_000;
        let shed = (0..n).filter(|_| g.should_shed("p")).count();
        let expected = n * permille as usize / 1000;
        let tol = n / 100; // ±1% of samples
        assert!(
            shed.abs_diff(expected) <= tol,
            "should_shed must pace ~{permille}permille: shed {shed} vs expected {expected}"
        );
        // An unknown definition is never shed.
        assert!(!g.should_shed("unknown-definition"));
    }

    // ── ADR-0020 Tier-1 global engine-saturation guard ───────────────────────

    fn tight_guard() -> GlobalGuard {
        GlobalGuard::new(tight_guard_cfg())
    }

    #[test]
    fn tier1_healthy_write_path_never_sheds() {
        let g = tight_guard();
        // Idle-then-healthy raft fsync (sub-ms, under the floored knee): baseline
        // calibrates, pressure stays at zero.
        for _ in 0..10 {
            assert_eq!(g.step(700.0, true, false, 0.0, true), 0);
        }
        assert!(!g.should_shed());
    }

    #[test]
    fn tier1_attacks_when_the_fsync_knee_is_crossed_and_releases_on_recovery() {
        let g = tight_guard();
        // Calibrate a healthy baseline of ~0.7ms (floored to 500us → knee 1ms).
        assert_eq!(g.step(700.0, true, false, 0.0, true), 0);

        // Shared write path saturates (fsync 3ms >> 1ms knee): attack.
        let p1 = g.step(3_000.0, true, false, 0.0, true);
        let p2 = g.step(3_000.0, true, false, 0.0, true);
        assert!(p1 > 0, "crossing the fsync knee must raise pressure");
        assert!(p2 > p1, "sustained saturation keeps rising: {p1} -> {p2}");

        // Write path recovers (fsync back under the knee): release.
        let p3 = g.step(700.0, true, false, 0.0, true);
        assert!(p3 < p2, "headroom must release pressure: {p2} -> {p3}");
    }

    #[test]
    fn tier1_baseline_floor_prevents_spurious_trip_on_low_idle_fsync() {
        let g = tight_guard();
        // A near-zero idle baseline (50us) would give a 100us knee and trip on any
        // real load; the 500us floor keeps the knee at 1ms so ~700us load is fine.
        g.step(50.0, true, false, 0.0, true); // baseline calibrates to 50us
        for _ in 0..5 {
            assert_eq!(
                g.step(700.0, true, false, 0.0, true),
                0,
                "sub-knee load must not trip the floored guard"
            );
        }
    }

    #[test]
    fn tier1_inactive_or_disabled_never_sheds() {
        // Not active (e.g. admission SLA mode): releases to zero even under a
        // saturating signal.
        let g = tight_guard();
        for _ in 0..10 {
            assert_eq!(g.step(5_000.0, false, false, 0.0, true), 0);
        }
        assert!(!g.should_shed());

        // Feature disabled: never sheds regardless of signal.
        let g = GlobalGuard::new(Tier1Config {
            enabled: false,
            ..tight_guard_cfg()
        });
        for _ in 0..10 {
            assert_eq!(g.step(5_000.0, true, false, 0.0, true), 0);
        }
        assert!(!g.should_shed());
    }

    #[test]
    fn tier1_should_shed_paces_the_pressure_fraction() {
        let g = tight_guard();
        g.step(700.0, true, false, 0.0, true); // baseline
        let mut permille = 0;
        for _ in 0..20 {
            permille = g.step(5_000.0, true, false, 0.0, true); // drive pressure up under saturation
        }
        assert!(permille > 0);
        let n = 10_000;
        let shed = (0..n).filter(|_| g.should_shed()).count();
        let expected = n * permille as usize / 1000;
        let tol = n / 100; // ±1% of samples
        assert!(
            shed.abs_diff(expected) <= tol,
            "should_shed must pace ~{permille}permille: shed {shed} vs expected {expected}"
        );
    }

    #[test]
    fn tier1_recovery_relax_defers_soft_knee_to_the_recovery_throttle() {
        // A returning owner sees elevated recovery fsync above the *soft* knee but
        // under the wide hard-ceiling backstop: with the recovery window flagged,
        // Tier-1 must NOT shed its own creates (the recovery throttle owns pacing).
        let g = tight_guard();
        assert_eq!(g.step(700.0, true, false, 0.0, true), 0); // healthy baseline (knee 1.4ms, ceiling 2.8ms)

        // 2.5ms fsync: crosses the 1.4ms soft knee (would attack in steady state) but
        // is under the 2.8ms hard ceiling → no shed while recovering.
        for _ in 0..20 {
            assert_eq!(
                g.step(2_500.0, true, true, 0.0, true),
                0,
                "recovery window must defer the soft knee to the recovery throttle"
            );
        }
        assert!(!g.should_shed());
    }

    #[test]
    fn tier1_recovery_hard_ceiling_still_sheds_a_drowning_node() {
        // Backstop: even in recovery, fsync past the wide hard ceiling still sheds.
        let g = tight_guard();
        assert_eq!(g.step(700.0, true, false, 0.0, true), 0); // baseline (hard ceiling 2.8ms)

        let p1 = g.step(10_000.0, true, true, 0.0, true); // 10ms >> 2.8ms hard ceiling
        let p2 = g.step(10_000.0, true, true, 0.0, true);
        assert!(
            p1 > 0,
            "past the hard ceiling a drowning node must still shed"
        );
        assert!(p2 > p1, "sustained overload keeps rising: {p1} -> {p2}");
    }

    #[test]
    fn tier1_recovery_freezes_baseline_so_steady_state_knee_is_unchanged() {
        // Recovery must not calibrate the baseline toward the congested latency, or
        // the steady-state knee would inflate afterward and stop protecting latency.
        let g = tight_guard();
        assert_eq!(g.step(700.0, true, false, 0.0, true), 0); // baseline ~500us floor → knee 1ms

        // A long recovery window at elevated (but sub-hard-ceiling) fsync.
        for _ in 0..30 {
            g.step(3_000.0, true, true, 0.0, true);
        }

        // Back to steady state: the soft knee is still ~1ms, so a fresh 3ms spike
        // attacks as before (baseline was NOT dragged up to 3ms during recovery).
        let p = g.step(3_000.0, true, false, 0.0, true);
        assert!(
            p > 0,
            "post-recovery soft knee must be intact (baseline stayed frozen)"
        );
    }

    #[test]
    fn tier1_recovery_without_baseline_defers_and_does_not_seed_from_congestion() {
        // Fresh restart straight into recovery (no healthy baseline yet): the guard
        // must not seed its baseline from congested recovery latency and must not
        // shed. After recovery clears it calibrates normally from healthy load.
        let g = tight_guard();
        for _ in 0..10 {
            assert_eq!(
                g.step(5_000.0, true, true, 0.0, true),
                0,
                "no baseline yet → defer, don't seed from congestion"
            );
        }
        // Recovery clears with healthy load: baseline calibrates, still no shed.
        for _ in 0..5 {
            assert_eq!(g.step(700.0, true, false, 0.0, true), 0);
        }
        // A genuine steady-state spike now attacks off the healthy baseline.
        assert!(g.step(3_000.0, true, false, 0.0, true) > 0);
    }

    #[test]
    fn tier1_recovery_relax_disabled_keeps_the_soft_knee_in_recovery() {
        // With the relax switch off, recovery behaves like steady state: the soft
        // knee still sheds (opt-out path for the revertibility guardrail).
        let g = GlobalGuard::new(Tier1Config {
            recovery_relax: false,
            ..tight_guard_cfg()
        });
        assert_eq!(g.step(700.0, true, false, 0.0, true), 0); // baseline
        let p = g.step(3_000.0, true, true, 0.0, true); // 3ms > 1ms soft knee, recovery ignored
        assert!(p > 0, "relax disabled must keep the soft knee in recovery");
    }

    #[test]
    fn tier1_exporter_fill_sheds_gradedly_independent_of_fsync_and_mode() {
        // The export-queue fill is a second, memory-protection saturation input:
        // it must produce a graded shed fraction, must actuate even when the fsync
        // guard is inactive (`active=false`, e.g. admission mode), and must combine
        // with the fsync term via max.
        let g = tight_guard();

        // Below the knee (0.8): no shed, in any mode.
        assert_eq!(g.step(700.0, false, false, 0.5, true), 0);
        // Above the knee while the fsync guard is INACTIVE: still sheds (graded).
        // fill=0.9 → (0.9-0.8)/(1-0.8) = 0.5 → ~500 permille.
        let mid = g.step(700.0, false, false, 0.9, true);
        assert!(
            (400..=600).contains(&mid),
            "graded exporter shed even with fsync inactive, got {mid}"
        );
        // At/above budget → full shed.
        assert_eq!(g.step(700.0, false, false, 1.0, true), 1000);
        assert_eq!(g.step(700.0, false, false, 1.5, true), 1000);
    }

    #[test]
    fn tier1_publishes_max_of_fsync_and_exporter_pressure() {
        // When both inputs are hot the guard publishes the larger of the two.
        let g = tight_guard();
        g.step(700.0, true, false, 0.0, true); // calibrate healthy baseline
        // Drive fsync pressure up under saturation with no export pressure.
        let mut fsync_only = 0;
        for _ in 0..20 {
            fsync_only = g.step(5_000.0, true, false, 0.0, true);
        }
        assert!(fsync_only > 0, "fsync term should be shedding");
        // A full export queue must pin the published pressure to the max (1000),
        // regardless of the (lower) fsync term.
        let combined = g.step(700.0, true, false, 1.0, true);
        assert_eq!(combined, 1000, "export saturation dominates via max");
    }

    #[test]
    fn tier1_latency_causal_gate_opens_only_above_band_and_rising() {
        // Gate cfg: w_target_s = 1.0s, deadband_frac = 0.1. Band L* = W_target·λ.
        let g = tight_guard();
        let lambda = 1000.0; // 1000 completions/s → band = 1000 instances, deadband 100.

        // Backlog well below band: no latency to relieve → gate closed.
        assert!(
            !g.latency_causal(0.0, lambda, 0.0),
            "flat/zero backlog must gate closed"
        );
        assert!(
            !g.latency_causal(500.0, lambda, 50.0),
            "below-band backlog gates closed"
        );

        // Above band and rising: realized sojourn past target and accumulating → open.
        assert!(
            g.latency_causal(2_000.0, lambda, 100.0),
            "above-band, rising backlog must gate open"
        );

        // Above band but draining: the shed would not buy latency (already recovering)
        // → gate closed so the fsync term releases instead of attacking.
        assert!(
            !g.latency_causal(2_000.0, lambda, -100.0),
            "above-band but draining must gate closed"
        );

        // Near-zero throughput (stalled node) with a real backlog: band collapses to
        // the deadband floor (1 instance), so a genuine backlog still gates open.
        assert!(
            g.latency_causal(50.0, 0.0, 0.0),
            "stalled node with a real backlog must gate open"
        );
    }

    #[test]
    fn tier1_does_not_shed_a_loaded_write_path_when_backlog_is_flat() {
        // The core ADR-0021 fix: a healthy-but-loaded disk (fsync far above the knee)
        // with a FLAT/zero backlog must not be shed — the elevated fsync is the normal
        // loaded operating point, and a shed buys no latency. (Reproduces the soak:
        // ~9ms loaded fsync vs a 1ms knee, active_backlog=0, shedding 99% for nothing.)
        let g = tight_guard();
        g.step(700.0, true, false, 0.0, true); // calibrate a healthy ~0.7ms baseline
        let mut permille = 0;
        for _ in 0..40 {
            // fsync 9ms >> 1ms knee, but latency_causal=false (backlog flat/zero).
            permille = g.step(9_000.0, true, false, 0.0, false);
        }
        assert_eq!(
            permille, 0,
            "loaded-but-not-backing-up write path must not shed"
        );
        assert!(!g.should_shed());
    }

    #[test]
    fn tier1_learns_the_loaded_baseline_when_the_knee_is_crossed_without_backlog() {
        // Breaking the idle-seed latch: when the knee is crossed but the shed is not
        // latency-causal, the baseline must CREEP UP toward the loaded signal so the
        // knee reflects steady-state load — otherwise it stays pinned at the sub-ms
        // warmup floor and trips forever. After learning the ~9ms loaded level, a
        // subsequent genuinely-causal 9ms signal is now *within* the learned knee and
        // no longer a runaway (the guard has adapted to the real operating point).
        let g = GlobalGuard::new(Tier1Config {
            baseline_creep: 0.5, // fast creep so the test converges quickly
            ..tight_guard_cfg()
        });
        g.step(700.0, true, false, 0.0, true); // seed a ~0.7ms (floored 500us) baseline
        // Sustained loaded fsync with a flat backlog: never sheds, and the baseline
        // climbs toward 9ms so the knee rises above the loaded operating point.
        for _ in 0..60 {
            assert_eq!(
                g.step(9_000.0, true, false, 0.0, false),
                0,
                "non-causal loaded fsync must never shed while it learns the baseline"
            );
        }
        // The learned baseline now puts the knee above 9ms: even a causal 9ms sample
        // is at/under the knee, so no spurious attack from the old idle-seeded knee.
        let p = g.step(9_000.0, true, false, 0.0, true);
        assert_eq!(
            p, 0,
            "after learning the loaded floor the knee no longer trips at 9ms"
        );
    }

    #[test]
    fn tier1_still_sheds_a_write_path_that_is_backing_work_up() {
        // Protection is preserved: when the fsync knee is crossed AND the backlog is
        // above band and rising (a genuine create-driven write-path saturation), the
        // guard still attacks.
        let g = tight_guard();
        g.step(700.0, true, false, 0.0, true); // baseline
        let p1 = g.step(3_000.0, true, false, 0.0, true); // knee crossed, gate open
        let p2 = g.step(3_000.0, true, false, 0.0, true);
        assert!(
            p1 > 0 && p2 > p1,
            "a backing-up write path must still be shed: {p1} -> {p2}"
        );
    }

    fn tight_guard_cfg() -> Tier1Config {
        Tier1Config {
            enabled: true,
            congestion_ratio: 2.0,
            attack: 0.25,
            attack_gain: 2.0,
            release: 0.05,
            ewma: 1.0, // no smoothing lag in tests: signal == latest sample
            baseline_creep: 0.05,
            baseline_floor_us: 500.0,
            recovery_relax: true,
            recovery_ratio: 4.0,
            exporter_knee: 0.8,
            w_target_s: 1.0,
            deadband_frac: 0.1,
        }
    }

    /// Poison both `ProcessGovernors` mutexes (a panic while holding each
    /// guard), then confirm the public API still works: lock poisoning on this
    /// recoverable observability path must never abort the process.
    #[test]
    fn process_governors_survive_poisoned_locks() {
        let gov = ProcessGovernors::new(ProcGovConfig::default());
        let state_ref: &std::sync::Mutex<std::collections::HashMap<String, DefGov>> = &gov.state;
        let pub_ref: &std::sync::Mutex<std::collections::HashMap<String, PubPressure>> =
            &gov.published;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = state_ref.lock();
            panic!("poison state");
        }));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = pub_ref.lock();
            panic!("poison published");
        }));

        // Every entry point recovers the poisoned guard instead of panicking.
        gov.step(1.0, &[("p".to_string(), 10, 100)]);
        let _ = gov.should_shed("p");
        let _ = gov.pressures();
    }

    /// Same guard for the Tier-1 `GlobalGuard` mutexes.
    #[test]
    fn global_guard_survives_poisoned_locks() {
        let g = GlobalGuard::new(Tier1Config::default());
        let state_ref: &std::sync::Mutex<GuardState> = &g.state;
        let pub_ref: &std::sync::Mutex<PubPressure> = &g.published;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = state_ref.lock();
            panic!("poison state");
        }));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = pub_ref.lock();
            panic!("poison published");
        }));

        let _ = g.step(1000.0, true, false, 0.0, false);
        let _ = g.should_shed();
    }
}
