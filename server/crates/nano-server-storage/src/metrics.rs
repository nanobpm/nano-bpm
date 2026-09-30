//! Lightweight Prometheus instrumentation for the durability hot path.
//!
//! Phase 1 deliberately covers only the **journal writer** and the **commit
//! pipeline** — the part of the system whose behaviour we most need to see when
//! tuning group-commit (batch size), partition count, and the linger window. The
//! `commit_batch_size` histogram is the headline metric: it directly answers
//! "how many writes share one fsync?", which throughput numbers can only hint
//! at.
//!
//! Everything here is off the allocation path: metrics are process-global
//! (`LazyLock`), recording is a handful of atomics (histogram `observe`, counter
//! `inc`), and there are **no labels**, so there is no per-event map lookup or
//! string work. The text encoding cost is paid only when `/metrics` is scraped.

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::core::Collector;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};

/// The process-wide metrics registry and the Phase-1 handles.
struct Metrics {
    registry: Registry,
    /// Writes coalesced into each group-commit (one fsync). Batch size ≈ 1 means
    /// the pipeline is serialized upstream and group-commit can't amortize.
    commit_batch_size: Histogram,
    /// Wall time of each `write` + `fsync` group-commit. On macOS `sync_all`
    /// issues `F_FULLFSYNC`, a true media barrier, so this is typically ms-scale.
    fsync_seconds: Histogram,
    /// Wall time of each Raft-log `sync_all()` (append + committed-marker + flusher
    /// barrier). Separate from the varstore `fsync_seconds` above so the recovery
    /// admission throttle can read the *Raft-log* disk-saturation signal directly —
    /// on a failover node this is the fsync that saturates the shared disk.
    raft_fsync_seconds: Histogram,
    /// Snapshot builds performed (one full state-machine serialize + `sync_all`).
    /// Differenced with `raft_snapshot_serialize_seconds`/`raft_snapshot_fsync_seconds`
    /// to attribute the returning-owner recovery notch to snapshot-build IO.
    raft_snapshot_builds_total: IntCounter,
    /// Wall time of the `serde_json` state-machine serialize inside each snapshot
    /// build. On a returning owner with a large resident SM this dominates.
    raft_snapshot_serialize_seconds: Histogram,
    /// Wall time of the snapshot file `sync_all()` inside each build — the media
    /// barrier that contends with the Raft-log fsync path and stalls appends.
    raft_snapshot_fsync_seconds: Histogram,
    /// Serialized bytes of the most recently built snapshot (resident-SM size
    /// proxy: why a returning owner's builds are heavier than a survivor's).
    raft_snapshot_bytes: IntGauge,
    /// Time a caller spends awaiting its commit's durability (queueing behind
    /// other commits + the fsync itself). The closed-loop latency clients feel.
    commit_wait_seconds: Histogram,
    /// Group-commits performed (i.e. number of fsyncs).
    commits_total: IntCounter,
    /// Individual durable writes acknowledged (sum of all batch sizes).
    writes_total: IntCounter,
    /// Journal bytes written (pre-fsync), across all partitions.
    bytes_total: IntCounter,
    /// Writes enqueued but not yet fsynced — the live commit-pipeline depth.
    inflight: IntGauge,
    /// In-flight create-payload bytes in the submit→apply window (the engine
    /// creation-mailbox balloon). Published by the mem-pressure sampler tick; the
    /// byte-aware admission gate sheds creates when this crosses its watermark.
    pipeline_bytes: IntGauge,
    /// Cumulative wall time the writer thread spent blocked in `recv` with no
    /// work (idle). Paired with `writer_busy_seconds`, a delta-scrape gives the
    /// writer's duty cycle: `busy / (busy + idle)`. If idle ≈ 0 the single
    /// writer is saturated and is the hard throughput ceiling.
    writer_idle_seconds: prometheus::Counter,
    /// Cumulative wall time the writer thread spent doing work (drain + linger +
    /// serialize + fsync + ack). The non-fsync remainder (`busy − fsync_sum`) is
    /// the writer's CPU cost; if that dominates, the ceiling is CPU not fsync.
    writer_busy_seconds: prometheus::Counter,

    // ---- Phase 2: falcon and protocol metrics ----
    /// Falcon WebSocket frames processed, by frame type.
    stream_frames_total: prometheus::IntCounterVec,
    /// How many times a streaming client stalled waiting for submission credits.
    stream_credit_stalls_total: IntCounter,
    /// How many times the read-model exporter retried a batch after a transient
    /// store write failure (e.g. SQLite `database is locked`) rather than
    /// dropping it. A climbing value means read-model projection is contending
    /// with the retention pruner / WAL checkpoint; sustained growth is the signal
    /// to widen `busy_timeout` or the exporter-queue budget.
    read_model_export_retries_total: IntCounter,
    /// How many read-model export batches the remote transport dropped after a
    /// PERMANENT delivery failure (a 4xx such as `413 Payload Too Large` or
    /// `400 Bad Request`), where retrying the identical bytes can never succeed
    /// and would head-of-line-block the whole export queue. A non-zero value
    /// means the remote exporter rejected a batch as unacceptable; the node
    /// dropped it (delivery is best-effort in remote mode) rather than wedging.
    read_model_export_drops_total: IntCounter,
    /// Active falcon WebSocket connections.
    stream_connections_active: IntGauge,
    /// Time spent processing each falcon frame (read + apply + reply).
    stream_frame_processing_seconds: Histogram,
    /// Peer raft/app-uplink dial (redial) attempts, by target node and outcome
    /// (`ok`|`fail`). Onset-diagnosis instrument: a surviving node's redial rate
    /// to a *dead* peer quantifies the "wasted work sending to the down node"
    /// (no dead-peer circuit-breaker exists, so every replication attempt to an
    /// unreachable learner redials).
    peer_connect_attempts_total: prometheus::IntCounterVec,
    /// Wall time spent inside `PeerSet::link()` acquiring a peer uplink. Onset
    /// instrument: `link()` awaits the redial `connect()` while holding the global
    /// links mutex, so a slow/black-holed peer head-of-line-blocks *all* peer-link
    /// acquisition — this histogram surfaces that stall (tail inflates when a peer
    /// is down).
    peer_link_seconds: Histogram,

    /// Process instance creates, split by protocol (rest vs stream).
    creates_total: prometheus::IntCounterVec,
    /// Job completions, split by protocol (rest vs stream).
    job_completions_total: prometheus::IntCounterVec,
    /// Ad-hoc sub-process (agentic orchestration) lifecycle events, by kind
    /// (`tool_activation` | `agent_iteration` | `completion` | `cancellation`).
    /// Lets an operator watch an agent loop's shape: tools activated per turn,
    /// iterations taken, and whether containers finished normally or were
    /// cancelled (`cancel_remaining_instances`). See ADR 0023.
    adhoc_events_total: prometheus::IntCounterVec,
    /// Diagnostic: stream CompleteJob outcomes by decision point, to localize a
    /// load-induced completion freeze (route_forward|route_local|leader_reject|
    /// propose_err|apply_err|forward_ok|forward_err).
    stream_complete_outcome_total: prometheus::IntCounterVec,

    /// Per-partition count of leadership self-promotions (a rejoining owner or a
    /// failover peer forming a fresh single-voter group via `promote_partition`).
    /// A healthy cluster promotes each partition ~once; a climbing count under
    /// load is the reclaim promote-ping-pong fingerprint (both the owner and the
    /// failover leader repeatedly re-forming competing groups for one partition).
    raft_promote_total: prometheus::IntCounterVec,

    /// Serialized bytes of all uncompacted Raft log entries currently held in the
    /// in-memory log indexes, summed across every owned partition. Under a burst
    /// this is byte-unbounded (snapshot policy counts entries, not bytes), so it
    /// is a prime suspect for the RSS balloon.
    raft_log_bytes: IntGauge,
    /// Bytes of the retained Raft log tail whose serialized entry is *resident in
    /// RAM* (as opposed to demoted to descriptor-only, read back from its on-disk
    /// segment on demand). With the byte-bounded hot-window cache this is capped at
    /// the RAM budget even when `raft_log_bytes` (the full on-disk tail footprint)
    /// grows under large payloads — the gap is what the adaptation reclaimed.
    raft_log_ram_bytes: IntGauge,
    /// Count of uncompacted Raft log entries in memory across all owned partitions.
    raft_log_entries: IntGauge,
    /// 1 while the node-wide Raft-log fsync-relief window is engaged (a failover
    /// incumbent / returning owner coalescing its `sync`-mode fsyncs during
    /// recovery), else 0. Lets a soak confirm the relief actually engaged.
    raft_fsync_relief_active: IntGauge,
    /// Distribution of a single appended Raft log entry's serialized byte length
    /// (one observation per entry, on every owned partition). A batched entry
    /// carries all coalesced commands' payloads, so this is the payload-size
    /// signal that governs the RSS cost of the retained log tail (see the
    /// entry-count `max_in_snapshot_log_to_keep` floor). Its mean (`_sum/_count`)
    /// and buckets tell whether large (e.g. 50 KB) payloads are a steady
    /// workload before we invest in adaptive spill/compression.
    raft_log_entry_bytes: Histogram,
    /// High-water mark of the largest single Raft log entry appended since start
    /// (never reset). Complements the histogram with the exact observed peak.
    raft_log_entry_bytes_max: IntGauge,
    /// Resident read-model export backlog bytes (forwarded but not yet projected).
    exporter_queue_bytes: IntGauge,
    /// Approx resident instance variable-payload bytes (burst-balloon attribution).
    resident_var_bytes: IntGauge,
    /// Serialized event bytes queued to the journal writer but not yet fsynced+acked.
    journal_inflight_bytes: IntGauge,
    /// Replication-window occupancy: net-live count of `ReplicatedBatch` values
    /// process-wide (see `crate::raft::LIVE_BATCHES`). openraft retains the
    /// recent, non-purged tail of each replicated log in memory (to catch up
    /// lagging replicas without a fresh snapshot install); at idle this plateaus
    /// near `retained_log_streams × KEEP_LOGS`. That retention lives outside the
    /// `RaftLogStore` (which demotes its copies to disk), so it is invisible to
    /// `nanobpm_raft_log_ram_bytes`. Paired with `raft_live_batch_bytes`, this
    /// makes the retention legible as a bounded plateau — accounted replication
    /// state, not a leak.
    raft_live_batches: IntGauge,
    /// Exact serialized byte footprint of the net-live `ReplicatedBatch`
    /// population counted by `raft_live_batches`: each guard carries its batch's
    /// size (see `crate::raft::LIVE_BATCH_BYTES`), so this is the true resident
    /// cost regardless of which structure retains the batches. Dominates
    /// resident heap under fat coalesced payloads yet stays bounded by the keep
    /// window.
    raft_live_batch_bytes: IntGauge,

    // ---- Capacity ceilings (the compressor/limiter LEDs, ADR 0013) ----
    /// The limiter "lit LED": 1 while this node is currently pressed against a
    /// capacity ceiling, else 0, labelled by `ceiling` (`throughput` = the
    /// create-processing concurrency / active-backlog limiter; `memory` = the
    /// always-in-circuit memory-safety rails — create-queue depth, exporter
    /// saturation, in-flight pipeline bytes, resident-memory watermark).
    ceiling_active: prometheus::IntGaugeVec,
    /// Current SLA mode as an info-style gauge: the active `mode` series is `1`,
    /// the other `0` (`mode=latency|admission`). Configurable per node and
    /// switchable at runtime, so every node publishes its own — the console reads
    /// it (self + each scraped peer) to show the mode cluster-wide.
    sla_mode: prometheus::IntGaugeVec,
    /// Cumulative count of ceiling "hits" — incremented on each rising edge
    /// (headroom → at-limit) per `ceiling`. Lets a dashboard show how often the
    /// limiter engaged over a window, like a peak-hold on a gain-reduction meter.
    ceiling_hits_total: prometheus::IntCounterVec,

    // ---- Worker provisioning per job type ----
    /// Activatable (waiting) jobs per `job_type` across all owned partitions —
    /// the depth workers still have to drain.
    job_type_activatable: prometheus::IntGaugeVec,
    /// Live subscribed workers per `job_type` — the Falcon (stream) roster plus
    /// live REST long-poll consumers (`activateJobs`), so a REST-only fleet is not
    /// read as zero workers.
    job_type_workers: prometheus::IntGaugeVec,
    /// Under-provisioning hint per `job_type`: 1 when jobs are waiting but no
    /// worker is subscribed to drain them (hard starvation), else 0. Pair with
    /// `job_type_activatable` / `job_type_workers` to spot soft under-provisioning
    /// (workers present but backlog growing).
    job_type_starved: prometheus::IntGaugeVec,
    /// Cumulative jobs actually dispatched to a worker per `job_type` — the drain
    /// throughput. Counted where a job is delivered to the worker socket (stream)
    /// or returned to the REST client, so peer-pulled jobs are attributed once, at
    /// the gateway that feeds the worker. Delta-scraping gives the per-type drain
    /// rate D; combined with the backlog level + slope and the server-saturation
    /// signals it answers "are workers the bottleneck for this type, and would more
    /// help?" (Little's Law) rather than just "is the backlog growing?".
    job_type_dispatched_total: prometheus::IntCounterVec,
    /// End-to-end job sojourn (create→complete wall latency, seconds) per
    /// `job_type`. This is the user-facing SLA/SLI surface — p50/p90/p99 of how
    /// long a job takes end to end. It is *reporting only*, deliberately NOT a
    /// control input: sojourn is dominated by external/worker service time, so
    /// throttling admission on it would wrongly penalize healthy traffic during a
    /// downstream outage. Read it against the engine's internal command latency
    /// (`nanobpm_backlog_governor{field="window_latency_us"}`): the gap ≈ external
    /// service time, and one job type's sojourn stretching while internal latency
    /// stays flat localizes a slow downstream to that specific type.
    job_sojourn_seconds: prometheus::HistogramVec,
    /// Per-partition Raft liveness alarm: 1 when the partition's openraft core has
    /// entered `Shutdown` (terminated, e.g. on a storage error) and is no longer
    /// applying, else 0. A stuck-at-1 partition strands its share of instances and
    /// jobs — the signal behind the RF>1 completion-freeze. Labelled by partition.
    raft_partition_shutdown: prometheus::IntGaugeVec,

    /// Per-partition engine-actor (deepthi) heartbeat: `1` while the single-writer
    /// thread is alive, `0` the instant it exits/panics. A stuck-at-0 partition
    /// has a DEAD single writer — every create/complete on it hangs forever (the
    /// sustained-load completion-freeze). Labelled by partition.
    actor_alive: prometheus::IntGaugeVec,
    /// Per-partition cumulative count of engine-actor jobs executed. Flat under
    /// any freeze; its delta is the actor's true throughput. Labelled by partition.
    actor_jobs_total: prometheus::IntGaugeVec,
    /// Per-partition elapsed milliseconds of the engine actor's currently-running
    /// job (`0` when idle/parked). An unbounded climb is the signature of a
    /// *wedged* single writer (stuck inside one command); flat-at-0 with a frozen
    /// `actor_jobs_total` means the stall is upstream (idle actor, no work
    /// arriving). Labelled by partition.
    actor_current_job_ms: prometheus::IntGaugeVec,
    /// Per-partition depth of the engine actor's High (completion/read) queue.
    /// Piling up while `actor_jobs_total` is frozen confirms a wedge with work
    /// queued behind it. Labelled by partition.
    actor_hi_depth: prometheus::IntGaugeVec,
    /// Per-partition depth of the engine actor's Low (creation) queue. Labelled by
    /// partition.
    actor_lo_depth: prometheus::IntGaugeVec,

    /// Cumulative count of admission sheds — one per `createProcessInstance`
    /// rejected by [`admission_shed`](crate::AppServer::admission_shed), labelled
    /// by `reason` (which rail tripped: `create_queue`, `active_backlog`,
    /// `create_backlog`, `exporter`, `pipeline_bytes`, `mem_watermark`). Lets a
    /// dashboard confirm that overload is being *shed* rather than silently
    /// accumulated in memory, and which rail is doing the shedding.
    admission_shed_total: prometheus::IntCounterVec,

    // ---- Admission-ceiling input signals (the numbers behind the LED) ----
    /// Live depth of the submitted-but-not-yet-applied create queue — the
    /// create-apply backlog that holds resident memory under an arrival flood and
    /// the `create_queue` / `create_backlog` shed signal. The single most useful
    /// number for "is the engine gathering creates toward OOM?"; plot against
    /// `nanobpm_admission_limit{limit="create_queue"}`.
    pending_create_queue: IntGauge,
    /// Live active-instance backlog (the read-model exporter's projected
    /// `created − completed`) — the `active_backlog` latency-rail signal. Stays ~0
    /// for fast create→complete workloads; climbs when instances park (absent/slow
    /// workers, timers, waiting events). Plot against
    /// `nanobpm_admission_limit{limit="backlog"}`.
    active_backlog: IntGauge,
    /// Cached resident-memory estimate (refreshed by the mem-pressure tick) that the
    /// `mem_watermark` rail keys off. Plot against
    /// `nanobpm_admission_limit{limit="mem_watermark"}`.
    mem_pressure_bytes: IntGauge,
    /// Live runnable (task-job) backlog — the parked-excluded count of
    /// created-but-uncompleted service-task jobs this node holds, refreshed each
    /// ~1 Hz monitor tick. This is the signal the `active_backlog` latency rail
    /// and the self-optimizing backlog governor actually gate on (parked
    /// instances create no jobs, so they never appear here). Plot against
    /// `nanobpm_admission_limit{limit="backlog"}` to watch the governor hold the
    /// runnable backlog at the throughput knee.
    runnable_backlog: IntGauge,
    /// The live per-job-type active dispatch width the worker-concurrency governor
    /// holds the push dispatcher's per-pass subscriber fan-out at (`0` = no cap).
    /// In `WorkerConcurrency::Auto` mode this tracks the governor converging on the
    /// worker concurrency that maximizes completion throughput; watch it against
    /// the subscribed-worker roster to see how much of an over-provisioned fleet is
    /// being parked. Also the value the server advertises to cooperating clients so
    /// their worker pools can self-size.
    active_worker_target: IntGauge,
    /// Drain-stall guard state (`nanobpm_drain_guard`), labelled by `state`
    /// (`metering` = completion-paced create-admission servo engaged; `halted` =
    /// hard safety valve engaged, create admission forced to 0). `1` = engaged. The
    /// create-flood wedge protection (options 3+4); pair with
    /// `nanobpm_drain_completes_per_sec` and `nanobpm_active_backlog` to see the
    /// drain collapse the guard reacted to.
    drain_guard_state: prometheus::IntGaugeVec,
    /// The completion drain throughput (`nanobpm_drain_completes_per_sec`) the
    /// drain-stall guard sampled this tick. Its collapse toward ~0 while the
    /// active backlog rises is the wedge signature the guard trips on.
    drain_completes_per_sec: prometheus::Gauge,
    /// The drain-stall servo's completion-fed create-admission token bucket level
    /// (`nanobpm_drain_credit_budget`). While metering, create submission credits
    /// are granted from this bucket (refilled +1 per completion, capped at the
    /// burst); its floor near 0 means intake is fully paced to the drain.
    drain_credit_budget: prometheus::IntGauge,
    /// The drain-stall servo's completion→create mint ratio in ‰ (parts-per-
    /// thousand), `nanobpm_drain_mint_permille`. `1000` = mint one create token per
    /// completion (intake≈drain, the metering hold); below `1000` while draining an
    /// overshoot down to the setpoint (intake < drain); `0` under the hard valve.
    drain_mint_permille: prometheus::IntGauge,
    /// The configured admission thresholds the ceiling rails trip at, labelled by
    /// `limit` (`backlog`, `create_queue` — counts; `pipeline_bytes`,
    /// `mem_watermark` — bytes; `0` = rail disabled). Reference lines so a dashboard
    /// can show each pressure signal's headroom to its shed point.
    admission_limit: prometheus::IntGaugeVec,
    /// Live state of the auto-mode active-backlog governor, labelled by `field`
    /// (`floor`/`ceiling` — the static AIMD cap bounds in runnable jobs;
    /// `baseline_latency_us`/`window_latency_us` — the self-calibrated baseline and
    /// last-window mean per-command latency the governor tunes on). Explains why
    /// `nanobpm_admission_limit{limit="backlog"}` (the live cap) sits where it
    /// does. Absent in `Fixed`/`Off` backlog modes.
    backlog_governor: prometheus::IntGaugeVec,
    /// ADR-0020 Tier-2 per-process-definition admission pressure, labelled by
    /// `proc` (BPMN process id), in per-mille (0–1000). Non-zero means that
    /// definition's in-flight backlog is accumulating past its end-to-end latency
    /// budget and a paced fraction of its creates is being shed — while healthy
    /// sibling definitions stay at 0. Only pressured definitions are published.
    tier2_pressure: prometheus::IntGaugeVec,
    /// ADR-0020 **Tier-1** global engine-saturation guard pressure in per-mille
    /// (0–1000): the paced fraction of *all* creates shed because the engine's
    /// shared write path (raft-log fsync) has crossed its latency knee. 0 =
    /// healthy write path. A single node-level gauge (no labels).
    tier1_pressure: prometheus::IntGauge,

    /// ADR-0020 Tier-1 export-queue fill signal, `nanobpm_exporter_fill_permille`
    /// (0–1000+): the *least-full* local read-model export shard's queue occupancy
    /// as a per-mille of its adaptive budget — the create-admission input fused into
    /// the Tier-1 guard (`for_create` steers each create to the least-full shard,
    /// so the min governs). 0 = drained / export backpressure unconfigured; ≥1000 =
    /// every shard at budget. A single node-level gauge (no labels).
    exporter_fill_permille: prometheus::IntGauge,

    // ---- Per-command engine-actor profiling (NANOBPM_CMD_PROFILE) ----
    /// Wall time of a single applied [`Command`](nanobpmn_engine_core::Command)
    /// on the engine actor, labelled by `kind`. Its `_count`/`_sum` give the mean
    /// per-command service time; correlate the rise of the mean against active
    /// backlog to test the "congestion collapse is O(active) per command"
    /// hypothesis. Only recorded when `NANOBPM_CMD_PROFILE` is set.
    cmd_seconds: prometheus::HistogramVec,
    /// jemalloc thread-allocated bytes attributed to a single applied command on
    /// the engine actor (the delta of `thread.allocated` across the apply),
    /// labelled by `kind`. THE discriminator for the create/complete collapse:
    /// if per-command *time* rises with active backlog while *alloc bytes/command*
    /// stays flat, the residual cost is hashmap-probe / cache-miss (bigger maps,
    /// no extra allocation); if alloc bytes/command rises, it is allocator/copy
    /// cost. Only recorded when `NANOBPM_CMD_PROFILE` is set.
    cmd_alloc_bytes: prometheus::HistogramVec,
    /// Live engine-state cardinality per partition, labelled by `partition` and
    /// `what` (`instances` = resident process instances, `jobs` = total jobs,
    /// `activated` = leased jobs). The independent variable the per-command
    /// `cmd_seconds`/`cmd_alloc_bytes` means are regressed against to localize the
    /// O(active) term. Sampled ~1 Hz off the hot path.
    engine_cardinality: prometheus::IntGaugeVec,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let registry = Registry::new();

    let commit_batch_size = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_journal_commit_batch_size",
            "Number of writes coalesced into one group-commit (fsync).",
        )
        .buckets(vec![
            1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0,
        ]),
    )
    .expect("valid histogram opts");

    // ~50µs .. 500ms, covering fast Linux fdatasync through slow macOS F_FULLFSYNC.
    let latency_buckets = vec![
        0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5,
    ];

    let fsync_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_journal_fsync_seconds",
            "Wall time of each journal write+fsync group-commit.",
        )
        .buckets(latency_buckets.clone()),
    )
    .expect("valid histogram opts");

    let raft_fsync_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_raft_fsync_seconds",
            "Wall time of each Raft-log sync_all() barrier (append/committed-marker/flusher).",
        )
        .buckets(latency_buckets.clone()),
    )
    .expect("valid histogram opts");

    // Snapshot-build IO timings can reach seconds for a large resident state
    // machine (the returning-owner recovery notch), so give them a wider tail
    // than the sub-second latency_buckets.
    let snapshot_buckets = vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];
    let raft_snapshot_builds_total = IntCounter::new(
        "nanobpm_raft_snapshot_builds_total",
        "Snapshot builds performed (full state-machine serialize + sync_all).",
    )
    .expect("valid counter opts");
    let raft_snapshot_serialize_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_raft_snapshot_serialize_seconds",
            "Wall time of the state-machine serialize inside each snapshot build.",
        )
        .buckets(snapshot_buckets.clone()),
    )
    .expect("valid histogram opts");
    let raft_snapshot_fsync_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_raft_snapshot_fsync_seconds",
            "Wall time of the snapshot file sync_all() inside each build.",
        )
        .buckets(snapshot_buckets),
    )
    .expect("valid histogram opts");
    let raft_snapshot_bytes = IntGauge::new(
        "nanobpm_raft_snapshot_bytes",
        "Serialized bytes of the most recently built snapshot (resident-SM size proxy).",
    )
    .expect("valid gauge opts");

    let commit_wait_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_commit_wait_seconds",
            "Time a caller awaits its commit becoming durable.",
        )
        .buckets(latency_buckets),
    )
    .expect("valid histogram opts");

    let commits_total = IntCounter::new(
        "nanobpm_journal_commits_total",
        "Group-commits (fsyncs) performed.",
    )
    .expect("valid counter");
    let writes_total = IntCounter::new(
        "nanobpm_journal_writes_total",
        "Individual durable writes acknowledged.",
    )
    .expect("valid counter");
    let bytes_total = IntCounter::new(
        "nanobpm_journal_bytes_total",
        "Journal bytes written before fsync.",
    )
    .expect("valid counter");
    let inflight = IntGauge::new(
        "nanobpm_commit_inflight",
        "Durable writes enqueued but not yet fsynced (pipeline depth).",
    )
    .expect("valid gauge");

    let pipeline_bytes = IntGauge::new(
        "nanobpm_pipeline_bytes",
        "In-flight create-payload bytes (engine creation-mailbox balloon).",
    )
    .expect("valid gauge");

    let writer_idle_seconds = prometheus::Counter::new(
        "nanobpm_journal_writer_idle_seconds",
        "Cumulative wall time the journal writer thread was idle (blocked in recv).",
    )
    .expect("valid counter");
    let writer_busy_seconds = prometheus::Counter::new(
        "nanobpm_journal_writer_busy_seconds",
        "Cumulative wall time the journal writer thread was busy (drain+linger+fsync+ack).",
    )
    .expect("valid counter");

    // Phase 2: falcon and protocol metrics
    use prometheus::IntCounterVec;
    use prometheus::Opts;

    let stream_frames_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_stream_frames_total",
            "Falcon frames processed by type.",
        ),
        &["type"],
    )
    .expect("valid counter vec");

    let stream_credit_stalls_total = IntCounter::new(
        "nanobpm_stream_credit_stalls_total",
        "Streaming clients stalled waiting for submission credits.",
    )
    .expect("valid counter");

    let read_model_export_retries_total = IntCounter::new(
        "nanobpm_read_model_export_retries_total",
        "Read-model export batches retried after a transient store write failure \
         (never silently dropped).",
    )
    .expect("valid counter");

    let read_model_export_drops_total = IntCounter::new(
        "nanobpm_read_model_export_drops_total",
        "Read-model export batches dropped by the remote transport after a \
         permanent delivery failure (a 4xx client error such as 413/400) that \
         retrying could never fix; dropped to avoid head-of-line-blocking the \
         export queue (best-effort remote delivery).",
    )
    .expect("valid counter");

    let stream_connections_active = IntGauge::new(
        "nanobpm_stream_connections_active",
        "Active falcon WebSocket connections.",
    )
    .expect("valid gauge");

    let stream_frame_processing_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_stream_frame_processing_seconds",
            "Time to process each falcon frame (read+apply+reply).",
        )
        .buckets(vec![
            0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01,
        ]),
    )
    .expect("valid histogram");

    let creates_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_creates_total",
            "Process instance creates by protocol (rest|stream).",
        ),
        &["protocol"],
    )
    .expect("valid counter vec");

    let peer_connect_attempts_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_peer_connect_attempts_total",
            "Peer uplink dial (redial) attempts, by target node and outcome (ok|fail). \
             Onset-diagnosis: redial rate to a dead peer quantifies wasted send work.",
        ),
        &["target", "outcome"],
    )
    .expect("valid counter vec");

    let peer_link_seconds = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_peer_link_seconds",
            "Wall time inside PeerSet::link() acquiring a peer uplink (the redial \
             connect is awaited under the global links mutex; tail inflates while a \
             peer is down and head-of-line-blocks healthy peers).",
        )
        .buckets(vec![
            0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0,
        ]),
    )
    .expect("valid histogram");

    let job_completions_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_job_completions_total",
            "Job completions by protocol (rest|stream).",
        ),
        &["protocol"],
    )
    .expect("valid counter vec");

    let adhoc_events_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_adhoc_events_total",
            "Ad-hoc sub-process lifecycle events by kind \
             (tool_activation|agent_iteration|completion|cancellation).",
        ),
        &["kind"],
    )
    .expect("valid counter vec");

    let stream_complete_outcome_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_stream_complete_outcome_total",
            "Stream CompleteJob outcomes by decision point (diagnostic).",
        ),
        &["outcome"],
    )
    .expect("valid counter vec");

    let raft_promote_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_raft_promote_total",
            "Per-partition leadership self-promotions (fresh single-voter group formed via promote_partition); a climbing count is the reclaim promote-ping-pong fingerprint.",
        ),
        &["partition"],
    )
    .expect("valid counter vec");

    let raft_log_bytes = IntGauge::new(
        "nanobpm_raft_log_bytes",
        "Serialized bytes of uncompacted in-memory Raft log entries (all partitions).",
    )
    .expect("valid gauge");
    let raft_log_ram_bytes = IntGauge::new(
        "nanobpm_raft_log_ram_bytes",
        "Serialized bytes of retained Raft log entries resident in RAM (all partitions); \
         capped by the hot-window RAM budget, the rest read from disk on demand.",
    )
    .expect("valid gauge");
    let raft_log_entries = IntGauge::new(
        "nanobpm_raft_log_entries",
        "Uncompacted in-memory Raft log entries (all partitions).",
    )
    .expect("valid gauge");
    let raft_fsync_relief_active = IntGauge::new(
        "nanobpm_raft_fsync_relief_active",
        "1 while the Raft-log fsync-relief window is engaged during recovery, else 0.",
    )
    .expect("valid gauge");
    // Per-entry serialized size. Buckets span 64 B .. ~256 MB (exp base 4) to
    // cover negligible batched creates through very large variable payloads.
    let raft_log_entry_bytes = Histogram::with_opts(
        HistogramOpts::new(
            "nanobpm_raft_log_entry_bytes",
            "Serialized byte length of a single appended Raft log entry (one observation per entry, all partitions).",
        )
        .buckets(prometheus::exponential_buckets(64.0, 4.0, 12).expect("valid buckets")),
    )
    .expect("valid histogram");
    let raft_log_entry_bytes_max = IntGauge::new(
        "nanobpm_raft_log_entry_bytes_max",
        "Largest single Raft log entry (serialized bytes) appended since start (high-water mark).",
    )
    .expect("valid gauge");

    let exporter_queue_bytes = IntGauge::new(
        "nanobpm_exporter_queue_bytes",
        "Resident read-model export backlog bytes (events forwarded but not yet projected, all shards).",
    )
    .expect("valid gauge");

    let resident_var_bytes = IntGauge::new(
        "nanobpm_resident_var_bytes",
        "Approx resident instance variable-payload bytes across all local partitions (burst-balloon attribution).",
    )
    .expect("valid gauge");

    let raft_live_batches = IntGauge::new(
        "nanobpm_raft_live_batches",
        "Replication-window occupancy: net-live ReplicatedBatch values process-wide (constructed/deserialized/cloned minus dropped). openraft keeps the recent non-purged log tail in memory to catch up lagging replicas; plateaus near retained_log_streams*KEEP_LOGS. Bounded, accounted replication state (not a leak); invisible to nanobpm_raft_log_ram_bytes.",
    )
    .expect("valid gauge");

    let raft_live_batch_bytes = IntGauge::new(
        "nanobpm_raft_live_batch_bytes",
        "Exact serialized byte footprint of all net-live ReplicatedBatch values process-wide (each guard carries its batch's size, summed on construct/clone minus drop). The byte magnitude of the replication-window retention counted by nanobpm_raft_live_batches; dominates resident heap under fat coalesced payloads yet stays bounded by the keep window. Retention lives in openraft in-memory state, outside nanobpm_raft_log_ram_bytes.",
    )
    .expect("valid gauge");

    let journal_inflight_bytes = IntGauge::new(
        "nanobpm_journal_inflight_bytes",
        "Serialized event bytes queued to the background journal writer but not yet fsynced+acked (engine->writer in-flight; events_arc roughly doubles the true heap).",
    )
    .expect("valid gauge");

    let ceiling_active = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_ceiling_active",
            "Capacity-ceiling LED: 1 while pressed against the limit, else 0 (ceiling=throughput|memory|exporter|flow_control).",
        ),
        &["ceiling"],
    )
    .expect("valid gauge vec");

    let sla_mode = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_sla_mode",
            "Active SLA mode (info gauge): 1 for the current mode, 0 otherwise (mode=latency|admission).",
        ),
        &["mode"],
    )
    .expect("valid gauge vec");

    let ceiling_hits_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_ceiling_hits_total",
            "Rising-edge count of capacity-ceiling hits (ceiling=throughput|memory|exporter|flow_control).",
        ),
        &["ceiling"],
    )
    .expect("valid counter vec");

    let job_type_activatable = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_job_type_activatable",
            "Activatable (waiting) jobs per job type across all owned partitions.",
        ),
        &["job_type"],
    )
    .expect("valid gauge vec");

    let job_type_workers = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_job_type_workers",
            "Live subscribed workers per job type (Falcon stream roster plus live REST long-poll consumers).",
        ),
        &["job_type"],
    )
    .expect("valid gauge vec");

    let job_type_starved = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_job_type_starved",
            "Worker under-provisioning hint: 1 when jobs are waiting but no worker is subscribed to drain them, else 0.",
        ),
        &["job_type"],
    )
    .expect("valid gauge vec");

    let job_type_dispatched_total = IntCounterVec::new(
        Opts::new(
            "nanobpm_job_type_dispatched_total",
            "Cumulative jobs dispatched to a worker per job type (the drain throughput); delta-scrape for the per-type drain rate.",
        ),
        &["job_type"],
    )
    .expect("valid counter vec");

    let job_sojourn_seconds = prometheus::HistogramVec::new(
        HistogramOpts::new(
            "nanobpm_job_sojourn_seconds",
            "End-to-end job sojourn (create->complete) per job type — the user-facing SLA/SLI. Reporting only, not a control signal.",
        )
        .buckets(prometheus::exponential_buckets(0.005, 3.0, 12).expect("valid buckets")),
        &["job_type"],
    )
    .expect("valid histogram vec");

    let raft_partition_shutdown = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_raft_partition_shutdown",
            "1 when a partition's Raft core has entered Shutdown (terminated, no longer applying) and is stranding its jobs/instances, else 0.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_alive = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_alive",
            "1 while a partition's engine-actor (single-writer) thread is alive; 0 the instant it exits/panics (a dead single writer freezes all completions on that partition).",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_jobs_total = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_jobs_total",
            "Cumulative engine-actor jobs executed per partition; flat under any completion-freeze, its delta is the actor's true throughput.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_current_job_ms = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_current_job_ms",
            "Elapsed milliseconds of the engine actor's currently-running job per partition (0 = idle). An unbounded climb is a wedged single writer.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_hi_depth = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_hi_depth",
            "Depth of the engine actor's High (completion/read) queue per partition.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let actor_lo_depth = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_actor_lo_depth",
            "Depth of the engine actor's Low (creation) queue per partition.",
        ),
        &["partition"],
    )
    .expect("valid gauge vec");
    let admission_shed_total = prometheus::IntCounterVec::new(
        Opts::new(
            "nanobpm_admission_shed_total",
            "Cumulative createProcessInstance sheds by admission control, labelled by the rail that tripped.",
        ),
        &["reason"],
    )
    .expect("valid counter vec");

    let pending_create_queue = IntGauge::new(
        "nanobpm_pending_create_queue",
        "Submitted-but-not-yet-applied create-queue depth: the create-apply backlog that holds resident memory under an arrival flood (the create_queue/create_backlog shed signal).",
    )
    .expect("valid gauge");
    let active_backlog = IntGauge::new(
        "nanobpm_active_backlog",
        "Active-instance backlog (exporter-projected created-minus-completed); the active_backlog latency-rail signal. ~0 for fast create->complete; climbs when instances park (absent/slow workers).",
    )
    .expect("valid gauge");
    let mem_pressure_bytes = IntGauge::new(
        "nanobpm_mem_pressure_bytes",
        "Cached resident-memory estimate the mem_watermark admission rail keys off.",
    )
    .expect("valid gauge");
    let runnable_backlog = IntGauge::new(
        "nanobpm_runnable_backlog",
        "Runnable (task-job) backlog: parked-excluded count of created-but-uncompleted service-task jobs this node holds; the signal the active_backlog rail and the backlog governor gate on.",
    )
    .expect("valid gauge");
    let active_worker_target = IntGauge::new(
        "nanobpm_active_worker_target",
        "Worker-concurrency governor's active dispatch width: max subscribers the push dispatcher fans each job type out to per pass (0 = no cap). Tracks the governor converging on the worker concurrency that maximizes completion throughput; also advertised to clients for self-sizing.",
    )
    .expect("valid gauge");
    let drain_guard_state = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_drain_guard",
            "Drain-stall guard state (state=metering|halted; 1=engaged). Create-flood wedge protection: metering = completion-paced create-admission servo (submission credits granted from a completion-fed token bucket); halted = hard safety valve (drain stalled ~0/s with a large backlog held) forcing create admission to 0.",
        ),
        &["state"],
    )
    .expect("valid gauge vec");
    let drain_completes_per_sec = prometheus::Gauge::new(
        "nanobpm_drain_completes_per_sec",
        "Completion drain throughput (completes/s) the drain-stall guard sampled this tick; its collapse toward ~0 while active_backlog rises is the wedge signature.",
    )
    .expect("valid gauge");
    let drain_credit_budget = prometheus::IntGauge::new(
        "nanobpm_drain_credit_budget",
        "Drain-stall servo token-bucket level: create submission credits available to grant. Refilled +1 per completion (capped at the burst); while metering, a floor near 0 means intake is fully paced to the completion drain.",
    )
    .expect("valid gauge");
    let drain_mint_permille = prometheus::IntGauge::new(
        "nanobpm_drain_mint_permille",
        "Drain-stall servo completion→create mint ratio in per-thousand: 1000 = one create token per completion (intake≈drain hold); below 1000 while draining an overshoot toward the setpoint (intake<drain); 0 under the hard valve.",
    )
    .expect("valid gauge");
    let admission_limit = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_admission_limit",
            "Configured admission shed thresholds (limit=backlog|create_queue are counts; pipeline_bytes|mem_watermark are bytes; 0 = disabled). Reference lines for each pressure signal's headroom.",
        ),
        &["limit"],
    )
    .expect("valid gauge vec");
    let backlog_governor = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_backlog_governor",
            "Auto-mode active-backlog governor live state (field=floor|ceiling are runnable-job cap bounds; rho_permille|rho_target_permille are engine-actor saturation ρ and its setpoint in per-mille; growth_per_s is the signed runnable-backlog growth rate). Explains where the governor holds nanobpm_admission_limit{limit=\"backlog\"}.",
        ),
        &["field"],
    )
    .expect("valid gauge vec");
    let tier2_pressure = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_tier2_pressure",
            "ADR-0020 Tier-2 per-process-definition admission pressure in per-mille (0-1000), labelled by proc (BPMN process id). Non-zero = that definition is accumulating in-flight backlog past its e2e latency budget and a paced fraction of its creates is shed; healthy siblings stay 0.",
        ),
        &["proc"],
    )
    .expect("valid gauge vec");

    let tier1_pressure = prometheus::IntGauge::new(
        "nanobpm_tier1_pressure",
        "ADR-0020 Tier-1 global engine-saturation guard pressure in per-mille (0-1000): the paced fraction of all creates shed because the engine's shared write path (raft-log fsync) crossed its latency knee. 0 = healthy write path.",
    )
    .expect("valid gauge");

    let exporter_fill_permille = prometheus::IntGauge::new(
        "nanobpm_exporter_fill_permille",
        "ADR-0020 Tier-1 export-queue fill in per-mille (0-1000+): the least-full local read-model export shard's queue occupancy as a fraction of its adaptive budget (the min governs because for_create steers to the least-full shard). 0 = drained / export backpressure unconfigured; >=1000 = every shard at budget.",
    )
    .expect("valid gauge");

    let cmd_seconds = prometheus::HistogramVec::new(
        HistogramOpts::new(
            "nanobpm_cmd_seconds",
            "Wall time of a single applied engine command on the actor, by kind (NANOBPM_CMD_PROFILE).",
        )
        .buckets(prometheus::exponential_buckets(0.000001, 4.0, 13).expect("valid buckets")),
        &["kind"],
    )
    .expect("valid histogram vec");
    let cmd_alloc_bytes = prometheus::HistogramVec::new(
        HistogramOpts::new(
            "nanobpm_cmd_alloc_bytes",
            "jemalloc thread-allocated bytes attributed to a single applied engine command, by kind (NANOBPM_CMD_PROFILE).",
        )
        .buckets(prometheus::exponential_buckets(64.0, 4.0, 12).expect("valid buckets")),
        &["kind"],
    )
    .expect("valid histogram vec");
    let engine_cardinality = prometheus::IntGaugeVec::new(
        Opts::new(
            "nanobpm_engine_cardinality",
            "Live engine-state cardinality per partition (what=instances|jobs|activated); the independent variable the per-command cost is regressed against to localize the O(active) term.",
        ),
        &["partition", "what"],
    )
    .expect("valid gauge vec");

    registry
        .register(Box::new(commit_batch_size.clone()))
        .and(registry.register(Box::new(fsync_seconds.clone())))
        .and(registry.register(Box::new(raft_fsync_seconds.clone())))
        .and(registry.register(Box::new(raft_snapshot_builds_total.clone())))
        .and(registry.register(Box::new(raft_snapshot_serialize_seconds.clone())))
        .and(registry.register(Box::new(raft_snapshot_fsync_seconds.clone())))
        .and(registry.register(Box::new(raft_snapshot_bytes.clone())))
        .and(registry.register(Box::new(commit_wait_seconds.clone())))
        .and(registry.register(Box::new(commits_total.clone())))
        .and(registry.register(Box::new(writes_total.clone())))
        .and(registry.register(Box::new(bytes_total.clone())))
        .and(registry.register(Box::new(inflight.clone())))
        .and(registry.register(Box::new(pipeline_bytes.clone())))
        .and(registry.register(Box::new(writer_idle_seconds.clone())))
        .and(registry.register(Box::new(writer_busy_seconds.clone())))
        .and(registry.register(Box::new(stream_frames_total.clone())))
        .and(registry.register(Box::new(stream_credit_stalls_total.clone())))
        .and(registry.register(Box::new(read_model_export_retries_total.clone())))
        .and(registry.register(Box::new(read_model_export_drops_total.clone())))
        .and(registry.register(Box::new(stream_connections_active.clone())))
        .and(registry.register(Box::new(stream_frame_processing_seconds.clone())))
        .and(registry.register(Box::new(peer_connect_attempts_total.clone())))
        .and(registry.register(Box::new(peer_link_seconds.clone())))
        .and(registry.register(Box::new(creates_total.clone())))
        .and(registry.register(Box::new(job_completions_total.clone())))
        .and(registry.register(Box::new(adhoc_events_total.clone())))
        .and(registry.register(Box::new(stream_complete_outcome_total.clone())))
        .and(registry.register(Box::new(raft_promote_total.clone())))
        .and(registry.register(Box::new(raft_log_bytes.clone())))
        .and(registry.register(Box::new(raft_log_ram_bytes.clone())))
        .and(registry.register(Box::new(raft_log_entries.clone())))
        .and(registry.register(Box::new(raft_fsync_relief_active.clone())))
        .and(registry.register(Box::new(raft_log_entry_bytes.clone())))
        .and(registry.register(Box::new(raft_log_entry_bytes_max.clone())))
        .and(registry.register(Box::new(exporter_queue_bytes.clone())))
        .and(registry.register(Box::new(resident_var_bytes.clone())))
        .and(registry.register(Box::new(journal_inflight_bytes.clone())))
        .and(registry.register(Box::new(raft_live_batches.clone())))
        .and(registry.register(Box::new(raft_live_batch_bytes.clone())))
        .and(registry.register(Box::new(ceiling_active.clone())))
        .and(registry.register(Box::new(sla_mode.clone())))
        .and(registry.register(Box::new(ceiling_hits_total.clone())))
        .and(registry.register(Box::new(job_type_activatable.clone())))
        .and(registry.register(Box::new(job_type_workers.clone())))
        .and(registry.register(Box::new(job_type_starved.clone())))
        .and(registry.register(Box::new(job_type_dispatched_total.clone())))
        .and(registry.register(Box::new(job_sojourn_seconds.clone())))
        .and(registry.register(Box::new(raft_partition_shutdown.clone())))
        .and(registry.register(Box::new(actor_alive.clone())))
        .and(registry.register(Box::new(actor_jobs_total.clone())))
        .and(registry.register(Box::new(actor_current_job_ms.clone())))
        .and(registry.register(Box::new(actor_hi_depth.clone())))
        .and(registry.register(Box::new(actor_lo_depth.clone())))
        .and(registry.register(Box::new(admission_shed_total.clone())))
        .and(registry.register(Box::new(pending_create_queue.clone())))
        .and(registry.register(Box::new(active_backlog.clone())))
        .and(registry.register(Box::new(mem_pressure_bytes.clone())))
        .and(registry.register(Box::new(runnable_backlog.clone())))
        .and(registry.register(Box::new(active_worker_target.clone())))
        .and(registry.register(Box::new(drain_guard_state.clone())))
        .and(registry.register(Box::new(drain_completes_per_sec.clone())))
        .and(registry.register(Box::new(drain_credit_budget.clone())))
        .and(registry.register(Box::new(drain_mint_permille.clone())))
        .and(registry.register(Box::new(admission_limit.clone())))
        .and(registry.register(Box::new(backlog_governor.clone())))
        .and(registry.register(Box::new(tier2_pressure.clone())))
        .and(registry.register(Box::new(tier1_pressure.clone())))
        .and(registry.register(Box::new(exporter_fill_permille.clone())))
        .and(registry.register(Box::new(cmd_seconds.clone())))
        .and(registry.register(Box::new(cmd_alloc_bytes.clone())))
        .and(registry.register(Box::new(engine_cardinality.clone())))
        .expect("register metrics");

    Metrics {
        registry,
        commit_batch_size,
        fsync_seconds,
        raft_fsync_seconds,
        raft_snapshot_builds_total,
        raft_snapshot_serialize_seconds,
        raft_snapshot_fsync_seconds,
        raft_snapshot_bytes,
        commit_wait_seconds,
        commits_total,
        writes_total,
        bytes_total,
        inflight,
        pipeline_bytes,
        writer_idle_seconds,
        writer_busy_seconds,
        stream_frames_total,
        stream_credit_stalls_total,
        read_model_export_retries_total,
        read_model_export_drops_total,
        stream_connections_active,
        stream_frame_processing_seconds,
        peer_connect_attempts_total,
        peer_link_seconds,
        creates_total,
        job_completions_total,
        adhoc_events_total,
        stream_complete_outcome_total,
        raft_promote_total,
        raft_log_bytes,
        raft_log_ram_bytes,
        raft_log_entries,
        raft_fsync_relief_active,
        raft_log_entry_bytes,
        raft_log_entry_bytes_max,
        exporter_queue_bytes,
        resident_var_bytes,
        journal_inflight_bytes,
        raft_live_batches,
        raft_live_batch_bytes,
        ceiling_active,
        sla_mode,
        ceiling_hits_total,
        job_type_activatable,
        job_type_workers,
        job_type_starved,
        job_type_dispatched_total,
        job_sojourn_seconds,
        raft_partition_shutdown,
        actor_alive,
        actor_jobs_total,
        actor_current_job_ms,
        actor_hi_depth,
        actor_lo_depth,
        admission_shed_total,
        pending_create_queue,
        active_backlog,
        mem_pressure_bytes,
        runnable_backlog,
        active_worker_target,
        drain_guard_state,
        drain_completes_per_sec,
        drain_credit_budget,
        drain_mint_permille,
        admission_limit,
        backlog_governor,
        tier2_pressure,
        tier1_pressure,
        exporter_fill_permille,
        cmd_seconds,
        cmd_alloc_bytes,
        engine_cardinality,
    }
});

/// Records one completed group-commit: its batch size, fsync duration, and bytes.
pub fn record_commit(batch_size: usize, fsync: Duration, bytes: usize) {
    record_write_batch(batch_size, bytes);
    record_fsync(batch_size, fsync);
}

/// Records a batch of durable writes appended (write_all) but not necessarily yet
/// fsynced. Used by the async-durability writer, which acks after the append and
/// fsyncs on a separate cadence. `record_commit` delegates here for the bytes/
/// writes counters.
pub fn record_write_batch(writes: usize, bytes: usize) {
    let m = &*METRICS;
    m.writes_total.inc_by(writes as u64);
    m.bytes_total.inc_by(bytes as u64);
}

/// Records one fsync (group-commit barrier): the number of writes it made durable
/// and its wall time. In sync mode `writes` == the batch; in async mode it is all
/// writes appended since the previous fsync.
pub fn record_fsync(writes: usize, fsync: Duration) {
    let m = &*METRICS;
    m.commit_batch_size.observe(writes as f64);
    m.fsync_seconds.observe(fsync.as_secs_f64());
    m.commits_total.inc();
}

/// Records how long a caller waited for its commit to become durable.
pub fn record_commit_wait(wait: Duration) {
    METRICS.commit_wait_seconds.observe(wait.as_secs_f64());
}

/// Records the wall time of one Raft-log `sync_all()` barrier. The recovery
/// admission throttle reads the windowed mean of this (see
/// [`raft_fsync_sum_count`]) to detect disk saturation on a failover node.
pub fn observe_raft_fsync(dur: Duration) {
    METRICS.raft_fsync_seconds.observe(dur.as_secs_f64());
}

/// Cumulative (sum_seconds, count) of Raft-log fsync barriers since boot. The
/// monitor tick differences these across ticks to get the window mean fsync
/// latency that drives the recovery admission throttle.
pub fn raft_fsync_sum_count() -> (f64, u64) {
    let h = &METRICS.raft_fsync_seconds;
    (h.get_sample_sum(), h.get_sample_count())
}

/// Records one snapshot build: the state-machine serialize time, the file
/// `sync_all()` time, and the serialized byte size. Lets a bounce soak attribute
/// the returning-owner recovery notch to snapshot-build IO contention (a large
/// resident SM makes the serialize + fsync stall the shared Raft-log fsync path).
pub fn observe_snapshot_build(serialize: Duration, fsync: Duration, bytes: u64) {
    METRICS.raft_snapshot_builds_total.inc();
    METRICS
        .raft_snapshot_serialize_seconds
        .observe(serialize.as_secs_f64());
    METRICS
        .raft_snapshot_fsync_seconds
        .observe(fsync.as_secs_f64());
    METRICS.raft_snapshot_bytes.set(bytes as i64);
}

/// Serialized bytes of the most recently built snapshot — a proxy for the resident
/// state-machine size. Used to detect the "large SM" window (a returning owner
/// draining a deep reclaim backlog, or a failover incumbent) that makes snapshot
/// builds expensive, so the build-concurrency limiter + cadence stretch engage.
pub fn last_snapshot_bytes() -> i64 {
    METRICS.raft_snapshot_bytes.get()
}

/// A durable write was enqueued (pipeline depth +1).
pub fn inflight_inc() {
    METRICS.inflight.inc();
}

/// `n` durable writes were fsynced and acknowledged (pipeline depth -n).
pub fn inflight_sub(n: usize) {
    METRICS.inflight.sub(n as i64);
}

/// Publishes the current in-flight create-payload byte gauge (called from the
/// mem-pressure sampler tick, off the hot path).
pub fn set_pipeline_bytes(bytes: u64) {
    METRICS.pipeline_bytes.set(bytes as i64);
}

/// Adjusts the aggregate in-memory Raft-log gauges by a signed delta. Each
/// partition's [`RaftLogStore`](crate::raft_logstore::RaftLogStore) reports the
/// change to its own in-memory index on append/purge/truncate; the gauges sum
/// across partitions to expose the total live Raft-log footprint.
pub fn raft_log_delta(entries_delta: i64, bytes_delta: i64) {
    METRICS.raft_log_entries.add(entries_delta);
    METRICS.raft_log_bytes.add(bytes_delta);
}

/// Sets the Raft-log fsync-relief gauge (1 = engaged during recovery, 0 = off).
/// Driven by the recovery supervisor as it toggles the process-global relief flag.
pub fn set_raft_fsync_relief(active: bool) {
    METRICS
        .raft_fsync_relief_active
        .set(if active { 1 } else { 0 });
}

/// Adjusts the aggregate resident (in-RAM) Raft-log byte gauge by a signed delta.
/// Called by each partition's log store when entries are appended (positive),
/// demoted to descriptor-only (negative), rehydrated (positive), or dropped by
/// purge/truncate (negative). The gap between `raft_log_bytes` (full tail) and
/// `raft_log_ram_bytes` (resident) is the RAM the hot-window cache reclaimed.
pub fn raft_log_ram_delta(bytes_delta: i64) {
    METRICS.raft_log_ram_bytes.add(bytes_delta);
}

/// Records one appended Raft log entry's serialized byte length: observes the
/// size distribution and advances the peak high-water mark. Called once per entry
/// from the log store's `append` (the `len` is already computed there, so this is
/// free of extra serialization). Diagnostic-only; drives the decision on whether
/// large payloads warrant adaptive log-tail spill/compression.
pub fn raft_log_entry_appended(len: usize) {
    let len = len as i64;
    METRICS.raft_log_entry_bytes.observe(len as f64);
    if len > METRICS.raft_log_entry_bytes_max.get() {
        METRICS.raft_log_entry_bytes_max.set(len);
    }
}

/// Publishes the aggregate resident read-model export-backlog byte gauge (summed
/// across shards), sampled off the hot path to attribute the in-flight pipeline
/// share of the RSS balloon.
pub fn set_exporter_queue_bytes(bytes: u64) {
    METRICS.exporter_queue_bytes.set(bytes as i64);
}

/// Publishes the aggregate resident instance variable-payload byte gauge (summed
/// across local partitions), sampled off the hot path. The decisive attribution
/// gauge for the burst RSS balloon: compare its peak against
/// `nanobpm_jemalloc_bytes{kind="allocated"}` — a match means the resident
/// instance variables ARE the balloon, a large shortfall means the balloon is
/// in-flight pipeline copies, not resident variables.
pub fn set_resident_var_bytes(bytes: u64) {
    METRICS.resident_var_bytes.set(bytes as i64);
}

/// Publishes the current net-live `ReplicatedBatch` count (see
/// `crate::raft::LIVE_BATCHES`) to `nanobpm_raft_live_batches`. Called from the
/// periodic metrics tick.
pub fn set_raft_live_batches(n: i64) {
    METRICS.raft_live_batches.set(n);
}

/// Publishes the current net-live `ReplicatedBatch` byte footprint (see
/// `crate::raft::LIVE_BATCH_BYTES`) to `nanobpm_raft_live_batch_bytes`. Called
/// from the periodic metrics tick.
pub fn set_raft_live_batch_bytes(n: i64) {
    METRICS.raft_live_batch_bytes.set(n);
}

/// `n` serialized event bytes were handed to the journal writer (in-flight +n).
pub fn journal_inflight_add(n: usize) {
    METRICS.journal_inflight_bytes.add(n as i64);
}

/// `n` serialized event bytes were fsynced+acked by the journal writer (-n). The
/// engine->writer in-flight byte gauge; a WHERE-in-the-pipeline attribution for
/// the burst RSS balloon (does the journal write backlog hold the ~12 GB?).
pub fn journal_inflight_sub(n: usize) {
    METRICS.journal_inflight_bytes.sub(n as i64);
}

/// Sets the capacity-ceiling LED for `ceiling` ("throughput"|"memory") and, on
/// a rising edge (previously below the limit, now at it), bumps its hit counter.
/// `previously_active` is the gauge value from the prior monitor tick; the caller
/// threads it so the rising-edge detection needs no extra state read.
pub fn set_ceiling_active(ceiling: &str, active: bool, previously_active: bool) {
    METRICS
        .ceiling_active
        .with_label_values(&[ceiling])
        .set(i64::from(active));
    if active && !previously_active {
        METRICS
            .ceiling_hits_total
            .with_label_values(&[ceiling])
            .inc();
    }
}

/// Publishes the current SLA mode as the `nanobpm_sla_mode` info gauge: sets the
/// active mode's series to `1` and the other to `0`. Idempotent and cheap;
/// called from the ~1 Hz monitor tick (and once at startup) so the exported mode
/// always reflects the latest runtime switch. `mode` is [`SlaMode::as_str`]
/// (`latency` | `admission`); any other value is treated as `latency`.
pub fn set_sla_mode(mode: &str) {
    let admission = mode == "admission";
    METRICS
        .sla_mode
        .with_label_values(&["admission"])
        .set(i64::from(admission));
    METRICS
        .sla_mode
        .with_label_values(&["latency"])
        .set(i64::from(!admission));
}

/// Publishes the admission-ceiling input signals — the raw numbers behind the
/// `nanobpm_ceiling_active` LED: the create-apply queue depth, the active-instance
/// backlog, and the resident-memory estimate. Called ~1 Hz from the monitor loop,
/// off the hot path. Pair each with its `nanobpm_admission_limit` reference line to
/// watch pressure climb toward (and headroom shrink to) the shed point.
pub fn set_admission_signals(
    pending_create_queue: i64,
    active_backlog: i64,
    mem_pressure_bytes: i64,
    runnable_backlog: i64,
) {
    METRICS.pending_create_queue.set(pending_create_queue);
    METRICS.active_backlog.set(active_backlog);
    METRICS.mem_pressure_bytes.set(mem_pressure_bytes);
    METRICS.runnable_backlog.set(runnable_backlog);
}

/// Publishes the worker-concurrency governor's live active dispatch width
/// (`nanobpm_active_worker_target`; `0` = no cap). Called ~1 Hz from the monitor
/// loop. Pair with the subscribed-worker roster to see how much of the fleet the
/// governor is parking, and export to cooperating clients for self-sizing.
pub fn set_active_worker_target(width: i64) {
    METRICS.active_worker_target.set(width);
}

/// Publishes the drain-stall guard state and the sampled drain throughput
/// (`nanobpm_drain_guard{state}` + `nanobpm_drain_completes_per_sec` +
/// `nanobpm_drain_credit_budget`). Called ~1 Hz from the monitor supervisor, off
/// the hot path.
pub fn set_drain_guard(metering: bool, halted: bool, completes_per_sec: f64, credit_budget: i64) {
    METRICS
        .drain_guard_state
        .with_label_values(&["metering"])
        .set(i64::from(metering));
    METRICS
        .drain_guard_state
        .with_label_values(&["halted"])
        .set(i64::from(halted));
    METRICS.drain_completes_per_sec.set(completes_per_sec);
    METRICS.drain_credit_budget.set(credit_budget);
}

/// Publishes the drain-stall servo's completion→create mint ratio (‰) this tick
/// (`nanobpm_drain_mint_permille`). Called ~1 Hz from the monitor supervisor.
pub fn set_drain_mint_permille(permille: i64) {
    METRICS.drain_mint_permille.set(permille);
}

/// Publishes one configured admission threshold as a reference line
/// (`backlog`/`create_queue` are counts, `pipeline_bytes`/`mem_watermark` are
/// bytes; `0` = that rail is disabled).
pub fn set_admission_limit(limit: &str, value: i64) {
    METRICS
        .admission_limit
        .with_label_values(&[limit])
        .set(value);
}

/// Publishes one field of the auto-mode active-backlog governor's live state,
/// labelled by `field` (`floor`/`ceiling` = the static AIMD cap bounds in runnable
/// jobs; `baseline_latency_us` = the self-calibrated uncongested per-command
/// latency the congestion threshold derives from; `window_latency_us` = the most
/// recent window's mean per-command latency). Together with
/// `nanobpm_admission_limit{limit="backlog"}` (the live cap) these explain *why*
/// the governor is holding the cap where it is. Only emitted in `Auto` mode.
pub fn set_backlog_governor(field: &str, value: i64) {
    METRICS
        .backlog_governor
        .with_label_values(&[field])
        .set(value);
}

/// Publishes one process definition's ADR-0020 Tier-2 admission pressure in
/// per-mille (`nanobpm_tier2_pressure{proc=...}`), the paced shed fraction the
/// admission gate applies to that definition's creates. Only pressured
/// definitions are emitted (healthy siblings are absent / implicitly 0).
pub fn set_tier2_pressure(proc: &str, permille: i64) {
    METRICS
        .tier2_pressure
        .with_label_values(&[proc])
        .set(permille);
}

/// Publishes the ADR-0020 Tier-1 global engine-saturation guard pressure in
/// per-mille (`nanobpm_tier1_pressure`), the paced shed fraction the admission
/// gate applies to *all* creates when the shared write path (raft-log fsync) is
/// saturated. 0 = healthy.
pub fn set_tier1_pressure(permille: i64) {
    METRICS.tier1_pressure.set(permille);
}

/// Publishes the ADR-0020 Tier-1 export-queue fill signal in per-mille
/// (`nanobpm_exporter_fill_permille`) — the least-full local read-model export
/// shard's queue occupancy as a fraction of its adaptive budget, fused into the
/// Tier-1 guard. 0 = drained / export backpressure unconfigured.
pub fn set_exporter_fill_permille(permille: i64) {
    METRICS.exporter_fill_permille.set(permille);
}

/// Publishes the per-job-type worker-provisioning gauges: waiting jobs, live
/// subscribed workers, and the hard-starvation hint (waiting jobs but no worker).
pub fn set_job_type_provisioning(job_type: &str, activatable: i64, workers: i64) {
    METRICS
        .job_type_activatable
        .with_label_values(&[job_type])
        .set(activatable);
    METRICS
        .job_type_workers
        .with_label_values(&[job_type])
        .set(workers);
    let starved = i64::from(activatable > 0 && workers == 0);
    METRICS
        .job_type_starved
        .with_label_values(&[job_type])
        .set(starved);
}

/// Removes every per-`job_type` provisioning series (`activatable` / `workers` /
/// `starved`) for a type that has left the active set, rather than merely zeroing
/// it. `with_label_values` *creates* a child the first time a label is seen, so
/// zeroing alone leaves the series in the registry forever: because `activateJobs`
/// is unauthenticated, a client rotating unique job types would otherwise grow the
/// registry (and every `/metrics` scrape) without bound. The caller still zeroes a
/// disappeared type for one tick *before* removing it (so a scrape racing the
/// removal never sees a stale non-zero value), then calls this to free the label.
///
/// A type that reappears simply re-creates its series on the next
/// [`set_job_type_provisioning`] — removal is not a tombstone. `remove_label_values`
/// only fails when the label set is absent (already removed), which is fine to
/// ignore: the goal "no series for this type" is met either way.
pub fn remove_job_type_provisioning(job_type: &str) {
    let _ = METRICS
        .job_type_activatable
        .remove_label_values(&[job_type]);
    let _ = METRICS.job_type_workers.remove_label_values(&[job_type]);
    let _ = METRICS.job_type_starved.remove_label_values(&[job_type]);
}

/// Records `n` jobs dispatched to a worker for `job_type` — the per-type drain
/// throughput. Called once per stream dispatch pass (with the jobs sent to the
/// socket) and once per REST activation (with the jobs returned), so a job is
/// counted exactly once, on the gateway that fed the worker. A no-op when `n==0`
/// to avoid instantiating series for types that never actually drained.
pub fn record_jobs_dispatched(job_type: &str, n: u64) {
    if n == 0 {
        return;
    }
    METRICS
        .job_type_dispatched_total
        .with_label_values(&[job_type])
        .inc_by(n);
}

/// Observes one job's end-to-end sojourn (create→complete, `seconds`) into the
/// per-`job_type` SLA histogram. Reporting only (see `job_sojourn_seconds`); a
/// no-op for a non-positive sample (jobs created before the engine carried
/// `created_at`, or a clock skew) so the reported distribution is never polluted.
pub fn observe_job_sojourn(job_type: &str, seconds: f64) {
    if seconds <= 0.0 {
        return;
    }
    METRICS
        .job_sojourn_seconds
        .with_label_values(&[job_type])
        .observe(seconds);
}

/// Publishes the per-partition Raft `Shutdown` alarm: `down = true` sets the gauge
/// to 1 (the partition's core has terminated and stopped applying), else 0.
pub fn set_raft_partition_shutdown(partition: u64, down: bool) {
    METRICS
        .raft_partition_shutdown
        .with_label_values(&[&partition.to_string()])
        .set(i64::from(down));
}

/// Publishes one partition's engine-actor (deepthi) heartbeat, sampled ~1 Hz by
/// the metrics monitor. Together these gauges make the sustained-load
/// completion-freeze diagnosable at a glance: `alive=0` = the single writer died
/// (see the actor-exit error log + panic hook); `alive=1` with a frozen `jobs`
/// and a climbing `current_job_ms` = wedged inside one command; `alive=1`, frozen
/// `jobs`, `current_job_ms=0`, depths 0 = idle (the stall is upstream in Raft
/// commit, not the actor).
pub fn set_actor_stats(
    partition: u64,
    alive: bool,
    jobs: u64,
    current_job_ms: u64,
    hi_depth: usize,
    lo_depth: usize,
) {
    let p = partition.to_string();
    METRICS
        .actor_alive
        .with_label_values(&[&p])
        .set(i64::from(alive));
    METRICS
        .actor_jobs_total
        .with_label_values(&[&p])
        .set(jobs as i64);
    METRICS
        .actor_current_job_ms
        .with_label_values(&[&p])
        .set(current_job_ms as i64);
    METRICS
        .actor_hi_depth
        .with_label_values(&[&p])
        .set(hi_depth as i64);
    METRICS
        .actor_lo_depth
        .with_label_values(&[&p])
        .set(lo_depth as i64);
}

/// Records the wall time and jemalloc-allocated bytes attributed to a single
/// applied engine command, labelled by kind. Called from the raft state-machine
/// apply loop (on the engine thread) only when `NANOBPM_CMD_PROFILE` is set — see
/// [`crate::cmd_profile`]. The two histograms together split the create/complete
/// congestion collapse's residual per-command cost into allocator (alloc bytes
/// rise with active) vs hashmap-probe/cache (time rises, alloc flat).
pub fn record_command(kind: &'static str, seconds: f64, alloc_bytes: u64) {
    METRICS
        .cmd_seconds
        .with_label_values(&[kind])
        .observe(seconds);
    METRICS
        .cmd_alloc_bytes
        .with_label_values(&[kind])
        .observe(alloc_bytes as f64);
}

/// Publishes a partition's live engine-state cardinality (resident instances,
/// total jobs, leased jobs) — the independent variable the per-command cost is
/// regressed against. Sampled ~1 Hz from the monitor loop, off the hot path.
pub fn set_engine_cardinality(partition: u64, instances: usize, jobs: usize, activated: usize) {
    let p = partition.to_string();
    METRICS
        .engine_cardinality
        .with_label_values(&[&p, "instances"])
        .set(instances as i64);
    METRICS
        .engine_cardinality
        .with_label_values(&[&p, "jobs"])
        .set(jobs as i64);
    METRICS
        .engine_cardinality
        .with_label_values(&[&p, "activated"])
        .set(activated as i64);
}

/// Records one admission shed (a `createProcessInstance` rejected to protect
/// latency or memory), labelled by the rail that tripped. Delta-scraping this
/// counter shows shed rate and which rail is active — the observability that
/// makes "is the cluster shedding or silently gathering?" answerable.
pub fn record_admission_shed(reason: &str) {
    METRICS
        .admission_shed_total
        .with_label_values(&[reason])
        .inc();
}

/// Accounts one writer-loop iteration: `idle` is the time blocked awaiting the
/// first request, `busy` is the time spent draining/lingering/fsyncing/acking
/// that batch. Delta-scraping the two counters yields the writer's duty cycle.
pub fn record_writer_cycle(idle: Duration, busy: Duration) {
    let m = &*METRICS;
    m.writer_idle_seconds.inc_by(idle.as_secs_f64());
    m.writer_busy_seconds.inc_by(busy.as_secs_f64());
}

/// Renders the registry in the Prometheus text exposition format.
pub fn gather() -> String {
    let mut buf = String::new();
    let families = METRICS.registry.gather();
    TextEncoder::new()
        .encode_utf8(&families, &mut buf)
        .expect("encode metrics");
    buf
}

/// The per-job-type provisioning signals, read straight off the metric handles —
/// the no-serialize counterpart to scraping `/metrics` for them.
///
/// The gateway's worker-provisioning advisor recomputes its advice on every ~1 Hz
/// monitor tick. Routing that through [`gather`] + a text re-parse would serialize
/// the *entire* registry (every unrelated family, including per-job-type SLA
/// histograms) into a fresh `String` each second and then scan it for the handful
/// of series it actually needs — continuous CPU/allocation work that grows with the
/// whole metric surface. This reads exactly the series the advisor consumes, so the
/// per-tick cost stays proportional to the provisioning signals, not the registry.
///
/// The field set mirrors the advisor's `Snapshot` one-for-one; the caller (the
/// gateway bin, which depends on both crates) maps it into the advisor's type.
/// Storage deliberately does **not** depend on the advisor crate — the advisor is a
/// shared leaf that ProcessOS also links, and the one-way rule is that Nano never
/// links back into a consumer of its metrics.
///
/// Only the console-gated advisor calls this; allowed (not gated) so the narrow
/// reader cannot drift from the always-built gauges it mirrors.
#[cfg_attr(not(feature = "console"), allow(dead_code))]
#[derive(Debug, Default)]
pub struct ProvisioningSignals {
    /// Per-`job_type` `(activatable, workers, dispatched_total)` triples.
    pub per_type: Vec<(String, ProvisioningJobType)>,
    /// Whether the throughput ceiling LED is lit.
    pub ceiling_throughput: bool,
    /// Cumulative journal-writer busy seconds.
    pub writer_busy_seconds: f64,
    /// Cumulative journal-writer idle seconds.
    pub writer_idle_seconds: f64,
    /// Pending create-queue depth.
    pub pending_create_queue: i64,
    /// Cumulative admission-shed count (summed over all rails).
    pub admission_shed_total: u64,
}

/// One job type's provisioning counters at the read instant. Mirrors the advisor's
/// `JobTypeSample` (kept structurally identical by the caller's mapping).
#[cfg_attr(not(feature = "console"), allow(dead_code))]
#[derive(Debug, Default, Clone, Copy)]
pub struct ProvisioningJobType {
    /// Activatable (waiting) jobs — the backlog level.
    pub activatable: i64,
    /// Live subscribed workers (Falcon stream roster + live REST long-pollers).
    pub workers: i64,
    /// Cumulative jobs dispatched — the drain throughput.
    pub dispatched_total: u64,
}

/// Reads exactly the per-job-type provisioning series the advisor consumes,
/// straight off the metric handles. See [`ProvisioningSignals`].
#[cfg_attr(not(feature = "console"), allow(dead_code))]
pub fn provisioning_signals() -> ProvisioningSignals {
    let m = &*METRICS;

    // Fold the three per-type vectors into one map keyed by `job_type`. Reading
    // each vector's collected family (rather than re-rendering text) keeps this
    // O(series the advisor wants), not O(whole registry).
    let mut per_type: std::collections::BTreeMap<String, ProvisioningJobType> =
        std::collections::BTreeMap::new();
    fn job_type_of(metric: &prometheus::proto::Metric) -> Option<String> {
        metric
            .get_label()
            .iter()
            .find(|l| l.get_name() == "job_type")
            .map(|l| l.get_value().to_string())
    }
    // `Collector::collect` on a `*Vec` yields its single `MetricFamily`; take it.
    for metric in m.job_type_activatable.collect()[0].get_metric() {
        if let Some(jt) = job_type_of(metric) {
            per_type.entry(jt).or_default().activatable = metric.get_gauge().get_value() as i64;
        }
    }
    for metric in m.job_type_workers.collect()[0].get_metric() {
        if let Some(jt) = job_type_of(metric) {
            per_type.entry(jt).or_default().workers = metric.get_gauge().get_value() as i64;
        }
    }
    // The dispatched counter is cumulative and is NOT removed by
    // `remove_job_type_provisioning` (unlike the activatable/worker gauges), so it
    // keeps a series for every historically-dispatched type. Folding it in with
    // `entry(...).or_default()` would re-add each such dead type as a zero-backlog
    // sample; if the type later becomes active again, the advisor diffs against
    // that stale sample instead of returning `Warming` and can immediately report
    // false backlog growth / under-provisioning. Apply the counter only to job
    // types the current activatable/worker gauges already introduced (`get_mut`),
    // so a type with no live backlog/worker signal contributes no sample at all.
    for metric in m.job_type_dispatched_total.collect()[0].get_metric() {
        if let Some(jt) = job_type_of(metric)
            && let Some(entry) = per_type.get_mut(&jt)
        {
            entry.dispatched_total = metric.get_counter().get_value() as u64;
        }
    }

    ProvisioningSignals {
        per_type: per_type.into_iter().collect(),
        ceiling_throughput: m.ceiling_active.with_label_values(&["throughput"]).get() != 0,
        writer_busy_seconds: m.writer_busy_seconds.get(),
        writer_idle_seconds: m.writer_idle_seconds.get(),
        pending_create_queue: m.pending_create_queue.get(),
        admission_shed_total: SHED_REASONS
            .iter()
            .map(|r| m.admission_shed_total.with_label_values(&[r]).get())
            .sum(),
    }
}

// ---- Phase 2: falcon and protocol metrics ----

/// Records a falcon frame processed (by frame type).
pub fn record_stream_frame(frame_type: &str) {
    METRICS
        .stream_frames_total
        .with_label_values(&[frame_type])
        .inc();
}

/// Records a streaming client stalling for submission credits.
pub fn record_stream_credit_stall() {
    METRICS.stream_credit_stalls_total.inc();
}

/// Records one retry of a read-model export batch after a transient store write
/// failure. Called once per retry attempt, so the counter reflects total retry
/// work, not just the number of contended batches.
pub fn record_read_model_export_retry() {
    METRICS.read_model_export_retries_total.inc();
}

/// Records one read-model export batch permanently dropped by the remote
/// transport after a 4xx client error (e.g. `413 Payload Too Large`) that
/// retrying could never fix. Dropping avoids head-of-line-blocking the export
/// queue; in remote mode delivery is best-effort (the compaction watermark
/// already advanced on enqueue), so a poison batch is logged + counted, not
/// allowed to wedge the pipeline.
pub fn record_read_model_export_drop() {
    METRICS.read_model_export_drops_total.inc();
}

/// Falcon connection opened (+1).
pub fn stream_connection_inc() {
    METRICS.stream_connections_active.inc();
}

/// Falcon connection closed (-1).
pub fn stream_connection_dec() {
    METRICS.stream_connections_active.dec();
}

/// Records time spent processing one falcon frame.
pub fn record_stream_frame_processing(elapsed: Duration) {
    METRICS
        .stream_frame_processing_seconds
        .observe(elapsed.as_secs_f64());
}

/// Records a process instance create (by protocol: "rest" or "stream").
pub fn record_create(protocol: &str) {
    METRICS.creates_total.with_label_values(&[protocol]).inc();
}

/// Records one ad-hoc sub-process lifecycle event. `kind` is one of
/// `tool_activation`, `agent_iteration`, `completion`, `cancellation`. Called
/// from the completion sites that have the engine's emitted events in scope
/// (see `App::record_adhoc_events`).
pub fn record_adhoc_event(kind: &'static str) {
    METRICS.adhoc_events_total.with_label_values(&[kind]).inc();
}

/// Records one peer uplink dial (redial) attempt to `target`, tagged by outcome
/// (`ok`|`fail`). Onset-diagnosis instrument for the redial rate to a dead peer.
pub fn record_peer_connect_attempt(target: u32, ok: bool) {
    let outcome = if ok { "ok" } else { "fail" };
    METRICS
        .peer_connect_attempts_total
        .with_label_values(&[&target.to_string(), outcome])
        .inc();
}

/// Records the wall time spent inside `PeerSet::link()` acquiring a peer uplink
/// (captures the connect-under-global-mutex head-of-line stall while a peer is down).
pub fn record_peer_link(elapsed: Duration) {
    METRICS.peer_link_seconds.observe(elapsed.as_secs_f64());
}

/// Records a job completion (by protocol: "rest" or "stream").
pub fn record_job_completion(protocol: &str) {
    METRICS
        .job_completions_total
        .with_label_values(&[protocol])
        .inc();
}

/// Diagnostic: records where a stream CompleteJob landed (route_forward,
/// route_local, leader_reject, propose_err, apply_err, forward_ok, forward_err).
pub fn record_complete_outcome(outcome: &str) {
    METRICS
        .stream_complete_outcome_total
        .with_label_values(&[outcome])
        .inc();
}

/// Diagnostic: counts a leadership self-promotion for `partition` (a fresh
/// single-voter group formed via `promote_partition`). A steadily climbing
/// per-partition count under load is the reclaim promote-ping-pong fingerprint.
pub fn record_promote(partition: u64) {
    METRICS
        .raft_promote_total
        .with_label_values(&[&partition.to_string()])
        .inc();
}

/// A plain, dependency-free snapshot of the current metric values, taken in one
/// pass over the process-global registry handles. The console maps this to a
/// JSON DTO; throughput **rates** are derived client-side from the deltas of two
/// successive snapshots (so this stays a pure point-in-time reading).
///
/// Every field is a cheap atomic load (`get`) or a histogram sum/count read — no
/// text encoding, no allocation, no labels lookup beyond the two protocol-split
/// counters. Safe to poll at ~1 Hz from the dashboard with negligible overhead.
#[derive(Clone, Debug, Default)]
#[cfg_attr(not(feature = "console"), allow(dead_code))]
pub struct MetricsSnapshot {
    // Throughput counters (monotonic; rates derived from deltas).
    pub creates_rest: u64,
    pub creates_stream: u64,
    pub completions_rest: u64,
    pub completions_stream: u64,

    // Live gauges.
    pub stream_connections_active: i64,
    pub commit_inflight: i64,

    // Journal / durability counters.
    pub commits_total: u64,
    pub writes_total: u64,
    pub bytes_total: u64,
    pub stream_credit_stalls_total: u64,

    // Histogram aggregates (sum is in the metric's unit; mean = sum/count).
    pub fsync_seconds_sum: f64,
    pub fsync_count: u64,
    pub commit_wait_seconds_sum: f64,
    pub commit_wait_count: u64,
    pub commit_batch_size_sum: f64,
    pub commit_batch_count: u64,
    pub frame_processing_seconds_sum: f64,
    pub frame_processing_count: u64,

    // Writer duty-cycle counters (busy / (busy+idle) = saturation).
    pub writer_idle_seconds: f64,
    pub writer_busy_seconds: f64,

    // Capacity-ceiling LED + admission-ceiling input signals (ADR 0013). The
    // `*_active` bools are the lit "clipping" LEDs; the rest are the numbers
    // behind them so a dashboard can show pressure vs. its shed threshold.
    pub ceiling_throughput_active: bool,
    pub ceiling_memory_active: bool,
    pub ceiling_exporter_active: bool,
    pub ceiling_flow_control_active: bool,
    pub exporter_fill_permille: i64,
    pub pending_create_queue: i64,
    pub active_backlog: i64,
    pub admission_backlog_limit: i64,
    pub admission_create_queue_limit: i64,
    pub admission_shed_total: u64,
}

/// The shed rails (labels of `nanobpm_admission_shed_total`), summed into a
/// single total for the dashboard's "shed since boot" counter.
const SHED_REASONS: [&str; 6] = [
    "create_queue",
    "active_backlog",
    "create_backlog",
    "exporter",
    "pipeline_bytes",
    "mem_watermark",
];

/// Reads every metric handle once and returns a point-in-time snapshot.
#[cfg_attr(not(feature = "console"), allow(dead_code))]
pub fn snapshot() -> MetricsSnapshot {
    let m = &*METRICS;
    MetricsSnapshot {
        creates_rest: m.creates_total.with_label_values(&["rest"]).get(),
        creates_stream: m.creates_total.with_label_values(&["stream"]).get(),
        completions_rest: m.job_completions_total.with_label_values(&["rest"]).get(),
        completions_stream: m.job_completions_total.with_label_values(&["stream"]).get(),

        stream_connections_active: m.stream_connections_active.get(),
        commit_inflight: m.inflight.get(),

        commits_total: m.commits_total.get(),
        writes_total: m.writes_total.get(),
        bytes_total: m.bytes_total.get(),
        stream_credit_stalls_total: m.stream_credit_stalls_total.get(),

        fsync_seconds_sum: m.fsync_seconds.get_sample_sum(),
        fsync_count: m.fsync_seconds.get_sample_count(),
        commit_wait_seconds_sum: m.commit_wait_seconds.get_sample_sum(),
        commit_wait_count: m.commit_wait_seconds.get_sample_count(),
        commit_batch_size_sum: m.commit_batch_size.get_sample_sum(),
        commit_batch_count: m.commit_batch_size.get_sample_count(),
        frame_processing_seconds_sum: m.stream_frame_processing_seconds.get_sample_sum(),
        frame_processing_count: m.stream_frame_processing_seconds.get_sample_count(),

        writer_idle_seconds: m.writer_idle_seconds.get(),
        writer_busy_seconds: m.writer_busy_seconds.get(),

        ceiling_throughput_active: m.ceiling_active.with_label_values(&["throughput"]).get() != 0,
        ceiling_memory_active: m.ceiling_active.with_label_values(&["memory"]).get() != 0,
        ceiling_exporter_active: m.ceiling_active.with_label_values(&["exporter"]).get() != 0,
        ceiling_flow_control_active: m.ceiling_active.with_label_values(&["flow_control"]).get()
            != 0,
        exporter_fill_permille: m.exporter_fill_permille.get(),
        pending_create_queue: m.pending_create_queue.get(),
        active_backlog: m.active_backlog.get(),
        admission_backlog_limit: m.admission_limit.with_label_values(&["backlog"]).get(),
        admission_create_queue_limit: m.admission_limit.with_label_values(&["create_queue"]).get(),
        admission_shed_total: SHED_REASONS
            .iter()
            .map(|r| m.admission_shed_total.with_label_values(&[r]).get())
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_type_starvation_flags_waiting_jobs_with_no_workers() {
        // Unique label so the assertion is isolated from the shared registry.
        set_job_type_provisioning("test-starved-type", 7, 0);
        set_job_type_provisioning("test-served-type", 7, 3);
        set_job_type_provisioning("test-idle-type", 0, 2);
        let out = gather();
        assert!(out.contains("nanobpm_job_type_starved{job_type=\"test-starved-type\"} 1"));
        assert!(out.contains("nanobpm_job_type_activatable{job_type=\"test-starved-type\"} 7"));
        // Workers present -> not starved even with a backlog.
        assert!(out.contains("nanobpm_job_type_starved{job_type=\"test-served-type\"} 0"));
        // No waiting jobs -> not starved even with idle workers.
        assert!(out.contains("nanobpm_job_type_starved{job_type=\"test-idle-type\"} 0"));
    }

    #[test]
    fn dispatched_counter_accumulates_per_type_and_ignores_zero() {
        // A zero-count dispatch must not instantiate a series (keeps the metric
        // surface free of types that never actually drained).
        record_jobs_dispatched("test-dispatch-zero", 0);
        assert!(!gather().contains("test-dispatch-zero"));

        // Non-zero dispatches accumulate for the type (drain throughput).
        record_jobs_dispatched("test-dispatch-type", 5);
        record_jobs_dispatched("test-dispatch-type", 3);
        assert!(
            gather()
                .contains("nanobpm_job_type_dispatched_total{job_type=\"test-dispatch-type\"} 8")
        );
    }

    #[test]
    fn provisioning_signals_read_the_same_values_the_exposition_renders() {
        // The narrow reader must agree with the `/metrics` text the advisor used to
        // parse — otherwise the console panel and the ProcessOS cockpit would see
        // different provisioning numbers for the same instant. Unique labels keep
        // the assertion isolated from the shared registry.
        set_job_type_provisioning("test-provsig-type", 9, 4);
        record_jobs_dispatched("test-provsig-type", 17);

        let sig = provisioning_signals();
        let sample = sig
            .per_type
            .iter()
            .find(|(jt, _)| jt == "test-provsig-type")
            .map(|(_, s)| *s)
            .expect("per-type series present in the narrow read");
        assert_eq!(sample.activatable, 9, "backlog level matches the gauge");
        assert_eq!(sample.workers, 4, "worker count matches the gauge");
        assert_eq!(
            sample.dispatched_total, 17,
            "drain throughput matches the counter"
        );
        // And the same values appear in the rendered exposition (parity).
        let text = gather();
        assert!(text.contains("nanobpm_job_type_activatable{job_type=\"test-provsig-type\"} 9"));
        assert!(text.contains("nanobpm_job_type_workers{job_type=\"test-provsig-type\"} 4"));
    }

    #[test]
    fn set_sla_mode_publishes_active_series_and_clears_the_other() {
        set_sla_mode("admission");
        let g = gather();
        assert!(g.contains("nanobpm_sla_mode{mode=\"admission\"} 1"));
        assert!(g.contains("nanobpm_sla_mode{mode=\"latency\"} 0"));

        // Switching flips exactly one series to 1 and the other to 0 (info gauge).
        set_sla_mode("latency");
        let g = gather();
        assert!(g.contains("nanobpm_sla_mode{mode=\"latency\"} 1"));
        assert!(g.contains("nanobpm_sla_mode{mode=\"admission\"} 0"));

        // Any unrecognised value fails safe to latency (never silently admission).
        set_sla_mode("bogus");
        assert!(gather().contains("nanobpm_sla_mode{mode=\"latency\"} 1"));
    }

    #[test]
    fn ceiling_hits_count_only_rising_edges() {
        // Drive a full low -> high -> high -> low -> high cycle and confirm the
        // hit counter advances by exactly one per rising edge, while the gauge
        // tracks the live state each tick.
        let before = ceiling_hits_total_for("test-ceiling");
        set_ceiling_active("test-ceiling", false, false); // stays low
        set_ceiling_active("test-ceiling", true, false); // rising edge (+1)
        set_ceiling_active("test-ceiling", true, true); // held high (no count)
        set_ceiling_active("test-ceiling", false, true); // falling edge
        set_ceiling_active("test-ceiling", true, false); // rising edge (+1)
        assert_eq!(ceiling_hits_total_for("test-ceiling"), before + 2);
        assert!(gather().contains("nanobpm_ceiling_active{ceiling=\"test-ceiling\"} 1"));
    }

    fn ceiling_hits_total_for(ceiling: &str) -> u64 {
        METRICS
            .ceiling_hits_total
            .with_label_values(&[ceiling])
            .get()
    }

    #[test]
    fn remove_job_type_provisioning_frees_the_label_series() {
        // Cardinality regression (Copilot review): a disappeared job type must have
        // its provisioning series *removed*, not merely zeroed — `with_label_values`
        // creates the child permanently, so zeroing alone leaves an unauthenticated
        // `activateJobs` caller free to grow the registry by rotating unique types.
        // Unique label keeps the assertion isolated from the shared registry.
        let jt = "test-remove-type";
        set_job_type_provisioning(jt, 5, 1);
        assert!(
            gather().contains("nanobpm_job_type_activatable{job_type=\"test-remove-type\"}"),
            "series present while the type is active"
        );

        remove_job_type_provisioning(jt);
        let text = gather();
        assert!(
            !text.contains("test-remove-type"),
            "activatable/workers/starved series are all removed once the type leaves the active set"
        );

        // Removal is not a tombstone: a type that reappears re-creates its series.
        set_job_type_provisioning(jt, 2, 0);
        assert!(gather().contains("nanobpm_job_type_activatable{job_type=\"test-remove-type\"} 2"));
    }

    #[test]
    fn provisioning_signals_skip_a_dispatched_counter_with_no_live_type() {
        // Stale-counter regression (Copilot review): the dispatched counter is
        // cumulative and is NOT removed by `remove_job_type_provisioning`, so it
        // keeps a series for every historically-dispatched type. Folding it in
        // unconditionally would re-add each dead type as a zero-backlog sample in
        // `PREV`; if the type later reactivates, the advisor diffs against that
        // stale sample instead of returning `Warming` and reports false backlog
        // growth. The narrow read must apply the counter only to types the live
        // activatable/worker gauges already introduced.
        let jt = "test-stale-dispatch-type";
        // A dispatched counter exists for a type that has NO live gauges (it left
        // the active set after dispatching). Unique label isolates the assertion.
        record_jobs_dispatched(jt, 7);
        assert!(
            gather().contains(
                "nanobpm_job_type_dispatched_total{job_type=\"test-stale-dispatch-type\"} 7"
            ),
            "the cumulative counter series persists after the type leaves the active set"
        );

        let sig = provisioning_signals();
        assert!(
            !sig.per_type.iter().any(|(t, _)| t == jt),
            "a dispatched counter with no live activatable/worker gauge contributes no sample"
        );

        // Once the type is live again its counter folds in normally.
        set_job_type_provisioning(jt, 3, 1);
        let sig = provisioning_signals();
        let sample = sig
            .per_type
            .iter()
            .find(|(t, _)| t == jt)
            .map(|(_, s)| *s)
            .expect("live type present");
        assert_eq!(sample.activatable, 3);
        assert_eq!(sample.workers, 1);
        assert_eq!(
            sample.dispatched_total, 7,
            "the live type picks up its cumulative counter"
        );
        remove_job_type_provisioning(jt);
    }
}
