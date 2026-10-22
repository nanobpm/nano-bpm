//! Remote read-model projection sinks (pluggable-exporter epic, issue #133, M2).
//!
//! Two sinks are provided on top of the [`ProjectionSink`] seam:
//!
//! * [`RemoteSink`] — computes the engine-facing [`ExportOutcome`] from a tiny
//!   in-RAM in-flight index (no disk), ships the batch's events to a central
//!   exporter over a [`BatchTransport`], and advances only the local shard's
//!   compaction watermark. This is the "decoupled disk IOPS / data-lake" mode:
//!   the heavy projection (instances, variables, jobs) leaves the node entirely.
//! * [`TeeSink`] — projects to a local (authoritative) sink AND mirrors the same
//!   batch to a remote sink. The local outcome is returned, so in-flight
//!   accounting, queries, and the compaction watermark stay exactly as today
//!   while a durable copy is streamed downstream. The safe intermediate.
//!
//! The projection contract (idempotent, never-drop-a-batch, per-shard order) is
//! documented on [`ProjectionSink`]; both sinks uphold it. Remote **delivery**
//! is currently best-effort with respect to durability: the watermark advances
//! once a batch is enqueued for the transport, not once the remote target has
//! acknowledged it. Ack-gated advancement is a later milestone (#133).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use nanobpmn_engine_core::{Event, Key};

use crate::readstore::{ExportOutcome, ProjectionSink, ReadStore};

/// Max serialized size (bytes) of a single outbound export POST body. A drained
/// batch is split into as many order-preserving JSON-array chunks as needed to
/// keep each POST under this budget, so a large batch is delivered as several
/// POSTs rather than rejected by the exporter as `413 Payload Too Large` (which
/// used to poison the pipeline). Kept well under the exporter's request body
/// limit (see `nano-exporter`'s `DefaultBodyLimit`). Overridable via
/// `NANOBPMN_EXPORTER_MAX_BODY_BYTES` (default 4 MiB).
fn max_body_bytes() -> usize {
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("NANOBPMN_EXPORTER_MAX_BODY_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(4 * 1024 * 1024)
    })
}

/// Serializes a log-ordered event slice into one or more order-preserving JSON
/// arrays, each with a serialized size at or under `budget` bytes. A single
/// event larger than `budget` is emitted alone (it cannot be split further — the
/// exporter body limit must accommodate the largest single event). The
/// concatenation of the chunks' element sequences equals the input order, so
/// per-shard delivery order is preserved.
fn serialize_chunks(events: &[&Event], budget: usize) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut chunks = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut count = 0usize;
    for &event in events {
        let enc = serde_json::to_vec(event)?;
        // Bytes added to the in-progress array by appending this element:
        // a separating comma when not the first element, plus the element.
        let add = enc.len() + usize::from(count > 0);
        // Close and flush the current chunk when the next element would push it
        // (including the closing `]`) over budget. Never flush an empty chunk —
        // an oversized single event still goes out on its own.
        if count > 0 && cur.len() + add + 1 > budget {
            cur.push(b']');
            chunks.push(std::mem::take(&mut cur));
            count = 0;
        }
        cur.push(if count == 0 { b'[' } else { b',' });
        cur.extend_from_slice(&enc);
        count += 1;
    }
    if count > 0 {
        cur.push(b']');
        chunks.push(cur);
    }
    Ok(chunks)
}

/// Ephemeral, in-RAM index of a shard's currently-Active instance keys.
///
/// Reproduces the SQLite projection's *genuine-transition* semantics (see
/// `readstore::project`) so an [`ExportOutcome`] can be derived without a
/// disk-backed read store: a `ProcessInstanceCreated` for a key not already
/// Active is `+1`; a `ProcessInstanceCompleted`/`Terminated` for a currently
/// Active key is `-1` and evicts it. Every other event is irrelevant to the
/// in-flight gauge and terminal eviction, so it is ignored. The set is
/// idempotent under replay: a re-delivered create (`insert` returns `false`) or
/// a re-delivered terminal (`remove` returns `false`) contributes zero, exactly
/// as the `INSERT ... DO NOTHING` / `UPDATE ... AND state = 0` guards do in SQL.
///
/// Memory is one `Key` (8 bytes) per *currently in-flight* instance only —
/// terminals are removed immediately — so it tracks the live working set, not
/// cumulative throughput.
#[derive(Default)]
pub struct InflightIndex {
    active: Mutex<HashSet<Key>>,
}

impl InflightIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a log-ordered batch, returning the exact net in-flight delta and
    /// the keys that made a genuine Active->terminal transition in this batch.
    pub fn apply(&self, events: &[&Event]) -> ExportOutcome {
        let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let mut inflight_delta: i64 = 0;
        let mut terminal_keys = Vec::new();
        for &event in events {
            match event {
                Event::ProcessInstanceCreated { instance_key, .. } => {
                    if active.insert(*instance_key) {
                        inflight_delta += 1;
                    }
                }
                Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key }
                    if active.remove(instance_key) =>
                {
                    inflight_delta -= 1;
                    terminal_keys.push(*instance_key);
                }
                _ => {}
            }
        }
        ExportOutcome {
            terminal_keys,
            inflight_delta,
        }
    }

    /// Number of instances currently tracked as Active.
    pub fn active_count(&self) -> usize {
        self.active.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// Ships a serialized, log-ordered event batch for one shard to a central
/// exporter. Implementations MUST preserve per-shard ordering and MUST apply
/// backpressure (block or bounded-buffer) rather than silently dropping — the
/// record stream must not lose a batch. `partition` is the shard's global
/// partition id (the ordering/routing key downstream).
pub trait BatchTransport: Send + Sync {
    /// Enqueue one shard batch (already serialized) for delivery.
    fn send(&self, partition: u64, payload: Vec<u8>);

    /// Best-effort count of batches still buffered, for metrics/backpressure.
    /// Consumed by the remote-exporter metrics wiring (M3, issue #133).
    #[allow(dead_code)]
    fn queued(&self) -> usize {
        0
    }
}

/// A transport that discards batches — used when remote export is selected but
/// no endpoint is configured (or in tests). Counts drops so misconfiguration is
/// visible rather than silent.
#[derive(Default)]
pub struct NullTransport {
    dropped: std::sync::atomic::AtomicU64,
}

impl BatchTransport for NullTransport {
    fn send(&self, _partition: u64, _payload: Vec<u8>) {
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl NullTransport {
    #[cfg(test)]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// One shard batch queued for HTTP delivery to the central exporter.
struct HttpBatch {
    partition: u64,
    payload: Vec<u8>,
}

/// Ships shard batches to a central exporter over HTTP `POST {endpoint}`.
///
/// The per-shard exporter runs on a plain OS thread (outside any tokio context)
/// and `reqwest` here is async-only, so delivery is driven by ONE dedicated
/// sender thread hosting a current-thread runtime. `send` enqueues onto a
/// bounded [`tokio::sync::mpsc`] channel; a full queue BLOCKS the caller
/// (backpressure), never drops — upholding the never-lose-a-batch invariant.
///
/// **Pipelined delivery.** Each POST is a full network round-trip to the central
/// exporter and on to Elasticsearch; delivering strictly one-at-a-time made that
/// round-trip latency the throughput ceiling (one in-flight POST per shard). The
/// sender therefore keeps up to `concurrency` POSTs in flight at once (a
/// [`tokio::sync::Semaphore`] bounds it), overlapping the round-trips. When all
/// permits are taken the recv loop stalls, the channel fills, and `send` blocks —
/// so backpressure still holds end-to-end.
///
/// **Ordering.** With `concurrency > 1`, batches for this shard may be delivered
/// out of order (and a retried batch may land after a later one). That is safe
/// for the reference append-only target: each event is an independent document,
/// and remote delivery is already best-effort — the journal-compaction watermark
/// advances on *enqueue*, not on remote ack (ack-gated ordering is a later
/// milestone, #133). `concurrency = 1` restores strictly-ordered, one-at-a-time
/// delivery for a future order-sensitive target. Failed POSTs (5xx / network)
/// are retried with a capped backoff; a permanent 4xx is dropped (counted) so a
/// poison batch cannot head-of-line-block the pipeline.
pub struct HttpBatchTransport {
    tx: tokio::sync::mpsc::Sender<HttpBatch>,
    depth: Arc<std::sync::atomic::AtomicUsize>,
}

impl HttpBatchTransport {
    /// Spawns the sender thread. `endpoint` is the central exporter's batch URL
    /// (e.g. `http://exporter-host:9700/ingest`); `capacity` bounds the in-RAM
    /// queue of undelivered batches (backpressure threshold); `concurrency`
    /// bounds the number of POSTs kept in flight at once (pipeline depth).
    // expect_used: spawning the exporter thread fails only on OS resource
    // exhaustion; a transport with no sender thread would silently drop every
    // batch, so construction aborts loudly instead.
    #[allow(
        clippy::expect_used,
        reason = "thread spawn failure is a fatal OS resource error at transport construction"
    )]
    pub fn new(endpoint: String, capacity: usize, concurrency: usize) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<HttpBatch>(capacity.max(1));
        let depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let depth_bg = depth.clone();
        let concurrency = concurrency.max(1);
        std::thread::Builder::new()
            .name("nanobpmn-exporter-http".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!("remote exporter: failed to build runtime: {e}");
                        return;
                    }
                };
                let client = reqwest::Client::new();
                let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
                rt.block_on(async move {
                    while let Some(batch) = rx.recv().await {
                        // Bound in-flight POSTs: this awaits (stalling the recv
                        // loop → filling the channel → blocking `send`) once
                        // `concurrency` deliveries are already outstanding.
                        let Ok(permit) = sem.clone().acquire_owned().await else {
                            break; // semaphore closed — shutting down
                        };
                        let client = client.clone();
                        let endpoint = endpoint.clone();
                        let depth_task = depth_bg.clone();
                        tokio::spawn(async move {
                            deliver_batch(&client, &endpoint, &batch).await;
                            depth_task.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            drop(permit);
                        });
                    }
                });
            })
            .expect("spawn remote exporter thread");
        Self { tx, depth }
    }
}

/// Delivers one batch, retrying transient failures with a capped backoff and
/// dropping a permanent 4xx (so it cannot head-of-line-block). Returns once the
/// batch is either acked (2xx) or permanently dropped.
async fn deliver_batch(client: &reqwest::Client, endpoint: &str, batch: &HttpBatch) {
    let mut backoff = std::time::Duration::from_millis(50);
    loop {
        let res = client
            .post(endpoint)
            .header("x-nano-partition", batch.partition)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(batch.payload.clone())
            .send()
            .await;
        match res {
            Ok(r) if r.status().is_success() => return,
            Ok(r) if r.status().is_client_error() => {
                // A 4xx (e.g. 413 Payload Too Large, 400 Bad Request) is
                // PERMANENT for this exact body: retrying the identical bytes can
                // never succeed and would hold a delivery slot forever, throttling
                // the pipeline (and, at concurrency 1, freezing it — the M2
                // wedge). Drop it (loud), count it, and move on — remote delivery
                // is best-effort (the compaction watermark already advanced on
                // enqueue), so a poison batch must not wedge the node.
                tracing::error!(
                    "remote exporter: partition {} POST -> {} (permanent; dropping {}-byte batch)",
                    batch.partition,
                    r.status(),
                    batch.payload.len()
                );
                crate::metrics::record_read_model_export_drop();
                return;
            }
            Ok(r) => tracing::warn!(
                "remote exporter: partition {} POST -> {}",
                batch.partition,
                r.status()
            ),
            Err(e) => tracing::warn!(
                "remote exporter: partition {} POST failed: {e}",
                batch.partition
            ),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
    }
}

impl BatchTransport for HttpBatchTransport {
    fn send(&self, partition: u64, payload: Vec<u8>) {
        self.depth
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Called from the exporter's plain OS thread (never inside a tokio
        // runtime), so `blocking_send` is safe: it blocks the caller when the
        // channel is full, which is the intended backpressure.
        if self
            .tx
            .blocking_send(HttpBatch { partition, payload })
            .is_err()
        {
            // Receiver gone (shutdown): undo the depth bump.
            self.depth
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn queued(&self) -> usize {
        self.depth.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Remote-only projection sink: derive the engine-facing outcome locally from
/// the in-flight index, hand the batch to the transport, and advance only the
/// local shard's compaction watermark. See the module docs.
pub struct RemoteSink {
    partition: u64,
    index: InflightIndex,
    /// Local shard retained ONLY to persist `exported_position` (the journal
    /// compaction watermark). Never receives the projected payload — that is
    /// offloaded to the remote target — so its per-batch disk cost is a single
    /// integer `UPDATE`.
    watermark: Arc<ReadStore>,
    transport: Arc<dyn BatchTransport>,
}

impl RemoteSink {
    pub fn new(
        partition: u64,
        watermark: Arc<ReadStore>,
        transport: Arc<dyn BatchTransport>,
    ) -> Self {
        Self {
            partition,
            index: InflightIndex::new(),
            watermark,
            transport,
        }
    }

    /// Currently in-flight instance count on this shard (from the index).
    /// Exposed for the remote-exporter metrics wiring (M3, issue #133).
    #[allow(dead_code)]
    pub fn active_count(&self) -> usize {
        self.index.active_count()
    }
}

impl ProjectionSink for RemoteSink {
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome> {
        // Engine-facing outcome first: this drives terminal eviction and the
        // in-flight gauge, and must be exact and synchronous regardless of the
        // remote target's state.
        let outcome = self.index.apply(events);
        // Serialize as one or more JSON arrays, each under the body budget, so a
        // large drained batch is split across several POSTs instead of being
        // rejected `413 Payload Too Large`. Order is preserved across chunks.
        for chunk in serialize_chunks(events, max_body_bytes())? {
            self.transport.send(self.partition, chunk);
        }
        // Advance the compaction watermark by the batch's event count so the
        // journal can still compact past a handed-off prefix. Cheap integer
        // write — the payload never touches this shard.
        self.watermark.advance_exported(events.len())?;
        Ok(outcome)
    }

    // `prune_terminal` keeps the default no-op: the remote target owns retention
    // of its own store; this node holds no projected history to prune.
}

/// Tee sink: project to a local authoritative sink AND mirror each batch to a
/// remote transport. Returns the LOCAL outcome, so in-flight accounting,
/// terminal eviction, queries, and the compaction watermark are byte-for-byte as
/// the local sink alone — the remote copy is a decoupled downstream feed. The
/// remote side ships the serialized batch only; it does NOT touch the watermark
/// (the local sink owns it) and needs no in-flight index (the local outcome is
/// authoritative).
pub struct TeeSink {
    partition: u64,
    local: Arc<dyn ProjectionSink>,
    transport: Arc<dyn BatchTransport>,
}

impl TeeSink {
    pub fn new(
        partition: u64,
        local: Arc<dyn ProjectionSink>,
        transport: Arc<dyn BatchTransport>,
    ) -> Self {
        Self {
            partition,
            local,
            transport,
        }
    }
}

impl ProjectionSink for TeeSink {
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome> {
        // Local is authoritative: its result is what the engine acts on, and a
        // local failure must surface (retried by the exporter loop) so the
        // never-drop-a-batch invariant holds for the canonical read model. Only
        // mirror to the remote once the local projection succeeded.
        let outcome = self.local.export(events)?;
        match serialize_chunks(events, max_body_bytes()) {
            Ok(chunks) => {
                for chunk in chunks {
                    self.transport.send(self.partition, chunk);
                }
            }
            Err(e) => {
                crate::metrics::record_read_model_export_retry();
                tracing::warn!("tee: remote mirror serialization failed (local unaffected): {e}");
            }
        }
        Ok(outcome)
    }

    fn prune_terminal(&self, max_keep: usize, max_delete: usize) -> anyhow::Result<usize> {
        // Retention applies to the local authoritative store; the remote target
        // manages its own history.
        self.local.prune_terminal(max_keep, max_delete)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nanobpmn_engine_core::Event;

    use super::*;

    fn created(key: Key) -> Event {
        Event::ProcessInstanceCreated {
            instance_key: key,
            process_id: "p".into(),
            variables: HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    fn completed(key: Key) -> Event {
        Event::ProcessInstanceCompleted { instance_key: key }
    }

    fn terminated(key: Key) -> Event {
        Event::ProcessInstanceTerminated { instance_key: key }
    }

    fn outcome_of(idx: &InflightIndex, evs: &[Event]) -> ExportOutcome {
        let refs: Vec<&Event> = evs.iter().collect();
        idx.apply(&refs)
    }

    #[test]
    fn create_then_complete_nets_to_zero_inflight() {
        let idx = InflightIndex::new();
        let o = outcome_of(&idx, &[created(1), created(2), completed(1)]);
        assert_eq!(o.inflight_delta, 1); // +2 creates, -1 completion
        assert_eq!(o.terminal_keys, vec![1]);
        assert_eq!(idx.active_count(), 1); // key 2 still active
    }

    /// Epic #1357 failure-injection guard: a poisoned in-flight-index mutex must
    /// be RECOVERED, not panicked — the exporter path must keep projecting even
    /// if some thread panicked while holding the index lock.
    #[test]
    fn poisoned_inflight_lock_is_recovered_not_panicked() {
        let idx = InflightIndex::new();
        outcome_of(&idx, &[created(1)]);

        let active = &idx.active;
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = active.lock().unwrap();
            panic!("deliberate poison for the recovery test");
        }));
        std::panic::set_hook(prev);
        assert!(poisoned.is_err(), "the poisoning panic must propagate");
        assert!(
            idx.active.lock().is_err(),
            "the mutex is now poisoned (test precondition)"
        );

        // Both locked paths still work.
        let o = outcome_of(&idx, &[created(2), completed(1)]);
        assert_eq!(o.inflight_delta, 0);
        assert_eq!(idx.active_count(), 1);
    }

    #[test]
    fn redelivered_create_is_idempotent() {
        let idx = InflightIndex::new();
        assert_eq!(outcome_of(&idx, &[created(7)]).inflight_delta, 1);
        // Re-delivery of the same create must not move the gauge.
        let o = outcome_of(&idx, &[created(7)]);
        assert_eq!(o.inflight_delta, 0);
        assert!(o.terminal_keys.is_empty());
        assert_eq!(idx.active_count(), 1);
    }

    #[test]
    fn redelivered_terminal_is_idempotent() {
        let idx = InflightIndex::new();
        outcome_of(&idx, &[created(3)]);
        let first = outcome_of(&idx, &[completed(3)]);
        assert_eq!(first.inflight_delta, -1);
        assert_eq!(first.terminal_keys, vec![3]);
        // Re-delivered completion: already evicted, must be a no-op.
        let second = outcome_of(&idx, &[completed(3)]);
        assert_eq!(second.inflight_delta, 0);
        assert!(second.terminal_keys.is_empty());
        assert_eq!(idx.active_count(), 0);
    }

    #[test]
    fn terminated_counts_as_terminal() {
        let idx = InflightIndex::new();
        outcome_of(&idx, &[created(9)]);
        let o = outcome_of(&idx, &[terminated(9)]);
        assert_eq!(o.inflight_delta, -1);
        assert_eq!(o.terminal_keys, vec![9]);
    }

    #[test]
    fn terminal_without_matching_active_is_ignored() {
        let idx = InflightIndex::new();
        // Completion for a never-seen key: not a genuine transition.
        let o = outcome_of(&idx, &[completed(42)]);
        assert_eq!(o.inflight_delta, 0);
        assert!(o.terminal_keys.is_empty());
    }

    #[test]
    fn remote_sink_serializes_and_advances_watermark() {
        let store = Arc::new(ReadStore::open(None).expect("open in-memory store"));
        let transport = Arc::new(NullTransport::default());
        let sink = RemoteSink::new(0, store.clone(), transport.clone());
        let start = store.exported_position();
        let evs = [created(1), created(2), completed(1)];
        let refs: Vec<&Event> = evs.iter().collect();
        let o = sink.export(&refs).expect("remote export");
        assert_eq!(o.inflight_delta, 1);
        assert_eq!(o.terminal_keys, vec![1]);
        // Watermark advanced by event count; one batch shipped to transport.
        assert_eq!(store.exported_position(), start + 3);
        assert_eq!(transport.dropped(), 1);
    }

    #[test]
    fn tee_returns_local_outcome_and_mirrors() {
        let local = Arc::new(ReadStore::open(None).unwrap());
        let transport = Arc::new(NullTransport::default());
        let tee = TeeSink::new(
            0,
            local.clone() as Arc<dyn ProjectionSink>,
            transport.clone(),
        );
        let evs = [created(5)];
        let refs: Vec<&Event> = evs.iter().collect();
        let o = tee.export(&refs).expect("tee export");
        assert_eq!(o.inflight_delta, 1);
        // Local projected (watermark advanced exactly once — no double count).
        assert_eq!(local.exported_position(), 1);
        // Batch mirrored to the transport exactly once.
        assert_eq!(transport.dropped(), 1);
    }

    #[test]
    fn serialize_chunks_single_when_under_budget() {
        let evs = [created(1), created(2), completed(1)];
        let refs: Vec<&Event> = evs.iter().collect();
        let chunks = serialize_chunks(&refs, 4 * 1024 * 1024).unwrap();
        assert_eq!(chunks.len(), 1);
        // Round-trips as a JSON array of exactly the input events, in order.
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&chunks[0]).unwrap();
        assert_eq!(parsed.len(), 3);
    }

    #[test]
    fn serialize_chunks_splits_over_budget_preserving_order() {
        // Ten events; a tiny budget forces multiple chunks. Every chunk must be
        // a valid JSON array, none may exceed the budget (except a lone oversized
        // event), and concatenating the chunks' elements must equal the input.
        let evs: Vec<Event> = (0..10).map(|k| created(k as Key)).collect();
        let refs: Vec<&Event> = evs.iter().collect();
        let one = serde_json::to_vec(refs[0]).unwrap().len();
        let budget = one * 3; // ~a couple events per chunk
        let chunks = serialize_chunks(&refs, budget).unwrap();
        assert!(chunks.len() > 1, "expected the batch to split");
        let mut total = 0usize;
        for chunk in &chunks {
            let parsed: Vec<serde_json::Value> = serde_json::from_slice(chunk).unwrap();
            assert!(!parsed.is_empty());
            total += parsed.len();
            // A multi-element chunk must respect the budget.
            if parsed.len() > 1 {
                assert!(
                    chunk.len() <= budget,
                    "chunk {} exceeded budget",
                    chunk.len()
                );
            }
        }
        assert_eq!(
            total, 10,
            "no event may be lost or duplicated across chunks"
        );
    }

    #[test]
    fn serialize_chunks_emits_oversized_single_event_alone() {
        // A single event larger than the budget cannot be split: it must still
        // be emitted (on its own) rather than dropped or looped forever.
        let evs = [created(1)];
        let refs: Vec<&Event> = evs.iter().collect();
        let chunks = serialize_chunks(&refs, 1).unwrap();
        assert_eq!(chunks.len(), 1);
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&chunks[0]).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn serialize_chunks_empty_is_no_chunks() {
        let chunks = serialize_chunks(&[], 4096).unwrap();
        assert!(chunks.is_empty());
    }

    /// Spawns a throwaway mock exporter on 127.0.0.1 that holds each request for
    /// `hold` and records the peak number of *simultaneous* in-flight requests.
    /// Returns (address, peak-counter, delivered-counter).
    fn spawn_mock_exporter(
        hold: std::time::Duration,
    ) -> (
        std::net::SocketAddr,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let peak = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(AtomicUsize::new(0));
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let peak_bg = peak.clone();
        let delivered_bg = delivered.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                let cur = Arc::new(AtomicUsize::new(0));
                loop {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    let (peak, delivered, cur) =
                        (peak_bg.clone(), delivered_bg.clone(), cur.clone());
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        let mut buf = [0u8; 8192];
                        let _ = sock.read(&mut buf).await;
                        tokio::time::sleep(hold).await;
                        delivered.fetch_add(1, Ordering::SeqCst);
                        cur.fetch_sub(1, Ordering::SeqCst);
                        let _ = sock
                            .write_all(b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n")
                            .await;
                    });
                }
            });
        });
        (addr_rx.recv().unwrap(), peak, delivered)
    }

    #[test]
    fn http_transport_pipelines_concurrent_posts() {
        use std::sync::atomic::Ordering;
        let (addr, peak, delivered) = spawn_mock_exporter(std::time::Duration::from_millis(150));
        let endpoint = format!("http://{addr}/ingest");
        let transport = HttpBatchTransport::new(endpoint, 64, 4);
        for i in 0..8u64 {
            transport.send(i, format!("[{{\"n\":{i}}}]").into_bytes());
        }
        // Serial (concurrency 1) would take ~8*150ms=1.2s; concurrency 4 ~300ms.
        // Poll for full delivery AND drain with generous slack for CI scheduling.
        // The mock server bumps `delivered` just *before* it writes the 204,
        // whereas `queued()` only decrements once the client has received that
        // 204 and the delivery task runs its `fetch_sub`. Gating solely on
        // `delivered == 8` therefore races the `queued() == 0` assertion below:
        // the last batch can be counted delivered while its response is still
        // in flight. Gate on both so the drain assertion is deterministic.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while (delivered.load(Ordering::SeqCst) < 8 || transport.queued() != 0)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(delivered.load(Ordering::SeqCst), 8, "all batches delivered");
        assert!(
            peak.load(Ordering::SeqCst) >= 2,
            "expected pipelined (concurrent) delivery, peak was {}",
            peak.load(Ordering::SeqCst)
        );
        assert_eq!(transport.queued(), 0, "outstanding depth drains to zero");
    }

    #[test]
    fn http_transport_concurrency_one_is_strictly_serial() {
        use std::sync::atomic::Ordering;
        let (addr, peak, delivered) = spawn_mock_exporter(std::time::Duration::from_millis(40));
        let endpoint = format!("http://{addr}/ingest");
        let transport = HttpBatchTransport::new(endpoint, 64, 1);
        for i in 0..5u64 {
            transport.send(i, format!("[{{\"n\":{i}}}]").into_bytes());
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while delivered.load(Ordering::SeqCst) < 5 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(delivered.load(Ordering::SeqCst), 5, "all batches delivered");
        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "concurrency=1 must never have two POSTs in flight (ordered delivery)"
        );
    }
}
