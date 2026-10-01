//! Multi-partition routing over a set of single-writer engine actors.
//!
//! Following Zeebe, a node may run several partitions, each its own
//! single-writer [`DeepthiHandle`] (a dedicated engine thread + journal). Keys
//! carry their owning partition in their high bits (see
//! [`nanobpmn_engine_core::partition_of`]), so a command that targets an
//! existing key routes to exactly one partition, while a fresh
//! `createProcessInstance` is balanced round-robin across them. Queries are
//! answered from the single shared read model and never touch a partition.
//!
//! The default is a single partition (id `0`), which preserves the historical
//! behaviour exactly: one engine thread, one journal, the `1, 2, 3, …` key
//! sequence. Set `NANOBPMN_PARTITIONS=<n>` to run `n` partitions.
//!
//! # Distributed-scaling seam
//!
//! Routing goes through a [`PartitionRouter`], which maps every [`PartitionId`]
//! to a [`Location`]: either `Local` (an engine actor owned by this node) or
//! `Remote` (owned by another node, addressed by [`NodeId`]). Today every
//! partition resolves to `Local` — this is a single process — so the indirection
//! is behaviour-preserving and free. It is the load-bearing seam for distributed
//! scaling (see `docs/distributed-scaling-design.md`): stage 1 turns some slots
//! into `Remote` and routes those commands over the network, without disturbing
//! the engine core or the local fast path.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use nanobpmn_engine_core::{Key, partition_of};

use crate::cluster::Topology;
use crate::deepthi::DeepthiHandle;

/// Identifies a partition by its id — the value encoded in the high bits of
/// every [`Key`] it mints (see [`nanobpmn_engine_core::partition_of`]).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PartitionId(pub u64);

/// Identifies a node in the cluster (an index into [`Topology::peers`]). In a
/// single-node cluster every partition is [`Location::Local`], so no `NodeId` is
/// ever produced.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)] // the inner id is read by the stage-1 forwarding layer
pub struct NodeId(pub u32);

/// Where a partition's leader currently lives, as resolved by
/// [`PartitionRouter::resolve`]. A single-node cluster only ever yields
/// [`Location::Local`].
#[allow(dead_code)] // Remote's NodeId is read by the stage-1 forwarding layer
pub enum Location<'a> {
    /// The partition is owned by this node; here is its engine actor.
    Local(&'a DeepthiHandle),
    /// The partition is owned by a remote node; forward to it (its base URL is
    /// [`Topology::peer_addr`]).
    Remote(NodeId),
}

/// Owner of a partition slot. `Local(i)` indexes into [`PartitionRouter::local`];
/// `Remote(node)` names the owning node.
#[derive(Clone, Copy)]
enum Owner {
    Local(usize),
    Remote(NodeId),
}

/// Maps each [`PartitionId`] to its current [`Location`].
///
/// A node owns a subset of the cluster's partitions ([`Topology::local_partitions`])
/// and holds an [`DeepthiHandle`] for each; the rest resolve `Remote(NodeId)`. The
/// router is the single resolution point for all command routing, so the gateway
/// forwarding layer only has to handle the [`Location::Remote`] arm — the local
/// fast path and the engine core are untouched. In a single-node cluster every
/// slot is `Local`, identical to pre-cluster behaviour.
pub struct PartitionRouter {
    /// Engine actors for the partitions this node owns, in ascending partition-id
    /// order. `local[i]` is referenced by an `Owner::Local(i)` slot.
    local: Vec<DeepthiHandle>,
    /// One entry per partition id (`owners[p]` owns partition `p`): `Local(i)`
    /// when this node owns it, `Remote(node)` otherwise.
    owners: Vec<Owner>,
    /// The cluster topology this router was built from (node ids → addresses,
    /// total partition count, ownership map).
    #[allow(dead_code)] // read via topology() once clustered startup is wired
    topology: Topology,
}

impl PartitionRouter {
    /// Builds a single-node router that owns every partition locally.
    /// `handles[i]` becomes the owner of [`PartitionId`] `i`. Must be non-empty.
    fn single_node(handles: Vec<DeepthiHandle>) -> Self {
        assert!(!handles.is_empty(), "at least one partition is required");
        let owners = (0..handles.len()).map(Owner::Local).collect();
        let topology = Topology::single(handles.len() as u64);
        Self {
            local: handles,
            owners,
            topology,
        }
    }

    /// Builds a router from a cluster [`Topology`]. `local_handles` are the engine
    /// actors for this node's owned partitions, in the same ascending order as
    /// [`Topology::local_partitions`]; every other partition resolves to the
    /// `Remote` node that owns it. A single-node topology is equivalent to
    /// [`single_node`](Self::single_node).
    fn from_topology(topology: Topology, local_handles: Vec<DeepthiHandle>) -> Self {
        let owned = topology.local_partitions();
        assert_eq!(
            owned.len(),
            local_handles.len(),
            "expected one engine handle per owned partition ({} owned, {} handles)",
            owned.len(),
            local_handles.len(),
        );
        // Map each owned partition id to its index in `local_handles`.
        let mut owners: Vec<Owner> = (0..topology.num_partitions)
            .map(|p| Owner::Remote(NodeId(topology.owner_of(p))))
            .collect();
        for (i, &p) in owned.iter().enumerate() {
            owners[p as usize] = Owner::Local(i);
        }
        assert!(!owners.is_empty(), "at least one partition is required");
        Self {
            local: local_handles,
            owners,
            topology,
        }
    }

    /// Total number of partitions in the cluster (local + remote). Single-node:
    /// equal to the number of local engine actors.
    fn partition_count(&self) -> usize {
        self.owners.len()
    }

    /// Resolves a partition to its current [`Location`]. An out-of-range id
    /// (a malformed key) falls back to partition 0 so it surfaces as a clean
    /// engine "not found" rather than a panic — preserving the historical
    /// single-partition behaviour.
    pub fn resolve(&self, p: PartitionId) -> Location<'_> {
        match self.owners.get(p.0 as usize) {
            Some(Owner::Local(i)) => Location::Local(&self.local[*i]),
            Some(Owner::Remote(n)) => Location::Remote(*n),
            None => Location::Local(&self.local[0]),
        }
    }

    /// The engine actor owning `p`, for the single-process fast path. Resolves
    /// through [`resolve`](Self::resolve) and unwraps the local case; a `Remote`
    /// slot is unreachable while running as a single process (stage 1 migrates
    /// the affected call sites to handle [`Location::Remote`] explicitly).
    fn local_for(&self, p: PartitionId) -> &DeepthiHandle {
        match self.resolve(p) {
            Location::Local(h) => h,
            Location::Remote(_) => {
                debug_assert!(
                    false,
                    "local_for called on a remote partition; callers must check resolve()/locate() and forward Remote"
                );
                &self.local[0]
            }
        }
    }

    /// All engine actors owned by this node (in partition-id order). Used by the
    /// operations that fan out locally: job activation, message correlation,
    /// timer ticks, eviction, idle compaction.
    fn local_handles(&self) -> &[DeepthiHandle] {
        &self.local
    }

    /// The cluster topology backing this router.
    #[allow(dead_code)] // surfaced via Partitions::topology() in the next increment
    fn topology(&self) -> &Topology {
        &self.topology
    }
}

/// Create-admission backpressure for the read-model exporter queue. Holds the
/// per-shard byte budget and one `Arc<AtomicU64>` gauge per **local** partition,
/// in ascending-partition order (aligned with [`PartitionRouter::local_handles`]
/// by index). The writer increments a gauge when it forwards a command's events;
/// the exporter thread decrements it once projected. [`Partitions::for_create`]
/// reads them to steer a create away from a saturated shard, and
/// [`Partitions::exporter_all_saturated`] reports when every local shard is at
/// budget (the create-admission shed condition).
struct ExporterBackpressure {
    /// Per-shard queued-bytes budget.
    budget: u64,
    /// One gauge per local partition, indexed like `local_handles()`.
    gauges: Vec<Arc<AtomicU64>>,
    /// Round-robin cursor for steering creates across shards with headroom,
    /// independent of `next_create` so steering and plain balancing don't
    /// interfere. Relaxed: it only needs to spread, not be exact.
    steer: AtomicUsize,
}

/// A cloneable router over one engine actor per partition.
///
/// Cheap to clone (it shares the underlying [`PartitionRouter`] and the
/// round-robin counters). Routing is a pure index computation; no locking.
#[derive(Clone)]
pub struct Partitions {
    router: Arc<PartitionRouter>,
    /// Round-robin cursor for balancing `createProcessInstance` across
    /// partitions. Relaxed is fine: it only needs to spread load, not be exact.
    next_create: Arc<AtomicUsize>,
    /// Round-robin cursor for the partition at which a job-activation pass begins
    /// its probe. Without it every activation starts at partition 0, so under
    /// load all workers hammer partition 0's engine thread while the others idle
    /// for activation. Rotating the start spreads activation evenly across every
    /// partition's writer; a pass still probes onward when its start partition is
    /// empty, so no job is ever left unactivated (no starvation). Relaxed: it
    /// only needs to spread load, not be exact.
    next_activate: Arc<AtomicUsize>,
    /// Round-robin cursor for cluster-wide create *placement*: it cycles over
    /// every partition in the cluster (local and remote), so a single gateway
    /// spreads creates across the whole cluster instead of only its own
    /// partitions. Drives [`next_create_placement`](Self::next_create_placement).
    /// Distinct from `next_create` (which balances among local handles once a
    /// create lands locally). Relaxed: spread, not exact.
    next_place: Arc<AtomicUsize>,
    /// Read-model exporter-queue backpressure, wired once at startup (after the
    /// engine exists) via [`set_exporter_backpressure`](Self::set_exporter_backpressure).
    /// `None`-until-set (and stays unset when the feature is off), so the create
    /// fast path pays only a cheap `OnceLock::get` when it is disabled.
    exporter_bp: Arc<OnceLock<ExporterBackpressure>>,
}

impl Partitions {
    /// Wraps one engine actor per partition for a single-node cluster.
    /// `handles[i]` owns partition id `i`. Must be non-empty.
    pub fn new(handles: Vec<DeepthiHandle>) -> Self {
        Self {
            router: Arc::new(PartitionRouter::single_node(handles)),
            next_create: Arc::new(AtomicUsize::new(0)),
            next_activate: Arc::new(AtomicUsize::new(0)),
            next_place: Arc::new(AtomicUsize::new(0)),
            exporter_bp: Arc::new(OnceLock::new()),
        }
    }

    /// Builds a router for a clustered node from its [`Topology`]. `local_handles`
    /// are the engine actors for this node's owned partitions, in ascending
    /// partition-id order (matching [`Topology::local_partitions`]); every other
    /// partition resolves to the remote node that owns it.
    pub fn with_topology(topology: Topology, local_handles: Vec<DeepthiHandle>) -> Self {
        Self {
            router: Arc::new(PartitionRouter::from_topology(topology, local_handles)),
            next_create: Arc::new(AtomicUsize::new(0)),
            next_activate: Arc::new(AtomicUsize::new(0)),
            next_place: Arc::new(AtomicUsize::new(0)),
            exporter_bp: Arc::new(OnceLock::new()),
        }
    }

    /// The cluster topology (node addresses, ownership map, partition count).
    #[allow(dead_code)] // consumed by the stage-1 forwarding layer
    pub fn topology(&self) -> &Topology {
        self.router.topology()
    }

    /// The local engine actor that owns global partition id `p`, or `None` when
    /// `p` is owned by a remote node (or out of range). Use this to route an
    /// operation addressed by *global partition id* (e.g. evicting a completed
    /// instance by its key's partition) — unlike indexing [`all`](Self::all),
    /// which is the compacted slice of owned handles, not indexed by global id.
    pub fn local_for_partition(&self, p: u64) -> Option<&DeepthiHandle> {
        match self.router.resolve(PartitionId(p)) {
            Location::Local(h) => Some(h),
            Location::Remote(_) => None,
        }
    }

    /// Number of partitions.
    // A `Partitions` always owns at least one partition, so there is no
    // meaningful `is_empty`; `len` is used only by the gateway binary's tests
    // (dead in a plain build, hence the `dead_code` allow).
    #[allow(dead_code, clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.router.partition_count()
    }

    /// True when running a single partition (the common, zero-overhead case).
    pub fn is_single(&self) -> bool {
        self.router.partition_count() == 1
    }

    /// Resolves a key to the [`Location`] of its owning partition. The routing
    /// seam stage 1 builds on: today every key resolves [`Location::Local`].
    #[allow(dead_code)]
    pub fn locate(&self, key: Key) -> Location<'_> {
        self.router.resolve(PartitionId(partition_of(key)))
    }

    /// The handle owning `key`, decoded from the key's partition bits. A key
    /// whose partition is out of range (malformed input) falls back to
    /// partition 0 so a bad key surfaces as a clean engine "not found" rather
    /// than a panic.
    pub fn by_key(&self, key: Key) -> &DeepthiHandle {
        self.router.local_for(PartitionId(partition_of(key)))
    }

    /// The id of the remote node owning `key`'s partition, or `None` when this
    /// node owns it (the local fast path) — the by-key forwarding seam. A
    /// gateway uses this to decide whether a by-key operation
    /// (complete/fail/cancel/…) must be forwarded to a peer over the command
    /// stream. Single-node clusters always return `None`.
    pub fn remote_owner(&self, key: Key) -> Option<u32> {
        match self.router.resolve(PartitionId(partition_of(key))) {
            Location::Local(_) => None,
            Location::Remote(NodeId(node)) => Some(node),
        }
    }

    /// The set of remote nodes this gateway can forward to: every node that owns
    /// at least one partition not owned locally, in ascending id order. Empty on a
    /// single-node cluster (every partition is `Local`), so the dispatcher's
    /// job-aggregation fan-out becomes a no-op and the hot path stays unchanged.
    pub fn peer_nodes(&self) -> Vec<u32> {
        let mut nodes: Vec<u32> = (0..self.router.partition_count() as u64)
            .filter_map(|p| match self.router.resolve(PartitionId(p)) {
                Location::Local(_) => None,
                Location::Remote(NodeId(node)) => Some(node),
            })
            .collect();
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }

    /// round-robin across the partitions **this node owns**. An instance lives on
    /// the partition that created it for its whole life (its key embeds the
    /// partition). In a single-node cluster the owned set is every partition in
    /// id order, so this is byte-identical to the pre-cluster round-robin; in a
    /// multi-node cluster each node creates only on its own partitions (the hot
    /// path needs no cross-node forwarding — clients spread across gateways).
    ///
    /// When exporter-queue backpressure is wired, this **steers** the create to a
    /// local shard whose export queue still has headroom (round-robin among those
    /// under budget), so a single saturated shard stops attracting new creates and
    /// its resident backlog drains rather than growing. If every local shard is at
    /// budget it falls back to plain round-robin; the create-admission shed gate
    /// (see [`exporter_all_saturated`](Self::exporter_all_saturated)) rejects in
    /// that case, so the writer is never blocked.
    ///
    /// Panics if this node hosts no local partition (startup rejects that
    /// topology); use [`try_for_create`](Self::try_for_create) where a
    /// recoverable error is wanted instead.
    pub fn for_create(&self) -> &DeepthiHandle {
        self.try_for_create()
            .expect("for_create requires at least one local partition")
    }

    /// [`for_create`](Self::for_create), but `None` (never a panic) when this
    /// node hosts no local partition.
    pub fn try_for_create(&self) -> Option<&DeepthiHandle> {
        let locals = self.router.local_handles();
        if locals.is_empty() {
            return None;
        }
        if locals.len() == 1 {
            return Some(&locals[0]);
        }
        if let Some(bp) = self.exporter_bp.get() {
            // Steer to the first shard with headroom, scanning round-robin from a
            // rotating start so load spreads evenly across the unsaturated shards.
            debug_assert_eq!(bp.gauges.len(), locals.len());
            let n = locals.len();
            let start = bp.steer.fetch_add(1, Ordering::Relaxed) % n;
            for off in 0..n {
                let i = (start + off) % n;
                if bp.gauges[i].load(Ordering::Relaxed) < bp.budget {
                    return Some(&locals[i]);
                }
            }
            // Every shard saturated: fall through to plain round-robin (the
            // create is about to be shed by admission control anyway).
        }
        let i = self.next_create.fetch_add(1, Ordering::Relaxed) % locals.len();
        Some(&locals[i])
    }

    /// Wires read-model exporter-queue backpressure. Called once at startup after
    /// the engine actors exist. `gauges` are one queued-bytes gauge per local
    /// partition in the same ascending-partition order as
    /// [`PartitionRouter::local_handles`], sharing the `Arc`s the writer
    /// increments and the exporter thread decrements; `budget` is the per-shard
    /// byte budget. A no-op (logs a bug in debug) if called twice.
    pub fn set_exporter_backpressure(&self, gauges: Vec<Arc<AtomicU64>>, budget: u64) {
        debug_assert_eq!(
            gauges.len(),
            self.router.local_handles().len(),
            "one exporter gauge per local partition"
        );
        let set = self.exporter_bp.set(ExporterBackpressure {
            budget,
            gauges,
            steer: AtomicUsize::new(0),
        });
        debug_assert!(set.is_ok(), "exporter backpressure set twice");
    }

    /// Sum of every local shard's exporter-queue byte gauge — the resident
    /// read-model export backlog (events forwarded to exporter threads but not yet
    /// projected). `0` when backpressure is disabled. A cheap relaxed sum with no
    /// engine round-trip; sampled for the `nanobpm_exporter_queue_bytes` gauge to
    /// attribute the in-flight pipeline share of the RSS balloon.
    pub fn exporter_queued_bytes_total(&self) -> u64 {
        match self.exporter_bp.get() {
            None => 0,
            Some(bp) => bp.gauges.iter().map(|g| g.load(Ordering::Relaxed)).sum(),
        }
    }

    /// Whether every local shard's exporter queue is at or above budget — the
    /// create-admission shed condition. `false` when backpressure is disabled
    /// (unbounded) or any shard still has headroom. Cheap relaxed loads; no
    /// engine round-trip.
    pub fn exporter_all_saturated(&self) -> bool {
        match self.exporter_bp.get() {
            None => false,
            Some(bp) => bp
                .gauges
                .iter()
                .all(|g| g.load(Ordering::Relaxed) >= bp.budget),
        }
    }

    /// The **least-full** local shard's exporter-queue fill, in per-mille of its
    /// budget (`0` = empty … `1000` = at budget, `>1000` = over). Because
    /// `for_create` steers each create to the *least*-full shard, the create-
    /// admission pressure is the *minimum* fill across shards — the same condition
    /// [`exporter_all_saturated`](Self::exporter_all_saturated) tests at the `1000`
    /// point, generalised to a graded signal for the Tier-1 guard. `0` when
    /// backpressure is disabled or the budget is `0`. Cheap relaxed loads.
    pub fn exporter_min_fill_permille(&self) -> u64 {
        match self.exporter_bp.get() {
            None => 0,
            Some(bp) if bp.budget == 0 => 0,
            Some(bp) => bp
                .gauges
                .iter()
                .map(|g| g.load(Ordering::Relaxed).saturating_mul(1000) / bp.budget)
                .min()
                .unwrap_or(0),
        }
    }

    /// Cluster-wide create *placement*: round-robins over **every** partition in
    /// the cluster and returns the remote node that owns the chosen partition, or
    /// `None` when it is local (create here, the fast path). This lets a single
    /// gateway spread `createProcessInstance` across the whole cluster — the
    /// stage-1 create-forwarding seam — rather than only its own partitions, so
    /// one client connection can drive every node. A remote placement is forwarded
    /// to the owner over the Falcon protocol; a local one runs in-process via
    /// [`for_create`](Self::for_create).
    ///
    /// Single-node (and any node owning every partition) always returns `None`:
    /// the placement is always local, so the create path is byte-identical to the
    /// pre-cluster behaviour with zero forwarding overhead.
    pub fn next_create_placement(&self) -> Option<u32> {
        let n = self.router.partition_count();
        if n <= 1 {
            return None;
        }
        let p = self.next_place.fetch_add(1, Ordering::Relaxed) % n;
        match self.router.resolve(PartitionId(p as u64)) {
            Location::Local(_) => None,
            Location::Remote(NodeId(node)) => Some(node),
        }
    }

    /// The next global partition id for cluster-wide create placement, advancing
    /// the shared round-robin cursor. Returns `None` for a single-partition
    /// cluster (placement is always local). Unlike [`next_create_placement`],
    /// which resolves the chosen partition to its *static owner*, this returns the
    /// raw partition id so a leader-aware caller (the Raft stream create path in
    /// `stream_leader_placement`) can route to the partition's current Raft
    /// **leader** instead of its owner — correct across failover, where leadership
    /// has moved off the owner (a down node's partitions lead from the incumbent;
    /// a recovered node reclaims them).
    pub fn next_create_partition(&self) -> Option<u64> {
        let n = self.router.partition_count();
        if n <= 1 {
            return None;
        }
        Some((self.next_place.fetch_add(1, Ordering::Relaxed) % n) as u64)
    }

    /// Number of partitions in the whole cluster (local + remote). Used by the
    /// create-placement protection layer (ADR 0014) to bound its reroute loop and
    /// to enumerate placement slots for load-aware weighting.
    pub fn partition_count(&self) -> usize {
        self.router.partition_count()
    }

    /// The remote node that owns global partition `p`, or `None` when this node
    /// owns it (a local placement). Used by the create-placement protection layer
    /// (ADR 0014) to enumerate the candidate owners of every placement slot.
    pub fn owner_of(&self, p: u64) -> Option<u32> {
        match self.router.resolve(PartitionId(p)) {
            Location::Local(_) => None,
            Location::Remote(NodeId(node)) => Some(node),
        }
    }

    /// Blind-round-robin create placement that **skips** owners in `tried`
    /// (ADR 0014 `protect` reroute). Probes forward from the shared placement
    /// cursor over every partition; returns the first owner that is remote and not
    /// yet tried, `None` when the sweep reaches a local slot or exhausts every
    /// partition without an untried remote owner (the caller then creates
    /// locally). Never reorders the base rotation for the un-rerouted first pick.
    pub fn next_create_placement_avoiding(&self, tried: &[u32]) -> Option<u32> {
        let n = self.router.partition_count();
        if n <= 1 {
            return None;
        }
        for _ in 0..n {
            let p = self.next_place.fetch_add(1, Ordering::Relaxed) % n;
            match self.router.resolve(PartitionId(p as u64)) {
                Location::Local(_) => return None,
                Location::Remote(NodeId(node)) => {
                    if !tried.contains(&node) {
                        return Some(node);
                    }
                }
            }
        }
        None
    }

    /// The local-partition index at which the next job-activation pass should
    /// begin probing, chosen round-robin. A pass probes this node's owned
    /// partitions in wrap-around order from here, so activation load spreads
    /// evenly across every local engine thread instead of concentrating on the
    /// first. Returns 0 for a single owned partition (the probe order is trivial).
    pub fn activate_start(&self) -> usize {
        let n = self.router.local_handles().len();
        if n <= 1 {
            return 0;
        }
        self.next_activate.fetch_add(1, Ordering::Relaxed) % n
    }

    /// Like [`for_create`](Self::for_create) but restricted to an explicit
    /// `candidates` set (the partitions this node currently *leads*,
    /// which after a failover differs from the statically owned set). Round-robins
    /// over the candidates using the shared create counter so create load spreads
    /// across every partition this node can commit to without a cross-node hop.
    /// Returns `None` when `candidates` is empty (this node leads nothing).
    pub fn for_create_among(&self, candidates: &[u64]) -> Option<u64> {
        match candidates.len() {
            0 => None,
            1 => Some(candidates[0]),
            n => {
                let i = self.next_create.fetch_add(1, Ordering::Relaxed) % n;
                Some(candidates[i])
            }
        }
    }

    /// The partition that owns deployments. Deployments are processed and
    /// journaled here, then replicated in-memory to the others (so every
    /// partition can instantiate the definition). Partition 0 also owns the
    /// single copy of each message-start / timer-start subscription.
    pub fn deploy_partition(&self) -> &DeepthiHandle {
        self.router.local_for(PartitionId(0))
    }

    /// Sum of resident variable-payload bytes across every local partition — the
    /// attribution gauge for the burst RSS balloon (does the resident instance
    /// variable footprint track the jemalloc live-heap peak, or is the balloon
    /// in-flight pipeline copies instead?). Runs the O(N) scan on each engine
    /// thread at `Low` priority so it never preempts completion work; awaited
    /// only by the ~1 Hz mem-pressure sampler, off the hot path.
    pub async fn resident_variable_bytes_total(&self) -> u64 {
        let mut total = 0u64;
        for h in self.all() {
            total += h
                .with_low(|j: &mut crate::journal::Journal| j.resident_variable_bytes())
                .await;
        }
        total
    }

    /// All partition handles, for operations that must fan out (job activation,
    /// message correlation, timer ticks, eviction, idle compaction).
    pub fn all(&self) -> &[DeepthiHandle] {
        self.router.local_handles()
    }

    /// Total depth of every partition's `Low` (creation) queue: the standing
    /// backlog of submitted-but-not-yet-applied creates across the node. The
    /// create-admission gate bounds this to cap create-side latency under overload.
    pub fn pending_create_queue(&self) -> usize {
        self.router
            .local_handles()
            .iter()
            .map(DeepthiHandle::pending_low)
            .sum()
    }

    /// Activatable (waiting) job counts per job type, summed across every owned
    /// partition. Feeds the ~1 Hz worker-provisioning monitor (paired with the
    /// falcon worker roster to flag under-provisioned / starved job types). Runs
    /// each partition's cheap map walk at `Low` priority so it never preempts
    /// completion work, and awaited only off the hot path.
    pub async fn activatable_job_counts(&self) -> std::collections::HashMap<String, u64> {
        let mut totals: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for h in self.all() {
            let per_partition = h
                .with_low(|j: &mut crate::journal::Journal| {
                    j.state()
                        .activatable_jobs
                        .iter()
                        .map(|(job_type, jobs)| (job_type.clone(), jobs.len() as u64))
                        .collect::<Vec<_>>()
                })
                .await;
            for (job_type, count) in per_partition {
                *totals.entry(job_type).or_insert(0) += count;
            }
        }
        totals
    }

    /// Total runnable (task-job) backlog: the count of *live* jobs — both
    /// `Created` (waiting for a worker) and `Activated` (leased, in-flight at a
    /// worker) — summed across every owned partition. This is the parked-excluded
    /// O(active) congestion signal the admission gate and the self-optimizing
    /// backlog governor read: only service tasks create jobs, so instances parked
    /// on timers/messages never appear here. Unlike
    /// [`activatable_job_counts`](Self::activatable_job_counts) (which counts only
    /// the *waiting* front), this includes leased-but-uncompleted jobs, so a
    /// worker-starved backlog that has drained into the activated set is still
    /// counted. It deliberately excludes terminal (`Completed`/`Failed`/`Errored`)
    /// jobs that linger in hot state until the exporter evicts their instance:
    /// folding those in (via a raw `jobs.len()`) let an exporter that fell behind
    /// — or was stalled by a locked read-model store — inflate the signal with
    /// dead weight and shed legitimate new work, a throughput collapse that never
    /// cleared. See [`State::live_job_count`](nanobpmn_engine_core::State::live_job_count).
    /// Runs each partition's cheap `live_job_count()` at `Low` priority (never
    /// preempts completion) and is awaited only off the hot path.
    pub async fn job_backlog(&self) -> usize {
        let mut total = 0usize;
        for h in self.all() {
            total += h
                .with_low(|j: &mut crate::journal::Journal| j.state().live_job_count())
                .await;
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::compose_key;

    use super::*;
    use crate::journal::Journal;

    fn spawn_partitions(n: u64) -> Partitions {
        let handles = (0..n)
            .map(|i| DeepthiHandle::spawn(Journal::in_memory_partition(i), i, None))
            .collect();
        Partitions::new(handles)
    }

    #[test]
    fn try_for_create_is_none_without_local_partitions() {
        // #1305 review: a node hosting no partition must yield a recoverable
        // `None`, not the modulo-by-zero panic of round-robin selection.
        // Node 1 of 2 with a single partition owns nothing locally.
        let topology = Topology {
            node_id: 1,
            peers: vec!["http://n0".into(), "http://n1".into()],
            num_partitions: 1,
            replication_factor: 1,
        };
        assert!(topology.local_partitions().is_empty());
        let parts = Partitions::with_topology(topology, Vec::new());
        assert!(parts.try_for_create().is_none());
        let parts = spawn_partitions(2);
        assert!(parts.try_for_create().is_some());
    }

    #[test]
    fn router_resolves_every_partition_locally() {
        let parts = spawn_partitions(4);
        assert_eq!(parts.len(), 4);
        assert!(!parts.is_single());
        for i in 0..4u64 {
            match parts.router.resolve(PartitionId(i)) {
                Location::Local(_) => {}
                Location::Remote(_) => {
                    panic!("partition {i} should be local in single-process mode")
                }
            }
        }
    }

    #[test]
    fn by_key_routes_to_the_partition_in_the_key() {
        let parts = spawn_partitions(4);
        // A key minted by partition 2 must resolve to the same handle as all()[2]
        // (both borrow router.local[2]).
        let key = compose_key(2, 7);
        assert_eq!(partition_of(key), 2);
        assert!(std::ptr::eq(parts.by_key(key), &parts.all()[2]));
    }

    #[test]
    fn out_of_range_key_falls_back_to_partition_zero() {
        let parts = spawn_partitions(2);
        // Partition id 9 doesn't exist (only 0, 1); must fall back, not panic.
        let key = compose_key(9, 1);
        assert!(std::ptr::eq(parts.by_key(key), &parts.all()[0]));
        match parts.router.resolve(PartitionId(9)) {
            Location::Local(_) => {}
            Location::Remote(_) => panic!("fallback must be local"),
        }
    }

    #[test]
    fn single_partition_is_single() {
        let parts = spawn_partitions(1);
        assert!(parts.is_single());
        assert_eq!(parts.len(), 1);
        assert_eq!(parts.activate_start(), 0);
    }

    #[test]
    fn for_create_steers_away_from_saturated_exporter_shards() {
        let parts = spawn_partitions(4);
        let gauges: Vec<Arc<AtomicU64>> = (0..4).map(|_| Arc::new(AtomicU64::new(0))).collect();
        let budget = 1000u64;
        parts.set_exporter_backpressure(gauges.clone(), budget);

        // Saturate shards 0, 1, 3; only shard 2 has headroom. Every create must
        // land on shard 2 regardless of the round-robin cursor.
        gauges[0].store(budget, Ordering::Relaxed);
        gauges[1].store(budget + 500, Ordering::Relaxed);
        gauges[3].store(budget, Ordering::Relaxed);
        assert!(!parts.exporter_all_saturated());
        for _ in 0..12 {
            assert!(
                std::ptr::eq(parts.for_create(), &parts.all()[2]),
                "creates must steer to the only shard under budget"
            );
        }

        // With every shard at budget, all-saturated reports true and for_create
        // falls back to plain round-robin (a valid handle; admission sheds it).
        gauges[2].store(budget, Ordering::Relaxed);
        assert!(parts.exporter_all_saturated());
        let _ = parts.for_create();

        // Draining a shard below budget clears the shed condition.
        gauges[2].store(0, Ordering::Relaxed);
        assert!(!parts.exporter_all_saturated());
        assert!(std::ptr::eq(parts.for_create(), &parts.all()[2]));
    }

    #[test]
    fn exporter_all_saturated_is_false_when_unwired() {
        // No backpressure wired -> unbounded -> never sheds.
        let parts = spawn_partitions(4);
        assert!(!parts.exporter_all_saturated());
    }

    #[test]
    fn clustered_router_resolves_owned_local_and_others_remote() {
        // Node 0 of a 2-node, 4-partition cluster owns partitions 0 and 2; it
        // holds engine handles only for those. Partitions 1 and 3 must resolve
        // Remote(node 1); 0 and 2 must resolve Local.
        let topology = Topology {
            node_id: 0,
            peers: vec!["http://n0".into(), "http://n1".into()],
            num_partitions: 4,
            replication_factor: 1,
        };
        let owned = topology.local_partitions();
        assert_eq!(owned, vec![0, 2]);
        let handles: Vec<DeepthiHandle> = owned
            .iter()
            .map(|p| DeepthiHandle::spawn(Journal::in_memory_partition(*p), *p, None))
            .collect();
        let parts = Partitions::with_topology(topology, handles);

        assert_eq!(parts.len(), 4);
        assert!(!parts.is_single());
        // local() / all() only holds the owned partitions.
        assert_eq!(parts.all().len(), 2);

        for p in [0u64, 2] {
            match parts.router.resolve(PartitionId(p)) {
                Location::Local(_) => {}
                Location::Remote(_) => panic!("partition {p} should be Local on node 0"),
            }
        }
        for p in [1u64, 3] {
            match parts.router.resolve(PartitionId(p)) {
                Location::Remote(NodeId(1)) => {}
                Location::Remote(NodeId(other)) => {
                    panic!("partition {p} owner should be node 1, got {other}")
                }
                Location::Local(_) => panic!("partition {p} should be Remote on node 0"),
            }
        }
        // by_key of an owned partition resolves locally; both 0 and 2 present.
        assert!(std::ptr::eq(
            parts.by_key(compose_key(2, 1)),
            &parts.all()[1]
        ));
    }
}
