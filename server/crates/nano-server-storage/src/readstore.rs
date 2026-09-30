//! Server-side facade over the shared read model.
//!
//! The SQLite projection and read-query surface now live in the standalone
//! [`nanobpmn_read_model`] crate (package `nanobpmn-read-model`) so the exact
//! same schema, FNV-1a fingerprint, event projection and read queries can compile
//! both here (feature `native`, a real bundled C SQLite — what this gateway ships)
//! and, for the in-browser WASM test engine, against an in-memory SQLite. This
//! module is now a thin shim that:
//!
//! * re-exports the shared [`ReadStore`] and every `*Row` / `ExportOutcome` result
//!   type so existing `crate::readstore::*` paths keep resolving unchanged, and
//! * keeps the **server-only orchestration** that never belonged in the shared
//!   crate: the sharded [`ReadModel`] fan-out and the [`ProjectionSink`] exporter
//!   seam. The per-shard WAL/checkpoint and adaptive-pruning methods stay on
//!   [`ReadStore`] itself but are compiled only under the shared crate's `native`
//!   feature (which this crate always enables).

use std::collections::HashMap;
use std::sync::Arc;

use nanobpmn_engine_core::{Event, Key, ReadQuery, partition_of};
// Re-export the shared read-model surface so `crate::readstore::ProcessInstanceRow`
// (and friends) resolve exactly as before the extraction.
pub use nanobpmn_read_model::*;

/// Names the [`ReadStore`] query method each gateway REST read routes through.
///
/// This is the gateway-side anchor for the shared [`ReadQuery`] surface enum
/// (defined in `engine-core` alongside `Command`). It exists so the gateway
/// *references* every read it serves through the one shared enum: the match is
/// exhaustive and wildcard-free, so adding a `ReadQuery` variant — the gateway
/// beginning to serve a new read — forces this map to be extended here as well
/// as classified in the wasm parity gate (`engine-wasm/src/surface_parity.rs`).
/// It changes no observable REST behaviour; the concrete handlers still call the
/// named [`ReadStore`] methods directly.
///
/// A few reads are *derived* — they have no single backing method but compose
/// several. Their entry names the composed methods joined with `+` (documented
/// on the corresponding `ReadQuery` variant); the value is illustrative, the
/// anchoring purpose is the exhaustive match.
#[allow(dead_code)]
pub(crate) fn read_store_method(query: ReadQuery) -> &'static str {
    match query {
        ReadQuery::GetFormByKey => "form_by_key",
        ReadQuery::GetResourceByKey => "resource_by_key",
        ReadQuery::SearchResources => "resources_meta",
        ReadQuery::SearchProcessInstances => "process_instances",
        ReadQuery::GetProcessInstance => "process_instance",
        ReadQuery::SearchUserTasks => "user_tasks",
        ReadQuery::GetUserTask => "user_task",
        ReadQuery::SearchVariables => "variables",
        ReadQuery::GetVariable => "variable",
        ReadQuery::SearchJobs => "jobs",
        ReadQuery::SearchIncidents => "incidents",
        ReadQuery::GetIncident => "incident",
        ReadQuery::SearchElementInstances => "element_instances",
        ReadQuery::GetElementInstance => "element_instance",
        // Derived reads (composite; see `ReadQuery` docs).
        ReadQuery::SearchElementInstanceIncidents => "element_instances+incidents",
        ReadQuery::SearchElementInstanceWaitStates => {
            "element_instances+jobs+message_subscriptions"
        }
        ReadQuery::SearchMessageSubscriptions => "message_subscriptions",
        ReadQuery::SearchCorrelatedMessageSubscriptions => "correlated_message_subscriptions",
        ReadQuery::SearchProcessDefinitions => "process_definitions",
        ReadQuery::GetProcessDefinitionXml => "process_definition_xml",
        ReadQuery::SearchDecisionInstances => "decision_instances",
        ReadQuery::GetDecisionInstance => "decision_instance",
        ReadQuery::SearchDecisionDefinitions => "decision_definitions",
        ReadQuery::GetDecisionDefinitionXml => "decision_definition_xml",
        ReadQuery::SearchDecisionRequirements => "decision_requirements",
        ReadQuery::GetDecisionRequirementsXml => "decision_requirements_xml",
        ReadQuery::SearchAgentInstances => "agent_instances",
        ReadQuery::GetAgentInstance => "agent_instance",
        ReadQuery::SearchAgentHistory => "agent_history",
    }
}

/// The projection sink the per-shard exporter thread writes into. Abstracts the
/// read model behind the single `export` seam so the sink can be the built-in
/// local SQLite store (the default) or, in later milestones, a tee/remote sink
/// that streams the record log into an external system (data lake / warehouse)
/// and decouples read-model disk IOPS from the node (see issue #133).
///
/// The exporter thread is the one ordered point every projected event flows
/// through, in strict log (fsync) order, off the command-commit/ack hot path.
/// Any implementation MUST uphold the invariants the exporter relies on:
///
/// * **Idempotent** — projecting an overlapping prefix again (e.g. after a
///   restart replays from the last durable watermark) must be a no-op for the
///   already-applied events and yield an `inflight_delta`/`terminal_keys` that
///   count only *genuine* state transitions, never raw event occurrences.
/// * **Never lose a batch** — `export` must fully apply the batch or return an
///   error (so the exporter retries); it must not partially apply and report
///   success. `exported_position` (the compaction watermark) advances by event
///   count on the exporter thread only after `export` succeeds, so a silently
///   dropped batch is unrecoverable read-model loss.
/// * **Per-shard order** — events within a shard arrive log-ordered; the sink
///   must preserve that order.
pub trait ProjectionSink: Send + Sync {
    /// Projects a batch of consecutive, log-ordered journal events, returning the
    /// exact in-flight delta and the keys of instances that genuinely reached a
    /// terminal state in this batch. See the trait-level invariants.
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome>;

    /// Caps retained terminal instances at `max_keep`, deleting up to
    /// `max_delete` of the oldest beyond the cap (0 = unbounded). Returns the
    /// number evicted. A no-op for append-only sinks that don't retain state.
    fn prune_terminal(&self, max_keep: usize, max_delete: usize) -> anyhow::Result<usize> {
        let _ = (max_keep, max_delete);
        Ok(0)
    }
}

impl ProjectionSink for ReadStore {
    fn export(&self, events: &[&Event]) -> anyhow::Result<ExportOutcome> {
        Ok(ReadStore::export(self, events)?)
    }

    fn prune_terminal(&self, max_keep: usize, max_delete: usize) -> anyhow::Result<usize> {
        Ok(ReadStore::prune_terminal_instances(
            self, max_keep, max_delete,
        )?)
    }
}

/// A sharded read model: one [`ReadStore`] per owned partition, presenting the
/// single-store query API by routing point lookups to the owning partition's
/// shard and merging scans/counts across shards. This is opening #1 — the read
/// model was the per-node throughput ceiling because ONE exporter thread +
/// `Mutex<Connection>` projected every partition's events on a single core;
/// sharding by partition lets projection (and the read store's SQLite writer)
/// scale with cores.
///
/// Invariants: each shard only ever sees its own partition's events (the shared
/// journal writer routes by partition and boot catch-up demuxes by partition),
/// so a shard's `exported_position` is exactly its partition's projected event
/// count. Process definitions are replicated to every owned partition under the
/// same (partition-0) key, so any shard answers a definition query.
pub struct ReadModel {
    /// Shards indexed positionally; `slot_by_partition` maps a global partition
    /// id to its index here.
    shards: Vec<Arc<ReadStore>>,
    slot_by_partition: HashMap<u64, usize>,
}

impl ReadModel {
    /// Builds a read model from `(global_partition_id, shard)` pairs. Requires at
    /// least one shard (a node always owns at least one partition).
    pub fn from_shards(shards: Vec<(u64, Arc<ReadStore>)>) -> Self {
        assert!(!shards.is_empty(), "read model needs at least one shard");
        let mut slot_by_partition = HashMap::with_capacity(shards.len());
        let mut list = Vec::with_capacity(shards.len());
        for (pid, store) in shards {
            let prev = slot_by_partition.insert(pid, list.len());
            assert!(
                prev.is_none(),
                "read model got duplicate partition id {pid}; owned partition list must be unique"
            );
            list.push(store);
        }
        Self {
            shards: list,
            slot_by_partition,
        }
    }

    /// A single in-memory shard for partition 0 — the trivial (single-partition /
    /// test) case.
    pub fn single_in_memory() -> Self {
        Self::from_shards(vec![(
            0,
            Arc::new(ReadStore::open(None).expect("open in-memory read store")),
        )])
    }

    /// In-memory shards, one per partition in `owned`.
    pub fn in_memory_partitions(owned: &[u64]) -> Self {
        let shards = owned
            .iter()
            .map(|&p| {
                (
                    p,
                    Arc::new(ReadStore::open(None).expect("open in-memory read store")),
                )
            })
            .collect();
        Self::from_shards(shards)
    }

    /// The shards paired with their global partition ids, for wiring exporter
    /// threads and gathering per-partition compaction watermarks.
    pub fn shards(&self) -> Vec<(u64, Arc<ReadStore>)> {
        let mut out = vec![None; self.shards.len()];
        for (&pid, &idx) in &self.slot_by_partition {
            out[idx] = Some((pid, Arc::clone(&self.shards[idx])));
        }
        out.into_iter().flatten().collect()
    }

    /// Number of shards (owned partitions).
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard_for(&self, key: Key) -> Option<&ReadStore> {
        self.slot_by_partition
            .get(&partition_of(key))
            .map(|&i| self.shards[i].as_ref())
    }

    // --- point lookups: route to the key's owning partition shard ---

    pub fn process_instance(&self, key: Key) -> Option<ProcessInstanceRow> {
        self.shard_for(key)?.process_instance(key)
    }

    /// Resolves the `rootProcessInstanceKey` for `key` by walking the
    /// `parentProcessInstanceKey` chain to the top-level ancestor (C8 parity,
    /// issue #977).
    ///
    /// A top-level instance (no parent) roots to its own key. A call-activity
    /// child — however deeply nested — roots to the top-level process instance
    /// that started the whole tree. This node's point lookups route per key, so
    /// the walk would cross partitions transparently were an ancestor on another
    /// local shard; in practice a call-activity hierarchy is partition-co-located
    /// (the engine mints a child on its parent's partition), so the whole chain
    /// resolves within one shard.
    ///
    /// Delegates to the shared [`resolve_root_process_instance_key`] so the
    /// gateway, the single-partition [`ReadStore`] and the `engine-wasm`
    /// `TestEngine` all resolve roots with the identical algorithm (no drift);
    /// best-effort boundaries and the cycle-guarding visited set are documented
    /// there.
    pub fn root_process_instance_key(&self, key: Key) -> Key {
        resolve_root_process_instance_key(key, |k| self.process_instance(k))
    }

    pub fn incident(&self, key: Key) -> Option<IncidentRow> {
        self.shard_for(key)?.incident(key)
    }

    pub fn element_instance(&self, key: Key) -> Option<ElementInstanceRow> {
        self.shard_for(key)?.element_instance(key)
    }

    pub fn variable(&self, key: Key) -> Option<VariableRow> {
        self.shard_for(key)?.variable(key)
    }

    pub fn instance_variables(&self, instance_key: Key) -> Vec<VariableRow> {
        self.shard_for(instance_key)
            .map(|s| s.instance_variables(instance_key))
            .unwrap_or_default()
    }

    /// The `Active` element instances for one process instance, routed to the
    /// instance's owning shard and selected by the `instance_key` index.
    pub fn active_element_instances(&self, instance_key: Key) -> Vec<ElementInstanceRow> {
        self.shard_for(instance_key)
            .map(|s| s.active_element_instances(instance_key))
            .unwrap_or_default()
    }

    // --- definitions: replicated to every owned partition under the same key ---

    pub fn process_definitions(&self) -> Vec<ProcessDefinitionRow> {
        // Every shard holds every definition (replicated on deploy); read from
        // the first shard, falling back if it has none yet (mid-catch-up).
        for s in &self.shards {
            let defs = s.process_definitions();
            if !defs.is_empty() {
                return defs;
            }
        }
        Vec::new()
    }

    /// Fetches a single process definition by key across shards (every shard
    /// holds every definition, so the first shard that knows the key answers).
    pub fn process_definition_by_key(&self, key: Key) -> Option<ProcessDefinitionRow> {
        for s in &self.shards {
            if let Some(row) = s.process_definition_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn process_definition_xml(&self, key: Key) -> Option<String> {
        for s in &self.shards {
            if let Some(xml) = s.process_definition_xml(key) {
                return Some(xml);
            }
        }
        None
    }

    pub fn process_definition_start_form_id(&self, key: Key) -> Option<Option<String>> {
        for s in &self.shards {
            if let Some(row) = s.process_definition_start_form_id(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn decision_requirements(&self) -> Vec<DecisionRequirementsRow> {
        for s in &self.shards {
            let defs = s.decision_requirements();
            if !defs.is_empty() {
                return defs;
            }
        }
        Vec::new()
    }

    pub fn decision_requirements_by_key(&self, key: Key) -> Option<DecisionRequirementsRow> {
        for s in &self.shards {
            if let Some(row) = s.decision_requirements_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn decision_requirements_xml(&self, key: Key) -> Option<String> {
        for s in &self.shards {
            if let Some(xml) = s.decision_requirements_xml(key) {
                return Some(xml);
            }
        }
        None
    }

    pub fn decision_definitions(&self) -> Vec<DecisionDefinitionRow> {
        for s in &self.shards {
            let defs = s.decision_definitions();
            if !defs.is_empty() {
                return defs;
            }
        }
        Vec::new()
    }

    pub fn decision_definition_by_key(&self, key: Key) -> Option<DecisionDefinitionRow> {
        for s in &self.shards {
            if let Some(row) = s.decision_definition_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn decision_definition_xml(&self, key: Key) -> Option<String> {
        for s in &self.shards {
            if let Some(xml) = s.decision_definition_xml(key) {
                return Some(xml);
            }
        }
        None
    }

    pub fn form_by_key(&self, key: Key) -> Option<FormRow> {
        for s in &self.shards {
            if let Some(row) = s.form_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn form_by_id(&self, form_id: &str) -> Option<FormRow> {
        for s in &self.shards {
            if let Some(row) = s.form_by_id(form_id) {
                return Some(row);
            }
        }
        None
    }

    pub fn resource_by_key(&self, key: Key) -> Option<ResourceRow> {
        for s in &self.shards {
            if let Some(row) = s.resource_by_key(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn resource_by_key_meta(&self, key: Key) -> Option<ResourceMetaRow> {
        for s in &self.shards {
            if let Some(row) = s.resource_by_key_meta(key) {
                return Some(row);
            }
        }
        None
    }

    pub fn resources_meta(&self) -> Vec<ResourceMetaRow> {
        self.shards
            .iter()
            .flat_map(|s| s.resources_meta())
            .collect()
    }

    pub fn process_instances(&self) -> Vec<ProcessInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.process_instances())
            .collect()
    }

    pub fn jobs(&self) -> Vec<JobRow> {
        self.shards.iter().flat_map(|s| s.jobs()).collect()
    }

    pub fn user_tasks(&self) -> Vec<UserTaskRow> {
        self.shards.iter().flat_map(|s| s.user_tasks()).collect()
    }

    pub fn user_task(&self, key: Key) -> Option<UserTaskRow> {
        self.shards.iter().find_map(|s| s.user_task(key))
    }

    pub fn incidents(&self) -> Vec<IncidentRow> {
        self.shards.iter().flat_map(|s| s.incidents()).collect()
    }

    pub fn element_instances(&self) -> Vec<ElementInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.element_instances())
            .collect()
    }

    /// Every projected agent instance across all shards. Filtering, sorting and
    /// pagination are applied by the REST handler (mirroring `element_instances`)
    /// so cross-shard ordering has a single home; the shared read store is the
    /// source of the rows.
    /// Panics on query failure; request handlers use [`Self::try_agent_instances`].
    pub fn agent_instances(&self) -> Vec<AgentInstanceRow> {
        self.try_agent_instances().expect("query agent_instances")
    }

    pub fn try_agent_instances(&self) -> rusqlite::Result<Vec<AgentInstanceRow>> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            rows.extend(shard.try_agent_instances(&AgentInstanceFilter::default(), None)?);
        }
        Ok(rows)
    }

    /// A single agent instance by its dedicated key, returning the first shard's
    /// match (keys are unique across shards, so at most one shard answers).
    /// Panics on query failure; request handlers use [`Self::try_agent_instance`].
    pub fn agent_instance(&self, key: Key) -> Option<AgentInstanceRow> {
        self.try_agent_instance(key).expect("query agent_instance")
    }

    pub fn try_agent_instance(&self, key: Key) -> rusqlite::Result<Option<AgentInstanceRow>> {
        for shard in &self.shards {
            if let Some(row) = shard.try_agent_instance(key)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    /// The agent history turns matching `filter` across all shards. The
    /// `commit_status` default (COMMITTED-only when unset) lives in
    /// [`AgentHistoryFilter`], so passing the filter through preserves it as the
    /// single source of truth; the REST handler sorts and paginates the result.
    /// Panics on query failure; request handlers use [`Self::try_agent_history`].
    pub fn agent_history(&self, filter: &AgentHistoryFilter) -> Vec<AgentHistoryRow> {
        self.try_agent_history(filter).expect("query agent_history")
    }

    pub fn try_agent_history(
        &self,
        filter: &AgentHistoryFilter,
    ) -> rusqlite::Result<Vec<AgentHistoryRow>> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            rows.extend(shard.try_agent_history(filter, None)?);
        }
        Ok(rows)
    }

    /// Every open message subscription across all shards (MESSAGE wait states).
    pub fn message_subscriptions(&self) -> Vec<MessageSubscriptionRow> {
        self.shards
            .iter()
            .flat_map(|s| s.message_subscriptions())
            .collect()
    }

    /// Every open timer / signal / conditional wait across all shards
    /// (TIMER / SIGNAL / CONDITION wait states).
    pub fn event_waits(&self) -> Vec<EventWaitRow> {
        self.shards.iter().flat_map(|s| s.event_waits()).collect()
    }

    /// Every correlated (historical) message subscription across all shards.
    pub fn correlated_message_subscriptions(&self) -> Vec<CorrelatedMessageSubscriptionRow> {
        self.shards
            .iter()
            .flat_map(|s| s.correlated_message_subscriptions())
            .collect()
    }

    /// Every decision-instance row across all shards. Decision instances live in
    /// the shard of their owning process instance (routed by `max_key`), so a
    /// full listing must concatenate across shards.
    pub fn decision_instances(&self) -> Vec<DecisionInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.decision_instances())
            .collect()
    }

    /// A single decision-instance by its composite `<key>-<idx>` id. The row is
    /// keyed by a string (not a partition-encoding numeric key), so its shard is
    /// unknown — scan every shard for the first match.
    pub fn decision_instance(&self, eval_instance_key: &str) -> Option<DecisionInstanceRow> {
        for s in &self.shards {
            if let Some(row) = s.decision_instance(eval_instance_key) {
                return Some(row);
            }
        }
        None
    }

    /// Every decision-instance row for a `decision_evaluation_key`, concatenated
    /// across shards (the evaluation's rows live in a single shard, but which one
    /// is unknown from the key alone).
    pub fn decision_instances_by_evaluation_key(
        &self,
        decision_evaluation_key: Key,
    ) -> Vec<DecisionInstanceRow> {
        self.shards
            .iter()
            .flat_map(|s| s.decision_instances_by_evaluation_key(decision_evaluation_key))
            .collect()
    }

    pub fn variables(&self) -> Vec<VariableRow> {
        self.shards.iter().flat_map(|s| s.variables()).collect()
    }

    // --- counts: sum across shards ---

    pub fn active_instance_count(&self) -> usize {
        self.shards.iter().map(|s| s.active_instance_count()).sum()
    }

    /// Reconciles orphaned `Active` rows across every shard against the engine's
    /// authoritative live-instance set (`live` = hot ∪ cold keys for all owned
    /// partitions). Because instance keys are globally unique (they encode the
    /// partition), a single global `live` set is safe to apply to every shard: a
    /// key that is genuinely live on its own partition is present in `live` and is
    /// never reconciled. Returns the total rows reconciled — the amount by which
    /// the in-flight gauge was over-counting. See
    /// [`ReadStore::reconcile_orphaned_active`].
    pub fn reconcile_orphaned_active(&self, live: &std::collections::HashSet<Key>) -> usize {
        self.shards
            .iter()
            .map(|s| s.reconcile_orphaned_active(live))
            .sum()
    }

    pub fn process_instance_count(&self, filter: &InstanceFilter) -> i64 {
        self.shards
            .iter()
            .map(|s| s.process_instance_count(filter))
            .sum()
    }

    /// Sum of every shard's `exported_position`. Monotonic across all shards, so
    /// it is a valid change cursor for the console's instance stream. NOTE: this
    /// is NOT the compaction watermark — segment deletion uses the per-partition
    /// vector from [`ReadModel::exported_watermarks`] (a global sum could pass
    /// while a lagging shard still needs the segment).
    pub fn exported_position(&self) -> usize {
        self.shards.iter().map(|s| s.exported_position()).sum()
    }

    /// Per-partition exported watermarks indexed by global partition id (length
    /// `num_partitions`; non-owned partitions stay 0). Feeds
    /// [`crate::seglog::compact_multi`]'s per-partition export gate.
    pub fn exported_watermarks(&self, num_partitions: usize) -> Vec<u64> {
        let mut v = vec![0u64; num_partitions];
        for (&pid, &idx) in &self.slot_by_partition {
            if (pid as usize) < num_partitions {
                v[pid as usize] = self.shards[idx].exported_position() as u64;
            }
        }
        v
    }

    // --- paged: single-shard pushes down to SQL; multi merges + slices ---

    pub fn process_instances_page(
        &self,
        limit: i64,
        offset: i64,
        filter: &InstanceFilter,
    ) -> Vec<ProcessInstanceRow> {
        if self.shards.len() == 1 {
            return self.shards[0].process_instances_page(limit, offset, filter);
        }
        let mut all: Vec<ProcessInstanceRow> = self
            .process_instances()
            .into_iter()
            .filter(|row| filter.matches(row))
            .collect();
        // Newest-first by key (keys are monotonic per partition), matching the
        // single-store `ORDER BY key DESC`.
        all.sort_by_key(|b| std::cmp::Reverse(b.key));
        let start = offset.max(0) as usize;
        let take = limit.max(0) as usize;
        all.into_iter().skip(start).take(take).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> Arc<ReadStore> {
        Arc::new(ReadStore::open(None).expect("open in-memory read store"))
    }

    #[test]
    fn agent_read_conveniences_never_hide_a_broken_shard() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use std::sync::atomic::{AtomicU64, Ordering};

        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "nano-agent-read-errors-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("read.sqlite");
        let broken = Arc::new(ReadStore::open(Some(&path)).unwrap());
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch("DROP TABLE agent_instances; DROP TABLE agent_history;")
            .unwrap();
        let model = ReadModel::from_shards(vec![(0, shard()), (1, broken)]);
        let filter = AgentHistoryFilter::default();
        let fallible = [
            model.try_agent_instances().is_err(),
            model.try_agent_instance(1).is_err(),
            model.try_agent_history(&filter).is_err(),
        ];
        let fail_loudly = [
            catch_unwind(AssertUnwindSafe(|| model.agent_instances())).is_err(),
            catch_unwind(AssertUnwindSafe(|| model.agent_instance(1))).is_err(),
            catch_unwind(AssertUnwindSafe(|| model.agent_history(&filter))).is_err(),
        ];
        drop(model);
        drop(connection);
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(fallible, [true; 3]);
        assert_eq!(
            fail_loudly, [true; 3],
            "query failures must not resemble absent data"
        );
    }

    #[test]
    #[should_panic(expected = "duplicate partition id")]
    fn from_shards_rejects_duplicate_partition_ids() {
        // A duplicate partition id would overwrite the slot mapping while still
        // pushing both shard handles, leaving one shard unreachable/misrouted.
        // The constructor must fail fast instead of silently misrouting.
        ReadModel::from_shards(vec![(1, shard()), (1, shard())]);
    }

    #[test]
    fn from_shards_accepts_distinct_partition_ids() {
        let model = ReadModel::from_shards(vec![(0, shard()), (2, shard()), (5, shard())]);
        let mut pids: Vec<u64> = model.shards().into_iter().map(|(pid, _)| pid).collect();
        pids.sort_unstable();
        assert_eq!(pids, vec![0, 2, 5]);
    }

    fn created(instance_key: Key) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        }
    }

    /// A sharded read model must filter and count identically to a single-shard
    /// model holding the same data: the multi-partition merge path applies the
    /// same [`InstanceFilter`] predicate SQLite does, so a filtered page + total
    /// are byte-for-byte equal across the single- and multi-shard paths.
    #[test]
    fn multi_shard_filtered_page_and_count_match_single_shard() {
        use nanobpmn_engine_core::{IncidentKind, ProcessInstanceState, compose_key};

        // Instances spread across partitions 0, 1, 2. Local counters chosen so
        // the raw keys interleave when sorted descending.
        let p0a = compose_key(0, 10);
        let p0b = compose_key(0, 40);
        let p1a = compose_key(1, 20);
        let p1b = compose_key(1, 50);
        let p2a = compose_key(2, 30);

        // The event stream, applied identically to every store.
        let raise = |instance_key: Key, incident_key: u64| Event::IncidentRaised {
            incident_key,
            instance_key,
            element_instance_key: instance_key + 1,
            element_id: "t".to_string(),
            kind: IncidentKind::JobNoRetries,
            reason: "boom".to_string(),
            job_key: Some(42),
            created_at: 5,
            redrive: None,
        };

        let per_partition: Vec<Vec<Event>> = vec![
            // partition 0
            vec![
                created(p0a),
                created(p0b),
                Event::ProcessInstanceCompleted { instance_key: p0b },
                raise(p0a, 900),
            ],
            // partition 1
            vec![
                created(p1a),
                created(p1b),
                Event::ProcessInstanceTerminated { instance_key: p1a },
            ],
            // partition 2
            vec![created(p2a), raise(p2a, 901)],
        ];

        // Single-shard reference: everything in one store.
        let single = shard();
        for events in &per_partition {
            let refs: Vec<&Event> = events.iter().collect();
            single.export(&refs).unwrap();
        }
        let single_model = ReadModel::from_shards(vec![(0, single)]);

        // Multi-shard: one store per partition, each seeing only its events.
        let shards: Vec<(u64, Arc<ReadStore>)> = per_partition
            .iter()
            .enumerate()
            .map(|(pid, events)| {
                let s = shard();
                let refs: Vec<&Event> = events.iter().collect();
                s.export(&refs).unwrap();
                (pid as u64, s)
            })
            .collect();
        let multi_model = ReadModel::from_shards(shards);
        assert!(multi_model.shards().len() > 1, "must exercise merge path");

        let filters = [
            InstanceFilter::default(),
            InstanceFilter {
                state: Some(ProcessInstanceState::Active),
                has_incident: None,
            },
            InstanceFilter {
                state: Some(ProcessInstanceState::Completed),
                has_incident: None,
            },
            InstanceFilter {
                state: Some(ProcessInstanceState::Terminated),
                has_incident: None,
            },
            InstanceFilter {
                state: None,
                has_incident: Some(true),
            },
            InstanceFilter {
                state: Some(ProcessInstanceState::Active),
                has_incident: Some(true),
            },
        ];

        for filter in &filters {
            assert_eq!(
                single_model.process_instance_count(filter),
                multi_model.process_instance_count(filter),
                "count parity for {filter:?}"
            );
            for &(limit, offset) in &[(100, 0), (2, 0), (2, 2), (1, 1)] {
                let single_keys: Vec<Key> = single_model
                    .process_instances_page(limit, offset, filter)
                    .into_iter()
                    .map(|r| r.key)
                    .collect();
                let multi_keys: Vec<Key> = multi_model
                    .process_instances_page(limit, offset, filter)
                    .into_iter()
                    .map(|r| r.key)
                    .collect();
                assert_eq!(
                    single_keys, multi_keys,
                    "page parity for {filter:?} limit={limit} offset={offset}"
                );
            }
        }
    }

    /// A `ProcessInstanceCreated` carrying a parent linkage, so a call-activity
    /// hierarchy can be seeded event-first.
    fn created_with_parent(instance_key: Key, parent_pi: Key, parent_ei: Key) -> Event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id: "p".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: Some(parent_pi),
            parent_element_instance_key: Some(parent_ei),
        }
    }

    /// `root_process_instance_key` walks the `parentProcessInstanceKey` chain to
    /// the top-level ancestor (C8 parity, issue #977): a top-level instance roots
    /// to its own key, and a call-activity child — however deeply nested — roots
    /// to the top-level instance. The walk crosses partitions, so seed parent,
    /// child and grandchild onto three different shards and route point lookups
    /// through the merged model.
    #[test]
    fn root_process_instance_key_walks_the_parent_chain_across_partitions() {
        use nanobpmn_engine_core::compose_key;

        let root = compose_key(0, 10); // top-level, no parent
        let child = compose_key(1, 20); // parent = root, via call-activity EI 111
        let grandchild = compose_key(2, 30); // parent = child, via call-activity EI 222

        let s0 = shard();
        let s1 = shard();
        let s2 = shard();
        s0.export(&[&created(root)]).unwrap();
        s1.export(&[&created_with_parent(child, root, 111)])
            .unwrap();
        s2.export(&[&created_with_parent(grandchild, child, 222)])
            .unwrap();
        let model = ReadModel::from_shards(vec![(0, s0), (1, s1), (2, s2)]);

        // Top-level self-roots; every descendant roots to the top-level ancestor.
        assert_eq!(model.root_process_instance_key(root), root);
        assert_eq!(model.root_process_instance_key(child), root);
        assert_eq!(model.root_process_instance_key(grandchild), root);
    }

    /// Best-effort boundaries: an unknown starting key self-roots (no row to
    /// walk), and if an ancestor row is missing the walk stops at the furthest
    /// ancestor key it could observe rather than fabricating a different root.
    #[test]
    fn root_process_instance_key_is_best_effort_at_missing_boundaries() {
        let s0 = shard();
        // `child` names a parent (7777) whose row was never projected/pruned.
        let child = 2000u64;
        s0.export(&[&created_with_parent(child, 7777, 111)])
            .unwrap();
        let model = ReadModel::from_shards(vec![(0, s0)]);

        // Unknown key: nothing to walk, roots to itself.
        assert_eq!(model.root_process_instance_key(9999), 9999);
        // Known child with an absent parent: the furthest known ancestor is the
        // parent key itself, so that is the reported root (not the child).
        assert_eq!(model.root_process_instance_key(child), 7777);
    }
}
