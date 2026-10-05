//! `impl Engine` methods: memory concern (extracted from the monolithic engine module).

use super::*;

/// The variable upserts produced by [`Engine::drain_dirty_vars`]: each dirty,
/// resident (non-spilled) instance's key paired with a cheap `Arc` clone of its
/// current variable map, for the host to serialize into the durable var store.
pub type VarUpserts = Vec<(Key, Arc<HashMap<String, Value>>)>;

impl Engine {
    /// Captures a complete, compact snapshot of this engine: the materialized
    /// [`State`] (already pruned of terminal instances) plus the scalar
    /// generator/clock metadata needed to resume minting keys and placing
    /// subscriptions without collision. Unlike an event log, its size tracks the
    /// *live* working set rather than growing with every command ever applied —
    /// the basis for bounded Raft state-machine snapshots.
    #[cfg(feature = "serde")]
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            state: self.state.clone(),
            partition_id: self.partition_id,
            next_local: self.next_local,
            num_partitions: self.num_partitions,
            now: self.now,
            start_dispatch_rr: self.start_dispatch_rr,
        }
    }

    /// Rebuilds an engine from an [`Engine::snapshot`] — the state-based
    /// counterpart to [`Engine::replay_partition`], restoring the exact
    /// materialized state and generator position in one step (no replay).
    #[cfg(feature = "serde")]
    pub fn from_snapshot(mut snapshot: EngineSnapshot) -> Self {
        assert!(
            snapshot.partition_id <= state::MAX_PARTITION_ID,
            "partition id {} exceeds MAX_PARTITION_ID {}",
            snapshot.partition_id,
            state::MAX_PARTITION_ID
        );
        for deployed in snapshot
            .state
            .processes
            .values_mut()
            .chain(snapshot.state.process_versions.values_mut())
        {
            deployed.definition.normalize_legacy_agent_tasks();
        }
        Self {
            state: snapshot.state,
            partition_id: snapshot.partition_id,
            next_local: snapshot.next_local,
            num_partitions: snapshot.num_partitions.max(1),
            now: snapshot.now,
            start_dispatch_rr: snapshot.start_dispatch_rr,
            lenient_completion: false,
            track_dirty_vars: false,
            dirty_vars: std::collections::HashSet::new(),
            forgotten_vars: std::collections::HashSet::new(),
            retired_tombstones: std::collections::HashSet::new(),
            torn_down_scopes: std::collections::HashSet::new(),
            cluster_variables: crate::cluster_vars::ClusterVariables::default(),
        }
    }

    // --- Lean (control-only) snapshot support: authoritative durable var store ---
    //
    // When lean-snapshot mode is enabled the periodic snapshot carries only
    // control state (no variable payloads); variables live in a host-owned
    // authoritative durable store, written incrementally at each checkpoint from
    // the dirty set below. See `server/src/varstore.rs` for the store and the
    // consistency contract with recovery.

    /// Enables/disables dirty-variable tracking (host-side bookkeeping for lean
    /// snapshots). When first enabled on a live engine, call
    /// [`mark_all_dirty`](Engine::mark_all_dirty) so the next checkpoint fully
    /// populates the durable store from a full-variable snapshot deployment.
    pub fn set_track_dirty_vars(&mut self, on: bool) {
        self.track_dirty_vars = on;
    }

    /// Whether dirty-variable tracking is on.
    pub fn tracks_dirty_vars(&self) -> bool {
        self.track_dirty_vars
    }

    /// Marks every live instance's variables dirty, so the next
    /// [`drain_dirty_vars`](Engine::drain_dirty_vars) writes the whole live
    /// working set to the durable store. Used on the first checkpoint after
    /// enabling lean mode on a pre-existing (full-snapshot) deployment, and after
    /// a recovery that rebuilt state from a full snapshot rather than the store.
    pub fn mark_all_dirty(&mut self) {
        self.dirty_vars = self.state.instances.keys().copied().collect();
        self.forgotten_vars.clear();
    }

    /// Drains the accumulated variable delta since the last checkpoint: the
    /// current variable maps of instances whose variables changed (upserts) and
    /// the keys of instances that reached a terminal state (forgets). Clears both
    /// sets. Spilled instances are **skipped** in the upserts — their authoritative
    /// payload is written through to the store at spill time, and their in-state
    /// map is an empty placeholder — so draining must not overwrite the store with
    /// that placeholder. Variable maps are returned as cheap `Arc` clones (a
    /// refcount bump, not a deep copy); the deep read happens off the engine
    /// thread when the host serializes them into the store.
    pub fn drain_dirty_vars(&mut self) -> (VarUpserts, Vec<Key>) {
        let mut upserts = Vec::with_capacity(self.dirty_vars.len());
        for key in self.dirty_vars.drain() {
            if let Some(instance) = self.state.instances.get(&key) {
                if instance.variables_spilled {
                    continue;
                }
                upserts.push((key, Arc::clone(&instance.variables)));
            }
        }
        let forgets: Vec<Key> = self.forgotten_vars.drain().collect();
        (upserts, forgets)
    }

    /// A control-only clone of this engine's state for a lean snapshot: identical
    /// to [`snapshot`](Engine::snapshot) but with every instance's variables
    /// replaced by an empty placeholder, so the serialized snapshot carries no
    /// variable payloads. The omitted variables are restored on recovery from the
    /// authoritative durable store (see [`install_variables`](Engine::install_variables)).
    /// The `Arc::clone` of the variables in the underlying `state.clone()` is a
    /// cheap refcount bump; emptying them in the clone drops that reference so the
    /// serializer never walks the payloads.
    #[cfg(feature = "serde")]
    pub fn snapshot_control_only(&self) -> EngineSnapshot {
        let mut state = self.state.clone();
        for instance in state.instances.values_mut() {
            if !instance.variables.is_empty() {
                instance.variables = Arc::new(HashMap::new());
            }
        }
        EngineSnapshot {
            state,
            partition_id: self.partition_id,
            next_local: self.next_local,
            num_partitions: self.num_partitions,
            now: self.now,
            start_dispatch_rr: self.start_dispatch_rr,
        }
    }

    /// Installs variables restored from the authoritative durable store onto a
    /// recovered instance (control state came from a lean snapshot + journal
    /// tail). Clears the spilled flag: after recovery the payload is resident.
    /// A no-op if the instance is not present (already terminal/evicted).
    pub fn install_variables(&mut self, key: Key, variables: HashMap<String, Value>) {
        if let Some(instance) = self.state.instances.get_mut(&key) {
            instance.variables = Arc::new(variables);
            instance.variables_spilled = false;
        }
    }

    /// one rebuilt via [`Engine::from_snapshot`]). Each event is applied through
    /// the same applier as runtime/replay, and this partition's local key
    /// generator is advanced past every replayed key it owns — identical to the
    /// per-event bookkeeping in [`Engine::replay_partition`]. This is the
    /// snapshot-plus-journal-tail recovery primitive: load a periodic snapshot,
    /// then catch up on only the events the snapshot did not yet cover.
    pub fn apply_replayed_events<I>(&mut self, events: I)
    where
        I: IntoIterator<Item = Event>,
    {
        for event in events {
            let max_key = event.max_key();
            if state::partition_of(max_key) == self.partition_id {
                self.next_local = self.next_local.max(state::local_of(max_key));
            }
            state::apply(&mut self.state, &event);
        }
    }

    /// Evicts a *completed* process instance and every entity it owns (jobs,
    /// timers, message subscriptions, incidents) from hot state, returning
    /// `true` if it was evicted. Process-level message-start subscriptions and
    /// timer-start events, and deployed definitions, are retained (they are not
    /// instance-scoped). No-op for an unknown or still-active instance.
    ///
    /// The engine keeps completed instances by default (so `is_completed`,
    /// `instance`, and the audit trail keep working). A host that has durably
    /// projected the instance's history into a separate read model can call
    /// this to keep hot state bounded to only in-flight work — the engine then
    /// never needs a completed instance again, because no command can target
    /// one (its jobs are settled, its timers fired, its subscriptions closed).
    pub fn evict_instance(&mut self, key: Key) -> bool {
        let terminal = matches!(
            self.state.instances.get(&key).map(|i| i.state),
            Some(ProcessInstanceState::Completed | ProcessInstanceState::Terminated)
        );
        if !terminal {
            return false;
        }
        self.state.instances.remove(&key);
        // Drop the instance's jobs via the reverse index (O(this instance's
        // jobs)), deindexing each from the activatable/activated indices. A
        // terminal instance's jobs are normally already settled and thus not in
        // the activatable index, but `deindex_job` is unconditional and safe.
        if let Some(job_keys) = self.state.jobs_by_instance.remove(&key) {
            for job_key in job_keys {
                if let Some(job) = self.state.jobs.remove(&job_key) {
                    self.state.deindex_job(&job.job_type, job_key, job.priority);
                }
            }
        }
        self.state.timers.retain(|_, t| t.instance_key != key);
        self.state
            .message_subscriptions
            .retain(|_, s| s.instance_key != key);
        self.state.incidents.retain(|_, i| i.instance_key != key);
        true
    }

    /// Evicts a batch of completed instances in a single pass. Each instance's
    /// jobs are dropped via the `jobs_by_instance` reverse index, so the cost is
    /// `O(evicted jobs)` rather than `O(total jobs)` — this is the steady-state
    /// exporter path where completions stream in continuously, and a backlog of
    /// in-flight instances must not make every eviction scan the whole job map.
    /// Timers, subscriptions and incidents are not reverse-indexed (they stay
    /// small or empty for job-only processes), so they are pruned with a `retain`
    /// pass. Non-terminal or unknown keys are ignored. Returns the number of
    /// instances evicted. Does **not** shrink the maps — capacity is reused by
    /// the next instances, which is exactly what is wanted under sustained load.
    pub fn evict_instances(&mut self, keys: &[Key]) -> usize {
        let terminal: HashSet<Key> = keys
            .iter()
            .copied()
            .filter(|k| {
                matches!(
                    self.state.instances.get(k).map(|i| i.state),
                    Some(ProcessInstanceState::Completed | ProcessInstanceState::Terminated)
                )
            })
            .collect();
        self.remove_instance_set(&terminal)
    }

    /// Unconditionally removes a batch of instances from hot state regardless of
    /// their local lifecycle state — the follower-side counterpart to the leader's
    /// exporter-driven [`Engine::evict_instances`]. Under RF>1 with leader-local
    /// completion, a follower replica applies each `CreateInstance` (durably
    /// replicated) but never the retirement (leader-local completion + exporter
    /// eviction never enter the raft log), so completed instances pile up as
    /// `Active` shells that [`Engine::evict_instances`] can never reap (they are
    /// not terminal *here*). The partition leader broadcasts the authoritative set
    /// of retired keys (the retirement digest) and the follower drops them here.
    /// Keys absent locally are ignored (already gone, or the create has not yet
    /// been applied — a benign, self-healing miss for a lagging learner, which is
    /// re-snapshotted from the leader anyway). Returns the number removed.
    pub fn retire_instances(&mut self, keys: &[Key]) -> usize {
        // Cap the tombstone set defensively. It normally holds only keys in the
        // brief window between a retirement digest and the create it races ahead of
        // (drained the moment that create applies), so it stays tiny; the cap only
        // guards a pathological stream of retirements for keys that never arrive.
        const MAX_TOMBSTONES: usize = 1 << 20;
        let mut victims: HashSet<Key> = HashSet::new();
        for &k in keys {
            if self.state.instances.contains_key(&k) {
                victims.insert(k);
            } else if self.retired_tombstones.len() < MAX_TOMBSTONES {
                // The create has not been applied here yet (async learner lag).
                // Remember the retirement so the create is reaped on arrival.
                self.retired_tombstones.insert(k);
            }
        }
        self.remove_instance_set(&victims)
    }

    /// Retires any freshly-created instance whose retirement digest already arrived
    /// (raced ahead of its `CreateInstance` on this follower replica). Called at the
    /// end of [`Engine::apply_command_at`] with the instance keys the command just
    /// materialized; a no-op (and near-free) when no retirement is pending. Returns
    /// the number reaped so the caller can drop them from any spill/cold store too.
    pub(super) fn reap_tombstoned(&mut self, created: &[Key]) -> usize {
        if self.retired_tombstones.is_empty() {
            return 0;
        }
        let hit: HashSet<Key> = created
            .iter()
            .copied()
            .filter(|k| self.retired_tombstones.remove(k))
            .collect();
        self.remove_instance_set(&hit)
    }

    /// The retirement low-water mark this partition's **owner** broadcasts to its
    /// follower replicas (consumed by [`Engine::retire_below`]): the smallest
    /// still-`Active` instance key here. Every key below it has terminated — it is
    /// either already evicted, or a resident `Completed`/`Terminated` shell (a
    /// reclaimed owner can retain such shells until a later sweep) — so a follower
    /// may safely reap any resident instance below it. When no instance is active,
    /// it is the next key this partition would mint, so a quiescent owner tells its
    /// followers to drop their entire replica backlog. Filtering to `Active`
    /// (rather than the min resident key) is what lets the mark keep advancing even
    /// while the owner still holds terminal shells, so it never pins a follower's
    /// backlog. Cheap: an owner keeps only its in-flight working set active.
    pub fn retirement_low_water(&self) -> Key {
        self.state
            .instances
            .values()
            .filter(|i| matches!(i.state, ProcessInstanceState::Active))
            .map(|i| i.key)
            .min()
            .unwrap_or_else(|| {
                state::compose_key(self.partition_id, self.next_local.saturating_add(1))
            })
    }

    /// Loss-tolerant reconciliation counterpart to the per-key retirement digest:
    /// on a **follower replica**, drops every resident instance whose key is
    /// strictly below `low_water` (the owner's [`Engine::retirement_low_water`]).
    /// Every such instance has terminated on the owner, so reaping its replica
    /// shell here is safe. Bounded by `max_remove` per call so a large accumulated
    /// backlog is drained across ticks without stalling the actor. Returns the
    /// reaped keys (so the host can forget any spilled rows). Idempotent: once the
    /// backlog below the mark is gone, re-running with the same or a higher mark is
    /// a cheap no-op. Because the owner re-broadcasts the mark every tick, this
    /// converges even when best-effort per-key digest frames are dropped under load.
    pub fn retire_below(&mut self, low_water: Key, max_remove: usize) -> Vec<Key> {
        if self.state.instances.is_empty() || max_remove == 0 {
            return Vec::new();
        }
        let victims: Vec<Key> = self
            .state
            .instances
            .keys()
            .copied()
            .filter(|&k| k < low_water)
            .take(max_remove)
            .collect();
        if victims.is_empty() {
            return victims;
        }
        // Any tombstone below the mark is now moot: a create for it, if it ever
        // arrives, is reaped wholesale by this same sweep, so drop them to keep the
        // tombstone set bounded to the still-active window.
        if !self.retired_tombstones.is_empty() {
            self.retired_tombstones.retain(|&k| k >= low_water);
        }
        let set: HashSet<Key> = victims.iter().copied().collect();
        self.remove_instance_set(&set);
        victims
    }

    /// Shared removal pass for [`Engine::evict_instances`] and
    /// [`Engine::retire_instances`]: drops every instance in `victims` from hot
    /// state along with its jobs (via the `jobs_by_instance` reverse index, so the
    /// cost is `O(evicted jobs)` not `O(total jobs)`), timers, subscriptions and
    /// incidents. Does **not** shrink the maps — capacity is reused by the next
    /// instances, which is exactly what is wanted under sustained load. Returns the
    /// number of instances removed.
    fn remove_instance_set(&mut self, victims: &HashSet<Key>) -> usize {
        if victims.is_empty() {
            return 0;
        }
        if self.track_dirty_vars {
            for key in victims {
                self.dirty_vars.remove(key);
                self.forgotten_vars.insert(*key);
            }
        }
        for key in victims {
            self.state.instances.remove(key);
            // Drop this instance's jobs via the reverse index (O(its jobs)),
            // deindexing each from the activatable/activated indices.
            if let Some(job_keys) = self.state.jobs_by_instance.remove(key) {
                for job_key in job_keys {
                    if let Some(job) = self.state.jobs.remove(&job_key) {
                        self.state.deindex_job(&job.job_type, job_key, job.priority);
                    }
                }
            }
        }
        self.state
            .timers
            .retain(|_, t| !victims.contains(&t.instance_key));
        self.state
            .message_subscriptions
            .retain(|_, s| !victims.contains(&s.instance_key));
        self.state
            .incidents
            .retain(|_, i| !victims.contains(&i.instance_key));
        victims.len()
    }

    /// Evicts every completed instance (see [`Engine::evict_instance`]) and
    /// shrinks the backing maps so freed capacity is returned. Returns the
    /// number of instances evicted. Intended to run once after a boot replay,
    /// when the read model is already caught up, so recovered hot state holds
    /// only in-flight instances rather than the whole history.
    pub fn evict_completed(&mut self) -> usize {
        let done: Vec<Key> = self
            .state
            .instances
            .iter()
            .filter(|(_, i)| {
                matches!(
                    i.state,
                    ProcessInstanceState::Completed | ProcessInstanceState::Terminated
                )
            })
            .map(|(k, _)| *k)
            .collect();
        for key in &done {
            self.evict_instance(*key);
        }
        if !done.is_empty() {
            self.shrink();
        }
        done.len()
    }

    /// Shrinks the capacity of the hot-state maps to fit their live contents,
    /// returning memory freed by eviction back to the allocator. (Rust maps
    /// never shrink on their own, so removal alone does not lower the resident
    /// footprint until this is called.)
    pub fn shrink(&mut self) {
        self.state.instances.shrink_to_fit();
        self.state.jobs.shrink_to_fit();
        self.state.timers.shrink_to_fit();
        self.state.message_subscriptions.shrink_to_fit();
        self.state.signal_subscriptions.shrink_to_fit();
        self.state.conditional_subscriptions.shrink_to_fit();
        self.state.incidents.shrink_to_fit();
        self.state.activatable_jobs.shrink_to_fit();
        self.state.activated_jobs.shrink_to_fit();
        self.state.jobs_by_instance.shrink_to_fit();
    }

    // --- Variable spill: host-managed hot-state memory reclamation. ---
    //
    // The 50 KB-class `variables` payload dominates the per-instance footprint of
    // a large *active* backlog (instances created and parked on a job, waiting for
    // a worker). Such instances are quiescent: the only way their token resumes is
    // job activation followed by completion. So the host can move their variables
    // to a disk-backed store and rehydrate them at activation time, bounding hot
    // RAM the way Zeebe's RocksDB-backed state does — but keeping the in-memory
    // speed for the working set. These methods are pure (no I/O): the host owns the
    // store and decides the policy; the engine only swaps the `Arc` in and out.

    /// Returns `true` when `key`'s variables have been spilled and not yet
    /// rehydrated (so `instance(key).variables` is an empty placeholder).
    pub fn is_variables_spilled(&self, key: Key) -> bool {
        self.state
            .instances
            .get(&key)
            .is_some_and(|i| i.variables_spilled)
    }

    /// Spills `key`'s variables out of hot state: replaces them with an empty
    /// placeholder, marks the instance spilled, and returns the payload for the
    /// host to persist. Returns `None` (a no-op) if the instance does not exist,
    /// is already spilled, or carries no variables — nothing worth spilling.
    ///
    /// Pure: it only moves an `Arc` out of the map. The host must persist the
    /// returned payload and rehydrate it (via [`Engine::rehydrate_variables`])
    /// before any command that reads this instance's variables.
    ///
    /// Only the **root** variable payload is shed — the dominant per-instance
    /// cost of a job-parked backlog. Any non-root scope-local maps
    /// (`scope_variables`: sub-process / multi-instance locals) stay resident;
    /// they ride with the control snapshot and are typically small, and keeping
    /// them in place means a rehydrate need only restore the root before the host
    /// recomputes the merged view via [`Engine::element_variables`].
    pub fn spill_variables(&mut self, key: Key) -> Option<Arc<HashMap<String, Value>>> {
        let instance = self.state.instances.get_mut(&key)?;
        if instance.variables_spilled || instance.variables.is_empty() {
            return None;
        }
        instance.variables_spilled = true;
        Some(std::mem::take(&mut instance.variables))
    }

    /// Restores previously [spilled](Engine::spill_variables) variables into hot
    /// state. A no-op if the instance is gone. Idempotent with respect to the
    /// spilled flag (clears it regardless).
    pub fn rehydrate_variables(&mut self, key: Key, variables: Arc<HashMap<String, Value>>) {
        if let Some(instance) = self.state.instances.get_mut(&key) {
            instance.variables = variables;
            instance.variables_spilled = false;
        }
    }

    /// The merged, scope-resolved variable view an activated job on
    /// `element_instance_key` should carry: the element's own scope plus every
    /// ancestor scope up to the (now-resident) root, nearer scopes shadowing
    /// farther ones. The host recomputes this **after** rehydrating a spilled
    /// instance's root variables, so a job on a nested scope (sub-process /
    /// multi-instance body or child) regains its full view — the scope-local
    /// bindings stay resident through a spill (only the root payload is shed),
    /// but the merged snapshot handed to a worker must fold the freshly restored
    /// root back in. For a flat (root-only) instance this returns the shared root
    /// `Arc` unchanged, so the flat activation path is byte-identical.
    pub fn element_variables(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Arc<HashMap<String, Value>> {
        self.variables_for_element(instance_key, element_instance_key)
    }

    /// Picks up to `limit` *resident* spill candidates: `Active` instances that
    /// are parked solely on a job (hold at least one job and have **no** armed
    /// timer or open message subscription), still carry their variables, and are
    /// not already spilled. Returned oldest-key first (keys are monotonic, so the
    /// oldest backlog — least likely to be activated next — is shed first). Used
    /// by the host to choose which instances to spill when hot RAM crosses its
    /// budget.
    ///
    /// Excluding instances with an armed timer or open subscription is a
    /// correctness guard: those tokens can resume the flow **without** going
    /// through job activation (a boundary timer firing, a message correlating),
    /// and the host only rehydrates on activation. Spilling such an instance
    /// would let an async resumption read the empty placeholder variables, so
    /// they are kept resident.
    pub fn spillable_instances(&self, limit: usize) -> Vec<Key> {
        if limit == 0 {
            return Vec::new();
        }
        let guarded = self.instances_with_async_token();
        let mut candidates: Vec<Key> = self
            .state
            .instances
            .iter()
            .filter(|(key, i)| self.is_spillable(key, i, &guarded))
            .map(|(key, _)| *key)
            .collect();
        candidates.sort_unstable();
        candidates.truncate(limit);
        candidates
    }

    /// How many resident instances are spill candidates right now (see
    /// [`Engine::spillable_instances`]). Lets the host size its spill budget
    /// without materialising the key list.
    pub fn resident_spillable_count(&self) -> usize {
        let guarded = self.instances_with_async_token();
        self.state
            .instances
            .iter()
            .filter(|(key, i)| self.is_spillable(key, i, &guarded))
            .count()
    }

    /// Total resident instances, regardless of spillability. O(1). Spill
    /// candidates are a strict subset of this set, so a host can use it as a
    /// cheap upper bound: when the whole resident set already fits the spill
    /// budget, [`resident_spillable_count`](Engine::resident_spillable_count) is
    /// guaranteed to as well, letting the host skip that O(N) scan on the hot
    /// command path.
    pub fn resident_instance_count(&self) -> usize {
        self.state.instances.len()
    }

    /// Per-process-definition in-flight instance backlog for this partition:
    /// `(process_id, in_flight_L_P, cumulative_created)`. O(distinct definitions
    /// with live instances) — a handful of small map clones, cheap for the ~1 Hz
    /// monitor tick that aggregates these across the node's partitions to drive
    /// the ADR-0020 Tier-2 per-definition admission compressors. `created` is
    /// monotonic (survives an instance going terminal), so the monitor can
    /// difference it into the create rate `λ_P` even for a definition whose
    /// in-flight count has since returned to zero (and dropped out of `L_P`).
    pub fn backlog_by_process(&self) -> Vec<(String, u64, u64)> {
        self.state
            .created_by_process
            .iter()
            .map(|(pid, created)| {
                let inflight = self
                    .state
                    .inflight_by_process
                    .get(pid)
                    .copied()
                    .unwrap_or(0);
                (pid.clone(), inflight, *created)
            })
            .collect()
    }

    /// Total approximate heap bytes of variables held resident by instances in
    /// this partition (spilled instances hold an empty map, so they contribute
    /// ~0). O(N) over the resident set — call it off the hot path (the 250ms
    /// mem-pressure sampler). This is the decisive attribution gauge for the
    /// burst balloon: if the cluster-wide sum tracks the jemalloc live-heap
    /// peak, the resident instance variables ARE the balloon; if it stays far
    /// below, the balloon is in-flight pipeline copies, not resident variables.
    pub fn resident_variable_bytes(&self) -> u64 {
        self.state
            .instances
            .values()
            .map(|i| {
                let root: u64 = i
                    .variables
                    .values()
                    .map(crate::model::Value::approx_bytes)
                    .sum();
                // Non-root scope-local maps (sub-process / MI-child locals) stay
                // resident through a variable spill, so they count toward the
                // resident footprint too — for a flat (root-only) instance this
                // inner sum is zero and the flat gauge is unchanged.
                let scoped: u64 = i
                    .scope_variables
                    .values()
                    .flat_map(|m| m.values())
                    .map(crate::model::Value::approx_bytes)
                    .sum();
                root + scoped
            })
            .sum()
    }

    /// Whether `instance` (keyed `key`) may have its variables spilled, given the
    /// set of instances that hold an async-resumable token (`guarded`). Shared by
    /// [`spillable_instances`](Engine::spillable_instances) and
    /// [`resident_spillable_count`](Engine::resident_spillable_count) so the
    /// budget accounting and the spill selection never diverge.
    pub(crate) fn is_spillable(
        &self,
        key: &Key,
        instance: &state::ProcessInstance,
        guarded: &std::collections::HashSet<Key>,
    ) -> bool {
        instance.state == ProcessInstanceState::Active
            && !instance.variables_spilled
            && !instance.variables.is_empty()
            && !guarded.contains(key)
            && self
                .state
                .jobs_by_instance
                .get(key)
                .is_some_and(|jobs| !jobs.is_empty())
    }

    /// The set of instance keys holding a token that can resume the flow without
    /// job activation: an armed (`Created`) timer or an open message
    /// subscription. Empty (and allocation-free on the fast path) when no timers
    /// or subscriptions exist — the common case under a create-heavy backlog.
    pub(crate) fn instances_with_async_token(&self) -> std::collections::HashSet<Key> {
        let mut guarded = std::collections::HashSet::new();
        if self.state.timers.is_empty()
            && self.state.message_subscriptions.is_empty()
            && self.state.conditional_subscriptions.is_empty()
        {
            return guarded;
        }
        for timer in self.state.timers.values() {
            if timer.state == state::TimerState::Created {
                guarded.insert(timer.instance_key);
            }
        }
        for sub in self.state.message_subscriptions.values() {
            if matches!(
                sub.state,
                state::MessageSubscriptionState::Open | state::MessageSubscriptionState::Opening
            ) {
                guarded.insert(sub.instance_key);
            }
        }
        // A conditional catch/boundary can resume without job activation (a later
        // variable update flips its condition), so keep those instances resident.
        for sub in self.state.conditional_subscriptions.values() {
            if sub.state == state::MessageSubscriptionState::Open {
                guarded.insert(sub.instance_key);
            }
        }
        guarded
    }

    // --- Cold spill: host-managed eviction of whole idle instances. ---
    //
    // Variable spill (above) sheds only the `variables` of a job-parked instance,
    // rehydrated at activation. It deliberately keeps instances parked on a timer
    // or message resident, because those resume *without* job activation. But the
    // long-lived, low-throughput workload — tens of thousands of instances each
    // waiting hours or days on a timer or an incoming message — is exactly those
    // instances, and their resident control state (the `active`/`scopes`/`join_*`
    // maps and the owned job/timer/subscription records) is what grows hot RAM
    // with the parked backlog even while nothing runs.
    //
    // Cold spill moves such an instance out of hot state *in full*: the host takes
    // a [`state::InstanceSnapshot`], persists it, and keeps only a slim routing
    // index (the instance's job keys, message name/correlation keys and timer
    // due-times) resident, so an event that targets the instance can rehydrate it
    // on demand. As with variable spill these methods are pure — no I/O: the host
    // owns the store, the index and the policy; the engine only lifts the instance
    // in and out of its maps and keeps its derived indices consistent.

    /// Picks up to `limit` *idle* cold-spill candidates: `Active` instances that
    /// hold at least one parked token and have **no activated (locked) job** — so
    /// no worker is mid-task on them and they are genuinely dormant. Returned
    /// oldest-key first (keys are monotonic, so the coldest backlog is shed
    /// first), the LRU order the host evicts in under memory pressure.
    ///
    /// Unlike [`spillable_instances`](Engine::spillable_instances) (variable
    /// spill) this **includes** instances parked on a timer or message — the
    /// whole point, since those are the long-lived waits — because the host
    /// rehydrates a cold instance on *any* targeting event (timer fire, message
    /// correlation, job activation or a direct command), not only activation.
    pub fn cold_spillable_instances(&self, limit: usize) -> Vec<Key> {
        if limit == 0 {
            return Vec::new();
        }
        let mut candidates: Vec<Key> = self
            .state
            .instances
            .iter()
            .filter(|(key, i)| self.is_cold_spillable(key, i))
            .map(|(key, _)| *key)
            .collect();
        candidates.sort_unstable();
        candidates.truncate(limit);
        candidates
    }

    /// How many resident instances are cold-spill candidates right now (see
    /// [`Engine::cold_spillable_instances`]).
    pub fn cold_spillable_count(&self) -> usize {
        self.state
            .instances
            .iter()
            .filter(|(key, i)| self.is_cold_spillable(key, i))
            .count()
    }

    /// Whether `instance` (keyed `key`) is an idle cold-spill candidate: `Active`,
    /// holding at least one parked token, and owning no job that currently holds
    /// an activation lock (a locked job means a worker is mid-task — keep it hot).
    pub(crate) fn is_cold_spillable(&self, key: &Key, instance: &state::ProcessInstance) -> bool {
        if instance.state != ProcessInstanceState::Active
            || instance.active.is_empty()
            || instance.variables_spilled
        {
            return false;
        }
        match self.state.jobs_by_instance.get(key) {
            Some(jobs) => !jobs.iter().any(|j| self.state.activated_jobs.contains(j)),
            None => true,
        }
    }

    /// Lifts an idle instance and every entity it owns (jobs, timers, message
    /// subscriptions, user tasks, incidents) out of hot state, returning a
    /// self-contained [`state::InstanceSnapshot`] for the host to persist. The
    /// engine's derived job indices are kept consistent (each job is deindexed as
    /// it leaves). Returns `None` — a no-op — for an unknown or terminal instance.
    ///
    /// If the instance's variables were previously variable-spilled the snapshot
    /// would capture an empty placeholder, so this refuses (returns `None`) while
    /// `variables_spilled` is set: the host must rehydrate the variables first so
    /// the cold snapshot is authoritative.
    pub fn snapshot_instance(&mut self, key: Key) -> Option<state::InstanceSnapshot> {
        let instance = self.state.instances.get(&key)?;
        if instance.state != ProcessInstanceState::Active || instance.variables_spilled {
            return None;
        }

        let mut jobs = Vec::new();
        if let Some(job_keys) = self.state.jobs_by_instance.remove(&key) {
            for job_key in job_keys {
                if let Some(job) = self.state.jobs.remove(&job_key) {
                    self.state.deindex_job(&job.job_type, job_key, job.priority);
                    jobs.push(job);
                }
            }
        }

        let timers = drain_owned(&mut self.state.timers, key);
        let message_subscriptions = drain_owned(&mut self.state.message_subscriptions, key);
        let signal_subscriptions = drain_owned(&mut self.state.signal_subscriptions, key);
        let conditional_subscriptions = drain_owned(&mut self.state.conditional_subscriptions, key);
        let user_tasks = drain_owned(&mut self.state.user_tasks, key);
        let incidents = drain_owned(&mut self.state.incidents, key);

        let instance = self.state.instances.remove(&key)?;
        Some(state::InstanceSnapshot {
            instance,
            jobs,
            timers,
            message_subscriptions,
            signal_subscriptions,
            conditional_subscriptions,
            user_tasks,
            incidents,
        })
    }

    /// Restores a previously [snapshotted](Engine::snapshot_instance) instance
    /// into hot state, re-inserting every owned entity and rebuilding the derived
    /// job indices, so command processing sees exactly the state that was lifted
    /// out. Idempotent-ish: re-inserting keys that already exist overwrites them.
    pub fn rehydrate_instance(&mut self, snapshot: state::InstanceSnapshot) {
        state::restore_instance_snapshot(&mut self.state, snapshot);
    }
}
