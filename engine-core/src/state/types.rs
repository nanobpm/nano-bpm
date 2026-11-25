//! State record types and key machinery.
//!
//! [`State`] is the engine's working memory together with every record type an
//! [`crate::Event`] payload embeds. This is the data-model half of the `state`
//! module: it **never references [`crate::event`]**, so both `event` and the
//! applier ([`super::apply`]) can depend on it without forming a cycle.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::model::{ElementId, IncomingFlow, ProcessDefinition, Value};

/// A globally-unique identifier for instances, element instances and jobs.
///
/// Following Zeebe, the high [`PARTITION_BITS`] bits encode the id of the
/// partition that minted the key and the low [`LOCAL_BITS`] bits are a
/// per-partition monotonic counter. This makes every key globally unique across
/// partitions *and* self-routing: the owning partition is recoverable with
/// [`partition_of`]. A single-partition engine uses partition id `0`, so its
/// keys are just `1, 2, 3, …` (identical to the pre-partitioning scheme).
pub type Key = u64;

/// Number of high bits in a [`Key`] reserved for the partition id (Zeebe uses
/// the same split). `PARTITION_BITS + LOCAL_BITS == 64`.
pub const PARTITION_BITS: u32 = 13;
/// Number of low bits in a [`Key`] holding the per-partition local counter.
pub const LOCAL_BITS: u32 = 64 - PARTITION_BITS;
/// Mask selecting the local-counter portion of a [`Key`].
pub const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;
/// Largest partition id representable in a [`Key`].
pub const MAX_PARTITION_ID: u64 = (1u64 << PARTITION_BITS) - 1;

/// Extracts the id of the partition that minted `key` (its high bits).
#[inline]
pub const fn partition_of(key: Key) -> u64 {
    key >> LOCAL_BITS
}

/// Extracts the per-partition local counter portion of `key` (its low bits).
#[inline]
pub const fn local_of(key: Key) -> u64 {
    key & LOCAL_MASK
}

/// Composes a [`Key`] from a partition id and a local counter value.
#[inline]
pub const fn compose_key(partition_id: u64, local: u64) -> Key {
    (partition_id << LOCAL_BITS) | (local & LOCAL_MASK)
}

/// Stable 64-bit FNV-1a hash of `bytes`. Deterministic across processes,
/// architectures and restarts (unlike the standard-library `DefaultHasher`,
/// which is randomly seeded), so every node in a cluster derives the same
/// placement for the same input.
#[inline]
pub const fn stable_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    hash
}

/// Partition that owns the message subscription for `correlation_key` in a
/// `num_partitions`-wide cluster (Zeebe-style placement: a subscription and a
/// published message land on the same partition iff they share a correlation
/// key). Deterministic via [`stable_hash`]. With `num_partitions == 1` this is
/// always partition `0`, so a single-partition host behaves exactly as before.
#[inline]
pub fn subscription_partition(correlation_key: &str, num_partitions: u64) -> u64 {
    if num_partitions <= 1 {
        return 0;
    }
    stable_hash(correlation_key.as_bytes()) % num_partitions
}

/// Lifecycle state of a process instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ProcessInstanceState {
    Active,
    Completed,
    /// Cancelled by an operator before completing: every token was discarded.
    /// Terminal, like `Completed`, but reached via [`crate::Command::CancelInstance`].
    Terminated,
    /// Cancellation is in progress but one or more user tasks are running their
    /// `canceling` task listeners (ADR 0037 §6). The instance's other tokens are
    /// already discarded; it transitions to `Terminated` once the last canceling
    /// chain drains. Not a resting state a listener-free instance ever reaches,
    /// so the ordinary synchronous cancel path is unchanged.
    Terminating,
    /// Suspended by an operator ([`crate::Command::SuspendInstance`]). A
    /// suspended instance makes no progress: its jobs are not activatable and
    /// its timers/other triggers do not fire, but it retains all its runtime
    /// state. Resuming ([`crate::Command::ResumeInstance`]) returns it to
    /// `Active` with its exact prior running state. Only reachable from `Active`
    /// (the sole valid live-transition source); terminal states reject
    /// suspension. Tracked alongside [`ProcessInstance::suspended_at`], which
    /// records the most-recent suspension instant.
    Suspended,
}

/// Arrivals at one parallel join, counted per identified incoming flow. The
/// single implementation of Zeebe's taken-sequence-flow bookkeeping (#1233): the
/// state applier and the engine's fire decision both go through it.
///
/// Kept sorted by flow with no zero counts, so its serialized form is
/// deterministic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FlowArrivals(Vec<(IncomingFlow, usize)>);

impl FlowArrivals {
    /// Count one more token over `flow`.
    pub fn record(&mut self, flow: &IncomingFlow) {
        self.add(flow, 1);
    }

    /// Record `tokens` arrivals over `flow` at once.
    pub fn add(&mut self, flow: &IncomingFlow, tokens: usize) {
        if tokens == 0 {
            return;
        }
        match self.0.binary_search_by(|(f, _)| f.cmp(flow)) {
            Ok(i) => self.0[i].1 += tokens,
            Err(i) => self.0.insert(i, (flow.clone(), tokens)),
        }
    }

    /// How many distinct incoming flows have at least one waiting token.
    pub fn distinct_flows(&self) -> usize {
        self.0.len()
    }

    /// Tokens waiting on `flow`.
    pub fn count(&self, flow: &IncomingFlow) -> usize {
        self.0
            .binary_search_by(|(f, _)| f.cmp(flow))
            .map_or(0, |i| self.0[i].1)
    }

    /// The join fired: consume one token per flow and keep the surplus for the
    /// next activation (Zeebe's "Tetris principle").
    pub fn consume_one_each(&mut self) {
        for (_, count) in &mut self.0 {
            *count -= 1;
        }
        self.0.retain(|(_, count)| *count > 0);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every counted flow with its token count, in flow order.
    pub fn iter(&self) -> impl Iterator<Item = (&IncomingFlow, usize)> {
        self.0.iter().map(|(f, c)| (f, *c))
    }

    /// Rename element ids inside the flow identities (process migration),
    /// merging flows that collapse onto the same identity.
    pub fn remap(&mut self, mut remap_id: impl FnMut(&mut ElementId)) {
        let old = std::mem::take(&mut self.0);
        for (mut flow, count) in old {
            remap_id(&mut flow.from);
            self.add(&flow, count);
        }
    }
}

/// The identity of one join's bookkeeping: the join gateway **in one runtime
/// flow-scope instance** (#1580). Zeebe counts a join's taken sequence flows
/// per flow-scope instance, so two live instances of the same scope (two
/// multi-instance children, an embedded sub-process entered by two concurrent
/// tokens, concurrent non-interrupting event sub-process executions) each own
/// an independent join: one execution's arrival can never satisfy another's.
///
/// `scope` is the enclosing flow-scope element instance (`0` = process root).
/// Serialized (within the join maps) as a list of `{scope, element_id, value}`
/// entries; see [`join_map_serde`] for the legacy element-keyed form.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JoinKey {
    pub scope: Key,
    pub element_id: ElementId,
}

impl JoinKey {
    pub fn new(scope: Key, element_id: impl Into<ElementId>) -> Self {
        Self {
            scope,
            element_id: element_id.into(),
        }
    }

    /// `element_id`'s join in the process root scope.
    pub fn root(element_id: impl Into<ElementId>) -> Self {
        Self::new(0, element_id)
    }
}

/// The placeholder scope a legacy (pre-#1580) element-keyed join entry decodes
/// with. [`ProcessInstance::resolve_legacy_join_scopes`] replaces it with the
/// real flow scope on load, and the state applier resolves a legacy event's
/// missing scope ([`ProcessInstance::collapse_legacy_join`]); it never survives
/// into a running engine.
pub const UNRESOLVED_JOIN_SCOPE: Key = Key::MAX;

/// Serde for the `BTreeMap<JoinKey, V>` join maps (#1580). Serializes as a list
/// of `{scope, element_id, value}` entries sorted by key (deterministic bytes).
/// Deserializes either that list or the legacy (pre-#1580) JSON object keyed
/// by element id, whose entries take [`UNRESOLVED_JOIN_SCOPE`] until
/// [`ProcessInstance::resolve_legacy_join_scopes`] runs.
#[cfg(feature = "serde")]
pub mod join_map_serde {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::marker::PhantomData;

    use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
    use serde::ser::{Serialize, SerializeSeq, Serializer};

    use super::{JoinKey, UNRESOLVED_JOIN_SCOPE};
    use crate::model::ElementId;

    #[derive(serde::Serialize)]
    struct EntryRef<'a, V> {
        scope: u64,
        element_id: &'a str,
        value: &'a V,
    }

    #[derive(serde::Deserialize)]
    struct Entry<V> {
        scope: u64,
        element_id: ElementId,
        value: V,
    }

    pub fn serialize<S: Serializer, V: Serialize>(
        map: &BTreeMap<JoinKey, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut entries: Vec<_> = map.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let mut seq = serializer.serialize_seq(Some(entries.len()))?;
        for (key, value) in entries {
            seq.serialize_element(&EntryRef {
                scope: key.scope,
                element_id: &key.element_id,
                value,
            })?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D, V>(deserializer: D) -> Result<BTreeMap<JoinKey, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        struct JoinMapVisitor<V>(PhantomData<V>);
        impl<'de, V: Deserialize<'de>> Visitor<'de> for JoinMapVisitor<V> {
            type Value = BTreeMap<JoinKey, V>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a list of join entries or a legacy element-keyed join map")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut map = BTreeMap::new();
                while let Some(Entry {
                    scope,
                    element_id,
                    value,
                }) = seq.next_element::<Entry<V>>()?
                {
                    map.insert(JoinKey { scope, element_id }, value);
                }
                Ok(map)
            }
            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut map = BTreeMap::new();
                while let Some((element_id, value)) = access.next_entry::<ElementId, V>()? {
                    map.insert(
                        JoinKey {
                            scope: UNRESOLVED_JOIN_SCOPE,
                            element_id,
                        },
                        value,
                    );
                }
                Ok(map)
            }
        }
        deserializer.deserialize_any(JoinMapVisitor(PhantomData))
    }
}

impl ProcessInstance {
    /// How many of `join`'s incoming flows have been taken: identified flows
    /// with a waiting token plus unidentified arrivals (each counted as one
    /// flow).
    pub fn join_flows_taken(&self, join: &JoinKey) -> usize {
        self.join_flow_arrivals
            .get(join)
            .map_or(0, FlowArrivals::distinct_flows)
            + self.join_counts.get(join).copied().unwrap_or(0)
    }

    /// Every join key holding bookkeeping (arrivals, unidentified counts or an
    /// open join instance), sorted and de-duplicated.
    pub fn join_keys(&self) -> Vec<JoinKey> {
        let mut keys: Vec<JoinKey> = self
            .join_counts
            .keys()
            .chain(self.join_flow_arrivals.keys())
            .chain(self.join_instances.keys())
            .cloned()
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    /// Collapse every slot of `element_id` into the single element-wide slot a
    /// **legacy** (pre-#1580) record addresses, and return its key.
    ///
    /// Before #1580 each join element had exactly one slot, so a scope-less
    /// record (or a legacy migration) must see the element's bookkeeping as one
    /// slot — exactly what loading the matching legacy snapshot produces. The
    /// slot keeps the scope of its open join instance (the smallest
    /// element-instance key wins, as legacy migration merging did); with no
    /// open instance its scope is unknown ([`UNRESOLVED_JOIN_SCOPE`]) until the
    /// next `ParallelJoinOpened` or a load resolves it.
    pub fn collapse_legacy_join(&mut self, element_id: &str) -> JoinKey {
        let keys: Vec<JoinKey> = self
            .join_keys()
            .into_iter()
            .filter(|k| k.element_id == element_id)
            .collect();
        let open = keys
            .iter()
            .filter_map(|k| self.join_instances.get(k).map(|eik| (*eik, k.scope)))
            .min();
        let target = JoinKey::new(
            open.map_or(UNRESOLVED_JOIN_SCOPE, |(_, scope)| scope),
            element_id,
        );
        let mut count = 0usize;
        let mut arrivals = FlowArrivals::default();
        for key in &keys {
            count += self.join_counts.remove(key).unwrap_or(0);
            if let Some(a) = self.join_flow_arrivals.remove(key) {
                for (flow, n) in a.iter() {
                    arrivals.add(flow, n);
                }
            }
            self.join_instances.remove(key);
        }
        if count > 0 {
            self.join_counts.insert(target.clone(), count);
        }
        if !arrivals.is_empty() {
            self.join_flow_arrivals.insert(target.clone(), arrivals);
        }
        if let Some((eik, _)) = open {
            self.join_instances.insert(target.clone(), eik);
        }
        target
    }

    /// Move every [`UNRESOLVED_JOIN_SCOPE`] entry of `element_id` onto `scope`
    /// (merging into any entry already there).
    pub fn rekey_unresolved_join(&mut self, element_id: &str, scope: Key) {
        let from = JoinKey::new(UNRESOLVED_JOIN_SCOPE, element_id);
        let to = JoinKey::new(scope, element_id);
        if let Some(n) = self.join_counts.remove(&from) {
            *self.join_counts.entry(to.clone()).or_insert(0) += n;
        }
        if let Some(arrivals) = self.join_flow_arrivals.remove(&from) {
            let slot = self.join_flow_arrivals.entry(to.clone()).or_default();
            for (flow, n) in arrivals.iter() {
                slot.add(flow, n);
            }
        }
        if let Some(eik) = self.join_instances.remove(&from) {
            self.join_instances.insert(to, eik);
        }
    }

    /// Resolve the scope of every legacy (pre-#1580) element-keyed join entry
    /// decoded from an old snapshot: the scope of the open join instance
    /// holding it (`scopes`, root when absent), as each element had exactly one
    /// slot. Run wherever a snapshot instance is loaded; a no-op otherwise.
    pub fn resolve_legacy_join_scopes(&mut self) {
        let unresolved: Vec<ElementId> = self
            .join_keys()
            .into_iter()
            .filter(|k| k.scope == UNRESOLVED_JOIN_SCOPE)
            .map(|k| k.element_id)
            .collect();
        for element_id in unresolved {
            let scope = self
                .join_instances
                .get(&JoinKey::new(UNRESOLVED_JOIN_SCOPE, element_id.as_str()))
                .and_then(|eik| self.scopes.get(eik).copied())
                .unwrap_or(0);
            self.rekey_unresolved_join(&element_id, scope);
        }
    }
}

impl ProcessInstanceState {
    /// A stable, Camunda-style uppercase label for this state, used in error
    /// messages describing an illegal lifecycle transition.
    pub fn as_str(self) -> &'static str {
        match self {
            ProcessInstanceState::Active => "ACTIVE",
            ProcessInstanceState::Completed => "COMPLETED",
            ProcessInstanceState::Terminated => "TERMINATED",
            ProcessInstanceState::Terminating => "TERMINATING",
            ProcessInstanceState::Suspended => "SUSPENDED",
        }
    }
}

/// Lifecycle state of a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JobState {
    /// Created and activatable: available for a worker to activate. A job is
    /// also back in this state once its activation lock expires.
    Created,
    /// Activated and locked to a worker until its `deadline`. While locked it
    /// cannot be activated by another worker. Leased completion requires its token.
    Activated,
    /// Failed with no retries left: an incident was raised and the job is parked.
    /// It is neither activatable nor completable. Updating its retries
    /// ([`crate::Command::UpdateJobRetries`]) and then resolving the incident
    /// ([`crate::Command::ResolveIncident`]) returns it to `Created`.
    Failed,
    /// Consumed by a thrown business error: the job is terminal (the error was
    /// either caught by a boundary event or raised an incident).
    Errored,
    /// Cancelled because its activity was interrupted by a boundary event (here,
    /// an interrupting timer boundary event firing). Terminal: the job is neither
    /// activatable nor completable.
    Canceled,
    /// Completed by a worker.
    Completed,
}

/// What a job represents. Ordinary service-task jobs are [`JobKind::BpmnElement`]
/// (the default, so records serialized before this field existed load as such);
/// [`JobKind::ExecutionListener`] jobs are the sequential execution-listener
/// chain that runs on an element's activation/completion transition (ADR 0037).
/// A listener job carries the transition it fires on, its 0-based position in
/// the element's listener list, and the element's enclosing scope (so the next
/// listener or the resumed lifecycle transition can be driven on completion).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JobKind {
    /// An ordinary job that backs a service task (or ad-hoc agent).
    #[default]
    BpmnElement,
    /// A job for one execution listener in an element's sequential chain.
    ExecutionListener {
        event_type: crate::model::ListenerEventType,
        index: usize,
        scope: Key,
    },
    /// A job for one task listener in a user task's sequential chain (ADR 0037
    /// §6). Carries the transition it fires on, its 0-based position in the
    /// user task's listener list (filtered to this event type), and the user
    /// task whose deferred transition it gates.
    TaskListener {
        event_type: crate::model::TaskListenerEventType,
        index: usize,
        user_task_key: Key,
    },
}

/// A job created for a service task, awaiting activation and completion.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Job {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance that is parked waiting on this job.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    pub job_type: String,
    pub state: JobState,
    /// Name of the worker holding the activation lock, if any — **except** on a
    /// terminal, incident-bearing park (`Failed`/`Errored`), where it is retained
    /// as the *last activating* worker for incident attribution (Zeebe parity), so
    /// a non-empty value on such a record is historical, not an active lock. It is
    /// cleared when the job returns to the activatable pool (`JobLockExpired`, or a
    /// `JobFailed` with retries remaining).
    pub worker: Option<String>,
    /// Logical instant at which the current activation lock expires, if locked.
    /// Compared against the caller-supplied `now`; the engine never reads a
    /// wall clock itself.
    pub deadline: Option<u64>,
    /// Logical instant at which the current activation lock was acquired, if
    /// locked (the `now` carried on the activating `ActivateJobs` command).
    /// Cleared whenever the lock is released. `None` for a job that is not
    /// currently activated, or for records serialized before this field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub activated_at: Option<u64>,
    /// The lock duration the job was activated with (the activating command's
    /// `timeout`), frozen at activation. Unlike `deadline - activated_at`, this
    /// is immune to later `UpdateJobTimeout` lock extensions (which move
    /// [`Job::deadline`] but not the originally-requested timeout), so it always
    /// reflects what the worker asked for. Cleared when the lock is released;
    /// `None` when not activated or for pre-field records.
    #[cfg_attr(feature = "serde", serde(default))]
    pub activation_timeout: Option<u64>,
    /// Opaque per-activation token, issued only when the worker opts into leasing.
    /// Every job kind supports leases. The token survives failure/timeout and
    /// fences stale lifecycle commands until a leasing activation replaces it.
    /// Non-leasing workers cannot activate a previously leased job.
    /// Persistence accepts historical numeric tokens; public commands use strings.
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "crate::lease::deserialize_optional")
    )]
    pub lease_token: Option<String>,
    /// Once authoritatively activated, this job stays in the durable domain
    /// across expiry and failure, even without a fencing token.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "std::ops::Not::not")
    )]
    pub durable_activation: bool,
    /// Whether this job has ever been activated. Completion is permitted for any
    /// job that has been activated at least once and is not yet completed —
    /// regardless of which worker currently holds (or held) the lock. This is
    /// what lets a slow worker still complete a job whose lock expired and was
    /// re-activated by someone else.
    pub activated: bool,
    /// Remaining retries. Set to [`DEFAULT_JOB_RETRIES`] when the job is created
    /// and updated by `FailJob`. A failure that drops it to zero raises an
    /// incident and parks the job ([`JobState::Failed`]).
    pub retries: i32,
    /// Activation priority (0..=100; default [`DEFAULT_JOB_PRIORITY`]), resolved
    /// from the service task's `zeebe:priorityDefinition` at job creation. Higher
    /// priority is activated first; equal priorities fall back to oldest-first
    /// (key order). Immutable for the job's lifetime.
    #[cfg_attr(feature = "serde", serde(default = "default_job_priority"))]
    pub priority: i32,
    /// The logical instant the job was created (the `now` carried on the creating
    /// command), in milliseconds since the Unix epoch. Used to observe activation
    /// age. `0` for jobs created before the engine carried this field.
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_at: u64,
    /// What this job represents (ordinary element job vs execution-listener job).
    /// Defaults to [`JobKind::BpmnElement`] for records serialized before
    /// execution listeners existed, so ordinary jobs are unaffected.
    #[cfg_attr(feature = "serde", serde(default))]
    pub kind: JobKind,
    /// The last worker-reported failure/thrown-error message (Zeebe job
    /// `errorMessage`, #1327), limited by [`crate::limit_error_message`]. Retained
    /// on the job so it survives a compaction-floor read-model rebuild, which
    /// re-seeds the row from engine state rather than the (compacted) events.
    /// `None` until the first `JobFailed`/`JobErrorThrown`, and for records
    /// serialized before this field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub error_message: Option<String>,
    /// The last thrown error code (Zeebe job `errorCode`, #1327), from
    /// `JobErrorThrown`. Retained for the same compaction-floor-rebuild reason as
    /// [`Job::error_message`]. `None` until the first thrown error, and for
    /// pre-field records.
    #[cfg_attr(feature = "serde", serde(default))]
    pub error_code: Option<String>,
    /// Whether the last `JobFailed`/`JobErrorThrown` left the job with retries > 0
    /// (Zeebe exporter `jobFailedWithRetriesLeft`, #1327). Retained for the same
    /// rebuild-durability reason. `false` until the first fail/thrown error, and
    /// for pre-field records.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "std::ops::Not::not")
    )]
    pub has_failed_with_retries_left: bool,
    /// Camunda 8 v2 `retryBackOff` (#933): the logical instant (ms) before
    /// which this job — failed with retries left and a positive backoff — is
    /// not activatable. Set from [`crate::Event::JobFailed`], cleared on the
    /// next activation. `None` = no pending backoff (and for pre-field
    /// records).
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub retry_backoff_until: Option<u64>,
    /// The settlements this job has already consumed (#1181): each lease token
    /// (per-activation fence) that settled it, with the outcome. This is the
    /// replay fence for a **live** job — unlike the bounded
    /// [`State::settled_jobs`] ledger it is never evicted while the job lives,
    /// so a retryable failure's replay is still answered idempotently after the
    /// ledger entry aged out, and a recorded token asserting a different
    /// outcome is rejected instead of settling the same activation twice.
    /// Populated by the same replicated `JobSettlementRecorded` event that
    /// feeds the ledger. `serde(default)`: jobs serialized before #1181 load
    /// with no consumed settlements.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub settled_tokens: Vec<JobSettlement>,
    /// Timing of the job's first terminal transition (#1440): the
    /// `JobCompleted`/`JobCanceled` instant and the deadline the job held just
    /// before it (Camunda keeps the last projected deadline on a completed /
    /// canceled job). The engine retains a terminal job until its owning
    /// instance is evicted, so a snapshot can carry a COMPLETED job of a still-
    /// active instance; this is what lets a snapshot-seeded read model recover
    /// `endTime`/`lastUpdateTime`/`deadline` without the (compacted) events.
    /// `None` while the job is live, and for jobs that went terminal before
    /// this field existed or via a pre-field event with no instant.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub terminal_end: Option<JobTerminalEnd>,
    /// Mapping-time secret-reference provenance (#1640): which value paths of
    /// the job's input-mapping targets hold a TRUSTED `camunda.secrets.<name>`
    /// reference, and which names. Copied from [`crate::Event::JobCreated`].
    /// Only paths and secret NAMES are stored — never plaintext. Empty (resolve
    /// nothing — fail closed) for jobs created before this field existed.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub secret_provenance: Vec<SecretProvenance>,
}

/// One trusted secret-reference site of a job's input mappings (#1640): the
/// dotted input-mapping target `path` (e.g. `token`, `payload.auth`) and the
/// secret names whose `camunda.secrets.<name>` references the mapping
/// structurally delivered there from a trusted source (a literal
/// `camunda.secrets.<name>` path or a direct read of a `SECRET_REFERENCE`
/// cluster variable — [`crate::feel::eval_with_provenance`]). Host-side
/// activation resolves only these names, only inside this path.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SecretProvenance {
    /// The dotted input-mapping target path.
    pub path: String,
    /// The trusted secret names (sorted).
    pub secrets: Vec<String>,
}

/// See [`Job::terminal_end`] (#1440).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JobTerminalEnd {
    /// Logical instant (ms since the Unix epoch) of the terminal transition.
    pub at: u64,
    /// The activation-lock deadline the job held when it went terminal.
    pub deadline: Option<u64>,
}

impl Job {
    /// Whether this job's current activation lock is journaled (replicated):
    /// leased or durably activated. The SINGLE predicate for soft-vs-durable
    /// lock state — used both to pick which deadlines a cascade stamps and to
    /// decide whether `job_terminal_deadline` reads the replicated lock or the
    /// stamp, so the two can never disagree (#1856 review). It deliberately
    /// reads live lock state, not model-derived eligibility: a soft-activated
    /// job migrated onto an agent-marked element is still a soft lock.
    pub fn has_replicated_lock(&self) -> bool {
        self.lease_token.is_some() || self.durable_activation
    }

    /// The activating worker as a durable **attribution**, or `None` when there
    /// is none. An *empty* worker string (`Some("")`) is not an attribution: it
    /// normalizes to `None` here so a terminal event captured from this job
    /// (`JobCompleted` / `JobFailed` / `JobErrorThrown`) never carries `Some("")`.
    ///
    /// The `JobActivated` reducer already normalizes an empty worker at the
    /// source, so a *freshly-activated* job can never hold `Some("")`. This guard
    /// closes the remaining path: a job **restored from a snapshot written before
    /// that normalization existed** can still carry `Some("")`, which would
    /// otherwise be captured verbatim into a terminal event and stamped as an
    /// empty attribution downstream (#1191). Likewise the digest-recovery
    /// sentinel [`crate::engine::LEASE_DIGEST_WORKER`] is a placeholder, not a
    /// worker, and is never attributed (#963). Single source for the three terminal
    /// capture sites in the engine, so the invariant cannot drift between them.
    pub fn attribution_worker(&self) -> Option<String> {
        self.worker
            .clone()
            .filter(|w| !w.is_empty() && w != crate::engine::LEASE_DIGEST_WORKER)
    }
}

/// Default job-activation priority when no `zeebe:priorityDefinition` is declared.
pub const DEFAULT_JOB_PRIORITY: i32 = 50;

/// serde default for [`Job::priority`] / [`crate::Event::JobCreated`] on records
/// serialized before the field existed.
#[cfg(feature = "serde")]
pub fn default_job_priority() -> i32 {
    DEFAULT_JOB_PRIORITY
}

/// serde default for `true`-by-default boolean fields (BPMN
/// `cancelRemainingInstances`) on records serialized before the field existed.
#[cfg(feature = "serde")]
pub fn default_true() -> bool {
    true
}

/// The ordering position of an activatable job in the per-type index: highest
/// priority first (stored negated so a `BTreeSet`'s ascending order yields it
/// first), then lowest key (oldest-first — keys are monotonic with creation
/// within a partition — as the SLA/age tiebreak).
///
/// The negation is computed in `i64` so it is **collision-free over the whole
/// accepted priority domain** (`i32::MIN..=i32::MAX`, see
/// [`crate::engine::expression::evaluate_job_priority`]): a plain `i32`
/// negation overflows at `i32::MIN`, and a saturating one maps both `i32::MIN`
/// and `i32::MIN + 1` to `i32::MAX`, letting an older minimum-priority job win
/// the key tiebreak over a newer, strictly-higher-priority one (#1398 review).
/// The index is in-memory only (rebuilt from `jobs` by [`resync_job_index`] /
/// [`restore_instance_snapshot`]), never persisted, so widening the stored key
/// is replay-safe.
#[inline]
pub(crate) fn activation_order(priority: i32, key: Key) -> (i64, Key) {
    (-i64::from(priority), key)
}

/// Retries a job starts with when first created.
pub const DEFAULT_JOB_RETRIES: i32 = 3;

/// serde default for [`Job::retries`] / [`crate::Event::JobCreated`] on records
/// serialized before the `retries` field existed on `JobCreated`.
#[cfg(feature = "serde")]
pub fn default_job_retries() -> i32 {
    DEFAULT_JOB_RETRIES
}

/// The in-flight lifecycle transition a user task is deferring while its task
/// listeners run (ADR 0037 §6). Held on [`UserTask::pending`] between the
/// command (or termination) that triggered the transition and the moment its
/// listener chain drains and the transition commits. Durable so replay and
/// failover resume the chain exactly; absent (the common case) for
/// listener-free user tasks, which transition synchronously and are
/// byte-identical to the pre-task-listener engine.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PendingUserTaskTransition {
    /// The transition being deferred (drives which listeners run and what final
    /// event commits).
    pub event_type: crate::model::TaskListenerEventType,
    /// Target assignee for an `Assigning` transition: `Some(name)` to assign,
    /// `None` to unassign. Ignored for other transitions.
    #[cfg_attr(feature = "serde", serde(default))]
    pub assignee: Option<String>,
    /// Normalised update fields for an `Updating` transition. Ignored otherwise.
    #[cfg_attr(feature = "serde", serde(default))]
    pub update: Option<PendingUserTaskUpdate>,
    /// Completion variables for a `Completing` transition, applied to the
    /// instance when the chain drains. Ignored otherwise.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variables: BTreeMap<String, Value>,
    /// Corrections accumulated from listeners completed so far.
    #[cfg_attr(feature = "serde", serde(default))]
    pub corrections: crate::model::UserTaskCorrections,
}

/// The normalised update payload captured on a deferred `Updating` transition
/// (mirrors [`crate::Event::UserTaskUpdated`] fields).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PendingUserTaskUpdate {
    #[cfg_attr(feature = "serde", serde(default))]
    pub candidate_groups: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub candidate_users: Option<Vec<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub due_date: Option<Option<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub follow_up_date: Option<Option<String>>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub priority: Option<i32>,
}

/// Lifecycle state of a (native) user task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UserTaskState {
    /// Created and available for a human to claim/complete.
    Created,
    /// Completed. Terminal: the parked token has resumed.
    Completed,
    /// Cancelled because its activity/instance was terminated. Terminal.
    Canceled,
}

/// A user task created for a `userTask` element, awaiting human completion.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserTask {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance that is parked waiting on this user task.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    pub state: UserTaskState,
    /// The currently assigned user, if any.
    pub assignee: Option<String>,
    /// Candidate groups that may claim the task.
    pub candidate_groups: Vec<String>,
    /// Candidate users that may claim the task.
    pub candidate_users: Vec<String>,
    /// The due date (an ISO-8601 string), if set.
    pub due_date: Option<String>,
    /// The follow-up date (an ISO-8601 string), if set.
    pub follow_up_date: Option<String>,
    /// The task priority (0..=100); defaults to 50.
    pub priority: i32,
    /// The resolved numeric key of the task's form, if its
    /// `zeebe:formDefinition` declared a `formId` that resolved against a
    /// currently-deployed form (latest version) at creation. Surfaced as the v2
    /// user-task `formKey`; downstream `GetFormByKey` serves the schema. `None`
    /// when the task declares no embedded form (or its `formId` matched none).
    /// Snapshots written before form linkage load as `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub form_key: Option<Key>,
    /// The external form reference declared via `zeebe:formDefinition
    /// externalReference`, surfaced verbatim as the v2 user-task
    /// `externalFormReference`. `None` when the task declares no external form.
    /// Snapshots written before form linkage load as `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub external_form_reference: Option<String>,
    /// The logical instant the task was created (the `now` carried on the
    /// activating command), in milliseconds since the Unix epoch.
    pub created_at: u64,
    /// The lifecycle transition (if any) this user task is currently deferring
    /// while its task listeners run (ADR 0037 §6). `None` (the default, and the
    /// only value for listener-free user tasks) means no transition is in
    /// flight; serialized-before-task-listeners records load as `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub pending: Option<PendingUserTaskTransition>,
}
/// A running (or completed) process instance.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessInstance {
    pub key: Key,
    pub process_id: String,
    /// The unique key of the process **definition version** this instance was
    /// created on. Resolves the instance's executable definition through
    /// [`State::process_versions`], so the instance always runs the version it
    /// started on even after a newer version is redeployed (Zeebe parity).
    /// `0` for instances created before version pinning existed (or snapshots
    /// written before this field); [`State::definition_for`] falls back to the
    /// latest-by-id index for those. `serde(default)` for snapshot back-compat.
    #[cfg_attr(feature = "serde", serde(default))]
    pub process_definition_key: Key,
    pub state: ProcessInstanceState,
    /// The replicated terminal epoch ([`State::terminal_epoch`]) at which this
    /// instance reached a terminal state (`Completed` / `Terminated`), recorded
    /// by [`State::note_terminated`] in the same apply arm that set `state`.
    /// `0` while the instance is live (and for instances written before this
    /// field existed, via `serde(default)`). This is the **target-specific**
    /// replicated lifecycle-validity stamp the orphan-quarantine gate reads
    /// (#1574 review, review 5899722557): unlike the global epoch distance, it
    /// names THIS instance's terminal transition, so an unrelated instance
    /// completing between a stamp and its commitment is never misread as the
    /// target's termination. Replicated-only (derived in `apply_event`), so
    /// every replica records the identical value.
    #[cfg_attr(feature = "serde", serde(default))]
    pub terminated_epoch: u64,
    /// When this instance is currently `SUSPENDED`, the millisecond-since-epoch
    /// instant it most recently entered suspension (the `suspendedDate` surfaced
    /// on the REST result). `None` whenever the instance is not currently
    /// suspended — set on `ProcessInstanceSuspended`, cleared on
    /// `ProcessInstanceResumed` (and on any terminal transition). Moves in
    /// lockstep with `state == Suspended`. `serde(default)` so snapshots written
    /// before suspend/resume support deserialize as `None`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub suspended_at: Option<u64>,
    /// The logical instant the instance was started (the `created_at` carried on
    /// the creating command), in milliseconds since the Unix epoch. This is the
    /// instance's start date. `0` for instances created before the engine
    /// recorded a start time.
    pub created_at: u64,
    /// User-defined tags associated with this instance. Empty for instances
    /// created before tags were supported.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tags: Vec<String>,
    /// Optional user-defined business identifier for this instance. `None` for
    /// instances created without a business id or before business ids were
    /// supported.
    #[cfg_attr(feature = "serde", serde(default))]
    pub business_id: Option<String>,
    /// When this instance is a **child process instance** spawned by a call
    /// activity, the `processInstanceKey` of the calling (parent) instance and
    /// the element instance key of the spawning call-activity element (C8
    /// `parentProcessInstanceKey` / `parentElementInstanceKey`). Both `None` for
    /// a top-level instance (API/message/timer/signal start) and for instances
    /// created before native call activities existed (`serde(default)`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub parent_process_instance_key: Option<Key>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub parent_element_instance_key: Option<Key>,
    /// Currently-active element instances, keyed by element-instance key. An
    /// element instance is "active" from `ACTIVATED` until `COMPLETED`; a service
    /// task therefore stays here while its job is pending, as does a token parked
    /// on an incident. When this becomes empty the instance has no remaining
    /// tokens and is complete.
    pub active: BTreeMap<Key, ElementId>,
    /// Maps each active element instance to the element instance of the embedded
    /// sub-process that encloses it. Element instances in the process-level
    /// (root) scope are absent. Used to detect when a sub-process scope has
    /// drained (all its inner tokens consumed) and to terminate a scope when an
    /// error boundary interrupts it.
    pub scopes: BTreeMap<Key, Key>,
    /// Process variables (used by exclusive-gateway conditions). Shared via `Arc`
    /// so activating a job (which snapshots the instance's variables) is a cheap
    /// refcount bump rather than a deep clone of the (up to 50 KB) decoded value
    /// tree. A mutation (`VariablesUpdated`) copies-on-write via `Arc::make_mut`.
    pub variables: Arc<BTreeMap<String, Value>>,
    /// For each open join: how many tokens have arrived over an *unidentified*
    /// flow (see [`Event::ParallelJoinTokenArrived`](crate::Event)'s `flow`):
    /// arrivals journaled before #1233 (#1237 for inclusive joins) and
    /// activations that did not come over a flow. A join counts each of these
    /// as one taken incoming flow; a firing clears them all.
    #[cfg_attr(feature = "serde", serde(with = "join_map_serde"))]
    pub join_counts: BTreeMap<JoinKey, usize>,
    /// For each open parallel- or inclusive-gateway join: arrivals per
    /// identified incoming flow (Zeebe's "number of taken sequence flows",
    /// #1233, #1237). Absent from
    /// pre-#1233 snapshots, hence `serde(default)`.
    #[cfg_attr(feature = "serde", serde(default, with = "join_map_serde"))]
    pub join_flow_arrivals: BTreeMap<JoinKey, FlowArrivals>,
    /// For each open parallel-gateway join: the element instance accumulating
    /// the arriving tokens.
    #[cfg_attr(feature = "serde", serde(with = "join_map_serde"))]
    pub join_instances: BTreeMap<JoinKey, Key>,
    /// The [`join_instances`](Self::join_instances) that are **parked**: they
    /// hold waiting tokens but have not activated (#1244). Zeebe keeps a join's
    /// waiting tokens as active sequence flows on the flow scope and writes no
    /// element-instance record until the join's guard accepts an activation.
    /// Nano parks those tokens on a key that is in `active` and `scopes`, so
    /// they keep the process instance and the enclosing scope alive and are
    /// torn down with it, but it emits no lifecycle event for it. The key leaves
    /// this set when the join activates (`ElementActivating`), and leaves
    /// `active` when the join is reset. Absent from pre-#1244 snapshots (whose
    /// open joins were activated visibly), hence `serde(default)`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub parked_joins: std::collections::BTreeSet<Key>,
    /// Element instances that are ACTIVATING but not yet ACTIVATED: inserted by
    /// `ElementActivating`, removed by `ElementActivated` (and on completion /
    /// teardown). The single source of truth for "is `ElementActivated` still
    /// owed?" (#1429): a drained `start` execution-listener chain writes the
    /// marker only for a key still in this set, so a modification-seeded
    /// activation, or a chain opened under a pre-#1429 build (which wrote the
    /// marker before the chain), never gets a second one. Absent from older
    /// snapshots (`serde(default)`), which therefore owe no marker.
    #[cfg_attr(feature = "serde", serde(default))]
    pub activating: std::collections::BTreeSet<Key>,
    /// The outgoing-flow selection a split gateway (exclusive, or an inclusive
    /// fork) made while ACTIVATING (#1402), keyed by the gateway's element
    /// instance. Completion takes exactly these flows instead of re-evaluating
    /// the conditions (FEEL is not required to be deterministic, and a
    /// listener may rewrite a condition variable). Inserted by
    /// [`crate::Event::GatewayRouteSelected`], removed when the gateway
    /// completes. Empty for instances with no in-flight split gateway and for
    /// snapshots written before the selection was journaled
    /// (`serde(default)`); a gateway completing without a recorded selection
    /// (a legacy journal, or an incident retry) re-selects from the current
    /// variables exactly as before.
    #[cfg_attr(feature = "serde", serde(default))]
    pub gateway_routes: BTreeMap<Key, Vec<usize>>,
    /// The event-based gateway race each armed catch element instance belongs
    /// to (#1676): catch element instance -> the gateway element instance whose
    /// deferred choice armed it. Inserted by an [`crate::Event::ElementActivating`]
    /// carrying a `race`, removed when the catch completes or terminates. A race
    /// winner withdraws only siblings sharing its race, so two activations of
    /// one gateway in the same scope never withdraw each other's catches.
    /// `serde(default)` and omitted when empty: older snapshots decode unchanged.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub races: BTreeMap<Key, Key>,
    /// Keys of incidents currently **active** on this instance (parked tokens).
    /// The full records live in [`State::incidents`] and are retained after
    /// resolution; resolving an incident removes its key from this active index
    /// (so `hasIncident` reflects only open incidents).
    pub incidents: Vec<Key>,
    /// `true` when this instance's `variables` payload has been spilled to the
    /// host's disk-backed store to bound hot-state memory under a large backlog.
    /// While spilled, `variables` holds an empty placeholder; the host rehydrates
    /// it (see [`crate::Engine::rehydrate_variables`]) before any command that
    /// needs the real payload — notably job activation. Purely a host-managed
    /// memory optimisation: it never changes the engine's logical state, is not
    /// journaled, and is irrelevant to replay (a replayed instance starts
    /// resident with its variables from the log).
    pub variables_spilled: bool,
    /// Active multi-instance bodies in this instance, keyed by the body element
    /// instance. Empty (the default) for instances with no multi-instance
    /// activity and when deserializing snapshots written before multi-instance
    /// support existed. Rides along in [`InstanceSnapshot::instance`] on spill,
    /// so it needs no separate drain/rehydrate wiring.
    #[cfg_attr(feature = "serde", serde(default))]
    pub multi_instances: BTreeMap<Key, MultiInstanceState>,
    /// Active ad-hoc sub-process containers in this instance, keyed by the
    /// container element instance (which is also the ad-hoc token scope). Empty
    /// (the default) for instances with no ad-hoc activity and when
    /// deserializing snapshots written before ad-hoc runtime support existed.
    /// Rides along in [`InstanceSnapshot::instance`] on spill like
    /// `multi_instances`, so it needs no separate drain/rehydrate wiring.
    #[cfg_attr(feature = "serde", serde(default))]
    pub adhoc_instances: BTreeMap<Key, AdHocState>,
    /// Non-root variable scopes: each scope-owning element instance (embedded
    /// sub-process, multi-instance body, multi-instance child) mapped to its
    /// parent scope-owner. The root (process-instance) scope is implicit — its
    /// key is the instance key and it is never present here. Empty for instances
    /// with only the root scope. Together with `scope_variables` this is the
    /// Zeebe-style hierarchical variable tree (Part C). `serde(default)` so
    /// snapshots written before scoping deserialize as root-only.
    #[cfg_attr(feature = "serde", serde(default))]
    pub scope_parents: BTreeMap<Key, Key>,
    /// Local variables held directly by each non-root scope (keyed by the
    /// scope-owning element instance). This includes a multi-instance child's
    /// `inputElement`/`loopCounter` bindings and a multi-instance body's
    /// `outputCollection`. The root scope's variables live in `variables`. A read
    /// resolves a name from the local scope upward to the root (first hit wins); a
    /// write follows Zeebe variable propagation. Empty for root-only instances,
    /// keeping the flat fast path unchanged.
    #[cfg_attr(feature = "serde", serde(default))]
    pub scope_variables: BTreeMap<Key, BTreeMap<String, Value>>,
    /// Message / signal payloads held for event sub-process start events with
    /// output mappings, keyed by the event sub-process container instance
    /// (see [`crate::Event::EventSubProcessPayloadHeld`]). Seeded LOCAL to the
    /// container's start event when it activates; dropped with the
    /// container's scope.
    #[cfg_attr(feature = "serde", serde(default))]
    pub esp_held_payloads: BTreeMap<Key, BTreeMap<String, Value>>,
    /// Compensable activities that completed successfully and carry a
    /// compensation boundary event, in completion order (oldest first). A
    /// [`crate::model::ElementKind::CompensationThrowEvent`] in the same scope
    /// consumes these (newest first) to run each one's compensation handler.
    /// Empty for instances with no compensation and for snapshots written
    /// before compensation support existed (`serde(default)`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub compensable: CompensationSubscriptions,
    /// In-flight compensation throw events, keyed by the throw event's element
    /// instance. Each records the handler activities it is still waiting on;
    /// when a throw's `pending_handlers` empties the throw completes. Empty for
    /// instances with no active compensation and for pre-compensation snapshots.
    #[cfg_attr(feature = "serde", serde(default))]
    pub compensation_waits: BTreeMap<Key, CompensationWait>,
    /// Engine-native AgentInstance objects owned by this process instance, keyed
    /// by their dedicated `agent_instance_key` (Camunda 8.10 AgentInstance, ADR
    /// Stage 3). Empty for instances with no agent element and for snapshots
    /// written before agent support existed (`serde(default)`). Rides along in
    /// [`InstanceSnapshot::instance`] on spill like `adhoc_instances`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub agent_instances: BTreeMap<Key, crate::agent::AgentInstance>,
    /// Append-only AgentHistory turn log, keyed by the owning
    /// `agent_instance_key` (Camunda 8.10 AgentHistory, ADR Stage 3 / slice S2).
    /// Each value is the ordered list of turns for that agent instance, sorted
    /// by `(loop_iteration, produced_at, agent_history_key)`; turns are only ever
    /// appended and their `commit_status` only ever transitions Pending ->
    /// Committed / Discarded. Empty for instances with no agent element and for
    /// snapshots written before agent-history support existed (`serde(default)`).
    /// Rides along in [`InstanceSnapshot::instance`] on spill like
    /// `agent_instances`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub agent_history: BTreeMap<Key, Vec<crate::agent::AgentHistoryRecord>>,
}

/// An instance's compensable subscriptions, in completion order (oldest
/// first), plus a per-scope count index DERIVED from them.
///
/// The index answers "does scope `S` hold any subscription?" in O(1). Every
/// completing sub-process and multi-instance child asks that question, so a
/// linear scan would make a large sequential multi-instance run quadratic on
/// the serialized engine path (#901 review). The index cannot drift from the
/// list: the list is private, every mutation (`push`, `retain`, `clear`) goes
/// through this type and updates both, and the serialized form is the plain
/// list (`serde(from/into)`), so the index is rebuilt on every snapshot
/// restore and the on-disk shape is unchanged. Read access is through `Deref`
/// to a slice; there is deliberately no `DerefMut`.
#[derive(Clone, Debug, Default)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(
        from = "Vec<CompensationSubscription>",
        into = "Vec<CompensationSubscription>"
    )
)]
pub struct CompensationSubscriptions {
    items: Vec<CompensationSubscription>,
    by_scope: BTreeMap<Key, usize>,
}

impl CompensationSubscriptions {
    /// Appends a subscription (completion order is significant).
    pub fn push(&mut self, subscription: CompensationSubscription) {
        *self.by_scope.entry(subscription.scope).or_default() += 1;
        self.items.push(subscription);
    }

    /// Keeps only the subscriptions `keep` accepts, preserving order.
    pub fn retain(&mut self, mut keep: impl FnMut(&CompensationSubscription) -> bool) {
        let by_scope = &mut self.by_scope;
        self.items.retain(|c| {
            let kept = keep(c);
            if !kept {
                if let Some(n) = by_scope.get_mut(&c.scope) {
                    *n -= 1;
                    if *n == 0 {
                        by_scope.remove(&c.scope);
                    }
                }
            }
            kept
        });
    }

    /// Removes every subscription.
    pub fn clear(&mut self) {
        self.items.clear();
        self.by_scope.clear();
    }

    /// Whether any subscription was recorded in `scope` (O(1)).
    pub fn has_scope(&self, scope: Key) -> bool {
        self.by_scope.contains_key(&scope)
    }
}

impl std::ops::Deref for CompensationSubscriptions {
    type Target = [CompensationSubscription];
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

// Equality is over the canonical list only; the index is derived from it.
impl PartialEq for CompensationSubscriptions {
    fn eq(&self, other: &Self) -> bool {
        self.items == other.items
    }
}
impl Eq for CompensationSubscriptions {}

impl From<Vec<CompensationSubscription>> for CompensationSubscriptions {
    fn from(items: Vec<CompensationSubscription>) -> Self {
        let mut out = Self::default();
        for c in items {
            out.push(c);
        }
        out
    }
}

impl From<CompensationSubscriptions> for Vec<CompensationSubscription> {
    fn from(subs: CompensationSubscriptions) -> Self {
        subs.items
    }
}

impl FromIterator<CompensationSubscription> for CompensationSubscriptions {
    fn from_iter<I: IntoIterator<Item = CompensationSubscription>>(iter: I) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

impl<'a> IntoIterator for &'a CompensationSubscriptions {
    type Item = &'a CompensationSubscription;
    type IntoIter = std::slice::Iter<'a, CompensationSubscription>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

#[cfg(test)]
mod compensation_subscriptions_tests {
    use super::*;

    fn sub(key: Key, scope: Key) -> CompensationSubscription {
        CompensationSubscription {
            element_instance_key: key,
            element_id: format!("a{key}"),
            handler: "h".to_string(),
            scope,
            scope_subscription: false,
            boundary_id: String::new(),
        }
    }

    /// The derived index must agree with a brute-force scan of the list after
    /// every kind of mutation.
    fn assert_index_consistent(subs: &CompensationSubscriptions) {
        let mut expected: BTreeMap<Key, usize> = BTreeMap::new();
        for c in subs.iter() {
            *expected.entry(c.scope).or_default() += 1;
        }
        assert_eq!(subs.by_scope, expected);
    }

    #[test]
    fn scope_index_tracks_push_retain_and_clear() {
        let mut subs = CompensationSubscriptions::default();
        assert!(!subs.has_scope(1));
        subs.push(sub(10, 1));
        subs.push(sub(11, 1));
        subs.push(sub(12, 2));
        assert_index_consistent(&subs);
        assert!(subs.has_scope(1) && subs.has_scope(2));

        subs.retain(|c| c.element_instance_key != 10);
        assert_index_consistent(&subs);
        assert!(subs.has_scope(1), "one scope-1 subscription remains");

        subs.retain(|c| c.scope != 1);
        assert_index_consistent(&subs);
        assert!(!subs.has_scope(1));
        assert_eq!(subs.len(), 1);

        subs.clear();
        assert_index_consistent(&subs);
        assert!(subs.is_empty() && !subs.has_scope(2));
    }

    #[test]
    fn order_is_preserved_and_from_vec_rebuilds_the_index() {
        let subs: CompensationSubscriptions = vec![sub(3, 7), sub(1, 7), sub(2, 8)].into();
        assert_index_consistent(&subs);
        let keys: Vec<Key> = subs.iter().map(|c| c.element_instance_key).collect();
        assert_eq!(keys, vec![3, 1, 2]);
    }

    /// The on-disk shape is the plain list (snapshot-compatible), and decoding
    /// rebuilds the index.
    #[cfg(feature = "serde")]
    #[test]
    fn serializes_as_the_plain_list_and_rebuilds_the_index_on_decode() {
        let subs: CompensationSubscriptions = vec![sub(1, 5), sub(2, 6)].into();
        let json = serde_json::to_value(&subs).unwrap();
        let plain: Vec<CompensationSubscription> = subs.iter().cloned().collect();
        assert_eq!(json, serde_json::to_value(&plain).unwrap());
        let back: CompensationSubscriptions = serde_json::from_value(json).unwrap();
        assert_eq!(back, subs);
        assert_index_consistent(&back);
        assert!(back.has_scope(5) && back.has_scope(6));
    }
}

/// A completed, compensable activity awaiting a possible compensation throw.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CompensationSubscription {
    /// The completed activity's element instance key.
    pub element_instance_key: Key,
    /// The completed activity's element id.
    pub element_id: ElementId,
    /// The compensation handler activity run to compensate it. Empty for a
    /// **scope subscription** (#901): a completed flow scope (sub-process)
    /// whose compensation is the fan-out into its nested subscriptions, not a
    /// handler of its own.
    pub handler: ElementId,
    /// The scope the activity completed in (`0` for the root scope).
    pub scope: Key,
    /// `true` for a flow-scope subscription (#901): recorded when a sub-process
    /// that holds at least one compensable subscription completes (Zeebe
    /// `isFlowScopeWithSubscriptions`), so a whole-scope throw can trigger the
    /// nested subscriptions through it. `serde(default)` decodes pre-#901
    /// snapshots as `false` — every pre-#901 subscription was a boundary one.
    #[cfg_attr(feature = "serde", serde(default))]
    pub scope_subscription: bool,
    /// The compensation boundary event this subscription belongs to (#1635);
    /// empty for a scope subscription or a pre-#1635 snapshot (resolution
    /// then falls back to boundary-id occurrence order).
    #[cfg_attr(feature = "serde", serde(default))]
    pub boundary_id: ElementId,
}

/// A compensation throw event's outstanding wait: the handler activities it
/// triggered that have not yet completed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CompensationWait {
    /// The throw event's element id (routed onward when the wait empties).
    pub throw_element_id: ElementId,
    /// The scope the throw event runs in.
    pub scope: Key,
    /// Handler activity ids still outstanding (one entry per triggered handler).
    pub pending_handlers: Vec<ElementId>,
    /// The element-instance key of each outstanding handler, positionally
    /// aligned with `pending_handlers` (#897). This is the handler-instance ->
    /// throw-instance binding: a completing handler instance is matched against
    /// these keys, so its completion retires the throw that activated IT — never
    /// another throw in the same scope waiting on the same handler element id.
    /// Empty for waits replayed from pre-#897 `CompensationTriggered` records
    /// (`serde(default)`); such a wait falls back to element-id completion
    /// matching, exactly the pre-#897 behaviour for that journal.
    #[cfg_attr(feature = "serde", serde(default))]
    pub pending_handler_keys: Vec<Key>,
    /// The compensable element-instance keys of the **scope subscriptions**
    /// (#901) this throw triggered and is still waiting on: completed
    /// sub-processes whose nested compensation fan-out has not fully drained.
    /// A scope subscription retires bottom-to-top (Zeebe
    /// `completeFlowScopeSubscriptionFromBottomToTop`) once every subscription
    /// inside its scope has completed; the throw completes only when both this
    /// list and `pending_handlers` are empty. `serde(default)` decodes
    /// pre-#901 waits with an empty list.
    #[cfg_attr(feature = "serde", serde(default))]
    pub pending_scope_subscriptions: Vec<Key>,
    /// Positionally aligned with `pending_handler_keys` (#901): the scope each
    /// outstanding handler's subscription was recorded in. A scope
    /// subscription is drained only once no outstanding handler sits in its
    /// scope. `serde(default)` decodes older waits with an empty list.
    #[cfg_attr(feature = "serde", serde(default))]
    pub pending_handler_scopes: Vec<Key>,
    /// Positionally aligned with `pending_scope_subscriptions` (#901): the
    /// scope each outstanding scope subscription was recorded in (its parent),
    /// so a parent scope drains only after its nested scope subscriptions.
    /// `serde(default)` decodes older waits with an empty list.
    #[cfg_attr(feature = "serde", serde(default))]
    pub pending_scope_subscription_parents: Vec<Key>,
}

/// Runtime state of an active multi-instance body (the element instance carrying
/// [`crate::model::MultiInstance`] characteristics). Reconstructed from the
/// multi-instance events, so it needs no bespoke snapshot handling beyond riding
/// along in the owning [`ProcessInstance`].
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MultiInstanceState {
    /// The activity element id this body loops over.
    pub element_id: ElementId,
    /// `true` runs children one at a time; `false` runs all in parallel.
    pub sequential: bool,
    /// The evaluated input collection driving the loop (one child per item).
    pub items: Vec<Value>,
    /// Instance-level list variable the per-child output is collected into.
    pub output_collection: Option<String>,
    /// FEEL expression collected per child into `output_collection`.
    pub output_element: Option<String>,
    /// FEEL boolean evaluated after each child; `true` completes the body early.
    pub completion_condition: Option<String>,
    /// The BPMN `cancelRemainingInstances` attribute (default `true`), resolved
    /// at parse and carried by [`crate::Event::MultiInstanceActivated`]: whether
    /// a fired completion condition cancels the children still running (`true`)
    /// or latches and defers the body's completion until they drain (`false`).
    /// Additive with a serde default of `true` — a body recorded before this
    /// field existed completes exactly as the pre-field engine did.
    #[cfg_attr(feature = "serde", serde(default = "default_true"))]
    pub cancel_remaining_instances: bool,
    /// Local variable name each item binds to in its child scope.
    pub input_element: Option<String>,
    /// How many children have been spawned so far (drives sequential's next
    /// index and detects when all items have been started).
    pub spawned: usize,
    /// Element instances of the children that are still active.
    pub active: std::collections::BTreeSet<Key>,
    /// Authoritative 0-based loop index of each child element instance, recorded
    /// at activation and read back on completion to position the child's output.
    /// The index is engine-owned runtime state, NOT derived from the child's
    /// mutable `loopCounter` variable — so no write path into the child scope
    /// (`zeebe:input` mapping, a worker `SetVariables`, an inner output mapping)
    /// can corrupt `outputCollection` indexing / join bookkeeping.
    #[cfg_attr(feature = "serde", serde(default))]
    pub child_indices: std::collections::BTreeMap<Key, usize>,
    /// Collected per-child output, positioned by the child's 0-based index.
    pub output_values: Vec<Option<Value>>,
    /// Latched once the `<completionCondition>` evaluated `true` with
    /// `cancelRemainingInstances=false`: the loop stops evaluating the condition
    /// and the body completes once its outstanding children drain, even if a
    /// variable the condition reads later changes (or becomes unreadable) — a
    /// satisfied condition is not reneged (Zeebe
    /// `ElementInstance#isCompletionConditionFulfilled`, mirrored from the ad-hoc
    /// container's latch). Additive with a serde default: a body recorded before
    /// this field existed has no latched fulfilment.
    #[cfg_attr(feature = "serde", serde(default))]
    pub completion_condition_fulfilled: bool,
}

/// Runtime state of an active ad-hoc sub-process container (ADR 0023 seam 2).
/// The container element instance is itself the ad-hoc token scope: activated
/// "tool" children run inside it and the container's `outputCollection`
/// accumulates here. Reconstructed from the ad-hoc events, so it rides along in
/// the owning [`ProcessInstance`] with no bespoke snapshot handling.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdHocState {
    /// The `adHocSubProcess` container element id.
    pub element_id: ElementId,
    /// The container's `zeebe:adHoc outputCollection` result-variable name, if
    /// declared — each activated tool's `output_element` is appended here.
    pub output_collection: Option<String>,
    /// The container's `zeebe:adHoc outputElement` FEEL expression, evaluated in
    /// each completed tool's scope and appended to `output_collection`.
    pub output_element: Option<String>,
    /// Element instances of tool children still running this turn. The container
    /// re-emits its agent job once this drains (and no completion was signalled).
    pub active: std::collections::BTreeSet<Key>,
    /// How many agent-job turns have run (drives the metrics + runaway guard).
    pub iterations: u32,
    /// Latched once the declared `<completionCondition>` has been satisfied with
    /// `cancelRemainingInstances=false`: the loop stops activating new tools and
    /// the container completes once its outstanding children drain, even if a
    /// later tool's output would no longer satisfy the condition (Zeebe
    /// `ElementInstance#isCompletionConditionFulfilled`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub completion_condition_fulfilled: bool,
}

/// Why an incident was raised. Maps to a recovery story and to the REST
/// `errorType` taxonomy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IncidentKind {
    /// A job exhausted its retries (`FailJob` with 0 left). Recoverable by
    /// updating retries and resolving.
    JobNoRetries,
    /// An exclusive gateway found no matching outgoing sequence flow.
    NoMatchingSequenceFlow,
    /// A sequence-flow condition (or other expression) failed to evaluate to the
    /// expected type — a FEEL parse error, a type error, or a non-boolean
    /// condition result.
    ExpressionEvaluation,
    /// A thrown business error was not caught by any boundary event.
    UnhandledError,
    /// A `businessRuleTask` failed to evaluate its decision — the decision id was
    /// not found, or evaluation produced an error (bad FEEL, hit-policy
    /// violation, missing input). Recoverable once the definition/inputs are
    /// fixed and the incident is resolved.
    DecisionEvaluation,
    /// A call activity could not spawn its child process: the resolved
    /// `calledElement` process id is not deployed (unknown definition), or the
    /// call-activity chain exceeded the recursion-depth cap. This is a
    /// missing-definition / execution problem, not a FEEL/type failure, so it
    /// maps to the C8 `CALLED_ELEMENT_ERROR` `errorType` (distinct from the
    /// `EXTRACT_VALUE_ERROR` a failed `calledElement` *expression* raises).
    /// Recoverable once the callee is deployed (or the recursion fixed) and the
    /// incident is resolved.
    CalledElementError,
    /// A `zeebe:ioMapping` source expression failed to evaluate — a FEEL parse
    /// error, a type error, or an operation on a missing value (e.g.
    /// `"x" + missingVar`). The element halts with the target variable unset
    /// rather than proceeding with a silent blank, matching Zeebe's
    /// `IO_MAPPING_ERROR`. This is the **single** taxonomy for every ioMapping
    /// failure — input (on activation) and output (on completion), on every
    /// element type (mainstream leaf activities, sub-processes, multi-instance
    /// bodies/children, ad-hoc tools and call activities).
    ///
    /// Recovery is **phase-driven**, not kind-driven: the incident carries an
    /// [`IoMappingRedrive`] descriptor recording the exact lifecycle phase to
    /// replay on resolution (`ACTIVATING` re-applies inputs, `COMPLETING`
    /// re-applies outputs), matching Zeebe's `BpmnVariableMappingBehavior`, which
    /// re-drives ioMapping failures uniformly by lifecycle phase. Recoverable
    /// once the mapping (or the missing variable it reads) is fixed and the
    /// incident is resolved.
    ///
    /// The `serde(alias = "IoMappingOutput")` keeps JSON journals written by the
    /// intermediate `IoMappingOutput` taxonomy (#939, never shipped in a release)
    /// deserializable so replay never fails to boot — mirroring the legacy
    /// numeric-code-7 → `IoMapping` mapping the read-model already carries. Such a
    /// legacy entry carries no `redrive`, so it replays via the activation phase
    /// (`redrive: None`); a still-open legacy *output* incident is the only case
    /// that differs, and can only exist in an intermediate-version dev journal.
    #[cfg_attr(feature = "serde", serde(alias = "IoMappingOutput"))]
    IoMapping,
    /// A value-extraction expression failed: a service-task / listener job
    /// `type` expression that does not evaluate to a non-empty string within
    /// [`crate::MAX_JOB_TYPE_LENGTH`] (Zeebe `EXTRACT_VALUE_ERROR`,
    /// `BpmnJobBehavior.evalTypeExp`). No job is created; the element parks on
    /// the incident, and resolving it (after fixing the variables the
    /// expression reads) retries the activation. Additive brand-new variant —
    /// old journals never carry it (#1390).
    ExtractValueError,
    /// An instance's off-heap variable payload was **definitively lost**: it was
    /// quarantined as an off-heap orphan (#1383/#1384) because its spill/cold
    /// row was absent from a readable store (e.g. state carried over from a
    /// pre-#1331 snapshot whose spill store was wiped). The instance keeps only
    /// its surviving resident variables; the spilled payload is unrecoverable.
    /// Raised by the host's storage layer (not by any BPMN element behaviour) via
    /// [`crate::Command::QuarantineOffheapOrphan`], so it is durable and
    /// replicated like any other incident. Not operator-resolvable into a retry —
    /// the data is gone — so it carries no `redrive` and no `job_key`. Additive
    /// brand-new variant — old journals never carry it (#1384).
    VariablesLost,
}

/// The lifecycle re-drive an incident replays when it is resolved. Carried by
/// every [`IncidentKind::IoMapping`] incident, and also by the ad-hoc
/// call-activity tool recovery paths (#1176) that park on an
/// [`IncidentKind::ExpressionEvaluation`] or [`IncidentKind::CalledElementError`]
/// incident. Recovery is **phase-driven** — the re-drive is chosen from the
/// element's lifecycle phase (activation vs completion) recorded here, not from
/// the incident *kind* — so a single `IoMapping` taxonomy can cover every
/// ioMapping failure (uniform `IO_MAPPING_ERROR`) while each specialized path
/// (multi-instance child/body, ad-hoc tool, call activity) still replays the
/// correct context-preserving step. Mirrors Zeebe's uniform re-drive of
/// ioMapping failures by lifecycle phase in `BpmnVariableMappingBehavior`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IoMappingRedrive {
    /// Mainstream **input**-mapping failure: re-run the element's activation body
    /// (re-apply inputs, then re-enact its behaviour) without re-emitting its
    /// `ElementActivating`/`ElementActivated` (engine `Step::RetryActivation`).
    Activation,
    /// Like [`Self::Activation`], but the failure happened BEFORE the element
    /// reached `ACTIVATED` (an input mapping or job-property expression failing
    /// in a fresh activation): the token is parked in `ACTIVATING`, as Zeebe's
    /// `onActivate` raises the incident before `transitionToActivated`. The
    /// redrive re-runs the activation body and emits `ElementActivated` only
    /// once it succeeds (a repeat failure stays in `ACTIVATING`). Additive
    /// brand-new variant — old journals (whose activation incidents always
    /// followed an `ElementActivated`) carry [`Self::Activation`].
    ActivationPending,
    /// **Output**-mapping failure that re-projects on completion: the mainstream
    /// leaf activity, a sub-process, a multi-instance child, an ad-hoc tool —
    /// anything parked in COMPLETING whose output re-projects via the element's
    /// normal completion (engine `Step::Complete`).
    Completion,
    /// A multi-instance **child** input-mapping failure: re-apply the child's
    /// inputs against its already-bound `inputElement`/`loopCounter` scope and
    /// re-enact its behaviour for the same child instance. Since #1724 a child
    /// whose first activation failed is parked in ACTIVATING (its
    /// `ElementActivated` withheld); the successful re-drive emits the marker
    /// once. Pre-#1724 journals activated the child first and owe no marker.
    MiChildActivation { body_key: Key, index: usize },
    /// A multi-instance **body** input-collection / body-input failure: re-run the
    /// body's fan-out (re-apply body input mappings, re-evaluate the input
    /// collection, spawn the children) for the same already-activated body.
    MiBodyActivation,
    /// A multi-instance **body** output-aggregation failure: re-run the body's
    /// completion (aggregate + re-apply the activity's output mappings).
    MiBodyCompletion,
    /// An ad-hoc **tool** input-mapping failure: re-activate the tool child with
    /// its original seed `variables` (no tool child was created on the failed
    /// pass, so re-activation is a clean retry).
    AdHocToolActivation {
        element_id: ElementId,
        variables: BTreeMap<String, Value>,
    },
    /// A call-activity **input**-mapping failure: re-attempt spawning the child
    /// process for the already-activated call activity (its boundary events were
    /// armed on the first pass and must not be re-armed).
    CallActivitySpawn,
    /// An ad-hoc **call-activity tool** spawn failure (#1176): identical recovery
    /// to [`Self::CallActivitySpawn`] (re-attempt the still-activated tool's
    /// child-process spawn), but carries the tool's single-pass *input projection*
    /// (`child_seed`) captured on the failed first pass so the post-resolve
    /// respawn reuses it **verbatim** instead of re-evaluating the tool's
    /// (possibly chained, non-idempotent) input mappings against the
    /// already-mutated child scope. On the first pass the tool's input mappings
    /// were already folded into the tool child's local scope (via
    /// `AdHocToolActivated`'s `local_variables`), so re-projecting them at respawn
    /// time reads a view that already contains the first pass's applied targets —
    /// `eval_io_mappings_in` is single-pass, so a chained mapping (`x -> y`,
    /// `y -> z`) is not idempotent across the retry. Mirrors the "project once,
    /// reuse verbatim" pattern PR #1171 introduced for the output side. Additive
    /// brand-new variant — old journals never carry it.
    AdHocCallActivitySpawn { child_seed: BTreeMap<String, Value> },
    /// An ad-hoc **call-activity tool** output-collection *type* incident (#1176):
    /// the tool's completion parked because the container `outputCollection`
    /// target is not a list (Zeebe `EXTRACT_VALUE_ERROR`). Carries the tool's
    /// single-pass *output projection* (`precomputed_output`, already evaluated
    /// once against the child process's produced variables by
    /// `complete_adhoc_call_activity_tool`) so the post-resolve redrive reuses it
    /// **verbatim** instead of re-evaluating the tool's (possibly chained) output
    /// mappings against the seeded child scope — the double-eval PR #1171 fixed on
    /// the clean path by threading `precomputed_output` through
    /// `continue_adhoc_inner_flow`. Additive brand-new variant.
    AdHocToolOutputCollection {
        precomputed_output: BTreeMap<String, Value>,
    },
    /// An ad-hoc container's completion was deferred because its
    /// `outputCollection` binding is missing from the container scope (a
    /// migration renamed the collection, #1630 review). The incident sits on
    /// the CONTAINER; resolution re-drives the container completion
    /// (engine `Step::CompleteAdHoc`) with the same `cancel` flag, once the
    /// operator has repaired the binding. Additive brand-new variant.
    AdHocContainerCompletion { cancel: bool },
    /// A job-worker element's job properties (`zeebe:taskDefinition` type /
    /// retries) failed to evaluate when its job was to be created (#1390, Zeebe
    /// `BpmnJobBehavior.evaluateJobExpressions` -> `EXTRACT_VALUE_ERROR`). The
    /// element instance stays activated with no job; resolution re-attempts the
    /// job creation (engine `Step::CreateJob`) against the then-current
    /// variables. Additive brand-new variant — old journals never carry it.
    JobCreation,
    /// An ad-hoc (agent) container's agent-job `type` / `retries` expression
    /// failed when the job for the next agent turn was to be minted (#1398
    /// review). The container stays activated with its collected results,
    /// active tools and iteration state intact; resolution re-mints ONLY the
    /// agent job (engine `Step::MintAdHocAgentJob`) — re-running the
    /// container's activation would reset its `outputCollection` and
    /// `AdHocState`. Covers nested containers (the job type is resolved off the
    /// parent catalog). Additive brand-new variant — old journals never carry it.
    AdHocAgentJob,
    /// An **execution listener**'s job `type` / `retries` expression failed
    /// when listener `listener_index` of the element's `event_type` chain was
    /// to be minted (#1398 review). Resolution RESTARTS the chain at listener 0
    /// (Zeebe parity, #1406 finding 7): a `start` chain re-runs the activation
    /// (input mappings re-applied), an `end` chain re-mints listener 0 without
    /// re-running the completion-time work. `listener_index` is kept as the
    /// diagnostic record of which listener failed. Additive brand-new variant
    /// — old journals never carry it.
    ExecutionListenerJob {
        event_type: crate::model::ListenerEventType,
        listener_index: usize,
        scope: Key,
    },
    /// A terminate end event's `end` execution-listener chain parked on an
    /// incident (#985, #1430 review) — a listener job-type `EXTRACT_VALUE_ERROR`
    /// or an output-mapping `IO_MAPPING_ERROR`. Resolution re-drives the
    /// terminate end's completion: output mappings are re-projected and the
    /// chain restarts at listener 0 (Zeebe parity, #1406 finding 7). The marker
    /// also suppresses a duplicate `ElementCompleting` on the re-drive.
    /// `outputs_applied` is historical (it recorded whether the projection had
    /// already run, back when the re-drive skipped it); it is still written —
    /// always `false` now — and decoded so the persisted shape is unchanged, but
    /// is no longer consulted. Additive brand-new variant — old journals never
    /// carry it.
    TerminateEndCompletion { outputs_applied: bool },
    /// A multi-instance **child**'s job properties (type / retries) failed to
    /// evaluate after its per-child input mappings were already applied (#1390,
    /// #1399 review): resolution re-runs only the child's behaviour against its
    /// existing scope — it must **not** re-apply the (possibly non-idempotent,
    /// e.g. `=item + 1 -> item`) input mappings that already succeeded. Additive
    /// brand-new variant — old journals never carry it.
    MiChildBehaviour { body_key: Key, index: usize },
    /// A call-activity **output**-mapping failure: re-project the captured child
    /// result through the call activity's output mappings and complete it. The
    /// child variables are captured here because the completed child instance
    /// that produced them is gone by resolution time.
    CallActivityCompletion {
        child_variables: BTreeMap<String, Value>,
    },
    /// An ad-hoc **tool** job-type failure (#1390, `EXTRACT_VALUE_ERROR`): the
    /// tool child was never created, so resolution re-activates the tool with
    /// its original seed `variables` — the same clean-retry shape as
    /// [`Self::AdHocToolActivation`]. Additive brand-new variant.
    AdHocToolJobType {
        element_id: ElementId,
        variables: BTreeMap<String, Value>,
    },
    /// A task-listener job's `type` / `retries` expression failed (#1398): the
    /// user task rests with its deferred transition and no listener job; the
    /// incident sits on the user task's element instance. Resolution re-mints
    /// the job of listener `listener_index` of the `event_type` chain on task
    /// `user_task_key` (Zeebe raises the task-listener job's
    /// `EXTRACT_VALUE_ERROR` and retries the job creation on resolution).
    TaskListenerJob {
        user_task_key: Key,
        event_type: crate::model::TaskListenerEventType,
        listener_index: usize,
    },
    /// An activation-time **behaviour** expression failed AFTER the element's
    /// input mappings and `start` execution listeners already succeeded (a job
    /// type/retries, user-task assignee/candidate/date, boundary or catch-event
    /// trigger expression, ad-hoc active-elements collection — #1398 review):
    /// resolution re-runs ONLY the activation body (the behaviour), never the
    /// already-applied input mappings (which may be non-idempotent, e.g.
    /// `=counter + 1 -> counter`) or the already-run start listeners (Zeebe
    /// retries the failed behaviour, not the whole activation). Distinct from
    /// [`Self::Activation`], which re-applies inputs because THEY failed.
    /// Additive brand-new variant — old journals never carry it.
    ActivationBody,
    /// Like [`Self::ActivationBody`], but the failure happened BEFORE the
    /// element reached `ACTIVATED` (a job-property expression failing in a fresh
    /// activation, Zeebe `JobWorkerTaskProcessor.onActivate` failing before
    /// `transitionToActivated`): the successful body re-run emits
    /// `ElementActivated` first. Distinct from [`Self::ActivationPending`],
    /// which re-applies the input mappings (they are what failed there); here
    /// the inputs already succeeded, so only the behaviour re-runs. Additive
    /// brand-new variant — old journals never carry it.
    ActivationBodyPending,
    /// A **timer boundary event**'s expression failed while its host's other
    /// subscriptions were being armed (#1398 review): the host's job and any
    /// already-armed boundaries stay live, so a full activation re-drive would
    /// duplicate the host job and re-arm unrelated boundaries. Resolution
    /// resumes arming AT this boundary — it and every sibling after it in the
    /// deterministic arming order (`Engine::armable_boundaries`), which were
    /// left un-armed — re-evaluating their expressions against
    /// the host's scope with `base` as the scheduling base (the host's flow
    /// scope — the same view `Engine::arm_boundary_events` evaluates against).
    /// Additive brand-new variant — old journals never carry it.
    BoundaryTimer {
        /// Id of the boundary event whose timer is re-armed on resolution.
        boundary_element_id: ElementId,
        /// Scope the boundary's expression evaluates against (the host's flow
        /// scope), captured so the retry sees the same view arming did.
        base: Key,
    },
    /// A **repeating timer cycle**'s FEEL expression failed while the fired
    /// timer was being re-armed for its next interval (#1596 review): a
    /// non-interrupting boundary or non-interrupting event sub-process timer
    /// whose cycle no longer evaluates. Resolution retries ONLY that re-arm —
    /// never the host's activation, which would re-arm the cycle with the
    /// definition's full `Rn` repetition count and let a bounded cycle fire
    /// more than `n` times. Carries the fired timer's `kind`, its `due_at`
    /// (`previous_due_at`, the base the next interval is scheduled from) and its
    /// pre-fire `remaining` repetition count, so the re-armed timer resumes the
    /// partially consumed cycle exactly. Additive brand-new variant — old
    /// journals never carry it.
    TimerCycleRearm {
        kind: TimerKind,
        previous_due_at: u64,
        remaining: Option<u32>,
    },
    /// A multi-instance **child**'s completion failed (its `outputElement` or
    /// the body's `completionCondition` raised — #1398 review): resolution
    /// re-drives the child's completion. `output_overlay` preserves the
    /// already-projected outputs of a CALL-ACTIVITY child (the callee's
    /// propagated variables plus the activity's applied `zeebe:output` mappings)
    /// — the callee process instance that produced them is gone by resolution
    /// time, so without them the retry would collect `null` or fail again.
    /// `None` for a service-task / sub-process child, whose outputs still live
    /// in its own scope. Additive brand-new variant — old journals never
    /// carry it.
    MiChildCompletion {
        body_key: Key,
        output_overlay: Option<BTreeMap<String, Value>>,
    },
}

impl IoMappingRedrive {
    /// Whether this redrive parks its element instance in ACTIVATING — the
    /// failure was raised before `ElementActivated`, so the token is owed that
    /// marker once the resolved re-drive succeeds (#1429). A legacy snapshot
    /// (written before the `activating` set existed) carries this owed state
    /// only implicitly, in its active incidents; restore reconstructs the set
    /// from exactly these redrives.
    pub fn parks_in_activating(&self) -> bool {
        matches!(
            self,
            IoMappingRedrive::ActivationPending | IoMappingRedrive::ActivationBodyPending
        )
    }

    /// Whether resolving this redrive re-runs the element's **activation body**
    /// (the step that, for a process-level start event, arms the root's event
    /// sub-process triggers — `Engine::try_activation_body`). Migration's
    /// root-steadiness guard keys on this: only an activation-phase redrive
    /// (or a pending start-listener job) means the start event's body will run
    /// again, so arming now would be repeated. A completion-phase redrive
    /// (`Completion`, `TerminateEndCompletion`, an `End`-listener chain, the
    /// MI/call-activity completions) resumes completion instead — the start
    /// event's body already ran, so the root IS steady and may be armed
    /// (#1379 review).
    pub(crate) fn redrives_activation(&self) -> bool {
        match self {
            IoMappingRedrive::Activation
            | IoMappingRedrive::ActivationPending
            | IoMappingRedrive::ActivationBody
            | IoMappingRedrive::ActivationBodyPending => true,
            // A `start`-listener chain restarts the activation (`RetryActivation`);
            // an `end` chain resumes completion.
            IoMappingRedrive::ExecutionListenerJob { event_type, .. } => {
                *event_type == crate::model::ListenerEventType::Start
            }
            _ => false,
        }
    }
}

/// Lifecycle state of an incident. Incidents are retained after resolution (as
/// `Resolved`) so they remain queryable as an audit trail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IncidentState {
    /// Raised and parking a token; awaiting resolution.
    Active,
    /// Resolved: the failed work was retried. The record is kept for history.
    Resolved,
}

/// A raised incident: a token parked because something went wrong (a job
/// exhausted its retries, an exclusive gateway matched no flow, or a thrown
/// business error went uncaught). Incidents are resolved with
/// [`crate::Command::ResolveIncident`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Incident {
    pub key: Key,
    pub instance_key: Key,
    /// The parked element instance the incident sits on.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// What went wrong.
    pub kind: IncidentKind,
    /// The lifecycle phase / projection to replay on resolution (phase-driven
    /// recovery — see [`IoMappingRedrive`]). Present for every
    /// [`IncidentKind::IoMapping`] incident, and also for the ad-hoc
    /// call-activity tool recovery paths (#1176) that park on an
    /// [`IncidentKind::ExpressionEvaluation`] (output-collection type) or
    /// [`IncidentKind::CalledElementError`] (child-spawn) incident while carrying
    /// a preserved single-pass projection to reuse verbatim on redrive. `None`
    /// for every other incident (whose recovery is derived from the kind).
    #[cfg_attr(feature = "serde", serde(default))]
    pub redrive: Option<IoMappingRedrive>,
    /// Human-readable explanation of why the incident was raised.
    pub reason: String,
    /// The job whose retry exhaustion caused this incident, if any. Only
    /// job-incidents (`Some`) can be recovered by updating retries and
    /// resolving; gateway/uncaught-error incidents carry `None`.
    pub job_key: Option<Key>,
    /// The logical instant at which the incident was raised, in the same units
    /// the host feeds the engine as `now` (Unix epoch milliseconds on the
    /// server). Sourced from the command's clock, recorded on the event, and so
    /// preserved exactly on replay.
    pub created_at: u64,
    /// Lifecycle state. `Active` while parking a token; `Resolved` once the
    /// failed work has been retried (the record is retained for audit).
    pub state: IncidentState,
    /// The logical instant at which the incident was resolved, if it has been.
    pub resolved_at: Option<u64>,
    /// A caller-supplied reference recorded against the resolution for
    /// traceability (the REST `operationReference`), if any.
    pub operation_reference: Option<i64>,
}

impl Incident {
    /// The element instance this incident leaves owed an `ElementActivated` at its
    /// listener-chain drain (#1429), if it does. Some only while the incident is
    /// [`IncidentState::Active`] and its redrive parks the token in ACTIVATING
    /// ([`IoMappingRedrive::parks_in_activating`]) — the single predicate behind
    /// [`reconstruct_owed_activations`], so a legacy snapshot's restore and a
    /// per-instance rehydrate can never drift on what counts as an owed marker.
    pub(crate) fn owed_activation(&self) -> Option<Key> {
        if self.state == IncidentState::Active
            && self
                .redrive
                .as_ref()
                .is_some_and(IoMappingRedrive::parks_in_activating)
        {
            Some(self.element_instance_key)
        } else {
            None
        }
    }
}

/// Lifecycle state of a timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimerState {
    /// Armed and waiting: it fires once a clock tick finds it due.
    Created,
    /// Fired: its token has been released. Retained so it is not re-fired.
    Triggered,
    /// Cancelled before firing because the element it guarded left the flow
    /// first (e.g. a boundary timer whose activity completed, or a sibling
    /// boundary timer when another boundary on the same activity fired).
    /// Retained for audit; never fires.
    Canceled,
}

/// What a timer guards, which decides what firing it does.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimerKind {
    /// A timer intermediate catch event: the timer's `element_instance_key` is
    /// the catch event itself, and firing completes it, resuming the token along
    /// the event's outgoing flow.
    IntermediateCatch,
    /// An interrupting timer boundary event attached to an activity: the timer's
    /// `element_instance_key`/`element_id` are the *attached activity*, and
    /// firing cancels the activity (and any job parked on it) and takes the
    /// boundary event's outgoing flow.
    InterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when the timer fires.
        boundary_element_id: ElementId,
    },
    /// A non-interrupting timer boundary event attached to an activity: like
    /// [`InterruptingBoundary`], but firing leaves the activity (and its job)
    /// running and merely spawns a parallel token along the boundary event's
    /// outgoing flow.
    ///
    /// [`InterruptingBoundary`]: TimerKind::InterruptingBoundary
    NonInterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when the timer fires.
        boundary_element_id: ElementId,
    },
    /// The timer start event of an **event sub-process** (#1555), armed when
    /// its enclosing scope activates. The timer's `element_instance_key` is the
    /// enclosing scope (the embedded sub-process instance, or `0` for the
    /// process root) and its `element_id` the event sub-process's start event.
    /// Firing activates the event sub-process in that scope — tearing the
    /// scope's other children down first when `interrupting`. A non-interrupting
    /// cycle re-arms after each fire.
    EventSubProcess {
        /// Id of the event sub-process element to activate.
        esp_element_id: ElementId,
        /// Whether firing interrupts the enclosing scope.
        interrupting: bool,
    },
}

/// An armed timer holding a token on a timer intermediate catch event until its
/// `due_at` instant passes. A clock tick ([`crate::Command::TriggerTimers`])
/// fires every due timer, releasing its token along the event's outgoing flow.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Timer {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate timer, or the attached activity for a boundary
    /// timer.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The logical instant at which the timer becomes due, in the host's clock
    /// units (Unix epoch milliseconds on the server). Carried on the
    /// [`crate::Event::TimerCreated`] event, so replay reconstructs it exactly.
    pub due_at: u64,
    pub state: TimerState,
    /// What the timer guards, and so what firing it does.
    pub kind: TimerKind,
    /// Remaining fires of a bounded cycle (`Rn/…`) including the next one
    /// (#1596); `None` when unbounded or not a cycle.
    #[cfg_attr(feature = "serde", serde(default))]
    pub repetitions: Option<u32>,
}

/// Lifecycle state of a message subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MessageSubscriptionState {
    /// Open and waiting: a matching correlated message releases its token.
    Open,
    /// Pending placement: the catch element's token is parked on the **instance**
    /// partition, but the canonical subscription lives on a *different* partition
    /// (`hash(correlation_key)`). This record is the instance partition's local
    /// view, awaiting a [`crate::Command::CorrelateMessageSubscription`]
    /// continuation routed back from the message partition. Only ever produced
    /// when `num_partitions > 1` and the correlation key hashes off-partition; a
    /// single-partition host never opens an `Opening` subscription.
    Opening,
    /// Correlated: a matching message arrived and released its token. Retained
    /// so it is not correlated twice.
    Correlated,
    /// Cancelled before correlation because the element it guarded left the flow
    /// first (e.g. a boundary subscription whose activity completed, or a sibling
    /// boundary subscription when another boundary on the same activity fired).
    /// Retained for audit; never correlates.
    Canceled,
}

/// What a message subscription guards, which decides what correlating it does.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MessageSubscriptionKind {
    /// A message intermediate catch event: the subscription's
    /// `element_instance_key` is the catch event itself, and correlating
    /// completes it, resuming the token along the event's outgoing flow.
    IntermediateCatch,
    /// An interrupting message boundary event attached to an activity: the
    /// subscription's `element_instance_key`/`element_id` are the *attached
    /// activity*, and correlating cancels the activity (and any job parked on it)
    /// and takes the boundary event's outgoing flow.
    InterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when a message is
        /// correlated.
        boundary_element_id: ElementId,
    },
    /// A non-interrupting message boundary event attached to an activity: like
    /// [`InterruptingBoundary`], but correlating leaves the activity (and its
    /// job) running and merely spawns a parallel token along the boundary event's
    /// outgoing flow. The subscription stays open, so every matching message
    /// spawns another token.
    ///
    /// [`InterruptingBoundary`]: MessageSubscriptionKind::InterruptingBoundary
    NonInterruptingBoundary {
        /// Id of the boundary event whose outgoing flow runs when a message is
        /// correlated.
        boundary_element_id: ElementId,
    },
    /// The message / signal / conditional start event of an **event
    /// sub-process** (#1555), armed when its enclosing scope activates. The
    /// subscription's `element_instance_key` is the enclosing scope (the
    /// embedded sub-process instance, or `0` for the process root) and its
    /// `element_id` the event sub-process's start event. Triggering activates
    /// the event sub-process in that scope (interrupting the scope's other
    /// children first when `interrupting`); a non-interrupting subscription
    /// stays open so every trigger spawns another event sub-process instance.
    EventSubProcess {
        /// Id of the event sub-process element to activate.
        esp_element_id: ElementId,
        /// Whether triggering interrupts the enclosing scope.
        interrupting: bool,
    },
}

impl MessageSubscriptionKind {
    /// Whether a subscription of this kind stays open after it triggers (a
    /// non-interrupting boundary or a non-interrupting event sub-process start
    /// fires repeatedly); every other kind settles on its first trigger. The
    /// single source of truth for the message / signal / conditional appliers.
    pub fn stays_open_after_trigger(&self) -> bool {
        matches!(
            self,
            MessageSubscriptionKind::NonInterruptingBoundary { .. }
                | MessageSubscriptionKind::EventSubProcess {
                    interrupting: false,
                    ..
                }
        )
    }
}

/// An open message subscription holding a token on a message catch element until
/// a matching message is correlated. A [`crate::Command::CorrelateMessage`] whose
/// name and correlation key match an open subscription releases its token (an
/// intermediate catch) or interrupts its activity (an interrupting boundary).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MessageSubscription {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate subscription, or the attached activity for a
    /// boundary subscription.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The BPMN message name this subscription waits for.
    pub message_name: String,
    /// The resolved correlation value: the stringified value of the instance
    /// variable the catch element correlates on, captured when the subscription
    /// opened. A message correlates only when its `correlation_key` equals this.
    pub correlation_key: String,
    pub state: MessageSubscriptionState,
    /// What the subscription guards, and so what correlating it does.
    pub kind: MessageSubscriptionKind,
    /// The `message_key`s already correlated to this subscription — recorded
    /// only for a kind that [`stays_open_after_trigger`](MessageSubscriptionKind::stays_open_after_trigger)
    /// (a one-shot kind settles on its first correlation, which already makes a
    /// redelivered continuation a no-op). A routed
    /// [`crate::Command::CorrelateMessageSubscription`] is delivered
    /// at-least-once, so a persistent subscription must recognise a
    /// redelivered `message_key` and ignore it, while still accepting every
    /// *distinct* message. Bounded by the distinct messages the subscription
    /// actually correlated, and dropped with the subscription. Additive
    /// (`serde(default)`, omitted when empty), so older snapshots decode
    /// unchanged and one-shot subscriptions serialize byte-identically.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")
    )]
    pub correlated_message_keys: std::collections::BTreeSet<Key>,
    /// The buffered delivery this (message-partition, canonical) subscription
    /// sent to a REMOTE instance and is awaiting the instance partition's
    /// acknowledgement for (#1949, Zeebe `CORRELATING`): the in-flight
    /// delivery and its reservation, event-sourced so they survive routing
    /// failures, leader changes and restarts (the host re-drives it from
    /// [`crate::Engine::pending_buffered_deliveries`]). While set, the
    /// subscription takes nothing more and the reserved message never
    /// expires; a [`crate::Event::MessageCorrelationAcknowledged`] settles
    /// it. Additive (`serde(default)`, omitted when `None`).
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub awaiting_ack: Option<AwaitingAck>,
    /// The earliest buffered entry `(seq, message_key)` whose reservation was
    /// released (#1949) while this subscription could not take it (it was
    /// awaiting its own ack): its next scan must start no later than this,
    /// so the released message is not skipped by the resume-after-consumed
    /// cursor. Cleared once that scan ran. Additive, omitted when `None`.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub rescan_from: Option<(u64, Key)>,
}

/// An in-flight buffered delivery awaiting its acknowledgement (#1949): see
/// [`MessageSubscription::awaiting_ack`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AwaitingAck {
    /// The reserved buffered message.
    pub message_key: Key,
    /// The routed delivery's stable key: a re-driven send reuses it, so the
    /// instance partition recognises the duplicate and only re-acks.
    pub delivery_key: Key,
}

/// An open **signal** subscription holding a token on a signal catch element (or
/// guarding an activity via a signal boundary) until a matching signal is
/// broadcast. Unlike a [`MessageSubscription`], a signal correlates by **name
/// only** (there is no correlation key): a [`crate::Command::BroadcastSignal`]
/// whose `signal_name` matches an open subscription releases its token (an
/// intermediate catch) or interrupts its activity (an interrupting boundary).
/// A broadcast fans out to **every** matching open subscription across all
/// instances. The `kind` reuses [`MessageSubscriptionKind`] (the token-advance
/// semantics are identical to messages).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SignalSubscription {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate subscription, or the attached activity for a
    /// boundary subscription.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The BPMN signal name this subscription waits for.
    pub signal_name: String,
    pub state: MessageSubscriptionState,
    /// What the subscription guards, and so what correlating it does. Reuses the
    /// message subscription kind — the outcomes are identical.
    pub kind: MessageSubscriptionKind,
}

/// An open **conditional** subscription holding a token on a conditional
/// intermediate catch event (or guarding an activity via a conditional boundary)
/// until its FEEL `condition` becomes `true`. Unlike message/signal
/// subscriptions there is no external trigger: the engine evaluates `condition`
/// against the instance variables when the subscription opens and again whenever
/// one of `referenced_vars` changes, firing when it yields `true`. An
/// interrupting subscription (intermediate catch or interrupting boundary) fires
/// once; a non-interrupting boundary stays open and can fire repeatedly. The
/// `kind` reuses [`MessageSubscriptionKind`] (the token-advance semantics are
/// identical to messages/signals).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConditionalSubscription {
    pub key: Key,
    pub instance_key: Key,
    /// The element instance the token rests on while waiting: the catch event
    /// itself for an intermediate subscription, or the attached activity for a
    /// boundary subscription.
    pub element_instance_key: Key,
    pub element_id: ElementId,
    /// The FEEL condition (as authored, `=`-marker included) evaluated to decide
    /// whether the event fires.
    pub condition: String,
    /// The root variable names `condition` references, sorted. The engine
    /// re-evaluates the condition only when a variable in this set changes; an
    /// empty set means the condition depends on no variable (evaluated on open
    /// only).
    pub referenced_vars: Vec<String>,
    pub state: MessageSubscriptionState,
    /// What the subscription guards, and so what firing it does.
    pub kind: MessageSubscriptionKind,
}

/// A process-level subscription on a **message start event**: a correlating
/// message whose name matches creates a new instance of `process_id` (starting
/// at `start_element_id`). Unlike a [`MessageSubscription`] it is not bound to an
/// instance and never settles — it stays open for the life of the deployment,
/// minting a new instance on every matching message. Keyed in [`State`] by
/// message name; re-deploying a process with the same start message replaces it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MessageStartSubscription {
    /// The definition (and version) whose instances this start event creates.
    pub process_definition_key: Key,
    pub process_id: String,
    /// The BPMN message name that triggers a new instance.
    pub message_name: String,
    /// The start event element a new instance begins at.
    pub start_element_id: ElementId,
    /// The MCP tool name declared on the start event via the
    /// `io.camunda.tool:name` zeebe:property (Camunda 8.10 MCP gateway, #1499),
    /// when present. Additive: `None` for subscriptions serialized before this
    /// field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tool_name: Option<String>,
    /// The start event's `zeebe:properties` subset whose keys carry the
    /// `io.camunda.tool:` prefix, retained so API surfaces (the
    /// `/mcp/processes` tool list, the REST `toolProperties` field) can read
    /// the tool description / input schema without re-parsing the BPMN XML.
    /// Empty when the start event declares no tool properties and for
    /// subscriptions serialized before this field existed.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tool_properties: std::collections::BTreeMap<String, String>,
}

/// A process-level **timer start event**: an armed timer that creates a new
/// instance of `process_id` when it becomes due. A `repeating` timer (a BPMN
/// cycle) re-arms for another `interval_millis` after each fire; a one-shot (a
/// BPMN duration) fires exactly once and is then retained as `due_at = None`.
/// Keyed in [`State`] by its own `timer_key`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StartTimer {
    pub timer_key: Key,
    /// The definition (and version) whose instances this start event creates.
    pub process_definition_key: Key,
    pub process_id: String,
    /// The start event element a new instance begins at.
    pub start_element_id: ElementId,
    /// The instant the timer is next due, or `None` once a one-shot has fired.
    pub due_at: Option<u64>,
    /// The period between fires (and the initial delay), in the host's clock units.
    pub interval_millis: u64,
    /// Whether the timer re-arms after firing (a cycle) or fires once.
    pub repeating: bool,
    /// Remaining fires of a bounded cycle (`Rn/…`) including the next one
    /// (#1596); `None` when unbounded or not a cycle.
    #[cfg_attr(feature = "serde", serde(default))]
    pub repetitions: Option<u32>,
}

/// A deployed process definition together with the identity the engine assigned
/// it at deploy time.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedProcess {
    /// Unique key for this specific process definition (and version).
    pub key: Key,
    /// Version number, incremented per process id across deployments (starts 1).
    pub version: i32,
    /// The deployment this definition was registered under (Zeebe
    /// `deploymentKey`): the membership handle a `deployment`-bound form or
    /// linked resource resolves against (#1193). `0` for a definition restored
    /// from a pre-#1193 snapshot (or seeded from one), whose deployment
    /// membership is unknown — such a definition matches no `deployment`
    /// binding and surfaces a resolvable incident instead of a mis-binding.
    #[cfg_attr(feature = "serde", serde(default))]
    pub deployment_key: Key,
    /// The static, executable definition.
    pub definition: ProcessDefinition,
}

/// How a leased job was settled — the outcome half of the idempotency key, so a
/// replay is recognized only when it re-asserts the *same* outcome (#1181).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum JobSettlementOutcome {
    Completed,
    Failed,
    ErrorThrown,
}

/// One settlement-ledger entry ([`State::settled_jobs`]): the outcome half of
/// the idempotency key, so a replay is recognized only when it re-asserts the
/// *same* outcome under the *same* lease token (#1181).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JobSettlement {
    /// The lease token of the activation that settled the job.
    pub lease_token: String,
    pub outcome: JobSettlementOutcome,
}

/// Upper bound on [`State::settled_jobs`]. Beyond it the entries with the
/// smallest job keys (the oldest jobs) are dropped; a replay that old degrades
/// to the pre-#1181 answer (409/404), never to a double settlement.
pub const SETTLED_JOB_LEDGER_CAP: usize = 65_536;

/// Upper bound on the lease tokens retained **per job** in
/// [`State::settled_jobs`] (#1181). Repeated fail/reactivate cycles append one
/// token per activation; beyond this the oldest token of that job is dropped,
/// so the ledger's total size is bounded by
/// `SETTLED_JOB_LEDGER_CAP * SETTLED_TOKENS_PER_JOB_CAP` entries. A replay of a
/// dropped token on a still-live job remains fenced by [`Job::settled_tokens`].
pub const SETTLED_TOKENS_PER_JOB_CAP: usize = 8;

/// Upper bound on [`State::routed_deliveries`] (#1497). Beyond it the entries
/// this partition **applied earliest** are dropped (FIFO in the recipient's own
/// applied order — never by key value: keys sort by source partition first, so
/// key-order eviction would let a fresh delivery from a low partition evict
/// itself the moment it is recorded). A re-delivery that old degrades to the
/// pre-#1497 answer (applied again), never to a lost first delivery — the same
/// degrade-to-previous-behaviour rule as [`SETTLED_JOB_LEDGER_CAP`].
pub const ROUTED_DELIVERY_LEDGER_CAP: usize = 65_536;

/// One routed-delivery ledger entry (#1497): the source-minted `delivery_key`
/// (the dedup key) plus, for a subscription follow-up, the `subscription_key`
/// it concerned, and whether it is a **close tombstone** (a
/// `CloseMessageSubscription`) that rejects a delayed `OpenMessageSubscription`
/// for that subscription (which bears its own, different `delivery_key`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RoutedDelivery {
    pub delivery_key: Key,
    /// `Some` for a subscription open/close/correlate; `None` for a
    /// start-instance dispatch.
    #[cfg_attr(feature = "serde", serde(default))]
    pub subscription_key: Option<Key>,
    /// `true` only for a `CloseMessageSubscription` delivery.
    #[cfg_attr(feature = "serde", serde(default))]
    pub close_tombstone: bool,
    /// For an ack-paced `CorrelateMessageSubscription` (#1949): the answer
    /// this (instance) partition gave, recorded when its
    /// [`crate::Event::MessageCorrelationAcknowledging`] is applied. A
    /// duplicate (re-driven) delivery is answered with exactly this receipt,
    /// independent of the subscription's later lifetime (resume, terminal
    /// eviction). Additive, omitted when `None`.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub receipt: Option<CorrelationReceipt>,
}

/// The durable answer to one ack-paced buffered delivery (#1949): see
/// [`RoutedDelivery::receipt`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CorrelationReceipt {
    pub accepted: bool,
    pub consumed: bool,
    pub deferred: bool,
}

/// The routed-delivery ledger (#1497): dedup set + applied-order FIFO + a
/// close-tombstone index, kept consistent by [`RoutedDeliveries::record`]
/// (the only mutator). Derived solely from replicated
/// [`crate::Event::RoutedDeliveryRecorded`] records applied in log order, so
/// every replica — and every snapshot/replay — holds the identical ledger.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(
        from = "std::collections::VecDeque<RoutedDelivery>",
        into = "std::collections::VecDeque<RoutedDelivery>"
    )
)]
pub struct RoutedDeliveries {
    /// Entries in the order this partition APPLIED them (oldest first). The
    /// eviction order and the serialized source of truth.
    order: std::collections::VecDeque<RoutedDelivery>,
    /// `delivery_key` membership index (derived from `order`).
    keys: std::collections::BTreeSet<Key>,
    /// `subscription_key -> delivery_key` of the live close tombstone (derived
    /// from `order`): an O(1) lookup instead of a ledger scan per open.
    tombstones: std::collections::BTreeMap<Key, Key>,
    /// `delivery_key -> receipt` of every retained entry carrying one
    /// (derived from `order`, #1949).
    receipts: std::collections::BTreeMap<Key, CorrelationReceipt>,
}

impl RoutedDeliveries {
    /// Whether `delivery_key` was already applied (and is still retained).
    pub fn contains(&self, delivery_key: Key) -> bool {
        self.keys.contains(&delivery_key)
    }
    /// Whether a retained close tombstone exists for `subscription_key`.
    pub fn is_tombstoned(&self, subscription_key: Key) -> bool {
        self.tombstones.contains_key(&subscription_key)
    }
    pub fn len(&self) -> usize {
        self.order.len()
    }
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
    /// Retained entries, oldest-applied first.
    pub fn iter(&self) -> impl Iterator<Item = &RoutedDelivery> {
        self.order.iter()
    }
    /// The durable answer recorded for ack-paced delivery `delivery_key`
    /// (#1949), while its entry is retained.
    pub fn receipt(&self, delivery_key: Key) -> Option<CorrelationReceipt> {
        self.receipts.get(&delivery_key).copied()
    }
    /// Attach `receipt` to the retained entry for `delivery_key` (#1949).
    /// First write wins: a receipt, once recorded, is the delivery's
    /// immutable outcome. A key not (or no longer) retained is a no-op.
    pub fn set_receipt(&mut self, delivery_key: Key, receipt: CorrelationReceipt) {
        if !self.keys.contains(&delivery_key) || self.receipts.contains_key(&delivery_key) {
            return;
        }
        // The ack follows its delivery's record within the same command, so
        // the entry is (almost always) the newest: search from the back.
        if let Some(entry) = self
            .order
            .iter_mut()
            .rev()
            .find(|e| e.delivery_key == delivery_key)
        {
            entry.receipt = Some(receipt);
            self.receipts.insert(delivery_key, receipt);
        }
    }
    /// Record an applied delivery, evicting the earliest-APPLIED entries
    /// beyond `cap`. A key already present is a no-op (never re-ordered).
    pub fn record(&mut self, entry: RoutedDelivery, cap: usize) {
        if !self.keys.insert(entry.delivery_key) {
            return;
        }
        if entry.close_tombstone {
            if let Some(sub) = entry.subscription_key {
                self.tombstones.insert(sub, entry.delivery_key);
            }
        }
        if let Some(receipt) = entry.receipt {
            self.receipts.insert(entry.delivery_key, receipt);
        }
        self.order.push_back(entry);
        while self.order.len() > cap {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            self.keys.remove(&old.delivery_key);
            self.receipts.remove(&old.delivery_key);
            if let Some(sub) = old.subscription_key {
                if self.tombstones.get(&sub) == Some(&old.delivery_key) {
                    self.tombstones.remove(&sub);
                }
            }
        }
    }
}

/// Serialized form is the applied-order list only; the indexes are rebuilt on
/// load, so a snapshot can never carry an index that drifted from `order`.
impl From<std::collections::VecDeque<RoutedDelivery>> for RoutedDeliveries {
    fn from(order: std::collections::VecDeque<RoutedDelivery>) -> Self {
        let mut l = Self::default();
        for e in order {
            l.record(e, usize::MAX);
        }
        l
    }
}

impl From<RoutedDeliveries> for std::collections::VecDeque<RoutedDelivery> {
    fn from(l: RoutedDeliveries) -> Self {
        l.order
    }
}

/// Record a routed delivery in `state`'s ledger (bounded by `cap`).
pub fn record_routed_delivery(state: &mut State, entry: RoutedDelivery, cap: usize) {
    state.routed_deliveries.record(entry, cap);
}

/// Record a settlement in the ledger, evicting the oldest entries beyond `cap`.
///
/// Entries are keyed by job but **retained per lease token** (#1181): a later
/// settlement under a newer activation's token must not erase the replay key of
/// an earlier settlement (a worker that lost the response under token T1 still
/// gets its idempotent no-op after token T2 settled the next activation). The
/// replay fence for a settlement whose job is still *live* does not depend on
/// this bounded map at all — it is kept on the job itself
/// ([`Job::settled_tokens`]) — so ledger eviction can never re-admit a
/// settlement the live job already consumed. Deterministic (keyed `BTreeMap`,
/// evicts lowest keys) so every replica applying the same events holds the same
/// ledger.
pub fn record_job_settlement(state: &mut State, job_key: Key, entry: JobSettlement, cap: usize) {
    match state.settled_jobs.get_mut(&job_key) {
        Some(known) => {
            if !known.iter().any(|s| s.lease_token == entry.lease_token) {
                known.push(entry);
                if known.len() > SETTLED_TOKENS_PER_JOB_CAP {
                    let excess = known.len() - SETTLED_TOKENS_PER_JOB_CAP;
                    known.drain(..excess);
                }
            }
        }
        None => {
            state.settled_jobs.insert(job_key, vec![entry]);
        }
    }
    while state.settled_jobs.len() > cap {
        if state.settled_jobs.pop_first().is_none() {
            break;
        }
    }
}

/// A published message held in the buffer until `deadline` (#1874, Zeebe
/// `MessageState`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BufferedMessage {
    pub key: Key,
    pub message_name: String,
    pub correlation_key: String,
    /// Absolute expiry (ms): `publish time + time_to_live`. The message is
    /// correlatable only while `now < deadline`.
    pub deadline: u64,
    pub message_id: Option<String>,
    pub variables: BTreeMap<String, crate::model::Value>,
    /// Instances this message already correlated to (Zeebe
    /// `existMessageCorrelation`): never correlated to them again.
    pub correlated_instances: BTreeSet<Key>,
    /// Message-partition-local enqueue order (#1874): the position of this
    /// message's `MessageBuffered` among every message this partition ever
    /// buffered. Buffered correlation takes the OLDEST eligible message by
    /// this, never by `key` — a key pre-minted by another partition (#1434)
    /// does not sort in arrival order. Derived on apply, so replay restores it.
    #[cfg_attr(feature = "serde", serde(default))]
    pub seq: u64,
}

impl BufferedMessage {
    /// `true` while the message may still correlate at `now`.
    pub fn is_live_at(&self, now: u64) -> bool {
        now < self.deadline
    }
}

/// The complete working state of the engine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct State {
    /// Latest deployed version of each process, keyed by BPMN process id. New
    /// instances created by id (with no explicit version) start the latest
    /// version. This is a fast latest-by-id index over `process_versions`.
    pub processes: BTreeMap<String, DeployedProcess>,
    /// Every deployed process definition ever seen, keyed by its unique
    /// process-definition key (so historical versions are retained, not
    /// overwritten by a redeploy). A running instance resolves *its* definition
    /// through here by its pinned `process_definition_key`, so replaying it
    /// against a newer redeployed version is impossible (Zeebe parity: an
    /// instance runs the version it was created on). `serde(default)` so
    /// snapshots written before version retention deserialize empty and fall
    /// back to the latest-by-id index via [`State::definition_for`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub process_versions: BTreeMap<Key, DeployedProcess>,
    pub instances: BTreeMap<Key, ProcessInstance>,
    pub jobs: BTreeMap<Key, Job>,
    /// The **settlement ledger** (#1181): for each *leased* job, every recorded
    /// settlement — the lease token (per-activation fence) and the outcome it
    /// produced. A replayed `CompleteJob`/`FailJob`/`ThrowJobError` bearing a
    /// recorded token and the same outcome is answered as an idempotent no-op
    /// instead of a 409, on any replica; a recorded token asserting a
    /// *different* outcome is rejected outright (a token settles one activation
    /// exactly once). Derived only from the replicated
    /// [`crate::Event::JobSettlementRecorded`], so every replica (including one
    /// that never saw a leader-local activation) holds the same ledger. Kept
    /// apart from [`State::jobs`] so it outlives host-driven instance eviction;
    /// bounded by [`SETTLED_JOB_LEDGER_CAP`] (live jobs additionally fence
    /// themselves via [`Job::settled_tokens`], so eviction of a live job's
    /// entry never re-admits its settlement). `serde(default)`: snapshots
    /// written before #1181 load with an empty ledger.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub settled_jobs: BTreeMap<Key, Vec<JobSettlement>>,
    /// The **routed-delivery ledger** (#1497): the source-minted
    /// `delivery_key` of every routed cross-partition follow-up this partition
    /// applied (`OpenMessageSubscription` / `CorrelateMessageSubscription` /
    /// `CloseMessageSubscription` / `DispatchStartInstance`). A re-delivered
    /// command carrying an already-recorded key is acknowledged without being
    /// re-applied (at-least-once transport ⇒ exactly-once effect); a
    /// `CloseMessageSubscription` records its key even when no canonical
    /// subscription exists yet, and its entry carries the `subscription_key`
    /// so it doubles as the **close tombstone** that rejects a delayed open
    /// landing after its close. Derived only from the replicated
    /// [`crate::Event::RoutedDeliveryRecorded`], so every replica holds the
    /// same ledger; bounded by [`ROUTED_DELIVERY_LEDGER_CAP`].
    /// `serde(default)`: snapshots written before #1497 load with an empty
    /// ledger.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "RoutedDeliveries::is_empty")
    )]
    pub routed_deliveries: RoutedDeliveries,
    /// The **cluster variables** (#1657): global + per-tenant configuration
    /// values FEEL resolves beneath instance variables. Replicated engine state,
    /// derived only from the `ClusterVariableCreated` / `Updated` / `Deleted`
    /// events, so every replica and every replay reads the same values (and
    /// `SECRET_REFERENCE` kinds) at the same log position. `serde(default)`:
    /// snapshots written before #1657 load with none.
    #[cfg_attr(
        feature = "serde",
        serde(
            default,
            skip_serializing_if = "crate::cluster_vars::ClusterVariableState::is_unused"
        )
    )]
    pub cluster_variables: crate::cluster_vars::ClusterVariableState,
    /// User tasks created for `userTask` elements, keyed by user-task key. A
    /// completed task is retained (transitioned to [`UserTaskState::Completed`])
    /// as an audit trail.
    pub user_tasks: BTreeMap<Key, UserTask>,
    /// All incidents ever raised, keyed by incident key, retained after
    /// resolution as an audit trail (each carries its [`IncidentState`]).
    pub incidents: BTreeMap<Key, Incident>,
    /// Armed and fired timers, keyed by timer key. A fired timer is retained
    /// (transitioned to [`TimerState::Triggered`]) so a clock tick never fires
    /// it twice.
    pub timers: BTreeMap<Key, Timer>,
    /// Open and settled message subscriptions, keyed by subscription key. A
    /// correlated subscription is retained (transitioned to
    /// [`MessageSubscriptionState::Correlated`]) so a later message never
    /// correlates it twice.
    pub message_subscriptions: BTreeMap<Key, MessageSubscription>,
    /// Messages **buffered** with a time-to-live (#1874), keyed by message key,
    /// awaiting a subscription to open before their deadline. Derived only from
    /// [`crate::Event::MessageBuffered`] / [`crate::Event::BufferedMessageCorrelated`]
    /// / [`crate::Event::MessageExpired`]. `serde(default)`: snapshots written
    /// before #1874 load with an empty buffer.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub buffered_messages: BTreeMap<Key, BufferedMessage>,
    /// Index of buffered messages carrying a `messageId` (#1874), keyed by
    /// `(message_name, correlation_key, message_id)` → message key (Zeebe's
    /// messageId column family). Makes publish-time dedup `O(log n)` instead
    /// of a scan of the whole buffer. Derived from `buffered_messages` and
    /// maintained only by [`State::insert_buffered_message`] /
    /// [`State::remove_buffered_message`]. In-memory only: rebuilt by
    /// [`State::rebuild_buffered_message_id_index`] on snapshot restore.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub buffered_message_ids: BTreeMap<(String, String, String), Key>,
    /// Index of buffered messages by `(message_name, correlation_key)` →
    /// ordered `(seq, message key)` set (#1874, Zeebe's name + correlation
    /// key message index). A subscription's buffered candidates are read from
    /// its matching set — `O(log n + matches)` — instead of a scan of the
    /// whole buffer. Derived from `buffered_messages` and maintained only by
    /// [`State::insert_buffered_message`] / [`State::remove_buffered_message`].
    /// In-memory only: rebuilt by [`State::rebuild_buffered_message_id_index`]
    /// on snapshot restore.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub buffered_by_correlation: BTreeMap<(String, String), BTreeSet<(u64, Key)>>,
    /// Next [`BufferedMessage::seq`] to assign (#1874): bumped by every
    /// applied `MessageBuffered`, never reused (an expired or removed message
    /// leaves its slot behind), so enqueue order is total and replayable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub next_buffered_seq: u64,
    /// Whether this partition has EVER routed an ack-paced buffered delivery
    /// to a remote instance (#1949). One-way: the host's sticky
    /// `message-correlation-ack-v1` peer requirement keys on it. Additive.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "std::ops::Not::not")
    )]
    pub message_correlation_ack_used: bool,
    /// Open and settled **signal** subscriptions, keyed by subscription key. A
    /// correlated subscription is retained (transitioned to
    /// [`MessageSubscriptionState::Correlated`]) as an audit trail.
    pub signal_subscriptions: BTreeMap<Key, SignalSubscription>,
    /// Open and settled **conditional** subscriptions, keyed by subscription key.
    /// A fired interrupting subscription is retained (transitioned to
    /// [`MessageSubscriptionState::Correlated`]) so it never fires twice; a
    /// non-interrupting boundary stays [`MessageSubscriptionState::Open`].
    pub conditional_subscriptions: BTreeMap<Key, ConditionalSubscription>,
    /// Process-level message start subscriptions, keyed by message name. A
    /// correlating message whose name matches creates a new instance.
    pub message_start_subscriptions: BTreeMap<String, MessageStartSubscription>,
    /// Process-level timer start events, keyed by timer key. Each creates a new
    /// instance when due; a cycle re-arms, a one-shot is retained with
    /// `due_at = None`.
    pub start_timers: BTreeMap<Key, StartTimer>,
    /// Index of jobs eligible to be *considered* for activation, grouped by job
    /// type and ordered by `(−priority, key)` (priority negated in `i64`, so
    /// every accepted `i32` priority keeps a distinct slot) — highest priority
    /// first, then oldest (lowest key) as the age/SLA tiebreak. A job is a
    /// member iff its
    /// state is `Created` (an `Activated` job is removed when it locks and re-added
    /// by `JobLockExpired` when its lock expires). This lets `ActivateJobs` serve a
    /// worker poll in roughly `O(max_jobs)` instead of scanning every job in the
    /// system — critical when a large backlog of pending jobs accumulates under
    /// load. It is fully derived from `jobs` and kept in lockstep by
    /// [`resync_job_index`].
    pub activatable_jobs: BTreeMap<String, BTreeSet<(i64, Key)>>,
    /// Jobs deferred by a Camunda 8 v2 `retryBackOff` (#933), ordered by their
    /// eligibility deadline (`retry_backoff_until`) then key. A job is a member
    /// iff it is `Created` with a pending (future) backoff. Keeping deferred
    /// jobs OUT of `activatable_jobs` means a worker poll never scans the
    /// delayed prefix: it promotes only the due prefix of this index (O(due))
    /// and then walks the ready set in O(max_jobs). Derived from `jobs`, kept in
    /// lockstep by [`resync_job_index`]. In-memory only — rebuilt from `jobs` on
    /// snapshot restore, so it needs no serde field.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub deferred_jobs: BTreeSet<(u64, Key)>,
    /// Reverse index of [`State::deferred_jobs`]: job key → the exact deadline
    /// slot it occupies there. Lets a transition remove or replace a job's
    /// deferred entry in O(log n) without scanning the whole deferred backlog,
    /// and guarantees a re-deferred job never leaves a stale `(old, key)` tuple
    /// behind. In-memory only, rebuilt with `deferred_jobs` on restore.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub deferred_job_deadlines: BTreeMap<Key, u64>,
    /// The keys of all jobs currently in the `Activated` state (holding a lock).
    /// Bounded by the number of concurrently working workers, so it lets
    /// `ExpireJobs` find expired locks in `O(activated)` rather than scanning
    /// every job. Derived from `jobs`, kept in lockstep by [`resync_job_index`].
    pub activated_jobs: std::collections::BTreeSet<Key>,
    /// Reverse index of instance key → the keys of every job that instance owns,
    /// regardless of job state. Jobs are only removed from `jobs` in bulk when
    /// their owning instance is evicted, so this index lets eviction drop an
    /// instance's jobs in `O(jobs of that instance)` instead of scanning every
    /// job in the system — the difference between bounded and `O(total backlog)`
    /// eviction under sustained overload. Derived from `jobs`: a key is inserted
    /// when a job is created and the whole entry is removed when the instance is
    /// evicted.
    pub jobs_by_instance: BTreeMap<Key, std::collections::BTreeSet<Key>>,
    /// Reverse index of process instance key → the keys of its resident
    /// call-activity child instances (#1827). Ending an instance cascades a
    /// cancel to its children; with this index that is `O(its children)`
    /// rather than a scan of every resident instance, which made each
    /// completion cost `O(total backlog)`. Derived from
    /// `instances[*].parent_process_instance_key` and maintained only by
    /// [`State::insert_instance`] / [`State::remove_instance`], the sole
    /// writers of `instances` in this crate. In-memory only: rebuilt by
    /// [`State::rebuild_children_index`] on snapshot restore.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub children_by_parent: BTreeMap<Key, std::collections::BTreeSet<Key>>,
    /// Per-process-definition **in-flight instance count** (created but not yet
    /// terminal), keyed by BPMN process id. Maintained by [`apply_event`] at
    /// `ProcessInstanceCreated` (+1) and the terminal transitions
    /// `ProcessInstanceCompleted`/`ProcessInstanceTerminated` (−1) — the *logical*
    /// lifecycle, unaffected by cold spill/rehydrate (those never change
    /// `instance.state`). This is the ADR-0020 Tier-2 signal `L_P`: bounding it
    /// bounds each definition's e2e instance sojourn `W_P = L_P/λ_P`. A definition
    /// drops out of the map once its count returns to zero.
    #[cfg_attr(feature = "serde", serde(default))]
    pub inflight_by_process: BTreeMap<String, u64>,
    /// Per-process-definition cumulative **created** count (monotonic), keyed by
    /// BPMN process id. The monitor differences it across ticks to get each
    /// definition's create rate `λ_P` for the Tier-2 throughput-scaled band
    /// `L*_P = W_target·λ_P`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_by_process: BTreeMap<String, u64>,
    /// Latest deployed decision requirements graph (parsed `.dmn`), keyed by DRG
    /// id. Versioned per DRG id across deployments, like processes. A fast
    /// latest-by-id index over [`State::decision_requirements_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_requirements: BTreeMap<String, DeployedDrg>,
    /// Every deployed DRG version ever seen, keyed by its unique
    /// decision-requirements key (so historical versions are retained, not
    /// overwritten by a redeploy). A `businessRuleTask` binding pinned to a
    /// specific version, or an EvaluateDecision by an older decision key,
    /// resolves through here. `serde(default)` so pre-retention snapshots
    /// deserialize empty and fall back to the latest-by-id index.
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_requirements_versions: BTreeMap<Key, DeployedDrg>,
    /// Latest deployed decision, keyed by decision id, for
    /// `businessRuleTask`/EvaluateDecision lookup. Points at the DRG it belongs
    /// to so required-decision chains resolve. A fast latest-by-id index over
    /// [`State::decision_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub decisions: BTreeMap<String, DeployedDecision>,
    /// Every deployed decision version ever seen, keyed by its unique decision
    /// key, so an EvaluateDecision request pinned to an older decision key can
    /// still resolve the exact version. `serde(default)` for legacy snapshots.
    #[cfg_attr(feature = "serde", serde(default))]
    pub decision_versions: BTreeMap<Key, DeployedDecision>,
    /// Latest deployed form (`form-js` `.form` JSON), keyed by form id. Versioned
    /// per form id across deployments, like processes. The engine does not execute
    /// forms; it retains them so `GetFormByKey` can serve the stored schema. A
    /// fast latest-by-id index over [`State::form_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub forms: BTreeMap<String, DeployedForm>,
    /// Every deployed form version ever seen, keyed by its unique form key, so a
    /// `userTask` form binding pinned to a specific version resolves the exact
    /// schema. `serde(default)` for legacy snapshots.
    #[cfg_attr(feature = "serde", serde(default))]
    pub form_versions: BTreeMap<Key, DeployedForm>,
    /// Latest deployed generic resource (any non-BPMN/DMN/form file, e.g. a
    /// Markdown agent prompt), keyed by `resource_id` (the filename). Versioned
    /// per `resource_id` across deployments, like forms. The engine does not
    /// execute generic resources; it retains them so `GetResourceByKey` and
    /// `searchResources` can serve the stored content. A fast latest-by-id index
    /// over [`State::resource_versions`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub resources: BTreeMap<String, DeployedResource>,
    /// Every deployed generic-resource version ever seen, keyed by its unique
    /// resource key, so a `zeebe:linkedResource` binding pinned to a specific
    /// version (or an older key) resolves the exact content. `serde(default)`
    /// for legacy snapshots.
    #[cfg_attr(feature = "serde", serde(default))]
    pub resource_versions: BTreeMap<Key, DeployedResource>,
    /// Instance key → the [`VariablesLostTombstone`] of its active loss
    /// episode (`Event::VariablesLostQuarantined`), retained as a **replicated dedup
    /// tombstone** (#1574 review). The `VariablesLost` incident record itself
    /// leaves hot state when its instance cold-spills
    /// ([`crate::Engine::snapshot_instance`] drains `incidents` into the cold
    /// row) and is dropped on terminal eviction, so a dedup guard that reads
    /// only `incidents` goes blind on exactly the replica that tiered — a
    /// duplicate committed quarantine then mints a second incident on that
    /// replica while a resident replica skips it, diverging the journals. This
    /// set is independent of instance residency and eviction, so the guard
    /// stays identical on every replica. `serde(default)` so snapshots written
    /// before it deserialize empty (such a snapshot predates the quarantine
    /// event, so there is nothing to dedup yet). The value deserializer accepts
    /// BOTH the legacy encoding (`"7": 9` — the bare incident key, pre-#1587)
    /// and the current tombstone object, so a legacy snapshot holding an ACTIVE
    /// quarantine still loads (as episode `0`, the episode-blind sentinel);
    /// `serde(default)` alone does not convert an existing numeric value.
    #[cfg_attr(feature = "serde", serde(default))]
    pub variables_lost_quarantined: BTreeMap<Key, VariablesLostTombstone>,
    /// Loss reports whose stamped quarantine applied SUPERSEDED
    /// (`Event::OrphanStampSuperseded`) and whose re-stamped retry has not yet
    /// committed (#1608), keyed by instance with the off-heap tier the report
    /// was detected against. A **replicated** record — inserted by the
    /// supersede, cleared by `VariablesLostQuarantined` or the instance's
    /// terminal event — so it survives every snapshot/compaction checkpoint
    /// and engine adoption: the host's in-memory orphan backlog is rebuilt from
    /// it after recovery, so a crash or failover between a supersede and its
    /// retry never silently drops the loss. `serde(default)` (additive): a
    /// snapshot written before it deserializes empty.
    /// Omitted when empty, so a snapshot with nothing pending keeps the
    /// pre-#1608 shape byte-for-byte.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub pending_orphan_supersedes: BTreeMap<Key, crate::command::OrphanKind>,
    /// Instance key → the loss-episode id of its most recently quarantined
    /// (`VariablesLostQuarantined`) episode (#1608 review). A **replicated**
    /// record that outlives the tombstone (lifted on resolve), so a delayed
    /// duplicate of an already-settled episode — which applies superseded —
    /// is recognised and NOT recorded in `pending_orphan_supersedes`;
    /// otherwise recovery would re-park, re-stamp and raise a second
    /// `VariablesLost` without a new loss. Cleared on the instance's terminal
    /// event. Additive; omitted when empty.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "BTreeMap::is_empty")
    )]
    pub settled_orphan_episodes: BTreeMap<Key, Key>,
    /// The most recent instance keys to reach a terminal state
    /// (`ProcessInstanceCompleted` / `ProcessInstanceTerminated`), oldest
    /// first, bounded by [`RECENTLY_TERMINATED_CAP`] (#1574 review). A
    /// **replicated** record — appended only by `apply_event`, so identical on
    /// every replica and independent of host-driven terminal eviction (which is
    /// replica-local in timing). It closes the orphan-quarantine cancellation
    /// race: a stamped `QuarantineOffheapOrphan` queued before a cancel can
    /// commit after it, when one replica still holds the terminal instance and
    /// another has already evicted it; both must no-op identically.
    /// `serde(default)`: older snapshots load empty.
    #[cfg_attr(feature = "serde", serde(default))]
    pub recently_terminated: std::collections::VecDeque<Key>,
    /// Total terminal transitions ever applied (`ProcessInstanceCompleted` /
    /// `ProcessInstanceTerminated`) — a **replicated** monotonic epoch
    /// (#1574 review). The [`recently_terminated`](Self::recently_terminated)
    /// ring is bounded, so once it wraps it can no longer answer "did this key
    /// terminate after the leader stamped its orphan proposal?" for an evicted
    /// instance. The epoch can: the leader stamps the value it saw, and the
    /// apply gate compares the distance against the ring's coverage
    /// (`terminal_epoch - stamp_epoch <= recently_terminated.len()` ⇒ every
    /// transition since the stamp is still in the ring). Replicated-only, so
    /// every replica computes the identical distance and decides identically.
    /// `serde(default)`: older snapshots load `0` (they predate the quarantine
    /// command, so no stamped proposal can be outstanding against them).
    #[cfg_attr(feature = "serde", serde(default))]
    pub terminal_epoch: u64,
    /// Lenient completion (#1661, ADR 0066 slice 2): when `true`, a
    /// `CompleteJob` / `FailJob` / `ThrowJobError` is accepted for a job that was
    /// never activated on this replica (the `activated` latch is not required).
    /// This supports a clustered mode where the activation lock is
    /// *leader-local* (not replicated through Raft): a follower never observes
    /// the `JobActivated` event, so a replicated completion would otherwise fail
    /// `JobNotActivated` and diverge from the leader. Possession of the job key
    /// *is* the capability (keys are only handed out by activation), so this
    /// stays within the at-least-once contract.
    ///
    /// Replicated engine state, derived only from
    /// [`crate::Event::LenientCompletionSet`] (emitted by the replicated
    /// [`crate::Command::SetLenientCompletion`]), so every replica flips it at
    /// the same log index — never host config a node changes on its own clock.
    /// `serde(default)`: older snapshots load strict (`false`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub lenient_completion: bool,
}

/// Bound on [`State::recently_terminated`]. The window it must cover is one
/// orphan proposal's round trip (enqueue → commit), so a few thousand terminal
/// transitions is ample while keeping the snapshot cost tiny (8 bytes/key).
pub const RECENTLY_TERMINATED_CAP: usize = 4_096;

/// The replicated dedup tombstone one active off-heap-orphan loss episode
/// leaves in [`State::variables_lost_quarantined`] (#1574, episode id #1587).
/// Recorded by [`crate::Event::VariablesLostQuarantined`]; lifted when the
/// episode's incident is resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct VariablesLostTombstone {
    /// The `VariablesLost` incident the quarantine raised: resolving exactly
    /// this incident lifts the tombstone, keyed on the replicated
    /// `IncidentResolved.incident_key` so the lift does not depend on whether
    /// the incident record is resident.
    pub incident_key: Key,
    /// The replicated loss-episode id the quarantine's stamp carried (#1587).
    /// A delayed duplicate of the SAME episode applies after the incident was
    /// resolved (and the tombstone lifted) and must be a no-op: with no live
    /// tombstone its episode matches nothing, so the apply gate suppresses it
    /// instead of raising a fresh incident. A genuinely new loss stamps a
    /// fresh, distinct episode id and is still reported. `0` for a tombstone
    /// written before this field existed (episode-blind, pre-#1587 behaviour).
    /// The id is the leader's allocator position at stamp time — unrelated
    /// key-minting work can advance the allocator before the proposal commits,
    /// so a position mismatch at apply time supersedes the stamp (re-stamp +
    /// re-propose via [`crate::Event::OrphanStampSuperseded`]) rather than
    /// dropping the report (#1587 review).
    #[cfg_attr(feature = "serde", serde(default))]
    pub episode: u64,
    /// The quarantined instance reached a terminal state while this tombstone
    /// was live (#1608 review). Set by the replicated terminal fold
    /// ([`State::note_terminated`]) so a later `IncidentResolved` decides
    /// "already terminal" from replicated state alone — never from whether the
    /// terminal shell is still resident (it may be evicted) or still in the
    /// bounded `recently_terminated` ring (it may have wrapped). Additive:
    /// omitted when `false`, defaulted on read.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "std::ops::Not::not")
    )]
    pub terminal: bool,
}

/// Backward-compatible deserialization for [`VariablesLostTombstone`]: a
/// snapshot written before #1587 encodes each map entry as the bare incident
/// key (`"7": 9`), which cannot deserialize into the struct — and
/// `serde(default)` does not convert an EXISTING numeric value, so without
/// this a legacy snapshot holding an active quarantine fails to load through
/// every direct `EngineSnapshot` reader, including Raft snapshot installation.
/// Accept the legacy bare key (as `{ incident_key, episode: 0 }` — the
/// episode-blind sentinel, matching the pre-#1587 behaviour that snapshot ran
/// under) alongside the current object form.
#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for VariablesLostTombstone {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Repr {
            /// Current form: the tombstone object.
            Current {
                incident_key: Key,
                #[serde(default)]
                episode: u64,
                #[serde(default)]
                terminal: bool,
            },
            /// Legacy (pre-#1587) form: the bare incident key.
            Legacy(Key),
        }
        Ok(match Repr::deserialize(deserializer)? {
            Repr::Current {
                incident_key,
                episode,
                terminal,
            } => VariablesLostTombstone {
                incident_key,
                episode,
                terminal,
            },
            Repr::Legacy(incident_key) => VariablesLostTombstone {
                incident_key,
                episode: 0,
                terminal: false,
            },
        })
    }
}

impl State {
    /// The settlement half of the live `VariablesLostQuarantined` fold
    /// (#1608): record `episode` as `instance_key`'s settled loss episode.
    /// The ONE canonical derivation of `settled_orphan_episodes`, shared by
    /// the live applier and [`State::backfill_settled_orphan_episodes`].
    pub(crate) fn fold_settlement_quarantined(&mut self, instance_key: Key, episode: Key) {
        if episode > 0 {
            self.settled_orphan_episodes.insert(instance_key, episode);
        }
    }

    /// The settlement half of the live terminal fold (#1608): a terminal
    /// instance's settled episode no longer matters.
    pub(crate) fn fold_settlement_terminal(&mut self, instance_key: Key) {
        self.settled_orphan_episodes.remove(&instance_key);
    }

    /// Best-effort backfill of `settled_orphan_episodes` from retained history
    /// that a snapshot covers (#1608 review). A snapshot written before #1608
    /// carries no settlement map, and an episode resolved before the upgrade
    /// has also lifted its tombstone — so a delayed duplicate's supersede in
    /// the replayed tail would be recorded pending and re-raise
    /// `VariablesLost` without a new loss. Recovery calls this with whatever
    /// prefix `[0, covered)` it still holds (cold archive + hot prefix)
    /// BEFORE replaying the tail. It folds the history through the same
    /// helpers the live applier uses, then merges by high-water mark (episodes
    /// grow monotonically per instance), so on a post-#1608 snapshot it is a
    /// no-op: the live fold already recorded exactly these values.
    ///
    /// Residual window (documented, accepted): when the prefix holding the
    /// quarantine was pruned, nothing can be backfilled and a delayed
    /// duplicate straddling the upgrade raises ONE extra, resolvable
    /// `VariablesLost` incident — never data loss.
    pub fn backfill_settled_orphan_episodes<'a, I>(&mut self, history: I)
    where
        I: IntoIterator<Item = &'a crate::Event>,
    {
        // Fold the retained history through the LIVE applier (the one
        // canonical derivation of settlement and pending supersedes), on a
        // scratch state that sees only the loss-lifecycle events.
        let mut folded = State::default();
        let mut superseded_at: BTreeMap<Key, Key> = BTreeMap::new();
        let mut terminated_in_history: BTreeSet<Key> = BTreeSet::new();
        for event in history {
            if !event.is_loss_lifecycle() {
                continue;
            }
            if let Some(instance_key) = event.terminal_instance_key() {
                terminated_in_history.insert(instance_key);
            }
            if let crate::Event::OrphanStampSuperseded {
                instance_key,
                episode,
                ..
            } = event
            {
                // Keep the HIGHEST superseded episode per instance, not
                // the last in journal order (#1608 review): a delayed
                // duplicate E5 arriving after the fresh supersede E9 must
                // not replace E9 here — the pending-supersede
                // reconstruction below would then compare E5 against the
                // settlement mark and drop the genuinely unresolved E9
                // report, leaving the dormant loss unreported.
                superseded_at
                    .entry(*instance_key)
                    .and_modify(|e| *e = (*e).max(*episode))
                    .or_insert(*episode);
            }
            crate::state::apply(&mut folded, event);
        }
        // A legacy (pre-#1608) tombstone carries `terminal: false` even when
        // the instance completed after its quarantine. Normalize it from the
        // retained terminal history BEFORE the snapshot's terminal shell can
        // be evicted or the bounded ring wraps, so a later `IncidentResolved`
        // decides terminality identically on every replica (#1608 review).
        self.normalize_legacy_tombstones(&terminated_in_history);
        // The snapshot is AUTHORITATIVE over the retained history: it may be
        // newer than it (an adopted leader snapshot) or the history may have a
        // pruned gap. Never resurrect an entry for an instance the snapshot
        // already knows terminated (a later termination the local history
        // does not hold) — the terminal fold cleared it there.
        for (instance_key, episode) in folded.settled_orphan_episodes {
            if self.snapshot_shows_terminal(instance_key) {
                continue;
            }
            let entry = self
                .settled_orphan_episodes
                .entry(instance_key)
                .or_insert(0);
            *entry = (*entry).max(episode);
        }
        // A legacy (pre-#1608) snapshot carries no pending-supersede map, so a
        // covered supersede whose retry never committed would stay dormant:
        // reconstruct it — unless the snapshot shows it settled (an active
        // tombstone or a settlement mark at/above its episode) or terminated.
        for (instance_key, kind) in folded.pending_orphan_supersedes {
            let episode = superseded_at.get(&instance_key).copied().unwrap_or(0);
            if self.snapshot_shows_terminal(instance_key)
                || self.variables_lost_quarantined.contains_key(&instance_key)
                || (episode > 0
                    && super::apply::settled_orphan_episode(self, instance_key)
                        .is_some_and(|s| episode <= s))
            {
                continue;
            }
            self.pending_orphan_supersedes
                .entry(instance_key)
                .or_insert(kind);
        }
    }

    /// Sets the replicated `terminal` flag on every loss tombstone whose
    /// instance the snapshot's own lifecycle records ([`Self::snapshot_shows_terminal`])
    /// or `terminated` (retained terminal history) prove terminal (#1608
    /// review). A tombstone restored from a pre-#1608 snapshot defaults the
    /// flag to `false`; without this, terminality after shell eviction / ring
    /// wrap would depend on residency and split replicas. Idempotent, and a
    /// no-op on a post-#1608 snapshot (the live terminal fold already set it).
    pub(crate) fn normalize_legacy_tombstones(&mut self, terminated: &BTreeSet<Key>) {
        let to_mark: Vec<Key> = self
            .variables_lost_quarantined
            .iter()
            .filter(|(k, t)| {
                !t.terminal && (terminated.contains(k) || self.snapshot_shows_terminal(**k))
            })
            .map(|(k, _)| *k)
            .collect();
        for key in to_mark {
            if let Some(t) = self.variables_lost_quarantined.get_mut(&key) {
                t.terminal = true;
            }
        }
    }

    /// Whether the SNAPSHOT's own lifecycle records prove `instance_key`
    /// reached a terminal state (#1608 review): the replicated terminal
    /// record ([`State::terminated_epoch_of`] — the resident instance's
    /// stamped epoch or its position in the bounded terminal ring), or the
    /// instance's own resident lifecycle state (`Completed` / `Terminated`).
    ///
    /// The state check closes the bounded-ring hole: once a terminal instance
    /// falls out of the 4,096-entry `recently_terminated` ring,
    /// `terminated_epoch_of` returns `None` even though the snapshot still
    /// holds the instance's terminal shell, and without it a lagging
    /// receiver's earlier supersede/quarantine would be restored as pending
    /// or settled — resurrecting a loss report for work the authoritative
    /// snapshot shows COMPLETED. `Terminating` is not terminal (the instance
    /// is still draining its canceling listeners), and a missing instance
    /// (genuinely cold / pruned) is not proven terminal either — a cold
    /// target's report must still be preserved.
    pub(crate) fn snapshot_shows_terminal(&self, instance_key: Key) -> bool {
        if self.terminated_epoch_of(instance_key).is_some() {
            return true;
        }
        self.instances.get(&instance_key).is_some_and(|i| {
            matches!(
                i.state,
                crate::state::types::ProcessInstanceState::Completed
                    | crate::state::types::ProcessInstanceState::Terminated
            )
        })
    }

    /// Records `instance_key` as terminal in the bounded replicated ring and
    /// advances the replicated terminal epoch. Also stamps the instance's own
    /// [`ProcessInstance::terminated_epoch`] with the new epoch — the
    /// target-specific lifecycle-validity record the orphan-quarantine gate
    /// reads (#1574 review). The epoch is advanced FIRST so the instance records
    /// the value its own terminal transition produced.
    pub(crate) fn note_terminated(&mut self, instance_key: Key) {
        if self.recently_terminated.len() >= RECENTLY_TERMINATED_CAP {
            self.recently_terminated.pop_front();
        }
        self.recently_terminated.push_back(instance_key);
        // #1608: a terminal instance's pending loss report no longer matters.
        self.pending_orphan_supersedes.remove(&instance_key);
        self.fold_settlement_terminal(instance_key);
        if let Some(t) = self.variables_lost_quarantined.get_mut(&instance_key) {
            t.terminal = true;
        }
        self.terminal_epoch += 1;
        if let Some(instance) = self.instances.get_mut(&instance_key) {
            instance.terminated_epoch = self.terminal_epoch;
        }
    }

    /// The replicated terminal epoch at which `instance_key` reached a terminal
    /// state, read from whichever replicated record still holds it: the resident
    /// instance's own [`ProcessInstance::terminated_epoch`], or — for an
    /// instance already evicted from hot state — its position in the bounded
    /// [`recently_terminated`](Self::recently_terminated) ring (the oldest
    /// retained entry is `terminal_epoch - len + 1`, so index `i` is epoch
    /// `terminal_epoch - len + 1 + i`). `None` when no replicated record can
    /// prove the instance terminated (live, or terminated so long ago the ring
    /// has wrapped past it). Replicated-only, so every replica computes the
    /// identical value for an instance it can still see.
    pub(crate) fn terminated_epoch_of(&self, instance_key: Key) -> Option<u64> {
        if let Some(instance) = self.instances.get(&instance_key) {
            return (instance.terminated_epoch > 0).then_some(instance.terminated_epoch);
        }
        let len = self.recently_terminated.len() as u64;
        self.recently_terminated
            .iter()
            .position(|k| *k == instance_key)
            .map(|i| self.terminal_epoch - len + 1 + i as u64)
    }
}

/// A deployed decision requirements graph together with the identity the engine
/// assigned it at deploy time.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedDrg {
    /// Unique key for this specific DRG (and version).
    pub key: Key,
    /// Version, incremented per DRG id across deployments (starts 1).
    pub version: i32,
    /// The parsed graph.
    pub drg: crate::dmn::DecisionRequirementsGraph,
}

/// A deployed decision, indexed by id for evaluation lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedDecision {
    /// Unique key for this specific decision (and version).
    pub key: Key,
    /// Version, incremented per decision id across deployments (starts 1).
    pub version: i32,
    /// The DRG this decision belongs to (needed to evaluate required decisions).
    pub decision_requirements_key: Key,
    /// The decision's id (the lookup key).
    pub decision_id: String,
    /// Human-readable name.
    pub decision_name: String,
    /// The full DRG this decision is part of, so evaluation can follow
    /// `requiredDecision` references natively.
    pub drg: crate::dmn::DecisionRequirementsGraph,
}

/// A deployed form together with the identity the engine assigned it at deploy
/// time. The engine stores forms but does not execute them — they are served
/// verbatim by `GetFormByKey`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedForm {
    /// Unique key for this specific form (and version).
    pub key: Key,
    /// Version, incremented per form id across deployments (starts 1).
    pub version: i32,
    /// The user-provided form identifier (the form-js document's `id`).
    pub form_id: String,
    /// The deploy resource name (e.g. `greeting.form`).
    pub resource_name: String,
    /// The verbatim form-js JSON document.
    pub schema: String,
    /// The deployment this form version was registered under (Zeebe
    /// `deploymentKey`): the membership handle a `deployment`-bound user-task
    /// form resolves against (#1193). `0` for a form restored from a pre-#1193
    /// snapshot (or seeded from one), whose membership is unknown — it never
    /// matches a `deployment` binding.
    #[cfg_attr(feature = "serde", serde(default))]
    pub deployment_key: Key,
    /// The optional version tag this form version carries (form-js
    /// `versionTag`), matched by a `versionTag`-bound user-task form (#1193).
    /// `None` when the form declares no tag (or predates tag tracking).
    #[cfg_attr(feature = "serde", serde(default))]
    pub version_tag: Option<String>,
}

/// A deployed generic resource together with the identity the engine assigned it
/// at deploy time. The engine stores generic resources but does not execute
/// them — they are served verbatim by `GetResourceByKey` / `searchResources`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeployedResource {
    /// Unique key for this specific resource (and version).
    pub key: Key,
    /// Version, incremented per `resource_id` across deployments (starts 1).
    pub version: i32,
    /// The resource identifier (its filename, for a generic resource).
    pub resource_id: String,
    /// The deploy resource name (the filename).
    pub resource_name: String,
    /// The verbatim resource content.
    pub content: String,
    /// The deployment this resource version was registered under (Zeebe
    /// `deploymentKey`): the membership handle a `deployment`-bound
    /// `zeebe:linkedResource` resolves against (#1193). `0` for a resource
    /// restored from a pre-#1193 snapshot (or seeded from one), whose
    /// membership is unknown — it never matches a `deployment` binding.
    #[cfg_attr(feature = "serde", serde(default))]
    pub deployment_key: Key,
    /// The optional version tag this resource version carries (Zeebe
    /// `versionTag` deploy metadata), matched by a `versionTag`-bound
    /// `zeebe:linkedResource` (#1193). `None` when the deploy carried no tag
    /// (or predates tag tracking).
    #[cfg_attr(feature = "serde", serde(default))]
    pub version_tag: Option<String>,
}

/// A self-contained snapshot of one process instance and every entity it owns
/// (jobs, timers, message subscriptions, user tasks, incidents), captured for
/// **cold spill**: the host-managed eviction of an idle but still-running
/// instance's full control state to disk, to be rehydrated on demand when an
/// event targets it.
///
/// Distinct from variable spill (which sheds only the `variables` payload of a
/// job-parked instance, rehydrated on activation): a cold snapshot moves the
/// *whole* instance — including instances parked on a timer or a message, the
/// genuinely long-lived waits — out of hot state, so a backlog of dormant
/// instances stops costing RAM. The snapshot is authoritative and self-contained
/// (its `instance.variables` hold the real payload, never a spilled
/// placeholder), so [`crate::Engine::rehydrate_instance`] reconstructs hot state
/// exactly. Like every spill artefact it is a cache, not a system of record: the
/// journal already holds the durable history, so a lost snapshot is replayable.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstanceSnapshot {
    pub instance: ProcessInstance,
    pub jobs: Vec<Job>,
    pub timers: Vec<Timer>,
    pub message_subscriptions: Vec<MessageSubscription>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub signal_subscriptions: Vec<SignalSubscription>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub conditional_subscriptions: Vec<ConditionalSubscription>,
    pub user_tasks: Vec<UserTask>,
    pub incidents: Vec<Incident>,
}

impl State {
    /// A fresh, empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts (or overwrites) a process instance, keeping
    /// [`State::children_by_parent`] in lockstep. With
    /// [`State::remove_instance`], the only writer of `instances` in this crate
    /// (#1827); the test-build invariant
    /// [`State::assert_children_index_covers_instances`] catches a bypass.
    pub fn insert_instance(&mut self, instance: ProcessInstance) {
        let key = instance.key;
        if let Some(parent) = instance.parent_process_instance_key {
            self.children_by_parent
                .entry(parent)
                .or_default()
                .insert(key);
        }
        if let Some(previous) = self.instances.insert(key, instance) {
            // An overwrite that re-parented the instance (never expected, but
            // keep the index exact rather than leak the old link).
            let new_parent = self.instances[&key].parent_process_instance_key;
            if previous.parent_process_instance_key != new_parent {
                if let Some(old) = previous.parent_process_instance_key {
                    self.unlink_child(old, key);
                }
            }
        }
    }

    /// Removes a process instance, unlinking it from its parent's entry in
    /// [`State::children_by_parent`]. Its own entry as a parent is kept: its
    /// children may stay resident (e.g. the parent alone was spilled to cold
    /// storage and will be rehydrated), and each child unlinks itself when it is
    /// removed, so an entry empties (and is dropped) with its last child.
    pub fn remove_instance(&mut self, key: Key) -> Option<ProcessInstance> {
        let instance = self.instances.remove(&key)?;
        if let Some(parent) = instance.parent_process_instance_key {
            self.unlink_child(parent, key);
        }
        Some(instance)
    }

    fn unlink_child(&mut self, parent: Key, child: Key) {
        if let Some(children) = self.children_by_parent.get_mut(&parent) {
            children.remove(&child);
            if children.is_empty() {
                self.children_by_parent.remove(&parent);
            }
        }
    }

    /// Re-derives [`State::children_by_parent`] from `instances` in one pass.
    /// The index is serde-skipped, so a restored snapshot carries it empty.
    pub fn rebuild_children_index(&mut self) {
        self.children_by_parent.clear();
        for instance in self.instances.values() {
            if let Some(parent) = instance.parent_process_instance_key {
                self.children_by_parent
                    .entry(parent)
                    .or_default()
                    .insert(instance.key);
            }
        }
    }

    /// Buffers `message`, keeping [`State::buffered_message_ids`] in step.
    pub fn insert_buffered_message(&mut self, message: BufferedMessage) {
        self.remove_buffered_message(message.key);
        if let Some(id) = &message.message_id {
            self.buffered_message_ids.insert(
                (
                    message.message_name.clone(),
                    message.correlation_key.clone(),
                    id.clone(),
                ),
                message.key,
            );
        }
        self.buffered_by_correlation
            .entry((
                message.message_name.clone(),
                message.correlation_key.clone(),
            ))
            .or_default()
            .insert((message.seq, message.key));
        self.buffered_messages.insert(message.key, message);
    }

    /// Removes a buffered message, keeping [`State::buffered_message_ids`] in step.
    pub fn remove_buffered_message(&mut self, key: Key) -> Option<BufferedMessage> {
        let removed = self.buffered_messages.remove(&key)?;
        self.unindex_buffered_message(&removed);
        Some(removed)
    }

    /// Drops `message`'s index entry only if it still points at `message`
    /// (a newer message may have reused the id after this one expired).
    fn unindex_buffered_message(&mut self, message: &BufferedMessage) {
        let ck = (
            message.message_name.clone(),
            message.correlation_key.clone(),
        );
        if let Some(set) = self.buffered_by_correlation.get_mut(&ck) {
            set.remove(&(message.seq, message.key));
            if set.is_empty() {
                self.buffered_by_correlation.remove(&ck);
            }
        }
        if let Some(id) = &message.message_id {
            let k = (
                message.message_name.clone(),
                message.correlation_key.clone(),
                id.clone(),
            );
            if self.buffered_message_ids.get(&k) == Some(&message.key) {
                self.buffered_message_ids.remove(&k);
            }
        }
    }

    /// The buffered message with this `messageId` for `name` + `correlation_key`
    /// that is still live at `now`, via the index (no buffer scan).
    pub fn live_buffered_message_by_id(
        &self,
        name: &str,
        correlation_key: &str,
        message_id: &str,
        now: u64,
    ) -> Option<&BufferedMessage> {
        let k = (
            name.to_string(),
            correlation_key.to_string(),
            message_id.to_string(),
        );
        self.buffered_message_ids
            .get(&k)
            .and_then(|key| self.buffered_messages.get(key))
            .filter(|m| m.is_live_at(now))
    }

    /// Re-derives [`State::buffered_message_ids`] from `buffered_messages`
    /// (the index is serde-skipped, so a restored snapshot carries it empty).
    /// When several entries share an id, the newest (highest `seq`) wins —
    /// the only one that can still be live.
    pub fn rebuild_buffered_message_id_index(&mut self) {
        self.buffered_message_ids.clear();
        self.buffered_by_correlation.clear();
        for m in self.buffered_messages.values() {
            self.buffered_by_correlation
                .entry((m.message_name.clone(), m.correlation_key.clone()))
                .or_default()
                .insert((m.seq, m.key));
        }
        let mut by_seq: Vec<&BufferedMessage> = self.buffered_messages.values().collect();
        by_seq.sort_by_key(|m| m.seq);
        for m in by_seq {
            if let Some(id) = &m.message_id {
                self.buffered_message_ids.insert(
                    (
                        m.message_name.clone(),
                        m.correlation_key.clone(),
                        id.clone(),
                    ),
                    m.key,
                );
            }
        }
    }

    /// Test-build invariant (#1874): every [`State::buffered_message_ids`]
    /// entry points at a buffered message carrying that exact
    /// `(name, correlation key, messageId)` and is the newest (highest `seq`)
    /// such message, and a triple held by exactly one buffered message is
    /// indexed. A write that bypassed [`State::insert_buffered_message`] /
    /// [`State::remove_buffered_message`] fails every engine test. (An older,
    /// necessarily expired duplicate may be left unindexed once its newer
    /// twin is removed: it can never make a publish a duplicate.)
    #[cfg(test)]
    pub(crate) fn assert_buffered_message_id_index_consistent(&self) {
        let mut by_triple: BTreeMap<(String, String, String), Vec<&BufferedMessage>> =
            BTreeMap::new();
        for m in self.buffered_messages.values() {
            if let Some(id) = &m.message_id {
                by_triple
                    .entry((
                        m.message_name.clone(),
                        m.correlation_key.clone(),
                        id.clone(),
                    ))
                    .or_default()
                    .push(m);
            }
        }
        for (triple, key) in &self.buffered_message_ids {
            let group = by_triple
                .get(triple)
                .unwrap_or_else(|| panic!("index entry {triple:?} has no buffered message"));
            let newest = group.iter().max_by_key(|m| m.seq).map(|m| m.key);
            assert_eq!(
                newest,
                Some(*key),
                "index entry {triple:?} is not the newest"
            );
        }
        let mut by_correlation: BTreeMap<(String, String), BTreeSet<(u64, Key)>> = BTreeMap::new();
        for m in self.buffered_messages.values() {
            by_correlation
                .entry((m.message_name.clone(), m.correlation_key.clone()))
                .or_default()
                .insert((m.seq, m.key));
        }
        assert_eq!(
            self.buffered_by_correlation, by_correlation,
            "buffered (name, correlation key) index drifted from the buffer"
        );
        for (triple, group) in &by_triple {
            if group.len() == 1 {
                assert!(
                    self.buffered_message_ids.contains_key(triple),
                    "buffered message {triple:?} missing from the messageId index"
                );
            }
        }
    }

    /// Test-build invariant (#1827): every resident instance with a parent is
    /// listed under that parent in [`State::children_by_parent`]. A missing
    /// link would let a cancelled parent orphan its child, so any write to
    /// `instances` that bypasses [`State::insert_instance`] fails here.
    #[cfg(test)]
    pub(crate) fn assert_children_index_covers_instances(&self) {
        for instance in self.instances.values() {
            if let Some(parent) = instance.parent_process_instance_key {
                assert!(
                    self.children_by_parent
                        .get(&parent)
                        .is_some_and(|c| c.contains(&instance.key)),
                    "children_by_parent misses child {} of parent {parent}: an \
                     `instances` write bypassed State::insert_instance",
                    instance.key
                );
            }
        }
    }

    /// The executable definition an instance runs against — resolved through the
    /// instance's pinned `process_definition_key` so it is always the version the
    /// instance was created on, even after a newer version is redeployed (Zeebe
    /// parity). Falls back to the latest-by-id index for instances (or snapshots)
    /// created before version pinning, whose `process_definition_key` is `0`.
    pub fn definition_for(&self, instance: &ProcessInstance) -> Option<&DeployedProcess> {
        if instance.process_definition_key != 0 {
            if let Some(deployed) = self.process_versions.get(&instance.process_definition_key) {
                return Some(deployed);
            }
        }
        self.processes.get(&instance.process_id)
    }

    /// The exact deployed definition identified by its process-definition
    /// `key`, if retained. Checks the full version-retention map first, then
    /// falls back to the latest-by-id index for engines restored from
    /// pre-retention snapshots — where `process_versions` deserializes empty
    /// (`serde(default)`) but `processes` still holds the latest deployed
    /// definition (including its key). Without the fallback, a valid by-key
    /// create would spuriously 400 after an upgrade until a redeploy
    /// repopulated `process_versions`.
    pub fn process_by_key(&self, key: Key) -> Option<&DeployedProcess> {
        if let Some(deployed) = self.process_versions.get(&key) {
            return Some(deployed);
        }
        self.processes.values().find(|d| d.key == key)
    }

    /// The exact deployed version of `process_id` with version number `version`,
    /// if one is retained. Used to resolve an explicit by-id + version create
    /// request to a concrete definition. Checks the full version-retention map
    /// first, then falls back to the latest-by-id index for engines restored
    /// from pre-retention snapshots (empty `process_versions`), where only the
    /// latest version per id — the only version legacy snapshots can preserve —
    /// is still available in `processes`.
    pub fn process_version(&self, process_id: &str, version: i32) -> Option<&DeployedProcess> {
        if let Some(deployed) = self
            .process_versions
            .values()
            .find(|d| d.definition.id == process_id && d.version == version)
        {
            return Some(deployed);
        }
        self.processes
            .get(process_id)
            .filter(|d| d.version == version)
    }

    /// The exact deployed DRG identified by its unique `key`, if retained.
    /// Checks the version-retention map first, then falls back to the
    /// latest-by-id index for engines restored from pre-retention snapshots
    /// (where `decision_requirements_versions` deserializes empty).
    pub fn drg_by_key(&self, key: Key) -> Option<&DeployedDrg> {
        if let Some(deployed) = self.decision_requirements_versions.get(&key) {
            return Some(deployed);
        }
        self.decision_requirements.values().find(|d| d.key == key)
    }

    /// The exact deployed version of DRG `drg_id` with version number `version`,
    /// if retained. Falls back to the latest-by-id index for pre-retention
    /// snapshots (only the latest version is available there).
    pub fn drg_version(&self, drg_id: &str, version: i32) -> Option<&DeployedDrg> {
        if let Some(deployed) = self
            .decision_requirements_versions
            .values()
            .find(|d| d.drg.id == drg_id && d.version == version)
        {
            return Some(deployed);
        }
        self.decision_requirements
            .get(drg_id)
            .filter(|d| d.version == version)
    }

    /// The exact deployed decision identified by its unique `key`, if retained.
    /// Checks the version-retention map first, then falls back to the
    /// latest-by-id index for pre-retention snapshots.
    pub fn decision_by_key(&self, key: Key) -> Option<&DeployedDecision> {
        if let Some(deployed) = self.decision_versions.get(&key) {
            return Some(deployed);
        }
        self.decisions.values().find(|d| d.key == key)
    }

    /// The decision `decision_id` exactly as deployed in the DRG version
    /// `decision_requirements_key` — how each decision evaluated within one
    /// DRG is identified (Zeebe resolves an evaluated decision's key and version
    /// from the evaluated DRG, not from the latest version of its id). Falls
    /// back to the latest-by-id index for pre-retention snapshots.
    pub fn decision_in_drg(
        &self,
        decision_requirements_key: Key,
        decision_id: &str,
    ) -> Option<&DeployedDecision> {
        let in_drg = |d: &&DeployedDecision| {
            d.decision_requirements_key == decision_requirements_key && d.decision_id == decision_id
        };
        self.decision_versions
            .values()
            .find(in_drg)
            .or_else(|| self.decisions.get(decision_id).filter(in_drg))
    }

    /// The exact deployed version of decision `decision_id` with version number
    /// `version`, if retained. Falls back to the latest-by-id index for
    /// pre-retention snapshots.
    pub fn decision_version(&self, decision_id: &str, version: i32) -> Option<&DeployedDecision> {
        if let Some(deployed) = self
            .decision_versions
            .values()
            .find(|d| d.decision_id == decision_id && d.version == version)
        {
            return Some(deployed);
        }
        self.decisions
            .get(decision_id)
            .filter(|d| d.version == version)
    }

    /// The exact deployed form identified by its unique `key`, if retained.
    /// Checks the version-retention map first, then falls back to the
    /// latest-by-id index for pre-retention snapshots.
    pub fn form_by_key(&self, key: Key) -> Option<&DeployedForm> {
        if let Some(deployed) = self.form_versions.get(&key) {
            return Some(deployed);
        }
        self.forms.values().find(|f| f.key == key)
    }

    /// The exact deployed version of form `form_id` with version number
    /// `version`, if retained. Falls back to the latest-by-id index for
    /// pre-retention snapshots.
    pub fn form_version(&self, form_id: &str, version: i32) -> Option<&DeployedForm> {
        if let Some(deployed) = self
            .form_versions
            .values()
            .find(|f| f.form_id == form_id && f.version == version)
        {
            return Some(deployed);
        }
        self.forms.get(form_id).filter(|f| f.version == version)
    }

    /// The exact deployed generic resource identified by its unique `key`, if
    /// retained. Checks the version-retention map first, then falls back to the
    /// latest-by-id index for pre-retention snapshots.
    pub fn resource_by_key(&self, key: Key) -> Option<&DeployedResource> {
        if let Some(deployed) = self.resource_versions.get(&key) {
            return Some(deployed);
        }
        self.resources.values().find(|r| r.key == key)
    }

    /// The exact deployed version of resource `resource_id` with version number
    /// `version`, if retained. Falls back to the latest-by-id index for
    /// pre-retention snapshots.
    pub fn resource_version(&self, resource_id: &str, version: i32) -> Option<&DeployedResource> {
        if let Some(deployed) = self
            .resource_versions
            .values()
            .find(|r| r.resource_id == resource_id && r.version == version)
        {
            return Some(deployed);
        }
        self.resources
            .get(resource_id)
            .filter(|r| r.version == version)
    }

    /// Count of jobs that represent *live* runnable congestion — `Created`
    /// (waiting for a worker, in the activatable index) plus `Activated`
    /// (leased, in-flight at a worker, in the activated index). Terminal jobs
    /// (`Completed`/`Failed`/`Errored`) are deliberately excluded: they are
    /// deindexed from both sets the instant they settle (see
    /// [`resync_job_index`]) but linger in `jobs` until their owning instance is
    /// evicted by the exporter. Counting `jobs.len()` instead would fold those
    /// dead, un-evicted terminal jobs into the backpressure reading, so an
    /// exporter that falls behind (or is stalled by a locked read-model store)
    /// silently inflates the admission/governor congestion signal and sheds
    /// legitimate new work — a self-inflicted throughput collapse that never
    /// clears, since the leaked terminal jobs are never re-exported. The two
    /// indices are disjoint and kept in lockstep with `jobs`, so their combined
    /// size is the exact live backlog at `O(#job_types)`.
    pub fn live_job_count(&self) -> usize {
        // Deferred (`retryBackOff`, #933) jobs are `Created` and live: they sit
        // in `deferred_jobs` instead of `activatable_jobs`, never in both.
        self.activated_jobs.len()
            + self.deferred_jobs.len()
            + self
                .activatable_jobs
                .values()
                .map(|set| set.len())
                .sum::<usize>()
    }

    /// Waiting (activatable) job counts per job type at logical instant `now`:
    /// the ready index plus every deferred (`retryBackOff`, #933) job whose
    /// deadline has been reached. Each job is counted once (a job is in exactly
    /// one of the two indices). The single canonical waiting-count derivation
    /// for monitors, so the deferred index can never be silently omitted.
    pub fn waiting_job_counts(&self, now: u64) -> BTreeMap<String, u64> {
        let mut counts: BTreeMap<String, u64> = self
            .activatable_jobs
            .iter()
            .filter(|(_, set)| !set.is_empty())
            .map(|(job_type, set)| (job_type.clone(), set.len() as u64))
            .collect();
        for &(until, key) in &self.deferred_jobs {
            if until > now {
                break;
            }
            if let Some(job) = self.jobs.get(&key) {
                *counts.entry(job.job_type.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Removes `key` from all three job indices (the activatable set under
    /// `job_type`, the deferred set, and the activated set), pruning an emptied
    /// per-type set. Used when a job is dropped from `jobs` entirely (eviction),
    /// where [`resync_job_index`] cannot run because the job is already gone.
    /// `priority` is the dropped job's priority, needed to locate its ordering
    /// slot.
    pub fn deindex_job(&mut self, job_type: &str, key: Key, priority: i32) {
        if let Some(set) = self.activatable_jobs.get_mut(job_type) {
            set.remove(&activation_order(priority, key));
            if set.is_empty() {
                self.activatable_jobs.remove(job_type);
            }
        }
        // The job is gone from `jobs`, so its exact deferred slot may be
        // unknown (a prior transition could have cleared the deadline); remove
        // by key to be safe.
        remove_deferred_job(self, key);
        self.activated_jobs.remove(&key);
    }
}

/// Reconstructs the element instances a legacy snapshot leaves ACTIVATING with
/// their `ElementActivated` still owed (#1429). The `activating` set is
/// `serde(default)`, so a snapshot written before it existed deserializes it
/// empty — but such a snapshot can still carry a token parked in ACTIVATING on
/// an activation-time incident (a split gateway's routing failure, an input /
/// job-property expression failure before `ElementActivated`). That owed state
/// survives only implicitly, as the incident's [`IoMappingRedrive::ActivationPending`]
/// / [`IoMappingRedrive::ActivationBodyPending`] tag. Without reconstruction the
/// resolved re-drive defers the marker to the start-listener drain, which sees
/// no pending key and completes the element without ever emitting it. Idempotent:
/// keys already in the set are re-inserted, and a resolved incident contributes
/// nothing.
pub(crate) fn reconstruct_owed_activations<'a>(
    instance: &mut ProcessInstance,
    incidents: impl IntoIterator<Item = &'a Incident>,
) {
    for incident in incidents {
        if incident.instance_key == instance.key {
            if let Some(element_key) = incident.owed_activation() {
                instance.activating.insert(element_key);
            }
        }
    }
}

/// Re-inserts a lifted-out [`InstanceSnapshot`] into `state`: the instance and
/// every owned entity, rebuilding the derived job indices. The single canonical
/// implementation behind both live cold rehydration
/// ([`Engine::rehydrate_instance`](crate::Engine::rehydrate_instance)) and the
/// host's folding of cold instances into a self-contained snapshot, so the two
/// can never drift. Re-inserting keys that already exist overwrites them.
pub fn restore_instance_snapshot(state: &mut State, snapshot: InstanceSnapshot) {
    let InstanceSnapshot {
        mut instance,
        jobs,
        timers,
        message_subscriptions,
        signal_subscriptions,
        conditional_subscriptions,
        user_tasks,
        incidents,
    } = snapshot;
    reconstruct_owed_activations(&mut instance, &incidents);
    instance.resolve_legacy_join_scopes();
    let key = instance.key;
    state.insert_instance(instance);
    for job in jobs {
        let job_key = job.key;
        state
            .jobs_by_instance
            .entry(key)
            .or_default()
            .insert(job_key);
        state.jobs.insert(job_key, job);
        resync_job_index(state, job_key);
    }
    for timer in timers {
        state.timers.insert(timer.key, timer);
    }
    for sub in message_subscriptions {
        state.message_subscriptions.insert(sub.key, sub);
    }
    for sub in signal_subscriptions {
        state.signal_subscriptions.insert(sub.key, sub);
    }
    for sub in conditional_subscriptions {
        state.conditional_subscriptions.insert(sub.key, sub);
    }
    for task in user_tasks {
        state.user_tasks.insert(task.key, task);
    }
    for incident in incidents {
        state.incidents.insert(incident.key, incident);
    }
    reconcile_incident_index(state, key);
}

/// Folds the ACTIVE incident raised while `instance_key` was cold into that
/// instance's `incidents` index (#1574 review). The only incident that can be
/// raised against a non-resident instance is the off-heap-orphan
/// `VariablesLost` quarantine, and it is always recorded in the replicated
/// tombstone [`State::variables_lost_quarantined`] (instance → incident). That
/// map is the O(1) instance-keyed lookup, so restore never scans the
/// partition-wide incident map (which would make bulk snapshot loading
/// quadratic). Restoring the stale cold row alone would leave the incident
/// unindexed and a later termination would resolve it only on peers that held
/// the instance resident — diverging the event log.
fn reconcile_incident_index(state: &mut State, instance_key: Key) {
    let Some(&tombstone) = state.variables_lost_quarantined.get(&instance_key) else {
        return;
    };
    let incident_key = tombstone.incident_key;
    let Some(incident) = state.incidents.get(&incident_key) else {
        return;
    };
    if incident.instance_key != instance_key || incident.state != IncidentState::Active {
        return;
    }
    let owed = incident.owed_activation();
    if let Some(instance) = state.instances.get_mut(&instance_key) {
        if instance.incidents.contains(&incident_key) {
            return;
        }
        instance.incidents.push(incident_key);
        instance.activating.extend(owed);
    }
}

/// Re-syncs the index membership of one job to match its current state.
/// Idempotent: a `Created` job is in the activatable index, an `Activated` job
/// is in the activated index, and anything else (or a missing job) is removed
/// from both. Call after any arm that changes a job's state. Keeping membership
/// a pure function of the job's current state means the indices can never drift
/// from `jobs`.
///
/// `Activated` jobs are deliberately *not* kept in the activatable index: a job
/// whose lock expires is returned to `Created` by [`Event::JobLockExpired`]
/// (driven by the host's periodic tick), which re-adds it here. Excluding locked
/// jobs keeps `activate_jobs`'s walk O(`max_jobs`) instead of O(in-flight
/// backlog) — without it, a large pool of locked jobs (lower keys) is rescanned
/// and skipped on every poll, which degrades into a throughput death spiral once
/// a backlog builds.
pub(crate) fn resync_job_index(state: &mut State, job_key: Key) {
    let Some(job) = state.jobs.get(&job_key) else {
        return;
    };
    let job_type = job.job_type.clone();
    let job_state = job.state;
    let order = activation_order(job.priority, job_key);
    let backoff_until = job.retry_backoff_until;
    // A `Created` job with a pending backoff is deferred, not activatable: it
    // leaves the hot ready set and is indexed by its eligibility deadline so a
    // poll promotes only the due prefix instead of scanning the delayed prefix.
    let deferred = job_state == JobState::Created && backoff_until.is_some();
    if job_state == JobState::Created && !deferred {
        state
            .activatable_jobs
            .entry(job_type)
            .or_default()
            .insert(order);
    } else if let Some(set) = state.activatable_jobs.get_mut(&job_type) {
        set.remove(&order);
        if set.is_empty() {
            state.activatable_jobs.remove(&job_type);
        }
    }
    if deferred {
        let until = backoff_until.unwrap_or(0);
        if let Some(old) = state.deferred_job_deadlines.insert(job_key, until) {
            state.deferred_jobs.remove(&(old, job_key));
        }
        state.deferred_jobs.insert((until, job_key));
    } else {
        remove_deferred_job(state, job_key);
    }
    if job_state == JobState::Activated {
        state.activated_jobs.insert(job_key);
    } else {
        state.activated_jobs.remove(&job_key);
    }
}

/// Removes `job_key` from the deferred index whatever its (possibly already
/// cleared) deadline slot. A transition out of deferred (activation, eviction,
/// terminal settle) may clear `retry_backoff_until` before re-syncing, so the
/// exact slot is looked up in the [`State::deferred_job_deadlines`] reverse
/// index — O(log n), never a scan of the deferred backlog.
fn remove_deferred_job(state: &mut State, job_key: Key) {
    if let Some(until) = state.deferred_job_deadlines.remove(&job_key) {
        state.deferred_jobs.remove(&(until, job_key));
    }
}

#[cfg(test)]
mod deferred_index_tests {
    use super::{resync_job_index, JobState, State};

    #[test]
    fn deferred_jobs_are_live_and_waiting_once_due() {
        let state = state_with_deferred_job(7, 1_000);
        let job_type = state.jobs[&7].job_type.clone();
        assert_eq!(state.live_job_count(), 1, "a deferred job is live");
        assert_eq!(
            state.waiting_job_counts(999).get(&job_type),
            None,
            "not waiting before its deadline"
        );
        assert_eq!(
            state.waiting_job_counts(1_000).get(&job_type),
            Some(&1),
            "waiting (counted once) once its deadline is reached"
        );
    }

    fn state_with_deferred_job(key: u64, until: u64) -> State {
        let mut job = super::job_attribution_worker_tests::job_with_worker(None);
        job.key = key;
        job.state = JobState::Created;
        job.retry_backoff_until = Some(until);
        let mut state = State::default();
        state.jobs.insert(key, job);
        resync_job_index(&mut state, key);
        state
    }

    /// Re-deferring a job with a new deadline replaces its slot instead of
    /// leaving a stale `(old, key)` tuple behind (#933 review).
    #[test]
    fn re_deferring_a_job_replaces_its_deadline_slot() {
        let mut state = state_with_deferred_job(7, 1_000);
        for until in [2_000, 500, 9_000] {
            if let Some(job) = state.jobs.get_mut(&7) {
                job.retry_backoff_until = Some(until);
            }
            resync_job_index(&mut state, 7);
            assert_eq!(
                state.deferred_jobs.iter().copied().collect::<Vec<_>>(),
                vec![(until, 7)]
            );
            assert_eq!(state.deferred_job_deadlines.get(&7), Some(&until));
        }
    }

    /// Leaving deferral (even with the deadline already cleared) removes the
    /// exact slot via the reverse index, for every job in a large backlog.
    #[test]
    fn undeferring_removes_the_exact_slot_in_a_large_backlog() {
        let mut state = State::default();
        for key in 1..=2_000u64 {
            let mut job = super::job_attribution_worker_tests::job_with_worker(None);
            job.key = key;
            job.state = JobState::Created;
            job.retry_backoff_until = Some(10_000 - key);
            state.jobs.insert(key, job);
            resync_job_index(&mut state, key);
        }
        assert_eq!(state.deferred_jobs.len(), 2_000);
        for key in 1..=2_000u64 {
            if let Some(job) = state.jobs.get_mut(&key) {
                job.retry_backoff_until = None;
            }
            resync_job_index(&mut state, key);
        }
        assert!(state.deferred_jobs.is_empty());
        assert!(state.deferred_job_deadlines.is_empty());
    }
}

#[cfg(test)]
mod job_attribution_worker_tests {
    use super::{Job, JobKind, JobState};

    pub(super) fn job_with_worker(worker: Option<&str>) -> Job {
        Job {
            key: 1,
            instance_key: 2,
            element_instance_key: 3,
            element_id: "t".to_string(),
            job_type: "review-round".to_string(),
            state: JobState::Activated,
            worker: worker.map(str::to_string),
            deadline: Some(60_000),
            activated_at: Some(1),
            activation_timeout: Some(60_000),
            lease_token: None,
            durable_activation: false,
            activated: true,
            retries: 3,
            priority: 0,
            created_at: 1,
            kind: JobKind::BpmnElement,
            error_message: None,
            error_code: None,
            has_failed_with_retries_left: false,
            retry_backoff_until: None,
            settled_tokens: Vec::new(),
            terminal_end: None,
            secret_provenance: Vec::new(),
        }
    }

    /// #1640: a job's mapping-time secret provenance survives the snapshot
    /// round-trip (paths + names only), and a pre-#1640 job record without
    /// the field decodes to EMPTY provenance (resolve nothing — fail closed).
    #[cfg(feature = "serde")]
    #[test]
    fn secret_provenance_snapshot_round_trip() {
        let mut job = job_with_worker(Some("w"));
        job.secret_provenance = vec![super::SecretProvenance {
            path: "payload.auth".to_string(),
            secrets: vec!["TOKEN".to_string()],
        }];
        let json = serde_json::to_string(&job).expect("serialize");
        assert!(json.contains("\"secret_provenance\""));
        let back: Job = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, job);

        let mut legacy: serde_json::Value = serde_json::from_str(&json).expect("json");
        legacy
            .as_object_mut()
            .expect("object")
            .remove("secret_provenance");
        let old: Job = serde_json::from_value(legacy).expect("legacy record decodes");
        assert!(old.secret_provenance.is_empty());
    }

    /// #1191 — the single-source capture helper the three terminal sites use must
    /// treat an *empty* worker as no attribution. The `JobActivated` reducer
    /// normalizes a fresh activation, but a job **restored from a snapshot written
    /// before that normalization** can still hold `Some("")`; capturing it
    /// verbatim would stamp an empty attribution onto the terminal event. The
    /// helper closes that gap: `Some("")` → `None`, while a genuine worker and a
    /// truly-absent worker pass through unchanged.
    #[test]
    fn empty_worker_normalizes_to_none_but_real_and_absent_pass_through() {
        assert_eq!(job_with_worker(Some("")).attribution_worker(), None);
        assert_eq!(
            job_with_worker(Some("w1")).attribution_worker(),
            Some("w1".to_string())
        );
        assert_eq!(job_with_worker(None).attribution_worker(), None);
    }
}

#[cfg(test)]
mod placement_tests {
    use super::{stable_hash, subscription_partition};

    #[test]
    fn stable_hash_is_deterministic_and_input_sensitive() {
        assert_eq!(stable_hash(b"order-42"), stable_hash(b"order-42"));
        assert_ne!(stable_hash(b"order-42"), stable_hash(b"order-43"));
        // FNV-1a offset basis for the empty input.
        assert_eq!(stable_hash(b""), 0xcbf2_9ce4_8422_2325);
    }

    #[test]
    fn single_partition_places_every_key_on_zero() {
        for key in ["", "a", "order-1", "customer-99"] {
            assert_eq!(subscription_partition(key, 1), 0);
        }
    }

    #[test]
    fn placement_is_stable_and_within_range() {
        let n = 4;
        for key in [
            "order-1",
            "order-2",
            "x",
            "really-long-correlation-key-value",
        ] {
            let p = subscription_partition(key, n);
            assert!(p < n);
            // Stable across calls.
            assert_eq!(p, subscription_partition(key, n));
        }
    }

    #[test]
    fn placement_spreads_across_partitions() {
        let n = 4;
        let mut seen = [0u32; 4];
        for i in 0..1000 {
            let key = format!("correlation-{i}");
            seen[subscription_partition(&key, n) as usize] += 1;
        }
        // Every partition gets a non-trivial share (no degenerate hashing).
        for count in seen {
            assert!(count > 150, "uneven placement: {seen:?}");
        }
    }
}

#[cfg(test)]
mod version_lookup_tests {
    use super::{DeployedProcess, State};
    use crate::model::ProcessBuilder;

    fn deployed(id: &str, key: u64, version: i32) -> DeployedProcess {
        let definition = ProcessBuilder::new(id)
            .start_event("start")
            .build()
            .expect("valid definition");
        DeployedProcess {
            key,
            version,
            deployment_key: 0,
            definition,
        }
    }

    /// A pre-retention snapshot deserializes `process_versions` empty (it is
    /// `serde(default)`), but `processes` still holds the latest deployed
    /// definition and its key. A by-key create for that key must still resolve.
    #[test]
    fn process_by_key_falls_back_to_latest_by_id_index_for_legacy_snapshots() {
        let mut state = State::default();
        let latest = deployed("order", 42, 3);
        state.processes.insert("order".to_string(), latest.clone());
        // process_versions is intentionally empty (legacy snapshot).
        assert!(state.process_versions.is_empty());

        let resolved = state
            .process_by_key(42)
            .expect("by-key resolves via fallback");
        assert_eq!(resolved.key, 42);
        assert_eq!(resolved.version, 3);
        // An unknown key is still absent.
        assert!(state.process_by_key(99).is_none());
    }

    /// By-id + version create for the *latest* version must resolve from the
    /// latest-by-id index when `process_versions` is empty (legacy snapshot);
    /// only that version is recoverable from such a snapshot.
    #[test]
    fn process_version_falls_back_to_latest_by_id_index_for_legacy_snapshots() {
        let mut state = State::default();
        let latest = deployed("order", 42, 3);
        state.processes.insert("order".to_string(), latest);
        assert!(state.process_versions.is_empty());

        let resolved = state
            .process_version("order", 3)
            .expect("latest version resolves via fallback");
        assert_eq!(resolved.version, 3);
        assert_eq!(resolved.key, 42);
        // A non-latest version is genuinely unrecoverable from a legacy snapshot.
        assert!(state.process_version("order", 2).is_none());
        assert!(state.process_version("order", 4).is_none());
    }

    /// When `process_versions` is populated it takes precedence and retains
    /// historical versions the latest-by-id index has overwritten.
    #[test]
    fn version_retention_map_takes_precedence_and_retains_history() {
        let mut state = State::default();
        let v1 = deployed("order", 10, 1);
        let v2 = deployed("order", 20, 2);
        state.processes.insert("order".to_string(), v2.clone());
        state.process_versions.insert(10, v1);
        state.process_versions.insert(20, v2);

        assert_eq!(state.process_by_key(10).expect("v1 retained").version, 1);
        assert_eq!(state.process_by_key(20).expect("v2 retained").version, 2);
        assert_eq!(state.process_version("order", 1).expect("v1").key, 10);
        assert_eq!(state.process_version("order", 2).expect("v2").key, 20);
    }
}

#[cfg(test)]
mod flow_arrivals_tests {
    use super::*;

    fn flow(from: &str, ordinal: usize) -> IncomingFlow {
        IncomingFlow {
            from: from.into(),
            ordinal,
        }
    }

    #[test]
    fn remap_merges_flows_that_collapse_onto_one_identity() {
        let mut arrivals = FlowArrivals::default();
        arrivals.add(&flow("a", 0), 2);
        arrivals.add(&flow("b", 0), 3);
        arrivals.record(&flow("c", 0));
        arrivals.remap(|id| {
            if id == "b" {
                *id = "a".into();
            }
        });
        assert_eq!(arrivals.count(&flow("a", 0)), 5);
        assert_eq!(arrivals.count(&flow("c", 0)), 1);
        assert_eq!(arrivals.distinct_flows(), 2);
    }

    #[test]
    fn consume_one_each_keeps_only_the_surplus() {
        let mut arrivals = FlowArrivals::default();
        arrivals.add(&flow("a", 0), 2);
        arrivals.record(&flow("a", 1));
        arrivals.add(&flow("b", 0), 0);
        assert_eq!(
            arrivals.distinct_flows(),
            2,
            "a zero-token add records nothing"
        );
        arrivals.consume_one_each();
        assert_eq!(
            arrivals.iter().collect::<Vec<_>>(),
            vec![(&flow("a", 0), 1)]
        );
    }
}
