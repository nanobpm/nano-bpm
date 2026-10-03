//! The slim resident routing index for cold-spilled instances.
//!
//! When [`crate::Journal`] cold-spills a dormant instance it lifts the whole
//! instance — its `active`/`scopes`/`join_*` maps and every job, timer, message
//! subscription, user task and incident it owns — out of the engine and into the
//! disk-backed store, keeping only what is needed to *route a future event back
//! to it*: the keys and match criteria recorded here.
//!
//! This index is deliberately tiny. Per cold instance it holds a handful of
//! `u64` keys and (for messages) two short strings — never the variables or the
//! control-state maps. So a backlog of 100k dormant instances costs a few MB of
//! index instead of the hundreds of MB their live state would, and any targeting
//! event (a worker activating its job type, a correlated message, a due timer, or
//! a direct command by key) is matched here first and the owning instance
//! rehydrated on demand before the engine processes it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use nanobpmn_engine_core::{
    InstanceSnapshot, JobState, Key, MessageSubscriptionState, TimerState, UserTaskState,
};

/// The routing facets of one cold instance, retained so its reverse-index
/// entries can be removed wholesale when it is rehydrated.
#[derive(Default)]
struct ColdFacets {
    /// Keys of every job the instance owns (any state), for key-targeted
    /// commands (`CompleteJob`, `FailJob`, `UpdateJobRetries`, ...).
    jobs: Vec<Key>,
    /// Job type of each *activatable* (`Created`) job, for worker activation.
    activatable_job_types: Vec<String>,
    /// (message name, correlation key) of each *open* subscription, for
    /// `CorrelateMessage`.
    messages: Vec<(String, String)>,
    /// `due_at` of each *armed* (`Created`) timer, for `TriggerTimers`.
    timers: Vec<u64>,
    /// Keys of every user task the instance owns, for user-task commands.
    user_tasks: Vec<Key>,
    /// Keys of every incident the instance owns, for `ResolveIncident`.
    incidents: Vec<Key>,
    /// Active element-instance (token) scope keys, for `SetVariables` targeting a
    /// local scope rather than the instance root.
    scopes: Vec<Key>,
}

/// A resident index from routing keys to the cold instance that owns them.
#[derive(Default)]
pub struct ColdIndex {
    /// Ordered by key so [`ColdIndex::retire_below`] can range-scan the instances
    /// strictly below the low-water mark in O(log n + victims) instead of scanning
    /// the whole index every retirement tick (see that method's note).
    instances: BTreeMap<Key, ColdFacets>,
    by_job: HashMap<Key, Key>,
    by_job_type: HashMap<String, BTreeSet<Key>>,
    by_message: HashMap<(String, String), HashSet<Key>>,
    by_timer: BTreeMap<u64, HashSet<Key>>,
    by_user_task: HashMap<Key, Key>,
    by_incident: HashMap<Key, Key>,
    by_scope: HashMap<Key, Key>,
}

impl ColdIndex {
    /// Records the routing facets of a freshly cold-spilled `snapshot`.
    pub fn insert(&mut self, snapshot: &InstanceSnapshot) {
        let key = snapshot.instance.key;
        let mut facets = ColdFacets::default();

        for job in &snapshot.jobs {
            self.by_job.insert(job.key, key);
            facets.jobs.push(job.key);
            if matches!(job.state, JobState::Created | JobState::Activated) {
                self.by_job_type
                    .entry(job.job_type.clone())
                    .or_default()
                    .insert(key);
                facets.activatable_job_types.push(job.job_type.clone());
            }
        }
        for sub in &snapshot.message_subscriptions {
            if sub.state == MessageSubscriptionState::Open {
                let id = (sub.message_name.clone(), sub.correlation_key.clone());
                self.by_message.entry(id.clone()).or_default().insert(key);
                facets.messages.push(id);
            }
        }
        for timer in &snapshot.timers {
            if timer.state == TimerState::Created {
                self.by_timer.entry(timer.due_at).or_default().insert(key);
                facets.timers.push(timer.due_at);
            }
        }
        for task in &snapshot.user_tasks {
            if task.state == UserTaskState::Created {
                self.by_user_task.insert(task.key, key);
                facets.user_tasks.push(task.key);
            }
        }
        for incident in &snapshot.incidents {
            self.by_incident.insert(incident.key, key);
            facets.incidents.push(incident.key);
        }
        for &scope in snapshot.instance.active.keys() {
            self.by_scope.insert(scope, key);
            facets.scopes.push(scope);
        }

        self.instances.insert(key, facets);
    }

    /// Forgets every entry for `key` (called when the instance is rehydrated).
    pub fn remove(&mut self, key: Key) {
        let Some(facets) = self.instances.remove(&key) else {
            return;
        };
        for job in facets.jobs {
            self.by_job.remove(&job);
        }
        for job_type in facets.activatable_job_types {
            if let Some(set) = self.by_job_type.get_mut(&job_type) {
                set.remove(&key);
                if set.is_empty() {
                    self.by_job_type.remove(&job_type);
                }
            }
        }
        for id in facets.messages {
            if let Some(set) = self.by_message.get_mut(&id) {
                set.remove(&key);
                if set.is_empty() {
                    self.by_message.remove(&id);
                }
            }
        }
        for due in facets.timers {
            if let Some(set) = self.by_timer.get_mut(&due) {
                set.remove(&key);
                if set.is_empty() {
                    self.by_timer.remove(&due);
                }
            }
        }
        for task in facets.user_tasks {
            self.by_user_task.remove(&task);
        }
        for incident in facets.incidents {
            self.by_incident.remove(&incident);
        }
        for scope in facets.scopes {
            self.by_scope.remove(&scope);
        }
    }

    /// Whether `key` is a cold instance.
    pub fn contains(&self, key: Key) -> bool {
        self.instances.contains_key(&key)
    }

    /// How many instances are currently cold.
    // `pub(crate)`: consumed only by `journal` (same crate); keeping it off the
    // public API also avoids the `len_without_is_empty` clippy lint without
    // growing an `is_empty` this index never needs.
    pub(crate) fn len(&self) -> usize {
        self.instances.len()
    }

    /// Iterator over every cold instance key. Used by reconciliation to include
    /// cold-spilled (still-running) instances in the engine's live-instance set,
    /// so they are never mistaken for orphaned read rows.
    pub fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.instances.keys().copied()
    }

    /// Removes up to `max_remove` cold instances whose key is strictly below
    /// `low_water`, returning them so the host can `forget` their spilled rows.
    ///
    /// The cold-tier counterpart of [`Engine::retire_below`](nanobpmn_engine_core::Engine::retire_below):
    /// a follower's low-water backstop reaps *hot* replica shells below the owner's
    /// mark, but cold-spilled replica instances are invisible to the engine, so
    /// without this sweep a cold instance whose best-effort per-key retirement
    /// digest frame was dropped would leak its `cold` row (and this index entry)
    /// forever — the unbounded var-spill growth observed after a soak. Bounded per
    /// call to match the engine sweep's budget so a large backlog drains across
    /// ticks without stalling the actor.
    ///
    /// `instances` is a [`BTreeMap`], so this range-scans the keys strictly below
    /// the mark in ascending order and stops after `max_remove` victims (or at the
    /// first key `>= low_water`) — O(log n + victims), and an O(log n) no-op in the
    /// steady state where nothing is below the mark. It must NOT scan the whole
    /// index: it runs on the single-writer replica actor every retirement tick, so
    /// a full O(n) sweep over a multi-million-row cold tier stalls live replication
    /// and inflates raft-fsync latency (the regression PR #287 originally shipped).
    pub fn retire_below(&mut self, low_water: Key, max_remove: usize) -> Vec<Key> {
        if self.instances.is_empty() || max_remove == 0 {
            return Vec::new();
        }
        let victims: Vec<Key> = self
            .instances
            .range(..low_water)
            .map(|(&k, _)| k)
            .take(max_remove)
            .collect();
        for &key in &victims {
            self.remove(key);
        }
        victims
    }

    /// The cold instance owning `job_key`, if any.
    pub fn instance_for_job(&self, job_key: Key) -> Option<Key> {
        self.by_job.get(&job_key).copied()
    }

    /// The cold instance owning `user_task_key`, if any.
    pub fn instance_for_user_task(&self, user_task_key: Key) -> Option<Key> {
        self.by_user_task.get(&user_task_key).copied()
    }

    /// The cold instance owning `incident_key`, if any.
    pub fn instance_for_incident(&self, incident_key: Key) -> Option<Key> {
        self.by_incident.get(&incident_key).copied()
    }

    /// The cold instance owning element-instance scope `scope_key`, if any — for
    /// a `SetVariables` addressing a local (token) scope rather than the instance
    /// root.
    pub fn instance_for_scope(&self, scope_key: Key) -> Option<Key> {
        self.by_scope.get(&scope_key).copied()
    }

    /// Up to `limit` cold instances with an activatable job of `job_type`, oldest
    /// (lowest key) first — the ones a worker poll for that type must rehydrate.
    pub fn instances_for_job_type(&self, job_type: &str, limit: usize) -> Vec<Key> {
        match self.by_job_type.get(job_type) {
            Some(set) => set.iter().take(limit).copied().collect(),
            None => Vec::new(),
        }
    }

    /// Cold instances with an open subscription matching `(message_name,
    /// correlation_key)` — the ones a `CorrelateMessage` must rehydrate.
    pub fn instances_for_message(&self, message_name: &str, correlation_key: &str) -> Vec<Key> {
        let id = (message_name.to_string(), correlation_key.to_string());
        match self.by_message.get(&id) {
            Some(set) => set.iter().copied().collect(),
            None => Vec::new(),
        }
    }

    /// Cold instances holding an armed timer due at or before `now` — the ones a
    /// `TriggerTimers(now)` must rehydrate so the tick can fire them.
    pub fn instances_due(&self, now: u64) -> Vec<Key> {
        let mut due = HashSet::new();
        for (_, set) in self.by_timer.range(..=now) {
            due.extend(set.iter().copied());
        }
        due.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nanobpmn_engine_core::{
        Job, MessageSubscription, MessageSubscriptionKind, ProcessInstance, ProcessInstanceState,
        Timer, TimerKind,
    };

    use super::*;

    fn instance(key: Key) -> ProcessInstance {
        ProcessInstance {
            key,
            process_id: "p".into(),
            process_definition_key: 0,
            state: ProcessInstanceState::Active,
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
            suspended_at: None,
            active: HashMap::new(),
            scopes: HashMap::new(),
            variables: Arc::new(HashMap::new()),
            join_counts: HashMap::new(),
            join_flow_arrivals: HashMap::new(),
            join_instances: HashMap::new(),
            incidents: Vec::new(),
            variables_spilled: false,
            multi_instances: HashMap::new(),
            adhoc_instances: HashMap::new(),
            scope_parents: HashMap::new(),
            scope_variables: HashMap::new(),
            compensable: Vec::new(),
            compensation_waits: HashMap::new(),
            agent_instances: HashMap::new(),
            agent_history: HashMap::new(),
        }
    }

    fn job(key: Key, instance_key: Key, job_type: &str) -> Job {
        Job {
            key,
            instance_key,
            element_instance_key: 0,
            element_id: "t".into(),
            job_type: job_type.into(),
            state: JobState::Created,
            worker: None,
            deadline: None,
            activated_at: None,
            activation_timeout: None,
            activated: false,
            lease_token: None,
            durable_activation: false,
            retries: 3,
            priority: nanobpmn_engine_core::DEFAULT_JOB_PRIORITY,
            created_at: 0,
            kind: nanobpmn_engine_core::JobKind::BpmnElement,
            error_message: None,
            error_code: None,
            has_failed_with_retries_left: false,
        }
    }

    fn snapshot_with_job(instance_key: Key, job_key: Key, job_type: &str) -> InstanceSnapshot {
        InstanceSnapshot {
            instance: instance(instance_key),
            jobs: vec![job(job_key, instance_key, job_type)],
            timers: Vec::new(),
            message_subscriptions: Vec::new(),
            signal_subscriptions: Vec::new(),
            conditional_subscriptions: Vec::new(),
            user_tasks: Vec::new(),
            incidents: Vec::new(),
        }
    }

    #[test]
    fn routes_jobs_and_job_types_then_clears_on_remove() {
        let mut idx = ColdIndex::default();
        idx.insert(&snapshot_with_job(100, 101, "payment"));
        idx.insert(&snapshot_with_job(200, 201, "payment"));

        assert_eq!(idx.instance_for_job(101), Some(100));
        assert_eq!(idx.instance_for_job(201), Some(200));
        // Oldest-first, limit-respecting.
        assert_eq!(idx.instances_for_job_type("payment", 1), vec![100]);
        assert_eq!(idx.instances_for_job_type("payment", 10), vec![100, 200]);
        assert!(idx.instances_for_job_type("other", 10).is_empty());

        idx.remove(100);
        assert_eq!(idx.instance_for_job(101), None);
        assert_eq!(idx.instances_for_job_type("payment", 10), vec![200]);
        idx.remove(200);
        assert!(idx.instances_for_job_type("payment", 10).is_empty());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn routes_messages_and_due_timers() {
        let mut idx = ColdIndex::default();
        let mut snap = snapshot_with_job(300, 301, "work");
        snap.message_subscriptions.push(MessageSubscription {
            key: 310,
            instance_key: 300,
            element_instance_key: 0,
            element_id: "m".into(),
            message_name: "approve".into(),
            correlation_key: "A".into(),
            state: MessageSubscriptionState::Open,
            kind: MessageSubscriptionKind::IntermediateCatch,
        });
        snap.timers.push(Timer {
            key: 320,
            instance_key: 300,
            element_instance_key: 0,
            element_id: "t".into(),
            due_at: 5_000,
            state: TimerState::Created,
            kind: TimerKind::IntermediateCatch,
        });
        idx.insert(&snap);

        assert_eq!(idx.instances_for_message("approve", "A"), vec![300]);
        assert!(idx.instances_for_message("approve", "B").is_empty());
        assert!(idx.instances_due(4_999).is_empty(), "not yet due");
        assert_eq!(idx.instances_due(5_000), vec![300], "due at boundary");
        assert!(idx.contains(300));

        idx.remove(300);
        assert!(idx.instances_for_message("approve", "A").is_empty());
        assert!(idx.instances_due(10_000).is_empty());
        assert!(!idx.contains(300));
    }

    #[test]
    fn routes_element_instance_scopes_for_set_variables() {
        let mut idx = ColdIndex::default();
        let mut snap = snapshot_with_job(400, 401, "work");
        // an active token scope within the instance (the SetVariables target)
        snap.instance.active.insert(450, "task".into());
        idx.insert(&snap);

        // both the instance root and the element-instance scope resolve to it
        assert!(idx.contains(400));
        assert_eq!(idx.instance_for_scope(450), Some(400));
        assert_eq!(idx.instance_for_scope(999), None);

        idx.remove(400);
        assert_eq!(idx.instance_for_scope(450), None);
    }

    #[test]
    fn retire_below_reaps_cold_instances_under_the_mark_bounded() {
        let mut idx = ColdIndex::default();
        for key in [10u64, 20, 30, 40] {
            idx.insert(&snapshot_with_job(key, key + 1, "work"));
        }

        // Bounded: at most `max_remove` victims per call, all strictly below mark.
        let reaped = idx.retire_below(35, 2);
        assert_eq!(reaped.len(), 2, "capped to max_remove");
        assert!(
            reaped.iter().all(|&k| k < 35),
            "only keys below the mark are reaped"
        );
        // Reaped instances are fully deindexed (routing entries gone too).
        for &k in &reaped {
            assert!(!idx.contains(k));
            assert_eq!(idx.instance_for_job(k + 1), None);
        }

        // A second pass drains the remaining below-mark instance; 40 (>= 35) stays.
        let rest = idx.retire_below(35, 100);
        assert_eq!(rest.len(), 1, "one below-mark instance left");
        assert!(rest[0] < 35);
        assert!(idx.contains(40), "instance at/above the mark is retained");
        assert_eq!(idx.len(), 1);

        // Idempotent no-op once nothing below the mark remains.
        assert!(idx.retire_below(35, 100).is_empty());
        // Empty index / zero budget are cheap no-ops.
        assert!(idx.retire_below(0, 100).is_empty());
        assert!(idx.retire_below(1_000, 0).is_empty());
    }
}
