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

use std::collections::{BTreeMap, BTreeSet};

use nanobpmn_engine_core::{
    Command, InstanceSnapshot, JobState, Key, MessageSubscriptionState, TimerState, UserTaskState,
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
    /// The calling parent instance (call-activity child), for instance-wide
    /// teardown commands that must page in cold descendants.
    parent: Option<Key>,
}

/// A resident index from routing keys to the cold instance that owns them.
#[derive(Default)]
pub struct ColdIndex {
    /// Ordered by key for deterministic iteration.
    instances: BTreeMap<Key, ColdFacets>,
    by_job: BTreeMap<Key, Key>,
    by_job_type: BTreeMap<String, BTreeSet<Key>>,
    by_message: BTreeMap<(String, String), BTreeSet<Key>>,
    by_timer: BTreeMap<u64, BTreeSet<Key>>,
    by_user_task: BTreeMap<Key, Key>,
    by_incident: BTreeMap<Key, Key>,
    by_scope: BTreeMap<Key, Key>,
    by_parent: BTreeMap<Key, BTreeSet<Key>>,
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
        if let Some(parent) = snapshot.instance.parent_process_instance_key {
            self.by_parent.entry(parent).or_default().insert(key);
            facets.parent = Some(parent);
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
        if let Some(parent) = facets.parent
            && let Some(set) = self.by_parent.get_mut(&parent)
        {
            set.remove(&key);
            if set.is_empty() {
                self.by_parent.remove(&parent);
            }
        }
    }

    /// Cold instances called by `parent` (direct call-activity children),
    /// ascending.
    pub fn children_of(&self, parent: Key) -> Vec<Key> {
        self.by_parent
            .get(&parent)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// The cold instances `command` addresses by key (job, user task,
    /// incident, scope, instance) or by message correlation — the instances a
    /// replica must page in before applying it. The single routing table for
    /// both apply-time rehydration and proposal-time deadline stamping
    /// (#1856 review), so the two cannot drift. Batch commands that page in
    /// on their own terms (`ActivateJobs`, `TriggerTimers`) are not covered.
    pub fn command_targets(&self, command: &Command) -> Vec<Key> {
        let mut targets: Vec<Key> = Vec::new();
        match command {
            Command::CompleteJob { job_key, .. }
            | Command::FailJob { job_key, .. }
            | Command::ThrowJobError { job_key, .. }
            | Command::UpdateJobRetries { job_key, .. }
            | Command::UpdateJobTimeout { job_key, .. }
            | Command::UpdateJob { job_key, .. } => {
                targets.extend(self.instance_for_job(*job_key));
            }
            Command::ActivateJobsByKey { job_keys, .. } => {
                targets.extend(
                    job_keys
                        .iter()
                        .filter_map(|key| self.instance_for_job(*key)),
                );
            }
            Command::AssignUserTask { user_task_key, .. }
            | Command::UnassignUserTask { user_task_key }
            | Command::UpdateUserTask { user_task_key, .. }
            | Command::CompleteUserTask { user_task_key, .. } => {
                targets.extend(self.instance_for_user_task(*user_task_key));
            }
            Command::ResolveIncident { incident_key, .. } => {
                targets.extend(self.instance_for_incident(*incident_key));
            }
            Command::SetVariables { scope_key, .. } => {
                // The scope may be the instance root or one of its active
                // element-instance (token) scopes; both can address a cold
                // instance and must page it back in before the update applies.
                if self.contains(*scope_key) {
                    targets.push(*scope_key);
                } else {
                    targets.extend(self.instance_for_scope(*scope_key));
                }
            }
            // Every by-instance-key lifecycle command must page a cold instance
            // in: spilling removes a LIVE instance from resident engine state, so
            // without rehydration the engine would see it as absent and judge
            // the command against replica-local spill state (e.g. history
            // deletion, #1948, would accept a cold live instance it rejects
            // when resident).
            Command::CancelInstance { instance_key, .. }
            | Command::SuspendInstance { instance_key }
            | Command::ResumeInstance { instance_key }
            | Command::DeleteProcessInstanceHistory { instance_key } => {
                if self.contains(*instance_key) {
                    targets.push(*instance_key);
                }
            }
            Command::ModifyInstance { instance_key, .. }
            | Command::MigrateInstance { instance_key, .. } => {
                // Modify/migrate address a single instance (activating/terminating its
                // element instances); page it in before the command applies.
                if self.contains(*instance_key) {
                    targets.push(*instance_key);
                }
            }
            Command::CorrelateMessage {
                message_name,
                correlation_key,
                ..
            } => {
                targets.extend(self.instances_for_message(message_name, correlation_key));
            }
            _ => {}
        }
        targets
    }

    /// The calling parent of the cold instance `key`, if any.
    pub fn parent_of(&self, key: Key) -> Option<Key> {
        self.instances.get(&key).and_then(|facets| facets.parent)
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

    /// Up to `limit` cold instances with an activatable job of `job_type`,
    /// starting strictly after `after` and wrapping around (#1388), so a caller
    /// can rotate past a row it could not read.
    pub fn instances_for_job_type_rotated(
        &self,
        job_type: &str,
        after: Option<Key>,
        limit: usize,
    ) -> Vec<Key> {
        let Some(set) = self.by_job_type.get(job_type) else {
            return Vec::new();
        };
        match after {
            None => set.iter().take(limit).copied().collect(),
            Some(after) => set
                .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .chain(set.range(..=after))
                .take(limit)
                .copied()
                .collect(),
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
        let mut due = BTreeSet::new();
        for (_, set) in self.by_timer.range(..=now) {
            due.extend(set.iter().copied());
        }
        due.into_iter().collect()
    }

    /// Whether any cold instance holds an armed timer due at or before `now`.
    /// O(log n) — the Raft tick pre-check (#1483) runs this every tick, so it
    /// must not collect the way [`ColdIndex::instances_due`] does.
    pub fn has_due_timers(&self, now: u64) -> bool {
        self.by_timer
            .range(..=now)
            .next()
            .is_some_and(|(_, set)| !set.is_empty())
    }
}

/// The cold tier's call-activity links and command roots, for walks that must
/// cross cold instances (#1856 review).
impl nanobpmn_engine_core::ColdTopology for ColdIndex {
    fn parent_of(&self, key: Key) -> Option<Key> {
        ColdIndex::parent_of(self, key)
    }

    fn children_of(&self, parent: Key) -> Vec<Key> {
        ColdIndex::children_of(self, parent)
    }

    fn command_roots(&self, command: &Command) -> Vec<Key> {
        let mut roots = self.command_targets(command);
        if let Command::TriggerTimers { now } = command {
            roots.extend(self.instances_due(*now));
        }
        roots
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
            terminated_epoch: 0,
            active: BTreeMap::new(),
            scopes: BTreeMap::new(),
            variables: Arc::new(BTreeMap::new()),
            join_counts: BTreeMap::new(),
            join_flow_arrivals: BTreeMap::new(),
            join_instances: BTreeMap::new(),
            parked_joins: Default::default(),
            activating: Default::default(),
            gateway_routes: BTreeMap::new(),
            races: BTreeMap::new(),
            gateway_subscriptions: BTreeMap::new(),
            incidents: Vec::new(),
            variables_spilled: false,
            multi_instances: BTreeMap::new(),
            adhoc_instances: BTreeMap::new(),
            scope_parents: BTreeMap::new(),
            scope_variables: BTreeMap::new(),
            esp_held_payloads: BTreeMap::new(),
            compensable: Default::default(),
            compensation_waits: BTreeMap::new(),
            agent_instances: BTreeMap::new(),
            agent_history: BTreeMap::new(),
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
            retry_backoff_until: None,
            settled_tokens: Vec::new(),
            terminal_end: None,
            secret_provenance: Vec::new(),
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
        // Rotation (#1388): start strictly after the cursor, wrapping around,
        // so an unreadable oldest row cannot starve later instances.
        assert_eq!(
            idx.instances_for_job_type_rotated("payment", Some(100), 1),
            vec![200]
        );
        assert_eq!(
            idx.instances_for_job_type_rotated("payment", Some(100), 10),
            vec![200, 100]
        );
        assert_eq!(
            idx.instances_for_job_type_rotated("payment", Some(200), 1),
            vec![100]
        );
        assert_eq!(
            idx.instances_for_job_type_rotated("payment", None, 1),
            vec![100]
        );
        assert!(
            idx.instances_for_job_type_rotated("other", Some(1), 10)
                .is_empty()
        );

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
            correlated_message_keys: Default::default(),
        });
        snap.timers.push(Timer {
            key: 320,
            instance_key: 300,
            element_instance_key: 0,
            element_id: "t".into(),
            due_at: 5_000,
            state: TimerState::Created,
            kind: TimerKind::IntermediateCatch,
            repetitions: None,
        });
        idx.insert(&snap);

        assert_eq!(idx.instances_for_message("approve", "A"), vec![300]);
        assert!(idx.instances_for_message("approve", "B").is_empty());
        assert!(idx.instances_due(4_999).is_empty(), "not yet due");
        assert_eq!(idx.instances_due(5_000), vec![300], "due at boundary");
        // The O(log n) tick pre-check probe (#1483) agrees with the scan.
        assert!(!idx.has_due_timers(4_999), "not yet due");
        assert!(idx.has_due_timers(5_000), "due at boundary");
        assert!(idx.contains(300));

        idx.remove(300);
        assert!(idx.instances_for_message("approve", "A").is_empty());
        assert!(idx.instances_due(10_000).is_empty());
        assert!(!idx.has_due_timers(10_000));
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

    /// Every by-instance-key lifecycle command addressing a cold instance pages
    /// it in (#1948 review): a cold LIVE instance must be judged exactly as its
    /// resident equivalent, not as absent.
    #[test]
    fn routes_by_instance_lifecycle_commands_to_cold_instances() {
        let mut idx = ColdIndex::default();
        idx.insert(&snapshot_with_job(700, 701, "work"));
        for command in [
            Command::CancelInstance {
                instance_key: 700,
                terminate_suspended: true,
            },
            Command::SuspendInstance { instance_key: 700 },
            Command::ResumeInstance { instance_key: 700 },
            Command::DeleteProcessInstanceHistory { instance_key: 700 },
        ] {
            assert_eq!(idx.command_targets(&command), vec![700], "{command:?}");
        }
        assert!(
            idx.command_targets(&Command::DeleteProcessInstanceHistory { instance_key: 999 })
                .is_empty()
        );
    }
}
