//! `impl Engine` methods: api concern (extracted from the monolithic engine module).

use super::*;
use crate::json::write_json_string;

/// The custom-header key under which resolved [`crate::model::LinkedResource`]s are delivered
/// to the worker (Zeebe `linkedResources`). The value is a JSON array of
/// `{ resourceKey, resourceType, linkName }` objects.
const LINKED_RESOURCES_HEADER: &str = "linkedResources";

impl Engine {
    /// Looks up a process instance.
    pub fn instance(&self, key: Key) -> Option<&state::ProcessInstance> {
        self.state.instances.get(&key)
    }

    /// Returns `true` if the instance exists and has completed.
    pub fn is_completed(&self, key: Key) -> bool {
        matches!(
            self.state.instances.get(&key).map(|i| i.state),
            Some(ProcessInstanceState::Completed)
        )
    }

    /// Looks up a job.
    pub fn job(&self, key: Key) -> Option<&state::Job> {
        self.state.jobs.get(&key)
    }

    /// Looks up an incident by key, whether active or resolved (resolved records
    /// are retained for audit).
    pub fn incident(&self, key: Key) -> Option<&state::Incident> {
        self.state.incidents.get(&key)
    }

    /// All incidents ever raised, active and resolved (resolved records are
    /// retained as an audit trail). Filter by [`state::Incident::state`] for a
    /// specific lifecycle state.
    pub fn incidents(&self) -> Vec<&state::Incident> {
        self.state.incidents.values().collect()
    }

    /// Only the currently-active (open) incidents.
    pub fn active_incidents(&self) -> Vec<&state::Incident> {
        self.state
            .incidents
            .values()
            .filter(|i| i.state == state::IncidentState::Active)
            .collect()
    }

    /// All jobs currently awaiting activation (created, or with an expired lock).
    pub fn pending_jobs(&self) -> Vec<&state::Job> {
        self.state
            .jobs
            .values()
            .filter(|j| j.state == state::JobState::Created)
            .collect()
    }

    /// The currently-held soft locks as `(job_key, deadline)` pairs.
    /// Durable activations, including unleased ones, are excluded. Used by the
    /// best-effort **lease digest** (leader-local activation, `digest` mode): a
    /// partition leader periodically broadcasts these so a future leader can
    /// recover them on takeover ([`Engine::recover_lease`]) and honour the
    /// deadline before redelivering, instead of redelivering immediately. Pure
    /// read; the digest is soft state and is never journaled or replicated.
    pub fn activated_leases(&self) -> Vec<(Key, u64)> {
        self.state
            .jobs
            .values()
            .filter(|j| {
                j.state == state::JobState::Activated
                    && !self.job_requires_durable_activation(j.key)
            })
            .filter_map(|j| j.deadline.map(|d| (j.key, d)))
            .collect()
    }

    /// Recovers a soft activation lease from a digest: if `job_key` is currently
    /// [`state::JobState::Created`], requires no durable activation, and `deadline` is still in the future, marks
    /// it [`state::JobState::Activated`] until `deadline` under a synthetic worker.
    /// Returns `true` if a lease was set.
    ///
    /// This is a pure leader-local soft-state mutation — it emits and journals
    /// **nothing**, because the lease it restores was never replicated (that is
    /// the whole point of leader-local activation). A newly-promoted leader calls
    /// this for each lease in the last digest it received from the previous
    /// leader, so it holds redelivery of in-flight jobs until their original
    /// deadline (the normal leader-local `expire_jobs` tick then reclaims them)
    /// rather than redelivering the instant it takes over. Idempotent: a job that
    /// is already activated (e.g. re-leased by this leader) is left untouched.
    pub fn recover_lease(&mut self, job_key: Key, deadline: u64, now: u64) -> bool {
        if self.job_requires_durable_activation(job_key) {
            return false;
        }
        if let Some(job) = self.state.jobs.get_mut(&job_key) {
            if job.state == state::JobState::Created && job.lease_token.is_none() && deadline > now
            {
                job.state = state::JobState::Activated;
                job.worker = Some(LEASE_DIGEST_WORKER.to_string());
                job.deadline = Some(deadline);
                job.activated = true;
                self.state.activated_jobs.insert(job_key);
                return true;
            }
        }
        false
    }

    /// Whether this job's element supports agent registration, using the same
    /// model eligibility check as CREATE. Hosts use this to keep agent activation
    /// context durable even when no lease token is requested.
    pub fn job_supports_agent_instance(&self, job_key: Key) -> bool {
        self.state.jobs.get(&job_key).is_some_and(|job| {
            self.agent_type_for_element(job.instance_key, &job.element_id, job.element_instance_key)
                .is_ok()
        })
    }

    /// The shared host routing, expiry and recovery boundary. An authoritative
    /// batch can include ordinary unleased jobs alongside agent jobs.
    pub fn job_requires_durable_activation(&self, job_key: Key) -> bool {
        self.state.jobs.get(&job_key).is_some_and(|job| {
            job.durable_activation
                || job.lease_token.is_some()
                || self.job_supports_agent_instance(job_key)
        })
    }

    /// Side-effect-free activation planning in priority-descending, key-ascending
    /// order. The host must serialize selection through replicated plan commit
    /// against every local activation; followers must apply the planned keys,
    /// never reselect from their potentially different soft-lock state.
    pub fn select_activatable_job_keys(
        &self,
        job_type: &str,
        max_jobs: usize,
        now: u64,
        with_lease: bool,
    ) -> Vec<Key> {
        self.state
            .activatable_jobs
            .get(job_type)
            .into_iter()
            .flat_map(|set| set.iter())
            .map(|&(_, key)| key)
            .filter(|key| {
                self.state.jobs.get(key).is_some_and(|job| {
                    super::job_activatable(job, now)
                        && (with_lease || job.lease_token.is_none())
                        // A suspended instance makes no progress: its jobs are
                        // not activatable until it is resumed (Camunda parity).
                        && !self.instance_is_suspended(job.instance_key)
                })
            })
            .take(max_jobs)
            .collect()
    }

    /// Activates up to `max_jobs` activatable jobs of `job_type` for `worker`,
    /// locking each until `now + timeout`, and returns them — the **pull** worker
    /// API for embedded use (no polling, no network). `now` is a caller-supplied
    /// logical instant. Equivalent to applying a [`Command::ActivateJobs`] and
    /// reading back the activated jobs.
    pub fn activate_jobs(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Vec<ActivatedJob> {
        self.activate_jobs_with_fetch(job_type, worker, max_jobs, timeout, now, Vec::new())
    }

    /// Like [`Engine::activate_jobs`] but records the declared read-set
    /// (`fetchVariables`) on each resulting [`Event::JobActivated`] — the
    /// engine-native read provenance for reification. An empty `fetch_variables`
    /// is exactly [`Engine::activate_jobs`] (fetch-all / undeclared reads), and
    /// keeps the activation event byte-identical.
    pub fn activate_jobs_with_fetch(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        fetch_variables: Vec<String>,
    ) -> Vec<ActivatedJob> {
        self.activate_jobs_with_options(
            job_type,
            worker,
            max_jobs,
            timeout,
            now,
            crate::JobActivationOptions {
                fetch_variables,
                with_lease: false,
            },
        )
    }

    /// Activate any job kind with explicit worker leasing options.
    #[allow(
        clippy::expect_used,
        reason = "ActivateJobs is an internal command that cannot fail"
    )]
    pub fn activate_jobs_with_options(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        options: crate::JobActivationOptions,
    ) -> Vec<ActivatedJob> {
        let events = self
            .apply_command_at(
                Command::activate_jobs_with_options(
                    job_type, worker, max_jobs, timeout, now, options,
                ),
                now,
            )
            .expect("ActivateJobs never fails");
        events
            .iter()
            .filter_map(|e| match e {
                Event::JobActivated {
                    job_key,
                    worker,
                    deadline,
                    ..
                } => self
                    .job(*job_key)
                    .map(|job| self.project_activated_job(job, worker.clone(), *deadline)),
                _ => None,
            })
            .collect()
    }

    /// Projects an already-activated job into an [`ActivatedJob`] snapshot (job
    /// detail + a shared copy of its instance variables) WITHOUT mutating state.
    /// Returns `None` if the key is unknown. Used by the Raft write path, where
    /// activation is applied through the replicated log (so the lock replicates
    /// to followers) and the projection for the worker happens in a separate read
    /// on the leader. Mirrors the projection inside [`Engine::activate_jobs`].
    pub fn activated_job(&self, job_key: Key) -> Option<ActivatedJob> {
        self.job(job_key).map(|job| {
            self.project_activated_job(
                job,
                job.worker.clone().unwrap_or_default(),
                job.deadline.unwrap_or(0),
            )
        })
    }

    /// Builds the [`ActivatedJob`] worker snapshot for `job`, joining the
    /// immutable process-definition identity (`bpmnProcessId`,
    /// `processDefinitionKey`/`version`) and the owning instance's `tags` /
    /// `businessId`, plus the task's static `zeebe:taskHeaders` — the full Zeebe
    /// `ActivatedJob` contract. All of it is derived from state at projection
    /// time (no journaled fields), so the log stays byte-identical. Custom
    /// headers are surfaced only for ordinary BPMN-element jobs (execution- and
    /// task-listener jobs carry their own, unparsed headers in Zeebe, so nano
    /// leaves them empty rather than borrow the underlying element's).
    fn project_activated_job(
        &self,
        job: &state::Job,
        worker: String,
        deadline: u64,
    ) -> ActivatedJob {
        let instance = self.instance(job.instance_key);
        let bpmn_process_id = instance.map(|i| i.process_id.clone()).unwrap_or_default();
        let (tags, business_id) = instance
            .map(|i| (i.tags.clone(), i.business_id.clone()))
            .unwrap_or_default();
        let deployed = instance.and_then(|i| self.state.definition_for(i));
        let process_definition_key = deployed.map(|d| d.key).unwrap_or(0);
        let process_definition_version = deployed.map(|d| d.version).unwrap_or(0);
        let custom_headers = if matches!(job.kind, state::JobKind::BpmnElement) {
            match self
                .process_of_instance(job.instance_key)
                .and_then(|p| p.element(&job.element_id))
                .map(|e| &e.kind)
            {
                Some(ElementKind::ServiceTask {
                    custom_headers,
                    linked_resources,
                    ..
                }) => {
                    let mut headers = custom_headers.clone();
                    // Resolve each `zeebe:linkedResource` id to a concrete
                    // deployed resource key and deliver the resolved set to the
                    // worker in the `linkedResources` custom header (Zeebe
                    // parity). Resolution happens at activation, so a `latest`
                    // binding always reflects the newest deployed version at the
                    // moment the job is handed out. Unresolvable ids are omitted
                    // (the id has no deployed resource yet). The header is a
                    // reserved, engine-computed key: when at least one id
                    // resolves, the computed value deliberately wins over any
                    // author-supplied `linkedResources` header. If nothing
                    // resolves we insert nothing, so we neither emit a
                    // surprising empty `[]` nor clobber the author's value.
                    if let Some(resolved) = self.resolve_linked_resources(linked_resources) {
                        headers.insert(LINKED_RESOURCES_HEADER.to_string(), resolved);
                    }
                    headers
                }
                _ => std::collections::BTreeMap::new(),
            }
        } else {
            std::collections::BTreeMap::new()
        };
        ActivatedJob {
            key: job.key,
            job_type: job.job_type.clone(),
            instance_key: job.instance_key,
            element_instance_key: job.element_instance_key,
            element_id: job.element_id.clone(),
            bpmn_process_id,
            process_definition_key,
            process_definition_version,
            worker,
            deadline,
            lease_token: job.lease_token.clone(),
            retries: job.retries,
            priority: job.priority,
            custom_headers,
            tags,
            business_id,
            variables: self.variables_for_element(job.instance_key, job.element_instance_key),
            kind: job.kind,
        }
    }

    /// Resolves a service task's declared [`crate::model::LinkedResource`]s to the JSON value
    /// of the `linkedResources` custom header: an array of
    /// `{ resourceKey, resourceType, linkName }`, one per resolvable entry.
    ///
    /// Each entry's `resource_id` is looked up against the deployed generic
    /// resources. Nano's read model retains the latest version per `resource_id`,
    /// so every binding type currently resolves to that latest version;
    /// deployment-pinned and version-tag bindings degrade to `latest` until the
    /// engine tracks per-deployment/per-tag resource membership. An id with no
    /// deployed resource is skipped (the resulting header simply omits it),
    /// mirroring a worker that finds no key for that link. JSON is written by
    /// hand to keep engine-core dependency-free and its output deterministic.
    ///
    /// Returns `None` when nothing resolves so the caller emits no header at all
    /// rather than a surprising empty `[]`.
    fn resolve_linked_resources(&self, linked: &[crate::model::LinkedResource]) -> Option<String> {
        let mut json = String::from("[");
        let mut first = true;
        for link in linked {
            let Some(resource) = self.state.resources.get(&link.resource_id) else {
                continue;
            };
            let _ = &link.binding_type; // All bindings resolve to the latest (see doc).
            if !first {
                json.push(',');
            }
            first = false;
            json.push_str("{\"resourceKey\":\"");
            {
                use core::fmt::Write as _;
                let _ = write!(json, "{}", resource.key);
            }
            json.push_str("\",\"resourceType\":");
            write_json_string(&mut json, &link.resource_type);
            json.push_str(",\"linkName\":");
            write_json_string(&mut json, &link.link_name);
            json.push('}');
        }
        if first {
            return None;
        }
        json.push(']');
        Some(json)
    }

    /// making it activatable again. Like [`Engine::trigger_timers`], the host
    /// drives this periodically; the engine never reads a clock. Returns the
    /// [`Event::JobLockExpired`] events produced (empty when nothing was due), so
    /// the host can wake job dispatch immediately rather than waiting for a tick.
    #[allow(
        clippy::expect_used,
        reason = "ExpireJobs is an internal command that cannot fail"
    )]
    pub fn expire_jobs(&mut self, now: u64) -> Vec<Event> {
        self.apply_command_at(Command::ExpireJobs { now }, now)
            .expect("ExpireJobs never fails")
    }

    /// Expire only durable activations, or only leader-local soft locks.
    #[allow(
        clippy::expect_used,
        reason = "ExpireJobsByDurability is an internal command that cannot fail"
    )]
    pub fn expire_jobs_by_durability(&mut self, now: u64, durable: bool) -> Vec<Event> {
        self.apply_command_at(Command::ExpireJobsByDurability { now, durable }, now)
            .expect("ExpireJobsByDurability never fails")
    }

    pub(super) fn expire_job_locks(
        &mut self,
        log: &mut Vec<Event>,
        now: u64,
        durable: Option<bool>,
    ) {
        let mut expired: Vec<(Key, Key)> = self
            .state
            .activated_jobs
            .iter()
            .filter_map(|key| {
                let job = self.state.jobs.get(key)?;
                (job.state == state::JobState::Activated
                    && job.deadline.is_some_and(|deadline| deadline <= now)
                    && durable.is_none_or(|durable| {
                        durable == self.job_requires_durable_activation(*key)
                    }))
                .then_some((job.key, job.instance_key))
            })
            .collect();
        expired.sort_unstable();
        for (job_key, instance_key) in expired {
            self.emit(
                log,
                Event::JobLockExpired {
                    job_key,
                    instance_key,
                },
            );
        }
    }

    /// Fires every armed timer whose due instant is at or before `now`, resuming
    /// the token parked on each. Like [`Engine::expire_jobs`], the host drives
    /// this periodically; the engine never reads a clock. Returns the events the
    /// tick produced (empty when nothing was due).
    #[allow(
        clippy::expect_used,
        reason = "TriggerTimers is an internal command that cannot fail"
    )]
    pub fn trigger_timers(&mut self, now: u64) -> Vec<Event> {
        self.apply_command_at(Command::TriggerTimers { now }, now)
            .expect("TriggerTimers never fails")
    }

    /// All armed and fired timers (fired ones are retained so they never
    /// re-fire). Filter by [`state::Timer::state`] for only-pending timers.
    pub fn timers(&self) -> Vec<&state::Timer> {
        self.state.timers.values().collect()
    }

    /// Publishes a message and correlates it to every open subscription whose
    /// name and correlation key match, merging the message `variables` into each
    /// correlated instance. Like [`Engine::trigger_timers`], the host drives
    /// this; the engine never reads a clock. Returns the events produced — the
    /// heading [`Event::MessagePublished`] (always) plus an
    /// [`Event::MessageCorrelated`] per correlated subscription. Mirrors applying
    /// a [`Command::CorrelateMessage`].
    #[allow(
        clippy::expect_used,
        reason = "CorrelateMessage is an internal command that cannot fail"
    )]
    pub fn correlate_message(
        &mut self,
        message_name: impl Into<String>,
        correlation_key: impl Into<String>,
        variables: HashMap<String, Value>,
        now: u64,
    ) -> Vec<Event> {
        self.apply_command_at(
            Command::correlate_message_with(message_name, correlation_key, variables),
            now,
        )
        .expect("CorrelateMessage never fails")
    }

    /// Broadcasts a signal, correlating it to **every** open subscription whose
    /// signal name matches (name-only correlation). The host drives this; the
    /// engine never reads a clock. Returns the events produced — the heading
    /// [`Event::SignalBroadcast`] (always) plus an [`Event::SignalCorrelated`]
    /// per correlated subscription. Mirrors applying a
    /// [`Command::BroadcastSignal`].
    #[allow(
        clippy::expect_used,
        reason = "BroadcastSignal is an internal command that cannot fail"
    )]
    pub fn broadcast_signal(
        &mut self,
        signal_name: impl Into<String>,
        variables: HashMap<String, Value>,
        now: u64,
    ) -> Vec<Event> {
        self.apply_command_at(
            Command::BroadcastSignal {
                signal_name: signal_name.into(),
                variables,
            },
            now,
        )
        .expect("BroadcastSignal never fails")
    }

    /// All open and settled message subscriptions (correlated/cancelled ones are
    /// retained). Filter by [`state::MessageSubscription::state`] for only-open
    /// subscriptions.
    pub fn message_subscriptions(&self) -> Vec<&state::MessageSubscription> {
        self.state.message_subscriptions.values().collect()
    }

    /// Looks up a message subscription by key, whether open or settled.
    pub fn message_subscription(&self, key: Key) -> Option<&state::MessageSubscription> {
        self.state.message_subscriptions.get(&key)
    }

    /// All open and settled signal subscriptions (correlated/cancelled ones are
    /// retained). Filter by [`state::SignalSubscription::state`] for only-open
    /// subscriptions.
    pub fn signal_subscriptions(&self) -> Vec<&state::SignalSubscription> {
        self.state.signal_subscriptions.values().collect()
    }

    /// All open and settled conditional-event subscriptions (fired/cancelled ones
    /// are retained). Filter by [`state::ConditionalSubscription::state`] for
    /// only-open subscriptions.
    pub fn conditional_subscriptions(&self) -> Vec<&state::ConditionalSubscription> {
        self.state.conditional_subscriptions.values().collect()
    }

    /// Activates jobs of `job_type` and dispatches each to `handler` — the
    /// **callback** worker API for embedded use. Whatever variables the handler
    /// returns complete the job (by key); returning `None` leaves the job locked.
    /// Returns the number of jobs the handler completed. Runs entirely on the
    /// single-writer thread, so there is no concurrency to reason about.
    #[allow(
        clippy::expect_used,
        reason = "completing a just-activated job cannot fail"
    )]
    pub fn poll_jobs<F>(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        mut handler: F,
    ) -> usize
    where
        F: FnMut(&ActivatedJob) -> Option<HashMap<String, Value>>,
    {
        let jobs = self.activate_jobs(job_type, worker, max_jobs, timeout, now);
        let mut completed = 0;
        for job in jobs {
            if let Some(variables) = handler(&job) {
                self.apply_command(Command::complete_job_with(job.key, variables))
                    .expect("activated job can be completed");
                completed += 1;
            }
        }
        completed
    }
}
