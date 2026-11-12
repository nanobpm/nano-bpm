//! Job-family appliers: jobs, listener jobs and incidents.

use crate::event::Event;
use crate::state::types::*;

pub(super) fn apply_job(state: &mut State, event: &Event) {
    match event {
        Event::JobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            created_at,
            priority,
            retries,
        } => {
            state.jobs.insert(
                *job_key,
                Job {
                    key: *job_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    job_type: job_type.clone(),
                    state: JobState::Created,
                    worker: None,
                    deadline: None,
                    activated_at: None,
                    activation_timeout: None,
                    lease_token: None,
                    durable_activation: false,
                    activated: false,
                    retries: *retries,
                    priority: *priority,
                    created_at: *created_at,
                    kind: JobKind::BpmnElement,
                    error_message: None,
                    error_code: None,
                    has_failed_with_retries_left: false,
                    settled_tokens: Vec::new(),
                    terminal_end: None,
                },
            );
            resync_job_index(state, *job_key);
            state
                .jobs_by_instance
                .entry(*instance_key)
                .or_default()
                .insert(*job_key);
        }

        Event::ExecutionListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            job_type,
            event_type,
            listener_index,
            scope,
            created_at,
            retries,
        } => {
            state.jobs.insert(
                *job_key,
                Job {
                    key: *job_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    job_type: job_type.clone(),
                    state: JobState::Created,
                    worker: None,
                    deadline: None,
                    activated_at: None,
                    activation_timeout: None,
                    lease_token: None,
                    durable_activation: false,
                    activated: false,
                    retries: *retries,
                    priority: DEFAULT_JOB_PRIORITY,
                    created_at: *created_at,
                    kind: JobKind::ExecutionListener {
                        event_type: *event_type,
                        index: *listener_index,
                        scope: *scope,
                    },
                    error_message: None,
                    error_code: None,
                    has_failed_with_retries_left: false,
                    settled_tokens: Vec::new(),
                    terminal_end: None,
                },
            );
            resync_job_index(state, *job_key);
            state
                .jobs_by_instance
                .entry(*instance_key)
                .or_default()
                .insert(*job_key);
        }

        Event::TaskListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id,
            user_task_key,
            job_type,
            event_type,
            listener_index,
            created_at,
            retries,
        } => {
            state.jobs.insert(
                *job_key,
                Job {
                    key: *job_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    job_type: job_type.clone(),
                    state: JobState::Created,
                    worker: None,
                    deadline: None,
                    activated_at: None,
                    activation_timeout: None,
                    lease_token: None,
                    durable_activation: false,
                    activated: false,
                    retries: *retries,
                    priority: DEFAULT_JOB_PRIORITY,
                    created_at: *created_at,
                    kind: JobKind::TaskListener {
                        event_type: *event_type,
                        index: *listener_index,
                        user_task_key: *user_task_key,
                    },
                    error_message: None,
                    error_code: None,
                    has_failed_with_retries_left: false,
                    settled_tokens: Vec::new(),
                    terminal_end: None,
                },
            );
            resync_job_index(state, *job_key);
            state
                .jobs_by_instance
                .entry(*instance_key)
                .or_default()
                .insert(*job_key);
        }

        Event::JobActivated {
            job_key,
            durable,
            worker,
            deadline,
            activated_at,
            lease_token,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Activated;
                // An *empty* activation worker string is not an attribution:
                // normalize it to `None` here, at the single source, so an
                // explicitly-supplied `""` never becomes `Some("")`. This keeps
                // `Job.worker` canonical for every downstream derivation — the
                // terminal `JobCompleted`/`JobFailed`/`JobErrorThrown` events and
                // the read-model attribution bindings — so no empty attribution
                // can be stamped or `COALESCE`d into a row that should stay NULL.
                job.worker = Some(worker.clone()).filter(|w| !w.is_empty());
                job.deadline = Some(*deadline);
                job.activated_at = *activated_at;
                // Freeze the requested lock duration at activation, from the two
                // instants the event carries (deadline = activated_at + timeout).
                // Immune to later UpdateJobTimeout extensions that move `deadline`.
                job.activation_timeout = activated_at.map(|a| deadline.saturating_sub(a));
                // Restore the opaque per-activation lease token from the event
                // (minted once at command-processing, ADR 0005-810-job-lease D2):
                // replay reads it here rather than regenerating it. `None` for a
                // lease-less activation.
                job.lease_token = lease_token.clone();
                job.durable_activation |= *durable;
                job.activated = true;
            }
            resync_job_index(state, *job_key);
        }

        Event::JobLockExpired { job_key, .. } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                if job.state == JobState::Activated {
                    job.state = JobState::Created;
                    job.worker = None;
                    job.deadline = None;
                    job.activated_at = None;
                    job.activation_timeout = None;
                }
            }
            resync_job_index(state, *job_key);
        }

        Event::JobFailed {
            job_key,
            retries,
            worker,
            error_message,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.retries = *retries;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
                // Zeebe parity (#1327): retain the worker's message and whether
                // retries remain so they survive a compaction-floor read-model
                // rebuild (which re-seeds the row from engine state). Keep the
                // last known message for events serialized before the field
                // existed (mirrors the read model's `COALESCE`).
                if error_message.is_some() {
                    job.error_message = error_message.clone();
                }
                job.has_failed_with_retries_left = *retries > 0;
                // With retries left the job returns to the activatable pool; with
                // none it parks (an incident is raised alongside this event).
                if *retries > 0 {
                    // Back to the activatable pool — no longer held, so drop the
                    // last activating worker (mirrors JobLockExpired).
                    job.worker = None;
                    job.state = JobState::Created;
                } else {
                    // Terminal, incident-bearing park: retain the activating
                    // `worker` so the incident (joined by `jobKey`) can attribute
                    // the failure to the worker/host that was running it (Zeebe
                    // parity — a failed JobRecord retains its `worker`). The event
                    // carries the worker so this survives a restart replay (where
                    // the volatile, unexported `JobActivated` lock is gone); fall
                    // back to any live value for events serialized before the field
                    // existed.
                    if worker.is_some() {
                        job.worker = worker.clone();
                    } else {
                        // Never retain the non-attributable digest sentinel
                        // (#963): it is replica-local soft state, so keeping it
                        // would diverge the leader's snapshot from followers'.
                        job.worker = job.attribution_worker();
                    }
                    job.state = JobState::Failed;
                }
            }
            resync_job_index(state, *job_key);
        }

        Event::JobErrorThrown {
            job_key,
            worker,
            error_code,
            error_message,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.state = JobState::Errored;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
                job.lease_token = None;
                // Zeebe parity (#1327): the thrown code/message land on the job
                // and survive a compaction-floor rebuild. The exporter keys
                // `jobFailedWithRetriesLeft` off the record's (unchanged) retries
                // for ERROR_THROWN as well as FAILED. Keep the last known message
                // for pre-field events (mirrors the read model's `COALESCE`).
                job.error_code = Some(error_code.clone());
                if error_message.is_some() {
                    job.error_message = error_message.clone();
                }
                job.has_failed_with_retries_left = job.retries > 0;
                // Terminal, incident-bearing transition: retain the activating
                // `worker` for attribution (Zeebe parity — throwError retains the
                // record incl. `worker`). Carried on the event so it survives a
                // restart replay; fall back to any live value for pre-field events.
                if worker.is_some() {
                    job.worker = worker.clone();
                } else {
                    // Scrub the replica-local digest sentinel (#963), as for
                    // `JobFailed` above.
                    job.worker = job.attribution_worker();
                }
            }
            resync_job_index(state, *job_key);
        }

        Event::JobSettlementRecorded {
            job_key,
            outcome,
            lease_token,
            ..
        } => {
            let settlement = JobSettlement {
                lease_token: lease_token.clone(),
                outcome: *outcome,
            };
            record_job_settlement(state, *job_key, settlement.clone(), SETTLED_JOB_LEDGER_CAP);
            // Fence the live job itself (#1181): a retryable failure leaves the
            // job alive (Created, token retained), and this per-job record —
            // never evicted while the job lives — is what rejects both a
            // double-apply of the same settlement (after the bounded ledger
            // aged the entry out) and a conflicting outcome under the same
            // token. Terminal jobs are dropped from `jobs` shortly after, so
            // the ledger remains the cross-eviction record.
            if let Some(job) = state.jobs.get_mut(job_key) {
                if !job
                    .settled_tokens
                    .iter()
                    .any(|s| s.lease_token == *lease_token)
                {
                    job.settled_tokens.push(settlement);
                }
            }
        }

        Event::JobCompleted {
            job_key,
            completed_at,
            deadline,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                // A completed job is no longer retry-eligible (#1551 review):
                // clear the flag a prior retryable failure set so the
                // `hasRetriesLeft` filter stops matching it. The clear is
                // UNCONDITIONAL — retry-eligibility is a property of the job's
                // current state, not of the terminal stamp, so it is NOT part
                // of the first-transition-wins contract: a replayed
                // `JobCompleted → JobFailed(retries: n) → JobCompleted`
                // sequence re-arms the flag at the failure, and only clearing
                // on every terminal transition leaves the completed job
                // non-retry-eligible (the read model clears unconditionally
                // too). Only the terminal INSTANT keeps first-transition-wins
                // (`stamp_terminal_end`).
                job.has_failed_with_retries_left = false;
                stamp_terminal_end(job, *completed_at, *deadline);
                job.state = JobState::Completed;
                job.worker = None;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
            }
            resync_job_index(state, *job_key);
        }

        Event::IncidentRaised {
            incident_key,
            instance_key,
            element_instance_key,
            element_id,
            kind,
            reason,
            job_key,
            created_at,
            redrive,
        } => {
            state.incidents.insert(
                *incident_key,
                Incident {
                    key: *incident_key,
                    instance_key: *instance_key,
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    kind: *kind,
                    redrive: redrive.clone(),
                    reason: reason.clone(),
                    job_key: *job_key,
                    created_at: *created_at,
                    state: IncidentState::Active,
                    resolved_at: None,
                    operation_reference: None,
                },
            );
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.incidents.push(*incident_key);
            }
        }

        Event::JobRetriesUpdated {
            job_key, retries, ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.retries = *retries;
                // Keep the retries-left flag in sync with the retry update
                // (#1551 review): it feeds the process-instance `hasRetriesLeft`
                // filter, so a failure whose retries are later drained to zero
                // must stop matching and a parked zero-retry failure topped up
                // (the incident-resolution flow) must start matching. The flag
                // derives from the failure-bearing states (Failed/Errored) plus
                // the CURRENT retries: a job that returned to the activatable
                // pool keeps the flag its retryable failure set (it can only
                // lose retries here, never gain them). A terminal job can never
                // re-arm it live — the engine rejects retries updates on
                // Completed/Canceled jobs — but a REPLAYED archived journal can
                // order a retries update after the terminal event, so also
                // require the job to be non-terminal here (a terminal job's
                // flag is cleared once, at its terminal transition).
                job.has_failed_with_retries_left = *retries > 0
                    && !matches!(job.state, JobState::Completed | JobState::Canceled)
                    && (job.has_failed_with_retries_left
                        || matches!(job.state, JobState::Failed | JobState::Errored));
            }
        }

        Event::JobTimeoutUpdated {
            job_key, deadline, ..
        } => {
            // The job stays Activated (still locked by the same worker); only its
            // lock deadline moves out. Index membership is unchanged.
            if let Some(job) = state.jobs.get_mut(job_key) {
                job.deadline = Some(*deadline);
            }
        }

        Event::IncidentResolved {
            incident_key,
            instance_key,
            job_key,
            resolved_at,
            operation_reference,
        } => {
            // Retain the record as an audit trail: transition it to Resolved
            // rather than dropping it.
            if let Some(incident) = state.incidents.get_mut(incident_key) {
                incident.state = IncidentState::Resolved;
                incident.resolved_at = Some(*resolved_at);
                incident.operation_reference = *operation_reference;
            }
            // Remove it from the instance's *active* index so `hasIncident`
            // reflects only open incidents.
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.incidents.retain(|k| k != incident_key);
            }
            // A recoverable job-incident: return the parked job to the
            // activatable pool so a worker can pick it up again — but *only* if
            // it is still `Failed` (parked). A job already driven to a terminal
            // state (`Canceled`/`Completed`/`Errored`) by a concurrent forced
            // teardown must not be resurrected by resolving its retained
            // incident: scope teardown emits `JobCanceled` and then
            // `IncidentResolved` for the same job in one batch, so an
            // unconditional reset would return the just-cancelled job to
            // `Created` and let a worker activate it against an element instance
            // that is being removed. The normal `ResolveIncident` command only
            // reaches here with a `Failed` job (retries validated first), so
            // this guard is transparent to it.
            if let Some(job_key) = job_key {
                let is_failed = matches!(
                    state.jobs.get(job_key).map(|j| j.state),
                    Some(JobState::Failed)
                );
                if is_failed {
                    if let Some(job) = state.jobs.get_mut(job_key) {
                        job.state = JobState::Created;
                        job.worker = None;
                        job.deadline = None;
                        job.activated_at = None;
                        job.activation_timeout = None;
                        job.lease_token = None;
                    }
                    resync_job_index(state, *job_key);
                }
            }
        }

        Event::JobCanceled {
            job_key,
            canceled_at,
            deadline,
            ..
        } => {
            if let Some(job) = state.jobs.get_mut(job_key) {
                // A canceled job is no longer retry-eligible (#1551 review):
                // clear the flag a prior retryable failure set so the
                // `hasRetriesLeft` filter stops matching it. Unconditional, as
                // for `JobCompleted` above: the flag is current-state
                // retry-eligibility, not part of the first-transition-wins
                // terminal stamp, so a re-delivered cancellation must still
                // clear what a replayed later failure re-armed.
                job.has_failed_with_retries_left = false;
                stamp_terminal_end(job, *canceled_at, *deadline);
                job.state = JobState::Canceled;
                job.worker = None;
                job.deadline = None;
                job.activated_at = None;
                job.activation_timeout = None;
                job.lease_token = None;
            }
            resync_job_index(state, *job_key);
        }
        _ => unreachable!(),
    }
}

/// Records the job's first terminal transition (#1440) with the deadline the
/// terminal EVENT carries — not the replica's `job.deadline`, which differs
/// between leader and followers for a leader-local (never-journaled) soft
/// activation. First-transition-wins (a
/// re-delivered or later terminal event is a no-op), mirroring the read model's
/// `end_ms IS NULL` guard. A pre-field event carries `0` (no instant): nothing
/// is recorded rather than inventing an epoch-zero `endTime`.
fn stamp_terminal_end(job: &mut crate::state::Job, at: u64, deadline: Option<u64>) {
    if job.terminal_end.is_none() && at != 0 {
        job.terminal_end = Some(crate::state::JobTerminalEnd { at, deadline });
    }
}
