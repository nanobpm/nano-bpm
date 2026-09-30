//! Instance-family appliers: process-instance lifecycle, variables, scopes and agent records.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::event::Event;
use crate::model::ElementId;
use crate::state::types::*;

/// Reads the process id of `instance_key` **iff** it is currently non-terminal,
/// so the caller can decrement the per-definition in-flight counter exactly once
/// (idempotent-safe against a re-delivered terminal event that finds the instance
/// already Completed/Terminated).
fn non_terminal_process_id(state: &State, instance_key: &Key) -> Option<String> {
    match state.instances.get(instance_key) {
        Some(i)
            if !matches!(
                i.state,
                ProcessInstanceState::Completed | ProcessInstanceState::Terminated
            ) =>
        {
            Some(i.process_id.clone())
        }
        _ => None,
    }
}

/// Decrements a definition's in-flight instance count on a terminal transition,
/// dropping the entry when it reaches zero (keeps the map bounded by the set of
/// definitions with live instances). `None` = the transition was a no-op (already
/// terminal / unknown instance), so nothing is decremented.
fn decrement_inflight_by_process(state: &mut State, process_id: Option<String>) {
    let Some(pid) = process_id else { return };
    if let Some(count) = state.inflight_by_process.get_mut(&pid) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            state.inflight_by_process.remove(&pid);
        }
    }
}

pub(super) fn apply_instance(state: &mut State, event: &Event) {
    match event {
        Event::ProcessInstanceCreated {
            instance_key,
            process_id,
            variables,
            created_at,
            tags,
            business_id,
            process_definition_key,
            parent_process_instance_key,
            parent_element_instance_key,
            ..
        } => {
            *state
                .inflight_by_process
                .entry(process_id.clone())
                .or_insert(0) += 1;
            *state
                .created_by_process
                .entry(process_id.clone())
                .or_insert(0) += 1;
            // Pin to the exact version the event carries; fall back to the
            // current latest for events written before version pinning (`0`).
            let pinned_key = if *process_definition_key != 0 {
                *process_definition_key
            } else {
                state.processes.get(process_id).map(|d| d.key).unwrap_or(0)
            };
            state.instances.insert(
                *instance_key,
                ProcessInstance {
                    key: *instance_key,
                    process_id: process_id.clone(),
                    process_definition_key: pinned_key,
                    state: ProcessInstanceState::Active,
                    suspended_at: None,
                    created_at: *created_at,
                    tags: tags.clone(),
                    business_id: business_id.clone(),
                    parent_process_instance_key: *parent_process_instance_key,
                    parent_element_instance_key: *parent_element_instance_key,
                    active: HashMap::new(),
                    scopes: HashMap::new(),
                    variables: Arc::new(variables.clone()),
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
                },
            );
        }

        Event::VariablesUpdated {
            instance_key,
            variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let map = Arc::make_mut(&mut instance.variables);
                for (k, v) in variables {
                    map.insert(k.clone(), v.clone());
                }
            }
        }

        Event::ScopedVariablesUpdated {
            instance_key,
            scope_key,
            variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                // A write targeting the root scope lands in the shared `variables`
                // Arc (the flat fast path); any other scope holds its own local map.
                if *scope_key == 0 || *scope_key == *instance_key {
                    let map = Arc::make_mut(&mut instance.variables);
                    for (k, v) in variables {
                        map.insert(k.clone(), v.clone());
                    }
                } else {
                    let map = instance.scope_variables.entry(*scope_key).or_default();
                    for (k, v) in variables {
                        map.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        Event::VariableScopeCreated {
            instance_key,
            scope_key,
            parent_scope_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.insert(*scope_key, *parent_scope_key);
                instance.scope_variables.entry(*scope_key).or_default();
            }
        }

        Event::VariableScopeDestroyed {
            instance_key,
            scope_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.remove(scope_key);
                instance.scope_variables.remove(scope_key);
            }
        }

        Event::AgentInstanceCreated {
            instance_key,
            agent_instance,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .agent_instances
                    .insert(agent_instance.agent_instance_key, agent_instance.clone());
            }
        }

        // UPDATE and COMPLETE both replay as an upsert of the whole record — the
        // event carries the full post-transition value, mirroring
        // `AgentInstanceCreated`, so state rebuilds identically on replay.
        Event::AgentInstanceUpdated {
            instance_key,
            agent_instance,
        }
        | Event::AgentInstanceCompleted {
            instance_key,
            agent_instance,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .agent_instances
                    .insert(agent_instance.agent_instance_key, agent_instance.clone());
            }
        }

        Event::AgentHistoryCreated {
            instance_key,
            record,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let log = instance
                    .agent_history
                    .entry(record.agent_instance_key)
                    .or_default();
                // Append-only, kept sorted by (loop_iteration, produced_at,
                // agent_history_key): find the insertion point rather than
                // pushing + re-sorting so replay is deterministic and cheap.
                let pos = log.partition_point(|r| r.order_key() <= record.order_key());
                log.insert(pos, record.clone());
            }
        }

        Event::AgentHistoryCommitted {
            instance_key,
            agent_instance_key,
            agent_history_keys,
        } => {
            if let Some(log) = state
                .instances
                .get_mut(instance_key)
                .and_then(|inst| inst.agent_history.get_mut(agent_instance_key))
            {
                let keys: HashSet<Key> = agent_history_keys.iter().copied().collect();
                for record in log.iter_mut() {
                    // Only PENDING turns transition; a committed/discarded turn
                    // is immutable (append-only).
                    if record.commit_status == crate::agent::AgentHistoryCommitStatus::Pending
                        && keys.contains(&record.agent_history_key)
                    {
                        record.commit_status = crate::agent::AgentHistoryCommitStatus::Committed;
                    }
                }
            }
        }

        Event::AgentHistoryDiscarded {
            instance_key,
            agent_instance_key,
            agent_history_keys,
        } => {
            if let Some(log) = state
                .instances
                .get_mut(instance_key)
                .and_then(|inst| inst.agent_history.get_mut(agent_instance_key))
            {
                let keys: HashSet<Key> = agent_history_keys.iter().copied().collect();
                for record in log.iter_mut() {
                    if record.commit_status == crate::agent::AgentHistoryCommitStatus::Pending
                        && keys.contains(&record.agent_history_key)
                    {
                        record.commit_status = crate::agent::AgentHistoryCommitStatus::Discarded;
                    }
                }
            }
        }

        // Dedup outcome only — the append-only log is intentionally left
        // untouched (no record is created for an idempotent retry).
        Event::AgentHistoryDeduplicated { .. } => {}

        Event::ProcessInstanceCompleted { instance_key } => {
            let terminal_pid = non_terminal_process_id(state, instance_key);
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Completed;
                instance.suspended_at = None;
                // Clear any residual runtime bookkeeping. On a normal completion
                // these are already empty (the instance completes only with an
                // empty `active` map); a top-level terminate end event completes
                // the instance while sibling tokens/scopes are still recorded, so
                // this forced teardown (mirroring `ProcessInstanceTerminated`)
                // keeps the terminal snapshot consistent — no live scope for the
                // dead-scope guard to read, no stranded MI/ad-hoc/join/
                // compensation payload on the terminal shell. Incidents are NOT
                // closed here (unlike `ProcessInstanceTerminated`): a normal
                // completion may legitimately leave a retained incident record
                // for later resolution, and a top-level terminate end resolves
                // its own incidents explicitly (`IncidentResolved` events emitted
                // ahead of this record — see `complete_terminate_end`).
                instance.active.clear();
                instance.scopes.clear();
                instance.multi_instances.clear();
                instance.adhoc_instances.clear();
                instance.join_counts.clear();
                instance.join_flow_arrivals.clear();
                instance.join_instances.clear();
                instance.compensable.clear();
                instance.compensation_waits.clear();
                // A terminal instance's variables are never read from hot state
                // again — workers are done, the exporter projects from events,
                // and recovery replays the journal + durable store. Drop the
                // payload now to reclaim heap immediately, decoupling
                // terminal-state memory from exporter-driven eviction (ADR 0012).
                // The instance shell stays resident until eviction so status
                // queries still resolve during the read-model projection gap.
                if !instance.variables.is_empty() {
                    instance.variables = Arc::new(HashMap::new());
                }
                instance.variables_spilled = false;
                instance.scope_variables.clear();
                instance.scope_parents.clear();
            }
            decrement_inflight_by_process(state, terminal_pid);
        }

        Event::ProcessInstanceTerminating { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Terminating;
            }
        }

        Event::ProcessInstanceSuspended { instance_key, at } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Suspended;
                instance.suspended_at = Some(*at);
            }
        }

        Event::ProcessInstanceResumed { instance_key } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Active;
                instance.suspended_at = None;
            }
        }

        Event::ProcessInstanceBusinessIdAssigned {
            instance_key,
            business_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.business_id = Some(business_id.clone());
            }
        }

        Event::ProcessInstanceMigrated {
            instance_key,
            target_process_id,
            target_process_definition_key,
            element_mappings,
        } => {
            let remap: HashMap<&str, &str> = element_mappings
                .iter()
                .map(|(s, t)| (s.as_str(), t.as_str()))
                .collect();
            let remap_id = |id: &mut ElementId| {
                if let Some(target) = remap.get(id.as_str()) {
                    *id = (*target).to_string();
                }
            };

            // Move the live-instance count from the source process id to the
            // target's, and re-point the instance itself.
            let source_process_id = state
                .instances
                .get(instance_key)
                .map(|i| i.process_id.clone());
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.process_id = target_process_id.clone();
                // Re-pin the instance to the target definition's version so
                // `definition_for` resolves execution against the migrated-to
                // model rather than the source it was created on.
                instance.process_definition_key = *target_process_definition_key;
                for element_id in instance.active.values_mut() {
                    remap_id(element_id);
                }
                // Both parallel-join maps are keyed by the join gateway's element
                // id, so they must be remapped together to stay in sync (a stale
                // `join_instances` key would make `join_eik` miss after migration
                // and re-open an already-open join). `collect()` would silently
                // drop entries if two source ids collapse onto one target id, so
                // merge deterministically instead: sum the arrival counts, and
                // keep the smallest element-instance key for the open join.
                if !instance.join_counts.is_empty() {
                    let mut remapped: HashMap<ElementId, usize> =
                        HashMap::with_capacity(instance.join_counts.len());
                    for (mut eid, count) in instance.join_counts.drain() {
                        remap_id(&mut eid);
                        *remapped.entry(eid).or_insert(0) += count;
                    }
                    instance.join_counts = remapped;
                }
                // Per-flow arrivals are keyed by the join AND name each flow's
                // source element, so both ids are remapped (the migration guard
                // has already checked every flow exists in the target).
                if !instance.join_flow_arrivals.is_empty() {
                    let mut remapped: HashMap<ElementId, FlowArrivals> =
                        HashMap::with_capacity(instance.join_flow_arrivals.len());
                    for (mut eid, mut arrivals) in instance.join_flow_arrivals.drain() {
                        remap_id(&mut eid);
                        arrivals.remap(remap_id);
                        let merged = remapped.entry(eid).or_default();
                        for (flow, count) in arrivals.iter() {
                            merged.add(flow, count);
                        }
                    }
                    instance.join_flow_arrivals = remapped;
                }
                if !instance.join_instances.is_empty() {
                    let mut remapped: HashMap<ElementId, Key> =
                        HashMap::with_capacity(instance.join_instances.len());
                    for (mut eid, eik) in instance.join_instances.drain() {
                        remap_id(&mut eid);
                        remapped
                            .entry(eid)
                            .and_modify(|existing| {
                                if eik < *existing {
                                    *existing = eik;
                                }
                            })
                            .or_insert(eik);
                    }
                    instance.join_instances = remapped;
                }
            }
            if source_process_id.as_deref() != Some(target_process_id.as_str()) {
                decrement_inflight_by_process(state, source_process_id);
                *state
                    .inflight_by_process
                    .entry(target_process_id.clone())
                    .or_insert(0) += 1;
            }

            // Re-point every element instance's attached runtime. Active jobs keep
            // their type (a worker already holds the lease) — only the element id
            // moves, mirroring Zeebe.
            for job in state.jobs.values_mut() {
                if job.instance_key == *instance_key {
                    remap_id(&mut job.element_id);
                }
            }
            for user_task in state.user_tasks.values_mut() {
                if user_task.instance_key == *instance_key {
                    remap_id(&mut user_task.element_id);
                }
            }
            for timer in state.timers.values_mut() {
                if timer.instance_key == *instance_key {
                    remap_id(&mut timer.element_id);
                }
            }
            for sub in state.message_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                }
            }
            for sub in state.signal_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                }
            }
            for sub in state.conditional_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                }
            }
            for incident in state.incidents.values_mut() {
                if incident.instance_key == *instance_key {
                    remap_id(&mut incident.element_id);
                }
            }
            // The scope tree (`scopes` / `scope_parents` / `scope_variables`) is
            // intentionally NOT remapped: the command handler rejects any
            // instance whose active tokens live inside a non-root flow scope
            // (embedded sub-process, multi-instance, or ad-hoc) as unsupported,
            // so an instance that reaches this applier is flat (root scope only)
            // and has nothing to remap. See the "flow scope unchanged"
            // precondition in `Command::MigrateInstance` validation.
        }

        Event::ProcessInstanceTerminated { instance_key } => {
            let terminal_pid = non_terminal_process_id(state, instance_key);
            // Close any incident still active on the instance: with the instance
            // gone the parked tokens are gone too, so `hasIncident` must clear.
            // The resource cancellations (jobs/timers/subscriptions) were emitted
            // as their own events ahead of this one.
            if let Some(instance) = state.instances.get(instance_key) {
                for incident_key in instance.incidents.clone() {
                    if let Some(incident) = state.incidents.get_mut(&incident_key) {
                        incident.state = IncidentState::Resolved;
                    }
                }
            }
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.state = ProcessInstanceState::Terminated;
                instance.suspended_at = None;
                instance.active.clear();
                instance.scopes.clear();
                instance.incidents.clear();
                // Drop the multi-instance / ad-hoc runtime records too: a forced
                // teardown removes the body/container token but these maps are
                // otherwise cleared only by `MultiInstanceCompleted` /
                // `AdHocCompleted`, so terminating an instance mid-loop would
                // retain the whole item/output/active-set payload on a terminal
                // instance until eviction (and leave a stale "live scope" for the
                // dead-scope guard to read). Immediate terminal cleanup (ADR 0012).
                instance.multi_instances.clear();
                instance.adhoc_instances.clear();
                // Same forced-teardown rationale for the remaining runtime
                // bookkeeping a mid-flight terminate can leave behind: an open
                // parallel join (`join_counts`/`join_flow_arrivals`/`join_instances`) or a pending
                // compensation (`compensable`/`compensation_waits`) is drained
                // naturally only on normal completion. Terminating mid-join or
                // mid-compensation would otherwise strand this bookkeeping —
                // potentially a large compensation list — on the terminal
                // instance shell until eviction, leaving its terminal snapshot
                // inconsistent with the MI/ad-hoc/variable state cleared above.
                instance.join_counts.clear();
                instance.join_flow_arrivals.clear();
                instance.join_instances.clear();
                instance.compensable.clear();
                instance.compensation_waits.clear();
                // Drop the variable payload on the terminal transition — see
                // `ProcessInstanceCompleted` above (ADR 0012).
                if !instance.variables.is_empty() {
                    instance.variables = Arc::new(HashMap::new());
                }
                instance.variables_spilled = false;
                instance.scope_variables.clear();
                instance.scope_parents.clear();
            }
            decrement_inflight_by_process(state, terminal_pid);
        }

        // A routing marker on the deploy partition: the instance is created on
        // the target partition (driven by the host's DispatchStartInstance), so
        // there is no local state to mutate here.
        Event::StartInstanceDispatched { .. } => {}
        _ => unreachable!(),
    }
}
