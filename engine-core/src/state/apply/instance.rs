//! Instance-family appliers: process-instance lifecycle, variables, scopes and agent records.

use std::collections::{BTreeMap, BTreeSet};
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

/// Drops the mapping-time secret provenance (#1640) of every job of
/// `instance_key` whose variable view the write to `scope_key` can change.
/// Provenance attests the value a trusted input mapping wrote; a later write
/// visible to the job may replace that value (even with equal reference text
/// from an untrusted source), so trust is revoked — fail closed: the reference
/// text then passes through unresolved.
///
/// A job sees its own element-instance scope and every ancestor scope up to the
/// root, so a write is relevant iff it targets the root (`None`, `0` or the
/// instance key) or one of those scopes. A write to a sibling branch (e.g. a
/// parallel task's own input mappings) cannot reach the job and keeps its
/// trust. Jobs without provenance are skipped before any scope walk.
fn revoke_secret_provenance(state: &mut State, instance_key: &Key, scope_key: Option<Key>) {
    let Some(keys) = state.jobs_by_instance.get(instance_key) else {
        return;
    };
    let scope = scope_key.filter(|s| *s != 0 && s != instance_key);
    let scopes = state.instances.get(instance_key).map(|i| &i.scopes);
    for key in keys {
        let Some(job) = state.jobs.get_mut(key) else {
            continue;
        };
        if job.secret_provenance.is_empty() {
            continue;
        }
        let visible = match scope {
            None => true,
            Some(target) => sees_scope(scopes, job.element_instance_key, target),
        };
        if visible {
            job.secret_provenance.clear();
        }
    }
}

/// Whether `target` is `element_instance_key` itself or one of its ancestor
/// scopes in the instance's element-instance tree. Cycle-safe; an unknown tree
/// answers `true` (fail closed).
fn sees_scope(scopes: Option<&BTreeMap<Key, Key>>, element_instance_key: Key, target: Key) -> bool {
    let Some(scopes) = scopes else {
        return true;
    };
    let mut current = element_instance_key;
    for _ in 0..=scopes.len() {
        if current == target {
            return true;
        }
        match scopes.get(&current) {
            Some(&parent) if parent != 0 => current = parent,
            _ => return false,
        }
    }
    true
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
            state.insert_instance(ProcessInstance {
                key: *instance_key,
                process_id: process_id.clone(),
                process_definition_key: pinned_key,
                state: ProcessInstanceState::Active,
                suspended_at: None,
                terminated_epoch: 0,
                created_at: *created_at,
                tags: tags.clone(),
                business_id: business_id.clone(),
                parent_process_instance_key: *parent_process_instance_key,
                parent_element_instance_key: *parent_element_instance_key,
                active: BTreeMap::new(),
                scopes: BTreeMap::new(),
                variables: Arc::new(variables.clone()),
                join_counts: BTreeMap::new(),
                join_flow_arrivals: BTreeMap::new(),
                join_instances: BTreeMap::new(),
                parked_joins: Default::default(),
                activating: Default::default(),
                gateway_routes: BTreeMap::new(),
                races: BTreeMap::new(),
                gateway_subscriptions: BTreeMap::new(),
                gateway_triggers: BTreeMap::new(),
                event_gateway_owned: false,
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
            });
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
            revoke_secret_provenance(state, instance_key, None);
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
            revoke_secret_provenance(state, instance_key, Some(*scope_key));
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
                instance.esp_held_payloads.remove(scope_key);
            }
        }

        Event::EventSubProcessPayloadHeld {
            instance_key,
            scope_key,
            variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .esp_held_payloads
                    .insert(*scope_key, variables.clone());
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
                let keys: BTreeSet<Key> = agent_history_keys.iter().copied().collect();
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
                let keys: BTreeSet<Key> = agent_history_keys.iter().copied().collect();
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

        // Off-heap orphan quarantine (#1574): record the replicated dedup
        // tombstone only. Residency-independent, so every replica lands
        // identically; no instance state is touched. The tombstone carries the
        // loss-episode id (#1587) so a delayed duplicate of the same episode,
        // applying after the incident was resolved and the tombstone lifted, is
        // a deterministic no-op instead of a fresh incident.
        Event::VariablesLostQuarantined {
            instance_key,
            incident_key,
            episode,
        } => {
            state.pending_orphan_supersedes.remove(instance_key);
            state.fold_settlement_quarantined(*instance_key, *episode);
            state.variables_lost_quarantined.insert(
                *instance_key,
                crate::state::types::VariablesLostTombstone {
                    incident_key: *incident_key,
                    episode: *episode,
                    terminal: false,
                },
            );
        }

        // A superseded orphan stamp (#1587 review) journals the
        // replica-deterministic decision so the leader's journal re-enqueues
        // the still-pending report.
        // #1608: it does record the report as durably pending, so the host
        // rebuilds its orphan backlog from replicated state after recovery.
        Event::OrphanStampSuperseded {
            instance_key,
            episode,
            kind,
        } => {
            // A delayed duplicate of an already-settled episode is not a
            // pending report (#1608 review): recording it would make recovery
            // re-raise `VariablesLost` without a new loss.
            //
            // Episodes are allocator positions, so they grow monotonically per
            // instance: the latest settled episode is a HIGH-WATER MARK. Any
            // nonzero episode at or below it (an original E1 superseded, its
            // retry quarantined at E2 and resolved, then E1 arriving late) is
            // settled too. A legacy snapshot's live tombstone (written before
            // the settlement map existed) counts as settled at its episode.
            if *episode > 0
                && settled_orphan_episode(state, *instance_key).is_some_and(|s| *episode <= s)
            {
                return;
            }
            state.pending_orphan_supersedes.insert(
                *instance_key,
                kind.unwrap_or(crate::command::OrphanKind::Resident),
            );
        }

        Event::ProcessInstanceCompleted { instance_key } => {
            state.note_terminated(*instance_key);
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
                instance.parked_joins.clear();
                instance.activating.clear();
                instance.multi_instances.clear();
                instance.adhoc_instances.clear();
                instance.join_counts.clear();
                instance.join_flow_arrivals.clear();
                instance.join_instances.clear();
                // A split gateway's carried activation-time selection (#1402)
                // is drained naturally when the gateway completes; a forced
                // teardown (terminate end event, or termination) mid-activation
                // would strand it on the terminal shell otherwise.
                instance.gateway_routes.clear();
                // An event-based gateway's armed-catch race membership (#1676)
                // is removed per catch as each completes or terminates, but a
                // whole-instance teardown (top-level terminate end, or
                // cancellation) bypasses those per-catch terminal events — clear
                // the map so no armed race is stranded on the terminal shell.
                instance.races.clear();
                // #2003: gateway-owned subscriptions likewise outlive no
                // whole-instance teardown.
                instance.gateway_subscriptions.clear();
                instance.gateway_triggers.clear();
                instance.compensable.clear();
                instance.compensation_waits.clear();
                instance.esp_held_payloads.clear();
                // A terminal instance's variables are never read from hot state
                // again — workers are done, the exporter projects from events,
                // and recovery replays the journal + durable store. Drop the
                // payload now to reclaim heap immediately, decoupling
                // terminal-state memory from exporter-driven eviction (ADR 0012).
                // The instance shell stays resident until eviction so status
                // queries still resolve during the read-model projection gap.
                if !instance.variables.is_empty() {
                    instance.variables = Arc::new(BTreeMap::new());
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

        // An audit record: the modification's state change is carried by the
        // activation / termination events that follow it.
        // Modification may activate a gateway-owned catch directly under the
        // gateway-owned subscription model: mark the instance (#2004) so the
        // legacy-race refusal does not mistake that catch for a legacy race.
        Event::ProcessInstanceModified { instance_key, .. } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.event_gateway_owned = true;
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
            join_scoped,
            compensation_subscriptions,
            compensation_migrated,
            ..
        } => {
            let remap: BTreeMap<&str, &str> = element_mappings
                .iter()
                .map(|(s, t)| (s.as_str(), t.as_str()))
                .collect();
            let remap_id = |id: &mut ElementId| {
                if let Some(target) = remap.get(id.as_str()) {
                    *id = (*target).to_string();
                }
            };
            // A retained boundary subscription names its boundary inside its
            // kind; re-point it with the same mapping (the command handler has
            // already re-created every subscription whose trigger changed).
            let remap_subscription_kind = |kind: &mut MessageSubscriptionKind| match kind {
                MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id,
                }
                | MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id,
                } => remap_id(boundary_element_id),
                MessageSubscriptionKind::EventSubProcess { esp_element_id, .. } => {
                    remap_id(esp_element_id)
                }
                MessageSubscriptionKind::IntermediateCatch => {}
            };

            // Move the live-instance count from the source process id to the
            // target's, and re-point the instance itself.
            let source_process_id = state
                .instances
                .get(instance_key)
                .map(|i| i.process_id.clone());
            // Multi-instance bodies and ad-hoc containers (#1630) adopt the
            // target element's loop / `zeebe:adHoc` wiring, as Zeebe reads it
            // from the target definition after migration. The evaluated input
            // collection, spawned children and collected outputs are runtime
            // state and are kept. Resolved before borrowing the instance.
            let target_def = state
                .process_by_key(*target_process_definition_key)
                .map(|d| d.definition.clone());
            if let Some(instance) = state.instances.get_mut(instance_key) {
                for mi in instance.multi_instances.values_mut() {
                    remap_id(&mut mi.element_id);
                    if let Some(spec) = target_def
                        .as_ref()
                        .and_then(|d| d.element(&mi.element_id))
                        .and_then(|e| e.multi_instance.as_ref())
                    {
                        mi.input_element = spec.input_element.clone();
                        mi.output_collection = spec.output_collection.clone();
                        mi.output_element = spec.output_element.clone();
                        mi.completion_condition = spec.completion_condition.clone();
                        mi.cancel_remaining_instances = spec.cancel_remaining_instances;
                    }
                }
                // Event-based gateways (#2004): a triggered catch follows the
                // mapping; a waiting gateway's subscription set is re-derived
                // from the target gateway (the command handler has already
                // deleted the unmapped catches' subscriptions and opened the
                // new ones).
                for trigger in instance.gateway_triggers.values_mut() {
                    remap_id(&mut trigger.catch_element_id);
                }
                let gateway_ids: Vec<(Key, ElementId)> = instance
                    .gateway_subscriptions
                    .keys()
                    .filter_map(|eik| instance.active.get(eik).map(|id| (*eik, id.clone())))
                    .collect();
                for (eik, gateway_id) in gateway_ids {
                    let mut target_gateway_id = gateway_id;
                    remap_id(&mut target_gateway_id);
                    let catches: Vec<ElementId> = target_def
                        .as_ref()
                        .map(|d| {
                            d.event_gateway_targets(&target_gateway_id)
                                .into_iter()
                                .map(|c| c.id.clone())
                                .collect()
                        })
                        .unwrap_or_default();
                    instance.gateway_subscriptions.insert(eik, catches);
                }
                for adhoc in instance.adhoc_instances.values_mut() {
                    remap_id(&mut adhoc.element_id);
                    if let Some(spec) = target_def
                        .as_ref()
                        .and_then(|d| d.adhoc.iter().find(|a| a.container_id == adhoc.element_id))
                    {
                        adhoc.output_collection = spec.output_collection.clone();
                        adhoc.output_element = spec.output_element.clone();
                    }
                }
            }
            if let Some(instance) = state.instances.get_mut(instance_key) {
                // Compensation (#1631): re-point every subscription the
                // migration planned, delete the rest (their compensation
                // boundary was unmapped, Zeebe parity), and re-point in-flight
                // throws at their mapped throw / handler elements. A legacy
                // record never carried compensation state (it was rejected).
                if *compensation_migrated {
                    // One plan entry per subscription occurrence, keyed by
                    // (element instance, element, source handler, source boundary id);
                    // duplicates
                    // (boundaries sharing a handler) are consumed one each.
                    let mut plan: BTreeMap<
                        (Key, &str, &str, &str),
                        std::collections::VecDeque<&crate::event::MigratedCompensationSubscription>,
                    > = BTreeMap::new();
                    for c in compensation_subscriptions {
                        plan.entry((
                            c.source_element_instance_key,
                            c.source_element_id.as_str(),
                            c.source_handler.as_str(),
                            c.source_boundary_id.as_str(),
                        ))
                        .or_default()
                        .push_back(c);
                    }
                    let mut migrated = Vec::new();
                    for c in instance.compensable.iter() {
                        let m = plan
                            .get_mut(&(
                                c.element_instance_key,
                                c.element_id.as_str(),
                                c.handler.as_str(),
                                c.boundary_id.as_str(),
                            ))
                            .and_then(|q| q.pop_front());
                        if let Some(m) = m {
                            let mut c = c.clone();
                            c.element_id = m.target_element_id.clone();
                            if !c.scope_subscription {
                                c.handler = m.target_handler.clone();
                                c.boundary_id = m.target_boundary_id.clone();
                            }
                            migrated.push(c);
                        }
                    }
                    instance.compensable = migrated.into();
                    for wait in instance.compensation_waits.values_mut() {
                        remap_id(&mut wait.throw_element_id);
                        for handler in &mut wait.pending_handlers {
                            remap_id(handler);
                        }
                    }
                }
                instance.process_id = target_process_id.clone();
                // Re-pin the instance to the target definition's version so
                // `definition_for` resolves execution against the migrated-to
                // model rather than the source it was created on.
                instance.process_definition_key = *target_process_definition_key;
                for element_id in instance.active.values_mut() {
                    remap_id(element_id);
                }
                // The join maps are keyed by the join gateway's element id in
                // its flow-scope instance (#1580; migration keeps element
                // instance keys, so only the element id is remapped), so they must be remapped together to stay in sync (a stale
                // `join_instances` key would make `join_eik` miss after migration
                // and re-open an already-open join). `collect()` would silently
                // drop entries if two source ids collapse onto one target id, so
                // merge deterministically instead: sum the arrival counts, and
                // keep the smallest element-instance key for the open join.
                if !instance.join_counts.is_empty() {
                    let mut remapped: BTreeMap<JoinKey, usize> = BTreeMap::new();
                    for (mut eid, count) in std::mem::take(&mut instance.join_counts) {
                        remap_id(&mut eid.element_id);
                        *remapped.entry(eid).or_insert(0) += count;
                    }
                    instance.join_counts = remapped;
                }
                // Per-flow arrivals are keyed by the join AND name each flow's
                // source element, so both ids are remapped (the migration guard
                // has already checked every flow exists in the target).
                if !instance.join_flow_arrivals.is_empty() {
                    let mut remapped: BTreeMap<JoinKey, FlowArrivals> = BTreeMap::new();
                    for (mut eid, mut arrivals) in std::mem::take(&mut instance.join_flow_arrivals)
                    {
                        remap_id(&mut eid.element_id);
                        arrivals.remap(remap_id);
                        let merged = remapped.entry(eid).or_default();
                        for (flow, count) in arrivals.iter() {
                            merged.add(flow, count);
                        }
                    }
                    instance.join_flow_arrivals = remapped;
                }
                if !instance.join_instances.is_empty() {
                    let mut remapped: BTreeMap<JoinKey, Key> = BTreeMap::new();
                    for (mut eid, eik) in std::mem::take(&mut instance.join_instances) {
                        remap_id(&mut eid.element_id);
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
                // A legacy (pre-#1580) migration merged joins element-wide,
                // across flow-scope instances: replay that merge.
                if !*join_scoped {
                    let mut elements: Vec<ElementId> = instance
                        .join_keys()
                        .into_iter()
                        .map(|k| k.element_id)
                        .collect();
                    elements.sort();
                    elements.dedup();
                    for element_id in elements {
                        instance.collapse_legacy_join(&element_id);
                    }
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
                    match &mut timer.kind {
                        TimerKind::InterruptingBoundary {
                            boundary_element_id,
                        }
                        | TimerKind::NonInterruptingBoundary {
                            boundary_element_id,
                        } => remap_id(boundary_element_id),
                        TimerKind::EventSubProcess { esp_element_id, .. } => {
                            remap_id(esp_element_id)
                        }
                        TimerKind::IntermediateCatch => {}
                    }
                }
            }
            for sub in state.message_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                    remap_subscription_kind(&mut sub.kind);
                }
            }
            for sub in state.signal_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                    remap_subscription_kind(&mut sub.kind);
                }
            }
            for sub in state.conditional_subscriptions.values_mut() {
                if sub.instance_key == *instance_key {
                    remap_id(&mut sub.element_id);
                    remap_subscription_kind(&mut sub.kind);
                }
            }
            for incident in state.incidents.values_mut() {
                if incident.instance_key == *instance_key {
                    remap_id(&mut incident.element_id);
                    // A pending ad-hoc tool retry names the tool inside its
                    // redrive descriptor (#1630 review): re-point it with the
                    // same mapping so resolving the incident after a tool rename
                    // re-activates the renamed tool instead of falling through
                    // on the unknown source id.
                    match &mut incident.redrive {
                        Some(crate::state::IoMappingRedrive::AdHocToolActivation {
                            element_id,
                            ..
                        })
                        | Some(crate::state::IoMappingRedrive::AdHocToolJobType {
                            element_id,
                            ..
                        }) => remap_id(element_id),
                        _ => {}
                    }
                    // A pending cycle re-arm retry names its boundary / ESP
                    // trigger inside the redrive descriptor (#1596 review):
                    // re-point it with the same mapping so resolving the
                    // incident after migration looks up the renamed element.
                    if let Some(crate::state::IoMappingRedrive::TimerCycleRearm { kind, .. }) =
                        &mut incident.redrive
                    {
                        match kind {
                            TimerKind::InterruptingBoundary {
                                boundary_element_id,
                            }
                            | TimerKind::NonInterruptingBoundary {
                                boundary_element_id,
                            } => remap_id(boundary_element_id),
                            TimerKind::EventSubProcess { esp_element_id, .. } => {
                                remap_id(esp_element_id)
                            }
                            TimerKind::IntermediateCatch => {}
                        }
                    }
                }
            }
            // The scope tree (`scopes` / `scope_parents` / `scope_variables`)
            // is keyed by element-instance keys, which migration preserves, so
            // it needs no remapping: a migrated sub-process keeps its scope
            // instance and its inner tokens were re-pointed via `active` above.
            // The command handler guarantees (Zeebe `requireUnchangedFlowScope`)
            // that every target element sits in the mapped target of its
            // source flow scope, so the preserved tree stays consistent.
        }

        Event::ProcessInstanceTerminated { instance_key } => {
            state.note_terminated(*instance_key);
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
                instance.parked_joins.clear();
                instance.activating.clear();
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
                // A split gateway's carried activation-time selection (#1402):
                // same forced-teardown rationale as the join bookkeeping above.
                instance.gateway_routes.clear();
                // An event-based gateway's armed-catch race membership (#1676):
                // same forced-teardown rationale — a cancel bypasses the
                // per-catch terminal events that would remove each entry.
                instance.races.clear();
                // #2003: gateway-owned subscriptions likewise outlive no
                // whole-instance teardown.
                instance.gateway_subscriptions.clear();
                instance.gateway_triggers.clear();
                instance.compensable.clear();
                instance.compensation_waits.clear();
                instance.esp_held_payloads.clear();
                // Drop the variable payload on the terminal transition — see
                // `ProcessInstanceCompleted` above (ADR 0012).
                if !instance.variables.is_empty() {
                    instance.variables = Arc::new(BTreeMap::new());
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

/// The highest settled (quarantined) nonzero loss episode of `instance_key`
/// (#1608 review): the replicated settlement map, or — for a snapshot written
/// before that map existed — the live dedup tombstone's episode.
pub(crate) fn settled_orphan_episode(state: &State, instance_key: Key) -> Option<Key> {
    let settled = state.settled_orphan_episodes.get(&instance_key).copied();
    let live = state
        .variables_lost_quarantined
        .get(&instance_key)
        .map(|t| t.episode)
        .filter(|e| *e > 0);
    settled.max(live)
}

#[cfg(test)]
mod pending_orphan_supersede_tests {
    use std::collections::BTreeMap;

    use crate::command::OrphanKind;
    use crate::state::{apply, State};
    use crate::Event;

    // #1608: the pending-supersede set is REPLICATED state, so it survives a
    // snapshot (unlike a tail scan, which misses a supersede compacted into
    // the snapshot) and is settled only by a quarantine or a terminal event.
    #[test]
    fn supersede_is_recorded_and_settled_by_quarantine_or_terminal() {
        let mut state = State::default();
        for key in [10, 20, 30] {
            apply(
                &mut state,
                &Event::OrphanStampSuperseded {
                    instance_key: key,
                    episode: 1,
                    kind: Some(OrphanKind::Cold),
                },
            );
        }
        // A legacy frame without `kind` defaults to Resident.
        apply(
            &mut state,
            &Event::OrphanStampSuperseded {
                instance_key: 40,
                episode: 1,
                kind: None,
            },
        );
        apply(
            &mut state,
            &Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 1,
            },
        );
        apply(
            &mut state,
            &Event::ProcessInstanceCompleted { instance_key: 20 },
        );
        let pending: Vec<_> = state
            .pending_orphan_supersedes
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();
        assert_eq!(
            pending,
            vec![(30, OrphanKind::Cold), (40, OrphanKind::Resident)]
        );
    }

    // #1608 review: a delayed duplicate of a settled (quarantined, then
    // resolved) episode applies superseded but must not be recorded pending —
    // a fresh stale-stamp supersede (different episode) still is.
    #[test]
    fn supersede_of_a_settled_episode_is_not_pending() {
        let mut state = State::default();
        apply(
            &mut state,
            &Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 5,
            },
        );
        state.variables_lost_quarantined.remove(&10); // resolve lifts it
        apply(
            &mut state,
            &Event::OrphanStampSuperseded {
                instance_key: 10,
                episode: 5,
                kind: Some(OrphanKind::Cold),
            },
        );
        assert!(state.pending_orphan_supersedes.is_empty());
        apply(
            &mut state,
            &Event::OrphanStampSuperseded {
                instance_key: 10,
                episode: 6,
                kind: Some(OrphanKind::Cold),
            },
        );
        assert_eq!(
            state.pending_orphan_supersedes.get(&10),
            Some(&OrphanKind::Cold)
        );
        apply(
            &mut state,
            &Event::ProcessInstanceCompleted { instance_key: 10 },
        );
        assert!(state.settled_orphan_episodes.is_empty());
    }

    fn supersede(state: &mut State, instance_key: u64, episode: u64) {
        apply(
            state,
            &Event::OrphanStampSuperseded {
                instance_key,
                episode,
                kind: Some(OrphanKind::Cold),
            },
        );
    }

    fn resolve(state: &mut State, instance_key: u64, incident_key: u64) {
        apply(
            state,
            &Event::IncidentResolved {
                incident_key,
                instance_key,
                job_key: None,
                resolved_at: 1,
                operation_reference: None,
            },
        );
    }

    // #1608 review: E1 superseded → retry quarantined at E2 → E2 resolved →
    // the ORIGINAL E1 proposal arrives late. E1 < E2, so it is settled by the
    // high-water mark and must not become pending again (recovery would
    // otherwise re-stamp it and raise a second `VariablesLost`).
    #[test]
    fn delayed_original_after_resolved_retry_is_not_pending() {
        let mut state = State::default();
        supersede(&mut state, 10, 5);
        apply(
            &mut state,
            &Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 8,
            },
        );
        resolve(&mut state, 10, 99);
        supersede(&mut state, 10, 5);
        assert!(state.pending_orphan_supersedes.is_empty());
        // ...and it stays settled across a snapshot (recovery) round trip.
        #[cfg(feature = "serde")]
        {
            let json = serde_json::to_string(&state).expect("serialize");
            let mut back: State = serde_json::from_str(&json).expect("deserialize");
            supersede(&mut back, 10, 5);
            assert!(back.pending_orphan_supersedes.is_empty());
        }
        // A genuinely fresh episode above the mark is still pending.
        supersede(&mut state, 10, 9);
        assert_eq!(
            state.pending_orphan_supersedes.get(&10),
            Some(&OrphanKind::Cold)
        );
    }

    // #1608 review: a legacy snapshot carries a nonzero-episode tombstone but
    // no settlement map. Resolving must carry the episode into the map before
    // lifting the tombstone, so a delayed duplicate is not recorded pending.
    #[test]
    fn legacy_tombstone_resolve_then_duplicate_is_not_pending() {
        let mut state = State::default();
        state.variables_lost_quarantined.insert(
            10,
            crate::state::types::VariablesLostTombstone {
                incident_key: 99,
                episode: 5,
                terminal: false,
            },
        );
        assert!(state.settled_orphan_episodes.is_empty());
        resolve(&mut state, 10, 99);
        assert!(state.variables_lost_quarantined.is_empty());
        assert_eq!(state.settled_orphan_episodes.get(&10), Some(&5));
        supersede(&mut state, 10, 5);
        assert!(state.pending_orphan_supersedes.is_empty());
    }

    // #1608 review: completion retains active incidents, so quarantine ->
    // completion -> ResolveIncident must NOT recreate the settlement entry the
    // terminal fold cleared (it would leak for a completed instance forever).
    #[test]
    fn resolve_after_completion_does_not_recreate_settlement() {
        let mut state = State::default();
        apply(
            &mut state,
            &Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 5,
            },
        );
        assert_eq!(state.settled_orphan_episodes.get(&10), Some(&5));
        apply(
            &mut state,
            &Event::ProcessInstanceCompleted { instance_key: 10 },
        );
        assert!(state.settled_orphan_episodes.is_empty());
        resolve(&mut state, 10, 99);
        assert!(state.variables_lost_quarantined.is_empty());
        assert!(state.settled_orphan_episodes.is_empty());
    }

    // #1608 review: the "already terminal" decision on resolve must not
    // depend on residency. Here the instance has no resident shell (as on an
    // evicted replica) and the bounded terminal ring has wrapped, so only the
    // tombstone's replicated `terminal` flag can say the instance completed.
    #[test]
    fn resolve_after_completion_and_ring_wrap_without_shell_does_not_recreate_settlement() {
        let mut state = State::default();
        apply(
            &mut state,
            &Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 5,
            },
        );
        apply(
            &mut state,
            &Event::ProcessInstanceCompleted { instance_key: 10 },
        );
        let cap = u64::try_from(crate::state::RECENTLY_TERMINATED_CAP).unwrap_or(u64::MAX);
        for other in 1_000..1_000 + cap {
            apply(
                &mut state,
                &Event::ProcessInstanceCompleted {
                    instance_key: other,
                },
            );
        }
        assert!(
            !state.snapshot_shows_terminal(10),
            "precondition: residency/ring no longer show the instance terminal"
        );
        resolve(&mut state, 10, 99);
        assert!(state.variables_lost_quarantined.is_empty());
        assert!(
            state.settled_orphan_episodes.is_empty(),
            "no settlement recreated for a completed, evicted instance"
        );
    }

    // #1608 review (operator decision, option 1): a PRE-#1608 snapshot taken
    // after a stamped loss episode was quarantined AND resolved carries neither
    // a settlement map nor a tombstone. Recovery backfills the map from the
    // retained history the snapshot covers — through the live fold — BEFORE
    // replaying the tail, so a delayed duplicate's supersede is not pending.
    fn legacy_resolved_snapshot() -> (State, Vec<Event>) {
        let history = vec![
            Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 8,
            },
            Event::IncidentResolved {
                incident_key: 99,
                instance_key: 10,
                job_key: None,
                resolved_at: 1,
                operation_reference: None,
            },
        ];
        let mut state = State::default();
        for event in &history {
            apply(&mut state, event);
        }
        // Strip what a pre-#1608 build never wrote.
        state.settled_orphan_episodes.clear();
        assert!(state.variables_lost_quarantined.is_empty());
        (state, history)
    }

    #[test]
    fn backfill_from_retained_history_settles_a_legacy_resolved_episode() {
        let (mut state, history) = legacy_resolved_snapshot();
        state.backfill_settled_orphan_episodes(history.iter());
        assert_eq!(state.settled_orphan_episodes.get(&10), Some(&8));
        // The delayed duplicate (E5 <= E8) in the tail: no new report.
        supersede(&mut state, 10, 5);
        assert!(state.pending_orphan_supersedes.is_empty());
    }

    // #1608 review: a legacy snapshot carries no pending-supersede map, so a
    // covered supersede whose retry never committed is reconstructed from
    // retained history — and one the history itself settles is not.
    #[test]
    fn backfill_reconstructs_an_unresolved_supersede_for_a_legacy_snapshot() {
        let history = [
            Event::OrphanStampSuperseded {
                instance_key: 10,
                episode: 4,
                kind: Some(OrphanKind::Cold),
            },
            Event::OrphanStampSuperseded {
                instance_key: 20,
                episode: 4,
                kind: None,
            },
            Event::VariablesLostQuarantined {
                instance_key: 20,
                incident_key: 98,
                episode: 6,
            },
        ];
        let mut state = State::default();
        for event in &history {
            apply(&mut state, event);
        }
        // Strip what a pre-#1608 build never wrote.
        state.pending_orphan_supersedes.clear();
        state.settled_orphan_episodes.clear();
        state.backfill_settled_orphan_episodes(history.iter());
        assert_eq!(
            state.pending_orphan_supersedes.get(&10),
            Some(&OrphanKind::Cold)
        );
        assert!(!state.pending_orphan_supersedes.contains_key(&20));
    }

    // #1608 review: the snapshot is authoritative — retained history older
    // than it (a quarantine whose later termination the local log never
    // held) must not resurrect settlement or a pending report for an
    // instance the snapshot shows terminated.
    #[test]
    fn backfill_never_resurrects_entries_for_an_instance_the_snapshot_terminated() {
        let local_history = [
            Event::OrphanStampSuperseded {
                instance_key: 10,
                episode: 4,
                kind: None,
            },
            Event::VariablesLostQuarantined {
                instance_key: 30,
                incident_key: 97,
                episode: 5,
            },
        ];
        let mut state = State::default();
        for event in &local_history {
            apply(&mut state, event);
        }
        apply(
            &mut state,
            &Event::ProcessInstanceTerminated { instance_key: 10 },
        );
        apply(
            &mut state,
            &Event::ProcessInstanceTerminated { instance_key: 30 },
        );
        state.backfill_settled_orphan_episodes(local_history.iter());
        assert!(state.pending_orphan_supersedes.is_empty());
        assert!(state.settled_orphan_episodes.is_empty());
    }

    // Residual window (documented): history pruned, nothing to backfill — the
    // duplicate is recorded pending exactly once (one extra, resolvable
    // `VariablesLost`), and the instance itself is untouched (no data loss).
    #[test]
    fn backfill_with_pruned_history_leaves_exactly_one_pending_report() {
        let (mut state, _history) = legacy_resolved_snapshot();
        state.backfill_settled_orphan_episodes(std::iter::empty());
        supersede(&mut state, 10, 5);
        supersede(&mut state, 10, 5);
        assert_eq!(state.pending_orphan_supersedes.len(), 1);
    }

    // The backfill is the live fold: a terminal event in the retained history
    // clears the episode, and a post-#1608 map is only ever raised (high-water
    // mark), never lowered.
    #[test]
    fn backfill_honours_terminal_events_and_never_lowers_the_mark() {
        let history = [
            Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 8,
            },
            Event::ProcessInstanceTerminated { instance_key: 10 },
            Event::VariablesLostQuarantined {
                instance_key: 20,
                incident_key: 98,
                episode: 3,
            },
            Event::VariablesLostQuarantined {
                instance_key: 30,
                incident_key: 97,
                episode: 0,
            },
        ];
        let mut state = State::default();
        state.settled_orphan_episodes.insert(20, 7);
        state.backfill_settled_orphan_episodes(history.iter());
        assert_eq!(state.settled_orphan_episodes.get(&10), None);
        assert_eq!(state.settled_orphan_episodes.get(&20), Some(&7));
        assert_eq!(state.settled_orphan_episodes.get(&30), None);
    }

    // #1608 review: a delayed duplicate E5 arriving AFTER the fresh supersede
    // E9 must not replace it in the backfill's superseded-episode record —
    // comparing E5 against the settlement mark would drop the genuinely
    // unresolved E9 report and leave the dormant loss unreported.
    #[test]
    fn backfill_keeps_the_highest_superseded_episode_per_instance() {
        let history = [
            // E8 quarantined (settled), then the fresh supersede E9, then the
            // delayed duplicate E5 of the settled episode.
            Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 8,
            },
            Event::IncidentResolved {
                incident_key: 99,
                instance_key: 10,
                job_key: None,
                resolved_at: 1,
                operation_reference: None,
            },
            Event::OrphanStampSuperseded {
                instance_key: 10,
                episode: 9,
                kind: Some(OrphanKind::Resident),
            },
            Event::OrphanStampSuperseded {
                instance_key: 10,
                episode: 5,
                kind: Some(OrphanKind::Resident),
            },
        ];
        let mut state = State::default();
        for event in &history {
            apply(&mut state, event);
        }
        // A pre-#1608 snapshot: neither map recorded.
        state.pending_orphan_supersedes.clear();
        state.settled_orphan_episodes.clear();
        state.backfill_settled_orphan_episodes(history.iter());
        // E9 is still reconstructed as pending — only the delayed duplicate
        // E5 (at/below the settlement mark E8) is dropped.
        assert_eq!(
            state.pending_orphan_supersedes.get(&10),
            Some(&OrphanKind::Resident)
        );
        assert_eq!(state.settled_orphan_episodes.get(&10), Some(&8));
    }

    // #1608 review: after the bounded terminal ring wraps past an evicted
    // terminal instance, `terminated_epoch_of` can no longer prove it
    // terminated. The backfill must still reconcile against the snapshot's
    // own lifecycle state — a resident terminal shell — or a lagging
    // receiver's earlier supersede restores a loss report for completed work.
    #[test]
    fn backfill_never_restores_a_report_for_a_terminal_shell_after_ring_wrap() {
        let mut state = State::default();
        // The instance completed and is still resident (a terminal shell the
        // exporter has not yet reaped)…
        apply(
            &mut state,
            &Event::ProcessInstanceCreated {
                instance_key: 10,
                process_id: "p".to_string(),
                variables: BTreeMap::new(),
                created_at: 0,
                tags: Vec::new(),
                business_id: None,
                process_definition_key: 1,
                version: 1,
                parent_process_instance_key: None,
                parent_element_instance_key: None,
            },
        );
        apply(
            &mut state,
            &Event::ProcessInstanceCompleted { instance_key: 10 },
        );
        // …but the bounded ring has wrapped past it and the instance itself
        // was evicted before its terminal epoch could be stamped, so the
        // replicated terminal record can no longer prove the termination —
        // only the resident shell's lifecycle state can.
        state.recently_terminated.clear();
        state
            .instances
            .get_mut(&10)
            .expect("resident shell")
            .terminated_epoch = 0;
        assert_eq!(state.terminated_epoch_of(10), None);
        // The lagging receiver's retained history holds only the earlier
        // supersede/quarantine of the completed work.
        let local_history = [
            Event::OrphanStampSuperseded {
                instance_key: 10,
                episode: 4,
                kind: None,
            },
            Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 97,
                episode: 5,
            },
        ];
        state.backfill_settled_orphan_episodes(local_history.iter());
        assert!(state.pending_orphan_supersedes.is_empty());
        assert!(state.settled_orphan_episodes.is_empty());
    }

    // #1608 review: a PRE-#1608 snapshot covering quarantine -> completion
    // restores its tombstone with `terminal: false`. Normalization from the
    // retained terminal history must set the flag, so that after the ring
    // wraps a resident-shell replica and an evicted replica resolve the
    // incident to identical state.
    fn legacy_terminal_tombstone_snapshot() -> (State, Vec<Event>) {
        let history = vec![
            Event::VariablesLostQuarantined {
                instance_key: 10,
                incident_key: 99,
                episode: 5,
            },
            Event::ProcessInstanceCompleted { instance_key: 10 },
        ];
        let mut state = State::default();
        for event in &history {
            apply(&mut state, event);
        }
        // Strip what a pre-#1608 build never wrote.
        state
            .variables_lost_quarantined
            .get_mut(&10)
            .expect("tombstone")
            .terminal = false;
        (state, history)
    }

    fn wrap_ring(state: &mut State) {
        let cap = u64::try_from(crate::state::RECENTLY_TERMINATED_CAP).unwrap_or(u64::MAX);
        for other in 1_000..1_000 + cap {
            apply(
                state,
                &Event::ProcessInstanceCompleted {
                    instance_key: other,
                },
            );
        }
    }

    #[test]
    fn backfill_normalizes_a_legacy_tombstone_from_retained_terminal_history() {
        let (mut evicted, history) = legacy_terminal_tombstone_snapshot();
        evicted.backfill_settled_orphan_episodes(history.iter());
        assert!(evicted.variables_lost_quarantined[&10].terminal);
        wrap_ring(&mut evicted);
        assert!(!evicted.snapshot_shows_terminal(10));
        resolve(&mut evicted, 10, 99);
        assert!(evicted.variables_lost_quarantined.is_empty());
        assert!(evicted.settled_orphan_episodes.is_empty());
    }

    #[test]
    fn normalize_from_snapshot_lifecycle_matches_resident_and_evicted_replicas() {
        // Resident replica: the snapshot still proves terminality itself.
        let (mut resident, _) = legacy_terminal_tombstone_snapshot();
        resident.normalize_legacy_tombstones(&std::collections::BTreeSet::new());
        assert!(resident.variables_lost_quarantined[&10].terminal);
        // Evicted replica restored from the same snapshot, then the ring wraps.
        let mut evicted = resident.clone();
        resident.instances.remove(&10);
        wrap_ring(&mut evicted);
        wrap_ring(&mut resident);
        resolve(&mut resident, 10, 99);
        resolve(&mut evicted, 10, 99);
        assert_eq!(
            resident.settled_orphan_episodes,
            evicted.settled_orphan_episodes
        );
        assert!(resident.settled_orphan_episodes.is_empty());
    }

    // A genuinely missing (cold / pruned) target is NOT proven terminal: its
    // report is preserved exactly as before the ring-wrap reconciliation.
    #[test]
    fn backfill_preserves_a_report_whose_target_the_snapshot_does_not_hold() {
        let history = [Event::OrphanStampSuperseded {
            instance_key: 10,
            episode: 4,
            kind: Some(OrphanKind::Cold),
        }];
        let mut state = State::default();
        state.backfill_settled_orphan_episodes(history.iter());
        assert_eq!(
            state.pending_orphan_supersedes.get(&10),
            Some(&OrphanKind::Cold)
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn pending_supersedes_survive_a_snapshot_round_trip() {
        let mut state = State::default();
        apply(
            &mut state,
            &Event::OrphanStampSuperseded {
                instance_key: 7,
                episode: 3,
                kind: Some(OrphanKind::Cold),
            },
        );
        let json = serde_json::to_string(&state).expect("serialize");
        let back: State = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            back.pending_orphan_supersedes,
            state.pending_orphan_supersedes
        );
        // Empty set keeps the pre-#1608 snapshot shape (field omitted).
        let empty = serde_json::to_string(&State::default()).expect("serialize");
        assert!(!empty.contains("pending_orphan_supersedes"));
    }
}
