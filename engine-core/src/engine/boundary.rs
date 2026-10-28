//! `impl Engine` methods: boundary concern (extracted from the monolithic engine module).

use super::*;

impl Engine {
    /// The compensation handler activity ids wired — via a compensation boundary
    /// event — to the completed activity `element_id`. Sorted by boundary id so
    /// selection stays deterministic when an activity carries several.
    pub(crate) fn compensation_handlers_for(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Vec<ElementId> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut wired: Vec<(ElementId, ElementId)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::CompensationBoundaryEvent {
                    attached_to,
                    handler,
                } if attached_to == element_id => Some((e.id.clone(), handler.clone())),
                _ => None,
            })
            .collect();
        wired.sort();
        wired.into_iter().map(|(_, handler)| handler).collect()
    }

    /// The compensable activities recorded in `scope` (completion order, oldest
    /// first). A compensation throw event consumes these newest-first.
    pub(crate) fn compensable_in_scope(
        &self,
        instance_key: Key,
        scope: Key,
    ) -> Vec<crate::state::CompensationSubscription> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        instance
            .compensable
            .iter()
            .filter(|c| c.scope == scope)
            .cloned()
            .collect()
    }

    /// If a completing element instance (identified by `element_id` running in
    /// `scope`) is a compensation handler a compensation throw event is still
    /// waiting on, returns that throw event's element instance key. A
    /// compensation handler has no incoming sequence flow, so it is only ever
    /// activated by compensation; its completion therefore routes back to the
    /// throw rather than along (non-existent) outgoing flows.
    pub(crate) fn pending_compensation_handler(
        &self,
        instance_key: Key,
        element_id: &str,
        scope: Key,
    ) -> Option<Key> {
        let instance = self.state.instances.get(&instance_key)?;
        instance
            .compensation_waits
            .iter()
            .filter(|(_, wait)| {
                wait.scope == scope && wait.pending_handlers.iter().any(|h| h == element_id)
            })
            .map(|(throw_eik, _)| *throw_eik)
            .min()
    }

    /// Finds the error boundary event attached to `task_element_id` that catches
    /// `error_code`, if any. When several match (a malformed model), the one with
    /// the smallest id is chosen so selection stays deterministic.
    pub(crate) fn find_error_boundary(
        &self,
        instance_key: Key,
        task_element_id: &str,
        error_code: &str,
    ) -> Option<ElementId> {
        let process = self.process_of_instance(instance_key)?;
        process
            .elements
            .values()
            .filter(|e| match &e.kind {
                ElementKind::ErrorBoundaryEvent {
                    attached_to,
                    error_code: ec,
                } => attached_to == task_element_id && ec == error_code,
                _ => false,
            })
            .map(|e| e.id.clone())
            .min()
    }

    /// The element id of an active element instance, if it is still active.
    pub(crate) fn element_id_of_instance(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Option<ElementId> {
        self.state
            .instances
            .get(&instance_key)?
            .active
            .get(&element_instance_key)
            .cloned()
    }

    /// Finds the error boundary that catches `error_code` thrown from the
    /// activity `from_element_id` (element instance `from_eik`), propagating up
    /// enclosing sub-process scopes until one is found. Returns
    /// `(boundary_id, caught_element_instance_key, caught_element_id)` — the
    /// boundary event and the activity it is attached to (the throwing task
    /// itself or an enclosing sub-process).
    pub(crate) fn find_catching_error_boundary(
        &self,
        instance_key: Key,
        from_element_id: &str,
        from_eik: Key,
        error_code: &str,
    ) -> Option<(ElementId, Key, ElementId)> {
        if let Some(boundary_id) =
            self.find_error_boundary(instance_key, from_element_id, error_code)
        {
            return Some((boundary_id, from_eik, from_element_id.to_string()));
        }
        // Propagate up the enclosing sub-process scopes (innermost first).
        for eik in self.ancestor_element_instances(instance_key, from_eik) {
            let element_id = self.element_id_of_instance(instance_key, eik)?;
            if let Some(boundary_id) =
                self.find_error_boundary(instance_key, &element_id, error_code)
            {
                return Some((boundary_id, eik, element_id));
            }
        }
        None
    }

    /// The escalation boundary attached to `activity_id` that catches
    /// `escalation_code`, or `None`. An **exact** code match wins over a
    /// **catch-all** (a boundary with an empty `escalation_code`) at the same
    /// activity (Zeebe: a specific code beats a catch-all). Among equally
    /// specific matches the lexicographically smallest boundary id is chosen for
    /// determinism. Returns the boundary id and whether it is interrupting.
    pub(crate) fn find_escalation_boundary(
        &self,
        instance_key: Key,
        activity_id: &str,
        escalation_code: &str,
    ) -> Option<(ElementId, bool)> {
        let process = self.process_of_instance(instance_key)?;
        let pick = |exact: bool| {
            process
                .elements
                .values()
                .filter(|e| match &e.kind {
                    ElementKind::EscalationBoundaryEvent {
                        attached_to,
                        escalation_code: ec,
                        ..
                    } => {
                        attached_to == activity_id
                            && if exact {
                                ec == escalation_code
                            } else {
                                ec.is_empty()
                            }
                    }
                    _ => false,
                })
                .min_by(|a, b| a.id.cmp(&b.id))
        };
        // An exact match (which, when `escalation_code` is itself empty, already
        // covers the catch-all boundaries) beats a separate catch-all fallback.
        let chosen = pick(true).or_else(|| {
            if escalation_code.is_empty() {
                None
            } else {
                pick(false)
            }
        })?;
        match &chosen.kind {
            ElementKind::EscalationBoundaryEvent { interrupting, .. } => {
                Some((chosen.id.clone(), *interrupting))
            }
            _ => None,
        }
    }

    /// Finds the escalation boundary that catches `escalation_code` raised from
    /// the scope `from_scope_eik` (the enclosing sub-process element instance the
    /// throw lives in, or `0` for the process root), propagating up the enclosing
    /// scopes until one is found. Returns `(boundary_id, caught_element_instance_key,
    /// caught_element_id, interrupting)` — the boundary event and the activity it
    /// is attached to (the enclosing sub-process). `None` when uncaught (an
    /// uncaught escalation is ignored, no incident).
    pub(crate) fn find_catching_escalation_boundary(
        &self,
        instance_key: Key,
        from_scope_eik: Key,
        escalation_code: &str,
    ) -> Option<(ElementId, Key, ElementId, bool)> {
        let scopes = std::iter::once(from_scope_eik)
            .filter(|eik| *eik != 0)
            .chain(self.ancestor_element_instances(instance_key, from_scope_eik));
        for eik in scopes {
            let element_id = self.element_id_of_instance(instance_key, eik)?;
            if let Some((boundary_id, interrupting)) =
                self.find_escalation_boundary(instance_key, &element_id, escalation_code)
            {
                return Some((boundary_id, eik, element_id, interrupting));
            }
        }
        None
    }

    /// Terminates a sub-process scope: cancels every in-play job, armed timer and
    /// open subscription on each element instance inside `scope_eik` (and nested
    /// scopes) and completes those element instances. The sub-process element
    /// instance itself is left for the caller to complete. Used when an
    /// interrupting error boundary catches an error inside the sub-process.
    pub(crate) fn terminate_subprocess_scope(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        scope_eik: Key,
    ) {
        // Mark the scope torn down for this drain so a sibling activation still
        // queued behind the interrupting boundary cannot recreate a token inside
        // it (mirrors the scoped terminate-end path; see [`torn_down_scopes`]).
        self.torn_down_scopes.insert(scope_eik);
        for event in self.scope_teardown_events(instance_key, scope_eik) {
            self.emit(log, event);
        }
    }

    /// The events that tear down a sub-process scope: for each element instance
    /// transitively inside `scope_eik`, the cancellations of **every** runtime
    /// resource it owns — its in-play job, armed timers, open message/signal/
    /// conditional subscriptions, any resting (`Created`) user task, any
    /// parallel-join bookkeeping it accumulates, and any call-activity child
    /// process instance it parks — followed by its `ElementCompleting` and
    /// `ElementCompleted`. The sub-process element instance itself is left for
    /// the caller to complete. Returns the events (does not emit) so it composes
    /// both inside an emit-driven command tail ([`terminate_subprocess_scope`])
    /// and inside a decide-only `process_step` result (a terminate end event's
    /// completion).
    ///
    /// Every descendant-owned resource must be swept here: `ElementCompleted`
    /// removes the token but does **not** cancel a user task, cancel a signal/
    /// conditional subscription, or reset a parallel join, so anything left
    /// behind can later re-drive (or be completed against) a token whose element
    /// is gone. A parked call-activity child is a *separate* process instance —
    /// not visited by [`scope_descendants`] — so it is terminated explicitly here
    /// (its own grandchildren are then reaped by the post-drain
    /// [`cascade_cancel_children`](crate::engine::Engine::cascade_cancel_children)
    /// sweep, which sees the `ProcessInstanceTerminated` this emits).
    pub(crate) fn scope_teardown_events(&self, instance_key: Key, scope_eik: Key) -> Vec<Event> {
        let mut events = Vec::new();
        for eik in self.scope_descendants(instance_key, scope_eik) {
            let element_id = self
                .element_id_of_instance(instance_key, eik)
                .unwrap_or_default();
            // Terminate any call-activity child process instance parked on this
            // element instance before the parent token is removed — the child is
            // a separate instance `scope_descendants` cannot see, so completing
            // the call-activity token without this would leave the child (and its
            // jobs/timers) running with no parent.
            if let Some(child) = self.call_activity_child_of(eik) {
                events.extend(self.discard_instance_events(child));
            }
            if let Some(job_key) = self.active_job_on(eik) {
                events.push(Event::JobCanceled {
                    job_key,
                    instance_key,
                });
            }
            events.extend(self.cancel_all_timers_on(eik));
            events.extend(self.cancel_all_subscriptions_on(eik));
            events.extend(self.cancel_all_signal_subscriptions_on(eik));
            events.extend(self.cancel_all_conditional_subscriptions_on(eik));
            events.extend(self.cancel_created_user_tasks_on(eik));
            // Resolve any incident parked on this element instance. Unlike the
            // whole-instance `ProcessInstanceTerminated` reducer (which closes
            // every open incident on the instance), scope teardown only removes
            // the token via `ElementCompleted`, which touches no incident state —
            // so without this the instance keeps a stale `hasIncident` for a
            // descendant whose element is gone, and a later `IncidentResolved`
            // could enqueue a re-drive against the vanished token.
            events.extend(self.resolve_incidents_on(instance_key, eik));
            // Drop the runtime record of a multi-instance body / ad-hoc container
            // being torn down. `ElementCompleted` clears the body/container token
            // but not the `multi_instances` / `adhoc_instances` map (those are
            // cleared only by `MultiInstanceCompleted` / `AdHocCompleted`), so
            // without this the terminated scope retains a stale active-child set
            // and output state — and the dead-scope guard would treat that stale
            // map as a live scope, letting a still-queued `ActivateMiChild` /
            // `ActivateAdHocTool` mint a child into the dead body/container.
            if let Some(instance) = self.state.instances.get(&instance_key) {
                if instance.multi_instances.contains_key(&eik) {
                    events.push(Event::MultiInstanceCompleted {
                        instance_key,
                        body_key: eik,
                    });
                }
                if instance.adhoc_instances.contains_key(&eik) {
                    events.push(Event::AdHocCompleted {
                        instance_key,
                        container_key: eik,
                        cancelled: true,
                    });
                }
            }
            // Reset a parallel join accumulating on this element instance so its
            // `join_counts`/`join_flow_arrivals`/`join_instances` bookkeeping (untouched by
            // `ElementCompleted`) cannot fire against a dead token if the scope
            // is ever re-entered.
            if self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.join_instances.get(&element_id))
                == Some(&eik)
            {
                events.push(Event::ParallelJoinReset {
                    instance_key,
                    element_id: element_id.clone(),
                });
            }
            events.push(Event::ElementCompleting {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
            });
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key: eik,
                element_id,
            });
        }
        // Scoped compensation cleanup. A completed compensable activity and an
        // in-flight compensation throw both record the `scope` (element instance
        // of the enclosing sub-process, or `0` for the root) they belong to. The
        // whole-instance `ProcessInstanceTerminated` reducer clears both maps, but
        // a *sub-process* terminate only removes the tokens above — leaving the
        // completed-activity subscriptions and pending handler waits scoped to the
        // torn-down sub-process (or a nested scope) attached to the still-live
        // parent until whole-instance eviction. That strands stale compensation
        // state on long-running parents and could run a handler for an activity in
        // a scope that no longer exists. Drop exactly the entries scoped to the
        // terminated scope and its descendants, carrying the scope set on the
        // event so replay reproduces the removal without recomputing the (by then
        // torn-down) scope tree.
        if let Some(instance) = self.state.instances.get(&instance_key) {
            let mut scopes: Vec<Key> = self.scope_descendants(instance_key, scope_eik);
            scopes.push(scope_eik);
            let scope_set: std::collections::HashSet<Key> = scopes.iter().copied().collect();
            let clears_any = instance
                .compensable
                .iter()
                .any(|c| scope_set.contains(&c.scope))
                || instance
                    .compensation_waits
                    .values()
                    .any(|w| scope_set.contains(&w.scope));
            if clears_any {
                scopes.sort_unstable();
                events.push(Event::ScopedCompensationCleared {
                    instance_key,
                    scopes,
                });
            }
        }
        events
    }

    /// Root-record teardown events for an activity interrupted at its boundary —
    /// the bookkeeping [`scope_teardown_events`](Self::scope_teardown_events)
    /// never touches for the scope *root* itself (it only sweeps descendants):
    /// the activity's own in-play job, any incident parked on it, and a
    /// multi-instance body's / ad-hoc container's active-child runtime record.
    /// That record is cleared only by `MultiInstanceCompleted` / `AdHocCompleted`
    /// (never the root's `ElementCompleted`), so without this an interrupting
    /// escalation boundary on a multi-instance or ad-hoc sub-process would leave
    /// a stale active-child set — hanging the instance or letting a still-queued
    /// child activation resurrect work after the handler.
    ///
    /// The caller still emits the root's own `ElementCompleting`/`ElementCompleted`
    /// and sweeps its descendants; this only fills the root-record gap. It is the
    /// returns-events sibling of the emit-driven root cleanup inlined in
    /// [`interrupt_activity_via_boundary`](Self::interrupt_activity_via_boundary)
    /// (#1173). A call activity is deliberately not handled: a boundary on a call
    /// activity can never catch a same-instance escalation (the callee is a
    /// separate instance), so `caught_eik` is never a call-activity host here.
    pub(crate) fn boundary_root_teardown_events(
        &self,
        instance_key: Key,
        root_eik: Key,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        if let Some(job_key) = self.active_job_on(root_eik) {
            events.push(Event::JobCanceled {
                job_key,
                instance_key,
            });
        }
        events.extend(self.resolve_incidents_on(instance_key, root_eik));
        if let Some(instance) = self.state.instances.get(&instance_key) {
            if instance.multi_instances.contains_key(&root_eik) {
                events.push(Event::MultiInstanceCompleted {
                    instance_key,
                    body_key: root_eik,
                });
            }
            if instance.adhoc_instances.contains_key(&root_eik) {
                events.push(Event::AdHocCompleted {
                    instance_key,
                    container_key: root_eik,
                    cancelled: true,
                });
            }
        }
        events
    }
    /// `element_instance_key`, sorted by key. `ElementCompleted` alone leaves a
    /// user task queryable as `Created` (and completable), so scope teardown must
    /// cancel it explicitly — mirroring the whole-instance
    /// [`discard_instance_events`](crate::engine::Engine::discard_instance_events)
    /// sweep, per element instance.
    pub(crate) fn cancel_created_user_tasks_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut tasks: Vec<&state::UserTask> = self
            .state
            .user_tasks
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::UserTaskState::Created
            })
            .collect();
        tasks.sort_by_key(|t| t.key);
        tasks
            .into_iter()
            .map(|t| Event::UserTaskCanceled {
                user_task_key: t.key,
                instance_key: t.instance_key,
            })
            .collect()
    }

    /// The `IncidentResolved` events for every **active** incident parked on
    /// `element_instance_key`, sorted by key. Mirrors — per element instance —
    /// the incident-closing the whole-instance `ProcessInstanceTerminated`
    /// reducer does for a top-level terminate: when scope teardown removes a
    /// descendant token, any incident sitting on it must be resolved too, or the
    /// instance keeps a stale `hasIncident` for a vanished element (and a later
    /// external resolve could re-drive the dead token). `resolved_at` is the
    /// command clock (`now`); a job-incident's parked job key rides along, but
    /// the `IncidentResolved` reducer only returns it to the activatable pool
    /// when the job is still `Failed`. During this teardown the same batch emits
    /// `JobCanceled` first, so the job is already terminal by the time its
    /// incident is resolved and the reducer's `Failed`-only guard intentionally
    /// leaves it cancelled rather than resurrecting it.
    pub(crate) fn resolve_incidents_on(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Vec<Event> {
        let mut incidents: Vec<&state::Incident> = self
            .state
            .incidents
            .values()
            .filter(|i| {
                i.instance_key == instance_key
                    && i.element_instance_key == element_instance_key
                    && i.state == state::IncidentState::Active
            })
            .collect();
        incidents.sort_by_key(|i| i.key);
        incidents
            .into_iter()
            .map(|i| Event::IncidentResolved {
                incident_key: i.key,
                instance_key,
                job_key: i.job_key,
                resolved_at: self.now,
                operation_reference: None,
            })
            .collect()
    }

    /// Cancels every armed (`Created`) timer resting on `element_instance_key`,
    /// regardless of kind, returning the `TimerCanceled` events (sorted by key).
    /// Used to tear down a sub-process scope on interruption.
    pub(crate) fn cancel_all_timers_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::TimerState::Created
            })
            .collect();
        timers.sort_by_key(|t| t.key);
        timers
            .into_iter()
            .map(|t| Event::TimerCanceled {
                timer_key: t.key,
                instance_key: t.instance_key,
                element_instance_key: t.element_instance_key,
                element_id: t.element_id.clone(),
            })
            .collect()
    }

    /// Cancels every open subscription resting on `element_instance_key`,
    /// regardless of kind, returning the `MessageSubscriptionCanceled` events
    /// (sorted by key). Used to tear down a sub-process scope on interruption.
    pub(crate) fn cancel_all_subscriptions_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut subs: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && matches!(
                        s.state,
                        state::MessageSubscriptionState::Open
                            | state::MessageSubscriptionState::Opening
                    )
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(Self::disarm_subscription_event)
            .collect()
    }

    /// The disarm event for a subscription being torn down. A cross-partition
    /// parked placeholder (state `Opening`, its canonical record lives on the
    /// `hash(correlation_key)` partition) emits a routable
    /// `MessageSubscriptionClosing` so the host disarms the canonical record;
    /// any other (local, `Open`) subscription emits a plain
    /// `MessageSubscriptionCanceled`. Single-partition runs only ever hold
    /// `Open` subs, so they always take the latter branch and stay
    /// byte-identical.
    pub(crate) fn disarm_subscription_event(s: &state::MessageSubscription) -> Event {
        if s.state == state::MessageSubscriptionState::Opening {
            Event::MessageSubscriptionClosing {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
                message_name: s.message_name.clone(),
                correlation_key: s.correlation_key.clone(),
            }
        } else {
            Event::MessageSubscriptionCanceled {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
            }
        }
    }

    /// The key of a still-in-play job parked on `element_instance_key`, if any.
    /// A job is "in play" until it is completed, errored or cancelled; this is
    /// the job an interrupting boundary event cancels. At most one such job
    /// exists per element instance.
    pub(crate) fn active_job_on(&self, element_instance_key: Key) -> Option<Key> {
        self.state
            .jobs
            .values()
            .find(|j| {
                j.element_instance_key == element_instance_key
                    && !matches!(
                        j.state,
                        state::JobState::Completed
                            | state::JobState::Errored
                            | state::JobState::Canceled
                    )
            })
            .map(|j| j.key)
    }

    /// All interrupting timer boundary events attached to `activity_id`, as
    /// `(boundary_id, duration_millis)` sorted by boundary id (so arming is
    /// deterministic). Empty when the activity has no timer boundaries.
    /// Arms a timer and/or opens a subscription for every boundary event
    /// attached to `element_id` (a service task or sub-process), returning the
    /// `TimerCreated`/`MessageSubscriptionCreated` events. Firing one later
    /// interrupts the activity (interrupting) or spawns a parallel token beside
    /// it (non-interrupting).
    pub(crate) fn arm_boundary_events(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        scope: Key,
        element_id: &str,
    ) -> Vec<Event> {
        // The scoped view the host activity's boundary expressions (timer/message
        // /signal names, correlation keys) evaluate against — its flow scope, so a
        // boundary on an activity nested in a sub-process sees the enclosing
        // scope's locals. Root-scope hosts get the shared root Arc (unchanged).
        let host_vars = self.variables_for_element(instance_key, scope);
        let mut events = Vec::new();
        for (boundary_id, duration_millis, interrupting) in
            self.attached_timer_boundaries(instance_key, element_id)
        {
            let timer_key = self.mint_key();
            let timer_def = self.timer_def_of(instance_key, &boundary_id);
            let (due_at, _) =
                self.resolve_timer(&host_vars, timer_def.as_ref(), self.now, duration_millis);
            let kind = if interrupting {
                state::TimerKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::TimerKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            events.push(Event::TimerCreated {
                timer_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                due_at,
                kind,
            });
        }
        for (boundary_id, message_name, correlation_key, interrupting) in
            self.attached_message_boundaries(instance_key, element_id)
        {
            let subscription_key = self.mint_key();
            // The message name may be a FEEL expression evaluated on activation
            // (when the boundary subscription opens) against the instance
            // variables (Zeebe parity).
            let message_name = self.resolve_event_name(&host_vars, &message_name);
            let correlation_value = self.resolve_correlation_value(&host_vars, &correlation_key);
            let kind = if interrupting {
                state::MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            // Placement matches the intermediate catch: a boundary subscription
            // whose correlation key hashes off-partition is opened on the message
            // partition; locally we keep only an `Opening` record (the host routes
            // the open). Single-partition always opens locally.
            if self.subscription_partition(&correlation_value) == self.partition_id {
                events.push(Event::MessageSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.to_string(),
                    message_name,
                    correlation_key: correlation_value,
                    kind,
                });
            } else {
                events.push(Event::MessageSubscriptionOpening {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.to_string(),
                    message_name,
                    correlation_key: correlation_value,
                    kind,
                });
            }
        }
        for (boundary_id, signal_name, interrupting) in
            self.attached_signal_boundaries(instance_key, element_id)
        {
            let subscription_key = self.mint_key();
            // The signal name may be a FEEL expression evaluated on activation
            // (when the boundary subscription opens) against the instance
            // variables (Zeebe parity).
            let signal_name = self.resolve_event_name(&host_vars, &signal_name);
            let kind = if interrupting {
                state::MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            events.push(Event::SignalSubscriptionCreated {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                signal_name,
                kind,
            });
        }
        for (boundary_id, condition, interrupting) in
            self.attached_conditional_boundaries(instance_key, element_id)
        {
            let subscription_key = self.mint_key();
            let referenced_vars = super::sorted_referenced_vars(&condition);
            let kind = if interrupting {
                state::MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            } else {
                state::MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id: boundary_id,
                }
            };
            // The condition is evaluated on activation and on each change to a
            // referenced variable by the re-evaluation pass in `run`; opening the
            // subscription here is enough (an already-true condition fires on the
            // first pass, right after this activation command drains).
            events.push(Event::ConditionalSubscriptionCreated {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                condition,
                referenced_vars,
                kind,
            });
        }
        events
    }

    /// Whether `element_id` is an embedded sub-process (interrupting it must tear
    /// down its whole inner token scope, not just cancel a job).
    pub(crate) fn is_subprocess(&self, instance_key: Key, element_id: &str) -> bool {
        matches!(
            self.element_kind(instance_key, element_id),
            Some(ElementKind::SubProcess { .. })
        )
    }

    /// Whether `element_instance_key` is a live ad-hoc sub-process container —
    /// i.e. it has an `adhoc_instances` runtime record. Unlike [`is_subprocess`]
    /// (which keys off the static `ElementKind`), an ad-hoc container flattens to
    /// a job-backed `ServiceTask` element, so its ad-hoc nature is only visible
    /// in runtime state. Used by [`interrupt_activity_via_boundary`] to route an
    /// interrupting boundary through inner-scope teardown (#1155).
    pub(crate) fn is_adhoc_container(&self, instance_key: Key, element_instance_key: Key) -> bool {
        self.state
            .instances
            .get(&instance_key)
            .is_some_and(|i| i.adhoc_instances.contains_key(&element_instance_key))
    }

    /// Whether `element_instance_key` is a live multi-instance **body** — i.e. it
    /// has a `multi_instances` runtime record. An interrupting boundary attached
    /// to a multi-instance activity is armed on the body (see
    /// `run_mi_body_activation`), so this is the routing signal
    /// [`interrupt_activity_via_boundary`] uses to tear down the whole loop
    /// (every active child of any kind, plus the body's own runtime record)
    /// rather than falling through to the single-activity branches — which would
    /// leave the children (and their jobs/scopes) running and the body's stale
    /// `active` set alive (#1170).
    pub(crate) fn is_multi_instance_body(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> bool {
        self.state
            .instances
            .get(&instance_key)
            .is_some_and(|i| i.multi_instances.contains_key(&element_instance_key))
    }

    /// Interrupts the activity `element_instance_key`/`element_id` because an
    /// interrupting timer or message boundary fired on it: tears down the work it
    /// owns, completes its element instance, and disarms any sibling boundaries.
    /// A service task's job is cancelled; a sub-process's whole inner token scope
    /// is terminated; a call activity's spawned child instance is cancelled. The
    /// caller then routes the boundary's outgoing flow.
    pub(crate) fn interrupt_activity_via_boundary(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
    ) {
        if self.is_multi_instance_body(instance_key, element_instance_key) {
            // An interrupting boundary attached to a multi-instance activity is
            // armed on its BODY (`run_mi_body_activation`), so `element_instance_key`
            // here is the multi-instance body — not one child. Interrupting it must
            // tear down the WHOLE loop: every still-active child of every kind (its
            // job, sub-process scope, ad-hoc inner scope, or call-activity child)
            // and the body's own runtime record. `terminate_subprocess_scope`
            // sweeps each descendant child generically — that is precisely the
            // per-descendant-kind teardown `scope_teardown_events` was built for —
            // so the sub-process / ad-hoc / call-activity / service-task branches
            // do not need re-implementing per child. It clears each child token via
            // `ElementCompleted`, but `MultiInstanceState.active` is only ever
            // cleared by `MultiInstanceChildCompleted` / `MultiInstanceCompleted`
            // (never `ElementCompleted`, `state.rs`), so we must drop the body's
            // runtime record explicitly — otherwise the terminated loop keeps a
            // stale active-child set and the MI body holds the process open after
            // the boundary flow finishes (#1170). `scope_teardown_events` emits
            // `MultiInstanceCompleted` only for a *nested* MI body it finds among
            // the descendants, never for the scope root itself, so this is the
            // body's own clear.
            //
            // Cancel the body's OWN execution-listener job first, if one is live.
            // The boundary is armed before the body's `start`-listener gate
            // (`run_mi_body_activation`), and the body parks on a start/end
            // listener job while its chain runs, so firing during a listener would
            // otherwise leave that job live on a removed body — mirrors the ad-hoc
            // container branch below.
            if let Some(job_key) = self.active_job_on(element_instance_key) {
                self.emit(
                    log,
                    Event::JobCanceled {
                        job_key,
                        instance_key,
                    },
                );
            }
            // Resolve any incident sitting on the body ROOT itself before it is
            // torn down. `terminate_subprocess_scope` resolves incidents for every
            // *descendant* it sweeps (`scope_teardown_events` -> `resolve_incidents_on`),
            // but never for the scope root — so a body parked on a FAILED
            // start/end-listener job (its `JobNoRetries` incident) would keep the
            // completed instance carrying a stale `hasIncident` for a vanished body,
            // and a later external resolve could re-drive the dead token. The
            // `JobCanceled` above makes the parked job terminal first, so the
            // `IncidentResolved` reducer's `Failed`-only guard leaves it cancelled.
            for ev in self.resolve_incidents_on(instance_key, element_instance_key) {
                self.emit(log, ev);
            }
            self.terminate_subprocess_scope(log, instance_key, element_instance_key);
            self.emit(
                log,
                Event::MultiInstanceCompleted {
                    instance_key,
                    body_key: element_instance_key,
                },
            );
        } else if self.is_subprocess(instance_key, element_id) {
            // Cancel every job/timer/subscription inside the sub-process and
            // complete its inner element instances first.
            self.terminate_subprocess_scope(log, instance_key, element_instance_key);
        } else if self.is_adhoc_container(instance_key, element_instance_key) {
            // An ad-hoc sub-process (#1155) is a job-backed activity whose
            // activated tools live in its own inner scope (each tool hangs off a
            // dedicated `#innerInstance` parented to the container). An
            // interrupting boundary must tear down that scope — the tool, its
            // `#innerInstance`, and any open user task — exactly like a plain
            // sub-process, otherwise the container leaves the flow via the
            // boundary while its inner tokens survive and the instance hangs
            // `Active` forever. Cancel the container's own agent job (if one is
            // still in play), sweep the inner scope, then drop the container's
            // ad-hoc runtime record so a still-queued `ActivateAdHocTool` cannot
            // mint a tool into the dead container (mirrors the dead-scope guard
            // in `scope_teardown_events`).
            if let Some(job_key) = self.active_job_on(element_instance_key) {
                self.emit(
                    log,
                    Event::JobCanceled {
                        job_key,
                        instance_key,
                    },
                );
            }
            self.terminate_subprocess_scope(log, instance_key, element_instance_key);
            self.emit(
                log,
                Event::AdHocCompleted {
                    instance_key,
                    container_key: element_instance_key,
                    cancelled: true,
                },
            );
        } else if let Some(child) = self.call_activity_child_of(element_instance_key) {
            // A call activity parks its token on a distinct child process
            // instance (Zeebe parity). Interrupting the call activity via a
            // boundary event must cancel that child too — the parent token is
            // about to leave via the boundary flow, so a surviving child would
            // be orphaned (running with nothing left to complete it). The
            // command tail's `cascade_cancel_children` reaps any transitive
            // grandchildren off the `ProcessInstanceTerminated` emitted here.
            self.discard_and_terminate_instance(log, child);
        } else if let Some(job_key) = self.active_job_on(element_instance_key) {
            self.emit(
                log,
                Event::JobCanceled {
                    job_key,
                    instance_key,
                },
            );
        }
        self.emit(
            log,
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
        self.emit(
            log,
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
        // Disarm any sibling boundary timers and message subscriptions on it.
        for event in self.cancel_boundary_timers_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_boundary_message_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_boundary_signal_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_boundary_conditional_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }
    }

    pub(crate) fn attached_timer_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, u64, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, u64, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::TimerBoundaryEvent {
                    attached_to,
                    duration_millis,
                    interrupting,
                    repeating: _,
                } if attached_to == activity_id => {
                    Some((e.id.clone(), *duration_millis, *interrupting))
                }
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every armed (`Created`) boundary timer resting on
    /// `element_instance_key` (interrupting or non-interrupting), returning the
    /// `TimerCanceled` events. Called when the guarded activity leaves the flow
    /// another way (it completed normally or a different boundary interrupted
    /// it), so a stale timer never fires later.
    pub(crate) fn cancel_boundary_timers_on(&self, element_instance_key: Key) -> Vec<Event> {
        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                t.element_instance_key == element_instance_key
                    && t.state == state::TimerState::Created
                    && matches!(
                        t.kind,
                        state::TimerKind::InterruptingBoundary { .. }
                            | state::TimerKind::NonInterruptingBoundary { .. }
                    )
            })
            .collect();
        timers.sort_by_key(|t| t.key);
        timers
            .into_iter()
            .map(|t| Event::TimerCanceled {
                timer_key: t.key,
                instance_key: t.instance_key,
                element_instance_key: t.element_instance_key,
                element_id: t.element_id.clone(),
            })
            .collect()
    }

    /// All interrupting message boundary events attached to `activity_id`, as
    /// `(boundary_id, message_name, correlation_key)` sorted by boundary id (so
    /// arming is deterministic). Empty when the activity has no message
    /// boundaries.
    pub(crate) fn attached_message_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, String, String, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, String, String, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::MessageBoundaryEvent {
                    attached_to,
                    message_name,
                    correlation_key,
                    interrupting,
                } if attached_to == activity_id => Some((
                    e.id.clone(),
                    message_name.clone(),
                    correlation_key.clone(),
                    *interrupting,
                )),
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every open boundary message subscription resting on
    /// `element_instance_key` (interrupting or non-interrupting), returning the
    /// `MessageSubscriptionCanceled` events. Called when the guarded activity
    /// leaves the flow another way (it completed normally or a different boundary
    /// interrupted it), so a stale subscription never correlates later.
    pub(crate) fn cancel_boundary_message_subscriptions_on(
        &self,
        element_instance_key: Key,
    ) -> Vec<Event> {
        let mut subs: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && matches!(
                        s.state,
                        state::MessageSubscriptionState::Open
                            | state::MessageSubscriptionState::Opening
                    )
                    && matches!(
                        s.kind,
                        state::MessageSubscriptionKind::InterruptingBoundary { .. }
                            | state::MessageSubscriptionKind::NonInterruptingBoundary { .. }
                    )
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(Self::disarm_subscription_event)
            .collect()
    }

    /// All signal boundary events attached to `activity_id`, as
    /// `(boundary_id, signal_name, interrupting)` sorted by boundary id.
    pub(crate) fn attached_signal_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, String, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, String, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::SignalBoundaryEvent {
                    attached_to,
                    signal_name,
                    interrupting,
                } if attached_to == activity_id => {
                    Some((e.id.clone(), signal_name.clone(), *interrupting))
                }
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every open boundary signal subscription resting on
    /// `element_instance_key`, returning the `SignalSubscriptionCanceled` events.
    /// Mirrors [`Self::cancel_boundary_message_subscriptions_on`].
    pub(crate) fn cancel_boundary_signal_subscriptions_on(
        &self,
        element_instance_key: Key,
    ) -> Vec<Event> {
        let mut subs: Vec<&state::SignalSubscription> = self
            .state
            .signal_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && s.state == state::MessageSubscriptionState::Open
                    && matches!(
                        s.kind,
                        state::MessageSubscriptionKind::InterruptingBoundary { .. }
                            | state::MessageSubscriptionKind::NonInterruptingBoundary { .. }
                    )
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(|s| Event::SignalSubscriptionCanceled {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
            })
            .collect()
    }

    /// All conditional boundary events attached to `activity_id`, as
    /// `(boundary_id, condition, interrupting)` sorted by boundary id.
    pub(crate) fn attached_conditional_boundaries(
        &self,
        instance_key: Key,
        activity_id: &str,
    ) -> Vec<(ElementId, String, bool)> {
        let Some(process) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        let mut found: Vec<(ElementId, String, bool)> = process
            .elements
            .values()
            .filter_map(|e| match &e.kind {
                ElementKind::ConditionalBoundaryEvent {
                    attached_to,
                    condition,
                    interrupting,
                } if attached_to == activity_id => {
                    Some((e.id.clone(), condition.clone(), *interrupting))
                }
                _ => None,
            })
            .collect();
        found.sort();
        found
    }

    /// Cancels every open boundary conditional subscription resting on
    /// `element_instance_key`, returning the `ConditionalSubscriptionCanceled`
    /// events. Mirrors [`Self::cancel_boundary_signal_subscriptions_on`].
    pub(crate) fn cancel_boundary_conditional_subscriptions_on(
        &self,
        element_instance_key: Key,
    ) -> Vec<Event> {
        let mut subs: Vec<&state::ConditionalSubscription> = self
            .state
            .conditional_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && s.state == state::MessageSubscriptionState::Open
                    && matches!(
                        s.kind,
                        state::MessageSubscriptionKind::InterruptingBoundary { .. }
                            | state::MessageSubscriptionKind::NonInterruptingBoundary { .. }
                    )
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(|s| Event::ConditionalSubscriptionCanceled {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
            })
            .collect()
    }

    /// Cancels every open signal subscription resting on `element_instance_key`
    /// (any kind: boundary or intermediate catch), returning the
    /// `SignalSubscriptionCanceled` events sorted by key.
    pub(crate) fn cancel_all_signal_subscriptions_on(
        &self,
        element_instance_key: Key,
    ) -> Vec<Event> {
        let mut subs: Vec<&state::SignalSubscription> = self
            .state
            .signal_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && s.state == state::MessageSubscriptionState::Open
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(|s| Event::SignalSubscriptionCanceled {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
            })
            .collect()
    }

    /// Cancels every open conditional subscription resting on
    /// `element_instance_key` (any kind), returning the
    /// `ConditionalSubscriptionCanceled` events sorted by key.
    pub(crate) fn cancel_all_conditional_subscriptions_on(
        &self,
        element_instance_key: Key,
    ) -> Vec<Event> {
        let mut subs: Vec<&state::ConditionalSubscription> = self
            .state
            .conditional_subscriptions
            .values()
            .filter(|s| {
                s.element_instance_key == element_instance_key
                    && s.state == state::MessageSubscriptionState::Open
            })
            .collect();
        subs.sort_by_key(|s| s.key);
        subs.into_iter()
            .map(|s| Event::ConditionalSubscriptionCanceled {
                subscription_key: s.key,
                instance_key: s.instance_key,
                element_instance_key: s.element_instance_key,
                element_id: s.element_id.clone(),
            })
            .collect()
    }

    /// Terminates a single active element instance as part of a
    /// [`Command::ModifyInstance`] terminate instruction. Tears down whatever
    /// work the element instance owns — a service task's job, a resting user
    /// task, a sub-process's whole inner token scope, armed timers, and open
    /// message/signal/conditional subscriptions (its own catch-event
    /// subscriptions and any attached boundary events) — then completes its
    /// element instance. Unlike [`Self::interrupt_activity_via_boundary`] this
    /// also cancels a resting user task and works for any active element
    /// instance, not only activities that carry boundary events. User-task
    /// `canceling` listeners are not run: `modify` is an operational action.
    pub(crate) fn terminate_element_instance(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
    ) {
        if self.is_subprocess(instance_key, element_id) {
            self.terminate_subprocess_scope(log, instance_key, element_instance_key);
        } else if let Some(job_key) = self.active_job_on(element_instance_key) {
            self.emit(
                log,
                Event::JobCanceled {
                    job_key,
                    instance_key,
                },
            );
        }

        let mut user_task_keys: Vec<Key> = self
            .state
            .user_tasks
            .values()
            .filter(|t| {
                t.instance_key == instance_key
                    && t.element_instance_key == element_instance_key
                    && t.state == state::UserTaskState::Created
            })
            .map(|t| t.key)
            .collect();
        user_task_keys.sort_unstable();
        for user_task_key in user_task_keys {
            self.emit(
                log,
                Event::UserTaskCanceled {
                    user_task_key,
                    instance_key,
                },
            );
        }

        for event in self.cancel_all_timers_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_all_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_all_signal_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }
        for event in self.cancel_all_conditional_subscriptions_on(element_instance_key) {
            self.emit(log, event);
        }

        self.emit(
            log,
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
        self.emit(
            log,
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
            },
        );
    }
}
