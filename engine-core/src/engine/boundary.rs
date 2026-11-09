//! `impl Engine` methods: boundary concern (extracted from the monolithic engine module).

use super::*;

impl Engine {
    /// The compensation boundary events attached to the completed activity
    /// `element_id`, as `(boundary id, handler id)` pairs sorted by boundary id
    /// so selection stays deterministic when an activity carries several. The
    /// single model query behind both subscription registration
    /// ([`compensation_handlers_for`]) and the boundary lifecycle a
    /// compensation trigger journals (#1424).
    pub(crate) fn compensation_boundaries_for(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Vec<(ElementId, ElementId)> {
        self.compensation_boundary_wiring(instance_key, Some(element_id))
            .remove(element_id)
            .unwrap_or_default()
    }

    /// The canonical compensation-boundary wiring query (#1424): every
    /// compensation boundary event of the instance's process, indexed by the
    /// activity it is attached to, as `(boundary id, handler id)` pairs sorted
    /// by boundary id. Subscription registration (via
    /// [`compensation_boundaries_for`]) and trigger resolution
    /// ([`plan_compensation_fanout`], indexed once per throw) both derive from
    /// this single implementation of the wiring rules. `only_activity`
    /// restricts the index to one attached activity *before* allocating, so
    /// ordinary element completion pays no cost for unrelated boundaries.
    pub(crate) fn compensation_boundary_wiring(
        &self,
        instance_key: Key,
        only_activity: Option<&str>,
    ) -> HashMap<ElementId, Vec<(ElementId, ElementId)>> {
        let mut wiring: HashMap<ElementId, Vec<(ElementId, ElementId)>> = HashMap::new();
        if let Some(process) = self.process_of_instance(instance_key) {
            for e in process.elements.values() {
                if let ElementKind::CompensationBoundaryEvent {
                    attached_to,
                    handler,
                } = &e.kind
                {
                    if only_activity.is_some_and(|a| a != attached_to.as_str()) {
                        continue;
                    }
                    wiring
                        .entry(attached_to.clone())
                        .or_default()
                        .push((e.id.clone(), handler.clone()));
                }
            }
        }
        for wired in wiring.values_mut() {
            wired.sort();
        }
        wiring
    }

    /// The compensation handler activity ids wired — via a compensation boundary
    /// event — to the completed activity `element_id`, in boundary-id order.
    pub(crate) fn compensation_handlers_for(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Vec<ElementId> {
        self.compensation_boundaries_for(instance_key, element_id)
            .into_iter()
            .map(|(_, handler)| handler)
            .collect()
    }

    /// Computes the full fan-out of a whole-scope compensation throw running in
    /// `scope` (#901, Zeebe `triggerCompensation` +
    /// `triggerCompensationFromTopToBottom`): the subscriptions recorded
    /// directly in `scope`, in subscription-creation order (Zeebe iterates its
    /// subscription column family by record key — creation order), recursing
    /// through each **scope subscription** into the subscriptions recorded
    /// inside that sub-process's scope. Appends, in trigger order:
    ///
    /// * `handlers` / `handler_keys` — one entry per handler to activate (its
    ///   element id and the freshly minted element-instance key it will be
    ///   activated with, #897);
    /// * `handler_scopes` — aligned with `handler_keys`: the scope each
    ///   handler's subscription was recorded in (the durable handler -> scope
    ///   relationship bottom-to-top scope completion needs);
    /// * `boundaries` — the compensation boundary event (element id + freshly
    ///   minted element-instance key) each triggered boundary subscription
    ///   fires through, in trigger order (#1424): Zeebe journals its full
    ///   ACTIVATING/ACTIVATED/COMPLETING/COMPLETED lifecycle before the
    ///   handlers activate;
    /// * `scope_sub_parents` — aligned with `scope_subs`: the scope each scope
    ///   subscription was recorded in (its enclosing scope subscription, or the
    ///   throw's own scope);
    /// * `scope_subs` — the compensable element-instance key of each triggered
    ///   scope subscription (the throw rests on these until the nested fan-out
    ///   drains, Zeebe `completeFlowScopeSubscriptionFromBottomToTop`);
    /// * `consumed` — every subscription the trigger removes (boundary and
    ///   scope subscriptions alike).
    ///
    /// A subscription already consumed by an earlier throw is gone from
    /// `compensable`, so it is never re-triggered (Zeebe's
    /// `notTriggeredSubscriptions` filter).
    #[allow(
        clippy::too_many_arguments,
        reason = "fan-out accumulators, all appended in lock-step"
    )]
    pub(crate) fn plan_compensation_fanout(
        &mut self,
        instance_key: Key,
        scope: Key,
        handlers: &mut Vec<ElementId>,
        handler_keys: &mut Vec<Key>,
        handler_scopes: &mut Vec<Key>,
        boundaries: &mut Vec<(ElementId, Key)>,
        scope_subs: &mut Vec<Key>,
        scope_sub_parents: &mut Vec<Key>,
        consumed: &mut Vec<Key>,
    ) {
        // Index the instance's subscriptions by scope ONCE per throw (creation
        // order preserved within each scope), so the recursive traversal is
        // linear in the subscription count rather than re-filtering the whole
        // `compensable` list per nested scope.
        let mut by_scope: HashMap<Key, Vec<crate::state::CompensationSubscription>> =
            HashMap::new();
        if let Some(instance) = self.state.instances.get(&instance_key) {
            for c in &instance.compensable {
                by_scope.entry(c.scope).or_default().push(c.clone());
            }
        }
        // Index the model's compensation boundary wiring ONCE per throw
        // (`attached_to` -> its `(boundary id, handler)` pairs in boundary-id
        // order, one entry per boundary OCCURRENCE), so resolving each
        // triggered subscription's boundary is a hash lookup instead of a
        // model-wide scan per subscription (the same canonical query
        // registration uses). This model wiring stays REUSABLE:
        // it is never consumed.
        let boundary_wiring = self.compensation_boundary_wiring(instance_key, None);
        // Boundary-occurrence consumption is tracked PER ACTIVITY INSTANCE
        // (`(element id, element instance key)`), not per model element: every
        // execution of an activity registers its OWN subscription, so each must
        // consume its own boundary occurrence — a multi-instance activity's two
        // completed children share the one model wiring of `attached_to`, and
        // consuming that per element id would journal the boundary's lifecycle
        // for the first child only (#1424 review). Within one instance, two
        // boundaries sharing a handler each journal their OWN lifecycle
        // (boundary-id order).
        let mut consumed_wiring: HashMap<(ElementId, Key), Vec<(ElementId, ElementId)>> =
            HashMap::new();
        // Explicit stack (no recursion) so a deeply nested scope chain cannot
        // overflow; LIFO of per-scope cursors reproduces depth-first order.
        let mut stack: Vec<(
            Key,
            std::vec::IntoIter<crate::state::CompensationSubscription>,
        )> = vec![(
            scope,
            by_scope.remove(&scope).unwrap_or_default().into_iter(),
        )];
        while let Some((current, iter)) = stack.last_mut() {
            let current = *current;
            let Some(target) = iter.next() else {
                stack.pop();
                continue;
            };
            consumed.push(target.element_instance_key);
            if target.scope_subscription {
                // A completed flow scope: no handler of its own — fan the
                // trigger out into the subscriptions nested inside it.
                scope_subs.push(target.element_instance_key);
                scope_sub_parents.push(current);
                let nested = by_scope
                    .remove(&target.element_instance_key)
                    .unwrap_or_default();
                stack.push((target.element_instance_key, nested.into_iter()));
            } else {
                // Consume this subscription's boundary occurrence from its OWN
                // activity instance's wiring (a per-instance copy of the shared
                // model wiring, populated on first use): the first remaining
                // boundary on the activity wired to this handler (boundary-id
                // order). A subscription whose boundary vanished from the model
                // journals no lifecycle.
                let wiring_key = (target.element_id.clone(), target.element_instance_key);
                let wired = consumed_wiring.entry(wiring_key).or_insert_with(|| {
                    boundary_wiring
                        .get(&target.element_id)
                        .cloned()
                        .unwrap_or_default()
                });
                let boundary = wired
                    .iter()
                    .position(|(_, h)| h == &target.handler)
                    .map(|i| wired.remove(i).0);
                if let Some(boundary) = boundary {
                    boundaries.push((boundary, self.mint_key()));
                }
                handlers.push(target.handler.clone());
                handler_keys.push(self.mint_key());
                handler_scopes.push(current);
            }
        }
    }

    /// Every `CompensationSubscriptionCreated` a completing element records —
    /// the single canonical registration point for EVERY completion path (leaf
    /// `complete` / `finalize_completion`, the drained sub-process path,
    /// `finalize_subprocess`, and the multi-instance body / child paths):
    ///
    /// - nothing at all when the element completes as, or inside, a compensation
    ///   handler a throw is still waiting on: undo work is not compensable, so a
    ///   later throw must never re-trigger it (review r4478383154 and thread
    ///   PRRT_kwDOS5PLLs60AxHh, #901). Both the completing instance AND its
    ///   enclosing `scope` are checked, because the drained sub-process path has
    ///   already removed the completing instance's own ancestry by the time it
    ///   registers;
    /// - otherwise its explicit compensation-boundary subscriptions when it
    ///   carries any, else its handlerless scope subscription (a sub-process
    ///   holding nested subscriptions) — never both (#901).
    pub(crate) fn completion_compensation_subscriptions(
        &self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        scope: Key,
    ) -> Vec<Event> {
        if self.is_within_pending_compensation_handler(instance_key, element_instance_key)
            || self.is_within_pending_compensation_handler(instance_key, scope)
        {
            return Vec::new();
        }
        let explicit: Vec<Event> = self
            .compensation_handlers_for(instance_key, element_id)
            .into_iter()
            .map(|handler| Event::CompensationSubscriptionCreated {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                handler,
                scope,
                scope_subscription: false,
            })
            .collect();
        if !explicit.is_empty() {
            return explicit;
        }
        self.scope_subscription_event(instance_key, element_instance_key, element_id, scope)
            .into_iter()
            .collect()
    }

    /// The `CompensationSubscriptionCreated` event a completing **flow scope**
    /// (an embedded sub-process) records when it holds at least one compensable
    /// subscription (#901, Zeebe
    /// `BpmnCompensationSubscriptionBehaviour.isFlowScopeWithSubscriptions`):
    /// the scope's own handlerless *scope subscription*, so a later whole-scope
    /// throw can trigger the nested subscriptions through it. `None` for any
    /// other element, and for a sub-process whose scope holds no subscription
    /// (nothing to compensate inside it).
    ///
    /// `element_instance_key` is the sub-process's own element instance (the
    /// scope the nested subscriptions are recorded in); `scope` is the scope
    /// the sub-process itself completed in.
    pub(crate) fn scope_subscription_event(
        &self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        scope: Key,
    ) -> Option<Event> {
        if !self.is_subprocess(instance_key, element_id) {
            return None;
        }
        // A sub-process with its OWN compensation handler (an explicit
        // compensation boundary event) records that boundary subscription
        // instead: the explicit handler overrides the nested ones, so a
        // whole-scope throw must run only it — never both (BPMN 2.0 §10.6.5,
        // Zeebe `createCompensationSubscription`). Centralised here so every
        // sub-process completion path inherits the rule.
        if !self
            .compensation_handlers_for(instance_key, element_id)
            .is_empty()
        {
            return None;
        }
        let instance = self.state.instances.get(&instance_key)?;
        // O(1) via the derived per-scope index: every completing sub-process
        // and multi-instance child asks this, so a scan would be quadratic.
        if instance.compensable.has_scope(element_instance_key) {
            Some(Event::CompensationSubscriptionCreated {
                instance_key,
                element_instance_key,
                element_id: element_id.to_string(),
                handler: String::new(),
                scope,
                scope_subscription: true,
            })
        } else {
            None
        }
    }

    /// If the completing element instance `element_instance_key` is a
    /// compensation handler a compensation throw event is still waiting on,
    /// returns that throw event's element instance key. The match is on the
    /// handler's element-INSTANCE key (#897): a throw journals the exact handler
    /// instances it activates (`CompensationTriggered::handler_instance_keys`),
    /// so two throws in the same scope waiting on the same handler element id
    /// each retire only on their OWN handler's completion — never on the other's
    /// via an `element_id` + `min()` guess. A compensation handler has no
    /// incoming sequence flow, so it is only ever activated by compensation; its
    /// completion therefore routes back to the throw rather than along
    /// (non-existent) outgoing flows.
    ///
    /// Waits replayed from pre-#897 journals carry no handler keys; for those
    /// the caller falls back to [`pending_compensation_handler_by_id`].
    pub(crate) fn pending_compensation_handler_by_instance(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Option<Key> {
        let instance = self.state.instances.get(&instance_key)?;
        instance
            .compensation_waits
            .iter()
            .find(|(_, wait)| wait.pending_handler_keys.contains(&element_instance_key))
            .map(|(throw_eik, _)| *throw_eik)
    }

    /// Pre-#897 fallback for [`pending_compensation_handler_by_instance`]: a wait
    /// replayed from a journal written before handler-instance tracking carries
    /// no `pending_handler_keys`, so a completing handler can only be matched by
    /// its element id + scope — the historical behaviour, kept so an old journal
    /// still replays to the same state it always did. Only waits WITHOUT the
    /// instance binding are considered (a bound wait is always matched by key).
    pub(crate) fn pending_compensation_handler_by_id(
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
                wait.pending_handler_keys.is_empty()
                    && wait.scope == scope
                    && wait.pending_handlers.iter().any(|h| h == element_id)
            })
            .map(|(throw_eik, _)| *throw_eik)
            .min()
    }

    /// Whether `element_instance_key` is — or runs inside — a compensation
    /// handler a throw is still waiting on: the instance key itself is bound in
    /// a wait's `pending_handler_keys`, or some ANCESTOR element instance of it
    /// is (a multi-instance handler's body is the bound instance, so anything
    /// completing inside one of its children — the child sub-process scope, or
    /// a leaf task within it — is part of the handler's undo work).
    ///
    /// Compensation routing uses this to suppress subscription registration for
    /// work completing inside a handler: undo work is not compensable, so a
    /// later throw must never re-trigger it (review r4478383154, #901). The
    /// pre-#897 element-id fallback deliberately does NOT extend here: an
    /// unbound wait is matched by the completing element's own id + scope at
    /// its own completion, and the nested registration this guards did not
    /// exist before #901 — so no historical journal depends on it.
    pub(crate) fn is_within_pending_compensation_handler(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> bool {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return false;
        };
        if instance
            .compensation_waits
            .values()
            .any(|wait| wait.pending_handler_keys.contains(&element_instance_key))
        {
            return true;
        }
        self.ancestor_element_instances(instance_key, element_instance_key)
            .iter()
            .any(|ancestor| {
                instance
                    .compensation_waits
                    .values()
                    .any(|wait| wait.pending_handler_keys.contains(ancestor))
            })
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
            // A parked join (#1244) never activated and owns no resource: its
            // waiting tokens just vanish with the scope, writing no
            // element-instance record (Zeebe has none to terminate).
            if self.is_parked_join(instance_key, eik) {
                events.push(Event::ParallelJoinReset {
                    instance_key,
                    element_id,
                });
                continue;
            }
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
                    canceled_at: self.now,
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
            let clears_any = scopes.iter().any(|s| instance.compensable.has_scope(*s))
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
                canceled_at: self.now,
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

    /// The boundary events of `def` attached to `activity_id` that arm a
    /// runtime trigger (timer, message, signal, conditional), cloned in arming
    /// order: grouped by trigger type (timers, messages, signals, conditionals)
    /// and sorted by boundary id within a group, so arming is deterministic.
    /// Error / escalation / compensation boundaries are catch-only and never arm.
    pub(crate) fn armable_boundaries(
        def: &crate::model::ProcessDefinition,
        activity_id: &str,
    ) -> Vec<crate::model::Element> {
        fn rank(kind: &ElementKind) -> Option<(u8, &String)> {
            match kind {
                ElementKind::TimerBoundaryEvent { attached_to, .. } => Some((0u8, attached_to)),
                ElementKind::MessageBoundaryEvent { attached_to, .. } => Some((1, attached_to)),
                ElementKind::SignalBoundaryEvent { attached_to, .. } => Some((2, attached_to)),
                ElementKind::ConditionalBoundaryEvent { attached_to, .. } => Some((3, attached_to)),
                _ => None,
            }
        }
        let mut found: Vec<(u8, &crate::model::Element)> = def
            .elements
            .values()
            .filter_map(|e| match rank(&e.kind) {
                Some((r, attached_to)) if attached_to == activity_id => Some((r, e)),
                _ => None,
            })
            .collect();
        found.sort_by(|a, b| (a.0, &a.1.id).cmp(&(b.0, &b.1.id)));
        found.into_iter().map(|(_, e)| e.clone()).collect()
    }

    /// Arms a timer and/or opens a subscription for every boundary event
    /// attached to `element_id` (a service task or sub-process), returning the
    /// `TimerCreated`/`MessageSubscriptionCreated` events. Firing one later
    /// interrupts the activity (interrupting) or spawns a parallel token beside
    /// it (non-interrupting). A boundary expression (timer, message / signal
    /// name, correlation key) that fails to evaluate is the host's
    /// `EXTRACT_VALUE_ERROR` (Zeebe `BpmnEventSubscriptionBehavior
    /// .subscribeToEvents` fails the host's activation, #1398): the caller
    /// raises it as an incident and arms nothing.
    pub(crate) fn arm_boundary_events(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        scope: Key,
        element_id: &str,
    ) -> Result<Vec<Event>, expression::Failure> {
        // The scoped view the host activity's boundary expressions (timer/message
        // /signal names, correlation keys) evaluate against — its flow scope, so a
        // boundary on an activity nested in a sub-process sees the enclosing
        // scope's locals. Root-scope hosts get the shared root Arc (unchanged).
        match self.arm_boundaries_from(instance_key, element_instance_key, scope, element_id, None)
        {
            (events, None) => Ok(events),
            (_, Some((_, failure))) => Err(failure),
        }
    }

    /// Arms `element_id`'s boundary events in their deterministic arming order
    /// ([`Self::armable_boundaries`]), starting at boundary `from` (inclusive;
    /// `None` = the first). Stops at the first expression failure and returns
    /// the events armed BEFORE it together with the failing boundary's id and
    /// failure (minted keys of the failed arming are rewound). Because the order
    /// is deterministic, a caller that keeps the partial events can later resume
    /// exactly the un-armed suffix by passing the failing id as `from` — the
    /// single arming loop shared by activation and the boundary-timer retry, so
    /// a resolved incident restores ALL of the host's remaining boundaries, not
    /// only the one that failed (#1398 review). An unknown `from` (the boundary
    /// vanished in a migration) arms nothing.
    pub(crate) fn arm_boundaries_from(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        scope: Key,
        element_id: &str,
        from: Option<&str>,
    ) -> (Vec<Event>, Option<(String, expression::Failure)>) {
        let host_vars = self.variables_for_element(instance_key, scope);
        let boundaries = self
            .process_of_instance(instance_key)
            .map(|p| Self::armable_boundaries(p, element_id))
            .unwrap_or_default();
        let start = match from {
            None => 0,
            Some(id) => match boundaries.iter().position(|b| b.id == id) {
                Some(i) => i,
                None => return (Vec::new(), None),
            },
        };
        let mut events = Vec::new();
        for b in &boundaries[start..] {
            let mark = self.next_local;
            match self.arm_boundary(
                instance_key,
                element_instance_key,
                element_id,
                &host_vars,
                b,
            ) {
                Ok(event) => events.extend(event),
                Err(failure) => {
                    self.next_local = mark;
                    return (events, Some((b.id.clone(), failure)));
                }
            }
        }
        (events, None)
    }

    /// Dry-runs the arming of `element_id`'s boundary events against
    /// `host_vars` WITHOUT emitting anything (minted keys are rewound): the
    /// first expression failure, if any. Lets an all-or-nothing command
    /// (process-instance modification) reject up front exactly when arming
    /// would fail — through the single arming implementation, not a parallel
    /// validator (#1398).
    pub(crate) fn check_boundary_arming(
        &mut self,
        instance_key: Key,
        element_id: &str,
        host_vars: &HashMap<String, Value>,
    ) -> Result<(), expression::Failure> {
        let boundaries = self
            .process_of_instance(instance_key)
            .map(|p| Self::armable_boundaries(p, element_id))
            .unwrap_or_default();
        let mark = self.next_local;
        let mut result = Ok(());
        for b in &boundaries {
            if let Err(failure) = self.arm_boundary(instance_key, 0, element_id, host_vars, b) {
                result = Err(failure);
                break;
            }
        }
        self.next_local = mark;
        result
    }

    /// Arms ONE boundary event `boundary` attached to the host element instance
    /// `element_instance_key` (element `host_element_id`), evaluating its
    /// expressions against `host_vars`. `None` for a boundary kind that arms no
    /// runtime trigger. The single arming implementation shared by activation
    /// ([`Self::arm_boundary_events`]) and migration (#1374), which arms the
    /// boundaries of the *target* definition before it is pinned. An
    /// expression failure is returned for the caller to raise (#1398).
    pub(crate) fn arm_boundary(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        host_element_id: &str,
        host_vars: &HashMap<String, Value>,
        boundary: &crate::model::Element,
    ) -> Result<Option<Event>, expression::Failure> {
        let boundary_kind = |interrupting: bool| {
            let boundary_element_id = boundary.id.clone();
            if interrupting {
                state::MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id,
                }
            } else {
                state::MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id,
                }
            }
        };
        match &boundary.kind {
            ElementKind::TimerBoundaryEvent {
                duration_millis,
                interrupting,
                ..
            } => {
                let (due_at, _) = self
                    .resolve_timer(
                        host_vars,
                        boundary.timer.as_ref(),
                        self.now,
                        *duration_millis,
                    )
                    // Tag the failure with the boundary's id so the incident the
                    // host raises can carry a boundary-scoped retry (re-arm only
                    // this timer) instead of a whole-activation re-drive that
                    // would duplicate the host's already-live job and other
                    // subscriptions (#1398 review).
                    .map_err(|f| f.with_boundary_timer(&boundary.id))?;
                let timer_key = self.mint_key();
                let boundary_element_id = boundary.id.clone();
                let kind = if *interrupting {
                    state::TimerKind::InterruptingBoundary {
                        boundary_element_id,
                    }
                } else {
                    state::TimerKind::NonInterruptingBoundary {
                        boundary_element_id,
                    }
                };
                Ok(Some(Event::TimerCreated {
                    timer_key,
                    instance_key,
                    element_instance_key,
                    element_id: host_element_id.to_string(),
                    due_at,
                    kind,
                }))
            }
            ElementKind::MessageBoundaryEvent {
                message_name,
                correlation_key,
                interrupting,
                ..
            } => {
                // The message name may be a FEEL expression evaluated on activation
                // (when the boundary subscription opens) against the instance
                // variables (Zeebe parity); a failure fails the host (#1398).
                let message_name = self.resolve_event_name(host_vars, message_name)?;
                let correlation_value =
                    self.resolve_correlation_value(host_vars, correlation_key)?;
                let subscription_key = self.mint_key();
                let kind = boundary_kind(*interrupting);
                // Placement matches the intermediate catch: a boundary subscription
                // whose correlation key hashes off-partition is opened on the message
                // partition; locally we keep only an `Opening` record (the host routes
                // the open). Single-partition always opens locally.
                Ok(Some(
                    if self.subscription_partition(&correlation_value) == self.partition_id {
                        Event::MessageSubscriptionCreated {
                            subscription_key,
                            instance_key,
                            element_instance_key,
                            element_id: host_element_id.to_string(),
                            message_name,
                            correlation_key: correlation_value,
                            kind,
                        }
                    } else {
                        Event::MessageSubscriptionOpening {
                            subscription_key,
                            instance_key,
                            element_instance_key,
                            element_id: host_element_id.to_string(),
                            message_name,
                            correlation_key: correlation_value,
                            kind,
                        }
                    },
                ))
            }
            ElementKind::SignalBoundaryEvent {
                signal_name,
                interrupting,
                ..
            } => {
                // The signal name may be a FEEL expression evaluated on activation
                // (when the boundary subscription opens) against the instance
                // variables (Zeebe parity); a failure fails the host (#1398).
                let signal_name = self.resolve_event_name(host_vars, signal_name)?;
                let subscription_key = self.mint_key();
                Ok(Some(Event::SignalSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id: host_element_id.to_string(),
                    signal_name,
                    kind: boundary_kind(*interrupting),
                }))
            }
            ElementKind::ConditionalBoundaryEvent {
                condition,
                interrupting,
                ..
            } => {
                let subscription_key = self.mint_key();
                // The condition is evaluated on activation and on each change to a
                // referenced variable by the re-evaluation pass in `run`; opening
                // the subscription here is enough (an already-true condition fires
                // on the first pass, right after this activation command drains).
                Ok(Some(Event::ConditionalSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id: host_element_id.to_string(),
                    condition: condition.clone(),
                    referenced_vars: super::sorted_referenced_vars(condition),
                    kind: boundary_kind(*interrupting),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Arms the trigger of the intermediate catch event `element` on which the
    /// token `element_instance_key` rests: a timer, a message subscription, a
    /// signal subscription, or a conditional subscription. Expressions evaluate
    /// against `vars` (the element's scoped view); a failure (timer, message /
    /// signal name, correlation key) is returned as an `EXTRACT_VALUE_ERROR`
    /// [`expression::Failure`] for the caller to raise (Zeebe
    /// `CatchEventBehavior.subscribeToEvents`, #1398) — never a silently
    /// unmatchable subscription. Empty for any other kind. The single arming
    /// implementation shared by activation and migration (#1374), which
    /// re-subscribes a catch event whose trigger changed.
    pub(crate) fn arm_catch_event(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        vars: &HashMap<String, Value>,
        element: &crate::model::Element,
    ) -> Result<Vec<Event>, expression::Failure> {
        let element_id = element.id.clone();
        Ok(match &element.kind {
            ElementKind::TimerIntermediateCatchEvent { duration_millis } => {
                let (due_at, _) =
                    self.resolve_timer(vars, element.timer.as_ref(), self.now, *duration_millis)?;
                let timer_key = self.mint_key();
                vec![Event::TimerCreated {
                    timer_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    due_at,
                    kind: state::TimerKind::IntermediateCatch,
                }]
            }
            ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            } => {
                let message_name = self.resolve_event_name(vars, message_name)?;
                let correlation_value = self.resolve_correlation_value(vars, correlation_key)?;
                let subscription_key = self.mint_key();
                let kind = state::MessageSubscriptionKind::IntermediateCatch;
                // Zeebe-style placement: the canonical subscription lives on the
                // partition owning `hash(correlation_key)`. When that is this
                // partition (always so single-partition), open it locally; else
                // park the token on an `Opening` record and let the host route an
                // `OpenMessageSubscription` to the message partition.
                if self.subscription_partition(&correlation_value) == self.partition_id {
                    vec![Event::MessageSubscriptionCreated {
                        subscription_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        message_name,
                        correlation_key: correlation_value,
                        kind,
                    }]
                } else {
                    vec![Event::MessageSubscriptionOpening {
                        subscription_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        message_name,
                        correlation_key: correlation_value,
                        kind,
                    }]
                }
            }
            ElementKind::SignalIntermediateCatchEvent { signal_name } => {
                let signal_name = self.resolve_event_name(vars, signal_name)?;
                let subscription_key = self.mint_key();
                vec![Event::SignalSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    signal_name,
                    kind: state::MessageSubscriptionKind::IntermediateCatch,
                }]
            }
            ElementKind::ConditionalIntermediateCatchEvent { condition } => {
                let subscription_key = self.mint_key();
                vec![Event::ConditionalSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    condition: condition.clone(),
                    referenced_vars: super::sorted_referenced_vars(condition),
                    kind: state::MessageSubscriptionKind::IntermediateCatch,
                }]
            }
            _ => Vec::new(),
        })
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

    /// Whether active element instance `element_instance_key` is a
    /// multi-instance **body**, independent of whether its fan-out has been
    /// registered yet: a body parked on an input-mapping / input-collection
    /// incident is active before `MultiInstanceActivated` records it in
    /// `multi_instances`. A body is an active instance of a multi-instance
    /// element whose parent scope is not itself an instance of that element
    /// (its children's parent scope is the body).
    pub(crate) fn is_multi_instance_body_structural(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> bool {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return false;
        };
        if instance.multi_instances.contains_key(&element_instance_key) {
            return true;
        }
        let Some(element_id) = instance.active.get(&element_instance_key) else {
            return false;
        };
        if self.multi_instance_of(instance_key, element_id).is_none() {
            return false;
        }
        let parent = instance.scopes.get(&element_instance_key).copied();
        !parent
            .and_then(|p| instance.active.get(&p))
            .is_some_and(|pe| pe == element_id)
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
                        canceled_at: self.now,
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
                        canceled_at: self.now,
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
                    canceled_at: self.now,
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
        // A parked join (#1244) never activated: drop its waiting tokens
        // without an element-instance record.
        if self.is_parked_join(instance_key, element_instance_key) {
            self.emit(
                log,
                Event::ParallelJoinReset {
                    instance_key,
                    element_id: element_id.to_string(),
                },
            );
            return;
        }
        // Any element instance that owns an inner scope — a sub-process, a
        // multi-instance body (its children), an ad-hoc container (its tools) —
        // has that whole scope torn down; a call activity's child process
        // instance is terminated with it. The root's own runtime record (its
        // job, parked incidents, MI / ad-hoc bookkeeping) is then cleared by
        // `boundary_root_teardown_events`.
        if self.is_subprocess(instance_key, element_id)
            || self.is_multi_instance_body(instance_key, element_instance_key)
            || self.is_adhoc_container(instance_key, element_instance_key)
        {
            self.terminate_subprocess_scope(log, instance_key, element_instance_key);
        }
        if let Some(child) = self.call_activity_child_of(element_instance_key) {
            for event in self.discard_instance_events(child) {
                self.emit(log, event);
            }
        }
        for event in self.boundary_root_teardown_events(instance_key, element_instance_key) {
            self.emit(log, event);
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
        // A terminated waiting join must not keep its arrivals: reset its
        // `join_counts`/`join_flow_arrivals`/`join_instances` bookkeeping exactly
        // as descendant-scope teardown does, so a later arrival cannot fire it
        // against the cancelled branch's token.
        if self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.join_instances.get(element_id))
            == Some(&element_instance_key)
        {
            self.emit(
                log,
                Event::ParallelJoinReset {
                    instance_key,
                    element_id: element_id.to_string(),
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
    }
}
