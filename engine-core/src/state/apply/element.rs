//! Element-family appliers: element lifecycle, sequence flows, joins, multi-instance, ad-hoc and compensation.

use crate::event::Event;
use crate::model::Value;
use crate::state::types::*;

pub(super) fn apply_element(state: &mut State, event: &Event) {
    match event {
        // COMPLETING is a transient transition with no state change.
        Event::ElementCompleting { .. } => {}

        // ACTIVATING registers the live token (an activation-time incident parks
        // it here, before ACTIVATED); ACTIVATED re-registers it idempotently.
        Event::ElementActivating {
            instance_key,
            element_instance_key,
            element_id,
            scope,
        }
        | Event::ElementActivated {
            instance_key,
            element_instance_key,
            element_id,
            scope,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .active
                    .insert(*element_instance_key, element_id.clone());
                // A non-zero scope records the enclosing sub-process instance.
                if *scope != 0 {
                    instance.scopes.insert(*element_instance_key, *scope);
                }
            }
        }

        Event::ElementCompleted {
            instance_key,
            element_instance_key,
            ..
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.active.remove(element_instance_key);
                instance.scopes.remove(element_instance_key);
                // Zeebe drops a scope's local variables (an activity's input
                // mappings, a sub-process's or multi-instance child's locals) when
                // the element completes. A no-op for elements that never opened a
                // scope.
                instance.scope_parents.remove(element_instance_key);
                instance.scope_variables.remove(element_instance_key);
            }
        }

        // Sequence flows are routing facts; token bookkeeping happens via the
        // activate/complete of the elements they connect.
        Event::SequenceFlowTaken { .. } => {}

        Event::ParallelJoinOpened {
            instance_key,
            element_instance_key,
            element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .join_instances
                    .insert(element_id.clone(), *element_instance_key);
            }
        }

        Event::ParallelJoinTokenArrived {
            instance_key,
            element_id,
            flow,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                match flow {
                    Some(flow) => instance
                        .join_flow_arrivals
                        .entry(element_id.clone())
                        .or_default()
                        .record(flow),
                    None => *instance.join_counts.entry(element_id.clone()).or_insert(0) += 1,
                }
            }
        }

        Event::ParallelJoinReset {
            instance_key,
            element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.join_counts.remove(element_id);
                instance.join_flow_arrivals.remove(element_id);
                instance.join_instances.remove(element_id);
            }
        }

        Event::ParallelJoinFired {
            instance_key,
            element_id,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                // Unidentified arrivals predate per-flow counting and were
                // consumed wholesale, as before #1233.
                instance.join_counts.remove(element_id);
                if let Some(arrivals) = instance.join_flow_arrivals.get_mut(element_id) {
                    arrivals.consume_one_each();
                    if arrivals.is_empty() {
                        instance.join_flow_arrivals.remove(element_id);
                    }
                }
                instance.join_instances.remove(element_id);
            }
        }

        // A compensable activity completed: remember it (in completion order) so
        // a later compensation throw event can run its handler.
        Event::CompensationSubscriptionCreated {
            instance_key,
            element_instance_key,
            element_id,
            handler,
            scope,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.compensable.push(CompensationSubscription {
                    element_instance_key: *element_instance_key,
                    element_id: element_id.clone(),
                    handler: handler.clone(),
                    scope: *scope,
                });
            }
        }

        // A compensation throw event fired: consume the compensable subscriptions
        // it triggered and record the handlers it now waits on.
        Event::CompensationTriggered {
            instance_key,
            throw_element_instance_key,
            throw_element_id,
            scope,
            handlers,
            consumed,
            handler_instance_keys,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance
                    .compensable
                    .retain(|c| !consumed.contains(&c.element_instance_key));
                instance.compensation_waits.insert(
                    *throw_element_instance_key,
                    CompensationWait {
                        throw_element_id: throw_element_id.clone(),
                        scope: *scope,
                        pending_handlers: handlers.clone(),
                        pending_handler_keys: handler_instance_keys.clone(),
                    },
                );
            }
        }

        // A compensation handler completed: drop it from its throw's outstanding
        // set, and drop the wait entirely once the last handler is done.
        Event::CompensationHandlerCompleted {
            instance_key,
            throw_element_instance_key,
            handler_element_id,
            handler_element_instance_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let done = if let Some(wait) = instance
                    .compensation_waits
                    .get_mut(throw_element_instance_key)
                {
                    // #897: match by the handler's element-instance key when the
                    // wait carries the binding (post-#897 journals); a pre-#897
                    // replayed wait has no keys and matches by element id only.
                    let pos = match handler_element_instance_key {
                        Some(key) => wait.pending_handler_keys.iter().position(|k| k == key),
                        None => wait
                            .pending_handlers
                            .iter()
                            .position(|h| h == handler_element_id),
                    };
                    if let Some(pos) = pos {
                        wait.pending_handlers.remove(pos);
                        if pos < wait.pending_handler_keys.len() {
                            wait.pending_handler_keys.remove(pos);
                        }
                    }
                    wait.pending_handlers.is_empty()
                } else {
                    false
                };
                if done {
                    instance
                        .compensation_waits
                        .remove(throw_element_instance_key);
                }
            }
        }

        // A sub-process scope was torn down: drop the compensation state scoped to
        // it (or a nested scope). Mirrors the whole-instance
        // `ProcessInstanceTerminated` sweep, but bounded to the terminated
        // sub-process — `scopes` is the terminated scope plus its descendants, so
        // any completed-activity subscription or pending handler wait belonging to
        // one of them is removed, leaving the still-live parent's compensation
        // state intact.
        Event::ScopedCompensationCleared {
            instance_key,
            scopes,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let scope_set: std::collections::HashSet<Key> = scopes.iter().copied().collect();
                instance
                    .compensable
                    .retain(|c| !scope_set.contains(&c.scope));
                instance
                    .compensation_waits
                    .retain(|_, w| !scope_set.contains(&w.scope));
            }
        }

        // A multi-instance body activated: record its runtime state so subsequent
        // child spawns/completions and the body's completion are reconstructable.
        Event::MultiInstanceActivated {
            instance_key,
            body_key,
            element_id,
            sequential,
            items,
            input_element,
            output_collection,
            output_element,
            completion_condition,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let total = items.len();
                instance.multi_instances.insert(
                    *body_key,
                    MultiInstanceState {
                        element_id: element_id.clone(),
                        sequential: *sequential,
                        items: items.clone(),
                        output_collection: output_collection.clone(),
                        output_element: output_element.clone(),
                        completion_condition: completion_condition.clone(),
                        input_element: input_element.clone(),
                        spawned: 0,
                        active: std::collections::BTreeSet::new(),
                        child_indices: std::collections::BTreeMap::new(),
                        output_values: vec![None; total],
                    },
                );
            }
        }

        // A multi-instance child activated: register its own variable scope
        // (holding its `inputElement`/`loopCounter` bindings, parented to the
        // body scope) and mark it active in the body it belongs to.
        Event::MultiInstanceChildActivated {
            instance_key,
            body_key,
            child_key,
            index,
            local_variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.insert(*child_key, *body_key);
                instance
                    .scope_variables
                    .insert(*child_key, local_variables.clone());
                if let Some(mi) = instance.multi_instances.get_mut(body_key) {
                    mi.active.insert(*child_key);
                    mi.child_indices.insert(*child_key, *index);
                    mi.spawned = mi.spawned.max(*index + 1);
                }
            }
        }

        // A multi-instance child completed: collect its output at its index and
        // drop it from the active set. Its local scope is torn down by the
        // child's `ElementCompleted` event.
        Event::MultiInstanceChildCompleted {
            instance_key,
            body_key,
            child_key,
            index,
            output,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                if let Some(mi) = instance.multi_instances.get_mut(body_key) {
                    mi.active.remove(child_key);
                    mi.child_indices.remove(child_key);
                    if let Some(slot) = mi.output_values.get_mut(*index) {
                        *slot = output.clone();
                    }
                }
            }
        }

        // A multi-instance body completed: drop its runtime state. The aggregated
        // output and the outgoing flow are carried by surrounding events.
        Event::MultiInstanceCompleted {
            instance_key,
            body_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.multi_instances.remove(body_key);
            }
        }

        // An ad-hoc container activated: register its runtime state. Its variable
        // scope is registered separately by the surrounding
        // `VariableScopeCreated` (the container element instance is the scope).
        Event::AdHocActivated {
            instance_key,
            container_key,
            element_id,
            output_collection,
            output_element,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.adhoc_instances.insert(
                    *container_key,
                    AdHocState {
                        element_id: element_id.clone(),
                        output_collection: output_collection.clone(),
                        output_element: output_element.clone(),
                        active: std::collections::BTreeSet::new(),
                        iterations: 0,
                        completion_condition_fulfilled: false,
                    },
                );
            }
        }

        // An ad-hoc tool child activated: register its own variable scope (holding
        // the activate-element seed variables, parented to the container scope)
        // and mark it active in its container.
        Event::AdHocToolActivated {
            instance_key,
            container_key,
            child_key,
            local_variables,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.scope_parents.insert(*child_key, *container_key);
                instance
                    .scope_variables
                    .insert(*child_key, local_variables.clone());
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.active.insert(*child_key);
                }
            }
        }

        // An ad-hoc tool child completed: append its output to the container's
        // `outputCollection` variable (the single source of truth, seeded to an
        // empty array on activation and visible in the container scope mid-run)
        // and drop it from the active set. Its local scope is torn down by the
        // child's `ElementCompleted` event. This applier only ever runs once the
        // append is known to be safe: `complete_adhoc_tool` DEFERS emitting
        // `AdHocToolCompleted` until its type guard confirms the target is an
        // array (parking a retry-on-resolve incident on the tool otherwise), so
        // the `Value::List` match below always holds for a declared collection.
        // A non-list is therefore only reachable when no collection is declared or
        // `output` is `None`, in which case nothing is appended (the output is
        // discarded, matching the pre-collection behaviour).
        Event::AdHocToolCompleted {
            instance_key,
            container_key,
            child_key,
            output,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                let name = instance
                    .adhoc_instances
                    .get(container_key)
                    .and_then(|a| a.output_collection.clone());
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.active.remove(child_key);
                }
                if let (Some(name), Some(value)) = (name, output) {
                    if let Some(Value::List(list)) = instance
                        .scope_variables
                        .get_mut(container_key)
                        .and_then(|m| m.get_mut(&name))
                    {
                        list.push(value.clone());
                    }
                }
            }
        }

        // The declared completion condition was satisfied with
        // `cancelRemainingInstances=false`: latch it so the container stops
        // activating new tools and completes once its children drain.
        Event::AdHocCompletionConditionFulfilled {
            instance_key,
            container_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.completion_condition_fulfilled = true;
                }
            }
        }

        // The ad-hoc container's agent job re-emitted for the next turn: bump the
        // iteration counter.
        Event::AdHocIterated {
            instance_key,
            container_key,
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                if let Some(adhoc) = instance.adhoc_instances.get_mut(container_key) {
                    adhoc.iterations = adhoc.iterations.saturating_add(1);
                }
            }
        }

        // An ad-hoc container completed: drop its runtime state. The aggregated
        // output and the outgoing flow are carried by surrounding events.
        Event::AdHocCompleted {
            instance_key,
            container_key,
            ..
        } => {
            if let Some(instance) = state.instances.get_mut(instance_key) {
                instance.adhoc_instances.remove(container_key);
            }
        }
        _ => unreachable!(),
    }
}
