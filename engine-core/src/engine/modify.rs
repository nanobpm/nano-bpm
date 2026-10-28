//! Process-instance modification (#1374): the behaviour behind
//! [`Command::ModifyInstance`], mirroring Zeebe's
//! `ProcessInstanceModificationModifyProcessor`.
//!
//! The command is **validated completely before anything is emitted**
//! ([`Engine::plan_modification`]) so it applies atomically — every instruction
//! or none — and is then applied ([`Engine::apply_modification`]):
//!
//! 1. activations, in instruction order: missing flow scopes are created
//!    (activated *without* running their start event, boundary events armed,
//!    Zeebe `ElementActivationBehavior`), variable instructions are written to
//!    their scope, and the element is queued (or activated synchronously when a
//!    variable instruction targets the element itself, so its local scope
//!    exists to write into). Instructions of one command share the flow-scope
//!    instances it creates;
//! 2. terminations (by key, then the outermost active instances of each
//!    terminate-by-id element), each followed by the flow-scope cascade:
//!    a scope left without active children that no activation needs is
//!    terminated too, up to the process instance (Zeebe
//!    `terminateFlowScopes`).
//!
//! All flow-scope tree walks go through [`super::flow_scope`].

use super::flow_scope::{element_flow_scope_chain, FlowScopeResolution};
use super::*;
use crate::command::{ActivateElementInstruction, VariableInstruction};

/// One validated activation: where it lands and which variables go where.
struct PlannedActivation {
    element_id: ElementId,
    resolution: FlowScopeResolution,
    /// Root-scope variables (the instruction's `variables` plus variable
    /// instructions without / with the process id as scope).
    global: Vec<HashMap<String, Value>>,
    /// Variable instructions on a flow scope (or the element itself), by
    /// scope element id, in instruction order.
    scoped: Vec<(ElementId, HashMap<String, Value>)>,
}

/// A fully validated modification, ready to apply.
pub(super) struct ModificationPlan {
    activations: Vec<PlannedActivation>,
    /// Element instances to terminate, in application order (by-key first,
    /// then by-id), deduplicated, none nested in another.
    terminations: Vec<Key>,
}

impl Engine {
    /// Validates a `ModifyInstance` against the current state without emitting
    /// anything (Zeebe validation order: activate targets, ancestors, variable
    /// scopes, terminate targets, then flow-scope/termination conflicts).
    pub(super) fn plan_modification(
        &self,
        instance_key: Key,
        activate_instructions: &[ActivateElementInstruction],
        terminate_instructions: &[Key],
        terminate_element_ids: &[ElementId],
    ) -> Result<ModificationPlan, EngineError> {
        let instance = self
            .state
            .instances
            .get(&instance_key)
            .ok_or(EngineError::InstanceNotFound { instance_key })?;
        let process_id = instance.process_id.clone();
        let def = self
            .process_of_instance(instance_key)
            .ok_or(EngineError::InstanceNotFound { instance_key })?;

        let mut activations = Vec::with_capacity(activate_instructions.len());
        for a in activate_instructions {
            let Some(element) = def.element(&a.element_id) else {
                return Err(EngineError::ElementNotFound {
                    instance_key,
                    element_id: a.element_id.clone(),
                });
            };
            let not_activatable = |reason: String| EngineError::ElementNotActivatable {
                instance_key,
                process_id: process_id.clone(),
                element_id: a.element_id.clone(),
                reason,
            };
            let unsupported_type = if element.kind.is_start_event() {
                Some("START_EVENT")
            } else if element.kind.attached_to().is_some() {
                Some("BOUNDARY_EVENT")
            } else {
                None
            };
            if let Some(ty) = unsupported_type {
                return Err(not_activatable(format!(
                    "The activation of elements with type '{ty}' is not supported"
                )));
            }

            // Zeebe's `-1` (and an absent key) = no ancestor selection.
            let ancestor = a.ancestor_element_instance_key;
            if let Some(body) = ancestor.filter(|k| instance.multi_instances.contains_key(k)) {
                return Err(not_activatable(format!(
                    "The activation of elements inside a multi-instance body is not supported: \
                     ancestor '{body}' is a multi-instance body"
                )));
            }
            let resolution = self.resolve_flow_scope(instance_key, &a.element_id, ancestor)?;
            if let Some(mi_scope) = resolution
                .to_create
                .iter()
                .find(|s| self.multi_instance_of(instance_key, s).is_some())
            {
                return Err(not_activatable(format!(
                    "The activation of elements inside a multi-instance element '{mi_scope}' \
                     that has no active instance is not supported"
                )));
            }

            let chain = element_flow_scope_chain(def, &a.element_id);
            let mut global = Vec::new();
            if !a.variables.is_empty() {
                global.push(a.variables.clone());
            }
            let mut scoped = Vec::new();
            for VariableInstruction {
                variables,
                scope_id,
            } in &a.variable_instructions
            {
                match scope_id.as_deref() {
                    None | Some("") => global.push(variables.clone()),
                    Some(id) if id == process_id => global.push(variables.clone()),
                    Some(id) if def.element(id).is_none() => {
                        return Err(invalid(
                            instance_key,
                            &process_id,
                            format!(
                                "variable instructions with a scope element id that could not \
                                 be found: '{id}'"
                            ),
                        ))
                    }
                    Some(id) if id == a.element_id || chain.iter().any(|s| s == id) => {
                        scoped.push((id.to_string(), variables.clone()));
                    }
                    Some(_) => {
                        return Err(invalid(
                            instance_key,
                            &process_id,
                            "variable instructions with a scope element that doesn't belong to \
                             the activating element's flow scope. These variables should be set \
                             before or after the modification."
                                .to_string(),
                        ))
                    }
                }
            }
            activations.push(PlannedActivation {
                element_id: a.element_id.clone(),
                resolution,
                global,
                scoped,
            });
        }

        let mut by_key: Vec<Key> = Vec::new();
        for &eik in terminate_instructions {
            if !instance.active.contains_key(&eik) {
                return Err(EngineError::ElementInstanceNotFound {
                    instance_key,
                    element_instance_key: eik,
                });
            }
            if !by_key.contains(&eik) {
                by_key.push(eik);
            }
        }
        // Terminate-by-id: every active instance of the element, top-down —
        // an instance inside one already being terminated goes with it.
        let mut by_id: Vec<Key> = instance
            .active
            .iter()
            .filter(|(_, id)| terminate_element_ids.contains(id))
            .map(|(k, _)| *k)
            .collect();
        by_id.sort_unstable();
        let mut terminations = by_key;
        terminations.extend(by_id);
        let nested_in_other = |eik: Key, set: &[Key]| {
            self.ancestor_element_instances(instance_key, eik)
                .iter()
                .any(|a| set.contains(a))
        };
        let all = terminations.clone();
        terminations.retain(|eik| !nested_in_other(*eik, &all));
        terminations.dedup();
        let mut seen = Vec::new();
        terminations.retain(|k| {
            let fresh = !seen.contains(k);
            seen.push(*k);
            fresh
        });

        // An activation may not land in a flow-scope instance this command
        // terminates (Zeebe `ERROR_MESSAGE_ACTIVATION_FLOW_SCOPE_TERMINATED`).
        for planned in &activations {
            let existing = planned.resolution.existing;
            if existing == 0 {
                continue;
            }
            if terminations.iter().any(|&t| {
                t == existing || self.is_ancestor_element_instance(instance_key, t, existing)
            }) {
                return Err(invalid(
                    instance_key,
                    &process_id,
                    format!(
                        "an activate instruction for element '{}' whose required flow scope \
                         instance is also being terminated: '{existing}'",
                        planned.element_id
                    ),
                ));
            }
        }

        Ok(ModificationPlan {
            activations,
            terminations,
        })
    }

    /// Applies a validated [`ModificationPlan`]: emits the scope creations,
    /// variable writes, terminations and cascade, and queues the activations.
    pub(super) fn apply_modification(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        instance_key: Key,
        plan: ModificationPlan,
    ) {
        // Scope instances that must survive the termination cascade: the ones
        // activations land in (root = 0 included).
        let mut required: Vec<Key> = Vec::new();
        // Flow scopes created by this command, shared across its instructions:
        // (parent scope instance, scope element id) -> created instance.
        let mut created: Vec<((Key, ElementId), Key)> = Vec::new();
        let any_activation = !plan.activations.is_empty();

        for planned in plan.activations {
            let mut scope = planned.resolution.existing;
            let mut lineage: Vec<(ElementId, Key)> = Vec::new();
            // Scopes this instruction opens: their boundary events are armed
            // only after the instruction's variables are written, so boundary
            // expressions (correlation keys, timers) see them.
            let mut fresh: Vec<(Key, ElementId, Key)> = Vec::new();
            for scope_id in &planned.resolution.to_create {
                let reuse = created
                    .iter()
                    .find(|((p, id), _)| *p == scope && id == scope_id)
                    .map(|(_, k)| *k);
                let key = match reuse {
                    Some(k) => k,
                    None => {
                        let k = self.open_flow_scope(log, instance_key, scope_id, scope);
                        fresh.push((k, scope_id.clone(), scope));
                        created.push(((scope, scope_id.clone()), k));
                        k
                    }
                };
                lineage.push((scope_id.clone(), key));
                scope = key;
            }
            required.push(scope);

            for variables in planned.global {
                for event in self.propagated_updates(instance_key, instance_key, variables, true) {
                    self.emit(log, event);
                }
            }
            let mut on_element = Vec::new();
            for (scope_id, variables) in planned.scoped {
                if scope_id == planned.element_id {
                    on_element.push(variables);
                    continue;
                }
                let target = lineage
                    .iter()
                    .find(|(id, _)| *id == scope_id)
                    .map(|(_, k)| *k)
                    .or_else(|| self.existing_scope_instance(instance_key, scope, &scope_id));
                if let Some(target) = target {
                    for event in self.propagated_updates(instance_key, target, variables, true) {
                        self.emit(log, event);
                    }
                }
            }

            for (key, scope_id, parent) in fresh {
                for event in self.arm_boundary_events(instance_key, key, parent, &scope_id) {
                    self.emit(log, event);
                }
            }

            if on_element.is_empty() {
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: planned.element_id,
                    scope,
                    via: None,
                });
            } else {
                self.activate_with_local_variables(
                    log,
                    queue,
                    instance_key,
                    planned.element_id,
                    scope,
                    on_element,
                );
            }
        }

        let mut terminated_any = false;
        for eik in plan.terminations {
            let Some(element_id) = self.element_id_of_instance(instance_key, eik) else {
                continue;
            };
            let parent = self.scope_of(instance_key, eik);
            self.release_mi_child(log, instance_key, eik, parent);
            self.terminate_element_instance(log, instance_key, eik, &element_id);
            terminated_any = true;
            self.terminate_drained_flow_scopes(log, instance_key, parent, &required);
        }

        // The terminations drained the last token and nothing replaces it: the
        // instance is terminated (Zeebe modify semantics), not auto-completed.
        let drained = self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.active.is_empty())
            .unwrap_or(true);
        if drained && !any_activation && terminated_any {
            self.emit(log, Event::ProcessInstanceTerminated { instance_key });
        }
    }

    /// Opens flow scope `scope_id` under `parent` without running its start
    /// event (Zeebe creates missing flow scopes this way): activates it and
    /// registers its variable scope. The caller arms its boundary events once
    /// the instruction's variables are written.
    fn open_flow_scope(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        scope_id: &str,
        parent: Key,
    ) -> Key {
        let key = self.mint_key();
        let element_id = scope_id.to_string();
        for event in [
            Event::ElementActivating {
                instance_key,
                element_instance_key: key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: key,
                element_id: element_id.clone(),
                scope: parent,
            },
            Event::VariableScopeCreated {
                instance_key,
                scope_key: key,
                parent_scope_key: parent,
            },
        ] {
            self.emit(log, event);
        }
        key
    }

    /// The instance of flow scope `scope_id` among `scope` and its ancestors.
    fn existing_scope_instance(
        &self,
        instance_key: Key,
        scope: Key,
        scope_id: &str,
    ) -> Option<Key> {
        if scope == 0 {
            return None;
        }
        let mut lineage = vec![scope];
        lineage.extend(self.ancestor_element_instances(instance_key, scope));
        lineage
            .into_iter()
            .find(|k| self.element_id_of_instance(instance_key, *k).as_deref() == Some(scope_id))
    }

    /// Zeebe order for element-scoped variable instructions: create the
    /// element instance and its variable scope, write `locals` into it, and only
    /// then run the activation (input mappings, listeners, job/user-task
    /// creation) — queued behind earlier instructions' activations so the
    /// command's instruction order is kept, and evaluated against the seeded
    /// scope so the element sees its own modification variables.
    fn activate_with_local_variables(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        instance_key: Key,
        element_id: ElementId,
        scope: Key,
        locals: Vec<HashMap<String, Value>>,
    ) {
        // A join target does not activate as one plain element instance (it is
        // guarded): run its regular activation and seed the instance it
        // activates.
        if self.join_kind(instance_key, &element_id).is_some() {
            self.activate_then_seed(log, queue, instance_key, element_id, scope, locals);
            return;
        }
        // A multi-instance target's element-scoped variables land on its body
        // (Zeebe): open and seed the body, then queue its fan-out, which
        // evaluates input mappings / input collection in the seeded body view.
        let is_multi_instance = self.multi_instance_of(instance_key, &element_id).is_some();
        let eik = self.mint_key();
        for event in [
            Event::ElementActivating {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
                scope,
            },
            Event::VariableScopeCreated {
                instance_key,
                scope_key: eik,
                parent_scope_key: scope,
            },
        ] {
            self.emit(log, event);
        }
        for variables in locals {
            for event in self.propagated_updates(instance_key, eik, variables, true) {
                self.emit(log, event);
            }
        }
        if is_multi_instance {
            queue.push_back(Step::RetryMiBodyActivation {
                instance_key,
                element_id,
                body_key: eik,
                scope,
            });
        } else {
            queue.push_back(Step::ResumeSeededActivation {
                instance_key,
                element_instance_key: eik,
                element_id,
            });
        }
    }

    /// Fallback for join targets: run the regular activation
    /// synchronously, then write `locals` into the instance it activated.
    fn activate_then_seed(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        instance_key: Key,
        element_id: ElementId,
        scope: Key,
        locals: Vec<HashMap<String, Value>>,
    ) {
        let step = Step::Activate {
            instance_key,
            element_id: element_id.clone(),
            scope,
            via: None,
        };
        let (events, followups) = self.process_step(step, queue);
        let eik = events.iter().find_map(|e| match e {
            Event::ElementActivated {
                element_instance_key,
                element_id: id,
                scope: s,
                ..
            } if *id == element_id && *s == scope => Some(*element_instance_key),
            _ => None,
        });
        for event in events {
            self.emit(log, event);
        }
        if let Some(eik) = eik {
            let registered = self
                .state
                .instances
                .get(&instance_key)
                .map(|i| i.scope_parents.contains_key(&eik))
                .unwrap_or(false);
            if !registered {
                self.emit(
                    log,
                    Event::VariableScopeCreated {
                        instance_key,
                        scope_key: eik,
                        parent_scope_key: scope,
                    },
                );
            }
            for variables in locals {
                for event in self.propagated_updates(instance_key, eik, variables, true) {
                    self.emit(log, event);
                }
            }
        }
        queue.extend(followups);
    }

    /// A terminated multi-instance child leaves its body's bookkeeping (active
    /// set, loop index) without contributing output, so a sibling's completion
    /// still completes the body and aggregates the collection.
    fn release_mi_child(&mut self, log: &mut Vec<Event>, instance_key: Key, child: Key, body: Key) {
        let index = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body))
            .and_then(|mi| {
                mi.active
                    .contains(&child)
                    .then(|| mi.child_indices.get(&child))
            })
            .flatten()
            .copied();
        if let Some(index) = index {
            self.emit(
                log,
                Event::MultiInstanceChildCompleted {
                    instance_key,
                    body_key: body,
                    child_key: child,
                    index,
                    output: None,
                },
            );
        }
    }

    /// Zeebe `terminateFlowScopes`: starting at `scope`, terminate each flow
    /// scope instance left without active children (and not `required` by an
    /// activation of this command), walking up to — but excluding — the root.
    fn terminate_drained_flow_scopes(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        mut scope: Key,
        required: &[Key],
    ) {
        while scope != 0 && !required.contains(&scope) {
            let Some(instance) = self.state.instances.get(&instance_key) else {
                return;
            };
            let has_children = instance
                .scopes
                .iter()
                .any(|(child, parent)| *parent == scope && instance.active.contains_key(child));
            if has_children || !instance.active.contains_key(&scope) {
                return;
            }
            let Some(element_id) = self.element_id_of_instance(instance_key, scope) else {
                return;
            };
            let parent = self.scope_of(instance_key, scope);
            self.release_mi_child(log, instance_key, scope, parent);
            self.terminate_element_instance(log, instance_key, scope, &element_id);
            scope = parent;
        }
    }
}

fn invalid(instance_key: Key, process_id: &str, reason: String) -> EngineError {
    EngineError::ModificationInstructionInvalid {
        instance_key,
        process_id: process_id.to_string(),
        reason,
    }
}
