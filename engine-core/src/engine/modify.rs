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
use crate::command::{ActivateElementInstruction, MoveInstruction, VariableInstruction};

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

/// A flow scope this command creates, identified exactly as
/// [`Engine::apply_modification`] reuses it: the existing instance the chain
/// hangs under plus the created element-id path down to the scope (application
/// keys reuse by `(parent instance, scope id)`, which is equivalent).
type CreatedScopePath = (Key, Vec<ElementId>);

/// One variable write a modification stages before a later activation's
/// boundary preflight runs — mirroring where [`Engine::apply_modification`]
/// emits the same write. Every scoped write is qualified by the exact scope
/// INSTANCE application lands it in, so a later activation only sees writes on
/// its own lineage (#1398 review).
enum StagedWrite {
    /// A root-scope write (a `global` batch).
    Root(HashMap<String, Value>),
    /// A write to an already-existing scope instance, resolved (to its
    /// variable scope) at staging time within the writing activation's
    /// lineage — exactly the instance application writes.
    ExistingScope(Key, HashMap<String, Value>),
    /// A write to a flow scope this command creates.
    CreatedScope(CreatedScopePath, HashMap<String, Value>),
}

/// The writes of the activations planned so far, in instruction order — the
/// state a later activation's preflight must see.
#[derive(Default)]
struct StagedWrites {
    writes: Vec<StagedWrite>,
}

impl StagedWrites {
    /// The staged writes to the created scope at `path`, in instruction order.
    fn created_scope_writes<'a>(
        &'a self,
        path: &'a CreatedScopePath,
    ) -> impl Iterator<Item = &'a HashMap<String, Value>> + 'a {
        self.writes.iter().filter_map(move |w| match w {
            StagedWrite::CreatedScope(p, vars) if p == path => Some(vars),
            _ => None,
        })
    }
}

impl Engine {
    /// Folds one planned activation's variable writes into the staged state,
    /// mirroring [`Engine::apply_modification`]: root batches first, then the
    /// scoped writes, each qualified by the scope instance application
    /// targets.
    fn stage_writes(
        &self,
        instance_key: Key,
        planned: &PlannedActivation,
        staged: &mut StagedWrites,
    ) {
        let existing = planned.resolution.existing;
        for variables in &planned.global {
            staged.writes.push(StagedWrite::Root(variables.clone()));
        }
        for (scope_id, variables) in &planned.scoped {
            if *scope_id == planned.element_id {
                // Written into the element's own fresh scope after arming:
                // never visible to a boundary preflight.
                continue;
            }
            if let Some(pos) = planned
                .resolution
                .to_create
                .iter()
                .position(|c| c == scope_id)
            {
                let path = (existing, planned.resolution.to_create[..=pos].to_vec());
                staged
                    .writes
                    .push(StagedWrite::CreatedScope(path, variables.clone()));
                continue;
            }
            if let Some(target) = self.existing_scope_target(instance_key, existing, scope_id) {
                staged
                    .writes
                    .push(StagedWrite::ExistingScope(target, variables.clone()));
            }
        }
    }

    /// The variable-scope instance an existing-scope write to `scope_id` lands
    /// in, resolved within the lineage of `existing` (the root instance key
    /// when it is the process scope).
    fn existing_scope_target(
        &self,
        instance_key: Key,
        existing: Key,
        scope_id: &str,
    ) -> Option<Key> {
        let instance = self.state.instances.get(&instance_key)?;
        let target = self.existing_scope_instance(instance_key, existing, scope_id)?;
        Some(self.variable_scope_of(instance, target))
    }
}

impl Engine {
    /// Desugars the move instructions of a `ModifyInstance` (Camunda 8.9
    /// `moveInstructions`) into the activate/terminate instructions they
    /// abbreviate, mirroring Zeebe's
    /// `ProcessInstanceModificationModifyProcessor` (`mapIdInstructions` +
    /// `mapMoveInstructionsByInstanceKey`): each moved source element instance
    /// is terminated and one fresh instance of the target element is activated
    /// per moved instance, with the move's variable instructions riding the
    /// activation.
    ///
    /// The desugared instructions are APPENDED to the caller's explicit
    /// instructions (Zeebe builds the final lists the same way), so the
    /// existing all-or-nothing validation in [`Self::plan_modification`]
    /// covers moves unchanged.
    ///
    /// The ancestor scope of each spawned activation is resolved per moved
    /// source instance (Zeebe `determineAncestorScopeKey`):
    ///
    /// * `SourceParent` — the source instance's direct flow-scope instance;
    /// * `Direct(k)` — the explicit ancestor key (`None` = Zeebe's `-1`, the
    ///   default find-or-create resolution);
    /// * `Inferred` — walk the source instance's ancestry for an instance
    ///   whose element is one of the target's flow scopes and use it; none →
    ///   the default resolution (`findProperAncestorScopeKeyForTarget`).
    ///
    /// Rejections (all [`EngineError::ModificationInstructionInvalid`], the
    /// 400-class): an empty source id or target id, a duplicated source
    /// element id (Zeebe `ERROR_MESSAGE_MOVE_*`), a by-key source instance
    /// that does not exist or belongs to another process instance, and a
    /// by-id source whose element id is unknown to the definition.
    ///
    /// For a multi-instance source only the BODY instances move
    /// (`shouldMoveAllMultiInstanceElementsByIds`): each body is terminated
    /// (its children go with it) and activates one fresh target instance.
    pub(super) fn desugar_move_instructions(
        &self,
        instance_key: Key,
        move_instructions: &[MoveInstruction],
        activate_instructions: &mut Vec<ActivateElementInstruction>,
        terminate_instructions: &mut Vec<Key>,
    ) -> Result<(), EngineError> {
        use crate::command::{AncestorScopeInstruction as Anc, MoveSource};

        let instance = self
            .state
            .instances
            .get(&instance_key)
            .ok_or(EngineError::InstanceNotFound { instance_key })?;
        let process_id = instance.process_id.clone();
        let def = self
            .process_of_instance(instance_key)
            .ok_or(EngineError::InstanceNotFound { instance_key })?;
        let invalid_move = |reason: String| {
            Err(EngineError::ModificationInstructionInvalid {
                instance_key,
                process_id: process_id.clone(),
                reason,
            })
        };

        // Zeebe `validateHasMoveInstructions` / `validateMoveBatchInstructions`:
        // every instruction needs a non-empty source and target.
        for m in move_instructions {
            let source_missing = match &m.source {
                MoveSource::ById(id) => id.is_empty(),
                // A by-key source is defined by its key; `0` is no key.
                MoveSource::ByKey(k) => *k == 0,
            };
            if source_missing || m.target_element_id.is_empty() {
                return invalid_move(format!(
                    "Expected to modify instance of process '{process_id}' but it contains one \
                     or more move instructions with either or both the source or target element \
                     id missing"
                ));
            }
        }

        // Zeebe `validateNoDuplicatedMoveInstructions`: no two by-id moves may
        // share a source element id.
        let mut seen_source_ids: Vec<&str> = Vec::new();
        for m in move_instructions {
            if let MoveSource::ById(id) = &m.source {
                if seen_source_ids.contains(&id.as_str()) {
                    return invalid_move(format!(
                        "Expected to modify instance of process '{process_id}' but it contains \
                         multiple move instructions with identical source element ids: '{id}'"
                    ));
                }
                seen_source_ids.push(id);
            }
        }

        // The ancestor scope for the activation one moved source instance
        // spawns (Zeebe `determineAncestorScopeKey`). `0` — the process root,
        // Zeebe's `-1` — means the default find-or-create resolution and is
        // carried as `None` (the activation instruction's "no ancestor
        // selection").
        let ancestor_for = |m: &MoveInstruction, source_parent: Key| -> Option<Key> {
            match &m.ancestor_scope {
                // A root-level source's parent is the process root (`0`).
                // That is an EXPLICIT selection, not "no selection": carry it
                // as the instance key, which the flow-scope resolver treats as
                // the root (it then creates the target's whole scope chain
                // beneath it rather than reusing/inferring an active scope).
                Anc::SourceParent => Some(if source_parent == 0 {
                    instance_key
                } else {
                    source_parent
                }),
                Anc::Direct(k) => Some(*k).filter(|k| *k != 0),
                Anc::Inferred => self.infer_ancestor_scope(
                    def,
                    instance_key,
                    source_parent,
                    &m.target_element_id,
                ),
            }
        };
        let mut spawn_activation = |m: &MoveInstruction, ancestor: Option<Key>| {
            activate_instructions.push(ActivateElementInstruction {
                element_id: m.target_element_id.clone(),
                variables: HashMap::new(),
                ancestor_element_instance_key: ancestor,
                variable_instructions: m.variable_instructions.clone(),
            });
        };

        // By-key moves first (Zeebe `mapMoveInstructionsByInstanceKey`): the
        // source instance must exist and belong to this process instance.
        for m in move_instructions {
            let MoveSource::ByKey(source_key) = &m.source else {
                continue;
            };
            if !instance.active.contains_key(source_key) {
                return invalid_move(format!(
                    "Expected to modify instance of process '{process_id}' but it contains one \
                     or more move instructions with a source element instance that could not be \
                     found: '{source_key}'"
                ));
            }
            let source_parent = self.scope_of(instance_key, *source_key);
            spawn_activation(m, ancestor_for(m, source_parent));
            terminate_instructions.push(*source_key);
        }

        // By-id moves (Zeebe `mapIdInstructions`): every active instance of the
        // source element id moves. A multi-instance source moves only its BODY
        // instances (the children go with the terminated body); a body is not
        // itself moved by an id instruction naming its element when enumerated
        // as a child of another body — the structural body check keeps exactly
        // the bodies.
        for m in move_instructions {
            let MoveSource::ById(source_id) = &m.source else {
                continue;
            };
            if def.element(source_id).is_none() {
                return invalid_move(format!(
                    "Expected to modify instance of process '{process_id}' but it contains one \
                     or more move instructions with a source element id that could not be found \
                     in the process: '{source_id}'"
                ));
            }
            let mut sources: Vec<Key> = instance
                .active
                .iter()
                .filter(|(_, id)| id.as_str() == source_id)
                .map(|(k, _)| *k)
                .collect();
            sources.sort_unstable();
            if self.multi_instance_of(instance_key, source_id).is_some() {
                sources.retain(|k| self.is_multi_instance_body_structural(instance_key, *k));
            }
            for source_key in sources {
                let source_parent = self.scope_of(instance_key, source_key);
                spawn_activation(m, ancestor_for(m, source_parent));
                terminate_instructions.push(source_key);
            }
        }
        Ok(())
    }

    /// The inferred ancestor scope instance for a move target (Zeebe
    /// `findProperAncestorScopeKeyForTarget`): walk the source instance's
    /// ancestry — starting at its direct flow scope — for an instance whose
    /// element is one of the target's flow scopes, and use it. `0` (the
    /// process root) matches a root-level target. No matching ancestor →
    /// `None` (Zeebe's `-1`): the activation's default find-or-create
    /// resolution applies.
    fn infer_ancestor_scope(
        &self,
        def: &crate::model::ProcessDefinition,
        instance_key: Key,
        source_parent: Key,
        target_element_id: &str,
    ) -> Option<Key> {
        let chain = element_flow_scope_chain(def, target_element_id);
        // Root-level target: the default find-or-create resolution lands it at
        // the process root (Zeebe's `-1`).
        if chain.is_empty() {
            return None;
        }
        let mut current = Some(source_parent);
        while let Some(key) = current {
            if key == 0 {
                break;
            }
            let matches = self
                .element_id_of_instance(instance_key, key)
                .is_some_and(|id| chain.contains(&id));
            if matches {
                return Some(key);
            }
            current = self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.scopes.get(&key).copied());
        }
        None
    }

    /// Validates a `ModifyInstance` against the current state without emitting
    /// anything (Zeebe validation order: activate targets, ancestors, variable
    /// scopes, terminate targets, then flow-scope/termination conflicts).
    pub(super) fn plan_modification(
        &mut self,
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
            if let Some(body) =
                ancestor.filter(|k| self.is_multi_instance_body_structural(instance_key, *k))
            {
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

            // An ad-hoc container has its own lifecycle (`AdHocActivated`,
            // tool ownership, completion); opening one through the generic
            // flow-scope path would skip it and strand the container once the
            // inner flow completes. Fail closed until supported.
            if let Some(container) = resolution
                .to_create
                .iter()
                .find(|s| def.adhoc.iter().any(|ad| &ad.container_id == *s))
            {
                return Err(not_activatable(format!(
                    "The activation of elements inside an ad-hoc sub-process '{container}' \
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
        // A parked join's key (#1244) is never published in a record: it is
        // not an element instance, so it cannot be named for termination.
        for &eik in terminate_instructions {
            if !instance.active.contains_key(&eik) || instance.parked_joins.contains(&eik) {
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
            // A parked join (#1244) is included: terminating the join by id
            // discards its waiting tokens (`terminate_element_instance` writes
            // only a `ParallelJoinReset` for it).
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

        // A compensation handler instance a throw event is still waiting on may
        // not be terminated: the throw's wait is bound to that exact handler
        // instance (`pending_handler_keys`, #897), so terminating it leaves the
        // throw waiting on a key nothing can ever complete — a re-activation of
        // the same handler element gets a NEW key and retires nothing. There is
        // no binding-replacement event to re-point the wait at, so fail closed
        // (the modification is validated completely before anything is emitted,
        // so this rejection is atomic). The check sits on the final termination
        // list, so it covers by-key and by-id instructions alike — and a handler
        // inside a sub-process being terminated goes with it, which the
        // descendant check below covers.
        let bound_to_wait = |eik: Key| {
            instance
                .compensation_waits
                .values()
                .any(|w| w.pending_handler_keys.contains(&eik))
        };
        if let Some(&eik) = terminations.iter().find(|&&eik| {
            bound_to_wait(eik)
                || self
                    .scope_descendants(instance_key, eik)
                    .iter()
                    .any(|&d| bound_to_wait(d))
        }) {
            return Err(invalid(
                instance_key,
                &process_id,
                format!(
                    "a terminate instruction for element instance '{eik}', which is (or \
                     contains) a compensation handler a compensation throw event is still \
                     waiting on: terminating it would leave the throw waiting forever"
                ),
            ));
        }

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

        // Join bookkeeping (waiting instance + arrivals) is keyed by element id
        // per process instance, not per flow-scope instance. Activating a join
        // whose waiting instance lives in a DIFFERENT flow-scope instance would
        // take over that instance's arrivals and variables: fail closed.
        for planned in &activations {
            if self.join_kind(instance_key, &planned.element_id).is_none() {
                continue;
            }
            let Some(waiting) = self.join_eik(instance_key, &planned.element_id) else {
                continue;
            };
            if terminations.iter().any(|&t| {
                t == waiting || self.is_ancestor_element_instance(instance_key, t, waiting)
            }) {
                continue;
            }
            let waiting_scope = self.scope_of(instance_key, waiting);
            if !planned.resolution.to_create.is_empty()
                || waiting_scope != planned.resolution.existing
            {
                return Err(invalid(
                    instance_key,
                    &process_id,
                    format!(
                        "an activate instruction for join element '{}' in a flow scope other \
                         than that of its waiting instance '{waiting}' (flow scope \
                         '{waiting_scope}')",
                        planned.element_id
                    ),
                ));
            }
        }

        // The same id-keyed bookkeeping also forces every activation of one
        // join element in a single command to land in ONE flow-scope instance:
        // with no join waiting, two activations under different scope instances
        // would still share the join's arrivals and could combine tokens from
        // unrelated scopes into a firing. Zeebe activates both (its joins are
        // per-scope); this engine keys joins per process instance, so it fails
        // closed instead. Activations sharing the to-be-created scope instance
        // (same existing parent, same innermost created scope — the applier
        // reuses one created instance for both) land together and are fine.
        let planned_joins: Vec<&PlannedActivation> = activations
            .iter()
            .filter(|p| self.join_kind(instance_key, &p.element_id).is_some())
            .collect();
        for (i, a) in planned_joins.iter().enumerate() {
            for b in &planned_joins[i + 1..] {
                // Distinct joins keep separately keyed bookkeeping: no collision.
                if a.element_id != b.element_id {
                    continue;
                }
                let (ra, rb) = (&a.resolution, &b.resolution);
                // Guaranteed same landing scope only when both create the same
                // innermost scope under the same parent (the applier shares
                // that one created instance), or both reuse the same existing
                // instance and create nothing. Everything else can land the
                // two activations in different scope instances.
                let shared_created = ra.existing == rb.existing
                    && !ra.to_create.is_empty()
                    && ra.to_create.last() == rb.to_create.last();
                let same_existing = ra.existing == rb.existing
                    && ra.to_create.is_empty()
                    && rb.to_create.is_empty();
                if !(shared_created || same_existing) {
                    return Err(invalid(
                        instance_key,
                        &process_id,
                        format!(
                            "multiple activate instructions for join element '{}' in different \
                             flow scopes of one modification: join arrivals are kept per \
                             process instance, so the activations would share one join",
                            a.element_id
                        ),
                    ));
                }
            }
        }

        // Scopes an activation creates arm their boundary events (Zeebe
        // `ElementActivationBehavior.activateFlowScope` → `subscribeToEvents`);
        // a failing boundary expression rejects the whole command —
        // "Expected to subscribe to catch event(s) of '<scope>' but <failure>"
        // — before anything is emitted (#1398).
        //
        // Each activation is checked against the variable view the command
        // reaches when that activation APPLIES: the current state plus every
        // earlier instruction's writes (application emits instructions in
        // order, so an earlier instruction's variables are live when a later
        // instruction's scope boundaries arm) plus this instruction's own
        // staged writes. Checking against the original state alone would
        // reject a valid command (a boundary key supplied by an earlier
        // instruction) or accept an invalid one whose failure then surfaces as
        // an incident AFTER earlier instructions already emitted — breaking
        // all-or-nothing (#1398 review).
        let mut staged = StagedWrites::default();
        // Scopes an earlier instruction already creates: application reuses
        // them (keyed by parent instance + scope id) and arms their boundaries
        // only once, on creation — so only the FIRST creation is checked; a
        // later activation sharing the scope keeps the captured subscription.
        let mut created: std::collections::HashSet<CreatedScopePath> =
            std::collections::HashSet::new();
        for planned in &activations {
            if planned.resolution.to_create.is_empty() {
                self.stage_writes(instance_key, planned, &mut staged);
                continue;
            }
            let mut host_vars = self.staged_parent_view(instance_key, planned, &staged);
            for (depth, scope_id) in planned.resolution.to_create.iter().enumerate() {
                // Checked against the PARENT-scope view, exactly as application
                // arms it (`arm_boundary_events` evaluates in the new scope's
                // flow scope): the scope's own locals are layered on only
                // AFTER its boundaries are checked, so they are visible to
                // nested scopes but never to the scope's own boundaries.
                let path = (
                    planned.resolution.existing,
                    planned.resolution.to_create[..=depth].to_vec(),
                );
                let checked = if created.insert(path.clone()) {
                    self.check_boundary_arming(instance_key, scope_id, &host_vars)
                } else {
                    Ok(())
                };
                // The scope's locals, layered in lexical order while
                // descending: earlier instructions' writes to this same
                // created scope instance (shared with this activation), then
                // this instruction's own.
                for variables in staged.created_scope_writes(&path) {
                    host_vars.extend(variables.clone());
                }
                for (id, variables) in &planned.scoped {
                    if id == scope_id {
                        host_vars.extend(variables.clone());
                    }
                }
                if let Err(failure) = checked {
                    return Err(invalid(
                        instance_key,
                        &process_id,
                        format!(
                            "Expected to subscribe to catch event(s) of '{scope_id}' but {}",
                            failure.message
                        ),
                    ));
                }
            }
            self.stage_writes(instance_key, planned, &mut staged);
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
                // Validated by `plan_modification` (`check_boundary_arming`)
                // against the same parent-scope view, so arming should not
                // fail here. Never drop a failure silently: should it happen
                // anyway, surface it as an incident on the created scope.
                match self.arm_boundary_events(instance_key, key, parent, &scope_id) {
                    Ok(armed) => {
                        for event in armed {
                            self.emit(log, event);
                        }
                    }
                    Err(failure) => {
                        let event = self.expression_incident(
                            instance_key,
                            key,
                            scope_id.clone(),
                            failure,
                            None,
                        );
                        self.emit(log, event);
                    }
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
                scope: parent,
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
    /// The variable view the first scope created by `planned` arms its
    /// boundaries against, INCLUDING every write this same command stages
    /// before that arming (#1398 review): `staged` holds every EARLIER
    /// instruction's writes (application emits them in instruction order, so
    /// they are live when this activation's scopes arm), and `planned`'s own
    /// root writes (`global`) and element-scoped writes to existing ancestor
    /// scopes land before arming too. Each staged write lands in its own
    /// scope's locals and the chain is then merged root-first, so lexical
    /// shadowing is preserved — a root write never overrides a nearer local of
    /// the same name.
    fn staged_parent_view(
        &self,
        instance_key: Key,
        planned: &PlannedActivation,
        staged: &StagedWrites,
    ) -> HashMap<String, Value> {
        let existing = planned.resolution.existing;
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return HashMap::new();
        };
        let leaf = self.variable_scope_of(instance, existing);
        let mut chain: Vec<Key> = Vec::new();
        let mut cur = leaf;
        while cur != instance.key && cur != 0 {
            chain.push(cur);
            match instance.scope_parents.get(&cur) {
                Some(&parent) if parent != cur => cur = parent,
                _ => break,
            }
        }
        let mut root = (*instance.variables).clone();
        let mut locals: HashMap<Key, HashMap<String, Value>> = chain
            .iter()
            .map(|k| {
                (
                    *k,
                    instance.scope_variables.get(k).cloned().unwrap_or_default(),
                )
            })
            .collect();
        // Earlier instructions' writes, in instruction order, then this
        // instruction's own pre-arming writes. A scoped write is visible only
        // when its resolved scope instance is on this activation's chain (or
        // is the root); a write to a sibling instance is not. Created-scope
        // writes are layered per scope while descending the created chain
        // (in `plan_modification`), never merged into the root here.
        let instance_root = instance.key;
        let mut land =
            |target: Key, variables: &HashMap<String, Value>, root: &mut HashMap<String, Value>| {
                if target == instance_root || target == 0 {
                    root.extend(variables.clone());
                } else if let Some(local) = locals.get_mut(&target) {
                    local.extend(variables.clone());
                }
            };
        let mut own = StagedWrites::default();
        self.stage_writes(instance_key, planned, &mut own);
        for staged_write in staged.writes.iter().chain(own.writes.iter()) {
            match staged_write {
                StagedWrite::Root(variables) => root.extend(variables.clone()),
                StagedWrite::ExistingScope(target, variables) => {
                    land(*target, variables, &mut root)
                }
                StagedWrite::CreatedScope(..) => {}
            }
        }
        for k in chain.iter().rev() {
            if let Some(local) = locals.remove(k) {
                root.extend(local);
            }
        }
        (*self.overlay_cluster_variables(std::sync::Arc::new(root))).clone()
    }

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
        // guarded): queue its seeded guarded activation (after this command's
        // terminations, in instruction order).
        if self.join_kind(instance_key, &element_id).is_some() {
            queue.push_back(Step::ActivateSeededJoin {
                instance_key,
                element_id,
                scope,
                locals,
            });
            return;
        }
        // A multi-instance target's element-scoped variables land on its body
        // (Zeebe): open and seed the body, then queue its fan-out, which
        // evaluates input mappings / input collection in the seeded body view.
        let is_multi_instance = self.multi_instance_of(instance_key, &element_id).is_some();
        let eik = self.mint_key();
        self.emit(
            log,
            Event::ElementActivating {
                instance_key,
                element_instance_key: eik,
                element_id: element_id.clone(),
                scope,
            },
        );
        // A multi-instance body becomes ACTIVATED in its fan-out
        // (`run_mi_body_activation`), once its activation-time expressions held.
        if !is_multi_instance {
            self.emit(
                log,
                Event::ElementActivated {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                    scope,
                },
            );
        }
        self.emit(
            log,
            Event::VariableScopeCreated {
                instance_key,
                scope_key: eik,
                parent_scope_key: scope,
            },
        );
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

    /// [`Step::ActivateSeededJoin`]: seed the join instance a modification's
    /// activation takes over, then run the guarded activation. The waiting
    /// instance (or, if none waits, a freshly opened one) gets its variable
    /// scope and `locals` BEFORE the guard evaluates, so an accepted join
    /// completes and routes with its locals in place, and a reused waiting
    /// instance (which emits no `ElementActivated`) keeps them too.
    pub(super) fn activate_seeded_join(
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
        if self.step_targets_dead_scope(&step) {
            return;
        }
        let eik = match self.join_eik(instance_key, &element_id) {
            Some(eik) => eik,
            None => {
                let mut events = Vec::new();
                let eik = self.open_join(instance_key, &element_id, scope, &mut events);
                for event in events {
                    self.emit(log, event);
                }
                eik
            }
        };
        let registered = self
            .state
            .instances
            .get(&instance_key)
            .is_some_and(|i| i.scope_parents.contains_key(&eik));
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
        let (events, followups) = self.process_step(step, queue);
        for event in events {
            self.emit(log, event);
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
