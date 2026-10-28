//! Flow-scope tree helpers (#1374): the single canonical implementation of
//! "which scope does this element / element instance live in" shared by the
//! engine, process-instance modification and migration.
//!
//! Two trees are involved:
//!
//! * the **model** flow-scope chain — an element's enclosing sub-processes, read
//!   off [`crate::model::Element::parent`] ([`element_flow_scope_chain`]);
//! * the **runtime** element-instance tree — `ProcessInstance::scopes` maps each
//!   element instance living in a non-root scope to its enclosing scope element
//!   instance; `0` is the process-level (root) scope.
//!
//! Do not hand-roll a walk over either tree elsewhere; extend this module.

use super::*;

/// The model flow-scope chain of `element_id`: the ids of its enclosing
/// sub-processes, **innermost first** (empty for a process-level element, or an
/// unknown id). Bounded by the element count, so a malformed cyclic `parent`
/// chain cannot loop forever.
pub(crate) fn element_flow_scope_chain(
    def: &crate::model::ProcessDefinition,
    element_id: &str,
) -> Vec<ElementId> {
    let mut chain = Vec::new();
    let mut current = def.element(element_id).and_then(|e| e.parent.clone());
    while let Some(parent) = current {
        if chain.len() > def.elements.len() || chain.contains(&parent) {
            break;
        }
        current = def.element(&parent).and_then(|e| e.parent.clone());
        chain.push(parent);
    }
    chain
}

/// Where an element would be activated: an existing flow-scope instance to
/// reuse plus the flow scopes that must be created beneath it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FlowScopeResolution {
    /// The existing element instance to activate under (`0` = process root).
    pub existing: Key,
    /// Flow-scope element ids to create beneath `existing`, **outermost
    /// first**, before the element itself is activated. Empty when the element's
    /// direct flow scope already exists.
    pub to_create: Vec<ElementId>,
}

impl Engine {
    /// The element instance of the scope that encloses `element_instance_key`
    /// (an embedded sub-process, MI body, ad-hoc inner instance, …), or `0` if
    /// it lives in the process-level scope.
    pub(crate) fn scope_of(&self, instance_key: Key, element_instance_key: Key) -> Key {
        self.state
            .instances
            .get(&instance_key)
            .and_then(|i| i.scopes.get(&element_instance_key).copied())
            .unwrap_or(0)
    }

    /// Every ancestor scope element instance of `element_instance_key`,
    /// **innermost first**, excluding the process root (`0`). Cycle-safe.
    pub(crate) fn ancestor_element_instances(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Vec<Key> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        let mut ancestors = Vec::new();
        let mut current = element_instance_key;
        while let Some(&parent) = instance.scopes.get(&current) {
            if parent == 0 || ancestors.len() > instance.scopes.len() || ancestors.contains(&parent)
            {
                break;
            }
            ancestors.push(parent);
            current = parent;
        }
        ancestors
    }

    /// Whether `ancestor` is an ancestor scope instance of `element_instance_key`.
    /// The process root (`0`) is an ancestor of every element instance of an
    /// existing process instance; an instance is not its own ancestor.
    pub(crate) fn is_ancestor_element_instance(
        &self,
        instance_key: Key,
        ancestor: Key,
        element_instance_key: Key,
    ) -> bool {
        if !self.state.instances.contains_key(&instance_key) {
            return false;
        }
        ancestor == 0
            || self
                .ancestor_element_instances(instance_key, element_instance_key)
                .contains(&ancestor)
    }

    /// Every element instance nested (transitively) inside `scope_eik`, sorted.
    pub(crate) fn scope_descendants(&self, instance_key: Key, scope_eik: Key) -> Vec<Key> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        let mut stack = vec![scope_eik];
        while let Some(parent) = stack.pop() {
            for (child, p) in &instance.scopes {
                if *p == parent {
                    result.push(*child);
                    stack.push(*child);
                }
            }
        }
        result.sort_unstable();
        result
    }

    /// The active element instances of flow-scope element `flow_scope_id`
    /// (sorted), or `[0]` (the process root) for `None`.
    pub(crate) fn flow_scope_instances(
        &self,
        instance_key: Key,
        flow_scope_id: Option<&str>,
    ) -> Vec<Key> {
        let Some(id) = flow_scope_id else {
            return vec![0];
        };
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        let mut keys: Vec<Key> = instance
            .active
            .iter()
            // A multi-instance body shares its element id with its children
            // but is not a flow-scope instance of the inner elements: only the
            // children are (Zeebe models the body as a separate element).
            .filter(|(k, e)| e.as_str() == id && !instance.multi_instances.contains_key(k))
            .map(|(k, _)| *k)
            .collect();
        keys.sort_unstable();
        keys
    }

    /// Find-or-create resolution of the flow scope `element_id` is activated
    /// in (Zeebe `ProcessInstanceModificationModifyProcessor` semantics).
    ///
    /// * `ancestor = None`: walk the element's model flow-scope chain innermost
    ///   first; the first scope with exactly one active instance is reused and
    ///   the scopes inside it are to be created. More than one active instance
    ///   is [`EngineError::AmbiguousFlowScope`]. No active instance anywhere =
    ///   activate under the root, creating the whole chain.
    /// * `ancestor = Some(k)`: `k` (or the process instance key itself, meaning
    ///   the root) must be an active element instance
    ///   ([`EngineError::AncestorScopeNotFound`]) whose element is one of the
    ///   element's flow scopes ([`EngineError::AncestorNotAnAncestor`]).
    ///
    /// This only *resolves*; activating the `to_create` scopes is the caller's
    /// job.
    pub(crate) fn resolve_flow_scope(
        &self,
        instance_key: Key,
        element_id: &str,
        ancestor: Option<Key>,
    ) -> Result<FlowScopeResolution, EngineError> {
        let instance = self
            .state
            .instances
            .get(&instance_key)
            .ok_or(EngineError::InstanceNotFound { instance_key })?;
        let process_id = instance.process_id.clone();
        let def = self
            .process_of_instance(instance_key)
            .ok_or(EngineError::InstanceNotFound { instance_key })?;
        if def.element(element_id).is_none() {
            return Err(EngineError::ElementNotFound {
                instance_key,
                element_id: element_id.to_string(),
            });
        }
        let chain = element_flow_scope_chain(def, element_id);
        let plan = |depth: usize, existing: Key| FlowScopeResolution {
            existing,
            to_create: chain[..depth].iter().rev().cloned().collect(),
        };

        if let Some(ancestor_key) = ancestor {
            if ancestor_key == instance_key {
                return Ok(plan(chain.len(), 0));
            }
            let ancestor_element = instance.active.get(&ancestor_key).ok_or_else(|| {
                EngineError::AncestorScopeNotFound {
                    instance_key,
                    process_id: process_id.clone(),
                    ancestor_element_instance_key: ancestor_key,
                }
            })?;
            let depth = chain
                .iter()
                .position(|s| s == ancestor_element)
                .ok_or_else(|| EngineError::AncestorNotAnAncestor {
                    instance_key,
                    process_id: process_id.clone(),
                    element_id: element_id.to_string(),
                    ancestor_element_instance_key: ancestor_key,
                })?;
            return Ok(plan(depth, ancestor_key));
        }

        for (depth, scope_id) in chain.iter().enumerate() {
            match self
                .flow_scope_instances(instance_key, Some(scope_id))
                .as_slice()
            {
                [] => continue,
                [only] => return Ok(plan(depth, *only)),
                _ => {
                    return Err(EngineError::AmbiguousFlowScope {
                        instance_key,
                        process_id,
                        element_id: element_id.to_string(),
                    })
                }
            }
        }
        Ok(plan(chain.len(), 0))
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test module; the production deny-unwrap gate (#1357) applies to library code"
)]
mod tests {
    use super::*;
    use crate::model::ProcessBuilder;

    /// `outer` sub-process containing `inner` sub-process containing task `t`;
    /// plus a root-level task `r`.
    fn nested() -> crate::model::ProcessDefinition {
        ProcessBuilder::new("nested")
            .start_event("s")
            .sub_process("outer", "os")
            .start_event("os")
            .contained_in("os", "outer")
            .sub_process("inner", "is")
            .contained_in("inner", "outer")
            .start_event("is")
            .contained_in("is", "inner")
            .service_task("t", "work")
            .contained_in("t", "inner")
            .end_event("ie")
            .contained_in("ie", "inner")
            .end_event("oe")
            .contained_in("oe", "outer")
            .service_task("r", "root-work")
            .end_event("e")
            .connect("s", "outer")
            .connect("os", "inner")
            .connect("is", "t")
            .connect("t", "ie")
            .connect("inner", "oe")
            .connect("outer", "e")
            .build()
            .expect("nested process")
    }

    fn started() -> (Engine, Key) {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(nested()))
            .unwrap();
        let inst = engine
            .apply_command(Command::create_instance("nested"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        (engine, inst)
    }

    fn eik(engine: &Engine, inst: Key, id: &str) -> Key {
        let keys = engine.flow_scope_instances(inst, Some(id));
        assert_eq!(keys.len(), 1, "exactly one active {id}");
        keys[0]
    }

    #[test]
    fn element_flow_scope_chain_is_innermost_first() {
        let def = nested();
        assert_eq!(element_flow_scope_chain(&def, "t"), vec!["inner", "outer"]);
        assert_eq!(element_flow_scope_chain(&def, "inner"), vec!["outer"]);
        assert!(element_flow_scope_chain(&def, "r").is_empty());
        assert!(element_flow_scope_chain(&def, "missing").is_empty());
    }

    #[test]
    fn scope_of_and_ancestors_walk_the_instance_tree() {
        let (engine, inst) = started();
        let (outer, inner, t) = (
            eik(&engine, inst, "outer"),
            eik(&engine, inst, "inner"),
            eik(&engine, inst, "t"),
        );
        assert_eq!(engine.scope_of(inst, t), inner);
        assert_eq!(engine.scope_of(inst, outer), 0);
        assert_eq!(
            engine.ancestor_element_instances(inst, t),
            vec![inner, outer]
        );
        assert!(engine.ancestor_element_instances(inst, outer).is_empty());
        assert!(engine.ancestor_element_instances(999, t).is_empty());
        assert_eq!(engine.scope_descendants(inst, outer).len(), 2);
    }

    #[test]
    fn is_ancestor_element_instance_checks_the_chain() {
        let (engine, inst) = started();
        let (outer, inner, t) = (
            eik(&engine, inst, "outer"),
            eik(&engine, inst, "inner"),
            eik(&engine, inst, "t"),
        );
        assert!(engine.is_ancestor_element_instance(inst, outer, t));
        assert!(engine.is_ancestor_element_instance(inst, inner, t));
        assert!(engine.is_ancestor_element_instance(inst, 0, t));
        assert!(!engine.is_ancestor_element_instance(inst, t, t));
        assert!(!engine.is_ancestor_element_instance(inst, t, outer));
        assert!(!engine.is_ancestor_element_instance(999, 0, t));
    }

    #[test]
    fn flow_scope_instances_lists_active_scope_instances() {
        let (engine, inst) = started();
        assert_eq!(engine.flow_scope_instances(inst, None), vec![0]);
        assert_eq!(engine.flow_scope_instances(inst, Some("outer")).len(), 1);
        assert!(engine.flow_scope_instances(inst, Some("r")).is_empty());
    }

    #[test]
    fn resolve_flow_scope_reuses_existing_or_plans_creation() {
        let (engine, inst) = started();
        let (outer, inner) = (eik(&engine, inst, "outer"), eik(&engine, inst, "inner"));
        // Existing direct flow scope is reused.
        assert_eq!(
            engine.resolve_flow_scope(inst, "t", None).unwrap(),
            FlowScopeResolution {
                existing: inner,
                to_create: vec![]
            }
        );
        // Root-level element activates under the root.
        assert_eq!(
            engine.resolve_flow_scope(inst, "r", None).unwrap(),
            FlowScopeResolution {
                existing: 0,
                to_create: vec![]
            }
        );
        // Explicit ancestor `outer` → `inner` must be created beneath it.
        assert_eq!(
            engine.resolve_flow_scope(inst, "t", Some(outer)).unwrap(),
            FlowScopeResolution {
                existing: outer,
                to_create: vec!["inner".to_string()]
            }
        );
        // The process instance key as ancestor = the root; whole chain created.
        assert_eq!(
            engine.resolve_flow_scope(inst, "t", Some(inst)).unwrap(),
            FlowScopeResolution {
                existing: 0,
                to_create: vec!["outer".to_string(), "inner".to_string()]
            }
        );
    }

    #[test]
    fn resolve_flow_scope_creates_the_chain_when_no_scope_is_active() {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(nested()))
            .unwrap();
        let inst = engine
            .apply_command(Command::create_instance("nested"))
            .unwrap()
            .iter()
            .find_map(|e| e.instance_key())
            .unwrap();
        let outer = eik(&engine, inst, "outer");
        engine
            .apply_command(Command::modify_instance(
                inst,
                vec![crate::command::ActivateElementInstruction::new("r")],
                vec![outer],
            ))
            .unwrap();
        assert_eq!(
            engine.resolve_flow_scope(inst, "t", None).unwrap(),
            FlowScopeResolution {
                existing: 0,
                to_create: vec!["outer".to_string(), "inner".to_string()]
            }
        );
    }

    #[test]
    fn resolve_flow_scope_rejections() {
        let (mut engine, inst) = started();
        let t = eik(&engine, inst, "t");
        assert!(matches!(
            engine.resolve_flow_scope(inst, "t", Some(t)),
            Err(EngineError::AncestorNotAnAncestor { ancestor_element_instance_key, .. })
                if ancestor_element_instance_key == t
        ));
        assert!(matches!(
            engine.resolve_flow_scope(inst, "t", Some(424_242)),
            Err(EngineError::AncestorScopeNotFound { .. })
        ));
        assert!(matches!(
            engine.resolve_flow_scope(inst, "nope", None),
            Err(EngineError::ElementNotFound { .. })
        ));
        assert!(matches!(
            engine.resolve_flow_scope(999_999, "t", None),
            Err(EngineError::InstanceNotFound { .. })
        ));
        // A second `outer` (and with it a second `inner`) makes the flow scope
        // of `t` ambiguous.
        engine
            .apply_command(Command::modify_instance(
                inst,
                vec![crate::command::ActivateElementInstruction::new("outer")],
                vec![],
            ))
            .unwrap();
        assert_eq!(engine.flow_scope_instances(inst, Some("outer")).len(), 2);
        assert_eq!(engine.flow_scope_instances(inst, Some("inner")).len(), 2);
        assert!(matches!(
            engine.resolve_flow_scope(inst, "t", None),
            Err(EngineError::AmbiguousFlowScope { .. })
        ));
    }
}
