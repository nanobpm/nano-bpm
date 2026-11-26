//! `impl Engine` methods: process-instance migration of catch-event
//! subscriptions and flow scopes (#1374, Zeebe
//! `ProcessInstanceMigrationPreconditions` /
//! `ProcessInstanceMigrationMigrateProcessor` /
//! `ProcessInstanceMigrationCatchEventBehaviour` parity).
//!
//! The command handler (`Command::MigrateInstance` in `engine/mod.rs`) owns the
//! element-level preconditions; this module owns everything attached to an
//! active element instance that migration must keep consistent:
//!
//! * **catch-event preconditions** — a catch event attached to an active
//!   element must stay attached to that element's mapped target
//!   ([`EngineError::MigrationCatchEventDetached`]), and two catch events of
//!   one element may not be merged onto one target
//!   ([`EngineError::MigrationCatchEventsMerged`]);
//! * **subscription migration** — every armed trigger (timer, message, signal
//!   or conditional subscription) of a boundary or intermediate catch event is
//!   *retained* (re-pointed by the applier) when its target's trigger is
//!   unchanged, *re-created* when the message name, correlation key, signal
//!   name, condition, timer definition or interrupting flag changes, *deleted*
//!   when its catch event is unmapped, and target boundaries with no mapped
//!   source are *created*. Arming goes through the one shared implementation
//!   ([`Engine::arm_boundary`] / [`Engine::arm_catch_event`]).

use std::collections::BTreeMap;

use super::*;
use crate::event::{
    MigratedChildInstance, MigratedElementInstance, MigratedSubscription,
    MigratedSubscriptionTrigger, SubscriptionMigrationAction,
};
use crate::model::{Element, ProcessDefinition};

/// What a trigger waits for — compared source vs target to decide between
/// retaining and re-creating a subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
enum TriggerFingerprint {
    /// `(duration, repeating, timer definition)` — `repeating` distinguishes a
    /// one-shot duration from an equal-interval cycle (both have `timer: None`).
    Timer(u64, bool, Option<crate::model::TimerDef>),
    Message(String, String),
    Signal(String),
    Conditional(String),
}

/// One armed runtime trigger resting on an active element instance.
struct ArmedTrigger {
    /// Sort key (the trigger's own key) for deterministic planning.
    key: Key,
    /// The catch element the trigger belongs to: the boundary event id, or the
    /// intermediate catch event itself.
    catch_id: ElementId,
    /// `Some(interrupting)` for a boundary trigger, `None` for an intermediate
    /// catch.
    interrupting: Option<bool>,
    fingerprint: TriggerFingerprint,
    /// The event disarming it.
    cancel: Event,
    /// Its audit form; `None` for a conditional subscription, which the
    /// persisted [`MigratedSubscriptionTrigger`] shape has no variant for (its
    /// cancel / create events are still emitted and replayed).
    report: Option<MigratedSubscriptionTrigger>,
}

/// The planned side effects of a migration beyond the element re-pointing the
/// `ProcessInstanceMigrated` applier performs.
#[derive(Default)]
pub(crate) struct MigrationPlan {
    /// Disarm events, emitted **before** `ProcessInstanceMigrated` (they name
    /// the source element ids).
    pub before: Vec<Event>,
    /// Arm events, emitted **after** `ProcessInstanceMigrated` (they name the
    /// target element ids).
    pub after: Vec<Event>,
    pub migrated_flow_scopes: Vec<MigratedElementInstance>,
    pub subscriptions: Vec<MigratedSubscription>,
    /// Call-activity child instances kept attached to their migrated
    /// call-activity element instance (#1553) — the Zeebe `childInstanceKeys`
    /// audit on `ProcessInstanceMigrated`. The child itself is NOT migrated:
    /// its parent linkage is by element-instance key, which migration
    /// preserves, so a child completing afterwards still releases the parent's
    /// (remapped) call-activity token.
    pub child_instances: Vec<MigratedChildInstance>,
}

/// `Some(interrupting)` when `kind` is a boundary event.
/// A node a migration mapping instruction may name (#1630): a definition
/// element, or an ad-hoc sub-process tool — tools are pruned from the
/// executable graph into the container's catalog, yet run as element instances
/// carrying their own id, so they must be mappable like any other activity.
pub(crate) enum MigrationNode<'a> {
    Element(&'a Element),
    AdHocTool {
        container_id: &'a str,
        tool: &'a crate::model::AdHocTool,
    },
}

impl MigrationNode<'_> {
    /// Same BPMN type on both sides (discriminant only — a service task may map
    /// to one with a different job type, Zeebe parity). An ad-hoc container
    /// must stay an ad-hoc container: its inner instances are derived from it.
    pub(crate) fn same_type_as(
        &self,
        source_def: &ProcessDefinition,
        other: &MigrationNode<'_>,
        target_def: &ProcessDefinition,
    ) -> bool {
        match (self, other) {
            (MigrationNode::Element(s), MigrationNode::Element(t)) => {
                std::mem::discriminant(&s.kind) == std::mem::discriminant(&t.kind)
                    && is_adhoc_container(source_def, &s.id)
                        == is_adhoc_container(target_def, &t.id)
            }
            (
                MigrationNode::AdHocTool { tool: s, .. },
                MigrationNode::AdHocTool { tool: t, .. },
            ) => {
                std::mem::discriminant(&s.kind) == std::mem::discriminant(&t.kind)
                    // A nested ad-hoc container catalogues as a `ServiceTask`
                    // tool, so the discriminant alone cannot tell it from a
                    // plain service-task tool. Apply the same container-status
                    // check as the element branch: a container tool must map to
                    // a container tool, or its derived `#innerInstance` and
                    // ad-hoc bookkeeping would attach to a target with no
                    // container definition (and the reverse would invent one).
                    && is_adhoc_container(source_def, &s.element_id)
                        == is_adhoc_container(target_def, &t.element_id)
            }
            _ => false,
        }
    }
}

/// Resolve a mapping-instruction element id against `def` (see [`MigrationNode`]).
pub(crate) fn migration_node<'a>(
    def: &'a ProcessDefinition,
    id: &str,
) -> Option<MigrationNode<'a>> {
    if let Some(element) = def.element(id) {
        return Some(MigrationNode::Element(element));
    }
    def.adhoc.iter().find_map(|ad| {
        ad.tools
            .iter()
            .find(|t| t.element_id == id)
            .map(|tool| MigrationNode::AdHocTool {
                container_id: ad.container_id.as_str(),
                tool,
            })
    })
}

/// Whether `id` is an ad-hoc sub-process container of `def`.
pub(crate) fn is_adhoc_container(def: &ProcessDefinition, id: &str) -> bool {
    def.adhoc.iter().any(|ad| ad.container_id == id)
}

/// The direct flow scope of a migrated active element id (`None` = the
/// process): an ad-hoc tool and a container's `#innerInstance` both sit in
/// their container (#1630); any other element in its enclosing sub-process.
pub(crate) fn migration_flow_scope(def: &ProcessDefinition, id: &str) -> Option<ElementId> {
    if let Some(container) = id.strip_suffix(ADHOC_INNER_INSTANCE_ID_POSTFIX) {
        if is_adhoc_container(def, container) {
            return Some(container.to_string());
        }
    }
    match migration_node(def, id) {
        Some(MigrationNode::AdHocTool { container_id, .. }) => Some(container_id.to_string()),
        _ => super::flow_scope::element_flow_scope_chain(def, id)
            .into_iter()
            .next(),
    }
}

fn boundary_interrupting(kind: &ElementKind) -> Option<bool> {
    match kind {
        ElementKind::TimerBoundaryEvent { interrupting, .. }
        | ElementKind::MessageBoundaryEvent { interrupting, .. }
        | ElementKind::SignalBoundaryEvent { interrupting, .. }
        | ElementKind::ConditionalBoundaryEvent { interrupting, .. } => Some(*interrupting),
        _ => kind.attached_to().map(|_| true),
    }
}

fn boundary_id_of(kind: &state::MessageSubscriptionKind) -> Option<(ElementId, bool)> {
    match kind {
        state::MessageSubscriptionKind::InterruptingBoundary {
            boundary_element_id,
        } => Some((boundary_element_id.clone(), true)),
        state::MessageSubscriptionKind::NonInterruptingBoundary {
            boundary_element_id,
        } => Some((boundary_element_id.clone(), false)),
        _ => None,
    }
}

fn timer_fingerprint(element: &Element) -> Option<TriggerFingerprint> {
    match &element.kind {
        ElementKind::TimerBoundaryEvent {
            duration_millis,
            repeating,
            ..
        } => Some(TriggerFingerprint::Timer(
            *duration_millis,
            *repeating,
            element.timer.clone(),
        )),
        ElementKind::TimerIntermediateCatchEvent { duration_millis } => Some(
            TriggerFingerprint::Timer(*duration_millis, false, element.timer.clone()),
        ),
        _ => None,
    }
}

impl Engine {
    /// No two event sub-processes of one live scope may migrate onto the same
    /// target event sub-process (#1379 review; Zeebe
    /// `requireNoDuplicateTargetsInCatchEventMappings` parity). A source event
    /// sub-process's counterpart is its mapping target, or itself when
    /// unmapped (implicit retention by id) — so `b -> a` collides with an
    /// unmapped `a` that the target still contains. Merging would leave two
    /// live triggers for one target event sub-process (its handler would run
    /// twice per trigger).
    pub(crate) fn check_migrated_event_subprocesses(
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &BTreeMap<String, String>,
        active: &[(Key, String)],
    ) -> Result<(), EngineError> {
        use super::event_subprocess::ArmableEsp;
        let mut scopes: Vec<(Option<&str>, Option<&str>)> = vec![(None, None)];
        let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for (_, source_id) in active {
            let owns = source
                .element(source_id)
                .is_some_and(|e| e.kind.owns_token_scope());
            if owns && seen.insert(source_id.as_str()) {
                if let Some(t) = mapped.get(source_id) {
                    scopes.push((Some(source_id.as_str()), Some(t.as_str())));
                }
            }
        }
        for (source_scope, target_scope) in scopes {
            let target_ids: Vec<String> = ArmableEsp::in_scope(target, target_scope)
                .into_iter()
                .map(|e| e.esp_id)
                .collect();
            let mut by_target: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for esp in ArmableEsp::in_scope(source, source_scope) {
                let tgt = mapped
                    .get(&esp.esp_id)
                    .cloned()
                    .unwrap_or_else(|| esp.esp_id.clone());
                if target_ids.contains(&tgt) {
                    by_target.entry(tgt).or_default().push(esp.esp_id);
                }
            }
            if let Some((tgt, mut sources)) = by_target.into_iter().find(|(_, s)| s.len() > 1) {
                sources.sort();
                return Err(EngineError::MigrationCatchEventsMerged {
                    instance_key,
                    element_id: source_scope.map_or_else(|| source.id.clone(), str::to_string),
                    target_catch_event_id: tgt,
                    source_catch_event_ids: sources,
                });
            }
        }
        Ok(())
    }

    /// Zeebe catch-event preconditions for one active element `source_id`
    /// mapped onto `target_id`: every mapped boundary of the source (or, for an
    /// event-based gateway, every mapped catch event it subscribes to) must
    /// target a boundary attached to `target_id` (a catch event of the target
    /// gateway), and no two may target the same catch event (no merging).
    pub(crate) fn check_migrated_catch_events(
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &BTreeMap<String, String>,
        source_id: &str,
        target_id: &str,
    ) -> Result<(), EngineError> {
        // An event-based gateway's catch events are its subscription targets
        // (#2004, Zeebe migrates the gateway's subscriptions with it): each
        // mapped one must stay a target of the target gateway.
        let mut boundaries: Vec<&Element> = source
            .elements
            .values()
            .filter(|e| e.kind.attached_to() == Some(source_id))
            .chain(source.event_gateway_targets(source_id))
            .collect();
        boundaries.sort_by(|a, b| a.id.cmp(&b.id));
        let target_gateway_targets = target.event_gateway_targets(target_id);
        let mut by_target: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for b in boundaries {
            let Some(tb) = mapped.get(&b.id) else {
                continue;
            };
            let attached = target
                .element(tb)
                .and_then(|e| e.kind.attached_to())
                .is_some_and(|a| a == target_id)
                || target_gateway_targets.iter().any(|t| t.id == *tb);
            if !attached {
                return Err(EngineError::MigrationCatchEventDetached {
                    instance_key,
                    element_id: source_id.to_string(),
                    target_element_id: target_id.to_string(),
                    catch_event_id: b.id.clone(),
                    target_catch_event_id: tb.clone(),
                });
            }
            by_target.entry(tb.as_str()).or_default().push(b.id.clone());
        }
        if let Some((tb, sources)) = by_target.into_iter().find(|(_, s)| s.len() > 1) {
            return Err(EngineError::MigrationCatchEventsMerged {
                instance_key,
                element_id: source_id.to_string(),
                target_catch_event_id: tb.to_string(),
                source_catch_event_ids: sources,
            });
        }
        Ok(())
    }

    /// Every armed trigger resting on any of `hosts` (element-instance key ->
    /// host element id), grouped by host and sorted by key. One pass over each
    /// partition-wide trigger table per migration, not one per active element.
    fn armed_triggers_on(
        &self,
        hosts: &BTreeMap<Key, ElementId>,
        source: &ProcessDefinition,
    ) -> BTreeMap<Key, Vec<ArmedTrigger>> {
        let mut by_host: BTreeMap<Key, Vec<ArmedTrigger>> = BTreeMap::new();
        for t in self.state.timers.values() {
            if !hosts.contains_key(&t.element_instance_key) {
                continue;
            }
            if t.state != state::TimerState::Created {
                continue;
            }
            let (catch_id, interrupting) = match &t.kind {
                state::TimerKind::InterruptingBoundary {
                    boundary_element_id,
                } => (boundary_element_id.clone(), Some(true)),
                state::TimerKind::NonInterruptingBoundary {
                    boundary_element_id,
                } => (boundary_element_id.clone(), Some(false)),
                // An intermediate catch's trigger names its catch element: the
                // host itself, or a catch an event-based gateway host
                // subscribes to (#2004).
                state::TimerKind::IntermediateCatch => (t.element_id.clone(), None),
                // An event sub-process trigger rests on its scope, not on a
                // token; it is not a per-element trigger to re-map here.
                state::TimerKind::EventSubProcess { .. } => continue,
            };
            let Some(fingerprint) = source.element(&catch_id).and_then(timer_fingerprint) else {
                continue;
            };
            by_host
                .entry(t.element_instance_key)
                .or_default()
                .push(ArmedTrigger {
                    key: t.key,
                    catch_id,
                    interrupting,
                    fingerprint,
                    cancel: Event::TimerCanceled {
                        timer_key: t.key,
                        instance_key: t.instance_key,
                        element_instance_key: t.element_instance_key,
                        element_id: t.element_id.clone(),
                    },
                    report: Some(MigratedSubscriptionTrigger::Timer {
                        timer_key: t.key,
                        due_at: t.due_at,
                    }),
                });
        }
        let catch_of = |kind: &state::MessageSubscriptionKind, catch_element_id: &ElementId| {
            match boundary_id_of(kind) {
                Some((id, interrupting)) => Some((id, Some(interrupting))),
                None if *kind == state::MessageSubscriptionKind::IntermediateCatch => {
                    Some((catch_element_id.clone(), None))
                }
                None => None,
            }
        };
        for s in self.state.message_subscriptions.values() {
            if !hosts.contains_key(&s.element_instance_key) {
                continue;
            }
            if !matches!(
                s.state,
                state::MessageSubscriptionState::Open | state::MessageSubscriptionState::Opening
            ) {
                continue;
            }
            let Some((catch_id, interrupting)) = catch_of(&s.kind, &s.element_id) else {
                continue;
            };
            by_host
                .entry(s.element_instance_key)
                .or_default()
                .push(ArmedTrigger {
                    key: s.key,
                    catch_id,
                    interrupting,
                    fingerprint: TriggerFingerprint::Message(
                        s.message_name.clone(),
                        s.correlation_key.clone(),
                    ),
                    cancel: Self::disarm_subscription_event(s),
                    report: Some(MigratedSubscriptionTrigger::Message {
                        subscription_key: s.key,
                        message_name: s.message_name.clone(),
                        correlation_key: s.correlation_key.clone(),
                    }),
                });
        }
        for s in self.state.signal_subscriptions.values() {
            if !hosts.contains_key(&s.element_instance_key) {
                continue;
            }
            if s.state != state::MessageSubscriptionState::Open {
                continue;
            }
            let Some((catch_id, interrupting)) = catch_of(&s.kind, &s.element_id) else {
                continue;
            };
            by_host
                .entry(s.element_instance_key)
                .or_default()
                .push(ArmedTrigger {
                    key: s.key,
                    catch_id,
                    interrupting,
                    fingerprint: TriggerFingerprint::Signal(s.signal_name.clone()),
                    cancel: Event::SignalSubscriptionCanceled {
                        subscription_key: s.key,
                        instance_key: s.instance_key,
                        element_instance_key: s.element_instance_key,
                        element_id: s.element_id.clone(),
                    },
                    report: Some(MigratedSubscriptionTrigger::Signal {
                        subscription_key: s.key,
                        signal_name: s.signal_name.clone(),
                    }),
                });
        }
        for s in self.state.conditional_subscriptions.values() {
            if !hosts.contains_key(&s.element_instance_key) {
                continue;
            }
            if s.state != state::MessageSubscriptionState::Open {
                continue;
            }
            let Some((catch_id, interrupting)) = catch_of(&s.kind, &s.element_id) else {
                continue;
            };
            by_host
                .entry(s.element_instance_key)
                .or_default()
                .push(ArmedTrigger {
                    key: s.key,
                    catch_id,
                    interrupting,
                    fingerprint: TriggerFingerprint::Conditional(s.condition.clone()),
                    cancel: Event::ConditionalSubscriptionCanceled {
                        subscription_key: s.key,
                        instance_key: s.instance_key,
                        element_instance_key: s.element_instance_key,
                        element_id: s.element_id.clone(),
                    },
                    report: None,
                });
        }
        for triggers in by_host.values_mut() {
            triggers.sort_by_key(|t| t.key);
        }
        by_host
    }

    /// The fingerprint `target` (a catch element) would arm with, evaluating
    /// its expressions against `vars`.
    fn target_fingerprint(
        &self,
        vars: &BTreeMap<String, Value>,
        target: &Element,
    ) -> Result<Option<TriggerFingerprint>, expression::Failure> {
        Ok(match &target.kind {
            ElementKind::MessageBoundaryEvent {
                message_name,
                correlation_key,
                ..
            }
            | ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            } => Some(TriggerFingerprint::Message(
                self.resolve_event_name(vars, message_name)?,
                self.resolve_correlation_value(vars, correlation_key)?,
            )),
            ElementKind::SignalBoundaryEvent { signal_name, .. }
            | ElementKind::SignalIntermediateCatchEvent { signal_name } => Some(
                TriggerFingerprint::Signal(self.resolve_event_name(vars, signal_name)?),
            ),
            ElementKind::ConditionalBoundaryEvent { condition, .. }
            | ElementKind::ConditionalIntermediateCatchEvent { condition } => {
                Some(TriggerFingerprint::Conditional(condition.clone()))
            }
            _ => timer_fingerprint(target),
        })
    }

    /// The audit form of a freshly armed trigger event.
    fn report_of(event: &Event) -> Option<MigratedSubscriptionTrigger> {
        match event {
            Event::TimerCreated {
                timer_key, due_at, ..
            } => Some(MigratedSubscriptionTrigger::Timer {
                timer_key: *timer_key,
                due_at: *due_at,
            }),
            Event::MessageSubscriptionCreated {
                subscription_key,
                message_name,
                correlation_key,
                ..
            }
            | Event::MessageSubscriptionOpening {
                subscription_key,
                message_name,
                correlation_key,
                ..
            } => Some(MigratedSubscriptionTrigger::Message {
                subscription_key: *subscription_key,
                message_name: message_name.clone(),
                correlation_key: correlation_key.clone(),
            }),
            Event::SignalSubscriptionCreated {
                subscription_key,
                signal_name,
                ..
            } => Some(MigratedSubscriptionTrigger::Signal {
                subscription_key: *subscription_key,
                signal_name: signal_name.clone(),
            }),
            _ => None,
        }
    }

    /// Plans the subscription and flow-scope side effects of migrating
    /// `instance_key` onto `target` under `mapped`. Every precondition has
    /// already passed; this only mints keys for the triggers it (re-)arms.
    ///
    /// A target catch event whose expression (message / signal name,
    /// correlation key, timer) fails to evaluate rejects the migration (Zeebe
    /// `ProcessInstanceMigrationCatchEventBehavior`: "... must be subscribed to
    /// a catch event. <failure>", `INVALID_STATE`, #1398) — no key is consumed.
    pub(crate) fn plan_migration(
        &mut self,
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &BTreeMap<String, String>,
    ) -> Result<MigrationPlan, EngineError> {
        let key_mark = self.next_local;
        let planned = self.plan_migration_inner(instance_key, source, target, mapped);
        planned.map_err(|(element_id, target_element_id, failure)| {
            self.next_local = key_mark;
            EngineError::MigrationCatchEventSubscriptionFailed {
                instance_key,
                element_id,
                target_element_id,
                reason: failure.message,
            }
        })
    }

    fn plan_migration_inner(
        &mut self,
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &BTreeMap<String, String>,
    ) -> Result<MigrationPlan, (ElementId, ElementId, expression::Failure)> {
        let mut plan = MigrationPlan::default();
        // `active` is a BTreeMap (#1662): collected already in key order.
        let active: Vec<(Key, ElementId)> = self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.active.iter().map(|(k, e)| (*k, e.clone())).collect())
            .unwrap_or_default();
        // Index triggers and lifecycle blockers once per migration so the
        // per-element loop below is a lookup, not a partition-wide scan.
        let hosts: BTreeMap<Key, ElementId> = active.iter().cloned().collect();
        let mut armed_by_host = self.armed_triggers_on(&hosts, source);
        let not_armable = self.hosts_not_armable_now(&hosts);
        let call_eiks: std::collections::BTreeSet<Key> = active
            .iter()
            .filter(|(_, e)| {
                matches!(
                    source.element(e).map(|el| &el.kind),
                    Some(ElementKind::CallActivity { .. })
                )
            })
            .map(|(k, _)| *k)
            .collect();
        let children = self.call_activity_children_of(&call_eiks);
        let active_snapshot = active.clone();
        for (eik, source_id) in active {
            let Some(target_id) = mapped.get(&source_id).cloned() else {
                // Every unmapped active element was rejected before planning.
                continue;
            };
            if source
                .element(&source_id)
                .is_some_and(|e| e.kind.owns_token_scope())
            {
                plan.migrated_flow_scopes.push(MigratedElementInstance {
                    element_instance_key: eik,
                    source_element_id: source_id.clone(),
                    target_element_id: target_id.clone(),
                });
            }
            // A migrated call activity keeps its child process instance
            // attached (Zeebe `MigrateCallActivityTest`: the child is NOT
            // migrated — it keeps running its own definition and its parent
            // link, keyed by element-instance key, survives the remap).
            // Record the kept child for the `ProcessInstanceMigrated` audit
            // (Zeebe's `childInstanceKeys`).
            if matches!(
                source.element(&source_id).map(|e| &e.kind),
                Some(ElementKind::CallActivity { .. })
            ) {
                if let Some(&child_instance_key) = children.get(&eik) {
                    plan.child_instances.push(MigratedChildInstance {
                        call_activity_element_instance_key: eik,
                        child_instance_key,
                        target_element_id: target_id.clone(),
                    });
                }
            }
            // A multi-instance CHILD shares its body's element id, but a
            // boundary on a multi-instance activity is armed on the BODY alone
            // (`run_mi_body_activation`): it interrupts / spawns beside the
            // whole loop, never one child. Reconciling boundaries against a
            // child would arm a newly-added target boundary once per active
            // child (duplicating a non-interrupting side path, or interrupting
            // one child instead of the loop). Skip boundary reconciliation for
            // a child — its other migration work above (flow scope, kept
            // call-activity child) still runs.
            if self.multi_instance_of(instance_key, &source_id).is_some()
                && !self.is_multi_instance_body_structural(instance_key, eik)
            {
                continue;
            }
            let scope = self.scope_of(instance_key, eik);
            // Boundaries evaluate against the enclosing scope (as at activation);
            // an intermediate catch evaluates against its own element-instance
            // view so its input-mapped locals stay visible.
            let boundary_vars = self.variables_for_element(instance_key, scope);
            let catch_vars = self.variables_for_element(instance_key, eik);
            // A target boundary explicitly mapped from a source boundary on this
            // element is never "new": if its source trigger is no longer armed
            // (a one-shot non-interrupting timer that already fired) it must not
            // be re-armed, or the side path would run twice.
            let fail = |f: expression::Failure| (source_id.clone(), target_id.clone(), f);
            let mut covered: Vec<ElementId> = Self::armable_boundaries(source, &source_id)
                .iter()
                .filter_map(|b| mapped.get(&b.id).cloned())
                .chain(
                    source
                        .event_gateway_targets(&source_id)
                        .into_iter()
                        .filter_map(|c| mapped.get(&c.id).cloned()),
                )
                .collect();
            for armed in armed_by_host.remove(&eik).unwrap_or_default() {
                let vars = if armed.interrupting.is_some() {
                    &boundary_vars
                } else {
                    &catch_vars
                };
                let target_catch = mapped
                    .get(&armed.catch_id)
                    .and_then(|tc| target.element(tc))
                    .cloned();
                let Some(target_catch) = target_catch else {
                    // Unmapped catch event: its trigger is deleted.
                    plan.before.push(armed.cancel);
                    if let Some(trigger) = armed.report {
                        plan.subscriptions.push(MigratedSubscription {
                            element_instance_key: eik,
                            element_id: armed.catch_id,
                            action: SubscriptionMigrationAction::Deleted,
                            trigger,
                        });
                    }
                    continue;
                };
                covered.push(target_catch.id.clone());
                let target_interrupting = match armed.interrupting {
                    Some(_) => boundary_interrupting(&target_catch.kind),
                    None => None,
                };
                let unchanged = self
                    .target_fingerprint(vars, &target_catch)
                    .map_err(fail)?
                    .as_ref()
                    == Some(&armed.fingerprint)
                    && target_interrupting == armed.interrupting;
                if unchanged {
                    if let Some(trigger) = armed.report {
                        plan.subscriptions.push(MigratedSubscription {
                            element_instance_key: eik,
                            element_id: target_catch.id.clone(),
                            action: SubscriptionMigrationAction::Retained,
                            trigger,
                        });
                    }
                    continue;
                }
                // The trigger changed: close it and open the target's.
                plan.before.push(armed.cancel);
                if let Some(trigger) = armed.report {
                    plan.subscriptions.push(MigratedSubscription {
                        element_instance_key: eik,
                        element_id: armed.catch_id.clone(),
                        action: SubscriptionMigrationAction::Deleted,
                        trigger,
                    });
                }
                let armed_events = if armed.interrupting.is_some() {
                    self.arm_boundary(instance_key, eik, &target_id, vars, &target_catch)
                        .map_err(fail)?
                        .into_iter()
                        .collect()
                } else {
                    self.arm_catch_event(instance_key, eik, vars, &target_catch)
                        .map_err(fail)?
                };
                for event in armed_events {
                    if let Some(trigger) = Self::report_of(&event) {
                        plan.subscriptions.push(MigratedSubscription {
                            element_instance_key: eik,
                            element_id: target_catch.id.clone(),
                            action: SubscriptionMigrationAction::Recreated,
                            trigger,
                        });
                    }
                    plan.after.push(event);
                }
            }
            // Only a host in its steady active phase gets newly armed target
            // boundaries. A host still in its start-listener chain or parked on
            // an activation-phase incident will (re-)run its activation body,
            // which arms the (now target) boundaries — arming here would
            // duplicate them. A host completing (end listeners / completion
            // incident) has already had its boundaries cancelled, so arming
            // here would leave triggers outliving the host.
            if not_armable.contains(&eik) {
                continue;
            }
            // An event-based gateway's target catch events with no mapped
            // source catch are newly subscribed, on the gateway (#2004).
            let new_catches: Vec<Element> = target
                .event_gateway_targets(&target_id)
                .into_iter()
                .filter(|c| !covered.contains(&c.id))
                .cloned()
                .collect();
            for catch in new_catches {
                for event in self
                    .arm_catch_event(instance_key, eik, &catch_vars, &catch)
                    .map_err(fail)?
                {
                    if let Some(trigger) = Self::report_of(&event) {
                        plan.subscriptions.push(MigratedSubscription {
                            element_instance_key: eik,
                            element_id: catch.id.clone(),
                            action: SubscriptionMigrationAction::Created,
                            trigger,
                        });
                    }
                    plan.after.push(event);
                }
            }
            // Target boundaries with no mapped source boundary are newly armed.
            for boundary in Self::armable_boundaries(target, &target_id) {
                if covered.contains(&boundary.id) {
                    continue;
                }
                if let Some(event) = self
                    .arm_boundary(instance_key, eik, &target_id, &boundary_vars, &boundary)
                    .map_err(fail)?
                {
                    if let Some(trigger) = Self::report_of(&event) {
                        plan.subscriptions.push(MigratedSubscription {
                            element_instance_key: eik,
                            element_id: boundary.id.clone(),
                            action: SubscriptionMigrationAction::Created,
                            trigger,
                        });
                    }
                    plan.after.push(event);
                }
            }
        }
        self.plan_event_subprocess_migration(
            &mut plan,
            instance_key,
            source,
            target,
            mapped,
            &active_snapshot,
            &not_armable,
        )?;
        Ok(plan)
    }

    /// Scope-owned event sub-process trigger reconciliation (#1379, Zeebe
    /// `ProcessInstanceMigrationMigrateProcessor` event-sub-process start
    /// subscription parity). An event sub-process trigger rests on its host
    /// *scope* — the process root (`0`) or a migrated embedded / event
    /// sub-process instance — not on an active element, so the per-element
    /// pass above never sees it. For every live scope:
    ///
    /// * an armed trigger whose event sub-process keeps its id, start event,
    ///   trigger definition and interrupting flag in the target scope is
    ///   **retained** untouched;
    /// * any other armed trigger (event sub-process removed, moved out of the
    ///   scope, renamed via a mapping instruction, or its trigger changed) is
    ///   **deleted**, and its target counterpart (if any) is **re-created**;
    /// * a target event sub-process with no source counterpart in the scope is
    ///   newly **created** — unless the scope is not in its steady phase or an
    ///   interrupting event sub-process already took it over.
    ///
    /// A source event sub-process whose trigger is no longer armed (a one-shot
    /// timer that already fired) is never re-armed, so its handler cannot run
    /// twice. Arming goes through the single shared implementation
    /// ([`Engine::arm_event_subprocess_triggers`]).
    #[allow(clippy::too_many_arguments, reason = "planner internals")]
    fn plan_event_subprocess_migration(
        &mut self,
        plan: &mut MigrationPlan,
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &BTreeMap<String, String>,
        active: &[(Key, ElementId)],
        not_armable: &BTreeSet<Key>,
    ) -> Result<(), (ElementId, ElementId, expression::Failure)> {
        use super::event_subprocess::ArmableEsp;
        // Live scopes: the root, plus every active scope-owning element
        // instance (embedded / event sub-process) with its mapped target.
        let mut scopes: Vec<(Key, Option<ElementId>, Option<ElementId>)> = vec![(0, None, None)];
        for (eik, source_id) in active {
            let owns = source
                .element(source_id)
                .is_some_and(|e| e.kind.owns_token_scope());
            if !owns {
                continue;
            }
            // A multi-instance embedded sub-process's structural BODY shares
            // its element id with its children, but event sub-process triggers
            // are armed on each CHILD scope only. Hosting triggers on the body
            // would arm an extra trigger per migrated loop (#1630 review).
            if self.multi_instance_of(instance_key, source_id).is_some()
                && self.is_multi_instance_body_structural(instance_key, *eik)
            {
                continue;
            }
            if let Some(target_id) = mapped.get(source_id) {
                scopes.push((*eik, Some(source_id.clone()), Some(target_id.clone())));
            }
        }
        let armed = self.armed_event_subprocess_triggers(instance_key);
        let counterpart = |id: &ElementId| mapped.get(id).cloned().unwrap_or_else(|| id.clone());
        // Target event sub-process ids accounted for by a CONSUMED source
        // counterpart: a source event sub-process that carries a subscription
        // trigger but is no longer armed (a one-shot timer that already fired).
        // Such a counterpart must never be (re-)armed — its handler already
        // ran. Each entry is keyed by `(source host scope, target esp id)`: the
        // host lineage is kept because the same definition id can be hosted by
        // several live instances of one scope element (e.g. two concurrent
        // instances of a non-interrupting event sub-process each embedding a
        // further sub-process), so a consumed trigger in one host must not
        // suppress the still-armed counterpart another host moves into the same
        // destination scope (#1379 review).
        //
        // Armedness is keyed by `(scope_eik, esp_id)` for the same reason: an
        // armed trigger in one host must not hide a consumed one in another.
        // A scope whose activation is still pending (a start-listener chain or
        // an activation-phase incident will (re-)run its activation body, which
        // arms its triggers) never armed its triggers yet, so its unarmed event
        // sub-processes are NOT consumed — they are skipped here. A
        // completion-pending scope (an end-listener chain or a
        // completion-phase incident) HAS armed them already: its unarmed
        // subscription triggers fired and ARE consumed (#1379 review).
        let armed_source: BTreeSet<(Key, &str)> = armed
            .iter()
            .map(|t| (t.scope_eik, t.esp_id.as_str()))
            .collect();
        // Each live scope instance's mapped TARGET scope-element id (`None`
        // for the root).
        let scope_target: BTreeMap<Key, Option<ElementId>> =
            scopes.iter().map(|(eik, _, t)| (*eik, t.clone())).collect();
        // The live scope instance(s) a counterpart `tgt_esp` of an event
        // sub-process hosted by source scope instance `host` lands in. The
        // target parent id is a TARGET id and is used unchanged. It is
        // resolved through `host`'s OWN runtime lineage, bounded by the
        // MAPPED COMMON MODEL ANCESTOR: the nearest runtime ancestor of
        // `host` whose mapped target id is a model ancestor of the target
        // parent (the root when that parent sits at the process level). Only
        // that instance's subtree is searched — itself first (a move
        // outward), then its live descendants with that target (a move inward
        // or sideways between siblings) — so the search never climbs past it
        // into an unrelated concurrent host instance and suppresses that
        // host's own trigger (#1379 review).
        let destinations_of = |host: Key, tgt_esp: &str| -> Vec<Key> {
            let Some(parent) = target.element(tgt_esp).map(|el| el.parent.clone()) else {
                return Vec::new();
            };
            let Some(parent) = parent else {
                // A process-level target ESP lives in the root scope.
                return vec![0];
            };
            // The mapped target ids of the target parent's MODEL ancestors
            // (itself included): the nearest runtime ancestor of `host` whose
            // mapped target id is in this set is the mapped common ancestor —
            // the search must not climb past it. An ancestor id absent from
            // the source model contributes nothing (its source counterpart
            // does not exist, so no live scope can map to it). Read off the
            // one canonical, cycle-safe chain walk (#1374): a malformed
            // cyclic `parent` chain (e.g. `contained_in("sub", "sub")`, which
            // deployment preflight does not reject) must not hang migration
            // (#1379 review).
            let chain: BTreeSet<ElementId> = std::iter::once(parent.clone())
                .chain(flow_scope::element_flow_scope_chain(target, &parent))
                .collect();
            let within = |a: Key, root: Key| -> bool {
                let mut c = a;
                loop {
                    if c == root {
                        return true;
                    }
                    if c == 0 {
                        return false;
                    }
                    c = self.scope_of(instance_key, c);
                }
            };
            // Widen from `host` outward one runtime ancestor at a time. The
            // first ancestor whose mapped target id is a model ancestor of
            // the target parent BOUNDS the search: it wins itself when its
            // target IS the parent (a move outward), else only its own live
            // descendants with that target are destinations (a move inward or
            // sideways between siblings below it). When no runtime ancestor
            // maps into the chain the root bounds the search, preserving the
            // root-level sibling resolution (#1379 review).
            let mut anc = host;
            loop {
                let bounds = anc == 0
                    || scope_target
                        .get(&anc)
                        .and_then(|t| t.as_deref())
                        .is_some_and(|t| chain.contains(t));
                // The ancestor itself wins over its descendants (a move
                // outward lands in exactly that instance).
                if scope_target.get(&anc).and_then(|t| t.as_deref()) == Some(parent.as_str()) {
                    return vec![anc];
                }
                let mut hits: Vec<Key> = scope_target
                    .iter()
                    .filter(|(eik, t)| t.as_deref() == Some(parent.as_str()) && within(**eik, anc))
                    .map(|(eik, _)| *eik)
                    .collect();
                if !hits.is_empty() {
                    hits.sort_unstable();
                    return hits;
                }
                if bounds {
                    // The mapped common ancestor has no matching scope in its
                    // subtree: the destination does not exist on this side.
                    return Vec::new();
                }
                anc = self.scope_of(instance_key, anc);
            }
        };
        // `(destination scope, target esp id)` pairs a CONSUMED source
        // counterpart lands in (staying or moved).
        let mut consumed_in: BTreeSet<(Key, ElementId)> = BTreeSet::new();
        for (scope_eik, source_scope, _) in &scopes {
            if self.activation_pending(instance_key, *scope_eik, source, active) {
                continue;
            }
            for e in ArmableEsp::in_scope(source, source_scope.as_deref()) {
                if e.trigger.is_subscription()
                    && !armed_source.contains(&(*scope_eik, e.esp_id.as_str()))
                {
                    let tgt = counterpart(&e.esp_id);
                    for d in destinations_of(*scope_eik, &tgt) {
                        consumed_in.insert((d, tgt.clone()));
                    }
                }
            }
        }
        // Target event sub-process ids with an ARMED source counterpart in any
        // scope: arming one in a different scope (a cross-scope move) replaces
        // an existing trigger, so it is audited as `Recreated`, not `Created`.
        let armed_targets: BTreeSet<ElementId> =
            armed.iter().map(|t| counterpart(&t.esp_id)).collect();
        // `(destination scope, TARGET esp id)` pairs an ARMED source trigger
        // moves INTO from a different host scope: it is cancelled in its host
        // and re-armed at exactly that destination, which a consumed
        // counterpart from another host must not suppress (#1379 review).
        let mut armed_in: BTreeSet<(Key, ElementId)> = BTreeSet::new();
        for t in &armed {
            let tgt = counterpart(&t.esp_id);
            for d in destinations_of(t.scope_eik, &tgt) {
                if d != t.scope_eik {
                    armed_in.insert((d, tgt.clone()));
                }
            }
        }
        for (scope_eik, source_scope, target_scope) in scopes {
            let source_esps = ArmableEsp::in_scope(source, source_scope.as_deref());
            let target_esps = ArmableEsp::in_scope(target, target_scope.as_deref());
            // Target event sub-processes a source counterpart accounts for
            // IN THIS SCOPE: its armed trigger (if any) is retained or
            // re-created above, and one with NO armed trigger (a fired one-shot
            // timer) that STAYS in this scope must not be re-armed. A source
            // event sub-process the migration MOVES OUT of this scope covers
            // nothing here — its consumed counterpart is suppressed by the
            // lineage-resolved `consumed_in` set instead, so a sibling host's still-armed
            // counterpart moving into this same scope is not hidden by it
            // (#1379 review). Error / escalation triggers are never
            // subscriptions — they have nothing to retain or re-create — so
            // they cover nothing: a target that turns one into a subscription
            // trigger is newly armed below (#1379 review).
            let covered: BTreeSet<ElementId> = source_esps
                .iter()
                .filter(|e| {
                    e.trigger.is_subscription()
                        && ArmableEsp::in_scope(target, target_scope.as_deref())
                            .iter()
                            .any(|g| g.esp_id == counterpart(&e.esp_id))
                })
                .map(|e| counterpart(&e.esp_id))
                .collect();
            let mut to_arm: Vec<ElementId> = Vec::new();
            // Target event sub-processes re-armed in place of a deleted source
            // trigger (vs newly created ones), by start-event id.
            let mut recreated: BTreeSet<ElementId> = BTreeSet::new();
            // Target ids whose armed trigger is retained in place here.
            let mut handled: BTreeSet<ElementId> = BTreeSet::new();
            for t in armed.iter().filter(|t| t.scope_eik == scope_eik) {
                let src = source_esps.iter().find(|e| e.esp_id == t.esp_id);
                let tgt_id = counterpart(&t.esp_id);
                let tgt = target_esps.iter().find(|e| e.esp_id == tgt_id);
                let retained = match (src, tgt) {
                    (Some(s), Some(g)) => {
                        // The trigger is retained only when it survives the
                        // `ProcessInstanceMigrated` remap naming the TARGET
                        // start event: the source start event's mapped
                        // counterpart must be the target's start event.
                        // Otherwise (a mapping such as `esp_start -> s`
                        // redirects it, e.g. onto the process start event) the
                        // applier would re-point the retained trigger's
                        // `element_id` at that wrong element, and a repeating
                        // timer would keep the incorrect id — so delete and
                        // re-create it under the target's actual start event
                        // instead (#1379 review).
                        s.esp_id == g.esp_id
                            && s.start_id == g.start_id
                            && counterpart(&s.start_id) == g.start_id
                            && t.start_id == g.start_id
                            && s.trigger == g.trigger
                            && s.interrupting == g.interrupting
                    }
                    _ => false,
                };
                if retained {
                    handled.insert(tgt_id.clone());
                    if let Some(trigger) = t.report.clone() {
                        plan.subscriptions.push(MigratedSubscription {
                            element_instance_key: scope_eik,
                            element_id: t.start_id.clone(),
                            action: SubscriptionMigrationAction::Retained,
                            trigger,
                        });
                    }
                    continue;
                }
                plan.before.push(t.cancel.clone());
                if let Some(trigger) = t.report.clone() {
                    plan.subscriptions.push(MigratedSubscription {
                        element_instance_key: scope_eik,
                        element_id: t.start_id.clone(),
                        action: SubscriptionMigrationAction::Deleted,
                        trigger,
                    });
                }
                if let Some(g) = tgt {
                    recreated.insert(g.start_id.clone());
                    if !to_arm.contains(&tgt_id) {
                        to_arm.push(tgt_id);
                    }
                }
            }
            // New target event sub-processes (no source counterpart) arm only
            // in a steady, not-yet-interrupted scope.
            let interrupted = active.iter().any(|(eik, id)| {
                matches!(
                    source.element(id).map(|e| &e.kind),
                    Some(ElementKind::EventSubProcess {
                        interrupting: true,
                        ..
                    })
                ) && self.scope_of(instance_key, *eik) == scope_eik
            });
            if !interrupted
                && !not_armable.contains(&scope_eik)
                && !self.root_pending(instance_key, scope_eik, source, active)
            {
                for g in &target_esps {
                    // A CONSUMED source counterpart suppresses arming here only
                    // when it is THIS scope's own trigger: one staying in this
                    // scope (its host IS this scope), or one moved in from
                    // another host that did NOT cancel an armed trigger of its
                    // own for the same target. When a sibling host's armed
                    // trigger moved in (in `armed_in`), THIS scope arms that
                    // trigger — a consumed counterpart from a DIFFERENT host
                    // must not suppress it (#1379 review).
                    let key = (scope_eik, g.esp_id.clone());
                    let armed_move_in = armed_in.contains(&key);
                    let consumed_here = consumed_in.contains(&key);
                    if !handled.contains(&g.esp_id)
                        && (armed_move_in || (!covered.contains(&g.esp_id) && !consumed_here))
                        && !to_arm.contains(&g.esp_id)
                    {
                        if armed_targets.contains(&g.esp_id) {
                            recreated.insert(g.start_id.clone());
                        }
                        to_arm.push(g.esp_id.clone());
                    }
                }
            }
            let esps: Vec<ArmableEsp> = target_esps
                .into_iter()
                .filter(|g| to_arm.contains(&g.esp_id))
                .collect();
            if esps.is_empty() {
                continue;
            }
            let vars = self.variables_for_element(instance_key, scope_eik);
            let fail_ids = (
                source_scope.unwrap_or_else(|| source.id.clone()),
                target_scope.unwrap_or_else(|| target.id.clone()),
            );
            let events = self
                .arm_event_subprocess_triggers(instance_key, scope_eik, esps, &vars)
                .map_err(|f| (fail_ids.0.clone(), fail_ids.1.clone(), f))?;
            for event in events {
                if let Some(trigger) = Self::report_of(&event) {
                    let start_id = match &event {
                        Event::TimerCreated { element_id, .. }
                        | Event::MessageSubscriptionCreated { element_id, .. }
                        | Event::MessageSubscriptionOpening { element_id, .. }
                        | Event::SignalSubscriptionCreated { element_id, .. } => element_id.clone(),
                        _ => String::new(),
                    };
                    let source_had = recreated.contains(&start_id);
                    plan.subscriptions.push(MigratedSubscription {
                        element_instance_key: scope_eik,
                        element_id: start_id,
                        action: if source_had {
                            SubscriptionMigrationAction::Recreated
                        } else {
                            SubscriptionMigrationAction::Created
                        },
                        trigger,
                    });
                }
                plan.after.push(event);
            }
        }
        Ok(())
    }

    /// Whether scope `scope_eik`'s activation body will still (re-)run — the
    /// state in which its event sub-process triggers have NOT all been armed
    /// yet, so an unarmed one must not be read as consumed (#1379 review).
    ///
    /// Unlike [`Engine::hosts_not_armable_now`] (which gates ARMING new
    /// triggers and so also excludes completion-pending hosts), this predicate
    /// gates CONSUMED detection only: a completion-pending scope (an
    /// end-listener chain or a completion-phase incident) already ran its
    /// activation body and armed its triggers, so an unarmed subscription
    /// trigger there fired and IS consumed — excluding it would re-arm the
    /// fired trigger on a scope move and run its handler twice.
    ///
    /// A scope is activation-pending while
    ///
    /// * a **start** execution-listener job is outstanding on the scope element
    ///   itself (the chain runs before the activation body), or
    /// * an active **activation-phase** incident parks the scope element (its
    ///   redrive re-runs the activation body — a legacy no-redrive ioMapping
    ///   incident also replays activation), or
    /// * it is the process root whose start event is still activation-pending
    ///   ([`Engine::root_pending`]).
    fn activation_pending(
        &self,
        instance_key: Key,
        scope_eik: Key,
        source: &ProcessDefinition,
        active: &[(Key, ElementId)],
    ) -> bool {
        if self.root_pending(instance_key, scope_eik, source, active) {
            return true;
        }
        if scope_eik == 0 {
            // The root's own activation is its start event's (covered by
            // `root_pending` above); it carries no listeners or incidents of
            // its own.
            return false;
        }
        let start_listener_pending = self.state.jobs.values().any(|j| {
            j.element_instance_key == scope_eik
                && matches!(
                    j.kind,
                    state::JobKind::ExecutionListener {
                        event_type: crate::model::ListenerEventType::Start,
                        ..
                    }
                )
                && matches!(
                    j.state,
                    state::JobState::Created | state::JobState::Activated | state::JobState::Failed
                )
        });
        if start_listener_pending {
            return true;
        }
        self.state.incidents.values().any(|inc| {
            inc.element_instance_key == scope_eik
                && inc.state == state::IncidentState::Active
                && match &inc.redrive {
                    // A legacy no-redrive ioMapping incident replays activation.
                    None => inc.kind == state::IncidentKind::IoMapping,
                    Some(redrive) => redrive.redrives_activation(),
                }
        })
    }

    /// Whether scope `scope_eik` is the process root and its arming is still
    /// pending: the root arms its event sub-processes in the process start
    /// event's activation body, so while that start event's activation will
    /// still (re-)run the root is not steady — arming now would be repeated
    /// when the start event's body runs (#1379 review).
    ///
    /// The start event's activation re-runs in exactly two parked states:
    ///
    /// * a **start** execution-listener job is outstanding (the chain runs
    ///   before the body), or
    /// * an **activation-phase** incident parks it (its redrive re-runs the
    ///   activation body — see [`state::IoMappingRedrive::redrives_activation`];
    ///   a legacy no-redrive ioMapping incident also replays activation).
    ///
    /// A **completion-phase** incident (an output-mapping `Completion` failure,
    /// or an `end`-listener chain that failed to mint) does NOT make the root
    /// pending: resolving it resumes the start event's completion, never its
    /// activation, so the root is already steady and may be armed (#1379
    /// review r4579023810).
    fn root_pending(
        &self,
        instance_key: Key,
        scope_eik: Key,
        source: &ProcessDefinition,
        active: &[(Key, ElementId)],
    ) -> bool {
        scope_eik == 0
            && active.iter().any(|(eik, id)| {
                self.scope_of(instance_key, *eik) == 0
                    && source.element(id).is_some_and(|e| e.kind.is_start_event())
                    && self.start_event_activation_pending(*eik)
            })
    }

    /// Whether element instance `eik` (a process-level start event) is parked
    /// in a state that will re-run its activation body: an outstanding **start**
    /// execution-listener job, or an active **activation-phase** incident.
    fn start_event_activation_pending(&self, eik: Key) -> bool {
        let start_listener_pending = self.state.jobs.values().any(|j| {
            j.element_instance_key == eik
                && matches!(
                    j.kind,
                    state::JobKind::ExecutionListener {
                        event_type: crate::model::ListenerEventType::Start,
                        ..
                    }
                )
                && matches!(
                    j.state,
                    state::JobState::Created | state::JobState::Activated | state::JobState::Failed
                )
        });
        if start_listener_pending {
            return true;
        }
        self.state.incidents.values().any(|inc| {
            inc.element_instance_key == eik
                && inc.state == state::IncidentState::Active
                && match &inc.redrive {
                    // A legacy no-redrive ioMapping incident replays activation.
                    None => inc.kind == state::IncidentKind::IoMapping,
                    Some(redrive) => redrive.redrives_activation(),
                }
        })
    }

    /// The `hosts` NOT in their steady active phase (computed in one pass), i.e. a
    /// host is steady when its
    /// activation body has run (boundaries armed) and it has not begun
    /// completing (boundaries cancelled). False while any execution-listener
    /// job (start chain not drained, end chain running — including a `Failed`
    /// listener job parked on an incident) is outstanding, or while an active
    /// ioMapping-style incident will re-drive its activation or completion.
    fn hosts_not_armable_now(&self, hosts: &BTreeMap<Key, ElementId>) -> BTreeSet<Key> {
        let mut blocked: BTreeSet<Key> = self
            .state
            .jobs
            .values()
            .filter(|j| {
                hosts.contains_key(&j.element_instance_key)
                    && matches!(
                        j.state,
                        state::JobState::Created
                            | state::JobState::Activated
                            | state::JobState::Failed
                    )
                    && matches!(j.kind, state::JobKind::ExecutionListener { .. })
            })
            .map(|j| j.element_instance_key)
            .collect();
        blocked.extend(
            self.state
                .incidents
                .values()
                .filter(|inc| {
                    hosts.contains_key(&inc.element_instance_key)
                        && inc.state == state::IncidentState::Active
                        && inc.job_key.is_none()
                        && match &inc.redrive {
                            // Boundaries were armed on the first pass of a call
                            // activity spawn; the host is otherwise steady.
                            Some(state::IoMappingRedrive::CallActivitySpawn)
                            | Some(state::IoMappingRedrive::AdHocCallActivitySpawn { .. }) => false,
                            Some(_) => true,
                            // Legacy/no-redrive ioMapping incidents replay activation.
                            None => inc.kind == state::IncidentKind::IoMapping,
                        }
                })
                .map(|inc| inc.element_instance_key),
        );
        blocked
    }
}

/// The BPMN event type of a boundary event kind (Zeebe `BpmnEventType`), or
/// `None` for a non-boundary kind.
fn boundary_event_type(kind: &ElementKind) -> Option<&'static str> {
    Some(match kind {
        ElementKind::ErrorBoundaryEvent { .. } => "ERROR",
        ElementKind::TimerBoundaryEvent { .. } => "TIMER",
        ElementKind::MessageBoundaryEvent { .. } => "MESSAGE",
        ElementKind::SignalBoundaryEvent { .. } => "SIGNAL",
        ElementKind::ConditionalBoundaryEvent { .. } => "CONDITIONAL",
        ElementKind::EscalationBoundaryEvent { .. } => "ESCALATION",
        ElementKind::CompensationBoundaryEvent { .. } => "COMPENSATION",
        _ => return None,
    })
}

/// The target boundary's event type when a compensation boundary event is
/// mapped onto a boundary event of a different type (#1631, Zeebe
/// `ERROR_BOUNDARY_HAS_WRONG_EVENT_TYPE`); `None` otherwise.
pub(crate) fn compensation_boundary_type_change(
    source: &MigrationNode<'_>,
    target: &MigrationNode<'_>,
) -> Option<&'static str> {
    match (source, target) {
        (MigrationNode::Element(s), MigrationNode::Element(t))
            if matches!(s.kind, ElementKind::CompensationBoundaryEvent { .. }) =>
        {
            boundary_event_type(&t.kind).filter(|ty| *ty != "COMPENSATION")
        }
        _ => None,
    }
}

/// The compensation boundary events attached to `activity_id` (sorted by id).
fn compensation_boundaries_of<'a>(
    def: &'a ProcessDefinition,
    activity_id: &str,
) -> Vec<&'a Element> {
    let mut boundaries: Vec<&Element> = def
        .elements
        .values()
        .filter(|e| {
            matches!(&e.kind, ElementKind::CompensationBoundaryEvent { attached_to, .. }
                if attached_to == activity_id)
        })
        .collect();
    boundaries.sort_by(|a, b| a.id.cmp(&b.id));
    boundaries
}

/// Plans the migration of an instance's compensation subscriptions (#1631,
/// Zeebe `ProcessInstanceMigrationCatchEventBehavior#handleCompensationCatchEvents`).
///
/// For every completed compensable task whose compensation boundary event is
/// mapped, the task maps onto the activity the target boundary is attached to,
/// and each enclosing flow scope maps level by level onto the target's (so the
/// #901 scope subscriptions of completed sub-processes follow). A level where
/// one side reaches the process root before the other is rejected
/// ([`EngineError::MigrationCompensationFlowScopeChanged`]). Subscriptions not
/// covered by the derived mapping are deleted by the applier.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_compensation_migration(
    instance_key: Key,
    source_process_id: &str,
    target_process_id: &str,
    source_def: &ProcessDefinition,
    target_def: &ProcessDefinition,
    mapped: &BTreeMap<String, String>,
    compensable: &[state::CompensationSubscription],
) -> Result<Vec<crate::event::MigratedCompensationSubscription>, EngineError> {
    // Boundary subscriptions are planned per boundary occurrence: every
    // compensation boundary of an activity records its own subscription (its
    // own handler), so each follows only its OWN boundary's mapping and takes
    // that mapped boundary's handler. Duplicate `(instance, handler)` pairs
    // (several boundaries sharing one handler) pair up with those boundaries
    // in boundary-id order. Explicit sub-process subscriptions are boundary
    // subscriptions too.
    let mut scope_map: BTreeMap<ElementId, ElementId> = BTreeMap::new();
    let mut out = Vec::new();
    let mut occurrence: BTreeMap<(Key, &str), usize> = BTreeMap::new();
    // Loops / multi-instance bodies accumulate many subscriptions per
    // activity: scan the model once per source activity and validate each
    // (source activity, target activity) pair once.
    let mut boundaries_by_activity: BTreeMap<&str, Vec<&Element>> = BTreeMap::new();
    let mut validated: BTreeSet<(&str, String)> = BTreeSet::new();
    for sub in compensable.iter().filter(|c| !c.scope_subscription) {
        let occ = occurrence
            .entry((sub.element_instance_key, sub.handler.as_str()))
            .or_insert(0);
        let index = *occ;
        *occ += 1;
        // A subscription that recorded its boundary (#1635) resolves it by
        // identity — stable across successive migrations; an older record
        // falls back to boundary-id occurrence order among same-handler
        // boundaries.
        let source_boundaries = boundaries_by_activity
            .entry(sub.element_id.as_str())
            .or_insert_with(|| compensation_boundaries_of(source_def, &sub.element_id));
        let source_boundary = if sub.boundary_id.is_empty() {
            source_boundaries
                .iter()
                .copied()
                .filter(|b| {
                    matches!(&b.kind, ElementKind::CompensationBoundaryEvent { handler, .. }
                        if *handler == sub.handler)
                })
                .nth(index)
        } else {
            source_boundaries
                .iter()
                .copied()
                .find(|b| b.id == sub.boundary_id)
        };
        let Some(source_boundary) = source_boundary else {
            continue;
        };
        let Some(target_boundary) = mapped
            .get(&source_boundary.id)
            .and_then(|tb| target_def.element(tb))
        else {
            continue;
        };
        let ElementKind::CompensationBoundaryEvent {
            attached_to: target_activity,
            handler: target_handler,
        } = &target_boundary.kind
        else {
            continue;
        };
        // The completed activity follows its mapped boundary, so it gets the
        // same catch-event preconditions as an active element (#1635): an
        // explicit activity mapping must agree with the boundary's
        // attachment, and no two source boundaries may merge onto one target
        // boundary (that would run its handler twice). Validated against
        // DEFINITION boundary ids, so repeated occurrences of one source
        // boundary (multi-instance bodies, loops) stay allowed.
        let activity_target = mapped
            .get(&sub.element_id)
            .map_or(target_activity.as_str(), String::as_str);
        if validated.insert((sub.element_id.as_str(), activity_target.to_string())) {
            Engine::check_migrated_catch_events(
                instance_key,
                source_def,
                target_def,
                mapped,
                &sub.element_id,
                activity_target,
            )?;
        }
        out.push(crate::event::MigratedCompensationSubscription {
            source_element_id: sub.element_id.clone(),
            source_element_instance_key: sub.element_instance_key,
            source_handler: sub.handler.clone(),
            source_boundary_id: sub.boundary_id.clone(),
            target_element_id: target_activity.clone(),
            target_handler: target_handler.clone(),
            target_boundary_id: target_boundary.id.clone(),
        });
        let source_chain = super::flow_scope::element_flow_scope_chain(source_def, &sub.element_id);
        let target_chain = super::flow_scope::element_flow_scope_chain(target_def, target_activity);
        for level in 0..source_chain.len().max(target_chain.len()) {
            match (source_chain.get(level), target_chain.get(level)) {
                (Some(s), Some(t)) => {
                    // A derived scope mapping must agree with an explicit
                    // mapping instruction for that scope and with every
                    // earlier derivation: otherwise a scope subscription (whose
                    // numeric scope key is preserved) would run its nested
                    // handlers under the wrong target scope.
                    let conflicts = mapped.get(s).is_some_and(|m| m != t)
                        || scope_map.get(s).is_some_and(|prev| prev != t);
                    if conflicts {
                        return Err(EngineError::MigrationCompensationFlowScopeChanged {
                            instance_key,
                            process_id: source_process_id.to_string(),
                            source_flow_scope_id: s.clone(),
                            target_flow_scope_id: t.clone(),
                        });
                    }
                    scope_map.insert(s.clone(), t.clone());
                }
                (s, t) => {
                    return Err(EngineError::MigrationCompensationFlowScopeChanged {
                        instance_key,
                        process_id: source_process_id.to_string(),
                        source_flow_scope_id: s
                            .cloned()
                            .unwrap_or_else(|| source_process_id.to_string()),
                        target_flow_scope_id: t
                            .cloned()
                            .unwrap_or_else(|| target_process_id.to_string()),
                    })
                }
            }
        }
    }
    // Handlerless scope subscriptions (#901) follow the flow scope of a
    // migrated nested subscription.
    for sub in compensable.iter().filter(|c| c.scope_subscription) {
        if let Some(target) = scope_map.get(&sub.element_id) {
            out.push(crate::event::MigratedCompensationSubscription {
                source_element_id: sub.element_id.clone(),
                source_element_instance_key: sub.element_instance_key,
                source_handler: sub.handler.clone(),
                source_boundary_id: String::new(),
                target_element_id: target.clone(),
                target_handler: sub.handler.clone(),
                target_boundary_id: String::new(),
            });
        }
    }
    Ok(out)
}
