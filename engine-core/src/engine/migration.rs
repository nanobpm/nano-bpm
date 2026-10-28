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
    MigratedElementInstance, MigratedSubscription, MigratedSubscriptionTrigger,
    SubscriptionMigrationAction,
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
}

/// `Some(interrupting)` when `kind` is a boundary event.
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
    /// Zeebe catch-event preconditions for one active element `source_id`
    /// mapped onto `target_id`: every mapped boundary of the source must target
    /// a boundary attached to `target_id` (stay attached), and no two may
    /// target the same catch event (no merging).
    pub(crate) fn check_migrated_catch_events(
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &HashMap<String, String>,
        source_id: &str,
        target_id: &str,
    ) -> Result<(), EngineError> {
        let mut boundaries: Vec<&Element> = source
            .elements
            .values()
            .filter(|e| e.kind.attached_to() == Some(source_id))
            .collect();
        boundaries.sort_by(|a, b| a.id.cmp(&b.id));
        let mut by_target: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for b in boundaries {
            let Some(tb) = mapped.get(&b.id) else {
                continue;
            };
            let attached = target
                .element(tb)
                .and_then(|e| e.kind.attached_to())
                .is_some_and(|a| a == target_id);
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
        hosts: &HashMap<Key, ElementId>,
        source: &ProcessDefinition,
    ) -> HashMap<Key, Vec<ArmedTrigger>> {
        let mut by_host: HashMap<Key, Vec<ArmedTrigger>> = HashMap::new();
        for t in self.state.timers.values() {
            let Some(host_element_id) = hosts.get(&t.element_instance_key) else {
                continue;
            };
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
                state::TimerKind::IntermediateCatch => (host_element_id.clone(), None),
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
        let catch_of = |kind: &state::MessageSubscriptionKind, host_element_id: &ElementId| {
            match boundary_id_of(kind) {
                Some((id, interrupting)) => Some((id, Some(interrupting))),
                None if *kind == state::MessageSubscriptionKind::IntermediateCatch => {
                    Some((host_element_id.clone(), None))
                }
                None => None,
            }
        };
        for s in self.state.message_subscriptions.values() {
            let Some(host_element_id) = hosts.get(&s.element_instance_key) else {
                continue;
            };
            if !matches!(
                s.state,
                state::MessageSubscriptionState::Open | state::MessageSubscriptionState::Opening
            ) {
                continue;
            }
            let Some((catch_id, interrupting)) = catch_of(&s.kind, host_element_id) else {
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
            let Some(host_element_id) = hosts.get(&s.element_instance_key) else {
                continue;
            };
            if s.state != state::MessageSubscriptionState::Open {
                continue;
            }
            let Some((catch_id, interrupting)) = catch_of(&s.kind, host_element_id) else {
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
            let Some(host_element_id) = hosts.get(&s.element_instance_key) else {
                continue;
            };
            if s.state != state::MessageSubscriptionState::Open {
                continue;
            }
            let Some((catch_id, interrupting)) = catch_of(&s.kind, host_element_id) else {
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
        vars: &HashMap<String, Value>,
        target: &Element,
    ) -> Option<TriggerFingerprint> {
        match &target.kind {
            ElementKind::MessageBoundaryEvent {
                message_name,
                correlation_key,
                ..
            }
            | ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            } => Some(TriggerFingerprint::Message(
                self.resolve_event_name(vars, message_name),
                self.resolve_correlation_value(vars, correlation_key),
            )),
            ElementKind::SignalBoundaryEvent { signal_name, .. }
            | ElementKind::SignalIntermediateCatchEvent { signal_name } => Some(
                TriggerFingerprint::Signal(self.resolve_event_name(vars, signal_name)),
            ),
            ElementKind::ConditionalBoundaryEvent { condition, .. }
            | ElementKind::ConditionalIntermediateCatchEvent { condition } => {
                Some(TriggerFingerprint::Conditional(condition.clone()))
            }
            _ => timer_fingerprint(target),
        }
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
    pub(crate) fn plan_migration(
        &mut self,
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &HashMap<String, String>,
    ) -> MigrationPlan {
        let mut plan = MigrationPlan::default();
        let mut active: Vec<(Key, ElementId)> = self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.active.iter().map(|(k, e)| (*k, e.clone())).collect())
            .unwrap_or_default();
        active.sort();
        // Index triggers and lifecycle blockers once per migration so the
        // per-element loop below is a lookup, not a partition-wide scan.
        let hosts: HashMap<Key, ElementId> = active.iter().cloned().collect();
        let mut armed_by_host = self.armed_triggers_on(&hosts, source);
        let not_armable = self.hosts_not_armable_now(&hosts);
        for (eik, source_id) in active {
            let Some(target_id) = mapped.get(&source_id).cloned() else {
                continue;
            };
            if matches!(
                source.element(&source_id).map(|e| &e.kind),
                Some(ElementKind::SubProcess { .. })
            ) {
                plan.migrated_flow_scopes.push(MigratedElementInstance {
                    element_instance_key: eik,
                    source_element_id: source_id.clone(),
                    target_element_id: target_id.clone(),
                });
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
            let mut covered: Vec<ElementId> = Self::armable_boundaries(source, &source_id)
                .iter()
                .filter_map(|b| mapped.get(&b.id).cloned())
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
                let unchanged = self.target_fingerprint(vars, &target_catch).as_ref()
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
                        .into_iter()
                        .collect()
                } else {
                    self.arm_catch_event(instance_key, eik, vars, &target_catch)
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
            // Target boundaries with no mapped source boundary are newly armed.
            for boundary in Self::armable_boundaries(target, &target_id) {
                if covered.contains(&boundary.id) {
                    continue;
                }
                if let Some(event) =
                    self.arm_boundary(instance_key, eik, &target_id, &boundary_vars, &boundary)
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
        plan
    }

    /// The `hosts` NOT in their steady active phase (computed in one pass), i.e. a
    /// host is steady when its
    /// activation body has run (boundaries armed) and it has not begun
    /// completing (boundaries cancelled). False while any execution-listener
    /// job (start chain not drained, end chain running — including a `Failed`
    /// listener job parked on an incident) is outstanding, or while an active
    /// ioMapping-style incident will re-drive its activation or completion.
    fn hosts_not_armable_now(&self, hosts: &HashMap<Key, ElementId>) -> HashSet<Key> {
        let mut blocked: HashSet<Key> = self
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
