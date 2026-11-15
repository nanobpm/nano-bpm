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
        mapped: &HashMap<String, String>,
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

    /// Event-based gateway race preconditions (#1554, Zeebe migrates an active
    /// event-based gateway together with its armed catch events). The engine
    /// models the gateway as a pass-through whose catch events are each an
    /// active element instance, so a resting race is a set of active catch
    /// events sharing one owning gateway ([`event_gateway_race_index`] — the
    /// same race definition the runtime withdrawal uses). Per source race:
    ///
    /// * every mapped member must be a race member of one target event-based
    ///   gateway — the mapped target of the source gateway when that is mapped
    ///   — otherwise [`EngineError::MigrationCatchEventDetached`];
    /// * no two members may map onto the same target catch event
    ///   ([`EngineError::MigrationCatchEventsMerged`]);
    /// * an unmapped member is withdrawn (its subscriptions deleted, as Zeebe
    ///   deletes an unmapped catch event's subscription) provided at least one
    ///   sibling is mapped; a race with no mapped member is left for the
    ///   unmapped-active-element rejection.
    ///
    /// A race is identified by its owning gateway **and** its token scope
    /// (`scope_of`), exactly as the runtime withdrawal scopes a race, so two
    /// live subprocess scopes resting at the same gateway are two races. Two
    /// distinct source races in one scope must not land on the same target
    /// gateway: the runtime would then treat them as one race and a win in
    /// either would withdraw the other's catches
    /// ([`EngineError::UnsupportedMigration`]).
    ///
    /// Returns the active `(element instance key, element id)`s to withdraw.
    pub(crate) fn check_migrated_event_gateway_races(
        instance_key: Key,
        source: &ProcessDefinition,
        target: &ProcessDefinition,
        mapped: &HashMap<String, String>,
        active: &[(Key, String)],
        scope_of: impl Fn(Key) -> Key,
    ) -> Result<Vec<(Key, String)>, EngineError> {
        // One indexed pass per definition, shared by every ownership lookup in
        // this check (O(V + E) each, not a full rescan per active element).
        let source_races = event_gateway_race_index(source);
        let target_races = event_gateway_race_index(target);
        let mut races: BTreeMap<(Key, &str), Vec<(Key, &str)>> = BTreeMap::new();
        for (eik, eid) in active {
            if let Some(owner) = source_races.owner_of(eid) {
                races
                    .entry((scope_of(*eik), owner.id.as_str()))
                    .or_default()
                    .push((*eik, eid.as_str()));
            }
        }
        // (scope, target gateway) -> the live origin that claimed it: a source
        // race (its gateway id), a standalone catch, or a gateway paused before
        // arming (its element id). Two origins in one scope must never land on
        // one target race, or a win in either withdraws the other's catches.
        let mut claimed: BTreeMap<(Key, String), String> = BTreeMap::new();
        let claim = |claimed: &mut BTreeMap<(Key, String), String>,
                     scope: Key,
                     tg: String,
                     origin: &str|
         -> Result<(), EngineError> {
            if let Some(other) = claimed.get(&(scope, tg.clone())) {
                return Err(EngineError::UnsupportedMigration {
                    instance_key,
                    element_id: origin.to_string(),
                    reason: format!(
                        "`{other}` and `{origin}` in one scope would merge into the \
                         single target event-based gateway race `{tg}`"
                    ),
                });
            }
            claimed.insert((scope, tg), origin.to_string());
            Ok(())
        };
        let mut withdraw = Vec::new();
        for ((scope, gateway), members) in races {
            let mapped_members: Vec<(&str, &String)> = members
                .iter()
                .filter_map(|(_, eid)| mapped.get(*eid).map(|t| (*eid, t)))
                .collect();
            let Some((_, first_target)) = mapped_members.first() else {
                continue;
            };
            let target_gateway = mapped
                .get(gateway)
                .cloned()
                .or_else(|| target_races.owner_of(first_target).map(|o| o.id.clone()));
            let mut by_target: BTreeMap<&str, Vec<String>> = BTreeMap::new();
            for (src, tgt) in &mapped_members {
                let owner = target_races.owner_of(tgt).map(|o| o.id.as_str());
                if owner.is_none() || owner != target_gateway.as_deref() {
                    return Err(EngineError::MigrationCatchEventDetached {
                        instance_key,
                        element_id: gateway.to_string(),
                        target_element_id: target_gateway
                            .clone()
                            .unwrap_or_else(|| gateway.to_string()),
                        catch_event_id: src.to_string(),
                        target_catch_event_id: tgt.to_string(),
                    });
                }
                by_target
                    .entry(tgt.as_str())
                    .or_default()
                    .push(src.to_string());
            }
            if let Some((tgt, mut sources)) = by_target.into_iter().find(|(_, s)| s.len() > 1) {
                sources.sort();
                return Err(EngineError::MigrationCatchEventsMerged {
                    instance_key,
                    element_id: gateway.to_string(),
                    target_catch_event_id: tgt.to_string(),
                    source_catch_event_ids: sources,
                });
            }
            if let Some(tg) = target_gateway {
                claim(&mut claimed, scope, tg, gateway)?;
            }
            withdraw.extend(
                members
                    .iter()
                    .filter(|(_, eid)| !mapped.contains_key(*eid))
                    .map(|(k, eid)| (*k, eid.to_string())),
            );
        }
        // Live origins outside any source race that still land in a target
        // race: a gateway paused before arming its catches (start listener /
        // incident) mapped onto a target gateway, or a standalone catch mapped
        // onto a target race member.
        for (eik, eid) in active {
            if source_races.owner_of(eid).is_some() {
                continue;
            }
            let Some(tgt) = mapped.get(eid) else {
                continue;
            };
            let source_is_gateway = matches!(
                source.element(eid).map(|e| &e.kind),
                Some(ElementKind::EventBasedGateway)
            );
            let tg = if source_is_gateway {
                target
                    .element(tgt)
                    .filter(|e| matches!(e.kind, ElementKind::EventBasedGateway))
                    .map(|e| e.id.clone())
            } else {
                target_races.owner_of(tgt).map(|o| o.id.clone())
            };
            if let Some(tg) = tg {
                claim(&mut claimed, scope_of(*eik), tg, eid)?;
            }
        }
        Ok(withdraw)
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
        mapped: &HashMap<String, String>,
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
        mapped: &HashMap<String, String>,
    ) -> Result<MigrationPlan, (ElementId, ElementId, expression::Failure)> {
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
        let call_eiks: std::collections::HashSet<Key> = active
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
                // An unmapped host still active here is a withdrawn race member
                // (#1554; every other unmapped host was rejected): its withdrawal
                // cancels the triggers, the migration fact reports them deleted.
                for armed in armed_by_host.remove(&eik).unwrap_or_default() {
                    if let Some(trigger) = armed.report {
                        plan.subscriptions.push(MigratedSubscription {
                            element_instance_key: eik,
                            element_id: armed.catch_id,
                            action: SubscriptionMigrationAction::Deleted,
                            trigger,
                        });
                    }
                }
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
        mapped: &HashMap<String, String>,
        active: &[(Key, ElementId)],
        not_armable: &HashSet<Key>,
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
            if let Some(target_id) = mapped.get(source_id) {
                scopes.push((*eik, Some(source_id.clone()), Some(target_id.clone())));
            }
        }
        let armed = self.armed_event_subprocess_triggers(instance_key);
        let counterpart = |id: &ElementId| mapped.get(id).cloned().unwrap_or_else(|| id.clone());
        // Target event sub-process ids accounted for by a CONSUMED source
        // counterpart: a source event sub-process (in ANY live scope) that
        // carries a subscription trigger but is no longer armed (a one-shot
        // timer that already fired). Such a counterpart must never be
        // (re-)armed — its handler already ran. This set is GLOBAL across
        // scopes so a consumed source event sub-process that the migration
        // moves into a different (already-live) scope still suppresses its
        // target counterpart: a per-scope `covered` set would lose it the
        // moment it crosses a scope boundary and arm it again, running its
        // handler twice (#1379 review).
        let armed_source_esp_ids: HashSet<&str> = armed.iter().map(|t| t.esp_id.as_str()).collect();
        let mut consumed: HashSet<ElementId> = HashSet::new();
        for (_, source_scope, _) in &scopes {
            for e in ArmableEsp::in_scope(source, source_scope.as_deref()) {
                if e.trigger.is_subscription() && !armed_source_esp_ids.contains(e.esp_id.as_str())
                {
                    consumed.insert(counterpart(&e.esp_id));
                }
            }
        }
        for (scope_eik, source_scope, target_scope) in scopes {
            let source_esps = ArmableEsp::in_scope(source, source_scope.as_deref());
            let target_esps = ArmableEsp::in_scope(target, target_scope.as_deref());
            // Target event sub-processes a source counterpart accounts for:
            // its armed trigger (if any) is retained or re-created above, and
            // one with NO armed trigger (a fired one-shot timer) must not be
            // re-armed. Error / escalation triggers are never subscriptions —
            // they have nothing to retain or re-create — so they cover
            // nothing: a target that turns one into a subscription trigger is
            // newly armed below (#1379 review).
            let covered: HashSet<ElementId> = source_esps
                .iter()
                .filter(|e| e.trigger.is_subscription())
                .map(|e| counterpart(&e.esp_id))
                .collect();
            let mut to_arm: Vec<ElementId> = Vec::new();
            // Target event sub-processes re-armed in place of a deleted source
            // trigger (vs newly created ones), by start-event id.
            let mut recreated: HashSet<ElementId> = HashSet::new();
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
                    if !covered.contains(&g.esp_id)
                        && !consumed.contains(&g.esp_id)
                        && !to_arm.contains(&g.esp_id)
                    {
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
