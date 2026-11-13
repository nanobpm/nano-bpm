//! Event sub-process runtime (#1555, Zeebe `EventSubProcessProcessor` parity).
//!
//! An event sub-process (`subProcess triggeredByEvent="true"`) is never
//! reached by sequence flow. Instead its start-event trigger is **armed** when
//! its enclosing scope — the process instance (scope `0`) or an embedded
//! sub-process instance — activates, and **disarmed** when that scope
//! completes or terminates. The trigger is recorded as a timer / message /
//! signal / conditional subscription of kind
//! [`state::TimerKind::EventSubProcess`] /
//! [`state::MessageSubscriptionKind::EventSubProcess`] whose
//! `element_instance_key` is the enclosing **scope** (not a token) and whose
//! `element_id` is the event sub-process's start event.
//!
//! When the trigger fires ([`Engine::trigger_event_subprocess`]):
//!
//! * **interrupting** — every other trigger armed on the scope is disarmed and
//!   every other element instance in the scope is torn down; the event
//!   sub-process then activates alone in the scope, which completes when it
//!   completes.
//! * **non-interrupting** — the event sub-process activates concurrently
//!   alongside the scope's other tokens; its trigger stays armed (a message /
//!   signal / conditional subscription is not settled, a cycle timer re-arms),
//!   so it can fire again. The scope cannot complete while an event
//!   sub-process instance is still running in it (it is an ordinary child
//!   token of the scope).
//!
//! The event payload (message / signal variables) propagates from the scope
//! upward, exactly like a boundary event's payload.
//!
//! Error and escalation triggers are **not** armed here: they are not
//! subscriptions but are caught by throw-resolution — the single
//! `resolve_throw_catch` walk in `boundary.rs` (Zeebe `CatchEventAnalyzer`
//! parity), which weighs a scope's hosted event sub-processes against the
//! boundary events attached to it.

use std::collections::{HashMap, VecDeque};

use super::{expression, Engine, Step};
use crate::{
    model::{ElementKind, EventSubProcessTrigger, TimerDef},
    state, Event, Key, Value,
};

/// One armable event sub-process of a scope, cloned out of the definition so
/// the engine can be mutably borrowed while arming.
struct ArmableEsp {
    esp_id: String,
    start_id: String,
    trigger: EventSubProcessTrigger,
    interrupting: bool,
}

impl Engine {
    /// The event sub-processes directly contained in scope element
    /// `scope_element` (`None` = the process root) of `instance_key`'s
    /// definition.
    fn armable_event_subprocesses(
        &self,
        instance_key: Key,
        scope_element: Option<&str>,
    ) -> Vec<ArmableEsp> {
        self.process_of_instance(instance_key)
            .map(|p| {
                p.event_subprocesses_in_scope(scope_element)
                    .map(|(esp, start, trigger, interrupting)| ArmableEsp {
                        esp_id: esp.clone(),
                        start_id: start.clone(),
                        trigger: trigger.clone(),
                        interrupting,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Arms the start-event triggers of every event sub-process directly
    /// contained in the scope `scope_eik` (element `scope_element`; `0` /
    /// `None` for the process root), evaluating their expressions (timer,
    /// message name, correlation key) against `vars` — the scope's
    /// **post-input** view. The caller passes the view its own activation body
    /// evaluates against: the scope's input mappings / multi-instance item
    /// bindings are journaled but not yet applied when arming runs, so reading
    /// persisted variables here would miss them. Returns
    /// the creation events; an expression failure is returned for the caller
    /// to raise as an incident on the activating element (minted keys are the
    /// caller's to rewind, as with boundary arming).
    pub(super) fn arm_event_subprocesses(
        &mut self,
        instance_key: Key,
        scope_eik: Key,
        scope_element: Option<&str>,
        vars: &HashMap<String, Value>,
    ) -> Result<Vec<Event>, expression::Failure> {
        let esps = self.armable_event_subprocesses(instance_key, scope_element);
        if esps.is_empty() {
            return Ok(Vec::new());
        }
        let mut events = Vec::new();
        for esp in esps {
            let kind = state::MessageSubscriptionKind::EventSubProcess {
                esp_element_id: esp.esp_id.clone(),
                interrupting: esp.interrupting,
            };
            match &esp.trigger {
                EventSubProcessTrigger::Timer(def) => {
                    let (due_at, _) = self.resolve_timer(vars, Some(def), self.now, 0)?;
                    let timer_key = self.mint_key();
                    events.push(Event::TimerCreated {
                        timer_key,
                        instance_key,
                        element_instance_key: scope_eik,
                        element_id: esp.start_id.clone(),
                        due_at,
                        kind: state::TimerKind::EventSubProcess {
                            esp_element_id: esp.esp_id.clone(),
                            interrupting: esp.interrupting,
                        },
                    });
                }
                EventSubProcessTrigger::Message {
                    message_name,
                    correlation_key,
                } => {
                    let message_name = self.resolve_event_name(vars, message_name)?;
                    let correlation_value =
                        self.resolve_correlation_value(vars, correlation_key)?;
                    let subscription_key = self.mint_key();
                    let element_id = esp.start_id.clone();
                    events.push(
                        if self.subscription_partition(&correlation_value) == self.partition_id {
                            Event::MessageSubscriptionCreated {
                                subscription_key,
                                instance_key,
                                element_instance_key: scope_eik,
                                element_id,
                                message_name,
                                correlation_key: correlation_value,
                                kind,
                            }
                        } else {
                            Event::MessageSubscriptionOpening {
                                subscription_key,
                                instance_key,
                                element_instance_key: scope_eik,
                                element_id,
                                message_name,
                                correlation_key: correlation_value,
                                kind,
                            }
                        },
                    );
                }
                EventSubProcessTrigger::Signal { signal_name } => {
                    let signal_name = self.resolve_event_name(vars, signal_name)?;
                    let subscription_key = self.mint_key();
                    events.push(Event::SignalSubscriptionCreated {
                        subscription_key,
                        instance_key,
                        element_instance_key: scope_eik,
                        element_id: esp.start_id.clone(),
                        signal_name,
                        kind,
                    });
                }
                EventSubProcessTrigger::Conditional { condition } => {
                    let subscription_key = self.mint_key();
                    events.push(Event::ConditionalSubscriptionCreated {
                        subscription_key,
                        instance_key,
                        element_instance_key: scope_eik,
                        element_id: esp.start_id.clone(),
                        condition: condition.clone(),
                        referenced_vars: super::sorted_referenced_vars(condition),
                        kind,
                    });
                }
                // Error and escalation event sub-processes arm no subscription —
                // they are caught when an error / escalation is thrown inside
                // the scope, by `resolve_throw_catch` (boundary.rs).
                EventSubProcessTrigger::Error { .. }
                | EventSubProcessTrigger::Escalation { .. } => {}
            }
        }
        Ok(events)
    }

    /// Dry-runs the arming of the event sub-processes contained in scope
    /// element `scope_element` (`None` = the process root) against `vars`
    /// WITHOUT emitting anything (minted keys are rewound): the first
    /// expression failure, if any. The ESP analogue of
    /// [`Engine::check_boundary_arming`](super::Engine), so an all-or-nothing
    /// command (process-instance modification) rejects up front exactly when
    /// arming would fail — through the single arming implementation, not a
    /// parallel validator (#1398).
    pub(super) fn check_event_subprocess_arming(
        &mut self,
        instance_key: Key,
        scope_element: Option<&str>,
        vars: &HashMap<String, Value>,
    ) -> Result<(), expression::Failure> {
        let mark = self.next_local;
        let result = self
            .arm_event_subprocesses(instance_key, 0, scope_element, vars)
            .map(|_| ());
        self.next_local = mark;
        result
    }

    /// The events disarming every still-armed event sub-process trigger on the
    /// scope `scope_eik` of `instance_key` (sorted by key within each family).
    /// Emitted when the scope completes or terminates, and by an interrupting
    /// trigger to disarm the scope's other event sub-processes.
    pub(super) fn disarm_event_subprocess_events(
        &self,
        instance_key: Key,
        scope_eik: Key,
    ) -> Vec<Event> {
        let on_scope = |ik: Key, eik: Key| ik == instance_key && eik == scope_eik;
        let is_esp = |k: &state::MessageSubscriptionKind| {
            matches!(k, state::MessageSubscriptionKind::EventSubProcess { .. })
        };
        let mut events = Vec::new();

        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| {
                on_scope(t.instance_key, t.element_instance_key)
                    && t.state == state::TimerState::Created
                    && matches!(t.kind, state::TimerKind::EventSubProcess { .. })
            })
            .collect();
        timers.sort_by_key(|t| t.key);
        events.extend(timers.into_iter().map(|t| Event::TimerCanceled {
            timer_key: t.key,
            instance_key: t.instance_key,
            element_instance_key: t.element_instance_key,
            element_id: t.element_id.clone(),
        }));

        let mut messages: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                on_scope(s.instance_key, s.element_instance_key)
                    && matches!(
                        s.state,
                        state::MessageSubscriptionState::Open
                            | state::MessageSubscriptionState::Opening
                    )
                    && is_esp(&s.kind)
            })
            .collect();
        messages.sort_by_key(|s| s.key);
        events.extend(messages.into_iter().map(Self::disarm_subscription_event));

        let mut signals: Vec<&state::SignalSubscription> = self
            .state
            .signal_subscriptions
            .values()
            .filter(|s| {
                on_scope(s.instance_key, s.element_instance_key)
                    && s.state == state::MessageSubscriptionState::Open
                    && is_esp(&s.kind)
            })
            .collect();
        signals.sort_by_key(|s| s.key);
        events.extend(
            signals
                .into_iter()
                .map(|s| Event::SignalSubscriptionCanceled {
                    subscription_key: s.key,
                    instance_key: s.instance_key,
                    element_instance_key: s.element_instance_key,
                    element_id: s.element_id.clone(),
                }),
        );

        let mut conditionals: Vec<&state::ConditionalSubscription> = self
            .state
            .conditional_subscriptions
            .values()
            .filter(|s| {
                on_scope(s.instance_key, s.element_instance_key)
                    && s.state == state::MessageSubscriptionState::Open
                    && is_esp(&s.kind)
            })
            .collect();
        conditionals.sort_by_key(|s| s.key);
        events.extend(
            conditionals
                .into_iter()
                .map(|s| Event::ConditionalSubscriptionCanceled {
                    subscription_key: s.key,
                    instance_key: s.instance_key,
                    element_instance_key: s.element_instance_key,
                    element_id: s.element_id.clone(),
                }),
        );
        events
    }

    /// The event sub-process element ids (sorted, deduplicated) of every
    /// still-armed event sub-process trigger of `instance_key`, across all four
    /// trigger families. Migration rejects an instance with any (#1379).
    pub(super) fn armed_event_subprocess_ids(&self, instance_key: Key) -> Vec<String> {
        let esp_of = |k: &state::MessageSubscriptionKind| match k {
            state::MessageSubscriptionKind::EventSubProcess { esp_element_id, .. } => {
                Some(esp_element_id.clone())
            }
            _ => None,
        };
        let open = |st: state::MessageSubscriptionState| {
            matches!(
                st,
                state::MessageSubscriptionState::Open | state::MessageSubscriptionState::Opening
            )
        };
        let s = &self.state;
        let mut ids: Vec<String> = s
            .timers
            .values()
            .filter(|t| t.instance_key == instance_key && t.state == state::TimerState::Created)
            .filter_map(|t| match &t.kind {
                state::TimerKind::EventSubProcess { esp_element_id, .. } => {
                    Some(esp_element_id.clone())
                }
                _ => None,
            })
            .chain(
                s.message_subscriptions
                    .values()
                    .filter(|x| x.instance_key == instance_key && open(x.state))
                    .filter_map(|x| esp_of(&x.kind)),
            )
            .chain(
                s.signal_subscriptions
                    .values()
                    .filter(|x| x.instance_key == instance_key && open(x.state))
                    .filter_map(|x| esp_of(&x.kind)),
            )
            .chain(
                s.conditional_subscriptions
                    .values()
                    .filter(|x| x.instance_key == instance_key && open(x.state))
                    .filter_map(|x| esp_of(&x.kind)),
            )
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Every element instance inside scope `scope_eik` of `instance_key`
    /// (transitively). For the process root (`0`) that is every live element
    /// instance — root children carry no `scopes` entry, so
    /// [`Self::scope_descendants`] cannot enumerate them.
    fn event_subprocess_scope_members(&self, instance_key: Key, scope_eik: Key) -> Vec<Key> {
        if scope_eik != 0 {
            return self.scope_descendants(instance_key, scope_eik);
        }
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        let mut all: Vec<Key> = instance
            .active
            .keys()
            .chain(instance.parked_joins.iter())
            .chain(instance.scopes.keys())
            .copied()
            .collect();
        all.sort_unstable();
        all.dedup();
        all
    }

    /// Fires the event sub-process `esp_element_id` in scope `scope_eik` of
    /// `instance_key` (its trigger just correlated / became due). An
    /// interrupting trigger first disarms the scope's other event sub-process
    /// triggers and tears down every other element instance in the scope; the
    /// event sub-process is then activated in the scope.
    pub(super) fn trigger_event_subprocess(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        instance_key: Key,
        scope_eik: Key,
        esp_element_id: String,
        interrupting: bool,
    ) {
        if interrupting {
            for event in self.disarm_event_subprocess_events(instance_key, scope_eik) {
                self.emit(log, event);
            }
            let members = self.event_subprocess_scope_members(instance_key, scope_eik);
            for event in self.element_teardown_events(instance_key, &members) {
                self.emit(log, event);
            }
            // A still-queued activation inside a torn-down nested scope must
            // not recreate a token there (the per-drain dead-scope guard).
            self.torn_down_scopes.extend(members);
            // The host scope itself survives (the event sub-process runs in
            // it), so the dead-scope guard cannot reject work still queued
            // *directly* in it — e.g. a non-interrupting sibling event
            // sub-process triggered earlier in the same timer / conditional
            // batch, or a token mid-flow on a host-level sequence flow. Purge
            // those activations now (revoking any join arrival they already
            // counted), so the interrupting handler runs alone in the scope.
            let mut kept = VecDeque::with_capacity(queue.len());
            while let Some(step) = queue.pop_front() {
                let in_host = matches!(
                    &step,
                    Step::Activate { instance_key: ik, scope, .. }
                    | Step::ActivateSeededJoin { instance_key: ik, scope, .. }
                        if *ik == instance_key && *scope == scope_eik
                );
                if in_host {
                    for event in self.revoke_dropped_join_arrival(&step) {
                        self.emit(log, event);
                    }
                } else {
                    kept.push_back(step);
                }
            }
            *queue = kept;
        }
        queue.push_back(Step::Activate {
            instance_key,
            element_id: esp_element_id,
            scope: scope_eik,
            via: None,
        });
    }

    /// Re-arms a fired non-interrupting **cycle** timer of an event
    /// sub-process for its next interval (based at the previous due time),
    /// re-evaluating a FEEL cycle against the scope's current variables. A
    /// duration / date timer fires once and is not re-armed.
    pub(super) fn rearm_event_subprocess_timer(
        &mut self,
        instance_key: Key,
        scope_eik: Key,
        start_id: &str,
        esp_element_id: &str,
        previous_due_at: u64,
    ) -> Result<Option<Event>, expression::Failure> {
        let def: Option<TimerDef> = match self.element_kind(instance_key, esp_element_id) {
            Some(ElementKind::EventSubProcess {
                trigger: EventSubProcessTrigger::Timer(def),
                interrupting: false,
                ..
            }) if def.kind == crate::model::TimerDefKind::Cycle => Some(def),
            _ => None,
        };
        let Some(def) = def else {
            return Ok(None);
        };
        let vars = self.variables_for_element(instance_key, scope_eik);
        let (due_at, _) = self.resolve_timer(&vars, Some(&def), previous_due_at, 0)?;
        Ok(Some(Event::TimerCreated {
            timer_key: self.mint_key(),
            instance_key,
            element_instance_key: scope_eik,
            element_id: start_id.to_string(),
            due_at,
            kind: state::TimerKind::EventSubProcess {
                esp_element_id: esp_element_id.to_string(),
                interrupting: false,
            },
        }))
    }

    /// Merges an event payload (message / signal variables) for a trigger
    /// resting on `element_instance_key`: an event sub-process trigger rests on
    /// its **scope**, so the payload propagates from that scope; any other
    /// trigger rests on a token, whose enclosing flow scope it propagates from.
    pub(super) fn event_payload_scope(
        &self,
        instance_key: Key,
        element_instance_key: Key,
        kind: &state::MessageSubscriptionKind,
    ) -> Key {
        if matches!(kind, state::MessageSubscriptionKind::EventSubProcess { .. }) {
            element_instance_key
        } else {
            self.scope_of(instance_key, element_instance_key)
        }
    }

    /// Merges a correlated message / signal payload. Propagates from
    /// [`event_payload_scope`](Self::event_payload_scope) upward — except for
    /// an event sub-process whose start event carries output mappings: Zeebe
    /// then creates the payload LOCAL to the start event (only the mapped
    /// targets propagate), so it is held in `esp_start_payloads` and seeded
    /// when that start event activates later in this drain.
    pub(super) fn merge_event_payload(
        &mut self,
        log: &mut Vec<Event>,
        instance_key: Key,
        element_instance_key: Key,
        kind: &state::MessageSubscriptionKind,
        variables: &HashMap<String, Value>,
    ) {
        if variables.is_empty() {
            return;
        }
        if let state::MessageSubscriptionKind::EventSubProcess { esp_element_id, .. } = kind {
            let start = match self.element_kind(instance_key, esp_element_id) {
                Some(ElementKind::EventSubProcess { start_event, .. }) => Some(start_event),
                _ => None,
            };
            let start_has_outputs = start.as_ref().is_some_and(|id| {
                self.process_of_instance(instance_key)
                    .and_then(|p| p.element(id))
                    .is_some_and(|e| !e.io.outputs.is_empty())
            });
            if let (Some(start), true) = (start, start_has_outputs) {
                self.esp_start_payloads
                    .insert((instance_key, start), variables.clone());
                return;
            }
        }
        let flow_scope = self.event_payload_scope(instance_key, element_instance_key, kind);
        for event in self.propagated_updates(instance_key, flow_scope, variables.clone(), false) {
            self.emit(log, event);
        }
    }
}
