//! Call-activity lifecycle: child spawn, output projection, and the
//! call-depth guard.
//!
//! Extracted verbatim from `engine/mod.rs` (issue #1206, epic #1201 slice 5):
//! a private `impl Engine` block holding the whole call-activity concern —
//! spawning a callee instance from a sequence-flow call activity or an ad-hoc
//! tool, projecting the child's output back onto the caller on completion, and
//! the recursion-depth guard. Pure movement of whole methods; no logic change.

use std::collections::HashMap;

use super::{Engine, Step, MAX_CALL_ACTIVITY_DEPTH};
use crate::event::Event;
use crate::model::{ElementKind, Value};
use crate::state::{self, Key, ProcessInstanceState};

impl Engine {
    /// Builds the [`Step::CompleteCallActivity`] that releases a parent's parked
    /// call-activity token when `child_instance_key` (a call-activity child)
    /// completes, or `None` when the instance is not a call-activity child or its
    /// parent's call-activity element instance is no longer active (interrupted /
    /// cancelled). The child's final variables are captured here so the call
    /// activity's output mappings can project them after the terminal
    /// `ProcessInstanceCompleted` drops them.
    pub(super) fn call_activity_completion_step(&self, child_instance_key: Key) -> Option<Step> {
        let child = self.state.instances.get(&child_instance_key)?;
        let parent = child.parent_process_instance_key?;
        let call_eik = child.parent_element_instance_key?;
        let parent_instance = self.state.instances.get(&parent)?;
        let element_id = parent_instance.active.get(&call_eik)?.clone();
        let child_variables = (*child.variables).clone();
        Some(Step::CompleteCallActivity {
            instance_key: parent,
            element_instance_key: call_eik,
            element_id,
            child_variables,
        })
    }

    /// The live call-activity child instance parked on `call_eik` (a parent's
    /// call-activity element instance), if any. A call activity spawns exactly
    /// one child, found here by its `parentElementInstanceKey` back-link.
    /// Includes a `Terminating` child so an interrupt still reaps one already
    /// mid-drain, and a `Suspended` child: suspension freezes the child's
    /// progress, not its lifetime, so a host teardown must still reap it
    /// (a surviving suspended child would run orphaned work once resumed).
    /// Selection is deterministic (lowest instance key) so that, even if state
    /// ever held multiple matches (a bug or partial-replay artifact),
    /// boundary-interrupt cancellation always targets the same child.
    pub(super) fn call_activity_child_of(&self, call_eik: Key) -> Option<Key> {
        self.state
            .instances
            .values()
            .filter(|i| {
                matches!(
                    i.state,
                    ProcessInstanceState::Active
                        | ProcessInstanceState::Terminating
                        | ProcessInstanceState::Suspended
                ) && i.parent_element_instance_key == Some(call_eik)
            })
            .map(|i| i.key)
            .min()
    }

    /// Depth of `instance_key` in the call-activity parent chain (0 for a
    /// top-level instance). Used to cap runaway recursion.
    pub(super) fn call_activity_depth(&self, instance_key: Key) -> usize {
        let mut depth = 0;
        let mut cursor = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.parent_process_instance_key);
        while let Some(parent) = cursor {
            depth += 1;
            if depth >= MAX_CALL_ACTIVITY_DEPTH {
                break;
            }
            cursor = self
                .state
                .instances
                .get(&parent)
                .and_then(|i| i.parent_process_instance_key);
        }
        depth
    }

    /// Re-drives a call activity's child-process spawn after its input-mapping
    /// incident is resolved (#946). The call-activity element instance is already
    /// ACTIVATED with its boundary events armed (they were armed on the first
    /// pass, before the input mapping failed); this re-derives the callee id and
    /// the activating variable view and re-attempts only the spawn — re-applying
    /// the now-fixed input mappings and creating the child — without re-arming the
    /// boundary events or re-emitting the element's activation.
    pub(super) fn retry_call_activity_spawn(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        preserved_seed: Option<HashMap<String, Value>>,
    ) -> (Vec<Event>, Vec<Step>) {
        // Ad-hoc call-activity tool (issue #1159): the tool element is pruned from
        // the flat element graph, so `element_kind` returns `None` for it and the
        // ordinary spawn path below would silently no-op. Re-run the ad-hoc tool
        // spawn (its catalog callee id + input mappings + `propagateAllParentVariables`
        // seed) instead, so resolving a spawn incident actually creates the child.
        if let Some((container_key, _inner_key)) =
            self.adhoc_tool_container_of(instance_key, element_instance_key)
        {
            return self.respawn_adhoc_call_activity_tool(
                instance_key,
                element_instance_key,
                container_key,
                element_id,
                preserved_seed,
            );
        }
        let (called, propagate_all_parent) = match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::CallActivity {
                called_process_id,
                propagate_all_parent_variables,
                ..
            }) => (called_process_id, propagate_all_parent_variables),
            _ => return (Vec::new(), Vec::new()),
        };
        // Evaluate the retry in the call activity's OWN variable view, exactly
        // as its first pass did. `variables_for_element` resolves the element
        // instance's registered scope when it has one — a MULTI-INSTANCE child's
        // `inputElement` / `loopCounter` bindings (#1175), or element-local
        // variables a process-instance modification seeded into it (#1374) —
        // and otherwise falls back to the enclosing scope. Evaluating against
        // the enclosing scope instead would re-fail (or resolve wrongly) a
        // callee-id or input mapping that reads those element-local bindings.
        // An MI child's completion still routes back through `complete_mi_child`
        // (via `complete_call_activity`'s MI-child detection).
        let eval_scope = element_instance_key;
        let element_vars = (*self.variables_for_element(instance_key, eval_scope)).clone();
        self.spawn_call_activity_child(
            instance_key,
            element_instance_key,
            &element_id,
            &called,
            propagate_all_parent,
            &element_vars,
        )
    }

    /// Spawns the child process instance for an activating call activity and
    /// links it back to the parent (`parentProcessInstanceKey` /
    /// `parentElementInstanceKey`). The parent's call-activity element instance
    /// stays ACTIVATED (its token parked) until the child finishes. Variables
    /// cross the boundary through the call activity's input mappings and, when
    /// `propagate_all_parent` is set (Zeebe `propagateAllParentVariables`, the
    /// default), *all* variables visible in the call activity's scope — the input
    /// mappings applied on top (isolated child scope). An unknown callee, or
    /// exceeding the recursion depth cap, parks the token on a recoverable
    /// incident instead.
    pub(super) fn spawn_call_activity_child(
        &mut self,
        parent_instance: Key,
        call_eik: Key,
        element_id: &str,
        called_process_id: &str,
        propagate_all_parent: bool,
        element_vars: &HashMap<String, Value>,
    ) -> (Vec<Event>, Vec<Step>) {
        // Input mappings are evaluated into the call activity's local scope
        // regardless (Zeebe: they seed the child's local variables). With
        // `propagate_all_parent` (the Zeebe `propagateAllParentVariables`
        // default), *all* variables visible in the call activity's scope also
        // cross into the child, with the input-mapping results layered on top
        // (child-local wins). With it off, only the input-mapping results cross —
        // an isolated child seeded purely by the mappings.
        let inputs = self.io_inputs(parent_instance, element_id);
        let input_results = if inputs.is_empty() {
            HashMap::new()
        } else {
            match self.eval_io_mappings_in(element_vars, &inputs) {
                Ok(updates) => updates,
                Err(failure) => {
                    // A call-activity input mapping that fails to evaluate halts the
                    // call activity with an `IO_MAPPING_ERROR` incident rather than
                    // starting the child process against a silently-unset variable
                    // (#939/#946). Resolution re-drives only the *spawn*
                    // (`CallActivitySpawn`) for the already-activated call activity —
                    // its boundary events were armed on the first pass and must not
                    // be re-armed.
                    let event = self.io_mapping_incident(
                        parent_instance,
                        call_eik,
                        element_id.to_string(),
                        failure,
                        state::IoMappingRedrive::CallActivitySpawn,
                    );
                    return (vec![event], Vec::new());
                }
            }
        };
        let child_vars = if propagate_all_parent {
            let mut merged = element_vars.clone();
            merged.extend(input_results);
            merged
        } else {
            input_results
        };
        self.spawn_call_activity_instance(
            parent_instance,
            call_eik,
            element_id,
            called_process_id,
            element_vars,
            child_vars,
            None,
        )
    }

    /// Shared child-process spawn for a call activity, whether it is reached on
    /// an ordinary sequence flow ([`spawn_call_activity_child`]) or as an ad-hoc
    /// tool ([`activate_adhoc_tool`], issue #1159). Resolves the callee id (a
    /// literal or a FEEL `=` expression, evaluated against `eval_view`), guards
    /// runaway recursion, resolves the called definition, and — on success —
    /// creates the child process instance seeded with the caller-prepared
    /// `child_seed` variables, linked back to `call_eik` via
    /// `parentElementInstanceKey`/`parentProcessInstanceKey` so its completion
    /// releases the parent's parked call-activity token. An unresolvable callee
    /// expression, an unknown callee, or exceeding the depth cap parks the token
    /// on a recoverable incident instead (no child is created). The caller owns
    /// the input mappings and the `propagateAllParentVariables` seed decision, so
    /// this single site owns only the callee-resolution + spawn machinery.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_call_activity_instance(
        &mut self,
        parent_instance: Key,
        call_eik: Key,
        element_id: &str,
        called_process_id: &str,
        eval_view: &HashMap<String, Value>,
        child_seed: HashMap<String, Value>,
        // The ad-hoc tool's single-pass input projection to preserve on any
        // recoverable spawn incident (issue #1159 / #1176), or `None` for a
        // mainstream sequence-flow call activity. When `Some`, a spawn incident
        // records this projection (`AdHocCallActivitySpawn.child_seed`) so the
        // post-resolve respawn overlays it onto a FRESH all-parent view instead of
        // re-projecting the tool's (possibly chained) input mappings against the
        // already-mutated child scope; a mainstream call activity (`None`)
        // re-derives its seed idempotently from its still-stable element scope and
        // so carries the plain [`state::IoMappingRedrive::CallActivitySpawn`].
        adhoc_input_projection: Option<HashMap<String, Value>>,
    ) -> (Vec<Event>, Vec<Step>) {
        // The recoverable-spawn-incident redrive: for an ad-hoc tool it preserves
        // the single-pass input projection so the respawn does not re-evaluate the
        // tool's (possibly chained) input mappings; for a mainstream call activity
        // it is the plain spawn-retry marker. Built **lazily** — only an incident
        // path needs it, and the ad-hoc variant clones just the preserved input
        // projection, so the hot success path must not pay that copy: it moves the
        // original `child_seed` straight into `ProcessInstanceCreated` instead.
        let spawn_redrive = || match &adhoc_input_projection {
            Some(projection) => state::IoMappingRedrive::AdHocCallActivitySpawn {
                child_seed: projection.clone(),
            },
            None => state::IoMappingRedrive::CallActivitySpawn,
        };
        // The callee id may be a literal or a FEEL `=` expression (C8
        // `zeebe:calledElement processId`), resolved against the activating view.
        // A failing expression must surface as an expression-evaluation incident
        // that names the expression — not fall back to the raw `=…` text, which
        // would masquerade as an "unknown called process '=…'" lookup miss and
        // point diagnosis at the wrong thing.
        let called = {
            let trimmed = called_process_id.trim();
            if trimmed.starts_with('=') {
                match crate::feel::eval_string(trimmed, eval_view) {
                    Ok(id) => id,
                    Err(err) => {
                        let incident_key = self.mint_key();
                        return (
                            vec![Event::IncidentRaised {
                                incident_key,
                                instance_key: parent_instance,
                                element_instance_key: call_eik,
                                element_id: element_id.to_string(),
                                kind: state::IncidentKind::ExpressionEvaluation,
                                // Recoverable via a spawn retry (issue #1159): once
                                // the operator fixes the variables the `=calledElement`
                                // expression reads, resolving the incident re-attempts
                                // the spawn (`RetryCallActivitySpawn`) rather than
                                // completing the parked call activity / ad-hoc tool
                                // with no child.
                                redrive: Some(spawn_redrive()),
                                reason: format!(
                                    "call activity '{element_id}' could not evaluate \
                                     calledElement expression '{called_process_id}': {err}"
                                ),
                                job_key: None,
                                created_at: self.now,
                            }],
                            Vec::new(),
                        );
                    }
                }
            } else {
                called_process_id.to_string()
            }
        };
        // Guard runaway recursion (self / mutually-recursive callees).
        if self.call_activity_depth(parent_instance) >= MAX_CALL_ACTIVITY_DEPTH {
            let incident_key = self.mint_key();
            return (
                vec![Event::IncidentRaised {
                    incident_key,
                    instance_key: parent_instance,
                    element_instance_key: call_eik,
                    element_id: element_id.to_string(),
                    kind: state::IncidentKind::CalledElementError,
                    redrive: Some(spawn_redrive()),
                    reason: format!(
                        "call activity '{element_id}' exceeded the maximum child-instance depth \
                         of {MAX_CALL_ACTIVITY_DEPTH} calling '{called}' (possible unbounded \
                         recursion)"
                    ),
                    job_key: None,
                    created_at: self.now,
                }],
                Vec::new(),
            );
        }
        // Resolve the called definition (latest deployed version by id); copy the
        // fields we need so the immutable `state` borrow ends before we mint keys.
        let resolved = self
            .state
            .processes
            .get(&called)
            .map(|d| (d.key, d.version, d.definition.start_event.clone()));
        let Some((process_definition_key, version, start_event)) = resolved else {
            let incident_key = self.mint_key();
            return (
                vec![Event::IncidentRaised {
                    incident_key,
                    instance_key: parent_instance,
                    element_instance_key: call_eik,
                    element_id: element_id.to_string(),
                    kind: state::IncidentKind::CalledElementError,
                    // Recoverable via a spawn retry (issue #1159): deploying the
                    // missing callee and resolving the incident re-attempts the
                    // spawn instead of completing the parent with no child.
                    redrive: Some(spawn_redrive()),
                    reason: format!(
                        "call activity '{element_id}' references unknown called process '{called}'"
                    ),
                    job_key: None,
                    created_at: self.now,
                }],
                Vec::new(),
            );
        };
        let child_key = self.mint_key();
        (
            vec![Event::ProcessInstanceCreated {
                instance_key: child_key,
                process_id: called,
                variables: child_seed,
                created_at: self.now,
                tags: Vec::new(),
                business_id: None,
                process_definition_key,
                version,
                parent_process_instance_key: Some(parent_instance),
                parent_element_instance_key: Some(call_eik),
            }],
            vec![Step::Activate {
                instance_key: child_key,
                element_id: start_event,
                scope: 0,
                via: None,
            }],
        )
    }

    /// Completes a call-activity token once its child process instance finished.
    /// With `propagateAllChildVariables` (the Zeebe default) the child's final
    /// variables are merged back into the parent scope (child wins on a name
    /// collision); the call activity's output mappings then project on top
    /// (isolated scopes). Completes the element, disarms its boundary events and
    /// takes its outgoing flow. A no-op if the element instance is no longer
    /// active (a boundary event interrupted the wait before the child finished).
    pub(super) fn complete_call_activity(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        child_variables: HashMap<String, Value>,
    ) -> (Vec<Event>, Vec<Step>) {
        let still_active = self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.active.contains_key(&element_instance_key))
            .unwrap_or(false);
        if !still_active {
            return (Vec::new(), Vec::new());
        }
        // A call activity used *as an ad-hoc tool* (issue #1159) is not a leaf on
        // an ordinary sequence flow: its element instance hangs off an
        // `AD_HOC_SUB_PROCESS_INNER_INSTANCE` whose scope is the ad-hoc container
        // that still lists it active. Its completion must feed the container's
        // `outputElement`/loop (via `complete_adhoc_tool`), not take an outgoing
        // sequence flow, so route it there — carrying the child's produced
        // variables so the tool's output mapping projects them for real.
        if let Some((container_key, inner_key)) =
            self.adhoc_tool_container_of(instance_key, element_instance_key)
        {
            return self.complete_adhoc_call_activity_tool(
                instance_key,
                element_instance_key,
                element_id,
                container_key,
                inner_key,
                child_variables,
            );
        }
        // Zeebe `propagateAllChildVariables` (default true when the attribute is
        // absent). When off, only the output mappings cross back.
        let propagate_all_child = match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::CallActivity {
                propagate_all_child_variables,
                ..
            }) => propagate_all_child_variables,
            _ => true,
        };
        let scope = self.scope_of(instance_key, element_instance_key);

        // A call-activity that is a MULTI-INSTANCE child (its token parks in an MI
        // body whose loop element is this call activity) must NOT take the
        // activity's outgoing flow on completion — its result feeds the loop's
        // output collection / completion condition, and the loop advances the next
        // child or drains the body. `complete_call_activity` is reached directly
        // (the callee process instance drained), bypassing the MI-child detection
        // in `complete`, so without this route a normally-completing MI
        // call-activity loop would leave the callee's key in the body's `active`
        // set and hang forever (#1170). Build the child's produced-output overlay
        // exactly as the ordinary path would (propagateAllChildVariables first,
        // then the activity's own `zeebe:output`, which overrides) and hand it to
        // `complete_mi_child`.
        let is_mi_child = scope != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false);
        if is_mi_child {
            let mut overlay: HashMap<String, Value> = HashMap::new();
            if propagate_all_child && !child_variables.is_empty() {
                overlay.extend(child_variables.clone());
            }
            let outputs = self.io_outputs(instance_key, &element_id);
            if !outputs.is_empty() {
                match self.eval_io_mappings_in(&child_variables, &outputs) {
                    Ok(updates) => overlay.extend(updates),
                    Err(failure) => {
                        // Same halt-on-mapping-failure semantics as the ordinary
                        // path: park the call activity on an incident whose
                        // resolution re-drives its completion against the captured
                        // child variables (#939/#946), rather than dropping the
                        // child with a silently-unset output.
                        let event = self.io_mapping_incident(
                            instance_key,
                            element_instance_key,
                            element_id,
                            failure,
                            state::IoMappingRedrive::CallActivityCompletion {
                                child_variables: child_variables.clone(),
                            },
                        );
                        return (vec![event], Vec::new());
                    }
                }
            }
            return self.complete_mi_child(
                instance_key,
                element_instance_key,
                element_id,
                scope,
                Some(overlay),
            );
        }

        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        ];
        events.extend(self.cancel_boundary_timers_on(element_instance_key));
        events.extend(self.cancel_boundary_message_subscriptions_on(element_instance_key));
        events.extend(self.cancel_boundary_signal_subscriptions_on(element_instance_key));
        events.extend(self.cancel_boundary_conditional_subscriptions_on(element_instance_key));
        // propagateAllChildVariables: merge every variable the child finished with
        // back into the parent scope (child wins on collision) *before* the output
        // mappings, so an explicit output mapping still overrides a propagated
        // value.
        if propagate_all_child && !child_variables.is_empty() {
            events.extend(self.propagated_updates(
                instance_key,
                scope,
                child_variables.clone(),
                false,
            ));
        }
        let outputs = self.io_outputs(instance_key, &element_id);
        if !outputs.is_empty() {
            match self.eval_io_mappings_in(&child_variables, &outputs) {
                Ok(updates) => {
                    if !updates.is_empty() {
                        events.extend(self.propagated_updates(instance_key, scope, updates, false));
                    }
                }
                Err(failure) => {
                    // A call-activity output mapping that fails to evaluate halts the
                    // call activity with an `IO_MAPPING_ERROR` incident instead of
                    // completing it with a silently-unset output (#939/#946).
                    // Resolution re-drives its *completion* against the captured
                    // child variables (`CallActivityCompletion`) — the completed
                    // child that produced them is gone by resolution time, so they
                    // are preserved on the incident and re-projected through the
                    // output mappings.
                    let event = self.io_mapping_incident(
                        instance_key,
                        element_instance_key,
                        element_id,
                        failure,
                        state::IoMappingRedrive::CallActivityCompletion {
                            child_variables: child_variables.clone(),
                        },
                    );
                    return (vec![event], Vec::new());
                }
            }
        }
        let mut followups = Vec::new();
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }
}
