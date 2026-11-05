//! `impl Engine` methods: resolve concern (extracted from the monolithic engine module).

use super::*;

impl Engine {
    /// Resolves the correlation value a subscription captures at open time by
    /// evaluating the stored `correlation_key` FEEL expression against the
    /// instance variables (a bare name like `orderId` is just a variable
    /// reference; `order.id` reads a context member). A missing variable, an
    /// empty key, or an evaluation error yields the empty string. That silent
    /// fallback is a known Zeebe divergence (Zeebe raises an incident), tracked
    /// in #1398; callers that can raise the incident use
    /// [`Self::resolve_correlation_value_checked`] instead.
    pub(crate) fn resolve_correlation_value(
        &self,
        vars: &HashMap<String, Value>,
        correlation_key: &str,
    ) -> String {
        self.resolve_correlation_value_checked(vars, correlation_key)
            .unwrap_or_default()
    }

    /// Like [`Self::resolve_correlation_value`] but distinguishes an
    /// *evaluation failure* from a legitimately empty key.
    ///
    /// A declared (non-empty) correlation-key expression that fails to evaluate
    /// — a missing variable, a `null` result, or a type error such as
    /// concatenating a string with `null` — returns `Err(reason)` so the caller
    /// can raise an incident instead of silently opening an unmatchable
    /// subscription with an empty key (Zeebe raises a correlation-key incident
    /// in exactly this case). An absent declaration (empty or whitespace-only
    /// string) resolves to `Ok("")`, matching the REST default `correlationKey`
    /// of `""`.
    pub(crate) fn resolve_correlation_value_checked(
        &self,
        vars: &HashMap<String, Value>,
        correlation_key: &str,
    ) -> Result<String, String> {
        if correlation_key.trim().is_empty() {
            return Ok(String::new());
        }
        crate::feel::eval_string(correlation_key, vars).map_err(|e| {
            format!("failed to evaluate correlation key expression '{correlation_key}': {e}")
        })
    }

    /// Resolves a message or signal event `name` at subscription-open time
    /// (Zeebe `evaluateStringExpression` on the message/signal name). A static
    /// value (`"order canceled"`) is returned verbatim; a FEEL expression (a
    /// leading `=`, e.g. `="order " + awaitingAction`) must evaluate to a
    /// `STRING` against `vars` (the scoped view the subscribing element sees),
    /// else an `EXTRACT_VALUE_ERROR` [`expression::Failure`] the caller raises
    /// as an incident — never a subscription named after the expression text.
    pub(crate) fn resolve_event_name_checked(
        &self,
        vars: &HashMap<String, Value>,
        raw: &str,
    ) -> Result<String, expression::Failure> {
        expression::evaluate_string(raw, vars)
    }

    /// The **lenient** event-name resolution still used by subscription sites
    /// with no incident path yet (boundary events, message start events, signal
    /// catches): on failure it falls back to the literal text. A known Zeebe
    /// divergence tracked in #1398 (allow-listed in `tests/feel_fallback_lint.rs`);
    /// a message intermediate catch uses [`Self::resolve_event_name_checked`].
    ///
    /// A message-start-event name is evaluated by Zeebe at deploy time against an
    /// empty context, so its caller passes an empty `vars`.
    pub(crate) fn resolve_event_name(&self, vars: &HashMap<String, Value>, raw: &str) -> String {
        self.resolve_event_name_checked(vars, raw)
            .unwrap_or_else(|_| raw.to_string())
    }

    /// The **lenient** retry-count resolution still used by job-creation sites
    /// with no incident path yet (listener / ad-hoc jobs): an unresolvable
    /// expression falls back to [`crate::state::DEFAULT_JOB_RETRIES`]. A known
    /// Zeebe divergence tracked in #1398 (allow-listed in
    /// `tests/feel_fallback_lint.rs`); element jobs use
    /// [`Self::resolve_job_retries`], which fails instead.
    pub(crate) fn resolve_retries(&self, vars: &HashMap<String, Value>, raw: Option<&str>) -> i32 {
        self.resolve_job_retries(vars, raw)
            .unwrap_or(crate::state::DEFAULT_JOB_RETRIES)
    }

    /// The raw `retries` expression declared on `element_id` (if any).
    pub(crate) fn retries_of(&self, instance_key: Key, element_id: &str) -> Option<String> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .and_then(|e| e.retries.clone())
    }

    /// The execution listeners declared on `element_id` for a given transition
    /// (start = activation, end = completion), in declaration order (ADR 0037).
    /// Empty for listener-free elements — the common case — so callers can gate
    /// the listener machinery on a cheap `is_empty` check.
    pub(crate) fn listeners_of(
        &self,
        instance_key: Key,
        element_id: &str,
        event_type: crate::model::ListenerEventType,
    ) -> Vec<crate::model::ExecutionListener> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| match event_type {
                crate::model::ListenerEventType::Start => e.start_listeners.clone(),
                crate::model::ListenerEventType::End => e.end_listeners.clone(),
            })
            .unwrap_or_default()
    }

    /// The task listeners of `event_type` declared on the user-task `element_id`
    /// in the given instance's process, in declaration order (ADR 0037 §6).
    /// Empty for non-user-task elements and user tasks without listeners of that
    /// event type, so the listener-free path is untouched.
    pub(crate) fn task_listeners_of(
        &self,
        instance_key: Key,
        element_id: &str,
        event_type: crate::model::TaskListenerEventType,
    ) -> Vec<crate::model::TaskListener> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| {
                e.task_listeners
                    .iter()
                    .filter(|l| l.event_type == event_type)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Evaluates the given io mappings against an explicit variable context
    /// `vars` (rather than the instance's persisted variables), returning the
    /// merged projection. Used when an as-yet-unapplied result (e.g. a script
    /// task's `resultVariable`) must be visible to output mappings within the
    /// same step.
    ///
    /// A source expression that fails to evaluate (a FEEL parse error, a type
    /// error, or an operation on a missing value like `"x" + missingVar`) is a
    /// hard failure returned as `Err` — it is **not** silently dropped. Callers
    /// raise an `IO_MAPPING_ERROR` incident and halt the element instead of
    /// proceeding with the target variable unset (Zeebe parity, #939). A bare
    /// reference to a missing variable (`=missingVar`) still evaluates to FEEL
    /// `null` (an `Ok`), so it is assigned as `null` and does not fail here.
    pub(crate) fn eval_io_mappings_in(
        &self,
        vars: &HashMap<String, Value>,
        mappings: &[crate::model::Mapping],
    ) -> Result<HashMap<String, Value>, IoMappingFailure> {
        let mut result: HashMap<String, Value> = HashMap::new();
        for m in mappings {
            match Self::eval_io_mapping_source(&m.source, vars) {
                Ok(value) => Self::assign_io_target(&mut result, vars, &m.target, value),
                Err(err) => {
                    return Err(IoMappingFailure {
                        reason: format!(
                            "failed to evaluate io mapping source '{}' for target '{}': {}",
                            m.source.trim(),
                            m.target,
                            err.0
                        ),
                    });
                }
            }
        }
        Ok(result)
    }

    /// Evaluates a single `zeebe:input`/`zeebe:output` mapping `source`,
    /// applying Zeebe's static-vs-FEEL rule: a `source` is a FEEL expression
    /// **only** when its (trimmed) text begins with the `=` marker; any other
    /// value is a **static literal string** passed through unevaluated (#1160).
    ///
    /// This matches Zeebe, which treats an ioMapping `source` the same as every
    /// other extension attribute (`zeebe:taskDefinition type`, `retries`, event
    /// names): `in-process` is the literal string `"in-process"`, not the FEEL
    /// subtraction `in - process`, and `{{secrets.FOO}}` is a literal the
    /// connector runtime resolves, not a malformed context expression. Only a
    /// leading `=` (e.g. `=1 + 1`) selects FEEL evaluation, where the marker is
    /// stripped by [`crate::feel::eval`].
    fn eval_io_mapping_source(
        source: &str,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, crate::feel::FeelError> {
        if source.trim_start().starts_with('=') {
            crate::feel::eval(source.trim(), vars)
        } else {
            // A static literal is passed through verbatim — matching the other
            // `resolve_*` helpers (`resolve_event_name` / `resolve_job_type`),
            // which return the raw text for the non-`=` branch. Trimming here
            // would silently drop significant leading/trailing whitespace.
            Ok(Value::Str(source.to_string()))
        }
    }

    /// Like [`Self::eval_io_mappings_in`], but *skips* a mapping whose source
    /// references a name in `tolerated` that is absent from `vars` — that mapping
    /// is not evaluated at all. Used at the multi-instance
    /// **body** level, where the activity's own mappings legitimately reference
    /// per-child bindings (`loopCounter` / the configured `inputElement`) that do
    /// not exist until a child is instantiated: those are authoritatively applied
    /// (and their failures raised) per child. Any other mapping that fails is a *genuine* failure and is returned as
    /// `Err`, so the body raises an `IO_MAPPING_ERROR` incident rather than
    /// silently projecting an empty result (#946).
    pub(crate) fn eval_io_mappings_tolerating(
        &self,
        vars: &HashMap<String, Value>,
        mappings: &[crate::model::Mapping],
        tolerated: &std::collections::HashSet<String>,
    ) -> Result<HashMap<String, Value>, IoMappingFailure> {
        let mut result: HashMap<String, Value> = HashMap::new();
        for m in mappings {
            // A mapping that reads a per-child binding absent at the body level is
            // skipped up front; the per-child pass applies it authoritatively.
            // Decided from the references, not from an evaluation failure: an
            // ill-typed FEEL operation on the absent (null) binding is a
            // suppressed failure yielding `null`, not an error (#1392), so a
            // failure-based check would project that `null` into the parent.
            let refs = crate::feel::referenced_variables(m.source.trim());
            if refs
                .iter()
                .any(|r| tolerated.contains(r) && !vars.contains_key(r))
            {
                continue;
            }
            match Self::eval_io_mapping_source(&m.source, vars) {
                Ok(value) => Self::assign_io_target(&mut result, vars, &m.target, value),
                Err(err) => {
                    return Err(IoMappingFailure {
                        reason: format!(
                            "failed to evaluate io mapping source '{}' for target '{}': {}",
                            m.source.trim(),
                            m.target,
                            err.0
                        ),
                    });
                }
            }
        }
        Ok(result)
    }

    pub(crate) fn outgoing(&self, instance_key: Key, element_id: &str) -> Vec<SequenceFlow> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.outgoing.clone())
            .unwrap_or_default()
    }

    pub(crate) fn incoming_count(&self, instance_key: Key, element_id: &str) -> usize {
        self.process_of_instance(instance_key)
            .map(|p| p.incoming_count(element_id))
            .unwrap_or(0)
    }

    pub(crate) fn variables(&self, instance_key: Key) -> Arc<HashMap<String, Value>> {
        let base = self
            .state
            .instances
            .get(&instance_key)
            .map(|i| Arc::clone(&i.variables))
            .unwrap_or_default();
        self.overlay_cluster_variables(base)
    }

    /// The variables visible to a specific element instance: its scope's local
    /// variables layered over its ancestors up to the root. For an element in the
    /// root scope with no sub-scopes this returns the shared instance-variables
    /// `Arc` directly (a cheap refcount bump); only an element that lives in (or
    /// under) a sub-scope — a sub-process, a multi-instance body or child — pays
    /// the clone-and-merge cost.
    pub(crate) fn variables_for_element(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Arc<HashMap<String, Value>> {
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Arc::default();
        };
        // The hierarchical scoped view visible to this element (root scope
        // fast-path returns the shared `variables` Arc unchanged). Multi-instance
        // child bindings (`inputElement`/`loopCounter`) and sub-process locals all
        // live in the child/sub-process scope, resolved by this walk.
        let scope = self.variable_scope_of(instance, element_instance_key);
        self.visible_variables(instance, scope)
    }

    /// The variable scope that directly owns `key`'s variables: `key` itself when
    /// it is a registered scope-owner (sub-process / multi-instance body or
    /// child), otherwise the nearest enclosing scope recorded when the element
    /// activated, defaulting to the root (process-instance) scope. Root-only
    /// instances always resolve to the instance key.
    pub(crate) fn variable_scope_of(
        &self,
        instance: &crate::state::ProcessInstance,
        key: Key,
    ) -> Key {
        if key == 0 || key == instance.key {
            return instance.key;
        }
        if instance.scope_parents.contains_key(&key) {
            return key;
        }
        // `scopes` maps an active element instance to its enclosing sub-process
        // scope-owner; walk to a registered scope, else fall back to root.
        match instance.scopes.get(&key) {
            Some(&enclosing) if instance.scope_parents.contains_key(&enclosing) => enclosing,
            _ => instance.key,
        }
    }

    /// The variables visible in `scope_key`, resolving each name from the scope
    /// upward to the root (a local binding shadows an ancestor's). The root scope
    /// with no child scopes is the hot path: it returns the shared `variables`
    /// `Arc` directly (a refcount bump, no clone), preserving the flat engine's
    /// job-activation cost. Only instances that actually opened a sub-scope pay
    /// the merge.
    pub(crate) fn visible_variables(
        &self,
        instance: &crate::state::ProcessInstance,
        scope_key: Key,
    ) -> Arc<HashMap<String, Value>> {
        let is_root = scope_key == 0 || scope_key == instance.key;
        if is_root || instance.scope_variables.is_empty() {
            return self.overlay_cluster_variables(Arc::clone(&instance.variables));
        }
        // Collect the scope chain leaf -> ... -> root, then merge root-first so
        // nearer scopes overwrite (shadow) farther ones.
        let mut chain: Vec<Key> = Vec::new();
        let mut cur = scope_key;
        loop {
            chain.push(cur);
            match instance.scope_parents.get(&cur) {
                Some(&parent) if parent != cur => cur = parent,
                _ => break,
            }
        }
        let mut merged = (*instance.variables).clone();
        for scope in chain.iter().rev() {
            if let Some(local) = instance.scope_variables.get(scope) {
                for (k, v) in local {
                    merged.insert(k.clone(), v.clone());
                }
            }
        }
        self.overlay_cluster_variables(Arc::new(merged))
    }

    /// Layers the host-injected [cluster variables](crate::cluster_vars) *beneath*
    /// an instance's own visible variables, so a FEEL expression evaluated against
    /// the result reads a cluster variable whenever the instance does not define a
    /// same-named local (instance/scope variables always shadow cluster ones).
    ///
    /// The common case — no cluster variables configured — returns `base`
    /// unchanged (a refcount bump, no clone), preserving the zero-copy variable
    /// resolution fast path. Only when the snapshot is non-empty does it pay a
    /// clone-and-merge. The engine is single-tenant, so the global scope plus the
    /// [`DEFAULT_TENANT`](crate::cluster_vars::DEFAULT_TENANT) tenant scope are
    /// overlaid; a variable scoped to any other tenant is not visible here.
    fn overlay_cluster_variables(
        &self,
        base: Arc<HashMap<String, Value>>,
    ) -> Arc<HashMap<String, Value>> {
        let guard = match self.cluster_variables.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.is_empty() {
            return base;
        }
        let mut merged: HashMap<String, Value> = HashMap::new();
        for (k, v) in guard.resolved_for(crate::cluster_vars::DEFAULT_TENANT) {
            merged.insert(k.clone(), v.clone());
        }
        for (k, v) in base.iter() {
            merged.insert(k.clone(), v.clone());
        }
        Arc::new(merged)
    }

    /// Builds the variable-write **events** for merging `variables` into `scope`
    /// with Zeebe propagation semantics (see [`Self::propagate_variables`]).
    ///
    /// A write that resolves to the root (process-instance) scope is emitted as
    /// the legacy flat [`crate::Event::VariablesUpdated`] so the log, read model
    /// and conditional-event re-evaluation stay byte-identical to the pre-scoping
    /// engine; a write that lands in any other scope is emitted as
    /// [`crate::Event::ScopedVariablesUpdated`]. The result is sorted by scope key
    /// for deterministic replay.
    pub(crate) fn propagated_updates(
        &self,
        instance_key: Key,
        scope: Key,
        variables: HashMap<String, Value>,
        local: bool,
    ) -> Vec<crate::Event> {
        let mut writes = self.propagate_variables(instance_key, scope, variables, local);
        writes.sort_by_key(|(dest, _)| *dest);
        writes
            .into_iter()
            .filter(|(_, vars)| !vars.is_empty())
            .map(|(dest, vars)| {
                if dest == 0 || dest == instance_key {
                    crate::Event::VariablesUpdated {
                        instance_key,
                        variables: vars,
                    }
                } else {
                    crate::Event::ScopedVariablesUpdated {
                        instance_key,
                        scope_key: dest,
                        variables: vars,
                    }
                }
            })
            .collect()
    }

    /// Resolves Zeebe variable **propagation** for a merge of `variables` into
    /// `target_scope`, returning the per-scope writes to emit (as
    /// [`crate::Event::ScopedVariablesUpdated`]). With `local` true every value
    /// lands in `target_scope`. Otherwise each name is written to the nearest
    /// ancestor scope (from the target upward) that already defines it; a name
    /// defined nowhere is created in the root scope. Root-only instances collapse
    /// to a single root write, matching the flat engine.
    pub(crate) fn propagate_variables(
        &self,
        instance_key: Key,
        target_scope: Key,
        variables: HashMap<String, Value>,
        local: bool,
    ) -> Vec<(Key, HashMap<String, Value>)> {
        if variables.is_empty() {
            return Vec::new();
        }
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return vec![(instance_key, variables)];
        };
        let root = instance.key;
        let target = self.variable_scope_of(instance, target_scope);
        // Fast path: local write, or a root-only instance — one scope, no walk.
        if local {
            return vec![(target, variables)];
        }
        if instance.scope_variables.is_empty() && instance.scope_parents.is_empty() {
            return vec![(root, variables)];
        }
        let mut by_scope: HashMap<Key, HashMap<String, Value>> = HashMap::new();
        for (name, value) in variables {
            let dest = self.scope_defining(instance, target, &name).unwrap_or(root);
            by_scope.entry(dest).or_default().insert(name, value);
        }
        by_scope.into_iter().collect()
    }

    /// The nearest scope (from `scope` upward to the root) that already defines
    /// `name`, or `None` if no scope in the chain holds it.
    fn scope_defining(
        &self,
        instance: &crate::state::ProcessInstance,
        scope: Key,
        name: &str,
    ) -> Option<Key> {
        let root = instance.key;
        let mut cur = scope;
        loop {
            let holds = if cur == root {
                instance.variables.contains_key(name)
            } else {
                instance
                    .scope_variables
                    .get(&cur)
                    .is_some_and(|m| m.contains_key(name))
            };
            if holds {
                return Some(cur);
            }
            if cur == root {
                return None;
            }
            match instance.scope_parents.get(&cur) {
                Some(&parent) if parent != cur => cur = parent,
                _ => return None,
            }
        }
    }

    /// Resolves a job's type at job-creation time (Zeebe
    /// `BpmnJobBehavior.evalTypeExp`). A static type is returned verbatim. A
    /// FEEL expression (a leading `=`, e.g. `=jobType` or `="worker-" + region`)
    /// must evaluate to a non-empty `STRING` of at most
    /// [`expression::MAX_JOB_TYPE_LENGTH`] characters; anything else (evaluation
    /// error, `null`, wrong type, empty, too long) is an `EXTRACT_VALUE_ERROR`
    /// [`expression::Failure`] that the caller raises as an incident with no job
    /// created (#1390).
    pub(crate) fn resolve_job_type(
        &self,
        vars: &HashMap<String, Value>,
        job_type: &str,
    ) -> Result<String, expression::Failure> {
        if !expression::is_expression(job_type) {
            return Ok(job_type.to_string());
        }
        expression::evaluate_non_empty_string(job_type, vars, expression::MAX_JOB_TYPE_LENGTH)
    }

    /// Resolves a job's retry count at job-creation time (Zeebe
    /// `BpmnJobBehavior.evalRetriesExp` -> `evaluateLongExpression`). `None` (no
    /// declaration) is [`crate::state::DEFAULT_JOB_RETRIES`]; a literal or FEEL
    /// value must be a `NUMBER` (floored at 0), else an `EXTRACT_VALUE_ERROR`.
    pub(crate) fn resolve_job_retries(
        &self,
        vars: &HashMap<String, Value>,
        raw: Option<&str>,
    ) -> Result<i32, expression::Failure> {
        let Some(raw) = raw else {
            return Ok(crate::state::DEFAULT_JOB_RETRIES);
        };
        let n = expression::evaluate_long(raw, vars)?;
        Ok(n.clamp(0, i64::from(i32::MAX)) as i32)
    }

    /// The **lenient** job-type resolution still used by job-creation sites that
    /// have no incident path yet (execution/task listener jobs, ad-hoc tool and
    /// agent jobs): on failure it falls back to the literal text. This is a
    /// known Zeebe divergence (Zeebe raises `EXTRACT_VALUE_ERROR` there too),
    /// tracked in #1398 and allow-listed in `tests/feel_fallback_lint.rs` — do
    /// not add new callers.
    pub(crate) fn resolve_job_type_lenient(
        &self,
        vars: &HashMap<String, Value>,
        job_type: &str,
    ) -> String {
        self.resolve_job_type(vars, job_type)
            .unwrap_or_else(|_| job_type.to_string())
    }

    /// Resolves a user-task string attribute (assignee, due/follow-up date)
    /// declared on the BPMN element. A literal (no leading `=`) is returned
    /// verbatim. A FEEL expression (leading `=`) is evaluated against `vars`
    /// (the element's scoped view): a string result yields `Some(string)`, while
    /// a **null** result or an **evaluation failure** (parse/runtime error,
    /// unresolved variable) yields `None` — the attribute is treated as absent
    /// (the task is created unassigned / with no date), matching Zeebe, which
    /// creates the task unassigned when the assignee expression resolves to null.
    /// `None` (the attribute was not declared) resolves to `None`.
    ///
    /// The raw `=expr` text is **never** stored: a value beginning with `=` is a
    /// FEEL expression, never a valid literal assignee/date, so falling back to
    /// it can only ever produce garbage that hides the task from assignee-aware
    /// views. The invariant callers rely on is that a resolved user-task string
    /// attribute never begins with `=`.
    pub(crate) fn resolve_user_task_string(
        &self,
        vars: &HashMap<String, Value>,
        raw: Option<&str>,
    ) -> Option<String> {
        let raw = raw?;
        let trimmed = raw.trim();
        if !trimmed.starts_with('=') {
            return Some(raw.to_string());
        }
        // A FEEL expression: a null result or an evaluation error means the
        // attribute is absent. `eval_string` returns `Err` for a null result as
        // well as for parse/runtime errors, so both collapse to `None` — never
        // the raw `=expr`.
        crate::feel::eval_string(trimmed, vars).ok()
    }

    /// Resolves a user-task candidate list (groups or users). A literal is a
    /// comma-separated list. A FEEL expression (leading `=`) is evaluated against
    /// `vars` (the element's scoped view); a list result yields its string items,
    /// a string result is split on commas, anything else (or a failure) yields an
    /// empty list. `None` resolves to an empty list.
    pub(crate) fn resolve_user_task_list(
        &self,
        vars: &HashMap<String, Value>,
        raw: Option<&str>,
    ) -> Vec<String> {
        let Some(raw) = raw else {
            return Vec::new();
        };
        let trimmed = raw.trim();
        let split = |s: &str| -> Vec<String> {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect()
        };
        if !trimmed.starts_with('=') {
            return split(raw);
        }
        match crate::feel::eval(trimmed, vars) {
            Ok(Value::List(items)) => items
                .into_iter()
                .filter_map(|v| match v {
                    Value::Str(s) => Some(s),
                    Value::Int(i) => Some(i.to_string()),
                    _ => None,
                })
                .filter(|s| !s.is_empty())
                .collect(),
            Ok(Value::Str(s)) => split(&s),
            _ => Vec::new(),
        }
    }

    /// Resolves a user-task priority expression. A literal integer or a FEEL
    /// expression yielding a number is clamped to `0..=100`; anything
    /// unresolvable (or absent) defaults to `50`.
    /// Resolves a raw priority expression (literal or `=FEEL`) against `vars`
    /// (the element's scoped view) to a `0..=100` value, defaulting to 50 when
    /// absent or unresolvable. Shared by user-task scheduling priority and
    /// service-task job (activation) priority.
    pub(crate) fn resolve_priority(&self, vars: &HashMap<String, Value>, raw: Option<&str>) -> i32 {
        const DEFAULT_PRIORITY: i32 = 50;
        let Some(raw) = raw else {
            return DEFAULT_PRIORITY;
        };
        let trimmed = raw.trim();
        let value = if let Some(expr) = trimmed.strip_prefix('=') {
            match crate::feel::eval(expr, vars) {
                Ok(Value::Int(i)) => i as i32,
                Ok(Value::Double(d)) => d as i32,
                _ => return DEFAULT_PRIORITY,
            }
        } else {
            match trimmed.parse::<i32>() {
                Ok(i) => i,
                Err(_) => return DEFAULT_PRIORITY,
            }
        };
        value.clamp(0, 100)
    }

    /// Resolves a user task's form linkage from its `zeebe:formDefinition`,
    /// enforcing the Zeebe invariant that `formId` and `externalReference` are
    /// mutually exclusive. An `externalReference` wins: it is surfaced verbatim
    /// and suppresses numeric `form_key` resolution, so a task never surfaces
    /// both a `formKey` and an `externalFormReference`. When no external
    /// reference is present, `formId` binds to the latest deployed form version
    /// (Zeebe `latest` binding), stamped as a numeric `form_key`; an unmatched
    /// `formId` leaves the key unset.
    ///
    /// Returns `(form_key, external_form_reference)`.
    pub(crate) fn resolve_user_task_form_linkage(
        &self,
        props: &crate::model::UserTaskProps,
    ) -> (Option<Key>, Option<String>) {
        let external_form_reference = props.external_form_reference.clone();
        let form_key = if external_form_reference.is_some() {
            None
        } else {
            props
                .form_id
                .as_deref()
                .and_then(|id| self.state.forms.get(id))
                .map(|f| f.key)
        };
        (form_key, external_form_reference)
    }

    /// Resolves a timer's due time (and, for a cycle, its re-arm interval) at
    /// timer-creation time, honouring a FEEL timer expression
    /// ([`crate::model::TimerDef`]) when one is declared on the element, and
    /// falling back to the statically-parsed `fallback_millis` otherwise.
    ///
    /// Returns `(due_at, interval_millis)` where `due_at` is an absolute epoch
    /// time in the engine's millisecond clock and `interval_millis` is the delay
    /// used to re-arm a repeating (cycle) timer (0 for an absolute date). `vars`
    /// is the scoped view the timer's element sees (empty for a process-level
    /// start timer evaluated at deploy against an empty context).
    pub(crate) fn resolve_timer(
        &self,
        vars: &HashMap<String, Value>,
        def: Option<&crate::model::TimerDef>,
        base_now: u64,
        fallback_millis: u64,
    ) -> (u64, u64) {
        let default = (base_now.saturating_add(fallback_millis), fallback_millis);
        let Some(def) = def else {
            return default;
        };

        // A `=`-prefixed expression is FEEL, evaluated against the scoped view (or
        // an empty context at deploy); a bare value (only for a literal timeDate)
        // is used directly. An evaluation failure falls back to the static
        // timer — a known Zeebe divergence (Zeebe raises EXTRACT_VALUE_ERROR),
        // tracked in #1398.
        let evaluated: Option<String> = if def.expr.trim_start().starts_with('=') {
            crate::feel::eval_string(&def.expr, vars).ok()
        } else {
            Some(def.expr.clone())
        };
        let Some(text) = evaluated else {
            return default;
        };
        let text = text.trim();

        match def.kind {
            crate::model::TimerDefKind::Duration => {
                match crate::temporal::parse_duration_millis(text) {
                    Some(ms) => (base_now.saturating_add(ms), ms),
                    None => default,
                }
            }
            crate::model::TimerDefKind::Cycle => match crate::temporal::parse_cycle_millis(text) {
                Some(ms) => (base_now.saturating_add(ms), ms),
                None => default,
            },
            crate::model::TimerDefKind::Date => {
                match crate::feel::temporal::DateTime::parse(text) {
                    Some(dt) => {
                        let millis = (dt.epoch_nanos() / 1_000_000).max(0) as u64;
                        (millis, 0)
                    }
                    None => default,
                }
            }
        }
    }

    /// Looks up the FEEL timer expression declared on `element_id` in the process
    /// definition that owns `instance_key` (if any).
    pub(crate) fn timer_def_of(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Option<crate::model::TimerDef> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .and_then(|e| e.timer.clone())
    }

    /// Resolves a variable scope key to the process instance that owns it. The
    /// key may be the process instance itself or any of its active element
    /// instances; nano keeps a single instance-level variable scope, so both map
    /// to the same instance.
    pub(crate) fn resolve_scope(&self, scope_key: Key) -> Option<Key> {
        if self.state.instances.contains_key(&scope_key) {
            return Some(scope_key);
        }
        self.state
            .instances
            .values()
            .find(|i| i.active.contains_key(&scope_key))
            .map(|i| i.key)
    }

    pub(crate) fn join_eik(&self, instance_key: Key, element_id: &str) -> Option<Key> {
        self.state
            .instances
            .get(&instance_key)?
            .join_instances
            .get(element_id)
            .copied()
    }

    /// Writes `value` to `target` (a plain name or a dotted path) inside the
    /// accumulating merge map, seeding nested context from the existing instance
    /// variables so a partial-path mapping (`order.total`) preserves the other
    /// members of `order`.
    fn assign_io_target(
        result: &mut HashMap<String, Value>,
        vars: &HashMap<String, Value>,
        target: &str,
        value: Value,
    ) {
        let parts: Vec<&str> = target.split('.').filter(|p| !p.is_empty()).collect();
        match parts.as_slice() {
            [] => {}
            [single] => {
                result.insert((*single).to_string(), value);
            }
            [top, rest @ ..] => {
                let mut root = result
                    .get(*top)
                    .or_else(|| vars.get(*top))
                    .cloned()
                    .unwrap_or_else(|| Value::Map(std::collections::BTreeMap::new()));
                Self::set_io_path(&mut root, rest, value);
                result.insert((*top).to_string(), root);
            }
        }
    }

    /// Sets a nested path inside a [`Value`], coercing intermediate nodes to
    /// maps as needed.
    fn set_io_path(node: &mut Value, parts: &[&str], value: Value) {
        let Some((head, tail)) = parts.split_first() else {
            *node = value;
            return;
        };
        if !matches!(node, Value::Map(_)) {
            *node = Value::Map(std::collections::BTreeMap::new());
        }
        if let Value::Map(map) = node {
            let entry = map
                .entry((*head).to_string())
                .or_insert_with(|| Value::Map(std::collections::BTreeMap::new()));
            Self::set_io_path(entry, tail, value);
        }
    }

    /// Cloned input mappings declared on an element (empty if none).
    pub(crate) fn io_inputs(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Vec<crate::model::Mapping> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.io.inputs.clone())
            .unwrap_or_default()
    }

    /// Cloned output mappings declared on an element (empty if none).
    pub(crate) fn io_outputs(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Vec<crate::model::Mapping> {
        self.process_of_instance(instance_key)
            .and_then(|p| p.element(element_id))
            .map(|e| e.io.outputs.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_correlation_treats_whitespace_only_key_as_absent() {
        // A whitespace-only declaration is not a real correlation-key
        // expression; it must resolve to Ok("") (absent) rather than being fed
        // to the FEEL parser and raising a spurious incident.
        let engine = Engine::new();
        let vars = HashMap::new();
        assert_eq!(
            engine.resolve_correlation_value_checked(&vars, "   "),
            Ok(String::new())
        );
        assert_eq!(
            engine.resolve_correlation_value_checked(&vars, ""),
            Ok(String::new())
        );
    }

    #[test]
    fn checked_correlation_surfaces_unevaluable_declared_key() {
        // A genuinely declared key that cannot evaluate (missing variable)
        // must return Err so the caller can raise an incident.
        let engine = Engine::new();
        let vars = HashMap::new();
        assert!(engine
            .resolve_correlation_value_checked(&vars, "missingVar")
            .is_err());
    }

    #[test]
    fn user_task_string_null_expression_resolves_absent() {
        // A `=FEEL` attribute whose expression evaluates to null must resolve to
        // None (attribute absent / task unassigned), NOT the raw `=expr` — this
        // is the red test for #900 (a null `=escalationAssignee` was stored as
        // the literal "=escalationAssignee").
        let engine = Engine::new();
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), Value::Null);
        assert_eq!(engine.resolve_user_task_string(&vars, Some("=x")), None);
    }

    #[test]
    fn user_task_string_missing_variable_resolves_absent() {
        // An unset/missing variable in the expression must resolve to None, not
        // the raw `=x`.
        let engine = Engine::new();
        let vars = HashMap::new();
        assert_eq!(engine.resolve_user_task_string(&vars, Some("=x")), None);
    }

    #[test]
    fn user_task_string_expression_resolves_to_string() {
        // A `=FEEL` expression yielding a real string is surfaced as that string.
        let engine = Engine::new();
        let mut vars = HashMap::new();
        vars.insert("x".to_string(), Value::Str("alice".to_string()));
        assert_eq!(
            engine.resolve_user_task_string(&vars, Some("=x")),
            Some("alice".to_string())
        );
    }

    #[test]
    fn user_task_string_literal_is_verbatim() {
        // A plain literal (no leading `=`) is returned unchanged.
        let engine = Engine::new();
        let vars = HashMap::new();
        assert_eq!(
            engine.resolve_user_task_string(&vars, Some("alice")),
            Some("alice".to_string())
        );
    }

    #[test]
    fn user_task_string_malformed_expression_resolves_absent() {
        // A malformed expression must resolve to None, never the raw `=1 + `.
        let engine = Engine::new();
        let vars = HashMap::new();
        assert_eq!(engine.resolve_user_task_string(&vars, Some("=1 + ")), None);
    }

    #[test]
    fn user_task_string_none_stays_none() {
        // An undeclared attribute resolves to None.
        let engine = Engine::new();
        let vars = HashMap::new();
        assert_eq!(engine.resolve_user_task_string(&vars, None), None);
    }

    #[test]
    fn user_task_string_never_begins_with_equals() {
        // The categorical invariant: a resolved user-task string attribute is
        // never a value beginning with `=`. Covers assignee + both dates (all
        // routed through resolve_user_task_string) against re-introduction.
        let engine = Engine::new();
        let mut vars = HashMap::new();
        vars.insert("nullVar".to_string(), Value::Null);
        vars.insert(
            "dateStr".to_string(),
            Value::Str("2026-01-01T00:00:00Z".to_string()),
        );
        let cases = [
            Some("=escalationAssignee"), // missing var -> None
            Some("=nullVar"),            // null -> None
            Some("=dateStr"),            // string -> the date, no leading '='
            Some("=1 + "),               // malformed -> None
            Some("alice"),               // literal -> verbatim
            None,
        ];
        for raw in cases {
            if let Some(resolved) = engine.resolve_user_task_string(&vars, raw) {
                assert!(
                    !resolved.starts_with('='),
                    "resolved user-task string attribute {resolved:?} must never begin with '='"
                );
            }
        }
    }

    #[test]
    fn cluster_variable_overlay_empty_returns_base_unchanged() {
        // The hot path: with no cluster variables configured, the overlay is a
        // no-op refcount bump — the very same allocation is returned.
        let engine = Engine::new();
        let mut base = HashMap::new();
        base.insert("x".to_string(), Value::Int(1));
        let base = Arc::new(base);
        let out = engine.overlay_cluster_variables(Arc::clone(&base));
        assert!(Arc::ptr_eq(&base, &out), "empty snapshot must not clone");
    }

    #[test]
    fn cluster_variable_overlay_layers_under_instance_variables() {
        use std::sync::{Arc as StdArc, RwLock};

        use crate::cluster_vars::{ClusterVariableSnapshot, DEFAULT_TENANT};

        let mut engine = Engine::new();
        let mut snap = ClusterVariableSnapshot::default();
        // A global var, and a same-named var the instance will shadow.
        snap.global
            .insert("region".to_string(), Value::Str("EMEA".to_string()));
        snap.global
            .insert("shared".to_string(), Value::Str("from-global".to_string()));
        // A default-tenant var (visible to the single-tenant engine).
        snap.tenants
            .entry(DEFAULT_TENANT.to_string())
            .or_default()
            .insert("tier".to_string(), Value::Int(2));
        // A var scoped to some other tenant must NOT be visible.
        snap.tenants
            .entry("other".to_string())
            .or_default()
            .insert("secret".to_string(), Value::Str("nope".to_string()));
        engine.set_cluster_variables(StdArc::new(RwLock::new(snap)));

        let mut base = HashMap::new();
        base.insert(
            "shared".to_string(),
            Value::Str("from-instance".to_string()),
        );
        let out = engine.overlay_cluster_variables(Arc::new(base));

        assert_eq!(out.get("region"), Some(&Value::Str("EMEA".to_string())));
        assert_eq!(out.get("tier"), Some(&Value::Int(2)));
        assert_eq!(
            out.get("secret"),
            None,
            "other-tenant var must be invisible"
        );
        assert_eq!(
            out.get("shared"),
            Some(&Value::Str("from-instance".to_string())),
            "instance variable must shadow the same-named cluster variable"
        );
    }
}
