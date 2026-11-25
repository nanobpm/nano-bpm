//! `searchUserTasks`: the single spec-derived implementation both the gateway
//! and `engine-wasm` call.

use nanobpmn_read_model as readstore;

use crate::{
    models,
    process_instance::{match_instance_variables, nullable_business_id},
    query,
    source::ReadSource,
    types,
};

/// `POST /v2/user-tasks/search`: filter (every field, `$or`, variable
/// filters), sort and paginate user tasks.
pub fn search_user_tasks<S: ReadSource + ?Sized>(
    store: &S,
    body: Option<&models::UserTaskSearchQuery>,
) -> models::UserTaskSearchQueryResult {
    let filter = body.and_then(|q| q.filter.as_ref());
    let tasks = store.user_tasks();
    // Variable-store join for `processInstanceVariables` / `localVariables`
    // (#1293), built only for the names the filter references.
    let variables = filter
        .map(|f| UserTaskVariableIndex::new(f, store))
        .unwrap_or_default();

    let mut matched: Vec<&readstore::UserTaskRow> = tasks
        .iter()
        .filter(|task| filter.is_none_or(|f| match_user_task_filter(task, f, &variables)))
        .collect();

    let sort = query::sort_keys(
        body.and_then(|q| q.sort.as_ref()),
        |r: &models::UserTaskSearchQuerySortRequest| (r.field.clone(), r.order),
    );
    query::sort_items(
        &mut matched,
        &sort,
        |task, field| user_task_sort_val(task, field),
        |task| task.key,
    );

    let sorted: Vec<(u64, &readstore::UserTaskRow)> =
        matched.into_iter().map(|task| (task.key, task)).collect();
    let page = query::paginate(sorted, body.and_then(|q| q.page.as_ref()));
    // One resolver spans the whole page so tasks sharing a parent chain walk
    // it once (mirrors the process-instance search; issue #1095).
    let roots = readstore::RootResolver::new(|k| store.process_instance(k));
    let items: Vec<models::UserTaskResult> = page
        .items
        .into_iter()
        .map(|task| user_task_result(task, &roots))
        .collect();
    models::UserTaskSearchQueryResult::new(page.response, items)
}

/// The `searchUserTasks` sort projection.
pub fn user_task_sort_val(task: &readstore::UserTaskRow, field: &str) -> query::SortVal {
    match field {
        "processInstanceKey" => query::SortVal::Num(task.instance_key as i128),
        "elementId" => query::SortVal::Str(task.element_id.clone()),
        "state" => query::SortVal::Str(user_task_state_enum(task.state).to_string()),
        "creationDate" => query::SortVal::Num(task.created_at_ms as i128),
        "businessId" => query::opt_str_sort_val(task.business_id.as_deref()),
        // Sort on the value `user_task_result` returns (clamped to
        // 0..=100) so rows reporting equal priorities tie.
        "priority" => query::SortVal::Num(i128::from(user_task_priority(task.priority))),
        // Sort on the instant, not the stored string, so differing
        // offsets/precision still order chronologically.
        "dueDate" => query::opt_num_sort_val(rfc3339_ns(task.due_date.as_deref())),
        "followUpDate" => query::opt_num_sort_val(rfc3339_ns(task.follow_up_date.as_deref())),
        // Constant in Nano: `user_task_result` always projects `name` and
        // `completionDate` as null, so every row ties and the task-key
        // tiebreak orders them.
        "name" | "completionDate" => query::SortVal::Num(0),
        _ => query::SortVal::Num(task.key as i128),
    }
}

/// The user-task filter predicate over one AND-group of fields, shared by the
/// top-level filter and each `$or` clause so the two cannot drift.
///
/// Every advertised field is matched against what the row actually projects.
/// Fields Nano never projects onto a user task — `name` (the result's `name`
/// is always null), `completionDate` (always null) and `tags` (always empty) —
/// match against an absent value: value operators exclude the row (honest,
/// never silently widening the search) while `$exists: false` still matches.
/// `tenantId` matches the `<default>` the result projects (single-tenant Nano),
/// exactly as the process-instance filter does — filtering on a value the
/// result reports must find the row (#1898 parity guard). The variable
/// filters match through `vars` (#1293): `processInstanceVariables` against
/// the owning instance's root-scope variables, `localVariables` against the
/// variables held at the task's element-instance scope.
pub fn match_user_task_fields(
    task: &readstore::UserTaskRow,
    f: &models::UserTaskFilterFields,
    vars: &UserTaskVariableIndex,
) -> bool {
    let assignee = task.assignee.clone().unwrap_or_default();
    // The task's due/follow-up dates are stored as RFC 3339 strings; project
    // them to epoch millis for the date-time filter algebra. An unparseable or
    // absent stored date is an absent value (value operators exclude it).
    let parse_ms = |d: &Option<String>| {
        d.as_deref()
            .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok())
            .map(|dt| dt.timestamp_millis())
    };
    f.user_task_key
        .as_ref()
        .is_none_or(|k| k.0 == task.key.to_string())
        && f.element_instance_key
            .as_ref()
            .is_none_or(|k| k.0 == task.element_instance_key.to_string())
        && query::match_process_instance_key(
            &f.process_instance_key,
            &task.instance_key.to_string(),
        )
        && query::match_process_definition_key(
            &f.process_definition_key,
            &task.process_definition_key,
        )
        && query::match_process_definition_id(
            &f.process_definition_id,
            Some(&task.process_definition_id),
        )
        && f.element_id.as_ref().is_none_or(|e| e == &task.element_id)
        && query::match_string(&f.assignee, &assignee)
        && query::match_user_task_state(&f.state, &user_task_state_enum(task.state).to_string())
        && query::match_string_opt(&f.business_id, task.business_id.as_deref())
        && query::match_integer(&f.priority, Some(i64::from(task.priority)))
        && f.candidate_group.as_ref().is_none_or(|g| match g {
            models::StringFilterProperty::String(s) => task.candidate_groups.contains(s),
            models::StringFilterProperty::AdvancedStringFilter(_) => task
                .candidate_groups
                .iter()
                .any(|cg| query::match_string_property(g, Some(cg))),
        })
        && f.candidate_user.as_ref().is_none_or(|u| match u {
            models::StringFilterProperty::String(s) => task.candidate_users.contains(s),
            models::StringFilterProperty::AdvancedStringFilter(_) => task
                .candidate_users
                .iter()
                .any(|cu| query::match_string_property(u, Some(cu))),
        })
        && query::match_date_time_ms(&f.creation_date, Some(task.created_at_ms as i64))
        && query::match_date_time_ms(&f.due_date, parse_ms(&task.due_date))
        && query::match_date_time_ms(&f.follow_up_date, parse_ms(&task.follow_up_date))
        // Never projected: completion date (result is always null).
        && query::match_date_time_ms(&f.completion_date, None)
        // Never projected: task name (result is always null).
        && query::match_string_opt(&f.name, None)
        // Never projected: task tags (result is always empty), so any listed
        // tag excludes the row.
        && f.tags.as_ref().is_none_or(|tags| tags.is_empty())
        && query::match_string(&f.tenant_id, "<default>")
        && match_instance_variables(
            &f.process_instance_variables,
            vars.process_instance.get(&task.instance_key),
        )
        && match_instance_variables(
            &f.local_variables,
            vars.local.get(&task.element_instance_key),
        )
}

/// `name -> serialized value` maps keyed by instance (root scope) and by
/// element-instance scope, restricted to the variable names a user-task filter
/// references (empty — no scan — when it references none).
#[derive(Default)]
pub struct UserTaskVariableIndex {
    process_instance: std::collections::HashMap<u64, std::collections::HashMap<String, String>>,
    local: std::collections::HashMap<u64, std::collections::HashMap<String, String>>,
}

impl UserTaskVariableIndex {
    pub fn new<S: ReadSource + ?Sized>(f: &models::UserTaskFilter, store: &S) -> Self {
        let mut groups = vec![user_task_filter_base(f)];
        groups.extend(f.dollar_or.iter().flatten().cloned());
        let names = |pick: fn(
            &models::UserTaskFilterFields,
        ) -> &Option<Vec<models::VariableValueFilterProperty>>| {
            groups
                .iter()
                .filter_map(|g| pick(g).as_ref())
                .flatten()
                .map(|e| e.name.clone())
                .collect::<std::collections::HashSet<String>>()
        };
        let root_names = names(|g| &g.process_instance_variables);
        let local_names = names(|g| &g.local_variables);
        let mut idx = UserTaskVariableIndex::default();
        if root_names.is_empty() && local_names.is_empty() {
            return idx;
        }
        // Only the referenced names are read, via the `name` index (#1886).
        let pushdown = readstore::VariablePushdown {
            names: Some(root_names.union(&local_names).cloned().collect()),
            ..Default::default()
        };
        for v in store.variables_matching(&pushdown) {
            if v.scope_key == v.instance_key && root_names.contains(&v.name) {
                idx.process_instance
                    .entry(v.instance_key)
                    .or_default()
                    .insert(v.name.clone(), v.value.clone());
            }
            if local_names.contains(&v.name) {
                idx.local
                    .entry(v.scope_key)
                    .or_default()
                    .insert(v.name, v.value);
            }
        }
        idx
    }
}

/// Copies every field of a top-level `UserTaskFilter` except `$or` into a
/// `UserTaskFilterFields`, so the base match reuses the `$or`-clause predicate.
pub fn user_task_filter_base(f: &models::UserTaskFilter) -> models::UserTaskFilterFields {
    let models::UserTaskFilter {
        state,
        assignee,
        business_id,
        priority,
        element_id,
        name,
        candidate_group,
        candidate_user,
        tenant_id,
        process_definition_id,
        creation_date,
        completion_date,
        follow_up_date,
        due_date,
        process_instance_variables,
        local_variables,
        user_task_key,
        process_definition_key,
        process_instance_key,
        element_instance_key,
        tags,
        dollar_or: _,
    } = f.clone();
    models::UserTaskFilterFields {
        state,
        assignee,
        business_id,
        priority,
        element_id,
        name,
        candidate_group,
        candidate_user,
        tenant_id,
        process_definition_id,
        creation_date,
        completion_date,
        follow_up_date,
        due_date,
        process_instance_variables,
        local_variables,
        user_task_key,
        process_definition_key,
        process_instance_key,
        element_instance_key,
        tags,
    }
}

/// Full user-task filter match: every top-level field AND, when a non-empty
/// `$or` list is present, at least one clause (each an AND of its fields).
pub fn match_user_task_filter(
    task: &readstore::UserTaskRow,
    f: &models::UserTaskFilter,
    vars: &UserTaskVariableIndex,
) -> bool {
    match_user_task_fields(task, &user_task_filter_base(f), vars)
        && match &f.dollar_or {
            Some(clauses) if !clauses.is_empty() => clauses
                .iter()
                .any(|c| match_user_task_fields(task, c, vars)),
            _ => true,
        }
}

/// Epoch nanoseconds of an optional RFC 3339 timestamp string (`None` when
/// absent or unparseable) — the sort projection for string-stored dates. Full
/// precision (not millis) so sub-millisecond instants the REST surface
/// returns verbatim still order correctly; `i128` cannot overflow.
pub fn rfc3339_ns(s: Option<&str>) -> Option<i128> {
    s.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| i128::from(d.timestamp()) * 1_000_000_000 + i128::from(d.timestamp_subsec_nanos()))
}

/// The user-task priority as exposed by REST (clamped to `0..=100`): the single
/// source for both the response projection and the sort projection.
pub fn user_task_priority(stored: i32) -> u8 {
    stored.clamp(0, 100) as u8
}

/// Maps an engine [`nanobpmn_engine_core::UserTaskState`] to the REST user-task
/// state enum.
pub fn user_task_state_enum(
    state: nanobpmn_engine_core::UserTaskState,
) -> models::UserTaskStateEnum {
    use nanobpmn_engine_core::UserTaskState;
    match state {
        UserTaskState::Created => models::UserTaskStateEnum::Created,
        UserTaskState::Completed => models::UserTaskStateEnum::Completed,
        UserTaskState::Canceled => models::UserTaskStateEnum::Canceled,
    }
}

/// Projects a [`readstore::UserTaskRow`] into the generated `UserTaskResult`. The
/// process-definition identity is denormalized onto the row at projection time.
///
/// `rootProcessInstanceKey` (issue #1095) is resolved from the task's
/// `processInstanceKey` by walking the call-activity parent chain via `roots`
/// (the same [`readstore::RootResolver`] the process-instance projection uses, so
/// there is one canonical root-walk, no duplicate derivation) — a top-level
/// (self-rooted) task reports its own `processInstanceKey`.
pub fn user_task_result(
    task: &readstore::UserTaskRow,
    roots: &readstore::RootResolver,
) -> models::UserTaskResult {
    // The business id snapshotted when the user task was created.
    let business_id = nullable_business_id(task.business_id.clone());
    let creation_date =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(task.created_at_ms as i64)
            .unwrap_or_else(chrono::Utc::now);

    let parse_date = |d: &Option<String>| -> types::Nullable<chrono::DateTime<chrono::Utc>> {
        match d
            .as_deref()
            .map(|s| s.parse::<chrono::DateTime<chrono::Utc>>())
        {
            Some(Ok(dt)) => types::Nullable::Present(dt),
            _ => types::Nullable::Null,
        }
    };

    models::UserTaskResult {
        name: types::Nullable::Null,
        state: user_task_state_enum(task.state),
        assignee: match &task.assignee {
            Some(a) => types::Nullable::Present(a.clone()),
            None => types::Nullable::Null,
        },
        element_id: task.element_id.clone(),
        candidate_groups: task.candidate_groups.clone(),
        candidate_users: task.candidate_users.clone(),
        process_definition_id: task.process_definition_id.clone(),
        creation_date,
        completion_date: types::Nullable::Null,
        follow_up_date: parse_date(&task.follow_up_date),
        due_date: parse_date(&task.due_date),
        tenant_id: "<default>".to_string(),
        external_form_reference: match &task.external_form_reference {
            Some(r) => types::Nullable::Present(r.clone()),
            None => types::Nullable::Null,
        },
        process_definition_version: task.process_definition_version,
        custom_headers: std::collections::HashMap::new(),
        priority: user_task_priority(task.priority),
        user_task_key: models::UserTaskKey(task.key.to_string()),
        element_instance_key: models::ElementInstanceKey(task.element_instance_key.to_string()),
        process_name: types::Nullable::Null,
        process_definition_key: models::ProcessDefinitionKey(task.process_definition_key.clone()),
        process_instance_key: models::ProcessInstanceKey(task.instance_key.to_string()),
        root_process_instance_key: types::Nullable::Present(models::ProcessInstanceKey(
            roots
                .root_process_instance_key(task.instance_key)
                .to_string(),
        )),
        business_id,
        form_key: match task.form_key {
            Some(k) => types::Nullable::Present(models::FormKey(k.to_string())),
            None => types::Nullable::Null,
        },
        tags: Vec::new(),
    }
}

#[cfg(test)]
mod sort_projection_tests {
    use super::{rfc3339_ns, user_task_priority};

    #[test]
    fn priority_sort_projection_matches_the_returned_clamped_value() {
        // Stored 100 and 101 both return priority 100, so they must tie in
        // the sort (letting the secondary sort / key tiebreak decide).
        assert_eq!(user_task_priority(100), user_task_priority(101));
        assert_eq!(user_task_priority(i32::MAX), 100);
        assert_eq!(user_task_priority(-5), 0);
        assert_eq!(user_task_priority(42), 42);
    }

    #[test]
    fn rfc3339_ns_orders_by_instant_not_string() {
        // Lexicographically "…T10:00:00+02:00" > "…T09:00:00Z", but it is the
        // earlier instant; sorting must follow the instant (#1298).
        let a = rfc3339_ns(Some("2026-01-01T10:00:00+02:00"));
        let b = rfc3339_ns(Some("2026-01-01T09:00:00Z"));
        assert!(a < b);
        assert_eq!(rfc3339_ns(None), None);
        assert_eq!(rfc3339_ns(Some("garbage")), None);
    }

    #[test]
    fn rfc3339_ns_keeps_sub_millisecond_precision() {
        let early = rfc3339_ns(Some("2026-01-01T00:00:00.000100Z"));
        let late = rfc3339_ns(Some("2026-01-01T00:00:00.000900Z"));
        assert!(early < late, "same-millisecond instants must not tie");
        // Pre-epoch instants stay ordered too.
        assert!(
            rfc3339_ns(Some("1969-12-31T23:59:59.5Z")) < rfc3339_ns(Some("1970-01-01T00:00:00Z"))
        );
    }
}
