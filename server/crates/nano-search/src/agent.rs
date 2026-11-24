//! `searchAgentInstances` / `searchAgentInstanceHistory`: the single
//! spec-derived implementation both the gateway and `engine-wasm` call.

use nanobpmn_engine_core as agent_model;
use nanobpmn_read_model as readstore;
use agent_model::Key;

use crate::{
    SearchError,
    convert::{ms_to_datetime, tenant_or_default},
    models, query, types,
};

/// `POST /v2/agent-instances/search`: filter (every spec operator), sort and
/// paginate agent instances. `fetch` yields the candidate rows (all agent
/// instances in the read model).
pub fn search_agent_instances<E>(
    body: Option<&models::AgentInstanceSearchQuery>,
    fetch: impl FnOnce() -> Result<Vec<readstore::AgentInstanceRow>, E>,
) -> Result<models::AgentInstanceSearchQueryResult, SearchError<E>> {
    let filter = body.and_then(|q| q.filter.as_ref());
    let rows = fetch().map_err(SearchError::Store)?;
    let mut matched: Vec<&readstore::AgentInstanceRow> = rows
        .iter()
        .filter(|row| filter.is_none_or(|f| match_agent_instance_filter(row, f)))
        .collect();

    let sort = query::sort_keys(
        body.and_then(|q| q.sort.as_ref()),
        |r: &models::AgentInstanceSearchQuerySortRequest| (r.field.clone(), r.order),
    );
    query::sort_items(
        &mut matched,
        &sort,
        |row, field| agent_instance_sort_val(row, field),
        |row| row.agent_instance_key,
    );

    let sorted: Vec<(u64, &readstore::AgentInstanceRow)> = matched
        .into_iter()
        .map(|row| (row.agent_instance_key, row))
        .collect();
    let page = query::paginate(sorted, body.and_then(|q| q.page.as_ref()));
    let items: Vec<models::AgentInstanceResult> = page
        .items
        .into_iter()
        .map(agent_instance_result)
        .collect::<Result<_, _>>()
        .map_err(SearchError::Projection)?;
    Ok(models::AgentInstanceSearchQueryResult::new(
        page.response,
        items,
    ))
}

/// The read-model pre-filter for an agent's history search, or `None` when the
/// request can match nothing (an explicit empty `commitStatus` inclusion set).
/// The read model applies the COMMITTED-only default when `commitStatus` is
/// absent (see [`readstore::AgentHistoryFilter`]); every other filter property
/// is applied in memory with full operator semantics.
pub fn agent_history_store_filter(
    agent_instance_key: Key,
    body: Option<&models::AgentInstanceHistorySearchQuery>,
) -> Option<readstore::AgentHistoryFilter> {
    let commit_status = body
        .and_then(|q| q.filter.as_ref())
        .and_then(|filter| filter.commit_status.as_ref())
        .map(agent_commit_status_filter_values);
    if matches!(&commit_status, Some(v) if v.is_empty()) {
        // The read model collapses `Some(vec![])` to the COMMITTED-only
        // default, so "match nothing" is honoured here by skipping the query.
        return None;
    }
    Some(readstore::AgentHistoryFilter {
        agent_instance_key: Some(agent_instance_key),
        commit_status,
        ..Default::default()
    })
}

/// `POST /v2/agent-instances/{agentInstanceKey}/history/search`: an agent's
/// conversation history, defaulting to COMMITTED-only turns. `fetch` runs the
/// read-model query for [`agent_history_store_filter`].
pub fn search_agent_instance_history<E>(
    agent_instance_key: Key,
    body: Option<&models::AgentInstanceHistorySearchQuery>,
    fetch: impl FnOnce(&readstore::AgentHistoryFilter) -> Result<Vec<readstore::AgentHistoryRow>, E>,
) -> Result<models::AgentInstanceHistorySearchQueryResult, SearchError<E>> {
    let rows = match agent_history_store_filter(agent_instance_key, body) {
        Some(store_filter) => fetch(&store_filter).map_err(SearchError::Store)?,
        None => Vec::new(),
    };
    let filter = body.and_then(|q| q.filter.as_ref());
    let mut matched: Vec<&readstore::AgentHistoryRow> = rows
        .iter()
        .filter(|row| filter.is_none_or(|f| match_agent_history_filter(row, f)))
        .collect();

    let sort = query::sort_keys(
        body.and_then(|q| q.sort.as_ref()),
        |r: &models::AgentInstanceHistorySearchQuerySortRequest| (r.field.clone(), r.order),
    );
    query::sort_items(
        &mut matched,
        &sort,
        |row, field| agent_history_sort_val(row, field),
        |row| row.agent_history_key,
    );

    let sorted: Vec<(u64, &readstore::AgentHistoryRow)> = matched
        .into_iter()
        .map(|row| (row.agent_history_key, row))
        .collect();
    let page = query::paginate(sorted, body.and_then(|q| q.page.as_ref()));
    let items: Vec<models::AgentInstanceHistoryItemResult> = page
        .items
        .into_iter()
        .map(agent_history_item_result)
        .collect::<Result<_, _>>()
        .map_err(SearchError::Projection)?;
    Ok(models::AgentInstanceHistorySearchQueryResult::new(
        page.response,
        items,
    ))
}

/// Matches an agent-history row against the REST history filter (every
/// property; `commitStatus` is pre-applied by the read-model query).
pub fn match_agent_history_filter(
    row: &readstore::AgentHistoryRow,
    f: &models::AgentInstanceHistoryFilter,
) -> bool {
    query::match_agent_history_item_key(&f.history_item_key, &row.agent_history_key.to_string())
        && query::match_element_instance_key(
            &f.element_instance_key,
            &row.element_instance_key.to_string(),
        )
        && query::match_job_key(&f.job_key, &row.job_key.to_string())
        && f.loop_iteration.as_ref().is_none_or(|lf| {
            query::match_integer(&Some(lf.clone()), Some(row.loop_iteration as i64))
        })
        && f.role
            .as_ref()
            .is_none_or(|rf| match_agent_history_role(rf, row.role))
        && query::match_date_time_ms(&f.produced_at, Some(row.produced_at_ms as i64))
}

/// Maps an engine agent-instance status to the REST status enum.
pub fn agent_instance_status_enum(
    status: agent_model::AgentInstanceStatus,
) -> models::AgentInstanceStatusEnum {
    use agent_model::AgentInstanceStatus as S;
    match status {
        S::Initializing => models::AgentInstanceStatusEnum::Initializing,
        S::ToolDiscovery => models::AgentInstanceStatusEnum::ToolDiscovery,
        S::Thinking => models::AgentInstanceStatusEnum::Thinking,
        S::ToolCalling => models::AgentInstanceStatusEnum::ToolCalling,
        S::Idle => models::AgentInstanceStatusEnum::Idle,
        S::Completed => models::AgentInstanceStatusEnum::Completed,
    }
}

/// Maps an engine agent-history role to the REST role enum.
pub fn agent_history_role_enum(
    role: agent_model::AgentHistoryRole,
) -> models::AgentInstanceHistoryRoleEnum {
    use agent_model::AgentHistoryRole as R;
    match role {
        R::User => models::AgentInstanceHistoryRoleEnum::User,
        R::Assistant => models::AgentInstanceHistoryRoleEnum::Assistant,
        R::ToolResult => models::AgentInstanceHistoryRoleEnum::ToolResult,
        R::Configuration => models::AgentInstanceHistoryRoleEnum::Configuration,
    }
}

/// Maps an engine commit status to the REST commit-status enum.
pub fn agent_commit_status_enum(
    status: agent_model::AgentHistoryCommitStatus,
) -> models::AgentInstanceHistoryCommitStatusEnum {
    use agent_model::AgentHistoryCommitStatus as C;
    match status {
        C::Committed => models::AgentInstanceHistoryCommitStatusEnum::Committed,
        C::Pending => models::AgentInstanceHistoryCommitStatusEnum::Pending,
        C::Discarded => models::AgentInstanceHistoryCommitStatusEnum::Discarded,
    }
}

/// Maps a REST commit-status enum to the engine commit status.
pub fn agent_commit_status_from(
    status: &models::AgentInstanceHistoryCommitStatusEnum,
) -> agent_model::AgentHistoryCommitStatus {
    use agent_model::AgentHistoryCommitStatus as C;
    match status {
        models::AgentInstanceHistoryCommitStatusEnum::Committed => C::Committed,
        models::AgentInstanceHistoryCommitStatusEnum::Pending => C::Pending,
        models::AgentInstanceHistoryCommitStatusEnum::Discarded => C::Discarded,
    }
}

/// Maps an engine content block to the REST message-content shape. The engine's
/// `object` payload is an opaque JSON string; it is re-parsed into a structured
/// value so the wire carries JSON, not a JSON-in-a-string.
pub fn agent_message_content(
    c: &agent_model::AgentHistoryContent,
) -> Result<models::AgentInstanceMessageContent, serde_json::Error> {
    use agent_model::AgentHistoryContentType as CT;
    Ok(match c.content_type {
        CT::Text => models::AgentInstanceMessageContent::AgentInstanceTextContent(
            models::AgentInstanceTextContent::new(c.text.clone().unwrap_or_default()),
        ),
        CT::Document => models::AgentInstanceMessageContent::AgentInstanceDocumentContent(
            models::AgentInstanceDocumentContent::new(serde_json::from_str(
                c.document_reference.as_deref().unwrap_or("null"),
            )?),
        ),
        CT::Object => models::AgentInstanceMessageContent::AgentInstanceObjectContent(
            models::AgentInstanceObjectContent::new(types::Object(serde_json::from_str(
                c.object.as_deref().unwrap_or("null"),
            )?)),
        ),
    })
}

/// Maps an engine tool descriptor to the REST `AgentTool`.
pub fn agent_tool_result(t: &agent_model::AgentTool) -> models::AgentTool {
    models::AgentTool::new(
        t.name.clone(),
        match &t.description {
            Some(d) => types::Nullable::Present(d.clone()),
            None => types::Nullable::Null,
        },
        match &t.element_id {
            Some(e) => types::Nullable::Present(e.clone()),
            None => types::Nullable::Null,
        },
    )
}

/// Maps an engine tool call to the REST `AgentInstanceToolCall`. The opaque
/// arguments string is re-parsed into a JSON object; a non-object or unparsable
/// payload maps to null so the field stays schema-valid.
pub fn agent_tool_call_result(c: &agent_model::AgentHistoryToolCall) -> models::AgentInstanceToolCall {
    let arguments = c
        .arguments
        .as_ref()
        .and_then(|s| {
            serde_json::from_str::<std::collections::HashMap<String, serde_json::Value>>(s).ok()
        })
        .map(|m| {
            types::Nullable::Present(m.into_iter().map(|(k, v)| (k, types::Object(v))).collect())
        })
        .unwrap_or(types::Nullable::Null);
    models::AgentInstanceToolCall::new(
        c.tool_call_id.clone(),
        c.tool_name.clone(),
        match &c.element_id {
            Some(e) => types::Nullable::Present(e.clone()),
            None => types::Nullable::Null,
        },
        arguments,
    )
}

/// Maps engine per-turn metrics to the REST `AgentInstanceHistoryItemMetrics`
/// (every 8.10 per-turn counter; each is null when the turn did not report it).
pub fn agent_history_metrics_result(
    m: &agent_model::AgentHistoryMetrics,
) -> models::AgentInstanceHistoryItemMetrics {
    let n = |v: Option<i64>| v.map_or(types::Nullable::Null, types::Nullable::Present);
    models::AgentInstanceHistoryItemMetrics {
        input_tokens: n(m.input_tokens),
        output_tokens: n(m.output_tokens),
        reasoning_token_count: n(m.reasoning_token_count),
        cache_creation_token_count: n(m.cache_creation_token_count),
        cache_read_token_count: n(m.cache_read_token_count),
        duration_ms: n(m.duration_ms),
    }
}

/// Projects an [`readstore::AgentInstanceRow`] into the REST `AgentInstanceResult`.
/// `version_tag` is the projected `process_definition_version_tag` (null when the
/// deployment carried no tag); tools are decoded from the row's stored JSON,
/// defaulting to an empty list when absent or malformed; `elementInstanceKeys`
/// reports every element instance associated with the agent instance.
pub fn agent_system_prompt(
    prompt: &Option<Vec<agent_model::AgentHistoryContent>>,
) -> Result<Vec<models::AgentInstanceMessageContent>, serde_json::Error> {
    prompt
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(agent_message_content)
        .collect()
}

pub fn agent_instance_result(
    row: &readstore::AgentInstanceRow,
) -> Result<models::AgentInstanceResult, serde_json::Error> {
    let tools: Vec<agent_model::AgentTool> = serde_json::from_str(&row.tools_json)?;
    let completion_date = match row.completion_date_ms {
        Some(ms) => types::Nullable::Present(ms_to_datetime(ms)),
        None => types::Nullable::Null,
    };
    let version_tag = match &row.process_definition_version_tag {
        Some(tag) => types::Nullable::Present(tag.clone()),
        None => types::Nullable::Null,
    };
    Ok(models::AgentInstanceResult::new(
        models::AgentInstanceKey(row.agent_instance_key.to_string()),
        models::AgentDefinitionKey(row.agent_definition_key.to_string()),
        agent_instance_status_enum(row.status),
        models::AgentInstanceDefinitionResult::new(
            row.model.clone().unwrap_or_default(),
            row.provider.clone().unwrap_or_default(),
            agent_system_prompt(&row.system_prompt)?,
        ),
        models::AgentInstanceMetrics {
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            reasoning_token_count: row.reasoning_token_count,
            cache_creation_token_count: row.cache_creation_token_count,
            cache_read_token_count: row.cache_read_token_count,
            model_calls: row.model_calls as i32,
            tool_calls: row.tool_calls as i32,
        },
        models::AgentInstanceLimits::new(
            row.max_model_calls as i32,
            row.max_tool_calls as i32,
            row.max_tokens,
        ),
        tools.iter().map(agent_tool_result).collect(),
        row.element_id.clone(),
        models::ProcessInstanceKey(row.process_instance_key.to_string()),
        models::ProcessInstanceKey(row.root_process_instance_key.to_string()),
        models::ProcessDefinitionKey(row.process_definition_key.to_string()),
        row.process_definition_id.clone(),
        row.process_definition_version,
        version_tag,
        tenant_or_default(&row.tenant_id),
        ms_to_datetime(row.creation_date_ms),
        ms_to_datetime(row.last_updated_date_ms),
        completion_date,
        row.element_instance_keys
            .iter()
            .map(|k| k.to_string())
            .collect(),
    ))
}

/// Projects an [`readstore::AgentHistoryRow`] into the REST
/// `AgentInstanceHistoryItemResult`. Content, tool calls and tools are decoded
/// from the row's stored JSON columns; metrics are the canonical input/output/
/// duration subset.
pub fn agent_history_item_result(
    row: &readstore::AgentHistoryRow,
) -> Result<models::AgentInstanceHistoryItemResult, serde_json::Error> {
    let content: Vec<agent_model::AgentHistoryContent> = serde_json::from_str(&row.content_json)?;
    let tool_calls: Vec<agent_model::AgentHistoryToolCall> =
        serde_json::from_str(&row.tool_calls_json)?;
    let tools: Vec<agent_model::AgentTool> = serde_json::from_str(&row.tools_json)?;
    let metrics = row
        .metrics_json
        .as_deref()
        .map(serde_json::from_str::<Option<agent_model::AgentHistoryMetrics>>)
        .transpose()?
        .flatten()
        .as_ref()
        .map(agent_history_metrics_result)
        .map_or(types::Nullable::Null, types::Nullable::Present);
    let limits: agent_model::AgentInstanceLimits = row
        .limits_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or_default();
    let mut result = models::AgentInstanceHistoryItemResult::new(
        models::AgentHistoryItemKey(row.agent_history_key.to_string()),
        row.history_item_id.clone().unwrap_or_default(),
        models::AgentInstanceKey(row.agent_instance_key.to_string()),
        models::ElementInstanceKey(row.element_instance_key.to_string()),
        models::JobKey(row.job_key.to_string()),
        row.job_lease.clone(),
        u32::try_from(row.loop_iteration)
            .ok()
            .filter(|iteration| *iteration > 0)
            .ok_or_else(|| {
                <serde_json::Error as serde::de::Error>::custom("invalid stored loopIteration")
            })?,
        agent_history_role_enum(row.role),
        content
            .iter()
            .map(agent_message_content)
            .collect::<Result<_, _>>()?,
        tool_calls.iter().map(agent_tool_call_result).collect(),
        metrics,
        agent_commit_status_enum(row.commit_status),
        ms_to_datetime(row.produced_at_ms),
        tools.iter().map(agent_tool_result).collect(),
        match &row.model {
            Some(m) => types::Nullable::Present(m.clone()),
            None => types::Nullable::Null,
        },
        match &row.provider {
            Some(p) => types::Nullable::Present(p.clone()),
            None => types::Nullable::Null,
        },
        models::AgentInstanceLimits::new(
            limits.max_model_calls as i32,
            limits.max_tool_calls as i32,
            limits.max_tokens,
        ),
        agent_system_prompt(&row.system_prompt)?,
    );
    // Deprecation window (#1283): also emit the pre-8.10 `jobLease` name so
    // clients that still read it keep working. Mirrors jobLeaseToken exactly.
    result.job_lease = Some(result.job_lease_token.clone());
    Ok(result)
}

/// The [`query::SortVal`] for an agent-instance row on the given REST sort field.
pub fn agent_instance_sort_val(row: &readstore::AgentInstanceRow, field: &str) -> query::SortVal {
    match field {
        "agentDefinitionKey" => query::SortVal::Num(row.agent_definition_key as i128),
        "status" => query::SortVal::Str(row.status.as_str().to_string()),
        "elementId" => query::SortVal::Str(row.element_id.clone()),
        "processInstanceKey" => query::SortVal::Num(row.process_instance_key as i128),
        "rootProcessInstanceKey" => query::SortVal::Num(row.root_process_instance_key as i128),
        "processDefinitionKey" => query::SortVal::Num(row.process_definition_key as i128),
        "tenantId" => query::SortVal::Str(row.tenant_id.clone()),
        "creationDate" => query::SortVal::Num(row.creation_date_ms as i128),
        "lastUpdatedDate" => query::SortVal::Num(row.last_updated_date_ms as i128),
        "completionDate" => query::opt_num_sort_val(row.completion_date_ms.map(|v| v as i128)),
        "agentInstanceKey" => query::SortVal::Num(row.agent_instance_key as i128),
        _ => query::SortVal::Num(row.agent_instance_key as i128),
    }
}

/// The [`query::SortVal`] for an agent-history row on the given REST sort field.
pub fn agent_history_sort_val(row: &readstore::AgentHistoryRow, field: &str) -> query::SortVal {
    match field {
        "producedAt" => query::SortVal::Num(row.produced_at_ms as i128),
        "loopIteration" => query::SortVal::Num(row.loop_iteration as i128),
        "historyItemKey" => query::SortVal::Num(row.agent_history_key as i128),
        _ => query::SortVal::Num(row.agent_history_key as i128),
    }
}

/// Matches an [`readstore::AgentInstanceRow`] against a REST agent-instance filter.
pub fn match_agent_instance_filter(
    row: &readstore::AgentInstanceRow,
    f: &models::AgentInstanceFilter,
) -> bool {
    query::match_agent_instance_key(&f.agent_instance_key, &row.agent_instance_key.to_string())
        && query::match_agent_definition_key(
            &f.agent_definition_key,
            &row.agent_definition_key.to_string(),
        )
        && query::match_agent_instance_status(&f.status, row.status.as_str())
        && query::match_element_id(&f.element_id, &row.element_id)
        && query::match_process_instance_key(
            &f.process_instance_key,
            &row.process_instance_key.to_string(),
        )
        && query::match_process_instance_key(
            &f.root_process_instance_key,
            &row.root_process_instance_key.to_string(),
        )
        && query::match_process_definition_key(
            &f.process_definition_key,
            &row.process_definition_key.to_string(),
        )
        && query::match_string_opt(&f.tenant_id, Some(&row.tenant_id))
        && query::match_date_time_ms(&f.creation_date, Some(row.creation_date_ms as i64))
        && query::match_date_time_ms(&f.last_updated_date, Some(row.last_updated_date_ms as i64))
        && query::match_date_time_ms(&f.completion_date, row.completion_date_ms.map(|v| v as i64))
        && query::match_string_opt(&f.process_definition_id, Some(&row.process_definition_id))
        && query::match_integer(
            &f.process_definition_version,
            Some(row.process_definition_version as i64),
        )
        && query::match_string_opt(
            &f.process_definition_version_tag,
            row.process_definition_version_tag.as_deref(),
        )
        && match_agent_instance_element_keys(&f.element_instance_keys, &row.element_instance_keys)
}

/// Matches the `elementInstanceKeys` filter: every provided key filter must be
/// satisfied by at least one of the instance's associated element instance keys
/// (the contract's "associated with all of the provided keys" semantics).
pub fn match_agent_instance_element_keys(
    filters: &Option<Vec<models::ElementInstanceKeyFilterProperty>>,
    keys: &[Key],
) -> bool {
    match filters {
        None => true,
        Some(fs) => fs.iter().all(|f| {
            keys.iter()
                .any(|k| query::match_element_instance_key(&Some(f.clone()), &k.to_string()))
        }),
    }
}

pub fn agent_commit_status_filter_values(
    filter: &models::AgentInstanceHistoryCommitStatusFilterProperty,
) -> Vec<agent_model::AgentHistoryCommitStatus> {
    use models::AgentInstanceHistoryCommitStatusFilterProperty as P;
    match filter {
        P::AgentInstanceHistoryCommitStatusEnum(e) => vec![agent_commit_status_from(e)],
        P::AdvancedAgentInstanceHistoryCommitStatusFilter(a) => {
            use models::AgentInstanceHistoryCommitStatusEnum as S;
            [S::Committed, S::Pending, S::Discarded]
                .into_iter()
                .filter(|status| {
                    a.dollar_exists != Some(false)
                        && a.dollar_eq.is_none_or(|value| value == *status)
                        && a.dollar_neq.is_none_or(|value| value != *status)
                        && a.dollar_in
                            .as_ref()
                            .is_none_or(|values| values.contains(status))
                })
                .map(|status| agent_commit_status_from(&status))
                .collect()
        }
    }
}

/// Matches an agent-history row's role against a REST role filter (exact enum or
/// advanced `$eq`/`$neq`/`$exists`/`$in`). A role is always present, so
/// `$exists: false` never matches.
pub fn match_agent_history_role(
    filter: &models::AgentInstanceHistoryRoleFilterProperty,
    role: agent_model::AgentHistoryRole,
) -> bool {
    use models::AgentInstanceHistoryRoleFilterProperty as P;
    let actual = agent_history_role_enum(role);
    match filter {
        P::AgentInstanceHistoryRoleEnum(e) => *e == actual,
        P::AdvancedAgentInstanceHistoryRoleFilter(a) => {
            a.dollar_eq.as_ref().is_none_or(|v| *v == actual)
                && a.dollar_neq.as_ref().is_none_or(|v| *v != actual)
                && a.dollar_exists.is_none_or(|ex| ex)
                && a.dollar_in
                    .as_ref()
                    .is_none_or(|list| list.contains(&actual))
        }
    }
}
