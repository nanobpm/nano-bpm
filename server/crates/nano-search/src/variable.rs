//! `searchVariables`: the single spec-derived implementation both the gateway
//! and `engine-wasm` call.

use nanobpmn_read_model::{self as readstore, VARIABLE_VALUE_PREVIEW_LEN};

use crate::{models, query, source::ReadSource, types};

/// `POST /v2/variables/search`: filter, sort and paginate variables.
/// `truncate` is the `truncateValues` query parameter (default `true`).
pub fn search_variables<S: ReadSource + ?Sized>(
    store: &S,
    truncate: bool,
    body: Option<&models::VariableSearchQuery>,
) -> models::VariableSearchQueryResult {
    let filter = body.and_then(|q| q.filter.as_ref());
    // Push the indexed equality / `$in` predicates down into the read store
    // (#1886) so a selective search reads only candidate rows instead of
    // full-scanning the table; the matcher below keeps full semantics.
    let vars = store.variables_matching(&variable_search_pushdown(filter));

    let mut matched: Vec<&readstore::VariableRow> = vars
        .iter()
        .filter(|v| match filter {
            None => true,
            Some(f) => {
                let tenant_ok = f.tenant_id.as_ref().is_none_or(|t| t == "<default>");
                let truncated = value_is_truncated(&v.value, truncate);
                tenant_ok
                    && query::match_string(&f.name, &v.name)
                    && query::match_string(&f.value, &v.value)
                    && query::match_variable_key(&f.variable_key, &v.key.to_string())
                    && query::match_scope_key(&f.scope_key, &v.scope_key.to_string())
                    && query::match_process_instance_key(
                        &f.process_instance_key,
                        &v.instance_key.to_string(),
                    )
                    && f.is_truncated.is_none_or(|want| want == truncated)
            }
        })
        .collect();

    let sort = query::sort_keys(
        body.and_then(|q| q.sort.as_ref()),
        |r: &models::VariableSearchQuerySortRequest| (r.field.clone(), r.order),
    );
    query::sort_items(&mut matched, &sort, |v, field| variable_sort_val(v, field), |v| v.key);

    let sorted: Vec<(u64, &readstore::VariableRow)> =
        matched.into_iter().map(|v| (v.key, v)).collect();
    let page = query::paginate(sorted, body.and_then(|q| q.page.as_ref()));
    let items: Vec<models::VariableSearchResult> = page
        .items
        .into_iter()
        .map(|v| variable_search_result(v, truncate))
        .collect();
    models::VariableSearchQueryResult::new(page.response, items)
}

/// The `searchVariables` sort projection.
pub fn variable_sort_val(v: &readstore::VariableRow, field: &str) -> query::SortVal {
    match field {
        "name" => query::SortVal::Str(v.name.clone()),
        "value" => query::SortVal::Str(v.value.clone()),
        "tenantId" => query::SortVal::Str("<default>".to_string()),
        "scopeKey" => query::SortVal::Num(v.scope_key as i128),
        "processInstanceKey" => query::SortVal::Num(v.instance_key as i128),
        "variableKey" => query::SortVal::Num(v.key as i128),
        _ => query::SortVal::Num(v.key as i128),
    }
}

/// Whether `value` would be truncated given the current `truncate` setting.
pub fn value_is_truncated(value: &str, truncate: bool) -> bool {
    truncate && value.len() > VARIABLE_VALUE_PREVIEW_LEN
}

/// Truncates `value` to the preview length on a char boundary when `truncate` is
/// on, returning the (possibly shortened) value and whether it was truncated.
pub fn truncate_value(value: &str, truncate: bool) -> (String, bool) {
    if value_is_truncated(value, truncate) {
        let mut end = VARIABLE_VALUE_PREVIEW_LEN;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        (value[..end].to_string(), true)
    } else {
        (value.to_string(), false)
    }
}

/// Derives the read-store pushdown for a variables search (#1886) from the
/// request filter: the indexed `variableKey` / `processInstanceKey` /
/// `scopeKey` / `name` equality and `$in` predicates. A tenant filter other
/// than `<default>` matches nothing (mirroring the in-memory matcher). The
/// result is a superset of the true match set; the caller re-applies the full
/// spec-derived matcher on the reduced rows.
pub fn variable_search_pushdown(
    filter: Option<&models::VariableFilter>,
) -> readstore::VariablePushdown {
    let Some(f) = filter else {
        return readstore::VariablePushdown::default();
    };
    let mut p = readstore::VariablePushdown {
        keys: query::key_candidates(query::variable_key_candidates(&f.variable_key)),
        instance_keys: query::key_candidates(query::process_instance_key_candidates(
            &f.process_instance_key,
        )),
        scope_keys: query::key_candidates(query::scope_key_candidates(&f.scope_key)),
        names: query::string_candidates(&f.name),
    };
    if f.tenant_id.as_ref().is_some_and(|t| t != "<default>") {
        p.keys = Some(Vec::new());
    }
    p
}

/// Projects a [`VariableRow`] into the generated `VariableSearchResult`, applying
/// value truncation per the request's `truncateValues` setting.
pub fn variable_search_result(
    v: &readstore::VariableRow,
    truncate: bool,
) -> models::VariableSearchResult {
    let (value, is_truncated) = truncate_value(&v.value, truncate);
    models::VariableSearchResult::new(
        v.name.clone(),
        "<default>".to_string(),
        models::VariableKey(v.key.to_string()),
        models::ScopeKey(v.scope_key.to_string()),
        models::ProcessInstanceKey(v.instance_key.to_string()),
        types::Nullable::Null,
        value,
        is_truncated,
    )
}

/// Projects a [`VariableRow`] into the generated `VariableResult` (single-get),
/// always carrying the full untruncated value.
pub fn variable_result(v: &readstore::VariableRow) -> models::VariableResult {
    models::VariableResult::new(
        v.name.clone(),
        "<default>".to_string(),
        models::VariableKey(v.key.to_string()),
        models::ScopeKey(v.scope_key.to_string()),
        models::ProcessInstanceKey(v.instance_key.to_string()),
        types::Nullable::Null,
        v.value.clone(),
    )
}
