//! `searchIncidents` / `searchElementInstanceIncidents` / `getIncident`: the
//! single spec-derived incident pipeline the gateway (and `engine-wasm`) call
//! (#991). Both incident searches share ONE filter predicate, ONE sort
//! projection and ONE result projection, so they cannot drift.

use nanobpmn_engine_core::{IncidentKind, IncidentState};
use nanobpmn_read_model as readstore;

use crate::{convert::epoch, models, query, source::ReadSource, types};

/// `POST /v2/incidents/search`: filter, sort and paginate every incident.
pub fn search_incidents<S: ReadSource + ?Sized>(
    store: &S,
    body: Option<&models::IncidentSearchQuery>,
) -> models::IncidentSearchQueryResult {
    search_incident_rows(&store.incidents(), body, |_| true)
}

/// Runs the incident search pipeline over `rows`, pre-restricted by `scope`
/// (e.g. the element-instance subtree of `searchElementInstanceIncidents`).
pub fn search_incident_rows(
    rows: &[readstore::IncidentRow],
    body: Option<&models::IncidentSearchQuery>,
    scope: impl Fn(&readstore::IncidentRow) -> bool,
) -> models::IncidentSearchQueryResult {
    let filter = body.and_then(|q| q.filter.as_ref());
    let mut matched: Vec<&readstore::IncidentRow> = rows
        .iter()
        .filter(|inc| scope(inc))
        .filter(|inc| filter.is_none_or(|f| match_incident_filter(inc, f)))
        .collect();

    // Sort: known fields, defaulting to incidentKey; entity key tiebreaks.
    let sort = query::sort_keys(
        body.and_then(|q| q.sort.as_ref()),
        |r: &models::IncidentSearchQuerySortRequest| (r.field.clone(), r.order),
    );
    query::sort_items(
        &mut matched,
        &sort,
        |inc, field| incident_sort_val(inc, field),
        |inc| inc.key,
    );

    let sorted: Vec<(u64, &readstore::IncidentRow)> =
        matched.into_iter().map(|inc| (inc.key, inc)).collect();
    let page = query::paginate(sorted, body.and_then(|q| q.page.as_ref()));
    let items: Vec<models::IncidentResult> = page.items.into_iter().map(incident_result).collect();
    models::IncidentSearchQueryResult::new(page.response, items)
}

/// The incident search sort projection.
pub fn incident_sort_val(inc: &readstore::IncidentRow, field: &str) -> query::SortVal {
    match field {
        "incidentKey" => query::SortVal::Num(inc.key as i128),
        "creationTime" => query::SortVal::Num(inc.created_at_ms as i128),
        "state" => query::SortVal::Str(incident_state_enum(inc.state).to_string()),
        "errorType" => query::SortVal::Str(incident_error_type_enum(inc.kind).to_string()),
        "processInstanceKey" => query::SortVal::Num(inc.instance_key as i128),
        "elementId" => query::SortVal::Str(inc.element_id.clone()),
        "elementInstanceKey" => query::SortVal::Num(inc.element_instance_key as i128),
        "jobKey" => query::opt_num_sort_val(inc.job_key.map(|k| k as i128)),
        "processDefinitionId" => query::SortVal::Str(inc.process_definition_id.clone()),
        "processDefinitionKey" => query::key_sort_val(&inc.process_definition_key),
        "tenantId" => query::SortVal::Str("<default>".to_string()),
        _ => query::SortVal::Num(inc.key as i128),
    }
}

/// Maps an engine [`IncidentKind`] to the REST `errorType` taxonomy.
pub fn incident_error_type_enum(kind: IncidentKind) -> models::IncidentErrorTypeEnum {
    match kind {
        IncidentKind::JobNoRetries => models::IncidentErrorTypeEnum::JobNoRetries,
        IncidentKind::NoMatchingSequenceFlow => models::IncidentErrorTypeEnum::ConditionError,
        IncidentKind::ExpressionEvaluation => models::IncidentErrorTypeEnum::ExtractValueError,
        IncidentKind::UnhandledError => models::IncidentErrorTypeEnum::UnhandledErrorEvent,
        IncidentKind::DecisionEvaluation => models::IncidentErrorTypeEnum::DecisionEvaluationError,
        IncidentKind::CalledElementError => models::IncidentErrorTypeEnum::CalledElementError,
        IncidentKind::IoMapping => models::IncidentErrorTypeEnum::IoMappingError,
        IncidentKind::ExtractValueError => models::IncidentErrorTypeEnum::ExtractValueError,
        // A nano-internal durability incident (#1384): an off-heap orphan's spilled
        // variables were definitively lost. The Camunda taxonomy has no analogue,
        // so it surfaces as `UNKNOWN` (the honest catch-all), not a BPMN error.
        IncidentKind::VariablesLost => models::IncidentErrorTypeEnum::Unknown,
    }
}

/// Maps an engine [`IncidentState`] to the REST incident `state` enum.
pub fn incident_state_enum(state: IncidentState) -> models::IncidentStateEnum {
    match state {
        IncidentState::Active => models::IncidentStateEnum::Active,
        IncidentState::Resolved => models::IncidentStateEnum::Resolved,
    }
}

/// Projects an [`IncidentRow`](readstore::IncidentRow) into the generated
/// `IncidentResult`. The process-definition identity is denormalized onto the
/// row at projection time.
pub fn incident_result(incident: &readstore::IncidentRow) -> models::IncidentResult {
    let job_key = match incident.job_key {
        Some(k) => types::Nullable::Present(models::JobKey(k.to_string())),
        None => types::Nullable::Null,
    };
    let creation_time =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(incident.created_at_ms as i64)
            .unwrap_or_else(epoch);

    models::IncidentResult::new(
        incident.process_definition_id.clone(),
        incident_error_type_enum(incident.kind),
        incident.reason.clone(),
        incident.element_id.clone(),
        creation_time,
        incident_state_enum(incident.state),
        "<default>".to_string(),
        models::IncidentKey(incident.key.to_string()),
        models::ProcessDefinitionKey(incident.process_definition_key.clone()),
        models::ProcessInstanceKey(incident.instance_key.to_string()),
        types::Nullable::Null,
        models::ElementInstanceKey(incident.element_instance_key.to_string()),
        job_key,
    )
}

/// The incident filter predicate. Every declared `IncidentFilter` field is
/// matched: the process-definition identity is denormalized onto the row at
/// projection time (no cross-entity lookup), and single-tenant Nano stores no
/// tenant on incidents, so `tenantId` matches against an absent value — value
/// operators exclude the row (honest, never silently widening the search) while
/// `$exists: false` still matches.
pub fn match_incident_filter(inc: &readstore::IncidentRow, f: &models::IncidentFilter) -> bool {
    query::match_basic_string(&f.incident_key, &inc.key.to_string())
        && query::match_process_instance_key(&f.process_instance_key, &inc.instance_key.to_string())
        && query::match_element_instance_key(
            &f.element_instance_key,
            &inc.element_instance_key.to_string(),
        )
        && query::match_process_definition_key(
            &f.process_definition_key,
            &inc.process_definition_key,
        )
        && query::match_string(&f.process_definition_id, &inc.process_definition_id)
        && query::match_job_key_opt(&f.job_key, inc.job_key.map(|k| k.to_string()).as_deref())
        && query::match_incident_state(&f.state, &incident_state_enum(inc.state).to_string())
        && query::match_incident_error_type(
            &f.error_type,
            &incident_error_type_enum(inc.kind).to_string(),
        )
        && query::match_string(&f.element_id, &inc.element_id)
        && query::match_string(&f.error_message, &inc.reason)
        && query::match_date_time_ms(&f.creation_time, Some(inc.created_at_ms as i64))
        && query::match_string_opt(&f.tenant_id, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: u64, eik: u64, state: IncidentState) -> readstore::IncidentRow {
        readstore::IncidentRow {
            key,
            instance_key: 1,
            element_instance_key: eik,
            element_id: "task".into(),
            kind: IncidentKind::JobNoRetries,
            state,
            reason: "boom".into(),
            job_key: None,
            created_at_ms: key,
            process_definition_id: "proc".into(),
            process_definition_key: "7".into(),
        }
    }

    fn keys(r: &models::IncidentSearchQueryResult) -> Vec<String> {
        r.items.iter().map(|i| i.incident_key.0.clone()).collect()
    }

    #[test]
    fn filters_sorts_and_scopes_through_one_pipeline() {
        let rows = vec![
            row(3, 10, IncidentState::Active),
            row(1, 11, IncidentState::Active),
            row(2, 10, IncidentState::Resolved),
        ];
        let all = search_incident_rows(&rows, None, |_| true);
        assert_eq!(keys(&all), ["1", "2", "3"]);

        let q: models::IncidentSearchQuery = serde_json::from_str(
            r#"{"filter":{"state":"ACTIVE"},"sort":[{"field":"creationTime","order":"DESC"}]}"#,
        )
        .unwrap();
        let active = search_incident_rows(&rows, Some(&q), |_| true);
        assert_eq!(keys(&active), ["3", "1"]);

        let scoped = search_incident_rows(&rows, None, |inc| inc.element_instance_key == 10);
        assert_eq!(keys(&scoped), ["2", "3"]);
        assert_eq!(
            scoped.items[0].error_type,
            models::IncidentErrorTypeEnum::JobNoRetries
        );
    }
}
