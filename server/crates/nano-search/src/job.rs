//! Shared engine-job → REST-model projection (#992).
//!
//! The gateway's `POST /v2/jobs/activation` handler and the in-browser
//! `engine-wasm` `handle()` driver both answer a job activation with the
//! generated `ActivatedJobResult`. One mapper serves both, so the wire shape of
//! an activated job cannot drift between the two surfaces.

use std::collections::{BTreeMap, HashMap};

use nanobpm_gateway_rest::{models, types};
use nanobpmn_engine_core::{ActivatedJob, Value};

/// Camunda's default physical tenant id (`PhysicalTenantIds.DEFAULT_PHYSICAL_TENANT_ID`),
/// the one every Nano activation is served by — Nano has no physical tenants.
pub const DEFAULT_PHYSICAL_TENANT_ID: &str = "default";

/// Maps an engine [`nanobpmn_engine_core::JobKind`] to the REST `jobKind` +
/// `listenerEventType` pair. Ordinary element jobs report `BpmnElement` /
/// `Unspecified`; an execution-listener job (ADR 0037) reports
/// `ExecutionListener` and its start/end event type; a task-listener job (ADR
/// 0037 §6) reports `TaskListener` and its user-task lifecycle event type.
pub fn job_kind_enums(
    kind: &nanobpmn_engine_core::JobKind,
) -> (models::JobKindEnum, models::JobListenerEventTypeEnum) {
    use nanobpmn_engine_core::{JobKind, ListenerEventType, TaskListenerEventType};
    match kind {
        JobKind::BpmnElement => (
            models::JobKindEnum::BpmnElement,
            models::JobListenerEventTypeEnum::Unspecified,
        ),
        JobKind::ExecutionListener { event_type, .. } => (
            models::JobKindEnum::ExecutionListener,
            match event_type {
                ListenerEventType::Start => models::JobListenerEventTypeEnum::Start,
                ListenerEventType::End => models::JobListenerEventTypeEnum::End,
            },
        ),
        JobKind::TaskListener { event_type, .. } => (
            models::JobKindEnum::TaskListener,
            match event_type {
                TaskListenerEventType::Creating => models::JobListenerEventTypeEnum::Creating,
                TaskListenerEventType::Assigning => models::JobListenerEventTypeEnum::Assigning,
                TaskListenerEventType::Updating => models::JobListenerEventTypeEnum::Updating,
                TaskListenerEventType::Completing => models::JobListenerEventTypeEnum::Completing,
                TaskListenerEventType::Canceling => models::JobListenerEventTypeEnum::Canceling,
            },
        ),
    }
}

/// Converts engine variables into the generated `Object` (JSON) map used by the
/// REST models.
pub fn to_object_map(variables: &BTreeMap<String, Value>) -> HashMap<String, types::Object> {
    variables
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                types::Object(nanobpmn_read_model::value_to_json(value)),
            )
        })
        .collect()
}

/// Maps an engine [`ActivatedJob`] (plus its resolved process-definition
/// identity) into the generated `ActivatedJobResult`. When `fetch_variable` is
/// `Some`, only the named variables are returned; `None` returns all of the
/// job's visible variables.
pub fn activated_job_result(
    job: ActivatedJob,
    process_id: String,
    version: i32,
    process_definition_key: String,
    fetch_variable: Option<&[String]>,
) -> models::ActivatedJobResult {
    let variables = match fetch_variable {
        Some(names) => {
            let filtered: BTreeMap<String, Value> = job
                .variables
                .iter()
                .filter(|(name, _)| names.iter().any(|n| n == *name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            to_object_map(&filtered)
        }
        None => to_object_map(&job.variables),
    };

    let (job_kind_enum, job_listener_event_type_enum) = job_kind_enums(&job.kind);

    // Zeebe `customHeaders`: the task's static `zeebe:taskHeaders`, surfaced as
    // an object map (each value a JSON string) so the REST contract matches.
    let custom_headers: HashMap<String, types::Object> = job
        .custom_headers
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                types::Object(serde_json::Value::String(v.clone())),
            )
        })
        .collect();
    let tags: Vec<models::Tag> = job.tags.iter().cloned().map(models::Tag).collect();

    let job_lease_token = job
        .lease_token
        .map_or(types::Nullable::Null, types::Nullable::Present);
    models::ActivatedJobResult {
        r_type: job.job_type,
        process_definition_id: process_id,
        process_definition_version: version,
        element_id: job.element_id,
        custom_headers,
        worker: job.worker,
        retries: job.retries,
        deadline: job.deadline as i64,
        variables,
        tenant_id: nanobpmn_engine_core::DEFAULT_TENANT.to_string(),
        // Nano has no physical tenants: every activation is served by Camunda's
        // default one (`PhysicalTenantIds.DEFAULT_PHYSICAL_TENANT_ID`).
        physical_tenant_id: DEFAULT_PHYSICAL_TENANT_ID.to_string(),
        job_key: models::JobKey(job.key.to_string()),
        process_instance_key: models::ProcessInstanceKey(job.instance_key.to_string()),
        process_definition_key: models::ProcessDefinitionKey(process_definition_key),
        element_instance_key: models::ElementInstanceKey(job.element_instance_key.to_string()),
        kind: job_kind_enum,
        listener_event_type: job_listener_event_type_enum,
        user_task: types::Nullable::Null,
        tags,
        root_process_instance_key: types::Nullable::Null,
        business_id: job
            .business_id
            .map_or(types::Nullable::Null, types::Nullable::Present),
        priority: job.priority,
        // Deprecation window (#1283): also emit the pre-8.10 `leaseToken` name so
        // workers that have not yet moved to `jobLeaseToken` keep receiving their
        // token. Mirrors jobLeaseToken exactly (null when the job is unleased).
        lease_token: Some(job_lease_token.clone()),
        job_lease_token,
    }
}
