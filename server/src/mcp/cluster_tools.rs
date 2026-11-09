//! The 18 static cluster tools: Camunda-compatible metadata (from
//! [`cluster_schemas`]) plus dispatch into the existing REST `*_impl` handlers.
//!
//! **No drift surface:** every tool deserializes its MCP arguments into the
//! generated `nanobpm-gateway-rest` spec types (path params, query params, or
//! request body) and calls the same `*_impl` handler the REST route uses. No
//! query/filter/mapping logic is re-implemented here — the only adaptation is
//! building the typed request from the flat MCP argument object.

use nanobpm_gateway_rest::models;
use serde_json::{Value, json};

use super::cluster_schemas::CLUSTER_TOOL_SCHEMAS;
use super::{DispatchFuture, Tool, ToolServer, map_rest_response, tool_error_result};
use crate::ServerImpl;

/// Register the `/mcp/cluster` tool server. Slice 3 (`/mcp/processes`) pushes
/// additional `ToolServer`s onto the returned vec without editing this file's
/// dispatch internals.
pub(crate) fn cluster_tool_servers() -> Vec<ToolServer> {
    vec![ToolServer {
        path: "/mcp/cluster",
        tools: build_tools(),
    }]
}

/// Build the 18 tools, wiring each schema entry to its dispatch by name.
fn build_tools() -> Vec<Tool> {
    CLUSTER_TOOL_SCHEMAS
        .iter()
        .map(|s| Tool {
            name: s.name,
            description: s.description,
            read_only: s.read_only,
            input_schema: s.input_schema(),
            dispatch: dispatch_for(s.name),
        })
        .collect()
}

/// Resolve a tool name to its dispatch fn. The set of names is exactly the 18
/// in the snapshot; an unknown name is unreachable (the registry is built from
/// the same table) but maps to a defensive `isError` rather than panicking.
fn dispatch_for(name: &str) -> fn(&ServerImpl, Value) -> DispatchFuture {
    match name {
        "getClusterStatus" => |_, _| Box::pin(async { not_implemented("getClusterStatus") }),
        "getTopology" => |s, _| Box::pin(get_topology(s)),
        "getIncident" => |s, a| Box::pin(get_incident(s, a)),
        "resolveIncident" => |s, a| Box::pin(resolve_incident(s, a)),
        "searchIncidents" => |s, a| Box::pin(search_incidents(s, a)),
        "getProcessDefinition" => |s, a| Box::pin(get_process_definition(s, a)),
        "getProcessDefinitionXml" => |s, a| Box::pin(get_process_definition_xml(s, a)),
        "searchProcessDefinitions" => |s, a| Box::pin(search_process_definitions(s, a)),
        "createProcessInstance" => |s, a| Box::pin(create_process_instance(s, a)),
        "getProcessInstance" => |s, a| Box::pin(get_process_instance(s, a)),
        "searchProcessInstances" => |s, a| Box::pin(search_process_instances(s, a)),
        "assignUserTask" => |s, a| Box::pin(assign_user_task(s, a)),
        "completeUserTask" => |s, a| Box::pin(complete_user_task(s, a)),
        "getUserTask" => |s, a| Box::pin(get_user_task(s, a)),
        "searchUserTaskVariables" => {
            |_, _| Box::pin(async { not_implemented("searchUserTaskVariables") })
        }
        "searchUserTasks" => |s, a| Box::pin(search_user_tasks(s, a)),
        "getVariable" => |s, a| Box::pin(get_variable(s, a)),
        "searchVariables" => |s, a| Box::pin(search_variables(s, a)),
        // Unreachable: the registry is built from the fixed 18-tool table, so
        // `dispatch_for` only ever sees a known name. A non-capturing fn
        // pointer keeps the match total without a defensive allocation.
        _ => |_, _| Box::pin(async { tool_error_result("Unknown tool", 400) }),
    }
}

/// The 501-shaped outcome for the two operations Nano has not implemented yet
/// (`getClusterStatus`, `searchUserTaskVariables`), matching the REST stub.
fn not_implemented(tool: &str) -> Value {
    tool_error_result(
        &format!("The `{tool}` operation is not implemented by this Nano gateway yet."),
        501,
    )
}

/// Pull a required integer key out of the flat MCP argument object.
fn arg_key(args: &Value, camel: &str) -> Result<String, Value> {
    args.get(camel)
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .map(|k| k.to_string())
        .ok_or_else(|| {
            tool_error_result(
                &format!("Missing or invalid required parameter `{camel}`."),
                400,
            )
        })
}

/// Deserialize the MCP arguments into a spec request-body model. The body
/// models use `#[serde(rename = "camelCase")]`, so the flat MCP argument object
/// deserializes directly.
fn arg_body<T: serde::de::DeserializeOwned>(args: Value) -> Result<T, Value> {
    serde_json::from_value(args)
        .map_err(|e| tool_error_result(&format!("Invalid arguments: {e}."), 400))
}

// --- cluster ---------------------------------------------------------------

async fn get_topology(server: &ServerImpl) -> Value {
    match server.get_topology_impl().await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("topology unavailable", 500),
    }
}

// --- incidents ---------------------------------------------------------------

async fn get_incident(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "incidentKey") {
        Ok(k) => models::GetIncidentPathParams { incident_key: k },
        Err(e) => return e,
    };
    match server.get_incident_impl(&path).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("get incident failed", 500),
    }
}

async fn resolve_incident(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "incidentKey") {
        Ok(k) => models::ResolveIncidentPathParams { incident_key: k },
        Err(e) => return e,
    };
    match server.resolve_incident_impl(&path, &None).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("resolve incident failed", 500),
    }
}

async fn search_incidents(server: &ServerImpl, args: Value) -> Value {
    let body: Option<models::IncidentSearchQuery> = match optional_body(args) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.search_incidents_impl(&body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("search incidents failed", 500),
    }
}

// --- process definitions -----------------------------------------------------

async fn get_process_definition(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "processDefinitionKey") {
        Ok(k) => models::GetProcessDefinitionPathParams {
            process_definition_key: k,
        },
        Err(e) => return e,
    };
    match server.get_process_definition_impl(&path).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("get process definition failed", 500),
    }
}

async fn get_process_definition_xml(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "processDefinitionKey") {
        Ok(k) => models::GetProcessDefinitionXmlPathParams {
            process_definition_key: k,
        },
        Err(e) => return e,
    };
    match server.get_process_definition_xml_impl(&path).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("get process definition xml failed", 500),
    }
}

async fn search_process_definitions(server: &ServerImpl, args: Value) -> Value {
    let body: Option<models::ProcessDefinitionSearchQuery> = match optional_body(args) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.search_process_definitions_impl(&body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("search process definitions failed", 500),
    }
}

// --- process instances ---------------------------------------------------------

async fn create_process_instance(server: &ServerImpl, args: Value) -> Value {
    let body: models::ProcessInstanceCreationInstruction = match arg_body(args) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.create_process_instance_impl(&body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("create process instance failed", 500),
    }
}

async fn get_process_instance(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "processInstanceKey") {
        Ok(k) => models::GetProcessInstancePathParams {
            process_instance_key: k,
        },
        Err(e) => return e,
    };
    match server.get_process_instance_impl(&path).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("get process instance failed", 500),
    }
}

async fn search_process_instances(server: &ServerImpl, args: Value) -> Value {
    let body: Option<models::ProcessInstanceSearchQuery> = match optional_body(args) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.search_process_instances_impl(&body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("search process instances failed", 500),
    }
}

// --- user tasks ----------------------------------------------------------------

async fn assign_user_task(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "userTaskKey") {
        Ok(k) => models::AssignUserTaskPathParams { user_task_key: k },
        Err(e) => return e,
    };
    // Camunda's MCP tool nests `action`/`allowOverride` under
    // `assignmentOptions`; the REST `UserTaskAssignmentRequest` expects them
    // flat alongside `assignee`. Flatten them (thin adapter, no logic).
    let mut body_json = json!({});
    if let Some(a) = args.get("assignee") {
        body_json["assignee"] = a.clone();
    }
    if let Some(opts) = args.get("assignmentOptions").and_then(Value::as_object) {
        if let Some(action) = opts.get("action") {
            body_json["action"] = action.clone();
        }
        if let Some(allow) = opts.get("allowOverride") {
            body_json["allowOverride"] = allow.clone();
        }
    }
    let body: models::UserTaskAssignmentRequest = match arg_body(body_json) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.assign_user_task_impl(&path, &body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("assign user task failed", 500),
    }
}

async fn complete_user_task(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "userTaskKey") {
        Ok(k) => models::CompleteUserTaskPathParams { user_task_key: k },
        Err(e) => return e,
    };
    // `completionOptions` holds the variables/action; map it onto the
    // completion request body the REST handler expects.
    let body: Option<models::UserTaskCompletionRequest> = match completion_body(&args) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.complete_user_task_impl(&path, &body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("complete user task failed", 500),
    }
}

async fn get_user_task(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "userTaskKey") {
        Ok(k) => models::GetUserTaskPathParams { user_task_key: k },
        Err(e) => return e,
    };
    match server.get_user_task_impl(&path).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("get user task failed", 500),
    }
}

async fn search_user_tasks(server: &ServerImpl, args: Value) -> Value {
    let body: Option<models::UserTaskSearchQuery> = match optional_body(args) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.search_user_tasks_impl(&body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("search user tasks failed", 500),
    }
}

// --- variables -----------------------------------------------------------------

async fn get_variable(server: &ServerImpl, args: Value) -> Value {
    let path = match arg_key(&args, "variableKey") {
        Ok(k) => models::GetVariablePathParams { variable_key: k },
        Err(e) => return e,
    };
    match server.get_variable_impl(&path).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("get variable failed", 500),
    }
}

async fn search_variables(server: &ServerImpl, args: Value) -> Value {
    // `truncateValues` is a query param; the rest is the search body.
    let truncate = args.get("truncateValues").and_then(Value::as_bool);
    let query = models::SearchVariablesQueryParams {
        truncate_values: truncate,
    };
    let body: Option<models::VariableSearchQuery> = match optional_body(args) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match server.search_variables_impl(&query, &body).await {
        Ok(resp) => map_rest_response(&resp),
        Err(()) => tool_error_result("search variables failed", 500),
    }
}

// --- argument adapters -----------------------------------------------------------

/// Deserialize an optional search body: an empty argument object maps to
/// `None` (no filters), a non-empty one to `Some(body)`.
fn optional_body<T: serde::de::DeserializeOwned>(args: Value) -> Result<Option<T>, Value> {
    let is_empty = args.as_object().map(|o| o.is_empty()).unwrap_or(false);
    if is_empty || args.is_null() {
        return Ok(None);
    }
    serde_json::from_value(args)
        .map(Some)
        .map_err(|e| tool_error_result(&format!("Invalid arguments: {e}."), 400))
}

/// Build the user-task completion body from the flat MCP arguments. Camunda's
/// `completeUserTask` nests `variables`/`action` under `completionOptions`;
/// the REST `UserTaskCompletionRequest` expects them at the top level.
fn completion_body(args: &Value) -> Result<Option<models::UserTaskCompletionRequest>, Value> {
    let Some(opts) = args.get("completionOptions") else {
        return Ok(None);
    };
    if opts.is_null() {
        return Ok(None);
    }
    serde_json::from_value(opts.clone())
        .map(Some)
        .map_err(|e| tool_error_result(&format!("Invalid `completionOptions`: {e}."), 400))
}
