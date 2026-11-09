//! MCP (Model Context Protocol) gateway surface.
//!
//! This module implements Nano's Camunda-compatible MCP servers as a thin
//! **protocol adapter** over the existing REST handlers. It adds no business
//! logic: every tool call deserializes its arguments into the generated
//! `nanobpm-gateway-rest` spec types and dispatches through the same `*_impl`
//! handler the corresponding REST route uses, so the MCP surface can never
//! drift from the REST behaviour.
//!
//! Two servers are mounted (both behind the same auth as the REST API and the
//! `NANOBPM_MCP_ENABLED` gate, default off):
//!
//! - `/mcp/cluster` — Camunda's 18 static cluster tools (slice 2, this module).
//! - `/mcp/processes` — dynamic per-process tools (slice 3, builds on the seam
//!   here).
//!
//! ## Transport
//!
//! A stateless, streamable-HTTP subset of MCP: JSON-RPC 2.0 over HTTP POST,
//! supporting `initialize`, `tools/list`, and `tools/call`. We implement this
//! narrow subset directly rather than pulling in a full MCP server crate
//! (e.g. `rmcp`): the endpoint is stateless (no session/SSE streams), the
//! method set is tiny and fixed, and a hand-rolled handler gives exact control
//! over the Camunda-shaped tool metadata and the `isError` + ProblemDetail
//! error mapping without a large dependency. See the PR for the rationale.
//!
//! ## The seam (for slices 3 and 4)
//!
//! [`mcp_router`] is the single mount/registration helper `main.rs` calls once.
//! Sibling slices add tools/routes by registering additional [`ToolServer`]s —
//! they do not edit the dispatch internals here.

pub(crate) mod cluster_schemas;
pub(crate) mod cluster_tools;

use serde_json::{Value, json};

use crate::ServerImpl;

/// The JSON-RPC 2.0 version string echoed on every response.
const JSONRPC: &str = "2.0";
/// The MCP protocol revision this server speaks. Camunda 8.9's gateway-mcp
/// negotiates the 2025-03-26 streamable-HTTP revision.
const PROTOCOL_VERSION: &str = "2025-03-26";
/// The `serverInfo` identity reported on `initialize`.
const SERVER_NAME: &str = "nanobpm-mcp-gateway";

/// The env gate (default **off**, matching `camunda.mcp.enabled`). Slice 4 owns
/// the full config/docs/banner; this slice reads the key so the server is dark
/// unless explicitly enabled. Accepts the usual truthy spellings.
pub(crate) const MCP_ENABLED_ENV: &str = "NANOBPM_MCP_ENABLED";

/// Whether the MCP surface is enabled. Truthy spellings: `1`, `true`, `yes`,
/// `on` (case-insensitive). Unset or anything else leaves it off.
pub(crate) fn mcp_enabled() -> bool {
    std::env::var(MCP_ENABLED_ENV)
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// A named MCP tool: its advertised `tools/list` metadata plus the dispatch
/// that executes a call. Implemented per-server (cluster tools here; process
/// tools in slice 3).
pub(crate) struct Tool {
    pub(crate) name: &'static str,
    pub(crate) description: &'static str,
    pub(crate) read_only: bool,
    pub(crate) input_schema: Value,
    /// Execute a call. Returns the MCP `CallToolResult` payload — a success
    /// (`isError` absent/false, content = the REST response body) or a failure
    /// (`isError: true`, content = the ProblemDetail), mirroring Camunda's
    /// `CallToolResultMapper`. Dispatch never returns a transport error for a
    /// *failed tool call*; only malformed JSON-RPC is a transport error.
    pub(crate) dispatch: for<'a> fn(&'a ServerImpl, Value) -> DispatchFuture<'a>,
}

/// The boxed future a tool dispatch returns (the `*_impl` handlers are async).
pub(crate) type DispatchFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'a>>;

/// A collection of tools served at one mount path. Slice 3 registers a second
/// `ToolServer` for `/mcp/processes` without touching the dispatch internals.
pub(crate) struct ToolServer {
    /// The mount path, e.g. `/mcp/cluster`.
    pub(crate) path: &'static str,
    /// The tools this server exposes, in `tools/list` order.
    pub(crate) tools: Vec<Tool>,
}

impl ToolServer {
    fn find(&self, name: &str) -> Option<&Tool> {
        self.tools.iter().find(|t| t.name == name)
    }

    /// Build the `tools/list` result payload.
    fn tools_list(&self) -> Value {
        let tools: Vec<Value> = self
            .tools
            .iter()
            .map(|t| {
                let mut tool = json!({
                    "name": t.name,
                    "title": t.name,
                    "description": t.description,
                    "inputSchema": t.input_schema,
                });
                if t.read_only {
                    tool["annotations"] = json!({ "readOnlyHint": true });
                }
                tool
            })
            .collect();
        json!({ "tools": tools })
    }
}

/// Build the MCP axum router mounting every registered [`ToolServer`].
///
/// This is the seam `main.rs` calls once. The returned router serves each
/// server's `path` at HTTP POST. Slice 4 layers the `NANOBPM_MCP_ENABLED` gate
/// and startup banner on top of this single call site.
pub(crate) fn mcp_router(server: ServerImpl) -> axum::Router {
    let mut app = axum::Router::new();
    for srv in cluster_tools::cluster_tool_servers() {
        // Each tool server is shared by every request to its path.
        let srv = std::sync::Arc::new(srv);
        let server = server.clone();
        app = app.route(
            srv.path,
            axum::routing::post(move |body: axum::body::Bytes| {
                let server = server.clone();
                let srv = srv.clone();
                async move { handle_http(&server, &srv, &body).await }
            }),
        );
    }
    app
}

/// The HTTP entry point: parse the JSON-RPC frame, dispatch, serialize the
/// response. A stateless POST → single JSON-RPC response (no SSE stream).
async fn handle_http(
    server: &ServerImpl,
    srv: &ToolServer,
    body: &[u8],
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let frame: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(rpc_error(Value::Null, -32700, &format!("Parse error: {e}"))),
            )
                .into_response();
        }
    };
    let resp = handle_frame(server, srv, frame).await;
    (axum::http::StatusCode::OK, axum::Json(resp)).into_response()
}

/// Dispatch one parsed JSON-RPC request frame to the right MCP method.
async fn handle_frame(server: &ServerImpl, srv: &ToolServer, frame: Value) -> Value {
    let id = frame.get("id").cloned().unwrap_or(Value::Null);
    let method = frame.get("method").and_then(Value::as_str).unwrap_or("");
    match method {
        "initialize" => rpc_result(id, initialize_result()),
        "notifications/initialized" | "initialized" => rpc_result(id, json!({})),
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(id, srv.tools_list()),
        "tools/call" => {
            let params = frame.get("params").cloned().unwrap_or(Value::Null);
            handle_tool_call(server, srv, id, params).await
        }
        other => rpc_error(id, -32601, &format!("Method not found: {other}")),
    }
}

/// Execute a `tools/call`: resolve the tool, run its dispatch, wrap the result.
async fn handle_tool_call(
    server: &ServerImpl,
    srv: &ToolServer,
    id: Value,
    params: Value,
) -> Value {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let Some(tool) = srv.find(name) else {
        return rpc_result(id, tool_error_result(&format!("Unknown tool: {name}"), 400));
    };
    let payload = (tool.dispatch)(server, args).await;
    rpc_result(id, payload)
}

/// The `initialize` result: protocol version, capabilities, server identity.
fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": { "tools": {} },
        "serverInfo": {
            "name": SERVER_NAME,
            "version": env!("NANOBPM_VERSION"),
        },
    })
}

/// Wrap a successful JSON-RPC result.
fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": JSONRPC, "id": id, "result": result })
}

/// Wrap a JSON-RPC transport-level error (only for malformed frames / unknown
/// methods — never for a failed tool call, which is `isError` instead).
fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": JSONRPC,
        "id": id,
        "error": { "code": code, "message": message },
    })
}

/// Build a `CallToolResult` success payload: the REST response body as the
/// text content (Camunda serializes the result to JSON text).
pub(crate) fn tool_ok_result(body: &Value) -> Value {
    json!({
        "content": [ { "type": "text", "text": body.to_string() } ],
        "isError": false,
    })
}

/// Build a `CallToolResult` failure payload mirroring Camunda's
/// `CallToolResultMapper`: `isError: true` with the ProblemDetail as content.
pub(crate) fn tool_error_result(detail: &str, status: u16) -> Value {
    let problem = json!({
        "title": "MCP tool call failed",
        "status": status,
        "detail": detail,
    });
    json!({
        "content": [ { "type": "text", "text": problem.to_string() } ],
        "isError": true,
    })
}

/// Build an `isError` payload from an existing ProblemDetail body (the shape
/// the REST `*_impl` handlers already produce), preserving its fields.
pub(crate) fn tool_error_from_problem(problem: &Value) -> Value {
    json!({
        "content": [ { "type": "text", "text": problem.to_string() } ],
        "isError": true,
    })
}

/// Extract `(status, body)` from a generated REST response enum.
///
/// The response enums are externally tagged: serializing one yields
/// `{"Status200_…": body}` for tuple variants, or
/// `{"Status401_…": {"body": …, "www_authenticate": …}}` for struct variants.
/// This recovers the numeric status from the variant-name prefix and the
/// payload body, so the MCP layer maps 2xx → success content and anything else
/// → `isError` + ProblemDetail, exactly mirroring the REST wire semantics.
pub(crate) fn response_status_body(resp: &impl serde::Serialize) -> (u16, Value) {
    let v = serde_json::to_value(resp).unwrap_or(Value::Null);
    // Unit variants (e.g. a 204 No Content) serialize as the bare variant-name
    // string; recover the status from its `Status<NNN>_` prefix, with no body.
    if let Some(variant) = v.as_str() {
        let status = parse_status_prefix(variant).unwrap_or(200);
        return (status, Value::Null);
    }
    let Some(obj) = v.as_object() else {
        return (200, v);
    };
    let Some((variant, payload)) = obj.iter().next() else {
        return (200, Value::Null);
    };
    let status = parse_status_prefix(variant).unwrap_or(200);
    // Struct variants carry the body under a `body` key; tuple variants are the
    // body directly.
    let body = payload
        .as_object()
        .and_then(|p| p.get("body").cloned())
        .unwrap_or_else(|| payload.clone());
    (status, body)
}

/// Parse the numeric status from a `Status<NNN>_…` response-enum variant name.
fn parse_status_prefix(variant: &str) -> Option<u16> {
    variant
        .strip_prefix("Status")
        .and_then(|s| s.get(..3))
        .and_then(|s| s.parse::<u16>().ok())
}

/// Map a dispatched REST response to its MCP `CallToolResult`: 2xx → success,
/// non-2xx → `isError` + the ProblemDetail body.
pub(crate) fn map_rest_response(resp: &impl serde::Serialize) -> Value {
    let (status, body) = response_status_body(resp);
    if (200..300).contains(&status) {
        tool_ok_result(&body)
    } else {
        tool_error_from_problem(&body)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (unit tests)"
)]
mod tests {
    use nanobpmn_engine_core::ProcessBuilder;

    use super::*;
    use crate::mcp::cluster_tools::cluster_tool_servers;

    /// The vendored Camunda snapshot the parity guard compares against.
    const SNAPSHOT: &str = include_str!("../../tests/mcp/tools-schema-snapshot.json");

    fn cluster_server() -> ToolServer {
        cluster_tool_servers().remove(0)
    }

    /// `tools/list` returns exactly the 18 cluster tools, in snapshot order,
    /// with the right names and read-only annotations.
    #[test]
    fn tools_list_returns_the_18_cluster_tools() {
        let srv = cluster_server();
        let list = srv.tools_list();
        let tools = list["tools"].as_array().expect("tools is an array");
        assert_eq!(tools.len(), 18, "exactly 18 cluster tools");

        let expected_read_only = [
            "getClusterStatus",
            "getTopology",
            "getIncident",
            "searchIncidents",
            "getProcessDefinition",
            "getProcessDefinitionXml",
            "searchProcessDefinitions",
            "getProcessInstance",
            "searchProcessInstances",
            "getUserTask",
            "searchUserTaskVariables",
            "searchUserTasks",
            "getVariable",
            "searchVariables",
        ];
        for tool in tools {
            let name = tool["name"].as_str().unwrap();
            // title mirrors name.
            assert_eq!(tool["title"].as_str().unwrap(), name);
            let read_only = tool
                .get("annotations")
                .and_then(|a| a["readOnlyHint"].as_bool())
                .unwrap_or(false);
            assert_eq!(
                read_only,
                expected_read_only.contains(&name),
                "readOnlyHint for {name}"
            );
            // Every tool advertises an object input schema.
            assert_eq!(tool["inputSchema"]["type"].as_str().unwrap(), "object");
        }
    }

    /// The parity guard: Nano's `tools/list` must match Camunda's vendored
    /// snapshot exactly — tool set, names, descriptions, required params, and
    /// input-schema types. Any unexplained drift fails here.
    #[test]
    fn tools_list_matches_camunda_snapshot() {
        let snapshot: Value = serde_json::from_str(SNAPSHOT).expect("snapshot parses");
        let expected = snapshot["tools"].as_array().expect("snapshot tools array");

        let srv = cluster_server();
        let list = srv.tools_list();
        let actual = list["tools"].as_array().unwrap();

        assert_eq!(
            actual.len(),
            expected.len(),
            "tool count drift: nano={} camunda={}",
            actual.len(),
            expected.len()
        );

        for exp in expected {
            let name = exp["name"].as_str().unwrap();
            let Some(act) = actual.iter().find(|t| t["name"].as_str() == Some(name)) else {
                panic!("Nano is missing Camunda tool `{name}`");
            };
            assert_eq!(act["title"], exp["title"], "title drift on `{name}`");
            assert_eq!(
                act["description"], exp["description"],
                "description drift on `{name}`"
            );
            // Compare the advertised input schema (parsed, so key order and
            // whitespace do not matter).
            assert_eq!(
                act["inputSchema"], exp["inputSchema"],
                "inputSchema drift on `{name}`"
            );
            // Annotations: both either carry readOnlyHint:true or omit it.
            let ann = |t: &Value| {
                t.get("annotations")
                    .and_then(|a| a["readOnlyHint"].as_bool())
                    .unwrap_or(false)
            };
            assert_eq!(
                ann(act),
                ann(exp),
                "readOnlyHint annotation drift on `{name}`"
            );
        }
    }

    /// A read tool (`getTopology`) dispatches through the shared handler and
    /// returns the Camunda-shaped success result.
    #[tokio::test]
    async fn call_get_topology_dispatches_through_shared_handler() {
        let server = ServerImpl::default();
        let srv = cluster_server();
        let out = handle_tool_call(
            &server,
            &srv,
            json!(1),
            json!({ "name": "getTopology", "arguments": {} }),
        )
        .await;
        let result = &out["result"];
        assert_eq!(result["isError"].as_bool(), Some(false));
        let text = result["content"][0]["text"].as_str().unwrap();
        let body: Value = serde_json::from_str(text).expect("topology body is json");
        // The TopologyResponse shape from the shared get_topology_impl.
        assert!(body.get("brokers").is_some(), "topology has brokers");
        assert!(
            body.get("clusterSize").is_some(),
            "topology has clusterSize"
        );
    }

    /// A write tool (`createProcessInstance`) dispatches through the shared
    /// handler and returns the Camunda-shaped result carrying the new key.
    #[tokio::test]
    async fn call_create_process_instance_dispatches_through_shared_handler() {
        let server = ServerImpl::default();
        // Deploy a minimal process so the create has a definition to bind to.
        let proc = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .expect("valid process");
        let mut names = std::collections::HashMap::new();
        names.insert("p".to_string(), "p.bpmn".to_string());
        server
            .deploy_resources_locally(
                vec![proc],
                &names,
                Vec::new(),
                &std::collections::HashMap::new(),
                "<default>",
            )
            .await
            .expect("deploy succeeds");
        // Wait for the definition to be projected so the create can resolve it.
        let mut projected = false;
        for _ in 0..300 {
            if server
                .store
                .process_definitions()
                .iter()
                .any(|pd| pd.process_id == "p")
            {
                projected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(projected, "the deployed definition is projected");
        let def_key: u64 = server
            .store
            .process_definitions()
            .iter()
            .find(|pd| pd.process_id == "p")
            .map(|pd| pd.key)
            .expect("definition key");

        let srv = cluster_server();
        let out = handle_tool_call(
            &server,
            &srv,
            json!(1),
            json!({
                "name": "createProcessInstance",
                "arguments": { "processDefinitionKey": def_key.to_string() }
            }),
        )
        .await;
        let result = &out["result"];
        assert_eq!(
            result["isError"].as_bool(),
            Some(false),
            "create succeeds: {result}"
        );
        let text = result["content"][0]["text"].as_str().unwrap();
        let body: Value = serde_json::from_str(text).expect("create body is json");
        assert!(
            body.get("processInstanceKey").is_some(),
            "create returns the new processInstanceKey: {body}"
        );
    }

    /// A failed call (`getProcessInstance` for a missing key) returns
    /// `isError: true` with the ProblemDetail payload, not a transport error.
    #[tokio::test]
    async fn call_missing_process_instance_returns_is_error_problem_detail() {
        let server = ServerImpl::default();
        let srv = cluster_server();
        let out = handle_tool_call(
            &server,
            &srv,
            json!(1),
            json!({
                "name": "getProcessInstance",
                "arguments": { "processInstanceKey": 999999 }
            }),
        )
        .await;
        // The JSON-RPC frame itself succeeds (no transport `error`).
        assert!(out.get("error").is_none(), "no transport error");
        let result = &out["result"];
        assert_eq!(result["isError"].as_bool(), Some(true));
        let text = result["content"][0]["text"].as_str().unwrap();
        let problem: Value = serde_json::from_str(text).expect("problem detail is json");
        assert_eq!(problem["status"].as_u64(), Some(404));
        assert!(problem.get("title").is_some(), "ProblemDetail has a title");
    }

    /// An unknown tool name returns an `isError` result, not a transport error.
    #[tokio::test]
    async fn call_unknown_tool_returns_is_error() {
        let server = ServerImpl::default();
        let srv = cluster_server();
        let out = handle_tool_call(
            &server,
            &srv,
            json!(1),
            json!({ "name": "noSuchTool", "arguments": {} }),
        )
        .await;
        assert!(out.get("error").is_none());
        assert_eq!(out["result"]["isError"].as_bool(), Some(true));
    }

    /// `initialize` negotiates the protocol version and server identity.
    #[tokio::test]
    async fn initialize_returns_protocol_and_server_info() {
        let server = ServerImpl::default();
        let srv = cluster_server();
        let out = handle_frame(
            &server,
            &srv,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }),
        )
        .await;
        let result = &out["result"];
        assert_eq!(result["protocolVersion"].as_str(), Some(PROTOCOL_VERSION));
        assert_eq!(result["serverInfo"]["name"].as_str(), Some(SERVER_NAME));
    }

    /// The MCP endpoint is dark unless `NANOBPM_MCP_ENABLED` is truthy. This
    /// mirrors the REST surface's env-gate pattern (e.g. `NANOBPM_CORS`).
    #[test]
    fn mcp_enabled_gate_defaults_off() {
        // Unset → off. (The test env never sets NANOBPM_MCP_ENABLED.)
        assert!(!mcp_enabled(), "MCP is off by default");
    }

    /// The two operations Nano has not implemented yet still list (parity) and
    /// return the 501-shaped `isError` the REST stub produces.
    #[tokio::test]
    async fn unimplemented_ops_list_and_return_501_is_error() {
        let server = ServerImpl::default();
        let srv = cluster_server();
        for tool in ["getClusterStatus", "searchUserTaskVariables"] {
            assert!(srv.find(tool).is_some(), "{tool} is listed");
            let args = if tool == "searchUserTaskVariables" {
                json!({ "userTaskKey": 1 })
            } else {
                json!({})
            };
            let out = handle_tool_call(
                &server,
                &srv,
                json!(1),
                json!({ "name": tool, "arguments": args }),
            )
            .await;
            assert_eq!(out["result"]["isError"].as_bool(), Some(true), "{tool}");
            let text = out["result"]["content"][0]["text"].as_str().unwrap();
            let problem: Value = serde_json::from_str(text).unwrap();
            assert_eq!(problem["status"].as_u64(), Some(501), "{tool} is 501");
        }
    }
}
