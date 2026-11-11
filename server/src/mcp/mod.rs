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
//! `NANOBPM_MCP_ENABLED` gate, default on — set it to `false`/`0`/`off`/`no`
//! to disable):
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

/// The env gate (default **on** — operator decision, epic #1499). Slice 4 owns
/// the full config/docs/banner; this slice reads the key so the server is live
/// unless explicitly disabled. Accepts the usual falsy spellings to opt out.
pub(crate) const MCP_ENABLED_ENV: &str = "NANOBPM_MCP_ENABLED";

/// Whether the MCP surface is enabled. **On by default**; only an explicit
/// falsy value — `0`, `false`, `no`, `off` (case-insensitive) — disables it.
/// Unset or anything else leaves it on.
pub(crate) fn mcp_enabled() -> bool {
    !matches!(
        std::env::var(MCP_ENABLED_ENV)
            .map(|v| v.to_ascii_lowercase())
            .as_deref(),
        Ok("0" | "false" | "no" | "off")
    )
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
            axum::routing::post(
                move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                    let server = server.clone();
                    let srv = srv.clone();
                    async move { handle_http(&server, &srv, &headers, &body).await }
                },
            ),
        );
    }
    app
}

/// Reject a request whose transport metadata is unsafe before any parsing or
/// dispatch happens. Two checks, both fail-closed:
///
/// - **Content-Type must be `application/json`.** A hostile page can send a
///   CORS-"simple" `text/plain` POST without a preflight; requiring JSON forces
///   a preflight, which the gateway's CORS policy then governs.
/// - **Origin, when present, must be loopback.** Browsers always
///   attach `Origin` to cross-origin POSTs, so this blocks DNS-rebinding /
///   drive-by pages from reaching an unauthenticated local gateway. Non-browser
///   MCP clients send no `Origin` and are unaffected.
fn check_transport(
    headers: &axum::http::HeaderMap,
) -> Result<(), (axum::http::StatusCode, &'static str)> {
    use axum::http::{StatusCode, header};
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let mime = content_type.split(';').next().unwrap_or("").trim();
    if !mime.eq_ignore_ascii_case("application/json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        ));
    }
    if let Some(origin) = headers.get(header::ORIGIN)
        && !origin_allowed(origin.to_str().unwrap_or(""))
    {
        return Err((StatusCode::FORBIDDEN, "Origin not allowed"));
    }
    Ok(())
}

/// An `Origin` is allowed iff its authority is a loopback host. The request's
/// `Host` header is deliberately NOT trusted: under DNS rebinding an attacker
/// controls both `Origin` and `Host` (`attacker.example` → gateway IP), so
/// header equality proves nothing. Anything unparseable (incl. `null`) is
/// rejected.
fn origin_allowed(origin: &str) -> bool {
    let Some(authority) = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    else {
        return false;
    };
    if authority.is_empty() || authority.contains('/') {
        return false;
    }
    let hostname = if let Some(rest) = authority.strip_prefix('[') {
        match rest.split_once(']') {
            Some((h, _)) => h,
            None => return false,
        }
    } else {
        authority.split(':').next().unwrap_or("")
    };
    matches!(
        hostname.to_ascii_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1"
    )
}

/// The HTTP entry point: check transport metadata, parse the JSON-RPC frame,
/// dispatch, serialize the response. A stateless POST → single JSON-RPC
/// response (no SSE stream); a notification → HTTP 202 with no body.
async fn handle_http(
    server: &ServerImpl,
    srv: &ToolServer,
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    if let Err((status, msg)) = check_transport(headers) {
        return (status, msg).into_response();
    }
    let frame: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(rpc_error(Value::Null, -32700, &format!("Parse error: {e}"))),
            )
                .into_response();
        }
    };
    match handle_frame(server, srv, frame).await {
        Some(resp) => (StatusCode::OK, axum::Json(resp)).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

/// A validated JSON-RPC 2.0 envelope. `id` is `None` for a notification.
struct Envelope {
    id: Option<Value>,
    method: String,
    params: Value,
}

/// Validate a JSON-RPC 2.0 request envelope: an object with
/// `jsonrpc: "2.0"`, a string `method`, and (if present) a string or integer
/// `id`. Anything else is `-32600 Invalid Request` — never dispatched.
fn parse_envelope(frame: Value) -> Result<Envelope, Value> {
    let invalid = |msg: &str| rpc_error(Value::Null, -32600, &format!("Invalid Request: {msg}"));
    let Value::Object(mut obj) = frame else {
        return Err(invalid("frame must be a JSON object"));
    };
    if obj.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC) {
        return Err(invalid("`jsonrpc` must be \"2.0\""));
    }
    let id = match obj.remove("id") {
        None => None,
        Some(v @ Value::String(_)) => Some(v),
        Some(v @ Value::Number(_)) if v.is_i64() || v.is_u64() => Some(v),
        Some(_) => return Err(invalid("`id` must be a string or integer")),
    };
    let Some(Value::String(method)) = obj.remove("method") else {
        return Err(match id {
            Some(id) => rpc_error(id, -32600, "Invalid Request: `method` must be a string"),
            None => invalid("`method` must be a string"),
        });
    };
    let params = obj.remove("params").unwrap_or(Value::Null);
    Ok(Envelope { id, method, params })
}

/// Dispatch one parsed JSON-RPC frame. Returns `None` for a notification
/// (no `id`): notifications are accepted and never answered.
async fn handle_frame(server: &ServerImpl, srv: &ToolServer, frame: Value) -> Option<Value> {
    let env = match parse_envelope(frame) {
        Ok(env) => env,
        Err(err) => return Some(err),
    };
    // A notification never gets a response, whatever its method. Only
    // side-effect-free notifications are defined for this server, so none is
    // dispatched (in particular a `tools/call` without an id is NOT executed).
    let id = env.id?;
    Some(match env.method.as_str() {
        "initialize" => rpc_result(id, initialize_result()),
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(id, srv.tools_list()),
        "tools/call" => handle_tool_call(server, srv, id, env.params).await,
        other => rpc_error(id, -32601, &format!("Method not found: {other}")),
    })
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

    /// A guard serialising tests that mutate the process environment (cargo
    /// runs them in one process with shared env). Holds a static lock for the
    /// guard's lifetime and restores the variable's prior value on drop.
    struct EnvVarGuard {
        key: &'static str,
        prior: Option<std::ffi::OsString>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvVarGuard {
        /// Set `NANOBPM_MCP_ENABLED` to `value` for the closure's duration.
        fn with_set<R>(self, value: &str, f: impl FnOnce() -> R) -> R {
            // SAFETY: the guard holds the env lock, so no other test thread
            // mutates the environment concurrently.
            unsafe { std::env::set_var(self.key, value) };
            f()
        }

        /// Unset `NANOBPM_MCP_ENABLED` for the closure's duration.
        fn with_unset<R>(self, f: impl FnOnce() -> R) -> R {
            // SAFETY: the guard holds the env lock, so no other test thread
            // mutates the environment concurrently.
            unsafe { std::env::remove_var(self.key) };
            f()
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: the guard still holds the env lock at drop time.
            match &self.prior {
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    /// Take the env lock and capture `NANOBPM_MCP_ENABLED`'s current value so
    /// the guard can restore it on drop.
    fn env_lock() -> EnvVarGuard {
        static L: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        let lock = L
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        EnvVarGuard {
            key: MCP_ENABLED_ENV,
            prior: std::env::var_os(MCP_ENABLED_ENV),
            _lock: lock,
        }
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

    /// Deploy `proc` and wait until its definition is projected; returns its key.
    async fn deploy_and_project(
        server: &ServerImpl,
        proc: nanobpmn_engine_core::ProcessDefinition,
    ) -> u64 {
        let id = proc.id.clone();
        let mut names = std::collections::HashMap::new();
        names.insert(id.clone(), format!("{id}.bpmn"));
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
        for _ in 0..300 {
            if let Some(pd) = server
                .store
                .process_definitions()
                .iter()
                .find(|pd| pd.process_id == id)
            {
                return pd.key;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("definition `{id}` never projected");
    }

    async fn call(server: &ServerImpl, name: &str, args: Value) -> Value {
        let srv = cluster_server();
        let out = handle_tool_call(
            server,
            &srv,
            json!(1),
            json!({ "name": name, "arguments": args }),
        )
        .await;
        out["result"].clone()
    }

    /// Advertised-but-unsupported creation options are rejected, not dropped.
    #[tokio::test]
    async fn create_rejects_unsupported_options_instead_of_dropping() {
        let server = ServerImpl::default();
        for (key, val) in [
            ("processDefinitionVersionTag", json!("v1")),
            ("variablesMetadata", json!([{ "name": "x" }])),
        ] {
            let mut args = json!({ "processDefinitionId": "p" });
            args[key] = val;
            let result = call(&server, "createProcessInstance", args).await;
            assert_eq!(result["isError"].as_bool(), Some(true), "{key}: {result}");
            let text = result["content"][0]["text"].as_str().unwrap();
            assert!(text.contains(key), "{key} named in error: {text}");
        }
    }

    /// Advertised-but-unsupported search filters fail closed, never widen.
    #[tokio::test]
    async fn search_rejects_unsupported_filters_instead_of_dropping() {
        let server = ServerImpl::default();
        for (tool, key, val) in [
            ("searchUserTasks", "isSuspended", json!(true)),
            ("searchVariables", "protectionMode", json!("REDACT")),
        ] {
            let mut filter = json!({});
            filter[key] = val;
            let result = call(&server, tool, json!({ "filter": filter })).await;
            assert_eq!(result["isError"].as_bool(), Some(true), "{tool}: {result}");
            let text = result["content"][0]["text"].as_str().unwrap();
            assert!(text.contains(key), "{key} named in error: {text}");
        }
    }

    /// A non-object `assignmentOptions` is rejected, not ignored (ignoring it
    /// would default `allowOverride` to true).
    #[tokio::test]
    async fn assign_rejects_non_object_assignment_options() {
        let server = ServerImpl::default();
        for opts in [json!([{ "allowOverride": false }]), json!("x"), json!(1)] {
            let result = call(
                &server,
                "assignUserTask",
                json!({ "userTaskKey": "1", "assignee": "bob", "assignmentOptions": opts }),
            )
            .await;
            assert_eq!(result["isError"].as_bool(), Some(true), "{opts}: {result}");
            let text = result["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("assignmentOptions"), "{text}");
        }
    }

    /// A null/omitted assignee unassigns (it does not store `""`), so a later
    /// `allowOverride: false` assignment succeeds.
    #[tokio::test]
    async fn null_assignee_unassigns_user_task() {
        let server = ServerImpl::default();
        let proc = ProcessBuilder::new("ut")
            .start_event("s")
            .user_task("t")
            .end_event("e")
            .connect("s", "t")
            .connect("t", "e")
            .build()
            .expect("valid process");
        let def_key = deploy_and_project(&server, proc).await;
        let created = call(
            &server,
            "createProcessInstance",
            json!({ "processDefinitionKey": def_key.to_string() }),
        )
        .await;
        assert_eq!(created["isError"].as_bool(), Some(false), "{created}");
        let mut task_key = None;
        for _ in 0..300 {
            if let Some(t) = server.store.user_tasks().first() {
                task_key = Some(t.key);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let task_key = task_key.expect("user task projected");
        let assignee = |server: &ServerImpl| {
            server
                .store
                .user_tasks()
                .iter()
                .find(|t| t.key == task_key)
                .and_then(|t| t.assignee.clone())
        };
        let r = call(
            &server,
            "assignUserTask",
            json!({ "userTaskKey": task_key, "assignee": "alice" }),
        )
        .await;
        assert_eq!(r["isError"].as_bool(), Some(false), "{r}");
        for unassign_args in [
            json!({ "userTaskKey": task_key, "assignee": null }),
            json!({ "userTaskKey": task_key }),
        ] {
            let r = call(&server, "assignUserTask", unassign_args).await;
            assert_eq!(r["isError"].as_bool(), Some(false), "{r}");
            let mut cleared = false;
            for _ in 0..300 {
                if assignee(&server).is_none() {
                    cleared = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            assert!(cleared, "assignee cleared, got {:?}", assignee(&server));
            let r = call(
                &server,
                "assignUserTask",
                json!({ "userTaskKey": task_key, "assignee": "bob",
                        "assignmentOptions": { "allowOverride": false } }),
            )
            .await;
            assert_eq!(
                r["isError"].as_bool(),
                Some(false),
                "reassign after unassign: {r}"
            );
        }
    }

    /// `{from, to}` date ranges become `$gte`/`$lt` on every search tool's
    /// filter, so the bounds reach the shared REST filter instead of vanishing.
    #[test]
    fn date_ranges_translate_to_rest_operators() {
        let args = json!({ "filter": { "startDate": { "from": "2024-01-01T00:00:00Z", "to": "2024-02-01T00:00:00Z" } } });
        let q: Option<nanobpm_gateway_rest::models::ProcessInstanceSearchQuery> =
            cluster_tools::optional_body(args).expect("valid");
        let v = serde_json::to_value(q.expect("some")).unwrap();
        let sd = &v["filter"]["startDate"];
        assert!(sd["$gte"].is_string(), "{v}");
        assert!(sd["$lt"].is_string(), "{v}");
        for (tool_args, field) in [
            (
                json!({ "filter": { "creationTime": { "from": "2024-01-01T00:00:00Z" } } }),
                "creationTime",
            ),
            (
                json!({ "filter": { "dueDate": { "to": "2024-01-01T00:00:00Z" } } }),
                "dueDate",
            ),
        ] {
            let mut a = tool_args;
            cluster_tools::translate_date_ranges(&mut a["filter"]).expect("valid");
            let obj = a["filter"][field].as_object().unwrap();
            assert!(obj.keys().all(|k| k == "$gte" || k == "$lt"), "{a}");
        }
        let mut bad = json!({ "startDate": { "from": 5 } });
        assert!(cluster_tools::translate_date_ranges(&mut bad).is_err());
        let mut bad = json!({ "startDate": { "from": "2024-01-01T00:00:00Z", "x": 1 } });
        assert!(cluster_tools::translate_date_ranges(&mut bad).is_err());
    }

    /// Malformed envelopes are `-32600` and never dispatched.
    #[tokio::test]
    async fn malformed_envelopes_are_invalid_request() {
        let server = ServerImpl::default();
        let srv = cluster_server();
        for frame in [
            json!({ "jsonrpc": "1.0", "id": 1, "method": "tools/list" }),
            json!({ "id": 1, "method": "tools/list" }),
            json!({ "jsonrpc": "2.0", "id": {}, "method": "tools/list" }),
            json!({ "jsonrpc": "2.0", "id": 1.5, "method": "tools/list" }),
            json!({ "jsonrpc": "2.0", "id": 1, "method": 7 }),
            json!({ "jsonrpc": "2.0", "id": 1 }),
            json!([{ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }]),
            json!({ "jsonrpc": "1.0", "id": 1, "method": "tools/call",
                    "params": { "name": "getTopology", "arguments": {} } }),
        ] {
            let out = handle_frame(&server, &srv, frame.clone())
                .await
                .expect("error response");
            assert_eq!(
                out["error"]["code"].as_i64(),
                Some(-32600),
                "{frame} -> {out}"
            );
            assert!(out.get("result").is_none(), "{frame} not dispatched");
        }
    }

    /// Notifications (no `id`) — known or unknown — get no JSON-RPC response.
    #[tokio::test]
    async fn notifications_get_no_response() {
        let server = ServerImpl::default();
        let srv = cluster_server();
        for method in [
            "notifications/initialized",
            "notifications/whatever",
            "tools/call",
        ] {
            let out =
                handle_frame(&server, &srv, json!({ "jsonrpc": "2.0", "method": method })).await;
            assert!(out.is_none(), "{method} answered: {out:?}");
        }
    }

    async fn post(headers: &[(&str, &str)], body: Value) -> (axum::http::StatusCode, Vec<u8>) {
        use tower::ServiceExt;
        let app = mcp_router(ServerImpl::default());
        let mut req = axum::http::Request::builder()
            .method("POST")
            .uri("/mcp/cluster");
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let resp = app
            .oneshot(req.body(axum::body::Body::from(body.to_string())).unwrap())
            .await
            .expect("route responds");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, bytes.to_vec())
    }

    /// HTTP-level: the MCP initialize handshake — request 200 with a body,
    /// `notifications/initialized` 202 with no body.
    #[tokio::test]
    async fn http_initialize_handshake() {
        let json_ct = [("content-type", "application/json")];
        let (st, body) = post(
            &json_ct,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }),
        )
        .await;
        assert_eq!(st, axum::http::StatusCode::OK);
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            v["result"]["protocolVersion"].as_str(),
            Some(PROTOCOL_VERSION)
        );
        let (st, body) = post(
            &json_ct,
            json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        )
        .await;
        assert_eq!(st, axum::http::StatusCode::ACCEPTED);
        assert!(body.is_empty(), "no body for a notification");
    }

    /// HTTP-level: non-JSON content types and foreign Origins are rejected
    /// before dispatch; loopback / absent Origin pass. A non-loopback Origin
    /// matching the (attacker-controlled) Host is rejected (DNS rebinding).
    #[tokio::test]
    async fn http_rejects_non_json_and_foreign_origin() {
        let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });
        for ct in [
            None,
            Some("text/plain"),
            Some("application/x-www-form-urlencoded"),
            Some("multipart/form-data"),
        ] {
            let headers: Vec<(&str, &str)> = ct.map(|c| ("content-type", c)).into_iter().collect();
            let (st, _) = post(&headers, frame.clone()).await;
            assert_eq!(st, axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE, "{ct:?}");
        }
        for origin in [
            "https://evil.example",
            "null",
            "http://localhost.evil.example",
            "http://127.0.0.1.evil:80",
        ] {
            let (st, _) = post(
                &[
                    ("content-type", "application/json"),
                    ("origin", origin),
                    ("host", "localhost:8080"),
                ],
                frame.clone(),
            )
            .await;
            assert_eq!(st, axum::http::StatusCode::FORBIDDEN, "{origin}");
        }
        // DNS rebinding: Origin and Host both attacker-controlled and equal.
        let (st, _) = post(
            &[
                ("content-type", "application/json"),
                ("origin", "http://attacker.example:8080"),
                ("host", "attacker.example:8080"),
            ],
            frame.clone(),
        )
        .await;
        assert_eq!(st, axum::http::StatusCode::FORBIDDEN, "rebinding");
        for origin in [
            "http://localhost:3000",
            "http://127.0.0.1:8080",
            "http://[::1]:9",
        ] {
            let (st, _) = post(
                &[
                    ("content-type", "application/json; charset=utf-8"),
                    ("origin", origin),
                    ("host", "gw.example:8080"),
                ],
                frame.clone(),
            )
            .await;
            assert_eq!(st, axum::http::StatusCode::OK, "{origin}");
        }
        let (st, _) = post(&[("content-type", "application/json")], frame).await;
        assert_eq!(
            st,
            axum::http::StatusCode::OK,
            "no Origin (non-browser client)"
        );
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
        .await
        .expect("a request gets a response");
        let result = &out["result"];
        assert_eq!(result["protocolVersion"].as_str(), Some(PROTOCOL_VERSION));
        assert_eq!(result["serverInfo"]["name"].as_str(), Some(SERVER_NAME));
    }

    /// The MCP endpoint is on by default; only an explicit falsy
    /// `NANOBPM_MCP_ENABLED` value (`false`/`0`/`off`/`no`, case-insensitive)
    /// disables it. This mirrors the REST surface's env-gate pattern (e.g.
    /// `NANOBPM_CORS`, which is likewise on unless set to `off`).
    ///
    /// Mutates the process env, so it serialises on a local lock — cargo runs
    /// tests in one process with shared env.
    #[test]
    fn mcp_enabled_gate_defaults_on_with_explicit_opt_out() {
        env_lock().with_unset(|| assert!(mcp_enabled(), "MCP is on by default (unset)"));
        for truthy in ["1", "true", "TRUE", "yes", "on", "enabled", ""] {
            env_lock().with_set(truthy, || {
                assert!(mcp_enabled(), "MCP stays on for {truthy:?}")
            });
        }
        for falsy in ["false", "FALSE", "0", "off", "OFF", "no", "No"] {
            env_lock().with_set(falsy, || {
                assert!(!mcp_enabled(), "MCP is off for {falsy:?}")
            });
        }
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
