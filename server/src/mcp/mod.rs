//! MCP (Model Context Protocol) gateway surface.
//!
//! This module implements Nano's Camunda-compatible MCP servers as a thin
//! **protocol adapter** over the existing REST handlers. It adds no business
//! logic: every tool call deserializes its arguments into the generated
//! `nanobpm-gateway-rest` spec types and dispatches through the same `*_impl`
//! handler the corresponding REST route uses, so the MCP surface can never
//! drift from the REST behaviour.
//!
//! Two servers are mounted (both behind the same auth check as the REST API —
//! [`mcp_auth`] applies the shared `ApiAuthBasic` credential extraction the
//! generated `/v2` handlers use — and the `NANOBPM_MCP_ENABLED` gate, default
//! on — set it to `false`/`0`/`off`/`no` to disable):
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
pub(crate) mod processes_tools;

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

/// The mount paths of every MCP server, in mount order — derived from the
/// registered [`ToolServer`]s so the banner/docs can never drift from what
/// [`mcp_router`] actually mounts.
pub(crate) fn mcp_endpoint_paths() -> Vec<&'static str> {
    cluster_tools::cluster_tool_servers()
        .into_iter()
        .chain(processes_tools::processes_tool_servers())
        .map(|s| s.path)
        .collect()
}

/// The startup-banner lines advertising the MCP endpoints under `base` (e.g.
/// `http://127.0.0.1:8080`). Empty when MCP is disabled, so the banner only
/// mentions endpoints that are actually mounted. (c8run prints only
/// `/mcp/cluster`; Nano prints both servers.)
#[cfg_attr(
    not(feature = "console"),
    allow(
        dead_code,
        reason = "the startup banner is printed by console builds only"
    )
)]
pub(crate) fn mcp_banner_lines(base: &str, enabled: bool) -> Vec<String> {
    if !enabled {
        return Vec::new();
    }
    mcp_endpoint_paths()
        .into_iter()
        .map(|path| format!("  MCP server     {base}{path}"))
        .collect()
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
    /// The static tools this server exposes, in `tools/list` order.
    pub(crate) tools: Vec<Tool>,
    /// An optional request-time tool resolver (the `/mcp/processes` per-process
    /// tools). Its tools list *before* the static ones (Camunda's
    /// `ProcessesToolRepository` order); a call resolves static tools first.
    pub(crate) dynamic: Option<DynamicTools>,
}

/// A request-time tool source: the tool set is resolved from engine state on
/// every `tools/list` / `tools/call` rather than fixed at mount time.
pub(crate) struct DynamicTools {
    /// The current dynamic tools as `tools/list` descriptors.
    pub(crate) list: fn(&ServerImpl) -> Vec<Value>,
    /// Execute a call against a dynamic tool name. Must return an `isError`
    /// result (never panic) for a name it does not resolve.
    pub(crate) call: for<'a> fn(&'a ServerImpl, String, Value) -> DispatchFuture<'a>,
}

impl ToolServer {
    fn find(&self, name: &str) -> Option<&Tool> {
        self.tools.iter().find(|t| t.name == name)
    }

    /// Build the `tools/list` result payload: dynamic tools (resolved now)
    /// followed by the static ones.
    fn tools_list(&self, server: &ServerImpl) -> Value {
        let mut tools: Vec<Value> = self
            .dynamic
            .as_ref()
            .map_or_else(Vec::new, |d| (d.list)(server));
        tools.extend(self.tools.iter().map(|t| {
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
        }));
        json!({ "tools": tools })
    }
}

/// Build the MCP axum router mounting every registered [`ToolServer`].
///
/// This is the seam `main.rs` calls once. The returned router serves each
/// server's `path` at HTTP POST. Slice 4 layers the `NANOBPM_MCP_ENABLED` gate
/// and startup banner on top of this single call site.
///
/// **Auth:** every mounted path is wrapped in [`mcp_auth`], which runs the
/// request through the *same* credential check the generated REST router
/// applies to each `/v2` operation —
/// [`apis::ApiAuthBasic::extract_claims_from_auth_header`] — and refuses the
/// request with `401` (and `WWW-Authenticate`) when it yields no claims. The
/// layer is applied to the whole MCP router here, so a future [`ToolServer`]
/// registered above is covered by construction and cannot bypass it; the
/// `every_mcp_route_sits_behind_auth` test guards that invariant.
pub(crate) fn mcp_router(server: ServerImpl) -> axum::Router {
    let mut app = axum::Router::new();
    let servers = cluster_tools::cluster_tool_servers()
        .into_iter()
        .chain(processes_tools::processes_tool_servers());
    for srv in servers {
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
    app.layer(axum::middleware::from_fn_with_state(server, mcp_auth))
}

/// The auth gate every MCP request passes through, mirroring the generated
/// REST router's per-operation check (`generated/src/server/mod.rs`): extract
/// claims from the `Authorization` header via the shared
/// [`apis::ApiAuthBasic`] implementation and reject with `401` when it refuses
/// the credentials. The `WWW-Authenticate` header advertises both schemes the
/// REST layer accepts (`basicAuth` and `bearerAuth` in `spec/rest-api.yaml`).
///
/// Today `ServerImpl`'s `ApiAuthBasic` accepts any credentials (the REST API
/// runs unsecured), so this passes exactly when `/v2` would — including with
/// no `Authorization` header at all. The parity is structural: when secured
/// mode is implemented in that one impl, both surfaces tighten together, and
/// an MCP route can never drift onto a weaker check.
async fn mcp_auth(
    axum::extract::State(server): axum::extract::State<ServerImpl>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use nanobpm_gateway_rest::apis::{ApiAuthBasic, BasicAuthKind};
    let claims = server
        .extract_claims_from_auth_header(BasicAuthKind::Bearer, req.headers(), "authorization")
        .await;
    mcp_auth_finish(claims.is_some(), req, next).await
}

/// The decision half of [`mcp_auth`], split out so the 401 mapping is testable
/// without a `ServerImpl` whose credentials are refused (the production impl
/// accepts everything today): `authorized == false` short-circuits with `401`
/// + `WWW-Authenticate`, exactly the generated router's `None`-claims branch.
async fn mcp_auth_finish(
    authorized: bool,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !authorized {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            [(
                axum::http::header::WWW_AUTHENTICATE,
                "Basic realm=\"nanobpm\", Bearer",
            )],
            "Unauthorized\n",
        )
            .into_response();
    }
    next.run(req).await
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
    match handle_message(server, srv, frame).await {
        Some(resp) => (StatusCode::OK, axum::Json(resp)).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

/// Upper bound on entries in one JSON-RPC batch, so a single POST cannot fan
/// out unbounded tool calls.
const MAX_BATCH: usize = 100;

/// Dispatch one HTTP message: a single frame, or (MCP 2025-03-26 requires
/// receiving them) a JSON-RPC batch. A batch's entries run in order; their
/// responses are aggregated into an array, and a batch of only notifications
/// yields `None` (HTTP 202). An empty or oversized batch is a single
/// `-32600`.
async fn handle_message(server: &ServerImpl, srv: &ToolServer, frame: Value) -> Option<Value> {
    let Value::Array(entries) = frame else {
        return handle_frame(server, srv, frame).await;
    };
    if entries.is_empty() {
        return Some(rpc_error(
            Value::Null,
            -32600,
            "Invalid Request: empty batch",
        ));
    }
    if entries.len() > MAX_BATCH {
        return Some(rpc_error(
            Value::Null,
            -32600,
            &format!("Invalid Request: batch exceeds {MAX_BATCH} entries"),
        ));
    }
    let mut out = Vec::new();
    for entry in entries {
        if let Some(resp) = handle_frame(server, srv, entry).await {
            out.push(resp);
        }
    }
    (!out.is_empty()).then_some(Value::Array(out))
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
        "tools/list" => rpc_result(id, srv.tools_list(server)),
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
    if let Some(tool) = srv.find(name) {
        return rpc_result(id, (tool.dispatch)(server, args).await);
    }
    if let Some(dynamic) = &srv.dynamic {
        return rpc_result(id, (dynamic.call)(server, name.to_string(), args).await);
    }
    rpc_result(id, tool_error_result(&format!("Unknown tool: {name}"), 400))
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
        let list = srv.tools_list(&ServerImpl::default());
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
        let list = srv.tools_list(&ServerImpl::default());
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

    /// Nano keys are `u64`: partition >= 4096 keys exceed `i64::MAX` and must
    /// parse in both numeric and string form; negatives/garbage are rejected.
    /// The advertised JSON Schema `integer` type also covers integral float
    /// representations (`3.0`, `3e0`), which serde stores as f64 — accept them,
    /// while genuinely fractional or negative floats stay rejected.
    #[test]
    fn arg_key_accepts_full_u64_key_range() {
        let big = 9_223_372_036_854_775_809_u64;
        for args in [json!({ "k": big }), json!({ "k": big.to_string() })] {
            assert_eq!(
                cluster_tools::arg_key(&args, "k").ok(),
                Some(big.to_string()),
                "{args}"
            );
        }
        for args in [json!({ "k": -1 }), json!({ "k": "x" }), json!({})] {
            assert!(cluster_tools::arg_key(&args, "k").is_err(), "{args}");
        }
    }

    /// Integral float representations of a key (`3.0`, `3e0`) are valid per
    /// the advertised JSON Schema `integer` type and must reach the handler;
    /// fractional/negative floats are not keys and stay rejected.
    #[test]
    fn arg_key_accepts_integral_float_representations() {
        for args in [json!({ "k": 3.0 }), json!({ "k": 3e0 })] {
            assert_eq!(
                cluster_tools::arg_key(&args, "k").ok(),
                Some("3".to_string()),
                "{args}"
            );
        }
        // Largest exactly-representable float key still accepted; exact
        // integer / string forms above 2^53 keep working.
        assert_eq!(
            cluster_tools::arg_key(&json!({ "k": 9007199254740991.0_f64 }), "k").ok(),
            Some("9007199254740991".to_string())
        );
        for args in [
            json!({ "k": 9007199254740993_u64 }),
            json!({ "k": "9007199254740993" }),
        ] {
            assert_eq!(
                cluster_tools::arg_key(&args, "k").ok(),
                Some("9007199254740993".to_string()),
                "{args}"
            );
        }
        for args in [
            json!({ "k": 3.5 }),
            json!({ "k": -3.0 }),
            json!({ "k": 1e300 }),
            // `u64::MAX as f64` rounds to 2^64: out of range, must not
            // saturate to a different key.
            json!({ "k": 18446744073709551616.0_f64 }),
            // At/above 2^53 an f64 may already have rounded the authored
            // literal to a different key: reject rather than mis-target.
            json!({ "k": 9007199254740992.0_f64 }),
            json!({ "k": 9007199254740993.0_f64 }),
            json!({ "k": 9007199254740992.5_f64 }),
        ] {
            assert!(cluster_tools::arg_key(&args, "k").is_err(), "{args}");
        }
    }

    /// Generated model validation runs on MCP bodies (it is skipped when the
    /// generated REST router is bypassed): more than 10 tags (`maxItems`)
    /// creates nothing.
    /// Non-object tool arguments (serde would read an array positionally) and
    /// a non-boolean `truncateValues` are rejected before dispatch too.
    #[tokio::test]
    async fn invalid_arguments_rejected_before_dispatch() {
        let server = ServerImpl::default();
        let proc = ProcessBuilder::new("v")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .expect("valid process");
        deploy_and_project(&server, proc).await;
        let tags: Vec<String> = (0..11).map(|i| format!("t{i}")).collect();
        let r = call(
            &server,
            "createProcessInstance",
            json!({ "processDefinitionId": "v", "tags": tags }),
        )
        .await;
        assert_eq!(r["isError"].as_bool(), Some(true), "{r}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            server.store.process_instances().is_empty(),
            "no instance created"
        );
        for (tool, args) in [
            ("createProcessInstance", json!(["v"])),
            ("searchProcessInstances", json!([{}])),
            ("searchVariables", json!({ "truncateValues": "false" })),
            ("searchVariables", json!({ "truncateValues": 0 })),
        ] {
            let r = call(&server, tool, args.clone()).await;
            assert_eq!(r["isError"].as_bool(), Some(true), "{tool} {args}: {r}");
        }
        let r = call(
            &server,
            "searchVariables",
            json!({ "truncateValues": false }),
        )
        .await;
        assert_eq!(r["isError"].as_bool(), Some(false), "{r}");
    }

    /// Non-object `completionOptions` (serde reads `[]` as an empty struct)
    /// is rejected and leaves the task open.
    #[tokio::test]
    async fn complete_rejects_non_object_completion_options() {
        let server = ServerImpl::default();
        let proc = ProcessBuilder::new("ct")
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
        // A custom `action` cannot be recorded yet: rejected, task left open.
        for opts in [
            json!([]),
            json!("x"),
            json!(3),
            json!({ "action": "approve" }),
        ] {
            let r = call(
                &server,
                "completeUserTask",
                json!({ "userTaskKey": task_key, "completionOptions": opts }),
            )
            .await;
            assert_eq!(r["isError"].as_bool(), Some(true), "{opts}: {r}");
            let text = r["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("completionOptions"), "{text}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let task = server
            .store
            .user_tasks()
            .into_iter()
            .find(|t| t.key == task_key)
            .expect("task row");
        assert_eq!(
            task.state,
            nanobpmn_engine_core::UserTaskState::Created,
            "task left open"
        );
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
            ("searchUserTasks", "processInstanceVariables", json!([])),
            ("searchUserTasks", "localVariables", json!([])),
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

    /// Unsupported filters nested inside a `$or` clause are rejected too —
    /// a nested clause must not bypass the top-level guard.
    #[tokio::test]
    async fn search_rejects_unsupported_filters_inside_or_clauses() {
        let server = ServerImpl::default();
        for (tool, key, val) in [
            ("searchUserTasks", "isSuspended", json!(true)),
            (
                "searchUserTasks",
                "localVariables",
                json!([{ "name": "missing", "value": "1" }]),
            ),
            ("searchUserTasks", "processInstanceVariables", json!([])),
            ("searchVariables", "protectionMode", json!("REDACT")),
        ] {
            let mut clause = json!({});
            clause[key] = val;
            let args = json!({ "filter": { "$or": [{}, clause] } });
            let result = call(&server, tool, args).await;
            assert_eq!(result["isError"].as_bool(), Some(true), "{tool}: {result}");
            let text = result["content"][0]["text"].as_str().unwrap();
            assert!(text.contains(key), "{key} named in error: {text}");
        }
    }

    /// Date ranges inside `$or` clauses are translated to the REST operator
    /// form (an untranslated `{from}` would be dropped and widen the clause).
    #[test]
    fn date_ranges_translated_inside_or_clauses() {
        let body: Option<nanobpm_gateway_rest::models::UserTaskSearchQuery> =
            cluster_tools::optional_body(json!({
                "filter": { "$or": [{ "dueDate": { "from": "2999-01-01T00:00:00Z" } }] }
            }))
            .expect("valid");
        let v = serde_json::to_value(body.expect("some")).expect("json");
        assert_eq!(
            v["filter"]["$or"][0]["dueDate"]["$gte"],
            json!("2999-01-01T00:00:00Z"),
            "{v}"
        );
    }

    /// Sort fields the shared REST sorter has no projection for are rejected
    /// (the sorter would silently fall back to key order and page wrongly).
    #[tokio::test]
    async fn search_rejects_unsupported_sort_fields() {
        let server = ServerImpl::default();
        for (tool, field) in [
            ("searchIncidents", "processDefinitionId"),
            ("searchIncidents", "jobKey"),
            ("searchProcessDefinitions", "resourceName"),
            ("searchProcessInstances", "startDate"),
            ("searchProcessInstances", "processDefinitionVersion"),
            ("searchUserTasks", "priority"),
            ("searchUserTasks", "dueDate"),
            ("searchUserTasks", "followUpDate"),
        ] {
            let args = json!({ "sort": [{ "field": field, "order": "DESC" }] });
            let result = call(&server, tool, args).await;
            assert_eq!(result["isError"].as_bool(), Some(true), "{tool}: {result}");
            let text = result["content"][0]["text"].as_str().unwrap();
            assert!(text.contains(field), "{field} named in error: {text}");
        }
        for (tool, field) in [
            ("searchIncidents", "creationTime"),
            ("searchProcessDefinitions", "version"),
            ("searchProcessInstances", "processDefinitionId"),
            ("searchUserTasks", "creationDate"),
        ] {
            let args = json!({ "sort": [{ "field": field, "order": "ASC" }] });
            let result = call(&server, tool, args).await;
            assert_eq!(result["isError"].as_bool(), Some(false), "{tool}: {result}");
        }
    }

    /// Every filter the MCP search tools advertise is either honoured by the
    /// shared REST predicate or rejected — never silently dropped (which would
    /// widen the search beyond the caller's restriction). These calls run
    /// against an empty store: a *silently ignored* restriction still returns
    /// success, so each assertion pairs "restriction accepted" with the
    /// behaviour that proves it was evaluated (an empty result set for a
    /// restriction nothing can match).
    #[tokio::test]
    async fn advertised_search_filters_never_silently_widen() {
        let server = ServerImpl::default();
        // A restriction nothing can match must yield an empty page, proving
        // the filter reached the predicate instead of being dropped.
        for (tool, filter) in [
            (
                "searchIncidents",
                json!({ "processDefinitionId": "no-such-process" }),
            ),
            (
                "searchIncidents",
                json!({ "creationTime": { "from": "2999-01-01T00:00:00Z" } }),
            ),
            (
                "searchProcessInstances",
                json!({ "processDefinitionName": "no-such-name" }),
            ),
            (
                "searchProcessInstances",
                json!({ "suspendedDate": { "from": "2999-01-01T00:00:00Z" } }),
            ),
            ("searchProcessInstances", json!({ "tags": ["no-such-tag"] })),
            ("searchUserTasks", json!({ "businessId": "no-such-order" })),
            ("searchUserTasks", json!({ "name": "no-such-task-name" })),
            ("searchUserTasks", json!({ "priority": 77 })),
            (
                "searchUserTasks",
                json!({ "processDefinitionId": "no-such-process" }),
            ),
            ("searchUserTasks", json!({ "elementInstanceKey": "12345" })),
            (
                "searchUserTasks",
                json!({ "dueDate": { "from": "2999-01-01T00:00:00Z" } }),
            ),
            ("searchUserTasks", json!({ "tags": ["no-such-tag"] })),
        ] {
            let result = call(&server, tool, json!({ "filter": filter })).await;
            assert_eq!(
                result["isError"].as_bool(),
                Some(false),
                "{tool} must accept the advertised filter: {result}"
            );
            let text = result["content"][0]["text"].as_str().unwrap();
            let body: Value = serde_json::from_str(text).expect("search body is json");
            assert_eq!(
                body["items"].as_array().map(Vec::len),
                Some(0),
                "{tool} with {filter}: a non-matching restriction must not return rows: {body}"
            );
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
        // Malformed options are rejected on the unassign path too, and the
        // assignment stays unchanged.
        for bad in [
            json!({ "userTaskKey": task_key, "assignee": null, "assignmentOptions": [] }),
            json!({ "userTaskKey": task_key, "assignmentOptions": "x" }),
            json!({ "userTaskKey": task_key, "assignmentOptions": { "allowOverride": "no" } }),
            // A custom `action` is not recorded yet: fail closed on both
            // the assign and unassign paths.
            json!({ "userTaskKey": task_key, "assignee": "bob", "assignmentOptions": { "action": "claim" } }),
            json!({ "userTaskKey": task_key, "assignee": null, "assignmentOptions": { "action": "drop" } }),
        ] {
            let r = call(&server, "assignUserTask", bad.clone()).await;
            assert_eq!(r["isError"].as_bool(), Some(true), "{bad}: {r}");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(assignee(&server).as_deref(), Some("alice"), "{bad}");
        }
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
            // A nested batch is not a valid entry inside a batch frame.
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

    /// HTTP-level JSON-RPC batches (MCP 2025-03-26): responses aggregate in
    /// order, notification-only batches get 202, empty/oversized are -32600.
    #[tokio::test]
    async fn http_batches() {
        let ct = [("content-type", "application/json")];
        let (st, body) = post(
            &ct,
            json!([
                { "jsonrpc": "2.0", "id": 1, "method": "ping" },
                { "jsonrpc": "2.0", "method": "notifications/initialized" },
                { "jsonrpc": "2.0", "id": 2, "method": "tools/list" },
                { "jsonrpc": "1.0", "id": 3, "method": "ping" },
            ]),
        )
        .await;
        assert_eq!(st, axum::http::StatusCode::OK);
        let v: Value = serde_json::from_slice(&body).unwrap();
        let arr = v.as_array().expect("batch response is an array");
        assert_eq!(arr.len(), 3, "{v}");
        assert_eq!(arr[0]["id"], json!(1));
        assert_eq!(arr[1]["id"], json!(2));
        assert!(arr[1]["result"]["tools"].is_array());
        assert_eq!(arr[2]["error"]["code"].as_i64(), Some(-32600));

        let (st, body) = post(
            &ct,
            json!([{ "jsonrpc": "2.0", "method": "notifications/initialized" }]),
        )
        .await;
        assert_eq!(st, axum::http::StatusCode::ACCEPTED);
        assert!(body.is_empty());

        let too_many: Vec<Value> = (0..=MAX_BATCH)
            .map(|i| json!({ "jsonrpc": "2.0", "id": i, "method": "ping" }))
            .collect();
        for batch in [json!([]), Value::Array(too_many)] {
            let (st, body) = post(&ct, batch).await;
            assert_eq!(st, axum::http::StatusCode::OK);
            let v: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["error"]["code"].as_i64(), Some(-32600), "{v}");
        }
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

    /// Auth parity, unsecured mode (the only mode the REST layer has today):
    /// `ServerImpl`'s `ApiAuthBasic` accepts any credentials, so `/v2` accepts
    /// a request with no, basic, or bearer credentials — and every MCP route
    /// must behave identically. When secured mode lands in that one impl, both
    /// surfaces tighten together; this test then pins the dev-mode contract.
    #[tokio::test]
    async fn every_mcp_route_sits_behind_auth() {
        let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });
        let app = mcp_router(ServerImpl::default());
        for path in mcp_endpoint_paths() {
            for (label, auth) in [
                ("no credentials", None),
                ("basic credentials", Some("Basic dXNlcjpwYXNz")),
                ("bearer credentials", Some("Bearer some-token")),
            ] {
                use tower::ServiceExt;
                let mut req = axum::http::Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json");
                if let Some(auth) = auth {
                    req = req.header("authorization", auth);
                }
                let resp = app
                    .clone()
                    .oneshot(req.body(axum::body::Body::from(frame.to_string())).unwrap())
                    .await
                    .expect("route responds");
                assert_eq!(
                    resp.status(),
                    axum::http::StatusCode::OK,
                    "{path} must accept a request exactly as unsecured /v2 does ({label})"
                );
            }
        }
    }

    /// The 401 branch of the MCP auth gate: when the shared credential
    /// extraction refuses a request (claims = `None`, the generated router's
    /// unsecured→secured switch), the MCP surface must answer `401` with a
    /// `WWW-Authenticate` challenge and never reach dispatch — the same
    /// contract the generated `/v2` handlers produce. `ServerImpl` accepts
    /// everything today, so this drives the decision half of `mcp_auth`
    /// directly; `every_mcp_route_sits_behind_auth` covers the wired layer.
    #[tokio::test]
    async fn refused_credentials_get_401_with_challenge() {
        use tower::ServiceExt;
        // An authorized control request passes through to the inner service.
        let app = axum::Router::new().route(
            "/probe",
            axum::routing::post(|| async { axum::http::StatusCode::OK }),
        );
        let app = app.layer(axum::middleware::from_fn(|req, next| {
            mcp_auth_finish(true, req, next)
        }));
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/probe")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .expect("route responds");
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        // A refused request is short-circuited with 401 + challenge.
        let app = axum::Router::new().route(
            "/probe",
            axum::routing::post(|| async { axum::http::StatusCode::OK }),
        );
        let app = app.layer(axum::middleware::from_fn(|req, next| {
            mcp_auth_finish(false, req, next)
        }));
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/probe")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .expect("route responds");
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
        let challenge = resp
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(challenge.contains("Basic"), "{challenge}");
        assert!(challenge.contains("Bearer"), "{challenge}");
    }

    /// A non-loopback `Origin` stays rejected (403) even when the request
    /// carries credentials the auth layer accepts — the browser hardening is
    /// defence in depth *on top of* auth, not a replacement for it.
    #[tokio::test]
    async fn foreign_origin_rejected_even_with_valid_credentials() {
        let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });
        for auth in ["Basic dXNlcjpwYXNz", "Bearer some-token"] {
            let (st, _) = post(
                &[
                    ("content-type", "application/json"),
                    ("authorization", auth),
                    ("origin", "https://evil.example"),
                ],
                frame.clone(),
            )
            .await;
            assert_eq!(
                st,
                axum::http::StatusCode::FORBIDDEN,
                "{auth}: a foreign Origin must still 403"
            );
        }
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

    /// The banner advertises BOTH MCP endpoints when enabled and neither when
    /// disabled; the paths are the ones the router mounts.
    #[test]
    fn banner_lists_both_endpoints_only_when_enabled() {
        assert_eq!(mcp_endpoint_paths(), vec!["/mcp/cluster", "/mcp/processes"]);
        let on = mcp_banner_lines("http://127.0.0.1:8080", true);
        assert_eq!(
            on,
            vec![
                "  MCP server     http://127.0.0.1:8080/mcp/cluster".to_string(),
                "  MCP server     http://127.0.0.1:8080/mcp/processes".to_string(),
            ]
        );
        assert!(mcp_banner_lines("http://127.0.0.1:8080", false).is_empty());
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
