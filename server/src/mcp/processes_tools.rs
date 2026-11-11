//! The `/mcp/processes` MCP server (Camunda 8.10 `gateway-mcp/processes`).
//!
//! Unlike `/mcp/cluster`'s fixed list, this server's tool set is **dynamic**:
//! one tool per open message start event subscription whose start event
//! declares an `io.camunda.tool:name` `zeebe:property`, resolved from the read
//! model at request time (never cached at mount). Mirrors Camunda's
//! `ProcessesToolRepository`:
//!
//! - **name** — `<toolName>_<messageSubscriptionKey>`; **title** — `toolName`.
//! - **description** — the `purpose` / `inputs` / `when_to_use` /
//!   `when_not_to_use` / `results` properties joined with Camunda's labels.
//! - **inputSchema** — the reserved optional `businessId`, plus one property
//!   per `io.camunda.tool:input_N_{name,type,description,required}` group.
//! - **outputSchema** — `{processInstanceKey}`.
//! - **call** — correlates the start message with a random-UUID correlation
//!   key through the SAME `correlate_message_impl` the REST `correlateMessage`
//!   route uses; every argument except `businessId` becomes a process variable.
//!
//! The three static tools (`searchIncidents`, `getProcessInstance`,
//! `searchVariables`) are the cluster server's own [`Tool`]s, shared via
//! [`shared_cluster_tools`] — not copies.

use std::collections::{BTreeMap, HashSet};

use nanobpm_gateway_rest::models;
// The tool-metadata property prefix (single source: engine-core).
use nanobpmn_engine_core::TOOL_PROPERTY_PREFIX;
use serde_json::{Map, Value, json};

use super::cluster_tools::shared_cluster_tools;
use super::{
    DynamicTools, ToolServer, response_status_body, tool_error_from_problem, tool_error_result,
};
use crate::ServerImpl;

/// The mount path of the processes server.
pub(crate) const PROCESSES_PATH: &str = "/mcp/processes";

/// The static tools the processes server exposes alongside the dynamic ones
/// (Camunda `ProcessesMcpServerStaticToolsTest`).
pub(crate) const PROCESSES_STATIC_TOOLS: [&str; 3] =
    ["searchIncidents", "getProcessInstance", "searchVariables"];

/// The reserved, non-variable tool argument.
const BUSINESS_ID_ARGUMENT: &str = "businessId";
/// Separates the tool name from the subscription key in a tool's full name.
const TOOL_NAME_DELIMITER: char = '_';

/// Description parts in order: `(property suffix, label)`; an empty label
/// means the value is emitted bare (Camunda `DESCRIPTION_PARTS`).
const DESCRIPTION_PARTS: [(&str, &str); 5] = [
    ("purpose", ""),
    ("inputs", "## Inputs"),
    ("when_to_use", "## When to use"),
    ("when_not_to_use", "## When not to use"),
    ("results", "## Results"),
];

const BUSINESS_ID_DESCRIPTION: &str = "Optional business id that enforces uniqueness of the \
     started process instance. While another instance of this process started with the same \
     business id is active, starting another one is rejected. All other arguments are passed as \
     process variables.";

/// Register the `/mcp/processes` tool server on the MCP seam.
pub(crate) fn processes_tool_servers() -> Vec<ToolServer> {
    vec![ToolServer {
        path: PROCESSES_PATH,
        tools: shared_cluster_tools(&PROCESSES_STATIC_TOOLS),
        dynamic: Some(DynamicTools {
            list: list_tools,
            call: |s, name, args| Box::pin(call_tool(s, name, args)),
        }),
    }]
}

/// One resolved process tool.
struct ProcessTool {
    /// `<toolName>_<subscriptionKey>`.
    name: String,
    tool_name: String,
    message_name: String,
    properties: BTreeMap<String, String>,
}

/// The open tool subscriptions: message start subscriptions that carry a
/// tool name and belong to the **latest** version of their process (a start
/// subscription of a superseded version is closed in Zeebe, so its tool is
/// gone even when the newer version no longer declares the message start).
fn open_tools(server: &ServerImpl) -> Vec<ProcessTool> {
    let latest: HashSet<u64> = server
        .store
        .process_definitions()
        .into_iter()
        .filter(|d| d.is_latest)
        .map(|d| d.key)
        .collect();
    let mut tools: Vec<ProcessTool> = server
        .store
        .message_start_subscriptions()
        .into_iter()
        .filter(|s| latest.contains(&s.process_definition_key))
        .filter_map(|s| {
            let tool_name = s.tool_name.filter(|n| !n.is_empty())?;
            let properties = serde_json::from_str::<BTreeMap<String, String>>(&s.tool_properties)
                .unwrap_or_default();
            Some(ProcessTool {
                name: format!("{tool_name}{TOOL_NAME_DELIMITER}{}", s.subscription_key),
                tool_name,
                message_name: s.message_name,
                properties,
            })
        })
        .collect();
    tools.sort_by(|a, b| a.name.cmp(&b.name));
    tools
}

fn prop<'a>(props: &'a BTreeMap<String, String>, suffix: &str) -> Option<&'a str> {
    props
        .get(&format!("{TOOL_PROPERTY_PREFIX}{suffix}"))
        .map(String::as_str)
        .filter(|v| !v.trim().is_empty())
}

/// Assemble the description from the tool properties (Camunda
/// `buildDescription`): present, non-blank parts joined by a blank line.
pub(crate) fn build_description(props: &BTreeMap<String, String>) -> String {
    DESCRIPTION_PARTS
        .iter()
        .filter_map(|(suffix, label)| {
            prop(props, suffix).map(|v| {
                if label.is_empty() {
                    v.to_string()
                } else {
                    format!("{label}\n{v}")
                }
            })
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Build the input schema: `businessId` plus every complete
/// `input_N_{name,type,description,required}` group, in `N` order.
pub(crate) fn build_input_schema(props: &BTreeMap<String, String>) -> Value {
    let mut indexes: Vec<u32> = props
        .keys()
        .filter_map(|k| k.strip_prefix(TOOL_PROPERTY_PREFIX)?.strip_prefix("input_"))
        .filter_map(|rest| rest.split_once('_')?.0.parse().ok())
        .collect();
    indexes.sort_unstable();
    indexes.dedup();

    let mut properties = Map::new();
    let mut required = Vec::new();
    for n in indexes {
        let Some(name) = prop(props, &format!("input_{n}_name")) else {
            continue;
        };
        if name == BUSINESS_ID_ARGUMENT {
            // Reserved argument: never shadowed by a declared input.
            continue;
        }
        let mut schema = Map::new();
        if let Some(ty) = prop(props, &format!("input_{n}_type")) {
            schema.insert("type".into(), json!(ty));
        }
        if let Some(desc) = prop(props, &format!("input_{n}_description")) {
            schema.insert("description".into(), json!(desc));
        }
        if prop(props, &format!("input_{n}_required"))
            .is_some_and(|r| r.trim().eq_ignore_ascii_case("true"))
        {
            required.push(json!(name));
        }
        properties.insert(name.to_string(), Value::Object(schema));
    }
    properties.insert(
        BUSINESS_ID_ARGUMENT.into(),
        json!({ "type": "string", "description": BUSINESS_ID_DESCRIPTION }),
    );
    let mut schema = json!({ "type": "object", "properties": properties });
    if !required.is_empty() {
        schema["required"] = Value::Array(required);
    }
    schema
}

fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "processInstanceKey": {
                "type": "integer",
                "format": "int64",
                "description": "The key of the started process instance. Use this to \
                    investigate the state of the started instance.",
            }
        },
        "required": ["processInstanceKey"],
    })
}

fn descriptor(tool: &ProcessTool) -> Value {
    json!({
        "name": tool.name,
        "title": tool.tool_name,
        "description": build_description(&tool.properties),
        "inputSchema": build_input_schema(&tool.properties),
        "outputSchema": output_schema(),
    })
}

/// `tools/list` provider: the current dynamic tools.
fn list_tools(server: &ServerImpl) -> Vec<Value> {
    open_tools(server).iter().map(descriptor).collect()
}

/// `tools/call` for a dynamic tool name.
async fn call_tool(server: &ServerImpl, name: String, args: Value) -> Value {
    // The subscription key is encoded after the last delimiter.
    let well_formed = name
        .rsplit_once(TOOL_NAME_DELIMITER)
        .is_some_and(|(_, key)| key.parse::<u64>().is_ok());
    if !well_formed {
        return tool_error_result(&format!("Tool not found: {name}"), 404);
    }
    let Some(tool) = open_tools(server).into_iter().find(|t| t.name == name) else {
        return tool_error_result(
            &format!("Tool {name} has been removed. Please refresh the tool list."),
            404,
        );
    };
    let mut arguments = match args {
        Value::Object(m) => m,
        Value::Null => Map::new(),
        _ => return tool_error_result("`arguments` must be an object", 400),
    };
    // `businessId` is a reserved tool argument, not a process variable.
    let business_id = match arguments.remove(BUSINESS_ID_ARGUMENT) {
        Some(Value::String(b)) if !b.trim().is_empty() => Some(b),
        _ => None,
    };
    let variables = arguments
        .into_iter()
        .map(|(k, v)| (k, nanobpm_gateway_rest::types::Object(v)))
        .collect();
    let body = models::MessageCorrelationRequest {
        name: tool.message_name,
        // A random key distributes calls and allows parallel instances.
        correlation_key: Some(uuid::Uuid::new_v4().to_string()),
        variables: Some(variables),
        tenant_id: None,
        business_id,
    };
    match server.correlate_message_impl(&body).await {
        Ok(resp) => {
            let (status, body) = response_status_body(&resp);
            if !(200..300).contains(&status) {
                return tool_error_from_problem(&body);
            }
            let key = &body["processInstanceKey"];
            let key = key
                .as_str()
                .and_then(|k| k.parse::<u64>().ok())
                .map_or_else(|| key.clone(), |k| json!(k));
            let structured = json!({ "processInstanceKey": key });
            json!({
                "content": [ { "type": "text", "text": structured.to_string() } ],
                "structuredContent": structured,
                "isError": false,
            })
        }
        Err(()) => tool_error_result("correlate message failed", 500),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (unit tests)"
)]
mod tests {
    use nanobpmn_engine_core::bpmn::parse_bpmn;

    use super::*;
    use crate::mcp::cluster_tools::cluster_tool_servers;

    const FIXTURE: &str =
        include_str!("../../../engine-core/tests/fixtures/mcp-process-with-inputs.bpmn");

    fn processes_server() -> ToolServer {
        processes_tool_servers().remove(0)
    }

    async fn deploy_xml(server: &ServerImpl, xml: &str) {
        let defs = parse_bpmn(xml).expect("fixture parses");
        let mut names = std::collections::HashMap::new();
        for d in &defs {
            names.insert(d.id.clone(), format!("{}.bpmn", d.id));
        }
        server
            .deploy_resources_locally(
                defs,
                &names,
                Vec::new(),
                &std::collections::HashMap::new(),
                "<default>",
            )
            .await
            .expect("deploy succeeds");
    }

    async fn rpc(server: &ServerImpl, method: &str, params: Value) -> Value {
        super::super::handle_frame(
            server,
            &processes_server(),
            json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }),
        )
        .await
        .expect("response")["result"]
            .clone()
    }

    async fn dynamic_tools(server: &ServerImpl) -> Vec<Value> {
        rpc(server, "tools/list", json!({})).await["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| !PROCESSES_STATIC_TOOLS.contains(&t["name"].as_str().unwrap()))
            .cloned()
            .collect()
    }

    /// Poll until the dynamic tool list satisfies `pred` (projection is async).
    async fn wait_tools(server: &ServerImpl, pred: impl Fn(&[Value]) -> bool) -> Vec<Value> {
        for _ in 0..500 {
            let tools = dynamic_tools(server).await;
            if pred(&tools) {
                return tools;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let tools = dynamic_tools(server).await;
        panic!("tool list never matched: {tools:?}");
    }

    fn sub_key(tool: &Value) -> String {
        let name = tool["name"].as_str().unwrap();
        name.rsplit_once('_').unwrap().1.to_string()
    }

    #[tokio::test]
    async fn deployed_tool_process_lists_with_description_and_schema() {
        let server = ServerImpl::default();
        deploy_xml(&server, FIXTURE).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let tool = &tools[0];
        let name = tool["name"].as_str().unwrap();
        assert!(name.starts_with("mcp-process-with-inputs_"), "{name}");
        assert!(sub_key(tool).parse::<u64>().is_ok(), "{name}");
        assert_eq!(tool["title"], json!("mcp-process-with-inputs"));
        assert_eq!(
            tool["description"],
            json!("Tool for executing a process with typed inputs triggered by MCP.")
        );
        let schema = &tool["inputSchema"];
        assert_eq!(schema["type"], json!("object"));
        assert_eq!(
            schema["properties"]["firstName"],
            json!({ "type": "string", "description": "The first name of the person." })
        );
        assert_eq!(
            schema["properties"]["amount"],
            json!({ "type": "integer", "description": "The amount to process." })
        );
        assert_eq!(schema["properties"]["businessId"]["type"], json!("string"));
        assert_eq!(schema["required"], json!(["amount"]));
        assert_eq!(
            tool["outputSchema"]["required"],
            json!(["processInstanceKey"])
        );
        // The static tools list after the dynamic one.
        let all = rpc(&server, "tools/list", json!({})).await["tools"].clone();
        let names: Vec<&str> = all
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(&names[1..], &PROCESSES_STATIC_TOOLS);
    }

    #[test]
    fn description_assembles_every_part_with_camunda_labels() {
        let mut props = BTreeMap::new();
        for (k, v) in [
            ("purpose", "Do it."),
            ("inputs", "An order."),
            ("when_to_use", "Always."),
            ("when_not_to_use", "   "),
            ("results", "A key."),
        ] {
            props.insert(format!("{TOOL_PROPERTY_PREFIX}{k}"), v.to_string());
        }
        assert_eq!(
            build_description(&props),
            "Do it.\n\n## Inputs\nAn order.\n\n## When to use\nAlways.\n\n## Results\nA key."
        );
        assert_eq!(build_description(&BTreeMap::new()), "");
    }

    #[test]
    fn input_schema_orders_groups_and_reserves_business_id() {
        let mut props = BTreeMap::new();
        for (k, v) in [
            ("input_10_name", "ten"),
            ("input_2_name", "two"),
            ("input_2_required", "TRUE"),
            ("input_3_name", "businessId"),
            ("input_4_type", "string"),
        ] {
            props.insert(format!("{TOOL_PROPERTY_PREFIX}{k}"), v.to_string());
        }
        let schema = build_input_schema(&props);
        let keys: Vec<&String> = schema["properties"].as_object().unwrap().keys().collect();
        assert_eq!(keys.len(), 3, "{schema}");
        assert_eq!(schema["required"], json!(["two"]));
        assert_eq!(schema["properties"]["businessId"]["type"], json!("string"));
    }

    #[tokio::test]
    async fn new_version_replaces_tool_and_dropping_the_marker_removes_it() {
        let server = ServerImpl::default();
        deploy_xml(&server, FIXTURE).await;
        let v1 = wait_tools(&server, |t| t.len() == 1).await;
        let v1_key = sub_key(&v1[0]);
        let v1_name = v1[0]["name"].as_str().unwrap().to_string();

        // A new version (changed purpose) replaces the tool, not duplicates it.
        let v2_xml = FIXTURE.replace(
            "Tool for executing a process with typed inputs triggered by MCP.",
            "Version two.",
        );
        deploy_xml(&server, &v2_xml).await;
        let v2 = wait_tools(&server, |t| t.len() == 1 && sub_key(&t[0]) != v1_key).await;
        assert_eq!(v2[0]["description"], json!("Version two."));

        // A call against the replaced tool reports it removed.
        let r = rpc(
            &server,
            "tools/call",
            json!({ "name": v1_name, "arguments": { "amount": 1 } }),
        )
        .await;
        assert_eq!(r["isError"], json!(true), "{r}");
        let text = r["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("has been removed. Please refresh the tool list."),
            "{text}"
        );

        // A version without the tool marker removes the tool from the list
        // (Nano's deleteResource is not implemented yet, so un-marking is the
        // lifecycle path that closes the tool subscription).
        let unmarked = FIXTURE.replace(
            r#"<zeebe:property name="io.camunda.tool:name" value="mcp-process-with-inputs" />"#,
            "",
        );
        assert_ne!(unmarked, FIXTURE);
        deploy_xml(&server, &unmarked).await;
        wait_tools(&server, <[Value]>::is_empty).await;
    }

    #[tokio::test]
    async fn malformed_tool_name_is_not_found() {
        let server = ServerImpl::default();
        let r = rpc(
            &server,
            "tools/call",
            json!({ "name": "nope", "arguments": {} }),
        )
        .await;
        assert_eq!(r["isError"], json!(true));
        assert!(
            r["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Tool not found: nope")
        );
    }

    #[tokio::test]
    async fn call_starts_instance_and_honours_business_id() {
        let server = ServerImpl::default();
        deploy_xml(&server, FIXTURE).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let name = tools[0]["name"].as_str().unwrap().to_string();
        let r = rpc(
            &server,
            "tools/call",
            json!({ "name": name, "arguments": {
                "firstName": "Ada", "amount": 3, "businessId": "order-7" } }),
        )
        .await;
        assert_eq!(r["isError"], json!(false), "{r}");
        let pik = r["structuredContent"]["processInstanceKey"]
            .as_u64()
            .expect("integer processInstanceKey");
        let text: Value = serde_json::from_str(r["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(text, r["structuredContent"]);

        let mut found = None;
        for _ in 0..500 {
            if let Some(inst) = server
                .store
                .process_instances()
                .into_iter()
                .find(|i| i.key == pik)
            {
                let vars = server.store.variables();
                let names: HashSet<String> = vars
                    .iter()
                    .filter(|v| v.instance_key == pik)
                    .map(|v| v.name.clone())
                    .collect();
                if names.contains("firstName") && names.contains("amount") {
                    found = Some((inst, names));
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let (inst, names) = found.expect("instance and variables projected");
        assert_eq!(inst.business_id.as_deref(), Some("order-7"));
        assert!(!names.contains("businessId"), "{names:?}");
    }

    #[tokio::test]
    async fn static_tools_share_the_cluster_implementation() {
        let server = ServerImpl::default();
        let processes = processes_server();
        let cluster = cluster_tool_servers().remove(0);
        for name in PROCESSES_STATIC_TOOLS {
            let p = processes.find(name).expect("listed on /mcp/processes");
            let c = cluster.find(name).expect("cluster tool");
            assert_eq!(p.description, c.description, "{name}");
            assert_eq!(p.input_schema, c.input_schema, "{name}");
            assert_eq!(p.read_only, c.read_only, "{name}");
        }
        // Same dispatch: identical results for the same call on both servers.
        let args = json!({ "processInstanceKey": "424242" });
        let a =
            (processes.find("getProcessInstance").unwrap().dispatch)(&server, args.clone()).await;
        let b = (cluster.find("getProcessInstance").unwrap().dispatch)(&server, args).await;
        assert_eq!(a, b);
        assert_eq!(a["isError"], json!(true));
    }

    #[tokio::test]
    async fn processes_route_is_mounted() {
        use tower::ServiceExt;
        let app = super::super::mcp_router(ServerImpl::default());
        let req = axum::http::Request::builder()
            .method("POST")
            .uri(PROCESSES_PATH)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }).to_string(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["result"]["tools"].as_array().unwrap().len(), 3, "{v}");
    }
}
