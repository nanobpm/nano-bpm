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
//! - **inputSchema** — the reserved optional `businessId`, plus either the
//!   start message's **data envelope** (`io.nanobpm.dataEnvelope.in` on the
//!   `bpmn:message` → a `nano:shape`, derived by the canonical
//!   [`nano_data_envelope::json_schema`] mapping, issue #1738) or — when the
//!   message carries no envelope (Camunda parity) — one property per
//!   `io.camunda.tool:input_N_{name,type,description,required}` group. When
//!   both are declared the envelope wins and the conflict is surfaced in the
//!   console catalog's `warnings` (never silently merged).
//! - **outputSchema** — `{processInstanceKey}`.
//! - **call** — correlates the start message with a random-UUID correlation
//!   key through the SAME `correlate_message_impl` the REST `correlateMessage`
//!   route uses; every argument except `businessId` becomes a process variable.
//!
//! **Scope:** like the other read-model query endpoints (which do not
//! scatter-gather across peers), tool resolution reads this node's read model.
//! In a multi-node cluster, point MCP clients at a node that owns the
//! partition holding the message start subscriptions; cluster-wide resolution
//! is a tracked follow-up of #1499.
//!
//! The three static tools (`searchIncidents`, `getProcessInstance`,
//! `searchVariables`) are the cluster server's own [`Tool`]s, shared via
//! [`shared_cluster_tools`] — not copies.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use nano_data_envelope::json_schema::{EnvelopeSchema, SchemaError, start_event_input_schema};
use nano_data_envelope::{BpmnScan, scan_bpmn};
use nanobpm_gateway_rest::models;
// The tool-metadata property prefix (single source: engine-core).
use nanobpmn_engine_core::TOOL_PROPERTY_PREFIX;
use serde_json::{Map, Value, json};

use super::cluster_tools::shared_cluster_tools;
use super::{
    DynamicTools, ToolServer, response_status_body, tool_error_from_problem, tool_error_result,
};
use crate::ServerImpl;
use crate::readstore::ProcessDefinitionRow;

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
#[cfg_attr(
    not(feature = "console"),
    allow(
        dead_code,
        reason = "the console catalog is served by console builds only"
    )
)]
struct ProcessTool {
    /// `<toolName>_<subscriptionKey>`.
    name: String,
    tool_name: String,
    message_name: String,
    process_definition_key: u64,
    process_definition_id: String,
    process_definition_version: i32,
    process_name: Option<String>,
    properties: BTreeMap<String, String>,
    /// The start message's data envelope: `Ok(Some(..))` when it resolves,
    /// `Ok(None)` with none, and `Err` when it is declared but its schema
    /// cannot be derived (unresolved ref, derivation limits) — the tool then
    /// rejects every call (never falls back to a weaker contract).
    envelope: Envelope,
}

type Envelope = Result<Option<EnvelopeSchema>, (String, SchemaError)>;

/// The per-definition data-envelope cache (issue #1748). A deployed definition
/// is immutable under its key, so its BPMN is scanned once and each start
/// event's envelope schema derived once (lazily, only for the subscription a
/// request actually resolves). Entries of definitions no longer serving an
/// open tool are pruned on each resolution pass.
#[derive(Default)]
pub(crate) struct EnvelopeCache {
    defs: Mutex<HashMap<u64, Arc<DefinitionEnvelopes>>>,
}

/// One definition's scan plus its derived envelopes by start element id.
struct DefinitionEnvelopes {
    scan: BpmnScan,
    schemas: Mutex<HashMap<String, Envelope>>,
}

impl EnvelopeCache {
    /// The data envelope of start event `start_element_id` of definition
    /// `def_key`, derived at most once per (definition, start event).
    fn envelope(
        &self,
        server: &ServerImpl,
        def_key: u64,
        process_id: &str,
        start_element_id: &str,
    ) -> Envelope {
        let cached = self
            .defs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&def_key)
            .cloned();
        let def = match cached {
            Some(def) => def,
            None => {
                // Scan outside the lock; a racing duplicate scan is harmless
                // (same immutable XML) and the first insert wins.
                let Some(xml) = server.store.process_definition_xml(def_key) else {
                    return Ok(None);
                };
                let fresh = Arc::new(DefinitionEnvelopes {
                    scan: scan_bpmn(&xml),
                    schemas: Mutex::new(HashMap::new()),
                });
                self.defs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(def_key)
                    .or_insert(fresh)
                    .clone()
            }
        };
        let mut schemas = def.schemas.lock().unwrap_or_else(|e| e.into_inner());
        schemas
            .entry(start_element_id.to_string())
            .or_insert_with(|| {
                // Deployed definitions carry no fused type registry, so only
                // model-declared shapes resolve; every external ref is
                // surfaced as an unresolved-contract warning.
                start_event_input_schema(&def.scan, start_element_id, Some(process_id), &())
            })
            .clone()
    }

    /// Drop the entries of definitions not in `live`.
    fn retain(&self, live: &HashSet<u64>) {
        self.defs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, _| live.contains(k));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.defs.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// The message explaining why a tool whose envelope schema could not be
/// derived rejects calls.
fn envelope_error_message(shape: &str, e: SchemaError) -> String {
    format!(
        "Data envelope `{shape}` cannot be used as the tool inputSchema: {e}; the tool rejects calls until the envelope is simplified."
    )
}

impl ProcessTool {
    /// The advertised (and enforced) input schema.
    /// `Err` (with the reason) when the envelope's schema could not be derived.
    fn input_schema(&self) -> Result<Value, String> {
        match &self.envelope {
            Ok(envelope) => Ok(resolve_input_schema(
                envelope.as_ref().map(|e| &e.schema),
                &self.properties,
            )),
            Err((shape, e)) => Err(envelope_error_message(shape, *e)),
        }
    }

    /// The advertised input schema; for an underivable envelope, only the
    /// reserved `businessId` (calls are rejected regardless, see
    /// [`call_tool`]).
    fn advertised_input_schema(&self) -> Value {
        self.input_schema().unwrap_or_else(|_| {
            json!({
                "type": "object",
                "properties": { BUSINESS_ID_ARGUMENT: business_id_schema() },
            })
        })
    }

    /// Authoring conflicts surfaced to the console (never silently merged).
    fn warnings(&self) -> Vec<String> {
        match &self.envelope {
            Ok(envelope) => envelope_warnings(envelope.as_ref(), &self.properties),
            Err((shape, e)) => vec![envelope_error_message(shape, *e)],
        }
    }
}

/// Authoring conflicts between the start message's data envelope and the
/// tool properties: an envelope next to `input_N_*` properties (the latter are
/// ignored), and an envelope field named like the reserved `businessId`
/// argument (stripped from the variables, so that field is never delivered).
pub(crate) fn envelope_warnings(
    envelope: Option<&EnvelopeSchema>,
    props: &BTreeMap<String, String>,
) -> Vec<String> {
    let Some(EnvelopeSchema {
        shape,
        schema,
        unresolved,
    }) = envelope
    else {
        return Vec::new();
    };
    // Unresolved-contract diagnostics: the schema does not enforce these.
    let mut warnings: Vec<String> = unresolved
        .iter()
        .map(|d| format!("Data envelope `{shape}` is incomplete: {d}."))
        .collect();
    if has_input_properties(props) {
        warnings.push(format!(
            "The start message declares data envelope `{shape}` and \
             `{TOOL_PROPERTY_PREFIX}input_N_*` properties; the envelope defines the \
             inputSchema and the input_N_* properties are ignored."
        ));
    }
    if schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|p| p.contains_key(BUSINESS_ID_ARGUMENT))
    {
        warnings.push(format!(
            "Data envelope `{shape}` declares a field `{BUSINESS_ID_ARGUMENT}`, which is \
             the reserved MCP argument and is never passed as a process variable; rename \
             the envelope field."
        ));
    }
    warnings
}

/// The open tool subscriptions: message start subscriptions that carry a
/// tool name and belong to the **latest** version of their process (a start
/// subscription of a superseded version is closed in Zeebe, so its tool is
/// gone even when the newer version no longer declares the message start).
///
/// With `only` set, just the tool of that full name is resolved — its
/// envelope is the only one derived (issue #1748).
fn open_tools(server: &ServerImpl, only: Option<&str>) -> Vec<ProcessTool> {
    let latest: HashMap<u64, ProcessDefinitionRow> = server
        .store
        .process_definitions()
        .into_iter()
        .filter(|d| d.is_latest)
        .map(|d| (d.key, d))
        .collect();
    let subscriptions: Vec<_> = server
        .store
        .message_start_subscriptions()
        .into_iter()
        .filter_map(|s| {
            latest.get(&s.process_definition_key)?;
            let tool_name = s.tool_name.clone().filter(|n| !n.is_empty())?;
            let name = format!("{tool_name}{TOOL_NAME_DELIMITER}{}", s.subscription_key);
            Some((s, tool_name, name))
        })
        .collect();
    // Prune cache entries of definitions that no longer serve an open tool
    // (only on a full pass: a filtered pass sees a subset).
    if only.is_none() {
        let live: HashSet<u64> = subscriptions
            .iter()
            .map(|(s, _, _)| s.process_definition_key)
            .collect();
        server.mcp_envelopes.retain(&live);
    }
    let mut tools: Vec<ProcessTool> = subscriptions
        .into_iter()
        .filter(|(_, _, name)| only.is_none_or(|o| o == name))
        .filter_map(|(s, tool_name, name)| {
            let def = latest.get(&s.process_definition_key)?;
            let properties = serde_json::from_str::<BTreeMap<String, String>>(&s.tool_properties)
                .unwrap_or_default();
            let envelope = server.mcp_envelopes.envelope(
                server,
                s.process_definition_key,
                &s.process_id,
                &s.start_element_id,
            );
            Some(ProcessTool {
                name,
                tool_name,
                message_name: s.message_name,
                process_definition_key: s.process_definition_key,
                process_definition_id: s.process_id,
                process_definition_version: def.version,
                process_name: def.name.clone(),
                properties,
                envelope,
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

/// Whether any `io.camunda.tool:input_N_*` property is declared.
fn has_input_properties(props: &BTreeMap<String, String>) -> bool {
    props.keys().any(|k| {
        k.strip_prefix(TOOL_PROPERTY_PREFIX)
            .is_some_and(|r| r.starts_with("input_"))
    })
}

/// The tool input schema: the data-envelope object schema (with the reserved
/// `businessId` added; an envelope field of that name is shadowed), or — with
/// no envelope — the `input_N_*` schema of [`build_input_schema`].
pub(crate) fn resolve_input_schema(
    envelope: Option<&Value>,
    props: &BTreeMap<String, String>,
) -> Value {
    let Some(envelope) = envelope else {
        return build_input_schema(props);
    };
    let mut schema = envelope.clone();
    if let Some(obj) = schema.as_object_mut() {
        if let Some(Value::Object(properties)) = obj.get_mut("properties") {
            properties.insert(BUSINESS_ID_ARGUMENT.into(), business_id_schema());
        }
        let required: Vec<Value> = obj
            .get("required")
            .and_then(Value::as_array)
            .map(|r| {
                r.iter()
                    .filter(|n| *n != BUSINESS_ID_ARGUMENT)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        obj.remove("required");
        if !required.is_empty() {
            obj.insert("required".into(), Value::Array(required));
        }
    }
    schema
}

fn business_id_schema() -> Value {
    json!({ "type": "string", "description": BUSINESS_ID_DESCRIPTION })
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
        if properties.contains_key(name) {
            // Repeated name: the lowest-numbered group wins, so `properties`
            // and the (JSON-Schema-unique) `required` array never diverge.
            continue;
        }
        let mut schema = Map::new();
        // Only JSON-Schema primitive types are advertised; an unknown type is
        // left unconstrained so clients compiling the schema never reject it.
        if let Some(ty) =
            prop(props, &format!("input_{n}_type")).filter(|ty| JSON_SCHEMA_TYPES.contains(ty))
        {
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
    properties.insert(BUSINESS_ID_ARGUMENT.into(), business_id_schema());
    let mut schema = json!({ "type": "object", "properties": properties });
    if !required.is_empty() {
        schema["required"] = Value::Array(required);
    }
    schema
}

/// The JSON-Schema primitive type names: the single source of truth for
/// which declared input types are advertised and enforced.
const JSON_SCHEMA_TYPES: [&str; 7] = [
    "string", "integer", "number", "boolean", "object", "array", "null",
];

/// Validate tool `arguments` against the input schema advertised in
/// `tools/list` ([`resolve_input_schema`]) — recursively, so an envelope's
/// nested objects and `list` arrays are enforced too: every `required`
/// property must be present and every supplied value with a declared
/// JSON-Schema `type` must match it (array `items` are checked per element).
/// Undeclared arguments pass through (they become process variables), and an
/// unrecognised declared type is not enforced.
pub(crate) fn validate_arguments(
    schema: &Value,
    arguments: &Map<String, Value>,
) -> Result<(), String> {
    validate_object(schema, arguments, "")
}

/// Append a field name to an error path unambiguously, in the same notation
/// as the console's `inputRows` (`console/src/lib/mcpProcesses.ts`): a plain
/// identifier as `.name`, anything else (dots, brackets, quotes, empty)
/// JSON-quoted as `["…"]`, so distinct argument paths never render the same.
fn join_path(parent: &str, name: &str) -> String {
    let mut chars = name.chars();
    let plain = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '$' | '-'));
    if !plain {
        format!("{parent}[{}]", Value::String(name.to_string()))
    } else if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}.{name}")
    }
}

fn validate_object(schema: &Value, object: &Map<String, Value>, path: &str) -> Result<(), String> {
    if let Some(required) = schema["required"].as_array() {
        for name in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(name) {
                return Err(format!(
                    "Missing required argument `{}`",
                    join_path(path, name)
                ));
            }
        }
    }
    let Some(properties) = schema["properties"].as_object() else {
        return Ok(());
    };
    for (name, value) in object {
        if let Some(property) = properties.get(name) {
            validate_value(property, value, &join_path(path, name))?;
        }
    }
    Ok(())
}

fn validate_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    if let Some(ty) = schema["type"].as_str() {
        let matches = match ty {
            "string" => value.is_string(),
            // JSON Schema `integer` includes integral floats (`3.0`, `3e0`).
            "integer" => {
                value.is_i64()
                    || value.is_u64()
                    || value
                        .as_f64()
                        .is_some_and(|f| f.is_finite() && f.fract() == 0.0)
            }
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "object" => value.is_object(),
            "array" => value.is_array(),
            "null" => value.is_null(),
            _ => true,
        };
        if !matches {
            return Err(format!("Argument `{path}` must be of type {ty}"));
        }
    }
    match value {
        Value::Object(object) => validate_object(schema, object, path),
        Value::Array(items) => match schema.get("items") {
            Some(item_schema) => items
                .iter()
                .enumerate()
                .try_for_each(|(i, v)| validate_value(item_schema, v, &format!("{path}[{i}]"))),
            None => Ok(()),
        },
        _ => Ok(()),
    }
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
        "inputSchema": tool.advertised_input_schema(),
        "outputSchema": output_schema(),
    })
}

/// The console's read-only view of the processes exposed as MCP tools
/// (`GET /console/api/mcp/processes`, issue #1612 — parity with Camunda's
/// admin "MCP processes" page). Derived from the SAME [`open_tools`] +
/// [`descriptor`] the `/mcp/processes` server lists, so the console can never
/// drift from what an agent is actually offered; it only adds the owning
/// process definition and the `NANOBPM_MCP_ENABLED` state. Served even when
/// MCP is disabled (with `enabled: false`) so an operator can see what would
/// be exposed.
#[cfg_attr(
    not(feature = "console"),
    allow(
        dead_code,
        reason = "the console catalog is served by console builds only"
    )
)]
pub(crate) fn console_catalog(server: &ServerImpl, enabled: bool) -> Value {
    let tools: Vec<Value> = open_tools(server, None)
        .iter()
        .map(|t| {
            let mut v = descriptor(t);
            v["toolName"] = json!(t.tool_name);
            v["messageName"] = json!(t.message_name);
            v["subscriptionKey"] = json!(
                t.name
                    .rsplit_once(TOOL_NAME_DELIMITER)
                    .map_or("", |(_, k)| k)
            );
            v["processDefinitionKey"] = json!(t.process_definition_key.to_string());
            v["processDefinitionId"] = json!(t.process_definition_id);
            v["processDefinitionVersion"] = json!(t.process_definition_version);
            v["processName"] = json!(t.process_name);
            v["inputSchemaSource"] = json!(if !matches!(t.envelope, Ok(None)) {
                "dataEnvelope"
            } else {
                "toolProperties"
            });
            v["dataEnvelope"] = json!(match &t.envelope {
                Ok(envelope) => envelope.as_ref().map(|e| &e.shape),
                Err((id, _)) => Some(id),
            });
            v["warnings"] = json!(t.warnings());
            v
        })
        .collect();
    json!({
        "enabled": enabled,
        "envVar": super::MCP_ENABLED_ENV,
        "path": PROCESSES_PATH,
        "staticTools": PROCESSES_STATIC_TOOLS,
        "tools": tools,
    })
}

/// `tools/list` provider: the current dynamic tools.
fn list_tools(server: &ServerImpl) -> Vec<Value> {
    open_tools(server, None).iter().map(descriptor).collect()
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
    let Some(tool) = open_tools(server, Some(&name)).into_iter().next() else {
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
    // Validate against the SAME schema advertised in `tools/list`.
    let schema = match tool.input_schema() {
        Ok(schema) => schema,
        Err(msg) => return tool_error_result(&msg, 400),
    };
    if let Err(msg) = validate_arguments(&schema, &arguments) {
        return tool_error_result(&msg, 400);
    }
    // `businessId` is a reserved tool argument, not a process variable. A
    // supplied string is passed through verbatim so the shared correlation
    // path enforces the business-id rules (never silently dropped).
    // (`validate_arguments` already rejected a non-string `businessId`.)
    let business_id = match arguments.remove(BUSINESS_ID_ARGUMENT) {
        Some(Value::String(b)) => Some(b),
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
    use std::collections::HashSet;

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

    /// Issue #1612: the console catalog is the `tools/list` descriptor plus
    /// definition metadata — never a re-derivation that can drift.
    #[tokio::test]
    async fn console_catalog_mirrors_tools_list_with_definition_metadata() {
        let server = ServerImpl::default();
        let empty = console_catalog(&server, false);
        assert_eq!(empty["enabled"], json!(false));
        assert_eq!(empty["tools"], json!([]));
        assert_eq!(empty["path"], json!(PROCESSES_PATH));
        assert_eq!(empty["envVar"], json!("NANOBPM_MCP_ENABLED"));
        assert_eq!(empty["staticTools"], json!(PROCESSES_STATIC_TOOLS));

        deploy_xml(&server, FIXTURE).await;
        let listed = wait_tools(&server, |t| t.len() == 1).await;
        let catalog = console_catalog(&server, true);
        assert_eq!(catalog["enabled"], json!(true));
        let entries = catalog["tools"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        for field in [
            "name",
            "title",
            "description",
            "inputSchema",
            "outputSchema",
        ] {
            assert_eq!(entry[field], listed[0][field], "{field} drifted");
        }
        assert_eq!(entry["toolName"], json!("mcp-process-with-inputs"));
        assert_eq!(entry["messageName"], json!("mcp-process-with-inputs"));
        assert_eq!(entry["subscriptionKey"], json!(sub_key(&listed[0])));
        assert_eq!(entry["processDefinitionId"], json!("mcpProcessWithInputs"));
        assert_eq!(entry["processDefinitionVersion"], json!(1));
        assert!(
            entry["processDefinitionKey"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .is_ok()
        );
    }

    /// A model authored with the console's built-in "MCP start event"
    /// component (#1611) lists under `/mcp/processes` with the Camunda
    /// description and the Nano typed `inputSchema`; the unfilled optional
    /// groups contribute nothing.
    #[tokio::test]
    async fn console_mcp_start_event_template_model_lists_typed_tool() {
        let server = ServerImpl::default();
        deploy_xml(
            &server,
            include_str!("../../../engine-core/tests/fixtures/mcp-start-event-template.bpmn"),
        )
        .await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let tool = &tools[0];
        let name = tool["name"].as_str().unwrap();
        assert!(name.starts_with("lookup_customer_"), "{name}");
        assert_eq!(
            tool["description"],
            json!(
                "Look up a customer record.\n\n## Inputs\nA customer email and an optional limit.\n\n## When to use\nThe user asks about a customer."
            )
        );
        assert_eq!(
            tool["inputSchema"],
            json!({
                "type": "object",
                "properties": {
                    "email": { "type": "string", "description": "The customer's email." },
                    "limit": { "type": "integer" },
                    "businessId": { "type": "string", "description": BUSINESS_ID_DESCRIPTION },
                },
                "required": ["email"],
            })
        );
    }

    const ENVELOPE_FIXTURE: &str = include_str!("fixtures/mcp-process-with-envelope.bpmn");
    const ENVELOPE_PROPERTY: &str =
        r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="PlaceOrder" />"#;

    /// Issue #1738: the start message's data envelope defines the inputSchema
    /// (nested reference, list, required, descriptions), takes precedence over
    /// `input_N_*`, and the console catalog reflects it with the conflict.
    #[tokio::test]
    async fn envelope_derives_input_schema_and_wins_over_input_properties() {
        let server = ServerImpl::default();
        deploy_xml(&server, ENVELOPE_FIXTURE).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let tool = &tools[0];
        assert_eq!(tool["title"], json!("place-order"));
        assert_eq!(tool["description"], json!("Place an order."));
        assert_eq!(
            tool["inputSchema"],
            json!({
                "type": "object",
                "properties": {
                    "orderId": { "type": "string", "description": "The order id." },
                    "note": { "type": "string" },
                    "customer": {
                        "type": "object",
                        "properties": { "email": { "type": "string" } },
                        "required": ["email"],
                        "description": "Who is ordering.",
                    },
                    "lines": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "sku": { "type": "string" },
                                "qty": { "type": "integer" },
                            },
                            "required": ["sku", "qty"],
                        },
                        "description": "The order lines.",
                    },
                    "businessId": { "type": "string", "description": BUSINESS_ID_DESCRIPTION },
                },
                "required": ["orderId", "customer", "lines"],
            })
        );

        let catalog = console_catalog(&server, true);
        let entry = &catalog["tools"][0];
        assert_eq!(entry["inputSchema"], tool["inputSchema"]);
        assert_eq!(entry["inputSchemaSource"], json!("dataEnvelope"));
        assert_eq!(entry["dataEnvelope"], json!("PlaceOrder"));
        let warnings = entry["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1, "{entry}");
        assert!(
            warnings[0].as_str().unwrap().contains("PlaceOrder"),
            "{warnings:?}"
        );

        // Calls validate against the SAME derived schema (nested + list).
        let name = tool["name"].as_str().unwrap().to_string();
        let ok = json!({ "orderId": "o-1", "customer": { "email": "a@b" },
            "lines": [ { "sku": "x", "qty": 2 } ] });
        for (args, expect) in [
            (
                json!({ "orderId": "o-1", "customer": { "email": "a@b" } }),
                "lines",
            ),
            (
                json!({ "orderId": "o-1", "customer": {}, "lines": [] }),
                "customer.email",
            ),
            (
                json!({ "orderId": "o-1", "customer": { "email": "a@b" },
                    "lines": [ { "sku": "x", "qty": "two" } ] }),
                "lines[0].qty",
            ),
        ] {
            let r = rpc(
                &server,
                "tools/call",
                json!({ "name": name, "arguments": args }),
            )
            .await;
            assert_eq!(r["isError"], json!(true), "{r}");
            let text = r["content"][0]["text"].as_str().unwrap();
            assert!(text.contains(expect), "{expect}: {text}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(server.store.process_instances().is_empty());
        // The legacy input_N `legacy` (required) is ignored under an envelope.
        let r = rpc(
            &server,
            "tools/call",
            json!({ "name": name, "arguments": ok }),
        )
        .await;
        assert_eq!(r["isError"], json!(false), "{r}");
    }

    /// An envelope that resolves but exceeds the derivation limits must NOT
    /// fall back to the `input_N_*` (weaker) contract: every call is rejected
    /// and the catalog reports why.
    #[tokio::test]
    async fn over_limit_envelope_rejects_calls_instead_of_falling_back() {
        let server = ServerImpl::default();
        let mut chain = String::from(
            r#"<nano:shape id="Deep"><nano:reference name="n" ref="D0" /></nano:shape>"#,
        );
        for i in 0..100 {
            chain.push_str(&format!(
                r#"<nano:shape id="D{i}"><nano:reference name="n" ref="D{n}" /></nano:shape>"#,
                n = i + 1
            ));
        }
        let xml = ENVELOPE_FIXTURE
            .replace(
                ENVELOPE_PROPERTY,
                r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="Deep" />"#,
            )
            .replace("<nano:shapes>", &format!("<nano:shapes>{chain}"));
        deploy_xml(&server, &xml).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let name = tools[0]["name"].as_str().unwrap().to_string();
        let entry = &console_catalog(&server, true)["tools"][0];
        assert_eq!(entry["inputSchemaSource"], json!("dataEnvelope"));
        assert_eq!(entry["dataEnvelope"], json!("Deep"));
        assert!(
            entry["warnings"][0].as_str().unwrap().contains("Deep"),
            "{entry}"
        );
        let r = rpc(
            &server,
            "tools/call",
            json!({ "name": name, "arguments": { "legacy": "x" } }),
        )
        .await;
        assert_eq!(r["isError"], json!(true), "{r}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(server.store.process_instances().is_empty());
    }

    /// Without an envelope the `input_N_*` properties define the schema
    /// (Camunda parity) and no conflict is reported.
    #[tokio::test]
    async fn without_envelope_input_properties_are_the_fallback() {
        let server = ServerImpl::default();
        let xml = ENVELOPE_FIXTURE.replace(ENVELOPE_PROPERTY, "");
        assert_ne!(xml, ENVELOPE_FIXTURE);
        deploy_xml(&server, &xml).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        assert_eq!(
            tools[0]["inputSchema"],
            json!({
                "type": "object",
                "properties": {
                    "legacy": { "type": "string" },
                    "businessId": { "type": "string", "description": BUSINESS_ID_DESCRIPTION },
                },
                "required": ["legacy"],
            })
        );
        let entry = &console_catalog(&server, true)["tools"][0];
        assert_eq!(entry["inputSchemaSource"], json!("toolProperties"));
        assert_eq!(entry["dataEnvelope"], Value::Null);
        assert_eq!(entry["warnings"], json!([]));
    }

    /// Issue #1748: the envelope is resolved through the start event's
    /// `messageRef` (message id), never by subscription message name — a
    /// decoy message sharing the name (declared first) must not be selected.
    #[tokio::test]
    async fn envelope_resolves_by_message_ref_not_name() {
        let server = ServerImpl::default();
        let decoy = r#"<bpmn:message id="Decoy" name="place-order">
    <bpmn:extensionElements><zeebe:properties>
      <zeebe:property name="io.nanobpm.dataEnvelope.in" value="Customer" />
    </zeebe:properties></bpmn:extensionElements>
  </bpmn:message>
  <bpmn:message id="Message_place_order""#;
        let xml = ENVELOPE_FIXTURE.replace(r#"<bpmn:message id="Message_place_order""#, decoy);
        assert_ne!(xml, ENVELOPE_FIXTURE);
        deploy_xml(&server, &xml).await;
        wait_tools(&server, |t| t.len() == 1).await;
        let entry = &console_catalog(&server, true)["tools"][0];
        assert_eq!(entry["dataEnvelope"], json!("PlaceOrder"), "{entry}");
    }

    /// Issue #1748: an envelope ref the model does not declare is NOT the
    /// silent legacy fallback — calls are rejected and the catalog says why.
    #[tokio::test]
    async fn unresolved_envelope_rejects_calls_instead_of_falling_back() {
        let server = ServerImpl::default();
        let xml = ENVELOPE_FIXTURE.replace(
            ENVELOPE_PROPERTY,
            r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="Nope" />"#,
        );
        deploy_xml(&server, &xml).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let name = tools[0]["name"].as_str().unwrap().to_string();
        let entry = &console_catalog(&server, true)["tools"][0];
        assert_eq!(entry["inputSchemaSource"], json!("dataEnvelope"));
        assert_eq!(entry["dataEnvelope"], json!("Nope"));
        assert!(
            entry["warnings"][0]
                .as_str()
                .unwrap()
                .contains("neither as a shape"),
            "{entry}"
        );
        let r = rpc(
            &server,
            "tools/call",
            json!({ "name": name, "arguments": { "legacy": "x" } }),
        )
        .await;
        assert_eq!(r["isError"], json!(true), "{r}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(server.store.process_instances().is_empty());
    }

    /// Issue #1748: a carried type the model cannot resolve is surfaced as an
    /// unresolved-contract warning, never treated as a complete schema; and
    /// envelopes are cached per deployed definition.
    #[tokio::test]
    async fn unresolved_carry_is_surfaced_and_envelopes_are_cached() {
        let server = ServerImpl::default();
        let xml = ENVELOPE_FIXTURE.replace(
            r#"<nano:shape id="PlaceOrder" name="Place order">"#,
            r#"<nano:shape id="PlaceOrder" name="Place order"><nano:carry ref="DomainOrder" />"#,
        );
        assert_ne!(xml, ENVELOPE_FIXTURE);
        deploy_xml(&server, &xml).await;
        wait_tools(&server, |t| t.len() == 1).await;
        assert_eq!(server.mcp_envelopes.len(), 1);
        let entry = &console_catalog(&server, true)["tools"][0];
        let warnings: Vec<&str> = entry["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("incomplete") && w.contains("`carry` of `DomainOrder`")),
            "{warnings:?}"
        );
        assert_eq!(server.mcp_envelopes.len(), 1);
    }

    #[test]
    fn envelope_schema_reserves_business_id() {
        let envelope = json!({
            "type": "object",
            "properties": { "businessId": { "type": "integer" } },
            "required": ["businessId"],
        });
        let schema = resolve_input_schema(Some(&envelope), &BTreeMap::new());
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "businessId": { "type": "string", "description": BUSINESS_ID_DESCRIPTION },
                },
            })
        );
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

    #[test]
    fn repeated_input_name_is_declared_once_and_required_is_unique() {
        let props: BTreeMap<String, String> = [
            ("input_1_name", "amount"),
            ("input_1_type", "number"),
            ("input_1_required", "true"),
            ("input_2_name", "amount"),
            ("input_2_type", "string"),
            ("input_2_required", "true"),
        ]
        .into_iter()
        .map(|(k, v)| (format!("{TOOL_PROPERTY_PREFIX}{k}"), v.to_string()))
        .collect();
        let schema = build_input_schema(&props);
        assert_eq!(schema["required"], json!(["amount"]));
        assert_eq!(schema["properties"]["amount"]["type"], json!("number"));
    }

    #[test]
    fn error_paths_quote_non_identifier_names() {
        assert_eq!(join_path("", "a"), "a");
        assert_eq!(join_path("a", "b"), "a.b");
        assert_eq!(join_path("", "customer.email"), r#"["customer.email"]"#);
        assert_eq!(join_path("customer", "email"), "customer.email");
        assert_eq!(join_path("a", "x[0]"), r#"a["x[0]"]"#);
        assert_eq!(join_path("a", ""), r#"a[""]"#);
        let schema = json!({"type":"object","required":["customer.email","customer"],
            "properties":{"customer":{"type":"object","required":["email"],
                "properties":{"email":{"type":"string"}}}}});
        let top = json!({"customer": {"email": "x"}});
        assert_eq!(
            validate_arguments(&schema, top.as_object().unwrap()).unwrap_err(),
            r#"Missing required argument `["customer.email"]`"#
        );
        let nested = json!({"customer.email": "x", "customer": {}});
        assert_eq!(
            validate_arguments(&schema, nested.as_object().unwrap()).unwrap_err(),
            "Missing required argument `customer.email`"
        );
    }

    #[test]
    fn validate_arguments_enforces_required_and_types() {
        let mut props = BTreeMap::new();
        for (k, v) in [
            ("input_1_name", "amount"),
            ("input_1_type", "integer"),
            ("input_1_required", "true"),
            ("input_2_name", "flag"),
            ("input_2_type", "boolean"),
            ("input_3_name", "free"),
            ("input_3_type", "custom"),
        ] {
            props.insert(format!("{TOOL_PROPERTY_PREFIX}{k}"), v.to_string());
        }
        let schema = build_input_schema(&props);
        // A non-JSON-Schema type is never advertised (left unconstrained).
        assert_eq!(schema["properties"]["free"].get("type"), None, "{schema}");
        assert_eq!(schema["properties"]["amount"]["type"], json!("integer"));
        let check = |v: Value| validate_arguments(&schema, v.as_object().unwrap());
        assert!(check(json!({})).unwrap_err().contains("amount"));
        // Integral floats are JSON-Schema integers.
        assert!(check(json!({ "amount": 3.0 })).is_ok());
        let e: Value = serde_json::from_str(r#"{"amount":3e0}"#).unwrap();
        assert!(check(e).is_ok());
        assert!(check(json!({ "amount": "x" })).is_err());
        assert!(check(json!({ "amount": 1.5 })).is_err());
        assert!(check(json!({ "amount": 1, "flag": "yes" })).is_err());
        assert!(check(json!({ "amount": 1, "businessId": 7 })).is_err());
        assert!(check(json!({ "amount": 1, "businessId": null })).is_err());
        assert!(check(json!({ "amount": 1, "flag": true, "free": [1], "extra": {} })).is_ok());
    }

    /// Invalid calls are rejected before correlation: no instance is created.
    #[tokio::test]
    async fn invalid_arguments_return_is_error_without_starting_an_instance() {
        let server = ServerImpl::default();
        deploy_xml(&server, FIXTURE).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let name = tools[0]["name"].as_str().unwrap().to_string();
        for args in [
            json!({}),
            json!({ "amount": "not-an-integer" }),
            json!({ "amount": 1, "businessId": 5 }),
        ] {
            let r = rpc(
                &server,
                "tools/call",
                json!({ "name": name, "arguments": args }),
            )
            .await;
            assert_eq!(r["isError"], json!(true), "{args}: {r}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(server.store.process_instances().is_empty());
    }

    /// A supplied empty `businessId` is not silently dropped: the shared
    /// correlation path rejects it instead of starting an anonymous instance.
    #[tokio::test]
    async fn empty_business_id_is_rejected_not_dropped() {
        let server = ServerImpl::default();
        deploy_xml(&server, FIXTURE).await;
        let tools = wait_tools(&server, |t| t.len() == 1).await;
        let name = tools[0]["name"].as_str().unwrap().to_string();
        let r = rpc(
            &server,
            "tools/call",
            json!({ "name": name, "arguments": { "amount": 1, "businessId": "" } }),
        )
        .await;
        assert_eq!(r["isError"], json!(true), "{r}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(server.store.process_instances().is_empty());
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
