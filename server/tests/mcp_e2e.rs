//! End-to-end check of the MCP gateway (issue #1499) against a **running node**
//! over the real wire protocol: a streamable-HTTP MCP client (JSON-RPC 2.0 over
//! HTTP POST, MCP revision 2025-03-26) performs the standard client lifecycle —
//! `initialize` → `notifications/initialized` → `tools/list` → `tools/call` —
//! against `/mcp/cluster` and `/mcp/processes`, exactly as an unmodified MCP
//! client configured for Camunda's endpoints would.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration-test target: unwrap/expect are acceptable in test setup/assertions; \
clippy.toml allow-unwrap-in-tests does not cover non-#[test] helper fns in tests/*.rs"
)]

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Child;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::server_command;
use serde_json::{Value, json};

const FIXTURE: &str = include_str!("../../engine-core/tests/fixtures/mcp-process-with-inputs.bpmn");

struct ScratchDir(PathBuf);

impl ScratchDir {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nanobpmn-mcp-e2e-{}-{nanos}-{seq}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self(path)
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A running server child, killed (whole process group) and reaped on drop.
struct ServerProcess {
    child: Child,
    port: u16,
    stdout_thread: Option<std::thread::JoinHandle<()>>,
}

impl ServerProcess {
    fn boot(journal: &std::path::Path, env: &[(&str, &str)]) -> Self {
        let mut command = server_command();
        command.env("NANOBPMN_JOURNAL", journal).env("PORT", "0");
        for (k, v) in env {
            command.env(k, v);
        }
        let mut child = command.spawn().expect("spawn server binary");
        let stdout = child.stdout.take().expect("server stdout is piped");
        let (tx, rx) = std::sync::mpsc::channel();
        // Keep draining stdout to EOF: dropping the read end would SIGPIPE the
        // child's later startup banner (console builds print one).
        let stdout_thread = std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            while matches!(reader.read_line(&mut line), Ok(n) if n > 0) {
                if let Some(rest) = line.trim().strip_prefix("LISTENING_PORT=")
                    && let Ok(port) = rest.parse::<u16>()
                {
                    let _ = tx.send(port);
                }
                line.clear();
            }
        });
        let mut server = Self {
            child,
            port: 0,
            stdout_thread: Some(stdout_thread),
        };
        server.port = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("server reports its listening port");
        server.wait_until_ready();
        server
    }

    fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok((status, _)) = http(self.port, "GET", "/v2/topology", "application/json", "")
                && status > 0
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {} never became ready", self.port);
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        #[cfg(unix)]
        // SAFETY: the child was spawned as a process-group leader.
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGKILL);
        }
        #[cfg(not(unix))]
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(t) = self.stdout_thread.take() {
            let _ = t.join();
        }
    }
}

/// One HTTP/1.1 request (`Connection: close`), returning `(status, body)`.
fn http(
    port: u16,
    method: &str,
    path: &str,
    content_type: &str,
    body: &str,
) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: {content_type}\r\n\
         Accept: application/json, text/event-stream\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(request.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let raw = String::from_utf8_lossy(&raw);
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    // Decode a chunked body if the server streamed one.
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_string()
    };
    Ok((status, body))
}

fn dechunk(mut s: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = s.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if n == 0 {
            break;
        }
        out.push_str(&rest[..n]);
        s = rest[n..].trim_start_matches("\r\n");
    }
    out
}

/// A minimal streamable-HTTP MCP client bound to one endpoint.
struct McpClient {
    port: u16,
    path: &'static str,
    next_id: u64,
}

impl McpClient {
    /// Connect and perform the MCP handshake: `initialize` (asserting the
    /// negotiated revision and the `tools` capability), then the
    /// `notifications/initialized` notification (HTTP 202, no body).
    fn connect(port: u16, path: &'static str) -> Self {
        let mut client = Self {
            port,
            path,
            next_id: 1,
        };
        let init = client.request(
            "initialize",
            json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "nano-mcp-e2e", "version": "1.0.0" }
            }),
        );
        assert_eq!(init["protocolVersion"], json!("2025-03-26"), "{init}");
        assert!(init["capabilities"]["tools"].is_object(), "{init}");
        assert!(init["serverInfo"]["name"].is_string(), "{init}");
        let (status, body) = client.post(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }));
        assert_eq!(status, 202, "initialized notification is accepted: {body}");
        client
    }

    fn post(&self, frame: &Value) -> (u16, String) {
        http(
            self.port,
            "POST",
            self.path,
            "application/json",
            &frame.to_string(),
        )
        .expect("MCP POST")
    }

    /// Send a JSON-RPC request and return its `result` (asserting no error).
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let (status, body) = self.post(&json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params
        }));
        assert_eq!(status, 200, "{method} on {}: {body}", self.path);
        let resp: Value = serde_json::from_str(&body).expect("JSON-RPC response");
        assert_eq!(resp["jsonrpc"], json!("2.0"));
        assert_eq!(resp["id"], json!(id), "response id echoes request id");
        assert!(resp.get("error").is_none(), "{method}: {resp}");
        resp["result"].clone()
    }

    fn tool_names(&mut self) -> Vec<String> {
        self.request("tools/list", json!({}))["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }

    fn call(&mut self, name: &str, arguments: Value) -> Value {
        self.request(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        )
    }
}

fn deploy(port: u16, xml: &str) {
    let boundary = "----nanoMcpE2EBoundary";
    let body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"resource\"; filename=\"process.bpmn\"\r\n\
         Content-Type: text/xml\r\n\
         \r\n\
         {xml}\r\n\
         --{boundary}--\r\n"
    );
    let (status, resp) = http(
        port,
        "POST",
        "/v2/deployments",
        &format!("multipart/form-data; boundary={boundary}"),
        &body,
    )
    .expect("deploy");
    assert_eq!(status, 200, "deploy: {resp}");
}

const CLUSTER_TOOLS: [&str; 18] = [
    "getClusterStatus",
    "getTopology",
    "getIncident",
    "resolveIncident",
    "searchIncidents",
    "getProcessDefinition",
    "getProcessDefinitionXml",
    "searchProcessDefinitions",
    "createProcessInstance",
    "getProcessInstance",
    "searchProcessInstances",
    "assignUserTask",
    "completeUserTask",
    "getUserTask",
    "searchUserTaskVariables",
    "searchUserTasks",
    "getVariable",
    "searchVariables",
];

#[test]
fn real_mcp_client_drives_cluster_and_processes_servers() {
    let scratch = ScratchDir::new();
    let node = ServerProcess::boot(&scratch.0.join("journal"), &[]);

    // /mcp/cluster: handshake, the 18 static tools, a real read call.
    let mut cluster = McpClient::connect(node.port, "/mcp/cluster");
    let mut names = cluster.tool_names();
    names.sort();
    let mut expected: Vec<String> = CLUSTER_TOOLS.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(names, expected, "exactly Camunda's 18 cluster tools");
    let topo = cluster.call("getTopology", json!({}));
    assert_eq!(topo["isError"], json!(false), "{topo}");

    // /mcp/processes: a deployed tool-process appears as a dynamic tool.
    deploy(node.port, FIXTURE);
    let mut processes = McpClient::connect(node.port, "/mcp/processes");
    let deadline = Instant::now() + Duration::from_secs(20);
    let tool = loop {
        if let Some(t) = processes
            .tool_names()
            .into_iter()
            .find(|n| n.starts_with("mcp-process-with-inputs_"))
        {
            break t;
        }
        assert!(Instant::now() < deadline, "tool-process never listed");
        std::thread::sleep(Duration::from_millis(100));
    };

    // tools/call starts a real process instance...
    let started = processes.call(
        &tool,
        json!({ "firstName": "Ada", "amount": 3, "businessId": "order-7" }),
    );
    assert_eq!(started["isError"], json!(false), "{started}");
    let pik = started["structuredContent"]["processInstanceKey"]
        .as_u64()
        .expect("processInstanceKey");

    // ...that the cluster server's getProcessInstance then finds.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let got = cluster.call("getProcessInstance", json!({ "processInstanceKey": pik }));
        if got["isError"] == json!(false) {
            let text = got["content"][0]["text"].as_str().unwrap_or_default();
            assert!(text.contains(&pik.to_string()), "{got}");
            break;
        }
        assert!(Instant::now() < deadline, "instance never visible: {got}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn mcp_disabled_node_does_not_mount_endpoints() {
    let scratch = ScratchDir::new();
    let node = ServerProcess::boot(
        &scratch.0.join("journal"),
        &[("NANOBPM_MCP_ENABLED", "false")],
    );
    for path in ["/mcp/cluster", "/mcp/processes"] {
        let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });
        let (status, _) = http(
            node.port,
            "POST",
            path,
            "application/json",
            &frame.to_string(),
        )
        .unwrap();
        assert_eq!(status, 404, "{path} is unmounted when disabled");
    }
}
