//! End-to-end tests for the **stage-1 distributed cluster gateway**.
//!
//! Each test boots TWO real server binaries that form a 2-node, 4-partition
//! cluster (node 0 owns partitions 0 & 2, node 1 owns 1 & 3) and then drives the
//! whole cluster *through a single node*, exactly as the benchmark does (it points
//! all load at one gateway). This proves the three forwarding seams end to end
//! over the real falcon WebSocket transport between the two processes:
//!
//! * **create placement** — creates submitted to one gateway are round-robined
//!   across every partition, so instances are minted on both nodes;
//! * **job aggregation** — a worker (here a REST `activateJobs` long poll) hitting
//!   one gateway is fed jobs from the whole cluster;
//! * **completion routing** — completing those jobs at the same gateway routes
//!   each completion to the partition's owner.
//!
//! Ports are pre-reserved (bind `:0`, capture, drop) because cluster peers must
//! know each other's URLs *before* binding — they cannot use the `PORT=0`
//! print-back trick the single-node harnesses rely on.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration-test target: unwrap/expect are acceptable in test setup/assertions; \
clippy.toml allow-unwrap-in-tests does not cover non-#[test] helper fns in tests/*.rs"
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SERVER_BIN: &str = env!("CARGO_BIN_EXE_nanobpm-gateway-rest-server");
const BASE_PATH: &str = "/v2";
const NUM_PARTITIONS: u64 = 4;

/// Builds a full route under the generated REST base path.
fn path(suffix: &str) -> String {
    format!("{BASE_PATH}{suffix}")
}

/// The partition that owns a key (the key's high bits encode it).
fn partition_of(key: u64) -> u64 {
    key >> 51
}

// ----------------------------------------------------------------------------
// Scratch dir
// ----------------------------------------------------------------------------

/// A unique temp directory that removes itself (and everything under it) on drop.
struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nanobpmn-cluster-e2e-{}-{nanos}-{seq}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self { path }
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// ----------------------------------------------------------------------------
// Server harness
// ----------------------------------------------------------------------------

/// Reserves an OS-assigned free TCP port by binding `127.0.0.1:0` and then
/// dropping the listener, so the port is (very likely) still free a moment later
/// when the server binds it. Needed because cluster peers embed each other's
/// fixed ports in `NANOBPMN_NODES` before any of them start.
fn reserve_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve a free port");
    listener.local_addr().expect("read reserved port").port()
}

/// Drains the server's piped stdout (which would otherwise fill its pipe buffer
/// and block the child) and confirms it reported the expected listening port.
fn drain_stdout(child: &mut Child, expected_port: u16) -> std::thread::JoinHandle<()> {
    let stdout = child.stdout.take().expect("server stdout is piped");
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if let Some(rest) = line.trim().strip_prefix("LISTENING_PORT=")
                        && let Ok(port) = rest.parse::<u16>()
                    {
                        debug_assert_eq!(port, expected_port, "node bound an unexpected port");
                    }
                }
            }
        }
    })
}

fn kill_child_tree(child: &mut Child) {
    #[cfg(unix)]
    {
        let pid = child.id();
        // SAFETY: server children are spawned as process-group leaders below.
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = child.kill();
}

/// One node of the cluster: a running server child, killed and reaped on drop.
struct Node {
    child: Child,
    port: u16,
    stdout_thread: Option<std::thread::JoinHandle<()>>,
}

impl Node {
    /// Boots node `node_id` of a cluster whose member base URLs are `nodes`,
    /// listening on the matching pre-reserved port, with its own data directory.
    fn boot(data_dir: &PathBuf, node_id: u32, nodes: &str, port: u16) -> Self {
        Self::boot_with(data_dir, node_id, nodes, port, &[])
    }

    /// Like [`boot`](Self::boot) but sets additional environment variables (e.g.
    /// `NANOBPMN_JOURNAL_SEGMENTED=1` for the bounded-disk path).
    fn boot_with(
        data_dir: &PathBuf,
        node_id: u32,
        nodes: &str,
        port: u16,
        extra_env: &[(&str, &str)],
    ) -> Self {
        std::fs::create_dir_all(data_dir).expect("create node data dir");
        let mut cmd = Command::new(SERVER_BIN);
        cmd.env("NANOBPMN_DATA_DIR", data_dir)
            .env("NANOBPMN_NODES", nodes)
            .env("NANOBPMN_NODE_ID", node_id.to_string())
            .env("NANOBPMN_PARTITIONS", NUM_PARTITIONS.to_string())
            .env("PORT", port.to_string());
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server binary");
        let stdout_thread = drain_stdout(&mut child, port);
        let node = Self {
            child,
            port,
            stdout_thread: Some(stdout_thread),
        };
        node.wait_until_ready();
        node
    }

    /// Polls a wired route until the node answers HTTP, so callers never race the
    /// bind/serve startup window. Probes `/topology`, which is served locally and
    /// never forwards to a peer, so a node reboots readily even when its peer is
    /// down.
    fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some((status, _)) = try_request(self.port, "GET", &path("/topology"), None) {
                assert_eq!(status, 200, "unexpected readiness probe status");
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("node on port {} never became ready", self.port);
    }

    fn request(&self, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
        try_request(self.port, method, path, body)
            .unwrap_or_else(|| panic!("{method} {path} failed: connection error"))
    }

    fn request_until(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        accept: impl Fn(u16, &str) -> bool,
    ) -> (u16, String) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let (status, resp) = self.request(method, path, body);
            if accept(status, &resp) || Instant::now() >= deadline {
                return (status, resp);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        kill_child_tree(&mut self.child);
        let _ = self.child.wait();
        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
    }
}

/// Minimal HTTP/1.1 client: sends one request with `Connection: close` and reads
/// the whole response to EOF. Returns `(status, body)` or `None` on a connection
/// error (used by the readiness poll).
fn try_request(port: u16, method: &str, path: &str, body: Option<&str>) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("set write timeout");

    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("write request");
    stream.flush().expect("flush request");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let raw = String::from_utf8_lossy(&raw);

    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("parse status line");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Some((status, body))
}

/// Whether a process-definition search response contains the `demo` definition.
fn body_has_demo(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|j| {
            j["items"].as_array().map(|items| {
                items
                    .iter()
                    .any(|i| i["processDefinitionId"].as_str() == Some("demo"))
            })
        })
        .unwrap_or(false)
}

/// Boots a 2-node, 4-partition cluster and waits until BOTH nodes have projected
/// the seeded `demo` definition (proving node 0's startup broadcast reached node
/// 1), so creates can be placed on either node's partitions.
fn boot_cluster(scratch: &ScratchDir) -> (Node, Node) {
    let p0 = reserve_port();
    let p1 = reserve_port();
    let nodes = format!("http://127.0.0.1:{p0},http://127.0.0.1:{p1}");

    let node0 = Node::boot(&scratch.path.join("node0"), 0, &nodes, p0);
    let node1 = Node::boot(&scratch.path.join("node1"), 1, &nodes, p1);

    for node in [&node0, &node1] {
        let (status, body) = node.request_until(
            "POST",
            &path("/process-definitions/search"),
            Some("{}"),
            |status, body| status == 200 && body_has_demo(body),
        );
        assert_eq!(status, 200, "definition search failed: {body}");
        assert!(
            body_has_demo(&body),
            "demo definition never replicated: {body}"
        );
    }
    (node0, node1)
}

/// Creates a `demo` instance through `node`, returning its instance key.
fn create_via(node: &Node) -> u64 {
    let (status, body) = node.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionId":"demo"}"#),
    );
    assert_eq!(status, 200, "create failed: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("create response is JSON");
    json["processInstanceKey"]
        .as_str()
        .expect("processInstanceKey present")
        .parse()
        .expect("numeric instance key")
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[test]
fn creates_at_one_gateway_are_placed_across_the_whole_cluster() {
    let scratch = ScratchDir::new();
    let (node0, node1) = boot_cluster(&scratch);

    // Drive every create through node 0 only. Placement round-robins over all
    // four partitions, so instances must be minted on partitions node 0 owns
    // (0 & 2, in-process) AND partitions node 1 owns (1 & 3, forwarded over the
    // Falcon protocol) — proving cross-node create forwarding.
    let mut seen = [0usize; NUM_PARTITIONS as usize];
    for _ in 0..(NUM_PARTITIONS * 4) {
        let key = create_via(&node0);
        seen[partition_of(key) as usize] += 1;
    }
    for (p, count) in seen.iter().enumerate() {
        assert!(
            *count > 0,
            "no instance landed on partition {p}; create placement did not span the cluster: {seen:?}"
        );
    }

    // node 1 never received a single create directly, yet the cluster minted
    // instances on its partitions — confirming node 0 forwarded them.
    drop(node1);
}

#[test]
fn a_worker_at_one_gateway_drains_jobs_from_the_whole_cluster() {
    let scratch = ScratchDir::new();
    let (node0, _node1) = boot_cluster(&scratch);

    // Create a batch through node 0 so jobs are parked on every partition.
    const N: usize = 16;
    let mut created_partitions = std::collections::HashSet::new();
    for _ in 0..N {
        let key = create_via(&node0);
        created_partitions.insert(partition_of(key));
    }
    assert!(
        created_partitions.len() > 1,
        "test needs jobs on multiple partitions, got {created_partitions:?}"
    );

    // A REST worker that ONLY ever talks to node 0: activate + complete in a loop.
    // Job aggregation must feed it jobs from node 1's partitions too, and each
    // completion must route back to the owning node. Success = every job
    // completes and the set spans partitions owned by BOTH nodes.
    let mut completed_partitions = std::collections::HashSet::new();
    let mut completed = 0usize;
    let deadline = Instant::now() + Duration::from_secs(20);
    while completed < N && Instant::now() < deadline {
        let (status, resp) = node0.request(
            "POST",
            &path("/jobs/activation"),
            Some(r#"{"type":"demo-work","worker":"int-test","maxJobsToActivate":8,"timeout":60000,"requestTimeout":250}"#),
        );
        assert_eq!(status, 200, "activation failed: {resp}");
        let json: serde_json::Value = serde_json::from_str(&resp).expect("activation JSON");
        let jobs = json["jobs"].as_array().expect("jobs array");
        for job in jobs {
            let job_key: u64 = job["jobKey"]
                .as_str()
                .expect("jobKey")
                .parse()
                .expect("numeric");
            completed_partitions.insert(partition_of(job_key));
            let (cstatus, cbody) = node0.request(
                "POST",
                &path(&format!("/jobs/{job_key}/completion")),
                Some(r#"{"variables":{}}"#),
            );
            assert_eq!(cstatus, 204, "completion failed for job {job_key}: {cbody}");
            completed += 1;
        }
    }

    assert_eq!(
        completed, N,
        "every parked job must complete via the single gateway"
    );
    // node 0 owns partitions 0 & 2; node 1 owns 1 & 3. The drained set must
    // include at least one partition from EACH node, proving aggregation +
    // completion routing crossed the node boundary.
    let from_node0 = completed_partitions.iter().any(|p| p % 2 == 0);
    let from_node1 = completed_partitions.iter().any(|p| p % 2 == 1);
    assert!(
        from_node0 && from_node1,
        "drained jobs must span both nodes' partitions, got {completed_partitions:?}"
    );
}

/// Bounded-disk clustered recovery: with the segmented journal enabled, a node
/// that does NOT own the deployment partition (node 1 owns 1 & 3) must recover a
/// replicated `ProcessDeployed` — and the instances that reference it — purely
/// from its own on-disk journal after an independent restart, with its peer
/// down. This exercises the segmented clustered boot path (`recover_multi` +
/// write-tag demux + the version-guarded deployment broadcast).
#[test]
fn segmented_node_recovers_replicated_deployment_after_restart() {
    let scratch = ScratchDir::new();
    let seg: &[(&str, &str)] = &[("NANOBPMN_JOURNAL_SEGMENTED", "1")];

    let p0 = reserve_port();
    let p1 = reserve_port();
    let nodes = format!("http://127.0.0.1:{p0},http://127.0.0.1:{p1}");
    let dir0 = scratch.path.join("node0");
    let dir1 = scratch.path.join("node1");

    let node0 = Node::boot_with(&dir0, 0, &nodes, p0, seg);
    let node1 = Node::boot_with(&dir1, 1, &nodes, p1, seg);

    // Both nodes must see the replicated demo definition before we proceed.
    for node in [&node0, &node1] {
        let (status, body) = node.request_until(
            "POST",
            &path("/process-definitions/search"),
            Some("{}"),
            |status, body| status == 200 && body_has_demo(body),
        );
        assert_eq!(status, 200, "definition search failed: {body}");
        assert!(body_has_demo(&body), "demo never replicated: {body}");
    }

    // Create instances until at least one lands on a partition node 1 owns
    // (1 or 3), so we have durable node-1 state that depends on the replicated
    // definition.
    let mut node1_instance: Option<u64> = None;
    for _ in 0..(NUM_PARTITIONS * 4) {
        let key = create_via(&node0);
        if partition_of(key) % 2 == 1 {
            node1_instance = Some(key);
            break;
        }
    }
    let node1_instance = node1_instance.expect("an instance must land on a node-1 partition");

    // Bring the WHOLE cluster down, then reboot ONLY node 1 (its peer stays
    // down), so nothing can re-replicate the definition to it: it must recover
    // the definition and the instance from its own segmented journal alone.
    drop(node0);
    drop(node1);
    let node1 = Node::boot_with(&dir1, 1, &nodes, p1, seg);

    let (status, body) = node1.request("POST", &path("/process-definitions/search"), Some("{}"));
    assert_eq!(status, 200, "post-restart definition search failed: {body}");
    assert!(
        body_has_demo(&body),
        "node 1 lost the replicated demo definition across a segmented restart: {body}"
    );

    let (istatus, ibody) = node1.request(
        "GET",
        &path(&format!("/process-instances/{node1_instance}")),
        None,
    );
    assert_eq!(
        istatus, 200,
        "node 1 lost instance {node1_instance} across a segmented restart: {ibody}"
    );
}

// ----------------------------------------------------------------------------
// Falcon create placement
// ----------------------------------------------------------------------------

/// A tiny blocking WebSocket text client — HTTP upgrade, masked client text
/// frames (a zero mask is the identity transform, valid per RFC 6455), and a
/// frame reader that skips non-text/control frames. Enough to drive
/// `createInstance` over `/falcon` and read each `commandResult`.
struct WsClient {
    stream: TcpStream,
}

impl WsClient {
    fn connect(port: u16) -> Self {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect ws");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("set read timeout");
        let request = "GET /falcon HTTP/1.1\r\n\
             Host: localhost\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n";
        stream
            .write_all(request.as_bytes())
            .expect("write ws handshake");
        // Read response headers byte-by-byte up to the blank line so following
        // WebSocket frame bytes stay unread on the socket.
        let mut headers = Vec::new();
        let mut byte = [0u8; 1];
        while stream.read_exact(&mut byte).is_ok() {
            headers.push(byte[0]);
            if headers.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let status_line = String::from_utf8_lossy(&headers).to_string();
        assert!(
            status_line.contains("101"),
            "expected 101 Switching Protocols, got: {status_line}"
        );
        Self { stream }
    }

    /// Sends a JSON value as one masked text frame (zero mask = identity).
    fn send(&mut self, value: &serde_json::Value) {
        let payload = serde_json::to_vec(value).expect("serialize client frame");
        let mut frame = vec![0x81u8]; // FIN + text opcode.
        let len = payload.len();
        if len < 126 {
            frame.push(0x80 | len as u8);
        } else if len <= u16::MAX as usize {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
        frame.extend_from_slice(&[0, 0, 0, 0]); // zero mask key.
        frame.extend_from_slice(&payload);
        self.stream.write_all(&frame).expect("write ws frame");
        self.stream.flush().expect("flush ws frame");
    }

    /// Reads one frame as `(opcode, payload)`, unmasking if needed.
    fn recv_frame(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0u8; 2];
        self.stream.read_exact(&mut header).expect("read ws header");
        let opcode = header[0] & 0x0F;
        let masked = header[1] & 0x80 != 0;
        let mut len = (header[1] & 0x7F) as usize;
        if len == 126 {
            let mut ext = [0u8; 2];
            self.stream.read_exact(&mut ext).expect("read ext len");
            len = u16::from_be_bytes(ext) as usize;
        } else if len == 127 {
            let mut ext = [0u8; 8];
            self.stream.read_exact(&mut ext).expect("read ext len");
            len = u64::from_be_bytes(ext) as usize;
        }
        let mut mask = [0u8; 4];
        if masked {
            self.stream.read_exact(&mut mask).expect("read mask");
        }
        let mut payload = vec![0u8; len];
        self.stream
            .read_exact(&mut payload)
            .expect("read ws payload");
        if masked {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= mask[i % 4];
            }
        }
        (opcode, payload)
    }

    /// Reads text frames until one whose `"type"` is `wanted`, skipping the
    /// rest (welcome/submissionCredits/heartbeat/...). Panics on close/timeout.
    fn recv_until(&mut self, wanted: &str) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let (opcode, payload) = self.recv_frame();
            match opcode {
                0x1 => {
                    let value: serde_json::Value =
                        serde_json::from_slice(&payload).expect("server frame is JSON");
                    if value["type"].as_str() == Some(wanted) {
                        return value;
                    }
                }
                0x8 => panic!("server closed the connection while awaiting {wanted}"),
                _ => {}
            }
        }
        panic!("timed out awaiting {wanted}");
    }
}

#[test]
fn stream_creates_at_one_gateway_are_placed_across_the_whole_cluster() {
    let scratch = ScratchDir::new();
    let (node0, node1) = boot_cluster(&scratch);

    // Drive every create through node 0's FALCON (fire-and-forget), the
    // path benchmark producers and the SDK use. Before stream-create placement
    // existed, all of these landed on node 0's own partitions only; the per-node
    // metrics then (correctly) showed every instance on one node. Placement must
    // now round-robin over all four partitions, minting instances on node 0's
    // (0 & 2, in-process) AND node 1's (1 & 3, forwarded over the command
    // stream) — matching the REST create-placement guarantee.
    let mut ws = WsClient::connect(node0.port);
    let mut seen = [0usize; NUM_PARTITIONS as usize];
    for corr in 0..(NUM_PARTITIONS * 4) {
        ws.send(&serde_json::json!({
            "type": "createInstance",
            "corr": corr,
            "processDefinitionId": "demo",
        }));
        let result = ws.recv_until("commandResult");
        assert_eq!(
            result["status"].as_u64(),
            Some(200),
            "stream create failed: {result}"
        );
        let key: u64 = result["body"]["processInstanceKey"]
            .as_str()
            .expect("processInstanceKey present")
            .parse()
            .expect("numeric instance key");
        seen[partition_of(key) as usize] += 1;
    }
    for (p, count) in seen.iter().enumerate() {
        assert!(
            *count > 0,
            "no stream-created instance landed on partition {p}; stream create \
             placement did not span the cluster: {seen:?}"
        );
    }

    // node 1 never received a create directly over its own socket, yet the
    // cluster minted instances on its partitions — proving node 0 forwarded
    // fire-and-forget stream creates to it.
    drop(node1);
}
