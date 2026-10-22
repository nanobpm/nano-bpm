//! End-to-end tests for the unified bidirectional Falcon protocol (WebSocket).
//!
//! Each test boots the *real* server binary on an ephemeral port over a throwaway
//! journal, opens a WebSocket to `/falcon`, and drives the full lifecycle
//! the stream is meant to carry: subscribe → job push → complete, and
//! create-with-await → async `InstanceCompleted`. The client is a tiny,
//! dependency-free RFC 6455 implementation (hardcoded handshake key, zero-masked
//! client frames) so the test suite keeps the same no-extra-deps philosophy as
//! the HTTP journal-replay e2e harness.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration-test target: unwrap/expect are acceptable in test setup/assertions; \
clippy.toml allow-unwrap-in-tests does not cover non-#[test] helper fns in tests/*.rs"
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

/// Path to the compiled server binary, provided by Cargo for integration tests.
const SERVER_BIN: &str = env!("CARGO_BIN_EXE_nanobpm-gateway-rest-server");

/// The seeded demo process is `start → service_task("work", "demo-work") → end`.
const DEMO_JOB_TYPE: &str = "demo-work";

// ----------------------------------------------------------------------------
// Server harness (mirrors journal_replay_e2e.rs, trimmed to what these tests need)
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
            "nanobpmn-cs-e2e-{}-{nanos}-{seq}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self { path }
    }

    fn journal_path(&self) -> PathBuf {
        self.path.join("test.journal")
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Reads the `LISTENING_PORT=<n>` line the server prints once it has bound.
fn read_listening_port(child: &mut Child) -> (u16, std::thread::JoinHandle<()>) {
    let stdout = child.stdout.take().expect("server stdout is piped");
    let (tx, rx) = std::sync::mpsc::channel();
    let stdout_thread = std::thread::spawn(move || {
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
                        let _ = tx.send(port);
                        // Keep draining stdout to EOF rather than returning:
                        // dropping the read end here SIGPIPEs the child's later
                        // startup banner (the `console` build prints one),
                        // panicking and killing the server before it serves.
                    }
                }
            }
        }
    });
    let port = match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(port) => port,
        Err(e) => {
            kill_child_tree(child);
            let _ = child.wait();
            let _ = stdout_thread.join();
            panic!("server never reported its listening port: {e}");
        }
    };
    (port, stdout_thread)
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

/// A running server child process, killed and reaped on drop.
struct ServerProcess {
    child: Child,
    port: u16,
    stdout_thread: Option<std::thread::JoinHandle<()>>,
}

impl ServerProcess {
    fn boot(journal: &Path, env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(SERVER_BIN);
        command
            .env("NANOBPMN_JOURNAL", journal)
            .env("PORT", "0")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        #[cfg(unix)]
        command.process_group(0);
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawn server binary");
        let (port, stdout_thread) = read_listening_port(&mut child);
        let server = Self {
            child,
            port,
            stdout_thread: Some(stdout_thread),
        };
        server.wait_until_ready();
        server
    }

    /// Polls a wired route until the server answers HTTP, so callers never race
    /// the bind/serve startup window.
    fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", self.port)) {
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let req = "GET /v2/process-instances/0 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
                if stream.write_all(req.as_bytes()).is_ok() {
                    let mut buf = Vec::new();
                    let _ = stream.read_to_end(&mut buf);
                    if !buf.is_empty() {
                        return;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {} never became ready", self.port);
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        kill_child_tree(&mut self.child);
        let _ = self.child.wait();
        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
    }
}

// ----------------------------------------------------------------------------
// Minimal synchronous WebSocket client (RFC 6455)
// ----------------------------------------------------------------------------

/// A blocking WebSocket text-frame client. Client frames are sent with a
/// zero-valued mask (valid per RFC 6455 — masking with 0x00000000 leaves the
/// payload unchanged); server frames arrive unmasked.
struct WsClient {
    stream: TcpStream,
    /// Text frames read while awaiting a different frame type. A `Job` push can
    /// race ahead of the `CommandResult` for the create that spawned it, so
    /// `recv_until` must buffer (not discard) frames it isn't currently waiting
    /// for, or a later `recv_until(&["job"])` would block forever.
    buffered: std::collections::VecDeque<Value>,
}

impl WsClient {
    /// Performs the HTTP upgrade handshake and returns a ready client.
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

        // Read the 101 response headers up to (and including) the blank line,
        // leaving any following WebSocket frame bytes unread on the stream.
        let status_line = read_http_headers(&mut stream);
        assert!(
            status_line.contains("101"),
            "expected 101 Switching Protocols, got: {status_line}"
        );
        Self {
            stream,
            buffered: std::collections::VecDeque::new(),
        }
    }

    /// Sends a JSON value as a single masked text frame.
    fn send(&mut self, value: &Value) {
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
        frame.extend_from_slice(&payload); // XOR with zero mask == identity.
        self.stream.write_all(&frame).expect("write ws frame");
        self.stream.flush().expect("flush ws frame");
    }

    /// Reads a single frame, returning `(opcode, payload)`.
    fn recv_frame(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0u8; 2];
        self.read_exact(&mut header);
        let opcode = header[0] & 0x0F;
        let masked = header[1] & 0x80 != 0;
        let mut len = (header[1] & 0x7F) as usize;
        if len == 126 {
            let mut ext = [0u8; 2];
            self.read_exact(&mut ext);
            len = u16::from_be_bytes(ext) as usize;
        } else if len == 127 {
            let mut ext = [0u8; 8];
            self.read_exact(&mut ext);
            len = u64::from_be_bytes(ext) as usize;
        }
        let mut mask = [0u8; 4];
        if masked {
            self.read_exact(&mut mask);
        }
        let mut payload = vec![0u8; len];
        self.read_exact(&mut payload);
        if masked {
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[i % 4];
            }
        }
        (opcode, payload)
    }

    /// Reads frames until a JSON text frame whose `"type"` is one of `wanted`,
    /// skipping heartbeats and buffering any other typed frame (so a frame read
    /// here is still available to a later call). Panics on timeout/close.
    fn recv_until(&mut self, wanted: &[&str]) -> Value {
        // A matching frame may already be buffered from an earlier call.
        if let Some(pos) = self
            .buffered
            .iter()
            .position(|v| wanted.contains(&v["type"].as_str().unwrap_or("")))
        {
            return self.buffered.remove(pos).expect("buffered frame present");
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let (opcode, payload) = self.recv_frame();
            match opcode {
                0x1 => {
                    let value: Value =
                        serde_json::from_slice(&payload).expect("server frame is JSON");
                    let frame_type = value["type"].as_str().unwrap_or("");
                    if wanted.contains(&frame_type) {
                        return value;
                    }
                    // Not what we're waiting for (welcome/submissionCredits/job/
                    // heartbeat/pressure/...): buffer it so a later call can find
                    // it instead of it being lost.
                    self.buffered.push_back(value);
                }
                0x8 => panic!("server closed the connection while awaiting {wanted:?}"),
                // Ping/pong/continuation: ignore.
                _ => {}
            }
        }
        panic!("timed out awaiting one of {wanted:?}");
    }

    fn read_exact(&mut self, buf: &mut [u8]) {
        self.stream.read_exact(buf).expect("read ws bytes");
    }

    /// Waits up to `within` for a `job` frame, returning it (or `None` on
    /// timeout/close). Skips unrelated frames. Used to assert the *absence* of a
    /// duplicate redelivery: a single leased job must not be pushed again before
    /// the worker completes it. Mutates the socket read timeout.
    fn recv_job_within(&mut self, within: Duration) -> Option<Value> {
        // A buffered job frame already counts as delivery within the window.
        if let Some(pos) = self
            .buffered
            .iter()
            .position(|v| v["type"].as_str() == Some("job"))
        {
            return self.buffered.remove(pos);
        }
        self.stream
            .set_read_timeout(Some(within))
            .expect("set read timeout");
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            match self.try_recv_frame() {
                Some((0x1, payload)) => {
                    let value: Value =
                        serde_json::from_slice(&payload).expect("server frame is JSON");
                    if value["type"].as_str() == Some("job") {
                        return Some(value);
                    }
                }
                Some((0x8, _)) | None => return None,
                Some(_) => {}
            }
        }
        None
    }

    /// Like [`Self::recv_frame`] but returns `None` if a read times out or the
    /// stream ends, instead of panicking.
    fn try_recv_frame(&mut self) -> Option<(u8, Vec<u8>)> {
        let mut header = [0u8; 2];
        if self.stream.read_exact(&mut header).is_err() {
            return None;
        }
        let opcode = header[0] & 0x0F;
        let masked = header[1] & 0x80 != 0;
        let mut len = (header[1] & 0x7F) as usize;
        if len == 126 {
            let mut ext = [0u8; 2];
            self.stream.read_exact(&mut ext).ok()?;
            len = u16::from_be_bytes(ext) as usize;
        } else if len == 127 {
            let mut ext = [0u8; 8];
            self.stream.read_exact(&mut ext).ok()?;
            len = u64::from_be_bytes(ext) as usize;
        }
        let mut mask = [0u8; 4];
        if masked {
            self.stream.read_exact(&mut mask).ok()?;
        }
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).ok()?;
        if masked {
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[i % 4];
            }
        }
        Some((opcode, payload))
    }

    /// Waits up to `within` for the server to close the connection, returning
    /// `true` only on a genuine Close frame (0x8) or EOF — **not** on a read
    /// timeout. Skips any other frames. Used to assert the reaper drops a silent
    /// (phantom) client.
    fn wait_for_close(&mut self, within: Duration) -> bool {
        // Poll in short slices so a read timeout doesn't masquerade as a close.
        self.stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            let mut header = [0u8; 2];
            match self.stream.read_exact(&mut header) {
                Ok(()) => {
                    // A frame header arrived; a Close opcode means the server shut
                    // us down. (We don't bother draining other frame bodies here —
                    // the reaper sends no data frames, so anything else is benign.)
                    if header[0] & 0x0F == 0x8 {
                        return true;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // No data this slice; keep waiting until the overall deadline.
                }
                // EOF or any other error: the socket is gone — a close.
                Err(_) => return true,
            }
        }
        false
    }
}

/// Reads an HTTP response header block byte-by-byte up to the terminating blank
/// line, returning the whole header text (frame bytes that follow stay buffered
/// in the socket).
fn read_http_headers(stream: &mut TcpStream) -> String {
    let mut headers = Vec::new();
    let mut byte = [0u8; 1];
    while stream.read_exact(&mut byte).is_ok() {
        headers.push(byte[0]);
        if headers.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&headers).to_string()
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[test]
fn welcome_grants_an_initial_submission_window() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path(), &[]);

    let mut ws = WsClient::connect(server.port);
    let welcome = ws.recv_until(&["welcome"]);
    assert!(
        welcome["submissionCredits"].as_i64().unwrap_or(0) > 0,
        "welcome should grant a positive submission window: {welcome}"
    );
    assert!(welcome["heartbeatMs"].as_u64().unwrap_or(0) > 0);
}

#[test]
fn subscribe_create_push_and_complete_a_job() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path(), &[]);

    let mut ws = WsClient::connect(server.port);
    ws.recv_until(&["welcome"]);

    // Subscribe to the demo job type with ample delivery credit.
    ws.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 10,
    }));

    // Create a demo instance; expect a 200 CommandResult correlated by `corr`.
    ws.send(&json!({
        "type": "createInstance",
        "corr": 1,
        "processDefinitionId": "demo",
    }));
    let result = ws.recv_until(&["commandResult"]);
    assert_eq!(result["corr"].as_u64(), Some(1));
    assert_eq!(
        result["status"].as_u64(),
        Some(200),
        "create failed: {result}"
    );

    // The dispatcher should push the parked job to our subscription.
    let job_frame = ws.recv_until(&["job"]);
    let job = &job_frame["job"];
    assert_eq!(job["type"].as_str(), Some(DEMO_JOB_TYPE));
    let job_key = job["jobKey"]
        .as_str()
        .expect("pushed job carries a string jobKey")
        .to_string();

    // Complete it; expect a 200 CommandResult.
    ws.send(&json!({
        "type": "completeJob",
        "corr": 2,
        "jobKey": job_key,
    }));
    let complete = ws.recv_until(&["commandResult"]);
    assert_eq!(complete["corr"].as_u64(), Some(2));
    assert_eq!(
        complete["status"].as_u64(),
        Some(200),
        "complete failed: {complete}"
    );
}

#[test]
fn create_with_await_completion_emits_instance_completed() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path(), &[]);

    // Two sockets: a worker that drains the job, and a submitter that awaits
    // completion. (await-completion only resolves once the job is completed.)
    let mut worker = WsClient::connect(server.port);
    worker.recv_until(&["welcome"]);
    worker.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 10,
    }));

    let mut submitter = WsClient::connect(server.port);
    submitter.recv_until(&["welcome"]);
    submitter.send(&json!({
        "type": "createInstance",
        "corr": 7,
        "processDefinitionId": "demo",
        "awaitCompletion": true,
    }));
    let result = submitter.recv_until(&["commandResult"]);
    assert_eq!(result["corr"].as_u64(), Some(7));
    assert_eq!(result["status"].as_u64(), Some(200));

    // Drain the job on the worker socket so the instance can finish.
    let job_frame = worker.recv_until(&["job"]);
    let job_key = job_frame["job"]["jobKey"]
        .as_str()
        .expect("worker receives a job")
        .to_string();
    worker.send(&json!({
        "type": "completeJob",
        "corr": 1,
        "jobKey": job_key,
    }));
    let complete = worker.recv_until(&["commandResult"]);
    assert_eq!(complete["status"].as_u64(), Some(200));

    // The submitter should now receive the async completion, routed by `corr`.
    let completed = submitter.recv_until(&["instanceCompleted"]);
    assert_eq!(completed["corr"].as_u64(), Some(7));
    assert_eq!(
        completed["processCompleted"].as_bool(),
        Some(true),
        "instance should have completed: {completed}"
    );
    assert!(completed["processInstanceKey"].as_str().is_some());
}

#[test]
fn a_malformed_frame_is_rejected_without_dropping_the_socket() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path(), &[]);

    let mut ws = WsClient::connect(server.port);
    ws.recv_until(&["welcome"]);

    // Send a syntactically invalid frame (unknown type / not a known variant).
    ws.send(&json!({ "type": "totallyBogusFrame" }));
    let err = ws.recv_until(&["commandResult"]);
    assert_eq!(err["status"].as_u64(), Some(400), "expected 400: {err}");

    // The socket must remain usable: a valid create still succeeds afterward.
    ws.send(&json!({
        "type": "createInstance",
        "corr": 99,
        "processDefinitionId": "demo",
    }));
    let ok = ws.recv_until(&["commandResult"]);
    assert_eq!(ok["corr"].as_u64(), Some(99));
    assert_eq!(ok["status"].as_u64(), Some(200), "create failed: {ok}");
}

#[test]
fn await_instance_recovers_completion_on_a_fresh_socket() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path(), &[]);

    // A worker drains the demo job so the instance can complete.
    let mut worker = WsClient::connect(server.port);
    worker.recv_until(&["welcome"]);
    worker.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 10,
    }));

    // First socket creates an instance *without* awaiting, then "disconnects"
    // (dropped below). It only keeps the processInstanceKey from the create ack —
    // exactly what a client would persist for recovery.
    let instance_key = {
        let mut submitter = WsClient::connect(server.port);
        submitter.recv_until(&["welcome"]);
        submitter.send(&json!({
            "type": "createInstance",
            "corr": 1,
            "processDefinitionId": "demo",
        }));
        let result = submitter.recv_until(&["commandResult"]);
        assert_eq!(result["status"].as_u64(), Some(200));
        result["body"]["processInstanceKey"]
            .as_str()
            .expect("create ack carries the instance key")
            .to_string()
        // submitter drops here, simulating a lost connection.
    };

    // Complete the job so the instance reaches a terminal state while the
    // submitter is gone.
    let job_frame = worker.recv_until(&["job"]);
    let job_key = job_frame["job"]["jobKey"].as_str().unwrap().to_string();
    worker.send(&json!({ "type": "completeJob", "corr": 1, "jobKey": job_key }));
    assert_eq!(
        worker.recv_until(&["commandResult"])["status"].as_u64(),
        Some(200)
    );

    // A brand-new socket re-awaits by key and recovers the terminal outcome
    // immediately (the read model is durable history).
    let mut reconnect = WsClient::connect(server.port);
    reconnect.recv_until(&["welcome"]);
    reconnect.send(&json!({
        "type": "awaitInstance",
        "corr": 42,
        "processInstanceKey": instance_key,
    }));
    let completed = reconnect.recv_until(&["instanceCompleted"]);
    assert_eq!(completed["corr"].as_u64(), Some(42));
    assert_eq!(
        completed["processCompleted"].as_bool(),
        Some(true),
        "re-await should recover completion: {completed}"
    );
    assert_eq!(
        completed["processInstanceKey"].as_str(),
        Some(instance_key.as_str())
    );
}

#[test]
fn await_instance_rejects_an_invalid_key() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path(), &[]);

    let mut ws = WsClient::connect(server.port);
    ws.recv_until(&["welcome"]);
    ws.send(&json!({
        "type": "awaitInstance",
        "corr": 5,
        "processInstanceKey": "not-a-key",
    }));
    let err = ws.recv_until(&["commandResult"]);
    assert_eq!(err["corr"].as_u64(), Some(5));
    assert_eq!(err["status"].as_u64(), Some(404), "expected 404: {err}");
}

#[test]
fn a_subscribe_without_a_timeout_does_not_redeliver_an_in_flight_job() {
    // Regression: a `Subscribe` that omits `timeout` must apply a sane default
    // job lock, not a 0ms lock. With a 0ms lock every leased job is instantly
    // re-activatable (`deadline == now`), so the dispatcher re-pushes it on the
    // next backstop tick before the worker completes it — surfacing as duplicate
    // delivery. We subscribe without a timeout, take delivery of one job, and
    // assert no second copy arrives across several backstop ticks.
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path(), &[]);

    let mut ws = WsClient::connect(server.port);
    ws.recv_until(&["welcome"]);

    // Subscribe WITHOUT a `timeout` field (the regression trigger).
    ws.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 10,
    }));

    // Create a single instance; its one job should be pushed exactly once.
    ws.send(&json!({
        "type": "createInstance",
        "corr": 1,
        "processDefinitionId": "demo",
    }));
    assert_eq!(
        ws.recv_until(&["commandResult"])["status"].as_u64(),
        Some(200)
    );

    let first = ws.recv_until(&["job"]);
    let job_key = first["job"]["jobKey"]
        .as_str()
        .expect("pushed job carries a key")
        .to_string();

    // Do NOT complete the job. Across ~5 backstop ticks (DISPATCH_TICK_MS=200ms),
    // a correctly-locked job must not be redelivered.
    let redelivered = ws.recv_job_within(Duration::from_millis(1200));
    assert!(
        redelivered.is_none(),
        "job {job_key} was redelivered before completion (lock too short): {redelivered:?}"
    );
}

#[test]
fn a_silent_client_is_reaped_as_a_phantom() {
    // A frozen/partitioned client whose TCP stays open (no FIN) would otherwise
    // hold its dispatch slot and credits indefinitely. With a short liveness
    // deadline the reaper must close the socket once the client stops
    // heartbeating.
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(
        &scratch.journal_path(),
        &[
            ("NANOBPMN_STREAM_LIVENESS_MS", "500"),
            ("NANOBPMN_STREAM_REAPER_MS", "150"),
        ],
    );

    let mut ws = WsClient::connect(server.port);
    ws.recv_until(&["welcome"]);
    ws.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 10,
    }));

    // Now go silent: send no further frames (no client heartbeats). The server
    // should reap us and close the socket within a couple of liveness windows.
    assert!(
        ws.wait_for_close(Duration::from_secs(4)),
        "server did not reap a silent client within the liveness deadline"
    );
}

#[test]
fn a_heartbeating_client_is_not_reaped() {
    // The reaper must not drop a healthy-but-idle client that keeps heartbeating.
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(
        &scratch.journal_path(),
        &[
            ("NANOBPMN_STREAM_LIVENESS_MS", "500"),
            ("NANOBPMN_STREAM_REAPER_MS", "150"),
        ],
    );

    let mut ws = WsClient::connect(server.port);
    ws.recv_until(&["welcome"]);

    // Heartbeat every 200ms for ~1.5s — comfortably past the 500ms deadline and
    // several reaper scans — then confirm the socket is still alive by completing
    // a normal create/push/complete round-trip.
    ws.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 10,
    }));
    for _ in 0..7 {
        ws.send(&json!({ "type": "heartbeat" }));
        std::thread::sleep(Duration::from_millis(200));
    }

    ws.send(&json!({
        "type": "createInstance",
        "corr": 1,
        "processDefinitionId": "demo",
    }));
    assert_eq!(
        ws.recv_until(&["commandResult"])["status"].as_u64(),
        Some(200),
        "a heartbeating client was wrongly reaped (create failed)"
    );
    let job_key = ws.recv_until(&["job"])["job"]["jobKey"]
        .as_str()
        .expect("pushed job carries a key")
        .to_string();
    ws.send(&json!({
        "type": "completeJob",
        "corr": 2,
        "jobKey": job_key,
    }));
    assert_eq!(
        ws.recv_until(&["commandResult"])["status"].as_u64(),
        Some(200)
    );
}

#[test]
fn a_drained_peer_does_not_starve_a_live_worker_under_the_governor() {
    // Issue #468: under the worker-concurrency governor (a narrow per-job-type
    // active dispatch width), a subscriber whose credits have drained to zero — a
    // worker that died or wedged and stopped replenishing — must not consume the
    // scarce per-pass width and starve a live, credited worker. Pin a fixed active
    // width of 1 so every dispatch pass has exactly one slot to spend, park a
    // zero-credit peer in the round-robin, and assert a live worker still drains
    // fresh jobs promptly — well inside the ~15s heartbeat-reap window the bug
    // used to make callers wait for. (The rigorous "never selects a zero-credit
    // subscriber" invariant is proven at the selection layer by the unit tests in
    // `falcon::registry_tests`; this is the end-to-end behavioural guard.)
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(
        &scratch.journal_path(),
        &[("NANOBPMN_WORKER_CONCURRENCY", "1")],
    );

    // Drained peer: subscribes with a single credit, takes exactly one job, and
    // never completes it. Its credits are now 0 but it stays connected, parked in
    // the width-1 rotation — the shape of a worker that died mid-flight.
    let mut drained = WsClient::connect(server.port);
    drained.recv_until(&["welcome"]);
    drained.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 1,
    }));

    let mut submitter = WsClient::connect(server.port);
    submitter.recv_until(&["welcome"]);
    submitter.send(&json!({
        "type": "createInstance",
        "corr": 1,
        "processDefinitionId": "demo",
    }));
    assert_eq!(
        submitter.recv_until(&["commandResult"])["status"].as_u64(),
        Some(200),
        "drain-priming create failed"
    );
    // The drained peer takes its one job and holds it (never completes) — credits
    // are now exhausted.
    let held = drained.recv_until(&["job"]);
    assert_eq!(held["job"]["type"].as_str(), Some(DEMO_JOB_TYPE));

    // A live, credited worker joins the same job type.
    let mut live = WsClient::connect(server.port);
    live.recv_until(&["welcome"]);
    live.send(&json!({
        "type": "subscribe",
        "jobType": DEMO_JOB_TYPE,
        "jobCredits": 10,
    }));

    // Submit several fresh instances. Despite the zero-credit peer sitting in the
    // width-1 rotation, the live worker must receive and drain them all promptly.
    const FRESH_JOBS: u64 = 3;
    for corr in 2..2 + FRESH_JOBS {
        submitter.send(&json!({
            "type": "createInstance",
            "corr": corr,
            "processDefinitionId": "demo",
        }));
        assert_eq!(
            submitter.recv_until(&["commandResult"])["status"].as_u64(),
            Some(200),
            "create {corr} failed"
        );
    }

    for n in 0..FRESH_JOBS {
        let job = live
            .recv_job_within(Duration::from_secs(5))
            .unwrap_or_else(|| {
                panic!(
                    "live worker starved on job {n}: a drained peer stalled the width-1 governor"
                )
            });
        let job_key = job["job"]["jobKey"]
            .as_str()
            .expect("pushed job carries a key")
            .to_string();
        live.send(&json!({
            "type": "completeJob",
            "corr": 100 + n,
            "jobKey": job_key,
        }));
        assert_eq!(
            live.recv_until(&["commandResult"])["status"].as_u64(),
            Some(200),
            "complete of fresh job {n} failed"
        );
    }
}
