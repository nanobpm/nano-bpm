//! End-to-end regression test for issue #1594: the response-contract guard must
//! never turn a successful, state-changing `activateJobs` into an error the
//! client can't recover from.
//!
//! The guard (`server/src/response_contract.rs`) buffers a response body to
//! validate it against the OpenAPI contract, capped at
//! `NANOBPM_RESPONSE_VALIDATION_MAX_BYTES` (default 8 MiB). Before the fix, an
//! activateJobs response larger than that cap was replaced with a 500
//! `response_contract_buffer_failed` — but the engine had ALREADY activated
//! (locked) the jobs, so no worker ever received them and they stayed locked
//! until their job timeout. An overloaded worker then kept polling, each poll
//! failed, and each failure locked another batch. The fix detects the
//! oversized response BEFORE consuming it (via `Content-Length` or the body's
//! own exact size hint — every first-party endpoint serializes to a single
//! known-length buffer) and passes it through unvalidated.
//!
//! This harness boots the real server binary with a tiny validation cap,
//! creates jobs whose activation response exceeds it, and asserts the
//! acceptance criterion: `activateJobs` either returns 200 with jobs, or
//! returns an error AND leaves every job immediately activatable. No job may
//! stay locked by a response the client never received.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration-test target: unwrap/expect are acceptable in test setup/assertions; \
clippy.toml allow-unwrap-in-tests does not cover non-#[test] helper fns in tests/*.rs"
)]

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::server_command;

/// The generated REST layer mounts every route under this base path.
const BASE_PATH: &str = "/v2";

/// The validation cap the test server runs with: small enough that a handful
/// of jobs with a sizeable variable payload exceed it, large enough that the
/// create/deploy responses stay under it.
const VALIDATION_CAP: &str = "16384";

/// A unique temp directory that removes itself (and everything under it) on
/// drop, so a passing or panicking test never leaves files behind.
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
        let path =
            std::env::temp_dir().join(format!("nanobpmn-e2e-{}-{nanos}-{seq}", std::process::id()));
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

/// Reads the `LISTENING_PORT=<n>` line the server prints on stdout once it has
/// bound its OS-assigned port. A background reader thread keeps the pipe
/// drained and bridges a `recv_timeout`, so a server that dies before binding
/// surfaces as a timeout rather than a hang.
fn read_listening_port(child: &mut Child) -> (u16, std::thread::JoinHandle<()>) {
    let stdout = child.stdout.take().expect("server stdout is piped");
    let (tx, rx) = std::sync::mpsc::channel();
    let stdout_thread = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break, // EOF (process exited) or read error.
                Ok(_) => {
                    if let Some(rest) = line.trim().strip_prefix("LISTENING_PORT=")
                        && let Ok(port) = rest.parse::<u16>()
                    {
                        let _ = tx.send(port);
                        // Keep draining stdout to EOF rather than returning:
                        // dropping the read end here SIGPIPEs the child's later
                        // startup banner, panicking and killing the server
                        // before it serves.
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
        // SAFETY: server children are spawned as process-group leaders by
        // `common::server_command`.
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = child.kill();
}

/// A running server child process. Killed and reaped on drop, so a panicking
/// assertion can never strand the process.
struct ServerProcess {
    child: Child,
    port: u16,
    stdout_thread: Option<std::thread::JoinHandle<()>>,
}

impl ServerProcess {
    /// Boots the server over `journal` with the response-validation cap pinned
    /// to [`VALIDATION_CAP`], waits until it answers HTTP, and returns the
    /// handle. The server binds an **OS-assigned** port (`PORT=0`) and reports
    /// it back on stdout, so each test gets a unique port with no
    /// reserve-then-rebind race.
    fn boot(journal: &Path) -> Self {
        let mut command = server_command();
        command
            .env("NANOBPMN_JOURNAL", journal)
            .env("PORT", "0")
            // The knob under test: a tiny validation buffer so the oversized
            // activation response exceeds it without needing 8 MiB of jobs.
            // Strict mode is the default, so the guard is fully armed.
            .env("NANOBPM_RESPONSE_VALIDATION_MAX_BYTES", VALIDATION_CAP);
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

    /// Polls a wired route until the server returns an HTTP response, so
    /// callers never race the bind/serve startup window.
    fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some((status, _)) =
                try_request(self.port, "GET", &path("/process-instances/0"), None)
            {
                // Any HTTP status (here: 404 for the bogus key) proves the
                // router is up and handling requests.
                assert_eq!(status, 404, "unexpected readiness probe status");
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {} never became ready", self.port);
    }

    fn request(&self, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
        try_request(self.port, method, path, body)
            .unwrap_or_else(|| panic!("{method} {path} failed: connection error"))
    }

    /// Stops the server and waits for it to exit, surfacing failures
    /// explicitly rather than relying on the drop guard.
    fn shutdown(mut self) {
        kill_child_tree(&mut self.child);
        let _ = self.child.wait();
        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
        // Mark as already reaped so Drop is a no-op.
        self.port = 0;
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if self.port != 0 {
            kill_child_tree(&mut self.child);
            let _ = self.child.wait();
        }
        if let Some(stdout_thread) = self.stdout_thread.take() {
            let _ = stdout_thread.join();
        }
    }
}

/// Builds a full route under the generated REST base path.
fn path(suffix: &str) -> String {
    format!("{BASE_PATH}{suffix}")
}

/// Minimal HTTP/1.1 client: sends one request with `Connection: close` and
/// reads the whole response to EOF. Returns `(status_code, body)` or `None` if
/// the TCP connection could not be established (used by the readiness poll).
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

/// Creates an instance of the pre-seeded `demo` process carrying one large
/// string variable, so its job's activation payload is sizeable.
fn create_demo_instance_with_payload(server: &ServerProcess, payload: &str) -> String {
    let body = format!(r#"{{"processDefinitionId":"demo","variables":{{"payload":"{payload}"}}}}"#);
    let (status, body) = server.request("POST", &path("/process-instances"), Some(&body));
    assert_eq!(status, 200, "create instance failed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("create response is JSON");
    json["processInstanceKey"]
        .as_str()
        .expect("processInstanceKey present")
        .to_string()
}

/// One-shot activation (`requestTimeout: -1` disables long polling) for up to
/// `max_jobs` `demo-work` jobs. Returns `(status, body)`.
fn activate_jobs(server: &ServerProcess, max_jobs: u32) -> (u16, String) {
    let body = format!(
        r#"{{"type":"demo-work","worker":"w","timeout":60000,"maxJobsToActivate":{max_jobs},"requestTimeout":-1}}"#
    );
    server.request("POST", &path("/jobs/activation"), Some(&body))
}

/// Counts the jobs in a 200 activation response body.
fn activated_job_count(body: &str) -> usize {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|j| j["jobs"].as_array().map(|a| a.len()))
        .unwrap_or(0)
}

/// Acceptance for #1594: an activation batch whose serialized response exceeds
/// the response-validation buffer limit must not leave jobs locked behind a
/// 500 the client never received jobs from.
///
/// With the pre-fix guard the oversized activation returned
/// `500 response_contract_buffer_failed` while the engine had already locked
/// the jobs — every one of them then stayed locked until its job timeout,
/// invisible to every subsequent poll.
#[test]
fn oversized_activation_never_strands_locked_jobs() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();
    let server = ServerProcess::boot(&journal);

    // Given: jobs whose per-job variables (~2 KB) make a batch of 16 exceed
    // the 16 KiB validation cap (16 jobs x ~2 KB payload + envelope > 16 KiB).
    let payload = "x".repeat(2048);
    const TOTAL_JOBS: u32 = 16;
    for _ in 0..TOTAL_JOBS {
        create_demo_instance_with_payload(&server, &payload);
    }

    // When: a worker asks for the whole batch in one activation.
    let (status, body) = activate_jobs(&server, TOTAL_JOBS);

    // Then: the call either succeeds with jobs, or fails WITHOUT locking
    // anything. The pre-fix behaviour — 500 with the jobs locked — is the
    // defect: it fails the drain assertion below because the locked jobs are
    // not activatable until their 60 s timeout.
    let mut delivered: usize = 0;
    if status == 200 {
        delivered = activated_job_count(&body);
        assert!(
            delivered > 0,
            "a 200 activation must carry at least one job: {body}"
        );
    } else {
        assert!(
            body.contains("response_contract") || status == 400 || status == 503,
            "an activation error must be a structured guard/backpressure response, got {status}: {body}"
        );
    }

    // And: every remaining job is immediately activatable — nothing stays
    // locked by a response the client never received. Drain with small,
    // comfortably-under-cap requests and count what the server will still
    // hand out.
    let mut drained = 0usize;
    for _ in 0..TOTAL_JOBS {
        let (status, body) = activate_jobs(&server, 1);
        assert_eq!(status, 200, "single-job activation must succeed: {body}");
        let n = activated_job_count(&body);
        if n == 0 {
            break;
        }
        drained += n;
    }

    assert_eq!(
        delivered + drained,
        TOTAL_JOBS as usize,
        "every created job must be delivered exactly once across the big \
         activation and the drain — a job missing here was locked by a \
         response the client never received (issue #1594): big activation \
         delivered {delivered} (status {status}), drain delivered {drained}"
    );

    server.shutdown();
}

/// Companion acceptance: a conformant, under-cap activation still validates
/// and passes untouched (strict validation stays the default and keeps
/// working), and the jobs it returns are real, locked activations (a second
/// poll sees nothing until the first batch's timeout).
#[test]
fn under_cap_activation_validates_and_locks_normally() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();
    let server = ServerProcess::boot(&journal);

    // Given: two jobs with small variables, so the activation response stays
    // under the 16 KiB cap and goes through full validation.
    for _ in 0..2 {
        create_demo_instance_with_payload(&server, "small");
    }

    // When: a worker activates both.
    let (status, body) = activate_jobs(&server, 2);
    assert_eq!(
        status, 200,
        "under-cap activation must validate + pass: {body}"
    );
    assert_eq!(activated_job_count(&body), 2, "both jobs activate: {body}");

    // Then: those jobs are genuinely locked — an immediate second poll gets
    // nothing (they re-activate only after their 60 s timeout). This pins
    // that the fix did not weaken activation locking on the happy path.
    let (status, body) = activate_jobs(&server, 2);
    assert_eq!(status, 200, "second poll must succeed: {body}");
    assert_eq!(
        activated_job_count(&body),
        0,
        "locked jobs must not be re-activated before their timeout: {body}"
    );

    server.shutdown();
}
