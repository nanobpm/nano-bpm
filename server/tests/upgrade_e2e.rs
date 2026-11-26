//! Mixed-binary upgrade harness for capability latches (#2052).
//!
//! Runs a REAL 3-node, RF=3 Raft cluster that mixes the previous released
//! server binary (`NANO_UPGRADE_OLD_BIN`, built by
//! `scripts/upgrade-harness.sh` from the newest release tag that predates the
//! latch's capability) with the current build, and checks the latch contract
//! end to end — the property the capability unit tests can only simulate with
//! the current binary:
//!
//! 1. **before the latch** — while any old member is present the latch is never
//!    recorded and the replicas never diverge (the product's own replica-digest
//!    comparison, ADR 0066 #1664, runs between old and new members);
//! 2. **rolling upgrade** — members are replaced one by one; the latch is
//!    recorded only once every member runs the new binary;
//! 3. **recovery** — a member restarted from snapshot + journal on either side
//!    of the latch rejoins without divergence;
//! 4. **rollback refusal** — after the latch an old binary started on a
//!    member's data is refused by the cluster with a clear error instead of
//!    silently misapplying the post-latch log.
//!
//! Ignored by default: it needs the old binary. Run it through
//! `scripts/upgrade-harness.sh run <outdir>` (nightly workflow
//! `upgrade-harness-nightly.yml`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration-test target: clippy.toml allow-unwrap-in-tests does not cover non-#[test] \
helper fns in tests/*.rs"
)]

mod common;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const NODES: usize = 3;
const PARTITIONS: u64 = 3;

/// Log line a new member writes when it refuses an old peer because the
/// replicated state already depends on the latched capability
/// (`JoinRejection::MissingFeature`, `src/falcon.rs`).
const REFUSAL: &str = "incompatible peer binary: the cluster's replicated state requires feature";
/// Log line of a replica-digest mismatch (ADR 0066 #1664).
const DIVERGENCE: &str = "REPLICA DIVERGENCE";

fn feature() -> String {
    std::env::var("NANO_UPGRADE_FEATURE").unwrap_or_else(|_| "message-correlation-ack-v1".into())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Bin {
    Old,
    New,
}

impl Bin {
    fn path(self) -> PathBuf {
        match self {
            Bin::Old => PathBuf::from(
                std::env::var("NANO_UPGRADE_OLD_BIN")
                    .expect("NANO_UPGRADE_OLD_BIN must name the previous release's server binary"),
            ),
            Bin::New => PathBuf::from(common::SERVER_BIN),
        }
    }
}

fn reserve_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One cluster member's fixed identity (port, data dir, log file) across
/// binary swaps.
struct Member {
    id: usize,
    port: u16,
    data: PathBuf,
    child: Option<(Child, Bin)>,
    /// One log file per boot, so assertions can target a single boot.
    boots: Vec<(Bin, PathBuf)>,
}

struct Cluster {
    root: PathBuf,
    nodes: String,
    members: Vec<Member>,
}

impl Cluster {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::var("NANO_UPGRADE_SCRATCH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(format!(
                "nano-upgrade-{name}-{}-{nanos}",
                std::process::id()
            ));
        std::fs::create_dir_all(&root).unwrap();
        let ports: Vec<u16> = (0..NODES).map(|_| reserve_port()).collect();
        let nodes = ports
            .iter()
            .map(|p| format!("http://127.0.0.1:{p}"))
            .collect::<Vec<_>>()
            .join(",");
        let members = ports
            .iter()
            .enumerate()
            .map(|(id, &port)| Member {
                id,
                port,
                data: root.join(format!("node{id}")),
                child: None,
                boots: Vec::new(),
            })
            .collect();
        eprintln!("[upgrade] scratch {}", root.display());
        Self {
            root,
            nodes,
            members,
        }
    }

    fn start(&mut self, id: usize, bin: Bin) {
        let m = &mut self.members[id];
        assert!(m.child.is_none(), "node {id} already running");
        std::fs::create_dir_all(&m.data).unwrap();
        let log = self
            .root
            .join(format!("node{id}-boot{}-{bin:?}.log", m.boots.len()));
        let file = std::fs::File::create(&log).unwrap();
        // The new binary goes through the shared spawn defaults; the old one
        // mirrors them (stdio is redirected to the per-boot log below).
        let mut cmd = match bin {
            Bin::New => common::server_command(),
            Bin::Old => Command::new(bin.path()),
        };
        cmd.env("NANOBPMN_DATA_DIR", &m.data)
            .env("NANOBPMN_NODES", &self.nodes)
            .env("NANOBPMN_NODE_ID", id.to_string())
            .env("NANOBPMN_RF", NODES.to_string())
            .env("NANOBPMN_PARTITIONS", PARTITIONS.to_string())
            .env("NANOBPMN_RAFT", "1")
            .env("PORT", m.port.to_string())
            .stdin(Stdio::null())
            .stdout(file.try_clone().unwrap())
            .stderr(file);
        #[cfg(unix)]
        if bin == Bin::Old {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = cmd.spawn().expect("spawn server");
        eprintln!(
            "[upgrade] node {id} started as {bin:?} (log {})",
            log.display()
        );
        m.child = Some((child, bin));
        m.boots.push((bin, log));
    }

    /// SIGKILL (crash) — recovery must come from snapshot + journal.
    fn kill(&mut self, id: usize) {
        if let Some((mut child, _)) = self.members[id].child.take() {
            #[cfg(unix)]
            // SAFETY: children are process-group leaders (see `start`).
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            #[cfg(not(unix))]
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// The exit status once the member's process has exited on its own.
    fn exited(&mut self, id: usize) -> Option<ExitStatus> {
        let (child, _) = self.members[id].child.as_mut()?;
        let status = child.try_wait().unwrap()?;
        self.members[id].child = None;
        Some(status)
    }

    fn running(&mut self, id: usize) -> bool {
        match &mut self.members[id].child {
            Some((child, _)) => child.try_wait().unwrap().is_none(),
            None => false,
        }
    }

    fn port(&self, id: usize) -> u16 {
        self.members[id].port
    }

    fn log_of_boot(&self, id: usize, boot: usize) -> String {
        std::fs::read_to_string(&self.members[id].boots[boot].1).unwrap_or_default()
    }

    fn last_boot(&self, id: usize) -> usize {
        self.members[id].boots.len() - 1
    }

    /// Every log of every boot, concatenated.
    fn all_logs(&self) -> String {
        let mut out = String::new();
        for m in &self.members {
            for (i, (bin, path)) in m.boots.iter().enumerate() {
                out.push_str(&format!("===== node {} boot {i} ({bin:?}) =====\n", m.id));
                out.push_str(&std::fs::read_to_string(path).unwrap_or_default());
            }
        }
        out
    }

    fn assert_no_divergence(&self, phase: &str) {
        let logs = self.all_logs();
        assert!(
            !logs.contains(DIVERGENCE),
            "{phase}: a replica diverged\n{}",
            tail(&logs)
        );
    }

    /// Whether any CURRENT boot of a new member has refused a peer for lacking
    /// the latched capability — i.e. the latch is recorded on that member.
    fn latch_refusals(&self) -> usize {
        let needle = format!("{REFUSAL} \"{}\"", feature());
        self.members
            .iter()
            .flat_map(|m| m.boots.iter())
            .filter(|(bin, _)| *bin == Bin::New)
            .map(|(_, p)| {
                std::fs::read_to_string(p)
                    .unwrap_or_default()
                    .matches(&needle)
                    .count()
            })
            .sum()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for id in 0..NODES {
            self.kill(id);
        }
        if !std::thread::panicking() && std::env::var_os("NANO_UPGRADE_KEEP").is_none() {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

fn tail(s: &str) -> &str {
    let start = s.len().saturating_sub(12_000);
    let mut i = start;
    while !s.is_char_boundary(i) {
        i += 1;
    }
    &s[i..]
}

fn request(port: u16, method: &str, path: &str, body: Option<&str>) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(15))).ok()?;
    let body = body.unwrap_or("");
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).ok()?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).ok()?;
    let raw = String::from_utf8_lossy(&raw);
    let status = raw.split_whitespace().nth(1)?.parse().ok()?;
    let body = raw.split_once("\r\n\r\n").map(|(_, b)| b.to_string())?;
    Some((status, body))
}

fn wait_for(what: &str, limit: Duration, mut f: impl FnMut() -> bool, c: &Cluster) {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("timed out waiting for {what}\n{}", tail(&c.all_logs()));
}

/// Creates `n` `demo` instances through `via`, retrying transient errors
/// (leader election after a member swap). Returns the instance keys.
fn create_instances(c: &Cluster, via: usize, n: usize) -> Vec<String> {
    let mut keys = Vec::new();
    for _ in 0..n {
        let mut key = None;
        wait_for(
            "a committed create",
            Duration::from_secs(60),
            || match request(
                c.port(via),
                "POST",
                "/v2/process-instances",
                Some(r#"{"processDefinitionId":"demo"}"#),
            ) {
                Some((200, body)) => {
                    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                    key = v["processInstanceKey"].as_str().map(str::to_string);
                    key.is_some()
                }
                _ => false,
            },
            c,
        );
        keys.extend(key);
    }
    keys
}

/// Every member answers every instance as ACTIVE. All callers expect every
/// member to be running, so an exited member fails the phase instead of
/// silently shrinking it to two replicas. (A local miss may be forwarded to a
/// peer, so per-member replication is proven separately on the raw Raft logs
/// by `assert_latch_ordering`.)
fn assert_visible_everywhere(c: &mut Cluster, keys: &[String]) {
    for id in 0..NODES {
        assert!(
            c.running(id),
            "node {id} exited unexpectedly\n{}",
            tail(&c.all_logs())
        );
        for key in keys {
            let port = c.port(id);
            let path = format!("/v2/process-instances/{key}");
            let mut last = None;
            wait_for(
                &format!("instance {key} on node {id}"),
                Duration::from_secs(60),
                || {
                    last = request(port, "GET", &path, None);
                    match &last {
                        Some((200, body)) => serde_json::from_str::<serde_json::Value>(body)
                            .is_ok_and(|v| v["state"] == "ACTIVE"),
                        _ => false,
                    }
                },
                c,
            );
        }
    }
}

fn wait_ready(c: &Cluster, id: usize) {
    let port = c.port(id);
    wait_for(
        &format!("node {id} ready"),
        Duration::from_secs(90),
        || matches!(request(port, "GET", "/v2/topology", None), Some((200, _))),
        c,
    );
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// One replicated command as recorded in a member's on-disk Raft log.
#[derive(Debug, PartialEq, Eq)]
struct LoggedItem {
    index: u64,
    now: u64,
    /// The latch's new-format envelope output (`remote_buffered_delivery`,
    /// #1949): what an old member would apply differently.
    stamped: bool,
    /// The whole replicated item, so entries with different commands never
    /// compare equal.
    payload: String,
}

/// Reads `<data>/raft/p<partition>/seg-*.ndjson` (one JSON entry per line;
/// later lines for the same index supersede earlier ones after a truncation).
fn raft_items(data: &Path, partition: u64) -> Vec<LoggedItem> {
    let dir = data.join("raft").join(format!("p{partition}"));
    let mut segs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("seg-") && n.ends_with(".ndjson"))
        })
        .collect();
    segs.sort();
    let mut by_index = std::collections::BTreeMap::new();
    for seg in segs {
        for line in std::fs::read_to_string(&seg).unwrap_or_default().lines() {
            let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) else {
                continue; // a torn tail line from a SIGKILL
            };
            let Some(index) = entry["log_id"]["index"].as_u64() else {
                continue;
            };
            let items: Vec<LoggedItem> = entry["payload"]["Normal"]["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|item| LoggedItem {
                    index,
                    now: item["now"].as_u64().unwrap_or_default(),
                    stamped: item["remote_buffered_delivery"].as_bool() == Some(true),
                    payload: item.to_string(),
                })
                .collect();
            by_index.insert(index, items);
        }
    }
    by_index.into_values().flatten().collect()
}

/// The latch's ordering contract over every member's Raft log, per partition:
/// no new-format item before `all_new_at` (while an old member was present),
/// replicas agree entry for entry, and
/// both sides of the latch actually carried traffic (non-vacuous).
fn assert_latch_ordering(c: &Cluster, all_new_at: u64) {
    for p in 0..PARTITIONS {
        let logs: Vec<Vec<LoggedItem>> = c.members.iter().map(|m| raft_items(&m.data, p)).collect();
        let longest = logs.iter().max_by_key(|l| l.len()).unwrap();
        for (id, log) in logs.iter().enumerate() {
            // Every member must itself hold both sides of the latch (an empty
            // or pre-latch-only follower fails), agree with its peers item for
            // item (payload included) over its whole log, and obey the
            // ordering contract on its own log.
            assert_eq!(
                log[..],
                longest[..log.len()],
                "partition {p}: node {id}'s raft log disagrees with its peers"
            );
            let early = log.iter().filter(|i| i.now < all_new_at).count();
            let late_stamped = log.iter().filter(|i| i.stamped).count();
            assert!(
                early > 0,
                "partition {p}: node {id} has no pre-latch traffic (vacuous)"
            );
            assert!(
                late_stamped > 0,
                "partition {p}: node {id} never saw the latch open (vacuous)"
            );
            if let Some(bad) = log.iter().find(|i| i.stamped && i.now < all_new_at) {
                panic!(
                    "partition {p}: node {id}: new-format output while an old member was \
                     present: {bad:?}"
                );
            }
        }
        let early = longest.iter().filter(|i| i.now < all_new_at).count();
        let late_stamped = longest.iter().filter(|i| i.stamped).count();
        // The envelope is NOT one-way: a leader re-decides it per batch and
        // drops it while a peer is unreachable (e.g. the recovery restart).
        // The latch itself is one-way in engine state — later disabled
        // envelopes are ignored (`Engine::set_remote_buffered_delivery`) — and
        // its one-way-ness is what phase 4 (rollback refusal) observes.
        let first = longest.iter().position(|i| i.stamped).unwrap();
        eprintln!(
            "[upgrade] partition {p}: {early} pre-latch item(s), latch at index {}, \
             {late_stamped} post-latch item(s)",
            longest[first].index
        );
    }
}

/// The whole latch contract in one rolling-upgrade lifecycle (bounded: three
/// small batches of instances, one member swap at a time).
#[test]
#[ignore = "needs NANO_UPGRADE_OLD_BIN; run via scripts/upgrade-harness.sh"]
fn capability_latch_survives_a_real_mixed_binary_rolling_upgrade() {
    let mut c = Cluster::new("latch");

    // ---- Phase 0: all-old cluster.
    for id in 0..NODES {
        c.start(id, Bin::Old);
    }
    for id in 0..NODES {
        wait_ready(&c, id);
    }
    let mut keys = create_instances(&c, 0, 6);
    assert_visible_everywhere(&mut c, &keys);

    // ---- Phase 1: rolling upgrade, one member at a time. While any old
    // member remains, no new member may record the latch (it would refuse
    // the old peer) and nothing may diverge.
    // When the last old member stopped: every entry proposed before it was
    // proposed while an old member was present.
    let mut all_new_at = 0;
    for id in 0..NODES {
        c.kill(id);
        all_new_at = now_ms();
        c.start(id, Bin::New);
        wait_ready(&c, id);
        keys.extend(create_instances(&c, id, 4));
        assert_visible_everywhere(&mut c, &keys);
        let old_left =
            (0..NODES).any(|j| c.members[j].child.as_ref().map(|x| x.1) == Some(Bin::Old));
        if old_left {
            assert_eq!(
                c.latch_refusals(),
                0,
                "latch recorded while an old member is present\n{}",
                tail(&c.all_logs())
            );
            // Recovery BEFORE the latch: crash + restart the upgraded member.
            c.kill(id);
            c.start(id, Bin::New);
            wait_ready(&c, id);
            keys.extend(create_instances(&c, (id + 1) % NODES, 2));
            assert_visible_everywhere(&mut c, &keys);
        }
        c.assert_no_divergence(&format!("after upgrading node {id}"));
    }

    // ---- Phase 2: fully upgraded. New traffic applies under the enabling
    // envelope, which records the latch.
    keys.extend(create_instances(&c, 0, 6));
    assert_visible_everywhere(&mut c, &keys);
    c.assert_no_divergence("after the latch");

    // ---- Phase 3: recovery AFTER the latch.
    c.kill(1);
    c.start(1, Bin::New);
    wait_ready(&c, 1);
    keys.extend(create_instances(&c, 1, 3));
    assert_visible_everywhere(&mut c, &keys);
    c.assert_no_divergence("after post-latch recovery");

    // ---- Phase 4: rollback refusal. Latches are one-way: the OLD binary
    // started on node 2's post-latch data must refuse it (fail closed on the
    // journal record it cannot replay) instead of silently applying the
    // post-latch log with legacy semantics.
    c.kill(2);
    c.start(2, Bin::Old);
    let deadline = Instant::now() + Duration::from_secs(90);
    let status = loop {
        if let Some(status) = c.exited(2) {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the old binary kept running on post-latch data\n{}",
            tail(&c.log_of_boot(2, c.last_boot(2)))
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    let old_log = c.log_of_boot(2, c.last_boot(2));
    assert!(
        !status.success(),
        "the old binary exited successfully on post-latch data\n{}",
        tail(&old_log)
    );
    assert!(
        old_log.contains("cannot replay") && old_log.contains("fail-closed"),
        "the old binary did not refuse post-latch data with a clear error\n{}",
        tail(&old_log)
    );

    assert_latch_ordering(&c, all_new_at);

    // An old binary REPLACING a member (fresh disk, so it has no post-latch
    // record of its own) is refused at the join handshake by the latched
    // members, naming the capability.
    std::fs::remove_dir_all(&c.members[2].data).unwrap();
    let before = c.latch_refusals();
    c.start(2, Bin::Old);
    wait_for(
        "the latched members to refuse an old replacement member",
        Duration::from_secs(60),
        || c.latch_refusals() > before,
        &c,
    );
    // The two latched members keep quorum and keep serving.
    keys.extend(create_instances(&c, 0, 2));
    c.kill(2);
    c.assert_no_divergence("after the rollback attempts");
}
