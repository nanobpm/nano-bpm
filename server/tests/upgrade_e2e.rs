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
//!    of the latch rejoins without divergence; after the latch EVERY member
//!    (the last-upgraded one included) is restarted in turn and must rejoin
//!    ACTIVE with its log matching the leader prefix and fresh digest claims
//!    still verifying (#2058). Before each of those restarts the harness forces
//!    a journal snapshot on every partition the member journals, grows a journal
//!    tail past it, and asserts the restart restored exactly that snapshot and
//!    replayed the tail;
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

/// The replicated-item envelope field the selected capability's latch stamps
/// (`nano-server-raft` `raft.rs`). An unsupported capability name fails the
/// test instead of silently checking another latch's bit.
fn envelope_field() -> &'static str {
    match feature().as_str() {
        "message-correlation-ack-v1" => "remote_buffered_delivery",
        "message-correlation-ack-v2" => "message_correlation_ack_v2",
        other => panic!("unsupported NANO_UPGRADE_FEATURE {other:?}"),
    }
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

/// Reserve `n` distinct free ports. Every listener stays bound until all `n`
/// are allocated, so the OS cannot hand the same port out twice.
fn reserve_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
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
        let ports = reserve_ports(NODES);
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
            // Journal snapshots only when the harness forces one through
            // `POST /debug/journal-snapshot` (#2058), so the snapshot a restart
            // recovers from is a recorded, deterministic checkpoint.
            .env("NANOBPMN_SNAPSHOT_INTERVAL_MS", "3600000")
            // The forced-snapshot endpoint is opt-in (off by default).
            .env("NANOBPMN_DEBUG_JOURNAL_SNAPSHOT", "1")
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

    /// How many times a new member has refused a peer for lacking a latched
    /// capability — i.e. a latch is recorded on that member. Any named
    /// capability counts: `check_join` reports only the FIRST missing one in
    /// `required_cluster_features` order, so an old binary lacking both ack-v1
    /// and ack-v2 is refused naming ack-v1 even when ack-v2 is the selected
    /// latch. The selected latch itself is verified independently by its
    /// envelope bit in [`assert_latch_ordering`].
    fn latch_refusals(&self) -> usize {
        let needle = format!("{REFUSAL} \"");
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

/// One replicated command as recorded in a member's on-disk Raft log.
#[derive(Debug, PartialEq, Eq)]
struct LoggedItem {
    index: u64,
    /// The selected latch's new-format envelope bit ([`envelope_field`]):
    /// what an old member would apply differently.
    stamped: bool,
    /// The whole replicated item, so entries with different commands never
    /// compare equal.
    payload: String,
    /// The batch-level replicated fields next to `items` in
    /// `payload.Normal` (the leader's `digest` claim and `digest_cutover`):
    /// byte-identical on every replica, so the prefix comparison covers them.
    /// The node-local `raft_state` digest is deliberately NOT read here.
    batch: String,
}

/// Reads `<data>/raft/p<partition>/seg-*.ndjson` (one JSON entry per line;
/// later lines for the same index supersede earlier ones after a truncation).
/// Returns one `payload.Normal` batch per index, the whole replicated batch
/// included.
fn raft_batches(data: &Path, partition: u64) -> Vec<(u64, serde_json::Value)> {
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
            by_index.insert(index, entry["payload"]["Normal"].clone());
        }
    }
    by_index.into_iter().collect()
}

/// Every replicated command in a member's Raft log, each carrying its batch's
/// replicated fields (see [`LoggedItem::batch`]).
fn raft_items(data: &Path, partition: u64) -> Vec<LoggedItem> {
    let mut out = Vec::new();
    for (index, normal) in raft_batches(data, partition) {
        let mut batch = normal.clone();
        if let Some(obj) = batch.as_object_mut() {
            obj.remove("items");
        }
        let batch = batch.to_string();
        for item in normal["items"].as_array().into_iter().flatten() {
            out.push(LoggedItem {
                index,
                stamped: item[envelope_field()].as_bool() == Some(true),
                payload: item.to_string(),
                batch: batch.clone(),
            });
        }
    }
    out
}

/// Once no old member remains, every member's log must carry the SAME
/// `digest_cutover` batch at the SAME index per partition: that is what makes
/// the replicas' digest chains comparable, so `assert_no_divergence` is
/// proven active rather than passing vacuously. A cutover can already land in
/// the all-old phase (v0.13.0 supports it), so additionally every member must
/// hold a post-`barrier[p]` (post-upgrade) batch carrying a `digest` claim, and
/// every member's claim at each such index must agree — proving the ledger
/// actually checked post-upgrade traffic on every partition and member.
fn assert_digest_cutover_agreed(c: &Cluster, barrier: &[u64]) {
    for p in 0..PARTITIONS {
        let after = barrier[p as usize];
        let claims: Vec<std::collections::BTreeMap<u64, serde_json::Value>> = c
            .members
            .iter()
            .map(|m| {
                raft_batches(&m.data, p)
                    .into_iter()
                    .filter(|(i, b)| *i > after && !b["digest"].is_null())
                    .map(|(i, b)| (i, b["digest"].clone()))
                    .collect()
            })
            .collect();
        for (id, mine) in claims.iter().enumerate() {
            assert!(
                !mine.is_empty(),
                "partition {p}: node {id} logged no post-upgrade digest claim (divergence check vacuous)"
            );
            for (other, theirs) in claims.iter().enumerate() {
                for (i, d) in mine {
                    if let Some(t) = theirs.get(i) {
                        assert_eq!(
                            d, t,
                            "partition {p}: digest claims of nodes {id} and {other} differ at index {i}"
                        );
                    }
                }
            }
        }
        let cutovers: Vec<Option<(u64, serde_json::Value)>> = c
            .members
            .iter()
            .map(|m| {
                raft_batches(&m.data, p)
                    .into_iter()
                    .find(|(_, b)| b["digest_cutover"].as_bool() == Some(true))
            })
            .collect();
        let first = cutovers[0].clone().unwrap_or_else(|| {
            panic!("partition {p}: node 0 never logged a digest_cutover (divergence check vacuous)")
        });
        for (id, cut) in cutovers.iter().enumerate() {
            assert_eq!(
                cut.as_ref(),
                Some(&first),
                "partition {p}: node {id}'s digest_cutover disagrees with node 0's"
            );
        }
        eprintln!(
            "[upgrade] partition {p}: digest cutover at index {}",
            first.0
        );
    }
}

/// Per member, per partition: the highest Raft log index on disk.
fn log_heads(c: &Cluster) -> Vec<Vec<u64>> {
    c.members
        .iter()
        .map(|m| {
            (0..PARTITIONS)
                .map(|p| {
                    raft_batches(&m.data, p)
                        .last()
                        .map(|(i, _)| *i)
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect()
}

/// One member's `/debug/raft` view: per partition, `(leader, applied)` —
/// the member's current leader (-1 if unknown) and its locally applied index
/// (-1 if none). `None` while the member does not answer.
fn raft_status(c: &Cluster, id: usize) -> Option<Vec<(i128, i128)>> {
    let (code, body) = request(c.port(id), "GET", "/debug/raft", None)?;
    if code != 200 {
        return None;
    }
    let mut out = vec![(-1i128, -1i128); PARTITIONS as usize];
    for line in body.lines() {
        let field = |k: &str| {
            line.split_whitespace()
                .find_map(|kv| kv.strip_prefix(k)?.strip_prefix('='))
                .and_then(|v| v.parse::<i128>().ok())
        };
        let (Some(p), Some(leader), Some(applied)) =
            (field("partition"), field("leader"), field("applied"))
        else {
            continue;
        };
        if let Some(slot) = usize::try_from(p).ok().and_then(|p| out.get_mut(p)) {
            *slot = (leader, applied);
        }
    }
    Some(out)
}

/// Per partition, the lowest applied index over every member: every entry at
/// or below it is committed (applied implies committed) and held, applied, by
/// ALL members. `None` while any member does not answer.
fn committed_floor(c: &Cluster) -> Option<Vec<i128>> {
    let all: Option<Vec<Vec<(i128, i128)>>> = (0..NODES).map(|id| raft_status(c, id)).collect();
    let all = all?;
    Some(
        (0..PARTITIONS as usize)
            .map(|p| all.iter().map(|s| s[p].1).min().unwrap_or(-1))
            .collect(),
    )
}

/// Log message of a recovered journal partition (`seglog::RECOVERED_PARTITION`).
const RECOVERED: &str = "journal partition recovered";

/// Drops ANSI SGR escapes (`ESC [ ... m`) the tracing formatter may wrap field
/// names in, so `key=value` pairs parse.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// The `u64` value of `key=` in a whitespace-separated `key=value` line.
fn kv(line: &str, key: &str) -> Option<u64> {
    line.split_whitespace()
        .find_map(|t| t.strip_prefix(key)?.strip_prefix('='))
        .and_then(|v| v.parse().ok())
}

/// Forces a journal snapshot on EVERY partition whose engine journal member
/// `id` holds (its owned partitions) and returns each `(partition, boundary)`
/// the durable snapshot now covers — non-empty, every boundary > 0.
fn force_journal_snapshot(c: &Cluster, id: usize) -> Vec<(usize, u64)> {
    let (code, body) = request(c.port(id), "POST", "/debug/journal-snapshot", None)
        .expect("node answers /debug/journal-snapshot");
    assert_eq!(
        code, 200,
        "forced journal snapshot on node {id} failed: {body}"
    );
    let out: Vec<(usize, u64)> = body
        .lines()
        .filter_map(|line| {
            let p = usize::try_from(kv(line, "partition")?).ok()?;
            Some((p, kv(line, "snapshot_index")?))
        })
        .collect();
    assert!(
        !out.is_empty() && out.iter().all(|(p, i)| *p < PARTITIONS as usize && *i > 0),
        "node {id}: the forced snapshot must cover a non-empty prefix of every owned partition, got {out:?}\n{body}"
    );
    out
}

/// Grows a journal tail after member `id`'s snapshot checkpoint: creates
/// instances (bounded) until the member has locally applied a new `Normal`
/// (command) entry on every checkpointed partition past
/// `applied_at_checkpoint`, so each one's journal holds events the snapshot does not cover.
fn grow_tail_past_checkpoint(
    c: &Cluster,
    id: usize,
    checkpoint: &[(usize, u64)],
    applied_at_checkpoint: &[(i128, i128)],
    keys: &mut Vec<String>,
) {
    for _ in 0..30 {
        let advanced = raft_status(c, id).is_some_and(|now| {
            checkpoint.iter().all(|(p, _)| {
                normal_applied_past(
                    &c.members[id].data,
                    *p,
                    applied_at_checkpoint[*p].1,
                    now[*p].1,
                )
            })
        });
        if advanced {
            return;
        }
        keys.extend(create_instances(c, id, 1));
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "node {id}: no journal tail on every partition after the snapshot checkpoint ({:?} -> {:?})\n{}",
        applied_at_checkpoint,
        raft_status(c, id),
        tail(&c.all_logs())
    );
}

/// Whether member data `data` holds, on partition `p`, a locally applied
/// `Normal` Raft entry (an engine command batch, which journals events) in
/// `(from, to]`. A bare applied-index increase is not enough: a `Blank`
/// (election) entry advances `applied` without journaling anything.
fn normal_applied_past(data: &Path, p: usize, from: i128, to: i128) -> bool {
    raft_batches(data, p as u64).iter().any(|(i, b)| {
        let i = i128::from(*i);
        i > from && i <= to && !b.is_null()
    })
}

/// The restart of member `id` went snapshot + journal: on every checkpointed
/// partition the boot log reports recovery FROM exactly the recorded
/// boundary and a non-empty replayed journal tail after it.
fn assert_recovered_from_checkpoint(c: &Cluster, id: usize, checkpoint: &[(usize, u64)]) {
    let log = strip_ansi(&c.log_of_boot(id, c.last_boot(id)));
    let mut seen = vec![None; PARTITIONS as usize];
    for line in log.lines().filter(|l| l.contains(RECOVERED)) {
        if let (Some(p), Some(snap), Some(replayed)) = (
            kv(line, "partition"),
            kv(line, "snapshot_index"),
            kv(line, "replayed_entries"),
        ) && let Some(slot) = usize::try_from(p).ok().and_then(|p| seen.get_mut(p))
        {
            *slot = Some((snap, replayed));
        }
    }
    for &(p, want) in checkpoint {
        match seen[p] {
            Some((snap, replayed)) => {
                assert_eq!(
                    snap,
                    want,
                    "node {id} partition {p}: recovery did not load the recorded snapshot checkpoint\n{}",
                    tail(&log)
                );
                assert!(
                    replayed > 0,
                    "node {id} partition {p}: recovery replayed no journal tail after the snapshot\n{}",
                    tail(&log)
                );
            }
            None => panic!(
                "node {id} partition {p}: no `{RECOVERED}` line in the restart log\n{}",
                tail(&log)
            ),
        }
    }
}

/// After a post-latch restart from snapshot + journal, member `id` must have
/// caught up and agree with EVERY peer over the committed prefix. Per
/// partition: the actual leader is resolved through `/debug/raft` (agreed by
/// the restarted member and the leader itself) and its applied index captured
/// as the target; the restarted member must locally APPLY up to that target,
/// and its on-disk log must equal every peer's — whole `payload.Normal` per
/// index (items plus `digest` / `digest_cutover`), compared BY LOG INDEX so a
/// purged log prefix still aligns — at every index up to the committed floor
/// (lowest applied index across members). Comparing against all peers (never
/// a "longest log" pick, which could be the restarted member itself) and only
/// over committed entries (an ex-leader's uncommitted tail is excluded by
/// construction) makes the check neither vacuous nor timing-dependent; it is
/// polled only to let replication and apply catch up.
fn assert_restarted_matches_prefix(c: &Cluster, id: usize) {
    use std::collections::BTreeMap;
    for p in 0..PARTITIONS as usize {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut target: Option<i128> = None;
        let mut last = String::from("no /debug/raft answer yet");
        loop {
            let done = (|| {
                let mine = raft_status(c, id)?[p];
                if target.is_none() {
                    let leader = usize::try_from(mine.0).ok().filter(|l| *l < NODES)?;
                    let theirs = raft_status(c, leader)?[p];
                    if theirs.0 != mine.0 {
                        last = format!("leader disagreement: {} vs {}", mine.0, theirs.0);
                        return None;
                    }
                    target = Some(theirs.1);
                }
                let want = target?;
                if mine.1 < want {
                    last = format!("applied {} < leader's applied {want}", mine.1);
                    return None;
                }
                let floor = committed_floor(c)?[p];
                if floor < want {
                    last = format!("committed floor {floor} < target {want}");
                    return None;
                }
                let floor = u64::try_from(floor).ok()?;
                let logs: Vec<BTreeMap<u64, serde_json::Value>> = c
                    .members
                    .iter()
                    .map(|m| {
                        raft_batches(&m.data, p as u64)
                            .into_iter()
                            .filter(|(i, _)| *i <= floor)
                            .collect()
                    })
                    .collect();
                for (other, theirs) in logs.iter().enumerate() {
                    if other == id {
                        continue;
                    }
                    // Require a NON-EMPTY shared committed interval: from the
                    // later of the two retained-log starts (purge boundary) up
                    // to the floor, both logs must cover the SAME indices with
                    // equal payloads. Disjoint or empty logs keep polling.
                    let (Some(a), Some(b)) = (logs[id].keys().next(), theirs.keys().next()) else {
                        last = format!("node {id} or node {other} holds no committed batch");
                        return None;
                    };
                    let lo = *a.max(b);
                    let mine_iv: Vec<_> = logs[id].range(lo..).collect();
                    let their_iv: Vec<_> = theirs.range(lo..).collect();
                    if mine_iv.is_empty() {
                        last = format!("no shared committed interval with node {other}");
                        return None;
                    }
                    if mine_iv != their_iv {
                        panic!(
                            "partition {p}: restarted node {id} disagrees with node {other} over \
                             committed interval [{lo}, {floor}]: {mine_iv:?} vs {their_iv:?}\n{}",
                            tail(&c.all_logs())
                        );
                    }
                }
                Some(())
            })();
            if done.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                panic!(
                    "partition {p}: restarted node {id} never caught up with the leader: {last}\n{}",
                    tail(&c.all_logs())
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

/// Post-restart digest claims keep verifying, on every partition and member:
/// each member holds a NEW `digest` claim beyond its pre-restart head
/// `before` that it has locally APPLIED (index at or below its own
/// `/debug/raft` applied index — a stored-but-unapplied claim proves
/// nothing), and claims agree across members at every committed index.
/// Creates may land on fewer than all partitions while leadership recovers,
/// so coverage is polled, driving bounded extra traffic, before it fails; a
/// disagreement fails at once.
fn assert_fresh_digest_claims(
    c: &Cluster,
    before: &[Vec<u64>],
    restarted: usize,
    keys: &mut Vec<String>,
) {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut round = 0usize;
    loop {
        match fresh_digest_gap(c, before, restarted) {
            None => return,
            Some(gap) if Instant::now() >= deadline => panic!(
                "after node {restarted}'s post-latch restart: {gap} (divergence check vacuous)\n{}",
                tail(&c.all_logs())
            ),
            Some(_) => {
                round += 1;
                keys.extend(create_instances(
                    c,
                    (restarted + round) % NODES,
                    PARTITIONS as usize,
                ));
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

/// The first missing fresh, locally applied claim (`None` = full coverage).
/// Panics on a claim disagreement at a committed index.
fn fresh_digest_gap(c: &Cluster, before: &[Vec<u64>], restarted: usize) -> Option<String> {
    let status: Vec<Vec<(i128, i128)>> = match (0..NODES).map(|id| raft_status(c, id)).collect() {
        Some(s) => s,
        None => return Some("a member did not answer /debug/raft".into()),
    };
    for p in 0..PARTITIONS as usize {
        let applied: Vec<u64> = status
            .iter()
            .map(|s| u64::try_from(s[p].1).unwrap_or(0))
            .collect();
        let floor = applied.iter().copied().min().unwrap_or(0);
        let claims: Vec<std::collections::BTreeMap<u64, serde_json::Value>> = c
            .members
            .iter()
            .enumerate()
            .map(|(id, m)| {
                let head = before[id][p];
                raft_batches(&m.data, p as u64)
                    .into_iter()
                    .filter(|(i, b)| *i > head && *i <= applied[id] && !b["digest"].is_null())
                    .map(|(i, b)| (i, b["digest"].clone()))
                    .collect()
            })
            .collect();
        for (id, mine) in claims.iter().enumerate() {
            for (other, theirs) in claims.iter().enumerate() {
                for (i, d) in mine.range(..=floor) {
                    if let Some(t) = theirs.get(i) {
                        assert_eq!(
                            d, t,
                            "partition {p}: after node {restarted}'s restart, digest claims of \
                             nodes {id} and {other} differ at index {i}"
                        );
                    }
                }
            }
        }
        if let Some(id) = claims.iter().position(|m| m.is_empty()) {
            return Some(format!(
                "partition {p}: node {id} has no locally applied digest claim past its \
                 pre-restart head"
            ));
        }
    }
    None
}

/// The latch's ordering contract over every member's Raft log, per partition:
/// no new-format item at or below `barrier[p]` — the partition's last Raft
/// log index when the last old member stopped (all prior submissions had been
/// awaited, so nothing was pending) — replicas agree entry for entry, and
/// both sides of the latch actually carried traffic (non-vacuous).
fn assert_latch_ordering(c: &Cluster, barrier: &[u64]) {
    for p in 0..PARTITIONS {
        let all_new_at = barrier[p as usize];
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
            let early = log.iter().filter(|i| i.index <= all_new_at).count();
            let late_stamped = log.iter().filter(|i| i.stamped).count();
            assert!(
                early > 0,
                "partition {p}: node {id} has no pre-latch traffic (vacuous)"
            );
            assert!(
                late_stamped > 0,
                "partition {p}: node {id} never saw the latch open (vacuous)"
            );
            if let Some(bad) = log.iter().find(|i| i.stamped && i.index <= all_new_at) {
                panic!(
                    "partition {p}: node {id}: new-format output while an old member was \
                     present: {bad:?}"
                );
            }
        }
        let early = longest.iter().filter(|i| i.index <= all_new_at).count();
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
    envelope_field(); // fail fast on an unsupported NANO_UPGRADE_FEATURE
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
    // Per-partition log barrier at the moment the last old member stopped:
    // every earlier submission was awaited (`assert_visible_everywhere`), so
    // nothing is pending and every entry at or below it was proposed while an
    // old member was present. Log indices, not clock stamps, order this.
    let mut barrier = vec![0u64; PARTITIONS as usize];
    for id in 0..NODES {
        c.kill(id);
        if id + 1 == NODES {
            for (p, b) in barrier.iter_mut().enumerate() {
                *b = c
                    .members
                    .iter()
                    .flat_map(|m| raft_items(&m.data, p as u64))
                    .map(|i| i.index)
                    .max()
                    .unwrap_or_default();
            }
        }
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
    assert_digest_cutover_agreed(&c, &barrier);

    // ---- Phase 3: recovery AFTER the latch, for EVERY member (#2058) —
    // including the last-upgraded one, whose log history differs (it ran the
    // old binary longest). Sequential, one member down at a time, so quorum
    // holds and the runtime stays within the nightly budget.
    for id in 0..NODES {
        // Deterministic snapshot checkpoint on every partition of this
        // member, then a journal tail past it, so the restart must go
        // snapshot + journal (not journal-only).
        let checkpoint = force_journal_snapshot(&c, id);
        // Read AFTER the snapshot: an entry applied past this is never covered.
        let applied_at_checkpoint = raft_status(&c, id).expect("node answers /debug/raft");
        grow_tail_past_checkpoint(&c, id, &checkpoint, &applied_at_checkpoint, &mut keys);
        let before = log_heads(&c);
        c.kill(id);
        c.start(id, Bin::New);
        wait_ready(&c, id);
        assert_recovered_from_checkpoint(&c, id, &checkpoint);
        // Drive traffic; `assert_fresh_digest_claims` adds bounded extra
        // creates until every partition and member holds a fresh claim.
        keys.extend(create_instances(&c, (id + 1) % NODES, PARTITIONS as usize));
        assert_restarted_matches_prefix(&c, id);
        assert_fresh_digest_claims(&c, &before, id, &mut keys);
        assert_visible_everywhere(&mut c, &keys);
        c.assert_no_divergence(&format!("after post-latch recovery of node {id}"));
    }

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

    assert_latch_ordering(&c, &barrier);

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
