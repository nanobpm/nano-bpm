//! Divergence-digest cost probe for ADR 0066 option C (#1655): what does it
//! cost each replica to compute a state digest so the leader can replicate
//! its own and followers can fail-stop on a mismatch?
//!
//! Two digest shapes are measured against the cost of applying the commands
//! themselves:
//!
//! * **incremental** — a rolling hash chained over the canonical
//!   (`serde_json::Value`, sorted-key) bytes of every event a command produced. Because engine
//!   state is the fold of its events (migration-by-replay, #1071), equal event
//!   chains imply equal state under a deterministic fold.
//! * **full** — a periodic hash of the whole `EngineSnapshot`, canonicalised
//!   through `serde_json::Value` (sorted object keys) so `HashMap` iteration
//!   order cannot leak into the digest.
//!
//! `DefaultHasher` stands in for the production hash (a stable, versioned one
//! such as xxh3/BLAKE3 would be pinned in the server); only the relative cost
//! is of interest here. Run with:
//!   cargo run --release --features serde --example divergence_digest

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};
use std::time::{Duration, Instant};

use nanobpmn_engine_core::{Command, Engine, ProcessBuilder, Value};

fn vars(n: usize) -> HashMap<String, Value> {
    (0..n)
        .map(|i| (format!("v{i}"), Value::Str(format!("value-{i:08}"))))
        .collect()
}

fn engine() -> Engine {
    let def = ProcessBuilder::new("p")
        .start_event("start")
        .service_task("t0", "job")
        .service_task("t1", "job")
        .end_event("end")
        .connect("start", "t0")
        .connect("t0", "t1")
        .connect("t1", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine
        .apply_command_at(Command::DeployProcess(def), 1_000)
        .unwrap();
    engine
}

struct Probe {
    apply: Duration,
    digest: Duration,
    hash_only: Duration,
    commands: usize,
}

/// Drives `n` instances (create + complete the first task, leaving each
/// instance live at `t1`) and times apply against each digest variant.
fn drive(engine: &mut Engine, n: usize) -> Probe {
    let (mut apply, mut digest, mut hash_only) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let mut chain = DefaultHasher::new();
    let mut raw = DefaultHasher::new();
    let mut commands = 0;
    for _ in 0..n {
        let create = Command::create_instance_with("p", vars(4));
        let t = Instant::now();
        let ev = engine.apply_command_at(create, 1_000).unwrap();
        apply += t.elapsed();
        let t = Instant::now();
        chain.write(&canonical(&ev));
        digest += t.elapsed();
        // The journal already encodes these events; with deterministic map
        // order those bytes are canonical and only the hash itself is extra.
        let encoded = serde_json::to_vec(&ev).unwrap();
        let t = Instant::now();
        raw.write(&encoded);
        hash_only += t.elapsed();
        let job = engine
            .activate_jobs("job", "w", 1, 60_000, 1_000)
            .into_iter()
            .next()
            .unwrap();
        let mut complete = Command::complete_job_with(job.key, vars(1));
        engine.stamp_terminal_attribution(&mut complete);
        let t = Instant::now();
        let ev = engine.apply_command_at(complete, 1_000).unwrap();
        apply += t.elapsed();
        let t = Instant::now();
        chain.write(&canonical(&ev));
        digest += t.elapsed();
        // The journal already encodes these events; with deterministic map
        // order those bytes are canonical and only the hash itself is extra.
        let encoded = serde_json::to_vec(&ev).unwrap();
        let t = Instant::now();
        raw.write(&encoded);
        hash_only += t.elapsed();
        commands += 2;
    }
    std::hint::black_box((chain.finish(), raw.finish()));
    Probe {
        apply,
        digest,
        hash_only,
        commands,
    }
}

/// Canonical bytes: through `serde_json::Value`, whose object keys are
/// sorted, so a `HashMap`'s per-process `RandomState` order cannot reach the
/// digest (plain `to_vec` of a `HashMap` would differ between replicas).
fn canonical<T: serde::Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(&serde_json::to_value(v).unwrap()).unwrap()
}

fn full_digest(engine: &Engine) -> (u64, usize) {
    let bytes = canonical(&engine.snapshot());
    let mut h = DefaultHasher::new();
    h.write(&bytes);
    (h.finish(), bytes.len())
}

fn main() {
    println!(
        "| live instances | apply / cmd | canonical re-encode + hash / cmd | overhead | hash of journal bytes / cmd | full digest | snapshot bytes |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    for n in [1_000usize, 10_000, 50_000] {
        let mut e = engine();
        let p = drive(&mut e, n);
        let t = Instant::now();
        let (d, bytes) = full_digest(&e);
        let full = t.elapsed();
        std::hint::black_box(d);
        let per = |d: Duration| d.as_nanos() as f64 / p.commands as f64 / 1_000.0;
        println!(
            "| {n} | {:.2} µs | {:.2} µs | {:.0} % | {:.2} µs | {:.1} ms | {bytes} |",
            per(p.apply),
            per(p.digest),
            100.0 * p.digest.as_secs_f64() / p.apply.as_secs_f64(),
            per(p.hash_only),
            full.as_secs_f64() * 1_000.0,
        );
    }
}
