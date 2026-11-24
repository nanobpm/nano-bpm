//! On-actor snapshot-clone profile (#50): how long does `Engine::snapshot()`
//! — the `state.clone()` a Raft snapshot build runs on the partition's
//! single-threaded engine actor (`server/src/raft.rs` `get_snapshot_builder`)
//! — stall the actor at realistic and large working sets?
//!
//! Pre-creates `N` resident one-service-task instances (each with a live job
//! and a `payload_kb` variable payload), then times `Engine::snapshot()` and
//! `Engine::snapshot_control_only()` several times per band, reporting the
//! median / max clone time (== actor stall: nothing else can run on the actor
//! while it clones) and the per-instance cost. Also reports the steady-state
//! per-command cost so the stall can be expressed as "commands not served".
//! Run with:
//!   cargo run --release --features serde --example snapshot_clone [max_n] [payload_kb] [reps]
//!
//! Excludes the server (serialization is already off-actor, see #49).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
#![allow(
    clippy::disallowed_methods,
    reason = "non-production target: the apply-path purity lint (#1663) does not govern test/example harness I/O"
)]
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use nanobpmn_engine_core::{Command, Engine, ProcessBuilder, Value};

fn payload(kb: usize) -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    if kb > 0 {
        m.insert("data".to_string(), Value::Str("x".repeat(kb * 1024)));
    }
    m
}

/// Times `f` `reps` times. The closure returns the snapshot so it is dropped
/// only *after* the elapsed time is sampled: teardown of the cloned maps is
/// not part of the on-actor clone cost being measured.
fn time_reps<T>(reps: usize, mut f: impl FnMut() -> T) -> (Duration, Duration) {
    let mut v: Vec<Duration> = (0..reps)
        .map(|_| {
            let s = Instant::now();
            let out = black_box(f());
            let d = s.elapsed();
            drop(out);
            d
        })
        .collect();
    v.sort();
    (v[v.len() / 2], v[v.len() - 1])
}

fn main() {
    let mut args = std::env::args().skip(1);
    let max_n: usize = args.next().map_or(200_000, |a| a.parse().unwrap());
    let kb: usize = args.next().map_or(10, |a| a.parse().unwrap());
    let reps: usize = args.next().map_or(5, |a| a.parse().unwrap()).max(1);

    let def = ProcessBuilder::new("bench")
        .start_event("start")
        .service_task("task", "test-job")
        .end_event("end")
        .connect("start", "task")
        .connect("task", "end")
        .build()
        .unwrap();
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let vars = payload(kb);

    println!("payload {kb} KB, {reps} reps per band (median / max)");
    let mut bands = vec![
        1_000usize, 10_000, 50_000, 100_000, 200_000, 500_000, 1_000_000,
    ];
    bands.retain(|&b| b <= max_n);
    if bands.last() != Some(&max_n) {
        bands.push(max_n);
    }
    let mut resident = 0usize;
    for band in bands {
        let s = Instant::now();
        let to_add = band - resident;
        for _ in resident..band {
            engine
                .apply_command_at(Command::create_instance_with("bench", vars.clone()), 1_000)
                .unwrap();
        }
        resident = band;
        let per_cmd = s.elapsed().as_secs_f64() * 1e6 / to_add.max(1) as f64;

        let (full_med, full_max) = time_reps(reps, || engine.snapshot());
        let (ctl_med, ctl_max) = time_reps(reps, || engine.snapshot_control_only());
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        println!(
            "resident {band:>8}: snapshot {:>8.2} / {:>8.2} ms ({:>5.0} ns/inst, ~{:>6.0} creates stalled)  control_only {:>8.2} / {:>8.2} ms  create {per_cmd:>5.1}us",
            ms(full_med),
            ms(full_max),
            full_med.as_secs_f64() * 1e9 / band as f64,
            full_med.as_secs_f64() * 1e6 / per_cmd,
            ms(ctl_med),
            ms(ctl_max),
        );
    }
}
