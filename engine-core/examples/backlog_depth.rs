//! Backlog-depth microbenchmark (#1827): does the engine's per-job cost grow
//! with the number of pending jobs? Pre-creates `N` one-service-task instances
//! (10 KB payload, like the SDK performance matrix), then drains them in
//! activate-32 + complete batches, reporting per-job cost per backlog band.
//! A flat profile means the engine is O(1)/O(log n) per job; a rising one
//! means some per-job work scales with the backlog. Run with:
//!   cargo run --release --example backlog_depth [N] [payload_kb] [call]
//!
//! With `call`, each instance is a parent whose call activity runs the
//! one-service-task process as its child, so every completed job ends a child
//! and then its parent: the path whose cascade used to scan every resident
//! instance (#1827).
//!
//! Like `hotpath`, this excludes the server (lock, HTTP, journal, read model).

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
use std::time::{Duration, Instant};

use nanobpmn_engine_core::{Command, Engine, ProcessBuilder, Value};

fn payload(kb: usize) -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    if kb > 0 {
        m.insert("data".to_string(), Value::Str("x".repeat(kb * 1024)));
    }
    m
}

fn main() {
    let mut args = std::env::args().skip(1);
    let n: usize = args.next().map_or(110_000, |a| a.parse().unwrap());
    let kb: usize = args.next().map_or(10, |a| a.parse().unwrap());
    let batch = 32;
    let band = n / 10;

    let call = args.next().as_deref() == Some("call");

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
    let process = if call {
        let parent = ProcessBuilder::new("bench-parent")
            .start_event("start")
            .call_activity("call", "bench")
            .end_event("end")
            .connect("start", "call")
            .connect("call", "end")
            .build()
            .unwrap();
        engine
            .apply_command(Command::DeployProcess(parent))
            .unwrap();
        "bench-parent"
    } else {
        "bench"
    };
    let vars = payload(kb);

    let t = Instant::now();
    for _ in 0..n {
        engine
            .apply_command_at(Command::create_instance_with(process, vars.clone()), 1_000)
            .unwrap();
    }
    println!(
        "precreated {n} instances ({kb} KB) in {:.1}s",
        t.elapsed().as_secs_f64()
    );

    let mut remaining = n;
    let (mut act, mut comp, mut evict, mut done) =
        (Duration::ZERO, Duration::ZERO, Duration::ZERO, 0usize);
    let mut band_start = remaining;
    while remaining > 0 {
        let s = Instant::now();
        let jobs = engine.activate_jobs("test-job", "w", batch, 60_000, 1_000);
        act += s.elapsed();
        assert!(
            !jobs.is_empty(),
            "backlog {remaining} but nothing activatable"
        );
        let s = Instant::now();
        for j in &jobs {
            engine
                .apply_command_at(Command::complete_job(j.key), 1_000)
                .unwrap();
        }
        comp += s.elapsed();
        // Keyed eviction, as the server's exporter does (`evict_instances` on
        // the completed batch), not the boot-time full scan `evict_completed`.
        let mut keys: Vec<_> = jobs.iter().map(|j| j.instance_key).collect();
        // In `call` mode each finished child's parent has finished too.
        let parents: Vec<_> = keys
            .iter()
            .filter_map(|k| engine.state().instances.get(k))
            .filter_map(|i| i.parent_process_instance_key)
            .collect();
        keys.extend(parents);
        let s = Instant::now();
        engine.evict_instances(&keys);
        evict += s.elapsed();
        done += jobs.len();
        remaining -= jobs.len();
        if band_start - remaining >= band || remaining == 0 {
            let us = |d: Duration| d.as_secs_f64() * 1e6 / done as f64;
            println!(
                "backlog {:>7}->{:<7} per job: activate {:>6.1}us  complete {:>6.1}us  evict {:>6.1}us  => {:>7.0} jobs/s",
                band_start,
                remaining,
                us(act),
                us(comp),
                us(evict),
                done as f64 / (act + comp + evict).as_secs_f64()
            );
            (act, comp, evict, done) = (Duration::ZERO, Duration::ZERO, Duration::ZERO, 0);
            band_start = remaining;
        }
    }
}
