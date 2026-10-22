//! Hot-path microbenchmark: isolates the single-threaded engine cost of the
//! benchmark workload (create instance -> activate job -> complete job) with a
//! large variable payload vs a tiny one. Run with:
//!   cargo run --release --example hotpath
//!
//! This does NOT involve the server's global lock, HTTP, journaling, or the
//! read-model exporter. It measures only what the engine itself spends per
//! process-instance lifecycle, so we can tell how much of the per-op critical
//! section is engine work vs variable copying.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::HashMap;
use std::time::Instant;

use nanobpmn_engine_core::{Command, Engine, Event, ProcessBuilder, Value};

fn build_payload(size_kb: usize) -> HashMap<String, Value> {
    let mut s = String::with_capacity(size_kb * 1024);
    while s.len() < size_kb * 1024 {
        s.push_str("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");
    }
    s.truncate(size_kb * 1024);
    let mut m = HashMap::new();
    if size_kb > 0 {
        m.insert("data".to_string(), Value::Str(s));
    }
    m
}

fn run(label: &str, payload_kb: usize, iters: usize) {
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

    let payload = build_payload(payload_kb);

    // Warm up.
    for _ in 0..1000 {
        lifecycle(&mut engine, &payload);
    }

    let mut t_create = std::time::Duration::ZERO;
    let mut t_activate = std::time::Duration::ZERO;
    let mut t_complete = std::time::Duration::ZERO;

    let t0 = Instant::now();
    for _ in 0..iters {
        let s = Instant::now();
        let events = engine
            .apply_command_at(
                Command::create_instance_with("bench", payload.clone()),
                1_000,
            )
            .unwrap();
        t_create += s.elapsed();
        let _instance_key = events.iter().find_map(Event::instance_key).unwrap();

        let s = Instant::now();
        let job = engine
            .activate_jobs("test-job", "w", 1, 60_000, 1_000)
            .into_iter()
            .next()
            .expect("job activatable");
        // Mimic the server: it walks the activated job's variable snapshot to
        // build the REST response. Touch the payload so the clone is not elided.
        let _vars_len = job.variables.get("data").map(|v| match v {
            Value::Str(s) => s.len(),
            _ => 0,
        });
        t_activate += s.elapsed();

        let s = Instant::now();
        engine
            .apply_command_at(Command::complete_job(job.key), 1_000)
            .unwrap();
        t_complete += s.elapsed();

        // Keep hot state bounded like the server does (evict terminal).
        engine.evict_completed();
    }
    let total = t0.elapsed();

    let per = total.as_secs_f64() / iters as f64;
    println!(
        "{label:<14} payload={payload_kb:>4}KB  {iters} iters  total={:.2}s  \
         {:>8.0} lifecycles/s  ({:>6.1}us/lifecycle)",
        total.as_secs_f64(),
        iters as f64 / total.as_secs_f64(),
        per * 1e6,
    );
    println!(
        "               create={:>6.1}us  activate={:>6.1}us  complete={:>6.1}us",
        t_create.as_secs_f64() / iters as f64 * 1e6,
        t_activate.as_secs_f64() / iters as f64 * 1e6,
        t_complete.as_secs_f64() / iters as f64 * 1e6,
    );
}

fn lifecycle(engine: &mut Engine, payload: &HashMap<String, Value>) {
    let events = engine
        .apply_command_at(
            Command::create_instance_with("bench", payload.clone()),
            1_000,
        )
        .unwrap();
    let _instance_key = events.iter().find_map(Event::instance_key).unwrap();
    let job = engine
        .activate_jobs("test-job", "w", 1, 60_000, 1_000)
        .into_iter()
        .next()
        .unwrap();
    engine
        .apply_command_at(Command::complete_job(job.key), 1_000)
        .unwrap();
    engine.evict_completed();
}

fn main() {
    let iters = 20_000;
    run("tiny", 0, iters);
    run("small-1kb", 1, iters);
    run("bench-50kb", 50, iters);
    run("huge-200kb", 200, iters / 2);
}
