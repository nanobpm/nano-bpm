//! Replication-entry size probe for ADR 0066 (replicate events vs commands,
//! #1655). For representative workloads it serializes, with the same
//! `serde_json` encoding the Raft log store uses, (a) the command each
//! lifecycle step would replicate under command replication and (b) the events
//! that command produced, which event replication would replicate instead.
//! Run with:
//!   cargo run --release --features serde --example replication_size

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
use std::collections::HashMap;

use nanobpmn_engine_core::{Command, Engine, Event, ProcessBuilder, Value};

fn payload(size_kb: usize) -> HashMap<String, Value> {
    let mut m = HashMap::new();
    if size_kb > 0 {
        let s: String = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            .chars()
            .cycle()
            .take(size_kb * 1024)
            .collect();
        m.insert("data".to_string(), Value::Str(s));
    }
    m
}

fn json_len<T: serde::Serialize>(v: &T) -> usize {
    serde_json::to_vec(v).unwrap().len()
}

#[derive(Default)]
struct Tally {
    command: usize,
    events: usize,
    event_count: usize,
}

impl Tally {
    fn add(&mut self, command: &Command, events: &[Event]) {
        self.command += json_len(command);
        self.events += json_len(&events);
        self.event_count += events.len();
    }
}

/// One instance lifecycle of a `tasks`-long sequential service-task chain:
/// create (with `create_kb` payload) then complete each job (with
/// `complete_kb` payload). Activation is leader-local (ADR 0002) and is not a
/// replicated command in either model, so it is not counted.
fn lifecycle(tasks: usize, create_kb: usize, complete_kb: usize) -> (Tally, (usize, usize)) {
    let mut b = ProcessBuilder::new("p").start_event("start");
    for i in 0..tasks {
        b = b.service_task(format!("t{i}"), "job");
    }
    b = b.end_event("end");
    let mut prev = "start".to_string();
    for i in 0..tasks {
        b = b.connect(prev.clone(), format!("t{i}"));
        prev = format!("t{i}");
    }
    let def = b.connect(prev, "end").build().unwrap();

    let mut engine = Engine::new();
    let deploy = Command::DeployProcess(def);
    let deploy_events = engine.apply_command_at(deploy.clone(), 1_000).unwrap();
    let deploy_sizes = (json_len(&deploy), json_len(&deploy_events));

    let mut t = Tally::default();
    let create = Command::create_instance_with("p", payload(create_kb));
    let ev = engine.apply_command_at(create.clone(), 1_000).unwrap();
    t.add(&create, &ev);
    for _ in 0..tasks {
        let job = engine
            .activate_jobs("job", "w", 1, 60_000, 1_000)
            .into_iter()
            .next()
            .unwrap();
        let complete = Command::complete_job_with(job.key, payload(complete_kb));
        let ev = engine.apply_command_at(complete.clone(), 1_000).unwrap();
        t.add(&complete, &ev);
    }
    (t, deploy_sizes)
}

fn main() {
    let mut deploys = Vec::new();
    println!("| workload | commands (B) | events (B) | events/commands | events per lifecycle |");
    println!("|---|---:|---:|---:|---:|");
    for (label, tasks, create_kb, complete_kb) in [
        ("1 task, no vars", 1, 0, 0),
        ("1 task, 1 KB create", 1, 1, 0),
        ("1 task, 50 KB create (benchmark)", 1, 50, 0),
        ("1 task, 50 KB create + 50 KB complete", 1, 50, 50),
        ("5 tasks, no vars", 5, 0, 0),
        ("5 tasks, 1 KB create + 1 KB per complete", 5, 1, 1),
    ] {
        let (t, (deploy_cmd, deploy_ev)) = lifecycle(tasks, create_kb, complete_kb);
        deploys.push((tasks, deploy_cmd, deploy_ev));
        println!(
            "| {label} | {} | {} | {:.2}x | {} |",
            t.command,
            t.events,
            t.events as f64 / t.command as f64,
            t.event_count
        );
    }
    deploys.dedup_by_key(|d| d.0);
    println!();
    println!("| deploy | command (B) | events (B) |");
    println!("|---|---:|---:|");
    for (tasks, cmd, ev) in deploys {
        println!("| {tasks}-task process | {cmd} | {ev} |");
    }
}
