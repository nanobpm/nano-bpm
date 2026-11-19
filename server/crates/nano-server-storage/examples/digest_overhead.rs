//! Measures the replica divergence digest's per-entry overhead (ADR 0066
//! slice 5, #1664): the chain fold over one Raft entry's command events,
//! compared with applying + journaling that entry in memory.
//!
//! ```bash
//! cd server && cargo run --release -p nano-server-storage --example digest_overhead
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples)"
)]

use std::time::Instant;

use nano_server_storage::digest::next_entry_digest;
use nano_server_storage::journal::Journal;
use nanobpmn_engine_core::{Command, ProcessBuilder};

const ENTRIES: u64 = 20_000;

fn main() {
    let proc = ProcessBuilder::new("bench")
        .start_event("s")
        .service_task("t", "work")
        .end_event("e")
        .connect("s", "t")
        .connect("t", "e")
        .build()
        .expect("valid process");
    let mut journal = Journal::in_memory();
    let _ = journal.apply_command(Command::DeployProcess(proc)).unwrap();

    // Capture one representative entry's events (a create instance).
    let mut entries = Vec::new();
    let started = Instant::now();
    for _ in 0..ENTRIES {
        let (events, _) = journal
            .apply_command(Command::create_instance("bench"))
            .unwrap();
        entries.push(events);
    }
    let apply = started.elapsed();

    let mut prev: Option<String> = None;
    let started = Instant::now();
    for (i, events) in entries.iter().enumerate() {
        let d = next_entry_digest(prev.as_deref(), i as u64 + 1, 0, events);
        prev = Some(serde_json::json!({ "digest": d }).to_string());
    }
    let fold = started.elapsed();

    let events: usize = entries.iter().map(|e| e.len()).sum();
    let per = |d: std::time::Duration| d.as_nanos() as f64 / ENTRIES as f64 / 1000.0;
    println!(
        "entries: {ENTRIES}, events/entry: {:.1}",
        events as f64 / ENTRIES as f64
    );
    println!("apply+journal (in-memory): {:.2} us/entry", per(apply));
    println!("digest fold + record:      {:.2} us/entry", per(fold));
    println!(
        "digest overhead:           {:.1}% of in-memory apply",
        fold.as_secs_f64() / apply.as_secs_f64() * 100.0
    );
}
