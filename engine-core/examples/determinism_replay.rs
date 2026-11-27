//! Determinism-CI corpus runner CLI (ADR 0066 slice 6, #1665). See
//! `determinism/runner.rs` for the record format; `scripts/determinism-ci.sh`
//! drives the matrix.
//!
//! ```bash
//! cargo run --features serde --example determinism_replay -- CORPUS --out RECORDS \
//!     [--state-every N] [--stop-after K --snapshot-out SNAP] \
//!     [--resume SNAP --start-at K] [--canary] [--skip-undecodable] \
//!     [--event-gateway-runtime]
//! ```
//!
//! Kept free of newer engine API: the previous-release cell builds this file
//! inside the previous release's source tree.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "non-production target (integration tests / examples); CLI argument parsing is not an apply path (#1663)"
)]

#[path = "determinism/runner.rs"]
mod runner;

use std::io::{BufReader, BufWriter, Write};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut corpus = None;
    let mut out = None;
    let mut snapshot_out = None;
    let mut resume = None;
    let mut opts = runner::RunOptions::default();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--out" => out = Some(val()),
            "--state-every" => opts.state_every = val().parse().expect("--state-every"),
            "--stop-after" => opts.stop_after = Some(val().parse().expect("--stop-after")),
            "--snapshot-out" => snapshot_out = Some(val()),
            "--resume" => resume = Some(val()),
            "--start-at" => opts.start_at = val().parse().expect("--start-at"),
            "--canary" => opts.canary = true,
            "--skip-undecodable" => opts.skip_undecodable = true,
            "--event-gateway-runtime" => opts.event_gateway_runtime = true,
            _ if corpus.is_none() => corpus = Some(arg),
            _ => panic!("unexpected argument {arg}"),
        }
    }
    let corpus = corpus.expect("usage: determinism_replay CORPUS --out RECORDS …");
    let entries = runner::read_corpus(BufReader::new(
        std::fs::File::open(&corpus).expect("corpus"),
    ))
    .expect("read corpus");
    let mut engine = match &resume {
        Some(path) => runner::load_snapshot(&std::fs::read(path).expect("snapshot")).expect("load"),
        None => nanobpmn_engine_core::Engine::new(),
    };
    let file = std::fs::File::create(out.expect("--out is required")).expect("create --out");
    let mut writer = BufWriter::new(file);
    let end = runner::run(&mut engine, entries, &opts, &mut writer).expect("run");
    writer.flush().expect("flush");
    if let Some(path) = snapshot_out {
        std::fs::write(path, runner::save_snapshot(&engine).expect("snapshot")).expect("write");
    }
    eprintln!(
        "determinism_replay: applied entries [{}, {end})",
        opts.start_at
    );
}
