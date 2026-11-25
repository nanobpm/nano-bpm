//! Native replay of the runtime parity corpus (#1247) — the Nano half of the
//! dual-engine branch-coverage measurement.
//!
//! The parity runner (`formal/parity/runtime/`) drives the corpus through the
//! `engine-wasm` `TestEngine`, which `cargo llvm-cov` cannot instrument. This
//! test plays the **same** `scenario.json` files (same step vocabulary, same
//! command mapping as `formal/parity/runtime/nano-backend.mjs` → the wasm
//! `TestEngine`) against `engine-core` natively, so
//! `formal/parity/coverage/nano-coverage.sh` can measure which `engine-core`
//! regions/branches the corpus actually exercises.
//!
//! Corpus directories: the committed seed corpus
//! (`formal/parity/runtime/corpus`) always, plus every directory listed in
//! `PARITY_CORPUS_DIRS` (`:`-separated) — the coverage script points it at the
//! bounded-exhaustive output of `formal/corpus/exhaustive.mjs --out`.
//!
//! Each scenario's `expect.completed`, `expect.jobsCreated` and
//! `expect.completedElements` are asserted, so the harness cannot silently drift
//! into executing a different run than the wasm driver does (a scenario that
//! stops matching fails here as well as in the runner).

// Uses the canonical `value_from_json` (serde feature) shared with engine-wasm.
#![cfg(feature = "serde")]
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
use std::path::{Path, PathBuf};

use nanobpmn_engine_core::{bpmn::parse_bpmn, Command, Engine, Event, Key, Value};
use serde_json::Value as Json;

/// The same activation cap as `observation.mjs`'s `MAX_JOBS_TO_ACTIVATE`
/// (100). `formal/parity/runtime/driver.test.mjs` reads this constant and
/// asserts it stays equal to the runtime cap, so a drift on either side fails
/// a test instead of silently diverging the native replay from the runtime
/// backends.
const MAX_JOBS_TO_ACTIVATE: usize = 100;

fn to_value(j: &Json) -> Value {
    // The single canonical conversion the wasm runner also uses, so both
    // runners execute the same scenario identically.
    nanobpmn_engine_core::value_from_json(j)
}

fn vars(j: Option<&Json>) -> BTreeMap<String, Value> {
    match j {
        Some(Json::Object(o)) => o.iter().map(|(k, v)| (k.clone(), to_value(v))).collect(),
        _ => BTreeMap::new(),
    }
}

fn corpus_dirs() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut dirs = vec![root.join("formal/parity/runtime/corpus")];
    if let Ok(extra) = std::env::var("PARITY_CORPUS_DIRS") {
        dirs.extend(
            extra
                .split(':')
                .filter(|s| !s.is_empty())
                .map(PathBuf::from),
        );
    }
    dirs
}

/// Every `<dir>/<name>/scenario.json`, sorted (deterministic order).
fn scenarios(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("corpus dir {}: {e}", dir.display()))
        .map(|e| e.unwrap().path().join("scenario.json"))
        .filter(|p| p.is_file())
        .collect();
    out.sort();
    out
}

struct Run {
    engine: Engine,
    now: u64,
    events: Vec<Event>,
}

impl Run {
    fn apply(&mut self, cmd: Command) {
        let evs = self
            .engine
            // Stamp every command with the scenario clock (as the wasm driver's
            // `apply_command_at(command, self.now)` does) so timers armed after
            // an `advanceTime` get deadlines relative to the advanced clock.
            .apply_command_at(cmd, self.now)
            .unwrap_or_else(|e| panic!("command failed: {e}"));
        self.events.extend(evs);
    }

    fn complete_jobs(&mut self, job_type: &str, max: usize, variables: BTreeMap<String, Value>) {
        let jobs = self
            .engine
            .activate_jobs(job_type, "parity-runner", max, 60_000, self.now);
        assert!(
            !jobs.is_empty(),
            "no '{job_type}' job was activatable (the runtime backends throw here too)"
        );
        for job in jobs {
            self.apply(Command::complete_job_with(job.key, variables.clone()));
        }
    }
}

fn run_scenario(path: &Path) -> (Key, Vec<Event>) {
    let sc: Json = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let dir = path.parent().unwrap();
    let xml = std::fs::read_to_string(dir.join(sc["bpmn"].as_str().unwrap())).unwrap();
    let mut run = Run {
        engine: Engine::new(),
        now: 0,
        events: Vec::new(),
    };
    run.apply(Command::DeployResources(parse_bpmn(&xml).unwrap()));
    run.apply(Command::create_instance_with(
        sc["processId"].as_str().unwrap(),
        vars(sc.get("variables")),
    ));
    let pik = run
        .events
        .iter()
        .find_map(|e| match e {
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        })
        .expect("instance created");

    for step in sc["steps"].as_array().cloned().unwrap_or_default() {
        let v = vars(step.get("variables"));
        let s = |k: &str| step[k].as_str().unwrap().to_string();
        match step["op"].as_str().unwrap() {
            "activateAndComplete" => run.complete_jobs(&s("jobType"), MAX_JOBS_TO_ACTIVATE, v),
            "completeOneJob" => run.complete_jobs(&s("jobType"), 1, v),
            "setVariables" => run.apply(Command::SetVariables {
                scope_key: pik,
                variables: v,
                local: false,
            }),
            "correlateMessage" | "publishMessage" => run.apply(Command::CorrelateMessage {
                message_name: s("name"),
                correlation_key: s("correlationKey"),
                variables: v,
                business_id: None,
                message_key: None,
                time_to_live: None,
                message_id: None,
            }),
            "broadcastSignal" => run.apply(Command::BroadcastSignal {
                signal_name: s("name"),
                variables: v,
            }),
            "advanceTime" => {
                run.now = run.now.saturating_add(step["ms"].as_u64().unwrap_or(0));
                let now = run.now;
                run.apply(Command::TriggerTimers { now });
                run.apply(Command::ExpireJobs { now });
            }
            other => panic!("unknown scenario step op {other:?}"),
        }
    }
    (pik, run.events)
}

fn counts<'a>(it: impl Iterator<Item = &'a str>) -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    for k in it {
        *m.entry(k.to_string()).or_insert(0) += 1;
    }
    m
}

fn expect_counts(j: Option<&Json>) -> Option<BTreeMap<String, u64>> {
    j.and_then(Json::as_object).map(|o| {
        o.iter()
            .map(|(k, v)| (k.clone(), v.as_u64().unwrap()))
            .collect()
    })
}

#[test]
fn parity_corpus_replays_natively() {
    let mut ran = 0usize;
    for dir in corpus_dirs() {
        for path in scenarios(&dir) {
            let sc: Json = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let (pik, events) = run_scenario(&path);
            let mine = || events.iter();
            let expect = &sc["expect"];
            let ctx = path.display();

            if let Some(want) = expect.get("completed").and_then(Json::as_bool) {
                let got = mine().any(
                    |e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == pik),
                );
                assert_eq!(got, want, "{ctx}: completed");
            }
            if let Some(want) = expect_counts(expect.get("jobsCreated")) {
                let got = counts(mine().filter_map(|e| match e {
                    Event::JobCreated {
                        instance_key,
                        job_type,
                        ..
                    } if *instance_key == pik => Some(job_type.as_str()),
                    _ => None,
                }));
                assert_eq!(got, want, "{ctx}: jobsCreated");
            }
            if let Some(want) = expect_counts(expect.get("completedElements")) {
                let got = counts(mine().filter_map(|e| match e {
                    Event::ElementCompleted {
                        instance_key,
                        element_id,
                        ..
                    } if *instance_key == pik => Some(element_id.as_str()),
                    _ => None,
                }));
                assert_eq!(got, want, "{ctx}: completedElements");
            }
            ran += 1;
        }
    }
    assert!(ran > 0, "the parity corpus is empty");
}
