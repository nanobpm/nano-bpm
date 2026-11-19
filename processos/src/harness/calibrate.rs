//! Calibration: ground the SimRunner's worker models in **measured production**.
//!
//! The SimRunner ([`super::sim`]) drives the real `engine-core`, but the per-task
//! worker behaviour it charges — service time and failure rate — comes from each
//! [`WorkerModel`]'s hand-authored numbers. That is fine for a synthetic scenario,
//! but to *speculatively execute* a hypothesis against a baseline that reflects
//! reality, the baseline workers should carry the service-time and failure-rate the
//! cluster actually measured.
//!
//! This module is that wire. Given a [`Scenario`] and the measured per-job-type
//! distributions (the [`super::cluster::ClusterRunSummary::by_job_type`] the
//! ClusterRunner produces, or any equivalent observation), it overrides the
//! *assigned* worker for each measured job type with the measured service time and
//! failure rate — leaving cost untouched (there is no cost channel in the trace
//! contract yet) and leaving *latent candidate* workers (the alternatives the LLM
//! may swap in, for which there is no production data) at their modelled values.
//!
//! Pure and deterministic; no I/O. The cluster adapter ([`calibrate_from_cluster`])
//! just maps the summary's job-type stats onto the generic [`MeasuredJobType`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::cluster::ClusterRunSummary;
use super::{Scenario, WorkerModel};

/// One job type's measured behaviour, the generic calibration input. Decoupled from
/// where it was observed (cluster union, single node, replayed window) so the same
/// calibration core serves every measurement path.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeasuredJobType {
    pub job_type: String,
    /// Mean service time observed for this job type, if any occurrence reported one.
    #[serde(default)]
    pub avg_service_ms: Option<u64>,
    /// Number of sampled occurrences of this job type.
    #[serde(default)]
    pub samples: usize,
    /// Total job failures observed for this type over those samples.
    #[serde(default)]
    pub failures: u32,
}

/// The outcome of calibrating a scenario: the full worker map with measured
/// overrides applied, plus which job types were (and were not) covered by data.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Calibration {
    /// The scenario's worker pool, with the assigned worker of each measured job
    /// type overridden from production. Keyed by worker id.
    pub workers: BTreeMap<String, WorkerModel>,
    /// Job types (in the scenario's default assignment) calibrated from data.
    pub calibrated: Vec<String>,
    /// Job types in the scenario's default assignment with no measured data — their
    /// workers keep their modelled numbers.
    pub uncalibrated: Vec<String>,
}

/// Per-worker accumulator so multiple job types sharing one worker fold into a
/// single sample-weighted service time and a pooled failure rate.
#[derive(Default)]
struct WorkerAcc {
    /// Sum of `avg_service_ms * samples` over job types that reported a service time.
    service_weighted: u128,
    /// Samples backing `service_weighted` (only occurrences with a service time).
    service_samples: u128,
    /// All sampled occurrences of this worker's job types.
    samples: u128,
    /// All failures over those occurrences.
    failures: u128,
}

/// Calibrate `scenario` from generic measured per-job-type distributions.
///
/// For each job type in the scenario's default assignment ([`Scenario::task_workers`])
/// that appears in `measured`, the assigned worker's `latency_ms` is set to the
/// sample-weighted measured service time and its `failure_rate` to the pooled
/// `failures / samples` (clamped to `[0,1]`). Cost and output are preserved. Workers
/// not referenced by any measured job type are left untouched.
pub fn calibrate_from_measured(scenario: &Scenario, measured: &[MeasuredJobType]) -> Calibration {
    // Index measurements by job type (last write wins on duplicates).
    let by_type: BTreeMap<&str, &MeasuredJobType> =
        measured.iter().map(|m| (m.job_type.as_str(), m)).collect();

    // Fold each measured, assigned job type into its worker's accumulator.
    let mut acc: BTreeMap<String, WorkerAcc> = BTreeMap::new();
    let mut calibrated: Vec<String> = Vec::new();
    let mut uncalibrated: Vec<String> = Vec::new();

    for (job_type, worker_id) in &scenario.task_workers {
        match by_type.get(job_type.as_str()) {
            Some(m) if m.samples > 0 => {
                let a = acc.entry(worker_id.clone()).or_default();
                a.samples += m.samples as u128;
                a.failures += m.failures as u128;
                if let Some(svc) = m.avg_service_ms {
                    a.service_weighted += svc as u128 * m.samples as u128;
                    a.service_samples += m.samples as u128;
                }
                calibrated.push(job_type.clone());
            }
            // No data, or data with zero samples: nothing to learn from.
            _ => uncalibrated.push(job_type.clone()),
        }
    }
    calibrated.sort();
    uncalibrated.sort();

    // Apply the overrides onto a clone of the worker pool.
    let mut workers = scenario.workers.clone();
    for (worker_id, a) in acc {
        let Some(w) = workers.get_mut(&worker_id) else {
            // The assignment references a worker absent from the pool; the SimRunner
            // validates the pool up front, so just skip rather than invent one.
            continue;
        };
        if let Some(latency) =
            (a.service_weighted + a.service_samples / 2).checked_div(a.service_samples)
        {
            // Round to nearest millisecond.
            w.latency_ms = latency as u64;
        }
        if a.samples > 0 {
            let rate = a.failures as f64 / a.samples as f64;
            w.failure_rate = rate.clamp(0.0, 1.0);
        }
    }

    Calibration {
        workers,
        calibrated,
        uncalibrated,
    }
}

/// Calibrate `scenario` from a live [`ClusterRunSummary`] — the ClusterRunner's
/// measured per-job-type breakdown mapped onto [`MeasuredJobType`]. This is the
/// live-path adapter the closed loop uses; the generic core powers the
/// caller-supplied `/api/harness/calibrate` endpoint.
#[allow(dead_code)]
pub fn calibrate_from_cluster(scenario: &Scenario, summary: &ClusterRunSummary) -> Calibration {
    let measured: Vec<MeasuredJobType> = summary
        .by_job_type
        .iter()
        .map(|s| MeasuredJobType {
            job_type: s.job_type.clone(),
            avg_service_ms: s.avg_service_ms,
            samples: s.samples,
            failures: s.failures,
        })
        .collect();
    calibrate_from_measured(scenario, &measured)
}

/// Return a copy of `scenario` with its worker pool replaced by the calibrated one,
/// ready to feed straight into the SimRunner / ranker / hypothesis loop.
pub fn apply(scenario: &Scenario, calibration: &Calibration) -> Scenario {
    let mut s = scenario.clone();
    s.workers = calibration.workers.clone();
    s
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::harness::{Objective, ScenarioInput};

    fn worker(id: &str, cost: f64, latency_ms: u64, failure_rate: f64) -> WorkerModel {
        WorkerModel {
            id: id.to_string(),
            cost,
            latency_ms,
            failure_rate,
            output: BTreeMap::new(),
        }
    }

    /// A two-task scenario: `classify -> fast`, `summarize -> deep`, with a latent
    /// candidate `cheap` that production never ran.
    fn scenario() -> Scenario {
        let mut workers = BTreeMap::new();
        workers.insert("fast".to_string(), worker("fast", 1.0, 10, 0.0));
        workers.insert("deep".to_string(), worker("deep", 5.0, 100, 0.1));
        workers.insert("cheap".to_string(), worker("cheap", 0.5, 200, 0.4));
        let mut task_workers = BTreeMap::new();
        task_workers.insert("classify".to_string(), "fast".to_string());
        task_workers.insert("summarize".to_string(), "deep".to_string());
        Scenario {
            name: "t".to_string(),
            test_model: String::new(),
            process_id: None,
            workers,
            task_workers,
            latent_options: BTreeMap::new(),
            inputs: vec![ScenarioInput {
                vars: BTreeMap::new(),
                expected: BTreeMap::new(),
            }],
            golden: None,
            objective: Objective::default(),
            seed: 1,
        }
    }

    #[test]
    fn calibrates_latency_and_failure_from_measured() {
        let s = scenario();
        let measured = vec![
            MeasuredJobType {
                job_type: "classify".to_string(),
                avg_service_ms: Some(42),
                samples: 100,
                failures: 5,
            },
            MeasuredJobType {
                job_type: "summarize".to_string(),
                avg_service_ms: Some(250),
                samples: 50,
                failures: 0,
            },
        ];
        let c = calibrate_from_measured(&s, &measured);
        assert_eq!(c.workers["fast"].latency_ms, 42);
        assert!((c.workers["fast"].failure_rate - 0.05).abs() < 1e-9);
        assert_eq!(c.workers["deep"].latency_ms, 250);
        assert_eq!(c.workers["deep"].failure_rate, 0.0);
        assert_eq!(c.calibrated, vec!["classify", "summarize"]);
        assert!(c.uncalibrated.is_empty());
    }

    #[test]
    fn preserves_cost_and_leaves_latent_workers_untouched() {
        let s = scenario();
        let measured = vec![MeasuredJobType {
            job_type: "classify".to_string(),
            avg_service_ms: Some(42),
            samples: 10,
            failures: 0,
        }];
        let c = calibrate_from_measured(&s, &measured);
        // Cost is never inferred from traces.
        assert_eq!(c.workers["fast"].cost, 1.0);
        // The latent candidate has no production data; it keeps its modelled numbers.
        assert_eq!(c.workers["cheap"].latency_ms, 200);
        assert_eq!(c.workers["cheap"].failure_rate, 0.4);
    }

    #[test]
    fn reports_uncalibrated_job_types() {
        let s = scenario();
        let measured = vec![MeasuredJobType {
            job_type: "classify".to_string(),
            avg_service_ms: Some(42),
            samples: 10,
            failures: 0,
        }];
        let c = calibrate_from_measured(&s, &measured);
        assert_eq!(c.calibrated, vec!["classify"]);
        assert_eq!(c.uncalibrated, vec!["summarize"]);
        // summarize's worker is unchanged.
        assert_eq!(c.workers["deep"].latency_ms, 100);
    }

    #[test]
    fn missing_service_keeps_latency_but_still_sets_failure() {
        let s = scenario();
        let measured = vec![MeasuredJobType {
            job_type: "classify".to_string(),
            avg_service_ms: None,
            samples: 20,
            failures: 4,
        }];
        let c = calibrate_from_measured(&s, &measured);
        assert_eq!(c.workers["fast"].latency_ms, 10); // unchanged
        assert!((c.workers["fast"].failure_rate - 0.2).abs() < 1e-9);
    }

    #[test]
    fn clamps_failure_rate_when_failures_exceed_samples() {
        let s = scenario();
        // Retries can drive failures past the occurrence count; rate must stay <= 1.
        let measured = vec![MeasuredJobType {
            job_type: "classify".to_string(),
            avg_service_ms: Some(10),
            samples: 5,
            failures: 9,
        }];
        let c = calibrate_from_measured(&s, &measured);
        assert_eq!(c.workers["fast"].failure_rate, 1.0);
    }

    #[test]
    fn shared_worker_uses_sample_weighted_service() {
        let mut s = scenario();
        // Point both tasks at the same worker.
        s.task_workers
            .insert("summarize".to_string(), "fast".to_string());
        let measured = vec![
            MeasuredJobType {
                job_type: "classify".to_string(),
                avg_service_ms: Some(10),
                samples: 90,
                failures: 0,
            },
            MeasuredJobType {
                job_type: "summarize".to_string(),
                avg_service_ms: Some(100),
                samples: 10,
                failures: 0,
            },
        ];
        let c = calibrate_from_measured(&s, &measured);
        // (10*90 + 100*10) / 100 = 19.
        assert_eq!(c.workers["fast"].latency_ms, 19);
    }

    #[test]
    fn zero_sample_measurement_is_not_calibration() {
        let s = scenario();
        let measured = vec![MeasuredJobType {
            job_type: "classify".to_string(),
            avg_service_ms: Some(42),
            samples: 0,
            failures: 0,
        }];
        let c = calibrate_from_measured(&s, &measured);
        assert!(c.calibrated.is_empty());
        assert_eq!(c.uncalibrated, vec!["classify", "summarize"]);
        assert_eq!(c.workers["fast"].latency_ms, 10); // untouched
    }

    #[test]
    fn apply_replaces_the_worker_pool() {
        let s = scenario();
        let measured = vec![MeasuredJobType {
            job_type: "classify".to_string(),
            avg_service_ms: Some(42),
            samples: 10,
            failures: 0,
        }];
        let c = calibrate_from_measured(&s, &measured);
        let calibrated = apply(&s, &c);
        assert_eq!(calibrated.workers["fast"].latency_ms, 42);
        // Original is untouched (pure).
        assert_eq!(s.workers["fast"].latency_ms, 10);
    }
}
