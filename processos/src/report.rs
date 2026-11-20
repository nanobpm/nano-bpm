//! Stage T1 **Insights** — fold Nano's exported traces into a per-process /
//! per-element performance report. This is the read-only foundation every later
//! ProcessOS capability (cost model, canary, reasoning) builds on: faithfully
//! surface "what happened" before anything counterfactual or autonomous is added.
//!
//! Pure aggregation over the read-contract DTOs — no engine, no LLM, no writes.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::contracts::{InstanceTrace, Metrics, NanoClient, Role, TraceSummary};
use crate::dataset::TraceSource;

/// The full Insights document served at `GET /api/insights`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Insights {
    pub generated_at_ms: u64,
    pub nano_base_url: String,
    /// True size of the underlying dataset when known (an in-memory captured
    /// dataset); `None` for a live gateway whose total isn't known up front.
    /// When present this is the authoritative instance count — `totals.instances`
    /// is the number actually *analyzed* for this report (== `datasetTotal` once
    /// the whole population fits the analysis window).
    pub dataset_total: Option<usize>,
    /// Number of trace details sampled to build the per-element aggregates.
    pub sampled_instances: usize,
    pub totals: Totals,
    pub live: Option<Metrics>,
    pub processes: Vec<ProcessInsight>,
    pub incident_clusters: Vec<IncidentCluster>,
    /// **G1 content fold** (doc #6): the application-domain signals projected onto
    /// the sampled traces, aggregated domain-free by generic [`Role`]/`Scope`. This is
    /// the "content" half of process+content — surfaced faithfully, never scored here.
    pub content: DomainContent,
}

/// Domain-free rollup of the [`crate::contracts::DomainSignal`]s present on the
/// sampled traces (doc #6 G1). Keys strictly on generic `role`/`scope`, never on an
/// app `kind`, so the reasoner stays domain-independent.
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DomainContent {
    /// Total domain signals projected across the sampled instances.
    pub signals: usize,
    /// Of those, how many are agent priors/hypotheses (not measured evidence).
    /// Surfaced but excluded from the measured pattern folds below.
    pub agent_priors: usize,
    /// Signal counts by generic role.
    pub by_role: Vec<LabelCount>,
    /// Redundant-recompute: a `knowledge` signal independently re-derived across
    /// ≥2 sibling actors within one instance (measured evidence only). The flagship
    /// domain-inefficiency (doc #6 §1.2), expressed purely over role + scope.
    pub redundant_recompute: Vec<RedundantRecompute>,
    /// Delayed correctness/value rollup, when instances supply `outcomeTruth`.
    pub outcome_truth: Option<OutcomeTruthRollup>,
}

/// A generic `{label, count}` tally, reused for role and status breakdowns.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelCount {
    pub label: String,
    pub count: usize,
}

/// One redundant-recompute finding: the same knowledge key produced independently
/// by multiple sibling actors within a single instance.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RedundantRecompute {
    /// The instance the recompute happened in.
    pub instance: String,
    /// The recurring knowledge key: the app's stable `dedupeKey` (never the
    /// unbounded free-text `body`, which is deliberately not used as a key).
    pub key: String,
    /// Distinct sibling actors that each independently produced it.
    pub actors: Vec<String>,
    /// Number of distinct actors (== `actors.len()`), the recompute multiplicity.
    pub occurrences: usize,
}

/// Aggregate of the per-instance `outcomeTruth` signals.
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeTruthRollup {
    /// Instances that supplied an `outcomeTruth`.
    pub instances: usize,
    /// Count by the richer `status` (e.g. `merged | escalated | abandoned`).
    pub by_status: Vec<LabelCount>,
    /// Mean `roundsToConverge` over instances that reported it.
    pub avg_rounds_to_converge: Option<f64>,
    /// Mean `reworkEvents` over instances that reported it.
    pub avg_rework_events: Option<f64>,
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub instances: usize,
    pub completed: usize,
    pub terminated: usize,
    pub active: usize,
    pub incidents: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessInsight {
    pub process_id: String,
    pub instances: usize,
    pub completed: usize,
    pub incident_instances: usize,
    pub avg_duration_ms: Option<u64>,
    pub p95_duration_ms: Option<u64>,
    /// p99 end-to-end duration — the tail the `process-optimization` doc (§2)
    /// flags as a signal means alone hide; the live/cluster path must surface it.
    pub p99_duration_ms: Option<u64>,
    /// The element with the highest average self-duration — the headline bottleneck.
    pub bottleneck: Option<ElementInsight>,
    pub elements: Vec<ElementInsight>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ElementInsight {
    pub element_id: String,
    pub count: usize,
    pub avg_duration_ms: Option<u64>,
    /// For service tasks: average time a job waited in queue before activation.
    pub avg_queue_ms: Option<u64>,
    /// For service tasks: average worker service time once activated.
    pub avg_service_ms: Option<u64>,
    pub incidents: u32,
    pub job_failures: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IncidentCluster {
    pub element_id: String,
    pub kind: String,
    pub count: usize,
    pub top_reason: String,
}

/// Fetch recent traces from Nano and fold them into an Insights report. `sample`
/// bounds how many trace *details* we pull for per-element aggregation (the list
/// gives totals cheaply; details cost one request each).
pub async fn build(nano: &NanoClient, limit: usize, sample: usize) -> Result<Insights, String> {
    build_over(&TraceSource::Live(nano.clone()), limit, sample).await
}

/// Build Insights over **any** trace source — a live gateway or a loaded dataset.
/// This is the source-agnostic core; [`build`] is the live-gateway convenience.
pub async fn build_over(
    src: &TraceSource,
    limit: usize,
    sample: usize,
) -> Result<Insights, String> {
    let summaries = src.list_traces(limit).await?;
    let live = src.metrics().await;

    let totals = totals_of(&summaries);

    // Pull details for a bounded sample (most recent first) for element-level folds.
    let take = sample.min(summaries.len());
    let mut details: Vec<InstanceTrace> = Vec::with_capacity(take);
    for s in summaries.iter().take(take) {
        if let Ok(t) = src.trace(&s.instance_key).await {
            details.push(t);
        }
    }

    let processes = fold_processes(&details);
    let incident_clusters = fold_incident_clusters(&details);
    let content = fold_domain_signals(&details);

    Ok(Insights {
        generated_at_ms: now_ms(),
        nano_base_url: src.label(),
        dataset_total: src.total(),
        sampled_instances: details.len(),
        totals,
        live,
        processes,
        incident_clusters,
        content,
    })
}

fn totals_of(summaries: &[TraceSummary]) -> Totals {
    let mut t = Totals {
        instances: summaries.len(),
        ..Default::default()
    };
    for s in summaries {
        match s.outcome.as_str() {
            "completed" => t.completed += 1,
            "terminated" => t.terminated += 1,
            _ => t.active += 1,
        }
        t.incidents += s.incident_count;
    }
    t
}

/// **G1 content fold** (doc #6): aggregate the [`crate::contracts::DomainSignal`]s
/// present on the sampled traces into a domain-free [`DomainContent`]. Keys strictly
/// on generic `role`/`scope`; app `kind` is never inspected. Only **measured**
/// signals feed the pattern folds — agent priors are counted separately.
fn fold_domain_signals(details: &[InstanceTrace]) -> DomainContent {
    let mut by_role: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut signals = 0usize;
    let mut agent_priors = 0usize;
    // instance -> knowledge key -> distinct sibling actors (measured evidence only).
    let mut recompute: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> = BTreeMap::new();

    let mut ot_instances = 0usize;
    let mut status_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut rounds: Vec<u32> = Vec::new();
    let mut rework: Vec<u32> = Vec::new();

    for t in details {
        for s in &t.domain_signals {
            signals += 1;
            *by_role.entry(s.role.as_str()).or_default() += 1;
            let measured = s.is_measured();
            if !measured {
                agent_priors += 1;
            }
            if measured && s.role == Role::Knowledge {
                // Aggregate only on the app's stable idempotency key. We deliberately
                // do NOT fall back to the free-text `body`: it is unbounded, would bloat
                // the fold and the `/api/insights` payload, and could duplicate sensitive
                // text into a top-level summary. A knowledge signal without a dedupe key
                // simply doesn't participate in recompute detection. Optional wire
                // fields that arrive as empty strings are treated as absent (like
                // `dedupeKey`) so we never key a finding under an empty actor/instance.
                let actor = s.scope.actor.clone().filter(|a| !a.is_empty());
                let key = s.dedupe_key.clone().filter(|k| !k.is_empty());
                if let (Some(actor), Some(key)) = (actor, key) {
                    let inst = s
                        .scope
                        .instance
                        .clone()
                        .filter(|i| !i.is_empty())
                        .unwrap_or_else(|| t.instance_key.clone());
                    recompute
                        .entry(inst)
                        .or_default()
                        .entry(key)
                        .or_default()
                        .insert(actor);
                }
            }
        }
        if let Some(ot) = &t.outcome_truth {
            ot_instances += 1;
            if let Some(st) = &ot.status {
                *status_counts.entry(st.clone()).or_default() += 1;
            }
            if let Some(r) = ot.rounds_to_converge {
                rounds.push(r);
            }
            if let Some(r) = ot.rework_events {
                rework.push(r);
            }
        }
    }

    let mut redundant_recompute: Vec<RedundantRecompute> = recompute
        .into_iter()
        .flat_map(|(inst, keys)| {
            keys.into_iter()
                .filter(|(_, actors)| actors.len() >= 2)
                .map(move |(key, actors)| RedundantRecompute {
                    instance: inst.clone(),
                    key,
                    occurrences: actors.len(),
                    actors: actors.into_iter().collect(),
                })
        })
        .collect();
    // Worst-first, then stable by instance/key.
    redundant_recompute.sort_by(|a, b| {
        b.occurrences
            .cmp(&a.occurrences)
            .then_with(|| a.instance.cmp(&b.instance))
            .then_with(|| a.key.cmp(&b.key))
    });

    let by_role = by_role
        .into_iter()
        .map(|(label, count)| LabelCount {
            label: label.to_string(),
            count,
        })
        .collect();

    let outcome_truth = (ot_instances > 0).then(|| {
        let mean = |v: &[u32]| {
            (!v.is_empty()).then(|| v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64)
        };
        OutcomeTruthRollup {
            instances: ot_instances,
            by_status: status_counts
                .into_iter()
                .map(|(label, count)| LabelCount { label, count })
                .collect(),
            avg_rounds_to_converge: mean(&rounds),
            avg_rework_events: mean(&rework),
        }
    });

    DomainContent {
        signals,
        agent_priors,
        by_role,
        redundant_recompute,
        outcome_truth,
    }
}
#[derive(Default)]
struct ElemAcc {
    count: usize,
    dur_sum: u64,
    dur_n: usize,
    queue_sum: u64,
    queue_n: usize,
    service_sum: u64,
    service_n: usize,
    incidents: u32,
    job_failures: u32,
}

#[derive(Default)]
struct ProcAcc {
    instances: usize,
    completed: usize,
    incident_instances: usize,
    durations: Vec<u64>,
    elements: BTreeMap<String, ElemAcc>,
}

fn fold_processes(details: &[InstanceTrace]) -> Vec<ProcessInsight> {
    let mut by_proc: BTreeMap<String, ProcAcc> = BTreeMap::new();

    for t in details {
        let p = by_proc.entry(t.process_id.clone()).or_default();
        p.instances += 1;
        if t.outcome == "completed" {
            p.completed += 1;
        }
        if !t.incidents.is_empty() {
            p.incident_instances += 1;
        }
        if let Some(d) = t.duration_ms {
            p.durations.push(d);
        }
        for e in &t.elements {
            let acc = p.elements.entry(e.element_id.clone()).or_default();
            acc.count += 1;
            if let Some(d) = e.duration_ms {
                acc.dur_sum += d;
                acc.dur_n += 1;
            }
            acc.incidents += e.incidents;
            if let Some(j) = &e.job {
                acc.job_failures += j.failures;
                if let Some(q) = j.queue_ms {
                    acc.queue_sum += q;
                    acc.queue_n += 1;
                }
                if let Some(s) = j.service_ms {
                    acc.service_sum += s;
                    acc.service_n += 1;
                }
            }
        }
    }

    by_proc
        .into_iter()
        .map(|(process_id, mut p)| {
            let elements: Vec<ElementInsight> = p
                .elements
                .iter()
                .map(|(id, a)| ElementInsight {
                    element_id: id.clone(),
                    count: a.count,
                    avg_duration_ms: mean(a.dur_sum, a.dur_n),
                    avg_queue_ms: mean(a.queue_sum, a.queue_n),
                    avg_service_ms: mean(a.service_sum, a.service_n),
                    incidents: a.incidents,
                    job_failures: a.job_failures,
                })
                .collect();

            // Bottleneck = element with the greatest average self-duration.
            let bottleneck = elements
                .iter()
                .filter(|e| e.avg_duration_ms.is_some())
                .max_by_key(|e| e.avg_duration_ms.unwrap_or(0))
                .cloned();

            p.durations.sort_unstable();
            ProcessInsight {
                process_id,
                instances: p.instances,
                completed: p.completed,
                incident_instances: p.incident_instances,
                avg_duration_ms: mean(p.durations.iter().sum(), p.durations.len()),
                p95_duration_ms: percentile(&p.durations, 95.0),
                p99_duration_ms: percentile(&p.durations, 99.0),
                bottleneck,
                elements,
            }
        })
        .collect()
}

fn fold_incident_clusters(details: &[InstanceTrace]) -> Vec<IncidentCluster> {
    // (element_id, kind) -> (count, reason histogram)
    let mut clusters: BTreeMap<(String, String), (usize, BTreeMap<String, usize>)> =
        BTreeMap::new();
    for t in details {
        for inc in &t.incidents {
            let entry = clusters
                .entry((inc.element_id.clone(), inc.kind.clone()))
                .or_default();
            entry.0 += 1;
            *entry.1.entry(inc.reason.clone()).or_default() += 1;
        }
    }

    let mut out: Vec<IncidentCluster> = clusters
        .into_iter()
        .map(|((element_id, kind), (count, reasons))| {
            let top_reason = reasons
                .into_iter()
                .max_by_key(|(_, n)| *n)
                .map(|(r, _)| r)
                .unwrap_or_default();
            IncidentCluster {
                element_id,
                kind,
                count,
                top_reason,
            }
        })
        .collect();
    out.sort_by_key(|b| std::cmp::Reverse(b.count));
    out
}

fn mean(sum: u64, n: usize) -> Option<u64> {
    if n == 0 {
        None
    } else {
        Some(sum / n as u64)
    }
}

/// Nearest-rank percentile over an already-sorted slice.
fn percentile(sorted: &[u64], pct: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    Some(sorted[idx])
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{DomainSignal, Scope};

    #[test]
    fn percentile_nearest_rank() {
        let v = vec![10u64, 20, 30, 40, 50];
        assert_eq!(percentile(&v, 95.0), Some(50));
        assert_eq!(percentile(&v, 50.0), Some(30));
        assert_eq!(percentile(&[], 95.0), None);
    }

    #[test]
    fn mean_guards_zero() {
        assert_eq!(mean(0, 0), None);
        assert_eq!(mean(100, 4), Some(25));
    }

    #[test]
    fn folds_elements_and_bottleneck() {
        use crate::contracts::{Element, Incident, InstanceTrace, Job};
        let t = InstanceTrace {
            instance_key: "1".into(),
            process_id: "p".into(),
            version: Some(1),
            outcome: "completed".into(),
            duration_ms: Some(100),
            elements: vec![
                Element {
                    element_id: "fast".into(),
                    duration_ms: Some(5),
                    incidents: 0,
                    job: None,
                },
                Element {
                    element_id: "slow".into(),
                    duration_ms: Some(80),
                    incidents: 0,
                    job: Some(Job {
                        job_type: "t".into(),
                        queue_ms: Some(60),
                        service_ms: Some(20),
                        failures: 2,
                    }),
                },
            ],
            incidents: vec![Incident {
                element_id: "slow".into(),
                kind: "JOB_NO_RETRIES".into(),
                reason: "boom".into(),
            }],
            started_at: 0,
            creation_variables: None,
            terminal_variables: None,
            stimuli: None,
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        };
        let procs = fold_processes(std::slice::from_ref(&t));
        assert_eq!(procs.len(), 1);
        let p = &procs[0];
        assert_eq!(p.instances, 1);
        assert_eq!(p.bottleneck.as_ref().unwrap().element_id, "slow");
        assert_eq!(p.bottleneck.as_ref().unwrap().avg_service_ms, Some(20));

        let clusters = fold_incident_clusters(&[t]);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].element_id, "slow");
        assert_eq!(clusters[0].top_reason, "boom");
    }

    fn signal(role: Role, actor: &str, key: &str, provenance: Option<&str>) -> DomainSignal {
        DomainSignal {
            role,
            scope: Scope {
                instance: Some("i1".into()),
                actor: Some(actor.into()),
                ..Default::default()
            },
            dedupe_key: Some(key.into()),
            provenance: provenance.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn fold_domain_signals_counts_by_role_and_flags_agent_priors() {
        let t = InstanceTrace {
            instance_key: "i1".into(),
            domain_signals: vec![
                signal(Role::Knowledge, "a", "k", None),
                signal(Role::Claim, "a", "c", None),
                signal(Role::Defect, "b", "d", Some("agent-retro")),
            ],
            ..Default::default()
        };
        let c = fold_domain_signals(&[t]);
        assert_eq!(c.signals, 3);
        assert_eq!(c.agent_priors, 1);
        let role = |r: &str| c.by_role.iter().find(|x| x.label == r).map(|x| x.count);
        assert_eq!(role("knowledge"), Some(1));
        assert_eq!(role("claim"), Some(1));
        assert_eq!(role("defect"), Some(1));
    }

    #[test]
    fn redundant_recompute_needs_two_sibling_actors_and_ignores_priors() {
        let t = InstanceTrace {
            instance_key: "i1".into(),
            domain_signals: vec![
                // Same key re-derived by two measured sibling actors => a finding.
                signal(Role::Knowledge, "a", "shared", None),
                signal(Role::Knowledge, "b", "shared", None),
                // A single-actor key => not a finding.
                signal(Role::Knowledge, "a", "solo", None),
                // An agent prior for the same key must NOT count as measured evidence.
                signal(Role::Knowledge, "c", "shared", Some("agent-hypothesis")),
            ],
            ..Default::default()
        };
        let c = fold_domain_signals(&[t]);
        assert_eq!(c.redundant_recompute.len(), 1);
        let rr = &c.redundant_recompute[0];
        assert_eq!(rr.key, "shared");
        assert_eq!(rr.occurrences, 2);
        assert_eq!(rr.actors, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn redundant_recompute_ignores_knowledge_without_a_dedupe_key() {
        let mut no_key = signal(Role::Knowledge, "a", "unused", None);
        no_key.dedupe_key = None;
        no_key.body = Some("a very long free-text body that must never become a key".into());
        let mut no_key_sibling = signal(Role::Knowledge, "b", "unused", None);
        no_key_sibling.dedupe_key = None;
        no_key_sibling.body = no_key.body.clone();
        let t = InstanceTrace {
            instance_key: "i1".into(),
            domain_signals: vec![no_key, no_key_sibling],
            ..Default::default()
        };
        let c = fold_domain_signals(&[t]);
        // Two sibling actors, same body, but no dedupe key => not a finding.
        assert!(c.redundant_recompute.is_empty());
    }

    #[test]
    fn redundant_recompute_treats_empty_actor_and_instance_as_absent() {
        // Empty-string actor => no finding, even with matching dedupe keys.
        let mut a = signal(Role::Knowledge, "", "k", None);
        a.scope.instance = Some(String::new());
        let mut b = signal(Role::Knowledge, "", "k", None);
        b.scope.instance = Some(String::new());
        let empty_actor = InstanceTrace {
            instance_key: "i1".into(),
            domain_signals: vec![a, b],
            ..Default::default()
        };
        assert!(fold_domain_signals(&[empty_actor])
            .redundant_recompute
            .is_empty());

        // Empty-string instance falls back to the trace instance_key, not an "" group.
        let mut c1 = signal(Role::Knowledge, "a", "k", None);
        c1.scope.instance = Some(String::new());
        let mut c2 = signal(Role::Knowledge, "b", "k", None);
        c2.scope.instance = Some(String::new());
        let empty_inst = InstanceTrace {
            instance_key: "fallback".into(),
            domain_signals: vec![c1, c2],
            ..Default::default()
        };
        let rr = fold_domain_signals(&[empty_inst]).redundant_recompute;
        assert_eq!(rr.len(), 1);
        assert_eq!(rr[0].instance, "fallback");
    }

    #[test]
    fn outcome_truth_rollup_averages_present_fields() {
        use crate::contracts::OutcomeTruth;
        let mk = |status: &str, rounds: Option<u32>, rework: Option<u32>| InstanceTrace {
            outcome_truth: Some(OutcomeTruth {
                status: Some(status.into()),
                rounds_to_converge: rounds,
                rework_events: rework,
                ..Default::default()
            }),
            ..Default::default()
        };
        let c = fold_domain_signals(&[
            mk("merged", Some(2), Some(1)),
            mk("merged", Some(4), None),
            mk("escalated", None, Some(3)),
        ]);
        let ot = c.outcome_truth.unwrap();
        assert_eq!(ot.instances, 3);
        assert_eq!(
            ot.by_status
                .iter()
                .find(|x| x.label == "merged")
                .unwrap()
                .count,
            2
        );
        assert_eq!(ot.avg_rounds_to_converge, Some(3.0));
        assert_eq!(ot.avg_rework_events, Some(2.0));
    }

    #[test]
    fn content_absent_is_empty_and_safe() {
        let c = fold_domain_signals(&[InstanceTrace::default()]);
        assert_eq!(c.signals, 0);
        assert!(c.by_role.is_empty());
        assert!(c.redundant_recompute.is_empty());
        assert!(c.outcome_truth.is_none());
    }
}
