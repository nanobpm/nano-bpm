//! M3 — the **at-scale measurement** half of the ClusterRunner.
//!
//! The ClusterRunner has two halves: (a) *drive* concurrent load against a real
//! Nano gateway (producer over the v2 REST API + workers over `/falcon`,
//! reusing the perf-matrix load path), and (b) *measure* the at-scale result. This
//! module is (b): given the live traces a real run produced (read over the T1
//! contract), it computes the signals the per-instance, sequential [`super::sim`]
//! **structurally cannot** — **throughput**, **tail latency (p50/p95/p99)**, and the
//! **queue-vs-service split under contention** — which the latent-exploration
//! analysis (`docs/processos-latent-process-exploration.md` §2) flags as exactly
//! the data means alone hide.
//!
//! It is pure (no I/O): point it at the traces from any loaded cluster (e.g. one
//! the perf-matrix is driving) to get the at-scale view. Driving the load itself
//! is the documented integration boundary; the science lives here and is tested.

use serde::Serialize;

use crate::contracts::{InstanceTrace, NanoClient, Topology, TraceSummary};

/// The at-scale result of a cluster run for one process: throughput + latency
/// distribution + the queue/service decomposition under load. This is the
/// ClusterRunner's analogue of [`super::sim::InstanceRun`], but *aggregate and
/// contention-aware* rather than per-instance and isolated.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRunSummary {
    pub process_id: String,
    /// Instances observed in the window.
    pub instances: usize,
    pub completed: usize,
    pub terminated: usize,
    /// Instances still running at read time.
    pub active: usize,
    /// Instances carrying at least one incident.
    pub with_incidents: usize,
    /// Wall-clock span from the earliest start to the latest end, in ms. `None`
    /// when fewer than one ended instance pins the window.
    pub window_ms: Option<u64>,
    /// Completed instances per second across the window — the throughput signal
    /// the isolated SimRunner cannot produce. `None` when the window is undefined
    /// or zero-length.
    pub throughput_per_sec: Option<f64>,
    /// End-to-end duration percentiles over instances that reported a duration.
    pub e2e_p50_ms: Option<u64>,
    pub e2e_p95_ms: Option<u64>,
    pub e2e_p99_ms: Option<u64>,
    /// Mean broker-wait (queue) and worker (service) ms across sampled jobs — the
    /// decomposition that tells "fixable by scaling" from "needs a structural
    /// change". Populated only when trace *details* are supplied.
    pub avg_queue_ms: Option<u64>,
    pub avg_service_ms: Option<u64>,
    /// Per-job-type resource breakdown — the granularity the worker-count question
    /// ("how many workers does *this job type* need to hold p99 < X?") and the
    /// regime-(b) queueing simulator are fitted from (see
    /// `processos-deployment-cooptimization.md` §4.2). Empty when no details.
    pub by_job_type: Vec<JobTypeStat>,
    /// Per-node distribution of the run across the cluster — the **distributed
    /// sensing** signal. Trace data is partition-local, so each node contributes a
    /// slice; uneven `instances`/`active_instances` here is the cross-node backlog
    /// skew (the fairness signal) that a cluster-wide throughput number alone hides.
    /// Populated only by the cluster-aware reader; empty for a pure summarize.
    pub by_node: Vec<NodeStat>,
}

/// One cluster node's contribution to the run plus its live load gauge. `instances`
/// is how many of this run's traces that node served (its partitions' share);
/// `active_instances` is the node's *current* in-flight backlog. A node with a high
/// `active_instances` while others sit near zero is being starved/over-loaded — the
/// imbalance distributed scaling must sense and correct.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStat {
    pub endpoint: String,
    /// Traces of the target process this node served (partition-local share).
    pub instances: usize,
    /// Live in-flight instances on this node at read time (`/console/api/metrics`).
    pub active_instances: Option<i64>,
    /// Cumulative completions this node has recorded (`/console/api/metrics`).
    pub completions_total: Option<u64>,
}

/// One job type's contention profile across the sampled run: how many jobs ran,
/// and the queue (broker-wait) vs service (worker) split. A high `avg_queue_ms`
/// with low `avg_service_ms` points at *too few workers* (scale); the reverse
/// points at a *slow worker* (bind/substitute) — the lever the split discriminates.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTypeStat {
    pub job_type: String,
    /// Number of sampled job occurrences of this type.
    pub samples: usize,
    pub avg_queue_ms: Option<u64>,
    pub avg_service_ms: Option<u64>,
    /// Total job failures observed for this type.
    pub failures: u32,
}

/// Summarize a cluster run for `process_id` from its live traces. `summaries`
/// (cheap list rows) drive throughput + the e2e distribution; `details` (one
/// request each, optional) add the queue/service split. Pure and deterministic.
pub fn summarize_run(
    process_id: &str,
    summaries: &[TraceSummary],
    details: &[InstanceTrace],
) -> ClusterRunSummary {
    let mine: Vec<&TraceSummary> = summaries
        .iter()
        .filter(|s| s.process_id == process_id)
        .collect();

    let mut completed = 0usize;
    let mut terminated = 0usize;
    let mut active = 0usize;
    let mut with_incidents = 0usize;
    let mut durations: Vec<u64> = Vec::new();
    let mut min_start: Option<u64> = None;
    let mut max_end: Option<u64> = None;

    for s in &mine {
        match s.outcome.as_str() {
            "completed" => completed += 1,
            "terminated" => terminated += 1,
            _ => active += 1,
        }
        if s.incident_count > 0 {
            with_incidents += 1;
        }
        if let Some(d) = s.duration_ms {
            durations.push(d);
        }
        min_start = Some(min_start.map_or(s.started_at, |m| m.min(s.started_at)));
        if let Some(e) = s.ended_at {
            max_end = Some(max_end.map_or(e, |m| m.max(e)));
        }
    }

    durations.sort_unstable();

    // Throughput: completed instances over the wall-clock window they spanned.
    let window_ms = match (min_start, max_end) {
        (Some(a), Some(b)) if b > a => Some(b - a),
        _ => None,
    };
    let throughput_per_sec = window_ms
        .filter(|w| *w > 0)
        .map(|w| completed as f64 / (w as f64 / 1000.0));

    // Queue/service split from details (optional, more expensive to fetch), kept
    // both in aggregate and broken down per job type.
    let mut queue_sum = 0u64;
    let mut queue_n = 0usize;
    let mut service_sum = 0u64;
    let mut service_n = 0usize;
    // job_type -> (queue_sum, queue_n, service_sum, service_n, samples, failures)
    let mut by_type: std::collections::BTreeMap<String, JobTypeAcc> =
        std::collections::BTreeMap::new();
    for t in details.iter().filter(|t| t.process_id == process_id) {
        for e in &t.elements {
            if let Some(j) = &e.job {
                let acc = by_type.entry(j.job_type.clone()).or_default();
                acc.samples += 1;
                acc.failures += j.failures;
                if let Some(q) = j.queue_ms {
                    queue_sum += q;
                    queue_n += 1;
                    acc.queue_sum += q;
                    acc.queue_n += 1;
                }
                if let Some(sv) = j.service_ms {
                    service_sum += sv;
                    service_n += 1;
                    acc.service_sum += sv;
                    acc.service_n += 1;
                }
            }
        }
    }

    let by_job_type: Vec<JobTypeStat> = by_type
        .into_iter()
        .map(|(job_type, a)| JobTypeStat {
            job_type,
            samples: a.samples,
            avg_queue_ms: mean(a.queue_sum, a.queue_n),
            avg_service_ms: mean(a.service_sum, a.service_n),
            failures: a.failures,
        })
        .collect();

    ClusterRunSummary {
        process_id: process_id.to_string(),
        instances: mine.len(),
        completed,
        terminated,
        active,
        with_incidents,
        window_ms,
        throughput_per_sec,
        e2e_p50_ms: percentile(&durations, 50.0),
        e2e_p95_ms: percentile(&durations, 95.0),
        e2e_p99_ms: percentile(&durations, 99.0),
        avg_queue_ms: mean(queue_sum, queue_n),
        avg_service_ms: mean(service_sum, service_n),
        by_job_type,
        by_node: Vec::new(),
    }
}

/// Per-job-type running aggregate while folding trace details.
#[derive(Default)]
struct JobTypeAcc {
    samples: usize,
    failures: u32,
    queue_sum: u64,
    queue_n: usize,
    service_sum: u64,
    service_n: usize,
}

/// Read the live dataset from the **whole cluster** and summarize the at-scale run
/// for `process_id` (or the busiest sampled process when `None`). Trace data is
/// partition-local, so this discovers every node via `/v2/topology` and unions
/// their traces before aggregating; on a topology-read failure it falls back to the
/// single `nano` endpoint. Network I/O is confined here; the aggregation is the pure
/// [`summarize_run`]. Errors only on a read-contract failure or when no live traces
/// exist.
pub async fn build_cluster_summary(
    nano: &NanoClient,
    process_id: Option<&str>,
    limit: usize,
    sample: usize,
) -> Result<ClusterRunSummary, String> {
    let endpoints = match nano.topology().await {
        Ok(topo) => cluster_endpoints(nano.base_url(), &topo),
        // A single-node deployment (or no topology) is still measurable on its own.
        Err(_) => vec![nano.base_url().to_string()],
    };
    let clients: Vec<NanoClient> = endpoints.into_iter().map(NanoClient::new).collect();
    build_cluster_summary_over(&clients, process_id, limit, sample).await
}

/// The cluster-aware core: list traces from every node (deduping by instance key
/// since each instance lives on exactly one partition), pick the target process,
/// then pull a bounded detail sample **from each instance's owning node** (trace
/// detail is only served by the node that owns it). Pure aggregation is delegated to
/// [`summarize_run`]. Factored out so a fixed endpoint set is unit-testable and so a
/// single-node read is just the one-client case.
pub async fn build_cluster_summary_over(
    clients: &[NanoClient],
    process_id: Option<&str>,
    limit: usize,
    sample: usize,
) -> Result<ClusterRunSummary, String> {
    let mut summaries: Vec<TraceSummary> = Vec::new();
    let mut owner: Vec<usize> = Vec::new();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (i, c) in clients.iter().enumerate() {
        for s in c.list_traces(limit).await? {
            if seen.insert(s.instance_key.clone()) {
                owner.push(i);
                summaries.push(s);
            }
        }
    }
    if summaries.is_empty() {
        return Err(
            "no live traces to summarize; load the cluster (e.g. via the perf-matrix) first"
                .to_string(),
        );
    }

    let target = match process_id {
        Some(id) => id.to_string(),
        None => busiest_process(&summaries)
            .ok_or_else(|| "no process could be selected from live traces".to_string())?,
    };
    if !summaries.iter().any(|s| s.process_id == target) {
        return Err(format!("process id '{target}' has no sampled live traces"));
    }

    // Pull a bounded sample of details (for the queue/service split), each from the
    // node that owns the instance.
    let mut details: Vec<InstanceTrace> = Vec::new();
    for (idx, s) in summaries.iter().enumerate() {
        if details.len() >= sample {
            break;
        }
        if s.process_id != target {
            continue;
        }
        if let Ok(t) = clients[owner[idx]].trace(&s.instance_key).await {
            details.push(t);
        }
    }

    let mut summary = summarize_run(&target, &summaries, &details);

    // Per-node distribution (the distributed-sensing signal): each node's share of
    // the target process plus its live backlog gauge. A node's live metrics failing
    // is non-fatal — it just leaves the gauges `None`.
    let mut by_node: Vec<NodeStat> = Vec::with_capacity(clients.len());
    for (i, c) in clients.iter().enumerate() {
        let instances = summaries
            .iter()
            .enumerate()
            .filter(|(idx, s)| owner[*idx] == i && s.process_id == target)
            .count();
        let m = c.metrics().await.ok();
        by_node.push(NodeStat {
            endpoint: c.base_url().to_string(),
            instances,
            active_instances: m.as_ref().map(|m| m.active_instances),
            completions_total: m.as_ref().map(|m| m.completions_total),
        });
    }
    summary.by_node = by_node;

    Ok(summary)
}

/// Derive one console base URL per cluster node from a topology response. Trace data
/// is partition-local, so the measurement must read every node. The base node's
/// scheme+host are reused for every endpoint (a broker's advertised `host` can be a
/// non-dialable bind address such as `0.0.0.0`); only the per-node `port` is taken
/// from topology. Ports are deduped and sorted for a stable endpoint set. An empty
/// topology degrades to the single base URL.
pub fn cluster_endpoints(base_url: &str, topo: &Topology) -> Vec<String> {
    if topo.brokers.is_empty() {
        return vec![base_url.trim_end_matches('/').to_string()];
    }
    let (scheme, host) = base_scheme_host(base_url);
    let mut ports: Vec<u16> = topo.brokers.iter().map(|b| b.port).collect();
    ports.sort_unstable();
    ports.dedup();
    ports
        .into_iter()
        .map(|p| format!("{scheme}://{host}:{p}"))
        .collect()
}

/// Split a base URL into its scheme and host, dropping any port and path. Defaults to
/// `http` and treats the whole string as the host when no scheme is present.
fn base_scheme_host(base_url: &str) -> (String, String) {
    let (scheme, rest) = base_url.split_once("://").unwrap_or(("http", base_url));
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = authority
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(authority);
    (scheme.to_string(), host.to_string())
}

/// The process id with the most rows in `summaries`.
fn busiest_process(summaries: &[TraceSummary]) -> Option<String> {
    let mut counts: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for s in summaries {
        *counts.entry(s.process_id.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .map(|(p, _)| p.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(
        key: &str,
        proc: &str,
        outcome: &str,
        start: u64,
        end: u64,
        dur: u64,
    ) -> TraceSummary {
        TraceSummary {
            instance_key: key.into(),
            process_id: proc.into(),
            version: Some(1),
            business_id: None,
            outcome: outcome.into(),
            started_at: start,
            ended_at: Some(end),
            duration_ms: Some(dur),
            element_count: 2,
            incident_count: 0,
        }
    }

    #[test]
    fn throughput_and_tail_over_the_window() {
        // 4 completed `order` instances spanning [1000, 3000] => 2s window => 2/s.
        let summaries = vec![
            summary("1", "order", "completed", 1000, 1500, 500),
            summary("2", "order", "completed", 1200, 1900, 700),
            summary("3", "order", "completed", 2000, 2600, 600),
            summary("4", "order", "completed", 2500, 3000, 900),
            // a different process is ignored
            summary("9", "other", "completed", 1000, 5000, 4000),
        ];
        let s = summarize_run("order", &summaries, &[]);
        assert_eq!(s.instances, 4);
        assert_eq!(s.completed, 4);
        assert_eq!(s.window_ms, Some(2000));
        assert_eq!(s.throughput_per_sec, Some(2.0));
        // durations sorted: 500,600,700,900 ; nearest-rank p95/p99 => 900.
        assert_eq!(s.e2e_p50_ms, Some(600));
        assert_eq!(s.e2e_p99_ms, Some(900));
    }

    #[test]
    fn counts_outcomes_and_incidents() {
        let mut a = summary("1", "p", "completed", 0, 100, 100);
        a.incident_count = 1;
        let summaries = vec![a, summary("2", "p", "terminated", 0, 50, 50), {
            let mut act = summary("3", "p", "active", 10, 10, 0);
            act.ended_at = None;
            act.duration_ms = None;
            act
        }];
        let s = summarize_run("p", &summaries, &[]);
        assert_eq!(s.completed, 1);
        assert_eq!(s.terminated, 1);
        assert_eq!(s.active, 1);
        assert_eq!(s.with_incidents, 1);
    }

    #[test]
    fn queue_service_split_from_details() {
        use crate::contracts::{Element, InstanceTrace, Job};
        let detail = InstanceTrace {
            instance_key: "1".into(),
            process_id: "order".into(),
            version: Some(1),
            outcome: "completed".into(),
            duration_ms: Some(100),
            elements: vec![Element {
                element_id: "charge".into(),
                duration_ms: Some(80),
                incidents: 0,
                job: Some(Job {
                    job_type: "payment".into(),
                    queue_ms: Some(60),
                    service_ms: Some(20),
                    failures: 0,
                }),
            }],
            incidents: vec![],
            started_at: 0,
            creation_variables: None,
            terminal_variables: None,
            stimuli: None,
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        };
        let summaries = vec![summary("1", "order", "completed", 0, 100, 100)];
        let s = summarize_run("order", &summaries, &[detail]);
        assert_eq!(s.avg_queue_ms, Some(60));
        assert_eq!(s.avg_service_ms, Some(20));
        // The same split appears per job type.
        assert_eq!(s.by_job_type.len(), 1);
        let jt = &s.by_job_type[0];
        assert_eq!(jt.job_type, "payment");
        assert_eq!(jt.samples, 1);
        assert_eq!(jt.avg_queue_ms, Some(60));
        assert_eq!(jt.avg_service_ms, Some(20));
    }

    #[test]
    fn per_job_type_breakdown_discriminates_the_lever() {
        use crate::contracts::{Element, InstanceTrace, Job};
        // `classify` is queue-bound (60 queue / 5 service => scale workers);
        // `summarize` is service-bound (5 queue / 90 service => slow worker).
        let job = |ty: &str, q: u64, sv: u64, f: u32| Element {
            element_id: ty.into(),
            duration_ms: Some(q + sv),
            incidents: 0,
            job: Some(Job {
                job_type: ty.into(),
                queue_ms: Some(q),
                service_ms: Some(sv),
                failures: f,
            }),
        };
        let detail = InstanceTrace {
            instance_key: "1".into(),
            process_id: "order".into(),
            version: Some(1),
            outcome: "completed".into(),
            duration_ms: Some(160),
            elements: vec![job("classify", 60, 5, 0), job("summarize", 5, 90, 2)],
            incidents: vec![],
            started_at: 0,
            creation_variables: None,
            terminal_variables: None,
            stimuli: None,
            stimuli_truncated: false,
            domain_signals: vec![],
            outcome_truth: None,
        };
        let summaries = vec![summary("1", "order", "completed", 0, 160, 160)];
        let s = summarize_run("order", &summaries, &[detail]);
        let by: std::collections::BTreeMap<_, _> = s
            .by_job_type
            .iter()
            .map(|j| (j.job_type.as_str(), j))
            .collect();
        let classify = by["classify"];
        let summarize = by["summarize"];
        // queue-bound: queue >> service
        assert!(classify.avg_queue_ms.unwrap() > classify.avg_service_ms.unwrap());
        // service-bound: service >> queue, and its failures are attributed here
        assert!(summarize.avg_service_ms.unwrap() > summarize.avg_queue_ms.unwrap());
        assert_eq!(summarize.failures, 2);
    }

    #[test]
    fn undefined_window_yields_no_throughput() {
        // All instances share a single instant => zero-length window.
        let summaries = vec![summary("1", "p", "completed", 5, 5, 0)];
        let s = summarize_run("p", &summaries, &[]);
        assert_eq!(s.window_ms, None);
        assert_eq!(s.throughput_per_sec, None);
    }

    #[test]
    fn endpoints_reuse_base_host_and_topology_ports() {
        use crate::contracts::{Broker, Topology};
        let topo = Topology {
            brokers: vec![
                // node 0 advertises a non-dialable bind address; we must NOT use it.
                Broker {
                    node_id: 0,
                    host: "0.0.0.0".into(),
                    port: 8080,
                },
                Broker {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 8081,
                },
                Broker {
                    node_id: 2,
                    host: "127.0.0.1".into(),
                    port: 8082,
                },
            ],
        };
        let eps = cluster_endpoints("http://127.0.0.1:8080", &topo);
        assert_eq!(
            eps,
            vec![
                "http://127.0.0.1:8080".to_string(),
                "http://127.0.0.1:8081".to_string(),
                "http://127.0.0.1:8082".to_string(),
            ]
        );
    }

    #[test]
    fn empty_topology_degrades_to_single_base() {
        use crate::contracts::Topology;
        let eps = cluster_endpoints("http://host:8080/", &Topology { brokers: vec![] });
        assert_eq!(eps, vec!["http://host:8080".to_string()]);
    }
}
