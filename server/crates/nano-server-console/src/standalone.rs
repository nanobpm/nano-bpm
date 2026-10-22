//! Standalone off-cluster console (ADR 0035 §B).
//!
//! Runs the console dashboard *without* an engine. Instead of reading a local
//! `ServerImpl`, it scrapes a configured set of gateway peers over HTTP — their
//! always-on `GET /v2/topology` (cluster membership) and full-fidelity
//! `GET /metrics` (per-node metrics, promoted in ADR 0035 §A) — and serves the
//! **observability subset** of the console API (topology, cluster metrics,
//! cluster health) plus the static console SPA. Engine-only endpoints
//! (instances, traces, workers, projects, models, …) return `503` because a
//! pure remote-scrape aggregator cannot serve engine-internal state.
//!
//! This makes "standalone console" the runtime sibling of the build-time
//! *observe* profile, sourced from remote scrapes rather than a local engine.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use super::{
    AggregateMetricsDto, ClusterHealthDto, ClusterMetricsDto, NodeDto, NodeHealthDto,
    NodeMetricsDto, PartitionDto, TopologyDto,
};

// ---------------------------------------------------------------------------
// Wire model: a peer's `GET /v2/topology` JSON (Camunda topology shape).
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct WireTopology {
    #[serde(default)]
    brokers: Vec<WireBroker>,
    #[serde(default)]
    partitions_count: u64,
    #[serde(default)]
    replication_factor: u32,
    #[serde(default)]
    gateway_version: String,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct WireBroker {
    node_id: u32,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: i32,
    #[serde(default)]
    partitions: Vec<WirePartition>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct WirePartition {
    partition_id: u64,
    #[serde(default)]
    role: String,
}

/// A broker host is unroutable-as-advertised when the node reports itself; the
/// gateway advertises `0.0.0.0` (or an unspecified address) for its own entry in
/// `/v2/topology`. We substitute the URL we actually reached it on.
fn is_self_host(host: &str) -> bool {
    matches!(
        host.trim(),
        "" | "0.0.0.0" | "::" | "[::]" | "0:0:0:0:0:0:0:0"
    )
}

/// Maps a wire topology (as returned by `queried_url`) to each node's reachable
/// base URL. The broker that advertises itself as `0.0.0.0` is the queried node,
/// so it is mapped back to `queried_url`; peers use their advertised `host:port`.
/// Returns `(node_id, base_url)` pairs in broker order.
fn node_urls_from_topology(wire: &WireTopology, queried_url: &str) -> Vec<(u32, String)> {
    let queried = queried_url.trim_end_matches('/');
    let mut self_used = false;
    wire.brokers
        .iter()
        .map(|b| {
            let url = if is_self_host(&b.host) && !self_used {
                self_used = true;
                queried.to_string()
            } else {
                format!("http://{}:{}", b.host, b.port)
            };
            (b.node_id, url)
        })
        .collect()
}

/// Builds the console [`TopologyDto`] from a peer's wire topology. Leadership per
/// partition is read from the brokers' `role` fields; `owner` mirrors the leader
/// (the engine's owner==preferred-leader), and a partition with no visible leader
/// is marked `recovering`. `node_id` is set to a sentinel (`u32::MAX`) because a
/// standalone console is not itself a cluster node, so no node is "self".
fn topology_dto_from_wire(wire: &WireTopology, node_urls: &[(u32, String)]) -> TopologyDto {
    let nodes: Vec<NodeDto> = node_urls
        .iter()
        .map(|(id, url)| NodeDto {
            node_id: *id,
            address: url.clone(),
            is_self: false,
        })
        .collect();

    let num_partitions = wire.partitions_count.max(
        wire.brokers
            .iter()
            .flat_map(|b| b.partitions.iter())
            .map(|p| p.partition_id)
            .max()
            .unwrap_or(0),
    );

    let partitions: Vec<PartitionDto> = (1..=num_partitions)
        .map(|pid| {
            let mut replicas: Vec<u32> = Vec::new();
            let mut leader: Option<u32> = None;
            for b in &wire.brokers {
                if let Some(part) = b.partitions.iter().find(|p| p.partition_id == pid) {
                    replicas.push(b.node_id);
                    if part.role.eq_ignore_ascii_case("leader") {
                        leader = Some(b.node_id);
                    }
                }
            }
            let owner = leader.or_else(|| replicas.first().copied()).unwrap_or(0);
            PartitionDto {
                partition_id: pid,
                replicas,
                owner,
                leader,
                raft_term: None,
                // No visible leader among the reachable brokers => a failover /
                // recovery is in progress for this partition.
                recovering: leader.is_none(),
            }
        })
        .collect();

    TopologyDto {
        // Sentinel: a standalone console is not a cluster node, so nothing is self.
        node_id: u32::MAX,
        num_nodes: nodes.len() as u32,
        num_partitions,
        replication_factor: wire.replication_factor,
        raft_enabled: wire.replication_factor > 1,
        gateway_version: wire.gateway_version.clone(),
        nodes,
        partitions,
    }
}

/// Sums a set of per-node metrics into the cluster [`AggregateMetricsDto`],
/// mirroring `super::cluster_metrics`' aggregation so the standalone and
/// embedded consoles report an identical shape.
fn aggregate(nodes: &[NodeMetricsDto], total_nodes: u32) -> AggregateMetricsDto {
    let mut agg = AggregateMetricsDto {
        total_nodes,
        ..Default::default()
    };
    for n in nodes {
        if let Some(m) = &n.metrics {
            agg.reachable_nodes += 1;
            agg.active_instances += m.active_instances;
            agg.creates_total += m.creates_total;
            agg.completions_total += m.completions_total;
            agg.connections_active += m.connections_active;
            agg.commit_inflight += m.commit_inflight;
            agg.resident_bytes += m.resident_bytes.unwrap_or(0);
        }
    }
    agg
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// RemoteCluster: the engine-less data source backing the observability routes.
// ---------------------------------------------------------------------------

/// The configured set of peers a standalone console scrapes. Peer discovery is
/// seed-based: any reachable peer's `/v2/topology` yields the whole cluster
/// membership, which then drives per-node `/metrics` scrapes.
pub struct RemoteCluster {
    seeds: Vec<String>,
}

impl RemoteCluster {
    pub fn new(seeds: Vec<String>) -> Self {
        Self {
            seeds: seeds
                .into_iter()
                .map(|s| s.trim_end_matches('/').to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        }
    }

    /// Fetches `/v2/topology` from the first reachable seed, returning the parsed
    /// wire topology and the URL it was reached on.
    async fn fetch_topology(&self) -> Option<(WireTopology, String)> {
        for seed in &self.seeds {
            if let Ok((status, body)) = super::peer_http_get(seed, "/v2/topology").await
                && status.is_success()
                && let Ok(wire) = serde_json::from_slice::<WireTopology>(&body)
            {
                return Some((wire, seed.clone()));
            }
        }
        None
    }

    /// Resolves the current node set `(node_id, base_url)`. Prefers the live
    /// cluster membership from a reachable seed's `/v2/topology`; falls back to
    /// the raw seed list (indexed) when no seed answers.
    async fn resolve_nodes(&self) -> Vec<(u32, String)> {
        match self.fetch_topology().await {
            Some((wire, url)) => node_urls_from_topology(&wire, &url),
            None => self
                .seeds
                .iter()
                .enumerate()
                .map(|(i, u)| (i as u32, u.clone()))
                .collect(),
        }
    }

    /// `GET /console/api/topology` — reconstructs the console topology view from
    /// a reachable seed. Degrades to a node-only view when no seed answers.
    async fn topology(&self) -> TopologyDto {
        match self.fetch_topology().await {
            Some((wire, url)) => {
                let node_urls = node_urls_from_topology(&wire, &url);
                topology_dto_from_wire(&wire, &node_urls)
            }
            None => TopologyDto {
                node_id: u32::MAX,
                num_nodes: self.seeds.len() as u32,
                num_partitions: 0,
                replication_factor: 0,
                raft_enabled: false,
                gateway_version: String::new(),
                nodes: self
                    .seeds
                    .iter()
                    .enumerate()
                    .map(|(i, u)| NodeDto {
                        node_id: i as u32,
                        address: u.clone(),
                        is_self: false,
                    })
                    .collect(),
                partitions: Vec::new(),
            },
        }
    }

    /// `GET /console/api/cluster/metrics` — scrapes every resolved node's metrics
    /// concurrently and returns the per-node breakdown plus a reachable aggregate.
    async fn cluster_metrics(&self) -> ClusterMetricsDto {
        let node_urls = self.resolve_nodes().await;
        let total = node_urls.len() as u32;

        let probes = node_urls.into_iter().map(|(id, url)| async move {
            match super::probe_peer_metrics(&url).await {
                Ok(metrics) => NodeMetricsDto {
                    node_id: id,
                    address: url,
                    is_self: false,
                    reachable: true,
                    error: None,
                    metrics: Some(metrics),
                },
                Err(err) => NodeMetricsDto {
                    node_id: id,
                    address: url,
                    is_self: false,
                    reachable: false,
                    error: Some(err),
                    metrics: None,
                },
            }
        });
        let nodes = futures_util::future::join_all(probes).await;
        let aggregate = aggregate(&nodes, total);

        ClusterMetricsDto {
            checked_at_ms: now_ms(),
            nodes,
            aggregate,
        }
    }

    /// `GET /console/api/cluster/health` — probes each resolved node's
    /// `/v2/topology` for live reachability, version, and latency.
    async fn cluster_health(&self) -> ClusterHealthDto {
        let node_urls = self.resolve_nodes().await;

        let probes = node_urls.into_iter().map(|(id, url)| async move {
            match super::probe_peer(&url).await {
                Ok((version, latency)) => NodeHealthDto {
                    node_id: id,
                    address: url,
                    is_self: false,
                    reachable: true,
                    version,
                    latency_ms: Some(latency.as_millis() as u64),
                    error: None,
                },
                Err(err) => NodeHealthDto {
                    node_id: id,
                    address: url,
                    is_self: false,
                    reachable: false,
                    version: None,
                    latency_ms: None,
                    error: Some(err),
                },
            }
        });
        let nodes = futures_util::future::join_all(probes).await;

        ClusterHealthDto {
            checked_at_ms: now_ms(),
            nodes,
        }
    }
}

// ---------------------------------------------------------------------------
// Router + run mode.
// ---------------------------------------------------------------------------

/// JSON 503 for engine-only endpoints not available off-cluster.
async fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": "unavailable in standalone console mode",
            "detail": "This console runs off-cluster from remote /metrics scrapes and \
                       cannot serve engine-internal state. Only the observability views \
                       (topology, metrics, health) are available.",
        })),
    )
        .into_response()
}

/// Serves a console SPA asset (reusing the embedded-asset machinery).
/// Builds the standalone console router: the SPA + landing pages, the three
/// remote-backed observability endpoints, and `503` stubs for the engine-only
/// endpoints the SPA may call.
pub fn router(cluster: Arc<RemoteCluster>) -> Router {
    let topo = cluster.clone();
    let metrics = cluster.clone();
    let health = cluster;

    Router::new()
        // Static surfaces (server-less handlers reused from the parent module).
        .route("/", get(super::landing))
        .route("/console", get(super::spa_index))
        .route("/console/", get(super::spa_index))
        .route("/console/{*path}", get(super::spa_asset))
        // Observability API (remote-scrape backed).
        .route(
            "/console/api/topology",
            get(move || {
                let c = topo.clone();
                async move { Json(c.topology().await).into_response() }
            }),
        )
        .route(
            "/console/api/cluster/metrics",
            get(move || {
                let c = metrics.clone();
                async move { Json(c.cluster_metrics().await).into_response() }
            }),
        )
        .route(
            "/console/api/cluster/health",
            get(move || {
                let c = health.clone();
                async move { Json(c.cluster_health().await).into_response() }
            }),
        )
        // Engine-only endpoints the SPA may probe: answer 503 rather than 404 so
        // the UI can distinguish "off-cluster" from "not found".
        .route("/console/api/metrics", get(unavailable))
        .route("/console/api/instances", get(unavailable))
        .route("/console/api/traces", get(unavailable))
        .route("/console/api/workers", get(unavailable))
        .route("/console/api/models", get(unavailable))
        .route("/console/api/projects", get(unavailable))
        .route("/console/api/extensions", get(unavailable))
        .route("/console/api/config/server", get(unavailable))
}

/// Entry point for standalone-console mode. Binds `PORT` and serves the router;
/// never starts the engine, journal, or Raft. Returns when the server stops.
pub async fn run(peers: Vec<String>) {
    let cluster = Arc::new(RemoteCluster::new(peers));
    let peer_list = cluster.seeds.join(", ");
    let app = router(cluster);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));
    let local_port = listener.local_addr().map(|a| a.port()).unwrap_or(port);
    println!("LISTENING_PORT={local_port}");
    let _ = std::io::Write::flush(&mut std::io::stdout());

    tracing::info!(
        peers = %peer_list,
        "standalone console mode: engine disabled; scraping remote peers"
    );
    let base = format!("http://127.0.0.1:{local_port}");
    println!("\nNano BPM standalone console is up (observability only):");
    println!("  Web console    {base}/console");
    println!("  Scraping peers {peer_list}");
    println!();
    let _ = std::io::Write::flush(&mut std::io::stdout());

    #[allow(
        clippy::expect_used,
        reason = "standalone-console entry point: serving the bound listener is the \
                  process's terminal task, so an unexpected serve error is fatal"
    )]
    {
        axum::serve(listener, app).await.expect("server error");
    }
}

#[cfg(test)]
mod tests {
    use super::super::{MetricsDto, RecoveryDto};
    use super::*;

    fn wire(json: &str) -> WireTopology {
        serde_json::from_str(json).expect("wire topology parses")
    }

    const TOPO: &str = r#"{
        "brokers": [
            {"nodeId": 0, "host": "0.0.0.0", "port": 8080,
             "partitions": [{"partitionId": 1, "role": "leader"},
                            {"partitionId": 2, "role": "follower"}]},
            {"nodeId": 1, "host": "10.0.0.11", "port": 8080,
             "partitions": [{"partitionId": 1, "role": "follower"},
                            {"partitionId": 2, "role": "leader"}]}
        ],
        "clusterSize": 2,
        "partitionsCount": 2,
        "replicationFactor": 2,
        "gatewayVersion": "9.9.9"
    }"#;

    #[test]
    fn self_broker_maps_to_queried_url() {
        let w = wire(TOPO);
        let urls = node_urls_from_topology(&w, "http://seed:8080/");
        assert_eq!(
            urls[0],
            (0, "http://seed:8080".to_string()),
            "self => queried url"
        );
        assert_eq!(
            urls[1],
            (1, "http://10.0.0.11:8080".to_string()),
            "peer => advertised"
        );
    }

    #[test]
    fn topology_dto_reconstructs_leadership() {
        let w = wire(TOPO);
        let urls = node_urls_from_topology(&w, "http://seed:8080");
        let dto = topology_dto_from_wire(&w, &urls);

        assert_eq!(dto.num_nodes, 2);
        assert_eq!(dto.num_partitions, 2);
        assert_eq!(dto.replication_factor, 2);
        assert!(dto.raft_enabled);
        assert_eq!(dto.gateway_version, "9.9.9");
        assert_eq!(dto.node_id, u32::MAX, "no node is self in standalone");

        // Partition 1 led by node 0, partition 2 by node 1.
        let p1 = dto.partitions.iter().find(|p| p.partition_id == 1).unwrap();
        assert_eq!(p1.leader, Some(0));
        assert_eq!(p1.owner, 0);
        assert!(!p1.recovering);
        assert_eq!(p1.replicas, vec![0, 1]);

        let p2 = dto.partitions.iter().find(|p| p.partition_id == 2).unwrap();
        assert_eq!(p2.leader, Some(1));
    }

    #[test]
    fn partition_without_leader_is_recovering() {
        let w = wire(
            r#"{"brokers":[{"nodeId":0,"host":"0.0.0.0","port":8080,
                 "partitions":[{"partitionId":1,"role":"follower"}]}],
                "partitionsCount":1,"replicationFactor":1,"gatewayVersion":"1.0"}"#,
        );
        let urls = node_urls_from_topology(&w, "http://s:8080");
        let dto = topology_dto_from_wire(&w, &urls);
        let p1 = &dto.partitions[0];
        assert_eq!(p1.leader, None);
        assert!(p1.recovering);
    }

    #[test]
    fn aggregate_sums_reachable_only() {
        let nodes = vec![
            NodeMetricsDto {
                node_id: 0,
                address: "a".into(),
                is_self: false,
                reachable: true,
                error: None,
                metrics: Some(MetricsDto {
                    active_instances: 5,
                    creates_total: 100,
                    completions_total: 90,
                    connections_active: 2,
                    commit_inflight: 1,
                    resident_bytes: Some(1000),
                    ..sample_metrics()
                }),
            },
            NodeMetricsDto {
                node_id: 1,
                address: "b".into(),
                is_self: false,
                reachable: false,
                error: Some("down".into()),
                metrics: None,
            },
        ];
        let agg = aggregate(&nodes, 2);
        assert_eq!(agg.total_nodes, 2);
        assert_eq!(agg.reachable_nodes, 1);
        assert_eq!(agg.active_instances, 5);
        assert_eq!(agg.creates_total, 100);
        assert_eq!(agg.resident_bytes, 1000);
    }

    /// A zeroed `MetricsDto` for tests that only set a few fields.
    fn sample_metrics() -> MetricsDto {
        MetricsDto {
            timestamp_ms: 0,
            active_instances: 0,
            creates_rest: 0,
            creates_stream: 0,
            creates_total: 0,
            completions_rest: 0,
            completions_stream: 0,
            completions_total: 0,
            connections_active: 0,
            commit_inflight: 0,
            commits_total: 0,
            writes_total: 0,
            bytes_total: 0,
            credit_stalls_total: 0,
            fsync_mean_ms: 0.0,
            commit_wait_mean_ms: 0.0,
            commit_batch_mean: 0.0,
            frame_processing_mean_ms: 0.0,
            writer_busy_ratio: 0.0,
            resident_bytes: None,
            ceiling_throughput: false,
            ceiling_memory: false,
            ceiling_exporter: false,
            ceiling_flow_control: false,
            exporter_fill_permille: 0,
            sla_mode: "latency".into(),
            pending_create_queue: 0,
            active_backlog: 0,
            admission_backlog_limit: 0,
            admission_create_queue_limit: 0,
            admission_shed_total: 0,
            recovery: RecoveryDto::default(),
        }
    }
}
