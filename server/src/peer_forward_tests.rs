//! Cross-crate peer-uplink integration tests (ADR 0064 Phase 2).
//!
//! `peer.rs` moved into the `nano-server-raft` library crate, but these tests
//! drive a **real** intra-cluster falcon server — `ServerImpl` plus the falcon
//! dispatcher/router — which lives in this binary and therefore cannot be a
//! dependency of the raft library crate. They stay here, exercising the
//! re-exported `crate::peer` client against an in-process gateway exactly as
//! they did inline before the extraction. (Full inline-test relocation is the
//! ADR 0064 Phase 4 concern; this is the minimum the extraction forces.)

use crate::peer::{PeerLink, PeerSet};

/// Serves a real intra-cluster falcon endpoint on an ephemeral port and
/// returns its HTTP base URL. Models a peer node: a `PeerLink` connects to
/// its `/cluster` channel exactly as a forwarding gateway would in a cluster.
async fn serve_peer() -> String {
    let server = crate::ServerImpl::default();
    let registry = crate::falcon::Registry::new();
    crate::falcon::spawn_dispatcher(server.clone(), registry.clone());
    let app = crate::falcon::cluster_router(server, registry, None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://127.0.0.1:{port}")
}

/// The uplink drives a peer's engine over the Falcon protocol: a forwarded
/// `createProcessInstance` runs on the peer and its `CommandResult` is mapped
/// straight back. This is the transport every stage-1 forwarding op rides on.
#[tokio::test]
async fn peer_link_forwards_create_instance() {
    let base = serve_peer().await;
    let link = PeerLink::connect(&base).await.expect("connect to peer");
    assert!(link.is_connected());

    let res = link
        .create_instance(Some("demo".to_string()), None, None)
        .await
        .expect("forwarded create returns a result");
    assert_eq!(res.status, 200, "peer should accept the forwarded create");
    let body = res.body.expect("create result carries a body");
    assert!(
        body.get("processInstanceKey").is_some(),
        "result should carry the peer-minted processInstanceKey, got {body}"
    );
}

/// Two independent forwarded creates get distinct correlation ids and both
/// resolve — proving the correlation table routes responses correctly.
#[tokio::test]
async fn peer_link_correlates_concurrent_requests() {
    let base = serve_peer().await;
    let link = PeerLink::connect(&base).await.expect("connect to peer");

    let (a, b) = tokio::join!(
        link.create_instance(Some("demo".to_string()), None, None),
        link.create_instance(Some("demo".to_string()), None, None),
    );
    let ka = a.expect("first create").body.unwrap();
    let kb = b.expect("second create").body.unwrap();
    assert_ne!(
        ka.get("processInstanceKey"),
        kb.get("processInstanceKey"),
        "two creates must mint distinct instance keys"
    );
}

/// `PeerSet` dials a peer lazily from the topology address and reuses the live
/// link on the next call — the connection manager every forwarding op uses.
#[tokio::test]
async fn peer_set_dials_lazily_and_forwards() {
    let peer_base = serve_peer().await;
    // Node 0 of a 2-node cluster; node 1 is the served peer.
    let topology = crate::cluster::Topology {
        node_id: 0,
        peers: vec!["http://self-unused".to_string(), peer_base],
        num_partitions: 4,
        replication_factor: 1,
    };
    let peers = PeerSet::new(topology);
    assert!(peers.has_peers());

    let link = peers.link(1).await.expect("dial peer node 1");
    let res = link
        .create_instance(Some("demo".to_string()), None, None)
        .await
        .expect("forwarded create");
    assert_eq!(res.status, 200);

    // Second call reuses the cached, still-connected link (no redial).
    let link2 = peers.link(1).await.expect("reuse cached link");
    assert!(link2.is_connected());
}

/// Serves a real intra-cluster falcon endpoint backed by a single node owning
/// ALL `num_partitions` partitions, and returns its HTTP base URL. The peer's
/// publish handler therefore leads every target partition, which is exactly
/// what a scoped-vs-unscoped `PublishMessage` is asserted against.
async fn serve_multi_partition_peer(num_partitions: u64) -> String {
    let topology = crate::cluster::Topology {
        node_id: 0,
        peers: vec!["http://n0".to_string()],
        num_partitions,
        replication_factor: 1,
    };
    let journals: Vec<crate::Journal> = topology
        .local_partitions()
        .iter()
        .map(|p| crate::Journal::in_memory_partition(*p))
        .collect();
    let server = crate::build_server_in_memory(journals, topology);
    let registry = crate::falcon::Registry::new();
    crate::falcon::spawn_dispatcher(server.clone(), registry.clone());
    let app = crate::falcon::cluster_router(server, registry, None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://127.0.0.1:{port}")
}

/// #1307: a forwarded `PublishMessage` must apply on ONLY the partitions the
/// fan-out still owes (`only_partitions`), never re-committing a partition a
/// prior send already delivered — the duplicate-publication guard for a
/// leadership move between fan-out sends. Deploy a message-start process (its
/// subscription lives solely on partition 0), then publish with a correlation
/// key that hashes to partition 1:
///
///  - scoped to `[1]` (partition 0 already delivered): NO instance is created —
///    the peer must not re-apply partition 0 (partition 1 still mints a key);
///  - unscoped (`None`, the legacy frame): the publish fires partition 0 and
///    starts exactly one instance.
#[tokio::test]
async fn publish_message_only_partitions_scopes_the_peer_apply() {
    let base = serve_multi_partition_peer(4).await;
    let link = PeerLink::connect(&base).await.expect("connect to peer");

    // A message-start process: its start subscription lives on partition 0.
    let intake_bpmn = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="intake">
          <bpmn:startEvent id="s">
            <bpmn:messageEventDefinition messageRef="Message_1" />
          </bpmn:startEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="Message_1" name="order-placed" />
      </bpmn:definitions>"#;
    let res = link
        .deploy(
            vec![("intake.bpmn".to_string(), intake_bpmn.to_string())],
            None,
        )
        .await
        .expect("forwarded deploy");
    assert_eq!(res.status, 200, "deploy should succeed: {res:?}");

    // A correlation key whose catch/boundary partition is 1 (not 0), so the two
    // target partitions are distinct and the scope is observable.
    let order_id = (0..)
        .map(|i| format!("order-{i}"))
        .find(|k| nanobpmn_engine_core::subscription_partition(k, 4) == 1)
        .expect("some key hashes to partition 1");

    // Scoped to partition 1 only: partition 0 is NOT re-applied, so the
    // message-start subscription there must NOT fire — no instance is created.
    // (Partition 1 still mints a message key: `CorrelateMessage` always records
    // the publication, matching Zeebe.)
    let res = link
        .publish_message(
            "order-placed".to_string(),
            order_id.clone(),
            None,
            None,
            Some(vec![1]),
            None,
        )
        .await
        .expect("scoped publish returns a result");
    assert_eq!(res.status, 200, "scoped publish should be 200: {res:?}");
    let body = res.body.expect("publish result carries a body");
    assert!(
        body.get("correlatedInstanceKey")
            .is_none_or(|v| v.is_null()),
        "a publish scoped to partition 1 must not fire the partition-0 message-start subscription: {body}"
    );

    // Unscoped (legacy): the publish reaches partition 0 and starts an instance.
    let res = link
        .publish_message("order-placed".to_string(), order_id, None, None, None, None)
        .await
        .expect("unscoped publish returns a result");
    assert_eq!(res.status, 200);
    let body = res.body.expect("publish result carries a body");
    assert!(
        body.get("messageKey").is_some_and(|v| v.as_str().is_some()),
        "the unscoped publish fires partition 0 and mints a message key: {body}"
    );
}

/// #1600: an `operationReference` riding a forwarded `PublishMessage` frame
/// must survive the Falcon hop AND persistence on the peer. A DISK-backed
/// 4-partition peer receives the publish over a real `PeerLink`; every record
/// the publish produces there — the `MessagePublished` on the correlation-key
/// partition and the message-start instance on partition 0 — is journaled with
/// the reference, while the forwarded deploy (sent unreferenced) carries none.
/// Guards the peer-forward / Falcon-handler leg against silently dropping it.
#[tokio::test]
async fn forwarded_publish_persists_operation_reference_on_the_peer() {
    use std::sync::Arc;

    let dir = std::env::temp_dir().join(format!(
        "nanobpmn-1600-peer-ref-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let topology = crate::cluster::Topology {
        node_id: 0,
        peers: vec!["http://n0".to_string()],
        num_partitions: 4,
        replication_factor: 1,
    };
    let owned = topology.local_partitions();
    let varstore = Arc::new(crate::varstore::VarStore::open(None).expect("varstore"));
    let (writer, recovery) =
        crate::journal::SharedWriter::open_segmented(&dir, &owned, 4, Some(&varstore))
            .expect("open segmented writer");
    let mut engines: std::collections::HashMap<u64, nanobpmn_engine_core::Engine> =
        recovery.engines.into_iter().collect();
    let journals: Vec<crate::Journal> = owned
        .iter()
        .map(|&p| {
            let engine = engines
                .remove(&p)
                .unwrap_or_else(|| nanobpmn_engine_core::Engine::with_partition(p));
            crate::Journal::from_engine_shared(p, engine, true, &writer)
        })
        .collect();
    let server = crate::build_server_in_memory(journals, topology);
    let registry = crate::falcon::Registry::new();
    crate::falcon::spawn_dispatcher(server.clone(), registry.clone());
    let app = crate::falcon::cluster_router(server, registry, None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    let link = PeerLink::connect(&format!("http://127.0.0.1:{port}"))
        .await
        .expect("connect to peer");

    let intake_bpmn = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="intake">
          <bpmn:startEvent id="s">
            <bpmn:messageEventDefinition messageRef="Message_1" />
          </bpmn:startEvent>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
        </bpmn:process>
        <bpmn:message id="Message_1" name="order-placed" />
      </bpmn:definitions>"#;
    let res = link
        .deploy(
            vec![("intake.bpmn".to_string(), intake_bpmn.to_string())],
            None,
        )
        .await
        .expect("forwarded deploy");
    assert_eq!(res.status, 200, "deploy should succeed: {res:?}");

    // A correlation key off partition 0, so the publish spans two partitions.
    let order_id = (0..)
        .map(|i| format!("order-{i}"))
        .find(|k| nanobpmn_engine_core::subscription_partition(k, 4) == 1)
        .expect("some key hashes to partition 1");
    const REF: i64 = 0x1600_0001;
    let res = link
        .publish_message(
            "order-placed".to_string(),
            order_id,
            None,
            None,
            None,
            Some(REF),
        )
        .await
        .expect("forwarded publish returns a result");
    assert_eq!(res.status, 200, "publish should be 200: {res:?}");

    // Wait until the peer's journal holds the message-start completion.
    let read_records = || -> Vec<nanobpmn_engine_core::Record> {
        std::fs::read_dir(&dir)
            .expect("read journal dir")
            .map(|e| e.expect("dir entry").path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("journal") && n.ends_with(".jsonl"))
            })
            .flat_map(|p| crate::seglog::read_segment_records(&p).expect("read segment"))
            .collect()
    };
    let mut records = read_records();
    for _ in 0..200 {
        if records.iter().any(|r| {
            matches!(
                r.event,
                nanobpmn_engine_core::Event::ProcessInstanceCompleted { .. }
            )
        }) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        records = read_records();
    }

    let published: Vec<_> = records
        .iter()
        .filter(|r| {
            matches!(
                r.event,
                nanobpmn_engine_core::Event::MessagePublished { .. }
            )
        })
        .collect();
    assert!(
        !published.is_empty(),
        "the publish journals MessagePublished"
    );
    for r in &published {
        assert_eq!(r.operation_reference, Some(REF), "{r:?}");
    }
    let completed = records
        .iter()
        .find(|r| {
            matches!(
                r.event,
                nanobpmn_engine_core::Event::ProcessInstanceCompleted { .. }
            )
        })
        .expect("the message-start instance completes on the peer");
    assert_eq!(completed.operation_reference, Some(REF), "{completed:?}");
    let deployed: Vec<_> = records
        .iter()
        .filter(|r| matches!(r.event, nanobpmn_engine_core::Event::ProcessDeployed { .. }))
        .collect();
    assert!(!deployed.is_empty());
    for r in &deployed {
        assert_eq!(r.operation_reference, None, "unreferenced deploy: {r:?}");
    }
    drop(link);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// ADR 0066 rolling-upgrade gate for cluster variables (#1657, PR #1670 review).
//
// The new cluster-variable command variants are Raft entries an older binary
// cannot decode, so the gateway must emit nothing unless EVERY peer advertises
// `FEATURE_CLUSTER_VARIABLES`. These drive a real multi-node layout: a gateway
// (node 0, owning partition 0) plus served peers owning the other partitions.
// ---------------------------------------------------------------------------

/// Serves a real cluster node `node_id` of `topology` and returns its base URL
/// plus a handle on its server (for asserting its partition state).
async fn serve_cluster_node(
    mut topology: crate::cluster::Topology,
    node_id: u32,
) -> (String, crate::ServerImpl) {
    topology.node_id = node_id;
    let journals: Vec<crate::Journal> = topology
        .local_partitions()
        .iter()
        .map(|p| crate::Journal::in_memory_partition(*p))
        .collect();
    let server = crate::build_server_in_memory(journals, topology);
    let registry = crate::falcon::Registry::new();
    crate::falcon::spawn_dispatcher(server.clone(), registry.clone());
    let app = crate::falcon::cluster_router(server.clone(), registry, None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://127.0.0.1:{port}"), server)
}

/// Serves a peer that predates capability advertisement: its `/cluster`
/// channel answers with a legacy `Welcome` carrying NO `features` field.
async fn serve_legacy_welcome_peer() -> String {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    let app = axum::Router::new().route(
        "/cluster",
        axum::routing::get(|ws: WebSocketUpgrade| async move {
            ws.on_upgrade(|mut socket| async move {
                let welcome = r#"{"type":"welcome","submissionCredits":1,"heartbeatMs":30000}"#;
                if socket.send(Message::Text(welcome.into())).await.is_err() {
                    return;
                }
                // Hold the socket open (a live-but-old peer), draining input.
                while let Some(Ok(_)) = socket.recv().await {}
            })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://127.0.0.1:{port}")
}

/// An unreachable base URL (a bound-then-dropped ephemeral port).
async fn unreachable_peer() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

fn cluster_topology(peers: Vec<String>) -> crate::cluster::Topology {
    let n = peers.len() as u64;
    crate::cluster::Topology {
        node_id: 0,
        peers,
        num_partitions: n,
        replication_factor: 1,
    }
}

fn create_var(name: &str) -> nanobpmn_engine_core::Command {
    nanobpmn_engine_core::Command::CreateClusterVariable {
        tenant_id: None,
        name: name.to_string(),
        value: nanobpmn_engine_core::Value::Int(7),
        kind: Default::default(),
        metadata: Default::default(),
        distributed: false,
    }
}

/// Asserts a write was rejected 503 BEFORE any partition was touched: the
/// gateway's partition 0 holds nothing, and so does every capable peer.
async fn assert_rejected_untouched(
    gateway: &crate::ServerImpl,
    capable_peers: &[crate::ServerImpl],
    name: &str,
) {
    let Err(err) = gateway.cluster_variable_mutate(create_var(name)).await else {
        panic!("the write must fail closed");
    };
    assert_eq!(
        err.0, 503,
        "rolling-upgrade gate must answer 503: {}",
        err.1
    );
    assert!(
        gateway.cluster_variable_get(None, name).await.is_none(),
        "partition 0 must be untouched by a rejected write"
    );
    for peer in capable_peers {
        assert!(
            peer.cluster_variable_get(None, name).await.is_none(),
            "a capable peer's partition must be untouched by a rejected write"
        );
    }
}

/// All peers capable: the write commits on partition 0 AND its fan-out copy
/// reaches the remotely-led partition 1 through the peer link.
#[tokio::test]
async fn cluster_variable_write_reaches_remote_partition_when_all_peers_capable() {
    let (peer_base, peer) = serve_cluster_node(
        cluster_topology(vec!["http://n0".into(), "http://n1".into()]),
        1,
    )
    .await;
    let gateway = crate::build_server_in_memory(
        vec![crate::Journal::in_memory_partition(0)],
        cluster_topology(vec!["http://self-unused".into(), peer_base]),
    );
    let rec = gateway
        .cluster_variable_mutate(create_var("ok"))
        .await
        .expect("an all-capable cluster accepts the write");
    assert!(rec.is_some());
    assert!(gateway.cluster_variable_get(None, "ok").await.is_some());
    assert!(
        peer.cluster_variable_get(None, "ok").await.is_some(),
        "the fan-out copy must reach the remotely-led partition 1"
    );
}

/// A peer whose `Welcome` predates the `features` field advertises nothing:
/// the write is rejected and no partition (local or capable peer) is touched.
#[tokio::test]
async fn cluster_variable_write_rejected_for_legacy_welcome_peer() {
    let topo = cluster_topology(vec![
        "http://n0".into(),
        "http://n1".into(),
        "http://n2".into(),
    ]);
    let (capable_base, capable) = serve_cluster_node(topo, 1).await;
    let legacy_base = serve_legacy_welcome_peer().await;
    let gateway = crate::build_server_in_memory(
        vec![crate::Journal::in_memory_partition(0)],
        cluster_topology(vec!["http://self-unused".into(), capable_base, legacy_base]),
    );
    assert_rejected_untouched(&gateway, &[capable], "legacy").await;
}

/// An unreachable peer cannot confirm support: fail closed, nothing touched.
#[tokio::test]
async fn cluster_variable_write_rejected_for_unreachable_peer() {
    let topo = cluster_topology(vec![
        "http://n0".into(),
        "http://n1".into(),
        "http://n2".into(),
    ]);
    let (capable_base, capable) = serve_cluster_node(topo, 1).await;
    let gone = unreachable_peer().await;
    let gateway = crate::build_server_in_memory(
        vec![crate::Journal::in_memory_partition(0)],
        cluster_topology(vec!["http://self-unused".into(), capable_base, gone]),
    );
    assert_rejected_untouched(&gateway, &[capable], "unreachable").await;
}
