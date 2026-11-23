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
    serve(crate::ServerImpl::default()).await
}

/// Serves `server`'s intra-cluster falcon endpoint; returns its base URL.
async fn serve(server: crate::ServerImpl) -> String {
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
        // Must match the served peer's partition count: a mismatch is refused
        // at the join handshake (#1661).
        num_partitions: 1,
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
        .publish_message(
            "order-placed".to_string(),
            order_id,
            None,
            None,
            None,
            None,
            None,
        )
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
            None,
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

/// #1602: an `operationReference` riding a forwarded `ForwardCreate` frame must
/// survive the Falcon hop AND persistence on the peer. A DISK-backed peer
/// receives the create over a real `PeerLink`; every record the create
/// produces there — the `ProcessInstanceCreated` and its element cascade — is
/// journaled with the reference, while the forwarded deploy (sent
/// unreferenced) carries none. Guards the peer-forward / Falcon-handler leg of
/// the REST create path against silently dropping it.
#[tokio::test]
async fn forwarded_create_persists_operation_reference_on_the_peer() {
    use std::sync::Arc;

    let dir = std::env::temp_dir().join(format!(
        "nanobpmn-1602-peer-ref-{}-{:?}",
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
    let base = serve(server).await;
    let link = PeerLink::connect(&base).await.expect("connect to peer");

    let auto_bpmn = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="auto">
          <bpmn:startEvent id="s" />
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let res = link
        .deploy(vec![("auto.bpmn".to_string(), auto_bpmn.to_string())], None)
        .await
        .expect("forwarded deploy");
    assert_eq!(res.status, 200, "deploy should succeed: {res:?}");

    const REF: i64 = 0x1602_0001;
    let res = link
        .forward_create(
            Some("auto".to_string()),
            None,
            None,
            Vec::new(),
            None,
            false,
            None,
            None,
            "rest",
            Some(REF),
        )
        .await
        .expect("forwarded create returns a result");
    assert_eq!(res.status, 200, "create should be 200: {res:?}");
    let instance_key: u64 = res
        .body
        .as_ref()
        .and_then(|b| b.get("processInstanceKey"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .expect("create result carries a numeric processInstanceKey");

    // Wait until the peer's journal holds the create cascade, then read every
    // journaled record envelope.
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
                nanobpmn_engine_core::Event::ProcessInstanceCreated { .. }
            )
        }) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        records = read_records();
    }

    let create_records: Vec<_> = records
        .iter()
        .filter(|r| r.event.instance_key() == Some(instance_key))
        .collect();
    assert!(
        create_records.iter().any(|r| matches!(
            r.event,
            nanobpmn_engine_core::Event::ProcessInstanceCreated { .. }
        )),
        "the forwarded create journals the instance: {create_records:?}"
    );
    for r in &create_records {
        assert_eq!(
            r.operation_reference,
            Some(REF),
            "every record of the forwarded create carries the reference: {r:?}"
        );
    }
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

/// A peer whose `/cluster` join handshake advertises no
/// `FEATURE_CLUSTER_VARIABLES` (an older binary, #1661 registry): the write is
/// rejected and no partition (local, capable or legacy peer) is touched.
#[tokio::test]
async fn cluster_variable_write_rejected_for_featureless_peer() {
    let topo = cluster_topology(vec![
        "http://n0".into(),
        "http://n1".into(),
        "http://n2".into(),
    ]);
    let (capable_base, capable) = serve_cluster_node(topo.clone(), 1).await;
    let (legacy_base, legacy) = serve_cluster_node(topo, 2).await;
    legacy
        .advertise_cluster_features
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let gateway = crate::build_server_in_memory(
        vec![crate::Journal::in_memory_partition(0)],
        cluster_topology(vec!["http://self-unused".into(), capable_base, legacy_base]),
    );
    assert_rejected_untouched(&gateway, &[capable, legacy], "legacy").await;
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

/// Serves node 0 of a two-node, one-partition cluster whose partition's
/// REPLICATED lenient-completion flag is `lenient` (#1661), set through the
/// journaled command. (A single node never requires peer features: it
/// replicates its log to no one.)
async fn serve_peer_lenient(lenient: bool) -> String {
    let server = crate::build_server_in_memory(
        vec![crate::Journal::in_memory()],
        crate::cluster::Topology {
            node_id: 0,
            peers: vec!["http://self-unused".into(), "http://other-unused".into()],
            num_partitions: 1,
            replication_factor: 1,
        },
    );
    for handle in server.engine.all() {
        handle
            .with(move |journal| {
                let (_events, _commit) = journal
                    .apply_command_at(
                        nanobpmn_engine_core::Command::SetLenientCompletion { lenient },
                        1,
                    )
                    .expect("set lenient");
            })
            .await;
    }
    serve(server).await
}

/// #1661: a joiner with the same `NANOBPMN_PARTITIONS` links, and learns the
/// peer's replicated-command features from the handshake.
#[tokio::test]
async fn join_with_matching_partition_count_links_and_learns_features() {
    let base = serve_peer_lenient(true).await;
    let link = PeerLink::connect_as(&base, Some(1))
        .await
        .expect("same partition count joins");
    assert!(
        link.supports(nano_falcon_protocol::cluster_join::FEATURE_REPLICATED_LENIENT_COMPLETION)
    );
}

/// #1661: a joiner whose partition count differs is refused at the handshake,
/// before any frame is exchanged — it would place keys on partitions the rest
/// of the cluster routes elsewhere.
#[tokio::test]
async fn join_with_mismatched_partition_count_is_rejected() {
    let base = serve_peer().await;
    let err = match PeerLink::connect_as(&base, Some(4)).await {
        Ok(_) => panic!("a partition-count mismatch must be refused at join"),
        Err(e) => e,
    };
    assert!(
        format!("{err:?}").contains("409"),
        "refused with 409 Conflict, got {err:?}"
    );
}

/// #1661 rolling upgrade: once a node's replicated state carries
/// `LenientCompletionSet`, a binary that does not advertise the feature (a
/// pre-#1661 build: no handshake headers at all) is refused — it could not
/// decode that command in the log it would replicate.
#[tokio::test]
async fn join_from_incompatible_binary_is_rejected_once_feature_is_in_use() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
    let base = serve_peer_lenient(true).await;
    let url = format!("{}/cluster", base.replacen("http://", "ws://", 1));
    let request = url.clone().into_client_request().expect("request");
    let err = match tokio_tungstenite::connect_async(request).await {
        Ok(_) => panic!("a featureless (pre-#1661) binary must be refused"),
        Err(e) => e,
    };
    assert!(format!("{err:?}").contains("409"), "got {err:?}");

    // A legacy binary still joins a node whose state does not yet depend on
    // the feature (the rolling-upgrade window).
    // It carries this build's version: this fixture exercises FEATURE
    // negotiation only, and a versionless joiner fails closed at every
    // determinism boundary on a stamped release build (#1702).
    let plain = serve_peer_lenient(false).await;
    let url = format!("{}/cluster", plain.replacen("http://", "ws://", 1));
    let mut request = url.into_client_request().expect("request");
    request.headers_mut().insert(
        nano_falcon_protocol::cluster_join::CLUSTER_VERSION_HEADER,
        tokio_tungstenite::tungstenite::http::HeaderValue::from_str(
            &nano_falcon_protocol::cluster_join::local_version_header(),
        )
        .expect("version header"),
    );
    tokio_tungstenite::connect_async(request)
        .await
        .expect("legacy binary joins before the feature is in use");
}

/// #1702: a mixed-version join ACROSS a declared determinism upgrade boundary
/// is refused at the handshake — in BOTH directions — with a typed,
/// operator-actionable error naming both versions and the boundary. The
/// checked-in allowlist (`divergence-allowlist.json`, the single source of
/// truth the boundary table is derived from at build time) declares v0.0.28
/// `upgrade-boundary` entries (#1244 fork/join order, #1402
/// `GatewayRouteSelected`), so 0.0.27 and 0.0.28 members must never share a
/// cluster. Later boundaries (e.g. v0.0.29, #1528) are read from the ledger,
/// never hard-coded here. A join WITHIN one boundary region still succeeds.
///
/// Drives the `/cluster` handshake at the HTTP level so each side presents
/// its own version header: the acceptor's answer always carries this build's
/// stamped version, the dial carries the override.
#[tokio::test]
async fn a_mixed_version_join_across_an_upgrade_boundary_is_refused() {
    use nano_falcon_protocol::cluster_join as cj;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};

    // The version-override hook is process-global: the gateway acceptor
    // below runs the boundary release (v0.0.28) while it names that version;
    // the dialer-side cases later switch it to v0.0.29 and dial raw
    // acceptors whose advertised versions are fixed per server.
    cj::set_advertised_version_override(Some("0.0.28"));
    let base = serve_peer().await;
    let url = format!("{}/cluster", base.replacen("http://", "ws://", 1));

    async fn dial_with_version(
        url: &str,
        version: Option<&str>,
    ) -> Result<
        (
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
        ),
        tokio_tungstenite::tungstenite::Error,
    > {
        let mut request = url.into_client_request().expect("request");
        let headers = request.headers_mut();
        headers.insert(
            HeaderName::from_static(cj::CLUSTER_FEATURES_HEADER),
            HeaderValue::from_str(&cj::local_features_header()).expect("features header"),
        );
        headers.insert(
            HeaderName::from_static(cj::CLUSTER_PARTITIONS_HEADER),
            HeaderValue::from(1),
        );
        if let Some(v) = version {
            headers.insert(
                HeaderName::from_static(cj::CLUSTER_VERSION_HEADER),
                HeaderValue::from_str(v).expect("version header"),
            );
        }
        tokio_tungstenite::connect_async(request).await
    }

    // Acceptor at the boundary release (v0.0.28) refuses a v0.0.27 joiner
    // with 409 and the typed error naming both versions and the boundary.
    let err = match dial_with_version(&url, Some("0.0.27")).await {
        Ok(_) => panic!("a join across the v0.0.28 boundary must be refused (acceptor side)"),
        Err(e) => e,
    };
    let tokio_tungstenite::tungstenite::Error::Http(resp) = &err else {
        panic!("a refused join is an HTTP error response, got {err:?}");
    };
    assert_eq!(resp.status(), 409, "refused with 409 Conflict");
    let msg = String::from_utf8_lossy(resp.body().as_deref().unwrap_or_default()).into_owned();
    for needle in ["0.0.28", "0.0.27", "no rolling upgrade", "quiesce"] {
        assert!(msg.contains(needle), "the refusal names {needle:?}: {msg}");
    }
    // Within the same boundary region the join still succeeds (both at the
    // boundary release).
    dial_with_version(&url, Some("0.0.28"))
        .await
        .expect("same-version members join");
    // A pre-#1702 binary (no version header) dialing the boundary release is
    // refused closed: it may be pre-boundary, and the boundary release cannot
    // tell (it is the first release that advertises versions).
    let err = match dial_with_version(&url, None).await {
        Ok(_) => panic!("a versionless binary joining the boundary release must be refused"),
        Err(e) => e,
    };
    let tokio_tungstenite::tungstenite::Error::Http(resp) = &err else {
        panic!("a refused join is an HTTP error response, got {err:?}");
    };
    assert_eq!(resp.status(), 409, "refused with 409 Conflict");
    let msg = String::from_utf8_lossy(resp.body().as_deref().unwrap_or_default()).into_owned();
    for needle in ["unknown", "0.0.28"] {
        assert!(msg.contains(needle), "the refusal names {needle:?}: {msg}");
    }
    // A NEWER release joins an acceptor at the NEWEST declared boundary (no
    // boundary lies above it). Derived from the ledger, not a hard-coded
    // pair: a later boundary (e.g. v0.0.29, #1528) must not stale this check.
    let newest = cj::UPGRADE_BOUNDARIES
        .last()
        .expect("the ledger declares at least the v0.0.28 boundary")
        .version;
    let (maj, min, patch) = cj::parse_release_triple(newest).expect("boundary is a release triple");
    let past_newest: &'static str = format!("{maj}.{min}.{}", patch + 1).leak();
    cj::set_advertised_version_override(Some(newest));
    dial_with_version(&url, Some(past_newest))
        .await
        .expect("no boundary above the newest: a newer member joins");
    cj::set_advertised_version_override(Some("0.0.28"));

    // Dialer side, through the REAL `PeerLink::connect_as` (#1702 review):
    // an acceptor that PERMITS the upgrade but answers a release across the
    // boundary must be refused by the dialer's own response check. The
    // acceptor is a raw WebSocket server so its advertised version is
    // independent of the process-global override (which names the dialer's
    // version, v0.0.29, here).
    cj::set_advertised_version_override(Some("0.0.29"));
    let old_release = raw_cluster_acceptor(Some("0.0.27")).await;
    let err = match PeerLink::connect_as(&old_release, Some(1)).await {
        Ok(_) => panic!("a v0.0.29 dialer must refuse a v0.0.27 acceptor (v0.0.28 boundary)"),
        Err(e) => e.to_string(),
    };
    for needle in ["0.0.29", "0.0.27", "0.0.28", "no rolling upgrade"] {
        assert!(
            err.contains(needle),
            "the dialer-side error names {needle:?}: {err}"
        );
    }
    // Same release: the identical acceptor shape links (the refusal above is
    // the version check, not a broken fixture).
    let same_release = raw_cluster_acceptor(Some("0.0.29")).await;
    PeerLink::connect_as(&same_release, Some(1))
        .await
        .expect("a same-release acceptor links");
    // A versionless (pre-#1702) acceptor fails closed even PAST the boundary:
    // a v0.0.27 binary is just as versionless as a v0.0.28 one.
    let versionless = raw_cluster_acceptor(None).await;
    let err = match PeerLink::connect_as(&versionless, Some(1)).await {
        Ok(_) => panic!("a versionless acceptor must be refused by a release dialer"),
        Err(e) => e.to_string(),
    };
    // The fail-closed error names the newest boundary at or below the
    // dialer's own release (v0.0.29 here), derived from the ledger so a later
    // boundary (#1528 declares v0.0.29) cannot stale it.
    let dialer = cj::parse_release_triple("0.0.29").expect("triple");
    let expected_boundary = cj::UPGRADE_BOUNDARIES
        .iter()
        .rev()
        .find(|b| cj::parse_release_triple(b.version).is_some_and(|v| v <= dialer))
        .expect("a boundary at or below the dialer")
        .version;
    assert!(
        err.contains("unknown") && err.contains(expected_boundary),
        "the fail-closed error names boundary v{expected_boundary}: {err}"
    );

    // The gateway's own pre-#1702 model (version advertisement cleared,
    // features still advertised) answers no version header.
    let legacy = crate::ServerImpl::default();
    legacy
        .advertise_cluster_version
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let legacy_base = serve(legacy).await;
    let legacy_url = format!("{legacy_base}/cluster").replacen("http://", "ws://", 1);
    let (_ws, legacy_resp) = dial_with_version(&legacy_url, Some("0.0.29"))
        .await
        .expect("a versionless acceptor still upgrades the socket");
    assert!(
        legacy_resp
            .headers()
            .get(cj::CLUSTER_VERSION_HEADER)
            .is_none(),
        "the legacy acceptor answers no version header"
    );
    assert!(
        legacy_resp
            .headers()
            .get(cj::CLUSTER_FEATURES_HEADER)
            .is_some(),
        "version advertisement is independent of feature advertisement"
    );
    cj::set_advertised_version_override(None);
}

/// A raw `/cluster` acceptor (#1702 tests) that upgrades every socket and
/// answers partition count 1, this build's features and `version` (none when
/// `None`) — a member running another release, without depending on the
/// process-global version override. Returns its `http://` base URL.
#[allow(
    clippy::result_large_err,
    reason = "tungstenite's accept_hdr callback signature fixes the Err type"
)]
async fn raw_cluster_acceptor(version: Option<&'static str>) -> String {
    use nano_falcon_protocol::cluster_join as cj;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let callback = move |_req: &Request, mut resp: Response| {
                    let headers = resp.headers_mut();
                    headers.insert(cj::CLUSTER_PARTITIONS_HEADER, HeaderValue::from(1));
                    if let Ok(v) = HeaderValue::from_str(&cj::local_features_header()) {
                        headers.insert(cj::CLUSTER_FEATURES_HEADER, v);
                    }
                    if let Some(v) = version {
                        headers.insert(cj::CLUSTER_VERSION_HEADER, HeaderValue::from_static(v));
                    }
                    Ok(resp)
                };
                if let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await {
                    use futures_util::StreamExt as _;
                    while let Some(Ok(_)) = ws.next().await {}
                }
            });
        }
    });
    format!("http://{addr}")
}

/// #1661 (outbound direction): once this node's replicated state requires a
/// feature, it must not DIAL a member that restarted on an older binary — on
/// either lane — or Raft would stream it entries it cannot decode.
#[tokio::test]
async fn outbound_link_to_incompatible_binary_is_refused_once_feature_is_required() {
    use nano_falcon_protocol::cluster_join::FEATURE_REPLICATED_LENIENT_COMPLETION as F;
    let old = crate::ServerImpl::default();
    old.advertise_cluster_features
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let base = serve(old).await;
    let err = match PeerLink::connect_requiring(&base, Some(1), &[F]).await {
        Ok(_) => panic!("an outbound link to a featureless binary must be refused"),
        Err(e) => e,
    };
    assert!(format!("{err:?}").contains(F), "got {err:?}");
    // Before the requirement exists the same peer links (rolling upgrade).
    let link = PeerLink::connect_requiring(&base, Some(1), &[])
        .await
        .expect("no requirement yet");
    assert!(!link.supports(F));
}

/// PR #1786 review (#1434): the publication key must survive the PeerLink +
/// Falcon hop. A 3-node cluster (one partition per node) where the GATEWAY
/// (node 2) owns neither target partition of the publish: node 0 (partition
/// 0, message starts) is sent first and mints the key, which must then ride
/// the send to node 1 (partition 1, the correlation-key partition) so the
/// catch correlation recorded there carries the key the client received.
/// Dropping the forwarded key would let node 1 mint its own, and this fails.
#[tokio::test]
async fn cross_node_publish_forwards_one_message_key_to_every_peer() {
    use nanobpmn_engine_core::ProcessBuilder;
    // Each peer is served as a self-contained node leading every partition (so
    // it can deploy and place instances without dialling anyone); the gateway
    // still addresses node 0 for partition 0 and node 1 for partition 1, and
    // each peer applies only the partitions that send scopes it to.
    let standalone = crate::cluster::Topology {
        node_id: 0,
        peers: vec!["http://self".into()],
        num_partitions: 3,
        replication_factor: 1,
    };
    let (n0_base, _n0) = serve_cluster_node(standalone.clone(), 0).await;
    let (n1_base, n1) = serve_cluster_node(standalone, 0).await;

    // A waiting catch on node 1 whose subscription lives on partition 1 (the
    // correlation key hashes there).
    let order_id = (0..)
        .map(|i| format!("order-{i}"))
        .find(|k| nanobpmn_engine_core::subscription_partition(k, 3) == 1)
        .expect("some key hashes to partition 1");
    let catcher = ProcessBuilder::new("catcher")
        .start_event("s")
        .message_intermediate_catch_event("wait", "order-placed", "orderId")
        .end_event("e")
        .connect("s", "wait")
        .connect("wait", "e")
        .build()
        .expect("valid catch process");
    let mut names = std::collections::HashMap::new();
    names.insert("catcher".to_string(), "catcher.bpmn".to_string());
    n1.deploy_resources_locally(
        vec![catcher],
        &names,
        Vec::new(),
        &std::collections::HashMap::new(),
        "<default>",
    )
    .await
    .expect("deploy the catcher on node 1");
    let n1_link = PeerLink::connect(&n1_base).await.expect("connect node 1");
    let mut vars = serde_json::Map::new();
    vars.insert(
        "orderId".into(),
        serde_json::Value::String(order_id.clone()),
    );
    let res = n1_link
        .create_instance(Some("catcher".into()), None, Some(vars))
        .await
        .expect("create on node 1");
    assert_eq!(res.status, 200, "{res:?}");

    // The gateway owns only partition 2: neither target partition.
    let gateway = crate::build_server_in_memory(
        vec![crate::Journal::in_memory_partition(2)],
        crate::cluster::Topology {
            node_id: 2,
            ..cluster_topology(vec![n0_base, n1_base, "http://self-unused".into()])
        },
    );
    let mut request = crate::models::MessagePublicationRequest::new("order-placed".to_string());
    request.correlation_key = Some(order_id);
    let resp = gateway
        .publish_message_impl(&request)
        .await
        .expect("publish returns");
    let crate::apis::message::PublishMessageResponse::Status200_TheMessageWasPublished(published) =
        resp
    else {
        panic!("expected a 200 publication, got {resp:?}");
    };
    let message_key = published.message_key.0.clone();

    let mut rows = Vec::new();
    for _ in 0..200 {
        rows = n1
            .store
            .correlated_message_subscriptions()
            .into_iter()
            .map(|r| r.message_key.to_string())
            .collect();
        if !rows.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(rows.len(), 1, "node 1 correlated the catch: {rows:?}");
    assert_eq!(
        rows[0], message_key,
        "node 1's correlation carries the key the client received"
    );
}

/// #1606: the by-key instance mutations forwarded over Falcon — set-variables,
/// suspend, resume and cancel — carry the REST request's `operationReference`
/// in their frame, and the owning peer journals every resulting record with it.
/// A DISK-backed peer receives each frame over a real `PeerLink`; the persisted
/// envelopes prove the reference survives the hop and persistence.
#[tokio::test]
async fn forwarded_instance_mutations_persist_operation_reference_on_the_peer() {
    use std::sync::Arc;

    let dir = std::env::temp_dir().join(format!(
        "nanobpmn-1606-peer-ref-{}-{:?}",
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
    let app = crate::falcon::cluster_router(server.clone(), registry, None);
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

    let bpmn = r#"
      <bpmn:definitions
          xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
        <bpmn:process id="work">
          <bpmn:startEvent id="s" />
          <bpmn:serviceTask id="t">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="work-1606" />
            </bpmn:extensionElements>
          </bpmn:serviceTask>
          <bpmn:endEvent id="e" />
          <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="t" />
          <bpmn:sequenceFlow id="f1" sourceRef="t" targetRef="e" />
        </bpmn:process>
      </bpmn:definitions>"#;
    let res = link
        .deploy(vec![("work.bpmn".to_string(), bpmn.to_string())], None)
        .await
        .expect("forwarded deploy");
    assert_eq!(res.status, 200, "deploy should succeed: {res:?}");

    let body =
        crate::models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
            crate::models::ProcessInstanceCreationInstructionById::new("work".to_string()),
        );
    let resp = server
        .create_process_instance_impl(&body)
        .await
        .expect("create returns a response");
    let crate::apis::process_instance::CreateProcessInstanceResponse::Status200_TheProcessInstanceWasCreated(created) =
        resp
    else {
        panic!("create must succeed: {resp:?}");
    };
    let key = created.process_instance_key.0.clone();
    let instance_key: u64 = key.parse().expect("numeric instance key");

    const SET_REF: i64 = 0x0016_0611;
    const SUSPEND_REF: i64 = 0x0016_0612;
    const RESUME_REF: i64 = 0x0016_0613;
    const CANCEL_REF: i64 = 0x0016_0614;
    let mut vars = serde_json::Map::new();
    vars.insert("fixed".to_string(), serde_json::Value::Bool(true));
    let res = link
        .set_variables(key.clone(), Some(vars), false, Some(SET_REF))
        .await
        .expect("forwarded set-variables");
    assert!(res.status < 300, "set-variables should succeed: {res:?}");
    let res = link
        .suspend_instance(key.clone(), Some(SUSPEND_REF))
        .await
        .expect("forwarded suspend");
    assert!(res.status < 300, "suspend should succeed: {res:?}");
    let res = link
        .resume_instance(key.clone(), Some(RESUME_REF))
        .await
        .expect("forwarded resume");
    assert!(res.status < 300, "resume should succeed: {res:?}");
    let res = link
        .cancel_instance(key.clone(), Some(CANCEL_REF))
        .await
        .expect("forwarded cancel");
    assert!(res.status < 300, "cancel should succeed: {res:?}");

    drop(link);
    drop(server);
    let records: Vec<nanobpmn_engine_core::Record> = std::fs::read_dir(&dir)
        .expect("read journal dir")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("journal") && n.ends_with(".jsonl"))
        })
        .flat_map(|p| crate::seglog::read_segment_records(&p).expect("read segment"))
        .collect();
    let refs_of = |pred: &dyn Fn(&nanobpmn_engine_core::Event) -> bool| -> Vec<Option<i64>> {
        records
            .iter()
            .filter(|r| r.event.instance_key() == Some(instance_key) && pred(&r.event))
            .map(|r| r.operation_reference)
            .collect()
    };
    use nanobpmn_engine_core::Event;
    for (what, refs, want) in [
        (
            "set-variables",
            refs_of(&|e| {
                matches!(e, Event::VariablesUpdated { .. }) && format!("{e:?}").contains("fixed")
            }),
            SET_REF,
        ),
        (
            "suspend",
            refs_of(&|e| matches!(e, Event::ProcessInstanceSuspended { .. })),
            SUSPEND_REF,
        ),
        (
            "resume",
            refs_of(&|e| matches!(e, Event::ProcessInstanceResumed { .. })),
            RESUME_REF,
        ),
        (
            "cancel",
            refs_of(&|e| matches!(e, Event::ProcessInstanceTerminated { .. })),
            CANCEL_REF,
        ),
    ] {
        assert!(!refs.is_empty(), "{what} journaled no record on the peer");
        assert!(
            refs.iter().all(|r| *r == Some(want)),
            "every forwarded {what} record carries its reference: {refs:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
