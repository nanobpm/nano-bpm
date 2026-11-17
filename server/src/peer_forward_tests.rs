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
