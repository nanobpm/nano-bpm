//! The Raft network (stage 3 milestone C): a transport-abstracted
//! [`RaftNetwork`] that replaces the milestone-A `Unreachable` stub and lets a
//! partition's replicas exchange AppendEntries / Vote / InstallSnapshot, raising
//! the replication factor from 1 to 3.
//!
//! # Transport seam
//!
//! openraft's network is split into *what* to send (the typed RPCs) and *how* to
//! carry the bytes. This module owns the *what* — [`RaftRpcRequest`] /
//! [`RaftRpcResponse`] and the [`PartitionNetwork`] that serializes a call into
//! them — and delegates the *how* to a [`RaftTransport`]. That keeps the carrier
//! pluggable:
//!
//! - [`LocalCluster`] dispatches in-process (used by the multi-voter tests here),
//!   proving replication and commit across a real 3-voter group.
//! - A falcon-backed transport (a node sending [`RaftRpcRequest`] frames
//!   to the peer that hosts the target replica) mounts the same `PartitionNetwork`
//!   onto the cluster's existing WebSocket protocol; that binding lands with
//!   leader routing, where the server actually hosts the Raft groups.
//!
//! The receiving side is symmetric: [`dispatch`] feeds an inbound
//! [`RaftRpcRequest`] into the local [`openraft::Raft`] and returns its
//! [`RaftRpcResponse`], whatever transport delivered it.

// Additive until leader routing mounts it; see `raft.rs` for the rationale.
#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use openraft::BasicNode;
use openraft::error::{NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::peer::PeerSet;
use crate::raft::{NodeId, RaftConfig};

/// A boxed, `Send` future — the return shape of the dyn-compatible
/// [`RaftTransport::send`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One of the three Raft RPCs, serialized for transport. Carrying the typed
/// openraft requests (rather than opaque bytes) keeps the wire format
/// self-describing and lets any transport reuse the same serde.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcRequest {
    AppendEntries(AppendEntriesRequest<RaftConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<RaftConfig>),
}

/// The matching response to a [`RaftRpcRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcResponse {
    AppendEntries(AppendEntriesResponse<NodeId>),
    Vote(VoteResponse<NodeId>),
    InstallSnapshot(InstallSnapshotResponse<NodeId>),
}

/// Serialized-JSON size (bytes) at or above which a Raft RPC payload is
/// deflate+base64 compressed before it crosses the wire. Small control RPCs
/// (Vote, empty-entry heartbeats) stay below this and are sent as raw JSON, so
/// they pay no compression cost; only the heavy AppendEntries / InstallSnapshot
/// payloads carrying large variable blobs are compressed — exactly the case a
/// user with big process variables hits. 1 KiB comfortably clears the control
/// traffic while catching anything with a non-trivial variable map.
pub(crate) const RAFT_RPC_COMPRESS_THRESHOLD: usize = 1024;

/// Encodes a serialized-JSON Raft RPC for the wire, compressing it (raw deflate,
/// then base64 so it rides the JSON falcon frame without escaping) only
/// when it is large enough to be worth it. Returns `(payload, compressed)`.
/// Compression failures fall back to the raw JSON — never an error.
pub(crate) fn encode_rpc_payload(json: String) -> (String, bool) {
    if json.len() < RAFT_RPC_COMPRESS_THRESHOLD {
        return (json, false);
    }
    use std::io::Write;

    use base64::Engine;
    use flate2::{Compression, write::DeflateEncoder};
    let mut enc = DeflateEncoder::new(Vec::new(), Compression::fast());
    if enc.write_all(json.as_bytes()).is_err() {
        return (json, false);
    }
    match enc.finish() {
        Ok(bytes) => (
            base64::engine::general_purpose::STANDARD.encode(bytes),
            true,
        ),
        Err(_) => (json, false),
    }
}

/// Reverses [`encode_rpc_payload`]: returns the raw JSON, decompressing iff
/// `compressed`.
pub fn decode_rpc_payload(payload: &str, compressed: bool) -> Result<String, String> {
    if !compressed {
        return Ok(payload.to_string());
    }
    use std::io::Read;

    use base64::Engine;
    use flate2::read::DeflateDecoder;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload.as_bytes())
        .map_err(|e| format!("raft rpc base64 decode: {e}"))?;
    let mut out = String::new();
    DeflateDecoder::new(&bytes[..])
        .read_to_string(&mut out)
        .map_err(|e| format!("raft rpc inflate: {e}"))?;
    Ok(out)
}

/// A transport failure — the peer could not be reached or did not answer. Raft
/// treats this as retryable (it backs off and retries), so a transient peer
/// outage degrades to slower commit, never lost data.
#[derive(Debug)]
pub struct TransportError(pub String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "raft transport error: {}", self.0)
    }
}
impl std::error::Error for TransportError {}

/// Carries a partition's Raft RPCs to a target replica. The single method routes
/// `req` to the node hosting replica `target` of `partition` and returns its
/// answer. Implementations: [`LocalCluster`] (in-process) and, with leader
/// routing, a falcon-backed carrier.
pub trait RaftTransport: Send + Sync + 'static {
    fn send<'a>(
        &'a self,
        target: NodeId,
        partition: u64,
        req: RaftRpcRequest,
    ) -> BoxFuture<'a, Result<RaftRpcResponse, TransportError>>;
}

/// Feeds an inbound RPC into a local Raft instance and returns its response. The
/// receiving half of any transport — the falcon server handler will call
/// this with the partition's hosted Raft once leader routing mounts the groups.
pub async fn dispatch(
    raft: &openraft::Raft<RaftConfig>,
    req: RaftRpcRequest,
) -> anyhow::Result<RaftRpcResponse> {
    Ok(match req {
        RaftRpcRequest::AppendEntries(r) => {
            RaftRpcResponse::AppendEntries(raft.append_entries(r).await?)
        }
        RaftRpcRequest::Vote(r) => RaftRpcResponse::Vote(raft.vote(r).await?),
        RaftRpcRequest::InstallSnapshot(r) => {
            RaftRpcResponse::InstallSnapshot(raft.install_snapshot(r).await?)
        }
    })
}

/// Cumulative bytes streamed to each replication target during `InstallSnapshot`,
/// keyed by target node id. openraft's leader-side metrics only expose a matched
/// `LogId` for a target, which stays `None` for the *whole* snapshot install — so
/// a large install that outlasts the fixed catch-up ceiling looks indistinguishable
/// from a dead learner and gets guillotined. This byte counter gives the hand-off
/// catch-up loop a leader-observable "install is actively transferring" signal, so
/// it can *extend* the deadline while bytes flow instead of aborting (ADR 0019
/// snapshot-transfer-aware deadline). Written by
/// [`PartitionConnection::install_snapshot`] after each acknowledged chunk; read
/// via [`crate::raft::RaftPartition::snapshot_bytes_sent`].
#[derive(Default)]
pub struct SnapshotSendProgress {
    bytes: Mutex<HashMap<NodeId, u64>>,
}

impl SnapshotSendProgress {
    /// Add an acknowledged chunk's byte count to `target`'s running total.
    fn record(&self, target: NodeId, chunk_len: usize) {
        let mut m = self.bytes.lock().unwrap_or_else(|e| e.into_inner());
        *m.entry(target).or_insert(0) += chunk_len as u64;
    }

    /// Cumulative bytes streamed to `target` so far, or `None` if no snapshot
    /// chunk has ever been sent to it (no install in progress).
    pub fn bytes_sent(&self, target: NodeId) -> Option<u64> {
        self.bytes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&target)
            .copied()
    }
}

/// The openraft [`RaftNetworkFactory`] for one partition: every connection it
/// mints carries that partition's RPCs over the shared [`RaftTransport`].
#[derive(Clone)]
pub struct PartitionNetwork {
    transport: Arc<dyn RaftTransport>,
    partition: u64,
    /// Shared with the owning [`RaftPartition`](crate::raft::RaftPartition) so the
    /// hand-off catch-up loop can observe snapshot-transfer byte progress.
    snapshot_progress: Arc<SnapshotSendProgress>,
}

impl PartitionNetwork {
    pub fn new(
        transport: Arc<dyn RaftTransport>,
        partition: u64,
        snapshot_progress: Arc<SnapshotSendProgress>,
    ) -> Self {
        Self {
            transport,
            partition,
            snapshot_progress,
        }
    }
}

impl RaftNetworkFactory<RaftConfig> for PartitionNetwork {
    type Network = PartitionConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        PartitionConnection {
            transport: self.transport.clone(),
            partition: self.partition,
            target,
            snapshot_progress: self.snapshot_progress.clone(),
        }
    }
}

/// A connection to one target replica of one partition.
pub struct PartitionConnection {
    transport: Arc<dyn RaftTransport>,
    partition: u64,
    target: NodeId,
    snapshot_progress: Arc<SnapshotSendProgress>,
}

impl PartitionConnection {
    /// Maps a transport failure to a retryable openraft [`Unreachable`] error.
    fn unreachable<E: std::error::Error + 'static>(
        &self,
        e: TransportError,
    ) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>> {
        RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e)))
    }

    /// A response of the wrong RPC kind is a protocol bug; surface it as a
    /// retryable network error rather than panicking the raft core.
    fn mismatch<E: std::error::Error + 'static>(
        &self,
    ) -> RPCError<NodeId, BasicNode, RaftError<NodeId, E>> {
        let e = TransportError(format!(
            "raft transport returned a mismatched response kind (partition {}, target {})",
            self.partition, self.target
        ));
        RPCError::Unreachable(Unreachable::new(&NetworkError::new(&e)))
    }
}

impl RaftNetwork<RaftConfig> for PartitionConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<RaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let resp = self
            .transport
            .send(
                self.target,
                self.partition,
                RaftRpcRequest::AppendEntries(req),
            )
            .await
            .map_err(|e| self.unreachable(e))?;
        match resp {
            RaftRpcResponse::AppendEntries(r) => Ok(r),
            _ => Err(self.mismatch()),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let resp = self
            .transport
            .send(self.target, self.partition, RaftRpcRequest::Vote(req))
            .await
            .map_err(|e| self.unreachable(e))?;
        match resp {
            RaftRpcResponse::Vote(r) => Ok(r),
            _ => Err(self.mismatch()),
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<RaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let chunk_len = req.data.len();
        let resp = self
            .transport
            .send(
                self.target,
                self.partition,
                RaftRpcRequest::InstallSnapshot(req),
            )
            .await
            .map_err(|e| self.unreachable(e))?;
        match resp {
            RaftRpcResponse::InstallSnapshot(r) => {
                // Record the acknowledged chunk so the hand-off catch-up loop sees
                // the install actively transferring and extends its deadline.
                self.snapshot_progress.record(self.target, chunk_len);
                Ok(r)
            }
            _ => Err(self.mismatch()),
        }
    }
}

/// A transport that always fails — used for a single-voter group, which never
/// sends an RPC (its quorum is itself), so `send` is unreachable in practice.
pub struct NullTransport;

impl RaftTransport for NullTransport {
    fn send<'a>(
        &'a self,
        target: NodeId,
        partition: u64,
        _req: RaftRpcRequest,
    ) -> BoxFuture<'a, Result<RaftRpcResponse, TransportError>> {
        Box::pin(async move {
            Err(TransportError(format!(
                "no raft transport configured (partition {partition}, target {target})"
            )))
        })
    }
}

/// An in-process transport that routes RPCs directly into peers' Raft instances.
/// Registered handles are looked up by `(partition, node)`; cloning shares the
/// same registry, so all members and the transport see one map.
#[derive(Clone, Default)]
pub struct LocalCluster {
    #[allow(clippy::type_complexity)]
    nodes: Arc<Mutex<HashMap<(u64, NodeId), openraft::Raft<RaftConfig>>>>,
}

impl LocalCluster {
    /// Publishes a node's Raft handle so peers can reach it. Call once per member
    /// after `Raft::new` and before driving the group.
    pub fn register(&self, partition: u64, node: NodeId, raft: openraft::Raft<RaftConfig>) {
        self.nodes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((partition, node), raft);
    }
}

impl RaftTransport for LocalCluster {
    fn send<'a>(
        &'a self,
        target: NodeId,
        partition: u64,
        req: RaftRpcRequest,
    ) -> BoxFuture<'a, Result<RaftRpcResponse, TransportError>> {
        let raft = self
            .nodes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(partition, target))
            .cloned();
        Box::pin(async move {
            let raft = raft.ok_or_else(|| {
                TransportError(format!(
                    "no registered node {target} for partition {partition}"
                ))
            })?;
            dispatch(&raft, req)
                .await
                .map_err(|e| TransportError(e.to_string()))
        })
    }
}

/// The production transport: carries a partition's Raft RPCs to peer nodes over
/// the cluster's existing falcon WebSocket. The target [`NodeId`] is the
/// cluster node id, so it maps straight onto the [`PeerSet`] uplink; the RPC is
/// serialized into a [`nano_falcon_protocol::ClientFrame::Raft`] frame and the
/// peer answers with the serialized [`RaftRpcResponse`] in its `CommandResult`.
#[derive(Clone)]
pub struct PeerTransport {
    peers: PeerSet,
}

impl PeerTransport {
    pub fn new(peers: PeerSet) -> Self {
        Self { peers }
    }
}

impl RaftTransport for PeerTransport {
    fn send<'a>(
        &'a self,
        target: NodeId,
        partition: u64,
        req: RaftRpcRequest,
    ) -> BoxFuture<'a, Result<RaftRpcResponse, TransportError>> {
        Box::pin(async move {
            // Serialize the request straight to a JSON string (no intermediate
            // `serde_json::Value` DOM), then compress it for the wire when the
            // payload is large (big variable blobs); small control RPCs stay raw.
            let json = serde_json::to_string(&req).map_err(|e| TransportError(e.to_string()))?;
            let (rpc, compressed) = encode_rpc_payload(json);
            let link = self
                .peers
                .link(target as u32)
                .await
                .map_err(|e| TransportError(e.to_string()))?;
            let result = link
                .raft_rpc(partition, rpc, compressed)
                .await
                .map_err(|e| TransportError(e.to_string()))?;
            if result.status != 200 {
                return Err(TransportError(format!(
                    "peer raft rpc returned status {} (partition {partition}, target {target})",
                    result.status
                )));
            }
            let body = result
                .body
                .ok_or_else(|| TransportError("peer raft rpc returned no body".into()))?;
            serde_json::from_value(body).map_err(|e| TransportError(e.to_string()))
        })
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn small_payloads_ride_raw_and_round_trip() {
        let json =
            r#"{"Vote":{"vote":{"leader_id":1,"committed":true},"last_log_id":null}}"#.to_string();
        let (payload, zip) = encode_rpc_payload(json.clone());
        assert!(!zip, "small RPC must not be compressed");
        assert_eq!(payload, json);
        assert_eq!(decode_rpc_payload(&payload, zip).unwrap(), json);
    }

    #[test]
    fn large_payloads_compress_and_round_trip() {
        // A big, highly-compressible variable blob (the case this targets).
        let json = format!(r#"{{"AppendEntries":{{"blob":"{}"}}}}"#, "x".repeat(8192));
        assert!(json.len() >= RAFT_RPC_COMPRESS_THRESHOLD);
        let (payload, zip) = encode_rpc_payload(json.clone());
        assert!(zip, "large RPC must be compressed");
        assert!(
            payload.len() < json.len(),
            "compression should shrink the payload"
        );
        assert_eq!(decode_rpc_payload(&payload, zip).unwrap(), json);
    }

    #[test]
    fn decode_passthrough_when_not_compressed() {
        let raw = "{\"k\":1}";
        assert_eq!(decode_rpc_payload(raw, false).unwrap(), raw);
    }

    #[test]
    fn decode_rejects_corrupt_compressed_payload() {
        assert!(decode_rpc_payload("not-valid-base64!!!", true).is_err());
    }

    /// A poisoned progress mutex (a panic while the guard is held mid-record)
    /// must not wedge snapshot progress tracking: `record`/`bytes_sent` recover
    /// the guard instead of panicking on the consensus hot path.
    #[test]
    fn snapshot_send_progress_survives_poisoned_lock() {
        let progress = SnapshotSendProgress::default();
        {
            let bytes: &Mutex<HashMap<NodeId, u64>> = &progress.bytes;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _g = bytes.lock();
                panic!("poison progress");
            }));
        }
        progress.record(1, 512);
        assert_eq!(progress.bytes_sent(1), Some(512));
    }
}
