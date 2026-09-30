//! Peer uplink: a node acting as a falcon **client** to its cluster
//! peers.
//!
//! Stage 1 of the distributed-scaling design (`docs/distributed-scaling-design.md`)
//! makes every node a gateway: a client connects to any one node, which forwards
//! operations it does not own to the node that does. The forwarding transport is
//! the existing falcon WebSocket protocol — a gateway opens a
//! [`PeerLink`] to each peer and drives that peer's engine over the same frames
//! (`CreateInstance`, `CompleteJob`, …) a normal client would send, so no new
//! node-to-node protocol or serialization is introduced.
//!
//! This module provides the transport only: a connection to one peer plus a
//! correlated request/response over it ([`PeerLink::request`]). Routing the
//! right operations to the right peer (create-forward, by-key forward, deploy
//! broadcast, job aggregation) is layered on top in the forwarding seam.
//!
//! Scope of this increment: unary request/response (`CommandResult`-answered
//! frames). Job-push subscription aggregation (`Subscribe`/`Job`) and async
//! `InstanceCompleted` await routing build on the same link later.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use nano_falcon_protocol::{ClientFrame, ReadKind, ServerFrame, UserTaskOp};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use crate::cluster::Topology;

/// Default ceiling on how long a forwarded request waits for its peer's
/// `CommandResult` before giving up. Overridable via `NANOBPMN_PEER_TIMEOUT_MS`.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

/// Default per-dial connect deadline. A peer that is process-killed answers with
/// a fast RST, but a host-down / black-holed peer (firewall drop, dead host)
/// leaves `connect_async` hanging at the OS TCP timeout (~75s). Bounding the
/// connect keeps a probe dial to a dead peer cheap. Override via
/// `NANOBPMN_PEER_CONNECT_TIMEOUT_MS`.
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 2_000;

/// Default circuit-breaker cooldown. Once a peer's dial fails, `link()` fast-fails
/// to it (without redialing) for this window, allowing only a single probe dial
/// per cooldown to detect recovery. This collapses the redial storm a dead node
/// otherwise induces — every survivor-led partition retries AppendEntries to the
/// dead learner ~2×/s, and without the breaker each retry redials, inflating
/// peer-link acquisition cluster-wide. Override via
/// `NANOBPMN_PEER_BREAKER_COOLDOWN_MS`.
const DEFAULT_BREAKER_COOLDOWN_MS: u64 = 500;

/// A failure forwarding a request to a peer.
#[derive(Debug)]
pub enum PeerError {
    /// The WebSocket to the peer could not be established.
    Connect(String),
    /// The link closed (peer down, network drop) before the response arrived.
    Closed,
    /// No response within the request timeout.
    Timeout,
}

impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerError::Connect(e) => write!(f, "peer connect failed: {e}"),
            PeerError::Closed => write!(f, "peer link closed before response"),
            PeerError::Timeout => write!(f, "peer request timed out"),
        }
    }
}

impl std::error::Error for PeerError {}

/// A peer's answer to a forwarded unary command — the `status`/`body` of its
/// `CommandResult` frame, mapped straight back to the originating client.
#[derive(Debug, Clone)]
pub struct PeerResult {
    pub status: u16,
    pub body: Option<Value>,
}

/// Outstanding forwarded requests awaiting a `CommandResult`, keyed by `corr`.
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<PeerResult>>>>;

/// A live falcon connection to one peer node.
///
/// Cheap to clone (shares the underlying socket writer and correlation table).
/// Allocates its own `corr` space, independent of the peer's other clients.
#[derive(Clone)]
pub struct PeerLink {
    /// App-forwarding lane (forwarded creates, jobs, queries, user-task ops).
    out_app: mpsc::Sender<Message>,
    pending_app: Pending,
    /// Dedicated Raft lane: AppendEntries / Vote / InstallSnapshot ride their own
    /// socket so a backlog of forwarded app commands can never delay a
    /// replication RPC past its election deadline (head-of-line isolation).
    out_raft: mpsc::Sender<Message>,
    pending_raft: Pending,
    next_corr: Arc<AtomicU64>,
    connected: Arc<AtomicBool>,
}

impl PeerLink {
    /// Opens a falcon client connection to `base_url` (a peer's HTTP base
    /// URL, e.g. `http://10.0.0.2:8080`). The reader/writer tasks run until the
    /// socket closes, at which point the link is marked disconnected and every
    /// outstanding request fails with [`PeerError::Closed`].
    pub async fn connect(base_url: &str) -> Result<Self, PeerError> {
        let connected = Arc::new(AtomicBool::new(true));
        let pending_app: Pending = Arc::new(Mutex::new(HashMap::new()));
        let pending_raft: Pending = Arc::new(Mutex::new(HashMap::new()));
        // Present the shared cluster secret (ADR 0039) on the intra-cluster
        // handshake so the peer's authenticated `/cluster` channel admits us.
        let secret = nano_falcon_protocol::cluster_secret_from_env();
        // App-forwarding traffic and Raft RPCs ride separate sockets so a backlog
        // of forwarded creates/jobs can never delay an AppendEntries/Vote past its
        // election deadline (head-of-line isolation for the replication transport).
        let out_app = Self::dial(
            &ws_url(base_url),
            pending_app.clone(),
            connected.clone(),
            secret.as_deref(),
        )
        .await?;
        let out_raft = Self::dial(
            &raft_ws_url(base_url),
            pending_raft.clone(),
            connected.clone(),
            secret.as_deref(),
        )
        .await?;

        Ok(Self {
            out_app,
            pending_app,
            out_raft,
            pending_raft,
            next_corr: Arc::new(AtomicU64::new(1)),
            connected,
        })
    }

    /// Opens one falcon socket to `ws_url`, spawning its writer and reader
    /// tasks. Returns the outbound sender; the reader resolves peer responses into
    /// `pending` and flips `connected` to false (failing every waiter) when the
    /// socket drops.
    async fn dial(
        ws_url: &str,
        pending: Pending,
        connected: Arc<AtomicBool>,
        secret: Option<&str>,
    ) -> Result<mpsc::Sender<Message>, PeerError> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = ws_url
            .into_client_request()
            .map_err(|e| PeerError::Connect(e.to_string()))?;
        if let Some(secret) = secret {
            let value = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(secret)
                .map_err(|e| PeerError::Connect(format!("invalid cluster secret: {e}")))?;
            request.headers_mut().insert(
                tokio_tungstenite::tungstenite::http::HeaderName::from_static(
                    nano_falcon_protocol::CLUSTER_SECRET_HEADER,
                ),
                value,
            );
        }
        let (ws, _resp) = match tokio::time::timeout(
            connect_timeout(),
            tokio_tungstenite::connect_async(request),
        )
        .await
        {
            Ok(res) => res.map_err(|e| PeerError::Connect(e.to_string()))?,
            Err(_) => {
                return Err(PeerError::Connect(format!(
                    "connect timed out after {:?}",
                    connect_timeout()
                )));
            }
        };
        // Disable Nagle on the peer socket: the Falcon protocol carries small,
        // latency-sensitive request/response frames (notably Raft AppendEntries),
        // and Nagle + delayed-ACK adds ~40ms per round-trip, collapsing Raft
        // commit throughput. The raft/app RPCs are explicitly framed, so there is
        // no benefit to coalescing them at the TCP layer.
        if let tokio_tungstenite::MaybeTlsStream::Plain(tcp) = ws.get_ref() {
            let _ = tcp.set_nodelay(true);
        }
        let (mut sink, mut stream) = ws.split();

        let (out, mut out_rx) = mpsc::channel::<Message>(1024);

        // Writer: serialize outbound frames onto the socket in submission order.
        tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        // Reader: decode peer frames and resolve the matching pending request.
        let reader_pending = pending.clone();
        let reader_connected = connected.clone();
        tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(Message::Text(txt)) => {
                        if let Ok(sf) = serde_json::from_str::<ServerFrame>(&txt) {
                            route_server_frame(sf, &reader_pending).await;
                        }
                    }
                    Ok(Message::Binary(bin)) => {
                        if let Ok(sf) = serde_json::from_slice::<ServerFrame>(&bin) {
                            route_server_frame(sf, &reader_pending).await;
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    // Ping/Pong/frame: tungstenite answers pings itself.
                    Ok(_) => {}
                }
            }
            // Link is down: fail every waiter so callers don't hang.
            reader_connected.store(false, Ordering::Relaxed);
            let mut map = reader_pending.lock().await;
            for (_, tx) in map.drain() {
                let _ = tx.send(PeerResult {
                    status: 502,
                    body: Some(Value::String("peer link closed".into())),
                });
            }
        });

        Ok(out)
    }

    /// Whether the link is still up.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Forwards a unary command and awaits the peer's `CommandResult`. `build`
    /// receives the freshly-allocated `corr` and must stamp it onto the frame so
    /// the response can be matched back. Fails if the link is down, the frame
    /// cannot be queued, or no response arrives within the request timeout.
    pub async fn request<F>(&self, build: F) -> Result<PeerResult, PeerError>
    where
        F: FnOnce(u64) -> ClientFrame,
    {
        self.request_on(&self.out_app, &self.pending_app, request_timeout(), build)
            .await
    }

    /// Like [`request`](Self::request) but with an explicit per-call deadline,
    /// overriding the default 30s [`request_timeout`]. Used by the write-forward
    /// path so a create/job command routed to a momentarily-leaderless (e.g.
    /// just-failed) partition fails fast and is retried against the new leader,
    /// instead of pinning the caller for 30s.
    pub async fn request_within<F>(
        &self,
        timeout: Duration,
        build: F,
    ) -> Result<PeerResult, PeerError>
    where
        F: FnOnce(u64) -> ClientFrame,
    {
        self.request_on(&self.out_app, &self.pending_app, timeout, build)
            .await
    }

    /// Sends `build(corr)` on the given lane and awaits the peer's matching
    /// `CommandResult`, giving up after `timeout`. Shared by the app-forwarding
    /// lane ([`request`]) and the dedicated Raft lane ([`raft_rpc`]); each lane
    /// carries its own socket and pending table, so neither can stall the other.
    async fn request_on<F>(
        &self,
        out: &mpsc::Sender<Message>,
        pending: &Pending,
        timeout: Duration,
        build: F,
    ) -> Result<PeerResult, PeerError>
    where
        F: FnOnce(u64) -> ClientFrame,
    {
        if !self.is_connected() {
            return Err(PeerError::Closed);
        }
        let corr = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let frame = build(corr);
        let txt = serde_json::to_string(&frame).expect("ClientFrame serializes");

        let (tx, rx) = oneshot::channel();
        pending.lock().await.insert(corr, tx);

        if out.send(Message::Text(txt.into())).await.is_err() {
            pending.lock().await.remove(&corr);
            return Err(PeerError::Closed);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(PeerError::Closed),
            Err(_) => {
                pending.lock().await.remove(&corr);
                Err(PeerError::Timeout)
            }
        }
    }

    /// Sends a fire-and-forget frame on the app lane without registering a pending
    /// correlation or awaiting a reply. Used by the best-effort lease digest
    /// broadcast: a partition leader pushes its current leases to a follower and
    /// never waits for an answer (loss/staleness is safe by construction). Returns
    /// `Err(Closed)` only if the link is down or the writer queue is full/closed.
    pub async fn send_oneway(&self, frame: ClientFrame) -> Result<(), PeerError> {
        if !self.is_connected() {
            return Err(PeerError::Closed);
        }
        let txt = serde_json::to_string(&frame).expect("ClientFrame serializes");
        self.out_app
            .send(Message::Text(txt.into()))
            .await
            .map_err(|_| PeerError::Closed)
    }

    /// Broadcasts a best-effort lease digest for `partition` to this follower.
    pub async fn send_lease_digest(
        &self,
        partition: u64,
        leases: Vec<(u64, u64)>,
        sent_at: u64,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::LeaseDigest {
            partition,
            leases,
            sent_at,
        })
        .await
    }

    /// Fire-and-forget a best-effort retirement digest for `partition` to this
    /// follower: the instance keys the leader has completed + exporter-evicted, so
    /// the follower drops them from its replica engine. See
    /// [`ClientFrame::RetirementDigest`].
    pub async fn send_retirement_digest(
        &self,
        partition: u64,
        keys: Vec<u64>,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::RetirementDigest { partition, keys })
            .await
    }

    /// Fire-and-forget the retirement low-water mark to this peer (a follower
    /// replica), which drops every resident replica instance below it. The
    /// loss-tolerant reconciliation backstop for the per-key retirement digest.
    /// See [`ClientFrame::RetirementWatermark`].
    pub async fn send_retirement_watermark(
        &self,
        partition: u64,
        low_water: u64,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::RetirementWatermark {
            partition,
            low_water,
        })
        .await
    }

    /// Fire-and-forget a leader-durable promotion announcement to this peer (ADR
    /// 0003): this node has app-promoted itself leader of `partition` at `epoch`
    /// after the previous sole-voter leader was lost. See [`ClientFrame::Promote`].
    pub async fn send_promote(
        &self,
        partition: u64,
        epoch: u64,
        leader_node: u64,
        leader_addr: String,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::Promote {
            partition,
            epoch,
            leader_node,
            leader_addr,
        })
        .await
    }

    /// Request/response variant of [`send_promote`](Self::send_promote) (issue
    /// #228): tell this peer we promoted ourselves leader of `partition` at `epoch`
    /// and WAIT until it has adopted the epoch and rebuilt as our receiver before
    /// returning. The promoting leader gates its `add_learner` on this ack so it
    /// never ships the fresh lineage into the peer's still-live prior-epoch group
    /// (where two committed `idx-1` entries would collide). `Ok` iff the peer
    /// confirmed the step-down within `timeout`; on timeout/link failure the caller
    /// defers the learner add to the recovery tick's reconcile.
    pub async fn promote_sync(
        &self,
        partition: u64,
        epoch: u64,
        leader_node: u64,
        leader_addr: String,
        timeout: Duration,
    ) -> Result<(), PeerError> {
        self.request_within(timeout, |corr| ClientFrame::PromoteSync {
            corr,
            partition,
            epoch,
            leader_node,
            leader_addr,
        })
        .await
        .map(|_| ())
    }

    /// Fire-and-forget the runtime SLA-mode switch to this peer, so the cluster
    /// applies a single uniform admission policy. `mode` is the
    /// `SlaMode` (`nano-server-runtime` `backpressure`) string. See
    /// [`ClientFrame::SetSlaMode`].
    pub async fn send_sla_mode(&self, mode: String) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::SetSlaMode { mode }).await
    }

    /// Fire-and-forget this node's current create-load index to a peer (ADR 0014
    /// `balanced`), so the peer's weighted placement can steer creates toward
    /// nodes with headroom. See [`ClientFrame::PressureReport`].
    pub async fn send_pressure(&self, node: u32, load: i64) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::PressureReport { node, load })
            .await
    }

    /// Fire-and-forget a promotion-epoch solicitation to this peer (leader-durable
    /// reclaim): ask it to re-announce the promotion epochs it currently leads so
    /// this node can reclaim its owned partitions at incumbent+1. See
    /// [`ClientFrame::SolicitPromotions`].
    pub async fn send_solicit_promotions(&self, from_node: u64) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::SolicitPromotions { from_node })
            .await
    }

    /// Fire-and-forget a leadership-hand-off request to this peer (the incumbent
    /// leader), asking it to hand `partition` back to `requester_node` via an
    /// openraft membership change. See [`ClientFrame::RequestHandoff`].
    pub async fn send_request_handoff(
        &self,
        partition: u64,
        requester_node: u64,
        requester_addr: String,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::RequestHandoff {
            partition,
            requester_node,
            requester_addr,
        })
        .await
    }

    /// Fire-and-forget a hand-off acknowledgement to the requesting owner (accept
    /// + our epoch, or decline). See [`ClientFrame::HandoffAck`].
    pub async fn send_handoff_ack(
        &self,
        partition: u64,
        incumbent_epoch: u64,
        accepted: bool,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::HandoffAck {
            partition,
            incumbent_epoch,
            accepted,
        })
        .await
    }

    /// Fire-and-forget a hand-off completion to the requesting owner (it now leads
    /// `partition` at `epoch`). See [`ClientFrame::HandoffComplete`].
    pub async fn send_handoff_complete(
        &self,
        partition: u64,
        epoch: u64,
        new_leader: u64,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::HandoffComplete {
            partition,
            epoch,
            new_leader,
        })
        .await
    }

    /// Fire-and-forget a hand-off failure to the requesting owner. See
    /// [`ClientFrame::HandoffFailed`].
    pub async fn send_handoff_failed(
        &self,
        partition: u64,
        joint_suspected: bool,
        reason: String,
    ) -> Result<(), PeerError> {
        self.send_oneway(ClientFrame::HandoffFailed {
            partition,
            joint_suspected,
            reason,
        })
        .await
    }

    /// Forwards a `createProcessInstance` to this peer (it creates on one of its
    /// own partitions). `await_completion` is intentionally unsupported here —
    /// it resolves over an async `InstanceCompleted` frame, wired in a later
    /// increment.
    pub async fn create_instance(
        &self,
        process_definition_id: Option<String>,
        process_definition_key: Option<String>,
        variables: Option<serde_json::Map<String, Value>>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::CreateInstance {
            corr,
            process_definition_id,
            process_definition_key,
            variables,
            await_completion: Some(false),
            fetch_variables: None,
            request_timeout: None,
        })
        .await
    }

    /// Forwards a `completeJob` to the peer that owns the job's partition.
    pub async fn complete_job(
        &self,
        job_key: String,
        lease_token: Option<String>,
        variables: Option<serde_json::Map<String, Value>>,
        adhoc_result: Option<nanobpmn_engine_core::AdHocJobResult>,
        task_result: Option<nanobpmn_engine_core::TaskListenerJobResult>,
        business_id: Option<String>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::CompleteJob {
            corr,
            job_key,
            lease_token,
            job_lease_token: None,
            variables,
            adhoc_result,
            task_result,
            business_id,
        })
        .await
    }

    /// Asks the peer to activate up to `max_jobs` of `job_type` on its OWN
    /// partitions for `worker` (job-stream aggregation). The peer leases the jobs
    /// under `timeout` and replies a `CommandResult` whose body is the JSON array
    /// of `ActivatedJobResult`.
    pub async fn activate_jobs(
        &self,
        job_type: String,
        worker: String,
        max_jobs: i64,
        timeout: u64,
        fetch_variable: Option<Vec<String>>,
        with_lease: bool,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::ActivateJobs {
            corr,
            job_type,
            worker,
            max_jobs,
            timeout: Some(timeout),
            fetch_variable,
            with_lease,
        })
        .await
    }

    /// Forwards a GET-by-key read to the peer that owns the key's partition
    /// (query forwarding). The peer answers from its local read model.
    pub async fn get_by_key(&self, kind: ReadKind, key: u64) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::GetByKey {
            corr,
            kind,
            key,
        })
        .await
    }

    /// Carries one Raft RPC (AppendEntries/Vote/InstallSnapshot, serialized to
    /// `rpc`) for `partition`'s replica group to this peer, which hosts a replica
    /// of that partition. Answered by a `CommandResult` whose body is the
    /// serialized `RaftRpcResponse`. This is the falcon binding of the
    /// per-partition Raft network (stage 3 leader routing).
    pub async fn raft_rpc(
        &self,
        partition: u64,
        rpc: String,
        zip: bool,
    ) -> Result<PeerResult, PeerError> {
        self.request_on(
            &self.out_raft,
            &self.pending_raft,
            request_timeout(),
            |corr| ClientFrame::Raft {
                corr,
                partition,
                rpc,
                zip,
            },
        )
        .await
    }

    /// Forwards a user-task by-key mutation to the peer that owns the task's
    /// partition. `payload` is the original REST request body.
    pub async fn forward_user_task(
        &self,
        op: UserTaskOp,
        user_task_key: String,
        payload: Option<Value>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::ForwardUserTask {
                corr,
                op,
                user_task_key,
                payload,
            }
        })
        .await
    }

    /// Forwards the external "activate ad-hoc activities" mutation (#614 gap 3)
    /// to the peer that owns the ad-hoc container's partition. `payload` is the
    /// original REST request body.
    pub async fn forward_ad_hoc_activation(
        &self,
        ad_hoc_instance_key: String,
        payload: Option<Value>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::ForwardAdHocActivation {
                corr,
                ad_hoc_instance_key,
                payload,
            }
        })
        .await
    }

    /// Forwards a client deploy to this peer (the deployment-partition owner),
    /// which processes it centrally and broadcasts it. Used when a gateway that
    /// does not own partition 0 receives a deploy.
    pub async fn deploy(
        &self,
        resources: Vec<(String, String)>,
        tenant_id: Option<String>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::Deploy {
            corr,
            resources,
            tenant_id,
        })
        .await
    }

    /// Broadcasts an already-minted deployment's `ProcessDeployed` events to this
    /// peer so it durably installs the definition(s) on its owned partitions.
    pub async fn install_deployment(
        &self,
        events: Vec<nanobpmn_engine_core::Event>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::InstallDeployment { corr, events })
            .await
    }

    /// Fans a published message out to this peer so it correlates against the
    /// subscriptions on its owned partitions. The peer answers with
    /// `{messageKey, correlatedInstanceKey}`.
    pub async fn publish_message(
        &self,
        name: String,
        correlation_key: String,
        variables: Option<serde_json::Map<String, Value>>,
        business_id: Option<String>,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::PublishMessage {
            corr,
            name,
            correlation_key,
            variables,
            business_id,
        })
        .await
    }

    /// Forwards a `failJob` to the peer that owns the job's partition.
    pub async fn fail_job(
        &self,
        job_key: String,
        lease_token: Option<String>,
        retries: i32,
        error_message: String,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::FailJob {
            corr,
            job_key,
            lease_token,
            job_lease_token: None,
            retries: Some(retries),
            error_message: Some(error_message),
        })
        .await
    }

    /// Forwards a `throwError` to the peer that owns the job's partition.
    pub async fn throw_error(
        &self,
        job_key: String,
        lease_token: Option<String>,
        error_code: String,
        error_message: String,
        variables: Option<serde_json::Map<String, Value>>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::ThrowError {
            corr,
            job_key,
            lease_token,
            job_lease_token: None,
            error_code,
            error_message: Some(error_message),
            variables,
        })
        .await
    }

    /// Forwards a `cancelProcessInstance` to the peer that owns the instance.
    pub async fn cancel_instance(&self, instance_key: String) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::CancelInstance {
            corr,
            instance_key,
        })
        .await
    }

    /// Forwards a `suspendProcessInstance` to the peer that owns the instance.
    pub async fn suspend_instance(&self, instance_key: String) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::SuspendInstance { corr, instance_key }
        })
        .await
    }

    /// Forwards a `resumeProcessInstance` to the peer that owns the instance.
    pub async fn resume_instance(&self, instance_key: String) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::ResumeInstance {
            corr,
            instance_key,
        })
        .await
    }

    /// Forwards a `migrateProcessInstance` to the peer that owns the instance.
    pub async fn migrate_instance(
        &self,
        instance_key: String,
        target_process_definition_key: String,
        mapping_instructions: Vec<(String, String)>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::MigrateInstance {
                corr,
                instance_key,
                target_process_definition_key,
                mapping_instructions,
            }
        })
        .await
    }

    /// Forwards a cross-partition subscription follow-up event (a
    /// `MessageSubscriptionOpening` or `RemoteMessageCorrelation`) to the peer
    /// that owns the target partition, so it applies the corresponding routed
    /// command on its own engine. Answered by a `CommandResult` (200).
    pub async fn route_subscription(
        &self,
        event: nanobpmn_engine_core::Event,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::RouteSubscription { corr, event })
            .await
    }

    /// Forwards a job-retries update to the peer that owns the job's partition.
    pub async fn update_job_retries(
        &self,
        job_key: String,
        lease_token: Option<String>,
        retries: i32,
        operation_reference: Option<i64>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::UpdateJobRetries {
                corr,
                job_key,
                lease_token,
                job_lease_token: None,
                retries,
                operation_reference,
            }
        })
        .await
    }

    /// Forwards a job lock-extension (timeout) to the peer that owns the job's
    /// partition.
    pub async fn update_job_timeout(
        &self,
        job_key: String,
        lease_token: Option<String>,
        timeout: i64,
        operation_reference: Option<i64>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::UpdateJobTimeout {
                corr,
                job_key,
                lease_token,
                job_lease_token: None,
                timeout,
                operation_reference,
            }
        })
        .await
    }

    pub async fn update_job(
        &self,
        job_key: String,
        retries: Option<i32>,
        timeout: Option<i64>,
        operation_reference: Option<i64>,
        lease_token: Option<String>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::UpdateJob {
            corr,
            job_key,
            retries,
            timeout,
            operation_reference,
            lease_token,
            job_lease_token: None,
        })
        .await
    }

    pub async fn forward_agent_instance(
        &self,
        agent_instance_key: Option<String>,
        body: Value,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::ForwardAgentInstance {
                corr,
                agent_instance_key,
                body,
            }
        })
        .await
    }

    /// Forwards an incident resolution to the peer that owns the incident.
    pub async fn resolve_incident(
        &self,
        incident_key: String,
        operation_reference: Option<i64>,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| {
            ClientFrame::ResolveIncident {
                corr,
                incident_key,
                operation_reference,
            }
        })
        .await
    }

    /// Forwards a by-key variable merge to the peer that owns the scope.
    pub async fn set_variables(
        &self,
        scope_key: String,
        variables: Option<serde_json::Map<String, Value>>,
        local: bool,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(fast_forward_timeout(), |corr| ClientFrame::SetVariables {
            corr,
            scope_key,
            variables,
            local,
        })
        .await
    }

    /// Forwards a `createProcessInstance` to this peer for cluster-wide create
    /// placement. The peer creates on one of its own partitions and answers with
    /// the full `CreateProcessInstanceResult` JSON.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_create(
        &self,
        process_definition_id: Option<String>,
        process_definition_key: Option<String>,
        variables: Option<serde_json::Map<String, Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        await_completion: bool,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
        origin_protocol: &str,
    ) -> Result<PeerResult, PeerError> {
        self.request(|corr| ClientFrame::ForwardCreate {
            corr,
            process_definition_id,
            process_definition_key,
            variables,
            tags,
            business_id,
            await_completion,
            fetch_variables,
            request_timeout,
            origin_protocol: Some(origin_protocol.to_string()),
        })
        .await
    }

    /// Like [`forward_create`](Self::forward_create) but bounded by an explicit
    /// `deadline` instead of the 30s default. The write-forward path uses a short
    /// deadline so a create routed to a just-failed partition leader returns
    /// `PeerError::Timeout` quickly and can be retried against the newly elected
    /// leader, rather than pinning the producer connection until the partition
    /// recovers. Only safe for non-`await_completion` creates, where the response
    /// arrives as soon as the instance commits.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_create_within(
        &self,
        deadline: Duration,
        process_definition_id: Option<String>,
        process_definition_key: Option<String>,
        variables: Option<serde_json::Map<String, Value>>,
        tags: Vec<String>,
        business_id: Option<String>,
        fetch_variables: Option<Vec<String>>,
        request_timeout: Option<i64>,
        origin_protocol: &str,
    ) -> Result<PeerResult, PeerError> {
        self.request_within(deadline, |corr| ClientFrame::ForwardCreate {
            corr,
            process_definition_id,
            process_definition_key,
            variables,
            tags,
            business_id,
            await_completion: false,
            fetch_variables,
            request_timeout,
            origin_protocol: Some(origin_protocol.to_string()),
        })
        .await
    }
}

/// Resolves a `CommandResult`/`InstanceCompleted` to its waiting request; other
/// frames (job pushes, credits, heartbeats) are not used by the unary uplink.
async fn route_server_frame(frame: ServerFrame, pending: &Pending) {
    match frame {
        ServerFrame::CommandResult { corr, status, body } => {
            if let Some(tx) = pending.lock().await.remove(&corr) {
                let _ = tx.send(PeerResult { status, body });
            }
        }
        // Async await-completion: surfaces as a 200 with the completion payload.
        ServerFrame::InstanceCompleted {
            corr,
            process_instance_key,
            process_completed,
            variables,
        } => {
            if let Some(tx) = pending.lock().await.remove(&corr) {
                let _ = tx.send(PeerResult {
                    status: 200,
                    body: Some(serde_json::json!({
                        "processInstanceKey": process_instance_key,
                        "processCompleted": process_completed,
                        "variables": variables,
                    })),
                });
            }
        }
        _ => {}
    }
}

/// The set of falcon uplinks to a node's cluster peers, built from the
/// [`Topology`]. The forwarding seam asks it for the link to a partition's owning
/// node; links are established lazily on first use and re-established
/// transparently after a drop, so a peer that is briefly down does not need a
/// restart to rejoin.
///
/// A single-node cluster has no peers, so this is empty and never dialed —
/// preserving the zero-overhead single-node path.
#[derive(Clone)]
pub struct PeerSet {
    topology: Topology,
    /// One slot per node id; `Some` once a link has been established. Guarded by
    /// an async mutex so concurrent forwards to the same peer share one dial.
    links: Arc<Mutex<HashMap<u32, PeerLink>>>,
    /// Fault-injection set: node ids treated as unreachable. Empty in production
    /// (never populated outside tests); lets a failover test faithfully simulate
    /// a peer's death from a survivor's point of view without having to tear down
    /// the dead node's already-accepted connections (which `axum::serve` drives on
    /// detached per-connection tasks that outlive an aborted serve task).
    unreachable: Arc<Mutex<HashSet<u32>>>,
    /// Per-peer reachability circuit-breaker. Once a dial to a peer fails, the
    /// peer is marked down and `link()` fast-fails to it — without redialing — for
    /// a cooldown, admitting only one probe dial per cooldown to detect recovery.
    /// This kills the redial storm a dead node otherwise induces (every
    /// survivor-led partition retries AppendEntries to the dead learner ~2×/s;
    /// without the breaker each retry redials, inflating peer-link acquisition
    /// cluster-wide — the measured onset amplifier of the node-down oscillation).
    breaker: Arc<Mutex<HashMap<u32, Breaker>>>,
    /// Per-peer connect serialization. Concurrent callers for the *same* peer
    /// share one in-flight dial, but the (possibly slow) connect is awaited under
    /// this per-peer lock — NOT under the global `links` mutex — so a stalled dial
    /// to one peer never freezes cache hits or dials to other peers. This removes
    /// the connect-under-global-lock head-of-line stall.
    connect_locks: Arc<Mutex<HashMap<u32, Arc<Mutex<()>>>>>,
}

/// Circuit-breaker state for one down peer.
struct Breaker {
    /// When the peer was first observed down (diagnostics only).
    down_since: std::time::Instant,
    /// Start of the current cooldown; the next probe dial is admitted once this
    /// is at least `breaker_cooldown()` old. Reserving it (setting it to `now`)
    /// under the breaker lock makes "one probe per cooldown" race-free.
    last_probe: std::time::Instant,
}

impl PeerSet {
    /// Builds the uplink set for `topology`. No connections are opened until a
    /// peer is first needed.
    pub fn new(topology: Topology) -> Self {
        Self {
            topology,
            links: Arc::new(Mutex::new(HashMap::new())),
            unreachable: Arc::new(Mutex::new(HashSet::new())),
            breaker: Arc::new(Mutex::new(HashMap::new())),
            connect_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Whether this node has any peers (false for a single-node cluster).
    pub fn has_peers(&self) -> bool {
        !self.topology.is_single_node()
    }

    /// Fault injection (tests only): mark `node_id` unreachable, dropping any
    /// cached link so subsequent `link`/reachability probes fail as if the peer
    /// had died. Production code never calls this.
    pub async fn fail_node(&self, node_id: u32) {
        self.unreachable.lock().await.insert(node_id);
        self.links.lock().await.remove(&node_id);
    }

    /// Fault injection (tests only): clear a prior `fail_node`, restoring
    /// reachability so the peer can be dialed again (models a node rejoining
    /// after an outage). Production code never calls this.
    pub async fn heal_node(&self, node_id: u32) {
        self.unreachable.lock().await.remove(&node_id);
        self.breaker.lock().await.remove(&node_id);
    }

    /// Returns a live link to peer `node_id`, dialing it if there is no cached
    /// link or the cached one has dropped. Concurrent callers for the same peer
    /// share the single in-flight dial (serialized by the map lock).
    pub async fn link(&self, node_id: u32) -> Result<PeerLink, PeerError> {
        let link_start = std::time::Instant::now();
        let result = self.link_inner(node_id).await;
        crate::metrics::record_peer_link(link_start.elapsed());
        result
    }

    async fn link_inner(&self, node_id: u32) -> Result<PeerLink, PeerError> {
        if self.unreachable.lock().await.contains(&node_id) {
            return Err(PeerError::Connect(format!(
                "node {node_id} marked unreachable (fault injection)"
            )));
        }
        // Fast path: a cached, live link. Only the global map lock is held, and
        // never across a connect, so this can never be blocked by a stalled dial.
        if let Some(link) = self.cached_live_link(node_id).await {
            return Ok(link);
        }
        // Circuit-breaker fast path removed here: the admit check *reserves* the
        // cooldown probe (advances `last_probe`), so it must run exactly once per
        // dial attempt. Running it here as well as under the lock would make the
        // reserving caller fail its own second check and never dial. We instead
        // gate once, under the connect lock, below.
        //
        // Serialize the dial per-peer WITHOUT the global `links` lock, so a slow
        // connect to this peer cannot freeze cache hits or dials to other peers.
        let connect_lock = self.connect_lock_for(node_id).await;
        let _guard = connect_lock.lock().await;
        // Re-check: another caller may have connected while we waited on the lock.
        if let Some(link) = self.cached_live_link(node_id).await {
            return Ok(link);
        }
        // Circuit-breaker gate, *under* the connect lock. Placing it here (rather
        // than before the lock) collapses the redial storm to a dead peer to one
        // dial per cooldown even on the first failure wave: when a healthy peer
        // dies, a whole concurrent burst of callers passes `cached_live_link` and
        // queues on the lock; the first to acquire it dials and — on failure —
        // marks the peer down, so every subsequent caller in the burst re-checks
        // here, finds the breaker open, and fast-fails instead of redialing.
        if !self.breaker_admit_dial(node_id).await {
            return Err(PeerError::Connect(format!(
                "node {node_id} circuit-open (unreachable, cooling down)"
            )));
        }
        let addr = self
            .topology
            .peer_addr(node_id)
            .ok_or_else(|| PeerError::Connect(format!("no address for node {node_id}")))?;
        // Onset-diagnosis instrument: count every dial to a peer, split by outcome.
        // A survivor's redial rate to a *dead* peer is the "wasted send work" signal.
        let dial = PeerLink::connect(addr).await;
        crate::metrics::record_peer_connect_attempt(node_id, dial.is_ok());
        match dial {
            Ok(link) => {
                self.breaker_clear(node_id).await;
                self.links.lock().await.insert(node_id, link.clone());
                Ok(link)
            }
            Err(e) => {
                self.breaker_mark_down(node_id).await;
                Err(e)
            }
        }
    }

    /// Returns the cached link to `node_id` iff it exists and is still connected;
    /// evicts a stale (dropped) link. Holds only the global map lock, briefly.
    async fn cached_live_link(&self, node_id: u32) -> Option<PeerLink> {
        let mut links = self.links.lock().await;
        if let Some(existing) = links.get(&node_id) {
            if existing.is_connected() {
                return Some(existing.clone());
            }
            links.remove(&node_id);
        }
        None
    }

    /// Circuit-breaker gate. Returns `true` if a dial should proceed:
    /// - peer not marked down (healthy) → always;
    /// - peer down but its cooldown has elapsed → admits exactly one probe by
    ///   reserving the cooldown (advancing `last_probe`) under the lock;
    /// - peer down and mid-cooldown → `false` (fast-fail, no redial).
    async fn breaker_admit_dial(&self, node_id: u32) -> bool {
        let mut breaker = self.breaker.lock().await;
        match breaker.get_mut(&node_id) {
            None => true,
            Some(state) => {
                if state.last_probe.elapsed() >= breaker_cooldown() {
                    state.last_probe = std::time::Instant::now();
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Marks `node_id` down (a dial just failed), arming the cooldown. Keeps the
    /// existing `down_since`/`last_probe` if the peer was already flagged.
    async fn breaker_mark_down(&self, node_id: u32) {
        let now = std::time::Instant::now();
        self.breaker.lock().await.entry(node_id).or_insert(Breaker {
            down_since: now,
            last_probe: now,
        });
    }

    /// Clears `node_id`'s breaker (a dial just succeeded — the peer is back).
    async fn breaker_clear(&self, node_id: u32) {
        if let Some(state) = self.breaker.lock().await.remove(&node_id) {
            tracing::info!(
                node = node_id,
                down_ms = state.down_since.elapsed().as_millis() as u64,
                "peer circuit-breaker closed: node {node_id} reachable again"
            );
        }
    }

    /// Fetches (or lazily creates) the per-peer connect serialization lock.
    async fn connect_lock_for(&self, node_id: u32) -> Arc<Mutex<()>> {
        self.connect_locks
            .lock()
            .await
            .entry(node_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

/// Maps a peer's HTTP base URL to its intra-cluster falcon WebSocket URL.
/// `http://h:p` → `ws://h:p/cluster`, `https://…` → `wss://…`.
///
/// Targets the authenticated `/cluster` channel (ADR 0039), not the public
/// `/falcon` client channel: peer traffic carries the full intra-cluster
/// protocol, which the public channel deliberately refuses.
fn ws_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    let ws_base = if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        // Assume a bare host:port is plaintext.
        format!("ws://{trimmed}")
    };
    format!("{ws_base}/cluster")
}

/// The dedicated Raft-lane socket URL: the falcon WS tagged `?raft=1`.
/// The tag lets the peer (and operators reading logs) tell the replication
/// socket apart from app-forwarding sockets; functionally the server serves both
/// identically, but isolating Raft RPCs on their own connection keeps them clear
/// of any app-command backlog.
fn raft_ws_url(base_url: &str) -> String {
    format!("{}?raft=1", ws_url(base_url))
}

fn request_timeout() -> Duration {
    let ms = std::env::var("NANOBPMN_PEER_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Per-dial connect deadline (see [`DEFAULT_CONNECT_TIMEOUT_MS`]).
fn connect_timeout() -> Duration {
    let ms = std::env::var("NANOBPMN_PEER_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Circuit-breaker cooldown between probe dials to a down peer (see
/// [`DEFAULT_BREAKER_COOLDOWN_MS`]).
fn breaker_cooldown() -> Duration {
    let ms = std::env::var("NANOBPMN_PEER_BREAKER_COOLDOWN_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_BREAKER_COOLDOWN_MS);
    Duration::from_millis(ms)
}

/// Per-call deadline for *fast*, non-await peer forwards (job complete/fail/throw,
/// activation pulls, by-key reads, simple mutations). Env
/// `NANOBPMN_WRITE_FORWARD_TIMEOUT_MS` (default 2500ms) — a little above the Raft
/// election ceiling so a forward racing a leader failure fails fast and is retried
/// against the new leader (or, for activation, simply skipped that cycle), instead
/// of pinning the caller for the 30s general [`request_timeout`]. The only forward
/// that keeps the long timeout is an `await_completion` create, which legitimately
/// blocks until the instance finishes.
fn fast_forward_timeout() -> Duration {
    let ms = std::env::var("NANOBPMN_WRITE_FORWARD_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(2500);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_maps_scheme_and_appends_path() {
        assert_eq!(ws_url("http://10.0.0.2:8080"), "ws://10.0.0.2:8080/cluster");
        assert_eq!(
            ws_url("http://10.0.0.2:8080/"),
            "ws://10.0.0.2:8080/cluster"
        );
        assert_eq!(ws_url("https://node:443"), "wss://node:443/cluster");
        assert_eq!(ws_url("host:9000"), "ws://host:9000/cluster");
    }

    /// A forwarded request to an unreachable peer fails cleanly (connect error),
    /// never hangs.
    #[tokio::test]
    async fn peer_link_connect_failure_is_reported() {
        // Port 1 is privileged/unused — connect must fail, not hang.
        let err = PeerLink::connect("http://127.0.0.1:1").await;
        assert!(matches!(err, Err(PeerError::Connect(_))));
    }

    /// A single-node cluster has no peers, so `PeerSet` is inert.
    #[tokio::test]
    async fn peer_set_single_node_has_no_peers() {
        let peers = PeerSet::new(crate::cluster::Topology::single(1));
        assert!(!peers.has_peers());
        assert!(
            peers.link(0).await.is_err(),
            "single node has no peer to dial"
        );
    }

    /// Two-node topology whose peer node 1 points at a dead port, for exercising
    /// the reachability circuit-breaker without a live peer.
    fn peers_with_dead_peer() -> PeerSet {
        PeerSet::new(crate::cluster::Topology {
            node_id: 0,
            // Port 1 is unused → connect refuses fast (ECONNREFUSED).
            peers: vec![
                "http://self-unused".to_string(),
                "http://127.0.0.1:1".to_string(),
            ],
            num_partitions: 4,
            replication_factor: 1,
        })
    }

    /// The first dial to a dead peer really dials (and fails); an immediate second
    /// dial is fast-failed by the open circuit — no redial — so a survivor cannot
    /// storm a dead peer with reconnects. The two errors are distinguishable: a
    /// real connect failure vs. the circuit-open short-circuit.
    #[tokio::test]
    async fn breaker_fast_fails_repeated_dials_to_a_dead_peer() {
        let peers = peers_with_dead_peer();

        let first_msg = match peers.link(1).await {
            Ok(_) => panic!("dead peer: first dial must fail"),
            Err(e) => e.to_string(),
        };
        assert!(
            !first_msg.contains("circuit-open"),
            "first call must actually dial, got: {first_msg}"
        );

        let second_msg = match peers.link(1).await {
            Ok(_) => panic!("dead peer: second dial must fail"),
            Err(e) => e.to_string(),
        };
        assert!(
            second_msg.contains("circuit-open"),
            "second call within cooldown must short-circuit, got: {second_msg}"
        );
    }

    /// After the cooldown elapses, exactly one probe dial is admitted again (so a
    /// recovered peer is rediscovered), rather than staying open forever.
    #[tokio::test]
    async fn breaker_reprobes_after_cooldown() {
        let peers = peers_with_dead_peer();

        assert!(peers.link(1).await.is_err(), "arm the breaker");
        let mid = match peers.link(1).await {
            Ok(_) => panic!("mid-cooldown dial must fail"),
            Err(e) => e.to_string(),
        };
        assert!(
            mid.contains("circuit-open"),
            "mid-cooldown call should short-circuit"
        );

        // Past the default cooldown, a probe dial is admitted (and fails for real
        // again, since the peer is still dead) — NOT short-circuited.
        tokio::time::sleep(breaker_cooldown() + Duration::from_millis(100)).await;
        let probe = match peers.link(1).await {
            Ok(_) => panic!("post-cooldown probe dial must fail (peer still dead)"),
            Err(e) => e.to_string(),
        };
        assert!(
            !probe.contains("circuit-open"),
            "post-cooldown call must re-dial, got: {probe}"
        );
    }

    /// A *concurrent burst* of dials to a peer that was still healthy at entry
    /// (breaker empty, so the pre-lock fast path admits everyone) collapses to a
    /// single real dial: the first caller to win the per-peer connect lock dials
    /// and — on failure — marks the peer down, and every other caller in the burst
    /// then re-checks the breaker *under the lock* and short-circuits. Regression
    /// test for the redial storm that occurred when the breaker was checked only
    /// *before* acquiring the connect lock: the whole burst passed the gate before
    /// anyone marked the peer down, so all of them dialled.
    #[tokio::test]
    async fn breaker_collapses_a_concurrent_dial_burst_to_one_dial() {
        let peers = std::sync::Arc::new(peers_with_dead_peer());

        let mut handles = Vec::new();
        for _ in 0..16 {
            let p = std::sync::Arc::clone(&peers);
            handles.push(tokio::spawn(async move {
                p.link(1).await.err().map(|e| e.to_string())
            }));
        }

        let mut real_dials = 0;
        let mut short_circuits = 0;
        for h in handles {
            let msg = h
                .await
                .expect("task panicked")
                .expect("dead peer: every dial must fail");
            if msg.contains("circuit-open") {
                short_circuits += 1;
            } else {
                real_dials += 1;
            }
        }

        assert_eq!(
            real_dials, 1,
            "exactly one caller in the burst may actually dial a dead peer"
        );
        assert_eq!(
            short_circuits, 15,
            "every other caller in the burst must short-circuit under the connect lock"
        );
    }
}
