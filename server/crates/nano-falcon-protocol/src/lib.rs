//! Falcon protocol wire types (ADR 0064 Phase 2).
//!
//! The pure, `ServerImpl`-free surface of Nano's Falcon protocol (§13 of
//! `docs/falcon-design.md`): the [`ClientFrame`]/[`ServerFrame`] tagged unions
//! exchanged over the WebSocket, their payload enums ([`Channel`],
//! [`ReadKind`], [`UserTaskOp`]), and the intra-cluster handshake auth constant
//! ([`CLUSTER_SECRET_HEADER`]) / secret reader ([`cluster_secret_from_env`]).
//!
//! This crate exists so the two *frames-only* consumers — the peer uplink and
//! the raft transport in `nano-server-raft` — depend on the wire vocabulary
//! without pulling in the falcon **handlers/dispatcher**, which call 26
//! `ServerImpl` methods and therefore stay in the gateway binary
//! (`server/src/falcon.rs`, the Phase 3 seam). The binary re-exports everything
//! here (`pub(crate) use nano_falcon_protocol::*;`) so its handler code keeps
//! its unqualified `ClientFrame`/`ServerFrame` paths unchanged.

use nanobpmn_engine_core::Event;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[cfg(test)]
mod lease_contract_tests {
    use super::*;

    #[test]
    fn atomic_job_updates_preserve_empty_changesets_and_lease() {
        let wire = serde_json::json!({
            "type": "updateJob", "corr": 1, "jobKey": "42", "leaseToken": "opaque"
        });
        let frame: ClientFrame = serde_json::from_value(wire).unwrap();
        let decoded = serde_json::to_value(frame).unwrap();
        assert_eq!(decoded["leaseToken"], "opaque");
        assert!(decoded["retries"].is_null());
        assert!(decoded["timeout"].is_null());
    }

    #[test]
    fn activation_frames_preserve_lease_opt_in() {
        for value in [
            serde_json::json!({"type":"subscribe","jobType":"work","withLease":true}),
            serde_json::json!({"type":"activateJobs","corr":1,"jobType":"work","worker":"W","maxJobs":1,"withLease":true}),
        ] {
            let frame: ClientFrame = serde_json::from_value(value).unwrap();
            assert_eq!(serde_json::to_value(frame).unwrap()["withLease"], true);
        }
    }

    #[test]
    fn mutation_frames_preserve_opaque_lease_tokens() {
        for mut wire in [
            serde_json::json!({"type": "completeJob"}),
            serde_json::json!({"type": "failJob", "retries": 2}),
            serde_json::json!({"type": "throwError", "errorCode": "ERR"}),
            serde_json::json!({"type": "updateJobRetries", "retries": 2}),
            serde_json::json!({"type": "updateJobTimeout", "timeout": 1000}),
        ] {
            wire["corr"] = serde_json::json!(1);
            wire["jobKey"] = serde_json::json!("42");
            wire["leaseToken"] = serde_json::json!("opaque:not-a-number");
            let frame: ClientFrame = serde_json::from_value(wire.clone()).unwrap();
            let decoded = serde_json::to_value(frame).unwrap();
            for (field, value) in wire.as_object().unwrap() {
                assert_eq!(decoded.get(field), Some(value), "{}: {field}", wire["type"]);
            }
            assert_eq!(decoded["leaseToken"], "opaque:not-a-number");
        }
    }

    #[test]
    fn mutation_frames_accept_canonical_job_lease_token_alias() {
        // Deprecation window (#1283): the public `/falcon` client channel must
        // accept the canonical Camunda 8.10 `jobLeaseToken` name on job commands,
        // not just the pre-8.10 `leaseToken`, or a migrated client's fence would
        // be silently dropped onto the unfenced path. The two spellings are kept
        // as *distinct* fields (not a `serde` alias) so the handler can reconcile
        // them exactly as the REST boundary does — accept a matching dual-sent
        // pair and count legacy-name use for the migration metric — rather than
        // serde collapsing them (which rejects an equal pair as a duplicate field
        // and bypasses the shared reconciliation/counting rule).
        for mut wire in [
            serde_json::json!({"type": "completeJob"}),
            serde_json::json!({"type": "failJob", "retries": 2}),
            serde_json::json!({"type": "throwError", "errorCode": "ERR"}),
            serde_json::json!({"type": "updateJobRetries", "retries": 2}),
            serde_json::json!({"type": "updateJobTimeout", "timeout": 1000}),
            serde_json::json!({"type": "updateJob", "retries": 2}),
        ] {
            wire["corr"] = serde_json::json!(1);
            wire["jobKey"] = serde_json::json!("42");

            // Canonical name alone: deserializes into the distinct `jobLeaseToken`
            // field and round-trips under that spelling (the legacy field stays
            // absent — the handler, not serde, projects it onto the lease fence).
            let mut canonical = wire.clone();
            canonical["jobLeaseToken"] = serde_json::json!("opaque:not-a-number");
            let frame: ClientFrame = serde_json::from_value(canonical).unwrap();
            let decoded = serde_json::to_value(frame).unwrap();
            assert_eq!(
                decoded["jobLeaseToken"], "opaque:not-a-number",
                "{}: canonical jobLeaseToken must survive the frame",
                wire["type"]
            );
            assert!(
                decoded.get("leaseToken").is_none(),
                "{}: legacy field stays absent when only the canonical name is sent",
                wire["type"]
            );

            // A dual-sent equal pair must NOT be rejected as a duplicate field
            // (the exact regression a `serde(alias)` would introduce); both
            // spellings survive for the handler to reconcile.
            let mut pair = wire.clone();
            pair["leaseToken"] = serde_json::json!("opaque:not-a-number");
            pair["jobLeaseToken"] = serde_json::json!("opaque:not-a-number");
            let frame: ClientFrame = serde_json::from_value(pair).expect(
                "an equal jobLeaseToken/leaseToken pair must deserialize, not be \
                 rejected as a duplicate field",
            );
            let decoded = serde_json::to_value(frame).unwrap();
            assert_eq!(decoded["leaseToken"], "opaque:not-a-number");
            assert_eq!(decoded["jobLeaseToken"], "opaque:not-a-number");
        }
    }
}

/// `serde` `skip_serializing_if` predicate: omit a `bool` field when it is `false`.
fn is_false(b: &bool) -> bool {
    !*b
}

/// Client → server frames.
///
/// `Serialize` is derived so a node can act as a falcon *client* to its
/// cluster peers (the intra-cluster forwarding uplink), speaking the same wire
/// protocol it serves.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientFrame {
    /// Opt into job push for a type, granting an initial credit batch.
    #[serde(rename_all = "camelCase")]
    Subscribe {
        job_type: String,
        #[serde(default)]
        with_lease: bool,
        #[serde(default)]
        job_credits: i64,
        #[serde(default)]
        fetch_variable: Option<Vec<String>>,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        worker: Option<String>,
    },
    /// Replenish job-push demand for a type.
    #[serde(rename_all = "camelCase")]
    JobCredits {
        job_type: String,
        n: i64,
    },
    /// Start a process instance (consumes one submission credit).
    #[serde(rename_all = "camelCase")]
    CreateInstance {
        corr: u64,
        #[serde(default)]
        process_definition_id: Option<String>,
        #[serde(default)]
        process_definition_key: Option<String>,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
        #[serde(default)]
        await_completion: Option<bool>,
        #[serde(default)]
        fetch_variables: Option<Vec<String>>,
        #[serde(default)]
        request_timeout: Option<i64>,
    },
    /// Complete an activated job (unmetered drain).
    #[serde(rename_all = "camelCase")]
    CompleteJob {
        corr: u64,
        job_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_token: Option<String>,
        /// Canonical Camunda 8.10 spelling of the lease token (#1283). Kept as a
        /// distinct field from the legacy `leaseToken` (not a `serde` alias) so the
        /// handler can reconcile the two exactly as the REST boundary does: accept a
        /// dual-sent equal pair, reject a conflicting pair, and count legacy-name use
        /// for the migration metric. `skip_serializing_if` keeps it off the
        /// intra-cluster peer wire, which stays on `leaseToken`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_lease_token: Option<String>,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
        /// Optional agentic ad-hoc sub-process result (Camunda `JobResult`),
        /// forwarded to the owning peer. `None`/skipped for ordinary
        /// completions so the frame is byte-unchanged on the hot path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        adhoc_result: Option<nanobpmn_engine_core::AdHocJobResult>,
        /// Optional user-task-listener result (Camunda `JobResult` for user-task
        /// jobs): a denial and/or corrections, forwarded to the owning peer.
        /// `None`/skipped for ordinary completions (byte-unchanged hot path).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_result: Option<nanobpmn_engine_core::TaskListenerJobResult>,
    },
    /// Fail an activated job (unmetered drain).
    #[serde(rename_all = "camelCase")]
    FailJob {
        corr: u64,
        job_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_token: Option<String>,
        /// Canonical Camunda 8.10 spelling of the lease token (#1283). Kept as a
        /// distinct field from the legacy `leaseToken` (not a `serde` alias) so the
        /// handler can reconcile the two exactly as the REST boundary does: accept a
        /// dual-sent equal pair, reject a conflicting pair, and count legacy-name use
        /// for the migration metric. `skip_serializing_if` keeps it off the
        /// intra-cluster peer wire, which stays on `leaseToken`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_lease_token: Option<String>,
        #[serde(default)]
        retries: Option<i32>,
        #[serde(default)]
        error_message: Option<String>,
    },
    /// Throw a BPMN error from an activated job (unmetered drain).
    #[serde(rename_all = "camelCase")]
    ThrowError {
        corr: u64,
        job_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_token: Option<String>,
        /// Canonical Camunda 8.10 spelling of the lease token (#1283). Kept as a
        /// distinct field from the legacy `leaseToken` (not a `serde` alias) so the
        /// handler can reconcile the two exactly as the REST boundary does: accept a
        /// dual-sent equal pair, reject a conflicting pair, and count legacy-name use
        /// for the migration metric. `skip_serializing_if` keeps it off the
        /// intra-cluster peer wire, which stays on `leaseToken`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_lease_token: Option<String>,
        error_code: String,
        #[serde(default)]
        error_message: Option<String>,
        /// Variables instantiated at the local scope of the error catch event
        /// (Camunda `JobErrorRequest.variables`), forwarded to the owning peer.
        /// `None`/skipped for a bare error-throw (byte-unchanged hot path).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        variables: Option<Map<String, Value>>,
    },
    /// Re-subscribe to completion of an already-created instance. Used for
    /// recovery: a client that persisted the `processInstanceKey` from an earlier
    /// `CreateInstance` `CommandResult` can, after a reconnect, re-request the
    /// terminal outcome over the new socket. Resolves immediately if the instance
    /// is already terminal (the read model is durable history), so it doubles as a
    /// poll. Answered by an `InstanceCompleted` correlated by `corr`.
    #[serde(rename_all = "camelCase")]
    AwaitInstance {
        corr: u64,
        process_instance_key: String,
        #[serde(default)]
        fetch_variables: Option<Vec<String>>,
        #[serde(default)]
        request_timeout: Option<i64>,
    },
    Heartbeat,
    /// **Intra-cluster only.** A non-deployment-partition gateway forwards a
    /// client's deploy to the partition-0 owner (the deploy authority), which
    /// processes it durably and broadcasts the result to every peer. Answered by
    /// a `corr`-correlated `CommandResult` carrying the deployment JSON (200) or
    /// an error (4xx). See the centralized cluster-deployment model.
    #[serde(rename_all = "camelCase")]
    Deploy {
        corr: u64,
        /// `(resourceName, bpmnXml)` pairs, exactly as received over HTTP.
        resources: Vec<(String, String)>,
        #[serde(default)]
        tenant_id: Option<String>,
    },
    /// **Intra-cluster only.** The deployment-partition owner broadcasts an
    /// already-minted deployment (its `ProcessDeployed` events) to a peer, which
    /// durably installs the definition(s) on its owned partitions without minting
    /// new keys or arming start subscriptions (those stay solely on the
    /// deployment partition). Answered by a `CommandResult` once the install is
    /// committed.
    #[serde(rename_all = "camelCase")]
    InstallDeployment {
        corr: u64,
        events: Vec<Event>,
    },
    /// **Intra-cluster only.** A gateway fans a published message out to this peer
    /// so it correlates the message against the subscriptions on *its* owned
    /// partitions (open message subscriptions are spread across the cluster with
    /// their instances; message-start subscriptions live solely on the
    /// partition-0 owner). Local-only: the peer correlates across its own
    /// partitions and does **not** re-forward, so there is no fan-out loop.
    /// Answered by a `CommandResult` carrying `{messageKey, correlatedInstanceKey}`.
    #[serde(rename_all = "camelCase")]
    PublishMessage {
        corr: u64,
        name: String,
        #[serde(default)]
        correlation_key: String,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key process-instance
    /// cancellation to the peer that owns the instance's partition. Answered by a
    /// `CommandResult` (204 on success, 404 unknown instance, 4xx/5xx otherwise).
    #[serde(rename_all = "camelCase")]
    CancelInstance {
        corr: u64,
        instance_key: String,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key process-instance
    /// suspension to the peer that owns the instance's partition. Answered by a
    /// `CommandResult` (204 on success, 400 illegal transition from a terminal
    /// state, 404 unknown instance, 5xx otherwise).
    #[serde(rename_all = "camelCase")]
    SuspendInstance {
        corr: u64,
        instance_key: String,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key process-instance
    /// resumption to the peer that owns the instance's partition. Answered by a
    /// `CommandResult` (204 on success, 400 illegal transition from a terminal
    /// state, 404 unknown instance, 5xx otherwise).
    #[serde(rename_all = "camelCase")]
    ResumeInstance {
        corr: u64,
        instance_key: String,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key process-instance
    /// migration to the peer that owns the instance's partition. Answered by a
    /// `CommandResult` (204 on success, 400 invalid mapping, 404 unknown
    /// instance/target, 409 rejected migration, 5xx otherwise).
    #[serde(rename_all = "camelCase")]
    MigrateInstance {
        corr: u64,
        instance_key: String,
        target_process_definition_key: String,
        mapping_instructions: Vec<(String, String)>,
    },
    /// **Intra-cluster only.** Routes a cross-partition subscription follow-up
    /// (a `MessageSubscriptionOpening` opening a canonical subscription on its
    /// `hash(correlationKey)` partition, or a `RemoteMessageCorrelation` advancing
    /// a parked token on its instance's partition) to the node owning the target
    /// partition. The owner applies the corresponding routed command on its own
    /// engine and drives its own pump for any further follow-ups (which may route
    /// on again), so there is no central fan-out. Answered by a `CommandResult`
    /// (200) once applied + durable.
    #[serde(rename_all = "camelCase")]
    RouteSubscription {
        corr: u64,
        event: Event,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key job-retries update to
    /// the peer that owns the job's partition. Answered by a `CommandResult`.
    #[serde(rename_all = "camelCase")]
    UpdateJobRetries {
        corr: u64,
        job_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_token: Option<String>,
        /// Canonical Camunda 8.10 spelling of the lease token (#1283). Kept as a
        /// distinct field from the legacy `leaseToken` (not a `serde` alias) so the
        /// handler can reconcile the two exactly as the REST boundary does: accept a
        /// dual-sent equal pair, reject a conflicting pair, and count legacy-name use
        /// for the migration metric. `skip_serializing_if` keeps it off the
        /// intra-cluster peer wire, which stays on `leaseToken`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_lease_token: Option<String>,
        retries: i32,
        #[serde(default)]
        operation_reference: Option<i64>,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key job lock-extension
    /// (timeout) to the peer that owns the job's partition. Answered by a
    /// `CommandResult`.
    #[serde(rename_all = "camelCase")]
    UpdateJobTimeout {
        corr: u64,
        job_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_token: Option<String>,
        /// Canonical Camunda 8.10 spelling of the lease token (#1283). Kept as a
        /// distinct field from the legacy `leaseToken` (not a `serde` alias) so the
        /// handler can reconcile the two exactly as the REST boundary does: accept a
        /// dual-sent equal pair, reject a conflicting pair, and count legacy-name use
        /// for the migration metric. `skip_serializing_if` keeps it off the
        /// intra-cluster peer wire, which stays on `leaseToken`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_lease_token: Option<String>,
        timeout: i64,
        #[serde(default)]
        operation_reference: Option<i64>,
    },
    /// Atomically forwards all property changes and their optional lease fence.
    #[serde(rename_all = "camelCase")]
    UpdateJob {
        corr: u64,
        job_key: String,
        #[serde(default)]
        retries: Option<i32>,
        #[serde(default)]
        timeout: Option<i64>,
        #[serde(default)]
        operation_reference: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_token: Option<String>,
        /// Canonical Camunda 8.10 spelling of the lease token (#1283). Kept as a
        /// distinct field from the legacy `leaseToken` (not a `serde` alias) so the
        /// handler can reconcile the two exactly as the REST boundary does: accept a
        /// dual-sent equal pair, reject a conflicting pair, and count legacy-name use
        /// for the migration metric. `skip_serializing_if` keeps it off the
        /// intra-cluster peer wire, which stays on `leaseToken`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job_lease_token: Option<String>,
    },
    /// Forwards a canonical agent CREATE or UPDATE request to its partition leader.
    #[serde(rename_all = "camelCase")]
    ForwardAgentInstance {
        corr: u64,
        #[serde(default)]
        agent_instance_key: Option<String>,
        body: Value,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key incident resolution to
    /// the peer that owns the incident's partition. Answered by a `CommandResult`.
    #[serde(rename_all = "camelCase")]
    ResolveIncident {
        corr: u64,
        incident_key: String,
        #[serde(default)]
        operation_reference: Option<i64>,
    },
    /// **Intra-cluster only.** A gateway forwards a by-key variable merge to the
    /// peer that owns the scope's partition. Answered by a `CommandResult`.
    #[serde(rename_all = "camelCase")]
    SetVariables {
        corr: u64,
        scope_key: String,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
        #[serde(default)]
        local: bool,
    },
    /// **Intra-cluster only.** A gateway asks a peer to activate jobs on the peer's
    /// own partitions for a worker attached to the gateway (job-stream
    /// aggregation). The peer leases the jobs under `timeout` and replies a
    /// `CommandResult` whose body is the JSON array of `ActivatedJobResult`. The
    /// peer activates locally only (its engine owns just its partitions), so there
    /// is no fan-out loop. At-least-once is preserved by the peer's lock: if the
    /// gateway dies before the worker completes, the lease expires and the job
    /// re-activates on its owner.
    #[serde(rename_all = "camelCase")]
    ActivateJobs {
        corr: u64,
        job_type: String,
        #[serde(default)]
        with_lease: bool,
        worker: String,
        max_jobs: i64,
        #[serde(default)]
        timeout: Option<u64>,
        #[serde(default)]
        fetch_variable: Option<Vec<String>>,
    },
    /// **Intra-cluster only.** A gateway forwards a `createProcessInstance` to a
    /// peer for cluster-wide create placement (the gateway round-robins creates
    /// across every partition; a partition owned by a peer is placed via this
    /// frame). The peer creates on one of *its own* partitions and does **not**
    /// re-forward (local-only, no placement loop). Answered by a `CommandResult`
    /// carrying the full `CreateProcessInstanceResult` JSON (200) or an error.
    #[serde(rename_all = "camelCase")]
    ForwardCreate {
        corr: u64,
        #[serde(default)]
        process_definition_id: Option<String>,
        #[serde(default)]
        process_definition_key: Option<String>,
        #[serde(default)]
        variables: Option<Map<String, Value>>,
        #[serde(default)]
        tags: Vec<String>,
        #[serde(default)]
        business_id: Option<String>,
        #[serde(default)]
        await_completion: bool,
        #[serde(default)]
        fetch_variables: Option<Vec<String>>,
        #[serde(default)]
        request_timeout: Option<i64>,
        /// The ingress protocol of the ORIGINAL client call on the forwarding
        /// node (`"stream"` or `"rest"`). The owner records the create under
        /// this label so `nanobpm_creates_total` reflects the client transport,
        /// counted exactly once at the committing owner — the forwarding node
        /// does not record a create it hands off. `#[serde(default)]` keeps the
        /// frame wire-compatible with peers that predate the field (a missing
        /// value is treated as `"rest"`).
        #[serde(default)]
        origin_protocol: Option<String>,
    },
    /// **Intra-cluster only.** A gateway forwards a GET-by-key read to the peer
    /// that owns the key's partition. Each node only projects its own partitions
    /// into its read model, so a read for a remote key must be answered by the
    /// owner. Answered by a `CommandResult` carrying the entity JSON (200) or 404.
    #[serde(rename_all = "camelCase")]
    GetByKey {
        corr: u64,
        kind: ReadKind,
        key: u64,
    },
    /// **Intra-cluster only.** A gateway forwards a user-task by-key mutation
    /// (assign / complete / unassign / update) to the peer that owns the task's
    /// partition. `payload` is the original REST request body, re-applied locally
    /// by the owner. Answered by a `CommandResult` whose status mirrors the REST
    /// outcome (204 / 404 / 409 / 500).
    #[serde(rename_all = "camelCase")]
    ForwardUserTask {
        corr: u64,
        op: UserTaskOp,
        user_task_key: String,
        #[serde(default)]
        payload: Option<Value>,
    },
    /// **Intra-cluster only.** A gateway forwards the external "activate ad-hoc
    /// activities" mutation (#614 gap 3) to the peer that owns the ad-hoc
    /// sub-process container's partition. `payload` is the original REST request
    /// body (`elements` + `cancelRemainingInstances`), re-applied locally by the
    /// owner. Answered by a `CommandResult` whose status mirrors the REST
    /// outcome (204 / 400 / 404 / 500).
    #[serde(rename_all = "camelCase")]
    ForwardAdHocActivation {
        corr: u64,
        ad_hoc_instance_key: String,
        #[serde(default)]
        payload: Option<Value>,
    },
    /// **Intra-cluster only.** Carries one serialized Raft RPC (AppendEntries /
    /// Vote / InstallSnapshot) for `partition`'s replica group to the node hosting
    /// it. Answered by a `CommandResult` whose body is the serialized
    /// `RaftRpcResponse` (200) or an error status. This is the falcon
    /// transport for per-partition Raft (stage 3 leader routing).
    Raft {
        corr: u64,
        partition: u64,
        /// The `RaftRpcRequest` serialized to a JSON string and carried verbatim,
        /// so the heavy AppendEntries / InstallSnapshot payloads (journal events +
        /// variables) are encoded once on the sender and parsed once on the
        /// receiver — never materialized into an intermediate `serde_json::Value`
        /// DOM (which, for a large variables map, would allocate a node per field).
        /// When `zip` is set the string is raw-deflate + base64 (large payloads
        /// only — see `nano_server_raft::raft_net::encode_rpc_payload`).
        rpc: String,
        /// Whether `rpc` is deflate+base64 compressed. Absent ⇒ `false` (raw JSON),
        /// so small control RPCs add no field and older raw frames still parse.
        #[serde(default, skip_serializing_if = "is_false")]
        zip: bool,
    },
    /// **Intra-cluster only.** A best-effort soft lease digest broadcast by a
    /// partition leader to its followers (leader-local activation, `digest` mode).
    /// Fire-and-forget: it carries the leader's currently-held activation leases
    /// `(jobKey, deadline)` for `partition` so that a follower, on being promoted
    /// to leader, can recover them ([`Engine::recover_lease`]) and honour each
    /// deadline before redelivering — narrowing the failover redelivery window
    /// that leader-local activation otherwise opens. It is **not** answered (no
    /// `corr`); a dropped or stale digest only costs a slightly wider window, so
    /// loss is safe by construction.
    #[serde(rename_all = "camelCase")]
    LeaseDigest {
        partition: u64,
        leases: Vec<(u64, u64)>,
        sent_at: u64,
    },
    /// **Intra-cluster only.** A best-effort retirement digest broadcast by a
    /// partition leader to its followers under RF>1. Carries the instance keys the
    /// leader has completed and exporter-evicted locally for `partition`. Because
    /// completion + eviction are leader-local (they never enter the raft log), a
    /// follower replica applies each `CreateInstance` but never the matching
    /// retirement, so completed instances accumulate as never-reaped `Active` shells
    /// (the RF>1 hot-state leak). On receipt the follower drops these keys from its
    /// replica engine (`nano_server_storage::journal::Journal::retire_instances`). Fire-and-forget
    /// (no `corr`) and idempotent: a dropped/stale digest only delays retirement
    /// (memory converges on the next digest), and a lagging learner that has not yet
    /// applied a create simply ignores the miss (it is re-snapshotted from the leader).
    #[serde(rename_all = "camelCase")]
    RetirementDigest {
        partition: u64,
        keys: Vec<u64>,
    },
    /// Loss-tolerant reconciliation backstop for [`ClientFrame::RetirementDigest`]
    /// (RF>1). The partition owner broadcasts `low_water` — the smallest key still
    /// `Active` on it — every tick; on receipt a follower drops every resident
    /// replica instance below it (`nano_server_storage::journal::Journal::retire_below`). Unlike
    /// the per-key digest this is idempotent and self-healing: because the mark is
    /// re-sent each tick and reaps below it wholesale, a follower converges even
    /// when best-effort per-key frames are dropped under load. Fire-and-forget (no
    /// `corr`); a dropped/stale watermark only delays convergence by a tick.
    #[serde(rename_all = "camelCase")]
    RetirementWatermark {
        partition: u64,
        low_water: u64,
    },
    /// Leader-durable auto-recovery announcement (ADR 0003): the sender has
    /// app-promoted itself leader of `partition` at `epoch` after detecting the
    /// previous (sole-voter) leader was lost. Recipients adopt the higher epoch,
    /// rejoin the partition as a learner of the new leader (so new writes ship to
    /// them again), and a stale leader at a lower epoch steps down (fencing).
    /// Fire-and-forget (no `corr`): the promoting leader retries `add_learner`, so a
    /// dropped announcement only delays a survivor's rejoin, never correctness.
    #[serde(rename_all = "camelCase")]
    Promote {
        partition: u64,
        epoch: u64,
        leader_node: u64,
        leader_addr: String,
    },
    /// **Leadership reclaim — synchronous step-down request (new leader → survivor).**
    /// Request/response variant of [`Promote`](ClientFrame::Promote): the promoting
    /// leader waits for the recipient to adopt the epoch and rebuild as a receiver
    /// (tearing down any prior-epoch group it still leads) BEFORE the leader ships it
    /// the fresh lineage via `add_learner`. `Promote` (control lane) and the
    /// `add_learner` AppendEntries (raft lane) travel on independent sockets with no
    /// ordering, so a naive "announce then add_learner" lets the leader's fresh
    /// committed `idx-1` reach the survivor's still-live prior-epoch group and collide
    /// two committed lineages (the openraft `has_log_id` invariant, issue #228). The
    /// recipient replies with a `CommandResult` (200) once its rebuild has completed;
    /// the `corr` matches the reply back to the awaiting promoter.
    #[serde(rename_all = "camelCase")]
    PromoteSync {
        corr: u64,
        partition: u64,
        epoch: u64,
        leader_node: u64,
        leader_addr: String,
    },
    /// **Intra-cluster only.** An operator switched the runtime SLA mode on one
    /// node (via the console SLA knob); that node fans the new mode out to every
    /// peer so the whole cluster applies a single, uniform admission policy (a
    /// split — some nodes shedding to preserve latency while others keep admitting
    /// — would make client behaviour depend on which node routed the create).
    /// `mode` is the `nano_server_runtime::backpressure::SlaMode` string (`"latency"` /
    /// `"admission"`). Fire-and-forget (no `corr`): the recipient applies it
    /// locally and does **not** re-broadcast (no fan-out loop); a briefly
    /// unreachable node keeps its prior mode until the next switch or a restart
    /// (which reseeds from `NANOBPMN_SLA_MODE`), which is safe for an operational
    /// knob.
    #[serde(rename_all = "camelCase")]
    SetSlaMode {
        mode: String,
    },
    /// Create-placement load gossip (ADR 0014 `balanced`): the sending node's
    /// current composite create-load index, broadcast periodically to every peer
    /// so their weighted placement can steer creates toward nodes with headroom.
    /// `node` is the sender's node id; `load` is its
    /// `create_load_index` (a shedding
    /// node reports `SHED_LOAD`). Fire-and-forget
    /// (no `corr`), not re-broadcast: a stale/absent report is treated as full
    /// headroom, and the reactive shed/reroute layer is the correctness backstop.
    #[serde(rename_all = "camelCase")]
    PressureReport {
        node: u32,
        load: i64,
    },
    /// Leader-durable reclaim solicitation (ADR 0003): a node that just (re)joined
    /// asks its peers to re-announce the promotion epochs they currently lead, so
    /// the rejoining owner can reclaim its statically-owned partitions at
    /// `incumbent_epoch + 1` and fence the failover leader in a SINGLE round. The
    /// promotion epoch is in-memory and resets on restart, so without this the
    /// rejoined owner starts at epoch 1, loses the fence to the higher-epoch
    /// failover leader, and must climb one epoch per recovery tick — each climb a
    /// fresh election that, under sustained writes, produces the `leader_reject`
    /// storm. `from_node` is the soliciting node's id. Fire-and-forget (no `corr`):
    /// the recipient replies with its standing [`Promote`](ClientFrame::Promote)
    /// frames for the partitions it leads; a dropped solicit is retried on the next
    /// recovery tick within the grace window.
    #[serde(rename_all = "camelCase")]
    SolicitPromotions {
        from_node: u64,
    },
    /// **Leadership hand-off — request (owner → incumbent).** A rejoining static
    /// owner asks the current failover leader of `partition` to hand leadership
    /// back via a real openraft membership change (add the owner as a learner,
    /// catch it up, then `change_membership` to it) rather than the owner forming
    /// a competing fresh single-voter group. This avoids the two-lineage election
    /// war (competing groups driving each other's raft term up under load).
    /// `requester_node`/`requester_addr` identify the returning owner so the
    /// incumbent can add it as a learner. Answered with [`HandoffAck`] then a
    /// terminal [`HandoffComplete`]/[`HandoffFailed`].
    #[serde(rename_all = "camelCase")]
    RequestHandoff {
        partition: u64,
        requester_node: u64,
        requester_addr: String,
    },
    /// **Leadership hand-off — acknowledgement (incumbent → owner).** The incumbent
    /// leader reserved its per-partition handoff lease and is starting the
    /// hand-off (`accepted = true`), or declined because it does not raft-lead
    /// `partition` or a hand-off/promotion is already in flight (`accepted =
    /// false`). `incumbent_epoch` is the app-promotion epoch the incumbent holds,
    /// so the owner can fence strictly above it if the hand-off later fails and it
    /// must fall back to a self-promote.
    #[serde(rename_all = "camelCase")]
    HandoffAck {
        partition: u64,
        incumbent_epoch: u64,
        accepted: bool,
    },
    /// **Leadership hand-off — success (incumbent → owner).** The incumbent's
    /// `change_membership` committed the uniform config with the owner as the sole
    /// voter; the incumbent has stepped down to a learner. `epoch` is the new
    /// app-promotion epoch (`incumbent_epoch + 1`, naming the owner) the owner
    /// must adopt so a later stale [`Promote`]/[`SolicitPromotions`] can't undo
    /// the hand-off. After this the owner genuinely raft-leads `partition`.
    #[serde(rename_all = "camelCase")]
    HandoffComplete {
        partition: u64,
        epoch: u64,
        new_leader: u64,
    },
    /// **Leadership hand-off — failure (incumbent → owner).** The incumbent
    /// aborted the hand-off (learner catch-up timed out, it lost leadership, or a
    /// membership step errored). `joint_suspected = true` means a
    /// `change_membership` may have committed the JOINT config but not the final
    /// uniform one, so the group could require a quorum of BOTH voter sets — the
    /// owner MUST NOT fall back to forming a fresh competing group (that would
    /// diverge), and should instead retry the hand-off / wait. `reason` is a short
    /// human-readable diagnostic.
    #[serde(rename_all = "camelCase")]
    HandoffFailed {
        partition: u64,
        joint_suspected: bool,
        reason: String,
    },
}

impl ClientFrame {
    /// Whether this frame belongs to the **public client protocol** (the surface
    /// documented in `docs/falcon.asyncapi.yaml`) and is therefore permitted on
    /// the client-facing `/falcon` channel.
    ///
    /// Every other variant is an **intra-cluster** frame — the peer-to-peer
    /// control/data plane a forwarding gateway uses to drive the owning node
    /// (deploy install, raft RPCs, promotion/handoff, direct variable/incident
    /// mutation, …). Those must only ever arrive on the authenticated cluster
    /// channel (`/cluster`); accepting them from an unauthenticated public
    /// client socket would expose the entire cluster control plane. See
    /// ADR 0039.
    ///
    /// Note the cluster channel is a **superset**: a forwarding gateway also
    /// sends genuine client frames (`CreateInstance`, `CompleteJob`, `FailJob`,
    /// `ThrowError`) to the owning peer, so `/cluster` accepts everything while
    /// `/falcon` is gated to exactly this public subset.
    pub fn is_public(&self) -> bool {
        matches!(
            self,
            ClientFrame::Subscribe { .. }
                | ClientFrame::JobCredits { .. }
                | ClientFrame::CreateInstance { .. }
                | ClientFrame::CompleteJob { .. }
                | ClientFrame::FailJob { .. }
                | ClientFrame::ThrowError { .. }
                | ClientFrame::AwaitInstance { .. }
                | ClientFrame::Heartbeat
        )
    }
}

/// Which Falcon channel a connection arrived on. The public `/falcon` channel is
/// restricted to the client protocol ([`ClientFrame::is_public`]); the
/// authenticated `/cluster` channel carries the full intra-cluster protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// Public, client-facing endpoint (`/falcon`). Peer/control frames rejected.
    Client,
    /// Authenticated intra-cluster endpoint (`/cluster`). Accepts every frame.
    Cluster,
}

/// The kind of entity a [`ClientFrame::GetByKey`] read targets, selecting which
/// read-model lookup the owning peer runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReadKind {
    ProcessInstance,
    Incident,
    UserTask,
    Variable,
    ElementInstance,
}

/// The user-task mutation a [`ClientFrame::ForwardUserTask`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UserTaskOp {
    Assign,
    Complete,
    Unassign,
    Update,
}
///
/// `Deserialize` is derived so the peer uplink (a node acting as a falcon
/// client to its cluster peers) can decode a peer's responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ServerFrame {
    /// Sent once on connect: the initial submission window and heartbeat cadence.
    #[serde(rename_all = "camelCase")]
    Welcome {
        submission_credits: i64,
        heartbeat_ms: u64,
    },
    /// A pushed activated job (consumes one job-delivery credit). `job` is the
    /// same shape as a REST `ActivatedJobResult`, carried as a pre-serialized raw
    /// JSON value so the hot dispatch path serializes each job exactly once (no
    /// intermediate `serde_json::Value` tree) and the writer emits it verbatim.
    Job {
        job: Box<serde_json::value::RawValue>,
    },
    /// Ack/result for a create/complete/fail/throwError, correlated by `corr`.
    #[serde(rename_all = "camelCase")]
    CommandResult {
        corr: u64,
        status: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<Value>,
    },
    /// Async await-completion: emitted when an awaited instance reaches a terminal
    /// state, routed back by the create's `corr`.
    #[serde(rename_all = "camelCase")]
    InstanceCompleted {
        corr: u64,
        process_instance_key: String,
        process_completed: bool,
        variables: Value,
    },
    /// Grants the client additional submission (create-side) capacity.
    SubmissionCredits {
        n: i64,
    },
    /// Coarse fleet pressure signal.
    #[serde(rename_all = "camelCase")]
    Pressure {
        level: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
    /// Server-advised worker concurrency: the active dispatch width the
    /// worker-concurrency governor has converged on. A cooperating SDK can park
    /// (idle) its subscribers down toward this target so an over-provisioned fleet
    /// stops swamping the single push dispatcher (which halves throughput past the
    /// ~50-worker/node knee). `0` = no advice (governor disabled). Edge-triggered:
    /// broadcast once each time the target changes, so clients self-size without
    /// polling. Purely advisory — the server also enforces the cap by truncating
    /// each job type's per-pass fan-out, so uncooperative clients are still bounded.
    #[serde(rename_all = "camelCase")]
    WorkerAdvice {
        recommended_concurrency: i64,
    },
    Heartbeat,
}

/// HTTP header a peer presents on the `/cluster` handshake to authenticate.
pub const CLUSTER_SECRET_HEADER: &str = "x-nano-cluster-secret";

/// Reads the expected intra-cluster shared secret from the environment
/// (`NANOBPMN_CLUSTER_SECRET`). An empty/absent value disables authentication.
pub fn cluster_secret_from_env() -> Option<String> {
    std::env::var("NANOBPMN_CLUSTER_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
}
