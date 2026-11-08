# ADR 0039 — Splitting the Falcon transport: a public client channel vs an authenticated cluster channel

Status: **Proposed.**
Date: 2026-07-24.
Relates to / refines:
`docs/falcon.asyncapi.yaml` (the public Falcon client protocol — the 8 client + 7 server frames this
ADR designates as the *only* frames a public connection may send),
`server/src/falcon.rs` (the `ClientFrame`/`ServerFrame` enums, the WebSocket router, and the new
`is_public()` classifier + `Channel` gate),
`server/src/peer.rs` (the intra-cluster uplink that now dials `/cluster` with a shared secret),
`server/src/main.rs` (the gateway wiring that serves the public app and, optionally, an internal
listener).

## Context

Falcon is Nano's WebSocket transport. A single Rust enum, `ClientFrame`, models **every** inbound
frame — but it carries **two very different populations**:

1. **8 public client frames** — the documented client protocol in `docs/falcon.asyncapi.yaml`:
   `Subscribe`, `JobCredits`, `CreateInstance`, `CompleteJob`, `FailJob`, `ThrowError`,
   `AwaitInstance`, `Heartbeat`. These are what SDKs and job workers send.

2. **25 intra-cluster peer frames** — the control/data plane the nodes speak to *each other*:
   `Promote`, `Raft`, `SetVariables`, `CancelInstance`, `ResolveIncident`, `Deploy`, handoff frames,
   `ForwardCreate`, etc. These are **undocumented on purpose** — they are the cluster's internal RPC.

Both populations arrived on **one unauthenticated route**: `GET /falcon`, merged straight into the
public gateway app. `ws_handler` read only a `worker` query parameter; the sole middleware was
logging + CORS. Inbound dispatch deserialized the frame as `ClientFrame` and dispatched **all 33
variants with no trust gate**.

**This is a trust-boundary vulnerability.** A public client could send `Promote`, `Raft`,
`SetVariables`, `CancelInstance`, `Deploy`, `ResolveIncident`, … and drive the cluster control and
data plane directly. A genuine *server* frame fails the `ClientFrame` deserialize and 400s gracefully,
so the exposure is one-directional — but the direction that *is* open (client → intra-cluster frames)
is the dangerous one.

A naive fix — split into two disjoint enums — does not fit the topology: the **peer uplink is a
superset consumer**. Peers legitimately send 4 *client* frames (`CreateInstance`, `CompleteJob`,
`FailJob`, `ThrowError` — forwarded work) **plus** the 25 peer frames. So the cluster channel needs
the full 33-variant set; only the *public* channel is the restricted subset of 8.

## Decision

Keep the single `ClientFrame` enum, but **classify and gate by connection channel** rather than by
enum type.

1. **`ClientFrame::is_public()`** (`server/src/falcon.rs`) returns `true` for exactly the 8 documented
   client frames and `false` for the 25 intra-cluster frames. This is the single source of truth for
   the trust boundary and is asserted against the AsyncAPI spec by the existing `asyncapi_spec_guard`
   test module, so the gate tracks the documented protocol automatically.

2. **Two routes, one enum:**
   - `router()` serves `GET /falcon` with `Channel::Client`. After parsing each frame, the reader
     rejects any `!frame.is_public()` frame with a `CommandResult { status: 403 }`, records a
     `rejected_peer_frame` metric, and continues. Public clients keep their exact protocol; peer
     frames are refused.
   - `cluster_router(server, registry, secret)` serves `GET /cluster` with `Channel::Cluster` and
     accepts the full frame set. When a shared secret is configured, the upgrade requires the
     `x-nano-cluster-secret` header, constant-time compared, else the handshake is refused with `401`.

3. **The peer uplink dials `/cluster`.** `peer.rs` `ws_url()` now targets `/cluster` (raft socket
   `/cluster?raft=1`); `dial()` attaches the `x-nano-cluster-secret` header when
   `NANOBPMN_CLUSTER_SECRET` is set. Public clients cannot reach the cluster frame set even by
   guessing the path unless they also hold the secret.

4. **Optional internal listener for port isolation.** `NANOBPMN_INTERNAL_ADDR` (e.g. `10.0.0.2:9090`)
   binds the `/cluster` router on a **separate socket** and keeps it *out* of the public app entirely.
   When unset (the default), `/cluster` is merged into the main app, isolated by path + secret. Either
   way the public `/falcon` route is untouched.

## Consequences

- **The vulnerability is closed at the boundary, not per-handler.** A public connection can no longer
  send any intra-cluster frame — the gate is one route-level check that cannot be forgotten per new
  peer frame, because `is_public()` defaults new variants to non-public.
- **Coordinated cluster upgrade required.** All nodes must move to `/cluster` (+ secret) together — a
  node still dialing `/falcon` for peer traffic would be 403'd. This is a breaking intra-cluster
  change; roll the whole cluster, not one node at a time.
  The same rule governs every later change to an intra-cluster wire shape on this channel — e.g.
  openraft's `AppendEntriesResponse::Conflict` gaining a follower last-log-id hint (#1454), which
  changes it structurally from a unit variant to a tuple variant carrying `Option<LogId>` (the exact
  serialized shape is serializer-dependent). Such changes ship as plain format changes with no
  mixed-version decoding; upgrade the whole cluster together.
- **Secret is optional but recommended.** With `NANOBPMN_CLUSTER_SECRET` unset the `/cluster` channel
  is open (a single node has no peers, so nothing is exposed); a startup warning fires for a
  multi-node deployment with no secret configured.
- **Deferred: peer-dial to the internal address.** `NANOBPMN_INTERNAL_ADDR` changes only *local*
  serving today; peers still dial the node's main advertised address. Dialing the internal address
  requires topology to carry a second per-node address — left for a follow-up.
- **Follow-up drift note:** `ServerFrame::WorkerAdvice` is broadcast to real clients but is missing
  from `docs/falcon.asyncapi.yaml` and unhandled by JS clients — a separate, documented public-message
  drift to close independently of this channel split.
