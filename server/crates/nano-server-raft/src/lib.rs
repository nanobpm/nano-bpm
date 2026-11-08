//! Raft/consensus layer of the nanobpm gateway server (ADR 0064 Phase 2).
//!
//! The per-partition consensus stack — the openraft state machine (`raft`), the
//! segmented-journal-backed log store (`raft_logstore`), the falcon-backed raft
//! transport (`raft_net`), and the intra-cluster peer uplink (`peer`). This is
//! the workspace's only `openraft` consumer.
//!
//! Extracted from the gateway binary crate under ADR 0064 so consensus — and
//! its heavy `openraft`/`tokio-tungstenite` dependency set — sits behind a
//! stable crate boundary. The modules were moved verbatim (visibility keywords
//! and `use`-path fixes aside); the gateway binary re-exports each at its own
//! crate root (`pub(crate) use nano_server_raft::raft;` etc.) so existing
//! `crate::raft::…` paths there keep resolving.
//!
//! `peer`/`raft_net` reach the falcon **wire frames** through
//! `nano-falcon-protocol` (the `ServerImpl`-driven falcon dispatcher stays in
//! the binary). Lower-layer items reached via `crate::journal`/`crate::metrics`/
//! `crate::seglog` (storage) and `crate::deepthi`/`crate::cluster`/
//! `crate::cmd_profile`/`crate::backpressure` (runtime) are re-exported below so
//! the moved files' unqualified `crate::…` paths keep resolving.

// Lower-layer modules the consensus stack reaches into, re-exported at the crate
// root so the moved files' existing `crate::…` paths keep resolving without
// per-line edits — the same seam the gateway binary uses for the storage crate.
pub(crate) use nano_server_runtime::{cluster, cmd_profile, deepthi};
pub(crate) use nano_server_storage::{journal, metrics};

pub mod fence;
pub mod peer;
pub mod raft;
pub mod raft_logstore;
pub mod raft_net;
