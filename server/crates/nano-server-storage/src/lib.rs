//! Storage layer of the nanobpm gateway server.
//!
//! Extracted from the `server` binary crate under ADR 0064 (server crate
//! decomposition, Phase 1) so the storage stack — and its heavy dependencies
//! (rusqlite, prometheus) — compiles behind a stable crate boundary instead of
//! being re-typechecked on every edit to the ~55k-line binary.
//!
//! The modules were moved verbatim (visibility keywords and `use`-path fixes
//! aside); the gateway binary re-exports each of them at its own crate root
//! (`pub(crate) use nano_server_storage::journal;` etc.) so existing
//! `crate::journal::…` paths there keep resolving. Dependency direction inside
//! the crate is acyclic: `metrics`/`memory` are leaves, `varspill` builds on
//! `sqlite_space`, `seglog` on `readstore`/`varstore`, `journal` on
//! `coldspill`/`memory`/`metrics`/`seglog`/`varspill`/`varstore`, and
//! `remote_sink` on `metrics`/`readstore`.

pub mod coldspill;
pub mod journal;
pub mod memory;
pub mod metrics;
pub mod readstore;
pub mod remote_sink;
pub mod seglog;
pub mod snapshot_health;
pub mod sqlite_space;
pub mod varspill;
pub mod varstore;
