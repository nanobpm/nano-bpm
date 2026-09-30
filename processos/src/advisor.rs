//! Worker-scaling advisor — re-export of the shared `nano-provisioning-advisor`
//! crate.
//!
//! The classification thresholds and Little's-Law worker sizing that this cockpit
//! route (`advisor_advise`) serves are the *same* logic the gateway's
//! `/console/api/provisioning` endpoint returns (#1294). To keep a single source
//! of truth — and avoid reimplementing the thresholds twice — the implementation
//! lives on the Nano side (`server/crates/nano-provisioning-advisor`). The
//! dependency edge is one-way: Nano never links ProcessOS, but ProcessOS may link
//! Nano crates (it already embeds `engine-core`), so it consumes the advisor by
//! re-export here. Existing `advisor::Snapshot` / `advisor::parse_snapshot` /
//! `advisor::advise` call sites keep resolving unchanged.
pub use nano_provisioning_advisor::*;
