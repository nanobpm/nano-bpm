//! Engine working state, split into the data model ([`types`]) and the applier
//! ([`apply`]). This split breaks the `event ↔ state` cycle: [`crate::event`]
//! may depend on [`types`] but never on [`apply`]. Re-exports keep every
//! existing `crate::state::…` path resolving.

mod apply;
#[cfg(any(test, kani))]
mod kani_model;
#[cfg(kani)]
mod kani_proofs;
pub(crate) mod types;

pub use apply::apply;
#[cfg(test)]
pub(crate) use types::activation_order;
pub use types::*;
