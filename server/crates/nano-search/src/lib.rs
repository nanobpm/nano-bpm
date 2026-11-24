//! The spec-derived search layer shared by the gateway server and the
//! in-browser `engine-wasm` test engine (#1883).
//!
//! Every search operation is ONE pipeline: the generated `*SearchQuery` model
//! (parsed from the request body) → [`query`] filter matching over read-model
//! rows → multi-field sort → cursor/offset [`query::paginate`] → the generated
//! `*SearchQueryResult` model. The server's REST handlers and the WASM read
//! methods both call the per-operation functions here, so the two surfaces
//! cannot drift from each other or from the spec (the server/WASM parity guard
//! in `engine-wasm` pins that).
//!
//! Operations migrated so far: `searchAgentInstances`,
//! `searchAgentInstanceHistory` ([`agent`]).

pub mod agent;
pub mod convert;
pub mod query;

pub use nanobpm_gateway_rest::{models, types};

/// Why a shared search pipeline failed.
#[derive(Debug)]
pub enum SearchError<E> {
    /// The row source (the read-model query) failed.
    Store(E),
    /// A stored projection column could not be decoded into the REST model.
    Projection(serde_json::Error),
}

impl<E: std::fmt::Display> std::fmt::Display for SearchError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchError::Store(e) => write!(f, "read-model query failed: {e}"),
            SearchError::Projection(e) => write!(f, "invalid projected read-model JSON: {e}"),
        }
    }
}

/// Parse a request body into the generated `*SearchQuery` model and run the
/// generated validation rules, exactly as the gateway's generated router does
/// before dispatching to a handler. An empty/whitespace or `null` body is an
/// absent query (the spec marks every search body optional). Errors are the
/// human-readable rejection the server reports as a 400.
pub fn parse_search_query<T>(json: &str) -> Result<Option<T>, String>
where
    T: serde::de::DeserializeOwned + validator::Validate,
{
    let trimmed = json.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let query: Option<T> =
        serde_json::from_str(trimmed).map_err(|e| format!("invalid search query: {e}"))?;
    if let Some(q) = &query {
        q.validate()
            .map_err(|e| format!("invalid search query: {e}"))?;
    }
    Ok(query)
}
