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
//! Operations: `searchAgentInstances`, `searchAgentInstanceHistory`
//! ([`agent`]), `searchProcessInstances` ([`process_instance`]),
//! `searchUserTasks` ([`user_task`]), `searchVariables` ([`variable`]) and the
//! single-entity reads `getFormByKey`, `getResource`, `getAgentInstance`
//! ([`get`]). Each pipeline reads rows through [`source::ReadSource`], so the
//! gateway's sharded read model and the WASM engine's single store share it.

pub mod agent;
pub mod convert;
pub mod get;
pub mod process_instance;
pub mod query;
pub mod source;
pub mod user_task;
pub mod variable;

pub use nanobpm_gateway_rest::{models, strict, types};
pub use nanobpmn_read_model as readstore;

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
/// human-readable rejection reported to the caller.
///
/// A property the spec does not define is REJECTED, never silently dropped:
/// serde ignores unknown fields, so a misspelt or legacy-shaped body (e.g. a
/// filter property at the top level instead of under `filter`) would otherwise
/// run as an unfiltered search and return the wrong rows with no error — the
/// silent-ignore defect class #1883 removes.
pub fn parse_search_query<T>(json: &str) -> Result<Option<T>, String>
where
    T: serde::de::DeserializeOwned + serde::Serialize + validator::Validate,
{
    // The SAME strict decoder the gateway's `StrictOptionalJson` extractor
    // runs (generated `strict` module), so the two surfaces cannot drift.
    let query = nanobpm_gateway_rest::strict::decode_strict::<T>(json.as_bytes())
        .map_err(|e| format!("invalid search query: {e}"))?;
    if let Some(q) = &query {
        q.validate()
            .map_err(|e| format!("invalid search query: {e}"))?;
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_null_bodies_are_absent_queries() {
        assert!(
            parse_search_query::<models::AgentInstanceSearchQuery>("  ")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_search_query::<models::AgentInstanceSearchQuery>("null")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn spec_shaped_queries_parse_with_every_operator() {
        let q = parse_search_query::<models::AgentInstanceSearchQuery>(
            r#"{"filter":{"creationDate":{"$gte":"2026-01-01T00:00:00Z","$lt":"2027-01-01T00:00:00Z"},
                "status":{"$in":["IDLE","THINKING"]},"elementId":{"$like":"agent*"}},
                "sort":[{"field":"creationDate","order":"DESC"}],"page":{"limit":2}}"#,
        )
        .unwrap()
        .unwrap();
        assert!(q.filter.is_some());
    }

    #[test]
    fn unknown_properties_are_rejected_not_silently_ignored() {
        for body in [
            r#"{"agentInstanceKey":"1"}"#,
            r#"{"filter":{"bogus":"x"}}"#,
            r#"{"filter":{"creationDate":{"$between":"x"}}}"#,
        ] {
            let err = parse_search_query::<models::AgentInstanceSearchQuery>(body).unwrap_err();
            assert!(err.contains("unknown property"), "{body}: {err}");
        }
    }
}
