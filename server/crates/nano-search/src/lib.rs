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
    let trimmed = json.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let raw: serde_json::Value =
        serde_json::from_str(trimmed).map_err(|e| format!("invalid search query: {e}"))?;
    let query: Option<T> =
        serde_json::from_value(raw.clone()).map_err(|e| format!("invalid search query: {e}"))?;
    if let Some(q) = &query {
        let canonical =
            serde_json::to_value(q).map_err(|e| format!("invalid search query: {e}"))?;
        reject_unknown_properties(&raw, &canonical, "")?;
        q.validate()
            .map_err(|e| format!("invalid search query: {e}"))?;
    }
    Ok(query)
}

/// Every non-null property of `raw` must survive the round trip through the
/// generated model (`canonical`); one that does not is unknown to the spec.
fn reject_unknown_properties(
    raw: &serde_json::Value,
    canonical: &serde_json::Value,
    path: &str,
) -> Result<(), String> {
    use serde_json::Value;
    match (raw, canonical) {
        (Value::Object(r), Value::Object(c)) => {
            for (key, value) in r {
                if value.is_null() {
                    continue;
                }
                let here = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match c.get(key) {
                    Some(cv) => reject_unknown_properties(value, cv, &here)?,
                    None => {
                        return Err(format!(
                            "invalid search query: unknown property `{here}` (not in the spec)"
                        ));
                    }
                }
            }
            Ok(())
        }
        (Value::Array(r), Value::Array(c)) if r.len() == c.len() => {
            r.iter().zip(c).enumerate().try_for_each(|(i, (rv, cv))| {
                reject_unknown_properties(rv, cv, &format!("{path}[{i}]"))
            })
        }
        _ => Ok(()),
    }
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
