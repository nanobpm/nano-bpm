//! The single-entity `get*` read operations (`getFormByKey`, `getResource`,
//! `getAgentInstance`): one implementation of key parsing, lookup and result
//! projection, shared by the gateway and `engine-wasm`.
//!
//! The outcome is a [`Lookup`]: each surface maps it onto its own transport —
//! the gateway onto HTTP 200 / 404 / 400 problem responses, the WASM engine onto
//! the result JSON / JSON `null` (the 404) / a thrown error (the 400). The
//! status each case carries is decided HERE, once, so the two cannot disagree.

use nanobpmn_engine_core::Key;
use nanobpmn_read_model as readstore;

use crate::{SearchError, agent::agent_instance_result, models, source::ReadSource, types};

/// The outcome of a single-entity read.
#[derive(Debug, Clone, PartialEq)]
pub enum Lookup<T> {
    /// HTTP 200: the entity.
    Found(T),
    /// HTTP 404: no such entity (`detail` is the problem detail).
    NotFound { title: &'static str, detail: String },
    /// HTTP 400: the path key is malformed (`detail` is the problem detail).
    Invalid { title: &'static str, detail: String },
}

impl<T> Lookup<T> {
    /// The HTTP status the gateway answers with for this outcome.
    pub fn status(&self) -> u16 {
        match self {
            Lookup::Found(_) => 200,
            Lookup::NotFound { .. } => 404,
            Lookup::Invalid { .. } => 400,
        }
    }
}

/// `GET /v2/forms/{formKey}`. A malformed key is a 404 (no form can have it).
pub fn get_form_by_key<S: ReadSource + ?Sized>(store: &S, raw: &str) -> Lookup<models::FormResult> {
    const TITLE: &str = "Form not found";
    let Ok(key) = raw.parse::<u64>() else {
        return Lookup::NotFound {
            title: TITLE,
            detail: format!("Form key '{raw}' is not a valid key."),
        };
    };
    match store.form_by_key(key) {
        Some(row) => Lookup::Found(form_result(&row)),
        None => Lookup::NotFound {
            title: TITLE,
            detail: format!("No form with key {key}."),
        },
    }
}

/// `GET /v2/resources/{resourceKey}`. A malformed key is a 404.
pub fn get_resource<S: ReadSource + ?Sized>(
    store: &S,
    raw: &str,
) -> Lookup<models::ResourceResult> {
    const TITLE: &str = "Resource not found";
    let Ok(key) = raw.parse::<u64>() else {
        return Lookup::NotFound {
            title: TITLE,
            detail: format!("Resource key '{raw}' is not a valid key."),
        };
    };
    match store.resource_by_key_meta(key) {
        Some(row) => Lookup::Found(resource_result(&row)),
        None => Lookup::NotFound {
            title: TITLE,
            detail: format!("No resource with key {key}."),
        },
    }
}

/// `GET /v2/agent-instances/{agentInstanceKey}`. A malformed (or zero) key is a
/// 400. `fetch` runs the read-model lookup.
pub fn get_agent_instance<E>(
    raw: &str,
    fetch: impl FnOnce(Key) -> Result<Option<readstore::AgentInstanceRow>, E>,
) -> Result<Lookup<models::AgentInstanceResult>, SearchError<E>> {
    let key: Key = match raw.parse() {
        Ok(k) if k > 0 => k,
        _ => {
            return Ok(Lookup::Invalid {
                title: "Invalid agent instance key",
                detail: format!("Agent instance key '{raw}' is not a valid key."),
            });
        }
    };
    match fetch(key).map_err(SearchError::Store)? {
        Some(row) => Ok(Lookup::Found(
            agent_instance_result(&row).map_err(SearchError::Projection)?,
        )),
        None => Ok(Lookup::NotFound {
            title: "Agent instance not found",
            detail: format!("No agent instance with key {key}."),
        }),
    }
}

pub fn form_result(row: &readstore::FormRow) -> models::FormResult {
    models::FormResult::new(
        row.tenant_id.clone(),
        row.form_id.clone(),
        row.schema.clone(),
        row.version as i64,
        models::FormKey(row.form_key.to_string()),
    )
}

pub fn resource_result(row: &readstore::ResourceMetaRow) -> models::ResourceResult {
    let version_tag = match &row.version_tag {
        Some(t) => types::Nullable::Present(t.clone()),
        None => types::Nullable::Null,
    };
    models::ResourceResult::new(
        row.resource_name.clone(),
        row.version,
        version_tag,
        row.resource_id.clone(),
        row.tenant_id.clone(),
        models::ResourceKey(row.resource_key.to_string()),
    )
}
