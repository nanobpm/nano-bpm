//! **Reasoning-control surface** (llama.cpp PR #23971, present in build b9780): probe support +
//! best-effort *mid-generation* reasoning interruption.
//!
//! The server exposes `POST /v1/chat/completions/control` which, given the OpenAI completion id
//! of a live turn, can force the model to **end its current reasoning block** and move on to the
//! final answer (`action: "reasoning_end"`) — without aborting the turn. This is exactly the
//! primitive the loop monitor and the operator's "wrap it up" command want when a model is
//! burning tokens going in circles in its thinking.
//!
//! Two halves of the contract:
//! 1. **Arm** — the original `POST /v1/chat/completions` must carry `reasoning_control: true` so
//!    the server creates the on-demand budget sampler for that turn ([`crate::agent`]'s
//!    `request_body` sets it). Without it the control call returns "reasoning control not enabled".
//! 2. **Interrupt** — `POST /v1/chat/completions/control` `{ "id": <chatcmpl-id>, "action":
//!    "reasoning_end" }`, keyed on the completion id (never a slot index — a finished completion
//!    simply matches nothing, avoiding a TOCTOU).
//!
//! Callers always retain the existing cancel/steer path as a fallback: when the endpoint is
//! absent (older build) or no live completion id is known, behaviour is unchanged.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// The only control action the server currently understands: end the reasoning block now.
pub const ACTION_END_REASONING: &str = "reasoning_end";

/// Does this HTTP status mean the control **route exists** (i.e. the surface is supported)?
///
/// The route is present unless the server 404s / 405s / 501s it. A `400` ("missing completion
/// id" / "unknown control action") from a deliberately-incomplete probe still proves the route
/// is wired, so anything outside the "absent" set counts as supported.
pub fn endpoint_exists(status: u16) -> bool {
    !matches!(status, 404 | 405 | 501)
}

/// Did the control endpoint **accept** a real interrupt request? Only a 2xx counts.
pub fn accepted(status: u16) -> bool {
    (200..300).contains(&status)
}

/// Build the control endpoint URL from an OpenAI-style base URL (e.g.
/// `http://127.0.0.1:8080/v1` → `http://127.0.0.1:8080/v1/chat/completions/control`).
fn control_url(base_url: &str) -> String {
    format!(
        "{}/chat/completions/control",
        base_url.trim_end_matches('/')
    )
}

/// Per-`base_url` cache of the support probe, so we pay the round-trip at most once per endpoint.
/// Only **definitive HTTP responses** are cached; transport errors (server not up yet) are not,
/// so a sidecar that starts later can still be detected on a subsequent call.
fn cache() -> &'static Mutex<BTreeMap<String, bool>> {
    static CACHE: OnceLock<Mutex<BTreeMap<String, bool>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn cached(base_url: &str) -> Option<bool> {
    cache().lock().ok().and_then(|m| m.get(base_url).copied())
}

fn remember(base_url: &str, supported: bool) {
    if let Ok(mut m) = cache().lock() {
        m.insert(base_url.to_string(), supported);
    }
}

fn client() -> Option<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .ok()
}

/// Probe whether `base_url` exposes the reasoning-control surface. Cached per endpoint. Returns
/// `false` on a 404/405/501 or transport error (the safe default — callers fall back to cancel).
///
/// The probe deliberately omits the completion id, so a *supporting* server replies `400
/// missing completion id` (route exists → supported) while an *older* server replies `404`.
pub async fn supports_control(base_url: &str) -> bool {
    if base_url.trim().is_empty() {
        return false;
    }
    if let Some(hit) = cached(base_url) {
        return hit;
    }
    let Some(client) = client() else {
        return false;
    };
    let body = serde_json::json!({ "action": ACTION_END_REASONING });
    match client.post(control_url(base_url)).json(&body).send().await {
        Ok(resp) => {
            let supported = endpoint_exists(resp.status().as_u16());
            remember(base_url, supported);
            supported
        }
        // Transport error (e.g. server not started) — not cached, treated as unsupported for now.
        Err(_) => false,
    }
}

/// Best-effort *end the reasoning block* of the live completion `completion_id` on `base_url`,
/// via the control surface. Returns `true` only if the surface exists **and** accepted the
/// request; on `false` the caller must fall back to the runtime cancel/steer path.
///
/// `completion_id` is the `id` (`chatcmpl-…`) from the in-flight streamed response; the original
/// turn must have been sent with `reasoning_control: true` (see the module docs).
pub async fn end_reasoning(base_url: &str, completion_id: &str) -> bool {
    if completion_id.trim().is_empty() || !supports_control(base_url).await {
        return false;
    }
    let Some(client) = client() else {
        return false;
    };
    let body = serde_json::json!({
        "id": completion_id,
        "action": ACTION_END_REASONING,
    });
    match client.post(control_url(base_url)).json(&body).send().await {
        Ok(resp) => accepted(resp.status().as_u16()),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_exists_treats_only_404_405_501_as_absent() {
        // Present: 2xx, and 4xx that the route itself returns (missing id / unknown action).
        for s in [200, 202, 400, 401, 403, 422, 500, 503] {
            assert!(
                endpoint_exists(s),
                "status {s} should mean the route exists"
            );
        }
        // Absent: the router has no such route / method / it's unimplemented.
        for s in [404, 405, 501] {
            assert!(
                !endpoint_exists(s),
                "status {s} should mean the route is absent"
            );
        }
    }

    #[test]
    fn accepted_is_2xx_only() {
        assert!(accepted(200));
        assert!(accepted(299));
        for s in [199, 300, 400, 404, 500] {
            assert!(!accepted(s), "status {s} should not count as accepted");
        }
    }

    #[test]
    fn control_url_is_built_under_the_base() {
        assert_eq!(
            control_url("http://127.0.0.1:8080/v1"),
            "http://127.0.0.1:8080/v1/chat/completions/control"
        );
        assert_eq!(
            control_url("http://127.0.0.1:8080/v1/"),
            "http://127.0.0.1:8080/v1/chat/completions/control"
        );
    }

    #[tokio::test]
    async fn empty_base_url_is_never_supported() {
        assert!(!supports_control("   ").await);
    }

    #[tokio::test]
    async fn end_reasoning_without_a_completion_id_is_a_noop() {
        assert!(!end_reasoning("http://127.0.0.1:1/v1", "  ").await);
    }
}
