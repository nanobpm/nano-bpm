//! Optional-request-body acceptance guard (#1531).
//!
//! rust-axum emits `Json<Option<T>>` for a `requestBody` that is not
//! `required: true`. axum's `Json` extractor rejects a request without a JSON
//! `Content-Type` with 415 before the `Option` is considered, so every
//! optional-body operation refused bodyless requests that Camunda 8 accepts
//! (e.g. `POST /v2/jobs/{jobKey}/completion` with no variables).
//! `scripts/postprocess-generated.py` swaps in an `OptionalJson` extractor.
//!
//! This guard covers the whole defect class, not one endpoint: it derives every
//! operation whose request body is optional from the preprocessed spec the
//! generator consumes (`build/spec/`, see `request_field_guard`), sends each
//! one through the real generated router with **no body and no
//! `Content-Type`**, and asserts the request was not rejected by body decoding
//! (415, or a 400/422 JSON-extractor rejection). Any domain answer (404, 400
//! problem detail, 2xx, ...) is fine. A future spec operation is covered
//! automatically.

use std::path::PathBuf;

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{Method, Request, StatusCode};
use serde_yaml::Value;
use tower::ServiceExt;

use super::{Journal, ServerImpl, build_server_in_memory, cluster};

fn spec_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../build/spec");
    assert!(
        dir.is_dir(),
        "{} missing: run `make generate` (it writes the preprocessed spec the generated crate is built from)",
        dir.display()
    );
    dir
}

/// `(METHOD, /v2-path-with-placeholders-filled, operationId)` for every
/// operation whose `requestBody` is not `required: true`.
fn optional_body_operations() -> Vec<(Method, String, String)> {
    let mut ops = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(spec_dir())
        .expect("read build/spec")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
        .collect();
    files.sort();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read spec file");
        let doc: Value = serde_yaml::from_str(&text).expect("parse spec yaml");
        let Some(paths) = doc.get("paths").and_then(Value::as_mapping) else {
            continue;
        };
        for (path, item) in paths {
            let path = path.as_str().expect("path key");
            let Some(item) = item.as_mapping() else {
                continue;
            };
            for (method, op) in item {
                let Some(method) = method.as_str() else {
                    continue;
                };
                let Some(body) = op.get("requestBody") else {
                    continue;
                };
                if body.get("required").and_then(Value::as_bool) == Some(true) {
                    continue;
                }
                let method: Method = method.to_uppercase().parse().expect("http method");
                let op_id = op
                    .get("operationId")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_string();
                ops.push((method, format!("/v2{}", fill_placeholders(path)), op_id));
            }
        }
    }
    ops
}

/// Replaces each `{param}` with a syntactically plausible key (a numeric
/// string satisfies both the numeric-key and free-string patterns).
fn fill_placeholders(path: &str) -> String {
    let mut out = String::new();
    let mut in_param = false;
    for c in path.chars() {
        match c {
            '{' => {
                in_param = true;
                out.push_str("2251799813685249");
            }
            '}' => in_param = false,
            _ if in_param => {}
            _ => out.push(c),
        }
    }
    out
}

fn server() -> ServerImpl {
    build_server_in_memory(vec![Journal::in_memory()], cluster::Topology::single(1))
}

#[tokio::test]
async fn optional_body_operations_accept_a_bodyless_request() {
    let ops = optional_body_operations();
    // Sanity floor: the spec has dozens of optional-body operations; an empty
    // derivation would make this guard vacuous.
    assert!(
        ops.len() >= 50,
        "derived only {} optional-body operations",
        ops.len()
    );

    let app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server());
    let mut failures = Vec::new();
    for (method, path, op_id) in &ops {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .header("host", "localhost")
                    .uri(path)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router responds");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8_lossy(&bytes);
        // axum's JSON extractor rejections answer 415 (missing content type)
        // or 400/422 with a plain-text "...JSON..." message; domain problem
        // details are JSON objects.
        let extractor_rejection = status == StatusCode::UNSUPPORTED_MEDIA_TYPE
            || ((status == StatusCode::BAD_REQUEST
                || status == StatusCode::UNPROCESSABLE_ENTITY)
                && (text.contains("Content-Type") || text.contains("JSON")))
            // A harness defect (missing required header) would make every
            // request short-circuit before body extraction; fail loudly.
            || text.contains("was missing");
        if extractor_rejection {
            failures.push(format!("{op_id} {method} {path} -> {status}: {text}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} optional-body operation(s) rejected a bodyless request:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The other half of the class: a PRESENT optional body must still carry a
/// JSON `Content-Type`. Derived over every optional-body operation, so an
/// extractor variant (e.g. the strict decoder of #1883) that buffers the body
/// and forgets the media-type check is caught on every route it serves.
#[tokio::test]
async fn present_optional_body_still_requires_json_content_type() {
    let ops = optional_body_operations();
    assert!(ops.len() >= 50, "derived only {} operations", ops.len());
    let app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server());
    let mut failures = Vec::new();
    for (method, path, op_id) in &ops {
        for content_type in [Some("text/plain"), None] {
            let mut req = Request::builder()
                .method(method.clone())
                .header("host", "localhost")
                .uri(path);
            if let Some(ct) = content_type {
                req = req.header("content-type", ct);
            }
            let response = app
                .clone()
                .oneshot(req.body(Body::from("{}")).expect("request"))
                .await
                .expect("router responds");
            if response.status() != StatusCode::UNSUPPORTED_MEDIA_TYPE {
                failures.push(format!(
                    "{op_id} {method} {path} (content-type {content_type:?}) -> {}",
                    response.status()
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} non-JSON body(ies) were not rejected with 415:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Strict-decoded bodies (#1883): an unknown property is a 400 carrying the
/// spec's error contract (`application/problem+json` `ProblemDetail`), not a
/// plain-text rejection. Covers every migrated route through the real router.
#[tokio::test]
async fn strict_body_unknown_property_is_a_problem_detail() {
    let app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server());
    for path in [
        "/v2/agent-instances/search",
        "/v2/agent-instances/2251799813685249/history/search",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .header("host", "localhost")
                    .header("content-type", "application/json")
                    .uri(path)
                    .body(Body::from(r#"{"bogus": 1}"#))
                    .expect("request"),
            )
            .await
            .expect("router responds");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        let ct = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert_eq!(ct, "application/problem+json", "{path}");
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let problem: nanobpm_gateway_rest::models::ProblemDetail =
            serde_json::from_slice(&bytes).expect("ProblemDetail body");
        assert_eq!(problem.status, 400, "{path}");
        assert!(
            problem.detail.contains("bogus"),
            "{path}: {}",
            problem.detail
        );
    }
}

#[tokio::test]
async fn bodyless_job_completion_reaches_the_handler() {
    let app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server());
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .header("host", "localhost")
                .uri("/v2/jobs/2251799813685249/completion")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router responds");
    // No such job: the handler's domain answer, not a 415 from the extractor.
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
}

/// Regression guard for the extension-dropping defect in `OptionalJson`:
/// rebuilding the request from only its headers loses the router's
/// `DefaultBodyLimit` extension, so the inner `Json` extraction re-applies the
/// default 2 MiB limit and 413s a body the router explicitly permits. The
/// completion request schema has only optional free-form fields, so a body of
/// `{"variables": {"pad": "<3 MiB>"}}` is schema-valid and must be decoded.
#[tokio::test]
async fn optional_body_above_default_limit_is_accepted_when_router_raises_it() {
    let app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server())
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024));
    let body = format!(
        "{{\"variables\": {{\"pad\": \"{}\"}}}}",
        "x".repeat(3 * 1024 * 1024)
    );
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .header("host", "localhost")
                .uri("/v2/jobs/2251799813685249/completion")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("router responds");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    // The 3 MiB body is decoded (no 413); the handler's domain answer for an
    // unknown job is 404.
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
}
