//! Contract test (#1869): a publish/correlate refused by the publication gate
//! (#1604 — a matching subscription open still pending past the deadline) is a
//! `503` that carries a `Retry-After` header on the wire, through the real
//! generated REST router composed with the production [`retry_after::layer`].

use axum::body::Body;
use axum::http::{Request, StatusCode, header::RETRY_AFTER};
use tower::ServiceExt;

use super::{Event, Journal, ServerImpl, build_server_in_memory, cluster, retry_after};

fn pending_server(name: &str, key: &str) -> (ServerImpl, crate::publication_gate::PendingOpens) {
    let server = build_server_in_memory(vec![Journal::in_memory()], cluster::Topology::single(1));
    *server
        .publication_gate_deadline_override
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(std::time::Duration::from_millis(20));
    let guard = server
        .publication_gate
        .register(&[Event::MessageSubscriptionOpening {
            subscription_key: 1,
            instance_key: 2,
            element_instance_key: 3,
            element_id: "catch".into(),
            message_name: name.into(),
            correlation_key: key.into(),
            kind: nanobpmn_engine_core::MessageSubscriptionKind::IntermediateCatch,
            delivery_key: 4,
        }])
        .expect("an opening registers");
    (server, guard)
}

async fn post(server: ServerImpl, path: &str) -> axum::response::Response {
    let app = retry_after::layer(nanobpm_gateway_rest::server::new::<
        ServerImpl,
        ServerImpl,
        (),
        (),
    >(server));
    app.oneshot(
        Request::post(path)
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"name":"m","correlationKey":"k"}"#))
            .expect("request"),
    )
    .await
    .expect("response")
}

#[tokio::test]
async fn gated_publish_503_carries_retry_after() {
    let (server, _pending) = pending_server("m", "k");
    let res = post(server, "/v2/messages/publication").await;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        res.headers().get(RETRY_AFTER).expect("Retry-After"),
        &retry_after::DEFAULT_RETRY_AFTER_SECS.to_string()
    );
}

#[tokio::test]
async fn gated_correlate_503_carries_retry_after() {
    let (server, _pending) = pending_server("m", "k");
    let res = post(server, "/v2/messages/correlation").await;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        res.headers().get(RETRY_AFTER).expect("Retry-After"),
        &retry_after::DEFAULT_RETRY_AFTER_SECS.to_string()
    );
}
