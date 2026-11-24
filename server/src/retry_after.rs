//! `Retry-After` on retryable `503`s (#1869).
//!
//! The generated rust-axum `Status503_TheServiceIsCurrentlyUnavailable`
//! variants carry only a problem-detail body — no response headers — so the
//! publish/correlate 503s raised by the publication gate (an open still
//! pending, #1604, or its routing pump failed, #1866) and every other
//! "service unavailable" answer on the REST surface reached clients without a
//! `Retry-After` hint.
//!
//! Rather than patch each 503 site (a drift surface: the next new 503 would
//! forget it), this layer secures the whole class: any `503` response that
//! does not already carry `Retry-After` gets [`DEFAULT_RETRY_AFTER_SECS`].
//! A handler that sets its own, more specific value (e.g. the datasource
//! gateway's back-pressure, #1837) is left untouched.

use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header::RETRY_AFTER};
use axum::middleware::Next;
use axum::response::Response;

/// Delay-seconds advertised on a `503` that carries no handler-specific
/// `Retry-After`. The publication-gate conditions clear as soon as the
/// subscription routing pump catches up, so a short retry is appropriate.
pub(crate) const DEFAULT_RETRY_AFTER_SECS: u64 = 1;

/// Wraps `router` with [`guard`]. The single wiring point used by `main` and
/// by the contract test, so the test exercises the production composition.
pub(crate) fn layer<S: Clone + Send + Sync + 'static>(router: axum::Router<S>) -> axum::Router<S> {
    router.layer(axum::middleware::from_fn(guard))
}

/// Axum middleware adding `Retry-After` to every `503` lacking one.
pub(crate) async fn guard(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    if res.status() == StatusCode::SERVICE_UNAVAILABLE && !res.headers().contains_key(RETRY_AFTER) {
        res.headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from(DEFAULT_RETRY_AFTER_SECS));
    }
    res
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header::RETRY_AFTER};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    fn app() -> Router {
        Router::new()
            .route(
                "/unavailable",
                get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            )
            .route(
                "/unavailable-with-hint",
                get(|| async {
                    (StatusCode::SERVICE_UNAVAILABLE, [(RETRY_AFTER, "30")]).into_response()
                }),
            )
            .route("/ok", get(|| async { StatusCode::OK }))
    }

    async fn call(path: &str) -> Response {
        layer(app())
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn bare_503_gains_default_retry_after() {
        let res = call("/unavailable").await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            res.headers().get(RETRY_AFTER).unwrap(),
            &DEFAULT_RETRY_AFTER_SECS.to_string()
        );
    }

    #[tokio::test]
    async fn handler_specific_retry_after_is_preserved() {
        let res = call("/unavailable-with-hint").await;
        assert_eq!(res.headers().get(RETRY_AFTER).unwrap(), "30");
    }

    #[tokio::test]
    async fn non_503_is_untouched() {
        let res = call("/ok").await;
        assert!(res.headers().get(RETRY_AFTER).is_none());
    }
}
