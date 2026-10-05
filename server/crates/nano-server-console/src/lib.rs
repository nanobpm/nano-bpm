//! Embedded web console for the self-contained single-node distribution.
//!
//! Feature-gated behind `console`. Serves a single-page app (the built Vite/React
//! bundle under `../console/dist`) at `/console` and a small JSON API under
//! `/console/api/*` that reads cluster/topology state straight off [`ServerImpl`].
//!
//! Design constraints (deliberate):
//! - This namespace is **separate** from the generated Camunda REST surface. The
//!   console API is nanobpmn-specific and MUST NOT leak into `spec/`, `generated/`,
//!   or `spec-patches/`.
//! - In debug builds `rust-embed` reads assets from disk (live frontend reload);
//!   release builds bake them into the binary for a single-file distribution.
//! - Everything here is additive and feature-gated, so the default gateway build
//!   is unaffected.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use axum::{
    Router,
    extract::{
        ConnectInfo, FromRequestParts, Path, Query, RawQuery, State,
        ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header, request::Parts},
    response::{
        IntoResponse, Json, Redirect, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{any, get},
};
use futures_util::stream::{Stream, unfold};
use futures_util::{SinkExt, StreamExt};
use nano_server_runtime::backpressure::SlaMode;
use nano_server_runtime::cluster::{RecoveryCounts, Topology};
use nano_server_storage::readstore::ReadModel;
use nanobpmn_engine_core::bpmn::parse_bpmn;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

pub mod agent_brief;
pub mod config;
pub mod connectors;
mod envelope_scan;
pub mod extensions;
pub mod projects;
pub mod pty;
pub mod server_update;
pub mod standalone;
pub mod terminal_settings;
// The in-memory instance-trace store was extracted into the `nano-trace-store`
// leaf crate (ADR 0064 Phase 3) to break the console↔core import cycle. Alias
// it back to `trace` here so every `trace::…` path in this module keeps
// resolving unchanged.
pub use nano_trace_store as trace;
pub mod trigger_sources;
pub mod triggers;
pub mod urban;
pub mod worker_export;
pub mod workers;
pub mod workspace;

/// The built frontend bundle. Path is relative to this source file
/// (`server/crates/nano-server-console/src/`), so it points at the repo-level
/// `console/dist`.
///
/// ADR 0034 ships two console build profiles. The default `console` feature
/// bakes in the full "studio" RAD IDE from `console/dist`. The additive
/// `console-observe` feature swaps in the lean operator bundle from
/// `console/dist-observe` (~158KB gzip vs ~4.7MB) — build it first with
/// `npm run build:observe` in `console/`. Only one `Assets` is compiled.
#[cfg(not(feature = "console-observe"))]
#[derive(RustEmbed)]
#[folder = "../../../console/dist"]
struct Assets;

#[cfg(feature = "console-observe")]
#[derive(RustEmbed)]
#[folder = "../../../console/dist-observe"]
struct Assets;

/// The standalone marketing landing page (self-contained: inline canvas particle
/// effect, no external assets), served at `/`.
const LANDING_HTML: &str = include_str!("landing.html");

/// The standalone feature-comparison page (self-contained), served at
/// `/features`.
const FEATURES_HTML: &str = include_str!("features.html");

/// The standalone runtime process-optimization explainer (self-contained,
/// inline SVG diagrams), served at `/optimization`.
const OPTIMIZATION_HTML: &str = include_str!("optimization.html");

/// The standalone stack-positioning page (self-contained) explaining where
/// Nano BPM and Nano Workforce sit relative to coding agents, served at
/// `/stack`.
const STACK_HTML: &str = include_str!("stack.html");

/// The "Where Nano sits in the landscape" comparison table, generated from
/// `website/data/landscape.json` by `website/build.mjs` — the single source of
/// truth it shares with the public `/architecture#landscape` page. Injected into
/// [`STACK_HTML`] at the `<!--LANDSCAPE_TABLE-->` marker so the console copy can
/// never drift from the public one. Regenerate with `node website/build.mjs`.
const LANDSCAPE_TABLE_HTML: &str = include_str!("landscape.gen.html");

/// Marker in `stack.html` where the generated landscape table is spliced in.
const LANDSCAPE_MARKER: &str = "<!--LANDSCAPE_TABLE-->";

/// The fully assembled `/stack` page: [`STACK_HTML`] with the generated
/// landscape table spliced in at [`LANDSCAPE_MARKER`]. Built once on first use.
///
/// Splices via [`str::split_once`] rather than [`str::replace`] so the marker
/// must appear *exactly once*: `split_once` matches only the first occurrence,
/// and the `rest.contains(...)` assert makes a duplicated marker a loud panic
/// instead of silently injecting the table twice. A missing marker is likewise
/// a panic — the table must never be silently dropped.
static STACK_PAGE: LazyLock<String> = LazyLock::new(|| {
    let (head, rest) = STACK_HTML.split_once(LANDSCAPE_MARKER).unwrap_or_else(|| {
        panic!("stack.html is missing the {LANDSCAPE_MARKER} landscape-table marker")
    });
    assert!(
        !rest.contains(LANDSCAPE_MARKER),
        "stack.html contains the {LANDSCAPE_MARKER} landscape-table marker more than once",
    );
    format!("{head}{}{rest}", LANDSCAPE_TABLE_HTML.trim())
});

/// Result of a console API core handler: a JSON body on success, or an HTTP
/// status + message on failure. The binary's generated trait layer
/// (`server/src/console_api.rs`) maps these onto the spec's typed response
/// variants.
pub type ApiResult = Result<serde_json::Value, (StatusCode, String)>;

/// The console's view of the running gateway node (ADR 0064 Phase 3, Option B
/// seam).
///
/// The web console reaches the live gateway **only** through this object-safe
/// trait, so the binary's `ServerImpl` god-object never crosses the crate
/// boundary. The binary implements it in `server/src/console_api.rs` and hands
/// the [`router`] an `Arc<dyn ConsoleServer>` ([`ConsoleServerRef`]). Every
/// console handler takes `&dyn ConsoleServer`, and the binary's generated
/// `impl apis::* for ServerImpl` blocks pass `&ServerImpl`, which coerces.
#[async_trait::async_trait]
pub trait ConsoleServer: Send + Sync + 'static {
    /// The read-model projection backing every list/detail view.
    fn store(&self) -> &Arc<ReadModel>;

    /// The in-memory instance-trace store (studio "Traces" tab).
    fn trace_store(&self) -> &trace::TraceStore;

    /// The static cluster topology (node/partition/replica layout).
    fn cluster_topology(&self) -> &Topology;

    /// The current backpressure SLA mode.
    fn sla_mode(&self) -> SlaMode;

    /// Switch the backpressure SLA mode at runtime.
    async fn switch_sla_mode(&self, mode: SlaMode);

    /// Whether Raft replication is enabled on this node.
    fn raft_enabled(&self) -> bool;

    /// Per-node recovery counters for the topology dashboard.
    fn recovery_counts(&self) -> RecoveryCounts;

    /// Live Raft leader/term for a partition this node hosts, as
    /// `(current_leader, current_term)`; `None` when the node does not host the
    /// group (the caller then falls back to the static topology leader).
    fn raft_partition_metrics(&self, partition: u64) -> Option<(Option<u32>, u64)>;

    /// Overlay the live engine `Activated` lease state onto a set of job keys for
    /// the instance-detail view. Returns an empty map when this node does not
    /// host the partition (non-leader) — the caller then leaves the read-model
    /// rows as-is.
    async fn instance_job_overlay(&self, partition: u64, keys: Vec<u64>) -> HashMap<u64, LiveJob>;
}

/// A shared, type-erased handle to the gateway node — the console router's axum
/// state (Option B seam, ADR 0064 Phase 3).
pub type ConsoleServerRef = Arc<dyn ConsoleServer>;

/// Mounts the console SPA and its JSON API onto the gateway.
pub fn router(server: ConsoleServerRef) -> Router {
    Router::new()
        .route("/", get(landing))
        .route("/features", get(features))
        .route("/optimization", get(optimization))
        .route("/stack", get(stack))
        .route("/swagger", get(swagger_index))
        .route("/swagger/", get(swagger_index))
        .route("/swagger/{*path}", get(swagger_asset))
        .route("/asyncapi", get(asyncapi_index))
        .route("/asyncapi/", get(asyncapi_index))
        .route("/asyncapi/{*path}", get(asyncapi_asset))
        .route("/docs", get(docs_index))
        .route("/docs/", get(docs_index))
        .route("/docs/{*path}", get(docs_asset))
        // Agent authoring surface (ADR 0051): the "point your agent here" brief
        // and its `llms.txt` discovery index. Rendered live per node (base URL
        // from the request, projects root + packs from disk) so an agent can act
        // with no other context. Hand-wired text responses, not in the OpenAPI
        // spec. Under any console profile these are read-only.
        .route("/agent", get(agent_brief_md))
        .route("/agent/", get(agent_brief_md))
        .route("/agent.md", get(agent_brief_md))
        .route("/llms.txt", get(llms_txt))
        .route("/whitepaper", get(whitepaper_index))
        .route("/whitepaper/", get(whitepaper_index))
        .route(
            "/console/api/gateway-proxy/{*path}",
            get(gateway_proxy)
                .post(gateway_proxy)
                .put(gateway_proxy)
                .delete(gateway_proxy)
                .patch(gateway_proxy),
        )
        // Console App View reverse proxy (ADR 0057, issue #638 — Slice 5). A dumb
        // same-origin passthrough to a *running* app's declared UI port on
        // loopback, so the studio can frame the app's own UI even when the host
        // isn't the user's machine (and without mixed content). It injects no
        // auth (the app authenticates itself). HTTP is proxied directly;
        // WebSocket upgrades are tunneled byte-opaquely to the same port (ADR
        // 0057 §3 amendment, issue #1054). Other Upgrade streams stay refused.
        .route("/console/app-view/{name}", any(app_view_root_redirect))
        .route("/console/app-view/{name}/", any(app_view_proxy_index))
        .route("/console/app-view/{name}/{*rest}", any(app_view_proxy))
        // App-shipped left-rail icon (ADR 0057, issue #638). When a project's
        // `ui.icon` names a project asset path (not a bundled glyph), the rail
        // renders it via <img> from this path-guarded, image-only route.
        .route("/console/app-view-icon/{name}", get(app_view_icon))
        // Streaming / binary / static routes that are intentionally excluded
        // from the console OpenAPI spec stay hand-wired here. Every typed
        // `/console/api/*` operation is served by the generated rust-axum router
        // (see `generated_api` and the merge in `main.rs`).
        .route("/console/api/stream", get(stream))
        .route("/console/api/workers/{name}/logs", get(worker_logs))
        // The vendored `@nanobpm/urban` type surface for the Studio editor's
        // Monaco IntelliSense. Hand-wired (not in the OpenAPI spec) like the
        // embedded worker SDK / Deno types it sits beside: a static text asset
        // baked into the binary, served verbatim.
        .route("/console/api/urban-types", get(urban_types))
        // GET on this path returns text OR a binary descriptor via `X-File-*`
        // headers, so it stays hand-wired; PUT/POST/DELETE are owned by the
        // generated router (axum merges the differing methods on the same path).
        .route("/console/api/projects/{name}/file", get(project_file_get))
        .route("/console/api/projects/{name}/logs", get(project_logs))
        .route("/console/api/projects/{name}/export", get(project_export))
        .route(
            "/console/api/projects/{name}/derived-models",
            get(project_derived_models),
        )
        // Loopback-only host filesystem browser for the Import-by-reference
        // picker (ADR 0041). Hand-wired (not in the OpenAPI spec) because it
        // exposes the server's filesystem and is gated on the peer being local.
        .route("/console/api/fs/browse", get(fs_browse))
        .route("/console/api/projects/{name}/pty", get(pty::pty_ws))
        // Terminal enablement: read (informational, ungated) + toggle (loopback
        // -gated, since it turns on a shell). Hand-wired like the other peer-
        // gated endpoints rather than living in the typed OpenAPI spec.
        .route(
            "/console/api/config/terminal",
            get(config_terminal).put(config_terminal_set),
        )
        // Trigger webhook ingress (ADR 0025 phase 2): the universal external
        // emit endpoint. Hand-wired (not in the OpenAPI spec) because it accepts
        // an arbitrary body + custom shared-secret auth and acks after persist.
        // Any external producer — including a pack source driver (§6) — POSTs
        // here. Under the observe profile the console guard refuses it (a
        // mutation), keeping observe truly read-only.
        .route(
            "/console/api/projects/{name}/hooks/{triggerId}",
            axum::routing::post(project_hook),
        )
        .route(
            "/console/api/export-workers-app",
            axum::routing::post(workers_export),
        )
        .route("/console", get(spa_index))
        .route("/console/", get(spa_index))
        .route("/console/{*path}", get(spa_asset))
        .with_state(server)
}

/// Test-only: a minimal router mounting just the trigger ingress route (the
/// real [`project_hook`] handler, which takes no `State`), so a hermetic
/// integration test can drive a pack source's driver end-to-end over HTTP.
#[cfg(test)]
pub(crate) fn test_ingress_router() -> Router {
    Router::new().route(
        "/console/api/projects/{name}/hooks/{triggerId}",
        axum::routing::post(project_hook),
    )
}

// The `*path` catch-all must not swallow `/console/api/*`. axum's matchit router
// ranks literal segments above wildcards, so the API routes above always win;
// the catch-all only handles SPA asset/deep-link requests. The list of
// `/console/api/...` routes is registered explicitly to keep that guarantee
// obvious rather than relying on registration order.

// ---------------------------------------------------------------------------
// Static asset serving (SPA)
// ---------------------------------------------------------------------------

/// Serves `index.html` for the SPA entry points (`/console`, `/console/`).
async fn spa_index(headers: HeaderMap) -> Response {
    serve_embedded("index.html", accepted_encodings(&headers))
}

/// Serves a built asset by path under `/console/`. Unknown paths that are not
/// API routes fall back to `index.html` so client-side routing (deep links like
/// `/console/explorer`) works on a full-page load.
async fn spa_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/');
    let enc = accepted_encodings(&headers);
    if Assets::get(path).is_some() {
        serve_embedded(path, enc)
    } else {
        // SPA fallback: let the client router resolve the route.
        serve_embedded("index.html", enc)
    }
}

/// Looks an asset up in the embedded bundle and returns it with a guessed
/// content type. Serving order (ADR 0034):
///   1. a build-time precompressed sibling — `<path>.br` (Brotli) or `<path>.gz`
///      (gzip) — when the client accepts that encoding. These are produced by
///      `console/scripts/precompress.mjs` at max quality, so the gateway streams
///      them with zero compression CPU on the hot path.
///   2. otherwise the raw asset, gzip-ed on the fly when worth it. This keeps
///      CI's stub bundle (no siblings) and any hand-built `dist` working.
///   3. otherwise the raw bytes.
fn serve_embedded(path: &str, enc: AcceptedEncodings) -> Response {
    if enc.br
        && let Some(resp) = precompressed_sibling(path, "br")
    {
        return resp;
    }
    if enc.gzip
        && let Some(resp) = precompressed_sibling(path, "gzip")
    {
        return resp;
    }
    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            let mime_str = mime.as_ref().to_string();
            let bytes = content.data.into_owned();
            if enc.gzip
                && is_compressible(&mime_str)
                && bytes.len() >= 1024
                && let Some(gz) = gzip(&bytes)
            {
                return (
                    [
                        (header::CONTENT_TYPE, mime_str),
                        (header::CONTENT_ENCODING, "gzip".to_string()),
                        (header::VARY, "Accept-Encoding".to_string()),
                    ],
                    gz,
                )
                    .into_response();
            }
            (
                [
                    (header::CONTENT_TYPE, mime_str),
                    (header::VARY, "Accept-Encoding".to_string()),
                ],
                bytes,
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            "console assets not found — build the frontend first (`make console` or \
             `cd console && npm install && npm run build`)",
        )
            .into_response(),
    }
}

/// Serves a build-time precompressed sibling of `path` (`<path>.br` or
/// `<path>.gz`) if one is embedded, returning `None` so the caller falls back
/// when it is absent. `encoding` is the `Content-Encoding` token (`"br"` /
/// `"gzip"`); the content type is guessed from the *original* path so a
/// `foo.js.br` is still served as JavaScript.
fn precompressed_sibling(path: &str, encoding: &str) -> Option<Response> {
    let sibling = format!("{path}.{}", if encoding == "br" { "br" } else { "gz" });
    let content = Assets::get(&sibling)?;
    let mime = mime_guess::from_path(path)
        .first_or_octet_stream()
        .as_ref()
        .to_string();
    Some(
        (
            [
                (header::CONTENT_TYPE, mime),
                (header::CONTENT_ENCODING, encoding.to_string()),
                (header::VARY, "Accept-Encoding".to_string()),
            ],
            content.data.into_owned(),
        )
            .into_response(),
    )
}

/// The content encodings a client advertised in `Accept-Encoding`.
#[derive(Clone, Copy)]
struct AcceptedEncodings {
    br: bool,
    gzip: bool,
}

/// Parses `Accept-Encoding` into the subset of encodings we can serve. `q=0`
/// niceties are ignored — clients that list an encoding at all accept it.
fn accepted_encodings(headers: &HeaderMap) -> AcceptedEncodings {
    let mut enc = AcceptedEncodings {
        br: false,
        gzip: false,
    };
    if let Some(val) = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
    {
        for token in val.split(',') {
            match token.trim().split(';').next().map(str::trim) {
                Some("br") => enc.br = true,
                Some("gzip") => enc.gzip = true,
                _ => {}
            }
        }
    }
    enc
}

/// Whether a MIME type benefits from gzip (text-like, JS/JSON, wasm, SVG).
/// Already-compressed binaries (png/woff2/…) are left untouched.
fn is_compressible(mime: &str) -> bool {
    mime.starts_with("text/")
        || matches!(
            mime,
            "application/javascript" | "application/json" | "application/wasm" | "image/svg+xml"
        )
}

/// Gzip a byte slice; returns `None` on the (unexpected) encoder failure so the
/// caller transparently falls back to the uncompressed body.
fn gzip(bytes: &[u8]) -> Option<Vec<u8>> {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(bytes).ok()?;
    enc.finish().ok()
}

// ---------------------------------------------------------------------------
// Landing page + Swagger UI (root-level, console feature only)
// ---------------------------------------------------------------------------

/// Serves the standalone marketing landing page at `/`.
async fn landing() -> Response {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        LANDING_HTML,
    )
        .into_response()
}

/// Serves the standalone feature-comparison page at `/features`.
async fn features() -> Response {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        FEATURES_HTML,
    )
        .into_response()
}

/// Serves the standalone runtime process-optimization explainer at
/// `/optimization`.
async fn optimization() -> Response {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        OPTIMIZATION_HTML,
    )
        .into_response()
}

/// Serves the standalone stack-positioning page at `/stack`.
async fn stack() -> Response {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        STACK_PAGE.as_str(),
    )
        .into_response()
}

#[cfg(test)]
mod stack_page_tests {
    use super::*;

    #[test]
    fn stack_html_carries_the_landscape_marker() {
        // The single-source-of-truth splice depends on this marker existing; if a
        // future edit drops it, STACK_PAGE's split_once finds no match and panics
        // on first request. This test catches that at test time instead.
        assert!(
            STACK_HTML.contains(LANDSCAPE_MARKER),
            "stack.html must contain the {LANDSCAPE_MARKER} landscape-table marker",
        );
    }

    #[test]
    fn stack_html_carries_exactly_one_landscape_marker() {
        // STACK_PAGE splices via split_once and panics on a duplicate marker, so a
        // second marker would inject the table twice (loudly). Guard the invariant
        // here too so the failure is caught at test time, not first request.
        assert_eq!(
            STACK_HTML.matches(LANDSCAPE_MARKER).count(),
            1,
            "stack.html must contain the {LANDSCAPE_MARKER} landscape-table marker exactly once",
        );
    }

    #[test]
    fn assembled_page_injects_the_generated_table_and_drops_the_marker() {
        let page = STACK_PAGE.as_str();
        assert!(
            !page.contains(LANDSCAPE_MARKER),
            "assembled /stack page still contains the un-substituted marker",
        );
        // Structural markers that only exist in the generated landscape artifact
        // (landscape.gen.html) must be present after injection. Assert on the
        // artifact's stable scaffolding — its `@generated` provenance header and
        // table structure — rather than specific positioning copy from
        // website/data/landscape.json, so edits to that copy don't break this
        // splice test.
        assert!(page.contains("@generated by website/build.mjs"));
        assert!(page.contains("<table class=\"landscape\">"));
        assert!(page.contains("<th scope=\"row\" class=\"feature\">"));
    }
}

/// Reconstructs the `scheme://host` base URL a client used to reach this node,
/// from the request headers, so a served document can print URLs the caller can
/// actually reach (through whatever proxy/host mapping is in front of us).
///
/// Prefers `X-Forwarded-Proto`/`Host` (set by a reverse proxy); falls back to the
/// `Host` header with an `http` scheme, and finally to `http://localhost` when no
/// host is advertised at all. No trailing slash.
fn request_base_url(headers: &HeaderMap) -> String {
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty())
        .unwrap_or("localhost");
    // Only honour a forwarded scheme we actually emit URLs for; a misconfigured
    // proxy (or a hostile client) supplying anything else must not leak into the
    // links a downstream agent might auto-follow. Normalised to canonical case.
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(str::trim)
        .filter(|s| s.eq_ignore_ascii_case("https"))
        .map(|_| "https")
        .unwrap_or("http");
    format!("{scheme}://{host}")
}

/// Serves the agent authoring brief (ADR 0051) at `/agent` (+ `/agent.md`) — the
/// "point your agent here" Markdown, rendered live for this node.
async fn agent_brief_md(headers: HeaderMap) -> Response {
    let body = agent_brief::render(&request_base_url(&headers));
    serve_text("text/markdown; charset=utf-8", body, &headers)
}

/// Serves the `/llms.txt` discovery index (ADR 0051): a short, link-first pointer
/// at the agent brief and this node's machine-readable specs.
async fn llms_txt(headers: HeaderMap) -> Response {
    let body = agent_brief::render_llms_txt(&request_base_url(&headers));
    serve_text("text/plain; charset=utf-8", body, &headers)
}

/// Serves a freshly-rendered text body with the given content type, gzipping it
/// when the client advertised `gzip` (these documents are regenerated per
/// request, so they are not pre-compressed like the embedded SPA assets).
fn serve_text(content_type: &'static str, body: String, headers: &HeaderMap) -> Response {
    // Regenerated per request (base URL, packs, templates are node/request
    // specific), so an intermediary must never cache and replay one host's or
    // client's variant to another. `Vary: Accept-Encoding` is set on both branches
    // so the encoding negotiation is honoured regardless of which we return.
    let enc = accepted_encodings(headers);
    if enc.gzip
        && let Some(gz) = gzip(body.as_bytes())
    {
        return (
            [
                (header::CONTENT_TYPE, content_type),
                (header::CONTENT_ENCODING, "gzip"),
                (header::VARY, "Accept-Encoding"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            gz,
        )
            .into_response();
    }
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::VARY, "Accept-Encoding"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

/// Serves the Swagger UI shell at `/swagger`.
async fn swagger_index(headers: HeaderMap) -> Response {
    serve_embedded("swagger/index.html", accepted_encodings(&headers))
}

/// Serves Swagger UI assets and the bundled OpenAPI spec under `/swagger/`. All
/// files (the UI assets and `openapi.json`) are built into the frontend bundle
/// under `dist/swagger/`.
async fn swagger_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/');
    serve_embedded(&format!("swagger/{path}"), accepted_encodings(&headers))
}

/// Serves the Falcon Protocol reference (AsyncAPI) at `/asyncapi`. The
/// page is generated at build time from `docs/falcon.asyncapi.yaml`
/// (see `console/scripts/copy-asyncapi.mjs`) into `dist/asyncapi/index.html`.
async fn asyncapi_index(headers: HeaderMap) -> Response {
    serve_embedded("asyncapi/index.html", accepted_encodings(&headers))
}

/// Serves any further assets under `/asyncapi/` (the page is currently a single
/// self-contained `index.html`, but this keeps the route shape parallel to
/// `/swagger/`).
async fn asyncapi_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/');
    serve_embedded(&format!("asyncapi/{path}"), accepted_encodings(&headers))
}

/// Serves the bundled documentation website at `/docs`. The pages are generated
/// at build time from `README.md` (see `console/scripts/build-docs.mjs`) into
/// `dist/docs/*.html`, one page per README H2 section.
async fn docs_index(headers: HeaderMap) -> Response {
    serve_embedded("docs/index.html", accepted_encodings(&headers))
}

/// Serves a documentation page (or asset) under `/docs/`. Page links are
/// extensionless (`/docs/usage`), so a trailing `.html` is added when the path
/// carries no file extension; explicit asset paths pass through unchanged.
async fn docs_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Response {
    let path = path.trim_start_matches('/').trim_end_matches('/');
    let enc = accepted_encodings(&headers);
    if path.is_empty() {
        return serve_embedded("docs/index.html", enc);
    }
    let last = path.rsplit('/').next().unwrap_or(path);
    let key = if last.contains('.') {
        format!("docs/{path}")
    } else {
        format!("docs/{path}.html")
    };
    serve_embedded(&key, enc)
}

/// Serves the bundled whitepaper at `/whitepaper`. The page is generated at
/// build time from `docs/whitepaper.md` (see
/// `console/scripts/build-whitepaper.mjs`) into `dist/whitepaper/index.html`, so
/// it refreshes on every console build and is embedded in the release binary.
async fn whitepaper_index(headers: HeaderMap) -> Response {
    serve_embedded("whitepaper/index.html", accepted_encodings(&headers))
}

#[derive(Serialize)]
pub struct TopologyDto {
    /// This gateway node's id.
    node_id: u32,
    num_nodes: u32,
    num_partitions: u64,
    replication_factor: u32,
    /// Whether per-partition Raft replication is active on this node.
    raft_enabled: bool,
    gateway_version: String,
    nodes: Vec<NodeDto>,
    partitions: Vec<PartitionDto>,
}

#[derive(Serialize)]
struct NodeDto {
    node_id: u32,
    /// `http://host:port` base URL (empty for self in a single-node cluster).
    address: String,
    is_self: bool,
}

#[derive(Serialize)]
struct PartitionDto {
    /// 1-based partition id (Camunda display convention).
    partition_id: u64,
    /// Nodes replicating this partition.
    replicas: Vec<u32>,
    /// The static owner (preferred leader) of this partition.
    owner: u32,
    /// The current serving leader: the live Raft leader when Raft is active,
    /// otherwise the static owner.
    leader: Option<u32>,
    /// Live Raft term for this partition's group (when Raft is active here).
    raft_term: Option<u64>,
    /// `true` when the partition is being served by a failover incumbent rather
    /// than its owner (the live leader differs from the owner). While `true` the
    /// owner is down or catching up — the cluster is not fully rebalanced. Only
    /// meaningful for partitions this node hosts a Raft group for; `false`
    /// otherwise (this node can't observe their live leader).
    recovering: bool,
}

/// `GET /console/api/topology` — the cluster/topology view's data source.
pub fn topology(server: &dyn ConsoleServer) -> TopologyDto {
    let topology = server.cluster_topology();
    let num_nodes = topology.num_nodes();
    let num_partitions = topology.num_partitions;
    let raft_on = server.raft_enabled();

    let nodes: Vec<NodeDto> = (0..num_nodes)
        .map(|node| NodeDto {
            node_id: node,
            address: topology.peer_addr(node).unwrap_or("").to_string(),
            is_self: node == topology.node_id,
        })
        .collect();

    let partitions: Vec<PartitionDto> = (0..num_partitions)
        .map(|p| {
            let owner = topology.owner_of(p);
            // Prefer the live Raft leader/term when this node hosts the group;
            // fall back to the static topology leader otherwise.
            let (leader, term, hosted) = match server.raft_partition_metrics(p) {
                Some((leader, term)) => (leader, Some(term), true),
                None => (Some(topology.leader_of(p)), None, false),
            };
            // A partition is "recovering" when we can see its live leader (we host
            // the group) and it is not its owner — a failover incumbent is serving
            // it while the owner is down or catching up.
            let recovering = hosted && leader != Some(owner);
            PartitionDto {
                partition_id: p + 1,
                replicas: topology.replicas_of(p),
                owner,
                leader,
                raft_term: term,
                recovering,
            }
        })
        .collect();

    TopologyDto {
        node_id: topology.node_id,
        num_nodes,
        num_partitions,
        replication_factor: topology.effective_rf(),
        raft_enabled: raft_on,
        gateway_version: env!("NANOBPM_VERSION").to_string(),
        nodes,
        partitions,
    }
}

// ---------------------------------------------------------------------------
// Cluster health (live per-node liveness probe)
// ---------------------------------------------------------------------------

/// Live health of every node in the cluster, as seen from this gateway. Unlike
/// [`topology`] (which reports the *configured* membership), this actively
/// probes each peer's always-on `GET /v2/topology` to report whether it is
/// reachable right now, its gateway version, and the round-trip latency.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterHealthDto {
    checked_at_ms: u64,
    nodes: Vec<NodeHealthDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeHealthDto {
    node_id: u32,
    /// `http://host:port` base URL (empty for self in a single-node cluster).
    address: String,
    is_self: bool,
    /// Whether the node answered the probe within the timeout.
    reachable: bool,
    /// The node's reported gateway version (when reachable).
    version: Option<String>,
    /// Probe round-trip time in milliseconds (when reachable).
    latency_ms: Option<u64>,
    /// Why the probe failed (when unreachable).
    error: Option<String>,
}

/// Per-peer probe timeout. Generous enough for a loaded node to answer, short
/// enough that one dead peer doesn't stall the whole health response (all peers
/// are probed concurrently, so the endpoint resolves in ~one timeout at worst).
const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// `GET /console/api/cluster/health` — probes every peer concurrently and
/// reports live reachability/version/latency. Self is reported without a network
/// round-trip (it is, by definition, up and serving this request).
pub async fn cluster_health(server: &dyn ConsoleServer) -> ClusterHealthDto {
    let topology = server.cluster_topology();
    let self_id = topology.node_id;
    let self_version = env!("NANOBPM_VERSION").to_string();
    let num_nodes = topology.num_nodes();

    let probes = (0..num_nodes).map(|node| {
        let is_self = node == self_id;
        let address = topology.peer_addr(node).unwrap_or("").to_string();
        let self_version = self_version.clone();
        async move {
            if is_self {
                return NodeHealthDto {
                    node_id: node,
                    address,
                    is_self: true,
                    reachable: true,
                    version: Some(self_version),
                    latency_ms: Some(0),
                    error: None,
                };
            }
            match probe_peer(&address).await {
                Ok((version, latency)) => NodeHealthDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: true,
                    version,
                    latency_ms: Some(latency.as_millis() as u64),
                    error: None,
                },
                Err(err) => NodeHealthDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: false,
                    version: None,
                    latency_ms: None,
                    error: Some(err),
                },
            }
        }
    });

    let nodes = futures_util::future::join_all(probes).await;
    let checked_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    Json(ClusterHealthDto {
        checked_at_ms,
        nodes,
    })
    .0
}

/// Probes one peer's `GET {base_url}/v2/topology`, returning its reported
/// `gatewayVersion` and the round-trip latency. Plain HTTP/1.1 (peers are
/// TLS-less, like the falcon uplink). Any transport error, non-2xx
/// status, or timeout is mapped to a short human-readable string.
async fn probe_peer(base_url: &str) -> Result<(Option<String>, Duration), String> {
    use http_body_util::BodyExt;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    if base_url.is_empty() {
        return Err("no address configured".to_string());
    }

    let uri: hyper::Uri = format!("{}/v2/topology", base_url.trim_end_matches('/'))
        .parse()
        .map_err(|e| format!("bad peer url: {e}"))?;

    let client: Client<_, http_body_util::Empty<hyper::body::Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();

    let started = std::time::Instant::now();
    let fut = async {
        let resp = client.get(uri).await.map_err(|e| format!("connect: {e}"))?;
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("read: {e}"))?
            .to_bytes();
        Ok::<_, String>((status, body))
    };

    let (status, body) = tokio::time::timeout(HEALTH_PROBE_TIMEOUT, fut)
        .await
        .map_err(|_| "timeout".to_string())??;

    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }

    // gatewayVersion is best-effort: a reachable node with an unparseable body
    // is still "up", just without a version string.
    let version = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.get("gatewayVersion")
                .and_then(|s| s.as_str())
                .map(str::to_string)
        });

    Ok((version, started.elapsed()))
}

// ---------------------------------------------------------------------------
// Cross-origin gateway proxy
// ---------------------------------------------------------------------------

/// Same-origin proxy for arbitrary Camunda REST calls (e.g. `/v2/deployments`,
/// `/v2/process-instances`) targeting a **foreign** gateway — typically a
/// Camunda 8 self-managed cluster running on a different port than the Nano
/// console. Browsers block direct cross-origin `fetch()` unless the target
/// gateway serves CORS headers, which stock `c8run` does not; routing the
/// request through this proxy sidesteps that requirement by making the call
/// server-side.
///
/// Contract:
/// - Client sends `{METHOD} /console/api/gateway-proxy/{path}` with header
///   `X-Gateway-Target: http(s)://host[:port]` and the original request body.
/// - Server forwards `{METHOD} {target}/{path}` verbatim (body + content-type +
///   authorization pass through) and streams the upstream status + body back.
/// - No caching, no rewriting — this is a dumb pass-through so the Camunda
///   REST semantics are unchanged.
///
/// This is deliberately generic (one handler for all `/v2/*`) rather than
/// endpoint-specific: we don't want to grow a shim every time the Camunda REST
/// surface adds a route.
async fn gateway_proxy(
    Path(rest): Path<String>,
    method: axum::http::Method,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let target = match headers
        .get("x-gateway-target")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
    {
        Some(t) if !t.is_empty() => t.trim_end_matches('/').to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "missing or empty X-Gateway-Target header",
            )
                .into_response();
        }
    };

    if !(target.starts_with("http://") || target.starts_with("https://")) {
        return (
            StatusCode::BAD_REQUEST,
            "X-Gateway-Target must be an absolute http(s) URL",
        )
            .into_response();
    }

    let url = format!("{target}/{rest}");
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("proxy client init: {e}"),
            )
                .into_response();
        }
    };

    let up_method = match reqwest::Method::from_bytes(method.as_str().as_bytes()) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("bad method: {e}")).into_response();
        }
    };

    let mut req = client.request(up_method, &url);
    // Forward only the request-shaping headers we actually need. Hop-by-hop
    // headers (Host, Connection, Content-Length) are dropped so reqwest can
    // recompute them for the upstream connection.
    for name in [header::CONTENT_TYPE, header::ACCEPT, header::AUTHORIZATION] {
        if let Some(v) = headers.get(&name) {
            req = req.header(name, v);
        }
    }
    if !body.is_empty() {
        req = req.body(body.to_vec());
    }

    let upstream = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("upstream {url}: {e}")).into_response();
        }
    };

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let ct = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let cd = upstream.headers().get(header::CONTENT_DISPOSITION).cloned();
    let bytes = match upstream.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("upstream read: {e}")).into_response();
        }
    };

    let mut out = Response::new(axum::body::Body::from(bytes));
    *out.status_mut() = status;
    if let Some(v) = ct {
        out.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    if let Some(v) = cd {
        out.headers_mut().insert(header::CONTENT_DISPOSITION, v);
    }
    out
}

// ---------------------------------------------------------------------------
// Console App View reverse proxy (ADR 0057, issue #638 — Slice 5)
//
// A same-origin HTTP passthrough to a *running* app's declared UI port on
// loopback. Same-origin (posture A) so the console can embed the app in a
// sandboxed iframe without mixed-content or cross-origin fetch breakage; the
// app authenticates itself (the console injects no credentials) and we never
// proxy its WebSocket / SSE stream (ADR 0057 §3 — WS deferred, replies 501).

/// Request headers we must never forward verbatim to the upstream: hop-by-hop
/// headers (recomputed per-connection) plus `accept-encoding` — reqwest is
/// built without the `gzip` feature so it can't transparently decode a
/// compressed body, and we forward the body untouched. Forcing identity keeps
/// the response coherent (no `content-encoding` lie).
/// Request headers we must never forward verbatim to the upstream: hop-by-hop
/// headers are recomputed per-connection. `accept-encoding` IS forwarded so the
/// app can compress — reqwest is built without any decode feature, so it hands
/// back the coded bytes untouched and we relay `content-encoding` verbatim (see
/// [`app_view_strip_response_header`]), keeping metadata and body coherent.
fn app_view_strip_request_header(name: &str) -> bool {
    matches!(
        name,
        "host"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "transfer-encoding"
            | "te"
            | "trailer"
            | "upgrade"
            | "content-length"
    )
}

/// Response headers we must not copy back: hop-by-hop, length/transfer framing
/// (the body is re-streamed by axum), the framing guards (`x-frame-options`,
/// `content-security-policy*`) that would otherwise stop the console from
/// embedding the app, and `service-worker-allowed` (a framed app must not be
/// able to widen a service-worker scope beyond its own proxy path onto the
/// console origin). CSP is dropped wholesale rather than surgically edited: an
/// app's own CSP has no authority over the console origin that now frames it,
/// and partial rewriting is error-prone. `content-encoding` is deliberately
/// NOT stripped — reqwest doesn't decode it, so the relayed bytes still match.
fn app_view_strip_response_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-connection"
            | "transfer-encoding"
            | "te"
            | "trailer"
            | "upgrade"
            | "content-length"
            | "x-frame-options"
            | "content-security-policy"
            | "content-security-policy-report-only"
            | "service-worker-allowed"
    )
}

/// Rewrite an upstream `Location` (redirect) header so the browser stays inside
/// the proxied namespace. Handles root-relative (`/foo`) and absolute-loopback
/// (`http://127.0.0.1:{port}/foo`) targets; anything else (cross-host absolute,
/// relative) is passed through unchanged.
fn app_view_rewrite_location(value: &str, name: &str, port: u16) -> String {
    let prefix = format!("/console/app-view/{name}");
    for origin in [
        format!("http://127.0.0.1:{port}"),
        format!("http://localhost:{port}"),
    ] {
        if let Some(rest) = value.strip_prefix(&origin) {
            let rest = if rest.is_empty() { "/" } else { rest };
            return format!("{prefix}{rest}");
        }
    }
    if value.starts_with('/') && !value.starts_with("//") {
        return format!("{prefix}{value}");
    }
    value.to_string()
}

/// Base path (`/console/app-view/{name}`) → redirect to the trailing-slash form
/// so the app's root-relative asset URLs resolve under the proxied namespace.
async fn app_view_root_redirect(Path(name): Path<String>) -> Response {
    if !workspace::is_safe_name(&name) {
        return (StatusCode::BAD_REQUEST, "invalid project name").into_response();
    }
    Redirect::permanent(&format!("/console/app-view/{name}/")).into_response()
}

/// Trailing-slash root (`/console/app-view/{name}/`) — proxies path `""`.
async fn app_view_proxy_index(
    Path(name): Path<String>,
    RawQuery(query): RawQuery,
    method: axum::http::Method,
    headers: HeaderMap,
    MaybeWebSocketUpgrade(ws): MaybeWebSocketUpgrade,
    body: axum::body::Bytes,
) -> Response {
    app_view_proxy_inner(name, String::new(), query, method, headers, ws, body).await
}

/// Wildcard (`/console/app-view/{name}/{*rest}`).
async fn app_view_proxy(
    Path((name, rest)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    method: axum::http::Method,
    headers: HeaderMap,
    MaybeWebSocketUpgrade(ws): MaybeWebSocketUpgrade,
    body: axum::body::Bytes,
) -> Response {
    app_view_proxy_inner(name, rest, query, method, headers, ws, body).await
}

/// An optional [`WebSocketUpgrade`] extractor: `Some` for a well-formed
/// WebSocket handshake, `None` otherwise — which covers *both* a plain HTTP
/// request *and* a request that advertises `Upgrade: websocket` but whose
/// handshake is malformed (the extractor's rejection is swallowed by `.ok()`
/// below). Callers that need to tell those two `None` cases apart must inspect
/// the `Upgrade` header themselves (see `app_view_proxy_inner`, which maps a
/// malformed WS handshake to 400 and a non-WS upgrade token to 501).
/// `WebSocketUpgrade` itself has no `OptionalFromRequestParts` impl (a non-WS
/// request is a hard rejection), so the app-view proxy — one route that must
/// serve *both* HTTP and WebSocket — wraps it here to branch at runtime.
struct MaybeWebSocketUpgrade(Option<WebSocketUpgrade>);

impl<S> FromRequestParts<S> for MaybeWebSocketUpgrade
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Infallible> {
        // `.ok()` swallows the rejection, so `None` here means either "not a
        // WebSocket request at all" or "WebSocket handshake was malformed".
        Ok(Self(
            WebSocketUpgrade::from_request_parts(parts, state)
                .await
                .ok(),
        ))
    }
}

async fn app_view_proxy_inner(
    name: String,
    rest: String,
    query: Option<String>,
    method: axum::http::Method,
    headers: HeaderMap,
    ws: Option<WebSocketUpgrade>,
    body: axum::body::Bytes,
) -> Response {
    if !workspace::is_safe_name(&name) {
        return (StatusCode::BAD_REQUEST, "invalid project name").into_response();
    }

    // Upgrade streams. Transparent WebSocket tunneling (ADR 0057 §3, issue
    // #1054): a request whose `Connection` header contains `upgrade` AND whose
    // `Upgrade` header is `websocket` is bridged byte-opaquely to the app's own
    // UI port (see `app_view_ws_tunnel`). A `websocket` token whose handshake is
    // malformed (so extraction yielded `None`) is a client error → 400. Any
    // OTHER `Upgrade` token (e.g. `h2c`) is refused with 501 — this path is
    // WebSocket-only, and the console never becomes a general stream proxy.
    let connection_upgrades = headers
        .get(header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase().contains("upgrade"))
        .unwrap_or(false);
    if connection_upgrades || headers.contains_key(header::UPGRADE) {
        let is_websocket = headers
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("websocket"))
            .unwrap_or(false);
        return match (is_websocket, ws) {
            (true, Some(ws)) => app_view_ws_tunnel(ws, name, rest, query, headers).await,
            (true, None) => (
                StatusCode::BAD_REQUEST,
                "malformed WebSocket handshake for the app view",
            )
                .into_response(),
            (false, _) => (
                StatusCode::NOT_IMPLEMENTED,
                "only WebSocket upgrades are proxied for the app view; other \
                 Upgrade streams are not supported",
            )
                .into_response(),
        };
    }

    let port = match app_view_resolve_port(&name).await {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let url = match query.as_deref() {
        Some(q) if !q.is_empty() => format!("http://127.0.0.1:{port}/{rest}?{q}"),
        _ => format!("http://127.0.0.1:{port}/{rest}"),
    };

    // Don't follow redirects: we rewrite `Location` so the browser stays inside
    // the proxied namespace.
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("proxy client init: {e}"),
            )
                .into_response();
        }
    };

    let up_method = match reqwest::Method::from_bytes(method.as_str().as_bytes()) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad method: {e}")).into_response(),
    };

    let mut req = client.request(up_method, &url);
    for (k, v) in headers.iter() {
        if app_view_strip_request_header(k.as_str()) {
            continue;
        }
        req = req.header(k, v);
    }
    if !body.is_empty() {
        req = req.body(body.to_vec());
    }

    let upstream = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("upstream {url}: {e}")).into_response();
        }
    };

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let up_headers = upstream.headers().clone();
    // Stream the upstream body rather than buffering it: a running app can emit
    // an arbitrarily large (or long-lived) response — e.g. a file download — and
    // buffering it whole in the console would be a memory-exhaustion hazard.
    let body_stream = axum::body::Body::from_stream(upstream.bytes_stream());

    let mut out = Response::new(body_stream);
    *out.status_mut() = status;
    let out_headers = out.headers_mut();
    for (k, v) in up_headers.iter() {
        let key = k.as_str();
        if app_view_strip_response_header(key) {
            continue;
        }
        if key == "location" {
            if let Ok(loc) = v.to_str() {
                let rewritten = app_view_rewrite_location(loc, &name, port);
                if let Ok(hv) = axum::http::HeaderValue::from_str(&rewritten) {
                    out_headers.append(header::LOCATION, hv);
                }
            }
            continue;
        }
        // `append` (not `insert`) so multi-valued headers — notably
        // `set-cookie` — survive intact.
        out_headers.append(k.clone(), v.clone());
    }
    out
}

/// Resolve a running app's embedded-UI loopback port, or the error response the
/// proxy should return. Shared by the HTTP and WebSocket paths so both enforce
/// the ADR 0057 guards in the same order: `is_running` → 503, then the app must
/// declare a reachable UI port → else 404 (headless). The safe-name check
/// happens earlier, in `app_view_proxy_inner`.
async fn app_view_resolve_port(name: &str) -> Result<u16, Box<Response>> {
    let sup = projects::supervisor();
    if !sup.is_running(name).await {
        return Err(Box::new(
            (StatusCode::SERVICE_UNAVAILABLE, "app is not running").into_response(),
        ));
    }
    let ui = sup.app_ui(name).await;
    match ui.port {
        Some(p) if ui.enabled => Ok(p),
        _ => Err(Box::new(
            (
                StatusCode::NOT_FOUND,
                "app declares no embedded UI (headless)",
            )
                .into_response(),
        )),
    }
}

/// Transparent WebSocket tunnel for the app-view proxy (ADR 0057 §3, issue
/// #1054).
///
/// The gateway completes the WS handshake with the browser and opens a client
/// connection to the app's own UI port, then bridges frames bidirectionally as
/// a byte-opaque pipe (see [`app_view_ws_bridge`]). It parses no frames and
/// injects no credentials of its own — it forwards the browser's *own*
/// end-to-end auth headers (`Cookie`/`Authorization`) upstream, exactly as the
/// HTTP path does — and does not re-resolve the port mid-stream, so the app
/// self-authenticates end-to-end just like on the HTTP path.
///
/// Ordering matters: the upstream connection is opened *before* the browser
/// handshake completes, so (a) an unreachable app fails fast as a 502 rather
/// than a half-open browser socket, and (b) the subprotocol the app selects can
/// be echoed back to the browser in the same 101 response.
async fn app_view_ws_tunnel(
    ws: WebSocketUpgrade,
    name: String,
    rest: String,
    query: Option<String>,
    headers: HeaderMap,
) -> Response {
    let port = match app_view_resolve_port(&name).await {
        Ok(p) => p,
        Err(resp) => return *resp,
    };

    let target = match query.as_deref() {
        Some(q) if !q.is_empty() => format!("ws://127.0.0.1:{port}/{rest}?{q}"),
        _ => format!("ws://127.0.0.1:{port}/{rest}"),
    };

    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = match target.as_str().into_client_request() {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("upstream ws {target}: {e}"),
            )
                .into_response();
        }
    };
    // Forward the browser's requested subprotocol(s) upstream verbatim so the
    // app can negotiate against them.
    if let Some(proto) = headers.get(header::SEC_WEBSOCKET_PROTOCOL).cloned() {
        request
            .headers_mut()
            .insert(header::SEC_WEBSOCKET_PROTOCOL, proto);
    }
    // Preserve the browser's *own* end-to-end auth context on the upgrade, just
    // as the HTTP proxy path forwards `Authorization`. Same-origin browsers send
    // `Cookie` automatically on the WS handshake (they cannot set custom headers
    // on it), so a cookie-authenticated app that works over the HTTP proxy would
    // otherwise silently fail to authenticate its WebSocket. This forwards
    // credentials the browser already presented — it injects none — so the
    // "self-authenticates end-to-end, injects no credentials" posture holds.
    for name in [header::COOKIE, header::AUTHORIZATION] {
        if let Some(v) = headers.get(&name).cloned() {
            request.headers_mut().insert(name, v);
        }
    }

    let (upstream, response) = match tokio_tungstenite::connect_async(request).await {
        Ok(pair) => pair,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("upstream ws {target}: {e}"),
            )
                .into_response();
        }
    };

    // Echo the app-selected subprotocol back to the browser. `WebSocketUpgrade`
    // only writes it into the 101 when the browser actually offered it, which it
    // did (we forwarded that same list upstream), so negotiation is preserved.
    let ws = match response
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
    {
        Some(selected) => ws.protocols([selected.to_owned()]),
        None => ws,
    };

    ws.on_upgrade(move |browser| app_view_ws_bridge(browser, upstream))
}

/// The upstream half of an app-view WebSocket tunnel: a client connection to the
/// app's loopback UI port.
type AppViewUpstream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Bridges one browser WebSocket to one upstream app WebSocket for the life of
/// the connection, copying frames verbatim in both directions.
///
/// A single `select!` loop owns both sockets and awaits each `send` before
/// reading the next frame, so neither direction can buffer unboundedly — the
/// same streaming-backpressure discipline the HTTP proxy documents and the PTY
/// socket relies on. When either side closes (a Close frame, EOF, or a transport
/// error) the close is propagated to the peer and the loop tears both halves
/// down, so a dead app can never leave a half-open browser socket spinning.
async fn app_view_ws_bridge(browser: WebSocket, upstream: AppViewUpstream) {
    let (mut browser_tx, mut browser_rx) = browser.split();
    let (mut upstream_tx, mut upstream_rx) = upstream.split();

    loop {
        tokio::select! {
            from_browser = browser_rx.next() => {
                match from_browser {
                    Some(Ok(msg)) => {
                        let closing = matches!(msg, Message::Close(_));
                        if upstream_tx.send(axum_to_tungstenite(msg)).await.is_err() {
                            break;
                        }
                        if closing {
                            break;
                        }
                    }
                    // Browser closed or errored: signal the app and stop.
                    _ => {
                        let _ = upstream_tx
                            .send(tokio_tungstenite::tungstenite::Message::Close(None))
                            .await;
                        break;
                    }
                }
            }
            from_upstream = upstream_rx.next() => {
                match from_upstream {
                    Some(Ok(msg)) => {
                        let closing =
                            matches!(msg, tokio_tungstenite::tungstenite::Message::Close(_));
                        if let Some(msg) = tungstenite_to_axum(msg)
                            && browser_tx.send(msg).await.is_err()
                        {
                            break;
                        }
                        if closing {
                            break;
                        }
                    }
                    // App gone (EOF or transport error) mid-session: close the
                    // browser socket cleanly with a "going away" frame.
                    _ => {
                        let _ = browser_tx
                            .send(Message::Close(Some(CloseFrame {
                                code: 1001,
                                reason: Utf8Bytes::from_static("upstream app closed"),
                            })))
                            .await;
                        break;
                    }
                }
            }
        }
    }

    let _ = upstream_tx.close().await;
    let _ = browser_tx.close().await;
}

/// Convert a browser (axum) frame into its upstream (tungstenite) equivalent.
/// Byte-opaque: payloads move verbatim; no inspection or mutation.
fn axum_to_tungstenite(msg: Message) -> tokio_tungstenite::tungstenite::Message {
    use tokio_tungstenite::tungstenite::Message as Ts;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame as TsCloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    match msg {
        Message::Text(t) => Ts::Text(t.as_str().into()),
        Message::Binary(b) => Ts::Binary(b),
        Message::Ping(b) => Ts::Ping(b),
        Message::Pong(b) => Ts::Pong(b),
        Message::Close(Some(cf)) => Ts::Close(Some(TsCloseFrame {
            code: CloseCode::from(cf.code),
            reason: cf.reason.as_str().into(),
        })),
        Message::Close(None) => Ts::Close(None),
    }
}

/// Convert an upstream (tungstenite) frame into its browser (axum) equivalent.
/// Raw `Frame` frames are dropped (never surfaced by a read), as recommended by
/// the tungstenite maintainers.
fn tungstenite_to_axum(msg: tokio_tungstenite::tungstenite::Message) -> Option<Message> {
    use tokio_tungstenite::tungstenite::Message as Ts;
    match msg {
        Ts::Text(t) => Some(Message::Text(t.as_str().into())),
        Ts::Binary(b) => Some(Message::Binary(b)),
        Ts::Ping(b) => Some(Message::Ping(b)),
        Ts::Pong(b) => Some(Message::Pong(b)),
        Ts::Close(Some(cf)) => Some(Message::Close(Some(CloseFrame {
            code: u16::from(cf.code),
            reason: cf.reason.as_str().into(),
        }))),
        Ts::Close(None) => Some(Message::Close(None)),
        Ts::Frame(_) => None,
    }
}

// ---------------------------------------------------------------------------
// App-shipped left-rail icon (ADR 0057, issue #638)
//
// A project's `ui.icon` is either a *bundled glyph name* (resolved client-side
// against the console's icon set) or a *project asset path* (e.g.
// `assets/icon.svg`). This route serves the latter: a path-guarded, image-only
// GET so the rail can render the app's own icon via <img>.

/// Whether a `ui.icon` value denotes a project asset path (served here) rather
/// than a bundled glyph name (resolved client-side). Heuristic mirrored in the
/// console (`isAssetIcon`): a value that contains a path separator or ends in a
/// file extension is an asset; a bare token (`workers`, `docs`) is a glyph.
fn app_view_icon_is_asset(icon: &str) -> bool {
    icon.contains('/')
        || std::path::Path::new(icon)
            .extension()
            .is_some_and(|e| !e.is_empty())
}

/// Map an icon file extension to the image content-type we're willing to serve.
/// `None` ⇒ not an allow-listed image type (we refuse to serve it, so the rail
/// falls back to the default glyph rather than leaking arbitrary project files).
fn app_view_icon_content_type(path: &std::path::Path) -> Option<&'static str> {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("svg") => Some("image/svg+xml"),
        Some("png") => Some("image/png"),
        Some("webp") => Some("image/webp"),
        Some("gif") => Some("image/gif"),
        Some("jpg") | Some("jpeg") => Some("image/jpeg"),
        Some("ico") => Some("image/x-icon"),
        Some("avif") => Some("image/avif"),
        _ => None,
    }
}

/// `GET /console/app-view-icon/{name}` — serve a project's app-shipped rail
/// icon. 404 when the project declares no asset-path icon (bundled names render
/// client-side) or the file is missing/too large/not an allowed image type.
async fn app_view_icon(Path(name): Path<String>) -> Response {
    if !workspace::is_safe_name(&name) {
        return (StatusCode::BAD_REQUEST, "invalid project name").into_response();
    }
    let Some(icon) = projects::supervisor().app_ui(&name).await.icon else {
        return (StatusCode::NOT_FOUND, "no app-shipped icon").into_response();
    };
    // Bundled glyph names are resolved by the console, not served as files.
    if !app_view_icon_is_asset(&icon) {
        return (StatusCode::NOT_FOUND, "icon is a bundled glyph name").into_response();
    }
    let Some(path) = projects::safe_project_path(&name, &icon) else {
        return (StatusCode::BAD_REQUEST, "invalid icon path").into_response();
    };
    let Some(content_type) = app_view_icon_content_type(&path) else {
        return (StatusCode::NOT_FOUND, "icon is not an allowed image type").into_response();
    };
    // `safe_project_path` is purely lexical (rejects `..` etc.), but the file it
    // points at can still be a *symlink* escaping the project dir. Canonicalize
    // both and require containment so this route can never read outside the
    // project (it's unauthenticated and remotely reachable).
    let real = match std::fs::canonicalize(&path) {
        Ok(p) => p,
        Err(_) => return (StatusCode::NOT_FOUND, "no such icon file").into_response(),
    };
    match projects::project_dir(&name).and_then(|d| std::fs::canonicalize(d).ok()) {
        Some(root) if real.starts_with(&root) => {}
        _ => return (StatusCode::NOT_FOUND, "icon resolves outside the project").into_response(),
    }
    // Bound the read: cap memory at MAX+1 bytes even if the file lies about its
    // size, and reject anything over the icon limit.
    const MAX_ICON_BYTES: u64 = 512 * 1024;
    let bytes = {
        use std::io::Read;
        let file = match std::fs::File::open(&real) {
            Ok(f) => f,
            Err(_) => return (StatusCode::NOT_FOUND, "no such icon file").into_response(),
        };
        let mut buf = Vec::new();
        if file.take(MAX_ICON_BYTES + 1).read_to_end(&mut buf).is_err() {
            return (StatusCode::NOT_FOUND, "could not read icon").into_response();
        }
        buf
    };
    if bytes.len() as u64 > MAX_ICON_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "icon file too large").into_response();
    }

    let mut out = Response::new(axum::body::Body::from(bytes));
    let h = out.headers_mut();
    h.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    // Defence in depth for scripted SVGs: served for <img> (scripts don't run
    // there), but a direct navigation to this URL would otherwise execute an
    // embedded <script> in the console origin. `sandbox` (no allow-scripts)
    // neutralises that even on direct load; nosniff keeps the type honest.
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        "sandbox; default-src 'none'; style-src 'unsafe-inline'"
            .parse()
            .unwrap(),
    );
    h.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    h.insert(
        header::CACHE_CONTROL,
        "private, max-age=60".parse().unwrap(),
    );
    out
}

// ---------------------------------------------------------------------------
// Metrics dashboard API
// ---------------------------------------------------------------------------

/// A point-in-time metrics snapshot for the dashboard. Counters are monotonic;
/// the frontend derives throughput **rates** from the deltas of two successive
/// polls (so this endpoint stays a cheap, stateless reading). `activeInstances`
/// is read on demand from the read model only when this endpoint is polled — it
/// is deliberately NOT an always-on `COUNT` in the engine tick loop, so opening
/// the dashboard never perturbs a running performance demo.
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MetricsDto {
    /// Server clock at snapshot time (ms). The frontend uses successive
    /// timestamps as the exact dt for rate computation.
    timestamp_ms: u64,
    /// Active (non-terminal) process instances in this node's read model.
    active_instances: i64,

    // Throughput counters (monotonic, split by protocol).
    creates_rest: u64,
    creates_stream: u64,
    creates_total: u64,
    completions_rest: u64,
    completions_stream: u64,
    completions_total: u64,

    // Live gauges.
    connections_active: i64,
    commit_inflight: i64,

    // Durability counters.
    commits_total: u64,
    writes_total: u64,
    bytes_total: u64,
    credit_stalls_total: u64,

    // Derived means (ms / count) from histogram aggregates — convenient for the
    // cards; the frontend doesn't have to carry sum+count itself.
    fsync_mean_ms: f64,
    commit_wait_mean_ms: f64,
    commit_batch_mean: f64,
    frame_processing_mean_ms: f64,

    // Writer duty cycle: busy / (busy + idle) over all time. A value near 1.0
    // means the single journal writer is saturated.
    writer_busy_ratio: f64,

    /// Resident memory (jemalloc `stats.resident`, bytes) — the figure that
    /// tracks the process's real footprint. `null` on non-jemalloc targets.
    #[serde(default)]
    resident_bytes: Option<u64>,

    // Capacity-ceiling "clipping" LEDs + the signals behind them (ADR 0013).
    /// 1 while this node is pressed against the throughput ceiling (create
    /// concurrency / active-backlog limiter) — the amber/red clipping LED.
    ceiling_throughput: bool,
    /// 1 while pressed against the always-on memory-safety rails.
    ceiling_memory: bool,
    /// 1 while export lag has crossed the Tier-1 knee and the global guard is
    /// shedding a graded fraction of create intake — export backpressure is
    /// compressing throughput (distinct from the hard `ceiling_memory` backstop).
    ceiling_exporter: bool,
    /// 1 while producer create-submission is being flow-controlled at the
    /// Falcon/REST edge — the completion-paced credit servo is metering grants,
    /// or a hard admission block is withholding credit from the clients.
    ceiling_flow_control: bool,
    /// Least-full export shard fill in per-mille of budget (0 = empty … 1000 = at
    /// budget) — the live signal behind the `ceiling_exporter` LED.
    exporter_fill_permille: i64,
    /// This node's active SLA mode (`latency` | `admission`). Per node, since it
    /// is configurable at startup (`NANOBPMN_SLA_MODE`) and switchable at runtime,
    /// and governs how the capacity ceilings behave.
    sla_mode: String,
    /// Live submitted-but-not-yet-applied create-queue depth (the OOM signal).
    pending_create_queue: i64,
    /// Live active-instance backlog (created − completed).
    active_backlog: i64,
    /// Configured shed thresholds (0 = rail disabled) so the UI can show headroom.
    admission_backlog_limit: i64,
    admission_create_queue_limit: i64,
    /// Cumulative admissions shed since boot (summed across all rails).
    admission_shed_total: u64,

    /// This node's Raft recovery/leadership state — surfaces whether the node is
    /// catching up after a restart (owns partitions a peer is still leading) or is
    /// acting as a failover incumbent handing leadership back. `null`-equivalent
    /// (all-zero, `recovering=false`) in steady state and on single-node/off-Raft.
    recovery: RecoveryDto,
}

/// Per-node Raft recovery summary for the console cluster view. Lets the UI show
/// "up but catching up" instead of a bare "up" while a restarted node reclaims
/// leadership of its owned partitions (and the incumbent hands it back).
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct RecoveryDto {
    /// This node owns one or more partitions it does not yet lead — it is still
    /// catching up after a restart before reclaiming leadership. While `true` the
    /// cluster is not fully rebalanced back onto this node.
    recovering: bool,
    /// Partitions this node statically owns (its steady-state leadership set).
    owned: u32,
    /// Owned partitions this node currently leads again (reclaimed / steady).
    reclaimed: u32,
    /// Owned partitions currently led by a peer failover incumbent — the ones
    /// this node is still catching up on.
    catching_up: u32,
    /// Partitions this node leads on behalf of a peer owner (this node is the
    /// failover incumbent, handing leadership back as the owner catches up).
    handing_off: u32,
    /// Largest replication lag (in log entries) of a returning owner this node is
    /// handing a partition back to, when known (incumbent side only).
    handoff_lag_entries: Option<u64>,
    /// Short human-readable summary, e.g. "reclaiming 2/4 partitions" or
    /// "handing back 3 (lag 12k)". Empty in steady state.
    detail: String,
}

/// Assembles a [`RecoveryDto`] from raw partition counts, deriving `recovering`
/// and the human-readable `detail`. Shared by [`build_recovery`] (live engine
/// state) and [`metrics_dto_from_prometheus`] (a peer's scraped gauges) so both
/// render identical recovery summaries.
fn recovery_dto_from_counts(
    owned: u32,
    reclaimed: u32,
    catching_up: u32,
    handing_off: u32,
    handoff_lag: Option<u64>,
) -> RecoveryDto {
    let recovering = catching_up > 0;
    let detail = if recovering {
        format!("reclaiming {catching_up}/{owned} partitions")
    } else if handing_off > 0 {
        match handoff_lag {
            Some(lag) => format!("handing back {handing_off} (lag {lag})"),
            None => format!("handing back {handing_off}"),
        }
    } else {
        String::new()
    };
    RecoveryDto {
        recovering,
        owned,
        reclaimed,
        catching_up,
        handing_off,
        handoff_lag_entries: handoff_lag,
        detail,
    }
}

/// Computes this node's [`RecoveryDto`] from the live Raft metrics of the groups
/// it hosts, via the base-build [`crate::recovery_counts`] (shared with the
/// Prometheus `/metrics` exporter so a local and a scraped node agree).
fn build_recovery(server: &dyn ConsoleServer) -> RecoveryDto {
    let c = server.recovery_counts();
    recovery_dto_from_counts(
        c.owned,
        c.reclaimed,
        c.catching_up,
        c.handing_off,
        c.handoff_lag_entries,
    )
}

/// Builds this node's metrics snapshot DTO. Shared by `GET /console/api/metrics`
/// (the local dashboard) and the self entry of the cluster-wide aggregation, so
/// both report identical numbers.
pub fn build_local_metrics(server: &dyn ConsoleServer) -> MetricsDto {
    let s = nano_server_storage::metrics::snapshot();

    let mean_ms = |sum: f64, count: u64| {
        if count == 0 {
            0.0
        } else {
            sum / count as f64 * 1000.0
        }
    };
    let mean = |sum: f64, count: u64| if count == 0 { 0.0 } else { sum / count as f64 };
    let busy_ratio = {
        let total = s.writer_busy_seconds + s.writer_idle_seconds;
        if total == 0.0 {
            0.0
        } else {
            s.writer_busy_seconds / total
        }
    };

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    MetricsDto {
        timestamp_ms,
        active_instances: server.store().active_instance_count() as i64,

        creates_rest: s.creates_rest,
        creates_stream: s.creates_stream,
        creates_total: s.creates_rest + s.creates_stream,
        completions_rest: s.completions_rest,
        completions_stream: s.completions_stream,
        completions_total: s.completions_rest + s.completions_stream,

        connections_active: s.stream_connections_active,
        commit_inflight: s.commit_inflight,

        commits_total: s.commits_total,
        writes_total: s.writes_total,
        bytes_total: s.bytes_total,
        credit_stalls_total: s.stream_credit_stalls_total,

        fsync_mean_ms: mean_ms(s.fsync_seconds_sum, s.fsync_count),
        commit_wait_mean_ms: mean_ms(s.commit_wait_seconds_sum, s.commit_wait_count),
        commit_batch_mean: mean(s.commit_batch_size_sum, s.commit_batch_count),
        frame_processing_mean_ms: mean_ms(s.frame_processing_seconds_sum, s.frame_processing_count),

        writer_busy_ratio: busy_ratio,

        resident_bytes: nano_server_storage::memory::resident_bytes().map(|b| b as u64),

        ceiling_throughput: s.ceiling_throughput_active,
        ceiling_memory: s.ceiling_memory_active,
        ceiling_exporter: s.ceiling_exporter_active,
        ceiling_flow_control: s.ceiling_flow_control_active,
        exporter_fill_permille: s.exporter_fill_permille,
        sla_mode: server.sla_mode().as_str().to_string(),
        pending_create_queue: s.pending_create_queue,
        active_backlog: s.active_backlog,
        admission_backlog_limit: s.admission_backlog_limit,
        admission_create_queue_limit: s.admission_create_queue_limit,
        admission_shed_total: s.admission_shed_total,

        recovery: build_recovery(server),
    }
}

// The metrics snapshot DTO is built by `build_local_metrics`; the typed
// `GET /console/api/metrics` operation is served by the generated router.

// ---------------------------------------------------------------------------
// Cluster-wide metrics (per-node aggregation)
// ---------------------------------------------------------------------------

/// Per-node metrics plus a cluster aggregate, for the dashboard's cluster view.
/// Each peer's `GET /console/api/metrics` is probed concurrently; unreachable
/// peers are reported with `reachable=false` and contribute nothing to the
/// aggregate. Self is read locally (no round-trip).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMetricsDto {
    checked_at_ms: u64,
    nodes: Vec<NodeMetricsDto>,
    aggregate: AggregateMetricsDto,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeMetricsDto {
    node_id: u32,
    address: String,
    is_self: bool,
    reachable: bool,
    error: Option<String>,
    metrics: Option<MetricsDto>,
}

/// Sums of the headline counters/gauges over all reachable nodes. Cluster-wide
/// throughput is derived client-side from successive deltas of `*_total`.
#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct AggregateMetricsDto {
    reachable_nodes: u32,
    total_nodes: u32,
    active_instances: i64,
    creates_total: u64,
    completions_total: u64,
    connections_active: i64,
    commit_inflight: i64,
    resident_bytes: u64,
}

/// `GET /console/api/cluster/metrics` — probes every node's metrics and returns
/// the per-node breakdown plus a reachable-node aggregate.
pub async fn cluster_metrics(server: &dyn ConsoleServer) -> ClusterMetricsDto {
    let topology = server.cluster_topology();
    let self_id = topology.node_id;
    let num_nodes = topology.num_nodes();

    // `build_local_metrics` only reads this node's state and is synchronous, so
    // compute it once up front. Peer probes are the only async work, and they
    // need just the address — this keeps the per-node futures `'static` without
    // cloning a (now type-erased) `&dyn ConsoleServer` into them.
    let self_metrics = build_local_metrics(server);

    let probes = (0..num_nodes).map(|node| {
        let is_self = node == self_id;
        let address = topology.peer_addr(node).unwrap_or("").to_string();
        let self_metrics = is_self.then(|| self_metrics.clone());
        async move {
            if is_self {
                return NodeMetricsDto {
                    node_id: node,
                    address,
                    is_self: true,
                    reachable: true,
                    error: None,
                    metrics: self_metrics,
                };
            }
            match probe_peer_metrics(&address).await {
                Ok(metrics) => NodeMetricsDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: true,
                    error: None,
                    metrics: Some(metrics),
                },
                Err(err) => NodeMetricsDto {
                    node_id: node,
                    address,
                    is_self: false,
                    reachable: false,
                    error: Some(err),
                    metrics: None,
                },
            }
        }
    });

    let nodes = futures_util::future::join_all(probes).await;

    let mut aggregate = AggregateMetricsDto {
        total_nodes: num_nodes,
        ..Default::default()
    };
    for n in &nodes {
        if let Some(m) = &n.metrics {
            aggregate.reachable_nodes += 1;
            aggregate.active_instances += m.active_instances;
            aggregate.creates_total += m.creates_total;
            aggregate.completions_total += m.completions_total;
            aggregate.connections_active += m.connections_active;
            aggregate.commit_inflight += m.commit_inflight;
            aggregate.resident_bytes += m.resident_bytes.unwrap_or(0);
        }
    }

    let checked_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    Json(ClusterMetricsDto {
        checked_at_ms,
        nodes,
        aggregate,
    })
    .0
}

/// Fetches a peer's metrics for the cluster dashboard. Prefers the rich
/// `GET {base_url}/console/api/metrics` JSON (present only when the peer is built
/// with the `console` feature); if that peer has no console (404), transparently
/// falls back to scraping the peer's always-on `GET {base_url}/metrics`
/// Prometheus exposition and reconstructing a [`MetricsDto`] from it. This lets a
/// single console node report metrics for console-less peers in the cluster.
async fn probe_peer_metrics(base_url: &str) -> Result<MetricsDto, String> {
    if base_url.is_empty() {
        return Err("no address configured".to_string());
    }
    match probe_peer_console_metrics(base_url).await? {
        Some(metrics) => Ok(metrics),
        // Peer has no console feature — reconstruct from its Prometheus endpoint.
        None => probe_peer_prometheus_metrics(base_url).await,
    }
}

/// Issues a `GET {base_url}{path}` and returns `(status, body)`, sharing one
/// plain-HTTP client + the health-probe timeout for both metrics probes.
async fn peer_http_get(
    base_url: &str,
    path: &str,
) -> Result<(hyper::StatusCode, hyper::body::Bytes), String> {
    use http_body_util::BodyExt;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let uri: hyper::Uri = format!("{}{}", base_url.trim_end_matches('/'), path)
        .parse()
        .map_err(|e| format!("bad peer url: {e}"))?;

    let client: Client<_, http_body_util::Empty<hyper::body::Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();

    let fut = async {
        let resp = client.get(uri).await.map_err(|e| format!("connect: {e}"))?;
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("read: {e}"))?
            .to_bytes();
        Ok::<_, String>((status, body))
    };

    tokio::time::timeout(HEALTH_PROBE_TIMEOUT, fut)
        .await
        .map_err(|_| "timeout".to_string())?
}

/// Probes `GET {base_url}/console/api/metrics`. `Ok(Some(_))` on success,
/// `Ok(None)` when the peer has no console (404 — caller falls back to
/// Prometheus), `Err` on any transport/parse failure.
async fn probe_peer_console_metrics(base_url: &str) -> Result<Option<MetricsDto>, String> {
    let (status, body) = peer_http_get(base_url, "/console/api/metrics").await?;
    if status.as_u16() == 404 {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }
    serde_json::from_slice::<MetricsDto>(&body)
        .map(Some)
        .map_err(|e| format!("parse: {e}"))
}

/// Scrapes a console-less peer's always-on `GET {base_url}/metrics` Prometheus
/// exposition and reconstructs a [`MetricsDto`]. Every dashboard field maps to a
/// permanent series. Since ADR 0035 the two formerly-approximated fields —
/// `active_instances` (the true active COUNT) and `recovery` (per-partition
/// leadership) — are exported as scrape-computed gauges, so a console-less peer
/// now reports full fidelity; the old approximations remain only as a fallback
/// for a pre-0035 peer (see [`metrics_dto_from_prometheus`]).
async fn probe_peer_prometheus_metrics(base_url: &str) -> Result<MetricsDto, String> {
    let (status, body) = peer_http_get(base_url, "/metrics").await?;
    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }
    let text = std::str::from_utf8(&body).map_err(|e| format!("utf8: {e}"))?;
    Ok(metrics_dto_from_prometheus(text))
}

/// A parsed Prometheus text-exposition scrape: one `(name{labels}, value)` per
/// sample line (comments/blank lines skipped). nanobpm label values never
/// contain spaces, so splitting each line on its first space is unambiguous.
struct PromScrape(Vec<(String, f64)>);

impl PromScrape {
    fn parse(text: &str) -> Self {
        let mut samples = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, rest)) = line.split_once(' ') else {
                continue;
            };
            let Some(tok) = rest.split_whitespace().next() else {
                continue;
            };
            let value = match tok {
                "+Inf" => f64::INFINITY,
                "-Inf" => f64::NEG_INFINITY,
                "NaN" => f64::NAN,
                other => match other.parse::<f64>() {
                    Ok(v) => v,
                    Err(_) => continue,
                },
            };
            samples.push((key.to_string(), value));
        }
        PromScrape(samples)
    }

    /// Value of a bare (label-free) series, e.g. `nanobpm_commit_inflight`.
    fn gauge(&self, name: &str) -> f64 {
        self.0
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| *v)
            .unwrap_or(0.0)
    }

    /// Value of a bare (label-free) series if present, else `None` — lets a
    /// caller distinguish a legitimately-zero gauge from an absent one (e.g. a
    /// pre-ADR-0035 peer that doesn't export it, so the caller can fall back).
    fn get(&self, name: &str) -> Option<f64> {
        self.0.iter().find(|(k, _)| k == name).map(|(_, v)| *v)
    }

    /// Value of the first series named `name` whose label set contains `frag`
    /// (e.g. `protocol="rest"`).
    fn labeled(&self, name: &str, frag: &str) -> f64 {
        let prefix = format!("{name}{{");
        self.0
            .iter()
            .find(|(k, _)| k.starts_with(&prefix) && k.contains(frag))
            .map(|(_, v)| *v)
            .unwrap_or(0.0)
    }

    /// Sum over every series named `name` regardless of labels (bare or any
    /// label set), e.g. summing `nanobpm_admission_shed_total{reason=...}`.
    fn sum(&self, name: &str) -> f64 {
        let braced = format!("{name}{{");
        self.0
            .iter()
            .filter(|(k, _)| k == name || k.starts_with(&braced))
            .map(|(_, v)| *v)
            .sum()
    }
}

/// Reconstructs a [`MetricsDto`] from a peer's Prometheus scrape. Mirrors
/// [`build_local_metrics`] field-for-field so a console-less peer reports the
/// same shape — and, since ADR 0035, the same fidelity — as a console peer,
/// falling back to the old proxies only for a pre-0035 peer.
fn metrics_dto_from_prometheus(text: &str) -> MetricsDto {
    let s = PromScrape::parse(text);

    let creates_rest = s.labeled("nanobpm_creates_total", "protocol=\"rest\"") as u64;
    let creates_stream = s.labeled("nanobpm_creates_total", "protocol=\"stream\"") as u64;
    let completions_rest = s.labeled("nanobpm_job_completions_total", "protocol=\"rest\"") as u64;
    let completions_stream =
        s.labeled("nanobpm_job_completions_total", "protocol=\"stream\"") as u64;

    let mean_ms = |sum: f64, count: f64| {
        if count == 0.0 {
            0.0
        } else {
            sum / count * 1000.0
        }
    };
    let mean = |sum: f64, count: f64| if count == 0.0 { 0.0 } else { sum / count };

    let busy = s.gauge("nanobpm_journal_writer_busy_seconds");
    let idle = s.gauge("nanobpm_journal_writer_idle_seconds");
    let writer_busy_ratio = if busy + idle == 0.0 {
        0.0
    } else {
        busy / (busy + idle)
    };

    let resident = s.labeled("nanobpm_jemalloc_bytes", "kind=\"resident\"");

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    MetricsDto {
        timestamp_ms,
        // Prefer the scrape-computed true COUNT (ADR 0035); fall back to the
        // active_backlog (created−completed) proxy for a pre-0035 peer that
        // doesn't export it.
        active_instances: s
            .get("nanobpm_active_instances")
            .map(|v| v as i64)
            .unwrap_or_else(|| s.gauge("nanobpm_active_backlog") as i64),

        creates_rest,
        creates_stream,
        creates_total: creates_rest + creates_stream,
        completions_rest,
        completions_stream,
        completions_total: completions_rest + completions_stream,

        connections_active: s.gauge("nanobpm_stream_connections_active") as i64,
        commit_inflight: s.gauge("nanobpm_commit_inflight") as i64,

        commits_total: s.gauge("nanobpm_journal_commits_total") as u64,
        writes_total: s.gauge("nanobpm_journal_writes_total") as u64,
        bytes_total: s.gauge("nanobpm_journal_bytes_total") as u64,
        credit_stalls_total: s.gauge("nanobpm_stream_credit_stalls_total") as u64,

        fsync_mean_ms: mean_ms(
            s.gauge("nanobpm_journal_fsync_seconds_sum"),
            s.gauge("nanobpm_journal_fsync_seconds_count"),
        ),
        commit_wait_mean_ms: mean_ms(
            s.gauge("nanobpm_commit_wait_seconds_sum"),
            s.gauge("nanobpm_commit_wait_seconds_count"),
        ),
        commit_batch_mean: mean(
            s.gauge("nanobpm_journal_commit_batch_size_sum"),
            s.gauge("nanobpm_journal_commit_batch_size_count"),
        ),
        frame_processing_mean_ms: mean_ms(
            s.gauge("nanobpm_stream_frame_processing_seconds_sum"),
            s.gauge("nanobpm_stream_frame_processing_seconds_count"),
        ),

        writer_busy_ratio,

        resident_bytes: (resident > 0.0).then_some(resident as u64),

        ceiling_throughput: s.labeled("nanobpm_ceiling_active", "ceiling=\"throughput\"") != 0.0,
        ceiling_memory: s.labeled("nanobpm_ceiling_active", "ceiling=\"memory\"") != 0.0,
        ceiling_exporter: s.labeled("nanobpm_ceiling_active", "ceiling=\"exporter\"") != 0.0,
        ceiling_flow_control: s.labeled("nanobpm_ceiling_active", "ceiling=\"flow_control\"")
            != 0.0,
        exporter_fill_permille: s.gauge("nanobpm_exporter_fill_permille") as i64,
        sla_mode: if s.labeled("nanobpm_sla_mode", "mode=\"admission\"") != 0.0 {
            "admission"
        } else {
            "latency"
        }
        .to_string(),
        pending_create_queue: s.gauge("nanobpm_pending_create_queue") as i64,
        active_backlog: s.gauge("nanobpm_active_backlog") as i64,
        admission_backlog_limit: s.labeled("nanobpm_admission_limit", "limit=\"backlog\"") as i64,
        admission_create_queue_limit: s.labeled("nanobpm_admission_limit", "limit=\"create_queue\"")
            as i64,
        admission_shed_total: s.sum("nanobpm_admission_shed_total") as u64,

        // Per-partition leadership is now exported on scrape (ADR 0035); read it
        // back when present, else default (pre-0035 peer).
        recovery: recovery_from_prometheus(&s),
    }
}

/// Reconstructs a [`RecoveryDto`] from a peer's scraped recovery gauges (ADR
/// 0035). Returns the steady-state default when none are present (a pre-0035
/// peer that doesn't export them).
fn recovery_from_prometheus(s: &PromScrape) -> RecoveryDto {
    match s.get("nanobpm_partition_owned") {
        None => RecoveryDto::default(),
        Some(owned) => recovery_dto_from_counts(
            owned as u32,
            s.gauge("nanobpm_partition_reclaimed") as u32,
            s.gauge("nanobpm_partition_catching_up") as u32,
            s.gauge("nanobpm_partition_handing_off") as u32,
            s.get("nanobpm_handoff_lag_entries").map(|v| v as u64),
        ),
    }
}

// ---------------------------------------------------------------------------
// Process Instance Explorer API
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct InstanceDto {
    /// u64 engine key rendered as a string — keys exceed JS's safe integer range.
    key: String,
    process_id: String,
    process_definition_key: String,
    version: i32,
    /// `Active` | `Suspended` | `Completed` | `Terminated`.
    state: String,
    start_date_ms: u64,
    has_incident: bool,
    business_id: Option<String>,
    tags: Vec<String>,
    /// ISO-8601 datetime of the most recent suspension while the instance is
    /// `Suspended`, else `None` (serialized `null`). ALWAYS present — mirrors the
    /// gateway v2 `ProcessInstanceResult.suspendedDate`. Derived from the read
    /// model's `suspended_date_ms` column, the same single source of truth that
    /// derives the `Suspended` state, so the two cannot drift.
    #[serde(rename = "suspendedDate")]
    suspended_date: Option<String>,
    /// C8 parent linkage for a call-activity **child** process instance: the key
    /// of the calling (parent) process instance. `None` for a top-level
    /// instance. Mirrors C8's `parentProcessInstanceKey`.
    parent_process_instance_key: Option<String>,
    /// C8 parent linkage for a call-activity **child** process instance: the
    /// element-instance key of the call-activity that spawned this instance in
    /// the parent. `None` for a top-level instance. Mirrors C8's
    /// `parentElementInstanceKey`.
    parent_element_instance_key: Option<String>,
}

impl From<&nano_server_storage::readstore::ProcessInstanceRow> for InstanceDto {
    fn from(r: &nano_server_storage::readstore::ProcessInstanceRow) -> Self {
        InstanceDto {
            key: r.key.to_string(),
            process_id: r.process_id.clone(),
            process_definition_key: r.process_definition_key.clone(),
            version: r.version,
            state: format!("{:?}", r.state),
            start_date_ms: r.start_date_ms,
            has_incident: r.has_incident,
            business_id: r.business_id.clone(),
            tags: r.tags.clone(),
            suspended_date: r.suspended_date_ms.map(|ms| {
                // A present column ALWAYS projects to a present datetime string:
                // map (not and_then) so an out-of-range millis value falls back
                // to epoch rather than collapsing to `null`, which would let the
                // wire `suspendedDate` drift from the `Suspended` state derived
                // from the same column. Mirrors the gateway v2 projection.
                //
                // `suspended_date_ms` is a `u64`, so use a checked `i64::try_from`
                // instead of an `as` cast: a value `> i64::MAX` would otherwise
                // wrap to a negative `i64` and yield an arbitrary *pre-epoch*
                // datetime rather than the intended epoch fallback.
                i64::try_from(ms)
                    .ok()
                    .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
                    .unwrap_or_else(|| {
                        chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0)
                            .expect("epoch is valid")
                    })
                    .to_rfc3339()
            }),
            parent_process_instance_key: r.parent_process_instance_key.map(|k| k.to_string()),
            parent_element_instance_key: r.parent_element_instance_key.map(|k| k.to_string()),
        }
    }
}

#[cfg(test)]
mod instance_dto_parent_linkage_tests {
    use nano_server_storage::readstore::ProcessInstanceRow;
    use nanobpmn_engine_core::{Key, ProcessInstanceState};

    use super::*;

    fn row(parent_pi: Option<Key>, parent_ei: Option<Key>) -> ProcessInstanceRow {
        ProcessInstanceRow {
            key: 100,
            process_id: "child".to_string(),
            process_definition_id: "child".to_string(),
            process_definition_key: "7".to_string(),
            version: 1,
            state: ProcessInstanceState::Active,
            start_date_ms: 1_700_000_000_000,
            has_incident: false,
            tags: vec![],
            business_id: None,
            parent_process_instance_key: parent_pi,
            parent_element_instance_key: parent_ei,
            suspended_date_ms: None,
        }
    }

    #[test]
    fn call_activity_child_passes_parent_linkage_through_as_strings() {
        let dto = InstanceDto::from(&row(Some(42), Some(4242)));
        assert_eq!(dto.parent_process_instance_key.as_deref(), Some("42"));
        assert_eq!(dto.parent_element_instance_key.as_deref(), Some("4242"));

        // Serialized wire shape carries the parent linkage as string|null fields.
        let json = serde_json::to_value(&dto).unwrap();
        assert_eq!(json["parent_process_instance_key"], "42");
        assert_eq!(json["parent_element_instance_key"], "4242");
    }

    #[test]
    fn top_level_instance_has_null_parent_linkage() {
        let dto = InstanceDto::from(&row(None, None));
        assert!(dto.parent_process_instance_key.is_none());
        assert!(dto.parent_element_instance_key.is_none());

        let json = serde_json::to_value(&dto).unwrap();
        assert!(json["parent_process_instance_key"].is_null());
        assert!(json["parent_element_instance_key"].is_null());
    }

    #[test]
    fn suspended_date_projects_to_an_always_present_nullable_iso_string() {
        // Not suspended: the key is present and null (mirrors the gateway v2
        // `ProcessInstanceResult.suspendedDate`, which is always present).
        let json = serde_json::to_value(InstanceDto::from(&row(None, None))).unwrap();
        assert!(
            json.as_object().unwrap().contains_key("suspendedDate"),
            "suspendedDate must always be present on the wire"
        );
        assert!(json["suspendedDate"].is_null());

        // Suspended: the read-model `suspended_date_ms` column renders as an
        // ISO-8601 datetime string, the same single source of truth that derives
        // the `Suspended` state so the two cannot drift.
        let mut r = row(None, None);
        r.state = ProcessInstanceState::Suspended;
        r.suspended_date_ms = Some(1_700_000_000_000);
        let dto = InstanceDto::from(&r);
        assert_eq!(
            dto.suspended_date.as_deref(),
            Some("2023-11-14T22:13:20+00:00")
        );
        assert_eq!(dto.state, "Suspended");

        // Out-of-range `u64` millis (> i64::MAX) must fall back to the epoch,
        // NOT wrap through an `as` cast into an arbitrary pre-epoch datetime:
        // the column is present, so the wire value stays present (never null)
        // and never drifts negative.
        let mut r = row(None, None);
        r.state = ProcessInstanceState::Suspended;
        r.suspended_date_ms = Some(u64::MAX);
        let dto = InstanceDto::from(&r);
        assert_eq!(
            dto.suspended_date.as_deref(),
            Some("1970-01-01T00:00:00+00:00"),
            "an out-of-range millis value falls back to epoch, not a wrapped pre-epoch time"
        );
    }
}

/// One page of process instances plus the total row count, so the console can
/// render a pager without a second request.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstancePage {
    items: Vec<InstanceDto>,
    total: i64,
    page: i64,
    page_size: i64,
}

#[derive(Serialize)]
struct VariableDto {
    name: String,
    /// Serialized-JSON value string, mirroring Camunda's wire representation.
    value: String,
    scope_key: String,
}

#[derive(Serialize)]
struct JobDto {
    key: String,
    element_id: String,
    job_type: String,
    state: String,
    retries: i32,
    worker: Option<String>,
    deadline_ms: Option<u64>,
    /// Logical instant (ms since epoch) the current activation lock was acquired.
    /// Only populated for a job the engine reports as `Activated` (studio overlay).
    activated_at_ms: Option<u64>,
    /// The current lock window (`deadline - activated_at`) the job is held for.
    /// Derived from the *live* deadline, so it moves when an `UpdateJobTimeout`
    /// extends (or shortens) the lock and the Explorer's "Timeout" reflects the
    /// modification. `None` unless currently activated.
    timeout_ms: Option<u64>,
}

#[derive(Serialize)]
struct IncidentDto {
    key: String,
    element_id: String,
    kind: String,
    state: String,
    reason: String,
    created_at_ms: u64,
}

#[derive(Serialize)]
pub struct InstanceDetailDto {
    instance: InstanceDto,
    variables: Vec<VariableDto>,
    jobs: Vec<JobDto>,
    incidents: Vec<IncidentDto>,
    active_elements: Vec<ActiveElementDto>,
    /// The child process instances this instance spawned via call activities
    /// (parent -> child navigation, #1115). Empty when it has no call activities
    /// (or none have spawned a child yet).
    called_instances: Vec<CalledInstanceDto>,
}

/// One child process instance spawned by a call activity in this (parent)
/// instance — the parent -> child navigation surface (#1115). Mirrors Operate's
/// Details-tab "Called Process Instance" row; a multi-instance call activity
/// yields N entries that all share one `calling_element_id`.
#[derive(Serialize)]
struct CalledInstanceDto {
    /// u64 child instance key rendered as a string.
    key: String,
    process_id: String,
    version: i32,
    /// `Active` | `Completed` | `Terminated`.
    state: String,
    has_incident: bool,
    start_date_ms: u64,
    /// BPMN id of the call-activity cell in the parent that spawned the child,
    /// resolved from the child's `parent_element_instance_key` via the parent's
    /// element-instance rows. Resolution does not depend on the cell still being
    /// active (a COMPLETED call activity resolves too). `None` when the calling
    /// element instance is no longer resolvable (e.g. evicted).
    calling_element_id: Option<String>,
    /// The call-activity cell's BPMN name, when the model carries one.
    calling_element_name: Option<String>,
}

/// Builds the `called_instances` list for a parent instance: for each child row
/// (already filtered to `parent_process_instance_key == parent`), resolves the
/// child's `parent_element_instance_key` back to the calling call-activity's
/// BPMN element id (and name) via `resolve_calling_element`.
///
/// Pure over the resolver so the single / multi / none cases are unit-testable
/// without a store. The resolver is expected to read a single element instance
/// by key **without** a state filter (see [`readstore::ReadModel::element_instance`]),
/// so a COMPLETED call-activity cell still resolves — satisfying the contract
/// that resolution must not depend on the cell being currently active. A
/// multi-instance call activity's children each carry their own inner
/// `parent_element_instance_key`, but all resolve to the same BPMN
/// `element_id`, so the N entries share one `calling_element_id`.
fn build_called_instances(
    children: &[nano_server_storage::readstore::ProcessInstanceRow],
    mut resolve_calling_element: impl FnMut(
        nanobpmn_engine_core::Key,
    ) -> Option<
        nano_server_storage::readstore::ElementInstanceRow,
    >,
) -> Vec<CalledInstanceDto> {
    children
        .iter()
        .map(|child| {
            let calling = child
                .parent_element_instance_key
                .and_then(&mut resolve_calling_element);
            CalledInstanceDto {
                key: child.key.to_string(),
                process_id: child.process_id.clone(),
                version: child.version,
                state: format!("{:?}", child.state),
                has_incident: child.has_incident,
                start_date_ms: child.start_date_ms,
                calling_element_id: calling.as_ref().map(|e| e.element_id.clone()),
                calling_element_name: calling.and_then(|e| e.element_name),
            }
        })
        .collect()
}

/// An element instance currently in the `Active` state — a live token position.
/// Unlike jobs (service tasks only), this covers wait states with no job
/// (intermediate catch events, receive tasks, timers, event-based gateways) and
/// active (sub)process bodies, so the Explorer overlay can highlight a waiting
/// instance's location.
#[derive(Serialize)]
pub struct ActiveElementDto {
    element_id: String,
    element_type: String,
    element_name: Option<String>,
}

/// Parses a console `state` filter string into an engine
/// [`ProcessInstanceState`]. Accepts exactly the spec's enum values
/// (`Active` / `Suspended` / `Completed` / `Terminated`) — the same names the
/// console projects for a row (see [`InstanceDto`]) — so "filter by what you
/// see" holds. An unrecognized value yields `None`, i.e. no state constraint
/// (unfiltered).
pub fn parse_instance_state_filter(
    state: &str,
) -> Option<nanobpmn_engine_core::ProcessInstanceState> {
    use nanobpmn_engine_core::ProcessInstanceState;
    match state {
        "Active" => Some(ProcessInstanceState::Active),
        "Suspended" => Some(ProcessInstanceState::Suspended),
        "Completed" => Some(ProcessInstanceState::Completed),
        "Terminated" => Some(ProcessInstanceState::Terminated),
        _ => None,
    }
}

/// `GET /console/api/instances?page=N&pageSize=M&state=…&hasIncident=…` — one
/// page of process instances, newest first, plus the total count for the pager.
/// Pagination and filtering are pushed into SQLite
/// (`process_instances_page` / `process_instance_count`) so a node with a large
/// read model returns a bounded, correctly-counted page instead of
/// materializing and sorting every row (which made the Process Explorer hang).
/// The `filter` (state + has-incident) is applied server-side so the pager
/// total and page boundaries stay correct.
pub fn instances(
    server: &dyn ConsoleServer,
    page: i64,
    page_size: i64,
    filter: nano_server_storage::readstore::InstanceFilter,
) -> InstancePage {
    let page = page.max(0);
    let page_size = page_size.clamp(1, 500);
    let total = server.store().process_instance_count(&filter);
    let rows =
        server
            .store()
            .process_instances_page(page_size, page.saturating_mul(page_size), &filter);
    InstancePage {
        items: rows.iter().map(InstanceDto::from).collect(),
        total,
        page,
        page_size,
    }
}

/// The live activation view of a single job, read from the authoritative engine
/// state (not the read model). Used to overlay the studio-only `Activated`
/// status — see [`apply_job_activation_overlay`] and [`instance_detail`].
#[derive(Clone, Debug)]
pub struct LiveJob {
    pub state: String,
    pub worker: Option<String>,
    pub deadline_ms: Option<u64>,
    pub activated_at_ms: Option<u64>,
}

/// Overlays live engine job state onto read-model job DTOs (studio-only, nano
/// enhancement — issue #608).
///
/// Job activation is a *volatile* lease that nano deliberately does not journal
/// or export under leader-local activation (see `console::trace`), so the read
/// model shows a leased job as `Created` until it completes. The engine, however,
/// holds the authoritative live state. This upgrades a read-model job still shown
/// as `Created` to the engine's live `Activated` view (state + worker + deadline)
/// when the engine reports it activated.
///
/// Under fully-replicated activation (`NANOBPMN_REPLICATE_ACTIVATION` resolving to
/// `Always`) the `JobActivated` event *is* exported, so the read-model row is
/// already `Activated` — but the projection stores only state/worker/deadline, not
/// `activated_at`, so `activated_at_ms`/`timeout_ms` stay null. An already-
/// `Activated` row is therefore enriched here too (its live `activated_at` filled
/// in and `timeout_ms` recomputed from the live deadline), so the "Timeout" field
/// reflects `UpdateJobTimeout` moves regardless of activation-replication mode.
///
/// Every other case is left exactly as the read model has it: a terminal/failed
/// row is never regressed, and a job the engine no longer holds (evicted on
/// completion) keeps its read-model state. Pure, so the merge is unit-testable
/// without an engine.
fn apply_job_activation_overlay(
    jobs: &mut [JobDto],
    live: &std::collections::HashMap<u64, LiveJob>,
) {
    for dto in jobs.iter_mut() {
        // Only enrich non-terminal, activatable rows: a job still shown as
        // `Created` (leader-local activation) or already `Activated` (replicated
        // activation). Terminal/failed states are never regressed.
        if dto.state != "Created" && dto.state != "Activated" {
            continue;
        }
        let Ok(key) = dto.key.parse::<u64>() else {
            continue;
        };
        if let Some(l) = live.get(&key)
            && l.state == "Activated"
        {
            dto.state = l.state.clone();
            dto.worker = l.worker.clone();
            dto.deadline_ms = l.deadline_ms;
            dto.activated_at_ms = l.activated_at_ms;
            // The current lock window (deadline - activated_at), derived from the
            // *live* deadline so it reflects any UpdateJobTimeout that moved the
            // deadline out (or in) since activation.
            dto.timeout_ms = l
                .deadline_ms
                .zip(l.activated_at_ms)
                .map(|(deadline, activated_at)| deadline.saturating_sub(activated_at));
        }
    }
}

/// `GET /console/api/instances/{key}` — one instance with its variables, jobs,
/// and incidents. Returns `None` when the key is malformed or unknown (404).
pub async fn instance_detail(server: &dyn ConsoleServer, key: &str) -> Option<InstanceDetailDto> {
    let key = key.parse::<u64>().ok()?;
    let row = server.store().process_instance(key)?;

    let variables: Vec<VariableDto> = server
        .store()
        .instance_variables(key)
        .iter()
        .map(|v| VariableDto {
            name: v.name.clone(),
            value: v.value.clone(),
            scope_key: v.scope_key.to_string(),
        })
        .collect();

    let mut jobs: Vec<JobDto> = server
        .store()
        .jobs()
        .iter()
        .filter(|j| j.instance_key == key)
        .map(|j| JobDto {
            key: j.key.to_string(),
            element_id: j.element_id.clone(),
            job_type: j.job_type.clone(),
            state: format!("{:?}", j.state),
            retries: j.retries,
            worker: j.worker.clone(),
            deadline_ms: j.deadline_ms,
            activated_at_ms: None,
            timeout_ms: None,
        })
        .collect();

    // Studio-only enhancement (#608): surface the live `Activated` lease that the
    // read model can't see under leader-local activation. For jobs still shown as
    // `Created` — and for rows already `Activated` under replicated activation,
    // which carry no `activated_at`/`timeout_ms` from the projection — ask the
    // owning partition's engine (the authoritative holder of the volatile
    // activation lease) for their true state and overlay
    // `Activated` + worker + deadline + live lock window. On a non-leader node no
    // handle is found (or the job isn't resident) and the row is left as-is (still
    // correct). `POST /jobs/search` is intentionally NOT overlaid, keeping it at
    // Zeebe parity (Zeebe has no queryable Activated job state either). A few point
    // lookups, off the hot path.
    let overlay_keys: Vec<u64> = jobs
        .iter()
        .filter(|d| d.state == "Created" || d.state == "Activated")
        .filter_map(|d| d.key.parse::<u64>().ok())
        .collect();
    if !overlay_keys.is_empty() {
        let live = server
            .instance_job_overlay(nanobpmn_engine_core::partition_of(key), overlay_keys)
            .await;
        apply_job_activation_overlay(&mut jobs, &live);
    }

    let incidents: Vec<IncidentDto> = server
        .store()
        .incidents()
        .iter()
        .filter(|i| i.instance_key == key)
        .map(|i| IncidentDto {
            key: i.key.to_string(),
            element_id: i.element_id.clone(),
            kind: format!("{:?}", i.kind),
            state: format!("{:?}", i.state),
            reason: i.reason.clone(),
            created_at_ms: i.created_at_ms,
        })
        .collect();

    let active_elements: Vec<ActiveElementDto> = server
        .store()
        .active_element_instances(key)
        .into_iter()
        .map(|e| ActiveElementDto {
            element_id: e.element_id,
            element_type: e.element_type,
            element_name: e.element_name,
        })
        .collect();

    // Parent -> child call-activity navigation (#1115): the child instances this
    // instance spawned via call activities, each tagged with the calling
    // call-activity cell. Children are found by the same parent-linkage scan the
    // v2 API uses (`parent_process_instance_key == key`); the calling element id
    // is resolved from the child's `parent_element_instance_key` via a point
    // lookup that ignores element state, so a COMPLETED call activity still
    // resolves. Empty when this instance spawned no children.
    let store = server.store();
    let children: Vec<nano_server_storage::readstore::ProcessInstanceRow> = store
        .process_instances()
        .into_iter()
        .filter(|r| r.parent_process_instance_key == Some(key))
        .collect();
    let called_instances = build_called_instances(&children, |eik| store.element_instance(eik));

    Some(InstanceDetailDto {
        instance: InstanceDto::from(&row),
        variables,
        jobs,
        incidents,
        active_elements,
        called_instances,
    })
}

#[cfg(test)]
mod called_instances_tests {
    use std::collections::HashMap;

    use nano_server_storage::readstore::{
        ElementInstanceRow, ElementInstanceState, ProcessInstanceRow,
    };
    use nanobpmn_engine_core::{Key, ProcessInstanceState};

    use super::*;

    fn child(key: Key, parent_pi: Key, parent_ei: Option<Key>) -> ProcessInstanceRow {
        ProcessInstanceRow {
            key,
            process_id: "child-proc".to_string(),
            process_definition_id: "child-proc".to_string(),
            process_definition_key: "9".to_string(),
            version: 3,
            state: ProcessInstanceState::Active,
            start_date_ms: 1_700_000_000_000,
            has_incident: false,
            tags: vec![],
            business_id: None,
            parent_process_instance_key: Some(parent_pi),
            parent_element_instance_key: parent_ei,
            suspended_date_ms: None,
        }
    }

    /// A COMPLETED call-activity element instance — resolution must not depend on
    /// the cell still being active, so the tests deliberately build it Completed.
    fn call_activity_element(
        eik: Key,
        parent_instance: Key,
        id: &str,
        name: Option<&str>,
    ) -> ElementInstanceRow {
        ElementInstanceRow {
            element_instance_key: eik,
            instance_key: parent_instance,
            process_definition_id: "parent-proc".to_string(),
            process_definition_key: "7".to_string(),
            element_id: id.to_string(),
            element_name: name.map(str::to_string),
            element_type: "CALL_ACTIVITY".to_string(),
            state: ElementInstanceState::Completed,
            start_date_ms: 1_600_000_000_000,
            end_date_ms: Some(1_600_000_001_000),
            scope_key: 0,
            incident_key: None,
            has_incident: false,
            tenant_id: "<default>".to_string(),
        }
    }

    #[test]
    fn single_child_resolves_calling_element_even_when_completed() {
        // Parent instance 42; its (COMPLETED) call-activity element instance 500
        // spawned child instance 100.
        let children = vec![child(100, 42, Some(500))];
        let elems: HashMap<Key, (&str, Option<&str>)> =
            HashMap::from([(500u64, ("CallChild", Some("Call the child")))]);

        let out = build_called_instances(&children, |eik| {
            elems
                .get(&eik)
                .map(|(id, name)| call_activity_element(eik, 42, id, *name))
        });

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key, "100");
        assert_eq!(out[0].process_id, "child-proc");
        assert_eq!(out[0].state, "Active");
        assert!(!out[0].has_incident);
        assert_eq!(out[0].calling_element_id.as_deref(), Some("CallChild"));
        assert_eq!(
            out[0].calling_element_name.as_deref(),
            Some("Call the child")
        );

        // Wire shape carries snake_case fields (console `Instance` convention).
        let json = serde_json::to_value(&out[0]).unwrap();
        assert_eq!(json["key"], "100");
        assert_eq!(json["calling_element_id"], "CallChild");
        assert_eq!(json["calling_element_name"], "Call the child");
    }

    #[test]
    fn multi_instance_children_share_one_calling_element_id() {
        // A multi-instance call activity `Fanout`: each inner instance is its own
        // element-instance key (600/601/602) but all resolve to the same BPMN id.
        let children = vec![
            child(100, 42, Some(600)),
            child(101, 42, Some(601)),
            child(102, 42, Some(602)),
        ];
        let elems: HashMap<Key, (&str, Option<&str>)> = HashMap::from([
            (600u64, ("Fanout", None)),
            (601u64, ("Fanout", None)),
            (602u64, ("Fanout", None)),
        ]);

        let out = build_called_instances(&children, |eik| {
            elems
                .get(&eik)
                .map(|(id, name)| call_activity_element(eik, 42, id, *name))
        });

        assert_eq!(out.len(), 3);
        assert!(
            out.iter()
                .all(|c| c.calling_element_id.as_deref() == Some("Fanout"))
        );
        let mut keys: Vec<&str> = out.iter().map(|c| c.key.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["100", "101", "102"]);
    }

    #[test]
    fn no_children_yields_empty_called_instances() {
        let children: Vec<ProcessInstanceRow> = vec![];
        let out = build_called_instances(&children, |_eik| {
            panic!("resolver must not be called when there are no children")
        });
        assert!(out.is_empty());
    }

    #[test]
    fn unresolvable_calling_element_leaves_ids_null() {
        // The child exists but its calling element instance was evicted — the
        // entry is still returned, with a null calling element id/name.
        let children = vec![child(100, 42, Some(999))];
        let out = build_called_instances(&children, |_eik| None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key, "100");
        assert!(out[0].calling_element_id.is_none());
        assert!(out[0].calling_element_name.is_none());
    }
}

#[cfg(test)]
mod job_activation_overlay_tests {
    use std::collections::HashMap;

    use super::*;

    fn job(key: &str, state: &str) -> JobDto {
        JobDto {
            key: key.to_string(),
            element_id: "task".to_string(),
            job_type: "work".to_string(),
            state: state.to_string(),
            retries: 3,
            worker: None,
            deadline_ms: None,
            activated_at_ms: None,
            timeout_ms: None,
        }
    }

    fn activated(worker: &str, deadline_ms: u64) -> LiveJob {
        LiveJob {
            state: "Activated".to_string(),
            worker: Some(worker.to_string()),
            deadline_ms: Some(deadline_ms),
            activated_at_ms: Some(deadline_ms.saturating_sub(300_000)),
        }
    }

    #[test]
    fn upgrades_created_to_activated_with_worker_and_deadline() {
        let mut jobs = vec![job("10", "Created")];
        let mut live = HashMap::new();
        live.insert(10u64, activated("w1", 5_000));
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].state, "Activated");
        assert_eq!(jobs[0].worker.as_deref(), Some("w1"));
        assert_eq!(jobs[0].deadline_ms, Some(5_000));
    }

    #[test]
    fn surfaces_activation_time_and_live_lock_window() {
        let mut jobs = vec![job("10", "Created")];
        let mut live = HashMap::new();
        // Activated at t=1_000 with a 2h lock → deadline 7_201_000. The overlay
        // derives the lock window from the *live* deadline (deadline - activated_at),
        // so it always reflects the current lock, including UpdateJobTimeout moves.
        live.insert(
            10u64,
            LiveJob {
                state: "Activated".to_string(),
                worker: Some("fleet".to_string()),
                deadline_ms: Some(7_201_000),
                activated_at_ms: Some(1_000),
            },
        );
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].activated_at_ms, Some(1_000));
        assert_eq!(jobs[0].timeout_ms, Some(7_200_000));
    }

    #[test]
    fn timeout_reflects_an_update_job_timeout_deadline_move() {
        // A job activated at t=1_000 with a 2h lock (deadline 7_201_000) then
        // extended via UpdateJobTimeout, which moves the deadline out to
        // 10_801_000 (activated_at unchanged). The Explorer "Timeout" must grow
        // to the new lock window (10_800_000 = 3h), not stay frozen at 2h.
        let mut jobs = vec![job("10", "Created")];
        let mut live = HashMap::new();
        live.insert(
            10u64,
            LiveJob {
                state: "Activated".to_string(),
                worker: Some("fleet".to_string()),
                deadline_ms: Some(10_801_000),
                activated_at_ms: Some(1_000),
            },
        );
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].timeout_ms, Some(10_800_000));
    }

    #[test]
    fn leaves_created_untouched_when_engine_has_no_live_entry() {
        // Engine no longer holds the job (e.g. evicted) — keep the read-model row.
        let mut jobs = vec![job("10", "Created")];
        let live: HashMap<u64, LiveJob> = HashMap::new();
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].state, "Created");
        assert!(jobs[0].worker.is_none());
        assert!(jobs[0].deadline_ms.is_none());
    }

    #[test]
    fn leaves_created_when_engine_still_reports_created() {
        let mut jobs = vec![job("10", "Created")];
        let mut live = HashMap::new();
        live.insert(
            10u64,
            LiveJob {
                state: "Created".to_string(),
                worker: None,
                deadline_ms: None,
                activated_at_ms: None,
            },
        );
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].state, "Created");
    }

    #[test]
    fn enriches_an_already_activated_row_from_replicated_activation() {
        // Under fully-replicated activation the `JobActivated` event is exported,
        // so the read-model row is already `Activated` with a worker + deadline —
        // but the projection stores no `activated_at`, leaving `activated_at_ms`
        // and `timeout_ms` null. The overlay must fill those in from the live
        // engine (and recompute the lock window from the live deadline) without
        // regressing the row.
        let mut jobs = vec![job("10", "Activated")];
        jobs[0].worker = Some("fleet".to_string());
        jobs[0].deadline_ms = Some(7_201_000);
        let mut live = HashMap::new();
        live.insert(
            10u64,
            LiveJob {
                state: "Activated".to_string(),
                worker: Some("fleet".to_string()),
                deadline_ms: Some(7_201_000),
                activated_at_ms: Some(1_000),
            },
        );
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].state, "Activated");
        assert_eq!(jobs[0].activated_at_ms, Some(1_000));
        assert_eq!(jobs[0].timeout_ms, Some(7_200_000));
    }

    #[test]
    fn leaves_activated_untouched_when_engine_has_no_live_entry() {
        // Engine no longer holds the job (e.g. evicted just after a replicated
        // activation) — keep the read-model row exactly as projected.
        let mut jobs = vec![job("10", "Activated")];
        jobs[0].worker = Some("fleet".to_string());
        jobs[0].deadline_ms = Some(7_201_000);
        let live: HashMap<u64, LiveJob> = HashMap::new();
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].state, "Activated");
        assert_eq!(jobs[0].worker.as_deref(), Some("fleet"));
        assert_eq!(jobs[0].deadline_ms, Some(7_201_000));
        assert!(jobs[0].activated_at_ms.is_none());
        assert!(jobs[0].timeout_ms.is_none());
    }

    #[test]
    fn never_regresses_a_terminal_row() {
        // A completed read-model job must not be overwritten even if a stale live
        // entry says Activated (guards against any race with eviction ordering).
        let mut jobs = vec![job("10", "Completed")];
        let mut live = HashMap::new();
        live.insert(10u64, activated("w1", 5_000));
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].state, "Completed");
        assert!(jobs[0].worker.is_none());
    }

    #[test]
    fn overlays_only_the_matching_keys() {
        let mut jobs = vec![job("10", "Created"), job("11", "Created")];
        let mut live = HashMap::new();
        live.insert(11u64, activated("w2", 9_000));
        apply_job_activation_overlay(&mut jobs, &live);
        assert_eq!(jobs[0].state, "Created"); // key 10: no live entry
        assert_eq!(jobs[1].state, "Activated"); // key 11: upgraded
        assert_eq!(jobs[1].worker.as_deref(), Some("w2"));
    }
}

/// (most-recent first). Backed by the in-memory [`trace::TraceStore`] folded
/// off the engine event stream (process-optimization design doc §3, Tier A).
pub fn traces(server: &dyn ConsoleServer, limit: usize) -> Vec<trace::TraceSummaryDto> {
    let limit = limit.clamp(1, 1000);
    server.trace_store().list(limit)
}

/// `GET /console/api/traces/{key}` — the full per-element trace for one
/// instance. `None` when the key is malformed or no longer retained in the ring.
pub fn trace_detail(server: &dyn ConsoleServer, key: &str) -> Option<trace::InstanceTraceDto> {
    let key = key.parse::<u64>().ok()?;
    server.trace_store().get(key)
}

/// `GET /console/api/traces/{key}/otel` — the instance trace rendered as an
/// OTLP/JSON trace document (root process span + per-element + per-job spans),
/// ingestible by an OpenTelemetry collector.
pub fn trace_otel(server: &dyn ConsoleServer, key: &str) -> Option<serde_json::Value> {
    let key = key.parse::<u64>().ok()?;
    server.trace_store().otel(key)
}

/// `GET /console/api/traces/config` — the capture configuration and in-memory
/// ring state of this node's [`trace::TraceStore`].
pub fn trace_config(server: &dyn ConsoleServer) -> trace::TraceConfigDto {
    server.trace_store().config()
}

/// `PUT /console/api/traces/config` — toggle variable / stimulus capture at
/// runtime. Node-local and non-persistent (resets to the `NANOBPMN_TRACE_*`
/// env defaults on restart). Returns the resulting configuration.
pub fn set_trace_config(
    server: &dyn ConsoleServer,
    variables: Option<bool>,
    stimuli: Option<bool>,
) -> trace::TraceConfigDto {
    server.trace_store().set_capture(variables, stimuli)
}

/// `GET /console/api/stream` — Server-Sent Events feed for live updates.
///
/// Emits an `instances` event whenever the read model's exported position
/// advances (i.e. the projection consumed new events). The position is a cheap
/// change signal: the client reacts by refetching the list/detail it cares
/// about, so the server stays stateless about *what* changed. An initial event
/// fires immediately so the client syncs on connect; keep-alive comments keep
/// intermediaries from dropping an idle connection.
async fn stream(
    State(server): State<ConsoleServerRef>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let store = server.store().clone();
    // `usize::MAX` as the seed guarantees the first poll differs, emitting an
    // immediate snapshot on connect.
    let s = unfold((store, usize::MAX), |(store, last)| async move {
        loop {
            let position = store.exported_position();
            if position != last {
                let active = store.active_instance_count();
                let data = format!(r#"{{"position":{position},"active":{active}}}"#);
                let event = Event::default().event("instances").data(data);
                return Some((Ok(event), (store, position)));
            }
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
    });
    Sse::new(s).keep_alive(KeepAlive::default())
}

// ---------------------------------------------------------------------------
// Modeler API — workspace-backed BPMN model library
// ---------------------------------------------------------------------------
//
// Models live on disk in the console workspace (see `workspace`), which is the
// authoring source of truth and is separate from the engine data dir. The engine
// holds *deployed* definitions (with their verbatim XML); the console reconciles
// the two. Deploy/pull/duplicate are intentionally **not** endpoints here: the
// frontend deploys via the standard `POST /v2/deployments`, pulls a deployed
// model via `GET /v2/process-definitions/{key}/xml`, and duplicates client-side
// (clone + rename the process id in bpmn-js, then save as a new model). This API
// is therefore pure workspace file CRUD plus a computed deploy status.

/// `not_deployed` (no deployed definition for the model's primary process id),
/// `in_sync` (deployed XML is byte-for-byte the file), `modified` (a definition
/// is deployed but differs), or `unparsable` (the file is not valid BPMN).
fn deploy_status_of(server: &dyn ConsoleServer, xml: &str) -> ModelStatus {
    let process_ids: Vec<String> = match parse_bpmn(xml) {
        Ok(defs) => defs.iter().map(|d| d.id.clone()).collect(),
        Err(_) => {
            return ModelStatus {
                process_ids: Vec::new(),
                deploy_status: "unparsable".into(),
                deployed_version: None,
                deployed_key: None,
            };
        }
    };
    // Status is reported against the file's primary (first) process id; a
    // multi-process resource is rare in the modeler.
    let primary = process_ids.first().cloned();
    let deployed = primary.as_ref().and_then(|id| {
        server
            .store()
            .process_definitions()
            .into_iter()
            // Deploy status compares against the current (latest) version, so
            // ignore superseded versions now that every version is surfaced.
            .find(|d| &d.process_id == id && d.is_latest)
    });
    let (deploy_status, deployed_version, deployed_key) = match deployed {
        None => ("not_deployed", None, None),
        Some(row) => {
            let deployed_xml = server
                .store()
                .process_definition_xml(row.key)
                .unwrap_or_default();
            let status = if deployed_xml == xml {
                "in_sync"
            } else {
                "modified"
            };
            (status, Some(row.version), Some(row.key.to_string()))
        }
    };
    ModelStatus {
        process_ids,
        deploy_status: deploy_status.into(),
        deployed_version,
        deployed_key,
    }
}

struct ModelStatus {
    process_ids: Vec<String>,
    deploy_status: String,
    deployed_version: Option<i32>,
    deployed_key: Option<String>,
}

#[derive(Serialize)]
struct ModelSummaryDto {
    name: String,
    process_ids: Vec<String>,
    deploy_status: String,
    deployed_version: Option<i32>,
    deployed_key: Option<String>,
    updated_at_ms: u64,
    size: u64,
}

#[derive(Serialize)]
struct ModelDto {
    name: String,
    xml: String,
    process_ids: Vec<String>,
    deploy_status: String,
    deployed_version: Option<i32>,
    deployed_key: Option<String>,
}

/// `GET /console/api/models` — the model library, with each model's deploy
/// status relative to the engine. Sorted by name.
pub fn models(server: &dyn ConsoleServer) -> ApiResult {
    let names = workspace::list_model_names().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not read workspace: {e}"),
        )
    })?;
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let Some(path) = workspace::model_path(&name) else {
            continue;
        };
        let xml = std::fs::read_to_string(&path).unwrap_or_default();
        let (updated_at_ms, size) = workspace::file_meta(&path);
        let status = deploy_status_of(server, &xml);
        out.push(ModelSummaryDto {
            name,
            process_ids: status.process_ids,
            deploy_status: status.deploy_status,
            deployed_version: status.deployed_version,
            deployed_key: status.deployed_key,
            updated_at_ms,
            size,
        });
    }
    Ok(serde_json::to_value(out).unwrap())
}

/// `GET /console/api/models/{name}` — one model's XML and deploy status.
pub fn model_get(server: &dyn ConsoleServer, name: &str) -> ApiResult {
    let Some(path) = workspace::model_path(name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid model name".to_string()));
    };
    let xml = std::fs::read_to_string(&path)
        .map_err(|_| (StatusCode::NOT_FOUND, "no such model".to_string()))?;
    let status = deploy_status_of(server, &xml);
    Ok(serde_json::to_value(ModelDto {
        name: name.to_string(),
        xml,
        process_ids: status.process_ids,
        deploy_status: status.deploy_status,
        deployed_version: status.deployed_version,
        deployed_key: status.deployed_key,
    })
    .unwrap())
}

/// `PUT /console/api/models/{name}` — overwrite (save) a model's XML. The body
/// is the raw BPMN XML. The model must already exist (use POST to create).
pub fn model_save(server: &dyn ConsoleServer, name: &str, xml: String) -> ApiResult {
    let Some(path) = workspace::model_path(name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid model name".to_string()));
    };
    if !path.exists() {
        return Err((
            StatusCode::NOT_FOUND,
            "no such model — create it first".to_string(),
        ));
    }
    if let Err(e) = std::fs::write(&path, &xml) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save model: {e}"),
        ));
    }
    let status = deploy_status_of(server, &xml);
    Ok(serde_json::to_value(ModelDto {
        name: name.to_string(),
        xml,
        process_ids: status.process_ids,
        deploy_status: status.deploy_status,
        deployed_version: status.deployed_version,
        deployed_key: status.deployed_key,
    })
    .unwrap())
}

/// `POST /console/api/models` — create a new model. 409 if a model with the
/// same name already exists.
pub fn model_create(server: &dyn ConsoleServer, name: String, xml: String) -> ApiResult {
    let Some(path) = workspace::model_path(&name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid model name".to_string()));
    };
    if let Err(e) = workspace::ensure_models_dir() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create workspace: {e}"),
        ));
    }
    if path.exists() {
        return Err((
            StatusCode::CONFLICT,
            "a model with that name already exists".to_string(),
        ));
    }
    if let Err(e) = std::fs::write(&path, &xml) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create model: {e}"),
        ));
    }
    let status = deploy_status_of(server, &xml);
    Ok(serde_json::to_value(ModelDto {
        name,
        xml,
        process_ids: status.process_ids,
        deploy_status: status.deploy_status,
        deployed_version: status.deployed_version,
        deployed_key: status.deployed_key,
    })
    .unwrap())
}

/// `DELETE /console/api/models/{name}` — remove a model from the workspace.
/// This never touches the engine; an already-deployed definition stays deployed.
pub fn model_delete(name: &str) -> ApiResult {
    let Some(path) = workspace::model_path(name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid model name".to_string()));
    };
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "no such model".to_string()))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete model: {e}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Workers API — workspace-backed worker code + a Deno subprocess supervisor
// ---------------------------------------------------------------------------
//
// A worker is a directory of source files under `workers/<name>/` (an entry
// `worker.ts` plus optional helpers and a `deno.json`). The supervisor (see
// `workers`) runs each enabled worker as a sandboxed Deno subprocess that speaks
// the Falcon protocol. This API is workspace file CRUD plus start/stop and a live
// log/metrics view; it never touches the engine data dir.

/// Default `worker.ts` scaffold for a new worker. Imports the embedded SDK via
/// the import map in `deno.json` and echoes the job's input back as output.
fn worker_scaffold_ts(job_type: &str) -> String {
    format!(
        r#"import {{ defineWorker }} from "@nanobpm/worker";

// A worker handles jobs of one BPMN job type. Return output variables to
// complete the job, or call job.fail(...) / job.error(code, msg). Throwing
// fails the job. You can `import` npm packages with `npm:` specifiers.
defineWorker({{
  type: "{job_type}",
  maxParallelJobs: 10,
  async handle(job) {{
    console.log(`handling job ${{job.jobKey}} for instance ${{job.processInstanceKey}}`);
    // ...do your work here, using job.variables...
    return {{ handledBy: "{job_type}" }};
  }},
}});
"#
    )
}

/// `deno.json` mapping the `@nanobpm/worker` specifier to the embedded SDK and
/// the `@lib/` alias to the shared workspace library, so a worker can both
/// `import { defineWorker } from "@nanobpm/worker"` and reuse shared logic with
/// `import { fmt } from "@lib/money.ts"`.
const WORKER_DENO_JSON: &str = r#"{
  "imports": {
    "@nanobpm/worker": "../../nano-generated/workers.ts",
    "@lib/": "../../lib/"
  }
}
"#;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSummaryDto {
    name: String,
    files: Vec<String>,
    updated_at_ms: u64,
    runtime: workers::WorkerRuntimeDto,
}

#[derive(Deserialize)]
struct FilePathQuery {
    path: String,
}

/// Query for the loopback-only filesystem browser (`fs_browse`). `path` is an
/// optional absolute host path; when absent the browser opens on the home dir.
#[derive(Deserialize)]
struct BrowseQuery {
    path: Option<String>,
}

pub async fn worker_summary(name: &str) -> Option<WorkerSummaryDto> {
    let dir = workspace::worker_dir(name)?;
    if !dir.is_dir() {
        return None;
    }
    let files = workspace::list_worker_files(name).unwrap_or_default();
    let (updated_at_ms, _) = workspace::file_meta(&dir);
    let runtime = workers::supervisor().runtime(name).await;
    Some(WorkerSummaryDto {
        name: name.to_string(),
        files,
        updated_at_ms,
        runtime,
    })
}

/// `GET /console/api/workers` — list workers with files and runtime status.
pub async fn workers_list() -> ApiResult {
    let names = workspace::list_worker_names().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not read workspace: {e}"),
        )
    })?;
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        if let Some(s) = worker_summary(&name).await {
            out.push(s);
        }
    }
    Ok(serde_json::json!({
        "workers": out,
        "denoAvailable": workers::supervisor().deno_available(),
        "nodeAvailable": workers::supervisor().node_available(),
    }))
}

/// `POST /console/api/workers` — scaffold a new worker directory.
pub async fn worker_create(name: String, job_type: Option<String>) -> ApiResult {
    let Some(dir) = workspace::worker_dir(&name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid worker name".to_string()));
    };
    if dir.exists() {
        return Err((
            StatusCode::CONFLICT,
            "a worker with that name already exists".to_string(),
        ));
    }
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create worker: {e}"),
        ));
    }
    let job_type = job_type.unwrap_or_else(|| name.clone());
    if let Err(e) = std::fs::write(dir.join("worker.ts"), worker_scaffold_ts(&job_type)) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not write worker.ts: {e}"),
        ));
    }
    let _ = std::fs::write(dir.join("deno.json"), WORKER_DENO_JSON);
    match worker_summary(&name).await {
        Some(s) => Ok(serde_json::to_value(s).unwrap()),
        None => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not read worker".to_string(),
        )),
    }
}

/// `GET /console/api/worker-sdk` — the embedded worker SDK TypeScript source.
pub fn worker_sdk_source() -> String {
    worker_export::worker_sdk_source().to_string()
}

/// `GET /console/api/deno-types` — the embedded Deno namespace ambient types.
pub fn deno_types_source() -> String {
    worker_export::deno_namespace_types().to_string()
}

/// `GET /console/api/urban-types` — the vendored `@nanobpm/urban` type surface,
/// served so the Studio editor's Monaco TS service resolves an Urban app's
/// `@nanobpm/urban` imports with full IntelliSense (offline; see
/// `scripts/vendor-urban-types.md`).
async fn urban_types() -> Response {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; charset=utf-8".to_string(),
        )],
        worker_export::urban_types_source(),
    )
        .into_response()
}

/// Body for `POST /console/api/export-workers-app`.
#[derive(Deserialize)]
struct ExportWorkersBody {
    /// The worker names to bundle into the standalone application.
    #[serde(default)]
    workers: Vec<String>,
}

/// `POST /console/api/export-workers-app` — bundle the selected workers into a
/// standalone, runnable Deno application, returned as a downloadable `.zip`
/// (see [`worker_export`]).
async fn workers_export(Json(body): Json<ExportWorkersBody>) -> Response {
    match worker_export::build_app(&body.workers) {
        Ok(zip) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/zip".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}\"", worker_export::zip_filename()),
                ),
                (header::CACHE_CONTROL, "no-store".to_string()),
            ],
            zip,
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// `GET /console/api/workers/{name}` — one worker's files and runtime status.
pub async fn worker_get(name: &str) -> ApiResult {
    match worker_summary(name).await {
        Some(s) => Ok(serde_json::to_value(s).unwrap()),
        None => Err((StatusCode::NOT_FOUND, "no such worker".to_string())),
    }
}

/// `DELETE /console/api/workers/{name}` — remove a worker (must be stopped).
pub async fn worker_delete(name: &str) -> ApiResult {
    let Some(dir) = workspace::worker_dir(name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid worker name".to_string()));
    };
    if workers::supervisor().is_active(name).await {
        return Err((
            StatusCode::CONFLICT,
            "stop the worker before deleting it".to_string(),
        ));
    }
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "no such worker".to_string()))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete worker: {e}"),
        )),
    }
}

/// `GET /console/api/workers/{name}/file?path=worker.ts` — read a worker file.
pub fn worker_file_get(name: &str, rel: &str) -> Result<String, (StatusCode, String)> {
    let Some(path) = workspace::worker_file_path(name, rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    std::fs::read_to_string(&path).map_err(|_| (StatusCode::NOT_FOUND, "no such file".to_string()))
}

/// `PUT /console/api/workers/{name}/file?path=worker.ts` — save (create or
/// overwrite) a worker file. Body is the raw file content.
pub fn worker_file_save(name: &str, rel: &str, body: &str) -> ApiResult {
    let Some(dir) = workspace::worker_dir(name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid worker name".to_string()));
    };
    if !dir.is_dir() {
        return Err((StatusCode::NOT_FOUND, "no such worker".to_string()));
    }
    let Some(path) = workspace::worker_file_path(name, rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    match std::fs::write(&path, body) {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save file: {e}"),
        )),
    }
}

/// `POST /console/api/workers/{name}/file` — create a new empty worker file.
pub fn worker_file_create(name: &str, rel: &str) -> ApiResult {
    let Some(dir) = workspace::worker_dir(name) else {
        return Err((StatusCode::BAD_REQUEST, "invalid worker name".to_string()));
    };
    if !dir.is_dir() {
        return Err((StatusCode::NOT_FOUND, "no such worker".to_string()));
    }
    let Some(path) = workspace::worker_file_path(name, rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    if path.exists() {
        return Err((
            StatusCode::CONFLICT,
            "a file with that name already exists".to_string(),
        ));
    }
    match std::fs::write(&path, "") {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create file: {e}"),
        )),
    }
}

/// `DELETE /console/api/workers/{name}/file?path=...` — remove a worker file.
pub fn worker_file_delete(name: &str, rel: &str) -> ApiResult {
    let Some(path) = workspace::worker_file_path(name, rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "no such file".to_string()))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete file: {e}"),
        )),
    }
}

/// `POST /console/api/workers/{name}/start` — start the worker subprocess.
pub async fn worker_start(name: &str) -> ApiResult {
    let sup = workers::supervisor();
    match sup.start(name).await {
        Ok(()) => Ok(serde_json::to_value(sup.runtime(name).await).unwrap()),
        Err(e) => Err((StatusCode::BAD_REQUEST, e)),
    }
}

// ---------------------------------------------------------------------------
// Shared library API — reusable TS/JS files under `<workspace>/lib/`, importable
// from every worker via the `@lib/` import-map alias. Mirrors the worker file
// CRUD; the library has no runtime of its own (it is only ever imported).
// ---------------------------------------------------------------------------

/// `GET /console/api/lib` — list the shared library files.
pub fn lib_list() -> ApiResult {
    match workspace::list_lib_files() {
        Ok(files) => Ok(serde_json::json!({ "files": files })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not read library: {e}"),
        )),
    }
}

/// `GET /console/api/lib/file?path=money.ts` — read a shared library file.
pub fn lib_file_get(rel: &str) -> Result<String, (StatusCode, String)> {
    let Some(path) = workspace::lib_file_path(rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    std::fs::read_to_string(&path).map_err(|_| (StatusCode::NOT_FOUND, "no such file".to_string()))
}

/// `PUT /console/api/lib/file?path=money.ts` — save (create or overwrite) a
/// shared library file. Body is the raw file content.
pub fn lib_file_save(rel: &str, body: &str) -> ApiResult {
    let Ok(_) = workspace::ensure_lib_dir() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not create library dir".to_string(),
        ));
    };
    let Some(path) = workspace::lib_file_path(rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    match std::fs::write(&path, body) {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save file: {e}"),
        )),
    }
}

/// `POST /console/api/lib/file` — create a new empty shared library file.
pub fn lib_file_create(rel: &str) -> ApiResult {
    let Ok(_) = workspace::ensure_lib_dir() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not create library dir".to_string(),
        ));
    };
    let Some(path) = workspace::lib_file_path(rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    if path.exists() {
        return Err((
            StatusCode::CONFLICT,
            "a file with that name already exists".to_string(),
        ));
    }
    match std::fs::write(&path, "") {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create file: {e}"),
        )),
    }
}

/// `DELETE /console/api/lib/file?path=...` — remove a shared library file.
pub fn lib_file_delete(rel: &str) -> ApiResult {
    let Some(path) = workspace::lib_file_path(rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "no such file".to_string()))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete file: {e}"),
        )),
    }
}

/// `POST /console/api/workers/{name}/stop` — stop the worker subprocess.
pub async fn worker_stop(name: &str) -> ApiResult {
    let sup = workers::supervisor();
    match sup.stop(name).await {
        Ok(()) => Ok(serde_json::to_value(sup.runtime(name).await).unwrap()),
        Err(e) => Err((StatusCode::BAD_REQUEST, e)),
    }
}

/// Live state for the worker-log SSE stream: replay the history buffer, then
/// stream live lines from the broadcast receiver.
enum LogStreamState {
    History(
        std::vec::IntoIter<workers::LogLine>,
        broadcast::Receiver<workers::LogLine>,
    ),
    Live(broadcast::Receiver<workers::LogLine>),
}

/// `GET /console/api/workers/{name}/logs` — SSE stream of a worker's logs.
/// Replays the recent buffer on connect, then streams live lines.
async fn worker_logs(
    Path(name): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let sup = workers::supervisor();
    let history = sup.log_history(&name).await;
    let rx = sup.subscribe(&name).await;

    let stream = unfold(
        LogStreamState::History(history.into_iter(), rx),
        |st| async move {
            match st {
                LogStreamState::History(mut it, rx) => match it.next() {
                    Some(line) => Some((Ok(log_event(&line)), LogStreamState::History(it, rx))),
                    None => recv_live(rx).await,
                },
                LogStreamState::Live(rx) => recv_live(rx).await,
            }
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn log_event(line: &workers::LogLine) -> Event {
    let data = serde_json::to_string(line).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("log").data(data)
}

/// Pulls the next live log line, skipping lag and ending the stream on close.
async fn recv_live(
    mut rx: broadcast::Receiver<workers::LogLine>,
) -> Option<(Result<Event, Infallible>, LogStreamState)> {
    loop {
        match rx.recv().await {
            Ok(line) => return Some((Ok(log_event(&line)), LogStreamState::Live(rx))),
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

// ---------------------------------------------------------------------------
// Projects API — the RAD environment. A project is a self-contained directory
// (resources/, workers/, lib/, main.ts, deno.json, nanobpm.project.json) that
// is itself a runnable Deno app. See `projects`.
// ---------------------------------------------------------------------------

/// `GET /console/api/projects` — list projects (tiles) with resource counts and
/// live run status.
pub async fn projects_list() -> ApiResult {
    let mut list = projects::list_projects().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not read projects: {e}"),
        )
    })?;
    let sup = projects::supervisor();
    let mut out = Vec::with_capacity(list.len());
    for mut p in list.drain(..) {
        p.running = sup.is_running(&p.name).await;
        // Only running apps appear in the left-rail running-apps surface, so
        // resolve the app-view descriptor lazily for those (avoids a manifest +
        // config read per stopped project on every listing).
        if p.running {
            p.app_ui = Some(sup.app_ui(&p.name).await);
        }
        out.push(p);
    }
    Ok(serde_json::json!({
        "projects": out,
        "denoAvailable": sup.deno_available(),
        "nodeAvailable": sup.node_available(),
        "urbanAvailable": urban::urban_available(),
        "platforms": projects::PLATFORMS,
        "templates": projects::project_templates(),
        "extensions": extensions_overview(),
    }))
}

/// Extensions + which lang/app packs are usable on this machine.
pub fn extensions_overview() -> serde_json::Value {
    let exts = extensions::all_extensions();
    let trust = extensions::load_trust();
    let list: Vec<_> = exts.iter().map(extension_json).collect();
    serde_json::json!({ "extensions": list, "yolo": trust.yolo })
}

/// Enrich a pack manifest into the spec's `Extension` response shape. The
/// generated model requires the computed `toolchainAvailable` and `trusted`
/// fields on top of the raw manifest, so both the overview list and the
/// install response must build entries through here — returning a bare
/// manifest makes the generated round-trip panic on the missing fields.
/// `toolchain_available` shells out (`<bin> --version`), so call this off the
/// async runtime (it already runs inside sync/`spawn_blocking` contexts).
fn extension_json(e: &extensions::ExtManifest) -> serde_json::Value {
    let trusted = extensions::is_trusted(&e.id);
    serde_json::json!({
        "id": e.id, "kind": e.kind, "displayName": e.display_name, "builtin": e.builtin,
        "icon": e.icon,
        "fileTypes": e.file_types, "templates": e.templates,
        "themes": e.themes,
        "intellisense": e.intellisense,
        "components": extensions::pack_component_templates(&e.id),
        "toolchainAvailable": extensions::toolchain_available(e),
        // Resolve trust once and reuse it for both the reported flag and the tour
        // filtering, so the two cannot disagree and the trust store is read once.
        "trusted": trusted,
        // Handoff steps are stripped for untrusted packs before they are ever
        // sent — see extensions::visible_tours.
        "tours": extensions::visible_tours(e, trusted),
    })
}

/// `POST /console/api/projects` — scaffold a new project.
pub fn project_create(
    name: &str,
    description: &str,
    template: &str,
    options: &std::collections::HashMap<String, String>,
) -> ApiResult {
    match projects::create_project_with_options(name, description, template, options) {
        Ok(cfg) => Ok(serde_json::to_value(cfg).unwrap()),
        Err(e) if e.contains("already exists") => Err((StatusCode::CONFLICT, e)),
        Err(e) if e.contains("invalid") => Err((StatusCode::BAD_REQUEST, e)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

/// `GET /console/api/extensions` — installed + built-in packs and trust state.
pub fn extensions_list() -> serde_json::Value {
    extensions_overview()
}

/// `GET /console/api/config/server` — SLA mode + read-only env-parameter registry.
pub fn config_server(server: &dyn ConsoleServer) -> serde_json::Value {
    config::server_config_json(server.sla_mode())
}

/// `PUT /console/api/config/server/sla` — switch the SLA mode at runtime. Body
/// `{"mode":"latency"|"admission"}`. An unrecognised mode is rejected (400)
/// rather than silently fail-safing, so an operator gets clear feedback; the
/// updated config is returned on success.
pub async fn config_server_sla(server: &dyn ConsoleServer, mode: &str) -> ApiResult {
    let mode = match mode.trim().to_ascii_lowercase().as_str() {
        "latency" => SlaMode::Latency,
        "admission" => SlaMode::Admission,
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unknown SLA mode {other:?}; expected \"latency\" or \"admission\""),
            ));
        }
    };
    server.switch_sla_mode(mode).await;
    Ok(config::server_config_json(server.sla_mode()))
}

/// `GET /console/api/config/ide` — toolchain dependencies + language-pack config.
/// Probing toolchains shells out (`<bin> --version`), so run it off the async
/// runtime's worker threads.
pub async fn config_ide() -> ApiResult {
    match tokio::task::spawn_blocking(config::ide_config_json).await {
        Ok(v) => Ok(v),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// `GET /console/api/extensions/marketplace` — packs on npm tagged `nano-ide-ext`,
/// categorised by language/app/example, with installed status. `force` is the
/// "check now" bypass that skips the cache TTL (still single-flight).
pub async fn extensions_marketplace(force: bool) -> ApiResult {
    match tokio::task::spawn_blocking(move || extensions::marketplace_refresh(force)).await {
        Ok(Ok(list)) => Ok(serde_json::json!({ "entries": list })),
        Ok(Err(e)) => Err((StatusCode::BAD_GATEWAY, e)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// `GET /console/api/extensions/readme?pkg=<name>` — a pack's README (markdown).
/// Reads an installed pack's bundled README, else fetches it from npm. Returns
/// 404 when no README can be found. `npm view` shells out, so run it off the
/// async runtime's worker threads.
pub async fn extensions_readme(pkg: String) -> ApiResult {
    let name = pkg.clone();
    match tokio::task::spawn_blocking(move || extensions::pack_readme(&name)).await {
        Ok(Some(r)) => Ok(serde_json::json!({
            "pkg": pkg,
            "readme": r.readme,
            "installed": r.installed,
        })),
        Ok(None) => Err((StatusCode::NOT_FOUND, "no README for that pack".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// `GET /console/api/extensions/changelog?pkg=<name>&from=<installed>&to=<latest>`
/// — a pack's changelog (markdown). Reads an installed pack's bundled
/// `CHANGELOG.md`, else downloads the published registry tarball and reads it.
/// When `from` (the installed version) is supplied and the headings parse, the
/// result is scoped to the delta between `from` and the latest — i.e. what
/// changed since the running version. Returns 404 when no changelog can be
/// found. Shells out (`npm pack`), so run it off the async worker threads.
pub async fn extensions_changelog(
    pkg: String,
    from: Option<String>,
    to: Option<String>,
) -> ApiResult {
    let name = pkg.clone();
    match tokio::task::spawn_blocking(move || {
        extensions::pack_changelog(&name, from.as_deref(), to.as_deref())
    })
    .await
    {
        Ok(Some(c)) => Ok(serde_json::json!({
            "pkg": pkg,
            "changelog": c.changelog,
            "installed": c.installed,
            "delta": c.delta,
        })),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            "no changelog for that pack".to_string(),
        )),
        // Join failure (the blocking task panicked). Log the underlying
        // `JoinError` so a real panic in `pack_changelog` is diagnosable in
        // production, then return a generic message — never surface the
        // internal `JoinError` string to the client — and let the API layer
        // map the 500 status to a distinct InternalError response.
        Err(join_err) => {
            tracing::error!(error = %join_err, "changelog blocking task join failed");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error reading changelog".to_string(),
            ))
        }
    }
}

/// `GET /console/api/server/update` — server version + self-update status.
pub async fn server_update() -> ApiResult {
    Ok(
        serde_json::to_value(server_update::status().await).unwrap_or_else(|_| {
            serde_json::json!({
                "current": env!("NANOBPM_VERSION"),
                "updateAvailable": false,
                "canSelfUpdate": false,
                "installMethod": "unknown",
            })
        }),
    )
}

/// `POST /console/api/extensions/install` — install a `nano-ide-ext-*` pkg from npm.
pub async fn extensions_install(pkg: String) -> ApiResult {
    match tokio::task::spawn_blocking(move || {
        extensions::install_from_npm(&pkg).map(|m| extension_json(&m))
    })
    .await
    {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err((StatusCode::BAD_REQUEST, e)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// `POST /console/api/urban/install` — install-before-create hook (#520).
/// Idempotently ensures the Urban toolkit (`nano-ide-app-urban` pack + its
/// `urban`/`create-urban-app` deps) is present, then reports whether the CLI
/// resolves. Runs on the blocking pool (npm + fs work).
pub async fn ensure_urban_toolkit() -> Result<bool, (StatusCode, String)> {
    match tokio::task::spawn_blocking(extensions::ensure_urban_toolkit).await {
        Ok(Ok(available)) => Ok(available),
        Ok(Err(e)) => Err((StatusCode::BAD_REQUEST, e)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// `POST /console/api/extensions/remove` — uninstall an installed pack.
pub fn extensions_remove(pkg: &str) -> ApiResult {
    match extensions::remove(pkg) {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) => Err((StatusCode::BAD_REQUEST, e)),
    }
}

/// `POST /console/api/extensions/trust` — toggle yolo / approve-always per pack.
pub fn extensions_trust(
    yolo: Option<bool>,
    approve: Option<String>,
    revoke: Option<String>,
) -> ApiResult {
    let mut t = extensions::load_trust();
    if let Some(y) = yolo {
        t.yolo = y;
    }
    if let Some(id) = approve {
        t.approved.insert(id);
    }
    if let Some(id) = revoke {
        t.approved.remove(&id);
    }
    match extensions::save_trust(&t) {
        Ok(()) => Ok(extensions_overview()),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

/// `GET /console/api/projects/{name}` — config + file tree + run state. The
/// `runState.status` here can be `crashed`, which the generated layer maps to
/// the spec's `error` run status.
pub async fn project_detail(name: &str) -> ApiResult {
    let Some(cfg) = projects::read_config(name) else {
        return Err((StatusCode::NOT_FOUND, "no such project".to_string()));
    };
    let tree = projects::file_tree(name).unwrap_or_default();
    let sup = projects::supervisor();
    // Run/Compile readiness is language-aware: Deno projects need the Deno
    // runtime *or* the Node fallback (>= 22.6) on hosts with no Deno build,
    // e.g. 32-bit ARM (ADR 0036); a polyglot lang pack (e.g. Rust) needs its
    // own toolchain (cargo).
    let runnable = if cfg.lang == "deno" {
        sup.deno_available() || sup.node_available()
    } else {
        extensions::lang_pack(&cfg.lang)
            .map(|p| extensions::toolchain_available(&p))
            .unwrap_or(false)
    };
    // When not runnable, tell the user exactly what we probed for (which binary,
    // on PATH) plus the pack's install hint — a bare "toolchain missing" leaves
    // them guessing which tool to install.
    let missing_toolchain = missing_toolchain_json(&cfg.lang, runnable);
    // Absolute on-disk location of the project, so the Console can show users
    // where their files live (header display + "copy path" in the file tree).
    // Canonicalize to resolve symlinks / relative roots; fall back to the joined
    // path if canonicalization fails (e.g. a transient FS error).
    let root_path = projects::project_dir(name).map(|p| {
        std::fs::canonicalize(&p)
            .unwrap_or(p)
            .to_string_lossy()
            .into_owned()
    });
    let app_ui = sup.app_ui(name).await;
    Ok(serde_json::json!({
        "config": cfg,
        "files": tree,
        "runState": sup.run_state(name).await,
        "appUi": app_ui,
        "denoAvailable": sup.deno_available(),
        "nodeAvailable": sup.node_available(),
        // Presence of the `@nanobpm/urban` CLI (epic #514 host dry-out). The
        // Studio gates urban-app affordances on this; the binary is delivered
        // by the marketplace pack in #520. See console::urban.
        "urbanAvailable": urban::urban_available(),
        "runnable": runnable,
        "missingToolchain": missing_toolchain,
        "platforms": projects::PLATFORMS,
        "rootPath": root_path,
    }))
}

/// Describe the missing run/compile toolchain for a project's `lang` so the
/// Console banner can name the exact executable the probe looked for (and the
/// pack's install hint), instead of a generic "toolchain missing". Returns
/// `None` when the project is runnable. Pure: given `lang` + `runnable` it only
/// reads the (already-probed) lang pack manifest, so it's unit-testable without
/// a supervisor or an on-disk project.
fn missing_toolchain_json(lang: &str, runnable: bool) -> Option<serde_json::Value> {
    if runnable {
        return None;
    }
    if lang == "deno" {
        return Some(serde_json::json!({
            "displayName": "Deno / Node",
            "probes": ["deno", "node"],
            "installHint": "Install the Deno runtime, or Node \u{2265} 22.6 (used as a fallback where Deno has no build, e.g. 32-bit ARM).",
            "installUrl": "https://deno.com/",
        }));
    }
    extensions::lang_pack(lang).map(|p| {
        let probes: Vec<&str> = p
            .toolchain
            .detect
            .first()
            .map(|b| vec![b.as_str()])
            .unwrap_or_default();
        serde_json::json!({
            "displayName": p.display_name,
            "probes": probes,
            "installHint": p.toolchain.install_hint,
            "installUrl": p.toolchain.install_url,
        })
    })
}

#[cfg(test)]
mod missing_toolchain_tests {
    use super::*;

    #[test]
    fn runnable_project_reports_no_missing_toolchain() {
        assert!(missing_toolchain_json("deno", true).is_none());
        assert!(missing_toolchain_json("rust", true).is_none());
    }

    #[test]
    fn missing_deno_names_both_runtimes() {
        let v = missing_toolchain_json("deno", false).expect("deno payload");
        let probes: Vec<_> = v["probes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p.as_str().unwrap())
            .collect();
        assert_eq!(probes, ["deno", "node"]);
        assert!(v["installUrl"].as_str().unwrap().contains("deno.com"));
    }

    #[test]
    fn unknown_lang_pack_yields_no_payload() {
        // No pack installed for this id, so there is nothing specific to name.
        assert!(missing_toolchain_json("no-such-lang", false).is_none());
    }
}

/// `DELETE /console/api/projects/{name}` — remove a project (must be stopped).
pub async fn project_delete(name: &str) -> ApiResult {
    if projects::supervisor().is_running(name).await {
        return Err((
            StatusCode::CONFLICT,
            "stop the application before deleting it".to_string(),
        ));
    }
    match projects::delete_project(name) {
        Ok(()) => {
            // Drop the memoised "inbox ensured" marker (#1340) so that a later
            // project reusing this name re-creates its table on a fresh
            // datasource rather than trusting a stale memo.
            triggers::forget_inbox(name);
            Ok(serde_json::Value::Null)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "no such project".to_string()))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete project: {e}"),
        )),
    }
}

/// `POST /console/api/projects/{name}/rename` — rename a project (must be stopped).
pub async fn project_rename(name: &str, new_name: &str) -> ApiResult {
    if projects::supervisor().is_running(name).await {
        return Err((
            StatusCode::CONFLICT,
            "stop the application before renaming it".to_string(),
        ));
    }
    match projects::rename_project(name, new_name.trim()) {
        Ok(cfg) => {
            // A rename moves the project (and its datasource) to a new name and
            // frees the old one. Drop the old name's memoised "inbox ensured"
            // marker (#1340): a fresh project later created under the old name
            // gets a brand-new datasource, and a stale marker would make
            // `ensure_inbox` skip the `CREATE`, so enqueue/drain fail on the
            // missing `trigger_inbox` table.
            triggers::forget_inbox(name);
            Ok(serde_json::to_value(cfg).unwrap())
        }
        Err(e) if e.contains("already exists") => Err((StatusCode::CONFLICT, e)),
        Err(e) if e.contains("no such") => Err((StatusCode::NOT_FOUND, e)),
        Err(e) if e.contains("invalid") => Err((StatusCode::BAD_REQUEST, e)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}

/// `GET /console/api/projects/{name}/config` — the project config.
pub fn project_config_get(name: &str) -> ApiResult {
    match projects::read_config(name) {
        Some(cfg) => Ok(serde_json::to_value(cfg).unwrap()),
        None => Err((StatusCode::NOT_FOUND, "no such project".to_string())),
    }
}

/// `PUT /console/api/projects/{name}/config` — update the project config.
pub fn project_config_put(name: &str, mut cfg: projects::ProjectConfig) -> ApiResult {
    if projects::read_config(name).is_none() {
        return Err((StatusCode::NOT_FOUND, "no such project".to_string()));
    }
    cfg.name = name.to_string();
    cfg.updated_ms = now_ms_proj();
    match projects::write_config(name, &cfg) {
        Ok(()) => Ok(serde_json::to_value(cfg).unwrap()),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save config: {e}"),
        )),
    }
}

/// `GET /console/api/projects/{name}/run-configs` — list the named run
/// configurations snapshotted from the scaffolding pack and the id of the
/// active one (or `null` when none is set — in which case the resolver picks
/// the `default: true` entry, else the first).
pub fn project_run_configs_list(name: &str) -> ApiResult {
    let Some(cfg) = projects::read_config(name) else {
        return Err((StatusCode::NOT_FOUND, "no such project".to_string()));
    };
    let (configs, active) = match cfg.toolchain.as_ref() {
        Some(tc) => (tc.run_configs.clone(), tc.active_run_config.clone()),
        None => (vec![], None),
    };
    Ok(serde_json::json!({ "runConfigs": configs, "active": active }))
}

/// `PUT /console/api/projects/{name}/active-run-config` — set which run config
/// the Run/Compile buttons should use. Body: `{ "id": "stock-rest" }`; pass
/// `null` (or omit) to clear the pin and revert to `default: true` / first.
/// Rejects unknown ids so the picker can't silently persist a typo.
pub fn project_active_run_config_put(name: &str, id: Option<String>) -> ApiResult {
    let Some(mut cfg) = projects::read_config(name) else {
        return Err((StatusCode::NOT_FOUND, "no such project".to_string()));
    };
    let Some(tc) = cfg.toolchain.as_mut() else {
        return Err((
            StatusCode::BAD_REQUEST,
            "project has no toolchain (no run configs to select)".to_string(),
        ));
    };
    if let Some(id) = id.as_deref()
        && !tc.run_configs.iter().any(|rc| rc.id == id)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("no run config with id '{id}'"),
        ));
    }
    tc.active_run_config = id;
    cfg.updated_ms = now_ms_proj();
    match projects::write_config(name, &cfg) {
        Ok(()) => Ok(serde_json::json!({
            "active": cfg.toolchain.as_ref().and_then(|t| t.active_run_config.clone()),
        })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save config: {e}"),
        )),
    }
}

pub fn now_ms_proj() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `GET /console/api/projects/{name}/files` — the recursive file tree.
pub fn project_files(name: &str) -> ApiResult {
    match projects::file_tree(name) {
        Some(tree) => Ok(serde_json::json!({ "files": tree })),
        None => Err((StatusCode::NOT_FOUND, "no such project".to_string())),
    }
}

/// `GET /console/api/projects/{name}/file?path=...` — read a file.
///
/// Text files are returned verbatim as `text/plain`. Binary files are *not*
/// streamed back (the editor cannot render them): instead the body is a small
/// JSON descriptor `{ absPath, size }` so the UI can show a placeholder. Every
/// response carries `X-File-Binary` (`true`/`false`) and `X-File-Size` (bytes).
async fn project_file_get(Path(name): Path<String>, Query(q): Query<FilePathQuery>) -> Response {
    let Some(path) = projects::safe_project_path(&name, &q.path) else {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    };
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(_) => return (StatusCode::NOT_FOUND, "no such file").into_response(),
    };
    let size = bytes.len();
    // A file is treated as binary if it contains a NUL byte or is not valid
    // UTF-8 — the same heuristic git uses for "is this text?".
    let text = match std::str::from_utf8(&bytes) {
        Ok(s) if !s.contains('\u{0}') => Some(s.to_owned()),
        _ => None,
    };
    match text {
        Some(s) => (
            StatusCode::OK,
            [
                (
                    header::CONTENT_TYPE,
                    "text/plain; charset=utf-8".to_string(),
                ),
                ("x-file-binary".parse().unwrap(), "false".to_string()),
                ("x-file-size".parse().unwrap(), size.to_string()),
            ],
            s,
        )
            .into_response(),
        None => {
            let abs = std::fs::canonicalize(&path)
                .unwrap_or(path)
                .to_string_lossy()
                .into_owned();
            let body = serde_json::json!({ "absPath": abs, "size": size }).to_string();
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/json".to_string()),
                    ("x-file-binary".parse().unwrap(), "true".to_string()),
                    ("x-file-size".parse().unwrap(), size.to_string()),
                ],
                body,
            )
                .into_response()
        }
    }
}

/// `GET /console/api/fs/browse?path=...` — list the immediate sub-directories of
/// an absolute host path so the Import-by-reference picker (ADR 0041) can browse
/// to a checked-out Urban app instead of requiring a hand-typed absolute path.
/// With no `path`, opens on the operator's home directory.
///
async fn fs_browse(
    ConnectInfo(peer): ConnectInfo<nano_server_net::PeerAddr>,
    headers: HeaderMap,
    Query(q): Query<BrowseQuery>,
) -> Response {
    if !request_is_loopback(&peer, &headers) {
        return (
            StatusCode::FORBIDDEN,
            "filesystem browsing is available on localhost only",
        )
            .into_response();
    }
    match projects::browse_dir(q.path.as_deref()) {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// `GET /console/api/config/terminal` — the integrated terminal's enablement
/// (`enabled`, env `locked`, whether this caller is `local`, and the `source`).
/// Ungated: it only reports state, and the pty upgrade is gated regardless.
async fn config_terminal(
    ConnectInfo(peer): ConnectInfo<nano_server_net::PeerAddr>,
    headers: HeaderMap,
) -> Response {
    let local = request_is_loopback(&peer, &headers);
    Json(terminal_settings::status_json(local)).into_response()
}

#[derive(Deserialize)]
struct TerminalToggle {
    enabled: bool,
}

/// `PUT /console/api/config/terminal` — enable/disable the terminal and persist
/// it. Loopback-gated (turning on a shell is local-operator-only); `409` when
/// `NANO_CONSOLE_TERMINAL` has locked it off.
async fn config_terminal_set(
    ConnectInfo(peer): ConnectInfo<nano_server_net::PeerAddr>,
    headers: HeaderMap,
    Json(body): Json<TerminalToggle>,
) -> Response {
    if !request_is_loopback(&peer, &headers) {
        return (
            StatusCode::FORBIDDEN,
            "the integrated terminal can only be configured on localhost",
        )
            .into_response();
    }
    match terminal_settings::gate().map(|g| g.set(body.enabled)) {
        Some(Ok(())) => Json(terminal_settings::status_json(true)).into_response(),
        Some(Err(_)) => (
            StatusCode::CONFLICT,
            "the integrated terminal is locked off by NANO_CONSOLE_TERMINAL and \
             cannot be enabled from the console",
        )
            .into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "terminal settings are not initialised",
        )
            .into_response(),
    }
}

/// True when a raw authority (`host`, `host:port`, `[v6]`, or `[v6]:port`)
/// names a loopback host. Shared by the `Host` and `Origin` checks so their
/// parsing can never drift.
fn authority_is_loopback(authority: &str) -> bool {
    let host_name = if let Some(rest) = authority.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else if authority.matches(':').count() > 1 {
        authority
    } else {
        authority.split(':').next().unwrap_or("")
    };
    matches!(host_name, "localhost" | "127.0.0.1" | "::1")
}

/// Single source of truth for the console's "local machine only" gate. All of:
///   1. the request arrives from a loopback peer (the socket is local),
///   2. it carries a loopback `Host` header (blocks a remote page pointing a
///      victim's browser at `http://localhost:<port>` via DNS-rebinding), and
///   3. if an `Origin` header is present, it is a loopback origin.
///
/// The `Origin` check closes Cross-Site WebSocket Hijacking / cross-site fetch:
/// a page on `https://evil.example` can open `ws://localhost:<port>/…` in the
/// victim's browser, which satisfies the peer-IP and `Host` gates, but the
/// browser stamps its own (non-loopback) `Origin` on the request. Non-browser
/// clients (curl, our tooling) send no `Origin` and stay allowed. Reused by
/// every endpoint that touches the operator's machine directly (filesystem
/// browsing, the integrated terminal) so the policy can never drift.
pub fn request_is_loopback(peer: &nano_server_net::PeerAddr, headers: &HeaderMap) -> bool {
    if !peer.0.ip().is_loopback() {
        return false;
    }
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !authority_is_loopback(host) {
        return false;
    }
    if let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        // Strip the scheme; `null` (opaque origins) and any remote host fail
        // the loopback check and are rejected.
        let authority = origin.split_once("://").map(|(_, a)| a).unwrap_or(origin);
        if !authority_is_loopback(authority) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod loopback_gate_tests {
    use std::net::SocketAddr;

    use super::*;

    fn req(peer_ip: &str, host: &str, origin: Option<&str>) -> bool {
        let peer = nano_server_net::PeerAddr(SocketAddr::new(peer_ip.parse().unwrap(), 12345));
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, host.parse().unwrap());
        if let Some(o) = origin {
            headers.insert(header::ORIGIN, o.parse().unwrap());
        }
        request_is_loopback(&peer, &headers)
    }

    #[test]
    fn allows_loopback_peer_host_and_no_origin() {
        // Non-browser clients (curl, our own tooling) send no Origin.
        assert!(req("127.0.0.1", "localhost:8080", None));
        assert!(req("127.0.0.1", "127.0.0.1:8080", None));
        assert!(req("::1", "[::1]:8080", None));
    }

    #[test]
    fn allows_loopback_origin() {
        assert!(req(
            "127.0.0.1",
            "localhost:5173",
            Some("http://localhost:5173")
        ));
        assert!(req(
            "127.0.0.1",
            "127.0.0.1",
            Some("https://127.0.0.1:8443")
        ));
    }

    #[test]
    fn rejects_non_loopback_peer() {
        assert!(!req("203.0.113.7", "localhost:8080", None));
    }

    #[test]
    fn rejects_non_loopback_host() {
        assert!(!req("127.0.0.1", "evil.example", None));
    }

    #[test]
    fn rejects_cross_site_origin_cswsh() {
        // The CSWSH case: a page on evil.example opens ws://localhost from the
        // victim's browser — peer + Host look local, but the browser stamps its
        // real Origin, which must be rejected.
        assert!(!req(
            "127.0.0.1",
            "localhost:8080",
            Some("https://evil.example")
        ));
        assert!(!req(
            "127.0.0.1",
            "localhost:8080",
            Some("http://attacker.localhost.evil.com")
        ));
        // Opaque origins (sandboxed iframe, file://) present as "null".
        assert!(!req("127.0.0.1", "localhost:8080", Some("null")));
    }
}

#[cfg(test)]
mod app_view_proxy_tests {
    use super::{
        app_view_rewrite_location, app_view_strip_request_header, app_view_strip_response_header,
    };

    #[test]
    fn strips_hop_by_hop_but_forwards_accept_encoding_requests() {
        for h in [
            "host",
            "connection",
            "content-length",
            "upgrade",
            "transfer-encoding",
        ] {
            assert!(app_view_strip_request_header(h), "should strip {h}");
        }
        // accept-encoding is forwarded (relayed content-encoding stays coherent).
        for h in [
            "cookie",
            "authorization",
            "content-type",
            "accept",
            "accept-encoding",
        ] {
            assert!(!app_view_strip_request_header(h), "should forward {h}");
        }
    }

    #[test]
    fn strips_framing_guards_and_length_from_responses() {
        for h in [
            "x-frame-options",
            "content-security-policy",
            "content-security-policy-report-only",
            "service-worker-allowed",
            "content-length",
            "connection",
        ] {
            assert!(app_view_strip_response_header(h), "should strip {h}");
        }
        // content-encoding is relayed (reqwest doesn't decode; bytes still match).
        for h in [
            "content-type",
            "set-cookie",
            "location",
            "etag",
            "vary",
            "content-encoding",
        ] {
            assert!(!app_view_strip_response_header(h), "should keep {h}");
        }
    }

    #[test]
    fn rewrites_root_relative_and_loopback_absolute_locations() {
        assert_eq!(
            app_view_rewrite_location("/login", "acme", 3000),
            "/console/app-view/acme/login"
        );
        assert_eq!(
            app_view_rewrite_location("http://127.0.0.1:3000/next", "acme", 3000),
            "/console/app-view/acme/next"
        );
        assert_eq!(
            app_view_rewrite_location("http://localhost:3000/", "acme", 3000),
            "/console/app-view/acme/"
        );
        // Bare origin (no path) still lands on the namespaced root.
        assert_eq!(
            app_view_rewrite_location("http://127.0.0.1:3000", "acme", 3000),
            "/console/app-view/acme/"
        );
    }

    #[test]
    fn passes_through_foreign_and_protocol_relative_locations() {
        // Cross-host absolute: leave alone.
        assert_eq!(
            app_view_rewrite_location("https://accounts.google.com/o", "acme", 3000),
            "https://accounts.google.com/o"
        );
        // Different loopback port: not ours, leave alone.
        assert_eq!(
            app_view_rewrite_location("http://127.0.0.1:9999/x", "acme", 3000),
            "http://127.0.0.1:9999/x"
        );
        // Protocol-relative (`//host/...`) must NOT be treated as root-relative.
        assert_eq!(
            app_view_rewrite_location("//evil.example/x", "acme", 3000),
            "//evil.example/x"
        );
    }
}

// Transparent WebSocket tunneling through the app-view proxy (ADR 0057 §3,
// issue #1054). These drive the real routes end-to-end: a WS client → the
// console proxy → a mock upstream app WS, with the global supervisor seeded to
// report the mock as the "running" app's UI port.
#[cfg(test)]
mod app_view_ws_tests {
    // Each test serializes on a std Mutex guard (`glock`) held across `.await`
    // and a few return the large tungstenite handshake error — both are benign
    // here and mirror the existing supervisor-test pattern.
    #![allow(clippy::await_holding_lock)]
    #![allow(clippy::result_large_err)]

    use std::sync::{Mutex, MutexGuard, OnceLock};

    use axum::Router;
    use axum::routing::any;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    use super::{app_view_proxy, app_view_proxy_index};

    // The proxy resolves the upstream through the *global* supervisor, so these
    // tests share process-wide state. Serialize them; each still uses a distinct
    // project name so their supervisor entries never collide.
    fn glock() -> MutexGuard<'static, ()> {
        static L: OnceLock<Mutex<()>> = OnceLock::new();
        L.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Bring up the app-view proxy routes on an ephemeral loopback port.
    async fn start_console() -> u16 {
        let app = Router::new()
            .route("/console/app-view/{name}/", any(app_view_proxy_index))
            .route("/console/app-view/{name}/{*rest}", any(app_view_proxy));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        port
    }

    /// A mock upstream app: negotiates a subprotocol (echoes the first the
    /// client offered) and echoes every data frame. When it observes the
    /// client's Close it signals `on_close` (if provided) so a test can assert
    /// browser→upstream close propagation. Returns the bound port.
    async fn start_echo_upstream(on_close: Option<mpsc::UnboundedSender<()>>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let on_close = on_close.clone();
                tokio::spawn(async move {
                    let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                                    mut resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        if let Some(proto) = req
                            .headers()
                            .get("sec-websocket-protocol")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.split(',').next())
                            .map(|s| s.trim().to_owned())
                            && let Ok(hv) = HeaderValue::from_str(&proto)
                        {
                            resp.headers_mut().insert("sec-websocket-protocol", hv);
                        }
                        Ok(resp)
                    };
                    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await
                    else {
                        return;
                    };
                    while let Some(Ok(msg)) = ws.next().await {
                        match msg {
                            WsMessage::Text(_) | WsMessage::Binary(_) => {
                                if ws.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            WsMessage::Close(_) => {
                                if let Some(tx) = &on_close {
                                    let _ = tx.send(());
                                }
                                break;
                            }
                            _ => {}
                        }
                    }
                });
            }
        });
        port
    }

    /// A mock upstream that captures the `Cookie`/`Authorization` headers it
    /// receives on the WS handshake and reports them back through `tx`, so a
    /// test can assert the tunnel forwards the browser's end-to-end auth
    /// context. Accepts the upgrade but exchanges no frames.
    #[allow(clippy::type_complexity)]
    async fn start_auth_capturing_upstream(
        tx: mpsc::UnboundedSender<(Option<String>, Option<String>)>,
    ) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                                    resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        let get = |name: &str| {
                            req.headers()
                                .get(name)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned)
                        };
                        let _ = tx.send((get("cookie"), get("authorization")));
                        Ok(resp)
                    };
                    let _ = tokio_tungstenite::accept_hdr_async(stream, callback).await;
                });
            }
        });
        port
    }

    /// A mock upstream that, on connect, sends one Text frame then closes — to
    /// assert upstream→browser close propagation.
    async fn start_closing_upstream() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };
                    let _ = ws.send(WsMessage::text("hello")).await;
                    let _ = ws.send(WsMessage::Close(None)).await;
                });
            }
        });
        port
    }

    /// Open a browser-side WS to the console proxy for project `name`/`path`.
    async fn connect(
        console_port: u16,
        name: &str,
        path: &str,
        subprotocols: &[&str],
    ) -> Result<
        (
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            tokio_tungstenite::tungstenite::handshake::client::Response,
        ),
        tokio_tungstenite::tungstenite::Error,
    > {
        let url = format!("ws://127.0.0.1:{console_port}/console/app-view/{name}/{path}");
        let mut request = url.into_client_request().unwrap();
        if !subprotocols.is_empty() {
            request.headers_mut().insert(
                "sec-websocket-protocol",
                HeaderValue::from_str(&subprotocols.join(", ")).unwrap(),
            );
        }
        tokio_tungstenite::connect_async(request).await
    }

    #[tokio::test]
    async fn upgrade_happy_path_round_trips_frames_and_negotiates_subprotocol() {
        let _g = glock();
        let name = "ws_happy";
        let upstream = start_echo_upstream(None).await;
        super::projects::supervisor()
            .test_force_running(name, upstream)
            .await;
        let console = start_console().await;

        let (mut ws, resp) = connect(console, name, "agentic", &["v1.nano", "other"])
            .await
            .expect("handshake should succeed");
        // The app-selected subprotocol is echoed back to the browser.
        assert_eq!(
            resp.headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok()),
            Some("v1.nano"),
        );

        // Text round-trips.
        ws.send(WsMessage::text("ping")).await.unwrap();
        match ws.next().await {
            Some(Ok(WsMessage::Text(t))) => assert_eq!(t.as_str(), "ping"),
            other => panic!("expected echoed text, got {other:?}"),
        }
        // Binary round-trips.
        ws.send(WsMessage::binary(vec![1u8, 2, 3])).await.unwrap();
        match ws.next().await {
            Some(Ok(WsMessage::Binary(b))) => assert_eq!(&b[..], &[1, 2, 3]),
            other => panic!("expected echoed binary, got {other:?}"),
        }

        super::projects::supervisor().test_force_stopped(name).await;
    }

    #[tokio::test]
    async fn upstream_close_reaches_the_browser() {
        let _g = glock();
        let name = "ws_upclose";
        let upstream = start_closing_upstream().await;
        super::projects::supervisor()
            .test_force_running(name, upstream)
            .await;
        let console = start_console().await;

        let (mut ws, _) = connect(console, name, "agentic", &[]).await.unwrap();
        // First the data frame, then a clean close propagated from upstream.
        assert!(matches!(ws.next().await, Some(Ok(WsMessage::Text(_)))));
        let mut saw_close = matches!(ws.next().await, Some(Ok(WsMessage::Close(_))) | None);
        if !saw_close {
            saw_close = matches!(ws.next().await, Some(Ok(WsMessage::Close(_))) | None);
        }
        assert!(saw_close, "upstream close should reach the browser");

        super::projects::supervisor().test_force_stopped(name).await;
    }

    #[tokio::test]
    async fn browser_close_reaches_the_upstream() {
        let _g = glock();
        let name = "ws_downclose";
        let (tx, mut rx) = mpsc::unbounded_channel();
        let upstream = start_echo_upstream(Some(tx)).await;
        super::projects::supervisor()
            .test_force_running(name, upstream)
            .await;
        let console = start_console().await;

        let (mut ws, _) = connect(console, name, "agentic", &[]).await.unwrap();
        ws.send(WsMessage::Close(None)).await.unwrap();
        // Drain so the close handshake completes.
        while ws.next().await.transpose().ok().flatten().is_some() {}

        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .ok()
            .flatten();
        assert!(
            observed.is_some(),
            "browser close should reach the upstream"
        );

        super::projects::supervisor().test_force_stopped(name).await;
    }

    #[tokio::test]
    async fn forwards_end_to_end_auth_headers_upstream() {
        let _g = glock();
        let name = "ws_auth";
        let (tx, mut rx) = mpsc::unbounded_channel();
        let upstream = start_auth_capturing_upstream(tx).await;
        super::projects::supervisor()
            .test_force_running(name, upstream)
            .await;
        let console = start_console().await;

        // A real browser sends `Cookie` automatically and cannot set
        // `Authorization` on a WS handshake; tungstenite lets us set both so we
        // can assert the tunnel forwards whatever end-to-end auth arrived.
        let url = format!("ws://127.0.0.1:{console}/console/app-view/{name}/agentic");
        let mut request = url.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("cookie", HeaderValue::from_static("session=abc123"));
        request
            .headers_mut()
            .insert("authorization", HeaderValue::from_static("Bearer tok-42"));
        let (_ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .expect("handshake should succeed");

        let (cookie, authorization) =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
                .await
                .ok()
                .flatten()
                .expect("upstream should observe the handshake");
        assert_eq!(cookie.as_deref(), Some("session=abc123"));
        assert_eq!(authorization.as_deref(), Some("Bearer tok-42"));

        super::projects::supervisor().test_force_stopped(name).await;
    }

    #[tokio::test]
    async fn upgrade_path_503_when_app_not_running() {
        let _g = glock();
        let console = start_console().await;
        // A name that was never marked running resolves to not-running → 503.
        match connect(console, "ws_stopped", "agentic", &[]).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status().as_u16(), 503);
            }
            other => panic!("expected 503 Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn upgrade_path_404_when_app_is_headless() {
        let _g = glock();
        let name = "ws_headless";
        // Running, but no reachable UI port (detected port 0) → headless → 404.
        super::projects::supervisor()
            .test_force_running(name, 0)
            .await;
        let console = start_console().await;
        match connect(console, name, "agentic", &[]).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status().as_u16(), 404);
            }
            other => panic!("expected 404 Http error, got {other:?}"),
        }
        super::projects::supervisor().test_force_stopped(name).await;
    }

    #[tokio::test]
    async fn upgrade_path_400_on_unsafe_name() {
        let _g = glock();
        let console = start_console().await;
        // `%24bad` decodes to `$bad`, which `is_safe_name` rejects → 400.
        match connect(console, "%24bad", "agentic", &[]).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status().as_u16(), 400);
            }
            other => panic!("expected 400 Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_websocket_upgrade_token_still_501() {
        let _g = glock();
        let name = "ws_h2c";
        // Running with a real port so the 501 is decided by the upgrade token,
        // not by a guard firing first.
        let upstream = start_echo_upstream(None).await;
        super::projects::supervisor()
            .test_force_running(name, upstream)
            .await;
        let console = start_console().await;

        // Raw HTTP GET with `Upgrade: h2c` — a non-WebSocket upgrade.
        let raw = tokio::net::TcpStream::connect(("127.0.0.1", console))
            .await
            .unwrap();
        let (mut rd, mut wr) = raw.into_split();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let req = format!(
            "GET /console/app-view/{name}/agentic HTTP/1.1\r\n\
             Host: 127.0.0.1\r\n\
             Connection: Upgrade\r\n\
             Upgrade: h2c\r\n\
             \r\n"
        );
        wr.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        // Read the status line + headers.
        let mut tmp = [0u8; 1024];
        loop {
            let n = rd.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let head = String::from_utf8_lossy(&buf);
        assert!(
            head.starts_with("HTTP/1.1 501"),
            "non-WebSocket Upgrade must still 501, got: {head:?}"
        );

        super::projects::supervisor().test_force_stopped(name).await;
    }

    #[tokio::test]
    async fn malformed_websocket_handshake_is_400() {
        let _g = glock();
        let name = "ws_malformed";
        // Real port so the status is decided by the (malformed) handshake, not
        // by a guard firing first.
        let upstream = start_echo_upstream(None).await;
        super::projects::supervisor()
            .test_force_running(name, upstream)
            .await;
        let console = start_console().await;

        // `Upgrade: websocket` but WITHOUT the mandatory `Sec-WebSocket-Key` /
        // `Sec-WebSocket-Version` headers → `WebSocketUpgrade` extraction fails,
        // so this is a malformed WS handshake, which must be a 400 (client
        // error) rather than a 501 (non-WS upgrade token).
        let raw = tokio::net::TcpStream::connect(("127.0.0.1", console))
            .await
            .unwrap();
        let (mut rd, mut wr) = raw.into_split();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let req = format!(
            "GET /console/app-view/{name}/agentic HTTP/1.1\r\n\
             Host: 127.0.0.1\r\n\
             Connection: Upgrade\r\n\
             Upgrade: websocket\r\n\
             \r\n"
        );
        wr.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = rd.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let head = String::from_utf8_lossy(&buf);
        assert!(
            head.starts_with("HTTP/1.1 400"),
            "malformed WebSocket handshake must be 400, got: {head:?}"
        );

        super::projects::supervisor().test_force_stopped(name).await;
    }
}

#[cfg(test)]
mod app_view_icon_tests {
    use std::path::Path;

    use super::{app_view_icon_content_type, app_view_icon_is_asset};

    #[test]
    fn asset_paths_vs_bundled_glyph_names() {
        // Asset paths: a separator or a file extension.
        for icon in [
            "assets/icon.svg",
            "icon.png",
            "brand/logo.webp",
            "a/b/c.ico",
        ] {
            assert!(app_view_icon_is_asset(icon), "{icon} should be an asset");
        }
        // Bundled glyph names: a bare token, no separator, no extension.
        for icon in ["workers", "docs", "explorer", "appDefault"] {
            assert!(!app_view_icon_is_asset(icon), "{icon} should be a glyph");
        }
        // Dotfiles have no extension (matches JS + Rust `Path::extension`): a
        // bare ".svg" is not treated as an asset, so client and server agree.
        assert!(
            !app_view_icon_is_asset(".svg"),
            ".svg is a dotfile, not an asset"
        );
    }

    #[test]
    fn only_allow_listed_image_types_resolve_a_content_type() {
        assert_eq!(
            app_view_icon_content_type(Path::new("a/icon.svg")),
            Some("image/svg+xml")
        );
        assert_eq!(
            app_view_icon_content_type(Path::new("ICON.PNG")),
            Some("image/png")
        );
        assert_eq!(
            app_view_icon_content_type(Path::new("logo.jpeg")),
            Some("image/jpeg")
        );
        // Non-image / dangerous extensions are refused (no content-type ⇒ 404),
        // so this route can never be used to exfiltrate a project's source.
        for p in ["worker.ts", "app.db", "secret.env", "noext", "icon.html"] {
            assert_eq!(app_view_icon_content_type(Path::new(p)), None, "{p}");
        }
    }
}

pub fn project_file_save(name: &str, rel: &str, body: &str) -> ApiResult {
    let Some(path) = projects::safe_project_path(name, rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match std::fs::write(&path, body) {
        Ok(()) => {
            // Saving the manifest can repoint the project's default datasource
            // (`data.default` / `data.sources`), swapping the DB out from under
            // the memoised "inbox ensured" marker (#1340). Drop it so the next
            // `ensure_inbox` re-creates `trigger_inbox` on the fresh datasource
            // instead of skipping the `CREATE` and failing on a missing table.
            if rel == "nano.app.json" {
                triggers::forget_inbox(name);
            }
            Ok(serde_json::Value::Null)
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save file: {e}"),
        )),
    }
}

/// `POST /console/api/projects/{name}/file` — create an empty file or a folder.
pub fn project_path_create(name: &str, rel: &str, dir: bool) -> ApiResult {
    let Some(path) = projects::safe_project_path(name, rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    if path.exists() {
        return Err((StatusCode::CONFLICT, "that path already exists".to_string()));
    }
    let res = if dir {
        std::fs::create_dir_all(&path)
    } else {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // Seed a valid minimal document for known resource kinds so the file is
        // deployable/openable immediately; unknown kinds (source files) stay
        // empty as before.
        let content = projects::starter_file_content(rel).unwrap_or_default();
        std::fs::write(&path, content)
    };
    match res {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not create: {e}"),
        )),
    }
}

#[cfg(test)]
mod inbox_memo_invalidation_tests {
    //! Regression tests for the trigger-inbox memo (#1340): the memo is keyed by
    //! project *name*, but a name does not identify a stable datasource — rename
    //! and datasource-reconfig swap the DB under a reused name. Every lifecycle
    //! transition that changes the datasource must invalidate the memo, or a
    //! fresh datasource skips the `CREATE` and fails on a missing
    //! `trigger_inbox` table.
    use super::*;

    fn temp_root() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering as AOrd};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "nano-lib-inbox-{}-{}",
            std::process::id(),
            N.fetch_add(1, AOrd::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        unsafe {
            std::env::set_var("NANOBPMN_PROJECTS_DIR", &p);
        }
        p
    }

    #[tokio::test]
    async fn rename_invalidates_the_old_names_inbox_memo() {
        let _root = temp_root();
        projects::create_project("renameme", "", "starter").unwrap();
        // Simulate a previously-ensured inbox under the old name.
        triggers::ensure_inbox_memo_for_test("renameme");
        assert!(triggers::inbox_is_memoised("renameme"));

        project_rename("renameme", "renamed").await.unwrap();
        assert!(
            !triggers::inbox_is_memoised("renameme"),
            "renaming a project drops the old name's inbox memo so a fresh \
             project reusing the name re-creates its table"
        );
    }

    #[test]
    fn saving_the_manifest_invalidates_the_inbox_memo() {
        let _root = temp_root();
        projects::create_project("cfgsave", "", "starter").unwrap();
        triggers::ensure_inbox_memo_for_test("cfgsave");
        assert!(triggers::inbox_is_memoised("cfgsave"));

        // A `nano.app.json` save can repoint the default datasource.
        project_file_save("cfgsave", "nano.app.json", "{ \"data\": {} }").unwrap();
        assert!(
            !triggers::inbox_is_memoised("cfgsave"),
            "saving nano.app.json drops the inbox memo so the next ensure \
             re-creates the table on the fresh datasource"
        );

        // An unrelated file save must NOT invalidate the memo.
        triggers::ensure_inbox_memo_for_test("cfgsave");
        project_file_save("cfgsave", "README.md", "hi").unwrap();
        assert!(
            triggers::inbox_is_memoised("cfgsave"),
            "a non-manifest save leaves the inbox memo intact"
        );
    }
}

/// Maps a datasource gateway error to an HTTP status + message.
fn data_error(e: projects::DataError) -> (StatusCode, String) {
    use projects::DataError::*;
    match e {
        NoProject => (StatusCode::NOT_FOUND, "no such project".to_string()),
        NoRuntime => (
            StatusCode::SERVICE_UNAVAILABLE,
            "No JavaScript runtime found for the Data panel. Install Node >= 22.6 \
             (the npm launcher provides one), or Deno (https://deno.com)."
                .to_string(),
        ),
        // A bad SQL statement / unknown source / missing manifest is the
        // maker's input, not a server fault.
        Op(m) => (StatusCode::BAD_REQUEST, m),
        Gateway(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
    }
}

/// Run one datasource op and surface it as an `ApiResult`.
async fn project_data_op(name: &str, request: serde_json::Value) -> ApiResult {
    projects::run_data_op(name, request)
        .await
        .map_err(data_error)
}

/// True if `sql` is a schema-changing (DDL) statement, so the domain types must
/// be regenerated. A cheap leading-keyword sniff — enough to avoid regenerating
/// on every row INSERT/UPDATE/DELETE while catching CREATE/ALTER/DROP TABLE.
fn sql_is_ddl(sql: &str) -> bool {
    let s = sql.trim_start().to_ascii_uppercase();
    s.starts_with("CREATE ") || s.starts_with("ALTER ") || s.starts_with("DROP ")
}

/// Which regeneration path a project takes, decided purely from its shape and
/// toolkit availability so the routing invariant is unit-testable in isolation.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
enum RegenPath {
    /// Urban-shaped app: `urban gen` is the sole, authoritative deriver.
    UrbanGen,
    /// Urban-shaped app whose toolkit is unavailable — skip and let the Studio
    /// surface the install prompt (#524). Crucially **not** the embedded path:
    /// ADR 0053 forbids a second deriver for an Urban app.
    SkipUrbanUnavailable,
    /// Legacy-shaped app (no `nano.app.json`): the console's embedded emitter.
    Embedded,
}

/// The ADR-0053 "one deriver" routing rule, factored out as a pure function: an
/// Urban-shaped app derives **only** through `urban gen` and never falls back to
/// the embedded emitter — regardless of toolkit availability — so the console can
/// never run a second, divergent deriver against it.
fn regen_path(is_urban: bool, urban_available: bool) -> RegenPath {
    match (is_urban, urban_available) {
        (true, true) => RegenPath::UrbanGen,
        (true, false) => RegenPath::SkipUrbanUnavailable,
        (false, _) => RegenPath::Embedded,
    }
}

/// Best-effort regeneration of a project's derived type artifacts after a
/// structural change, so typed workers track the current shape (ADR 0029 §4.1/§6).
/// The path depends on the project shape: an **Urban-shaped app** (`nano.app.json`)
/// delegates to `urban gen` (the sole, authoritative deriver of the full
/// `nano-generated/*` artifact set — no embedded fallback, #514 dry-out); a
/// legacy-shaped project regenerates `domain-rows.d.ts` from the default
/// datasource's live schema via the embedded emitter. Failure is logged, never
/// surfaced — the maker's operation already succeeded and the types are an
/// authoring-time contract only.
pub async fn regenerate_domain_types(name: &str) {
    // #514 dry-out (ADR 0052/0053/0054): an Urban-shaped app (`nano.app.json`)
    // delegates artifact generation to the shared `@nanobpm/urban` toolkit
    // (`urban gen`) — the manifest is the single contract and urban is the *one*
    // deriver. `urban gen` is **authoritative**: there is no embedded fallback,
    // because the embedded emitter writes the legacy vendored-runtime shape, so
    // falling back to it would re-introduce the very parallel implementation ADR
    // 0053 exists to eliminate (a second deriver producing a divergent artifact
    // set). Regeneration is a best-effort, authoring-time convenience (the
    // artifacts are always regenerable), so skipping on an unreachable toolkit is
    // safe — the Studio already surfaces an install prompt (#524).
    //
    // #776: resolve the toolkit availability **project-scoped** so an app whose
    // `@nanobpm/urban` lives only in its own `node_modules/.bin/urban` still
    // routes to `UrbanGen` (the host-global `urban_available()` never saw the
    // project-local install), while the ADR-0053 no-embedded-fallback routing
    // is preserved. Resolve the project directory once and reuse it for the
    // Urban-shape check and the availability probe, so we don't repeat
    // `project_dir`/`read_project_ref` I/O (or drift if the ref changes between
    // the two calls).
    let (is_urban, urban_available) = projects::project_dir(name)
        .map(|d| {
            (
                d.join("nano.app.json").is_file(),
                urban::urban_available_for(&d),
            )
        })
        .unwrap_or((false, false));
    match regen_path(is_urban, urban_available) {
        RegenPath::UrbanGen => {
            if let Err(msg) = projects::gen_via_urban(name).await {
                tracing::debug!(project = name, error = %msg, "urban gen failed");
            }
        }
        RegenPath::SkipUrbanUnavailable => {
            tracing::debug!(
                project = name,
                "urban gen skipped: urban CLI unavailable (Studio surfaces the install prompt)"
            );
        }
        // Legacy-shaped projects keep the embedded emitter until they are migrated
        // to the Urban shape (#522 follow-ups).
        RegenPath::Embedded => {
            if let Err((_, msg)) =
                project_data_op(name, serde_json::json!({ "op": "domaintypes" })).await
            {
                tracing::debug!(project = name, "domain-rows.d.ts regen skipped: {msg}");
            }
        }
    }
}

#[cfg(test)]
mod regen_path_tests {
    use super::{RegenPath, regen_path};

    /// The ADR-0053 invariant, as a truth table: an Urban-shaped app derives only
    /// through `urban gen` and **never** routes to the embedded emitter — even
    /// when the toolkit is unavailable it skips rather than falling back, so the
    /// console can never run a second deriver against an Urban app.
    #[test]
    fn urban_app_never_routes_to_embedded() {
        assert_eq!(regen_path(true, true), RegenPath::UrbanGen);
        assert_eq!(regen_path(true, false), RegenPath::SkipUrbanUnavailable);
        // The load-bearing assertion: neither Urban branch is `Embedded`.
        assert_ne!(regen_path(true, true), RegenPath::Embedded);
        assert_ne!(regen_path(true, false), RegenPath::Embedded);
    }

    /// A legacy-shaped project (no `nano.app.json`) always uses the embedded
    /// emitter, independent of whether the Urban toolkit happens to be installed.
    #[test]
    fn legacy_app_always_uses_embedded() {
        assert_eq!(regen_path(false, true), RegenPath::Embedded);
        assert_eq!(regen_path(false, false), RegenPath::Embedded);
    }
}

/// Whether a saved project file is a process model that carries the worker/message
/// I/O + custom-header contract the `domaintypes` op derives (ADR 0033 §3). The
/// scan surface is exactly `resources/processes/*.bpmn` (`envelope_scan::
/// scan_project` reads that directory non-recursively), so the regeneration
/// trigger matches it precisely: a `.bpmn` directly under `resources/processes/`
/// (extension case-insensitive). A `.bpmn` saved elsewhere is not scanned, so it
/// must not spuriously retrigger a regeneration that could not reflect it.
pub fn is_model_resource(rel: &str) -> bool {
    // Mirror `safe_project_path`'s leading-slash tolerance so the trigger matches
    // exactly what was saved.
    let path = std::path::Path::new(rel.trim_start_matches('/'));
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("bpmn"))
        && path.parent() == Some(std::path::Path::new("resources/processes"))
}

/// Whether a saved project file is a code-first workflow *source* — a `.ts`
/// directly under `workflows/` — whose save should (re)generate the on-disk BPMN
/// model(s) with diagram layout (ADR 0048). This is the code-first inverse of
/// [`is_model_resource`]: there the authored `.bpmn` is the source of truth and a
/// save re-derives the typed SDK; here the workflow *code* is the source of truth
/// and a save re-generates the `resources/processes/*.bpmn` the SDK derives from.
/// Matched precisely (a `.ts` in `workflows/`, not nested, not the project root)
/// so an unrelated `.ts` save never triggers a Deno round-trip.
pub fn is_workflow_source(rel: &str) -> bool {
    let path = std::path::Path::new(rel.trim_start_matches('/'));
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("ts"))
        && path.parent() == Some(std::path::Path::new("workflows"))
}

/// Best-effort regeneration of a code-first project's on-disk BPMN models (with
/// diagram layout) after a `workflows/*.ts` save (ADR 0048). On success the
/// generated `.bpmn` land on the model-first scan surface, so we then refresh the
/// derived domain/worker types from them — mirroring the `is_model_resource`
/// path. Failure is logged, never surfaced: the save already succeeded and the
/// models are a derived, always-regenerable artifact.
pub async fn regenerate_workflow_models(name: &str) {
    match projects::generate_models(name).await {
        Ok(ids) if !ids.is_empty() => {
            tracing::debug!(
                project = name,
                count = ids.len(),
                "regenerated workflow models"
            );
            // The generated `.bpmn` now feed the envelope/domaintypes derivation.
            regenerate_domain_types(name).await;
        }
        Ok(_) => {}
        Err(msg) => tracing::debug!(project = name, "workflow model regen skipped: {msg}"),
    }
}

/// `GET /console/api/projects/{name}/data/sources` — the datasources the App
/// manifest declares (resolved driver/url) plus the default source name.
pub async fn project_data_sources(name: &str) -> ApiResult {
    project_data_op(name, serde_json::json!({ "op": "sources" })).await
}

/// `GET /console/api/projects/{name}/data/{source}/schema` — tables/columns/indexes.
pub async fn project_data_schema(name: &str, source: &str) -> ApiResult {
    project_data_op(
        name,
        serde_json::json!({ "op": "schema", "source": source }),
    )
    .await
}

/// `POST /console/api/projects/{name}/data/{source}/query` — run a row-returning
/// statement, returning `{ columns, rows }`.
pub async fn project_data_query(
    name: &str,
    source: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> ApiResult {
    project_data_op(
        name,
        serde_json::json!({ "op": "query", "source": source, "sql": sql, "params": params }),
    )
    .await
}

/// `POST /console/api/projects/{name}/data/{source}/exec` — run a non-row
/// statement (INSERT/UPDATE/DELETE/DDL), returning `{ changed, lastInsertId? }`.
pub async fn project_data_exec(
    name: &str,
    source: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> ApiResult {
    let res = project_data_op(
        name,
        serde_json::json!({ "op": "exec", "source": source, "sql": sql, "params": params }),
    )
    .await;
    // A bare `CREATE/ALTER/DROP TABLE` can arrive through exec; refresh the
    // domain types so workers track the new shape (ADR 0029 §4.1/§6).
    if res.is_ok() && sql_is_ddl(sql) {
        regenerate_domain_types(name).await;
    }
    res
}

/// `POST /console/api/projects/{name}/data/{source}/script` — run several
/// statements atomically in one transaction (the structure editor's table
/// rebuild). Returns `{ changed }`.
pub async fn project_data_script(name: &str, source: &str, statements: Vec<String>) -> ApiResult {
    let res = project_data_op(
        name,
        serde_json::json!({ "op": "script", "source": source, "statements": statements }),
    )
    .await;
    // The structure editor's table rebuild runs through `script`, so this is the
    // primary schema-change trigger — refresh the domain types (ADR 0029 §4.1/§6).
    if res.is_ok() {
        regenerate_domain_types(name).await;
    }
    res
}

/// `GET /console/api/projects/{name}/data/{source}/migrations` — the ordered
/// migration files with applied status.
pub async fn project_data_migrations(name: &str, source: &str) -> ApiResult {
    project_data_op(
        name,
        serde_json::json!({ "op": "migrations", "source": source }),
    )
    .await
}

/// `POST /console/api/projects/{name}/data/{source}/migrate` — apply pending
/// migrations, returning the names applied.
pub async fn project_data_migrate(name: &str, source: &str) -> ApiResult {
    let res = project_data_op(
        name,
        serde_json::json!({ "op": "migrate", "source": source }),
    )
    .await;
    // Migrations are DDL by nature — refresh the domain types (ADR 0029 §4.1/§6).
    if res.is_ok() {
        regenerate_domain_types(name).await;
    }
    res
}

/// `POST /console/api/projects/{name}/data/{source}/domaintypes` — the maker's
/// explicit "regenerate now" affordance (ADR 0029 §4.1/§6).
///
/// For a **legacy** project this reifies `source`'s live schema into
/// `nano-generated/domain-rows.d.ts` through the embedded gateway, returning
/// `{ path, text, tables }`.
///
/// For an **Urban-shaped app** (#522 slice b) persisting derivation is
/// `urban gen`'s job — the one authoritative deriver (ADR 0053) — not a second
/// `urban data domaintypes` emit. So we run `urban gen` (which writes the full
/// `nano-generated/*` artifact set from the manifest + models), then read back the
/// `{ text, tables, shapeDiagnostics, migrated }` for the panel via a read-only
/// `urban data` introspection (`write:false`, no second write) and report the
/// path `urban gen` persisted. The editor's cached SDK typings are invalidated
/// client-side either way.
pub async fn project_data_domaintypes(name: &str, source: &str) -> ApiResult {
    // Route to the one deriver (`urban gen`) only when the app is Urban-shaped
    // *and* its toolkit resolves *and* that toolkit actually carries the `data`
    // op. During the transition — before the marketplace pack ships a `urban`
    // carrying the `data`/`gen` ops — an Urban app whose toolkit is unresolved
    // (or too old for `data`) keeps the embedded write path so "Regenerate types"
    // still works; slice c removes this fallback once the pack guarantees urban.
    //
    // The `data`-capability gate matters here, not just `urban_available_for`:
    // the branch runs `urban gen` (persist) and then reads back through
    // `urban data` introspection. An `urban` that has `gen` but predates `data`
    // would otherwise persist via `urban gen` yet fall back to the *embedded*
    // introspection inside `run_data_op` — re-introducing a Node/Deno dependency
    // and contradicting this doc comment. Gating the whole branch on
    // `urban_supports_data` keeps it atomic (all-urban or all-embedded), mirroring
    // the capability gate in `run_data_op`.
    let urban_ready = match projects::project_dir(name) {
        Some(d) if d.join("nano.app.json").is_file() => match urban::find_urban_for(&d) {
            Some(u) => urban::urban_supports_data(&u).await,
            None => false,
        },
        _ => false,
    };
    if urban_ready {
        // Persist via the one deriver. This branch is gated on the toolkit
        // resolving *and* carrying the `data` op, so the toolkit *is* present — a
        // failure here is a real `urban gen` error (invalid manifest/models,
        // toolkit error output), not a dependency outage, so map it to
        // `500 INTERNAL_SERVER_ERROR`
        // rather than `503`. There is no `503 SERVICE_UNAVAILABLE` path here: an
        // unresolved (or too-old-for-`data`) toolkit doesn't error out — the
        // `urban_ready == false` branch falls back to the embedded write path
        // above — so clients never see a transient-outage status for a genuine
        // gen failure.
        projects::gen_via_urban(name)
            .await
            .map_err(|m| (StatusCode::INTERNAL_SERVER_ERROR, m))?;
        // Read-only introspection for the response contract. `write:false` never
        // writes files or migrates the DB, so this is a pure fetch of the emitted
        // text + table count (routed through `urban data`), and the full derived
        // maps are scanned server-side in `run_data_op` (no maps supplied here).
        let mut res = project_data_op(
            name,
            serde_json::json!({ "op": "domaintypes", "source": source, "write": false }),
        )
        .await?;
        // `urban gen` did persist the artifacts, so report the real written path
        // (the `write:false` fetch returns `path: null`).
        if let Some(obj) = res.as_object_mut() {
            obj.insert(
                "path".to_string(),
                serde_json::json!(format!("{}/domain-rows.d.ts", projects::GEN_DIR)),
            );
        }
        return Ok(res);
    }
    project_data_op(
        name,
        serde_json::json!({ "op": "domaintypes", "source": source }),
    )
    .await
}

/// `POST /console/api/projects/{name}/data/{source}/domaintypes/preview` — resolve
/// the composed shapes the modeller is editing (sent in the body) for the shape
/// composer's live field preview + inline diagnostics (ADR 0040 §9/§10). It avoids
/// the saved-model scan (the caller-supplied `derivedShapes` win over the disk scan
/// in `run_data_op`) and, with `write:false`, does not materialise the `domaintypes`
/// outputs (`domain-rows.d.ts` + the worker/message bindings) — so it never
/// regenerates the typed SDK the maker consumes. (It still runs through
/// `run_data_op`, which ensures the `nano-generated/` SDK scaffolding exists.)
pub async fn project_data_preview_domaintypes(
    name: &str,
    source: &str,
    shapes: serde_json::Value,
    meta: Option<serde_json::Value>,
) -> ApiResult {
    let mut request = serde_json::json!({
        "op": "domaintypes",
        "source": source,
        "write": false,
        "derivedShapes": shapes,
        // The preview only needs `text` + `shapeDiagnostics` (both independent
        // of worker/message IO), so supply empty lists: with all derived maps
        // present, `run_data_op` skips the `resources/processes/*.bpmn` scan
        // entirely on this debounced, latency-sensitive path.
        "derivedWorkers": [],
        "derivedMessages": [],
    });
    // The composer edits model-level metadata alongside shapes, so preview the
    // in-editor `nano:meta` too (ADR 0040 §5) — it feeds the resolved fuse/accessor
    // the same way `derivedShapes` do. Injected only when the caller supplied it: an
    // omitted `meta` leaves `derivedMeta` absent so `run_data_op` falls back to the
    // saved-model scan (an explicit `meta: []` still previews an emptied list).
    if let Some(meta) = meta {
        request["derivedMeta"] = meta;
    }
    project_data_op(name, request).await
}

// --- triggers (ADR 0025) --------------------------------------------------

fn trigger_error(e: triggers::TriggerError) -> (StatusCode, String) {
    use triggers::TriggerError::*;
    match e {
        // A datasource failure carries its own HTTP mapping (no source, missing
        // manifest, bad SQL → 400/404/503).
        Data(d) => data_error(d),
        // An unknown trigger / bad manifest is the maker's input.
        Manifest(m) => (StatusCode::BAD_REQUEST, m),
        Feel(m) => (StatusCode::BAD_REQUEST, m),
        Apply(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        Unauthorized(m) => (StatusCode::UNAUTHORIZED, m),
    }
}

/// `POST /console/api/projects/{name}/triggers/enqueue` — the manual/synthetic
/// source (ADR 0025 phase 1): persist an event into the durable inbox, returning
/// `{ enqueued, id? }`. A repeated idempotency key is a no-op (`enqueued=false`).
pub async fn project_trigger_enqueue(
    name: &str,
    trigger_id: &str,
    idempotency_key: Option<String>,
    body: serde_json::Value,
) -> ApiResult {
    triggers::enqueue(name, trigger_id, idempotency_key.as_deref(), &body)
        .await
        .map(|o| serde_json::to_value(o).unwrap_or_default())
        .map_err(trigger_error)
}

/// `POST /console/api/projects/{name}/hooks/{triggerId}` — the webhook ingress
/// (ADR 0025 phase 2). Persists the event into the durable inbox and acks after
/// persist (§2): `202 Accepted` when a new row was enqueued, `200 OK` on a
/// duplicate idempotency key. Auth (when the trigger declares it) comes from an
/// `X-Webhook-Token` header or `Authorization: Bearer <token>`; the optional
/// `Idempotency-Key` header supplies the dedup key.
async fn project_hook(
    Path((name, trigger_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let token = headers
        .get("x-webhook-token")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.strip_prefix("Bearer "))
                .map(str::to_string)
        });
    let idem = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    // Accept any body: parse JSON, else wrap the raw text so nothing is lost.
    let event: serde_json::Value = if body.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&body)
            .unwrap_or_else(|_| serde_json::json!({ "raw": String::from_utf8_lossy(&body) }))
    };
    match triggers::webhook_ingest(
        &name,
        &trigger_id,
        token.as_deref(),
        idem.as_deref(),
        &event,
    )
    .await
    {
        Ok(outcome) => {
            let code = if outcome.enqueued {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            (
                code,
                Json(serde_json::to_value(&outcome).unwrap_or_default()),
            )
                .into_response()
        }
        Err(e) => {
            let (code, msg) = trigger_error(e);
            (code, msg).into_response()
        }
    }
}

/// `GET /console/api/projects/{name}/triggers/inbox` — inbox counts by state
/// plus the most recently updated rows.
pub async fn project_trigger_inbox(name: &str) -> ApiResult {
    triggers::inbox_status(name)
        .await
        .map(|s| serde_json::to_value(s).unwrap_or_default())
        .map_err(trigger_error)
}

/// `GET /console/api/projects/{name}/triggers` — the manifest's declared
/// triggers resolved against the source registry (ADR 0025 phase 2), for the
/// Triggers panel + source picker.
pub async fn project_triggers(name: &str) -> ApiResult {
    triggers::triggers_overview(name)
        .await
        .map_err(trigger_error)
}

pub async fn project_trigger_add(
    name: &str,
    id: &str,
    kind: &str,
    config: &std::collections::BTreeMap<String, String>,
    connection: Option<&str>,
    action: &serde_json::Value,
) -> ApiResult {
    triggers::add_trigger(name, id, kind, config, connection, action).map_err(trigger_error)?;
    triggers::triggers_overview(name)
        .await
        .map_err(trigger_error)
}

/// `GET /console/api/projects/{name}/connectors` — enabled connectors + registry.
pub fn project_connectors(name: &str) -> ApiResult {
    connectors::connectors_overview(name).map_err(trigger_error)
}

/// `POST /console/api/projects/{name}/connectors` — enable a connector, then
/// return the refreshed overview.
pub fn project_connector_add(
    name: &str,
    task_type: &str,
    connection: Option<&str>,
    config: &std::collections::BTreeMap<String, String>,
) -> ApiResult {
    connectors::add_connector(name, task_type, connection, config).map_err(trigger_error)?;
    connectors::connectors_overview(name).map_err(trigger_error)
}

/// `DELETE /console/api/projects/{name}/file?path=...` — remove a file or folder.
pub fn project_path_delete(name: &str, rel: &str) -> ApiResult {
    let Some(path) = projects::safe_project_path(name, rel) else {
        return Err((StatusCode::BAD_REQUEST, "invalid path".to_string()));
    };
    let res = if path.is_dir() {
        std::fs::remove_dir_all(&path)
    } else {
        std::fs::remove_file(&path)
    };
    match res {
        Ok(()) => Ok(serde_json::Value::Null),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err((StatusCode::NOT_FOUND, "no such path".to_string()))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not delete: {e}"),
        )),
    }
}

/// `POST /console/api/projects/{name}/run` — deploy + start the application.
pub async fn project_run(name: &str) -> ApiResult {
    let sup = projects::supervisor();
    match sup.run(name).await {
        Ok(()) => Ok(serde_json::to_value(sup.run_state(name).await).unwrap()),
        Err(e) => Err((StatusCode::BAD_REQUEST, e)),
    }
}

/// `POST /console/api/projects/{name}/stop` — stop the application.
pub async fn project_stop(name: &str) -> ApiResult {
    let sup = projects::supervisor();
    match sup.stop(name).await {
        Ok(()) => Ok(serde_json::to_value(sup.run_state(name).await).unwrap()),
        Err(e) => Err((StatusCode::BAD_REQUEST, e)),
    }
}

/// `POST /console/api/projects/{name}/compile` — compile the project (host or
/// cross-compile). Runs in the background; progress streams over the log SSE.
pub fn project_compile(name: &str, targets: Vec<String>) -> ApiResult {
    if projects::read_config(name).is_none() {
        return Err((StatusCode::NOT_FOUND, "no such project".to_string()));
    }
    let name = name.to_string();
    tokio::spawn(async move {
        let _ = projects::supervisor().compile(&name, &targets).await;
    });
    Ok(serde_json::json!({ "started": true }))
}

/// Query for `project_export`: include compiled `dist/` binaries (default off,
/// since they are large and platform-specific).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportQuery {
    #[serde(default)]
    dist: bool,
}

/// `GET /console/api/projects/{name}/derived-models` — derive the executable
/// BPMN from a code-first workflow project's `workflows/*.ts` (ADR 0045) and
/// return `[{id, kind, xml}]` for the console's read-only viewer. Degrades
/// gracefully: derivation problems surface as an error status, not a panic.
async fn project_derived_models(Path(name): Path<String>) -> Response {
    match projects::derive_models(&name).await {
        Ok(models) => (StatusCode::OK, Json(models)).into_response(),
        Err(e) if e.contains("no such") || e.contains("invalid project") => {
            (StatusCode::NOT_FOUND, e).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// `GET /console/api/projects/{name}/export[?dist=true]` — download the project
/// as a zip. Source-only by default; pass `dist=true` to bundle compiled
/// binaries from `dist/`.
async fn project_export(Path(name): Path<String>, Query(q): Query<ExportQuery>) -> Response {
    // Export hook: refresh the generated domain types so the downloaded zip
    // ships source that types against the current schema + manifest `types`
    // registry (ADR 0029 §6). Best-effort — a failure never blocks the export.
    regenerate_domain_types(&name).await;
    match projects::export_zip(&name, q.dist) {
        Ok(zip) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/zip".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!(
                        "attachment; filename=\"{}\"",
                        projects::export_filename(&name)
                    ),
                ),
                (header::CACHE_CONTROL, "no-store".to_string()),
            ],
            zip,
        )
            .into_response(),
        Err(e) if e.contains("no such") => (StatusCode::NOT_FOUND, e).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// SSE state for a project's run/compile log stream.
enum ProjLogState {
    History(
        std::vec::IntoIter<projects::LogLine>,
        broadcast::Receiver<projects::LogLine>,
    ),
    Live(broadcast::Receiver<projects::LogLine>),
}

/// `GET /console/api/projects/{name}/logs` — SSE stream of run/compile output.
async fn project_logs(
    Path(name): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let sup = projects::supervisor();
    let history = sup.log_history(&name).await;
    let rx = sup.subscribe(&name).await;
    let stream = unfold(
        ProjLogState::History(history.into_iter(), rx),
        |st| async move {
            match st {
                ProjLogState::History(mut it, rx) => match it.next() {
                    Some(line) => Some((Ok(proj_log_event(&line)), ProjLogState::History(it, rx))),
                    None => proj_recv_live(rx).await,
                },
                ProjLogState::Live(rx) => proj_recv_live(rx).await,
            }
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn proj_log_event(line: &projects::LogLine) -> Event {
    let data = serde_json::to_string(line).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("log").data(data)
}

async fn proj_recv_live(
    mut rx: broadcast::Receiver<projects::LogLine>,
) -> Option<(Result<Event, Infallible>, ProjLogState)> {
    loop {
        match rx.recv().await {
            Ok(line) => return Some((Ok(proj_log_event(&line)), ProjLogState::Live(rx))),
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

#[cfg(test)]
mod prom_scrape_tests {
    use super::*;

    /// A minimal but representative slice of a real `/metrics` exposition,
    /// exercising bare gauges, protocol-labelled counters, histogram sum/count,
    /// multi-label sums, and the jemalloc/ceiling label lookups.
    const SAMPLE: &str = r#"
# HELP nanobpm_creates_total Process instance creates by protocol (rest|stream).
# TYPE nanobpm_creates_total counter
nanobpm_creates_total{protocol="rest"} 10
nanobpm_creates_total{protocol="stream"} 90
nanobpm_job_completions_total{protocol="rest"} 5
nanobpm_job_completions_total{protocol="stream"} 45
nanobpm_stream_connections_active 4
nanobpm_commit_inflight 2
nanobpm_journal_commits_total 1000
nanobpm_journal_writes_total 2000
nanobpm_journal_bytes_total 3000
nanobpm_stream_credit_stalls_total 7
nanobpm_journal_fsync_seconds_sum 2
nanobpm_journal_fsync_seconds_count 100
nanobpm_commit_wait_seconds_sum 0.5
nanobpm_commit_wait_seconds_count 50
nanobpm_journal_commit_batch_size_sum 400
nanobpm_journal_commit_batch_size_count 100
nanobpm_stream_frame_processing_seconds_sum 1
nanobpm_stream_frame_processing_seconds_count 200
nanobpm_journal_writer_busy_seconds 30
nanobpm_journal_writer_idle_seconds 10
nanobpm_jemalloc_bytes{kind="allocated"} 111
nanobpm_jemalloc_bytes{kind="resident"} 999
nanobpm_ceiling_active{ceiling="throughput"} 1
nanobpm_ceiling_active{ceiling="memory"} 0
nanobpm_ceiling_active{ceiling="exporter"} 1
nanobpm_ceiling_active{ceiling="flow_control"} 0
nanobpm_sla_mode{mode="latency"} 0
nanobpm_sla_mode{mode="admission"} 1
nanobpm_exporter_fill_permille 640
nanobpm_pending_create_queue 3
nanobpm_active_backlog 42
nanobpm_admission_limit{limit="backlog"} 500
nanobpm_admission_limit{limit="create_queue"} 900
nanobpm_admission_shed_total{reason="active_backlog"} 4
nanobpm_admission_shed_total{reason="create_queue"} 6
"#;

    #[test]
    fn reconstructs_metrics_dto_from_prometheus_text() {
        let m = metrics_dto_from_prometheus(SAMPLE);

        assert_eq!(m.creates_rest, 10);
        assert_eq!(m.creates_stream, 90);
        assert_eq!(m.creates_total, 100);
        assert_eq!(m.completions_rest, 5);
        assert_eq!(m.completions_stream, 45);
        assert_eq!(m.completions_total, 50);

        assert_eq!(m.connections_active, 4);
        assert_eq!(m.commit_inflight, 2);
        assert_eq!(m.commits_total, 1000);
        assert_eq!(m.writes_total, 2000);
        assert_eq!(m.bytes_total, 3000);
        assert_eq!(m.credit_stalls_total, 7);

        // 2s / 100 * 1000 = 20 ms; 0.5s / 50 * 1000 = 10 ms.
        assert!((m.fsync_mean_ms - 20.0).abs() < 1e-9);
        assert!((m.commit_wait_mean_ms - 10.0).abs() < 1e-9);
        // 400 / 100 = 4.0 mean batch; 1s / 200 * 1000 = 5 ms frame.
        assert!((m.commit_batch_mean - 4.0).abs() < 1e-9);
        assert!((m.frame_processing_mean_ms - 5.0).abs() < 1e-9);
        // busy 30 / (30 + 10) = 0.75.
        assert!((m.writer_busy_ratio - 0.75).abs() < 1e-9);

        assert_eq!(m.resident_bytes, Some(999));
        assert!(m.ceiling_throughput);
        assert!(!m.ceiling_memory);
        assert!(m.ceiling_exporter);
        assert!(!m.ceiling_flow_control);
        assert_eq!(m.exporter_fill_permille, 640);
        assert_eq!(m.sla_mode, "admission");
        assert_eq!(m.pending_create_queue, 3);
        assert_eq!(m.active_backlog, 42);
        // No nanobpm_active_instances in this (pre-0035) sample, so it falls
        // back to the active_backlog proxy.
        assert_eq!(m.active_instances, 42);
        assert_eq!(m.admission_backlog_limit, 500);
        assert_eq!(m.admission_create_queue_limit, 900);
        // Summed across both shed reasons.
        assert_eq!(m.admission_shed_total, 10);

        // No recovery gauges in this (pre-0035) sample — steady-state default.
        assert!(!m.recovery.recovering);
        assert_eq!(m.recovery.owned, 0);
    }

    /// A node exporting the ADR 0035 scrape-computed gauges reports full
    /// fidelity: the true active COUNT and per-partition recovery, not proxies.
    #[test]
    fn full_fidelity_reads_promoted_gauges() {
        let sample = r#"
nanobpm_active_backlog 42
nanobpm_active_instances 37
nanobpm_partition_owned 4
nanobpm_partition_reclaimed 2
nanobpm_partition_catching_up 2
nanobpm_partition_handing_off 0
"#;
        let m = metrics_dto_from_prometheus(sample);
        // True COUNT wins over the active_backlog proxy.
        assert_eq!(m.active_instances, 37);
        assert!(m.recovery.recovering, "catching_up > 0 => recovering");
        assert_eq!(m.recovery.owned, 4);
        assert_eq!(m.recovery.reclaimed, 2);
        assert_eq!(m.recovery.catching_up, 2);
        assert_eq!(m.recovery.detail, "reclaiming 2/4 partitions");
    }

    /// The incumbent (handing-off) side, including the optional lag gauge.
    #[test]
    fn full_fidelity_handoff_side() {
        let sample = r#"
nanobpm_partition_owned 0
nanobpm_partition_reclaimed 0
nanobpm_partition_catching_up 0
nanobpm_partition_handing_off 3
nanobpm_handoff_lag_entries 1200
"#;
        let m = metrics_dto_from_prometheus(sample);
        assert!(!m.recovery.recovering);
        assert_eq!(m.recovery.handing_off, 3);
        assert_eq!(m.recovery.handoff_lag_entries, Some(1200));
        assert_eq!(m.recovery.detail, "handing back 3 (lag 1200)");
    }

    #[test]
    fn missing_series_default_to_zero_not_panic() {
        let m = metrics_dto_from_prometheus("nanobpm_commit_inflight 1\n");
        assert_eq!(m.commit_inflight, 1);
        assert_eq!(m.creates_total, 0);
        assert_eq!(m.resident_bytes, None);
        assert_eq!(m.fsync_mean_ms, 0.0);
        assert!(!m.ceiling_throughput);
        assert_eq!(m.sla_mode, "latency");
    }
}

#[cfg(test)]
mod asset_encoding_tests {
    use super::*;

    #[test]
    fn is_model_resource_matches_bpmn_only() {
        // The scan surface: `.bpmn` directly under `resources/processes/`,
        // extension matched case-insensitively.
        assert!(is_model_resource("resources/processes/order.bpmn"));
        assert!(is_model_resource("resources/processes/order.BPMN")); // case-insensitive
        assert!(is_model_resource("/resources/processes/order.bpmn")); // leading-slash tolerated
        // A `.bpmn` outside the scan surface is NOT a scanned model, so it must
        // not trigger a regeneration that could not reflect it.
        assert!(!is_model_resource("order.bpmn")); // project root, not scanned
        assert!(!is_model_resource("resources/processes/sub/order.bpmn")); // nested, not scanned
        assert!(!is_model_resource("resources/order.bpmn"));
        assert!(!is_model_resource("workers/charge/worker.ts"));
        assert!(!is_model_resource("resources/forms/f.form"));
        assert!(!is_model_resource("nano.app.json"));
        assert!(!is_model_resource("README")); // no extension
    }

    #[test]
    fn is_workflow_source_matches_workflows_ts_only() {
        // The code-first trigger surface: a `.ts` directly under `workflows/`.
        assert!(is_workflow_source("workflows/pr-review.ts"));
        assert!(is_workflow_source("workflows/order.TS")); // case-insensitive
        assert!(is_workflow_source("/workflows/pr-review.ts")); // leading-slash tolerated
        // Anything outside `workflows/*.ts` must not trigger a Deno round-trip.
        assert!(!is_workflow_source("pr-review.ts")); // project root
        assert!(!is_workflow_source("workflows/sub/pr-review.ts")); // nested
        assert!(!is_workflow_source("scripts/approve.ts"));
        assert!(!is_workflow_source("main.ts"));
        assert!(!is_workflow_source("workflows/pr-review.js")); // not TS
        assert!(!is_workflow_source("workflows/README")); // no extension
        assert!(!is_workflow_source("resources/processes/order.bpmn"));
    }

    fn accept(value: &str) -> AcceptedEncodings {
        let mut headers = HeaderMap::new();
        if !value.is_empty() {
            headers.insert(header::ACCEPT_ENCODING, value.parse().unwrap());
        }
        accepted_encodings(&headers)
    }

    #[test]
    fn parses_brotli_and_gzip_from_accept_encoding() {
        let both = accept("br, gzip");
        assert!(both.br && both.gzip);

        let gz = accept("gzip");
        assert!(!gz.br && gz.gzip);

        let br = accept("br");
        assert!(br.br && !br.gzip);
    }

    #[test]
    fn tolerates_whitespace_quality_values_and_order() {
        // q-values are stripped to the token; ordering and spacing don't matter.
        let enc = accept("gzip;q=0.8,  br;q=1.0, deflate");
        assert!(enc.br && enc.gzip);
    }

    #[test]
    fn absent_or_unknown_encoding_accepts_nothing() {
        let none = accept("");
        assert!(!none.br && !none.gzip);

        let other = accept("deflate, zstd");
        assert!(!other.br && !other.gzip);
    }

    #[test]
    fn request_base_url_prefers_forwarded_proto_and_host() {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, "nano.example.test".parse().unwrap());
        // No forwarded proto → http (the console is typically plain-http/local).
        assert_eq!(request_base_url(&h), "http://nano.example.test");
        // A proxy's X-Forwarded-Proto https wins so the printed URLs are reachable.
        h.insert("x-forwarded-proto", "https, http".parse().unwrap());
        assert_eq!(request_base_url(&h), "https://nano.example.test");
    }

    #[test]
    fn request_base_url_rejects_non_http_forwarded_scheme() {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, "nano.example.test".parse().unwrap());
        // A bogus/hostile scheme must not leak into emitted links — fall back to http.
        h.insert("x-forwarded-proto", "javascript".parse().unwrap());
        assert_eq!(request_base_url(&h), "http://nano.example.test");
        // http is accepted and canonicalised.
        h.insert("x-forwarded-proto", "HTTP".parse().unwrap());
        assert_eq!(request_base_url(&h), "http://nano.example.test");
    }

    #[test]
    fn request_base_url_falls_back_to_localhost() {
        // No Host header at all (e.g. a bare HTTP/1.0 probe) still yields a URL.
        assert_eq!(request_base_url(&HeaderMap::new()), "http://localhost");
    }

    #[tokio::test]
    async fn agent_endpoint_serves_markdown_brief() {
        use axum::body::to_bytes;
        let mut h = HeaderMap::new();
        h.insert(header::HOST, "node.local:9000".parse().unwrap());
        let resp = agent_brief_md(h).await;
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/markdown; charset=utf-8")
        );
        let body = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        // The request's host threads into the actionable import URL.
        assert!(text.contains("http://node.local:9000/console/api/projects/import"));
        assert!(text.contains("## Link it in"));
    }

    #[tokio::test]
    async fn agent_endpoint_is_uncacheable_and_varies_on_encoding() {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, "node.local".parse().unwrap());
        // Uncompressed branch (no Accept-Encoding) still carries both headers, so a
        // proxy neither caches a per-host body nor mixes up encoding variants.
        let resp = agent_brief_md(h).await;
        assert_eq!(
            resp.headers()
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
        assert_eq!(
            resp.headers()
                .get(header::VARY)
                .and_then(|v| v.to_str().ok()),
            Some("Accept-Encoding")
        );
        assert!(resp.headers().get(header::CONTENT_ENCODING).is_none());
    }

    #[tokio::test]
    async fn agent_endpoint_gzips_when_requested() {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, "node.local".parse().unwrap());
        h.insert(header::ACCEPT_ENCODING, "gzip".parse().unwrap());
        let resp = agent_brief_md(h).await;
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok()),
            Some("gzip")
        );
    }
}
