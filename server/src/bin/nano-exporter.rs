//! `nano-exporter` — central read-model exporter service (pluggable-exporter
//! epic, issue #133, M2).
//!
//! One (or, later, a small pool of) VM(s) that receive projected read-model
//! event batches from nanobpmn nodes running in `remote`/`tee` exporter mode and
//! write them to a downstream data-lake target (Elasticsearch first, abstracted
//! for Postgres/others next). Centralising projection here removes the per-node
//! read-model disk IOPS from the cluster's hot path — that is the experiment
//! this service exists to enable.
//!
//! Wire protocol (mirrors `remote_sink::HttpBatchTransport`): the node sends
//! `POST {endpoint}` with header `x-nano-partition: <id>` and a body that is a
//! JSON array of engine events (opaque here — we treat each as a raw JSON
//! document, so the service is decoupled from the engine's event schema and
//! keeps working across schema evolution). A 2xx acks the batch; any other
//! status makes the node retry (so a target outage backpressures rather than
//! loses data).
//!
//! Targets are pluggable behind [`RemoteTarget`]:
//! * [`ElasticsearchTarget`] — bulk-appends to an ES index via `_bulk`.
//! * [`StdoutTarget`] — logs batch summaries; the default when no ES URL is set,
//!   so the service runs standalone for smoke tests.

use std::sync::Arc;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let bind = std::env::var("NANO_EXPORTER_BIND").unwrap_or_else(|_| "0.0.0.0:9700".to_string());
    let target = build_target();
    tracing::info!("nano-exporter: target = {}", target.name());

    // axum defaults request bodies to 2 MiB; a drained 50 KB-payload export
    // batch blows past that and returns 413, which (before the node-side chunk +
    // drop fix) poisoned the export pipeline. Raise the limit generously so a
    // full chunked batch is accepted. Overridable via NANO_EXPORTER_MAX_BODY_BYTES.
    let max_body = std::env::var("NANO_EXPORTER_MAX_BODY_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(64 * 1024 * 1024);

    let app = Router::new()
        .route("/ingest", post(ingest))
        .route("/health", get(|| async { "ok" }))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(target);

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("nano-exporter: bind {bind} failed: {e}"))?;
    tracing::info!("nano-exporter: listening on {bind}");
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("nano-exporter: server error: {e}"))?;
    Ok(())
}

/// Selects the downstream target from the environment: Elasticsearch when
/// `NANO_EXPORTER_ES_URL` is set, otherwise the stdout logger.
fn build_target() -> Arc<dyn RemoteTarget> {
    match std::env::var("NANO_EXPORTER_ES_URL")
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(url) => {
            let index = std::env::var("NANO_EXPORTER_ES_INDEX")
                .unwrap_or_else(|_| "nano-events".to_string());
            Arc::new(ElasticsearchTarget::new(url, index))
        }
        None => Arc::new(StdoutTarget),
    }
}

/// `POST /ingest` handler: parse the batch and hand it to the target. A target
/// error maps to 503 so the sending node retries (never-drop-a-batch).
async fn ingest(
    State(target): State<Arc<dyn RemoteTarget>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    let partition = headers
        .get("x-nano-partition")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(u64::MAX);

    let events: Vec<Value> = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("nano-exporter: partition {partition} malformed batch: {e}");
            // Malformed body is a permanent error — 400 so the node does not spin
            // retrying an unparseable batch forever.
            return StatusCode::BAD_REQUEST;
        }
    };

    match target.write_batch(partition, &events).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            tracing::warn!(
                "nano-exporter: partition {partition} target write failed ({} events): {e}",
                events.len()
            );
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

/// A downstream sink for projected read-model event batches. Implementations
/// MUST be idempotent-friendly (batches may be re-delivered under the node's
/// at-least-once retry) and MUST return `Err` on a transient failure so the node
/// retries instead of the service silently dropping data.
#[async_trait::async_trait]
trait RemoteTarget: Send + Sync {
    async fn write_batch(&self, partition: u64, events: &[Value]) -> anyhow::Result<()>;
    fn name(&self) -> &'static str;
}

/// Bulk-appends events to an Elasticsearch index via the `_bulk` API. Each event
/// becomes one `index` action; the index's own mapping handles the JSON. This is
/// the reference target (mirrors Zeebe/Camunda 8's ES exporter shape).
struct ElasticsearchTarget {
    url: String,
    index: String,
    client: reqwest::Client,
}

impl ElasticsearchTarget {
    fn new(url: String, index: String) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            index,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl RemoteTarget for ElasticsearchTarget {
    async fn write_batch(&self, _partition: u64, events: &[Value]) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        // Build the NDJSON _bulk body: an action line + a source line per event.
        let mut body = String::with_capacity(events.len() * 128);
        for event in events {
            body.push_str("{\"index\":{}}\n");
            body.push_str(&serde_json::to_string(event)?);
            body.push('\n');
        }
        let bulk_url = format!("{}/{}/_bulk", self.url, self.index);
        let resp = self
            .client
            .post(&bulk_url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            anyhow::bail!("elasticsearch _bulk returned {status}");
        }
        // ES returns 200 even when individual items error; surface the aggregate
        // `errors` flag so a mapping problem is retried rather than lost.
        let parsed: Value = serde_json::from_str(&resp.text().await?)?;
        if parsed.get("errors").and_then(Value::as_bool) == Some(true) {
            anyhow::bail!("elasticsearch _bulk reported item-level errors");
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "elasticsearch"
    }
}

/// Logs a one-line summary per batch. Default target for standalone runs / smoke
/// tests when no Elasticsearch URL is configured.
struct StdoutTarget;

#[async_trait::async_trait]
impl RemoteTarget for StdoutTarget {
    async fn write_batch(&self, partition: u64, events: &[Value]) -> anyhow::Result<()> {
        tracing::info!(
            "nano-exporter[stdout]: partition {partition} received {} event(s)",
            events.len()
        );
        Ok(())
    }

    fn name(&self) -> &'static str {
        "stdout"
    }
}
