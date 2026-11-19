//! **Loaded trace datasets** — the offline counterpart to a live `NanoClient`.
//!
//! A consultant working a customer engagement does not always have a live Nano
//! gateway to point at; they have *captured data* — a folder of exported instance
//! traces (the same JSON the gateway serves at `GET /console/api/traces/{key}`).
//! `DatasetSource` loads such a folder into memory once and serves the exact same
//! read surface the Insights/report layer expects, so ProcessOS can reason over a
//! customer's history with no engine running.
//!
//! The [`TraceSource`] enum is the seam: every read-path consumer takes a
//! `&TraceSource` and is agnostic to whether the bytes came from a live gateway or
//! a directory on disk.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::contracts::{InstanceTrace, Metrics, NanoClient, TraceSummary};

/// An in-memory dataset of instance traces loaded from a directory.
///
/// Accepts either a directory of per-instance `*.json` files (each an
/// `InstanceTrace`, matching `GET /console/api/traces/{key}`) **or** a single
/// `traces.json` array of the same. An optional `metrics.json` (a `Metrics`)
/// supplies the live-gauge panel; absent, the gauges are simply omitted.
pub struct DatasetSource {
    path: PathBuf,
    traces: Vec<InstanceTrace>,
    summaries: Vec<TraceSummary>,
    /// `instance_key` -> index into `traces`, so per-key reads are O(1) even on
    /// full-population folds over large captured datasets.
    by_key: BTreeMap<String, usize>,
    metrics: Option<Metrics>,
}

impl DatasetSource {
    /// Load every trace JSON under `dir`. Returns an error only when the directory
    /// cannot be read or contains no parseable trace; individual unparseable files
    /// are skipped (a dataset is curated, but tolerant loading avoids one stray
    /// file breaking the whole engagement).
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, String> {
        let dir = dir.as_ref();
        if !dir.is_dir() {
            return Err(format!(
                "dataset path is not a directory: {}",
                dir.display()
            ));
        }

        let mut traces: Vec<InstanceTrace> = Vec::new();

        // 1) A single bundled array (traces.json), if present.
        let bundle = dir.join("traces.json");
        if bundle.is_file() {
            if let Ok(bytes) = std::fs::read(&bundle) {
                if let Ok(arr) = serde_json::from_slice::<Vec<InstanceTrace>>(&bytes) {
                    traces.extend(arr);
                }
            }
        }

        // 2) Per-instance files, both at the top level and under an `instances/`
        //    (or `traces/`) subfolder, so common dump layouts all work.
        for sub in [dir.to_path_buf(), dir.join("instances"), dir.join("traces")] {
            collect_instance_files(&sub, &mut traces);
        }

        if traces.is_empty() {
            return Err(format!(
                "no parseable instance traces under {}",
                dir.display()
            ));
        }

        // De-dup by instance key (a file may also appear in the bundle); keep first.
        traces.sort_by_key(|b| std::cmp::Reverse(b.started_at));
        traces.dedup_by(|a, b| a.instance_key == b.instance_key);

        let summaries = traces.iter().map(summary_of).collect();
        let by_key = traces
            .iter()
            .enumerate()
            .map(|(i, t)| (t.instance_key.clone(), i))
            .collect();

        let metrics = {
            let m = dir.join("metrics.json");
            std::fs::read(&m)
                .ok()
                .and_then(|b| serde_json::from_slice::<Metrics>(&b).ok())
        };

        Ok(Self {
            path: dir.to_path_buf(),
            traces,
            summaries,
            by_key,
            metrics,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> usize {
        self.traces.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.traces.is_empty()
    }

    fn list_traces(&self, limit: usize) -> Vec<TraceSummary> {
        self.summaries.iter().take(limit).cloned().collect()
    }

    fn trace(&self, instance_key: &str) -> Option<InstanceTrace> {
        self.by_key
            .get(instance_key)
            .and_then(|&i| self.traces.get(i))
            .cloned()
    }
}

/// Read a single directory level for `*.json` instance traces (non-recursive
/// beyond the level it is called on). `traces.json` and `metrics.json` are
/// reserved names handled separately and skipped here.
fn collect_instance_files(dir: &Path, out: &mut Vec<InstanceTrace>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name == "traces.json" || name == "metrics.json" {
            continue;
        }
        if !name.ends_with(".json") {
            continue;
        }
        if let Ok(bytes) = std::fs::read(&p) {
            if let Ok(t) = serde_json::from_slice::<InstanceTrace>(&bytes) {
                out.push(t);
            }
        }
    }
}

/// Derive a `TraceSummary` (the list-view shape) from a full instance trace.
fn summary_of(t: &InstanceTrace) -> TraceSummary {
    let ended_at = t.duration_ms.map(|d| t.started_at + d);
    TraceSummary {
        instance_key: t.instance_key.clone(),
        process_id: t.process_id.clone(),
        version: t.version,
        business_id: None,
        outcome: t.outcome.clone(),
        started_at: t.started_at,
        ended_at,
        duration_ms: t.duration_ms,
        element_count: t.elements.len(),
        incident_count: t.incidents.len(),
    }
}

/// The read seam: a trace source is either a **live** Nano gateway or a **loaded**
/// dataset on disk. Both answer the same three reads the Insights layer needs.
#[derive(Clone)]
pub enum TraceSource {
    /// A live customer/deployment Nano instance, read over its public contract.
    Live(NanoClient),
    /// A captured dataset loaded from a folder.
    Dataset(Arc<DatasetSource>),
}

impl TraceSource {
    pub async fn list_traces(&self, limit: usize) -> Result<Vec<TraceSummary>, String> {
        match self {
            TraceSource::Live(c) => c.list_traces(limit).await,
            TraceSource::Dataset(d) => Ok(d.list_traces(limit)),
        }
    }

    pub async fn trace(&self, instance_key: &str) -> Result<InstanceTrace, String> {
        match self {
            TraceSource::Live(c) => c.trace(instance_key).await,
            TraceSource::Dataset(d) => d
                .trace(instance_key)
                .ok_or_else(|| format!("instance {instance_key} not in dataset")),
        }
    }

    /// Live gauges, when available. A dataset only has them if a `metrics.json`
    /// shipped with it; a live source may transiently fail — both map to `None`.
    pub async fn metrics(&self) -> Option<Metrics> {
        match self {
            TraceSource::Live(c) => c.metrics().await.ok(),
            TraceSource::Dataset(d) => d.metrics.clone(),
        }
    }

    /// A human label for the `nanoBaseUrl`/source field of a report.
    pub fn label(&self) -> String {
        match self {
            TraceSource::Live(c) => c.base_url().to_string(),
            TraceSource::Dataset(d) => format!("dataset:{}", d.path().display()),
        }
    }

    /// The true population size when it is known up front — i.e. for an in-memory
    /// dataset, where every trace is already loaded. `None` for a live gateway,
    /// whose total is only discoverable by paging the trace list.
    pub fn total(&self) -> Option<usize> {
        match self {
            TraceSource::Live(_) => None,
            TraceSource::Dataset(d) => Some(d.len()),
        }
    }
}

impl From<NanoClient> for TraceSource {
    fn from(c: NanoClient) -> Self {
        TraceSource::Live(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, body: &str) {
        std::fs::write(dir.join(name), body).unwrap();
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "processos-dataset-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    const T1: &str = r#"{
        "instanceKey":"1","processId":"order","version":1,"outcome":"completed",
        "startedAt":1000,"durationMs":250,
        "elements":[{"elementId":"a"}],
        "incidents":[]
    }"#;

    const T2: &str = r#"{
        "instanceKey":"2","processId":"order","version":1,"outcome":"active",
        "startedAt":2000,
        "elements":[],
        "incidents":[{"elementId":"a","kind":"x","reason":"boom"}]
    }"#;

    #[test]
    fn loads_per_instance_files_and_derives_summaries() {
        let dir = tmpdir("perfile");
        write(&dir, "1.json", T1);
        write(&dir, "2.json", T2);

        let ds = DatasetSource::open(&dir).unwrap();
        assert_eq!(ds.len(), 2);

        // newest first by startedAt
        let sums = ds.list_traces(10);
        assert_eq!(sums[0].instance_key, "2");
        assert_eq!(sums[1].instance_key, "1");

        // derived fields
        let s1 = sums.iter().find(|s| s.instance_key == "1").unwrap();
        assert_eq!(s1.outcome, "completed");
        assert_eq!(s1.element_count, 1);
        assert_eq!(s1.duration_ms, Some(250));
        assert_eq!(s1.ended_at, Some(1250));

        let s2 = sums.iter().find(|s| s.instance_key == "2").unwrap();
        assert_eq!(s2.incident_count, 1);
        assert_eq!(s2.ended_at, None);

        // detail fetch
        assert!(ds.trace("1").is_some());
        assert!(ds.trace("missing").is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn loads_a_bundled_array_and_dedups() {
        let dir = tmpdir("bundle");
        write(&dir, "traces.json", &format!("[{T1},{T2}]"));
        // also drop instance 1 as a stray file -> must be de-duped, not doubled
        write(&dir, "1.json", T1);

        let ds = DatasetSource::open(&dir).unwrap();
        assert_eq!(ds.len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_or_missing_dir_errors() {
        let dir = tmpdir("empty");
        assert!(DatasetSource::open(&dir).is_err());
        assert!(DatasetSource::open(dir.join("nope")).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trace_source_dataset_arm_reads_in_memory() {
        let dir = tmpdir("src");
        write(&dir, "1.json", T1);
        let src = TraceSource::Dataset(Arc::new(DatasetSource::open(&dir).unwrap()));
        assert_eq!(src.list_traces(10).await.unwrap().len(), 1);
        assert!(src.trace("1").await.is_ok());
        assert!(src.trace("x").await.is_err());
        assert!(src.label().starts_with("dataset:"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
