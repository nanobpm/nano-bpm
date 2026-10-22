//! The pilot process — plastic surface **(a)** of §10: the BPMN choreography that
//! drives ProcessOS's own optimization loop, made operator-editable and **forkable**.
//!
//! Factory-fresh, an instance ships with the built-in default pilot. The operator
//! individuates the instance by forking it — and because §10.2's insight is that the
//! *placement of the user tasks is the human↔droid delegation dial*, reshaping this
//! process is literally "learning to fly the craft with a droid copilot": moving a
//! user task later cedes more to the droid; adding one keeps a hand on the controls.
//!
//! The fork is **file-backed** under the ProcessOS data dir, so it is durable across
//! restarts and an explicit, versioned, inspectable artifact (the §10.1 honest-cost
//! mitigation — the tuning lives in the BPMN, not in someone's head). The supervisor
//! deploys whatever this store resolves into the own engine on boot; the API can
//! re-fork and hot-redeploy at runtime.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use nanobpmn_engine_core::bpmn::parse_bpmn;
use serde::Serialize;

/// The built-in default pilot process, embedded so a factory-fresh instance always has
/// a craft to fly regardless of the directory the binary is launched from.
pub const DEFAULT_PILOT_BPMN: &str = include_str!("../pilot/pilot-self-optimize.bpmn");

/// The filename the fork is persisted under, inside the data dir.
const PILOT_FILE: &str = "pilot.bpmn";

/// The resource name used when deploying the pilot to the own engine.
pub const DEPLOY_FILENAME: &str = "pilot-self-optimize.bpmn";

/// Where the current pilot XML came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// The built-in default.
    Default,
    /// An operator-authored fork loaded from / written to disk.
    Forked,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Source::Default => "default",
            Source::Forked => "forked",
        }
    }
}

/// A serializable snapshot of the current pilot process.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PilotDoc {
    /// The process id(s) the BPMN declares (the cockpit drives `pilotSelfOptimize`).
    pub process_ids: Vec<String>,
    /// `"default"` (built-in) or `"forked"` (operator-authored).
    pub source: &'static str,
    /// The BPMN XML.
    pub xml: String,
    /// The resource name used to deploy it.
    pub deploy_filename: &'static str,
}

struct PilotState {
    xml: String,
    source: Source,
}

/// A durable, file-backed store for the pilot process. Seeded with the built-in
/// default on first open; an operator fork is written to disk and survives restarts.
pub struct PilotStore {
    path: PathBuf,
    state: Mutex<PilotState>,
}

impl PilotStore {
    /// Open (or create) the store under `data_dir`. A non-empty `pilot.bpmn` already
    /// there is loaded as a fork; otherwise the built-in default is seeded and written
    /// (best-effort) so it is an inspectable artifact from the start.
    pub fn open(data_dir: &Path) -> Self {
        let path = data_dir.join(PILOT_FILE);
        let state = match std::fs::read_to_string(&path) {
            Ok(xml) if !xml.trim().is_empty() => PilotState {
                xml,
                source: Source::Forked,
            },
            _ => {
                let _ = std::fs::create_dir_all(data_dir);
                let _ = std::fs::write(&path, DEFAULT_PILOT_BPMN);
                PilotState {
                    xml: DEFAULT_PILOT_BPMN.to_string(),
                    source: Source::Default,
                }
            }
        };
        Self {
            path,
            state: Mutex::new(state),
        }
    }

    /// The XML the supervisor should deploy on boot.
    pub fn current_xml(&self) -> String {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .xml
            .clone()
    }

    /// A serializable snapshot of the current pilot.
    pub fn doc(&self) -> PilotDoc {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        doc_of(&s.xml, s.source)
    }

    /// Author a fork: validate it parses to at least one process, persist it, and make
    /// it current. Returns the new doc, or a validation error (nothing is written).
    pub fn save(&self, xml: &str) -> Result<PilotDoc, String> {
        let ids = process_ids(xml)?;
        if ids.is_empty() {
            return Err("pilot BPMN declares no process".to_string());
        }
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&self.path, xml)
            .map_err(|e| format!("write {}: {e}", self.path.display()))?;
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.xml = xml.to_string();
        s.source = Source::Forked;
        Ok(doc_of(&s.xml, s.source))
    }

    /// Restore the built-in default and persist it.
    pub fn reset(&self) -> PilotDoc {
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&self.path, DEFAULT_PILOT_BPMN);
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.xml = DEFAULT_PILOT_BPMN.to_string();
        s.source = Source::Default;
        doc_of(&s.xml, s.source)
    }
}

fn doc_of(xml: &str, source: Source) -> PilotDoc {
    PilotDoc {
        process_ids: process_ids(xml).unwrap_or_default(),
        source: source.as_str(),
        xml: xml.to_string(),
        deploy_filename: DEPLOY_FILENAME,
    }
}

/// Parse the BPMN and return its declared process ids, or a validation error.
fn process_ids(xml: &str) -> Result<Vec<String>, String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("invalid BPMN: {e}"))?;
    Ok(defs.into_iter().map(|d| d.id).collect())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn tmp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pos-pilot-{}-{n}", std::process::id()))
    }

    #[test]
    fn the_embedded_default_is_the_pilot_process() {
        assert!(DEFAULT_PILOT_BPMN.contains("pilotSelfOptimize"));
        let ids = process_ids(DEFAULT_PILOT_BPMN).unwrap();
        assert!(ids.iter().any(|id| id == "pilotSelfOptimize"));
    }

    #[test]
    fn open_seeds_the_default_and_writes_it() {
        let dir = tmp();
        let store = PilotStore::open(&dir);
        let doc = store.doc();
        assert_eq!(doc.source, "default");
        assert!(doc.process_ids.iter().any(|id| id == "pilotSelfOptimize"));
        // The default was materialized to disk as an inspectable artifact.
        assert!(dir.join(PILOT_FILE).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_saved_fork_is_durable_across_reopen() {
        let dir = tmp();
        let forked = DEFAULT_PILOT_BPMN.to_string();
        // A real, parseable variant: tweak a non-id attribute so bytes differ but the
        // process id stays the one the cockpit drives.
        let forked = forked.replace(
            "<bpmn:process",
            "<!-- forked by the operator --><bpmn:process",
        );
        {
            let store = PilotStore::open(&dir);
            let doc = store.save(&forked).unwrap();
            assert_eq!(doc.source, "forked");
        }
        // Reopen: the fork (not the default) is loaded.
        let reopened = PilotStore::open(&dir);
        let doc = reopened.doc();
        assert_eq!(doc.source, "forked");
        assert!(doc.xml.contains("forked by the operator"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_rejects_invalid_bpmn() {
        let dir = tmp();
        let store = PilotStore::open(&dir);
        assert!(store.save("not bpmn at all <<<").is_err());
        // The store is unchanged — still the default.
        assert_eq!(store.doc().source, "default");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reset_restores_the_default() {
        let dir = tmp();
        let store = PilotStore::open(&dir);
        let forked = DEFAULT_PILOT_BPMN.replace("<bpmn:process", "<!-- fork --><bpmn:process");
        store.save(&forked).unwrap();
        assert_eq!(store.doc().source, "forked");
        let doc = store.reset();
        assert_eq!(doc.source, "default");
        assert!(!store.doc().xml.contains("<!-- fork -->"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
