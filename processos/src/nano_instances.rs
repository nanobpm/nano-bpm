//! Configurable **Nano instances** — the live engines ProcessOS can connect to and read
//! over the public trace contract.
//!
//! Historically the analysis target was fixed at boot from `NANO_TARGET_URL` /
//! `NANO_BASE_URL`. The Console now lets the operator define several named instances and
//! pick which one is **active**; the active instance's base URL becomes the analysis
//! target at request time. On first run the store seeds a single instance pointing at the
//! boot-configured URL (typically `http://localhost:8080`) so an out-of-the-box ProcessOS
//! is already wired to a local Nano.
//!
//! Persisted to `<config_dir>/nano-instances.json` (no secrets — base URLs only), so the
//! configured instances follow the operator across workspaces and restarts.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A live Nano engine ProcessOS can read traces/metrics from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NanoInstance {
    pub id: String,
    pub name: String,
    pub base_url: String,
    /// Starred instances float to the top of the Console card layout.
    #[serde(default)]
    pub starred: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Persisted {
    #[serde(default)]
    instances: Vec<NanoInstance>,
    #[serde(default)]
    active: Option<String>,
}

/// A file-backed registry of Nano instances with a single active selection.
pub struct NanoInstanceStore {
    path: PathBuf,
    state: RwLock<Persisted>,
}

impl NanoInstanceStore {
    /// Open the store at `path`, seeding a single default instance pointing at
    /// `default_url` (the boot-configured target, usually `http://localhost:8080`) when
    /// nothing usable is persisted yet.
    pub fn open(path: impl Into<PathBuf>, default_url: &str) -> Self {
        let path = path.into();
        let mut state: Persisted = std::fs::read_to_string(&path)
            .ok()
            .and_then(|b| serde_json::from_str(&b).ok())
            .unwrap_or_default();
        state.instances.retain(|i| !i.base_url.trim().is_empty());
        if state.instances.is_empty() {
            let inst = NanoInstance {
                id: new_id(),
                name: "Local Nano".to_string(),
                base_url: normalize_url(default_url),
                starred: false,
            };
            state.active = Some(inst.id.clone());
            state.instances.push(inst);
        }
        // Repair a dangling/empty active pointer.
        let active_ok = state
            .active
            .as_ref()
            .is_some_and(|a| state.instances.iter().any(|i| &i.id == a));
        if !active_ok {
            state.active = state.instances.first().map(|i| i.id.clone());
        }
        let store = Self {
            path,
            state: RwLock::new(state),
        };
        store.persist();
        store
    }

    /// Every configured instance plus the active id. Starred instances are returned first
    /// (stable within each group), so the Console card layout floats them to the top.
    pub fn list(&self) -> (Vec<NanoInstance>, Option<String>) {
        let g = self.state.read().unwrap_or_else(|e| e.into_inner());
        let mut instances = g.instances.clone();
        instances.sort_by_key(|i| !i.starred);
        (instances, g.active.clone())
    }

    /// The base URL of the active instance, if any is configured.
    pub fn active_base_url(&self) -> Option<String> {
        let g = self.state.read().unwrap_or_else(|e| e.into_inner());
        let active = g.active.as_ref()?;
        g.instances
            .iter()
            .find(|i| &i.id == active)
            .map(|i| i.base_url.clone())
    }

    /// Add a new instance. The first instance added becomes active automatically.
    pub fn add(&self, name: &str, base_url: &str) -> Result<NanoInstance, String> {
        let url = normalize_url(base_url);
        if url.is_empty() {
            return Err("base URL must not be empty".to_string());
        }
        let inst = NanoInstance {
            id: new_id(),
            name: clean_name(name, &url),
            base_url: url,
            starred: false,
        };
        {
            let mut g = self.state.write().unwrap_or_else(|e| e.into_inner());
            if g.active.is_none() {
                g.active = Some(inst.id.clone());
            }
            g.instances.push(inst.clone());
        }
        self.persist();
        Ok(inst)
    }

    /// Edit an existing instance's name and/or base URL.
    pub fn update(&self, id: &str, name: &str, base_url: &str) -> Result<NanoInstance, String> {
        let url = normalize_url(base_url);
        if url.is_empty() {
            return Err("base URL must not be empty".to_string());
        }
        let out = {
            let mut g = self.state.write().unwrap_or_else(|e| e.into_inner());
            let inst = g
                .instances
                .iter_mut()
                .find(|i| i.id == id)
                .ok_or_else(|| format!("no such instance: {id}"))?;
            inst.name = clean_name(name, &url);
            inst.base_url = url;
            inst.clone()
        };
        self.persist();
        Ok(out)
    }

    /// Remove an instance. If it was active, selection falls back to the first remaining.
    pub fn remove(&self, id: &str) -> Result<(), String> {
        {
            let mut g = self.state.write().unwrap_or_else(|e| e.into_inner());
            let before = g.instances.len();
            g.instances.retain(|i| i.id != id);
            if g.instances.len() == before {
                return Err(format!("no such instance: {id}"));
            }
            if g.active.as_deref() == Some(id) {
                g.active = g.instances.first().map(|i| i.id.clone());
            }
        }
        self.persist();
        Ok(())
    }

    /// Star or unstar an instance, so the Console floats it to the top of the card layout.
    pub fn set_star(&self, id: &str, starred: bool) -> Result<NanoInstance, String> {
        let out = {
            let mut g = self.state.write().unwrap_or_else(|e| e.into_inner());
            let inst = g
                .instances
                .iter_mut()
                .find(|i| i.id == id)
                .ok_or_else(|| format!("no such instance: {id}"))?;
            inst.starred = starred;
            inst.clone()
        };
        self.persist();
        Ok(out)
    }

    /// Make an instance the active analysis target.
    pub fn select(&self, id: &str) -> Result<(), String> {
        {
            let mut g = self.state.write().unwrap_or_else(|e| e.into_inner());
            if !g.instances.iter().any(|i| i.id == id) {
                return Err(format!("no such instance: {id}"));
            }
            g.active = Some(id.to_string());
        }
        self.persist();
        Ok(())
    }

    /// Look up a single instance by id.
    pub fn get(&self, id: &str) -> Option<NanoInstance> {
        let g = self.state.read().unwrap_or_else(|e| e.into_inner());
        g.instances.iter().find(|i| i.id == id).cloned()
    }

    fn persist(&self) {
        let snapshot = {
            let g = self.state.read().unwrap_or_else(|e| e.into_inner());
            g.clone()
        };
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(&snapshot) {
            Ok(body) => {
                if let Err(e) = std::fs::write(&self.path, body) {
                    tracing::warn!(path = %self.path.display(), error = %e, "nano-instances: persist failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "nano-instances: serialize failed"),
        }
    }
}

/// Trim a trailing slash and default a bare host:port to `http://`.
fn normalize_url(u: &str) -> String {
    let u = u.trim().trim_end_matches('/');
    if u.is_empty() {
        return String::new();
    }
    if u.starts_with("http://") || u.starts_with("https://") {
        u.to_string()
    } else {
        format!("http://{u}")
    }
}

/// Fall back to the host:port as a display name when none is given.
fn clean_name(name: &str, url: &str) -> String {
    let n = name.trim();
    if n.is_empty() {
        url.trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string()
    } else {
        n.to_string()
    }
}

fn new_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    format!("nano-{now}-{seq}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        // new_id() is millisecond+process-global-counter based, which collides
        // across nextest's per-test processes; add the PID so each test process
        // writes to a private file.
        std::env::temp_dir().join(format!(
            "processos-nano-{}-{}-{}.json",
            std::process::id(),
            new_id(),
            n
        ))
    }

    #[test]
    fn seeds_default_localhost_and_persists() {
        let path = tmp();
        let _ = std::fs::remove_file(&path);
        {
            let store = NanoInstanceStore::open(&path, "http://localhost:8080");
            let (list, active) = store.list();
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].base_url, "http://localhost:8080");
            assert_eq!(active.as_deref(), Some(list[0].id.as_str()));
            assert_eq!(
                store.active_base_url().as_deref(),
                Some("http://localhost:8080")
            );
        }
        // Reopen: the seeded instance survives, no second seed.
        {
            let store = NanoInstanceStore::open(&path, "http://localhost:8080");
            assert_eq!(store.list().0.len(), 1);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn add_select_update_remove_flow() {
        let path = tmp();
        let _ = std::fs::remove_file(&path);
        let store = NanoInstanceStore::open(&path, "http://localhost:8080");
        let seed = store.list().0[0].id.clone();

        // Bare host:port is normalized to http://.
        let staging = store.add("Staging", "nano.staging:8080").unwrap();
        assert_eq!(staging.base_url, "http://nano.staging:8080");
        assert_eq!(store.list().0.len(), 2);

        // Selecting switches the active target.
        store.select(&staging.id).unwrap();
        assert_eq!(
            store.active_base_url().as_deref(),
            Some("http://nano.staging:8080")
        );

        // Updating edits in place.
        let edited = store
            .update(&staging.id, "Staging EU", "https://eu.example/")
            .unwrap();
        assert_eq!(edited.name, "Staging EU");
        assert_eq!(edited.base_url, "https://eu.example");

        // Starring floats the instance to the top of the listing, unstarring restores order.
        store.set_star(&staging.id, true).unwrap();
        assert_eq!(store.list().0[0].id, staging.id);
        assert!(store.list().0[0].starred);
        store.set_star(&staging.id, false).unwrap();
        assert_eq!(store.list().0[0].id, seed);
        assert!(store.set_star("nope", true).is_err());

        // Removing the active falls back to a remaining instance.
        store.remove(&staging.id).unwrap();
        assert_eq!(
            store.active_base_url().as_deref(),
            Some("http://localhost:8080")
        );
        assert_eq!(store.list().1.as_deref(), Some(seed.as_str()));

        // Unknown ids error.
        assert!(store.select("nope").is_err());
        assert!(store.remove("nope").is_err());
        assert!(store.add("x", "   ").is_err());
        let _ = std::fs::remove_file(&path);
    }
}
