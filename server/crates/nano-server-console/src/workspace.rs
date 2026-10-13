//! Filesystem workspace for the console — the user's authoring **source of
//! truth** (BPMN models today; worker code later), deliberately kept **separate
//! from the engine's data dir** so that deleting cluster data (`NANOBPMN_DATA_DIR`)
//! leaves models and workers intact.
//!
//! Layout under the workspace root:
//! ```text
//! <workspace>/models/<name>.bpmn
//! <workspace>/workers/<name>/{worker.ts, node.json, ...}
//! <workspace>/nano-generated/worker-sdk.ts   (the embedded Node worker SDK)
//! ```
//! The root defaults to `./nanobpm-workspace` (relative to the server's cwd) and
//! is overridden by `NANOBPMN_WORKSPACE_DIR`. Unlike the engine data dir — which
//! forbids creating a deep path so a typo fails fast — the workspace is explicit
//! user content, so it is created on demand.

use std::path::PathBuf;
use std::time::UNIX_EPOCH;

/// Root of the console workspace. `NANOBPMN_WORKSPACE_DIR` overrides the default
/// `./nanobpm-workspace`.
pub fn workspace_dir() -> PathBuf {
    match std::env::var("NANOBPMN_WORKSPACE_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => PathBuf::from("nanobpm-workspace"),
    }
}

/// Directory holding `*.bpmn` model files.
pub fn models_dir() -> PathBuf {
    workspace_dir().join("models")
}

/// Ensures the models directory exists and returns it.
pub fn ensure_models_dir() -> std::io::Result<PathBuf> {
    let dir = models_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Whether `name` is a safe, flat model identifier: it can never escape the
/// models directory (no path separators, no `..`) and stays a tidy filename.
pub fn is_safe_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name != "."
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// The on-disk path for model `name` (`<models>/<name>.bpmn`), or `None` when
/// the name is unsafe.
pub fn model_path(name: &str) -> Option<PathBuf> {
    is_safe_name(name).then(|| models_dir().join(format!("{name}.bpmn")))
}

/// A model file's `(modified_ms, size_bytes)`, defaulting to `(0, 0)` when the
/// metadata or mtime is unavailable.
pub fn file_meta(path: &std::path::Path) -> (u64, u64) {
    let Ok(meta) = std::fs::metadata(path) else {
        return (0, 0);
    };
    let modified_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    (modified_ms, meta.len())
}

/// Lists the `*.bpmn` model file stems in the workspace, ensuring the directory
/// exists first. Returns an empty list (not an error) when the directory is
/// freshly created and empty.
pub fn list_model_names() -> std::io::Result<Vec<String>> {
    let dir = ensure_models_dir()?;
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("bpmn")
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            && is_safe_name(stem)
        {
            names.push(stem.to_string());
        }
    }
    names.sort();
    Ok(names)
}

// ---------------------------------------------------------------------------
// Workers — each worker is a directory of source files under `workers/<name>/`.
// ---------------------------------------------------------------------------

/// Directory holding worker subdirectories.
pub fn workers_dir() -> PathBuf {
    workspace_dir().join("workers")
}

/// Ensures the workers directory exists and returns it.
pub fn ensure_workers_dir() -> std::io::Result<PathBuf> {
    let dir = workers_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// The on-disk directory for worker `name` (`<workers>/<name>`), or `None` when
/// the name is unsafe.
pub fn worker_dir(name: &str) -> Option<PathBuf> {
    is_safe_name(name).then(|| workers_dir().join(name))
}

/// Whether `file` is a safe, flat filename within a worker directory: no path
/// separators or `..`, and a tidy name. Workers are flat dirs in v1.
pub fn is_safe_worker_file(file: &str) -> bool {
    is_safe_name(file)
}

/// The on-disk path for `file` inside worker `name`, or `None` if either the
/// worker name or the file name is unsafe.
pub fn worker_file_path(name: &str, file: &str) -> Option<PathBuf> {
    if !is_safe_worker_file(file) {
        return None;
    }
    worker_dir(name).map(|d| d.join(file))
}

/// Lists worker directory names in the workspace, ensuring the directory exists
/// first. Hidden entries (dot-prefixed) are skipped.
pub fn list_worker_names() -> std::io::Result<Vec<String>> {
    let dir = ensure_workers_dir()?;
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str()
            && is_safe_name(name)
        {
            names.push(name.to_string());
        }
    }
    names.sort();
    Ok(names)
}

/// Lists the flat source files in worker `name` (sorted), excluding hidden and
/// nested entries. Returns an error if the worker directory cannot be read.
pub fn list_worker_files(name: &str) -> std::io::Result<Vec<String>> {
    let Some(dir) = worker_dir(name) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid worker name",
        ));
    };
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if let Some(f) = entry.file_name().to_str()
            && is_safe_worker_file(f)
        {
            files.push(f.to_string());
        }
    }
    files.sort();
    Ok(files)
}

/// The generated directory holding the embedded Node worker SDK.
pub fn sdk_dir() -> PathBuf {
    workspace_dir().join(super::projects::GEN_DIR)
}

/// Path to the embedded worker SDK file (`nano-generated/worker-sdk.ts`).
pub fn sdk_path() -> PathBuf {
    sdk_dir().join("worker-sdk.ts")
}

// ---------------------------------------------------------------------------
// Shared library — reusable TS/JS files under `lib/`, importable from every
// worker via the `@lib/` import-map alias (`workers/<name>/node.json`).
// ---------------------------------------------------------------------------

/// Directory holding shared library source files (`<workspace>/lib/`). Workers
/// reach it through the `@lib/` import-map alias, so logic can be authored once
/// and reused across workers.
pub fn lib_dir() -> PathBuf {
    workspace_dir().join("lib")
}

/// Ensures the shared library directory exists and returns it.
pub fn ensure_lib_dir() -> std::io::Result<PathBuf> {
    let dir = lib_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// The on-disk path for shared library `file` (`<lib>/<file>`), or `None` when
/// the file name is unsafe (flat, no separators or `..` — same rule as workers).
pub fn lib_file_path(file: &str) -> Option<PathBuf> {
    is_safe_worker_file(file).then(|| lib_dir().join(file))
}

/// Lists the flat source files in the shared library directory (sorted),
/// excluding hidden and nested entries. Returns an empty list (not an error)
/// when the directory is freshly created and empty.
pub fn list_lib_files() -> std::io::Result<Vec<String>> {
    let dir = ensure_lib_dir()?;
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if let Some(f) = entry.file_name().to_str()
            && is_safe_worker_file(f)
        {
            files.push(f.to_string());
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_traversal_and_separators() {
        assert!(!is_safe_name(""));
        assert!(!is_safe_name(".."));
        assert!(!is_safe_name("../etc/passwd"));
        assert!(!is_safe_name("a/b"));
        assert!(!is_safe_name("a\\b"));
        assert!(!is_safe_name("foo..bar"));
        assert!(model_path("../secret").is_none());
    }

    #[test]
    fn accepts_tidy_names() {
        assert!(is_safe_name("order"));
        assert!(is_safe_name("order-v2"));
        assert!(is_safe_name("order_2.final"));
        let p = model_path("order").unwrap();
        assert!(p.ends_with("order.bpmn"));
    }

    #[test]
    fn worker_paths_are_contained() {
        // Worker names reuse the same safety predicate as models.
        assert!(worker_dir("../escape").is_none());
        assert!(worker_dir("order-worker").is_some());
        // A file within a worker must be a safe flat filename.
        assert!(worker_file_path("w", "worker.ts").is_some());
        assert!(worker_file_path("w", "node.json").is_some());
        assert!(worker_file_path("w", "../../etc/passwd").is_none());
        assert!(worker_file_path("w", "sub/dir.ts").is_none());
        assert!(worker_file_path("../w", "worker.ts").is_none());
    }

    #[test]
    fn lib_paths_are_contained() {
        // Shared library files reuse the worker file-name safety rule.
        assert!(lib_file_path("util.ts").is_some());
        assert!(lib_file_path("format-money.ts").is_some());
        assert!(lib_file_path("../../etc/passwd").is_none());
        assert!(lib_file_path("sub/dir.ts").is_none());
        assert!(lib_file_path("..").is_none());
        assert!(lib_dir().ends_with("lib"));
    }
}
