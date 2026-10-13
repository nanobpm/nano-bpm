//! External-tool preflight.
//!
//! ProcessOS shells out to two optional CLI tools that are **not** bundled: the
//! llama.cpp `llama-server` (which powers the local LLM sidecars used for
//! investigations and chat) and **Node** (which the Nano engine ProcessOS
//! supervises uses to run its embedded job workers). When either is absent the
//! failure otherwise surfaces deep inside a spawn as a cryptic
//! "No such file or directory". This module detects both up front so the Console
//! can tell the operator exactly *what* is missing, *why* it's needed, and link
//! them to the official, OS-aware install instructions.

use std::process::Command;

use serde::Serialize;

/// OS-aware install instructions (cover macOS/Linux/Windows package managers).
const NODE_INSTALL_URL: &str = "https://nodejs.org/en/download";
const LLAMA_INSTALL_URL: &str = "https://github.com/ggml-org/llama.cpp/blob/master/docs/install.md";

/// One external tool ProcessOS depends on, and whether it is available.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dependency {
    /// Stable identifier (`node` | `llama`).
    pub id: &'static str,
    /// Human-facing name.
    pub name: &'static str,
    /// What ProcessOS needs the tool for.
    pub purpose: &'static str,
    /// The binary ProcessOS will invoke (resolved from settings/env where set).
    pub bin: String,
    /// Whether the binary is present and runnable.
    pub present: bool,
    /// First line of `<bin> --version`, when present.
    pub version: Option<String>,
    /// Official, OS-aware install instructions.
    pub install_url: &'static str,
    /// A one-line, actionable hint shown when the tool is missing.
    pub hint: String,
    /// Whether the tool is a hardware-accelerated build, when it can be determined.
    /// `Some(true)` = a GPU/Metal/Vulkan/… backend is available; `Some(false)` = a
    /// CPU-only build; `None` = not applicable (Node) or undeterminable (older
    /// `llama-server` without `--list-devices`).
    pub accelerated: Option<bool>,
    /// A non-blocking advisory shown even when the tool is present (e.g. a CPU-only
    /// llama.cpp build that will make local inference slow). Distinct from `hint`,
    /// which is reserved for the missing-tool case.
    pub note: Option<String>,
}

/// Probe `<bin> --version`. Returns `Some(first non-empty output line)` when the
/// binary ran (presence), or `None` when it could not be spawned (missing).
///
/// Presence is "did it spawn", *not* the exit code — some tools print their
/// version and exit non-zero. `node` prints to stdout; `llama-server` to stderr,
/// so both streams are considered.
fn probe(bin: &str) -> Option<String> {
    let out = Command::new(bin).arg("--version").output().ok()?;
    let stream: &[u8] = if out.stdout.is_empty() {
        &out.stderr
    } else {
        &out.stdout
    };
    let line = String::from_utf8_lossy(stream)
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string();
    Some(line)
}

/// Minimum Node that can run the supervised engine's embedded workers:
/// `--experimental-strip-types` (TS erasure) landed in 22.6. This mirrors the
/// engine's own gate (`workers::NODE_MIN`, `nano-server-console`) — keep the two
/// in lockstep, because reporting an older Node as "present" would suppress this
/// install hint while the supervised engine still refuses to start the workers.
const NODE_MIN: (u32, u32) = (22, 6);

/// Parse `node --version` output — `"v24.15.0\n"`, `"v22.6.1"`, `"v22"` — into
/// `(major, minor)`. Mirrors the engine's parser of the same shape.
fn parse_node_version(s: &str) -> Option<(u32, u32)> {
    let v = s.trim().trim_start_matches('v');
    let mut parts = v.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor))
}

/// Pick the Node binary to report from the probed candidates.
///
/// `probed` is `(candidate, first --version line or None when it did not spawn)`
/// in resolution order. Returns `(bin, version, ran_but_too_old)`: the first
/// candidate whose version is **>= [`NODE_MIN`]** (with that raw version), or —
/// when none qualifies — `(None, None, whether anything spawned at all)`, so the
/// caller can distinguish "nothing installed" (install hint) from "something too
/// old" (upgrade hint). Pure, so the version gate is tested without spawning.
fn select_node(probed: &[(String, Option<String>)]) -> (Option<String>, Option<String>, bool) {
    let mut ran_but_too_old = false;
    for (cand, raw) in probed {
        let Some(raw) = raw else { continue };
        match parse_node_version(raw) {
            Some(v) if v >= NODE_MIN => return (Some(cand.clone()), Some(raw.clone()), false),
            // Spawned but not a new-enough (or unparseable) Node — keep looking.
            _ => ran_but_too_old = true,
        }
    }
    (None, None, ran_but_too_old)
}

/// Resolve the Node binary the supervised Nano engine would use, mirroring that
/// engine's own resolution order: `NANOBPMN_NODE_BIN`, then `node` on `PATH`,
/// then the default `~/.local/bin/node` install location. A candidate counts only
/// when its `--version` is **>= [`NODE_MIN`]** — an older Node is skipped so a
/// newer one further down the list can still win.
fn check_node() -> Dependency {
    let mut candidates: Vec<String> = Vec::new();
    if let Ok(b) = std::env::var("NANOBPMN_NODE_BIN") {
        let b = b.trim().to_string();
        if !b.is_empty() {
            candidates.push(b);
        }
    }
    candidates.push("node".to_string());
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            candidates.push(format!("{home}/.local/bin/node"));
        }
    }

    let probed: Vec<(String, Option<String>)> =
        candidates.iter().map(|c| (c.clone(), probe(c))).collect();
    let (resolved, version, ran_but_too_old) = select_node(&probed);
    // Report the binary that resolved, or the most user-meaningful name when
    // nothing did.
    let bin = resolved.unwrap_or_else(|| candidates[0].clone());
    let present = version.is_some();
    let hint = if present {
        String::new()
    } else if ran_but_too_old {
        format!(
            "Node was found but is older than {}.{} — Nano's embedded workers need `--experimental-strip-types`. Upgrade Node (see the link), or set NANOBPMN_NODE_BIN to a {}.{}+ install.",
            NODE_MIN.0, NODE_MIN.1, NODE_MIN.0, NODE_MIN.1
        )
    } else {
        "Node was not found. Install it (see the link) so `node` is on PATH, or set NANOBPMN_NODE_BIN to its path. Until then, embedded Nano workers cannot run."
            .to_string()
    };

    Dependency {
        id: "node",
        name: "Node",
        purpose: "Running Nano's embedded job workers — the Nano engine ProcessOS supervises spawns Node worker processes.",
        bin,
        present,
        version,
        install_url: NODE_INSTALL_URL,
        hint,
        accelerated: None,
        note: None,
    }
}

/// Best-effort backend probe: ask `llama-server --list-devices` and look for a hardware
/// accelerator in its output. `Some(true)` = a GPU/Metal/Vulkan/… device is available;
/// `Some(false)` = it ran but reported only CPU; `None` = couldn't tell (an older build without
/// the flag, or the spawn failed) — in which case we stay quiet rather than nag. The flag only
/// enumerates devices; it does not load a model, so it is cheap.
fn detect_llama_accel(bin: &str) -> Option<bool> {
    let out = Command::new(bin).arg("--list-devices").output().ok()?;
    // If the command failed, assume we can't reliably interpret the output.
    if !out.status.success() {
        return None;
    }
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    let lc = text.to_ascii_lowercase();
    // Accelerator backend markers llama.cpp prints when a GPU device or backend is present.
    const ACCEL: &[&str] = &[
        "cuda", "metal", "vulkan", "rocm", "hip", "sycl", "cann", "musa", "opencl",
    ];
    if ACCEL.iter().any(|k| lc.contains(k)) {
        return Some(true);
    }
    // The flag was recognised (it printed a device listing) but no accelerator appeared → CPU-only.
    if lc.contains("available devices") {
        return Some(false);
    }
    None
}

/// Resolve the `llama-server` binary ProcessOS supervises: the operator-configured
/// `llama_bin` from settings, else `llama-server` on `PATH`.
fn check_llama(llama_bin: Option<&str>) -> Dependency {
    let bin = llama_bin
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("llama-server")
        .to_string();
    let probed = probe(&bin);
    let present = probed.is_some();
    let version = probed.filter(|s| !s.is_empty());

    // Only worth probing the backend when the binary is actually present.
    let accelerated = if present {
        detect_llama_accel(&bin)
    } else {
        None
    };
    // A non-blocking advisory when the present binary is a CPU-only build: local inference will be
    // slow and the `-ngl` GPU-offload default is a no-op until an accelerated build is installed.
    let note = if accelerated == Some(false) {
        Some(
            "llama-server appears to be a CPU-only build — local model inference will be slow. \
             Install a GPU-accelerated build (CUDA / Metal / Vulkan / ROCm) so GPU offload (-ngl) \
             takes effect."
                .to_string(),
        )
    } else {
        None
    };

    Dependency {
        id: "llama",
        name: "llama.cpp (llama-server)",
        purpose: "Running the local LLM sidecars that power investigations and chat.",
        bin,
        present,
        version,
        install_url: LLAMA_INSTALL_URL,
        hint: if present {
            String::new()
        } else {
            "`llama-server` was not found. Install llama.cpp (see the link) so it is on PATH, or set the llama binary path in Settings. Until then, local model sidecars cannot start."
                .to_string()
        },
        accelerated,
        note,
    }
}

/// Check every external dependency. `llama_bin` is the operator-configured path
/// from settings (or `None` to use `llama-server` on `PATH`).
pub fn check(llama_bin: Option<&str>) -> Vec<Dependency> {
    vec![check_node(), check_llama(llama_bin)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_resolves_a_present_binary() {
        // Every platform we target ships a shell; `sh` reliably exists on PATH.
        // (We only assert spawnability, not a particular version string.)
        assert!(Command::new("sh").arg("-c").arg("exit 0").output().is_ok());
    }

    #[test]
    fn probe_reports_missing_binary_as_none() {
        assert!(probe("definitely-not-a-real-binary-xyzzy-42").is_none());
    }

    #[test]
    fn check_returns_both_dependencies_with_install_links() {
        let deps = check(None);
        assert_eq!(deps.len(), 2);
        let node = deps.iter().find(|d| d.id == "node").expect("node entry");
        let llama = deps.iter().find(|d| d.id == "llama").expect("llama entry");
        assert!(node.install_url.starts_with("https://"));
        assert!(llama.install_url.starts_with("https://"));
        // A missing tool always carries an actionable hint; a present one does not.
        assert_eq!(node.present, node.hint.is_empty());
        assert_eq!(llama.present, llama.hint.is_empty());
    }

    #[test]
    fn check_llama_honours_configured_binary() {
        let dep = check_llama(Some("/no/such/llama-server"));
        assert_eq!(dep.bin, "/no/such/llama-server");
        assert!(!dep.present);
        assert!(!dep.hint.is_empty());
        // A missing binary is never probed for acceleration and carries no advisory note.
        assert_eq!(dep.accelerated, None);
        assert_eq!(dep.note, None);
    }

    #[test]
    fn accel_probe_is_unknown_for_a_missing_binary() {
        assert_eq!(
            detect_llama_accel("definitely-not-a-real-binary-xyzzy-42"),
            None
        );
    }

    #[test]
    fn parse_node_version_reads_major_minor() {
        assert_eq!(parse_node_version("v24.15.0\n"), Some((24, 15)));
        assert_eq!(parse_node_version("v22.6.1"), Some((22, 6)));
        assert_eq!(parse_node_version("v22"), Some((22, 0)));
    }

    #[test]
    fn parse_node_version_rejects_garbage() {
        assert_eq!(parse_node_version(""), None);
        assert_eq!(parse_node_version("not-a-version"), None);
        assert_eq!(parse_node_version("vx.y.z"), None);
    }

    /// The gate the supervised engine enforces: a Node below `NODE_MIN` must not
    /// count as usable, so ProcessOS never suppresses its install hint for a
    /// runtime the engine will refuse.
    #[test]
    fn node_min_gate_rejects_older_than_22_6() {
        let usable = |s| parse_node_version(s).is_some_and(|v| v >= NODE_MIN);
        assert!(!usable("v18.20.4"));
        assert!(!usable("v20.11.0"));
        assert!(!usable("v22.5.1")); // 22.5 lacks --experimental-strip-types
        assert!(usable("v22.6.0")); // the boundary
        assert!(usable("v22.7.0"));
        assert!(usable("v24.15.0"));
    }

    /// A present-but-too-old Node must read as *absent with an upgrade hint*, so
    /// the missing-dependency banner fires rather than a false "all good".
    #[test]
    fn too_old_a_node_is_not_selected() {
        let probed = [("node".to_string(), Some("v22.5.1".to_string()))];
        let (bin, version, ran) = select_node(&probed);
        assert_eq!(bin, None);
        assert_eq!(version, None);
        assert!(
            ran,
            "a spawned too-old Node must be remembered for the hint"
        );
    }

    /// A too-old candidate ahead of a new-enough one is skipped, not fatal — the
    /// new-enough one wins, with its own version reported.
    #[test]
    fn a_too_old_candidate_yields_to_a_newer_one() {
        let probed = [
            ("stale-node".to_string(), Some("v18.20.4".to_string())),
            ("node".to_string(), Some("v24.15.0".to_string())),
        ];
        let (bin, version, ran) = select_node(&probed);
        assert_eq!(bin.as_deref(), Some("node"));
        assert_eq!(version.as_deref(), Some("v24.15.0"));
        assert!(!ran);
    }

    /// The order is candidate order: the *first* new-enough Node wins.
    #[test]
    fn the_first_new_enough_candidate_wins() {
        let probed = [
            ("env-node".to_string(), Some("v22.6.0".to_string())),
            ("node".to_string(), Some("v24.15.0".to_string())),
        ];
        assert_eq!(select_node(&probed).0.as_deref(), Some("env-node"));
    }

    /// Nothing spawned at all → install hint, not upgrade hint.
    #[test]
    fn nothing_found_is_not_reported_as_too_old() {
        let probed = [("node".to_string(), None)];
        let (bin, version, ran) = select_node(&probed);
        assert_eq!(bin, None);
        assert_eq!(version, None);
        assert!(
            !ran,
            "a binary that never spawned is 'missing', not 'too old'"
        );
    }
}
