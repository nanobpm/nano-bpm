//! Shared build-script helper that stamps a crate's reported gateway version.
//!
//! The version is the single source of truth for a release: CI exports
//! `NANOBPM_VERSION` from the pushed tag; locally we fall back to `git describe`,
//! then the compiling crate's `Cargo.toml` version. A leading `v` is stripped so
//! `v1.2.3` -> `1.2.3`.
//!
//! Both the gateway binary and the extracted `nano-server-console` crate call
//! [`emit`] from their `build.rs`, so the derivation lives in exactly one place
//! and their `env!("NANOBPM_VERSION")` values can never drift (ADR 0064).

use std::env;
use std::path::Path;
use std::process::Command;

/// Emits the `cargo:` directives that stamp `NANOBPM_VERSION` for the compiling
/// crate and watch the git state the fallback derivation reads. Call this once
/// from a crate's `build.rs` `main`.
pub fn emit() {
    emit_with(resolve_version());
}

/// Placeholder [`emit_cluster_identity`] stamps when neither `NANOBPM_VERSION`
/// nor git metadata names the build. Deliberately NOT a `MAJOR.MINOR.PATCH`
/// release tag, so a cluster-join version check treats it as a dev build.
pub const UNSTAMPED_VERSION: &str = "unstamped";

/// Like [`emit`], but for a crate whose stamp is a **cluster identity** (the
/// #1702 determinism upgrade-boundary check): it never falls back to the
/// compiling crate's own `CARGO_PKG_VERSION`, which is a crate version, not
/// the gateway release, and would masquerade as a release tag (e.g. a source
/// tarball build advertising `0.0.16`). Without an explicit or git-derived
/// version it stamps [`UNSTAMPED_VERSION`] instead.
pub fn emit_cluster_identity() {
    emit_with(cluster_identity_version(resolve_explicit_or_git()));
}

/// The cluster-identity stamp for an explicit/git-derived version (pure, so
/// the no-`CARGO_PKG_VERSION`-fallback rule is unit-testable).
pub fn cluster_identity_version(explicit_or_git: Option<String>) -> String {
    explicit_or_git.unwrap_or_else(|| UNSTAMPED_VERSION.to_string())
}

fn emit_with(version: String) {
    println!("cargo:rerun-if-env-changed=NANOBPM_VERSION");
    // Emitting *any* `rerun-if-*` opts this build script out of Cargo's default
    // "rerun when any package file changes" heuristic, so unless we say otherwise
    // Cargo caches the stamped version across `git pull`/`checkout`. That is how a
    // binary built from fresh `main` can still report an old commit (observed on a
    // cross-build whose reported gateway version lagged the code by weeks). Watch
    // the git state that `git describe` reads so the stamp re-derives on every HEAD
    // movement: `logs/HEAD` is appended on each commit/checkout/reset, and `HEAD`
    // itself changes on branch switch. `git rev-parse --git-path` resolves these to
    // the real files even with packed refs or a linked worktree (where `.git` is a
    // file). When git is unavailable (release tarball), no paths are emitted and the
    // explicit `NANOBPM_VERSION` env — already watched above — governs.
    for git_ref in ["logs/HEAD", "HEAD"] {
        if let Some(path) = git_path(git_ref).filter(|p| Path::new(p).exists()) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    println!("cargo:rustc-env=NANOBPM_VERSION={version}");
}

/// Path to a file inside the git dir (e.g. `HEAD`, `logs/HEAD`), resolved via
/// `git rev-parse --git-path` so it is correct for packed refs and linked
/// worktrees. The path may be relative (git resolves it against the current
/// directory, which we pin to `CARGO_MANIFEST_DIR`); callers here consume it the
/// same way — `Path::exists` and `cargo:rerun-if-changed` both resolve relative
/// paths against that manifest dir — so it is deliberately not forced absolute.
/// `None` when git is absent or the command fails.
fn git_path(rel: &str) -> Option<String> {
    let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let out = Command::new("git")
        .args(["rev-parse", "--git-path", rel])
        .current_dir(&manifest)
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!path.is_empty()).then_some(path)
}

/// Resolves the reported version from `NANOBPM_VERSION`, then `git describe`,
/// then the compiling crate's `CARGO_PKG_VERSION`, stripping a leading `v`.
pub fn resolve_version() -> String {
    resolve_explicit_or_git()
        .unwrap_or_else(|| env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string()))
}

/// The version from `NANOBPM_VERSION` or `git describe` (leading `v`
/// stripped), or `None` when neither names the build.
pub fn resolve_explicit_or_git() -> Option<String> {
    if let Ok(v) = env::var("NANOBPM_VERSION") {
        let v = v.trim().trim_start_matches('v');
        if !v.is_empty() {
            return Some(v.to_string());
        }
    }
    let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty"])
        .current_dir(&manifest)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| {
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .trim_start_matches('v')
                .to_string()
        })
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_identity_never_falls_back_to_a_crate_version() {
        assert_eq!(cluster_identity_version(None), UNSTAMPED_VERSION);
        assert_eq!(
            cluster_identity_version(Some("0.0.28".into())),
            "0.0.28".to_string()
        );
        // The placeholder must not look like a MAJOR.MINOR.PATCH release.
        assert_ne!(UNSTAMPED_VERSION.split('.').count(), 3);
    }
}
