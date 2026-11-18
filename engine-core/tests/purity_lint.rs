//! #1663 (ADR 0066 audit #2/#7/#11): the apply-crate purity lint actually fires.
//!
//! `engine-core/clippy.toml` bans host clock, host environment/entropy and the
//! platform float transcendentals via `disallowed-methods`. A mistyped path or
//! a broken config there silently stops the ban applying. This test builds a throwaway crate that uses
//! engine-core's real `clippy.toml`, seeds one violation per banned category,
//! and asserts clippy rejects each one — and that a clean control passes.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples)"
)]

use std::path::{Path, PathBuf};
use std::process::Command;

const CLIPPY_TOML: &str = include_str!("../clippy.toml");

fn seeded_crate(name: &str, body: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("purity_lint")
        .join(name);
    std::fs::create_dir_all(dir.join("src")).unwrap();
    std::fs::write(
        dir.join("Cargo.toml"),
        format!(
            "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[workspace]\n"
        ),
    )
    .unwrap();
    std::fs::write(dir.join("clippy.toml"), CLIPPY_TOML).unwrap();
    std::fs::write(dir.join("src/lib.rs"), body).unwrap();
    dir
}

fn clippy(dir: &Path) -> (bool, String) {
    let out = Command::new(env!("CARGO"))
        .args(["clippy", "--quiet", "--offline", "--", "-D", "warnings"])
        .current_dir(dir)
        .env("CARGO_TARGET_DIR", dir.join("target"))
        .output()
        .expect("run cargo clippy");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn lint_fires_on_seeded_violations() {
    let dir = seeded_crate(
        "purity_seeded",
        r#"
pub fn clock() -> std::time::SystemTime { std::time::SystemTime::now() }
pub fn mono() -> std::time::Instant { std::time::Instant::now() }
pub fn env() -> Option<String> { std::env::var("X").ok() }
pub fn ln(x: f64) -> f64 { x.ln() }
pub fn exp(x: f64) -> f64 { x.exp() }
pub fn pow(a: f64, b: f64) -> f64 { a.powf(b) }
"#,
    );
    let (ok, stderr) = clippy(&dir);
    assert!(!ok, "seeded violations must fail clippy:\n{stderr}");
    for banned in [
        "std::time::SystemTime::now",
        "std::time::Instant::now",
        "std::env::var",
        "f64::ln",
        "f64::exp",
        "f64::powf",
    ] {
        assert!(
            stderr.contains(&format!("use of a disallowed method `{banned}`")),
            "{banned} not flagged:\n{stderr}"
        );
    }
}

#[test]
fn clean_control_passes() {
    let dir = seeded_crate(
        "purity_clean",
        "pub fn add(a: f64, b: f64) -> f64 { a + b }\n",
    );
    let (ok, stderr) = clippy(&dir);
    assert!(ok, "clean crate must pass clippy:\n{stderr}");
}
