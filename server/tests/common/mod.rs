//! Shared helpers for the `tests/*.rs` end-to-end harnesses.
//!
//! Each harness is its own integration-test crate, so this `common/` directory is
//! a Rust module each one includes with `mod common;` — there is one canonical
//! implementation of the server-spawn stdio/process-group defaults, not three
//! copies that can drift apart (#1474).

// This module is compiled into every e2e harness that includes it, so it is a
// non-#[test] helper in a tests/ target: clippy.toml's `allow-unwrap-in-tests`
// does not reach it (same gap the harnesses document), so allow it here too.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration-test helper module: clippy.toml allow-unwrap-in-tests does not cover \
non-#[test] code under tests/, including shared tests/common modules"
)]

use std::process::{Command, Stdio};

/// Path to the compiled server binary, provided by Cargo for integration tests.
pub const SERVER_BIN: &str = env!("CARGO_BIN_EXE_nanobpm-gateway-rest-server");

/// Builds a `Command` for the server binary with the shared spawn defaults every
/// e2e harness uses:
///
/// * **stdin → null.** Input isolation: the server must not read the test
///   runner's stdin, so a stray read can never consume or block on the runner's
///   input. (This is about isolating the child from the runner's input stream,
///   not about stdout/stderr — the concern nextest's `leak-timeout` guards.)
/// * **stdout → piped**, so the harness can read the `LISTENING_PORT=<n>` line
///   the server prints once it has bound.
/// * **stderr → null**, so server diagnostics never interleave with test output.
/// * **its own process group** (Unix), so the harness can `kill(-pid, …)` the
///   whole tree — the server and anything it spawns — on drop.
///
/// Callers layer their env vars on top, then `.spawn()`.
pub fn server_command() -> Command {
    let mut command = Command::new(SERVER_BIN);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command
}
