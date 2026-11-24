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

/// Closes every pipe this test process **inherited** beyond stdin/stdout/stderr
/// (#1779). Runs before `main` (see [`CLOSE_INHERITED_PIPES`]), when no test code
/// has opened anything yet, so every pipe found at fd >= 3 is a stray.
///
/// Root cause it removes: on macOS the test runner (nextest) creates each test's
/// stdout/stderr pipe with `pipe()` and only then sets `FD_CLOEXEC`, so a
/// sibling test process spawned in that window inherits the pipe's write end.
/// These harnesses live for a second or more and spawn a server (which inherits
/// it again), so the stray kept the *other* test's stdout open long after that
/// test exited, and nextest failed it with `LEAK-FAIL` under parallel load. The
/// harness cannot fix the runner, but it can refuse to hold handles it never
/// asked for. Only FIFOs are closed: that is the leaking class, and leaving
/// other inherited descriptors alone keeps the sanitiser narrow.
#[cfg(unix)]
extern "C" fn close_inherited_pipes() {
    // SAFETY: plain descriptor-table syscalls on fds we do not otherwise use;
    // nothing in this process has opened a descriptor yet (pre-main).
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let max_fd = if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            limit.rlim_cur.min(65_536) as libc::c_int
        } else {
            1_024
        };
        for fd in 3..max_fd {
            let mut stat: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut stat) == 0 && (stat.st_mode & libc::S_IFMT) == libc::S_IFIFO {
                libc::close(fd);
            }
        }
    }
}

/// Registers [`close_inherited_pipes`] as a pre-`main` initializer of every e2e
/// harness binary that includes this module.
#[cfg(target_vendor = "apple")]
#[used]
#[unsafe(link_section = "__DATA,__mod_init_func")]
static CLOSE_INHERITED_PIPES: extern "C" fn() = close_inherited_pipes;

/// Registers [`close_inherited_pipes`] as a pre-`main` initializer of every e2e
/// harness binary that includes this module.
#[cfg(all(unix, not(target_vendor = "apple")))]
#[used]
#[unsafe(link_section = ".init_array")]
static CLOSE_INHERITED_PIPES: extern "C" fn() = close_inherited_pipes;
