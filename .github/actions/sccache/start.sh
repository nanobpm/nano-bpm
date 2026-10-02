#!/usr/bin/env bash
# Start sccache and enable it as RUSTC_WRAPPER only if the server comes up
# (#1325). The cache is an accelerator, never a correctness dependency: when the
# GHA cache backend is unavailable (e.g. "ServerBusy: Egress is over the account
# limit"), sccache fails at server startup and, wired unconditionally, failed
# every rustc call. Here we degrade to an uncached build with a warning instead.
#
# Inputs (env): GITHUB_ENV (file to append job env to), SCCACHE (binary; default
# `sccache`, overridable for tests).
set -uo pipefail
sccache_bin="${SCCACHE:-sccache}"
: "${GITHUB_ENV:?GITHUB_ENV must be set}"

export SCCACHE_GHA_ENABLED=true
# Mid-build cache I/O errors fall back to plain rustc rather than failing.
export SCCACHE_IGNORE_SERVER_IO_ERROR=1

if out=$("$sccache_bin" --start-server 2>&1); then
  {
    echo "SCCACHE_GHA_ENABLED=true"
    echo "SCCACHE_IGNORE_SERVER_IO_ERROR=1"
    echo "RUSTC_WRAPPER=$sccache_bin"
  } >>"$GITHUB_ENV"
  echo "sccache server started; RUSTC_WRAPPER=$sccache_bin"
else
  # Single-line annotation: newlines would end the workflow command.
  echo "::warning title=sccache unavailable::building without a compiler cache: $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-500)"
fi
# CARGO_INCREMENTAL=0 either way: keeps cached and uncached builds identical.
echo "CARGO_INCREMENTAL=0" >>"$GITHUB_ENV"
exit 0
