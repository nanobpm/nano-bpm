#!/usr/bin/env bash
# cross-build.sh — cross-compile the gateway (nanobpm-gateway-rest-server) for a
# Linux or Windows target locally, mirroring the CI recipe in
# .github/workflows/publish-c8ctl-binaries.yml.
#
# Two backends, picked automatically from the target triple:
#   * Linux (`*-unknown-linux-gnu*`): cargo-zigbuild + a glibc floor. zig provides
#     the cross C compiler/linker for the bundled C deps (rusqlite, jemalloc-sys)
#     and pins a minimum glibc so the `-gnu` binaries run on older distros. This
#     lets you build for e.g. a Linux x86-64 box or a Raspberry Pi (ARMv7) from
#     any host (macOS/Linux) without Docker or a per-target gcc.
#   * Windows (`*-pc-windows-msvc`): cargo-xwin, which downloads the MSVC CRT +
#     Windows SDK import libs and links with lld — so you get the SAME msvc ABI
#     the released `.exe` uses (CI builds it natively on a windows-2022 runner),
#     cross-compiled from macOS/Linux. `--glibc` is ignored for Windows.
#
# It does NOT regenerate the REST layer or the web console: the git-ignored
# generated/ + server/src/stub_impls.rs must already exist (plus generated-console/
# and console/dist for the default console build; --no-console needs neither). The
# Makefile `cross-*` targets share `release`'s prerequisites, so
# `make cross-linux-x64` / `make cross-windows` build those for you first — prefer
# the make targets.
#
# Prerequisites (verified below, with install hints):
#   * rustup + the target's std (auto-added via `rustup target add`)
#   * Linux targets:   zig (brew install zig) + cargo-zigbuild (cargo install cargo-zigbuild)
#   * Windows targets: cargo-xwin (cargo install cargo-xwin) + an `lld-link`
#                      linker on PATH, which cargo-xwin invokes for the msvc
#                      target (brew install llvm | apt-get install lld clang)
#
# Usage:
#   scripts/cross-build.sh <target-triple> [--glibc <ver>] [--no-console] [--out <path>]
#
#   <target-triple>  e.g. x86_64-unknown-linux-gnu, armv7-unknown-linux-gnueabihf,
#                    aarch64-unknown-linux-gnu, arm-unknown-linux-gnueabihf,
#                    x86_64-pc-windows-msvc
#   --glibc <ver>    minimum glibc to link against (default: 2.31 = Debian 11
#                    "bullseye" / Ubuntu 20.04 — matches CI). Use "" to disable.
#                    Ignored for Windows targets.
#   --no-console     build the API-only gateway (skip the embedded web console);
#                    faster, and does not require console/dist.
#   --console        build with the embedded web console (the default; provided
#                    as the explicit inverse of --no-console).
#   --out <path>     output path for the staged binary (default:
#                    dist/nanobpm-gateway-rest-server-<os>-<arch>). A relative
#                    path is resolved against the current directory.
#
# Final line is machine-parseable: "BUILD_OK <target> <path> (<secs>s)".
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_NAME="nanobpm-gateway-rest-server"

die() { echo "cross-build: $*" >&2; exit 1; }
# Guard optional-value flags so a missing value fails with a clear message rather
# than a cryptic `set -u` "$2: unbound variable". Call as `need_val "$@"`.
need_val() { [ $# -ge 2 ] || die "flag $1 needs a value (e.g. $1 <value>)"; }

# Wrap the entire body in main() so bash parses the whole script before running
# any of it. Bash reads a script by byte offset as it executes; without this,
# editing the file on disk while a long `cargo` build is in flight would make
# bash resume at a stale offset afterwards and fail with a spurious
# "syntax error near unexpected token". A fully-parsed main() is edit-safe.
main() {
TARGET=""; GLIBC="2.31"; CONSOLE=1; OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --glibc)      need_val "$@"; GLIBC="$2"; shift 2 ;;
    --no-console) CONSOLE=0; shift ;;
    --console)    CONSOLE=1; shift ;;
    --out)        need_val "$@"; OUT="$2"; shift 2 ;;
    -h|--help)    awk 'NR==1{next} /^#/{print; next} {exit}' "$0"; exit 0 ;;
    -*)           die "unknown flag: $1" ;;
    *)            if [ -z "$TARGET" ]; then TARGET="$1"; else die "unexpected arg: $1"; fi; shift ;;
  esac
done
[ -n "$TARGET" ] || die "missing <target-triple> (e.g. x86_64-unknown-linux-gnu). See --help."

# Windows targets use a different backend (cargo-xwin, msvc ABI) and produce a
# `.exe`. Only the msvc target is supported here; a `-gnu` Windows triple would
# need a MinGW toolchain (not cargo-xwin), so reject it with a clear message
# rather than routing it through cargo-xwin and failing cryptically.
case "$TARGET" in
  *-pc-windows-msvc) WINDOWS=1 ;;
  *-pc-windows-gnu*) die "$TARGET is a MinGW (gnu) target, which cargo-xwin can't build. Use the msvc triple instead, e.g. ${TARGET%-gnu*}-msvc." ;;
  *)                 WINDOWS=0 ;;
esac

# Map the Rust triple to the CI asset name so local artifacts match the released
# ones (nanobpm-gateway-rest-server-{linux-x64,linux-arm64,…,win32-x64.exe}).
asset_for() {
  case "$1" in
    x86_64-unknown-linux-gnu)       echo "$BIN_NAME-linux-x64" ;;
    aarch64-unknown-linux-gnu)      echo "$BIN_NAME-linux-arm64" ;;
    armv7-unknown-linux-gnueabihf)  echo "$BIN_NAME-linux-armv7" ;;
    arm-unknown-linux-gnueabihf)    echo "$BIN_NAME-linux-armv6" ;;
    x86_64-pc-windows-msvc)         echo "$BIN_NAME-win32-x64.exe" ;;
    aarch64-pc-windows-msvc)        echo "$BIN_NAME-win32-arm64.exe" ;;
    *)                              echo "$BIN_NAME-$1$([ "$WINDOWS" = 1 ] && echo .exe)" ;;
  esac
}
[ -n "$OUT" ] || OUT="$PROJECT_ROOT/dist/$(asset_for "$TARGET")"

# --- toolchain checks -------------------------------------------------------
command -v rustup  >/dev/null 2>&1 || die "rustup not found — install Rust from https://rustup.rs"
if [ "$WINDOWS" = 1 ]; then
  command -v cargo-xwin >/dev/null 2>&1 || die "cargo-xwin not found — 'cargo install cargo-xwin'"
  # cargo-xwin links msvc targets with lld-link; without it the build fails deep
  # in the link step with an opaque error, so check for it up front.
  command -v lld-link >/dev/null 2>&1 || die "lld-link not found on PATH — cargo-xwin needs it to link the msvc target ('brew install llvm' | 'apt-get install lld clang')."
else
  command -v zig     >/dev/null 2>&1 || die "zig not found — 'brew install zig' or https://ziglang.org/download"
  command -v cargo-zigbuild >/dev/null 2>&1 || die "cargo-zigbuild not found — 'cargo install cargo-zigbuild'"
fi

if ! rustup target list --installed 2>/dev/null | grep -qx "$TARGET"; then
  echo "cross-build: adding rust std for $TARGET (rustup target add $TARGET)"
  rustup target add "$TARGET"
fi

# --- prerequisite sources (git-ignored; produced by codegen + console build) ---
[ -f "$PROJECT_ROOT/generated/Cargo.toml" ]     || die "generated/ missing — run 'make generate' (or use the 'make cross-*' targets)."
[ -f "$PROJECT_ROOT/server/src/stub_impls.rs" ] || die "server/src/stub_impls.rs missing — run 'make generate'."
if [ "$CONSOLE" = 1 ]; then
  [ -d "$PROJECT_ROOT/console/dist" ]                    || die "console/dist missing — run 'make console-frontend' (or use 'make cross-*'; or pass --no-console)."
  [ -f "$PROJECT_ROOT/generated-console/Cargo.toml" ]   || die "generated-console/ missing — run 'make generate'."
fi

# The `.<glibc>` suffix pins the minimum glibc; zigbuild still writes to the
# unsuffixed target dir (server/target/<triple>/release/). Windows (cargo-xwin)
# has no glibc floor and builds the plain triple.
zig_target="$TARGET"
[ "$WINDOWS" = 0 ] && [ -n "$GLIBC" ] && zig_target="$TARGET.$GLIBC"

features=(--bin "$BIN_NAME")
if [ "$CONSOLE" = 1 ]; then
  features=(--features console "${features[@]}")
  # Force the RustEmbed derive to re-run so the current console/dist is baked in,
  # even if the gateway sources are otherwise unchanged (mirrors `make release`).
  touch "$PROJECT_ROOT/server/crates/nano-server-console/src/lib.rs"
fi

# Archive with zig's (llvm) ar/ranlib, not the host's. jemalloc-sys runs its own
# autoconf build, which calls plain `ar crus` / `ranlib` (it reads `AR`/`RANLIB`,
# not cc-rs's per-target `AR_<triple>`). On macOS that is Apple's ar, which
# silently drops the ELF members and leaves a 96-byte `libjemalloc.a`, so the
# final link fails with `undefined symbol: _rjem_malloc` etc. A warm target/
# masks this (the archive was built before the host toolchain changed); a fresh
# worktree or clean build hits it. llvm-ar handles host (Mach-O/ELF) objects
# too, so build scripts compiled for the host are unaffected.
if [ "$WINDOWS" = 0 ]; then
  zig_tools="$PROJECT_ROOT/server/target/zig-tools"
  mkdir -p "$zig_tools"
  printf '#!/bin/sh\nexec zig ar "$@"\n' >"$zig_tools/ar"
  printf '#!/bin/sh\nexec zig ranlib "$@"\n' >"$zig_tools/ranlib"
  chmod +x "$zig_tools/ar" "$zig_tools/ranlib"
  export AR="$zig_tools/ar" RANLIB="$zig_tools/ranlib"

  # armv6: link the ARM-RTABI unaligned-access shim (see the sourced script).
  # shellcheck source=scripts/zig-arm-shim-env.sh
  . "$SCRIPT_DIR/zig-arm-shim-env.sh"

  # Self-heal a target dir poisoned before this fix: cargo won't re-run the
  # jemalloc build script for an env change, so drop any empty archive's build
  # dir and let it rebuild. A real libjemalloc.a is megabytes.
  for archive in "$PROJECT_ROOT"/server/target/"$TARGET"/release/build/tikv-jemalloc-sys-*/out/lib/libjemalloc.a; do
    [ -f "$archive" ] || continue
    if [ "$(wc -c <"$archive")" -lt 1024 ]; then
      stale="${archive%/out/lib/libjemalloc.a}"
      echo "cross-build: removing empty jemalloc archive build ($stale)"
      rm -rf "$stale"
    fi
  done
fi

if [ "$WINDOWS" = 1 ]; then
  echo "cross-build: $BIN_NAME -> $TARGET (backend: cargo-xwin/msvc, console: $CONSOLE)"
else
  echo "cross-build: $BIN_NAME -> $TARGET (backend: cargo-zigbuild, glibc floor: ${GLIBC:-none}, console: $CONSOLE)"
fi
start=$(date +%s)
(
  cd "$PROJECT_ROOT/server"
  if [ "$WINDOWS" = 1 ]; then
    cargo xwin build --release --target "$zig_target" "${features[@]}"
  else
    cargo zigbuild --release --target "$zig_target" "${features[@]}"
  fi
)
secs=$(( $(date +%s) - start ))

bin_file="$BIN_NAME"
[ "$WINDOWS" = 1 ] && bin_file="$BIN_NAME.exe"
built="$PROJECT_ROOT/server/target/$TARGET/release/$bin_file"
[ -f "$built" ] || die "expected binary not found at $built"
mkdir -p "$(dirname "$OUT")"
cp "$built" "$OUT"

echo "cross-build: staged $(command -v file >/dev/null 2>&1 && file -b "$OUT" || echo "$OUT")"
echo "cross-build: -> $OUT"
echo "BUILD_OK $TARGET $OUT (${secs}s)"
}

main "$@"
