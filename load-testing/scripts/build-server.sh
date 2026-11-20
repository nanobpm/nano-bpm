#!/usr/bin/env bash
#
# build-server.sh — deterministic build of the nanobpmn gateway server WITH the
# embedded web console, with a hard verification gate.
#
# Why this exists
# ---------------
# `console` is a NON-default cargo feature, and the console SPA is embedded via
# rust-embed `#[folder = "../console/dist"]`. rust-embed embeds whatever is in
# that directory AT COMPILE TIME and does NOT error when it is missing or empty —
# so it is trivially easy to ship a binary that either lacks the console feature
# or embeds an empty asset bundle, and only discover it when `/console` 404s on a
# live node. This script removes that guesswork:
#
#   1. (re)builds the console frontend unless --skip-frontend
#   2. strips macOS junk (._*, .DS_Store) that pollutes the embed
#   3. asserts console/dist has a real index.html + hashed JS bundle
#   4. builds the server with --features console
#   5. VERIFIES the produced binary actually embeds the console bundle, and
#      FAILS LOUDLY if it does not
#   6. prints the binary path, sha256 and the embedded asset name
#
# Usage
#   load-testing/scripts/build-server.sh [--profile <name>] [--skip-frontend] [--stage <path>]
#
#   --profile <name>   cargo profile (default: deploy-fast; use `release` or `dev`)
#   --skip-frontend    do NOT run `npm run build` in console/ (use existing dist)
#   --stage <path>     copy the verified binary to <path> after building
#
# Env
#   CARGO   cargo binary to use (default: cargo, falls back to ~/.cargo/bin/cargo)
set -euo pipefail

PROFILE="deploy-fast"
SKIP_FRONTEND=0
STAGE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --skip-frontend) SKIP_FRONTEND=1; shift ;;
    --stage) STAGE="$2"; shift 2 ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Repo root = two levels up from this script (load-testing/scripts/..).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

CARGO="${CARGO:-cargo}"
command -v "$CARGO" >/dev/null 2>&1 || CARGO="$HOME/.cargo/bin/cargo"
command -v "$CARGO" >/dev/null 2>&1 || { echo "FATAL: cargo not found (set CARGO=...)" >&2; exit 1; }

DIST="console/dist"

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\n\033[1;31mFATAL: %s\033[0m\n' "$*" >&2; exit 1; }

# 1. Build the frontend unless skipped.
if [ "$SKIP_FRONTEND" -eq 0 ]; then
  say "Building console frontend (npm run build)"
  ( cd console && npm run build )
else
  say "Skipping frontend build (--skip-frontend); using existing $DIST"
fi

# 2. Strip macOS junk that rust-embed would otherwise embed as bogus assets.
say "Stripping macOS junk from $DIST"
find "$DIST" \( -name '._*' -o -name '.DS_Store' \) -print -delete 2>/dev/null || true

# 3. Assert the dist is real, not empty (the silent-empty-embed trap).
[ -f "$DIST/index.html" ] || die "$DIST/index.html missing — frontend not built. Run without --skip-frontend."
JS_BUNDLE="$(find "$DIST/assets" -name 'index-*.js' 2>/dev/null | head -1 || true)"
[ -n "$JS_BUNDLE" ] || die "no assets/index-*.js in $DIST — the embed would be empty."
JS_NAME="assets/$(basename "$JS_BUNDLE")"
say "Frontend OK: index.html + $JS_NAME ($(find "$DIST" -type f | wc -l | tr -d ' ') files)"

# 4. Build the server with the console feature.
say "Building server: profile=$PROFILE --features console"
"$CARGO" build --manifest-path server/Cargo.toml --profile "$PROFILE" --features console

# Locate the produced binary in Cargo's EFFECTIVE target dir (redirected for a
# spaced checkout, #1716) — never a guessed server/target that may be stale.
# shellcheck source=scripts/lib-cargo-target-dir.sh
. "$SCRIPT_DIR/../../scripts/lib-cargo-target-dir.sh"
SERVER_TARGET="$(cargo_target_dir server)" || die "cannot resolve server target dir"

BIN_SUBDIR="$PROFILE"
[ "$PROFILE" = "dev" ] && BIN_SUBDIR="debug"
BIN="$SERVER_TARGET/$BIN_SUBDIR/nanobpm-gateway-rest-server"
[ -x "$BIN" ] || die "expected binary not found at $BIN"

# 5. HARD VERIFICATION: for an OPTIMIZED build (the kind we deploy) the console
# bundle must actually be baked into the binary. In a debug build rust-embed reads
# console/dist from disk at runtime (no `debug-embed` feature here, see
# server/Cargo.toml), so there is nothing to find in the binary — verified on disk
# in step 3 instead.
case "$PROFILE" in
  dev|debug)
    say "Profile '$PROFILE' is a debug build — rust-embed serves the console from \
console/dist at runtime, not baked into the binary. Skipping the in-binary embed \
check (dist verified on disk above)."
    EMBED_HITS="n/a (debug: served from disk)"
    ;;
  *)
    say "Verifying the console is embedded in the binary"
    if command -v strings >/dev/null 2>&1; then
      EMBED_HITS="$(strings "$BIN" | grep -c 'assets/index-[A-Za-z0-9_]*\.js' || true)"
    else
      EMBED_HITS="$(grep -a -c 'assets/index-[A-Za-z0-9_]*\.js' "$BIN" || true)"
    fi
    [ "${EMBED_HITS:-0}" -ge 1 ] || die "console NOT embedded in $BIN (0 asset refs). \
Did the frontend build produce $DIST? Was --features console honored?"
    ;;
esac

# 6. Report.
if command -v sha256sum >/dev/null 2>&1; then
  SHA="$(sha256sum "$BIN" | cut -c1-16)"
else
  SHA="$(shasum -a 256 "$BIN" | cut -c1-16)"
fi
say "BUILD OK"
printf '  binary : %s\n' "$BIN"
printf '  sha256 : %s\n' "$SHA"
case "$PROFILE" in
  dev|debug) printf '  console: served from disk at runtime (%s)\n' "$JS_NAME" ;;
  *)         printf '  console: embedded (%s asset refs, %s)\n' "$EMBED_HITS" "$JS_NAME" ;;
esac

if [ -n "$STAGE" ]; then
  cp -f "$BIN" "$STAGE"
  say "Staged to $STAGE"
fi
