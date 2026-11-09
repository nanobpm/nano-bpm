#!/usr/bin/env bash
# build-tagged.sh — deterministic, hash-tagged server builds for fast bisection.
#
# Fetches a ref from origin, checks out the EXACT commit, regenerates the REST
# layer + console SPA from source (via the toolchain that bootstrap-build-host.sh
# installs), builds the gateway, and stores it under ~/builds/<shortsha>/ with
# metadata.json. Idempotent: an already-built commit is a no-op (--force to rebuild).
#
# Requires a provisioned build host — run scripts/bootstrap-build-host.sh once
# (installs Java, uv, Node, wasm-pack). No rsync, no shipped archives: the source
# comes straight from origin over the node's read-only deploy key.
#
# Because the OpenAPI codegen and the console SPA only change when spec/ / console/
# / engine-wasm/ change, the console/dist bundle is CACHED keyed by those trees'
# content hashes — so a server-only bisect rebuilds just the gateway crate.
#
# Usage: build-tagged.sh <ref> [--no-console] [--profile <p>] [--force] [--stage]
#   <ref>         git sha / branch / tag to build (required; e.g. 3f6e1f0, main, jwulf/x)
#   --no-console  build API-only gateway (skip codegen-of-frontend + embed)
#   --profile P   cargo profile (default: deploy-fast — matches deployed soak binaries)
#   --force       rebuild even if the tagged binary already exists
#   --stage       after build, copy the binary to ~/nano-gw-new (for stage-binary.sh)
#
# Final line is machine-parseable: "BUILD_OK <short> <binsha16> <path> (<secs>s)"
#                               or "CACHED  <short> <binsha16> <path>"
set -euo pipefail

REPO="$HOME/nano-bpm-src"
BUILDS="$HOME/builds"
DIST_CACHE="$HOME/build-cache/console-dist"
# shellcheck disable=SC1090
source "$HOME/.cargo/env" 2>/dev/null || true
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

REF=""; CONSOLE=1; PROFILE="deploy-fast"; FORCE=0; STAGE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --no-console) CONSOLE=0; shift ;;
    --console)    CONSOLE=1; shift ;;
    --profile)    PROFILE="$2"; shift 2 ;;
    --force)      FORCE=1; shift ;;
    --stage)      STAGE=1; shift ;;
    -h|--help)    sed -n '2,27p' "$0"; exit 0 ;;
    -*) echo "unknown arg: $1" >&2; exit 2 ;;
    *) REF="$1"; shift ;;
  esac
done
[ -n "$REF" ] || { echo "usage: build-tagged.sh <ref> [--no-console] [--profile P] [--force] [--stage]" >&2; exit 2; }
[ -d "$REPO/.git" ] || { echo "ERROR: repo not found at $REPO (clone git@github.com:nanobpm/nano-bpm.git)" >&2; exit 3; }

cd "$REPO"
git fetch --quiet --all --tags

# Resolve ref -> full commit sha (try as-is, then origin/<ref> for remote branches).
FULL=$(git rev-parse --verify --quiet "${REF}^{commit}" || true)
[ -n "$FULL" ] || FULL=$(git rev-parse --verify --quiet "origin/${REF}^{commit}" || true)
[ -n "$FULL" ] || { echo "ERROR: cannot resolve ref '$REF' to a commit" >&2; exit 4; }
SHORT=${FULL:0:12}
OUT="$BUILDS/$SHORT"
BIN="$OUT/nano-gw"

if [ "$FORCE" = 0 ] && [ -x "$BIN" ]; then
  echo "CACHED  $SHORT $(sha256sum "$BIN" | cut -c1-16) $BIN"
  if [ "$STAGE" = 1 ]; then cp -f "$BIN" "$HOME/nano-gw-new"; echo "STAGED ~/nano-gw-new <- $SHORT"; fi
  exit 0
fi

git checkout --quiet --force "$FULL"
git checkout --quiet -- . 2>/dev/null || true   # discard stray edits (build products are git-ignored)

echo "=== building $SHORT  profile=$PROFILE console=$CONSOLE ==="
git log -1 --format='    %h %s (%cI)' "$FULL"

# --- 1. Regenerate the REST layer (generated/ + server/src/stub_impls.rs) --------
echo "--- codegen (make generate) ---"
make -s generate

# --- 2. Console SPA (cached by content hash of every SPA source tree) ----------
if [ "$CONSOLE" = 1 ]; then
  KEY=$( { git rev-parse "$FULL:console" "$FULL:process-explorer" "$FULL:engine-wasm" "$FULL:spec"; } 2>/dev/null | sha256sum | cut -c1-16 )
  CACHED_DIST="$DIST_CACHE/$KEY"
  if [ -d "$CACHED_DIST" ]; then
    echo "--- console/dist cache HIT ($KEY) ---"
    rm -rf "$REPO/console/dist"; mkdir -p "$REPO/console/dist"
    cp -a "$CACHED_DIST/." "$REPO/console/dist/"
  else
    echo "--- console/dist cache MISS ($KEY) — building SPA ---"
    make -s console-frontend
    mkdir -p "$CACHED_DIST"
    cp -a "$REPO/console/dist/." "$CACHED_DIST/"
  fi
  # Force RustEmbed to re-embed the current console/dist.
  touch "$REPO/server/crates/nano-server-console/src/lib.rs"
fi

# --- 3. Build the gateway --------------------------------------------------------
FEATURES=""; [ "$CONSOLE" = 1 ] && FEATURES="--features console"
PDIR="$PROFILE"; [ "$PROFILE" = "dev" ] && PDIR="debug"
cd "$REPO/server"
START=$(date +%s)
# shellcheck disable=SC2086
cargo build --profile "$PROFILE" $FEATURES
END=$(date +%s)
BUILT="target/$PDIR/nanobpm-gateway-rest-server"
[ -x "$BUILT" ] || { echo "ERROR: expected binary not found at server/$BUILT" >&2; exit 5; }

mkdir -p "$OUT"
cp -f "$BUILT" "$BIN"
BSHA=$(sha256sum "$BIN" | cut -c1-16)
cd "$REPO"
SUBJ=$(git log -1 --format=%s "$FULL" | sed 's/\\/\\\\/g; s/"/\\"/g')
cat > "$OUT/metadata.json" <<EOF
{
  "commit": "$FULL",
  "short": "$SHORT",
  "ref": "$REF",
  "subject": "$SUBJ",
  "commit_date": "$(git log -1 --format=%cI "$FULL")",
  "profile": "$PROFILE",
  "console": $CONSOLE,
  "rustc": "$(rustc --version)",
  "binary_sha256_16": "$BSHA",
  "built_at": "$(date -u +%FT%TZ)",
  "build_secs": $((END - START))
}
EOF

echo "BUILD_OK $SHORT $BSHA $BIN ($((END - START))s)"
if [ "$STAGE" = 1 ]; then cp -f "$BIN" "$HOME/nano-gw-new"; echo "STAGED ~/nano-gw-new <- $SHORT"; fi
