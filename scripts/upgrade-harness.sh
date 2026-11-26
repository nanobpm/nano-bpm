#!/usr/bin/env bash
# Mixed-binary upgrade harness for capability latches (#2052).
#
# Capability latches (`message-correlation-ack-v1` #1949, and later ack-v2 /
# event-gateway-runtime) are unit-tested by SIMULATING capability sets with the
# current binary. This harness runs a REAL 3-node Raft cluster that mixes the
# previous released server binary with the current build and checks, per latch:
#   1. before the latch: no latch is recorded while an old member is present,
#      and every replica applies every entry (replica states are identical);
#   2. rolling upgrade: the latch is recorded exactly once per partition, only
#      after every member runs the new binary;
#   3. recovery: a member restarted from snapshot + journal on both sides of the
#      latch converges to its peers' state;
#   4. rollback refusal: an old binary started on post-latch data refuses to
#      serve it (latches are one-way — rollback means restoring a pre-latch
#      backup, see USERGUIDE.md "Rolling upgrades and capability latches").
#
# The scenarios live in `server/tests/upgrade_e2e.rs` (ignored unless
# NANO_UPGRADE_OLD_BIN is set); this script only provides the binaries.
#
# Usage:
#   scripts/upgrade-harness.sh old-ref FEATURE        # newest v* tag lacking FEATURE
#   scripts/upgrade-harness.sh build-old OUTDIR [TAG] # build TAG's server into OUTDIR/bin
#   scripts/upgrade-harness.sh run OUTDIR [TAG]       # build-old + run the scenarios
#
# OUTDIR/bin/<tag>/nanobpm-gateway-rest-server is reused when present, so CI
# caches OUTDIR/bin keyed by the tag.
set -euo pipefail
# Command substitutions ($(cmd_build_old ...)) run with errexit OFF unless this
# is set: without it a failed build could fall through and cache a stale binary.
shopt -s inherit_errexit

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME=nanobpm-gateway-rest-server
# The latch under test. The old binary must predate it, otherwise the "before
# the latch" scenario is vacuous; `old-ref` derives the tag from git so it never
# needs a hand-maintained pin.
FEATURE="${NANO_UPGRADE_FEATURE:-message-correlation-ack-v1}"
FEATURES_SRC=server/crates/nano-falcon-protocol/src/cluster_join.rs

# shellcheck source=scripts/lib-cargo-target-dir.sh
. "$ROOT/scripts/lib-cargo-target-dir.sh"

cmd_old_ref() {
  local feature="$1" tag
  git -C "$ROOT" fetch --quiet --tags origin 2>/dev/null || true
  for tag in $(git -C "$ROOT" tag --list 'v[0-9]*' --sort=-v:refname); do
    git -C "$ROOT" merge-base --is-ancestor "$tag" HEAD 2>/dev/null || continue
    if ! git -C "$ROOT" show "$tag:$FEATURES_SRC" 2>/dev/null | grep -qF "\"$feature\""; then
      echo "$tag"
      return 0
    fi
  done
  echo "::error::no release tag predates capability '$feature'" >&2
  return 1
}

cmd_build_old() {
  local out="$1" tag="${2:-}"
  [ -n "$tag" ] || tag=$(cmd_old_ref "$FEATURE")
  mkdir -p "$out"
  out=$(cd "$out" && pwd -P)
  case "$out" in
    *" "*) echo "::error::OUTDIR contains a space ($out); tikv-jemalloc-sys cannot build (#1716)" >&2; exit 1 ;;
  esac
  local dest="$out/bin/$tag/$BIN_NAME"
  if [ -x "$dest" ]; then
    echo "old binary ($tag) cached at $dest" >&2
    echo "$dest"
    return 0
  fi
  local src="$out/old-src"
  git -C "$ROOT" worktree remove --force "$src" 2>/dev/null || true
  rm -rf "$src"
  git -C "$ROOT" worktree add --detach "$src" "$tag" >&2
  # Isolated target dir: a shared absolute CARGO_TARGET_DIR would let the old
  # build overwrite HEAD's binary (same trap as determinism-ci.sh prev-release).
  local target="$out/old-target"
  mkdir -p "$target" "$src/console/dist"
  [ -f "$src/console/dist/index.html" ] || echo stub > "$src/console/dist/index.html"
  # The old release resolves its generator JAR under its own PROJECT_ROOT, so
  # seed it with HEAD's cached JARs (restored by CI); the old release selects
  # and checksum-verifies its pinned version, so a Maven outage cannot fail it.
  if compgen -G "$ROOT/build/tools/openapi-generator-cli-*.jar" >/dev/null; then
    mkdir -p "$src/build/tools"
    cp -n "$ROOT"/build/tools/openapi-generator-cli-*.jar "$src/build/tools/"
  fi
  (cd "$src" && make generate) >&2
  (export CARGO_TARGET_DIR="$target"; cd "$src/server" && cargo build --bin "$BIN_NAME") >&2
  local built; built=$(cd "$src/server" && CARGO_TARGET_DIR="$target" cargo_target_dir Cargo.toml cargo)
  mkdir -p "$(dirname "$dest")"
  cp "$built/debug/$BIN_NAME" "$dest"
  git -C "$ROOT" worktree remove --force "$src" >&2 || true
  echo "$dest"
}

cmd_run() {
  local out="$1" tag="${2:-}"
  [ -n "$tag" ] || tag=$(cmd_old_ref "$FEATURE")
  echo "old release for '$FEATURE': $tag" >&2
  local old; old=$(cmd_build_old "$out" "$tag")
  (cd "$ROOT/server" && NANO_UPGRADE_OLD_BIN="$old" NANO_UPGRADE_OLD_TAG="$tag" \
    NANO_UPGRADE_FEATURE="$FEATURE" \
    cargo test --test upgrade_e2e -- --include-ignored --test-threads=1 --nocapture)
}

case "${1:-}" in
  old-ref) shift; cmd_old_ref "${1:-$FEATURE}" ;;
  build-old) shift; cmd_build_old "$@" ;;
  run) shift; cmd_run "$@" ;;
  *) sed -n '2,27p' "$0"; exit 2 ;;
esac
