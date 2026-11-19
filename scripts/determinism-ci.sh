#!/usr/bin/env bash
# Determinism CI (ADR 0066 slice 6, #1665).
#
# Re-executes the command corpus (engine-core/examples/determinism/) and
# compares the generated records + canonical state checkpoints across:
#   * hash seeds and process restarts  — `cell` (several fresh processes, each
#     with its own random std hash seed, plus snapshot-restart splits);
#   * build profiles and platforms      — `cell` per matrix entry, then
#     `compare` over the per-cell digests;
#   * the previous release              — `prev-release`;
# and proves the gate can fail:
#   * `canary` — a seeded HashMap-iteration non-determinism MUST be detected.
#
# Usage:
#   scripts/determinism-ci.sh cell OUTDIR [debug|release]
#   scripts/determinism-ci.sh compare DIGEST_FILE...
#   scripts/determinism-ci.sh canary OUTDIR
#   scripts/determinism-ci.sh prev-release OUTDIR [TAG]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE="$ROOT/engine-core"
RUNS="${DET_RUNS:-3}"
STATE_EVERY="${DET_STATE_EVERY:-50}"
RANDOM_INSTANCES="${DET_RANDOM:-400}"

sha() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

# build PROFILE — builds both examples in ENGINE (cwd-independent).
build() {
  local flag=()
  [ "$1" = release ] && flag=(--release)
  (cd "$ENGINE" && cargo build --features serde ${flag[@]+"${flag[@]}"} --example determinism_corpus --example determinism_replay)
  BIN="$ENGINE/target/$1/examples"
}

emit() { "$BIN/determinism_corpus" emit "$1" --random "$RANDOM_INSTANCES"; }

cmd_cell() {
  local out="$1" profile="${2:-debug}"
  mkdir -p "$out"
  build "$profile"
  emit "$out/corpus.jsonl"
  local total; total=$(grep -c . "$out/corpus.jsonl")
  local ref=""
  for n in $(seq 1 "$RUNS"); do
    "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/run$n.jsonl" --state-every "$STATE_EVERY"
    local d; d=$(sha "$out/run$n.jsonl")
    echo "run $n (fresh process / hash seed): $d"
    if [ -z "$ref" ]; then ref="$d"; elif [ "$d" != "$ref" ]; then
      echo "::error::determinism: run $n differs from run 1 (hash-seed / process non-determinism)"
      diff <(head -c 200000 "$out/run1.jsonl") <(head -c 200000 "$out/run$n.jsonl") | head -20 || true
      exit 1
    fi
  done
  for split in 1 $((total / 3)) $((total / 2 + 1)); do
    "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/a.jsonl" --state-every "$STATE_EVERY" \
      --stop-after "$split" --snapshot-out "$out/snap.json"
    "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/b.jsonl" --state-every "$STATE_EVERY" \
      --resume "$out/snap.json" --start-at "$split"
    cat "$out/a.jsonl" "$out/b.jsonl" >"$out/restart.jsonl"
    local d; d=$(sha "$out/restart.jsonl")
    echo "restart at $split: $d"
    if [ "$d" != "$ref" ]; then
      echo "::error::determinism: process restart at entry $split diverges from an uninterrupted run"
      exit 1
    fi
  done
  printf '%s %s\n' "$(sha "$out/corpus.jsonl")" "$ref" >"$out/digest.txt"
  echo "cell digest: $(cat "$out/digest.txt")"
}

cmd_compare() {
  [ "$#" -ge 2 ] || { echo "compare needs >= 2 digest files" >&2; exit 2; }
  local ref; ref=$(cat "$1")
  for f in "$@"; do
    echo "$f: $(cat "$f")"
    if [ "$(cat "$f")" != "$ref" ]; then
      echo "::error::determinism: $f differs from $1 (profile/platform non-determinism)"
      exit 1
    fi
  done
  echo "all $# cells agree"
}

cmd_canary() {
  local out="$1"
  mkdir -p "$out"
  build debug
  emit "$out/corpus.jsonl"
  "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/c1.jsonl" --canary
  "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/c2.jsonl" --canary
  if [ "$(sha "$out/c1.jsonl")" = "$(sha "$out/c2.jsonl")" ]; then
    echo "::error::determinism: the seeded HashMap canary was NOT detected — the gate is blind"
    exit 1
  fi
  echo "canary non-determinism detected (gate is live)"
}

cmd_prev_release() {
  local out="$1" tag="${2:-}"
  mkdir -p "$out"
  if [ -z "$tag" ]; then
    git -C "$ROOT" fetch --quiet --tags origin || true
    tag=$(git -C "$ROOT" tag --list 'v[0-9]*' --sort=-v:refname | head -1)
  fi
  [ -n "$tag" ] || { echo "::error::no previous release tag found"; exit 1; }
  echo "previous release: $tag"
  build debug
  emit "$out/corpus.jsonl"
  "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/head.jsonl"

  local prev="$out/prev-src"
  rm -rf "$prev"
  git -C "$ROOT" worktree remove --force "$prev" 2>/dev/null || true
  git -C "$ROOT" worktree add --detach "$prev" "$tag" >/dev/null
  trap 'git -C "$ROOT" worktree remove --force "$prev" 2>/dev/null || true' EXIT
  # The previous release has no runner: build HEAD's (API-minimal) runner
  # against the previous release's engine.
  mkdir -p "$prev/engine-core/examples/determinism"
  cp "$ENGINE/examples/determinism_replay.rs" "$prev/engine-core/examples/"
  cp "$ENGINE/examples/determinism/runner.rs" "$prev/engine-core/examples/determinism/"
  (cd "$prev/engine-core" && cargo build --features serde --example determinism_replay)
  "$prev/engine-core/target/debug/examples/determinism_replay" "$out/corpus.jsonl" --out "$out/prev.jsonl"
  "$BIN/determinism_corpus" cross-release "$out/head.jsonl" "$out/prev.jsonl"
}

case "${1:-}" in
  cell) shift; cmd_cell "$@" ;;
  compare) shift; cmd_compare "$@" ;;
  canary) shift; cmd_canary "$@" ;;
  prev-release) shift; cmd_prev_release "$@" ;;
  *) sed -n '2,20p' "$0"; exit 2 ;;
esac
