#!/usr/bin/env bash
# Determinism CI (ADR 0066 slice 6, #1665).
#
# Re-executes the command corpus (engine-core/examples/determinism/) and
# compares the generated records + canonical state checkpoints across:
#   * hash seeds and process restarts  — `cell` (several fresh processes, each
#     with its own random std hash seed, plus snapshot-restart splits), over
#     both the pre-latch corpus and the post-latch (#2004) corpus;
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

# Resolve Cargo's EFFECTIVE target dir rather than guessing <crate>/target: a
# redirected CARGO_TARGET_DIR (a spaced worktree pins one via .cargo/config.toml,
# #1716) moves the examples, and the guess would read a stale/absent binary.
# shellcheck source=scripts/lib-cargo-target-dir.sh
. "$ROOT/scripts/lib-cargo-target-dir.sh"

sha() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

# build PROFILE — builds both examples in ENGINE (cwd-independent).
build() {
  local flag=()
  [ "$1" = release ] && flag=(--release)
  (cd "$ENGINE" && cargo build --features serde ${flag[@]+"${flag[@]}"} --example determinism_corpus --example determinism_replay)
  # Same cwd ($ENGINE) and manifest as the build above, so a relative
  # CARGO_TARGET_DIR and config discovery resolve exactly as they did for it.
  local target; target=$(cd "$ENGINE" && cargo_target_dir Cargo.toml cargo) || {
    echo "::error::cannot resolve engine-core effective target dir (#1716)"; exit 1;
  }
  BIN="$target/$1/examples"
}

emit() { "$BIN/determinism_corpus" emit "$1" --random "$RANDOM_INSTANCES"; }

# cell_mode OUTDIR [--post-latch] — one corpus through every same-release
# check (fresh processes / hash seeds, restart splits). Prints the corpus and
# run digests as "CORPUS_SHA RUN_SHA". `--post-latch` (#2004) generates the
# corpus under the `event-gateway-runtime-v1` envelope (keys harvested from
# the latched engine) and replays it with `--event-gateway-runtime`, so the
# gateway-owned trigger runtime is determinism-checked too.
cell_mode() {
  local out="$1" emit_flags=() replay_flags=()
  if [ "${2:-}" = --post-latch ]; then
    emit_flags=(--post-latch); replay_flags=(--event-gateway-runtime)
  fi
  mkdir -p "$out"
  "$BIN/determinism_corpus" emit "$out/corpus.jsonl" --random "$RANDOM_INSTANCES" ${emit_flags[@]+"${emit_flags[@]}"}
  local total; total=$(grep -c . "$out/corpus.jsonl")
  local ref=""
  for n in $(seq 1 "$RUNS"); do
    "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/run$n.jsonl" --state-every "$STATE_EVERY" \
      ${replay_flags[@]+"${replay_flags[@]}"}
    local d; d=$(sha "$out/run$n.jsonl")
    echo "$out run $n (fresh process / hash seed): $d" >&2
    if [ -z "$ref" ]; then ref="$d"; elif [ "$d" != "$ref" ]; then
      echo "::error::determinism ($out): run $n differs from run 1 (hash-seed / process non-determinism)" >&2
      diff <(head -c 200000 "$out/run1.jsonl") <(head -c 200000 "$out/run$n.jsonl") | head -20 >&2 || true
      exit 1
    fi
  done
  for split in 1 $((total / 3)) $((total / 2 + 1)); do
    "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/a.jsonl" --state-every "$STATE_EVERY" \
      --stop-after "$split" --snapshot-out "$out/snap.json" ${replay_flags[@]+"${replay_flags[@]}"}
    "$BIN/determinism_replay" "$out/corpus.jsonl" --out "$out/b.jsonl" --state-every "$STATE_EVERY" \
      --resume "$out/snap.json" --start-at "$split" ${replay_flags[@]+"${replay_flags[@]}"}
    cat "$out/a.jsonl" "$out/b.jsonl" >"$out/restart.jsonl"
    local d; d=$(sha "$out/restart.jsonl")
    echo "$out restart at $split: $d" >&2
    if [ "$d" != "$ref" ]; then
      echo "::error::determinism ($out): process restart at entry $split diverges from an uninterrupted run" >&2
      exit 1
    fi
  done
  printf '%s %s' "$(sha "$out/corpus.jsonl")" "$ref"
}

cmd_cell() {
  local out="$1" profile="${2:-debug}"
  mkdir -p "$out"
  build "$profile"
  # Pre-latch (the mixed-version / previous-release shape) and post-latch
  # (#2004) corpora; the cell digest covers both.
  local pre post
  pre=$(cell_mode "$out/pre-latch")
  post=$(cell_mode "$out/post-latch" --post-latch)
  printf '%s %s\n' "$pre" "$post" >"$out/digest.txt"
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

  PREV_SRC="$out/prev-src"
  git -C "$ROOT" worktree remove --force "$PREV_SRC" 2>/dev/null || true
  rm -rf "$PREV_SRC"
  git -C "$ROOT" worktree add --detach "$PREV_SRC" "$tag" >/dev/null
  trap 'git -C "$ROOT" worktree remove --force "$PREV_SRC" 2>/dev/null || true' EXIT
  # The previous release has no runner: build HEAD's (API-minimal) runner
  # against the previous release's engine.
  mkdir -p "$PREV_SRC/engine-core/examples/determinism"
  cp "$ENGINE/examples/determinism_replay.rs" "$PREV_SRC/engine-core/examples/"
  # Lines tagged `determinism:head-only` call head-only engine API (e.g. a
  # feature-gate switch, #2004) the previous release does not have.
  grep -v 'determinism:head-only' "$ENGINE/examples/determinism/runner.rs" \
    >"$PREV_SRC/engine-core/examples/determinism/runner.rs"
  # Build the previous release into an ISOLATED target dir, never the shared
  # one: under an absolute CARGO_TARGET_DIR (#1716) HEAD and the previous
  # release would resolve to the SAME directory, so this build would overwrite
  # HEAD's determinism_replay and both replays below would run the old binary —
  # the cross-release gate would pass without testing HEAD. Export the isolated
  # dir so the build and the resolution below see the same setting; `local`
  # keeps it scoped to this function. Resolve it ABSOLUTE and fail on a space:
  # the previous release's C dependencies (tikv-jemalloc-sys) cannot build under
  # a spaced target dir (#1716), and a relative one would resolve under
  # $PREV_SRC, not $out.
  local prev_target; prev_target=$(mkdir -p "$out/prev-target" && cd "$out/prev-target" && pwd -P)
  case "$prev_target" in
    *" "*) echo "::error::prev-release target dir contains a space ($prev_target); tikv-jemalloc-sys cannot build (#1716). Use a space-free OUTDIR."; exit 1 ;;
  esac
  (export CARGO_TARGET_DIR="$prev_target"; cd "$PREV_SRC/engine-core" && cargo build --features serde --example determinism_replay)
  # Resolve the previous-release binary with that same isolated target dir.
  prev_target=$(cd "$PREV_SRC/engine-core" && CARGO_TARGET_DIR="$prev_target" cargo_target_dir Cargo.toml cargo) || {
    echo "::error::cannot resolve previous-release effective target dir (#1716)"; exit 1;
  }
  local prev_bin="$prev_target/debug/examples/determinism_replay"

  # Probe which entries the old build cannot decode (newer command variants
  # or model shapes), drop them, then replay the same filtered corpus on both.
  # Regenerate (not line-filter) the corpus without them so later keys are
  # re-harvested; it fails if a kept entry's outcome would change. State
  # checkpoints are compared too whenever the snapshot format matches.
  #
  # Every dropped entry must be EXPLAINED by the additive set derived from
  # the golden corpus — vocabulary(golden @ HEAD) − vocabulary(golden @ $tag)
  # — and the kept share must clear the coverage floor; behavioural changes
  # are compared against the ratified divergence allowlist, which expires
  # once $tag is at or past each entry's introducing release (#1698).
  local prev_golden="$out/prev-golden"
  rm -rf "$prev_golden"
  mkdir -p "$prev_golden"
  git -C "$ROOT" archive "$tag" engine-core/tests/fixtures/golden | tar -x -C "$prev_golden"
  local golden=(--golden-head "$ENGINE/tests/fixtures/golden"
    --golden-prev "$prev_golden/engine-core/tests/fixtures/golden")
  "$prev_bin" "$out/corpus.jsonl" --out "$out/probe.jsonl" --skip-undecodable
  "$BIN/determinism_corpus" emit-compatible "$out/probe.jsonl" "$out/compat.jsonl" \
    --random "$RANDOM_INSTANCES" "${golden[@]}"
  "$prev_bin" "$out/compat.jsonl" --out "$out/prev.jsonl" --state-every "$STATE_EVERY"
  "$BIN/determinism_replay" "$out/compat.jsonl" --out "$out/head.jsonl" --state-every "$STATE_EVERY"
  "$BIN/determinism_corpus" cross-release "$out/head.jsonl" "$out/prev.jsonl" \
    --corpus "$out/compat.jsonl" --prev-tag "$tag" \
    --allowlist "$ENGINE/examples/determinism/divergence-allowlist.json" "${golden[@]}"
}

case "${1:-}" in
  cell) shift; cmd_cell "$@" ;;
  compare) shift; cmd_compare "$@" ;;
  canary) shift; cmd_canary "$@" ;;
  prev-release) shift; cmd_prev_release "$@" ;;
  *) sed -n '2,20p' "$0"; exit 2 ;;
esac
