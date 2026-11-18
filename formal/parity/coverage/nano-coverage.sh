#!/usr/bin/env bash
# Nano half of the dual-engine coverage measurement (#1247): replay the parity
# corpus natively over engine-core (engine-core/tests/parity_corpus.rs) under
# `cargo llvm-cov` and write the JSON export.
#
#   formal/parity/coverage/nano-coverage.sh [out.json] [exhaustive-bound]
#   node formal/parity/coverage/report.mjs --nano out.json --check
#
# The corpus is the committed seed corpus plus the bounded-exhaustive scenarios
# (formal/corpus/exhaustive.mjs --out, bound `ci` by default) — the same two
# corpora the runtime parity runner plays in CI.
# Needs cargo-llvm-cov (`cargo install cargo-llvm-cov`, plus the
# llvm-tools-preview component).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
out="${1:-$repo/target/parity-coverage/nano-llvm-cov.json}"
bound="${2:-ci}"
mkdir -p "$(dirname "$out")"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
node "$repo/formal/corpus/exhaustive.mjs" --bound "$bound" --out "$work/exhaustive"

PARITY_CORPUS_DIRS="$work/exhaustive" cargo llvm-cov \
  --manifest-path "$repo/engine-core/Cargo.toml" \
  --test parity_corpus --json --output-path "$out"
echo "wrote $out"
