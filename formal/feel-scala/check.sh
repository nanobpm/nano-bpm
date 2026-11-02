#!/usr/bin/env bash
# feel-scala differential gate: evaluates a Lean-generated FEEL corpus with the
# feel-scala version the pinned Zeebe embeds, then compares every row against
# the reference/nano outcome (compare.mjs) under known-divergences.json.
#
# Usage: formal/feel-scala/check.sh <corpus.tsv>
# Run by formal/lean/feel-diff.sh AFTER feel_diff has proven nano == reference.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
corpus="${1:?usage: check.sh <corpus.tsv>}"
jar="$("$here/fetch-feel-scala.sh")"
outcomes="$(mktemp -t feel-scala-outcomes.XXXXXX)"
trap 'rm -f "$outcomes"' EXIT

java -cp "$jar" "$here/FeelScalaEval.java" < "$corpus" > "$outcomes"
node "$here/compare.mjs" "$corpus" "$outcomes"
