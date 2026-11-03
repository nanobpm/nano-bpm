#!/usr/bin/env bash
# FEEL differential fuzz: generate a corpus from the Lean reference semantics
# (formal/lean/Feel) and check the Rust FEEL evaluator (engine-core/src/feel)
# agrees on every case. Any divergence is a hard failure — no tolerated
# mismatch, no retries.
#
# Usage: formal/lean/feel-diff.sh [num-cases]
#   num-cases defaults to $FEEL_FUZZ_CASES, else 3000.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # formal/lean
repo_root="$(cd "$here/../.." && pwd)"
cases="${1:-${FEEL_FUZZ_CASES:-3000}}"

corpus="$(mktemp -t feel-corpus.XXXXXX.tsv)"
trap 'rm -f "$corpus"' EXIT

# The repo-wide .cargo/config.toml forces `-fuse-ld=mold` on the Linux target to
# speed up local links, but mold is not preinstalled on every CI runner (the
# formal job builds engine-core here without it). When mold is absent the link
# dies with `collect2: fatal error: cannot find 'ld'`, so drop that flag for this
# build and use rustc's bundled, self-contained lld (its default linker on
# x86_64-linux) instead — no system-linker dependency. A non-empty RUSTFLAGS
# outranks and fully replaces the target-scoped config rustflags (an empty value
# is ignored by cargo, so it must carry the lld flag). Only done on Linux without
# mold, so dev machines and the mold-provisioned Rust CI jobs keep the speedup.
if [ "$(uname -s)" = "Linux" ] && ! command -v mold >/dev/null 2>&1; then
  export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-C link-arg=-fuse-ld=lld"
fi

echo "==> lake build (all Lean targets)"
( cd "$here" && lake build )

echo "==> generating $cases FEEL cases from the Lean reference"
( cd "$here" && lake exe feelfuzz "$cases" ) > "$corpus"

# The generator emits exactly one row per requested case. A short (truncated) or
# empty corpus means the generator crashed mid-stream or produced nothing — the
# Rust checker would then silently pass over the rows it *did* see, so verify the
# count here before handing it off to the differential gate.
generated="$(grep -c . "$corpus" || true)"
if [ "$generated" -ne "$cases" ]; then
  echo "FAIL: expected $cases generated FEEL cases but the corpus has $generated" >&2
  exit 1
fi

echo "==> checking Rust engine-core/src/feel against the Lean reference"
( cd "$repo_root/engine-core" && cargo run --quiet --example feel_diff -- "$corpus" )

# feel-scala reference (#1391 step 5): the same corpus through the feel-scala
# version the pinned Zeebe embeds. nano == reference was just proven above, so
# the reference column is nano's outcome; every nano≠feel-scala row must be a
# known divergence with an issue reference (and every listed one must still
# diverge). See formal/feel-scala/README.md.
echo "==> checking the corpus against feel-scala (Zeebe's FEEL engine)"
"$repo_root/formal/feel-scala/check.sh" "$corpus"
