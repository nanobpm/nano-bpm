#!/usr/bin/env bash
# Regenerate formal/lean/Refinement/Extracted/JoinGuard.lean from the pure join
# guard kernel engine-core/src/engine/join_guard.rs (Charon -> LLBC -> Aeneas ->
# Lean), #2010. CI runs this and fails on `git diff`, so the committed extraction
# (and therefore the Lean proof built on it) can never drift from the Rust.
#
# Pins: the Aeneas binary release (which bundles the matching Charon) and the
# Rust nightly that Charon is built against (the release's `rust-toolchain`).
# The same Aeneas commit is the Lean library pinned in lakefile.lean.
#
#   formal/lean/Refinement/extract.sh           # regenerate in place
#   formal/lean/Refinement/extract.sh --check   # regenerate, fail on any diff
set -euo pipefail

AENEAS_TAG="nightly-2026.11.25-fb44cb4"
AENEAS_SHA256="5d67d93ed92c50f9d0206c96b437b48d59281c42b8a088512faa1326199d6b40"
CHARON_TOOLCHAIN="nightly-2026-09-17"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT_DIR="$ROOT/formal/lean/Refinement/Extracted"
CACHE="${AENEAS_CACHE_DIR:-$HOME/.cache/nanobpm-aeneas}/$AENEAS_TAG"

if [[ ! -x "$CACHE/aeneas" ]]; then
  mkdir -p "$CACHE"
  tarball="$CACHE/aeneas-linux-x86_64.tar.gz"
  curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
    "https://github.com/AeneasVerif/aeneas/releases/download/$AENEAS_TAG/aeneas-linux-x86_64.tar.gz" \
    -o "$tarball"
  echo "$AENEAS_SHA256  $tarball" | sha256sum -c -
  tar xzf "$tarball" -C "$CACHE"
  rm -f "$tarball"
fi

# Charon drives rustc's internals: it needs exactly its own nightly with
# rustc-dev. Installed on demand (idempotent).
rustup toolchain install "$CHARON_TOOLCHAIN" --profile minimal \
  -c rustc-dev,llvm-tools,rust-src >/dev/null

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Run from the repo root so the source spans recorded in the extraction are
# repo-relative (and so identical on every machine).
cd "$ROOT"
RUSTUP_TOOLCHAIN="$CHARON_TOOLCHAIN" PATH="$CACHE:$PATH" \
  "$CACHE/charon" rustc --preset=aeneas --dest-file "$work/join_guard.llbc" -- \
  --crate-type=lib --crate-name join_guard --edition 2021 \
  engine-core/src/engine/join_guard.rs >"$work/charon.log" 2>&1 \
  || { cat "$work/charon.log"; exit 1; }
"$CACHE/aeneas" -backend lean -dest "$work/out" "$work/join_guard.llbc" >"$work/aeneas.log" 2>&1 \
  || { cat "$work/aeneas.log"; exit 1; }
# Aeneas reports untranslatable bodies as errors but still writes a partial
# file (with `axiom`s in place of the bodies): never accept one.
if grep -q "Error" "$work/aeneas.log"; then
  cat "$work/aeneas.log"
  echo "extract.sh: Aeneas could not translate the whole kernel" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
cp "$work/out/JoinGuard.lean" "$OUT_DIR/JoinGuard.lean"

if [[ "${1:-}" == "--check" ]]; then
  if ! git diff --exit-code -- "$OUT_DIR" || [[ -n "$(git status --porcelain -- "$OUT_DIR")" ]]; then
    echo "::error::formal/lean/Refinement/Extracted is stale: run formal/lean/Refinement/extract.sh and commit the result (then re-check Proof.lean)" >&2
    exit 1
  fi
  echo "extract.sh: extraction up to date"
fi
