#!/usr/bin/env bash
# Run the engine-core Kani proof harnesses (#1960). See formal/kani/README.md.
# Installs the pinned cargo-kani on first use (cached by CI under ~/.cargo and
# ~/.kani), then proves each harness with a bounded default unwind.
set -euo pipefail

KANI_VERSION="0.67.0"
# An explicit KANI_UNWIND overrides every harness (--unwind beats
# #[kani::unwind]); otherwise each harness's own attribute wins and 3 is only
# the default for loops a harness does not bound itself.
if [[ -n "${KANI_UNWIND:-}" ]]; then
  UNWIND_ARGS=(--unwind "$KANI_UNWIND")
else
  UNWIND_ARGS=(--default-unwind 3)
fi
# Wall-clock box per harness, including its ~2 min cold `cargo kani` build
# (so ~5 min of verification): one that cannot prove within it must shrink its
# input domain, not be given more time (README "Running").
HARNESS_TIMEOUT="${KANI_HARNESS_TIMEOUT:-420}"

# Pass harness names to prove only those (CI runs one harness per matrix leg).
ALL_HARNESSES=(
  flow_arrivals_preserve_join_invariant
  job_index_resync_keeps_lockstep
  parallel_join_fires_only_when_every_flow_taken
)
if [[ $# -gt 0 ]]; then
  for h in "$@"; do
    [[ " ${ALL_HARNESSES[*]} " == *" $h "* ]] || { echo "unknown harness: $h" >&2; exit 2; }
  done
  HARNESSES=("$@")
else
  HARNESSES=("${ALL_HARNESSES[@]}")
fi

root="$(cd "$(dirname "$0")/../.." && pwd)"
if ! cargo kani --version 2>/dev/null | grep -q "$KANI_VERSION"; then
  cargo install --locked kani-verifier --version "$KANI_VERSION"
fi
# Kani's backing Rust toolchain lives under ~/.rustup and is only symlinked
# from ~/.kani, so a restored ~/.kani cache can hold dangling links. Re-run
# setup whenever the install is absent or any link in it is broken.
if [[ ! -d "$HOME/.kani" ]] || [[ -n "$(find "$HOME/.kani" -xtype l -print -quit)" ]]; then
  rm -rf "$HOME/.kani"
  cargo kani setup
fi

cd "$root/engine-core"
for h in "${HARNESSES[@]}"; do
  echo "::group::kani $h"
  start=$(date +%s)
  log="$(mktemp)"
  rc=0
  timeout "$HARNESS_TIMEOUT" cargo kani "${UNWIND_ARGS[@]}" --harness "$h" 2>&1 | tee "$log" || rc=$?
  echo "::endgroup::"
  elapsed=$(( $(date +%s) - start ))
  grep -E '^(VERIFICATION:|Verification Time:|Complete - |[[:space:]]*\*\* [0-9]+ of [0-9]+ cover)' "$log" || true
  # Kani exits 0 even when a kani::cover! is UNSATISFIABLE/UNREACHABLE, so
  # enforce cover reachability here: every reported cover must be satisfied.
  cover_bad=""
  while read -r sat total; do
    [[ "$sat" == "$total" ]] || cover_bad="$sat of $total"
  done < <(sed -nE 's/^[[:space:]]*\*\* ([0-9]+) of ([0-9]+) cover properties satisfied.*/\1 \2/p' "$log")
  rm -f "$log"
  if [[ $rc -ne 0 ]]; then
    echo "::error::kani $h failed (exit $rc, ${elapsed}s; 124 = exceeded ${HARNESS_TIMEOUT}s box)"
    exit 1
  fi
  if [[ -n "$cover_bad" ]]; then
    echo "::error::kani $h: only $cover_bad cover properties satisfied (unreachable cover)"
    exit 1
  fi
  echo "kani $h: VERIFICATION:- SUCCESSFUL in ${elapsed}s"
done
