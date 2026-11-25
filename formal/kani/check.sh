#!/usr/bin/env bash
# Run the engine-core Kani proof harnesses (#1960). See formal/kani/README.md.
# Installs the pinned cargo-kani on first use (cached by CI under ~/.cargo and
# ~/.kani), then proves each harness with a bounded default unwind.
set -euo pipefail

KANI_VERSION="0.67.0"
# An explicit KANI_UNWIND overrides every harness (--unwind beats
# #[kani::unwind]); otherwise each harness's own attribute wins and 4 is only
# the default for loops a harness does not bound itself.
if [[ -n "${KANI_UNWIND:-}" ]]; then
  UNWIND_ARGS=(--unwind "$KANI_UNWIND")
else
  UNWIND_ARGS=(--default-unwind 3)
fi
# Wall-clock box per harness: one that cannot prove within it must shrink its
# input domain, not be given more time (README "Running").
HARNESS_TIMEOUT="${KANI_HARNESS_TIMEOUT:-300}"

HARNESSES=(
  flow_arrivals_preserve_join_invariant
  job_lease_lifecycle_keeps_indexes_in_lockstep
  parallel_join_fires_only_when_every_flow_taken
)

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
  grep -E '^(VERIFICATION:|Verification Time:|Complete - )' "$log" || true
  rm -f "$log"
  if [[ $rc -ne 0 ]]; then
    echo "::error::kani $h failed (exit $rc, ${elapsed}s; 124 = exceeded ${HARNESS_TIMEOUT}s box)"
    exit 1
  fi
  echo "kani $h: VERIFICATION:- SUCCESSFUL in ${elapsed}s"
done
