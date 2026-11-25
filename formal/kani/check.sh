#!/usr/bin/env bash
# Run the engine-core Kani proof harnesses (#1960). See formal/kani/README.md.
# Installs the pinned cargo-kani on first use (cached by CI under ~/.cargo and
# ~/.kani), then proves each harness with a bounded default unwind.
set -euo pipefail

KANI_VERSION="0.67.0"
UNWIND="${KANI_UNWIND:-4}"
HARNESSES=(
  flow_arrivals_preserve_join_invariant
  job_lease_lifecycle_keeps_indexes_in_lockstep
  parallel_diamond_creates_one_instance
)

root="$(cd "$(dirname "$0")/../.." && pwd)"
if ! cargo kani --version 2>/dev/null | grep -q "$KANI_VERSION"; then
  cargo install --locked kani-verifier --version "$KANI_VERSION"
  cargo kani setup
fi

cd "$root/engine-core"
for h in "${HARNESSES[@]}"; do
  echo "::group::kani $h"
  start=$(date +%s)
  cargo kani --default-unwind "$UNWIND" --harness "$h"
  echo "kani $h: proven in $(( $(date +%s) - start ))s"
  echo "::endgroup::"
done
