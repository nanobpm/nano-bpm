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
  UNWIND_ARGS=(--default-unwind 4)
fi
HARNESSES=(
  flow_arrivals_preserve_join_invariant
  job_lease_lifecycle_keeps_indexes_in_lockstep
  parallel_diamond_creates_one_instance
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
  cargo kani "${UNWIND_ARGS[@]}" --harness "$h"
  echo "kani $h: proven in $(( $(date +%s) - start ))s"
  echo "::endgroup::"
done
