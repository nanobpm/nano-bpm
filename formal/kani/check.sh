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
  # Peak memory (#1994): report max RSS via GNU time so each run records how
  # close the harness is to the runner's memory ceiling.
  timefile="$(mktemp)"
  timer=()
  [[ -x /usr/bin/time ]] && timer=(/usr/bin/time -v -o "$timefile")
  timeout "$HARNESS_TIMEOUT" "${timer[@]}" cargo kani "${UNWIND_ARGS[@]}" --harness "$h" 2>&1 | tee "$log" || rc=$?
  echo "::endgroup::"
  elapsed=$(( $(date +%s) - start ))
  grep -E '^(VERIFICATION:|Verification Time:|Complete - )' "$log" || true
  # Kani reports an unreachable `kani::cover!` without failing the run; treat
  # it as a vacuous proof (a guarded branch the harness never exercises).
  # Only cover records count: an ordinary assertion may legitimately be
  # reported UNREACHABLE (e.g. a default match branch) on a passing proof.
  if awk '/^Check [0-9]+: /{cover = ($3 ~ /\.cover\./)} cover && /Status: (UNSATISFIABLE|UNREACHABLE)/{found=1} END{exit !found}' "$log"; then
    echo "::error::kani $h has an unsatisfiable/unreachable cover! (vacuous branch)"
    rc=1
  fi
  rm -f "$log"
  peak_kb="$(sed -n 's/^\s*Maximum resident set size (kbytes): //p' "$timefile")"
  rm -f "$timefile"
  [[ -n "$peak_kb" ]] && echo "kani $h: peak RSS $((peak_kb / 1024)) MiB"
  if [[ $rc -ne 0 ]]; then
    echo "::error::kani $h failed (exit $rc, ${elapsed}s; 124 = exceeded ${HARNESS_TIMEOUT}s box)"
    exit 1
  fi
  echo "kani $h: VERIFICATION:- SUCCESSFUL in ${elapsed}s"
done
