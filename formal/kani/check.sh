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
# Memory headroom (#1994): a harness whose peak RSS exceeds this percentage of
# the host's MemTotal fails even if it proved, so a bound that only fits
# sometimes is caught before the OOM killer makes it intermittent.
MAX_RSS_PCT="${KANI_MAX_RSS_PCT:-60}"
# When set, the first failure is recorded there as `<class> <harness> <detail>`
# (class: timeout | verification | vacuous-cover | memory | error) so the
# nightly workflow can file/dedupe an issue by signature.
FAILURE_FILE="${KANI_FAILURE_FILE:-}"
fail() { # class harness detail
  [[ -n "$FAILURE_FILE" ]] && echo "$1 $2 $3" > "$FAILURE_FILE"
  return 0
}
# KANI_REQUIRE_RSS=1 (set by CI) fails a harness whose peak RSS could not be
# measured, so the memory bound can never be skipped silently.
REQUIRE_RSS="${KANI_REQUIRE_RSS:-0}"
mem_total_kb="$(sed -n 's/^MemTotal:\s*\([0-9]*\) kB/\1/p' /proc/meminfo 2>/dev/null || true)"

# Pass harness names to prove only those (CI runs one harness per matrix leg).
ALL_HARNESSES=(
  flow_arrivals_preserve_join_invariant
  job_index_resync_keeps_lockstep
  parallel_join_fires_only_when_every_flow_taken
  join_ready_kernel_total_and_exact
  ack_outcome_kernel_is_consistent
)
# `--list` prints the canonical harness list (one per line) and exits; the
# nightly workflow derives its matrix and failure report from it.
if [[ "${1:-}" == --list ]]; then
  printf '%s\n' "${ALL_HARNESSES[@]}"
  exit 0
fi
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
  cover_bad=0
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
  failed_check="$(awk '/^Check [0-9]+: /{name=$3} /Status: FAILURE/{print name; exit}' "$log")"
  if awk '/^Check [0-9]+: /{cover = ($3 ~ /\.cover\./)} cover && /Status: (UNSATISFIABLE|UNREACHABLE)/{found=1} END{exit !found}' "$log"; then
    echo "::error::kani $h has an unsatisfiable/unreachable cover! (vacuous branch)"
    rc=1
    cover_bad=1
  fi
  rm -f "$log"
  peak_kb="$(sed -n 's/^\s*Maximum resident set size (kbytes): //p' "$timefile")"
  rm -f "$timefile"
  if [[ -z "$peak_kb" && "$REQUIRE_RSS" == 1 && $rc -eq 0 ]]; then
    echo "::error::kani $h: peak RSS not measured (/usr/bin/time missing)"
    fail error "$h" "no-rss"
    exit 1
  fi
  if [[ -n "$peak_kb" ]]; then
    echo "kani $h: peak RSS $((peak_kb / 1024)) MiB, wall ${elapsed}s"
    if [[ -n "$mem_total_kb" ]] && (( peak_kb * 100 > mem_total_kb * MAX_RSS_PCT )); then
      echo "::error::kani $h peak RSS $((peak_kb / 1024)) MiB exceeds ${MAX_RSS_PCT}% of $((mem_total_kb / 1024)) MiB host memory (shrink the bound)"
      [[ $rc -eq 0 ]] && { fail memory "$h" "over-${MAX_RSS_PCT}pct"; exit 1; }
    fi
  fi
  if [[ $rc -ne 0 ]]; then
    if [[ $rc -eq 124 ]]; then fail timeout "$h" "box-${HARNESS_TIMEOUT}s"
    elif [[ $cover_bad -eq 1 ]]; then fail vacuous-cover "$h" "-"
    elif [[ -n "$failed_check" ]]; then fail verification "$h" "$failed_check"
    else fail error "$h" "exit-$rc"; fi
    echo "::error::kani $h failed (exit $rc, ${elapsed}s; 124 = exceeded ${HARNESS_TIMEOUT}s box)"
    exit 1
  fi
  echo "kani $h: VERIFICATION:- SUCCESSFUL in ${elapsed}s"
done
