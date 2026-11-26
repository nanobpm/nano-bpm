#!/usr/bin/env bash
# Self-check for the Kani memory cap (#2040): a deliberately oversized
# workload must fail cleanly (exit 125, a `memguard:` message naming the cap,
# a recorded peak) instead of exhausting the host; a workload inside the cap
# must pass through untouched with its own exit status. Run by the nightly
# Kani workflow before any harness, so a broken guard is reported, not silent.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Four processes, each growing by 16 MiB every 100 ms to 192 MiB: every one
# stays inside the per-process kernel limit (RLIMIT_DATA = cap), but the tree
# totals ~768 MiB, so only the tree-wide sampler can (and must) stop it.
hog='import time
a = []
for _ in range(12):
    a.append(bytearray(16 << 20))
    time.sleep(0.1)
time.sleep(30)'
tree="for i in 1 2 3 4; do python3 -c '$hog' & done; wait"

# The cgroup layer is off here: this check pins the sampler alone, and a
# delayed sample must not let MemoryMax kill the tree first.
rc=0
MEMGUARD_CGROUP=0 MEMGUARD_INTERVAL=0.1 "$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- sh -c "$tree" 2>"$tmp/err" || rc=$?
[[ $rc -eq 125 ]] || { echo "selftest: oversized workload exited $rc, want 125" >&2; cat "$tmp/err" >&2; exit 1; }
grep -q 'memguard: .* exceeded cap 256 MiB' "$tmp/err" || { echo "selftest: no cap message" >&2; cat "$tmp/err" >&2; exit 1; }
peak="$(cat "$tmp/peak")"
(( peak > 256 * 1024 && peak < 2 * 1024 * 1024 )) || { echo "selftest: implausible peak ${peak} kB" >&2; exit 1; }

# A single burst far beyond the cap must be refused by the kernel limit
# (RLIMIT_DATA) before it is ever resident: the sampler alone would miss it.
rc=0
"$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- python3 -c 'a = bytearray(2 << 30); a[::4096] = b"x" * len(a[::4096])' 2>"$tmp/err" || rc=$?
[[ $rc -ne 0 ]] || { echo "selftest: 2 GiB burst under a 256 MiB cap succeeded" >&2; exit 1; }
burst="$(cat "$tmp/peak")"
(( burst <= 256 * 1024 )) || { echo "selftest: burst reached ${burst} kB, above the cap" >&2; exit 1; }

# Concurrent sub-cap bursts: four processes each touch 200 MiB at once (each
# inside RLIMIT_DATA) for ~800 MiB in total, faster than one sample. Only the
# aggregate cgroup limit can stop this; it must end non-zero with a bounded
# peak. Skipped (with a notice) where no transient scope can be created.
if grep -q 'no transient cgroup scope' "$tmp/err"; then
  echo "selftest: no cgroup scope on this host; concurrent-burst check skipped" >&2
else
  conc='import time
a = bytearray(200 << 20); a[::4096] = b"x" * len(a[::4096]); time.sleep(3)'
  rc=0
  MEMGUARD_INTERVAL=5 "$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- sh -c "rc=0; for i in 1 2 3 4; do python3 -c '$conc' & p=\"\$p \$!\"; done; for q in \$p; do wait \$q || rc=1; done; exit \$rc" 2>"$tmp/err" || rc=$?
  [[ $rc -eq 125 ]] || { echo "selftest: concurrent ~800 MiB burst exited $rc, want 125 (memory failure)" >&2; cat "$tmp/err" >&2; exit 1; }
  grep -q 'memguard: ' "$tmp/err" || { echo "selftest: cgroup OOM kill not reported" >&2; cat "$tmp/err" >&2; exit 1; }
fi

# Descendants in another process group (a nested GNU timeout) must die with
# the tree on a cap violation, not survive it.
rc=0
MEMGUARD_CGROUP=0 MEMGUARD_INTERVAL=0.1 "$here/memguard.sh" $((64 * 1024)) "$tmp/peak" -- \
  sh -c "timeout 60 python3 -c '$hog' & echo \$! >'$tmp/nested'; wait" 2>/dev/null || rc=$?
[[ $rc -eq 125 ]] || { echo "selftest: nested-timeout workload exited $rc, want 125" >&2; exit 1; }
sleep 0.5
! pgrep -f "bytearray\(16 << 20\)" >/dev/null || { echo "selftest: nested-timeout descendant survived the cap kill" >&2; exit 1; }

# SIGTERM to the guard must take the workload down with it.
# shellcheck disable=SC2016 # expanded by the inner sh
MEMGUARD_CGROUP=0 "$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- sh -c 'sleep 300 & echo $! >"$0"; wait' "$tmp/child" &
g=$!
for _ in $(seq 50); do [[ -s "$tmp/child" ]] && break; sleep 0.1; done
kill -TERM "$g"; rc=0; wait "$g" || rc=$?
sleep 0.5
! kill -0 "$(cat "$tmp/child")" 2>/dev/null || { echo "selftest: workload survived SIGTERM to memguard (rc $rc)" >&2; exit 1; }

rc=0
"$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- sh -c 'exit 7' || rc=$?
[[ $rc -eq 7 ]] || { echo "selftest: in-budget workload exited $rc, want 7" >&2; exit 1; }

# A command found only on the caller's PATH must still resolve through the
# scope wrapper (sudo resets PATH to secure_path; memguard re-pins it).
mkdir -p "$tmp/bin"
printf '#!/bin/sh\nexit 9\n' >"$tmp/bin/memguard-path-probe"
chmod +x "$tmp/bin/memguard-path-probe"
rc=0
PATH="$tmp/bin:$PATH" "$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- memguard-path-probe || rc=$?
[[ $rc -eq 9 ]] || { echo "selftest: caller-PATH command exited $rc, want 9" >&2; exit 1; }

echo "memguard selftest: OK (oversized workload killed at peak $((peak / 1024)) MiB)"
