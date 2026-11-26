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

# Grows by 32 MiB every 50 ms up to 4 GiB: far beyond the 256 MiB cap, but
# slow enough that the 1 s sampler kills it long before host memory runs out.
hog='import time
a = []
for _ in range(128):
    a.append(bytearray(32 << 20))
    time.sleep(0.05)'

rc=0
python3 -c "$hog" >/dev/null 2>&1 || true # warm python startup (not measured)
"$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- python3 -c "$hog" 2>"$tmp/err" || rc=$?
[[ $rc -eq 125 ]] || { echo "selftest: oversized workload exited $rc, want 125" >&2; cat "$tmp/err" >&2; exit 1; }
grep -q 'memguard: .* exceeded cap 256 MiB' "$tmp/err" || { echo "selftest: no cap message" >&2; cat "$tmp/err" >&2; exit 1; }
peak="$(cat "$tmp/peak")"
(( peak > 256 * 1024 && peak < 2 * 1024 * 1024 )) || { echo "selftest: implausible peak ${peak} kB" >&2; exit 1; }

rc=0
"$here/memguard.sh" $((256 * 1024)) "$tmp/peak" -- sh -c 'exit 7' || rc=$?
[[ $rc -eq 7 ]] || { echo "selftest: in-budget workload exited $rc, want 7" >&2; exit 1; }

echo "memguard selftest: OK (oversized workload killed at peak $((peak / 1024)) MiB)"
