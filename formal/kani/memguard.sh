#!/usr/bin/env bash
# Hard memory cap for one Kani run (#2040). Usage:
#
#   memguard.sh <cap_kb> <peak_file> -- <command> [args...]
#
# Runs <command> in its own process group and samples the group's total RSS
# every second. If it exceeds <cap_kb>, the whole group is killed and the
# script exits 125 after printing a `memguard: ... exceeded cap` line, so an
# over-budget harness fails cleanly *before* the kernel OOM killer takes the
# runner down (which would leave no cost and no failure record). Otherwise it
# exits with the command's status. The sampled peak RSS (kB) is written to
# <peak_file> in both cases.
#
# The check `KANI_MAX_RSS_PCT` used to do only after exit (via /usr/bin/time)
# could never fire on the run that mattered: a harness that exhausts memory
# never exits normally. formal/kani/memguard-selftest.sh proves the cap fires.
set -uo pipefail

[[ $# -ge 4 && "$3" == -- ]] || { echo "usage: memguard.sh <cap_kb> <peak_file> -- <cmd...>" >&2; exit 2; }
cap_kb="$1"
peak_file="$2"
shift 3
interval="${MEMGUARD_INTERVAL:-1}"

# In a non-interactive shell a background child is not a process-group
# leader, so setsid execs in place: $! is the group id of the whole tree.
setsid "$@" &
pid=$!
peak=0
over=0
while kill -0 "$pid" 2>/dev/null; do
  rss="$(ps -o rss= -g "$pid" 2>/dev/null | awk '{s += $1} END {print s + 0}')"
  (( rss > peak )) && peak=$rss
  if (( rss > cap_kb )); then
    over=1
    kill -KILL -- "-$pid" 2>/dev/null
    break
  fi
  sleep "$interval"
done
wait "$pid"
rc=$?
echo "$peak" > "$peak_file"
if (( over )); then
  echo "memguard: process group RSS $((peak / 1024)) MiB exceeded cap $((cap_kb / 1024)) MiB; killed (shrink the bound)" >&2
  exit 125
fi
exit "$rc"
