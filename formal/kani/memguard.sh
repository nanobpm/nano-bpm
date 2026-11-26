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
# exits with the command's status, except that a cgroup OOM kill (confirmed
# by the scope's systemd Result=oom-kill) also exits 125 (a single-process burst stopped by the kernel limit shows
# up as the command's own allocation failure, i.e. a non-zero status). The sampled peak RSS (kB) is written to
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

# Kernel-backed limit first: RLIMIT_DATA (heap + private writable mappings,
# Linux >= 4.7) is set to the cap in the child before it execs, so a burst
# allocation fails inside the kernel instead of outrunning the 1 s sampler.
# It is per process (inherited by every child), so the sampler below still
# enforces the cap on the whole tree and records the peak for reporting.
# In a non-interactive shell a background child is not a process-group
# leader, so setsid execs in place: $! is the group id of the whole tree.
#
# Aggregate kernel limit: RLIMIT_DATA is per process, so N concurrent
# sub-cap bursts could still sum past the cap between two samples. When a
# transient systemd scope is available (user manager, or passwordless sudo
# as on GitHub-hosted runners) the whole tree runs in its own cgroup with
# MemoryMax = cap + 1/4 and no swap: the sampler still fires first at the
# cap for slow growth (exit 125), and the cgroup OOM-kills a burst that
# outruns it (non-zero exit). Without one we warn and rely on the per-process
# limit plus the sampler. MEMGUARD_CGROUP=0 disables the probe.
scope=()
ctl=()
show=()
unit="memguard-$$-${RANDOM}.scope"
if [[ "${MEMGUARD_CGROUP:-1}" != 0 ]] && command -v systemd-run >/dev/null 2>&1; then
  hard_kb=$(( cap_kb + cap_kb / 4 ))
  props=(-p "MemoryMax=${hard_kb}K" -p MemorySwapMax=0)
  # systemd >= 254 expands $VAR/$$ in the command line; pass argv verbatim.
  if systemd-run --help 2>/dev/null | grep -q -- --expand-environment; then
    props+=(--expand-environment=no)
  fi
  if systemd-run --user --scope -q "${props[@]}" true >/dev/null 2>&1; then
    scope=(systemd-run --user --scope -q --unit="$unit" "${props[@]}" --)
    ctl=(systemctl --user)
    show=(systemctl --user)
  elif sudo -n systemd-run --scope -q "${props[@]}" true >/dev/null 2>&1; then
    scope=(sudo -n --preserve-env systemd-run --scope -q --unit="$unit" --uid="$(id -u)" --gid="$(id -g)" "${props[@]}" -- env PATH="$PATH" HOME="$HOME")
    ctl=(sudo -n systemctl)
    show=(systemctl)
  fi
fi
if (( ${#scope[@]} == 0 )); then
  echo "memguard: no transient cgroup scope available; aggregate limit is sampled only" >&2
fi
export MEMGUARD_SCOPED=$(( ${#scope[@]} > 0 ))
(ulimit -d "$cap_kb" && exec setsid "${scope[@]}" "$@") &
pid=$!
# setsid made $pid a session leader, so the session id names the whole tree,
# including descendants that moved to their own process group (e.g. GNU
# `timeout`). On the sudo-scope path, though, $pid is root-owned sudo (the
# guard cannot signal it) and sudo's use_pty puts the workload in a new
# session; there the scope's cgroup is the authoritative tree: we sample its
# cgroup.procs and kill it with `systemctl kill`. Liveness comes from the
# shell's own job table, never `kill -0` (EPERM on a root-owned child).
cg=""
cgroup_dir() {
  [[ -n "$cg" || ${#ctl[@]} -eq 0 ]] && return
  local c
  c="$("${show[@]}" show -p ControlGroup --value "$unit" 2>/dev/null)"
  [[ -n "$c" && -r "/sys/fs/cgroup$c/cgroup.procs" ]] && cg="/sys/fs/cgroup$c"
}
tree_rss() {
  local pids=""
  [[ -n "$cg" ]] && pids="$(tr '\n' ',' <"$cg/cgroup.procs" 2>/dev/null)"
  {
    ps -o rss= -s "$pid" 2>/dev/null
    [[ -n "${pids%,}" ]] && ps -o rss= -p "${pids%,}" 2>/dev/null
  } | awk '{s += $1} END {print s + 0}'
}
kill_tree() {
  (( ${#ctl[@]} )) && "${ctl[@]}" kill -s KILL "$unit" 2>/dev/null
  kill -KILL -- "-$pid" 2>/dev/null
  pkill -KILL -s "$pid" 2>/dev/null
}
alive() { jobs -rp | grep -qx "$pid"; }
# Terminating the guard must not orphan the workload.
trap 'kill_tree; wait "$pid" 2>/dev/null; echo "$peak" > "$peak_file"; exit 143' TERM INT HUP
peak=0
over=0
while alive; do
  cgroup_dir
  rss="$(tree_rss)"
  (( rss > peak )) && peak=$rss
  if (( rss > cap_kb )); then
    over=1
    kill_tree
    break
  fi
  sleep "$interval" & wait $!
done
wait "$pid"
rc=$?
kill_tree
echo "$peak" > "$peak_file"
if (( over )); then
  echo "memguard: process group RSS $((peak / 1024)) MiB exceeded cap $((cap_kb / 1024)) MiB; killed (shrink the bound)" >&2
  exit 125
fi
# A SIGKILL'd workload is reported as a memory failure only when systemd
# confirms the scope ended in a cgroup OOM kill (not e.g. `timeout -s KILL`).
result=""
if (( ${#ctl[@]} )); then
  result="$("${show[@]}" show -p Result --value "$unit" 2>/dev/null)"
  "${ctl[@]}" reset-failed "$unit" 2>/dev/null
fi
if [[ "$result" == oom-kill ]]; then
  echo "memguard: workload OOM-killed by the aggregate cgroup limit $((hard_kb / 1024)) MiB (cap $((cap_kb / 1024)) MiB, sampled peak $((peak / 1024)) MiB); shrink the bound" >&2
  exit 125
fi
exit "$rc"
