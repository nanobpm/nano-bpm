# shellcheck shell=bash
# Sourced by check.sh and memguard-selftest.sh (#2040): the single definition
# of "this workload output reports an allocator out-of-memory failure". Under
# memguard, RLIMIT_DATA refuses an over-cap allocation with ENOMEM, so the
# tool exits with its own status (not memguard's 125) and prints its own
# allocator diagnostic; these are the spellings of that diagnostic for the
# tools a Kani run spawns (CBMC, rustc/Rust std, C++ runtimes, Python, libc).
alloc_oom_in() { # log-file -> 0 if it reports an allocator OOM
  grep -Eq 'Out of memory|out of memory|std::bad_alloc|memory allocation of [0-9]+ bytes failed|MemoryError|Cannot allocate memory' "$1"
}
