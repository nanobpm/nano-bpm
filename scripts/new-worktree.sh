#!/usr/bin/env bash
#
# new-worktree.sh — create an agent worktree that builds warm.
#
# A fresh `git worktree add` recompiles every crates.io dependency: a first
# `cargo test --workspace --no-run` costs ~400 CPU-seconds per worktree. With a
# dozen agents fanning out at once, that saturates the cores. Seeding the
# dependencies cuts it to ~280 CPU-seconds (setup included). It does not make a
# single worktree faster on an idle machine, and saves little disk: our own
# crates' output, mostly the incremental cache, is still written per worktree.
#
# This script:
#   1. creates the worktree in ~/workspace/nanobpmn-worktrees/<name>;
#   2. seeds server/target/debug from the main checkout with a copy-on-write
#      clone (APFS `cp -c`, or `cp --reflink=always` on btrfs/XFS). The clone
#      itself uses no extra disk until files diverge. crates.io
#      dependencies are then already fresh; only the path crates (ours, the
#      generated REST crate, vendored openraft) compile, because Cargo keys
#      their artifacts by absolute path. If the filesystem can't clone, it
#      skips seeding rather than copying tens of GB;
#   3. runs the code generation the build needs (gitignored outputs: generated/,
#      generated-console/, stub_impls.rs, response_contract_schema.json). It is
#      regenerated, not copied: copied files get fresh mtimes, so Make would
#      treat them as current even when the branch's spec differs;
#   4. stubs console/dist exactly as CI does, so the console crate's rust-embed
#      derive compiles without building the SPA.
#
# Usage: scripts/new-worktree.sh <name> [<branch>] [<base>]
#   <branch>  defaults to <name>; created from <base> (default origin/main).
#             An existing branch is checked out instead.
# Env:     NANOBPMN_SEED  checkout whose server/target/debug seeds the clone
#                         (default: the main checkout of this repository).
#
# Keep the seed warm: run `cargo test --workspace --no-run` in the seed's
# server/ after pulling main. The seed is only read, never written.
set -euo pipefail

name=${1:?usage: scripts/new-worktree.sh <name> [<branch>] [<base>]}
branch=${2:-$name}
base=${3:-origin/main}

main_checkout=$(git rev-parse --path-format=absolute --git-common-dir)
main_checkout=${main_checkout%/.git}
seed=${NANOBPMN_SEED:-$main_checkout}
wt_root=${NANOBPMN_WORKTREES:-$(dirname "$main_checkout")/nanobpmn-worktrees}
wt="$wt_root/$name"

[ -e "$wt" ] && { echo "error: $wt already exists" >&2; exit 1; }
mkdir -p "$wt_root"

if git -C "$main_checkout" show-ref --verify --quiet "refs/heads/$branch"; then
    git -C "$main_checkout" worktree add "$wt" "$branch"
else
    git -C "$main_checkout" worktree add -b "$branch" "$wt" "$base"
fi

# A spaced worktree path breaks tikv-jemalloc-sys's autoconf configure when
# CARGO_TARGET_DIR resolves under it (#1716). Default the target dir for this
# worktree to a space-free path via the worktree's .cargo/config.toml — Cargo
# reads build.target-dir from config on EVERY invocation, so direct `cargo`
# runs honor it (a direnv .envrc would not: nothing sources it without a
# configured direnv hook) — unless the caller already exported a
# CARGO_TARGET_DIR (which overrides config). A caller-set value with a space
# is rejected with a clear message instead of an autoconf error that looks
# like a toolchain bug.
wt_target_dir=""
if [[ "$wt" = *" "* ]]; then
    if [ -n "${CARGO_TARGET_DIR:-}" ]; then
        if [[ "$CARGO_TARGET_DIR" = *" "* ]]; then
            echo "error: CARGO_TARGET_DIR ($CARGO_TARGET_DIR) contains a space and the worktree path ($wt) does too;" >&2
            echo "       tikv-jemalloc-sys cannot build with a spaced target dir (#1716). Use a space-free CARGO_TARGET_DIR." >&2
            exit 1
        fi
    else
        hash=$(printf '%s' "$wt" | cksum | cut -d' ' -f1)
        wt_target_dir="$HOME/.local/share/nanobpm/target/$hash"
        mkdir -p "$wt/.cargo"
        printf '[build]\ntarget-dir = "%s"\n' "$wt_target_dir" > "$wt/.cargo/config.toml"
        echo "note: worktree path contains a space; pinned a space-free CARGO_TARGET_DIR via $wt/.cargo/config.toml (#1716)" >&2
    fi
fi

# Copy-on-write clone of one directory; returns non-zero if the filesystem
# can't clone (never falls back to a real copy).
cow_clone() {
    case "$(uname -s)" in
        Darwin) cp -c -R "$1" "$2" 2>/dev/null ;;
        *) cp -R --reflink=always "$1" "$2" 2>/dev/null ;;
    esac
}

seed_target() {
    # When the worktree's target dir is redirected to a space-free path
    # (#1716), Cargo writes <target-dir>/debug directly (no workspace subdir),
    # so seed the REDIRECTED dir and keep the two aligned.
    local target_root="$wt/server/target"
    if [ -n "$wt_target_dir" ]; then
        target_root="$wt_target_dir"
    fi
    local from="$seed/server/target/debug" to="$target_root/debug"
    if [ ! -d "$from/deps" ]; then
        echo "seed: no $from/deps; building cold" >&2
        return 0
    fi
    mkdir -p "$to"
    [ -f "$seed/server/target/CACHEDIR.TAG" ] && cp "$seed/server/target/CACHEDIR.TAG" "$target_root/"
    # Skip incremental/: it is keyed by absolute path, so another checkout's
    # session cache is never reused.
    local d
    for d in deps build .fingerprint; do
        [ -d "$from/$d" ] || continue
        if ! cow_clone "$from/$d" "$to/$d"; then
            echo "seed: filesystem can't copy-on-write clone; building cold" >&2
            rm -rf "$target_root"
            return 0
        fi
    done
    echo "seed: cloned server/target/debug from $seed into $target_root" >&2
}

seed_target &
seed_pid=$!

# Request only `generate`: it already produces both generated crates, the
# server stubs, and the response schema. Naming the individual file targets
# alongside it would re-run a generator per target, and under an inherited
# parallel MAKEFLAGS (e.g. -j4) those recipes run concurrently and overwrite
# the same output dirs / JAR temp file.
make -C "$wt" generate

# Mirror CI's "Stub console assets" step: `console` embeds console/dist and
# `console-observe` embeds console/dist-observe, so stub both.
for d in console/dist console/dist-observe; do
    if [ ! -f "$wt/$d/index.html" ]; then
        mkdir -p "$wt/$d"
        printf '<!doctype html>\n' > "$wt/$d/index.html"
    fi
done

wait "$seed_pid"

echo
echo "Worktree ready: $wt (branch $branch)"
echo "  cd $wt/server && cargo test --workspace --no-run"
