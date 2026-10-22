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

# Copy-on-write clone of one directory; returns non-zero if the filesystem
# can't clone (never falls back to a real copy).
cow_clone() {
    case "$(uname -s)" in
        Darwin) cp -c -R "$1" "$2" 2>/dev/null ;;
        *) cp -R --reflink=always "$1" "$2" 2>/dev/null ;;
    esac
}

seed_target() {
    local from="$seed/server/target/debug" to="$wt/server/target/debug"
    if [ ! -d "$from/deps" ]; then
        echo "seed: no $from/deps; building cold" >&2
        return 0
    fi
    mkdir -p "$to"
    [ -f "$seed/server/target/CACHEDIR.TAG" ] && cp "$seed/server/target/CACHEDIR.TAG" "$wt/server/target/"
    # Skip incremental/: it is keyed by absolute path, so another checkout's
    # session cache is never reused.
    local d
    for d in deps build .fingerprint; do
        [ -d "$from/$d" ] || continue
        if ! cow_clone "$from/$d" "$to/$d"; then
            echo "seed: filesystem can't copy-on-write clone; building cold" >&2
            rm -rf "$wt/server/target"
            return 0
        fi
    done
    echo "seed: cloned server/target/debug from $seed" >&2
}

seed_target &
seed_pid=$!

make -C "$wt" generate \
    "$wt/generated-console/Cargo.toml" \
    "$wt/server/src/stub_impls.rs" \
    "$wt/server/src/response_contract_schema.json"

if [ ! -f "$wt/console/dist/index.html" ]; then
    mkdir -p "$wt/console/dist"
    printf '<!doctype html>\n' > "$wt/console/dist/index.html"
fi

wait "$seed_pid"

echo
echo "Worktree ready: $wt (branch $branch)"
echo "  cd $wt/server && cargo test --workspace --no-run"
