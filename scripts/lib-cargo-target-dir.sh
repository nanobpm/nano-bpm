#!/usr/bin/env bash
# Sourced helper: print Cargo's EFFECTIVE target directory for a crate dir
# (#1716). Shell twin of engine-core/scripts/lib-cargo-target-dir.mjs — both
# ask Cargo itself (`cargo metadata`), so neither re-implements Cargo's
# precedence (CARGO_TARGET_DIR env > build.target-dir config > <ws>/target).
# Fails closed: if Cargo cannot report the directory, return non-zero rather
# than guess `<crate>/target` and risk staging a stale artifact.
#
# Usage: cargo_target_dir <manifest-path> [cargo-executable]
# Runs from the CALLER's current directory with the same Cargo executable and
# --manifest-path the build used, so config discovery and a relative
# CARGO_TARGET_DIR resolve exactly as they did for the build.
cargo_target_dir() {
    local manifest="$1" cargo_bin="${2:-cargo}" out dir
    out=$("$cargo_bin" metadata --manifest-path "$manifest" --no-deps --format-version 1 2>/dev/null) || {
        echo "error: $cargo_bin metadata failed for $manifest; cannot resolve the effective target dir (#1716)" >&2
        return 1
    }
    dir=$(printf '%s' "$out" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const d=JSON.parse(s).target_directory;if(!d)process.exit(1);process.stdout.write(d)})') || {
        echo "error: $cargo_bin metadata for $manifest reported no target_directory (#1716)" >&2
        return 1
    }
    printf '%s\n' "$dir"
}
