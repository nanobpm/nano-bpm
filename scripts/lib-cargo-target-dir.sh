#!/usr/bin/env bash
# Sourced helper: print Cargo's EFFECTIVE target directory for a crate dir
# (#1716). Shell twin of engine-core/scripts/lib-cargo-target-dir.mjs — both
# ask Cargo itself (`cargo metadata`), so neither re-implements Cargo's
# precedence (CARGO_TARGET_DIR env > build.target-dir config > <ws>/target).
# Fails closed: if Cargo cannot report the directory, return non-zero rather
# than guess `<crate>/target` and risk staging a stale artifact.
cargo_target_dir() {
    local crate_dir="$1" out dir
    out=$(cd "$crate_dir" && cargo metadata --no-deps --format-version 1 2>/dev/null) || {
        echo "error: cargo metadata failed in $crate_dir; cannot resolve the effective target dir (#1716)" >&2
        return 1
    }
    dir=$(printf '%s' "$out" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const d=JSON.parse(s).target_directory;if(!d)process.exit(1);process.stdout.write(d)})') || {
        echo "error: cargo metadata in $crate_dir reported no target_directory (#1716)" >&2
        return 1
    }
    printf '%s\n' "$dir"
}
