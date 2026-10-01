#!/usr/bin/env bash
# Sourced (not executed) by every zigbuild path: scripts/cross-build.sh and the
# Linux legs of .github/workflows/publish-c8ctl-binaries.yml. Single source of
# truth for the armv6 ARM-RTABI shim link (#1322).
#
# Why: since Rust 1.99 (LLVM 23) the prebuilt libstd for strict-align ARM
# (arm-unknown-linux-gnueabihf = armv6) calls `__aeabi_uread4` / `__aeabi_uwrite4`
# / `__aeabi_uread8` / `__aeabi_uwrite8` for unaligned accesses. Rust's own
# compiler_builtins defines them (rust-lang/compiler-builtins#1317), but
# cargo-zigbuild drops the compiler_builtins rlib on ARM and uses zig's
# compiler-rt, which lacks them, so the link fails with
# `ld.lld: undefined symbol: __aeabi_uread4`. cargo-zigbuild >= 0.23.3 injects
# the same shim itself, but also feeds it into jemalloc's C configure/compile
# steps, which breaks our 32-bit ARM build. So we stay on 0.23.2 and link the
# shim at rustc's final link only (`-C link-arg`, never C build scripts). The
# definitions are weak, so a toolchain that provides them wins without a
# duplicate-symbol error. armv7 isn't strict-align and never emits these calls.
#
# The path must be absolute: path deps outside the server workspace (e.g. the
# engine-core cdylib) link with their own package dir as cwd, so a relative
# `.cargo/config.toml` link-arg would not resolve for them.
#
# Usage: . scripts/zig-arm-shim-env.sh   (from any cwd)
_nano_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_RUSTFLAGS="${CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_RUSTFLAGS:+$CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_RUSTFLAGS }-C link-arg=$_nano_root/server/build-support/aeabi_unaligned.c"
unset _nano_root
