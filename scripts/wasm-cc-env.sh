#!/usr/bin/env bash
# The ONE definition of the wasm32 C toolchain env (#1368).
#
# The read-model `wasm` backend compiles a trimmed SQLite C amalgamation (via
# the `cc` crate + read-model/build.rs) to wasm32-unknown-unknown. Apple clang
# and GCC have no wasm backend; upstream LLVM clang does. The `cc` crate picks
# the compiler + archiver from the per-target knobs CC_wasm32_unknown_unknown /
# AR_wasm32_unknown_unknown. EVERY wasm32 C build derives them from here:
#
#   * the dist wasm build (`make console-wasm`, via the Makefile's
#     WASM_READ_MODEL_ENV, which is `$(shell scripts/wasm-cc-env.sh)`),
#   * `make engine-wasm-check-read-model` (same variable),
#   * the lint-only wasm32 Clippy entries (`"wasmCc": true` in
#     scripts/clippy-matrix.json, run by scripts/clippy-matrix.mjs),
#   * CI, which installs LLVM and then runs `scripts/wasm-cc-env.sh
#     --github-env --require` so later steps inherit the same values.
#
# Resolution: an already-exported CC_/AR_wasm32_unknown_unknown wins (the
# caller's explicit choice); otherwise probe Homebrew LLVM (macOS — Apple clang
# has no usable wasm backend), then the distro LLVM (clang-18 / clang,
# llvm-ar-18 / llvm-ar), taking the first clang that lists a wasm32 backend.
#
# Usage:
#   scripts/wasm-cc-env.sh               print shell-quoted `CC_…=… AR_…=…` assignments
#                                        (nothing if absent) — safe to splice into a
#                                        shell command line, e.g. a Make recipe, even
#                                        when a value is `sccache clang` or a path
#                                        containing spaces
#   scripts/wasm-cc-env.sh --json        print `{"CC_…":"…","AR_…":"…"}` (lossless,
#                                        for scripts/clippy-matrix.mjs; `{}` if absent)
#   scripts/wasm-cc-env.sh --require     same, but exit 1 when no toolchain found
#   scripts/wasm-cc-env.sh --github-env  also append both to "$GITHUB_ENV"
set -euo pipefail

require=0
github_env=0
json=0
for arg in "$@"; do
  case "$arg" in
    --require) require=1 ;;
    --github-env) github_env=1 ;;
    --json) json=1 ;;
    *) echo "wasm-cc-env.sh: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

HOMEBREW_LLVM=/opt/homebrew/opt/llvm/bin

cc_bin="${CC_wasm32_unknown_unknown:-}"
if [ -z "$cc_bin" ]; then
  for c in "$HOMEBREW_LLVM/clang" "$(command -v clang-18 || true)" "$(command -v clang || true)"; do
    if [ -n "$c" ] && [ -x "$c" ] && "$c" --print-targets 2>/dev/null | grep -q wasm32; then
      cc_bin="$c"
      break
    fi
  done
fi

ar_bin="${AR_wasm32_unknown_unknown:-}"
if [ -z "$ar_bin" ]; then
  for a in "$HOMEBREW_LLVM/llvm-ar" "$(command -v llvm-ar-18 || true)" "$(command -v llvm-ar || true)"; do
    if [ -n "$a" ] && [ -x "$a" ]; then
      ar_bin="$a"
      break
    fi
  done
fi

if [ -z "$cc_bin" ] || [ -z "$ar_bin" ]; then
  if [ "$require" = 1 ]; then
    echo "wasm-cc-env.sh: no LLVM clang with a wasm32 backend (cc='$cc_bin') and llvm-ar (ar='$ar_bin') found" >&2
    exit 1
  fi
  if [ "$json" = 1 ]; then echo '{}'; fi
  exit 0
fi

# A newline cannot be carried by any output format ($GITHUB_ENV lines, Make's
# $(shell)), so reject it rather than silently truncate the value.
case "$cc_bin$ar_bin" in
  *$'\n'*) echo "wasm-cc-env.sh: CC/AR value contains a newline" >&2; exit 1 ;;
esac

json_str() {
  local v=${1//\\/\\\\}
  v=${v//\"/\\\"}
  printf '"%s"' "$v"
}

if [ "$github_env" = 1 ]; then
  : "${GITHUB_ENV:?--github-env needs GITHUB_ENV}"
  {
    echo "CC_wasm32_unknown_unknown=$cc_bin"
    echo "AR_wasm32_unknown_unknown=$ar_bin"
  } >> "$GITHUB_ENV"
fi
if [ "$json" = 1 ]; then
  printf '{"CC_wasm32_unknown_unknown":%s,"AR_wasm32_unknown_unknown":%s}\n' "$(json_str "$cc_bin")" "$(json_str "$ar_bin")"
else
  printf 'CC_wasm32_unknown_unknown=%q AR_wasm32_unknown_unknown=%q\n' "$cc_bin" "$ar_bin"
fi
