#!/usr/bin/env bash
#
# Generate the Rust REST layer (models + axum router + Api traits) for the
# **console API** from spec-console/console-api.yaml.
#
# This mirrors scripts/generate.sh (which generates the Camunda REST surface
# under spec/), but for the hand-authored console API. The console spec is a
# single self-contained file, so there is no multi-file preprocess/patch step.
#
# Output (generated-console/) is a git-ignored build artifact: regenerated on
# demand from the spec. The server crate depends on it under the `console`
# feature and implements the generated per-tag Api traits.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SPEC_REL="spec-console/console-api.yaml"
OUTPUT_REL="generated-console"
CONFIG_REL="openapi-generator-config-console.yaml"

# JAR location and (rate-limit tolerant) download live in the shared helper so
# generate.sh and generate-console.sh never drift apart.
# shellcheck source=scripts/lib-openapi-generator.sh
source "${SCRIPT_DIR}/lib-openapi-generator.sh"

if ! command -v java >/dev/null 2>&1; then
  echo "error: 'java' not found on PATH. Install a JRE/JDK (Java 11+) to run the" >&2
  echo "       openapi-generator CLI, or set JAVA_HOME and add java to PATH." >&2
  exit 1
fi

if command -v uv >/dev/null 2>&1; then
  PY=(uv run --project "${PROJECT_ROOT}" python)
else
  echo "warning: 'uv' not found; falling back to system python3 (needs PyYAML installed)." >&2
  PY=(python3)
fi

# Fetch the generator JAR once, into the build cache (shared with generate.sh).
ensure_openapi_generator_jar

echo "Generating console API Rust layer with openapi-generator-cli ${OPENAPI_GENERATOR_VERSION} (local Java)"
echo "  spec:   ${SPEC_REL}"
echo "  output: ${OUTPUT_REL}"

# Filter the generator's benign identifier-rename WARNs + per-file INFO progress
# (see scripts/openapi-log-filter.awk); the full unfiltered log is teed to
# build/openapi-generate-console.log and a count of hidden lines is printed. Any
# other message passes through. GEN_VERBOSE=1 shows everything.
GEN_LOG="${PROJECT_ROOT}/build/openapi-generate-console.log"
mkdir -p "$(dirname "${GEN_LOG}")"
: >"${GEN_LOG}"
java -jar "${JAR}" generate \
  -g rust-axum \
  -i "${PROJECT_ROOT}/${SPEC_REL}" \
  -o "${PROJECT_ROOT}/${OUTPUT_REL}" \
  -c "${PROJECT_ROOT}/${CONFIG_REL}" \
  --skip-validate-spec \
  2>&1 | awk -f "${SCRIPT_DIR}/openapi-log-filter.awk" -v raw="${GEN_LOG}" -v verbose="${GEN_VERBOSE:-}"

echo "Post-processing generated code to fix known rust-axum generator bugs"
"${PY[@]}" "${SCRIPT_DIR}/postprocess-generated.py" "${PROJECT_ROOT}/${OUTPUT_REL}"

# Format the generated console crate. rustfmt.toml enables the unstable
# `group_imports` option, which only the pinned nightly rustfmt (the `make fmt`
# / CI fmt gate — see FMT_TOOLCHAIN in the Makefile) can apply. Stable rustfmt
# silently ignores it and prints a noisy "can't set `group_imports` … unstable
# features are only available in nightly channel" warning on every generate.
# Prefer the pinned nightly when installed (so generated output matches CI);
# otherwise fall back to the default toolchain and drop that one expected
# warning. This mirrors scripts/generate.sh.
FMT_TOOLCHAIN="${FMT_TOOLCHAIN:-nightly-2026-06-26}"
FMT_LABEL=""
if command -v rustup >/dev/null 2>&1 && rustup run "${FMT_TOOLCHAIN}" rustfmt --version >/dev/null 2>&1; then
  FMT_LABEL=" (${FMT_TOOLCHAIN})"
  run_fmt() { rustup run "${FMT_TOOLCHAIN}" "$@"; }
else
  run_fmt() { "$@"; }
fi
# Suppress ONLY the expected stable-channel warning about the unstable
# group_imports option (harmless: stable can't apply it, and the nightly path
# never emits it). Require both substrings on the same line so unrelated future
# rustfmt diagnostics are never accidentally swallowed.
strip_fmt_warn() { grep -v -E 'group_imports.*unstable features are only available in nightly channel' || true; }

# Gate on whether the SELECTED formatter toolchain can actually run the tool,
# not on a bare PATH shim: `rustup run <toolchain> cargo` can succeed even when
# `cargo` isn't on PATH (and vice-versa).
if run_fmt cargo --version >/dev/null 2>&1; then
  echo "Formatting generated console crate with cargo fmt${FMT_LABEL}"
  (cd "${PROJECT_ROOT}/${OUTPUT_REL}" && run_fmt cargo fmt) 2>&1 | strip_fmt_warn || true
fi

echo "Done. Generated console crate is in ${OUTPUT_REL}"
