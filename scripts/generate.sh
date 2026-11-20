#!/usr/bin/env bash
#
# Generate the Rust REST layer (models + axum router + Api traits) and the server
# stub implementations from the bundled OpenAPI specification.
#
# This project is self-contained: the spec lives under spec/ and the generator
# runs from the official openapi-generator-cli JAR via the local Java runtime —
# no Docker and no global install required. The JAR is downloaded once (pinned by
# version) into a git-ignored cache under build/.
#
# Output (generated/ and server/src/stub_impls.rs) and the temporary sanitized
# spec (build/spec/) are build artifacts and are git-ignored: regenerated on
# demand from the spec.
set -euo pipefail

# Resolve paths relative to the project root so the script works from anywhere.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SPEC_DIR_REL="spec"
SPEC_ENTRYPOINT="rest-api.yaml"
# The source spec under spec/ is never edited. It is first sanitized (and local
# overlays from spec-patches/ are applied) into a temporary build directory (see
# preprocess-spec.py) that the generator consumes.
SANITIZED_SPEC_DIR_REL="build/spec"
SPEC_REL="${SANITIZED_SPEC_DIR_REL}/${SPEC_ENTRYPOINT}"
OUTPUT_REL="generated"
CONFIG_REL="openapi-generator-config.yaml"
PATCHES_REL="spec-patches/patches.yaml"

# The pinned generator JAR is cached under build/tools (git-ignored). Location
# and (rate-limit tolerant) download live in the shared helper so generate.sh
# and generate-console.sh never drift apart.
# shellcheck source=scripts/lib-openapi-generator.sh
source "${SCRIPT_DIR}/lib-openapi-generator.sh"

if ! command -v java >/dev/null 2>&1; then
  echo "error: 'java' not found on PATH. Install a JRE/JDK (Java 11+) to run the" >&2
  echo "       openapi-generator CLI, or set JAVA_HOME and add java to PATH." >&2
  exit 1
fi

# Run the Python helpers through uv (which provisions a locked environment with
# PyYAML from pyproject.toml). Fall back to the system python3 if uv is absent —
# in that case PyYAML must already be importable. Run `make setup` to provision.
if command -v uv >/dev/null 2>&1; then
  PY=(uv run --project "${PROJECT_ROOT}" python)
else
  echo "warning: 'uv' not found; falling back to system python3 (needs PyYAML installed)." >&2
  echo "         Run 'make setup' to provision the build environment with uv." >&2
  PY=(python3)
fi

# Fetch the generator JAR once, into the build cache (shared with generate.sh).
ensure_openapi_generator_jar

echo "Sanitizing spec into ${SANITIZED_SPEC_DIR_REL}"
"${PY[@]}" "${SCRIPT_DIR}/preprocess-spec.py" \
  "${PROJECT_ROOT}/${SPEC_DIR_REL}" \
  "${PROJECT_ROOT}/${SANITIZED_SPEC_DIR_REL}" \
  "${PROJECT_ROOT}/${PATCHES_REL}"

echo "Generating Rust REST layer with openapi-generator-cli ${OPENAPI_GENERATOR_VERSION} (local Java)"
echo "  spec:   ${SPEC_REL}"
echo "  output: ${OUTPUT_REL}"

# The spec entrypoint $refs sibling YAML files; pointing the generator at the
# sanitized copy under the project root keeps those relative references
# resolvable.
#
# The rust-axum generator drowns real diagnostics under ~2350 identical benign
# "identifier renamed" WARNs and per-file "writing file" INFO lines. Pipe its
# (merged) output through openapi-log-filter.awk, which hides ONLY those exact
# known-noise shapes, passes every other line through, tees the full unfiltered
# output to build/openapi-generate.log, and prints a count of what it hid. Set
# GEN_VERBOSE=1 to see everything. The awk filter's exit status is independent
# of java's, so the `set -euo pipefail` at the top of this script is what
# preserves the generator's non-zero exit through the pipe.
GEN_LOG="${PROJECT_ROOT}/build/openapi-generate.log"
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

echo "Generating stub trait implementations for the server"
"${PY[@]}" "${SCRIPT_DIR}/gen-stub-server.py" \
  "${PROJECT_ROOT}/${OUTPUT_REL}/src/apis" \
  "${PROJECT_ROOT}/server/src/stub_impls.rs"

# Spec-generated response-contract bundle (issue #1011): per-(operationId,status)
# response-body schemas the gateway validates its own responses against. Derived
# from the SAME sanitized spec the generator consumes, so it never drifts; the
# implemented-operation set is imported from gen-stub-server.py's OVERRIDES. Like
# stub_impls.rs it is git-ignored and regenerated here.
echo "Generating spec-derived response contract for the server"
"${PY[@]}" "${SCRIPT_DIR}/gen-response-contract.py" \
  "${PROJECT_ROOT}/${SANITIZED_SPEC_DIR_REL}" \
  "${SPEC_ENTRYPOINT}" \
  "${PROJECT_ROOT}/server/src/response_contract_schema.json"

# Format the generated artifacts with the host toolchain. Only generated files
# are formatted: the whole generated/ crate, and the single generated
# server/src/stub_impls.rs. The server's hand-written sources are left untouched
# so regeneration never churns tracked, manually formatted code.
# Format the generated Rust. rustfmt.toml enables the unstable `group_imports`
# option, which only the pinned nightly rustfmt (the `make fmt` / CI fmt gate —
# see FMT_TOOLCHAIN in the Makefile) can apply. Stable rustfmt silently ignores it
# and prints a noisy "can't set `group_imports` … unstable features are only
# available in nightly channel" warning on every `make`/generate. Prefer the
# pinned nightly when it's installed (so generated output matches CI); otherwise
# fall back to the default toolchain and drop that one expected warning.
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
# not on a bare PATH shim: `rustup run <toolchain> cargo/rustfmt` can succeed
# even when `cargo`/`rustfmt` aren't on PATH (and vice-versa).
if run_fmt cargo --version >/dev/null 2>&1; then
  echo "Formatting generated crate with cargo fmt${FMT_LABEL}"
  ( cd "${PROJECT_ROOT}/${OUTPUT_REL}" && run_fmt cargo fmt ) 2>&1 | strip_fmt_warn || true
fi
if run_fmt rustfmt --version >/dev/null 2>&1; then
  echo "Formatting generated server stub impls with rustfmt${FMT_LABEL}"
  run_fmt rustfmt --edition 2024 "${PROJECT_ROOT}/server/src/stub_impls.rs" 2>&1 | strip_fmt_warn || true
fi

echo "Done. Generated crate is in ${OUTPUT_REL}"
