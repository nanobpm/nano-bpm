#!/usr/bin/env bash
#
# Shared helper for locating, fetching and verifying the pinned
# openapi-generator-cli JAR.
#
# Sourced by scripts/generate.sh and scripts/generate-console.sh so the pinned
# version, its SHA-256, the JAR location and the (outage tolerant) download
# logic live in exactly one place — no drift between the two generators. CI
# caches build/tools via .github/actions/openapi-generator-cache, keyed on this
# file's hash, so bumping the pin below busts the cache (#1684).
#
# Contract: the caller must have PROJECT_ROOT set. After sourcing, call
# `ensure_openapi_generator_jar` to populate the `JAR` variable with a
# ready-to-use, checksum-verified path, downloading it into the build cache
# (build/tools, git-ignored) on first use.

# shellcheck shell=bash

# The single source of truth for the generator pin. A leading "v" is tolerated
# (the former Docker image tag used it); the Maven coordinate has no "v".
OPENAPI_GENERATOR_DEFAULT_VERSION="7.21.0"
OPENAPI_GENERATOR_DEFAULT_SHA256="124727eacdb64fd6c9c1c3a78e353207ec1378d071bbdc15de79b0a30ed2ad6d"

OPENAPI_GENERATOR_VERSION="${OPENAPI_GENERATOR_VERSION:-${OPENAPI_GENERATOR_DEFAULT_VERSION}}"
OPENAPI_GENERATOR_VERSION="${OPENAPI_GENERATOR_VERSION#v}"

# Overriding the version requires supplying its checksum: an unverified JAR is
# never executed.
if [[ -z "${OPENAPI_GENERATOR_SHA256:-}" ]]; then
  if [[ "${OPENAPI_GENERATOR_VERSION}" == "${OPENAPI_GENERATOR_DEFAULT_VERSION}" ]]; then
    OPENAPI_GENERATOR_SHA256="${OPENAPI_GENERATOR_DEFAULT_SHA256}"
  else
    echo "error: OPENAPI_GENERATOR_VERSION=${OPENAPI_GENERATOR_VERSION} overrides the pin; set OPENAPI_GENERATOR_SHA256 to its SHA-256 too." >&2
    exit 1
  fi
fi

# Print the SHA-256 of $1.
_openapi_generator_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "error: neither 'sha256sum' nor 'shasum' is available to verify the generator JAR." >&2
    exit 1
  fi
}

# Succeeds iff $1 matches the pinned checksum; otherwise reports the mismatch.
verify_openapi_generator_jar() {
  local actual
  actual="$(_openapi_generator_sha256 "$1")"
  if [[ "${actual}" != "${OPENAPI_GENERATOR_SHA256}" ]]; then
    echo "error: SHA-256 mismatch for $1" >&2
    echo "  expected ${OPENAPI_GENERATOR_SHA256}" >&2
    echo "  actual   ${actual}" >&2
    return 1
  fi
}

ensure_openapi_generator_jar() {
  local tools_dir="${PROJECT_ROOT}/build/tools"
  local jar_path="org/openapitools/openapi-generator-cli/${OPENAPI_GENERATOR_VERSION}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"
  # Primary Maven Central host, then its alternate hostname as a fallback.
  local mirrors=(
    "https://repo1.maven.org/maven2"
    "https://repo.maven.apache.org/maven2"
  )
  JAR="${tools_dir}/openapi-generator-cli-${OPENAPI_GENERATOR_VERSION}.jar"

  if [[ -f "${JAR}" ]]; then
    # A cache hit must never serve a corrupt or partial file: fail loudly.
    if ! verify_openapi_generator_jar "${JAR}"; then
      echo "error: cached generator JAR failed verification; delete ${JAR} (or the CI cache entry) and retry." >&2
      exit 1
    fi
    return 0
  fi

  echo "Downloading openapi-generator-cli ${OPENAPI_GENERATOR_VERSION} into build/tools"
  mkdir -p "${tools_dir}"

  # Maven Central occasionally rate-limits (HTTP 429) or hiccups, which must not
  # fail the whole build: try each mirror, retrying with backoff.
  local fetch_jar
  if command -v curl >/dev/null 2>&1; then
    fetch_jar() {
      curl -fsSL --retry 3 --retry-delay 2 --retry-connrefused --retry-all-errors \
        -o "${JAR}.tmp" "$1"
    }
  elif command -v wget >/dev/null 2>&1; then
    fetch_jar() {
      wget -q --tries=3 --waitretry=2 -O "${JAR}.tmp" "$1"
    }
  else
    echo "error: neither 'curl' nor 'wget' is available to download the generator JAR." >&2
    exit 1
  fi

  local attempts=3
  local attempt mirror
  for attempt in $(seq 1 "${attempts}"); do
    for mirror in "${mirrors[@]}"; do
      if fetch_jar "${mirror}/${jar_path}"; then
        # A downloaded file with the wrong checksum is a hard failure, never retried.
        if ! verify_openapi_generator_jar "${JAR}.tmp"; then
          rm -f "${JAR}.tmp"
          exit 1
        fi
        mv "${JAR}.tmp" "${JAR}"
        return 0
      fi
      echo "warning: download from ${mirror} failed (attempt ${attempt}/${attempts})." >&2
      rm -f "${JAR}.tmp"
    done
    if [[ "${attempt}" -lt "${attempts}" ]]; then
      sleep $(( attempt * 5 ))
    fi
  done
  echo "error: failed to download the generator JAR after ${attempts} attempts across ${#mirrors[@]} mirrors." >&2
  exit 1
}
