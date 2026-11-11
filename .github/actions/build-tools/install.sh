#!/usr/bin/env bash
# Implementation of .github/actions/build-tools (#1546). See action.yml.
set -euo pipefail

# Pinned release artifacts (x86_64 Linux) + their sha256.
MOLD_VERSION=3.0.0
MOLD_SHA256=6c90d4a474c7c0409dfb575be03a5345878ac14fdba18de8b40fa58c60121189
BINARYEN_VERSION=133
BINARYEN_SHA256=2dc9c7813f5375db93d96ead4b78222fcc3e2677bbb832297af4797782a37489

# Bounded retry with exponential backoff: retry <attempts> <cmd...>
retry() {
  local attempts=$1 n=1 delay=5
  shift
  until "$@"; do
    if [ "$n" -ge "$attempts" ]; then
      echo "build-tools: giving up after $n attempts: $*" >&2
      return 1
    fi
    echo "build-tools: attempt $n failed, retrying in ${delay}s: $*" >&2
    sleep "$delay"
    n=$((n + 1))
    delay=$((delay * 2))
  done
}

# The ONLY apt invocation in CI: retries per fetch, plus bounded backoff.
apt_install() {
  # Error-Mode=any: a failed index fetch (e.g. DNS) fails `update`, so the
  # bounded retry actually re-runs it instead of installing from stale indexes.
  retry 4 sudo apt-get -o Acquire::Retries=5 -o APT::Update::Error-Mode=any update
  retry 4 sudo apt-get -o Acquire::Retries=5 install -y --no-install-recommends "$@"
}

# Release archives are kept in an Actions-cached dir (action.yml), so a warm
# cache never touches GitHub Releases (#1461 failure mode). Keyed by sha256.
CACHE_DIR=${BUILD_TOOLS_CACHE:-$HOME/.cache/nanobpm-build-tools}

# Fetch a release tarball (cache miss only), verify its sha256 (always, also
# on a restored artifact), unpack into /usr/local.
release_install() {
  local url=$1 sha=$2 file
  mkdir -p "$CACHE_DIR"
  file="$CACHE_DIR/$sha.tar.gz"
  if [ -f "$file" ] && ! echo "$sha  $file" | sha256sum -c --status -; then
    echo "build-tools: cached archive fails sha256, refetching: $file" >&2
    rm -f "$file"
  fi
  if [ ! -f "$file" ]; then
    retry 4 curl -fsSL --retry 5 --retry-all-errors --connect-timeout 20 -o "$file.part" "$url"
    mv "$file.part" "$file"
  fi
  echo "$sha  $file" | sha256sum -c -
  sudo tar -xzf "$file" -C /usr/local --strip-components=1
}

if [ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ] && { [ "$WANT_MOLD" = true ] || [ "$WANT_BINARYEN" = true ]; }; then
  echo "build-tools: mold/binaryen release downloads are pinned for Linux x86_64 only" >&2
  exit 1
fi

if [ "$WANT_MOLD" = true ]; then
  release_install \
    "https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-x86_64-linux.tar.gz" \
    "$MOLD_SHA256"
  mold --version
fi

if [ "$WANT_BINARYEN" = true ]; then
  release_install \
    "https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/binaryen-version_${BINARYEN_VERSION}-x86_64-linux.tar.gz" \
    "$BINARYEN_SHA256"
  wasm-opt --version
fi

if [ "$WANT_LLVM" = true ]; then
  # The runner image ships LLVM clang + llvm-ar; apt only if it is missing.
  if ! scripts/wasm-cc-env.sh --require >/dev/null 2>&1; then
    echo "build-tools: no preinstalled wasm-capable LLVM; falling back to apt" >&2
    apt_install llvm clang
  fi
  scripts/wasm-cc-env.sh --github-env --require
fi

if [ -n "$EXTRA_APT" ]; then
  # shellcheck disable=SC2086 # intentional word-splitting of the package list
  apt_install $EXTRA_APT
fi
