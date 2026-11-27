#!/usr/bin/env bash
# Camunda half of the dual-engine coverage measurement (#1247): start a live
# Camunda 8 (start-camunda.sh) with the JaCoCo agent attached, play the parity
# corpus through the camunda backend, stop the broker (the agent dumps on JVM
# exit) and render a JaCoCo XML report for `zeebe/engine` (the XML carries
# package/sourcefile/line names, which report.mjs maps onto the pinned
# zeebe-surface.json sources — no source checkout needed).
#
#   formal/parity/coverage/zeebe-coverage.sh [out.xml] [exhaustive-bound]
#   node formal/parity/coverage/report.mjs --zeebe out.xml --check
#
# Needs docker, java, node, and the engine-wasm read-model pkg (the runner
# always plays nano as the reference). No retries: a failing run is a defect.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
out="${1:-$repo/target/parity-coverage/zeebe-jacoco.xml}"
bound="${2:-ci}"
port="${PARITY_CAMUNDA_PORT:-8080}"
# Pinned, immutable artifacts (#1770): fetched once into a cache keyed on the
# version (the workflow caches this dir with actions/cache) and SHA-256
# verified, so a run makes no network request and fails closed on a mismatch.
# Bump the version and both hashes together.
JACOCO_VERSION="0.8.13"
JACOCO_AGENT_SHA256="47e700ccb0fdb9e27c5241353f8161938f4e53c3561dd35e063c5fe88dc3349b"
JACOCO_CLI_SHA256="8f748683833d4dc4d72cea5d6b43f49344687b831e0582c97bcb9b984e3de0a3"
mkdir -p "$(dirname "$out")"
out="$(cd "$(dirname "$out")" && pwd)/$(basename "$out")"

# Under the repo (not $TMPDIR): docker-on-VM setups (colima, Docker Desktop)
# only share the home tree into the VM, and /jacoco must be a real bind mount.
mkdir -p "$repo/target"
work="$(mktemp -d "$repo/target/parity-zeebe.XXXXXX")"
trap 'docker rm -f parity-camunda >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

# shellcheck source=fetch-pinned.sh
source "$here/fetch-pinned.sh"

# Maven Central first, then Google's Central mirror (same bytes; the SHA-256
# pin makes any mirror safe). The workflow restores/saves the cache dir, so a
# PR run normally makes no request at all (#2079).
mirrors=(https://repo1.maven.org/maven2 https://maven-central.storage-download.googleapis.com/maven2)
cache="${JACOCO_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/nanobpm-jacoco/$JACOCO_VERSION}"
mkdir -p "$cache" "$work/jacoco"
agent_path="org/jacoco/org.jacoco.agent/$JACOCO_VERSION/org.jacoco.agent-$JACOCO_VERSION-runtime.jar"
cli_path="org/jacoco/org.jacoco.cli/$JACOCO_VERSION/org.jacoco.cli-$JACOCO_VERSION-nodeps.jar"
agent_urls=() cli_urls=()
for m in "${mirrors[@]}"; do agent_urls+=("$m/$agent_path"); cli_urls+=("$m/$cli_path"); done
fetch_pinned "org.jacoco.agent-$JACOCO_VERSION-runtime.jar" "$cache/jacocoagent.jar" "$JACOCO_AGENT_SHA256" "${agent_urls[@]}"
fetch_pinned "org.jacoco.cli-$JACOCO_VERSION-nodeps.jar" "$cache/jacococli.jar" "$JACOCO_CLI_SHA256" "${cli_urls[@]}"
# --fetch-only: populate the cache and stop (the workflow saves it right away,
# so a later failure in the measurement cannot lose it).
if [[ "${JACOCO_FETCH_ONLY:-}" == 1 ]]; then echo "JaCoCo jars cached in $cache"; exit 0; fi
# Copy (not mount) the agent: /jacoco is bind-mounted and written by the container.
cp "$cache/jacocoagent.jar" "$work/jacoco/jacocoagent.jar"
cp "$cache/jacococli.jar" "$work/jacococli.jar"
# The container user must be able to write the exec dump.
chmod 0777 "$work/jacoco"

node "$repo/formal/corpus/exhaustive.mjs" --bound "$bound" --out "$work/exhaustive"

JACOCO_DIR="$work/jacoco" "$repo/formal/parity/runtime/start-camunda.sh" "" "$port"
export CAMUNDA_REST_ADDRESS="http://localhost:$port"
node "$repo/formal/parity/runtime/run.mjs" --backend both --require-camunda
node "$repo/formal/corpus/exhaustive.mjs" --bound "$bound" --backend both --require-camunda

# Settle: let every periodic engine checker (message TTL at 1 s, job
# deadlines/back-off at their 1 s defaults) tick over the final state, so the
# background passes are measured deterministically rather than by whether a
# tick happened to land before shutdown.
sleep "${PARITY_COVERAGE_SETTLE_S:-15}"

# Locate the engine classes before stopping, then stop gracefully so the agent
# writes its dump (output=file dumps on JVM shutdown).
jar="$(docker exec parity-camunda sh -c 'find / -name "zeebe-workflow-engine-*.jar" 2>/dev/null | head -1')"
[[ -n "$jar" ]] || { echo "zeebe-workflow-engine jar not found in the container" >&2; exit 1; }
docker cp "parity-camunda:$jar" "$work/engine.jar"
docker stop -t 120 parity-camunda >/dev/null
[[ -s "$work/jacoco/zeebe.exec" ]] || { echo "JaCoCo produced no exec dump" >&2; exit 1; }

java -jar "$work/jacococli.jar" report "$work/jacoco/zeebe.exec" \
  --classfiles "$work/engine.jar" \
  --xml "$out" >/dev/null
echo "wrote $out"
