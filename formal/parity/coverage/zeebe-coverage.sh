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
JACOCO_VERSION="${JACOCO_VERSION:-0.8.13}"
mkdir -p "$(dirname "$out")"
out="$(cd "$(dirname "$out")" && pwd)/$(basename "$out")"

# Under the repo (not $TMPDIR): docker-on-VM setups (colima, Docker Desktop)
# only share the home tree into the VM, and /jacoco must be a real bind mount.
mkdir -p "$repo/target"
work="$(mktemp -d "$repo/target/parity-zeebe.XXXXXX")"
trap 'docker rm -f parity-camunda >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

base="https://repo1.maven.org/maven2/org/jacoco"
mkdir -p "$work/jacoco"
curl -fsSL "$base/org.jacoco.agent/$JACOCO_VERSION/org.jacoco.agent-$JACOCO_VERSION-runtime.jar" -o "$work/jacoco/jacocoagent.jar"
curl -fsSL "$base/org.jacoco.cli/$JACOCO_VERSION/org.jacoco.cli-$JACOCO_VERSION-nodeps.jar" -o "$work/jacococli.jar"
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
