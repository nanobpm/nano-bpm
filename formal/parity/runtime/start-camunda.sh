#!/usr/bin/env bash
# Start a dedicated, throwaway Camunda 8 for the live parity differential (#1408)
# and wait until its partition is healthy. Used by the daily workflow
# (.github/workflows/parity-camunda-daily.yml) and for local runs — one config,
# no drift.
#
#   formal/parity/runtime/start-camunda.sh [image] [port]
#   CAMUNDA_REST_ADDRESS=http://localhost:<port> \
#     node formal/parity/runtime/run.mjs --backend both
#   docker rm -f parity-camunda
#
# image defaults to the one derived from the Zeebe pin (camunda-image.mjs).
# Config notes:
#   * clock-controlled: the runner pins/advances the clock over /v2/clock.
#   * H2 in-memory secondary storage: `none` fails to boot the 8.10 webapp
#     beans; H2 is the lightest config that starts (as Camunda's own CI uses).
#   * Unprotected API + no authorizations: a throwaway, per-job container,
#     published on 127.0.0.1 only.
#   * 16 MB log segments, disk monitoring off: the default 1 GiB segment
#     preallocation fails on small CI/dev disks.
#   * JACOCO_DIR (optional, #1247): a host dir holding jacocoagent.jar. When
#     set it is mounted at /jacoco and the JaCoCo agent is attached to the
#     broker JVM (zeebe/engine classes only), dumping /jacoco/zeebe.exec on JVM
#     exit — see formal/parity/coverage/zeebe-coverage.sh. The periodic
#     message-TTL checker is shortened to 1 s so the background expiry pass is
#     reached deterministically within the coverage run's settle window instead
#     of depending on whether the default 1-minute tick happened to land
#     before shutdown (a measured run-to-run coverage difference).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
image="${1:-$(node "$here/camunda-image.mjs")}"
port="${2:-8080}"
name=parity-camunda

docker rm -f "$name" >/dev/null 2>&1 || true
jacoco=()
if [ -n "${JACOCO_DIR:-}" ]; then
  jacoco=(-v "$JACOCO_DIR:/jacoco"
    -e "JAVA_TOOL_OPTIONS=-javaagent:/jacoco/jacocoagent.jar=destfile=/jacoco/zeebe.exec,output=file,includes=io.camunda.zeebe.engine.*"
    -e ZEEBE_BROKER_EXPERIMENTAL_ENGINE_MESSAGES_TTLCHECKERINTERVAL=PT1S)
fi
# Loopback only: the API is unauthenticated, so never publish it beyond this host.
docker run -d --name "$name" -p "127.0.0.1:$port:8080" ${jacoco[@]+"${jacoco[@]}"} \
  -e CAMUNDA_SYSTEM_CLOCK_CONTROLLED=true \
  -e ZEEBE_BROKER_DATA_LOGSEGMENTSIZE=16MB \
  -e ZEEBE_BROKER_DATA_DISK_ENABLEMONITORING=false \
  -e CAMUNDA_DATA_SECONDARY_STORAGE_TYPE=rdbms \
  -e 'CAMUNDA_DATA_SECONDARY_STORAGE_RDBMS_URL=jdbc:h2:mem:parity;DB_CLOSE_DELAY=-1;MODE=PostgreSQL' \
  -e CAMUNDA_DATA_SECONDARY_STORAGE_RDBMS_USERNAME=sa \
  -e CAMUNDA_DATA_SECONDARY_STORAGE_RDBMS_PASSWORD= \
  -e CAMUNDA_DATA_SECONDARY_STORAGE_RDBMS_AUTO_DDL=true \
  -e CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI=true \
  -e CAMUNDA_SECURITY_AUTHORIZATIONS_ENABLED=false \
  "$image" >/dev/null
echo "started $image on :$port"

# Ready = the single partition reports a healthy leader. Bounded: a container
# that never gets there fails the job loudly with its logs.
for _ in $(seq 1 60); do
  if curl -sf -m 5 "http://localhost:$port/v2/topology" |
    grep -q '"role":"leader","health":"healthy"'; then
    echo "camunda ready"
    exit 0
  fi
  sleep 5
done
echo "camunda did not become ready within 300s" >&2
docker logs "$name" 2>&1 | tail -100 >&2
exit 1
