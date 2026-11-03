#!/usr/bin/env bash
# The reproducible Zeebe engine-test harvest (#1249): fetch the pinned test
# sources, harvest + serialise the fluent-builder models through the Zeebe model
# API, normalise them into the corpus format (BPMN+DI + scenario), run them
# differentially against Nano, and emit the report.
#
#   formal/harvest/harvest.sh            # full pipeline -> formal/harvest/corpus + report.json
#
# Pinned to the Zeebe revision in zeebe-test-pin.json. Requires: node, a JDK
# (javac/java) for the model-API serialisation, git, curl, sha256sum, and the
# committed engine-wasm artifacts (make console-wasm) for the differential run.
#
# The committed corpus/ and report.json are a DERIVED artifact: CI regenerates
# them and `git diff --exit-code`s the tree, so a forgotten regeneration fails
# the build instead of shipping drift (the same discipline as formal/parity).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
corpus="$here/corpus"

checkout="$("$here/fetch-tests.sh")"

# Harvest + serialise through the Zeebe model API.
node "$here/harvest.mjs" "$checkout" --out "$corpus"
# Normalise into the corpus format (BPMN+DI + scenario + coverage tags).
node "$here/normalise.mjs" --corpus "$corpus"
# Run differentially against Nano.
node "$here/run.mjs" --corpus "$corpus"
# Emit the acceptance report.
node "$here/report.mjs" --corpus "$corpus" --out "$here/report.json"
