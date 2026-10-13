#!/usr/bin/env bash
#
# Generate the TypeScript AppManifest types from the Urban App manifest schema
# (spec-app/nano-app.schema.json — ADR 0027, spec-first). This mirrors
# scripts/generate-console.sh but for the manifest: one JSON Schema is the
# single source of truth, and the TypeScript types are generated from it so a
# hand-written DTO can never drift from the schema.
#
# The Urban App is a Node/TypeScript binary, so the manifest is consumed only by
# TypeScript (the console App panels + the Node App loader) — this emits TS, not
# Rust (ADR 0027 §1/§3).
#
# Output: spec-app/gen/nano-app.d.ts (committed). Also validates the example
# manifests against the schema (fail-closed, ADR 0027 §4).
#
# Usage: scripts/generate-app-manifest.sh          (regenerate + validate)
#        scripts/generate-app-manifest.sh --check   (fail if committed output is stale)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SPEC_DIR="${PROJECT_ROOT}/spec-app"

if ! command -v npm >/dev/null 2>&1; then
  echo "error: 'npm' not found on PATH. Install Node.js (18+) to generate the" >&2
  echo "       Urban App manifest TypeScript types." >&2
  exit 1
fi

cd "${SPEC_DIR}"

if [[ ! -d node_modules ]]; then
  echo "Installing spec-app dev dependencies (ajv, json-schema-to-typescript, esbuild)"
  npm install --no-audit --no-fund
fi

echo "Validating example manifests against nano-app.schema.json"
npm run --silent validate

if [[ "${1:-}" == "--check" ]]; then
  echo "Checking generated TypeScript types are up to date"
  npm run --silent gen -- --check
else
  echo "Generating TypeScript AppManifest types into spec-app/gen/"
  npm run --silent gen
fi

# Build the browser-consumable package artifact (dist/index.js + dist/index.d.ts)
# — a self-contained ESM bundle + flattened declarations the console imports
# without re-bundling the moddle parsers. Committed like gen/ and console/dist.
echo "Building spec-app dist/ (bundled ESM + declarations)"
npm run --silent build

if [[ "${1:-}" == "--check" ]]; then
  echo "Checking committed spec-app/dist/ is up to date"
  if ! git -C "${PROJECT_ROOT}" diff --exit-code -- spec-app/dist; then
    echo "error: spec-app/dist/ is stale. Run 'make generate-app-manifest'" >&2
    echo "       (or 'npm run build' in spec-app/) and commit the result." >&2
    exit 1
  fi
fi

echo "Done."
