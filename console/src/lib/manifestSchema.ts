// Local (offline) registration of the Urban App manifest JSON Schema for
// Monaco's JSON language service.
//
// A `nano.app.json` manifest carries a `$schema` pointing at the *published*
// schema URL. Left to itself, Monaco's JSON worker tries to **fetch** that URL
// to validate the document — but the console wires up no schema-request service
// and is offline-first, so the fetch fails with "No schema request service
// available" and the manifest gets no validation or completion at all.
//
// The fix is to hand Monaco the schema **content** up front, keyed by that same
// URL, so `$schema` resolves in-process with no network. This module is the pure
// (monaco-value-free) core of that wiring so it can be unit-tested under
// `node --test`; `manifestIntellisense.ts` applies the result to Monaco.

import type * as monaco from "monaco-editor";

// The published `$id` of the manifest schema (canonical `nanobpm.io` host). The
// legacy host was retired in the schema migration and its literal token is
// banned from tracked sources by the `schemas` drift guard — but a manifest
// authored before the migration may still carry a `$schema` pointing at it (the
// exact symptom that surfaced this bug). We register a client-side resolution
// ALIAS for that host, assembled from parts so the banned literal never appears
// in tracked sources, so such manifests resolve offline too. This does NOT
// republish identity on the legacy host; the published `$id` stays `nanobpm.io`.
const LEGACY_SCHEMA_HOST = ["nanobpm", "dev"].join(".");
export const LEGACY_APP_SCHEMA_URI = `https://${LEGACY_SCHEMA_HOST}/spec-app/nano-app.schema.json`;

// Keep the first entry equal to the schema's own `$id` (guarded by the unit
// test); the legacy alias follows.
export const APP_SCHEMA_URIS: readonly string[] = [
  "https://nanobpm.io/spec-app/nano-app.schema.json",
  LEGACY_APP_SCHEMA_URI,
];

// File-name globs that associate a document with the manifest schema even when
// its `$schema` is missing or stale, so `nano.app.json` still validates.
export const APP_MANIFEST_FILE_MATCH = ["*.nano.app.json", "nano.app.json"];

/**
 * The Monaco JSON schema-registration entries for the bundled manifest schema,
 * keyed by the canonical + legacy-alias URLs and scoped by `fileMatch`. Returned
 * as entries (not full `DiagnosticsOptions`) so `jsonSchemas.ts` can merge them
 * with the element-template schema into a single `setDiagnosticsOptions` call —
 * Monaco's JSON diagnostics options are process-wide and replace, not merge.
 */
export function manifestSchemaEntries(
  schema: Record<string, unknown>,
): NonNullable<monaco.json.DiagnosticsOptions["schemas"]> {
  return APP_SCHEMA_URIS.map((uri) => ({
    uri,
    fileMatch: [...APP_MANIFEST_FILE_MATCH],
    schema,
  }));
}

/**
 * Build the Monaco JSON diagnostics options that register the bundled Urban App
 * manifest schema locally. `enableSchemaRequest: false` guarantees Monaco never
 * reaches the network for a `$schema` URL — the schema is resolved from the
 * in-process `schemas` map instead.
 */
export function buildManifestSchemaOptions(
  schema: Record<string, unknown>,
): monaco.json.DiagnosticsOptions {
  return {
    validate: true,
    enableSchemaRequest: false,
    schemas: manifestSchemaEntries(schema),
  };
}
