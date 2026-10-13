// Local (offline) registration of the Camunda Zeebe **element-template** JSON
// Schema for Monaco's JSON language service — the element-template analog of
// `manifestSchema.ts`.
//
// A component file under a project's `components/` or `.camunda/element-templates/`
// dir carries a `$schema` pointing at the *published* schema URL
// (`@camunda/zeebe-element-templates-json-schema` on unpkg, see
// `urbanComponents.ts`). Left to itself, Monaco's JSON worker tries to **fetch**
// that URL to validate the document — but the console wires up no schema-request
// service and is offline-first, so the fetch fails ("No schema request service
// available") and the template gets no validation or completion.
//
// The fix mirrors the manifest schema: hand Monaco the schema **content** up
// front, keyed by that same URL, so `$schema` resolves in-process with no
// network. This module is the pure (monaco-value-free) core so it can be
// unit-tested under `node --test`; `jsonSchemas.ts` applies the result to Monaco.

import type * as monaco from "monaco-editor";

// The pinned schema version bundled offline. Kept as a named constant so the
// `$schema` URL below and the seed templates (`urbanComponents.ts`, which
// imports `ELEMENT_TEMPLATE_SCHEMA_URL`) can never drift from the version whose
// content we actually register. Bump this together with the
// `@camunda/zeebe-element-templates-json-schema` dependency.
export const ELEMENT_TEMPLATE_SCHEMA_VERSION = "0.44.0";

// The `$schema` URL our seed/authored element templates point at. Monaco keys
// schema resolution off this exact string, so it MUST equal the `$schema`
// written into element-template files — `urbanComponents.ts` imports this same
// constant to guarantee they can't diverge.
export const ELEMENT_TEMPLATE_SCHEMA_URL = `https://unpkg.com/@camunda/zeebe-element-templates-json-schema@${ELEMENT_TEMPLATE_SCHEMA_VERSION}/resources/schema.json`;

// The schema document's own `$id`. Registered as a resolution alias so a
// template that points its `$schema` at the canonical Camunda id (rather than
// the unpkg mirror) resolves offline too.
export const ELEMENT_TEMPLATE_SCHEMA_ID =
  "http://camunda.org/schema/zeebe-element-templates/1.0";

// Keep the first entry equal to the published `$schema` our templates carry
// (guarded by the unit test); the canonical `$id` alias follows.
export const ELEMENT_TEMPLATE_SCHEMA_URIS: readonly string[] = [
  ELEMENT_TEMPLATE_SCHEMA_URL,
  ELEMENT_TEMPLATE_SCHEMA_ID,
];

// File globs that associate a `.json` document with the element-template schema
// even when its `$schema` is missing or stale, so a component still validates.
// Recursive to mirror `projectComponents` (which collects every `.json` *under*
// `.camunda/element-templates` + `components`, nested paths included); kept as
// literals here so this pure module stays free of the `../gen`/`./api` imports
// that module pulls in.
export const ELEMENT_TEMPLATE_FILE_MATCH = [
  "**/components/**/*.json",
  "**/.camunda/element-templates/**/*.json",
];

/**
 * The Monaco JSON schema-registration entries for the bundled element-template
 * schema, keyed by the published `$schema` URL (+ the canonical `$id` alias) and
 * scoped by `fileMatch` to the component dirs. Returned as entries (not full
 * `DiagnosticsOptions`) so `jsonSchemas.ts` can merge them with the manifest
 * schema into a single `setDiagnosticsOptions` call — Monaco's JSON diagnostics
 * options are process-wide and replace, not merge.
 */
export function elementTemplateSchemaEntries(
  schema: Record<string, unknown>,
): NonNullable<monaco.json.DiagnosticsOptions["schemas"]> {
  return ELEMENT_TEMPLATE_SCHEMA_URIS.map((uri) => ({
    uri,
    fileMatch: [...ELEMENT_TEMPLATE_FILE_MATCH],
    schema,
  }));
}
