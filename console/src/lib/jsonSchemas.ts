// Monaco glue for the bundled JSON schemas. Imports the schema *content* for the
// Urban App manifest and the Camunda element template and registers both with
// Monaco's JSON language service in one process-wide call, so a `$schema` URL in
// either kind of file resolves offline (no "No schema request service
// available"). The per-schema entry shapes live in the pure, unit-tested leaf
// modules (`manifestSchema.ts`, `elementTemplateSchema.ts`); this module holds
// the monaco-editor value import that can't run under `node --test`, plus the
// trivial merge — Monaco's JSON diagnostics options are process-wide and
// **replace** (not merge), so every bundled schema must go in this one call.

import * as monaco from "monaco-editor";
import appSchema from "@nanobpm/nano-app-schema/schema";
import elementTemplateSchema from "@camunda/zeebe-element-templates-json-schema/resources/schema.json";
import { manifestSchemaEntries } from "./manifestSchema";
import { elementTemplateSchemaEntries } from "./elementTemplateSchema";

let registered = false;

/**
 * Register every bundled JSON schema (Urban App manifest + Camunda element
 * template) with Monaco as in-process content, and disable remote schema
 * requests. Idempotent, and process-wide for all JSON models — so both a
 * `nano.app.json` manifest and a `components/*.json` element template validate +
 * autocomplete against their bundled schema offline. This is the fix for the
 * "No schema request service available" error Monaco raises when it would
 * otherwise fetch a document's `$schema` URL.
 *
 * Call this from any editor entry point that opens JSON (the manifest editor and
 * the generic code editor both do), not just the manifest path — an element
 * template can be edited without a manifest ever being open.
 */
export function ensureBundledJsonSchemas(): void {
  if (registered) return;
  // Mark registered only after the call succeeds, so a transient failure (e.g.
  // Monaco JSON defaults not yet initialized) doesn't permanently prevent a
  // later retry from registering the schemas for the session.
  monaco.json.jsonDefaults.setDiagnosticsOptions({
    validate: true,
    enableSchemaRequest: false,
    schemas: [
      ...manifestSchemaEntries(appSchema as Record<string, unknown>),
      ...elementTemplateSchemaEntries(
        elementTemplateSchema as Record<string, unknown>,
      ),
    ],
  });
  registered = true;
}
