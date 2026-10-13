// Generate the TypeScript AppManifest types from nano-app.schema.json
// (ADR 0027 §3, spec-first). The manifest is consumed only by TypeScript —
// the console App panels and the Node App loader — so this emits TS, not Rust
// (ADR 0027 §1: nano.app.json is read by the compiled App + console panels,
// not the Rust server).
//
// Run:  npm run gen              # write gen/nano-app.d.ts
//       npm run gen -- --check   # fail if the committed output is stale
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { compile } from "json-schema-to-typescript";

const here = dirname(fileURLToPath(import.meta.url));
const specDir = join(here, "..");
const schemaPath = join(specDir, "nano-app.schema.json");
const outDir = join(specDir, "gen");
const outPath = join(outDir, "nano-app.d.ts");

const banner = `/**
 * GENERATED — do not edit by hand.
 *
 * TypeScript types for the Urban App manifest (nano.app.json), generated from
 * spec-app/nano-app.schema.json (ADR 0027). Regenerate with:  npm run gen
 * (from spec-app/) or  make generate-app-manifest  (from the repo root).
 */
`;

// json-schema-to-typescript escapes the block-comment terminator `*/` inside
// JSDoc by inserting a space (`*/` -> `* /`), which mangles glob defaults such
// as `resources/**/*.bpmn` into the misleading `resources/** /*.bpmn`. Pre-escape
// `*/` in every `description` as `*\/` instead: that sequence contains no `*/`
// (so it never terminates the comment and the library leaves it untouched), and
// the emitted JSDoc reads `resources/**\/*.bpmn` — a backslash before the slash
// rather than a glob-breaking space. That keeps the `**/` and `*.bpmn` segments
// visually contiguous so the glob stays unambiguous for TS consumers.
function escapeGlobsInDescriptions(node) {
  if (Array.isArray(node)) {
    for (const item of node) escapeGlobsInDescriptions(item);
    return;
  }
  if (node && typeof node === "object") {
    for (const [key, value] of Object.entries(node)) {
      if (key === "description" && typeof value === "string") {
        node[key] = value.replace(/\*\//g, "*\\/");
      } else {
        escapeGlobsInDescriptions(value);
      }
    }
  }
}

const schema = JSON.parse(readFileSync(schemaPath, "utf8"));
escapeGlobsInDescriptions(schema);

const ts =
  banner +
  (await compile(schema, schema.title, {
    bannerComment: "",
    additionalProperties: false,
    style: { singleQuote: false },
    cwd: specDir,
  }));

const check = process.argv.includes("--check");
if (check) {
  let current = "";
  try {
    current = readFileSync(outPath, "utf8");
  } catch {
    /* missing counts as stale */
  }
  if (current !== ts) {
    console.error(
      "gen/nano-app.d.ts is stale — run `npm run gen` (or `make generate-app-manifest`) and commit the result.",
    );
    process.exit(1);
  }
  console.log("gen/nano-app.d.ts is up to date.");
} else {
  mkdirSync(outDir, { recursive: true });
  writeFileSync(outPath, ts);
  console.log(`Wrote ${outPath.slice(specDir.length + 1)}`);
}
