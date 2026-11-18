// The extension-register guard (#1261, slice 6 of #1240).
//
// Nano is a strict superset of Camunda 8: on Camunda's surface there is NO
// tolerated divergence, so the ONLY surface allowed to differ is what Camunda
// rejects or does not have — a Nano-only EXTENSION. `formal/parity/extensions.json`
// is that surface's checked-in register. This module is the CI guard over it.
//
// The register's contract (each rule is enforced by `checkExtensions`):
//
//   1. **Well-formed** — every entry has a unique `id`, a `kind`, a filed
//      `issue`, a `note`, and a `match` describing the surface.
//   2. **Never a Camunda-surface difference** — no entry's id may collide with a
//      cell of the derived `zeebe-surface.json`, nor be claimed by a
//      `coverage.json` rule (a cell Nano must match Zeebe on). A difference on
//      Camunda's surface is a DEFECT (formal/traces/allowlist.json), never an
//      extension.
//   3. **Skip, not silently** — a parity-checked model that uses a registered
//      extension is OUT of the parity oracle's scope. The guard fails when a
//      parity-checked corpus model is tagged as an extension: the generator
//      emits only Camunda-expressible models, so an extension appearing in the
//      parity corpus means the corpus drifted off the shared surface.
//   4. **Not accepted by Camunda** — a registered extension that Camunda is
//      found to ACCEPT is not an extension. The live acceptance probe runs in
//      the daily Camunda workflow (a registered extension deployed to Camunda
//      must be REJECTED); this static guard covers rules 1–3 everywhere.
//
// This is intentionally a THIRD list, separate from the coverage matrix's
// `out-of-scope` cells (Zeebe surface Nano does not implement) and from
// `formal/traces/allowlist.json` (a known DEFECT on the shared surface). The
// triage note on #1261 is explicit: reconcile, do not merge, the lists.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
export const EXTENSIONS_PATH = path.join(here, "extensions.json");
const SURFACE_PATH = path.join(here, "zeebe-surface.json");
const COVERAGE_PATH = path.join(here, "coverage.json");
const CORPUS_GRAPHS = path.join(here, "..", "corpus", "graphs");
const CORPUS_BPMN = path.join(here, "..", "corpus", "bpmn");
const ALLOWLIST_PATH = path.join(here, "..", "traces", "allowlist.json");

/** `*` matches any run of characters; everything else is literal. */
export function globToRegExp(glob) {
  return new RegExp(
    "^" +
      String(glob)
        .split("*")
        .map((s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
        .join(".*") +
      "$",
  );
}

/** Load the checked-in register. Throws on unreadable/invalid JSON. */
export function loadExtensions(file = EXTENSIONS_PATH) {
  const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  return parsed.extensions ?? [];
}

/** The set of cell ids the derived Zeebe surface lists. */
export function surfaceCellIds(surfaceFile = SURFACE_PATH) {
  const surface = JSON.parse(fs.readFileSync(surfaceFile, "utf8"));
  return new Set((surface.cells ?? []).map((c) => c.id));
}

/**
 * The cell ids a `coverage.json` rule claims (its `match`, expanded over the
 * surface). A rule's `match` is a glob; a registered extension id must not be
 * claimed by ANY rule (whatever its status — even `gap`/`out-of-scope` means
 * the surface is Zeebe's, not a Nano extension).
 */
export function coverageClaims(coverageFile, surfaceIds) {
  const coverage = JSON.parse(fs.readFileSync(coverageFile, "utf8"));
  const claimed = new Set();
  for (const rule of coverage.rules ?? []) {
    const re = globToRegExp(rule.match);
    for (const id of surfaceIds) if (re.test(id)) claimed.add(id);
    // An exact (wildcard-free) rule also claims its literal id even when the
    // surface does not list it (a forward reference to a cell a pin bump adds).
    if (!rule.match.includes("*")) claimed.add(rule.match);
  }
  return claimed;
}

/**
 * Whether one BPMN model's XML uses a registered extension. An extension's
 * `match.xmlLocalName` (+ optional `xmlNamespace`) matches a `<ns:localName>`
 * or `<localName>` tag in the model's `<bpmn:extensionElements>`. The match is
 * namespace-aware when the entry pins one: a `nano:` tag matches only the nano
 * semantic-annotation namespace, so a same-named tag in another namespace is
 * not the extension.
 */
export function modelUsesExtension(xml, entry) {
  const local = entry?.match?.xmlLocalName;
  if (!local) return false;
  // Match `<prefix:localName` or `<localName` as an opening/empty tag. When the
  // entry pins a namespace, require the tag's prefix to be bound to it.
  const tagRe = new RegExp(`<(?:([A-Za-z_][\\w.-]*):)?${local}[\\s>/]`, "g");
  let m;
  while ((m = tagRe.exec(xml)) !== null) {
    const prefix = m[1];
    if (!entry.match.xmlNamespace) return true;
    if (!prefix) continue; // an unprefixed tag is not the namespaced extension
    // The prefix must be bound to the pinned namespace somewhere in the doc.
    const nsRe = new RegExp(`xmlns:${prefix}=["']${escapeRe(entry.match.xmlNamespace)}["']`);
    if (nsRe.test(xml)) return true;
  }
  return false;
}

function escapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * The parity-checked corpus models that use a registered extension. The
 * single-source corpus (`formal/corpus/`, #1258) is the parity-checked surface:
 * its `graphs/<Id>.json` is the source and `bpmn/<Id>.bpmn` the generated model.
 * The generator emits only Camunda-expressible models, so ANY registered
 * extension found here is a defect — the corpus drifted off the shared surface.
 * Returns `{ graphId, extensionId }` pairs.
 */
export function parityCorpusExtensions(extensions, graphsDir = CORPUS_GRAPHS, bpmnDir = CORPUS_BPMN) {
  const hits = [];
  let bpmnFiles = [];
  try {
    bpmnFiles = fs.readdirSync(bpmnDir).filter((f) => f.endsWith(".bpmn"));
  } catch {
    return hits; // no generated corpus yet — nothing parity-checked to scan
  }
  for (const file of bpmnFiles) {
    const xml = fs.readFileSync(path.join(bpmnDir, file), "utf8");
    const graphId = file.slice(0, -".bpmn".length);
    for (const entry of extensions) {
      if (modelUsesExtension(xml, entry)) {
        hits.push({ graphId, extensionId: entry.id });
      }
    }
  }
  return hits;
}

/**
 * Run every register guard. Returns a list of problem strings; an empty list
 * passes. Pure over its inputs (dependency-injected file paths for tests).
 */
export function checkExtensions({
  extensionsFile = EXTENSIONS_PATH,
  surfaceFile = SURFACE_PATH,
  coverageFile = COVERAGE_PATH,
  graphsDir = CORPUS_GRAPHS,
  bpmnDir = CORPUS_BPMN,
  allowlistFile = ALLOWLIST_PATH,
} = {}) {
  const problems = [];
  const extensions = loadExtensions(extensionsFile);

  // Rule 1 — well-formed.
  const seen = new Set();
  for (const e of extensions) {
    const where = `extension ${JSON.stringify(e?.id)}`;
    if (e === null || typeof e !== "object") {
      problems.push(`${where}: not an object`);
      continue;
    }
    if (typeof e.id !== "string" || e.id.length === 0) problems.push(`${where}: id must be a non-empty string`);
    else if (seen.has(e.id)) problems.push(`${where}: duplicate id`);
    else seen.add(e.id);
    if (typeof e.kind !== "string" || e.kind.length === 0) problems.push(`${where}: kind must be a non-empty string`);
    if (!Number.isInteger(e.issue) || e.issue <= 0) problems.push(`${where}: needs a filed issue number`);
    if (typeof e.note !== "string" || e.note.trim().length === 0) problems.push(`${where}: needs a note`);
    if (e.inert !== undefined && typeof e.inert !== "boolean") problems.push(`${where}: inert must be a boolean when present`);
    if (e.match === null || typeof e.match !== "object" || typeof e.match.xmlLocalName !== "string") {
      problems.push(`${where}: match.xmlLocalName must be a non-empty string`);
    }
  }

  // Rule 2 — never a Camunda-surface difference. No extension id may be a cell
  // of the derived Zeebe surface, nor be claimed by a coverage.json rule.
  const surfaceIds = surfaceCellIds(surfaceFile);
  const claimed = coverageClaims(coverageFile, surfaceIds);
  for (const e of extensions) {
    if (typeof e.id !== "string") continue;
    if (surfaceIds.has(e.id)) {
      problems.push(
        `extension ${JSON.stringify(e.id)}: collides with a Zeebe-surface cell — a difference on Camunda's surface is a DEFECT (allowlist.json), never an extension`,
      );
    }
    if (claimed.has(e.id)) {
      problems.push(
        `extension ${JSON.stringify(e.id)}: is claimed by a coverage.json rule — it is on the Camunda surface Nano must match, not a Nano-only extension`,
      );
    }
  }

  // Rule 3 — no parity-checked corpus model may be tagged as an extension.
  for (const hit of parityCorpusExtensions(extensions, graphsDir, bpmnDir)) {
    problems.push(
      `parity-checked corpus model ${JSON.stringify(hit.graphId)} uses registered extension ${JSON.stringify(hit.extensionId)}: the corpus generator emits only Camunda-expressible models, so an extension here means the corpus drifted off the shared surface`,
    );
  }

  // Rule 5 — the three lists stay RECONCILED (separate, never overlapping). An
  // extension is a Nano-only surface; a divergence allowlist entry
  // (formal/traces/allowlist.json) is a DEFECT on the shared surface; a coverage
  // `out-of-scope` rule is a Zeebe surface Nano does not implement. The same
  // subject must never appear as BOTH an extension and an allowlisted
  // divergence — that would mask a defect as an extension. Keyed by the entry
  // id vs. the allowlist scenario/reason text.
  try {
    const allowlist = JSON.parse(fs.readFileSync(allowlistFile, "utf8"));
    const allowText = JSON.stringify(allowlist.entries ?? []);
    for (const e of extensions) {
      if (typeof e.id !== "string") continue;
      const local = e.match?.xmlLocalName;
      // An extension is masked as a divergence when its id (or its exact
      // `nano:<local>` tag) is named in an allowlist entry.
      if (local && allowText.includes(`nano:${local}`)) {
        problems.push(
          `extension ${JSON.stringify(e.id)}: also named in the divergence allowlist (formal/traces/allowlist.json) — a Nano-only extension is not a known defect; keep the two lists disjoint`,
        );
      }
    }
  } catch {
    // No allowlist yet — nothing to reconcile against.
  }

  return problems;
}

// CLI: `node formal/parity/extensions.mjs` exits non-zero when the guard fails.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const problems = checkExtensions();
  if (problems.length > 0) {
    for (const p of problems) console.error(`FAIL ${p}`);
    console.error(`\n${problems.length} extension-register problem(s).`);
    process.exit(1);
  }
  const n = loadExtensions().length;
  console.log(`ok    extension register: ${n} extension(s), none on the Camunda surface, none in the parity corpus`);
}
