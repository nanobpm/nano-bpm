#!/usr/bin/env node
// Compile the landing-page flow examples (website/snippets/*.ts) to the BPMN the
// in-browser demo runs (website/demo/src/<flow>.bpmn), so the code a visitor reads
// on nanobpm.io and the run they watch are the same flow.
//
//   NAGE_SRC=~/workspace/agent-graph-engine node website/scripts/compile-demo-flows.mjs
//
// It uses Nano Agent Graph Engine's own compiler (checkProject in src/compile.ts),
// which is not a public export yet — hence a local NAGE checkout. Re-run after
// editing a snippet; commit the regenerated .bpmn. website/demo-flows.test.mjs
// fails if a snippet's steps and its committed BPMN drift apart.
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { DEMO_FLOWS } from "../demo-flows.mjs";

const website = fileURLToPath(new URL("..", import.meta.url));
const nageSrc = process.env.NAGE_SRC ?? join(homedir(), "workspace", "agent-graph-engine");
const { checkProject } = await import(pathToFileURL(join(nageSrc, "src", "compile.ts")).href);

const project = mkdtempSync(join(tmpdir(), "nage-demo-flows-"));
process.env.AGE_STATE_DIR = join(project, ".state");
try {
  mkdirSync(join(project, "flows"));
  for (const f of DEMO_FLOWS) {
    copyFileSync(join(website, "snippets", f.snippet), join(project, "flows", `${f.id}.flow.ts`));
    for (const dep of f.deps ?? []) copyFileSync(join(website, "snippets", dep), join(project, "flows", dep));
  }
  const results = await checkProject(project);
  for (const f of DEMO_FLOWS) {
    const r = results.find((x) => x.file.endsWith(`${f.id}.flow.ts`));
    // Prompt files are the agent's instructions, not part of the graph: the
    // example doesn't ship them, so "prompt file … not found" is expected.
    const errors = (r?.problems ?? []).filter((p) => p.severity === "error" && !/^prompt file .* not found$/.test(p.message));
    if (!r || errors.length) throw new Error(`${f.snippet}: ${errors.map((p) => p.message).join("; ") || "not compiled"}`);
    const flow = r.flows.find((x) => x.key === f.id);
    if (!flow) throw new Error(`${f.snippet}: no flow "${f.id}" (got ${r.flows.map((x) => x.key).join(", ")})`);
    const header =
      `<?xml version="1.0" encoding="UTF-8"?>\n` +
      `<!-- GENERATED from website/snippets/${f.snippet} by website/scripts/compile-demo-flows.mjs\n` +
      `     (Nano Agent Graph Engine's compiler). Do not edit; re-run the script. -->\n`;
    const out = join(website, "demo", "src", `${f.id}.bpmn`);
    writeFileSync(out, header + flow.xml.replace(/^<\?xml[^>]*>\s*/, ""));
    console.log(`${f.snippet} -> demo/src/${f.id}.bpmn`);
  }
} finally {
  rmSync(project, { recursive: true, force: true });
}
