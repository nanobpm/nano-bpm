// Provenance stamp for the compiled demo BPMN (website/demo/src/<id>.bpmn): one
// canonical implementation shared by the compiler (scripts/compile-demo-flows.mjs,
// which writes it) and the drift guard (demo-flows.test.mjs, which re-checks it).
//
// NAGE's compiler isn't a public export, so CI cannot regenerate the BPMN and diff
// it. Instead the generated header records sha256 of (a) every snippet the flow
// is compiled from and (b) the BPMN body the compiler emitted. Any edit to the
// code — a step, a gateway condition, a loop edge, a multi-instance attribute —
// changes (a); any hand edit to the BPMN changes (b). Either fails the guard
// until the BPMN is regenerated from the code.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const sha256 = (s) => createHash("sha256").update(s).digest("hex");

/** sha256 over the flow's snippet and its deps, in declaration order. */
export function sourceHash(snippetsDir, flow) {
  const h = createHash("sha256");
  for (const file of [flow.snippet, ...(flow.deps ?? [])]) {
    h.update(`${file}\0`).update(readFileSync(join(snippetsDir, file))).update("\0");
  }
  return h.digest("hex");
}

/** The generated BPMN file text: stamped header + compiler output body. */
export function stampedBpmn(flow, srcHash, body) {
  return (
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<!-- GENERATED from website/snippets/${flow.snippet} by website/scripts/compile-demo-flows.mjs\n` +
    `     (Nano Agent Graph Engine's compiler). Do not edit; re-run the script.\n` +
    `     source-sha256: ${srcHash}\n` +
    `     bpmn-sha256: ${sha256(body)} -->\n` +
    body
  );
}

/** Parse a generated BPMN file back into its stamp and body (null if unstamped). */
export function readStamp(text) {
  const m = text.match(/source-sha256: ([0-9a-f]{64})\n\s*bpmn-sha256: ([0-9a-f]{64}) -->\n/);
  if (!m) return null;
  const body = text.slice(m.index + m[0].length);
  return { source: m[1], bpmn: m[2], bodyHash: sha256(body) };
}
