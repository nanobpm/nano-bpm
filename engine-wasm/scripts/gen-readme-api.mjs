#!/usr/bin/env node
// Generate the API reference section of engine-wasm/README.md (#1899, #1883 item 4).
//
// Single sources of truth:
//   - the WASM export list: every `pub fn` inside a `#[wasm_bindgen] impl TestEngine`
//     block of engine-wasm/src/lib.rs (its JS name, parameters, first doc line, and
//     whether the block is gated on the `read-model` feature);
//   - the REST spec: spec/*.yaml, for the operationId each method mirrors and that
//     operation's request/response schema.
//
// A method mirrors the spec operation whose operationId equals its JS name, unless
// OPERATION_ALIASES names a different operationId. Every alias target must exist in
// the spec (the script fails otherwise), so the alias table cannot rot silently.
//
// Usage:
//   node engine-wasm/scripts/gen-readme-api.mjs          # rewrite the README section
//   node engine-wasm/scripts/gen-readme-api.mjs --check  # exit 1 if it is stale (CI)

import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const crateDir = join(here, "..");
const repoRoot = join(crateDir, "..");
const specDir = join(repoRoot, "spec");
const libRs = join(crateDir, "src", "lib.rs");
const readmePath = join(crateDir, "README.md");

export const BEGIN = "<!-- BEGIN GENERATED API (engine-wasm/scripts/gen-readme-api.mjs) -->";
export const END = "<!-- END GENERATED API -->";

/** WASM method → spec operationId, only where the JS name differs from the operationId. */
export const OPERATION_ALIASES = {
  deploy: "createDeployment",
  deployDecision: "createDeployment",
  deployForm: "createDeployment",
  deployResource: "createDeployment",
  createInstance: "createProcessInstance",
  cancelInstance: "cancelProcessInstance",
  modify: "modifyProcessInstance",
  migrate: "migrateProcessInstance",
  throwError: "throwJobError",
  updateRetries: "updateJob",
  updateTimeout: "updateJob",
  setVariables: "createElementInstanceVariables",
  getResourceByKey: "getResource",
};

const snakeToCamel = (s) => s.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());

/** Parse the exported TestEngine methods out of lib.rs. */
export function parseExports(src) {
  const lines = src.split("\n");
  const methods = [];
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].trim() !== "impl TestEngine {" || lines[i - 1]?.trim() !== "#[wasm_bindgen]") continue;
    const readModel = /^#\[cfg\(feature = "read-model"\)\]$/.test(lines[i - 2]?.trim() ?? "");
    let depth = 0;
    let docs = [];
    let attr = null;
    for (let j = i; j < lines.length; j++) {
      const line = lines[j];
      const t = line.trim();
      if (depth === 1) {
        if (t.startsWith("///")) docs.push(t.replace(/^\/\/\/ ?/, ""));
        else if (t.startsWith("#[wasm_bindgen")) attr = t;
        else if (t.startsWith("pub fn ")) {
          // Gather the full signature up to the opening brace of the body.
          let sig = "";
          for (let k = j; k < lines.length; k++) {
            sig += " " + lines[k].trim();
            if (/\{\s*$/.test(lines[k])) break;
          }
          methods.push(toMethod(sig, attr, docs, readModel));
          docs = [];
          attr = null;
        } else if (t !== "" && !t.startsWith("//") && !t.startsWith("#[")) {
          docs = [];
          attr = null;
        }
      }
      for (const ch of line.replace(/"(?:\\.|[^"\\])*"/g, "").replace(/\/\/.*$/, "")) {
        if (ch === "{") depth++;
        else if (ch === "}") depth--;
      }
      if (j > i && depth === 0) {
        i = j;
        break;
      }
    }
  }
  return methods;
}

function toMethod(sig, attr, docs, readModel) {
  const m = /pub fn (\w+)\s*\((.*?)\)\s*(?:->\s*(.*?))?\s*\{/.exec(sig);
  if (!m) throw new Error(`cannot parse signature: ${sig}`);
  const [, rustName, rawParams, ret] = m;
  const jsName = /js_name = (\w+)/.exec(attr ?? "")?.[1] ?? snakeToCamel(rustName);
  const kind = /\bconstructor\b/.test(attr ?? "") ? "constructor" : /\bgetter\b/.test(attr ?? "") ? "getter" : "method";
  const params = rawParams
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p && !/^&?(mut )?self$/.test(p))
    .map((p) => {
      const [name, type] = p.split(/:\s*/);
      return { name: snakeToCamel(name), type: jsType(type) };
    });
  // First sentence of the first doc paragraph, joined onto one line.
  const para = [];
  for (const d of docs) {
    if (d === "") break;
    para.push(d);
  }
  const summary = firstSentence(para.join(" "));
  return { jsName, kind, params, returns: jsReturn(ret), summary, readModel };
}

/**
 * The first sentence of `text`. A terminator only counts outside inline code spans and when it
 * does not end a common abbreviation (`e.g.`, `i.e.`, `etc.`, `vs.`), so neither `` `x?` `` nor
 * "e.g. foo" cuts the sentence short and leaves an unbalanced backtick.
 */
export function firstSentence(text) {
  let inCode = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === "`") inCode = !inCode;
    else if (!inCode && /[.!?]/.test(c) && (i + 1 === text.length || /\s/.test(text[i + 1]))) {
      if (c === "." && /(?:^|[\s(])(?:e\.g|i\.e|etc|vs|cf)$/i.test(text.slice(0, i))) continue;
      return text.slice(0, i + 1);
    }
  }
  return text;
}

function jsType(t) {
  t = t.trim();
  const opt = /^Option<(.*)>$/.exec(t);
  if (opt) return `${jsType(opt[1])}?`;
  if (t === "&str" || t === "String") return "string";
  if (/^(f64|f32|u32|i32|u16|u8|i64|u64|usize)$/.test(t)) return "number";
  if (t === "bool") return "boolean";
  return t;
}

function jsReturn(t) {
  if (!t) return "void";
  const r = /^Result<(.*),\s*JsValue>$/.exec(t.trim());
  const inner = r ? r[1] : t;
  if (inner === "()") return r ? "void (throws)" : "void";
  return jsType(inner) + (r ? " (throws)" : "");
}

/** Index every operation in spec/*.yaml by operationId. Line-oriented, matching the vendored layout. */
export function parseSpec(files) {
  const ops = new Map();
  for (const [file, text] of files) {
    const lines = text.split("\n");
    let path = null;
    let method = null;
    let op = null;
    let section = null;
    let seenSuccess = false;
    let prev = "";
    for (const line of lines) {
      const afterSchema = /^\s*schema:\s*$/.test(prev);
      prev = line;
      let m;
      if ((m = /^ {2}(\/\S*):\s*$/.exec(line))) {
        path = m[1];
        op = null;
      } else if (path && (m = /^ {4}(get|post|put|patch|delete):\s*$/.exec(line))) {
        method = m[1].toUpperCase();
        op = null;
        section = null;
        seenSuccess = false;
      } else if (/^\S/.test(line)) {
        path = null;
        op = null;
      } else if (path && method && (m = /^ {6}operationId:\s*(\w+)/.exec(line))) {
        op = { operationId: m[1], method, path, file, request: null, response: null };
        if (!ops.has(m[1])) ops.set(m[1], op);
      } else if (op && (m = /^ {6}(requestBody|responses):\s*$/.exec(line))) {
        section = m[1];
      } else if (op && /^ {6}\S/.test(line)) {
        section = null;
      } else if (op && section && section !== "requestBody" && (m = /^ {8}"?(\d{3})"?:/.exec(line))) {
        section = m[1].startsWith("2") && !seenSuccess ? "success" : "other";
        if (m[1].startsWith("2")) seenSuccess = true;
      } else if (op && afterSchema && (m = /\$ref:\s*['"]?([^'"\s]+)['"]?/.exec(line))) {
        const name = m[1].split("/").pop();
        if (section === "requestBody" && !op.request) op.request = name;
        else if (section === "success" && !op.response) op.response = name;
      }
    }
  }
  return ops;
}

export function render(methods, ops) {
  const out = [];
  out.push(BEGIN);
  out.push("");
  out.push(
    "_Generated by `node engine-wasm/scripts/gen-readme-api.mjs` from the `#[wasm_bindgen]` exports in",
    "`engine-wasm/src/lib.rs` and the REST spec in [`spec/`](../spec). Do not edit by hand; CI fails",
    "when this section is stale._",
    "",
    "Every argument and result that carries structured data is a JSON **string**. The **REST operation**",
    "column names the spec operation the method mirrors, and the REST request/response columns name",
    "that operation's schemas. They are **REST counterparts for orientation, not the WASM payload",
    "contract**: some methods take or return a different shape (for example `deploy` returns",
    "`{ processIds, snapshot }`, and `evaluateDecision` takes a plain variables object). The method's",
    "doc comment in [`src/lib.rs`](src/lib.rs) is authoritative for its WASM JSON. `—` marks an",
    "embedded-only method.",
  );
  for (const [title, readModel] of [
    ["### Lean surface (both subpaths)", false],
    ["### Read-model surface (`@nanobpm/engine-wasm/readmodel` only)", true],
  ]) {
    out.push("", title, "", "| Method | Returns | REST operation | REST request schema | REST response schema | Description |", "| --- | --- | --- | --- | --- | --- |");
    for (const m of methods.filter((x) => x.readModel === readModel)) {
      const opId = OPERATION_ALIASES[m.jsName] ?? (ops.has(m.jsName) ? m.jsName : null);
      const op = opId ? ops.get(opId) : null;
      if (opId && !op) throw new Error(`OPERATION_ALIASES maps ${m.jsName} to unknown operationId ${opId}`);
      const args = m.params.map((p) => `${p.name}: ${p.type}`).join(", ");
      const call =
        m.kind === "constructor" ? `new TestEngine(${args})` : m.kind === "getter" ? `${m.jsName}` : `${m.jsName}(${args})`;
      const link = (f) => `[\`${op.operationId}\`](../spec/${f}) (\`${op.method} ${op.path}\`)`;
      const cell = (s) => s.replace(/\|/g, "\\|");
      out.push(
        `| \`${cell(call)}\` | ${m.kind === "getter" ? "property: " : ""}${cell(m.returns)} | ${op ? link(op.file) : "—"} | ${op?.request ? `\`${op.request}\`` : "—"} | ${op?.response ? `\`${op.response}\`` : "—"} | ${cell(m.summary)} |`,
      );
    }
  }
  out.push("", END);
  return out.join("\n");
}

export function generate() {
  const files = readdirSync(specDir)
    .filter((f) => f.endsWith(".yaml"))
    .sort()
    .map((f) => [f, readFileSync(join(specDir, f), "utf8")]);
  return render(parseExports(readFileSync(libRs, "utf8")), parseSpec(files));
}

export function splice(readme, section) {
  const a = readme.indexOf(BEGIN);
  const b = readme.indexOf(END);
  if (a < 0 || b < a) throw new Error(`README is missing the ${BEGIN} / ${END} markers`);
  return readme.slice(0, a) + section + readme.slice(b + END.length);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const readme = readFileSync(readmePath, "utf8");
  const next = splice(readme, generate());
  if (process.argv.includes("--check")) {
    if (next !== readme) {
      console.error(
        `${relative(repoRoot, readmePath)}: the generated API section is stale. ` +
          "Run `node engine-wasm/scripts/gen-readme-api.mjs` and commit the result.",
      );
      process.exit(1);
    }
    console.log("engine-wasm README API section is up to date.");
  } else {
    writeFileSync(readmePath, next);
  }
}
