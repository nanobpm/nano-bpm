#!/usr/bin/env node
// Harvests the Zeebe engine-test process models into the parity corpus (#1249).
//
//   node formal/harvest/harvest.mjs <zeebe-test-checkout> [--out formal/harvest/corpus]
//
// `zeebe/engine/src/test` builds thousands of processes with the fluent builder
// `Bpmn.createExecutableProcess(id).startEvent()...done()` and asserts on the
// record stream. This harvester reuses Camunda's own edge-case catalogue:
//
//   1. SCAN   — walk the pinned test sources (fetch-tests.sh) for
//               `Bpmn.createExecutableProcess(...)` chains.
//   2. PARSE  — statically resolve each chain into an ordered IR of builder
//               calls. Only chains built ENTIRELY from statically resolvable
//               no-arg / string-literal builder calls are harvested; a chain
//               that uses a lambda (`serviceTask("t", b -> …)`), a variable
//               process id, a helper method, or a non-builder call is recorded
//               UNPARSEABLE (it cannot be reproduced faithfully).
//   3. SERIALISE — replay the IR through the REAL Zeebe model API
//               (tools/HarvestExtract.java, compiled against the pinned
//               zeebe-bpmn-model jar) so the BPMN XML is exactly what Zeebe's
//               own builder emits.
//
// The output is a derived artifact: never hand-edit it. Regenerate after a pin
// bump. The result feeds normalise.mjs (BPMN+DI + scenario) and run.mjs (the
// Nano differential).

import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, dirname, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const PIN = JSON.parse(readFileSync(join(here, 'zeebe-test-pin.json'), 'utf8'));

export class HarvestError extends Error {}

// ---------------------------------------------------------------------------
// Java source tokenising (comments/strings preserved enough to find chains)
// ---------------------------------------------------------------------------

/** Strip comments (line numbers preserved) so chain detection is not fooled by
 * commented-out code. String literals are kept (they carry ids/conditions). */
export function stripComments(src) {
  let out = '';
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    const n = src[i + 1];
    if (c === '"') {
      if (src.startsWith('"""', i)) {
        const end = src.indexOf('"""', i + 3);
        const stop = end < 0 ? src.length : end + 3;
        out += src.slice(i, stop);
        i = stop;
        continue;
      }
      let j = i + 1;
      while (j < src.length && src[j] !== '"' && src[j] !== '\n') j += src[j] === '\\' ? 2 : 1;
      out += src.slice(i, j + 1);
      i = j + 1;
    } else if (c === '/' && n === '/') {
      while (i < src.length && src[i] !== '\n') { out += ' '; i++; }
    } else if (c === '/' && n === '*') {
      const end = src.indexOf('*/', i + 2);
      const stop = end < 0 ? src.length : end + 2;
      out += src.slice(i, stop).replace(/[^\n]/g, ' ');
      i = stop;
    } else {
      out += c;
      i++;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Fluent-chain extraction
// ---------------------------------------------------------------------------

// The builder methods that ADVANCE the chain and that we can replay through the
// Zeebe model API. Each maps to a HarvestExtract reflective invoke. Anything
// not in this set (or used with an unresolvable argument) makes the chain
// unparseable.
const REPLAYABLE = new Set([
  // element builders
  'startEvent', 'endEvent', 'serviceTask', 'sendTask', 'receiveTask',
  'userTask', 'businessRuleTask', 'scriptTask', 'manualTask', 'task',
  'callActivity', 'subProcess', 'adHocSubProcess',
  'exclusiveGateway', 'parallelGateway', 'inclusiveGateway', 'eventBasedGateway',
  'intermediateCatchEvent', 'intermediateThrowEvent', 'boundaryEvent',
  // chain navigation
  'moveToNode', 'moveToActivity', 'moveToLastGateway',
  'moveToLastExclusiveGateway', 'moveToLastParallelGateway',
  'moveToLastInclusiveGateway', 'connectTo', 'sequenceFlowId',
  // flow / element configuration
  'conditionExpression', 'condition', 'defaultFlow', 'name',
  // event definitions (no-arg / string)
  'message', 'signal', 'timerWithDuration', 'timerWithDate', 'timerWithCycle',
  'errorEventDefinition', 'escalationEventDefinition', 'compensateEventDefinition',
  'terminateEventDefinition', 'cancelEventDefinition', 'linkEventDefinition',
  'messageEventDefinition', 'signalEventDefinition', 'conditionalEventDefinition',
  // zeebe extensions (string)
  'zeebeJobType', 'zeebeJobRetries', 'zeebeTaskHeader',
  'zeebeCalledElement', 'zeebeProcessId', 'zeebeAssignee', 'zeebeCandidateGroups',
  'zeebeFormKey', 'zeebeTaskSchedule', 'zeebeDueDate', 'zeebeFollowUpDate',
  'zeebeInput', 'zeebeOutput', 'zeebeProperty',
  'zeebeResultVariable', 'zeebeResultCollection', 'zeebeLoopCardinality',
  'zeebeCollection', 'zeebeElementVariable', 'zeebeCompletionCondition',
  'zeebeCancelRemainingInstances', 'zeebeActiveElementsCollection',
  'zeebeCalledDecision', 'zeebeDecisionId', 'zeebeVersionTag',
  'zeebeBindingType', 'zeebePriority', 'zeebeExecutionListener',
  'zeebeTaskListener', 'zeebeUserTaskFormId', 'zeebeIsStartableViaMessage',
  // generic element-scoped
  'id', 'documentation', 'camundaAsyncBefore', 'camundaAsyncAfter',
]);

// Methods that, when present, ALWAYS make the chain unparseable: they take a
// lambda/Consumer, a non-literal, or mutate the model in a way we cannot
// reproduce by replaying a flat call list.
const UNSUPPORTED_HINTS = [
  /\.zeebeJobType\s*\(\s*[^"']/, // zeebeJobType(<expr>) — only literal supported
];

/**
 * Extract the argument list text of the call that starts at `open` (the index of
 * `(`). Returns { args, end } where `end` is just past the matching `)`, or null.
 */
function callArgs(src, open) {
  let depth = 0;
  let i = open;
  const start = i;
  for (; i < src.length; i++) {
    const c = src[i];
    if (c === '"' || c === "'") {
      const q = c;
      i++;
      while (i < src.length && src[i] !== q) i += src[i] === '\\' ? 2 : 1;
    } else if (c === '(') depth++;
    else if (c === ')' && --depth === 0) {
      return { args: src.slice(start + 1, i), end: i + 1 };
    }
  }
  return null;
}

/** Split a top-level argument list on commas (respecting nesting/quotes). */
export function splitArgs(argText) {
  const args = [];
  let depth = 0;
  let cur = '';
  for (let i = 0; i < argText.length; i++) {
    const c = argText[i];
    if (c === '"' || c === "'") {
      const q = c;
      cur += c;
      i++;
      while (i < argText.length && argText[i] !== q) { cur += argText[i]; i += argText[i] === '\\' ? 2 : 1; }
      cur += argText[i] ?? '';
    } else if (c === '(' || c === '[' || c === '{') { depth++; cur += c; }
    else if (c === ')' || c === ']' || c === '}') { depth--; cur += c; }
    else if (c === ',' && depth === 0) { args.push(cur); cur = ''; }
    else cur += c;
  }
  if (cur.trim() !== '') args.push(cur);
  return args.map((a) => a.trim());
}

/** Unescape a Java string literal body (the part between quotes). */
export function javaUnescape(body) {
  let out = '';
  for (let i = 0; i < body.length; i++) {
    const c = body[i];
    if (c !== '\\') { out += c; continue; }
    const e = body[++i];
    switch (e) {
      case 'n': out += '\n'; break;
      case 't': out += '\t'; break;
      case 'r': out += '\r'; break;
      case 'b': out += '\b'; break;
      case 'f': out += '\f'; break;
      case '"': out += '"'; break;
      case "'": out += "'"; break;
      case '\\': out += '\\'; break;
      case 'u': out += String.fromCharCode(parseInt(body.substr(i + 1, 4), 16)); i += 4; break;
      default: out += e;
    }
  }
  return out;
}

/**
 * Resolve one argument expression to a replayable value (string / number /
 * boolean), or return UNRESOLVED. Only string literals, numeric literals,
 * booleans, concatenations of string literals, and identifiers bound to a
 * `static final` constant in the same file are replayable.
 */
const UNRESOLVED = Symbol('unresolved');
export function resolveArg(expr, constants = {}) {
  const t = expr.trim();
  // concatenation of string literals: "a" + "b" — checked BEFORE the single
  // literal, since a concatenation also starts and ends with a quote.
  if (/^"([^"\\]|\\.)*"(\s*\+\s*"([^"\\]|\\.)*")+$/.test(t)) {
    // split on the top-level `+` between literals and concatenate the parts
    const parts = t.split(/"\s*\+\s*"/);
    // first part starts with `"`, last ends with `"`, middle parts are bare
    return parts
      .map((p, idx) => {
        let body = p;
        if (idx === 0) body = body.slice(1); // strip leading quote
        if (idx === parts.length - 1) body = body.slice(0, -1); // strip trailing quote
        return javaUnescape(body);
      })
      .join('');
  }
  // string literal
  if (t.startsWith('"') && t.endsWith('"') && t.length >= 2) {
    return javaUnescape(t.slice(1, -1));
  }
  if (/^-?\d+$/.test(t)) return parseInt(t, 10);
  if (/^-?\d+\.\d+$/.test(t)) return parseFloat(t);
  if (t === 'true') return true;
  if (t === 'false') return false;
  // A `static final String X = "..."` constant in the same file (e.g. PROCESS_ID).
  if (/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(t) && Object.hasOwn(constants, t)) {
    return constants[t];
  }
  return UNRESOLVED;
}

/**
 * Collect the `static final String`/`int` constants a test file declares, so a
 * chain that names `PROCESS_ID` (the overwhelmingly common pattern) resolves to
 * its literal. Only single-literal initialisers are collected.
 */
export function collectConstants(src) {
  const constants = {};
  const re = /\bstatic\s+final\s+(?:String|int|long|boolean)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*("(?:[^"\\]|\\.)*"|-?\d+|true|false)\s*;/g;
  let m;
  while ((m = re.exec(src))) {
    const v = resolveArg(m[2]);
    if (v !== UNRESOLVED) constants[m[1]] = v;
  }
  return constants;
}

// Sentinel marking that a lambda argument already pushed its calls (see parseChain).
const LAMBDA_CONSUMED = Symbol('lambda-consumed');

/**
 * Resolve a single-parameter configuration lambda whose body is a FLAT chain of
 * builder calls on the parameter: `b -> b.zeebeJobType("x").name("y")` (or the
 * block form `b -> { b.zeebeJobType("x"); }` with one statement). Returns the
 * ordered list of `{m, args}` config calls, or UNRESOLVED when the body nests a
 * further lambda, references a non-literal, or calls an unsupported method.
 */
export function resolveLambda(expr, constants = {}) {
  const arrow = expr.indexOf('->');
  if (arrow < 0) return UNRESOLVED;
  const param = expr.slice(0, arrow).trim().replace(/^\(|\)$/g, '').trim();
  // Only a single, simple identifier parameter (no type annotation, no destructure).
  if (!/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(param)) return UNRESOLVED;
  let body = expr.slice(arrow + 2).trim();
  // Block body: strip braces; only a single `return`-less statement chain.
  if (body.startsWith('{') && body.endsWith('}')) body = body.slice(1, -1).trim();
  // The body must be `param.method(...).method(...)` — i.e. every call is on the
  // parameter or on the value the previous call returned. We require the body to
  // START with `<param>.` and contain no further `->` (no nested lambda).
  if (!body.startsWith(param + '.')) return UNRESOLVED;
  if (body.includes('->')) return UNRESOLVED;
  // Strip a trailing semicolon.
  if (body.endsWith(';')) body = body.slice(0, -1).trim();
  // Walk the call chain on the parameter.
  const calls = [];
  let i = param.length;
  for (;;) {
    while (i < body.length && /\s/.test(body[i])) i++;
    if (i >= body.length) break;
    if (body[i] !== '.') return UNRESOLVED;
    i++;
    const ms = i;
    while (i < body.length && /[A-Za-z0-9_$]/.test(body[i])) i++;
    const name = body.slice(ms, i);
    while (i < body.length && /\s/.test(body[i])) i++;
    if (body[i] !== '(') return UNRESOLVED;
    const ca = callArgs(body, i);
    if (!ca) return UNRESOLVED;
    i = ca.end;
    if (!REPLAYABLE.has(name)) return UNRESOLVED;
    const rawArgs = splitArgs(ca.args);
    const args = [];
    for (const a of rawArgs) {
      if (a === '') continue;
      const v = resolveArg(a, constants);
      if (v === UNRESOLVED) return UNRESOLVED;
      args.push(v);
    }
    calls.push({ m: name, args });
  }
  return calls;
}

/**
 * Parse one `Bpmn.createExecutableProcess(...)` chain starting at `start`
 * (index of `Bpmn.createExecutableProcess`). Returns
 *   { processId, calls: [{m, args}], end, reason? }
 * where `reason` is set when the chain is UNPARSEABLE (calls may be partial).
 */
export function parseChain(src, start, constants = {}) {
  const calls = [];
  let i = start + 'Bpmn.createExecutableProcess'.length;
  // skip whitespace to the opening paren of createExecutableProcess
  while (i < src.length && /\s/.test(src[i])) i++;
  if (src[i] !== '(') return { processId: null, calls, end: start, reason: 'no-open-paren' };
  const head = callArgs(src, i);
  if (!head) return { processId: null, calls, end: start, reason: 'unbalanced-head' };
  const headArgs = splitArgs(head.args);
  const pid = headArgs.length ? resolveArg(headArgs[0], constants) : UNRESOLVED;
  if (typeof pid !== 'string') {
    return { processId: null, calls, end: head.end, reason: 'process-id-not-literal' };
  }
  i = head.end;

  // Walk the fluent chain: a run of `.name(args)` until `.done()` or a token
  // that breaks the chain (a non-method, a `;`, or an unresolvable call).
  for (;;) {
    // skip whitespace/newlines
    while (i < src.length && /\s/.test(src[i])) i++;
    if (src[i] !== '.') {
      return { processId: pid, calls, end: i, reason: 'chain-ended-without-done' };
    }
    i++; // past '.'
    const mstart = i;
    while (i < src.length && /[A-Za-z0-9_]/.test(src[i])) i++;
    const name = src.slice(mstart, i);
    while (i < src.length && /\s/.test(src[i])) i++;
    if (name === 'done') {
      // consume the ()
      if (src[i] === '(') {
        const d = callArgs(src, i);
        i = d ? d.end : i + 2;
      }
      return { processId: pid, calls, end: i, reason: null };
    }
    if (src[i] !== '(') {
      return { processId: pid, calls, end: i, reason: `method-${name}-no-args` };
    }
    const ca = callArgs(src, i);
    if (!ca) return { processId: pid, calls, end: i, reason: `unbalanced-${name}` };
    i = ca.end;
    if (!REPLAYABLE.has(name)) {
      return { processId: pid, calls, end: i, reason: `unsupported-method:${name}` };
    }
    const rawArgs = splitArgs(ca.args);
    const args = [];
    let bad = null;
    for (const a of rawArgs) {
      if (a === '') continue;
      // A trailing configuration lambda: `name("id", b -> b.zeebeJobType("x")…)`.
      // Replayed as the element call with its resolvable leading args followed by
      // the lambda body's own builder calls (flattened into the chain). Only a
      // SINGLE-PARAMETER lambda whose body is a flat chain of string-literal
      // calls on the parameter is resolvable; anything nested stays unresolved.
      if (/->/.test(a)) {
        const lam = resolveLambda(a, constants);
        if (lam === UNRESOLVED) { bad = a; break; }
        // push the element call itself first (a SNAPSHOT of the leading args —
        // `args` is reused below), then the lambda's config calls.
        calls.push({ m: name, args: [...args] });
        for (const c of lam) calls.push(c);
        // signal the element call was already pushed
        bad = null;
        // mark that we consumed args (so we don't re-push below)
        args.length = 0;
        args.push(LAMBDA_CONSUMED);
        break;
      }
      const v = resolveArg(a, constants);
      if (v === UNRESOLVED) { bad = a; break; }
      args.push(v);
    }
    if (bad !== null) {
      return { processId: pid, calls, end: i, reason: `unresolved-arg:${name}(${bad.slice(0, 40)})` };
    }
    // If a lambda already pushed the element call + its config, skip re-pushing.
    if (args[0] === LAMBDA_CONSUMED) continue;
    calls.push({ m: name, args });
  }
}

/** Find every createExecutableProcess chain in a source file. */
export function parseFile(src) {
  const clean = stripComments(src);
  const constants = collectConstants(clean);
  const chains = [];
  let idx = 0;
  for (;;) {
    const at = clean.indexOf('Bpmn.createExecutableProcess', idx);
    if (at < 0) break;
    chains.push(parseChain(clean, at, constants));
    idx = at + 'Bpmn.createExecutableProcess'.length;
  }
  return chains;
}

// ---------------------------------------------------------------------------
// Deterministic canonicalisation
// ---------------------------------------------------------------------------

// The Zeebe model API stamps every id it generates (definitions, sequence
// flows, DI shapes/edges, and any element the test left anonymous) with a
// random UUID — so two harvests of the SAME model differ byte-for-byte. The
// committed corpus is a drift-guarded derived artifact, so the XML must be
// reproducible. Canonicalise every `<prefix>_<uuid>` id to `<prefix>_<n>` keyed
// by first-appearance order; the model is semantically identical (ids are pure
// identifiers) but byte-stable across runs.
const UUID_ID = /([A-Za-z][A-Za-z0-9]*)_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g;

export function canonicaliseXml(xml) {
  const assigned = new Map();
  const counters = new Map();
  return xml.replace(UUID_ID, (match, prefix) => {
    if (!assigned.has(match)) {
      const n = (counters.get(prefix) ?? 0) + 1;
      counters.set(prefix, n);
      assigned.set(match, `${prefix}_${n}`);
    }
    return assigned.get(match);
  });
}

// ---------------------------------------------------------------------------
// Walking the pinned checkout
// ---------------------------------------------------------------------------

function* walk(dir) {
  for (const entry of readdirSync(dir)) {
    const p = join(dir, entry);
    const st = statSync(p);
    if (st.isDirectory()) yield* walk(p);
    else if (p.endsWith('.java')) yield p;
  }
}

/** Derive a stable, filesystem-safe harvest id from the test file + index. */
function harvestId(file, index, processId) {
  const base = file.replace(/\.java$/, '').replace(/[^A-Za-z0-9]+/g, '-');
  const pid = String(processId).replace(/[^A-Za-z0-9_-]+/g, '_');
  return `${base}-${index}-${pid}`;
}

// ---------------------------------------------------------------------------
// The Zeebe model-API serialisation (delegates to the compiled Java extractor)
// ---------------------------------------------------------------------------

function mvnPath(groupId, artifactId, version) {
  const g = groupId.replaceAll('.', '/');
  return `${g}/${artifactId}/${version}/${artifactId}-${version}.jar`;
}

/** Ensure the pinned model jars are present (download + sha256-verify) and
 * return their absolute paths. Cached under ~/.cache/nanobpm-harvest/lib. */
export function ensureModelJars(cacheRoot) {
  const libDir = join(cacheRoot, 'lib');
  mkdirSync(libDir, { recursive: true });
  const jars = [];
  for (const a of PIN.modelArtifacts) {
    const rel = mvnPath(a.groupId, a.artifactId, a.version);
    const dest = join(libDir, `${a.artifactId}-${a.version}.jar`);
    if (!existsSync(dest)) {
      const url = `https://repo1.maven.org/maven2/${rel}`;
      execFileSync('curl', ['-fsSL', url, '-o', dest], { stdio: 'inherit' });
    }
    const actual = execFileSync('sha256sum', [dest]).toString().split(/\s+/)[0];
    if (a.sha256 && actual !== a.sha256) {
      throw new HarvestError(
        `${a.artifactId}-${a.version}.jar sha256 mismatch: got ${actual}, want ${a.sha256}`);
    }
    jars.push(dest);
  }
  return jars;
}

/** Compile (once) and run the Java extractor over `programs`, returning the
 * per-program serialisation results. Requires a JDK on PATH. */
export function serialiseThroughZeebe(programs, cacheRoot) {
  const jars = ensureModelJars(cacheRoot);
  const classesDir = join(cacheRoot, 'classes');
  mkdirSync(classesDir, { recursive: true });
  const srcFile = join(here, 'tools', 'HarvestExtract.java');
  const classFile = join(classesDir, 'HarvestExtract.class');
  const cp = jars.join(':');
  if (!existsSync(classFile) || statSync(classFile).mtimeMs < statSync(srcFile).mtimeMs) {
    execFileSync('javac', ['-cp', cp, '-d', classesDir, srcFile], { stdio: 'inherit' });
  }
  const out = execFileSync(
    'java', ['-cp', `${classesDir}:${cp}`, 'HarvestExtract'],
    { input: JSON.stringify(programs), maxBuffer: 1 << 28 });
  return JSON.parse(out.toString());
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

export function harvest(checkoutRoot, { serialise = true, cacheRoot } = {}) {
  const root = cacheRoot ?? join(process.env.XDG_CACHE_HOME ?? join(process.env.HOME, '.cache'), 'nanobpm-harvest');
  const harvested = [];
  const unparseable = [];
  let scannedFiles = 0;

  for (const testPath of PIN.testPaths) {
    const abs = join(checkoutRoot, testPath);
    if (!existsSync(abs)) throw new HarvestError(`pinned test path not found in checkout: ${testPath}`);
    for (const file of walk(abs)) {
      scannedFiles++;
      const src = readFileSync(file, 'utf8');
      const rel = relative(checkoutRoot, file);
      const chains = parseFile(src);
      chains.forEach((chain, index) => {
        const id = harvestId(rel, index, chain.processId ?? 'unknown');
        const entry = {
          id,
          source: rel,
          processId: chain.processId,
          calls: chain.calls,
        };
        if (chain.reason) {
          unparseable.push({ ...entry, reason: chain.reason });
        } else {
          harvested.push(entry);
        }
      });
    }
  }

  // Serialise each harvested IR through the real Zeebe model API.
  let serialised = [];
  if (serialise && harvested.length) {
    const results = serialiseThroughZeebe(
      harvested.map((h) => ({ id: h.id, processId: h.processId, calls: h.calls })), root);
    const byId = new Map(results.map((r) => [r.id, r]));
    for (const h of harvested) {
      const r = byId.get(h.id);
      if (r && r.ok) {
        // Canonicalise the model API's random-UUID ids so the committed corpus
        // is byte-reproducible (drift-guarded); the model is unchanged.
        serialised.push({ ...h, xml: canonicaliseXml(r.xml) });
      } else {
        unparseable.push({ ...h, reason: `serialise:${r?.error ?? 'no-result'}` });
      }
    }
  } else {
    serialised = harvested;
  }

  return { scannedFiles, harvested: serialised, unparseable };
}

function main() {
  const args = process.argv.slice(2);
  const checkout = args.find((a) => !a.startsWith('--'));
  if (!checkout) {
    console.error('usage: node formal/harvest/harvest.mjs <zeebe-test-checkout> [--out dir] [--no-serialise]');
    process.exit(2);
  }
  const outIdx = args.indexOf('--out');
  const outDir = outIdx >= 0 ? args[outIdx + 1] : join(here, 'corpus');
  const serialise = !args.includes('--no-serialise');

  const { scannedFiles, harvested, unparseable } = harvest(checkout, { serialise });

  mkdirSync(outDir, { recursive: true });
  mkdirSync(join(outDir, 'ir'), { recursive: true });
  for (const h of harvested) {
    writeFileSync(join(outDir, 'ir', `${h.id}.json`), JSON.stringify(h, null, 2) + '\n');
  }

  const summary = {
    generator: 'formal/harvest/harvest.mjs',
    zeebe: { repository: PIN.repository, ref: PIN.ref, sha: PIN.sha, modelVersion: PIN.modelVersion },
    scannedFiles,
    harvested: harvested.length,
    unparseable: unparseable.length,
    unparseableReasons: unparseable.reduce((m, u) => {
      const k = u.reason.split(':')[0];
      m[k] = (m[k] ?? 0) + 1;
      return m;
    }, {}),
  };
  writeFileSync(join(outDir, 'harvest-summary.json'), JSON.stringify(summary, null, 2) + '\n');
  writeFileSync(join(outDir, 'unparseable.json'), JSON.stringify(unparseable, null, 2) + '\n');
  console.log(JSON.stringify(summary, null, 2));
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main();
}
