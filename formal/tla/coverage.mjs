#!/usr/bin/env node
// TLC action-coverage gate (#1248).
//
//   FORMAL_LOG_DIR=/tmp/tlc formal/tla/check.sh
//   node formal/corpus/exhaustive.mjs --bound ci --tlc --coverage-dir /tmp/ex
//   node formal/tla/coverage.mjs /tmp/tlc --extra /tmp/ex   # the CI gate
//   node formal/tla/coverage.mjs /tmp/tlc --report          # print, never fail
//
// The first directory holds check.sh's logs and must have one for EVERY
// registered model. `--extra DIR` adds more model logs (the bounded-exhaustive
// enumeration's MC*/ZMC* graphs — the only models with event-based gateways,
// since the hand-written corpus is deterministic-route only); a log there counts
// toward a spec when its file name matches that spec's model glob.
//
// check.sh runs TLC with `-coverage`, so every model's log ends with TLC's
// coverage statistics. TLC only prints the actions and sub-expressions it
// actually evaluated: an action whose quantifier ranges over an empty set (for
// example `CorrelateEvent` when no model has an event-based gateway) is simply
// ABSENT from the report — which is why "-coverage printed nothing" for it in
// #1243. Absence cannot be detected from the log alone, so the EXPECTED set is
// derived from the spec source (the single source of truth):
//
//   - actions: every disjunct of `Next`, and transitively every action
//     operator (a definition that primes a variable or says UNCHANGED) it
//     calls. Each Next disjunct must FIRE (found > 0) in at least one model.
//     A Next disjunct must NAME its action: an inline one (e.g. a bare
//     `UNCHANGED vars`) is silently dropped here, so specs name it instead
//     (SnapshotReplay's `RecoverAsIs`).
//   - disjuncts: every line of an action body that begins a conjunct, a
//     disjunct, a CASE arm, or an IF branch (`/\`, `\/`, `[]`, `CASE`, `THEN`,
//     `ELSE`). Each must be evaluated (TLC count > 0) in at least one model.
//     Line obligations follow EVERY definition reachable from a Next disjunct
//     (including pure guard helpers such as `ZeebeInclusiveReady` or
//     `TokenFlow.JoinFires`), not only the action closure: an action's guard
//     is evaluated exactly when the action is, so a guard branch TLC never
//     evaluated is a real coverage gap.
//
// Coverage is aggregated over all of a spec's models: an action need only fire
// in one of them. Every registered spec (formal/tla/specs/*.spec) is gated, so a
// new spec family is covered without editing this file.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));

// Blank out TLA+ comments, keeping line and column positions.
export function stripComments(src) {
  let out = '';
  let depth = 0;
  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    const n = src[i + 1];
    if (depth === 0 && c === '\\' && n === '*') {
      while (i < src.length && src[i] !== '\n') { out += ' '; i++; }
      if (i < src.length) out += '\n';
      continue;
    }
    if (c === '(' && n === '*') { depth++; out += '  '; i++; continue; }
    if (depth > 0 && c === '*' && n === ')') { depth--; out += '  '; i++; continue; }
    if (depth > 0) { out += c === '\n' ? '\n' : ' '; continue; }
    // A string literal can hold `\*` (not in our specs), but must be kept.
    out += c;
  }
  return out;
}

// Top-level operator definitions: name -> { start, end, lines: [{no, text}] }.
export function parseDefs(src) {
  const lines = stripComments(src).split('\n');
  const defs = new Map();
  let cur = null;
  lines.forEach((text, idx) => {
    const no = idx + 1;
    const m = text.match(/^([A-Za-z_][A-Za-z0-9_]*)(\s*\([^)]*\))?\s*==/);
    if (m) {
      cur = { name: m[1], start: no, lines: [{ no, text: text.slice(m[0].length) }] };
      defs.set(m[1], cur);
      return;
    }
    if (/^(====|----|[A-Z]+\b(?!\s*==)|[A-Za-z_]\w*\s*==)/.test(text) && !/^\s/.test(text)) {
      cur = null;
      return;
    }
    if (cur) cur.lines.push({ no, text });
  });
  return defs;
}

const isDirectAction = (def) => def.lines.some(({ text }) => /'|\bUNCHANGED\b/.test(text));

// An operator is an action if it primes/UNCHANGEDs a variable itself or
// references (transitively) one that does — e.g. `Drain(f) == \E S : DrainVia(f, S)`.
// Memoised per definition map; cycles resolve to the direct answer.
const actionMemo = new WeakMap();
export function isActionIn(name, defs) {
  let memo = actionMemo.get(defs);
  if (!memo) actionMemo.set(defs, (memo = new Map()));
  const visit = (n, onPath) => {
    if (memo.has(n)) return memo.get(n);
    const def = defs.get(n);
    if (isDirectAction(def)) { memo.set(n, true); return true; }
    onPath.add(n);
    let res = false;
    for (const r of refs(def, defs)) {
      if (!onPath.has(r) && visit(r, onPath)) { res = true; break; }
    }
    onPath.delete(n);
    if (res || onPath.size === 0) memo.set(n, res);
    return res;
  };
  return visit(name, new Set());
}

function refs(def, defs) {
  const out = new Set();
  for (const { text } of def.lines) {
    for (const w of text.match(/[A-Za-z_][A-Za-z0-9_]*/g) ?? []) {
      if (w !== def.name && defs.has(w)) out.add(w);
    }
  }
  return out;
}

function actionClosure(name, defs) {
  const seen = new Set();
  const stack = [name];
  while (stack.length) {
    const n = stack.pop();
    if (seen.has(n)) continue;
    seen.add(n);
    for (const r of refs(defs.get(n), defs)) if (isActionIn(r, defs)) stack.push(r);
  }
  return seen;
}

// Every definition reachable from `name`, whether or not it primes a variable:
// the line obligations of an action include the pure guard helpers it calls
// (an action's guard is evaluated exactly when the action is).
function reachableClosure(name, defs) {
  const seen = new Set();
  const stack = [name];
  while (stack.length) {
    const n = stack.pop();
    if (seen.has(n)) continue;
    seen.add(n);
    for (const r of refs(defs.get(n), defs)) stack.push(r);
  }
  return seen;
}

// The expected coverage obligations of a spec module.
export function expectations(src) {
  const defs = parseDefs(src);
  const next = defs.get('Next');
  if (!next) throw new Error('spec has no Next definition');
  // Next's disjuncts: split on top-level `\/` (one per line or inline).
  const disjuncts = [];
  for (const { text } of next.lines) {
    for (const part of text.split('\\/')) {
      const called = (part.match(/[A-Za-z_][A-Za-z0-9_]*/g) ?? []).filter(
        (w) => defs.has(w) && isActionIn(w, defs),
      );
      if (called.length) disjuncts.push(called[called.length - 1]);
    }
  }
  const actions = new Set();
  for (const d of disjuncts) for (const a of actionClosure(d, defs)) actions.add(a);
  // Line obligations come from the FULL reachable closure (guards included);
  // firing obligations stay on the action closure (`actions` above).
  const obligated = new Set();
  for (const d of disjuncts) for (const a of reachableClosure(d, defs)) obligated.add(a);
  const lines = new Map(); // line number -> "Action: text"
  const extent = new Map(); // line number -> last line of that conjunct/disjunct
  const alias = new Map(); // line number -> [start, end] of a bare operator it references
  for (const a of obligated) {
    const def = defs.get(a);
    def.lines.forEach(({ no, text }, i) => {
      const t = text.trim();
      if (!t) return;
      if (/^(\/\\|\\\/|\[\]|CASE\b|THEN\b|ELSE\b)/.test(t) || (i === 0 && def.lines.length === 1)) {
        lines.set(no, `${a}: ${t}`);
        // TLC attributes a bare operator reference (`/\ Settled`) to the
        // operator's DEFINITION, not the call site.
        const bare = t.match(/^(?:\/\\|\\\/)?\s*~?\s*([A-Za-z_]\w*)$/);
        const target = bare && defs.get(bare[1]);
        if (target) alias.set(no, [target.start, target.lines[target.lines.length - 1].no]);
      }
    });
    // A conjunct/disjunct extends to the line before the next one in the same
    // action: TLC attributes e.g. `/\ LET x == .. IN body` to `body`'s line.
    const own = [...lines.keys()].filter((n) => n >= def.start && n <= def.lines[def.lines.length - 1].no).sort((a, b) => a - b);
    own.forEach((n, k) => extent.set(n, (own[k + 1] ?? def.lines[def.lines.length - 1].no + 1) - 1));
  }
  return { disjuncts, closures: new Map(disjuncts.map((d) => [d, actionClosure(d, defs)])), lines, alias, extent };
}

// Parse the LAST coverage block of a TLC log for module `mod`.
export function parseCoverage(log, mod) {
  const i = log.lastIndexOf('The coverage statistics');
  const fired = new Map(); // action name -> found
  const lines = new Set(); // evaluated start lines in `mod`
  if (i < 0) return { present: false, fired, lines };
  const rows = log.slice(i).split('\n');
  rows.forEach((l, k) => {
    let m = l.match(/^<(\w+) line \d+, col \d+ to line \d+, col \d+ of module (\w+)[^>]*>(?:: (\d+):(\d+))?/);
    if (m) {
      if (m[2] === mod && m[4] !== undefined) fired.set(m[1], (fired.get(m[1]) ?? 0) + Number(m[4]));
      return;
    }
    m = l.match(/^\s*(\|*)line (\d+), col \d+ to line (\d+), col \d+ of module (\w+): (\d+)(?::(\d+))?/);
    if (!m || m[4] !== mod || Number(m[6] ?? m[5]) === 0) return;
    const from = Number(m[2]);
    lines.add(from);
    // A LEAF location (TLC did not decompose it further) is evaluated as one
    // unit, so every line it spans was evaluated — TLC reports e.g. a block of
    // primed assignments under one `\E` as a single multi-line location.
    const next = rows[k + 1]?.match(/^\s*(\|*)line /);
    if (!next || next[1].length <= m[1].length) {
      for (let n = from + 1; n <= Number(m[3]); n++) lines.add(n);
    }
  });
  return { present: true, fired, lines };
}

export function checkSpec({ name, src, logs }) {
  const exp = expectations(src);
  const fired = new Map();
  const covered = new Set();
  const problems = [];
  for (const [model, log] of logs) {
    const cov = parseCoverage(log, name);
    if (!cov.present) problems.push(`${model}: TLC log has no coverage statistics`);
    for (const [a, n] of cov.fired) fired.set(a, (fired.get(a) ?? 0) + n);
    for (const l of cov.lines) covered.add(l);
  }
  for (const d of exp.disjuncts) {
    const ok = [...exp.closures.get(d)].some((a) => (fired.get(a) ?? 0) > 0);
    if (!ok) problems.push(`action ${d} never fires in any model`);
  }
  for (const [no, what] of [...exp.lines].sort((a, b) => a[0] - b[0])) {
    const [lo, hi] = exp.alias.get(no) ?? [no, no];
    let hit = false;
    for (let l = no; !hit && l <= (exp.extent.get(no) ?? no); l++) hit = covered.has(l);
    for (let l = lo; !hit && l <= hi; l++) hit = covered.has(l);
    if (!hit) problems.push(`line ${no} never evaluated — ${what}`);
  }
  return problems;
}

function readDescriptor(file) {
  const t = fs.readFileSync(file, 'utf8');
  const get = (k, d) => (t.match(new RegExp(`^${k}="([^"]*)"`, 'm')) ?? [])[1] ?? d;
  return { name: get('SPEC_NAME'), dir: get('SPEC_MODELS_DIR', '.'), glob: get('SPEC_MODELS_GLOB') };
}

const globRe = (g) => new RegExp(`^${g.replace(/[.+^${}()|\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')}$`);

function main(argv) {
  const report = argv.includes('--report');
  const extra = [];
  const pos = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--extra') extra.push(argv[++i]);
    else if (!argv[i].startsWith('--')) pos.push(argv[i]);
  }
  const logDir = pos[0];
  if (!logDir) {
    console.error('usage: coverage.mjs <FORMAL_LOG_DIR> [--report]');
    return 2;
  }
  let failed = false;
  const specsDir = path.join(here, 'specs');
  for (const f of fs.readdirSync(specsDir).filter((f) => f.endsWith('.spec')).sort()) {
    const d = readDescriptor(path.join(specsDir, f));
    const modelDir = path.join(here, d.dir);
    const re = globRe(d.glob);
    const models = fs.readdirSync(modelDir).filter((m) => re.test(m)).map((m) => m.replace(/\.tla$/, ''));
    const logs = [];
    for (const m of models) {
      const p = path.join(logDir, `${m}.log`);
      if (fs.existsSync(p)) logs.push([m, fs.readFileSync(p, 'utf8')]);
    }
    for (const dir of extra) {
      for (const f of fs.readdirSync(dir).filter((f) => f.endsWith('.log')).sort()) {
        if (re.test(f.replace(/\.log$/, '.tla'))) logs.push([f, fs.readFileSync(path.join(dir, f), 'utf8')]);
      }
    }
    const problems = [];
    const missing = models.filter((m) => !fs.existsSync(path.join(logDir, `${m}.log`)));
    if (missing.length) {
      problems.push(`missing TLC logs for registered models: ${missing.join(', ')}`);
    }
    problems.push(...checkSpec({ name: d.name, src: fs.readFileSync(path.join(modelDir, `${d.name}.tla`), 'utf8'), logs }));
    const tag = problems.length ? (report ? 'note' : 'FAIL') : 'ok  ';
    console.log(`${tag}  coverage ${d.name}: ${problems.length ? `${problems.length} gap(s)` : 'every action and disjunct fires'}`);
    for (const p of problems) console.log(`        ${p}`);
    if (problems.length) failed = true;
  }
  return failed && !report ? 1 : 0;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  process.exit(main(process.argv.slice(2)));
}
