#!/usr/bin/env node
// Mutation testing of the token-flow specs' guards (#1248).
//
//   node formal/tla/mutate.mjs            # run every mutant; fail on an unjustified survivor
//   node formal/tla/mutate.mjs --list     # print the mutants, run nothing
//   node formal/tla/mutate.mjs --only <substring-of-id>
//
// "The suite can catch real defects": each mutant is the spec with ONE guard
// operator changed (a relational or logical operator flipped, a quantifier
// swapped, a negation dropped, a set operator changed, or a guard conjunct
// weakened to TRUE). A mutant is KILLED when some registered model
// (formal/tla/specs/<Spec>.spec, the same EXPECTED table check.sh enforces) no
// longer gets its expected verdict. A ZeebeTokenFlow mutant is also run against
// the TokenFlow models, which instantiate the Zeebe guard through `RefinesZeebe`.
//
// The mutated region is DERIVED from the spec, not listed: every operator
// definition reachable from `Next` (the actions and the guards they call — see
// coverage.mjs `expectations`). Fairness and property definitions are not
// mutated.
//
// A mutant that survives every registered model is then run against the CI
// bound's bounded-exhaustive graphs (formal/corpus/exhaustive.mjs — the only
// models with event-based gateways), killed when any graph's verdict differs
// from the unmodified spec's verdict on it.
//
// A survivor must either be killed by a new model, or be recorded in
// formal/tla/mutation/equivalent.json with the reason it is EQUIVALENT (no
// behaviour of any process graph distinguishes it). A recorded entry that is
// now killed, or that no longer exists, is stale and also fails the run, so the
// justification list cannot rot.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { parseDefs } from './coverage.mjs';
import { resolveBound } from '../corpus/exhaustive.mjs';
import { enumerateGraphs, artifactsFor } from '../corpus/enumerate.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const EQUIVALENT = path.join(here, 'mutation', 'equivalent.json');

// The mutated specs, and the spec families whose models each one is checked
// against.
export const TARGETS = [
  { file: 'TokenFlow.tla', families: ['TokenFlow'] },
  { file: 'ZeebeTokenFlow.tla', families: ['ZeebeTokenFlow', 'TokenFlow'] },
];

// Token-level mutation operators: [name, regex, replacement]. Each match of a
// regex in the mutated region yields one mutant.
export const OPERATORS = [
  ['eq->neq', /(?<![=<>'\\/#])=(?![=<>])/g, '#'],
  ['neq->eq', /#/g, '='],
  ['ge->gt', />=/g, '>'],
  ['gt->ge', /(?<![-<>|])>(?![=>])/g, '>='],
  ['le->lt', /<=/g, '<'],
  ['lt->le', /(?<!<)<(?![=<>-])/g, '<='],
  ['and->or', /(?<=\S\s*)\/\\/g, '\\/'],
  ['or->and', /(?<=\S\s*)\\\//g, '/\\'],
  ['forall->exists', /\\A(?=\s)/g, '\\E'],
  ['exists->forall', /\\E(?=\s)/g, '\\A'],
  ['drop-not', /~/g, ''],
  ['cup->cap', /\\cup/g, '\\cap'],
  ['minus->cup', /(?<=\s)\\(?=\s)/g, '\\cup'],
  ['inc->noop', /\+ 1\b/g, '+ 0'],
];

// Lines that are a whole guard conjunct: `/\ <expr>` on one line, with no
// primed variable or UNCHANGED (those are effects, not guards).
const GUARD_CONJUNCT = /^(\s*)\/\\\s+(?!.*('|\bUNCHANGED\b|\bLET\b|\bIF\b|\bCASE\b))(.+)$/;

// The definitions reachable from Next (actions and the operators they call).
function reachableDefs(defs) {
  const seen = new Set();
  const stack = ['Next'];
  while (stack.length) {
    const n = stack.pop();
    if (seen.has(n) || !defs.has(n)) continue;
    seen.add(n);
    for (const { text } of defs.get(n).lines) {
      for (const w of text.match(/[A-Za-z_][A-Za-z0-9_]*/g) ?? []) if (defs.has(w)) stack.push(w);
    }
  }
  seen.delete('Next');
  seen.delete('vars'); // the variable tuple, not a guard
  return seen;
}

// Is the `=` at `at` an assignment — `x' = e` or an `EXCEPT ![k][j] = e` /
// `EXCEPT !.f = e` update target — rather than a guard comparison such as
// `Tgt[f] = n` or `pending[g] = 0`?
export function isAssignmentLhs(text, at) {
  let i = at - 1;
  while (i >= 0 && /\s/.test(text[i])) i--;
  if (text[i] === "'") return true;
  // Walk back over a selector chain: `[...]` groups (nested-aware) and `.field`.
  let sawSelector = false;
  for (;;) {
    if (text[i] === ']') {
      let depth = 0;
      for (; i >= 0; i--) {
        if (text[i] === ']') depth++;
        else if (text[i] === '[' && --depth === 0) break;
      }
      if (i < 0) return false;
      i--;
      sawSelector = true;
    } else if (/\w/.test(text[i] ?? '')) {
      let j = i;
      while (j >= 0 && /\w/.test(text[j])) j--;
      if (text[j] !== '.') return false;
      i = j - 1;
      sawSelector = true;
    } else break;
  }
  return sawSelector && text[i] === '!';
}

export function mutantsOf(file, src) {
  const defs = parseDefs(src);
  const raw = src.split('\n');
  const out = [];
  for (const name of [...reachableDefs(defs)].sort()) {
    const def = defs.get(name);
    const count = new Map();
    const push = (op, lineNo, mutatedLine) => {
      const k = `${op}`;
      const n = (count.get(k) ?? 0) + 1;
      count.set(k, n);
      const lines = raw.slice();
      lines[lineNo - 1] = mutatedLine;
      out.push({
        id: `${file}:${name}:${op}#${n}`,
        file, def: name, line: lineNo,
        before: raw[lineNo - 1].trim(), after: mutatedLine.trim(),
        src: lines.join('\n'),
      });
    };
    for (const { no, text } of def.lines) {
      // `text` has comments blanked (same columns); for the header line it is
      // the body after `==`, so offset back into the raw line.
      const offset = no === def.start ? raw[no - 1].length - text.length : 0;
      // Mask string literals so `"<create>"` is never mutated.
      const masked = text.replace(/"[^"]*"/g, (q) => ' '.repeat(q.length));
      for (const [op, re, rep] of OPERATORS) {
        for (const m of masked.matchAll(re)) {
          // `x' = e` and `[f EXCEPT ![k] = e]` are assignments, not guards.
          if (op === 'eq->neq' && isAssignmentLhs(masked, m.index)) continue;
          const at = offset + m.index;
          const line = raw[no - 1];
          push(op, no, line.slice(0, at) + rep + line.slice(at + m[0].length));
        }
      }
      const g = no !== def.start && text.match(GUARD_CONJUNCT);
      const nextLine = raw[no] ?? '';
      // Only a single-line conjunct: the next line starts a new conjunct/arm at
      // the same or a shallower indent (or ends the definition).
      if (g && (!/^\s/.test(nextLine) || nextLine.trim() === '' || nextLine.search(/\S/) <= g[1].length)) {
        push('guard->TRUE', no, `${g[1]}/\\ TRUE`);
      }
    }
  }
  return out;
}

function readExpected(family) {
  const file = path.join(here, 'specs', `${family}.spec`);
  const out = execFileSync('bash', ['-c',
    'set -euo pipefail; source "$1"; printf "%s\\n" "${SPEC_CONSTANTS[@]}"; echo "---"; printf "%s\\n" "${SPEC_INVARIANTS[@]}"; echo "---"; printf "%s\\n" "${SPEC_PROPERTIES[@]}"; echo "---"; printf "%s\\n" "${SPEC_EXPECTED[@]}"',
    '_', file], { encoding: 'utf8' });
  const [constants, invariants, properties, expected] = out.split('---\n').map((s) => s.split('\n').filter(Boolean));
  const cfg = ['SPECIFICATION Spec', 'CONSTANTS', ...constants.map((c) => `    ${c}`),
    'INVARIANTS', ...invariants.map((c) => `    ${c}`),
    ...(properties.length ? ['PROPERTIES', ...properties.map((c) => `    ${c}`)] : [])].join('\n') + '\n';
  return { cfg, properties, models: expected.map((r) => r.trim().split(/\s+/)).map(([m, v]) => ({ model: m, want: v })) };
}

function tlaJar() {
  const src = fs.readFileSync(path.join(here, 'check.sh'), 'utf8');
  const version = /^TLA_VERSION="(.*)"$/m.exec(src)[1];
  const jar = process.env.TLA2TOOLS_JAR ||
    path.join(process.env.XDG_CACHE_HOME || path.join(os.homedir(), '.cache'), 'nanobpm-formal', `tla2tools-${version}.jar`);
  if (!fs.existsSync(jar)) throw new Error(`no tla2tools.jar at ${jar}: run formal/tla/check.sh once to fetch the pinned one`);
  return jar;
}

/** TLC log -> 'pass' | 'violates:<sorted>' | 'error', with check.sh's rules. */
export function verdict(log, properties, code, stopsAtFirst = false) {
  const v = new Set();
  for (const m of log.matchAll(/Invariant ([A-Za-z0-9_]+) is violated/g)) v.add(m[1]);
  if (/Temporal properties were violated/.test(log)) v.add(properties[0]);
  if (/Deadlock reached/.test(log)) v.add('Deadlock');
  if (/^Warning/m.test(log)) return 'error';
  if (v.size) return stopsAtFirst || /\b0 states left on queue/.test(log) ? `violates:${[...v].sort().join(',')}` : 'error';
  if (code === 0 && /Model checking completed\. No error has been found\./.test(log)) return 'pass';
  return 'error';
}

const normalise = (want) => (want.startsWith('violates:')
  ? `violates:${want.slice(9).split(',').sort().join(',')}` : want);

function runTlc(jar, dir, model, cfg, violating) {
  return new Promise((resolve) => {
    fs.writeFileSync(path.join(dir, `${model}.cfg`), cfg);
    const p = spawn('java', ['-XX:+UseParallelGC', '-Xmx1g', '-XX:TieredStopAtLevel=1',
      `-Djava.io.tmpdir=${path.join(dir, 'tmp')}`, '-cp', jar, 'tlc2.TLC', '-workers', '1', '-cleanup',
      ...(violating ? ['-continue', '-deadlock'] : []),
      '-metadir', path.join(dir, `${model}.meta`), '-config', `${model}.cfg`, `${model}.tla`], { cwd: dir });
    // With -continue a mutant can emit unbounded error traces, so keep only
    // the lines the verdict reads plus a bounded tail (for diagnostics).
    const KEEP = /violated|Deadlock|states left on queue|No error has been found|^Warning|Error|error|memory/;
    const kept = [];
    let tail = [];
    let partial = '';
    const take = (d) => {
      const rows = (partial + d).split('\n');
      partial = rows.pop();
      for (const r of rows) {
        if (KEEP.test(r) && kept.length < 10000) kept.push(r);
        tail.push(r);
      }
      if (tail.length > 400) tail = tail.slice(-200);
    };
    p.stdout.on('data', take);
    p.stderr.on('data', take);
    p.on('close', (code) => resolve({ code, log: [...kept, '---- tail ----', ...tail, partial].join('\n') }));
  });
}

// Run one mutant: returns { killed, by } — `by` names the model whose verdict
// changed (or 'invalid' when TLC rejects the mutated module).
// TLC evaluation errors caused by the mutated definitions (e.g. applying a
// function outside its domain): the mutant changed behaviour, so it is killed.
const EVAL_ERROR = /^Error: (?:Attempted to|The (?:first|second) argument|TLC threw an unexpected exception|Evaluating )|^Error: .*(?:not in the domain|is not enumerable|was not a)/m;

/** 'invalid' | 'eval' | null — null means the error says nothing about the mutant. */
export function classifyError(log) {
  if (INVALID.test(log)) return 'invalid';
  if (EVAL_ERROR.test(log)) return 'eval';
  return null;
}

const INVALID = /\*\*\* Parse Error \*\*\*|Semantic errors:|Unknown operator|Parsing or semantic analysis failed/;

async function runMutant(jar, work, mutant, families, tier2 = null) {
  const dir = fs.mkdtempSync(path.join(work, 'm-'));
  try {
    fs.mkdirSync(path.join(dir, 'tmp'));
    for (const f of fs.readdirSync(here)) if (f.endsWith('.tla')) fs.copyFileSync(path.join(here, f), path.join(dir, f));
    fs.writeFileSync(path.join(dir, mutant.file), mutant.src);
    // One ordered list of checks, cheapest-and-most-likely-to-kill first:
    // registered `pass` models (fast; they stop at the first violation), the
    // bounded-exhaustive graphs with an event-based gateway, the registered
    // violating models (they explore their whole liveness graph), then the
    // remaining bounded-exhaustive graphs. Registered models are judged
    // against check.sh's EXPECTED verdict; exhaustive graphs against the
    // unmutated spec's verdict, both run with -continue -deadlock.
    const registered = families.flatMap((fam) => fam.models.map(({ model, want }) => ({
      model, want: normalise(want), cfg: fam.cfg, properties: fam.properties,
      stopsAtFirst: want === 'pass', family: null,
    })));
    const t2 = (tier2 ? await tier2() : []).filter((t) => mutant.families.includes(t.family));
    const order = [
      ...registered.filter((m) => m.stopsAtFirst),
      ...t2.filter((t) => t.evt),
      ...registered.filter((m) => !m.stopsAtFirst),
      ...t2.filter((t) => !t.evt),
    ];
    for (const m of order) {
      if (m.family) fs.writeFileSync(path.join(dir, `${m.model}.tla`), m.src);
      const t = Date.now();
      const { code, log } = await runTlc(jar, dir, m.model, m.cfg, !m.stopsAtFirst);
      if (process.env.MUTATE_VERBOSE) console.log(`    ${mutant.id} ${m.model}: ${Date.now() - t}ms`);
      // A resource failure says nothing about the mutant: fail the run
      // rather than count it as a kill.
      if (/ran out of memory|OutOfMemoryError/.test(log)) {
        throw new Error(`TLC ran out of memory on ${m.model} for mutant ${mutant.id}`);
      }
      // A `pass` model runs without -continue, so TLC stops at the first
      // violation (the queue is not drained): any violation is a kill.
      const got = verdict(log, m.properties, code, m.stopsAtFirst);
      if (got === 'error') {
        const cls = classifyError(log);
        if (!cls) {
          // Warning-only output, a JVM killed before a verdict, a truncated
          // -continue run: none says anything about the mutant.
          throw new Error(`unclassified TLC error on ${m.model} for mutant ${mutant.id}:\n${log.slice(-2000)}`);
        }
        return { killed: true, by: cls === 'invalid' ? 'invalid' : `${m.model}(${cls})`, got };
      }
      if (got !== m.want) return { killed: true, by: m.model, got };
    }
    return { killed: false };
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

async function pool(items, limit, fn) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) { const i = next++; await fn(items[i], i); }
  }));
}

/** Compare survivors against the justification list; returns problem strings. */
export function judge(survivors, allIds, equivalent) {
  const problems = [];
  const justified = new Map(equivalent.map((e) => [e.id, e]));
  for (const s of survivors) {
    if (!justified.has(s.id)) problems.push(`SURVIVED ${s.id} (line ${s.line}): ${s.before}  ==>  ${s.after}`);
  }
  const surv = new Set(survivors.map((s) => s.id));
  for (const e of equivalent) {
    if (!e.reason || !e.reason.trim()) problems.push(`equivalent.json entry ${e.id} has no reason`);
    if (!allIds.has(e.id)) problems.push(`stale equivalent.json entry ${e.id}: no such mutant`);
    else if (!surv.has(e.id)) problems.push(`stale equivalent.json entry ${e.id}: the mutant is now killed (delete the entry)`);
  }
  return problems;
}

/**
 * The mutants an `--only` substring selects. A nonempty filter that matches NO
 * mutant is a defect (a typo in the manual workflow input, or a rename of the
 * guard the PR slice selects), not an empty success: running zero mutants and
 * reporting "0 survived" would validate nothing. Throws naming the closest
 * ids so the typo is obvious.
 */
export function selectMutants(mutants, only) {
  if (!only) return mutants;
  const selected = mutants.filter((m) => m.id.includes(only));
  if (selected.length === 0) {
    const ids = mutants.map((m) => m.id);
    const near = ids.filter((id) => id.toLowerCase().includes(only.toLowerCase().slice(0, 6))).slice(0, 5);
    throw new Error(
      `--only ${JSON.stringify(only)} matched no mutant (${ids.length} known). ` +
      (near.length ? `Closest: ${near.join(', ')}` : `e.g. ${ids.slice(0, 5).join(', ')}`));
  }
  return selected;
}

async function main(argv) {
  const only = argv.includes('--only') ? argv[argv.indexOf('--only') + 1] : null;
  const mutants = [];
  for (const t of TARGETS) {
    for (const m of mutantsOf(t.file, fs.readFileSync(path.join(here, t.file), 'utf8'))) mutants.push({ ...m, families: t.families });
  }
  if (argv.includes('--list')) {
    for (const m of mutants) console.log(`${m.id}  L${m.line}: ${m.before}  ==>  ${m.after}`);
    console.log(`${mutants.length} mutant(s)`);
    return 0;
  }
  const selected = selectMutants(mutants, only);
  const jar = tlaJar();
  const families = Object.fromEntries(['TokenFlow', 'ZeebeTokenFlow'].map((f) => [f, readExpected(f)]));
  const work = fs.mkdtempSync(path.join(os.tmpdir(), 'nanobpm-mutate-'));
  // Tier 2 (lazy, computed once): every CI-bound exhaustive graph model with
  // the UNMUTATED spec's verdict as its oracle.
  let tier2Promise = null;
  const tier2 = () => (tier2Promise ??= (async () => {
    const entries = enumerateGraphs(resolveBound({ bound: 'ci' })).map(({ graph }) => ({ graph, art: artifactsFor(graph) }));
    const models = entries.flatMap(({ graph, art }) => ['ZeebeTokenFlow', 'TokenFlow'].map((family) => ({
      family, model: graph.families[family].module, src: art.tla[family], stopsAtFirst: false,
      evt: Object.values(graph.nodes).includes('evt'),
      cfg: families[family].cfg, properties: families[family].properties,
    })));
    await pool(models, Math.max(1, Math.floor(os.availableParallelism() / 2)), async (m) => {
      // Own directory (and JVM temp dir) per concurrent TLC run: sharing one
      // clobbers the extracted standard modules.
      const base = fs.mkdtempSync(path.join(work, 'base-'));
      fs.mkdirSync(path.join(base, 'tmp'));
      for (const f of fs.readdirSync(here)) if (f.endsWith('.tla')) fs.copyFileSync(path.join(here, f), path.join(base, f));
      fs.writeFileSync(path.join(base, `${m.model}.tla`), m.src);
      const { code, log } = await runTlc(jar, base, m.model, m.cfg, true);
      m.want = verdict(log, m.properties, code);
      if (m.want === 'error') throw new Error(`TLC failed on unmutated ${m.model}:\n${log}`);
      fs.rmSync(base, { recursive: true, force: true });
    });
    console.log(`  tier 2: ${models.length} bounded-exhaustive model(s) baselined`);
    return models;
  })());
  const survivors = [];
  let invalid = 0;
  let done = 0;
  const t0 = Date.now();
  try {
    await pool(selected, Math.max(1, Math.floor(os.availableParallelism() / 2)), async (m) => {
      const r = await runMutant(jar, work, m, m.families.map((f) => families[f]), tier2);
      if (process.env.MUTATE_VERBOSE) console.log(`${m.id}: ${r.killed ? `killed by ${r.by} (${r.got})` : 'SURVIVED'}`);
      if (!r.killed) survivors.push(m);
      else if (r.by === 'invalid') invalid++;
      if (++done % 25 === 0) console.log(`  ${done}/${selected.length} mutants`);
    });
  } finally {
    fs.rmSync(work, { recursive: true, force: true });
  }
  survivors.sort((a, b) => (a.id < b.id ? -1 : 1));
  console.log(`${selected.length} mutant(s) in ${Math.round((Date.now() - t0) / 1000)}s: ` +
    `${selected.length - survivors.length} killed (${invalid} rejected by TLC), ${survivors.length} survived`);
  const equivalent = JSON.parse(fs.readFileSync(EQUIVALENT, 'utf8')).mutants;
  for (const s of survivors) {
    const e = equivalent.find((x) => x.id === s.id);
    console.log(`${e ? 'equivalent' : 'SURVIVED  '} ${s.id}  L${s.line}: ${s.before}  ==>  ${s.after}`);
  }
  if (only) return survivors.some((s) => !equivalent.find((x) => x.id === s.id)) ? 1 : 0;
  const problems = judge(survivors, new Set(mutants.map((m) => m.id)), equivalent);
  for (const p of problems) console.error(`FAIL ${p}`);
  if (!problems.length) console.log('ok    every surviving mutant is justified as equivalent');
  return problems.length ? 1 : 0;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main(process.argv.slice(2)).then((c) => process.exit(c), (e) => { console.error(e); process.exit(2); });
}
