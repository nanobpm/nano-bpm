#!/usr/bin/env node
// Per-Zeebe-test-class ratchet over the UNPARSEABLE harvest list (#1525).
//
//   node formal/harvest/unparseable-ratchet.mjs            # check (CI gate)
//   node formal/harvest/unparseable-ratchet.mjs --update   # rewrite the baseline
//
// The report counts unparseable models in aggregate only, so a whole family of
// Zeebe behaviours (e.g. every `multiinstance/*Test.java`) could silently stay
// — or newly become — unparseable without anyone seeing it. This guard keys the
// unparseable list by its Zeebe test class (the `.java` source path) and
// compares it against a committed, SHRINK-ONLY baseline
// (`unparseable-baseline.json`), the same discipline as
// `formal/parity/gaps.json`:
//
//   * a class absent from the baseline, or whose unparseable count GREW, is a
//     regression and fails the guard;
//   * a class whose count SHRANK (or vanished) also fails until the baseline is
//     regenerated with `--update`, so progress is locked in and the baseline
//     never carries slack a later regression could hide behind.
//
// The single source of truth for the per-class grouping is `unparseableByClass`
// below; report.mjs surfaces the same grouping in report.json.

import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
export const BASELINE_FILE = join(here, 'unparseable-baseline.json');

/** The Zeebe test class of a harvested/unparseable entry: its source path minus `.java`. */
export function testClassOf(source) {
  return String(source).replace(/\.java$/, '');
}

/** Count entries per test class, as a key-sorted plain object. */
function countByClass(entries) {
  const m = new Map();
  for (const e of entries) {
    const c = testClassOf(e.source);
    m.set(c, (m.get(c) ?? 0) + 1);
  }
  return Object.fromEntries([...m.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
}

/** Per-class unparseable counts from a corpus dir's `unparseable.json`. */
export function unparseableByClass(corpusDir) {
  const f = join(corpusDir, 'unparseable.json');
  return countByClass(existsSync(f) ? JSON.parse(readFileSync(f, 'utf8')) : []);
}

/** Per-class harvested counts from a corpus dir's `ir/*.json`. */
export function harvestedByClass(corpusDir) {
  const dir = join(corpusDir, 'ir');
  if (!existsSync(dir)) return {};
  const entries = readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .map((f) => JSON.parse(readFileSync(join(dir, f), 'utf8')));
  return countByClass(entries);
}

/**
 * Compare current per-class unparseable counts against the baseline. Returns a
 * list of error strings (empty = pass).
 */
export function checkRatchet(current, baseline) {
  const errors = [];
  if (!baseline || typeof baseline.classes !== 'object' || baseline.classes === null) {
    return ['unparseable-baseline.json must have a `classes` object'];
  }
  const base = baseline.classes;
  const keys = Object.keys(base);
  const sorted = [...keys].sort();
  if (keys.join('\n') !== sorted.join('\n')) {
    errors.push('unparseable-baseline.json classes must be sorted: run unparseable-ratchet.mjs --update');
  }
  for (const [cls, n] of Object.entries(current)) {
    if (!(cls in base)) {
      errors.push(`regression: ${cls} is newly unparseable (${n} model(s)); it is not in the baseline`);
    } else if (n > base[cls]) {
      errors.push(`regression: ${cls} unparseable grew ${base[cls]} -> ${n}`);
    }
  }
  for (const [cls, n] of Object.entries(base)) {
    const cur = current[cls] ?? 0;
    if (cur < n) {
      errors.push(`${cls} unparseable shrank ${n} -> ${cur}: run unparseable-ratchet.mjs --update to lock in the progress`);
    }
  }
  return errors;
}

function main() {
  const args = process.argv.slice(2);
  const cIdx = args.indexOf('--corpus');
  const corpusDir = cIdx >= 0 ? args[cIdx + 1] : join(here, 'corpus');
  const current = unparseableByClass(corpusDir);
  if (args.includes('--update')) {
    const out = {
      generator: 'formal/harvest/unparseable-ratchet.mjs',
      issue: 1525,
      note: 'Shrink-only per-Zeebe-test-class unparseable baseline. Regenerate with --update only when counts shrink.',
      classes: current,
    };
    writeFileSync(BASELINE_FILE, JSON.stringify(out, null, 2) + '\n');
    console.log(`wrote ${BASELINE_FILE} (${Object.keys(current).length} classes)`);
    return;
  }
  const baseline = existsSync(BASELINE_FILE) ? JSON.parse(readFileSync(BASELINE_FILE, 'utf8')) : null;
  const errors = checkRatchet(current, baseline);
  if (errors.length) {
    for (const e of errors) console.error(`::error::${e}`);
    process.exit(1);
  }
  console.log(`unparseable ratchet OK (${Object.keys(current).length} classes)`);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main();
}
