#!/usr/bin/env node
// feel-scala differential gate (#1391 step 5).
//
// Compares, row by row, the reference outcome column of the Lean-generated FEEL
// corpus with feel-scala's outcome for the same row (FeelScalaEval.java). The
// reference column IS nano's outcome: feel-diff.sh first runs
// engine-core/examples/feel_diff.rs, which fails on any Rust≠Lean row, so by the
// time this gate runs nano == reference on every row.
//
// Every divergence must be listed in known-divergences.json with an issue
// reference. The gate fails on:
//   - an unlisted divergence,
//   - a listed divergence whose outcomes changed,
//   - a STALE listed divergence (the row no longer diverges, or is no longer in
//     the corpus) — the list may only describe divergences that still exist,
//   - a listed issue reference that is malformed / undeclared.
//
// Usage: compare.mjs <corpus.tsv> <feel-scala-outcomes.txt> [known-divergences.json]
//        compare.mjs --stubs <corpus.tsv> <outcomes.txt> [known-divergences.json]
//          (print UNLISTED divergent rows as JSON stubs — rows already listed in
//           known-divergences.json are excluded, so the output can be appended)
import { readFileSync, writeSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
export const DEFAULT_KNOWN = join(here, 'known-divergences.json');

const key = (expr, context) => `${expr}\t${context}`;

/** Pairs corpus rows with feel-scala outcomes; throws on a count mismatch. */
export function parseRows(corpusText, outcomesText) {
  const rows = corpusText.split('\n').filter((l) => l !== '');
  const outs = outcomesText.split('\n').filter((l) => l !== '');
  if (rows.length === 0) throw new Error('empty corpus — refusing to pass the feel-scala gate');
  if (rows.length !== outs.length) {
    throw new Error(`corpus has ${rows.length} rows but feel-scala produced ${outs.length} outcomes`);
  }
  return rows.map((line, i) => {
    const [expr, context, nano, ...rest] = line.split('\t');
    if (nano === undefined || rest.length) throw new Error(`corpus line ${i + 1}: expected 3 fields`);
    return { expr, context, nano, feelScala: outs[i] };
  });
}

const ISSUE_REF = /^#\d+$/;

/**
 * True iff `ref` is a well-formed `#N` string declared as an OWN property of
 * `issues` (inherited names such as `toString` / `__proto__` never count).
 */
export function citesDeclaredIssue(issues, ref) {
  return typeof ref === 'string' && ISSUE_REF.test(ref) && Object.hasOwn(issues, ref) && Boolean(issues[ref]);
}

/** Returns the list of failure messages (empty = pass). */
export function compare(rows, known) {
  const failures = [];
  const issues = known.issues ?? {};
  for (const ref of Object.keys(issues)) {
    if (!ISSUE_REF.test(ref)) failures.push(`issue key ${JSON.stringify(ref)} is not of the form #N`);
  }
  const listed = new Map();
  for (const c of known.cases ?? []) {
    const k = key(c.expr, c.context);
    if (listed.has(k)) failures.push(`duplicate known divergence: ${c.expr} [${c.context}]`);
    if (!citesDeclaredIssue(issues, c.issue)) failures.push(`known divergence ${c.expr} [${c.context}] cites undeclared issue ${JSON.stringify(c.issue)}`);
    listed.set(k, { ...c, seen: false });
  }
  let divergent = 0;
  for (const r of rows) {
    const entry = listed.get(key(r.expr, r.context));
    if (entry) entry.seen = true;
    if (r.nano === r.feelScala) {
      if (entry) failures.push(`STALE known divergence (now agrees: ${r.nano}): ${r.expr} [${r.context}] (${entry.issue}) — remove it`);
      continue;
    }
    divergent += 1;
    if (!entry) {
      failures.push(`UNLISTED divergence:\n  expr:       ${r.expr}\n  context:    ${r.context}\n  nano:       ${r.nano}\n  feel-scala: ${r.feelScala}`);
    } else if (entry.nano !== r.nano || entry.feelScala !== r.feelScala) {
      failures.push(`CHANGED known divergence ${r.expr} [${r.context}]: listed nano=${entry.nano} feel-scala=${entry.feelScala}, now nano=${r.nano} feel-scala=${r.feelScala}`);
    }
  }
  for (const e of listed.values()) {
    if (!e.seen) failures.push(`STALE known divergence (row not in corpus): ${e.expr} [${e.context}] (${e.issue}) — remove it`);
  }
  return { failures, divergent, checked: rows.length };
}

/**
 * Divergent rows NOT already listed in `known`, as JSON stubs (`issue: ''`) for
 * the author to append to known-divergences.json and assign an issue. Emitting
 * an already-listed row would append a duplicate the gate then rejects, so
 * listed `(expr, context)` keys are excluded. A corpus can repeat a row; one
 * stub covers every occurrence of its key.
 */
export function unlistedStubs(rows, known) {
  const listed = new Set((known.cases ?? []).map((c) => key(c.expr, c.context)));
  const seen = new Set();
  return rows
    .filter((r) => r.nano !== r.feelScala)
    .filter((r) => !listed.has(key(r.expr, r.context)))
    .filter((r) => !seen.has(key(r.expr, r.context)) && seen.add(key(r.expr, r.context)))
    .map((r) => ({ ...r, issue: '' }));
}

function main(argv) {
  if (argv[0] === '--stubs') {
    const [, corpus, outcomes, knownPath = DEFAULT_KNOWN] = argv;
    if (!corpus || !outcomes) {
      console.error('usage: compare.mjs --stubs <corpus.tsv> <outcomes.txt> [known-divergences.json]');
      return 2;
    }
    const rows = parseRows(readFileSync(corpus, 'utf8'), readFileSync(outcomes, 'utf8'));
    const known = JSON.parse(readFileSync(knownPath, 'utf8'));
    // Synchronous: a buffered pipe write would be cut short by process.exit.
    writeSync(1, `${JSON.stringify(unlistedStubs(rows, known), null, 2)}\n`);
    return 0;
  }
  const [corpus, outcomes, knownPath = DEFAULT_KNOWN] = argv;
  if (!corpus || !outcomes) {
    console.error('usage: compare.mjs <corpus.tsv> <feel-scala-outcomes.txt> [known-divergences.json]');
    return 2;
  }
  const rows = parseRows(readFileSync(corpus, 'utf8'), readFileSync(outcomes, 'utf8'));
  const known = JSON.parse(readFileSync(knownPath, 'utf8'));
  const { failures, divergent, checked } = compare(rows, known);
  if (failures.length) {
    for (const f of failures) console.error(f);
    console.error(`FAIL: ${failures.length} feel-scala gate failure(s) over ${checked} case(s)`);
    return 1;
  }
  console.log(`ok: ${checked} FEEL cases checked against feel-scala; ${divergent} known divergence(s), all listed`);
  return 0;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) process.exit(main(process.argv.slice(2)));
