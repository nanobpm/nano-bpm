#!/usr/bin/env node
// Guard + report for the Zeebe parity coverage matrix (#1245).
//
// Every cell of the derived `zeebe-surface.json` must be claimed by a rule in
// `coverage.json`, and every claim must be checkable:
//
//   parity       evidence: conformance-corpus fixtures carrying a captured
//                Zeebe verdict (accept/reject — a `diverge` fixture is not parity)
//                that declare each claimed cell in `<!-- zeebe-cells: … -->`
//   nano-tested  evidence: `path::test_fn`, a `#[test]` in that file whose
//                attribute block declares each claimed cell in
//                `// zeebe-cells: …`
//   gap          issue: the tracking issue that closes the gap. Ratcheted:
//                only cells frozen in `gaps.json` may be gaps, so a cell a pin
//                bump adds cannot hide under a wildcard, and the baseline may
//                only shrink (`--update-gaps` never adds to it)
//   out-of-scope issue + note: why the cell has no Nano meaning
//
// Fails on an unmapped cell, a dead rule (claims no cell), unverifiable
// evidence, a malformed rule, or a surface extracted from a different Zeebe
// revision than `zeebe-pin.json`, or a `coverage.json` not yet reviewed at that
// revision (`reviewedAt`), or a `gaps.json` out of step with the gap cells.
// Prints per-family counts, and appends a Markdown table to
// $GITHUB_STEP_SUMMARY when set.
//
// Usage: node formal/parity/check.mjs [--update-gaps | --base-gaps <target-branch gaps.json> [--base-pin <target-branch zeebe-pin.json>]]

import { appendFileSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { coarseCellId } from './extract.mjs';

export const STATUSES = ['parity', 'nano-tested', 'gap', 'out-of-scope'];
export const CORPUS = 'engine-core/tests/conformance/corpus/';
const RULE_KEYS = new Set(['match', 'status', 'evidence', 'issue', 'note']);

/**
 * Cell families that name a single Zeebe source site (`failure:<Class>.<method>:…`,
 * `rejection:<Class>.<method>:…`). Because one site is one behaviour, a wildcard
 * `match` must never claim a whole family of them at once — that would let one
 * rule (or one piece of evidence) silently absorb dozens of distinct sites,
 * exactly the drift the per-site cells exist to expose. These cells may only be
 * claimed by an exact, wildcard-free rule.
 */
export const PER_SITE_FAMILIES = new Set(['failure', 'rejection']);

/** True when `match` contains no `*` wildcard. */
export function isExactMatch(match) {
  return typeof match === 'string' && !match.includes('*');
}

/** `*` matches any run of characters; everything else is literal. */
export function globToRegExp(glob) {
  const body = glob
    .split('*')
    .map((s) => s.replace(/[.+?^${}()|[\]\\]/g, '\\$&'))
    .join('.*');
  return new RegExp(`^${body}$`);
}

/** The cell ids a corpus fixture declares it exercises (`<!-- zeebe-cells: … -->`). */
export function fixtureCells(text) {
  const m = /<!--\s*zeebe-cells:\s*([\s\S]*?)\s*-->/.exec(text);
  return m ? m[1].split(/\s+/).filter(Boolean) : [];
}

/**
 * The cell ids a `#[test] fn name` declares (`// zeebe-cells: …` among the
 * comment and attribute lines directly above the fn).
 */
export function testCells(text, name) {
  const out = [];
  for (const line of testHeader(text, name) ?? []) {
    const m = /^\s*\/\/\s*zeebe-cells:\s*(.*)$/.exec(line);
    if (m) out.push(...m[1].split(/\s+/).filter(Boolean));
  }
  return out;
}

/** The comment and attribute lines directly above `fn name`, or null if there is no such fn. */
function testHeader(text, name) {
  const lines = text.split('\n');
  const fn = lines.findIndex((l) => new RegExp(`\\bfn\\s+${name}\\s*\\(`).test(l));
  if (fn < 0) return null;
  let i = fn;
  while (i > 0 && /^\s*(#\[|\/\/)/.test(lines[i - 1])) i--;
  return lines.slice(i, fn);
}

/** Cells an evidence reference declares, or null if it cannot be read. */
function declaredCells(status, ref, fs) {
  if (typeof ref !== 'string') return null;
  if (status === 'parity') return fs.exists(ref) ? fixtureCells(fs.read(ref)) : null;
  const sep = ref.indexOf('::');
  const path = ref.slice(0, sep);
  return sep > 0 && fs.exists(path) ? testCells(fs.read(path), ref.slice(sep + 2)) : null;
}

/** Problems with one evidence reference, given repo-file access `fs`. */
export function evidenceProblems(status, ref, fs) {
  if (typeof ref !== 'string' || ref.length === 0) return ['evidence must be a non-empty string'];
  if (status === 'parity') {
    if (!ref.startsWith(CORPUS) || !ref.endsWith('.bpmn')) {
      return [`parity evidence must be a ${CORPUS}*.bpmn fixture: ${ref}`];
    }
    if (!fs.exists(ref)) return [`missing fixture: ${ref}`];
    const verdict = /<!--\s*verdict:\s*(\w+)/.exec(fs.read(ref));
    if (!verdict) return [`fixture has no verdict header: ${ref}`];
    if (!['accept', 'reject'].includes(verdict[1])) {
      return [`fixture verdict '${verdict[1]}' is not Zeebe parity: ${ref}`];
    }
    return [];
  }
  const sep = ref.indexOf('::');
  if (sep < 0) return [`nano-tested evidence must be path::test_fn: ${ref}`];
  const [path, name] = [ref.slice(0, sep), ref.slice(sep + 2)];
  if (!/^[a-z_][a-z0-9_]*$/.test(name)) return [`bad test name in ${ref}`];
  if (!fs.exists(path)) return [`missing file: ${path}`];
  const attrs = (testHeader(fs.read(path), name) ?? []).filter((l) => /^\s*#\[/.test(l));
  if (!attrs.some((l) => /^\s*#\[test\]/.test(l))) return [`no #[test] fn ${name} in ${path}`];
  // `cargo test` skips an ignored test, so it would be evidence CI never runs.
  if (attrs.some((l) => /\bignore\b/.test(l))) return [`#[test] fn ${name} in ${path} is #[ignore]d, so CI never runs it`];
  return [];
}

/** Problems with the shape of one rule (independent of the surface). */
export function ruleProblems(rule, fs) {
  const where = `rule ${JSON.stringify(rule?.match)}`;
  if (rule === null || typeof rule !== 'object') return [`${where}: not an object`];
  const out = [];
  for (const k of Object.keys(rule)) if (!RULE_KEYS.has(k)) out.push(`${where}: unknown key '${k}'`);
  if (typeof rule.match !== 'string' || rule.match.length === 0) out.push(`${where}: match must be a non-empty string`);
  if (!STATUSES.includes(rule.status)) {
    out.push(`${where}: status must be one of ${STATUSES.join(', ')}`);
    return out;
  }
  const evidenced = rule.status === 'parity' || rule.status === 'nano-tested';
  if (evidenced) {
    if (!Array.isArray(rule.evidence) || rule.evidence.length === 0) out.push(`${where}: ${rule.status} needs evidence`);
    else for (const ref of rule.evidence) for (const p of evidenceProblems(rule.status, ref, fs)) out.push(`${where}: ${p}`);
  } else {
    if (rule.evidence !== undefined) out.push(`${where}: ${rule.status} takes no evidence`);
    if (!Number.isInteger(rule.issue) || rule.issue <= 0) out.push(`${where}: ${rule.status} needs an issue number`);
    if (typeof rule.note !== 'string' || rule.note.trim().length === 0) out.push(`${where}: ${rule.status} needs a note`);
  }
  if (evidenced && rule.issue !== undefined && (!Number.isInteger(rule.issue) || rule.issue <= 0)) {
    out.push(`${where}: issue must be a positive integer`);
  }
  return out;
}

/**
 * Check `coverage` against `surface`, the `gaps` baseline (`gaps.json`'s cell
 * list) and, when given, the `pin`. Returns `{ errors, assignments }`, where
 * `assignments` maps each cell id to the status of the first rule claiming it.
 */
export function evaluate(surface, coverage, fs, gaps, pin) {
  const errors = [];
  if (pin) {
    const id = (z) => `${z?.repository}@${z?.ref}(${z?.sha})`;
    if (['repository', 'ref', 'sha'].some((k) => surface?.zeebe?.[k] !== pin[k])) {
      errors.push(`zeebe-surface.json was extracted from ${id(surface?.zeebe)}, but zeebe-pin.json pins ${id(pin)}: regenerate it`);
    }
  }
  // Every bump is an explicit review of the surface diff, including cells whose
  // detail changed; the gap ratchet below separately stops new cells hiding.
  if (pin && coverage?.reviewedAt !== pin.sha) {
    errors.push(`coverage.json was reviewed at ${coverage?.reviewedAt}, but zeebe-pin.json pins ${pin.sha}: review the new cells in the zeebe-surface.json diff, map them, then set reviewedAt`);
  }
  const rules = Array.isArray(coverage?.rules) ? coverage.rules : [];
  if (!Array.isArray(coverage?.rules)) errors.push('coverage.json must have a rules array');
  const matchers = rules.map((r) => (typeof r?.match === 'string' ? globToRegExp(r.match) : null));
  for (const r of rules) errors.push(...ruleProblems(r, fs));
  const claims = rules.map(() => []);
  const assignments = new Map();
  for (const { id } of surface.cells) {
    const family = id.slice(0, id.indexOf(':'));
    const k = matchers.findIndex((m) => m !== null && m.test(id));
    if (k < 0) {
      errors.push(`unmapped cell: ${id}`);
      continue;
    }
    // A per-site cell names one Zeebe source site; only an exact rule may claim
    // it. If the first match is a wildcard, the cell is unclaimable as mapped.
    if (PER_SITE_FAMILIES.has(family) && !isExactMatch(rules[k].match)) {
      errors.push(
        `per-site cell ${id} is claimed by wildcard rule ${JSON.stringify(rules[k].match)}: ` +
          `${family}: cells need an exact (wildcard-free) match`,
      );
      continue;
    }
    claims[k].push(id);
    assignments.set(id, rules[k].status);
  }
  const known = new Set(surface.cells.map((c) => c.id));
  rules.forEach((r, k) => {
    if (matchers[k] !== null && claims[k].length === 0) errors.push(`dead rule (claims no cell): ${r.match}`);
    // A claim is only as good as its evidence: each claimed cell must be one a
    // fixture or test declares it exercises, and evidence may only declare real cells.
    if (!['parity', 'nano-tested'].includes(r?.status) || !Array.isArray(r.evidence)) return;
    const declared = new Set();
    for (const ref of r.evidence) {
      const cells = declaredCells(r.status, ref, fs);
      if (cells === null) continue;
      if (cells.length === 0) errors.push(`rule ${JSON.stringify(r.match)}: ${ref} declares no zeebe-cells`);
      for (const c of cells) {
        if (!known.has(c)) errors.push(`${ref} declares a cell not in zeebe-surface.json: ${c}`);
        declared.add(c);
      }
    }
    for (const c of claims[k]) {
      if (!declared.has(c)) errors.push(`rule ${JSON.stringify(r.match)}: no evidence declares ${c}`);
    }
  });
  errors.push(...gapProblems(assignments, gaps));
  return { errors, assignments };
}

/** The gap ratchet: gap cells and the `gaps.json` baseline must agree exactly. */
export function gapProblems(assignments, gaps) {
  if (!Array.isArray(gaps)) return ['gaps.json must have a cells array'];
  const errors = [];
  const sorted = [...new Set(gaps)].sort();
  if (sorted.length !== gaps.length || sorted.some((c, i) => c !== gaps[i])) {
    errors.push('gaps.json cells must be sorted and unique: run check.mjs --update-gaps');
  }
  const baseline = new Set(gaps);
  for (const [id, status] of assignments) {
    if (status === 'gap' && !baseline.has(id)) {
      errors.push(`new gap cell: ${id} is not in the gaps.json baseline; give it evidence or an out-of-scope rule`);
    }
  }
  for (const id of baseline) {
    const status = assignments.get(id);
    if (status === 'gap') continue;
    errors.push(
      status === undefined
        ? `gaps.json lists ${id}, which is no longer a mapped cell: run check.mjs --update-gaps`
        : `gaps.json lists ${id}, now ${status}: run check.mjs --update-gaps to shrink the baseline`,
    );
  }
  return errors;
}

/**
 * Ids the head baseline illegitimately adds over the target branch's: the
 * ratchet may only shrink, with two narrow exceptions.
 *
 * The ratchet exists so a Zeebe **pin bump** cannot hide newly-uncovered cells
 * under the baseline. So it is **pin-aware** (`basePin` is the target branch's
 * `zeebe-pin.json`):
 *
 *  - **Pin bump** (`basePin.sha` differs from `headPin.sha`, or `basePin` is
 *    absent — fail-closed): the baseline may only shrink. Any head id the base
 *    does not list is rejected.
 *  - **Same pin**: a head id the base does not list is allowed **only** as a
 *    *refinement* — a per-site cell whose coarse parent (per `coarseCellId`,
 *    the same grammar the extractor emits) was a base gap the head no longer
 *    lists. E.g. `rejection:<Class>.<method>:<CODE>` may appear only when
 *    `rejection:<Class>:<CODE>` was a base gap and is gone from head. Any other
 *    same-pin growth still fails.
 *  - **`failure:` seed**: a `failure:` cell's coarse parent is `incident:<TYPE>`,
 *    which is a *covered* cell, not a base gap — so the refinement rule can
 *    never admit the very first `failure:` cells. They are allowed to appear
 *    only when the base baseline contains **no** `failure:` cells at all (the
 *    one-time introduction of the family); the seed is gated on the
 *    target branch's **surface** (`baseSurfaceIds`, the cell ids of its
 *    `zeebe-surface.json`) containing no `failure:` cell — not on the gap
 *    baseline, because an empty `failure:` baseline also describes a family
 *    that was introduced and then fully covered. Without the base surface the
 *    seed is refused (fail-closed); once the family exists on the target
 *    branch it is shrink-only like everything else.
 */
export function baselineGrowth(base, head, basePin, headPin, baseSurfaceIds) {
  if (!Array.isArray(base)) return ['base gaps.json must have a cells array'];
  const before = new Set(base);
  const added = head.filter((id) => !before.has(id));
  if (added.length === 0) return [];
  const removed = new Set(base.filter((id) => !new Set(head).has(id)));
  // Fail closed: without the base pin we cannot tell a refinement from a bump.
  const samePin = basePin != null && headPin != null && basePin.sha === headPin.sha;
  const familyIntroduced =
    !Array.isArray(baseSurfaceIds) ||
    baseSurfaceIds.some((id) => id.startsWith('failure:')) ||
    base.some((id) => id.startsWith('failure:'));
  const errors = [];
  for (const id of added) {
    if (samePin) {
      if (id.startsWith('failure:') && !familyIntroduced) continue; // one-time family seed
      // Refinement applies to rejection: cells only; failure: cells enter solely
      // through the one-time seed above, never via a removed coarse gap.
      const parent = id.startsWith('rejection:') ? coarseCellId(id) : null;
      if (parent !== null && before.has(parent) && removed.has(parent)) continue; // a refinement
    }
    errors.push(
      `gaps.json adds ${id}, which the target branch's baseline does not list: the baseline may only shrink`,
    );
  }
  return errors;
}

/** The shrunk baseline: current gap cells that were already in it. */
export function shrinkGaps(assignments, gaps) {
  const baseline = new Set(gaps);
  return [...assignments].filter(([id, s]) => s === 'gap' && baseline.has(id)).map(([id]) => id).sort();
}

/** Per-family status counts: `{ family: { status: n } }`. */
export function tally(assignments) {
  const out = {};
  for (const [id, status] of assignments) {
    const family = id.slice(0, id.indexOf(':'));
    out[family] ??= Object.fromEntries(STATUSES.map((s) => [s, 0]));
    out[family][status]++;
  }
  return out;
}

export function markdownReport(counts, surface) {
  const lines = [
    `### Zeebe parity coverage — ${surface.zeebe.ref} @ \`${surface.zeebe.sha.slice(0, 12)}\``,
    '',
    `| family | ${STATUSES.join(' | ')} | total |`,
    `|---|${STATUSES.map(() => '---:').join('|')}|---:|`,
  ];
  const total = Object.fromEntries(STATUSES.map((s) => [s, 0]));
  for (const family of Object.keys(counts).sort()) {
    const row = counts[family];
    const n = STATUSES.reduce((a, s) => a + row[s], 0);
    for (const s of STATUSES) total[s] += row[s];
    lines.push(`| ${family} | ${STATUSES.map((s) => row[s]).join(' | ')} | ${n} |`);
  }
  const all = STATUSES.reduce((a, s) => a + total[s], 0);
  lines.push(`| **all** | ${STATUSES.map((s) => `**${total[s]}**`).join(' | ')} | **${all}** |`);
  return `${lines.join('\n')}\n`;
}

function main() {
  const here = dirname(fileURLToPath(import.meta.url));
  const root = join(here, '..', '..');
  const load = (f) => JSON.parse(readFileSync(join(here, f), 'utf8'));
  const fs = {
    exists: (p) => existsSync(join(root, p)),
    read: (p) => readFileSync(join(root, p), 'utf8'),
  };
  const surface = load('zeebe-surface.json');
  const gapsFile = load('gaps.json');
  const coverage = load('coverage.json');
  if (process.argv.includes('--update-gaps')) {
    const { assignments } = evaluate(surface, coverage, fs, gapsFile.cells);
    const cells = shrinkGaps(assignments, gapsFile.cells);
    writeFileSync(join(here, 'gaps.json'), `${JSON.stringify({ ...gapsFile, cells }, null, 2)}\n`);
    console.log(`gaps.json: ${gapsFile.cells.length} -> ${cells.length} cells`);
    return;
  }
  const headPin = load('zeebe-pin.json');
  const { errors, assignments } = evaluate(surface, coverage, fs, gapsFile.cells, headPin);
  const baseArg = process.argv.indexOf('--base-gaps');
  if (baseArg !== -1) {
    const basePath = process.argv[baseArg + 1];
    if (!basePath) throw new Error('--base-gaps needs a file path');
    const pinArg = process.argv.indexOf('--base-pin');
    let basePin = null;
    if (pinArg !== -1) {
      const pinPath = process.argv[pinArg + 1];
      if (!pinPath) throw new Error('--base-pin needs a file path');
      basePin = JSON.parse(readFileSync(pinPath, 'utf8'));
    }
    const surfArg = process.argv.indexOf('--base-surface');
    let baseSurfaceIds = null;
    if (surfArg !== -1) {
      const surfPath = process.argv[surfArg + 1];
      if (!surfPath) throw new Error('--base-surface needs a file path');
      const cells = JSON.parse(readFileSync(surfPath, 'utf8')).cells;
      if (!Array.isArray(cells)) throw new Error('base zeebe-surface.json must have a cells array');
      baseSurfaceIds = cells.map((c) => c.id);
    }
    errors.push(...baselineGrowth(JSON.parse(readFileSync(basePath, 'utf8')).cells, gapsFile.cells, basePin, headPin, baseSurfaceIds));
  }
  const report = markdownReport(tally(assignments), surface);
  process.stdout.write(report);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, report);
  if (errors.length > 0) {
    for (const e of errors) console.error(`check: ${e}`);
    console.error(`check: ${errors.length} problem(s)`);
    process.exit(1);
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) main();
