#!/usr/bin/env node
// Guard + report for the Zeebe parity coverage matrix (#1245).
//
// Every cell of the derived `zeebe-surface.json` must be claimed by a rule in
// `coverage.json`, and every claim must be checkable:
//
//   parity       evidence: `traces:<scenario>` — a captured Zeebe trace
//                (formal/traces/goldens/<scenario>.cells.json) whose recorded
//                cells touch each claimed cell — or, for `validation:` cells
//                only, a conformance-corpus fixture carrying a captured Zeebe
//                verdict (accept/reject — a `diverge` fixture is not parity):
//                a deploy-time verdict never appears in a runtime trace, so
//                the fixture's declared `<!-- zeebe-cells: … -->` stand in for
//                the trace touch. Every non-`validation:` parity cell must be
//                touched by a captured trace (#1391) — and only by records the
//                normaliser KEEPS: an intent `formal/traces/normaliser.json`
//                drops is never compared by the differential runner, so a raw
//                trace touch is not parity evidence for it.
//   nano-tested  evidence: `path::test_fn`, a `#[test]` in that file whose
//                attribute block declares each claimed cell in
//                `// zeebe-cells: …`. TRANSITIONAL and ratcheted
//                (`nano-tested.json`): the set may only shrink — promote a
//                cell to `parity` as soon as a captured trace covers it;
//                CI fails when the set grows (`--base-nano-tested`).
//   gap          issue: the tracking issue that closes the gap. Ratcheted:
//                only cells frozen in `gaps.json` may be gaps, so a cell a pin
//                bump adds cannot hide under a wildcard, and CI keeps the
//                baseline shrink-only across branches (`--base-gaps`);
//                `--update-gaps` regenerates it from the current gap
//                assignments (dropping promoted cells, admitting a cell that
//                legitimately regresses to gap)
//   out-of-scope issue + note: why the cell has no Nano meaning
//
// Fails on an unmapped cell, a dead rule (claims no cell), unverifiable
// evidence, a malformed rule, a cell id in a trace cells file that
// `extract.mjs` does not know (grammar drift between the capture harness and
// the extractor), or a surface extracted from a different Zeebe
// revision than `zeebe-pin.json`, or a `coverage.json` not yet reviewed at that
// revision (`reviewedAt`), or a `gaps.json` / `nano-tested.json` out of step
// with the assignments.
// Prints per-family counts, and appends a Markdown table to
// $GITHUB_STEP_SUMMARY when set.
//
// Usage: node formal/parity/check.mjs [--update-gaps | --update-ratchet | --base-gaps <target-branch gaps.json> [--base-pin <target-branch zeebe-pin.json>] [--base-surface <target-branch zeebe-surface.json>] [--base-nano-tested <target-branch nano-tested.json>]]

import { appendFileSync, existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { coarseCellId } from './extract.mjs';

export const STATUSES = ['parity', 'nano-tested', 'gap', 'out-of-scope'];
export const CORPUS = 'engine-core/tests/conformance/corpus/';
export const TRACES = 'formal/traces/';
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

/**
 * The cell ids a captured Zeebe trace touched (`formal/traces/goldens/
 * <scenario>.cells.json`). Returns null when the cells file cannot be read.
 */
export function traceCells(text) {
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  return Array.isArray(parsed?.cells) ? parsed.cells : null;
}

/** Cells an evidence reference declares, or null if it cannot be read. */
function declaredCells(status, ref, fs) {
  if (typeof ref !== 'string') return null;
  if (status === 'parity') {
    if (ref.startsWith('traces:')) {
      const path = `${TRACES}goldens/${ref.slice('traces:'.length)}.cells.json`;
      return fs.exists(path) ? traceCells(fs.read(path)) : null;
    }
    return fs.exists(ref) ? fixtureCells(fs.read(ref)) : null;
  }
  const sep = ref.indexOf('::');
  const path = ref.slice(0, sep);
  return sep > 0 && fs.exists(path) ? testCells(fs.read(path), ref.slice(sep + 2)) : null;
}

/** Problems with one evidence reference, given repo-file access `fs`. */
export function evidenceProblems(status, ref, fs) {
  if (typeof ref !== 'string' || ref.length === 0) return ['evidence must be a non-empty string'];
  if (status === 'parity') {
    if (ref.startsWith('traces:')) {
      const scenario = ref.slice('traces:'.length);
      // The scenario name becomes a file path segment — never allow traversal.
      if (!/^[A-Za-z0-9][A-Za-z0-9-]*$/.test(scenario)) return [`bad scenario name in ${ref}`];
      const cellsPath = `${TRACES}goldens/${scenario}.cells.json`;
      if (!fs.exists(`${TRACES}scenarios/${scenario}/script.json`)) return [`missing scenario: ${ref}`];
      if (!fs.exists(`${TRACES}goldens/${scenario}.trace.json`)) return [`missing golden trace for ${ref}`];
      if (!fs.exists(cellsPath)) return [`missing touched-cells file for ${ref}`];
      const cells = traceCells(fs.read(cellsPath));
      if (cells === null) return [`unparseable touched-cells file: ${cellsPath}`];
      if (cells.length === 0) return [`${cellsPath} declares no cells`];
      return [];
    }
    if (!ref.startsWith(CORPUS) || !ref.endsWith('.bpmn')) {
      return [`parity evidence must be a traces:<scenario> ref or a ${CORPUS}*.bpmn fixture: ${ref}`];
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
 * The union of cells touched by every committed captured Zeebe trace
 * (`formal/traces/goldens/ *.cells.json`), and the problems found reading them.
 *
 * Every id in a cells file must be a cell `extract.mjs` emits for the pinned
 * Zeebe (`known`): the capture harness (Java) and the static extractor (JS)
 * implement one shared cell-id grammar, so an unknown id means that grammar
 * drifted — fail loudly rather than silently dropping the cell. Cells files
 * captured at another Zeebe revision are stale: refresh them
 * (`formal/traces/capture.sh`) on a pin bump.
 */
export function loadTouchedCells(fs, known, pinSha, normaliser) {
  const touched = new Set();
  const errors = [];
  const pinFile = `${TRACES}goldens/PIN`;
  if (!fs.exists(pinFile)) {
    return { touched, errors: [`missing ${pinFile}: capture the Zeebe goldens (formal/traces/capture.sh)`] };
  }
  const capturedAt = fs.read(pinFile).trim();
  if (pinSha && capturedAt !== pinSha) {
    errors.push(
      `trace goldens were captured at ${capturedAt}, but zeebe-pin.json pins ${pinSha}: refresh them (formal/traces/capture.sh)`,
    );
  }
  // Scenarios the differential runner ACCEPTS mismatches for
  // (`allowlist.json`, `zeebe_traces.rs::verdict`) are known divergences: their
  // traces are never parity evidence.
  const allowed = new Set();
  const allowFile = `${TRACES}allowlist.json`;
  if (fs.exists(allowFile)) {
    let entries;
    try {
      entries = JSON.parse(fs.read(allowFile))?.entries;
    } catch {
      entries = undefined;
    }
    if (!Array.isArray(entries)) errors.push(`${allowFile} must have an entries array`);
    else for (const e of entries) if (typeof e?.scenario === 'string') allowed.add(e.scenario);
  }
  for (const path of fs.list(`${TRACES}goldens`).filter((f) => f.endsWith('.cells.json'))) {
    const full = `${TRACES}goldens/${path}`;
    const scenario = path.slice(0, -'.cells.json'.length);
    // An orphan cells file (no scenario script or no golden trace) is not
    // backed by anything the differential runner replays: reject it.
    if (!fs.exists(`${TRACES}scenarios/${scenario}/script.json`) || !fs.exists(`${TRACES}goldens/${scenario}.trace.json`)) {
      errors.push(`${full} has no matching scenario script and golden trace: orphan cells file`);
      continue;
    }
    const cells = traceCells(fs.read(full));
    if (cells === null) {
      errors.push(`unparseable touched-cells file: ${full}`);
      continue;
    }
    for (const c of cells) {
      if (known && !known.has(c)) errors.push(`${full} lists a cell extract.mjs does not know: ${c}`);
      // Grammar is validated for every cells file (allow-listed included), but
      // only non-allow-listed scenarios are eligible parity evidence.
      if (!allowed.has(scenario)) touched.add(c);
    }
  }
  // An intent cell is parity evidence only when the differential runner
  // actually COMPARES its records: `normaliser.json` (the one spec shared by
  // the Java capture harness and the Rust runner) drops every EVENT record
  // whose valueType/intent no lane lists, so a raw trace touch of a dropped
  // intent proves nothing about Nano. Remove those ids from the eligible set.
  if (normaliser !== undefined) {
    const kept = keptIntents(normaliser);
    if (kept === null) {
      errors.push(`${TRACES}normaliser.json must have a lanes array`);
    } else {
      for (const c of [...touched]) {
        if (c.startsWith('intent:') && !kept.has(c)) touched.delete(c);
      }
    }
  }
  return { touched, errors };
}

/**
 * The `intent:<Record>:<INTENT>` cells whose records `normaliser.json` keeps
 * for comparison (lane valueType in PascalCase + listed intent), or null when
 * the spec is malformed. The valueType->Record mapping is the identity
 * transform both normaliser implementations share (PROCESS_INSTANCE ->
 * ProcessInstance): the capture harness derives the same stem from the intent
 * enum's class name (`CellIds.intentCell`).
 */
export function keptIntents(normaliser) {
  if (!Array.isArray(normaliser?.lanes)) return null;
  const kept = new Set();
  for (const lane of normaliser.lanes) {
    if (typeof lane?.valueType !== 'string' || !Array.isArray(lane?.intents)) return null;
    const stem = lane.valueType
      .split('_')
      .filter(Boolean)
      .map((p) => p.charAt(0) + p.slice(1).toLowerCase())
      .join('');
    for (const intent of lane.intents) kept.add(`intent:${stem}:${intent}`);
  }
  return kept;
}

/**
 * Check `coverage` against `surface`, the `gaps` baseline (`gaps.json`'s cell
 * list), the `nanoTested` baseline (`nano-tested.json`'s cell list) and, when
 * given, the `pin`. `normaliser` is the parsed `formal/traces/normaliser.json`
 * spec: intent cells its lanes drop are not eligible parity evidence. Returns
 * `{ errors, assignments }`, where `assignments` maps each cell id to the
 * status of the first rule claiming it.
 */
export function evaluate(surface, coverage, fs, gaps, pin, nanoTested, normaliser) {
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
  // `parity` means "verified against a captured Zeebe trace" (#1391): load the
  // cells every committed trace touches, then require every parity-claimed
  // cell to be among them.
  const { touched, errors: traceErrors } = loadTouchedCells(fs, known, pin?.sha, normaliser);
  errors.push(...traceErrors);
  const parityTouched = new Set(touched);
  // A corpus fixture carries a captured Zeebe VERDICT (accept/reject), which is
  // itself parity evidence for the DEPLOY-VALIDATION cells it declares: a
  // deploy-time verdict never appears in a runtime trace, so `validation:` is
  // the one family a fixture may vouch for without a trace touching the cell.
  // Every other family stays strictly trace-touched — a fixture declaration
  // must never stand in for a captured trace.
  for (const r of rules) {
    if (r?.status !== 'parity' || !Array.isArray(r.evidence)) continue;
    for (const ref of r.evidence) {
      if (typeof ref === 'string' && !ref.startsWith('traces:') && fs.exists(ref)) {
        for (const c of fixtureCells(fs.read(ref))) {
          if (c.startsWith('validation:')) parityTouched.add(c);
        }
      }
    }
  }
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
      if (r.status === 'parity' && !parityTouched.has(c)) {
        errors.push(`parity cell ${c} is not touched by any captured Zeebe trace (formal/traces/goldens/*.cells.json)`);
      }
    }
  });
  errors.push(...gapProblems(assignments, gaps));
  // The nano-tested ratchet is validated whenever a baseline is supplied — and a
  // supplied-but-malformed baseline (a nano-tested.json without a `cells` array,
  // which reaches here as `undefined`) must fail closed, not skip the check and
  // let nano-tested assignments pass with no baseline. Only the regeneration
  // paths (`--update-gaps` / `--update-ratchet`) omit the argument entirely, and
  // they pass no sixth argument at all; distinguish "not requested" (arguments
  // length) from "malformed" (explicit undefined) so the former still regenerates.
  if (arguments.length >= 6) errors.push(...nanoTestedProblems(assignments, nanoTested));
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
 * The nano-tested ratchet (#1391): `nano-tested` is a TRANSITIONAL status — it
 * means "asserted by a nano test, not yet verified against a captured Zeebe
 * trace". The committed `nano-tested.json` baseline must list exactly the
 * cells currently assigned `nano-tested`, so the set cannot drift silently;
 * CI (`--base-nano-tested`) additionally refuses any growth against the target
 * branch. Promote a cell to `parity` (with `traces:<scenario>` evidence) as
 * soon as a captured trace covers it, then regenerate with `--update-ratchet`.
 */
export function nanoTestedProblems(assignments, nanoTested) {
  if (!Array.isArray(nanoTested)) return ['nano-tested.json must have a cells array'];
  const errors = [];
  const sorted = [...new Set(nanoTested)].sort();
  if (sorted.length !== nanoTested.length || sorted.some((c, i) => c !== nanoTested[i])) {
    errors.push('nano-tested.json cells must be sorted and unique: run check.mjs --update-ratchet');
  }
  const baseline = new Set(nanoTested);
  for (const [id, status] of assignments) {
    if (status === 'nano-tested' && !baseline.has(id)) {
      errors.push(`new nano-tested cell: ${id} is not in the nano-tested.json baseline; promote it to parity (a captured trace) or run check.mjs --update-ratchet`);
    }
  }
  for (const id of baseline) {
    const status = assignments.get(id);
    if (status === 'nano-tested') continue;
    errors.push(
      status === undefined
        ? `nano-tested.json lists ${id}, which is no longer a mapped cell: run check.mjs --update-ratchet`
        : `nano-tested.json lists ${id}, now ${status}: run check.mjs --update-ratchet to shrink the baseline`,
    );
  }
  return errors;
}

/** The current nano-tested cells that were already in the baseline (shrink-only). */
// Regenerates the baseline from the assignments (mirroring `shrinkGaps`): the
// only legitimate additions are pin-bump cells, and whether an addition is
// legitimate is decided by `nanoTestedGrowth` against the target branch — the
// single policing point — not by the regen. `_nanoTested` is unused but kept
// for call-site symmetry.
export function shrinkNanoTested(assignments, _nanoTested) {
  return [...assignments].filter(([, s]) => s === 'nano-tested').map(([id]) => id).sort();
}

/**
 * Ids the head nano-tested baseline illegitimately adds over the target
 * branch's. `nano-tested` is transitional, so the ratchet is strict: the set
 * may only shrink. The one exception is a Zeebe **pin bump** (`basePin.sha`
 * differs from `headPin.sha`, or `basePin` absent — fail-closed), and even then
 * only for **brand-new surface cells** — ids the bump introduced, i.e. absent
 * from the base surface (`baseSurfaceIds`, the cell ids of the target branch's
 * `zeebe-surface.json`). An id the base surface already lists must never
 * (re-)enter the baseline: that is an existing cell regressing from `parity`
 * to `nano-tested`, which a pin bump must not launder. Without the base
 * surface no addition is exempt (fail-closed).
 */
export function nanoTestedGrowth(base, head, basePin, headPin, baseSurfaceIds) {
  if (!Array.isArray(base)) {
    // The target branch predates the ratchet (no nano-tested.json there):
    // everything was effectively nano-tested, so any head set is a shrink.
    if (base === null) return [];
    return ['base nano-tested.json must have a cells array'];
  }
  const before = new Set(base);
  const added = head.filter((id) => !before.has(id));
  if (added.length === 0) return [];
  const pinBump = basePin == null || headPin == null || basePin.sha !== headPin.sha;
  const baseCells = Array.isArray(baseSurfaceIds) ? new Set(baseSurfaceIds) : null;
  return added
    .filter((id) => !pinBump || baseCells?.has(id) !== false)
    .map(
      (id) =>
        `nano-tested.json adds ${id}, which the target branch's baseline does not list: nano-tested is transitional and may only shrink — promote the cell to parity with a captured trace instead` +
        (pinBump && baseCells?.has(id)
          ? ' (a pin bump exempts only brand-new surface cells, and the base surface already lists this one)'
          : ''),
    );
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

/** The shrunk baseline: current gap cells, plus any newly-gapped cell (a cell
 * may regress to `gap` — e.g. evidence reclassified — and the baseline must
 * then admit it; CI's `--base-gaps` comparison is what keeps the baseline
 * shrink-only ACROSS branches). */
export function shrinkGaps(assignments, gaps) {
  return [...assignments].filter(([, s]) => s === 'gap').map(([id]) => id).sort();
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
    list: (p) => (existsSync(join(root, p)) ? readdirSync(join(root, p)) : []),
  };
  const surface = load('zeebe-surface.json');
  const gapsFile = load('gaps.json');
  const nanoTestedFile = load('nano-tested.json');
  const coverage = load('coverage.json');
  if (process.argv.includes('--update-gaps')) {
    const { assignments } = evaluate(surface, coverage, fs, gapsFile.cells);
    const cells = shrinkGaps(assignments, gapsFile.cells);
    writeFileSync(join(here, 'gaps.json'), `${JSON.stringify({ ...gapsFile, cells }, null, 2)}\n`);
    console.log(`gaps.json: ${gapsFile.cells.length} -> ${cells.length} cells`);
    return;
  }
  if (process.argv.includes('--update-ratchet')) {
    const { assignments } = evaluate(surface, coverage, fs, gapsFile.cells);
    const cells = shrinkNanoTested(assignments, nanoTestedFile.cells);
    writeFileSync(join(here, 'nano-tested.json'), `${JSON.stringify({ ...nanoTestedFile, cells }, null, 2)}\n`);
    console.log(`nano-tested.json: ${nanoTestedFile.cells.length} -> ${cells.length} cells`);
    return;
  }
  const headPin = load('zeebe-pin.json');
  const normaliser = JSON.parse(readFileSync(join(root, TRACES, 'normaliser.json'), 'utf8'));
  const { errors, assignments } = evaluate(surface, coverage, fs, gapsFile.cells, headPin, nanoTestedFile.cells, normaliser);
  // The base pin and base surface feed BOTH baseline ratchets, so parse them
  // independently of `--base-gaps`: a standalone `--base-nano-tested` check
  // must not silently treat a supplied pin as absent (absent reads as a pin
  // bump, which exempts growth).
  let basePin = null;
  const pinArg = process.argv.indexOf('--base-pin');
  if (pinArg !== -1) {
    const pinPath = process.argv[pinArg + 1];
    if (!pinPath) throw new Error('--base-pin needs a file path');
    basePin = JSON.parse(readFileSync(pinPath, 'utf8'));
  }
  let baseSurfaceIds = null;
  const surfArg = process.argv.indexOf('--base-surface');
  if (surfArg !== -1) {
    const surfPath = process.argv[surfArg + 1];
    if (!surfPath) throw new Error('--base-surface needs a file path');
    const cells = JSON.parse(readFileSync(surfPath, 'utf8')).cells;
    if (!Array.isArray(cells)) throw new Error('base zeebe-surface.json must have a cells array');
    baseSurfaceIds = cells.map((c) => c.id);
  }
  const baseArg = process.argv.indexOf('--base-gaps');
  if (baseArg !== -1) {
    const basePath = process.argv[baseArg + 1];
    if (!basePath) throw new Error('--base-gaps needs a file path');
    errors.push(...baselineGrowth(JSON.parse(readFileSync(basePath, 'utf8')).cells, gapsFile.cells, basePin, headPin, baseSurfaceIds));
  }
  const ntArg = process.argv.indexOf('--base-nano-tested');
  if (ntArg !== -1) {
    const ntPath = process.argv[ntArg + 1];
    if (!ntPath) throw new Error('--base-nano-tested needs a file path');
    errors.push(
      ...nanoTestedGrowth(JSON.parse(readFileSync(ntPath, 'utf8')).cells, nanoTestedFile.cells, basePin, headPin, baseSurfaceIds),
    );
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
