#!/usr/bin/env node
// Dual-engine branch-coverage report + ratchet for the parity corpus (#1247).
//
// Measures how much of EACH engine the parity corpus actually exercises:
//
//   nano    `cargo llvm-cov` JSON export over engine-core while the corpus
//           replays natively (engine-core/tests/parity_corpus.rs) — produced
//           by nano-coverage.sh. Branches when the export carries them
//           (`--branch`, nightly toolchains), otherwise LLVM code regions —
//           every branch arm is its own region, so region coverage is the
//           stable-toolchain branch proxy. The unit is recorded in the
//           baseline so a switch can never compare apples to oranges.
//   zeebe   JaCoCo XML report for `zeebe/engine` collected from a live
//           Camunda 8 with the JaCoCo agent attached while the corpus runs —
//           produced by zeebe-coverage.sh. Unit: bytecode branches.
//
// Triage rules (the tags on every uncovered site):
//   * an uncovered Zeebe branch is a MISSING SCENARIO — tagged with the
//     coverage-matrix cells (zeebe-surface.json) sourced at that line, or, when
//     none is, the cells sourced in the same file, each with its matrix status
//     (coverage.json, first matching rule);
//   * an uncovered Nano region/branch is either a registered Nano-only
//     extension (nano-extensions.json) or a MISSING SCENARIO.
//
// Ratchet (baseline.json): each side has a `floorPercent`. `--check` fails
// when a measured side drops below its floor minus the side's optional
// `slackPercent` (or measures in a different unit). `--update` raises floors
// to the measured values (truncated to 0.1 %) — the slack is applied ONCE, at
// check time, never also subtracted when seeding; it
// refuses to LOWER a floor unless `--allow-lower` is passed, so a drop is
// always an explicit, reviewed baseline change.
//
// Deterministic measured set (#1707): the zeebe side measures a LIVE broker,
// whose background scheduler sweeps and correlation races execute
// timing-dependent branch arms on identical inputs (two same-commit runs
// measured 1037 vs 1033 branches, flipping the gate). `zeebe-excludes.json`
// removes that surface from the measured set — whole packages (`packages`,
// recursive) and individual lines (`lines`) — so the ratchet compares only
// branches the corpus deterministically reaches. `--self-test N` re-parses
// each input N times and fails loudly with the differing branches when a
// re-parse is not byte-identical, so a nondeterministic measurement fails
// the gate instead of flapping it.
//
// Usage:
//   node formal/parity/coverage/report.mjs [--nano llvm-cov.json] [--zeebe jacoco.xml]
//        [--out report.json] [--check] [--update [--allow-lower]] [--baseline FILE]
//        [--self-test N]

import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

import { globToRegExp } from '../check.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, '../../..');
export const ZEEBE_ENGINE_SRC = 'zeebe/engine/src/main/java/';

/** Truncate a percentage to one decimal (the ratchet's granularity). */
export function floor1(pct) {
  return Math.floor(pct * 10) / 10;
}

function pct(covered, total) {
  return total === 0 ? 100 : (covered / total) * 100;
}

/**
 * An empty measurement (no instrumented sites at all) is a collection failure,
 * not 100 % coverage: `pct` maps `total === 0` to 100, which would PASS the
 * ratchet and let `--update` raise the floor to 100 % on an empty report.
 * Fail closed on it in both `--check` and `--update`.
 */
function emptyMeasurementErrors(measured) {
  const errors = [];
  for (const side of ['nano', 'zeebe']) {
    const m = measured[side];
    if (m && m.total === 0) {
      errors.push(
        `${side}: the coverage report contains no instrumented ${m.unit}s — the measurement is empty ` +
          '(collection failed or the filter matched nothing), so it must not pass the ratchet or seed a floor',
      );
    }
  }
  return errors;
}

/** First rule whose `match` glob matches `id`, or undefined. */
function firstMatch(rules, id) {
  return rules.find((r) => globToRegExp(r.match).test(id));
}

// ---------------------------------------------------------------- nano side --

/** Repo-relative path with `/` separators. */
function repoPath(file, repo = REPO) {
  const r = relative(repo, file);
  return r.split(sep).join('/');
}

/**
 * Parse a `cargo llvm-cov --json` export into per-site coverage for engine-core
 * sources. Generic instantiations repeat a function's regions, so sites are
 * de-duplicated by coordinates and the max count wins (a site is covered when
 * ANY instantiation executed it).
 */
export function parseLlvmCov(exportJson, { repo = REPO, include = 'engine-core/src/' } = {}) {
  const data = exportJson.data?.[0];
  if (!data) throw new Error('llvm-cov export has no data[0]');
  const sites = new Map(); // key -> { file, line, col, count }
  const branchSites = new Map();
  let sawBranches = false;
  const put = (map, file, line, col, endLine, endCol, arm, count) => {
    const key = `${file}:${line}:${col}:${endLine}:${endCol}:${arm}`;
    const prev = map.get(key);
    if (!prev || count > prev.count) map.set(key, { file, line, col, arm, count });
  };
  for (const f of data.files ?? []) {
    const file = repoPath(f.filename, repo);
    if (!file.startsWith(include)) continue;
    if (Array.isArray(f.branches) && f.branches.length > 0) sawBranches = true;
    for (const b of f.branches ?? []) {
      const [ls, cs, le, ce, t, fc] = b;
      put(branchSites, file, ls, cs, le, ce, 'true', t);
      put(branchSites, file, ls, cs, le, ce, 'false', fc);
    }
  }
  for (const fn of data.functions ?? []) {
    for (const r of fn.regions ?? []) {
      const [ls, cs, le, ce, count, fileId, , kind] = r;
      if (kind !== 0) continue; // code regions only (not expansion/skipped/gap)
      const file = repoPath(fn.filenames[fileId], repo);
      if (!file.startsWith(include)) continue;
      put(sites, file, ls, cs, le, ce, 'region', count);
    }
  }
  const unit = sawBranches ? 'branch' : 'region';
  const all = [...(sawBranches ? branchSites : sites).values()];
  return summarise(unit, all);
}

function summarise(unit, all) {
  const covered = all.filter((s) => s.count > 0).length;
  const uncovered = all
    .filter((s) => s.count === 0)
    .sort((a, b) => a.file.localeCompare(b.file) || a.line - b.line || (a.col ?? 0) - (b.col ?? 0));
  return { unit, covered, total: all.length, percent: pct(covered, all.length), uncovered };
}

/** Tag each uncovered Nano site: a registered extension, or a missing scenario. */
export function tagNano(uncovered, extensions) {
  return uncovered.map((s) => {
    const ext = firstMatch(extensions, s.file);
    return ext
      ? { ...s, tag: `nano-extension:${ext.id}` }
      : { ...s, tag: 'missing-scenario' };
  });
}

// --------------------------------------------------------------- zeebe side --

/**
 * Parse a JaCoCo XML report into per-line branch coverage for `zeebe/engine`.
 * One site per source line carrying branches (`mb`/`cb` counters); a line is
 * uncovered when any of its branches is missed.
 *
 * `exclude` (zeebe-excludes.json, #1707) removes the timing-dependent surface
 * from the measured set: `packages` drops every line whose JaCoCo package is
 * the named package or a subpackage of it; `lines` drops individual
 * `file:line` sites. Excluded sites count toward neither `covered` nor
 * `total` and are listed under `excluded` for audit.
 */
export function parseJacoco(xml, { exclude = {} } = {}) {
  const pkgRules = exclude.packages ?? [];
  const lineRules = new Set(exclude.lines ?? []);
  const sites = [];
  const excluded = [];
  const pkgRe = /<package name="([^"]*)">([\s\S]*?)<\/package>/g;
  for (const [, pkg, body] of xml.matchAll(pkgRe)) {
    const pkgRule = pkgRules.find((p) => pkg === p || pkg.startsWith(`${p}/`));
    const srcRe = /<sourcefile name="([^"]*)">([\s\S]*?)<\/sourcefile>/g;
    for (const [, name, sbody] of body.matchAll(srcRe)) {
      const file = `${ZEEBE_ENGINE_SRC}${pkg}/${name}`;
      for (const [attrs] of sbody.matchAll(/<line\b[^>]*\/>/g)) {
        const a = Object.fromEntries([...attrs.matchAll(/(\w+)="(\d+)"/g)].map(([, k, v]) => [k, Number(v)]));
        const branches = (a.mb ?? 0) + (a.cb ?? 0);
        if (branches === 0) continue;
        const site = { file, line: a.nr, missed: a.mb ?? 0, coveredBranches: a.cb ?? 0 };
        if (pkgRule) excluded.push({ ...site, rule: `package:${pkgRule}` });
        else if (lineRules.has(`${file}:${a.nr}`)) excluded.push({ ...site, rule: `line:${file}:${a.nr}` });
        else sites.push(site);
      }
    }
  }
  const covered = sites.reduce((n, s) => n + s.coveredBranches, 0);
  const total = sites.reduce((n, s) => n + s.coveredBranches + s.missed, 0);
  const uncovered = sites
    .filter((s) => s.missed > 0)
    .sort((x, y) => x.file.localeCompare(y.file) || x.line - y.line);
  // `sitesAll` is the full per-site state the self-test compares; `excluded`
  // is the removed timing-dependent surface (#1707), kept for audit.
  return { unit: 'branch', covered, total, percent: pct(covered, total), uncovered, sitesAll: sites, excluded };
}

/**
 * Load and validate the checked-in zeebe exclusion list (zeebe-excludes.json,
 * #1707). Every entry must carry a `match` and the `issue` that justifies
 * removing that surface from the measured set — an unexplained exclusion is a
 * silent coverage drop, exactly the drift surface the ratchet exists to catch.
 * Returns `{ packages: [...match strings], lines: [...match strings] }`.
 */
export function loadZeebeExcludes(spec) {
  const errors = [];
  const packages = [];
  const lines = [];
  const seen = new Set();
  for (const [kind, list] of [
    ['packages', spec.packages ?? []],
    ['lines', spec.lines ?? []],
  ]) {
    if (!Array.isArray(list)) {
      errors.push(`zeebe-excludes: "${kind}" must be an array`);
      continue;
    }
    for (const [i, e] of list.entries()) {
      const where = `${kind}[${i}]`;
      if (typeof e?.match !== 'string' || e.match.length === 0) {
        errors.push(`zeebe-excludes: ${where} has no "match"`);
        continue;
      }
      if (typeof e?.issue !== 'number') errors.push(`zeebe-excludes: ${where} (${e.match}) names no justifying "issue"`);
      if (seen.has(e.match)) errors.push(`zeebe-excludes: duplicate exclusion "${e.match}"`);
      seen.add(e.match);
      (kind === 'packages' ? packages : lines).push(e.match);
    }
  }
  if (errors.length) throw new Error(errors.join('\n'));
  return { packages, lines };
}

/**
 * Gate self-test (#1707): re-running the measurement on identical inputs must
 * give an identical result. Re-parses each measured input `runs` times and
 * compares the full per-site signature (every branch line's missed/covered
 * counters), failing loudly with the differing branches when any re-parse
 * disagrees — a nondeterministic measurement must fail the gate, never flap
 * it. Returns a list of human-readable differences (empty = deterministic).
 */
export function selfTest(measureFns, { runs = 3 } = {}) {
  const errors = [];
  for (const [side, fn] of Object.entries(measureFns)) {
    const first = signature(fn());
    for (let i = 2; i <= runs; i++) {
      const again = fn();
      const diff = diffSignatures(first, signature(again));
      if (diff.length) {
        errors.push(
          `${side}: self-test run ${i} differs from run 1 on identical input — the measurement is nondeterministic:\n` +
            diff.map((d) => `  ${d}`).join('\n'),
        );
      }
    }
  }
  return errors;
}

/** The full comparable state of one measurement: counts + every branch site. */
function signature(m) {
  const sites = new Map();
  for (const s of m.sitesAll ?? []) sites.set(`${s.file}:${s.line}`, `${s.missed}/${s.coveredBranches}`);
  return { covered: m.covered, total: m.total, sites };
}

function diffSignatures(a, b) {
  const diff = [];
  if (a.covered !== b.covered || a.total !== b.total) {
    diff.push(`covered/total: ${a.covered}/${a.total} vs ${b.covered}/${b.total}`);
  }
  for (const [k, v] of a.sites) {
    const w = b.sites.get(k);
    if (w === undefined) diff.push(`${k}: present in run 1 (${v}), absent later`);
    else if (w !== v) diff.push(`${k}: ${v} vs ${w}`);
  }
  for (const [k, v] of b.sites) if (!a.sites.has(k)) diff.push(`${k}: absent in run 1, present later (${v})`);
  return diff;
}

/**
 * Tag each uncovered Zeebe line against the coverage matrix: the cells sourced
 * at exactly that line (`scope: "line"`), else the cells sourced in the same
 * file (`scope: "file"`), each with its matrix status. Every uncovered Zeebe
 * branch is a missing scenario.
 */
export function tagZeebe(uncovered, surface, rules) {
  const byLine = new Map();
  const byFile = new Map();
  for (const cell of surface.cells) {
    for (const src of cell.sources ?? []) {
      const i = src.lastIndexOf(':');
      const file = src.slice(0, i);
      const push = (m, k) => (m.get(k) ?? m.set(k, new Set()).get(k)).add(cell.id);
      push(byLine, src);
      push(byFile, file);
    }
  }
  const status = (id) => firstMatch(rules, id)?.status ?? 'unmapped';
  return uncovered.map((s) => {
    const line = byLine.get(`${s.file}:${s.line}`);
    const set = line ?? byFile.get(s.file);
    const cells = [...(set ?? [])].sort().map((id) => ({ id, status: status(id) }));
    return { ...s, tag: 'missing-scenario', scope: line ? 'line' : set ? 'file' : 'none', cells };
  });
}

// ----------------------------------------------------------------- ratchet --

/** Ratchet violations of `measured` (`{nano?, zeebe?}`) against `baseline`. */
export function checkRatchet(measured, baseline) {
  const errors = [];
  for (const side of ['nano', 'zeebe']) {
    const m = measured[side];
    const b = baseline[side];
    if (!m || !b) continue;
    if (m.unit !== b.unit) {
      errors.push(`${side}: measured in ${m.unit}s but the baseline floor is in ${b.unit}s — re-baseline explicitly (--update --allow-lower)`);
      continue;
    }
    // The floor is a hard ratchet on the DETERMINISTIC side; a side measured
    // against a live broker (zeebe JaCoCo) carries irreducible
    // broker-scheduling variance (#1679), so its `slackPercent` is the
    // documented absorption band: the effective floor is `floorPercent −
    // slackPercent`. A drop beyond the band is a real coverage loss and fails.
    const slackPercent = b.slackPercent ?? 0;
    const effectiveFloor = floor1(b.floorPercent - slackPercent);
    if (m.percent < effectiveFloor) {
      errors.push(
        `${side}: ${m.unit} coverage ${m.percent.toFixed(2)}% dropped below the ratchet floor ${b.floorPercent}%` +
          (slackPercent ? ` (effective ${effectiveFloor}% after the ${slackPercent}% scheduling-variance slack, #1679)` : '') +
          ` (${m.covered}/${m.total}) — add a parity scenario for the uncovered code, or register a Nano-only extension`,
      );
    }
  }
  return errors;
}

/** The baseline after `--update`: floors only rise unless `allowLower`. */
export function updateBaseline(measured, baseline, { allowLower = false } = {}) {
  const next = { ...baseline };
  const errors = [];
  for (const side of ['nano', 'zeebe']) {
    const m = measured[side];
    if (!m) continue;
    const b = baseline[side];
    // The slack is applied once, at CHECK time (checkRatchet); seeding the
    // floor from `measured − slack` as well would widen the band to 2×slack.
    const floorPercent = floor1(m.percent);
    if (b && !allowLower && (b.unit !== m.unit || floorPercent < b.floorPercent)) {
      errors.push(`${side}: refusing to lower the floor ${b.floorPercent}% (${b.unit}) to ${floorPercent}% (${m.unit}) without --allow-lower`);
      continue;
    }
    next[side] = { ...(b ?? {}), unit: m.unit, floorPercent };
  }
  return { next, errors };
}

// -------------------------------------------------------------------- main --

export function markdown(measured, baseline) {
  const rows = ['nano', 'zeebe'].map((side) => {
    const m = measured[side];
    const b = baseline[side];
    const floor = b ? `${b.floorPercent}% (${b.unit})` : '—';
    if (!m) return `| ${side} | not measured | — | ${floor} |`;
    const tags = {};
    for (const s of m.uncovered) tags[s.tag] = (tags[s.tag] ?? 0) + 1;
    const t = Object.entries(tags).map(([k, v]) => `${k}: ${v}`).join(', ') || '—';
    return `| ${side} | ${m.percent.toFixed(2)}% of ${m.unit === 'branch' ? 'branches' : 'regions'} (${m.covered}/${m.total}) | ${t} | ${floor} |`;
  });
  return [
    '### Parity corpus coverage (#1247)',
    '',
    '| engine | covered | uncovered by tag | ratchet floor |',
    '|---|---|---|---|',
    ...rows,
    '',
  ].join('\n');
}

function parseArgs(argv) {
  const a = { nano: null, zeebe: null, out: null, check: false, update: false, allowLower: false, selfTest: 0, baseline: join(HERE, 'baseline.json') };
  for (let i = 0; i < argv.length; i++) {
    const x = argv[i];
    if (x === '--nano') a.nano = argv[++i];
    else if (x === '--zeebe') a.zeebe = argv[++i];
    else if (x === '--out') a.out = argv[++i];
    else if (x === '--baseline') a.baseline = argv[++i];
    else if (x === '--check') a.check = true;
    else if (x === '--update') a.update = true;
    else if (x === '--allow-lower') a.allowLower = true;
    else if (x === '--self-test') a.selfTest = Number(argv[++i] ?? '3');
    else throw new Error(`unknown argument ${x}`);
  }
  if (!a.nano && !a.zeebe) throw new Error('pass --nano and/or --zeebe');
  if (a.selfTest && (!Number.isInteger(a.selfTest) || a.selfTest < 2)) throw new Error('--self-test needs an integer ≥ 2');
  return a;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const readJson = (p) => JSON.parse(readFileSync(p, 'utf8'));
  const errors = [];
  // The measurement fns behind the self-test: re-parse the identical input.
  const measure = {};
  if (args.nano) {
    const raw = readFileSync(args.nano, 'utf8');
    measure.nano = () => parseLlvmCov(JSON.parse(raw));
  }
  if (args.zeebe) {
    const raw = readFileSync(args.zeebe, 'utf8');
    const exclude = loadZeebeExcludes(readJson(join(HERE, 'zeebe-excludes.json')));
    measure.zeebe = () => parseJacoco(raw, { exclude });
  }
  // Gate self-test (#1707): identical inputs must give an identical covered
  // count — fail loudly with the differing branches when they do not.
  if (args.selfTest) errors.push(...selfTest(measure, { runs: args.selfTest }));

  const measured = {};
  if (args.nano) {
    const m = measure.nano();
    m.uncovered = tagNano(m.uncovered, readJson(join(HERE, 'nano-extensions.json')).extensions);
    measured.nano = m;
  }
  if (args.zeebe) {
    const m = measure.zeebe();
    const surface = readJson(join(HERE, '..', 'zeebe-surface.json'));
    const rules = readJson(join(HERE, '..', 'coverage.json')).rules;
    m.uncovered = tagZeebe(m.uncovered, surface, rules);
    measured.zeebe = m;
  }
  const baseline = readJson(args.baseline);
  if (args.out) writeFileSync(args.out, JSON.stringify(measured, null, 1) + '\n');
  const md = markdown(measured, baseline);
  process.stdout.write(md);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, md);

  errors.push(...emptyMeasurementErrors(measured));
  if (args.update) {
    const { next, errors: e } = updateBaseline(measured, baseline, { allowLower: args.allowLower });
    errors.push(...e);
    if (errors.length === 0) writeFileSync(args.baseline, JSON.stringify(next, null, 2) + '\n');
  }
  if (args.check) errors.push(...checkRatchet(measured, baseline));
  if (errors.length) {
    for (const e of errors) console.error(`coverage: ${e}`);
    process.exit(1);
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) main();
