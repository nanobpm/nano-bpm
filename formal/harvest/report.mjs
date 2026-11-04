#!/usr/bin/env node
// The harvest report (#1249): harvested / runnable / unverified / out-of-scope
// counts for the Zeebe engine-test corpus, plus the per-model detail.
//
//   node formal/harvest/report.mjs [--corpus formal/harvest/corpus] [--out formal/harvest/report.json]
//
// Reads the pipeline's intermediate artifacts (harvest-summary, normalised,
// out-of-scope, run-results) and emits ONE report.json — the acceptance
// artifact. The counts are:
//
//   harvested    — models the harvester extracted AND serialised through the
//                  Zeebe model API (a faithful reproduction of the test's model);
//   runnable     — harvested models Nano deployed, ran, and completed cleanly
//                  under the deterministic empty-variable drive;
//   unverified   — harvested models Nano accepted and ran but which did NOT
//                  complete cleanly (no completion, incident, stranded
//                  element/job). This slice harvests NO independent Zeebe
//                  oracle — the engine tests assert on Zeebe's record stream,
//                  which the static harvest does not reproduce — so a non-clean
//                  Nano outcome is UNVERIFIED: it may be a genuine Nano gap, the
//                  expected Zeebe outcome, or a driver limitation. These counts
//                  are never presented as parity divergences;
//   out-of-scope — models not runnable under this slice: unparseable builder
//                  chains (lambda/dynamic surface), models Nano rejects
//                  (unimplemented surface), and scenarios needing a driver
//                  capability (variables/messages/timers/call-activity) the
//                  deterministic runner does not supply.
//
// The report is a derived artifact, committed, and drift-guarded in CI.

import { appendFileSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const PIN = JSON.parse(readFileSync(join(here, 'zeebe-test-pin.json'), 'utf8'));

export function buildReport(corpusDir) {
  const read = (name, fallback) =>
    existsSync(join(corpusDir, name))
      ? JSON.parse(readFileSync(join(corpusDir, name), 'utf8'))
      : fallback;

  const summary = read('harvest-summary.json', {});
  const normalised = read('normalised.json', []);
  const outOfScopeNorm = read('out-of-scope.json', []);
  const unparseable = read('unparseable.json', []);
  const runResults = read('run-results.json', []);

  const runnable = runResults.filter((r) => r.status === 'runnable');
  const unverified = runResults.filter((r) => r.status === 'unverified');
  const runOutOfScope = runResults.filter((r) => r.status === 'out-of-scope');

  const report = {
    generator: 'formal/harvest/report.mjs',
    issue: 1249,
    zeebe: {
      repository: PIN.repository,
      ref: PIN.ref,
      sha: PIN.sha,
      modelVersion: PIN.modelVersion,
    },
    counts: {
      scannedFiles: summary.scannedFiles ?? 0,
      harvested: (summary.harvested ?? 0),
      runnable: runnable.length,
      unverified: unverified.length,
      // out-of-scope = unparseable chains + normalise-failures + run-time
      // out-of-scope (nano-rejected or driver-capability).
      outOfScope: unparseable.length + outOfScopeNorm.length + runOutOfScope.length,
    },
    // Non-clean Nano runs WITHOUT an independent Zeebe oracle: each may be a
    // genuine Nano gap, the expected Zeebe outcome, or a driver limitation.
    // Investigate before treating any as a parity issue.
    unverified: unverified.map((r) => ({ id: r.id, processId: r.processId, detail: r.detail })),
    outOfScopeBreakdown: {
      unparseable: unparseable.length,
      normalise: outOfScopeNorm.length,
      run: runOutOfScope.length,
    },
    runnableIds: runnable.map((r) => r.id),
  };
  return report;
}

function main() {
  const args = process.argv.slice(2);
  const cIdx = args.indexOf('--corpus');
  const corpusDir = cIdx >= 0 ? args[cIdx + 1] : join(here, 'corpus');
  const oIdx = args.indexOf('--out');
  const outFile = oIdx >= 0 ? args[oIdx + 1] : join(here, 'report.json');

  const report = buildReport(corpusDir);
  writeFileSync(outFile, JSON.stringify(report, null, 2) + '\n');

  // Human-readable summary (and the CI step summary).
  const c = report.counts;
  const lines = [
    `# Zeebe engine-test harvest report (#1249)`,
    ``,
    `Pinned Zeebe: \`${PIN.ref}\` @ \`${PIN.sha.slice(0, 12)}\` (model API ${PIN.modelVersion})`,
    ``,
    `| Metric | Count |`,
    `|---|---|`,
    `| Test files scanned | ${c.scannedFiles} |`,
    `| **Harvested** (extracted + Zeebe-serialised) | ${c.harvested} |`,
    `| **Runnable** (Nano completes cleanly) | ${c.runnable} |`,
    `| **Unverified** (Nano run not clean; no Zeebe oracle) | ${c.unverified} |`,
    `| **Out of scope** (unparseable / rejected / driver) | ${c.outOfScope} |`,
  ];
  if (report.unverified.length) {
    lines.push(``, `## Unverified (non-clean Nano runs — no Zeebe oracle, investigate before treating as parity issues)`, ``);
    for (const d of report.unverified) lines.push(`- \`${d.processId}\` — ${d.detail}`);
  }
  const text = lines.join('\n') + '\n';
  console.log(text);
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, text);
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main();
}
