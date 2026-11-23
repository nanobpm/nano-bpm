// Guard (#1830): nothing is published from a ref that is not a release tag.
//
// A manual `workflow_dispatch` run of publish-c8ctl-binaries.yml on the branch
// `chore/rust-toolchain-upgrade` overwrote the plugin's bundled server and
// pinned nanobpmn-binary.json to that branch, so c8ctl-plugin-nano 1.70.x to
// 1.75.x shipped an unreleased side-branch build for seven weeks (#1827).
//
// The rule, for every job in every workflow that publishes (npm, GitHub release
// assets, S3, Maven, a credentialed write to another repo, OIDC trusted
// publishing, or a release script):
//   1. a job-level `if:` admits tag refs only, and
//   2. it runs .github/actions/assert-release-ref, which fails closed unless the
//      tag matches the workflow's pattern and its commit is on `main`.
// Manual runs of a release workflow still build; they cannot publish.
//
// Run: node --test scripts/release-ref-gate.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const WORKFLOWS = join(root, '.github', 'workflows');

/** Jobs that publish but are not versioned releases. Each needs a reason. */
const EXEMPT = {
  'pages.yml:deploy':
    'deploys the website from main (push to main / schedule); the github-pages environment, not a release tag, gates it',
};

const PUBLISH_SIGNALS = [
  /\bnpm publish\b/,
  /\bgh release (upload|create|edit)\b/,
  /\baws s3 (cp|sync|mv)\b/,
  /\bmvn\b.*\bdeploy\b/,
  /\bcargo publish\b/,
  /x-access-token:/, // credentialed clone of another repo, to push to it
  /^\s*id-token:\s*write\b/, // OIDC trusted publishing (npm, Maven, Pages)
  /\bscripts\/[\w-]*release[\w-]*\.(mjs|js|sh|py)\b/,
];
const TAG_IF = /github\.ref_type\s*==\s*'tag'|startsWith\(\s*github\.ref\s*,\s*'refs\/tags\//;
const ASSERT_STEP = /^\s*(-\s+)?uses:\s*\.\/\.github\/actions\/assert-release-ref\s*$/;

/** Split a workflow into its jobs: { id, lines } with comments stripped. */
export function jobsOf(text) {
  const lines = text.split('\n');
  const start = lines.findIndex((l) => /^jobs:\s*$/.test(l));
  if (start < 0) return [];
  const jobs = [];
  for (const raw of lines.slice(start + 1)) {
    if (/^\S/.test(raw)) break; // next top-level key
    const line = raw.replace(/(^|\s)#.*$/, '');
    const head = /^  ([A-Za-z0-9_-]+):\s*$/.exec(line);
    if (head) jobs.push({ id: head[1], lines: [] });
    else if (jobs.length) jobs[jobs.length - 1].lines.push(line);
  }
  return jobs;
}

/** The job-level `if:` expression, including folded continuation lines. */
export function jobIf(job) {
  const i = job.lines.findIndex((l) => /^    if:/.test(l));
  if (i < 0) return '';
  const out = [job.lines[i]];
  for (const l of job.lines.slice(i + 1)) {
    if (!/^      /.test(l) || /^      \S+:/.test(l)) break;
    out.push(l);
  }
  return out.join(' ');
}

/** Why `job` violates the rule (empty when it publishes safely or not at all). */
export function violations(job) {
  if (!job.lines.some((l) => PUBLISH_SIGNALS.some((re) => re.test(l)))) return [];
  const why = [];
  if (!TAG_IF.test(jobIf(job))) why.push('publishes without a job-level tag-only `if:`');
  if (!job.lines.some((l) => ASSERT_STEP.test(l)))
    why.push('publishes without the ./.github/actions/assert-release-ref step');
  return why;
}

test('jobsOf / violations: an ungated publishing job is flagged, a gated one is not', () => {
  const wf = [
    'on:',
    '  workflow_dispatch:',
    'jobs:',
    '  build:',
    '    runs-on: ubuntu-24.04',
    '    steps:',
    '      - run: make dist  # npm publish happens later',
    '  publish:',
    '    needs: build',
    '    steps:',
    '      - run: npm publish --access public',
    '  gated:',
    "    if: github.ref_type == 'tag'",
    '    steps:',
    '      - uses: ./.github/actions/assert-release-ref',
    '      - run: gh release upload v1 dist/*',
    '  oidc:',
    '    if: >-',
    "      startsWith(github.ref, 'refs/tags/x-v')",
    '    permissions:',
    '      id-token: write',
    '    steps:',
    '      - run: node scripts/x-release.mjs',
  ].join('\n');
  const byId = Object.fromEntries(jobsOf(wf).map((j) => [j.id, violations(j)]));
  assert.deepEqual(byId.build, [], 'a comment mentioning publish is not a publish');
  assert.equal(byId.publish.length, 2);
  assert.deepEqual(byId.gated, []);
  assert.deepEqual(byId.oidc, ['publishes without the ./.github/actions/assert-release-ref step']);
});

test('every publishing job in every workflow is gated on a main-reachable release tag', () => {
  const offenders = [];
  const seen = new Set();
  for (const file of readdirSync(WORKFLOWS).filter((f) => /\.ya?ml$/.test(f))) {
    for (const job of jobsOf(readFileSync(join(WORKFLOWS, file), 'utf8'))) {
      const key = `${file}:${job.id}`;
      seen.add(key);
      if (key in EXEMPT) continue;
      for (const why of violations(job)) offenders.push(`${key} ${why}`);
    }
  }
  assert.deepEqual(offenders, []);
  // An exemption for a job that no longer exists is a stale hole: drop it.
  assert.deepEqual(Object.keys(EXEMPT).filter((k) => !seen.has(k)), []);
});

test('assert-release-ref fails closed: tag ref, tag pattern, and reachable from main', () => {
  const action = readFileSync(join(root, '.github', 'actions', 'assert-release-ref', 'action.yml'), 'utf8');
  assert.match(action, /GITHUB_REF_TYPE/);
  assert.match(action, /tag-pattern/);
  assert.match(action, /compare\/.*main/);
  assert.match(action, /behind|identical/);
});
