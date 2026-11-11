// Guard (#1546): an Ubuntu apt mirror outage must not fail required CI jobs.
// A ~1h `Temporary failure resolving 'azure.archive.ubuntu.com'` failed every
// required Rust job at `apt-get install mold` before any code compiled. Native
// build tools come ONLY through the composite action .github/actions/build-tools
// (pinned, sha256-verified release downloads with retries; apt as a retried
// fallback). No workflow or other action may call apt/apt-get directly.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const ALLOWED = join('.github', 'actions', 'build-tools');

/** Non-comment lines invoking apt / apt-get, directly or hidden behind
 * Playwright's `--with-deps` / `install-deps` (which shell out to apt-get). */
export function bareApt(text) {
  return text
    .split('\n')
    .map((line, i) => ({ code: line.replace(/#.*$/, ''), n: i + 1 }))
    .filter(({ code }) => /\bapt(-get)?(\s+-{1,2}[\w-]+(=\S+)?(\s+(?!update\b|install\b|upgrade\b|dist-upgrade\b|full-upgrade\b)[^\s-]\S*)?)*\s+(update|install|upgrade|dist-upgrade|full-upgrade)\b/.test(code) || /\bplaywright\s+install(-deps\b|\b.*--with-deps\b)/.test(code))
    .map(({ n }) => `line ${n}`);
}

function walk(dir) {
  return readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    return statSync(p).isDirectory() ? walk(p) : [p];
  });
}

test('bareApt detects direct apt-get calls and ignores comments', () => {
  assert.deepEqual(bareApt('run: sudo apt-get update && sudo apt-get install -y mold'), ['line 1']);
  assert.deepEqual(bareApt('x\n  sudo apt install -y foo'), ['line 2']);
  // Global options before the subcommand must not hide the call.
  assert.deepEqual(bareApt('sudo apt-get -y install mold'), ['line 1']);
  assert.deepEqual(bareApt('sudo apt-get -o Acquire::Retries=5 install mold'), ['line 1']);
  assert.deepEqual(bareApt('sudo apt -qq update'), ['line 1']);
  assert.deepEqual(bareApt('sudo apt-get --quiet=2 -o A=b upgrade'), ['line 1']);
  // Playwright's system-dependency install is apt in disguise.
  assert.deepEqual(bareApt('run: npx playwright install --with-deps chromium'), ['line 1']);
  assert.deepEqual(bareApt('run: npx playwright install-deps'), ['line 1']);
  assert.deepEqual(bareApt('run: npx playwright install chromium'), []);
  assert.deepEqual(bareApt('# sudo apt-get install mold\nuses: ./.github/actions/build-tools'), []);
});

test('no workflow or action calls apt except .github/actions/build-tools', () => {
  const offenders = walk(join(root, '.github'))
    .filter((p) => /\.(ya?ml|sh)$/.test(p))
    .map((p) => relative(root, p))
    .filter((p) => !p.startsWith(ALLOWED))
    .flatMap((p) => bareApt(readFileSync(join(root, p), 'utf8')).map((l) => `${p} ${l}`));
  assert.deepEqual(offenders, []);
});

test('build-tools pins and verifies its release downloads', () => {
  const sh = readFileSync(join(root, ALLOWED, 'install.sh'), 'utf8');
  assert.match(sh, /MOLD_SHA256=[0-9a-f]{64}/);
  assert.match(sh, /BINARYEN_SHA256=[0-9a-f]{64}/);
  assert.match(sh, /sha256sum -c/);
  assert.match(sh, /Acquire::Retries/);
  assert.match(sh, /APT::Update::Error-Mode=any/);
  const yml = readFileSync(join(root, ALLOWED, 'action.yml'), 'utf8');
  assert.match(yml, /uses: actions\/cache@/, 'release archives must be cached (#1461)');
});
