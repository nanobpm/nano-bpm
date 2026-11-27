// Defect-class guard (#1770): a CI fetch of a pinned, immutable artifact must
// be resilient to a transient upstream error (Maven Central once 404'd for a
// minute and failed the zeebe-coverage job). Every `curl … -o <file>` download
// in the workflows and CI scripts must retry on ALL errors (plain `--retry`
// does not retry an HTTP 404), and the JaCoCo jars must be SHA-256 pinned.
// fetch-pinned.sh is the one exception: it runs its own status-aware backoff
// loop (Retry-After, 429/5xx, mirrors — #2079), pinned by fetch-pinned.test.mjs.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { basename, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = fileURLToPath(new URL('../../../', import.meta.url));
const roots = ['.github', 'formal', 'scripts'];
const skip = new Set(['node_modules', 'target', '.lake']);

function* walk(dir) {
  for (const name of readdirSync(dir)) {
    if (skip.has(name)) continue;
    const p = join(dir, name);
    if (statSync(p).isDirectory()) yield* walk(p);
    else if (/\.(sh|ya?ml)$/.test(name)) yield p;
  }
}

test('every curl file download retries on all errors', () => {
  const offenders = [];
  for (const root of roots) {
    for (const file of walk(join(repo, root))) {
      if (basename(file) === 'fetch-pinned.sh') continue;
      // Join backslash continuations so a multi-line curl is one command.
      const text = readFileSync(file, 'utf8').replace(/\\\n/g, ' ');
      text.split('\n').forEach((line, i) => {
        if (/^\s*#/.test(line)) return;
        const m = /\bcurl\s[^|;&]*/.exec(line);
        if (!m || !/\s-o\s|\s-[a-zA-Z]*o\s|--output\s/.test(m[0])) return;
        if (!/--retry-all-errors/.test(m[0])) offenders.push(`${relative(repo, file)}:${i + 1}: ${m[0].trim()}`);
      });
    }
  }
  assert.deepEqual(offenders, [], 'curl downloads without --retry-all-errors');
});

test('JaCoCo jars are SHA-256 pinned and verified', () => {
  const s = readFileSync(join(repo, 'formal/parity/coverage/zeebe-coverage.sh'), 'utf8');
  assert.match(s, /^JACOCO_VERSION="[0-9.]+"$/m);
  assert.match(s, /^JACOCO_AGENT_SHA256="[0-9a-f]{64}"$/m);
  assert.match(s, /^JACOCO_CLI_SHA256="[0-9a-f]{64}"$/m);
  assert.doesNotMatch(s, /\bcurl\b/, 'jars must go through fetch_pinned (fetch-pinned.sh), never a bare curl');
  assert.match(s, /source "\$here\/fetch-pinned\.sh"/);
});
