// Guard (#1907): GitHub Actions' GITHUB_TOKEN is not permitted to create pull
// requests in this repo ("GitHub Actions is not permitted to create or approve
// pull requests (createPullRequest)"). The v0.12.0 release's allowlist-prune
// job pushed its branch, then failed at `gh pr create`, turning the release run
// red and silently dropping the prune. Every workflow step that runs
// `gh pr create` must therefore set its own step-level GH_TOKEN to a non-
// GITHUB_TOKEN credential (a GitHub App token from
// actions/create-github-app-token, or a dedicated cross-repo secret).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

const indentOf = (line) => line.match(/^ */)[0].length;
const isDefault = (v) => /github\.token|secrets\.GITHUB_TOKEN|\$GITHUB_TOKEN\b/.test(v);

/** Offending `gh pr create` lines: the enclosing step has no step-level
 * GH_TOKEN, or it may resolve to GITHUB_TOKEN (including as a `||` fallback). */
export function prCreateOffenders(text) {
  const lines = text.split('\n');
  const out = [];
  lines.forEach((line, i) => {
    if (!/\bgh\s+pr\s+create\b/.test(line.replace(/#.*$/, ''))) return;
    // Enclosing step: nearest preceding `- ` list item less indented than this line.
    let start = -1;
    for (let j = i; j >= 0; j--) {
      const m = lines[j].match(/^( *)- /);
      if (m && m[1].length < indentOf(line)) { start = j; break; }
    }
    if (start < 0) { out.push(`line ${i + 1} (no enclosing step)`); return; }
    const stepIndent = indentOf(lines[start]);
    let end = lines.length;
    for (let j = start + 1; j < lines.length; j++) {
      if (lines[j].trim() !== '' && indentOf(lines[j]) <= stepIndent) { end = j; break; }
    }
    const tok = lines.slice(start, end).map((l) => l.match(/^\s*GH_TOKEN:\s*(.+)$/)).find(Boolean);
    if (!tok) out.push(`line ${i + 1} (step sets no GH_TOKEN)`);
    else if (isDefault(tok[1])) out.push(`line ${i + 1} (GH_TOKEN is GITHUB_TOKEN)`);
  });
  return out;
}

function walk(dir) {
  return readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    return statSync(p).isDirectory() ? walk(p) : [p];
  });
}

const step = (token, cmd = 'gh pr create --base main') =>
  `jobs:\n  j:\n    steps:\n      - name: x\n        env:\n${token === null ? '' : `          GH_TOKEN: ${token}\n`}        run: |\n          ${cmd}\n      - name: next\n        env:\n          GH_TOKEN: \${{ steps.app-token.outputs.token }}\n        run: echo`;

test('prCreateOffenders flags GITHUB_TOKEN and missing tokens', () => {
  assert.deepEqual(prCreateOffenders(step('${{ github.token }}')).length, 1);
  assert.deepEqual(prCreateOffenders(step('${{ secrets.GITHUB_TOKEN }}')).length, 1);
  assert.deepEqual(prCreateOffenders(step('${{ secrets.X || secrets.GITHUB_TOKEN }}')).length, 1);
  // A later step's App token must not mask a missing token in this one.
  assert.deepEqual(prCreateOffenders(step(null)).length, 1);
  assert.deepEqual(prCreateOffenders(step('${{ steps.app-token.outputs.token }}')), []);
  assert.deepEqual(prCreateOffenders(step('${{ secrets.C8CTL_PLUGIN_REPO_TOKEN }}')), []);
  assert.deepEqual(prCreateOffenders(step(null, '# gh pr create in a comment')), []);
});

test('no workflow or action runs `gh pr create` with GITHUB_TOKEN', () => {
  const offenders = walk(join(root, '.github'))
    .filter((p) => /\.ya?ml$/.test(p))
    .flatMap((p) => prCreateOffenders(readFileSync(p, 'utf8')).map((l) => `${relative(root, p)} ${l}`));
  assert.deepEqual(offenders, []);
});
