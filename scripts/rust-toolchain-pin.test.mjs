// Guard: every CI/release workflow installs the ONE Rust toolchain pinned in
// rust-toolchain.toml. An unpinned `dtolnay/rust-toolchain@stable` let Rust
// 1.99.0 land unannounced and break clippy (double_must_use, #1313) and the
// zig armv6 release link (`__aeabi_uread4`, v0.0.26) on the same day.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

/** The pinned channel from the single source of truth. */
export function pinnedChannel(toml) {
  const m = toml.match(/^\s*channel\s*=\s*"([^"]+)"/m);
  assert.ok(m, 'rust-toolchain.toml must declare a channel');
  return m[1];
}

/**
 * Every `dtolnay/rust-toolchain@<ref>` use whose ref is not the pin. The only
 * allowed exception is `@master` with an explicit pinned `nightly-YYYY-MM-DD`
 * toolchain input (the rustfmt job, pinned via FMT_TOOLCHAIN).
 */
export function drift(yaml, channel) {
  const lines = yaml.split('\n');
  const bad = [];
  lines.forEach((line, i) => {
    const m = line.match(/dtolnay\/rust-toolchain@([^\s#]+)/);
    if (!m) return;
    // The step's explicit `toolchain:` input overrides the action ref, so it
    // must be inspected too. The step spans the following lines that are
    // indented deeper than its `- ` list marker.
    const indent = line.search(/\S/);
    const step = [];
    for (let j = i + 1; j < lines.length; j++) {
      const l = lines[j];
      if (l.trim() === '') continue;
      if (l.search(/\S/) <= indent) break;
      step.push(l);
    }
    const input = step.join('\n').match(/^\s*toolchain:\s*['"]?([^'"\s#]+)/m)?.[1];
    if (m[1] === channel) {
      if (input === undefined || input === channel) return;
      bad.push(`line ${i + 1}: @${m[1]} overridden by toolchain: ${input}`);
      return;
    }
    if (m[1] === 'master' && input && /^nightly-\d{4}-\d{2}-\d{2}$/.test(input)) return;
    bad.push(`line ${i + 1}: @${m[1]}`);
  });
  return bad;
}

test('pinned channel is an exact stable version, not a moving channel', () => {
  const channel = pinnedChannel(readFileSync(join(root, 'rust-toolchain.toml'), 'utf8'));
  assert.match(channel, /^\d+\.\d+\.\d+$/);
});

test('every workflow installs the pinned toolchain', () => {
  const channel = pinnedChannel(readFileSync(join(root, 'rust-toolchain.toml'), 'utf8'));
  const dir = join(root, '.github', 'workflows');
  const offenders = readdirSync(dir)
    .filter((f) => /\.ya?ml$/.test(f))
    .flatMap((f) => drift(readFileSync(join(dir, f), 'utf8'), channel).map((d) => `${f} ${d}`));
  assert.deepEqual(offenders, [], `workflows must use dtolnay/rust-toolchain@${channel}`);
});

test('drift detector rejects @stable and unpinned @master, allows a pinned nightly', () => {
  assert.deepEqual(drift('  - uses: dtolnay/rust-toolchain@stable', '1.98.1'), ['line 1: @stable']);
  assert.deepEqual(drift('  - uses: dtolnay/rust-toolchain@master\n    with:\n      toolchain: stable', '1.98.1'), ['line 1: @master']);
  assert.deepEqual(drift('  - uses: dtolnay/rust-toolchain@master\n    with:\n      toolchain: nightly-2026-06-26', '1.98.1'), []);
  assert.deepEqual(drift('  - uses: dtolnay/rust-toolchain@1.98.1', '1.98.1'), []);
  assert.deepEqual(
    drift('  - uses: dtolnay/rust-toolchain@1.98.1\n    with:\n      targets: wasm32-unknown-unknown', '1.98.1'),
    [],
  );
  // An explicit input overrides the pinned ref (#1318 review).
  assert.deepEqual(
    drift('  - uses: dtolnay/rust-toolchain@1.98.1\n    with:\n      toolchain: stable', '1.98.1'),
    ['line 1: @1.98.1 overridden by toolchain: stable'],
  );
  // The next step's input is not attributed to this one.
  assert.deepEqual(
    drift('  - uses: dtolnay/rust-toolchain@1.98.1\n  - uses: other@v1\n    with:\n      toolchain: stable', '1.98.1'),
    [],
  );
});
