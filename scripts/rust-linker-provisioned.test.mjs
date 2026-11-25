// Guard (#1964): .cargo/config.toml forces `-C link-arg=-fuse-ld=mold` on
// x86_64-unknown-linux-gnu, so EVERY Linux CI job that builds Rust must
// provision mold (`.github/actions/build-tools` with `mold: 'true'`). Without it
// the first link dies with `collect2: fatal error: cannot find 'ld'` — exactly
// how zeebe-fuzz-nightly.yml crashed ("nano runner failed") on every run while
// the jobs in ci.yml, which install mold, stayed green.
//
// Defect class: a Linux job installs a Rust toolchain but not the linker the
// repo-wide config requires. A job that genuinely never links on the host
// (rustfmt only, cross builds with their own linker) opts out explicitly with a
// `# linker: <reason>` comment inside the job, so the exemption is reviewable.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { relative, join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ciYamlFiles } from './rust-toolchain-pin.test.mjs';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

/** Split a workflow into its jobs: `[{ id, line, text }]`. */
export function jobs(yaml) {
  const lines = yaml.split('\n');
  const start = lines.findIndex((l) => /^jobs:\s*(#.*)?$/.test(l));
  if (start < 0) return [];
  const out = [];
  for (let i = start + 1; i < lines.length; i++) {
    const l = lines[i];
    if (/^\S/.test(l)) break;
    const m = l.match(/^ {2}([A-Za-z0-9_-]+):\s*(#.*)?$/);
    if (m) out.push({ id: m[1], line: i + 1, body: [] });
    else if (out.length) out[out.length - 1].body.push(l);
  }
  return out.map((j) => ({ id: j.id, line: j.line, text: j.body.join('\n') }));
}

/** Jobs that install Rust on a Linux runner without provisioning mold. */
export function unprovisioned(yaml) {
  return jobs(yaml)
    .filter((j) => /dtolnay\/rust-toolchain@/.test(j.text))
    .filter((j) => /runs-on:[^\n]*ubuntu/.test(j.text) || /runs-on:\s*\$\{\{/.test(j.text) && /ubuntu/.test(j.text))
    .filter((j) => !/^\s*mold:\s*['"]?true['"]?/m.test(j.text))
    .filter((j) => !/#\s*linker:\s*\S/.test(j.text))
    .map((j) => `job ${j.id} (line ${j.line})`);
}

test('detects a Linux Rust job without mold, and accepts provisioned or opted-out ones', () => {
  const job = (extra) => `on: push\njobs:\n  a:\n    runs-on: ubuntu-24.04\n    steps:\n      - uses: dtolnay/rust-toolchain@1.99.0\n${extra}`;
  assert.deepEqual(unprovisioned(job('      - run: cargo test\n')), ['job a (line 3)']);
  assert.deepEqual(unprovisioned(job("      - uses: ./.github/actions/build-tools\n        with:\n          mold: 'true'\n")), []);
  assert.deepEqual(unprovisioned(job('      # linker: rustfmt only, never links\n')), []);
  assert.deepEqual(unprovisioned(job('').replace('ubuntu-24.04', 'macos-14')), []);
  // A mold step in a SIBLING job does not cover this one.
  const two = job('') + "  b:\n    runs-on: ubuntu-24.04\n    steps:\n      - uses: ./.github/actions/build-tools\n        with:\n          mold: 'true'\n";
  assert.deepEqual(unprovisioned(two), ['job a (line 3)']);
});

test('every Linux job that installs Rust provisions the mold linker', () => {
  const offenders = ciYamlFiles().flatMap((f) =>
    unprovisioned(readFileSync(f, 'utf8')).map((o) => `${relative(root, f)}: ${o}`));
  assert.deepEqual(offenders, [], 'install mold via .github/actions/build-tools (mold: \'true\') or add a `# linker: <reason>` opt-out');
});
