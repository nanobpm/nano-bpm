// Resolves Cargo's effective target directory for a crate directory, so
// consumers of a built artifact read the same directory the build wrote to
// (#1716). A bare `engine-core/target` guess breaks whenever the build's
// target dir was redirected — an exported CARGO_TARGET_DIR (the Makefile sets
// one for a checkout path containing a space, because tikv-jemalloc-sys's
// autoconf configure word-splits a spaced build dir), a CARGO_TARGET_DIR from
// the caller's environment, or a `build.target-dir` in some .cargo/config.toml
// up the tree. In those cases the consumer would fail on a fresh checkout or,
// worse, package/verify a STALE artifact left in `engine-core/target`.
//
// Precedence mirrors Cargo itself:
//   1. CARGO_TARGET_DIR from the environment (wins over config);
//   2. `cargo metadata --no-deps` (applies .cargo/config.toml build.target-dir,
//      resolved relative to the config file like Cargo does);
// Fails CLOSED: if neither source resolves the directory, throw. Guessing
// <crateDir>/target could package a stale artifact as current.

import { execFileSync } from 'node:child_process';
import { isAbsolute, resolve } from 'node:path';

export function resolveTargetDir(crateDir) {
  const fromEnv = process.env.CARGO_TARGET_DIR;
  if (fromEnv) {
    return isAbsolute(fromEnv) ? fromEnv : resolve(crateDir, fromEnv);
  }
  let out;
  try {
    out = execFileSync(
      'cargo',
      ['metadata', '--no-deps', '--format-version', '1'],
      { cwd: crateDir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (e) {
    throw new Error(
      `cannot resolve Cargo's effective target dir in ${crateDir}: cargo metadata failed (#1716): ${e.stderr || e.message}`,
    );
  }
  const dir = JSON.parse(out).target_directory;
  if (typeof dir !== 'string' || dir === '') {
    throw new Error(`cargo metadata in ${crateDir} reported no target_directory (#1716)`);
  }
  return dir;
}
