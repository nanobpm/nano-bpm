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
//   3. <crateDir>/target (fallback when cargo cannot run).

import { execFileSync } from 'node:child_process';
import { isAbsolute, join, resolve } from 'node:path';

export function resolveTargetDir(crateDir) {
  const fromEnv = process.env.CARGO_TARGET_DIR;
  if (fromEnv) {
    return isAbsolute(fromEnv) ? fromEnv : resolve(crateDir, fromEnv);
  }
  try {
    const out = execFileSync(
      'cargo',
      ['metadata', '--no-deps', '--format-version', '1'],
      { cwd: crateDir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
    );
    const dir = JSON.parse(out).target_directory;
    if (dir) return dir;
  } catch {
    // cargo unavailable or metadata failed — fall through to the default.
  }
  return join(crateDir, 'target');
}
