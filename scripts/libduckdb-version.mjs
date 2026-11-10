#!/usr/bin/env node
// Derive the prebuilt libduckdb release the processos build links against
// (#1461). The single source of truth is the `libduckdb-sys` version locked in
// processos/Cargo.lock; duckdb-rs encodes the DuckDB release as
// 1.MAJOR_MINOR_PATCH.x (e.g. 1.10504.0 => DuckDB 1.5.4), mirroring
// `duckdb_version_from_pkg_version` in libduckdb-sys' build.rs.
//
// Usage: node scripts/libduckdb-version.mjs [--github-output]
//   prints `crate=<crate version>` and `duckdb=<duckdb version>` lines (to
//   $GITHUB_OUTPUT with --github-output).
import { appendFileSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

/** The locked `libduckdb-sys` crate version in a Cargo.lock text. */
export function lockedCrateVersion(lock) {
  const m = lock.match(/\[\[package\]\]\s*\nname = "libduckdb-sys"\s*\nversion = "([^"]+)"/);
  if (!m) throw new Error('processos/Cargo.lock has no libduckdb-sys package');
  return m[1];
}

/** duckdb-rs `1.MAJOR_MINOR_PATCH.x` crate version => DuckDB release version. */
export function duckdbVersion(crateVersion) {
  const encoded = Number.parseInt(crateVersion.split('.')[1] ?? '', 10);
  if (!Number.isInteger(encoded)) throw new Error(`unexpected libduckdb-sys version ${crateVersion}`);
  return `${Math.floor(encoded / 10000)}.${Math.floor(encoded / 100) % 100}.${encoded % 100}`;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const crate = lockedCrateVersion(readFileSync(join(root, 'processos', 'Cargo.lock'), 'utf8'));
  const out = `crate=${crate}\nduckdb=${duckdbVersion(crate)}\n`;
  if (process.argv.includes('--github-output')) appendFileSync(process.env.GITHUB_OUTPUT, out);
  else process.stdout.write(out);
}
