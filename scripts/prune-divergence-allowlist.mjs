#!/usr/bin/env node
// Prune the previous-release determinism divergence allowlist after a gateway
// release tag (#1801): drop every entry whose `introduced_in <= TAG` — exactly
// the set the determinism gate's `check_expiry` declares STALE the moment TAG
// becomes the previous release. Before this existed nothing pruned them, so
// every release turned `main` (and every open PR) red until a human opened a
// pruning PR by hand.
//
// The append-only upgrade-boundaries.json ledger is NEVER touched: expiring
// an allowlist entry must not lift the cluster-join restriction it recorded
// (#1702).
//
// Usage:
//   node scripts/prune-divergence-allowlist.mjs TAG [--write] [--allowlist PATH]
//
// Without --write, prints the pruned JSON to stdout and the removed entries to
// stderr (dry run). With --write, rewrites the allowlist file in place
// (2-space JSON + trailing newline, the file's checked-in style). Exits 0 with
// no changes when nothing is stale (idempotent: re-running a release is safe).

import { readFileSync, writeFileSync } from "node:fs";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { pruneAllowlist } from "./allowlist-version.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_ALLOWLIST = "engine-core/examples/determinism/divergence-allowlist.json";

function parseArgs(argv) {
  const opts = { tag: null, write: false, allowlist: DEFAULT_ALLOWLIST };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--write") opts.write = true;
    else if (a === "--allowlist") opts.allowlist = argv[++i];
    else if (!a.startsWith("--") && !opts.tag) opts.tag = a;
    else throw new Error(`unknown argument ${a}`);
  }
  if (!opts.tag) throw new Error("usage: prune-divergence-allowlist.mjs TAG [--write] [--allowlist PATH]");
  return opts;
}

const opts = parseArgs(process.argv.slice(2));
const path = resolve(root, opts.allowlist);
const allowlist = JSON.parse(readFileSync(path, "utf8"));
const { pruned, removed } = pruneAllowlist(allowlist, opts.tag);

if (removed.length === 0) {
  console.error(`prune-divergence-allowlist: no entries expire at ${opts.tag} (allowlist unchanged)`);
  // Dry run always emits the (unchanged) allowlist JSON so a caller parsing
  // stdout never gets empty input; --write leaves the file byte-identical.
  if (!opts.write) process.stdout.write(`${JSON.stringify(pruned, null, 2)}\n`);
  process.exit(0);
}
for (const r of removed) {
  console.error(`prune-divergence-allowlist: expired at ${opts.tag}: ${r.what} (introduced_in ${r.introduced_in})`);
}
const out = `${JSON.stringify(pruned, null, 2)}\n`;
if (opts.write) {
  writeFileSync(path, out);
  console.error(`prune-divergence-allowlist: removed ${removed.length} entr${removed.length === 1 ? "y" : "ies"} from ${opts.allowlist}`);
} else {
  process.stdout.write(out);
}
