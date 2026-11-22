#!/usr/bin/env node
// CI gate (#1801): every divergence-allowlist entry's `introduced_in` must be
// DERIVED, not hand-guessed — it must equal the NEXT gateway release: the
// pending server/Cargo.toml version bump. With no bump committed, any entry
// fails (no guessed next-patch: the release may be a minor/major). #1528's entries were labelled v0.0.29 but shipped
// in v0.0.28, and the append-only upgrade-boundaries.json ledger recorded the
// spurious boundary (it forbids edits, so it is permanent). This check fails
// the PR that introduces a mislabel, before the ledger can record it.
//
// Usage: node scripts/check-allowlist-introduced-in.mjs
// Exit 0 when the allowlist is empty or every entry matches; exit 1 listing
// every mismatch.

import { readFileSync } from "node:fs";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { checkIntroducedIn, nextGatewayRelease, releaseTagNames } from "./allowlist-version.mjs";
import { cargoPackageVersion } from "./release-tags.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ALLOWLIST = "engine-core/examples/determinism/divergence-allowlist.json";

const allowlist = JSON.parse(readFileSync(join(root, ALLOWLIST), "utf8"));
const workspaceVersion = cargoPackageVersion(readFileSync(join(root, "server/Cargo.toml"), "utf8"));

const tags = releaseTagNames(root);

const expected = nextGatewayRelease({ workspaceVersion, tags });
const err = checkIntroducedIn(allowlist, expected);
if (err) {
  console.error(`check-allowlist-introduced-in: expected introduced_in == ${expected ?? "<no committed release version>"} (the next gateway release)\n${err}`);
  process.exit(1);
}
console.log(`check-allowlist-introduced-in: every entry's introduced_in is the next gateway release (${expected ?? "none committed; allowlist empty"})`);
