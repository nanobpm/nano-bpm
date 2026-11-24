// Unit + drift tests for scripts/allowlist-version.mjs and its two callers
// (#1801): scripts/check-allowlist-introduced-in.mjs (the PR gate against a
// hand-guessed `introduced_in`) and scripts/prune-divergence-allowlist.mjs
// (the release-time pruning the tag-triggered workflow turns into a PR).
// Run: node --test scripts/allowlist-version.test.mjs

import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  allowlistEntries,
  checkIntroducedIn,
  cmpRelease,
  latestGatewayTag,
  nextGatewayRelease,
  parseRelease,
  pruneAllowlist,
  releaseTagNames,
} from "./allowlist-version.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const ALLOWLIST_REL = "engine-core/examples/determinism/divergence-allowlist.json";

function entry(variant, introduced_in, extra = {}) {
  return { variant, introduced_in, issue: "#1", upgrade: "feature-gated", rationale: "feature-gated: x", ...extra };
}

// --- parseRelease / cmpRelease ------------------------------------------------

test("parseRelease accepts v-prefixed and bare triples, rejects the rest", () => {
  assert.deepEqual(parseRelease("v0.0.28"), [0, 0, 28]);
  assert.deepEqual(parseRelease("1.2.3"), [1, 2, 3]);
  assert.equal(parseRelease("v0.0.28.1"), null);
  assert.equal(parseRelease("nano-bernd-npm-v0.3.0"), null);
  assert.equal(parseRelease(""), null);
});

test("cmpRelease orders by SemVer precedence", () => {
  assert.ok(cmpRelease([0, 0, 28], [0, 0, 29]) < 0);
  assert.ok(cmpRelease([0, 1, 0], [0, 0, 99]) > 0);
  assert.equal(cmpRelease([1, 0, 0], [1, 0, 0]), 0);
});

// --- nextGatewayRelease --------------------------------------------------------

test("nextGatewayRelease is the pending workspace bump when it is ahead of every tag", () => {
  // The release PR has bumped server/Cargo.toml to 0.0.29; v0.0.28 is tagged.
  const next = nextGatewayRelease({ workspaceVersion: "0.0.29", tags: ["v0.0.27", "v0.0.28"] });
  assert.equal(next, "v0.0.29");
});

test("nextGatewayRelease is null (no guess) when no bump is pending", () => {
  // A guessed next patch (v0.0.29) would be wrong if the release becomes a
  // minor/major — and an upgrade-boundary recorded under it is permanent.
  const next = nextGatewayRelease({ workspaceVersion: "0.0.28", tags: ["v0.0.28", "v0.0.27"] });
  assert.equal(next, null);
});

test("nextGatewayRelease follows a minor/major bump, never a patch guess", () => {
  assert.equal(nextGatewayRelease({ workspaceVersion: "0.1.0", tags: ["v0.0.28"] }), "v0.1.0");
  assert.equal(nextGatewayRelease({ workspaceVersion: "1.0.0", tags: ["v0.0.28"] }), "v1.0.0");
});

test("checkIntroducedIn without a committed bump accepts an unreleased label (allowlist-only feature PR)", () => {
  const allowlist = {
    observational_variants: [entry("A", "v0.0.29", { upgrade: "upgrade-boundary" })],
    divergences: [],
  };
  assert.equal(checkIntroducedIn(allowlist, null, "v0.0.28"), null);
});

test("bump PR rejects a provisional label the release skipped (v0.0.29 vs v0.1.0)", () => {
  const allowlist = { observational_variants: [entry("A", "v0.0.29")], divergences: [] };
  assert.match(checkIntroducedIn(allowlist, "v0.1.0", "v0.0.28"), /next gateway release is v0\.1\.0/);
});

test("checkIntroducedIn without a committed bump rejects already-shipped and unparseable labels", () => {
  const shipped = { observational_variants: [], divergences: [{ scenario: "s", introduced_in: "v0.0.28" }] };
  assert.match(checkIntroducedIn(shipped, null, "v0.0.28"), /already shipped/);
  const older = { observational_variants: [], divergences: [{ scenario: "s", introduced_in: "v0.0.27" }] };
  assert.match(checkIntroducedIn(older, null, "v0.0.28"), /already shipped/);
  const bad = { observational_variants: [], divergences: [{ scenario: "s", introduced_in: "next" }] };
  assert.match(checkIntroducedIn(bad, null, "v0.0.28"), /not a release tag/);
  // An empty allowlist is still clean.
  assert.equal(checkIntroducedIn({ observational_variants: [], divergences: [] }, null), null);
});

test("nextGatewayRelease ignores non-gateway tags when finding the latest", () => {
  const next = nextGatewayRelease({
    workspaceVersion: "0.0.29",
    tags: ["v0.0.28", "nano-bernd-npm-v0.3.0", "bojtos-npm-v0.99.0"],
  });
  assert.equal(next, "v0.0.29");
});

// --- checkIntroducedIn (#1528's mislabel would have failed this) ---------------

test("checkIntroducedIn rejects an entry labelled for a LATER release than the next one", () => {
  // Exactly the #1528 defect: labelled v0.0.29 while v0.0.28 was the release
  // about to ship — the ledger then recorded a spurious 0.0.29 boundary.
  const allowlist = { observational_variants: [entry("GatewayRouteSelected", "v0.0.29")], divergences: [] };
  const err = checkIntroducedIn(allowlist, "v0.0.28");
  assert.match(err, /GatewayRouteSelected/);
  assert.match(err, /v0\.0\.29/);
  assert.match(err, /v0\.0\.28/);
});

test("checkIntroducedIn rejects an entry labelled for an EARLIER (already shipped) release", () => {
  const allowlist = { observational_variants: [], divergences: [{ scenario: "s", introduced_in: "v0.0.27" }] };
  assert.match(checkIntroducedIn(allowlist, "v0.0.28"), /scenario s/);
});

test("checkIntroducedIn accepts entries labelled with the next release, and an empty allowlist", () => {
  const allowlist = { observational_variants: [entry("A", "v0.0.28")], divergences: [{ scenario: "s", introduced_in: "v0.0.28" }] };
  assert.equal(checkIntroducedIn(allowlist, "v0.0.28"), null);
  assert.equal(checkIntroducedIn({ observational_variants: [], divergences: [] }, "v0.0.28"), null);
});

test("checkIntroducedIn reports an unparseable introduced_in rather than ignoring it", () => {
  const allowlist = { observational_variants: [entry("A", "soon")], divergences: [] };
  assert.match(checkIntroducedIn(allowlist, "v0.0.28"), /not a release tag/);
});

// --- pruneAllowlist -------------------------------------------------------------

test("pruneAllowlist drops exactly the entries the gate would call STALE (introduced_in <= tag)", () => {
  const allowlist = {
    observational_variants: [entry("ShippedNow", "v0.0.28"), entry("Future", "v0.0.29")],
    divergences: [
      { scenario: "old", introduced_in: "v0.0.27", issue: "#1", upgrade: "upgrade-boundary", rationale: "upgrade-boundary: x" },
      { scenario: "new", introduced_in: "v0.0.29", issue: "#2", upgrade: "feature-gated", rationale: "feature-gated: x" },
    ],
  };
  const { pruned, removed } = pruneAllowlist(allowlist, "v0.0.28");
  assert.deepEqual(
    pruned.observational_variants.map((o) => o.variant),
    ["Future"],
  );
  assert.deepEqual(
    pruned.divergences.map((d) => d.scenario),
    ["new"],
  );
  assert.equal(removed.length, 2);
  // The input is not mutated.
  assert.equal(allowlist.observational_variants.length, 2);
});

test("pruneAllowlist keeps an unparseable introduced_in (fail closed: a human must look)", () => {
  const allowlist = { observational_variants: [entry("Weird", "soon")], divergences: [] };
  const { pruned, removed } = pruneAllowlist(allowlist, "v0.0.28");
  assert.equal(pruned.observational_variants.length, 1);
  assert.equal(removed.length, 0);
});

test("pruneAllowlist is a no-op when nothing has expired (release re-run is safe)", () => {
  const allowlist = { observational_variants: [entry("A", "v0.0.29")], divergences: [] };
  const { pruned, removed } = pruneAllowlist(allowlist, "v0.0.28");
  assert.deepEqual(pruned, allowlist);
  assert.equal(removed.length, 0);
});

// --- CLI: prune-divergence-allowlist.mjs ---------------------------------------

test("prune-divergence-allowlist.mjs --write rewrites the file; a second run is a no-op", () => {
  const dir = mkdtempSync(join(tmpdir(), "allowlist-prune-"));
  const file = join(dir, "allowlist.json");
  writeFileSync(
    file,
    JSON.stringify({ observational_variants: [entry("A", "v0.0.28"), entry("B", "v0.0.29")], divergences: [] }, null, 2) + "\n",
  );
  const script = join(root, "scripts", "prune-divergence-allowlist.mjs");
  execFileSync("node", [script, "v0.0.28", "--write", "--allowlist", file], { encoding: "utf8" });
  const after = JSON.parse(readFileSync(file, "utf8"));
  assert.deepEqual(
    after.observational_variants.map((o) => o.variant),
    ["B"],
  );
  // Idempotent: the second run exits 0 and leaves the file byte-identical.
  const before2 = readFileSync(file, "utf8");
  execFileSync("node", [script, "v0.0.28", "--write", "--allowlist", file], { encoding: "utf8" });
  assert.equal(readFileSync(file, "utf8"), before2);
});

test("prune-divergence-allowlist.mjs dry run emits the unchanged JSON when nothing expires", () => {
  const dir = mkdtempSync(join(tmpdir(), "allowlist-prune-"));
  const file = join(dir, "allowlist.json");
  const doc = { observational_variants: [entry("B", "v0.0.29")], divergences: [] };
  writeFileSync(file, JSON.stringify(doc, null, 2) + "\n");
  const script = join(root, "scripts", "prune-divergence-allowlist.mjs");
  const out = execFileSync("node", [script, "v0.0.28", "--allowlist", file], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  assert.deepEqual(JSON.parse(out), doc);
});

test("prune-divergence-allowlist.mjs rejects a non-release tag", () => {
  const script = join(root, "scripts", "prune-divergence-allowlist.mjs");
  assert.throws(() =>
    execFileSync("node", [script, "not-a-tag", "--allowlist", join(root, ALLOWLIST_REL)], { encoding: "utf8", stdio: "pipe" }),
  );
});

// --- drift guards against the checked-in files ---------------------------------

test("the checked-in allowlist parses and every entry carries introduced_in", () => {
  const allowlist = JSON.parse(readFileSync(join(root, ALLOWLIST_REL), "utf8"));
  for (const { what, introduced_in } of allowlistEntries(allowlist)) {
    assert.ok(parseRelease(introduced_in), `${what} has no parseable introduced_in`);
  }
});

test("the checked-in allowlist is clean for the CURRENT next gateway release", async () => {
  // The repo-state half of the #1801 gate: with the allowlist empty (post
  // v0.0.28 pruning, #1802) this passes vacuously; the first NEW entry must
  // name the release its PR will ship in. Derived from git tags + the
  // workspace version, via the SAME tag source as the CI gate
  // (releaseTagNames: remote tags first), so a shallow checkout without local
  // tags can't derive a stale expected version.
  const tags = releaseTagNames(root);
  const { cargoPackageVersion } = await import("./release-tags.mjs");
  const workspaceVersion = cargoPackageVersion(readFileSync(join(root, "server/Cargo.toml"), "utf8"));
  const expected = nextGatewayRelease({ workspaceVersion, tags });
  const allowlist = JSON.parse(readFileSync(join(root, ALLOWLIST_REL), "utf8"));
  // Same policy as the CLI gate: also reject already-shipped labels when no
  // bump is pending (expected === null).
  const latestTag = latestGatewayTag(tags)?.tag ?? null;
  assert.equal(checkIntroducedIn(allowlist, expected, latestTag), null);
});

test("the pruning job is wired into the tag-triggered gateway release workflow", () => {
  // #1801 root cause 1: the release itself must open the pruning PR. Pin the
  // wiring so a workflow edit can't silently drop it and re-open the red-main
  // window.
  const wf = readFileSync(join(root, ".github/workflows/publish-c8ctl-binaries.yml"), "utf8");
  assert.match(wf, /prune-divergence-allowlist\.mjs/);
  assert.match(wf, /chore\/prune-determinism-allowlist-\$\{TAG\}/);
  const job = wf.slice(wf.indexOf("  prune-allowlist:"), wf.indexOf("\n  publish:"));
  // GITHUB_TOKEN may not create PRs (#1907) and its pushes do not trigger the
  // required CI: the push and the PR use a GitHub App installation token.
  assert.match(job, /uses: actions\/create-github-app-token@/);
  assert.match(job, /token: \$\{\{ steps\.app-token\.outputs\.token \}\}/);
  assert.match(job, /GH_TOKEN: \$\{\{ steps\.app-token\.outputs\.token \}\}/);
  assert.doesNotMatch(job, /GITHUB_TOKEN \}\}|github\.token/);
  // Re-run recovery: an already-pushed branch is reused, not recreated.
  assert.match(job, /git ls-remote --exit-code --heads origin "\$branch"/);
});
