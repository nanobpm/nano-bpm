// Unit + drift tests for scripts/release-tags.mjs (#1289).
// Run: node --test scripts/release-tags.test.mjs

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  GITHUB_MAX_TAGS_PER_PUSH,
  TRAINS,
  cargoPackageVersion,
  hasNewRun,
  parseLsRemoteTags,
  planTags,
  pomProjectVersion,
  pushCommands,
} from "./release-tags.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const readRepo = (p) => readFileSync(join(root, p), "utf8");

function fakeRepo({ gateway = "0.0.24", npm = "0.3.0", jvm = "0.3.0", wasm = "0.10.0", wasmBuilt = wasm } = {}) {
  const files = {
    "server/Cargo.toml": `[package]\nname = "x"\nversion = "${gateway}"\n\n[dependencies]\nfoo = { version = "9.9.9" }\n`,
    "clients/nano-bernd/package.json": JSON.stringify({ version: npm }),
    "clients/nano-bernd-jvm/pom.xml":
      `<project><parent><version>1.0.0</version></parent><artifactId>x</artifactId><version>${jvm}</version>` +
      `<dependencies><dependency><version>7.7.7</version></dependency></dependencies></project>`,
    "engine-wasm/pkg.package.json": JSON.stringify({ version: wasm }),
    "engine-wasm/pkg/package.json": JSON.stringify({ version: wasmBuilt }),
  };
  return (p) => {
    if (!(p in files)) throw new Error(`unexpected read ${p}`);
    return files[p];
  };
}

// --- the #1289 failure mode ---------------------------------------------------

test("every push carries exactly one tag, however many tags are released", () => {
  const tags = ["v1", "nano-bernd-npm-v1", "nano-bernd-jvm-v1", "bojtos-npm-v1", "v2", "v3"];
  const cmds = pushCommands(tags);
  assert.equal(cmds.length, tags.length);
  for (const [i, cmd] of cmds.entries()) {
    const refs = cmd.filter((a) => a.startsWith("refs/tags/"));
    assert.deepEqual(refs, [`refs/tags/${tags[i]}`]);
    assert.ok(refs.length <= GITHUB_MAX_TAGS_PER_PUSH);
  }
});

test("a full release plans more tags than GitHub accepts in a single push", () => {
  // Guards the premise: if this ever stops being true the one-per-push rule is
  // still correct, but it is why batching silently broke v0.0.24.
  const plan = planTags({ read: fakeRepo(), existingTags: [] });
  assert.ok(plan.length > GITHUB_MAX_TAGS_PER_PUSH);
});

// --- derivation ---------------------------------------------------------------

test("tags are derived from each train's version source", () => {
  const plan = planTags({ read: fakeRepo(), existingTags: [] });
  assert.deepEqual(
    plan.map((p) => p.tag),
    ["v0.0.24", "nano-bernd-npm-v0.3.0", "nano-bernd-jvm-v0.3.0", "bojtos-npm-v0.10.0"],
  );
});

test("already-tagged versions are marked existing, so only bumped trains are cut", () => {
  const plan = planTags({ read: fakeRepo(), existingTags: ["nano-bernd-npm-v0.3.0", "nano-bernd-jvm-v0.3.0"] });
  assert.deepEqual(plan.filter((p) => !p.exists).map((p) => p.tag), ["v0.0.24", "bojtos-npm-v0.10.0"]);
});

test("--only restricts to named trains and rejects unknown ones", () => {
  const plan = planTags({ read: fakeRepo(), existingTags: [], only: ["engine-wasm"] });
  assert.deepEqual(plan.map((p) => p.tag), ["bojtos-npm-v0.10.0"]);
  assert.throws(() => planTags({ read: fakeRepo(), existingTags: [], only: ["nope"] }), /unknown train/);
});

test("--only can't split the nano-bernd hosts: one alone is rejected, both together are fine", () => {
  for (const one of ["nano-bernd-npm", "nano-bernd-jvm"]) {
    assert.throws(() => planTags({ read: fakeRepo(), existingTags: [], only: [one] }), /must always release together/);
    assert.throws(
      () => planTags({ read: fakeRepo(), existingTags: [], only: [one, "gateway"] }),
      /must always release together/,
    );
  }
  const both = planTags({ read: fakeRepo(), existingTags: [], only: ["nano-bernd-npm", "nano-bernd-jvm"] });
  assert.deepEqual(both.map((p) => p.tag), ["nano-bernd-npm-v0.3.0", "nano-bernd-jvm-v0.3.0"]);
});

test("a run left over from an earlier push of the same tag does not count as started", () => {
  // The delete-and-re-push recovery: tag v0.0.24 already has run 101 from the
  // first push. If the re-push event is dropped, no NEW run appears.
  assert.equal(hasNewRun([101], [101]), false);
  assert.equal(hasNewRun([101], [202, 101]), true);
  assert.equal(hasNewRun([], []), false);
  assert.equal(hasNewRun([], [303]), true);
});

test("nano-bernd npm and JVM versions must match", () => {
  assert.throws(() => planTags({ read: fakeRepo({ jvm: "0.2.9" }), existingTags: [] }), /must always release together/);
});

test("engine-wasm template and built package versions must match", () => {
  assert.throws(() => planTags({ read: fakeRepo({ wasmBuilt: "0.9.3" }), existingTags: [] }), /make console-wasm/);
});

test("version parsers ignore dependency and parent versions", () => {
  assert.equal(cargoPackageVersion('[dependencies]\na = { version = "1" }\n[package]\nversion = "2.0.0"\n'), "2.0.0");
  assert.equal(
    pomProjectVersion("<project><parent><version>9</version></parent><version>3.1.0</version></project>"),
    "3.1.0",
  );
});

/** The `on.push.tags` patterns of a workflow (inline `[...]` or `- '…'` list form). */
function workflowTagPatterns(yml) {
  const lines = yml.split("\n");
  const i = lines.findIndex((l) => /^\s+tags:/.test(l));
  if (i < 0) return [];
  const strip = (l) => l.replace(/\s+#.*$/, "");
  const quoted = (l) => [...l.matchAll(/['"]([^'"]+)['"]/g)].map((m) => m[1]);
  const inline = strip(lines[i]).match(/tags:\s*\[(.*)\]/);
  if (inline) return quoted(inline[1]);
  const out = [];
  for (const l of lines.slice(i + 1)) {
    const m = strip(l).match(/^\s+-\s*(.+)$/);
    if (!m) break;
    out.push(...(quoted(m[1]).length ? quoted(m[1]) : [m[1].trim()]));
  }
  return out;
}

test("ls-remote parsing keeps each tag once, peeled or not", () => {
  const out = "aaa\trefs/tags/v0.0.24\nbbb\trefs/tags/v0.0.24^{}\nccc\trefs/tags/bojtos-npm-v0.10.0\n";
  assert.deepEqual(parseLsRemoteTags(out), ["v0.0.24", "bojtos-npm-v0.10.0"]);
});

// --- drift guards against the real repo ---------------------------------------

test("the real repo's versions parse for every train", () => {
  const plan = planTags({ read: readRepo, existingTags: [] });
  assert.equal(plan.length, TRAINS.length);
  for (const p of plan) assert.match(p.tag, /\d+\.\d+\.\d+/);
});

test("each train's workflows exist and trigger on that train's tag prefix", () => {
  for (const t of TRAINS) {
    for (const wf of t.workflows) {
      const yml = readRepo(`.github/workflows/${wf}`);
      const tags = workflowTagPatterns(yml);
      assert.ok(
        tags.includes(`${t.tagPrefix}*`),
        `${wf} must trigger on '${t.tagPrefix}*' (found: ${tags.join(", ") || "none"})`,
      );
    }
  }
});

test("every tag-triggered release/publish workflow belongs to a train", () => {
  const owned = new Set(TRAINS.flatMap((t) => t.workflows));
  const wfs = readdirSync(join(root, ".github/workflows")).filter((f) => /^(release|publish)-.*\.ya?ml$/.test(f));
  const tagTriggered = wfs.filter((f) => /^\s*tags:/m.test(readRepo(`.github/workflows/${f}`)));
  const orphans = tagTriggered.filter((f) => !owned.has(f));
  // Trains released independently of the engine version (not part of a
  // coordinated engine release) are listed here explicitly, with a reason.
  const independent = new Set([
    "release-ai-assert-npm.yml", // @nanobpm/ai-assert: own versioning, own RELEASING.md
    "release-engine-testkit-npm.yml", // @nanobpm/engine-testkit: own versioning
    "release-nano-app-schema-npm.yml", // @nanobpm/nano-app-schema: own versioning
  ]);
  assert.deepEqual(orphans.filter((f) => !independent.has(f)), [], "add the workflow to TRAINS or to the independent list");
});
