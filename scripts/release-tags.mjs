#!/usr/bin/env node
// Tag and push nano-bpm releases — the ONE canonical way to cut release tags
// (RELEASE.md, #1289).
//
// Every release train is described once in TRAINS: its tag prefix, where its
// version lives, and which workflows the tag must trigger. Tags are DERIVED
// from those version sources, so a tag can never disagree with the version it
// publishes, and only trains whose current version is not yet tagged are cut.
//
// Failure mode this exists to kill (#1289): GitHub does not create push events
// for tags when MORE THAN THREE tags are pushed in one `git push`. The v0.0.24
// release pushed four tags at once and no release workflow ran — silently.
// So this script (a) pushes every tag in its OWN `git push`, and (b) after each
// push, confirms each of the train's workflows actually started, failing loudly
// with the remediation if one did not.
//
// Usage:
//   node scripts/release-tags.mjs                 # dry run: show the plan
//   node scripts/release-tags.mjs --push          # tag HEAD (must be origin/main) and push
//   node scripts/release-tags.mjs --only engine-wasm --push
//
// Requires `git` and, for --push, an authenticated `gh` (to confirm the runs).

import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/** GitHub drops tag push events when more than this many tags share a push. */
export const GITHUB_MAX_TAGS_PER_PUSH = 3;

/** Read `version = "x"` from a Cargo.toml's [package] table. */
export function cargoPackageVersion(toml) {
  const pkg = toml.split(/^\[/m).find((s) => s.startsWith("package]"));
  const m = pkg?.match(/^\s*version\s*=\s*"([^"]+)"/m);
  if (!m) throw new Error("no [package] version in Cargo.toml");
  return m[1];
}

/** Read the project's own <version> from a pom.xml (not a parent's or a dependency's). */
export function pomProjectVersion(xml) {
  const body = xml
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/<parent>[\s\S]*?<\/parent>/g, "")
    .replace(/<(dependencies|dependencyManagement|build|plugins|profiles)>[\s\S]*?<\/\1>/g, "");
  const m = body.match(/<version>\s*([^<\s]+)\s*<\/version>/);
  if (!m) throw new Error("no project <version> in pom.xml");
  return m[1];
}

export function packageJsonVersion(json) {
  const v = JSON.parse(json).version;
  if (!v) throw new Error("no version in package.json");
  return v;
}

/**
 * The single source of truth for every release train. `version(read)` derives
 * the version from the repo (`read(relPath)` returns file text) and throws on
 * any inconsistency that would publish the wrong thing.
 */
export const TRAINS = [
  {
    id: "gateway",
    tagPrefix: "v",
    version: (read) => cargoPackageVersion(read("server/Cargo.toml")),
    workflows: ["publish-c8ctl-binaries.yml", "publish-processos-binaries.yml"],
  },
  {
    id: "nano-bernd-npm",
    tagPrefix: "nano-bernd-npm-v",
    version: (read) => berndVersion(read),
    workflows: ["release-nano-bernd-npm.yml"],
  },
  {
    id: "nano-bernd-jvm",
    tagPrefix: "nano-bernd-jvm-v",
    version: (read) => berndVersion(read),
    workflows: ["release-nano-bernd-jvm.yml"],
  },
  {
    id: "engine-wasm",
    tagPrefix: "bojtos-npm-v",
    version: (read) => {
      const tmpl = packageJsonVersion(read("engine-wasm/pkg.package.json"));
      const built = packageJsonVersion(read("engine-wasm/pkg/package.json"));
      if (tmpl !== built) {
        throw new Error(
          `engine-wasm version mismatch: pkg.package.json=${tmpl} but pkg/package.json=${built} — run 'make console-wasm' first`,
        );
      }
      return tmpl;
    },
    workflows: ["release-bojtos-npm.yml"],
  },
];

/** Trains that wrap the same wasm blob and must always be tagged together. */
export const BERND_TRAINS = ["nano-bernd-npm", "nano-bernd-jvm"];

/** The npm and JVM nano-bernd hosts wrap the same wasm and must share a version. */
function berndVersion(read) {
  const npm = packageJsonVersion(read("clients/nano-bernd/package.json"));
  const jvm = pomProjectVersion(read("clients/nano-bernd-jvm/pom.xml"));
  if (npm !== jvm) {
    throw new Error(
      `nano-bernd npm (${npm}) and JVM (${jvm}) versions differ — they must always release together`,
    );
  }
  return npm;
}

/**
 * Decide which tags to cut: one per train whose derived tag does not exist yet.
 * `only` restricts to the named train ids.
 */
export function planTags({ read, existingTags, only = null }) {
  const known = new Set(TRAINS.map((t) => t.id));
  for (const id of only ?? []) {
    if (!known.has(id)) throw new Error(`unknown train "${id}" (known: ${[...known].join(", ")})`);
  }
  if (only) {
    const bernd = only.filter((id) => BERND_TRAINS.includes(id));
    if (bernd.length === 1) {
      throw new Error(
        `--only ${bernd[0]}: the nano-bernd npm and JVM hosts must always release together — select both (${BERND_TRAINS.join(",")})`,
      );
    }
  }
  const existing = new Set(existingTags);
  return TRAINS.filter((t) => !only || only.includes(t.id)).map((t) => {
    const tag = `${t.tagPrefix}${t.version(read)}`;
    return { train: t.id, tag, workflows: t.workflows, exists: existing.has(tag) };
  });
}

/** Tag names from `git ls-remote --tags` output (peeled `^{}` entries collapsed). */
export function parseLsRemoteTags(out) {
  const tags = new Set();
  for (const line of out.split("\n")) {
    const m = line.match(/\trefs\/tags\/(.+?)(\^\{\})?$/);
    if (m) tags.add(m[1]);
  }
  return [...tags];
}

/**
 * The git push commands for the planned tags — ONE tag per push, always, so no
 * push can ever cross GitHub's >3-tags-per-push event cutoff (#1289).
 */
export function pushCommands(tags) {
  return tags.map((tag) => ["git", "push", "origin", `refs/tags/${tag}`]);
}

// --- CLI --------------------------------------------------------------------

function sh(cmd, args, opts = {}) {
  return execFileSync(cmd, args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], ...opts }).trim();
}

function parseArgs(argv) {
  const opts = { push: false, only: null, waitSeconds: 180 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--push") opts.push = true;
    else if (a === "--only") opts.only = (argv[++i] ?? "").split(",").filter(Boolean);
    else if (a === "--wait-seconds") opts.waitSeconds = Number(argv[++i]);
    else throw new Error(`unknown argument ${a}`);
  }
  return opts;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Did THIS push start a run? Runs outlive tag deletion, so a re-pushed tag can
 * already have runs from an earlier push; only a run id absent from the
 * pre-push snapshot counts (#1289).
 */
export function hasNewRun(beforeIds, afterIds) {
  const before = new Set(beforeIds);
  return afterIds.some((id) => !before.has(id));
}

function runIds(workflow, tag) {
  const out = sh("gh", [
    "run", "list", "--workflow", workflow, "--branch", tag, "--limit", "50", "--json", "databaseId",
  ]);
  return JSON.parse(out || "[]").map((r) => r.databaseId);
}

/** Wait until `workflow` has a run for `tag` that wasn't in `beforeIds`. */
async function workflowStarted(workflow, tag, beforeIds, waitSeconds) {
  const deadline = Date.now() + waitSeconds * 1000;
  while (Date.now() < deadline) {
    if (hasNewRun(beforeIds, runIds(workflow, tag))) return true;
    await sleep(10_000);
  }
  return false;
}

async function main() {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const opts = parseArgs(process.argv.slice(2));
  const read = (p) => readFileSync(join(root, p), "utf8");
  const git = (...a) => sh("git", a, { cwd: root });

  // The REMOTE decides whether a release exists; local tags can be stale or
  // clobbered (a divergent local tag must not block or mislead a release).
  git("fetch", "-q", "origin", "main");
  const remoteTags = parseLsRemoteTags(git("ls-remote", "--tags", "origin"));
  const plan = planTags({ read, existingTags: remoteTags, only: opts.only });
  const todo = plan.filter((p) => !p.exists);
  for (const p of plan) {
    console.log(`${p.exists ? "  exists " : "  NEW    "} ${p.tag.padEnd(28)} (${p.train}) -> ${p.workflows.join(", ")}`);
  }
  if (todo.length === 0) {
    console.log("Nothing to tag: every train's current version is already tagged. Bump a version first.");
    return;
  }
  if (!opts.push) {
    console.log("\nDry run. Re-run with --push to tag HEAD and push each tag separately.");
    return;
  }

  if (git("status", "--porcelain") !== "") throw new Error("working tree is dirty — release must tag a clean tree");
  const head = git("rev-parse", "HEAD");
  if (head !== git("rev-parse", "origin/main")) {
    throw new Error("HEAD is not origin/main — tag the merged release commit on main");
  }

  const failed = [];
  const cmds = pushCommands(todo.map((p) => p.tag));
  for (const [i, p] of todo.entries()) {
    const local = sh("git", ["tag", "--list", p.tag], { cwd: root });
    if (local) {
      const at = git("rev-list", "-n", "1", p.tag);
      if (at !== head) throw new Error(`local tag ${p.tag} points at ${at.slice(0, 8)}, not HEAD — delete it locally first`);
    } else {
      git("tag", p.tag, head);
    }
    const before = Object.fromEntries(p.workflows.map((wf) => [wf, runIds(wf, p.tag)]));
    try {
      sh(cmds[i][0], cmds[i].slice(1), { cwd: root });
    } catch (e) {
      git("tag", "-d", p.tag);
      throw new Error(`push of ${p.tag} failed (local tag removed, rerun is safe): ${e.message}`);
    }
    console.log(`pushed ${p.tag}`);
    for (const wf of p.workflows) {
      if (await workflowStarted(wf, p.tag, before[wf], opts.waitSeconds)) console.log(`  ✓ ${wf} started`);
      else failed.push(`${wf} did not start for ${p.tag}`);
    }
  }
  if (failed.length) {
    throw new Error(
      `${failed.join("; ")}.\nNothing was published by the missing runs. Re-trigger by deleting and re-pushing the tag alone:\n` +
        `  git push origin --delete <tag> && git push origin refs/tags/<tag>`,
    );
  }
  console.log("\nAll release workflows started. Watch them: gh run list --limit 10");
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((e) => {
    console.error(`release-tags: ${e.message}`);
    process.exit(1);
  });
}
