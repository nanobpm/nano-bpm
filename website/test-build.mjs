// Shared test helper: run build.mjs into a private temp output directory.
// build.mjs wipes its output dir before writing, so tests (which `node --test`
// runs in parallel processes) must never share `website/_site`.
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after } from "node:test";
import { fileURLToPath } from "node:url";

const BUILD = fileURLToPath(new URL("./build.mjs", import.meta.url));

/**
 * Build the site into a fresh temp dir; returns `read(relPath)` over it.
 * Builds are offline by default (no nano-coder docs fetch: tests must not hit
 * the network or depend on nano-coder's latest release); `env` overrides.
 */
export function buildSite(env = {}) {
  const dir = mkdtempSync(join(tmpdir(), "nano-site-"));
  after(() => rmSync(dir, { recursive: true, force: true }));
  execFileSync(process.execPath, [BUILD], { stdio: "pipe", env: { ...process.env, NANO_CODER_DOCS_OFFLINE: "1", ...env, SITE_OUT_DIR: dir } });
  const read = (rel) => readFileSync(join(dir, rel), "utf8");
  read.dir = dir;
  return read;
}
