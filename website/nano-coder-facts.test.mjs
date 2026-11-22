// Guard: no rendered page describes nano-coder with stale pre-1.0 facts
// (Rust, ~6MB). The cargo note naming the "last Rust release" is historical; the launch post's title/slug keep "6MB" as history only.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const BUILD = fileURLToPath(new URL("./build.mjs", import.meta.url));

function walk(dir, out = []) {
  for (const n of readdirSync(dir)) {
    const p = join(dir, n);
    if (statSync(p).isDirectory()) walk(p, out);
    else if (/\.(html|xml|json|txt)$/.test(n)) out.push(p);
  }
  return out;
}

test("no rendered page pairs nano-coder with Rust or 6MB outside the launch post title/slug", () => {
  const dir = mkdtempSync(join(tmpdir(), "nano-site-"));
  try {
    execFileSync(process.execPath, [BUILD], { stdio: "pipe", env: { ...process.env, SITE_OUT_DIR: dir } });
    const bad = [];
    for (const f of walk(dir)) {
      const text = readFileSync(f, "utf8")
        .replaceAll("A 6MB coding agent, and why fleets need one", "")
        .replaceAll("a-6mb-coding-agent", "");
      for (const s of text.split(/(?<=[.!?])\s+|\n/)) {
        if (/nano-coder/i.test(s) && !/last Rust release/.test(s) && /\bRust\b|\b6\s?MB\b/i.test(s)) bad.push(`${f}: ${s.trim().slice(0, 160)}`);
      }
    }
    assert.deepEqual(bad, []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
