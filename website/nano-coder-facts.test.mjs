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

// Build the site once into a temp dir and return every renderable file's text,
// keyed by path, so each scoped assertion runs against the same output.
function buildSite() {
  const dir = mkdtempSync(join(tmpdir(), "nano-site-"));
  try {
    execFileSync(process.execPath, [BUILD], { stdio: "pipe", env: { ...process.env, NANO_CODER_DOCS_OFFLINE: "1", SITE_OUT_DIR: dir } });
    const files = new Map();
    for (const f of walk(dir)) files.set(f, readFileSync(f, "utf8"));
    return { dir, files };
  } catch (err) {
    rmSync(dir, { recursive: true, force: true });
    throw err;
  }
}

// Strip the launch post's historical title/slug ("6MB") everywhere it legitimately
// survives, so the stale-fact checks below don't flag the preserved history.
function stripHistorical(text) {
  return text
    .replaceAll("A 6MB coding agent, and why fleets need one", "")
    .replaceAll("a-6mb-coding-agent", "");
}

test("no rendered page pairs nano-coder with Rust or 6MB outside the launch post title/slug", () => {
  const { dir, files } = buildSite();
  try {
    const bad = [];
    for (const [f, raw] of files) {
      const text = stripHistorical(raw);
      for (const s of text.split(/(?<=[.!?])\s+|\n/)) {
        if (/nano-coder/i.test(s) && !/last Rust release/.test(s) && /\bRust\b|\b6\s?MB\b/i.test(s)) bad.push(`${f}: ${s.trim().slice(0, 160)}`);
      }
    }
    assert.deepEqual(bad, []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// Scoped guards for the three stale descriptions the sentence-splitting check above
// misses: the product headline drops the "nano-coder" name, and the feature card /
// homepage paragraph separate the language claim from the name across a sentence
// boundary. Each asserts the exact original wording is gone, so restoring any of
// them fails here even though the name-scoped check can't see it.
test("stale Rust product headline, feature card, and homepage wording are gone", () => {
  const { dir, files } = buildSite();
  try {
    const bad = [];
    // The original product-page headline led with the pre-1.0 size and never named
    // nano-coder in the same sentence. Any residual "A 6MB coding agent" outside the
    // preserved launch-post title is the stale headline.
    const STALE_HEADLINE = /A\s+6\s?MB\s+coding agent/i;
    // The original nano-coder feature card stated the language outright, as a
    // capitalized sentence-final "Written in Rust." Match that exact card wording
    // (case-sensitive) so the architecture page's legitimate engine card — the
    // lowercase "The load-bearing runtime, written in Rust." — is not flagged.
    const STALE_FEATURE_CARD = /Written in Rust\./;
    // The original homepage band described nano-coder as a Rust agent.
    const STALE_HOMEPAGE = /It is a Rust coding agent/i;
    for (const [f, raw] of files) {
      const text = stripHistorical(raw);
      for (const [label, re] of [["headline", STALE_HEADLINE], ["feature-card", STALE_FEATURE_CARD], ["homepage", STALE_HOMEPAGE]]) {
        const m = text.match(re);
        if (m) bad.push(`${f} [${label}]: ${m[0]}`);
      }
    }
    assert.deepEqual(bad, []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
