// Guards the homepage's Nano Agent Graph Engine install surface against drift
// from its single source of truth (nage-facts.mjs): only hosts we have tested
// get an install tab, and untested hosts appear only under "Also works with",
// so the site never claims more than we have verified.
import assert from "node:assert/strict";
import test from "node:test";

import { nageFacts } from "./nage-facts.mjs";
import { buildSite } from "./test-build.mjs";

test("install tabs are exactly the tested hosts; untested hosts are only 'also works with'", () => {
  const html = buildSite()("index.html");
  const block = html.match(/<div class="host-tabs" id="install">[\s\S]*?<\/div>\s*<div class="cta">/);
  assert.ok(block, "homepage renders the host install tabs");
  const tabs = [...block[0].matchAll(/role="tab" id="tab-host-[^"]+"[^>]*>([^<]+)<\/button>/g)].map((m) => m[1]);
  const { hosts } = nageFacts();
  assert.deepEqual(tabs, hosts.filter((h) => h.tested).map((h) => h.label));
  assert.ok(tabs.length > 0 && tabs[0] === "Claude Code", "Claude Code is the first tab");
  const also = block[0].match(/Also works with ([\s\S]*?)<\/p>/);
  for (const h of hosts.filter((x) => !x.tested)) {
    assert.ok(also?.[1].includes(h.label), `${h.label} is listed under "Also works with"`);
    assert.ok(/not yet tested/.test(also[1]), "untested hosts are labelled as untested");
  }
});

test("the homepage renders the NAGE flow example from the type-checked snippet", () => {
  const html = buildSite()("index.html");
  // Every identifier the snippet imports must reach the page (it is rendered
  // through the highlighter, so compare on the tokens rather than raw text).
  for (const name of ["defineFlow", "defineForms", "forEach", "human"]) {
    assert.match(html, new RegExp(`>${name}<`), `${name} is shown`);
  }
  assert.match(html, /@nanobpm\/nano-agent-graph-engine/);
});

// NAGE's published facts are derived from the locked package, never hand-copied
// (#1517): a bump that changes the CLI name or Node floor must flow to the site,
// and a lock entry missing any of them must fail the build, not render stale copy.
test("NAGE version, CLI and Node floor derive from the locked published package", async () => {
  const { mkdtempSync, writeFileSync, readFileSync, rmSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { publishedNage } = await import("./nage-facts.mjs");
  const lock = JSON.parse(readFileSync(new URL("./snippets/package-lock.json", import.meta.url), "utf8"));
  const entry = lock.packages["node_modules/@nanobpm/nano-agent-graph-engine"];
  const f = nageFacts();
  assert.equal(f.version, entry.version);
  assert.equal(f.node, `Node ${entry.engines.node.replace(/^>=\s*/, "")} or newer`);
  assert.ok(f.hosts.find((h) => h.id === "terminal").commands.includes(f.cli));

  const dir = mkdtempSync(join(tmpdir(), "nage-lock-"));
  try {
    const write = (e) => {
      const p = join(dir, `${Math.random()}.json`);
      writeFileSync(p, JSON.stringify({ packages: e ? { "node_modules/@nanobpm/nano-agent-graph-engine": e } : {} }));
      return p;
    };
    assert.deepEqual(publishedNage(write({ version: "9.9.9", engines: { node: ">=30.1" }, bin: { nage: "x" } })), {
      version: "9.9.9",
      node: "Node 30.1 or newer",
      cli: "nage",
    });
    assert.throws(() => publishedNage(write(null)), /not in/);
    assert.throws(() => publishedNage(write({ engines: { node: ">=22" }, bin: { nage: "x" } })), /lacks/);
    assert.throws(() => publishedNage(write({ version: "1.0.0", bin: { nage: "x" } })), /lacks/);
    assert.throws(() => publishedNage(write({ version: "1.0.0", engines: { node: "^22" }, bin: { nage: "x" } })), /lacks/);
    assert.throws(() => publishedNage(write({ version: "1.0.0", engines: { node: ">=22" }, bin: { age: "x" } })), /lacks/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// Selecting an example card must not start the WASM demo hidden in its
// (unselected) Running model sub-tab: the tab script only loads the selected
// panel's OWN iframes.
test("tab script lazy-loads only iframes owned by the selected tabpanel", () => {
  const html = buildSite()("index.html");
  assert.match(html, /frame\.closest\('\[role="tabpanel"\]'\) !== panel\) return;/);
  assert.doesNotMatch(html, /var frame = panel\.querySelector\("iframe\[data-src\]"\)/);
});
