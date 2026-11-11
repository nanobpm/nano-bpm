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
