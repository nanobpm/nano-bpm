// Guards the homepage's Camunda 8 SDK language chips (langChips() in build.mjs):
// every chip ships an inlined logo from website/logos/, every logo there is
// rendered (no orphaned or forgotten files), and inlined logos can't share SVG
// ids. Ids are page-global once inlined, so a duplicate makes one logo silently
// paint with another's gradient.
import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import test from "node:test";

import { buildSite } from "./test-build.mjs";

const LOGOS = new URL("./logos/", import.meta.url);

function chips() {
  const html = buildSite()("index.html");
  const band = html.match(/<div class="provs langs"[^>]*>([\s\S]*?)<\/div>/);
  assert.ok(band, "homepage renders the SDK language band");
  return [...band[1].matchAll(/<span class="lang lang-([a-z]+)">(<svg[\s\S]*?<\/svg>)([^<]+)<\/span>/g)].map(
    ([, id, svg, label]) => ({ id, svg, label: label.trim() }),
  );
}

test("every SDK language chip inlines its logo, and every logo file is used", () => {
  const rendered = chips();
  const files = readdirSync(LOGOS)
    .filter((f) => f.endsWith(".svg"))
    .map((f) => f.slice(0, -4))
    .sort();
  assert.deepEqual(rendered.map((c) => c.id).sort(), files, "chips and website/logos/*.svg match one-to-one");
  for (const c of rendered) assert.match(c.svg, /^<svg aria-hidden="true" focusable="false" /, `${c.label} logo is inlined`);
});

test("the SDK languages include D (camunda/orchestration-cluster-api-dlang)", () => {
  assert.ok(chips().some((c) => c.id === "d" && c.label === "D"), "D chip is rendered");
});

test("inlined language logos don't share SVG ids", () => {
  const seen = new Map();
  for (const c of chips()) {
    for (const [, id] of c.svg.matchAll(/\bid="([^"]+)"/g)) {
      assert.ok(!seen.has(id), `id "${id}" in ${c.label} also appears in ${seen.get(id)}`);
      seen.set(id, c.label);
    }
  }
});
