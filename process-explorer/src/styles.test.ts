import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";

const dir = new URL("./", import.meta.url);
const css = readFileSync(new URL("styles.css", dir), "utf8");
const defined = new Set(
  [...css.matchAll(/\.(nano-[a-z0-9-]+)/g)].map((m) => m[1]),
);

// Class-name literals in component source: `nano-foo` inside a string/template.
// `nano-badge-${…}` template prefixes are expanded against the defined set.
const CLASS_RE = /["'`\s](nano-[a-z0-9-]+)(?=["'`\s]|\$\{)/g;
const NOT_CLASSES = new Set(["nano-app-schema", "nano-bpm"]);

test("every nano-* class the package's components use ships in styles.css", () => {
  const missing: string[] = [];
  for (const f of readdirSync(dir)) {
    if (!f.endsWith(".tsx") || f.includes(".test.")) continue;
    const src = readFileSync(new URL(f, dir), "utf8");
    for (const m of src.matchAll(CLASS_RE)) {
      const cls = m[1];
      if (NOT_CLASSES.has(cls)) continue;
      const isPrefix = src.startsWith("${", m.index + 1 + cls.length);
      const ok = isPrefix
        ? [...defined].some((d) => d.startsWith(cls))
        : defined.has(cls);
      if (!ok) missing.push(`${f}: ${cls}`);
    }
  }
  assert.deepEqual(missing, []);
});

test("styles.css is exported for hosts", () => {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", dir), "utf8"));
  assert.equal(pkg.exports["./styles.css"], "./src/styles.css");
});
