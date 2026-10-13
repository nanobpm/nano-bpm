// Tests for the canonical --nano-* token palette (issue #1005) — `node --test`.
// Pure constants, so this runs CI-safe without a browser or a JS runtime. These lock the
// palette map, the CSS-variable naming, and — critically — that the committed
// `tokens.css` artifact is exactly what the map renders, so the ./tokens.css and
// ./tokens subpath exports can never drift from each other.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import {
  TOKEN_KEYS,
  NANO_PALETTE,
  tokenCssVar,
  isTokenKey,
  renderPaletteCss,
} from "../src/tokens.ts";

const specRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const HEX = /^#[0-9a-f]{6}$/;

test("TOKEN_KEYS is the 18-entry theming vocabulary, unique", () => {
  assert.equal(TOKEN_KEYS.length, 18);
  assert.equal(new Set(TOKEN_KEYS).size, TOKEN_KEYS.length);
});

test("NANO_PALETTE defines every token for both appearances, all 6-digit hex", () => {
  for (const appearance of ["dark", "light"]) {
    const palette = NANO_PALETTE[appearance];
    assert.deepEqual(
      Object.keys(palette).sort(),
      [...TOKEN_KEYS].sort(),
      `${appearance} palette must define exactly the TOKEN_KEYS`,
    );
    for (const key of TOKEN_KEYS) {
      assert.match(palette[key], HEX, `${appearance}.${key} must be #rrggbb`);
    }
  }
});

test("tokenCssVar mirrors the console cssVar() mapping", () => {
  assert.equal(tokenCssVar("app"), "--nano-app");
  assert.equal(tokenCssVar("edgeStrong"), "--nano-edge-strong");
  assert.equal(tokenCssVar("textMuted"), "--nano-text-muted");
  assert.equal(tokenCssVar("accentStrong"), "--nano-accent-strong");
  assert.equal(tokenCssVar("accent2"), "--nano-accent-2");
  assert.equal(tokenCssVar("onAccent"), "--nano-on-accent");
});

test("isTokenKey narrows known keys only", () => {
  assert.ok(isTokenKey("accent2"));
  assert.ok(!isTokenKey("nope"));
});

test("committed tokens.css is exactly what the map renders (no CSS↔map drift)", () => {
  const committed = readFileSync(join(specRoot, "tokens.css"), "utf8");
  assert.equal(
    committed,
    renderPaletteCss(),
    "tokens.css is stale — run `npm run build` after editing src/tokens.ts",
  );
});

test("renderPaletteCss emits both appearances as :root custom properties", () => {
  const css = renderPaletteCss();
  assert.match(css, /:root,\n:root\[data-appearance="dark"\] \{/);
  assert.match(css, /:root\[data-appearance="light"\] \{/);
  assert.ok(css.includes(`--nano-app: ${NANO_PALETTE.dark.app};`));
  assert.ok(css.includes(`--nano-app: ${NANO_PALETTE.light.app};`));
  assert.ok(!css.includes("prefers-color-scheme"), "console CSS has no standalone fallback");
});

test("standaloneFallback adds the OS-follow block for the Urban runtime", () => {
  const css = renderPaletteCss({ standaloneFallback: true });
  assert.match(css, /@media \(prefers-color-scheme: light\)/);
  assert.match(css, /:root:not\(\[data-appearance\]\)/);
});
