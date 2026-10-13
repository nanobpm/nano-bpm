// Builds the browser-consumable package artifact from the library source.
//
// The library is authored as raw TypeScript (Node runs it via type-stripping;
// see README). TypeScript/Vite consumers like the console, however, must not
// re-bundle the moddle parsers themselves — dmn-moddle is CJS (no ESM default
// export for Rollup) and its zeebe.json import carries `type: "json"` attributes
// that clash with bpmn-js importing the same file without them. So we ship a
// pre-built, self-contained artifact:
//
//   dist/index.js    — a single minified ESM bundle with every dependency inlined
//                      (esbuild resolves the CJS interop + JSON attributes once,
//                      here, isolating them from the consumer's bundler).
//   dist/index.d.ts  — a single flattened declaration file (dts-bundle-generator).
//   dist/tokens.js   — the ./tokens subpath: the machine-readable --nano-* palette
//   dist/tokens.d.ts   map + its types (issue #1005), a tiny dep-free bundle.
//   tokens.css       — the ./tokens.css subpath: the palette as :root custom
//                      properties, generated from the same map so they can't drift.
//
// dist/ is committed (like console/dist and gen/nano-app.d.ts) so consumers and
// CI (`npm ci`) need no build-on-install step. Regenerate with `npm run build`
// (or `make generate-app-manifest`).

import { build } from "esbuild";
import { generateDtsBundle } from "dts-bundle-generator";
import { fileURLToPath, pathToFileURL } from "node:url";
import { dirname, join } from "node:path";
import { mkdirSync, writeFileSync } from "node:fs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const entry = join(root, "src", "index.ts");
const tokensEntry = join(root, "src", "tokens.ts");
const outDir = join(root, "dist");
mkdirSync(outDir, { recursive: true });

// 1) Runtime bundle: one ESM file, all deps inlined, browser-targeted.
await build({
  entryPoints: [entry],
  outfile: join(outDir, "index.js"),
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  minify: true,
  legalComments: "none",
});
console.log("built dist/index.js");

// 2) Flattened declarations: one .d.ts with the public API, no external
//    relative imports (so consumers resolve types without seeing src/).
const [dts] = generateDtsBundle(
  [{ filePath: entry, output: { noBanner: true, exportReferencedTypes: false } }],
  { preferredConfigPath: join(root, "tsconfig.json") },
);
writeFileSync(join(outDir, "index.d.ts"), dts);
console.log("built dist/index.d.ts");

// 3) The `./tokens` subpath: the machine-readable `--nano-*` palette map
//    (issue #1005). Bundled as its own tiny artifact — pure constants, no deps —
//    so a consumer that only needs the palette (the console drift test, the Urban
//    runtime's gen-runtime inliner) does not pull in the moddle parsers.
await build({
  entryPoints: [tokensEntry],
  outfile: join(outDir, "tokens.js"),
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  minify: true,
  legalComments: "none",
});
console.log("built dist/tokens.js");

const [tokensDts] = generateDtsBundle(
  [{ filePath: tokensEntry, output: { noBanner: true, exportReferencedTypes: false } }],
  { preferredConfigPath: join(root, "tsconfig.json") },
);
writeFileSync(join(outDir, "tokens.d.ts"), tokensDts);
console.log("built dist/tokens.d.ts");

// 4) The `./tokens.css` subpath: the palette rendered as :root custom properties,
//    generated from the same NANO_PALETTE map as the ./tokens export so the CSS
//    artifact and the machine-readable map can never drift. The console @imports
//    this in place of its former inline hex.
const { renderPaletteCss } = await import(pathToFileURL(join(outDir, "tokens.js")).href);
writeFileSync(join(root, "tokens.css"), renderPaletteCss());
console.log("built tokens.css");
