// Node module-customization hooks for the console's `node --test` unit tests
// (registered via `--import ./scripts/register-test-hooks.ts` in `npm test`).
//
// Why: the generated API clients (`src/gen`, `src/gen-c8`) are written for a
// bundler (Vite) and import each other with extensionless specifiers such as
// `from './client'` or `from '../core/types.gen'`. Plain
// `node --experimental-strip-types` cannot resolve those, so a unit test that
// imports the REAL generated client (the regression guard for issue #1350)
// fails with ERR_MODULE_NOT_FOUND / ERR_UNSUPPORTED_DIR_IMPORT. These hooks
// mirror what Vite does for the app: retry a failed relative resolution with
// explicit `.ts` / `.tsx` / `/index.ts` candidates. Test files and the
// console's own `src/lib` code already use explicit `.ts` extensions, so this
// only ever kicks in for the generated code (and any future bundler-style
// import a test pulls in).
//
// The `load` hook additionally teaches node to read `.tsx` modules: component
// unit tests (e.g. ConfigModal.test.tsx, the configuration-save-gate
// regression) render real components with react-dom/server, and
// `--experimental-strip-types` refuses the `.tsx` extension outright. esbuild
// (already a transitive devDependency via Vite — the app's bundler) transpiles
// the module to ESM with the automatic JSX runtime, mirroring the app's build.
import { existsSync, statSync } from "node:fs";
import { register } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const hookSource = `
import { existsSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export async function resolve(specifier, context, nextResolve) {
  try {
    return await nextResolve(specifier, context);
  } catch (err) {
    if (
      (err.code === "ERR_MODULE_NOT_FOUND" ||
        err.code === "ERR_UNSUPPORTED_DIR_IMPORT") &&
      (specifier.startsWith("./") || specifier.startsWith("../"))
    ) {
      const base = dirname(fileURLToPath(context.parentURL));
      const abs = join(base, specifier);
      for (const cand of [abs + ".ts", abs + ".tsx", join(abs, "index.ts")]) {
        if (existsSync(cand) && !statSync(cand).isDirectory()) {
          return { url: pathToFileURL(cand).href, shortCircuit: true };
        }
      }
    }
    throw err;
  }
}

export async function load(url, context, nextLoad) {
  if (url.endsWith(".tsx")) {
    const { readFile } = await import("node:fs/promises");
    const { createRequire } = await import("node:module");
    // Bare "esbuild" must resolve against the console's node_modules, not the
    // hooks worker's data:-URL context (which has no resolution base).
    const require = createRequire(pathToFileURL(process.cwd() + "/"));
    const esbuildUrl = pathToFileURL(require.resolve("esbuild")).href;
    const { transform } = await import(esbuildUrl);
    const source = await readFile(fileURLToPath(url), "utf8");
    const { code } = await transform(source, {
      loader: "tsx",
      jsx: "automatic",
      format: "esm",
      target: "es2022",
      sourcemap: "inline",
      sourcefile: fileURLToPath(url),
    });
    return { format: "module", source: code, shortCircuit: true };
  }
  return nextLoad(url, context);
}
`;

register(`data:text/javascript,${encodeURIComponent(hookSource)}`);
