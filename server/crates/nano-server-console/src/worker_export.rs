//! "Export workers as an application" — bundles a selection of the console's
//! embedded workers into a **standalone, runnable Node application**, downloaded
//! as a single `.zip` the user can run and manage themselves.
//!
//! The generated app is self-contained:
//!
//! ```text
//! nano-workers-app/
//!   README.md              how to run it
//!   package.json           npm identity + `start` script (the documented entry)
//!   node.json              import map declaring each worker's npm dependencies
//!   node-register.mjs      `--import` bootstrap that registers the loader
//!   node-loader.mjs        ESM loader that makes Node honour node.json's imports
//!   main.ts                entrypoint: deploy resources/*.bpmn, then run the workers
//!   sdk/worker-sdk.ts      the embedded Nano worker SDK (verbatim)
//!   workers/<name>/…       one folder per selected worker (worker.ts + helpers)
//!   resources/             the user drops their .bpmn models here (deployed on startup)
//! ```
//!
//! On startup `main.ts` deploys every `.bpmn` in `resources/` to the gateway
//! (`POST /v2/deployments`, idempotent) and then starts each bundled worker,
//! which connects to the gateway's Falcon protocol and processes its job type.
//!
//! The archive is written by a tiny self-contained **STORED** (uncompressed) zip
//! builder so the server needs no zip crate — the bundled files are small text.

use std::collections::BTreeSet;

use super::workspace;

/// The worker SDK source, baked into the binary (the same file the supervisor
/// materialises to disk). Shipped verbatim into the exported app's `sdk/` so it
/// stays a single source of truth with the running server.
const WORKER_SDK_TS: &str = include_str!("worker_sdk.ts");

/// The Node ESM loader + its `--import` bootstrap, baked into the binary and
/// materialised into every scaffolded project's `nano-generated/` (ADR 0065).
/// Shipped verbatim into the exported app so its `node.json` import map resolves
/// bare specifiers exactly as it does in a managed project — no second, drifting
/// loader implementation.
const NODE_LOADER_MJS: &str = include_str!("node_loader.mjs");
const NODE_REGISTER_MJS: &str = include_str!("node_register.mjs");

/// The vendored `@nanobpm/urban` public type surface, baked into the binary and
/// served to the Studio editor so Monaco resolves an Urban app's `@nanobpm/urban`
/// imports with full IntelliSense. See `scripts/vendor-urban-types.md`.
const URBAN_TYPES_DTS: &str = include_str!("urban_types.d.ts");

/// Top-level folder the zip extracts into (so unzipping never splats into cwd).
const APP_ROOT: &str = "nano-workers-app";

/// Build the standalone-application zip for the given worker names. Returns the
/// zip bytes, or an error string (bad/empty selection, unreadable worker).
pub fn build_app(worker_names: &[String]) -> Result<Vec<u8>, String> {
    if worker_names.is_empty() {
        return Err("select at least one worker to export".into());
    }

    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    let at = |path: &str| format!("{APP_ROOT}/{path}");

    // npm packages the selected workers import, gathered for the generated
    // dependency manifest (node.json import map).
    let mut packages: BTreeSet<String> = BTreeSet::new();

    // Validate + collect each worker's source files (skip its node.json — the app
    // uses a single root import map instead).
    let mut valid_names: Vec<String> = Vec::new();
    for name in worker_names {
        let Some(dir) = workspace::worker_dir(name) else {
            return Err(format!("invalid worker name: {name}"));
        };
        if !dir.is_dir() {
            return Err(format!("no such worker: {name}"));
        }
        let files = workspace::list_worker_files(name)
            .map_err(|e| format!("could not read worker '{name}': {e}"))?;
        let mut copied_entrypoint = false;
        for file in files {
            if file == "node.json" {
                continue;
            }
            let Some(path) = workspace::worker_file_path(name, &file) else {
                continue;
            };
            let bytes =
                std::fs::read(&path).map_err(|e| format!("could not read {name}/{file}: {e}"))?;
            if file == "worker.ts" {
                copied_entrypoint = true;
            }
            // Scan TS/JS sources for imported npm packages.
            if is_source_file(&file)
                && let Ok(text) = std::str::from_utf8(&bytes)
            {
                collect_packages(text, &mut packages);
            }
            entries.push((at(&format!("workers/{name}/{file}")), bytes));
        }
        if !copied_entrypoint {
            return Err(format!("worker '{name}' has no worker.ts entrypoint"));
        }
        valid_names.push(name.clone());
    }

    // Bundle the shared library (`<workspace>/lib/`) so workers that `import
    // "@lib/…"` keep working in the exported app. Scanned for npm packages too,
    // since a shared module may pull in its own dependencies.
    let lib_files = workspace::list_lib_files().unwrap_or_default();
    let has_lib = !lib_files.is_empty();
    for file in &lib_files {
        let Some(path) = workspace::lib_file_path(file) else {
            continue;
        };
        let bytes = std::fs::read(&path).map_err(|e| format!("could not read lib/{file}: {e}"))?;
        if is_source_file(file)
            && let Ok(text) = std::str::from_utf8(&bytes)
        {
            collect_packages(text, &mut packages);
        }
        entries.push((at(&format!("lib/{file}")), bytes));
    }

    // Generated app scaffolding.
    entries.push((at("sdk/worker-sdk.ts"), WORKER_SDK_TS.as_bytes().to_vec()));
    entries.push((at("node.json"), node_json(&packages, has_lib).into_bytes()));
    entries.push((at("package.json"), package_json(&packages).into_bytes()));
    entries.push((
        at("node-register.mjs"),
        NODE_REGISTER_MJS.as_bytes().to_vec(),
    ));
    entries.push((at("node-loader.mjs"), NODE_LOADER_MJS.as_bytes().to_vec()));
    entries.push((at("main.ts"), main_ts(&valid_names).into_bytes()));
    entries.push((at("README.md"), readme_md(&valid_names).into_bytes()));
    entries.push((
        at("resources/README.txt"),
        RESOURCES_README.as_bytes().to_vec(),
    ));

    // Stable order so the archive is deterministic.
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(build_stored_zip(&entries))
}

/// Suggested download filename.
pub fn zip_filename() -> &'static str {
    "nano-workers-app.zip"
}

/// The embedded worker SDK source. Served to the console editor so Monaco can
/// offer full IntelliSense for `@nanobpm/worker`, and shipped verbatim into
/// exported apps — a single source of truth for all three consumers.
pub fn worker_sdk_source() -> &'static str {
    WORKER_SDK_TS
}

/// The vendored `@nanobpm/urban` type surface, served to the Studio editor as a
/// Monaco extra-lib so an Urban app's `@nanobpm/urban` imports (`AppJobHandler`,
/// `OperationHandler`, `runFromEnv`, `selectHost`, the engine client, …) resolve
/// with full IntelliSense — fully offline, like the worker SDK.
pub fn urban_types_source() -> &'static str {
    URBAN_TYPES_DTS
}

const NODE_JSON_HEAD: &str =
    "{\n  \"imports\": {\n    \"@nanobpm/worker\": \"./sdk/worker-sdk.ts\"";

/// Build the app's `node.json`: a pure **import map** (no scripts — `start`
/// lives once in `package.json`, so the run command cannot drift). It aliases the
/// bundled worker SDK under `@nanobpm/worker`, the shared library under `@lib/`
/// when present, and every npm package the selected workers import, mapped to its
/// `npm:` specifier (plus a trailing-slash subpath form) so the Node loader
/// resolves bare imports without edits.
fn node_json(packages: &BTreeSet<String>, has_lib: bool) -> String {
    let mut s = String::from(NODE_JSON_HEAD);
    if has_lib {
        s.push_str(",\n    ");
        s.push_str(&format!(
            "{}: {}",
            json_string("@lib/"),
            json_string("./lib/")
        ));
    }
    for pkg in packages {
        let key = json_string(pkg);
        let val = json_string(&format!("npm:{pkg}"));
        let key_slash = json_string(&format!("{pkg}/"));
        let val_slash = json_string(&format!("npm:/{pkg}/"));
        s.push_str(",\n    ");
        s.push_str(&format!("{key}: {val}"));
        s.push_str(",\n    ");
        s.push_str(&format!("{key_slash}: {val_slash}"));
    }
    s.push_str("\n  }\n}\n");
    s
}

/// The exported app's `package.json`. The documented run flow is `npm install`
/// (materialises the `node.json` import map's `npm:` deps into `node_modules`)
/// then `npm start`, so this is where both the dependency list and the
/// `--import` loader bootstrap live.
///
/// The loader is what makes Node honour `node.json`'s import map: `npm start`
/// runs Node with `--import ./node-register.mjs`, which registers
/// `node-loader.mjs` on the module-resolution thread. Without it the
/// `@nanobpm/worker` alias (and every `npm:` alias) would not resolve, so the
/// bundled `node.json` would be inert.
///
/// Dependencies **derive** from the same import map the loader reads — a single
/// source of truth: each `npm:` value is reduced to its installable coordinate by
/// [`super::projects::npm_dep_from_import`], the one grammar reader shared with
/// the managed-project path.
fn package_json(packages: &BTreeSet<String>) -> String {
    let mut deps: Vec<String> = Vec::new();
    // The SDK alias the bundled workers import lives in `node.json` too, but it
    // maps to the bundled `./sdk/worker-sdk.ts`, so only `npm:` values install.
    for pkg in packages {
        if let Some((name, range)) = super::projects::npm_dep_from_import(&format!("npm:{pkg}")) {
            deps.push(format!(
                "    {}: {}",
                json_string(&name),
                json_string(&range)
            ));
        }
    }
    let dependencies = if deps.is_empty() {
        String::new()
    } else {
        format!("  \"dependencies\": {{\n{}\n  }},\n", deps.join(",\n"))
    };
    let dev_dependencies = format!(
        "  \"devDependencies\": {{\n    {}: \"^22\"\n  }}\n",
        json_string("@types/node")
    );
    format!(
        r#"{{
  "name": "nano-workers-app",
  "version": "0.0.0",
  "private": true,
  "type": "module",
  "main": "main.ts",
  "engines": {{
    "node": ">=22.6"
  }},
  "scripts": {{
    "start": "node --experimental-strip-types --import ./node-register.mjs main.ts"
  }},
{dependencies}{dev_dependencies}}}
"#
    )
}

/// True for TypeScript/JavaScript files whose imports we scan.
fn is_source_file(file: &str) -> bool {
    let f = file.to_ascii_lowercase();
    [".ts", ".tsx", ".mts", ".cts", ".js", ".jsx", ".mjs", ".cjs"]
        .iter()
        .any(|ext| f.ends_with(ext))
}

/// Scan a TS/JS source for imported npm package base names, adding each to `out`.
fn collect_packages(source: &str, out: &mut BTreeSet<String>) {
    for spec in import_specifiers(source) {
        if let Some(pkg) = npm_package_name(&spec) {
            out.insert(pkg);
        }
    }
}

/// Extract module specifiers from `from "x"`, `import("x")`, and side-effect
/// `import "x"` forms. Pragmatic (string-level, not a full parser) — worker
/// files are small, developer-authored sources, so this is sufficient.
fn import_specifiers(src: &str) -> Vec<String> {
    let b = src.as_bytes();
    let mut specs = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if matches_word(b, i, b"from") {
            if let Some((spec, j)) = read_string_after(src, i + 4) {
                specs.push(spec);
                i = j;
                continue;
            }
        } else if matches_word(b, i, b"import") {
            let mut k = i + 6;
            while k < b.len() && (b[k] as char).is_whitespace() {
                k += 1;
            }
            if k < b.len() && b[k] == b'(' {
                // dynamic import("x")
                if let Some((spec, j)) = read_string_after(src, k + 1) {
                    specs.push(spec);
                    i = j;
                    continue;
                }
            } else if k < b.len() && (b[k] == b'"' || b[k] == b'\'' || b[k] == b'`') {
                // side-effect import "x"
                if let Some((spec, j)) = read_string_after(src, k) {
                    specs.push(spec);
                    i = j;
                    continue;
                }
            }
        }
        i += 1;
    }
    specs
}

/// `kw` occurs at `i` on identifier-character word boundaries.
fn matches_word(b: &[u8], i: usize, kw: &[u8]) -> bool {
    if i + kw.len() > b.len() || &b[i..i + kw.len()] != kw {
        return false;
    }
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    let before_ok = i == 0 || !ident(b[i - 1]);
    let after_ok = i + kw.len() == b.len() || !ident(b[i + kw.len()]);
    before_ok && after_ok
}

/// Skip whitespace from `pos`, then read a single-quoted/double-quoted/backtick
/// string literal. Returns its contents and the index just past the close quote.
fn read_string_after(src: &str, pos: usize) -> Option<(String, usize)> {
    let b = src.as_bytes();
    let mut i = pos;
    while i < b.len() && (b[i] as char).is_whitespace() {
        i += 1;
    }
    if i >= b.len() {
        return None;
    }
    let quote = b[i];
    if quote != b'"' && quote != b'\'' && quote != b'`' {
        return None;
    }
    i += 1;
    let start = i;
    while i < b.len() && b[i] != quote {
        i += 1;
    }
    if i >= b.len() {
        return None;
    }
    Some((src[start..i].to_string(), i + 1))
}

/// Reduce an import specifier to its npm base package name, or `None` for
/// imports that are not npm-resolvable (relative, URL, `jsr:`, `node:`, the
/// worker SDK alias, …).
fn npm_package_name(spec: &str) -> Option<String> {
    let s = spec.trim();
    let s = s.strip_prefix("npm:").unwrap_or(s);
    if s.is_empty() || s.starts_with('.') || s.starts_with('/') {
        return None;
    }
    for prefix in ["http:", "https:", "jsr:", "node:", "file:", "data:"] {
        if s.starts_with(prefix) {
            return None;
        }
    }
    if s == "@nanobpm/worker" {
        return None;
    }
    // `@lib/…` is the shared-library alias (mapped to ./lib/), not an npm package.
    if s == "@lib" || s.starts_with("@lib/") {
        return None;
    }
    let base = if let Some(rest) = s.strip_prefix('@') {
        // @scope/name[/sub][@version]
        let (scope, after) = rest.split_once('/')?;
        let name = after.split('/').next().unwrap_or(after);
        let name = name.split('@').next().unwrap_or(name);
        if scope.is_empty() || name.is_empty() {
            return None;
        }
        format!("@{scope}/{name}")
    } else {
        // name[/sub][@version]
        let first = s.split('/').next().unwrap_or(s);
        first.split('@').next().unwrap_or(first).to_string()
    };
    if base.is_empty() { None } else { Some(base) }
}

const RESOURCES_README: &str = "Drop the .bpmn model files your workers serve into this folder.\n\
Every .bpmn here is deployed to the gateway when the app starts (idempotently).\n";

/// The app entrypoint: deploy `resources/*.bpmn`, then start each worker.
fn main_ts(names: &[String]) -> String {
    let list = names
        .iter()
        .map(|n| format!("  {}", json_string(n)))
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        r#"// Standalone nano workers application — generated by the Nano Console.
//
// On startup it (1) deploys every BPMN model in ./resources to the gateway, then
// (2) starts each bundled worker, which connects to the gateway's Falcon protocol
// and processes its job type. Point it at your gateway with NANOBPMN_BASE_URL
// (default http://127.0.0.1:8080). Run it with `npm start`.

import {{ readdir, readFile }} from "node:fs/promises";

const BASE_URL = (process.env.NANOBPMN_BASE_URL ?? "http://127.0.0.1:8080").replace(/\/+$/, "");

// One folder under ./workers per worker bundled into this app.
const WORKERS: string[] = [
{list}
];

async function deployResources(): Promise<void> {{
  let models: string[];
  try {{
    models = (await readdir("resources", {{ withFileTypes: true }}))
      .filter((e) => e.isFile() && e.name.toLowerCase().endsWith(".bpmn"))
      .map((e) => e.name)
      .sort();
  }} catch {{
    console.log("[deploy] no ./resources directory — nothing to deploy");
    return;
  }}
  if (models.length === 0) {{
    console.log("[deploy] no .bpmn models in ./resources — add your models there and restart");
    return;
  }}
  for (const name of models) {{
    try {{
      const xml = await readFile(`resources/${{name}}`, "utf8");
      const form = new FormData();
      form.append("resources", new Blob([xml], {{ type: "text/xml" }}), name);
      const res = await fetch(`${{BASE_URL}}/v2/deployments`, {{ method: "POST", body: form }});
      if (res.ok) {{
        console.log(`[deploy] ${{name}} ok`);
      }} else {{
        const detail = await res.text().catch(() => "");
        console.error(`[deploy] ${{name}} failed: HTTP ${{res.status}} ${{detail}}`);
      }}
    }} catch (err) {{
      console.error(`[deploy] ${{name}} failed: ${{err instanceof Error ? err.message : err}}`);
    }}
  }}
}}

async function startWorkers(): Promise<void> {{
  for (const name of WORKERS) {{
    try {{
      // Label this worker's falcon activations with its folder name.
      process.env.NANOBPMN_WORKER_NAME = name;
      await import(`./workers/${{name}}/worker.ts`);
      console.log(`[worker] ${{name}} started`);
    }} catch (err) {{
      console.error(`[worker] ${{name}} failed to start: ${{err instanceof Error ? err.message : err}}`);
    }}
  }}
}}

console.log(`nano workers app -> gateway ${{BASE_URL}}`);
await deployResources();
await startWorkers();
console.log(`${{WORKERS.length}} worker(s) running. Press Ctrl+C to stop.`);
"#
    )
}

/// The exported app's README.
fn readme_md(names: &[String]) -> String {
    let worker_list = names
        .iter()
        .map(|n| format!("- `{n}`"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"# Nano workers application

A standalone bundle of job workers exported from the **Nano Console**. It connects
to a running **Nano BPM** gateway, deploys your BPMN models, and runs the workers —
no Console required.

## Prerequisites

- **[Node](https://nodejs.org) >= 22.6** — the runtime the workers run on.
- A running **Nano BPM** gateway (default `http://127.0.0.1:8080`).

## 1. Add your models

Drop the `.bpmn` files your workers serve into the **`resources/`** folder. Every
model there is deployed to the gateway on startup (deployment is idempotent, so
restarting safely re-deploys).

## 2. Point at your gateway (optional)

Defaults to `http://127.0.0.1:8080`. Override with an environment variable:

```sh
export NANOBPMN_BASE_URL=http://my-gateway:8080
```

## 3. Run

```sh
npm install
npm start
```

(equivalently: `node --experimental-strip-types --import ./node-register.mjs main.ts`)

The app deploys `resources/*.bpmn`, then starts these workers:

{worker_list}

Each worker connects to the gateway's Falcon protocol and processes its job type.
Workers print periodic telemetry lines (prefixed `@@NBPM_`, used by the Console)
alongside their own `console.log` output. Press **Ctrl+C** to stop.

## Layout

```text
main.ts              entrypoint (deploy models, then run workers)
package.json         npm identity + `start` script (this is the entrypoint)
node.json            import map declaring each worker's npm dependencies
node-register.mjs    `--import` bootstrap that registers the loader below
node-loader.mjs      ESM loader that makes Node honour node.json's imports
sdk/worker-sdk.ts    the embedded Nano worker SDK
lib/                 shared library modules (imported as `@lib/…`, if any)
workers/<name>/      one folder per worker (worker.ts + any helpers)
resources/           drop your .bpmn models here
```

## Editing workers

Each `workers/<name>/worker.ts` imports `@nanobpm/worker` and calls `defineWorker`.
Edit the handler, add `npm:` dependencies as needed, and re-run. A new `npm:`
dependency must be added to **both** `node.json` (the import map the loader reads)
and `package.json` (so `npm install` fetches it) — the two are paired by hand.
"#
    )
}

// ---------------------------------------------------------------------------
// Minimal STORED (uncompressed) zip writer — no external zip crate needed.
// ---------------------------------------------------------------------------

/// Build a zip archive from `(path, bytes)` entries using STORED (no
/// compression). The bundled files are small text, so compression buys little
/// and a dependency-free writer keeps the gateway lean.
pub(crate) fn build_stored_zip(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
    // Fixed DOS timestamp (1980-01-01 00:00) — valid and deterministic.
    const DOS_DATE: u16 = 0x0021;
    const DOS_TIME: u16 = 0x0000;
    // General-purpose bit 11: filenames are UTF-8.
    const FLAG_UTF8: u16 = 0x0800;

    let mut out: Vec<u8> = Vec::new();
    let mut central: Vec<u8> = Vec::new();

    for (name, data) in entries {
        let name_bytes = name.as_bytes();
        let crc = crc32fast::hash(data);
        let size = data.len() as u32;
        let offset = out.len() as u32;

        // Local file header.
        out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        out.extend_from_slice(&20u16.to_le_bytes()); // version needed
        out.extend_from_slice(&FLAG_UTF8.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // method: stored
        out.extend_from_slice(&DOS_TIME.to_le_bytes());
        out.extend_from_slice(&DOS_DATE.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes()); // compressed
        out.extend_from_slice(&size.to_le_bytes()); // uncompressed
        out.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // extra len
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(data);

        // Central directory record.
        central.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        central.extend_from_slice(&20u16.to_le_bytes()); // version made by
        central.extend_from_slice(&20u16.to_le_bytes()); // version needed
        central.extend_from_slice(&FLAG_UTF8.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes()); // method
        central.extend_from_slice(&DOS_TIME.to_le_bytes());
        central.extend_from_slice(&DOS_DATE.to_le_bytes());
        central.extend_from_slice(&crc.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes()); // extra len
        central.extend_from_slice(&0u16.to_le_bytes()); // comment len
        central.extend_from_slice(&0u16.to_le_bytes()); // disk number start
        central.extend_from_slice(&0u16.to_le_bytes()); // internal attrs
        central.extend_from_slice(&0u32.to_le_bytes()); // external attrs
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(name_bytes);
    }

    let central_offset = out.len() as u32;
    let central_size = central.len() as u32;
    let count = entries.len() as u16;
    out.extend_from_slice(&central);

    // End of central directory.
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // disk number
    out.extend_from_slice(&0u16.to_le_bytes()); // disk with cd
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&central_size.to_le_bytes());
    out.extend_from_slice(&central_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // comment len
    out
}

/// JSON-encode a string (for embedding worker names in generated TS).
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_selection_is_rejected() {
        assert!(build_app(&[]).is_err());
    }

    #[test]
    fn json_string_escapes() {
        assert_eq!(json_string("a-b_1"), "\"a-b_1\"");
        assert_eq!(json_string("a\"b\\c"), "\"a\\\"b\\\\c\"");
    }

    #[test]
    fn main_ts_lists_workers_and_deploys() {
        let ts = main_ts(&["alpha".into(), "beta".into()]);
        assert!(ts.contains("\"alpha\""));
        assert!(ts.contains("\"beta\""));
        assert!(ts.contains("/v2/deployments"));
        assert!(ts.contains("./workers/${name}/worker.ts"));
    }

    #[test]
    fn stored_zip_has_valid_signatures_and_eocd() {
        let entries = vec![
            ("nano-workers-app/main.ts".to_string(), b"hello".to_vec()),
            ("nano-workers-app/README.md".to_string(), b"# hi".to_vec()),
        ];
        let zip = build_stored_zip(&entries);
        // Local file header signature at the very start.
        assert_eq!(&zip[0..4], &0x0403_4b50u32.to_le_bytes());
        // End-of-central-directory signature appears near the end.
        let eocd = &0x0605_4b50u32.to_le_bytes();
        assert!(
            zip.windows(4).any(|w| w == eocd),
            "EOCD signature must be present"
        );
        // Entry count in EOCD (last 22 bytes) must be 2.
        let tail = &zip[zip.len() - 22..];
        let count = u16::from_le_bytes([tail[10], tail[11]]);
        assert_eq!(count, 2);
    }

    #[test]
    fn collects_npm_packages_from_imports() {
        let src = r#"
            import { defineWorker } from "@nanobpm/worker";
            import { Camunda8 } from "@camunda8/orchestration-cluster-api";
            import _ from "npm:lodash@4.17.21";
            import "side-effect-pkg";
            const x = await import("@scope/dyn/sub");
            import rel from "./helper.ts";
            import shared from "@lib/money.ts";
            import url from "https://example.com/x.ts";
            import sub from "lodash/fp";
        "#;
        let mut got = BTreeSet::new();
        collect_packages(src, &mut got);
        // @nanobpm/worker and relative/URL imports are excluded; scoped + subpath
        // imports reduce to their base package; npm: prefix + version stripped.
        let want: BTreeSet<String> = [
            "@camunda8/orchestration-cluster-api",
            "lodash",
            "side-effect-pkg",
            "@scope/dyn",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn npm_package_name_extracts_base() {
        assert_eq!(npm_package_name("lodash").as_deref(), Some("lodash"));
        assert_eq!(npm_package_name("lodash/fp").as_deref(), Some("lodash"));
        assert_eq!(npm_package_name("npm:chalk@5").as_deref(), Some("chalk"));
        assert_eq!(
            npm_package_name("@camunda8/sdk/foo").as_deref(),
            Some("@camunda8/sdk")
        );
        assert_eq!(npm_package_name("./local").as_deref(), None);
        assert_eq!(npm_package_name("node:fs").as_deref(), None);
        assert_eq!(npm_package_name("@nanobpm/worker").as_deref(), None);
        // The shared-library alias is local, not an npm package.
        assert_eq!(npm_package_name("@lib/money.ts").as_deref(), None);
        assert_eq!(npm_package_name("@lib").as_deref(), None);
    }

    #[test]
    fn node_json_emits_dependency_manifest() {
        let mut pkgs = BTreeSet::new();
        pkgs.insert("@camunda8/sdk".to_string());
        pkgs.insert("lodash".to_string());
        let json = node_json(&pkgs, false);
        // SDK alias is always present.
        assert!(json.contains("\"@nanobpm/worker\": \"./sdk/worker-sdk.ts\""));
        // Each package maps to its npm: specifier, plus a subpath form.
        assert!(json.contains("\"@camunda8/sdk\": \"npm:@camunda8/sdk\""));
        assert!(json.contains("\"@camunda8/sdk/\": \"npm:/@camunda8/sdk/\""));
        assert!(json.contains("\"lodash\": \"npm:lodash\""));
        // No shared library here, so no @lib/ alias.
        assert!(!json.contains("@lib/"));
        // Valid JSON.
        assert!(serde_json::from_str::<serde_json::Value>(&json).is_ok());
    }

    #[test]
    fn node_json_without_packages_is_valid() {
        let json = node_json(&BTreeSet::new(), false);
        assert!(serde_json::from_str::<serde_json::Value>(&json).is_ok());
        // node.json is a pure import map: the run command lives once, in
        // package.json, so it cannot drift between the two files.
        assert!(!json.contains("scripts"));
        assert!(!json.contains("node "));
    }

    #[test]
    fn node_json_maps_shared_library_when_present() {
        let json = node_json(&BTreeSet::new(), true);
        // The @lib/ alias points at the bundled lib/ folder.
        assert!(json.contains("\"@lib/\": \"./lib/\""));
        assert!(serde_json::from_str::<serde_json::Value>(&json).is_ok());
    }

    #[test]
    fn package_json_installs_the_import_maps_npm_deps() {
        let mut pkgs = BTreeSet::new();
        pkgs.insert("@camunda8/sdk".to_string());
        pkgs.insert("lodash".to_string());
        let json = package_json(&pkgs);
        let v: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        // Dependencies derive from the same packages node.json aliases.
        assert_eq!(v["dependencies"]["lodash"], "*");
        assert_eq!(v["dependencies"]["@camunda8/sdk"], "*");
        // The documented run flow must carry the loader bootstrap: without
        // `--import ./node-register.mjs` the node.json import map is inert.
        assert_eq!(
            v["scripts"]["start"],
            "node --experimental-strip-types --import ./node-register.mjs main.ts"
        );
        assert_eq!(v["engines"]["node"], ">=22.6");
        assert_eq!(v["devDependencies"]["@types/node"], "^22");
    }

    #[test]
    fn package_json_without_packages_still_runs() {
        let json = package_json(&BTreeSet::new());
        let v: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert!(v.get("dependencies").is_none());
        assert!(
            v["scripts"]["start"]
                .as_str()
                .unwrap()
                .contains("node-register.mjs")
        );
    }

    /// The archive the Console hands the user must carry everything the Run
    /// section documents: an npm manifest, the loader + its bootstrap, and a
    /// `main.ts` that deploys then starts the workers.
    #[test]
    fn exported_app_contains_the_runnable_scaffolding() {
        // The loader + register sources baked into the binary are the real ones
        // (single source of truth with the managed-project path), not placeholders.
        assert!(NODE_LOADER_MJS.contains("export async function resolve"));
        assert!(NODE_REGISTER_MJS.contains("register("));
        // The app root is a single top-level folder so unzipping never splats.
        assert_eq!(APP_ROOT, "nano-workers-app");
    }

    /// End-to-end: `build_app` over a real on-disk workspace must emit, under
    /// `nano-workers-app/`, every file the README documents as runnable — and the
    /// generated `package.json` must carry the `--import` bootstrap. This is the
    /// regression guard for a "downloads but will not run" export (no npm
    /// manifest, no loader → the bundled `node.json` import map is inert).
    ///
    /// `build_app` reads the workspace from `NANOBPMN_WORKSPACE_DIR`, so this is
    /// serialized against every other env-mutating test by [`crate::projects::env_lock`].
    #[test]
    fn build_app_ships_a_runnable_app() {
        let _guard = crate::projects::env_lock();
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "nano-wexport-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("workers/acme")).unwrap();
        std::fs::write(
            root.join("workers/acme/worker.ts"),
            "import { defineWorker } from \"@nanobpm/worker\";\nimport _ from \"npm:lodash@4\";\ndefineWorker({});\n",
        )
        .unwrap();
        let prev = std::env::var("NANOBPMN_WORKSPACE_DIR").ok();
        unsafe { std::env::set_var("NANOBPMN_WORKSPACE_DIR", &root) };

        let zip = build_app(&["acme".to_string()]).expect("build_app succeeds");
        let names = zip_entry_names(&zip);
        for want in [
            "nano-workers-app/package.json",
            "nano-workers-app/node.json",
            "nano-workers-app/node-register.mjs",
            "nano-workers-app/node-loader.mjs",
            "nano-workers-app/main.ts",
            "nano-workers-app/README.md",
            "nano-workers-app/workers/acme/worker.ts",
        ] {
            assert!(
                names.contains(&want.to_string()),
                "missing {want} in {names:?}"
            );
        }

        // The bootstrap the entrypoint needs is really in the manifest.
        let pkg = String::from_utf8(
            read_zip_entry(&zip, "nano-workers-app/package.json").expect("package.json entry"),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&pkg).expect("valid JSON");
        assert!(
            v["scripts"]["start"]
                .as_str()
                .unwrap()
                .contains("--import ./node-register.mjs")
        );
        // Its deps derive from the import map the loader reads: worker_export's
        // node.json aliases `lodash` as `npm:lodash`, so the manifest installs it
        // as `*` — the two files agree rather than drifting apart.
        let nj = String::from_utf8(
            read_zip_entry(&zip, "nano-workers-app/node.json").expect("node.json entry"),
        )
        .unwrap();
        assert!(nj.contains("\"lodash\": \"npm:lodash\""));
        assert_eq!(v["dependencies"]["lodash"], "*");

        match prev {
            Some(v) => unsafe { std::env::set_var("NANOBPMN_WORKSPACE_DIR", v) },
            None => unsafe { std::env::remove_var("NANOBPMN_WORKSPACE_DIR") },
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Enumerate the file names in a STORED zip by walking its central directory.
    fn zip_entry_names(zip: &[u8]) -> Vec<String> {
        let mut names = Vec::new();
        // Locate EOCD (signature 0x06054b50) scanning back over the comment.
        let eocd = zip
            .windows(4)
            .rposition(|w| w == 0x0605_4b50u32.to_le_bytes())
            .expect("EOCD");
        let count = u16::from_le_bytes([zip[eocd + 10], zip[eocd + 11]]) as usize;
        let mut off = u32::from_le_bytes([
            zip[eocd + 16],
            zip[eocd + 17],
            zip[eocd + 18],
            zip[eocd + 19],
        ]) as usize;
        for _ in 0..count {
            assert_eq!(&zip[off..off + 4], &0x0201_4b50u32.to_le_bytes());
            let name_len = u16::from_le_bytes([zip[off + 28], zip[off + 29]]) as usize;
            let extra_len = u16::from_le_bytes([zip[off + 30], zip[off + 31]]) as usize;
            let comment_len = u16::from_le_bytes([zip[off + 32], zip[off + 33]]) as usize;
            let name = String::from_utf8(zip[off + 46..off + 46 + name_len].to_vec()).unwrap();
            names.push(name);
            off += 46 + name_len + extra_len + comment_len;
        }
        names
    }

    /// Read entry `name`'s bytes from a STORED zip via its local file header.
    fn read_zip_entry(zip: &[u8], name: &str) -> Option<Vec<u8>> {
        let mut off = 0usize;
        while off + 4 <= zip.len() {
            if zip[off..off + 4] != 0x0403_4b50u32.to_le_bytes() {
                break;
            }
            let method = u16::from_le_bytes([zip[off + 8], zip[off + 9]]);
            assert_eq!(method, 0, "expected STORED entry");
            let comp =
                u32::from_le_bytes([zip[off + 18], zip[off + 19], zip[off + 20], zip[off + 21]])
                    as usize;
            let name_len = u16::from_le_bytes([zip[off + 26], zip[off + 27]]) as usize;
            let extra_len = u16::from_le_bytes([zip[off + 28], zip[off + 29]]) as usize;
            let entry_name =
                String::from_utf8(zip[off + 30..off + 30 + name_len].to_vec()).unwrap();
            let data_at = off + 30 + name_len + extra_len;
            if entry_name == name {
                return Some(zip[data_at..data_at + comp].to_vec());
            }
            off = data_at + comp;
        }
        None
    }
}
