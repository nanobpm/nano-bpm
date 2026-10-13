//! The **agent authoring surface** (ADR 0051): a Markdown brief served at
//! `/agent` that teaches an external AI agent — Claude Code, Copilot CLI, or any
//! MCP-driven assistant a user "points here" — everything it needs to
//!
//! 1. **author a Nano App on disk** (outside the console IDE), and
//! 2. **link that app into this running node** by reference (ADR 0041), and,
//! 3. as a by-product, **explain how Nano works** to the user in its own words.
//!
//! The premise (the user's): *if you can explain the thing to an agent, the
//! agent can explain it to the user* — so one good brief satisfies both "let an
//! agent build me an app" and "have an agent tell me how this works". The brief
//! is therefore the single canonical answer to *"what is this and how do I use
//! it"* for a machine reader.
//!
//! It is rendered **live, per node**: the base URL is taken from the request, and
//! the on-disk projects root, installed packs, and scaffold templates are read at
//! request time. An agent pointed at the URL with no other context can act — it
//! knows exactly where the projects root is on this host and exactly which
//! endpoint to POST an import to.
//!
//! A companion `/llms.txt` (the emerging convention for "machine-readable site
//! index") points tooling at this brief plus the OpenAPI/AsyncAPI/JSON-Schema
//! specs, so discovery does not depend on a human reading the console UI.

use std::fmt::Write as _;

use super::{extensions, projects};

/// The canonical App-manifest JSON Schema, served from the owned namespace home
/// (nanobpm.io, per the published-schemas convention) so an editor or agent can
/// use it for `nano.app.json` autocompletion and validation.
pub const APP_SCHEMA_URL: &str = "https://nanobpm.io/spec-app/nano-app.schema.json";

/// Human-facing name of an extension-pack kind, for the "installed packs" table.
fn kind_label(kind: &extensions::ExtKind) -> &'static str {
    use extensions::ExtKind::*;
    match kind {
        Lang => "language",
        App => "app/runtime",
        Example => "example app",
        Theme => "theme",
        Trigger => "trigger source",
    }
}

/// Render the full agent brief for a node reachable at `base_url`
/// (e.g. `http://localhost:8080`, no trailing slash).
///
/// Everything node-specific — the projects root on disk, the import endpoint,
/// the installed packs and scaffold templates — is resolved here, at request
/// time, so the document an agent reads is true for *this* running node.
pub fn render(base_url: &str) -> String {
    let projects_root = projects::projects_root();
    let projects_root = projects_root.display();
    let import_url = format!("{base_url}/console/api/projects/import");
    let server_version = env!("CARGO_PKG_VERSION");

    let mut m = String::new();

    let _ = write!(
        m,
        r#"# Nano — agent authoring brief

You are an AI agent a user has pointed at a running **Nano** node. This document
is written **for you, the agent**. It tells you how to do two things the user
wants:

1. **Author a Nano application on disk** — outside the console's built-in editors —
   and **link it into this node** so it appears in their Projects gallery and runs.
2. **Explain how Nano works** to the user. Everything below is accurate for *this*
   node; once you understand it you can answer the user's "how does this work?" in
   your own words.

If the user only asked "how does this work?", read this whole brief and explain it.
If they asked you to *build* something, jump to **Author an app** and **Link it in**.

Nano is a distillation of Camunda's **Zeebe** workflow engine, rewritten in Rust and
shrunk to a single self-contained binary. It runs **BPMN** processes and **DMN**
decisions natively (no JVM), evaluates **FEEL** expressions, and its Rapid
Application Development face — codenamed **Urban** — binds triggers, a process,
decisions, forms, pages and data into one **App** artifact. Think Borland Delphi:
the *binding* is the product.

---

## This node (live)

| Fact | Value |
|------|-------|
| Base URL | `{base_url}` |
| Console (human UI) | `{base_url}/console` |
| Projects root (on this host's disk) | `{projects_root}` |
| Import endpoint (link an app in) | `POST {import_url}` |
| Engine REST (Camunda 8 v2 API) | `{base_url}/v2` |
| App manifest JSON Schema | `{APP_SCHEMA_URL}` |
| OpenAPI (console API) | `{base_url}/swagger` |
| AsyncAPI (command stream) | `{base_url}/asyncapi` |
| Human docs | `{base_url}/docs` |
| Server build | `nano {server_version}` |

"#,
    );

    // ---- installed packs (live) -----------------------------------------
    let packs: Vec<_> = extensions::all_extensions()
        .into_iter()
        .filter(|e| !e.builtin)
        .collect();
    m.push_str("### Installed extension packs\n\n");
    if packs.is_empty() {
        m.push_str(
            "None installed beyond the built-ins. Packs (npm packages tagged \
             `nano-ide-ext`) add languages, app templates, example apps, trigger \
             sources and themes.\n\n",
        );
    } else {
        m.push_str("| Pack | Kind |\n|------|------|\n");
        for e in &packs {
            let _ = writeln!(
                m,
                "| `{}` — {} | {} |",
                e.id,
                e.display_name,
                kind_label(&e.kind)
            );
        }
        m.push('\n');
    }

    // ---- scaffold templates (live) --------------------------------------
    let templates = projects::project_templates();
    m.push_str("### Scaffold templates offered by this node\n\n");
    if templates.is_empty() {
        m.push_str("_none_\n\n");
    } else {
        m.push_str("| Template id | Language | Description |\n|---|---|---|\n");
        for t in &templates {
            let id = t.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let lang = t.get("lang").and_then(|v| v.as_str()).unwrap_or("");
            let desc = t
                .get("description")
                .and_then(|v| v.as_str())
                .or_else(|| t.get("label").and_then(|v| v.as_str()))
                .unwrap_or("");
            let _ = writeln!(m, "| `{id}` | {lang} | {desc} |");
        }
        m.push('\n');
    }

    m.push_str(&format!(
        r#"---

## What a Nano App is

A **Nano App** (Urban, ADR 0022/0027) is a directory headed by a **`nano.app.json`**
manifest — pure declared data, no code in the manifest. The manifest binds:

- **`data`** — named datasources + a domain **type registry** (records with fields).
- **`triggers`** — event sources (cron, webhook, file, or pack sources like mqtt/imap)
  bound to process starts.
- **`models`** — globs pointing at the BPMN/DMN/form files the editors produce.
- **`workers` / `llm`** — service-task handlers (referenced files) and LLM bindings.
- **`actions`** — named app entrypoints (referenced handler files) that start or signal
  a process — invoked from a button, webhook or trigger.
- **`surfaces` / `pages`** — forms and Page-Composer pages over the domain.

An optional top-level **`entrypoint`** (e.g. `"src/main.ts"`) overrides the default
`main.ts` the Studio supervisor runs/compiles, so an app whose real entrypoint is not a
root `main.ts` is self-describing and needs no placeholder root shim (#957).

The manifest is the source of truth; the TypeScript types and the `$schema` an
editor uses for autocompletion are generated from it. Point your `$schema` at
`{APP_SCHEMA_URL}` for validation and completion.

### A minimal manifest

```json
{{
  "$schema": "{APP_SCHEMA_URL}",
  "schemaVersion": 1,
  "id": "my-app",
  "name": "My App",
  "models": {{ "processes": ["processes/*.bpmn"] }}
}}
```

A directory counts as a Nano app/project when it contains **`nano.app.json`**
(Urban App) **or** **`nanobpm.project.json`** (an IDE project). Either is importable.

---

## Author an app (outside the IDE)

**`@nanobpm/urban` is the one toolchain** that scaffolds, derives, validates and runs
an Urban app; this node's console is a thin host over it (the manifest is the contract,
`urban` is the single interpreter). Don't start from a blank directory — scaffold with
**`create-urban-app`**. It emits a **Node** app — Node (>= 22.6) is the sole JS/TS
runtime.

### 1. Scaffold — `create-urban-app`

Pick an authoring style:

**Model-first (default)** — the process is an authored `processes/*.bpmn`, served by `urban run`:

```
npm create urban-app@latest my-app
```

**Code-first** — the process is authored in TypeScript with `defineFlow` in `workflows/*.ts`
(`defineFlow` is the blessed path; `defineWorkflow` is the low-level escape hatch):

```
npm create urban-app@latest my-app --code-first
```

Flags: `--dir <path>`, `--id <slug>`, `--preset full|headless`,
`--style model|code` (`--code-first` is shorthand for `--style code`).

What you get — **model-first `my-app/`**:

```
  nano.app.json                # the manifest (declared binding)
  main.ts                      # entrypoint (npm start → urban run)
  processes/greet.bpmn         # BPMN 2.0 (Zeebe extension elements)
  forms/greeting.form          # form-js form
  workers/greet.ts             # service-task handler (referenced by the manifest)
  db/migrations/001_init.sql   # SQLite schema (also derivable with `urban gen`)
  package.json                 # scripts: check / gen / start / dev / deploy
```

…or **code-first `my-app/`**:

```
  nano.app.json                # the manifest
  main.ts                      # entrypoint (npm start → node main.ts)
  workflows/greet.ts           # the process, authored with defineFlow (blessed path)
  scripts/greet.ts             # start an instance (npm run greet -- Adam)
  db/migrations/001_init.sql
  package.json                 # scripts: check / gen / start / dev / greet
```

### 2. Author the models

- **BPMN** (model-first): a `<bpmn:process isExecutable="true">` with Zeebe extension
  elements (`zeebe:taskDefinition type="..."` on a `serviceTask`, `zeebe:calledDecision`
  on a `businessRuleTask`, `zeebe:taskHeaders`, timers as ISO-8601 / cron). The engine is
  Zeebe-compatible, so anything you know from Camunda 8 modelling applies.
- **defineFlow** (code-first): `defineFlow(id, envelopes, (w) => ...)` in `workflows/*.ts`
  builds the same BPMN under the hood and can run workers in-process; eject to model-first
  any time.
- **DMN**: standard DMN 1.3 decision tables; inputs/outputs use **FEEL**. They run on the
  cluster's native Rust decision engine — no JVM.
- **FEEL**: expressions are conventionally `=`-prefixed. Same dialect as Camunda 8.
- **Service-task handlers**: destructure `job.variables` and return result variables; the
  DataLayer owns column type defaults, so returning `undefined` for a key omits it on write
  (the column `DEFAULT`/`NULL` governs) — keep `null` distinct, and don't hand-coerce.

---

## Local development loop (validate before you link)

The scaffolded app's npm scripts wrap the `urban` CLI — **this is the loop that validates
the app end-to-end.** Point the toolchain at **this node's** engine so instances run here:

```bash
cd my-app
export CAMUNDA_REST_ADDRESS={base_url}/v2

npm install
npm run check      # urban check — validate the manifest
npm run gen        # urban gen — derive artifacts (SQLite migrations, worker-IO types)
npm run dev        # hot-reload: watch sources, re-derive + reload on change
npm start          # run once (model-first: urban run; code-first: node main.ts)
```

- `npm run gen:check` (`urban gen --check`) is the **drift gate** — it fails when the
  derived artifacts are stale; run it in CI.
- `urban stubs --write` scaffolds write-once handler stubs for each service task in the
  model and wires them into the manifest.
- `check` proves the manifest, `gen` proves the derivation, `dev`/`start` run it against
  this node's engine at `$CAMUNDA_REST_ADDRESS` (default `http://localhost:8080/v2`). Once
  it runs clean, **link it in** (below) so it appears in the Projects gallery.

Tip: fetch `{APP_SCHEMA_URL}` and use it to drive completion/validation of the manifest
as you write it.

---

## Link it in (import by reference — ADR 0041)

Linking registers a **pointer** to your directory. Nano does **not** copy it; it
reads it **live**, so your edits show up on the next Run — a no-copy dev loop. Two
equivalent ways:

### A. POST the import endpoint (preferred)

```bash
curl -sS -X POST '{import_url}' \
  -H 'content-type: application/json' \
  -d '{{"name": "My App", "path": "/absolute/path/to/my-app"}}'
```

- `path` **must be absolute** and resolve to a directory containing `nano.app.json`
  or `nanobpm.project.json`, or the import is refused.
- `name` is the human-facing project name (spaces allowed); it is slugged for the
  on-disk key. It must not collide with an existing workspace project.
- On success you get the registered reference back; the app now appears in the
  Projects gallery at `{base_url}/console`.

### B. Drop a reference file

Write `<slug>.project-ref.json` into the projects root
(`{projects_root}`):

```json
{{ "source": "path", "path": "/absolute/path/to/my-app" }}
```

The node resolves the project to the external directory on the next read. A real
workspace project of the same name always shadows a reference, and deleting or
renaming the reference never touches your external checkout.

---

## How Nano works (so you can explain it)

- **Engine**: a Zeebe-lineage BPMN engine in Rust. A single-writer command stream
  drives instance creation and job activation/completion; state is replicated via
  Raft across the cluster's nodes. Tokens flow through the model exactly as in
  Camunda 8: start event → tasks/gateways → end.
- **Jobs & workers**: a `serviceTask` with a `zeebe:taskDefinition type` creates a
  **job**; a worker (a referenced handler file, an external worker, or an **LLM
  binding**) activates it, does the work, and completes it with result variables.
- **Decisions**: a `businessRuleTask` with `zeebe:calledDecision` evaluates a DMN
  decision **natively** on the cluster (no JVM) and writes the result back.
- **FEEL** everywhere: conditions, input/output mappings, decision inputs, form
  default values.
- **Test without a cluster**: the console can run a model entirely in the browser via
  **μ-nano** — a ~0.5 MB WebAssembly build of the *same* `engine-core` Rust code, so
  token flow, gateways, timers, DMN and FEEL behave exactly as they will on the server.
- **Deploy / Run**: from a model you Deploy (idempotent) then Start an instance; an
  App can also cross-compile to a single native binary with the engine embedded
  (**Bernd**) and run with no external server.

### Where to read more (machine-readable)

- App manifest schema: `{APP_SCHEMA_URL}`
- Console REST API (OpenAPI): `{base_url}/swagger`
- Command stream (AsyncAPI): `{base_url}/asyncapi`
- Human docs: `{base_url}/docs`
- This brief (always current for this node): `{base_url}/agent`
"#,
    ));

    m
}

/// Render the `/llms.txt` discovery index for a node reachable at `base_url`.
///
/// Follows the `llms.txt` convention: a short, link-first index a machine reader
/// fetches to discover the detailed docs. Here it points at the full agent brief
/// plus the node's machine-readable specs.
pub fn render_llms_txt(base_url: &str) -> String {
    format!(
        "# Nano\n\n\
> A Rust distillation of Camunda's Zeebe workflow engine with a Rapid Application \
Development face (\"Urban\"). This node can author and run BPMN/DMN apps.\n\n\
Point your agent at the authoring brief below: it explains how to author a Nano \
App on disk and link it into this running node, and how the engine works.\n\n\
## Agent\n\n\
- [Agent authoring brief]({base_url}/agent): scaffold a Nano App with create-urban-app, validate it with the urban CLI, and link it into this node; how Nano works.\n\n\
## Specs\n\n\
- [App manifest JSON Schema]({APP_SCHEMA_URL}): schema for `nano.app.json`.\n\
- [Console REST API (OpenAPI)]({base_url}/swagger): every console endpoint.\n\
- [Command stream (AsyncAPI)]({base_url}/asyncapi): the instance command stream.\n\n\
## Docs\n\n\
- [Human documentation]({base_url}/docs): the console's built-in docs.\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brief_is_actionable_for_this_node() {
        let base = "https://nano.example.test";
        let md = render(base);
        // The base URL threads through to the URLs an agent must actually hit.
        assert!(md.contains("https://nano.example.test/console"));
        assert!(md.contains("POST https://nano.example.test/console/api/projects/import"));
        // The two jobs the brief exists to enable are both spelled out.
        assert!(md.contains("## Author an app"));
        assert!(md.contains("## Link it in"));
        assert!(md.contains("## How Nano works"));
        // The blessed scaffold + validate loop: create-urban-app and the urban CLI.
        assert!(md.contains("npm create urban-app@latest"));
        assert!(md.contains("--code-first"));
        assert!(md.contains("## Local development loop"));
        assert!(md.contains("urban check"));
        assert!(md.contains("urban gen --check"));
        // Node hosting is the sole path — the defunct Deno entrypoint must not drift back.
        assert!(!md.contains("deno run main.ts"));
        assert!(md.contains("npm start"));
        // The engine REST address the toolchain must target on this node.
        assert!(md.contains("CAMUNDA_REST_ADDRESS"));
        assert!(md.contains("https://nano.example.test/v2"));
        // It points at the schema for manifest validation/authoring.
        assert!(md.contains(APP_SCHEMA_URL));
        // The on-disk projects root (where a ref file may be dropped) is disclosed.
        assert!(md.contains(&projects::projects_root().display().to_string()));
        // A curl the agent can copy-paste to link an app in.
        assert!(
            md.contains("curl -sS -X POST 'https://nano.example.test/console/api/projects/import'")
        );
    }

    #[test]
    fn llms_txt_points_at_brief_and_specs() {
        let base = "http://localhost:8080";
        let txt = render_llms_txt(base);
        assert!(txt.starts_with("# Nano"));
        assert!(txt.contains("http://localhost:8080/agent"));
        assert!(txt.contains(APP_SCHEMA_URL));
        assert!(txt.contains("http://localhost:8080/swagger"));
    }
}
