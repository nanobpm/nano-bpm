# ADR 0065 — Deno removal: Node (≥ 22.6) is the sole JS/TS runtime

Status: **Accepted.**
Date: 2026-10-13.
Supersedes (in full):
ADR 0036 (`0036-dual-runtime-workers-deno-node-fallback.md` — dual-runtime workers, Deno-preferred),
ADR 0038 (`0038-node-first-runtime.md` — Node-first, Deno optional for `deno compile`).
Relates to:
ADR 0009 (`0009-gui-application-projects.md`, the node-gui app template),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, the datasource gateway now Node-only),
ADR 0025 (`0025-urban-trigger-runtime.md`, the out-of-process trigger driver — now Node-only),
ADR 0033 (`0033-urban-element-templates-first-class-components.md`, the element-template emitter —
strip-only, Node-only),
ADR 0040 (`0040-fused-domain-model.md`, the TS half of the console gateway this keeps in-tree),
ADR 0052 (`0052-urban-runtime-decoupled-manifest-interpreter.md`, the Urban runtime manifest
interpreter — the `node.json` import map this now names).

## Context

Deno was **deprecated** in October 2026 — the Deno team joined Cloudflare, and the project entered a
wind-down (monthly releases continue for a transitional period, but there is no forward roadmap and
the ecosystem's migration targets are Node). The Nano stack had carried Deno support for two reasons,
both now moot:

1. **ADR 0036** made TypeScript workers *dual-runtime* (**Deno preferred, Node fallback ≥ 22.6**) so
   they ran on 32-bit ARM, where Deno publishes no `linux-armv7l` build.
2. **ADR 0038** inverted the preference to **Node-first**, leaving Deno *optional* — the sole
   Deno-exclusive capability being `deno compile` (single-file packaging).

The dual-runtime mechanism was never free: it demanded a runtime-shim (`RT`) in every TS SDK, a
`node_deno_shim.mjs` that emulated `Deno.*` globals under Node, a hand-maintained `deno_ns.d.ts`
(12.3k lines, pinned by construction to a specific `deno types` output) vendored into the console
editor for IntelliSense, a `find_deno()`/`usable_node()` selection split through every run path, and a
language-pack id space (`deno`, `deno-gui`) that shadowed the real runtime (`node`). Every one of those
surfaces is a **drift surface**: two sources of truth for "what runs TypeScript," with the merge/lint
cost paid on every change and the bug surface (a shim that misses a call site, a type file that goes
stale) accruing silently.

With Deno deprecated, the "alternative runtime" is a liability, not a fallback: nobody should be
selecting it, and supporting it keeps an entire parallel path (and its type surface) alive.

## Decision

**Remove Deno entirely. Node (≥ 22.6) is the sole JS/TS runtime** for workers, project `main.ts`,
the datasource CLI gateway, and out-of-process trigger drivers. `deno compile` — the one
Deno-exclusive capability — is removed with it; there is no longer a "compile a project to a
single-file binary" feature.

Concretely:

### 1. Runtime: one selection path

`find_deno()` and the `deno_available()` flag are gone. `workers::usable_node()` is the single
runtime resolver (`NANOBPMN_NODE_BIN`, then every `node` on `PATH`, then `~/.local/bin/node` — no
`~/.nvm`; a `NANOBPMN_NODE_BIN` older than the floor is skipped, so a supported Node further down
still wins). The three run paths
(`workers::start`, `projects::run`, `projects::run_data_op`) and the trigger-driver supervisor
(`trigger_sources.rs`) launch Node unconditionally and report a single "no Node runtime" error
(`DataError::NoRuntime`) when it is absent. TypeScript runs natively via
`--experimental-strip-types --no-warnings --import nano-generated/node-register.mjs`.

### 2. Canonical naming: `node.json`, `nodeBin`, `NANOBPMN_NODE_BIN`, `node`/`node-gui`

The manifest/import-map file is **`node.json`** (with a `node.jsonc` variant), not `deno.json`.
`NANOBPMN_DENO_BIN` becomes **`NANOBPMN_NODE_BIN`**; the language-pack config key is **`nodeBin`**;
the built-in packs are **`node`** and **`node-gui`**; `default_lang()` is `"node"`. The specifier
loader is `node_loader.mjs` + `node_register.mjs` (already Node-only; the `deno.json`- and
`deno.jsonc`-reading compatibility shim is deleted alongside `node_deno_shim.mjs`).

**Legacy ids still resolve.** A project config scaffolded before this ADR (`lang: "deno"`, or an app
pack `deno-gui`) keeps working: `extensions::lang_pack`/`pack` alias the legacy ids onto the
built-in Node packs, and the loader treats a `deno.json`/`deno.jsonc` left on disk as an alias of
`node.json`/`node.jsonc`.
This is a **compatibility alias, not a second runtime** — the alias only fixes the *file name / id*;
execution is Node either way.

### 3. Editor IntelliSense: derive the Node surface, don't vendor it

The vendored `deno_ns.d.ts` is deleted. The console editor's ambient Node surface is now **derived**
from the console's real `@types/node` devDependency (+ its `undici-types` dependency) via a lazy Vite
`import.meta.glob("?raw")` over `node_modules/@types/node/**` and `node_modules/undici-types/**`,
registered as Monaco extra-libs. The `@types/node/ts5.6` and `ts5.7` directories are excluded (they
are older-TypeScript variants; the root `index.d.ts` targets ≥ 5.8). This required bumping
`monaco-editor` ^0.52.2 → ^0.57.0, whose bundled TypeScript (5.9.3) is new enough for
`@types/node@24`; the deprecated `monaco.languages.typescript` / `monaco.languages.json` namespaces
were migrated to the top-level `monaco.typescript` / `monaco.json`, and the worker imports moved
under monaco-editor 0.57's `exports` map. This eliminates the last *hand-maintained* copy of a
runtime's type surface — it is now a pure function of the installed `@types/node`.

### 4. CI: the `console (deno typecheck + test)` job becomes `console-ts`

The `console (deno typecheck + test)` job (`denoland/setup-deno` + `deno test`) is replaced by
**`console-ts (server TS typecheck + test)`** — a Node lane (`actions/setup-node`, `node --test`) at
the same spot in `ci.yml`, gated by the same `code_gate` filter. It restores the whole-import-graph
check `deno test` used to give with `tsc -p server/crates/nano-server-console/tsconfig.json`, because
Node's `--experimental-strip-types` *erases* types without checking them, and runs the same unit
tests on **Node's built-in test runner** (`node --experimental-strip-types --test "src/*_test.ts"` —
one runtime, one runner). The `tsc` invocation uses the console's pinned `typescript` + `@types/node`
(the one place in the repo that `npm ci`s them), so there is no second TypeScript/`@types/node` pin to
drift; the materialisation entrypoint `data_cli.ts` stays excluded (it imports the SDK's hyphenated
emit-time filenames, which only exist post-render).

## Consequences

- **One source of truth for the JS/TS runtime.** No `RT` shim, no `node_deno_shim.mjs`, no
  `deno_ns.d.ts`, no runtime-preference branch. The failure mode ADR 0036/0038 recorded (a Node path
  that was "lightly exercised" because Deno was tried first) is eliminated **categorically** — there
  is no second path to be stale.
- **The type surface is derived, not duplicated.** The editor's ambient Node types track the console's
  installed `@types/node` automatically, so a dependency bump moves types with it.
- **`deno compile` is gone.** Users lose standalone single-file packaging. This was already the only
  Deno-exclusive feature (ADR 0038 §3) and had no Node equivalent; the console's compile modal/platform
  picker is removed.
- **32-bit ARM is unaffected.** Node ships an official `linux-armv7l` binary; Node was already the
  Node-first default (ADR 0038), so the Pi path is now simply "the path."
- **Back-compat is alias-only.** Old `nanobpm.project.json` (`lang: "deno"`) and `deno.json` files keep
  resolving, but they are rewritten/routed to Node; no Deno ever executes.
- **Docs and the language-pack registry** (`extensions.rs`) name Node only; historical ADRs 0036/0038
  are marked superseded.

### Caveat — the capability sandbox is fully off

ADR 0038 already accepted losing Deno's permission sandbox in exchange for Node-first execution. With
Deno removed there is no sandboxed-runtime option at all: Node runs code with full ambient authority.
This is consistent with Nano's threat model ("the maker runs their own trusted code" — workers and apps
are authored or installed by the operator). **Revisit if that changes** — specifically if Nano ever
runs *untrusted* third-party code, at which point an OS-level sandbox around the Node path (not a Deno
reintroduction) is the design to pursue.

## Open questions

1. **Standalone packaging.** With `deno compile` gone, is there an appetite for a Node-bundling path
   (e.g. `node --experimental-sea-config` single-executable apps, or `bun build --compile`), or does
   "run the project directory with Node" suffice? *(No implementation scheduled.)*
2. **Legacy alias lifetime.** How long do the `deno`/`deno-gui`/`deno.json` compatibility aliases stay?
   They are cheap (a name match), but a future cleanup could migrate on-disk configs and drop them.
3. **`@types/node` pin cadence.** The editor's ambient surface now tracks the console's `@types/node`
   version. Do we want an explicit floor (documented minimum) so a *downgrade* can't silently shrink
   the editor's Node surface?