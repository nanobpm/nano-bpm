# ADR 0036 — Dual-runtime workers: Deno-preferred, Node fallback

Status: **Superseded by [ADR 0065](0065-deno-removal.md)** — Deno is removed entirely; Node (≥ 22.6) is
the sole JS/TS runtime. (Historically: **Accepted**, with the preference inverted by ADR 0038 — the
dual-runtime mechanism stood, but the default became **Node-first**, Deno required only for `deno
compile`.)
Date: 2026-07-24.
Relates to:
ADR 0022 (`0022-nano-rad-application.md`, **Urban** — §E's embedded job workers are the runtime this
ADR makes portable),
ADR 0034 (`0034-console-build-profiles.md`, the console/server packaging this runtime ships inside),
`server/src/console/worker_sdk.ts` (the worker SDK this ADR makes runtime-agnostic),
`server/src/console/data_sdk.ts` (the datasource SDK, already on `node:sqlite`),
`server/src/console/projects.rs` + `server/src/console/workers.rs` (the two run paths that pick a
runtime),
`server/src/console/node_loader.mjs` + `node_register.mjs` (the import-map loader that lets Node honour
`deno.json`).

## Context

Nano's embedded job workers are authored in TypeScript and, until now, run **only** under Deno: the
console spawns `deno run` per enabled worker, and the worker SDK reaches for `Deno.env`, `Deno.stdout`,
`Deno.exit`, and `Deno.addSignalListener`. This is a clean sandboxed model on x86-64 and arm64.

It breaks on **32-bit ARM** (Raspberry Pi and friends): **Deno publishes no `linux-armv7l` build**. This
is a Deno release-matrix decision, not a V8 gap — V8 supports 32-bit ARM, and Node ships an official
`linux-armv7l` binary. Concretely, a user on a Pi running Nano via the npm launcher
(`c8ctl-plugin-nano`) can author projects, run **Python** workers (via `uv`), and compile+run **Rust**
workers, but sees "Run/Compile disabled — no Deno" for TypeScript workers even though a perfectly good
JavaScript runtime — **Node — is already present**, because the launcher *is* Node.

Two facts make a fallback cheap:

1. The worker SDK touches a **tiny** runtime surface (env, stdout write, exit, SIGTERM). Everything
   else it uses — `WebSocket`, timers, `fetch` — is a web/global API present in both runtimes.
2. The datasource SDK already uses `node:sqlite`, a **Node built-in** (Deno also implements it).

The only real gap is that TypeScript projects carry a **`deno.json` import map** (`@nanobpm/worker`,
`@lib/…`, `npm:…`), which Node does not read.

## Decision

Make workers **runtime-agnostic**: **prefer Deno, fall back to Node ≥ 22.6.** The switch is transparent
— no project change, no user configuration.

### 1. Runtime shim in the SDKs

`worker_sdk.ts` and `data_sdk.ts` gain a small `RT` adapter that resolves to `Deno` when
`globalThis.Deno` exists, else to `process`/`node:*`:

- env read, stdout write, `exit`, SIGTERM handler (worker SDK);
- cwd, env, `readTextFile` (data SDK; Node arm uses `node:fs/promises`).

No other SDK code changes; the wire protocol and control-line format are identical on both runtimes.

The `RT` adapter covers the SDKs, but **app/template code** (`main.ts`, the scaffolder's
`URBAN_MAIN_TS`/`GUI_MAIN_TS`) calls `Deno.*` directly and cannot be rewritten around an adapter.
So the Node bootstrap also installs a minimal **`Deno` global shim** (`node-deno-shim.mjs`, imported
from `node-register.mjs` so it loads on the main thread before the entry). It backs the surface the
app tier + SDK adapters actually use — `env`, `cwd`, `exit`, `args`, `readTextFile`/`writeTextFile`,
`mkdir`/`readDir`/`remove`/`stat`, `stdin`/`stdout`, `addSignalListener`, and `serve` (a Web
`Request`→`Response` bridge over `node:http`) — with Node built-ins. It no-ops when a real `Deno`
global is present. This is what makes "code authored for Deno runs unchanged under Node" true for app
code, not just the SDKs; without it `Deno.serve`/`Deno.env` throw "Deno is not defined".

Since the SDK adapters branch on `globalThis.Deno`, the shim is deliberately complete enough that they
take (and work on) their `Deno` arm under the fallback.

### 2. Node honours `deno.json` via a `module.register` loader

Two files are materialised beside the SDK (`nano-generated/`):

- `node-loader.mjs` — a `resolve` hook implementing the `deno.json` import map: exact keys, `/`-suffixed
  prefix keys, and `npm:` → bare specifier. It bases relative map values on **`process.cwd()`**, which
  equals the `deno.json` directory in **both** run models (project root for `main.ts`; the worker dir
  for a legacy `worker.ts`). `jsr:` / `http(s):` specifiers throw (unsupported under the fallback).
- `node-register.mjs` — `module.register("./node-loader.mjs", import.meta.url)`, passed via `--import`.

Node argv: `node --experimental-strip-types --no-warnings --import <nano-generated/node-register.mjs> <entry>`.
`--experimental-strip-types` runs `.ts` directly (landed 22.6); `WebSocket` (22.4) and `node:sqlite`
(22.5) are built in. Hence the **`NODE_MIN = (22, 6)`** floor, gated by `usable_node()` probing
`node --version`.

### 3. Runtime selection

Both run paths (`projects.rs::run`, `workers.rs::start`) select a runtime in order: **Deno if found,
else `usable_node()`, else a clear error**. `find_node()` resolves `NANOBPMN_NODE_BIN` (exported by the
npm launcher as its own `process.execPath`) → `PATH` → `~/.local/bin`.

### 4. `compile` stays Deno-only

`deno compile` (single-binary output) has no Node equivalent, so **Compile still requires Deno**. Only
**Run** falls back to Node. The compile error message says so.

### 5. Honest "runnable" + banner

A Deno-language project is `runnable` when `deno_available() || node_available()`. The Projects banner
no longer claims Run/Compile are globally disabled without Deno — it is project-scoped and mentions the
Node ≥ 22.6 fallback. (Python/Rust never depended on Deno and were always runnable.)

## Consequences

- **32-bit ARM works.** TypeScript workers run on a Pi with no Deno build, using the Node the launcher
  already ships. Deno projects are, in practice, always runnable.
- **No behavioural change on x86-64/arm64.** Deno is still preferred; the fallback only engages when
  Deno is absent.
- **Caveats (documented):**
  - `jsr:` / `http(s):` imports and `deno compile` are Deno-only; a project using them won't run under
    the Node fallback.
  - A stray `package.json` with a default/`commonjs` type can make Node load `.ts` as CommonJS and break
    type stripping; real scaffolds have none. npm-dependency users may need `"type":"module"`.
  - The Node floor is 22.6; older Node is treated as absent.
- **Follow-up:** the `c8ctl-plugin-nano` launcher exports `NANOBPMN_NODE_BIN=process.execPath` so the
  server always finds a known-good Node regardless of `PATH`.
