# ADR 0038 — Node-first runtime: Deno optional, only for `deno compile`

Status: **Superseded by [ADR 0065](0065-deno-removal.md)** — Deno is removed entirely; Node (≥ 22.6) is
the sole JS/TS runtime, and `deno compile` is gone with it. (Historically: **Accepted.**)
Date: 2026-07-24.
Relates to / refines:
ADR 0036 (`0036-dual-runtime-workers-deno-node-fallback.md`, which made workers dual-runtime with
**Deno preferred**; this ADR inverts the preference to **Node-first** and generalises the invariant to
every Deno-invoking surface),
ADR 0022 (`0022-nano-rad-application.md`, **Urban** — the RAD runtime this keeps portable),
ADR 0024 (`0024-urban-data-layer-datasource-abstraction.md`, the datasource DB Manager gateway, now
Node-first),
`server/src/console/projects.rs` + `server/src/console/workers.rs` (the three run paths that select a
runtime), `server/src/console/data_cli.ts` + `data_sdk.ts` (the datasource seam, now fully shimmed).

## Context

ADR 0036 made Nano's TypeScript workers dual-runtime — **prefer Deno, fall back to Node ≥ 22.6** — so
they run on 32-bit ARM (Raspberry Pi), where Deno publishes no `linux-armv7l` build. That fixed
workers, but left two things unresolved:

1. **The preference was backwards for our reality.** Deno is an *optional* install; **Node is the
   runtime we are guaranteed to have** — the npm launcher (`c8ctl-plugin-nano`) *is* Node, so every
   Nano install already ships a usable Node. Making Deno the preferred path meant the *primary,
   best-tested* code path was the one that isn't always present, and the *always-present* path (Node)
   was the lightly-exercised fallback. In practice this hid latent Node-path bugs — e.g. `data_sdk.ts`
   still called bare `Deno.cwd()`/`Deno.env` at two sites the shim missed, which never ran because
   Deno was always tried first.

2. **The degradation was incomplete.** The **DB Manager gateway** (`run_data_op` → `data-cli.ts`) still
   hard-required Deno (`find_deno().ok_or(NoDeno)`), so the whole Data panel was dead on a Pi even
   though `data_sdk.ts` already used `node:sqlite`. The invariant "everything degrades to Node" was
   stated for workers but not enforced project-wide.

Landing Nano on a real Raspberry Pi 2B (ARMv6/v7, no Deno) surfaced both.

## Decision

**Node-first, everywhere.** Node ≥ 22.6 is the primary runtime for **running** TypeScript — workers,
Urban App `main.ts`, and the datasource CLI gateway. Deno is an **equal alternative for Run** and is
**required only for `deno compile`** (single-binary packaging), which has no Node equivalent.

Concretely:

### 1. Selection order inverted (all three run paths)

`workers.rs::start`, `projects.rs::run`, and `projects.rs::run_data_op` now select
**`usable_node()` first, then `find_deno()`**, else a clear "no JS runtime" error. Node is chosen on
every host that has it (i.e. always, via the launcher); Deno runs only where Node is absent — the exact
mirror of ADR 0036's order.

### 2. The datasource gateway degrades too

`run_data_op` builds a Node command (`--experimental-strip-types --no-warnings --import
nano-generated/node-register.mjs data-cli.ts`) just like the worker/run paths. `data-cli.ts` gained the same
`RT` runtime shim already in `data_sdk.ts`/`worker_sdk.ts` for its own host calls (stdin, `readDir`,
`readTextFile`, `exit`). The two bare-`Deno.*` sites the ADR 0036 shim missed in `data_sdk.ts` are
fixed. `DataError::NoDeno` → `NoRuntime`.

### 3. `deno compile` is the sole Deno-exclusive capability

Compile still requires Deno (`projects.rs::compile`), with a message saying so. **This is the one and
only thing a user loses on a Deno-less host (e.g. 32-bit ARM): the ability to package a project to a
standalone single-file binary.** Everything else — author, Run, workers, the Data panel — works on Node.

### 4. Honest availability signals

The console API carries **both** `denoAvailable` (⇒ Compile) and a new `nodeAvailable` (⇒ Run) on
`WorkersResponse`/`ProjectDetail`/`ProjectsResponse`. The Projects/Workers banners no longer claim
"Run requires Deno"; they warn only when **no** JS runtime is present, and a Deno-less host gets a
soft note that Deno is needed only to Compile. The `deno` entry in the dependency panel is reframed as
**optional (compile-only)**.

### 5. Visible generated dir + a real `package.json` entrypoint

The reified SDK moved out of the hidden `.nanobpm/` into a **visible `nano-generated/`** directory —
isolation without opaque magic, so users can open and read the generated types/loaders. The directory
name is a single `GEN_DIR` constant (Rust `projects.rs`, TS `domain_types.ts`, console `CodeEditor.tsx`)
templated into every emitted `deno.json`/`tsconfig.json` via a `{GEN}` marker — *derive, don't duplicate*.
The scaffolded `package.json` also gains an entrypoint (`"main": "main.ts"` + a `"start"` script that
delegates to `deno task start`, since the app entrypoint uses `Deno.*` globals); the `@nanobpm/*` alias
stays in `deno.json`/`tsconfig` `paths` because npm `imports` keys must be `#`-prefixed.

## Consequences

- **The universal path is the default path.** The runtime we always have (Node) is now the primary,
  best-tested one; the "no Deno" host is no longer a second-class fallback but the normal case.
- **32-bit ARM is fully first-class** — not just workers (ADR 0036) but the Data panel and App Run too.
  Only Compile is unavailable, and its UI says so.
- **The invariant is project-wide and forward-looking:** *any* new Deno-invoking surface (e.g. a future
  I/O trigger dispatch runtime) MUST carry a Node fallback via the `RT`-shim pattern; only `deno
  compile` may hard-require Deno.

### The accepted tradeoff — capability sandbox

Deno runs code under a permission sandbox (`--allow-read=<dir> --allow-write=<dir> --allow-net
--allow-env --no-prompt`); **Node has no capability sandbox** — code runs with full ambient authority.
Choosing Node-first means the sandbox is **off by default**. We accept this **because Nano's threat
model is already "the maker runs their own trusted code"** (workers and App code are authored by, or
installed by, the operator) — the same assumption ADR 0036 recorded when it added the Node fallback at
all. The containment Deno offered here was scoped filesystem access, not isolation of untrusted code.

**Revisit if that threat model changes** — specifically if Nano ever runs *untrusted* third-party code
(e.g. a marketplace that ships executable workers/apps). At that point the sandbox has real value and
we would either prefer Deno for untrusted execution or add an OS-level sandbox around the Node path.

### Caveats (unchanged from ADR 0036, now on the primary path)

- Native TS on Node is `--experimental-strip-types` (landed 22.6, stabilising); Deno runs TS natively.
- `jsr:` / `http(s):` imports and `deno compile` are Deno-only. Urban scaffolds use
  `node:`/`npm:`/relative specifiers + the `deno.json` import-map loader, so the Node path covers them.
