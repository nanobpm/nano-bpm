# ADR 0009 — GUI application projects (served-UI binaries)

Status: **Accepted — implemented (extension manifest, builtin node/rust/node-gui packs, lang+app project axes, toolchain run/compile, marketplace UI).** The built-in pack ids were renamed `deno`→`node` and `deno-gui`→`node-gui` when Deno was removed ([ADR 0065](0065-deno-removal.md)); the legacy ids still resolve as aliases.
Date: 2026-06-29.
Relates to: ADR 0007 (`0007-rad-extension-system.md`, the pack contract), ADR 0005
(`0005-embedded-u-nano.md`, embed-Nano binary direction), `server/src/console/projects.rs`
(scaffold + `deno compile` supervisor), ADR 0008 (polyglot, sibling axis).

## Context

Projects are console apps today (`main.ts` deploys + runs workers, exits). We want a second
**output kind**: a GUI app — a compiled binary that runs a webserver and serves a frontend UI
for the user's process application. Output kind is a different axis from language (ADR 0008):
a project picks both. First target is Deno-based; Embedded Nano (ADR 0005, one self-contained
binary) is the intended future direction, so design the pack so embed is a later capability,
not a rewrite.

## Decision (proposed)

Ship `nano-ide-app-deno-gui`, a `kind:"app"` pack per ADR 0007:

- **template**: scaffolds a `Deno.serve` backend (deploy + workers + REST/stream proxy) plus a
  static frontend bundle; entrypoint serves UI and talks to the engine via `@nanobpm/nano-sdk`.
- **toolchain**: reuses `deno compile` (already in the supervisor) to emit a self-contained
  binary with the webserver + embedded frontend — no separate hosting.
- **app kind** in `ProjectConfig` (separate field from `lang`): `console` (default, today) vs
  `gui`. Compile/export already exists; GUI just changes scaffold + bundled assets.

Connectivity stays remote-Nano/Camunda. **Embed Nano** (ADR 0005) is a later flag on this pack:
same binary, micro-nano embedded, no external server. Keep the backend's engine access behind
the SDK transport seam so swapping to embedded is a config change.

## Consequences

- `lang` × `app` matrix: e.g. Deno+GUI now, Rust+GUI later — both packs over 0007.
- Reuses `deno compile` cross-targets; frontend assets bundled into the binary, offline-capable.
- Clear runway to embed-Nano without a rewrite.

## Open questions

- Frontend stack the template ships (vanilla vs React)?
- Auth/exposure defaults for the served UI?
- Embed-Nano timing relative to GUI v1.
