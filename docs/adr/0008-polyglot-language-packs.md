# ADR 0008 — Polyglot RAD: language packs (Rust first)

Status: **Accepted — implemented (extension manifest, builtin node/rust/node-gui packs, lang+app project axes, toolchain run/compile, marketplace UI).** The built-in pack ids were renamed `deno`→`node` and `deno-gui`→`node-gui` when Deno was removed ([ADR 0065](0065-deno-removal.md)); the legacy ids still resolve as aliases.
Date: 2026-06-29.
Relates to: ADR 0007 (`0007-rad-extension-system.md`, the pack contract this consumes),
`server/src/console/projects.rs` (scaffolder, supervisor, `PLATFORMS`),
`console/src/lib/editorLang.ts` + `CodeEditor.tsx` (Monaco), the Rust loadgen perf work
(`~/workspace/ts-performance-matrix/rust-worker/`, native producer ~32k stream vs ~20k REST).

## Context

Authoring is Deno-only today. We want polyglot projects starting with **Rust**, both for
performance-critical workers/producers (Rust pipelines the Falcon protocol where JS cannot —
~32k vs ~20k) and to prove the extension contract. First deliverable: a **Rust throughput
demo** matching the existing Deno `throughput`/`throughput-stream` demos.

## Decision (proposed)

Ship `@nanobpm/nano-ide-lang-rust`, a `kind:"lang"` pack per ADR 0007:

- **fileTypes**: `.rs → monacoLang:"rust"`, bundling a TextMate grammar so the editor lazy-
  loads it only when a `.rs` file is open. IntelliSense tier 0 = grammar highlight; tier 1 =
  optional `rust-analyzer` over the consented toolchain.
- **toolchain**: `detect:["cargo --version"]`, `run:"cargo run --release"`,
  `compile:"cargo build --release"`, `targets` = host triple (cross-compile is a later cargo
  config concern). Uses the user's installed Rust; missing cargo => install link, never auto.
- **templates**: `rust-throughput` — a Cargo project producing instances + a `test-job` worker
  over the Falcon protocol, mirroring the Deno demo so the README A/B extends to a third column.

`ProjectConfig` gains a `lang` field (default `deno`); supervisor reads the pack's toolchain
instead of hard-coding `deno run`/`deno compile`. Deno becomes the built-in lang pack.

## Consequences

- Throughput demos become tri-modal: Deno REST, Deno stream, Rust stream — the Rust column is
  where the Falcon protocol actually wins, making the perf story honest in-product.
- Supervisor must surface non-Deno target triples; `PLATFORMS` becomes pack-supplied.
- Validates ADR 0007 end-to-end before GUI (0009).

## Open questions

- Bundle rust-analyzer wiring now or grammar-only first?
- Cross-compilation: defer to user's cargo + targets, or offer `cross`?
