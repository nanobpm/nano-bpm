# ADR 0007 — RAD extension system (npm-installable IDE packs)

Status: **Accepted — implemented (extension manifest, builtin node/rust/node-gui packs, lang+app project axes, toolchain run/compile, marketplace UI).** The built-in pack ids were renamed `deno`→`node` and `deno-gui`→`node-gui` when Deno was removed ([ADR 0065](0065-deno-removal.md)); the legacy ids still resolve as aliases.
Date: 2026-06-29.
Relates to: ADR 0005 (`docs/adr/0005-embedded-u-nano.md`, the application-binary direction),
`server/src/console/projects.rs` (`TEMPLATES`, scaffolder, run/compile supervisor),
`console/src/components/CodeEditor.tsx` + `console/src/lib/editorLang.ts` (Monaco editor),
the polyglot lang-pack ADR (0008, planned) and GUI app-project ADR (0009, planned).

## Context

The RAD environment today is mono-runtime (Deno) and mono-output (console app). Two
directions want to expand it: **polyglot** authoring (Rust first) and **GUI applications**
(a compiled binary serving its own UI). Both need the same three things, just configured
differently:

1. **A grammar/editor profile** so Monaco understands the file types in the project;
2. **A project scaffolder** that stamps out the right starter files for the language/output;
3. **A supervisor profile** that knows how to run/compile the project using the user's
   on-machine toolchain.

Rather than special-case Rust and GUI in `projects.rs`, factor these three seams behind a
single **extension contract** so capabilities arrive as npm packages we (and later the
community) publish. This is the load-bearing ADR for both follow-ons.

### What already exists (load-bearing facts)

- **Projects are self-contained dirs** with a `nanobpm.project.json` config and a
  template-driven scaffolder (`projects.rs::TEMPLATES`, `create_project`). Templates are a
  fixed `&[(id,label)]` slice with hard-coded file-stamping — exactly the seam to externalise.
- **Run/compile is a supervisor** that spawns `deno run` / `deno compile` (`projects.rs`,
  ~L1003–1420), assuming Deno + Deno target triples (`PLATFORMS`). Toolchain is implicit.
- **Editor is Monaco**, bundled offline; `editorLang.ts::languageForFile` maps extensions to
  Monaco language ids (ts/js/json/md). No tree-sitter; Monaco uses TextMate grammars + per-
  language workers. Only json/ts/js workers are bundled.
- **Console is embedded** in the gateway binary and offline-first (Monaco workers bundled, not
  CDN). Any extension mechanism must degrade gracefully offline and never require the registry
  to be reachable for existing projects to open.

## Decision (proposed)

A **RAD extension** is an npm package named `nano-ide-ext-*` (scoped community packs allowed,
first-party under `@nanobpm/`). Two specialisations: `nano-ide-lang-*` (a language pack) and
`nano-ide-app-*` (an output/runtime pack). All carry a single manifest the host reads;
nothing is `eval`'d — the host drives **declared data**, not extension-supplied code, except
for explicitly-listed toolchain commands the user consents to.

### Manifest contract (`nano-ide.ext.json`)

```jsonc
{
  "id": "rust",                       // unique kind id
  "kind": "lang" | "app",             // which seam(s) it drives
  "displayName": "Rust",
  "fileTypes": [{ "ext": ".rs", "monacoLang": "rust", "grammar": "grammars/rust.tmLanguage.json" }],
  "templates": [{ "id": "rust-throughput", "label": "Throughput (Rust)", "dir": "templates/throughput" }],
  "components": ["components/read-thermostat.json"], // BPMN component element templates (ADR 0033 §4)
  "toolchain": {                       // commands run on the USER's machine
    "detect": ["cargo --version"],     // probe; missing => offer install link, never auto-install
    "run":     "cargo run --release",
    "compile": "cargo build --release",
    "targets": ["aarch64-apple-darwin", "x86_64-unknown-linux-gnu"]
  },
  "requires": { "node": ">=22" }
}
```

The host loads grammars only for file types present in the open project (lazy per the user's
"only load the grammar for the current file type" requirement), stamps templates via the
existing scaffolder, and configures the supervisor from `toolchain` instead of hard-coding Deno.

### Trust model

Toolchain commands run on the user's machine, so the default is **allowlist + explicit
consent**: on install, surface the declared `fileTypes`, `templates`, and every `toolchain`
command; the first run prompts before executing each command. Two escape hatches: a per-
extension **"approve always"** and a global **yolo mode** that suppresses prompts. Off by
default; the consent UI must list exactly what will run. (User-confirmed.)

### Discovery / install

UI installs by npm package name; discovery via npm registry search for the `nano-ide-ext-*`
keyword, augmented by a curated first-party list. Install = fetch tarball into a per-console
`extensions/` dir; never a global install. Offline: previously-installed extensions keep
working; registry search degrades to the curated cache.

## Consequences

- `projects.rs` `TEMPLATES`/scaffolder/supervisor become extension-driven; Deno is just the
  built-in `nano-ide-app-deno` pack — zero regression, existing projects unchanged.
- Project config gains `lang` + `app` kind fields (separate axes; e.g. Rust + GUI).
- Editor lazy-loads only the current file's grammar.
- Polyglot (0008) and GUI (0009) become packs, not core changes; embed-Nano (0005) is a future
  `nano-ide-app-*` capability.

## Open questions

- Grammar depth: ship TextMate only, or allow LSP (rust-analyzer) via the consented toolchain?
- Version pinning / lockfile for installed extensions; signature verification for non-first-party.
- Cross-compile UX when the toolchain can't target a selected platform (Deno can; cargo needs targets).
