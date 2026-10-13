# spec-app — the Urban App manifest (`nano.app.json`)

This directory is the **single source of truth** for the Urban App manifest, the
declared-data document that binds an Urban RAD application together (models,
data, triggers, surfaces, workers, llm, security). See
[`docs/adr/0027-urban-app-manifest-spec.md`](../docs/adr/0027-urban-app-manifest-spec.md).

It is the sibling of [`spec-console/`](../spec-console/): one hand-authored
schema drives generated types, so hand-written DTOs can never drift from the
wire shape.

## Files

| Path | Role |
|---|---|
| `nano-app.schema.json` | The canonical **JSON Schema** (draft 2020-12) for `nano.app.json`. Source of truth **and** the `$schema` an editor uses for autocompletion. |
| `gen/nano-app.d.ts` | **Generated** TypeScript types (`AppManifest`, …). Committed; do not edit by hand. |
| `src/symbol-index.ts` | The **project symbol index** (ADR 0029): parses the project's BPMN/DMN/form files into the ids/shapes the manifest references. |
| `src/validate.ts` | The **fail-closed validator** (ADR 0027 §4): schema shape + cross-reference rules, with JSON-pointer diagnostics. |
| `src/index.ts` | Package entry — re-exports the types, the index, and the validator. |
| `src/tokens.ts` | The canonical **`--nano-*` colour palette** (issue #1005) — the machine-readable map + CSS renderer behind the `./tokens` and `./tokens.css` subpath exports. |
| `tokens.css` | **Generated** palette CSS (`:root` custom properties) — the `./tokens.css` subpath export the console imports. Committed; regenerate with `npm run build`. |
| `examples/*.nano.app.json` | Valid example manifests (fixtures — must pass validation). |
| `examples/invalid/*.nano.app.json` | Fixtures that must be **rejected** (proves the schema is fail-closed, ADR 0027 §4). |
| `test/` | `node --test` unit tests + model fixtures for the index and validator. |

## The symbol index & validator (ADR 0029 + 0027 §4)

Beyond the schema, this package is the **manifest library** consumed by the
console App panels and the Node App loader/compile gate:

```ts
import { buildSymbolIndex, validateManifest } from "@nanobpm/nano-app-schema";

// 1. Index the project's models (the one enumeration source).
const index = await buildSymbolIndex([
  { path: "processes/heating.bpmn", kind: "bpmn", text /* file body */ },
  { path: "decisions/triage.dmn",   kind: "dmn",  text },
  { path: "forms/confirm.form",     kind: "form", text },
]);
// -> { processes, messages, decisions, forms, parseErrors }

// 2. Validate a manifest fail-closed. Pass the index to enable the
//    model-resolving rules (start/message/decision); omit it for a
//    manifest-only lint (schema + intra-manifest references).
const { ok, diagnostics } = validateManifest(manifest, index);
// diagnostics: [{ severity, pointer /* JSON Pointer */, message, code }]
```

**Shape first, then cross-reference.** A manifest that fails schema validation
returns those errors and stops; otherwise the cross-reference rules run: every id
the manifest names (`triggers[].action.start`/`.message`, `surfaces.chat.agent`,
`workers[].llm`, `llm[].output.decision`, `data.default`, connections) must
resolve — within the manifest or against the symbol index. These are the same §4
rules the three gates (console save, the App compile, App boot) enforce, so a
mistyped id becomes an inline marker instead of a silent runtime no-op.


## The token palette (`./tokens`, `./tokens.css`) — issue #1005

The `--nano-*` colour palette is part of the same shared presentation contract as
the schema, so it ships as **subpath exports of this one package** rather than a
separate npm unit — its only consumers (the console and the Urban runtime) are
exactly the two that already depend on `@nanobpm/nano-app-schema`. The palette
values live once in `src/tokens.ts` (`NANO_PALETTE`), and are exposed two ways:

```ts
// Machine-readable map — the console drift test binds against it; the Urban
// runtime imports it at gen-runtime time to inline the palette.
import { NANO_PALETTE, TOKEN_KEYS, tokenCssVar, renderPaletteCss } from "@nanobpm/nano-app-schema/tokens";
```

```css
/* The palette as :root custom properties — the console @imports this in place
   of any inline hex, so its rendered theme is byte-identical. */
@import "@nanobpm/nano-app-schema/tokens.css";
```

`tokens.css` is **generated** from `NANO_PALETTE` by `npm run build`; a unit test
locks the committed CSS to the map so the two can never drift. The console's own
`tokens.drift.test.ts` additionally locks the console's `TOKEN_KEYS`/`cssVar()` to
these exports — the build-time guard that replaces the old runtime-only bridge.

## TypeScript-only, by design

The Urban App is a **Node/TypeScript** binary, so the manifest is consumed only
by TypeScript — the console **App panels** (authoring) and the **Node App
loader** (running). It is *not* read by the Rust server, which handles project
files as opaque bytes (ADR 0027 §1). So this schema generates **TypeScript**
types only; there is no Rust emitter.

## Env & secret substitution

Any string value may be a `${VAR}` or `${VAR:-default}` reference, resolved at
App boot / IDE Run and **never persisted** (ADR 0027 §5). Consequence: a
committed `nano.app.json` carries no secrets. The schema validates the *shape*
of such a reference (via the `envTemplate` `$def`), not its resolved value.
Control-plane enums that the ADR examples never template (e.g. `runtime.engine`,
`security.mode`) stay strict; the fields the ADRs actually template — datasource
`driver`/`url`, credentials, model ids, paths — accept templates.

## Regenerate & validate

From the repo root:

```sh
make generate-app-manifest
```

or directly (from this directory):

```sh
npm install          # first time
npm run validate     # validate every example against the schema
npm run gen          # regenerate gen/nano-app.d.ts
npm run typecheck    # tsc --noEmit over src/
npm run test:unit    # node --test (index + validator)
npm test             # all of the above: validate + gen --check + typecheck + unit (CI gate)
```

`npm run gen -- --check` (and `scripts/generate-app-manifest.sh --check`) fail if
the committed `gen/nano-app.d.ts` is stale — run it in CI after any schema edit.
