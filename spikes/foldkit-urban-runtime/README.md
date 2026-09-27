# Foldkit Urban-App renderer spike (issue #958)

A **second, independent renderer** for the Urban App `page.json` contract
(ADR 0042), written in [Foldkit](https://foldkit.dev) (Elm-architecture,
Effect-based TypeScript) instead of the shipped hand-rolled vanilla-DOM
`RENDERER_JS` in `server/src/console/app_pages.ts`.

It exists to exercise ADR 0042's pluggable-renderer seam: it consumes the
**identical** `/app/pages`, `/app/data`, `/app/actions/start` API and the same
`text` / `actionForm` / `dataGrid` node vocabulary, so swapping renderers needs
**zero data migration**. It is **not** wired into production serving — the
vanilla renderer stays the default.

## Layout

| Path | What |
| --- | --- |
| `src/schema.ts` | `page.json` contract as an Effect Schema (validates every page — the main win over the untyped vanilla renderer) |
| `src/main.ts` | The renderer: `Model`, fact-named `Message` union, exhaustive `update`, `Command`s (fetch page/grid, start process), interval-refresh `Subscription`, and the `view` |
| `src/entry.ts` | Boots the Foldkit `Runtime` |
| `mock/` | Vite dev/preview middleware mocking `/app/*` + a representative "Fleet" fixture |
| `verify.py` | Playwright headless render check |

## Run / measure

```sh
npm install
npm run typecheck
npm run build        # prints raw + gzip bundle size (reportCompressedSize)
npm run preview      # serves the built bundle with the /app/* mock
```

## Findings (see ADR 0042 addendum)

Gzipped, JS-only:

| Renderer | raw | gzip |
| --- | --- | --- |
| Vanilla `RENDERER_JS` (baseline) | 12.3 KB | **3.5 KB** |
| Foldkit spike | 308.6 KB | **102.8 KB** |

≈ **30× larger gzipped**. The weight is the Effect + Foldkit runtime and is
largely fixed regardless of page complexity. For the small, mostly-static,
end-user pages this surface serves, that floor is not justified — so the
recommendation is to **keep the vanilla renderer as the default** and revisit
Foldkit for a developer-facing, long-lived, stateful surface once it reaches a
stable (≥1.0) release.
