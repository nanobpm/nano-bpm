# website — the `nanobpm.io` publishing site

Generates the static site served at **https://nanobpm.io**, the canonical home
for every schema and namespace this repository publishes. The site is
**generated from the in-repo sources of truth on every deploy** — there are no
hand-maintained copies — so a published URL can never drift from the artifact it
names.

## What is published

| URL | Source of truth | Kind |
|---|---|---|
| `https://nanobpm.io/spec-app/nano-app.schema.json` | [`spec-app/nano-app.schema.json`](../spec-app/nano-app.schema.json) | JSON Schema (the `$schema` editors fetch for `nano.app.json`) |
| `https://nanobpm.io/schema/shapes/1.0` | [`console/src/moddle/nanoShapes.ts`](../console/src/moddle/nanoShapes.ts) | BPMN `nano:` moddle namespace (ADR 0040) |

Each artifact's **output path is derived from its own declared identity** (the
schema's `$id`, the moddle descriptor's `uri`). The build asserts that identity
resolves to `nanobpm.io`, and a repo-wide guard fails the build if any tracked
file still references the legacy `.dev` domain — so the URLs, the `$id`s and the
served files can only ever agree.

## Site structure

| Path | Content | Source |
|---|---|---|
| `/` | Marketing landing page ("The load-bearing runtime for agent systems") | `homeHtml()` in [`build.mjs`](build.mjs) — always generated (pure static HTML, no toolchain) |
| `/demo/` | The in-browser Bojtos demo (ADR 0043), a live wasm-engine run | [`website/demo/`](demo) Vite app (`base: "/demo/"`); copied in when its `dist/` has been built |
| `/schemas/` | The published-schema registry (the table of the URLs above) | `schemasHtml()` in [`build.mjs`](build.mjs) |
| `/nano-coder/docs/` | nano-coder usage docs (website-only; never in the gateway's `/docs/`) | nanobpm/nano-coder's `docs/` at its **latest release**, rendered by [`nano-coder-docs.mjs`](nano-coder-docs.mjs) with the shared [`console/scripts/docs-render.mjs`](../console/scripts/docs-render.mjs) |

The landing page and schema registry are always emitted, so `node website/build.mjs`
runs without the demo's npm toolchain (e.g. the zero-dep `schemas` CI drift check);
`/demo/` is included only when `website/demo/dist` exists. The published schema URLs
in the table above own their own paths and are unaffected by this layout.

## Build

```sh
npm ci --prefix website  # markdown-it, for /nano-coder/docs/ (pinned to the console's)
node website/build.mjs   # Node >= 23.6 / 24 (imports the .ts descriptor directly)
```

`/nano-coder/docs/` is fetched live from nano-coder's latest release
(`GITHUB_TOKEN` used when set). If the fetch fails, the build falls back to the
vendored snapshot in `website/data/nano-coder-docs/` (with its `VERSION`) and
warns; in CI (`CI=true`) a failed fetch fails the build instead. A release with
no `docs/` is skipped with a warning (no nav link). `NANO_CODER_DOCS_OFFLINE=1`
skips the fetch (the tests use it), and `NANO_CODER_DOCS_SNAPSHOT=<dir>` points
at another snapshot. Refresh the snapshot with
`node website/scripts/update-nano-coder-docs.mjs`. nano-coder's release workflow
sends the `nano-coder-released` `repository_dispatch` to rebuild the site; the
daily cron rebuild is the backstop.

Output lands in `website/_site/` (git-ignored). CI builds it on every PR (drift
guard) and deploys it to GitHub Pages on merge to `main` — see
[`.github/workflows/pages.yml`](../.github/workflows/pages.yml).

## One-time hosting setup (repo admin)

1. **Settings → Pages → Build and deployment → Source: GitHub Actions.**
2. DNS for `nanobpm.io` at the registrar:
   - apex `A`/`ALIAS` → GitHub Pages IPs (`185.199.108–111.153`), or an
     `ALIAS`/`ANAME` to `<org>.github.io`.
   - `www` `CNAME` → `<org>.github.io` (optional).
3. The `CNAME` file is emitted into `_site` by the build, so Pages keeps the
   custom domain across deploys. Enable **Enforce HTTPS** once the cert issues.
