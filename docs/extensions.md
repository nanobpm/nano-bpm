# Extensions — using, authoring & publishing guide

The Nano BPM console (the RAD IDE) is **extensible**. Language support, project
templates, example apps, agentic-SDLC apps, event triggers, outbound
connectors, and colour themes are all shipped as **extension packs** — plain
npm packages the console discovers, installs, and wires in at runtime.

This guide covers three things:

1. [**Extensions and the marketplace**](#extensions-and-the-marketplace) — what a
   pack is, how discovery/install/trust work, and where packs live on disk.
2. [**Using an extension**](#using-an-extension) — how to find, install, trust,
   update, and remove a pack from the console, as a consumer.
3. [**Creating and publishing extensions**](#creating-and-publishing-extensions) —
   the package layout, the `nano-ide.ext.json` manifest, every pack kind with a
   minimal example, and how to publish.

The **authoritative manifest schema** is the Rust source
[`server/src/console/extensions.rs`](../server/src/console/extensions.rs) — Nano
BPM is the single source of truth; packs target it. First-party packs live in the
[`nanobpm/nano-ide`](https://github.com/nanobpm/nano-ide) repo (see
[`docs/nano-repositories.md`](nano-repositories.md)).

---

## Extensions and the marketplace

### What an extension is

An extension is a **plain npm package** that carries a single
[`nano-ide.ext.json`](#the-nano-ideextjson-manifest) manifest at its package
root. The manifest is read as **declared data** — nothing in it is ever `eval`'d.
It drives a few well-defined console seams:

- the editor grammar map (which Monaco language a file extension gets);
- the project scaffolder (which starter templates / example apps exist);
- the run/compile supervisor (which on-machine toolchain runs or compiles a
  project);
- the BPMN palette (installable element templates), plus inbound **triggers**
  and outbound **workers/connectors**;
- console colour **themes**.

### The marketplace: discovery, categories, official vs community

The console's **Extensions** tab (a Studio-profile tab) is the marketplace. It
discovers packs by shelling out to
`npm search keywords:nano-ide-ext --searchlimit=250` — **every** public npm
package tagged with the `nano-ide-ext` keyword is listed (the explicit
`--searchlimit` is required because `npm search` otherwise returns only its top
20 hits, which would silently hide packs as the ecosystem grows past 20).

A pack's **category** is derived from a *second* keyword, tested in a fixed
priority order (first match wins):

| Marketplace category | Keyword           |
| -------------------- | ----------------- |
| `lang`               | `nano-ide-lang`   |
| `app`                | `nano-ide-app`    |
| `example`            | `nano-ide-example`|
| `theme`              | `nano-ide-theme`  |
| `trigger`            | `nano-ide-trigger`|
| `agentic-sdlc`       | `nano-ide-agentic-sdlc` |
| `other`              | *(none of the above)* |

A pack is shown as **official** when it is published under the `@nanobpm/` npm
scope; anything else carrying the marketplace keyword is a **community** pack. The
console will install either.

> **Category vs manifest `kind`.** The marketplace *category* above is derived
> from npm keywords for browsing. The manifest's `kind` field is a smaller set —
> `lang`, `app`, `example`, `theme`, `trigger` — that tells the host which seams
> the pack drives. `agentic-sdlc` is a *curated category* for `example`/`app`
> packs that happen to be agentic-SDLC apps (e.g. `@nanobpm/urban-pr-review`,
> whose `kind` is `example`); it is not a separate manifest `kind`. Outbound
> **connectors** are likewise a *capability* (a pack's `workers[]` +
> `components[]`), not a distinct kind — see [Connectors](#connector--outbound-workers).

### Install, update, and where packs live

When you install a pack from the console, the server:

1. runs `npm pack <pkg>` to fetch the published tarball, then
2. extracts it with `tar xzf … --strip-components=1` into
   `<workspace>/extensions/<scope__name>/` (a `@scope/name` package flattens to
   `scope__name`), and
3. reads the root `nano-ide.ext.json` as data.

Because install uses `npm pack` (not `npm install`), **no `preinstall`/
`postinstall`/`prepare` scripts ever run** — a pack is inert content, not code the
host executes at install time. `npm` must be on the host's `PATH`.

**Updates** are detected by comparing the installed version (from the pack's
bundled `package.json`) against the latest published version (`npm view <name>
version --prefer-online`); a newer version surfaces an **Update** affordance. The
marketplace listing is **cached server-side for a few minutes** and computed
**single-flight** (concurrent callers — several Studio tabs, the left-rail update
badge — share one computation, and the per-installed-pack `npm view` probes run
at a small bounded concurrency rather than one process per pack), so polling for
updates can never fan an unbounded burst of `npm` processes out at the host. The
Extensions tab's **Check now** button bypasses the cache TTL (still
single-flight) for an on-demand refresh. Re-installing replaces the directory
cleanly (no stale files).

**Built-ins.** The `deno` language runtime and the `deno-gui` app template ship
**built into the server binary**, so the console works offline with zero installs
and existing Deno projects are unchanged. Built-ins cannot be removed; every other
pack (including `rust`) is installed from npm.

### Trust and consent

A pack's *manifest* is pure data, but a `lang`/`app` pack's **toolchain commands
run on your machine**, and a `trigger`/connector pack's **driver or worker child
runs as a supervised process**. Both are gated by a per-workspace trust store at
`<workspace>/extensions/trust.json`:

- **per-extension approve-always** — allow one pack's code to run without
  prompting;
- a global **yolo** bypass — trust everything.

Both are **off by default**; built-in packs are pre-trusted. Manifest reading,
grammar/theme/template wiring, and BPMN element templates need no trust (they run
nothing).

> **Scope of the gate.** The trust store gates every path that runs pack code:
> `lang`/`app` toolchain **run/compile** commands, and a `trigger`/connector
> pack's **driver/worker child process** (withheld until the contributing pack is
> approved). Prefer official (`@nanobpm/`) packs, and only approve packs whose
> driver/worker/toolchain code you trust.

---

## Using an extension

This section is for people who **install and run** extensions. To author a pack,
read [Creating and publishing extensions](#creating-and-publishing-extensions).

### Before you begin

Install Node.js 22.18 or later on the console host. Put `npm` on the host `PATH`.
The console runs `npm` to fetch and install every pack. Built-in packs need no
install. To run Nano itself, see the
[User Guide](../USERGUIDE.md#get-started-with-c8ctl).

### Find an extension

1. Open the console.
2. Select the **Extensions** tab.
3. Browse the list. The marketplace shows every public pack that carries the
   `nano-ide-ext` keyword.
4. Read each pack's category and source. An **official** pack uses the `@nanobpm/`
   scope. A **community** pack does not.

### Install an extension

1. Select a pack.
2. Select **Install**. The console fetches the pack with `npm pack` and extracts it
   into `<workspace>/extensions/<scope__name>/`.
3. Wait for the install to finish.

The install runs no pack scripts. A pack is inert content, not code that the host
runs at install time.

### Trust an extension

Some packs run code on your machine. A `lang` or `app` pack runs toolchain
commands. A `trigger` or connector pack runs a supervised child process.

The console blocks pack code until you approve it. To approve a pack:

1. Run the pack's action. For example, compile a project or start a trigger.
2. Read the trust prompt.
3. Approve the pack. The console records your choice in
   `<workspace>/extensions/trust.json`.

Approve only packs whose code you trust. Prefer official (`@nanobpm/`) packs.

### Update or remove an extension

The console compares the installed version against the latest published version. A
newer version shows an **Update** action. Select **Update** to replace the pack
cleanly.

To remove a pack, select **Remove**. You cannot remove a built-in pack.

---

## Creating and publishing extensions

### Package layout

A pack is an npm package whose **published tarball root** contains the manifest:

```
my-pack/
├─ package.json          # name, version, keywords (see below)
├─ nano-ide.ext.json     # the manifest — MUST be at the package root
├─ README.md             # shown in the console's pack detail view
└─ …                     # kind-specific assets (templates, appDir, driver, …)
```

`--strip-components=1` means the manifest must sit at the **package root**, i.e.
be included in the tarball at the top level. Use `package.json`'s `files` field
(or `.npmignore`) to ship `nano-ide.ext.json`, the `README.md`, and every asset a
kind references (template sources, `appDir` contents, trigger `driver` /
worker `entry` files and **their bundled dependencies** — drivers run with the
pack directory as their working directory, so vendor anything they import).

`package.json` essentials:

```jsonc
{
  "name": "nano-ide-lang-rust",          // convention: nano-ide-<kind>-<slug>
                                          // official packs use the @nanobpm/ scope
  "version": "1.0.0",
  "description": "Rust language pack for the Nano console",
  "keywords": ["nano-ide-ext", "nano-ide-lang"],   // discovery + category
  "files": ["nano-ide.ext.json", "README.md", "templates/"],
  "repository": "github:you/nano-ide-lang-rust",
  "license": "MIT"
}
```

- `keywords` **must** include `nano-ide-ext` (discovery) plus the one category
  keyword from the [table above](#the-marketplace-discovery-categories-official-vs-community).
- The `@nanobpm/` scope marks a pack **official** (requires org membership to
  publish); community packs use any unscoped or your-own-scope name.

### The `nano-ide.ext.json` manifest

Read as data by the host. Keys are **camelCase**. The common fields:

| Field         | Type     | Notes |
| ------------- | -------- | ----- |
| `id`          | string   | Stable, unique pack id (e.g. `rust`). Used by the console for install/remove and template wiring. |
| `kind`        | enum     | `lang` \| `app` \| `example` \| `theme` \| `trigger`. |
| `displayName` | string   | Human name shown in the console. |
| `icon`        | string?  | Optional inline SVG (preferred) or `data:`/`http:` URL, used to badge project cards. |

The remaining fields are kind-specific and covered below. Every kind-specific
array defaults to empty, so a manifest only declares what it needs.

#### Guided journeys (`tours[]`) — optional, any kind

Any pack may also contribute **guided journeys** — interactive, step-by-step
tours that teach the capability the pack adds, so onboarding scales with the
ecosystem instead of a hardcoded list in the console
([ADR 0049](adr/0049-guided-journeys.md)). Each `tours[]` entry has an `id`,
`title`, one-line `blurb`, optional `profiles` (`studio`/`observe`; empty ⇒
studio only), optional journey-level `preconditions[]` (gates that must all hold
for the journey to be offered) and `successWhen` (the gate that marks the journey
as having actually worked — absent ⇒ orientation only), and `steps[]`. A step is
one of three `kind`s:

- **`spotlight`** — highlight an element by CSS `selector`. Views tag the target
  with a `data-tour="…"` attribute, so the selector is the attribute form
  `[data-tour="…"]` (a bare word like `workers-tab` is read as a tag selector and
  won't match);
- **`note`** — anchorless, centered framing;
- **`handoff`** — offer a terminal command or URL in `copy` for the user to run.
  The console renders `copy` as **inert text and never executes it**; for an
  untrusted pack the handoff command is stripped before it ever reaches the
  client. A handoff step may `verifyPollingJobType` to auto-advance once an
  external worker is seen polling that job type — the only code-free verification
  a pack can declare.

```jsonc
{
  "tours": [
    {
      "id": "first-worker",
      "title": "Run your first worker",
      "blurb": "Wire a job type to an external worker and watch it poll.",
      "steps": [
        { "id": "open-workers", "kind": "spotlight", "selector": "[data-tour=\"workers-tab\"]",
          "title": "Open Workers", "body": "This is where connected workers appear." },
        { "id": "start-worker", "kind": "handoff", "title": "Start the worker",
          "body": "Run this in your project, then come back.",
          "copy": "deno run -A worker.ts", "verifyPollingJobType": "greet" }
      ]
    }
  ]
}
```

Journeys are optional; a pack needs none. They are the same mechanism the console
uses for its own built-in onboarding.

---

### `lang` — a language pack

Adds file types + editor language, an on-machine **toolchain** to run/compile
projects, optional starter **templates**, and curated **IntelliSense**.

```jsonc
{
  "id": "rust",
  "kind": "lang",
  "displayName": "Rust",
  "fileTypes": [{ "ext": ".rs", "monacoLang": "rust" }],
  "toolchain": {
    "detect": ["cargo", "--version"],      // proves the toolchain is installed
    "run": ["cargo", "run"],               // cwd = project dir
    "compile": ["cargo", "build", "--release"],
    "targets": ["x86_64-unknown-linux-gnu", "aarch64-apple-darwin"],
    "installUrl": "https://rustup.rs",
    "installHint": "Install Rust via rustup, then reopen the project."
  },
  "templates": [
    { "id": "rust-worker", "label": "Rust job worker", "description": "A minimal Nano worker in Rust." }
  ],
  "configFields": [
    { "key": "cargoBin", "label": "Cargo binary", "env": "NANOBPMN_CARGO_BIN", "default": "cargo (on PATH)" }
  ],
  "intellisense": [
    {
      "monacoLang": "rust",
      "triggerCharacters": ["."],
      "completions": [{ "label": "activateJobs", "kind": "method", "detail": "Poll for jobs" }],
      "hovers": [{ "symbol": "defineWorker", "contents": "Register a job worker." }],
      "signatures": [{ "trigger": "defineWorker", "label": "defineWorker(opts)", "parameters": [{ "label": "opts" }] }]
    }
  ]
}
```

- `toolchain.runConfigs[]` lets one project expose several named run/compile
  targets (surfaced in a Run/Target dropdown); each has `id`, `label`, optional
  `default`, `run`, `compile`, and an `env` map that overrides project env.
- `toolchain` commands are **trust-gated** (see [Trust](#trust-and-consent)).
- `configFields[]` are read-only panel knobs; when `env` is set the panel shows
  the value resolved from that environment variable.

Keywords: `["nano-ide-ext", "nano-ide-lang"]`.

### `app` — a runnable project template

A starter **project** the New-Project picker offers: one or more `templates[]`
plus a `toolchain` run/compile profile (often reusing a lang pack's runtime).

```jsonc
{
  "id": "camunda8-rest",
  "kind": "app",
  "displayName": "Camunda 8 · REST worker",
  "templates": [
    { "id": "c8-rest", "label": "Camunda 8 REST worker", "description": "Poll and complete jobs over REST.", "lang": "deno" }
  ],
  "toolchain": { "run": ["deno", "run", "-A", "main.ts"] }
}
```

Keywords: `["nano-ide-ext", "nano-ide-app"]`.

### `example` — a complete app copied into a new project

Ships a ready-to-run app under `appDir`; selecting it **copies the whole
directory** into a new project. Declare the lang packs it needs via `requires[]`.

```jsonc
{
  "id": "urban-pr-review",
  "kind": "example",
  "displayName": "Urban PR Review — model-first",
  "appDir": ".",
  "requires": ["deno"],
  "summary": "A durable multi-round loop driving a GitHub PR to convergence against an automated reviewer."
}
```

Keywords: `["nano-ide-ext", "nano-ide-example"]`. An example that is an
**agentic-SDLC** app additionally carries `nano-ide-agentic-sdlc` to surface under
that curated category:
`["nano-ide-ext", "nano-ide-example", "nano-ide-agentic-sdlc"]`.

### `theme` — console colour themes

Pure data — no toolchain, nothing runs. Each theme maps the console's design-token
vocabulary (see [`console/src/theme/themes.ts`](../console/src/theme/themes.ts))
to CSS colours over a `light` or `dark` base.

```jsonc
{
  "id": "nord",
  "kind": "theme",
  "displayName": "Nord",
  "themes": [
    {
      "id": "nord-dark",
      "label": "Nord (dark)",
      "appearance": "dark",
      "tokens": { "app": "#2e3440", "panel": "#3b4252", "accent": "#88c0d0" }
    }
  ]
}
```

Keywords: `["nano-ide-ext", "nano-ide-theme"]`. Unknown token keys are ignored;
missing keys fall back to the base appearance.

### `trigger` — an inbound event source

Contributes one or more trigger source **kinds** (the *inbound* edge: external
event → process start). Each entry declares a `kind` (the trigger type string a
BPMN manifest trigger references, e.g. `mqtt` — note the field is `kind`, whereas
`workers[]` uses `type`), the config the console renders, and an optional
out-of-process **driver** the runtime auto-launches and supervises while an app
using that trigger runs.

```jsonc
{
  "id": "mqtt",
  "kind": "trigger",
  "displayName": "MQTT",
  "triggerSources": [
    {
      "kind": "mqtt",
      "displayName": "MQTT topic",
      "transport": "webhook",
      "configFields": [
        { "key": "broker", "label": "Broker URL", "env": "MQTT_BROKER" },
        { "key": "topic", "label": "Topic" }
      ],
      "driver": "driver.ts"
    }
  ]
}
```

- `transport` is `webhook` (the universal ingress) in v1 — the driver POSTs each
  event to the trigger ingress. It is forward-declared so a pack states its
  contract explicitly.
- With a `driver`, the runtime launches `driver.ts` (a Node/Deno
  `.ts`/`.js`/`.mjs`) with the **pack directory as its working directory**,
  restarts it with backoff on crash, and kills it when the app stops. Omit
  `driver` for a declaration-only source you run out-of-band.
- Drivers/workers run as **supervised child processes** and their deps must be
  vendored in the tarball. The driver/worker child is **trust-gated** (like a
  `lang`/`app` toolchain command): it is withheld until the pack is approved in
  Extensions.

Keywords: `["nano-ide-ext", "nano-ide-trigger"]`.

### Connector — outbound workers

A **connector** is the *outbound* edge (an engine job → an external effect, e.g.
"post a Slack message"). It is a **capability** you add to a pack (typically an
`app` or `example`), not a separate kind: declare `workers[]` (the runtime edge)
alongside `components[]` (the BPMN element templates that put the task on the
palette — see [ADR 0050](adr/0050-urban-connectors-outbound-io-and-project-enablement.md)
and [ADR 0033](adr/0033-urban-element-templates-first-class-components.md)).

```jsonc
{
  "id": "slack-connector",
  "kind": "app",
  "displayName": "Slack connector",
  "components": ["templates/slack-send-message.json"],   // element templates
  "workers": [
    {
      "type": "slack:send-message",        // MUST equal the template's zeebe:taskDefinition:type
      "entry": "workers/slack.ts",         // uses @nanobpm/worker's defineWorker
      "displayName": "Slack · send message",
      "maxParallelJobs": 10,
      "configFields": [
        { "key": "token", "label": "Slack bot token", "env": "SLACK_BOT_TOKEN" }
      ]
    }
  ]
}
```

- `type` is the design→runtime **seam**: it must equal the backing element
  template's `zeebe:taskDefinition:type`, so a task dragged from the palette
  resolves to this worker.
- With an `entry`, the runtime **auto-launches + supervises** the worker (one
  child process per enabled worker) while an app that enables it runs. Omit
  `entry` for a declaration-only worker run out-of-band.
- Config-field defaults are **env pointers, never inline secrets**.

### Publishing

1. Ensure `package.json` has the right `keywords` (`nano-ide-ext` + one category)
   and that `files`/`.npmignore` include `nano-ide.ext.json` and every referenced
   asset (bundle driver/worker deps — they are not installed on the host).
2. `npm publish --access public` (official `@nanobpm/*` packs require org
   membership; community packs publish under any name you own).
3. The pack appears in the console marketplace once npm's search index picks it
   up (minutes to hours). For a pack a user already has installed, the console
   also does a direct `npm view` so a freshly-published fix surfaces its **Update**
   affordance promptly.
4. To ship a fix, bump `version` and re-publish; the console detects the newer
   version and offers **Update**.

Because the host never runs your install scripts and reads the manifest as data,
a pack is safe to browse; the only code that ever executes is a `lang`/`app`
toolchain command or a `trigger`/connector driver/worker child process — each
**trust-gated** on the user's machine (withheld until the pack is approved in
Extensions).
