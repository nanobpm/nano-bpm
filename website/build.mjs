// Builds the static site served at https://nanobpm.io — the canonical home for
// every schema/namespace the project publishes. The site is *generated from the
// in-repo sources of truth on every deploy* (no hand-maintained copies), so a
// published URL can never drift from the artifact it names:
//
//   * https://nanobpm.io/spec-app/nano-app.schema.json  <- spec-app/nano-app.schema.json
//   * https://nanobpm.io/schema/shapes/1.0              <- console/src/moddle/nanoShapes.ts
//
// The output path of each artifact is *derived from its own declared identity*
// (the schema's `$id`, the moddle descriptor's `uri`), and the build asserts
// that identity resolves to this domain — so a stray edit that points an `$id`
// somewhere else fails the build instead of shipping an unresolvable schema.
//
// Run: `node website/build.mjs` (Node >= 23.6 / 24 — imports the `.ts` moddle
// descriptor directly via built-in type stripping; matches the spec-app CI job).

import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  mkdirSync,
  writeFileSync,
  copyFileSync,
  cpSync,
  existsSync,
  readdirSync,
  rmSync,
  readFileSync,
} from "node:fs";
import { dirname, join } from "node:path";

const DOMAIN = "nanobpm.io";
const ORIGIN = `https://${DOMAIN}`;

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, "..");
const outDir = join(here, "_site");

/** Resolve a declared identity URI to its on-disk output path under `_site`. */
function outFor(uri, { indexHtml = false } = {}) {
  const u = new URL(uri);
  if (u.origin !== ORIGIN) {
    throw new Error(
      `published identity must live on ${ORIGIN}, got ${uri} (origin ${u.origin}). ` +
        `Migrate the artifact's declared identity onto the owned domain.`,
    );
  }
  const rel = indexHtml ? join(u.pathname, "index.html") : u.pathname;
  return join(outDir, rel);
}

function write(path, contents) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, contents);
  return path;
}

// --- Drift guard: no tracked file may reference the legacy `.dev` domain ------
// Built from parts so this guard never matches its own source.
const legacyNeedle = ["nanobpm", "dev"].join("\\.");
try {
  const hits = execFileSync(
    "git",
    ["grep", "-nI", "-E", legacyNeedle, "--", ".", ":!website/build.mjs"],
    { cwd: repoRoot, encoding: "utf8" },
  );
  if (hits.trim()) {
    throw new Error(
      `legacy schema domain still referenced in tracked sources — migrate to ${ORIGIN}:\n${hits}`,
    );
  }
} catch (e) {
  // `git grep` exits 1 (no matches) => clean. Any other failure is real.
  if (typeof e.status === "number" && e.status === 1) {
    /* clean */
  } else {
    throw e;
  }
}

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

// Whether the bundled docs (console/public/docs) were generated for this build.
// Computed up-front so the landing nav + footer only advertise /docs/ when the
// docs are actually shipped — the zero-dep schemas CI run of build.mjs skips the
// docs build, and pages.yml (the real deploy) builds them first.
const docsPresent = existsSync(
  join(repoRoot, "console", "public", "docs", "index.html"),
);

// Same graceful pattern for the whitepaper: the console build renders
// docs/whitepaper.md into console/public/whitepaper/index.html; we publish that
// self-contained page verbatim to /whitepaper and only advertise it on builds
// that actually ship it (the zero-dep schemas CI run skips the console build).
const whitepaperPresent = existsSync(
  join(repoRoot, "console", "public", "whitepaper", "index.html"),
);

const published = [];

// --- 1. Urban App manifest JSON Schema (spec-app/nano-app.schema.json) --------
{
  const src = join(repoRoot, "spec-app", "nano-app.schema.json");
  const schema = JSON.parse(readFileSync(src, "utf8"));
  if (!schema.$id) throw new Error(`${src} has no $id`);
  const dest = outFor(schema.$id); // asserts origin === nanobpm.io
  mkdirSync(dirname(dest), { recursive: true });
  copyFileSync(src, dest);
  published.push({
    title: schema.title || "AppManifest",
    url: schema.$id,
    kind: "JSON Schema (draft 2020-12)",
    desc:
      "The Urban App manifest (<code>nano.app.json</code>) — editors fetch this via the " +
      "<code>$schema</code> hint for autocompletion and validation.",
  });
}

// --- 2. Nano composed-shapes BPMN moddle namespace (nanoShapes.ts) ------------
{
  const mod = await import(
    join(repoRoot, "console", "src", "moddle", "nanoShapes.ts")
  );
  const descriptor = mod.nanoShapesModdle ?? mod.default;
  if (!descriptor?.uri) throw new Error("nanoShapes.ts exports no `uri`");
  const dest = outFor(descriptor.uri, { indexHtml: true }); // asserts origin
  write(dest, shapesNamespaceHtml(descriptor));
  published.push({
    title: `${descriptor.name} moddle namespace (${descriptor.prefix}:)`,
    url: descriptor.uri,
    kind: "BPMN moddle extension",
    desc:
      "The <code>nano:</code> extension elements (composed shapes, ADR 0040) that bpmn-js " +
      "serialises into <code>.bpmn</code>. This URI is the XML namespace identifier.",
  });
}

// --- Landing + Pages plumbing -------------------------------------------------
// The marketing landing page owns the site root (`/`) and is always generated
// (pure static HTML, no toolchain) so this build stays runnable without the
// demo's npm build — e.g. the zero-dep `schemas` CI drift check. The generated
// schema registry lives at `/schemas/`, and the ADR 0043 Bojtos in-browser demo,
// when its dist has been built, is served at `/demo/` (its Vite `base`). The
// published schema/namespace URLs are unaffected (they own their own paths).
write(join(outDir, "index.html"), homeHtml());
copyFileSync(join(here, "install-copy.mjs"), join(outDir, "install-copy.mjs"));
write(join(outDir, "architecture", "index.html"), architectureHtml());
write(join(outDir, "blog", "index.html"), blogIndexHtml());
write(join(outDir, "nano-coder", "index.html"), nanoCoderHtml());
for (const post of blogPosts()) {
  write(join(outDir, "blog", post.slug, "index.html"), blogPostHtml(post));
}
write(join(outDir, "schemas", "index.html"), schemasHtml(published));

// The console /stack page (served by the gateway binary, NOT part of this site)
// shares its landscape table with /architecture#landscape. Emit that table as a
// checked-in derived artifact the server injects into stack.html at serve time,
// so the single source of truth (website/data/landscape.json) drives both
// surfaces. The `schemas` CI job runs this build then `git diff --exit-code`s the
// artifact, so forgetting to regenerate fails the build instead of shipping drift.
writeFileSync(
  join(repoRoot, "server", "crates", "nano-server-console", "src", "landscape.gen.html"),
  `<!-- @generated by website/build.mjs from website/data/landscape.json — DO NOT EDIT.\n` +
    `     Single source of truth shared with the public /architecture#landscape page.\n` +
    `     Run \`node website/build.mjs\` to regenerate. -->\n` +
    `${landscapeTableHtml()}\n`,
);

// --- Bundled documentation site: console/public/docs -> /docs -----------------
// The end-user docs (USERGUIDE.md chunked + the standalone guides, including
// Getting Started with Urban) are generated by console/scripts/build-docs.mjs
// into console/public/docs/*.html — the SAME self-contained pages the gateway
// embeds and serves at a node's /docs. We publish that generated site verbatim
// to nanobpm.io/docs so a change to any doc source flows to both surfaces from
// one renderer (no drift, no second doc-site generator). When the docs have not
// been built (e.g. the zero-dep `schemas` CI drift check runs build.mjs alone),
// /docs is simply omitted — same graceful pattern as the demo dist below.
const docsDist = join(repoRoot, "console", "public", "docs");
if (docsPresent) {
  const docsOut = join(outDir, "docs");
  cpSync(docsDist, docsOut, { recursive: true });
  // The sidebar links between doc pages are extensionless (`/docs/usage`). The
  // in-app gateway resolves those by appending `.html` (see server docs_asset).
  // GitHub Pages does NOT append `.html` for us, so mirror each `<slug>.html`
  // as `<slug>/index.html` — a directory index resolves the extensionless link
  // reliably on Pages (via the standard trailing-slash redirect). `index.html`
  // is skipped (it already serves `/docs/`). The pages are self-contained with
  // only absolute links, so serving from a subdirectory changes nothing.
  for (const entry of readdirSync(docsDist, { withFileTypes: true })) {
    if (!entry.isFile() || !entry.name.endsWith(".html")) continue;
    if (entry.name === "index.html") continue;
    const slug = entry.name.slice(0, -".html".length);
    const dir = join(docsOut, slug);
    mkdirSync(dir, { recursive: true });
    copyFileSync(join(docsDist, entry.name), join(dir, "index.html"));
  }
}

// --- Bundled whitepaper: console/public/whitepaper -> /whitepaper -------------
// The design paper is rendered from docs/whitepaper.md by
// console/scripts/build-whitepaper.mjs into console/public/whitepaper/index.html
// — the SAME self-contained page the gateway serves at a node's /whitepaper. We
// publish it verbatim so one renderer feeds both surfaces (no drift). Omitted on
// builds that didn't run the console build, exactly like /docs above.
if (whitepaperPresent) {
  cpSync(
    join(repoRoot, "console", "public", "whitepaper"),
    join(outDir, "whitepaper"),
    { recursive: true },
  );
}

const demoDist = join(here, "demo", "dist");
if (existsSync(join(demoDist, "index.html"))) {
  cpSync(demoDist, join(outDir, "demo"), { recursive: true });
  console.log("landing at / · demo at /demo/ · schema registry at /schemas/");
} else {
  console.log(
    "landing at / · schema registry at /schemas/ · (demo dist not built — /demo/ omitted; run the demo build for the live try-it page)",
  );
}
write(join(outDir, "CNAME"), `${DOMAIN}\n`);
write(join(outDir, ".nojekyll"), ""); // serve paths verbatim, skip Jekyll

console.log(`built _site for ${ORIGIN}:`);
for (const p of published) console.log(`  ${p.url}`);

// --- HTML helpers -------------------------------------------------------------
function esc(s) {
  return String(s).replace(
    /[&<>"]/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c],
  );
}

function page(title, body) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(title)}</title>
<style>
  :root { color-scheme: light dark; }
  body { font: 16px/1.6 system-ui, sans-serif; max-width: 46rem; margin: 3rem auto; padding: 0 1.25rem; }
  code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  code { background: color-mix(in srgb, currentColor 10%, transparent); padding: .1em .3em; border-radius: 4px; }
  pre { background: color-mix(in srgb, currentColor 8%, transparent); padding: 1rem; border-radius: 8px; overflow-x: auto; }
  h1 { font-size: 1.6rem; } h2 { font-size: 1.15rem; margin-top: 2rem; }
  a { color: inherit; } .muted { opacity: .7; }
  table { border-collapse: collapse; width: 100%; }
  td, th { text-align: left; padding: .35rem .6rem; border-bottom: 1px solid color-mix(in srgb, currentColor 15%, transparent); vertical-align: top; }
</style>
</head>
<body>
${body}
<hr>
<p class="muted">Served from <a href="/">nanobpm.io</a>.</p>
</body>
</html>
`;
}

// Official language logos (devicon originals) for the Polyglot section. Read from
// website/logos/*.svg at build time and inlined, so the page stays zero-dependency
// and renders offline (no external icon CDN). Rust's mark is monochrome and given
// a light root fill in the source SVG so it reads on the dark band.
function langChips() {
  const langs = [
    ["typescript", "TypeScript"],
    ["java", "Java"],
    ["rust", "Rust"],
    ["python", "Python"],
    ["csharp", "C#"],
    ["go", "Go"],
  ];
  return langs
    .map(([id, label]) => {
      const svg = readFileSync(join(here, "logos", `${id}.svg`), "utf8")
        .trim()
        .replace(/^<svg /, '<svg aria-hidden="true" focusable="false" ');
      return `      <span class="lang lang-${id}">${svg}${label}</span>`;
    })
    .join("\n");
}

/**
 * Extract a `//#region NAME … //#endregion NAME` block from a source file,
 * stripping the marker lines and any common leading indentation. Used to render
 * real, type-checked snippets on the site verbatim (see website/snippets/), so
 * the marketing code can never drift from the API it demonstrates.
 *
 * Blank lines immediately adjacent to the markers are dropped (they're just
 * breathing room around the region tags), but every code line is kept
 * byte-for-byte after de-indentation — so what's rendered is exactly what CI
 * type-checks.
 */
function readSnippetRegion(relPath, region) {
  const src = readFileSync(join(here, relPath), "utf8");
  const lines = src.split("\n");
  const start = lines.findIndex((l) => l.trim() === `//#region ${region}`);
  const end = lines.findIndex((l, i) => i > start && l.trim() === `//#endregion ${region}`);
  if (start === -1 || end === -1 || end <= start) {
    throw new Error(`snippet region "${region}" not found in ${relPath}`);
  }
  const body = lines.slice(start + 1, end);
  const indent = Math.min(
    ...body.filter((l) => l.trim() !== "").map((l) => l.match(/^ */)[0].length),
  );
  const out = body.map((l) => l.slice(indent));
  while (out.length && out[0].trim() === "") out.shift();
  while (out.length && out[out.length - 1].trim() === "") out.pop();
  return out.join("\n");
}

// Wires every installCommand() copy button on a page to the shipped handler.
function installCopyScript() {
  return `<script type="module">
import { bindInstallCopy } from "/install-copy.mjs";
bindInstallCopy(document, navigator.clipboard);
</script>`;
}

function installCommand(label, cmd = "curl -fsSL https://nanobpm.io/install.sh | sh") {
  return `<div class="install-copy" data-install-copy>
    <div class="install install-command" role="group" aria-label="${esc(label)}">
      <code>${esc(cmd)}</code>
      <button type="button" class="install-copy-button" aria-label="Copy install command" title="Copy install command" hidden>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true" focusable="false">
          <rect x="8" y="8" width="12" height="12" rx="2"/>
          <path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/>
        </svg>
      </button>
    </div>
    <p class="install-copy-status" role="status"></p>
  </div>`;
}

function homeHtml() {
  // The Code-first tab renders the `hero` region of website/snippets/hero-pr-review.ts
  // VERBATIM — a real, compilable `defineFlow` example type-checked against the
  // published @nanobpm/workflow in CI ("website snippet (typecheck)" job), so the
  // landing hero can never drift from the actual API. It's the SAME urban-pr-review
  // convergence loop the Model-first tab runs live (same task ids / signal / key).
  const heroCode = readSnippetRegion("snippets/hero-pr-review.ts", "hero");

  const body = `${siteNav()}

<section class="hero">
  <p class="eyebrow"><span class="arp">Advanced Research Prototype</span></p>
  <h1>Agent Graph Orchestration<br><span class="hl-sub">for the Developer Workstation.</span></h1>
  <p class="subhead">Graphs that run the loops. An entropy sink for agent fleets.</p>
  <p class="lede">A RAAD — Rapid Agent Application Development — environment that runs on
  your machine. Compose coding agents, tools, and human approvals into durable workflows.
  Code-first or Model-first Graphs, provider-agnostic, and small enough to start on a Raspberry Pi.</p>
  <div class="cta">
    <a class="btn primary" href="#try">See it live ↓</a>
    <a class="btn ghost" href="/demo/">Open the browser demo →</a>
  </div>
  ${installCommand("Install a Nano Workforce")}
  <p class="install-note">One command: installs the CLI, hires your coding agents,
  and brings up a Nano engine, a workforce, and the app.</p>
  <details class="install-manual">
    <summary>Or set it up by hand</summary>
    <div class="install multi" aria-label="Install and start the Nano engine by hand">
      <code>npm i -g @camunda8/cli</code>
      <code>c8ctl load plugin c8ctl-plugin-nano</code>
      <code>c8ctl nano start</code>
    </div>
    <p class="install-note">Just the engine. See the full manual sequence in
    <a href="/docs/get-started-with-c8ctl">Get started with c8ctl →</a></p>
  </details>
</section>

<section id="try" class="demo-tabs wrap">
  <h2 class="tabs-title">Code-first <span class="grad">or</span> Model-first Graphs.</h2>
  <p class="tabs-sub">One app — two authoring surfaces, the same engine.</p>
  <div class="tablist" role="tablist" aria-label="Authoring surface">
    <button class="tab" role="tab" id="tab-code" aria-controls="panel-code" aria-selected="true" tabindex="0">Code-first</button>
    <button class="tab" role="tab" id="tab-model" aria-controls="panel-model" aria-selected="false" tabindex="-1">Model-first</button>
  </div>

  <div class="tabpanel" id="panel-code" role="tabpanel" aria-labelledby="tab-code">
    <figure class="code pop">
      <figcaption>A loop where local coding agents iterate to convergence against GitHub Copilot’s adversarial PR reviews — authored as code.</figcaption>
      <pre><code>${highlightTs(heroCode)}</code></pre>
      <p class="code-note">Nano derives the executable model, the job types, the message
      correlation, and a generic worker. You write steps and handlers — nothing else.</p>
    </figure>
  </div>

  <div class="tabpanel" id="panel-model" role="tabpanel" aria-labelledby="tab-model" hidden>
    <iframe class="demo-frame" title="Live Model-first urban-pr-review demo on the wasm engine"
      data-src="/demo/?embed=1" loading="lazy" allow="fullscreen" allowfullscreen></iframe>
    <p class="tab-note">The real <code>urban-pr-review</code> BPMN model, executing live on the
    WebAssembly build of the engine — no server. Or <a href="/demo/">open it full-page →</a></p>
  </div>
</section>

<section class="band problem">
  <div class="wrap">
    <h2>Reasoning is an LLM concern. Durability isn't.</h2>
    <p>A primary agent orchestrating subagents is a good pattern — reasoning about what to run
    next is exactly what an LLM is for. But <em>holding</em> the dependency graph and the run state
    <b>in context</b> is not. That's durable state living in the most volatile, most expensive place
    you have: one compaction and the plan is gone; one overnight reboot and the run dies, the state
    with it, the tokens re-spent. And every token spent bookkeeping <em>what's done, what's blocked,
    what's next</em> is a token not spent on the code. It degrades both. Nano takes the orchestration
    out of context — durable, inspectable primitives that drive work to convergence instead of drift.
    The agent designs the graph; the engine runs it.</p>
  </div>
</section>

<section class="pillars wrap">
  <article>
    <h3>DRY your agent SDLC</h3>
    <p>Declare durable steps with <code>w.run</code> and durable waits with <code>w.signal</code>.
    Nano derives the model, job types, correlation, and workers — one source of truth, no copy-paste
    orchestration across every workflow.</p>
    <p class="proof">Author once. Nano derives the rest.</p>
  </article>
  <article>
    <h3>Code-first or Model-first Graphs</h3>
    <p>Write the workflow as code, or draw it as BPMN — the same durable engine runs both. Switch
    surfaces without re-platforming; the model and the code are two views of one runtime.</p>
    <p class="proof">Same engine. Same app. Your choice of surface.</p>
  </article>
  <article>
    <h3>Durable by default</h3>
    <p>Survives crashes and forced reboots. On restart a workflow resumes at the exact step it left
    off — completed steps aren't replayed. Delivery is at-least-once: make your activities idempotent
    (e.g. with idempotency keys) and they run effectively once.</p>
    <p class="proof">SIGKILL → cold restart → completed steps are never replayed. With a negative control.</p>
  </article>
</section>

<section class="band providers">
  <div class="wrap">
    <h2>Provider-agnostic. <span class="grad">Mix frontier with local.</span></h2>
    <p>Blend frontier models with local LLMs — on one workstation or across many. Route the expensive
    steps to a frontier model and keep the rest private and free. Bring the coding harness you already use:</p>
    <div class="provs" aria-label="Supported agent harnesses">
${agentHarnesses().map((h) => `      <span>${esc(h)}</span>`).join("\n")}
      <span class="more">and more</span>
    </div>
  </div>
</section>

<section class="band polyglot">
  <div class="wrap">
    <h2>Polyglot by design. <span class="grad">Meet your stack.</span></h2>
    <p>Because Nano speaks the Camunda 8 REST API, it inherits Camunda's polyglot SDK surface —
    drive workflows and workers from the language your team already ships in, with first-class
    Camunda 8 SDKs across the stack — plus the wider ecosystem of tooling built on that API, which
    works against Nano unchanged:</p>
    <div class="provs langs" aria-label="Supported SDK languages">
${langChips()}
    </div>
  </div>
</section>

<section class="how wrap">
  <h2>Add an agent + coding harness to your workforce.</h2>
  <p class="how-sub">Turn any interactive CLI agent harness into a durable Nano job worker.
  <b>Hire</b> persists an agent profile; <b>work</b> polls the cluster and runs jobs through it.</p>
  <div class="install multi" aria-label="Hire and run an agent worker">
    <code>c8ctl nano hire copilot</code>
    <code>c8ctl nano work copilot</code>
  </div>
  <ol class="steps">
    <li><span>1</span><div><b>Hire.</b> Persist an agent profile — a name, a rank, and the CLI
      command (e.g. <code>copilot</code>) that backs it.</div></li>
    <li><span>2</span><div><b>Work.</b> The harness polls for jobs and runs them, so a coding agent
      becomes a first-class, durable worker in your orchestration.</div></li>
    <li><span>3</span><div><b>It just resumes.</b> Every run is an ordinary durable Nano instance —
      crash-resume is implicit, and you never think about the journal.</div></li>
  </ol>
</section>

${compareHtml()}

<section class="band problem" id="nano-coder">
  <div class="wrap">
    <p class="eyebrow"><code>@nanobpm/nano-coder</code></p>
    <h2>Your fleet's workers, at ${esc(nanoCoderFacts().rss)} each.</h2>
    <p>nano-coder is a Rust coding agent built to be the worker your graphs fan out. Node-based agent
    CLIs take ${esc(nanoCoderFacts().nodeRss)} apiece; nano-coder takes about ${esc(nanoCoderFacts().rss)}, so a
    laptop runs a fleet instead of a handful. Interactive in your terminal, or headless over ACP
    under Nano Workforce.</p>
    ${installCommand("Install nano-coder", nanoCoderFacts().install)}
    <p><a href="/nano-coder/">Meet nano-coder →</a> · <a href="/blog/a-6mb-coding-agent/">Why the worker's size matters →</a></p>
  </div>
</section>

<section class="band backbone">
  <div class="wrap">
    <h2>RAAD.<br><span class="grad">Rapid Agent Application Development.</span></h2>
    <p class="raad-sub">An Advanced Research Prototype for agent graph orchestration on the developer
    workstation. Start in one line.</p>
    ${installCommand("Get started")}
    <p class="install-note center">Prefer to do it by hand?
    <a href="/docs/get-started-with-c8ctl">Manual setup →</a></p>
    <div class="cta">
      <a class="btn primary" href="/demo/">Try it in your browser →</a>
    </div>
  </div>
</section>

<footer class="site-foot wrap">
  <p><a href="/architecture/">Architecture</a> · <a href="/demo/">Browser demo</a>${docsPresent ? ' · <a href="/docs/">Documentation</a>' : ""} · <a href="/schemas/">Published schemas</a></p>
  <p class="muted">Nano is an Advanced Research Prototype. Free for personal or evaluation use.</p>
</footer>

<script>
// Accessible tabs for the Code-first / Model-first panel. Lazy-loads the demo
// iframe (data-src -> src) only when the Model-first tab is first selected, so
// the home page never eagerly loads /demo/ (keeps first paint light).
(function () {
  var tabs = Array.prototype.slice.call(document.querySelectorAll(".demo-tabs .tab"));
  if (!tabs.length) return;
  function select(tab) {
    tabs.forEach(function (t) {
      var on = t === tab;
      t.setAttribute("aria-selected", on ? "true" : "false");
      t.tabIndex = on ? 0 : -1;
      var panel = document.getElementById(t.getAttribute("aria-controls"));
      if (panel) panel.hidden = !on;
    });
    var panel = document.getElementById(tab.getAttribute("aria-controls"));
    if (panel) {
      var frame = panel.querySelector("iframe[data-src]");
      if (frame) {
        frame.setAttribute("src", frame.getAttribute("data-src"));
        frame.removeAttribute("data-src");
      }
    }
  }
  tabs.forEach(function (tab, i) {
    tab.addEventListener("click", function () { select(tab); });
    tab.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
        e.preventDefault();
        var n = e.key === "ArrowRight" ? (i + 1) % tabs.length : (i - 1 + tabs.length) % tabs.length;
        tabs[n].focus(); select(tabs[n]);
      }
    });
  });
})();
</script>
${installCopyScript()}`;

  return homePage("nanobpm.io — Agent Graph Orchestration for the Developer Workstation", body);
}

// A landscape comparison grid, rendered from a single source of truth so the
// header set and every row stay aligned. Nano Workforce is the first product
// column and is visually highlighted (`col-nano`) — the column each peer is
// read against. Cells carry an optional tone (`yes` / `partial` / `no`) that decorates
// them with a ✓ / ~ / — marker; plain cells are descriptive. Every claim traces
// to the tool's own docs (onorca.dev, herdr.dev, code.claude.com/docs) and is
// kept deliberately fair — these tools solve overlapping-but-distinct problems.
function compareHtml() {
  // Product columns, in render order. Nano first (highlighted), then the peers.
  const cols = [
    { key: "nano", label: "Nano Workforce", brand: true },
    { key: "orca", label: "Orca" },
    { key: "herdr", label: "Herdr" },
    { key: "claude", label: "Claude Dynamic Workflows" },
  ];

  // Rows, in render order. Each cell is `[value]` (plain, descriptive) or
  // `[value, tone]` where tone ∈ {yes, partial, no}. Order matches `cols`.
  const rows = [
    {
      label: "What it is",
      cells: [
        ["Durable agent-workflow engine"],
        ["Desktop orchestrator IDE"],
        ["Terminal runtime for agents"],
        ["In-Claude subagent scripting"],
      ],
    },
    {
      label: "Orchestration lives in",
      cells: [
        ["The durable graph"],
        ["You, in the GUI"],
        ["You / agents via socket"],
        ["The JavaScript the model writes"],
      ],
    },
    {
      label: "Authoring surface",
      cells: [
        ["Code-first & Model-first (BPMN)"],
        ["Manual, worktree GUI"],
        ["Interactive + socket API"],
        ["A script Claude writes"],
      ],
    },
    {
      label: "Survives crash / reboot",
      cells: [
        ["Journal-backed step resume", "yes"],
        ["Scrollback only", "partial"],
        ["Terminals reattach", "partial"],
        ["Resumes in the session", "partial"],
      ],
    },
    {
      label: "Human approval steps",
      cells: [
        ["First-class durable waits", "yes"],
        ["", "no"],
        ["Flags a blocked agent", "partial"],
        ["", "no"],
      ],
    },
    {
      label: "Agent harnesses",
      cells: [
        ["Provider-agnostic"],
        ["Codex, Claude, OpenCode, Pi"],
        ["Claude, Codex, Cursor, Grok…"],
        ["Claude only"],
      ],
    },
    {
      label: "Frontier + local models",
      cells: [
        ["Mix both on one box", "yes"],
        ["Via the harness", "partial"],
        ["Via the harness", "partial"],
        ["Anthropic only", "no"],
      ],
    },
    {
      label: "Headless / server / CI",
      cells: [
        ["Engine + Camunda 8 REST", "yes"],
        ["Desktop app", "no"],
        ["Background server", "partial"],
        ["Inside Claude Code", "partial"],
      ],
    },
    {
      label: "Distributed fleet",
      cells: [
        ["Any mix of hardware, local to fleet", "yes"],
        ["One desktop (+ SSH box)", "partial"],
        ["One host, reattach over SSH", "partial"],
        ["Single machine", "no"],
      ],
    },
    {
      label: "Footprint",
      cells: [
        ["Small — runs on a Raspberry Pi"],
        ["Electron desktop"],
        ["One Rust binary"],
        ["Claude Code"],
      ],
    },
    {
      label: "License",
      cells: [
        ["Free to evaluate & personal use"],
        ["MIT"],
        ["Apache-2.0"],
        ["Proprietary"],
      ],
    },
  ];

  // Tone vocabulary — the only values a cell's optional second element may take.
  // Both the shape check below and the accessible labels are derived from it, so
  // there is a single source of truth for what a valid tone is.
  const TONE_LABELS = { yes: "Yes", partial: "Partial", no: "No" };

  // Fail fast with a clear message if the data drifts out of shape. Without this,
  // a mismatched row/column count surfaces later as an opaque
  // `Cannot read properties of undefined` while rendering.
  rows.forEach((r, ri) => {
    if (r.cells.length !== cols.length) {
      throw new Error(
        `compareHtml: row ${ri} ("${r.label}") has ${r.cells.length} cell(s) but there are ${cols.length} column(s)`,
      );
    }
    r.cells.forEach((cell, ci) => {
      if (!Array.isArray(cell) || cell.length < 1 || cell.length > 2) {
        throw new Error(
          `compareHtml: row ${ri} ("${r.label}"), column ${ci} must be a [value] or [value, tone] tuple`,
        );
      }
      const [value, tone] = cell;
      if (typeof value !== "string") {
        throw new Error(
          `compareHtml: row ${ri} ("${r.label}"), column ${ci} value must be a string`,
        );
      }
      if (tone !== undefined && !Object.hasOwn(TONE_LABELS, tone)) {
        throw new Error(
          `compareHtml: row ${ri} ("${r.label}"), column ${ci} has invalid tone "${tone}" (expected one of ${Object.keys(TONE_LABELS).join(", ")})`,
        );
      }
    });
  });

  const colClass = (c) => (c?.key === "nano" ? " col-nano" : "");
  const head =
    `      <th scope="col"><span class="visually-hidden">Capability</span></th>\n` +
    cols
      .map(
        (c) =>
          `      <th scope="col" class="${c.brand ? "brandcol" : ""}${colClass(c)}">${esc(c.label)}</th>`,
      )
      .join("\n");

  const body = rows
    .map((r) => {
      const tds = r.cells
        .map((cell, i) => {
          const [value, tone] = cell;
          const toneCls = tone ? ` ${tone}` : "";
          const text = value === "" ? "" : esc(value);
          // The ✓/~/— glyphs are CSS `::before` content, which assistive tech
          // often does not announce — prepend visually-hidden text so the
          // yes/partial/no signal reaches screen readers without overriding the
          // cell's visible descriptive text (as an aria-label would). A trailing
          // ": " (only when the cell has visible text) makes it read as
          // "Yes: …" with a reliable pause, instead of "YesJournal-backed…".
          const toneLabel = tone
            ? `<span class="visually-hidden">${TONE_LABELS[tone]}${text ? ": " : ""}</span>`
            : "";
          return `      <td class="${colClass(cols[i]).trim()}"><span class="cell${toneCls}">${toneLabel}${text}</span></td>`;
        })
        .join("\n");
      return `    <tr>\n      <th scope="row" class="rowlabel">${esc(r.label)}</th>\n${tds}\n    </tr>`;
    })
    .join("\n");

  return `<section class="band compare wrap" id="compare">
  <h2>How Nano compares.</h2>
  <p class="compare-sub">The agent-tooling landscape spans desktop IDEs, terminal runtimes, and in-model
  scripting. Nano Workforce is the durable engine underneath — the one that keeps running when the
  machine doesn't.</p>
  <div class="compare-scroll" role="region" aria-label="How Nano compares, horizontally scrollable table" tabindex="0">
    <table class="compare-table">
      <thead>
        <tr>
${head}
        </tr>
      </thead>
      <tbody>
${body}
      </tbody>
    </table>
  </div>
  <p class="compare-foot">Overlapping but distinct: Orca and Herdr host the interactive terminals your
  agents run in, and Nano can drive those same harnesses as durable workers. This grid maps each tool to
  the job it leads on. Sourced from each project's own docs; corrections welcome.</p>
</section>`;
}

// --- Landscape positioning table (single source of truth) --------------------
// The "Where Nano sits in the landscape" table appears on TWO surfaces: the
// public /architecture#landscape page (below) and the in-app console /stack page
// (server/crates/nano-server-console/src/stack.html, served by the gateway binary). To keep them
// from drifting, both are rendered from ONE data source — website/data/landscape.json
// — by the single renderer here. The console copy is emitted as a checked-in
// derived artifact (server/crates/nano-server-console/src/landscape.gen.html) that the server injects
// into stack.html at serve time; the `schemas` CI job runs this build and a
// `git diff --exit-code` on that artifact, so a stale copy fails the build.
// A hoisted function (not a module-level `const`) so it is callable from the
// top-level `write(architectureHtml())` above without a temporal-dead-zone error.
function landscapeTones() {
  return { yes: "yes", partial: "partial", no: "no" };
}

function loadLandscapeData() {
  const raw = readFileSync(join(here, "data", "landscape.json"), "utf8");
  const data = JSON.parse(raw);
  const tones = landscapeTones();
  const { columns, rows } = data;
  if (!Array.isArray(columns) || columns.length < 2) {
    throw new Error("landscape.json: `columns` must be an array of >= 2 columns");
  }
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error("landscape.json: `rows` must be a non-empty array");
  }
  // The first column is the row-header ("Capability"); the rest are compared
  // products. Each row therefore carries one cell per product column.
  const productCount = columns.length - 1;
  rows.forEach((r, ri) => {
    if (typeof r.feature !== "string") {
      throw new Error(`landscape.json: row ${ri} is missing a string \`feature\` label`);
    }
    if (!Array.isArray(r.cells) || r.cells.length === 0) {
      throw new Error(`landscape.json: row ${ri} ("${r.feature}") must have a non-empty \`cells\` array`);
    }
    // Cells may span multiple product columns; the spans must sum to exactly the
    // number of product columns, so no row is under- or over-full (the failure
    // mode this whole single-source design exists to prevent).
    let span = 0;
    r.cells.forEach((cell, ci) => {
      if (typeof cell.text !== "string") {
        throw new Error(`landscape.json: row ${ri} ("${r.feature}"), cell ${ci} needs a string \`text\``);
      }
      if (cell.tone !== undefined && !Object.hasOwn(tones, cell.tone)) {
        throw new Error(
          `landscape.json: row ${ri} ("${r.feature}"), cell ${ci} has invalid tone "${cell.tone}" (expected one of ${Object.keys(tones).join(", ")})`,
        );
      }
      const cs = cell.span ?? 1;
      if (!Number.isInteger(cs) || cs < 1) {
        throw new Error(`landscape.json: row ${ri} ("${r.feature}"), cell ${ci} has an invalid \`span\` (must be a positive integer)`);
      }
      span += cs;
    });
    if (span !== productCount) {
      throw new Error(
        `landscape.json: row ${ri} ("${r.feature}") cells span ${span} column(s) but there are ${productCount} product column(s)`,
      );
    }
  });
  return data;
}

// Render the shared landscape table to HTML. Kept purely data-driven (matching
// the header/body treatment of the /architecture CSS: `table.landscape`,
// `td.feature`, `td.nano`, tone spans) so both surfaces get byte-identical markup.
function landscapeTableHtml() {
  const { columns, rows } = loadLandscapeData();
  const tones = landscapeTones();
  const head = columns
    .map((c) => {
      const cls = c.nano ? ' class="nano"' : "";
      const sub = c.sub ? `<small>${esc(c.sub)}</small>` : "";
      return `          <th scope="col"${cls}>${esc(c.label)}${sub}</th>`;
    })
    .join("\n");
  const body = rows
    .map((r) => {
      const cells = r.cells
        .map((cell) => {
          const attrs =
            (cell.nano ? ' class="nano"' : "") +
            (cell.span && cell.span > 1 ? ` colspan="${cell.span}"` : "");
          const inner = cell.tone
            ? `<span class="${tones[cell.tone]}">${esc(cell.text)}</span>`
            : esc(cell.text);
          return `          <td${attrs}>${inner}</td>`;
        })
        .join("\n");
      return `        <tr>\n          <th scope="row" class="feature">${esc(r.feature)}</th>\n${cells}\n        </tr>`;
    })
    .join("\n");
  return `<table class="landscape">
      <thead>
        <tr>
${head}
        </tr>
      </thead>
      <tbody>
${body}
      </tbody>
    </table>`;
}

// The shared site header/nav. Emitted verbatim on every full-page (homePage
// shell) surface so the brand + link set can't drift between the landing page
// and the architecture page. `docsPresent` is a module-level const (computed
// up-front), so /docs/ is advertised here on exactly the builds that ship it.
function siteNav() {
  // Inline `currentColor` brand marks for the community links, so they inherit
  // the nav link colour + hover state. Declared inside the function: the build
  // calls siteNav() at module load, before a module-level const would leave the
  // temporal dead zone.
  const ICON_GITHUB =
    '<svg class="icon" viewBox="0 0 16 16" width="15" height="15" fill="currentColor" aria-hidden="true" focusable="false"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82a7.6 7.6 0 012-.27c.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>';
  const ICON_DISCORD =
    '<svg class="icon" viewBox="0 0 24 24" width="16" height="16" fill="currentColor" aria-hidden="true" focusable="false"><path d="M20.317 4.369a19.79 19.79 0 00-4.885-1.515.074.074 0 00-.079.037c-.211.375-.444.865-.608 1.25a18.27 18.27 0 00-5.487 0 12.64 12.64 0 00-.617-1.25.077.077 0 00-.079-.037A19.736 19.736 0 003.677 4.37a.07.07 0 00-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 00.031.057 19.9 19.9 0 005.993 3.03.078.078 0 00.084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 00-.041-.106 13.1 13.1 0 01-1.872-.892.077.077 0 01-.008-.128c.126-.094.252-.192.372-.291a.074.074 0 01.077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 01.078.009c.12.099.246.198.373.292a.077.077 0 01-.006.127 12.3 12.3 0 01-1.873.891.077.077 0 00-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 00.084.028 19.839 19.839 0 006.002-3.03.077.077 0 00.032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 00-.031-.03zM8.02 15.331c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.418 2.157-2.418 1.211 0 2.176 1.094 2.157 2.418 0 1.334-.955 2.419-2.157 2.419zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.418 2.157-2.418 1.211 0 2.176 1.094 2.157 2.418 0 1.334-.946 2.419-2.157 2.419z"></path></svg>';
  return `<header class="nav">
  <a class="brand" href="/">nanobpm<span class="dim">.io</span></a>
  <nav>
    <a href="/architecture/">Architecture</a>
    <a href="/architecture/#landscape">Compare</a>
    <a href="/nano-coder/">nano-coder</a>
    <a href="/blog/">Blog</a>
    <a href="/demo/">Demo</a>
    ${whitepaperPresent ? '<a href="/whitepaper/">Whitepaper</a>' : ""}
    ${docsPresent ? '<a href="/docs/">Docs</a>' : ""}
    <a href="/schemas/">Schemas</a>
    <a class="ext" href="https://github.com/nanobpm" rel="noopener noreferrer" target="_blank" aria-label="Nano BPM on GitHub">${ICON_GITHUB}<span>GitHub</span></a>
    <a class="ext" href="https://discord.gg/W5dBe2D8y" rel="noopener noreferrer" target="_blank" aria-label="Nano BPM on Discord">${ICON_DISCORD}<span>Discord</span></a>
  </nav>
</header>`;
}

// The stack, top (application) to bottom (engine foundation). Single source of
// truth for the architecture page: the diagram and the layer detail are all
// derived from this array, so they can never disagree. Declared inside the
// function so it stays in scope at call time (module-level `const` would be in
// the temporal dead zone when the emit runs above).
function architectureHtml() {
  const ARCH_LAYERS = [
    {
      name: "Nano Workforce",
      role: "Application",
      tagline: "Agent-powered SDLC Orchestration",
      desc:
        "The application at the top of the stack. Hire agents and coding harnesses, " +
        "then orchestrate the whole software-development lifecycle — plan, implement, " +
        "review, test, merge, QA, and run retrospectives — as durable graphs that resume " +
        "across crashes and reboots.",
      tags: ["Agent orchestration", "SDLC", "Durable runs"],
      rung: "hires workers like",
    },
    {
      name: "Agent workers",
      role: "Agent worker · bring your own",
      tagline: "nano-coder, or the harness you already use",
      desc:
        "The coding agents Workforce dispatches work to. nano-coder is optional: Workforce " +
        "hires any supported harness — " +
        agentHarnesses().filter((h) => h !== "nano-coder").join(", ") +
        " and more — and you can mix them in one fleet. nano-coder is the purpose-built option: " +
        `a Rust agent at ${nanoCoderFacts().rss} resident memory, so a laptop can host a dense fleet, ` +
        "running interactively or headless over ACP.",
      descLink: { text: "nano-coder", href: "/nano-coder/" },
      tags: agentHarnesses(),
      rung: "driven by graphs built on",
    },
    {
      name: "Urban",
      role: "Application framework",
      tagline: "The Nano application framework",
      desc:
        "Author Nano apps as code or model. Go code-first and Urban derives the executable " +
        "model, job types, message correlation, and a generic worker from your code; go " +
        "model-first and the authored BPMN model is the source of truth. Either way, one " +
        "source of truth — no hand-wired orchestration.",
      tags: ["TypeScript-only", "More languages coming", "Code-first or Model-first"],
    },
    {
      name: "Nano Studio",
      role: "IDE",
      tagline: "Rapid Agent Application Development IDE",
      desc:
        "The RAAD environment — a polyglot IDE to scaffold, run, and inspect agent " +
        "applications on your workstation. Model-and-run, live traces, and the web console " +
        "in one place.",
      tags: ["Polyglot", "RAAD", "On-workstation"],
    },
    {
      name: "Nano",
      role: "Engine · foundation",
      tagline: "Durable Agent Graph engine",
      desc:
        "The load-bearing runtime, written in Rust and compatible with the Camunda 8 API. " +
        "Durable by default: on restart a graph resumes at the exact step it left off — " +
        "completed (journal-committed) steps are never replayed. Delivery is at-least-once, " +
        "so activities must be idempotent. Small enough to start on a Raspberry Pi.",
      tags: ["Rust", "Camunda 8 API compatible", "At-least-once · idempotent recovery"],
      foundation: true,
    },
  ];

  const layers = ARCH_LAYERS.map((l, i) => {
    const tags = l.tags
      .map((t) => `<span>${esc(t)}</span>`)
      .join("");
    const connector =
      i < ARCH_LAYERS.length - 1
        ? `\n  <div class="rung" aria-hidden="true"><span>${esc(l.rung ?? "runs on")}</span></div>`
        : "";
    return `<article class="layer${l.foundation ? " foundation" : ""}">
    <div class="layer-head">
      <span class="depth" aria-hidden="true">${i + 1}</span>
      <div>
        <p class="role">${esc(l.role)}</p>
        <h3>${l.href ? `<a href="${l.href}">${esc(l.name)}</a>` : esc(l.name)} <span class="tagline">— ${esc(l.tagline)}</span></h3>
      </div>
    </div>
    <p>${l.descLink ? esc(l.desc).replace(esc(l.descLink.text), `<a href="${l.descLink.href}">${esc(l.descLink.text)}</a>`) : esc(l.desc)}</p>
    <div class="tags">${tags}</div>
  </article>${connector}`;
  }).join("\n");

  const body = `${siteNav()}

<style>
  .arch-hero { max-width: var(--wrap); margin-inline: auto; padding: 3.6rem 1.4rem 1rem; text-align: center; }
  .arch-hero h1 { font-size: clamp(2rem, 5vw, 3.1rem); margin: .4rem 0 .6rem; font-weight: 700; }
  .arch-hero .lede { font-size: clamp(1.02rem, 2.2vw, 1.2rem); color: var(--muted); max-width: 42rem; margin: 0 auto; }

  .stack { display: flex; flex-direction: column; gap: 0; max-width: 52rem; margin: 2.6rem auto 0; }
  .layer {
    border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel);
    backdrop-filter: blur(6px); padding: 1.3rem 1.5rem 1.3rem 1.7rem; position: relative; overflow: hidden;
  }
  .layer::before {
    content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 3px;
    background: linear-gradient(180deg, var(--emerald), var(--sky));
  }
  .layer.foundation { box-shadow: 0 0 0 1px rgba(52,211,153,.25), 0 24px 60px -30px rgba(56,189,248,.5); }
  .layer-head { display: flex; align-items: flex-start; gap: .9rem; }
  .layer .depth {
    flex: none; width: 1.9rem; height: 1.9rem; border-radius: 8px; background: rgba(255,255,255,.05);
    border: 1px solid var(--line); color: var(--sky); font-weight: 700;
    display: grid; place-items: center; font-size: .95rem;
  }
  .layer .role { text-transform: uppercase; letter-spacing: .14em; font-size: .72rem; font-weight: 700; color: var(--sky); margin: .15rem 0 .1rem; }
  .layer h3 { font-size: 1.25rem; margin: 0; }
  .layer h3 .tagline { color: var(--muted); font-weight: 500; font-size: .95rem; }
  .layer > p { margin: .7rem 0 0; color: var(--muted); font-size: .98rem; }
  .layer .tags { margin-top: .85rem; display: flex; gap: .5rem; flex-wrap: wrap; }
  .layer .tags span {
    border: 1px solid var(--line); background: rgba(255,255,255,.04); padding: .28rem .7rem;
    border-radius: 999px; font-size: .82rem; color: var(--ink); font-weight: 500;
  }
  .rung { display: grid; place-items: center; height: 2.1rem; position: relative; }
  .rung::before { content: ""; width: 1px; height: 100%; background: linear-gradient(180deg, var(--sky), transparent); position: absolute; }
  .rung span {
    position: relative; font-size: .72rem; text-transform: uppercase; letter-spacing: .14em;
    color: var(--muted); background: var(--bg); padding: 0 .5rem;
  }

  .arch-foot { max-width: 52rem; margin: 2.8rem auto 0; text-align: center; }
  .arch-foot .cta { display: flex; gap: .8rem; justify-content: center; flex-wrap: wrap; margin-top: 1.4rem; }

  .arch-section { max-width: 52rem; margin: 3.6rem auto 0; }
  .arch-section > h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); text-align: center; margin: 0 0 .5rem; }
  .arch-section > .lede { text-align: center; margin: 0 auto 1.6rem; }
  .table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel); backdrop-filter: blur(6px); }
  table.landscape { width: 100%; border-collapse: collapse; font-size: .92rem; }
  table.landscape th, table.landscape td { text-align: left; padding: .7rem .9rem; border-bottom: 1px solid var(--line); vertical-align: top; }
  table.landscape thead th { color: var(--muted); font-weight: 700; font-size: .82rem; }
  table.landscape thead th small { display: block; font-weight: 400; color: var(--muted); opacity: .8; font-size: .82em; margin-top: .2rem; }
  table.landscape thead th.nano { color: var(--emerald); }
  table.landscape tbody tr:last-child td, table.landscape tbody tr:last-child th { border-bottom: none; }
  table.landscape .feature { color: var(--muted); white-space: nowrap; font-weight: inherit; }
  table.landscape td.nano { color: var(--ink); background: rgba(52,211,153,.06); }
  table.landscape .yes { color: var(--emerald); font-weight: 600; }
  table.landscape .no { color: #f87171; }
  table.landscape .partial { color: #fbbf24; }
  .arch-note { font-size: .84rem; color: var(--muted); opacity: .85; text-align: center; margin-top: .9rem; }
  blockquote.crib { margin: 1.2rem 0; padding: .5rem 0 .5rem 1.1rem; border-left: 3px solid rgba(52,211,153,.5); color: var(--ink); font-style: italic; }
  blockquote.crib .src { display: block; font-style: normal; color: var(--muted); font-size: .88rem; margin-top: .4rem; }
</style>

<section class="arch-hero">
  <p class="eyebrow"><span class="arp">Architecture</span></p>
  <h1>One stack, <span class="grad">${["zero", "one", "two", "three", "four", "five", "six", "seven"][ARCH_LAYERS.length]} layers.</span></h1>
  <p class="lede">From the agent-powered application at the top to the durable Rust engine at
  the foundation — each layer builds on the one below it.</p>
</section>

<section class="wrap">
  <div class="stack">
${layers}
  </div>
</section>

<section class="arch-section wrap" id="landscape">
  <h2>Where it sits in the landscape</h2>
  <p class="lede">Nano Workforce isn&rsquo;t a faster coding agent &mdash; it&rsquo;s a
  different layer. Here&rsquo;s where it sits relative to the tools you already know.</p>
  <div class="table-wrap">
    ${landscapeTableHtml()}
  </div>
  <p class="arch-note">Categories blur and vendors move fast &mdash; this is an indicative
  positioning map, not a scorecard. The point is the axis: single-agent tools operate one
  session at a time; Nano Workforce coordinates many of them durably. For a head-to-head against
  named orchestrators, see the <a href="/#compare">comparison on the home page</a>.</p>
</section>

<section class="arch-section wrap" id="complementary">
  <h2>Complementary, not competing</h2>
  <p class="lede">OpenCode, Claude Code and the Copilot CLI are exactly the kind of worker
  Nano Workforce hires. Adopting Nano doesn&rsquo;t replace your coding agent &mdash; it puts
  a durable orchestrator around it.</p>
  <blockquote class="crib">
    &ldquo;The agents are external workers you <em>hire</em> (any coding-agent CLI harness,
    e.g. the GitHub Copilot CLI). Nano Workforce owns the orchestration; the agents do the work.&rdquo;
    <span class="src">&mdash; Nano Workforce README</span>
  </blockquote>
  <p class="lede" style="margin-top:1.2rem">So the question isn&rsquo;t &ldquo;Nano <em>or</em>
  OpenCode?&rdquo; &mdash; it&rsquo;s &ldquo;which agent do I want Nano to drive?&rdquo; Swap the
  harness without rewriting the workflow; the plan / review / merge graph stays the same.</p>
</section>

<section class="arch-section wrap" id="effect">
  <h2>Effect for the process; Nano for the workflow</h2>
  <p class="lede">People ask how Nano relates to <a href="https://effect.website" rel="noopener noreferrer" target="_blank">Effect</a>,
  TypeScript&rsquo;s structured-concurrency and typed-error runtime. They live on
  different axes &mdash; and they compose.</p>
  <p class="lede" style="margin-top:1rem">Effect is the best way to make one
  <em>process</em> robust: fibers, typed errors, and resource safety <em>inside</em>
  a running program. Nano makes the <em>workflow</em> robust: the same plan &rarr;
  review &rarr; merge graph survives the process dying, the machine rebooting, and
  a redeploy in the middle &mdash; then resumes at the exact step it left off.
  In-memory structured concurrency ends when the memory does; a durable graph
  doesn&rsquo;t.</p>
  <div class="table-wrap">
    <table class="landscape">
      <thead><tr>
        <th>Concern</th>
        <th>Effect <small>in-process</small></th>
        <th class="nano">Nano <small>durable</small></th>
      </tr></thead>
      <tbody>
        <tr><th class="feature">Unit of execution</th><td>Fiber (in one process)</td><td class="nano">Journalled graph step</td></tr>
        <tr><th class="feature">Survives crash / reboot / redeploy</th><td class="no">No &mdash; state is in memory</td><td class="nano"><span class="yes">Yes</span> &mdash; resumes at the last committed step</td></tr>
        <tr><th class="feature">Retries &amp; timeouts</th><td>Per run, in memory</td><td class="nano">Durable, at-least-once with idempotent recovery</td></tr>
        <tr><th class="feature">Concurrency model</th><td class="yes">Fibers, first-class</td><td class="nano">Parallel &amp; multi-instance branches; correlation</td></tr>
        <tr><th class="feature">Error model</th><td class="yes">Typed error channel</td><td class="nano">Incidents &amp; boundary events on the graph</td></tr>
        <tr><th class="feature">Time horizon</th><td>Milliseconds &ndash; minutes</td><td class="nano">Seconds &ndash; weeks (timers, human tasks)</td></tr>
        <tr><th class="feature">Where it runs</th><td>Any TS/JS runtime</td><td class="nano">Rust engine, Camunda&nbsp;8 API compatible</td></tr>
      </tbody>
    </table>
  </div>
  <p class="lede" style="margin-top:1.4rem">So it isn&rsquo;t &ldquo;Nano <em>or</em>
  Effect?&rdquo; If you love the Effect model, keep it <em>inside</em> your workers
  &mdash; and let Nano carry the run across everything that outlives the process.
  We love that model enough that Urban&rsquo;s own glue code uses a tiny,
  zero-dependency distillation of it:</p>
  <blockquote class="crib">
    &ldquo;effectlite &mdash; a tiny, zero-dependency, Effect-<em>like</em> core for
    Urban glue code &hellip; the three Effect ergonomics we actually reach for:
    typed-error <code>Result</code> with generator do-notation, tagged errors with
    exhaustive matching, and <code>scoped</code> resource release &mdash; in ~100
    lines with no runtime dependencies.&rdquo;
    <span class="src">&mdash; <code>@nanobpm/urban</code>, <code>src/effect</code></span>
  </blockquote>
</section>

<section class="arch-foot wrap">
  <p class="muted">Nano is the load-bearing runtime; every layer above is optional and composes on top of it.</p>
  <div class="cta">
    <a class="btn primary" href="/demo/">Try it in your browser →</a>
    ${docsPresent ? '<a class="btn ghost" href="/docs/">Read the docs →</a>' : '<a class="btn ghost" href="/schemas/">Published schemas →</a>'}
  </div>
</section>

<footer class="site-foot wrap">
  <p><a href="/">Home</a> · <a href="/demo/">Browser demo</a>${docsPresent ? ' · <a href="/docs/">Documentation</a>' : ""} · <a href="/schemas/">Published schemas</a></p>
  <p class="muted">Nano is an Advanced Research Prototype. Free for personal or evaluation use.</p>
</footer>`;

  return homePage("nanobpm.io — Architecture", body);
}

// Minimal, dependency-free TS/JS highlighter for the fixed hero snippet. Ordered
// alternation, left-to-right; every emitted token is HTML-escaped via tok()/esc().
// Kept in-repo so `node website/build.mjs` stays zero-dependency (CI schemas job).
function highlightTs(src) {
  const re =
    /(\/\/[^\n]*)|("(?:[^"\\]|\\.)*")|\b(import|from|export|const|let|var|async|await|return|new|of|in|function)\b|(=>)|\b(\d+)\b|\.([A-Za-z_$][\w$]*)(?=\s*\()|([A-Za-z_$][\w$]*)(?=\s*\()|\.([A-Za-z_$][\w$]*)|([A-Za-z_$][\w$]*)|([{}()[\];,:.])/g;
  const dot = '<span class="tok-punc">.</span>';
  let out = "";
  let last = 0;
  for (let m = re.exec(src); m !== null; m = re.exec(src)) {
    out += esc(src.slice(last, m.index));
    last = re.lastIndex;
    if (m[1] !== undefined) out += tok("com", m[1]);
    else if (m[2] !== undefined) out += tok("str", m[2]);
    else if (m[3] !== undefined) out += tok("kw", m[3]);
    else if (m[4] !== undefined) out += tok("arw", m[4]);
    else if (m[5] !== undefined) out += tok("num", m[5]);
    else if (m[6] !== undefined) out += dot + tok("fn", m[6]);
    else if (m[7] !== undefined) out += tok("fn", m[7]);
    else if (m[8] !== undefined) out += dot + tok("prop", m[8]);
    else if (m[9] !== undefined) out += tok("id", m[9]);
    else if (m[10] !== undefined) out += tok("punc", m[10]);
  }
  out += esc(src.slice(last));
  return out;
}

function tok(cls, text) {
  return `<span class="tok-${cls}">${esc(text)}</span>`;
}

// --- Blog ---------------------------------------------------------------------
// Single source of truth for the blog: the index cards and each post page are
// both derived from BLOG_POSTS(), so they can never disagree. A post's `body` is
// a function returning the article HTML (rendered inside the branded homePage
// shell, so it inherits siteNav, the token colours, and figure.code styling —
// no per-post CSS duplication). Newest first.
function blogPosts() {
  return [
    {
      slug: "a-6mb-coding-agent",
      title: "A 6MB coding agent, and why fleets need one",
      subtitle: "Introducing @nanobpm/nano-coder — the worker at the bottom of the Nano stack",
      date: "2026-10-21",
      dek:
        "When a graph fans out ten coding agents, the agent's footprint becomes the fleet's " +
        "ceiling. nano-coder is a Rust coding agent that runs in about 6MB, so the ceiling moves.",
      body: nanoCoderPostBody,
    },
    {
      slug: "the-bottleneck-moved-from-code-to-coordination",
      title: "The bottleneck moved from code to coordination",
      subtitle:
        "When agents can write the code, most of a ten-agent workforce is spent waiting — and waiting is a process-engineering problem",
      date: "2026-08-19",
      dek:
        "Reliable agents, codebases architected for safety, and real CI made writing code " +
        "cheap. The friction didn’t vanish — it moved to coordination: dependency graphs, " +
        "review cycles, wait states. That’s process engineering.",
      body: coordinationPostBody,
    },
    {
      slug: "effect-for-the-process-nano-for-the-workflow",
      title: "We love Effect so much we shipped 100 lines of it",
      subtitle:
        "Why Urban distills Effect instead of depending on it — and where Nano picks up where Effect leaves off",
      date: "2026-08-19",
      dek:
        "Effect v4 is a great way to make a process robust. Nano makes the workflow " +
        "robust. Here’s why Urban ships a ~100-line distillation of Effect instead of " +
        "the dependency — and how the two compose.",
      body: effectlitePostBody,
    },
  ];
}

function fmtDate(iso) {
  const d = new Date(`${iso}T00:00:00Z`);
  return d.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric", timeZone: "UTC" });
}

// Shared article typography, scoped to `.post` / the blog index. A function (not a
// module-level const) so it is hoisted past the temporal dead zone: the emit at the
// top of the file calls blogIndexHtml() before a const would initialize.
function blogStyle() {
  return `<style>
  .post, .blog-index { max-width: 44rem; margin-inline: auto; padding: 2.6rem 1.4rem 1rem; }
  .post-head { margin-bottom: 2.2rem; }
  .post-head .eyebrow, .blog-index .eyebrow { text-transform: uppercase; letter-spacing: .16em; font-size: .74rem; font-weight: 700; color: var(--sky); margin: 0 0 .8rem; }
  .post-head h1 { font-size: clamp(1.9rem, 4.6vw, 2.7rem); margin: 0 0 .7rem; font-weight: 700; }
  .post-head .sub { font-size: clamp(1.05rem, 2.2vw, 1.2rem); color: var(--muted); margin: 0 0 .8rem; }
  .post-head .meta { font-size: .86rem; color: var(--muted); opacity: .8; }
  .post p { color: var(--ink); font-size: 1.06rem; margin: 1.1rem 0; }
  .post h2 { font-size: 1.5rem; margin: 2.4rem 0 .6rem; }
  .post ol, .post ul { color: var(--ink); font-size: 1.06rem; padding-left: 1.3rem; }
  .post li { margin: .5rem 0; }
  .post li strong { color: var(--ink); }
  .post a { color: var(--accent); }
  .post code { background: rgba(255,255,255,.06); border: 1px solid var(--line); padding: .08em .34em; border-radius: 5px; font-size: .92em; }
  .post figure.code { margin: 1.8rem 0; max-width: 100%; }
  .post blockquote { margin: 1.6rem 0; padding: .3rem 0 .3rem 1.2rem; border-left: 3px solid rgba(52,211,153,.5); color: var(--ink); font-style: italic; }
  .post .kicker { color: var(--emerald); font-weight: 700; font-style: normal; }
  .post hr { border: none; border-top: 1px solid var(--line); margin: 2.6rem 0 1.6rem; }
  .post .tail { font-size: .96rem; color: var(--muted); }
  .blog-index h1 { font-size: clamp(2rem, 5vw, 3rem); margin: .2rem 0 .4rem; font-weight: 700; }
  .blog-index > .lede { color: var(--muted); font-size: 1.1rem; margin: 0 0 2rem; }
  .post-card { display: block; border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel); backdrop-filter: blur(6px); padding: 1.3rem 1.5rem; margin: 0 0 1.1rem; transition: transform .12s ease, box-shadow .12s; }
  .post-card:hover { text-decoration: none; transform: translateY(-2px); box-shadow: 0 18px 50px -30px rgba(56,189,248,.5); }
  .post-card .date { font-size: .8rem; color: var(--sky); text-transform: uppercase; letter-spacing: .1em; font-weight: 700; }
  .post-card h2 { font-size: 1.3rem; margin: .4rem 0 .4rem; color: var(--ink); }
  .post-card p { color: var(--muted); margin: 0; font-size: .98rem; }
</style>`;
}

// The coding-agent harnesses Workforce can hire, in display order. Single source
// for the home page's harness chips and the architecture page's worker layer,
// so "bring your own harness" can't drift between them.
function agentHarnesses() {
  return ["nano-coder", "GitHub Copilot", "Claude Code", "Pi", "Open Coder", "Little Coder"];
}

// Single source of truth for the nano-coder claims repeated across the site
// (product page, home band, architecture layer, launch post). Hoisted function so
// it is callable during the top-level emit. Platforms mirror the npm package's
// optionalDependencies (@nanobpm/nano-coder-{darwin,linux}-{x64,arm64}).
function nanoCoderFacts() {
  return {
    install: "npm install -g @nanobpm/nano-coder",
    platforms: "macOS and Linux (x64, arm64)",
    rss: "~6MB",
    nodeRss: "150–660MB",
    repo: "https://github.com/nanobpm/nano-coder",
  };
}

// The @nanobpm/nano-coder product page: the coding agent that Nano Workforce
// orchestrates as a headless ACP worker. Facts mirror the nanobpm/nano-coder README.
function nanoCoderHtml() {
  const F = nanoCoderFacts();
  const FEATURES = [
    [`${F.rss} resident memory`, `Written in Rust. Node-based agent CLIs take ${F.nodeRss}; nano-coder lets you run a dense fleet of workers on one laptop.`],
    ["Interactive or headless", "A streaming terminal REPL with steering and queued prompts — or headless over ACP (JSON-RPC 2.0 on stdio) as a Nano Workforce worker."],
    ["Bring your own model", "OpenAI-compatible and Anthropic endpoints, remote or local, selected per model as <code>provider/model</code>."],
    ["Survives long tasks", "Task plans live outside the conversation, so work survives compaction, resume and a change of worker. Sessions are append-only JSONL."],
    ["Guardrails built in", "Destructive-command guards, allow/deny rules, and an optional OS sandbox (Seatbelt on macOS, Landlock on Linux)."],
    ["Speaks your repo", "Reads <code>AGENTS.md</code> / <code>CLAUDE.md</code>, loads <code>SKILL.md</code> skills on demand, and runs Claude Code–compatible hooks."],
  ];
  const cards = FEATURES.map(([h, p]) => `  <div class="post-card"><h2>${esc(h)}</h2><p>${p}</p></div>`).join("\n");
  const body = `${siteNav()}
${blogStyle()}
<section class="blog-index">
  <p class="eyebrow"><code>@nanobpm/nano-coder</code></p>
  <h1>A 6MB coding agent.<br><span class="grad">Run a fleet on your laptop.</span></h1>
  <p class="lede">nano-coder is the worker at the bottom of the Nano stack: a terminal coding agent
  small enough that Nano Workforce graphs can fan out dozens of them on one machine — durable,
  resumable, and supervised by the engine.</p>
  ${installCommand("Install nano-coder", F.install)}
  <p class="install-note">Prebuilt binaries for ${esc(F.platforms)}. Or <code>cargo install nano-coder</code>.</p>
${cards}
  <p class="lede">Why the worker's size matters: <a href="/blog/a-6mb-coding-agent/">A 6MB coding agent, and why fleets need one →</a></p>
  <div class="cta">
    <a class="btn primary" href="${F.repo}" rel="noopener noreferrer" target="_blank">nano-coder on GitHub →</a>
    <a class="btn ghost" href="https://www.npmjs.com/package/@nanobpm/nano-coder" rel="noopener noreferrer" target="_blank">npm package →</a>
  </div>
</section>

<footer class="site-foot wrap">
  <p><a href="/">Home</a> · <a href="/architecture/">Architecture</a> · <a href="/demo/">Browser demo</a> · <a href="/schemas/">Published schemas</a></p>
  <p class="muted">Nano is an Advanced Research Prototype. Free for personal or evaluation use.</p>
</footer>
${installCopyScript()}`;
  return homePage("nanobpm.io — nano-coder", body);
}

function blogIndexHtml() {
  const cards = blogPosts()
    .map(
      (p) => `  <a class="post-card" href="/blog/${esc(p.slug)}/">
    <span class="date">${esc(fmtDate(p.date))}</span>
    <h2>${esc(p.title)}</h2>
    <p>${esc(p.dek)}</p>
  </a>`,
    )
    .join("\n");
  const body = `${siteNav()}
${blogStyle()}
<section class="blog-index">
  <p class="eyebrow">Blog</p>
  <h1>From the <span class="grad">Nano</span> team</h1>
  <p class="lede">Notes on durable agent orchestration, the engine, and the framework.</p>
${cards}
</section>

<footer class="site-foot wrap">
  <p><a href="/">Home</a> · <a href="/architecture/">Architecture</a> · <a href="/demo/">Browser demo</a> · <a href="/schemas/">Published schemas</a></p>
  <p class="muted">Nano is an Advanced Research Prototype. Free for personal or evaluation use.</p>
</footer>`;
  return homePage("nanobpm.io — Blog", body);
}

function blogPostHtml(post) {
  const body = `${siteNav()}
${blogStyle()}
<article class="post">
  <header class="post-head">
    <p class="eyebrow"><a href="/blog/" style="color:inherit">Blog</a></p>
    <h1>${esc(post.title)}</h1>
    <p class="sub">${esc(post.subtitle)}</p>
    <p class="meta">${esc(fmtDate(post.date))} · the Nano team</p>
  </header>
${post.body()}
</article>

<footer class="site-foot wrap">
  <p><a href="/blog/">← All posts</a> · <a href="/architecture/#effect">Architecture: Effect &amp; Nano</a> · <a href="/demo/">Browser demo</a></p>
  <p class="muted">Nano is an Advanced Research Prototype. Free for personal or evaluation use.</p>
</footer>`;
  return homePage(`${post.title} — nanobpm.io`, body);
}

// The article body for the nano-coder launch post.
function nanoCoderPostBody() {
  const F = nanoCoderFacts();
  return `  <p>Nano Workforce turns software delivery into durable graphs: plan, implement, review,
  test, merge. The interesting graphs fan out — ten slices of an epic, each with its own coding
  agent, each looping through review until it converges. At that point the question stops being
  <em>how good is one agent</em> and becomes <em>how many can this machine hold?</em></p>

  <h2>The footprint is the ceiling</h2>
  <p>Node-based agent CLIs sit at ${F.nodeRss} of resident memory each. Multiply by a fleet and
  the laptop is full before the work is. Most of that memory does nothing: an agent spends its
  life waiting on a model or a test run.</p>

  <h2>nano-coder</h2>
  <p><a href="/nano-coder/">nano-coder</a> is a coding agent written in Rust that runs in about
  ${F.rss}. It is a complete terminal agent — streaming REPL, tool calling, task plans that survive
  compaction, OS sandboxing, <code>AGENTS.md</code> and skills — and it also runs headless over ACP
  (JSON-RPC 2.0 on stdio), which is how Workforce drives it as a durable job worker.</p>
  <p>Because the engine owns durability, the worker doesn't have to: if a nano-coder process dies,
  the Nano instance that dispatched it is still at the same step, and the next worker picks the job
  up. Small workers plus a durable orchestrator is the whole design.</p>

  <h2>Try it</h2>
  <figure class="code"><pre><code>${esc(F.install)}</code></pre></figure>
  <p>Prebuilt binaries ship for ${F.platforms}; or <code>cargo install nano-coder</code>. Source and
  docs are on <a href="${F.repo}">GitHub</a>.</p>`;
}

// The article body for the "code to coordination" post.
function coordinationPostBody() {
  return `  <p>For two years the whole industry optimized one number: how good is the model at
  writing code. It worked. Cross a threshold — reliable enough agents, a codebase
  <em>architected</em> so that mistakes are contained rather than catastrophic, and CI plus
  integration tests that actually catch regressions — and something quietly flips.
  <strong>Writing the code stops being the hard part.</strong></p>

  <p>That’s not a prediction; plenty of teams are already there on well-shaped repos. And the
  interesting thing about clearing a bottleneck is that it doesn’t end the story — it just
  <em>reveals the next one</em>. When any single change is cheap to produce and safe to land, the
  friction doesn’t disappear. It moves. It moves to <strong>coordination</strong>.</p>

  <h2>What made the code cheap</h2>
  <p>Three things have to be true at once, and each is doing real work:</p>
  <ol>
    <li><strong>Agents are reliable enough.</strong> A competent agent, given a well-scoped task,
    lands a correct change most of the time — and knows when it’s stuck.</li>
    <li><strong>The codebase is architected for safety.</strong> Clear seams, narrow interfaces,
    idempotent operations, blast-radius limits. A wrong edit fails loudly and locally instead of
    corrupting something three modules away.</li>
    <li><strong>CI and integration testing are a real safety net.</strong> Regressions get caught
    before they merge, so you can let many hands move quickly without holding your breath.</li>
  </ol>
  <p>Get those three and you can point <em>ten</em> agents at a backlog. Which is exactly when the
  new problem shows up.</p>

  <h2>So where did the friction go? Watch the agents wait.</h2>
  <p>Put ten-plus agents on one product and watch what they actually spend time on. A surprising
  amount of it is <strong>nothing</strong>. Waiting:</p>
  <ul>
    <li>Agent B needs the interface Agent A is still writing — so B waits on A’s merge.</li>
    <li>A pull request sits through a <strong>review cycle</strong>: request review, get comments,
    fix, re-request, converge. Round-trips, not keystrokes.</li>
    <li>A fan-out of twelve slices has to <strong>fan back in</strong> — the integration step can’t
    start until the slowest slice lands.</li>
    <li>Two agents reach for the same file and one has to back off, rebase, and retry.</li>
    <li>A change needs a human decision — a product tradeoff, an approval — and everything
    downstream <strong>blocks on a person</strong> who isn’t looking yet.</li>
  </ul>
  <p>None of that is a coding problem. You cannot fix it with a smarter model. It is the shape of
  the <em>dependency graph</em> and the <em>latency of the hand-offs</em> between agents.</p>

  <blockquote>With ten agents, the constraint isn’t how fast any one of them writes code. It’s how
  much of the time the other nine are blocked on it.</blockquote>

  <h2>Waiting is a first-class thing, not an accident</h2>
  <p>The instinct is to treat waiting as a bug — spin faster, poll harder, add a retry. But most of
  these waits are <em>legitimate</em>: the review genuinely has to happen; the dependency genuinely
  has to land first; the human genuinely has to decide. The problem isn’t that the wait exists. It’s
  that nothing in the system is designed to <strong>represent</strong> a wait, reason about it, and
  wake up cleanly when it resolves.</p>
  <p>So teams reinvent it, badly: a script that sleeps and re-checks; a spreadsheet of “who’s blocked
  on whom”; an agent burning tokens re-reading a PR every minute to see if review came back; state
  that lives only in one process, so a restart loses the whole plan. That’s a scheduler and a
  dependency resolver, hand-rolled and leaky, once per team.</p>

  <h2>This is a process-engineering problem</h2>
  <p>Step back and the shape is familiar. You have units of work with dependencies between them. Some
  run in parallel, some must serialize. Some pause on an external signal — a review verdict, a merge,
  a human approval — and resume when it arrives. Some fan out into N instances and join when all N
  finish. You want the whole thing to survive a process dying or a machine rebooting, and pick up
  exactly where it left off.</p>
  <p>That is not a novel AI problem. It is the <strong>oldest problem in workflow orchestration</strong>,
  and the field has a mature vocabulary for it: tasks and dependencies, parallel and multi-instance
  branches, message correlation, timers, and — crucially — <strong>wait states</strong> as a
  first-class primitive. A durable process engine doesn’t poll for a review to come back; it parks the
  branch on a wait state and is woken by the event. It doesn’t lose the plan on restart; the plan is a
  journalled graph, not a variable in memory.</p>
  <p>This is the bet behind <a href="/">Nano</a>. <strong>Nano Workforce</strong> models the software
  lifecycle — plan, implement, review, test, merge, QA, retro — as a durable graph, and the agents are
  workers it schedules against that graph. Dependencies are edges. Review cycles are wait states with
  timeouts and escalation. Fan-out/fan-in is a multi-instance activity with a join. A human decision is
  a user task the graph blocks on without burning a single token while it waits. The engine is Rust,
  Camunda&nbsp;8 API compatible, and small enough to run on a workstation — because coordinating ten
  agents shouldn’t require a cluster.</p>
  <p>The point isn’t “use a workflow engine because workflow engines exist.” It’s that the problem you
  hit at ten agents <em>is</em> a workflow problem, and rebuilding a durable scheduler by hand — one
  team at a time, one sleep-loop at a time — is the actual tax.</p>

  <h2>The takeaway</h2>
  <p>The next 10x in agent-assisted development probably isn’t a model that writes better functions.
  On a codebase that’s already safe to change, the functions are cheap. The next 10x is in the
  <em>gaps between the agents</em>: scheduling the work, representing the waits, resolving the
  dependencies, and never losing the plan when a process dies.</p>
  <p><span class="kicker">When the code writes itself, coordination is the job.</span></p>

  <hr>
  <p class="tail">See how the layers fit together on the
  <a href="/architecture/">Nano architecture page</a>, or try the durable engine in your browser at
  <a href="/demo/">nanobpm.io/demo</a>.</p>`;
}

// The article body for the effectlite post. Real code is highlighted with the
// same zero-dep highlighter the home hero uses.
function effectlitePostBody() {
  const snippet = `import { gen, ok, fail, tag, matchTags } from "@nanobpm/urban/effect";

const result = gen(function* () {
  const repo = yield* cloneRepo(url);   // yields Fail<CloneError> on failure
  const built = yield* build(repo);     // yields Fail<BuildError> on failure
  return built;                         // Result<Artifact, CloneError | BuildError>
});

// The compiler forces a handler for EVERY failure mode — omit one and it won't compile.
matchTags(result.error, {
  CloneError: (e) => retryLater(e),
  BuildError: (e) => report(e),
});`;
  return `  <p><a href="https://effect.website" rel="noopener noreferrer" target="_blank">Effect</a>
  is having a moment, and deservedly so. v4 lands the clearest version yet of an idea
  TypeScript has needed for years: make effects — errors, async, resources, concurrency —
  <em>first-class values</em> the compiler can reason about, instead of exceptions you hope
  someone remembers to catch.</p>

  <p>We build <a href="/">Nano</a> and its application framework, <strong>Urban</strong>, in
  TypeScript. So people ask the obvious question: <em>are you using Effect?</em></p>

  <p>The honest answer is more interesting than yes or no. <strong>We looked hard at Effect,
  loved the model, and shipped ~100 lines of it into Urban instead of taking the
  dependency.</strong> Here’s the reasoning — and why, for our layer of the stack, that was
  the right call rather than a compromise.</p>

  <h2>What we actually reach for</h2>
  <p>Strip Effect down to what a framework’s <em>glue code</em> — workers, provisioning,
  resource lifecycles — reaches for every day, and it’s a short list:</p>
  <ol>
    <li><strong>A typed error channel.</strong> A function that can fail should say so in its
    type, and the failure should compose automatically through a sequence of steps,
    short-circuiting on the first error. <code>Effect.gen</code> + <code>yield*</code> is the
    canonical ergonomic here.</li>
    <li><strong>Tagged errors with exhaustive handling.</strong> Model each failure mode as a
    discriminated variant, then let the compiler <em>force</em> you to handle every one — no
    silently dropped case. That’s <code>Data.TaggedError</code> + <code>catchTags</code>.</li>
    <li><strong>Scoped resources.</strong> Acquire something, guarantee its release on
    <em>every</em> exit path — success, failure, or a thrown exception. That’s
    <code>Effect.scoped</code> + <code>acquireRelease</code>.</li>
  </ol>

  <p>Those three carry the vast majority of the day-to-day value. So we built exactly those
  three, and nothing else, as <strong>effectlite</strong>: a zero-dependency, Effect-<em>like</em>
  core that lives in <code>@nanobpm/urban</code>’s <code>src/effect</code>.
  <code>Result&lt;A, E&gt;</code> with generator do-notation whose <code>E</code> composes through
  <code>yield*</code> just like the real thing; <code>tag()</code> + exhaustive
  <code>matchTags</code>; and <code>scoped</code> + <code>acquireRelease</code>. About a hundred
  lines of implementation, no runtime deps.</p>

  <figure class="code">
    <figcaption>Typed errors that compose through <code>yield*</code> — and an exhaustive match the compiler enforces.</figcaption>
    <pre><code>${highlightTs(snippet)}</code></pre>
  </figure>

  <p>If you know Effect, that reads like home. That’s the point.</p>

  <h2>Why not just depend on <code>effect</code>?</h2>
  <p>Two reasons, and neither is a knock on Effect — they’re about <em>our</em> layer.</p>
  <p><strong>1. Bundle weight against a polyglot, embed-everywhere surface.</strong> Urban’s
  published value isn’t a TS runtime — it’s a portable app model plus a client renderer that
  ships to <em>many</em> runtimes: Node, and embedded hosts on the JVM, GraalVM, and more,
  feeding language kits for Java, Rust, Python, C# and more. The imperative glue that benefits
  from Effect’s ergonomics is a <em>thin seam</em> around a declarative core. Pulling a full
  effect runtime into that seam is weight in exactly the place we work hardest to keep light.</p>
  <p><strong>2. The viral paradigm.</strong> Effect’s greatest strength —
  <code>Effect&lt;A, E, R&gt;</code> colouring every signature so the compiler tracks errors and
  requirements end-to-end — is also a whole-codebase commitment. It pays off spectacularly when
  your <em>entire</em> application is written in it. It pays off far less when you want three
  ergonomics in the 5% of your surface that’s imperative, while keeping the other 95% plain,
  portable, and dependency-free. Adopting the paradigm halfway is the worst of both worlds;
  distilling the three pieces we use is the best of both.</p>
  <p>This isn’t “Effect is too heavy.” It’s “Effect is a runtime for making an <em>entire
  application</em> robust, and our application’s robustness lives one layer down — in the engine,
  not the language.”</p>

  <h2>The real story: durable &gt; in-process</h2>
  <p>Here’s the part worth internalizing, because it’s where Nano and Effect genuinely
  <em>complement</em> each other rather than compete.</p>
  <p>Effect makes a <strong>process</strong> robust. Fibers, typed errors, resource safety — all
  of it operates <em>inside a running program</em>. It is the best-in-class answer to “how do I
  make this program correct and resilient while it runs.”</p>
  <p>Nano makes the <strong>workflow</strong> robust. Nano Workforce orchestrates the whole
  software lifecycle — plan, implement, review, test, merge — as a durable graph on a Rust engine
  that’s Camunda&nbsp;8 API compatible. When the process crashes, the machine reboots, or you
  redeploy mid-run, the graph <em>resumes at the exact step it left off</em>; journal-committed
  steps are never replayed.</p>
  <p>That’s a different axis. In-memory structured concurrency — however elegant — ends when the
  memory does. A fiber does not survive <code>kill -9</code>. A durable graph does. For work that
  spans minutes to weeks, waits on humans, and must outlive every process that touches it,
  durability isn’t a nicer error channel — it’s the whole game.</p>
  <p>So the two compose cleanly:</p>
  <ul>
    <li><strong>Effect, inside your workers</strong> — make each activity correct and resilient
    while it runs.</li>
    <li><strong>Nano, around the workers</strong> — carry the <em>run</em> across everything that
    outlives the process.</li>
  </ul>
  <p>You lose nothing by loving both. If you want the Effect model in the code Nano drives, use it
  — and let Nano own the part Effect was never trying to own.</p>

  <h2>The takeaway</h2>
  <p>We’re not on the Effect bandwagon, and we’re not going to pretend to be — the community would
  spot a missing <code>import { Effect }</code> in about four seconds. What we <em>are</em> is a
  team that admired the model enough to distill its best ideas into a hundred honest lines, and a
  stack that picks up exactly where an in-process effect system has to stop: at the boundary of the
  process itself.</p>
  <p><span class="kicker">Effect for the process. Nano for the workflow.</span></p>

  <hr>
  <p class="tail">See where this fits in the stack on the
  <a href="/architecture/#effect">Nano architecture page</a>, or try the durable engine in your
  browser at <a href="/demo/">nanobpm.io/demo</a>.</p>`;
}

function homePage(title, body) {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(title)}</title>
<meta name="description" content="Nano is an Advanced Research Prototype for agent graph orchestration on the developer workstation: a RAAD — Rapid Agent Application Development — environment that composes coding agents, tools, and human approvals into durable, code-first or model-first workflows, provider-agnostic across frontier and local LLMs.">
<style>
  :root {
    --bg: #08080a; --panel: rgba(22,24,30,.55); --line: rgba(120,130,150,.16);
    --ink: #e4e4e7; --muted: #a1a1aa; --emerald: #34d399; --sky: #38bdf8;
    --accent: #7dd3fc; --code-bg: #0d1117; --radius: 12px; --wrap: 62rem;
  }
  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }
  body {
    margin: 0; background: var(--bg); color: var(--ink); position: relative;
    font: 17px/1.65 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    -webkit-font-smoothing: antialiased;
  }
  #field { position: fixed; inset: 0; z-index: 0; display: block; pointer-events: none; }
  .glow {
    position: fixed; inset: 0; z-index: 1; pointer-events: none;
    background:
      radial-gradient(60% 60% at 50% 18%, rgba(56,189,248,.12), transparent 70%),
      radial-gradient(50% 50% at 78% 62%, rgba(52,211,153,.10), transparent 70%);
  }
  header, section, footer { position: relative; z-index: 2; }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .wrap { max-width: var(--wrap); margin-inline: auto; padding-inline: 1.4rem; }
  h1, h2, h3 { line-height: 1.12; letter-spacing: -0.02em; }

  .nav {
    display: flex; align-items: center; justify-content: space-between;
    max-width: var(--wrap); margin-inline: auto; padding: 1.1rem 1.4rem;
  }
  .brand { font-weight: 700; font-size: 1.1rem; color: var(--ink); letter-spacing: -0.02em; }
  .brand:hover { text-decoration: none; }
  .brand .dim { color: var(--muted); font-weight: 500; }
  .nav nav a { color: var(--muted); margin-left: 1.4rem; font-size: .95rem; }
  .nav nav a:hover { color: var(--ink); text-decoration: none; }
  .nav nav a.ext { display: inline-flex; align-items: center; gap: .4rem; }
  .nav nav a.ext .icon { display: block; }

  .hero { max-width: var(--wrap); margin-inline: auto; padding: 4rem 1.4rem 1.5rem; text-align: center; }
  .eyebrow {
    text-transform: uppercase; letter-spacing: 0.18em; font-size: .78rem; font-weight: 700;
    color: var(--sky); margin: 0 0 1rem;
  }
  .hero h1 {
    font-size: clamp(2.2rem, 5.6vw, 3.7rem); margin: 0 0 .6rem; font-weight: 700;
    /* Match the console landing page's heading gradient (server/crates/nano-server-console/src/landing.html). */
    background: linear-gradient(135deg, #ffffff 0%, var(--sky) 55%, var(--emerald) 100%);
    -webkit-background-clip: text; background-clip: text; color: transparent;
  }
  .hero h1 .hl-sub { font-size: .68em; font-weight: 600; font-style: italic; }
  .subhead { font-size: clamp(1.2rem, 2.8vw, 1.55rem); color: var(--ink); font-weight: 600; max-width: 40rem; margin: 0 auto 1.6rem; letter-spacing: -0.01em; }
  .grad {
    background: linear-gradient(90deg, var(--emerald), var(--sky));
    -webkit-background-clip: text; background-clip: text; color: transparent;
  }
  .lede { font-size: clamp(1.05rem, 2.2vw, 1.25rem); color: var(--muted); max-width: 40rem; margin: 0 auto 1.8rem; }
  .cta { display: flex; gap: .8rem; justify-content: center; flex-wrap: wrap; }
  .btn {
    display: inline-block; padding: .72rem 1.2rem; border-radius: 10px; font-weight: 600;
    font-size: .98rem; border: 1px solid var(--line); color: var(--ink);
    background: rgba(255,255,255,.05); transition: transform .12s ease, box-shadow .12s, background .12s;
  }
  .btn:hover { text-decoration: none; transform: translateY(-2px); }
  .btn.primary {
    color: #052e1a; border-color: transparent;
    background: linear-gradient(135deg, var(--emerald), var(--sky));
    box-shadow: 0 8px 30px rgba(56,189,248,.3);
  }
  .btn.ghost { background: rgba(255,255,255,.05); color: var(--ink); }
  .btn.ghost:hover { background: rgba(255,255,255,.09); }
  .install { margin: 1.4rem auto 0; }
  .install code {
    background: rgba(255,255,255,.04); border: 1px solid var(--line); color: var(--ink);
    padding: .55rem .9rem; border-radius: 8px; font-size: .95rem; display: inline-block;
  }
  .install code::before { content: "$ "; color: var(--muted); }
  .install.multi { display: flex; flex-direction: column; gap: .5rem; align-items: flex-start; max-width: max-content; margin-inline: auto; }
  .install.multi.center { align-items: center; }
  .install.multi code { text-align: left; }
  .install-copy { width: fit-content; max-width: 100%; margin: 1.4rem auto 0; }
  .install.install-command {
    display: flex; align-items: stretch; margin: 0; text-align: left;
    background: rgba(255,255,255,.04); border: 1px solid var(--line); border-radius: 8px;
  }
  .install-command code { background: none; border: 0; min-width: 0; overflow-wrap: anywhere; align-self: center; }
  .install-copy-button {
    display: grid; place-items: center; flex: 0 0 44px; min-height: 44px; padding: 0;
    color: var(--muted); background: transparent; border: 0; border-left: 1px solid var(--line);
    border-radius: 0 7px 7px 0; cursor: pointer;
  }
  .install-copy-button[hidden] { display: none; }
  .install-copy-button:hover { color: var(--ink); background: rgba(255,255,255,.08); }
  .install-copy-button:focus-visible { outline: 2px solid var(--sky); outline-offset: 3px; }
  .install-copy-button[aria-busy="true"] { cursor: progress; }
  .install-copy-status { min-height: 1.2em; margin: .3rem 0 0; font-size: .85rem; line-height: 1.2; color: var(--sky); }
  .install-note { margin: .6rem auto 0; color: var(--muted); font-size: .9rem; max-width: 52ch; }
  .install-note.center { text-align: center; }
  .install-manual { margin: .9rem auto 0; max-width: max-content; }
  .install-manual > summary { cursor: pointer; color: var(--muted); font-size: .9rem; list-style: revert; }
  .install-manual[open] > summary { margin-bottom: .6rem; }

  .arp {
    display: inline-block; border: 1px solid rgba(56,189,248,.4); border-radius: 999px;
    padding: .28rem .8rem; background: rgba(56,189,248,.10); color: var(--sky);
  }

  .demo-tabs { padding-top: 3.5rem; text-align: center; }
  .tabs-title { font-size: clamp(1.6rem, 3.6vw, 2.2rem); margin: 0 0 .4rem; }
  .tabs-sub { color: var(--muted); margin: 0 auto 1.4rem; font-size: 1rem; }
  .tabs-sub code, .how-sub code, .raad-sub code, .band.providers code { background: rgba(255,255,255,.05); padding: .08em .35em; border-radius: 5px; font-size: .9em; color: var(--sky); }  .tablist { display: inline-flex; gap: .5rem; padding: .3rem; border: 1px solid var(--line); border-radius: 12px; background: rgba(255,255,255,.03); margin-bottom: 1.4rem; }
  .tab {
    padding: .55rem 1.15rem; border-radius: 9px; border: 1px solid transparent; cursor: pointer;
    background: transparent; color: var(--muted); font-weight: 600; font-size: .95rem; font-family: inherit;
    transition: background .12s, color .12s, box-shadow .12s;
  }
  .tab:hover { color: var(--ink); }
  .tab[aria-selected="true"] {
    color: #052e1a; border-color: transparent;
    background: linear-gradient(135deg, var(--emerald), var(--sky));
    box-shadow: 0 8px 24px rgba(56,189,248,.28);
  }
  .tabpanel[hidden] { display: none; }
  .demo-frame {
    width: 100%; height: 560px; border: 1px solid rgba(120,130,150,.2); border-radius: var(--radius);
    background: var(--code-bg); box-shadow: 0 24px 60px -24px rgba(0,0,0,.7); display: block;
  }
  .tab-note { font-size: .92rem; color: var(--muted); margin: .8rem .2rem 0; }
  figure.code.pop pre {
    border-color: rgba(56,189,248,.35);
    box-shadow: 0 24px 60px -20px rgba(56,189,248,.35), 0 0 0 1px rgba(52,211,153,.12) inset;
  }

  .band.providers { background: var(--panel); border-block: 1px solid var(--line); backdrop-filter: blur(6px); text-align: center; }
  .band.polyglot { text-align: center; }
  .band.providers h2, .band.polyglot h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 .8rem; }
  .band.providers p, .band.polyglot p { color: var(--muted); font-size: 1.06rem; max-width: 44rem; margin: 0 auto; }
  .provs { display: flex; flex-wrap: wrap; gap: .6rem; justify-content: center; margin-top: 1.3rem; }
  .provs span {
    border: 1px solid var(--line); background: rgba(255,255,255,.04); padding: .42rem .85rem;
    border-radius: 999px; font-size: .92rem; color: var(--ink); font-weight: 500;
  }
  .provs span.more { color: var(--muted); font-style: italic; }
  .provs.langs span.lang { display: inline-flex; align-items: center; gap: .5rem; }
  .provs.langs span.lang > svg { flex: none; width: 1.2rem; height: 1.2rem; display: block; }
  .how-sub { color: var(--muted); max-width: 46rem; margin: 0 0 1.2rem; }
  .how .install.multi { margin: 0 0 1.8rem; }
  .band.backbone .raad-sub { color: var(--muted); max-width: 40rem; margin: 0 auto 1.4rem; font-size: 1.05rem; }
  .band.backbone .install.multi { margin-bottom: 1.6rem; }

  figure.code { margin: 2.8rem auto 0; max-width: 52rem; text-align: left; }
  figure.code figcaption { font-size: .9rem; color: var(--muted); margin: 0 0 .5rem .2rem; }
  figure.code pre {
    background: var(--code-bg); color: #c9d1d9; border-radius: var(--radius);
    padding: 1.15rem 1.25rem; overflow-x: auto; font-size: .9rem; line-height: 1.65;
    box-shadow: 0 24px 60px -24px rgba(0,0,0,.7); border: 1px solid rgba(120,130,150,.2);
  }
  .code-note { font-size: .92rem; color: var(--muted); margin: .7rem .2rem 0; }

  .tok-com { color: #8b949e; font-style: italic; }
  .tok-str { color: #a5d6ff; }
  .tok-kw { color: #ff7b72; }
  .tok-fn { color: #d2a8ff; }
  .tok-prop { color: #79c0ff; }
  .tok-num { color: #79c0ff; }
  .tok-arw { color: #ff7b72; }
  .tok-id, .tok-punc { color: #c9d1d9; }

  .band { margin-top: 4rem; padding: 3.2rem 0; }
  .band.problem { background: var(--panel); border-block: 1px solid var(--line); backdrop-filter: blur(6px); }
  .band.problem h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 .8rem; max-width: 34rem; }
  .band.problem p { color: var(--muted); font-size: 1.08rem; max-width: 44rem; margin: 0; }

  .pillars { padding: 3.6rem 1.4rem; display: grid; gap: 1.6rem; grid-template-columns: repeat(3, 1fr); }
  .pillars article {
    border: 1px solid var(--line); border-radius: var(--radius); padding: 1.6rem 1.5rem;
    background: var(--panel); backdrop-filter: blur(6px);
  }
  .pillars h3 { font-size: 1.2rem; margin: 0 0 .5rem; }
  .pillars h3::before { content: ""; display: inline-block; width: .55rem; height: .55rem; border-radius: 2px; background: linear-gradient(135deg, var(--emerald), var(--sky)); margin-right: .55rem; vertical-align: middle; }
  .pillars p { margin: 0 0 .7rem; color: var(--muted); font-size: .98rem; }
  .pillars .proof { color: var(--ink); font-weight: 600; font-size: .9rem; margin-bottom: 0; }
  .pillars .proof code { background: rgba(255,255,255,.05); padding: .08em .35em; border-radius: 5px; font-size: .85em; font-weight: 500; color: var(--sky); }

  .how { padding: 1.5rem 1.4rem 3.8rem; }
  .how h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 1.6rem; }
  .steps { list-style: none; margin: 0; padding: 0; display: grid; gap: 1.1rem; max-width: 46rem; }
  .steps li { display: flex; gap: 1rem; align-items: flex-start; }
  .steps li span {
    flex: none; width: 1.9rem; height: 1.9rem; border-radius: 50%; background: rgba(255,255,255,.05);
    border: 1px solid var(--line); color: var(--sky); font-weight: 700;
    display: grid; place-items: center; font-size: .95rem;
  }
  .steps li div { color: var(--muted); }
  .steps li b { color: var(--ink); }
  .steps code { background: rgba(255,255,255,.05); padding: .08em .35em; border-radius: 5px; font-size: .85em; color: var(--sky); }

  .band.backbone {
    background: linear-gradient(180deg, rgba(15,17,21,.55), rgba(8,8,10,.85));
    border-block: 1px solid var(--line); text-align: center; padding: 3.8rem 0; margin-top: 0;
    backdrop-filter: blur(6px);
  }
  .band.backbone h2 { font-size: clamp(1.8rem, 4.4vw, 2.8rem); margin: 0 0 1.6rem; }

  .site-foot { padding: 2.6rem 1.4rem 3.2rem; }
  .site-foot p { margin: .3rem 0; }
  .site-foot .muted, .muted { color: var(--muted); }
  .site-foot .muted { font-size: .9rem; max-width: 42rem; }

  .band.compare { text-align: center; margin-top: 4rem; }
  .band.compare h2 { font-size: clamp(1.5rem, 3.4vw, 2rem); margin: 0 0 .6rem; }
  .band.compare .compare-sub { color: var(--muted); max-width: 46rem; margin: 0 auto 1.8rem; font-size: 1.02rem; }
  .compare-scroll { overflow-x: auto; -webkit-overflow-scrolling: touch; border-radius: var(--radius); }
  .compare-scroll:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
  .compare-table {
    width: 100%; min-width: 760px; border-collapse: collapse; text-align: left;
    margin-inline: auto; font-size: .93rem;
  }
  .compare-table th, .compare-table td {
    padding: .72rem .85rem; border-bottom: 1px solid var(--line); vertical-align: top;
  }
  .compare-table thead th {
    font-size: .82rem; letter-spacing: -0.01em; color: var(--ink); font-weight: 700;
  }
  .compare-table thead th.brandcol { color: var(--sky); }
  .compare-table tbody th.rowlabel { color: var(--muted); font-weight: 600; white-space: nowrap; }
  .compare-table .cell { display: block; color: var(--ink); }
  .visually-hidden {
    position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px;
    overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;
  }
  .compare-table .cell.yes::before { content: "✓ "; color: var(--emerald); font-weight: 700; }
  .compare-table .cell.partial::before { content: "~ "; color: var(--sky); font-weight: 700; }
  .compare-table .cell.no { color: var(--muted); }
  .compare-table .cell.no::before { content: "— "; color: var(--muted); }
  .compare-table .col-nano { background: rgba(56,189,248,.06); }
  .compare-table thead th.col-nano { background: rgba(56,189,248,.12); }
  .compare-table th.col-nano, .compare-table td.col-nano { border-inline: 1px solid rgba(56,189,248,.20); }
  .compare-table tbody tr:last-child td, .compare-table tbody tr:last-child th { border-bottom: 0; }
  .compare-foot { color: var(--muted); font-size: .88rem; margin: 1.3rem auto 0; max-width: 48rem; }

  @media (max-width: 800px) { .pillars { grid-template-columns: 1fr; } .demo-frame { height: 460px; } }
  @media (prefers-reduced-motion: reduce) { #field { display: none; } }
</style>
</head>
<body>
<canvas id="field" aria-hidden="true"></canvas>
<div class="glow" aria-hidden="true"></div>
${body}
<script>
// A rotating "constellation" particle field (ported from the Nano server's own
// landing page): points drift, the whole field slowly rotates around the centre,
// and nearby points are linked by lines whose opacity falls off with distance.
// Pure canvas, no dependencies, so the page stays self-contained and works offline.
(function () {
  var canvas = document.getElementById("field");
  if (!canvas) return;
  var ctx = canvas.getContext("2d");
  if (!ctx) return;
  // Respect prefers-reduced-motion: the canvas is CSS-hidden, but bail out here too
  // so we never start the rAF loop or resize handler for users who disabled motion.
  var reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)");
  if (reduce && reduce.matches) return;
  var w, h, cx, cy, points, dpr, rot = 0;
  function resize() {
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    w = canvas.width = Math.floor(innerWidth * dpr);
    h = canvas.height = Math.floor(innerHeight * dpr);
    canvas.style.width = innerWidth + "px";
    canvas.style.height = innerHeight + "px";
    cx = w / 2; cy = h / 2; seed();
  }
  function seed() {
    var count = Math.min(140, Math.floor((w * h) / (22000 * dpr)));
    points = [];
    for (var i = 0; i < count; i++) {
      points.push({
        a: Math.random() * Math.PI * 2,
        r: Math.pow(Math.random(), 0.6) * Math.min(w, h) * 0.55,
        vx: (Math.random() - 0.5) * 0.25 * dpr,
        vy: (Math.random() - 0.5) * 0.25 * dpr,
        x: 0, y: 0, driftX: 0, driftY: 0,
      });
    }
  }
  var LINK = 130;
  function frame() {
    rot += 0.0006;
    ctx.clearRect(0, 0, w, h);
    var cosR = Math.cos(rot), sinR = Math.sin(rot), link = LINK * dpr, i, j, p, a, b;
    for (i = 0; i < points.length; i++) {
      p = points[i];
      var bx = Math.cos(p.a) * p.r, by = Math.sin(p.a) * p.r;
      p.driftX += p.vx; p.driftY += p.vy;
      if (Math.abs(p.driftX) > 60 * dpr) p.vx *= -1;
      if (Math.abs(p.driftY) > 60 * dpr) p.vy *= -1;
      p.x = cx + (bx * cosR - by * sinR + p.driftX);
      p.y = cy + (bx * sinR + by * cosR + p.driftY);
    }
    for (i = 0; i < points.length; i++) {
      for (j = i + 1; j < points.length; j++) {
        a = points[i]; b = points[j];
        var d = Math.hypot(a.x - b.x, a.y - b.y);
        if (d < link) {
          var t = 1 - d / link;
          ctx.strokeStyle = "rgba(80, 200, 230, " + (0.18 * t) + ")";
          ctx.lineWidth = dpr * 0.6;
          ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
        }
      }
    }
    for (i = 0; i < points.length; i++) {
      p = points[i];
      ctx.beginPath(); ctx.arc(p.x, p.y, dpr * 1.6, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(150, 230, 220, 0.85)"; ctx.fill();
    }
    requestAnimationFrame(frame);
  }
  addEventListener("resize", resize);
  resize(); frame();
})();
</script>
</body>
</html>
`;
}

function schemasHtml(items) {
  const rows = items
    .map(
      (i) => `<tr>
  <td><a href="${esc(i.url)}"><code>${esc(i.url)}</code></a><br>
      <span class="muted">${esc(i.kind)}</span></td>
  <td>${i.desc}</td>
</tr>`,
    )
    .join("\n");
  return page(
    "nanobpm.io — published schemas",
    `<h1>Published schemas &amp; namespaces</h1>
<p><a href="/">← nanobpm.io</a></p>
<p>The canonical home for the schemas and namespaces published by
Nano / ProcessOS. Every URL below is generated from its in-repo source of truth on
each deploy, so it never drifts from the artifact it names.</p>
<table>
<tr><th>Identifier</th><th>Description</th></tr>
${rows}
</table>`,
  );
}

function shapesNamespaceHtml(d) {
  const types = (d.types || [])
    .map((t) => {
      const props = (t.properties || [])
        .map(
          (p) =>
            `<tr><td><code>${esc(p.name)}</code></td><td><code>${esc(
              p.type,
            )}</code>${p.isMany ? " []" : ""}${
              p.isAttr ? ' <span class="muted">(attr)</span>' : ""
            }</td></tr>`,
        )
        .join("");
      const sc = (t.superClass || []).join(", ");
      return `<h2><code>${esc(d.prefix)}:${esc(t.name)}</code></h2>
${sc ? `<p class="muted">extends ${esc(sc)}</p>` : ""}
${props ? `<table><tr><th>property</th><th>type</th></tr>${props}</table>` : "<p class='muted'>(no properties)</p>"}`;
    })
    .join("\n");
  return page(
    `${d.name} shapes namespace — ${d.uri}`,
    `<h1>${esc(d.name)} composed-shapes namespace</h1>
<p>XML namespace <code>${esc(d.uri)}</code> · prefix <code>${esc(d.prefix)}:</code></p>
<p>This is the BPMN <strong>moddle extension</strong> namespace for Nano's composed
shapes (ADR 0040). It is an XML namespace <em>identifier</em>: bpmn-js registers the
descriptor in code (it is not fetched at runtime), but this page documents the
elements a <code>.bpmn</code> may carry under this namespace.</p>
<p>Declare it on the root element:</p>
<pre>&lt;bpmn:definitions xmlns:${esc(d.prefix)}="${esc(d.uri)}" …&gt;</pre>
<h2 class="muted">Element types</h2>
${types}`,
  );
}
