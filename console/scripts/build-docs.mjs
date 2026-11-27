// Builds the bundled documentation website from the repository USERGUIDE.md.
//
// The single source of truth is ../USERGUIDE.md (the task-oriented end-user
// manual, deliberately separate from README.md which is the project/operating
// reference and DEVELOPMENT.md which is source-focused): it is chunked on its H2
// (`##`) headings into one page per section (the preamble before the first H2
// becomes the "Overview" home page), rendered to HTML with markdown-it, and
// wrapped in a branded shell with a persistent sidebar. Output lands in
// public/docs/, which Vite ships into dist/ and the gateway embeds, so /docs
// works fully offline.
//
// USERGUIDE.md is the source; the generated HTML is git-ignored. Re-run via
// `npm run build` (or directly) to refresh it.
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import MarkdownIt from "markdown-it";

import {
  createDocsMarkdown,
  DOCS_CSS,
  esc,
  isExternalHref,
  repoRelative,
  slugify,
} from "./docs-render.mjs";

const root = process.cwd();
const readmePath = join(root, "..", "USERGUIDE.md");
// Extra standalone guides bundled alongside the User Guide. Each lives in the
// repo `docs/` dir (so its repo-relative links resolve against `docs/`, not the
// repo root) and is shipped as one self-contained page under `/docs/<slug>`.
const extraGuides = [
  {
    slug: "getting-started-urban",
    title: "Getting Started with Urban",
    path: join(root, "..", "docs", "getting-started-urban.md"),
    linkBase: "docs",
  },
  {
    slug: "extensions",
    title: "Authoring extensions",
    path: join(root, "..", "docs", "extensions.md"),
    linkBase: "docs",
  },
  {
    slug: "runbook-snapshot-blockers",
    title: "Runbook: snapshot blockers",
    path: join(root, "..", "docs", "runbook-snapshot-blockers.md"),
    linkBase: "docs",
  },
];
const outDir = join(root, "public", "docs");

// When publishing the docs to the static nanobpm.io site (via pages.yml), the
// app-only top-bar links (/swagger, /asyncapi, /console) have no counterpart:
// they are live routes on a running Nano node only. Omit them off-node so the
// published docs carry no dead links. The in-app build (Vite) leaves them on.
const SITE_BUILD = process.env.DOCS_SITE_BUILD === "1";

// Repo links in the README are relative to the repo root; rewrite them to GitHub
// blob URLs so they resolve from the shipped, standalone docs site.
const REPO_BLOB = "https://github.com/nanobpm/nano-bpm/blob/main/";

// H2 sections that belong in the GitHub README for contributors but NOT in the
// shipped, end-user docs website (the binary distribution can't build from
// source). Matched against the H2 heading text, case-insensitively.
const DOCS_EXCLUDE = new Set(["building from source"]);

const markdown = readFileSync(readmePath, "utf8");
const lines = markdown.split("\n");

// --- 1. Chunk the README into sections on H2 boundaries ---------------------
const sections = [];
let current = { heading: null, level: 1, lines: [] };
let inFence = false;
for (const line of lines) {
  if (/^```/.test(line.trim())) inFence = !inFence;
  const h2 = !inFence && /^## (.+)$/.exec(line);
  if (h2) {
    if (current.lines.join("").trim() || current.heading)
      sections.push(current);
    current = { heading: h2[1].trim(), level: 2, lines: [] };
    continue;
  }
  current.lines.push(line);
}
if (current.lines.join("").trim() || current.heading) sections.push(current);

// Drop sections that are contributor-only (e.g. building from source) from the
// shipped User Guide; they remain in the GitHub README.
const visibleSections = sections.filter(
  (s) => !(s.heading && DOCS_EXCLUDE.has(s.heading.toLowerCase())),
);

// First section is the preamble (H1 + intro) -> the engine "Overview" page.
const preamble = visibleSections.shift();
const overviewH1 = /^# (.+)$/m.exec(preamble.lines.join("\n"));
const pages = [
  // Home page (/docs): the task-focused Nano Workforce Getting Started. Its own
  // source file so product onboarding leads the docs; the engine User Guide
  // (below) is demoted to a sidebar page.
  {
    slug: "index",
    title: "Getting Started with Nano Workforce",
    heading: "Getting Started with Nano Workforce",
    markdown: readFileSync(join(root, "..", "docs", "getting-started.md"), "utf8"),
    href: "/docs",
    linkBase: "docs",
  },
  // The engine User Guide preamble, no longer the home page.
  {
    slug: "overview",
    title: "Overview",
    heading: overviewH1 ? overviewH1[1].trim() : "Overview",
    markdown: preamble.lines.join("\n"),
    href: "/docs/overview",
    linkBase: "",
  },
  ...visibleSections.map((s) => {
    const slug = slugify(s.heading);
    return {
      slug,
      title: s.heading,
      heading: s.heading,
      // Re-prepend the H2 so it renders (with an id) at the top of its page.
      markdown: `## ${s.heading}\n${s.lines.join("\n")}`,
      href: `/docs/${slug}`,
      linkBase: "",
    };
  }),
  // Standalone guides shipped as their own single page (rendered verbatim,
  // keeping their own H1). Their repo-relative links resolve against `linkBase`.
  ...extraGuides.map((g) => ({
    slug: g.slug,
    title: g.title,
    heading: g.title,
    markdown: readFileSync(g.path, "utf8"),
    href: `/docs/${g.slug}`,
    linkBase: g.linkBase,
  })),
];

// --- 2. Map every heading slug to the page that owns it ---------------------
// So intra-document anchor links (`#some-heading`) can target the right page.
const anchorToPage = new Map();
for (const page of pages) {
  for (const line of page.markdown.split("\n")) {
    const m = /^#{1,6} (.+)$/.exec(line);
    if (m) anchorToPage.set(slugify(m[1].trim()), page);
  }
}

// --- 3. markdown-it: heading ids + link rewriting (shared renderer) -------
// Rewrite a source link target for the standalone, multi-page docs site. `base`
// is the source file's dir relative to the repo root (e.g. "" for USERGUIDE.md,
// "docs" for docs/extensions.md) so repo-relative links resolve correctly.
function rewriteHref(href, base) {
  if (!href) return href;
  if (isExternalHref(href)) return href;
  // Site-absolute doc route (e.g. /docs/extensions) — already a working link on
  // both the in-node /docs and nanobpm.io/docs. Leave it untouched.
  if (href.startsWith("/")) return href;
  if (href.startsWith("#")) {
    const anchor = href.slice(1);
    const page = anchorToPage.get(anchor);
    if (page)
      return page.slug === "index"
        ? `/docs#${anchor}`
        : `${page.href}#${anchor}`;
    return href; // unknown anchor — leave as-is
  }
  // Repo-relative path (optionally with its own #fragment) -> GitHub blob URL.
  const [path, frag] = href.split("#");
  return REPO_BLOB + repoRelative(base, path) + (frag ? `#${frag}` : "");
}

const md = createDocsMarkdown(MarkdownIt, {
  rewriteHref: (href, env) => rewriteHref(href, env.linkBase ?? ""),
});

// --- 4. Page shell ----------------------------------------------------------
function navList(activeSlug) {
  return pages
    .map((p) => {
      const cls = p.slug === activeSlug ? "nav-link active" : "nav-link";
      return `<a class="${cls}" href="${p.href}">${esc(p.title)}</a>`;
    })
    .join("\n");
}

function shell(page) {
  const body = md.render(page.markdown, { linkBase: page.linkBase ?? "" });
  const subtitle = page.slug === "index" ? "Documentation" : esc(page.title);
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Nano BPM · ${esc(page.title)}</title>
    <style>
${DOCS_CSS}
    </style>
  </head>
  <body>
    <div class="nbpm-bar">
      <a class="brand" href="/"><span class="dot"></span><strong>Nano BPM</strong></a>
      <span class="badge">Advanced Research Prototype</span>
      <span class="sub">${subtitle}</span>
      <span class="spacer"></span>
      ${
        SITE_BUILD
          ? ""
          : `<a class="x" href="/swagger">REST API</a>
      <a class="x" href="/asyncapi">Falcon protocol</a>
      <a class="x" href="/console">Web console</a>`
      }
    </div>
    <div class="layout">
      <nav class="sidebar">
        <div class="nav-title">Documentation</div>
        ${navList(page.slug)}
      </nav>
      <main class="content">
        ${body}
      </main>
    </div>
  </body>
</html>`;
}

// --- 5. Emit -----------------------------------------------------------------
// Wipe any previously generated pages so renamed/removed README sections don't
// leave orphaned, stale HTML behind (the whole dir is regenerated each build).
rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });
for (const page of pages) {
  writeFileSync(join(outDir, `${page.slug}.html`), shell(page));
}
console.log(`generated docs site -> public/docs/ (${pages.length} pages)`);
