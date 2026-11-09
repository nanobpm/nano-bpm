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

const esc = (s) =>
  String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

// GitHub-compatible heading slug (matches the source docs' own intra-doc
// anchors). GitHub maps each space to a hyphen WITHOUT collapsing runs, so a
// removed punctuation char sitting between spaces (e.g. the em-dash in
// "Connector — outbound workers") yields a double hyphen — replace ` ` per-char,
// not `\s+`, or such anchors silently break.
function slugify(text) {
  return String(text)
    .toLowerCase()
    .replace(/<[^>]+>/g, "")
    .replace(/[^\w\- ]+/g, "")
    .trim()
    .replace(/ /g, "-");
}

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

// --- 3. markdown-it: heading ids + link rewriting ---------------------------
const md = new MarkdownIt({ html: false, linkify: true, breaks: false });

// Inject GitHub-style ids onto headings so in-page anchors resolve.
md.renderer.rules.heading_open = (tokens, idx, options, _env, self) => {
  const inline = tokens[idx + 1];
  const text = inline && inline.type === "inline" ? inline.content : "";
  const id = slugify(text.replace(/`/g, ""));
  if (id) tokens[idx].attrSet("id", id);
  return self.renderToken(tokens, idx, options);
};

// Resolve a repo-relative link (optionally with `../`/`./` segments) against the
// source file's directory into a repo-root-relative path, for the GitHub blob URL.
function repoRelative(base, path) {
  const baseParts = base ? base.split("/").filter(Boolean) : [];
  const out = [...baseParts];
  for (const seg of path.replace(/^\.\//, "").split("/")) {
    if (seg === "..") out.pop();
    else if (seg !== "." && seg !== "") out.push(seg);
  }
  return out.join("/");
}

// Rewrite a source link target for the standalone, multi-page docs site. `base`
// is the source file's dir relative to the repo root (e.g. "" for USERGUIDE.md,
// "docs" for docs/extensions.md) so repo-relative links resolve correctly.
function rewriteHref(href, base) {
  if (!href) return href;
  if (/^(https?:|mailto:)/i.test(href)) return href;
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

const defaultLinkOpen =
  md.renderer.rules.link_open ||
  ((tokens, idx, options, _env, self) =>
    self.renderToken(tokens, idx, options));
md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
  const hrefIdx = tokens[idx].attrIndex("href");
  if (hrefIdx >= 0) {
    tokens[idx].attrs[hrefIdx][1] = rewriteHref(
      tokens[idx].attrs[hrefIdx][1],
      env?.linkBase ?? "",
    );
  }
  return defaultLinkOpen(tokens, idx, options, env, self);
};

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
      :root {
        color-scheme: dark;
        --bg: #08080a; --fg: #e4e4e7; --muted: #a1a1aa; --line: rgba(120, 130, 150, 0.16);
        --card: rgba(22, 24, 30, 0.55); --accent: #7dd3fc; --code-bg: #0d1117;
      }
      * { box-sizing: border-box; }
      html { scroll-behavior: smooth; }
      body { margin: 0; background: var(--bg); color: var(--fg);
        font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; line-height: 1.6; }
      code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.86em; }
      a { color: var(--accent); }
      .nbpm-bar { display: flex; align-items: center; gap: 0.75rem; padding: 0.6rem 1rem;
        background: rgba(16, 18, 24, 0.72); backdrop-filter: blur(8px);
        border-bottom: 1px solid var(--line); position: sticky; top: 0; z-index: 10; }
      .nbpm-bar a.brand { display: flex; align-items: center; gap: 0.5rem; text-decoration: none; color: var(--fg); }
      .nbpm-bar strong { font-weight: 600; }
      .nbpm-bar .dot { width: 0.6rem; height: 0.6rem; border-radius: 9999px;
        background: linear-gradient(135deg, #34d399, #38bdf8); }
      .nbpm-bar .sub { color: var(--muted); font-size: 0.85rem; }
      .nbpm-bar .badge { font-size: 0.6rem; font-weight: 700; letter-spacing: 0.1em;
        text-transform: uppercase; color: var(--accent); padding: 0.15rem 0.5rem;
        border: 1px solid color-mix(in srgb, var(--accent) 40%, transparent); border-radius: 9999px;
        background: color-mix(in srgb, var(--accent) 10%, transparent); white-space: nowrap; }
      .nbpm-bar .spacer { flex: 1; }
      .nbpm-bar a.x { color: var(--accent); text-decoration: none; font-size: 0.85rem; }
      .nbpm-bar a.x:hover { text-decoration: underline; }
      .layout { display: flex; align-items: flex-start; max-width: 1180px; margin: 0 auto; }
      .sidebar { width: 250px; flex: 0 0 250px; position: sticky; top: 49px; align-self: flex-start;
        max-height: calc(100vh - 49px); overflow-y: auto; padding: 1.25rem 0.75rem 3rem;
        border-right: 1px solid var(--line); }
      .sidebar .nav-title { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em;
        color: var(--muted); font-weight: 700; padding: 0 0.6rem; margin-bottom: 0.4rem; }
      .nav-link { display: block; padding: 0.32rem 0.6rem; border-radius: 6px; text-decoration: none;
        color: var(--muted); font-size: 0.9rem; }
      .nav-link:hover { background: rgba(255, 255, 255, 0.05); color: var(--fg); }
      .nav-link.active { background: color-mix(in srgb, var(--accent) 16%, transparent);
        color: var(--accent); font-weight: 600; }
      .content { min-width: 0; flex: 1; padding: 1.5rem 2rem 5rem; }
      .content h1 { font-size: 1.8rem; margin: 0.2rem 0 1rem; }
      .content h2 { font-size: 1.35rem; margin: 2rem 0 0.6rem; padding-bottom: 0.3rem;
        border-bottom: 1px solid var(--line); }
      .content h3 { font-size: 1.1rem; margin: 1.4rem 0 0.4rem; }
      .content h4 { font-size: 0.98rem; margin: 1.1rem 0 0.3rem; }
      .content p, .content li { color: #d4d4d8; }
      .content a { text-decoration: none; }
      .content a:hover { text-decoration: underline; }
      .content ul, .content ol { padding-left: 1.4rem; }
      .content li { margin: 0.25rem 0; }
      .content :not(pre) > code { background: var(--code-bg); border: 1px solid var(--line);
        border-radius: 4px; padding: 0.05rem 0.32rem; }
      .content pre { background: var(--code-bg); color: #e6edf3; border: 1px solid var(--line);
        padding: 0.9rem 1rem; border-radius: 8px;
        overflow-x: auto; font-size: 0.84rem; line-height: 1.5; }
      .content pre code { background: none; border: 0; padding: 0; color: inherit; }
      .content blockquote { margin: 1rem 0; padding: 0.3rem 1rem; border-left: 3px solid var(--accent);
        background: rgba(255, 255, 255, 0.03); color: var(--muted); }
      .content table { border-collapse: collapse; width: 100%; margin: 1rem 0; font-size: 0.9rem; }
      .content th, .content td { border: 1px solid var(--line); padding: 0.4rem 0.6rem; text-align: left; vertical-align: top; }
      .content th { background: var(--card); }
      .content img { max-width: 100%; }
      .content hr { border: 0; border-top: 1px solid var(--line); margin: 2rem 0; }
      .source-note { margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--line);
        color: var(--muted); font-size: 0.82rem; }
      @media (max-width: 800px) {
        .layout { flex-direction: column; }
        .sidebar { position: static; width: 100%; max-height: none; border-right: 0;
          border-bottom: 1px solid var(--line); }
        .content { padding: 1.25rem; }
      }
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
