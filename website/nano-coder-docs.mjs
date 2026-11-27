// nano-coder usage docs, published website-only at nanobpm.io/nano-coder/docs/.
//
// Source of truth: nanobpm/nano-coder's `docs/` directory at its LATEST release
// tag (only the latest released version is published, no per-version pages).
// Rendered with the shared docs pipeline (console/scripts/docs-render.mjs), so
// slugs, heading ids and the page style are the same as /docs/ — but the pages
// are written ONLY under `<site>/nano-coder/docs/`, never into
// console/public/docs (the gateway-embedded /docs is Nano BPM's own manual).
//
// Source resolution (resolveNanoCoderDocs):
//   - live: GET /repos/nanobpm/nano-coder/releases/latest, then the tag's
//     tarball, `docs/` extracted (GITHUB_TOKEN used when present);
//   - fallback: the vendored snapshot website/data/nano-coder-docs/ (+ VERSION),
//     refreshed by `node website/scripts/update-nano-coder-docs.mjs`, with a
//     warning. In CI (CI=true) a failed fetch is fatal instead, so a broken
//     fetch is never deployed silently;
//   - a release with no `docs/` (before nano-coder#397 shipped) is not an
//     error: the section is skipped with a warning and no nav link is emitted.
//   - NANO_CODER_DOCS_OFFLINE=1 skips the network entirely (tests, offline CI
//     drift checks); NANO_CODER_DOCS_SNAPSHOT overrides the snapshot directory.

import { execFileSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

import {
  CODE_COPY_CSS,
  CODE_COPY_SCRIPT,
  createDocsMarkdown,
  DOCS_CSS,
  esc,
  escAttr,
  isExternalHref,
  repoRelative,
} from "../console/scripts/docs-render.mjs";

export const NANO_CODER_REPO = "nanobpm/nano-coder";
/** Site route the docs are published under (trailing slash). */
export const DOCS_ROUTE = "/nano-coder/docs/";

const here = dirname(fileURLToPath(import.meta.url));
export const DEFAULT_SNAPSHOT_DIR = join(here, "data", "nano-coder-docs");

// --- Front matter --------------------------------------------------------------

/** Split a `---` YAML front-matter block (flat `key: value` only) from markdown. */
export function parseFrontMatter(text) {
  const m = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/.exec(text);
  if (!m) return { data: {}, body: text };
  const data = {};
  for (const line of m[1].split(/\r?\n/)) {
    const kv = /^([A-Za-z_][\w-]*)\s*:\s*(.*)$/.exec(line);
    if (!kv) continue;
    let v = kv[2].trim();
    if (/^".*"$/.test(v)) {
      try {
        v = JSON.parse(v);
      } catch {
        v = v.slice(1, -1);
      }
    } else if (/^'.*'$/.test(v)) v = v.slice(1, -1).replace(/''/g, "'");
    else if (/^-?\d+(\.\d+)?$/.test(v)) v = Number(v);
    data[kv[1]] = v;
  }
  return { data, body: text.slice(m[0].length) };
}

// --- Source tree ---------------------------------------------------------------

function walk(dir, base = dir) {
  const out = [];
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) out.push(...walk(p, base));
    else if (e.isFile()) out.push(relative(base, p).split("\\").join("/"));
  }
  return out.sort();
}

/** Page route for a docs-relative `.md` path (`index.md` -> the docs root). */
export function routeFor(mdPath) {
  const noExt = mdPath.replace(/\.md$/i, "");
  if (noExt === "index") return DOCS_ROUTE;
  if (noExt.endsWith("/index")) return `${DOCS_ROUTE}${noExt.slice(0, -"index".length)}`;
  return `${DOCS_ROUTE}${noExt}/`;
}

/**
 * Load the docs tree: one page per `.md` file, ordered and grouped by front
 * matter (`section`, `order`, `title`, `description`). Sections are ordered by
 * their lowest page `order`; pages within a section by `order`, then title.
 */
export function loadDocsTree(docsDir) {
  const files = walk(docsDir);
  const pages = files
    .filter((f) => /\.md$/i.test(f))
    .map((path) => {
      const { data, body } = parseFrontMatter(readFileSync(join(docsDir, path), "utf8"));
      const h1 = /^# (.+)$/m.exec(body);
      return {
        path,
        route: routeFor(path),
        title: String(data.title ?? (h1 ? h1[1].trim() : path.replace(/\.md$/i, ""))),
        description: data.description == null ? "" : String(data.description),
        order: typeof data.order === "number" ? data.order : Number.MAX_SAFE_INTEGER,
        section: data.section == null ? "" : String(data.section),
        body,
      };
    });
  const byOrder = (a, b) => a.order - b.order || a.title.localeCompare(b.title);
  pages.sort(byOrder);
  const sections = [];
  for (const p of pages) {
    let s = sections.find((x) => x.name === p.section);
    if (!s) sections.push((s = { name: p.section, pages: [] }));
    s.pages.push(p);
  }
  // The snapshot's own VERSION marker is metadata, not a published asset.
  const assets = files.filter((f) => !/\.md$/i.test(f) && !/(^|\/)\./.test(f) && f !== "VERSION");
  return { pages, sections, assets };
}

// --- Link rewriting --------------------------------------------------------------

/**
 * Rewrite a link/image target found in `fromPath` (docs-relative) for the site:
 *  - `x.md#a` (inside docs/)          -> `/nano-coder/docs/x/#a`
 *  - other files inside docs/ (images) -> `/nano-coder/docs/<path>`
 *  - anything outside docs/            -> GitHub at the release tag
 *    (`tree/` for a directory-looking target, `blob/` otherwise)
 *  - external URLs, `#anchor` and site-absolute `/…` links are left untouched.
 */
export function rewriteDocsTarget(href, fromPath, tag) {
  if (!href || isExternalHref(href) || href.startsWith("#") || href.startsWith("/")) return href;
  if (/^[a-z][a-z0-9+.-]*:/i.test(href)) return href;
  const hashAt = href.indexOf("#");
  const path = hashAt >= 0 ? href.slice(0, hashAt) : href;
  const frag = hashAt >= 0 ? href.slice(hashAt) : "";
  // Resolve against the page's directory inside the repo (`docs/<dir>`).
  const fromDir = ["docs", ...fromPath.split("/").slice(0, -1)].join("/");
  const repoPath = repoRelative(fromDir, path);
  if (escapesRoot(fromDir, path)) return href; // climbs above the repo — leave as written
  if (repoPath === "docs" || repoPath.startsWith("docs/")) {
    const inDocs = repoPath.slice("docs/".length);
    if (inDocs === "" || /\.md$/i.test(inDocs)) return `${routeFor(inDocs || "index.md")}${frag}`;
    return `${DOCS_ROUTE}${inDocs}${frag}`;
  }
  const kind = path.endsWith("/") || repoPath === "" ? "tree" : "blob";
  return `https://github.com/${NANO_CODER_REPO}/${kind}/${tag}/${repoPath}${frag}`;
}

function escapesRoot(base, path) {
  let depth = base.split("/").filter(Boolean).length;
  for (const seg of path.replace(/^\.\//, "").split("/")) {
    if (seg === "..") {
      depth -= 1;
      if (depth < 0) return true;
    } else if (seg !== "." && seg !== "") depth += 1;
  }
  return false;
}

// --- Rendering -------------------------------------------------------------------

// Strip HTML comments (generated-block markers, contributor notes) from prose
// before rendering: the shared renderer runs with `html: false`, so a comment
// would otherwise be published as visible text. Code regions are left verbatim,
// so a literal `<!-- -->` in an example survives. Block code (fenced or
// indented, at any nesting — blockquotes, lists) is located by the renderer's
// own parser (`md`), not by a hand-rolled line scanner, so every shape the
// renderer treats as code is protected. Inline code spans, including ones that
// wrap across lines, are skipped by the prose scanner.
export function stripDocComments(body, md) {
  const lines = body.split("\n");
  const code = new Array(lines.length).fill(false);
  for (const t of md.parse(body, {})) {
    if ((t.type === "fence" || t.type === "code_block") && t.map) {
      for (let l = t.map[0]; l < t.map[1] && l < lines.length; l++) code[l] = true;
    }
  }
  const out = [];
  let inComment = false;
  let l = 0;
  while (l < lines.length) {
    if (code[l]) { out.push(lines[l]); l++; continue; }
    // A run of prose lines, scanned as one string so code spans and comments
    // may wrap across lines; results are split back per source line.
    let e = l;
    while (e < lines.length && !code[e]) e++;
    const src = lines.slice(l, e).join("\n");
    let res = "";
    let i = 0;
    while (i < src.length) {
      if (inComment) {
        const end = src.indexOf("-->", i);
        // Keep the newlines a comment swallows, so the line mapping holds.
        const stop = end < 0 ? src.length : end + 3;
        res += src.slice(i, stop).replace(/[^\n]/g, "");
        if (end >= 0) inComment = false;
        i = stop;
        continue;
      }
      if (src[i] === "`") {
        let n = 1;
        while (src[i + n] === "`") n++;
        const tick = "`".repeat(n);
        // A code span ends at the next run of exactly n backticks within the
        // same paragraph (a blank line ends a paragraph).
        const para = src.slice(i + n).search(/\n[ \t]*\n/);
        const limit = para < 0 ? src.length : i + n + para;
        let j = src.indexOf(tick, i + n);
        while (j >= 0 && j < limit && src[j + n] === "`") {
          let k = j;
          while (src[k] === "`") k++;
          j = src.indexOf(tick, k);
        }
        if (j >= 0 && j < limit) { res += src.slice(i, j + n); i = j + n; continue; }
        res += tick; i += n; continue;
      }
      if (src.startsWith("<!--", i)) { inComment = true; i += 4; continue; }
      res += src[i]; i++;
    }
    const resLines = res.split("\n");
    for (let k = 0; k < resLines.length; k++) {
      // Drop lines that held only a comment, so markers don't leave blank gaps.
      if (resLines[k].trim() === "" && lines[l + k].trim() !== "") continue;
      out.push(resLines[k]);
    }
    l = e;
  }
  return out.join("\n");
}

/** H2/H3 headings of a rendered token stream, for the on-page TOC. */
function tocFromTokens(tokens) {
  const toc = [];
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t.type !== "heading_open" || (t.tag !== "h2" && t.tag !== "h3")) continue;
    const inline = tokens[i + 1];
    if (!inline || inline.type !== "inline") continue;
    // The unique id the shared renderer's core rule assigned at parse time.
    const text = inline.content.replace(/`/g, "");
    const id = t.attrGet("id");
    if (id) toc.push({ level: t.tag === "h2" ? 2 : 3, id, text });
  }
  return toc;
}

const LAYOUT_CSS = `
      .nbpm-bar .x.current { font-weight: 600; }
      .version { margin: 0 0 1.2rem; padding: 0.45rem 0.8rem; border: 1px solid var(--line);
        border-radius: 8px; background: var(--card); color: var(--muted); font-size: 0.85rem; }
      .sidebar .nav-section { margin-top: 1rem; }
      .layout { max-width: 1380px; }
      .toc { width: 220px; flex: 0 0 220px; position: sticky; top: 49px; align-self: flex-start;
        max-height: calc(100vh - 49px); overflow-y: auto; padding: 1.5rem 0.75rem; font-size: 0.82rem; }
      .toc .nav-title { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em;
        color: var(--muted); font-weight: 700; margin-bottom: 0.4rem; }
      .toc a { display: block; color: var(--muted); text-decoration: none; padding: 0.15rem 0; }
      .toc a:hover { color: var(--fg); }
      .toc a.l3 { padding-left: 0.8rem; }
      .content h2 .anchor, .content h3 .anchor { margin-left: 0.4rem; color: var(--muted);
        opacity: 0; text-decoration: none; font-weight: 400; }
      .content h2:hover .anchor, .content h3:hover .anchor,
      .content .anchor:focus, .content .anchor:focus-visible { opacity: 1; }
      @media (max-width: 1100px) { .toc { display: none; } }`;

function sidebarHtml(sections, activeRoute) {
  return sections
    .map((s) => {
      const links = s.pages
        .map((p) => {
          const cls = p.route === activeRoute ? "nav-link active" : "nav-link";
          return `<a class="${cls}" href="${escAttr(p.route)}">${esc(p.title)}</a>`;
        })
        .join("\n        ");
      const title = s.name ? `<div class="nav-title">${esc(s.name)}</div>\n        ` : "";
      return `<div class="nav-section">\n        ${title}${links}\n      </div>`;
    })
    .join("\n      ");
}

function pageHtml(page, ctx, md) {
  const env = { fromPath: page.path };
  const tokens = md.parse(stripDocComments(page.body, md), env);
  const toc = tocFromTokens(tokens);
  // Visible "#" anchor on each H2/H3 (heading ids are set by the shared renderer).
  let body = md.renderer.render(tokens, md.options, env);
  body = body.replace(/<(h[23]) id="([^"]+)">([\s\S]*?)<\/\1>/g, (_m, tag, id, inner) =>
    `<${tag} id="${id}">${inner}<a class="anchor" href="#${id}" aria-label="Link to this section">#</a></${tag}>`);
  const tocHtml = toc.length
    ? `<aside class="toc"><div class="nav-title">On this page</div>
        ${toc.map((h) => `<a class="l${h.level}" href="#${escAttr(h.id)}">${esc(h.text)}</a>`).join("\n        ")}
      </aside>`
    : "";
  const desc = page.description || `nano-coder documentation: ${page.title}.`;
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${esc(page.title)} · nano-coder docs</title>
    <meta name="description" content="${escAttr(desc)}" />
    <link rel="canonical" href="https://nanobpm.io${escAttr(page.route)}" />
    <style>${DOCS_CSS}${CODE_COPY_CSS}${LAYOUT_CSS}
    </style>
  </head>
  <body>
    <div class="nbpm-bar">
      <a class="brand" href="/"><span class="dot"></span><strong>nanobpm.io</strong></a>
      <span class="sub">nano-coder docs</span>
      <span class="spacer"></span>
      <a class="x" href="/nano-coder/">nano-coder</a>
      <a class="x current" href="${DOCS_ROUTE}">Docs</a>
      <a class="x" href="https://github.com/${NANO_CODER_REPO}" rel="noopener noreferrer">GitHub</a>
    </div>
    <div class="layout">
      <nav class="sidebar" aria-label="nano-coder docs">
      ${sidebarHtml(ctx.sections, page.route)}
      </nav>
      <main class="content">
        <p class="version" data-nano-coder-version="${escAttr(ctx.version)}">Docs for nano-coder ${esc(ctx.version)} · <a href="https://github.com/${NANO_CODER_REPO}/releases/tag/${escAttr(ctx.tag)}">release notes</a></p>
        ${body}
        <p class="source-note">Source: <a href="https://github.com/${NANO_CODER_REPO}/blob/${escAttr(ctx.tag)}/docs/${escAttr(page.path)}">docs/${esc(page.path)}</a> at ${esc(ctx.tag)}.</p>
      </main>
      ${tocHtml}
    </div>
    ${CODE_COPY_SCRIPT}
  </body>
</html>`;
}

/**
 * Render a nano-coder `docs/` tree into `<siteOut>/nano-coder/docs/`.
 * `version` is the release tag (e.g. `v1.37.0`). Returns the rendered pages.
 */
export function renderNanoCoderDocs({ MarkdownIt, docsDir, version, siteOut }) {
  const tag = version;
  const tree = loadDocsTree(docsDir);
  if (tree.pages.length === 0) return [];
  const md = createDocsMarkdown(MarkdownIt, {
    rewriteHref: (href, env) => rewriteDocsTarget(href, env.fromPath ?? "index.md", tag),
    rewriteSrc: (src, env) => rewriteDocsTarget(src, env.fromPath ?? "index.md", tag),
  });
  const ctx = { sections: tree.sections, version, tag };
  const outRoot = join(siteOut, ...DOCS_ROUTE.split("/").filter(Boolean));
  for (const page of tree.pages) {
    const rel = page.route.slice(DOCS_ROUTE.length);
    const file = join(outRoot, ...rel.split("/").filter(Boolean), "index.html");
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, pageHtml(page, ctx, md));
  }
  for (const asset of tree.assets) {
    const dest = join(outRoot, ...asset.split("/"));
    mkdirSync(dirname(dest), { recursive: true });
    copyFileSync(join(docsDir, asset), dest);
  }
  return tree.pages;
}

// --- Source resolution -----------------------------------------------------------

function ghHeaders(env) {
  const h = { Accept: "application/vnd.github+json", "User-Agent": "nanobpm-website-build" };
  if (env.GITHUB_TOKEN) h.Authorization = `Bearer ${env.GITHUB_TOKEN}`;
  return h;
}

/**
 * Fetch `docs/` of nano-coder's latest release into a fresh temp dir.
 * Resolves `{ version, docsDir }` (`docsDir` null when the tag has no docs/);
 * rejects on any network/API failure.
 */
export async function fetchLatestDocs({ env = process.env, fetchImpl = fetch } = {}) {
  const rel = await fetchImpl(`https://api.github.com/repos/${NANO_CODER_REPO}/releases/latest`, {
    headers: ghHeaders(env),
  });
  if (!rel.ok) throw new Error(`releases/latest: HTTP ${rel.status}`);
  const { tag_name: version } = await rel.json();
  if (!/^v\d/.test(version ?? "")) throw new Error(`unexpected latest release tag ${JSON.stringify(version)}`);
  const tar = await fetchImpl(`https://api.github.com/repos/${NANO_CODER_REPO}/tarball/${version}`, {
    headers: ghHeaders(env),
  });
  if (!tar.ok) throw new Error(`tarball ${version}: HTTP ${tar.status}`);
  const work = mkdtempSync(join(tmpdir(), "nano-coder-docs-"));
  const src = join(work, "src");
  try {
    const tgz = join(work, "src.tgz");
    writeFileSync(tgz, Buffer.from(await tar.arrayBuffer()));
    mkdirSync(src);
    execFileSync("tar", ["-xzf", tgz, "-C", src, "--strip-components=1"], { stdio: "pipe" });
  } catch (err) {
    rmSync(work, { recursive: true, force: true }); // never leak the temp dir on a failed fetch/extract
    throw err;
  }
  const docsDir = join(src, "docs");
  return { version, docsDir: existsSync(docsDir) ? docsDir : null, cleanup: () => rmSync(work, { recursive: true, force: true }) };
}

/** Read the vendored snapshot, or null when there is none. */
export function readSnapshot(dir = DEFAULT_SNAPSHOT_DIR) {
  const vf = join(dir, "VERSION");
  if (!existsSync(vf)) return null;
  const version = readFileSync(vf, "utf8").trim();
  return version ? { version, docsDir: dir } : null;
}

/**
 * Decide where this build's nano-coder docs come from.
 * Returns `{ version, docsDir, source, cleanup? }`, or `null` to skip the
 * section. Throws only in CI when the live fetch fails.
 */
export async function resolveNanoCoderDocs({ env = process.env, fetchImpl = fetch, warn = console.warn } = {}) {
  const snapshotDir = env.NANO_CODER_DOCS_SNAPSHOT || DEFAULT_SNAPSHOT_DIR;
  const fromSnapshot = (why) => {
    const snap = readSnapshot(snapshotDir);
    if (snap) {
      warn(`warning: nano-coder docs: ${why}; using vendored snapshot ${snap.version}`);
      return { ...snap, source: "snapshot" };
    }
    warn(`warning: nano-coder docs: ${why}, and no vendored snapshot — /nano-coder/docs/ omitted`);
    return null;
  };
  if (env.NANO_CODER_DOCS_OFFLINE === "1") return fromSnapshot("offline build (NANO_CODER_DOCS_OFFLINE=1)");
  let got;
  try {
    got = await fetchLatestDocs({ env, fetchImpl });
  } catch (e) {
    if (env.CI === "true") {
      throw new Error(`nano-coder docs: live fetch failed in CI (${e.message}); refusing to deploy a stale or missing /nano-coder/docs/`);
    }
    return fromSnapshot(`live fetch failed (${e.message})`);
  }
  if (!got.docsDir) {
    got.cleanup();
    warn(`warning: nano-coder docs: latest release ${got.version} has no docs/ — /nano-coder/docs/ omitted`);
    return null;
  }
  return { version: got.version, docsDir: got.docsDir, source: "live", cleanup: got.cleanup };
}
