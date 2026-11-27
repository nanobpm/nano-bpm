// Shared markdown -> HTML docs renderer (dependency-free).
//
// One canonical implementation of the docs pipeline, used by:
//   - console/scripts/build-docs.mjs   (the gateway-embedded /docs, also
//     published to nanobpm.io/docs), and
//   - website/nano-coder-docs.mjs      (the website-only /nano-coder/docs/).
//
// It deliberately does NOT import markdown-it: each caller passes its own
// `MarkdownIt` constructor (the console from console/node_modules, the website
// from website/node_modules), so this module resolves from either package and
// the slugging, heading-id, link-rewrite and page-style rules cannot drift
// between the two doc sites.

/** HTML-escape text content. */
export const esc = (s) =>
  String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

/** HTML-escape an attribute value (adds quote escaping). */
export const escAttr = (s) => esc(s).replace(/"/g, "&quot;");

// GitHub-compatible heading slug (matches the source docs' own intra-doc
// anchors). GitHub maps each space to a hyphen WITHOUT collapsing runs, so a
// removed punctuation char sitting between spaces (e.g. the em-dash in
// "Connector — outbound workers") yields a double hyphen — replace ` ` per-char,
// not `\s+`, or such anchors silently break.
export function slugify(text) {
  return String(text)
    .toLowerCase()
    .replace(/<[^>]+>/g, "")
    .replace(/[^\w\- ]+/g, "")
    .trim()
    .replace(/ /g, "-");
}

// Resolve a relative link (optionally with `../`/`./` segments) against the
// source file's directory `base` (repo-root-relative, "" for the root) into a
// root-relative path. A `..` that climbs above the root is CLAMPED (dropped), so
// "../../x" from base "docs" resolves to "x"; callers that must detect a link
// escaping their tree need to check that separately before resolving.
export function repoRelative(base, path) {
  const baseParts = base ? base.split("/").filter(Boolean) : [];
  const out = [...baseParts];
  for (const seg of path.replace(/^\.\//, "").split("/")) {
    if (seg === "..") out.pop();
    else if (seg !== "." && seg !== "") out.push(seg);
  }
  return out.join("/");
}

/**
 * Build a markdown-it instance with the shared docs rules:
 *  - GitHub-style `id`s on every heading (so in-page anchors resolve), and
 *  - link (`href`) and image (`src`) rewriting through the caller's hooks,
 *    which receive `(target, env)` (env is the `md.render` env).
 */
export function createDocsMarkdown(MarkdownIt, { rewriteHref, rewriteSrc } = {}) {
  const md = new MarkdownIt({ html: false, linkify: true, breaks: false });

  md.renderer.rules.heading_open = (tokens, idx, options, _env, self) => {
    const inline = tokens[idx + 1];
    const text = inline && inline.type === "inline" ? inline.content : "";
    const id = slugify(text.replace(/`/g, ""));
    if (id) tokens[idx].attrSet("id", id);
    return self.renderToken(tokens, idx, options);
  };

  if (rewriteHref) {
    const defaultLinkOpen =
      md.renderer.rules.link_open ||
      ((tokens, idx, options, _env, self) =>
        self.renderToken(tokens, idx, options));
    md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
      const i = tokens[idx].attrIndex("href");
      if (i >= 0) tokens[idx].attrs[i][1] = rewriteHref(tokens[idx].attrs[i][1], env ?? {});
      return defaultLinkOpen(tokens, idx, options, env, self);
    };
  }
  if (rewriteSrc) {
    const defaultImage = md.renderer.rules.image;
    md.renderer.rules.image = (tokens, idx, options, env, self) => {
      const i = tokens[idx].attrIndex("src");
      if (i >= 0) tokens[idx].attrs[i][1] = rewriteSrc(tokens[idx].attrs[i][1], env ?? {});
      return defaultImage(tokens, idx, options, env, self);
    };
  }
  return md;
}

/** True for an absolute URL/scheme link that must never be rewritten. */
export const isExternalHref = (href) => /^(https?:|mailto:)/i.test(href);

// The shared docs page stylesheet (dark theme, sticky top bar, sidebar layout).
export const DOCS_CSS = `      :root {
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
      }`;

// Copy buttons on every code block. Self-contained, progressive enhancement:
// without JS (or without the Clipboard API) the blocks simply have no button.
export const CODE_COPY_CSS = `
      .content pre { position: relative; }
      .content pre .copy-code { position: absolute; top: 0.45rem; right: 0.45rem; font: inherit;
        font-size: 0.72rem; color: var(--muted); background: rgba(255, 255, 255, 0.06);
        border: 1px solid var(--line); border-radius: 5px; padding: 0.15rem 0.5rem; cursor: pointer; }
      .content pre .copy-code:hover { color: var(--fg); }`;

export const CODE_COPY_SCRIPT = `<script>
  (() => {
    if (!navigator.clipboard) return;
    for (const pre of document.querySelectorAll(".content pre")) {
      const code = pre.querySelector("code");
      if (!code) continue;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "copy-code";
      btn.textContent = "Copy";
      btn.setAttribute("aria-label", "Copy code");
      btn.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(code.innerText);
          btn.textContent = "Copied";
        } catch {
          btn.textContent = "Failed";
        }
        setTimeout(() => (btn.textContent = "Copy"), 1500);
      });
      pre.appendChild(btn);
    }
  })();
</script>`;
