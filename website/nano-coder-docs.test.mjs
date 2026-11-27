// /nano-coder/docs/: nano-coder's release docs/ rendered website-only
// (website/nano-coder-docs.mjs). Renders a fixture docs/ tree through the real
// site build, plus unit checks of the source-resolution fallback rules.
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { buildSite } from "./test-build.mjs";
import { fetchLatestDocs, parseFrontMatter, resolveNanoCoderDocs, rewriteDocsTarget, routeFor } from "./nano-coder-docs.mjs";

const FIXTURE = fileURLToPath(new URL("./test-fixtures/nano-coder-docs/", import.meta.url));
const repoRoot = fileURLToPath(new URL("../", import.meta.url));

const site = buildSite({ NANO_CODER_DOCS_SNAPSHOT: FIXTURE });
const page = (route) => site(join(route, "index.html"));

test("every fixture page is published under /nano-coder/docs/", () => {
  for (const r of ["nano-coder/docs/", "nano-coder/docs/cli/", "nano-coder/docs/tools/", "nano-coder/docs/guides/deep/"]) {
    assert.ok(existsSync(join(site.dir, r, "index.html")), `${r} missing`);
  }
});

test("sidebar is grouped by front-matter section and ordered by `order`", () => {
  const html = page("nano-coder/docs/tools/");
  const nav = html.slice(html.indexOf('<nav class="sidebar"'), html.indexOf("</nav>"));
  const seq = [...nav.matchAll(/nav-title">([^<]+)<|nav-link[^"]*" href="([^"]+)"/g)].map((m) => m[1] ?? m[2]);
  assert.deepEqual(seq, [
    "Get started",
    "/nano-coder/docs/",
    "/nano-coder/docs/cli/",
    "Using nano-coder",
    "/nano-coder/docs/tools/",
    "/nano-coder/docs/guides/deep/",
  ]);
  assert.match(nav, /nav-link active" href="\/nano-coder\/docs\/tools\/"/);
});

test("links: .md -> route (anchors kept), outside docs/ -> GitHub at the tag", () => {
  const home = page("nano-coder/docs/");
  assert.match(home, /href="\/nano-coder\/docs\/tools\/#web-tools"/);
  assert.match(home, /href="\/nano-coder\/docs\/guides\/deep\/"/);
  assert.match(home, /href="https:\/\/github\.com\/nanobpm\/nano-coder\/blob\/v1\.2\.3\/CHANGELOG\.md#v1"/);
  assert.match(home, /href="https:\/\/github\.com\/nanobpm\/nano-coder\/tree\/v1\.2\.3\/src"/);
  assert.match(page("nano-coder/docs/guides/deep/"), /href="\/nano-coder\/docs\/tools\/#fetch"/);
  const tools = page("nano-coder/docs/tools/");
  assert.match(tools, /href="\/nano-coder\/docs\/"/);
  assert.match(tools, /href="#web-tools"/);
  assert.doesNotMatch(home + tools, /href="(?!https:)[^"]*\.md[#"]/, "no unrewritten relative .md link");
});

test("every internal link and anchor in the rendered docs resolves", () => {
  const root = join(site.dir, "nano-coder", "docs");
  const files = [];
  (function walk(d) {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      const p = join(d, e.name);
      if (e.isDirectory()) walk(p);
      else if (e.name.endsWith(".html")) files.push(p);
    }
  })(root);
  for (const f of files) {
    const html = readFileSync(f, "utf8");
    for (const [, href] of html.matchAll(/(?:href|src)="(\/nano-coder\/docs\/[^"]*|#[^"]+)"/g)) {
      const [path, anchor] = href.split("#");
      let target = f;
      if (path) target = join(site.dir, path, path.endsWith("/") ? "index.html" : "");
      assert.ok(existsSync(target), `${f}: ${href} -> missing ${target}`);
      if (anchor) assert.ok(readFileSync(target, "utf8").includes(`id="${anchor}"`), `${f}: dangling anchor ${href}`);
    }
  }
});

test("images under docs/ are copied alongside the pages", () => {
  assert.match(page("nano-coder/docs/"), /src="\/nano-coder\/docs\/img\/diagram\.svg"/);
  assert.ok(existsSync(join(site.dir, "nano-coder", "docs", "img", "diagram.svg")));
  assert.ok(!existsSync(join(site.dir, "nano-coder", "docs", "VERSION")), "snapshot VERSION marker must not be published");
});

test("version banner, SEO tags, TOC, heading anchors and copy buttons", () => {
  const tools = page("nano-coder/docs/tools/");
  assert.match(tools, /Docs for nano-coder v1\.2\.3/);
  assert.match(tools, /<title>Built-in tools · nano-coder docs<\/title>/);
  assert.match(tools, /<meta name="description" content="The tools the agent can call\." \/>/);
  assert.match(tools, /<aside class="toc">[\s\S]*href="#web-tools"[\s\S]*class="l3" href="#fetch"/);
  assert.match(tools, /<h2 id="web-tools">Web tools<a class="anchor" href="#web-tools"/);
  assert.match(page("nano-coder/docs/"), /<pre><code class="language-sh">/);
  // Keyboard users must see the permalink they tab to.
  assert.match(tools, /\.anchor:focus-visible[^{]*\{ opacity: 1; \}/);

  assert.match(tools, /className = "copy-code"/);
  for (const r of ["nano-coder/docs/cli/", "nano-coder/docs/guides/deep/"]) assert.match(page(r), /Docs for nano-coder v1\.2\.3/);
});

test("site nav and the /nano-coder/ marketing page link the docs", () => {
  assert.match(site("index.html"), /<a href="\/nano-coder\/docs\/">nano-coder docs<\/a>/);
  assert.match(site("nano-coder/index.html"), /href="\/nano-coder\/docs\/">Docs →<\/a>/);
});

test("no docs (offline, no snapshot): section skipped, no nav link", () => {
  const empty = mkdtempSync(join(tmpdir(), "nano-coder-nosnap-"));
  try {
    const bare = buildSite({ NANO_CODER_DOCS_SNAPSHOT: empty });
    assert.ok(!existsSync(join(bare.dir, "nano-coder", "docs")));
    assert.doesNotMatch(bare("index.html"), /nano-coder docs/);
    assert.doesNotMatch(bare("nano-coder/index.html"), /Docs →/);
  } finally {
    rmSync(empty, { recursive: true, force: true });
  }
});

test("no nano-coder page ends up in the gateway-embedded console/public/docs", () => {
  const dir = join(repoRoot, "console", "public", "docs");
  if (!existsSync(dir)) return;
  for (const f of readdirSync(dir, { recursive: true })) {
    if (!String(f).endsWith(".html")) continue;
    assert.doesNotMatch(readFileSync(join(dir, String(f)), "utf8"), /Docs for nano-coder/, `${f} is a nano-coder docs page`);
  }
});

// --- resolution / fallback rules ------------------------------------------------

const failingFetch = async () => {
  throw new Error("network down");
};

test("fetch failure with a snapshot present: builds from snapshot and warns", async () => {
  const warnings = [];
  const got = await resolveNanoCoderDocs({
    env: { NANO_CODER_DOCS_SNAPSHOT: FIXTURE },
    fetchImpl: failingFetch,
    warn: (m) => warnings.push(m),
  });
  assert.equal(got.version, "v1.2.3");
  assert.equal(got.source, "snapshot");
  assert.match(warnings.join("\n"), /live fetch failed \(network down\).*snapshot v1\.2\.3/);
});

test("fetch failure in CI is fatal", async () => {
  await assert.rejects(
    resolveNanoCoderDocs({ env: { CI: "true", NANO_CODER_DOCS_SNAPSHOT: FIXTURE }, fetchImpl: failingFetch, warn: () => {} }),
    /live fetch failed in CI/,
  );
});

test("latest release without docs/ is skipped (not an error, even in CI)", async () => {
  const { execFileSync } = await import("node:child_process");
  const work = mkdtempSync(join(tmpdir(), "nano-coder-tar-"));
  try {
    // A release tarball with a top-level dir but no docs/.
    execFileSync("mkdir", ["-p", join(work, "nano-coder-abc", "src")]);
    execFileSync("tar", ["-czf", join(work, "t.tgz"), "-C", work, "nano-coder-abc"]);
    const tgz = readFileSync(join(work, "t.tgz"));
    const fetchImpl = async (url) =>
      url.endsWith("/releases/latest")
        ? { ok: true, json: async () => ({ tag_name: "v1.0.0" }) }
        : { ok: true, arrayBuffer: async () => tgz.buffer.slice(tgz.byteOffset, tgz.byteOffset + tgz.byteLength) };
    const warnings = [];
    const got = await resolveNanoCoderDocs({ env: { CI: "true" }, fetchImpl, warn: (m) => warnings.push(m) });
    assert.equal(got, null);
    assert.match(warnings.join("\n"), /v1\.0\.0 has no docs\//);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
});

test("unit: front matter, routes and link rewriting", () => {
  const { data, body } = parseFrontMatter('---\ntitle: "A: b"\norder: 3\nsection: \'S\'\n---\n# H\n');
  assert.deepEqual(data, { title: "A: b", order: 3, section: "S" });
  assert.equal(body, "# H\n");
  assert.equal(routeFor("index.md"), "/nano-coder/docs/");
  assert.equal(routeFor("a/index.md"), "/nano-coder/docs/a/");
  assert.equal(rewriteDocsTarget("https://x.dev/a.md", "index.md", "v1"), "https://x.dev/a.md");
  assert.equal(rewriteDocsTarget("../README.md", "index.md", "v1"), "https://github.com/nanobpm/nano-coder/blob/v1/README.md");
  assert.equal(rewriteDocsTarget("../../x.md", "index.md", "v1"), "../../x.md");
});

test("website markdown-it is pinned to the console's (one renderer, one version)", () => {
  const spec = (p) => JSON.parse(readFileSync(join(repoRoot, p), "utf8")).dependencies["markdown-it"];
  assert.equal(spec("website/package.json"), spec("console/package.json"));
  const locked = (p) => JSON.parse(readFileSync(join(repoRoot, p), "utf8")).packages["node_modules/markdown-it"].version;
  assert.equal(locked("website/package-lock.json"), locked("console/package-lock.json"));
});

test("fetchLatestDocs removes its temp dir when the tarball fails to extract", async () => {
  const before = new Set(readdirSync(tmpdir()).filter((n) => n.startsWith("nano-coder-docs-")));
  const fetchImpl = async (url) =>
    url.endsWith("/releases/latest")
      ? { ok: true, json: async () => ({ tag_name: "v9.9.9" }) }
      : { ok: true, arrayBuffer: async () => new TextEncoder().encode("not a tarball").buffer };
  await assert.rejects(fetchLatestDocs({ env: {}, fetchImpl }));
  const leaked = readdirSync(tmpdir()).filter((n) => n.startsWith("nano-coder-docs-") && !before.has(n));
  assert.deepEqual(leaked, []);
});

test("colliding headings get unique ids across levels, and the TOC targets them", () => {
  const tools = page("nano-coder/docs/tools/");
  const ids = [...tools.matchAll(/<h[1-6] id="([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(ids, ["built-in-tools", "web-tools", "fetch", "built-in-tools-1", "fetch-1"]);
  const toc = tools.match(/<aside class="toc">([\s\S]*?)<\/aside>/)[1];
  const tocIds = [...toc.matchAll(/href="#([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(tocIds, ["web-tools", "fetch", "built-in-tools-1", "fetch-1"]);
});
