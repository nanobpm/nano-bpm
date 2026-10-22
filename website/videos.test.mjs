import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { loadVideos, validateVideos, videosSectionHtml } from "./videos.mjs";

const DATA = fileURLToPath(new URL("./data/videos.json", import.meta.url));

test("an empty videos list renders the Coming soon placeholder", () => {
  const html = videosSectionHtml({ title: "Videos", placeholder: "Coming soon", videos: [] });
  assert.match(html, /<section class="videos" id="videos"/);
  assert.match(html, /<h2 id="videos-title">Videos<\/h2>/);
  assert.match(html, /<p class="videos-soon">Coming soon<\/p>/);
  assert.doesNotMatch(html, /videos-list/);
});

test("a non-empty videos list renders its entries (escaped) instead of the placeholder", () => {
  const html = videosSectionHtml({
    title: "Videos",
    placeholder: "Coming soon",
    videos: [
      { title: "Intro <1>", url: "https://example.com/a?x=1&y=2", description: "First & best" },
      { title: "Second", url: "https://example.com/b" },
    ],
  });
  assert.doesNotMatch(html, /Coming soon/);
  assert.match(html, /<ul class="videos-list">/);
  assert.match(html, /<a href="https:\/\/example\.com\/a\?x=1&amp;y=2"[^>]*>Intro &lt;1&gt;<\/a><p>First &amp; best<\/p>/);
  assert.match(html, /<a href="https:\/\/example\.com\/b"[^>]*>Second<\/a><\/li>/);
});

test("invalid data is rejected", () => {
  assert.throws(() => validateVideos({ title: "Videos", placeholder: "x" }), /`videos` must be an array/);
  assert.throws(() => validateVideos({ title: "", placeholder: "x", videos: [] }), /`title`/);
  assert.throws(
    () => validateVideos({ title: "V", placeholder: "x", videos: [{ title: "t", url: "javascript:alert(1)" }] }),
    /https `url`/,
  );
});

test("the built homepage and console artifact both carry the shared Videos section", () => {
  execFileSync(process.execPath, [fileURLToPath(new URL("./build.mjs", import.meta.url))], { stdio: "pipe" });
  const section = videosSectionHtml(loadVideos(DATA));
  const home = readFileSync(new URL("./_site/index.html", import.meta.url), "utf8");
  assert.ok(home.includes(section), "homepage embeds the shared section");
  assert.match(home, /id="videos"[\s\S]*?Coming soon/);
  assert.match(home, /<header class="nav">[\s\S]*?<a href="\/#videos">Videos<\/a>[\s\S]*?<\/header>/);
  const gen = readFileSync(
    new URL("../server/crates/nano-server-console/src/videos.gen.html", import.meta.url),
    "utf8",
  );
  assert.ok(gen.includes(section), "console artifact embeds the same section");
});
