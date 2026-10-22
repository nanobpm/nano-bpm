// The "Videos" section appears on TWO surfaces: the public nanobpm.io homepage
// (`/#videos`) and the in-app console landing page (`/`, served by the gateway
// binary). To keep them from drifting, both are rendered from ONE data source —
// website/data/videos.json — by this module. website/build.mjs renders it into
// the homepage AND into the checked-in derived artifact
// server/crates/nano-server-console/src/videos.gen.html, which the server splices
// into landing.html at the <!--VIDEOS_SECTION--> marker.
//
// With an empty `videos` list the section shows the `placeholder` text ("Coming
// soon"); adding `{ title, url, description? }` entries renders them as a list
// with no template change.

import { readFileSync } from "node:fs";

const esc = (s) =>
  String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");

/** Validate the parsed videos.json shape; throws a descriptive error on drift. */
export function validateVideos(data) {
  if (!data || typeof data !== "object") throw new Error("videos.json: must be an object");
  for (const key of ["title", "placeholder"]) {
    if (typeof data[key] !== "string" || !data[key].trim()) {
      throw new Error(`videos.json: \`${key}\` must be a non-empty string`);
    }
  }
  if (!Array.isArray(data.videos)) throw new Error("videos.json: `videos` must be an array");
  data.videos.forEach((v, i) => {
    if (!v || typeof v.title !== "string" || !v.title.trim()) {
      throw new Error(`videos.json: video ${i} needs a non-empty string \`title\``);
    }
    if (typeof v.url !== "string" || !/^https:\/\//.test(v.url)) {
      throw new Error(`videos.json: video ${i} ("${v.title}") needs an https \`url\``);
    }
    if (v.description !== undefined && typeof v.description !== "string") {
      throw new Error(`videos.json: video ${i} ("${v.title}") \`description\` must be a string`);
    }
  });
  return data;
}

/** Read + validate a videos.json file. */
export function loadVideos(path) {
  return validateVideos(JSON.parse(readFileSync(path, "utf8")));
}

/** Render the shared Videos section (identical markup on both surfaces). */
export function videosSectionHtml(data) {
  validateVideos(data);
  const body = data.videos.length
    ? `<ul class="videos-list">
${data.videos
  .map(
    (v) =>
      `    <li><a href="${esc(v.url)}" rel="noopener noreferrer" target="_blank">${esc(v.title)}</a>` +
      (v.description ? `<p>${esc(v.description)}</p>` : "") +
      `</li>`,
  )
  .join("\n")}
  </ul>`
    : `<p class="videos-soon">${esc(data.placeholder)}</p>`;
  return `<section class="videos" id="videos" aria-labelledby="videos-title">
  <h2 id="videos-title">${esc(data.title)}</h2>
  ${body}
</section>`;
}
