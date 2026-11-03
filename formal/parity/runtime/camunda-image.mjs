#!/usr/bin/env node
// Resolve the Camunda 8 container image for the live parity differential
// (#1408) FROM the Zeebe pin, so the runtime we diff against can never drift
// from the sources the parity matrix was extracted from.
//
// Single source of truth: `formal/parity/zeebe-surface.json` → `zeebe.ref`
// (e.g. `stable/8.10`). That names a minor line, not an image tag, and Docker
// Hub has no floating `8.10` tag, so we pick the highest released `8.10.N`
// patch (no `-SNAPSHOT`, `-rc`, `-alpha`).
//
// CLI: `node camunda-image.mjs` prints `camunda/camunda:<tag>`.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export const IMAGE_REPO = "camunda/camunda";

const surfacePath = fileURLToPath(new URL("../zeebe-surface.json", import.meta.url));

/** `stable/8.10` → `8.10`. Throws on any ref that does not name a minor line. */
export function minorFromRef(ref) {
  const m = /^stable\/(\d+\.\d+)$/.exec(ref ?? "");
  if (!m) {
    throw new Error(
      `cannot derive a Camunda image line from Zeebe ref ${JSON.stringify(ref)} ` +
        "(expected stable/<major>.<minor>)",
    );
  }
  return m[1];
}

/** Highest released `<minor>.N` tag in `tags`; throws when there is none. */
export function latestPatch(minor, tags) {
  const re = new RegExp(`^${minor.replace(".", "\\.")}\\.(\\d+)$`);
  let best = null;
  for (const tag of tags) {
    const m = re.exec(tag);
    if (m && (best === null || Number(m[1]) > best)) best = Number(m[1]);
  }
  if (best === null) throw new Error(`no released ${IMAGE_REPO}:${minor}.N tag found`);
  return `${minor}.${best}`;
}

async function releasedTags(minor) {
  const tags = [];
  let url =
    `https://hub.docker.com/v2/repositories/${IMAGE_REPO}/tags` +
    `?page_size=100&name=${encodeURIComponent(`${minor}.`)}`;
  while (url) {
    const res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
    if (!res.ok) throw new Error(`Docker Hub tag lookup failed: HTTP ${res.status}`);
    const page = await res.json();
    tags.push(...page.results.map((r) => r.name));
    url = page.next;
  }
  return tags;
}

export async function resolveImage() {
  const surface = JSON.parse(readFileSync(surfacePath, "utf8"));
  const minor = minorFromRef(surface.zeebe?.ref);
  return `${IMAGE_REPO}:${latestPatch(minor, await releasedTags(minor))}`;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  resolveImage().then(
    (image) => console.log(image),
    (err) => {
      console.error(err.message);
      process.exit(1);
    },
  );
}
