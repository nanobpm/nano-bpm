// Refresh the vendored nano-coder docs snapshot (website/data/nano-coder-docs/)
// from nanobpm/nano-coder's latest release. The snapshot is the offline /
// fetch-failure fallback for website/build.mjs (see website/nano-coder-docs.mjs);
// CI deploys always use the live fetch.
//
//   node website/scripts/update-nano-coder-docs.mjs
//
// Uses GITHUB_TOKEN when set. If the latest release has no docs/, the snapshot
// is removed (a stale snapshot would publish docs for an older release).
import { cpSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { DEFAULT_SNAPSHOT_DIR, fetchLatestDocs } from "../nano-coder-docs.mjs";

const got = await fetchLatestDocs();
try {
  rmSync(DEFAULT_SNAPSHOT_DIR, { recursive: true, force: true });
  if (!got.docsDir) {
    console.log(`nano-coder ${got.version} has no docs/ — snapshot removed`);
  } else {
    mkdirSync(DEFAULT_SNAPSHOT_DIR, { recursive: true });
    cpSync(got.docsDir, DEFAULT_SNAPSHOT_DIR, { recursive: true });
    writeFileSync(join(DEFAULT_SNAPSHOT_DIR, "VERSION"), `${got.version}\n`);
    console.log(`nano-coder docs snapshot -> ${got.version} (${DEFAULT_SNAPSHOT_DIR})`);
  }
} finally {
  got.cleanup();
}
