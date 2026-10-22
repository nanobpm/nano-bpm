// Atomically publish a file: write to a unique temp file in the destination
// directory, then rename(2) it over the target. Readers therefore observe
// either the old or the new complete contents — never a truncated file —
// even when several builds publish the same shared artifact concurrently
// (e.g. parallel `node --test` files each running build.mjs).
import { renameSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { randomBytes } from "node:crypto";

export function writeFileAtomic(path, contents) {
  const tmp = join(
    dirname(path),
    `.${basename(path)}.${process.pid}.${randomBytes(6).toString("hex")}.tmp`,
  );
  try {
    writeFileSync(tmp, contents);
    renameSync(tmp, path);
  } catch (err) {
    rmSync(tmp, { force: true });
    throw err;
  }
}
