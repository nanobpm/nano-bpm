import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Worker } from "node:worker_threads";
import { writeFileAtomic } from "./atomic-write.mjs";

test("concurrent publishers never expose a partial file to readers", async () => {
  const dir = mkdtempSync(join(tmpdir(), "atomic-write-"));
  try {
    const target = join(dir, "artifact.html");
    const body = "x".repeat(1 << 20);
    writeFileAtomic(target, body);
    const url = new URL("./atomic-write.mjs", import.meta.url).href;
    const workers = [0, 1].map(
      () =>
        new Worker(
          `const { writeFileAtomic } = await import(${JSON.stringify(url)});
           for (let i = 0; i < 50; i++) writeFileAtomic(${JSON.stringify(target)}, ${JSON.stringify(body)});`,
          { eval: true, type: "module" },
        ),
    );
    const done = Promise.all(workers.map((w) => new Promise((ok, ko) => { w.on("exit", ok); w.on("error", ko); })));
    let finished = false;
    done.then(() => (finished = true));
    while (!finished) {
      assert.equal(readFileSync(target, "utf8").length, body.length);
      await new Promise((r) => setImmediate(r));
    }
    await done;
    assert.deepEqual(readdirSync(dir), ["artifact.html"], "no temp files leak");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
