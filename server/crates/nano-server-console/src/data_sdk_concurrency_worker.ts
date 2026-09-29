// Web Worker for the data SDK cross-connection lock test (data_sdk_test.ts,
// #1287). Each worker is a separate isolate, so it opens its OWN SQLite
// connection through the SDK — exactly like two concurrent data-op processes
// (e.g. a webhook enqueue racing an inbox poll) hitting one project DB.
//
// Protocol (deterministic — no reliance on scheduler interleaving):
//   { role: "holder", cwd } -> takes the write lock (BEGIN IMMEDIATE), replies
//                              "locked"; on "release" it COMMITs, replies "released".
//   { role: "writer", cwd } -> opens its connection, replies "ready", then
//                              IMMEDIATELY (same synchronous turn) performs one
//                              write, replying { ok, error, waitedMs }. The test
//                              starts its hold timer only on "ready", so the write
//                              is guaranteed to be attempted while the lock is held.

import { type DataSource, openDataSource } from "./data_sdk.ts";

type Msg =
  | { role: "holder"; cwd: string }
  | { role: "writer"; cwd: string }
  | "release";

let held: DataSource | undefined;

self.onmessage = async (e: MessageEvent<Msg>) => {
  const m = e.data;
  if (m === "release") {
    await held!.exec("COMMIT");
    held!.close();
    self.postMessage("released");
    return;
  }
  const db = await openDataSource("app", { cwd: m.cwd });
  if (m.role === "holder") {
    await db.exec("BEGIN IMMEDIATE");
    await db.exec("INSERT INTO hits (who) VALUES ('holder')");
    held = db;
    self.postMessage("locked");
    return;
  }
  self.postMessage("ready");
  const t0 = performance.now();
  try {
    await db.exec("INSERT INTO hits (who) VALUES ('writer')");
    self.postMessage({ ok: true, error: "", waitedMs: performance.now() - t0 });
  } catch (err) {
    self.postMessage({
      ok: false,
      error: String((err as Error).message ?? err),
      waitedMs: performance.now() - t0,
    });
  } finally {
    db.close();
  }
};
