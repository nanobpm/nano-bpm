// Web Worker for the data SDK cross-connection concurrency test
// (data_sdk_test.ts, #1287). Each worker is a separate isolate, so it opens its
// OWN SQLite connection through the SDK — exactly like two concurrent data-op
// processes (e.g. a webhook enqueue racing an inbox poll) hitting one project DB.
// It hammers DDL + writes and reports how many operations failed.

import { openDataSource } from "./data_sdk.ts";

self.onmessage = async (e: MessageEvent<{ cwd: string; id: number; n: number }>) => {
  const { cwd, id, n } = e.data;
  let failures = 0;
  let firstError = "";
  const db = await openDataSource("app", { cwd });
  for (let i = 0; i < n; i++) {
    try {
      // DDL on every iteration mirrors `ensure_inbox`, which runs before every
      // inbox read — the write lock is contended even by "readers".
      await db.exec("CREATE TABLE IF NOT EXISTS hits (id INTEGER PRIMARY KEY, who TEXT)");
      await db.exec("INSERT INTO hits (who) VALUES (?)", [`${id}-${i}`]);
    } catch (err) {
      failures++;
      if (!firstError) firstError = String((err as Error).message ?? err);
    }
  }
  db.close();
  self.postMessage({ failures, firstError });
};
