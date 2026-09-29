// Deno unit tests for the Urban datasource SDK (ADR 0024 phase-1 core).
//
// Run in CI by the `console-deno` job, and locally with:
//   deno test --allow-read --allow-write --allow-env server/src/console/data_sdk_test.ts
//
// They cover the env-template alias flip, url→path resolution, and a full SQLite
// roundtrip (exec/query/tx/schema) against a temp manifest+db.

import { assertEquals, assertRejects } from "jsr:@std/assert@1";
import {
  isReadStatement,
  openDataSource,
  resolveEnvTemplate,
  resolveSource,
  SQLITE_BUSY_TIMEOUT_MS,
  sqlitePath,
} from "./data_sdk.ts";

Deno.test("resolveEnvTemplate expands vars and :- defaults", () => {
  const env = (k: string) => ({ SET: "pg", EMPTY: "" }[k]);
  assertEquals(resolveEnvTemplate("${SET}", env), "pg");
  assertEquals(resolveEnvTemplate("${MISSING:-sqlite}", env), "sqlite");
  assertEquals(resolveEnvTemplate("${EMPTY:-fallback}", env), "fallback");
  assertEquals(resolveEnvTemplate("${MISSING}", env), "");
  assertEquals(
    resolveEnvTemplate("postgres://${DB_HOST:-localhost}:${DB_PORT:-5432}/app", env),
    "postgres://localhost:5432/app",
  );
});

Deno.test("resolveSource flips driver+url from env", () => {
  const env = (k: string) => ({ NANO_APP_DB_DRIVER: "postgres" }[k]);
  const r = resolveSource(
    "app",
    { driver: "${NANO_APP_DB_DRIVER:-sqlite}", url: "${NANO_APP_DB_URL:-file:./app.db}" },
    env,
  );
  assertEquals(r.driver, "postgres");
  assertEquals(r.url, "file:./app.db");
});

Deno.test("sqlitePath resolves file: urls against project root", () => {
  assertEquals(sqlitePath("file:./app.db", "/proj"), "/proj/app.db");
  assertEquals(sqlitePath("file:app.db", "/proj"), "/proj/app.db");
  assertEquals(sqlitePath("file:/abs/app.db", "/proj"), "/abs/app.db");
  assertEquals(sqlitePath("app.db", "/proj"), "/proj/app.db");
  assertEquals(sqlitePath(":memory:", "/proj"), ":memory:");
});

Deno.test("openDataSource: SQLite roundtrip, tx, and schema", async () => {
  const root = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  // A worker runs from a nested cwd; the SDK must walk up to the manifest.
  const cwd = `${root}/workers/save`;
  await Deno.mkdir(cwd, { recursive: true });

  const db = await openDataSource("app", { cwd });
  await db.exec(
    "CREATE TABLE orders(id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER)",
  );
  await db.exec("CREATE INDEX idx_orders_name ON orders(name)");

  const ins = await db.exec("INSERT INTO orders(name, qty) VALUES (?, ?)", ["widget", 3]);
  assertEquals(ins.changed, 1);
  assertEquals(Number(ins.lastInsertId), 1);

  // tx commits on success
  await db.tx(async (t) => {
    await t.exec("INSERT INTO orders(name, qty) VALUES (?, ?)", ["gadget", 7]);
  });
  // tx rolls back on throw
  await assertRejects(() =>
    db.tx(async (t) => {
      await t.exec("INSERT INTO orders(name, qty) VALUES (?, ?)", ["ghost", 1]);
      throw new Error("boom");
    })
  );

  const rows = await db.query("SELECT name, qty FROM orders ORDER BY id");
  assertEquals(rows, [
    { name: "widget", qty: 3 },
    { name: "gadget", qty: 7 },
  ]);

  const schema = await db.schema();
  assertEquals(schema.length, 1);
  assertEquals(schema[0].name, "orders");
  assertEquals(schema[0].columns.map((c) => c.name), ["id", "name", "qty"]);
  assertEquals(schema[0].columns[0].primaryKey, true);
  assertEquals(schema[0].columns[1].notNull, true);
  assertEquals(schema[0].indexes.includes("idx_orders_name"), true);
  assertEquals(schema[0].foreignKeys, []);

  db.close();

  // The default source is picked when no name is given, and re-opens are cached.
  const dflt = await openDataSource(undefined, { cwd });
  assertEquals((await dflt.query("SELECT COUNT(*) c FROM orders"))[0].c, 2);
  dflt.close();

  await Deno.remove(root, { recursive: true });
});

Deno.test("schema() introspects foreign keys", async () => {
  const root = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  const db = await openDataSource("app", { cwd: root });
  await db.exec("CREATE TABLE customer(id INTEGER PRIMARY KEY, name TEXT)");
  await db.exec(
    "CREATE TABLE ord(" +
      "id INTEGER PRIMARY KEY, " +
      "customer_id INTEGER REFERENCES customer(id) ON DELETE CASCADE, " +
      "parent_id INTEGER REFERENCES ord)",
  );

  const schema = await db.schema();
  const customer = schema.find((t) => t.name === "customer")!;
  const ord = schema.find((t) => t.name === "ord")!;

  // The parent table has no outgoing FKs.
  assertEquals(customer.foreignKeys, []);

  // FKs are keyed by their local column; a named target keeps its column, an
  // unnamed self-reference resolves to the parent PK (empty refColumn), and
  // "NO ACTION" normalises to an empty onDelete.
  const byCol = Object.fromEntries(ord.foreignKeys.map((f) => [f.column, f]));
  assertEquals(byCol["customer_id"], {
    column: "customer_id",
    refTable: "customer",
    refColumn: "id",
    onDelete: "CASCADE",
  });
  assertEquals(byCol["parent_id"], {
    column: "parent_id",
    refTable: "ord",
    refColumn: "",
    onDelete: "",
  });

  db.close();
  await Deno.remove(root, { recursive: true });
});

Deno.test("openDataSource: unknown driver hints at a pack", async () => {
  const root = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({ data: { default: "app", sources: { app: { driver: "mysql", url: "x" } } } }),
  );
  await assertRejects(
    () => openDataSource("app", { cwd: root }),
    Error,
    "nano-ide-data-mysql",
  );
  await Deno.remove(root, { recursive: true });
});

Deno.test("isReadStatement: plain reads are reads, plain writes are writes", () => {
  for (const sql of ["SELECT 1", "  select * from t  ", "EXPLAIN QUERY PLAN SELECT 1", "explain select 1"]) {
    assertEquals(isReadStatement(sql), true, sql);
  }
  for (
    const sql of [
      "INSERT INTO t VALUES (1)",
      "update t set a = 1",
      "DELETE FROM t",
      "CREATE TABLE t (a int)",
      "DROP TABLE t",
      "REPLACE INTO t VALUES (1)",
    ]
  ) {
    assertEquals(isReadStatement(sql), false, sql);
  }
});

Deno.test("isReadStatement: mutating RETURNING and data-modifying CTEs are writes", () => {
  // The query-op bypass: a mutation that returns rows must not be a read.
  assertEquals(isReadStatement("INSERT INTO t VALUES (1) RETURNING id"), false, "INSERT … RETURNING");
  assertEquals(isReadStatement("update t set a = 1 returning *"), false, "UPDATE … RETURNING");
  assertEquals(isReadStatement("DELETE FROM t WHERE id = 1 RETURNING id"), false, "DELETE … RETURNING");
  assertEquals(isReadStatement("WITH c AS (SELECT 1) SELECT * FROM c"), true, "WITH … SELECT");
  assertEquals(isReadStatement("WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"), false, "WITH … INSERT");
  assertEquals(isReadStatement("with c as (select 1) delete from t"), false, "WITH … DELETE");
});

Deno.test("isReadStatement: PRAGMA is a read only in its argument-less query form", () => {
  assertEquals(isReadStatement("PRAGMA foreign_keys"), true, "read PRAGMA");
  assertEquals(isReadStatement("pragma table_info"), true, "read PRAGMA");
  assertEquals(isReadStatement("PRAGMA foreign_keys = ON"), false, "assigning PRAGMA");
  assertEquals(isReadStatement("pragma table_info(t)"), false, "call-form PRAGMA");
});

Deno.test("isReadStatement: leading SQL comments don't hide the verb", () => {
  // A read preceded by a comment must stay a read (the misclassification bug).
  assertEquals(isReadStatement("-- note\nSELECT 1"), true, "line-comment read");
  assertEquals(isReadStatement("/* note */ SELECT 1"), true, "block-comment read");
  assertEquals(isReadStatement("  -- a\n  /* b */ select * from t"), true, "stacked-comment read");
  assertEquals(isReadStatement("/* x */ WITH c AS (SELECT 1) SELECT * FROM c"), true, "commented CTE read");
  // The conservative direction still holds: a commented write stays a write.
  assertEquals(isReadStatement("-- note\nINSERT INTO t VALUES (1)"), false, "line-comment write");
  assertEquals(isReadStatement("/* note */ WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"), false, "commented CTE write");
  // An input that is only a comment is not a read.
  assertEquals(isReadStatement("-- just a comment"), false, "comment-only");
});

// --- concurrent connections (#1287) -----------------------------------------
//
// Every data op is its own process with its own SQLite connection, so a project
// DB routinely sees concurrent writers (a webhook enqueue racing an inbox poll).
// With no busy timeout, node:sqlite fails the loser immediately with
// "database is locked" — which silently dropped trigger-inbox events.

async function tempSqliteProject(): Promise<string> {
  const root = await Deno.makeTempDir();
  await Deno.writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  return root;
}

Deno.test("openDataSource: SQLite connections wait on a lock instead of failing (busy_timeout)", async () => {
  const root = await tempSqliteProject();
  const db = await openDataSource("app", { cwd: root });
  const rows = await db.query("PRAGMA busy_timeout");
  const ms = Number(Object.values(rows[0] ?? {})[0] ?? 0);
  db.close();
  await Deno.remove(root, { recursive: true });
  assertEquals(ms === SQLITE_BUSY_TIMEOUT_MS && ms > 0, true, `busy_timeout is ${ms}ms; a contended write must wait, not fail`);
});

Deno.test("openDataSource: a write contended by another connection's lock waits, then succeeds", async () => {
  const root = await tempSqliteProject();
  const setup = await openDataSource("app", { cwd: root });
  await setup.exec("CREATE TABLE hits (id INTEGER PRIMARY KEY, who TEXT)");
  setup.close();

  const url = new URL("./data_sdk_concurrency_worker.ts", import.meta.url);
  const holder = new Worker(url, { type: "module" });
  const writer = new Worker(url, { type: "module" });
  // Per-worker message queue: a worker may post several messages (the writer
  // sends "ready" then its result), and none may be lost between awaits.
  const inbox = (w: Worker) => {
    const queued: unknown[] = [];
    const waiters: Array<{ resolve: (v: unknown) => void; reject: (e: unknown) => void }> = [];
    w.onmessage = (e) => {
      const waiter = waiters.shift();
      if (waiter) waiter.resolve(e.data);
      else queued.push(e.data);
    };
    w.onerror = (e) => {
      e.preventDefault();
      const err = e.error ?? new Error(e.message);
      for (const waiter of waiters.splice(0)) waiter.reject(err);
    };
    return () =>
      queued.length > 0
        ? Promise.resolve(queued.shift())
        : new Promise<unknown>((resolve, reject) => waiters.push({ resolve, reject }));
  };
  const fromHolder = inbox(holder);
  const fromWriter = inbox(writer);
  const HOLD_MS = 300;
  try {
    // 1. Another connection takes the write lock — contention is now guaranteed.
    holder.postMessage({ role: "holder", cwd: root });
    assertEquals(await fromHolder(), "locked");

    // 2. A second connection writes while the lock is held. "ready" is posted
    //    in the same synchronous turn as the write, so the hold timer below
    //    cannot start before the write is attempted.
    writer.postMessage({ role: "writer", cwd: root });
    assertEquals(await fromWriter(), "ready");

    // 3. Release well inside the busy timeout.
    await new Promise((r) => setTimeout(r, HOLD_MS));
    holder.postMessage("release");
    assertEquals(await fromHolder(), "released");

    const r = await fromWriter() as { ok: boolean; error: string; waitedMs: number };
    assertEquals(r.ok, true, `contended write failed after ${r.waitedMs}ms: ${r.error}`);
    // It really waited for the lock (not an uncontended fast path).
    assertEquals(r.waitedMs >= HOLD_MS / 2, true, `write only waited ${r.waitedMs}ms`);
  } finally {
    holder.terminate();
    writer.terminate();
  }

  const db = await openDataSource("app", { cwd: root });
  const rows = await db.query("SELECT who FROM hits ORDER BY id");
  db.close();
  await Deno.remove(root, { recursive: true });
  assertEquals(rows.map((x) => x.who), ["holder", "writer"]);
});
