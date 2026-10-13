// Unit tests for the Urban App page runtime (ADR 0042 §3).
// Run: `node --experimental-strip-types --test server/crates/nano-server-console/src/app_pages_test.ts`
import { test } from "node:test";
import { assert, assertEquals } from "./test_support.ts";
import { createPagesHandler, servePages, type PagesContext } from "./app_pages.ts";

function ctx(over: Partial<PagesContext> & { port?: number } = {}): PagesContext {
  return {
    db: {
      schema: () => Promise.resolve([{ name: "pull_requests" }]),
      query: (sql: string) =>
        /table_info/.test(sql)
          ? Promise.resolve([{ name: "pr_key" }, { name: "status" }])
          : Promise.resolve([{ pr_key: "o/r#1", status: "converging" }]),
    },
    nano: fakeEngine(),
    readPage: (path: string) =>
      path.endsWith("home.page.json")
        ? Promise.resolve(JSON.stringify({ schemaVersion: "1.0", title: "T", nodes: [] }))
        : Promise.reject(new Error("nope")),
    ...over,
  };
}

function fakeEngine(over: Partial<PagesContext["nano"]> = {}): PagesContext["nano"] {
  return {
    createProcessInstance: (_i) => Promise.resolve({ processInstanceKey: 42 }),
    cancelProcessInstance: (_i) => Promise.resolve({}),
    publishMessage: (_i) => Promise.resolve({}),
    ...over,
  };
}

test("GET / serves the renderer shell with the home page id", async () => {
  const res = await createPagesHandler(ctx({ homePage: "home" }))(new Request("http://x/"));
  assertEquals(res.status, 200);
  const html = await res.text();
  assert(html.includes('data-home="home"'));
  assert(html.includes("/app/runtime.js"));
});

test("GET /app/pages/home returns the page.json", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/pages/home"));
  assertEquals(res.status, 200);
  const doc = await res.json();
  assertEquals(doc.title, "T");
});

test("GET /app/pages/missing is 404", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/pages/missing"));
  assertEquals(res.status, 404);
});

test("GET /app/data returns rows for a known table", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/data/app/pull_requests"));
  assertEquals(res.status, 200);
  const body = await res.json();
  assertEquals(body.rows.length, 1);
  assertEquals(body.rows[0].pr_key, "o/r#1");
});

test("GET /app/data rejects an unknown table (no SQL injection surface)", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/data/app/secrets"));
  assertEquals(res.status, 404);
});

test("GET /app/data rejects a source other than the injected default", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/data/other/pull_requests"));
  assertEquals(res.status, 404);
  assertEquals((await res.json()).error, 'unknown datasource "other"');
});

test("POST /app/actions/start starts a process with the posted variables", async () => {
  let seen: unknown = null;
  const c = ctx({
    nano: fakeEngine({
      createProcessInstance: (i) => {
        seen = i;
        return Promise.resolve({ processInstanceKey: 7 });
      },
    }),
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/start/convergence-loop", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ variables: { pr: "o/r#1" } }),
    }),
  );
  assertEquals(res.status, 200);
  assertEquals((await res.json()).processInstanceKey, 7);
  assertEquals(seen, { processDefinitionId: "convergence-loop", variables: { pr: "o/r#1" } });
});

test("POST /app/actions/start defaults a non-object `variables` to {}", async () => {
  let seen: unknown = null;
  const c = ctx({
    nano: fakeEngine({
      createProcessInstance: (i) => {
        seen = i;
        return Promise.resolve({ processInstanceKey: 9 });
      },
    }),
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/start/p", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ variables: ["not", "an", "object"] }),
    }),
  );
  assertEquals(res.status, 200);
  assertEquals(seen, { processDefinitionId: "p", variables: {} });
});

test("POST /app/actions/start surfaces an engine error as 502", async () => {
  const c = ctx({
    nano: fakeEngine({ createProcessInstance: () => Promise.reject(new Error("engine down")) }),
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/start/p", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    }),
  );
  assertEquals(res.status, 502);
  assert((await res.json()).error.includes("engine down"));
});

test("unknown route is 404", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/nope"));
  assertEquals(res.status, 404);
});

test("GET /app/data retries schema introspection after a transient failure", async () => {
  let calls = 0;
  const handle = createPagesHandler(
    ctx({
      db: {
        schema: () => {
          calls += 1;
          return calls === 1
            ? Promise.reject(new Error("database is locked"))
            : Promise.resolve([{ name: "pull_requests" }]);
        },
        query: (_sql: string) => Promise.resolve([{ pr_key: "o/r#1" }]),
      },
    }),
  );
  // First hit: introspection rejects → surfaced as an error, NOT cached.
  const first = await handle(new Request("http://x/app/data/app/pull_requests"));
  assertEquals(first.status, 500);
  // Second hit: the datasource has recovered → introspection retried, succeeds.
  const second = await handle(new Request("http://x/app/data/app/pull_requests"));
  assertEquals(second.status, 200);
  assertEquals(calls, 2);
});

test("GET /app/data applies whitelisted where + order as bound params", async () => {
  let seenSql = "";
  let seenParams: unknown[] = [];
  const c = ctx({
    db: {
      schema: () => Promise.resolve([{ name: "pull_requests" }]),
      query: (sql: string, params?: unknown[]) => {
        if (/table_info/.test(sql)) {
          return Promise.resolve([{ name: "status" }, { name: "updated_at" }]);
        }
        seenSql = sql;
        seenParams = params ?? [];
        return Promise.resolve([{ status: "converging" }]);
      },
    },
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/data/app/pull_requests?where=status:converging&order=updated_at:desc"),
  );
  assertEquals(res.status, 200);
  assert(seenSql.includes("WHERE status = ?"));
  assert(seenSql.includes("ORDER BY updated_at DESC"));
  assertEquals(seenParams, ["converging"]);
});

test("GET /app/data rejects a where column not on the table (no injection)", async () => {
  const res = await createPagesHandler(ctx())(
    new Request("http://x/app/data/app/pull_requests?where=evil'--:1"),
  );
  assertEquals(res.status, 400);
});

test("POST /app/actions/cancel cancels the posted instance", async () => {
  let seen: unknown = null;
  const c = ctx({
    nano: fakeEngine({
      cancelProcessInstance: (i) => {
        seen = i;
        return Promise.resolve({});
      },
    }),
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/cancel", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ processInstanceKey: "123" }),
    }),
  );
  assertEquals(res.status, 200);
  assertEquals((await res.json()).ok, true);
  assertEquals(seen, { processInstanceKey: "123" });
});

test("POST /app/actions/cancel requires a key", async () => {
  const res = await createPagesHandler(ctx())(
    new Request("http://x/app/actions/cancel", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    }),
  );
  assertEquals(res.status, 400);
});

test("POST /app/actions/message publishes a correlated message", async () => {
  let seen: unknown = null;
  const c = ctx({
    nano: fakeEngine({
      publishMessage: (i) => {
        seen = i;
        return Promise.resolve({});
      },
    }),
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/message", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "escalation-answered", correlationKey: "o/r#1", variables: { answer: "yes" } }),
    }),
  );
  assertEquals(res.status, 200);
  assertEquals(seen, { name: "escalation-answered", correlationKey: "o/r#1", variables: { answer: "yes" } });
});

test("POST /app/actions/message requires name and correlationKey", async () => {
  const res = await createPagesHandler(ctx())(
    new Request("http://x/app/actions/message", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "m" }),
    }),
  );
  assertEquals(res.status, 400);
});

test("GET /app/data supports a whitelisted IN filter (set membership)", async () => {
  let seenSql = "";
  let seenParams: unknown[] = [];
  const c = ctx({
    db: {
      schema: () => Promise.resolve([{ name: "pull_requests" }]),
      query: (sql: string, params?: unknown[]) => {
        if (/table_info/.test(sql)) return Promise.resolve([{ name: "status" }]);
        seenSql = sql;
        seenParams = params ?? [];
        return Promise.resolve([]);
      },
    },
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/data/app/pull_requests?where=status:in:converging,escalated,waiting_review"),
  );
  assertEquals(res.status, 200);
  assert(seenSql.includes("status IN (?, ?, ?)"));
  assertEquals(seenParams, ["converging", "escalated", "waiting_review"]);
});

test("servePages starts a node:http server that serves the page runtime", async () => {
  // Red/green guard for the Node-only runtime (ADR 0065): `servePages` used to be
  // `Deno.serve`, which is undefined under Node, so a scaffolded Urban app's real
  // `main.ts` (`servePages({ db, nano, port })`) would throw the moment it served.
  // This drives the actual socket path — the one the unit handler tests skip.
  const server = servePages(ctx({ port: 0, homePage: "home" }));
  try {
    await new Promise<void>((resolve, reject) => {
      if (server.listening) resolve();
      else {
        server.once("listening", resolve);
        server.once("error", reject);
      }
    });
    const addr = server.address();
    assert(addr && typeof addr === "object", "server must expose its bound address");
    const port = (addr as { port: number }).port;
    // GET / — the renderer shell.
    const home = await fetch(`http://127.0.0.1:${port}/`);
    assertEquals(home.status, 200);
    assert((await home.text()).includes("/app/runtime.js"));
    // GET /app/data/<source>/<table> — the data bridge.
    const data = await fetch(`http://127.0.0.1:${port}/app/data/app/pull_requests`);
    assertEquals(data.status, 200);
    assertEquals((await data.json()).rows.length, 1);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
