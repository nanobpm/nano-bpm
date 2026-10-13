// Node unit tests for the Urban domain-type reifier (ADR 0029 §4.1) and the Fused
// Domain Model emitters (ADR 0040 §1/§5).
//
// Run in CI by the `console-ts` job, and locally with:
//   node --experimental-strip-types --test server/crates/nano-server-console/src/
//
// They cover the SQLite-affinity → TS mapping, interface-name sanitising, the
// `domain-rows.d.ts` emitter, a full schema() → emit → write roundtrip against a
// temp manifest + SQLite db, and the model-metadata / domain.json fuse emitters.

import { test } from "node:test";
import {
  assertEquals,
  assertStringIncludes,
  makeTempDir,
  mkdir,
  readTextFile,
  remove,
  writeTextFile,
} from "./test_support.ts";
import type { TableMeta } from "./data_sdk.ts";
import type { ShapeDecl } from "./domain_types.ts";
import {
  DOMAIN_BINDINGS,
  DOMAIN_DTS,
  emitDomainBindings,
  emitDomainDts,
  emitDomainDtsForSources,
  emitDomainModel,
  emitDomainModelJson,
  emitDomainTypeRegistry,
  emitMeta,
  emitWorkerBindings,
  emitWorkerBindingsRuntime,
  foldMeta,
  interfaceName,
  resolveShapes,
  sqliteAffinityToTs,
  WORKER_BINDINGS_DTS,
  WORKER_BINDINGS_TS,
} from "./domain_types.ts";

test("sqliteAffinityToTs applies affinity rules + Urban overrides", () => {
  assertEquals(sqliteAffinityToTs("INTEGER"), "number");
  assertEquals(sqliteAffinityToTs("BIGINT"), "number");
  assertEquals(sqliteAffinityToTs("TEXT"), "string");
  assertEquals(sqliteAffinityToTs("VARCHAR(255)"), "string");
  assertEquals(sqliteAffinityToTs("REAL"), "number");
  assertEquals(sqliteAffinityToTs("DOUBLE"), "number");
  assertEquals(sqliteAffinityToTs("NUMERIC"), "number");
  assertEquals(sqliteAffinityToTs("BLOB"), "Uint8Array");
  assertEquals(sqliteAffinityToTs(""), "unknown"); // NONE affinity
  // Maker-friendly overrides the DB Manager's type list implies:
  assertEquals(sqliteAffinityToTs("BOOLEAN"), "boolean");
  assertEquals(sqliteAffinityToTs("TIMESTAMP"), "string");
  assertEquals(sqliteAffinityToTs("DATE"), "string");
});

test("interfaceName PascalCases and sanitises to an identifier", () => {
  assertEquals(interfaceName("customers"), "Customers");
  assertEquals(interfaceName("order_items"), "OrderItems");
  assertEquals(interfaceName("2fa-tokens"), "T_2faTokens");
});

test("emitDomainDts renders interfaces + a DomainTables map", () => {
  const tables: TableMeta[] = [
    {
      name: "customers",
      indexes: [], foreignKeys: [],
      columns: [
        { name: "id", type: "INTEGER", primaryKey: true, notNull: false },
        { name: "name", type: "TEXT", primaryKey: false, notNull: true },
        { name: "tier", type: "TEXT", primaryKey: false, notNull: false },
        { name: "active", type: "BOOLEAN", primaryKey: false, notNull: true },
      ],
    },
  ];
  const dts = emitDomainDts(tables);
  assertStringIncludes(dts, "export interface Customers {");
  assertStringIncludes(dts, "id: number;"); // PK ⇒ not null
  assertStringIncludes(dts, "name: string;"); // NOT NULL ⇒ not null
  assertStringIncludes(dts, "tier: string | null;"); // nullable widens
  assertStringIncludes(dts, "active: boolean;");
  assertStringIncludes(dts, 'export interface DomainTables {');
  assertStringIncludes(dts, '"customers": Customers;');
});

test("emitDomainDts quotes non-identifier column names", () => {
  const dts = emitDomainDts([
    {
      name: "t",
      indexes: [], foreignKeys: [],
      columns: [{ name: "first name", type: "TEXT", primaryKey: false, notNull: true }],
    },
  ]);
  assertStringIncludes(dts, '"first name": string;');
});

test("emitDomainDts handles an empty schema", () => {
  assertStringIncludes(emitDomainDts([]), "export interface DomainTables {}");
});

test("emitDomainDtsForSources is byte-identical to emitDomainDts for one source", () => {
  const tables: TableMeta[] = [{
    name: "customers",
    indexes: [], foreignKeys: [],
    columns: [
      { name: "id", type: "INTEGER", primaryKey: true, notNull: false },
      { name: "name", type: "TEXT", primaryKey: false, notNull: true },
    ],
  }];
  assertEquals(
    emitDomainDtsForSources([{ source: "app", tables }], "app"),
    emitDomainDts(tables),
  );
  // No sources at all also degrades to the empty single-source output.
  assertEquals(emitDomainDtsForSources([], "app"), emitDomainDts([]));
});

test("emitDomainDtsForSources unions sources + resolves name collisions", () => {
  const customers: TableMeta[] = [{
    name: "customers",
    indexes: [], foreignKeys: [],
    columns: [{ name: "id", type: "INTEGER", primaryKey: true, notNull: false }],
  }];
  const events: TableMeta[] = [{
    name: "customers", // same table name in a different source
    indexes: [], foreignKeys: [],
    columns: [{ name: "at", type: "TIMESTAMP", primaryKey: false, notNull: true }],
  }];
  const dts = emitDomainDtsForSources(
    [{ source: "app", tables: customers }, { source: "analytics", tables: events }],
    "analytics",
  );
  // Collision-free, source-prefixed interfaces.
  assertStringIncludes(dts, "export interface AppCustomers {");
  assertStringIncludes(dts, "export interface AnalyticsCustomers {");
  // A DomainSources map keyed by alias then wire table name.
  assertStringIncludes(dts, "export interface DomainSources {");
  assertStringIncludes(dts, '"app": {');
  assertStringIncludes(dts, '"customers": AppCustomers;');
  assertStringIncludes(dts, '"analytics": {');
  assertStringIncludes(dts, '"customers": AnalyticsCustomers;');
  // DomainTables aliases the declared default source.
  assertStringIncludes(dts, 'export type DomainTables = DomainSources["analytics"];');
});

test("emitDomainDtsForSources emits an empty object for a source with no tables", () => {
  const dts = emitDomainDtsForSources(
    [
      { source: "app", tables: [{ name: "t", indexes: [], foreignKeys: [], columns: [{ name: "id", type: "INTEGER", primaryKey: true, notNull: false }] }] },
      { source: "empty", tables: [] },
    ],
    "app",
  );
  assertStringIncludes(dts, '"empty": {};');
  // Falls back to the first source when the default is unknown.
  const dts2 = emitDomainDtsForSources(
    [
      { source: "a", tables: [] },
      { source: "b", tables: [] },
    ],
    "missing",
  );
  assertStringIncludes(dts2, 'export type DomainTables = DomainSources["a"];');
});

test("emitDomainTypeRegistry renders declared types with refs, optional + list", () => {
  const dts = emitDomainTypeRegistry({
    taxLine: { fields: { amount: { type: "number" }, note: { type: "string", optional: true } } },
    taxSubmission: {
      name: "Tax Submission",
      fields: {
        filedAt: { type: "datetime" },
        active: { type: "boolean", optional: true },
        blob: { type: "json" },
        lines: { type: "taxLine", list: true },
        ref: { type: "unknownType" }, // unresolved → unknown
      },
    },
  });
  assertStringIncludes(dts, "export interface DomainTypes {");
  assertStringIncludes(dts, '"taxLine": {');
  assertStringIncludes(dts, "amount: number;");
  assertStringIncludes(dts, "note?: string;"); // optional widens the key
  assertStringIncludes(dts, '"taxSubmission": {');
  assertStringIncludes(dts, "filedAt: string;"); // datetime → ISO string
  assertStringIncludes(dts, "active?: boolean;");
  assertStringIncludes(dts, "blob: unknown;"); // json → unknown
  assertStringIncludes(dts, 'lines: DomainTypes["taxLine"][];'); // ref + list
  assertStringIncludes(dts, "ref: unknown;"); // unresolved ref degrades
});

test("emitDomainTypeRegistry is empty for an empty registry", () => {
  assertEquals(emitDomainTypeRegistry({}), "");
});

test("emitDomainModel appends the registry to the table spine", () => {
  const tables: TableMeta[] = [{
    name: "customers",
    indexes: [], foreignKeys: [],
    columns: [{ name: "id", type: "INTEGER", primaryKey: true, notNull: false }],
  }];
  // No declared types → byte-identical to the table-only spine.
  assertEquals(
    emitDomainModel([{ source: "app", tables }], "app", {}),
    emitDomainDtsForSources([{ source: "app", tables }], "app"),
  );
  // With declared types → both DomainTables and DomainTypes are present.
  const full = emitDomainModel(
    [{ source: "app", tables }],
    "app",
    { note: { fields: { text: { type: "string" } } } },
  );
  assertStringIncludes(full, "export interface DomainTables {");
  assertStringIncludes(full, "export interface DomainTypes {");
  assertStringIncludes(full, '"note": {');
});

test("schema() → emit → write roundtrip (the CLI op's path)", async () => {
  const root = await makeTempDir();
  await writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  const cwd = `${root}/workers/gen`;
  await mkdir(cwd, { recursive: true });

  const { openDataSource } = await import("./data_sdk.ts");
  const db = await openDataSource("app", { cwd });
  await db.exec(
    "CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier TEXT, credit REAL)",
  );
  const tables = await db.schema();
  db.close();

  // Mirror the data-cli `domaintypes` op: emit from schema() and write.
  const text = emitDomainDts(tables);
  const outDir = `${root}/nano-generated`;
  await mkdir(outDir, { recursive: true });
  const path = `${outDir}/${DOMAIN_DTS}`;
  await writeTextFile(path, text);

  assertEquals(tables.length, 1);
  const written = await readTextFile(path);
  assertEquals(written, text);
  assertStringIncludes(written, "export interface Customers {");
  assertStringIncludes(written, "id: number;");
  assertStringIncludes(written, "tier: string | null;");
  assertStringIncludes(written, "credit: number | null;");
  assertStringIncludes(written, '"customers": Customers;');

  await remove(root);
});

test("emitDomainBindings renders openDomain with a typed Table per table", () => {
  const tables: TableMeta[] = [
    {
      name: "orders",
      columns: [
        { name: "id", type: "INTEGER", notNull: true, primaryKey: true },
        { name: "customer_id", type: "INTEGER", notNull: true, primaryKey: false },
        { name: "status", type: "TEXT", notNull: false, primaryKey: false },
      ],
      indexes: [],
      foreignKeys: [],
    },
  ];
  const out = emitDomainBindings([{ source: "app", tables }], "app");
  // Imports only the sibling SDK (relative) + a type-only domain-rows.d.ts — no
  // jsr:/https: so it survives the Node loader (ADR 0065).
  assertStringIncludes(out, 'import { openDataSource, type DataSource, type Table } from "./data-sdk.ts";');
  assertStringIncludes(out, `import type { Orders } from "./${DOMAIN_DTS}";`);
  assertStringIncludes(out, "export interface Domain {");
  assertStringIncludes(out, "readonly raw: DataSource;");
  assertStringIncludes(out, "readonly orders: Table<Orders>;");
  assertStringIncludes(out, "export async function openDomain(source?: string): Promise<Domain> {");
  // The gateway is bound with the table's real primary key.
  assertStringIncludes(out, 'orders: raw.table<Orders>("orders", "id"),');
  assertStringIncludes(out, "close: () => raw.close(),");
  assertEquals(DOMAIN_BINDINGS, "domain.ts");
});

test("emitDomainBindings drops reserved-named tables (raw/close) from the typed surface", () => {
  const tables: TableMeta[] = [
    {
      name: "raw",
      columns: [{ name: "id", type: "INTEGER", notNull: true, primaryKey: true }],
      indexes: [],
      foreignKeys: [],
    },
    {
      name: "close",
      columns: [{ name: "id", type: "INTEGER", notNull: true, primaryKey: true }],
      indexes: [],
      foreignKeys: [],
    },
    {
      name: "orders",
      columns: [{ name: "id", type: "INTEGER", notNull: true, primaryKey: true }],
      indexes: [],
      foreignKeys: [],
    },
  ];
  // Single-source: the escape hatch must not be clobbered, and the emitted
  // object literal / interface must not carry a duplicate `raw`/`close` member.
  const single = emitDomainBindings([{ source: "app", tables }], "app");
  assertStringIncludes(single, "readonly raw: DataSource;");
  assertStringIncludes(single, "close(): void;");
  assertStringIncludes(single, 'orders: raw.table<Orders>("orders", "id"),');
  assertEquals(single.includes('raw: raw.table<'), false);
  assertEquals(single.includes('close: raw.table<'), false);
  // Multi-source keyed: reserved names are absent from the runtime descriptors,
  // so the `for … db[t.name] = …` loop can never overwrite `raw`/`close`.
  const multi = emitDomainBindings(
    [
      { source: "app", tables },
      {
        source: "analytics",
        tables: [{
          name: "events",
          columns: [{ name: "id", type: "INTEGER", notNull: true, primaryKey: true }],
          indexes: [],
          foreignKeys: [],
        }],
      },
    ],
    "app",
  );
  assertStringIncludes(multi, '"app": [{ name: "orders", pk: "id" }],');
  assertEquals(multi.includes('name: "raw"'), false);
  assertEquals(multi.includes('name: "close"'), false);
  // The handle type omits the reserved keys so `db.raw`/`db.close` stay the
  // escape hatch even if the row-type spine keys such a table.
  assertStringIncludes(multi, 'as K extends "raw" | "close" ? never : K');
});

test("emitDomainBindings uses the first declared PK (not always id)", () => {
  const tables: TableMeta[] = [
    {
      name: "sessions",
      columns: [
        { name: "token", type: "TEXT", notNull: true, primaryKey: true },
        { name: "user_id", type: "INTEGER", notNull: true, primaryKey: false },
      ],
      indexes: [],
      foreignKeys: [],
    },
  ];
  const out = emitDomainBindings([{ source: "app", tables }], "app");
  assertStringIncludes(out, 'sessions: raw.table<Sessions>("sessions", "token"),');
});

test("emitDomainBindings handles an empty schema (raw-only Domain)", () => {
  const out = emitDomainBindings([{ source: "app", tables: [] }], "app");
  assertStringIncludes(out, "export interface Domain {");
  assertStringIncludes(out, "readonly raw: DataSource;");
  assertStringIncludes(out, "export async function openDomain(");
  // No table imports and no Table fields.
  assertEquals(out.includes(`from "./${DOMAIN_DTS}"`), false);
  assertEquals(out.includes("raw.table<"), false);
});

test("emitDomainBindings emits a keyed openDomain<K> for multiple datasources", () => {
  const appTables: TableMeta[] = [
    {
      name: "orders",
      columns: [{ name: "id", type: "INTEGER", notNull: true, primaryKey: true }],
      indexes: [],
      foreignKeys: [],
    },
  ];
  const analyticsTables: TableMeta[] = [
    {
      name: "events",
      columns: [{ name: "uuid", type: "TEXT", notNull: true, primaryKey: true }],
      indexes: [],
      foreignKeys: [],
    },
  ];
  const out = emitDomainBindings(
    [
      { source: "app", tables: appTables },
      { source: "analytics", tables: analyticsTables },
    ],
    "app",
  );
  // Row-type spine is indexed from domain-rows.d.ts (no per-interface imports).
  assertStringIncludes(out, `import type { DomainSources } from "./${DOMAIN_DTS}";`);
  // A source union + a generic, source-keyed accessor defaulting to "app".
  assertStringIncludes(out, "export type DomainSource = keyof DomainSources;");
  assertStringIncludes(
    out,
    'export async function openDomain<K extends DomainSource = "app">(',
  );
  assertStringIncludes(out, "): Promise<Domain<DomainSources[K]>> {");
  // Per-source runtime table descriptors carrying each table's real primary key.
  assertStringIncludes(out, 'const DEFAULT_SOURCE = "app";');
  assertStringIncludes(out, '"app": [{ name: "orders", pk: "id" }],');
  assertStringIncludes(out, '"analytics": [{ name: "events", pk: "uuid" }],');
  // Gateways are bound dynamically for the selected source.
  assertStringIncludes(out, "db[t.name] = raw.table(t.name, t.pk);");
  // The single-source concrete shape is NOT used in the multi case.
  assertEquals(out.includes("export interface Domain {"), false);
});

test("Table<T> CRUD roundtrip via the data SDK (ADR 0029 §6)", async () => {
  const root = await makeTempDir();
  await writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  const { openDataSource } = await import("./data_sdk.ts");
  const db = await openDataSource("app", { cwd: root });
  try {
    await db.exec(
      "CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT NOT NULL, qty INTEGER NOT NULL, status TEXT)",
    );
    const orders = db.table<
      { id: number; item: string; qty: number; status: string | null }
    >("orders", "id");

    // insert returns the new rowid; get fetches it back typed.
    const id = Number(await orders.insert({ item: "Widget", qty: 3, status: "received" }));
    assertEquals(id, 1);
    const row = await orders.get(id);
    assertEquals(row?.item, "Widget");
    assertEquals(row?.qty, 3);

    // update by pk, then re-read.
    assertEquals(await orders.update(id, { status: "fulfilled" }), 1);
    assertEquals((await orders.get(id))?.status, "fulfilled");

    // find/count on an equality filter.
    await orders.insert({ item: "Gadget", qty: 12, status: "received" });
    assertEquals((await orders.find({ status: "received" })).length, 1);
    assertEquals(await orders.count(), 2);
    assertEquals((await orders.findOne({ item: "Gadget" }))?.qty, 12);

    // delete by pk.
    assertEquals(await orders.delete(id), 1);
    assertEquals(await orders.count(), 1);
    assertEquals(await orders.get(id), undefined);
  } finally {
    db.close();
    await remove(root);
  }
});

// Regression: the generated `domain-rows.d.ts` emits row types as `interface`s, and
// an interface (unlike an inline type literal) has no implicit string index
// signature — so it is NOT assignable to `Record<string, unknown>`. The Table
// bound must therefore be `T extends object` (ADR 0029 §6.1), or every worker
// that imports the reified `domain.ts` fails a TypeScript check with TS2344. This test
// passes an `interface` through `db.table<T>()`, so the harness's own type-check
// is the guard (a regression to `T extends Row` would fail to compile here).
interface RegOrderRow {
  id: number;
  item: string;
  qty: number;
  status: string | null;
}

test("Table<T> accepts an interface row type (ADR 0029 §6.1 — the domain-rows.d.ts shape)", async () => {
  const root = await makeTempDir();
  await writeTextFile(
    `${root}/nano.app.json`,
    JSON.stringify({
      data: { default: "app", sources: { app: { driver: "sqlite", url: "file:./app.db" } } },
    }),
  );
  const { openDataSource } = await import("./data_sdk.ts");
  const db = await openDataSource("app", { cwd: root });
  try {
    await db.exec(
      "CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT NOT NULL, qty INTEGER NOT NULL, status TEXT)",
    );
    // The `interface` type argument is the crux — this line would not compile
    // under a `T extends Row` bound.
    const orders = db.table<RegOrderRow>("orders", "id");
    const id = Number(await orders.insert({ item: "Widget", qty: 3, status: "received" }));
    assertEquals((await orders.get(id))?.item, "Widget");
    assertEquals(await orders.update(id, { status: "done" }), 1);
    assertEquals((await orders.findOne({ status: "done" }))?.id, id);
    assertEquals(await orders.count({ item: "Widget" }), 1);
  } finally {
    db.close();
    await remove(root);
  }
});

test("emitWorkerBindings maps taskType → declared input/output types (ADR 0033 §3)", () => {
  const out = emitWorkerBindings(
    [
      { taskType: "save-order", inputType: "orderRequest", outputType: "savedOrder" },
      { taskType: "review-order", inputType: "savedOrder" }, // input only
      { taskType: "noop" }, // no declared types → absent
      { taskType: "ghost", inputType: "undeclared" }, // undeclared id → absent
    ],
    ["orderRequest", "savedOrder"],
  );
  assertStringIncludes(out, `import type { DomainTypes } from "./${DOMAIN_DTS}";`);
  assertStringIncludes(out, "export interface WorkerInputs {");
  assertStringIncludes(out, `"save-order": DomainTypes["orderRequest"];`);
  assertStringIncludes(out, `"review-order": DomainTypes["savedOrder"];`);
  assertStringIncludes(out, "export interface WorkerOutputs {");
  assertStringIncludes(out, `"save-order": DomainTypes["savedOrder"];`);
  // Every declared taskType is in the model-derived union, including type-less
  // (`noop`) and undeclared-type (`ghost`) workers…
  assertStringIncludes(
    out,
    `export type WorkerTaskType = "save-order" | "review-order" | "noop" | "ghost";`,
  );
  // …but type-less/undeclared workers carry no typed input/output entry.
  assertEquals(out.includes('"noop": DomainTypes'), false);
  assertEquals(out.includes('"ghost": DomainTypes'), false);
  // review-order has no output → exactly one typed entry (its input).
  assertEquals(out.split(`"review-order": DomainTypes`).length, 2);
});

test("emitWorkerBindings maps header keys → typed customHeaders shape (ADR 0033 §3)", () => {
  const out = emitWorkerBindings(
    [
      { taskType: "charge", headerKeys: ["region", "priority"] },
      { taskType: "review", inputType: "savedOrder", headerKeys: ["region", " region ", "region", "", "   "] }, // trim + dedup + drop blank
      { taskType: "noop" }, // no headers → absent from WorkerHeaders
    ],
    ["savedOrder"],
  );
  assertStringIncludes(out, "export interface WorkerHeaders {");
  assertStringIncludes(out, "export type WorkerHdrs = Record<string, unknown>;");
  // Known keys map to `string` (wire type) with an index-signature escape hatch.
  assertStringIncludes(
    out,
    `"charge": { "region": string; "priority": string; [key: string]: string };`,
  );
  // Untrimmed duplicates, blank and whitespace-only keys collapse to one clean key.
  assertStringIncludes(
    out,
    `"review": { "region": string; [key: string]: string };`,
  );
  // A worker with no declared headers carries no WorkerHeaders entry.
  assertEquals(out.includes('"noop": {'), false);
});

test("emitWorkerBindings with no declared worker types is a valid empty map", () => {
  const out = emitWorkerBindings([{ taskType: "noop" }], []);
  assertStringIncludes(out, "export interface WorkerInputs {}");
  assertStringIncludes(out, "export interface WorkerOutputs {}");
  assertStringIncludes(out, "export interface WorkerHeaders {}");
  assertStringIncludes(out, "export type WorkerVars = Record<string, unknown>;");
  assertStringIncludes(out, "export type WorkerHdrs = Record<string, unknown>;");
  // The taskType is still surfaced in the model-derived union.
  assertStringIncludes(out, `export type WorkerTaskType = "noop";`);
  // No registry import when nothing references it.
  assertEquals(out.includes("import type { DomainTypes }"), false);
});

test("emitWorkerBindings with no workers at all yields a permissive WorkerTaskType", () => {
  const out = emitWorkerBindings([], []);
  assertStringIncludes(out, "export type WorkerTaskType = string;");
});

test("emitWorkerBindingsRuntime is a taskType-keyed typed defineWorker wrapper", () => {
  const out = emitWorkerBindingsRuntime();
  assertStringIncludes(out, `export * from "./worker-sdk.ts";`);
  assertStringIncludes(out, `import type { WorkerInputs, WorkerOutputs, WorkerHeaders, WorkerTaskType, WorkerVars, WorkerHdrs } from "./${WORKER_BINDINGS_DTS}";`);
  assertStringIncludes(out, "export function defineWorker<K extends WorkerTaskType>(");
  assertStringIncludes(out, "opts: { type: K } & WorkerOptions<InFor<K> & object, OutFor<K> & object, HdrFor<K> & object>,");
  assertStringIncludes(out, "type HdrFor<K extends WorkerTaskType> = K extends keyof WorkerHeaders ? WorkerHeaders[K] : WorkerHdrs;");
  // Node strip-only safety (ADR 0065): no TS parameter properties/enums.
  assertEquals(out.includes("constructor(private"), false);
  assertEquals(out.includes("enum "), false);
  // File basenames are stable.
  assertEquals(WORKER_BINDINGS_TS, "workers.ts");
  assertEquals(WORKER_BINDINGS_DTS, "worker-io.d.ts");
});

// --- model-level metadata: foldMeta / emitMeta (ADR 0040 §5) -----------------

test("foldMeta is last-wins over duplicate keys and skips empty keys", () => {
  const folded = foldMeta([
    { key: "owner", value: "ops" },
    { key: "owner", value: "sre" }, // later declaration wins
    { key: "  ", value: "ignored" }, // blank key skipped
    { key: " region ", value: " eu " }, // key trimmed; value carried as-authored
  ]);
  assertEquals(folded["owner"], "sre");
  assertEquals(folded["region"], " eu ");
  assertEquals(Object.keys(folded).sort(), ["owner", "region"]);
});

test("foldMeta uses a null-prototype dict so a __proto__ key round-trips", () => {
  const folded = foldMeta([{ key: "__proto__", value: "polluted" }]);
  // A plain `{}` would route this through Object.prototype's setter and create no
  // own property; the null-proto dict makes it a real, enumerable own key.
  assertEquals(Object.getPrototypeOf(folded), null);
  assertEquals(Object.keys(folded), ["__proto__"]);
  assertEquals(folded["__proto__"], "polluted");
  // Inherited keys are absent (no prototype chain), so a lookup misses cleanly.
  assertEquals(folded["toString"] as unknown, undefined);
});

test("emitMeta builds the AppMeta interface + a null-prototype accessor dict", () => {
  const out = emitMeta([
    { key: "classification", value: "internal" },
    { key: "owner", value: "sre" },
  ]);
  assertStringIncludes(out, "classification: string;");
  assertStringIncludes(out, "owner: string;");
  // Null-prototype construction + bracket assignment (never an object literal).
  assertStringIncludes(out, "const m: Record<string, string> = Object.create(null);");
  assertStringIncludes(out, `m["classification"] = "internal";`);
  assertStringIncludes(out, `m["owner"] = "sre";`);
  // Node strip-only safety (ADR 0065): no enums, plain JS + erased annotations.
  assertEquals(out.includes("enum "), false);
});

test("emitMeta emits an empty null-prototype accessor when no metadata is declared", () => {
  const out = emitMeta([]);
  assertStringIncludes(out, "export interface AppMeta {}");
  assertStringIncludes(out, "export const appMeta: AppMeta = Object.create(null) as AppMeta;");
});

test("emitMeta assigns a user-authored __proto__ key by bracket notation, not bare", () => {
  const out = emitMeta([{ key: "__proto__", value: "polluted" }]);
  // Bracket assignment on the null-proto dict makes __proto__ an own data property;
  // a bare `__proto__: "…"` in an object literal would instead set the prototype.
  assertStringIncludes(out, `m["__proto__"] = "polluted";`);
  assertEquals(out.includes(`__proto__: "polluted"`), false);
});

// --- the structured fused domain model: domain.json (ADR 0040 §1) ------------

test("emitDomainModelJson fuses tables, manifest types, shapes, and metadata", () => {
  const json = emitDomainModelJson({
    sources: [{
      source: "app",
      tables: [{
        name: "orders",
        columns: [{ name: "id", type: "INTEGER", notNull: true, primaryKey: true }],
        indexes: [],
        foreignKeys: [],
      }],
    }],
    default: "app",
    manifestTypes: { Order: { fields: { item: { type: "string" } } } },
    shapes: [{
      decl: { id: "ApprovedOrder", process: "orders", ops: [] },
      def: { fields: { item: { type: "string" }, approved: { type: "boolean" } } },
    }],
    meta: [{ process: "orders", key: "classification", value: "internal" }],
    diagnostics: [],
  });
  const model = JSON.parse(json);
  assertEquals(model.version, 1);
  assertEquals(model.default, "app");
  assertEquals(typeof model.inputsHash, "string");
  assertEquals(model.inputsHash.length > 0, true);
  const byId = new Map<string, { kind: string; provenance: string }>(
    model.entities.map((e: { id: string; kind: string; provenance: string }) => [e.id, e]),
  );
  assertEquals(byId.get("app.orders")!.kind, "table");
  assertEquals(byId.get("app.orders")!.provenance, "db:app.orders");
  assertEquals(byId.get("Order")!.kind, "type");
  assertEquals(byId.get("Order")!.provenance, "manifest:Order");
  assertEquals(byId.get("ApprovedOrder")!.kind, "shape");
  assertEquals(byId.get("ApprovedOrder")!.provenance, "model:orders");
  assertEquals(model.meta, [{ process: "orders", key: "classification", value: "internal" }]);
});

test("emitDomainModelJson inputsHash is stable across calls and shifts on any change", () => {
  const base = {
    sources: [],
    manifestTypes: { Order: { fields: { item: { type: "string" } } } },
    shapes: [],
    meta: [{ key: "owner", value: "sre" }],
    diagnostics: [],
  };
  const a = JSON.parse(emitDomainModelJson({ ...base }));
  const b = JSON.parse(emitDomainModelJson({ ...base }));
  assertEquals(a.inputsHash, b.inputsHash); // deterministic
  const changed = JSON.parse(
    emitDomainModelJson({ ...base, meta: [{ key: "owner", value: "ops" }] }),
  );
  assertEquals(changed.inputsHash !== a.inputsHash, true); // staleness detectable
});

// --- resolveShapes: the composition fuse + its drift guards (ADR 0040 §9/§10) --
//
// The "envelope payload types live in the model" work (a `nano:shape` composed
// purely of `extend` ops IS a flat payload record) leans on two behaviours of the
// reifier that these tests lock against regression: (1) an all-`extend` shape
// reifies field-for-field into a `DomainTypes` entry, and (2) a shape id that
// duplicates an existing fused entity it does not compose is rejected as drift
// rather than silently shadowing it ("derive, don't duplicate; drift-guard
// anything that is not derived").

test("resolveShapes reifies an all-extend shape into a flat domain type", () => {
  const shapes: ShapeDecl[] = [{
    id: "PrReviewRoundIn",
    name: "PR review round — input",
    ops: [
      { op: "extend", name: "prUrl", type: "string" },
      { op: "extend", name: "prNumber", type: "integer" },
      { op: "extend", name: "answer", type: "string", optional: true },
      { op: "extend", name: "labels", type: "string", list: true },
    ],
  }];
  const { types, diagnostics } = resolveShapes(shapes, {}, []);
  assertEquals(diagnostics, []);
  assertEquals(types.PrReviewRoundIn, {
    name: "PR review round — input",
    fields: {
      prUrl: { type: "string" },
      prNumber: { type: "integer" },
      answer: { type: "string", optional: true },
      labels: { type: "string", list: true },
    },
  });
});

test("resolveShapes drift-guards a shape id colliding with a manifest type", () => {
  // A pure-extend shape whose id also names a manifest type it does not compose is
  // duplication, not a fuse: reject it (error) and omit it, leaving the manifest
  // type as the single source of truth.
  const shapes: ShapeDecl[] = [{
    id: "Order",
    ops: [{ op: "extend", name: "item", type: "string" }],
  }];
  const manifest = { Order: { fields: { item: { type: "string" } } } };
  const { types, diagnostics } = resolveShapes(shapes, manifest, []);
  assertEquals(Object.keys(types), []); // shape omitted — no silent shadow
  assertEquals(diagnostics.length, 1);
  assertEquals(diagnostics[0].kind, "same-id-collision");
  assertEquals(diagnostics[0].severity, "error");
  assertEquals(diagnostics[0].shape, "Order");
});

test("resolveShapes rejects a duplicate shape id as a fuse-identity collision", () => {
  const shapes: ShapeDecl[] = [
    { id: "Dup", ops: [{ op: "extend", name: "a", type: "string" }] },
    { id: "Dup", ops: [{ op: "extend", name: "b", type: "string" }] },
  ];
  const { types, diagnostics } = resolveShapes(shapes, {}, []);
  assertEquals(Object.keys(types), []); // both omitted
  assertEquals(diagnostics.length, 1);
  assertEquals(diagnostics[0].kind, "duplicate-id");
  assertEquals(diagnostics[0].severity, "error");
});

test("resolveShapes flags an extend whose type is neither scalar nor a fused entity", () => {
  const shapes: ShapeDecl[] = [{
    id: "Bad",
    ops: [{ op: "extend", name: "ref", type: "NoSuchType" }],
  }];
  const { types, diagnostics } = resolveShapes(shapes, {}, []);
  assertEquals(Object.keys(types), []); // broken shape omitted
  assertEquals(diagnostics.length, 1);
  assertEquals(diagnostics[0].kind, "unresolved-reference");
  assertEquals(diagnostics[0].severity, "error");
});

test("resolveShapes returns an empty resolution for no shapes", () => {
  const { types, diagnostics } = resolveShapes([], { Order: { fields: { item: { type: "string" } } } }, []);
  assertEquals(types, {});
  assertEquals(diagnostics, []);
});
