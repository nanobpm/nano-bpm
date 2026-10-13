// nanobpmn datasource CLI (Node runtime) — ADR 0024 phase-2
// (DB Manager gateway); Node-only per ADR 0065.
//
// Materialised verbatim to <project>/nano-generated/data-cli.ts next to data-sdk.ts.
// The Rust console server invokes it as a one-shot subprocess to serve the
// console **Data** panel (Tables / SQL / Migrations). It is the panel's bridge
// to the datasource seam: every operation runs THROUGH `@nanobpm/data`
// (`./data-sdk.ts`), so the panel browses whatever the named datasource
// currently resolves to — embedded SQLite in the IDE, a server DB in
// production once a `nano-ide-data-*` driver pack is installed — never a
// parallel, SQLite-only path (ADR 0024 §4).
//
// Protocol: a single JSON request object is read from stdin; a single JSON
// response object is written to stdout and the process exits 0. Handled errors
// are reported as `{ ok: false, error }` (still exit 0) so the caller can parse
// them; only an unreadable request exits non-zero. The process cwd is the
// project root, so `openDataSource`/`listSources` discover `nano.app.json`
// there and file-backed sources resolve against it.
//
//   Request:  { op, source?, sql?, params?, statements? }
//   op = "sources" | "schema" | "query" | "exec" | "script" | "migrations"
//      | "migrate" | "domaintypes"

import { isReadStatement, listSources, manifestTypes, manifestWorkers, openDataSource, type WorkerDecl } from "./data-sdk.ts";
import {
  DOMAIN_BINDINGS,
  DOMAIN_DTS,
  DOMAIN_MODEL_JSON,
  type DomainTypeDef,
  type DomainTypeRegistry,
  emitDomainBindings,
  emitDomainModel,
  emitDomainModelJson,
  emitMessageBindings,
  emitMessageBindingsRuntime,
  emitMeta,
  emitWorkerBindings,
  emitWorkerBindingsRuntime,
  GEN_DIR,
  MESSAGE_BINDINGS_DTS,
  MESSAGE_BINDINGS_TS,
  type MessageBindingDecl,
  META_TS,
  type MetaDecl,
  resolveShapes,
  type ShapeDecl,
  type SourceSchema,
  WORKER_BINDINGS_DTS,
  WORKER_BINDINGS_TS,
} from "./domain-types.ts";

interface Request {
  op: string;
  source?: string;
  sql?: string;
  params?: unknown[];
  statements?: string[];
  /** `domaintypes`: also write `nano-generated/domain-rows.d.ts` (default true). */
  write?: boolean;
  /**
   * `domaintypes`: the model-derived worker-IO map (`taskType -> {in,out}`),
   * scanned from the process models by the server (ADR 0040 slice 1 / ADR 0033
   * §6 increment 12). When present, it is the authoritative source of each
   * worker's envelope types; it is overlaid on the manifest `workers[]` (which
   * still carries non-IO bindings such as `llm`). Absent → fall back to the
   * manifest projection alone.
   */
  derivedWorkers?: WorkerDecl[];
  /**
   * `domaintypes`: the model-derived message-payload map (`messageName ->
   * {in,out}`), scanned from the process models' `bpmn:message` envelopes by the
   * server (ADR 0040 slice 2). It is authoritative for the typed `publishMessage`
   * registry (there is no manifest projection for messages, unlike `workers[]`),
   * so it is emitted directly. Absent → no message registry is derived.
   */
  derivedMessages?: MessageBindingDecl[];
  /**
   * `domaintypes`: the model-derived composed shapes (`nano:shape` declarations),
   * scanned from the process models by the server (ADR 0040 §9/§10). Each is
   * resolved through the fuse into a flat domain type and folded into the manifest
   * `types` registry, so a composed shape becomes a first-class `DomainTypes` entry
   * every consumer types against. Absent → no shapes are resolved.
   */
  derivedShapes?: ShapeDecl[];
  /**
   * `domaintypes`: the model-derived model-level metadata (`nano:meta` key/value
   * siblings of `nano:shapes`), scanned from the process models by the server (ADR
   * 0040 §5). Folded into the typed `meta.ts` accessor (`@nanobpm/meta`) and the
   * structured fuse (`domain.json`), tagged with `model:<processId>` provenance.
   * Absent → no metadata is derived.
   */
  derivedMeta?: MetaDecl[];
}

/**
 * Overlay the model-derived worker-IO map onto the manifest `workers[]`. The
 * model is authoritative for the envelope (`inputType`/`outputType`), so every
 * scanned `taskType` takes its I/O from `derived` (clearing a stale manifest
 * value when the model carries none); manifest entries the scan did not cover
 * (e.g. a worker declared only for an `llm` binding, with no service task) are
 * preserved. New `taskType`s seen only in the model are appended.
 */
export function overlayDerivedWorkerIo(
  manifest: WorkerDecl[],
  derived: WorkerDecl[],
): WorkerDecl[] {
  const byType = new Map<string, WorkerDecl>();
  for (const w of manifest) byType.set(w.taskType, { ...w });
  for (const d of derived) {
    const existing = byType.get(d.taskType);
    const merged: WorkerDecl = existing
      ? { ...existing, taskType: d.taskType }
      : { taskType: d.taskType };
    if (d.inputType) merged.inputType = d.inputType;
    else delete merged.inputType;
    if (d.outputType) merged.outputType = d.outputType;
    else delete merged.outputType;
    // Header keys are authored on the model, so the derived set is authoritative:
    // replace (not union) the manifest's, and clear when the model declares none.
    if (d.headerKeys && d.headerKeys.length > 0) merged.headerKeys = [...d.headerKeys];
    else delete merged.headerKeys;
    byType.set(d.taskType, merged);
  }
  return [...byType.values()];
}

// Runtime adapter: the host calls the CLI needs (stdin, readDir, file IO, exit).
// Deno support was removed in ADR 0065 — Node's `process` / `node:fs` is the
// only runtime. `data-sdk.ts` carries the same Node-only shim for
// SQLite/`node:sqlite`; this closes the gateway's own host calls so the Data
// panel works on Node.
interface DirEntry {
  name: string;
  isFile: boolean;
}
interface CliRuntime {
  readStdin(): Promise<string>;
  readDir(dir: string): Promise<DirEntry[]>;
  readTextFile(path: string): Promise<string>;
  mkdir(path: string): Promise<void>;
  writeTextFile(path: string, data: string): Promise<void>;
  exit(code: number): never;
}

function concatDecode(chunks: Uint8Array[]): string {
  let len = 0;
  for (const c of chunks) len += c.length;
  const buf = new Uint8Array(len);
  let off = 0;
  for (const c of chunks) {
    buf.set(c, off);
    off += c.length;
  }
  return new TextDecoder().decode(buf);
}

const RT: CliRuntime = ((): CliRuntime => {
  const p = (globalThis as unknown as { process: NonNullable<NodeJS.Process> }).process;
  return {
    readStdin: async () => {
      const chunks: Uint8Array[] = [];
      for await (const c of p.stdin) chunks.push(c as Uint8Array);
      return concatDecode(chunks);
    },
    readDir: async (dir) => {
      const { readdir } = await import("node:fs/promises");
      const ents = await readdir(dir, { withFileTypes: true });
      return ents.map((e) => ({ name: e.name, isFile: e.isFile() }));
    },
    readTextFile: async (path) => (await import("node:fs/promises")).readFile(path, "utf8"),
    mkdir: async (path) => {
      await (await import("node:fs/promises")).mkdir(path, { recursive: true });
    },
    writeTextFile: async (path, data) => {
      await (await import("node:fs/promises")).writeFile(path, data, "utf8");
    },
    exit: (c) => p.exit(c),
  };
})();

/// JSON can't carry a bigint (SQLite INTEGER/rowid may exceed 2^53). Coerce any
/// bigint a driver returns to a Number when it is safe, else a decimal string,
/// recursively, so the response always serialises.
function jsonSafe(v: unknown): unknown {
  if (typeof v === "bigint") {
    return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(v)
      : v.toString();
  }
  if (v instanceof Uint8Array) return { $blob: v.length };
  if (Array.isArray(v)) return v.map(jsonSafe);
  if (v && typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(v)) out[k] = jsonSafe(val);
    return out;
  }
  return v;
}

/// The union of keys seen across the result rows, in first-seen order — the
/// column set a grid renders. SQLite's `.all()` omits a column set for an empty
/// result, so an empty query yields `[]`; that is honest for a table browser.
function columnsOf(rows: Record<string, unknown>[]): string[] {
  const seen = new Set<string>();
  const cols: string[] = [];
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (!seen.has(k)) {
        seen.add(k);
        cols.push(k);
      }
    }
  }
  return cols;
}

const MIGRATIONS_TABLE = "_nano_migrations";

/// True when this process is running inside an Urban-shaped app — i.e. a
/// `nano.app.json` manifest sits at the project root (the process cwd). For such
/// apps the `@nanobpm/urban` runtime owns schema migration (it keeps its own
/// `_urban_migrations` ledger), so the console must not run a second, competing
/// migration pass on the run path. See the `domaintypes` op and #673.
async function isUrbanShapedApp(): Promise<boolean> {
  try {
    for (const e of await RT.readDir(".")) {
      if (e.isFile && e.name === "nano.app.json") return true;
    }
  } catch {    // unreadable cwd — treat as not-Urban (fall through to legacy behaviour)
  }
  return false;
}

/// Split a migration file into individual statements on `;` boundaries,
/// dropping blank and comment-only fragments. A minimal splitter for the
/// common DDL case (ADR 0024's "minimal dialect stance"); it does not parse
/// semicolons inside string literals.
function splitStatements(sql: string): string[] {
  return sql
    .split(";")
    .map((s) => s.trim())
    .filter((s) => s.length > 0 && !s.split("\n").every((l) => l.trim().startsWith("--")));
}

async function migrationDir(source: string): Promise<string> {
  const { sources, default: def } = await listSources();
  const name = source || def || sources[0]?.name;
  const src = sources.find((s) => s.name === name);
  return src?.migrations ?? "db/migrations";
}

/// Ordered `*.sql` files in the source's migrations directory (empty when the
/// directory is absent — a project may declare no migrations).
async function listMigrationFiles(dir: string): Promise<string[]> {
  const names: string[] = [];
  try {
    for (const e of await RT.readDir(dir)) {
      if (e.isFile && e.name.endsWith(".sql")) names.push(e.name);
    }
  } catch {    // no migrations directory — treat as an empty set
  }
  names.sort();
  return names;
}

/// Apply a source's not-yet-applied `*.sql` migrations, in filename order, each
/// in its own transaction, recording every applied file in the migrations
/// table. Returns the names applied (empty when already up to date). Idempotent.
///
/// Shared by the `migrate` op and the `domaintypes` regen: the latter introspects
/// the *live* schema (`db.schema()`), so on a fresh DB a regen that runs before
/// `migrate` would introspect zero tables and emit an empty `Domain` (only
/// `raw`+`close`). Ensuring the schema is migrated first keeps codegen and the DB
/// in step.
async function applyPendingMigrations(source: string | undefined): Promise<string[]> {
  const dir = await migrationDir(source ?? "");
  const files = await listMigrationFiles(dir);
  const db = await openDataSource(source);
  try {
    await db.exec(
      `CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`,
    );
    const done = new Set(
      (await db.query(`SELECT name FROM ${MIGRATIONS_TABLE}`)).map((r) =>
        String(r.name)
      ),
    );
    const applied: string[] = [];
    for (const name of files) {
      if (done.has(name)) continue;
      const sql = await RT.readTextFile(`${dir}/${name}`);
      await db.tx(async (t) => {
        for (const stmt of splitStatements(sql)) await t.exec(stmt);
        await t.exec(
          `INSERT INTO ${MIGRATIONS_TABLE} (name, applied_at) VALUES (?, ?)`,
          [name, new Date().toISOString()],
        );
      });
      applied.push(name);
    }
    return applied;
  } finally {
    db.close();
  }
}

async function run(req: Request): Promise<unknown> {
  switch (req.op) {
    case "sources": {
      const { sources, default: def } = await listSources();
      return { default: def, sources };
    }
    case "schema": {
      const db = await openDataSource(req.source);
      return { tables: await db.schema() };
    }
    case "query": {
      // The `query` op is allowlisted while the app is running, so it must be
      // read-only: `db.query` runs `prepare(sql).all(...)`, which happily
      // executes a mutating `INSERT/UPDATE/DELETE … RETURNING` or a
      // data-modifying CTE and returns its rows. Reject any non-read statement
      // here (using the canonical `isReadStatement` split) so a client cannot
      // launder a write through the read path and past the running-app gate.
      const sql = req.sql ?? "";
      if (!isReadStatement(sql)) {
        throw new Error(
          "query op accepts read-only statements only; use the exec op for a mutation",
        );
      }
      const db = await openDataSource(req.source);
      const rows = (await db.query(sql, req.params ?? [])) as Record<
        string,
        unknown
      >[];
      return { columns: columnsOf(rows), rows: jsonSafe(rows) };
    }
    case "exec": {
      const db = await openDataSource(req.source);
      const r = await db.exec(req.sql ?? "", req.params ?? []);
      return jsonSafe(r);
    }
    case "script": {
      // Run several statements atomically in one transaction — the console's
      // structure editor uses this for the SQLite 12-step table rebuild
      // (create → copy → drop → rename), so a mid-rebuild failure rolls back
      // and leaves the table untouched. Statements come pre-split from the
      // caller; `?`-params are not threaded (DDL needs none).
      const db = await openDataSource(req.source);
      const statements = Array.isArray(req.statements)
        ? req.statements
        : splitStatements(req.sql ?? "");
      let changed = 0;
      await db.tx(async (t) => {
        for (const stmt of statements) {
          if (stmt.trim()) changed += (await t.exec(stmt)).changed;
        }
      });
      return jsonSafe({ changed });
    }
    case "migrations": {
      const dir = await migrationDir(req.source ?? "");
      const files = await listMigrationFiles(dir);
      const db = await openDataSource(req.source);
      await db.exec(
        `CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`,
      );
      const applied = new Map<string, string>(
        (await db.query(`SELECT name, applied_at FROM ${MIGRATIONS_TABLE}`)).map((
          r,
        ) => [String(r.name), String(r.applied_at)]),
      );
      return {
        dir,
        entries: files.map((name) => ({
          name,
          applied: applied.has(name),
          appliedAt: applied.get(name) ?? null,
        })),
      };
    }
    case "migrate": {
      const applied = await applyPendingMigrations(req.source);
      return { applied, pending: 0 };
    }
    case "domaintypes": {
      // ADR 0029 §4.1/§4.2/§6: reify the domain model into TypeScript. Union
      // *every* declared datasource (the table spine) and fold in the manifest
      // `types` registry (transient/non-persisted shapes) so an App with
      // multiple databases plus declared types gets one complete domain model,
      // emit `domain-rows.d.ts`, and (unless `write:false`) materialise it to
      // `nano-generated/domain-rows.d.ts` next to the SDK so workers type against the live
      // DBs. cwd is the project root, so the relative path lands in the project.
      const { default: def, sources } = await listSources();
      // Ensure every source's schema is migrated *before* introspecting it — the
      // domain model is derived from the live `db.schema()`, so a regen that
      // races ahead of `migrate` on a fresh DB would emit an empty `Domain`.
      // Gated on a real (persisting) regen: the `write:false` composer preview is
      // latency-sensitive and must stay read-only (no DB side-effects per
      // keystroke).
      //
      // Also gated off for an Urban-shaped app (a `nano.app.json` at the project
      // root — the process cwd): there the `@nanobpm/urban` runtime (`urban run`)
      // owns schema migration and records it in its own ledger. This op runs on
      // the run path (the supervisor's boot hook regenerates domain types before
      // starting the app), so auto-migrating here would apply the app's migrations
      // a *second* time, into the console's separate `_nano_migrations` ledger —
      // and a non-idempotent `ALTER TABLE … ADD COLUMN` in a later migration then
      // crashes the `urban run` pass with "duplicate column" (one DB, two ledgers;
      // Magikcraft/nano-bpm#673). One owner, one ledger: for Urban apps, urban
      // migrates. Domain types for Urban apps come from `urban gen` (derived from
      // the manifest, not the live schema), so skipping the console's schema-first
      // regen here costs nothing. The explicit `migrate` op is unaffected — it is
      // a user-initiated authoring action, not part of the run path.
      const migrated: Record<string, string[]> = {};
      if (req.write !== false && !(await isUrbanShapedApp())) {
        for (const s of sources) {
          const applied = await applyPendingMigrations(s.name);
          if (applied.length) migrated[s.name] = applied;
        }
      }
      const schemas: SourceSchema[] = [];
      for (const s of sources) {
        const db = await openDataSource(s.name);
        try {
          schemas.push({ source: s.name, tables: await db.schema() });
        } finally {
          db.close();
        }
      }
      const types = await manifestTypes() as DomainTypeRegistry;
      // Composed motion shapes (ADR 0040 §9/§10): resolve each `nano:shape` scanned
      // from the model against the leaf fuse (DB tables + manifest types) into a flat
      // domain type, and fold the resolved shapes into the registry *before* it feeds
      // the domain model and the worker/message bindings. A composed shape thus becomes
      // a first-class `DomainTypes` entry, so envelopes/workers/messages can reference
      // it with no extra codegen path. Broken shapes are omitted; their diagnostics are
      // returned so the panel can surface them like the `workers[]` drift warning.
      const shapeResolution = resolveShapes(req.derivedShapes ?? [], types, schemas);
      // Snapshot the manifest `types` *before* folding the resolved shapes, so
      // `domain.json` can tag each entity with its true provenance (manifest vs
      // model) — once folded they are indistinguishable in `types`.
      const manifestTypesSnapshot: DomainTypeRegistry = { ...types };
      const shapeEntities: { decl: ShapeDecl; def: DomainTypeDef }[] = [];
      const shapeDeclById = new Map((req.derivedShapes ?? []).map((s) => [s.id, s]));
      for (const [id, def] of Object.entries(shapeResolution.types)) {
        types[id] = def;
        shapeEntities.push({ decl: shapeDeclById.get(id) ?? { id, ops: [] }, def });
      }
      const text = emitDomainModel(schemas, def, types);
      // The typed data-object accessor (`db.orders.insert(...)`, ADR 0029 §6) is
      // generated alongside the `.d.ts` spine so workers get both the row types
      // and the runtime gateway from one op.
      const bindings = emitDomainBindings(schemas, def);
      // The model-level metadata accessor (`@nanobpm/meta`, ADR 0040 §5): the
      // `nano:meta` key/values the models declare, folded into a typed `meta.ts`.
      const derivedMeta = req.derivedMeta ?? [];
      const metaAccessor = emitMeta(derivedMeta);
      // The structured Fused Domain Model (`domain.json`, ADR 0040 §1): a fast-read
      // cache of every fused entity + metadata + diagnostics for the IDE/codegen.
      // Computed only when persisting: it is the largest emitted artifact (every
      // fused entity's fields + an FNV-1a hash over the whole model) and the
      // latency-sensitive composer preview (`write:false`) consumes only `text` +
      // `shapeDiagnostics`, so a debounced keystroke must not pay to build it.
      // The worker-IO map (ADR 0033 §3): `taskType → {in,out}`. The model is the
      // source of truth for the envelope, so when the server injected the
      // model-derived map (`derivedWorkers`) it is overlaid on the manifest
      // `workers[]` (which still carries non-IO bindings). This retires the
      // hand-maintained projection as the authority (ADR 0040 slice 1). The
      // runtime wrapper (`workers.ts`) is static — written verbatim.
      const manifestW = await manifestWorkers();
      const workers = req.derivedWorkers
        ? overlayDerivedWorkerIo(manifestW, req.derivedWorkers)
        : manifestW;
      const workerBindings = emitWorkerBindings(workers, Object.keys(types));
      const workerRuntime = emitWorkerBindingsRuntime();
      // The message-payload registry (ADR 0040 slice 2): `messageName → {in,out}`,
      // scanned from the process models' `bpmn:message` envelopes. There is no
      // manifest projection for messages, so the model-derived map is authoritative
      // and emitted directly (no overlay). Feeds the typed `publishMessage`; the
      // runtime wrapper (`messages.ts`) is static — written verbatim.
      const messages = req.derivedMessages ?? [];
      const messageBindings = emitMessageBindings(messages, Object.keys(types));
      const messageRuntime = emitMessageBindingsRuntime();
      let path: string | null = null;
      let bindingsPath: string | null = null;
      let workerBindingsPath: string | null = null;
      let messageBindingsPath: string | null = null;
      let metaPath: string | null = null;
      let domainModelPath: string | null = null;
      let domainModelJson: string | null = null;
      if (req.write !== false) {
        await RT.mkdir(GEN_DIR);
        path = `${GEN_DIR}/${DOMAIN_DTS}`;
        await RT.writeTextFile(path, text);
        bindingsPath = `${GEN_DIR}/${DOMAIN_BINDINGS}`;
        await RT.writeTextFile(bindingsPath, bindings);
        workerBindingsPath = `${GEN_DIR}/${WORKER_BINDINGS_DTS}`;
        await RT.writeTextFile(workerBindingsPath, workerBindings);
        await RT.writeTextFile(`${GEN_DIR}/${WORKER_BINDINGS_TS}`, workerRuntime);
        messageBindingsPath = `${GEN_DIR}/${MESSAGE_BINDINGS_DTS}`;
        await RT.writeTextFile(messageBindingsPath, messageBindings);
        await RT.writeTextFile(`${GEN_DIR}/${MESSAGE_BINDINGS_TS}`, messageRuntime);
        metaPath = `${GEN_DIR}/${META_TS}`;
        await RT.writeTextFile(metaPath, metaAccessor);
        domainModelJson = emitDomainModelJson({
          sources: schemas,
          default: def,
          manifestTypes: manifestTypesSnapshot,
          shapes: shapeEntities,
          meta: derivedMeta,
          diagnostics: shapeResolution.diagnostics,
        });
        domainModelPath = `${GEN_DIR}/${DOMAIN_MODEL_JSON}`;
        await RT.writeTextFile(domainModelPath, domainModelJson);
      }
      const tables = schemas.reduce((n, s) => n + s.tables.length, 0);
      return {
        path,
        text,
        tables,
        bindingsPath,
        bindings,
        workerBindingsPath,
        workerBindings,
        messageBindingsPath,
        messageBindings,
        metaPath,
        meta: metaAccessor,
        domainModelPath,
        domainModel: domainModelJson,
        migrated,
        shapeDiagnostics: shapeResolution.diagnostics,
      };
    }
    default:
      throw new Error(`unknown op "${req.op}"`);
  }
}

async function main(): Promise<void> {
  let req: Request;
  try {
    req = JSON.parse(await RT.readStdin()) as Request;
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: `bad request: ${(e as Error).message}` }));
    RT.exit(1);
  }
  try {
    const result = await run(req);
    console.log(JSON.stringify({ ok: true, ...(result as object) }));
  } catch (e) {
    console.log(JSON.stringify({ ok: false, error: (e as Error).message }));
  }
}

await main();
