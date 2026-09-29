// nanobpmn embedded datasource SDK (Deno-preferred, Node-capable) — ADR 0024.
//
// Materialised verbatim to <workspace>/<project>/nano-generated/data-sdk.ts and
// imported as `@nanobpm/data` (or `./data-sdk.ts` from the sibling worker SDK).
// It is the runtime half of Urban's "BDE alias": a named, swappable data
// connection. Consumers bind to a datasource BY NAME (`data.app`), never by
// driver, so the same App bundle runs on embedded SQLite in the IDE and — once
// a `nano-ide-data-*` driver pack is installed — on a server database in
// production by flipping `NANO_APP_DB_*` env, with no source change.
//
//   import { openDataSource } from "@nanobpm/data";
//   const db = await openDataSource();          // the manifest's default source
//   await db.exec("INSERT INTO orders(id) VALUES (?)", [id]);
//   const rows = await db.query("SELECT * FROM orders");
//
// Or, inside a worker handler, via the injected context:
//
//   defineWorker({ type: "save", async handle(job, ctx) {
//     const db = await ctx.data("app");
//     await db.exec("INSERT INTO orders(id) VALUES (?)", [job.variables.id]);
//   }});
//
// Core ships SQLite only (the `node:sqlite` built-in — embedded, single-file —
// present in both Deno and Node >= 22.5). Other drivers arrive as ADR 0007 packs
// on the `nano-ide-data-*` axis; an unknown driver throws a pack-install hint.

// Runtime adapter: the few host calls that differ between Deno (native `Deno.*`)
// and Node (`process` / `node:fs`). Detected once at load; see ADR 0036.
interface DataRuntime {
  cwd(): string;
  env(key: string): string | undefined;
  readTextFile(path: string): Promise<string>;
}
const RT: DataRuntime = ((): DataRuntime => {
  const g = globalThis as unknown as {
    Deno?: {
      cwd(): string;
      env: { get(k: string): string | undefined };
      readTextFile(p: string): Promise<string>;
    };
    process?: { cwd(): string; env: Record<string, string | undefined> };
  };
  if (g.Deno) {
    const d = g.Deno;
    return { cwd: () => d.cwd(), env: (k) => d.env.get(k), readTextFile: (p) => d.readTextFile(p) };
  }
  const p = g.process!;
  return {
    cwd: () => p.cwd(),
    env: (k) => p.env[k],
    readTextFile: async (path) => (await import("node:fs/promises")).readFile(path, "utf8"),
  };
})();

/** One column of a table, from the datasource's introspected schema. */
export interface ColumnMeta {
  name: string;
  type: string;
  notNull: boolean;
  primaryKey: boolean;
}

/** One foreign-key constraint: `column` in this table references
 * `refTable(refColumn)`. `refColumn` is empty when the FK targets the parent's
 * primary key without naming a column. `onDelete` is the referential action
 * (e.g. `CASCADE`), empty when none was declared. */
export interface ForeignKeyMeta {
  column: string;
  refTable: string;
  refColumn: string;
  onDelete: string;
}

/** One table: its columns, the names of its indexes, and its foreign keys.
 * Powers the DB Manager, form data-binding, and the ADR 0029 domain-type ↔ table
 * projection. */
export interface TableMeta {
  name: string;
  columns: ColumnMeta[];
  indexes: string[];
  foreignKeys: ForeignKeyMeta[];
}

export type Row = Record<string, unknown>;

export interface ExecResult {
  /** Rows changed by an INSERT/UPDATE/DELETE. */
  changed: number;
  /** Rowid of the last inserted row, when the driver reports one. */
  lastInsertId?: number | bigint;
}

/// Strip leading whitespace and any leading SQL comments (`-- …` line comments
/// and `/* … */` block comments) so the read/write classifier can see the real
/// first token. Only *leading* comments are removed; the remainder is left
/// intact so the body scans (mutating verb inside a CTE, assigning/call-form
/// PRAGMA) are unaffected. An unterminated comment consumes the rest of the
/// input, which then classifies as a (safe) write.
function stripLeadingSqlComments(sql: string): string {
  let s = sql.trimStart();
  for (;;) {
    if (s.startsWith("--")) {
      const nl = s.indexOf("\n");
      s = nl === -1 ? "" : s.slice(nl + 1);
    } else if (s.startsWith("/*")) {
      const end = s.indexOf("*/");
      s = end === -1 ? "" : s.slice(end + 2);
    } else {
      break;
    }
    s = s.trimStart();
  }
  return s;
}

/// Whether `sql` is a pure *read* (safe to serve through the row-returning
/// `query` op, and to allow while the app is running). This is the **canonical**
/// server data-gateway read-vs-write split (issue #889): the console's
/// `console/src/lib/sqlStatement.ts` is an explicit mirror of it, so the client
/// routing and this server gate can never drift.
///
/// Conservative by construction — anything not provably read-only is a write, so
/// the dangerous permissive failure (mis-classifying a mutation as a read, e.g.
/// `INSERT … RETURNING`, a data-modifying CTE, or an assigning `PRAGMA`) can
/// never slip a write down the `query` path and past the running-app edit gate:
/// - `SELECT` / `EXPLAIN` are always reads.
/// - A CTE (`WITH …`) is a read only when it carries no mutating verb; a
///   `WITH … INSERT/UPDATE/DELETE/REPLACE …` statement mutates.
/// - `PRAGMA name` reads a setting, but `PRAGMA name = value` mutates. The
///   call form `PRAGMA name(x)` is often a read (e.g. `table_info(t)`); we
///   still treat any `=`/`(` as a write, conservatively erring toward the safe
///   classification rather than enumerating the read-only call-form pragmas.
/// - Everything else (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, …) is a
///   write.
///
/// Leading SQL comments (`-- …` line and `/* … */` block) are stripped before
/// the leading-verb check, so a commented read (`-- note\nSELECT 1`) is still a
/// read rather than being misclassified as a write.
export function isReadStatement(sql: string): boolean {
  const s = stripLeadingSqlComments(sql);
  if (/^(select|explain)\b/i.test(s)) return true;
  if (/^with\b/i.test(s)) {
    // A CTE that contains any mutating verb is a data-modifying statement.
    return !/\b(insert|update|delete|replace)\b/i.test(s);
  }
  if (/^pragma\b/i.test(s)) {
    // `=` (assignment) or `(` (call form) makes the PRAGMA a write.
    return !/[=(]/.test(s);
  }
  return false;
}

/// The one thin, uniform interface behind every driver (ADR 0024 §2) — the
/// `TDataSet` equivalent. The driver underneath is interchangeable because every
/// consumer shares exactly this surface.
export interface DataSource {
  /** Run a SELECT (or any row-returning statement) and collect the rows. */
  query(sql: string, params?: unknown[]): Promise<Row[]>;
  /** Run a non-row statement (INSERT/UPDATE/DELETE/DDL). */
  exec(sql: string, params?: unknown[]): Promise<ExecResult>;
  /** Run `fn` inside a transaction, committing on success and rolling back on
   * throw. The handle passed to `fn` targets the same connection. */
  tx<T>(fn: (t: DataSource) => Promise<T>): Promise<T>;
  /** Introspect the datasource's tables/columns/indexes. */
  schema(): Promise<TableMeta[]>;
  /** A typed gateway over one table — the RAD "TTable": manipulate rows as typed
   * records instead of hand-writing SQL. The row type comes from the generated
   * `domain-rows.d.ts` (ADR 0029); `pk` is the primary-key column (default "id"). */
  table<T extends object = Row>(name: string, pk?: string): Table<T>;
  /** Close the underlying connection. */
  close(): void;
}

// --- env-template resolution (the alias flip) ------------------------------

/// Expand `${VAR}` and `${VAR:-default}` against `env`. An unset or empty var
/// falls back to the `:-default` (or "" when none is given). This is what lets a
/// manifest `driver`/`url` read `${NANO_APP_DB_DRIVER:-sqlite}` and become
/// Postgres in production purely from the environment (ADR 0024 §1).
export function resolveEnvTemplate(
  tpl: string,
  env: (key: string) => string | undefined,
): string {
  return tpl.replace(
    /\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}/g,
    (_m, name: string, dflt: string | undefined) => {
      const v = env(name);
      if (v !== undefined && v !== "") return v;
      return dflt ?? "";
    },
  );
}

interface RawSource {
  driver: unknown;
  url: string;
  migrations?: string;
}

interface ManifestData {
  default?: string;
  sources: Record<string, RawSource>;
}

export interface ResolvedSource {
  name: string;
  driver: string;
  url: string;
  migrations?: string;
}

/// Resolve one raw manifest source (env-templated `driver`/`url`) to concrete
/// values against `env`.
export function resolveSource(
  name: string,
  raw: RawSource,
  env: (key: string) => string | undefined,
): ResolvedSource {
  const driver = typeof raw.driver === "string"
    ? resolveEnvTemplate(raw.driver, env)
    : String(raw.driver);
  return {
    name,
    driver,
    url: resolveEnvTemplate(raw.url, env),
    migrations: raw.migrations,
  };
}

/// List every datasource the manifest declares (resolved against the
/// environment), plus the `default` source name. Powers the DB Manager's
/// datasource picker — it enumerates the aliases without opening a connection.
export async function listSources(
  cwd?: string,
): Promise<{ default?: string; sources: ResolvedSource[] }> {
  const { data } = await findManifest(cwd ?? RT.cwd());
  const env = (k: string) => RT.env(k);
  const sources = Object.entries(data.sources).map(([name, raw]) =>
    resolveSource(name, raw, env)
  );
  return { default: data.default, sources };
}

// --- manifest discovery ----------------------------------------------------

interface ManifestLocation {
  root: string;
  data: ManifestData;
  /** The raw `types` registry block (ADR 0029 §4.2), or `{}` when absent. */
  types: Record<string, unknown>;
  /** The raw `workers` array (ADR 0022 §E / ADR 0033 §3), or `[]` when absent. */
  workers: WorkerDecl[];
}

/** One `workers[]` entry, narrowed to the fields the typed-worker codegen reads
 * (ADR 0033 §3). `inputType`/`outputType` name declared domain types. */
export interface WorkerDecl {
  taskType: string;
  inputType?: string;
  outputType?: string;
  /** The `zeebe:header` keys declared on the task (ADR 0033 §3). Model-derived;
   * reified into a typed `job.customHeaders` shape (known keys, `string` values). */
  headerKeys?: string[];
}

/// Walk up from `startDir` to the first directory containing `nano.app.json` and
/// return that directory (the project root) plus its `data` block. Workers run
/// with their cwd inside `workers/<name>/`, so the manifest sits above them.
async function findManifest(startDir: string): Promise<ManifestLocation> {
  let dir = startDir.replace(/\/+$/, "");
  for (let i = 0; i < 12; i++) {
    try {
      const text = await RT.readTextFile(`${dir}/nano.app.json`);
      const json = JSON.parse(text) as {
        data?: ManifestData;
        types?: Record<string, unknown>;
        workers?: WorkerDecl[];
      };
      const data = json.data ?? { sources: {} };
      return {
        root: dir,
        data: { default: data.default, sources: data.sources ?? {} },
        types: json.types ?? {},
        workers: Array.isArray(json.workers) ? json.workers : [],
      };
    } catch {
      // not here — keep walking up
    }
    const slash = dir.lastIndexOf("/");
    if (slash <= 0) break;
    const parent = dir.slice(0, slash);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(
    `nano.app.json not found at or above ${startDir}; datasources require an Urban manifest`,
  );
}

/// The manifest's domain-type registry (ADR 0029 §4.2): the transient/declared
/// shapes with no backing table. Returns `{}` when the manifest declares none.
/// The domain-type reifier folds these in alongside the datasource table spine.
export async function manifestTypes(
  cwd?: string,
): Promise<Record<string, unknown>> {
  return (await findManifest(cwd ?? RT.cwd())).types;
}

/// The manifest's `workers` declarations (ADR 0033 §3), narrowed to the fields
/// the typed-worker codegen reads (`taskType` + `inputType`/`outputType`).
/// Returns `[]` when the manifest declares no workers.
export async function manifestWorkers(
  cwd?: string,
): Promise<WorkerDecl[]> {
  return (await findManifest(cwd ?? RT.cwd())).workers;
}

/// Turn a datasource `url` into a filesystem path for file-backed drivers.
/// Accepts `file:./app.db`, `file:app.db`, `file:/abs/app.db`, a bare relative
/// or absolute path, or `:memory:`. Relative paths resolve against the project
/// root so `file:./app.db` is the same file wherever the consumer's cwd is.
export function sqlitePath(url: string, root: string): string {
  let p = url.startsWith("file:") ? url.slice("file:".length) : url;
  if (p === "" || p === ":memory:") return ":memory:";
  while (p.startsWith("./")) p = p.slice(2);
  if (!p.startsWith("/")) p = `${root}/${p}`;
  return p;
}

// --- SQLite driver (core) --------------------------------------------------

import { DatabaseSync } from "node:sqlite";

/** How long a connection waits for a contended SQLite lock before failing (#1287). */
export const SQLITE_BUSY_TIMEOUT_MS = 5000;

function quoteIdent(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

// --- typed table gateway (the RAD "TTable") --------------------------------

/** Build a parameterised ` WHERE a = ? AND b = ?` clause from an equality map;
 * an empty map yields an empty clause (matches all rows). Takes `object` (not
 * `Row`) so a `Partial<T>` for a generated `interface` row type (which lacks a
 * string index signature) is accepted; keys/values are read via a `Row` cast. */
function whereClause(where: object): { clause: string; params: unknown[] } {
  const w = where as Row;
  const keys = Object.keys(w);
  if (keys.length === 0) return { clause: "", params: [] };
  const clause = " WHERE " +
    keys.map((k) => `${quoteIdent(k)} = ?`).join(" AND ");
  return { clause, params: keys.map((k) => w[k]) };
}

/// A typed gateway over a single table — the record-oriented data object a RAD
/// worker binds to instead of hand-writing SQL (the Delphi `TTable`/data-module
/// idea, ADR 0029 §6). It builds parameterised SQL from a typed row object's own
/// keys, so callers manipulate rows as records. `T` comes from the generated
/// `domain-rows.d.ts`; this class is generic *runtime* and knows nothing about any
/// specific schema, so it stays a plain dual-runtime (Node + Deno) module — no
/// codegen, no Deno-only APIs. `pk` is the primary-key column (default `id`).
export class Table<T extends object = Row> {
  readonly name: string;
  readonly pk: string;
  #src: DataSource;

  constructor(src: DataSource, name: string, pk = "id") {
    this.#src = src;
    this.name = name;
    this.pk = pk;
  }

  /** Insert one row (only the present keys are written); returns the new
   * primary-key value (the inserted rowid for an INTEGER PRIMARY KEY). */
  async insert(row: Partial<T>): Promise<number | bigint> {
    const keys = Object.keys(row);
    if (keys.length === 0) {
      throw new Error(`Table(${this.name}).insert: no columns to insert`);
    }
    const cols = keys.map(quoteIdent).join(", ");
    const ph = keys.map(() => "?").join(", ");
    const r = await this.#src.exec(
      `INSERT INTO ${quoteIdent(this.name)} (${cols}) VALUES (${ph})`,
      keys.map((k) => (row as Row)[k]),
    );
    return r.lastInsertId ?? 0;
  }

  /** Fetch the row with the given primary key, or `undefined`. */
  async get(id: unknown): Promise<T | undefined> {
    const rows = await this.#src.query(
      `SELECT * FROM ${quoteIdent(this.name)} WHERE ${quoteIdent(this.pk)} = ? LIMIT 1`,
      [id],
    );
    return rows[0] as T | undefined;
  }

  /** Every row (optionally capped at `limit`). */
  async all(limit?: number): Promise<T[]> {
    const lim = typeof limit === "number"
      ? ` LIMIT ${Math.max(0, Math.floor(limit))}`
      : "";
    return (await this.#src.query(
      `SELECT * FROM ${quoteIdent(this.name)}${lim}`,
    )) as T[];
  }

  /** Rows matching an equality filter (keys ANDed). An empty filter matches
   * all rows. */
  async find(where: Partial<T> = {}): Promise<T[]> {
    const { clause, params } = whereClause(where as Row);
    return (await this.#src.query(
      `SELECT * FROM ${quoteIdent(this.name)}${clause}`,
      params,
    )) as T[];
  }

  /** The first row matching an equality filter, or `undefined`. */
  async findOne(where: Partial<T> = {}): Promise<T | undefined> {
    const { clause, params } = whereClause(where as Row);
    const rows = await this.#src.query(
      `SELECT * FROM ${quoteIdent(this.name)}${clause} LIMIT 1`,
      params,
    );
    return rows[0] as T | undefined;
  }

  /** Patch the row with the given primary key; returns rows changed. */
  async update(id: unknown, patch: Partial<T>): Promise<number> {
    const keys = Object.keys(patch);
    if (keys.length === 0) return 0;
    const set = keys.map((k) => `${quoteIdent(k)} = ?`).join(", ");
    const r = await this.#src.exec(
      `UPDATE ${quoteIdent(this.name)} SET ${set} WHERE ${quoteIdent(this.pk)} = ?`,
      [...keys.map((k) => (patch as Row)[k]), id],
    );
    return r.changed;
  }

  /** Delete the row with the given primary key; returns rows changed. */
  async delete(id: unknown): Promise<number> {
    const r = await this.#src.exec(
      `DELETE FROM ${quoteIdent(this.name)} WHERE ${quoteIdent(this.pk)} = ?`,
      [id],
    );
    return r.changed;
  }

  /** Count rows matching an equality filter (all rows when omitted). */
  async count(where: Partial<T> = {}): Promise<number> {
    const { clause, params } = whereClause(where as Row);
    const rows = await this.#src.query(
      `SELECT COUNT(*) AS n FROM ${quoteIdent(this.name)}${clause}`,
      params,
    );
    return Number((rows[0] as Row)?.n ?? 0);
  }
}

class SqliteDataSource implements DataSource {
  #db: DatabaseSync;
  #onClose?: () => void;

  constructor(path: string, onClose?: () => void) {
    this.#db = new DatabaseSync(path);
    // Every data op runs in its own process with its own connection, so a
    // project DB routinely has concurrent writers (a webhook enqueue racing an
    // inbox poll). node:sqlite's default busy timeout is 0, which fails the
    // loser immediately with "database is locked" and dropped trigger events
    // (#1287). Wait for the lock instead. Set FIRST: the WAL switch below
    // itself contends for the lock.
    this.#db.exec(`PRAGMA busy_timeout = ${SQLITE_BUSY_TIMEOUT_MS};`);
    if (path !== ":memory:") this.#db.exec("PRAGMA journal_mode = WAL;");
    this.#db.exec("PRAGMA foreign_keys = ON;");
    this.#onClose = onClose;
  }

  query(sql: string, params: unknown[] = []): Promise<Row[]> {
    const rows = this.#db.prepare(sql).all(...(params as never[]));
    return Promise.resolve(rows as Row[]);
  }

  exec(sql: string, params: unknown[] = []): Promise<ExecResult> {
    const r = this.#db.prepare(sql).run(...(params as never[]));
    return Promise.resolve({
      changed: Number(r.changes),
      lastInsertId: r.lastInsertRowid,
    });
  }

  async tx<T>(fn: (t: DataSource) => Promise<T>): Promise<T> {
    this.#db.exec("BEGIN");
    try {
      const out = await fn(this);
      this.#db.exec("COMMIT");
      return out;
    } catch (e) {
      this.#db.exec("ROLLBACK");
      throw e;
    }
  }

  schema(): Promise<TableMeta[]> {
    // Exclude SQLite internals (`sqlite_%`) and Nano's own bookkeeping tables
    // (`_nano_%`, e.g. the `_nano_migrations` ledger): neither is a user/domain
    // table, so they must never surface in the domain model, DB Manager, or forms.
    const tables = this.#db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '\\_nano\\_%' ESCAPE '\\' ORDER BY name",
      )
      .all() as Array<{ name: string }>;
    const out: TableMeta[] = [];
    for (const t of tables) {
      const cols = this.#db
        .prepare(`PRAGMA table_info(${quoteIdent(t.name)})`)
        .all() as Array<{ name: string; type: string; notnull: number; pk: number }>;
      const idx = this.#db
        .prepare(`PRAGMA index_list(${quoteIdent(t.name)})`)
        .all() as Array<{ name: string }>;
      const fks = this.#db
        .prepare(`PRAGMA foreign_key_list(${quoteIdent(t.name)})`)
        .all() as Array<
          { from: string; table: string; to: string | null; on_delete?: string }
        >;
      out.push({
        name: t.name,
        columns: cols.map((c) => ({
          name: c.name,
          type: c.type,
          notNull: !!c.notnull,
          primaryKey: !!c.pk,
        })),
        indexes: idx.map((i) => String(i.name)),
        foreignKeys: fks.map((f) => ({
          column: f.from,
          refTable: f.table,
          refColumn: f.to ?? "",
          onDelete: f.on_delete && f.on_delete.toUpperCase() !== "NO ACTION"
            ? f.on_delete.toUpperCase()
            : "",
        })),
      });
    }
    return Promise.resolve(out);
  }

  close(): void {
    this.#db.close();
    this.#onClose?.();
  }

  table<T extends object = Row>(name: string, pk = "id"): Table<T> {
    return new Table<T>(this, name, pk);
  }
}

// --- open (the named-alias entrypoint) -------------------------------------

// One handle per resolved (driver,url), so repeated opens in a process share a
// connection rather than reopening the file.
const CACHE = new Map<string, DataSource>();

export interface OpenOptions {
  /** Directory to begin the manifest search from. Defaults to the runtime cwd. */
  cwd?: string;
}

/// Open the named datasource (or the manifest's `default` when `name` is
/// omitted), resolving its driver/url from the environment. Bind by NAME — this
/// is the seam the SQLite→server flip happens behind (ADR 0024 §1).
export async function openDataSource(
  name?: string,
  opts?: OpenOptions,
): Promise<DataSource> {
  const cwd = opts?.cwd ?? RT.cwd();
  const { root, data } = await findManifest(cwd);
  const srcName = name ?? data.default ?? Object.keys(data.sources)[0];
  if (!srcName) {
    throw new Error("no datasource declared in nano.app.json (data.sources is empty)");
  }
  const raw = data.sources[srcName];
  if (!raw) {
    throw new Error(
      `datasource "${srcName}" is not declared in nano.app.json data.sources`,
    );
  }
  const resolved = resolveSource(srcName, raw, (k) => RT.env(k));
  const key = `${resolved.driver}::${resolved.url}`;
  const hit = CACHE.get(key);
  if (hit) return hit;

  let ds: DataSource;
  if (resolved.driver === "sqlite") {
    ds = new SqliteDataSource(sqlitePath(resolved.url, root), () => CACHE.delete(key));
  } else {
    throw new Error(
      `datasource driver "${resolved.driver}" is not bundled; install a nano-ide-data-${resolved.driver} pack (ADR 0024 §3)`,
    );
  }
  CACHE.set(key, ds);
  return ds;
}

/// Alias reading like the ADR's `ctx.data(...)`: `data("app")`.
export const data = openDataSource;
