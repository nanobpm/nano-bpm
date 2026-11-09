import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import CodeEditor from "./CodeEditor";
import { isReadStatement } from "../lib/sqlStatement";
import { Button, Input, inputClass } from "@nanobpm/process-explorer/ui";
import {
  execData,
  execDataScript,
  getDataMigrations,
  getDataSchema,
  getDataSources,
  migrateData,
  queryData,
  regenerateDomainTypes,
  saveProjectFile,
  type DataColumnMeta,
  type DataForeignKey,
  type DataMigrationEntry,
  type DataQueryResult,
  type DataSourceInfo,
  type DataTableMeta,
} from "../gen";

// The DB Manager — ADR 0024 §4. Three sub-surfaces (Tables / SQL / Migrations)
// over a *named* datasource, so it browses whatever `data.<source>` currently
// resolves to (embedded SQLite in the IDE, a server DB in production once a
// `nano-ide-data-*` driver pack is installed). Every read/write goes through
// the server data gateway, never a parallel SQLite path.

type SubTab = "tables" | "sql" | "migrations";
const ROW_LIMIT = 200;

// Synthetic alias for a table's implicit `rowid`, selected alongside `*` so the
// grid can target UPDATE/DELETE at an exact row. `WITHOUT ROWID` tables have no
// rowid — the query then fails and the browser falls back to a read-only grid.
const ROWID_COL = "__rowid";

function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (typeof e === "string") return e;
  if (e && typeof e === "object") {
    const o = e as { error?: unknown; detail?: unknown };
    if (typeof o.error === "string") return o.error;
    if (typeof o.detail === "string") return o.detail;
  }
  return String(e);
}

/** Quote an identifier for SQLite (double quotes, doubled to escape). */
function quoteIdent(id: string): string {
  return `"${id.replaceAll('"', '""')}"`;
}

// --- New-table DDL model ----------------------------------------------------

/** Common SQLite column affinities offered in the New Table dialog. */
const COLUMN_TYPES = [
  "INTEGER",
  "TEXT",
  "REAL",
  "NUMERIC",
  "BLOB",
  "BOOLEAN",
  "TIMESTAMP",
];

/** `ON DELETE` referential actions offered for a foreign key ("" = omit). */
const ON_DELETE_ACTIONS = ["", "CASCADE", "SET NULL", "RESTRICT", "NO ACTION"];

/** A column's optional foreign-key reference to another table's column. */
interface ColumnRef {
  table: string;
  column: string;
  onDelete: string;
}

interface NewColumn {
  name: string;
  type: string;
  primaryKey: boolean;
  notNull: boolean;
  default: string;
  references: ColumnRef | null;
}

function blankColumn(): NewColumn {
  return {
    name: "",
    type: "TEXT",
    primaryKey: false,
    notNull: false,
    default: "",
    references: null,
  };
}

/** Build a `CREATE TABLE` statement from the dialog's form model. */
function buildCreateTable(table: string, cols: NewColumn[]): string {
  const pkCols = cols.filter((c) => c.primaryKey && c.name.trim());
  const defs = cols
    .filter((c) => c.name.trim())
    .map((c) => {
      let s = `  ${quoteIdent(c.name.trim())} ${c.type || "TEXT"}`;
      // A single primary key is declared inline; an INTEGER one becomes the
      // rowid alias. Composite keys use a table-level constraint instead.
      if (pkCols.length === 1 && c.primaryKey) s += " PRIMARY KEY";
      if (c.notNull && !c.primaryKey) s += " NOT NULL";
      if (c.default.trim()) s += ` DEFAULT ${c.default.trim()}`;
      return s;
    });
  if (pkCols.length > 1) {
    defs.push(
      `  PRIMARY KEY (${pkCols.map((c) => quoteIdent(c.name.trim())).join(", ")})`,
    );
  }
  // Foreign keys are emitted as table-level constraints (SQLite requires them
  // after all column defs). `PRAGMA foreign_keys = ON` is set by the datasource,
  // so these are enforced, not merely declarative.
  for (const c of cols) {
    const clause = fkClause(c.name, c.references);
    if (clause) defs.push(clause);
  }
  return `CREATE TABLE ${quoteIdent(table.trim() || "new_table")} (\n${defs.join(",\n")}\n);`;
}

/** A column's foreign-key clause for a `CREATE TABLE` (inline table constraint).
 * Returns null when the reference is incomplete. An empty `refColumn` targets
 * the parent's primary key (SQLite allows omitting the column list). */
function fkClause(colName: string, r: ColumnRef | null): string | null {
  if (!r || !colName.trim() || !r.table.trim()) return null;
  let s = `  FOREIGN KEY (${quoteIdent(colName.trim())}) REFERENCES ${quoteIdent(r.table.trim())}`;
  if (r.column.trim()) s += ` (${quoteIdent(r.column.trim())})`;
  if (r.onDelete) s += ` ON DELETE ${r.onDelete}`;
  return s;
}

/** A stable key for a column reference, so two FKs can be compared for equality. */
function refKey(r: ColumnRef | null): string {
  return r ? `${r.table}\u0000${r.column}\u0000${r.onDelete}` : "";
}

// --- Structure-editor DDL model --------------------------------------------

/**
 * A column as seen by the structure editor. `originalName` is the column's name
 * in the live table (null for a freshly-added column) and is the stable key used
 * to copy data during a rebuild; the other fields are the edited target shape.
 * `references` is the column's foreign key (null when it has none).
 */
interface EditColumn {
  originalName: string | null;
  name: string;
  type: string;
  primaryKey: boolean;
  notNull: boolean;
  default: string;
  references: ColumnRef | null;
  drop: boolean;
}

/** Seed the structure editor from a live table's columns + foreign keys. */
function editColumnsFrom(
  columns: DataColumnMeta[],
  foreignKeys: DataForeignKey[],
): EditColumn[] {
  const fkByColumn = new Map(foreignKeys.map((f) => [f.column, f]));
  return columns.map((c) => {
    const fk = fkByColumn.get(c.name);
    return {
      originalName: c.name,
      name: c.name,
      type: c.type || "TEXT",
      primaryKey: c.primaryKey,
      notNull: c.notNull,
      default: "",
      references: fk
        ? { table: fk.refTable, column: fk.refColumn, onDelete: fk.onDelete }
        : null,
      drop: false,
    };
  });
}

function blankEditColumn(): EditColumn {
  return {
    originalName: null,
    name: "",
    type: "TEXT",
    primaryKey: false,
    notNull: false,
    default: "",
    references: null,
    drop: false,
  };
}

/** A single column's `CREATE TABLE` definition (no table-level PK handling). */
function columnDef(c: EditColumn, inlinePk: boolean): string {
  let s = `${quoteIdent(c.name.trim())} ${c.type || "TEXT"}`;
  if (inlinePk && c.primaryKey) s += " PRIMARY KEY";
  if (c.notNull && !c.primaryKey) s += " NOT NULL";
  if (c.default.trim()) s += ` DEFAULT ${c.default.trim()}`;
  return s;
}

/**
 * Does turning `orig` into `edit` need a full table rebuild? Native `ALTER
 * TABLE` can rename the table, add, rename and drop columns, but cannot change
 * an existing column's type or its NOT NULL / PRIMARY KEY / DEFAULT — those force
 * the SQLite 12-step rebuild.
 */
function needsRebuild(orig: DataColumnMeta, edit: EditColumn): boolean {
  return (
    (edit.type || "TEXT") !== (orig.type || "TEXT") ||
    edit.notNull !== orig.notNull ||
    edit.primaryKey !== orig.primaryKey ||
    edit.default.trim() !== ""
  );
}

/**
 * Compute the DDL to reshape `oldName` into `newName` with `cols`. Returns the
 * ordered statement list plus whether a rebuild was chosen, so the caller can
 * warn about the rebuild's caveats. Foreign keys *are* carried through the
 * rebuild (they are re-emitted from each kept column's `references`); a
 * foreign-key add/change/remove itself forces the rebuild, since SQLite cannot
 * add or drop an FK on an existing column via `ALTER TABLE`.
 */
function buildStructureStatements(
  oldName: string,
  newName: string,
  original: DataColumnMeta[],
  originalFks: DataForeignKey[],
  cols: EditColumn[],
): { statements: string[]; rebuild: boolean } {
  const kept = cols.filter((c) => !c.drop && c.name.trim());
  const byOriginal = new Map(original.map((c) => [c.name, c]));
  // The original FK per column, so an edited reference can be compared for change.
  const origRefByColumn = new Map(
    originalFks.map((f): [string, ColumnRef] => [
      f.column,
      { table: f.refTable, column: f.refColumn, onDelete: f.onDelete },
    ]),
  );
  const fkChanged = kept.some((c) => {
    const before = c.originalName
      ? (origRefByColumn.get(c.originalName) ?? null)
      : null;
    return refKey(before) !== refKey(c.references);
  });
  const rebuild =
    fkChanged ||
    kept.some((c) => {
      const o = c.originalName ? byOriginal.get(c.originalName) : undefined;
      return o ? needsRebuild(o, c) : false;
    });

  if (rebuild) {
    // SQLite 12-step rebuild: create a new table, copy data by (old→new) column
    // mapping, drop the old, rename the new into place. New columns get their
    // declared DEFAULT / NULL. Runs atomically via the `script` endpoint.
    const tmp = `${newName.trim()}__rebuild`;
    const pkCount = kept.filter((c) => c.primaryKey).length;
    const defs = kept.map((c) => `  ${columnDef(c, pkCount === 1)}`);
    if (pkCount > 1) {
      defs.push(
        `  PRIMARY KEY (${kept
          .filter((c) => c.primaryKey)
          .map((c) => quoteIdent(c.name.trim()))
          .join(", ")})`,
      );
    }
    // Re-emit each kept column's foreign key as a table-level constraint so the
    // rebuild preserves (and applies newly added) FKs. A self-reference targets
    // the temp table so it resolves during CREATE/INSERT; the final RENAME TABLE
    // rewrites the reference to the new table name automatically.
    for (const c of kept) {
      let ref = c.references;
      if (ref && ref.table === oldName) ref = { ...ref, table: tmp };
      const clause = fkClause(c.name, ref);
      if (clause) defs.push(clause);
    }
    const copyCols = kept.filter((c) => c.originalName);
    const create = `CREATE TABLE ${quoteIdent(tmp)} (\n${defs.join(",\n")}\n)`;
    const insert =
      copyCols.length > 0
        ? `INSERT INTO ${quoteIdent(tmp)} (${copyCols
            .map((c) => quoteIdent(c.name.trim()))
            .join(", ")}) SELECT ${copyCols
            .map((c) => quoteIdent(c.originalName as string))
            .join(", ")} FROM ${quoteIdent(oldName)}`
        : null;
    const statements = [create];
    if (insert) statements.push(insert);
    statements.push(`DROP TABLE ${quoteIdent(oldName)}`);
    statements.push(
      `ALTER TABLE ${quoteIdent(tmp)} RENAME TO ${quoteIdent(newName.trim())}`,
    );
    return { statements, rebuild: true };
  }

  // Non-destructive path: individual ALTER TABLE statements. Drops and renames
  // first (against the old name), adds next, then the table rename last.
  const statements: string[] = [];
  for (const c of cols) {
    if (c.originalName && c.drop) {
      statements.push(
        `ALTER TABLE ${quoteIdent(oldName)} DROP COLUMN ${quoteIdent(c.originalName)}`,
      );
    }
  }
  for (const c of cols) {
    if (
      c.originalName &&
      !c.drop &&
      c.name.trim() &&
      c.name.trim() !== c.originalName
    ) {
      statements.push(
        `ALTER TABLE ${quoteIdent(oldName)} RENAME COLUMN ${quoteIdent(c.originalName)} TO ${quoteIdent(c.name.trim())}`,
      );
    }
  }
  for (const c of cols) {
    if (!c.originalName && !c.drop && c.name.trim()) {
      statements.push(
        `ALTER TABLE ${quoteIdent(oldName)} ADD COLUMN ${columnDef(c, false)}`,
      );
    }
  }
  if (newName.trim() && newName.trim() !== oldName) {
    statements.push(
      `ALTER TABLE ${quoteIdent(oldName)} RENAME TO ${quoteIdent(newName.trim())}`,
    );
  }
  return { statements, rebuild: false };
}

/** Next ordered migration filename, e.g. `003_create_orders.sql`. */
function nextMigrationName(
  existing: DataMigrationEntry[],
  table: string,
): string {
  const max = existing.reduce((m, e) => {
    const n = parseInt(e.name, 10);
    return Number.isNaN(n) ? m : Math.max(m, n);
  }, 0);
  const num = String(max + 1).padStart(3, "0");
  const slug =
    table
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "") || "table";
  return `${num}_create_${slug}.sql`;
}

/** Human cell rendering: NULL italic, blobs badged, objects as compact JSON. */
function cell(v: unknown): { text: string; muted: boolean } {
  if (v === null || v === undefined) return { text: "NULL", muted: true };
  if (typeof v === "object") {
    const o = v as { $blob?: number };
    if (typeof o.$blob === "number")
      return { text: `‹blob ${o.$blob}B›`, muted: true };
    return { text: JSON.stringify(v), muted: false };
  }
  return { text: String(v), muted: false };
}

export default function DataPanel({
  name,
  appRunning,
}: {
  name: string;
  appRunning: boolean;
}) {
  const [sources, setSources] = useState<DataSourceInfo[]>([]);
  const [source, setSource] = useState<string>("");
  const [tab, setTab] = useState<SubTab>("tables");
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let live = true;
    setLoading(true);
    setLoadError(null);
    getDataSources({ path: { name }, throwOnError: true })
      .then((r) => {
        if (!live) return;
        const list = r.data.sources ?? [];
        setSources(list);
        setSource(r.data.default ?? list[0]?.name ?? "");
      })
      .catch((e) => live && setLoadError(errMsg(e)))
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [name]);

  const active = useMemo(
    () => sources.find((s) => s.name === source),
    [sources, source],
  );

  if (loading) {
    return (
      <div className="p-8 text-sm text-fg-faint">Loading datasources…</div>
    );
  }
  if (loadError) {
    return (
      <div className="p-8 text-sm text-danger">
        Couldn’t load datasources: {loadError}
      </div>
    );
  }
  if (sources.length === 0) {
    return (
      <div className="flex h-full items-center justify-center p-8 text-center text-sm text-fg-faint">
        <div>
          <p className="font-medium text-fg-muted">No datasources declared.</p>
          <p className="mt-1">
            Add a <code className="text-fg-muted">data.sources</code> block to{" "}
            <code className="text-fg-muted">nano.app.json</code> (ADR 0024) to
            use the DB Manager.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <label className="flex items-center gap-1.5 text-xs text-fg-faint">
          <span>Datasource:</span>
          <select
            value={source}
            onChange={(e) => setSource(e.target.value)}
            className="rounded-md border border-edge-strong bg-bg-subtle px-2 py-1 text-sm text-fg hover:bg-hover"
          >
            {sources.map((s) => (
              <option key={s.name} value={s.name}>
                {s.name} ({s.driver})
              </option>
            ))}
          </select>
        </label>
        {active && (
          <span className="truncate text-xs text-fg-faint" title={active.url}>
            {active.url}
          </span>
        )}
        <div className="flex-1" />
        <nav className="flex items-center gap-1 text-sm">
          {(["tables", "sql", "migrations"] as SubTab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`rounded-md px-2.5 py-1 capitalize ${
                tab === t
                  ? "bg-accent/15 font-semibold text-accent"
                  : "text-fg-faint hover:bg-hover"
              }`}
            >
              {t}
            </button>
          ))}
        </nav>
      </div>

      <div className="min-h-0 flex-1 overflow-hidden">
        {source && tab === "tables" && (
          <TablesTab
            key={`t-${source}`}
            name={name}
            source={source}
            appRunning={appRunning}
          />
        )}
        {source && tab === "sql" && (
          <SqlTab
            key={`s-${source}`}
            name={name}
            source={source}
            appRunning={appRunning}
          />
        )}
        {source && tab === "migrations" && (
          <MigrationsTab
            key={`m-${source}`}
            name={name}
            source={source}
            appRunning={appRunning}
          />
        )}
      </div>
    </div>
  );
}

// --- Tables -----------------------------------------------------------------

function TablesTab({
  name,
  source,
  appRunning,
}: {
  name: string;
  source: string;
  appRunning: boolean;
}) {
  const [tables, setTables] = useState<DataTableMeta[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [rows, setRows] = useState<DataQueryResult | null>(null);
  const [editable, setEditable] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadingRows, setLoadingRows] = useState(false);
  const [showNew, setShowNew] = useState(false);
  const [showAddRow, setShowAddRow] = useState(false);
  const [showStructure, setShowStructure] = useState(false);
  const [editRow, setEditRow] = useState<Record<string, unknown> | null>(null);
  const [regenBusy, setRegenBusy] = useState(false);
  const [regenNote, setRegenNote] = useState<string | null>(null);

  const loadSchema = useCallback(async () => {
    setError(null);
    try {
      const r = await getDataSchema({
        path: { name, source },
        throwOnError: true,
      });
      setTables(r.data.tables ?? []);
      setSelected((cur) => cur ?? r.data.tables?.[0]?.name ?? null);
    } catch (e) {
      setError(errMsg(e));
    }
  }, [name, source]);

  useEffect(() => {
    void loadSchema();
  }, [loadSchema]);

  const loadRows = useCallback(async () => {
    if (!selected) return;
    setLoadingRows(true);
    setError(null);
    const from = `${quoteIdent(selected)} LIMIT ${ROW_LIMIT}`;
    try {
      // Prefer selecting the rowid so the grid can edit/delete individual rows.
      const r = await queryData({
        path: { name, source },
        body: { sql: `SELECT rowid AS ${ROWID_COL}, * FROM ${from}` },
        throwOnError: true,
      });
      setRows(r.data);
      setEditable(true);
    } catch {
      // WITHOUT ROWID tables (or an existing `rowid` column) — fall back to a
      // plain read-only browse.
      try {
        const r = await queryData({
          path: { name, source },
          body: { sql: `SELECT * FROM ${from}` },
          throwOnError: true,
        });
        setRows(r.data);
        setEditable(false);
      } catch (e) {
        setError(errMsg(e));
      }
    } finally {
      setLoadingRows(false);
    }
  }, [name, source, selected]);

  useEffect(() => {
    void loadRows();
  }, [loadRows]);

  const meta = tables.find((t) => t.name === selected);

  const regenTypes = useCallback(async () => {
    setRegenBusy(true);
    setRegenNote(null);
    try {
      const r = await regenerateDomainTypes({
        path: { name, source },
        throwOnError: true,
      });
      const n = r.data.tables;
      // The reify rewrote `nano-generated/*` — drop the editor's cached SDK typings so
      // the next code file opened picks up the regenerated `job.variables`/domain
      // types without a reload.
      const { invalidateProjectSdkLibs } = await import("./CodeEditor");
      invalidateProjectSdkLibs();
      setRegenNote(
        `Generated ${r.data.path ?? "domain-rows.d.ts"} — ${n} ${n === 1 ? "table" : "tables"} across all datasources.`,
      );
    } catch (e) {
      setRegenNote(errMsg(e));
    } finally {
      setRegenBusy(false);
    }
  }, [name, source]);

  const deleteRow = useCallback(
    async (rowid: unknown) => {
      if (!selected) return;
      try {
        await execData({
          path: { name, source },
          body: {
            sql: `DELETE FROM ${quoteIdent(selected)} WHERE rowid = ?`,
            params: [rowid],
          },
          throwOnError: true,
        });
        await loadRows();
      } catch (e) {
        setError(errMsg(e));
      }
    },
    [name, source, selected, loadRows],
  );

  return (
    <div className="flex h-full min-h-0">
      <aside className="w-56 shrink-0 overflow-y-auto border-r border-edge bg-panel">
        <div className="flex items-center justify-between gap-2 px-3 py-2">
          <span className="text-[10px] font-bold uppercase tracking-wider text-fg-faint">
            Tables ({tables.length})
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => void regenTypes()}
              disabled={regenBusy || appRunning}
              className="rounded px-1.5 py-0.5 text-xs font-medium text-fg-muted hover:bg-hover disabled:opacity-40"
              title={
                appRunning
                  ? "Stop the app to regenerate domain types"
                  : "Regenerate the TypeScript domain types (nano-generated/domain-rows.d.ts) from every datasource"
              }
            >
              {regenBusy ? "…" : "⟳ Types"}
            </button>
            <button
              onClick={() => setShowNew(true)}
              disabled={appRunning}
              className="rounded px-1.5 py-0.5 text-xs font-medium text-accent hover:bg-accent/10 disabled:opacity-40"
              title={
                appRunning
                  ? "Stop the app to create a table"
                  : "Create a new table"
              }
            >
              ＋ New
            </button>
          </div>
        </div>
        {regenNote && (
          <p
            className="px-3 pb-1.5 text-[11px] leading-snug text-fg-faint"
            title={regenNote}
          >
            {regenNote}
          </p>
        )}
        {tables.map((t) => (
          <button
            key={t.name}
            onClick={() => setSelected(t.name)}
            className={`block w-full truncate px-3 py-1.5 text-left text-sm ${
              selected === t.name
                ? "bg-accent/15 font-medium text-accent"
                : "text-fg-muted hover:bg-hover"
            }`}
            title={`${t.name} — ${t.columns.length} columns`}
          >
            {t.name}
          </button>
        ))}
        {tables.length === 0 && (
          <p className="px-3 py-2 text-xs text-fg-faint">No tables yet.</p>
        )}
      </aside>
      <div className="flex min-w-0 flex-1 flex-col">
        {meta && (
          <div className="flex items-center gap-2 border-b border-edge bg-panel px-4 py-1.5">
            <span className="text-sm font-medium text-fg">{meta.name}</span>
            <div className="flex-1" />
            <button
              onClick={() => setShowAddRow(true)}
              disabled={!editable || appRunning}
              className="rounded px-1.5 py-0.5 text-xs font-medium text-accent hover:bg-accent/10 disabled:opacity-40"
              title={
                appRunning
                  ? "Stop the app to insert rows"
                  : editable
                    ? "Insert a row"
                    : "This table has no rowid — add rows from the SQL tab"
              }
            >
              ＋ Add row
            </button>
            <button
              onClick={() => setShowStructure(true)}
              disabled={appRunning}
              className="rounded px-1.5 py-0.5 text-xs font-medium text-fg-muted hover:bg-hover disabled:opacity-40"
              title={
                appRunning
                  ? "Stop the app to edit table structure"
                  : "Edit table structure"
              }
            >
              ✎ Structure
            </button>
          </div>
        )}
        {meta && (
          <ColumnStrip
            columns={meta.columns}
            indexes={meta.indexes}
            foreignKeys={meta.foreignKeys}
          />
        )}
        {error && <div className="px-4 py-2 text-sm text-danger">{error}</div>}
        <div className="min-h-0 flex-1 overflow-auto">
          {loadingRows ? (
            <div className="p-6 text-sm text-fg-faint">Loading rows…</div>
          ) : rows ? (
            <ResultGrid
              result={rows}
              rowKey={editable ? ROWID_COL : undefined}
              onEditRow={
                editable && !appRunning ? (row) => setEditRow(row) : undefined
              }
              onDeleteRow={
                editable && !appRunning
                  ? (rowid) => void deleteRow(rowid)
                  : undefined
              }
            />
          ) : (
            <div className="p-6 text-sm text-fg-faint">Select a table.</div>
          )}
        </div>
        {rows && (
          <div className="border-t border-edge px-4 py-1 text-xs text-fg-faint">
            {rows.rows.length} row{rows.rows.length === 1 ? "" : "s"}
            {rows.rows.length >= ROW_LIMIT ? ` (first ${ROW_LIMIT})` : ""}
            {!editable && rows.rows.length > 0 && " · read-only (no rowid)"}
          </div>
        )}
      </div>
      {showNew && (
        <NewTableDialog
          name={name}
          source={source}
          tables={tables}
          onClose={() => setShowNew(false)}
          onCreated={(table) => {
            setShowNew(false);
            void loadSchema().then(() => setSelected(table));
          }}
        />
      )}
      {showAddRow && meta && (
        <RowDialog
          name={name}
          source={source}
          table={meta.name}
          columns={meta.columns}
          onClose={() => setShowAddRow(false)}
          onSaved={() => {
            setShowAddRow(false);
            void loadRows();
          }}
        />
      )}
      {editRow && meta && (
        <RowDialog
          name={name}
          source={source}
          table={meta.name}
          columns={meta.columns}
          existing={editRow}
          onClose={() => setEditRow(null)}
          onSaved={() => {
            setEditRow(null);
            void loadRows();
          }}
        />
      )}
      {showStructure && meta && (
        <EditStructureDialog
          name={name}
          source={source}
          table={meta.name}
          columns={meta.columns}
          foreignKeys={meta.foreignKeys}
          tables={tables}
          onClose={() => setShowStructure(false)}
          onSaved={(newName) => {
            setShowStructure(false);
            void loadSchema();
            // A rename changes `selected` (which re-fires the row load); an
            // in-place structure change keeps the same name, so reload directly.
            if (newName === selected) void loadRows();
            else setSelected(newName);
          }}
        />
      )}
    </div>
  );
}

function ColumnStrip({
  columns,
  indexes,
  foreignKeys,
}: {
  columns: DataColumnMeta[];
  indexes: string[];
  foreignKeys: DataForeignKey[];
}) {
  const fkByCol = new Map(foreignKeys.map((fk) => [fk.column, fk]));
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-edge bg-bg-subtle px-4 py-1.5 text-xs">
      {columns.map((c) => {
        const fk = fkByCol.get(c.name);
        return (
          <span key={c.name} className="text-fg-muted">
            <span className="font-medium text-fg">{c.name}</span>
            <span className="text-fg-faint"> {c.type || "?"}</span>
            {c.primaryKey && (
              <span className="ml-0.5 text-accent" title="primary key">
                🔑
              </span>
            )}
            {c.notNull && !c.primaryKey && (
              <span className="ml-0.5 text-fg-faint" title="NOT NULL">
                *
              </span>
            )}
            {fk && (
              <span
                className="ml-0.5 text-accent"
                title={`foreign key → ${fk.refTable}${fk.refColumn ? `.${fk.refColumn}` : ""}${
                  fk.onDelete ? ` (on delete ${fk.onDelete})` : ""
                }`}
              >
                ↗{fk.refTable}
              </span>
            )}
          </span>
        );
      })}
      {indexes.length > 0 && (
        <span className="text-fg-faint" title={indexes.join(", ")}>
          · {indexes.length} index{indexes.length === 1 ? "" : "es"}
        </span>
      )}
    </div>
  );
}

// --- Shared modal + New-table dialog ---------------------------------------

/** Minimal centered modal (the console has no dialog primitive yet). */
function Modal({
  title,
  onClose,
  children,
  footer,
  wide,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  footer: ReactNode;
  wide?: boolean;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        className={`flex max-h-[85vh] w-full ${wide ? "max-w-2xl" : "max-w-lg"} flex-col overflow-hidden rounded-lg border border-edge bg-panel shadow-xl`}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-edge px-4 py-2.5">
          <h2 className="text-sm font-semibold text-fg">{title}</h2>
          <button
            onClick={onClose}
            className="rounded p-1 text-fg-faint hover:bg-hover"
            title="Close"
          >
            ✕
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto p-4">{children}</div>
        <div className="flex items-center justify-end gap-2 border-t border-edge px-4 py-2.5">
          {footer}
        </div>
      </div>
    </div>
  );
}

/**
 * Encapsulates table creation as a form (the Delphi-style affordance): the user
 * fills in columns and we generate the `CREATE TABLE` DDL. Two apply paths match
 * the two documented workflows — run it now against the datasource, or save it
 * as an ordered migration file (the deployable path, ADR 0024 §4).
 */
function NewTableDialog({
  name,
  source,
  tables,
  onClose,
  onCreated,
}: {
  name: string;
  source: string;
  tables: DataTableMeta[];
  onClose: () => void;
  onCreated: (table: string) => void;
}) {
  const [table, setTable] = useState("");
  const [cols, setCols] = useState<NewColumn[]>([
    {
      name: "id",
      type: "INTEGER",
      primaryKey: true,
      notNull: false,
      default: "",
      references: null,
    },
  ]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);

  const namedCols = cols.filter((c) => c.name.trim());
  const valid = table.trim().length > 0 && namedCols.length > 0;
  const sql = useMemo(() => buildCreateTable(table, cols), [table, cols]);

  const setCol = (i: number, patch: Partial<NewColumn>) =>
    setCols((cs) => cs.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const addCol = () => setCols((cs) => [...cs, blankColumn()]);
  const removeCol = (i: number) =>
    setCols((cs) => cs.filter((_, j) => j !== i));

  // FK targets: existing tables in this datasource (the new table can't yet
  // reference itself since it doesn't exist). Columns come from the picked table.
  const fkTables = tables.map((t) => t.name);
  const columnsOf = (t: string) =>
    tables.find((x) => x.name === t)?.columns.map((c) => c.name) ?? [];
  const toggleFk = (i: number, on: boolean) =>
    setCol(i, {
      references: on
        ? { table: fkTables[0] ?? "", column: "", onDelete: "" }
        : null,
    });
  const setRef = (i: number, patch: Partial<ColumnRef>) =>
    setCols((cs) =>
      cs.map((c, j) =>
        j === i && c.references
          ? { ...c, references: { ...c.references, ...patch } }
          : c,
      ),
    );

  const runNow = useCallback(async () => {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      await execData({
        path: { name, source },
        body: { sql },
        throwOnError: true,
      });
      onCreated(table.trim());
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [name, source, sql, table, onCreated]);

  const saveMigration = useCallback(async () => {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      const r = await getDataMigrations({
        path: { name, source },
        throwOnError: true,
      });
      const dir = r.data.dir || "db/migrations";
      const file = nextMigrationName(r.data.entries ?? [], table);
      const path = `${dir}/${file}`;
      await saveProjectFile({
        path: { name },
        query: { path },
        body: `${sql}\n`,
        throwOnError: true,
      });
      setNote(`Wrote ${path}. Apply it from the Migrations tab.`);
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [name, source, sql, table]);

  return (
    <Modal
      title="New table"
      onClose={onClose}
      wide
      footer={
        <>
          {note && (
            <span className="mr-auto truncate text-xs text-ok">{note}</span>
          )}
          {error && (
            <span className="mr-auto truncate text-xs text-danger">
              {error}
            </span>
          )}
          <Button variant="secondary" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button
            variant="secondary"
            onClick={() => void saveMigration()}
            disabled={!valid || busy}
          >
            Save as migration
          </Button>
          <Button
            variant="primary"
            onClick={() => void runNow()}
            disabled={!valid || busy}
          >
            {busy ? "Working…" : "Create now"}
          </Button>
        </>
      }
    >
      <label className="mb-3 block">
        <span className="mb-1 block text-xs font-medium text-fg-muted">
          Table name
        </span>
        <Input
          value={table}
          onChange={(e) => setTable(e.target.value)}
          placeholder="orders"
          autoFocus
        />
      </label>

      <div className="mb-1 flex items-center justify-between">
        <span className="text-xs font-medium text-fg-muted">Columns</span>
        <button
          onClick={addCol}
          className="text-xs font-medium text-accent hover:underline"
        >
          ＋ Add column
        </button>
      </div>
      <div className="space-y-1.5">
        <div className="flex items-center gap-2 px-0.5 text-[10px] uppercase tracking-wide text-fg-faint">
          <span className="flex-1">Name</span>
          <span className="w-28">Type</span>
          <span className="w-24">Default</span>
          <span className="w-8 text-center" title="Primary key">
            PK
          </span>
          <span className="w-10 text-center" title="NOT NULL">
            Req
          </span>
          <span className="w-8 text-center" title="Foreign key">
            FK
          </span>
          <span className="w-5" />
        </div>
        {cols.map((c, i) => (
          <div key={i} className="flex flex-col gap-1.5">
            <div className="flex items-center gap-2">
              <Input
                className="flex-1"
                value={c.name}
                onChange={(e) => setCol(i, { name: e.target.value })}
                placeholder="column"
              />
              <select
                className={`${inputClass} w-28`}
                value={c.type}
                onChange={(e) => setCol(i, { type: e.target.value })}
              >
                {COLUMN_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
              <Input
                className="w-24"
                value={c.default}
                onChange={(e) => setCol(i, { default: e.target.value })}
                placeholder="—"
                title="Raw SQL default, e.g. 0, 'active', CURRENT_TIMESTAMP"
              />
              <input
                type="checkbox"
                className="w-8"
                checked={c.primaryKey}
                onChange={(e) => setCol(i, { primaryKey: e.target.checked })}
                title="Primary key"
              />
              <input
                type="checkbox"
                className="w-10"
                checked={c.notNull}
                disabled={c.primaryKey}
                onChange={(e) => setCol(i, { notNull: e.target.checked })}
                title="NOT NULL"
              />
              <input
                type="checkbox"
                className="w-8"
                checked={c.references !== null}
                disabled={fkTables.length === 0}
                onChange={(e) => toggleFk(i, e.target.checked)}
                title={
                  fkTables.length === 0
                    ? "No other tables to reference yet"
                    : "Foreign key to another table"
                }
              />
              <button
                onClick={() => removeCol(i)}
                disabled={cols.length === 1}
                className="w-5 text-fg-faint hover:text-danger disabled:opacity-30"
                title="Remove column"
              >
                ✕
              </button>
            </div>
            {c.references && (
              <div className="flex items-center gap-2 pl-3 text-xs text-fg-faint">
                <span className="text-fg-faint">↳ references</span>
                <select
                  className={`${inputClass} w-40`}
                  value={c.references.table}
                  onChange={(e) =>
                    setRef(i, { table: e.target.value, column: "" })
                  }
                >
                  {fkTables.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
                <span>.</span>
                <select
                  className={`${inputClass} w-40`}
                  value={c.references.column}
                  onChange={(e) => setRef(i, { column: e.target.value })}
                >
                  <option value="">column…</option>
                  {columnsOf(c.references.table).map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
                </select>
                <span className="ml-1">on delete</span>
                <select
                  className={`${inputClass} w-28`}
                  value={c.references.onDelete}
                  onChange={(e) => setRef(i, { onDelete: e.target.value })}
                >
                  {ON_DELETE_ACTIONS.map((a) => (
                    <option key={a} value={a}>
                      {a || "—"}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="mt-4">
        <span className="mb-1 block text-[10px] uppercase tracking-wide text-fg-faint">
          Generated SQL
        </span>
        <pre className="max-h-40 overflow-auto rounded-md border border-edge bg-inset p-3 font-mono text-xs text-fg-muted">
          {sql}
        </pre>
      </div>
    </Modal>
  );
}

// --- Row add / edit dialog --------------------------------------------------

/** Per-field editing state: a raw string plus an explicit NULL flag. */
interface FieldState {
  raw: string;
  isNull: boolean;
}

/** Render a live value into an editable field state (edit mode seed). */
function seedField(v: unknown): FieldState {
  if (v === null || v === undefined) return { raw: "", isNull: true };
  if (typeof v === "object") return { raw: JSON.stringify(v), isNull: false };
  return { raw: String(v), isNull: false };
}

/**
 * The Delphi-style row form: one field per column, with a NULL toggle. In *add*
 * mode a blank required field is omitted so column defaults / autoincrement
 * apply; in *edit* mode every column is written and the row is targeted by its
 * `__rowid`. All values go through bound `?` params — never string-concatenated.
 */
function RowDialog({
  name,
  source,
  table,
  columns,
  existing,
  onClose,
  onSaved,
}: {
  name: string;
  source: string;
  table: string;
  columns: DataColumnMeta[];
  existing?: Record<string, unknown>;
  onClose: () => void;
  onSaved: () => void;
}) {
  const editing = existing != null;
  const [fields, setFields] = useState<Record<string, FieldState>>(() => {
    const out: Record<string, FieldState> = {};
    for (const c of columns) {
      out[c.name] = editing
        ? seedField(existing?.[c.name])
        : { raw: "", isNull: false };
    }
    return out;
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const setField = (col: string, patch: Partial<FieldState>) =>
    setFields((f) => ({ ...f, [col]: { ...f[col], ...patch } }));

  const paramFor = (f: FieldState): unknown => (f.isNull ? null : f.raw);

  const save = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      let sql: string;
      let params: unknown[];
      if (editing) {
        const cols = columns;
        sql = `UPDATE ${quoteIdent(table)} SET ${cols
          .map((c) => `${quoteIdent(c.name)} = ?`)
          .join(", ")} WHERE rowid = ?`;
        params = [
          ...cols.map((c) => paramFor(fields[c.name])),
          existing?.[ROWID_COL],
        ];
      } else {
        // Include a column only if the user set NULL or typed a value; blank
        // untouched fields are omitted so DEFAULT / autoincrement apply.
        const provided = columns.filter(
          (c) => fields[c.name].isNull || fields[c.name].raw !== "",
        );
        if (provided.length === 0) {
          sql = `INSERT INTO ${quoteIdent(table)} DEFAULT VALUES`;
          params = [];
        } else {
          sql = `INSERT INTO ${quoteIdent(table)} (${provided
            .map((c) => quoteIdent(c.name))
            .join(", ")}) VALUES (${provided.map(() => "?").join(", ")})`;
          params = provided.map((c) => paramFor(fields[c.name]));
        }
      }
      await execData({
        path: { name, source },
        body: { sql, params },
        throwOnError: true,
      });
      onSaved();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [editing, columns, table, fields, existing, name, source, onSaved]);

  return (
    <Modal
      title={editing ? `Edit row in ${table}` : `Add row to ${table}`}
      onClose={onClose}
      footer={
        <>
          {error && (
            <span className="mr-auto truncate text-xs text-danger">
              {error}
            </span>
          )}
          <Button variant="secondary" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button variant="primary" onClick={() => void save()} disabled={busy}>
            {busy ? "Saving…" : editing ? "Save changes" : "Insert row"}
          </Button>
        </>
      }
    >
      <div className="space-y-2.5">
        {columns.map((c) => {
          const f = fields[c.name];
          return (
            <label key={c.name} className="block">
              <span className="mb-1 flex items-center gap-1.5 text-xs font-medium text-fg-muted">
                {c.name}
                <span className="text-fg-faint">{c.type || "?"}</span>
                {c.primaryKey && (
                  <span className="text-accent" title="primary key">
                    🔑
                  </span>
                )}
                {c.notNull && !c.primaryKey && (
                  <span className="text-fg-faint" title="NOT NULL">
                    *
                  </span>
                )}
              </span>
              <div className="flex items-center gap-2">
                <Input
                  className="flex-1"
                  value={f.isNull ? "" : f.raw}
                  disabled={f.isNull}
                  placeholder={c.notNull ? "required" : "—"}
                  onChange={(e) => setField(c.name, { raw: e.target.value })}
                />
                {!c.notNull && (
                  <label
                    className="flex items-center gap-1 text-xs text-fg-faint"
                    title="Store NULL"
                  >
                    <input
                      type="checkbox"
                      checked={f.isNull}
                      onChange={(e) =>
                        setField(c.name, { isNull: e.target.checked })
                      }
                    />
                    NULL
                  </label>
                )}
              </div>
            </label>
          );
        })}
      </div>
    </Modal>
  );
}

// --- Structure editor -------------------------------------------------------

/**
 * The full structure editor: rename the table, add / rename / drop columns, and
 * change a column's type or constraints. Non-destructive edits use native
 * `ALTER TABLE`; a type/constraint change triggers the SQLite 12-step rebuild
 * (create → copy → drop → rename), which the `script` endpoint runs atomically.
 */
function EditStructureDialog({
  name,
  source,
  table,
  columns,
  foreignKeys,
  tables,
  onClose,
  onSaved,
}: {
  name: string;
  source: string;
  table: string;
  columns: DataColumnMeta[];
  foreignKeys: DataForeignKey[];
  tables: DataTableMeta[];
  onClose: () => void;
  onSaved: (newName: string) => void;
}) {
  const [newName, setNewName] = useState(table);
  const [cols, setCols] = useState<EditColumn[]>(() =>
    editColumnsFrom(columns, foreignKeys),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const setCol = (i: number, patch: Partial<EditColumn>) =>
    setCols((cs) => cs.map((c, j) => (j === i ? { ...c, ...patch } : c)));
  const addCol = () => setCols((cs) => [...cs, blankEditColumn()]);

  // FK targets: every table in this datasource, including this one (a table may
  // reference itself). Columns come from the picked table's live schema; when the
  // picked table is the one being edited, offer its edited column names.
  const fkTables = tables.map((t) => t.name);
  const columnsOf = (t: string): string[] =>
    t === table
      ? cols.filter((c) => !c.drop && c.name.trim()).map((c) => c.name.trim())
      : (tables.find((x) => x.name === t)?.columns.map((c) => c.name) ?? []);
  const toggleFk = (i: number, on: boolean) =>
    setCol(i, {
      references: on
        ? { table: fkTables[0] ?? "", column: "", onDelete: "" }
        : null,
    });
  const setRef = (i: number, patch: Partial<ColumnRef>) =>
    setCols((cs) =>
      cs.map((c, j) =>
        j === i && c.references
          ? { ...c, references: { ...c.references, ...patch } }
          : c,
      ),
    );

  const { statements, rebuild } = useMemo(
    () => buildStructureStatements(table, newName, columns, foreignKeys, cols),
    [table, newName, columns, foreignKeys, cols],
  );
  const kept = cols.filter((c) => !c.drop && c.name.trim());
  const valid =
    newName.trim().length > 0 && kept.length > 0 && statements.length > 0;

  const apply = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      await execDataScript({
        path: { name, source },
        body: { statements },
        throwOnError: true,
      });
      onSaved(newName.trim());
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [name, source, statements, newName, onSaved]);

  return (
    <Modal
      title={`Edit structure — ${table}`}
      onClose={onClose}
      wide
      footer={
        <>
          {error && (
            <span className="mr-auto truncate text-xs text-danger">
              {error}
            </span>
          )}
          <Button variant="secondary" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={() => void apply()}
            disabled={!valid || busy}
          >
            {busy ? "Applying…" : "Apply changes"}
          </Button>
        </>
      }
    >
      <label className="mb-3 block">
        <span className="mb-1 block text-xs font-medium text-fg-muted">
          Table name
        </span>
        <Input value={newName} onChange={(e) => setNewName(e.target.value)} />
      </label>

      <div className="mb-1 flex items-center justify-between">
        <span className="text-xs font-medium text-fg-muted">Columns</span>
        <button
          onClick={addCol}
          className="text-xs font-medium text-accent hover:underline"
        >
          ＋ Add column
        </button>
      </div>
      <div className="space-y-1.5">
        <div className="flex items-center gap-2 px-0.5 text-[10px] uppercase tracking-wide text-fg-faint">
          <span className="flex-1">Name</span>
          <span className="w-28">Type</span>
          <span className="w-24">Default</span>
          <span className="w-8 text-center" title="Primary key">
            PK
          </span>
          <span className="w-10 text-center" title="NOT NULL">
            Req
          </span>
          <span className="w-8 text-center" title="Foreign key">
            FK
          </span>
          <span className="w-8 text-center" title="Drop column">
            Del
          </span>
        </div>
        {cols.map((c, i) => (
          <div key={i} className="flex flex-col gap-1.5">
            <div
              className={`flex items-center gap-2 ${c.drop ? "opacity-40 line-through" : ""}`}
            >
              <Input
                className="flex-1"
                value={c.name}
                onChange={(e) => setCol(i, { name: e.target.value })}
                placeholder="column"
              />
              <select
                className={`${inputClass} w-28`}
                value={c.type}
                onChange={(e) => setCol(i, { type: e.target.value })}
              >
                {COLUMN_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
              <Input
                className="w-24"
                value={c.default}
                onChange={(e) => setCol(i, { default: e.target.value })}
                placeholder="—"
                title="Raw SQL default, e.g. 0, 'active', CURRENT_TIMESTAMP"
              />
              <input
                type="checkbox"
                className="w-8"
                checked={c.primaryKey}
                onChange={(e) => setCol(i, { primaryKey: e.target.checked })}
                title="Primary key"
              />
              <input
                type="checkbox"
                className="w-10"
                checked={c.notNull}
                disabled={c.primaryKey}
                onChange={(e) => setCol(i, { notNull: e.target.checked })}
                title="NOT NULL"
              />
              <input
                type="checkbox"
                className="w-8"
                checked={c.references !== null}
                disabled={c.drop || fkTables.length === 0}
                onChange={(e) => toggleFk(i, e.target.checked)}
                title={
                  fkTables.length === 0
                    ? "No tables to reference"
                    : "Foreign key to another table"
                }
              />
              <input
                type="checkbox"
                className="w-8"
                checked={c.drop}
                onChange={(e) => setCol(i, { drop: e.target.checked })}
                title="Drop this column"
              />
            </div>
            {c.references && !c.drop && (
              <div className="flex items-center gap-2 pl-3 text-xs text-fg-faint">
                <span className="text-fg-faint">↳ references</span>
                <select
                  className={`${inputClass} w-40`}
                  value={c.references.table}
                  onChange={(e) =>
                    setRef(i, { table: e.target.value, column: "" })
                  }
                >
                  {fkTables.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
                <span>.</span>
                <select
                  className={`${inputClass} w-40`}
                  value={c.references.column}
                  onChange={(e) => setRef(i, { column: e.target.value })}
                >
                  <option value="">primary key</option>
                  {columnsOf(c.references.table).map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
                </select>
                <span className="ml-1">on delete</span>
                <select
                  className={`${inputClass} w-28`}
                  value={c.references.onDelete}
                  onChange={(e) => setRef(i, { onDelete: e.target.value })}
                >
                  {ON_DELETE_ACTIONS.map((a) => (
                    <option key={a} value={a}>
                      {a || "—"}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </div>
        ))}
      </div>

      {rebuild && (
        <p className="mt-3 rounded-md border border-warn/40 bg-warn/10 px-3 py-2 text-xs text-warn">
          A type, constraint or foreign-key change requires rebuilding the
          table. Foreign keys are re-created, but indexes and CHECK constraints
          are not carried over — add those back via a migration if needed.
        </p>
      )}

      <div className="mt-4">
        <span className="mb-1 block text-[10px] uppercase tracking-wide text-fg-faint">
          Generated SQL {rebuild ? "(rebuild)" : "(ALTER)"}
        </span>
        <pre className="max-h-40 overflow-auto rounded-md border border-edge bg-inset p-3 font-mono text-xs text-fg-muted">
          {statements.length
            ? statements.map((s) => `${s};`).join("\n")
            : "— no changes —"}
        </pre>
      </div>
    </Modal>
  );
}

// --- SQL --------------------------------------------------------------------
function SqlTab({
  name,
  source,
  appRunning,
}: {
  name: string;
  source: string;
  appRunning: boolean;
}) {
  const [sql, setSql] = useState("SELECT 1;");
  const [result, setResult] = useState<DataQueryResult | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const sqlRef = useRef(sql);
  sqlRef.current = sql;

  // While the app is running the server refuses mutating statements with 409
  // `app_running` (issue #889); reads stay allowed. `isReadStatement` mirrors
  // the server's read/write split (see ../lib/sqlStatement) so the UI never
  // surfaces an avoidable error.
  const writeBlocked = appRunning && !isReadStatement(sql);

  const run = useCallback(async () => {
    const text = sqlRef.current.trim().replace(/;\s*$/, "");
    if (!text) return;
    if (appRunning && !isReadStatement(text)) {
      setError("Stop the app before running mutating statements.");
      return;
    }
    setRunning(true);
    setError(null);
    setStatus(null);
    try {
      if (isReadStatement(text)) {
        const r = await queryData({
          path: { name, source },
          body: { sql: text },
          throwOnError: true,
        });
        setResult(r.data);
        setStatus(
          `${r.data.rows.length} row${r.data.rows.length === 1 ? "" : "s"}`,
        );
      } else {
        const r = await execData({
          path: { name, source },
          body: { sql: text },
          throwOnError: true,
        });
        setResult(null);
        setStatus(
          `${r.data.changed} row${r.data.changed === 1 ? "" : "s"} changed` +
            (r.data.lastInsertId != null
              ? ` · last id ${r.data.lastInsertId}`
              : ""),
        );
      }
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setRunning(false);
    }
  }, [name, source, appRunning]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="min-h-0 flex-[2] overflow-hidden border-b border-edge">
        <CodeEditor
          value={sql}
          language="sql"
          onChange={setSql}
          onSave={() => void run()}
        />
      </div>
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <Button
          onClick={() => void run()}
          disabled={running || writeBlocked}
          title={
            writeBlocked
              ? "Stop the app before running mutating statements"
              : undefined
          }
        >
          {running ? "Running…" : "▶ Run"}
        </Button>
        <span className="text-xs text-fg-faint">⌘/Ctrl+S runs</span>
        {writeBlocked && (
          <span className="text-xs text-warn">
            Stop the app to run mutating statements
          </span>
        )}
        {status && <span className="text-xs text-ok">{status}</span>}
        {error && <span className="truncate text-xs text-danger">{error}</span>}
      </div>
      <div className="min-h-0 flex-[3] overflow-auto">
        {result ? (
          <ResultGrid result={result} />
        ) : (
          <div className="p-6 text-sm text-fg-faint">
            Run a statement to see results. SELECT/WITH/PRAGMA return rows;
            anything else reports rows changed.
          </div>
        )}
      </div>
    </div>
  );
}

// --- Migrations -------------------------------------------------------------

function MigrationsTab({
  name,
  source,
  appRunning,
}: {
  name: string;
  source: string;
  appRunning: boolean;
}) {
  const [entries, setEntries] = useState<DataMigrationEntry[]>([]);
  const [dir, setDir] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const r = await getDataMigrations({
        path: { name, source },
        throwOnError: true,
      });
      setEntries(r.data.entries ?? []);
      setDir(r.data.dir);
    } catch (e) {
      setError(errMsg(e));
    }
  }, [name, source]);

  useEffect(() => {
    void load();
  }, [load]);

  const pending = entries.filter((e) => !e.applied).length;

  const apply = useCallback(async () => {
    if (appRunning) {
      setError("Stop the app before applying migrations.");
      return;
    }
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      const r = await migrateData({
        path: { name, source },
        throwOnError: true,
      });
      setNote(
        r.data.applied.length
          ? `Applied ${r.data.applied.length}: ${r.data.applied.join(", ")}`
          : "Nothing to apply — already up to date.",
      );
      await load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [name, source, load, appRunning]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <Button
          onClick={() => void apply()}
          disabled={busy || pending === 0 || appRunning}
          title={
            appRunning ? "Stop the app before applying migrations" : undefined
          }
        >
          {busy
            ? "Applying…"
            : pending > 0
              ? `Apply ${pending} pending`
              : "Up to date"}
        </Button>
        <span className="text-xs text-fg-faint">
          {dir || "db/migrations"} · {entries.length} file
          {entries.length === 1 ? "" : "s"}
        </span>
        {appRunning && (
          <span className="text-xs text-warn">Stop the app to migrate</span>
        )}
        {note && <span className="text-xs text-ok">{note}</span>}
        {error && <span className="truncate text-xs text-danger">{error}</span>}
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        {entries.length === 0 ? (
          <div className="p-6 text-sm text-fg-faint">
            No migrations in <code>{dir || "db/migrations"}</code>. Add ordered{" "}
            <code>*.sql</code> files there.
          </div>
        ) : (
          <table className="w-full text-sm">
            <tbody>
              {entries.map((e) => (
                <tr key={e.name} className="border-b border-edge/50">
                  <td className="px-4 py-1.5">
                    {e.applied ? (
                      <span
                        className="text-ok"
                        title={e.appliedAt ?? undefined}
                      >
                        ✓ applied
                      </span>
                    ) : (
                      <span className="text-warn">• pending</span>
                    )}
                  </td>
                  <td className="px-4 py-1.5 font-mono text-fg-muted">
                    {e.name}
                  </td>
                  <td className="px-4 py-1.5 text-xs text-fg-faint">
                    {e.appliedAt ? new Date(e.appliedAt).toLocaleString() : ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// --- Shared results grid ----------------------------------------------------

function ResultGrid({
  result,
  rowKey,
  onEditRow,
  onDeleteRow,
}: {
  result: DataQueryResult;
  /** Column holding a per-row identity (e.g. `__rowid`); hidden and used for edit/delete. */
  rowKey?: string;
  onEditRow?: (row: Record<string, unknown>) => void;
  onDeleteRow?: (rowid: unknown) => void;
}) {
  if (result.columns.length === 0 && result.rows.length === 0) {
    return <div className="p-6 text-sm text-fg-faint">No rows.</div>;
  }
  const cols = rowKey
    ? result.columns.filter((c) => c !== rowKey)
    : result.columns;
  const actions = Boolean(rowKey && (onEditRow || onDeleteRow));
  return (
    <table className="min-w-full border-collapse text-sm">
      <thead className="sticky top-0 bg-bg-subtle">
        <tr>
          {cols.map((c) => (
            <th
              key={c}
              className="border-b border-edge px-3 py-1.5 text-left font-semibold text-fg-muted"
            >
              {c}
            </th>
          ))}
          {actions && <th className="w-20 border-b border-edge px-3 py-1.5" />}
        </tr>
      </thead>
      <tbody>
        {result.rows.map((row, i) => (
          <tr key={i} className="group hover:bg-hover/50">
            {cols.map((c) => {
              const { text, muted } = cell(row[c]);
              return (
                <td
                  key={c}
                  className={`whitespace-pre border-b border-edge/40 px-3 py-1 font-mono text-xs ${
                    muted ? "italic text-fg-faint" : "text-fg"
                  }`}
                >
                  {text}
                </td>
              );
            })}
            {actions && (
              <td className="border-b border-edge/40 px-3 py-1 text-right">
                <span className="inline-flex gap-1.5 opacity-0 group-hover:opacity-100">
                  {onEditRow && (
                    <button
                      onClick={() => onEditRow(row)}
                      className="text-fg-faint hover:text-accent"
                      title="Edit row"
                    >
                      ✎
                    </button>
                  )}
                  {onDeleteRow && (
                    <button
                      onClick={() => {
                        if (confirm("Delete this row?"))
                          onDeleteRow(row[rowKey as string]);
                      }}
                      className="text-fg-faint hover:text-danger"
                      title="Delete row"
                    >
                      🗑
                    </button>
                  )}
                </span>
              </td>
            )}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
