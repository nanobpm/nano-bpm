// Urban domain-type reifier — ADR 0029 §4.1 + §6 (spike).
//
// The datasource schema is the *spine* of the domain model: a table is already a
// declared record type, so `DataSource.schema()`'s `TableMeta` reifies straight
// into a named TypeScript record. This module turns those tables into a generated
// `domain-rows.d.ts` (the "models directory" makers asked for — but generated, never
// hand-edited, so it can't drift from the DB), and the worker SDK + App loader
// type against it. Types are a compile/boot-time contract only: they are erased
// by the run/compile toolchain, and the shipped App is still untyped JSON on the wire
// (ADR 0029 §3) — the engine stays Zeebe-pure.
//
//   import { emitDomainDts } from "./domain-types.ts";
//   const tables = await (await openDataSource("app")).schema();
//   await writeFile("nano-generated/domain-rows.d.ts", emitDomainDts(tables));
//
// then a worker is typed end-to-end:
//
//   import type { DomainTables } from "./nano-generated/domain-rows.d.ts";
//   const rows = await db.query<DomainTables["customers"]>("SELECT * FROM customers");
//   rows[0].tier          // string | null — checked while authoring
//
// The manifest `types` registry (ADR 0029 §4.2, transient/non-persisted shapes)
// is the *other* source: `emitDomainTypeRegistry` renders it as a `DomainTypes`
// map keyed by type id, and `emitDomainModel` composes the table spine + the
// registry into the single generated file.
//
// This module is a **pure emitter**: it imports only the `TableMeta`/`ColumnMeta`
// *types* from the data SDK (a type-only import, erased at runtime), so it is
// materialised verbatim next to `data-cli.ts` as `nano-generated/domain-types.ts` and
// the data CLI's `domaintypes` op imports `emitDomainDts` from it. Opening the
// datasource, running `schema()`, and writing the file are the CLI op's job
// (`data_cli.ts`), which already owns the datasource seam and the file writer.

import type { ColumnMeta, TableMeta } from "./data_sdk.ts";

/**
 * The per-project generated-SDK directory. Visible (not dot-hidden) so makers
 * can read the generated accessors. Single source of truth for the TS side;
 * the Rust scaffolder (`projects.rs::GEN_DIR`) must agree.
 */
export const GEN_DIR = "nano-generated";

/** The generated file's basename, written under a project's `nano-generated/`. */
// Distinct stem from the runtime accessor `domain.ts`: TypeScript otherwise pairs
// `domain-rows.d.ts` as the *declaration of* `domain.ts`, so the accessor importing its
// own row types reads as a circular self-import (TS2303/TS2459) under a project
// `tsconfig.json` `paths` map. `-rows` also avoids colliding with the emitter
// module `domain-types.ts`. See the scaffolder's `PROJECT_TSCONFIG_JSON`.
export const DOMAIN_DTS = "domain-rows.d.ts";

/**
 * Map a SQLite declared column type to a TypeScript field type. SQLite is
 * dynamically typed but assigns each column a *type affinity* from its declared
 * name (the CREATE TABLE text), so we apply the affinity rules
 * (https://sqlite.org/datatype3.html §3.1) with two maker-friendly overrides the
 * DB Manager's type list implies: `BOOLEAN` → `boolean` and date/time types →
 * `string` (ISO text), matching how Urban surfaces them.
 */
export function sqliteAffinityToTs(declaredType: string): string {
  const t = declaredType.toUpperCase();
  if (t.length === 0) return "unknown"; // NONE affinity — opaque
  if (t.includes("BOOL")) return "boolean";
  if (t.includes("DATE") || t.includes("TIME")) return "string";
  if (t.includes("INT")) return "number";
  if (t.includes("CHAR") || t.includes("CLOB") || t.includes("TEXT")) return "string";
  if (t.includes("BLOB")) return "Uint8Array";
  if (t.includes("REAL") || t.includes("FLOA") || t.includes("DOUB")) return "number";
  return "number"; // NUMERIC affinity
}

/** A TS identifier is safe to use bare as a property name / type name. */
function isIdent(name: string): boolean {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name);
}

/** PascalCase a table name into an interface name, sanitised to an identifier. */
export function interfaceName(table: string): string {
  const pascal = table
    .replace(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : ""))
    .join("");
  const safe = pascal.replace(/[^A-Za-z0-9_$]/g, "");
  return /^[A-Za-z_$]/.test(safe) ? safe : `T_${safe}`;
}

/** The TS field type for a column: nullable columns widen with `| null`. */
function fieldType(col: ColumnMeta): string {
  const base = sqliteAffinityToTs(col.type);
  // A NOT NULL column (or the INTEGER PRIMARY KEY rowid alias) is never null.
  return col.notNull || col.primaryKey ? base : `${base} | null`;
}

/** One `export interface` block for a table under an explicit interface name. */
function tableInterfaceNamed(name: string, t: TableMeta): string {
  const fields = t.columns
    .map((c) => {
      const key = isIdent(c.name) ? c.name : JSON.stringify(c.name);
      const pk = c.primaryKey ? " (primary key)" : "";
      return `  /** ${c.type || "?"}${pk} */\n  ${key}: ${fieldType(c)};`;
    })
    .join("\n");
  return `export interface ${name} {\n${fields}\n}`;
}

/** One `export interface` block for a table (name derived from the table). */
function tableInterface(t: TableMeta): string {
  return tableInterfaceNamed(interfaceName(t.name), t);
}

/**
 * Emit the full `domain-rows.d.ts` from a datasource's tables: one `interface` per
 * table plus a `DomainTables` lookup keyed by the *raw* table name (the stable
 * wire identifier), so callers write `DomainTables["customers"]`.
 */
export function emitDomainDts(tables: TableMeta[]): string {
  const header =
    "// AUTO-GENERATED by nanobpmn from the datasource schema (ADR 0029 §4.1).\n" +
    "// Do not edit — regenerated from the live tables. Erased by the toolchain.\n" +
    "// eslint-disable\n";
  if (tables.length === 0) {
    return `${header}\nexport interface DomainTables {}\n`;
  }
  const interfaces = tables.map(tableInterface).join("\n\n");
  const map = tables
    .map((t) => `  ${JSON.stringify(t.name)}: ${interfaceName(t.name)};`)
    .join("\n");
  return `${header}\n${interfaces}\n\n/** Every table keyed by its wire name — index it for a row type. */\nexport interface DomainTables {\n${map}\n}\n`;
}

/** One datasource's introspected tables, paired with its manifest alias. */
export interface SourceSchema {
  source: string;
  tables: TableMeta[];
}

/**
 * The interface name for a table in a multi-source union: the source alias is
 * PascalCased and prefixed to the table's interface name so tables that share a
 * name across datasources (e.g. `app.customers` and `analytics.customers`) get
 * distinct, collision-free interfaces (`AppCustomers`, `AnalyticsCustomers`).
 */
function prefixedInterfaceName(source: string, table: string): string {
  return `${interfaceName(source)}${interfaceName(table)}`;
}

/**
 * Emit `domain-rows.d.ts` for an App that declares *multiple* datasources: one
 * `interface` per (source, table), a `DomainSources` map keyed by alias then
 * wire table name, and a `DomainTables` alias to the default source so the
 * single-source convention (`DomainTables["customers"]`) keeps working. When
 * there is one source (or none) this is byte-identical to `emitDomainDts` so the
 * common case stays stable.
 */
export function emitDomainDtsForSources(
  sources: SourceSchema[],
  defaultSource?: string,
): string {
  if (sources.length <= 1) {
    return emitDomainDts(sources[0]?.tables ?? []);
  }
  const header =
    "// AUTO-GENERATED by nanobpmn from the datasource schemas (ADR 0029 §4.1/§6).\n" +
    "// Do not edit — regenerated from the live tables. Erased by the toolchain.\n" +
    "// eslint-disable\n";

  const interfaces = sources
    .flatMap((s) =>
      s.tables.map((t) =>
        tableInterfaceNamed(prefixedInterfaceName(s.source, t.name), t)
      )
    )
    .join("\n\n");

  const sourceMap = sources
    .map((s) => {
      if (s.tables.length === 0) return `  ${JSON.stringify(s.source)}: {};`;
      const tableMap = s.tables
        .map((t) =>
          `    ${JSON.stringify(t.name)}: ${prefixedInterfaceName(s.source, t.name)};`
        )
        .join("\n");
      return `  ${JSON.stringify(s.source)}: {\n${tableMap}\n  };`;
    })
    .join("\n");

  const def = defaultSource && sources.some((s) => s.source === defaultSource)
    ? defaultSource
    : sources[0].source;

  return `${header}\n${interfaces}\n\n` +
    `/** Every datasource keyed by alias, then table by wire name. */\n` +
    `export interface DomainSources {\n${sourceMap}\n}\n\n` +
    `/** The default datasource's tables — index by wire name. */\n` +
    `export type DomainTables = DomainSources[${JSON.stringify(def)}];\n`;
}

// --- manifest `types` registry (ADR 0029 §4.2) ------------------------------

/** One field of a declared domain type in the manifest `types` registry. */
export interface DomainFieldDef {
  /** A primitive keyword or the id of another registry type (nominal ref). */
  type: string;
  optional?: boolean;
  list?: boolean;
}

/** A declared (transient) domain record type from the manifest `types` block. */
export interface DomainTypeDef {
  name?: string;
  match?: string;
  table?: string;
  fields: Record<string, DomainFieldDef>;
}

/** The manifest `types` registry: type id → declared record type. */
export type DomainTypeRegistry = Record<string, DomainTypeDef>;

/** Manifest primitive field keywords → TS types (ADR 0029 §4.2 / schema). */
const PRIMITIVE_TS: Record<string, string> = {
  string: "string",
  number: "number",
  integer: "number",
  boolean: "boolean",
  date: "string",
  datetime: "string",
  json: "unknown",
};

/**
 * The TS type for a registry field. A primitive keyword maps directly (and takes
 * precedence over an identically named type, per the schema); anything else is a
 * nominal reference to another registry type, emitted as an indexed access into
 * `DomainTypes` (self-contained, so no top-level interface names can collide with
 * table interfaces). An unresolved reference degrades to `unknown`. `list` wraps
 * the base type in `[]`.
 */
function fieldTsType(field: DomainFieldDef, ids: Set<string>): string {
  const base = PRIMITIVE_TS[field.type] ??
    (ids.has(field.type) ? `DomainTypes[${JSON.stringify(field.type)}]` : "unknown");
  return field.list ? `${base}[]` : base;
}

/**
 * Emit the `DomainTypes` block for the manifest `types` registry: an inline map
 * keyed by type id, so a worker/form types against `DomainTypes["taxSubmission"]`
 * exactly as it types against `DomainTables["customers"]`. Returns `""` when the
 * registry is empty, so the table spine stays byte-identical when no types are
 * declared. Field references resolve to `DomainTypes[<id>]`; `optional` widens
 * the key with `?`.
 */
export function emitDomainTypeRegistry(types: DomainTypeRegistry): string {
  const ids = new Set(Object.keys(types));
  if (ids.size === 0) return "";
  const entries = Object.entries(types)
    .map(([id, def]) => {
      const fields = Object.entries(def.fields ?? {})
        .map(([fname, field]) => {
          const key = isIdent(fname) ? fname : JSON.stringify(fname);
          const opt = field.optional ? "?" : "";
          return `    ${key}${opt}: ${fieldTsType(field, ids)};`;
        })
        .join("\n");
      const body = fields.length > 0 ? `{\n${fields}\n  }` : "{}";
      return `  ${JSON.stringify(id)}: ${body};`;
    })
    .join("\n");
  return `/** Declared (transient) domain types from the manifest \`types\` registry (ADR 0029 §4.2), keyed by id. */\nexport interface DomainTypes {\n${entries}\n}\n`;
}

/**
 * Compose the full `domain-rows.d.ts`: the datasource table spine (every source,
 * ADR 0029 §6) followed by the manifest `types` registry (§4.2). This is the
 * single entry point the `domaintypes` op uses.
 */
export function emitDomainModel(
  sources: SourceSchema[],
  defaultSource: string | undefined,
  types: DomainTypeRegistry,
): string {
  const spine = emitDomainDtsForSources(sources, defaultSource);
  const registry = emitDomainTypeRegistry(types);
  return registry ? `${spine}\n${registry}` : spine;
}

// --- domain bindings: the typed data-object accessor (ADR 0029 §6) ----------

/** The generated bindings file's basename — the typed `openDomain()` accessor
 * materialised next to `domain-rows.d.ts` under a project's `nano-generated/`. */
export const DOMAIN_BINDINGS = "domain.ts";

/** The primary-key column for a table's `Table` gateway: its first declared PK
 * column, or `id` when the table declares none. */
function primaryKeyOf(t: TableMeta): string {
  return t.columns.find((c) => c.primaryKey)?.name ?? "id";
}

/**
 * Emit `domain.ts`: a typed data-object accessor over the App's default
 * datasource, so a worker writes `db.orders.insert({...})` / `db.orders.get(id)`
 * instead of hand-writing SQL (ADR 0029 §6 — the Delphi data-module idea). The
 * generic `Table<T>` runtime lives in `data-sdk.ts`; this generated module only
 * binds each table name to its row type (from `domain-rows.d.ts`) and primary key, so
 * it imports nothing but the sibling SDK (relative) and a type-only `domain-rows.d.ts`
 * — staying runtime-agnostic and erased by the run/compile toolchain. When the App
 * declares multiple datasources this emits a *keyed* `openDomain<K>(source?)` whose
 * returned accessor is typed against that source's tables (defaulting to the default
 * source); `db.raw` is the per-source escape hatch. Single-source apps keep the
 * zero-arg `openDomain()` concrete form (byte-stable).
 */
/** Members `openDomain()` reserves on the returned handle — the `DomainBase`
 * escape hatch (`raw`/`close`). A table whose wire name is one of these would
 * clobber the escape hatch (or the type intersection), so it is dropped from
 * the typed table surface; it stays reachable via `db.raw.table("<name>", pk)`. */
const RESERVED_DOMAIN_MEMBERS = new Set(["raw", "close"]);

export function emitDomainBindings(
  sources: SourceSchema[],
  defaultSource?: string,
): string {  const header =
    "// AUTO-GENERATED by nanobpmn from the datasource schema (ADR 0029 §4.1/§6).\n" +
    "// The typed data-object layer: `openDomain()` → one `Table<T>` per table, so\n" +
    "// workers manipulate typed rows instead of hand-writing SQL. Do not edit —\n" +
    "// regenerated from the live tables. Erased to plain JS by the toolchain.\n" +
    "// eslint-disable\n";

  const multi = sources.length > 1;
  const def = defaultSource && sources.some((s) => s.source === defaultSource)
    ? defaultSource
    : sources[0]?.source;
  const primary = sources.find((s) => s.source === def);
  const tables = (primary?.tables ?? []).filter(
    (t) => !RESERVED_DOMAIN_MEMBERS.has(t.name),
  );

  // Multi-source apps: emit a *keyed* accessor so `openDomain("analytics")` is
  // typed against that source's tables, not the default's. The row-type spine
  // (`DomainSources`, keyed by alias then wire table name) is already emitted in
  // `domain-rows.d.ts`; here we bind a runtime table-descriptor map per source and
  // a generic `openDomain<K>` that indexes it. Single-source apps keep the
  // zero-arg concrete form below (byte-stable).
  if (multi) {
    return emitKeyedDomainBindings(header, sources, def!);
  }

  // Interface names must match `domain-rows.d.ts` exactly (prefixed when multi-source).
  const typeName = (table: string) =>
    multi ? prefixedInterfaceName(primary!.source, table) : interfaceName(table);
  const propKey = (table: string) =>
    isIdent(table) ? table : JSON.stringify(table);

  const typeImports = [...new Set(tables.map((t) => typeName(t.name)))].sort();
  const importTypes = typeImports.length > 0
    ? `import type { ${typeImports.join(", ")} } from "./${DOMAIN_DTS}";\n`
    : "";

  const fields = tables
    .map((t) => `  readonly ${propKey(t.name)}: Table<${typeName(t.name)}>;`)
    .join("\n");
  const binds = tables
    .map((t) =>
      `    ${propKey(t.name)}: raw.table<${typeName(t.name)}>(` +
      `${JSON.stringify(t.name)}, ${JSON.stringify(primaryKeyOf(t))}),`
    )
    .join("\n");

  const domainIface = `export interface Domain {\n` +
    `  /** The underlying datasource handle — the raw-SQL escape hatch. */\n` +
    `  readonly raw: DataSource;\n` +
    (fields ? `${fields}\n` : "") +
    `  /** Close the underlying connection. */\n  close(): void;\n}`;

  const openFn = `/**\n` +
    ` * Open the App's ${multi ? `default (\`${def}\`) ` : ""}datasource as a typed domain: each table is\n` +
    ` * a \`Table<T>\` gateway (\`db.orders.insert(...)\`, \`db.orders.get(id)\`), with the\n` +
    ` * raw \`DataSource\` at \`db.raw\` for anything the gateway doesn't cover.\n` +
    ` */\n` +
    `export async function openDomain(source?: string): Promise<Domain> {\n` +
    `  const raw = await openDataSource(source);\n` +
    `  return {\n    raw,\n` +
    (binds ? `${binds}\n` : "") +
    `    close: () => raw.close(),\n  };\n}`;

  return `${header}\n` +
    `import { openDataSource, type DataSource, type Table } from "./data-sdk.ts";\n` +
    importTypes +
    `\n${domainIface}\n\n${openFn}\n\nexport type { Table };\n`;
}

/**
 * Emit the *keyed* `domain.ts` for a multi-datasource App. `openDomain(source)`
 * is runtime-polymorphic (it opens whichever source you name) but was previously
 * type-*mono*morphic — always typed as the default source's `Domain`, so
 * `openDomain("analytics").customers` was mistyped against `app`'s tables. Here
 * `openDomain<K extends DomainSource>(source?: K)` selects the row-type map for
 * the requested source from `DomainSources` (emitted in `domain-rows.d.ts`), and a
 * runtime per-source table-descriptor map binds the matching `Table<T>` gateways.
 * Zero-arg / default-source calls keep resolving to the default source.
 */
function emitKeyedDomainBindings(
  header: string,
  sources: SourceSchema[],
  def: string,
): string {
  const tableDescriptors = sources
    .map((s) => {
      const rows = s.tables
        .filter((t) => !RESERVED_DOMAIN_MEMBERS.has(t.name))
        .map((t) =>
          `{ name: ${JSON.stringify(t.name)}, pk: ${JSON.stringify(primaryKeyOf(t))} }`
        )
        .join(", ");
      return `  ${JSON.stringify(s.source)}: [${rows}],`;
    })
    .join("\n");

  const runtimeMap =
    `const DOMAIN_TABLES: Record<string, { name: string; pk: string }[]> = {\n` +
    `${tableDescriptors}\n};\n` +
    `const DEFAULT_SOURCE = ${JSON.stringify(def)};`;

  const types =
    `/** The App's datasource aliases (ADR 0024). */\n` +
    `export type DomainSource = keyof DomainSources;\n\n` +
    `interface DomainBase {\n` +
    `  /** The underlying datasource handle — the raw-SQL escape hatch. */\n` +
    `  readonly raw: DataSource;\n` +
    `  /** Close the underlying connection. */\n  close(): void;\n}\n\n` +
    `type DomainTablesOf<M extends Record<string, object>> = { readonly [K in keyof M as K extends "raw" | "close" ? never : K]: Table<M[K]> };\n\n` +
    `/** A datasource opened as a typed domain: one \`Table<T>\` per table, plus\n` +
    ` * \`raw\`/\`close\`. Defaults to the default source (\`${def}\`) when unparameterised. */\n` +
    `export type Domain<M extends Record<string, object> = DomainSources[${JSON.stringify(def)}]> =\n` +
    `  DomainBase & DomainTablesOf<M>;`;

  const openFn =
    `/**\n` +
    ` * Open one of the App's datasources as a typed domain. The \`source\` key\n` +
    ` * selects that source's tables (\`openDomain("analytics").customers\`); omit it\n` +
    ` * for the default source (\`${def}\`). \`db.raw\` is the raw-SQL escape hatch.\n` +
    ` */\n` +
    `export async function openDomain<K extends DomainSource = ${JSON.stringify(def)}>(\n` +
    `  source?: K,\n` +
    `): Promise<Domain<DomainSources[K]>> {\n` +
    `  const key = (source ?? DEFAULT_SOURCE) as string;\n` +
    `  const raw = await openDataSource(key);\n` +
    `  const db: Record<string, unknown> = { raw, close: () => raw.close() };\n` +
    `  for (const t of DOMAIN_TABLES[key] ?? []) db[t.name] = raw.table(t.name, t.pk);\n` +
    `  return db as Domain<DomainSources[K]>;\n}`;

  return `${header}\n` +
    `import { openDataSource, type DataSource, type Table } from "./data-sdk.ts";\n` +
    `import type { DomainSources } from "./${DOMAIN_DTS}";\n\n` +
    `${runtimeMap}\n\n${types}\n\n${openFn}\n\nexport type { Table };\n`;
}


/** The generated worker-IO type map's basename — the `taskType → {in,out}` map
 * materialised next to `domain-rows.d.ts` under a project's `nano-generated/`. */
// Distinct stem from the runtime wrapper `workers.ts` (same pairing hazard as
// `DOMAIN_DTS`): a `worker-io.d.ts` reads as the declaration of `workers.ts`.
export const WORKER_BINDINGS_DTS = "worker-io.d.ts";

/** The generated typed-`defineWorker` wrapper's basename — re-exports the worker
 * SDK and overrides `defineWorker` with a taskType-keyed typed signature. */
export const WORKER_BINDINGS_TS = "workers.ts";

/** One `workers[]` declaration the codegen reads: the join `taskType` plus the
 * declared input/output domain-type ids (ADR 0033 §3). */
export interface WorkerBindingDecl {
  taskType: string;
  inputType?: string;
  outputType?: string;
  /** The `zeebe:header` keys declared on the task; reified into a typed
   * `job.customHeaders` shape (known keys, `string` values — headers are strings
   * on the wire). Empty/absent leaves `customHeaders` the untyped fallback. */
  headerKeys?: string[];
}

/** The TS type expression for a worker's declared input/output type id: an index
 * into the `DomainTypes` registry when the id is declared, else `undefined` (the
 * caller omits the entry so the taskType falls back to `WorkerVars`). */
function typeRefFor(id: string | undefined, declared: Set<string>): string | undefined {
  return id != null && declared.has(id) ? `DomainTypes[${JSON.stringify(id)}]` : undefined;
}

/** The TS type expression for a worker's declared custom-header keys: an object
 * type mapping each declared key to `string` (Zeebe headers are strings on the
 * wire) plus a `string` index signature so undeclared headers stay accessible
 * with the same honest wire type. Returns `undefined` when no keys are declared
 * (the caller omits the entry so the taskType falls back to `WorkerHdrs`). */
function headerRefFor(keys: string[] | undefined): string | undefined {
  const clean = [
    ...new Set(
      (keys ?? []).filter((k) => typeof k === "string").map((k) => k.trim()).filter((k) =>
        k.length > 0
      ),
    ),
  ];
  if (clean.length === 0) return undefined;
  const fields = clean.map((k) => `${JSON.stringify(k)}: string`).join("; ");
  return `{ ${fields}; [key: string]: string }`;
}

/**
 * Emit `worker-io.d.ts`: the machine-readable bridge from the process model to the
 * worker type system (ADR 0033 §3). Each declared worker's `taskType` maps to the
 * TS type of its input payload (`job.variables`, from `inputType`) and its result
 * (from `outputType`), resolved against the `DomainTypes` registry in
 * `domain-rows.d.ts`. Only entries whose type is actually declared are emitted; a
 * taskType with no declared type is absent and falls back to `WorkerVars` in the
 * typed `defineWorker`. Imports `DomainTypes` type-only (so it erases at compile
 * and stays dual-runtime) — and only when at least one entry references it, so an
 * app with no declared types still yields a valid (empty) map.
 */
export function emitWorkerBindings(
  workers: WorkerBindingDecl[],
  declaredTypeIds: Iterable<string>,
): string {
  const declared = new Set(declaredTypeIds);
  const propKey = (t: string) => JSON.stringify(t);
  const taskTypes = [
    ...new Set(
      workers
        .map((w) => w?.taskType)
        .filter((t): t is string => typeof t === "string" && t.length > 0),
    ),
  ];
  // The model-derived set of job types the typed `defineWorker` accepts. A finite
  // union when workers are declared (so `type:` autocompletes and rejects typos);
  // `string` for an app with none, so `defineWorker` stays usable pre-declaration.
  const taskTypeUnion = taskTypes.length > 0
    ? taskTypes.map((t) => JSON.stringify(t)).join(" | ")
    : "string";
  const inputs: string[] = [];
  const outputs: string[] = [];
  const headers: string[] = [];
  for (const w of workers) {
    if (typeof w?.taskType !== "string" || w.taskType.length === 0) continue;
    const inRef = typeRefFor(w.inputType, declared);
    if (inRef) inputs.push(`  ${propKey(w.taskType)}: ${inRef};`);
    const outRef = typeRefFor(w.outputType, declared);
    if (outRef) outputs.push(`  ${propKey(w.taskType)}: ${outRef};`);
    const hdrRef = headerRefFor(w.headerKeys);
    if (hdrRef) headers.push(`  ${propKey(w.taskType)}: ${hdrRef};`);
  }

  const header =
    "// AUTO-GENERATED by nanobpmn from the App manifest (ADR 0033 §3).\n" +
    "// The bridge from the process model to the worker type system: each declared\n" +
    "// worker's `taskType` maps to the TS type of its input payload (`job.variables`)\n" +
    "// and result, so the typed `defineWorker` types a handler by its job type. Do\n" +
    "// not edit — regenerated from the manifest. Erased to plain JS at compile.\n" +
    "// eslint-disable\n";

  const needsRegistry = inputs.length > 0 || outputs.length > 0;
  const importTypes = needsRegistry
    ? `import type { DomainTypes } from "./${DOMAIN_DTS}";\n`
    : "";

  const inputsIface = inputs.length > 0
    ? `export interface WorkerInputs {\n${inputs.join("\n")}\n}\n`
    : `export interface WorkerInputs {}\n`;
  const outputsIface = outputs.length > 0
    ? `export interface WorkerOutputs {\n${outputs.join("\n")}\n}\n`
    : `export interface WorkerOutputs {}\n`;
  const headersIface = headers.length > 0
    ? `export interface WorkerHeaders {\n${headers.join("\n")}\n}\n`
    : `export interface WorkerHeaders {}\n`;

  return `${header}\n` +
    importTypes +
    `\n/** Untyped fallback for a job whose worker declares no input/output type. */\n` +
    `export type WorkerVars = Record<string, unknown>;\n\n` +
    `/** Untyped fallback for a job whose worker declares no custom headers. */\n` +
    `export type WorkerHdrs = Record<string, unknown>;\n\n` +
    `/** Every declared worker \`taskType\` (ADR 0033 §3): the model-derived set the\n` +
    ` * typed \`defineWorker\` accepts, so \`type\` autocompletes and rejects unknown jobs. */\n` +
    `export type WorkerTaskType = ${taskTypeUnion};\n\n` +
    `/** Input payload (\`job.variables\`) per declared worker, keyed by \`taskType\`. */\n` +
    inputsIface +
    `\n/** Output payload (worker result) per declared worker, keyed by \`taskType\`. */\n` +
    outputsIface +
    `\n/** Custom headers (\`job.customHeaders\`) per declared worker, keyed by \`taskType\`.\n` +
    ` * Header values are strings on the wire, so each declared key maps to \`string\`;\n` +
    ` * the extra index signature keeps undeclared headers accessible (non-breaking). */\n` +
    headersIface;
}

/**
 * The static typed-`defineWorker` wrapper (`workers.ts`). It re-exports the whole
 * worker SDK and overrides `defineWorker` with a signature that keys off the
 * `type:` string literal: when a job type is present in the generated
 * `WorkerInputs`/`WorkerOutputs` (ADR 0033 §3), the handler's `job.variables` and
 * result are typed from the declared domain type; otherwise they fall back to
 * `WorkerVars`. The body is a pass-through — the wire stays untyped JSON (ADR 0029
 * §3). This file never changes with the schema, so it is written verbatim (unlike
 * the regenerated `worker-io.d.ts`). Dual-runtime: only type-level constructs +
 * `export *` + a type assertion, so Node's strip-only mode accepts it (ADR 0065).
 */
export function emitWorkerBindingsRuntime(): string {
  return "// AUTO-GENERATED by nanobpmn (ADR 0033 §3): the typed `defineWorker`.\n" +
    "// Re-exports the worker SDK and overrides `defineWorker` with a taskType-keyed\n" +
    "// typed signature (job.variables, job.customHeaders + result typed from the\n" +
    "// worker's declared input/output/header contract). Erased to a pass-through at\n" +
    "// runtime. Do not edit.\n" +
    "// eslint-disable\n\n" +
    `import { defineWorker as defineWorkerRaw } from "./worker-sdk.ts";\n` +
    `import type { WorkerOptions } from "./worker-sdk.ts";\n` +
    `import type { WorkerInputs, WorkerOutputs, WorkerHeaders, WorkerTaskType, WorkerVars, WorkerHdrs } from "./${WORKER_BINDINGS_DTS}";\n\n` +
    `export * from "./worker-sdk.ts";\n\n` +
    `type InFor<K extends WorkerTaskType> = K extends keyof WorkerInputs ? WorkerInputs[K] : WorkerVars;\n` +
    `type OutFor<K extends WorkerTaskType> = K extends keyof WorkerOutputs ? WorkerOutputs[K] : WorkerVars;\n` +
    `type HdrFor<K extends WorkerTaskType> = K extends keyof WorkerHeaders ? WorkerHeaders[K] : WorkerHdrs;\n\n` +
    `/**\n` +
    ` * Typed \`defineWorker\`: \`type\` is constrained to the model's declared job types\n` +
    ` * (\`WorkerTaskType\`, ADR 0033 §3) so it autocompletes and rejects unknown jobs,\n` +
    ` * and the handler's \`job.variables\`, \`job.customHeaders\` + result are typed from\n` +
    ` * the worker's declared \`inputType\`/\`outputType\`/header keys. A declared job type\n` +
    ` * with no declared type falls back to WorkerVars / WorkerHdrs.\n` +
    ` */\n` +
    `export function defineWorker<K extends WorkerTaskType>(\n` +
    `  opts: { type: K } & WorkerOptions<InFor<K> & object, OutFor<K> & object, HdrFor<K> & object>,\n` +
    `): void {\n` +
    `  defineWorkerRaw(opts as unknown as WorkerOptions);\n` +
    `}\n`;
}

// --- message-carried envelopes (ADR 0040 slice 2) --------------------------

/** The generated message-payload type map's basename. Distinct stem from the
 * runtime wrapper `messages.ts` (same pairing hazard as `DOMAIN_DTS`): a
 * `message-io.d.ts` reads as the declaration of `messages.ts`. */
export const MESSAGE_BINDINGS_DTS = "message-io.d.ts";

/** The generated typed-`publishMessage` wrapper's basename — re-exports the
 * worker SDK's `publishMessage` with a messageName-keyed typed signature. */
export const MESSAGE_BINDINGS_TS = "messages.ts";

/** One message binding the codegen reads: the message `name` plus the declared
 * envelope type ids (ADR 0040 slice 2). `inputType` is the *received* payload —
 * i.e. what a `publishMessage` caller sends. `outputType` is reserved. */
export interface MessageBindingDecl {
  messageName: string;
  inputType?: string;
  outputType?: string;
}

/**
 * Emit `message-io.d.ts`: the machine-readable bridge from the process model's
 * `bpmn:message` envelopes to the message type system (ADR 0040 slice 2). Each
 * named message contributes to the `MessageName` union (so `publishMessage`
 * autocompletes and rejects unknown messages); a message whose envelope declares
 * an `inputType` also maps its name to that payload type in `MessagePayloads`, so
 * `publishMessage(name, { variables })` is typed. Only entries whose type is
 * actually declared are emitted; a message with no declared type is absent from
 * `MessagePayloads` and its payload falls back to `MessageVars`. Imports
 * `DomainTypes` type-only, and only when at least one entry references it.
 */
export function emitMessageBindings(
  messages: MessageBindingDecl[],
  declaredTypeIds: Iterable<string>,
): string {
  const declared = new Set(declaredTypeIds);
  const propKey = (m: string) => JSON.stringify(m);
  const names = [
    ...new Set(
      messages
        .map((m) => m?.messageName)
        .filter((n): n is string => typeof n === "string" && n.length > 0),
    ),
  ];
  // The model-derived set of message names the typed `publishMessage` accepts. A
  // finite union when messages are declared; `string` for a model with none, so
  // `publishMessage` stays usable pre-declaration.
  const nameUnion = names.length > 0
    ? names.map((n) => JSON.stringify(n)).join(" | ")
    : "string";
  const payloads: string[] = [];
  for (const m of messages) {
    if (typeof m?.messageName !== "string" || m.messageName.length === 0) continue;
    const ref = typeRefFor(m.inputType, declared);
    if (ref) payloads.push(`  ${propKey(m.messageName)}: ${ref};`);
  }

  const header =
    "// AUTO-GENERATED by nanobpmn from the process model (ADR 0040 slice 2).\n" +
    "// The bridge from a `bpmn:message`'s data envelope to the message type system:\n" +
    "// each named message maps to the TS type of its payload, so the typed\n" +
    "// `publishMessage` types a call by its message name. Do not edit — regenerated\n" +
    "// from the model. Erased to plain JS at compile.\n" +
    "// eslint-disable\n";

  const needsRegistry = payloads.length > 0;
  const importTypes = needsRegistry
    ? `import type { DomainTypes } from "./${DOMAIN_DTS}";\n`
    : "";

  const payloadsIface = payloads.length > 0
    ? `export interface MessagePayloads {\n${payloads.join("\n")}\n}\n`
    : `export interface MessagePayloads {}\n`;

  return `${header}\n` +
    importTypes +
    `\n/** Untyped fallback payload for a message that declares no envelope type. */\n` +
    `export type MessageVars = Record<string, unknown>;\n\n` +
    `/** Every declared message \`name\` (ADR 0040 slice 2): the model-derived set the\n` +
    ` * typed \`publishMessage\` accepts, so \`name\` autocompletes and rejects unknown messages. */\n` +
    `export type MessageName = ${nameUnion};\n\n` +
    `/** Payload (\`variables\`) per declared message, keyed by \`name\`. */\n` +
    payloadsIface;
}

/**
 * The static typed-`publishMessage` wrapper (`messages.ts`). It re-exports the
 * worker SDK's `publishMessage` (and its option/result types) and overrides
 * `publishMessage` with a signature that keys off the `name` string literal: when
 * a message is present in the generated `MessagePayloads` (ADR 0040 slice 2), the
 * call's `variables` are typed from the declared domain type; otherwise they fall
 * back to `MessageVars`. The body is a pass-through — the wire stays untyped JSON.
 * This file never changes with the model, so it is written verbatim (unlike the
 * regenerated `message-io.d.ts`). Dual-runtime: only type-level constructs + a
 * type assertion, so Node's strip-only mode accepts it (ADR 0065).
 */
export function emitMessageBindingsRuntime(): string {
  return "// AUTO-GENERATED by nanobpmn (ADR 0040 slice 2): the typed `publishMessage`.\n" +
    "// Re-exports the worker SDK's `publishMessage` and overrides it with a\n" +
    "// messageName-keyed typed signature (variables typed from the message's declared\n" +
    "// envelope). Erased to a pass-through at runtime. Do not edit.\n" +
    "// eslint-disable\n\n" +
    `import { publishMessage as publishMessageRaw } from "./worker-sdk.ts";\n` +
    `import type { PublishMessageOptions, PublishMessageResult } from "./worker-sdk.ts";\n` +
    `import type { MessageName, MessagePayloads, MessageVars } from "./${MESSAGE_BINDINGS_DTS}";\n\n` +
    `export type { PublishMessageOptions, PublishMessageResult } from "./worker-sdk.ts";\n\n` +
    `type PayloadFor<K extends MessageName> = K extends keyof MessagePayloads ? MessagePayloads[K] : MessageVars;\n\n` +
    `/**\n` +
    ` * Typed \`publishMessage\`: \`name\` is constrained to the model's declared message\n` +
    ` * names (\`MessageName\`, ADR 0040 slice 2) so it autocompletes and rejects unknown\n` +
    ` * messages, and \`variables\` are typed from the message's declared envelope. A\n` +
    ` * declared message with no domain type falls back to MessageVars.\n` +
    ` */\n` +
    `export function publishMessage<K extends MessageName>(\n` +
    `  name: K,\n` +
    `  opts?: PublishMessageOptions<PayloadFor<K> & object>,\n` +
    `): Promise<PublishMessageResult> {\n` +
    `  return publishMessageRaw(name, opts as unknown as PublishMessageOptions);\n` +
    `}\n`;
}

// --- composed motion shapes: the fuse composition algebra (ADR 0040 §9/§10) ---
//
// A composed shape is a named, model-scoped declaration authored in the Modeller
// and carried in the `.bpmn` as a `nano:shape` (see `console/src/lib/shapeCarrier.ts`
// and the Rust scan `envelope_scan.rs`). It composes existing fused entities — DB
// tables, manifest `types`, and other shapes — via four ordered operations:
//
//   carry(ref)                 spread every field of `ref`
//   project(ref, fields, via)  spread only the named fields of `ref`
//   extend(name, type, ...)    add a process-authored field
//   reference(name, ref, ...)  nest (or spread) another shape
//
// Resolution folds the ops, in author (XML) order, into a flat `DomainTypeDef`, so
// a composed shape enters the same `DomainTypes` registry a manifest `type` does —
// every downstream consumer (envelope pickers, `defineWorker`, `publishMessage`,
// FEEL scopes) types against `DomainTypes["ApprovedOrder"]` with no new codegen
// path. Broken shapes are omitted (degrade to untyped) and reported as diagnostics.

/** One composition operation of a `nano:shape`, in author (XML) order. */
export type ShapeOp =
  | { op: "carry"; ref: string }
  | { op: "project"; ref: string; fields: string[]; via?: string }
  | { op: "extend"; name: string; type: string; optional?: boolean; list?: boolean }
  | { op: "reference"; name: string; ref: string; spread?: boolean; list?: boolean };

/** A composed motion-shape declaration lifted from the model (ADR 0040 §9). */
export interface ShapeDecl {
  /** The fuse identity; also the `DomainTypes` key the resolved shape lands under. */
  id: string;
  /** Optional human label (`nano:shape name`). */
  name?: string;
  /** The defining process id, for the `model:<processId>` provenance tag. */
  process?: string;
  /** The ordered composition operations. */
  ops: ShapeOp[];
  /** Model-level metadata (`nano:meta`), free-form key/value (ADR 0040 §5). */
  meta?: Record<string, string>;
}

/** A scan/resolve-time problem with a shape, surfaced like the `workers[]` drift
 * warning (never a silent merge). A shape with any `error` diagnostic is omitted
 * from the fuse; a `warning` (e.g. a deliberate field shadow) still resolves. */
export interface ShapeDiagnostic {
  /** The offending shape id. */
  shape: string;
  kind:
    | "unresolved-reference"
    | "reference-cycle"
    | "field-conflict"
    | "unknown-field"
    | "duplicate-id"
    | "ambiguous-reference"
    | "nominal-table-ref"
    | "same-id-collision";
  severity: "error" | "warning";
  message: string;
}

/** The result of resolving the project's shapes against the leaf fuse. */
export interface ShapeResolution {
  /** The resolved shapes as registry entries, keyed by shape id, ready to fold
   * into the manifest `types` registry before `emitDomainModel`. */
  types: DomainTypeRegistry;
  diagnostics: ShapeDiagnostic[];
}

/** An entity the fuse can resolve a `carry`/`project`/`reference` against: its
 * field map, plus (DB tables only) its FK columns for `via`-path validation. */
interface FuseEntity {
  fields: Record<string, DomainFieldDef>;
  /** FK column name → referenced table, for `project via` validation. */
  fks?: Record<string, string>;
}

/** Map one datasource column to a domain field. A nullable column widens to
 * `optional` (the composed shape is a motion snapshot: an absent value reads as
 * undefined, not SQL NULL). The manifest keyword is inferred from the column's
 * SQLite affinity so it flows through `fieldTsType` like any declared field;
 * affinities with no keyword (BLOB/opaque) fall back to `json`. */
function columnToField(col: ColumnMeta): DomainFieldDef {
  const ts = sqliteAffinityToTs(col.type);
  const keyword = ts === "string" || ts === "number" || ts === "boolean" ? ts : "json";
  const nullable = !(col.notNull || col.primaryKey);
  return nullable ? { type: keyword, optional: true } : { type: keyword };
}

/** Build the leaf entity index the shape fold resolves references against: every
 * datasource table (by raw wire name, and by `source.table` to disambiguate a
 * name shared across sources) and every manifest `type` (by id). A raw table name
 * shared across sources keeps the first (default-source-first) binding; the
 * qualified `source.table` alias is always unambiguous.
 *
 * When a manifest `type` id collides with a bare table name, the **type wins** the
 * unqualified id (it is the more first-class, `DomainTypes`-visible entity) and the
 * collision is surfaced via `ambiguousIds` so resolution can warn; the table stays
 * reachable through its `source.table` alias. `tableIds` records every id (bare +
 * qualified) that resolves to a DB table, so a nominal reference (which the emitter
 * can only express against `DomainTypes` keys) can be rejected. */
function leafEntityIndex(
  sources: SourceSchema[],
  types: DomainTypeRegistry,
): { index: Map<string, FuseEntity>; tableIds: Set<string>; ambiguousIds: Set<string> } {
  const index = new Map<string, FuseEntity>();
  const tableIds = new Set<string>();
  for (const s of sources) {
    for (const t of s.tables) {
      const fields: Record<string, DomainFieldDef> = {};
      const fks: Record<string, string> = {};
      for (const c of t.columns) fields[c.name] = columnToField(c);
      // Store FK targets as the unambiguous `source.table` id (FKs are intra-source)
      // so `via`-path following resolves the intended table, never a same-named
      // table in another source or a type-preferred bare id.
      for (const fk of t.foreignKeys ?? []) fks[fk.column] = `${s.source}.${fk.refTable}`;
      const entity: FuseEntity = { fields, fks };
      index.set(`${s.source}.${t.name}`, entity);
      tableIds.add(`${s.source}.${t.name}`);
      if (!index.has(t.name)) {
        index.set(t.name, entity);
        tableIds.add(t.name);
      }
    }
  }
  const ambiguousIds = new Set<string>();
  for (const [id, def] of Object.entries(types)) {
    // The manifest type wins an unqualified id that also names a table; the table
    // remains reachable by its `source.table` alias.
    if (tableIds.has(id)) ambiguousIds.add(id);
    index.set(id, { fields: { ...def.fields } });
  }
  return { index, tableIds, ambiguousIds };
}

/** Whether a manifest field keyword resolves to a primitive (vs a nominal ref). */
function isPrimitiveKeyword(type: string): boolean {
  return Object.prototype.hasOwnProperty.call(PRIMITIVE_TS, type);
}

/** The shape ids a shape references (carry/project/reference targets, and an
 * `extend` whose type names a shape) — the edges of the shape dependency graph. */
function referencedShapeIds(shape: ShapeDecl, shapeIds: Set<string>): string[] {
  const refs = new Set<string>();
  for (const op of shape.ops) {
    if (op.op === "carry" || op.op === "project" || op.op === "reference") {
      if (shapeIds.has(op.ref)) refs.add(op.ref);
    } else if (op.op === "extend" && shapeIds.has(op.type)) {
      refs.add(op.type);
    }
  }
  return [...refs];
}

/** Structural equality of two domain field defs (`type` + normalized optional/list
 * flags), so a differing property insertion order does not read as a conflict. */
function sameFieldDef(a: DomainFieldDef, b: DomainFieldDef): boolean {
  return (
    a.type === b.type &&
    !!a.optional === !!b.optional &&
    !!a.list === !!b.list
  );
}

/**
 * Resolve the project's composed shapes into `DomainTypeDef`s and diagnostics
 * (ADR 0040 §10). Leaves (DB tables, manifest types) fuse first; shapes resolve in
 * dependency order so a shape can carry another shape regardless of declaration
 * order. A shape in a reference cycle, or one that names an unresolvable id, is
 * omitted (degrades to untyped) with an `error` diagnostic; a deliberate field
 * shadow with a differing type resolves with a `field-conflict` warning.
 */
export function resolveShapes(
  shapes: ShapeDecl[],
  types: DomainTypeRegistry,
  sources: SourceSchema[] = [],
): ShapeResolution {
  const diagnostics: ShapeDiagnostic[] = [];
  const resolved: DomainTypeRegistry = {};
  if (shapes.length === 0) return { types: resolved, diagnostics };

  const { index, tableIds, ambiguousIds } = leafEntityIndex(sources, types);
  // A nominal reference (an `extend` type or a non-spread `reference`) must resolve
  // to a `DomainTypes` key at emit time — a manifest type or a resolved shape id.
  // DB tables are spread-only (their fields flatten via carry/project); nominally
  // referencing one would degrade to `unknown` in the emitted `.d.ts`, so we reject.
  const nominalIds = new Set<string>(Object.keys(types));
  // Duplicate shape ids are fuse-identity collisions: since resolution keys by id,
  // a later declaration would silently shadow an earlier one. Report every id that
  // appears more than once and omit all of its declarations from resolution.
  const idCounts = new Map<string, number>();
  for (const s of shapes) if (s.id) idCounts.set(s.id, (idCounts.get(s.id) ?? 0) + 1);
  const duplicated = new Set<string>();
  for (const [id, n] of idCounts) {
    if (n > 1) {
      duplicated.add(id);
      diagnostics.push({
        shape: id,
        kind: "duplicate-id",
        severity: "error",
        message: `shape id "${id}" is declared ${n} times; ids are fuse identities and must be unique`,
      });
    }
  }
  const byId = new Map<string, ShapeDecl>();
  for (const s of shapes) if (s.id && !duplicated.has(s.id)) byId.set(s.id, s);
  const shapeIds = new Set(byId.keys());

  // Cycle detection over the shape-only dependency graph (DFS three-colour). Every
  // shape on a back edge is failed; the reported path aids the maker's fix.
  const failed = new Set<string>();
  const colour = new Map<string, 0 | 1 | 2>(); // 0=unvisited 1=on-stack 2=done
  const visit = (id: string, stack: string[]): void => {
    colour.set(id, 1);
    stack.push(id);
    for (const dep of referencedShapeIds(byId.get(id)!, shapeIds)) {
      const c = colour.get(dep) ?? 0;
      if (c === 1) {
        const cycle = stack.slice(stack.indexOf(dep)).concat(dep);
        for (const n of cycle) {
          if (!failed.has(n)) {
            failed.add(n);
            diagnostics.push({
              shape: n,
              kind: "reference-cycle",
              severity: "error",
              message: `shape "${n}" is part of a reference cycle: ${cycle.join(" → ")}`,
            });
          }
        }
      } else if (c === 0) {
        visit(dep, stack);
      }
    }
    stack.pop();
    colour.set(id, 2);
  };
  for (const id of byId.keys()) if ((colour.get(id) ?? 0) === 0) visit(id, []);

  // Topological resolution: resolve a shape only after its (non-failed) shape
  // dependencies, adding each resolved shape to the index so later shapes see it.
  const done = new Set<string>();
  const resolveOne = (shape: ShapeDecl): void => {
    if (done.has(shape.id) || failed.has(shape.id)) return;
    // Resolve shape dependencies first (they may themselves be pending).
    for (const dep of referencedShapeIds(shape, shapeIds)) {
      const d = byId.get(dep);
      if (d && !done.has(dep) && !failed.has(dep)) resolveOne(d);
    }

    const fields: Record<string, DomainFieldDef> = {};
    let broken = false;
    const addField = (name: string, field: DomainFieldDef): void => {
      const existing = fields[name];
      if (existing && !sameFieldDef(existing, field)) {
        diagnostics.push({
          shape: shape.id,
          kind: "field-conflict",
          severity: "warning",
          message:
            `field "${name}" is contributed twice with differing types; the later value wins (author-order fold)`,
        });
      }
      fields[name] = field; // last (author-order) wins
    };
    const lookup = (ref: string): FuseEntity | undefined => index.get(ref);
    const unresolved = (ref: string): void => {
      broken = true;
      diagnostics.push({
        shape: shape.id,
        kind: "unresolved-reference",
        severity: "error",
        message: `shape "${shape.id}" references unknown entity "${ref}"`,
      });
    };
    // A bare id that names both a manifest type and a table resolves to the type;
    // warn so the (silent) precedence is visible and the maker can qualify.
    const noteAmbiguity = (ref: string): void => {
      if (ambiguousIds.has(ref)) {
        diagnostics.push({
          shape: shape.id,
          kind: "ambiguous-reference",
          severity: "warning",
          message:
            `"${ref}" names both a manifest type and a table; resolved to the type — qualify as "<source>.${ref}" to target the table`,
        });
      }
    };
    // Report a nominal reference (extend type / non-spread reference) that targets a
    // DB table, which cannot be expressed as a `DomainTypes` ref (it would degrade to
    // `unknown`); the maker should spread it instead (carry/project or spread=true).
    const nominalTableRef = (kind: string, ref: string): void => {
      broken = true;
      diagnostics.push({
        shape: shape.id,
        kind: "nominal-table-ref",
        severity: "error",
        message:
          `${kind} nominally references table "${ref}", which is not a DomainTypes entity; spread its fields (carry/project or reference spread="true") instead`,
      });
    };

    for (const op of shape.ops) {
      switch (op.op) {
        case "carry": {
          const e = lookup(op.ref);
          if (!e) { unresolved(op.ref); break; }
          noteAmbiguity(op.ref);
          for (const [k, f] of Object.entries(e.fields)) addField(k, f);
          break;
        }
        case "project": {
          const e = lookup(op.ref);
          if (!e) { unresolved(op.ref); break; }
          noteAmbiguity(op.ref);
          for (const fname of op.fields) {
            const f = e.fields[fname];
            if (!f) {
              broken = true;
              diagnostics.push({
                shape: shape.id,
                kind: "unknown-field",
                severity: "error",
                message: `projected field "${fname}" is not a field of "${op.ref}"`,
              });
              continue;
            }
            addField(fname, f);
          }
          if (op.via) validateVia(shape.id, op.via, index, diagnostics);
          break;
        }
        case "extend": {
          if (!isPrimitiveKeyword(op.type)) {
            if (nominalIds.has(op.type)) {
              // ok — a manifest type or an already-resolved shape id
            } else if (tableIds.has(op.type)) {
              nominalTableRef(`extend field "${op.name}"`, op.type);
              break;
            } else {
              broken = true;
              diagnostics.push({
                shape: shape.id,
                kind: "unresolved-reference",
                severity: "error",
                message:
                  `extend field "${op.name}" has type "${op.type}", which is neither a scalar keyword nor a fused entity`,
              });
              break;
            }
          }
          const field: DomainFieldDef = { type: op.type };
          if (op.optional) field.optional = true;
          if (op.list) field.list = true;
          addField(op.name, field);
          break;
        }
        case "reference": {
          const e = lookup(op.ref);
          if (!e) { unresolved(op.ref); break; }
          if (op.spread) {
            noteAmbiguity(op.ref);
            for (const [k, f] of Object.entries(e.fields)) addField(k, f);
          } else if (!nominalIds.has(op.ref)) {
            // `e` exists but the id is not a DomainTypes key — it is a DB table.
            nominalTableRef(`reference "${op.name}"`, op.ref);
          } else {
            const field: DomainFieldDef = { type: op.ref };
            if (op.list) field.list = true;
            addField(op.name, field);
          }
          break;
        }
      }
    }

    // same-id collision: the shape id shadows a leaf entity it does not source.
    const sourcesRef = new Set(
      shape.ops.flatMap((o) => (o.op === "extend" ? [] : [o.ref])),
    );
    if (index.has(shape.id) && !sourcesRef.has(shape.id)) {
      diagnostics.push({
        shape: shape.id,
        kind: "same-id-collision",
        severity: "error",
        message:
          `shape id "${shape.id}" collides with an existing fused entity it does not compose`,
      });
      broken = true;
    }

    done.add(shape.id);
    if (broken) {
      failed.add(shape.id);
      return;
    }
    const def: DomainTypeDef = { name: shape.name, fields };
    resolved[shape.id] = def;
    // Later shapes may carry this one; expose its resolved fields to the index and
    // mark it nominal-referenceable (it will be a `DomainTypes` key).
    index.set(shape.id, { fields });
    nominalIds.add(shape.id);
  };
  for (const s of shapes) if (s.id && !duplicated.has(s.id)) resolveOne(s);

  return { types: resolved, diagnostics };
}

/** Validate a `project via` FK path (`Entity.column[.column...]`): the leading
 * entity must resolve and carry the named column (an FK column on a DB table, or
 * any field on a manifest type/shape). Deeper hops are validated leniently — the
 * FK target is followed when the leading entity is a DB table with that FK. */
function validateVia(
  shape: string,
  via: string,
  index: Map<string, FuseEntity>,
  diagnostics: ShapeDiagnostic[],
): void {
  const parts = via.split(".");
  if (parts.length < 2) {
    diagnostics.push({
      shape,
      kind: "unknown-field",
      severity: "warning",
      message: `via path "${via}" is not of the form Entity.column`,
    });
    return;
  }
  // Resolve the starting entity as the *longest* dotted prefix present in the index
  // (so a qualified `source.table` start works, not just a bare id), leaving at least
  // one trailing segment as a hop column. The remaining segments are hop columns.
  let entity: FuseEntity | undefined;
  let start = 0;
  for (let p = 1; p < parts.length; p++) {
    const candidate = parts.slice(0, p).join(".");
    const hit = index.get(candidate);
    if (hit) {
      entity = hit;
      start = p;
    }
  }
  if (!entity) {
    diagnostics.push({
      shape,
      kind: "unknown-field",
      severity: "warning",
      message: `via path "${via}" starts at unknown entity "${parts.slice(0, -1).join(".")}"`,
    });
    return;
  }
  let cursor: FuseEntity = entity;
  for (let i = start; i < parts.length; i++) {
    const col = parts[i];
    const hasField = Object.prototype.hasOwnProperty.call(cursor.fields, col);
    const fkTarget: string | undefined = cursor.fks?.[col];
    if (!hasField && !fkTarget) {
      diagnostics.push({
        shape,
        kind: "unknown-field",
        severity: "warning",
        message: `via path "${via}" hops through unknown column "${col}"`,
      });
      return;
    }
    // Follow the FK to the next entity when we can; otherwise stop (lenient).
    const next: FuseEntity | undefined = fkTarget ? index.get(fkTarget) : undefined;
    if (!next) return;
    cursor = next;
  }
}

// --- model-level metadata: the typed accessor (ADR 0040 §5) ------------------
//
// A model carries free-form governance/state metadata *about the process itself*
// (e.g. `classification=internal`) as `nano:meta` key/value siblings of the
// `nano:shapes` container (see `console/src/lib/shapeCarrier.ts` and the Rust scan
// `envelope_scan.rs`). The scan lifts these into `derivedMeta`; the `domaintypes`
// op folds them into a typed `meta.ts` accessor so app code can query them
// app-wide (`@nanobpm/meta`) without a second store, and into the structured fuse
// (`domain.json`) tagged with their `model:<processId>` provenance.

/** The basename of the generated typed model-metadata accessor (`@nanobpm/meta`). */
export const META_TS = "meta.ts";

/** One model-level metadata entry (`nano:meta`), tagged with its defining process
 * for the `model:<processId>` provenance (ADR 0040 §5). */
export interface MetaDecl {
  /** The defining process id, for provenance. */
  process?: string;
  key: string;
  value: string;
}

/** Fold the scanned model-level metadata into a single key→value record. When two
 * models declare the same key the last (scan order) wins — the fold is a flat
 * app-wide view where a later declaration overrides an earlier one (matching the
 * editor's "last write wins" affordance and JS object semantics). Empty keys are
 * ignored. The accumulator is a **null-prototype** dict: keys are user-authored, so
 * a plain `{}` would route a `__proto__` key through the `Object.prototype` setter
 * (creating no own property → silently dropped from `Object.keys`) and let inherited
 * members shadow real ones. `Object.create(null)` makes every assignment an own data
 * property, so all keys (including `__proto__`) round-trip deterministically. */
export function foldMeta(metas: MetaDecl[]): Record<string, string> {
  const out: Record<string, string> = Object.create(null);
  for (const m of metas) {
    const key = (m?.key ?? "").trim();
    if (!key) continue;
    out[key] = m.value ?? "";
  }
  return out;
}

/**
 * Emit `meta.ts`: the typed model-metadata accessor (`@nanobpm/meta`, ADR 0040
 * §5). `AppMeta` keys the declared metadata as string-literal properties so
 * `meta("classification")` autocompletes and is checked; `appMeta` is the concrete
 * record; the overloaded `meta()` returns the typed value for a known key and
 * `string | undefined` for a dynamic one. Dual-runtime: only type-level constructs
 * plus a plain object, so Node's strip-only mode accepts it (ADR 0065).
 */
export function emitMeta(metas: MetaDecl[]): string {
  const folded = foldMeta(metas);
  const keys = Object.keys(folded);
  const propKey = (k: string) => (isIdent(k) ? k : JSON.stringify(k));
  const header =
    "// AUTO-GENERATED by nanobpmn from the process model (ADR 0040 §5): the typed\n" +
    "// model-metadata accessor. Each `nano:meta` key/value the models declare becomes\n" +
    "// a checked `AppMeta` property, so `meta(key)` is typed and app-wide queryable\n" +
    "// without a second store. Do not edit — regenerated from the model.\n" +
    "// eslint-disable\n\n";
  const iface = keys.length > 0
    ? `export interface AppMeta {\n${keys.map((k) => `  ${propKey(k)}: string;`).join("\n")}\n}\n`
    : `export interface AppMeta {}\n`;
  // `appMeta` is a **null-prototype** dict populated by bracket assignment: meta
  // keys are user-authored, so a prototype chain would let `meta("toString")` /
  // `meta("constructor")` return an inherited function instead of `undefined`, and
  // a literal `__proto__` key would corrupt the object. `Object.create(null)` +
  // `m[key] = …` makes every lookup an own-property read and every write a plain
  // data property (safe even for a `__proto__` key). Strip-safe (plain JS + erased
  // annotations, ADR 0065).
  const constDecl = keys.length > 0
    ? `export const appMeta: AppMeta = (() => {\n` +
      `  const m: Record<string, string> = Object.create(null);\n` +
      keys.map((k) => `  m[${JSON.stringify(k)}] = ${JSON.stringify(folded[k])};`).join("\n") +
      `\n  return m as AppMeta;\n})();\n`
    : `export const appMeta: AppMeta = Object.create(null) as AppMeta;\n`;
  const accessor =
    `\n/** Read a model-level metadata value. A declared key is typed; a dynamic key\n` +
    ` * returns \`string | undefined\`. */\n` +
    `export function meta<K extends keyof AppMeta>(key: K): AppMeta[K];\n` +
    `export function meta(key: string): string | undefined;\n` +
    `export function meta(key: string): string | undefined {\n` +
    `  return (appMeta as Record<string, string>)[key];\n` +
    `}\n`;
  return `${header}/** Every model-level metadata key the App's models declare (ADR 0040 §5). */\n` +
    iface + "\n" + constDecl + accessor;
}

// --- the structured fused domain model: domain.json (ADR 0040 §1, OQ1) -------
//
// The computed fuse is persisted as a generated, git-ignored `nano-generated/
// domain.json` — a fast-read structured index for the IDE/codegen so a reader need
// not re-scan every model + datasource. It is a *cache*, never a source: the
// `domaintypes` op regenerates it write-through on any source change, and an
// `inputsHash` over its own content lets a reader detect staleness cheaply.

/** The basename of the generated structured fuse cache (ADR 0040 §1). */
export const DOMAIN_MODEL_JSON = "domain.json";

/** A field of a fused entity in `domain.json`: the resolved domain field plus its
 * name. `type` is a primitive keyword or the id of another fused entity. */
interface FusedFieldJson extends DomainFieldDef {
  name: string;
}

/** One fused entity in `domain.json`, tagged with its provenance and kind. */
interface FusedEntityJson {
  id: string;
  kind: "table" | "type" | "shape";
  /** `db:<source>.<table>` | `manifest:<id>` | `model:<processId>`. */
  provenance: string;
  name?: string;
  fields: FusedFieldJson[];
}

/** FNV-1a (32-bit) hex of a string — a small, dependency-free content tag for the
 * fuse cache's `inputsHash` (staleness detection, not security). */
function fnv1aHex(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

/** Convert a resolved field map to the ordered `domain.json` field list. */
function fieldsJson(fields: Record<string, DomainFieldDef>): FusedFieldJson[] {
  return Object.entries(fields ?? {}).map(([name, f]) => {
    const out: FusedFieldJson = { name, type: f.type };
    if (f.optional) out.optional = true;
    if (f.list) out.list = true;
    return out;
  });
}

/**
 * Emit `domain.json`: the structured Fused Domain Model (ADR 0040 §1). Assembles
 * every fused entity — DB tables (`db:` provenance), manifest `types` (`manifest:`),
 * and composed motion shapes (`model:<processId>`, or bare `model` for an unsaved
 * editor shape with no process) — with its resolved fields, plus
 * the model-level metadata and the shape diagnostics. An `inputsHash` — FNV-1a
 * (hex) over the compact `JSON.stringify` of the model object with `inputsHash`
 * omitted (property-insertion order, not the pretty-printed bytes) — tags the
 * cache for staleness.
 */
export function emitDomainModelJson(input: {
  sources: SourceSchema[];
  default?: string;
  manifestTypes: DomainTypeRegistry;
  /** Resolved shapes paired with their declaration (for provenance/label). */
  shapes: { decl: ShapeDecl; def: DomainTypeDef }[];
  meta: MetaDecl[];
  diagnostics: ShapeDiagnostic[];
}): string {
  const entities: FusedEntityJson[] = [];
  for (const s of input.sources) {
    for (const t of s.tables) {
      const fields: Record<string, DomainFieldDef> = {};
      for (const c of t.columns) fields[c.name] = columnToField(c);
      entities.push({
        id: `${s.source}.${t.name}`,
        kind: "table",
        provenance: `db:${s.source}.${t.name}`,
        fields: fieldsJson(fields),
      });
    }
  }
  for (const [id, def] of Object.entries(input.manifestTypes)) {
    entities.push({
      id,
      kind: "type",
      provenance: `manifest:${id}`,
      ...(def.name ? { name: def.name } : {}),
      fields: fieldsJson(def.fields),
    });
  }
  for (const { decl, def } of input.shapes) {
    entities.push({
      id: decl.id,
      kind: "shape",
      provenance: decl.process ? `model:${decl.process}` : "model",
      ...(def.name ?? decl.name ? { name: def.name ?? decl.name } : {}),
      fields: fieldsJson(def.fields),
    });
  }
  const meta = input.meta
    .filter((m) => (m?.key ?? "").trim().length > 0)
    .map((m) => ({ ...(m.process ? { process: m.process } : {}), key: m.key, value: m.value ?? "" }));
  const model = {
    $generated: "nanobpmn ADR 0040 §1 Fused Domain Model — do not edit; regenerated by the domaintypes op",
    version: 1,
    ...(input.default ? { default: input.default } : {}),
    sources: input.sources.map((s) => s.source),
    entities,
    meta,
    diagnostics: input.diagnostics,
    inputsHash: "",
  };
  model.inputsHash = fnv1aHex(JSON.stringify({ ...model, inputsHash: undefined }));
  return `${JSON.stringify(model, null, 2)}\n`;
}
