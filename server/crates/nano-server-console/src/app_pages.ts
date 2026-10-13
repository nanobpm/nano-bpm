// The Urban App page runtime (ADR 0042 §3) — materialised into an app as
// `nano-generated/app-pages.ts` and imported as `@nanobpm/app`. It turns a composed
// `page.json` (authored in the console Page Composer) into a served, data-bound
// screen with no hand-written frontend or API.
//
// It imports neither `@nanobpm/data` nor `@nanobpm/nano-sdk`: the app's `main.ts`
// passes the already-opened datasource + engine client in, so this module stays
// self-contained, testable (`app_pages_test.ts`), and free of the hyphenated
// emit-time specifiers that make `data-cli.ts` un-typecheckable in-tree.
//
// Endpoints (ADR 0026 §1 action API):
//   GET  /app/pages/<id>                        → the page's page.json
//   GET  /app/data/<source>/<table>[?where&order]→ rows (filtered/ordered, whitelisted)
//   POST /app/actions/start/<process>           → createProcessInstance (attended-sync)
//   POST /app/actions/cancel                    → cancelProcessInstance (row cancel)
//   POST /app/actions/message                   → publishMessage (row/detail answer)
//   GET  /  (+ /app/runtime.js)                 → the schema-driven browser renderer

import { createServer } from "node:http";

/** The subset of the `@nanobpm/data` DataSource the runtime needs. */
export interface PagesDataSource {
  query(sql: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
  schema(): Promise<{ name: string }[]>;
}

/** The subset of the `@nanobpm/nano-sdk` engine client the runtime needs. */
export interface PagesEngine {
  createProcessInstance(
    input: { processDefinitionId: string; variables?: Record<string, unknown> },
  ): Promise<{ processInstanceKey?: string | number }>;
  /** Cancel a running instance (row cancel action). */
  cancelProcessInstance(
    input: { processInstanceKey: string | number },
  ): Promise<unknown>;
  /** Publish a correlated message (row/detail publishMessage action). */
  publishMessage(
    input: {
      name: string;
      correlationKey: string;
      variables?: Record<string, unknown>;
    },
  ): Promise<unknown>;
}

export interface PagesContext {
  db: PagesDataSource;
  nano: PagesEngine;
  /** Directory holding `*.page.json` (relative to the app root). Default `pages`. */
  pagesDir?: string;
  /** The page served at `/`. Default `home`. */
  homePage?: string;
  /** Max rows a `dataGrid` fetch returns. Default 200. */
  rowLimit?: number;
  /** The name of the injected default datasource (the alias apps bind to). Default `app`. */
  sourceName?: string;
  /** Read a page file; injectable for tests. Defaults to `node:fs` readFile. */
  readPage?: (path: string) => Promise<string>;
}

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

/** A SQL identifier guard — a table name must be a bare identifier *and* a known
 * table (checked against `schema()`), so `/app/data/:table` can never inject SQL. */
const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Hard cap on a request body the `node:http` adapter will buffer. The page
 * runtime's handler never reads a request body (every route is addressed by path
 * and query), so nothing legitimate approaches this — but the adapter sits on a
 * public port, and an unbounded `for await (chunk of req)` would let a remote
 * client force memory growth at request-body speed. Over the cap: answer 413
 * without accumulating, and tear the socket down.
 */
const MAX_REQUEST_BODY_BYTES = 1 << 20; // 1 MiB

/**
 * The request handler, factored out of `servePages` so it is pure over its context
 * (injected `db`/`nano`/`readPage`) and unit-testable without a real HTTP server.
 */
export function createPagesHandler(ctx: PagesContext): (req: Request) => Promise<Response> {
  const pagesDir = ctx.pagesDir ?? "pages";
  const homePage = ctx.homePage ?? "home";
  const rowLimit = Math.max(0, Math.floor(Number(ctx.rowLimit) || 200));
  const sourceName = ctx.sourceName ?? "app";
  const readPage = ctx.readPage ??
    (async (p: string) => (await import("node:fs/promises")).readFile(p, "utf8"));

  // The table-name whitelist is memoised: an Urban app runs its migrations at
  // boot (before `servePages`), so the schema is stable for the process lifetime,
  // and the renderer refreshes grids repeatedly — re-introspecting the sqlite
  // schema (multiple PRAGMAs per table) on every `/app/data` hit would be a hot
  // path. Introspect once, lazily, and reuse. A rejected introspection is NOT
  // cached (the in-flight promise is cleared on failure) so a transient error
  // (locked/unavailable datasource) doesn't wedge every future request.
  let tableNames: Promise<Set<string>> | null = null;
  const knownTables = (): Promise<Set<string>> =>
    (tableNames ??= ctx.db.schema().then(
      (t) => new Set(t.map((x) => x.name)),
      (err) => {
        tableNames = null;
        throw err;
      },
    ));

  // Per-table column whitelist, introspected once per table via `PRAGMA
  // table_info`. Every filter/order column named in a `/app/data` query is checked
  // against this set before it reaches the SQL, so — like the table whitelist —
  // an attacker-supplied `where`/`order` can never inject. `table` is already
  // IDENT-guarded and table-whitelisted before we get here.
  const tableColumns = new Map<string, Promise<Set<string>>>();
  const knownColumns = (table: string): Promise<Set<string>> => {
    const cached = tableColumns.get(table);
    if (cached) return cached;
    const p = ctx.db
      .query(`PRAGMA table_info(${table})`)
      .then((rows) => new Set(rows.map((r) => String(r.name))))
      .catch((err) => {
        tableColumns.delete(table);
        throw err;
      });
    tableColumns.set(table, p);
    return p;
  };

  return async function handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const { pathname } = url;

    // ── the renderer shell ────────────────────────────────────────────────
    if (req.method === "GET" && (pathname === "/" || pathname === "/index.html")) {
      return new Response(rendererShell(homePage), {
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }
    if (req.method === "GET" && pathname === "/app/runtime.js") {
      return new Response(RENDERER_JS, {
        headers: { "content-type": "text/javascript; charset=utf-8" },
      });
    }

    // ── GET /app/pages/<id> ───────────────────────────────────────────────
    const pageMatch = pathname.match(/^\/app\/pages\/([A-Za-z0-9_-]+)$/);
    if (req.method === "GET" && pageMatch) {
      const id = pageMatch[1];
      try {
        const text = await readPage(`${pagesDir}/${id}.page.json`);
        return new Response(text, { headers: { "content-type": "application/json" } });
      } catch {
        return json({ error: `page "${id}" not found` }, 404);
      }
    }

    // ── GET /app/data/<source>/<table> ────────────────────────────────────
    const dataMatch = pathname.match(/^\/app\/data\/([A-Za-z0-9_-]+)\/([A-Za-z0-9_]+)$/);
    if (req.method === "GET" && dataMatch) {
      const source = dataMatch[1];
      const table = dataMatch[2];
      // v1 exposes only the injected default datasource; a request naming any
      // other source is rejected rather than silently served off the default.
      if (source !== sourceName) {
        return json({ error: `unknown datasource "${source}"` }, 404);
      }
      if (!IDENT.test(table)) return json({ error: "invalid table name" }, 400);
      let tables: Set<string>;
      try {
        tables = await knownTables();
      } catch {
        // A transient introspection failure isn't cached (see knownTables) — the
        // next request retries. Surface it as a 500 so the renderer can retry too.
        return json({ error: "schema introspection failed" }, 500);
      }
      if (!tables.has(table)) {
        return json({ error: `unknown table "${table}"` }, 404);
      }
      // Parse ?where=col:value (equality) or ?where=col:in:v1,v2 (set membership),
      // repeatable and ANDed, plus ?order=col:dir. Every column is whitelisted
      // against the table's real columns before it reaches the SQL; values are
      // always bound as `?` parameters (commas in an `in` list split values).
      let columns: Set<string>;
      try {
        columns = await knownColumns(table);
      } catch {
        return json({ error: "schema introspection failed" }, 500);
      }
      const params: unknown[] = [];
      const clauses: string[] = [];
      for (const raw of url.searchParams.getAll("where")) {
        const colon = raw.indexOf(":");
        if (colon <= 0) return json({ error: "invalid where clause" }, 400);
        const field = raw.slice(0, colon);
        const rest = raw.slice(colon + 1);
        if (!columns.has(field)) {
          return json({ error: `unknown column "${field}"` }, 400);
        }
        if (rest.startsWith("in:")) {
          const values = rest.slice(3).split(",");
          clauses.push(`${field} IN (${values.map(() => "?").join(", ")})`);
          for (const v of values) params.push(v);
        } else {
          clauses.push(`${field} = ?`);
          params.push(rest);
        }
      }
      let orderSql = "";
      const orderRaw = url.searchParams.get("order");
      if (orderRaw) {
        const colon = orderRaw.indexOf(":");
        const field = colon > 0 ? orderRaw.slice(0, colon) : orderRaw;
        const dir =
          colon > 0 && orderRaw.slice(colon + 1).toLowerCase() === "desc"
            ? "DESC"
            : "ASC";
        if (!columns.has(field)) {
          return json({ error: `unknown column "${field}"` }, 400);
        }
        orderSql = ` ORDER BY ${field} ${dir}`;
      }
      const whereSql = clauses.length ? ` WHERE ${clauses.join(" AND ")}` : "";
      const rows = await ctx.db.query(
        `SELECT * FROM ${table}${whereSql}${orderSql} LIMIT ${rowLimit}`,
        params,
      );
      return json({ rows });
    }

    // ── POST /app/actions/start/<process> ─────────────────────────────────
    const startMatch = pathname.match(/^\/app\/actions\/start\/([A-Za-z0-9_.-]+)$/);
    if (req.method === "POST" && startMatch) {
      const process = startMatch[1];
      let variables: Record<string, unknown> = {};
      try {
        const body = await req.json();
        if (body && typeof body === "object") {
          const v = (body as { variables?: unknown }).variables;
          // Only a plain object is a valid variable map — reject arrays/scalars/null
          // so a malformed body can't reach the engine as bad `variables`.
          if (v && typeof v === "object" && !Array.isArray(v)) {
            variables = v as Record<string, unknown>;
          }
        }
      } catch {
        return json({ error: "body must be JSON" }, 400);
      }
      try {
        const res = await ctx.nano.createProcessInstance({ processDefinitionId: process, variables });
        return json({ processInstanceKey: res.processInstanceKey ?? null });
      } catch (e) {
        return json({ error: String((e as Error)?.message ?? e) }, 502);
      }
    }

    // ── POST /app/actions/cancel ──────────────────────────────────────────
    if (req.method === "POST" && pathname === "/app/actions/cancel") {
      let key: string | number | undefined;
      try {
        const body = await req.json();
        const k = (body as { processInstanceKey?: unknown })?.processInstanceKey;
        if (typeof k === "string" || typeof k === "number") key = k;
      } catch {
        return json({ error: "body must be JSON" }, 400);
      }
      if (key === undefined || key === "") {
        return json({ error: "processInstanceKey is required" }, 400);
      }
      try {
        await ctx.nano.cancelProcessInstance({ processInstanceKey: key });
        return json({ ok: true });
      } catch (e) {
        return json({ error: String((e as Error)?.message ?? e) }, 502);
      }
    }

    // ── POST /app/actions/message ─────────────────────────────────────────
    if (req.method === "POST" && pathname === "/app/actions/message") {
      let name = "";
      let correlationKey = "";
      let variables: Record<string, unknown> = {};
      try {
        const body = (await req.json()) as {
          name?: unknown;
          correlationKey?: unknown;
          variables?: unknown;
        };
        if (typeof body?.name === "string") name = body.name;
        if (typeof body?.correlationKey === "string" || typeof body?.correlationKey === "number") {
          correlationKey = String(body.correlationKey);
        }
        const v = body?.variables;
        if (v && typeof v === "object" && !Array.isArray(v)) {
          variables = v as Record<string, unknown>;
        }
      } catch {
        return json({ error: "body must be JSON" }, 400);
      }
      if (!name) return json({ error: "message name is required" }, 400);
      if (!correlationKey) {
        return json({ error: "correlationKey is required" }, 400);
      }
      try {
        await ctx.nano.publishMessage({ name, correlationKey, variables });
        return json({ ok: true });
      } catch (e) {
        return json({ error: String((e as Error)?.message ?? e) }, 502);
      }
    }

    return json({ error: "not found" }, 404);
  };
}

/**
 * Start the page runtime on `port`, returning the `node:http` server handle. The
 * app's `main.ts` typically does: `servePages({ db, nano, port })`.
 *
 * Node is the sole JS/TS runtime (ADR 0065): the handler is a web-standard
 * `Request → Response` function (so it stays unit-testable without a socket), and
 * this adapter bridges it onto `node:http` — the one place a real HTTP server is
 * needed. The old `Deno.serve` binding is gone with the runtime.
 */
export function servePages(
  ctx: PagesContext & { port?: number },
): import("node:http").Server {
  const handle = createPagesHandler(ctx);
  const server = createServer((req, res) => {
    void (async () => {
      // node:http gives a Node request/response, not a web `Request`; rebuild a
      // web `Request` so `createPagesHandler` stays the single request-handling
      // path (no second, drifting router). Bodies are capped: nothing the handler
      // routes on reads one, so an over-cap body is refused (413) rather than
      // buffered — the adapter sits on a public port and must not let a client
      // force unbounded allocation.
      const chunks: Buffer[] = [];
      let total = 0;
      let over = false;
      for await (const chunk of req) {
        total += (chunk as Buffer).length;
        if (total > MAX_REQUEST_BODY_BYTES) {
          over = true;
          break;
        }
        chunks.push(chunk as Buffer);
      }
      if (over) {
        // Stop reading (the `break` above leaves the rest unread) and answer; the
        // connection is closed by `Connection: close` so the client is not left
        // blocked writing into a socket we have stopped draining.
        res.writeHead(413, { "content-type": "text/plain", connection: "close" });
        res.end("request body too large");
        req.destroy();
        return;
      }
      const body = chunks.length ? Buffer.concat(chunks) : undefined;
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      const request = new Request(url, {
        method: req.method,
        headers: req.headers as Record<string, string>,
        body: req.method === "GET" || req.method === "HEAD" ? undefined : body,
      });
      const response = await handle(request);
      res.writeHead(response.status, Object.fromEntries(response.headers));
      res.end(Buffer.from(await response.arrayBuffer()));
    })().catch((e) => {
      res.writeHead(500, { "content-type": "text/plain" });
      res.end(String((e as Error)?.message ?? e));
    });
  });
  return server.listen(ctx.port ?? 8090);
}

function escapeAttr(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function rendererShell(homePage: string): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Urban App</title>
  <style>${RENDERER_CSS}</style>
</head>
<body>
  <main id="page" data-home="${escapeAttr(homePage)}"><p class="pc-empty">Loading…</p></main>
  <script type="module" src="/app/runtime.js"></script>
</body>
</html>`;
}

const RENDERER_CSS = `
:root { color-scheme: light dark; --pc-edge:#d0d0d8; --pc-accent:#3b5bdb; }
* { box-sizing: border-box; }
body { margin:0; font:15px/1.5 system-ui,sans-serif; padding:2rem; max-width:64rem; margin-inline:auto; }
.pc-empty { opacity:.6; }
.pc-heading { font-size:1.6rem; font-weight:650; margin:0 0 .25rem; }
.pc-sub { opacity:.7; margin:.25rem 0 1rem; }
.pc-body { margin:.5rem 0; }
.pc-card { border:1px solid var(--pc-edge); border-radius:.6rem; padding:1rem 1.15rem; margin:1rem 0; }
.pc-card h2 { font-size:1rem; margin:0 0 .75rem; }
.pc-field { display:flex; flex-direction:column; gap:.25rem; margin-bottom:.6rem; }
.pc-field label { font-size:.8rem; opacity:.75; }
.pc-field input { padding:.5rem .6rem; border:1px solid var(--pc-edge); border-radius:.4rem; font:inherit; }
.pc-btn { padding:.5rem .9rem; border:0; border-radius:.4rem; background:var(--pc-accent); color:#fff; font:inherit; cursor:pointer; }
.pc-btn:disabled { opacity:.5; cursor:default; }
.pc-msg { font-size:.85rem; margin-top:.5rem; min-height:1.2em; }
.pc-msg.err { color:#c0392b; }
.pc-msg.ok { color:#2b8a3e; }
table.pc-grid { width:100%; border-collapse:collapse; font-size:.9rem; }
table.pc-grid th, table.pc-grid td { text-align:left; padding:.4rem .6rem; border-bottom:1px solid var(--pc-edge); }
table.pc-grid th { font-weight:600; opacity:.75; }
.pc-tabs { display:flex; gap:.5rem; margin-bottom:.75rem; }
.pc-tab { padding:.35rem .8rem; border:1px solid var(--pc-edge); border-radius:.4rem; background:transparent; color:inherit; font:inherit; cursor:pointer; }
.pc-tab.active { background:var(--pc-accent); color:#fff; border-color:var(--pc-accent); }
.pc-btn-sm { padding:.25rem .55rem; font-size:.8rem; margin-right:.3rem; }
.pc-chevron { background:transparent; color:inherit; border:1px solid var(--pc-edge); }
.pc-row-actions { white-space:nowrap; text-align:right; }
.pc-detail { padding:.75rem .25rem; }
.pc-detail-field { display:flex; gap:.5rem; font-size:.85rem; margin:.15rem 0; }
.pc-detail-label { opacity:.7; min-width:8rem; }
.pc-link { color:var(--pc-accent); }
.pc-child { margin:.6rem 0; }
.pc-child-title { font-size:.8rem; font-weight:600; opacity:.7; margin-bottom:.25rem; }
.pc-transcript { white-space:pre-wrap; max-height:22rem; overflow:auto; background:rgba(120,120,160,.08); padding:.5rem; border-radius:.4rem; font-size:.8rem; margin-top:.4rem; }
.pc-subform { margin-top:.75rem; padding:.6rem; border:1px dashed var(--pc-edge); border-radius:.5rem; }
.pc-subform-title { font-weight:600; font-size:.85rem; margin-bottom:.4rem; }
.pc-prompt { font-size:.85rem; opacity:.8; margin-bottom:.4rem; white-space:pre-wrap; }
.pc-textarea { width:100%; min-height:4rem; padding:.5rem; border:1px solid var(--pc-edge); border-radius:.4rem; font:inherit; }
`;

// The schema-driven browser renderer (ADR 0042 §3). Plain ES module string served at
// /app/runtime.js — it does NOT ship Craft.js (authoring is console-side only). It
// fetches the home page's page.json and renders text / actionForm / dataGrid nodes,
// wiring actionForm → /app/actions/start and dataGrid → /app/data (with a refresh).
const RENDERER_JS = String.raw`
const root = document.getElementById("page");
const HOME = root.dataset.home || "home";

async function getJSON(url, opts) {
  const r = await fetch(url, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || ("HTTP " + r.status));
  return body;
}

function el(tag, attrs = {}, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") n.className = v;
    else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2), v);
    else if (v != null) n.setAttribute(k, v);
  }
  for (const kid of kids) n.append(kid);
  return n;
}

function renderText(node) {
  const v = node.props.variant;
  const cls = v === "heading" ? "pc-heading" : v === "sub" ? "pc-sub" : "pc-body";
  return el(v === "heading" ? "h1" : "p", { class: cls }, node.props.text || "");
}

function renderActionForm(node) {
  const p = node.props;
  const card = el("section", { class: "pc-card" });
  if (p.title) card.append(el("h2", {}, p.title));
  const inputs = {};
  for (const f of p.fields || []) {
    const input = el("input", { type: "text", placeholder: f.label || f.key });
    inputs[f.key] = input;
    card.append(el("div", { class: "pc-field" }, el("label", {}, f.label || f.key), input));
  }
  const msg = el("p", { class: "pc-msg" });
  const btn = el("button", { class: "pc-btn" }, p.submitLabel || "Submit");
  btn.addEventListener("click", async () => {
    const variables = {};
    for (const [k, input] of Object.entries(inputs)) variables[k] = input.value;
    btn.disabled = true; msg.className = "pc-msg"; msg.textContent = "Submitting…";
    try {
      const res = await getJSON("/app/actions/start/" + encodeURIComponent(p.action.process),
        { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ variables }) });
      msg.className = "pc-msg ok";
      msg.textContent = "Started (instance " + (res.processInstanceKey ?? "?") + ")";
      for (const input of Object.values(inputs)) input.value = "";
      document.dispatchEvent(new CustomEvent("pc:refresh"));
    } catch (e) {
      msg.className = "pc-msg err"; msg.textContent = String(e.message || e);
    } finally { btn.disabled = false; }
  });
  card.append(btn, msg);
  return card;
}

function renderDataGrid(node) {
  const p = node.props;
  const card = el("section", { class: "pc-card" });
  if (p.title) card.append(el("h2", {}, p.title));
  const cols = p.columns || [];
  const tabs = p.tabs || [];
  const rowActions = p.rowActions || [];
  const detail = p.detail || null;
  const hasExtra = rowActions.length > 0 || detail != null;
  let activeFilter = p.data.filter || [];

  if (tabs.length) {
    const bar = el("div", { class: "pc-tabs" });
    activeFilter = tabs[0].filter || [];
    tabs.forEach((t, i) => {
      const b = el("button", { class: "pc-tab" + (i === 0 ? " active" : "") }, t.label);
      b.addEventListener("click", () => {
        activeFilter = t.filter || [];
        for (const c of bar.children) c.classList.remove("active");
        b.classList.add("active");
        refresh();
      });
      bar.append(b);
    });
    card.append(bar);
  }

  const headCells = cols.map((c) => el("th", {}, c.header || c.field));
  if (hasExtra) headCells.push(el("th", {}, ""));
  const thead = el("thead", {}, el("tr", {}, ...headCells));
  const tbody = el("tbody", {});
  const table = el("table", { class: "pc-grid" }, thead, tbody);
  card.append(table);
  const span = String((cols.length || 1) + (hasExtra ? 1 : 0));

  function dataUrl(source, tbl, filters, order) {
    let u = "/app/data/" + encodeURIComponent(source) + "/" + encodeURIComponent(tbl);
    const qs = [];
    for (const f of filters || []) {
      if (Array.isArray(f.in)) qs.push("where=" + encodeURIComponent(f.field + ":in:" + f.in.join(",")));
      else qs.push("where=" + encodeURIComponent(f.field + ":" + f.eq));
    }
    if (order && order.field) qs.push("order=" + encodeURIComponent(order.field + ":" + (order.dir || "asc")));
    return qs.length ? u + "?" + qs.join("&") : u;
  }

  async function fireAction(action, row) {
    if (action.kind === "cancelProcess") {
      return getJSON("/app/actions/cancel", { method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ processInstanceKey: row[action.keyField] }) });
    }
    if (action.kind === "publishMessage") {
      return getJSON("/app/actions/message", { method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: action.message, correlationKey: row[action.correlationKeyField],
          variables: { ...(action.variables || {}) } }) });
    }
    if (action.kind === "startProcess") {
      return getJSON("/app/actions/start/" + encodeURIComponent(action.process), { method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ variables: { ...(action.variables || {}) } }) });
    }
    throw new Error("unknown action");
  }

  function rowActionButton(row, ra) {
    if (ra.showWhenField && !row[ra.showWhenField]) return null;
    const b = el("button", { class: "pc-btn pc-btn-sm" }, ra.label);
    b.addEventListener("click", async (ev) => {
      ev.stopPropagation();
      if (ra.confirm && !confirm(ra.confirm)) return;
      b.disabled = true;
      try {
        await fireAction(ra.action, row);
        document.dispatchEvent(new CustomEvent("pc:refresh"));
      } catch (e) {
        b.disabled = false;
        alert(String(e.message || e));
      }
    });
    return b;
  }

  function detailForm(row) {
    const f = detail.form;
    if (!f || !row[f.showWhenField]) return null;
    const box = el("div", { class: "pc-subform" });
    if (f.title) box.append(el("div", { class: "pc-subform-title" }, f.title));
    if (f.promptField && row[f.promptField] != null) {
      box.append(el("div", { class: "pc-prompt" }, String(row[f.promptField])));
    }
    const input = el("textarea", { class: "pc-textarea", placeholder: f.inputLabel || f.inputKey });
    const msg = el("p", { class: "pc-msg" });
    const btn = el("button", { class: "pc-btn pc-btn-sm" }, f.submitLabel || "Submit");
    btn.addEventListener("click", async () => {
      btn.disabled = true; msg.className = "pc-msg"; msg.textContent = "Sending…";
      try {
        await getJSON("/app/actions/message", { method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ name: f.action.message, correlationKey: row[f.action.correlationKeyField],
            variables: { [f.inputKey]: input.value, ...(f.action.variables || {}) } }) });
        msg.className = "pc-msg ok"; msg.textContent = "Sent";
        document.dispatchEvent(new CustomEvent("pc:refresh"));
      } catch (e) {
        btn.disabled = false; msg.className = "pc-msg err"; msg.textContent = String(e.message || e);
      }
    });
    box.append(el("div", { class: "pc-field" }, input), btn, msg);
    return box;
  }

  async function childGrid(cg, row) {
    const wrap = el("div", { class: "pc-child" });
    if (cg.title) wrap.append(el("div", { class: "pc-child-title" }, cg.title));
    const ccols = cg.columns || [];
    const cbody = el("tbody", {});
    const ctable = el("table", { class: "pc-grid" },
      el("thead", {}, el("tr", {}, ...ccols.map((c) => el("th", {}, c.header || c.field)),
        ...(cg.lazyField ? [el("th", {}, "")] : []))), cbody);
    wrap.append(ctable);
    try {
      const { rows } = await getJSON(dataUrl(cg.source || "app", cg.table,
        [{ field: cg.childField, eq: row[cg.parentField] }], cg.orderBy));
      const cspan = String((ccols.length || 1) + (cg.lazyField ? 1 : 0));
      if (!rows.length) {
        cbody.append(el("tr", {}, el("td", { colspan: cspan }, "None")));
      }
      for (const cr of rows) {
        const cells = ccols.map((c) => el("td", {}, cr[c.field] == null ? "" : String(cr[c.field])));
        if (cg.lazyField) {
          const lf = cg.lazyField;
          const has = cr[lf.field] != null && String(cr[lf.field]).trim() !== "";
          const cell = el("td", {});
          if (has) {
            const toggle = el("button", { class: "pc-btn pc-btn-sm" }, lf.label || "Show");
            const pre = el("pre", { class: "pc-transcript", hidden: "" });
            pre.textContent = String(cr[lf.field]);
            toggle.addEventListener("click", () => {
              pre.hidden = !pre.hidden;
              toggle.textContent = pre.hidden ? (lf.label || "Show") : "Hide";
            });
            cell.append(toggle, pre);
          }
          cells.push(cell);
        }
        cbody.append(el("tr", {}, ...cells));
      }
    } catch (e) {
      cbody.append(el("tr", {}, el("td", { colspan: cspan }, String(e.message || e))));
    }
    return wrap;
  }

  function detailPanel(row) {
    const box = el("div", { class: "pc-detail" });
    if (detail.linkField && row[detail.linkField]) {
      const href = String(row[detail.linkField]);
      // Render as a link only for http(s); anything else (e.g. a javascript: URL
      // smuggled through row data) is shown as inert text. External links get
      // rel="noopener noreferrer" so the opened page can't reach window.opener.
      if (/^https?:\/\//i.test(href)) {
        box.append(el("a", { class: "pc-link", href, target: "_blank", rel: "noopener noreferrer" }, href));
      } else {
        box.append(el("span", { class: "pc-link" }, href));
      }
    }
    for (const df of detail.fields || []) {
      box.append(el("div", { class: "pc-detail-field" },
        el("span", { class: "pc-detail-label" }, df.label || df.field),
        el("span", {}, row[df.field] == null ? "" : String(row[df.field]))));
    }
    for (const cg of detail.children || []) {
      const holder = el("div", {});
      box.append(holder);
      childGrid(cg, row).then((w) => holder.replaceChildren(w));
    }
    const form = detailForm(row);
    if (form) box.append(form);
    return box;
  }

  function renderRow(row) {
    const cells = cols.map((c) => el("td", {}, row[c.field] == null ? "" : String(row[c.field])));
    let toggle = null;
    if (hasExtra) {
      const actionCell = el("td", { class: "pc-row-actions" });
      if (detail) {
        toggle = el("button", { class: "pc-btn pc-btn-sm pc-chevron" }, "▸");
        actionCell.append(toggle);
      }
      for (const ra of rowActions) {
        const b = rowActionButton(row, ra);
        if (b) actionCell.append(b);
      }
      cells.push(actionCell);
    }
    const tr = el("tr", {}, ...cells);
    tbody.append(tr);
    if (detail && toggle) {
      const dtr = el("tr", { hidden: "" }, el("td", { colspan: span }));
      let built = false;
      toggle.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const open = dtr.hidden;
        dtr.hidden = !open;
        toggle.textContent = open ? "▾" : "▸";
        if (open && !built) {
          built = true;
          dtr.firstChild.append(detailPanel(row));
        }
      });
      tbody.append(dtr);
    }
  }

  async function refresh() {
    try {
      const { rows } = await getJSON(dataUrl(p.data.source, p.data.table, activeFilter, p.data.orderBy));
      tbody.replaceChildren();
      for (const row of rows) renderRow(row);
      if (!rows.length) tbody.append(el("tr", {}, el("td", { colspan: span }, "No rows")));
    } catch (e) {
      tbody.replaceChildren(el("tr", {}, el("td", { colspan: span }, String(e.message || e))));
    }
  }
  document.addEventListener("pc:refresh", refresh);
  if (p.refreshMs && p.refreshMs > 0) setInterval(refresh, p.refreshMs);
  refresh();
  return card;
}

const RENDERERS = { text: renderText, actionForm: renderActionForm, dataGrid: renderDataGrid };

async function main() {
  try {
    const doc = await getJSON("/app/pages/" + encodeURIComponent(HOME));
    if (doc.title) document.title = doc.title;
    root.replaceChildren(...(doc.nodes || []).map((n) => (RENDERERS[n.type] || (() => el("div")))(n)));
  } catch (e) {
    root.replaceChildren(el("p", { class: "pc-msg err" }, "Failed to load page: " + String(e.message || e)));
  }
}
main();
`;
