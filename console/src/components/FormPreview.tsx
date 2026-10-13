import { useEffect, useRef, useState } from "react";
import { Form } from "@bpmn-io/form-js-viewer";
import "@bpmn-io/form-js/dist/assets/form-js.css";
import {
  applyDataSourceOptions,
  collectFormDataBindings,
  preResolveFormSchema,
  rowsToOptions,
  type DataQueryResolver,
  type FormOption,
} from "@nanobpm/nano-app-schema";

import { queryData } from "../gen";

// Live form preview with data-aware controls (ADR 0024 §5). Renders a form-js
// *viewer* of the current schema, but first resolves every field's `dataSource`
// binding through the Node data gateway (ADR 0024 phase 2): each bound choice
// field's option list comes live from `SELECT …` against a named datasource,
// exactly the Borland data-aware control — bound to an alias, so it survives the
// SQLite→Postgres flip. Unbound forms just render.

interface BindingError {
  field: string;
  message: string;
}

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

/**
 * Runs each datasource binding's query and returns the resolved option lists
 * keyed by field id (falling back to key), plus any per-field errors. Queries
 * for the same (source, sql) are de-duplicated so a form reusing one list hits
 * the gateway once.
 */
async function resolveBindings(
  schema: unknown,
  name: string,
): Promise<{ resolved: Map<string, FormOption[]>; errors: BindingError[] }> {
  const bindings = collectFormDataBindings(schema);
  const resolved = new Map<string, FormOption[]>();
  const errors: BindingError[] = [];
  const cache = new Map<string, Promise<Record<string, unknown>[]>>();

  await Promise.all(
    bindings.map(async (b) => {
      const cacheKey = `${b.binding.source}\u0000${b.binding.query}`;
      let rowsPromise = cache.get(cacheKey);
      if (!rowsPromise) {
        rowsPromise = queryData({
          path: { name, source: b.binding.source },
          body: { sql: b.binding.query },
          throwOnError: true,
        }).then((r) => r.data.rows ?? []);
        cache.set(cacheKey, rowsPromise);
      }
      const fieldRef = b.fieldId ?? b.fieldKey ?? b.path;
      try {
        const rows = await rowsPromise;
        resolved.set(fieldRef, rowsToOptions(rows, b.binding));
      } catch (e) {
        errors.push({
          field: b.fieldKey ?? b.fieldId ?? b.path,
          message: errMsg(e),
        });
      }
    }),
  );

  return { resolved, errors };
}

export default function FormPreview({
  schema,
  name,
  defaultSource,
}: {
  schema: string;
  name: string;
  /** The App manifest's `data.default` alias — the source for a `data.query(sql)` call. */
  defaultSource?: string;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const formRef = useRef<Form | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [bindingErrors, setBindingErrors] = useState<BindingError[]>([]);
  const [resolving, setResolving] = useState(true);

  useEffect(() => {
    let disposed = false;
    setError(null);
    setBindingErrors([]);
    setResolving(true);

    let parsed: unknown;
    try {
      parsed = JSON.parse(schema);
    } catch (e) {
      setError(`Form schema is not valid JSON: ${errMsg(e)}`);
      setResolving(false);
      return;
    }

    (async () => {
      const { resolved, errors } = await resolveBindings(parsed, name);
      if (disposed) return;
      const withOptions = applyDataSourceOptions(parsed, resolved);

      // Pre-resolve `data.query(...)` FEEL calls (ADR 0024 §5): FEEL evaluates
      // synchronously, so each static call is run read-only through the gateway
      // now and its rows seeded as form data under a `__dq*` binding, which the
      // rewritten expression references. A `data.query(sql)` (default form)
      // resolves against the manifest's `data.default`.
      const runDataQuery: DataQueryResolver = async (source, sql) => {
        const src = source ?? defaultSource;
        if (!src) {
          throw new Error(
            "data.query() names no source and the app declares no data.default",
          );
        }
        const r = await queryData({
          path: { name, source: src },
          body: { sql },
          throwOnError: true,
        });
        return r.data.rows ?? [];
      };
      const pre = await preResolveFormSchema(withOptions, runDataQuery);
      if (disposed) return;
      setBindingErrors([
        ...errors,
        ...pre.errors.map((e) => ({ field: e.path, message: e.message })),
      ]);
      const finalSchema = pre.schema;

      const container = containerRef.current;
      if (!container) return;
      // Rebuild the viewer on every schema/data change — form-js has no clean
      // partial re-import, and a preview is cheap.
      formRef.current?.destroy();
      const form = new Form({ container });
      formRef.current = form;
      try {
        await form.importSchema(finalSchema as never, pre.data as never);
      } catch (e) {
        if (!disposed) setError(`Could not render form: ${errMsg(e)}`);
      } finally {
        if (!disposed) setResolving(false);
      }
    })();

    return () => {
      disposed = true;
      formRef.current?.destroy();
      formRef.current = null;
    };
  }, [schema, name, defaultSource]);

  return (
    <div className="flex h-full flex-col bg-white">
      {(error || bindingErrors.length > 0 || resolving) && (
        <div className="shrink-0 border-b border-edge bg-panel px-4 py-1.5 text-xs">
          {resolving && (
            <span className="text-fg-faint">
              Resolving datasource bindings…
            </span>
          )}
          {error && <span className="text-danger">{error}</span>}
          {bindingErrors.map((be) => (
            <div key={be.field} className="text-warn">
              Field <code className="text-fg-muted">{be.field}</code>:{" "}
              {be.message}
            </div>
          ))}
        </div>
      )}
      <div ref={containerRef} className="min-h-0 flex-1 overflow-auto p-4" />
    </div>
  );
}
