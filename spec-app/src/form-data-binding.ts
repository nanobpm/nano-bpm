// Form-field datasource binding — ADR 0024 §5 ("data-aware controls").
//
// A form-js *choice* field (select / checklist / radio / taglist) may carry a
// `dataSource` binding so its option list comes **live** from a named datasource
// (ADR 0024 §1) instead of a hand-typed static list — the Borland data-aware
// control, reborn. The field binds to a datasource *alias*, never a driver, so
// the same form runs against SQLite in the IDE and Postgres in production by
// flipping env only (the whole point of the BDE-alias seam).
//
// This module is the pure, runtime-agnostic core: it *collects* the bindings
// out of a form schema, maps query rows to form-js option `values`, and applies
// resolved options back onto a schema clone. The actual query execution (which
// needs the datasource gateway, ADR 0024 phase 2) lives in the console; keeping
// the shape + mapping here makes it spec-first and unit-testable, and lets the
// validator and the console preview agree on one contract.

/** The datasource binding attached to a choice field's `dataSource` property. */
export interface FormFieldDataBinding {
  /** Datasource alias (a key of manifest `data.sources`). */
  source: string;
  /** A read query (SELECT …) whose rows become the field's options. */
  query: string;
  /** Row column mapped to each option's `value` (default `"value"`). */
  value?: string;
  /** Row column mapped to each option's `label` (default `"label"`). */
  label?: string;
}

/** A binding located within a form schema, with the field it belongs to. */
export interface CollectedFormBinding {
  /** The bound field's `key` (its data path), when it has one. */
  fieldKey?: string;
  /** The field component's `id` (form-js always assigns one). */
  fieldId?: string;
  /** JSON-pointer-ish path to the field within the schema, for diagnostics. */
  path: string;
  binding: FormFieldDataBinding;
}

/** A single resolved option, matching form-js's static `values` entry shape. */
export interface FormOption {
  value: string;
  label: string;
}

/** The form-js component `type`s that render an option list. */
export const CHOICE_FIELD_TYPES: ReadonlySet<string> = new Set([
  "select",
  "checklist",
  "radio",
  "taglist",
]);

/**
 * Reads a well-formed `dataSource` binding off a component, or `undefined`.
 * Deliberately strict: `source` and `query` must be non-empty strings, so a
 * half-typed or malformed binding is simply ignored (and separately flagged by
 * the validator) rather than throwing at render time.
 */
export function readFieldDataBinding(component: unknown): FormFieldDataBinding | undefined {
  if (!component || typeof component !== "object") return undefined;
  const ds = (component as { dataSource?: unknown }).dataSource;
  if (!ds || typeof ds !== "object") return undefined;
  const o = ds as Record<string, unknown>;
  if (typeof o.source !== "string" || o.source.length === 0) return undefined;
  if (typeof o.query !== "string" || o.query.length === 0) return undefined;
  const binding: FormFieldDataBinding = { source: o.source, query: o.query };
  if (typeof o.value === "string" && o.value.length > 0) binding.value = o.value;
  if (typeof o.label === "string" && o.label.length > 0) binding.label = o.label;
  return binding;
}

/**
 * Walks a form-js schema (recursing into layout components: groups, dynamic
 * lists) and returns every field carrying a datasource binding. Order is
 * document order so diagnostics are stable.
 */
export function collectFormDataBindings(schema: unknown): CollectedFormBinding[] {
  const out: CollectedFormBinding[] = [];
  const walk = (components: unknown, prefix: string): void => {
    if (!Array.isArray(components)) return;
    components.forEach((c, i) => {
      if (!c || typeof c !== "object") return;
      const comp = c as Record<string, unknown>;
      const path = `${prefix}/${i}`;
      const binding = readFieldDataBinding(comp);
      if (binding) {
        out.push({
          fieldKey: typeof comp.key === "string" ? comp.key : undefined,
          fieldId: typeof comp.id === "string" ? comp.id : undefined,
          path,
          binding,
        });
      }
      if (Array.isArray(comp.components)) walk(comp.components, `${path}/components`);
    });
  };
  const root = (schema as { components?: unknown })?.components;
  walk(root, "/components");
  return out;
}

/** Coerce a cell to the string form form-js option value/label expect. */
function optionString(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

/**
 * Maps datasource query rows to form-js static options. Uses the binding's
 * `value`/`label` columns, defaulting to `"value"`/`"label"`; when only one
 * mapping resolves, it doubles as the other so a `SELECT name` still renders.
 */
export function rowsToOptions(
  rows: ReadonlyArray<Record<string, unknown>>,
  binding: FormFieldDataBinding,
): FormOption[] {
  const valueKey = binding.value ?? "value";
  const labelKey = binding.label ?? "label";
  return rows.map((row) => {
    const hasValue = Object.prototype.hasOwnProperty.call(row, valueKey);
    const hasLabel = Object.prototype.hasOwnProperty.call(row, labelKey);
    const rawValue = hasValue ? row[valueKey] : hasLabel ? row[labelKey] : undefined;
    const rawLabel = hasLabel ? row[labelKey] : rawValue;
    return { value: optionString(rawValue), label: optionString(rawLabel) };
  });
}

/**
 * Returns a deep clone of `schema` with each bound field's options replaced by
 * the resolved list (keyed by the field's `id`, falling back to `key`). The
 * field is switched to form-js's static source (`values` set, `valuesKey`
 * cleared) so a plain viewer renders the live options with no extra wiring. The
 * input schema is never mutated.
 */
export function applyDataSourceOptions(
  schema: unknown,
  resolved: ReadonlyMap<string, ReadonlyArray<FormOption>>,
): unknown {
  const clone = structuredCloneShallowSafe(schema);
  const walk = (components: unknown): void => {
    if (!Array.isArray(components)) return;
    for (const c of components) {
      if (!c || typeof c !== "object") continue;
      const comp = c as Record<string, unknown>;
      if (readFieldDataBinding(comp)) {
        const id = typeof comp.id === "string" ? comp.id : undefined;
        const key = typeof comp.key === "string" ? comp.key : undefined;
        const options =
          (id && resolved.get(id)) || (key && resolved.get(key)) || undefined;
        if (options) {
          comp.values = options.map((o) => ({ value: o.value, label: o.label }));
          delete comp.valuesKey;
          delete comp.valuesExpression;
        }
      }
      if (Array.isArray(comp.components)) walk(comp.components);
    }
  };
  walk((clone as { components?: unknown })?.components);
  return clone;
}

/** `structuredClone` when available (Node ≥17, browsers), else a JSON clone. */
function structuredCloneShallowSafe<T>(value: T): T {
  const sc = (globalThis as { structuredClone?: (v: T) => T }).structuredClone;
  if (typeof sc === "function") return sc(value);
  return JSON.parse(JSON.stringify(value)) as T;
}
