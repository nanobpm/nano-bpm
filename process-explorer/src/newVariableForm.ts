// Pure logic for the Process Explorer's "New variable" affordance, kept out of
// the React component so it can be unit-tested with the Node test runner (no
// DOM).
//
// Creating a variable is UI-only: it is a `setInstanceVariables` call
// (`PUT /instances/{key}/variables`) with a *new* name. The only new decisions
// are (1) which scopes an operator may target, (2) parsing the value the same
// way inline edit does, and (3) refusing to silently overwrite a name that
// already exists on the chosen scope (that is what inline edit is for).

import type { Variable } from "./gen";

/**
 * The single source of truth for the "value must be valid JSON" message. Both
 * the inline `VariableRow` editor and the new-variable form parse the value with
 * `JSON.parse`, so they must surface the *same* error — this constant keeps them
 * from drifting apart.
 */
export const VALUE_JSON_ERROR =
  'Value must be valid JSON (e.g. 42, true, "text").';

/**
 * Parse a raw editor string as a typed JSON value, mirroring `VariableRow.save`.
 * Returns the parsed value on success, or the shared JSON error on failure so an
 * invalid value is rejected *before* any request is made.
 */
export function parseVariableJson(
  draft: string,
): { ok: true; value: unknown } | { ok: false; error: string } {
  try {
    return { ok: true, value: JSON.parse(draft) };
  } catch {
    return { ok: false, error: VALUE_JSON_ERROR };
  }
}

/**
 * The distinct scope keys an operator may create a variable on, most-general
 * first: the process-instance scope (the passed `instanceKey`, always offered
 * even when it currently holds no variables) followed by every other scope key
 * that already appears among the instance's variables (element-instance /
 * subprocess scopes), de-duplicated and sorted with numeric collation (scope
 * keys are numeric strings, so a lexicographic sort would place "10" before
 * "2").
 *
 * Active elements are intentionally *not* a source here: `ActiveElement` carries
 * no scope key (only `element_id` / `element_type`), so the addressable nested
 * scopes are exactly those already witnessed on a variable.
 */
export function scopeKeyOptions(
  instanceKey: string,
  variables: ReadonlyArray<Pick<Variable, "scope_key">>,
): string[] {
  const others = new Set<string>();
  for (const v of variables) {
    if (v.scope_key !== instanceKey) others.add(v.scope_key);
  }
  return [
    instanceKey,
    ...Array.from(others).sort((a, b) =>
      a.localeCompare(b, undefined, { numeric: true }),
    ),
  ];
}

/**
 * Whether a variable `name` already exists on `scopeKey`. Used to route a
 * would-be create that collides with an existing row to inline edit instead of a
 * silent overwrite.
 */
export function variableExists(
  variables: ReadonlyArray<Pick<Variable, "name" | "scope_key">>,
  scopeKey: string,
  name: string,
): boolean {
  return variables.some((v) => v.scope_key === scopeKey && v.name === name);
}

export type NewVariableInput = {
  name: string;
  valueDraft: string;
  scopeKey: string;
  variables: ReadonlyArray<Pick<Variable, "name" | "scope_key">>;
};

export type NewVariableResult =
  | { ok: true; name: string; value: unknown; scopeKey: string }
  | { ok: false; error: string };

/**
 * Validate a new-variable submission end-to-end, in the order an operator hits
 * the problems: a blank name, a duplicate name on the chosen scope, then an
 * invalid JSON value. Returns the trimmed name, parsed value and scope on
 * success so the caller can issue a single `setInstanceVariables` call.
 */
export function validateNewVariable(
  input: NewVariableInput,
): NewVariableResult {
  const name = input.name.trim();
  if (name === "") {
    return { ok: false, error: "Name is required." };
  }
  if (variableExists(input.variables, input.scopeKey, name)) {
    return {
      ok: false,
      error: `"${name}" already exists on this scope — edit it inline instead.`,
    };
  }
  const parsed = parseVariableJson(input.valueDraft);
  if (!parsed.ok) return parsed;
  return { ok: true, name, value: parsed.value, scopeKey: input.scopeKey };
}
