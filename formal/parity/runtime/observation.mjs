// Shared, backend-agnostic observation vocabulary for the two-backend parity
// runner (#1260, slice 5 of #1240).
//
// The spec vocabulary "may only use what BOTH engines expose" (#1240). Camunda's
// internal state is visible only through its exported records / REST read model,
// so a bare Zeebe gateway (no Elasticsearch/secondary storage) exposes only
// process completion and the final process variables via the v2 REST API
// (`createProcessInstance` with `awaitCompletion`). Nano's `engine-wasm`
// `TestEngine` additionally exposes the full element-instance history, taken
// sequence flows, jobs created and incidents through its event stream.
//
// Each backend therefore declares which fields it can authoritatively `provide`.
// The differential oracle (`diffObservations`) compares only the fields BOTH
// backends provide — the intersection is the honest Camunda-surface contract —
// while the per-scenario `expect` block (checked against nano, the always-on
// reference) pins the richer element/flow multiset that only nano exposes here.

/** The canonical field set an observation can carry. */
export const OBSERVATION_FIELDS = Object.freeze([
  "completed",
  "variables",
  "completedElements",
  "sequenceFlows",
  "jobsCreated",
  "incidents",
]);

/**
 * Normalise a Camunda v2 variable-search item list (`{ name, value }`, where
 * `value` is a JSON-encoded string — the shape both Zeebe's `/v2/variables/search`
 * and nano's read-model `searchVariables` return) into a plain `{ name: value }`
 * object with parsed JSON values. Deterministic and order-independent.
 */
export function variablesFromSearchItems(items) {
  const out = {};
  for (const item of items ?? []) {
    let value = item.value;
    if (typeof value === "string") {
      try {
        value = JSON.parse(value);
      } catch {
        // A bare string variable is stored JSON-encoded ("\"hi\""); a raw
        // non-JSON string should never occur, but keep it verbatim if it does.
      }
    }
    out[item.name] = value;
  }
  return out;
}

/** Increment a key in a multiset (plain object of counts). */
export function bump(multiset, key, by = 1) {
  multiset[key] = (multiset[key] ?? 0) + by;
  return multiset;
}

/** A fresh, empty observation with every field zeroed. */
export function emptyObservation() {
  return {
    completed: false,
    variables: {},
    completedElements: {},
    sequenceFlows: {},
    jobsCreated: {},
    incidents: {},
  };
}

// Deterministic, key-sorted JSON so two structurally equal values compare equal
// regardless of insertion order.
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    const out = {};
    for (const key of Object.keys(value).sort()) out[key] = canonical(value[key]);
    return out;
  }
  return value;
}

function canonicalJson(value) {
  return JSON.stringify(canonical(value));
}

function fieldsEqual(field, a, b) {
  return canonicalJson(a) === canonicalJson(b);
}

/**
 * Differential oracle. Compares two observations over the fields BOTH backends
 * provide (`provideA` ∩ `provideB`). Returns `{ ok, mismatches }`; a non-empty
 * `mismatches` fails the scenario. There is no tolerance and no retry — a
 * difference is, by the #1240 product rule, a Nano defect (or, if the difference
 * is nondeterministic, a driver defect to root-cause).
 */
export function diffObservations(a, b, provideA, provideB) {
  const shared = OBSERVATION_FIELDS.filter(
    (f) => provideA.has(f) && provideB.has(f),
  );
  const mismatches = [];
  for (const field of shared) {
    if (!fieldsEqual(field, a[field], b[field])) {
      mismatches.push({
        field,
        a: a[field],
        b: b[field],
      });
    }
  }
  return { ok: mismatches.length === 0, mismatches, comparedFields: shared };
}

/**
 * Check an observation against a scenario `expect` block. Only the fields named
 * in `expect` are checked, so a scenario can pin just `completed` + `variables`,
 * or additionally the element/flow multisets. Returns `{ ok, mismatches }`.
 */
export function checkExpectations(observation, expect) {
  if (!expect) return { ok: true, mismatches: [] };
  const mismatches = [];
  for (const field of Object.keys(expect)) {
    if (!OBSERVATION_FIELDS.includes(field)) {
      mismatches.push({ field, error: `unknown expect field '${field}'` });
      continue;
    }
    if (!fieldsEqual(field, observation[field], expect[field])) {
      mismatches.push({ field, expected: expect[field], actual: observation[field] });
    }
  }
  return { ok: mismatches.length === 0, mismatches };
}
