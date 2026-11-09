import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyFilterChange,
  explorerStackView,
  filtersQueryKey,
  INSTANCE_DEEP_LINK_PARAM,
  INSTANCE_STATE_FILTERS,
  parseExplorerFilters,
  readInstanceParam,
  shouldResetPageOnFilterChange,
  toInstanceQuery,
} from "./explorerFilters.ts";

test("parseExplorerFilters returns the unconstrained defaults for empty params", () => {
  const filters = parseExplorerFilters(new URLSearchParams());
  assert.deepEqual(filters, { state: undefined, hasIncident: false });
});

test("parseExplorerFilters reads a valid state and the incident flag", () => {
  const filters = parseExplorerFilters(
    new URLSearchParams("state=Active&incident=1"),
  );
  assert.deepEqual(filters, { state: "Active", hasIncident: true });
});

test("parseExplorerFilters ignores an unknown state value (falls back to All)", () => {
  const filters = parseExplorerFilters(new URLSearchParams("state=Bogus"));
  assert.equal(filters.state, undefined);
});

test("INSTANCE_STATE_FILTERS offers Suspended between Active and the terminal states", () => {
  // Drives both the desktop segmented group and the mobile chip row, so this is
  // the single source that makes "Suspended" a selectable filter option.
  assert.deepEqual(INSTANCE_STATE_FILTERS, [
    "Active",
    "Suspended",
    "Completed",
    "Terminated",
  ]);
});

test("parseExplorerFilters reads the Suspended state and round-trips it to the query", () => {
  const filters = parseExplorerFilters(new URLSearchParams("state=Suspended"));
  assert.deepEqual(filters, { state: "Suspended", hasIncident: false });
  assert.deepEqual(toInstanceQuery(filters), { state: "Suspended" });
  assert.deepEqual(filtersQueryKey(filters), ["Suspended", false]);
});

test("parseExplorerFilters treats any incident value other than '1' as unchecked", () => {
  assert.equal(
    parseExplorerFilters(new URLSearchParams("incident=true")).hasIncident,
    false,
  );
  assert.equal(
    parseExplorerFilters(new URLSearchParams("incident=0")).hasIncident,
    false,
  );
});

test("toInstanceQuery omits unconstrained dimensions (no param = current behaviour)", () => {
  assert.deepEqual(
    toInstanceQuery({ state: undefined, hasIncident: false }),
    {},
  );
});

test("toInstanceQuery includes only the constrained dimensions", () => {
  assert.deepEqual(
    toInstanceQuery({ state: "Completed", hasIncident: false }),
    {
      state: "Completed",
    },
  );
  assert.deepEqual(toInstanceQuery({ state: undefined, hasIncident: true }), {
    hasIncident: true,
  });
  assert.deepEqual(toInstanceQuery({ state: "Active", hasIncident: true }), {
    state: "Active",
    hasIncident: true,
  });
});

test("filtersQueryKey is stable and distinguishes combinations", () => {
  assert.deepEqual(filtersQueryKey({ state: undefined, hasIncident: false }), [
    null,
    false,
  ]);
  assert.deepEqual(filtersQueryKey({ state: "Active", hasIncident: true }), [
    "Active",
    true,
  ]);
});

test("applyFilterChange sets and clears the state param", () => {
  const set = applyFilterChange(new URLSearchParams(), {
    kind: "state",
    state: "Terminated",
  });
  assert.equal(set.get("state"), "Terminated");

  const cleared = applyFilterChange(set, { kind: "state", state: undefined });
  assert.equal(cleared.get("state"), null);
});

test("applyFilterChange sets incident=1 and removes it when unchecked", () => {
  const set = applyFilterChange(new URLSearchParams(), {
    kind: "hasIncident",
    hasIncident: true,
  });
  assert.equal(set.get("incident"), "1");

  const cleared = applyFilterChange(set, {
    kind: "hasIncident",
    hasIncident: false,
  });
  assert.equal(cleared.get("incident"), null);
});

test("applyFilterChange preserves unrelated params (e.g. instance deep-link)", () => {
  const next = applyFilterChange(new URLSearchParams("instance=42"), {
    kind: "state",
    state: "Active",
  });
  assert.equal(next.get("instance"), "42");
  assert.equal(next.get("state"), "Active");
});

test("a filter change always resets the pager to the first page", () => {
  assert.equal(shouldResetPageOnFilterChange(), true);
});

test("INSTANCE_DEEP_LINK_PARAM is the stable `instance` wire name", () => {
  assert.equal(INSTANCE_DEEP_LINK_PARAM, "instance");
});

test("readInstanceParam returns null when the param is absent", () => {
  assert.equal(readInstanceParam(new URLSearchParams()), null);
});

test("readInstanceParam returns the trimmed key when present", () => {
  assert.equal(
    readInstanceParam(new URLSearchParams("instance=abc-123")),
    "abc-123",
  );
  assert.equal(
    readInstanceParam(new URLSearchParams("instance=%20abc%20")),
    "abc",
  );
});

test("readInstanceParam treats a blank/whitespace-only value as absent", () => {
  assert.equal(readInstanceParam(new URLSearchParams("instance=")), null);
  assert.equal(readInstanceParam(new URLSearchParams("instance=%20%20")), null);
});

test("explorerStackView shows the list when nothing is selected", () => {
  assert.equal(explorerStackView(null), "list");
  assert.equal(explorerStackView(""), "list");
});

test("explorerStackView shows the detail once an instance is selected — this is the ?instance= mobile deep-link contract", () => {
  assert.equal(explorerStackView("inst-1"), "detail");
});
