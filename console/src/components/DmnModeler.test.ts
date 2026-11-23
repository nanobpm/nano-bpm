import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Issue #1048 regression guard: switching between the DRD and a decision table
// fires dmn-js's `views.changed`, which is NAVIGATION, not an edit — the
// document is untouched. The handler must not invalidate the document-load
// gate: if it did, a duplicate XML delivered by ProjectWorkspace's readiness
// effect after a view switch would re-import and reset the active view and
// selection. It must still surface the change notification (suppression-
// guarded, like the commandStack.changed handler).
//
// dmn-js needs a DOM the node --test runner does not provide, so this guards
// the wiring structurally: it reads DmnModeler.tsx and pins the shape of the
// views.changed handler. The gate semantics themselves are covered by
// documentLoadGate.test.ts.

const source = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "DmnModeler.tsx"),
  "utf8",
);

// The views.changed handler body, from its declaration to the modeler
// subscription that closes the effect scope it lives in.
const handler = source.slice(
  source.indexOf("const handleViewsChanged = () => {"),
  source.indexOf('modeler.on("views.changed", handleViewsChanged);'),
);

test("views.changed never invalidates the document-load gate", () => {
  assert.ok(handler.length > 0, "handleViewsChanged must exist");
  assert.equal(
    /loadGateRef\.current\.(invalidate|invalidateIf|shouldLoad|settle)\(/.test(
      handler,
    ),
    false,
    "navigation is not an edit: the gate must stay armed for duplicate delivery",
  );
});

test("views.changed does not delegate to the edit handler", () => {
  assert.equal(
    handler.includes("handleChanged()"),
    false,
    "handleChanged invalidates the gate; navigation must not call it",
  );
});

test("views.changed still surfaces the change notification, suppression-guarded", () => {
  assert.ok(
    handler.includes("if (suppressChange.current) return;"),
    "programmatic loads must not notify",
  );
  assert.ok(
    handler.includes("onChangeRef.current?.();"),
    "a view switch still notifies the parent",
  );
  // The guard must precede the notification.
  assert.ok(
    handler.indexOf("if (suppressChange.current) return;") <
      handler.indexOf("onChangeRef.current?.();"),
  );
});

test("the gate is invalidated only by real divergence (edit / blank / failed import)", () => {
  // Every gate mutation in the component must be one of: the edit handler, a
  // blank load, or the stale-failure re-arm (which lives in the shared
  // gatedImport helper). A new call site that invalidates on navigation
  // reopens the selection-wipe.
  const invalidations = [
    ...source.matchAll(/loadGateRef\.current\.(invalidate|invalidateIf)\(/g),
  ].map((m) => m[0]);
  assert.deepEqual(
    invalidations.sort(),
    ["loadGateRef.current.invalidate(", "loadGateRef.current.invalidate("],
    "expected exactly: edit handler + createBlank invalidate",
  );
  assert.match(source, /return gatedImport\(/);
});
