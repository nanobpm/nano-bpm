import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildAncestorChain,
  breadcrumbHops,
  activateHop,
  type AncestorSource,
  type BreadcrumbHop,
} from "./instanceBreadcrumb.ts";

/** A tiny in-memory instance store keyed by instance key. */
function store(rows: AncestorSource[]) {
  const byKey = new Map(rows.map((r) => [r.key, r]));
  const calls: string[] = [];
  const fetchInstance = async (key: string) => {
    calls.push(key);
    return byKey.get(key) ?? null;
  };
  return { fetchInstance, calls };
}

test("buildAncestorChain returns just the instance for a top-level (no parent)", async () => {
  const { fetchInstance } = store([
    { key: "root", process_id: "root-proc", parent_process_instance_key: null },
  ]);
  const chain = await buildAncestorChain("root", fetchInstance);
  assert.deepEqual(chain, [{ key: "root", processId: "root-proc" }]);
});

test("buildAncestorChain climbs parent_process_instance_key root-first for a nested chain", async () => {
  const { fetchInstance } = store([
    {
      key: "root",
      process_id: "root-proc",
      parent_process_instance_key: null,
    },
    {
      key: "parent",
      process_id: "parent-proc",
      parent_process_instance_key: "root",
    },
    {
      key: "child",
      process_id: "child-proc",
      parent_process_instance_key: "parent",
    },
  ]);
  // Start from the deepest child; expect grandparent(root) -> parent -> child.
  const chain = await buildAncestorChain("child", fetchInstance);
  assert.deepEqual(chain, [
    { key: "root", processId: "root-proc" },
    { key: "parent", processId: "parent-proc" },
    { key: "child", processId: "child-proc" },
  ]);
});

test("buildAncestorChain stops at a missing/evicted ancestor and returns what resolved", async () => {
  // `parent`'s row is absent (evicted) — the climb from `child` stops there.
  const { fetchInstance } = store([
    {
      key: "child",
      process_id: "child-proc",
      parent_process_instance_key: "parent",
    },
  ]);
  const chain = await buildAncestorChain("child", fetchInstance);
  assert.deepEqual(chain, [{ key: "child", processId: "child-proc" }]);
});

test("buildAncestorChain is resilient to a throwing fetch", async () => {
  const fetchInstance = async (key: string) => {
    if (key === "child") {
      return {
        key: "child",
        process_id: "child-proc",
        parent_process_instance_key: "parent",
      };
    }
    throw new Error("network");
  };
  const chain = await buildAncestorChain("child", fetchInstance);
  assert.deepEqual(chain, [{ key: "child", processId: "child-proc" }]);
});

test("buildAncestorChain guards against a linkage cycle", async () => {
  const { fetchInstance, calls } = store([
    { key: "a", process_id: "a", parent_process_instance_key: "b" },
    { key: "b", process_id: "b", parent_process_instance_key: "a" },
  ]);
  const chain = await buildAncestorChain("a", fetchInstance);
  // Both resolved exactly once; the cycle guard breaks the loop.
  assert.equal(chain.length, 2);
  assert.equal(calls.length, 2);
});

test("breadcrumbHops returns no segments for a top-level instance", () => {
  assert.deepEqual(
    breadcrumbHops([{ key: "root", processId: "root-proc" }]),
    [],
  );
  assert.deepEqual(breadcrumbHops([]), []);
});

test("breadcrumbHops renders every level in order, only the last is current", () => {
  const hops = breadcrumbHops([
    { key: "root", processId: "root-proc" },
    { key: "parent", processId: "parent-proc" },
    { key: "child", processId: "child-proc" },
  ]);
  assert.deepEqual(hops, [
    { key: "root", label: "root-proc", isCurrent: false },
    { key: "parent", label: "parent-proc", isCurrent: false },
    { key: "child", label: "child-proc", isCurrent: true },
  ]);
});

test("activateHop navigates to the hop's instance key for a non-current hop", () => {
  const hop: BreadcrumbHop = {
    key: "parent",
    label: "parent-proc",
    isCurrent: false,
  };
  const seen: string[] = [];
  activateHop(hop, (k) => seen.push(k));
  assert.deepEqual(seen, ["parent"]);
});

test("activateHop does not navigate for the current hop", () => {
  const hop: BreadcrumbHop = {
    key: "child",
    label: "child-proc",
    isCurrent: true,
  };
  const seen: string[] = [];
  activateHop(hop, (k) => seen.push(k));
  assert.deepEqual(seen, []);
});

test("activateHop is a no-op when no navigation handler is provided", () => {
  const hop: BreadcrumbHop = {
    key: "parent",
    label: "parent-proc",
    isCurrent: false,
  };
  assert.doesNotThrow(() => activateHop(hop, undefined));
});
