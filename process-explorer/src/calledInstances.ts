// Pure helpers for the InstanceDetail parent->child "Called Process Instances"
// slice (see InstanceDetail.tsx).
//
// A parent instance's `called_instances` (the snake_case wire field from #1115)
// lists the child process instances it spawned through its call activities. Each
// entry names the call-activity cell (`calling_element_id`) that spawned it,
// resolved from the child's `parent_element_instance_key`. This module holds the
// side-effect-free decisions that back two behaviours, so the console's
// node:test unit suite can cover them directly (the component itself only runs
// in a browser):
//
//   1. Rendering the "Called Process Instances" table (`groupCalledInstances`).
//   2. Resolving what a click on a diagram call-activity cell should do,
//      matching Camunda Operate (`resolveCallActivitySelection`): a single child
//      navigates straight to it; a multi-instance call activity reveals its
//      children ("View all"); a call activity that has not spawned yet shows a
//      "no called instance" affordance; a non-call-activity element is ignored.
//
// `calling_element_id` matching is the single source of truth for "which child
// belongs to which cell", so both the table grouping and the click resolution
// route through it.

import type { CalledInstance } from "./gen";

/**
 * Extract the BPMN ids of every `callActivity` cell in a process model.
 *
 * The viewer's `onElementSelect` (from #1114) surfaces only an element id, not
 * its type, so to tell a call-activity cell that simply has not spawned a child
 * yet (→ a "no called instance" affordance) apart from an ordinary
 * non-call-activity element (→ ignored, no navigation), we need the set of
 * call-activity ids from the model. This is a deliberately dependency-free scan
 * (no DOMParser) so it runs identically in the browser and under node:test.
 * Matches both namespaced (`<bpmn:callActivity …>`) and bare (`<callActivity …>`)
 * tags and tolerates attribute order.
 */
export function callActivityElementIds(
  xml: string | null | undefined,
): Set<string> {
  const ids = new Set<string>();
  if (!xml) return ids;
  // <…callActivity … id="X" …> — the id attribute may appear anywhere in the
  // tag, so match the tag first, then pull its id out of the captured attrs.
  const tag = /<(?:[\w.-]+:)?callActivity\b([^>]*)>/g;
  const idAttr = /\bid\s*=\s*"([^"]+)"/;
  let m: RegExpExecArray | null;
  while ((m = tag.exec(xml)) !== null) {
    const attrs = m[1];
    const idMatch = idAttr.exec(attrs);
    if (idMatch) ids.add(idMatch[1]);
  }
  return ids;
}

/** The called instances that a given call-activity cell (`elementId`) spawned. */
export function calledInstancesForElement(
  elementId: string,
  called: readonly CalledInstance[],
): CalledInstance[] {
  return called.filter((c) => c.calling_element_id === elementId);
}

/**
 * A call activity's cell and the child instances it spawned, grouped for the
 * "Called Process Instances" table. Entries whose `calling_element_id` could not
 * be resolved (`null` — e.g. the calling element instance was evicted) are
 * collected under an `elementId` of `null` so they still render (never dropped).
 */
export interface CalledInstanceGroup {
  elementId: string | null;
  elementName: string | null;
  instances: CalledInstance[];
}

/**
 * Group `called_instances` by their calling call-activity cell, preserving
 * first-seen order (both of the groups and of the rows within each group). A
 * multi-instance call activity yields one group with N instances; distinct call
 * activities yield distinct groups. Unresolved-cell entries (`calling_element_id`
 * === null) are collected under a single `null` group, positioned by first-seen
 * order like any other group (not forced to the end).
 */
export function groupCalledInstances(
  called: readonly CalledInstance[],
): CalledInstanceGroup[] {
  const groups: CalledInstanceGroup[] = [];
  const byId = new Map<string | null, CalledInstanceGroup>();
  for (const c of called) {
    const id = c.calling_element_id;
    let g = byId.get(id);
    if (!g) {
      g = { elementId: id, elementName: c.calling_element_name, instances: [] };
      byId.set(id, g);
      groups.push(g);
    }
    // Prefer a non-null name if a later row in the same group carries one.
    if (g.elementName == null && c.calling_element_name != null) {
      g.elementName = c.calling_element_name;
    }
    g.instances.push(c);
  }
  return groups;
}

/** The outcome of selecting an element on the diagram. */
export type CallActivitySelection =
  /** A non-call-activity element (or the same cell re-clicked with no data):
   * do nothing, do not navigate. */
  | { kind: "ignore" }
  /** Exactly one child — navigate straight to it (Operate double-click parity). */
  | { kind: "navigate"; instanceKey: string }
  /** A multi-instance call activity — reveal/filter its rows ("View all"). */
  | { kind: "reveal"; elementId: string }
  /** A call activity that has not spawned a child yet — "no called instance". */
  | { kind: "none"; elementId: string };

/**
 * Decide what selecting the diagram element `elementId` should do, given the
 * parent's `called_instances` and the set of call-activity ids in the model.
 *
 * - 1 matching child  → `navigate` to it.
 * - >1 matching child → `reveal` the section filtered to that cell.
 * - 0 matches, but the element IS a call activity → `none` (awaiting spawn).
 * - 0 matches and NOT a call activity → `ignore` (never navigate).
 *
 * `callActivityIds` is optional: when it is omitted/empty, a zero-match
 * selection can't be confirmed as a call activity and is treated as `ignore`
 * (fail safe — never navigate on an unknown element).
 */
export function resolveCallActivitySelection(
  elementId: string,
  called: readonly CalledInstance[],
  callActivityIds?: ReadonlySet<string>,
): CallActivitySelection {
  const matches = calledInstancesForElement(elementId, called);
  if (matches.length === 1) {
    return { kind: "navigate", instanceKey: matches[0].key };
  }
  if (matches.length > 1) {
    return { kind: "reveal", elementId };
  }
  if (callActivityIds?.has(elementId)) {
    return { kind: "none", elementId };
  }
  return { kind: "ignore" };
}
