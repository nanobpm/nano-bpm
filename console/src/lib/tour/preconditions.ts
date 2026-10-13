// The shared precondition library.
//
// Journeys import from here rather than each defining their own gates, so
// "would Run actually work?" has exactly one answer across the console. The
// alternative — three journey files each hand-rolling a runtime check — is how
// the tour ends up telling one user to install Node and promising another that
// Run works on the same machine.
//
// Pure: no runtime imports, so these are unit-testable without the Vite globals.

import type { Precondition } from "./types";

/**
 * A JavaScript runtime is present, so Run can actually start something.
 *
 * `repair` (not `skip`) when absent: the Run button is right there and the user
 * will press it, so the step must be replaced by an install hint rather than
 * quietly dropped. This is the precondition ADR 0049 was written around — the
 * spike's Run step promised "hit Run to boot the engine and execute it" on hosts
 * where the console already knew, from this very data, that it could not.
 */
export const hasJsRuntime: Precondition = {
  id: "has-js-runtime",
  test: (ctx) => (ctx.nodeAvailable ? "ok" : "repair"),
};

/** At least one project exists. `skip`: nothing to point at, and that is fine. */
export const hasProject: Precondition = {
  id: "has-project",
  test: (ctx) => (ctx.projects.length > 0 ? "ok" : "skip"),
};

/**
 * More than one node — i.e. the cluster views show something meaningful.
 *
 * `repair` so a single-node user gets "here is what changes at RF=3" instead of
 * a cluster view with one row in it. Absent `nodeCount` is treated as
 * single-node: assume the less impressive reality rather than claim a cluster.
 */
export const hasCluster: Precondition = {
  id: "has-cluster",
  test: (ctx) => ((ctx.nodeCount ?? 1) > 1 ? "ok" : "repair"),
};

/** Traces have been captured. `skip`: an empty trace table teaches nothing. */
export const hasTraces: Precondition = {
  id: "has-traces",
  test: (ctx) => ((ctx.traceCount ?? 0) > 0 ? "ok" : "skip"),
};

/**
 * An external worker is polling `jobType` (#404 supplies `consumers`).
 *
 * Not a `Precondition` but a `Predicate` factory: this is what a handoff step's
 * `verify` uses to notice that a hired agent harness connected. Returns false
 * when `consumers` is absent — i.e. before #404 lands, the handoff step stays
 * self-reported rather than auto-advancing on a signal we cannot see.
 */
export function isPolling(jobType: string) {
  return (ctx: { consumers?: { jobType: string }[] }): boolean =>
    ctx.consumers?.some((c) => c.jobType === jobType) ?? false;
}
