// Building the `TourContext` snapshot that preconditions and success predicates
// read.
//
// The base comes from ONE `listProjects()` call — which already returns the
// projects, `nodeAvailable`, the template menu and the installed extensions —
// plus the current route. Everything else is contributed by whichever slice
// needs it, through `registerContextSource`.
//
// That extension point is what keeps the journey slices independent: the
// topology slice can add `nodeCount`, the agentic journey `runState`, and #404's
// consumer panel `consumers`, without any of them editing this file or each
// other's. It is also why the extra `TourContext` fields are optional — a
// predicate runs whether or not its source is registered yet, so a journey
// degrades rather than breaks when a sibling slice has not landed.

import type { TourContext } from "./types";

export type ContextSource = (
  ctx: TourContext,
) => Promise<Partial<TourContext>> | Partial<TourContext>;

const sources = new Set<ContextSource>();

/**
 * Register an extra context source. Returns an unsubscribe function (call it
 * from a React effect's cleanup).
 *
 * Sources are invoked on every refresh and merged over the base in registration
 * order. A source that throws is ignored — one slice's failing fetch must not
 * take down another journey's preconditions.
 *
 * Prefer reading a cache your view already maintains over adding a new poll:
 * a handoff step's `verify` is polled while its step is showing, so a source
 * that fetches on every call would multiply requests for no benefit.
 */
export function registerContextSource(source: ContextSource): () => void {
  sources.add(source);
  return () => {
    sources.delete(source);
  };
}

/** Test seam. */
export function clearContextSources(): void {
  sources.clear();
}

/** The subset of `listProjects()`'s response the base context needs. */
export interface ProjectsSnapshot {
  projects?: TourContext["projects"];
  nodeAvailable?: boolean;
  templates?: TourContext["templates"];
  extensions?: { extensions?: TourContext["extensions"] };
}

export interface BaseContextInput {
  profile: TourContext["profile"];
  route: string;
  scratch?: Record<string, unknown>;
  snapshot?: ProjectsSnapshot | null;
}

/**
 * The base context, with conservative defaults.
 *
 * Runtime availability defaults to **false** when the snapshot is missing: an
 * unknown runtime must read as absent so a Run step repairs into an install hint
 * rather than promising something unverified. Guessing "probably fine" here is
 * exactly the defect ADR 0049 exists to fix.
 */
export function baseContext(input: BaseContextInput): TourContext {
  const s = input.snapshot ?? null;
  return {
    profile: input.profile,
    route: input.route,
    projects: s?.projects ?? [],
    nodeAvailable: s?.nodeAvailable ?? false,
    templates: s?.templates ?? [],
    extensions: s?.extensions?.extensions ?? [],
    scratch: input.scratch ?? {},
  };
}

/** Apply every registered source over a base context. */
export async function enrichContext(base: TourContext): Promise<TourContext> {
  let ctx = base;
  for (const source of sources) {
    try {
      const patch = await source(ctx);
      ctx = { ...ctx, ...patch };
    } catch {
      // A failing source contributes nothing; predicates handle undefined.
    }
  }
  return ctx;
}

export async function buildContext(
  input: BaseContextInput,
): Promise<TourContext> {
  return enrichContext(baseContext(input));
}
