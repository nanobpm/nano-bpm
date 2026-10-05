/**
 * The single-flight guard for the console's marketplace fetch (the Extensions
 * view's poll / "Check now" button). The server shells out to npm per
 * installed pack on a cache miss, so a new fetch must never stack on top of
 * an unfinished one — that overlapping fan-out is the feedback loop that
 * swap-thrashed a small host (#1330).
 *
 * A plain boolean guard ("drop the click while a fetch is in flight") is not
 * enough. Two kinds of request carry state the user must see and so must not
 * be silently dropped when a fetch is already running:
 *   - an explicit `force` ("Check now"), which would otherwise serve the user
 *     the cached listing they asked to bypass; and
 *   - a reload after a local install / update / removal, which would otherwise
 *     leave the Install/Update/Remove affordances stale until the next poll
 *     (potentially minutes later) — a background fetch already in flight can
 *     have captured pre-mutation state, so dropping the post-mutation reload
 *     strands the UI on it.
 * Both are **queued** and run once the current fetch settles (at most one
 * queued — coalesced, keeping the strongest `force`; a second request while one
 * is already queued adds nothing). Only a plain background poll tick, which
 * carries nothing new, is redundant and dropped mid-flight.
 */

/** Opaque handle to the guard's mutable state (opaque to the view). */
export interface MarketFetchGate {
  inFlight: boolean;
  /**
   * The strongest request that arrived while a fetch was in flight and must
   * still run once it settles, or `null` if none is pending. A forced queued
   * request (bypass the TTL) wins over a non-forced one, so an escalation is
   * never lost by coalescing.
   */
  queued: null | { force: boolean };
  /**
   * Resolvers for must-run callers (a forced "Check now" or a post-mutation
   * reload) that were queued — or coalesced into the queue — behind the
   * in-flight fetch. They must **not** resolve until the queued reload they
   * depend on has actually run: otherwise an install/update handler that
   * `await`s its post-mutation reload clears `busy` — re-enabling the
   * Install/Update button against stale pre-mutation state — before the reload
   * settles, permitting a duplicate installation (#1330). Accumulates for the
   * currently pending `queued` slot.
   */
  queuedWaiters: Array<() => void>;
  /**
   * Resolvers captured for the run that is currently draining the queue — the
   * waiters that accumulated while that request was queued. Resolved when that
   * draining run settles (its `marketFetchEnd`), i.e. once the reload the
   * callers depend on has actually completed.
   */
  drainingWaiters: Array<() => void>;
}

export function createMarketFetchGate(): MarketFetchGate {
  return {
    inFlight: false,
    queued: null,
    queuedWaiters: [],
    drainingWaiters: [],
  };
}

/**
 * A fetch request. Two orthogonal flags:
 * - `force` — bypass the server's cache TTL (an explicit "Check now").
 * - `mustRun` — this request carries state the user must see (a forced check,
 *   or a reload after a local install / update / removal), so it must **not**
 *   be silently dropped when a fetch is already in flight — it is queued to run
 *   when that fetch settles. A plain background poll tick carries nothing new,
 *   leaves `mustRun` false, and is dropped while a fetch is in flight. A forced
 *   request is always must-run.
 */
export interface MarketFetchRequest {
  force?: boolean;
  mustRun?: boolean;
}

/**
 * Decide what a `load(req)` request should do. Mutates `gate` and returns:
 * - `"run"` — start the fetch now (the gate is marked in-flight);
 * - `"drop"` — a fetch is already running and this request adds nothing new;
 * - `"queued"` — a fetch is running; this must-run request runs when it settles.
 */
export function marketFetchBegin(
  gate: MarketFetchGate,
  req: MarketFetchRequest,
): "run" | "drop" | "queued" {
  const force = req.force ?? false;
  // A forced request is inherently must-run: dropping it would serve the user
  // the cached listing they explicitly asked to bypass.
  const mustRun = (req.mustRun ?? false) || force;
  if (gate.inFlight) {
    if (!mustRun) return "drop";
    // Coalesce into at most one queued request, keeping the strongest `force`.
    const nextForce = (gate.queued?.force ?? false) || force;
    const changed = gate.queued === null || nextForce !== gate.queued.force;
    gate.queued = { force: nextForce };
    return changed ? "queued" : "drop";
  }
  gate.inFlight = true;
  return "run";
}

/**
 * Register a resolver for a must-run request (a forced "Check now" or a
 * post-mutation reload) that could not run immediately — it was queued, or
 * coalesced into the queue, behind the in-flight fetch. The resolver fires once
 * the queued reload it is waiting on has run to completion, so the caller can
 * `await` the reload it depends on rather than resolving against pre-mutation
 * state (#1330). Only a redundant background poll (dropped, never must-run)
 * resolves immediately and registers no waiter.
 */
export function marketFetchQueueWaiter(
  gate: MarketFetchGate,
  resolve: () => void,
): void {
  gate.queuedWaiters.push(resolve);
}

/**
 * Settle the in-flight fetch and report the queued request (if any) that must
 * run next, consuming the queue marker. Call exactly once per `"run"`.
 *
 * Also resolves the waiters captured for the run that just settled (the
 * draining run). If another request is queued, the waiters that accumulated
 * while it was queued become the next draining set — resolved when *that* run
 * settles — so a must-run caller never resolves before the reload it depends on
 * has actually completed.
 */
export function marketFetchEnd(gate: MarketFetchGate): {
  runQueued: null | { force: boolean };
} {
  gate.inFlight = false;
  const runQueued = gate.queued;
  gate.queued = null;
  const settled = gate.drainingWaiters;
  gate.drainingWaiters = runQueued ? gate.queuedWaiters : [];
  gate.queuedWaiters = [];
  for (const resolve of settled) resolve();
  return { runQueued };
}
