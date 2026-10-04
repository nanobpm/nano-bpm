/**
 * The single-flight guard for the console's marketplace fetch (the Extensions
 * view's poll / "Check now" button). The server shells out to npm per
 * installed pack on a cache miss, so a new fetch must never stack on top of
 * an unfinished one — that overlapping fan-out is the feedback loop that
 * swap-thrashed a small host (#1330).
 *
 * A plain boolean guard ("drop the click while a fetch is in flight") is not
 * enough: an explicit `force` ("Check now") dropped during a background poll
 * silently serves the user the cached listing they asked to bypass. So a
 * forced request that arrives mid-flight is **queued** and run once the
 * current fetch settles (at most one queued — a second click while one is
 * already queued is a no-op), while a non-forced poll tick during a fetch is
 * simply redundant and dropped.
 */

/** Opaque handle to the guard's mutable state (opaque to the view). */
export interface MarketFetchGate {
  inFlight: boolean;
  queuedForce: boolean;
}

export function createMarketFetchGate(): MarketFetchGate {
  return { inFlight: false, queuedForce: false };
}

/**
 * Decide what a `load(force)` request should do. Mutates `gate` and returns:
 * - `"run"` — start the fetch now (the gate is marked in-flight);
 * - `"drop"` — a fetch is already running and this request adds nothing;
 * - `"queued"` — a fetch is running; this forced request runs when it settles.
 */
export function marketFetchBegin(gate: MarketFetchGate, force: boolean): "run" | "drop" | "queued" {
  if (gate.inFlight) {
    if (force && !gate.queuedForce) {
      gate.queuedForce = true;
      return "queued";
    }
    return "drop";
  }
  gate.inFlight = true;
  return "run";
}

/**
 * Settle the in-flight fetch and report whether a queued forced refresh must
 * run next (consuming the queue marker). Call exactly once per `"run"`.
 */
export function marketFetchEnd(gate: MarketFetchGate): { runQueuedForce: boolean } {
  gate.inFlight = false;
  const runQueuedForce = gate.queuedForce;
  gate.queuedForce = false;
  return { runQueuedForce };
}
