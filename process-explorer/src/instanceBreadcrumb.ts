// Pure ancestor-chain helpers for the InstanceDetail call-activity breadcrumb
// (see InstanceDetail.tsx).
//
// When viewing a call-activity **child** instance, InstanceDetail shows an
// Operate-style breadcrumb climbing from the root process instance down to the
// current one. The chain is built by following each instance's
// `parent_process_instance_key` (the snake_case wire field added by the
// foundation slice #1113 — NOT camelCase) hop-by-hop until it is `null` (root).
//
// The climbing and the "which hop is clickable / what a click navigates to"
// decisions are side-effect-free, so they live here where the console's
// node:test unit suite can cover them directly (the component itself only runs
// in a browser).

/** The subset of a console `Instance` the ancestor climb depends on. */
export interface AncestorSource {
  key: string;
  process_id: string;
  /// C8 parent linkage: the key of the calling (parent) process instance, or
  /// `null` for a top-level instance. #1113 delivers this on the console
  /// `Instance` type as the snake_case `parent_process_instance_key`.
  parent_process_instance_key: string | null;
}

/** One resolved ancestor (or the current instance) in the breadcrumb chain. */
export interface AncestorInstance {
  key: string;
  processId: string;
}

/** A rendered breadcrumb segment. The last (current) hop is not clickable. */
export interface BreadcrumbHop {
  key: string;
  label: string;
  isCurrent: boolean;
}

/**
 * Build the ancestor chain for `startKey`, **root-first**, by climbing
 * `parent_process_instance_key` hop-by-hop until it is `null` (the root).
 *
 * Resilience: a missing/evicted ancestor (the fetch returns `null`/`undefined`
 * or throws) stops the climb and returns what resolved so far — the breadcrumb
 * degrades to a partial chain rather than failing. A `maxHops` cap and a
 * visited-set cycle guard keep a corrupt linkage from looping forever.
 */
export async function buildAncestorChain(
  startKey: string,
  fetchInstance: (key: string) => Promise<AncestorSource | null | undefined>,
  maxHops = 100,
): Promise<AncestorInstance[]> {
  const chain: AncestorInstance[] = [];
  const seen = new Set<string>();
  let key: string | null = startKey;
  for (let i = 0; key != null && i < maxHops; i++) {
    if (seen.has(key)) break; // cycle guard: a corrupt parent linkage
    seen.add(key);
    let inst: AncestorSource | null | undefined;
    try {
      inst = await fetchInstance(key);
    } catch {
      inst = null; // network / evicted ancestor: stop with what resolved
    }
    if (!inst) break;
    chain.push({ key: inst.key, processId: inst.process_id });
    key = inst.parent_process_instance_key;
  }
  chain.reverse(); // climbed child→root; render root→child
  return chain;
}

/**
 * The breadcrumb segments for a resolved chain. A top-level instance (no
 * parent — a chain of just itself, or an empty/failed climb) shows **no**
 * breadcrumb, so this returns `[]`. Otherwise every hop is a segment and only
 * the last one is marked `isCurrent` (not clickable).
 */
export function breadcrumbHops(chain: AncestorInstance[]): BreadcrumbHop[] {
  if (chain.length <= 1) return [];
  return chain.map((c, i) => ({
    key: c.key,
    label: c.processId,
    isCurrent: i === chain.length - 1,
  }));
}

/**
 * Activate a breadcrumb hop: navigate to that instance via `onNavigateInstance`
 * unless it is the current instance (a no-op) or no navigation handler was
 * provided. This is the single place the component's click/keyboard handlers
 * delegate to, so the "click navigates to the right key" behaviour is unit
 * testable without a DOM.
 */
export function activateHop(
  hop: BreadcrumbHop,
  onNavigateInstance?: (instanceKey: string) => void,
): void {
  if (hop.isCurrent) return;
  onNavigateInstance?.(hop.key);
}
