// Opt-in gate for LIVE real-adapter activation (issue #297, slice S4).
//
// LIVE activation of a real AI backend — constructing/selecting an adapter, loading its
// optional dependency, and performing network I/O — is gated behind an explicit env
// opt-in. This is SEPARATE from the STATIC seam-inventory descriptor (see ./index.ts),
// which is registered unconditionally at import and needs no opt-in.
//
// Contract key (blackboard dedupe_key `env:URBAN_TESTKIT_AI_REAL`): setting
// `URBAN_TESTKIT_AI_REAL` to a truthy value (`1`/`true`/`yes`/`on`) is REQUIRED before any
// real adapter may be constructed. With it unset, every construction factory throws before
// touching an optional dependency or the network.

/** The env var that opts LIVE real-adapter activation in. Off (unset) by default. */
export const REAL_AI_OPT_IN_ENV = "URBAN_TESTKIT_AI_REAL";

/**
 * Env read over Node's `process.env` (the sole JS/TS runtime, ADR 0065). Uses `Reflect` so
 * it needs no `as` cast; returns `undefined` when the host exposes no env or the key is
 * absent.
 */
export function readEnvVar(name: string): string | undefined {
  const globalObject: unknown = globalThis;
  if (typeof globalObject !== "object" || globalObject === null) {
    return undefined;
  }
  const proc = Reflect.get(globalObject, "process");
  if (typeof proc === "object" && proc !== null) {
    const env = Reflect.get(proc, "env");
    if (typeof env === "object" && env !== null) {
      const value = Reflect.get(env, name);
      if (typeof value === "string") {
        return value;
      }
    }
  }
  return undefined;
}

/** True only when the LIVE real-adapter opt-in env is set to a truthy value. */
export function isRealAiEnabled(): boolean {
  const raw = readEnvVar(REAL_AI_OPT_IN_ENV);
  if (raw === undefined) {
    return false;
  }
  const normalized = raw.trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

/**
 * Throws unless the LIVE opt-in is set. Every real-adapter construction factory calls this
 * FIRST — before any dynamic `import()` or network I/O — so the default CI path can never
 * instantiate a real adapter or load an optional dependency.
 */
export function assertRealAiEnabled(): void {
  if (!isRealAiEnabled()) {
    throw new Error(
      `real AI adapter requires explicit opt-in: set ${REAL_AI_OPT_IN_ENV}=1 ` +
        "(live activation loads an optional dependency and performs network I/O)",
    );
  }
}

/**
 * Lazily imports an OPTIONAL dependency by a NON-LITERAL specifier so `tsc` performs no
 * static module resolution for it (the dep is intentionally absent on the default lane).
 * Only ever called from inside an opt-in-gated construction path.
 */
export async function importOptionalDependency(specifier: string): Promise<unknown> {
  const load = (moduleSpecifier: string): Promise<unknown> => import(moduleSpecifier);
  try {
    return await load(specifier);
  } catch (cause) {
    // The dep is intentionally absent on the default lane; surface an actionable message
    // (which optional dependency, and that it must be installed) while preserving the raw
    // module-resolution error as the cause instead of leaking it uncontextualized.
    throw new Error(
      `optional dependency '${specifier}' could not be loaded; install it to activate ` +
        `this real AI adapter (it is an optional peer dependency and may not be installed)`,
      { cause },
    );
  }
}
