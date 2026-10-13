// The served Urban app's URL — its OWN port, distinct from the console origin.
//
// A compiled Urban app is its own web server (ADR 0009 / ADR 0022 §"single
// binary"): `main.ts` does `servePages({ port })`, and that port is the app's,
// not the console/gateway origin's. So "open the served UI" cannot reuse the
// console origin — it has to point at the app's actual port, read from the
// project config rather than hardcoded.
//
// This module is deliberately **side-effect-free** (no registration, no
// top-level `window`/network access): both the workspace UI and the RAD guided
// journey import these helpers, so it must not drag journey registration into
// the workspace route's dependency graph.

/**
 * The urban-starter scaffold's default served port. Single-sourced here with a
 * citation so the fallback is the scaffold's real default, not a magic number:
 * `URBAN_MAIN_TS` in server/crates/nano-server-console/src/projects.rs does
 * `Number(process.env.PORT ?? 8090)` and `servePages({ port })`.
 */
export const URBAN_APP_DEFAULT_PORT = 8090;

/** The minimal shape of a project config a served-app port is read out of. */
export interface PortConfig {
  env?: Record<string, string> | null;
  toolchain?: {
    runConfigs?:
      | { id: string; default?: boolean; env?: Record<string, string> | null }[]
      | null;
    activeRunConfig?: string | null;
  } | null;
}

function portFromEnv(env?: Record<string, string> | null): number | undefined {
  const raw = env?.PORT;
  if (raw === undefined) return undefined;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

/**
 * The port the served Urban app listens on, read from the project config: the
 * active run config's `PORT` env, then the project-level `PORT` env, then the
 * scaffold default. Never a hardcoded literal in the URL itself — the default is
 * the scaffold's own documented default (see `URBAN_APP_DEFAULT_PORT`).
 */
export function servedAppPort(config: PortConfig | null | undefined): number {
  const tc = config?.toolchain;
  const configs = tc?.runConfigs ?? [];
  const active =
    (tc?.activeRunConfig
      ? configs.find((c) => c.id === tc.activeRunConfig)
      : undefined) ??
    configs.find((c) => c.default) ??
    configs[0];
  return (
    portFromEnv(active?.env) ??
    portFromEnv(config?.env) ??
    URBAN_APP_DEFAULT_PORT
  );
}

/**
 * The served app's URL: the console's own origin (so the host/scheme match how
 * the user reached the IDE — a LAN address or a tunnel, never a hardcoded
 * `localhost`) but on the *app's* port. Pure, so it is unit-testable without a
 * browser.
 */
export function servedAppUrl(origin: string, port: number): string {
  try {
    const u = new URL(origin);
    u.port = String(port);
    u.pathname = "/";
    u.search = "";
    u.hash = "";
    return u.toString();
  } catch {
    return `http://127.0.0.1:${port}/`;
  }
}

// ---------------------------------------------------------------------------
// The running app's served URL, published by the workspace for the RAD journey's
// success probe. Persisted (not passed in memory) so the signal survives the
// reload a resumable journey allows, and so the journey source — which can run
// from any route — reads the *real* configured port instead of guessing the
// default. Same localStorage-affordance pattern as the localdev journey.
// ---------------------------------------------------------------------------

const SERVED_APP_URL_KEY = "nano.servedApp.url";

function safeStorage(): Storage | null {
  try {
    return globalThis.localStorage ?? null;
  } catch {
    return null;
  }
}

/** Publish the currently-running Urban app's served URL (its real port). */
export function rememberServedAppUrl(url: string): void {
  try {
    safeStorage()?.setItem(SERVED_APP_URL_KEY, url);
  } catch {
    /* a signal we could not persist just reads as no running app */
  }
}

/** Clear the published served URL (the app stopped or the workspace unmounted). */
export function forgetServedAppUrl(): void {
  try {
    safeStorage()?.removeItem(SERVED_APP_URL_KEY);
  } catch {
    /* nothing to do */
  }
}

/** The currently-running Urban app's served URL, or `null` if none is running. */
export function readServedAppUrl(): string | null {
  try {
    return safeStorage()?.getItem(SERVED_APP_URL_KEY) ?? null;
  } catch {
    return null;
  }
}
