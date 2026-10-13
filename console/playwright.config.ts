// Playwright config for the guided-journey e2e guards (ADR 0049, #417) and the
// mobile-first console guards (issue #1005, unit A7).
//
// Scoped deliberately narrowly: the desktop journey suite exists to catch the
// class of breakage no other gate can see — a journey step pointing at a
// `data-tour` anchor that has been renamed or deleted. Nothing in a normal build
// or unit test fails when that happens; the step just silently spotlights nothing.
//
// The mobile suite (`e2e/mobile/`) is the compensating control for the same class
// of problem one layer up: the mobile-first views (A1–A6) branch their entire
// presentation on `useIsNarrow()`, and nothing in a build or unit test proves the
// card home, the hamburger sheet, the Explorer list→detail flow, the instance
// drill-in cards, the app UI/Logs cards or the `?instance=` deep-link actually
// render — and stay free of horizontal overflow — at a phone viewport. Only a
// browser sized to a phone can.
//
// The suite never talks to a real gateway. Every test stubs the console API, so a
// run is deterministic and needs no engine and no scaffolded project. (Playwright
// itself still runs on Node — that is the test *runner*; what these tests stub is
// the **gateway-side** runtime, so no Node is required on the machine the gateway
// would be simulating.) That is what keeps it honest under this repo's
// no-intermittent-failures, no-retries rule.

import { defineConfig, devices } from "@playwright/test";

/** Kept off 5173 so a developer's own `npm run dev` can stay up during a run. */
const PORT = Number(process.env.E2E_PORT ?? 5177);

/**
 * The observe-profile dev server runs on its own port so a single Playwright run
 * can exercise BOTH build profiles (ADR 0034): the default `studio` server on
 * `PORT`, and a `VITE_CONSOLE_PROFILE=observe` server here. The home spec runs
 * against each — the operator ("observe") build has no Studio route and a
 * different landing, so the card home must render the right set in each profile.
 */
const OBSERVE_PORT = Number(process.env.E2E_OBSERVE_PORT ?? PORT + 1);

// The observe profile needs a SECOND dev server on OBSERVE_PORT. It is on by
// default so CI and full local runs keep the operator-build home coverage, but
// Playwright starts every `webServer` entry regardless of `--project`, so a
// desktop-only run (`--project=chromium`) would otherwise still force
// OBSERVE_PORT to be free. Set `E2E_WITH_OBSERVE=0` to drop the observe project
// and its dev server, so such runs need only one port.
const WITH_OBSERVE = process.env.E2E_WITH_OBSERVE !== "0";

// Match the webServer bind address (127.0.0.1, set below) exactly: on an
// IPv6-first host `localhost` can resolve to ::1 while the server listens only
// on 127.0.0.1, reintroducing the poll-until-timeout class the bind avoids.
const STUDIO_URL = `http://127.0.0.1:${PORT}/console/`;
const OBSERVE_URL = `http://127.0.0.1:${OBSERVE_PORT}/console/`;

/**
 * A phone viewport (375×812, the iPhone-class size unit A7 targets) driven by the
 * Chromium engine. We compose it from Desktop Chrome rather than a webkit iPhone
 * descriptor so the run uses the one browser Playwright installs by default, and
 * add `isMobile`/`hasTouch` so the touch-only affordances (the BpmnViewer pinch /
 * pan) receive real touch events.
 */
const phone = {
  ...devices["Desktop Chrome"],
  viewport: { width: 375, height: 812 },
  isMobile: true,
  hasTouch: true,
  deviceScaleFactor: 3,
};

export default defineConfig({
  testDir: "./e2e",
  // The gateway serves the SPA under /console and the router shares that
  // basename, so tests navigate relative paths like "projects".
  use: {
    baseURL: STUDIO_URL,
    trace: "retain-on-failure",
  },
  projects: [
    {
      // The desktop journey/rail/modeler guards. They assert desktop-only chrome
      // (rail widths, the resizable panes), so they must NOT run at a phone
      // viewport — the mobile suite lives under `e2e/mobile/` and is excluded here.
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
      testIgnore: /mobile\//,
    },
    {
      // Every mobile guard, at 375×812, against the default `studio` build.
      name: "mobile-studio",
      use: { ...phone, baseURL: STUDIO_URL },
      testMatch: /mobile\/.*\.spec\.ts$/,
    },
    {
      // Only the home guard re-runs against the lean `observe` build: it is the
      // one surface whose card set and landing route differ by profile. The other
      // mobile guards exercise studio-only routes (/projects, /apps, …). Gated on
      // WITH_OBSERVE so a desktop-only run needn't hold OBSERVE_PORT.
      name: "mobile-observe",
      use: { ...phone, baseURL: OBSERVE_URL },
      testMatch: /mobile\/home\.spec\.ts$/,
    },
  ].filter((p) => WITH_OBSERVE || p.name !== "mobile-observe"),
  // No retries, anywhere. A guard that only passes on a second attempt is
  // reporting a real defect (in the app or in the test), and this repo does not
  // paper over either.
  // One worker: the suite shares the vite dev servers, and running specs in
  // parallel against them made first-hit lazy-chunk compilation the slowest thing
  // in the run — a race whose losers surfaced as different tests failing on
  // different runs. That is a real ordering defect, not something to absorb: a
  // deterministic serial run removes the race so every run exercises the same
  // path, which is what this repo's no-intermittent-failures rule requires.
  workers: 1,
  retries: 0,
  forbidOnly: !!process.env.CI,
  reporter: process.env.CI ? "list" : "line",
  webServer: [
    {
      // Bind explicitly: vite's default `localhost` can resolve to ::1, which left
      // Playwright polling 127.0.0.1 until it timed out.
      command: `npm run dev -- --port ${PORT} --strictPort --host 127.0.0.1`,
      url: STUDIO_URL,
      reuseExistingServer: !process.env.CI,
      timeout: 120_000,
    },
    {
      // The observe-profile server for the home guard. `VITE_CONSOLE_PROFILE` is
      // read at dev-server start (vite `define`s `__STUDIO__` from it), so a
      // separate process is the only way to serve the operator build alongside
      // the studio one. Gated on WITH_OBSERVE (see above) so a desktop-only run
      // needn't start it or hold OBSERVE_PORT.
      command: `npm run dev -- --port ${OBSERVE_PORT} --strictPort --host 127.0.0.1`,
      url: OBSERVE_URL,
      reuseExistingServer: !process.env.CI,
      timeout: 120_000,
      env: { VITE_CONSOLE_PROFILE: "observe" },
    },
  ].filter((s) => WITH_OBSERVE || s.url !== OBSERVE_URL),
});
