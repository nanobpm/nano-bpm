// Unit guards for Journey 2 — 90-second RAD fullstack prototype (#410).
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/rad.test.ts`.
// The module is import-safe here precisely because everything that touches
// `window` / the network lives inside a function body.

import { test } from "node:test";
import assert from "node:assert/strict";
import { resolveSteps } from "./registry.ts";
import type { TourContext } from "./types.ts";
import {
  RAD_JOURNEY_ID,
  SCRATCH_INSTANCE_STARTED,
  SCRATCH_SERVED_ANSWERED,
  URBAN_APP_DEFAULT_PORT,
  rad,
  radSucceeded,
  servedAppPort,
  servedAppUrl,
} from "./journeys/rad.ts";

function ctx(over: Partial<TourContext> = {}): TourContext {
  return {
    profile: "studio",
    route: "/projects",
    projects: [],
    nodeAvailable: true,
    templates: [],
    extensions: [],
    scratch: {},
    ...over,
  };
}

test("servedAppUrl keeps the live origin's host/scheme but swaps to the app port", () => {
  // The served app is its own web server on its own port — the URL must follow
  // however the user reached the IDE (LAN, tunnel), never hardcode localhost.
  assert.equal(
    servedAppUrl("http://192.168.1.5:8080", 8090),
    "http://192.168.1.5:8090/",
  );
  assert.equal(
    servedAppUrl("https://nano.example:8443/", 9000),
    "https://nano.example:9000/",
  );
  assert.equal(
    servedAppUrl("http://localhost:3000", 8090),
    "http://localhost:8090/",
  );
});

test("servedAppUrl falls back to a loopback URL on a malformed origin", () => {
  assert.equal(servedAppUrl("not a url", 8090), "http://127.0.0.1:8090/");
});

test("servedAppPort reads PORT from config, preferring the active run config", () => {
  // Default when nothing is configured is the scaffold's own documented default.
  assert.equal(servedAppPort(null), URBAN_APP_DEFAULT_PORT);
  assert.equal(servedAppPort({}), URBAN_APP_DEFAULT_PORT);
  // Project-level env.
  assert.equal(servedAppPort({ env: { PORT: "7000" } }), 7000);
  // Active run config wins over project env.
  assert.equal(
    servedAppPort({
      env: { PORT: "7000" },
      toolchain: {
        activeRunConfig: "prod",
        runConfigs: [
          { id: "dev", default: true, env: { PORT: "8100" } },
          { id: "prod", env: { PORT: "9100" } },
        ],
      },
    }),
    9100,
  );
  // With no active id, the default-flagged run config is used.
  assert.equal(
    servedAppPort({
      toolchain: {
        runConfigs: [{ id: "dev", default: true, env: { PORT: "8100" } }],
      },
    }),
    8100,
  );
  // A non-numeric / empty PORT is ignored, falling through to the default.
  assert.equal(
    servedAppPort({ env: { PORT: "not-a-port" } }),
    URBAN_APP_DEFAULT_PORT,
  );
});

test("successEvent requires BOTH the served app answering AND an instance existing", () => {
  assert.equal(radSucceeded(ctx()), false);
  assert.equal(
    radSucceeded(ctx({ scratch: { [SCRATCH_SERVED_ANSWERED]: true } })),
    false,
    "served answering alone is not success — the UI must have started something",
  );
  assert.equal(
    radSucceeded(ctx({ scratch: { [SCRATCH_INSTANCE_STARTED]: true } })),
    false,
    "an instance alone is not success — the served UI must be reachable",
  );
  assert.equal(
    radSucceeded(
      ctx({
        scratch: {
          [SCRATCH_SERVED_ANSWERED]: true,
          [SCRATCH_INSTANCE_STARTED]: true,
        },
      }),
    ),
    true,
  );
});

test("the run-and-serve step gates on a JS runtime and authors an install-hint repair", () => {
  const step = rad.steps.find((s) => s.id === "run-and-serve");
  assert.ok(step, "expected a run-and-serve step");
  assert.equal(step!.precondition?.id, "has-js-runtime");
  assert.ok(
    step!.repair,
    "Run without a runtime must repair into an install hint, not vanish",
  );
});

test("with no runtime, resolveSteps substitutes the install-hint repair for the serve step", () => {
  const noRuntime = ctx({ nodeAvailable: false });
  const resolved = resolveSteps(rad, noRuntime);
  assert.ok(
    resolved.steps.some((s) => s.step.id === "served-app-needs-runtime"),
    "the runtime repair step should appear when both runtimes are absent",
  );
  assert.ok(
    !resolved.steps.some((s) => s.step.id === "run-and-serve"),
    "the original serve step should be replaced, not kept, when repairing",
  );
});

test("with a runtime present, resolveSteps keeps the serve step as authored", () => {
  const resolved = resolveSteps(rad, ctx());
  assert.ok(resolved.steps.some((s) => s.step.id === "run-and-serve"));
  assert.ok(
    !resolved.steps.some((s) => s.step.id === "served-app-needs-runtime"),
  );
});

test("the journey is studio-only, five steps, and terminates cleanly", () => {
  assert.equal(rad.id, RAD_JOURNEY_ID);
  assert.deepEqual(rad.profiles, ["studio"]);
  assert.equal(rad.steps.length, 5);
});
