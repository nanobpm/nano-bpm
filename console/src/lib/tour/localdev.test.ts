// Unit guards for Journey 1 — headless Camunda-compatible local dev (#409).
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/localdev.test.ts`.
// The module is import-safe here precisely because everything that touches
// `window` / `localStorage` / the network lives inside a function body.

import { test } from "node:test";
import assert from "node:assert/strict";
import { enrichContext } from "./context.ts";
import { resolveSteps } from "./registry.ts";
import type { TourContext } from "./types.ts";
import {
  LOCALDEV_JOURNEY_ID,
  SCRATCH_BASE_URL_COPIED,
  SCRATCH_EXPLORER_REACHED,
  localdev,
  localdevSucceeded,
  markBaseUrlCopied,
  markExplorerReached,
  resetLocaldevSignals,
  swaggerUrl,
  v2BaseUrl,
} from "./journeys/localdev.ts";

function ctx(over: Partial<TourContext> = {}): TourContext {
  return {
    profile: "studio",
    route: "/explorer",
    projects: [],
    nodeAvailable: true,
    templates: [],
    extensions: [],
    scratch: {},
    ...over,
  };
}

test("v2BaseUrl derives from the live origin, on a non-8080 port, no double slash", () => {
  assert.equal(v2BaseUrl("http://127.0.0.1:9137"), "http://127.0.0.1:9137/v2");
  assert.equal(
    v2BaseUrl("https://nano.example:8443/"),
    "https://nano.example:8443/v2",
  );
  assert.equal(v2BaseUrl("http://localhost:3000"), "http://localhost:3000/v2");
});

test("swaggerUrl derives from the same origin", () => {
  assert.equal(
    swaggerUrl("http://127.0.0.1:9137"),
    "http://127.0.0.1:9137/swagger",
  );
});

test("the handoff step offers the origin-derived v2 URL, not a hardcoded 8080", () => {
  const handoff = localdev.steps.find((s) => s.kind === "handoff");
  assert.ok(handoff && handoff.kind === "handoff");
  // In a Node context the module falls back to a placeholder origin; the point
  // this asserts is that the copy value is a /v2 URL derived through v2BaseUrl,
  // never a literal the browser would ignore.
  assert.match(handoff.copy, /\/v2$/);
  assert.equal(
    handoff.verify,
    undefined,
    "handoff is self-reported (I've done it)",
  );
});

test("successEvent needs BOTH the base URL taken and Explorer reached", () => {
  assert.equal(localdevSucceeded(ctx()), false, "neither signal");
  assert.equal(
    localdevSucceeded(ctx({ scratch: { [SCRATCH_BASE_URL_COPIED]: true } })),
    false,
    "only copied",
  );
  assert.equal(
    localdevSucceeded(ctx({ scratch: { [SCRATCH_EXPLORER_REACHED]: true } })),
    false,
    "only explorer",
  );
  assert.equal(
    localdevSucceeded(
      ctx({
        scratch: {
          [SCRATCH_BASE_URL_COPIED]: true,
          [SCRATCH_EXPLORER_REACHED]: true,
        },
      }),
    ),
    true,
    "both signals present",
  );
});

test("the optional traces step skips cleanly with zero traces", () => {
  for (const traceCount of [undefined, 0]) {
    const res = resolveSteps(localdev, ctx({ traceCount }));
    assert.ok(
      !res.steps.some((s) => s.step.id === "traces-why-slow"),
      `traces step must be absent when traceCount=${traceCount}`,
    );
    assert.ok(
      res.skipped.some((s) => s.preconditionId === "has-traces"),
      "the skip must be reported for analytics",
    );
  }
});

test("the optional traces step appears once traces exist", () => {
  const res = resolveSteps(localdev, ctx({ traceCount: 3 }));
  assert.ok(
    res.steps.some((s) => s.step.id === "traces-why-slow"),
    "traces step must be present when traceCount > 0",
  );
});

test("the journey is profile-agnostic and deliberately terminal", () => {
  assert.equal(localdev.id, LOCALDEV_JOURNEY_ID);
  assert.deepEqual([...localdev.profiles].sort(), ["observe", "studio"]);
  assert.equal(
    localdev.nextJourneys,
    undefined,
    "a headless user's time is respected — no chained journey",
  );
  assert.ok(
    localdev.steps.length <= 5,
    "ADR 0049 caps a journey at five steps",
  );
});

// Regression (issue #440): the journey's own flow must be able to satisfy
// `successEvent`. `successEvent` needs the base-URL-taken signal, but that was
// only latched by the Explorer durable affordance — a path a user following the
// journey never touches. The handoff step's copy must latch it too, or success
// is unreachable via the tour. This guards the whole class: the flow that the
// journey walks must reach its own success.
test("issue #440: following the journey flow (handoff copy + reach Explorer) satisfies successEvent", async () => {
  // Stub localStorage so the persisted signals actually round-trip in Node.
  const store = new Map<string, string>();
  const stub = {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, v),
    removeItem: (k: string) => void store.delete(k),
    clear: () => store.clear(),
    key: () => null,
    length: 0,
  };
  Reflect.set(globalThis, "localStorage", stub);
  try {
    resetLocaldevSignals();

    const handoff = localdev.steps.find((s) => s.kind === "handoff");
    assert.ok(handoff && handoff.kind === "handoff", "journey 1 has a handoff");
    assert.equal(
      handoff.onCopied,
      markBaseUrlCopied,
      "the handoff latches the base-URL-taken signal on copy",
    );

    // Walk the journey's own flow: copy in the handoff popover, reach Explorer.
    handoff.onCopied?.();
    markExplorerReached();

    // Enrich a bare context through the registered sources, exactly as the
    // runner does at completion, then evaluate the real success predicate.
    const enriched = await enrichContext(ctx());
    assert.equal(enriched.scratch[SCRATCH_BASE_URL_COPIED], true);
    assert.equal(enriched.scratch[SCRATCH_EXPLORER_REACHED], true);
    assert.equal(localdevSucceeded(enriched), true);

    resetLocaldevSignals();
  } finally {
    Reflect.deleteProperty(globalThis, "localStorage");
  }
});
