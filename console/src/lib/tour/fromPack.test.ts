// Unit tests for the pack-tour adapter (ADR 0049 §7, #414 Part C).
//
// This adapter is the single structural validator for third-party tour data: the
// wire format is deliberately free-form so the step union is not typed in three
// places. Everything here is therefore a boundary test — what a malformed,
// hostile, or merely old pack can and cannot make the console do.
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/fromPack.test.ts`.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  journeyFromPackTour,
  packJourneyId,
  registerPackTours,
} from "./journeys/fromPack.ts";
import { allJourneys, clearJourneys, getJourney } from "./registry.ts";
import type { TourContext } from "./types.ts";

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

/** A minimal valid tour: one note step. */
const validTour = (over: Record<string, unknown> = {}) => ({
  id: "t1",
  title: "T",
  blurb: "B",
  steps: [{ id: "s1", kind: "note", title: "St", body: "Sb" }],
  ...over,
});

// ── Namespacing: a pack must not be able to shadow a built-in ───────────────

test("pack journey ids are namespaced by pack", () => {
  assert.equal(packJourneyId("mqtt", "start"), "pack:mqtt:start");
});

test("a pack cannot hijack a built-in journey id", () => {
  // `registerJourney` is keyed by id and last-write-wins, so an unprefixed pack
  // tour called "overview" would REPLACE the console's own overview journey,
  // silently, for everyone with that pack installed. Namespacing makes it
  // impossible rather than merely unlikely.
  const j = journeyFromPackTour("evil-pack", validTour({ id: "overview" }));
  assert.ok(j);
  assert.equal(j.id, "pack:evil-pack:overview");
  assert.notEqual(j.id, "overview");
});

// ── The declarative vocabulary ──────────────────────────────────────────────

test("gates map to the console's own precondition library", () => {
  const j = journeyFromPackTour(
    "p",
    validTour({
      preconditions: ["hasProject"],
      successWhen: "hasTraces",
      steps: [
        { id: "s1", kind: "note", title: "T", body: "B" },
        {
          id: "s2",
          title: "T",
          body: "B",
          selector: '[data-tour="run"]',
          precondition: "hasJsRuntime",
          repair: { id: "fix", kind: "note", title: "T", body: "B" },
        },
      ],
    }),
  );
  assert.ok(j);
  assert.equal(
    j.preconditions?.[0].test(ctx({ projects: [{ name: "p" } as never] })),
    "ok",
  );
  assert.equal(j.preconditions?.[0].test(ctx({ projects: [] })), "skip");
  // successWhen reads as a predicate: satisfied ⇒ true.
  assert.equal(j.successEvent(ctx({ traceCount: 1 })), true);
  assert.equal(j.successEvent(ctx({ traceCount: 0 })), false);
  assert.equal(j.steps[1].precondition?.id, "has-js-runtime");
  assert.equal(j.steps[1].repair?.id, "fix");
});

test("an absent successWhen makes the journey orientation-only", () => {
  // Recorded as complete without claiming an outcome — the same deliberate
  // exception the console's own overview journey takes.
  const j = journeyFromPackTour("p", validTour());
  assert.equal(j?.successEvent(ctx()), true);
});

test("profiles default to studio only", () => {
  // The conservative default: most pack capabilities are authoring surfaces the
  // lean operator build does not ship at all.
  assert.deepEqual(journeyFromPackTour("p", validTour())?.profiles, ["studio"]);
  assert.deepEqual(
    journeyFromPackTour("p", validTour({ profiles: ["observe"] }))?.profiles,
    ["observe"],
  );
  // Unknown profile names are discarded, falling back to the default.
  assert.deepEqual(
    journeyFromPackTour("p", validTour({ profiles: ["mainframe"] }))?.profiles,
    ["studio"],
  );
});

test("a handoff step's verify is derived from verifyPollingJobType", () => {
  const j = journeyFromPackTour(
    "p",
    validTour({
      steps: [
        {
          id: "s1",
          kind: "handoff",
          title: "T",
          body: "B",
          copy: "mosquitto_pub -t x -m '{}'",
          copyLabel: "Copy it",
          verifyPollingJobType: "mqtt:demo",
        },
      ],
    }),
  );
  const step = j?.steps[0];
  assert.equal(step?.kind, "handoff");
  if (step?.kind !== "handoff") return;
  assert.equal(step.copy, "mosquitto_pub -t x -m '{}'");
  assert.equal(step.copyLabel, "Copy it");
  // False until #404's consumer source is registered, so the step stays
  // self-reported rather than auto-advancing on a signal we cannot see.
  assert.equal(step.verify?.(ctx()), false);
  assert.equal(
    step.verify?.(ctx({ consumers: [{ jobType: "mqtt:demo", worker: "w" }] })),
    true,
  );
});

// ── Rejection: whole journey ─────────────────────────────────────────────────

test("a journey missing identity or its card line is rejected", () => {
  assert.equal(journeyFromPackTour("p", { title: "T", blurb: "B" }), null);
  assert.equal(journeyFromPackTour("p", validTour({ title: "" })), null);
  // blurb is what the picker card renders; without it the card is blank.
  assert.equal(journeyFromPackTour("p", validTour({ blurb: undefined })), null);
  assert.equal(journeyFromPackTour("p", null), null);
  assert.equal(journeyFromPackTour("p", "nope"), null);
});

test("a journey over the five-step cap is rejected whole", () => {
  const steps = Array.from({ length: 6 }, (_, i) => ({
    id: `s${i}`,
    kind: "note",
    title: "T",
    body: "B",
  }));
  assert.equal(journeyFromPackTour("p", validTour({ steps })), null);
});

test("an unknown gate rejects the journey rather than widening its reach", () => {
  // Dropping an unrecognised journey-level gate would OFFER a journey its author
  // meant to restrict — failing open on a question about applicability.
  assert.equal(
    journeyFromPackTour("p", validTour({ preconditions: ["hasWidgets"] })),
    null,
  );
  assert.equal(
    journeyFromPackTour("p", validTour({ successWhen: "hasWidgets" })),
    null,
  );
});

test("a journey with no usable steps is rejected", () => {
  assert.equal(journeyFromPackTour("p", validTour({ steps: [] })), null);
  assert.equal(
    journeyFromPackTour("p", validTour({ steps: [{ id: "only-junk" }] })),
    null,
  );
});

// ── Rejection: individual steps ──────────────────────────────────────────────

test("one malformed step is dropped, not the whole journey", () => {
  // A pack should not lose a journey to a single typo; the surviving steps still
  // teach something.
  const j = journeyFromPackTour(
    "p",
    validTour({
      steps: [
        { id: "good", kind: "note", title: "T", body: "B" },
        { id: "bad-spotlight", title: "T", body: "B" }, // no selector
        { id: "bad-handoff", kind: "handoff", title: "T", body: "B" }, // no copy
        { id: "bad-kind", kind: "hologram", title: "T", body: "B" },
        {
          id: "bad-route",
          kind: "note",
          title: "T",
          body: "B",
          route: "relative",
        },
      ],
    }),
  );
  assert.deepEqual(
    j?.steps.map((s) => s.id),
    ["good"],
  );
});

test("a repairable gate without a repair step drops that step", () => {
  // Silently keeping it would cost the user the install hint they needed, which is
  // the exact defect ADR 0049 exists to prevent.
  const j = journeyFromPackTour(
    "p",
    validTour({
      steps: [
        { id: "keep", kind: "note", title: "T", body: "B" },
        {
          id: "gated",
          title: "T",
          body: "B",
          selector: '[data-tour="run"]',
          precondition: "hasJsRuntime",
        },
      ],
    }),
  );
  assert.deepEqual(
    j?.steps.map((s) => s.id),
    ["keep"],
  );
});

test("a nested repair is ignored, and the step still loads", () => {
  const j = journeyFromPackTour(
    "p",
    validTour({
      steps: [
        {
          id: "gated",
          title: "T",
          body: "B",
          selector: '[data-tour="run"]',
          precondition: "hasJsRuntime",
          repair: {
            id: "fix",
            kind: "note",
            title: "T",
            body: "B",
            repair: { id: "deeper", kind: "note", title: "T", body: "B" },
          },
        },
      ],
    }),
  );
  assert.equal(j?.steps[0].repair?.id, "fix");
  assert.equal(j?.steps[0].repair?.repair, undefined);
});

test("duplicate step ids are dropped — they are the analytics keys", () => {
  const j = journeyFromPackTour(
    "p",
    validTour({
      steps: [
        { id: "dup", kind: "note", title: "T", body: "B" },
        { id: "dup", kind: "note", title: "T2", body: "B2" },
      ],
    }),
  );
  assert.equal(j?.steps.length, 1);
  assert.equal(j?.steps[0].title, "T");
});

// ── Registration ─────────────────────────────────────────────────────────────

test("registerPackTours registers valid journeys and skips the rest", () => {
  clearJourneys();
  const ids = registerPackTours([
    { id: "mqtt", tours: [validTour({ id: "a" }), { id: "broken" }] },
    { id: "other", tours: [validTour({ id: "b" })] },
    { id: "no-tours" },
  ] as never);
  assert.deepEqual(ids, ["pack:mqtt:a", "pack:other:b"]);
  assert.equal(allJourneys().length, 2);
  assert.ok(getJourney("pack:mqtt:a"));
  clearJourneys();
});

test("registerPackTours is idempotent across context refreshes", () => {
  // It runs on every context refresh, so repeating must replace rather than
  // duplicate — the registry is keyed by id.
  clearJourneys();
  const packs = [{ id: "mqtt", tours: [validTour({ id: "a" })] }] as never;
  registerPackTours(packs);
  registerPackTours(packs);
  registerPackTours(packs);
  assert.equal(allJourneys().length, 1);
  clearJourneys();
});

// ── The real pilot, end to end ───────────────────────────────────────────────

/**
 * The MQTT pilot tour, copied VERBATIM from `trigger-mqtt`'s manifest in
 * nanobpm/nano-ide (PR #49).
 *
 * Synthetic fixtures prove the rules; this proves the contract holds for the data a
 * real pack actually ships. It is a copy rather than a read of the sibling checkout
 * because a test that needs another repo on disk cannot run in CI — the tradeoff is
 * that it can drift, so it asserts the *properties* the pilot relies on rather than
 * a snapshot, and the pack's own `npm run validate` guards the source side.
 */
const MQTT_PILOT = {
  id: "mqtt-message-starts-a-process",
  title: "Start a process from a broker message",
  blurb:
    "Wire an MQTT topic to a process start — the “when this happens…” half of an automation.",
  profiles: ["studio"],
  preconditions: ["hasProject"],
  steps: [
    {
      id: "what-this-adds",
      kind: "note",
      title: "An MQTT message can start a process",
      body: "Installing this pack adds a trigger source kind, `mqtt`, that any App can declare in its `nano.app.json`. You name a broker and a topic filter; the runtime owns the durable inbox, dispatch, retry and lifecycle, and auto-launches this pack's driver (ADR 0025). Each message can start a process or correlate into a running one.",
    },
    {
      id: "run-the-app",
      title: "Run the app so the driver starts",
      body: "The trigger driver is a supervised out-of-process child of your running app — nothing subscribes until the app is running.",
      selector: '[data-tour="run"]',
      side: "bottom",
      align: "start",
      precondition: "hasJsRuntime",
      repair: {
        id: "need-a-runtime",
        kind: "note",
        title: "Install a JavaScript runtime first",
        body: "Running an app needs Node ≥ 22.6 (the npm launcher ships one). Until then you can still author the trigger — only starting it is unavailable.",
      },
    },
    {
      id: "publish-a-test-message",
      kind: "handoff",
      title: "Publish a test message",
      body: "Any MQTT client will do — this step is outside the console because the broker is. Adjust the topic to match the filter you declared.",
      copy: "mosquitto_pub -h localhost -t home/porch/motion -m '{\"value\":1}'",
      copyLabel: "Copy command",
    },
    {
      id: "see-the-instance",
      title: "See what the message started",
      body: "The message that arrived is now a process instance, and its payload is the instance's variables — an event your process can reason about.",
      route: "/explorer",
      selector: '[data-tour="nav-explorer"]',
      side: "right",
      align: "start",
    },
  ],
} as const;

test("the real MQTT pilot tour converts into a runnable journey", () => {
  const j = journeyFromPackTour("nano-ide-trigger-mqtt", MQTT_PILOT);
  assert.ok(j, "the shipped pilot must convert");

  assert.equal(
    j.id,
    "pack:nano-ide-trigger-mqtt:mqtt-message-starts-a-process",
  );
  assert.deepEqual(j.profiles, ["studio"]);
  // Not offered without a project to attach a trigger to.
  assert.equal(j.preconditions?.length, 1);
  assert.equal(j.preconditions?.[0].test(ctx({ projects: [] })), "skip");

  // All four steps survive validation — no silent losses in the shipped pilot.
  assert.equal(j.steps.length, 4);
  assert.deepEqual(
    j.steps.map((s) => s.kind),
    ["note", "spotlight", "handoff", "spotlight"],
  );

  // The runtime-gated step carries its repair, so a host with no Node gets an
  // install hint instead of being told to press Run.
  const gated = j.steps[1];
  assert.equal(gated.precondition?.id, "has-js-runtime");
  assert.ok(gated.repair, "a hasJsRuntime gate must author a repair");

  // The handoff is the reason handoff exists: the broker is outside the console.
  const handoff = j.steps[2];
  assert.equal(handoff.kind, "handoff");
  if (handoff.kind !== "handoff") return;
  assert.match(handoff.copy, /^mosquitto_pub /);
  // No verifyPollingJobType on the pilot, so it self-reports rather than claiming
  // to have observed something it cannot see.
  assert.equal(handoff.verify, undefined);

  // successWhen is deliberately absent: the closed gate vocabulary cannot express
  // "an instance started from a broker message", so the pilot declines to claim an
  // outcome rather than proxying a different one (raised on #414).
  assert.equal(j.successEvent(ctx()), true);
});
