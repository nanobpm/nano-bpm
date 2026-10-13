// Unit tests for journey persistence, the v1 → v2 migration, deep links and
// context assembly (ADR 0049 §2, §4, §5).
//
// Node-native: `node --experimental-strip-types --test src/lib/tour/state.test.ts`.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  LEGACY_SEEN_KEY,
  OVERVIEW_JOURNEY_ID,
  STORAGE_KEY,
  activeJourneyId,
  emptyState,
  hasCompleted,
  readState,
  recordFor,
  resetState,
  startupPanelEnabled,
  withJourney,
  withStartupPanel,
  writeState,
} from "./state.ts";
import {
  readTourParam,
  stripTourParam,
  consumeDeepLinkTourParam,
} from "./deepLink.ts";
import {
  baseContext,
  clearContextSources,
  enrichContext,
  registerContextSource,
} from "./context.ts";

/** An in-memory localStorage stand-in. */
function fakeStorage(seed: Record<string, string> = {}) {
  const map = new Map(Object.entries(seed));
  return {
    map,
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
  };
}

/** Storage that throws on every access — private mode / storage disabled. */
const hostileStorage = {
  getItem() {
    throw new Error("denied");
  },
  setItem() {
    throw new Error("denied");
  },
  removeItem() {
    throw new Error("denied");
  },
};

// ── state ────────────────────────────────────────────────────────────────────

test("readState: empty storage yields an empty v2 state", () => {
  assert.deepEqual(readState(fakeStorage()), emptyState());
});

test("readState: round-trips through writeState", () => {
  const s = fakeStorage();
  const written = withJourney(emptyState(), "localdev", {
    status: "active",
    stepIndex: 2,
  });
  writeState(written, s);
  assert.deepEqual(readState(s), written);
});

test("readState: migrates the v1 seen flag to a completed overview", () => {
  const s = fakeStorage({ [LEGACY_SEEN_KEY]: "1" });
  const state = readState(s);
  assert.equal(
    hasCompleted(state, OVERVIEW_JOURNEY_ID),
    true,
    "an upgrader who already saw the tour must not be re-toured",
  );
});

test("readState: migration never overwrites a newer v2 record", () => {
  // Someone who replayed the overview after upgrading is mid-journey; the legacy
  // flag must not stomp that back to completed.
  const s = fakeStorage({
    [LEGACY_SEEN_KEY]: "1",
    [STORAGE_KEY]: JSON.stringify({
      version: 2,
      journeys: { [OVERVIEW_JOURNEY_ID]: { status: "active", stepIndex: 3 } },
    }),
  });
  const rec = recordFor(readState(s), OVERVIEW_JOURNEY_ID);
  assert.equal(rec.status, "active");
  assert.equal(rec.stepIndex, 3);
});

test("readState: corrupt or wrong-version payloads degrade to empty, not a throw", () => {
  assert.deepEqual(
    readState(fakeStorage({ [STORAGE_KEY]: "{not json" })),
    emptyState(),
  );
  assert.deepEqual(
    readState(fakeStorage({ [STORAGE_KEY]: JSON.stringify({ version: 1 }) })),
    emptyState(),
  );
  assert.deepEqual(
    readState(fakeStorage({ [STORAGE_KEY]: JSON.stringify(null) })),
    emptyState(),
  );
});

test("state access never throws when storage is unavailable", () => {
  // Onboarding state is never worth breaking the app over.
  assert.deepEqual(readState(hostileStorage), emptyState());
  assert.doesNotThrow(() => writeState(emptyState(), hostileStorage));
  assert.doesNotThrow(() => resetState(hostileStorage));
  assert.deepEqual(readState(null), emptyState());
});

test("withJourney: merges without mutating, and activeJourneyId finds the live one", () => {
  const before = emptyState();
  const after = withJourney(before, "rad", { status: "active", stepIndex: 1 });
  assert.deepEqual(before, emptyState(), "must not mutate the input");
  assert.equal(activeJourneyId(after), "rad");
  const done = withJourney(after, "rad", { status: "completed" });
  assert.equal(activeJourneyId(done), undefined);
  assert.equal(
    recordFor(done, "rad").stepIndex,
    1,
    "unspecified fields persist",
  );
});

test("recordFor: a malformed stored record degrades to unseen, never throws", () => {
  // isTourState only guarantees `journeys` is an object; a hand-corrupted record
  // (string, or missing/invalid fields) must not reach a caller that reads
  // `.status`. Each of these must fall back to the unseen default.
  for (const bad of [
    "oops",
    42,
    null,
    {},
    { status: "active" },
    { status: "bogus", stepIndex: 0 },
    { stepIndex: 2 },
  ]) {
    const state = {
      version: 2 as const,
      journeys: { overview: bad as never },
    };
    assert.deepEqual(recordFor(state, "overview"), {
      status: "unseen",
      stepIndex: 0,
    });
  }
  // A well-formed record is still returned as-is.
  const good = withJourney(emptyState(), "overview", {
    status: "completed",
    stepIndex: 3,
  });
  assert.equal(recordFor(good, "overview").status, "completed");
  assert.equal(recordFor(good, "overview").stepIndex, 3);
});

test("resetState: clears the legacy flag too, or the migration would resurrect it", () => {
  const s = fakeStorage({
    [LEGACY_SEEN_KEY]: "1",
    [STORAGE_KEY]: JSON.stringify(emptyState()),
  });
  resetState(s);
  assert.deepEqual(readState(s), emptyState());
  assert.equal(hasCompleted(readState(s), OVERVIEW_JOURNEY_ID), false);
});

// ── startup panel preference (#464) ────────────────────────────────────────

test("startupPanelEnabled: defaults to true when unset (fresh install shows it)", () => {
  assert.equal(startupPanelEnabled(emptyState()), true);
});

test("startupPanelEnabled: only an explicit false suppresses it", () => {
  assert.equal(
    startupPanelEnabled(withStartupPanel(emptyState(), false)),
    false,
  );
  assert.equal(startupPanelEnabled(withStartupPanel(emptyState(), true)), true);
});

test("withStartupPanel: persists the preference without mutating or losing journeys", () => {
  const seeded = withJourney(emptyState(), "localdev", {
    status: "completed",
    stepIndex: 0,
  });
  const off = withStartupPanel(seeded, false);
  assert.equal(off.showStartupPanel, false);
  // Original untouched, journey records preserved.
  assert.equal(seeded.showStartupPanel, undefined);
  assert.equal(off.journeys.localdev.status, "completed");
  // Round-trips through storage.
  const s = fakeStorage();
  writeState(off, s);
  assert.equal(startupPanelEnabled(readState(s)), false);
});

// ── deep links ───────────────────────────────────────────────────────────────

test("readTourParam: reads the journey id from a query string or a full URL", () => {
  assert.equal(readTourParam("?tour=localdev"), "localdev");
  assert.equal(readTourParam("?a=1&tour=agentic-author&b=2"), "agentic-author");
  assert.equal(
    readTourParam("http://127.0.0.1:8080/console?tour=localdev"),
    "localdev",
  );
  assert.equal(
    readTourParam("?TOUR=x"),
    null,
    "the param name is case-sensitive",
  );
  assert.equal(
    readTourParam("?tour=LocalDev"),
    "localdev",
    "ids are lowercased",
  );
  assert.equal(readTourParam(""), null);
  assert.equal(readTourParam("?other=1"), null);
});

test("readTourParam: rejects ids that are not journey-id shaped", () => {
  // A malformed or hostile value is treated as absent rather than carried around.
  assert.equal(readTourParam("?tour=%3Cscript%3E"), null);
  assert.equal(readTourParam("?tour=has%20space"), null);
  assert.equal(readTourParam("?tour=-leading"), null);
  assert.equal(readTourParam("?tour="), null);
  assert.equal(readTourParam(`?tour=${"x".repeat(200)}`), null);
});

test("consumeDeepLinkTourParam: reads an injected search (module snapshot path is browser-only)", () => {
  // The default (no-arg) path snapshots window.location at module load, which a
  // Node test has no way to set; the injected-search overload is the seam that
  // keeps the parsing rule under test. It parses exactly like readTourParam.
  assert.equal(consumeDeepLinkTourParam("?tour=localdev"), "localdev");
  assert.equal(
    consumeDeepLinkTourParam(
      "http://127.0.0.1:8080/console?tour=agentic-author",
    ),
    "agentic-author",
  );
  assert.equal(consumeDeepLinkTourParam("?other=1"), null);
  assert.equal(consumeDeepLinkTourParam("?tour=%3Cscript%3E"), null);
});

test("stripTourParam: removes only the tour param and preserves the rest", () => {
  assert.equal(stripTourParam("/console?tour=localdev"), "/console");
  assert.equal(stripTourParam("/console?a=1&tour=x&b=2"), "/console?a=1&b=2");
  assert.equal(stripTourParam("/console?tour=x#frag"), "/console#frag");
  assert.equal(
    stripTourParam("http://127.0.0.1:8080/console?tour=x&a=1"),
    "http://127.0.0.1:8080/console?a=1",
  );
});

test("stripTourParam: returns the input unchanged when there is nothing to strip", () => {
  // Lets the caller skip a needless history write.
  const url = "/console?a=1";
  assert.equal(stripTourParam(url), url);
  assert.equal(stripTourParam("/projects"), "/projects");
});

// ── context ──────────────────────────────────────────────────────────────────

test("baseContext: a missing snapshot reads runtimes as ABSENT", () => {
  // Guessing "probably fine" is the defect ADR 0049 exists to fix: an unknown
  // runtime must repair the Run step, not promise it works.
  const c = baseContext({ profile: "studio", route: "/projects" });
  assert.equal(c.nodeAvailable, false);
  assert.deepEqual(c.projects, []);
  assert.deepEqual(c.templates, []);
  assert.deepEqual(c.extensions, []);
  assert.deepEqual(c.scratch, {});
});

test("baseContext: unwraps the listProjects snapshot, including nested extensions", () => {
  const c = baseContext({
    profile: "studio",
    route: "/projects",
    snapshot: {
      projects: [{ name: "p" } as never],
      nodeAvailable: false,
      templates: [{ id: "starter" } as never],
      extensions: { extensions: [{ id: "rust" } as never] },
    },
  });
  assert.equal(c.projects.length, 1);
  assert.equal(c.nodeAvailable, false);
  assert.equal(c.templates.length, 1);
  assert.equal(c.extensions.length, 1);
});

test("enrichContext: merges registered sources in order", async () => {
  clearContextSources();
  const un1 = registerContextSource(() => ({ nodeCount: 3 }));
  const un2 = registerContextSource(async () => ({ traceCount: 7 }));
  const c = await enrichContext(
    baseContext({ profile: "studio", route: "/x" }),
  );
  assert.equal(c.nodeCount, 3);
  assert.equal(c.traceCount, 7);
  un1();
  un2();
  const bare = await enrichContext(
    baseContext({ profile: "studio", route: "/x" }),
  );
  assert.equal(bare.nodeCount, undefined, "unsubscribe must remove the source");
  clearContextSources();
});

test("enrichContext: one failing source cannot take down the others", async () => {
  clearContextSources();
  registerContextSource(() => {
    throw new Error("boom");
  });
  registerContextSource(async () => {
    throw new Error("async boom");
  });
  registerContextSource(() => ({ nodeCount: 2 }));
  const c = await enrichContext(
    baseContext({ profile: "studio", route: "/x" }),
  );
  assert.equal(c.nodeCount, 2);
  clearContextSources();
});
