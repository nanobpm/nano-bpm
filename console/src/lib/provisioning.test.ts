// Unit tests for the job-type provisioning presentation helpers (issue #1294).
// Node-native: `node --experimental-strip-types --test src/lib/provisioning.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import type { ProvisioningRecommendation } from "./api.ts";
import {
  hasProvisioningSignal,
  PROVISIONING_BADGE,
  provisioningBadgeLabel,
  provisioningTrend,
  provisioningTrendLabel,
} from "./provisioning.ts";

function rec(
  over: Partial<ProvisioningRecommendation>,
): ProvisioningRecommendation {
  return {
    jobType: "pay",
    class: "adequate",
    confidence: "high",
    backlog: 0,
    backlogSlopePerS: 0,
    drainPerS: 0,
    workers: 0,
    suggestWorkerDelta: 0,
    rationale: "",
    ...over,
  };
}

test("starved renders a danger badge with the suggested worker count", () => {
  assert.deepEqual(PROVISIONING_BADGE.starved, {
    label: "Starved",
    className: "border-danger/40 bg-danger/10 text-danger",
  });
  const label = provisioningBadgeLabel(
    rec({ class: "starved", suggestWorkerDelta: 1 }),
  );
  assert.equal(label, "Starved · +1 worker");
});

test("under-provisioned folds a multi-worker suggestion into the badge", () => {
  const label = provisioningBadgeLabel(
    rec({ class: "under-provisioned", suggestWorkerDelta: 3 }),
  );
  assert.equal(label, "Under-provisioned · +3 workers");
  assert.equal(PROVISIONING_BADGE["under-provisioned"]?.label, "Under-provisioned");
});

test("server-bound shows its badge but never suggests scaling workers", () => {
  const label = provisioningBadgeLabel(
    rec({ class: "server-bound", suggestWorkerDelta: 0 }),
  );
  assert.equal(label, "Server-bound");
  assert.equal(PROVISIONING_BADGE["server-bound"]?.label, "Server-bound");
});

test("adequate shows an ok badge with no suffix", () => {
  assert.equal(provisioningBadgeLabel(rec({ class: "adequate" })), "Adequate");
});

test("warming has no badge — only its rationale is shown", () => {
  assert.equal(PROVISIONING_BADGE.warming, null);
  assert.equal(provisioningBadgeLabel(rec({ class: "warming" })), null);
});

test("trend classifies growth, drain and a flat dead-band", () => {
  assert.equal(provisioningTrend(5), "growing");
  assert.equal(provisioningTrend(-5), "draining");
  assert.equal(provisioningTrend(0), "flat");
  assert.equal(provisioningTrend(0.01), "flat", "near-zero stays flat");
});

test("trend label carries an arrow + magnitude, em dash when flat", () => {
  assert.equal(provisioningTrendLabel(12.3), "▲ 12.3");
  assert.equal(provisioningTrendLabel(-4), "▼ 4.0");
  assert.equal(provisioningTrendLabel(0), "—");
});

test("only job types with a live signal are shown", () => {
  // Waiting jobs, connected workers, or an actionable class each qualify.
  assert.equal(hasProvisioningSignal(rec({ backlog: 5 })), true);
  assert.equal(hasProvisioningSignal(rec({ workers: 2 })), true);
  assert.equal(hasProvisioningSignal(rec({ class: "starved" })), true);
  assert.equal(
    hasProvisioningSignal(rec({ class: "under-provisioned" })),
    true,
  );
  assert.equal(hasProvisioningSignal(rec({ class: "server-bound" })), true);
  // An idle adequate/warming type with no backlog or workers is filtered out.
  assert.equal(hasProvisioningSignal(rec({ class: "adequate" })), false);
  assert.equal(hasProvisioningSignal(rec({ class: "warming" })), false);
});
