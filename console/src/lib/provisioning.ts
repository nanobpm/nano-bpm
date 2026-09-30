// Pure presentation helpers for the Workers view's job-type provisioning panel
// (issue #1294). Kept out of the TSX view so the class→badge mapping, the
// backlog-trend classification and the "which rows to show" predicate are
// unit-testable with the node-native runner (the view itself renders React and
// has no jsdom harness). The *classification* itself (starved / under-provisioned
// / server-bound) is the server's — computed once by the shared advisor and read
// over `/console/api/provisioning`; these helpers only turn that verdict into UI.

import type { ProvisioningClass, ProvisioningRecommendation } from "./api";

/// A coloured badge for a provisioning class: the human label plus the Tailwind
/// classes that tint it. `warming` (no history yet) has no badge — it is not an
/// actionable state, so the row shows only its rationale.
export interface ProvisioningBadge {
  label: string;
  className: string;
}

export const PROVISIONING_BADGE: Record<
  ProvisioningClass,
  ProvisioningBadge | null
> = {
  starved: {
    label: "Starved",
    className: "border-danger/40 bg-danger/10 text-danger",
  },
  "under-provisioned": {
    label: "Under-provisioned",
    className: "border-warn/40 bg-warn/10 text-warn",
  },
  "server-bound": {
    label: "Server-bound",
    className: "border-edge bg-raised text-fg-muted",
  },
  adequate: { label: "Adequate", className: "border-ok/40 bg-ok/10 text-ok" },
  warming: null,
};

/// The badge label with the suggested worker delta folded in, e.g.
/// `"Starved · +1 worker"` or `"Under-provisioned · +3 workers"`. Returns `null`
/// when the class carries no badge (`warming`). A zero (or negative) suggestion —
/// as `server-bound` and `adequate` always carry — appends nothing, so the panel
/// never advises scaling when it wouldn't help.
export function provisioningBadgeLabel(
  rec: Pick<ProvisioningRecommendation, "class" | "suggestWorkerDelta">,
): string | null {
  const badge = PROVISIONING_BADGE[rec.class];
  if (!badge) return null;
  if (rec.suggestWorkerDelta > 0) {
    const n = rec.suggestWorkerDelta;
    return `${badge.label} · +${n} worker${n === 1 ? "" : "s"}`;
  }
  return badge.label;
}

/// Backlog trend off the growth slope (jobs/s): growing (falling behind),
/// draining (catching up), or flat. A small dead-band keeps a near-zero slope
/// from flickering the arrow.
export type ProvisioningTrend = "growing" | "draining" | "flat";

const TREND_DEADBAND_PER_S = 0.05;

export function provisioningTrend(slopePerS: number): ProvisioningTrend {
  if (slopePerS > TREND_DEADBAND_PER_S) return "growing";
  if (slopePerS < -TREND_DEADBAND_PER_S) return "draining";
  return "flat";
}

/// A compact trend cell: an arrow + magnitude for growing/draining, an em dash
/// when flat. Magnitude is the absolute slope to one decimal.
export function provisioningTrendLabel(slopePerS: number): string {
  switch (provisioningTrend(slopePerS)) {
    case "growing":
      return `▲ ${slopePerS.toFixed(1)}`;
    case "draining":
      return `▼ ${Math.abs(slopePerS).toFixed(1)}`;
    default:
      return "—";
  }
}

/// Whether a job type has a live signal worth a row: jobs waiting, workers
/// connected, or an actionable class (starved / under-provisioned / server-bound).
/// Filters out `adequate`/`warming` idle types so an at-rest instance shows an
/// empty panel rather than a wall of zero rows.
export function hasProvisioningSignal(
  rec: Pick<ProvisioningRecommendation, "backlog" | "workers" | "class">,
): boolean {
  return (
    rec.backlog > 0 ||
    rec.workers > 0 ||
    rec.class === "starved" ||
    rec.class === "under-provisioned" ||
    rec.class === "server-bound"
  );
}
