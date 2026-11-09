import type { ReactNode } from "react";
import { Card } from "@nanobpm/process-explorer/ui";
import type { Journey } from "../lib/tour/types";

/**
 * The cold-arrival journey picker (#411, ADR 0049 §2): on a home view's empty
 * state, the empty state *is* the picker. One card per outcome-shaped journey
 * offerable here (derived from the registry, never hardcoded), plus a quiet
 * "just show me around" link for the demoted overview.
 *
 * Deliberately NOT modal and NOT focus-trapping — the normal way into a journey
 * is a `?tour=` deep link, so self-selection is only the cold-arrival fallback
 * and must cost no friction. Each card is a native `<button>`, so keyboard focus
 * reaches it, Enter/Space activate it, and nothing traps Escape.
 *
 * Matches the New Project template gallery's visual language (same grid, same
 * `Card`) so it reads as part of the product rather than an onboarding bolt-on.
 */
export default function JourneyPicker({
  title,
  subtitle,
  journeys,
  onPick,
  onOverview,
  overviewLabel = "Just show me around",
  fallback,
}: {
  title?: ReactNode;
  subtitle?: ReactNode;
  journeys: Journey[];
  onPick: (journeyId: string) => void;
  /** Runs the profile's overview — the quiet fallback link. Omit to hide it. */
  onOverview?: () => void;
  overviewLabel?: string;
  /** Rendered when there is nothing to offer (no cards and no overview link),
   * so a host that has no journeys yet keeps its plain empty state. */
  fallback?: ReactNode;
}) {
  if (journeys.length === 0 && !onOverview) return <>{fallback ?? null}</>;

  return (
    <div>
      {title && <h2 className="text-lg font-semibold text-fg">{title}</h2>}
      {subtitle && journeys.length > 0 && (
        <p className="mt-1 text-sm text-fg-faint">{subtitle}</p>
      )}

      {journeys.length > 0 && (
        <div className="mt-5 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {journeys.map((journey) => (
            <button
              key={journey.id}
              type="button"
              onClick={() => onPick(journey.id)}
              className="block w-full rounded-xl text-left outline-none focus-visible:ring-2 focus-visible:ring-accent"
            >
              <Card className="flex h-full flex-col p-4 transition-colors hover:border-accent hover:ring-1 hover:ring-accent">
                <span className="text-base font-semibold text-fg">
                  {journey.title}
                </span>
                <p className="mt-1 flex-1 text-sm text-fg-faint">
                  {journey.blurb}
                </p>
                <span className="mt-3 text-xs font-medium text-accent-strong">
                  Start →
                </span>
              </Card>
            </button>
          ))}
        </div>
      )}

      {onOverview && (
        <button
          type="button"
          onClick={onOverview}
          className="mt-5 rounded text-sm text-fg-faint underline decoration-dotted underline-offset-4 outline-none transition-colors hover:text-fg focus-visible:ring-2 focus-visible:ring-accent"
        >
          {overviewLabel} →
        </button>
      )}
    </div>
  );
}
