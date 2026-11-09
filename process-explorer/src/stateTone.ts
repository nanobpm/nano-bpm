/**
 * Badge tone for an instance's lifecycle state. Single source of truth shared by
 * the Explorer list and the Instance Detail "Called Process Instances" section so
 * the two never drift as states / tone rules evolve. An incident always wins
 * (`danger`), regardless of `state`.
 */
export function stateTone(
  state: string,
  hasIncident: boolean,
): "danger" | "info" | "ok" | "neutral" | "warn" {
  if (hasIncident) return "danger";
  switch (state) {
    case "Active":
      return "info";
    case "Suspended":
      // Paused, not terminal — a distinct amber "held" tone so a suspended
      // instance reads apart from a running (info) or terminated (neutral) one.
      return "warn";
    case "Completed":
      return "ok";
    case "Terminated":
      return "neutral";
    default:
      return "neutral";
  }
}
