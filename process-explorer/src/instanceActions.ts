// Pure, React-free helpers for the Process Explorer instance-detail operator
// actions, extracted so they can be unit-tested with `node --test` (see
// instanceActions.test.ts) without a DOM.

// The instance `state` string the console API emits is the Debug form of the
// engine's ProcessInstanceState: "Active", "Completed", "Terminated", or the
// transient "Terminating". Only a running (Active) instance can be cancelled —
// a completed/terminated one has no tokens left to discard, and a Terminating
// one is already being cancelled.
export function isCancellable(state: string): boolean {
  return state === "Active";
}

// Suspend pauses a running instance (Active -> Suspended); only a running
// instance can be suspended (a terminal/transient one has nothing to pause).
// Mirrors the engine's ACTIVE<->SUSPENDED live-transition rule so the button is
// never offered when the engine would reject it with a 400/404.
export function isSuspendable(state: string): boolean {
  return state === "Active";
}

// Resume reverses a suspension (Suspended -> Active); only a Suspended instance
// can be resumed.
export function isResumable(state: string): boolean {
  return state === "Suspended";
}

// Confirmation copy for the (destructive, irreversible) cancel action. Kept
// here so the exact wording is asserted by a test rather than buried in JSX.
// The instance key is included because a processId is NOT unique across
// instances — the operator must be able to see exactly which run they are
// terminating before confirming.
export function cancelConfirmMessage(
  processId: string,
  instanceKey: string,
): string {
  return `Cancel process instance "${processId}" (${instanceKey})? This discards all its tokens (pending jobs, timers, message subscriptions) and terminates it. This cannot be undone.`;
}
