/**
 * Pure display helpers for rendering an incident *reason* in the Process
 * Explorer (see {@link ../components/IncidentReason}).
 *
 * An incident reason is whatever the worker/engine emitted — often a full,
 * multiline stderr dump (a failing `git clone`, an agent-harness traceback).
 * The console must render that text **faithfully**: preserved line breaks, no
 * lossy truncation. Any "collapsing" is purely *visual* (a CSS line clamp with
 * an expand toggle); the full string is always kept for expand + copy.
 *
 * The decision of *whether* to offer the expand/collapse affordance is the one
 * bit of logic worth testing in isolation, so it lives here — dependency-free —
 * rather than being tangled into the component.
 */

/** How many lines a collapsed reason shows before the "Show more" toggle. */
export const REASON_COLLAPSE_LINES = 3;

/**
 * Beyond this many characters a *single-line* reason is long enough to be worth
 * clamping even though it has no newline to count. (A run-on line still wraps to
 * several visual rows.)
 */
export const REASON_COLLAPSE_CHARS = 240;

/** Count the newline-delimited lines in a string (an empty string is 0 lines). */
export function countReasonLines(reason: string): number {
  if (reason.length === 0) return 0;
  // Scan for newlines rather than `split("\n")`: an incident reason can be a
  // large stderr dump, and we only need the count — no need to allocate an
  // array proportional to the number of lines.
  let lines = 1;
  for (
    let i = reason.indexOf("\n");
    i !== -1;
    i = reason.indexOf("\n", i + 1)
  ) {
    lines++;
  }
  return lines;
}

/** True when the reason spans more than one physical line. */
export function isMultilineReason(reason: string): boolean {
  return reason.includes("\n");
}

/**
 * Whether the reason is big enough that we should collapse it by default and
 * offer an expand toggle. It is long if it has more lines than the clamp shows,
 * or if a single run-on line exceeds the character bound. This is a *display*
 * decision only — it never mutates or truncates the underlying text.
 */
export function shouldCollapseReason(
  reason: string,
  lines: number = REASON_COLLAPSE_LINES,
  chars: number = REASON_COLLAPSE_CHARS,
): boolean {
  // Check the cheap character bound first: it short-circuits a long run-on
  // single-line reason without scanning the whole string for newlines.
  return reason.length > chars || countReasonLines(reason) > lines;
}
