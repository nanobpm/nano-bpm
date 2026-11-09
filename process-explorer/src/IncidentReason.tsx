import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import { copyText, selectElementText } from "./clipboard";
import {
  REASON_COLLAPSE_LINES,
  shouldCollapseReason,
} from "./incidentReasonText.ts";

// Renders an incident *reason* legibly. Worker/engine reasons are frequently a
// full multiline stderr dump (a failing `git clone`, an agent traceback), so we
// preserve whitespace and line breaks (`whitespace-pre-wrap`), collapse a long
// reason to a few lines with an expand toggle, and offer a copy button so an
// operator can paste the *whole* text into an issue. The collapse is purely
// visual — the full string is always kept for expand + copy, never truncated.

/** Clamp a collapsed reason to REASON_COLLAPSE_LINES lines. Derived from the
 * shared constant so the visual clamp and the "should collapse?" decision can
 * never drift apart. */
const clampStyle: CSSProperties = {
  display: "-webkit-box",
  WebkitLineClamp: REASON_COLLAPSE_LINES,
  WebkitBoxOrient: "vertical",
  overflow: "hidden",
};

export function IncidentReason({ reason }: { reason: string }) {
  // Memoize the collapse decision by `reason` so flipping local state (copied /
  // expanded) doesn't repeatedly rescan large multiline stderr dumps.
  const collapsible = useMemo(() => shouldCollapseReason(reason), [reason]);
  const [expanded, setExpanded] = useState(false);
  const [copied, setCopied] = useState(false);
  const preRef = useRef<HTMLPreElement>(null);
  const copiedTimer = useRef<number | undefined>(undefined);

  const clamped = collapsible && !expanded;

  useEffect(
    () => () => {
      if (copiedTimer.current !== undefined)
        window.clearTimeout(copiedTimer.current);
    },
    [],
  );

  useEffect(() => {
    // Reset per-reason UI state when the row updates to a new reason (the
    // parent refetches live), so a previously expanded / "Copied" state doesn't
    // leak across reasons — the new reason must render clamped with an honest
    // "Copy" label. Also clear any in-flight revert timer for the old reason.
    setExpanded(false);
    setCopied(false);
    if (copiedTimer.current !== undefined) {
      window.clearTimeout(copiedTimer.current);
      copiedTimer.current = undefined;
    }
  }, [reason]);

  async function onCopy() {
    const ok = await copyText(reason);
    if (!ok) {
      // Insecure context with no clipboard access — expand so the whole reason
      // is laid out, then select it so the operator can copy by hand (mirrors
      // Projects.tsx / tour runner). Defer selection to the next frame so the
      // expanded layout is applied before we select. Clear any stale "Copied"
      // state from a prior successful copy so the button doesn't lie.
      setCopied(false);
      if (copiedTimer.current !== undefined)
        window.clearTimeout(copiedTimer.current);
      setExpanded(true);
      requestAnimationFrame(() => {
        if (preRef.current) selectElementText(preRef.current);
      });
      return;
    }
    // Reset any in-flight revert timer so rapid clicks don't stack timeouts
    // (which could flip the label back to "Copy" while still showing success).
    if (copiedTimer.current !== undefined)
      window.clearTimeout(copiedTimer.current);
    setCopied(true);
    copiedTimer.current = window.setTimeout(() => setCopied(false), 1500);
  }

  return (
    <div className="flex min-w-0 flex-col items-start gap-1">
      <pre
        ref={preRef}
        // The full reason is always in the DOM (and the `title`) — only the
        // rendered height is clamped, so nothing is lost.
        className="min-w-0 max-w-full whitespace-pre-wrap break-words font-mono text-xs leading-relaxed text-danger"
        style={clamped ? clampStyle : undefined}
        title={reason}
      >
        {reason}
      </pre>
      <div className="flex items-center gap-3">
        {collapsible && (
          <button
            type="button"
            className="rounded text-[11px] font-medium text-fg-muted underline-offset-2 outline-none hover:text-fg hover:underline focus-visible:ring-2 focus-visible:ring-accent/60"
            aria-expanded={expanded}
            onClick={() => setExpanded((v) => !v)}
          >
            {expanded ? "Show less" : "Show more"}
          </button>
        )}
        <button
          type="button"
          className="rounded text-[11px] font-medium text-fg-muted underline-offset-2 outline-none hover:text-fg hover:underline focus-visible:ring-2 focus-visible:ring-accent/60"
          onClick={onCopy}
        >
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
    </div>
  );
}
