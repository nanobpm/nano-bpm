import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { BpmnViewer } from "@nanobpm/process-explorer";
import { Badge } from "@nanobpm/process-explorer/ui";
import {
  DEFINITION_PREVIEW_MAX_XML,
  DEFINITION_PREVIEW_STASH_KEY,
} from "../lib/appViewMessage";

/// Read-only preview of a BPMN document that is NOT deployed — no process
/// definition, no instance. The XML is handed in out-of-band via same-origin
/// sessionStorage (the definition-preview App-View bridge target stashes it,
/// then routes here) because a laid-out diagram is far larger than a URL budget.
///
/// This is how a staged delivery-graph proposal previews its generated Diagram
/// Interchange before dispatch: the compiler already emits full DI, so the
/// operator sees the exact laid-out model that a dispatch would run, rendered by
/// the SAME bpmn-js viewer the live process explorer uses — one DI surface, not
/// two.
function readStashedXml(): string | null {
  if (typeof window === "undefined") return null;
  try {
    const xml = window.sessionStorage.getItem(DEFINITION_PREVIEW_STASH_KEY);
    // Re-validate here, mirroring the bridge guard: the stash is normally
    // written by the (already validated) App-View bridge, but defending at the
    // read too keeps preview behaviour predictable — an invalid/oversized value
    // yields the empty state rather than a silently blank bpmn-js canvas.
    if (
      typeof xml === "string" &&
      xml.trim().startsWith("<") &&
      xml.length <= DEFINITION_PREVIEW_MAX_XML
    ) {
      return xml;
    }
    return null;
  } catch {
    return null;
  }
}

export default function DefinitionPreview() {
  // Read once on mount: the stash is a one-shot handoff, and re-reading on every
  // render would fight a later navigation that clears it.
  const [xml] = useState<string | null>(readStashedXml);
  // Set when bpmn-js can't import the stashed XML, so we show an explicit error
  // instead of a silently blank canvas.
  const [importFailed, setImportFailed] = useState(false);

  // Enforce the one-shot contract: once we've captured the XML into component
  // state, drop it from sessionStorage so it can't leak (a laid-out diagram is
  // large) or resurface as a STALE preview if the user later revisits
  // `/explorer?preview=1` without a fresh handoff. The captured `xml` still
  // renders — clearing storage doesn't disturb the state we already hold.
  useEffect(() => {
    try {
      window.sessionStorage.removeItem(DEFINITION_PREVIEW_STASH_KEY);
    } catch {
      // Storage unavailable — nothing to clear.
    }
  }, []);

  return (
    <div className="flex h-full flex-col">
      <header className="flex items-center justify-between border-b border-edge px-5 py-4">
        <div>
          <h1 className="flex items-center gap-2 text-xl font-semibold text-fg">
            Definition preview
            <Badge tone="info">not deployed</Badge>
          </h1>
          <p className="text-xs text-fg-faint">
            A read-only view of a compiled BPMN model that has not been deployed
            or dispatched — the generated diagram interchange exactly as a
            dispatch would run it.
          </p>
        </div>
        <Link to="/explorer" className="text-sm text-accent underline">
          ← Process instances
        </Link>
      </header>
      <div className="relative min-h-0 flex-1">
        {xml && !importFailed ? (
          <BpmnViewer
            xml={xml}
            onImportError={() => setImportFailed(true)}
            onImportSuccess={() => setImportFailed(false)}
          />
        ) : (
          <div className="flex h-full items-center justify-center px-6 text-center text-sm text-fg-faint">
            {importFailed ? (
              <>
                This model could not be rendered — the compiled BPMN failed to
                load. Re-stage the proposal and try again.
              </>
            ) : (
              <>
                No definition to preview. Open this from a staged delivery-graph
                proposal&rsquo;s{" "}
                <span className="mx-1 font-medium">Preview DI</span> action.
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
