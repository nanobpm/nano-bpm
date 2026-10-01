import { useEffect, useRef } from "react";
import NavigatedViewer from "bpmn-js/lib/NavigatedViewer";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";
import {
  selectedElementId,
  type SelectableElement,
} from "./bpmnViewerSelection.ts";

interface Canvas {
  /// diagram-js canvas zoom: no arg reads the current scale, a number sets it
  /// (optionally about a `center` point in container-pixel coordinates), and
  /// the string `"fit-viewport"` fits the whole diagram.
  zoom(newScale?: number | string, center?: { x: number; y: number }): number;
  /// Pans the viewport by a pixel delta (used for one-finger touch panning).
  scroll(delta: { dx: number; dy: number }): void;
  addMarker(elementId: string, marker: string): void;
  removeMarker(elementId: string, marker: string): void;
}

/// diagram-js's overlay service: HTML pinned to a diagram element, moving and
/// scaling with it.
interface Overlays {
  add(
    elementId: string,
    type: string,
    overlay: {
      position: {
        top?: number;
        right?: number;
        bottom?: number;
        left?: number;
      };
      html: HTMLElement;
    },
  ): string;
  remove(filter: { type: string }): void;
}

/// A small pill drawn on a flow element's top-right corner (e.g. an agent
/// session: status and turn count). Clicking it calls `onBadgeClick`.
export interface ElementBadge {
  elementId: string;
  label: string;
  /// Accessible name and tooltip.
  title: string;
  /// `active` pulses (work still arriving); `settled` is static.
  tone: "active" | "settled";
}

const BADGE_OVERLAY = "nano-badge";

/// diagram-js's event bus. We only subscribe to `element.click` to surface a
/// selection to the caller, so this is deliberately the minimal shape.
interface EventBus {
  on(
    event: string,
    callback: (event: { element?: SelectableElement }) => void,
  ): void;
  off(
    event: string,
    callback: (event: { element?: SelectableElement }) => void,
  ): void;
}

// Pinch-zoom clamp. Matches diagram-js's own zoom range so a pinch can never
// shrink the diagram to an unreadable speck (which would also make the
// `.nano-active` / `.nano-incident` overlays illegible) nor blow it up past use.
const MIN_ZOOM = 0.2;
const MAX_ZOOM = 4;

function touchDistance(touches: TouchList): number {
  return Math.hypot(
    touches[0].clientX - touches[1].clientX,
    touches[0].clientY - touches[1].clientY,
  );
}

function touchMidpoint(touches: TouchList): { x: number; y: number } {
  return {
    x: (touches[0].clientX + touches[1].clientX) / 2,
    y: (touches[0].clientY + touches[1].clientY) / 2,
  };
}

interface BpmnViewerProps {
  xml: string | null;
  /// Element ids to highlight as carrying an active token (e.g. pending jobs).
  activeElementIds?: string[];
  /// Element ids to highlight as having an incident.
  incidentElementIds?: string[];
  /// Re-fit the diagram to the viewport whenever the container is resized (e.g.
  /// a phone rotating, or the diagram opening into a full-screen mobile card).
  /// Off by default so the resizable desktop model pane keeps a manual zoom.
  fitOnResize?: boolean;
  /// Called when bpmn-js fails to import the supplied XML (malformed /
  /// unsupported), so a caller can surface an explicit error instead of the
  /// otherwise-blank canvas. Optional; omitting it preserves the prior
  /// silently-blank behaviour.
  onImportError?: (err: unknown) => void;
  /// Called after the XML imports cleanly (pairs with onImportError so a caller
  /// can clear a previous error when a later, valid document loads).
  onImportSuccess?: () => void;
  /// Called with a clicked element's BPMN id when the operator selects an
  /// element on the diagram. Clicks on empty canvas / the diagram root are
  /// ignored (never fired). Optional; omitting it leaves the viewer's existing
  /// read-only behaviour completely unchanged.
  onElementSelect?: (elementId: string) => void;
  /// Pills overlaid on flow elements (agent sessions, #1314).
  badges?: ElementBadge[];
  /// Called with the element id when its badge is clicked.
  onBadgeClick?: (elementId: string) => void;
}

/// Renders a deployed BPMN definition with diagram-js (read-only), overlaying
/// markers on the elements that currently hold work or an incident. The XML is
/// fetched from the gateway's getProcessDefinitionXML endpoint by the caller.
export default function BpmnViewer({
  xml,
  activeElementIds = [],
  incidentElementIds = [],
  fitOnResize = false,
  onImportError,
  onImportSuccess,
  onElementSelect,
  badges = [],
  onBadgeClick,
}: BpmnViewerProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewerRef = useRef<NavigatedViewer | null>(null);
  // Keep the latest import callbacks in refs so the import effect can call them
  // without listing them in its deps (a new inline callback identity each render
  // must not re-fire the effect / re-import).
  const onImportErrorRef = useRef(onImportError);
  onImportErrorRef.current = onImportError;
  const onImportSuccessRef = useRef(onImportSuccess);
  onImportSuccessRef.current = onImportSuccess;
  // Same rationale for the element-select callback: keep the latest in a ref so
  // the (deps-`[]`) mount effect's eventBus listener always calls the current
  // callback without re-subscribing on every render.
  const onElementSelectRef = useRef(onElementSelect);
  onElementSelectRef.current = onElementSelect;
  const onBadgeClickRef = useRef(onBadgeClick);
  onBadgeClickRef.current = onBadgeClick;
  // Set once the viewer has been destroyed, so async work already in flight (an
  // importXML, a marker pass) doesn't touch a dead instance.
  const disposedRef = useRef(false);
  // Serializes every viewer op on a single chain. Overlapping importXML calls
  // corrupt diagram-js's internal state — it then throws from deep inside a
  // render ("Cannot read properties of undefined (reading 'root-0')"). This is
  // the same race the modeler hit and was fixed the same way (`runLoad`, commit
  // 999ad23): InstanceDetail re-renders on every live SSE tick with freshly
  // derived activeElementIds/incidentElementIds arrays, so the effect below
  // re-fires constantly and, unserialized, those imports race each other.
  const opChainRef = useRef<Promise<unknown>>(Promise.resolve());
  // The XML currently imported into the live viewer, so a marker-only update
  // (the common live-tick case) skips a full, flashing re-import. Reset whenever
  // the viewer is (re)created.
  const importedXmlRef = useRef<string | null>(null);
  // Markers currently applied, so a marker pass can clear the previous set
  // before adding the new one (a re-import wipes them, a marker-only pass does
  // not).
  const markedActiveRef = useRef<string[]>([]);
  const markedIncidentRef = useRef<string[]>([]);

  useEffect(() => {
    if (!containerRef.current) return;
    disposedRef.current = false;
    const container = containerRef.current;
    const viewer = new NavigatedViewer({ container });
    viewerRef.current = viewer;
    importedXmlRef.current = null;
    markedActiveRef.current = [];
    markedIncidentRef.current = [];

    // NavigatedViewer ships mouse/keyboard/wheel navigation only — this
    // diagram-js has no touch module — so a phone gets neither pan nor
    // pinch-zoom out of the box. Implement both against the canvas API: one
    // finger pans (canvas.scroll, mirroring MoveCanvas), two fingers pinch-zoom
    // about their midpoint (canvas.zoom). The container carries
    // `touch-action: none` so the browser doesn't hijack the gesture as page
    // scroll/zoom.
    const canvas = () => viewer.get<Canvas>("canvas");
    let lastPan: { x: number; y: number } | null = null;
    let pinch: { dist: number; zoom: number } | null = null;

    const onTouchStart = (e: TouchEvent) => {
      if (disposedRef.current) return;
      if (e.touches.length === 1) {
        lastPan = { x: e.touches[0].clientX, y: e.touches[0].clientY };
        pinch = null;
      } else if (e.touches.length === 2) {
        lastPan = null;
        pinch = { dist: touchDistance(e.touches), zoom: canvas().zoom() };
      }
    };
    const onTouchMove = (e: TouchEvent) => {
      if (disposedRef.current) return;
      if (e.touches.length === 1 && lastPan) {
        const t = e.touches[0];
        canvas().scroll({
          dx: t.clientX - lastPan.x,
          dy: t.clientY - lastPan.y,
        });
        lastPan = { x: t.clientX, y: t.clientY };
        e.preventDefault();
      } else if (e.touches.length === 2 && pinch && pinch.dist > 0) {
        const dist = touchDistance(e.touches);
        const rect = container.getBoundingClientRect();
        const mid = touchMidpoint(e.touches);
        const next = Math.min(
          MAX_ZOOM,
          Math.max(MIN_ZOOM, (pinch.zoom * dist) / pinch.dist),
        );
        canvas().zoom(next, { x: mid.x - rect.left, y: mid.y - rect.top });
        e.preventDefault();
      }
    };
    const onTouchEnd = (e: TouchEvent) => {
      if (e.touches.length >= 2) return;
      pinch = null;
      // Continue panning from a remaining finger after a pinch releases one.
      lastPan =
        e.touches.length === 1
          ? { x: e.touches[0].clientX, y: e.touches[0].clientY }
          : null;
    };

    container.addEventListener("touchstart", onTouchStart, { passive: true });
    container.addEventListener("touchmove", onTouchMove, { passive: false });
    container.addEventListener("touchend", onTouchEnd, { passive: true });
    container.addEventListener("touchcancel", onTouchEnd, { passive: true });

    // Surface element selection to the caller. Clicks on empty canvas / the
    // diagram root resolve to `null` and are ignored (see bpmnViewerSelection).
    const eventBus = viewer.get<EventBus>("eventBus");
    const onElementClick = (event: { element?: SelectableElement }) => {
      if (disposedRef.current) return;
      const id = selectedElementId(event.element);
      if (id) onElementSelectRef.current?.(id);
    };
    eventBus.on("element.click", onElementClick);

    return () => {
      disposedRef.current = true;
      eventBus.off("element.click", onElementClick);
      container.removeEventListener("touchstart", onTouchStart);
      container.removeEventListener("touchmove", onTouchMove);
      container.removeEventListener("touchend", onTouchEnd);
      container.removeEventListener("touchcancel", onTouchEnd);
      viewer.destroy();
      viewerRef.current = null;
    };
  }, []);

  // Stable dep keys: the parent hands new array identities every render, so
  // depend on their content, not their reference, to avoid needless re-runs.
  // Sort a copy first so the key is stable for the same *set* of ids even if
  // the backend reorders jobs/incidents between SSE ticks.
  const activeKey = [...activeElementIds].sort().join(",");
  const incidentKey = [...incidentElementIds].sort().join(",");
  const badgeKey = badges
    .map((b) => `${b.elementId}\u0000${b.label}\u0000${b.title}\u0000${b.tone}`)
    .sort()
    .join("\u0001");

  useEffect(() => {
    const viewer = viewerRef.current;
    if (!viewer || !xml) return;
    const op = opChainRef.current.then(async () => {
      if (disposedRef.current || viewerRef.current !== viewer) return;
      // Only (re)import when the document itself changed; a live tick that only
      // moves the active/incident markers reuses the imported diagram.
      if (importedXmlRef.current !== xml) {
        try {
          await viewer.importXML(xml);
        } catch (err) {
          // malformed/unsupported XML — leave the canvas blank and let the
          // caller surface an explicit error (default: silently blank).
          if (!disposedRef.current && viewerRef.current === viewer) {
            onImportErrorRef.current?.(err);
          }
          return;
        }
        if (disposedRef.current || viewerRef.current !== viewer) return;
        importedXmlRef.current = xml;
        onImportSuccessRef.current?.();
        // A fresh import clears every marker, so drop our bookkeeping too.
        markedActiveRef.current = [];
        markedIncidentRef.current = [];
        viewer.get<Canvas>("canvas").zoom("fit-viewport");
      }
      const canvas = viewer.get<Canvas>("canvas");
      for (const id of markedActiveRef.current) {
        try {
          canvas.removeMarker(id, "nano-active");
        } catch {
          /* element may not exist in this version's diagram */
        }
      }
      for (const id of markedIncidentRef.current) {
        try {
          canvas.removeMarker(id, "nano-incident");
        } catch {
          /* ignore unknown element */
        }
      }
      for (const id of activeElementIds) {
        try {
          canvas.addMarker(id, "nano-active");
        } catch {
          /* element may not exist in this version's diagram */
        }
      }
      for (const id of incidentElementIds) {
        try {
          canvas.addMarker(id, "nano-incident");
        } catch {
          /* ignore unknown element */
        }
      }
      markedActiveRef.current = activeElementIds;
      markedIncidentRef.current = incidentElementIds;

      const overlays = viewer.get<Overlays>("overlays");
      overlays.remove({ type: BADGE_OVERLAY });
      for (const badge of badges) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = `nano-badge nano-badge-${badge.tone}`;
        button.textContent = badge.label;
        button.title = badge.title;
        button.setAttribute("aria-label", badge.title);
        button.dataset.elementId = badge.elementId;
        // Stop the click reaching diagram-js, which would also fire
        // `element.click` (the badge is a child of the element's overlay).
        button.addEventListener("click", (e) => {
          e.stopPropagation();
          onBadgeClickRef.current?.(badge.elementId);
        });
        try {
          overlays.add(badge.elementId, BADGE_OVERLAY, {
            position: { top: -10, right: 12 },
            html: button,
          });
        } catch {
          /* element may not exist in this version's diagram */
        }
      }
    });
    // Keep the chain alive even when this op fails, so one bad import doesn't
    // wedge every later load.
    opChainRef.current = op.catch(() => {});
  }, [xml, activeKey, incidentKey, badgeKey]);

  // On mobile the diagram opens into a full-screen card and the phone can
  // rotate; re-fit to the viewport whenever the container resizes so the whole
  // model stays framed. Serialized on the same op chain as imports so a re-fit
  // can't race an in-flight importXML. Opt-in (the resizable desktop pane keeps
  // its manual zoom across a drag).
  useEffect(() => {
    if (!fitOnResize || typeof ResizeObserver === "undefined") return;
    const container = containerRef.current;
    if (!container) return;
    const ro = new ResizeObserver(() => {
      const viewer = viewerRef.current;
      if (!viewer || disposedRef.current || importedXmlRef.current === null)
        return;
      opChainRef.current = opChainRef.current
        .then(() => {
          if (disposedRef.current || viewerRef.current !== viewer) return;
          viewer.get<Canvas>("canvas").zoom("fit-viewport");
        })
        .catch(() => {});
    });
    ro.observe(container);
    return () => ro.disconnect();
  }, [fitOnResize]);

  // Render a single, stable structure so the container ref points at the same
  // DOM node for the component's whole life (the viewer is created against it
  // on mount, before the async XML query resolves). The empty-state message is
  // an overlay shown until the XML arrives — never an alternate tree that would
  // leave the ref unmounted and prevent the viewer from being created.
  return (
    <div className="relative h-full w-full">
      <div ref={containerRef} className="h-full w-full touch-none" />
      {!xml && (
        <div className="absolute inset-0 flex items-center justify-center text-sm text-fg-faint">
          No diagram available for this definition.
        </div>
      )}
    </div>
  );
}
