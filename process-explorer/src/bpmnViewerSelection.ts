// Pure selection-resolution helper for BpmnViewer (see BpmnViewer.tsx).
//
// The viewer wires bpmn-js's `element.click` event to an optional
// `onElementSelect` callback. The decision of *whether* a click should fire the
// callback — and with *which* BPMN id — is a side-effect-free function of the
// clicked diagram-js element, so it lives here where the console's node:test
// unit suite can cover it directly (the viewer itself only runs in a browser).

/** The subset of a diagram-js element shape this resolution depends on. */
export interface SelectableElement {
  id?: string;
  type?: string;
  /// A label element (e.g. a flow/task caption) carries a reference back to the
  /// element it labels; a click on the label should resolve to that element.
  labelTarget?: { id?: string; type?: string } | null;
}

/// diagram-js root shapes (the canvas plane / process / collaboration). A click
/// on empty canvas fires `element.click` with the current root, which is not a
/// real selectable element — ignore it.
const ROOT_TYPES = new Set([
  "bpmn:Process",
  "bpmn:Collaboration",
  "bpmn:Definitions",
]);

/**
 * Resolve the BPMN element id a click should select, or `null` when the click
 * should be ignored (empty canvas / diagram root, or an element with no id).
 * A click on a label resolves to the element the label belongs to.
 */
export function selectedElementId(
  element: SelectableElement | null | undefined,
): string | null {
  if (!element) return null;
  // Clicking a label selects the labelled element, not the label shape.
  const target = element.labelTarget ?? element;
  const id = target.id;
  if (!id) return null;
  if (target.type && ROOT_TYPES.has(target.type)) return null;
  return id;
}
