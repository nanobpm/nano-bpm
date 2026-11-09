// Pure routing decision for postMessages the embedded Urban app sends to the
// console host (see AppView.tsx). The origin/source trust check stays in the
// component (it needs the live iframe ref); this decides only what an *already
// trusted* message means, so the console's node:test unit suite can cover the
// routing table directly without a renderer.

import { INSTANCE_DEEP_LINK_PARAM } from "@nanobpm/process-explorer/filters";

/// The sessionStorage key the host stashes a raw-XML definition preview under
/// before navigating to `/explorer?preview=1`. The XML is carried out-of-band
/// (in same-origin storage) rather than in the URL because a laid-out BPMN
/// document is far larger than any URL length budget. The DefinitionPreview
/// view reads it straight back.
export const DEFINITION_PREVIEW_STASH_KEY = "nano.explorer.definitionPreview";

/// A defensive cap on a previewed definition's XML size, in characters (UTF-16
/// code units — NOT bytes; a char can encode to up to 3-4 UTF-8 bytes). A
/// laid-out delivery graph is a few KB; anything past this is not a real diagram
/// and would risk overflowing the sessionStorage quota (typically ~5MB), so we
/// drop it. The write is also wrapped in try/catch as a backstop, so an
/// over-quota value degrades to the preview's empty state rather than throwing.
export const DEFINITION_PREVIEW_MAX_XML = 2_000_000;

export type AppViewMessageAction =
  | { kind: "theme" }
  | { kind: "navigate"; path: string }
  /// Navigate AND first write `stash.value` to sessionStorage under
  /// `stash.key`. Used to hand a raw BPMN document to the definition-preview
  /// view without putting it in the URL. The value is data rendered read-only
  /// by bpmn-js (which builds SVG from the parsed model and executes nothing),
  /// carried across the same trust boundary as the `instance` deep-link — the
  /// message is already origin/source-checked in AppView before we act on it.
  | { kind: "navigate"; path: string; stash: { key: string; value: string } };

/**
 * Interpret a trusted message posted by the embedded app. Returns the action
 * the host should take, or `null` when the message is unknown or malformed.
 *
 * - `nano-app-ready` → reply with the current theme.
 * - `nano-navigate` with a *known* target → navigate the console in-host. Only
 *   whitelisted targets are honoured, and the path is constructed HERE from
 *   structured params (never a raw href from the app), so app/row data can't
 *   smuggle a path or scheme across the frame boundary. The `instance` key is
 *   trimmed and URL-encoded, and the query param name is the single canonical
 *   {@link INSTANCE_DEEP_LINK_PARAM} the standalone Explorer landing reads — so
 *   the embedded (`hostNavigate`) and standalone
 *   (`/console/explorer?instance=<key>`) deep-link paths provably converge on
 *   the one mobile instance-view target instead of duplicating the wire name.
 */
export function decideAppViewMessage(
  data: unknown,
): AppViewMessageAction | null {
  if (typeof data !== "object" || data === null) return null;
  const msg = data as { type?: unknown; target?: unknown; params?: unknown };

  if (msg.type === "nano-app-ready") return { kind: "theme" };

  if (msg.type === "nano-navigate") {
    if (msg.target === "processExplorer") {
      const params = msg.params;
      const instance =
        typeof params === "object" && params !== null
          ? (params as { instance?: unknown }).instance
          : undefined;
      if (typeof instance === "string" && instance.trim() !== "") {
        const query = new URLSearchParams({
          [INSTANCE_DEEP_LINK_PARAM]: instance.trim(),
        });
        return {
          kind: "navigate",
          path: "/explorer?" + query.toString(),
        };
      }
    }
    // A staged delivery-graph proposal (or any not-yet-deployed model) previews
    // its generated DI here: the app hands the compiled BPMN XML, we stash it
    // same-origin and route to the read-only definition preview. No instance,
    // no deployed definition — the XML is rendered straight by bpmn-js.
    if (msg.target === "definitionPreview") {
      const params = msg.params;
      const xml =
        typeof params === "object" && params !== null
          ? (params as { xml?: unknown }).xml
          : undefined;
      if (
        typeof xml === "string" &&
        xml.trim().startsWith("<") &&
        xml.length <= DEFINITION_PREVIEW_MAX_XML
      ) {
        return {
          kind: "navigate",
          path: "/explorer?preview=1",
          stash: { key: DEFINITION_PREVIEW_STASH_KEY, value: xml },
        };
      }
    }
    // Unknown target or missing/blank instance → ignore (defensive default).
    return null;
  }

  return null;
}
