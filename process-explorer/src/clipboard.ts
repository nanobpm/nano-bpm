/**
 * Copy to the clipboard, falling back when the async API is unavailable.
 *
 * `navigator.clipboard` requires a secure context, and this console is routinely
 * served over plain HTTP on a LAN address or a tunnel — exactly where copying a
 * handoff command or an agent URL matters most. Returns false when nothing could
 * be copied, so the caller can fall back to selecting the text for the user.
 *
 * Kept in its own dependency-free module (no `driver.js`/tour CSS) so a plain
 * view can copy a string without dragging the tour runner's bundle in with it.
 */
export async function copyText(text: string): Promise<boolean> {
  try {
    // The DOM lib types `navigator.clipboard` as always present, but it is
    // genuinely absent outside a secure context — so probe it at runtime via
    // `typeof` rather than a truthiness check TypeScript would call redundant.
    const clipboard = globalThis.navigator?.clipboard;
    if (clipboard && typeof clipboard.writeText === "function") {
      await clipboard.writeText(text);
      return true;
    }
  } catch {
    // Fall through to the legacy path.
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}

/**
 * Select an element's text, so the user can copy it manually when
 * {@link copyText} could not (an insecure context with no `execCommand`). A
 * best-effort visual affordance — silently a no-op where the Selection API is
 * unavailable.
 */
export function selectElementText(node: HTMLElement): void {
  try {
    const sel = globalThis.getSelection?.();
    if (!sel) return;
    const range = document.createRange();
    range.selectNodeContents(node);
    sel.removeAllRanges();
    sel.addRange(range);
  } catch {
    // No Selection API (or a detached node) — nothing more we can do.
  }
}
