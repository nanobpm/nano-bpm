// Unit tests for the pure geometry behind `usePaneResize` (the Explorer's
// resizable, reload-persistent process-list and model-space panes). Node-native:
// run with `node --experimental-strip-types --test src/lib/usePaneResize.test.ts`.
// The React/DOM/localStorage wiring is exercised in the browser; these cover the
// clamping, drag-delta, and keyboard math that decide the actual sizes.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  clampSize,
  sizeFromDelta,
  sizeFromKey,
  beginDragSession,
} from "./usePaneResize.ts";

function fakeTarget() {
  const listeners = new Map<string, Set<(ev: any) => void>>();
  return {
    addEventListener(type: string, fn: (ev: any) => void) {
      if (!listeners.has(type)) listeners.set(type, new Set());
      listeners.get(type)!.add(fn);
    },
    removeEventListener(type: string, fn: (ev: any) => void) {
      listeners.get(type)?.delete(fn);
    },
    count(type: string) {
      return listeners.get(type)?.size ?? 0;
    },
    fire(type: string, ev: any) {
      for (const fn of [...(listeners.get(type) ?? [])]) fn(ev);
    },
  };
}

function fakeBody() {
  const style: any = {
    cursor: "",
    userSelect: "",
    removeProperty(name: string) {
      const camel = name.replace(/-([a-z])/g, (_m: string, c: string) =>
        c.toUpperCase(),
      );
      style[camel] = "";
    },
  };
  return { style };
}

test("clampSize: holds a value within [min, max]", () => {
  assert.equal(clampSize(300, 256, 640), 300);
  assert.equal(clampSize(100, 256, 640), 256); // below min
  assert.equal(clampSize(900, 256, 640), 640); // above max
  assert.equal(clampSize(256, 256, 640), 256); // on min
  assert.equal(clampSize(640, 256, 640), 640); // on max
});

test("clampSize: an inverted range (viewport smaller than min) collapses to min", () => {
  // e.g. a very narrow window where `max = innerWidth - 480` falls below `min`.
  assert.equal(clampSize(400, 256, 100), 256);
});

test("sizeFromDelta: a trailing-edge handle grows with a positive delta", () => {
  // Explorer list: handle on the list's right edge, dragging right widens it.
  assert.equal(sizeFromDelta(448, 60, false, 256, 640), 508);
  assert.equal(sizeFromDelta(448, -60, false, 256, 640), 388);
});

test("sizeFromDelta: an inverted (leading-edge) handle reverses the delta", () => {
  assert.equal(sizeFromDelta(448, 60, true, 256, 640), 388);
  assert.equal(sizeFromDelta(448, -60, true, 256, 640), 508);
});

test("sizeFromDelta: the result is clamped to the bounds", () => {
  assert.equal(sizeFromDelta(620, 100, false, 256, 640), 640); // hits max
  assert.equal(sizeFromDelta(280, -100, false, 256, 640), 256); // hits min
});

test("sizeFromKey (x axis): ArrowRight grows, ArrowLeft shrinks, by one step", () => {
  assert.equal(sizeFromKey(448, "ArrowRight", "x", 16, 256, 640), 464);
  assert.equal(sizeFromKey(448, "ArrowLeft", "x", 16, 256, 640), 432);
});

test("sizeFromKey (y axis): ArrowDown grows the model space, ArrowUp shrinks it", () => {
  assert.equal(sizeFromKey(288, "ArrowDown", "y", 16, 144, 640), 304);
  assert.equal(sizeFromKey(288, "ArrowUp", "y", 16, 144, 640), 272);
});

test("sizeFromKey: an inverted (leading-edge) handle reverses the arrow mapping, matching sizeFromDelta", () => {
  // On an inverted handle the arrow that physically moves the separator toward
  // the pane grows it, so the grow/shrink keys swap relative to the default.
  assert.equal(sizeFromKey(448, "ArrowRight", "x", 16, 256, 640, true), 432);
  assert.equal(sizeFromKey(448, "ArrowLeft", "x", 16, 256, 640, true), 464);
  assert.equal(sizeFromKey(288, "ArrowDown", "y", 16, 144, 640, true), 272);
  assert.equal(sizeFromKey(288, "ArrowUp", "y", 16, 144, 640, true), 304);
});

test("sizeFromKey: an off-axis or non-arrow key is ignored (returns null)", () => {
  // Vertical handle ignores horizontal arrows and vice-versa.
  assert.equal(sizeFromKey(288, "ArrowLeft", "y", 16, 144, 640), null);
  assert.equal(sizeFromKey(448, "ArrowUp", "x", 16, 256, 640), null);
  assert.equal(sizeFromKey(448, "Enter", "x", 16, 256, 640), null);
});

test("sizeFromKey: a nudge past a bound is clamped", () => {
  assert.equal(sizeFromKey(636, "ArrowRight", "x", 16, 256, 640), 640);
  assert.equal(sizeFromKey(260, "ArrowLeft", "x", 16, 256, 640), 256);
});

test("beginDragSession: registers move + end listeners and applies the drag body styles", () => {
  const target = fakeTarget();
  const body = fakeBody();
  beginDragSession(
    target as any,
    body as any,
    "x",
    () => {},
    () => {},
  );
  assert.equal(target.count("pointermove"), 1);
  assert.equal(target.count("pointerup"), 1);
  assert.equal(target.count("pointercancel"), 1);
  assert.equal(body.style.cursor, "col-resize");
  assert.equal(body.style.userSelect, "none");
});

test("beginDragSession: stop() tears down without a pointer event (the unmount-mid-drag path)", () => {
  const target = fakeTarget();
  const body = fakeBody();
  let ended = 0;
  const stop = beginDragSession(
    target as any,
    body as any,
    "y",
    () => {},
    () => {
      ended++;
    },
  );
  // Simulate an unmount mid-drag: no pointerup/pointercancel fires, we call stop directly.
  stop();
  assert.equal(target.count("pointermove"), 0);
  assert.equal(target.count("pointerup"), 0);
  assert.equal(target.count("pointercancel"), 0);
  assert.equal(body.style.cursor, "");
  assert.equal(body.style.userSelect, "");
  // A bare stop() is teardown only — it must not fire the onEnd (state) callback.
  assert.equal(ended, 0);
  // Idempotent: calling again is a no-op and never fires onEnd.
  stop();
  assert.equal(ended, 0);
});

test("beginDragSession: a pointerup fires onEnd and tears everything down", () => {
  const target = fakeTarget();
  const body = fakeBody();
  let ended = 0;
  beginDragSession(
    target as any,
    body as any,
    "x",
    () => {},
    () => {
      ended++;
    },
  );
  target.fire("pointerup", {});
  assert.equal(ended, 1);
  assert.equal(target.count("pointermove"), 0);
  assert.equal(body.style.cursor, "");
  assert.equal(body.style.userSelect, "");
});

test("beginDragSession: onMove receives the axis-projected coordinate", () => {
  const xt = fakeTarget();
  let xc = -1;
  beginDragSession(
    xt as any,
    fakeBody() as any,
    "x",
    (c) => (xc = c),
    () => {},
  );
  xt.fire("pointermove", { clientX: 123, clientY: 456 });
  assert.equal(xc, 123);

  const yt = fakeTarget();
  let yc = -1;
  beginDragSession(
    yt as any,
    fakeBody() as any,
    "y",
    (c) => (yc = c),
    () => {},
  );
  yt.fire("pointermove", { clientX: 123, clientY: 456 });
  assert.equal(yc, 456);
});
