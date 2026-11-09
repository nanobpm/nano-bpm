import { test } from "node:test";
import assert from "node:assert/strict";
import {
  decideAppViewMessage,
  DEFINITION_PREVIEW_MAX_XML,
  DEFINITION_PREVIEW_STASH_KEY,
} from "./appViewMessage.ts";
import {
  INSTANCE_DEEP_LINK_PARAM,
  readInstanceParam,
  explorerStackView,
} from "@nanobpm/process-explorer/filters";

test("nano-app-ready asks the host to reply with the theme", () => {
  assert.deepEqual(decideAppViewMessage({ type: "nano-app-ready" }), {
    kind: "theme",
  });
});

test("nano-navigate to processExplorer builds an in-console deep link", () => {
  assert.deepEqual(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: "2251799813685249" },
    }),
    { kind: "navigate", path: "/explorer?instance=2251799813685249" },
  );
});

test("the instance key is trimmed and URL-encoded (path built host-side, never a raw href)", () => {
  assert.deepEqual(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: "  a b/c?d=e  " },
    }),
    { kind: "navigate", path: "/explorer?instance=a+b%2Fc%3Fd%3De" },
  );
});

// A6 deep-link convergence lock: the embedded (`hostNavigate` → processExplorer)
// path and the standalone (`/console/explorer?instance=<key>`) landing must hit
// the ONE mobile instance-view target. The bridge builds its query from A3's
// canonical `INSTANCE_DEEP_LINK_PARAM`, and its output must be readable by the
// same `readInstanceParam` the Explorer landing uses — so producer and consumer
// can't silently drift onto different wire names. `explorerStackView` then
// confirms a read key resolves to the mobile *detail* pane, not the list.
test("A6: the embedded processExplorer path is consumable by the standalone Explorer landing", () => {
  for (const key of [
    "2251799813685249",
    "  a b/c?d=e  ",
    "weird=&key",
    "key with spaces",
  ]) {
    const action = decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: key },
    });
    assert.ok(action && action.kind === "navigate");

    // The bridge uses the one canonical param name (not a hardcoded synonym).
    const params = new URL(action.path, "http://console.invalid").searchParams;
    assert.ok(params.has(INSTANCE_DEEP_LINK_PARAM));

    // The standalone landing reader recovers exactly the trimmed instance the
    // app asked for, and that selection resolves to the mobile detail view.
    const landed = readInstanceParam(params);
    assert.equal(landed, key.trim());
    assert.equal(explorerStackView(landed), "detail");
  }
});

test("an unknown navigate target is ignored (whitelist, not passthrough)", () => {
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "somethingElse",
      params: { instance: "x" },
    }),
    null,
  );
});

test("a nano-navigate with a missing or blank instance is ignored", () => {
  assert.equal(
    decideAppViewMessage({ type: "nano-navigate", target: "processExplorer" }),
    null,
  );
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: "   " },
    }),
    null,
  );
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "processExplorer",
      params: { instance: 123 },
    }),
    null,
  );
});

test("unknown types and non-object payloads are ignored", () => {
  assert.equal(decideAppViewMessage({ type: "nano-theme" }), null);
  assert.equal(decideAppViewMessage(null), null);
  assert.equal(decideAppViewMessage("nano-app-ready"), null);
  assert.equal(decideAppViewMessage(42), null);
  assert.equal(decideAppViewMessage(undefined), null);
});

test("nano-navigate to definitionPreview stashes the XML and routes to the preview view", () => {
  const xml = "<bpmn:definitions><bpmndi:BPMNDiagram/></bpmn:definitions>";
  assert.deepEqual(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "definitionPreview",
      params: { xml },
    }),
    {
      kind: "navigate",
      path: "/explorer?preview=1",
      stash: { key: DEFINITION_PREVIEW_STASH_KEY, value: xml },
    },
  );
});

test("definitionPreview rejects non-XML, non-string, oversized, or missing payloads", () => {
  // not a string
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "definitionPreview",
      params: { xml: 123 },
    }),
    null,
  );
  // does not start with '<' (not a document — no path/scheme smuggling)
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "definitionPreview",
      params: { xml: "javascript:alert(1)" },
    }),
    null,
  );
  // missing params
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "definitionPreview",
    }),
    null,
  );
  // oversized
  assert.equal(
    decideAppViewMessage({
      type: "nano-navigate",
      target: "definitionPreview",
      params: { xml: "<" + "x".repeat(DEFINITION_PREVIEW_MAX_XML) },
    }),
    null,
  );
});
