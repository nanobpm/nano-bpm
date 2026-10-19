import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { renderToStaticMarkup } from "react-dom/server";
import { ConfigModal } from "./ConfigModal.tsx";
import type { ProjectConfig } from "../gen";

// Issue #1350: the configuration-save gate. While the app runs, the Configure
// modal must render Save disabled with the stop-first notice — and the gate
// must track `appRunning` exactly (the server owns the 409; the client mirrors
// it so the write is never offered while the app runs).

const baseConfig: ProjectConfig = {
  name: "demo",
  description: "A demo project",
  deployTarget: "http://localhost:8080",
  main: "main.ts",
  platforms: [],
  lang: "ts",
  app: "",
  env: { FOO: "bar" },
  createdMs: 0,
  updatedMs: 0,
};

const noop = () => {};
const neverCalled = () =>
  Promise.reject(new Error("onSave must not be called in a render test"));

const renderModal = (appRunning: boolean) =>
  renderToStaticMarkup(
    <ConfigModal
      config={baseConfig}
      appRunning={appRunning}
      onClose={noop}
      onSaved={noop}
      onSave={neverCalled}
    />,
  );

test("while the app runs, Save is disabled and the stop-first notice is shown", () => {
  const html = renderModal(true);
  const save = saveButton(html);
  assert.ok(
    / disabled[="]/.test(save),
    `Save must be disabled while the app runs: ${save}`,
  );
  assert.ok(
    /Stop the app before saving changes/.test(save),
    `Save must carry the stop-first tooltip: ${save}`,
  );
  assert.ok(
    html.includes("Stop the app before changing its configuration"),
    "the stop-first notice must be visible",
  );
});

test("while the app is stopped, Save is enabled and no notice is shown", () => {
  const html = renderModal(false);
  const save = saveButton(html);
  assert.ok(
    !/ disabled[="]/.test(save),
    `Save must be enabled while the app is stopped: ${save}`,
  );
  assert.ok(
    !html.includes("Stop the app before changing its configuration"),
    "no stop-first notice when the app is stopped",
  );
});

/** Extracts the Save button's opening tag (the primary action is the only
 * button whose text is exactly "Save"). */
function saveButton(html: string): string {
  const m = html.match(/<button[^>]*>Save<\/button>/);
  assert.ok(m, `no Save button found in:\n${html}`);
  return m[0];
}

// --- Defect-class guard -----------------------------------------------------
// The reviewed defect was a missing gate on the *configuration* write: the
// Configure modal's Save stayed live while the app ran, so the write reached
// the server and failed 409 `app_running`. The file editor's Save already
// carries the same gate (issue #889). Guard the class — "a configuration-write
// affordance left live while the app runs" — by asserting the modal's save
// button is disabled iff `appRunning`, so the gate cannot be removed or
// inverted without a red test. (A `busy` spinner-disable is not the gate.)

test("the configuration save affordance is gated on appRunning, not on busy", () => {
  // Render both states and diff the Save button's disabled attribute: it must
  // track `appRunning` exactly.
  const running = saveButton(renderModal(true));
  const stopped = saveButton(renderModal(false));
  assert.ok(/ disabled[="]/.test(running), "gated while running");
  assert.ok(!/ disabled[="]/.test(stopped), "live while stopped");

  // And the gate is `appRunning`, not the local `busy` spinner: the source
  // must disable on `appRunning` and short-circuit `save()` on it.
  const src = readFileSync(
    join(dirname(fileURLToPath(import.meta.url)), "ConfigModal.tsx"),
    "utf8",
  );
  assert.ok(
    /disabled=\{[^}]*appRunning/.test(src),
    "Save button must disable on appRunning",
  );
  assert.ok(
    /if \(appRunning\) return/.test(src),
    "save() must short-circuit on appRunning",
  );
});
