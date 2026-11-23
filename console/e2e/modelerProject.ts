// Shared modeler-workspace harness for the e2e suites that mount the REAL
// bpmn-js modeler on a stubbed project and assert on its `saveXML` output
// (agent-task inspection #955, MCP start-event component #1611). One project,
// one `.bpmn` file; each spec supplies the model it opens.

import { expect, type Page } from "@playwright/test";

export const PROJECT = "agent-demo";
export const BPMN_PATH = "resources/processes/agent-task.bpmn";

/** The file tree the workspace renders — `resources/processes/agent-task.bpmn`
 *  plus the usual project scaffolding. */
const FILE_TREE = {
  files: [
    {
      name: "resources",
      path: "resources",
      kind: "dir",
      children: [
        {
          name: "processes",
          path: "resources/processes",
          kind: "dir",
          children: [
            { name: "agent-task.bpmn", path: BPMN_PATH, kind: "file" },
          ],
        },
      ],
    },
    { name: "main.ts", path: "main.ts", kind: "file" },
    { name: "package.json", path: "package.json", kind: "file" },
  ],
};

/** Layer the modeler's project-workspace routes over the shared console stubs so
 *  opening the process serves our agent-task model. Registered after
 *  `stubConsoleApi` so these more specific handlers take priority. */
export async function stubProjectFiles(
  page: Page,
  bpmn: string,
): Promise<void> {
  await page.route(`**/console/api/projects/${PROJECT}`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        config: {
          name: PROJECT,
          description: "",
          deployTarget: "http://localhost:8080",
          main: "main.ts",
          lang: "node",
          app: "console",
        },
        files: FILE_TREE.files,
        rootPath: `/tmp/${PROJECT}`,
        runState: {
          status: "stopped",
          pid: null,
          startedAtMs: null,
          lastError: null,
          compiling: false,
        },
        appUi: { enabled: true },
        runnable: true,
        missingToolchain: null,
        nodeAvailable: true,
        urbanAvailable: false,
        platforms: [],
      }),
    }),
  );
  await page.route(`**/console/api/projects/${PROJECT}/files`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(FILE_TREE),
    }),
  );
  // The file endpoint returns the raw file text (not JSON) — see
  // `lib/api.getProjectFile`. `nano.app.json` is probed and legitimately absent
  // here (non-App project), so 404 it; the requested `.bpmn` serves our model.
  // Anchor on the singular `file?` query endpoint so this handler can't shadow
  // the `/files` tree listing registered above (a `/file*` glob also matches
  // `/files`, and the later route wins).
  await page.route(
    new RegExp(`/console/api/projects/${PROJECT}/file\\?`),
    (route) => {
      const path =
        new URL(route.request().url()).searchParams.get("path") ?? "";
      if (path === BPMN_PATH) {
        return route.fulfill({
          status: 200,
          contentType: "application/xml",
          headers: { "X-File-Size": String(bpmn.length) },
          body: bpmn,
        });
      }
      return route.fulfill({ status: 404, body: "not found" });
    },
  );
  // Workspace side panels the modeler view fans out to; empty shapes keep them
  // from crashing the (error-boundary-less) SPA.
  await page.route(`**/console/api/projects/${PROJECT}/connectors`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ connectors: [] }),
    }),
  );
  await page.route(`**/console/api/projects/${PROJECT}/logs`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: "[]" }),
  );
}

/** Open the stubbed `.bpmn` and return once the modeler has settled on it
 *  (see `waitForModelLoaded`). `settledOn` is the id of a shape in the model the
 *  spec stubbed, so "settled" means "this document is on the canvas". */
export async function openModel(page: Page, settledOn: string): Promise<void> {
  await page.getByText("resources", { exact: true }).first().click();
  await page.getByText("processes", { exact: true }).first().click();
  await page.getByText("agent-task.bpmn", { exact: true }).first().click();
  await waitForModelLoaded(page, settledOn);
}

/** Wait until the modeler has imported the document (its `elementId` shape is
 *  on the canvas) AND has no load still queued (`data-load-state="idle"` on the
 *  canvas container, published by BpmnModeler's load chain). Interacting before
 *  then races a pending import, which clears the canvas and resets the
 *  selection and properties panel under the test (issue #1048). This is the
 *  readiness signal: wait on it, never on a sleep.
 *
 *  The ceiling is explicit because this one wait spans the lazy modeler chunk
 *  (compiled on first hit by the vite dev server), the file fetch and the
 *  import, which on a loaded runner can exceed the 5s default expect budget. It
 *  is a bound on a signal, not a delay: the wait returns the moment the
 *  document is on an idle canvas. */
export async function waitForModelLoaded(
  page: Page,
  elementId: string,
): Promise<void> {
  await expect(
    page.locator(
      `[data-load-state="idle"] .djs-element[data-element-id="${elementId}"]`,
    ),
  ).not.toHaveCount(0, { timeout: MODEL_LOAD_CEILING_MS });
}

const MODEL_LOAD_CEILING_MS = 30_000;

/** Intercept the workspace's file-write (`PUT …/file?path=…`, sent as raw
 *  `text/plain` — see the generated `saveProjectFile`) and hand the serialised
 *  body to `onSave`, so a test can assert on the modeler's real `saveXML`
 *  output. Registered after the `beforeEach` stubs so it wins for PUT and falls
 *  back to the GET file handler otherwise. */
export async function captureSavedXml(
  page: Page,
  onSave: (xml: string) => void,
): Promise<void> {
  await page.route(
    new RegExp(`/console/api/projects/${PROJECT}/file\\?`),
    async (route) => {
      if (route.request().method() === "PUT") {
        onSave(route.request().postData() ?? "");
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ path: BPMN_PATH, bytes: 0 }),
        });
      }
      return route.fallback();
    },
  );
}

/** Click the workspace Save button (enabled only while the editor is dirty) and
 *  wait for the write to settle (the button disables again). */
export async function clickSave(page: Page): Promise<void> {
  const save = page.getByRole("button", { name: "Save", exact: true });
  await save.click();
  await expect(save).toBeDisabled();
}
