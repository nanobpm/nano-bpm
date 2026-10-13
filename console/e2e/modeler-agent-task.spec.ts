// Modeler agent-task inspection guard (#955).
//
// WHY THIS SUITE EXISTS: an *agent task* is a `bpmn:ServiceTask` carrying a
// `zeebe:linkedResource` with `linkName="prompt"` (#952). Selecting one renders
// the "Agent task" properties group, whose fixed "Resource type" / "Link name"
// fields are `@bpmn-io/properties-panel` `TextFieldEntry`s. That component calls
// `debounce(onInput)` in a `useMemo` on every render — even when `disabled` — so
// an entry built without a `debounce` throws "is not a function" mid-render.
//
// The console has NO error boundary (see fixtures.assertNoPageCrash), so that
// throw unmounts the entire properties panel: the user loses not just the Agent
// task group but every other group (Data envelope included) and it looks like
// "the modeler lost its palette". Nothing in a unit test or build catches a
// render-time throw in a properties-panel entry; only a browser mounting the
// real modeler does. This is the compensating guard for that failure class.

import { expect, test, type Page } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";

const PROJECT = "agent-demo";
const BPMN_PATH = "resources/processes/agent-task.bpmn";

/** A minimal but DI-complete process whose sole service task is an agent task —
 *  it carries the canonical prompt `zeebe:linkedResource` the toolchain emits. */
const AGENT_TASK_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="agenttest" targetNamespace="http://nanobpm.io">
  <bpmn:process id="agent-task-demo" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="work" name="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="demo-job" />
        <zeebe:linkedResources>
          <zeebe:linkedResource resourceId="feature.md" bindingType="latest" resourceType="GenericScript" linkName="prompt" />
        </zeebe:linkedResources>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end">
      <bpmn:incoming>f2</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="D">
    <bpmndi:BPMNPlane id="P" bpmnElement="agent-task-demo">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="180" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="work_di" bpmnElement="work">
        <dc:Bounds x="270" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="430" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="216" y="118" />
        <di:waypoint x="270" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="370" y="118" />
        <di:waypoint x="430" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

/** A DI-complete process whose service task carries the canonical
 *  `<zeebe:agentDefinition agentType="external"/>` marker AND the
 *  `io.nanobpm.agentTask.autoSubscribe=false` opt-out — the marker-based agent
 *  task recognition path (no prompt link) that #1180 adds to the modeler. */
const MARKER_TASK_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="markertest" targetNamespace="http://nanobpm.io">
  <bpmn:process id="agent-task-demo" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="work" name="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="demo-job" />
        <zeebe:agentDefinition agentType="external" />
        <zeebe:properties>
          <zeebe:property name="io.nanobpm.agentTask.autoSubscribe" value="false" />
        </zeebe:properties>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end">
      <bpmn:incoming>f2</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="D">
    <bpmndi:BPMNPlane id="P" bpmnElement="agent-task-demo">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="180" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="work_di" bpmnElement="work">
        <dc:Bounds x="270" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="430" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="216" y="118" />
        <di:waypoint x="270" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="370" y="118" />
        <di:waypoint x="430" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

/** A DI-complete process whose service task is a PLAIN service task — no prompt
 *  link and no external-agent marker. Authoring the marker onto it from the
 *  panel exercises the `value=true` toggle path (`moddle.create("zeebe:Agent\
 *  Definition")` + live bpmn-js wiring) from an unmarked task, which the
 *  marker-seeded fixtures above never reach (issue #1186). */
const PLAIN_TASK_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="plaintest" targetNamespace="http://nanobpm.io">
  <bpmn:process id="agent-task-demo" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="work" name="work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="demo-job" />
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end">
      <bpmn:incoming>f2</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="work" />
    <bpmn:sequenceFlow id="f2" sourceRef="work" targetRef="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="D">
    <bpmndi:BPMNPlane id="P" bpmnElement="agent-task-demo">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="180" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="work_di" bpmnElement="work">
        <dc:Bounds x="270" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="430" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="216" y="118" />
        <di:waypoint x="270" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="370" y="118" />
        <di:waypoint x="430" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

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
async function stubProjectFiles(
  page: Page,
  bpmn: string = AGENT_TASK_BPMN,
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

/** Open `resources/processes/agent-task.bpmn` in the workspace tree so the
 *  modeler mounts. */
async function openAgentModel(page: Page): Promise<void> {
  await page.getByText("resources", { exact: true }).first().click();
  await page.getByText("processes", { exact: true }).first().click();
  await page.getByText("agent-task.bpmn", { exact: true }).first().click();
}

/** Intercept the workspace's file-write (`PUT …/file?path=…`, sent as raw
 *  `text/plain` — see the generated `saveProjectFile`) and hand the serialised
 *  body to `onSave`, so a test can assert on the modeler's real `saveXML`
 *  output. Registered after the `beforeEach` stubs so it wins for PUT and falls
 *  back to the GET file handler otherwise. */
async function captureSavedXml(
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
async function clickSave(page: Page): Promise<void> {
  const save = page.getByRole("button", { name: "Save", exact: true });
  await save.click();
  await expect(save).toBeDisabled();
}

/** Flip a `ToggleSwitchEntry` by its entry id. The underlying `<input>` is
 *  visually hidden behind the slider, so we click the switcher label (which is
 *  what a user clicks) rather than the sr-only checkbox. */
async function toggleSwitch(page: Page, entryId: string): Promise<void> {
  await page
    .locator(
      `[data-entry-id="${entryId}"] .bio-properties-panel-toggle-switch__slider`,
    )
    .click();
}

/** Expand the (default-collapsed) "Agent task" properties group so its controls
 *  are interactable, then confirm a control is visible. */
async function openAgentGroup(page: Page): Promise<void> {
  const slider = page.locator(
    `[data-entry-id="nano-agent-external-toggle"] .bio-properties-panel-toggle-switch__slider`,
  );
  if (!(await slider.isVisible())) {
    await page
      .locator(
        '.bio-properties-panel-group:has(.bio-properties-panel-group-header-title:text-is("Agent task")) .bio-properties-panel-group-header',
      )
      .click();
  }
  await expect(slider).toBeVisible();
}

/** Select the sole agent service task and expand its "Agent task" properties
 *  group so the marker/opt-out controls are interactable. A Save deselects the
 *  element (and re-collapses the group), so call this before each round of
 *  toggling. */
async function selectAgentTaskAndOpenGroup(page: Page): Promise<void> {
  await page.locator('.djs-element[data-element-id="work"]').first().click();
  await openAgentGroup(page);
}

test.describe("modeler — agent task inspection", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page, { projects: [{ name: PROJECT, lang: "node" }] });
    await stubProjectFiles(page);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("selecting a prompt-bound service task renders the Agent task group without unmounting the panel", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await page.goto(`projects/${PROJECT}`);

    // Open resources/processes/agent-task.bpmn → the modeler mounts.
    await page.getByText("resources", { exact: true }).first().click();
    await page.getByText("processes", { exact: true }).first().click();
    await page.getByText("agent-task.bpmn", { exact: true }).first().click();

    // The custom renderer decorates the prompt-bound task with an AGENT badge —
    // proof the model imported and the agent-task renderer ran.
    await expect(
      page.locator("svg").getByText("AGENT", { exact: true }),
    ).toBeVisible();

    // Select the agent task. Before the fix this threw mid-render
    // ("debounce is not a function") and unmounted the whole panel.
    await page.locator('.djs-element[data-element-id="work"]').first().click();

    // The Agent task group and ALL its fields render — including the two fixed
    // fields whose missing `debounce` caused the crash.
    await expect(
      page.locator(
        '.bio-properties-panel-group-header-title:has-text("Agent task")',
      ),
    ).toBeVisible();
    await expect(
      page.locator("#bio-properties-panel-nano-agent-resourceType"),
    ).toHaveCount(1);
    await expect(
      page.locator("#bio-properties-panel-nano-agent-linkName"),
    ).toHaveCount(1);

    // And the pre-existing groups are still there — the panel did not unmount.
    await expect(
      page.locator(
        '.bio-properties-panel-group-header-title:has-text("Task definition")',
      ),
    ).toBeVisible();

    noCrash();
  });
});

test.describe("modeler — external agent marker + opt-out", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page, { projects: [{ name: PROJECT, lang: "node" }] });
    await stubProjectFiles(page, MARKER_TASK_BPMN);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("recognises a marker-only agent task and surfaces the marker + opt-out controls", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    await page.goto(`projects/${PROJECT}`);

    await page.getByText("resources", { exact: true }).first().click();
    await page.getByText("processes", { exact: true }).first().click();
    await page.getByText("agent-task.bpmn", { exact: true }).first().click();

    // The renderer recognises the task from the external-agent marker alone (no
    // prompt link) and decorates it — AGENT badge plus the "NO --auto" opt-out.
    await expect(
      page.locator("svg").getByText("AGENT", { exact: true }),
    ).toBeVisible();
    await expect(page.locator("svg").getByText("NO --auto")).toBeVisible();

    await page.locator('.djs-element[data-element-id="work"]').first().click();

    // The Agent task group renders the marker toggle and, because the marker is
    // present, the opt-out toggle — without unmounting the panel.
    await expect(
      page.locator(
        '.bio-properties-panel-group-header-title:has-text("Agent task")',
      ),
    ).toBeVisible();
    await expect(
      page.locator("#bio-properties-panel-nano-agent-external-toggle"),
    ).toHaveCount(1);
    await expect(
      page.locator("#bio-properties-panel-nano-agent-optout"),
    ).toHaveCount(1);

    noCrash();
  });

  test("round-trips the marker + opt-out through a real saveXML, exercising both controls", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);

    // Capture the XML the modeler serialises on Save — a real `saveXML` round
    // trip through the augmented moddle descriptor (`zeebeModdleWithAgent`).
    // This is the surface #1180's descriptor guards: without it bpmn-moddle
    // silently drops `zeebe:agentDefinition` on save, and the authored marker is
    // lost. The GET already proved `fromXML` (the badges rendered); this proves
    // `toXML` preserves the marker and the opt-out too.
    let savedXml = "";
    await captureSavedXml(page, (xml) => {
      savedXml = xml;
    });

    await page.goto(`projects/${PROJECT}`);
    await openAgentModel(page);
    await expect(
      page.locator("svg").getByText("AGENT", { exact: true }),
    ).toBeVisible();

    // Toggle the opt-out control OFF (a real `setValue` through the panel
    // wiring), save, and assert the serialised XML: the marker survives, the
    // opt-out is gone.
    savedXml = "";
    await selectAgentTaskAndOpenGroup(page);
    await toggleSwitch(page, "nano-agent-optout");
    await clickSave(page);
    await expect.poll(() => savedXml).toContain('agentType="external"');
    expect(savedXml).not.toContain("io.nanobpm.agentTask.autoSubscribe");

    // Toggle the opt-out control back ON, save, and assert the opt-out property
    // round-trips through the real serialiser alongside the marker. (A Save
    // deselects the task, so re-select and re-open the group first.)
    savedXml = "";
    await selectAgentTaskAndOpenGroup(page);
    await toggleSwitch(page, "nano-agent-optout");
    await clickSave(page);
    await expect
      .poll(() => savedXml)
      .toContain("io.nanobpm.agentTask.autoSubscribe");
    expect(savedXml).toContain('value="false"');
    expect(savedXml).toContain('agentType="external"');

    noCrash();
  });

  test("removing the marker clears the orphaned opt-out in the saved XML", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);

    // The moderate #1186 finding: the opt-out toggle is hidden once the marker
    // is gone, so removing the marker must also clear `autoSubscribe="false"` —
    // otherwise it is stranded in the saved BPMN with no control to remove it.
    let savedXml = "";
    await captureSavedXml(page, (xml) => {
      savedXml = xml;
    });

    await page.goto(`projects/${PROJECT}`);
    await openAgentModel(page);
    await expect(
      page.locator("svg").getByText("AGENT", { exact: true }),
    ).toBeVisible();
    await expect(page.locator("svg").getByText("NO --auto")).toBeVisible();
    await selectAgentTaskAndOpenGroup(page);

    // Toggle the marker OFF. With no prompt link the task stops being an agent
    // task, so both decorations disappear — and the opt-out goes with it.
    await toggleSwitch(page, "nano-agent-external-toggle");
    await expect(
      page.locator("svg").getByText("AGENT", { exact: true }),
    ).toHaveCount(0);
    await expect(page.locator("svg").getByText("NO --auto")).toHaveCount(0);

    await clickSave(page);
    // Neither the marker nor the orphaned opt-out survives the save.
    await expect.poll(() => savedXml).not.toContain("agentDefinition");
    expect(savedXml).not.toContain("io.nanobpm.agentTask.autoSubscribe");

    noCrash();
  });
});

test.describe("modeler — authoring the external marker from an unmarked task", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page, { projects: [{ name: PROJECT, lang: "node" }] });
    // A PLAIN service task — no prompt link, no marker — so toggling the marker
    // ON drives the `value=true` panel path from scratch (issue #1186).
    await stubProjectFiles(page, PLAIN_TASK_BPMN);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("toggling the marker ON authors the marker and round-trips it through saveXML", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);

    let savedXml = "";
    await captureSavedXml(page, (xml) => {
      savedXml = xml;
    });

    await page.goto(`projects/${PROJECT}`);
    await openAgentModel(page);

    // Nothing agentic yet: the plain service task carries no marker, so the
    // renderer draws no AGENT badge.
    await expect(
      page.locator("svg").getByText("AGENT", { exact: true }),
    ).toHaveCount(0);

    // Toggle the external-agent marker ON — a real `setValue` through the panel
    // wiring, which calls `moddle.create("zeebe:AgentDefinition")` on a task that
    // never had the marker (the exact path the marker-seeded fixtures skip).
    await selectAgentTaskAndOpenGroup(page);
    await toggleSwitch(page, "nano-agent-external-toggle");

    // `isAgentTask` now recognises the task from the freshly-authored marker, so
    // the renderer decorates it live — proof the marker was created and wired.
    await expect(
      page.locator("svg").getByText("AGENT", { exact: true }),
    ).toBeVisible();

    await clickSave(page);
    // The authored marker round-trips through the real serialiser (the augmented
    // `zeebeModdleWithAgent` descriptor), rather than being dropped on save.
    await expect.poll(() => savedXml).toContain('agentType="external"');
    expect(savedXml).toContain("agentDefinition");

    noCrash();
  });
});
