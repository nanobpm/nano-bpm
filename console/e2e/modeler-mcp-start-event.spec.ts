// Built-in "MCP start event" component — real-modeler serialization guard
// (#1611).
//
// WHY THIS SUITE EXISTS: the template's contract is what bpmn-js-element-
// templates actually PERSISTS, not what the template JSON says: optional
// typed-input fields must emit no `zeebe:property` when untouched (or cleared),
// Camunda's non-optional `io.camunda.tool:*` fields must persist, and the
// `bpmn:Message#property` binding must generate a message. A unit test over the
// template JSON can only re-implement those rules. This drives the real modeler
// — palette create → properties-panel edits → Save — and asserts the captured
// `saveXML`, then pins the shared server fixture
// (`engine-core/tests/fixtures/mcp-start-event-template.bpmn`, deployed by
// `server/src/mcp/processes_tools.rs`) to exactly what that authoring path
// emits, so the fixture cannot drift from the modeler.

import { readFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
import {
  assertNoPageCrash,
  resetTourState,
  stubConsoleApi,
  suppressStartupPanel,
} from "./fixtures.ts";
import {
  PROJECT,
  captureSavedXml,
  clickSave,
  openModel,
  stubProjectFiles,
} from "./modelerProject.ts";

const TEMPLATE_ID = "io.camunda.mcp.start-message";
const T = "io.camunda.tool:";

/** A DI-complete process with nothing but a lone end event — the canvas the
 *  MCP start event is created onto. */
const EMPTY_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="mcptest" targetNamespace="http://nanobpm.io">
  <bpmn:process id="mcp-demo" isExecutable="true">
    <bpmn:endEvent id="end" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="D">
    <bpmndi:BPMNPlane id="P" bpmnElement="mcp-demo">
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="500" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

const NAMED_ENTITIES: Record<string, string> = {
  amp: "&",
  apos: "'",
  quot: '"',
  lt: "<",
  gt: ">",
};

/** Decode XML attribute entities — named and numeric (bpmn-js writes `'` as
 *  `&#39;`, a hand-written fixture may use `&apos;` or a literal). */
function decodeXmlAttr(raw: string): string {
  return raw.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (whole, ent: string) => {
    if (ent[0] !== "#") return NAMED_ENTITIES[ent] ?? whole;
    const hex = ent[1] === "x" || ent[1] === "X";
    return String.fromCodePoint(
      parseInt(ent.slice(hex ? 2 : 1), hex ? 16 : 10),
    );
  });
}

/** `zeebe:property` name → value pairs in a serialised model, in order. */
function zeebeProperties(xml: string): Array<[string, string]> {
  return [
    ...xml.matchAll(/<zeebe:property name="([^"]+)" value="([^"]*)"/g),
  ].map((m) => [decodeXmlAttr(m[1]), decodeXmlAttr(m[2])]);
}

/** Create the MCP start event from its palette entry onto empty canvas. The
 *  new shape is left selected, so the properties panel shows the template. */
async function createMcpStartEvent(page: Page): Promise<void> {
  await page
    .locator(`.djs-palette .entry[data-action="create.urban-${TEMPLATE_ID}"]`)
    .click();
  const canvas = page.locator(".djs-container svg").first();
  const box = await canvas.boundingBox();
  if (!box) throw new Error("canvas not laid out");
  await page.mouse.click(box.x + 200, box.y + 200);
  await expect(
    page.locator(".bio-properties-panel-group-header-title", {
      hasText: "Tool definition",
    }),
  ).toBeVisible();
}

/** The properties-panel group with the given header title. */
function group(page: Page, title: string) {
  return page.locator(".bio-properties-panel-group", {
    has: page.locator(".bio-properties-panel-group-header-title", {
      hasText: new RegExp(`^${title.replace(/[()]/g, "\\$&")}$`),
    }),
  });
}

/** Expand a (possibly collapsed) group. */
async function openGroup(page: Page, title: string): Promise<void> {
  const g = group(page, title);
  const header = g.locator(".bio-properties-panel-group-header").first();
  if (!(await header.getAttribute("class"))?.includes("open")) {
    await header.click();
  }
}

/** Fill (or, with `""`, clear) a text field / select a dropdown option. */
async function setField(
  page: Page,
  groupTitle: string,
  label: string,
  value: string,
): Promise<void> {
  const field = group(page, groupTitle).getByLabel(label, { exact: true });
  if ((await field.evaluate((el) => el.tagName)) === "SELECT") {
    await field.selectOption(value);
  } else {
    await field.fill(value);
  }
  // Let the panel's debounced write land before the next interaction.
  await expect(field).toHaveValue(value);
}

const TYPED = (n: number) => `Typed input ${n} (Nano, optional)`;

test.describe("modeler — built-in MCP start event component", () => {
  test.beforeEach(async ({ page }) => {
    await stubConsoleApi(page, { projects: [{ name: PROJECT, lang: "node" }] });
    await stubProjectFiles(page, EMPTY_BPMN);
    await resetTourState(page);
    await suppressStartupPanel(page);
  });

  test("an untouched component persists only Camunda's tool fields and a generated message", async ({
    page,
  }) => {
    const noCrash = assertNoPageCrash(page);
    let saved = "";
    await captureSavedXml(page, (xml) => {
      saved = xml;
    });
    await page.goto(`projects/${PROJECT}`);
    await openModel(page, "end");
    await createMcpStartEvent(page);
    await clickSave(page);

    expect(saved).toContain(`zeebe:modelerTemplate="${TEMPLATE_ID}"`);
    expect(saved).toMatch(/<bpmn:messageEventDefinition [^>]*messageRef="/);
    expect(saved).toMatch(
      /<bpmn:message id="[^"]+" name="[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"/,
    );
    expect(zeebeProperties(saved)).toEqual(
      [
        "name",
        "purpose",
        "inputs",
        "when_to_use",
        "when_not_to_use",
        "results",
      ].map((s) => [`${T}${s}`, ""]),
    );
    noCrash();
  });

  test("authoring the shared server fixture emits exactly its properties; a cleared group emits nothing", async ({
    page,
  }) => {
    // ~20 sequential panel edits, each awaiting its debounced write.
    test.setTimeout(120_000);
    const noCrash = assertNoPageCrash(page);
    let saved = "";
    await captureSavedXml(page, (xml) => {
      saved = xml;
    });
    await page.goto(`projects/${PROJECT}`);
    await openModel(page, "end");
    await createMcpStartEvent(page);

    const TOOL = "Tool definition";
    await setField(page, TOOL, "Name", "lookup_customer");
    await setField(page, TOOL, "What it does", "Look up a customer record.");
    await setField(
      page,
      TOOL,
      "Which inputs it needs",
      "A customer email and an optional limit.",
    );
    await setField(
      page,
      TOOL,
      "When to use",
      "The user asks about a customer.",
    );

    await openGroup(page, TYPED(1));
    await setField(page, TYPED(1), "Name", "email");
    await setField(page, TYPED(1), "Type", "string");
    await setField(page, TYPED(1), "Description", "The customer's email.");
    await setField(page, TYPED(1), "Required", "true");

    await openGroup(page, TYPED(2));
    await setField(page, TYPED(2), "Name", "limit");
    await setField(page, TYPED(2), "Type", "integer");

    // Populate group 3, then clear it: clearing must remove the properties,
    // not persist empty values.
    await openGroup(page, TYPED(3));
    await setField(page, TYPED(3), "Name", "scratch");
    await setField(page, TYPED(3), "Type", "boolean");
    await setField(page, TYPED(3), "Name", "");
    await setField(page, TYPED(3), "Type", "");

    await clickSave(page);

    expect(saved).toMatch(/<bpmn:messageEventDefinition [^>]*messageRef="/);
    const fixture = readFileSync(
      new URL(
        "../../engine-core/tests/fixtures/mcp-start-event-template.bpmn",
        import.meta.url,
      ),
      "utf8",
    );
    expect(zeebeProperties(saved)).toEqual(zeebeProperties(fixture));
    expect(saved).not.toContain(`${T}input_3_`);
    noCrash();
  });
});
