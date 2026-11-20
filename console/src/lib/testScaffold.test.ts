// Unit tests for the test-run payload scaffolder (ADR 0040 §9/§10).
// Node-native: run with `node --experimental-strip-types --test src/lib/testScaffold.test.ts`.
// Covers the pure model scan + JSON-skeleton generation without a browser DOM.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseModelEnvelopes,
  scaffoldForType,
  scaffoldJobOutput,
  scaffoldStartVars,
  prefillMessagePublish,
  prefillSignalBroadcast,
} from "./testScaffold.ts";

// A compact model that mirrors the migrated urban-pr-review shape: payload types
// carried in-model as all-`extend` nano:shapes, envelopes on the service task,
// and a start event flowing into the entry task.
const XML = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
  xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:extensionElements>
      <nano:shapes>
        <nano:shape id="ReviewIn" name="review — input">
          <nano:extend name="prUrl" type="string" />
          <nano:extend name="prNumber" type="integer" />
          <nano:extend name="approved" type="boolean" />
          <nano:extend name="answer" type="string" optional="true" />
          <nano:extend name="labels" type="string" list="true" />
          <nano:extend name="submittedAt" type="datetime" />
        </nano:shape>
        <nano:shape id="ReviewOut">
          <nano:extend name="status" type="string" />
          <nano:extend name="summary" type="string" />
        </nano:shape>
      </nano:shapes>
    </bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="review" name="Review">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="senior:pr-review" />
        <zeebe:properties>
          <zeebe:property name="io.nanobpm.dataEnvelope.in" value="ReviewIn" />
          <zeebe:property name="io.nanobpm.dataEnvelope.out" value="ReviewOut" />
        </zeebe:properties>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="review" />
  </bpmn:process>
</bpmn:definitions>`;

test("parseModelEnvelopes lifts shapes, task envelopes, and start targets", () => {
  const m = parseModelEnvelopes(XML);
  assert.deepEqual(m.tasks.get("review"), { in: "ReviewIn", out: "ReviewOut" });
  assert.equal(m.shapes.get("ReviewIn")?.length, 6);
  assert.deepEqual(m.shapes.get("ReviewOut"), [
    { name: "status", type: "string" },
    { name: "summary", type: "string" },
  ]);
  assert.deepEqual(m.shapes.get("ReviewIn")?.[3], {
    name: "answer",
    type: "string",
    optional: true,
  });
  assert.deepEqual(m.shapes.get("ReviewIn")?.[4], {
    name: "labels",
    type: "string",
    list: true,
  });
  assert.ok(m.startTargets.has("review"));
});

test("scaffoldForType emits a typed placeholder per field", () => {
  const m = parseModelEnvelopes(XML);
  assert.deepEqual(JSON.parse(scaffoldForType(m, "ReviewIn")), {
    prUrl: "",
    prNumber: 0,
    approved: false,
    answer: "",
    labels: [],
    submittedAt: "",
  });
});

test("scaffoldForType returns {} for an unknown or empty type", () => {
  const m = parseModelEnvelopes(XML);
  assert.equal(scaffoldForType(m, "Nope"), "{}");
  assert.equal(scaffoldForType(m, undefined), "{}");
});

test("scaffoldJobOutput scaffolds from the element's out envelope", () => {
  const m = parseModelEnvelopes(XML);
  assert.deepEqual(JSON.parse(scaffoldJobOutput(m, "review")), {
    status: "",
    summary: "",
  });
  assert.equal(scaffoldJobOutput(m, "unknownElement"), "{}");
});

test("scaffoldStartVars uses the lone entry task's input envelope", () => {
  const m = parseModelEnvelopes(XML);
  const start = JSON.parse(scaffoldStartVars(m));
  assert.equal(start.prUrl, "");
  assert.equal(start.prNumber, 0);
});

test("scaffoldStartVars yields {} when the entry is ambiguous", () => {
  // Two start-target tasks with differing input envelopes → not model-typed.
  const xml = XML.replace(
    '<bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="review" />',
    '<bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="review" />' +
      '<bpmn:serviceTask id="other"><bpmn:extensionElements><zeebe:properties>' +
      '<zeebe:property name="io.nanobpm.dataEnvelope.in" value="ReviewOut" />' +
      "</zeebe:properties></bpmn:extensionElements></bpmn:serviceTask>" +
      '<bpmn:sequenceFlow id="f2" sourceRef="Start" targetRef="other" />',
  );
  assert.equal(scaffoldStartVars(parseModelEnvelopes(xml)), "{}");
});

test("scaffoldStartVars yields {} when the start fans out and only one target is typed", () => {
  // Two start targets, but only `review` carries an input envelope. The start is
  // still ambiguous (it fans out), so the process input is not model-typed.
  const xml = XML.replace(
    '<bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="review" />',
    '<bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="review" />' +
      '<bpmn:serviceTask id="other" /><bpmn:sequenceFlow id="f2" sourceRef="Start" targetRef="other" />',
  );
  const m = parseModelEnvelopes(xml);
  assert.equal(m.startTargets.size, 2);
  assert.equal(scaffoldStartVars(m), "{}");
});

// --- none-start-event envelopes (process start inputs) ---------------------

test("parseModelEnvelopes lifts a none start event's input envelope", () => {
  const xml = XML.replace(
    '<bpmn:startEvent id="Start">',
    '<bpmn:startEvent id="Start"><bpmn:extensionElements><zeebe:properties>' +
      '<zeebe:property name="io.nanobpm.dataEnvelope.in" value="ReviewIn" />' +
      "</zeebe:properties></bpmn:extensionElements>",
  );
  const m = parseModelEnvelopes(xml);
  assert.equal(m.startEnvelopes.get("p"), "ReviewIn");
});

test("scaffoldStartVars prefers the declared start envelope over the entry task's", () => {
  // The start event declares `ReviewOut` while the entry task declares
  // `ReviewIn`: the start event is the authoritative start-input carrier.
  const xml = XML.replace(
    '<bpmn:startEvent id="Start">',
    '<bpmn:startEvent id="Start"><bpmn:extensionElements><zeebe:properties>' +
      '<zeebe:property name="io.nanobpm.dataEnvelope.in" value="ReviewOut" />' +
      "</zeebe:properties></bpmn:extensionElements>",
  );
  const m = parseModelEnvelopes(xml);
  assert.deepEqual(JSON.parse(scaffoldStartVars(m)), {
    status: "",
    summary: "",
  });
});

test("a message/timer start event is not a none start and carries no start envelope", () => {
  for (const def of [
    '<bpmn:messageEventDefinition messageRef="Msg_1" />',
    "<bpmn:timerEventDefinition><bpmn:timeCycle>R/PT1H</bpmn:timeCycle></bpmn:timerEventDefinition>",
  ]) {
    const xml = XML.replace(
      '<bpmn:startEvent id="Start">',
      '<bpmn:startEvent id="Start"><bpmn:extensionElements><zeebe:properties>' +
        '<zeebe:property name="io.nanobpm.dataEnvelope.in" value="ReviewIn" />' +
        `</zeebe:properties></bpmn:extensionElements>${def}`,
    );
    const m = parseModelEnvelopes(xml);
    assert.equal(m.startEnvelopes.size, 0);
  }
});

test("a none start with no envelope leaves the entry-task heuristic in place", () => {
  // Sanity: the none start carries no envelope here, so the heuristic still
  // applies (the lone entry task's input envelope).
  const m = parseModelEnvelopes(XML);
  assert.equal(m.startEnvelopes.size, 0);
  assert.equal(JSON.parse(scaffoldStartVars(m)).prUrl, "");
});

test("scaffoldStartVars scopes to the selected process in a multi-process model", () => {
  // Two processes, each with its own start → entry task and distinct input
  // envelope. Unscoped, the union of start targets is ambiguous → "{}"; scoped
  // to a process id, each scaffolds its own entry input.
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
  xmlns:nano="https://nanobpm.io/schema/shapes/1.0">
  <bpmn:process id="pa" isExecutable="true">
    <bpmn:extensionElements><nano:shapes>
      <nano:shape id="AIn"><nano:extend name="a" type="string" /></nano:shape>
    </nano:shapes></bpmn:extensionElements>
    <bpmn:startEvent id="sa"><bpmn:outgoing>fa</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="ta"><bpmn:extensionElements><zeebe:properties>
      <zeebe:property name="io.nanobpm.dataEnvelope.in" value="AIn" />
    </zeebe:properties></bpmn:extensionElements><bpmn:incoming>fa</bpmn:incoming></bpmn:serviceTask>
    <bpmn:sequenceFlow id="fa" sourceRef="sa" targetRef="ta" />
  </bpmn:process>
  <bpmn:process id="pb" isExecutable="true">
    <bpmn:extensionElements><nano:shapes>
      <nano:shape id="BIn"><nano:extend name="b" type="integer" /></nano:shape>
    </nano:shapes></bpmn:extensionElements>
    <bpmn:startEvent id="sb"><bpmn:outgoing>fb</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="tb"><bpmn:extensionElements><zeebe:properties>
      <zeebe:property name="io.nanobpm.dataEnvelope.in" value="BIn" />
    </zeebe:properties></bpmn:extensionElements><bpmn:incoming>fb</bpmn:incoming></bpmn:serviceTask>
    <bpmn:sequenceFlow id="fb" sourceRef="sb" targetRef="tb" />
  </bpmn:process>
</bpmn:definitions>`;
  const m = parseModelEnvelopes(xml);
  assert.deepEqual([...(m.startTargetsByProcess.get("pa") ?? [])], ["ta"]);
  assert.deepEqual([...(m.startTargetsByProcess.get("pb") ?? [])], ["tb"]);
  // Unscoped union spans both processes → ambiguous.
  assert.equal(scaffoldStartVars(m), "{}");
  assert.deepEqual(JSON.parse(scaffoldStartVars(m, "pa")), { a: "" });
  assert.deepEqual(JSON.parse(scaffoldStartVars(m, "pb")), { b: 0 });
});

test("parseModelEnvelopes tolerates a model with no shapes or envelopes", () => {
  const m = parseModelEnvelopes(
    '<bpmn:definitions><bpmn:process id="p"><bpmn:startEvent id="s" /></bpmn:process></bpmn:definitions>',
  );
  assert.equal(m.shapes.size, 0);
  assert.equal(m.tasks.size, 0);
});

test("prefillMessagePublish seeds name + correlation key from the subscription", () => {
  const form = prefillMessagePublish({
    messageName: "review-ready",
    correlationKey: "pr-42",
  });
  assert.deepEqual(form, {
    messageName: "review-ready",
    correlationKey: "pr-42",
    variables: "{}",
  });
});

test("prefillMessagePublish tolerates a missing correlation key", () => {
  const form = prefillMessagePublish({ messageName: "kickoff" });
  assert.equal(form.messageName, "kickoff");
  assert.equal(form.correlationKey, "");
  assert.equal(form.variables, "{}");
});

test("prefillSignalBroadcast seeds the signal name only (name-only correlation)", () => {
  const form = prefillSignalBroadcast({ signalName: "cancel-all" });
  assert.deepEqual(form, { signalName: "cancel-all", variables: "{}" });
});
