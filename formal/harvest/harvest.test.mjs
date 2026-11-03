// Unit tests for the harvest pipeline (#1249): the fluent-chain parser, the
// normaliser's graph/scenario derivation, and the runner's classification.
// Run with `node --test formal/harvest/*.test.mjs` (no JVM, no network).

import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  stripComments, splitArgs, resolveArg, collectConstants, resolveLambda,
  parseChain, parseFile, canonicaliseXml,
} from './harvest.mjs';
import { bpmnToGraph, scenarioFor, cellsFor } from './normalise.mjs';

// --- stripComments -----------------------------------------------------------

test('stripComments blanks line and block comments, keeps strings', () => {
  const src = 'String s = "not // a comment"; // real comment\n/* block\n comment */ int x = 1;';
  const out = stripComments(src);
  assert.ok(out.includes('"not // a comment"'));
  assert.ok(!out.includes('real comment'));
  assert.ok(!out.includes('block'));
  assert.ok(out.includes('int x = 1;'));
});

// --- splitArgs ---------------------------------------------------------------

test('splitArgs splits top-level commas, respecting nesting and quotes', () => {
  assert.deepEqual(splitArgs('"a", "b"'), ['"a"', '"b"']);
  assert.deepEqual(splitArgs('"a,b", f(x, y)'), ['"a,b"', 'f(x, y)']);
  assert.deepEqual(splitArgs(''), []);
  assert.deepEqual(splitArgs('"id", b -> b.zeebeJobType("x")'), ['"id"', 'b -> b.zeebeJobType("x")']);
});

// --- resolveArg --------------------------------------------------------------

test('resolveArg resolves literals and constants, rejects expressions', () => {
  assert.equal(resolveArg('"hello"'), 'hello');
  assert.equal(resolveArg('"a" + "b"'), 'ab');
  assert.equal(resolveArg('42'), 42);
  assert.equal(resolveArg('true'), true);
  assert.equal(resolveArg('PROCESS_ID', { PROCESS_ID: 'process' }), 'process');
  assert.equal(typeof resolveArg('someVar'), 'symbol'); // UNRESOLVED
  assert.equal(typeof resolveArg('b -> b.x()'), 'symbol'); // lambda, not a value
});

// --- collectConstants --------------------------------------------------------

test('collectConstants picks up static final literals', () => {
  const src = `
    private static final String PROCESS_ID = "process";
    private static final int RETRIES = 3;
    private static final String COMPUTED = other.trim();
  `;
  const c = collectConstants(src);
  assert.equal(c.PROCESS_ID, 'process');
  assert.equal(c.RETRIES, 3);
  assert.equal(c.COMPUTED, undefined); // non-literal not collected
});

// --- resolveLambda -----------------------------------------------------------

test('resolveLambda flattens a flat config-lambda chain', () => {
  const lam = resolveLambda('b -> b.zeebeJobType("type1").name("Task")');
  assert.deepEqual(lam, [
    { m: 'zeebeJobType', args: ['type1'] },
    { m: 'name', args: ['Task'] },
  ]);
});

test('resolveLambda rejects a nested lambda', () => {
  assert.equal(typeof resolveLambda('b -> b.zeebeLinkedResources(l -> l.resourceId("x"))'), 'symbol');
});

// --- parseChain / parseFile --------------------------------------------------

test('parseChain harvests a simple linear process', () => {
  const src = `
    Bpmn.createExecutableProcess("process")
        .startEvent("start")
        .serviceTask("task1", b -> b.zeebeJobType("type1"))
        .endEvent("end")
        .done();
  `;
  const chain = parseChain(stripComments(src), src.indexOf('Bpmn.createExecutableProcess'), {});
  assert.equal(chain.processId, 'process');
  assert.equal(chain.reason, null);
  assert.deepEqual(chain.calls.map((c) => c.m),
    ['startEvent', 'serviceTask', 'zeebeJobType', 'endEvent']);
  // the lambda's config call is flattened after its element, args preserved
  assert.deepEqual(chain.calls[1], { m: 'serviceTask', args: ['task1'] });
  assert.deepEqual(chain.calls[2], { m: 'zeebeJobType', args: ['type1'] });
});

test('parseChain resolves a PROCESS_ID constant', () => {
  const src = `
    private static final String PROCESS_ID = "process";
    BpmnModelInstance m = Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
  `;
  const clean = stripComments(src);
  const chain = parseChain(clean, clean.indexOf('Bpmn.createExecutableProcess'), collectConstants(clean));
  assert.equal(chain.processId, 'process');
  assert.equal(chain.reason, null);
});

test('parseChain flags a non-literal process id as unparseable', () => {
  const src = 'Bpmn.createExecutableProcess(processId).startEvent().endEvent().done();';
  const clean = stripComments(src);
  const chain = parseChain(clean, clean.indexOf('Bpmn.createExecutableProcess'), {});
  assert.equal(chain.reason, 'process-id-not-literal');
});

test('parseChain flags an unsupported builder method', () => {
  const src = 'Bpmn.createExecutableProcess("p").startEvent().eventSubProcess("sub", s -> {}).done();';
  const clean = stripComments(src);
  const chain = parseChain(clean, clean.indexOf('Bpmn.createExecutableProcess'), {});
  assert.match(chain.reason, /^unsupported-method:eventSubProcess/);
});

test('parseFile finds every chain in a file', () => {
  const src = `
    private static final String P = "p";
    BpmnModelInstance a = Bpmn.createExecutableProcess(P).startEvent().endEvent().done();
    BpmnModelInstance b = Bpmn.createExecutableProcess("q").startEvent().endEvent().done();
  `;
  const chains = parseFile(src);
  assert.equal(chains.length, 2);
  assert.deepEqual(chains.map((c) => c.processId).sort(), ['p', 'q']);
});

// --- bpmnToGraph / scenarioFor ----------------------------------------------

const SIMPLE_XML = `<?xml version="1.0"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:ns0="http://camunda.org/schema/zeebe/1.0">
  <process id="process" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <serviceTask id="task1">
      <extensionElements><ns0:taskDefinition type="type1"/></extensionElements>
      <incoming>f1</incoming><outgoing>f2</outgoing>
    </serviceTask>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="task1"/>
    <sequenceFlow id="f2" sourceRef="task1" targetRef="end"/>
  </process>
</definitions>`;

test('bpmnToGraph extracts nodes, edges and job types', () => {
  const g = bpmnToGraph(SIMPLE_XML);
  assert.equal(g.nodes.start, 'start');
  assert.equal(g.nodes.task1, 'task');
  assert.equal(g.nodes.end, 'end');
  assert.equal(g.jobTypes.task1, 'type1');
  assert.equal(g.edges.length, 2);
});

test('scenarioFor derives the job and completion order', () => {
  const g = bpmnToGraph(SIMPLE_XML);
  g.processId = 'process';
  const { scenario, outOfScope } = scenarioFor(g);
  assert.equal(outOfScope, null);
  assert.deepEqual(scenario.jobs, [{ element: 'task1', jobType: 'type1' }]);
  assert.deepEqual(scenario.jobCompletionOrder, ['task1']);
  assert.deepEqual(scenario.needs, []);
});

test('scenarioFor flags a conditional model as needing variables', () => {
  const xml = SIMPLE_XML.replace(
    '<sequenceFlow id="f1" sourceRef="start" targetRef="task1"/>',
    '<sequenceFlow id="f1" sourceRef="start" targetRef="task1"><conditionExpression>=a &gt; 0</conditionExpression></sequenceFlow>');
  const g = bpmnToGraph(xml);
  g.processId = 'process';
  const { scenario } = scenarioFor(g);
  assert.ok(scenario.needs.includes('variables'));
});

test('scenarioFor rejects a model with no start event', () => {
  const xml = SIMPLE_XML.replace(/<startEvent[\s\S]*?<\/startEvent>/, '');
  const g = bpmnToGraph(xml);
  const { scenario, outOfScope } = scenarioFor(g);
  assert.equal(scenario, null);
  assert.equal(outOfScope, 'no-start-event');
});

test('cellsFor tags element cells present in the surface', () => {
  const g = bpmnToGraph(SIMPLE_XML);
  const cells = cellsFor(g);
  assert.ok(cells.includes('element:StartEvent'));
  assert.ok(cells.includes('element:ServiceTask'));
  assert.ok(cells.includes('element:EndEvent'));
});

// --- canonicaliseXml ---------------------------------------------------------

test('canonicaliseXml rewrites random-UUID ids deterministically', () => {
  const a = '<sequenceFlow id="sequenceFlow_0f0dc508-08e2-46b9-aaff-6be563c2f5b8" sourceRef="startEvent_ce2fdb52-2c89-46e2-b61b-3971c615c0af"/>';
  const b = '<sequenceFlow id="sequenceFlow_11111111-2222-4333-8444-555555555555" sourceRef="startEvent_66666666-7777-4888-8999-aaaaaaaaaaaa"/>';
  // different UUIDs, same shape -> identical canonical form
  assert.equal(canonicaliseXml(a), canonicaliseXml(b));
  // repeated id references collapse to the SAME canonical id (referential integrity)
  const repeated = '<x id="sequenceFlow_0f0dc508-08e2-46b9-aaff-6be563c2f5b8"/><y sourceRef="sequenceFlow_0f0dc508-08e2-46b9-aaff-6be563c2f5b8"/>';
  const out = canonicaliseXml(repeated);
  assert.ok(out.includes('id="sequenceFlow_1"'));
  assert.ok(out.includes('sourceRef="sequenceFlow_1"'));
  // a non-UUID id is left untouched
  assert.equal(canonicaliseXml('<serviceTask id="task1"/>'), '<serviceTask id="task1"/>');
});
