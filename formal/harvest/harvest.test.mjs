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

// --- job-derivation: only job-producing tags schedule jobs (#1397 review) ---
// The defect class: the normaliser derived a job for EVERY task-shaped element,
// so a userTask / callActivity / abstract task made the runner stop on
// `expected-job-not-created` — a driver error counted as a Nano divergence.

const TASK_VARIANTS_XML = `<?xml version="1.0"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:ns0="http://camunda.org/schema/zeebe/1.0">
  <process id="process" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <userTask id="ut"><incoming>f1</incoming><outgoing>f2</outgoing></userTask>
    <task id="plain"><incoming>f2</incoming><outgoing>f3</outgoing></task>
    <manualTask id="manual"><incoming>f3</incoming><outgoing>f4</outgoing></manualTask>
    <receiveTask id="recv"><incoming>f4</incoming><outgoing>f5</outgoing></receiveTask>
    <serviceTask id="svc">
      <extensionElements><ns0:taskDefinition type="svc-type"/></extensionElements>
      <incoming>f5</incoming><outgoing>f6</outgoing>
    </serviceTask>
    <endEvent id="end"><incoming>f6</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="ut"/>
    <sequenceFlow id="f2" sourceRef="ut" targetRef="plain"/>
    <sequenceFlow id="f3" sourceRef="plain" targetRef="manual"/>
    <sequenceFlow id="f4" sourceRef="manual" targetRef="recv"/>
    <sequenceFlow id="f5" sourceRef="recv" targetRef="svc"/>
    <sequenceFlow id="f6" sourceRef="svc" targetRef="end"/>
  </process>
</definitions>`;

test('scenarioFor derives jobs ONLY for job-producing tags', () => {
  const g = bpmnToGraph(TASK_VARIANTS_XML);
  g.processId = 'process';
  const { scenario, outOfScope } = scenarioFor(g);
  assert.equal(outOfScope, null);
  // Only the serviceTask schedules a job; userTask goes to userTasks, and the
  // abstract task / manualTask / receiveTask are pass-throughs (no job).
  assert.deepEqual(scenario.jobs, [{ element: 'svc', jobType: 'svc-type' }]);
  assert.deepEqual(scenario.jobCompletionOrder, ['svc']);
  assert.deepEqual(scenario.userTasks, ['ut']);
  assert.deepEqual(scenario.userTaskCompletionOrder, ['ut']);
  assert.deepEqual(scenario.needs, []);
});

test('scenarioFor tags a callActivity as needing the call-activity driver, never a job', () => {
  const xml = `<?xml version="1.0"?>
  <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:ns0="http://camunda.org/schema/zeebe/1.0">
    <process id="root" isExecutable="true">
      <startEvent id="start"><outgoing>f1</outgoing></startEvent>
      <callActivity id="call">
        <extensionElements><ns0:calledElement processId="child"/></extensionElements>
        <incoming>f1</incoming><outgoing>f2</outgoing>
      </callActivity>
      <endEvent id="end"><incoming>f2</incoming></endEvent>
      <sequenceFlow id="f1" sourceRef="start" targetRef="call"/>
      <sequenceFlow id="f2" sourceRef="call" targetRef="end"/>
    </process>
  </definitions>`;
  const g = bpmnToGraph(xml);
  g.processId = 'root';
  const { scenario, outOfScope } = scenarioFor(g);
  assert.equal(outOfScope, null);
  assert.deepEqual(scenario.jobs, []);
  assert.deepEqual(scenario.jobCompletionOrder, []);
  assert.ok(scenario.needs.includes('call-activity'));
});

// --- runScenario classification ---------------------------------------------
// A non-clean Nano run is UNVERIFIED (this slice harvests no Zeebe oracle),
// never "diverging"; a user task is completed via completeUserTask.

/** A minimal TestEngine stub: deploy ok, parks one user task, completes it. */
function userTaskEngineStub() {
  let utCompleted = false;
  return class {
    deploy() { return '{}'; }
    createInstance() {
      return JSON.stringify({
        instances: [{ completed: false, activeElements: [{}] }],
        jobs: [],
        userTasks: [{ key: '7', elementId: 'ut', state: 'Created' }],
        incidents: [],
      });
    }
    completeUserTask() {
      utCompleted = true;
      return JSON.stringify({
        instances: [{ completed: true, activeElements: [] }],
        jobs: [], userTasks: [], incidents: [],
      });
    }
    completeJob() { throw new Error('no jobs in this model'); }
    free() {}
    get utCompleted() { return utCompleted; }
  };
}

test('runScenario completes a user task via completeUserTask and reports runnable', async () => {
  const { runScenario } = await import('./run.mjs');
  const Eng = userTaskEngineStub();
  const r = runScenario(Eng, 'x', '<xml/>', {
    process: 'process', jobs: [], jobCompletionOrder: [],
    userTasks: ['ut'], userTaskCompletionOrder: ['ut'],
    messageCorrelation: [], timerTicks: [], needs: [],
  });
  assert.equal(r.status, 'runnable');
  assert.deepEqual(r.observed.completed, ['ut']);
});

test('runScenario classifies a non-clean Nano run as unverified, never diverging', async () => {
  const { runScenario } = await import('./run.mjs');
  class Stuck {
    deploy() { return '{}'; }
    createInstance() {
      return JSON.stringify({
        instances: [{ completed: false, activeElements: [{}] }],
        jobs: [], userTasks: [], incidents: [],
      });
    }
    free() {}
  }
  const r = runScenario(Stuck, 'x', '<xml/>', {
    process: 'process', jobs: [], jobCompletionOrder: [],
    userTasks: [], userTaskCompletionOrder: [],
    messageCorrelation: [], timerTicks: [], needs: [],
  });
  assert.equal(r.status, 'unverified');
  assert.equal(r.detail, 'instance-did-not-complete');
});

test('runScenario routes a call-activity scenario to out-of-scope (driver capability)', async () => {
  const { runScenario } = await import('./run.mjs');
  class Eng {
    deploy() { return '{}'; }
    free() {}
  }
  const r = runScenario(Eng, 'x', '<xml/>', {
    process: 'root', jobs: [], jobCompletionOrder: [],
    userTasks: [], userTaskCompletionOrder: [],
    messageCorrelation: [], timerTicks: [], needs: ['call-activity'],
  });
  assert.equal(r.status, 'out-of-scope');
  assert.match(r.detail, /driver-capability: call-activity/);
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
