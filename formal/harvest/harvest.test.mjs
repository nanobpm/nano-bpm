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

test('splitArgs keeps escaped quotes inside a string literal intact', () => {
  // Regression: the escaped char after a backslash was dropped, turning
  // "= contains(str,\"b\")" into a literal with a \b (backspace) control char.
  const [arg] = splitArgs('"= contains(str,\\"b\\")"');
  assert.equal(arg, '"= contains(str,\\"b\\")"');
  assert.equal(resolveArg(arg), '= contains(str,"b")');
});

test('splitArgs splits top-level commas, respecting nesting and quotes', () => {
  assert.deepEqual(splitArgs('"a", "b"'), ['"a"', '"b"']);
  assert.deepEqual(splitArgs('"a,b", f(x, y)'), ['"a,b"', 'f(x, y)']);
  assert.deepEqual(splitArgs(''), []);
  assert.deepEqual(splitArgs('"id", b -> b.zeebeJobType("x")'), ['"id"', 'b -> b.zeebeJobType("x")']);
});

// --- resolveArg --------------------------------------------------------------

test('resolveArg resolves literals, constants and placeholder locals, rejects expressions', () => {
  assert.equal(resolveArg('"hello"'), 'hello');
  assert.equal(resolveArg('"a" + "b"'), 'ab');
  assert.equal(resolveArg('42'), 42);
  assert.equal(resolveArg('true'), true);
  assert.equal(resolveArg('PROCESS_ID', { PROCESS_ID: 'process' }), 'process');
  // a lowercase identifier resolves to a placeholder (its own name): a runtime
  // value like `jobType`, opaque to the harvested model (#1524)
  assert.equal(resolveArg('jobType'), 'jobType');
  // an UNDECLARED uppercase constant stays unresolved — a real fidelity gap
  assert.equal(typeof resolveArg('SOME_CONSTANT'), 'symbol'); // UNRESOLVED
  assert.equal(typeof resolveArg('b -> b.x()'), 'symbol'); // lambda, not a value
  assert.equal(typeof resolveArg('a.b()'), 'symbol'); // a call, not a value
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

test('resolveLambda rejects a nested lambda on an unsupported method', () => {
  // zeebeLinkedResources is not in REPLAYABLE, so its lambda arg stays unresolved.
  assert.equal(typeof resolveLambda('b -> b.zeebeLinkedResources(l -> l.resourceId("x"))'), 'symbol');
});

// --- multi-instance support (#1524) ------------------------------------------
// The defect class: Zeebe's `processing/bpmn/multiinstance/*Test.java` models
// all landed in unparseable.json because the harvester (a) did not know the
// `multiInstance(b -> …)` builder method and its MI config calls, and (b) could
// not resolve lambda args bound to test-class constants/locals (`jobType`) —
// so the MI output-element scoping tests had no harvested coverage (#1518).

test('resolveLambda flattens a multiInstance config lambda (nested one level)', () => {
  const lam = resolveLambda(
    't -> t.zeebeJobType("type1").multiInstance(b -> b.zeebeInputCollectionExpression("items").zeebeInputElement("item"))');
  assert.deepEqual(lam, [
    { m: 'zeebeJobType', args: ['type1'] },
    { m: 'multiInstance', args: [] },
    { m: 'zeebeInputCollectionExpression', args: ['items'] },
    { m: 'zeebeInputElement', args: ['item'] },
    { m: 'pop', args: [] }, // return to the enclosing activity builder
  ]);
});

test('resolveLambda resolves constant-bound args inside a multiInstance lambda', () => {
  const constants = { INPUT_COLLECTION: 'items', INPUT_ELEMENT: 'item', JOB_TYPE: 'type1' };
  const lam = resolveLambda(
    't -> t.zeebeJobType(JOB_TYPE).multiInstance(b -> b.parallel().zeebeInputCollectionExpression(INPUT_COLLECTION).zeebeInputElement(INPUT_ELEMENT).zeebeOutputElementExpression("result").zeebeOutputCollection("results"))',
    constants);
  assert.deepEqual(lam, [
    { m: 'zeebeJobType', args: ['type1'] },
    { m: 'multiInstance', args: [] },
    { m: 'parallel', args: [] },
    { m: 'zeebeInputCollectionExpression', args: ['items'] },
    { m: 'zeebeInputElement', args: ['item'] },
    { m: 'zeebeOutputElementExpression', args: ['result'] },
    { m: 'zeebeOutputCollection', args: ['results'] },
    { m: 'pop', args: [] },
  ]);
});

test('resolveLambda resolves a lambda arg bound to a test-class local (jobType)', () => {
  // `jobType` is a non-constant identifier (an instance field / local in the
  // Zeebe test). Its exact value is irrelevant to the harvested MODEL — the
  // scenario driver completes jobs by element id — so it resolves to a stable
  // placeholder rather than making the chain unparseable.
  const lam = resolveLambda('t -> t.zeebeJobType(jobType)');
  assert.deepEqual(lam, [{ m: 'zeebeJobType', args: ['jobType'] }]);
});

test('resolveLambda still rejects a lambda whose body is not a flat chain', () => {
  // a helper-method call on the lambda parameter (miBuilder.accept(m)) is not a
  // builder call and must stay unresolved — replaying it would invent surface.
  assert.equal(
    typeof resolveLambda('m -> { m.zeebeInputCollectionExpression("items"); miBuilder.accept(m); }'),
    'symbol');
});

test('parseChain harvests a multi-instance service task (Zeebe MultiInstanceActivityTest shape)', () => {
  const src = `
    private static final String PROCESS_ID = "process";
    private static final String ELEMENT_ID = "task";
    private static final String INPUT_COLLECTION_EXPRESSION = "items";
    private static final String INPUT_ELEMENT_VARIABLE = "item";
    private static final String OUTPUT_COLLECTION_VARIABLE = "results";
    private static final String OUTPUT_ELEMENT_EXPRESSION = "result";
    BpmnModelInstance m = Bpmn.createExecutableProcess(PROCESS_ID)
        .startEvent()
        .serviceTask(
            ELEMENT_ID,
            t -> t.zeebeJobType(jobType).multiInstance(
                b -> b.zeebeInputCollectionExpression(INPUT_COLLECTION_EXPRESSION)
                    .zeebeInputElement(INPUT_ELEMENT_VARIABLE)
                    .zeebeOutputElementExpression(OUTPUT_ELEMENT_EXPRESSION)
                    .zeebeOutputCollection(OUTPUT_COLLECTION_VARIABLE)))
        .endEvent()
        .done();
  `;
  const clean = stripComments(src);
  const chain = parseChain(clean, clean.indexOf('Bpmn.createExecutableProcess'), collectConstants(clean));
  assert.equal(chain.processId, 'process');
  assert.equal(chain.reason, null);
  assert.deepEqual(chain.calls.map((c) => c.m), [
    'startEvent', 'serviceTask', 'zeebeJobType', 'multiInstance',
    'zeebeInputCollectionExpression', 'zeebeInputElement',
    'zeebeOutputElementExpression', 'zeebeOutputCollection', 'pop', 'endEvent',
  ]);
  assert.deepEqual(chain.calls[1], { m: 'serviceTask', args: ['task'] });
  assert.deepEqual(chain.calls[4], { m: 'zeebeInputCollectionExpression', args: ['items'] });
});

test('parseChain harvests a multi-instance sub-process (sequential, expression collection)', () => {
  const src = `
    BpmnModelInstance m = Bpmn.createExecutableProcess("process")
        .startEvent()
        .subProcess("subprocess")
        .multiInstance(
            m -> m.sequential().zeebeInputCollectionExpression("=[1,2,3]").zeebeInputElement("item"))
        .embeddedSubProcess()
        .startEvent()
        .userTask("A")
        .endEvent()
        .done();
  `;
  const clean = stripComments(src);
  const chain = parseChain(clean, clean.indexOf('Bpmn.createExecutableProcess'), {});
  assert.equal(chain.reason, null);
  assert.deepEqual(chain.calls.map((c) => c.m), [
    'startEvent', 'subProcess', 'multiInstance', 'sequential',
    'zeebeInputCollectionExpression', 'zeebeInputElement', 'pop',
    'embeddedSubProcess', 'startEvent', 'userTask', 'endEvent',
  ]);
});

test('parseChain harvests a receive task with a message lambda AND a multiInstance lambda', () => {
  const src = `
    private static final String MESSAGE_NAME = "test";
    private static final String INPUT_ELEMENT = "item";
    BpmnModelInstance m = Bpmn.createExecutableProcess("process")
        .startEvent()
        .receiveTask(
            "task",
            t -> t.message(m -> m.name(MESSAGE_NAME).zeebeCorrelationKeyExpression(INPUT_ELEMENT))
                .multiInstance(
                    b -> b.zeebeInputCollectionExpression("items").zeebeInputElement(INPUT_ELEMENT)))
        .endEvent()
        .done();
  `;
  const clean = stripComments(src);
  const chain = parseChain(clean, clean.indexOf('Bpmn.createExecutableProcess'), collectConstants(clean));
  assert.equal(chain.reason, null);
  assert.deepEqual(chain.calls.map((c) => c.m), [
    'startEvent', 'receiveTask', 'message', 'name', 'zeebeCorrelationKeyExpression', 'pop',
    'multiInstance', 'zeebeInputCollectionExpression', 'zeebeInputElement', 'pop', 'endEvent',
  ]);
  assert.deepEqual(chain.calls[3], { m: 'name', args: ['test'] });
});

test('collectConstants picks up non-static final locals bound to literals', () => {
  const src = `
    final String processId = "processId";
    final var jobType = "nestedMiJobType";
    BpmnModelInstance m = Bpmn.createExecutableProcess(processId).startEvent().endEvent().done();
  `;
  const clean = stripComments(src);
  const c = collectConstants(clean);
  assert.equal(c.processId, 'processId');
  assert.equal(c.jobType, 'nestedMiJobType');
  const chain = parseChain(clean, clean.indexOf('Bpmn.createExecutableProcess'), c);
  assert.equal(chain.processId, 'processId');
  assert.equal(chain.reason, null);
});

// --- named Consumer fields + .andThen composition (#1524) ---------------------
// The Zeebe MI tests bind the shared MI config to a
// `static final Consumer<MultiInstanceLoopCharacteristicsBuilder>
// INPUT_VARIABLE_BUILDER = m -> m.zeebeInput…;` and reference it by name (or
// `.andThen(...)`-compose it) in the chain. The harvester resolves the named
// field and the composition to the flat MI config calls.

test('parseChain resolves a named Consumer field as the multiInstance argument', () => {
  const src = `
    private static final String PROCESS_ID = "process";
    private static final Consumer<MultiInstanceLoopCharacteristicsBuilder> MI =
        m -> m.zeebeInputCollectionExpression("items").zeebeInputElement("item");
    BpmnModelInstance p = Bpmn.createExecutableProcess(PROCESS_ID)
        .startEvent()
        .serviceTask("task", t -> t.zeebeJobType(jobType).multiInstance(MI))
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.equal(chains.length, 1);
  assert.equal(chains[0].reason, null);
  assert.deepEqual(chains[0].calls.map((c) => c.m), [
    'startEvent', 'serviceTask', 'zeebeJobType', 'multiInstance',
    'zeebeInputCollectionExpression', 'zeebeInputElement', 'pop', 'endEvent',
  ]);
});

test('parseChain resolves a named Consumer .andThen(inline lambda) composition', () => {
  const src = `
    private static final Consumer<MultiInstanceLoopCharacteristicsBuilder> MI =
        m -> m.zeebeInputCollectionExpression("items");
    BpmnModelInstance p = Bpmn.createExecutableProcess("process")
        .startEvent()
        .serviceTask("task", t -> t.zeebeJobType(jobType).multiInstance(MI.andThen(m -> m.parallel())))
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.equal(chains[0].reason, null);
  assert.deepEqual(chains[0].calls.map((c) => c.m), [
    'startEvent', 'serviceTask', 'zeebeJobType', 'multiInstance',
    'zeebeInputCollectionExpression', 'parallel', 'pop', 'endEvent',
  ]);
});

test('parseChain leaves a runtime-parameterised Consumer composition unparseable', () => {
  // `INPUT_VARIABLE_BUILDER.andThen(builder)` where `builder` is a method
  // PARAMETER (the runtime parallel/sequential customizer in the Zeebe
  // parameterized test) has no static value — the chain stays unparseable
  // rather than invent behaviour. This is why MultiInstanceActivityTest's
  // process(miBuilder) helper models are not harvested (#1524 follow-up).
  const src = `
    private static final Consumer<MultiInstanceLoopCharacteristicsBuilder> IVB =
        m -> m.zeebeInputCollectionExpression("items");
    BpmnModelInstance p = Bpmn.createExecutableProcess("process")
        .startEvent()
        .serviceTask("task", t -> t.zeebeJobType(jobType).multiInstance(IVB.andThen(builder)))
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.match(chains[0].reason, /^unresolved-arg:serviceTask/);
});

// --- subProcess Consumer scope restoration (#1714 review) --------------------
// The defect class: a flattened `subProcess(id, sp -> …)` Consumer body left the
// replay inside the SubProcessBuilder scope, so the outer `.endEvent()` chained
// off the body's LAST end event — an outgoing flow from an end event, an invalid
// model rejected at deployment (ContainerElementVariablePropagationTest-8/-9/-10).
// `subProcess` is a scope-opening method: the Consumer body is followed by a
// `pop` that restores the enclosing builder, exactly like multiInstance/message.

test('parseChain restores the enclosing scope after a subProcess Consumer body', () => {
  // The ContainerElementVariablePropagationTest-10 shape: the sub-process body
  // ends with `.endEvent()` and the OUTER chain continues with `.endEvent()`.
  // Without the scope-closing `pop` the outer endEvent replays inside `sp`.
  const src = `
    BpmnModelInstance m = Bpmn.createExecutableProcess("processId")
        .startEvent()
        .subProcess("sp", sp -> sp
            .zeebeInputExpression("={p:0}", "a")
            .zeebeOutputExpression("1", "a.b")
            .embeddedSubProcess()
            .startEvent()
            .endEvent())
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.equal(chains.length, 1);
  assert.equal(chains[0].reason, null);
  assert.deepEqual(chains[0].calls.map((c) => c.m), [
    'startEvent', 'subProcess', 'zeebeInputExpression', 'zeebeOutputExpression',
    'embeddedSubProcess', 'startEvent', 'endEvent', 'pop', 'endEvent',
  ]);
});

test('parseChain pops the subProcess scope before an outer moveTo* navigation call', () => {
  // Same class, different continuation: the outer chain navigates after the
  // sub-process body. The `pop` must precede it so the navigation targets the
  // process scope, not the sub-process body.
  const src = `
    BpmnModelInstance m = Bpmn.createExecutableProcess("processId")
        .startEvent()
        .subProcess("sp", sp -> sp.embeddedSubProcess().startEvent().endEvent())
        .moveToNode("start")
        .serviceTask("task", t -> t.zeebeJobType("type1"))
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.equal(chains[0].reason, null);
  assert.deepEqual(chains[0].calls.map((c) => c.m), [
    'startEvent', 'subProcess', 'embeddedSubProcess', 'startEvent', 'endEvent',
    'pop', 'moveToNode', 'serviceTask', 'zeebeJobType', 'endEvent',
  ]);
});

test('parseChain resolves a named Consumer field as the subProcess argument', () => {
  // Same scope-restoration class, Consumer-field form: `subProcess("sp", BODY)`
  // where BODY is a collected `Consumer<SubProcessBuilder>` field. The flattened
  // body must still close with `pop` before the outer chain continues.
  const src = `
    private static final Consumer<SubProcessBuilder> BODY =
        sp -> sp.embeddedSubProcess().startEvent().endEvent();
    BpmnModelInstance p = Bpmn.createExecutableProcess("process")
        .startEvent()
        .subProcess("sp", BODY)
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.equal(chains[0].reason, null);
  assert.deepEqual(chains[0].calls.map((c) => c.m), [
    'startEvent', 'subProcess', 'embeddedSubProcess', 'startEvent', 'endEvent',
    'pop', 'endEvent',
  ]);
});

test('parseChain resolves a Consumer .andThen composition as the subProcess argument', () => {
  // `subProcess("sp", BODY.andThen(sp -> …))`: the composition's parts flatten
  // in order and the scope closes once, after the last part.
  const src = `
    private static final Consumer<SubProcessBuilder> BODY =
        sp -> sp.embeddedSubProcess();
    BpmnModelInstance p = Bpmn.createExecutableProcess("process")
        .startEvent()
        .subProcess("sp", BODY.andThen(sp -> sp.startEvent().endEvent()))
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.equal(chains[0].reason, null);
  assert.deepEqual(chains[0].calls.map((c) => c.m), [
    'startEvent', 'subProcess', 'embeddedSubProcess', 'startEvent', 'endEvent',
    'pop', 'endEvent',
  ]);
});

test('parseChain keeps a helper-call Consumer argument on subProcess unparseable', () => {
  // `subProcess("sp", customize(sp))`: the Consumer body is a helper call the
  // harvester cannot see into, so the scope would never be restored — the chain
  // stays unparseable rather than emit a `pop` that underflows the replay stack.
  const src = `
    BpmnModelInstance p = Bpmn.createExecutableProcess("process")
        .startEvent()
        .subProcess("sp", customize(sp))
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.match(chains[0].reason, /^unresolved-arg:subProcess/);
});

test('parseChain flattens a multiInstance scope nested inside a subProcess Consumer', () => {
  // The ContainerElementVariablePropagationTest MI shape: the subProcess
  // Consumer body opens a FURTHER scope (multiInstance with its own config
  // lambda). Both scopes close, in order: the MI pop before the rest of the
  // body, the subProcess pop before the outer chain continues.
  const src = `
    BpmnModelInstance m = Bpmn.createExecutableProcess("processId")
        .startEvent()
        .subProcess("sp", sp -> sp
            .multiInstance(mi -> mi.sequential().zeebeInputCollectionExpression("=[1]"))
            .embeddedSubProcess()
            .startEvent()
            .endEvent())
        .endEvent()
        .done();
  `;
  const chains = parseFile(src);
  assert.equal(chains[0].reason, null);
  assert.deepEqual(chains[0].calls.map((c) => c.m), [
    'startEvent', 'subProcess', 'multiInstance', 'sequential',
    'zeebeInputCollectionExpression', 'pop', 'embeddedSubProcess', 'startEvent',
    'endEvent', 'pop', 'endEvent',
  ]);
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
  // An UPPERCASE undeclared identifier names a constant the harvester failed to
  // resolve — a real fidelity gap, so the chain stays unparseable. (A lowercase
  // identifier resolves to a placeholder — see resolveArg — so it does not
  // exercise this path.)
  const src = 'Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();';
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

test('scenarioFor tags a multi-instance model as needing variables (driver limitation)', () => {
  // A multi-instance service task carries a multiInstanceLoopCharacteristics
  // child; its input collection must be supplied as a variable at create, which
  // the empty-variable runner cannot do — so it routes to out-of-scope
  // (driver limitation), never to unverified (#1524).
  const xml = `<?xml version="1.0"?>
  <definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:ns0="http://camunda.org/schema/zeebe/1.0">
    <process id="process" isExecutable="true">
      <startEvent id="start"><outgoing>f1</outgoing></startEvent>
      <serviceTask id="task">
        <extensionElements><ns0:taskDefinition type="type1"/></extensionElements>
        <multiInstanceLoopCharacteristics id="mi1">
          <extensionElements><ns0:loopCharacteristics inputCollection="=items" inputElement="item"/></extensionElements>
        </multiInstanceLoopCharacteristics>
        <incoming>f1</incoming><outgoing>f2</outgoing>
      </serviceTask>
      <endEvent id="end"><incoming>f2</incoming></endEvent>
      <sequenceFlow id="f1" sourceRef="start" targetRef="task"/>
      <sequenceFlow id="f2" sourceRef="task" targetRef="end"/>
    </process>
  </definitions>`;
  const g = bpmnToGraph(xml);
  g.processId = 'process';
  const { scenario, outOfScope } = scenarioFor(g);
  assert.equal(outOfScope, null);
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
