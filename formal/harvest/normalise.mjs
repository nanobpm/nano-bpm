#!/usr/bin/env node
// Normalises the harvested Zeebe engine-test models into the parity corpus
// format (#1249): BPMN with DI (already produced by the Zeebe model API in
// harvest.mjs) plus a scenario script, each tagged against the coverage matrix.
//
//   node formal/harvest/normalise.mjs [--corpus formal/harvest/corpus]
//
// Reads the harvested IR (`corpus/ir/*.json`, each carrying the Zeebe-serialised
// `xml`), and for every model:
//
//   * parses the BPMN into a node/edge graph;
//   * derives the scenario script (`jobs`, `jobCompletionOrder`,
//     `messageCorrelation`, `timerTicks`) from the graph — the same contract the
//     corpus generator (formal/corpus/generate.mjs) emits, so the scenario
//     driver (#1240 slice 5) consumes both corpora uniformly;
//   * tags the model against the Zeebe coverage matrix (`zeebe-cells`) by the
//     BPMN element types it contains;
//   * writes `corpus/bpmn/<id>.bpmn` (the Zeebe-serialised model, DI included)
//     and `corpus/scenarios/<id>.json`.
//
// A model whose graph cannot be normalised (no start event, an element kind the
// scenario contract cannot schedule, a cycle the deterministic route cannot
// complete) is recorded OUT-OF-SCOPE with its reason — it is still harvested,
// just not runnable. The output is a derived artifact: never hand-edit it.

import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const SURFACE = JSON.parse(readFileSync(join(here, '..', 'parity', 'zeebe-surface.json'), 'utf8'));

export class NormaliseError extends Error {}

// ---------------------------------------------------------------------------
// BPMN XML -> graph (a minimal, dependency-free reader over the Zeebe-serialised
// model; the model API emits a stable, predictable shape)
// ---------------------------------------------------------------------------

// BPMN flow-node tag -> the coverage-matrix element cell + the scenario kind.
const ELEMENT_TAGS = {
  startEvent: 'start', endEvent: 'end',
  serviceTask: 'task', sendTask: 'task', receiveTask: 'task',
  userTask: 'task', businessRuleTask: 'task', scriptTask: 'task', manualTask: 'task',
  task: 'task', callActivity: 'task',
  exclusiveGateway: 'xor', parallelGateway: 'and', inclusiveGateway: 'or',
  eventBasedGateway: 'eventBased',
  intermediateCatchEvent: 'intermediateCatch', intermediateThrowEvent: 'intermediateThrow',
  boundaryEvent: 'boundary',
  subProcess: 'subProcess', adHocSubProcess: 'adHocSubProcess',
};

// BPMN element tag -> the Zeebe BpmnElementType name used in coverage cells.
const ELEMENT_CELL = {
  startEvent: 'StartEvent', endEvent: 'EndEvent',
  serviceTask: 'ServiceTask', sendTask: 'SendTask', receiveTask: 'ReceiveTask',
  userTask: 'UserTask', businessRuleTask: 'BusinessRuleTask', scriptTask: 'ScriptTask',
  manualTask: 'ManualTask', task: 'Task', callActivity: 'CallActivity',
  exclusiveGateway: 'ExclusiveGateway', parallelGateway: 'ParallelGateway',
  inclusiveGateway: 'InclusiveGateway', eventBasedGateway: 'EventBasedGateway',
  intermediateCatchEvent: 'IntermediateCatchEvent', intermediateThrowEvent: 'IntermediateThrowEvent',
  boundaryEvent: 'BoundaryEvent', subProcess: 'SubProcess', adHocSubProcess: 'AdHocSubProcess',
};

/** Extract the sequence-flow graph and per-element info from BPMN XML. */
export function bpmnToGraph(xml) {
  // sequence flows
  const edges = [];
  const flowRe = /<sequenceFlow\b[^>]*\bid="([^"]+)"[^>]*\bsourceRef="([^"]+)"[^>]*\btargetRef="([^"]+)"[^>]*>/g;
  let m;
  const condFlows = new Set();
  for (;;) {
    m = flowRe.exec(xml);
    if (!m) break;
    edges.push({ id: m[1], from: m[2], to: m[3] });
  }
  // flows carrying a conditionExpression (block form)
  const condRe = /<sequenceFlow\b[^>]*\bid="([^"]+)"[^>]*>([\s\S]*?)<\/sequenceFlow>/g;
  for (;;) {
    m = condRe.exec(xml);
    if (!m) break;
    if (/<conditionExpression\b/.test(m[2])) condFlows.add(m[1]);
  }

  // flow nodes (self-closing or block), with their job type when present
  const nodes = {};
  const jobTypes = {};
  const events = {};
  for (const tag of Object.keys(ELEMENT_TAGS)) {
    // match both `<tag id="x">...</tag>` and `<tag id="x"/>`
    const re = new RegExp(`<${tag}\\b([^>]*?)(?:/>|>([\\s\\S]*?)</${tag}>)`, 'g');
    for (;;) {
      m = re.exec(xml);
      if (!m) break;
      const attrs = m[1];
      const body = m[2] ?? '';
      const id = /\bid="([^"]+)"/.exec(attrs)?.[1];
      if (!id) continue;
      nodes[id] = ELEMENT_TAGS[tag];
      nodes[`__tag:${id}`] = tag;
      const jt = /<[^>]*taskDefinition\b[^>]*\btype="([^"]+)"/.exec(body)?.[1];
      if (jt) jobTypes[id] = jt;
      // event definitions present on this element
      const defs = [];
      for (const ed of ['messageEventDefinition', 'timerEventDefinition', 'errorEventDefinition',
        'signalEventDefinition', 'escalationEventDefinition', 'conditionalEventDefinition',
        'linkEventDefinition', 'compensateEventDefinition', 'terminateEventDefinition',
        'cancelEventDefinition']) {
        if (new RegExp(`<${ed}\\b`).test(body)) defs.push(ed);
      }
      if (defs.length) events[id] = defs;
    }
  }
  return { nodes, edges, jobTypes, events, condFlows };
}

// ---------------------------------------------------------------------------
// Scenario derivation
// ---------------------------------------------------------------------------

/**
 * Derive the scenario script for a harvested graph. Returns
 *   { scenario, outOfScope: null } on success, or
 *   { scenario: null, outOfScope: reason } when the graph cannot be scheduled.
 */
export function scenarioFor(graph) {
  const nodeIds = Object.keys(graph.nodes).filter((k) => !k.startsWith('__tag:'));
  const starts = nodeIds.filter((id) => graph.nodes[id] === 'start');
  if (starts.length === 0) return { scenario: null, outOfScope: 'no-start-event' };

  // Kahn topological order over the full graph (harvested models are the happy
  // path — Zeebe tests that need specific branch data are tagged, not driven, by
  // this slice; the scenario driver owns data-dependent routing).
  const indeg = Object.create(null);
  const succ = Object.create(null);
  for (const id of nodeIds) indeg[id] = 0;
  for (const e of graph.edges) {
    if (indeg[e.to] === undefined || indeg[e.from] === undefined) continue;
    (succ[e.from] ??= []).push(e.to);
    indeg[e.to] += 1;
  }
  const byId = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
  const ready = nodeIds.filter((id) => indeg[id] === 0);
  const order = [];
  const seen = new Set();
  while (ready.length) {
    ready.sort(byId);
    const id = ready.shift();
    if (seen.has(id)) continue;
    seen.add(id);
    order.push(id);
    for (const to of succ[id] ?? []) if (--indeg[to] === 0) ready.push(to);
  }
  // A node never reached by the topo order is on a cycle the deterministic route
  // cannot complete -> not schedulable by this slice.
  if (seen.size < nodeIds.length) {
    return { scenario: null, outOfScope: 'cyclic-or-unreachable' };
  }

  const jobs = order
    .filter((id) => graph.nodes[id] === 'task')
    .map((id) => ({ element: id, jobType: graph.jobTypes[id] ?? id }));

  // Driver capabilities the scenario needs beyond "complete each Created job
  // with empty variables". A model that carries any of these cannot be driven
  // deterministically by this slice's empty-variable runner; the runner records
  // it out-of-scope (a driver limitation, NOT a Nano divergence) rather than
  // mis-report a data-dependent route as a parity defect.
  const needs = [];
  if (graph.condFlows.size > 0) needs.push('variables'); // conditional routing
  const nodeSet = nodeIds.map((id) => graph.nodes[id]);
  if (nodeSet.includes('eventBased')) needs.push('event-based-gateway');
  if (nodeSet.includes('intermediateCatch') || nodeSet.includes('boundary')) needs.push('catch-event');
  if (nodeSet.includes('subProcess') || nodeSet.includes('adHocSubProcess')) needs.push('sub-process');
  if (Object.keys(graph.events).length > 0) needs.push('event-definition');

  const scenario = {
    generated: 'formal/harvest/normalise.mjs',
    process: graph.processId,
    jobs,
    jobCompletionOrder: jobs.map((j) => j.element),
    messageCorrelation: [],
    timerTicks: [],
    // The driver capabilities this scenario assumes. Empty = the happy-path
    // empty-variable runner can drive it. Non-empty routes the runner to
    // out-of-scope (driver limitation) until the scenario driver (#1240 slice 5)
    // supplies the capability.
    needs,
  };
  return { scenario, outOfScope: null };
}

// ---------------------------------------------------------------------------
// Coverage tagging
// ---------------------------------------------------------------------------

const SURFACE_CELLS = new Set(SURFACE.cells.map((c) => c.id));

/** The coverage-matrix cells a harvested model exercises, by element type. */
export function cellsFor(graph) {
  const cells = new Set();
  const nodeIds = Object.keys(graph.nodes).filter((k) => !k.startsWith('__tag:'));
  for (const id of nodeIds) {
    const tag = graph.nodes[`__tag:${id}`];
    const cellType = ELEMENT_CELL[tag];
    if (cellType) {
      const cell = `element:${cellType}`;
      if (SURFACE_CELLS.has(cell)) cells.add(cell);
    }
    for (const def of graph.events[id] ?? []) {
      // event cells are keyed by the event definition on its carrier position
      const base = def.replace(/EventDefinition$/, '');
      const cell = `event:${tag}:${base}`;
      if (SURFACE_CELLS.has(cell)) cells.add(cell);
    }
  }
  return [...cells].sort();
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

export function normalise(corpusDir) {
  const irDir = join(corpusDir, 'ir');
  const bpmnDir = join(corpusDir, 'bpmn');
  const scenDir = join(corpusDir, 'scenarios');
  mkdirSync(bpmnDir, { recursive: true });
  mkdirSync(scenDir, { recursive: true });

  const normalised = [];
  const outOfScope = [];
  for (const f of readdirSync(irDir).filter((x) => x.endsWith('.json')).sort()) {
    const entry = JSON.parse(readFileSync(join(irDir, f), 'utf8'));
    if (!entry.xml) {
      outOfScope.push({ id: entry.id, source: entry.source, reason: 'no-serialised-xml' });
      continue;
    }
    const graph = bpmnToGraph(entry.xml);
    graph.processId = entry.processId;
    const { scenario, outOfScope: reason } = scenarioFor(graph);
    const cells = cellsFor(graph);
    if (reason) {
      outOfScope.push({ id: entry.id, source: entry.source, reason, cells });
      continue;
    }
    // Stamp the coverage cells into the BPMN as a zeebe-cells comment (the same
    // evidence convention the parity matrix guard reads).
    const stamped = entry.xml.replace(
      /(<definitions\b[^>]*>)/,
      `$1\n  <!-- zeebe-cells: ${cells.join(' ')} -->`);
    writeFileSync(join(bpmnDir, `${entry.id}.bpmn`), stamped);
    writeFileSync(join(scenDir, `${entry.id}.json`), JSON.stringify(scenario, null, 2) + '\n');
    normalised.push({ id: entry.id, source: entry.source, processId: entry.processId, cells });
  }
  return { normalised, outOfScope };
}

function main() {
  const args = process.argv.slice(2);
  const cIdx = args.indexOf('--corpus');
  const corpusDir = cIdx >= 0 ? args[cIdx + 1] : join(here, 'corpus');
  if (!existsSync(join(corpusDir, 'ir'))) {
    console.error(`no harvested IR at ${corpusDir}/ir — run harvest.mjs first`);
    process.exit(2);
  }
  const { normalised, outOfScope } = normalise(corpusDir);
  writeFileSync(join(corpusDir, 'normalised.json'), JSON.stringify(normalised, null, 2) + '\n');
  writeFileSync(join(corpusDir, 'out-of-scope.json'), JSON.stringify(outOfScope, null, 2) + '\n');
  console.log(JSON.stringify({ normalised: normalised.length, outOfScope: outOfScope.length }, null, 2));
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main();
}
