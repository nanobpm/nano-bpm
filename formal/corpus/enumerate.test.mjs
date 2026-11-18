// Unit tests for the bounded-exhaustive enumeration (#1246) and the pure parts
// of its runner. No JVM, no wasm: the TLC and runtime phases run in CI jobs.
import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  enumerateGraphs,
  enumerateSkeletons,
  canonicalKey,
  materialize,
  wellFormed,
  routesFor,
  exploreOrders,
  scenariosFor,
  artifactsFor,
  flowCount
} from './enumerate.mjs'
import {
  parseArgs,
  resolveBound,
  verdictFromLog,
  compareVerdicts,
  shrink,
  classify,
  errorClass,
  loadDivergences,
  readSpec
} from './exhaustive.mjs'
import { routeVariable, messageName, CORRELATION_KEY_VARIABLE, validateGraphs } from './generate.mjs'

const graphOf = (kinds, edges) => materialize({ kinds, edges })

test('the enumeration size is pinned for small bounds (drift guard on the well-formedness rules)', () => {
  const count = (gateways, flows) => enumerateGraphs({ gateways, flows }).length
  // S->E and S->T->E.
  assert.equal(count(0, 2), 2)
  assert.equal(count(1, 5), 30)
  assert.equal(count(2, 5), 93)
})

test('every enumerated graph is well-formed, within the bound, and connected S -> E', () => {
  const bound = { gateways: 2, flows: 6 }
  for (const { skel, graph } of enumerateGraphs(bound)) {
    assert.ok(skel.kinds.length <= bound.gateways)
    assert.ok(flowCount(skel) <= bound.flows)
    assert.equal(graph.edges.length, flowCount(skel))
    const out = {}; const inn = {}
    for (const n of Object.keys(graph.nodes)) { out[n] = []; inn[n] = [] }
    for (const e of graph.edges) { out[e.from].push(e.to); inn[e.to].push(e.from) }
    const reach = (from, adj) => {
      const seen = new Set([from]); const st = [from]
      while (st.length) for (const m of adj[st.pop()]) if (!seen.has(m)) { seen.add(m); st.push(m) }
      return seen
    }
    const all = Object.keys(graph.nodes).length
    assert.equal(reach('S', out).size, all, `${graph.key}: every node reachable from S`)
    assert.equal(reach('E', inn).size, all, `${graph.key}: every node reaches E`)
    for (const [n, kind] of Object.entries(graph.nodes)) {
      if (['task', 'msg'].includes(kind)) assert.deepEqual([inn[n].length, out[n].length], [1, 1])
      if (kind === 'msg') assert.equal(graph.nodes[inn[n][0]], 'evt', 'a catch event follows an event-based gateway')
      if (kind === 'evt') assert.ok(out[n].every((m) => graph.nodes[m] === 'msg'))
    }
  }
})

test('isomorphism reduction: one graph per class, and relabelling gateways is invisible', () => {
  const graphs = enumerateGraphs({ gateways: 2, flows: 6 })
  assert.equal(new Set(graphs.map((g) => g.graph.key)).size, graphs.length)
  assert.equal(new Set(graphs.map((g) => g.graph.id)).size, graphs.length)
  // An and-split G1 into an and-join G2 — and the same graph numbered the
  // other way round (topological numbering is not unique in general).
  const a = { kinds: ['and', 'xor'], edges: [[0, 1, 'direct'], [1, 3, 'task'], [1, 3, 'direct'], [2, 3, 'direct']] }
  const b = { kinds: ['xor', 'and'], edges: [[0, 2, 'direct'], [2, 3, 'task'], [2, 3, 'direct'], [1, 3, 'direct']] }
  assert.equal(canonicalKey(a).key, canonicalKey(b).key)
  assert.deepEqual(materialize(a), materialize(b))
})

test('well-formedness rejects mixed / no-op gateways and misplaced message edges', () => {
  assert.equal(wellFormed(['xor'], [[0, 1, 'direct'], [1, 2, 'direct']]), false, '1-in-1-out no-op')
  assert.equal(wellFormed(['evt'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'msg']]), true)
  assert.equal(wellFormed(['evt'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'task']]), false)
  assert.equal(wellFormed(['xor'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'msg']]), false)
  assert.equal(wellFormed([], [[0, 1, 'direct'], [0, 1, 'task']]), false, 'S has one out-flow')
})

test('the gateway kinds of the bound are honoured', () => {
  for (const skel of enumerateSkeletons({ gateways: 2, flows: 5, kinds: ['and'] })) {
    assert.ok(skel.kinds.every((k) => k === 'and'))
  }
  assert.throws(() => [...enumerateSkeletons({ gateways: 1, flows: 3, kinds: ['complex'] })], /unknown gateway kind/)
})

test('enumerated graphs are rejected by the deterministic-route corpus validator only for evt/msg', () => {
  const evt = graphOf(['evt'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'msg']])
  assert.throws(() => validateGraphs([evt]), /evt\/msg are generated only by the variable-routed enumeration/)
})

const parallelDiamond = () => graphOf(['and', 'and'], [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'task'], [2, 3, 'direct']])

test('reference: a parallel diamond has both completion orders, each completing', () => {
  const g = parallelDiamond()
  assert.deepEqual(routesFor(g), [{}])
  const runs = exploreOrders(g, {})
  assert.deepEqual(runs.map((r) => r.steps.map((s) => s.node)), [['T1', 'T2'], ['T2', 'T1']])
  assert.ok(runs.every((r) => r.completed && r.oneSafe))
  assert.deepEqual(runs[0].jobsCreated, { T1: 1, T2: 1 })
})

test('reference: an exclusive split into a parallel join gets stuck, on every route', () => {
  const g = graphOf(['xor', 'and'], [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'direct'], [2, 3, 'direct']])
  const routes = routesFor(g)
  assert.equal(routes.length, 2)
  for (const r of routes) {
    for (const run of exploreOrders(g, r)) assert.equal(run.completed, false)
  }
})

test('reference: an inclusive split routes every non-empty subset and its join waits for each', () => {
  const g = graphOf(['or', 'or'], [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'task'], [2, 3, 'direct']])
  const routes = routesFor(g)
  assert.deepEqual(routes.map((r) => r[routeVariable('G1')].length).sort(), [1, 1, 2])
  for (const r of routes) {
    for (const run of exploreOrders(g, r)) {
      assert.equal(run.completed, true)
      assert.equal(run.steps.length, r[routeVariable('G1')].length)
    }
  }
})

test('reference: an event-based gateway is a wait state resolved by one correlation', () => {
  const g = graphOf(['evt'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'msg']])
  const runs = exploreOrders(g, {})
  assert.deepEqual(runs.map((r) => r.steps.map((s) => s.catch)), [['M1'], ['M2']])
  assert.ok(runs.every((r) => r.completed))
})

test('scenarios: one per route and command order, from the one graph source', () => {
  const g = graphOf(['evt'], [[0, 1, 'task'], [1, 2, 'msg'], [1, 2, 'msg']])
  const { scenarios, skipped } = scenariosFor(g)
  assert.equal(skipped, 0)
  assert.equal(scenarios.length, 2)
  const [s] = scenarios
  assert.equal(s.processId, g.id)
  assert.equal(s.variables[CORRELATION_KEY_VARIABLE], s.name, 'each scenario has its own correlation key')
  assert.deepEqual(s.steps, [
    { op: 'activateAndComplete', jobType: 'T1' },
    { op: 'publishMessage', name: messageName(g.id, 'M1'), correlationKey: s.name }
  ])
  assert.deepEqual(s.expect, { completed: true, jobsCreated: { T1: 1 }, variables: s.variables })
  const stuck = scenariosFor(graphOf(['xor', 'and'], [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'direct'], [2, 3, 'direct']]))
  assert.ok(stuck.scenarios.every((sc) => sc.expect.completed === false && sc.backends?.join() === 'nano'))
})

test('artifacts: variable-routed BPMN with DI, messages, and MC/ZMC models', () => {
  const g = graphOf(['evt', 'xor'], [[0, 2, 'direct'], [2, 1, 'direct'], [2, 3, 'task'], [1, 3, 'msg'], [1, 3, 'msg']])
  const a = artifactsFor(g)
  assert.match(a.bpmn, /<bpmn:eventBasedGateway /)
  assert.match(a.bpmn, /<bpmn:messageEventDefinition messageRef="Message_M1"\/>/)
  assert.match(a.bpmn, new RegExp(`correlationKey="=${CORRELATION_KEY_VARIABLE}"`))
  assert.match(a.bpmn, new RegExp(`=${routeVariable("G")}\\d = &quot;f\\d&quot;`))
  for (const n of Object.keys(g.nodes)) assert.match(a.bpmn, new RegExp(`bpmnElement="${n}"`), `DI shape for ${n}`)
  assert.match(a.tla.TokenFlow, new RegExp(`MODULE MC${g.id} `))
  assert.match(a.tla.ZeebeTokenFlow, new RegExp(`MODULE ZMC${g.id} `))
  assert.match(a.tla.TokenFlow, /"evt"/)
})

test('runner arguments and named bounds', () => {
  assert.deepEqual(resolveBound(parseArgs(['--bound', 'ci']), { ci: { gateways: 2, flows: 5 } }),
    { gateways: 2, flows: 5, kinds: ['xor', 'and', 'or', 'evt'] })
  assert.deepEqual(resolveBound(parseArgs(['--bound', 'ci', '--flows', '7', '--kinds', 'and']), { ci: { gateways: 2, flows: 5 } }),
    { gateways: 2, flows: 7, kinds: ['and'] })
  assert.throws(() => resolveBound(parseArgs(['--bound', 'nope']), {}), /unknown --bound/)
  assert.throws(() => resolveBound(parseArgs([]), {}), /non-negative integer gateways/)
  assert.throws(() => parseArgs(['--require-camunda']), /needs --backend/)
  // The committed bounds file names the CI and nightly bounds.
  for (const name of ['ci', 'nightly']) resolveBound(parseArgs(['--bound', name]))
})

test('TLC verdicts parse like check.sh and compare in the Zeebe vocabulary', () => {
  const props = readSpec('TokenFlow').properties
  assert.deepEqual(props, ['Termination'])
  const ok = 'Model checking completed. No error has been found.\n'
  assert.equal(verdictFromLog(ok, props, 0), 'pass')
  const bad = 'Error: Invariant NoStuckInstance is violated.\nError: Temporal properties were violated.\n10 states generated, 5 distinct states found, 0 states left on queue.\n'
  assert.equal(verdictFromLog(bad, props, 12), 'violates:NoStuckInstance,Termination')
  assert.equal(verdictFromLog(bad.replace(' 0 states left', ' 3 states left'), props, 12), 'error', 'an incomplete exploration is no verdict')
  assert.equal(verdictFromLog('Warning: x\n' + ok, props, 0), 'error')

  assert.deepEqual(compareVerdicts('pass', 'pass'), [])
  assert.deepEqual(compareVerdicts('violates:JoinBookkeepingCoherent', 'violates:TakenBookkeepingCoherent'), [])
  const [refines] = compareVerdicts('violates:RefinesZeebe', 'pass')
  assert.equal(refines.kind, 'tlc-refines')
  const [verdict] = compareVerdicts('pass', 'violates:NoStuckInstance,Termination')
  assert.equal(verdict.signature, 'tlc-verdict:nano=pass;zeebe=NoStuckInstance,Termination')
})

test('shrinking keeps the smallest graph and then the shortest scenario per divergence class', () => {
  const e = (key) => ({ graph: { key } })
  const sc = (n) => ({ steps: new Array(n).fill({}) })
  const order = new Map([['a', 0], ['b', 1]])
  const shrunk = shrink([
    { signature: 'x', entry: e('b'), scenario: sc(1) },
    { signature: 'x', entry: e('a'), scenario: sc(3) },
    { signature: 'x', entry: e('a'), scenario: sc(2) },
    { signature: 'y', entry: e('b') }
  ], order)
  assert.deepEqual(shrunk.map((d) => [d.signature, d.entry.graph.key, d.scenario?.steps.length]), [['x', 'a', 2], ['y', 'b', undefined]])
})

test('the divergence corpus: fresh classes fail, stale and unfiled committed cases fail', () => {
  const committed = [
    { signature: 'known', issue: '#1', graph: { key: 'k1' } },
    { signature: 'gone', issue: '#2', graph: { key: 'k2' } },
    { signature: 'outside', issue: '#3', graph: { key: 'k3' } },
    { signature: 'unfiled', issue: null, graph: { key: 'k4' } }
  ]
  const inScope = (c) => c.graph.key !== 'k3'
  const r = classify([{ signature: 'known' }, { signature: 'new' }, { signature: 'unfiled' }], committed, inScope)
  assert.deepEqual(r.known.map((d) => d.signature), ['known', 'unfiled'])
  assert.deepEqual(r.fresh.map((d) => d.signature), ['new'])
  assert.deepEqual(r.stale.map((c) => c.signature), ['gone'])
  assert.deepEqual(r.unfiled.map((c) => c.signature), ['unfiled'])
})

test('the committed divergence corpus is well-formed: every case is filed and re-materialisable', () => {
  for (const c of loadDivergences()) {
    assert.match(c.issue ?? '', /^#\d+$/, `${c.signature} needs a filed issue`)
    assert.ok(c.graph?.key && c.graph.id, `${c.signature} carries its minimal graph`)
  }
})

test('error classes mask ids and numbers so one defect is one signature', () => {
  assert.equal(errorClass(new Error("no 'T1' job became activatable within 30000ms\nmore")),
    errorClass(new Error("no 'T7' job became activatable within 5ms")))
})
