// The differential fuzzer's extended vocabulary (#1673): embedded subprocesses,
// interrupting timer / message boundary events, timer catch events with clock
// ticks, and instance cancellation. Pure (no engine): the Reference semantics,
// the skeleton rules, the BPMN emitter and the scenario steps. The nano
// end-to-end run of the same graphs lives in
// formal/parity/runtime/vocabulary.test.mjs (it needs the engine-wasm pkg).

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  EDGE_LABELS,
  LABEL_CODE,
  enumerateSkeletons,
  wellFormed,
  materialize,
  exploreOrders,
  randomRun,
  scenariosFor,
  Reference
} from './enumerate.mjs'
import { bpmnFor, TIMER_TICK_MS, TIMER_DURATION, messageName } from './generate.mjs'
import { parseArgs, randomSkeleton, reductions, picker, WAIT_LABELS } from './fuzz.mjs'
import { prng } from '../traces/fuzz/generate.mjs'

const graphOf = (edges, kinds = []) => materialize({ kinds, edges })

test('every edge label has a distinct key letter (graph keys/ids cannot collide)', () => {
  assert.deepEqual(Object.keys(LABEL_CODE).sort(), [...EDGE_LABELS].sort())
  assert.equal(new Set(Object.values(LABEL_CODE)).size, EDGE_LABELS.length)
  // A timer catch and a task on the same edge are different graphs.
  assert.notEqual(graphOf([[0, 1, 'timer']]).key, graphOf([[0, 1, 'task']]).key)
  assert.notEqual(graphOf([[0, 1, 'btimer']]).key, graphOf([[0, 1, 'bmsg']]).key)
})

test('the default enumeration vocabulary is the legacy one (committed corpus unchanged)', () => {
  for (const s of enumerateSkeletons({ gateways: 1, flows: 4 })) {
    for (const [, , l] of s.edges) assert.ok(['direct', 'task', 'msg'].includes(l), `unexpected label ${l}`)
  }
})

test('enumeration with extended labels: a boundary edge only targets a merge-capable node', () => {
  let boundaries = 0
  for (const s of enumerateSkeletons({ gateways: 1, flows: 4, labels: ['direct', 'task', 'btimer', 'bmsg', 'timer', 'sub'] })) {
    assert.ok(wellFormed(s.kinds, s.edges))
    for (const [, j, l] of s.edges) {
      if (l !== 'btimer' && l !== 'bmsg') continue
      boundaries++
      const E = s.kinds.length + 1
      assert.ok(j === E || s.kinds[j - 1] !== 'evt', 'boundary into an event-based gateway')
    }
  }
  assert.ok(boundaries > 0, 'the extended enumeration produces boundary edges')
  assert.throws(() => [...enumerateSkeletons({ gateways: 0, flows: 2, labels: ['nope'] })], /unknown edge label/)
})

test('timer catch: a wait state the clock tick fires', () => {
  const g = graphOf([[0, 1, 'timer']])
  const runs = exploreOrders(g)
  assert.equal(runs.length, 1)
  const [run] = runs
  assert.deepEqual(run.steps.map((c) => c.op), ['tick'])
  assert.equal(run.completed, true)
  assert.equal(run.completedElements.W1, 1)
  const [sc] = scenariosFor(g, runs).scenarios
  assert.deepEqual(sc.steps, [{ op: 'advanceTime', ms: TIMER_TICK_MS }])
})

test('timer boundary: the tick interrupts the task (terminated, never completed)', () => {
  const g = graphOf([[0, 1, 'btimer']])
  const runs = exploreOrders(g)
  const byOps = Object.fromEntries(runs.map((r) => [r.steps.map((c) => c.op).join(','), r]))
  assert.deepEqual(Object.keys(byOps).sort(), ['complete', 'tick'])
  const fired = byOps.tick
  assert.equal(fired.completed, true)
  assert.equal(fired.jobsCreated.T1, 1)
  assert.equal(fired.completedElements.T1, undefined, 'an interrupted task never completes')
  assert.equal(fired.completedElements.B1, 1)
  assert.equal(fired.completedElements.E, 1)
  // Completing the task first withdraws the timer: no tick is left to offer.
  const done = byOps.complete
  assert.equal(done.completedElements.T1, 1)
  assert.equal(done.completedElements.B1, undefined)
})

test('message boundary: correlation interrupts the task; scenario publishes its message', () => {
  const g = graphOf([[0, 1, 'bmsg']])
  const runs = exploreOrders(g)
  const fired = runs.find((r) => r.steps[0].op === 'correlateBoundary')
  assert.ok(fired)
  assert.equal(fired.completedElements.T1, undefined)
  assert.equal(fired.completedElements.B1, 1)
  const sc = scenariosFor(g, [fired]).scenarios[0]
  assert.equal(sc.steps[0].op, 'publishMessage')
  assert.equal(sc.steps[0].name, messageName(g.id, 'B1'))
  assert.equal(sc.steps[0].correlationKey, sc.variables.correlationKey)
})

test('embedded subprocess: inner start/task/end run in scope; the container completes once', () => {
  const g = graphOf([[0, 1, 'sub']])
  assert.deepEqual({ ...g.scopeOf }, { P1s: 'P1', P1t: 'P1', P1e: 'P1' })
  const runs = exploreOrders(g)
  assert.equal(runs.length, 1)
  const [run] = runs
  assert.deepEqual(run.steps, [{ op: 'complete', node: 'P1t' }])
  assert.equal(run.completed, true)
  for (const n of ['S', 'P1s', 'P1t', 'P1e', 'P1', 'E']) assert.equal(run.completedElements[n], 1, n)
})

test('cancellation is opt-in and ends the run not completed (nano-only scenario)', () => {
  const g = graphOf([[0, 1, 'task']])
  assert.ok(exploreOrders(g).every((r) => !r.cancelled))
  // A run is never cancelled before its first command (one case, not one per graph).
  const g2 = materialize({ kinds: ['and'], edges: [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'task']] })
  const runs = exploreOrders(g2, {}, { cancel: true })
  const cancelled = runs.filter((r) => r.cancelled)
  assert.ok(cancelled.length > 0)
  for (const r of cancelled) {
    assert.equal(r.completed, false)
    assert.equal(r.steps.at(-1).op, 'cancel')
    assert.ok(r.steps.length > 1)
  }
  const sc = scenariosFor(g2, cancelled).scenarios[0]
  assert.deepEqual(sc.steps.at(-1), { op: 'cancelInstance' })
  assert.equal(sc.expect.cancelled, true)
  assert.deepEqual(sc.backends, ['nano'])
  // A cancelled Reference offers no further command.
  const ref = new Reference(g2)
  ref.settle()
  assert.deepEqual(ref.apply({ op: 'cancel' }).commands(), [])
})

test('cancellation with an armed event-based gateway counts the gateway as passed', () => {
  const g = materialize({ kinds: ['evt'], edges: [[0, 1, 'task'], [1, 2, 'msg'], [1, 2, 'msg']] })
  const run = exploreOrders(g, {}, { cancel: true }).find((r) => r.cancelled && r.steps.length === 2)
  assert.ok(run)
  assert.equal(run.completedElements.G1, 1)
  assert.equal(run.completedElements.M1, undefined)
  assert.equal(run.completedElements.M2, undefined)
})

test('randomRun with a cancel rate of 1 cancels on its second step', () => {
  const g = materialize({ kinds: ['and'], edges: [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'task']] })
  const run = randomRun(g, picker(prng(7)), { cancelRate: 1 })
  assert.equal(run.cancelled, true)
  assert.equal(run.steps.length, 2)
})

test('bpmnFor: subprocess nesting, boundary attachment and timer durations (with DI)', () => {
  const g = materialize({ kinds: ['xor'], edges: [[0, 1, 'btimer'], [1, 2, 'sub'], [1, 2, 'bmsg'], [1, 2, 'timer']] })
  const xml = bpmnFor(g, { routing: 'variables' })
  // The inner chain is nested inside its container.
  const sub = xml.slice(xml.indexOf('<bpmn:subProcess id="P1"'), xml.indexOf('</bpmn:subProcess>'))
  for (const inner of ['P1s', 'P1t', 'P1e']) assert.ok(sub.includes(`id="${inner}"`), inner)
  assert.ok(/<bpmn:boundaryEvent id="B1" cancelActivity="true" attachedToRef="T1">/.test(xml))
  assert.ok(/<bpmn:boundaryEvent id="B2" cancelActivity="true" attachedToRef="T2">/.test(xml))
  assert.equal(TIMER_DURATION, 'PT60S')
  assert.equal(xml.split(`>${TIMER_DURATION}</bpmn:timeDuration>`).length - 1, 2, 'one timer boundary + one timer catch')
  assert.ok(xml.includes(`name="${messageName(g.id, 'B2')}"`), 'the message boundary declares its message')
  // Every element and flow has DI: a shape per node, an edge per flow.
  for (const n of Object.keys(g.nodes)) assert.ok(xml.includes(`bpmnElement="${n}"`), `DI shape for ${n}`)
  for (const e of g.edges) assert.ok(xml.includes(`<bpmndi:BPMNEdge id="${e.id}_di" bpmnElement="${e.id}">`), `DI edge for ${e.id}`)
  // The collapsed subprocess has its own plane.
  assert.ok(xml.includes('<bpmndi:BPMNPlane id="P1_plane" bpmnElement="P1">'))
  assert.ok(xml.includes('bpmnElement="P1" isExpanded="false"'))
})

test('fuzz: --labels / --cancel-rate parse and validate', () => {
  assert.deepEqual(parseArgs(['--seed', '1']).labels, WAIT_LABELS)
  assert.deepEqual(parseArgs(['--seed', '1', '--labels', 'task,sub']).labels, ['task', 'sub'])
  assert.equal(parseArgs(['--seed', '1', '--cancel-rate', '0']).cancelRate, 0)
  assert.throws(() => parseArgs(['--seed', '1', '--labels', 'msg']), /unknown wait-state label/)
  assert.throws(() => parseArgs(['--seed', '1', '--cancel-rate', '2']), /--cancel-rate/)
})

test('fuzz: random skeletons draw every extended label and stay well-formed', () => {
  const seen = new Set()
  for (let s = 0; s < 300; s++) {
    const skel = randomSkeleton(picker(prng(s)), {})
    assert.ok(wellFormed(skel.kinds, skel.edges), `seed ${s}`)
    for (const [, , l] of skel.edges) seen.add(l)
    // Every extended skeleton materialises and emits BPMN.
    if (s < 40) bpmnFor(materialize(skel), { routing: 'variables' })
  }
  for (const l of WAIT_LABELS) assert.ok(seen.has(l), `label ${l} drawn`)
})

test('fuzz: a boundary edge shrinks to a task edge, then to a direct one', () => {
  const skel = { kinds: ['xor'], edges: [[0, 1, 'btimer'], [1, 2, 'direct'], [1, 2, 'direct']] }
  const labels = reductions(skel).map((r) => r.edges[0][2])
  assert.ok(labels.includes('task'))
  assert.ok(labels.includes('direct'))
})
