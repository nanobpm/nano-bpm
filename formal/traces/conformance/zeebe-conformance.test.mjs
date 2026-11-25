// Tests for the Zeebe exporter-trace validation runner (#1962).
//
// The pure parts (scenario translation, record projection, decision
// observation, TLC log parsing) are tested directly; the TLA+ side is tested by
// running TLC on fixed milestone traces — the exact shapes the pinned Zeebe
// exported — so the model fix this runner forced (an event-based gateway takes
// no flow to its catch event) is guarded red/green without a Zeebe JVM. Needs
// java and the pinned tla2tools.jar (formal/tla/check.sh fetches it); the
// formal (tlc) CI job runs this after check.sh.
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'

import { loadGraphs } from '../../corpus/generate.mjs'
import { readSpec, tlaJar } from '../../corpus/exhaustive.mjs'
import {
  acceptanceFromLog, captureScript, casesFor, decisionsFromLog, inSpecVocabulary, isCyclic,
  checkCapturedPins, modelDecisions, observedDecisions, parseArgs, pinnedSha, signatureOf, stepMilestones, traceMilestones,
  validateTrace
} from './zeebe-conformance.mjs'

const graph = (id) => loadGraphs().find((g) => g.id === id)

// An event-based gateway choosing between two message catches (the enumerated
// shape `evt|0>1d,1>2m,1>2m` whose real Zeebe trace exposed the model defect).
const EVT = {
  id: 'EvtChoice',
  start: 'S',
  nodes: { S: 'start', G1: 'evt', M1: 'msg', M2: 'msg', E: 'end' },
  edges: [
    { id: 'f1', from: 'S', to: 'G1' },
    { id: 'f2', from: 'G1', to: 'M1' },
    { id: 'f4', from: 'G1', to: 'M2' },
    { id: 'f3', from: 'M1', to: 'E' },
    { id: 'f5', from: 'M2', to: 'E' }
  ]
}

const pi = (intent, elementId, bpmnElementType) => ({ valueType: 'PROCESS_INSTANCE', intent, elementId, bpmnElementType })

test('arguments parse; unknown ones are rejected', () => {
  const a = parseArgs(['--bound', 'ci', '--graph', 'X', '--graph', 'Y', '--jobs', '2', '--file', '--no-corpus'])
  assert.equal(a.bound, 'ci')
  assert.deepEqual(a.graphs, ['X', 'Y'])
  assert.equal(a.jobs, 2)
  assert.ok(a.file)
  assert.ok(!a.corpus)
  assert.throws(() => parseArgs(['--nope']), /unknown argument/)
  assert.throws(() => parseArgs(['--bound']), /needs a value/)
})

test('--jobs must be a positive integer: zero or NaN workers would validate nothing (#1964)', () => {
  for (const bad of ['0', '-1', 'x', '1.5', '']) {
    assert.throws(() => parseArgs(['--jobs', bad]), /--jobs must be a positive integer/)
  }
  assert.equal(parseArgs(['--jobs', '3']).jobs, 3)
})

test('--captured traces must carry the current Zeebe pin (#1964)', () => {
  const pin = pinnedSha()
  assert.match(pin, /^[0-9a-f]{40}$/)
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'zc-pin-'))
  try {
    const g = path.join(root, 'goldens')
    fs.mkdirSync(g)
    const write = (p, z) => {
      fs.writeFileSync(path.join(g, 'PIN'), p == null ? '' : `${p}\n`)
      fs.writeFileSync(path.join(g, 'a.trace.json'), JSON.stringify(z === undefined ? {} : { zeebePin: z }))
    }
    write(pin, pin)
    checkCapturedPins(root, ['a'], pin)
    write('old', pin)
    assert.throws(() => checkCapturedPins(root, ['a'], pin), /Zeebe pin old/)
    fs.rmSync(path.join(g, 'PIN'))
    assert.throws(() => checkCapturedPins(root, ['a'], pin), /missing PIN/)
    write(pin, 'old')
    assert.throws(() => checkCapturedPins(root, ['a'], pin), /trace a is for Zeebe pin old/)
    write(pin, undefined)
    assert.throws(() => checkCapturedPins(root, ['a'], pin), /trace a is for Zeebe pin \(missing\)/)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('only graphs in the ZeebeTokenFlow vocabulary are in scope', () => {
  assert.ok(inSpecVocabulary(EVT))
  assert.ok(!inSpecVocabulary({ ...EVT, nodes: { ...EVT.nodes, T: 'timer' } }))
})

test('a corpus scenario becomes a capture-harness script; ticks and cancels have no spec trace', () => {
  const g = graph('ParallelDiamond')
  const s = captureScript(g, { variables: {}, steps: [{ op: 'completeOneJob', jobType: 'A', variables: { x: 1 } }] })
  assert.deepEqual(s.steps, [
    { op: 'createInstance', bpmnProcessId: 'ParallelDiamond' },
    { op: 'completeJob', jobType: 'A', variables: { x: 1 } }
  ])
  assert.equal(captureScript(g, { variables: {}, steps: [{ op: 'advanceTime' }] }), null)
  assert.equal(captureScript(g, { variables: {}, steps: [{ op: 'cancel' }] }), null)
})

test('every hand-written corpus graph yields cases, a cyclic one unrolled to a bound', () => {
  const loop = graph('ExclusiveLoop')
  assert.ok(isCyclic(loop))
  assert.ok(!isCyclic(graph('ParallelDiamond')))
  const cases = casesFor(loop)
  assert.ok(cases.length > 0)
  for (const g of loadGraphs().filter(inSpecVocabulary)) assert.ok(casesFor(g).length > 0, g.id)
})

test('records project onto the spec milestones; wait-state jobs and non-join gateways are silent', () => {
  const g = graph('ParallelDiamond')
  const ms = stepMilestones(g, [
    pi('ELEMENT_ACTIVATED', 'ParallelDiamond', 'PROCESS'),
    pi('SEQUENCE_FLOW_TAKEN', 'f1', 'SEQUENCE_FLOW'),
    pi('ELEMENT_ACTIVATED', 'P1', 'PARALLEL_GATEWAY'), // a split: not a join
    pi('ELEMENT_ACTIVATED', 'A', 'SERVICE_TASK'),
    { valueType: 'JOB', intent: 'CREATED', elementId: 'A' },
    pi('ELEMENT_ACTIVATED', 'P2', 'PARALLEL_GATEWAY'), // the join
    pi('ELEMENT_COMPLETED', 'ParallelDiamond', 'PROCESS')
  ])
  assert.deepEqual(ms, ['flow:f1', 'act:A', 'fire:P2', 'done'])
  assert.throws(() => stepMilestones(g, [pi('ELEMENT_ACTIVATED', 'Ghost', 'SERVICE_TASK')]), /unknown element Ghost/)
})

test('join decisions observed: each arrival fires or defers within its step', () => {
  const g = graph('ParallelDiamond') // P2 joins f4, f5
  // Both arrivals in one step, one firing: the other arrival deferred.
  assert.deepEqual([...observedDecisions(g, [['flow:f4', 'flow:f5', 'fire:P2']])].sort(), ['defer:P2', 'fire:P2'])
  // One arrival per step: defer, then fire.
  assert.deepEqual([...observedDecisions(g, [['flow:f4'], ['flow:f5', 'fire:P2']])].sort(), ['defer:P2', 'fire:P2'])
  const { trace, steps } = traceMilestones(g, { steps: [{ records: [pi('SEQUENCE_FLOW_TAKEN', 'f1')] }, { records: [] }] })
  assert.deepEqual(trace, ['flow:f1'])
  assert.equal(steps.length, 2)
})

test('TLC logs: an invariant violation is acceptance, anything else unexpected is an error', () => {
  const ok = 'Error: Invariant TraceUnmatched is violated.\nError: The behavior up to this point is:\nState 1'
  assert.equal(acceptanceFromLog(ok), 'accepted')
  assert.equal(acceptanceFromLog('Model checking completed. No error has been found.'), 'rejected')
  assert.equal(acceptanceFromLog(`${ok}\nError: TLC threw an unexpected exception.`), 'error')
  assert.equal(acceptanceFromLog('Warning: something\nModel checking completed. No error has been found.'), 'error')
  assert.equal(acceptanceFromLog('Error: Parsing or semantic analysis failed.'), 'error')
  assert.deepEqual([...decisionsFromLog('<<"DECISION", "fire:J">>\n<<"DECISION", "defer:J">>\n<<"DECISION", "fire:J">>')].sort(), ['defer:J', 'fire:J'])
})

test('divergence signatures are stable per class', () => {
  const a = signatureOf({ kind: 'trace', graphKey: 'k', unmatched: 'flow:f1' })
  assert.equal(a, signatureOf({ kind: 'trace', graphKey: 'k', unmatched: 'flow:f1', name: 'other' }))
  assert.notEqual(a, signatureOf({ kind: 'coverage', graphKey: 'k', decision: 'flow:f1' }))
})

// ---------------------------------------------------------------------------
// TLC: the model against fixed Zeebe-shaped traces.

const ctx = () => ({ jar: tlaJar(), spec: readSpec('ZeebeTokenFlow'), work: fs.mkdtempSync(path.join(os.tmpdir(), 'zc-test-')) })

test('TLC accepts a real Zeebe trace and rejects a reordered or truncated one', async () => {
  const c = ctx()
  const g = graph('ParallelDiamond')
  // Captured from the pinned Zeebe (ParallelDiamond, completing B then A).
  const trace = ['flow:f1', 'flow:f3', 'flow:f2', 'act:B', 'act:A', 'flow:f4', 'flow:f5', 'fire:P2', 'flow:f6', 'done']
  assert.deepEqual(await validateTrace(c, 'pd', g, trace), { ok: true })
  // The join activated before its second arrival: not a behaviour.
  const early = ['flow:f1', 'flow:f3', 'flow:f2', 'act:B', 'act:A', 'fire:P2', 'flow:f4', 'flow:f5', 'flow:f6', 'done']
  const r = await validateTrace(c, 'early', g, early)
  assert.equal(r.ok, false)
  assert.equal(r.matched, 5)
  // Zeebe stopped without completing the instance although nothing was left
  // to wait on: the model must not be allowed to stop there.
  assert.equal((await validateTrace(c, 'trunc', g, trace.slice(0, -1))).ok, false)
})

test('a prefix probe may stop WITHIN one step\'s emitted set, blaming the first bogus entry (#1964)', async () => {
  const c = ctx()
  const g = graph('ParallelDiamond')
  // S emits {f1}; the fork P1 emits {f2, f3}, so f1, f3 is a valid prefix.
  const r = await validateTrace(c, 'pd-bogus', g, ['flow:f1', 'flow:f3', 'flow:bogus', 'act:A'])
  assert.equal(r.ok, false)
  assert.equal(r.matched, 2)
  assert.equal(r.unmatched, 'flow:bogus')
  // A wrong entry mid-set is still blamed on itself, not on a valid sibling.
  const r2 = await validateTrace(c, 'pd-bogus2', g, ['flow:f1', 'flow:bogus', 'flow:f3'])
  assert.equal(r2.matched, 1)
  assert.equal(r2.unmatched, 'flow:bogus')
})

test('an event-based gateway takes NO flow to the triggered catch (Zeebe, #1962)', async () => {
  const c = ctx()
  // The exact milestones the pinned Zeebe exported correlating M1: the catch
  // event is activated directly, no SEQUENCE_FLOW_TAKEN for f2.
  assert.deepEqual(await validateTrace(c, 'evt', EVT, ['flow:f1', 'act:G1', 'flow:f3', 'done']), { ok: true })
  // Red: the shape the model claimed before #1962 (taking the gateway flow).
  assert.equal((await validateTrace(c, 'evt-old', EVT, ['flow:f1', 'act:G1', 'flow:f2', 'flow:f3', 'done'])).ok, false)
})

test('the unconstrained model enumerates both join decisions of a diamond', async () => {
  const d = await modelDecisions(ctx(), graph('ParallelDiamond'))
  assert.deepEqual([...d].sort(), ['defer:P2', 'fire:P2'])
})
