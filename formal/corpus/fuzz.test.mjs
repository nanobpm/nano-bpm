// Unit tests for the differential fuzzer (#1250): seeded determinism, the
// generator's well-formedness, the random command order's validity, the
// shrinker's 1-minimality and the permanent-corpus rules. Pure — no wasm, no
// JVM, no Camunda.
import assert from 'node:assert/strict'
import fs from 'node:fs'
import { test } from 'node:test'

import { Reference, wellFormed, materialize, randomRun, flowCount, stepSignatures, commandSignature } from './enumerate.mjs'
import {
  parseArgs,
  caseSeed,
  picker,
  randomSkeleton,
  fuzzCase,
  reductions,
  shrinkGraph,
  shrinkRuns,
  corpusProblems,
  rederive,
  rederivedScenarios,
  canonJson,
  judgeCase,
  caseRunnable,
  shorterRepro,
  executionNamer,
  loadCorpus,
  caseRecord,
  unexplained,
  knownKey,
  MAX_GATEWAYS,
  issueBody,
  corpusIdentity,
  uncommittedIssues
} from './fuzz.mjs'
import { prng } from '../traces/fuzz/generate.mjs'

const BOUNDS = JSON.parse(fs.readFileSync(new URL('./bounds.json', import.meta.url), 'utf8'))

test('parseArgs: the run is deterministic from a required seed', () => {
  assert.throws(() => parseArgs([]), /--seed is required/)
  assert.equal(parseArgs(['--seed', '7']).seed, 7)
  assert.equal(parseArgs(['--replay-only']).seed, null)
  assert.throws(() => parseArgs(['--seed', '-1']), /non-negative integer/)
  assert.throws(() => parseArgs(['--seed', '1', '--backend', 'x']), /--backend/)
  assert.throws(() => parseArgs(['--seed', '1', '--require-camunda']), /--require-camunda/)
  assert.throws(() => parseArgs(['--seed', '1', '--file-issues']), /--file-issues needs --record/)
  assert.throws(() => parseArgs(['--seed', '1', '--max-gateways', String(MAX_GATEWAYS + 1)]), /capped/)
  assert.throws(() => parseArgs(['--seed', '1', '--min-gateways', '4', '--max-gateways', '3']), /exceeds/)
  assert.throws(() => parseArgs(['--seed', '1', '--kinds', 'nope']), /unknown gateway kind/)
  assert.throws(() => parseArgs(['--seed', '1', '--retries', '2']), /unknown argument/)
})

test('caseSeed: stable and distinct per (seed, index)', () => {
  assert.equal(caseSeed(42, 0), caseSeed(42, 0))
  const seeds = new Set(Array.from({ length: 1000 }, (_, i) => caseSeed(42, i)))
  assert.equal(seeds.size, 1000)
  assert.notEqual(caseSeed(1, 0), caseSeed(2, 0))
})

test('randomSkeleton: every seed yields a well-formed skeleton within the gateway range', () => {
  for (let s = 0; s < 2000; s++) {
    const skel = randomSkeleton(picker(prng(s)), { minGateways: 1, maxGateways: 6 })
    assert.ok(wellFormed(skel.kinds, skel.edges), `seed ${s}`)
    assert.ok(skel.kinds.length >= 1 && skel.kinds.length <= 6, `seed ${s}`)
  }
  // Restricted kinds are honoured (an evt fallback never leaks in).
  for (let s = 0; s < 200; s++) {
    const skel = randomSkeleton(picker(prng(s)), { kinds: ['and', 'or'] })
    assert.ok(skel.kinds.every((k) => ['and', 'or'].includes(k)), `seed ${s}`)
  }
})

test('the default fuzz reaches beyond the nightly exhaustive bound', () => {
  let beyond = 0
  for (let i = 0; i < 200; i++) {
    const { skel } = fuzzCase(caseSeed(1, i), {})
    if (skel.kinds.length > BOUNDS.nightly.gateways || flowCount(skel) > BOUNDS.nightly.flows) beyond++
  }
  assert.equal(beyond, 200)
})

test('fuzzCase: identical seed -> byte-identical graph, BPMN and scenario', () => {
  for (const s of [0, 1, 99, 123456789]) {
    assert.equal(JSON.stringify(fuzzCase(s, {})), JSON.stringify(fuzzCase(s, {})))
  }
  assert.notEqual(JSON.stringify(fuzzCase(1, {}).graph), JSON.stringify(fuzzCase(2, {}).graph))
})

test('fuzzCase: the BPMN carries DI and the scenario is a single complete run', () => {
  const e = fuzzCase(5, {})
  assert.match(e.artifacts.bpmn, /<bpmndi:BPMNDiagram/)
  assert.equal(e.artifacts.scenarios.length, 1)
  assert.equal(e.artifacts.scenarios[0].processId, e.graph.id)
})

test('randomRun: every step is a command the reference offers, and the run ends settled', () => {
  for (let s = 0; s < 300; s++) {
    const graph = materialize(randomSkeleton(picker(prng(s)), {}))
    const run = randomRun(graph, picker(prng(s + 1)))
    let state = new Reference(graph, run.route)
    for (const c of run.steps) {
      assert.equal(state.pendingChoices().length, 0, `seed ${s}`)
      assert.ok(state.commands().some((x) => JSON.stringify(x) === JSON.stringify(c)), `seed ${s}: ${JSON.stringify(c)}`)
      state = state.apply(c)
    }
    assert.equal(state.commands().length, 0, `seed ${s}`)
    assert.equal(state.completed, run.completed, `seed ${s}`)
    assert.deepEqual({ ...state.completedElements }, run.completedElements, `seed ${s}`)
  }
})

test('reductions: every candidate is well-formed and strictly smaller', () => {
  for (let s = 0; s < 200; s++) {
    const skel = randomSkeleton(picker(prng(s)), {})
    const size = (x) => x.kinds.length * 1000 + flowCount(x)
    for (const r of reductions(skel)) {
      assert.ok(wellFormed(r.kinds, r.edges), `seed ${s}`)
      assert.ok(size(r) < size(skel), `seed ${s}`)
    }
  }
})

test('reductions: gateway removal bridges to every successor, not just the first edge', () => {
  // S->AND, AND->E listed before AND->OR, OR->E twice (task + direct).
  const skel = { kinds: ['and', 'or'], edges: [[0, 1, 'direct'], [1, 3, 'direct'], [1, 2, 'direct'], [2, 3, 'task'], [2, 3, 'direct']] }
  const orOnly = reductions(skel).filter((r) => r.kinds.length === 1 && r.kinds[0] === 'or')
  assert.ok(orOnly.some((r) => r.edges.some(([i, j]) => i === 0 && j === 1)), 'S->OR bridge candidate is offered')
})

test('randomSkeleton: never generates a gateway kind outside the requested vocabulary', () => {
  for (const kinds of [['evt'], ['xor'], ['and', 'evt'], ['or']]) {
    for (let s = 0; s < 300; s++) {
      const skel = randomSkeleton(picker(prng(s)), { kinds })
      for (const k of skel.kinds) assert.ok(kinds.includes(k), `seed ${s} kinds ${kinds}: got ${k}`)
    }
  }
})

test('unexplained: a replay divergence no open case explains is surfaced', () => {
  const d = (signature, key) => ({ signature, entry: { graph: { key } } })
  const keys = new Set([knownKey('a', 'g1')])
  // A divergence on the open case's own graph is explained...
  assert.deepEqual(unexplained([d('a', 'g1')], keys), [])
  // ...but the SAME signature on any other graph is a distinct defect: a
  // field-based signature must never suppress a whole class across graphs.
  assert.deepEqual(unexplained([d('a', 'g2')], keys).map((x) => x.entry.graph.key), ['g2'])
  assert.deepEqual(unexplained([d('a', 'g1'), d('a', 'g2'), d('b', 'g1')], keys).map((x) => [x.signature, x.entry.graph.key]), [['a', 'g2'], ['b', 'g1']])
  // Two open cases sharing one signature explain only their own graphs each.
  const shared = new Set([knownKey('a', 'g1'), knownKey('a', 'g2')])
  assert.deepEqual(unexplained([d('a', 'g1'), d('a', 'g2')], shared), [])
  assert.deepEqual(unexplained([d('a', 'g3')], shared).map((x) => x.entry.graph.key), ['g3'])
})

test('shrinkGraph: the result is 1-minimal for the divergence predicate', async () => {
  // A stand-in divergence: "the graph has an inclusive gateway and a task".
  const diverges = (skel) => skel.kinds.includes('or') && skel.edges.some((e) => e[2] === 'task')
  let tried = 0
  for (let s = 0; s < 400 && tried < 25; s++) {
    const skel = randomSkeleton(picker(prng(s)), { minGateways: 4, maxGateways: 6 })
    if (!diverges(skel)) continue
    tried++
    const reproduce = async (cand) => (diverges(cand) ? { signature: 'x', size: flowCount(cand) } : null)
    const { skel: min, d } = await shrinkGraph(skel, reproduce, { signature: 'x' })
    assert.ok(diverges(min))
    assert.equal(d.size, flowCount(min))
    assert.ok(min.kinds.length <= skel.kinds.length && flowCount(min) <= flowCount(skel))
    assert.ok(reductions(min).every((r) => !diverges(r)), `seed ${s} not 1-minimal`)
  }
  assert.equal(tried, 25)
})

test('shrinkRuns: deterministic, de-duplicated candidate orders', () => {
  const graph = materialize(randomSkeleton(picker(prng(3)), {}))
  const a = shrinkRuns(graph)
  assert.equal(JSON.stringify(a), JSON.stringify(shrinkRuns(graph)))
  assert.equal(new Set(a.map((r) => JSON.stringify([r.route, r.steps]))).size, a.length)
})

const sampleCase = () => {
  const e = fuzzCase(11, {})
  return { ...caseRecord(11, e.skel, { signature: 'nano:x', kind: 'nano', entry: e, scenario: e.artifacts.scenarios[0], detail: [] }), issue: '#1' }
}

test('corpusProblems: a committed case needs an issue, a fixed flag and a reproducing skeleton', () => {
  const ok = sampleCase()
  assert.deepEqual(corpusProblems([ok]), [])
  assert.match(corpusProblems([{ ...ok, issue: null }])[0], /no filed issue/)
  assert.match(corpusProblems([{ ...ok, fixed: undefined }])[0], /boolean "fixed"/)
  assert.match(corpusProblems([{ ...ok, scenario: undefined }])[0], /"skeleton" and "scenario"/)
  assert.match(corpusProblems([{ ...ok, graph: { ...ok.graph, key: 'other' } }])[0], /no longer materialises/)
})

test('corpusProblems: a frozen scenario must still be a current-Reference behaviour (#1693)', () => {
  const ok = sampleCase()
  assert.ok(rederive(ok), 'a freshly recorded case re-derives')
  const staleExpect = { ...ok, scenario: { ...ok.scenario, expect: { ...ok.scenario.expect, completed: !ok.scenario.expect.completed } } }
  assert.match(corpusProblems([staleExpect])[0], /frozen "expect" is stale/)
  const unreachable = { ...ok, scenario: { ...ok.scenario, steps: [...ok.scenario.steps, { op: 'completeOneJob', jobType: 'NoSuchTask' }] } }
  assert.match(corpusProblems([unreachable])[0], /no longer a behaviour of the current Reference/)
})

test('canonJson ignores key order and the per-execution correlation key', () => {
  assert.equal(canonJson({ b: 1, a: { d: 2, c: 3 }, correlationKey: 'x' }), canonJson({ a: { c: 3, d: 2 }, b: 1, correlationKey: 'y' }))
  assert.notEqual(canonJson([1, 2]), canonJson([2, 1]))
})

test('judgeCase: open cases must reproduce, fixed cases must agree (regression guard)', () => {
  const c = sampleCase()
  assert.equal(judgeCase(c, [{ signature: 'nano:x' }]), null)
  assert.match(judgeCase(c, []), /stale/)
  assert.match(judgeCase(c, [{ signature: 'nano:other' }]), /stale/)
  const fixed = { ...c, fixed: true }
  assert.equal(judgeCase(fixed, []), null)
  assert.match(judgeCase(fixed, [{ signature: 'nano:x' }]), /REGRESSION/)
})

test('caseRunnable: a Camunda-only case is skipped when Camunda did not run', () => {
  assert.ok(caseRunnable({ kind: 'nano' }, new Set(['nano'])))
  assert.ok(!caseRunnable({ kind: 'differential' }, new Set(['nano'])))
})

test('caseRunnable: a FIXED Camunda case is still replayed on Nano as a regression guard', () => {
  assert.ok(caseRunnable({ kind: 'differential', fixed: true }, new Set(['nano'])))
  assert.ok(caseRunnable({ kind: 'camunda', fixed: true }, new Set(['nano'])))
  assert.ok(!caseRunnable({ kind: 'camunda', fixed: false }, new Set(['nano'])))
})

test('shorterRepro keeps the known shorter reproducer over a longer sampled one', () => {
  const d = (n) => ({ scenario: { steps: new Array(n).fill({}) } })
  const known = d(2)
  assert.equal(shorterRepro(known, d(5)), known)
  assert.equal(shorterRepro(known, null), known)
  const better = d(1)
  assert.equal(shorterRepro(known, better), better)
  assert.equal(shorterRepro(known, d(2)), known, 'a tie keeps the known one')
})

test('executionNamer gives every execution a distinct scenario name (correlation namespace)', () => {
  const next = executionNamer('r7')
  const names = [next('replay'), next('shrink'), next('shrink'), next('shrink')]
  assert.equal(new Set(names).size, names.length)
  const again = executionNamer('r7')
  assert.equal(again('replay'), names[0], 'deterministic per run')
})

test('the committed fuzz corpus is well-formed', () => {
  assert.deepEqual(corpusProblems(loadCorpus()), [])
})

test('renameScenario gives a stored scenario a fresh name and correlation key everywhere', async () => {
  const { renameScenario } = await import('./fuzz.mjs')
  const sc = { name: 'G-o1', variables: { correlationKey: 'G-o1', x: 1 }, steps: [{ op: 'publishMessage', name: 'm', correlationKey: 'G-o1' }], expect: { variables: { correlationKey: 'G-o1' } } }
  const r = renameScenario(sc, 'G-replayx1')
  assert.equal(r.name, 'G-replayx1')
  assert.equal(r.variables.correlationKey, 'G-replayx1')
  assert.equal(r.steps[0].correlationKey, 'G-replayx1')
  assert.equal(r.expect.variables.correlationKey, 'G-replayx1')
  assert.equal(sc.variables.correlationKey, 'G-o1', 'original untouched')
  assert.ok(!JSON.stringify(r).includes('G-o1'))
})

test('shrinkGraph checkpoints each accepted reduction, so an aborted shrink keeps a reproducer', async () => {
  const { shrinkGraph } = await import('./fuzz.mjs')
  const skel = randomSkeleton(picker(prng(7)), { minGateways: 4, maxGateways: 6 })
  let calls = 0
  let latest = null
  const reproduce = async (cand) => { if (++calls > 1) throw new Error('cleanup failed'); return { signature: 's', cand } }
  await assert.rejects(shrinkGraph(skel, reproduce, { signature: 's' }, (b) => { latest = b }), /cleanup failed/)
  assert.ok(latest, 'a reproducer was checkpointed before the failure')
  assert.equal(latest.d.signature, 's')
  assert.ok(latest.d.cand, 'the checkpoint is the accepted reduction, not just the initial case')
})

test('guided rederive (#1747 review): following the recorded order finds exactly what exhaustive search finds', () => {
  for (const c of loadCorpus()) {
    const steps = canonJson(c.scenario.steps)
    const pick = (ss) => ss.filter((s) => canonJson(s.steps) === steps).map((s) => canonJson(s.expect))
    assert.deepEqual(pick(rederivedScenarios(c)), pick(rederivedScenarios(c, { exhaustive: true })), `${c.signature} (${c.graph?.id})`)
  }
})

test('stepSignatures / commandSignature fail closed on an unknown op', () => {
  assert.throws(() => stepSignatures([{ op: 'teleport' }]), /unknown scenario step op/)
  assert.throws(() => commandSignature({ id: 'g' }, { op: 'teleport' }), /unknown command op/)
  assert.deepEqual(stepSignatures([{ op: 'setVariables', variables: {} }, { op: 'advanceTime', ms: 1 }, { op: 'cancelInstance' }]), ['tick', 'cancel'])
})

// M8 fuzz triage: every filed finding must have a committed reproducer. #1753–
// #1758 were filed claiming "committed in fuzz-corpus.json" from a run whose
// corpus diff never landed, so nothing replayed them. Triaged duplicates are
// aliased to the issue whose committed case (same graph, same seed) covers them.
// #1757 is deliberately absent from this list: it is not a fuzz parity issue at
// all but a Mergify merge-queue draft (for #1743), so there is no reproducer to
// commit and nothing for the guard below to check.
export const TRIAGED_FUZZ_ISSUES = ['#1753', '#1754', '#1755', '#1756', '#1758']
export const TRIAGE_ALIASES = { '#1753': '#1693', '#1754': '#1675' }

test('every triaged fuzz issue has a committed reproducer (or an alias that does)', () => {
  assert.deepEqual(uncommittedIssues(TRIAGED_FUZZ_ISSUES, loadCorpus(), TRIAGE_ALIASES), [])
})

test('uncommittedIssues reports a filed finding absent from the corpus', () => {
  const cases = [{ issue: '#1' }, { issue: '#2' }]
  assert.deepEqual(uncommittedIssues(['#1', '#3', '#4'], cases, { '#4': '#2' }), ['#3'])
})

test('issueBody states the commit obligation and carries the corpus identity', () => {
  const c = loadCorpus()[0]
  const body = issueBody(c)
  assert.doesNotMatch(body, /committed in `formal\/corpus\/fuzz-corpus.json`/)
  assert.match(body, /NOT guarded until that corpus diff is committed/)
  assert.ok(body.includes(corpusIdentity(c)))
})
