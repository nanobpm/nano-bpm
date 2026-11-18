// Unit tests for the differential fuzzer (#1250): seeded determinism, the
// generator's well-formedness, the random command order's validity, the
// shrinker's 1-minimality and the permanent-corpus rules. Pure — no wasm, no
// JVM, no Camunda.
import assert from 'node:assert/strict'
import fs from 'node:fs'
import { test } from 'node:test'

import { Reference, wellFormed, materialize, randomRun, flowCount } from './enumerate.mjs'
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
  judgeCase,
  caseRunnable,
  loadCorpus,
  caseRecord,
  MAX_GATEWAYS
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

test('the committed fuzz corpus is well-formed', () => {
  assert.deepEqual(corpusProblems(loadCorpus()), [])
})
