// Tests for the generative Zeebe differential harness (#1521). Pure node, no
// JVM: the generator's determinism + well-formedness, the driver's validity
// invariants, the shrinker, and THE drift guard tying the committed fixed-seed
// budget (scenarios/g-*) to the generator.
import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import { test } from 'node:test'

import {
  COLLIDING, GENERATED_PREFIX, TRACES_DIR, blocks, budgetDrift, generateCase, generateTree,
  loadBudget, renderCase, scenarioName
} from './generate.mjs'
import { parseSeeds, reductions, signature, size } from './sweep.mjs'

const SEEDS = parseSeeds('1..200')

test('a seed always yields byte-identical artifacts', () => {
  for (const s of [1, 7, 42, 1521]) {
    assert.deepEqual(generateCase(s), generateCase(s))
  }
  assert.notDeepEqual(generateCase(1).xml, generateCase(2).xml)
})

test('every generated model carries complete DI and unique ids', () => {
  for (const s of SEEDS) {
    const { xml } = generateCase(s)
    const ids = [...xml.matchAll(/<bpmn:\w+ id="([^"]+)"/g)].map((m) => m[1])
    assert.equal(new Set(ids).size, ids.length, `seed ${s}: duplicate ids`)
    const flowNodes = [...xml.matchAll(/<bpmn:(\w+) id="([^"]+)"/g)]
      .filter(([, tag]) => !['sequenceFlow', 'message', 'process', 'definitions'].includes(tag))
      .map((m) => m[2])
    const shapes = new Set([...xml.matchAll(/<bpmndi:BPMNShape [^>]*bpmnElement="([^"]+)"/g)].map((m) => m[1]))
    const edges = new Set([...xml.matchAll(/<bpmndi:BPMNEdge [^>]*bpmnElement="([^"]+)"/g)].map((m) => m[1]))
    for (const id of flowNodes) assert.ok(shapes.has(id), `seed ${s}: ${id} has no BPMNShape`)
    for (const [, id] of xml.matchAll(/<bpmn:sequenceFlow id="([^"]+)"/g)) {
      assert.ok(edges.has(id), `seed ${s}: flow ${id} has no BPMNEdge`)
    }
    // every sequence flow references existing nodes
    for (const [, src, tgt] of xml.matchAll(/sourceRef="([^"]+)" targetRef="([^"]+)"/g)) {
      assert.ok(ids.includes(src) && ids.includes(tgt), `seed ${s}: dangling flow ${src}->${tgt}`)
    }
  }
})

test('the generator covers the issue element mix across the per-PR budget sweep', () => {
  const seen = new Set()
  for (const s of SEEDS) {
    for (const b of blocks(generateTree(s).body)) {
      seen.add(b.kind === 'mi' ? `mi-${b.sequential ? 'seq' : 'par'}-${b.body.kind}` : b.kind)
      if (b.kind === 'task' && b.boundary) seen.add(`boundary-${b.boundary.kind}`)
      if (b.kind === 'task' && b.user) seen.add('user')
      if (b.kind === 'task' && (b.inputs || b.outputs)) seen.add('io')
    }
  }
  for (const k of ['task', 'user', 'io', 'timer', 'message', 'xor', 'and', 'or', 'sub', 'call',
    'mi-par-task', 'mi-par-sub', 'mi-seq-task', 'mi-seq-sub', 'boundary-timer', 'boundary-message']) {
    assert.ok(seen.has(k), `no generated case exercises ${k}`)
  }
})

test('driver scripts only use the trace vocabulary and collide on variable names', () => {
  const ops = new Set(['createInstance', 'completeJob', 'completeUserTask', 'publishMessage', 'advanceClock', 'updateVariables'])
  let collisions = 0
  for (const s of SEEDS) {
    const { script } = generateCase(s)
    assert.equal(script.steps[0].op, 'createInstance')
    const jobs = new Set()
    for (const st of script.steps) {
      assert.ok(ops.has(st.op), `seed ${s}: op ${st.op}`)
      assert.ok(!st.expectRejection, `seed ${s}: generated drivers never expect a rejection`)
      if (st.op === 'completeJob') {
        // The runners complete the LATEST job of a type: each type is completed once.
        assert.ok(!jobs.has(st.jobType), `seed ${s}: job type ${st.jobType} completed twice`)
        jobs.add(st.jobType)
      }
      for (const k of Object.keys(st.variables ?? {})) {
        if (st.op !== 'createInstance' && COLLIDING.includes(k)) collisions++
      }
    }
  }
  assert.ok(collisions > SEEDS.length, 'completions must write the shared colliding names')
})

test('a multi-instance outputElement is mostly the bare `=result` reference (#1518)', () => {
  const mis = SEEDS.flatMap((s) => blocks(generateTree(s).body).filter((b) => b.kind === 'mi'))
  assert.ok(mis.filter((m) => m.outputElement === '=result').length > mis.length / 2)
})

test('reductions shrink the tree and stay renderable', () => {
  const tree = generateTree(3)
  const rs = reductions(tree)
  assert.ok(rs.length > 0)
  for (const t of rs) {
    assert.ok(size(t) < size(tree), 'every reduction is strictly smaller')
    renderCase(t) // renders without throwing
  }
  // the original is untouched
  assert.deepEqual(tree, generateTree(3))
})

test('a truncated script is a prefix of the full one', () => {
  const tree = generateTree(5)
  const full = renderCase(tree).script.steps
  const cut = renderCase(tree, { maxSteps: 3 }).script.steps
  assert.deepEqual(cut, full.slice(0, 3))
})

test('divergence signatures ignore runtime values', () => {
  const d = (v) => `step 2:\n  zeebe: {"op":"completeJob","records":[{"valueType":"VARIABLE","intent":"CREATED","value":${v}}]}\n  nano:  {"op":"completeJob","records":[]}`
  assert.equal(signature(d(1)), signature(d(2)))
  assert.notEqual(signature(d(1)), signature(d(1).replace('CREATED', 'UPDATED')))
})

test('seed specs parse as ranges and lists', () => {
  assert.deepEqual(parseSeeds('3..5'), [3, 4, 5])
  assert.deepEqual(parseSeeds('9,1'), [9, 1])
  assert.throws(() => parseSeeds('5..3'))
})

// THE drift guard (CI): the committed g-* scenarios are exactly the budget's
// seeds, regenerated byte-for-byte, and each has a captured Zeebe golden.
test('committed fixed-seed budget matches the generator', () => {
  const { seeds } = loadBudget()
  assert.ok(seeds.length > 0)
  assert.deepEqual(budgetDrift(), [])
  for (const s of seeds) {
    const g = path.join(TRACES_DIR, 'goldens', `${scenarioName(s)}.trace.json`)
    assert.ok(fs.existsSync(g), `${scenarioName(s)} has no Zeebe golden — run capture.sh ${scenarioName(s)}`)
  }
  assert.ok(seeds.every((s) => scenarioName(s).startsWith(GENERATED_PREFIX)))
})

test('the allowlist ratchet refuses masking a seed that already passed', async () => {
  const { ratchetViolations } = await import('./ratchet.mjs')
  const entry = (s) => ({ scenario: s, issue: '#1', reason: 'x' })
  // a fixed seed newly needs an entry: masked regression
  assert.equal(ratchetViolations({
    baseAllowlist: { entries: [] }, baseBudget: { seeds: [1, 2] }, headAllowlist: { entries: [entry('g-2')] }
  }).length, 1)
  // a new budget seed may arrive with its (issue-linked) entry
  assert.deepEqual(ratchetViolations({
    baseAllowlist: { entries: [] }, baseBudget: { seeds: [1] }, headAllowlist: { entries: [entry('g-2')] }
  }), [])
  // an existing entry is grandfathered; shrinking is always fine
  assert.deepEqual(ratchetViolations({
    baseAllowlist: { entries: [entry('g-1')] }, baseBudget: { seeds: [1] }, headAllowlist: { entries: [entry('g-1')] }
  }), [])
  // hand-written scenarios are outside this ratchet
  assert.deepEqual(ratchetViolations({
    baseAllowlist: { entries: [] }, baseBudget: { seeds: [1] }, headAllowlist: { entries: [entry('timer-catch')] }
  }), [])
  // a base without the budget (pre-#1521) accepts anything
  assert.deepEqual(ratchetViolations({ baseAllowlist: null, baseBudget: null, headAllowlist: { entries: [entry('g-1')] } }), [])
})
