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
  flowCount,
  Reference
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
  // 2-gateway/5-flow includes the MIXED join/split gateways (#1246 review).
  assert.equal(count(2, 5), 102)
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

test('well-formedness rejects no-op gateways and misplaced message edges, but keeps mixed join/split gateways', () => {
  assert.equal(wellFormed(['xor'], [[0, 1, 'direct'], [1, 2, 'direct']]), false, '1-in-1-out no-op')
  assert.equal(wellFormed(['evt'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'msg']]), true)
  assert.equal(wellFormed(['evt'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'task']]), false)
  assert.equal(wellFormed(['xor'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'msg']]), false)
  assert.equal(wellFormed([], [[0, 1, 'direct'], [0, 1, 'task']]), false, 'S has one out-flow')
  // Mixed gateways (>= 2 in, >= 2 out) are supported by the engine and the TLA
  // specs, so the enumeration must include them (#1246 review): S -> A(and),
  // two flows A -> B, two flows B(or) -> E is the cited 2-gateway/5-flow case.
  const mixed = [[0, 1, 'direct'], [1, 2, 'direct'], [1, 2, 'direct'], [2, 3, 'direct'], [2, 3, 'direct']]
  assert.equal(wellFormed(['and', 'or'], mixed), true, 'mixed and-split -> or-split/join')
  assert.equal(wellFormed(['or', 'and'], mixed), true, 'mixed or-split -> and-split/join')
  assert.equal(wellFormed(['xor', 'xor'], mixed), true, 'mixed xor-split -> xor-split/join')
  // An event-based gateway stays split-only (a join evt is unmodelled).
  assert.equal(wellFormed(['and', 'evt'], [[0, 1, 'direct'], [1, 2, 'direct'], [1, 2, 'direct'], [2, 3, 'msg'], [2, 3, 'msg']]), false, 'evt with 2 in-flows is not a pure split')
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
  const runs = exploreOrders(g)
  assert.deepEqual(runs.map((r) => r.steps.map((s) => s.node)), [['T1', 'T2'], ['T2', 'T1']])
  assert.ok(runs.every((r) => r.completed))
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

test('reference: a fork takes its flows in Zeebe\'s reverse document order, so an inclusive join is guarded AFTER the sibling branch routes away (#1694, #1695)', () => {
  // Gen_a0e091fa90 (fuzz seed 3666021433): parallel fork G1 -> {inclusive join
  // G2 (f2), exclusive G3 (f3)}; G3 -> {G2 (f6), E (f7)}; G2 -> T1 -> E. Zeebe
  // (live 8.10.6) takes f3 BEFORE f2 (ExecutableFlowNode.getOutgoing() is
  // reverse declaration order), so G3 has already routed to E when G2's
  // ACTIVATE is guarded: no live path remains, G2 fires and T1's job is
  // created. Taking f2 first would park G2 forever (the defect).
  const skel = { kinds: ['and', 'xor', 'or'], edges: [[1, 2, 'direct'], [2, 4, 'direct'], [1, 3, 'direct'], [3, 4, 'task'], [0, 1, 'direct'], [2, 3, 'direct']] }
  const g = materialize(skel)
  assert.equal(g.key, 'and,or,xor|0>1d,1>2d,1>3d,2>4t,3>2d,3>4d')
  const ref = new Reference(g, { route_G3: [['f7']] })
  assert.deepEqual({ ...ref.jobsCreated }, { T1: 1 })
  assert.deepEqual({ ...ref.joinsFired }, { G2: 1 })
  assert.deepEqual({ ...ref.takenFlows }, { f1: 1, f2: 1, f3: 1, f7: 1, f4: 1 })
  assert.equal(ref.completed, false)
})

test('reference: an inclusive split routes every non-empty subset and its join waits for each', () => {
  const g = graphOf(['or', 'or'], [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'task'], [2, 3, 'direct']])
  const routes = routesFor(g)
  // Each route is the choice SEQUENCE (one entry per activation); a single
  // activation's route is a one-element sequence of the chosen subset.
  assert.deepEqual(routes.map((r) => r[routeVariable('G1')][0].length).sort(), [1, 1, 2])
  for (const r of routes) {
    for (const run of exploreOrders(g, r)) {
      assert.equal(run.completed, true)
      assert.equal(run.steps.length, r[routeVariable('G1')][0].length)
    }
  }
})

test('reference: a gateway reached twice routes PER ACTIVATION, not per instance (#1246 review)', () => {
  // S -> P(and split) -> {T1, T2} -> X(xor merge) -> {E, T3 -> E}: the two
  // parallel tokens each activate X, and the valid execution that routes the
  // first to T3 and the second to E creates exactly ONE T3 job — a per-instance
  // route (all activations share one choice) can only make zero or two.
  const g = graphOf(['and', 'xor'], [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'task'], [2, 3, 'task'], [2, 3, 'direct']])
  const runs = exploreOrders(g)
  // Key-order-insensitive: a fork queues its tasks in Zeebe's reverse document
  // order (#1694), so the multiset — not its insertion order — is the oracle.
  const jobsKey = (jobs) => JSON.stringify(Object.fromEntries(Object.entries(jobs).sort(([a], [b]) => (a < b ? -1 : 1))))
  const byJobs = new Map(runs.map((r) => [jobsKey(r.jobsCreated), r]))
  assert.ok(byJobs.has('{"T1":1,"T2":1}'), 'route both to E: no T3')
  assert.ok(byJobs.has('{"T1":1,"T2":1,"T3":1}'), 'route one to T3, one to E: exactly one T3 job')
  assert.ok(byJobs.has('{"T1":1,"T2":1,"T3":2}'), 'route both to T3: two T3 jobs')
  // The one-T3 run's route is a per-activation sequence with DIFFERENT entries.
  const mixed = byJobs.get('{"T1":1,"T2":1,"T3":1}')
  const seq = mixed.route[routeVariable('G2')]
  assert.equal(seq.length, 2)
  assert.notDeepEqual(seq[0], seq[1], 'the two activations of X choose differently')
  assert.ok(runs.every((r) => r.completed))
})

test('reference: an event-based gateway is a wait state resolved by one correlation', () => {
  const g = graphOf(['evt'], [[0, 1, 'direct'], [1, 2, 'msg'], [1, 2, 'msg']])
  const runs = exploreOrders(g)
  assert.deepEqual(runs.map((r) => r.steps.map((s) => s.catch)), [['M1'], ['M2']])
  assert.ok(runs.every((r) => r.completed))
})

test('scenarios: one per (route, command order) run, from the one graph source', () => {
  const g = graphOf(['evt'], [[0, 1, 'task'], [1, 2, 'msg'], [1, 2, 'msg']])
  const { scenarios, skipped } = scenariosFor(g)
  assert.equal(skipped, 0)
  assert.equal(scenarios.length, 2)
  const [s] = scenarios
  assert.equal(s.processId, g.id)
  assert.equal(s.variables[CORRELATION_KEY_VARIABLE], s.name, 'each scenario has its own correlation key')
  assert.deepEqual(s.steps, [
    { op: 'completeOneJob', jobType: 'T1' },
    { op: 'publishMessage', name: messageName(g.id, 'M1'), correlationKey: s.name }
  ])
  // The oracle is branch-sensitive via the taken flows: the engine arms EVERY
  // catch (M1 AND the losing M2 both complete — the loser is withdrawn), so the
  // catch's completion does not distinguish the branch. The taken OUTGOING flow
  // does: only the winner's continuation (M1->E) is taken, never M2->E.
  assert.deepEqual(s.expect, {
    completed: true,
    jobsCreated: { T1: 1 },
    variables: s.variables,
    completedElements: { S: 1, T1: 1, G1: 1, M1: 1, M2: 1, E: 1 },
    sequenceFlows: { 'S->T1': 1, 'T1->G1': 1, 'G1->M1': 1, 'G1->M2': 1, 'M1->E': 1 }
  })
  const stuck = scenariosFor(graphOf(['xor', 'and'], [[0, 1, 'direct'], [1, 2, 'task'], [1, 2, 'direct'], [2, 3, 'direct']]))
  assert.ok(stuck.scenarios.every((sc) => sc.expect.completed === false && sc.backends?.join() === 'nano'))
})

test('a gateway the FIRST command activates routes via start variables, not a leading setVariables step (#1246 review)', () => {
  // S -> T1 -> XOR -> {E, E}: the only command (completeOneJob T1) activates the
  // gateway, so the route must be decided before that command runs. Emitting it
  // as a leading `setVariables` step is rejected by Camunda — `setVariables`
  // needs the instance key, which only a job activation exposes. The route must
  // ride the START variables instead.
  const g = graphOf(['xor'], [[0, 1, 'task'], [1, 2, 'direct'], [1, 2, 'direct']])
  const { scenarios } = scenariosFor(g)
  assert.ok(scenarios.length >= 2, 'one scenario per XOR branch')
  for (const s of scenarios) {
    assert.notEqual(s.steps[0]?.op, 'setVariables', 'no leading setVariables step')
    assert.ok(s.variables[routeVariable('G1')], 'the route rides the start variables')
    assert.deepEqual(s.steps, [{ op: 'completeOneJob', jobType: 'T1' }])
  }
})

test('each activation of a twice-activated gateway reads its own choice at the route head (#1246 review)', () => {
  // Gen_de3a1f331f: S -> AND -> {XOR, T1 -> XOR}; XOR -> {E, T2 -> E}. The
  // create settle activates XOR (activation 0) and leaves T1 waiting; completing
  // T1 activates XOR again (activation 1). The scenario that routes activation 0
  // to T2 (f6) and activation 1 to E (f5) must deliver the FULL suffix
  // [[f6],[f5]] — the BPMN reads the head `route[1]` at activation 0, so the
  // head must be f6. Slicing from the about-to-be-consumed index (1) drops f6
  // and delivers [[f5]], so activation 0 takes f5 (no T2) and the run reports a
  // false divergence. The delivered head must be activation 0's own choice.
  const g = graphOf(['and', 'xor'], [[0, 1, 'direct'], [1, 2, 'direct'], [1, 2, 'task'], [2, 3, 'direct'], [2, 3, 'task']])
  const rv = routeVariable('G2')
  const { scenarios } = scenariosFor(g)
  // The runs that create exactly one T2 (route one XOR activation to f6, the
  // other to f5) carry a two-entry route. The head MUST be activation 0's own
  // choice: the run whose activation 0 takes f6 (T2) starts with [f6]; slicing
  // from the about-to-be-consumed index would drop it and deliver [f5] first.
  // Each XOR activation must read ITS OWN choice at the head: activation 0
  // reads the start variables, activation 1 (drained by T1's completion) reads
  // the suffix T1's completion carries. A stale create-time index would leave
  // the head unchanged so both activations take the same flow.
  const oneT2 = scenarios.filter((s) => s.expect.jobsCreated.T2 === 1)
  assert.equal(oneT2.length, 3, 'f6-then-f5 (two orders) and f5-then-f6 runs')
  for (const s of oneT2) {
    const first = s.variables[rv][0][0]
    const t1 = s.steps.find((st) => st.op === 'completeOneJob' && st.jobType === 'T1')
    const second = first === 'f6' ? 'f5' : 'f6'
    assert.deepEqual(t1.variables?.[rv], [[second]], `${s.name}: T1's completion moves the head to activation 1's choice`)
    assert.ok(!s.steps.some((st) => st.op === 'setVariables'), `${s.name}: no stale re-delivery`)
  }
})

test('a message-first scenario delivers a pre-first-job route suffix through the first job completion, not a setVariables step (#1246 review)', () => {
  // S -> AND -> {XOR, T1 -> XOR}; XOR -> {EVT, E}; EVT -> {M1 -> E, M2 -> E}.
  // A message-first command order (publishMessage before any job completion)
  // that must re-route XOR's second activation has NO job activation before the
  // publish — so a `setVariables` step there has no instance key and is
  // rejected by Camunda on a valid process. The suffix must ride the FIRST job
  // completion's `variables` instead (a job completion carries variables and
  // runs before the token drains to the gateway).
  const g = graphOf(['and', 'xor', 'evt'], [[0, 1, 'direct'], [1, 2, 'direct'], [1, 2, 'task'], [2, 4, 'direct'], [2, 3, 'direct'], [3, 4, 'msg'], [3, 4, 'msg']])
  const { scenarios } = scenariosFor(g)
  let msgFirstReRoute = 0
  for (const s of scenarios) {
    const firstJob = s.steps.findIndex((st) => st.op === 'completeOneJob')
    const earlySet = s.steps.findIndex((st) => st.op === 'setVariables')
    const msgFirst = s.steps[0]?.op === 'publishMessage'
    if (msgFirst) {
      // No setVariables may precede the first job completion (no instance key).
      assert.ok(earlySet < 0 || firstJob >= 0 && firstJob < earlySet,
        `${s.name}: no setVariables before the first job activation`)
    }
    if (msgFirst && firstJob >= 0 && s.steps[firstJob].variables) {
      msgFirstReRoute++
      assert.ok(s.steps[firstJob].variables[routeVariable('G3')], `${s.name}: the route suffix rides the first completion`)
    }
  }
  assert.ok(msgFirstReRoute > 0, 'the graph produces a message-first re-route scenario')
})

test('a routed join reads its OWN activation choice, not the next one (#1246 review)', () => {
  // S -> AND -2x-> OR-join -> {E, E}: the inclusive gateway G2 is BOTH a join
  // (two incoming flows) and a split (two outgoing). `choice()` indexes
  // `route[route_G2][activations_G2]`, so the join branch must read the choice
  // BEFORE advancing the counter — incrementing first skips firing#0's route
  // entry and reads firing#1's. With firing#0 routed to f4 and firing#1 to f5,
  // the first (and here only) firing must take f4; taking f5 is the skip.
  const g = graphOf(['and', 'or'], [[0, 1, 'direct'], [1, 2, 'direct'], [1, 2, 'direct'], [2, 3, 'direct'], [2, 3, 'direct']])
  const rv = routeVariable('G2')
  const ref = new Reference(g, { [rv]: [['f4'], ['f5']] })
  assert.equal(ref.activations.G2, 1, 'the join fired once during the create settle')
  assert.equal(ref.takenFlows.f4, 1, 'firing#0 took its OWN route entry (index 0)')
  assert.equal(ref.takenFlows.f5, undefined, 'firing#1 route entry (index 1) was NOT consumed early')
})

test('a correlation into a SURVIVING instance (parallel branch still running) asks for a correlation acknowledgement (#1246 review)', () => {
  // S -> AND -> {XOR, T1 -> XOR}; XOR -> {EVT, E}; EVT -> {M1 -> E, M2 -> E}.
  // A message-first scenario that correlates M1 while the T1 branch is still
  // running creates NO job (M1 -> E) and the instance does NOT complete (T1 is
  // still active). With no barrier the driver would advance and complete T1
  // BEFORE the buffered message correlates — inverting the scripted order. The
  // step must carry `settled.ack` so the backend acknowledges the correlation
  // (a synchronous /messages/correlation) before advancing.
  const g = graphOf(['and', 'xor', 'evt'], [[0, 1, 'direct'], [1, 2, 'direct'], [1, 2, 'task'], [2, 4, 'direct'], [2, 3, 'direct'], [3, 4, 'msg'], [3, 4, 'msg']])
  const { scenarios } = scenariosFor(g)
  let acked = 0
  for (const s of scenarios) {
    s.steps.forEach((st, i) => {
      if (st.op !== 'publishMessage') return
      const laterJob = s.steps.slice(i + 1).some((x) => x.op === 'completeOneJob')
      if (st.settled?.ack) {
        acked++
        // An ack'd correlation must not also carry a job barrier.
        assert.equal(st.settled.jobType, undefined, `${s.name}: ack and jobType are mutually exclusive`)
      } else if (st.settled?.jobType) {
        // a correlation-created job barrier: fine
      } else {
        // No barrier at all is only acceptable when the correlation COMPLETES
        // the instance (the awaitCompletion create settling is the barrier) —
        // i.e. no job step follows.
        assert.ok(!laterJob, `${s.name}: an unbarriered correlation into a surviving instance inverts the order`)
      }
    })
  }
  assert.ok(acked > 0, 'the graph produces a correlation into a surviving instance that needs an ack')
})

test('a correlation whose catch leads straight to the end event gets NO job barrier (#1246 review)', () => {
  // S -> AND -> {T1 -> E, EVT -> {M1 -> E, M2 -> E}}: correlating M1 (or M2)
  // resumes the catch straight into the end event — it creates NO job. The
  // already-waiting T1 is left as the sole task, so `barrierJob()` would name
  // T1; but T1 was waiting BEFORE the correlation, so Camunda can activate and
  // release it before the message correlates — no barrier at all. The step
  // must carry no `settled` jobType (the awaitCompletion create settling is
  // the barrier), never the pre-existing task.
  const g = graphOf(['and', 'evt'], [[0, 1, 'direct'], [1, 2, 'direct'], [1, 3, 'task'], [2, 3, 'msg'], [2, 3, 'msg']])
  const { scenarios } = scenariosFor(g)
  let publishes = 0
  for (const s of scenarios) {
    for (const step of s.steps) {
      if (step.op !== 'publishMessage') continue
      publishes++
      assert.equal(step.settled?.jobType, undefined, `correlation into the end event must not barrier on the pre-existing task (${s.name})`)
    }
  }
  assert.ok(publishes > 0, 'the graph produces publishMessage steps')
})

test('a correlation creating a job at an ALREADY-WAITING type falls back to settled.ack, not a type barrier (#1246 review)', () => {
  // S -> AND -> {EVT, XOR}; EVT -> {M1, M2} -> XOR; XOR -> T1 -> E. The create
  // settle drains the AND's direct token through the XOR to T1, so a T1 job is
  // ALREADY waiting (jobsCreated[T1] = 1) before any command. Correlating M1
  // routes the XOR to T1 AGAIN, creating a SECOND job of the SAME type
  // (jobsCreated[T1] = 2). `barrierJob()` still names T1 (the sole waiting
  // task), but Camunda's type-only activation can return the OLDER job before
  // the message correlates — the type barrier does not acknowledge THIS
  // correlation. The step must therefore NOT carry `settled.jobType`; with the
  // instance surviving (the second T1 job is still waiting) it must carry
  // `settled.ack` so the backend acknowledges the correlation before advancing.
  const g = graphOf(['and', 'evt', 'xor'], [[0, 1, 'direct'], [1, 2, 'direct'], [1, 3, 'direct'], [2, 3, 'msg'], [2, 3, 'msg'], [3, 4, 'task']])
  const { scenarios } = scenariosFor(g)
  let acked = 0
  let barriered = 0
  for (const s of scenarios) {
    s.steps.forEach((step, i) => {
      if (step.op !== 'publishMessage') return
      const messageFirst = !s.steps.slice(0, i).some((x) => x.op === 'completeOneJob')
      if (messageFirst) {
        // Message-first: the create settle already left a T1 job waiting, so the
        // correlation creates a SECOND job of that already-waiting type. Never a
        // type barrier (Camunda could return the older job); the instance
        // survives (the second T1 job is still waiting), so it must be ack'd.
        assert.equal(step.settled?.jobType, undefined,
          `message-first correlation creating a second job at an already-waiting type must not barrier on that type (${s.name})`)
        assert.equal(step.settled?.ack, true,
          `message-first correlation into a surviving instance must be acknowledged (${s.name})`)
        acked++
      } else {
        // Job-first: the first T1 job was completed before the correlation, so
        // the correlation creates the ONLY waiting T1 job — a valid type barrier.
        assert.equal(step.settled?.jobType, 'T1',
          `job-first correlation creating the only waiting job keeps the type barrier (${s.name})`)
        assert.equal(step.settled?.ack, undefined, `${s.name}: a type barrier needs no separate ack`)
        barriered++
      }
    })
  }
  assert.ok(acked > 0, 'the graph produces a message-first correlation into an already-waiting type that needs an ack')
  assert.ok(barriered > 0, 'the graph still produces a job-first correlation that validly barriers on the created job')
})

test('event-based gateway oracle: substituting the other catch FAILS the branch-sensitive expectation', () => {
  // The negative oracle guard (#1246 review): the two catches of an event-based
  // gateway produce identical completion/variables AND identical completed
  // elements (the engine arms both, withdrawing the loser). Only the taken
  // OUTGOING flow distinguishes them — swapping the correlated catch takes the
  // other catch's continuation and must mismatch the sequenceFlows oracle.
  const g = graphOf(['evt'], [[0, 1, 'task'], [1, 2, 'msg'], [1, 2, 'msg']])
  const { scenarios } = scenariosFor(g)
  const [m1, m2] = scenarios
  // Both complete both catches (the loser is withdrawn)…
  assert.equal(m1.expect.completedElements.M1, 1)
  assert.equal(m1.expect.completedElements.M2, 1)
  // …but only the SELECTED catch's outgoing flow is taken.
  assert.equal(m1.expect.sequenceFlows['M1->E'], 1)
  assert.equal(m1.expect.sequenceFlows['M2->E'], undefined)
  assert.equal(m2.expect.sequenceFlows['M2->E'], 1)
  assert.equal(m2.expect.sequenceFlows['M1->E'], undefined)
  // An observation that resumed M2 instead of M1 fails M1's flow oracle.
  const wrongCatchFlows = { ...m1.expect.sequenceFlows, 'M2->E': 1 }
  delete wrongCatchFlows['M1->E']
  assert.notDeepEqual(wrongCatchFlows, m1.expect.sequenceFlows,
    'resuming the wrong catch must be observable in the taken-flow multiset')
})

test('artifacts: variable-routed BPMN with DI, messages, and MC/ZMC models', () => {
  const g = graphOf(['evt', 'xor'], [[0, 2, 'direct'], [2, 1, 'direct'], [2, 3, 'task'], [1, 3, 'msg'], [1, 3, 'msg']])
  const a = artifactsFor(g)
  assert.match(a.bpmn, /<bpmn:eventBasedGateway /)
  assert.match(a.bpmn, /<bpmn:messageEventDefinition messageRef="Message_M1"\/>/)
  assert.match(a.bpmn, new RegExp(`correlationKey="=${CORRELATION_KEY_VARIABLE}"`))
  assert.match(a.bpmn, new RegExp(`=${routeVariable("G")}\\d\\[1\\]\\[1\\] = &quot;f\\d&quot;`))
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
    { signature: 'known', issue: '#1', graph: { key: 'k1' }, scenario: { name: 'k1-o0' } },
    { signature: 'gone', issue: '#2', graph: { key: 'k2' } },
    { signature: 'outside', issue: '#3', graph: { key: 'k3' } },
    { signature: 'unfiled', issue: null, graph: { key: 'k4' } }
  ]
  const inScope = (c) => c.graph.key !== 'k3'
  const r = classify(
    [
      { signature: 'known', entry: { graph: { key: 'k1' } }, scenario: { name: 'k1-o0' } },
      { signature: 'new', entry: { graph: { key: 'k9' } } },
      { signature: 'unfiled', entry: { graph: { key: 'k4' } } }
    ],
    committed,
    inScope
  )
  assert.deepEqual(r.known.map((d) => d.signature), ['known', 'unfiled'])
  assert.deepEqual(r.fresh.map((d) => d.signature), ['new'])
  assert.deepEqual(r.stale.map((c) => c.signature), ['gone'])
  assert.deepEqual(r.unfiled.map((c) => c.signature), ['unfiled'])
})

test('a committed case is validated against its OWN reproducer, not just its signature (#1246 review)', () => {
  // The committed case's graph stopped failing, but ANOTHER graph still trips
  // the same field-based signature: signature-only classification would call
  // the case "known" and pass, yet the committed reproduction no longer
  // reproduces. It must surface as `obsolete` (re-record or delete), not pass.
  const committed = [
    { signature: 'nano:variables:completed=true/false', issue: '#7', graph: { key: 'old' }, scenario: { name: 'old-o0' } }
  ]
  const r = classify(
    [{ signature: 'nano:variables:completed=true/false', entry: { graph: { key: 'other' } }, scenario: { name: 'other-o3' } }],
    committed,
    () => true
  )
  assert.deepEqual(r.known.map((d) => d.signature), ['nano:variables:completed=true/false'], 'the signature still reproduces on some graph')
  assert.deepEqual(r.obsolete.map((c) => c.signature), ['nano:variables:completed=true/false'], 'but the committed case itself did not')
  assert.deepEqual(r.stale, [], 'not stale: the signature still reproduces')
  // When the committed case's OWN graph/scenario reproduces, it is not obsolete.
  const ok = classify(
    [{ signature: 'nano:variables:completed=true/false', entry: { graph: { key: 'old' } }, scenario: { name: 'old-o0' } }],
    committed,
    () => true
  )
  assert.deepEqual(ok.obsolete, [])
})

test('a committed case is NOT obsolete when shrink discards its reproducer but the full divergences still have it (#1246 review)', () => {
  // shrink() keeps ONE minimal case per signature. A committed case whose own
  // graph/scenario still fails — but is NOT the shrunk minimal case (a smaller
  // graph now wins the same signature) — must NOT be reported `obsolete`: the
  // committed reproduction still reproduces, shrink merely discarded it. So
  // classification validates against the FULL divergences, not the shrunk list.
  const committed = [
    { signature: 'nano:variables:completed=true/false', issue: '#9', graph: { key: 'big' }, scenario: { name: 'big-o0' } }
  ]
  // The full run produced BOTH the committed reproducer (big) and a smaller
  // graph (small) tripping the same signature; shrink keeps only `small`.
  const sc = { steps: [{}] }
  const full = [
    { signature: 'nano:variables:completed=true/false', entry: { graph: { key: 'big' } }, scenario: { name: 'big-o0', ...sc } },
    { signature: 'nano:variables:completed=true/false', entry: { graph: { key: 'small' } }, scenario: { name: 'small-o0', ...sc } }
  ]
  const order = new Map([['small', 0], ['big', 1]])
  const shrunk = shrink(full, order)
  assert.deepEqual(shrunk.map((d) => d.entry.graph.key), ['small'], 'shrink keeps only the minimal case')
  const r = classify(shrunk, committed, () => true, full)
  assert.deepEqual(r.obsolete, [], 'the committed case still reproduces in the full divergences — not obsolete')
  assert.deepEqual(r.known.map((d) => d.entry.graph.key), ['small'], 'the shrunk minimal case is the reported known case')
  // Without the full set (the old behaviour), the discarded reproducer reads as
  // "did not reproduce" and the committed case is misreported obsolete.
  const lossy = classify(shrunk, committed, () => true)
  assert.deepEqual(lossy.obsolete.map((c) => c.signature), ['nano:variables:completed=true/false'], 'defaulting the full set to the shrunk list reproduces the bug')
})

test('the reproducer key includes the signature: same graph+scenario, different signature, cannot cross-validate (#1246 review)', () => {
  // Two committed cases share a graph and scenario but fail with DIFFERENT
  // signatures. If `(A, g1, s1)` now fails only with known signature B while
  // another graph still produces A, keying the reproducer on graph+scenario
  // alone would let A's committed case pass on B's failure. The signature must
  // be part of the key so each committed case reproduces its OWN failure class.
  const committed = [
    { signature: 'sigA', issue: '#11', graph: { key: 'g1' }, scenario: { name: 's1' } },
    { signature: 'sigB', issue: '#12', graph: { key: 'g1' }, scenario: { name: 's1' } }
  ]
  // This run: g1/s1 fails only with sigB; sigA still reproduces on another graph.
  const full = [
    { signature: 'sigB', entry: { graph: { key: 'g1' } }, scenario: { name: 's1' } },
    { signature: 'sigA', entry: { graph: { key: 'g2' } }, scenario: { name: 's2' } }
  ]
  const r = classify(full, committed, () => true, full)
  assert.deepEqual(r.obsolete.map((c) => c.signature), ['sigA'], 'sigA committed case did not reproduce its OWN class on g1/s1 — obsolete')
  assert.deepEqual(r.obsolete.filter((c) => c.signature === 'sigB'), [], 'sigB reproduced on its own g1/s1 — not obsolete')
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

test('Reference takes a fork\'s flows in reverse document order, as Zeebe (#1693, #1675)', () => {
  // Gen_a0e9274136: G1 forks to G2 (f2) and G3 (f3); G2 can reach the inclusive
  // join G3 over f4 but routes to E (f5). Zeebe takes f3 BEFORE f2, so G3 is
  // evaluated while G2's activation is still in transit and is rejected; G2
  // then routes away and G3 is never re-evaluated — live Camunda 8 strands the
  // instance with no T1 job. A document-order (f2 first) reference drains G2
  // before G3 and wrongly expects completion.
  const g = materialize({ kinds: ['or', 'or', 'or'], edges: [[0, 1, 'direct'], [1, 2, 'direct'], [1, 3, 'direct'], [2, 3, 'direct'], [2, 4, 'direct'], [3, 4, 'task']] })
  const ref = new Reference(g, { route_G1: [['f2', 'f3']], route_G2: [['f5']] })
  assert.equal(ref.completed, false)
  assert.equal(ref.jobsCreated.T1, undefined)
  assert.equal(ref.joinsFired.G3, undefined)
  assert.deepEqual(ref.commands(), [])
  // The order is per element: the reversed fork still takes BOTH flows.
  assert.deepEqual({ ...ref.takenFlows }, { f1: 1, f2: 1, f3: 1, f5: 1 })
})

test('exploreOrders rejects follow combined with maxSteps', async () => {
  const { exploreOrders } = await import('./enumerate.mjs')
  assert.throws(() => exploreOrders({}, {}, { follow: [], maxSteps: 3 }), /mutually exclusive/)
})

test('one publish correlates a message boundary ONCE per instance, as Zeebe (#1988, #1792)', () => {
  // and,xor|0>1d,1>2d,1>2d,2>3n: a parallel fork sends two tokens through an
  // XOR to T1, whose interrupting message boundary B1 routes to E. Zeebe's
  // MessageCorrelateBehavior.correlateToMessageEvents correlates a published
  // message at most once per bpmnProcessId, so ONE publish interrupts one T1
  // activation: B1 completes once and the other T1 token stays parked.
  const g = materialize({ kinds: ['and', 'xor'], edges: [[0, 1, 'direct'], [1, 2, 'direct'], [1, 2, 'direct'], [2, 3, 'bmsg']] })
  const runs = exploreOrders(g)
  for (const r of runs) {
    const publishes = r.steps.filter((s) => s.op === 'correlateBoundary').length
    assert.equal(r.completedElements.B1 ?? 0, publishes, 'each publish fires exactly one B1')
  }
  const ref = new Reference(g)
  assert.equal(ref.waiting.T1, 2)
  const after = ref.apply({ op: 'correlateBoundary', node: 'B1', task: 'T1' })
  assert.equal(after.completedElements.B1, 1)
  assert.equal(after.waiting.T1, 1, 'one publish leaves a T1 token waiting')
  assert.equal(after.completed, false)
})

test('a decision pause inside one tick keeps the gateway routing cache: explore == replay (#2024)', () => {
  // and,xor,xor: two parallel timers reach XOR G2 in ONE tick. The first
  // routes G2 -> G3 and pauses at undecided G3. Resuming (decide) must reuse
  // G2's cached same-command choice for the second timer, exactly as replay
  // with the recorded routes does — not open a fresh G2 decision.
  const g = materialize({ kinds: ['and', 'xor', 'xor'], edges: [[0, 1, 'direct'], [1, 2, 'timer'], [1, 2, 'timer'], [2, 3, 'direct'], [2, 4, 'direct'], [3, 4, 'direct'], [3, 4, 'task']] })
  const paused = new Reference(g, { route_G2: [['f6']] }).apply({ op: 'tick' })
  assert.deepEqual(paused.pendingChoices().map((p) => p.node), ['G3'])
  const explored = paused.decide('G3', 0, ['f9'])
  const replayed = new Reference(g, { route_G2: [['f6']], route_G3: [['f9']] }).apply({ op: 'tick' })
  assert.deepEqual(explored.pendingChoices(), [])
  assert.deepEqual({ ...explored.takenFlows }, { ...replayed.takenFlows })
  assert.deepEqual({ ...explored.waiting }, { ...replayed.waiting })
})
