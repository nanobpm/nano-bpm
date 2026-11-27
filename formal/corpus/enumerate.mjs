// Bounded-exhaustive process-graph enumeration (#1246, part of #1240 / #1224).
//
// Hand-picked MC models found #1233 and #1241 by luck. This module replaces luck
// with a bound: it enumerates EVERY well-formed process graph up to N gateways
// and M sequence flows, reduces them by isomorphism, and derives — through the
// SAME one-source emitters as the hand-written corpus (generate.mjs: tlaFor,
// bpmnFor) — each graph's TLA+ models (MC* for TokenFlow, ZMC* for
// ZeebeTokenFlow), its BPMN (with DI) and its runtime scenario scripts. The
// scenario scripts cover every command order (job completions and message
// correlations), as TLC covers every interleaving, and every routing choice —
// chosen PER ACTIVATION of a diverging gateway (the spec's `Choices(n)`), so a
// token that reaches the same gateway twice explores each branching.
//
// `exhaustive.mjs` is the runner that feeds the output through TLC, the Nano
// backend and the Camunda 8 backend. This module is pure (no I/O beyond the
// generator's emitters), so it is unit-tested without a JVM or a wasm build.
//
// ## Well-formed graphs
//
// A graph is built from a SKELETON over the non-wait-state nodes — the start
// event S, the gateways G1..Gk and the end event E — whose edges are labelled:
//
//   direct  a plain sequence flow                         (1 flow)
//   task    a flow through one serviceTask (a wait state)  (2 flows)
//   msg     a flow through one message intermediateCatchEvent (2 flows);
//           exactly the outgoing edges of an event-based gateway
//   timer   a flow through one timer intermediateCatchEvent (2 flows), a wait
//           state the `tick` command (advanceTime) resumes (#1673)
//   sub     a flow through an embedded subProcess wrapping one inner service
//           task (container + inner start/task/end), a wait state from the
//           outside — only its 2 OUTER flows count against the bound (#1673)
//   btimer  a `task` whose serviceTask carries an interrupting TIMER boundary
//           event; the boundary's outgoing flow lands on the edge's target
//           (3 flows: in, task-out, boundary-out)              (#1673)
//   bmsg    the same with an interrupting MESSAGE boundary     (#1673)
//
// so a task (and a catch event) has exactly one incoming and one outgoing flow,
// and two wait states are never chained without a gateway between them (a chain
// T1 -> T2 behaves as one task, so it adds nothing but size). Well-formedness:
//
//   - S has exactly one outgoing flow; E has at least one incoming flow.
//   - Every gateway is a SPLIT (1 in, >= 2 out), a JOIN (>= 2 in, 1 out) or a
//     MIXED gateway (>= 2 in, >= 2 out — synchronise, then route, exactly as
//     engine-core's inclusive/parallel gateways do) — never a 1-in-1-out no-op.
//   - An event-based gateway (`evt`) is a split whose every outgoing edge is a
//     `msg` edge; no other node has a `msg` edge. A `timer` edge is allowed
//     anywhere a `task` edge is (it is a wait state like any other).
//   - A `btimer`/`bmsg` edge carries its boundary's OUTGOING flow to the same
//     target, so the target of such an edge has TWO incoming flows (the task's
//     and the boundary's). That target must be able to receive two tokens over
//     the run: an `xor` merge (pass-through) or an `and`/`or` join/mixed
//     gateway — never a plain 1-in element, which the boundary path would
//     over-activate. The boundary can fire at most once per task activation
//     (interrupting), and only INSTEAD of the task's own completion, so the
//     pair still delivers exactly one token to the target per activation.
//   - The skeleton is acyclic. Every node then lies on a path S -> E (S is the
//     only source, E the only sink), so there is no dead element.
//
// Acyclicity is a deliberate part of the bound: the runtime scenarios drive a
// route chosen by start variables, which a loop would re-take forever. Loops stay
// covered by the hand-written corpus (ExclusiveLoop) and by TLC's fairness.
// Parallel flows (two flows with the same endpoints) ARE enumerated.
import { createHash } from 'node:crypto'

import {
  bpmnFor,
  tlaFor,
  routeVariable,
  messageName,
  CORRELATION_KEY_VARIABLE,
  TIMER_TICK_MS
} from './generate.mjs'

export const GATEWAY_KINDS = ['xor', 'and', 'or', 'evt']
// Every edge label of the skeleton vocabulary (#1673): the wait-state kinds the
// Reference models a command for. `BOUNDARY_LABELS` carry an interrupting
// boundary event on their task.
export const EDGE_LABELS = ['direct', 'task', 'msg', 'timer', 'sub', 'btimer', 'bmsg']
export const BOUNDARY_LABELS = ['btimer', 'bmsg']
const LABEL_COST = { direct: 1, task: 2, msg: 2, timer: 2, sub: 2, btimer: 3, bmsg: 3 }

// ---------------------------------------------------------------------------
// Skeleton enumeration

/**
 * Every well-formed skeleton with at most `gateways` gateways and at most `flows`
 * sequence flows, over the gateway `kinds`. A skeleton is
 * `{ kinds: [kind of G1..Gk], edges: [[from, to, label], ...] }` with node index
 * 0 = S, 1..k = gateways, k+1 = E. Enumerating only edges `from < to` (a
 * topological numbering) yields every DAG; `canonicalKey` then removes the
 * duplicates the numbering introduces.
 */
export function * enumerateSkeletons ({ gateways, flows, kinds = GATEWAY_KINDS, labels = null }) {
  for (const k of kinds) {
    if (!GATEWAY_KINDS.includes(k)) throw new Error(`unknown gateway kind ${JSON.stringify(k)} (known: ${GATEWAY_KINDS.join(', ')})`)
  }
  // The edge labels in scope: the full vocabulary by default, narrowed by
  // `labels` (a fuzzer `--labels` flag shapes the generator the way `--kinds`
  // shapes the gateways). `msg` is forced by an `evt` gateway regardless.
  // Default: the legacy #1246 vocabulary, so the committed exhaustive corpus
  // and its pinned bounds are unchanged; the fuzzer opts into the rest.
  const plain = (labels ?? ['direct', 'task']).filter((l) => l !== 'msg')
  for (const l of plain) {
    if (!EDGE_LABELS.includes(l) || l === 'msg') throw new Error(`unknown edge label ${JSON.stringify(l)} (known: ${EDGE_LABELS.filter((x) => x !== 'msg').join(', ')}; msg is implied by evt)`)
  }
  for (let k = 0; k <= gateways; k++) {
    for (const kindTuple of tuples(kinds, k)) {
      // Slots: (from, to, label) with from < to; S (0) -> E (k+1) only when k = 0.
      const E = k + 1
      const slots = []
      for (let i = 0; i <= k; i++) {
        for (let j = i + 1; j <= E; j++) {
          if (i === 0 && j === E && k > 0) continue // S would need a 2nd out-flow
          const fromEvt = i > 0 && kindTuple[i - 1] === 'evt'
          // A boundary's outgoing flow lands on the edge's target, so a
          // boundary edge may only target a node that accepts two incoming
          // tokens: an `xor` merge or an `and`/`or` join/mixed gateway (E is
          // fine too — it simply consumes both). A 1-in-1-out target would be
          // over-activated by the boundary path.
          const boundaryOk = j === E || (j > 0 && kindTuple[j - 1] !== 'evt')
          for (const label of fromEvt ? ['msg'] : boundaryOk ? plain : plain.filter((l) => !BOUNDARY_LABELS.includes(l))) slots.push([i, j, label])
        }
      }
      yield * multisets(slots, flows, (edges) => wellFormed(kindTuple, edges))
        .map((edges) => ({ kinds: kindTuple, edges }))
    }
  }
}

function * tuples (alphabet, n) {
  if (n === 0) { yield []; return }
  for (const t of tuples(alphabet, n - 1)) for (const a of alphabet) yield [...t, a]
}

// Every multiset over `slots` whose total flow cost is <= budget, filtered.
function multisets (slots, budget, accept) {
  const out = []
  const pick = []
  const rec = (idx, left) => {
    if (idx === slots.length) {
      if (pick.length > 0 && accept(pick)) out.push(pick.map((e) => [...e]))
      return
    }
    const slot = slots[idx]
    const cost = LABEL_COST[slot[2]]
    // S has exactly one outgoing flow: never place a second S-edge.
    const sTaken = slot[0] === 0 && pick.some((e) => e[0] === 0)
    rec(idx + 1, left)
    if (sTaken) return
    let n = 0
    while (left - cost * (n + 1) >= 0) {
      n++
      pick.push(slot)
      rec(idx + 1, left - cost * n)
      if (slot[0] === 0) break
    }
    for (let i = 0; i < n; i++) pick.pop()
  }
  rec(0, budget)
  return out
}

/** The skeleton well-formedness rules (see the module header). */
export function wellFormed (kinds, edges) {
  const k = kinds.length
  const E = k + 1
  const inDeg = new Array(k + 2).fill(0)
  const outDeg = new Array(k + 2).fill(0)
  for (const [i, j, label] of edges) {
    outDeg[i]++
    inDeg[j]++
    const fromEvt = i > 0 && i <= k && kinds[i - 1] === 'evt'
    if (fromEvt !== (label === 'msg')) return false
    // A boundary edge's target receives the boundary's outgoing flow too, so it
    // must be a merge-capable node (a gateway other than the split-only `evt`)
    // or E — never a plain pass-through that the boundary path over-activates.
    if (BOUNDARY_LABELS.includes(label)) {
      if (j !== E && (j === 0 || j > k || kinds[j - 1] === 'evt')) return false
      inDeg[j]++ // the boundary's own outgoing flow
    }
  }
  if (outDeg[0] !== 1 || inDeg[0] !== 0) return false
  if (inDeg[E] < 1 || outDeg[E] !== 0) return false
  for (let g = 1; g <= k; g++) {
    const split = inDeg[g] === 1 && outDeg[g] >= 2
    const join = inDeg[g] >= 2 && outDeg[g] === 1
    // A MIXED gateway (>= 2 in, >= 2 out) synchronises, then routes — supported
    // by the engine (model.rs: an inclusive/parallel gateway that is both a join
    // and a split first synchronises, then evaluates its outgoing conditions) and
    // by the TLA specs (IsJoin synchronises, Choices routes the firing), so the
    // enumeration must include it (#1246's every-well-formed-graph requirement).
    const mixed = inDeg[g] >= 2 && outDeg[g] >= 2
    if (!split && !join && !mixed) return false
    // An event-based gateway is a pure deferred-choice SPLIT: a mixed/join evt
    // would need msg edges only on its outgoing side and a synchronising join on
    // the incoming side, which neither spec models — keep it split-only.
    if (kinds[g - 1] === 'evt' && !split) return false
  }
  return true
}

// One distinct key letter per edge label. The legacy letters (d/t/m) are the
// labels' initials, so legacy graph keys/ids are unchanged; the #1673 labels
// need explicit letters (`timer` would collide with `task`, `btimer` with
// `bmsg`). A drift guard (enumerate.test.mjs) pins that the codes are unique.
export const LABEL_CODE = Object.freeze({ direct: 'd', task: 't', msg: 'm', timer: 'c', sub: 'p', btimer: 'b', bmsg: 'n' })
const edgeStr = ([i, j, label]) => `${i}>${j}${LABEL_CODE[label]}`

/**
 * The isomorphism-invariant key of a skeleton: the lexicographically least
 * encoding over every relabelling of the gateways (S and E are fixed). Two
 * skeletons are isomorphic iff their keys are equal. Returns `{ key, perm }`,
 * where `perm[old] = new` realises the canonical numbering.
 */
export function canonicalKey (skel) {
  const k = skel.kinds.length
  let best = null
  for (const p of permutations(k)) {
    // p maps gateway old index (1..k) -> new index (1..k)
    const map = (i) => (i === 0 || i === k + 1 ? i : p[i - 1])
    const kinds = new Array(k)
    for (let i = 1; i <= k; i++) kinds[map(i) - 1] = skel.kinds[i - 1]
    const edges = skel.edges.map(([i, j, l]) => edgeStr([map(i), map(j), l])).sort()
    const key = `${kinds.join(',')}|${edges.join(',')}`
    if (best === null || key < best.key) best = { key, perm: [0, ...p, k + 1] }
  }
  return best
}

function * permutations (k) {
  const a = Array.from({ length: k }, (_, i) => i + 1)
  const rec = function * (n) {
    if (n <= 1) { yield [...a]; return }
    for (let i = 0; i < n; i++) {
      yield * rec(n - 1)
      const j = n % 2 === 0 ? i : 0
      ;[a[j], a[n - 1]] = [a[n - 1], a[j]]
    }
  }
  yield * rec(k)
}

/** The flow count of a skeleton (its size against the `flows` bound). */
export const flowCount = (skel) => skel.edges.reduce((s, e) => s + LABEL_COST[e[2]], 0)

/**
 * Materialise a skeleton into a corpus graph (the generator's graph-source
 * shape), numbered canonically so isomorphic skeletons yield the SAME graph. The
 * graph id derives from the canonical key, so it is stable across bounds.
 */
export function materialize (skel) {
  const { key, perm } = canonicalKey(skel)
  const k = skel.kinds.length
  const kinds = new Array(k)
  for (let i = 1; i <= k; i++) kinds[perm[i] - 1] = skel.kinds[i - 1]
  const edges = skel.edges.map(([i, j, l]) => [perm[i], perm[j], l])
    .sort((a, b) => (edgeStr(a) < edgeStr(b) ? -1 : edgeStr(a) > edgeStr(b) ? 1 : 0))
  const graph = { boundaryOf: Object.create(null), scopeOf: Object.create(null) }
  const hash = createHash('sha256').update(key).digest('hex').slice(0, 10)
  const id = `Gen_${hash}`
  const name = (i) => (i === 0 ? 'S' : i === k + 1 ? 'E' : `G${i}`)
  const nodes = { S: 'start' }
  for (let i = 1; i <= k; i++) nodes[`G${i}`] = kinds[i - 1]
  const graphEdges = []
  let f = 0; let t = 0; let m = 0; let w = 0; let p = 0; let b = 0
  const flow = (from, to) => graphEdges.push({ id: `f${++f}`, from, to })
  for (const [i, j, label] of edges) {
    if (label === 'direct') {
      flow(name(i), name(j))
    } else if (label === 'sub') {
      // An embedded subProcess wrapping ONE inner service task (#1673): the
      // container `P` is entered by the skeleton edge's entry flow and left by
      // its exit flow; its inner chain (inner start -> inner task -> inner end)
      // is scoped to it. Entering the container activates the inner start; the
      // inner end's arrival completes the container and routes the exit flow.
      // The container carries 2 outer + 3 inner flows, but only the 2 OUTER
      // ones count against the `flows` bound (the inner chain is fixed
      // structure, not enumerated shape).
      const sub = `P${++p}`
      nodes[sub] = 'sub'
      nodes[`${sub}s`] = 'start'
      nodes[`${sub}t`] = 'task'
      nodes[`${sub}e`] = 'subend'
      flow(name(i), sub)
      flow(sub, name(j))
      flow(`${sub}s`, `${sub}t`)
      flow(`${sub}t`, `${sub}e`)
      for (const inner of ['s', 't', 'e']) graph.scopeOf[`${sub}${inner}`] = sub
    } else if (BOUNDARY_LABELS.includes(label)) {
      // A service task carrying an interrupting boundary event (#1673): the
      // task's own outgoing flow AND the boundary's outgoing flow both land on
      // the edge's target. The boundary node has no incoming flow (it is
      // attached, not flowed into); `attachedTo` links it to its task.
      const task = `T${++t}`
      const bd = `B${++b}`
      nodes[task] = 'task'
      nodes[bd] = label === 'btimer' ? 'btimer' : 'bmsg'
      flow(name(i), task)
      flow(task, name(j))
      flow(bd, name(j))
      graph.boundaryOf[bd] = task
    } else {
      const n = label === 'task' ? `T${++t}` : label === 'msg' ? `M${++m}` : `W${++w}`
      nodes[n] = label
      flow(name(i), n)
      flow(n, name(j))
    }
  }
  nodes.E = 'end'
  const shape = `${kinds.length} gateway(s) [${kinds.join(', ') || '-'}], ${graphEdges.length} flow(s)`
  return {
    id,
    key,
    start: 'S',
    nodes,
    edges: graphEdges,
    // The task each boundary node is attached to (boundary node id -> task id),
    // empty when the graph has no boundary events (#1673).
    // (Both maps are omitted when empty, so a legacy-vocabulary graph's
    // serialised form is byte-identical to before #1673.)
    ...(Object.keys(graph.boundaryOf).length > 0 ? { boundaryOf: graph.boundaryOf } : {}),
    // The embedded subProcess each inner node is scoped to (inner node id ->
    // container id), empty when the graph has no subprocesses (#1673).
    ...(Object.keys(graph.scopeOf).length > 0 ? { scopeOf: graph.scopeOf } : {}),
    families: {
      TokenFlow: { module: `MC${id}`, comment: [`Bounded-exhaustive graph ${key}`, shape] },
      ZeebeTokenFlow: { module: `ZMC${id}`, comment: [`Zeebe reference of bounded-exhaustive graph ${key}`, shape] }
    }
  }
}

/**
 * Every well-formed graph up to the bound, one per isomorphism class, ordered by
 * size (flows, then gateways) and then by canonical key — so the FIRST graph
 * exhibiting a failure is a smallest one (the enumeration's own shrinking order).
 */
export function enumerateGraphs (bound) {
  const seen = new Map()
  for (const skel of enumerateSkeletons(bound)) {
    const { key } = canonicalKey(skel)
    if (!seen.has(key)) seen.set(key, skel)
  }
  return [...seen.values()]
    .map((skel) => ({ skel, graph: materialize(skel) }))
    .sort((a, b) => sizeOrder(a.skel, b.skel) || (a.graph.key < b.graph.key ? -1 : 1))
}

const sizeOrder = (a, b) => (flowCount(a) - flowCount(b)) || (a.kinds.length - b.kinds.length)

// ---------------------------------------------------------------------------
// Routes and the reference command-level executor

/**
 * `flows` in the order Zeebe takes them: reverse declaration (document) order
 * (`Reference.take`, #1694). `bpmnFor` declares every `sequenceFlow` in
 * `graph.edges` order, so a flow's index there IS its document position.
 */
export function executionOrder (graph, flows) {
  const position = (e) => graph.edges.indexOf(e)
  return [...flows].sort((a, b) => position(b) - position(a))
}

const outsOf = (graph) => {
  const out = Object.create(null)
  const inn = Object.create(null)
  for (const n of Object.keys(graph.nodes)) { out[n] = []; inn[n] = [] }
  for (const e of graph.edges) { out[e.from].push(e); inn[e.to].push(e) }
  return { out, inn }
}

/**
 * Every routing choice of ONE activation of a diverging exclusive / inclusive
 * gateway: one flow for an `xor`, a non-empty subset for an `or`. The choice is
 * made PER ACTIVATION, not per instance — the TLA specs pick a branch in
 * `Choices(n)` at every drain, and a token that reaches the same gateway twice
 * (a parallel split feeding it through an exclusive merge) may route
 * differently each time. `exploreOrders` therefore branches on the choice at
 * each activation, and a run's `route` is the choice SEQUENCE it took
 * (`routeVariable(g) -> [choice per activation]`), which the scenario replays
 * through the BPMN's variable conditions (generate.mjs reads `route[i]` for the
 * i-th activation).
 */
export function choicesFor (graph, node) {
  const { out } = outsOf(graph)
  const ids = out[node].map((e) => e.id)
  if (ids.length <= 1) return [ids]
  return graph.nodes[node] === 'xor' ? ids.map((id) => [id]) : nonEmptySubsets(ids)
}

/**
 * The routings of a graph, one per choice sequence its diverging gateways can
 * take. Kept for the cross-checks that need the route list (the TLC
 * reference-vs-stuck check); `scenariosFor` derives its own routes from the
 * explored runs, so a gateway activated twice with different choices is covered
 * without enumerating the full (exponential) sequence space up front.
 */
export function routesFor (graph) {
  const seen = new Map()
  for (const run of exploreOrders(graph)) {
    const key = JSON.stringify(run.route)
    if (!seen.has(key)) seen.set(key, run.route)
  }
  return [...seen.values()]
}

function nonEmptySubsets (xs) {
  const out = []
  for (let mask = 1; mask < (1 << xs.length); mask++) out.push(xs.filter((_, i) => mask & (1 << i)))
  return out
}

/**
 * A command-level executor of `formal/tla/TokenFlow.tla`: the engine's FIFO
 * drain of the activation queue, flows counted on a join when taken, the
 * parallel join guard, the inclusive join's arrival-time `has_active_path_to`
 * guard. A diverging gateway's route is chosen PER ACTIVATION (the spec's
 * `Choices(n)`), recorded into `route` as the activation's index-th entry. It
 * is the scheduler that derives the scenario scripts (which commands are
 * possible after each settled step); TLC remains the oracle for the spec itself
 * and `exhaustive.mjs` cross-checks the two (a stuck reference run must be a
 * TLC Termination/NoStuckInstance violation).
 */
export class Reference {
  constructor (graph, route = {}) {
    this.graph = graph
    this.route = route
    const { out, inn } = outsOf(graph)
    this.out = out
    this.inn = inn
    this.queue = ['<create>']
    this.waiting = Object.create(null)
    this.joinOpen = Object.create(null)
    this.joinTokens = Object.create(null)
    this.jobsCreated = Object.create(null)
    // Completed-element multiset (the branch-sensitive oracle): which element
    // instances finished — an event-based gateway resumed on the wrong catch
    // completes the WRONG catch, visible here even when completion and the
    // variables match.
    this.completedElements = Object.create(null)
    this.takenFlows = Object.create(null)
    // The spec's `fired` ghost (#1261): how many times each join gateway has
    // fired (consumed one token per incoming flow and activated). Recorded here
    // so the spec oracle can read the ZeebeTokenFlow `JoinFired` milestone
    // straight off the run instead of re-deriving it.
    this.joinsFired = Object.create(null)
    this.completed = false
    // Set by the `cancel` command (#1673): the instance was terminated by the
    // driver, so it never completes. A cancelled run's scenario is nano-only
    // (the bare-gateway Camunda backend observes an instance only through its
    // awaitCompletion create response, which a cancellation rejects).
    this.cancelled = false
    // Armed timers (#1673): node id -> due clocks (in ticks), one entry per
    // armed activation (a node reached twice holds two timers). A `timer` catch
    // arms on activation; an interrupting timer boundary arms when its task
    // activates. The virtual clock advances one tick per `tick` command.
    // Each entry is `{ due, seq }`: `seq` is the arming order (Zeebe's timer
    // key order), which fixes the order the TRIGGERs of one tick fire in.
    this.clock = 0
    this.timerSeq = 0
    this.timers = Object.create(null)
    // The TRIGGERs of the current tick still to fire, in firing order (#2024).
    // `settle` fires the next one only once the previous one's follow-ups have
    // drained, as Zeebe's ProcessingStateMachine drains each command's
    // follow-ups before it reads the next TRIGGER from the log.
    this.pendingFires = []
    // Per-gateway count of activations already routed: the next choice of `n`
    // is `route[routeVariable(n)][activations[n]]` (padded with its last entry).
    this.activations = Object.create(null)
    for (const n of Object.keys(graph.nodes)) {
      this.waiting[n] = 0
      this.joinOpen[n] = false
      this.activations[n] = 0
      if (this.isJoin(n)) {
        this.joinTokens[n] = Object.create(null)
        for (const e of inn[n]) this.joinTokens[n][e.id] = 0
      }
    }
    this.settleChoice = Object.create(null)
    this.settle()
  }

  clone () {
    const c = Object.create(Reference.prototype)
    Object.assign(c, this)
    c.queue = [...this.queue]
    c.waiting = { ...this.waiting }
    c.joinOpen = { ...this.joinOpen }
    c.jobsCreated = { ...this.jobsCreated }
    c.completedElements = { ...this.completedElements }
    c.takenFlows = { ...this.takenFlows }
    c.joinsFired = { ...this.joinsFired }
    c.activations = { ...this.activations }
    c.timers = Object.fromEntries(Object.entries(this.timers).map(([k, v]) => [k, v.map((t) => ({ ...t }))]))
    c.pendingFires = [...this.pendingFires]
    c.settleChoice = { ...this.settleChoice }
    c.route = Object.fromEntries(Object.entries(this.route).map(([k, v]) => [k, [...v]]))
    c.joinTokens = Object.fromEntries(Object.entries(this.joinTokens).map(([j, a]) => [j, { ...a }]))
    return c
  }

  kind (n) { return this.graph.nodes[n] }
  isJoin (n) { return ['and', 'or'].includes(this.kind(n)) && this.inn[n].length > 1 }
  isRouted (n) { return ['xor', 'or'].includes(this.kind(n)) && this.out[n].length > 1 }
  target (f) { return f === '<create>' ? this.graph.start : this.graph.edges.find((e) => e.id === f).to }
  /** The task a boundary node is attached to (undefined for non-boundaries). */
  attachedTo (n) { return (this.graph.boundaryOf ?? {})[n] }

  /** The flow ids `n`'s next activation takes under `route` (its choice). */
  choice (n, settleChoice) {
    const outs = this.out[n]
    if (!this.isRouted(n)) return outs
    // Within one settle the variable is unchanged, so every same-settle
    // activation reads the SAME choice (the engine evaluates the condition
    // against the un-popped head). Reuse the cached choice for this drain.
    if (settleChoice && settleChoice[n]) return settleChoice[n]
    const seq = this.route[routeVariable(n)]
    const pick = seq[Math.min(this.activations[n], seq.length - 1)]
    const chosen = outs.filter((e) => pick.includes(e.id))
    if (settleChoice) settleChoice[n] = chosen
    return chosen
  }

  /**
   * The pending routing decisions of a settled state: one entry per queued
   * activation of a diverging gateway, each with the choices it may take. The
   * explorer branches on these BEFORE issuing the next command, so a gateway
   * visited twice explores both branchings (per-activation routing).
   */
  pendingChoices () {
    const out = []
    // One decision per gateway per SETTLE: every queued activation of a
    // diverging gateway in this drain reads the SAME (unchanged) route variable,
    // so they share a single choice — whether a pure split reached by two
    // parallel tokens or a join consuming all its incoming tokens in one firing.
    // Per-activation routing is only expressible across a command boundary.
    const seen = new Set()
    for (const f of this.queue) {
      const n = this.target(f)
      if (!this.isRouted(n) || seen.has(n)) continue
      seen.add(n)
      const seq = this.route[routeVariable(n)] ?? []
      const activation = this.activations[n]
      // Only an activation BEYOND the recorded route is a fresh decision; an
      // in-range one is already pinned by the route being replayed.
      if (activation >= seq.length) out.push({ node: n, activation, choices: choicesFor(this.graph, n) })
    }
    return out
  }

  /** Extend the route with `node`'s `activation`-th choice, then re-settle. */
  decide (node, activation, choice) {
    const next = this.clone()
    const key = routeVariable(node)
    const seq = next.route[key] ?? (next.route[key] = [])
    while (seq.length < activation) seq.push(seq[seq.length - 1] ?? choice)
    seq.push([...choice])
    next.settle()
    return next
  }

  liveSources () {
    const s = new Set()
    for (const n of Object.keys(this.graph.nodes)) if (this.waiting[n] > 0 || this.joinOpen[n]) s.add(n)
    for (const f of this.queue) s.add(this.target(f))
    return s
  }

  pathReaches (src, j, blocked) {
    const seen = new Set([src])
    const stack = [src]
    while (stack.length) {
      const n = stack.pop()
      if (n === j) return true
      for (const e of this.out[n]) {
        if (!blocked.has(e.id) && !seen.has(e.to)) { seen.add(e.to); stack.push(e.to) }
      }
      // A waiting task can also leave via an attached boundary event (#1673):
      // its boundary is an alternative path out, so traverse the attachment
      // too — otherwise a live boundary-bearing activation looks unable to
      // reach the join and an inclusive join fires prematurely.
      for (const [bd, task] of Object.entries(this.graph.boundaryOf ?? {})) {
        if (task === n && !seen.has(bd)) { seen.add(bd); stack.push(bd) }
      }
    }
    return false
  }

  joinFires (j) {
    const arr = this.joinTokens[j]
    const ins = this.inn[j]
    const allTaken = ins.every((e) => arr[e.id] >= 1)
    if (this.kind(j) === 'and') return allTaken
    if (allTaken) return true
    if (!ins.some((e) => arr[e.id] > 0)) return false
    const blocked = new Set(ins.filter((e) => arr[e.id] > 0).map((e) => e.id))
    for (const m of this.liveSources()) {
      if (m !== j && this.pathReaches(m, j, blocked)) return false
    }
    return true
  }

  settle () {
    // Within ONE settle (no command intervenes), a gateway's route variable is
    // unchanged, so the engine reads the SAME head choice for every activation
    // it drains here — a pure split reached by two parallel tokens routes both
    // the same way. Per-activation routing is only expressible across a command
    // (a wait state re-reads the variable). Cache the choice for this drain.
    // The cache lives on the state (cloned, reset only by `apply`) so that a
    // decision pause + `decide` resume inside ONE command keeps it (#2024):
    // otherwise exploration re-decides a gateway that replay reuses.
    const settleChoice = this.settleChoice
    for (;;) {
      if (!this.queue.length) {
        // Quiescent: fire the tick's next TRIGGER, if any (#2024).
        if (!this.pendingFires.length) break
        this.fireTimer(this.pendingFires.shift())
        continue
      }
      const f = this.queue[0]
      const n = this.target(f)
      // A queued activation of a diverging gateway whose choice is not yet
      // recorded is a DECISION POINT: stop draining so the explorer branches.
      // A same-settle re-activation is NOT a decision — its choice is cached.
      if (this.isRouted(n) && !settleChoice[n] && (this.route[routeVariable(n)] ?? []).length <= this.activations[n]) return
      this.queue.shift()
      const kind = this.kind(n)
      if (kind === 'task' || kind === 'evt') {
        this.waiting[n] += 1
        if (kind === 'task') {
          this.jobsCreated[n] = (this.jobsCreated[n] ?? 0) + 1
          // Arm the interrupting TIMER boundary attached to this task (#1673):
          // its timer is due one tick after the task activates. (A message
          // boundary opens a subscription instead — no clock state.)
          for (const [bd, task] of Object.entries(this.graph.boundaryOf ?? {})) {
            if (task === n && this.kind(bd) === 'btimer') (this.timers[bd] ??= []).push({ due: this.clock + 1, seq: this.timerSeq++ })
          }
        }
      } else if (kind === 'timer') {
        // A timer intermediate catch (#1673): a wait state resumed by the
        // `tick` command once its timer is due. Every generated timer is due
        // after exactly one tick (the scenario drives the clock).
        this.waiting[n] += 1
        ;(this.timers[n] ??= []).push({ due: this.clock + 1, seq: this.timerSeq++ })
      } else if (kind === 'sub') {
        // Entering an embedded sub-process (#1673): the container rests as a
        // wait state while its inner flow runs — activate its inner start event
        // (a pass-through drained right away). The container completes when the
        // inner end drains (the `subend` branch below).
        this.waiting[n] += 1
        this.complete(`${n}s`)
        this.take(this.out[`${n}s`])
      } else if (kind === 'subend') {
        // A sub-process's inner end event consumes its token; its drain is what
        // completes the container scope, which then routes its outgoing flow.
        this.complete(n)
        const sub = n.slice(0, -1) // `${sub}e` -> the container id
        this.waiting[sub] -= 1
        this.complete(sub)
        this.take(this.out[sub])
      } else if (kind === 'end') {
        this.complete(n) // the token is consumed at the end event
      } else if (this.isJoin(n)) {
        const arr = this.joinTokens[n]
        if (this.joinFires(n)) {
          this.joinsFired[n] = (this.joinsFired[n] ?? 0) + 1
          for (const id of Object.keys(arr)) if (arr[id] > 0) arr[id] -= 1
          this.joinOpen[n] = Object.values(arr).some((c) => c > 0)
          // Read THIS firing's choice BEFORE advancing the activation counter:
          // `choice()` indexes `seq[activations[n]]`, so incrementing first
          // would skip the current choice and read the NEXT activation's route
          // (the pass-through branch below reads-then-increments for the same
          // reason). A routed join (an inclusive gateway that is BOTH a join
          // and a split) must consume its own route entry (#1246 review).
          const flows = this.choice(n, settleChoice)
          this.activations[n] += 1
          this.complete(n)
          this.take(flows)
        } else {
          this.joinOpen[n] = this.joinOpen[n] || Object.values(arr).some((c) => c > 0)
        }
      } else {
        // A routed gateway's choice is read once per SETTLE (the variable is
        // unchanged mid-drain): the FIRST drain of this gateway in the settle
        // reads `seq[activations]`, caches it, and advances the counter;
        // same-settle re-activations reuse the cached choice and do NOT consume
        // another route entry (the engine reads the same un-popped head).
        const firstThisSettle = this.isRouted(n) && !settleChoice[n]
        this.complete(n) // a pass-through element (start, split, msg) completes as it drains
        this.take(this.choice(n, settleChoice))
        if (firstThisSettle) this.activations[n] += 1
      }
    }
    const live = Object.keys(this.graph.nodes).some((n) => this.waiting[n] > 0 || this.joinOpen[n])
    if (!live) this.completed = true
  }

  /** Fire one due timer: a catch completes, a boundary terminates its task. */
  fireTimer (n) {
    const task = this.attachedTo(n)
    // An interrupting timer boundary TERMINATES its task (its job is
    // cancelled, the task never completes); a timer catch completes.
    this.waiting[task ?? n] -= 1
    this.complete(n)
    this.take(this.out[n])
  }

  /** The commands possible in this settled state. */
  commands () {
    if (this.completed || this.cancelled) return []
    const cmds = []
    for (const n of Object.keys(this.graph.nodes)) {
      if (this.kind(n) === 'task' && this.waiting[n] > 0) {
        cmds.push({ op: 'complete', node: n })
        // An interrupting MESSAGE boundary on a waiting task can fire INSTEAD
        // of the task's completion (#1673). A TIMER boundary fires on the
        // `tick` that makes it due (see apply), never as its own command.
        for (const [bd, task] of Object.entries(this.graph.boundaryOf ?? {})) {
          if (task === n && this.kind(bd) === 'bmsg') cmds.push({ op: 'correlateBoundary', node: bd, task: n })
        }
      }
      if (this.waiting[n] === 0) continue
      if (this.kind(n) === 'evt') {
        for (const e of this.out[n]) cmds.push({ op: 'correlate', node: n, flow: e.id, catch: e.to })
      }
    }
    // The clock advances only while some armed timer is not yet due (#1673):
    // the `tick` that makes a `timer` catch / timer boundary fireable. A tick
    // that would release nothing is never offered, so the walk stays finite.
    if (Object.values(this.timers).some((dues) => dues.some((t) => t.due > this.clock))) cmds.push({ op: 'tick' })
    return cmds
  }

  /** Record `n`'s completion in the branch-sensitive oracle. */
  complete (n) {
    this.completedElements[n] = (this.completedElements[n] ?? 0) + 1
  }

  /**
   * Take `flows` (a subset of one node's outgoing flows), queueing their
   * activations in ZEEBE'S EXECUTION ORDER — reverse document order (#1694):
   * Zeebe's `ModelWalker` transforms sibling `sequenceFlow`s last-declared
   * first, so `ExecutableFlowNode.getOutgoing()` (and therefore
   * `takeOutgoingSequenceFlows`) runs in reverse declaration order, and the
   * batch drains the resulting ACTIVATE commands FIFO. The order is
   * observable: an inclusive join reached by a fork's first-declared flow is
   * guarded AFTER a sibling branch that could still reach it has routed away
   * (in document order it would be guarded BEFORE, and park forever).
   * engine-core's single source of the same order is
   * `Element::outgoing_in_execution_order`.
   *
   * `record: false` activates the targets WITHOUT taking the flows: an
   * event-based gateway's trigger activates the winning catch directly
   * (#2004, Zeebe `activateTriggeredEvent`), writing no SEQUENCE_FLOW_TAKEN.
   */
  take (flows, { record = true } = {}) {
    for (const e of executionOrder(this.graph, flows)) {
      this.queue.push(e.id)
      // The branch-sensitive milestone: which FLOWS were taken. An event-based
      // gateway takes no flow (it activates only the winning catch directly,
      // #2004); the winner's completion and continuation show the selection.
      if (record) this.takenFlows[e.id] = (this.takenFlows[e.id] ?? 0) + 1
      if (this.isJoin(e.to)) this.joinTokens[e.to][e.id] += 1
    }
  }

  /**
   * The job type a correlation barrier can wait on: the single task this
   * settled state waits on, when it is the ONLY wait state and the instance is
   * not complete (a completed instance's barrier is the awaitCompletion create
   * settling, and a state with several waits has no single job to poll).
   */
  barrierJob () {
    if (this.completed) return null
    const held = Object.keys(this.graph.nodes).filter((n) => this.waiting[n] > 0 && this.kind(n) === 'task')
    const otherWaits = Object.keys(this.graph.nodes).some((n) => this.waiting[n] > 0 && this.kind(n) !== 'task')
    if (otherWaits || this.queue.length > 0) return null
    return held.length === 1 ? held[0] : null
  }

  apply (cmd) {
    const next = this.clone()
    next.settleChoice = Object.create(null) // a new command re-reads route variables
    // Instance cancellation (#1673): every token is discarded, every timer and
    // subscription withdrawn, and the instance is terminated — it never
    // completes. Nothing drains afterwards.
    if (cmd.op === 'cancel') {
      next.cancelled = true
      next.queue = []
      // A waiting event-based gateway (#2004, Zeebe): the gateway itself
      // holds the token on its own subscriptions, so a cancellation TERMINATES
      // it — it never completes, takes no flow, and no catch was activated.
      for (const n of Object.keys(next.graph.nodes)) { next.waiting[n] = 0; next.joinOpen[n] = false }
      next.timers = Object.create(null)
      return next
    }
    // A clock tick (#1673): advance the virtual clock and fire EVERY timer it
    // makes due within the one command, so no other command can interleave
    // between a timer becoming due and its firing. The TRIGGERs fire one at a
    // time in arming (timer key) order, each drained before the next (#2024):
    // Zeebe's DueDateTimerCheckScheduler writes one TIMER TRIGGER per due timer
    // and ProcessingStateMachine.batchProcessing drains each command's
    // follow-ups before it processes the next command read from the log.
    if (cmd.op === 'tick') {
      next.clock += 1
      const fires = []
      for (const n of Object.keys(next.timers)) {
        for (const t of next.timers[n]) if (t.due <= next.clock) fires.push({ node: n, ...t })
        next.timers[n] = next.timers[n].filter((t) => t.due > next.clock)
        if (next.timers[n].length === 0) delete next.timers[n]
      }
      fires.sort((a, b) => a.due - b.due || a.seq - b.seq)
      next.pendingFires = fires.map((f) => f.node)
      next.settle()
      return next
    }
    if (cmd.op === 'correlateBoundary') {
      // An interrupting message boundary (#1673): the attached task is
      // TERMINATED (its job cancelled, never completed) and the boundary event
      // completes, routing its outgoing flow.
      // One published message correlates AT MOST ONCE per process (#1988):
      // Zeebe's MessageCorrelateBehavior.correlateToMessageEvents skips every
      // further subscription of the same bpmnProcessId once one correlated. So
      // a publish interrupts exactly ONE waiting activation of the task; any
      // other activation stays parked on its own (still open) subscription.
      next.waiting[cmd.task] -= 1
      next.complete(cmd.node)
      next.take(next.out[cmd.node])
      next.settle()
      return next
    }
    next.waiting[cmd.node] -= 1
    // Completing a task completes the task; correlating completes the gateway.
    // The event-based gateway waits on its own subscriptions (#2004, Zeebe
    // `EventBasedGatewayProcessor`): the first trigger completes the gateway
    // and activates only the winning catch directly — no gateway->catch flow
    // is taken — which completes as a pass-through when its flow drains (settle
    // records it). The losing catches are never activated nor completed.
    next.complete(cmd.node)
    if (cmd.op === 'complete') {
      // Completing a task withdraws its still-armed interrupting boundary timer
      // (#1673): the boundary can no longer fire once its activity is gone.
      for (const [bd, task] of Object.entries(next.graph.boundaryOf ?? {})) {
        if (task === cmd.node && next.timers[bd]) {
          next.timers[bd].shift() // the earliest-armed one belongs to the oldest token
          if (next.timers[bd].length === 0) delete next.timers[bd]
        }
      }
    }
    if (cmd.op === 'complete') next.take(next.out[cmd.node])
    else next.take(next.out[cmd.node].filter((e) => e.id === cmd.flow), { record: false })
    next.settle()
    return next
  }
}

/**
 * Every command order from instance creation to a settled state with no command
 * left (completed or stuck), branching on every pending routing decision AND
 * every possible next command — a depth-first enumeration of the reference's
 * (choice × command) tree, so a gateway reached twice explores each per-
 * activation routing. A run is `completed` when the instance finished; a run
 * that ends with wait states still held is stuck (left to TLC / the nano-only
 * scenarios).
 */
export function exploreOrders (graph, route = {}, { limit = Infinity, cancel = false, follow = null, maxSteps = Infinity } = {}) {
  // A `maxSteps` cut yields truncated prefixes, which a guided `follow`
  // exploration must never emit: the two options are mutually exclusive.
  if (follow && maxSteps !== Infinity) throw new Error('exploreOrders: `follow` and `maxSteps` are mutually exclusive')
  const runs = []
  // Instance cancellation (#1673) is opt-in: with `cancel`, every settled
  // state after the first command may also end the run with a `cancel`.
  const rec = (state, steps) => {
    if (runs.length >= limit) return
    // Branch on routing decisions first: a queued unrouted activation of a
    // diverging gateway must choose before any command runs (the choice decides
    // which wait states exist).
    const pending = state.pendingChoices()
    if (pending.length > 0) {
      const p = pending[0]
      for (const c of p.choices) rec(state.decide(p.node, p.activation, c), steps)
      return
    }
    const cmds = state.commands()
    // A cyclic graph (the hand-written ExclusiveLoop) has unboundedly long
    // runs: `maxSteps` unrolls it to a bound, keeping each run cut there as a
    // settled PREFIX (a not-completed run) — still a behaviour to observe.
    if (steps.length >= maxSteps) {
      runs.push(runOf(state, steps))
      return
    }
    if (cancel && steps.length > 0 && cmds.length > 0) cmds.push({ op: 'cancel' })
    if (follow) {
      // Guided: only a run whose command signatures ARE `follow` can yield a
      // scenario with those driver steps, so prune every other command (and
      // any run that ends early or would run past the followed sequence).
      if (steps.length === follow.length) {
        if (cmds.length === 0) runs.push(runOf(state, steps))
        return
      }
      for (const c of cmds) if (commandSignature(graph, c) === follow[steps.length]) rec(state.apply(c), [...steps, c])
      return
    }
    if (cmds.length === 0) {
      runs.push(runOf(state, steps))
      return
    }
    for (const c of cmds) rec(state.apply(c), [...steps, c])
  }
  rec(new Reference(graph, route), [])
  return runs
}

/**
 * The driver-visible signature of a Reference command: what `scenariosFor`
 * turns it into, minus route deliveries. Fails closed on an unknown op.
 */
export function commandSignature (graph, c) {
  switch (c.op) {
    case 'complete': return `job:${c.node}`
    case 'tick': return 'tick'
    case 'cancel': return 'cancel'
    case 'correlate': return `msg:${messageName(graph.id, c.catch)}`
    case 'correlateBoundary': return `msg:${messageName(graph.id, c.node)}`
    default: throw new Error(`commandSignature: unknown command op ${JSON.stringify(c.op)}`)
  }
}

/**
 * The command signatures a scenario's driver steps record (the inverse of
 * `commandSignature`; `setVariables` deliveries carry no command). Fails
 * closed on an unknown step op, so a new step kind cannot be silently skipped.
 */
export function stepSignatures (steps) {
  const out = []
  for (const s of steps) {
    switch (s.op) {
      case 'setVariables': break
      case 'completeOneJob': out.push(`job:${s.jobType}`); break
      case 'advanceTime': out.push('tick'); break
      case 'cancelInstance': out.push('cancel'); break
      case 'publishMessage': out.push(`msg:${s.name}`); break
      default: throw new Error(`stepSignatures: unknown scenario step op ${JSON.stringify(s.op)}`)
    }
  }
  return out
}

/** A finished reference walk in the `exploreOrders` run shape. */
const runOf = (state, steps) => ({
  steps,
  route: state.route,
  completed: state.completed,
  // Set when the run ends in a `cancel` command (#1673): the instance was
  // terminated by the driver. A cancelled run's scenario is nano-only (the
  // bare-gateway Camunda backend observes an instance only through its
  // awaitCompletion create response, which a cancellation rejects).
  cancelled: state.cancelled,
  jobsCreated: { ...state.jobsCreated },
  completedElements: { ...state.completedElements },
  takenFlows: { ...state.takenFlows },
  // The spec's `fired` ghost (#1261): per-join firing count, so the spec
  // oracle reads the ZeebeTokenFlow `JoinFired` milestone off the run.
  joinsFired: { ...state.joinsFired }
})

/**
 * ONE command order from instance creation to a settled state with no command
 * left, chosen by `pick(n)` (an integer in `[0, n)`): the random walk through the
 * SAME (choice x command) tree `exploreOrders` enumerates, for graphs too large
 * to enumerate every order of (the differential fuzzer, #1250). Every pending
 * routing decision and every next command is a `pick`, so a seeded `pick` makes
 * the run deterministic.
 */
export function randomRun (graph, pick, { cancelRate = 0 } = {}) {
  let state = new Reference(graph)
  const steps = []
  for (;;) {
    const pending = state.pendingChoices()
    if (pending.length > 0) {
      const p = pending[0]
      state = state.decide(p.node, p.activation, p.choices[pick(p.choices.length)])
      continue
    }
    const cmds = state.commands()
    if (cmds.length === 0) return runOf(state, steps)
    // Instance cancellation (#1673) is opt-in: with `cancelRate` > 0, each
    // step after the first cancels the instance with that probability.
    const c = cancelRate > 0 && steps.length > 0 && pick(1000) < cancelRate * 1000
      ? { op: 'cancel' }
      : cmds[pick(cmds.length)]
    steps.push(c)
    state = state.apply(c)
  }
}

// ---------------------------------------------------------------------------
// Scenario scripts (the runtime runner's format, formal/parity/runtime/driver.mjs)

/**
 * Every runtime scenario of a graph: one per explored (route, command order)
 * run. Steps use the driver's ops — `completeOneJob` for a job (exactly one job
 * of the task's type, so a multi-token run that holds two same-type jobs is
 * still driven deterministically) and `publishMessage` for a correlation.
 * `expect` is the reference outcome, branch-sensitively: it pins the completed
 * ELEMENT multiset (nano exposes it), so an event-based gateway resumed on the
 * wrong catch fails the oracle even though completion and the variables match.
 * A non-completing run is marked `backends: ["nano"]`, since the bare-gateway
 * Camunda backend only observes an instance through completion.
 */
export function scenariosFor (graph, runs = exploreOrders(graph), { names = (o) => `${graph.id}-o${o}` } = {}) {
  const hasMsg = Object.values(graph.nodes).some((k) => k === 'msg' || k === 'bmsg')
  const scenarios = []
  runs.forEach((run, o) => {
    const name = names(o)
    const variables = {}
    if (hasMsg) variables[CORRELATION_KEY_VARIABLE] = name
    // Replay the run against a fresh reference to schedule its route: whenever
    // the replay reaches a routing decision, decide it from the run's recorded
    // route and emit a `setVariables` step carrying the gateway's REMAINING
    // choice suffix (the BPMN conditions read the head `route[1]`, so the
    // suffix must be set before the activation that drains it). Decisions
    // pending before the FIRST command go into the start variables instead of
    // a step.
    const steps = []
    let state = new Reference(graph)
    // Deliver each gateway's remaining choice suffix (from `from` on) as a
    // `setVariables` step — or, only before the FIRST command is queued, into
    // the start variables. Skips a suffix identical to what the instance holds.
    const held = Object.create(null)
    // A delivery that is due while NO job has completed yet cannot be a
    // `setVariables` step: Camunda's `setVariables` needs the instance key,
    // which only a job ACTIVATION exposes, and a message-first scenario
    // (publishMessage before any job) activates none — the update would
    // deterministically fail on a valid process (#1246 review). The route is
    // due before the next gateway activation, and the first job completion
    // precedes it, so fold the suffix into that completion's `variables` (a
    // job completion carries variables and runs before the token drains to the
    // gateway). Buffer it here; the first `completeOneJob` picks it up.
    let pendingCompletionVars = null
    const deliver = (k, remaining, asStart) => {
      if (JSON.stringify(held[k]) === JSON.stringify(remaining)) return
      held[k] = remaining
      if (asStart) Object.assign(variables, { [k]: remaining })
      else if (steps.some((s) => s.op === 'completeOneJob')) steps.push({ op: 'setVariables', variables: { [k]: remaining } })
      else (pendingCompletionVars ??= {})[k] = remaining
    }
    // Within ONE pending-decision pass, the lowest activation index each
    // gateway paused at: the delivered suffix starts there. It is local to the
    // pass — a persistent index would go stale once the BPMN consumed that
    // activation and pin the head to an old choice (#1246 review).
    const emitPendingDecisions = () => {
      const pendingFrom = Object.create(null)
      let pending = state.pendingChoices()
      const decided = []
      while (pending.length > 0) {
        const p = pending[0]
        // Keep the LOWEST pending index per gateway: the suffix the BPMN needs
        // starts at the first not-yet-drained choice.
        if (pendingFrom[p.node] === undefined) pendingFrom[p.node] = p.activation
        decided.push(p.node)
        state = state.decide(p.node, p.activation, run.route[routeVariable(p.node)][p.activation])
        pending = state.pendingChoices()
      }
      // Only a gateway that actually had a pending decision needs its suffix
      // (re)delivered — an unrelated gateway's held suffix is unchanged. The
      // suffix starts at the FIRST (lowest) pending activation index: `decide`
      // re-settles and may drain an activation, advancing the counter, but the
      // BPMN still needs that choice as the head it reads next.
      for (const node of new Set(decided)) {
        const k = routeVariable(node)
        const seq = state.route[k]
        const from = Math.min(pendingFrom[node], seq.length)
        deliver(k, seq.slice(from), steps.length === 0)
      }
    }
    emitPendingDecisions()
    // The variables the PROCESS produced (the oracle's `variables`): the start
    // variables minus the routing schedule the driver fed in.
    const expectVariables = Object.fromEntries(Object.entries(variables).filter(([k]) => !k.startsWith('route_')))
    // Set when a step has no Camunda settlement barrier (a timer tick).
    let nanoOnly = false
    for (const c of run.steps) {
      // A command's completion drains every queued activation in ONE settle —
      // including a CHAIN of gateways (T1 -> G1 -> G2) that all route before the
      // next wait state. So the route for EVERY gateway the command's settle
      // reaches must be set before the command runs. Apply the command to a
      // scratch reference SEEDED WITH THE RUN'S FULL ROUTE (so its settle
      // proceeds past every decision); for each routed gateway whose activation
      // count the command advances, deliver the suffix starting at the
      // activation index it is about to consume, so the head is its choice.
      const seeded = state.clone()
      seeded.route = Object.fromEntries(Object.entries(run.route).map(([k, v]) => [k, v.map((c) => [...c])]))
      const after = seeded.apply(c)
      const cmdVars = {}
      for (const n of Object.keys(graph.nodes)) {
        if (!state.isRouted(n)) continue
        const k = routeVariable(n)
        const seq = run.route[k]
        if (!seq) continue
        if (after.activations[n] > seeded.activations[n]) {
          // Deliver the suffix from the activation THIS command is about to
          // consume — never a later one. The BPMN reads the HEAD `route[1]` at
          // every activation, so the head must be the choice of the FIRST
          // activation that reads it after this delivery. Starting the slice at
          // `seeded.activations[n]` (the about-to-be-consumed index) is wrong
          // whenever an earlier pending decision already delivered the suffix
          // from a LOWER index: the re-delivery would drop the entries between
          // that index and this one, so the first activation reads what should
          // be a LATER activation's choice (Gen_de3a1f331f-o2: the create-time
          // activation must take f6, but slicing from index 1 delivers [f5], so
          // the BPMN takes f5 and creates T2 twice — a false divergence,
          // #1246 review). Take the lowest index any pending decision for this
          // gateway already delivered from, when it is lower.
          // The instance already ran its create settle, so the suffix can never
          // go back into the start variables (that would overwrite the head the
          // create-time activation already consumed). It rides THIS command's own
          // variables — a job completion's or a message's are merged into the
          // instance before the token drains to the gateway — so it needs no
          // instance key and is in place exactly when the activation reads it.
          const remaining = seq.slice(seeded.activations[n])
          if (JSON.stringify(held[k]) !== JSON.stringify(remaining)) {
            // Before the first command, a gateway the create settle never
            // reached has no held suffix: the start variables can still carry
            // it untouched (nothing has read them yet for this gateway).
            if (steps.length === 0 && held[k] === undefined) Object.assign(variables, { [k]: remaining })
            else cmdVars[k] = remaining
            held[k] = remaining
          }
        }
      }
      if (c.op === 'complete') {
        const step = { op: 'completeOneJob', jobType: c.node }
        // A route suffix buffered while no job had completed rides THIS
        // completion's variables: the completion carries them into the instance
        // before the token drains to the gateway that reads them (#1246 review).
        if (pendingCompletionVars) {
          step.variables = pendingCompletionVars
          pendingCompletionVars = null
        }
        if (Object.keys(cmdVars).length > 0) step.variables = { ...step.variables, ...cmdVars }
        steps.push(step)
      } else if (c.op === 'tick') {
        // Advance the virtual clock one tick (#1673). The driver's advanceTime
        // moves BOTH engines' pinned clocks by the same tick; due timers are
        // released by the engine. The step is a no-op when no timer is armed.
        //
        // A tick that (re)activates a routed gateway (a timer catch / boundary
        // leading to one) must deliver that gateway's route suffix BEFORE the
        // timer fires — the tick carries no variables of its own, so the
        // suffix rides a `setVariables` step placed immediately ahead of it
        // (#1673 review: discarding `cmdVars` here left the gateway reading
        // its previous route).
        // (A suffix the start variables can still carry was already placed
        // there above; `cmdVars` holds only mid-run deliveries.)
        //
        // A route suffix still buffered for the first job completion (a
        // message-first run that ticks before any job completes) rides this
        // `setVariables` step too (#1990): a tick never completes a job, so
        // without this the buffer is never delivered and the gateway reads an
        // unset route (an incident, not a routing). A ticking scenario is
        // nano-only (below), and nano's setVariables needs no job activation.
        const tickVars = { ...pendingCompletionVars, ...cmdVars }
        pendingCompletionVars = null
        if (Object.keys(tickVars).length > 0) steps.push({ op: 'setVariables', variables: tickVars })
        steps.push({ op: 'advanceTime', ms: TIMER_TICK_MS })
        // The bare-gateway Camunda backend has NO timer-readiness barrier: its
        // create is in flight under awaitCompletion and a job completion's or
        // clock update's response does not acknowledge that the next timer is
        // armed, so a tick can overtake the timer it is meant to fire (#1673
        // review). Until a query-API barrier exists, a scenario that ticks is
        // observed by nano only — never a false Camunda divergence.
        nanoOnly = true
      } else if (c.op === 'correlateBoundary') {
        // An interrupting MESSAGE boundary (#1673): correlate its message. The
        // boundary's subscription is keyed on the scenario correlation key,
        // exactly like a catch event's.
        // It takes the SAME settlement barrier as a catch correlation (#1673
        // review): without one Camunda can run the next command before the
        // boundary interrupts its task.
        const step = { op: 'publishMessage', name: messageName(graph.id, c.node), correlationKey: name }
        // A route suffix buffered while no job had completed rides THIS
        // publish, exactly as it would ride a job completion: the message's
        // variables are merged into the instance before the token drains to
        // the gateway that reads them (#1755).
        if (pendingCompletionVars) {
          step.variables = pendingCompletionVars
          pendingCompletionVars = null
        }
        if (Object.keys(cmdVars).length > 0) step.variables = { ...step.variables, ...cmdVars }
        const settled = messageSettlement(seeded, after)
        if (settled) step.settled = settled
        steps.push(step)
      } else if (c.op === 'cancel') {
        // Instance cancellation (#1673): the driver cancels the instance. The
        // run ends here — no further steps, and the scenario is nano-only (a
        // cancelled instance's awaitCompletion create rejects on Camunda, so
        // the bare-gateway backend cannot observe it).
        steps.push({ op: 'cancelInstance' })
      } else {
        const step = { op: 'publishMessage', name: messageName(graph.id, c.catch), correlationKey: name }
        // A route suffix buffered while no job had completed rides THIS
        // publish, exactly as it would ride a job completion: the message's
        // variables are merged into the instance before the token drains to
        // the gateway that reads them (#1755).
        if (pendingCompletionVars) {
          step.variables = pendingCompletionVars
          pendingCompletionVars = null
        }
        if (Object.keys(cmdVars).length > 0) step.variables = { ...step.variables, ...cmdVars }
        // Settlement barrier: see `messageSettlement`.
        const settled = messageSettlement(seeded, after)
        if (settled) step.settled = settled
        steps.push(step)
      }
      state = state.apply(c)
      emitPendingDecisions()
    }
    scenarios.push({
      name,
      graph: graph.id,
      bpmn: 'process.bpmn',
      processId: graph.id,
      variables,
      steps,
      expect: {
        completed: run.completed,
        // A cancelled run (#1673) pins `cancelled: true`: the nano observation
        // reads it off the ProcessInstanceCancelled event. Absent for a run
        // that was not cancelled (the observation defaults to false).
        ...(run.cancelled ? { cancelled: true } : {}),
        jobsCreated: run.jobsCreated,
        // The route variables are the scenario's own schedule, not an outcome —
        // the oracle pins only the variables the PROCESS produced (the
        // correlation key), not the routing schedule the driver fed in.
        variables: expectVariables,
        completedElements: run.completedElements,
        // The branch-sensitive milestone: which flows were taken. An event-based
        // gateway arms every catch (each completes, the losers withdrawn), so
        // only the taken OUTGOING flow of the selected catch distinguishes the
        // branch — resuming the wrong catch takes the wrong flow and fails here.
        sequenceFlows: sequenceFlowExpect(graph, run.takenFlows)
      },
      // The run this scenario was generated from, kept so the spec oracle
      // (#1261) can re-derive the ZeebeTokenFlow milestone multiset the run
      // exhibits (a Reference run IS a behaviour of the spec by construction)
      // and assert the backend's observation equals it. Not serialised into the
      // runtime corpus export (`--out` strips it); it is the in-memory link from
      // a scenario back to its generating run.
      run,
      // A non-completing run — including a CANCELLED one (#1673) — is observed
      // by nano only: the bare-gateway Camunda backend learns an instance's
      // outcome through its awaitCompletion create response, which a stuck or
      // cancelled instance never answers with `processCompleted: true`.
      ...(run.completed && !nanoOnly ? {} : { backends: ['nano'] })
    })
  })
  return { scenarios, skipped: 0 }
}

/**
 * The Camunda settlement barrier for a message correlation — a catch event's
 * OR an interrupting boundary's (#1673 review: one selection for every
 * message step, so no message kind can skip it).
 *
 * The barrier must be a job THIS correlation creates, not a task that was
 * already waiting: long-polling a pre-existing job is no barrier, since Camunda
 * can activate and release it before the message correlates (#1246 review).
 * Only a job whose count the correlation INCREASED is created by it — and only
 * when NO job of that type was already waiting, since type-only activation
 * could return the older job. Otherwise, while the instance survives, ask the
 * backend to acknowledge the correlation (`ack`); a correlation that completes
 * the instance is settled by the awaitCompletion create.
 */
export function messageSettlement (seeded, after) {
  const b = after.barrierJob()
  if (b && (after.jobsCreated[b] ?? 0) > (seeded.jobsCreated[b] ?? 0) && (seeded.waiting[b] ?? 0) === 0) {
    return { jobType: b }
  }
  if (!after.completed) return { ack: true }
  return null
}

// The reference's taken-flow multiset in the observation vocabulary: nano's
// SequenceFlowTaken event carries `from`/`to` (no per-flow id), so two distinct
// flows sharing endpoints collapse to one `from->to` key — matching the engine's
// own observation (and the documented #1227 per-flow-identity gap).
function sequenceFlowExpect (graph, takenFlows) {
  const byId = Object.fromEntries(graph.edges.map((e) => [e.id, e]))
  const out = {}
  for (const [fid, count] of Object.entries(takenFlows)) {
    const e = byId[fid]
    if (!e) continue
    const key = `${e.from}->${e.to}`
    out[key] = (out[key] ?? 0) + count
  }
  return out
}

/**
 * Every artifact of one enumerated graph, from the one source: the MC/ZMC
 * models, the variable-routed BPMN (with DI), and the scenario scripts.
 */
export function artifactsFor (graph) {
  const { scenarios, skipped } = scenariosFor(graph)
  return {
    tla: Object.fromEntries(Object.keys(graph.families).map((fam) => [fam, tlaFor(graph, fam)])),
    bpmn: bpmnFor(graph, { routing: 'variables' }),
    scenarios,
    skipped
  }
}
