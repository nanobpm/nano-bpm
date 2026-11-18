// Bounded-exhaustive process-graph enumeration (#1246, part of #1240 / #1224).
//
// Hand-picked MC models found #1233 and #1241 by luck. This module replaces luck
// with a bound: it enumerates EVERY well-formed process graph up to N gateways
// and M sequence flows, reduces them by isomorphism, and derives — through the
// SAME one-source emitters as the hand-written corpus (generate.mjs: tlaFor,
// bpmnFor) — each graph's TLA+ models (MC* for TokenFlow, ZMC* for
// ZeebeTokenFlow), its BPMN (with DI) and its runtime scenario scripts. The
// scenario scripts cover every command order (job completions and message
// correlations), as TLC covers every interleaving, for every routing choice.
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
//
// so a task (and a catch event) has exactly one incoming and one outgoing flow,
// and two wait states are never chained without a gateway between them (a chain
// T1 -> T2 behaves as one task, so it adds nothing but size). Well-formedness:
//
//   - S has exactly one outgoing flow; E has at least one incoming flow.
//   - Every gateway is a SPLIT (1 in, >= 2 out) or a JOIN (>= 2 in, 1 out) —
//     never a mixed gateway, never a 1-in-1-out no-op.
//   - An event-based gateway (`evt`) is a split whose every outgoing edge is a
//     `msg` edge; no other node has a `msg` edge.
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
  CORRELATION_KEY_VARIABLE
} from './generate.mjs'

export const GATEWAY_KINDS = ['xor', 'and', 'or', 'evt']
const LABEL_COST = { direct: 1, task: 2, msg: 2 }

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
export function * enumerateSkeletons ({ gateways, flows, kinds = GATEWAY_KINDS }) {
  for (const k of kinds) {
    if (!GATEWAY_KINDS.includes(k)) throw new Error(`unknown gateway kind ${JSON.stringify(k)} (known: ${GATEWAY_KINDS.join(', ')})`)
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
          for (const label of fromEvt ? ['msg'] : ['direct', 'task']) slots.push([i, j, label])
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
  }
  if (outDeg[0] !== 1 || inDeg[0] !== 0) return false
  if (inDeg[E] < 1 || outDeg[E] !== 0) return false
  for (let g = 1; g <= k; g++) {
    const split = inDeg[g] === 1 && outDeg[g] >= 2
    const join = inDeg[g] >= 2 && outDeg[g] === 1
    if (!split && !join) return false
    if (kinds[g - 1] === 'evt' && !split) return false
  }
  return true
}

const edgeStr = ([i, j, label]) => `${i}>${j}${label[0]}`

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
  const hash = createHash('sha256').update(key).digest('hex').slice(0, 10)
  const id = `Gen_${hash}`
  const name = (i) => (i === 0 ? 'S' : i === k + 1 ? 'E' : `G${i}`)
  const nodes = { S: 'start' }
  for (let i = 1; i <= k; i++) nodes[`G${i}`] = kinds[i - 1]
  const graphEdges = []
  let f = 0; let t = 0; let m = 0
  const flow = (from, to) => graphEdges.push({ id: `f${++f}`, from, to })
  for (const [i, j, label] of edges) {
    if (label === 'direct') {
      flow(name(i), name(j))
    } else {
      const w = label === 'task' ? `T${++t}` : `M${++m}`
      nodes[w] = label === 'task' ? 'task' : 'msg'
      flow(name(i), w)
      flow(w, name(j))
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

const outsOf = (graph) => {
  const out = Object.create(null)
  const inn = Object.create(null)
  for (const n of Object.keys(graph.nodes)) { out[n] = []; inn[n] = [] }
  for (const e of graph.edges) { out[e.from].push(e); inn[e.to].push(e) }
  return { out, inn }
}

/**
 * Every routing choice: the product over the diverging exclusive / inclusive
 * gateways of their choices (one flow / a non-empty subset). Each route is the
 * start-variable assignment bpmnFor's variable conditions read. Event-based
 * gateways are not routed by data: their choice is the correlation COMMAND, so it
 * is covered by the command orders instead.
 */
export function routesFor (graph) {
  const { out } = outsOf(graph)
  const splits = Object.keys(graph.nodes)
    .filter((n) => ['xor', 'or'].includes(graph.nodes[n]) && out[n].length > 1)
  let routes = [{}]
  for (const g of splits) {
    const ids = out[g].map((e) => e.id)
    const choices = graph.nodes[g] === 'xor'
      ? ids
      : nonEmptySubsets(ids)
    routes = routes.flatMap((r) => choices.map((c) => ({ ...r, [routeVariable(g)]: c })))
  }
  return routes
}

function nonEmptySubsets (xs) {
  const out = []
  for (let mask = 1; mask < (1 << xs.length); mask++) out.push(xs.filter((_, i) => mask & (1 << i)))
  return out
}

/**
 * A command-level executor of `formal/tla/TokenFlow.tla` for ONE route: the
 * engine's FIFO drain of the activation queue, flows counted on a join when taken,
 * the parallel join guard, the inclusive join's arrival-time
 * `has_active_path_to` guard. It is the scheduler that derives the scenario
 * scripts (which commands are possible after each settled step); TLC remains the
 * oracle for the spec itself and `exhaustive.mjs` cross-checks the two (a stuck
 * reference run must be a TLC Termination/NoStuckInstance violation).
 */
export class Reference {
  constructor (graph, route) {
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
    this.completed = false
    for (const n of Object.keys(graph.nodes)) {
      this.waiting[n] = 0
      this.joinOpen[n] = false
      if (this.isJoin(n)) {
        this.joinTokens[n] = Object.create(null)
        for (const e of inn[n]) this.joinTokens[n][e.id] = 0
      }
    }
    this.settle()
  }

  clone () {
    const c = Object.create(Reference.prototype)
    Object.assign(c, this)
    c.queue = [...this.queue]
    c.waiting = { ...this.waiting }
    c.joinOpen = { ...this.joinOpen }
    c.jobsCreated = { ...this.jobsCreated }
    c.joinTokens = Object.fromEntries(Object.entries(this.joinTokens).map(([j, a]) => [j, { ...a }]))
    return c
  }

  kind (n) { return this.graph.nodes[n] }
  isJoin (n) { return ['and', 'or'].includes(this.kind(n)) && this.inn[n].length > 1 }
  target (f) { return f === '<create>' ? this.graph.start : this.graph.edges.find((e) => e.id === f).to }

  take (flows) {
    for (const e of flows) {
      this.queue.push(e.id)
      if (this.isJoin(e.to)) this.joinTokens[e.to][e.id] += 1
    }
  }

  choice (n) {
    const outs = this.out[n]
    if (outs.length > 1 && this.kind(n) === 'xor') return outs.filter((e) => e.id === this.route[routeVariable(n)])
    if (outs.length > 1 && this.kind(n) === 'or') return outs.filter((e) => this.route[routeVariable(n)].includes(e.id))
    return outs
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
    while (this.queue.length) {
      const f = this.queue.shift()
      const n = this.target(f)
      const kind = this.kind(n)
      if (kind === 'task' || kind === 'evt') {
        this.waiting[n] += 1
        if (kind === 'task') this.jobsCreated[n] = (this.jobsCreated[n] ?? 0) + 1
      } else if (kind === 'end') {
        // the token is consumed
      } else if (this.isJoin(n)) {
        const arr = this.joinTokens[n]
        if (this.joinFires(n)) {
          for (const id of Object.keys(arr)) if (arr[id] > 0) arr[id] -= 1
          this.joinOpen[n] = Object.values(arr).some((c) => c > 0)
          this.take(this.choice(n))
        } else {
          this.joinOpen[n] = this.joinOpen[n] || Object.values(arr).some((c) => c > 0)
        }
      } else {
        this.take(this.choice(n))
      }
    }
    const live = Object.keys(this.graph.nodes).some((n) => this.waiting[n] > 0 || this.joinOpen[n])
    if (!live) this.completed = true
  }

  /** The commands possible in this settled state. */
  commands () {
    if (this.completed) return []
    const cmds = []
    for (const n of Object.keys(this.graph.nodes)) {
      if (this.waiting[n] === 0) continue
      if (this.kind(n) === 'task') cmds.push({ op: 'complete', node: n })
      else for (const e of this.out[n]) cmds.push({ op: 'correlate', node: n, flow: e.id, catch: e.to })
    }
    return cmds
  }

  /** Whether every wait state holds at most one token (see `exploreOrders`). */
  oneSafe () {
    return Object.keys(this.graph.nodes).every((n) => this.waiting[n] <= 1)
  }

  apply (cmd) {
    const next = this.clone()
    next.waiting[cmd.node] -= 1
    next.take(cmd.op === 'complete' ? next.out[cmd.node] : next.out[cmd.node].filter((e) => e.id === cmd.flow))
    next.settle()
    return next
  }
}

/**
 * Every command order from instance creation to a settled state with no command
 * left (completed or stuck), for one route: a depth-first enumeration of the
 * reference's command tree. A run that ever holds two tokens on one wait state is
 * NOT 1-safe; such a run is flagged (`oneSafe: false`) because the runtime
 * driver addresses a job by type and a message by name, so it cannot single out
 * one of two identical wait states — those graphs stay covered by TLC.
 */
export function exploreOrders (graph, route) {
  const runs = []
  const rec = (state, steps, safe) => {
    const ok = safe && state.oneSafe()
    const cmds = state.commands()
    if (cmds.length === 0) {
      runs.push({ steps, completed: state.completed, jobsCreated: { ...state.jobsCreated }, oneSafe: ok })
      return
    }
    for (const c of cmds) rec(state.apply(c), [...steps, c], ok)
  }
  rec(new Reference(graph, route), [], true)
  return runs
}

// ---------------------------------------------------------------------------
// Scenario scripts (the runtime runner's format, formal/parity/runtime/driver.mjs)

/**
 * Every runtime scenario of a graph: one per (route, command order). Steps use
 * the driver's ops — `activateAndComplete` for a job (the job type is the task
 * id, as bpmnFor emits) and `publishMessage` for a correlation. `expect` is the
 * reference outcome. A scenario whose run is not 1-safe is omitted (`skipped`
 * counts them); a non-completing one is marked `backends: ["nano"]`, since the
 * bare-gateway Camunda backend only observes an instance through completion.
 */
export function scenariosFor (graph) {
  const hasMsg = Object.values(graph.nodes).includes('msg')
  const scenarios = []
  let skipped = 0
  routesFor(graph).forEach((route, r) => {
    exploreOrders(graph, route).forEach((run, o) => {
      if (!run.oneSafe) { skipped++; return }
      const name = `${graph.id}-r${r}-o${o}`
      const variables = { ...route }
      if (hasMsg) variables[CORRELATION_KEY_VARIABLE] = name
      const steps = run.steps.map((c) => c.op === 'complete'
        ? { op: 'activateAndComplete', jobType: c.node }
        : { op: 'publishMessage', name: messageName(graph.id, c.catch), correlationKey: name })
      scenarios.push({
        name,
        graph: graph.id,
        bpmn: 'process.bpmn',
        processId: graph.id,
        variables,
        steps,
        expect: { completed: run.completed, jobsCreated: run.jobsCreated, variables },
        ...(run.completed ? {} : { backends: ['nano'] })
      })
    })
  })
  return { scenarios, skipped }
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
