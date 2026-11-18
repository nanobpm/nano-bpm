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
    this.completed = false
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
    c.activations = { ...this.activations }
    c.route = Object.fromEntries(Object.entries(this.route).map(([k, v]) => [k, [...v]]))
    c.joinTokens = Object.fromEntries(Object.entries(this.joinTokens).map(([j, a]) => [j, { ...a }]))
    return c
  }

  kind (n) { return this.graph.nodes[n] }
  isJoin (n) { return ['and', 'or'].includes(this.kind(n)) && this.inn[n].length > 1 }
  isRouted (n) { return ['xor', 'or'].includes(this.kind(n)) && this.out[n].length > 1 }
  target (f) { return f === '<create>' ? this.graph.start : this.graph.edges.find((e) => e.id === f).to }

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
    const settleChoice = Object.create(null)
    while (this.queue.length) {
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
        if (kind === 'task') this.jobsCreated[n] = (this.jobsCreated[n] ?? 0) + 1
      } else if (kind === 'end') {
        this.complete(n) // the token is consumed at the end event
      } else if (this.isJoin(n)) {
        const arr = this.joinTokens[n]
        if (this.joinFires(n)) {
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

  /** Record `n`'s completion in the branch-sensitive oracle. */
  complete (n) {
    this.completedElements[n] = (this.completedElements[n] ?? 0) + 1
  }

  take (flows) {
    for (const e of flows) {
      this.queue.push(e.id)
      // The branch-sensitive milestone: which FLOWS were taken. An event-based
      // gateway arms every catch (each completes, the losers withdrawn), so the
      // catch's COMPLETION does not distinguish the selected branch — its taken
      // outgoing flow does (only the winner's continuation is taken).
      this.takenFlows[e.id] = (this.takenFlows[e.id] ?? 0) + 1
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
    next.waiting[cmd.node] -= 1
    // Completing a task completes the task; correlating completes the gateway.
    // The selected CATCH completes as a pass-through when its flow drains
    // (settle records it). The engine arms EVERY catch of an event-based
    // gateway at once and withdraws the losers when the winner fires
    // (withdraw_event_gateway_siblings), recording each loser's completion too
    // — so the branch-sensitive milestone is the catch's own OUTGOING
    // continuation (only the winner's is taken), not the catch's completion.
    next.complete(cmd.node)
    if (cmd.op === 'correlate') {
      // The engine arms EVERY catch of an event-based gateway at once: the
      // gateway's completion takes every outgoing flow (each catch completes,
      // the losers then withdrawn). So every outgoing flow is taken, and each
      // losing catch completes — but only the WINNER's own outgoing
      // continuation is taken below.
      for (const e of next.out[cmd.node]) {
        if (e.id !== cmd.flow) {
          next.takenFlows[e.id] = (next.takenFlows[e.id] ?? 0) + 1
          next.complete(e.to)
        }
      }
    }
    next.take(cmd.op === 'complete' ? next.out[cmd.node] : next.out[cmd.node].filter((e) => e.id === cmd.flow))
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
export function exploreOrders (graph, route = {}) {
  const runs = []
  const rec = (state, steps) => {
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
    if (cmds.length === 0) {
      runs.push({
        steps,
        route: state.route,
        completed: state.completed,
        jobsCreated: { ...state.jobsCreated },
        completedElements: { ...state.completedElements },
        takenFlows: { ...state.takenFlows }
      })
      return
    }
    for (const c of cmds) rec(state.apply(c), [...steps, c])
  }
  rec(new Reference(graph, route), [])
  return runs
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
export function scenariosFor (graph) {
  const hasMsg = Object.values(graph.nodes).includes('msg')
  const scenarios = []
  exploreOrders(graph).forEach((run, o) => {
    const name = `${graph.id}-o${o}`
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
      } else {
        const step = { op: 'publishMessage', name: messageName(graph.id, c.catch), correlationKey: name }
        if (Object.keys(cmdVars).length > 0) step.variables = cmdVars
        // The barrier must be a job THIS correlation creates, not a task that
        // was already waiting: `barrierJob()` returns the sole waiting task,
        // but when the catch leads straight to the end event the correlation
        // activates no job — it only leaves a PRE-EXISTING task as the sole
        // wait. Long-polling that pre-existing job is no barrier: Camunda can
        // activate and release it before the message correlates, letting the
        // next command run before the message-first step settles (#1246
        // review). Only a job whose count the correlation INCREASED is created
        // by it; otherwise the correlation produces no job and the driver
        // relies on the awaitCompletion create settling as the barrier.
        //
        // The count increase is NOT sufficient on its own: when a job of the
        // SAME type was ALREADY waiting at the time of the correlation
        // (`seeded.waiting[b] > 0`), Camunda's type-only activation can return
        // that OLDER job before the message correlates, so the type barrier
        // still does not acknowledge THIS correlation (#1246 review). Use the
        // job-type barrier only when NO job of that type was waiting; otherwise
        // fall through to the `settled.ack` correlation acknowledgement below.
        const createdByCorrelation = (b) => b &&
          (after.jobsCreated[b] ?? 0) > (seeded.jobsCreated[b] ?? 0) &&
          (seeded.waiting[b] ?? 0) === 0
        const barrier = after.barrierJob()
        if (createdByCorrelation(barrier)) {
          step.settled = { jobType: barrier }
        } else if (!after.completed) {
          // The correlation created no job but the instance SURVIVES (a
          // parallel branch is still running): there is no correlation-created
          // job to barrier on and no completion to await, so without an
          // acknowledgement the driver advances while the buffered message is
          // in flight and Camunda can run the next command (complete the
          // still-waiting task) BEFORE the message correlates — inverting the
          // scripted order (#1246 review). Ask the backend to acknowledge the
          // correlation before advancing.
          step.settled = { ack: true }
        }
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
      ...(run.completed ? {} : { backends: ['nano'] })
    })
  })
  return { scenarios, skipped: 0 }
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
