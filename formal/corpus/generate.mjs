#!/usr/bin/env node
// Single-source corpus generator (#1258, #1240 slice 3).
//
// One graph description under formal/corpus/graphs/<Id>.json is the SINGLE
// source for every downstream artifact of that process graph:
//
//   - formal/tla/<Module>.tla     one per registered spec family (TokenFlow ->
//                                 MC*, ZeebeTokenFlow -> ZMC*), EXTENDS that
//                                 family's base module. This removes the
//                                 hand-maintained MC/ZMC twin duplication.
//   - formal/corpus/bpmn/<Id>.bpmn      BPMN 2.0 XML WITH a generated BPMNDI
//                                 diagram (bounds + waypoints) so it renders.
//   - formal/corpus/scenarios/<Id>.json a scenario script: job-completion order,
//                                 message correlation, timer ticks.
//
//   node formal/corpus/generate.mjs            # (re)write every artifact
//   node formal/corpus/generate.mjs --check    # regenerate to a temp dir and
//                                              # fail on drift (the CI guard)
//
// The generated .tla stay registered through the #1226 per-spec registry
// (specs/TokenFlow.spec globs MC*.tla, specs/ZeebeTokenFlow.spec globs ZMC*.tla)
// with no edit to the descriptors or check.sh, so `formal/tla/check.sh` keeps
// model-checking them. The committed artifacts are a derived product: check.sh
// runs `--check` on a full model-check, the same drift discipline as
// formal/parity and gen-traces.sh, so a forgotten regeneration fails CI.
import fs from 'node:fs'

import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const repoTla = path.join(here, '..', 'tla')

// The spec families a graph may target: the base TLA+ module a generated model
// EXTENDS, and the check.sh glob that keeps it registered.
export const FAMILIES = {
  TokenFlow: { base: 'TokenFlow', glob: 'MC*.tla' },
  ZeebeTokenFlow: { base: 'ZeebeTokenFlow', glob: 'ZMC*.tla' }
}

// A check.sh glob (MC*.tla) as an anchored regex. validateGraphs uses it to
// require a generated module to stay inside its family's descriptor glob, and
// the --check orphan scan uses the SAME rule to spot stray generated models —
// one source of truth, so the two can never drift (#1258 review).
const globToRe = (glob) =>
  new RegExp('^' + glob.replace(/[.]/g, '\\$&').replace(/\*/g, '.*') + '$')

// The document-wide BPMN ids bpmnFor synthesises AROUND the per-node/flow ids:
// the definitions id, the two constant BPMNDI container ids, and a `${id}_di`
// shape/edge per node and flow. Defined once so both the emitter (bpmnFor) and
// validateGraphs's id-collision guard derive the SAME xsd:ID space with no
// drift — a node named `A_di` and a node `A` both map to shape id `A_di`, a
// duplicate xsd:ID the guard must reject (#1258 review).
const BPMN_DIAGRAM_ID = 'BPMNDiagram_1'
const BPMN_PLANE_ID = 'BPMNPlane_1'
const definitionsId = (graphId) => `Definitions_${graphId}`
const diId = (id) => `${id}_di`

// node kind -> BPMN element + diagram footprint. `task` becomes a serviceTask
// whose job type is the node id (the same key the trace tables and scenario
// jobs use).
const KIND = {
  start: { el: 'startEvent', w: 36, h: 36 },
  end: { el: 'endEvent', w: 36, h: 36 },
  task: { el: 'serviceTask', w: 110, h: 80 },
  and: { el: 'parallelGateway', w: 50, h: 50 },
  or: { el: 'inclusiveGateway', w: 50, h: 50 },
  xor: { el: 'exclusiveGateway', w: 50, h: 50 }
}
// CASE arm order for MCKind; `task` is the OTHER fallback.
const KIND_ORDER = ['start', 'end', 'and', 'or', 'xor']

// The one grammar every corpus identifier must obey. Graph ids, family module
// names, node ids and flow (edge) ids all flow verbatim into generated
// artifacts, so a hostile character breaks a downstream model:
//   - flow ids are emitted as BARE TLA+ record fields (MCEdges == [f1 |-> ..]),
//     and node ids / endpoints as TLA+ string literals — a `"`, `\` or newline
//     produces invalid TLA+ while the BPMN side (xmlEscape) would silently
//     diverge. Escaping the string literals could not rescue the bare-field
//     case, so we REJECT out-of-grammar ids rather than escape (#1258 review).
//   - graph ids and module names are also used as file-name components and
//     TLA+/BPMN identifiers.
// This grammar is a valid TLA+ identifier, an XML NCName, and a safe filename
// component, so a graph that passes here generates a well-formed model.
export const SAFE_ID = /^[A-Za-z][A-Za-z0-9_]*$/

// SAFE_ID matches the TLA+ *lexical* identifier grammar, but the TLA+ keywords
// are a reserved subset a graph must still not use: a reserved word emitted as a
// BARE flow-id record field (MCEdges == [MODULE |-> ..]) or as a family module
// name (---- MODULE MODULE ----) is a syntax error even though it matches
// SAFE_ID (#1258 review). Reject the full reserved set on every checked id so a
// valid graph source can never generate unparsable TLA+.
export const TLA_RESERVED = new Set([
  'ASSUME', 'ASSUMPTION', 'AXIOM', 'BOOLEAN', 'CASE', 'CHOOSE', 'CONSTANT',
  'CONSTANTS', 'DOMAIN', 'ELSE', 'ENABLED', 'EXCEPT', 'EXTENDS', 'FALSE', 'IF',
  'IN', 'INSTANCE', 'LET', 'LOCAL', 'MODULE', 'OTHER', 'SF_', 'STRING',
  'SUBSET', 'THEN', 'THEOREM', 'TRUE', 'UNCHANGED', 'UNION', 'VARIABLE',
  'VARIABLES', 'WF_', 'WITH'
])

// Reject a corpus that would generate colliding or malformed artifacts BEFORE
// anything is emitted. `artifacts()` keys outputs by graph id + family module,
// and `--check` builds a DEDUPLICATED expected-path set, so a duplicate id or
// module would let one graph silently overwrite another while --check still
// reports clean (#1258 review). Duplicate flow ids within a graph collapse the
// same way in the TLA+ MCEdges record. A graph must also target only a KNOWN
// spec family and name its generated module inside that family's descriptor glob
// (TokenFlow -> MC*, ZeebeTokenFlow -> ZMC*): a module like `Foo` (or the base
// `TokenFlow`) writes formal/tla/Foo.tla outside the MC*/ZMC* glob — never
// model-checked, and able to clobber the hand-written base — while --check still
// reports clean. And every BPMN id in one process (the process id `g.id`, node
// ids, flow ids, plus the document-wide `Definitions_*`/`*_di` ids bpmnFor
// derives from them) must be unique, or `bpmnFor` emits duplicate XML `id=` and
// the diagram is invalid even though each id passes the grammar. Edge endpoints
// and the start must reference real nodes, a graph must target at least one
// known family, and the deterministic route must terminate (a taken xor branch
// that loops back yields a non-executable BPMN/scenario pair). Fail loudly on any.
export function validateGraphs (graphs) {
  const bad = (msg) => { throw new Error(`corpus graph invalid: ${msg}`) }
  const checkId = (label, value) => {
    if (typeof value !== 'string' || !SAFE_ID.test(value)) {
      bad(`${label} ${JSON.stringify(value)} is not a safe identifier (must match ${SAFE_ID})`)
    }
    if (TLA_RESERVED.has(value)) {
      bad(`${label} ${JSON.stringify(value)} is a TLA+ reserved word`)
    }
  }
  const seenIds = new Map()
  const seenModules = new Map()
  for (const g of graphs) {
    checkId('graph id', g.id)
    if (seenIds.has(g.id)) bad(`duplicate graph id ${JSON.stringify(g.id)}`)
    seenIds.set(g.id, true)
    // Every BPMN id emitted for this one process must be unique across the
    // process id, node ids and flow ids (they share one XML id space).
    const bpmnIds = new Set([g.id])
    const claimBpmnId = (label, value) => {
      if (bpmnIds.has(value)) {
        bad(`graph ${g.id} ${label} ${JSON.stringify(value)} collides with another BPMN id in the same process (process/node/flow ids must be unique)`)
      }
      bpmnIds.add(value)
    }
    if (Object.keys(g.families ?? {}).length === 0) {
      bad(`graph ${g.id} must target at least one spec family (${Object.keys(FAMILIES).join(', ')}); an empty families object emits BPMN/scenario but no model-checked MC*/ZMC* module, silently dropping it from TLC coverage`)
    }
    for (const [fam, spec] of Object.entries(g.families ?? {})) {
      if (!FAMILIES[fam]) {
        bad(`graph ${g.id} targets unknown spec family ${JSON.stringify(fam)} (known: ${Object.keys(FAMILIES).join(', ')})`)
      }
      checkId(`graph ${g.id} family ${fam} module`, spec.module)
      if (!globToRe(FAMILIES[fam].glob).test(`${spec.module}.tla`)) {
        bad(`graph ${g.id} family ${fam} module ${JSON.stringify(spec.module)} must match ${FAMILIES[fam].glob} to stay in the descriptor glob (else the generated model is never checked and may clobber the hand-written base)`)
      }
      if (seenModules.has(spec.module)) {
        bad(`duplicate module ${JSON.stringify(spec.module)} (graphs ${seenModules.get(spec.module)} and ${g.id})`)
      }
      seenModules.set(spec.module, g.id)
    }
    const nodeIds = new Set(Object.keys(g.nodes))
    for (const n of nodeIds) { checkId(`graph ${g.id} node id`, n); claimBpmnId('node id', n) }
    checkId(`graph ${g.id} start node`, g.start)
    if (!nodeIds.has(g.start)) bad(`graph ${g.id} start ${JSON.stringify(g.start)} is not one of its nodes`)
    const seenEdgeIds = new Set()
    for (const e of g.edges) {
      checkId(`graph ${g.id} flow id`, e.id)
      if (seenEdgeIds.has(e.id)) bad(`graph ${g.id} has duplicate flow id ${JSON.stringify(e.id)}`)
      seenEdgeIds.add(e.id)
      claimBpmnId('flow id', e.id)
      checkId(`graph ${g.id} flow ${e.id} source`, e.from)
      checkId(`graph ${g.id} flow ${e.id} target`, e.to)
      // Endpoints must reference real nodes, or bpmnFor emits a sourceRef/targetRef
      // pointing at a non-existent element and layout has no coordinates for it.
      if (!nodeIds.has(e.from)) bad(`graph ${g.id} flow ${e.id} source ${JSON.stringify(e.from)} is not a node`)
      if (!nodeIds.has(e.to)) bad(`graph ${g.id} flow ${e.id} target ${JSON.stringify(e.to)} is not a node`)
    }
    // bpmnFor also emits document-wide ids in the SAME xsd:ID space: the
    // definitions id, the two constant BPMNDI container ids, and a `${id}_di`
    // shape/edge per node and flow. Claim them too so e.g. a node `A_di` cannot
    // collide with node `A`'s generated shape id (a duplicate xsd:ID) (#1258).
    claimBpmnId('definitions id', definitionsId(g.id))
    claimBpmnId('diagram id', BPMN_DIAGRAM_ID)
    claimBpmnId('plane id', BPMN_PLANE_ID)
    for (const n of nodeIds) claimBpmnId('shape id', diId(n))
    for (const e of g.edges) claimBpmnId('edge di id', diId(e.id))
    // The deterministic route (takenFlows) must terminate. takenFlows selects one
    // xor branch by declaration order; if a taken edge loops back, the taken
    // subgraph reachable from start has a cycle — the scenario schedules each task
    // once while the BPMN loops forever creating that job, a non-executable pair
    // (#1258 review). Reject it. (A loop whose taken branch is the EXIT, like
    // ExclusiveLoop's X -> E, is acyclic here and passes.)
    const takenRoute = takenFlows(g)
    const takenOut = Object.create(null)
    for (const e of g.edges) if (takenRoute.has(e.id)) (takenOut[e.from] ??= []).push(e.to)
    const dfsState = new Map()
    const findCycle = (u) => {
      dfsState.set(u, 1)
      for (const v of takenOut[u] ?? []) {
        if (dfsState.get(v) === 1) {
          bad(`graph ${g.id} deterministic route loops back through ${JSON.stringify(v)} (a taken xor branch is a back edge); the scenario schedules each task once while the BPMN would loop forever — reject the non-terminating pair`)
        }
        if (!dfsState.has(v)) findCycle(v)
      }
      dfsState.set(u, 2)
    }
    findCycle(g.start)
  }
  return graphs
}

export function loadGraphs () {
  const dir = path.join(here, 'graphs')
  const graphs = fs.readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .sort()
    .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')))
  return validateGraphs(graphs)
}

// ---------------------------------------------------------------------------
// TLA+ model
export function tlaFor (graph, familyName) {
  const fam = graph.families[familyName]
  const nodes = Object.keys(graph.nodes)
  const rule = '-'.repeat(30)
  const lines = []
  lines.push(`${rule} MODULE ${fam.module} ${rule}`)
  lines.push('(* GENERATED from formal/corpus/graphs/' + graph.id +
    '.json by formal/corpus/generate.mjs — DO NOT EDIT BY HAND.')
  lines.push('   Edit the graph source and re-run the generator (see formal/corpus/README.md).')
  lines.push('')
  for (const c of fam.comment) lines.push('   ' + c)
  lines.push('*)')
  lines.push(`EXTENDS ${FAMILIES[familyName].base}`)
  lines.push('')
  lines.push(`MCNodes == {${nodes.map((n) => `"${n}"`).join(', ')}}`)

  // MCKind CASE, grouped by kind in a fixed order, task as OTHER.
  const byKind = {}
  for (const n of nodes) (byKind[graph.nodes[n]] ??= []).push(n)
  const arms = []
  for (const k of KIND_ORDER) {
    const ns = byKind[k]
    if (!ns || ns.length === 0) continue
    const sel = ns.length === 1 ? `n = "${ns[0]}"` : `n \\in {${ns.map((n) => `"${n}"`).join(', ')}}`
    arms.push(`${sel} -> "${k}"`)
  }
  arms.push('OTHER -> "task"')
  lines.push('MCKind   == [n \\in MCNodes |->')
  arms.forEach((a, i) => {
    lines.push(`              ${i === 0 ? 'CASE ' : '  [] '}${a}${i === arms.length - 1 ? ']' : ''}`)
  })

  lines.push(`MCStart  == "${graph.start}"`)
  const edgeLines = graph.edges.map((e) => `${e.id} |-> <<"${e.from}", "${e.to}">>`)
  lines.push('MCEdges  == [' + edgeLines.map((l, i) =>
    (i === 0 ? '' : '             ') + l + (i === edgeLines.length - 1 ? ']' : ',')
  ).join('\n'))
  lines.push('MCFlows  == DOMAIN MCEdges')
  lines.push('MCSrc    == [f \\in MCFlows |-> MCEdges[f][1]]')
  lines.push('MCTgt    == [f \\in MCFlows |-> MCEdges[f][2]]')
  lines.push('='.repeat(77))
  return lines.join('\n') + '\n'
}

// ---------------------------------------------------------------------------
// Layered layout shared by the BPMN diagram (ranks/rows, mirrors the
// processos "Sugiyama-lite" pass so a graph reads left-to-right).
function layout (graph) {
  const nodes = Object.keys(graph.nodes)
  const n = nodes.length
  // ID-indexed maps use null prototypes so a node named `constructor` (or any
  // other Object.prototype key that still matches SAFE_ID) is a plain data key,
  // not an inherited method that would throw on `.push()` / mis-read (#1258
  // review).
  const rank = Object.create(null)
  for (const id of nodes) rank[id] = 0
  for (let it = 0; it < n + 2; it++) {
    for (const e of graph.edges) {
      const nr = Math.min(rank[e.from] + 1, n)
      if (rank[e.to] < nr) rank[e.to] = nr
    }
  }
  const preds = Object.create(null)
  for (const e of graph.edges) (preds[e.to] ??= []).push(e.from)
  const rowOf = Object.create(null)
  const maxRank = Math.max(...nodes.map((id) => rank[id]))
  const desired = (id) => {
    const ps = (preds[id] ?? []).filter((p) => rowOf[p] !== undefined)
    if (ps.length === 0) return 0
    return ps.reduce((s, p) => s + rowOf[p], 0) / ps.length
  }
  for (let r = 0; r <= maxRank; r++) {
    const here2 = nodes.filter((id) => rank[id] === r)
      .sort((a, b) => (desired(a) - desired(b)) || (a < b ? -1 : a > b ? 1 : 0))
    const used = new Set()
    for (const id of here2) {
      let row = Math.max(0, Math.round(desired(id)))
      while (used.has(row)) row++
      used.add(row)
      rowOf[id] = row
    }
  }
  const OX = 160; const OY = 100; const COL = 190; const ROW = 110
  const rects = Object.create(null)
  for (const id of nodes) {
    const { w, h } = KIND[graph.nodes[id]]
    const cx = OX + rank[id] * COL
    const cy = OY + rowOf[id] * ROW
    rects[id] = { x: cx - w / 2, y: cy - h / 2, w, h, cx, cy }
  }
  return rects
}

// --- Orthogonal edge routing -------------------------------------------------
// A generated diagram is only human-renderable if edges do not run through
// unrelated shapes (#1258 review). The layout places nodes on a rank×row grid
// (COL=190 wide, ROW=110 tall; node half-extents <=55 wide / <=40 tall), so the
// column-midpoint verticals and row-midpoint horizontals are guaranteed-clear
// gutters. The simple straight / single-bend route is kept whenever it is
// already clear (the common adjacent case, so most edges are byte-unchanged);
// only edges that would cross an intervening node — same-row spans and
// backward/cyclic edges — detour through those gutters.
const ROUTE_MARGIN = 6
function segHitsRect (a, b, r) {
  const loX = Math.min(a[0], b[0]); const hiX = Math.max(a[0], b[0])
  const loY = Math.min(a[1], b[1]); const hiY = Math.max(a[1], b[1])
  return loX <= r.x + r.w + ROUTE_MARGIN && hiX >= r.x - ROUTE_MARGIN &&
    loY <= r.y + r.h + ROUTE_MARGIN && hiY >= r.y - ROUTE_MARGIN
}
function pathClear (pts, obstacles) {
  for (let i = 0; i + 1 < pts.length; i++) {
    for (const r of obstacles) if (segHitsRect(pts[i], pts[i + 1], r)) return false
  }
  return true
}
function waypoints (s, t, obstacles = []) {
  const sx = s.x + s.w; const sy = s.cy
  const tx = t.x; const ty = t.cy
  const ROW = 110; const COL = 190
  const backward = t.cx <= s.cx
  // Preferred simple routes (unchanged for the many already-clear edges). A
  // backward/cyclic edge is never routed straight: exiting the source's right
  // side toward a target on its left doubles the segment back THROUGH the source
  // shape, so it always takes the loop-under detour below.
  const simple = Math.abs(sy - ty) < 0.5
    ? [[sx, sy], [tx, ty]]
    : (() => { const G = 18; return [[sx, sy], [tx - G, sy], [tx - G, ty], [tx, ty]] })()
  if (!backward && pathClear(simple, obstacles)) return simple
  // The straight route would cross a node (or is a backward edge). Detour
  // through the row-gutter just below the lower of the two endpoints: a U for a
  // backward/same-row edge, and for a forward span exit the source's right
  // gutter, run the clear lane, and re-enter the target's left gutter. Both use
  // only guaranteed-clear column/row midpoint gutters.
  const laneY = Math.max(sy, ty) + ROW / 2
  const candidates = []
  if (backward) {
    // Loop under both nodes: down out of the source, across, up into the target.
    candidates.push([[s.cx, s.y + s.h], [s.cx, laneY], [t.cx, laneY], [t.cx, t.y + t.h]])
  }
  // Forward (or backward fallback): right gutter of source -> lane -> left
  // gutter of target. Column/row midpoints never fall inside a node's extent.
  const vxs = s.cx + COL / 2
  const vxt = t.cx - COL / 2
  candidates.push([[sx, sy], [vxs, sy], [vxs, laneY], [vxt, laneY], [vxt, ty], [tx, ty]])
  for (const c of candidates) if (pathClear(c, obstacles)) return c
  return simple // no clear detour found — keep the simple route rather than fail
}

const xmlEscape = (s) => String(s)
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')

// ---------------------------------------------------------------------------
// Deterministic routing — the SINGLE source both the BPMN conditions and the
// scenario job order derive from, so the two artifacts stay executable as a
// pair (#1258 review). If they disagreed, the scenario could schedule a job for
// a task on a dead exclusive branch — one the BPMN never routes a token to — and
// the runner would wait for a job that is never created. A diverging INCLUSIVE
// (`or`) split takes EVERY outgoing flow; an EXCLUSIVE (`xor`) split takes
// exactly ONE selected branch, leaving the rest as faithfully-present dead
// flows. Returns the set of taken flow ids.
export function takenFlows (graph) {
  const outFlows = Object.create(null)
  for (const e of graph.edges) (outFlows[e.from] ??= []).push(e)
  const taken = new Set()
  for (const [from, outs] of Object.entries(outFlows)) {
    if (graph.nodes[from] === 'xor' && outs.length > 1) {
      taken.add(outs[1].id) // exclusive split: one selected branch is live
    } else {
      for (const e of outs) taken.add(e.id) // every other flow carries a token
    }
  }
  return taken
}

// Nodes a token can actually reach under `takenFlows` — the set the scenario may
// schedule jobs for. A task behind a dead exclusive branch is unreachable, so
// scheduling its job would deadlock the runner (the job is never created).
export function reachableNodes (graph) {
  const taken = takenFlows(graph)
  const outFlows = Object.create(null)
  for (const e of graph.edges) (outFlows[e.from] ??= []).push(e)
  const seen = new Set([graph.start])
  const stack = [graph.start]
  while (stack.length) {
    const id = stack.pop()
    for (const e of outFlows[id] ?? []) {
      if (taken.has(e.id) && !seen.has(e.to)) { seen.add(e.to); stack.push(e.to) }
    }
  }
  return seen
}

// ---------------------------------------------------------------------------
// BPMN 2.0 XML + BPMNDI
export function bpmnFor (graph) {
  const nodes = Object.keys(graph.nodes)
  const outFlows = Object.create(null); const inFlows = Object.create(null)
  for (const e of graph.edges) {
    (outFlows[e.from] ??= []).push(e)
    ;(inFlows[e.to] ??= []).push(e)
  }
  const isSplit = (id) => ['xor', 'or'].includes(graph.nodes[id]) && (outFlows[id]?.length ?? 0) > 1
  const taken = takenFlows(graph)

  const L = []
  L.push('<?xml version="1.0" encoding="UTF-8"?>')
  L.push('<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"')
  L.push('    xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"')
  L.push('    xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"')
  L.push('    xmlns:di="http://www.omg.org/spec/DD/20100524/DI"')
  L.push('    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"')
  L.push('    xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"')
  L.push(`    id="${xmlEscape(definitionsId(graph.id))}" targetNamespace="http://nanobpm.io/corpus">`)
  L.push(`  <bpmn:process id="${xmlEscape(graph.id)}" isExecutable="true">`)

  for (const id of nodes) {
    const kind = graph.nodes[id]
    const el = KIND[kind].el
    const attrs = [`id="${xmlEscape(id)}"`]
    // A diverging gateway declares NO default flow: a default is only ever taken
    // when no outgoing condition matches, but every branch here is conditioned,
    // so a default would be provably unreachable and misrepresent the graph. An
    // INCLUSIVE (`or`) split conditions every branch `=true` (all are taken); an
    // EXCLUSIVE (`xor`) split conditions every branch too — exactly one `=true`
    // and the rest `=false` — so one deterministic route is taken and the other
    // branch is still faithfully present (see the sequenceFlow loop below).
    const kids = []
    for (const e of inFlows[id] ?? []) kids.push(`      <bpmn:incoming>${xmlEscape(e.id)}</bpmn:incoming>`)
    for (const e of outFlows[id] ?? []) kids.push(`      <bpmn:outgoing>${xmlEscape(e.id)}</bpmn:outgoing>`)
    if (kind === 'task') {
      kids.unshift(
        '      <bpmn:extensionElements>',
        `        <zeebe:taskDefinition type="${xmlEscape(id)}"/>`,
        '      </bpmn:extensionElements>')
    }
    if (kids.length === 0) {
      L.push(`    <bpmn:${el} ${attrs.join(' ')}/>`)
    } else {
      L.push(`    <bpmn:${el} ${attrs.join(' ')}>`)
      L.push(...kids)
      L.push(`    </bpmn:${el}>`)
    }
  }

  for (const e of graph.edges) {
    // A diverging gateway conditions ALL its outgoing flows and declares no
    // default. An inclusive (`or`) split makes every branch `=true`, so every
    // branch is taken and matches the scenario's scheduled tasks. An exclusive
    // (`xor`) split takes exactly ONE selected branch (`takenFlows`): that flow
    // is `=true` and every other flow is `=false`, so the engine deterministically
    // routes down the one live branch while the dead branches stay faithfully
    // present — rather than an unreachable `default` an all-`=true` gateway would
    // never fall back to. The scenario derives its job set from the SAME
    // `takenFlows`/`reachableNodes`, so a task behind a `=false` branch is never
    // scheduled for a job the BPMN cannot create. A non-split flow carries no
    // condition.
    let cond = null
    if (isSplit(e.from)) {
      cond = taken.has(e.id) ? '=true' : '=false'
    }
    if (cond) {
      L.push(`    <bpmn:sequenceFlow id="${xmlEscape(e.id)}" sourceRef="${xmlEscape(e.from)}" targetRef="${xmlEscape(e.to)}">`)
      L.push(`      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">${cond}</bpmn:conditionExpression>`)
      L.push('    </bpmn:sequenceFlow>')
    } else {
      L.push(`    <bpmn:sequenceFlow id="${xmlEscape(e.id)}" sourceRef="${xmlEscape(e.from)}" targetRef="${xmlEscape(e.to)}"/>`)
    }
  }
  L.push('  </bpmn:process>')

  // BPMNDI
  const rects = layout(graph)
  const fmt = (v) => String(Math.round(v))
  L.push(`  <bpmndi:BPMNDiagram id="${BPMN_DIAGRAM_ID}">`)
  L.push(`    <bpmndi:BPMNPlane id="${BPMN_PLANE_ID}" bpmnElement="${xmlEscape(graph.id)}">`)
  for (const id of nodes) {
    const b = rects[id]
    const marker = ['xor', 'or'].includes(graph.nodes[id]) ? ' isMarkerVisible="true"' : ''
    L.push(`      <bpmndi:BPMNShape id="${xmlEscape(diId(id))}" bpmnElement="${xmlEscape(id)}"${marker}>`)
    L.push(`        <dc:Bounds x="${fmt(b.x)}" y="${fmt(b.y)}" width="${fmt(b.w)}" height="${fmt(b.h)}"/>`)
    L.push('      </bpmndi:BPMNShape>')
  }
  for (const e of graph.edges) {
    L.push(`      <bpmndi:BPMNEdge id="${xmlEscape(diId(e.id))}" bpmnElement="${xmlEscape(e.id)}">`)
    // Obstacles are every node except this edge's own endpoints, so routing
    // keeps the segments out of unrelated shapes (a renderable diagram).
    const obstacles = nodes.filter((id) => id !== e.from && id !== e.to).map((id) => rects[id])
    for (const [x, y] of waypoints(rects[e.from], rects[e.to], obstacles)) {
      L.push(`        <di:waypoint x="${fmt(x)}" y="${fmt(y)}"/>`)
    }
    L.push('      </bpmndi:BPMNEdge>')
  }
  L.push('    </bpmndi:BPMNPlane>')
  L.push('  </bpmndi:BPMNDiagram>')
  L.push('</bpmn:definitions>')
  return L.join('\n') + '\n'
}

// ---------------------------------------------------------------------------
// Scenario script: job-completion order, message correlation, timer ticks.
export function scenarioFor (graph) {
  const nodes = Object.keys(graph.nodes)
  const rects = layout(graph) // reuse the rank via layout coordinates
  const OX = 160; const COL = 190
  const rankOf = (id) => Math.round((rects[id].cx - OX) / COL)
  // Only a task a token can REACH under the deterministic route (`reachableNodes`)
  // becomes a job: a task behind a dead exclusive branch never activates, so
  // scheduling its job would deadlock the runner (#1258 review). The BPMN
  // conditions derive from the SAME routing, keeping the pair executable.
  const reach = reachableNodes(graph)
  const jobs = nodes.filter((id) => graph.nodes[id] === 'task' && reach.has(id))
    .map((id) => ({ element: id, jobType: id }))
  // A deterministic, causally-plausible completion order: by rank, then id.
  const completionOrder = jobs
    .map((j) => j.element)
    .sort((a, b) => (rankOf(a) - rankOf(b)) || (a < b ? -1 : a > b ? 1 : 0))
  return JSON.stringify({
    // GENERATED — see formal/corpus/generate.mjs. Edit the graph source instead.
    generated: 'formal/corpus/generate.mjs',
    process: graph.id,
    // Every REACHABLE serviceTask becomes a job; the driver completes them in
    // this order (tasks behind a dead exclusive branch never activate).
    jobs,
    jobCompletionOrder: completionOrder,
    // This corpus has no message catch/throw or timer elements yet, so these
    // are empty. They are part of the scenario contract (#1240 slice 3) for the
    // scenario driver (slice 5) and populate once such elements enter a graph.
    messageCorrelation: [],
    timerTicks: []
  }, null, 2) + '\n'
}

// ---------------------------------------------------------------------------
// Emit / drift-check
function artifacts (graph) {
  const out = []
  for (const familyName of Object.keys(graph.families)) {
    out.push({
      path: path.join(repoTla, `${graph.families[familyName].module}.tla`),
      content: tlaFor(graph, familyName)
    })
  }
  out.push({ path: path.join(here, 'bpmn', `${graph.id}.bpmn`), content: bpmnFor(graph) })
  out.push({ path: path.join(here, 'scenarios', `${graph.id}.json`), content: scenarioFor(graph) })
  return out
}

function main () {
  const check = process.argv.includes('--check')
  const graphs = loadGraphs()
  if (check) {
    let drift = false
    for (const g of graphs) {
      for (const a of artifacts(g)) {
        const rel = path.relative(path.join(here, '..', '..'), a.path)
        const current = fs.existsSync(a.path) ? fs.readFileSync(a.path, 'utf8') : null
        if (current !== a.content) {
          console.error(`FAIL corpus drift: ${rel} (run node formal/corpus/generate.mjs and commit)`)
          drift = true
        }
      }
    }
    // A stray generated file whose graph source was removed must also fail.
    const expected = new Set(graphs.flatMap((g) => artifacts(g).map((a) => a.path)))
    for (const sub of ['bpmn', 'scenarios']) {
      const d = path.join(here, sub)
      if (!fs.existsSync(d)) continue
      for (const f of fs.readdirSync(d)) {
        const p = path.join(d, f)
        if (!expected.has(p)) {
          console.error(`FAIL corpus drift: ${path.relative(path.join(here, '..', '..'), p)} has no graph source (delete it)`)
          drift = true
        }
      }
    }
    // The generated MC*/ZMC* models are written straight into formal/tla beside
    // the hand-written base modules (TokenFlow.tla, ZeebeTokenFlow.tla), so scan
    // them too — otherwise removing/renaming a graph leaves a stale model in the
    // descriptor glob while --check reports clean. Restrict the scan to the
    // families' own generated-model globs (MC*.tla / ZMC*.tla) so a hand-written
    // root module — a new spec's base, say — is never mistaken for a stray
    // generated file. Base modules do not match those globs, so they are skipped
    // for free.
    const generatedModelRes = Object.values(FAMILIES).map((f) => globToRe(f.glob))
    const isGeneratedModel = (name) => generatedModelRes.some((re) => re.test(name))
    if (fs.existsSync(repoTla)) {
      for (const ent of fs.readdirSync(repoTla, { withFileTypes: true })) {
        if (!ent.isFile() || !isGeneratedModel(ent.name)) continue
        const p = path.join(repoTla, ent.name)
        if (!expected.has(p)) {
          console.error(`FAIL corpus drift: ${path.relative(path.join(here, '..', '..'), p)} has no graph source (delete it)`)
          drift = true
        }
      }
    }
    if (drift) process.exit(1)
    console.log(`ok    corpus artifacts current (${graphs.length} graphs)`)
    return
  }
  fs.mkdirSync(path.join(here, 'bpmn'), { recursive: true })
  fs.mkdirSync(path.join(here, 'scenarios'), { recursive: true })
  for (const g of graphs) {
    for (const a of artifacts(g)) {
      fs.mkdirSync(path.dirname(a.path), { recursive: true })
      fs.writeFileSync(a.path, a.content)
    }
  }
  console.log(`wrote artifacts for ${graphs.length} graphs`)
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main()
}
