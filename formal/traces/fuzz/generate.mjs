#!/usr/bin/env node
// Generative differential testing against Zeebe (#1521): the seeded case
// generator.
//
// One seed deterministically yields one differential-trace scenario:
//
//   - a random, well-formed, executable BPMN model WITH BPMN DI, built from a
//     block-structured tree (sequences of service / user tasks, exclusive /
//     parallel / inclusive gateway blocks, embedded sub-processes, parallel and
//     sequential multi-instance with input/output elements and collections,
//     timer and message catch events, interrupting boundary timers and
//     messages, input/output mappings and call activities), and
//   - a random-but-VALID driver script in the formal/traces script vocabulary
//     (job and user-task completions carrying variables whose names DELIBERATELY
//     collide across parallel branches and multi-instance children, message
//     publishes, clock advances, variable updates), produced by simulating the
//     tree's token flow so every command targets a wait state that is open.
//
// The scenario then runs through the SAME two engines as every hand-written
// scenario: the pinned Zeebe (formal/traces/capture) writes the golden and the
// nano runner (engine-core/tests/zeebe_traces.rs) compares its normalised record
// stream — including VARIABLE records with their scope keys — against it.
//
//   node formal/traces/fuzz/generate.mjs <seed>... [--out <scenarios-dir>]
//       write scenarios/g-<seed>/{process.bpmn,script.json}
//   node formal/traces/fuzz/generate.mjs --budget [--out <dir>]
//       write the fixed-seed CI budget (budget.json)
//   node formal/traces/fuzz/generate.mjs --check
//       fail if the committed g-* scenarios drift from the generator (CI)
//
// The element vocabulary is weighted towards what nano supports (the
// processos ELEMENT_KIND_SPECS registry); the ids reuse the corpus id grammar
// (formal/corpus/generate.mjs SAFE_ID) so generated models obey the same rules.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { SAFE_ID } from '../../corpus/generate.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
export const TRACES_DIR = path.join(here, '..')
export const BUDGET_FILE = path.join(here, 'budget.json')
/** Committed fixed-seed scenarios are `scenarios/<GENERATED_PREFIX><seed>`. */
export const GENERATED_PREFIX = 'g-'
export const PROCESS_ID = 'fuzz'
/** Every timer duration is a whole number of these; `advanceClock` steps by one. */
export const TICK_MS = 60_000

// ───────────────────────────── seeded PRNG ──────────────────────────────

/** mulberry32: a tiny, well-mixed 32-bit PRNG — identical on every platform. */
export function prng (seed) {
  let a = seed >>> 0
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  const int = (lo, hi) => lo + Math.floor(next() * (hi - lo + 1))
  const pick = (xs) => xs[Math.floor(next() * xs.length)]
  const chance = (p) => next() < p
  const weighted = (table) => {
    const total = table.reduce((s, [, w]) => s + w, 0)
    let r = next() * total
    for (const [v, w] of table) {
      r -= w
      if (r < 0) return v
    }
    return table[table.length - 1][0]
  }
  return { next, int, pick, chance, weighted }
}

// ───────────────────────────── the block tree ──────────────────────────────
//
// A model is a SEQUENCE of blocks. Block kinds:
//   task       { id, user?, inputs?, outputs?, boundary?: {kind:'timer'|'message', id, ticks?, message?} }
//   timer      { id, ticks }
//   message    { id, message }
//   xor        { id, join, var, branches: [seq], defaultIdx }
//   and        { id, join, branches: [seq] }
//   or         { id, join, branches: [seq], flags: [var] }
//   sub        { id, body: seq, start, end }
//   mi         { id, sequential, items: var, output, body: task|sub }
//   call       { id, child: { process, task, start, end } }
//
// Every id is unique within the document; `n` is the id counter.

// The colliding variable vocabulary: completions, publishes and updates all
// write from this small set, so parallel branches and multi-instance children
// race on the same names (the #1518 defect class).
export const COLLIDING = ['result', 'v', 'note']

/** The single instance-creation variable carrying all routing inputs. */
export const CONFIG_VAR = 'cfg'

/** Default generation budget; `opts` may override any field (the shrinker does). */
export const DEFAULTS = { maxBlocks: 4, maxDepth: 2, maxSteps: 30 }

export function generateTree (seed, opts = {}) {
  const o = { ...DEFAULTS, ...opts }
  const r = prng(seed)
  let n = 0
  const id = (p) => `${p}${++n}`
  const initVars = { ck: 'k1' }

  // ctx: { depth, inMi }
  const block = (ctx) => {
    const leafy = ctx.depth >= o.maxDepth
    const table = [
      ['task', 6],
      ['timer', 1],
      ['xor', leafy ? 0 : 2],
      ['and', leafy ? 0 : 2],
      ['or', leafy ? 0 : 1],
      ['sub', leafy ? 0 : 1],
      ['mi', leafy || ctx.inMi ? 0 : 3],
      ['message', ctx.inMi ? 0 : 1],
      ['call', ctx.inMi ? 0 : 1]
    ]
    const kind = r.weighted(table)
    const inner = { ...ctx, depth: ctx.depth + 1 }
    switch (kind) {
      case 'task': {
        const t = { kind: 'task', id: id('t') }
        if (!ctx.inMi && r.chance(0.2)) t.user = true
        if (r.chance(0.25)) t.inputs = [{ source: '=cfg.ck', target: 'local' }]
        if (r.chance(0.25)) t.outputs = [{ source: '=result', target: `o${n}` }]
        if (r.chance(0.2)) {
          t.boundary = !ctx.inMi && r.chance(0.4)
            ? { kind: 'message', id: id('b'), message: `msg${n}`, merge: id('m') }
            : { kind: 'timer', id: id('b'), ticks: r.int(1, 2), merge: id('m') }
        }
        return t
      }
      case 'timer': return { kind: 'timer', id: id('w'), ticks: r.int(1, 2) }
      case 'message': {
        const mid = id('c')
        return { kind: 'message', id: mid, message: `msg${n}` }
      }
      case 'xor': {
        const b = { kind: 'xor', id: id('x'), branches: [] }
        b.join = id('xj')
        b.var = `x${n}`
        const k = r.int(2, 3)
        for (let i = 0; i < k; i++) b.branches.push(seq(inner, r.int(1, 2)))
        b.defaultIdx = k - 1
        initVars[b.var] = r.int(0, k - 1)
        return b
      }
      case 'and': {
        const b = { kind: 'and', id: id('p'), branches: [] }
        b.join = id('pj')
        const k = r.int(2, 3)
        for (let i = 0; i < k; i++) b.branches.push(seq(inner, r.int(1, 2)))
        return b
      }
      case 'or': {
        const b = { kind: 'or', id: id('i'), branches: [], flags: [] }
        b.join = id('ij')
        const k = r.int(2, 3)
        const on = []
        for (let i = 0; i < k; i++) {
          b.branches.push(seq(inner, r.int(1, 2)))
          const f = `${b.id}f${i}`
          b.flags.push(f)
          on.push(r.chance(0.6))
        }
        if (!on.some(Boolean)) on[r.int(0, k - 1)] = true
        b.flags.forEach((f, i) => { initVars[f] = on[i] })
        return b
      }
      case 'sub': {
        const s = { kind: 'sub', id: id('s') }
        s.start = id('ss')
        s.body = seq(inner, r.int(1, 2))
        s.end = id('se')
        return s
      }
      case 'mi': {
        const m = { kind: 'mi', id: id('mi'), sequential: r.chance(0.4) }
        const miCtx = { ...inner, inMi: true }
        if (r.chance(0.5)) {
          m.body = { kind: 'sub', id: m.id, start: id('ss'), end: null }
          m.body.body = seq(miCtx, r.int(1, 2))
          m.body.end = id('se')
        } else {
          const t = { kind: 'task', id: m.id }
          if (r.chance(0.2)) t.outputs = [{ source: '=result', target: `o${n}` }]
          m.body = t
        }
        m.items = `items${n}`
        m.output = `out${n}`
        // A bare variable reference: Zeebe seeds it as a nil LOCAL of every
        // inner instance (MultiInstanceBodyProcessor) — #1518.
        m.outputElement = r.chance(0.8) ? '=result' : '=item'
        initVars[m.items] = Array.from({ length: r.int(1, 3) }, (_, i) => i + 1)
        return m
      }
      case 'call': {
        const c = { kind: 'call', id: id('ca') }
        c.child = { process: `${PROCESS_ID}_${c.id}`, start: id('cs'), task: id('ct'), end: id('ce') }
        return c
      }
    }
    throw new Error(`unreachable kind ${kind}`)
  }
  const seq = (ctx, k) => {
    const out = []
    for (let i = 0; i < k; i++) out.push(block(ctx))
    return out
  }
  const body = seq({ depth: 0, inMi: false }, r.int(1, o.maxBlocks))
  return { seed, opts: o, start: 'start', end: 'end', body, initVars }
}

// ───────────────────────────── tree walking ──────────────────────────────

/** Every block of a sequence, depth-first (the shrinker's candidate space). */
export function blocks (seqOrBlock, out = []) {
  const visit = (b) => {
    out.push(b)
    for (const s of children(b)) s.forEach(visit)
  }
  ;(Array.isArray(seqOrBlock) ? seqOrBlock : [seqOrBlock]).forEach(visit)
  return out
}

/** The sub-sequences directly owned by a block. */
export function children (b) {
  switch (b.kind) {
    case 'xor': case 'and': case 'or': return b.branches
    case 'sub': return [b.body]
    case 'mi': return b.body.kind === 'sub' ? [b.body.body] : []
    default: return []
  }
}

// ───────────────────────────── BPMN + DI rendering ──────────────────────────────

const DIMS = {
  event: [36, 36],
  task: [100, 80],
  gateway: [50, 50]
}
const GAP = 50
const LANE_GAP = 40
const PAD = 30

const xmlEscape = (s) => String(s)
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')

/** Renders a tree to `{ xml }`: semantic model + one BPMN plane per process. */
export function renderBpmn (tree) {
  const sem = [] // [processId, lines[]]
  const planes = [] // [processId, shapes[], edges[]]
  const messages = new Set()
  let flowN = 0
  const flowId = () => `f${++flowN}`

  // Layout primitives: each laid-out fragment is
  // { w, h, inY, outY, place(x, y) -> { inPt, outPt } } relative to its box.
  const mkProcess = (pid) => {
    const lines = []
    const shapes = []
    const edges = []
    const flow = (from, to, extra = '', pts = null) => {
      const f = flowId()
      if (extra) lines.push(`<bpmn:sequenceFlow id="${f}" sourceRef="${from}" targetRef="${to}">${extra}</bpmn:sequenceFlow>`)
      else lines.push(`<bpmn:sequenceFlow id="${f}" sourceRef="${from}" targetRef="${to}" />`)
      return { id: f, setPts: (p) => edges.push({ id: f, pts: p }), pts }
    }
    return { pid, lines, shapes, edges, flow }
  }

  // ── fragment builders (semantic lines are emitted while placing) ──
  // A fragment knows its size up front; `place(P, x, y)` emits XML + DI and
  // returns entry/exit points plus the entry/exit element ids.
  const node = (tag, idv, dims, inner = '', attrs = '') => ({
    w: dims[0],
    h: dims[1],
    place (P, x, y) {
      P.lines.push(inner
        ? `<bpmn:${tag} id="${idv}"${attrs}>${inner}</bpmn:${tag}>`
        : `<bpmn:${tag} id="${idv}"${attrs} />`)
      P.shapes.push({ id: idv, x, y, w: dims[0], h: dims[1] })
      return { first: idv, last: idv, inPt: [x, y + dims[1] / 2], outPt: [x + dims[0], y + dims[1] / 2] }
    }
  })

  const taskXml = (t, typeExpr) => {
    const ext = []
    if (t.user) ext.push('<zeebe:userTask />')
    else ext.push(`<zeebe:taskDefinition type="${xmlEscape(typeExpr)}" />`)
    if (t.inputs || t.outputs) {
      const io = []
      for (const m of t.inputs ?? []) io.push(`<zeebe:input source="${xmlEscape(m.source)}" target="${m.target}" />`)
      for (const m of t.outputs ?? []) io.push(`<zeebe:output source="${xmlEscape(m.source)}" target="${m.target}" />`)
      ext.push(`<zeebe:ioMapping>${io.join('')}</zeebe:ioMapping>`)
    }
    return `<bpmn:extensionElements>${ext.join('')}</bpmn:extensionElements>`
  }

  const jobTypeExpr = (taskId, ctx) => ctx.inMi ? `="${taskId}_" + string(loopCounter)` : taskId

  const miXml = (m) =>
    `<bpmn:multiInstanceLoopCharacteristics${m.sequential ? ' isSequential="true"' : ''}>` +
    '<bpmn:extensionElements>' +
    `<zeebe:loopCharacteristics inputCollection="=cfg.${m.items}" inputElement="item" outputCollection="${m.output}" outputElement="${xmlEscape(m.outputElement)}" />` +
    '</bpmn:extensionElements></bpmn:multiInstanceLoopCharacteristics>'

  const seqFrag = (seq, ctx) => {
    const parts = seq.map((b) => frag(b, ctx))
    const h = Math.max(...parts.map((p) => p.h))
    const w = parts.reduce((s, p) => s + p.w, 0) + GAP * (parts.length - 1)
    return {
      w,
      h,
      place (P, x, y) {
        let cx = x
        let prev = null
        let first = null
        let inPt = null
        let outPt = null
        for (const p of parts) {
          const r = p.place(P, cx, y + (h - p.h) / 2)
          if (prev) {
            const f = P.flow(prev.last, r.first)
            f.setPts([prev.outPt, r.inPt])
          } else {
            first = r.first
            inPt = r.inPt
          }
          prev = r
          outPt = r.outPt
          cx += p.w + GAP
        }
        return { first, last: prev.last, inPt, outPt }
      }
    }
  }

  // Wraps a fragment between a split and a join gateway with branches stacked.
  const gatewayFrag = (splitTag, joinTag, splitId, joinId, branches, ctx, flowExtra, splitAttrs) => {
    const parts = branches.map((s) => seqFrag(s, ctx))
    const [gw, gh] = DIMS.gateway
    const bw = Math.max(...parts.map((p) => p.w))
    const bh = parts.reduce((s, p) => s + p.h, 0) + LANE_GAP * (parts.length - 1)
    const h = Math.max(bh, gh)
    const w = gw + GAP + bw + GAP + gw
    return {
      w,
      h,
      place (P, x, y) {
        const cy = y + h / 2
        const split = node(splitTag, splitId, DIMS.gateway, '', splitAttrs(branches)).place(P, x, cy - gh / 2)
        const joinX = x + w - gw
        // Branch flows are emitted after the branch contents (document order
        // is irrelevant to both engines).
        let by = y + (h - bh) / 2
        const pending = []
        const splitFlows = []
        parts.forEach((p, i) => {
          const r = p.place(P, x + gw + GAP, by)
          pending.push([i, r])
          by += p.h + LANE_GAP
        })
        const join = node(joinTag, joinId, DIMS.gateway).place(P, joinX, cy - gh / 2)
        for (const [i, r] of pending) {
          const sx = x + gw / 2
          const f = P.flow(splitId, r.first, flowExtra(i), null)
          splitFlows[i] = f.id
          f.setPts(r.inPt[1] === cy
            ? [split.outPt, r.inPt]
            : [[sx, r.inPt[1] < cy ? cy - gh / 2 : cy + gh / 2], [sx, r.inPt[1]], r.inPt])
          const g = P.flow(r.last, joinId)
          const jx = joinX + gw / 2
          g.setPts(r.outPt[1] === cy
            ? [r.outPt, join.inPt]
            : [r.outPt, [jx, r.outPt[1]], [jx, r.outPt[1] < cy ? cy - gh / 2 : cy + gh / 2]])
        }
        return { first: splitId, last: joinId, inPt: split.inPt, outPt: join.outPt, splitFlows }
      }
    }
  }

  const cond = (expr) => `<bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">${xmlEscape(expr)}</bpmn:conditionExpression>`

  const subFrag = (s, ctx, loop = '') => {
    const inner = seqFrag(s.body, ctx)
    const [ew, eh] = DIMS.event
    const iw = ew + GAP + inner.w + GAP + ew
    const ih = Math.max(inner.h, eh)
    const w = iw + 2 * PAD
    const h = ih + 2 * PAD
    return {
      w,
      h,
      place (P, x, y) {
        const outer = P.lines
        P.lines = []
        const cy = y + PAD + ih / 2
        const st = node('startEvent', s.start, DIMS.event).place(P, x + PAD, cy - eh / 2)
        const r = inner.place(P, x + PAD + ew + GAP, y + PAD + (ih - inner.h) / 2)
        const en = node('endEvent', s.end, DIMS.event).place(P, x + PAD + ew + GAP + inner.w + GAP, cy - eh / 2)
        P.flow(s.start, r.first).setPts([st.outPt, r.inPt])
        P.flow(r.last, s.end).setPts([r.outPt, en.inPt])
        const body = P.lines.join('')
        P.lines = outer
        P.lines.push(`<bpmn:subProcess id="${s.id}">${loop}${body}</bpmn:subProcess>`)
        P.shapes.push({ id: s.id, x, y, w, h, expanded: true })
        return { first: s.id, last: s.id, inPt: [x, y + h / 2], outPt: [x + w, y + h / 2] }
      }
    }
  }

  const callChildren = []

  const frag = (b, ctx) => {
    switch (b.kind) {
      case 'task': {
        const tag = b.user ? 'userTask' : 'serviceTask'
        const t = node(tag, b.id, DIMS.task, taskXml(b, jobTypeExpr(b.id, ctx)))
        if (!b.boundary) return t
        // task + attached interrupting boundary event, both paths merged by an
        // exclusive gateway so the block stays single-entry/single-exit.
        const [ew, eh] = DIMS.event
        const [gw, gh] = DIMS.gateway
        const w = DIMS.task[0] + GAP + gw
        const h = DIMS.task[1] + eh / 2 + LANE_GAP + gh / 2
        return {
          w,
          h,
          place (P, x, y) {
            const r = t.place(P, x, y)
            const bd = b.boundary
            const def = bd.kind === 'timer'
              ? `<bpmn:timerEventDefinition><bpmn:timeDuration>PT${bd.ticks}M</bpmn:timeDuration></bpmn:timerEventDefinition>`
              : `<bpmn:messageEventDefinition messageRef="Message_${bd.message}" />`
            if (bd.kind === 'message') messages.add(bd.message)
            P.lines.push(`<bpmn:boundaryEvent id="${bd.id}" attachedToRef="${b.id}">${def}</bpmn:boundaryEvent>`)
            const bx = x + DIMS.task[0] / 2 - ew / 2
            const by = y + DIMS.task[1] - eh / 2
            P.shapes.push({ id: bd.id, x: bx, y: by, w: ew, h: eh })
            const gx = x + DIMS.task[0] + GAP
            const gy = y + DIMS.task[1] / 2 - gh / 2
            const m = node('exclusiveGateway', bd.merge, DIMS.gateway).place(P, gx, gy)
            P.flow(b.id, bd.merge).setPts([r.outPt, m.inPt])
            P.flow(bd.id, bd.merge).setPts([[bx + ew / 2, by + eh], [bx + ew / 2, by + eh + LANE_GAP], [gx + gw / 2, by + eh + LANE_GAP], [gx + gw / 2, gy + gh]])
            return { first: b.id, last: bd.merge, inPt: r.inPt, outPt: m.outPt }
          }
        }
      }
      case 'timer':
        return node('intermediateCatchEvent', b.id, DIMS.event,
          `<bpmn:timerEventDefinition><bpmn:timeDuration>PT${b.ticks}M</bpmn:timeDuration></bpmn:timerEventDefinition>`)
      case 'message':
        messages.add(b.message)
        return node('intermediateCatchEvent', b.id, DIMS.event,
          `<bpmn:messageEventDefinition messageRef="Message_${b.message}" />`)
      case 'xor': {
        const frg = gatewayFrag('exclusiveGateway', 'exclusiveGateway', b.id, b.join, b.branches, ctx,
          (i) => i === b.defaultIdx ? '' : cond(`=cfg.${b.var} = ${i}`),
          () => '')
        // The default flow is the default branch's split flow: patch the split
        // gateway's attribute once its flow ids are known.
        return {
          w: frg.w,
          h: frg.h,
          place (P, x, y) {
            const r = frg.place(P, x, y)
            const line = `<bpmn:exclusiveGateway id="${b.id}" />`
            const idx = P.lines.indexOf(line)
            const def = r.splitFlows[b.defaultIdx]
            if (idx < 0 || !def) throw new Error(`xor ${b.id}: cannot set the default flow`)
            P.lines[idx] = `<bpmn:exclusiveGateway id="${b.id}" default="${def}" />`
            return r
          }
        }
      }
      case 'and':
        return gatewayFrag('parallelGateway', 'parallelGateway', b.id, b.join, b.branches, ctx, () => '', () => '')
      case 'or':
        return gatewayFrag('inclusiveGateway', 'inclusiveGateway', b.id, b.join, b.branches, ctx,
          (i) => cond(`=cfg.${b.flags[i]}`), () => '')
      case 'sub':
        return subFrag(b, ctx)
      case 'mi': {
        const miCtx = { ...ctx, inMi: true }
        if (b.body.kind === 'sub') return subFrag(b.body, miCtx, miXml(b))
        const t = b.body
        return node('serviceTask', t.id, DIMS.task, taskXml(t, jobTypeExpr(t.id, miCtx)) + miXml(b))
      }
      case 'call': {
        callChildren.push(b.child)
        return node('callActivity', b.id, DIMS.task,
          // Variables cross the call boundary through ONE input and ONE
          // output mapping, never whole-document propagation: Zeebe copies a
          // multi-variable document in internal hash order (nano: name order)
          // — the same ordering noise the one-variable payloads avoid.
          `<bpmn:extensionElements><zeebe:calledElement processId="${b.child.process}" propagateAllParentVariables="false" propagateAllChildVariables="false" /><zeebe:ioMapping><zeebe:input source="=cfg" target="${CONFIG_VAR}" /><zeebe:output source="=result" target="o${b.id.replace(/^\D+/, '')}" /></zeebe:ioMapping></bpmn:extensionElements>`)
      }
    }
    throw new Error(`unknown block ${b.kind}`)
  }

  const processWith = (pid, startId, endId, seq, ctx) => {
    const P = mkProcess(pid)
    const inner = seqFrag(seq, ctx)
    const [ew, eh] = DIMS.event
    const x0 = 160
    const y0 = 100
    const cy = y0 + inner.h / 2
    const st = node('startEvent', startId, DIMS.event).place(P, x0, cy - eh / 2)
    const r = inner.place(P, x0 + ew + GAP, y0)
    const en = node('endEvent', endId, DIMS.event).place(P, x0 + ew + GAP + inner.w + GAP, cy - eh / 2)
    P.flow(startId, r.first).setPts([st.outPt, r.inPt])
    P.flow(r.last, endId).setPts([r.outPt, en.inPt])
    sem.push([pid, P.lines])
    planes.push([pid, P.shapes, P.edges])
  }

  processWith(PROCESS_ID, tree.start, tree.end, tree.body, { inMi: false })
  for (const c of callChildren) {
    processWith(c.process, c.start, c.end, [{ kind: 'task', id: c.task }], { inMi: false })
  }

  const round = (v) => Math.round(v)
  const out = []
  out.push('<?xml version="1.0" encoding="UTF-8"?>')
  out.push('<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"' +
    ' xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"' +
    ' xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"' +
    ' xmlns:di="http://www.omg.org/spec/DD/20100524/DI"' +
    ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' +
    ' xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"' +
    ` id="Definitions_fuzz_${tree.seed}" targetNamespace="http://bpmn.io/schema/bpmn">`)
  for (const m of [...messages].sort()) {
    out.push(`  <bpmn:message id="Message_${m}" name="${m}"><bpmn:extensionElements><zeebe:subscription correlationKey="=cfg.ck" /></bpmn:extensionElements></bpmn:message>`)
  }
  for (const [pid, lines] of sem) {
    out.push(`  <bpmn:process id="${pid}" isExecutable="true">`)
    for (const l of lines) out.push(`    ${l}`)
    out.push('  </bpmn:process>')
  }
  planes.forEach(([pid, shapes, edges], i) => {
    out.push(`  <bpmndi:BPMNDiagram id="BPMNDiagram_${i + 1}">`)
    out.push(`    <bpmndi:BPMNPlane id="BPMNPlane_${i + 1}" bpmnElement="${pid}">`)
    for (const s of shapes) {
      out.push(`      <bpmndi:BPMNShape id="${s.id}_di" bpmnElement="${s.id}"${s.expanded ? ' isExpanded="true"' : ''}>` +
        `<dc:Bounds x="${round(s.x)}" y="${round(s.y)}" width="${s.w}" height="${s.h}" /></bpmndi:BPMNShape>`)
    }
    for (const e of edges) {
      out.push(`      <bpmndi:BPMNEdge id="${e.id}_di" bpmnElement="${e.id}">` +
        e.pts.map(([x, y]) => `<di:waypoint x="${round(x)}" y="${round(y)}" />`).join('') +
        '</bpmndi:BPMNEdge>')
    }
    out.push('    </bpmndi:BPMNPlane>')
    out.push('  </bpmndi:BPMNDiagram>')
  })
  out.push('</bpmn:definitions>')
  const xml = out.join('\n') + '\n'
  for (const m of xml.matchAll(/ id="([^"]+)"/g)) {
    if (!SAFE_ID.test(m[1])) throw new Error(`generated id ${m[1]} is outside the corpus id grammar`)
  }
  return xml
}

// ───────────────────────────── driver simulation ──────────────────────────────
//
// A token-flow simulator over the block tree. Every exec node exposes
// `waits()` — the commands that are valid right now �� and is driven by
// `fire(wait)`. Timers are global: one `advanceClock` of TICK_MS fires every
// due timer at once, exactly as both engines do.

function simulate (tree, r, maxSteps) {
  const clock = { now: 0 }
  const vars = tree.initVars

  // Each exec: { done, waits(): [{op, key, ...}], fire(w) }
  const start = (b, ctx) => {
    switch (b.kind) {
      case 'task': return taskExec(b, ctx)
      case 'timer': return timerExec(b.ticks)
      case 'message': return { done: false, waits () { return this.done ? [] : [{ op: 'publishMessage', name: b.message, ex: this }] }, fire () { this.done = true } }
      case 'xor': {
        const v = vars[b.var]
        const i = v >= 0 && v < b.defaultIdx ? v : b.defaultIdx
        return seqExec(b.branches[i], ctx)
      }
      case 'and': return parExec(b.branches.map((s) => seqExec(s, ctx)))
      case 'or': return parExec(b.branches.filter((_, i) => vars[b.flags[i]]).map((s) => seqExec(s, ctx)))
      case 'sub': return seqExec(b.body, ctx)
      case 'mi': {
        const items = vars[b.items]
        const mk = (i) => b.body.kind === 'sub'
          ? seqExec(b.body.body, { ...ctx, loop: i + 1 })
          : taskExec(b.body, { ...ctx, loop: i + 1 })
        if (!b.sequential) return parExec(items.map((_, i) => mk(i)))
        return seqOfExec(items.map((_, i) => () => mk(i)))
      }
      case 'call': return taskExec({ kind: 'task', id: b.child.task }, ctx)
    }
    throw new Error(`unknown block ${b.kind}`)
  }
  const jobType = (t, ctx) => ctx.loop ? `${t.id}_${ctx.loop}` : t.id
  const taskExec = (t, ctx) => {
    const ex = { done: false }
    const due = t.boundary?.kind === 'timer' ? clock.now + t.boundary.ticks * TICK_MS : null
    ex.waits = () => {
      if (ex.done) return []
      const w = [t.user
        ? { op: 'completeUserTask', elementId: t.id, ex }
        : { op: 'completeJob', jobType: jobType(t, ctx), ex }]
      if (due !== null) w.push({ op: 'timer', due, ex })
      if (t.boundary?.kind === 'message') w.push({ op: 'publishMessage', name: t.boundary.message, ex })
      return w
    }
    ex.fire = () => { ex.done = true }
    return ex
  }
  const timerExec = (ticks) => {
    const ex = { done: false }
    const due = clock.now + ticks * TICK_MS
    ex.waits = () => ex.done ? [] : [{ op: 'timer', due, ex }]
    ex.fire = () => { ex.done = true }
    return ex
  }
  // Thunk-sequenced execs: each starts only when the previous one is done
  // (so its timers are armed at the right clock).
  const seqOfExec = (thunks) => {
    let i = 0
    let cur = null
    const ex = { done: false }
    const settle = () => {
      while (!ex.done && (cur === null || cur.done)) {
        if (i >= thunks.length) { ex.done = true; return }
        cur = thunks[i++]()
      }
    }
    settle()
    ex.waits = () => ex.done ? [] : cur.waits()
    ex.settle = () => { cur.settle?.(); settle() }
    return ex
  }
  const seqExec = (seq, ctx) => seqOfExec(seq.map((b) => () => start(b, ctx)))
  const parExec = (execs) => {
    const ex = { done: execs.every((e) => e.done) }
    ex.waits = () => ex.done ? [] : execs.flatMap((e) => e.waits())
    ex.settle = () => {
      execs.forEach((e) => e.settle?.())
      ex.done = execs.every((e) => e.done)
    }
    return ex
  }

  const value = () => r.weighted([[r.int(0, 99), 3], [`s${r.int(0, 9)}`, 1], [null, 1]])
  // ONE variable per command document. Zeebe creates a document's variables
  // in its internal hash order (not document order), nano in name order — an
  // order the trace normaliser compares but neither engine documents, so a
  // multi-variable document would diverge on noise. Collisions come from
  // successive commands writing the same few names instead.
  const payload = () => ({ [r.weighted([[COLLIDING[0], 3], [COLLIDING[1], 1], [COLLIDING[2], 1]])]: value() })

  // Every routing/collection input rides in the ONE `cfg` variable (see payload).
  const steps = [{ op: 'createInstance', bpmnProcessId: PROCESS_ID, variables: { [CONFIG_VAR]: { ...vars } } }]
  const root = seqExec(tree.body, { loop: 0 })
  while (steps.length < maxSteps) {
    root.settle?.()
    if (root.done) break
    const ws = root.waits()
    if (ws.length === 0) break
    const timers = ws.filter((w) => w.op === 'timer')
    const others = ws.filter((w) => w.op !== 'timer')
    const choices = [...others]
    if (timers.length) choices.push({ op: 'advanceClock' })
    if (r.chance(0.1)) choices.push({ op: 'updateVariables' })
    const w = r.pick(choices)
    switch (w.op) {
      case 'completeJob':
        steps.push({ op: 'completeJob', jobType: w.jobType, variables: payload() })
        w.ex.fire(w)
        break
      case 'completeUserTask':
        steps.push({ op: 'completeUserTask', elementId: w.elementId, variables: payload() })
        w.ex.fire(w)
        break
      case 'publishMessage': {
        // A message name is unique per catch / boundary, so it wakes exactly one wait.
        steps.push({ op: 'publishMessage', name: w.name, correlationKey: vars.ck, variables: payload() })
        w.ex.fire(w)
        break
      }
      case 'advanceClock': {
        clock.now += TICK_MS
        steps.push({ op: 'advanceClock', millis: TICK_MS })
        for (const t of timers) if (t.due <= clock.now) t.ex.fire(t)
        break
      }
      case 'updateVariables': {
        steps.push({ op: 'updateVariables', variables: payload() })
        break
      }
    }
  }
  return steps
}

// ───────────────────────────── the case ──────────────────────────────

export function describe (tree) {
  const kinds = blocks(tree.body).map((b) => b.kind === 'mi' ? (b.sequential ? 'mi-seq' : 'mi-par') : b.kind)
  return `Generated by formal/traces/fuzz/generate.mjs (#1521), seed ${tree.seed}: ${kinds.join(', ')}.`
}

/** Render a (possibly shrunk) tree into a scenario. The driver PRNG is seeded
 *  from the tree seed, so the same tree always yields the same script. */
export function renderCase (tree, opts = {}) {
  const o = { ...DEFAULTS, ...tree.opts, ...opts }
  const xml = renderBpmn(tree)
  const steps = simulate(structuredClone(tree), prng(tree.seed ^ 0x5bd1e995), o.maxSteps)
  const script = { description: describe(tree), generator: { seed: tree.seed, opts: tree.opts }, steps }
  return { xml, script }
}

export function generateCase (seed, opts = {}) {
  return renderCase(generateTree(seed, opts))
}

export function scenarioName (seed) {
  return `${GENERATED_PREFIX}${seed}`
}

export function writeCase (scenariosDir, name, { xml, script }) {
  const dir = path.join(scenariosDir, name)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(path.join(dir, 'process.bpmn'), xml)
  fs.writeFileSync(path.join(dir, 'script.json'), JSON.stringify(script, null, 2) + '\n')
}

export function loadBudget () {
  return JSON.parse(fs.readFileSync(BUDGET_FILE, 'utf8'))
}

/** The committed generated scenarios that disagree with the generator. */
export function budgetDrift (scenariosDir = path.join(TRACES_DIR, 'scenarios')) {
  const { seeds } = loadBudget()
  const problems = []
  const want = new Set(seeds.map(scenarioName))
  for (const d of fs.readdirSync(scenariosDir)) {
    if (d.startsWith(GENERATED_PREFIX) && !want.has(d)) problems.push(`${d}: not in budget.json (orphan)`)
  }
  for (const seed of seeds) {
    const name = scenarioName(seed)
    const dir = path.join(scenariosDir, name)
    const { xml, script } = generateCase(seed)
    const read = (f) => fs.existsSync(path.join(dir, f)) ? fs.readFileSync(path.join(dir, f), 'utf8') : null
    if (read('process.bpmn') !== xml) problems.push(`${name}/process.bpmn drifts from the generator`)
    if (read('script.json') !== JSON.stringify(script, null, 2) + '\n') problems.push(`${name}/script.json drifts from the generator`)
  }
  return problems
}

function main (argv) {
  let out = path.join(TRACES_DIR, 'scenarios')
  const seeds = []
  let check = false
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a === '--out') out = argv[++i]
    else if (a === '--check') check = true
    else if (a === '--budget') seeds.push(...loadBudget().seeds)
    else if (/^\d+$/.test(a)) seeds.push(Number(a))
    else throw new Error(`unknown argument ${a}`)
  }
  if (check) {
    const problems = budgetDrift()
    if (problems.length) {
      console.error(problems.join('\n') + '\nregenerate: node formal/traces/fuzz/generate.mjs --budget, then capture.sh the changed g-* scenarios')
      process.exit(1)
    }
    console.log('generated scenarios match the generator')
    return
  }
  for (const s of seeds) {
    writeCase(out, scenarioName(s), generateCase(s))
    console.log(`wrote ${scenarioName(s)}`)
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main(process.argv.slice(2))
}
