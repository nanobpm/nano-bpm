#!/usr/bin/env node
// Zeebe exporter-record trace validation against ZeebeTokenFlow.tla (#1962).
//
// Link A of the parity-proof chain:
//
//   Zeebe Java --(A: this runner)--> ZeebeTokenFlow.tla <--(B: RefinesZeebe)--> TokenFlow.tla
//
// `ZeebeTokenFlow.tla` is hand-transcribed from `zeebe/engine`. This runner
// checks the transcription against the real engine by EXHAUSTIVE BOUNDED
// CONFORMANCE:
//
//   1. every graph of the hand-written corpus (formal/corpus/graphs) and of the
//      bounded-exhaustive enumeration (formal/corpus/enumerate.mjs, a named
//      bound of bounds.json) is turned into its BPMN and its scenarios — one
//      per completion order and routing (`artifactsFor`, the SAME source the
//      runtime parity runner uses);
//   2. each scenario runs on the pinned Zeebe engine (the in-JVM `EngineRule`
//      capture harness, formal/traces/capture.sh, pinned by
//      formal/parity/zeebe-pin.json) and its exporter records are normalised by
//      the shared normaliser (formal/traces/normaliser.json, #1259);
//   3. SOUNDNESS — the normalised stream is projected onto the spec vocabulary
//      (`taken`/`active`/`fired`/`done` <-> SEQUENCE_FLOW_TAKEN /
//      ELEMENT_ACTIVATED / ELEMENT_COMPLETED) and TLC checks it is a behaviour
//      of ZeebeTokenFlow, with the trace as a constraint
//      (formal/tla/conformance/ZeebeTokenFlowTrace.tla). A rejected trace means
//      the reference model is wrong;
//   4. COVERAGE — the converse on the bounded corpus: every join decision
//      (fire / defer) the UNCONSTRAINED model can take on a graph must be
//      observed in Zeebe for some completion order of that graph.
//
//   node formal/traces/conformance/zeebe-conformance.mjs [--bound ci|nightly]
//        [--gateways N] [--flows M] [--kinds xor,and,or,evt] [--no-corpus]
//        [--graph ID]... [--work DIR] [--captured DIR] [--jobs N] [--file]
//
// `--captured DIR` reuses a traces root an earlier run captured (skips the
// JVM capture). `--file` files each NEW divergence class as an issue (an open
// issue carrying the signature gets a comment instead), like the parity fuzz.
// Any divergence fails the run (with --file: any NEW one). No retries.
import { spawnSync } from 'node:child_process'
import crypto from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { enumerateGraphs, exploreOrders, scenariosFor } from '../../corpus/enumerate.mjs'
import { bpmnFor, loadGraphs, tlaFor } from '../../corpus/generate.mjs'
import { readSpec, resolveBound, runTlc, pool, tlaJar } from '../../corpus/exhaustive.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const TRACES_DIR = path.join(here, '..')
const TRACE_MODULE = path.join(here, '..', '..', 'tla', 'conformance', 'ZeebeTokenFlowTrace.tla')

/** The element kinds ZeebeTokenFlow models (its `ElementKinds`). */
export const SPEC_KINDS = new Set(['start', 'end', 'task', 'xor', 'and', 'or', 'evt', 'msg'])

// ---------------------------------------------------------------------------
// Arguments

export function parseArgs (argv) {
  const a = { bound: null, gateways: null, flows: null, kinds: null, corpus: true, graphs: [], work: null, captured: null, jobs: os.availableParallelism(), file: false }
  for (let i = 0; i < argv.length; i++) {
    const x = argv[i]
    const val = () => { if (i + 1 >= argv.length) throw new Error(`${x} needs a value`); return argv[++i] }
    if (x === '--bound') a.bound = val()
    else if (x === '--gateways') a.gateways = Number(val())
    else if (x === '--flows') a.flows = Number(val())
    else if (x === '--kinds') a.kinds = val().split(',')
    else if (x === '--no-corpus') a.corpus = false
    else if (x === '--graph') a.graphs.push(val())
    else if (x === '--work') a.work = val()
    else if (x === '--captured') a.captured = val()
    else if (x === '--jobs') {
      const v = val()
      // A zero/NaN worker count would run no validation and "pass" (#1964).
      if (!/^[1-9][0-9]*$/.test(v)) throw new Error(`--jobs must be a positive integer, got ${JSON.stringify(v)}`)
      a.jobs = Number(v)
    }
    else if (x === '--file') a.file = true
    else throw new Error(`unknown argument: ${x}`)
  }
  return a
}

// ---------------------------------------------------------------------------
// Graphs and scenarios

/** A graph is in scope iff every node kind is one ZeebeTokenFlow models. */
export const inSpecVocabulary = (graph) => Object.values(graph.nodes).every((k) => SPEC_KINDS.has(k))

/** The graphs to validate: the hand-written corpus plus the bounded enumeration. */
export function selectGraphs (args) {
  const out = []
  if (args.corpus) out.push(...loadGraphs())
  if (args.bound || args.gateways != null || args.flows != null) {
    out.push(...enumerateGraphs(resolveBound(args)).map((x) => x.graph))
  }
  const picked = args.graphs.length ? out.filter((g) => args.graphs.includes(g.id)) : out
  return picked.filter(inSpecVocabulary)
}

/**
 * One corpus scenario (`scenariosFor`'s driver vocabulary) as a capture-harness
 * script (formal/traces/README.md). Returns null for a scenario outside the
 * token-flow vocabulary (a timer tick or a cancellation): ZeebeTokenFlow models
 * neither, so its trace has nothing to be validated against.
 */
export function captureScript (graph, scenario) {
  const steps = [{ op: 'createInstance', bpmnProcessId: graph.id, ...(Object.keys(scenario.variables).length ? { variables: scenario.variables } : {}) }]
  for (const s of scenario.steps) {
    const vars = s.variables && Object.keys(s.variables).length ? { variables: s.variables } : {}
    if (s.op === 'completeOneJob') steps.push({ op: 'completeJob', jobType: s.jobType, ...vars })
    else if (s.op === 'setVariables') steps.push({ op: 'updateVariables', variables: s.variables })
    else if (s.op === 'publishMessage') steps.push({ op: 'publishMessage', name: s.name, correlationKey: s.correlationKey, ...vars })
    else return null
  }
  return { description: `Generated by formal/traces/conformance/zeebe-conformance.mjs (#1962) from graph ${graph.key ?? graph.id}.`, steps }
}

/** Whether the graph has a cycle (the hand-written ExclusiveLoop does). */
export function isCyclic (graph) {
  const state = Object.create(null)
  const visit = (n) => {
    if (state[n] === 1) return true
    if (state[n] === 2) return false
    state[n] = 1
    const cyc = graph.edges.some((e) => e.from === n && visit(e.to))
    state[n] = 2
    return cyc
  }
  return Object.keys(graph.nodes).some(visit)
}

/** How many commands a cyclic graph's runs are unrolled to. */
export const CYCLIC_MAX_STEPS = 4

/**
 * Every validatable case of a graph: `{ name, graph, xml, script }` — one per
 * completion order and routing, the same scenario source (`scenariosFor`) and
 * variable-routed BPMN (`bpmnFor`) as the runtime parity runner. A cyclic
 * graph's runs are unrolled to `CYCLIC_MAX_STEPS` commands.
 */
export function casesFor (graph) {
  const runs = exploreOrders(graph, {}, isCyclic(graph) ? { maxSteps: CYCLIC_MAX_STEPS } : {})
  const { scenarios } = scenariosFor(graph, runs)
  const bpmn = bpmnFor(graph, { routing: 'variables' })
  const out = []
  for (const sc of scenarios) {
    const script = captureScript(graph, sc)
    if (script) out.push({ name: sc.name, graph, xml: bpmn, script })
  }
  return out
}

// ---------------------------------------------------------------------------
// Projection: normalised Zeebe records -> spec milestones

const inFlows = (graph, n) => graph.edges.filter((e) => e.to === n)
/** A join in ZeebeTokenFlow's sense (`IsJoin`): an and/or with >1 incoming flow. */
export const isJoin = (graph, n) => ['and', 'or'].includes(graph.nodes[n]) && inFlows(graph, n).length > 1

/**
 * Project one normalised step's records onto the spec's milestone vocabulary
 * (the label set of ZeebeTokenFlowTrace.tla), in record order.
 */
export function stepMilestones (graph, records) {
  const out = []
  for (const r of records) {
    if (r.valueType !== 'PROCESS_INSTANCE') continue
    if (r.intent === 'SEQUENCE_FLOW_TAKEN') out.push(`flow:${r.elementId}`)
    else if (r.intent === 'ELEMENT_COMPLETED' && r.bpmnElementType === 'PROCESS') out.push('done')
    else if (r.intent === 'ELEMENT_ACTIVATED' && r.bpmnElementType !== 'PROCESS') {
      const kind = graph.nodes[r.elementId]
      if (kind === undefined) throw new Error(`record for unknown element ${r.elementId} in graph ${graph.id}`)
      if (kind === 'task' || kind === 'evt') out.push(`act:${r.elementId}`)
      else if (isJoin(graph, r.elementId)) out.push(`fire:${r.elementId}`)
    }
  }
  return out
}

/** The whole captured trace as `{ trace, steps }` (steps: per-step milestones). */
export function traceMilestones (graph, golden) {
  const steps = golden.steps.map((s) => stepMilestones(graph, s.records))
  return { trace: steps.flat(), steps }
}

/**
 * The join decisions a captured trace OBSERVES. Every flow taken into a join
 * is one activation attempt of it (Zeebe writes an ACTIVATE_ELEMENT command
 * per taken flow, the model queues one), and every attempt is decided within
 * the step that took the flow: it either activates the join (`fire`, an
 * ELEMENT_ACTIVATED) or is rejected and waits (`defer`). So per step and join,
 * `fires` attempts fired and `arrivals - fires` deferred — exact, not a guess.
 */
export function observedDecisions (graph, steps) {
  const out = new Set()
  const tgt = Object.fromEntries(graph.edges.map((e) => [e.id, e.to]))
  for (const ms of steps) {
    const arrivals = {}
    const fires = {}
    for (const m of ms) {
      if (m.startsWith('fire:')) fires[m.slice(5)] = (fires[m.slice(5)] ?? 0) + 1
      if (m.startsWith('flow:') && isJoin(graph, tgt[m.slice(5)])) arrivals[tgt[m.slice(5)]] = (arrivals[tgt[m.slice(5)]] ?? 0) + 1
    }
    for (const [j, n] of Object.entries(fires)) if (n > 0) out.add(`fire:${j}`)
    for (const [j, n] of Object.entries(arrivals)) if (n > (fires[j] ?? 0)) out.add(`defer:${j}`)
  }
  return out
}

// ---------------------------------------------------------------------------
// TLC

const tlaString = (s) => `"${s.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`

/** The graph's MC constant definitions, taken from the corpus generator. */
export function graphDefs (graph) {
  const fam = { module: 'X', comment: [] }
  const tla = tlaFor({ ...graph, families: { ZeebeTokenFlow: fam } }, 'ZeebeTokenFlow')
  const lines = tla.split('\n')
  const from = lines.findIndex((l) => l.startsWith('MCNodes'))
  const to = lines.findIndex((l) => /^=+$/.test(l))
  if (from < 0 || to < from) throw new Error(`cannot find the MC definitions in tlaFor(${graph.id})`)
  return lines.slice(from, to).join('\n')
}

export function traceModule (module, graph, trace) {
  return [
    `---- MODULE ${module} ----`,
    '\\* GENERATED by formal/traces/conformance/zeebe-conformance.mjs (#1962).',
    'EXTENDS ZeebeTokenFlowTrace',
    graphDefs(graph),
    `MCTrace == <<${trace.map(tlaString).join(', ')}>>`,
    '===='
  ].join('\n') + '\n'
}

export function traceCfg (spec, { matchLimit, unconstrained = false, printDecisions = false }) {
  return [
    'INIT TraceInit',
    'NEXT TraceNext',
    'CONSTANTS', ...spec.constants.map((c) => `    ${c}`),
    '    Trace <- MCTrace',
    `    MatchLimit = ${matchLimit}`,
    `    Unconstrained = ${unconstrained ? 'TRUE' : 'FALSE'}`,
    `    PrintDecisions = ${printDecisions ? 'TRUE' : 'FALSE'}`,
    ...(unconstrained ? [] : ['INVARIANT TraceUnmatched'])
  ].join('\n') + '\n'
}

/** 'accepted' | 'rejected' | 'error' from a constrained TLC run. */
export function acceptanceFromLog (log) {
  if (/^Warning/m.test(log)) return 'error'
  // TLC reports an invariant violation as two `Error:` lines (the violation and
  // "The behavior up to this point is:"); any OTHER `Error:` is a TLC failure.
  const errors = [...log.matchAll(/^Error: (.*)$/gm)].map((m) => m[1])
  if (errors.some((e) => !/^(Invariant TraceUnmatched is violated\.|The behavior up to this point is:)$/.test(e))) return 'error'
  if (/Invariant TraceUnmatched is violated/.test(log)) return 'accepted'
  if (/Model checking completed\. No error has been found\./.test(log)) return 'rejected'
  return 'error'
}

/** The decisions an unconstrained TLC run printed. */
export function decisionsFromLog (log) {
  return new Set([...log.matchAll(/<<"DECISION", "((?:fire|defer):[^"]+)">>/g)].map((m) => m[1]))
}

const moduleName = (s) => `ZTV_${s.replace(/[^A-Za-z0-9_]/g, '_')}`

async function tlcAccepts (ctx, name, graph, trace, matchLimit) {
  const module = moduleName(name)
  const r = await runTlc(ctx.jar, ctx.work, module, traceModule(module, graph, trace),
    traceCfg(ctx.spec, { matchLimit }), false, [TRACE_MODULE])
  const v = acceptanceFromLog(r.log)
  if (v === 'error') {
    const log = path.join(ctx.work, `${module}.error.log`)
    fs.writeFileSync(log, r.log)
    throw new Error(`TLC error validating ${name} — see ${log}`)
  }
  return v === 'accepted'
}

/** Validate one trace; on rejection, binary-search the longest accepted prefix. */
export async function validateTrace (ctx, name, graph, trace) {
  if (await tlcAccepts(ctx, name, graph, trace, trace.length)) return { ok: true }
  let lo = 0 // accepted (the empty prefix always is)
  let hi = trace.length // rejected
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1
    if (await tlcAccepts(ctx, name, graph, trace, mid)) lo = mid
    else hi = mid
  }
  // The full trace additionally needs the model AT REST where Zeebe stopped.
  const unmatched = hi === trace.length ? `${trace[lo] ?? '(nothing)'} (or the model cannot come to rest after it, as Zeebe did)` : trace[lo]
  return { ok: false, matched: lo, unmatched }
}

export async function modelDecisions (ctx, graph) {
  const module = moduleName(`cov_${graph.id}`)
  const r = await runTlc(ctx.jar, ctx.work, module, traceModule(module, graph, []),
    traceCfg(ctx.spec, { matchLimit: 0, unconstrained: true, printDecisions: true }), false, [TRACE_MODULE])
  if (!/Model checking completed\. No error has been found\./.test(r.log) || /^Warning/m.test(r.log)) {
    throw new Error(`TLC error enumerating decisions of ${graph.id}:\n${r.log.slice(-3000)}`)
  }
  return decisionsFromLog(r.log)
}

// ---------------------------------------------------------------------------
// Capture

function run (cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { encoding: 'utf8', maxBuffer: 1 << 28, ...opts })
  return { code: r.status ?? 1, out: (r.stdout ?? '') + (r.stderr ?? '') }
}

export function stage (root, cases) {
  fs.rmSync(root, { recursive: true, force: true })
  fs.mkdirSync(path.join(root, 'scenarios'), { recursive: true })
  fs.copyFileSync(path.join(TRACES_DIR, 'normaliser.json'), path.join(root, 'normaliser.json'))
  for (const c of cases) {
    const dir = path.join(root, 'scenarios', c.name)
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(dir, 'process.bpmn'), c.xml)
    fs.writeFileSync(path.join(dir, 'script.json'), JSON.stringify(c.script, null, 2) + '\n')
  }
}

function capture (root, log) {
  const r = run(path.join(TRACES_DIR, 'capture.sh'), [], { env: { ...process.env, NANO_TRACES_CAPTURE_DIR: root } })
  fs.writeFileSync(log, r.out)
  if (r.code !== 0) throw new Error(`Zeebe capture failed — see ${log}`)
}

// ---------------------------------------------------------------------------
// Divergences

export const signatureOf = (d) => crypto.createHash('sha256')
  .update(d.kind === 'trace' ? `trace|${d.graphKey}|${d.unmatched}` : `coverage|${d.graphKey}|${d.decision}`)
  .digest('hex').slice(0, 12)

function fileIssue (d, sig) {
  const marker = `zeebe-conformance-signature:${sig}`
  const found = run('gh', ['issue', 'list', '--state', 'open', '--search', marker, '--json', 'number', '--jq', '.[0].number'])
  const what = d.kind === 'trace'
    ? `Zeebe exported a trace that is **not a behaviour of \`ZeebeTokenFlow.tla\`**: the reference model is wrong (fix the model; \`RefinesZeebe\` then surfaces the matching Nano divergence).\n\nScenario \`${d.name}\`; the model matches ${d.matched} milestone(s), then cannot produce \`${d.unmatched}\`.\n\nTrace:\n\`\`\`\n${d.trace.join('\n')}\n\`\`\``
    : `The model allows join decision \`${d.decision}\` on graph \`${d.graphId}\` that Zeebe never takes for any completion order of the bounded corpus (coverage gap: the model is more permissive than Zeebe, or the scenario generator misses an order).`
  const body = [
    `The Zeebe conformance run (https://github.com/nanobpm/nano-bpm/issues/1962) found a divergence on graph \`${d.graphKey}\`.`,
    '',
    what,
    '',
    `Reproduce: \`node formal/traces/conformance/zeebe-conformance.mjs --graph ${d.graphId}${d.bound ? ` --bound ${d.bound}` : ''}\``,
    '',
    `<!-- ${marker} -->`
  ].join('\n')
  if (found.code === 0 && found.out.trim()) {
    run('gh', ['issue', 'comment', found.out.trim(), '--body', body])
    return { known: true, ref: `#${found.out.trim()} (commented)` }
  }
  const title = d.kind === 'trace'
    ? `parity(zeebe-conformance): Zeebe trace rejected by ZeebeTokenFlow (${d.graphId}, signature ${sig})`
    : `parity(zeebe-conformance): model join decision never observed in Zeebe (${d.graphId} ${d.decision}, signature ${sig})`
  const r = run('gh', ['issue', 'create', '--title', title, '--body', body])
  if (r.code !== 0) throw new Error(`gh issue create failed: ${r.out}`)
  return { known: false, ref: r.out.trim() }
}

/** The Zeebe commit pinned in formal/parity/zeebe-pin.json (the one pin). */
export const pinnedSha = () =>
  JSON.parse(fs.readFileSync(path.join(here, '..', '..', 'parity', 'zeebe-pin.json'), 'utf8')).sha

/**
 * Reject a `--captured` trace dir not captured on `pin`: its PIN file and every
 * case's golden `zeebePin` must equal it, else a stale capture would be
 * "validated" in place of the pinned engine's behaviour (#1964).
 */
export function checkCapturedPins (root, names, pin) {
  const pinFile = path.join(root, 'goldens', 'PIN')
  const filePin = fs.existsSync(pinFile) ? fs.readFileSync(pinFile, 'utf8').trim() : null
  if (filePin !== pin) throw new Error(`captured traces in ${root} are for Zeebe pin ${filePin ?? '(missing PIN)'}, expected ${pin}`)
  for (const name of names) {
    const golden = JSON.parse(fs.readFileSync(path.join(root, 'goldens', `${name}.trace.json`), 'utf8'))
    if (golden.zeebePin !== pin) throw new Error(`captured trace ${name} is for Zeebe pin ${golden.zeebePin ?? '(missing)'}, expected ${pin}`)
  }
}

// ---------------------------------------------------------------------------
// Main

async function main (argv) {
  const args = parseArgs(argv)
  const graphs = selectGraphs(args)
  if (graphs.length === 0) throw new Error('no graphs selected')
  // Absolute: runTlc runs TLC with cwd inside it, so a relative path would re-resolve (#1964).
  const work = args.work ? path.resolve(args.work) : fs.mkdtempSync(path.join(os.tmpdir(), 'nano-zeebe-conformance-'))
  fs.mkdirSync(work, { recursive: true })
  const cases = graphs.flatMap(casesFor)
  console.log(`conformance: ${graphs.length} graph(s), ${cases.length} scenario(s), work dir ${work}`)

  const root = args.captured ?? path.join(work, 'traces')
  if (args.captured) checkCapturedPins(root, cases.map((c) => c.name), pinnedSha())
  else {
    stage(root, cases)
    console.log('conformance: capturing on the pinned Zeebe engine (formal/traces/capture.sh)…')
    capture(root, path.join(work, 'capture.log'))
  }

  const ctx = { jar: tlaJar(), spec: readSpec('ZeebeTokenFlow'), work: fs.mkdtempSync(path.join(work, 'tlc-')) }
  const byGraph = new Map(graphs.map((g) => [g.id, { graph: g, observed: new Set() }]))
  const divergences = []

  // Soundness: every captured trace is a behaviour of ZeebeTokenFlow.
  let checked = 0
  await pool(cases, args.jobs, async (c) => {
    const golden = JSON.parse(fs.readFileSync(path.join(root, 'goldens', `${c.name}.trace.json`), 'utf8'))
    const { trace, steps } = traceMilestones(c.graph, golden)
    for (const d of observedDecisions(c.graph, steps)) byGraph.get(c.graph.id).observed.add(d)
    const v = await validateTrace(ctx, c.name, c.graph, trace)
    checked++
    if (!v.ok) {
      divergences.push({ kind: 'trace', name: c.name, graphId: c.graph.id, graphKey: c.graph.key ?? c.graph.id, trace, matched: v.matched, unmatched: v.unmatched, bound: args.bound })
      console.log(`REJECTED ${c.name}: model matches ${v.matched}/${trace.length}, then cannot produce ${v.unmatched}\n  ${trace.join(' ')}`)
    }
  })
  console.log(`conformance: ${checked} trace(s) validated by TLC`)

  // Coverage: every model-enabled join decision is observed in Zeebe.
  let decisions = 0
  await pool([...byGraph.values()], args.jobs, async ({ graph, observed }) => {
    if (!Object.keys(graph.nodes).some((n) => isJoin(graph, n))) return
    for (const d of await modelDecisions(ctx, graph)) {
      decisions++
      if (!observed.has(d)) {
        divergences.push({ kind: 'coverage', graphId: graph.id, graphKey: graph.key ?? graph.id, decision: d, bound: args.bound })
        console.log(`UNCOVERED ${graph.id}: the model can take ${d}, no Zeebe trace of the graph does`)
      }
    }
  })
  console.log(`conformance: ${decisions} model join decision(s) checked for coverage`)

  // One divergence per signature (the smallest-first graph order keeps the first).
  const seen = new Map()
  for (const d of divergences) {
    const sig = signatureOf(d)
    if (!seen.has(sig)) seen.set(sig, d)
  }
  fs.writeFileSync(path.join(work, 'divergences.json'), JSON.stringify([...seen.entries()].map(([sig, d]) => ({ signature: sig, ...d })), null, 2) + '\n')
  let fresh = 0
  if (args.file) {
    for (const [sig, d] of seen) {
      const f = fileIssue(d, sig)
      console.log(`${sig}: ${f.known ? 'known divergence' : 'NEW divergence filed'} ${f.ref}`)
      if (!f.known) fresh++
    }
  }
  console.log(`conformance: ${seen.size} divergence class(es)${args.file ? `, ${fresh} new filed` : ''}`)
  process.exit((args.file ? fresh : seen.size) ? 1 : 0)
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main(process.argv.slice(2)).catch((err) => { console.error(err); process.exit(1) })
}

