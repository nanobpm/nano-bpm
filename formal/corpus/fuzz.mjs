#!/usr/bin/env node
// Differential fuzzing with shrinking (#1250, part of #1240 / #1224).
//
// The bounded-exhaustive run (exhaustive.mjs, #1246) proves parity for EVERY
// graph up to a small bound. This fuzzer reaches beyond it: seeded RANDOM
// well-formed process graphs larger than the bound (more gateways, more flows)
// driven by a seeded RANDOM command order (job completions, message
// correlations, timer ticks, instance cancellations, per-activation routing
// choices) over subprocesses, boundary events and timers too (#1673), run against both engines and
// checked by the SAME differential oracle (exhaustive.mjs `checkScenario`:
// the reference outcome, nano, and — when reachable — live Camunda 8).
//
//   node formal/corpus/fuzz.mjs --seed S [--cases N]
//        [--min-gateways A] [--max-gateways B] [--kinds xor,and,or,evt]
//        [--labels task,timer,sub,btimer,bmsg] [--cancel-rate R]
//        [--backend nano|camunda|both] [--require-camunda]
//        [--record] [--file-issues] [--replay-only] [--out DIR]
//
// Everything is a pure function of `--seed`: case `i` draws from its own PRNG
// seeded by `caseSeed(seed, i)`, so a failure is reproduced by re-running the
// same seed; the failing case's own seed is printed with every divergence.
// There are NO retries: a divergence is a defect.
//
// Any divergence is SHRUNK (`shrinkGraph`): greedily remove gateways, flows and
// wait states while the graph stays well-formed and still reproduces the SAME
// divergence signature on some command order, then keep the fewest-step
// reproducing scenario. `--record` appends the minimal reproducer to
// fuzz-corpus.json — the PERMANENT corpus — and `--file-issues` files a parity
// issue for each NEW signature (gh CLI; a signature with an open issue gets a
// comment there instead, #1975) and stamps the issue into the case.
//
// Every run first REPLAYS the committed corpus: an open case (`fixed: false`)
// must still reproduce its own signature (else it fails as stale — flip it to
// `fixed: true`, never delete it); a fixed case must show NO divergence (a
// regression guard forever). A committed case without a filed `issue` fails.
// A fuzzed divergence is known only on an open committed case's OWN graph:
// field-based signatures group unrelated defects (exhaustive.mjs `classify`),
// so a signature-only match would let an open case suppress a DISTINCT defect
// on any other graph that happens to mismatch the same fields.
import { execFileSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { GATEWAY_KINDS, EDGE_LABELS, BOUNDARY_LABELS, wellFormed, materialize, randomRun, exploreOrders, scenariosFor, stepSignatures } from './enumerate.mjs'
import { bpmnFor } from './generate.mjs'
import { openBackends, checkScenario } from './exhaustive.mjs'
import { prng } from '../traces/fuzz/generate.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
export const CORPUS_PATH = path.join(here, 'fuzz-corpus.json')
/** canonicalKey enumerates k! relabellings: keep random graphs within reach. */
export const MAX_GATEWAYS = 7

// ---------------------------------------------------------------------------
// Arguments

export function parseArgs (argv) {
  const a = { seed: null, cases: 200, minGateways: 3, maxGateways: 5, kinds: GATEWAY_KINDS, labels: WAIT_LABELS, cancelRate: DEFAULT_CANCEL_RATE, backend: 'nano', requireCamunda: false, record: false, fileIssues: false, replayOnly: false, out: null }
  for (let i = 0; i < argv.length; i++) {
    const x = argv[i]
    const val = () => { if (i + 1 >= argv.length) throw new Error(`${x} needs a value`); return argv[++i] }
    const int = () => { const v = Number(val()); if (!Number.isInteger(v) || v < 0) throw new Error(`${x} needs a non-negative integer`); return v }
    if (x === '--seed') a.seed = int()
    else if (x === '--cases') a.cases = int()
    else if (x === '--min-gateways') a.minGateways = int()
    else if (x === '--max-gateways') a.maxGateways = int()
    else if (x === '--kinds') a.kinds = val().split(',')
    else if (x === '--labels') a.labels = val().split(',')
    else if (x === '--cancel-rate') { const v = Number(val()); if (!(v >= 0 && v <= 1)) throw new Error('--cancel-rate needs a number in [0, 1]'); a.cancelRate = v }
    else if (x === '--backend') a.backend = val()
    else if (x === '--require-camunda') a.requireCamunda = true
    else if (x === '--record') a.record = true
    else if (x === '--file-issues') a.fileIssues = true
    else if (x === '--replay-only') a.replayOnly = true
    else if (x === '--out') a.out = val()
    else throw new Error(`unknown argument: ${x}`)
  }
  if (!['nano', 'camunda', 'both'].includes(a.backend)) throw new Error(`--backend must be nano|camunda|both, got ${a.backend}`)
  if (a.requireCamunda && a.backend === 'nano') throw new Error('--require-camunda needs --backend camunda|both')
  if (a.fileIssues && !a.record) throw new Error('--file-issues needs --record')
  if (!a.replayOnly && a.seed === null) throw new Error('--seed is required (the run is deterministic from its seed)')
  if (a.minGateways > a.maxGateways) throw new Error('--min-gateways exceeds --max-gateways')
  if (a.maxGateways > MAX_GATEWAYS) throw new Error(`--max-gateways is capped at ${MAX_GATEWAYS}`)
  for (const k of a.kinds) if (!GATEWAY_KINDS.includes(k)) throw new Error(`unknown gateway kind ${JSON.stringify(k)}`)
  for (const l of a.labels) if (!WAIT_LABELS.includes(l)) throw new Error(`unknown wait-state label ${JSON.stringify(l)} (known: ${WAIT_LABELS.join(', ')})`)
  if (a.labels.length === 0) throw new Error('--labels needs at least one wait-state label')
  return a
}

// ---------------------------------------------------------------------------
// Seeded generation

/** The seed of case `i` of a run: a 32-bit mix of the run seed and the index. */
export function caseSeed (seed, i) {
  let h = (Math.imul(seed >>> 0, 0x9e3779b1) ^ Math.imul(i + 1, 0x85ebca6b)) >>> 0
  h = Math.imul(h ^ (h >>> 16), 0x7feb352d) >>> 0
  return (h ^ (h >>> 15)) >>> 0
}

/**
 * The wait-state edge labels a random skeleton draws from (#1673): every
 * enumerate.mjs label except `direct` (drawn by `taskBias`) and `msg` (forced
 * by an event-based gateway).
 */
export const WAIT_LABELS = EDGE_LABELS.filter((l) => l !== 'direct' && l !== 'msg')
/** The default per-step probability a random walk cancels the instance (#1673). */
export const DEFAULT_CANCEL_RATE = 0.05

/** `pick(n)`: a uniform integer in [0, n) from a seeded PRNG. */
export const picker = (rng) => (n) => Math.floor(rng.next() * n)

/**
 * A random well-formed skeleton (enumerate.mjs's shape and rules) with
 * `minGateways..maxGateways` gateways over `kinds`. Built constructively — a
 * spanning DAG from S, then degree repair so every gateway is a split, join or
 * mixed gateway and every event-based gateway a pure split — then checked
 * against `wellFormed`, the one definition of well-formedness.
 */
export function randomSkeleton (pick, { minGateways = 3, maxGateways = 5, kinds = GATEWAY_KINDS, labels = WAIT_LABELS, taskBias = 0.5 } = {}) {
  const k = minGateways + pick(maxGateways - minGateways + 1)
  const ks = Array.from({ length: k }, () => kinds[pick(kinds.length)])
  const E = k + 1
  const raw = []
  const deg = () => {
    const inD = new Array(k + 2).fill(0)
    const outD = new Array(k + 2).fill(0)
    for (const [i, j] of raw) { outD[i]++; inD[j]++ }
    return { inD, outD }
  }
  raw.push([0, k === 0 ? E : 1])
  // Spanning: every gateway past the first is reached from an earlier gateway.
  for (let g = 2; g <= k; g++) raw.push([1 + pick(g - 1), g])
  // A flow out of `g` to a later node that never gives an event-based gateway
  // (a pure split: one incoming flow only) a second incoming flow — so the
  // requested `kinds` vocabulary is never overridden.
  const open = (g) => {
    const { inD: now } = deg()
    const js = []
    for (let j = g + 1; j <= E; j++) if (j === E || ks[j - 1] !== 'evt' || now[j] === 0) js.push(j)
    return js[pick(js.length)] // never empty: E is always open
  }
  // A few extra random forward flows between gateways / to E.
  const extra = pick(k + 1)
  for (let x = 0; x < extra && k > 0; x++) {
    const i = 1 + pick(k)
    raw.push([i, open(i)])
  }
  // Degree repair: every gateway leads on, and is never a 1-in-1-out no-op.
  for (let g = 1; g <= k; g++) {
    let d = deg()
    if (d.outD[g] === 0) raw.push([g, open(g)])
    d = deg()
    const needSplit = ks[g - 1] === 'evt' ? d.outD[g] < 2 : d.inD[g] < 2 && d.outD[g] < 2
    if (needSplit) raw.push([g, open(g)])
  }
  if (deg().inD[E] === 0) raw.push([k, E])
  const edges = raw.map(([i, j]) => {
    const fromEvt = i > 0 && ks[i - 1] === 'evt'
    if (fromEvt) return [i, j, 'msg']
    if (pick(1000) >= taskBias * 1000) return [i, j, 'direct']
    // A boundary edge needs a merge-capable target (enumerate.mjs
    // `wellFormed`); off one, a drawn boundary label degrades to a plain wait
    // state of the same family (`task`, or the first non-boundary label).
    const label = labels[pick(labels.length)]
    if (!BOUNDARY_LABELS.includes(label) || j === E || ks[j - 1] !== 'evt') return [i, j, label]
    return [i, j, labels.find((l) => !BOUNDARY_LABELS.includes(l)) ?? 'direct']
  })
  if (!wellFormed(ks, edges)) throw new Error(`randomSkeleton produced an ill-formed skeleton: ${JSON.stringify({ kinds: ks, edges })}`)
  return { kinds: ks, edges }
}

/** A runtime entry (`{ skel, graph, artifacts }`) of one graph and its runs. */
export function entryFor (skel, runs, name) {
  const graph = materialize(skel)
  const { scenarios } = scenariosFor(graph, runs(graph), { names: (o) => `${graph.id}-${name}-o${o}` })
  return { skel, graph, artifacts: { bpmn: bpmnFor(graph, { routing: 'variables' }), scenarios } }
}

/** Fuzz case `seed`: one random graph, one random command order. */
export function fuzzCase (seed, opts = {}) {
  const pick = picker(prng(seed))
  const skel = randomSkeleton(pick, opts)
  const cancelRate = opts.cancelRate ?? DEFAULT_CANCEL_RATE
  return entryFor(skel, (graph) => [randomRun(graph, pick, { cancelRate })], `s${seed}`)
}

// ---------------------------------------------------------------------------
// Shrinking

/**
 * Every one-step reduction of a skeleton that stays well-formed: drop a
 * gateway (with its flows), drop a flow, simplify a wait-state flow (a
 * boundary flow to a task one, any wait-state flow to a direct one).
 * Ordered most-reducing first, so the greedy pass removes gateways first.
 */
export function reductions (skel) {
  const out = []
  const k = skel.kinds.length
  for (let g = 1; g <= k; g++) {
    const kinds = skel.kinds.filter((_, i) => i !== g - 1)
    const re = (n) => (n > g ? n - 1 : n)
    const edges = skel.edges.filter(([i, j]) => i !== g && j !== g).map(([i, j, l]) => [re(i), re(j), l])
    // The gateway's single predecessor flow may now need a new target: bridge
    // each (pred -> g) to each (g -> succ) when g was a 1-in pass-through.
    const preds = skel.edges.filter(([, j]) => j === g)
    const succs = skel.edges.filter(([i]) => i === g)
    const bridges = preds.length === 1
      ? succs.map(([, j]) => [...edges, [re(preds[0][0]), re(j), preds[0][2]]])
      : []
    for (const cand of [edges, ...bridges]) if (wellFormed(kinds, cand)) out.push({ kinds, edges: cand })
  }
  for (let e = 0; e < skel.edges.length; e++) {
    const edges = skel.edges.filter((_, i) => i !== e)
    if (wellFormed(skel.kinds, edges)) out.push({ kinds: skel.kinds, edges })
  }
  for (let e = 0; e < skel.edges.length; e++) {
    const l = skel.edges[e][2]
    if (l === 'direct' || l === 'msg') continue
    for (const simpler of BOUNDARY_LABELS.includes(l) ? ['task', 'direct'] : ['direct']) {
      const edges = skel.edges.map((x, i) => (i === e ? [x[0], x[1], simpler] : x))
      if (wellFormed(skel.kinds, edges)) out.push({ kinds: skel.kinds, edges })
    }
  }
  return out
}

const stepCount = (sc) => sc.steps.length

/**
 * Greedily shrink a divergence. `reproduce(skel)` is async and returns the
 * fewest-step divergence of that graph matching the target signature, or null.
 * Keeps applying the first reproducing reduction until none reproduces, so the
 * result is 1-minimal: no single reduction of it still diverges.
 */
export async function shrinkGraph (skel, reproduce, initial, onBest = () => {}) {
  let best = { skel, d: initial }
  onBest(best)
  let progress = true
  while (progress) {
    progress = false
    for (const cand of reductions(best.skel)) {
      const d = await reproduce(cand)
      if (d) { best = { skel: cand, d }; onBest(best); progress = true; break }
    }
  }
  return best
}

/** Command orders tried per shrink candidate: the first DFS ones plus random walks. */
export const SHRINK_ORDERS = 64
export const SHRINK_WALKS = 32

/** The candidate runs of a graph during shrinking (deterministic). */
export function shrinkRuns (graph) {
  // Cancellation is in the vocabulary (#1673), so a cancel-dependent
  // divergence must stay reproducible while shrinking.
  const runs = exploreOrders(graph, {}, { limit: SHRINK_ORDERS, cancel: true })
  for (let w = 0; w < SHRINK_WALKS; w++) runs.push(randomRun(graph, picker(prng(w + 1)), { cancelRate: DEFAULT_CANCEL_RATE }))
  const seen = new Set()
  return runs.filter((r) => {
    const key = JSON.stringify([r.route, r.steps])
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

// ---------------------------------------------------------------------------
// The permanent corpus

export function loadCorpus (file = CORPUS_PATH) {
  return JSON.parse(fs.readFileSync(file, 'utf8')).cases
}

/** The committed record of a minimal divergence. */
export function caseRecord (seed, skel, d) {
  const { id, key } = d.entry.graph
  return {
    signature: d.signature,
    kind: d.kind,
    issue: null,
    fixed: false,
    seed,
    graph: { id, key },
    skeleton: skel,
    scenario: d.scenario,
    detail: d.detail
  }
}

/** Structural problems of the committed corpus (each one fails the run). */
export function corpusProblems (cases) {
  const problems = []
  for (const c of cases) {
    const label = `${c.signature} (${c.graph?.id})`
    if (!c.issue) problems.push(`committed fuzz case ${label} has no filed issue — file one and set "issue"`)
    if (typeof c.fixed !== 'boolean') problems.push(`committed fuzz case ${label} needs a boolean "fixed"`)
    if (!c.skeleton || !c.scenario) problems.push(`committed fuzz case ${label} needs "skeleton" and "scenario"`)
    else if (materialize(c.skeleton).key !== c.graph.key) problems.push(`committed fuzz case ${label}: its skeleton no longer materialises to graph ${c.graph.key}`)
    else {
      const derived = rederive(c)
      if (!derived) problems.push(`committed fuzz case ${label}: its scenario is no longer a behaviour of the current Reference — re-derive it (rederive / rederivedScenarios) and re-judge the case`)
      else if (canonJson(derived.expect) !== canonJson(c.scenario.expect)) problems.push(`committed fuzz case ${label}: its frozen "expect" is stale against the current Reference — re-derive it and re-judge the case`)
    }
  }
  return problems
}

/**
 * Canonical JSON of a scenario part: object keys sorted, and the
 * per-execution `correlationKey` (derived from the scenario NAME) ignored, so
 * two derivations of the same behaviour compare equal.
 */
export function canonJson (v) {
  return JSON.stringify(v, (k, x) => k === 'correlationKey'
    ? undefined
    : x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) : x)
}

/**
 * Every scenario the CURRENT Reference derives for a committed case's graph
 * under the case's own route variables, whose start variables equal the
 * case's — restricted to runs following the case's recorded command sequence
 * unless `exhaustive` (every command order, cancellation included).
 */
export function rederivedScenarios (c, { exhaustive = false } = {}) {
  const graph = materialize(c.skeleton)
  const vars = c.scenario.variables ?? {}
  const route = Object.fromEntries(Object.entries(vars).filter(([k]) => k.startsWith('route_')))
  // By default follow only the case's own recorded command sequence: every
  // other order yields different steps, and enumerating them all grows
  // factorially with independent parallel jobs (#1747 review).
  const follow = exhaustive ? null : stepSignatures(c.scenario.steps)
  const { scenarios } = scenariosFor(graph, exploreOrders(graph, route, { cancel: true, follow }))
  return scenarios.filter((s) => canonJson(s.variables ?? {}) === canonJson(vars))
}

/**
 * The current-Reference scenario a committed case's frozen scenario must still
 * be — the same start variables and the same steps — or null.
 *
 * A committed case freezes its scenario's `expect` from the Reference that
 * found it. If the Reference is later corrected (#1693: it took a fork's flows
 * in document order where Zeebe takes them reversed), the frozen expectation
 * silently keeps "reproducing" a divergence that is a REFERENCE defect, not an
 * engine one. Re-deriving every case on each replay makes that drift fail
 * loudly instead.
 */
export function rederive (c) {
  const steps = canonJson(c.scenario.steps)
  return rederivedScenarios(c).find((s) => canonJson(s.steps) === steps) ?? null
}

/**
 * Judge one committed case against the divergences its own scenario produced
 * this run: an open case must reproduce its signature, a fixed one must agree.
 */
export function judgeCase (c, divergences) {
  const sigs = divergences.map((d) => d.signature)
  if (c.fixed) return sigs.length === 0 ? null : `REGRESSION fixed fuzz case ${c.signature} (${c.graph.id}) diverges again: ${sigs.join(', ')}`
  return sigs.includes(c.signature) ? null : `stale fuzz case ${c.signature} (${c.graph.id}) no longer reproduces — set "fixed": true (keep it as a regression guard)`
}

/**
 * The divergences of a replayed scenario no open corpus case explains: they
 * are new failures and go through the shrink/record path like fuzz findings.
 *
 * `openKeys` holds one `knownKey(signature, graphKey)` per open committed
 * case, so an open case explains only a divergence on ITS OWN graph. Keying
 * on the signature alone would let a field-based signature shared across
 * graphs (a committed fixed case and an open one can carry the same field
 * set) absorb a DISTINCT defect on an unrelated graph — it would be logged
 * "known", never shrunk, recorded or filed (exhaustive.mjs `classify`
 * documents the same hazard and scopes by signature + graph + scenario).
 */
export function unexplained (divergences, openKeys) {
  return divergences.filter((d) => !openKeys.has(knownKey(d.signature, d.entry.graph.key)))
}

/** The identity of one known divergence: its signature on one graph. */
export const knownKey = (signature, graphKey) => JSON.stringify([signature, graphKey])

/**
 * Whether a committed case is replayed this run. A FIXED case is a regression
 * guard checked on whatever backends ran (Nano always does): it must show no
 * divergence anywhere. Only an OPEN case needs its own phase, since its
 * reproduction / staleness verdict is about that phase's signature.
 */
export const caseRunnable = (c, phasesRan) => c.fixed === true || phasesRan.has(c.kind)

/** The fewer-step of two divergences (the first on a tie; null-tolerant). */
export const shorterRepro = (a, b) => (!a ? b : !b ? a : b.scenario.steps.length < a.scenario.steps.length ? b : a)

/**
 * A per-run execution namespace: every replay / shrink execution gets a fresh
 * deterministic scenario name (and so correlation key), so a repeated graph
 * can never interact with messages or instances an earlier attempt left on a
 * backend.
 */
/**
 * A clone of stored scenario `sc` under the fresh execution name `name`: every
 * occurrence of its old name (the scenario name, the start-variable
 * correlation key, each message step's key, expected variables) is replaced,
 * so a replay never reuses a key an earlier execution left on a backend.
 */
export function renameScenario (sc, name) {
  const old = sc.name
  const walk = (v) => v === old ? name
    : Array.isArray(v) ? v.map(walk)
      : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, walk(x)]))
        : v
  return { ...walk(sc), name }
}

export function executionNamer (tag) {
  let n = 0
  return (kind) => `${kind}${tag}x${++n}`
}

/**
 * The body of the parity issue filed for committed-to-be case `c`.
 *
 * The reproducer is written to the run's WORKING-TREE corpus before the issue
 * is filed; it is not on `main` until that corpus diff is committed. The body
 * must not claim otherwise: #1753–#1758 were filed saying "committed in
 * fuzz-corpus.json" from a run whose corpus diff never landed, so their
 * reproducers lived only in the issue text and no replay guarded them. The
 * body therefore states the commit obligation and carries the case identity
 * (`corpusIdentity`) so triage can match the issue to its corpus case; the
 * `uncommittedIssues` guard then fails any triaged issue whose committed
 * reproducer is still absent.
 */
export function issueBody (c) {
  return [
    'Found by the differential fuzzer (`formal/corpus/fuzz.mjs`, #1250) and shrunk to a minimal reproducer.',
    '',
    signatureLine(c.signature),
    `- run seed: \`${c.seed}\``,
    `- graph: \`${c.graph.id}\` (\`${c.graph.key}\`)`,
    `- corpus identity: \`${corpusIdentity(c)}\``,
    '',
    'Reproducer — recorded in the filing run\'s working-tree `formal/corpus/fuzz-corpus.json`. It is NOT guarded until that corpus diff is committed with `"issue"` set to this issue: commit it (open, or `"fixed": true` once resolved) as part of triage.',
    '',
    '```json',
    JSON.stringify({ skeleton: c.skeleton, scenario: c.scenario, detail: c.detail }, null, 2),
    '```',
    '',
    'Part of #1240.'
  ].join('\n')
}

/** The stable identity of a corpus case: its graph key and signature. */
export const corpusIdentity = (c) => `${c.graph.key}#${c.signature}`

/**
 * The issues among `issues` (`#N`) that no committed case names — a filed
 * finding whose reproducer never reached the corpus, so nothing replays it.
 * `aliases` maps an issue triaged as a duplicate to the issue whose committed
 * case covers it.
 */
export function uncommittedIssues (issues, cases, aliases = {}) {
  const named = new Set(cases.map((x) => x.issue))
  return issues.filter((i) => !named.has(aliases[i] ?? i))
}

/** The issue-body line that carries a filed finding's divergence signature. */
export const signatureLine = (signature) => `- signature: \`${signature}\``

/**
 * The open fuzz issue (`#N`) already filed for `signature`, or null. `open` is
 * the open issues as `{ number, body }`. The match is on the exact signature
 * line of `issueBody`, never a substring: field signatures nest
 * (`spec-oracle:completed,sequenceFlows` is a prefix of
 * `spec-oracle:completed,sequenceFlows,…`), and GitHub search is fuzzy. The
 * OLDEST match wins, so a signature with duplicates already filed converges
 * on the canonical issue.
 */
export function existingIssueFor (signature, open) {
  const line = signatureLine(signature)
  const hits = open.filter((i) => String(i.body ?? '').split('\n').some((l) => l.trim() === line)).map((i) => i.number)
  return hits.length ? `#${Math.min(...hits)}` : null
}

/** The comment that reports a re-found signature on its existing open issue. */
export function recurrenceComment (c) {
  return [
    `The differential fuzzer found this signature again (run seed \`${c.seed}\`, graph \`${c.graph.id}\` / \`${c.graph.key}\`) — commented here instead of filing a duplicate.`,
    '',
    '```json',
    JSON.stringify({ skeleton: c.skeleton, scenario: c.scenario, detail: c.detail }, null, 2),
    '```'
  ].join('\n')
}

/**
 * File the finding `c`, deduplicated by signature: an OPEN issue already
 * carrying the signature gets a comment and is returned; only an unseen
 * signature creates an issue (nightly runs used to re-file one signature per
 * graph — #1792/#1895/#1966, #1793/#1896/#1967 — #1975). `gh` runs the CLI.
 */
export function fileIssue (c, gh = (args) => execFileSync('gh', args, { encoding: 'utf8' })) {
  const open = JSON.parse(gh(['issue', 'list', '--state', 'open', '--search', 'parity(fuzz) in:title', '--limit', '1000', '--json', 'number,body']))
  const existing = existingIssueFor(c.signature, open)
  if (existing) {
    gh(['issue', 'comment', existing.slice(1), '--body', recurrenceComment(c)])
    return existing
  }
  const url = gh(['issue', 'create', '--title', `parity(fuzz): ${c.signature.slice(0, 180)}`, '--body', issueBody(c)]).trim()
  return `#${url.split('/').pop()}`
}

// ---------------------------------------------------------------------------

async function main () {
  const args = parseArgs(process.argv.slice(2))
  const log = (m) => console.log(m)
  const t0 = Date.now()
  const cases = loadCorpus()
  const failures = corpusProblems(cases)
  const backends = await openBackends(args, log)
  const fresh = new Map()
  // An open case marks only a divergence on ITS OWN graph as known: a
  // field-based signature shared across graphs (e.g. a fixed case and an open
  // one carrying the same mismatched fields) must never let the open case
  // absorb a distinct defect on an unrelated graph.
  const known = new Set(cases.filter((c) => !c.fixed).map((c) => knownKey(c.signature, c.graph.key)))
  // A completed minimum is persisted at once, so a later scenario error or a
  // failing backend close can never discard it.
  const nextName = executionNamer(`r${args.seed ?? 'replay'}`)
  // The latest reproducer (the discovered one, then each accepted reduction)
  // is checkpointed, so a shrink aborted by a backend error still keeps it.
  const found = async (seed, skel, d) => {
    let latest = { seed, skel, d }
    let min
    try {
      min = { seed, ...(await shrink(backends, skel, d, nextName, (b) => { latest = { seed, ...b } })) }
    } catch (err) {
      fresh.set(d.signature, latest)
      if (args.record) record(latest)
      failures.push(`shrinking ${d.signature} aborted (unshrunk-or-partial reproducer kept): ${err.message ?? err}`)
      throw err
    }
    fresh.set(d.signature, min)
    if (args.record) record(min)
  }
  let ran = 0
  try {
    // 1. Replay the permanent corpus.
    for (const c of cases) {
      if (!c.skeleton || !c.scenario || !caseRunnable(c, backends.phasesRan)) continue
      const name = nextName('replay')
      const entry = entryFor(c.skeleton, () => [], name)
      const divs = await checkScenario(backends, entry, renameScenario(c.scenario, `${entry.graph.id}-${name}`))
      const verdict = judgeCase(c, divs)
      if (verdict) failures.push(verdict)
      // A fixed case's divergence is already a REGRESSION verdict; an open
      // case's must still not hide a different, unknown divergence — and an
      // open case explains a divergence only on its own graph, so a distinct
      // defect sharing its field-based signature is surfaced, not absorbed.
      if (c.fixed) continue
      for (const d of unexplained(divs, known)) {
        if (fresh.has(d.signature)) continue
        log(`new divergence ${d.signature} while replaying ${c.signature} — shrinking`)
        await found(c.seed, c.skeleton, d)
      }
    }
    log(`replayed ${cases.length} committed fuzz case(s)`)
    if (args.replayOnly) return finish()

    // 2. Fuzz.
    const opts = { minGateways: args.minGateways, maxGateways: args.maxGateways, kinds: args.kinds, labels: args.labels, cancelRate: args.cancelRate }
    for (let i = 0; i < args.cases; i++) {
      const s = caseSeed(args.seed, i)
      const entry = fuzzCase(s, opts)
      if (args.out) writeEntry(args.out, entry)
      for (const sc of entry.artifacts.scenarios) {
        ran++
        for (const d of await checkScenario(backends, entry, sc)) {
          if (known.has(knownKey(d.signature, d.entry.graph.key))) { log(`known ${d.signature} on ${d.entry.graph.key} (case seed ${s})`); continue }
          if (fresh.has(d.signature)) continue
          log(`divergence ${d.signature} at case ${i} (case seed ${s}, ${entry.graph.id}) — shrinking`)
          await found(s, entry.skel, d)
        }
      }
      if ((i + 1) % 50 === 0) log(`  fuzz ${i + 1}/${args.cases}`)
    }
  } finally {
    await backends.close()
  }
  return finish()

  function finish () {
    for (const [sig, { seed, skel, d }] of fresh) {
      failures.push(`FAIL divergence ${sig} (case seed ${seed})\n  minimal graph ${d.entry.graph.id} ${JSON.stringify(skel)}\n  scenario ${JSON.stringify(d.scenario.steps)}\n  ${JSON.stringify(d.detail)}`)
    }
    if (args.record && fresh.size > 0) {
      console.error(`recorded ${fresh.size} minimal case(s) in formal/corpus/fuzz-corpus.json${args.fileIssues ? ' (issues filed)' : ' — file an issue for each and set "issue"'}`)
    }
    for (const f of failures) console.error(f.startsWith('FAIL') ? f : `FAIL ${f}`)
    log(`${ran} fuzzed scenario(s) in ${Math.round((Date.now() - t0) / 1000)}s: ${fresh.size} new divergence class(es)`)
    if (failures.length > 0) process.exitCode = 1
    else log('ok    no unexplained divergence')
  }

  function record ({ seed, skel, d }) {
    const file = JSON.parse(fs.readFileSync(CORPUS_PATH, 'utf8'))
    const rec = caseRecord(seed, skel, d)
    file.cases.push(rec)
    // Persist the reproducer BEFORE asking GitHub for an issue: a failing
    // `gh issue create` must never discard it.
    fs.writeFileSync(CORPUS_PATH, JSON.stringify(file, null, 2) + '\n')
    if (!args.fileIssues) return
    try {
      rec.issue = fileIssue(rec)
    } catch (err) {
      failures.push(`could not file an issue for ${rec.signature} (reproducer recorded with "issue": null): ${err.message}`)
      return
    }
    fs.writeFileSync(CORPUS_PATH, JSON.stringify(file, null, 2) + '\n')
  }
}

async function shrink (backends, skel, d, nextName, onBest) {
  const reproduce = async (cand) => {
    let best = null
    const entry = entryFor(cand, shrinkRuns, nextName('shrink'))
    for (const sc of entry.artifacts.scenarios) {
      if (best && stepCount(sc) >= stepCount(best.scenario)) continue
      const hit = (await checkScenario(backends, entry, sc)).find((x) => x.signature === d.signature)
      if (hit) best = hit
    }
    return best
  }
  // The original graph's own fewest-step reproducer is the starting point.
  // The original graph's known reproducer `d` is kept when the bounded
  // shrink sample only finds longer ones.
  const start = shorterRepro(d, await reproduce(skel))
  const { skel: minSkel, d: minD } = await shrinkGraph(skel, reproduce, start, onBest)
  return { skel: minSkel, d: minD }
}

function writeEntry (out, entry) {
  const dir = path.join(out, entry.graph.id)
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(path.join(dir, 'process.bpmn'), entry.artifacts.bpmn)
  fs.writeFileSync(path.join(dir, 'graph.json'), JSON.stringify(entry.graph, null, 2) + '\n')
  for (const sc of entry.artifacts.scenarios) fs.writeFileSync(path.join(dir, `${sc.name}.json`), JSON.stringify(sc, null, 2) + '\n')
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((err) => { console.error(err); process.exit(1) })
}
