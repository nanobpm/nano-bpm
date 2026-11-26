#!/usr/bin/env node
// Bounded-exhaustive parity run (#1246, part of #1240 / #1224).
//
// Enumerates every well-formed process graph up to the bound (enumerate.mjs),
// emits each graph's MC/ZMC models, BPMN and scenario scripts from the one
// source, and runs them through:
//
//   tlc      TLC on the nano spec (TokenFlow, MC*) and on the Zeebe reference
//            (ZeebeTokenFlow, ZMC*). A divergence is a `RefinesZeebe`
//            violation, or a verdict (the set of violated properties) that
//            differs between the two specs. The reference scheduler that derives
//            the scenarios is cross-checked too: a reference run that gets stuck
//            must be a TLC Termination / NoStuckInstance violation.
//   nano     every scenario against the engine-wasm TestEngine, checked against
//            the reference outcome (completion + jobs created + variables).
//   camunda  every completing scenario against a live Camunda 8, diffed against
//            nano over the fields both expose (formal/parity/runtime).
//
//   node formal/corpus/exhaustive.mjs --bound ci|nightly
//        [--gateways N] [--flows M] [--kinds xor,and,or,evt]
//        [--tlc] [--backend nano|camunda|both] [--require-camunda]
//        [--record] [--out DIR] [--coverage-dir DIR]
//
// `--coverage-dir DIR` (with --tlc) runs TLC with `-coverage` and keeps each
// model log as DIR/<module>.log for the action-coverage gate
// (formal/tla/coverage.mjs, #1248).
//
// The bound is configurable: `--bound` reads a named bound from bounds.json
// (CI runs `ci`, the nightly/manual workflow `nightly`); --gateways/--flows/
// --kinds override it.
//
// Any divergence FAILS the run. Each one is shrunk to a minimal case: graphs are
// enumerated smallest-first (flows, then gateways), so the first graph showing a
// divergence class (its `signature`) is a smallest such graph within the bound,
// and of its failing scenarios the one with the fewest steps is kept. `--record`
// writes those minimal cases into divergences.json, the committed corpus of
// known divergences. A committed case needs a filed `issue` — an entry without
// one fails the run, so a recorded divergence cannot be parked silently — and a
// committed case that no longer reproduces inside the bound fails as stale, so a
// fix must delete it (the ratchet only shrinks). No retries anywhere.
import { spawn, execFileSync } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { enumerateGraphs, artifactsFor, exploreOrders, routesFor, GATEWAY_KINDS } from './enumerate.mjs'
import { checkSpecOracle } from './spec-oracle.mjs'
import { installSignalCleanup, backendEligible } from '../parity/runtime/run.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const tlaDir = path.join(here, '..', 'tla')
export const DIVERGENCES_PATH = path.join(here, 'divergences.json')
export const BOUNDS_PATH = path.join(here, 'bounds.json')

// ---------------------------------------------------------------------------
// Arguments and the bound

export function parseArgs (argv) {
  const a = { bound: null, gateways: null, flows: null, kinds: null, tlc: false, backend: null, requireCamunda: false, record: false, out: null, coverageDir: null }
  for (let i = 0; i < argv.length; i++) {
    const x = argv[i]
    const val = () => { if (i + 1 >= argv.length) throw new Error(`${x} needs a value`); return argv[++i] }
    if (x === '--bound') a.bound = val()
    else if (x === '--gateways') a.gateways = Number(val())
    else if (x === '--flows') a.flows = Number(val())
    else if (x === '--kinds') a.kinds = val().split(',')
    else if (x === '--tlc') a.tlc = true
    else if (x === '--backend') a.backend = val()
    else if (x === '--require-camunda') a.requireCamunda = true
    else if (x === '--record') a.record = true
    else if (x === '--out') a.out = val()
    else if (x === '--coverage-dir') a.coverageDir = val()
    else throw new Error(`unknown argument: ${x}`)
  }
  if (a.backend && !['nano', 'camunda', 'both'].includes(a.backend)) throw new Error(`--backend must be nano|camunda|both, got ${a.backend}`)
  if (a.requireCamunda && !['camunda', 'both'].includes(a.backend)) throw new Error('--require-camunda needs --backend camunda|both')
  return a
}

/** The bound to run: a named bound from bounds.json, overridden per field. */
export function resolveBound (args, bounds = JSON.parse(fs.readFileSync(BOUNDS_PATH, 'utf8'))) {
  let base = {}
  if (args.bound) {
    if (!bounds[args.bound]) throw new Error(`unknown --bound ${args.bound} (known: ${Object.keys(bounds).join(', ')})`)
    base = bounds[args.bound]
  }
  const bound = {
    gateways: args.gateways ?? base.gateways,
    flows: args.flows ?? base.flows,
    kinds: args.kinds ?? base.kinds ?? GATEWAY_KINDS
  }
  for (const k of ['gateways', 'flows']) {
    if (!Number.isInteger(bound[k]) || bound[k] < 0) throw new Error(`the bound needs a non-negative integer ${k} (use --bound or --${k})`)
  }
  return bound
}

// ---------------------------------------------------------------------------
// TLC

// The spec descriptors (formal/tla/specs/*.spec) are the single source of each
// family's constants, invariants and properties; read them the way check.sh
// does (sourced by bash) rather than restating them here.
export function readSpec (family) {
  const file = path.join(tlaDir, 'specs', `${family}.spec`)
  const out = execFileSync('bash', ['-c',
    'set -euo pipefail; source "$1"; printf "%s\\n" "${SPEC_CONSTANTS[@]}"; echo "---"; printf "%s\\n" "${SPEC_INVARIANTS[@]}"; echo "---"; printf "%s\\n" "${SPEC_PROPERTIES[@]}"',
    '_', file], { encoding: 'utf8' })
  const [constants, invariants, properties] = out.split('---\n').map((s) => s.split('\n').filter(Boolean))
  return { constants, invariants, properties }
}

// The pinned tla2tools.jar: version and digest come from check.sh (its single
// source); check.sh fetches it on first run.
export function tlaJar () {
  const src = fs.readFileSync(path.join(tlaDir, 'check.sh'), 'utf8')
  const version = /^TLA_VERSION="(.*)"$/m.exec(src)[1]
  const jar = process.env.TLA2TOOLS_JAR ||
    path.join(process.env.XDG_CACHE_HOME || path.join(os.homedir(), '.cache'), 'nanobpm-formal', `tla2tools-${version}.jar`)
  if (!fs.existsSync(jar)) throw new Error(`no tla2tools.jar at ${jar}: run formal/tla/check.sh once to fetch the pinned one`)
  return jar
}

const cfgFor = (spec) => [
  'SPECIFICATION Spec',
  'CONSTANTS', ...spec.constants.map((c) => `    ${c}`),
  ...(spec.invariants.length ? ['INVARIANTS', ...spec.invariants.map((c) => `    ${c}`)] : []),
  ...(spec.properties.length ? ['PROPERTIES', ...spec.properties.map((c) => `    ${c}`)] : [])
].join('\n') + '\n'

/** Parse a TLC log into a verdict: 'pass', 'violates:<sorted,props>' or 'error'. */
export function verdictFromLog (log, properties, exitCode) {
  const violated = new Set()
  for (const m of log.matchAll(/Invariant ([A-Za-z0-9_]+) is violated/g)) violated.add(m[1])
  if (/Temporal properties were violated/.test(log)) violated.add(properties[0])
  if (/Deadlock reached/.test(log)) violated.add('Deadlock')
  if (/^Warning/m.test(log)) return 'error'
  if (violated.size > 0) return /\b0 states left on queue/.test(log) ? `violates:${[...violated].sort().join(',')}` : 'error'
  if (exitCode === 0 && /Model checking completed\. No error has been found\./.test(log)) return 'pass'
  return 'error'
}

// One TLC run in its OWN directory and JVM temp dir: concurrent TLC processes
// sharing either clobber each other's extracted standard modules.
// `extra` lists further module files (absolute paths) the model needs, e.g.
// the trace-validation wrapper of formal/traces/conformance (#1962).
export function runTlc (jar, work, module, tla, cfg, coverage = false, extra = []) {
  const dir = fs.mkdtempSync(path.join(work, `${module}-`))
  for (const base of ['TokenFlow', 'ZeebeTokenFlow']) fs.copyFileSync(path.join(tlaDir, `${base}.tla`), path.join(dir, `${base}.tla`))
  for (const file of extra) fs.copyFileSync(file, path.join(dir, path.basename(file)))
  fs.writeFileSync(path.join(dir, `${module}.tla`), tla)
  fs.mkdirSync(path.join(dir, 'tmp'))
  return new Promise((resolve) => {
    fs.writeFileSync(path.join(dir, `${module}.cfg`), cfg)
    const p = spawn('java', [
      // Each model is tiny: a C1-only JVM starts fastest and lets many run side
      // by side. Use a PARALLEL GC (as formal/tla/check.sh does) so TLC does not
      // emit its "use a throughput-optimised GC" advisory — WITHOUT `-nowarning`,
      // which would set TLCGlobals.warn = false and silence EVERY warning,
      // defeating the fail-closed `Warning:` gate in verdictFromLog (#1246
      // review). Warnings stay visible; any that remain fail the run.
      '-XX:+UseParallelGC', '-Xmx256m', '-XX:TieredStopAtLevel=1', `-Djava.io.tmpdir=${path.join(dir, 'tmp')}`, '-cp', jar, 'tlc2.TLC', '-workers', '1', '-cleanup',
      // TLC action coverage (#1248): the final statistics block is parsed by
      // formal/tla/coverage.mjs, aggregated with formal/tla/check.sh's models.
      ...(coverage ? ['-coverage', '60'] : []),
      '-continue', '-deadlock', '-lncheck', 'final', '-metadir', path.join(dir, `${module}.meta`), '-config', `${module}.cfg`, `${module}.tla`],
    { cwd: dir })
    let log = ''
    p.stdout.on('data', (d) => { log += d })
    p.stderr.on('data', (d) => { log += d })
    p.on('close', (code) => {
      fs.rmSync(dir, { recursive: true, force: true })
      resolve({ code, log })
    })
  })
}

// The nano spec's name for an invariant the Zeebe reference names differently
// (the only renamed one): verdicts are compared in the Zeebe vocabulary.
const TO_ZEEBE_NAME = { JoinBookkeepingCoherent: 'TakenBookkeepingCoherent' }

/**
 * Compare one graph's nano-spec and Zeebe-spec verdicts. `RefinesZeebe` is the
 * nano spec's direct refinement obligation; the rest must match by name.
 */
export function compareVerdicts (mc, zmc) {
  const props = (v) => (v.startsWith('violates:') ? v.slice('violates:'.length).split(',') : [])
  const refines = props(mc).includes('RefinesZeebe')
  const mcZ = props(mc).filter((p) => p !== 'RefinesZeebe').map((p) => TO_ZEEBE_NAME[p] ?? p).sort().join(',')
  const z = props(zmc).sort().join(',')
  const out = []
  if (refines) out.push({ kind: 'tlc-refines', signature: 'tlc-refines:RefinesZeebe', detail: { mc, zmc } })
  if (mcZ !== z) out.push({ kind: 'tlc-verdict', signature: `tlc-verdict:nano=${mcZ || 'pass'};zeebe=${z || 'pass'}`, detail: { mc, zmc } })
  return out
}

export async function pool (items, limit, fn) {
  const results = new Array(items.length)
  let next = 0
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++
      results[i] = await fn(items[i], i)
    }
  }))
  return results
}

async function tlcPhase (entries, log, coverageDir = null) {
  const jar = tlaJar()
  const specs = { TokenFlow: readSpec('TokenFlow'), ZeebeTokenFlow: readSpec('ZeebeTokenFlow') }
  const work = fs.mkdtempSync(path.join(os.tmpdir(), 'nanobpm-exhaustive-'))
  const divergences = []
  let done = 0
  try {
    await pool(entries, os.availableParallelism(), async (entry) => {
      const verdict = {}
      for (const fam of ['TokenFlow', 'ZeebeTokenFlow']) {
        const module = entry.graph.families[fam].module
        const { code, log: tlcLog } = await runTlc(jar, work, module, entry.artifacts.tla[fam], cfgFor(specs[fam]), coverageDir !== null)
        if (coverageDir !== null) fs.writeFileSync(path.join(coverageDir, `${module}.log`), tlcLog)
        verdict[fam] = verdictFromLog(tlcLog, specs[fam].properties, code)
        if (verdict[fam] === 'error') {
          throw new Error(`TLC failed on ${module} (${entry.graph.key}):\n${tlcLog}`)
        }
      }
      entry.verdict = verdict
      for (const d of compareVerdicts(verdict.TokenFlow, verdict.ZeebeTokenFlow)) divergences.push({ ...d, entry })
      // Cross-check the scenario scheduler against the spec: a stuck reference
      // run is a reachable stuck state, which TLC must report.
      const stuck = routesFor(entry.graph).some((r) => exploreOrders(entry.graph, r).some((run) => !run.completed))
      const tlcStuck = /NoStuckInstance|Termination/.test(verdict.TokenFlow)
      if (stuck && !tlcStuck) {
        divergences.push({ kind: 'reference-vs-tlc', signature: 'reference-vs-tlc:stuck-but-tlc-live', detail: { verdict }, entry })
      }
      if (++done % 50 === 0) log(`  tlc ${done}/${entries.length}`)
    })
  } finally {
    fs.rmSync(work, { recursive: true, force: true })
  }
  return divergences
}

// ---------------------------------------------------------------------------
// Runtime backends

/**
 * Open the runtime backends a run asks for: nano always, Camunda for
 * `--backend camunda|both` (skip-tolerant unless `--require-camunda`). Returns
 * `{ nano, camunda, CleanupError, phasesRan, close }`; shared with the
 * differential fuzzer (fuzz.mjs, #1250) so both runners check scenarios the
 * same way.
 */
export async function openBackends (args, log) {
  const wantCamunda = args.backend === 'camunda' || args.backend === 'both'
  const { NanoBackend } = await import('../parity/runtime/nano-backend.mjs')
  const nano = await new NanoBackend().init()
  let camunda = null
  let CleanupError = null
  if (wantCamunda) {
    const { CamundaBackend, BackendCleanupError } = await import('../parity/runtime/camunda-backend.mjs')
    CleanupError = BackendCleanupError
    const address = process.env.CAMUNDA_REST_ADDRESS
    if (address) camunda = new CamundaBackend({ address, token: process.env.CAMUNDA_AUTH_TOKEN, basicAuth: process.env.CAMUNDA_BASIC_AUTH })
    if (camunda && !(await camunda.ping())) camunda = null
    if (!camunda) {
      if (args.requireCamunda) throw new Error('Camunda backend required (--require-camunda), but CAMUNDA_REST_ADDRESS is unset or unreachable')
      log('::notice::no reachable Camunda 8 (CAMUNDA_REST_ADDRESS) — SKIPPING the Camunda backend (skip-tolerant)')
    }
  }
  const close = async () => {
    await nano.close()
    if (camunda) await camunda.close()
  }
  // Interrupting a manual run (SIGINT/SIGTERM) must not leave the shared
  // Camunda cluster clock pinned at epoch zero or instances in flight: Node's
  // default signal handling exits WITHOUT running a `finally`, so install the
  // same cleanup the parity runner uses (#1246 review).
  installSignalCleanup(close)
  // The phases that ACTUALLY run — a skipped Camunda backend (unset/unreachable
  // address) means no camunda/differential case can reproduce, so the stale
  // check must not ask for one (#1246 review).
  const phasesRan = new Set(['nano', 'spec-oracle', ...(camunda ? ['camunda', 'differential'] : [])])
  return { nano, camunda, CleanupError, phasesRan, close }
}

const fieldsSig = (ms) => ms.map((m) => m.field).sort().join(',')

/**
 * Run ONE scenario of `entry` (`{ graph, artifacts }`) through the open
 * backends and return its divergences (empty when every backend agrees with
 * the reference outcome and with each other).
 */
export async function checkScenario (backends, entry, sc) {
  const { runScenario } = await import('../parity/runtime/driver.mjs')
  const { checkExpectations, diffObservations } = await import('../parity/runtime/observation.mjs')
  const { nano, camunda, CleanupError } = backends
  const divergences = []
  const scenario = { ...sc, xml: entry.artifacts.bpmn }
  let nanoObs = null
  try {
    nanoObs = await runScenario(nano, scenario)
    const check = checkExpectations(nanoObs, scenario.expect)
    if (!check.ok) {
      divergences.push({ kind: 'nano', signature: `nano:${fieldsSig(check.mismatches)}:completed=${nanoObs.completed}/${scenario.expect.completed}`, detail: check.mismatches, entry, scenario: sc })
    }
    // The SPEC oracle (#1261): nano's observation must be a behaviour of
    // `ZeebeTokenFlow`. The scenario's `run` (attached by scenariosFor) is
    // by construction a spec behaviour — the Reference IS an executable
    // ZeebeTokenFlow — so the observation's milestone multiset must equal
    // the run's. A mismatch is a parity defect on the token-flow surface.
    if (sc.run) {
      const spec = checkSpecOracle(entry.graph, sc.run, nanoObs)
      if (!spec.ok) {
        divergences.push({ kind: 'spec-oracle', signature: `spec-oracle:${fieldsSig(spec.mismatches)}`, detail: spec.mismatches, entry, scenario: sc })
      }
    }
  } catch (err) {
    divergences.push({ kind: 'nano', signature: `nano:error:${errorClass(err)}`, detail: String(err.message ?? err), entry, scenario: sc })
  }
  if (camunda && backendEligible(sc, 'camunda')) {
    try {
      const obs = await runScenario(camunda, scenario)
      const check = checkExpectations(obs, { completed: scenario.expect.completed, variables: scenario.expect.variables })
      if (!check.ok) divergences.push({ kind: 'camunda', signature: `camunda:${fieldsSig(check.mismatches)}`, detail: check.mismatches, entry, scenario: sc })
      if (nanoObs) {
        const diff = diffObservations(nanoObs, obs, nano.provides, camunda.provides)
        if (!diff.ok) divergences.push({ kind: 'differential', signature: `differential:${fieldsSig(diff.mismatches)}`, detail: diff.mismatches, entry, scenario: sc })
      }
    } catch (err) {
      // A backend CLEANUP failure (reset()'s cancellation/clock reset) is
      // not a scenario divergence: isolation was not restored, so every
      // later scenario's observation is untrustworthy. Abort the phase
      // rather than record a misleading `camunda:error` and continue
      // (#1246 review).
      if (CleanupError && err instanceof CleanupError) throw err
      divergences.push({ kind: 'camunda', signature: `camunda:error:${errorClass(err)}`, detail: String(err.message ?? err), entry, scenario: sc })
    }
  }
  return divergences
}

async function runtimePhase (entries, args, log) {
  const backends = await openBackends(args, log)
  const divergences = []
  let ran = 0
  try {
    for (const entry of entries) {
      for (const sc of entry.artifacts.scenarios) {
        divergences.push(...await checkScenario(backends, entry, sc))
        if (++ran % 500 === 0) log(`  runtime ${ran} scenarios`)
      }
    }
  } finally {
    await backends.close()
  }
  log(`  runtime: ${ran} scenario(s) on nano${backends.camunda ? ' + camunda' : ''}`)
  return { divergences, phasesRan: backends.phasesRan }
}

// A stable class for an error message: its first line with ids and numbers
// masked, so one defect reached from many graphs is one signature.
export function errorClass (err) {
  return String(err?.message ?? err).split('\n')[0]
    .replace(/'[^']*'/g, "'_'").replace(/"[^"]*"/g, '"_"').replace(/\d+/g, 'N').slice(0, 120)
}

// ---------------------------------------------------------------------------
// Shrinking and the committed divergence corpus

/**
 * Shrink divergences to one minimal case per signature: the first graph in
 * enumeration (size) order, then the fewest-steps scenario of that graph.
 */
export function shrink (divergences, order) {
  const best = new Map()
  for (const d of divergences) {
    const cur = best.get(d.signature)
    const rank = order.get(d.entry.graph.key)
    const steps = d.scenario ? d.scenario.steps.length : 0
    if (!cur || rank < cur.rank || (rank === cur.rank && steps < cur.steps)) best.set(d.signature, { d, rank, steps })
  }
  return [...best.values()].sort((a, b) => a.rank - b.rank).map((b) => b.d)
}

export function caseFor (d) {
  const { id, key, start, nodes, edges } = d.entry.graph
  return {
    signature: d.signature,
    kind: d.kind,
    issue: null,
    graph: { id, key, start, nodes, edges },
    ...(d.scenario ? { scenario: d.scenario } : {}),
    ...(d.entry.verdict ? { verdict: d.entry.verdict } : {}),
    detail: d.detail
  }
}

export function loadDivergences (file = DIVERGENCES_PATH) {
  return JSON.parse(fs.readFileSync(file, 'utf8')).divergences
}

/**
 * Classify a run's (shrunk) divergences against the committed corpus. Returns
 * `{ known, fresh, stale, unfiled, obsolete }`: `fresh` divergence classes not
 * committed; `stale` committed cases inside this run's scope that no longer
 * reproduce; `unfiled` committed cases with no issue. Any non-empty list fails
 * the run.
 *
 * A committed case is `known` only when ITS OWN graph/scenario reproduced with
 * the same signature this run — not merely when some OTHER graph produced the
 * signature. Field-based signatures can group unrelated defects, so keying
 * `known` on the signature alone would let a committed reproducer whose own
 * case stopped failing pass as "known" while a different graph still trips the
 * class — the committed reproduction no longer reproduces, and the run must
 * say so (`obsolete`) rather than ratchet on a dead case (#1246 review).
 */
export function classify (shrunk, committed, inScope, divergences = shrunk) {
  const bySig = new Map(committed.map((c) => [c.signature, c]))
  // `seen`/`reproduced` validate a committed case against the FULL divergence
  // set, not the shrunk minimal cases: `shrink` keeps ONE case per signature, so
  // a committed reproducer discarded by shrinking (a smaller graph or shorter
  // scenario now wins the same signature) would otherwise read as "did not
  // reproduce" and be misreported `obsolete` even though it still fails (#1246).
  const seen = new Set(divergences.map((d) => d.signature))
  // The exact reproducer key: signature + graph + scenario (a TLC divergence
  // has no scenario). Two cases share a signature only when the same defect
  // shows on both; a committed case is validated against ITS OWN reproducer.
  // The signature is part of the key so two committed cases that share a graph
  // and scenario but fail with DIFFERENT signatures cannot cross-validate:
  // `(A, g1, s1)` failing only with known signature B while another graph still
  // produces A must not let A's committed reproducer pass on B's failure — each
  // committed case must reproduce its own failure CLASS (#1246 review).
  const reproKey = (signature, graphKey, scenarioName) => `${signature}${graphKey}${scenarioName ?? ''}`
  const seenRepro = new Set(divergences.map((d) => reproKey(d.signature, d.entry.graph.key, d.scenario?.name)))
  const reproduced = (c) => seenRepro.has(reproKey(c.signature, c.graph.key, c.scenario?.name))
  return {
    known: shrunk.filter((d) => bySig.has(d.signature)),
    fresh: shrunk.filter((d) => !bySig.has(d.signature)),
    stale: committed.filter((c) => inScope(c) && !seen.has(c.signature)),
    obsolete: committed.filter((c) => inScope(c) && seen.has(c.signature) && !reproduced(c)),
    unfiled: committed.filter((c) => !c.issue)
  }
}

// ---------------------------------------------------------------------------

async function main () {
  const args = parseArgs(process.argv.slice(2))
  const bound = resolveBound(args)
  const log = (m) => console.log(m)
  if (!args.tlc && !args.backend && !args.out) throw new Error('nothing to do: pass --tlc and/or --backend, or --out to emit artifacts')
  const t0 = Date.now()
  const entries = enumerateGraphs(bound).map(({ skel, graph }) => ({ skel, graph, artifacts: artifactsFor(graph) }))
  const scenarios = entries.reduce((s, e) => s + e.artifacts.scenarios.length, 0)
  log(`bound: ${bound.gateways} gateway(s) [${bound.kinds.join(', ')}], ${bound.flows} flow(s): ` +
    `${entries.length} graph(s), ${scenarios} scenario(s)`)

  if (args.out) {
    // The export is a RUNTIME corpus: run.mjs's discoverScenarios only reads
    // `DIR/*/scenario.json`, so each scenario lands directly under the output
    // root (named by the scenario, which already carries the graph id), its
    // `bpmn` pointing at the graph's process.bpmn next to it. The graph's
    // graph.json / *.tla sit in a per-graph `graphs/<id>/` subdir (not a
    // scenario dir, so discovery ignores them).
    for (const e of entries) {
      const gdir = path.join(args.out, 'graphs', e.graph.id)
      fs.mkdirSync(gdir, { recursive: true })
      fs.writeFileSync(path.join(gdir, 'graph.json'), JSON.stringify(e.graph, null, 2) + '\n')
      fs.writeFileSync(path.join(gdir, 'process.bpmn'), e.artifacts.bpmn)
      for (const [fam, tla] of Object.entries(e.artifacts.tla)) fs.writeFileSync(path.join(gdir, `${e.graph.families[fam].module}.tla`), tla)
      for (const sc of e.artifacts.scenarios) {
        const sdir = path.join(args.out, sc.name)
        fs.mkdirSync(sdir, { recursive: true })
        // Copy the BPMN next to the scenario so `bpmn: process.bpmn` resolves
        // inside the scenario dir, exactly as the runner's loadScenario reads.
        // Strip the in-memory `run` link (#1261 spec oracle): it is not part of
        // the serialised scenario contract and holds a Reference class instance.
        fs.writeFileSync(path.join(sdir, 'process.bpmn'), e.artifacts.bpmn)
        const { run: _run, ...serialisable } = sc
        fs.writeFileSync(path.join(sdir, 'scenario.json'), JSON.stringify({ ...serialisable, bpmn: 'process.bpmn' }, null, 2) + '\n')
      }
    }
    log(`wrote ${entries.length} graph(s) to ${args.out}`)
  }

  const divergences = []
  // The phases that ACTUALLY run drive the stale check's scope: a phase that
  // did not run (no --tlc, no --backend, or a Camunda backend skipped as
  // unreachable) cannot reproduce its committed cases, so it must not ask for
  // them (#1246 review).
  const phasesRan = new Set()
  if (args.tlc) {
    log('tlc: nano spec (MC*) vs Zeebe reference (ZMC*) per graph')
    if (args.coverageDir) fs.mkdirSync(args.coverageDir, { recursive: true })
    divergences.push(...await tlcPhase(entries, log, args.coverageDir))
    for (const k of ['tlc-refines', 'tlc-verdict', 'reference-vs-tlc']) phasesRan.add(k)
  }
  if (args.backend) {
    log(`runtime: ${args.backend}`)
    const { divergences: d, phasesRan: rp } = await runtimePhase(entries, args, log)
    divergences.push(...d)
    for (const k of rp) phasesRan.add(k)
  }
  if (!args.tlc && !args.backend) return

  const order = new Map(entries.map((e, i) => [e.graph.key, i]))
  const shrunk = shrink(divergences, order)
  const committed = loadDivergences()
  // A committed case is in this run's scope when its graph is enumerated here
  // and its kind belongs to a phase that ACTUALLY ran.
  const result = classify(shrunk, committed, (c) => order.has(c.graph.key) && phasesRan.has(c.kind), divergences)

  for (const d of result.known) {
    const c = committed.find((x) => x.signature === d.signature)
    log(`known ${d.signature} (${c.issue}) — minimal case ${c.graph.id}`)
  }
  for (const d of result.fresh) {
    console.error(`FAIL divergence ${d.signature}\n  minimal graph ${d.entry.graph.id} (${d.entry.graph.key})` +
      (d.scenario ? `\n  scenario ${d.scenario.name}: ${JSON.stringify(d.scenario.steps)}` : '') +
      `\n  ${JSON.stringify(d.detail)}`)
  }
  for (const c of result.stale) console.error(`FAIL stale committed divergence ${c.signature} (${c.graph.id}): no longer reproduces — delete it from formal/corpus/divergences.json`)
  for (const c of result.obsolete) console.error(`FAIL obsolete committed divergence ${c.signature} (${c.graph.id}): its own graph/scenario did not reproduce this run (another graph still trips the signature) — re-record the minimal case or delete it from formal/corpus/divergences.json`)
  for (const c of result.unfiled) console.error(`FAIL committed divergence ${c.signature} (${c.graph.id}) has no filed issue — file one and set "issue"`)

  if (args.record && result.fresh.length > 0) {
    const file = JSON.parse(fs.readFileSync(DIVERGENCES_PATH, 'utf8'))
    file.divergences.push(...result.fresh.map(caseFor))
    fs.writeFileSync(DIVERGENCES_PATH, JSON.stringify(file, null, 2) + '\n')
    console.error(`recorded ${result.fresh.length} minimal case(s) in formal/corpus/divergences.json — file an issue for each and set "issue"`)
  }

  const failed = result.fresh.length + result.stale.length + result.obsolete.length + result.unfiled.length
  log(`${entries.length} graph(s) in ${Math.round((Date.now() - t0) / 1000)}s: ${divergences.length} divergence(s) in ${shrunk.length} class(es), ${result.known.length} known`)
  if (failed > 0) process.exit(1)
  log('ok    no unexplained divergence within the bound')
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((err) => { console.error(err); process.exit(1) })
}
