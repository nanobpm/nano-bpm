#!/usr/bin/env node
// Generative differential sweep against Zeebe (#1521): generate -> capture on
// the pinned Zeebe -> replay on nano -> shrink every divergence -> emit a
// ready-to-commit scenario + golden (and optionally file it).
//
//   node formal/traces/fuzz/sweep.mjs --seeds 1000..1063 [--work <dir>]
//        [--no-shrink] [--emit] [--file]
//
//   --seeds A..B | A,B,C   the seeds to run (inclusive range or list)
//   --work <dir>           scratch traces root (default: a fresh temp dir)
//   --no-shrink            report divergences without minimising them
//   --emit                 write each minimised divergence into the committed
//                          corpus as scenarios/fuzz-<seed>/ + goldens/fuzz-<seed>.*
//   --file                 file a GitHub issue per NEW divergence signature
//                          (`gh`; an open issue carrying the signature gets a
//                          comment instead, so a nightly sweep never spams)
//
// Needs exactly what formal/traces/capture.sh needs (JDK 21 in
// ZEEBE_TRACES_JAVA_HOME, the cached pinned camunda build) plus cargo. Exits 1
// when any case diverges (or the generator produced a case Zeebe rejects —
// a generator defect), 0 when every case matches.
import { spawnSync } from 'node:child_process'
import crypto from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import {
  TRACES_DIR, children, generateTree, renderCase, writeCase
} from './generate.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const repo = path.join(here, '..', '..', '..')

export function parseSeeds (spec) {
  const m = /^(\d+)\.\.(\d+)$/.exec(spec)
  if (m) {
    const [a, b] = [Number(m[1]), Number(m[2])]
    if (b < a) throw new Error(`empty seed range ${spec}`)
    return Array.from({ length: b - a + 1 }, (_, i) => a + i)
  }
  return spec.split(',').map((s) => {
    if (!/^\d+$/.test(s)) throw new Error(`bad seed ${s}`)
    return Number(s)
  })
}

// ───────────────────────────── shrinking ──────────────────────────────

/** Size of a tree: blocks + MI items (the shrinker's progress measure). */
export function size (tree) {
  let n = 0
  const visit = (seq) => {
    for (const b of seq) {
      n++
      if (b.kind === 'mi') n += tree.initVars[b.items].length
      if (b.boundary) n++
      if (b.inputs || b.outputs) n++
      children(b).forEach(visit)
    }
  }
  visit(tree.body)
  return n
}

/** Every one-step reduction of a tree: drop a block from a sequence of >= 2,
 *  hoist a composite block's child sequence in its place, or drop an MI item. */
export function reductions (tree) {
  const out = []
  const seqs = [] // [owner path to a seq array]
  const collect = (seq) => {
    seqs.push(seq)
    for (const b of seq) children(b).forEach(collect)
  }
  collect(tree.body)
  seqs.forEach((seq, si) => {
    seq.forEach((b, bi) => {
      const edit = (f) => {
        const t = structuredClone(tree)
        const ts = []
        const c2 = (s) => { ts.push(s); for (const x of s) children(x).forEach(c2) }
        c2(t.body)
        f(t, ts[si], ts[si][bi])
        out.push(t)
      }
      if (seq.length >= 2) edit((t, s) => s.splice(bi, 1))
      for (let ci = 0; ci < children(b).length; ci++) {
        edit((t, s, blk) => s.splice(bi, 1, ...children(blk)[ci]))
      }
      if (b.kind === 'mi' && tree.initVars[b.items].length > 1) {
        edit((t, s, blk) => { t.initVars[blk.items] = t.initVars[blk.items].slice(0, -1) })
      }
      if (b.kind === 'task' && b.boundary) edit((t, s, blk) => { delete blk.boundary })
      if (b.kind === 'task' && (b.inputs || b.outputs)) {
        edit((t, s, blk) => { delete blk.inputs; delete blk.outputs })
      }
    })
  })
  return out
}

// ───────────────────────────── the two engines ──────────────────────────────

function run (cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { encoding: 'utf8', maxBuffer: 1 << 28, ...opts })
  return { code: r.status ?? 1, out: (r.stdout ?? '') + (r.stderr ?? '') }
}

/** A fresh traces root with the committed normaliser and the given cases. */
function stage (root, cases) {
  fs.rmSync(root, { recursive: true, force: true })
  fs.mkdirSync(path.join(root, 'scenarios'), { recursive: true })
  fs.copyFileSync(path.join(TRACES_DIR, 'normaliser.json'), path.join(root, 'normaliser.json'))
  for (const [name, c] of cases) writeCase(path.join(root, 'scenarios'), name, c)
}

function capture (root, log) {
  const r = run(path.join(TRACES_DIR, 'capture.sh'), [], {
    env: { ...process.env, NANO_TRACES_CAPTURE_DIR: root }
  })
  fs.writeFileSync(log, r.out)
  if (r.code !== 0) {
    const last = [...r.out.matchAll(/^capture (\S+)$/gm)].pop()
    throw new Error(`Zeebe capture failed${last ? ` on ${last[1]}` : ''} (a generated case Zeebe rejects is a generator defect) — see ${log}`)
  }
}

function nano (root, log) {
  const outcomes = path.join(root, 'outcomes.json')
  const r = run('cargo', ['test', '--features', 'serde', '--test', 'zeebe_traces', '--',
    '--exact', 'fuzz_sweep_outcomes'], {
    cwd: path.join(repo, 'engine-core'),
    env: { ...process.env, NANO_TRACES_DIR: root, NANO_TRACES_OUTCOMES: outcomes }
  })
  fs.writeFileSync(log, r.out)
  if (r.code !== 0) throw new Error(`nano runner failed — see ${log}`)
  return JSON.parse(fs.readFileSync(outcomes, 'utf8'))
}

/** Run cases through both engines: `name -> null | {step, diff}`. */
export function differential (root, cases) {
  stage(root, cases)
  capture(root, path.join(root, 'capture.log'))
  return nano(root, path.join(root, 'nano.log'))
}

/** Greedy minimisation: cut the script at the first diverging step, then
 *  apply tree reductions while the case still diverges WITH THE ORIGINAL
 *  SIGNATURE — a reduction that trades the defect for a different (possibly
 *  already-filed) one is rejected, so `--file` always reports the defect the
 *  seed actually found. Each round captures every candidate in ONE Zeebe run.
 *  `run` is the differential oracle (injectable for tests). */
export function shrink (work, tree, verdict, run = differential) {
  const target = signature(verdict.diff)
  let best = { tree, steps: verdict.step === null ? undefined : verdict.step + 1, verdict }
  for (let round = 0; ; round++) {
    const cands = reductions(best.tree).map((t) => ({ tree: t }))
    if (cands.length === 0) return best
    const cases = cands.map((c, i) => [`r${round}-${i}`, renderCase(c.tree, best.steps ? { maxSteps: best.steps } : {})])
    const res = run(path.join(work, `shrink-${tree.seed}-${round}`), cases)
    const hits = cands
      .map((c, i) => ({ ...c, verdict: res[`r${round}-${i}`] }))
      .filter((c) => c.verdict && signature(c.verdict.diff) === target)
      .sort((a, b) => size(a.tree) - size(b.tree))
    if (hits.length === 0) return best
    const h = hits[0]
    best = { tree: h.tree, steps: h.verdict.step === null ? best.steps : Math.min(best.steps ?? Infinity, h.verdict.step + 1), verdict: h.verdict }
  }
}

/** A stable fingerprint of a divergence class: the diverging step's op and
 *  the KINDS (valueType:intent:elementType) of the records only one engine
 *  wrote at that step — runtime values, keys and ids dropped, so the same
 *  defect found from different seeds/shapes maps to one signature (one issue). */
export function signature (diff) {
  const m = /zeebe: (.*)\n\s*nano:\s+(.*)/.exec(diff)
  if (!m) {
    // Not a record-level trace diff (e.g. `nano could not replay the script:
    // …`): fingerprint a NORMALISED failure category — digits, quoted text and
    // hex keys dropped — never the empty-trace `|order` default, so distinct
    // failure causes never collapse onto one (already-filed) signature.
    const category = String(diff).split('\n')[0]
      .replace(/"[^"]*"|'[^']*'|`[^`]*`/g, '_')
      .replace(/\b[0-9a-f]{8,}\b|\d+/gi, '#')
      .replace(/\s+/g, ' ').trim()
    return crypto.createHash('sha256').update(`failure|${category}`).digest('hex').slice(0, 12)
  }
  const [, z, n] = m
  const parse = (line) => { try { return JSON.parse(line) } catch { return {} } }
  const zs = parse(z)
  const ns = parse(n)
  const recs = (s) => (s.records ?? []).map((r) => JSON.stringify(r))
  const [zr, nr] = [recs(zs), recs(ns)]
  const kind = (side) => (j) => {
    const r = JSON.parse(j)
    return `${side}:${r.valueType}:${r.intent}:${r.bpmnElementType ?? r.rejectionType ?? ''}`
  }
  const only = [
    ...zr.filter((x) => !nr.includes(x)).map(kind('zeebe')),
    ...nr.filter((x) => !zr.includes(x)).map(kind('nano'))
  ]
  const shape = only.length ? [...new Set(only)].sort().join(',') : 'order'
  return crypto.createHash('sha256').update(`${zs.op ?? ''}|${shape}`).digest('hex').slice(0, 12)
}

function emit (root, name, seedName) {
  for (const f of ['process.bpmn', 'script.json']) {
    fs.mkdirSync(path.join(TRACES_DIR, 'scenarios', name), { recursive: true })
    fs.copyFileSync(path.join(root, 'scenarios', seedName, f), path.join(TRACES_DIR, 'scenarios', name, f))
  }
  for (const f of ['trace.json', 'cells.json']) {
    const g = JSON.parse(fs.readFileSync(path.join(root, 'goldens', `${seedName}.${f}`), 'utf8'))
    g.scenario = name
    fs.writeFileSync(path.join(TRACES_DIR, 'goldens', `${name}.${f}`), JSON.stringify(g) + '\n')
  }
}

function fileIssue (seed, sig, diff, scriptPath) {
  const marker = `fuzz-signature:${sig}`
  const found = run('gh', ['issue', 'list', '--state', 'open', '--search', marker, '--json', 'number', '--jq', '.[0].number'])
  const body = [
    `The generative Zeebe differential sweep (#1521) found a divergence: seed \`${seed}\`.`,
    '',
    'Reproduce: `node formal/traces/fuzz/sweep.mjs --seeds ' + seed + '` (see formal/README.md).',
    '',
    'First diverging step (minimised):',
    '```',
    diff,
    '```',
    '',
    'Minimised script:',
    '```json',
    fs.readFileSync(scriptPath, 'utf8'),
    '```',
    '',
    `<!-- ${marker} -->`
  ].join('\n')
  if (found.code === 0 && found.out.trim()) {
    run('gh', ['issue', 'comment', found.out.trim(), '--body', body])
    return { known: true, ref: `#${found.out.trim()} (commented)` }
  }
  const r = run('gh', ['issue', 'create', '--title', `fuzz: Zeebe divergence (signature ${sig}, seed ${seed})`, '--body', body])
  if (r.code !== 0) throw new Error(`gh issue create failed: ${r.out}`)
  return { known: false, ref: r.out.trim() }
}

function main (argv) {
  let seeds = []
  let work = null
  let doShrink = true
  let doEmit = false
  let doFile = false
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a === '--seeds') seeds = parseSeeds(argv[++i])
    else if (a === '--work') work = argv[++i]
    else if (a === '--no-shrink') doShrink = false
    else if (a === '--emit') doEmit = true
    else if (a === '--file') doFile = true
    else throw new Error(`unknown argument ${a}`)
  }
  if (seeds.length === 0) throw new Error('--seeds is required')
  work ??= fs.mkdtempSync(path.join(os.tmpdir(), 'nano-fuzz-'))
  console.log(`sweep: ${seeds.length} seeds, work dir ${work}`)
  const trees = new Map(seeds.map((s) => [s, generateTree(s)]))
  const verdicts = differential(path.join(work, 'sweep'), seeds.map((s) => [`g-${s}`, renderCase(trees.get(s))]))
  let diverged = 0
  let fresh = 0
  for (const seed of seeds) {
    const v = verdicts[`g-${seed}`]
    if (!v) continue
    diverged++
    console.log(`\nseed ${seed}: DIVERGES\n${v.diff}`)
    // --no-shrink alone is a report-only shortcut; with --emit/--file the
    // UNSHRUNK tree still goes through emission and filing.
    if (!doShrink && !doEmit && !doFile) continue
    const best = doShrink ? shrink(work, trees.get(seed), v) : { tree: trees.get(seed), steps: undefined, verdict: v }
    const name = `fuzz-${seed}`
    const root = path.join(work, `min-${seed}`)
    const c = renderCase(best.tree, best.steps ? { maxSteps: best.steps } : {})
    c.script.generator.tree = best.tree
    c.script.generator.maxSteps = best.steps ?? null
    const res = differential(root, [[name, c]])
    const sig = signature(res[name]?.diff ?? v.diff)
    console.log(`seed ${seed}: minimised to ${c.script.steps.length} steps (signature ${sig})\n${res[name]?.diff ?? '(minimised case no longer diverges — keeping the unshrunk report)'}`)
    if (doEmit) {
      emit(root, name, name)
      console.log(`seed ${seed}: emitted scenarios/${name} + goldens/${name}.* — add an allowlist.json entry with the issue`)
    }
    if (doFile) {
      const f = fileIssue(seed, sig, res[name]?.diff ?? v.diff, path.join(root, 'scenarios', name, 'script.json'))
      console.log(`seed ${seed}: ${f.known ? 'known divergence' : 'NEW divergence filed'} ${f.ref}`)
      if (!f.known) fresh++
    }
  }
  console.log(`\nsweep: ${diverged}/${seeds.length} seeds diverge${doFile ? `, ${fresh} new signature(s) filed` : ''}`)
  // With --file a divergence whose signature already has an OPEN issue is a
  // tracked defect (commented with the new seed), so the run fails only on a
  // NEW class; without --file every divergence fails the run.
  process.exit((doFile ? fresh : diverged) ? 1 : 0)
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main(process.argv.slice(2))
}
