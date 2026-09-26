#!/usr/bin/env node
// Parse TLC `-tool` witness output into a committed trace-validation fixture
// (#1226, Deliverable B). Reads the full TLC tool-mode output on stdin and
// writes a JSON fixture on stdout describing:
//   - the process graph (nodes + kinds + flows), evaluated by TLC itself via an
//     `ASSUME PrintT(<<"GRAPHJSON", …>>)` in the generated witness module (so we
//     never hand-parse the model's TLA+ CASE expressions), and
//   - the ordered observable milestones of the shortest completing behaviour
//     (the counterexample to the witness invariant `~completed`), derived from
//     the per-state deltas of `pending` (flow taken), `waiting` (task
//     activated), `fireCount` (join fired) and `completed` (instance completed).
//
// The fixture is spec-agnostic in shape, but the milestone *extraction* above
// reads a fixed observable state vocabulary (`pending`/`waiting`/`fireCount`/
// `completed`) that is currently the TokenFlow family's; a sibling spec with a
// different state vocabulary must extend `milestones()` (the guard
// `assertObservableVocabulary` fails loudly rather than emit a sparse trace).
// The Rust harness (engine-core/tests/trace_validation) replays the fixture
// against the real engine and asserts the observable milestone multiset equals
// the spec's.
//
// Usage: gen-traces.sh runs TLC and pipes its output here:
//   parse.mjs --spec TokenFlow --model MCParallelDiamond < tlc.out > fixture.json

import fs from 'node:fs'

function argv (flag) {
  const i = process.argv.indexOf(flag)
  return i >= 0 ? process.argv[i + 1] : undefined
}

// --- A small recursive-descent parser for the subset of TLA+ value syntax TLC
// prints: numbers, strings, TRUE/FALSE, sets {…}, tuples <<…>>, records
// [k |-> v, …] and functions (k :> v @@ …). Whitespace/newlines are
// insignificant between tokens.
export function parseTlaValue (text) {
  let i = 0
  const s = text
  const ws = () => { while (i < s.length && /\s/.test(s[i])) i++ }
  const eat = (tok) => {
    ws()
    if (s.startsWith(tok, i)) { i += tok.length; return true }
    return false
  }
  const expect = (tok) => {
    if (!eat(tok)) throw new Error(`expected '${tok}' at ${i}: ${s.slice(i, i + 40)}`)
  }
  function value () {
    ws()
    const c = s[i]
    if (c === '"') return string()
    if (c === '{') return set()
    if (c === '[') return record()
    if (c === '(') return func()
    if (s.startsWith('<<', i)) return tuple()
    if (s.startsWith('TRUE', i)) { i += 4; return true }
    if (s.startsWith('FALSE', i)) { i += 5; return false }
    return number()
  }
  function string () {
    expect('"')
    let out = ''
    while (i < s.length && s[i] !== '"') { out += s[i]; i++ }
    expect('"')
    return out
  }
  function number () {
    ws()
    let j = i
    if (s[j] === '-') j++
    while (j < s.length && /[0-9]/.test(s[j])) j++
    if (j === i) throw new Error(`expected number at ${i}: ${s.slice(i, i + 40)}`)
    const n = Number(s.slice(i, j)); i = j
    return n
  }
  function set () {
    expect('{')
    const out = []
    ws()
    if (eat('}')) return out
    for (;;) { out.push(value()); ws(); if (eat(',')) continue; expect('}'); break }
    return out
  }
  function tuple () {
    expect('<<')
    const out = []
    ws()
    if (eat('>>')) return out
    for (;;) { out.push(value()); ws(); if (eat(',')) continue; expect('>>'); break }
    return out
  }
  function record () {
    expect('[')
    const out = {}
    for (;;) {
      ws()
      // TLC renders a finite function's key as a bare identifier (`[k |-> v]`)
      // when it is alphanumeric, but quotes a string-domain key (`["S" |-> v]`),
      // e.g. the `kind`/`edges`/state maps built from `MCNodes`/`MCFlows`. Accept
      // both forms so a quoted key is not misparsed as an empty key.
      let key
      if (s[i] === '"') {
        key = string()
      } else {
        let j = i
        while (j < s.length && /[A-Za-z0-9_]/.test(s[j])) j++
        key = s.slice(i, j); i = j
      }
      expect('|->')
      out[key] = value()
      ws()
      if (eat(',')) continue
      expect(']'); break
    }
    return out
  }
  function func () {
    expect('(')
    const out = {}
    for (;;) {
      const key = value() // typically a quoted string domain element
      expect(':>')
      out[String(key)] = value()
      ws()
      if (eat('@@')) continue
      expect(')'); break
    }
    return out
  }
  const v = value()
  return v
}

// Extract the payloads of every `@!@!@STARTMSG <code>:<sev> @!@!@ … ENDMSG`
// block from TLC tool-mode output.
function toolMessages (out) {
  const msgs = []
  const re = /@!@!@STARTMSG (\d+):\d+ @!@!@\n([\s\S]*?)\n@!@!@ENDMSG \1 @!@!@/g
  let m
  while ((m = re.exec(out))) msgs.push({ code: Number(m[1]), body: m[2] })
  return msgs
}

function extractGraph (out) {
  // The `ASSUME PrintT(<<"GRAPHJSON", rec>>)` value is printed as a tuple. TLC
  // prints it outside the tool STARTMSG framing, so scan the raw text.
  const idx = out.indexOf('"GRAPHJSON"')
  if (idx < 0) throw new Error('no GRAPHJSON marker in TLC output')
  const open = out.lastIndexOf('<<', idx)
  if (open < 0) throw new Error('malformed GRAPHJSON tuple')
  const tup = parseTlaValue(out.slice(open))
  const rec = tup[1]
  const flows = Object.entries(rec.edges).map(([id, pair]) => ({ id, from: pair[0], to: pair[1] }))
  flows.sort((a, b) => a.id.localeCompare(b.id))
  const nodes = {}
  for (const n of Object.keys(rec.kind).sort()) nodes[n] = rec.kind[n]
  return { start: rec.start, nodes, flows }
}

// The counterexample states, in order. Each state message begins with
// "<n>: <action>" and is followed by the `/\ var = value` conjuncts.
function extractStates (out) {
  const states = []
  for (const { body } of toolMessages(out)) {
    const m = /^(\d+): (.*)$/.exec(body.split('\n')[0])
    if (!m) continue
    const rest = body.slice(body.indexOf('\n') + 1)
    const vars = {}
    // Split on lines starting with "/\ " at column 0 (top-level conjuncts).
    const parts = rest.split(/\n(?=\/\\ )/)
    for (let part of parts) {
      part = part.replace(/^\/\\ /, '')
      const eq = part.indexOf(' = ')
      if (eq < 0) continue
      const name = part.slice(0, eq).trim()
      const val = part.slice(eq + 3)
      try { vars[name] = parseTlaValue(val) } catch { /* ignore non-value conjuncts */ }
    }
    states.push({ step: Number(m[1]), action: m[2], vars })
  }
  return states
}

function num (fn, key) { return (fn && key in fn) ? fn[key] : 0 }

// The observable state vocabulary this extractor projects onto milestones. It is
// currently the TokenFlow family's: `pending` (flow taken), `waiting` (task
// activated), `fireCount` (join fired) and `completed` (instance completed). A
// sibling spec whose TLA state uses different variable names would otherwise
// yield a silently sparse/empty milestone trace here while the fixture still
// looks well-formed. `assertObservableVocabulary` turns that silent drift into a
// loud failure; a genuinely different sibling family must extend `milestones()`
// with its own mapping (future work, #1227/#1240) rather than reuse this one.
const REQUIRED_STATE_VARS = ['pending', 'waiting', 'fireCount', 'completed']

function assertObservableVocabulary (spec, model, states) {
  if (!states.length) {
    process.stderr.write(
      `error: ${spec}/${model}: no witness states were parsed from the TLC output; ` +
      `cannot extract a milestone trace.\n`)
    process.exit(1)
  }
  // Require the observable vocabulary in *every* witnessed state, not merely the
  // union across states. `extractStates` deliberately drops any conjunct whose
  // value fails to parse, so a union check would let a single malformed/incomplete
  // state slip through as long as some other state defined the same variable;
  // `milestones()` would then default that transition's missing map to `{}` and
  // silently omit (or miscount) observations. A per-state check turns any parse
  // gap — or a sibling spec's divergent state vocabulary — into a loud failure.
  for (const s of states) {
    const missing = REQUIRED_STATE_VARS.filter((v) => !(v in s.vars))
    if (missing.length) {
      process.stderr.write(
        `error: ${spec}/${model}: state ${s.step} (${s.action}) does not define the ` +
        `TokenFlow-family observable variables [${missing.join(', ')}]. This means TLC ` +
        `printed a value parse.mjs could not parse (so extractStates dropped it), or a ` +
        `sibling spec uses a different state vocabulary and must extend parse.mjs ` +
        `milestones() with its own mapping (see formal/README.md, #1227/#1240) rather ` +
        `than silently emitting a sparse trace.\n`)
      process.exit(1)
    }
  }
}

// Milestones from the per-state deltas, spec vocabulary -> observable events.
function milestones (graph, states) {
  const out = []
  const flowOf = {}
  for (const f of graph.flows) flowOf[f.id] = f
  for (let k = 1; k < states.length; k++) {
    const a = states[k - 1].vars
    const b = states[k].vars
    // Flow taken: pending[g] increased (skip the synthetic create pseudo-flow).
    for (const g of Object.keys(b.pending || {})) {
      if (g === '<create>' || !flowOf[g]) continue
      const d = num(b.pending, g) - num(a.pending, g)
      for (let x = 0; x < d; x++) out.push({ kind: 'flow', from: flowOf[g].from, to: flowOf[g].to })
    }
    // Task activated: waiting[n] increased.
    for (const n of Object.keys(b.waiting || {})) {
      const d = num(b.waiting, n) - num(a.waiting, n)
      for (let x = 0; x < d; x++) out.push({ kind: 'task', node: n })
    }
    // Join fired: fireCount[n] increased.
    for (const n of Object.keys(b.fireCount || {})) {
      const d = num(b.fireCount, n) - num(a.fireCount, n)
      for (let x = 0; x < d; x++) out.push({ kind: 'joinFired', node: n })
    }
    // Instance completed.
    if (a.completed === false && b.completed === true) out.push({ kind: 'completed' })
  }
  return out
}

function milestoneKey (m) {
  return [m.kind, m.from || '', m.to || '', m.node || ''].join('\u0000')
}

function main () {
  const spec = argv('--spec')
  const model = argv('--model')
  if (!spec || !model) { process.stderr.write('usage: parse.mjs --spec S --model M < tlc.out\n'); process.exit(2) }
  const out = fs.readFileSync(0, 'utf8')
  if (!/Invariant \w+ is violated/.test(out)) {
    process.stderr.write(`error: TLC did not produce a completing witness behaviour for ${model}\n`)
    process.stderr.write(out)
    process.exit(1)
  }
  const graph = extractGraph(out)
  const states = extractStates(out)
  // Guard the observable-state-vocabulary contract loudly: a sibling spec whose
  // TLA state does not use TokenFlow's variable names must extend milestones()
  // rather than silently emit a sparse trace here.
  assertObservableVocabulary(spec, model, states)
  // Sort the milestone multiset canonically: the engine replay compares
  // multisets, so a stable committed order keeps the fixture diff-free.
  const ms = milestones(graph, states).sort((x, y) => milestoneKey(x).localeCompare(milestoneKey(y)))
  const fixture = { spec, model, graph, milestones: ms }
  process.stdout.write(JSON.stringify(fixture, null, 2) + '\n')
}

// Run the CLI only when executed directly, not when imported by a test.
if (import.meta.url === `file://${process.argv[1]}`) main()
