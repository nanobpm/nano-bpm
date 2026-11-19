// Tests for the TLC action-coverage gate (#1248): the expected obligations are
// derived from the spec source, so an action TLC never mentions (its
// quantifier ranges over an empty set) is still reported as a gap.
import test from 'node:test';
import assert from 'node:assert/strict';
import { checkSpec, expectations, parseCoverage, stripComments } from './coverage.mjs';

const SPEC = `---- MODULE Toy ----
VARIABLE x
Guard == x < 3
Inc ==
    /\\ Guard
    /\\ x' = x + 1
Never(n) ==
    /\\ n > 0
    /\\ x' = 0
(* a block comment
   spanning lines *)
Next ==
    \\/ Inc
    \\/ \\E n \\in {} : Never(n)
====
`;

const header = (name, line, found, mod = 'Toy') =>
  `<${name} line ${line}, col 1 to line ${line}, col 3 of module ${mod}>: ${found}:${found}`;
const loc = (from, to, n, depth = 0, mod = 'Toy') =>
  `  ${'|'.repeat(depth)}line ${from}, col 5 to line ${to}, col 9 of module ${mod}: ${n}`;
const log = (...rows) => ['The coverage statistics at now', ...rows, 'End of statistics.'].join('\n');

test('stripComments keeps line structure', () => {
  const out = stripComments('a \\* c\n(* x\n y *) b\n');
  assert.equal(out.split('\n').length, 4);
  assert.match(out, /^a /);
  assert.match(out.split('\n')[2], /b$/);
});

test('expectations derive Next disjuncts and action lines', () => {
  const exp = expectations(SPEC);
  assert.deepEqual(exp.disjuncts, ['Inc', 'Never']);
  // Line 3 (`Guard == x < 3`) is obligated too: line obligations follow every
  // reachable definition, including the pure guard helper.
  assert.deepEqual([...exp.lines.keys()].sort((a, b) => a - b), [3, 5, 6, 8, 9]);
  // `/\\ Guard` is a bare reference: TLC reports it at Guard's definition.
  assert.deepEqual(exp.alias.get(5), [3, 3]);
});

test('an action absent from the TLC report is a gap', () => {
  const l = log(header('Inc', 4, 3), loc(3, 3, 4), loc(6, 6, 3));
  const problems = checkSpec({ name: 'Toy', src: SPEC, logs: [['M1', l]] });
  assert.ok(problems.includes('action Never never fires in any model'));
  assert.ok(problems.some((p) => p.startsWith('line 8 ')));
  assert.ok(!problems.some((p) => p.startsWith('line 5 ') || p.startsWith('line 6 ')));
});

test('coverage aggregates over models', () => {
  const a = log(header('Inc', 4, 3), loc(3, 3, 4), loc(6, 6, 3));
  const b = log(header('Never', 7, 1), loc(8, 8, 1), loc(9, 9, 1));
  assert.deepEqual(checkSpec({ name: 'Toy', src: SPEC, logs: [['A', a], ['B', b]] }), []);
});

test('a zero-count location does not cover its line', () => {
  const a = log(header('Inc', 4, 3), loc(3, 3, 4), loc(6, 6, 0));
  const problems = checkSpec({ name: 'Toy', src: SPEC, logs: [['A', a]] });
  assert.ok(problems.some((p) => p.startsWith('line 6 ')));
});

test('a leaf multi-line location covers every line it spans; a decomposed one does not', () => {
  const leaf = parseCoverage(log(header('Inc', 4, 1), loc(5, 9, 1)), 'Toy');
  assert.deepEqual([...leaf.lines].sort(), [5, 6, 7, 8, 9]);
  const parent = parseCoverage(log(header('Inc', 4, 1), loc(5, 9, 1), loc(5, 5, 1, 1)), 'Toy');
  assert.deepEqual([...parent.lines].sort(), [5]);
});

test('a log without coverage statistics is reported', () => {
  const problems = checkSpec({ name: 'Toy', src: SPEC, logs: [['A', 'Model checking completed.']] });
  assert.ok(problems.includes('A: TLC log has no coverage statistics'));
});

test('a wrapper action (no prime of its own) is followed to the actions it calls', () => {
  const src = `---- MODULE Wrap ----
VARIABLE x
Step(n) ==
    /\\ n > 0
    /\\ x' = n
Wrap(f) == \\E n \\in {f} : Step(n)
Loop == Loop2
Loop2 == Loop
Next ==
    \\/ Wrap(1)
    \\/ Loop
====
`;
  const exp = expectations(src);
  assert.deepEqual(exp.disjuncts, ['Wrap']);
  assert.deepEqual([...exp.closures.get('Wrap')].sort(), ['Step', 'Wrap']);
  assert.ok(exp.lines.has(4) && exp.lines.has(5));
});

test('a collapsed multi-line leaf covers its span — a documented gate limitation', () => {
  // #1682 review (declined): TLC omits zero-count unprimed children and
  // collapses subtrees, so a guard `\/ x > 10` followed by `\/ x < 0` can
  // surface as ONE positive leaf spanning both arms while the second arm
  // short-circuited and never ran. The gate deliberately treats the leaf's
  // span as per-line coverage: TLC's own report does not distinguish the
  // collapse, and obligating folded arms makes the gate unsatisfiable (a
  // VariableScopes CASE inside a collapsed IF leaf reports no per-arm rows
  // even when the arm DID run). The branch-level gap is instead closed by the
  // spec-side mutation suite (formal/tla/mutate.mjs), which flips guard
  // operators and requires a verdict change. This pins the leaf-spread
  // behaviour so a future tightening is a deliberate, visible choice.
  const src = `---- MODULE ShortCircuit ----
VARIABLE x
Ready ==
    \\/ x > 10
    \\/ x < 0
Inc ==
    /\\ Ready
    /\\ x' = x + 1
Next ==
    \\/ Inc
====
`;
  // TLC's report: Ready's two disjunct arms collapsed into a single positive
  // leaf (lines 4-5), with NO per-arm rows.
  const collapsed = log(header('Inc', 6, 2, 'ShortCircuit'), loc(4, 5, 2, 0, 'ShortCircuit'), loc(8, 8, 2, 0, 'ShortCircuit'));
  assert.deepEqual(checkSpec({ name: 'ShortCircuit', src, logs: [['M', collapsed]] }), []);
});

test('an inline Next disjunct (no named action) is dropped — specs must name it', () => {
  // SnapshotReplay's `\/ UNCHANGED vars` stutter: the gate cannot track an
  // anonymous disjunct, so the spec names it (`RecoverAsIs`). This pins the
  // drop so a future inline disjunct is a deliberate, visible choice.
  const src = `---- MODULE Inline ----
VARIABLE x
Inc ==
    /\\ x < 3
    /\\ x' = x + 1
Next ==
    \\/ Inc
    \\/ UNCHANGED x
====
`;
  const exp = expectations(src);
  assert.deepEqual(exp.disjuncts, ['Inc']);
  // The named form IS tracked:
  const named = src.replace('Next ==\n    \\/ Inc\n    \\/ UNCHANGED x',
    'Stutter == UNCHANGED x\nNext ==\n    \\/ Inc\n    \\/ Stutter');
  assert.deepEqual(expectations(named).disjuncts, ['Inc', 'Stutter']);
});

test('line obligations include pure guard helpers reachable from an action', () => {
  const src = `---- MODULE Guarded ----
VARIABLE x
Ready ==
    \\/ x > 10
    \\/ x < 0
Inc ==
    /\\ Ready
    /\\ x' = x + 1
Next ==
    \\/ Inc
====
`;
  const exp = expectations(src);
  // Ready's disjuncts (lines 4-5) are obligations even though Ready is not an
  // action; the firing closure stays action-only.
  assert.deepEqual([...exp.lines.keys()].sort((a, b) => a - b), [4, 5, 7, 8]);
  const l = log(header('Inc', 6, 2, 'Guarded'), loc(7, 8, 2, 0, 'Guarded'));
  const problems = checkSpec({ name: 'Guarded', src, logs: [['M', l]] });
  assert.ok(problems.some((p) => p.startsWith('line 4 ')));
  assert.ok(problems.some((p) => p.startsWith('line 5 ')));
  // `/\\ Ready` (line 7) is a bare reference: TLC attributes it to Ready's
  // definition (lines 4-5), so covering 4-5 + the primed line satisfies Inc.
  const covered = log(header('Inc', 6, 2, 'Guarded'), loc(4, 5, 1, 0, 'Guarded'), loc(8, 8, 2, 0, 'Guarded'));
  assert.deepEqual(checkSpec({ name: 'Guarded', src, logs: [['M', covered]] }), []);
});

test('trailing blank / comment-only lines never remove a one-line guard obligation', () => {
  for (const trailer of ['\n', '\n\n', '\n\\* trailing comment\n', '\n(* block *)\n']) {
    const src = `---- MODULE Spaced ----
VARIABLE x
Ready == x > 10${trailer}Inc ==
    /\\ Ready
    /\\ x' = x + 1
Next ==
    \\/ Inc
====
`;
    const exp = expectations(src);
    assert.ok(exp.lines.has(3), `Ready's line lost its obligation with trailer ${JSON.stringify(trailer)}`);
  }
});

// TLC never instruments constant-level definitions (folded at startup or
// evaluated outside the coverage counters), a variable tuple used only as an
// UNCHANGED / subscript operand, nor a one-line Next-disjunct wrapper (its
// coverage is the firing obligation). Obligating those lines made every real
// spec report false gaps (`InitFlow == "<create>"`, `vars == <<..>>`, ...).
test('constant-level defs, variable tuples and one-line disjunct wrappers carry no line obligation', () => {
  const src = `---- MODULE Lvl ----
CONSTANT N
VARIABLES x, y
Zero == 0

Bound == N + 1
Bump(p) == [p EXCEPT ![1] = @ + 1]
vars == <<x, y>>
Ready == x > Zero

Step(k) ==
    /\\ Ready
    /\\ x' = x + k
    /\\ UNCHANGED y
Wrap == \\E k \\in {1} : Step(k)

Next ==
    \\/ Wrap
    \\/ UNCHANGED vars
====
`;
  const exp = expectations(src);
  for (const n of [4, 6, 7, 8, 15]) assert.ok(!exp.lines.has(n), `line ${n} must carry no obligation`);
  assert.ok(exp.lines.has(9), 'a state-level one-line guard keeps its obligation');
  assert.ok(exp.lines.has(12) && exp.lines.has(13), 'action conjuncts keep their obligations');
});

test('a guard receiving state through a PARAMETER keeps its line obligations', () => {
  // #1682 review: `TokenFlow.PathReaches(src, j, blocked)` reads no state
  // variable itself — its state dependence arrives through the `blocked`
  // argument from `HasActivePathTo`. The gate therefore cannot obligate the
  // helper's own lines (TLC folds a parameterized operator's subexpressions
  // into the caller's instrumentation — PathReaches' lines never appear in a
  // coverage report even when the caller's guard runs), so this pins the
  // CURRENT contract: no per-line obligation on the helper, and the guard is
  // instead covered through the caller's bare-reference alias (the caller's
  // line obligation is satisfied only when the helper's span was evaluated).
  const src = `---- MODULE Param ----
VARIABLES waiting, joinTokens
Nodes == {"t", "j"}
In(n) == {}
Out(n) == {}
Tgt == [f \\in {} |-> "t"]
Succ(n, blocked) == {Tgt[g] : g \\in Out(n) \\ blocked}
PathReaches(src, j, blocked) ==
    LET R[k \\in 0..1] ==
            IF k = 0 THEN {src}
            ELSE UNION {{n} \\cup Succ(n, blocked) : n \\in R[k-1]}
    IN  j \\in R[1]
HasActivePathTo(j) ==
    \\E m \\in {n \\in Nodes : waiting[n] > 0} : PathReaches(m, j, {})
JoinFires(j) ==
    \\/ \\A g \\in {} : joinTokens[j][g] >= 1
    \\/ ~HasActivePathTo(j)
ArriveJoin(j) ==
    /\\ JoinFires(j)
    /\\ waiting' = waiting
    /\\ UNCHANGED joinTokens
Next ==
    \\/ \\E j \\in {"j"} : ArriveJoin(j)
====
`;
  const exp = expectations(src);
  const obligated = [...exp.lines.keys()].sort((a, b) => a - b);
  assert.ok(!obligated.includes(10) && !obligated.includes(11) && !obligated.includes(12),
    `PathReaches' own lines carry no obligation (TLC folds them into the caller), got ${JSON.stringify(obligated)}`);
  // The caller-side guard branches ARE obligated.
  assert.ok(obligated.includes(16) && obligated.includes(17),
    `JoinFires' disjunct arms stay obligated, got ${JSON.stringify(obligated)}`);
});

test('VARIABLE declarations spanning lines are all recognised', () => {
  const src = `---- MODULE Multi ----
VARIABLES
    a,   \\* first
    b
Ready == b > 0
Inc ==
    /\\ Ready
    /\\ a' = a + 1
    /\\ UNCHANGED b
Next ==
    \\/ Inc
====
`;
  assert.ok(expectations(src).lines.has(5));
});
