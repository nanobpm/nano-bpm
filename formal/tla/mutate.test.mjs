// Tests for the spec guard-mutation generator (#1248).
import test from 'node:test';
import assert from 'node:assert/strict';
import { classifyError, isAssignmentLhs, mutantsOf, selectMutants, verdict } from './mutate.mjs';

const lhs = (t) => isAssignmentLhs(t, t.indexOf('= '));

test('primed and EXCEPT update targets are assignments', () => {
  assert.equal(lhs("x' = 1"), true);
  assert.equal(lhs('[p EXCEPT ![g] = 0]'), true);
  assert.equal(lhs('[p EXCEPT ![g][h] = 0]'), true);
  assert.equal(lhs('[p EXCEPT ![T[g]] = 0]'), true);
  assert.equal(lhs('[p EXCEPT !.f = 0]'), true);
});

test('indexed comparisons are guards', () => {
  assert.equal(lhs('Tgt[f] = n'), false);
  assert.equal(lhs('Kind[n] = "end"'), false);
  assert.equal(lhs('pending[g] = 0'), false);
  assert.equal(lhs('p[T[g]][h] = 0'), false);
  assert.equal(lhs('r.f = 0'), false);
});

test('mutantsOf mutates indexed equality guards but not EXCEPT targets', () => {
  const src = `---- MODULE M ----
VARIABLE p
A(g) ==
    /\\ p[g] = 0
    /\\ p' = [p EXCEPT ![g] = 1]
Next == \\E g \\in {1} : A(g)
====
`;
  const eq = mutantsOf('M.tla', src).filter((m) => m.id.includes('eq->neq'));
  assert.deepEqual(eq.map((m) => m.line), [4]);
});

test('unclassified TLC errors are not kills', () => {
  assert.equal(classifyError('Warning: something'), null);
  assert.equal(classifyError(''), null);
  assert.equal(classifyError('*** Parse Error ***'), 'invalid');
  assert.equal(classifyError('Error: Attempted to apply function:\n<<1>>'), 'eval');
});

test('a violation with states still queued is a verdict only when TLC stops at the first', () => {
  // The tier-2 fix (#1682 review): a baseline-passing model runs its mutant
  // WITHOUT -continue, so TLC halts at the first violated invariant and the
  // undrained queue is fine — the kill is recorded, not mistaken for an error.
  // With -continue (stopsAtFirst = false) the same log is an ERROR: TLC kept
  // exploring past the violation, so the partial invariant set says nothing.
  const log = 'Invariant TypeOK is violated.\n42 states left on queue.';
  assert.equal(verdict(log, ['Spec'], 1, true), 'violates:TypeOK');
  assert.equal(verdict(log, ['Spec'], 1, false), 'error');
  // A drained queue is a complete exploration: the violation set is the
  // verdict either way.
  const drained = 'Invariant TypeOK is violated.\n0 states left on queue.';
  assert.equal(verdict(drained, ['Spec'], 1, false), 'violates:TypeOK');
});

test('an unmatched --only filter is a defect, not an empty success', () => {
  const mutants = [
    { id: 'TokenFlow.tla:InclusiveReady:eq->neq#1' },
    { id: 'ZeebeTokenFlow.tla:CorrelateEvent:and->or#2' },
  ];
  // No filter selects everything.
  assert.equal(selectMutants(mutants, null).length, 2);
  // A matching substring selects.
  assert.deepEqual(selectMutants(mutants, 'InclusiveReady').map((m) => m.id), ['TokenFlow.tla:InclusiveReady:eq->neq#1']);
  // A typo / a renamed guard matches nothing and must throw, never exit 0
  // having run zero mutants.
  assert.throws(() => selectMutants(mutants, 'InclusiveRedy'), /--only "InclusiveRedy" matched no mutant/);
  assert.throws(() => selectMutants(mutants, 'InclusiveRedy'), /Closest: TokenFlow\.tla:InclusiveReady:eq->neq#1/);
});
