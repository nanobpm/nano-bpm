// Tests for the spec guard-mutation generator (#1248).
import test from 'node:test';
import assert from 'node:assert/strict';
import { classifyError, isAssignmentLhs, mutantsOf } from './mutate.mjs';

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
