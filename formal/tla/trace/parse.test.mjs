// Tests for the TLA+ value parser in parse.mjs. TLC renders a finite function
// with string-domain keys using the record syntax but QUOTES the key
// (`["S" |-> "start"]`), which the bare-key scan silently misparsed — the
// `kind`/`edges`/state maps generated from `MCNodes`/`MCFlows` therefore made
// gen-traces.sh fail before it could emit a fixture (Copilot review, PR #1268).
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { parseTlaValue } from './parse.mjs';

test('parses a record with bare identifier keys', () => {
  assert.deepEqual(parseTlaValue('[a |-> 1, b |-> "x"]'), { a: 1, b: 'x' });
});

test('parses a record with quoted string keys (TLC function rendering)', () => {
  assert.deepEqual(parseTlaValue('["S" |-> "start"]'), { S: 'start' });
});

test('parses a record mixing quoted and bare keys', () => {
  assert.deepEqual(
    parseTlaValue('["S" |-> "start", n2 |-> 3]'),
    { S: 'start', n2: 3 },
  );
});

test('parses quoted keys with non-identifier characters', () => {
  assert.deepEqual(parseTlaValue('["s-1" |-> TRUE]'), { 's-1': true });
});

test('parses nested records with quoted keys', () => {
  assert.deepEqual(
    parseTlaValue('["kind" |-> ["n1" |-> "task"]]'),
    { kind: { n1: 'task' } },
  );
});

test('still parses functions in the (k :> v @@ ...) form', () => {
  assert.deepEqual(parseTlaValue('("a" :> 1 @@ "b" :> 2)'), { a: 1, b: 2 });
});

test('VariableScopes fixture: derives the acting child and projects scopes (#1522)', async () => {
  const { variableScopesFixture } = await import('./parse.mjs');
  const st = (cstate, sv, collected = [-1, -1]) => ({
    phase: 'running', cstate, sv, collected, rootResults: [-1, -1],
  });
  const empty = { item: -1, loopCounter: -1, result: -1, seen: -1 };
  const c = (i) => ({ item: i, loopCounter: i, result: 0, seen: -1 });
  const s0 = st(['active', 'active'], { 0: empty, 1: c(1), 2: c(2), 3: empty });
  const s1 = st(['written', 'active'],
    { 0: { ...empty, seen: 1 }, 1: { ...c(1), result: 1 }, 2: c(2), 3: empty });
  const fx = variableScopesFixture('VariableScopes', 'M', { n: 2 }, [
    { step: 1, action: '<Activate line 1>', vars: s0 },
    { step: 2, action: '<Write line 2>', vars: s1 },
  ]);
  assert.equal(fx.steps.length, 1);
  assert.equal(fx.steps[0].action, 'Write');
  assert.equal(fx.steps[0].child, 1);
  assert.deepEqual(fx.steps[0].state.root, { seen: 1 });
  assert.deepEqual(fx.steps[0].state.children['1'], { item: 1, loopCounter: 1, result: 1 });
  assert.deepEqual(fx.steps[0].state.children['2'], { item: 2, loopCounter: 2, result: null });
  assert.deepEqual(fx.initial.collected, ['undef', 'undef']);
});

test('VariableScopes fixture: a state missing the vocabulary fails loudly', async () => {
  const { variableScopesFixture } = await import('./parse.mjs');
  assert.throws(() => variableScopesFixture('VariableScopes', 'M', { n: 1 },
    [{ step: 1, action: 'Init', vars: { phase: 'init' } }]), /observable variables/);
});
