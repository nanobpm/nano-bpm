// Tests for the feel-scala differential gate (#1391 step 5).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { compare, citesDeclaredIssue, parseRows, DEFAULT_KNOWN } from './compare.mjs';

const corpus = ['1\t\tok:num:1', '(1 + true)\t\terr', '(if 1 then 2 else 3)\tv0=null\tok:null'].join('\n');
const agreeing = ['ok:num:1', 'err', 'ok:null'].join('\n');
const issues = { '#1392': 'type error → null' };

test('agreeing corpus with empty list passes', () => {
  const { failures } = compare(parseRows(corpus, agreeing), { issues: {}, cases: [] });
  assert.deepEqual(failures, []);
});

test('a seeded (unlisted) divergence fails the gate', () => {
  const seeded = ['ok:num:1', 'ok:null', 'ok:null'].join('\n');
  const { failures, divergent } = compare(parseRows(corpus, seeded), { issues, cases: [] });
  assert.equal(divergent, 1);
  assert.equal(failures.length, 1);
  assert.match(failures[0], /UNLISTED divergence/);
  assert.match(failures[0], /\(1 \+ true\)/);
});

test('a listed divergence with an issue reference passes', () => {
  const seeded = ['ok:num:1', 'ok:null', 'ok:null'].join('\n');
  const known = { issues, cases: [{ expr: '(1 + true)', context: '', nano: 'err', feelScala: 'ok:null', issue: '#1392' }] };
  assert.deepEqual(compare(parseRows(corpus, seeded), known).failures, []);
});

test('a stale listed divergence (now agreeing) fails', () => {
  const known = { issues, cases: [{ expr: '(1 + true)', context: '', nano: 'err', feelScala: 'ok:null', issue: '#1392' }] };
  const { failures } = compare(parseRows(corpus, agreeing), known);
  assert.equal(failures.length, 1);
  assert.match(failures[0], /STALE known divergence \(now agrees/);
});

test('a listed divergence absent from the corpus is stale', () => {
  const known = { issues, cases: [{ expr: 'gone', context: '', nano: 'err', feelScala: 'ok:null', issue: '#1392' }] };
  const { failures } = compare(parseRows(corpus, agreeing), known);
  assert.match(failures.join('\n'), /row not in corpus/);
});

test('a listed divergence whose outcomes changed fails', () => {
  const seeded = ['ok:num:1', 'ok:bool:true', 'ok:null'].join('\n');
  const known = { issues, cases: [{ expr: '(1 + true)', context: '', nano: 'err', feelScala: 'ok:null', issue: '#1392' }] };
  assert.match(compare(parseRows(corpus, seeded), known).failures.join('\n'), /CHANGED known divergence/);
});

test('every listed divergence must cite a declared #N issue', () => {
  const seeded = ['ok:num:1', 'ok:null', 'ok:null'].join('\n');
  const known = { issues: { 'see 1392': 'x' }, cases: [{ expr: '(1 + true)', context: '', nano: 'err', feelScala: 'ok:null', issue: '' }] };
  const out = compare(parseRows(corpus, seeded), known).failures.join('\n');
  assert.match(out, /not of the form #N/);
  assert.match(out, /undeclared issue/);
});

test('inherited / non-#N issue names are never a declared issue', () => {
  const seeded = ['ok:num:1', 'ok:null', 'ok:null'].join('\n');
  for (const issue of ['toString', '__proto__', 'constructor', 'hasOwnProperty', 42, null]) {
    const known = { issues: {}, cases: [{ expr: '(1 + true)', context: '', nano: 'err', feelScala: 'ok:null', issue }] };
    assert.match(compare(parseRows(corpus, seeded), known).failures.join('\n'), /undeclared issue/, String(issue));
    assert.equal(citesDeclaredIssue({}, issue), false);
  }
});

test('empty corpus and outcome-count mismatch are rejected', () => {
  assert.throws(() => parseRows('', ''), /empty corpus/);
  assert.throws(() => parseRows(corpus, 'ok:num:1'), /3 rows but feel-scala produced 1/);
});

test('committed known-divergences.json is well-formed', () => {
  const known = JSON.parse(readFileSync(DEFAULT_KNOWN, 'utf8'));
  const keys = new Set();
  for (const c of known.cases) {
    assert.ok(citesDeclaredIssue(known.issues, c.issue), `undeclared issue ${c.issue}`);
    assert.notEqual(c.nano, c.feelScala);
    const k = `${c.expr}\t${c.context}`;
    assert.ok(!keys.has(k), `duplicate ${k}`);
    keys.add(k);
  }
});
