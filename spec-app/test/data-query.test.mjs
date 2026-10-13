// Tests for the `data.query()` runtime pre-resolution engine (ADR 0024 §5) —
// `node --test`. Pure logic: a fake `resolve` stands in for the datasource
// gateway, so this runs CI-safe with no browser and no gateway (the Node test
// runner is the only runtime needed).
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  scanDataQueryCalls,
  preResolveDataQuery,
  preResolveFormSchema,
  DATA_QUERY_BINDING_PREFIX,
} from "../src/data-query.ts";

test("scanDataQueryCalls parses the two-argument (aliased) form", () => {
  const [c] = scanDataQueryCalls('= data.query("app", "SELECT id FROM t")');
  assert.equal(c.source, "app");
  assert.equal(c.sql, "SELECT id FROM t");
  assert.equal(c.static, true);
});

test("scanDataQueryCalls parses the single-argument (default-source) form", () => {
  const [c] = scanDataQueryCalls('= data.query("SELECT 1")');
  assert.equal(c.source, null);
  assert.equal(c.sql, "SELECT 1");
  assert.equal(c.static, true);
});

test("scanDataQueryCalls is conservative for a dynamic SQL argument", () => {
  const [c] = scanDataQueryCalls('= data.query("app", "SELECT " + x)');
  assert.equal(c.source, "app");
  assert.equal(c.sql, null); // not a plain literal → not statically resolvable
  assert.equal(c.static, false);
});

test("scanDataQueryCalls spans cover the whole call (balanced, string-aware)", () => {
  const feel = 'before data.query("app", "SELECT \\", (x)\\" FROM t") after';
  const [c] = scanDataQueryCalls(feel);
  assert.equal(feel.slice(c.span[0], c.span[1]), 'data.query("app", "SELECT \\", (x)\\" FROM t")');
});

test("scanDataQueryCalls tolerates whitespace and finds multiple calls", () => {
  const calls = scanDataQueryCalls('= data . query ( "a" , "x" ) + data.query("b","y")');
  assert.deepEqual(calls.map((c) => c.source), ["a", "b"]);
});

test("scanDataQueryCalls skips an unbalanced (mid-edit) call", () => {
  assert.deepEqual(scanDataQueryCalls('= data.query("app", "SELECT'), []);
});

test("preResolveDataQuery rewrites a call to a binding and returns its rows", async () => {
  const rows = [{ id: 1 }, { id: 2 }];
  const calls = [];
  const resolve = async (source, sql) => {
    calls.push([source, sql]);
    return rows;
  };
  const r = await preResolveDataQuery('= count(data.query("app", "SELECT id FROM t"))', resolve);
  assert.equal(r.expr, `= count(${DATA_QUERY_BINDING_PREFIX}0)`);
  assert.deepEqual(r.context[`${DATA_QUERY_BINDING_PREFIX}0`], rows);
  assert.deepEqual(calls, [["app", "SELECT id FROM t"]]);
});

test("preResolveDataQuery dedups identical queries to one binding + one call", async () => {
  let n = 0;
  const resolve = async () => [{ n: n++ }];
  const r = await preResolveDataQuery(
    '= data.query("app","SELECT 1") + data.query("app","SELECT 1")',
    resolve,
  );
  const name = `${DATA_QUERY_BINDING_PREFIX}0`;
  assert.equal(r.expr, `= ${name} + ${name}`);
  assert.equal(n, 1); // resolved once
  assert.deepEqual(Object.keys(r.context), [name]);
});

test("preResolveDataQuery leaves a dynamic-SQL call untouched", async () => {
  const feel = '= data.query("app", "SELECT " + string(x))';
  const r = await preResolveDataQuery(feel, async () => []);
  assert.equal(r.expr, feel);
  assert.deepEqual(r.context, {});
});

test("preResolveDataQuery nextIndex threads binding names across expressions", async () => {
  const resolve = async () => [];
  const a = await preResolveDataQuery('= data.query("app","x")', resolve);
  const b = await preResolveDataQuery('= data.query("app","y")', resolve, a.nextIndex);
  assert.equal(a.expr, `= ${DATA_QUERY_BINDING_PREFIX}0`);
  assert.equal(b.expr, `= ${DATA_QUERY_BINDING_PREFIX}1`);
});

test("preResolveFormSchema rewrites =-expressions and seeds their rows as data", async () => {
  const schema = {
    type: "default",
    components: [
      { type: "textfield", key: "name" },
      { type: "checkbox", key: "vip", conditional: { hide: '= not(data.query("app", "SELECT 1"))' } },
    ],
  };
  const rows = [{ x: 1 }];
  const out = await preResolveFormSchema(schema, async () => rows);
  const name = `${DATA_QUERY_BINDING_PREFIX}0`;
  assert.equal(out.schema.components[1].conditional.hide, `= not(${name})`);
  assert.deepEqual(out.data[name], rows);
  assert.deepEqual(out.errors, []);
  // Input schema is untouched (deep copy).
  assert.equal(schema.components[1].conditional.hide, '= not(data.query("app", "SELECT 1"))');
});

test("preResolveFormSchema dedups one query used by two fields to a single fetch", async () => {
  let fetches = 0;
  const schema = {
    components: [
      { conditional: { hide: '= data.query("app","SELECT 1")' } },
      { conditional: { hide: '= data.query("app","SELECT 1")' } },
    ],
  };
  const out = await preResolveFormSchema(schema, async () => {
    fetches++;
    return [];
  });
  assert.equal(fetches, 1);
  const name = `${DATA_QUERY_BINDING_PREFIX}0`;
  assert.equal(out.schema.components[0].conditional.hide, `= ${name}`);
  assert.equal(out.schema.components[1].conditional.hide, `= ${name}`);
});

test("preResolveFormSchema binds a failing query to [] and reports the path", async () => {
  const schema = { components: [{ conditional: { hide: '= data.query("app","BAD")' } }] };
  const out = await preResolveFormSchema(schema, async () => {
    throw new Error("boom");
  });
  const name = `${DATA_QUERY_BINDING_PREFIX}0`;
  assert.deepEqual(out.data[name], []);
  assert.equal(out.errors.length, 1);
  assert.match(out.errors[0].message, /boom/);
  assert.equal(out.errors[0].path, "components.0.conditional.hide");
});
