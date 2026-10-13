// Tests for the responsive-presentation vocabulary (issue #1005) — `node --test`.
// Pure constants/guards, so this runs CI-safe without a browser or a JS runtime. These lock
// the enum members, the defaults, and the narrowing guards that both consumers
// (the console Page Composer / `pageSchema` and the Urban runtime) bind against.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  MOBILE_MAX_WIDTH,
  COLUMN_MOBILE_PRIORITIES,
  isColumnMobilePriority,
  NAV_VARIANTS,
  isNavVariant,
  NAV_OVERFLOW_MODES,
  isNavOverflow,
  NAV_ITEM_GROUPS,
  NAV_ITEM_GROUP_DEFAULT,
  isNavItemGroup,
  DATA_GRID_MOBILE_PRESENTATIONS,
  DATA_GRID_MOBILE_PRESENTATION_DEFAULT,
  isDataGridMobilePresentation,
  LAYOUT_MOBILE_VARIANTS,
  LAYOUT_MOBILE_VARIANT_DEFAULT,
  isLayoutMobileVariant,
} from "../src/page-presentation.ts";

test("MOBILE_MAX_WIDTH is the one canonical breakpoint", () => {
  assert.equal(MOBILE_MAX_WIDTH, "640px");
});

test("column mobile priorities are exactly primary|chip|hidden", () => {
  assert.deepEqual([...COLUMN_MOBILE_PRIORITIES], ["primary", "chip", "hidden"]);
  assert.ok(isColumnMobilePriority("chip"));
  assert.ok(!isColumnMobilePriority("nope"));
  assert.ok(!isColumnMobilePriority(undefined));
});

test("nav variants include the launcher cards grid", () => {
  assert.deepEqual([...NAV_VARIANTS], ["bar", "rail", "cards"]);
  assert.ok(isNavVariant("cards"));
  assert.ok(!isNavVariant("grid"));
});

test("nav overflow modes carry menu", () => {
  assert.deepEqual([...NAV_OVERFLOW_MODES], ["menu"]);
  assert.ok(isNavOverflow("menu"));
  assert.ok(!isNavOverflow("hidden"));
});

test("nav item groups default to primary", () => {
  assert.deepEqual([...NAV_ITEM_GROUPS], ["primary", "secondary"]);
  assert.equal(NAV_ITEM_GROUP_DEFAULT, "primary");
  assert.ok(isNavItemGroup(NAV_ITEM_GROUP_DEFAULT));
  assert.ok(!isNavItemGroup("tertiary"));
});

test("dataGrid mobile presentation defaults to cards", () => {
  assert.deepEqual([...DATA_GRID_MOBILE_PRESENTATIONS], ["cards", "table"]);
  assert.equal(DATA_GRID_MOBILE_PRESENTATION_DEFAULT, "cards");
  assert.ok(isDataGridMobilePresentation(DATA_GRID_MOBILE_PRESENTATION_DEFAULT));
  assert.ok(!isDataGridMobilePresentation("list"));
});

test("page mobile layout variants are lockable with a stack default", () => {
  assert.deepEqual([...LAYOUT_MOBILE_VARIANTS], ["stack", "cards"]);
  assert.equal(LAYOUT_MOBILE_VARIANT_DEFAULT, "stack");
  assert.ok(isLayoutMobileVariant(LAYOUT_MOBILE_VARIANT_DEFAULT));
  assert.ok(!isLayoutMobileVariant("grid"));
});
