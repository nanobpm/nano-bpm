// Unit tests for the pure "update from template" helpers. Node-native: run with
// `node --experimental-strip-types --test src/lib/templateUpdate.test.ts`.
// These back the projects list, the IDE workspace and the extensions
// reverse-index, so their join/classification logic is guarded in one place.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  planChangeCount,
  planIsClean,
  planNothingToDo,
  projectsNewlyUpdatable,
  projectsUsingPack,
  summarizeBatch,
  targetTitle,
  toUpdateTarget,
  updatableProjectsUsingPack,
  type BatchItemResult,
} from "./templateUpdate.ts";
import type { ProjectSummary, UpdatePlan } from "../gen";

// A minimal UpdatePlan with only the fields the helpers read; the rest are
// filled with empty defaults so the shape type-checks.
function plan(p: Partial<UpdatePlan>): UpdatePlan {
  return {
    pack: "pack",
    applied: false,
    versionBumped: false,
    create: [],
    overwrite: [],
    merged: [],
    preserved: [],
    conflicts: [],
    orphans: [],
    ...p,
  } as UpdatePlan;
}

function project(p: Partial<ProjectSummary>): ProjectSummary {
  return {
    name: "proj",
    description: "",
    deployTarget: "",
    updatedMs: 0,
    processes: 0,
    decisions: 0,
    forms: 0,
    workers: 0,
    running: false,
    source: "workspace",
    lang: "node",
    updateAvailable: false,
    ...p,
  } as ProjectSummary;
}

test("planChangeCount unions create + overwrite + merged (never conflicts)", () => {
  assert.equal(
    planChangeCount(
      plan({
        create: ["a"],
        overwrite: ["b", "c"],
        merged: ["d"],
        conflicts: ["x"],
      }),
    ),
    4,
  );
  assert.equal(planChangeCount(plan({})), 0);
});

test("planIsClean: has changes and no conflicts", () => {
  assert.equal(planIsClean(plan({ create: ["a"] })), true);
  assert.equal(planIsClean(plan({ create: ["a"], conflicts: ["x"] })), false);
  assert.equal(planIsClean(plan({})), false);
});

test("planNothingToDo: no writable changes and no conflicts", () => {
  assert.equal(planNothingToDo(plan({})), true);
  assert.equal(
    planNothingToDo(plan({ preserved: ["keep"], orphans: ["x"] })),
    true,
  );
  assert.equal(planNothingToDo(plan({ create: ["a"] })), false);
  assert.equal(planNothingToDo(plan({ conflicts: ["x"] })), false);
});

test("projectsUsingPack inverts the scaffoldedFrom.pack breadcrumb", () => {
  const projects = [
    project({ name: "a", scaffoldedFrom: { pack: "@nanobpm/starter" } }),
    project({ name: "b", scaffoldedFrom: { pack: "@nanobpm/other" } }),
    project({ name: "c" }), // built-in / not scaffolded
    project({ name: "d", scaffoldedFrom: { pack: "@nanobpm/starter" } }),
  ];
  assert.deepEqual(
    projectsUsingPack(projects, "@nanobpm/starter").map((p) => p.name),
    ["a", "d"],
  );
});

test("updatableProjectsUsingPack keeps only updatable workspace copies", () => {
  const projects = [
    // updatable workspace copy — included
    project({
      name: "a",
      scaffoldedFrom: { pack: "P" },
      updateAvailable: true,
    }),
    // up to date — excluded
    project({
      name: "b",
      scaffoldedFrom: { pack: "P" },
      updateAvailable: false,
    }),
    // updatable but an external path import — excluded (never overwritten)
    project({
      name: "c",
      scaffoldedFrom: { pack: "P" },
      updateAvailable: true,
      source: "path",
    }),
    // different pack — excluded
    project({
      name: "d",
      scaffoldedFrom: { pack: "Q" },
      updateAvailable: true,
    }),
  ];
  assert.deepEqual(
    updatableProjectsUsingPack(projects, "P").map((p) => p.name),
    ["a"],
  );
});

test("toUpdateTarget / targetTitle carry name, displayName, latestVersion", () => {
  const t = toUpdateTarget(
    project({ name: "ord", displayName: "Orders", latestVersion: "2.0.0" }),
  );
  assert.deepEqual(t, {
    name: "ord",
    displayName: "Orders",
    latestVersion: "2.0.0",
  });
  assert.equal(targetTitle(t), "Orders");
  assert.equal(targetTitle({ name: "raw" }), "raw");
});

test("summarizeBatch buckets updated / conflicted / errored, skipping no-ops", () => {
  const results: BatchItemResult[] = [
    { name: "a", plan: plan({ create: ["x"], applied: true }) }, // updated
    { name: "b", plan: plan({ applied: true }) }, // no-op (nothing written)
    { name: "c", plan: plan({ conflicts: ["y"], applied: true }) }, // conflict
    { name: "d", error: "boom" }, // errored
  ];
  assert.deepEqual(summarizeBatch(results), {
    updated: ["a"],
    conflicted: ["c"],
    errored: ["d"],
  });
});

test("projectsNewlyUpdatable scopes to the updated pack via latestVersion delta", () => {
  // Before: X-project already eligible at target v2; Y-project eligible at v1;
  // Z-project up to date (no target).
  const before = [
    project({
      name: "x",
      scaffoldedFrom: { pack: "X", version: "1.0.0" },
      updateAvailable: true,
      latestVersion: "2.0.0",
    }),
    project({
      name: "y",
      scaffoldedFrom: { pack: "Y", version: "1.0.0" },
      updateAvailable: true,
      latestVersion: "1.5.0",
    }),
    project({ name: "z", scaffoldedFrom: { pack: "X", version: "3.0.0" } }),
  ];
  // Updating extension X to v3 advances X-projects' target (x: 2→3, z: none→3)
  // but leaves Y untouched.
  const after = [
    project({
      name: "x",
      scaffoldedFrom: { pack: "X", version: "1.0.0" },
      updateAvailable: true,
      latestVersion: "3.0.0",
    }),
    project({
      name: "y",
      scaffoldedFrom: { pack: "Y", version: "1.0.0" },
      updateAvailable: true,
      latestVersion: "1.5.0",
    }),
    project({
      name: "z",
      scaffoldedFrom: { pack: "X", version: "3.0.0" },
      updateAvailable: true,
      latestVersion: "3.0.0",
    }),
  ];
  assert.deepEqual(
    projectsNewlyUpdatable(before, after).map((p) => p.name),
    ["x", "z"],
  );
});

test("projectsNewlyUpdatable excludes path imports and non-updatable projects", () => {
  const before = [
    project({ name: "p", updateAvailable: false }),
    project({ name: "q", updateAvailable: false }),
  ];
  const after = [
    // External path import — never overwritten in place, so excluded even though
    // its target moved.
    project({
      name: "p",
      source: "path",
      updateAvailable: true,
      latestVersion: "2.0.0",
    }),
    // Target moved but the project is not flagged updatable — excluded.
    project({ name: "q", updateAvailable: false, latestVersion: "2.0.0" }),
  ];
  assert.deepEqual(projectsNewlyUpdatable(before, after), []);
});

test("projectsNewlyUpdatable is empty when no target changed (unrelated update)", () => {
  const before = [
    project({
      name: "a",
      updateAvailable: true,
      latestVersion: "2.0.0",
    }),
  ];
  const after = [
    project({
      name: "a",
      updateAvailable: true,
      latestVersion: "2.0.0",
    }),
  ];
  assert.deepEqual(projectsNewlyUpdatable(before, after), []);
});
