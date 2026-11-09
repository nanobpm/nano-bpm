// Unit tests for the Process Explorer instance-action helpers. Node-native: run
// with `node --experimental-strip-types --test src/lib/instanceActions.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  isCancellable,
  isSuspendable,
  isResumable,
  cancelConfirmMessage,
} from "./instanceActions.ts";

test("isCancellable is true only for a running (Active) instance", () => {
  assert.equal(isCancellable("Active"), true);
});

test("isCancellable is false for finished or transient states", () => {
  // Completed / Terminated have no tokens to discard; Terminating is already
  // being cancelled. Guards the defect class of offering Cancel on an instance
  // the engine would reject with a 404.
  for (const state of ["Completed", "Terminated", "Terminating", ""]) {
    assert.equal(isCancellable(state), false, `state=${state}`);
  }
});

test("isSuspendable is true only for a running (Active) instance", () => {
  assert.equal(isSuspendable("Active"), true);
  // Only a live transition ACTIVE->SUSPENDED is valid; everything else (already
  // suspended, terminal, transient) must not offer Suspend.
  for (const state of [
    "Suspended",
    "Completed",
    "Terminated",
    "Terminating",
    "",
  ]) {
    assert.equal(isSuspendable(state), false, `state=${state}`);
  }
});

test("isResumable is true only for a Suspended instance", () => {
  assert.equal(isResumable("Suspended"), true);
  for (const state of [
    "Active",
    "Completed",
    "Terminated",
    "Terminating",
    "",
  ]) {
    assert.equal(isResumable(state), false, `state=${state}`);
  }
});

test("suspend and resume are mutually exclusive for any state", () => {
  // The detail header offers at most one of the two, so the pair must never both
  // be true — guards the defect class of showing Suspend and Resume together.
  for (const state of ["Active", "Suspended", "Completed", "Terminated", ""]) {
    assert.equal(
      isSuspendable(state) && isResumable(state),
      false,
      `state=${state}`,
    );
  }
});

test("cancelConfirmMessage names the instance and warns it is irreversible", () => {
  const msg = cancelConfirmMessage("order-fulfilment", "2251799813685249");
  assert.match(msg, /order-fulfilment/);
  // The instance key must appear so an operator can't confuse two runs of the
  // same process definition (processId is not unique across instances).
  assert.match(msg, /2251799813685249/);
  assert.match(msg, /cannot be undone/i);
});
