// Node unit tests for the trigger pack-driver SDK (`trigger_sdk.ts`,
// ADR 0025 §6) — the checked, retrying emit half of the pack-source contract.
//
// Run in CI by the `console-ts` job, and locally with:
//   node --experimental-strip-types --test server/crates/nano-server-console/src/trigger_sdk_test.ts
//
// These are the defect-class guards for #1543: a pack driver that ignored the
// ingress response silently lost an event when the emit failed (a non-2xx, or
// a rejected `fetch`). `emitEvent` must (a) resolve only on a durable 2xx,
// (b) retry a failed emit, and (c) throw — loudly — once its attempts are
// exhausted, so a dropped trigger event always names its cause.

import { test } from "node:test";
import { createServer, type Server } from "node:http";
import {
  assert,
  assertEquals,
  assertRejects,
  assertStringIncludes,
} from "./test_support.ts";
import { emitEvent, EmitError } from "./trigger_sdk.ts";

const noSleep = () => Promise.resolve();

/** Start a loopback HTTP server that answers each POST with the queued
 * `(status, body)` responses in order (repeating the last one when exhausted),
 * recording every request it receives. Returns the base URL, the recorded
 * requests, and a `close`. */
async function stubIngress(
  responses: Array<[number, string]>,
): Promise<{ url: string; seen: Array<{ idem?: string; token?: string; body: string }>; close: () => Promise<void> }> {
  const seen: Array<{ idem?: string; token?: string; body: string }> = [];
  let n = 0;
  const server: Server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      seen.push({
        idem: req.headers["idempotency-key"] as string | undefined,
        token: req.headers["x-webhook-token"] as string | undefined,
        body: Buffer.concat(chunks).toString("utf8"),
      });
      const [status, body] = responses[Math.min(n++, responses.length - 1)];
      res.writeHead(status, { "content-type": "application/json" });
      res.end(body);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return {
    url: `http://127.0.0.1:${port}/hook`,
    seen,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}

test("emitEvent resolves with the ack on a durable 2xx, sending token + idempotency key", async () => {
  const stub = await stubIngress([[202, '{"enqueued":true,"id":7}']]);
  try {
    const ack = (await emitEvent(stub.url, { n: 1 }, {
      token: "s3cret",
      idempotencyKey: "evt-1",
      sleep: noSleep,
    })) as { enqueued: boolean; id: number };
    assertEquals(ack.enqueued, true);
    assertEquals(ack.id, 7);
    assertEquals(stub.seen.length, 1);
    assertEquals(stub.seen[0].idem, "evt-1");
    assertEquals(stub.seen[0].token, "s3cret");
    assertEquals(JSON.parse(stub.seen[0].body).n, 1);
  } finally {
    await stub.close();
  }
});

test("emitEvent treats a duplicate-key 200 as durable (the retry no-op)", async () => {
  const stub = await stubIngress([[200, '{"enqueued":false}']]);
  try {
    const ack = (await emitEvent(stub.url, { n: 1 }, { sleep: noSleep })) as {
      enqueued: boolean;
    };
    assertEquals(ack.enqueued, false);
  } finally {
    await stub.close();
  }
});

test("emitEvent retries a non-2xx until the durable insert lands", async () => {
  // The #1287 class: a contended inbox surfaces 500s, then the write goes
  // through. The emit must not drop the event on the first failure.
  const stub = await stubIngress([[500, "database is locked"], [500, "database is locked"], [202, '{"enqueued":true,"id":9}']]);
  try {
    const ack = (await emitEvent(stub.url, { n: 2 }, {
      idempotencyKey: "evt-2",
      sleep: noSleep,
    })) as { enqueued: boolean };
    assertEquals(ack.enqueued, true);
    assertEquals(stub.seen.length, 3, "two failed attempts, then the retry");
    // Every attempt carries the same idempotency key, so a retry after an
    // ambiguous outcome collapses instead of duplicating.
    assert(stub.seen.every((r) => r.idem === "evt-2"));
  } finally {
    await stub.close();
  }
});

test("emitEvent throws an EmitError naming the cause once attempts are exhausted", async () => {
  const stub = await stubIngress([[500, "database is locked"]]);
  try {
    await assertRejects(
      () => emitEvent(stub.url, { n: 3 }, { maxAttempts: 3, sleep: noSleep }),
      EmitError,
      "HTTP 500",
    );
    assertEquals(stub.seen.length, 3, "exactly maxAttempts tries, no more");
  } finally {
    await stub.close();
  }
});

test("emitEvent throws on a transport error (a rejected fetch is not a silent drop)", async () => {
  // Nothing is listening: every attempt rejects. The old fire-and-forget
  // driver lost the event here without a trace; the SDK must surface it.
  const closed = await stubIngress([[202, "{}"]]);
  await closed.close();
  await assertRejects(
    () => emitEvent(closed.url, { n: 4 }, { maxAttempts: 2, sleep: noSleep }),
    EmitError,
    "transport:",
  );
});

test("EmitError carries the last status and the attempt count", async () => {
  const stub = await stubIngress([[503, "unavailable"]]);
  try {
    let caught: unknown;
    try {
      await emitEvent(stub.url, { n: 5 }, { maxAttempts: 2, sleep: noSleep });
    } catch (e) {
      caught = e;
    }
    assert(caught instanceof EmitError, "an EmitError");
    assertEquals(caught.status, 503);
    assertEquals(caught.attempts, 2);
    assertStringIncludes(caught.message, "503");
  } finally {
    await stub.close();
  }
});

test("emitEvent retries back off (base doubling, capped) before giving up", async () => {
  const stub = await stubIngress([[500, "busy"]]);
  const delays: number[] = [];
  try {
    await assertRejects(
      () =>
        emitEvent(stub.url, { n: 6 }, {
          maxAttempts: 4,
          baseDelayMs: 100,
          maxDelayMs: 250,
          sleep: (ms) => {
            delays.push(ms);
            return Promise.resolve();
          },
        }),
      EmitError,
    );
    assertEquals(delays, [100, 200, 250], "100 → 200 → capped at 250");
  } finally {
    await stub.close();
  }
});

test("emitEvent preserves an explicit JSON null body; only undefined defaults to {}", async () => {
  const stub = await stubIngress([[202, '{"enqueued":true}']]);
  try {
    await emitEvent(stub.url, null, { sleep: noSleep });
    await emitEvent(stub.url, undefined, { sleep: noSleep });
    assertEquals(stub.seen[0].body, "null");
    assertEquals(stub.seen[1].body, "{}");
  } finally {
    await stub.close();
  }
});

test("emitEvent honors a maxDelayMs below the base delay (the cap is not raised to base)", async () => {
  const stub = await stubIngress([[500, "busy"]]);
  const delays: number[] = [];
  try {
    await assertRejects(
      () =>
        emitEvent(stub.url, { n: 7 }, {
          maxAttempts: 3,
          // Deliberately leave baseDelayMs at its 250 ms default and ask for a
          // 100 ms cap: every retry wait must be 100 ms, never the base.
          maxDelayMs: 100,
          sleep: (ms) => {
            delays.push(ms);
            return Promise.resolve();
          },
        }),
      EmitError,
    );
    assertEquals(delays, [100, 100], "the requested cap wins over the larger base delay");
  } finally {
    await stub.close();
  }
});

test("emitEvent treats a 2xx as durable even when the ack body is interrupted", async () => {
  // The ingress has already persisted the event when it sends a 2xx
  // (persist-before-ack), so a connection that dies mid-body must NOT cause a
  // second POST — the emit resolves instead of retrying a durable event.
  let posts = 0;
  const interrupted = (async () => {
    posts++;
    // Status/headers arrive (202 = durable), then the socket dies mid-body.
    return new Response('{"enqueued":tru', {
      status: 202,
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
  const ack = await emitEvent("http://trigger.invalid/hook", { n: 8 }, {
    maxAttempts: 3,
    sleep: noSleep,
    fetchFn: interrupted,
  });
  assertEquals(ack, {}, "a best-effort ack read falls back to {}");
  assertEquals(posts, 1, "exactly one POST — no retry of a durable 2xx");
});
