import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  appendChunks,
  createTranscriptFolder,
  foldTranscript,
  LIVE_FOLD_TAIL,
} from "./liveTranscript.ts";

const ev = (o: Record<string, unknown>) =>
  JSON.stringify({ nwfTranscriptEvent: 1, ...o });

describe("foldTranscript", () => {
  it("merges deltas, lets a snapshot replace them, and pairs tool results", () => {
    const blocks = foldTranscript([
      ev({ kind: "lifecycle", phase: "open" }),
      ev({
        kind: "message",
        role: "user",
        text: "Do it",
        mode: "snapshot",
        final: true,
      }),
      ev({ kind: "message", role: "assistant", text: "Look", messageId: "m1" }),
      ev({ kind: "message", role: "assistant", text: "ing", messageId: "m1" }),
      ev({
        kind: "message",
        role: "assistant",
        text: "Looking.",
        mode: "snapshot",
        messageId: "m1",
        final: true,
      }),
      ev({
        kind: "tool-call",
        name: "Bash",
        callId: "t1",
        args: { command: "git log\n--oneline" },
      }),
      ev({ kind: "tool-result", callId: "t1", ok: false, content: "boom" }),
      ev({ kind: "message", role: "assistant", text: "Next" }),
    ]);
    assert.deepEqual(blocks, [
      {
        kind: "text",
        role: "user",
        text: "Do it",
        messageId: undefined,
        open: false,
      },
      {
        kind: "text",
        role: "assistant",
        text: "Looking.",
        messageId: "m1",
        open: false,
      },
      {
        kind: "tool",
        name: "Bash",
        detail: "git log",
        callId: "t1",
        ok: false,
        result: "boom",
      },
      {
        kind: "text",
        role: "assistant",
        text: "Next",
        messageId: undefined,
        open: true,
      },
    ]);
  });

  it("keeps a different messageId as a separate block, and raw lines as output", () => {
    const blocks = foldTranscript([
      ev({ kind: "message", role: "assistant", text: "A", messageId: "m1" }),
      ev({ kind: "message", role: "assistant", text: "B", messageId: "m2" }),
      "plain harness output",
    ]);
    assert.deepEqual(
      blocks.map((b) => (b.kind === "text" ? `${b.role}:${b.text}` : b.name)),
      ["assistant:A", "assistant:B", "output:plain harness output"],
    );
  });

  it("parses events regardless of JSON property order or whitespace", () => {
    const blocks = foldTranscript([
      // Marker not the first property.
      '{"kind":"message","text":"Hello","nwfTranscriptEvent":1}',
      // Whitespace before the marker.
      '{  "nwfTranscriptEvent" : 1, "kind": "message", "text": " world", "final": true }',
      // Reordered tool call + result still pair.
      '{"args":{"command":"ls"},"kind":"tool-call","callId":"t1","name":"Bash","nwfTranscriptEvent":1}',
      '{"ok":true,"content":"done","nwfTranscriptEvent":1,"callId":"t1","kind":"tool-result"}',
      // A JSON line without the marker stays raw output.
      '{"some":"other json"}',
    ]);
    assert.deepEqual(blocks, [
      {
        kind: "text",
        role: "assistant",
        text: "Hello world",
        messageId: undefined,
        open: false,
      },
      {
        kind: "tool",
        name: "Bash",
        detail: "ls",
        callId: "t1",
        ok: true,
        result: "done",
      },
      {
        kind: "text",
        role: "output",
        text: '{"some":"other json"}',
        open: false,
      },
    ]);
  });
});

describe("appendChunks", () => {
  it("carries a partial line to the next read", () => {
    const a = appendChunks("", ['{"a":1}\n{"b"']);
    assert.deepEqual(a, { lines: ['{"a":1}'], carry: '{"b"' });
    assert.deepEqual(appendChunks(a.carry, [":2}\n"]), {
      lines: ['{"b":2}'],
      carry: "",
    });
  });
});

describe("createTranscriptFolder", () => {
  it("matches foldTranscript when fed the same lines in one push or many", () => {
    const lines = [
      ev({
        kind: "message",
        role: "user",
        text: "Do it",
        mode: "snapshot",
        final: true,
      }),
      ev({ kind: "message", role: "assistant", text: "Look", messageId: "m1" }),
      ev({ kind: "message", role: "assistant", text: "ing", messageId: "m1" }),
      ev({
        kind: "tool-call",
        name: "Bash",
        callId: "t1",
        args: { command: "git log" },
      }),
      ev({ kind: "tool-result", callId: "t1", ok: true, content: "done" }),
      ev({ kind: "message", role: "assistant", text: "Next" }),
      "plain harness output",
    ];
    const batch = foldTranscript(lines);
    const onePush = createTranscriptFolder(Number.MAX_SAFE_INTEGER);
    assert.deepEqual(onePush.push(lines), batch);
    const perLine = createTranscriptFolder(Number.MAX_SAFE_INTEGER);
    for (const line of lines) perLine.push([line]);
    assert.deepEqual(perLine.blocks(), batch);
  });

  it("appends deltas to an open message across pushes", () => {
    const f = createTranscriptFolder();
    f.push([ev({ kind: "message", role: "assistant", text: "Hel" })]);
    f.push([ev({ kind: "message", role: "assistant", text: "lo" })]);
    const blocks = f.push([
      ev({ kind: "message", role: "assistant", text: "!", final: true }),
    ]);
    assert.deepEqual(blocks, [
      {
        kind: "text",
        role: "assistant",
        text: "Hello!",
        messageId: undefined,
        open: false,
      },
    ]);
  });

  it("pairs a tool result with a call pruned from the display tail", () => {
    const f = createTranscriptFolder();
    f.push([ev({ kind: "tool-call", name: "Bash", callId: "t1", args: {} })]);
    // Bury the call under more blocks than the tail retains.
    f.push(
      Array.from({ length: LIVE_FOLD_TAIL + 10 }, (_, i) =>
        ev({
          kind: "message",
          role: "assistant",
          text: `m${i}`,
          start: true,
          final: true,
        }),
      ),
    );
    const blocks = f.push([
      ev({ kind: "tool-result", callId: "t1", ok: true, content: "done" }),
    ]);
    assert.equal(blocks.length, LIVE_FOLD_TAIL);
    // The call itself is pruned from the display; the pairing must not crash or
    // corrupt the tail, and a later unmatched result is simply dropped.
    assert.equal(blocks[0].kind, "text");
    f.push([ev({ kind: "tool-result", callId: "nope", content: "x" })]);
    assert.equal(f.blocks().length, LIVE_FOLD_TAIL);
  });

  it("bounds retained blocks to the tail plus the open message", () => {
    const f = createTranscriptFolder();
    const many = Array.from({ length: LIVE_FOLD_TAIL * 3 }, (_, i) =>
      ev({
        kind: "message",
        role: "assistant",
        text: `m${i}`,
        start: true,
        final: true,
      }),
    );
    const blocks = f.push(many);
    assert.equal(blocks.length, LIVE_FOLD_TAIL);
    // The tail is the newest blocks, in order.
    assert.deepEqual(
      blocks.map((b) => (b.kind === "text" ? b.text : b.name)),
      many.slice(-LIVE_FOLD_TAIL).map((_, i) => `m${LIVE_FOLD_TAIL * 2 + i}`),
    );
  });
});
