import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { appendChunks, foldTranscript } from "./liveTranscript.ts";

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
