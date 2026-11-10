/// Live tail of an in-flight agent run: the producer's transcript stream (Nano
/// Workforce's transcript format, one `{"nwfTranscriptEvent":1,…}` JSON event per
/// line) folded into display blocks. Committed turns stay the Scrubber's job; this
/// shows what the agent is doing *now* — text as it streams, tool calls before
/// their results land. Pure: the hook in useAgentSessions.ts feeds it lines.

/// One page of a transcript stream read (nwf's `/agentic/transcripts?stream=&from=`).
export interface TranscriptPage {
  status: "open" | "completed";
  /// Where the next read starts (opaque to the explorer; the producer's byte offset).
  nextOffset: number;
  entries: { offset: number; chunk: string }[];
}

export type LiveBlock =
  | {
      kind: "text";
      role: string;
      text: string;
      messageId?: string;
      open: boolean;
    }
  | {
      kind: "tool";
      name: string;
      detail: string;
      callId?: string;
      ok?: boolean;
      result?: string;
    };

interface Ev {
  nwfTranscriptEvent?: number;
  kind?: string;
  role?: string;
  text?: string;
  mode?: "delta" | "snapshot";
  messageId?: string;
  start?: boolean;
  final?: boolean;
  name?: string;
  callId?: string;
  args?: Record<string, unknown>;
  ok?: boolean;
  content?: string;
}

/// What a tool call is doing, in one line: its command, path or pattern.
export function toolDetail(args: unknown): string {
  if (!args || typeof args !== "object") return "";
  const a = args as Record<string, unknown>;
  const v =
    a.command ?? a.file_path ?? a.path ?? a.pattern ?? a.url ?? a.description;
  return typeof v === "string" ? v.split("\n")[0].slice(0, 200) : "";
}

/// How many display blocks the incremental folder retains. LiveTail renders
/// only the newest few, so folding keeps a small tail rather than the whole
/// run's blocks.
export const LIVE_FOLD_TAIL = 24;

/// Incremental transcript folder: feed lines as they stream in, read the
/// display tail back out. Replaying the full log through `foldTranscript` on
/// every poll is quadratic work over an ever-growing transcript and retains it
/// all; this keeps each poll's fold proportional to the new lines and bounds
/// retained memory to the display tail plus the state later events can still
/// update (the open message, and tool calls awaiting their result).
export interface TranscriptFolder {
  /// Fold more lines in. Returns the retained display tail.
  push(lines: string[]): LiveBlock[];
  /// The retained display tail.
  blocks(): LiveBlock[];
}

export function createTranscriptFolder(
  tail = LIVE_FOLD_TAIL,
): TranscriptFolder {
  // Newest `tail` display blocks, minus the open text block while it is open:
  // a message's deltas can span many polls and arbitrarily many pruned blocks,
  // so the block they append to must survive pruning until it closes.
  let blocks: LiveBlock[] = [];
  let open: Extract<LiveBlock, { kind: "text" }> | undefined;
  // Tool calls still awaiting their result. A result pairs with the most recent
  // unmatched call, which may already be pruned from the display tail; these
  // entries are small (no args, no result payload) and a result removes one.
  const pendingTools: Extract<LiveBlock, { kind: "tool" }>[] = [];

  const prune = () => {
    if (blocks.length > tail) blocks = blocks.slice(blocks.length - tail);
  };
  const closeOpen = () => {
    if (!open) return;
    open.open = false;
    blocks.push(open);
    open = undefined;
    prune();
  };

  const foldLine = (raw: string) => {
    const line = raw.trim();
    if (!line) return;
    let ev: Ev | null = null;
    if (line.startsWith("{")) {
      try {
        // Parse first, then check the marker: JSON property order and whitespace
        // are not significant, so a prefix match would misrender a valid event
        // whose marker is not the first property.
        const parsed: unknown = JSON.parse(line);
        if (
          typeof parsed === "object" &&
          parsed !== null &&
          (parsed as Ev).nwfTranscriptEvent !== undefined
        ) {
          ev = parsed as Ev;
        }
      } catch {
        ev = null;
      }
    }
    if (!ev) {
      // Raw harness output: shown as is.
      closeOpen();
      blocks.push({ kind: "text", role: "output", text: raw, open: false });
      prune();
      return;
    }
    if (ev.kind === "message") {
      const role = ev.role ?? "assistant";
      const same =
        open &&
        open.role === role &&
        !ev.start &&
        (ev.messageId === undefined ||
          open.messageId === undefined ||
          open.messageId === ev.messageId);
      if (!same) {
        closeOpen();
        open = {
          kind: "text",
          role,
          text: "",
          messageId: ev.messageId,
          open: true,
        };
      }
      const b = open!;
      if (ev.mode === "snapshot") b.text = ev.text ?? "";
      else b.text += ev.text ?? "";
      if (ev.messageId) b.messageId = ev.messageId;
      if (ev.final) closeOpen();
    } else if (ev.kind === "tool-call") {
      closeOpen();
      const b: Extract<LiveBlock, { kind: "tool" }> = {
        kind: "tool",
        name: ev.name ?? "tool",
        detail: toolDetail(ev.args),
        callId: ev.callId,
      };
      blocks.push(b);
      prune();
      pendingTools.push(b);
    } else if (ev.kind === "tool-result") {
      closeOpen();
      for (let i = pendingTools.length - 1; i >= 0; i--) {
        const b = pendingTools[i];
        if (ev.callId === undefined || b.callId === ev.callId) {
          b.ok = ev.ok !== false;
          b.result = (ev.content ?? "").slice(0, 2000);
          pendingTools.splice(i, 1);
          break;
        }
      }
    }
    // turn / step / lifecycle / permission: no block of their own here.
  };

  return {
    push(lines) {
      for (const raw of lines) foldLine(raw);
      return this.blocks();
    },
    blocks() {
      return open ? [...blocks, open] : blocks;
    },
  };
}

/// Fold transcript lines into blocks. Message deltas append to the open block of
/// the same role (and messageId, when given); a snapshot replaces it; `final`
/// closes it; a tool call or result is a boundary. Batch form of
/// `createTranscriptFolder` without pruning; a live poll folds incrementally.
export function foldTranscript(lines: string[]): LiveBlock[] {
  return createTranscriptFolder(Number.MAX_SAFE_INTEGER).push(lines);
}

/// Split appended chunks into complete lines, carrying a partial last line over.
export function appendChunks(
  carry: string,
  chunks: string[],
): { lines: string[]; carry: string } {
  const all = carry + chunks.join("");
  const nl = all.lastIndexOf("\n");
  if (nl < 0) return { lines: [], carry: all };
  return { lines: all.slice(0, nl).split("\n"), carry: all.slice(nl + 1) };
}
