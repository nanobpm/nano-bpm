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

/// Fold transcript lines into blocks. Message deltas append to the open block of
/// the same role (and messageId, when given); a snapshot replaces it; `final`
/// closes it; a tool call or result is a boundary.
export function foldTranscript(lines: string[]): LiveBlock[] {
  const out: LiveBlock[] = [];
  const openText = () => {
    const b = out[out.length - 1];
    return b?.kind === "text" && b.open ? b : undefined;
  };
  const closeText = () => {
    const b = openText();
    if (b) b.open = false;
  };
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;
    let ev: Ev | null = null;
    if (line.startsWith('{"nwfTranscriptEvent"')) {
      try {
        ev = JSON.parse(line) as Ev;
      } catch {
        ev = null;
      }
    }
    if (!ev) {
      // Raw harness output: shown as is.
      closeText();
      out.push({ kind: "text", role: "output", text: raw, open: false });
      continue;
    }
    if (ev.kind === "message") {
      const role = ev.role ?? "assistant";
      let b = openText();
      const same =
        b &&
        b.role === role &&
        !ev.start &&
        (ev.messageId === undefined ||
          b.messageId === undefined ||
          b.messageId === ev.messageId);
      if (!same) {
        closeText();
        b = {
          kind: "text",
          role,
          text: "",
          messageId: ev.messageId,
          open: true,
        };
        out.push(b);
      }
      b = b!;
      if (ev.mode === "snapshot") b.text = ev.text ?? "";
      else b.text += ev.text ?? "";
      if (ev.messageId) b.messageId = ev.messageId;
      if (ev.final) b.open = false;
    } else if (ev.kind === "tool-call") {
      closeText();
      out.push({
        kind: "tool",
        name: ev.name ?? "tool",
        detail: toolDetail(ev.args),
        callId: ev.callId,
      });
    } else if (ev.kind === "tool-result") {
      closeText();
      for (let i = out.length - 1; i >= 0; i--) {
        const b = out[i];
        if (
          b.kind === "tool" &&
          b.ok === undefined &&
          (ev.callId === undefined || b.callId === ev.callId)
        ) {
          b.ok = ev.ok !== false;
          b.result = (ev.content ?? "").slice(0, 2000);
          break;
        }
      }
    }
    // turn / step / lifecycle / permission: no block of their own here.
  }
  return out;
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
