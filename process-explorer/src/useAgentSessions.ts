import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useExplorerData } from "./dataSource.tsx";
import { orderHistory } from "./agentHistory.ts";
import {
  appendChunks,
  createTranscriptFolder,
  type LiveBlock,
} from "./liveTranscript.ts";

/// React Query keys. Both are in InstanceDetail's `useLiveInvalidation` list, so
/// every live edge from the data source (on the console: the exported position
/// advanced, which includes a newly committed AgentHistory item) refetches
/// them: "live at turn".
export const AGENT_INSTANCES_KEY = "agent-instances";
export const AGENT_HISTORY_KEY = "agent-history";

/// Every AgentInstance run inside one process instance (any BPMN element).
/// Empty when the data source runs no agents.
export function useAgentInstances(processInstanceKey: string | undefined) {
  const agents = useExplorerData().agents;
  return useQuery({
    queryKey: [AGENT_INSTANCES_KEY, processInstanceKey],
    enabled: processInstanceKey !== undefined && agents !== undefined,
    queryFn: () => agents!.listAgentInstances(processInstanceKey!),
  });
}

/// The committed turn log of one AgentInstance, in engine order. PENDING items
/// (an in-flight job's uncommitted writes) and DISCARDED ones (a failed job's)
/// are not part of the agent's history; the data source leaves them out.
export function useAgentHistory(agentInstanceKey: string | undefined) {
  const agents = useExplorerData().agents;
  return useQuery({
    queryKey: [AGENT_HISTORY_KEY, agentInstanceKey],
    enabled: agentInstanceKey !== undefined && agents !== undefined,
    // No `placeholderData(prev => prev)`: it would also carry data across a
    // query-key change, so picking another run would briefly render the previous
    // run's transcript under the new run's header. A same-key refetch already
    // retains its cached data, so a new run simply shows loading until its own
    // history arrives (the Scrubber is keyed by agentInstanceKey and resets).
    queryFn: async () =>
      orderHistory(await agents!.getAgentHistory(agentInstanceKey!)),
  });
}

/// How often an open transcript stream is polled.
export const LIVE_POLL_MS = 1000;

/// The live tail of one in-flight AgentInstance, when the data source streams
/// transcripts (`agents.followTranscript`): polled from the last offset while the
/// stream is open, folded into display blocks. `undefined` when unsupported or
/// disabled; `[]` until the first event.
export function useLiveTranscript(
  agentInstanceKey: string | undefined,
  enabled: boolean,
): LiveBlock[] | undefined {
  // Keep the agents object, not the bare method: a host implementation whose
  // followTranscript uses `this` (e.g. to reach its API client) loses that
  // receiver through `agents?.followTranscript` and throws on every poll — the
  // catch below then suppresses it and the panel stays at "Waiting for the
  // agent…". Calling it as `agents.followTranscript(...)` preserves the
  // receiver, as the agent queries above do.
  const agents = useExplorerData().agents;
  const follow = agents?.followTranscript;
  const on = enabled && agentInstanceKey !== undefined && follow !== undefined;
  // The run key travels with the blocks: between a selection change and the
  // effect's reset the render still holds the previous run's state, and the
  // keyed LiveTail would mount under the new run's header showing the old
  // transcript. Returning blocks only for the run they were folded from closes
  // that stale-render window.
  const [state, setState] = useState<
    { key: string; blocks: LiveBlock[] } | undefined
  >(undefined);
  useEffect(() => {
    if (!on) {
      setState(undefined);
      return;
    }
    const key = agentInstanceKey!;
    let stopped = false;
    let from = 0;
    let carry = "";
    // Fold incrementally: replaying the full log on every poll is quadratic
    // work over an ever-growing transcript and retains it all, so new lines
    // fold into the folder's bounded state instead (see createTranscriptFolder).
    const folder = createTranscriptFolder();
    let timer: ReturnType<typeof setTimeout> | undefined;
    setState({ key, blocks: [] });
    const tick = async () => {
      try {
        // Drain every page the producer already has — `completed` describes the
        // stream, not the page: a completed stream can still span pages, so a
        // single read would lose all later pages. An empty page (no entries, no
        // offset advance) is the end of its data.
        for (;;) {
          const page = await agents!.followTranscript!(key, from);
          if (stopped) return;
          if (!page) break;
          const advanced = page.nextOffset !== from;
          from = page.nextOffset;
          const r = appendChunks(
            carry,
            page.entries.map((e) => e.chunk),
          );
          carry = r.carry;
          if (r.lines.length) {
            setState({ key, blocks: folder.push(r.lines) });
          }
          if (page.status === "completed") {
            if (page.entries.length === 0 && !advanced) {
              // A final line with no trailing newline never leaves `carry`
              // while polling; flush it before the last fold.
              if (carry.trim()) {
                const rest = carry;
                carry = "";
                setState({ key, blocks: folder.push([rest]) });
              }
              return;
            }
            continue;
          }
          if (page.entries.length === 0 && !advanced) break;
        }
      } catch {
        // A failed poll is retried on the next tick.
      }
      if (!stopped) timer = setTimeout(tick, LIVE_POLL_MS);
    };
    void tick();
    return () => {
      stopped = true;
      if (timer) clearTimeout(timer);
    };
  }, [on, agentInstanceKey, agents, follow]);
  return on && state?.key === agentInstanceKey ? state.blocks : undefined;
}
