import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useExplorerData } from "./dataSource.tsx";
import { orderHistory } from "./agentHistory.ts";
import {
  appendChunks,
  foldTranscript,
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
  const follow = useExplorerData().agents?.followTranscript;
  const on = enabled && agentInstanceKey !== undefined && follow !== undefined;
  const [blocks, setBlocks] = useState<LiveBlock[] | undefined>(undefined);
  useEffect(() => {
    if (!on) {
      setBlocks(undefined);
      return;
    }
    let stopped = false;
    let from = 0;
    let carry = "";
    const lines: string[] = [];
    let timer: ReturnType<typeof setTimeout> | undefined;
    setBlocks([]);
    const tick = async () => {
      try {
        const page = await follow!(agentInstanceKey!, from);
        if (stopped) return;
        if (page) {
          from = page.nextOffset;
          const r = appendChunks(
            carry,
            page.entries.map((e) => e.chunk),
          );
          carry = r.carry;
          if (r.lines.length) {
            lines.push(...r.lines);
            setBlocks(foldTranscript(lines));
          }
          if (page.status === "completed") return;
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
  }, [on, agentInstanceKey, follow]);
  return blocks;
}
