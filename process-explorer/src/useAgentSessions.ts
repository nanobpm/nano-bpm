import { useQuery } from "@tanstack/react-query";
import { useExplorerData } from "./dataSource.tsx";
import { orderHistory } from "./agentHistory.ts";

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
