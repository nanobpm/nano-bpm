// The Process Explorer's one seam to the engine it shows.
//
// The Explorer renders the console API's wire model (`./gen`, generated from
// spec-console/console-api.yaml, and `./gen-c8`, the Camunda REST subset) but
// never fetches it itself: the host passes an `ExplorerDataSource`. The nano
// console's source wraps its generated `/console/api` + `/v2` clients; another
// host (e.g. an embedded engine) maps its own state onto the same shapes.
//
// Operator actions are optional. A host that can't suspend, resume, cancel,
// resolve or write variables leaves the method out and the Explorer hides the
// matching control, so a read-only host never renders a dead button.
import { createContext, useContext, useEffect, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type {
  InstanceDetail,
  InstancePage,
  InstanceTrace,
  ListInstancesData,
  SetInstanceVariablesRequest,
} from "./gen";
import type {
  AgentInstanceHistoryItemResult,
  AgentInstanceResult,
} from "./gen-c8/types.gen";

/** The list query: paging plus the Explorer's state / incident filters. */
export type InstanceListQuery = NonNullable<ListInstancesData["query"]>;

export interface ExplorerDataSource {
  /** One page of process instances, newest first. */
  listInstances(query: InstanceListQuery): Promise<InstancePage>;
  /** An instance with its variables, jobs, incidents and live tokens. */
  getInstance(instanceKey: string): Promise<InstanceDetail>;
  /**
   * The instance's execution trace, or `undefined` when there is none (never
   * traced, or evicted). Reject only on a real failure: the Explorer shows an
   * error for a rejection and an empty state for `undefined`.
   */
  getTrace(instanceKey: string): Promise<InstanceTrace | undefined>;
  /** The deployed BPMN XML of a process definition, or `null` if unavailable. */
  getProcessXml(processDefinitionKey: string): Promise<string | null>;

  cancelInstance?(instanceKey: string): Promise<unknown>;
  suspendInstance?(instanceKey: string): Promise<unknown>;
  resumeInstance?(instanceKey: string): Promise<unknown>;
  resolveIncident?(instanceKey: string, incidentKey: string): Promise<unknown>;
  setVariables?(
    instanceKey: string,
    request: SetInstanceVariablesRequest,
  ): Promise<unknown>;

  /**
   * Call `onChange` whenever the engine's state may have moved on (and once
   * after a reconnect, to catch up). Returns an unsubscribe function. Without
   * it the Explorer shows what it fetched and refreshes only after its own
   * actions.
   */
  subscribe?(onChange: () => void): () => void;

  /** Agent sessions (#1314). Leave out when the engine runs no agents. */
  agents?: {
    /** Every AgentInstance run inside one process instance. */
    listAgentInstances(
      processInstanceKey: string,
    ): Promise<AgentInstanceResult[]>;
    /** The committed turn log of one AgentInstance (any order). */
    getAgentHistory(
      agentInstanceKey: string,
    ): Promise<AgentInstanceHistoryItemResult[]>;
  };
}

const DataSourceContext = createContext<ExplorerDataSource | null>(null);

/** Makes `source` the data source of every Explorer component below it. */
export function ExplorerDataProvider({
  source,
  children,
}: {
  source: ExplorerDataSource;
  children: ReactNode;
}) {
  return (
    <DataSourceContext.Provider value={source}>
      {children}
    </DataSourceContext.Provider>
  );
}

export function useExplorerData(): ExplorerDataSource {
  const source = useContext(DataSourceContext);
  if (!source) {
    throw new Error(
      "Process Explorer components need an <ExplorerDataProvider source={…}> above them",
    );
  }
  return source;
}

/**
 * Invalidates the given React Query key prefixes whenever the data source
 * reports a change. A no-op for a source without `subscribe`.
 */
export function useLiveInvalidation(queryKeys: string[]) {
  const queryClient = useQueryClient();
  const source = useExplorerData();
  useEffect(() => {
    if (!source.subscribe) return;
    return source.subscribe(() => {
      for (const key of queryKeys) {
        queryClient.invalidateQueries({ queryKey: [key] });
      }
    });
    // queryKeys is a stable literal at the call sites; intentionally not a dep.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryClient, source]);
}
