import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { getClusterHealth, getTopology, type NodeHealth } from "../gen";
import { nodeConsoleUrl } from "../lib/api";
import {
  Badge,
  Card,
  ErrorText,
  PageHeader,
  SectionLabel,
} from "@nanobpm/process-explorer/ui";
import JourneyPicker from "../components/JourneyPicker";
import { CONSOLE_PROFILE, IS_STUDIO } from "../lib/profile";
import { useTour } from "../lib/tour/tourContext";
import { pickerJourneys } from "../lib/tour/picker";

export default function Topology() {
  // Topology is the observe build's home, so it carries the cold-arrival journey
  // picker there (parallel to Projects in studio). In studio Topology is a
  // secondary view, so we don't duplicate the picker. The section only renders
  // when the registry actually offers a non-overview journey for this profile —
  // today observe has only its overview, so nothing shows until #409 lands a
  // real observe journey.
  const tour = useTour();
  const journeys = tour
    ? pickerJourneys(tour.availableJourneys, CONSOLE_PROFILE)
    : [];
  const { data, isLoading, error } = useQuery({
    queryKey: ["topology"],
    queryFn: async () => (await getTopology({ throwOnError: true })).data,
    refetchInterval: 2000,
  });

  // Live per-node liveness, probed independently (slower cadence: each refresh
  // probes every peer over the network).
  const { data: health } = useQuery({
    queryKey: ["clusterHealth"],
    queryFn: async () => (await getClusterHealth({ throwOnError: true })).data,
    refetchInterval: 5000,
  });

  const healthOf = (nodeId: number): NodeHealth | undefined =>
    health?.nodes.find((n) => n.nodeId === nodeId);

  return (
    <div className="p-8">
      <PageHeader
        title="Cluster topology"
        subtitle={
          <>
            Live view of nodes and partition placement
            {data ? ` · gateway v${data.gateway_version}` : ""}
          </>
        }
      />

      {!IS_STUDIO && journeys.length > 0 && (
        <section className="mb-8">
          <JourneyPicker
            title="Get started"
            subtitle="Pick a guided journey — or explore the cluster on your own."
            journeys={journeys}
            onPick={(id) => tour?.startJourney(id)}
            onOverview={tour ? () => tour.startTour() : undefined}
          />
        </section>
      )}

      {isLoading && <p className="text-fg-muted">Loading…</p>}
      {error && <ErrorText>Failed to load topology: {String(error)}</ErrorText>}

      {data && (
        <div className="space-y-8">
          <section className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <Stat label="Nodes" value={data.num_nodes} />
            <Stat label="Partitions" value={data.num_partitions} />
            <Stat label="Replication factor" value={data.replication_factor} />
            <Stat label="Raft" value={data.raft_enabled ? "enabled" : "off"} />
          </section>

          <section>
            <SectionLabel>Nodes</SectionLabel>
            <div className="flex flex-wrap gap-3">
              {data.nodes.map((n) => {
                const h = healthOf(n.node_id);
                const down = h && !h.reachable && !n.is_self;
                // A node is "catching up" when it owns a partition currently served
                // by a failover incumbent (leader != owner). Observable for any
                // partition this (queried) node hosts a Raft group for.
                const recovering =
                  !down &&
                  data.partitions.some(
                    (p) => p.owner === n.node_id && p.recovering,
                  );
                // Other nodes link to the equivalent page on their own IP; the
                // self node (empty address) is the current page, so not a link.
                const href = n.is_self
                  ? null
                  : nodeConsoleUrl(n.address, "/topology");
                const cls = `block min-w-[12rem] rounded-lg border px-4 py-3 ${
                  down
                    ? "border-danger/40 bg-danger/10"
                    : recovering
                      ? "border-warn/40 bg-warn/10"
                      : n.is_self
                        ? "border-ok/40 bg-ok/10"
                        : "border-edge bg-raised"
                }${
                  href
                    ? " cursor-pointer transition-colors hover:border-info hover:bg-hover"
                    : ""
                }`;
                const inner = (
                  <>
                    <div className="flex items-center gap-2">
                      <span
                        className={`inline-block h-2 w-2 shrink-0 rounded-full ${
                          h
                            ? h.reachable
                              ? recovering
                                ? "bg-warn"
                                : "bg-ok"
                              : "bg-danger"
                            : "bg-fg-faint"
                        }`}
                        title={
                          h
                            ? h.reachable
                              ? recovering
                                ? "catching up after restart"
                                : "reachable"
                              : `unreachable: ${h.error ?? "no response"}`
                            : "probing…"
                        }
                      />
                      <span className="font-medium">node {n.node_id}</span>
                      {n.is_self && <Badge tone="ok">this</Badge>}
                      {recovering && <Badge tone="warn">catching up</Badge>}
                      {href && (
                        <span className="ml-auto text-xs text-info" aria-hidden>
                          open ↗
                        </span>
                      )}
                    </div>
                    <div className="mt-1 text-xs text-fg-faint">
                      {n.address || "local"}
                    </div>
                    <div className="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-xs">
                      {h?.reachable ? (
                        <>
                          {h.version && (
                            <span className="text-fg-muted">v{h.version}</span>
                          )}
                          {h.latencyMs != null && !n.is_self && (
                            <span className="text-fg-faint">
                              {h.latencyMs} ms
                            </span>
                          )}
                          {recovering ? (
                            <span className="text-warn">
                              reclaiming leadership
                            </span>
                          ) : (
                            n.is_self && (
                              <span className="text-ok">healthy</span>
                            )
                          )}
                        </>
                      ) : h ? (
                        <span className="text-danger">
                          unreachable{h.error ? ` · ${h.error}` : ""}
                        </span>
                      ) : (
                        <span className="text-fg-faint">probing…</span>
                      )}
                    </div>
                  </>
                );
                return href ? (
                  <a
                    key={n.node_id}
                    href={href}
                    target="_blank"
                    rel="noreferrer"
                    title={`Open node ${n.node_id} console (${n.address})`}
                    className={cls}
                  >
                    {inner}
                  </a>
                ) : (
                  <div key={n.node_id} className={cls}>
                    {inner}
                  </div>
                );
              })}
            </div>
          </section>

          <section>
            <SectionLabel>Partitions</SectionLabel>
            <table className="w-full max-w-2xl border-collapse text-sm">
              <thead>
                <tr className="border-b border-edge text-left text-fg-faint">
                  <th className="py-2 pr-4 font-medium">Partition</th>
                  <th className="py-2 pr-4 font-medium">Owner</th>
                  <th className="py-2 pr-4 font-medium">Leader</th>
                  <th className="py-2 pr-4 font-medium">Replicas</th>
                  <th className="py-2 pr-4 font-medium">Raft term</th>
                </tr>
              </thead>
              <tbody>
                {data.partitions.map((p) => (
                  <tr key={p.partition_id} className="border-b border-edge">
                    <td className="py-2 pr-4">{p.partition_id}</td>
                    <td className="py-2 pr-4 text-fg-muted">node {p.owner}</td>
                    <td className="py-2 pr-4">
                      {p.leader === null ? (
                        <span className="text-warn">no leader</span>
                      ) : p.recovering ? (
                        <span
                          className="text-warn"
                          title={`served by failover incumbent node ${p.leader}; owner node ${p.owner} is catching up`}
                        >
                          node {p.leader} · recovering
                        </span>
                      ) : (
                        `node ${p.leader}`
                      )}
                    </td>
                    <td className="py-2 pr-4 text-fg-muted">
                      {p.replicas.map((r) => `node ${r}`).join(", ")}
                    </td>
                    <td className="py-2 pr-4 text-fg-muted">
                      {p.raft_term ?? "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <Card className="px-4 py-3">
      <div className="text-xs uppercase tracking-wide text-fg-faint">
        {label}
      </div>
      <div className="mt-1 text-xl font-semibold">{value}</div>
    </Card>
  );
}
