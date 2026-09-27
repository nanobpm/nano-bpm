// A representative urban-starter page.json + a tiny in-memory table, used only by
// the dev/preview mock middleware so the spike renders without a live backend.
export const PAGE = {
  title: "Fleet",
  nodes: [
    { type: "text", props: { variant: "heading", text: "Agent Fleet" } },
    { type: "text", props: { variant: "sub", text: "Live runs, driven by page.json." } },
    {
      type: "actionForm",
      props: {
        title: "Launch a run",
        submitLabel: "Launch",
        fields: [
          { key: "goal", label: "Goal" },
          { key: "budget", label: "Token budget" },
        ],
        action: { process: "agent-run" },
      },
    },
    {
      type: "dataGrid",
      props: {
        title: "Runs",
        refreshMs: 4000,
        columns: [
          { field: "id", header: "ID" },
          { field: "goal", header: "Goal" },
          { field: "state", header: "State" },
        ],
        tabs: [
          { label: "Active", filter: [{ field: "state", in: ["converging", "waiting", "escalated"] }] },
          { label: "Done", filter: [{ field: "state", eq: "converged" }] },
          { label: "All", filter: [] },
        ],
        data: { source: "app", table: "runs", orderBy: { field: "id", dir: "desc" } },
      },
    },
  ],
};

export const RUNS = [
  { id: 3, goal: "refactor auth", state: "converging" },
  { id: 2, goal: "add telemetry", state: "escalated" },
  { id: 1, goal: "port to foldkit", state: "converged" },
];
