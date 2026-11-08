// @nanobpm/process-explorer — the nano console's Process Explorer as a
// component library. See README.md.

// The data seam and the views.
export {
  ExplorerDataProvider,
  useExplorerData,
  useLiveInvalidation,
  type ExplorerDataSource,
  type InstanceListQuery,
} from "./dataSource.tsx";
export { default as Explorer, type ExplorerProps } from "./Explorer.tsx";
export { default as InstanceDetail } from "./InstanceDetail.tsx";

// Building blocks the views are made of, for hosts that compose their own.
export { default as BpmnViewer, type ElementBadge } from "./BpmnViewer.tsx";
export { default as AgentSessionPanel } from "./AgentSessionPanel.tsx";
export { TraceTimeline, msFmt, type TraceTimeFmt } from "./TraceTimeline.tsx";
export { fmtClock, fmtDuration } from "./traceTime.ts";
export { IncidentReason } from "./IncidentReason.tsx";
export { ResizeHandle } from "./ResizeHandle.tsx";
export * from "./usePaneResize.ts";
export * from "./explorerFilters.ts";
export * from "./instanceActions.ts";
export { stateTone } from "./stateTone.ts";
export { copyText, selectElementText } from "./clipboard.ts";

// The console's UI kit (Tailwind classes over the `--nano-*` tokens).
export * from "./ui.tsx";

// The wire model the data source speaks, generated from
// spec-console/console-api.yaml and the Camunda REST subset.
export type * from "./gen/types.gen.ts";
export type {
  AgentInstanceHistoryItemResult,
  AgentInstanceResult,
} from "./gen-c8/types.gen.ts";
