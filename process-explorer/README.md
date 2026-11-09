# @nanobpm/process-explorer

The nano console's Process Explorer as React components:

- the instance list with its state filters;
- the instance detail, which has the BPMN diagram with live tokens, variables, incidents, the execution trace, called and parent instances, and agent sessions.

The components don't call any backend directly. They read and act through an
`ExplorerDataSource` that the host provides. The console's source wraps its
generated `/v2` clients (`console/src/lib/explorerSource.ts`). Other hosts, such
as an embedded engine or a local daemon, implement the same interface.

```tsx
import { Explorer, ExplorerDataProvider } from "@nanobpm/process-explorer";

<QueryClientProvider client={queryClient}>
  <ExplorerDataProvider source={mySource}>
    <Explorer searchParams={params} setSearchParams={setParams} />
  </ExplorerDataProvider>
</QueryClientProvider>;
```

## The data source

Reads are required: `listInstances`, `getInstance`, `getTrace` and
`getProcessXml`. Everything else is optional, and the UI hides an affordance
when its method is absent:

- `cancelInstance`, `suspendInstance`, `resumeInstance`, `resolveIncident` and
  `setVariables` drive the action buttons and the variable editor.
- `subscribe(onChange)` pushes "something changed" so open views refetch.
  Without it, views refresh only on navigation or reload.
- `agents` (`listAgentInstances`, `getAgentHistory`) powers the agent-session
  badges and scrubber.

The wire types are generated from `spec-console/console-api.yaml` and
`spec/rest-api.yaml` into `src/gen` and `src/gen-c8`. This happens as part of
the console's `npm run gen:api`, and they are types only. Don't edit them by hand.

## Entry points

| Import                                 | What                                                                 |
| -------------------------------------- | -------------------------------------------------------------------- |
| `@nanobpm/process-explorer`            | Explorer, InstanceDetail, BpmnViewer, TraceTimeline, the data source |
| `@nanobpm/process-explorer/ui`         | The console UI kit (Button, Badge, Modal, …)                         |
| `@nanobpm/process-explorer/filters`    | URL filter helpers, which have no React or bpmn-js deps              |
| `@nanobpm/process-explorer/clipboard`  | `copyText`, `selectElementText`                                      |
| `@nanobpm/process-explorer/styles.css` | Handwritten component CSS every host must import                     |

## How it is built

The package is consumed as TypeScript source and has no build step or install
of its own:

- **Dependency:** the console depends on it as `file:../process-explorer`.
- **Peer dependencies:** the console supplies them (React, TanStack Query, bpmn-js).
- **Vite:** the console's Vite config aliases each `exports` entry to the real path.
- **Styling:** Tailwind scans `src/` through an `@source` in `console/src/index.css`, and the theme comes from the host's tokens. The handwritten component rules (diagram markers, agent badges, touch targets, card grid, safe-area helpers) ship in `src/styles.css`: every host must import `@nanobpm/process-explorer/styles.css` (the console imports it from `index.css`).
- **Query cache:** `ExplorerDataProvider` gives each data source its own TanStack Query cache (inheriting the host's default options), so two sources never share cached instances or diagrams.
- **Console checks:** `npm run typecheck`, `npm test` (`test:explorer`) and `npm run format:check` cover this package too.
