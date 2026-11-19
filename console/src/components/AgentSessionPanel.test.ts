// Discovery shim for the AgentSessionPanel component tests. Node's default
// `--test` glob (`**/*.test.ts`, …) does not include `.test.tsx`, so the
// pending-state and playhead regression guards in `AgentSessionPanel.test.tsx`
// would be silently skipped by `npm test` and console CI without this entry
// point. Importing the suite registers its tests in this (discoverable)
// file's run.
import "./AgentSessionPanel.test.tsx";
