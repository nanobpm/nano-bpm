// Discovery shim for the TraceTimeline component tests. Node's default
// `--test` glob (`**/*.test.ts`, …) does not include `.test.tsx`, so the
// regressions in `TraceTimeline.test.tsx` would be silently skipped by
// `npm test` and console CI without this entry point.
import "./TraceTimeline.test.tsx";
