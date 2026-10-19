// Discovery shim for the ConfigModal component tests. Node's default
// `--test` glob (`**/*.test.ts`, …) does not include `.test.tsx`, so the
// configuration-save-gate regression in `ConfigModal.test.tsx` would be
// silently skipped by `npm test` and console CI without this entry point.
// Importing the suite registers its tests in this (discoverable) file's run.
import "./ConfigModal.test.tsx";
