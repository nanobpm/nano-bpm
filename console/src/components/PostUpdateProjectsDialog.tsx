import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  runProject,
  stopProject,
  updateProjectFromTemplate,
  type ProjectSummary,
} from "../gen";
import { Button, Spinner } from "@nanobpm/process-explorer/ui";
import {
  POST_UPDATE_PROJECTS_MESSAGE,
  runProjectUpdate,
  summarizeProjectUpdates,
  type ProjectUpdateOps,
  type ProjectUpdateOutcome,
  type ProjectUpdatePhase,
} from "../lib/projectUpdateRun";

export { POST_UPDATE_PROJECTS_MESSAGE };

/// The default lifecycle operations, wired to the console SDK. Split out (and
/// overridable) so the orchestration can be exercised without a live gateway.
const liveOps: ProjectUpdateOps = {
  stop: async (name) => {
    await stopProject({ path: { name }, throwOnError: true });
  },
  apply: async (name) =>
    (
      await updateProjectFromTemplate({
        path: { name },
        // No `takeTheirs`/`resolveConflicts`: reuse the existing data-preserving
        // overlay untouched — the safe subset is written and conflicts are left
        // unwritten for manual resolution, never silently overwritten.
        body: { apply: true },
        throwOnError: true,
      })
    ).data,
  start: async (name) => {
    await runProject({ path: { name }, throwOnError: true });
  },
};

/// The post-extension-update dialog (issue #1143): after a successful extension
/// update, offer the projects scaffolded from that extension that can now be
/// updated, and run each selected project through the stop → update → restart
/// lifecycle that preserves its prior running state.
///
/// The eligibility set (`projects`) is computed by the caller
/// ([`projectsNewlyUpdatable`]); this component owns selection, confirmation and
/// the per-project run, reusing the shared `updateProjectFromTemplate` overlay
/// via [`runProjectUpdate`] rather than reimplementing template updates.
export function PostUpdateProjectsDialog({
  projects,
  onClose,
  onApplied,
  ops = liveOps,
}: {
  projects: ProjectSummary[];
  /// Dismiss the dialog. Cancelling/dismissing before running leaves every
  /// project unchanged; the completed extension update stays installed.
  onClose: () => void;
  /// Called once after the batch completes so the caller can refresh its reverse
  /// index (the updated projects are no longer eligible).
  onApplied?: () => void | Promise<void>;
  /// Injectable lifecycle ops — defaults to the live SDK-backed operations.
  ops?: ProjectUpdateOps;
}) {
  const [selected, setSelected] = useState<Set<string>>(
    () => new Set(projects.map((p) => p.name)),
  );
  const [phase, setPhase] = useState<"select" | "running" | "done">("select");
  const [progress, setProgress] = useState<Record<string, ProjectUpdatePhase>>(
    {},
  );
  const [outcomes, setOutcomes] = useState<ProjectUpdateOutcome[]>([]);
  // Synchronous in-flight guard: `phase` re-renders asynchronously, so a fast
  // double-click could call `run()` twice before it flips to "running". A ref is
  // updated synchronously, so the second call bails before starting a parallel
  // batch over the same projects.
  const runningRef = useRef(false);

  const running = phase === "running";
  const outcomeByName = useMemo(() => {
    const m = new Map<string, ProjectUpdateOutcome>();
    for (const o of outcomes) m.set(o.name, o);
    return m;
  }, [outcomes]);

  // Escape dismisses only while the user is still choosing — never mid-run, so a
  // stray keypress can't abandon the UI while stops/updates/restarts are live.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && phase !== "running") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, phase]);

  const toggle = useCallback((name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }, []);

  const run = useCallback(async () => {
    // Guard against duplicate submissions: a second click while a run is in
    // flight would start a parallel batch over the same projects. The ref is set
    // synchronously so the guard holds before React re-renders `phase`.
    if (runningRef.current) return;
    const targets = projects.filter((p) => selected.has(p.name));
    if (targets.length === 0) return;
    runningRef.current = true;
    setPhase("running");
    setOutcomes([]);
    setProgress(
      Object.fromEntries(targets.map((p) => [p.name, "pending" as const])),
    );
    const results: ProjectUpdateOutcome[] = [];
    try {
      for (const p of targets) {
        const outcome = await runProjectUpdate(
          { name: p.name, running: p.running },
          ops,
          (ph) => setProgress((prev) => ({ ...prev, [p.name]: ph })),
        );
        results.push(outcome);
        setOutcomes([...results]);
      }
      await onApplied?.();
    } finally {
      // Always leave the in-flight state once the batch has run, even if the
      // caller's `onApplied` refresh hook rejects — otherwise the dialog would
      // stay stuck in "running" with Cancel/outside-click disabled and no way
      // out. The per-project outcomes are already recorded, so "done" still
      // shows the (already-applied) results.
      runningRef.current = false;
      setPhase("done");
    }
  }, [projects, selected, ops, onApplied]);

  const summary = useMemo(
    () => (phase === "done" ? summarizeProjectUpdates(outcomes) : null),
    [phase, outcomes],
  );

  const selectedCount = selected.size;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={() => {
        if (phase !== "running") onClose();
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="post-update-projects-title"
        className="flex max-h-[85vh] w-full max-w-lg flex-col overflow-hidden rounded-lg border border-edge bg-panel shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="border-b border-edge px-4 py-2.5">
          <h2
            id="post-update-projects-title"
            className="text-sm font-semibold text-fg"
          >
            Update projects from the updated extension
          </h2>
        </div>
        <div className="min-h-0 flex-1 space-y-3 overflow-auto p-4 text-sm">
          <p className="text-fg-faint">{POST_UPDATE_PROJECTS_MESSAGE}</p>
          <ul className="grid gap-1.5">
            {projects.map((p) => {
              const title = p.displayName ?? p.name;
              const from = p.scaffoldedFrom?.version;
              const isSelected = selected.has(p.name);
              const ph = progress[p.name];
              const outcome = outcomeByName.get(p.name);
              return (
                <li
                  key={p.name}
                  className="flex items-center justify-between gap-3 rounded border border-edge bg-bg-subtle px-2.5 py-1.5"
                >
                  <label className="flex min-w-0 flex-1 items-center gap-2">
                    <input
                      type="checkbox"
                      checked={isSelected}
                      disabled={phase !== "select"}
                      onChange={() => toggle(p.name)}
                      aria-label={`Update ${title}`}
                    />
                    <span className="min-w-0">
                      <span
                        className="block truncate text-sm text-fg"
                        title={p.name}
                      >
                        {title}
                      </span>
                      <span className="block text-[11px] text-fg-faint">
                        {from ? `v${from}` : "unversioned"}
                        {p.latestVersion ? ` → v${p.latestVersion}` : ""}
                        {p.running ? " · running" : " · stopped"}
                      </span>
                    </span>
                  </label>
                  <span className="shrink-0 text-[11px]">
                    <ProjectRunState phase={ph} outcome={outcome} />
                  </span>
                </li>
              );
            })}
          </ul>
          {summary && (
            <div className="space-y-1 border-t border-edge pt-2 text-[11px]">
              {summary.updated.length > 0 && (
                <p className="text-ok">
                  ✓ Updated {summary.updated.join(", ")}.
                </p>
              )}
              {summary.conflicts.length > 0 && (
                <p className="text-danger">
                  ⚠ Conflicts in {summary.conflicts.join(", ")} — resolve by
                  hand (your local changes were kept, not overwritten).
                </p>
              )}
              {summary.failed.length > 0 && (
                <p className="text-danger">
                  ⚠ Failed: {summary.failed.join(", ")}.
                </p>
              )}
            </div>
          )}
        </div>
        <div className="flex items-center justify-end gap-2 border-t border-edge px-4 py-2.5">
          {phase === "done" ? (
            <Button onClick={onClose}>Close</Button>
          ) : (
            <>
              <Button variant="ghost" onClick={onClose} disabled={running}>
                Cancel
              </Button>
              <Button
                onClick={() => void run()}
                disabled={running || selectedCount === 0}
                aria-busy={running}
              >
                {running ? (
                  <>
                    <Spinner /> Updating…
                  </>
                ) : (
                  `Update ${selectedCount} project${selectedCount === 1 ? "" : "s"}`
                )}
              </Button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

/// The right-aligned status cell for one project: a live phase label while its
/// lifecycle runs, then a terminal outcome badge.
function ProjectRunState({
  phase,
  outcome,
}: {
  phase?: ProjectUpdatePhase;
  outcome?: ProjectUpdateOutcome;
}) {
  if (outcome) {
    switch (outcome.status) {
      case "updated":
        return (
          <span className="text-ok">
            ✓ {outcome.restarted ? "updated · restarted" : "updated"}
          </span>
        );
      case "conflicts":
        return <span className="text-warn">⚠ conflicts</span>;
      case "stop-failed":
        return (
          <span className="text-danger" title={outcome.error ?? undefined}>
            ⚠ stop failed
          </span>
        );
      case "update-failed":
        return (
          <span className="text-danger" title={outcome.error ?? undefined}>
            ⚠ update failed
          </span>
        );
      case "restart-failed":
        return (
          <span className="text-danger" title={outcome.error ?? undefined}>
            ⚠ restart failed
          </span>
        );
    }
  }
  switch (phase) {
    case "pending":
      return <span className="text-fg-faint">queued…</span>;
    case "stopping":
      return (
        <span className="inline-flex items-center gap-1 text-fg-faint">
          <Spinner /> stopping…
        </span>
      );
    case "updating":
      return (
        <span className="inline-flex items-center gap-1 text-fg-faint">
          <Spinner /> updating…
        </span>
      );
    case "restarting":
      return (
        <span className="inline-flex items-center gap-1 text-fg-faint">
          <Spinner /> restarting…
        </span>
      );
    default:
      return null;
  }
}
