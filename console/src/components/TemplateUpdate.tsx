import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { updateProjectFromTemplate, type UpdatePlan } from "../gen";
import { Button } from "@nanobpm/process-explorer/ui";
import {
  planChangeCount,
  planIsClean,
  planNothingToDo,
  targetTitle,
  type BatchItemResult,
  type UpdateTarget,
} from "../lib/templateUpdate";

/// The shared "Update from template" flow, used by the projects list, the IDE
/// workspace and the extensions reverse-index so the dry-run → review → apply
/// behaviour (and its modals) lives in exactly one place. Callers:
///   - render {modals} once,
///   - call startUpdate(target) from a badge/button,
///   - optionally drive an "Update all" batch with applyBatch(targets),
///   - pass onApplied to refresh their own view after a successful write.
export function useTemplateUpdate(opts?: {
  onApplied?: (name: string) => void | Promise<void>;
}): {
  startUpdate: (target: UpdateTarget) => Promise<void>;
  applyBatch: (targets: UpdateTarget[]) => Promise<BatchItemResult[]>;
  previewName: string | null;
  busy: boolean;
  error: string | null;
  clearError: () => void;
  modals: ReactNode;
} {
  const onApplied = opts?.onApplied;
  // The full diff/plan review modal (conflicts, applied result, or a stale
  // "nothing to do"); and the lightweight Continue/Cancel confirm for a clean
  // plan. Only one is ever open at a time.
  const [reviewPlan, setReviewPlan] = useState<{
    target: UpdateTarget;
    plan: UpdatePlan;
    takeTheirs: string[];
  } | null>(null);
  const [confirm, setConfirm] = useState<{
    target: UpdateTarget;
    plan: UpdatePlan;
  } | null>(null);
  const [busy, setBusy] = useState(false);
  // The project whose dry run is in flight — drives an inline spinner on the
  // clicked affordance while the preview round-trips to npm.
  const [previewName, setPreviewName] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const clearError = useCallback(() => setError(null), []);

  const dryRun = useCallback(
    async (target: UpdateTarget, takeTheirs?: string[]): Promise<UpdatePlan> =>
      (
        await updateProjectFromTemplate({
          path: { name: target.name },
          body: { apply: false, takeTheirs },
          throwOnError: true,
        })
      ).data,
    [],
  );

  const apply = useCallback(
    async (target: UpdateTarget, takeTheirs?: string[]): Promise<UpdatePlan> =>
      (
        await updateProjectFromTemplate({
          path: { name: target.name },
          body: { apply: true, takeTheirs },
          throwOnError: true,
        })
      ).data,
    [],
  );

  /// Begin an update: run the dry run, then route by outcome — a clean plan
  /// (changes, no conflicts) gets the one-tap confirm; conflicts or a stale
  /// "nothing to do" open the full review modal. Nothing is written yet.
  const startUpdate = useCallback(
    async (target: UpdateTarget) => {
      setBusy(true);
      setPreviewName(target.name);
      // Clear any stale error / modal state from a prior attempt so a new
      // update never opens on top of a leftover banner or dialog.
      setError(null);
      setConfirm(null);
      setReviewPlan(null);
      try {
        const plan = await dryRun(target);
        if (planIsClean(plan)) {
          setConfirm({ target, plan });
        } else {
          setReviewPlan({ target, plan, takeTheirs: [] });
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(false);
        setPreviewName(null);
      }
    },
    [dryRun],
  );

  /// Apply a single update (a snapshot is taken server-side first), show the
  /// applied result in the review modal, and let the caller refresh.
  /// `takeTheirs` carries the conflicts the user chose to resolve take-upstream.
  const applyOne = useCallback(
    async (target: UpdateTarget, takeTheirs: string[] = []) => {
      setBusy(true);
      setError(null);
      try {
        const plan = await apply(target, takeTheirs);
        setConfirm(null);
        setReviewPlan({ target, plan, takeTheirs });
        await onApplied?.(target.name);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(false);
      }
    },
    [apply, onApplied],
  );

  /// Resolve some/all conflicts take-upstream and re-run the dry run so the
  /// review modal re-renders the resulting plan (the chosen files move from
  /// "Conflicts" to "Updated"). Nothing is written yet — the user still applies.
  const resolveConflicts = useCallback(
    async (target: UpdateTarget, takeTheirs: string[]) => {
      setBusy(true);
      setError(null);
      try {
        const plan = await dryRun(target, takeTheirs);
        setReviewPlan({ target, plan, takeTheirs });
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(false);
      }
    },
    [dryRun],
  );

  /// Sequentially apply an update to each target, stopping at the first project
  /// left with conflicts (which is opened in the review modal for the user to
  /// resolve). Returns the per-project results so the caller can summarise the
  /// batch. Errors abort the run the same way conflicts do.
  const applyBatch = useCallback(
    async (targets: UpdateTarget[]): Promise<BatchItemResult[]> => {
      const results: BatchItemResult[] = [];
      setBusy(true);
      setError(null);
      try {
        for (const target of targets) {
          setPreviewName(target.name);
          try {
            const plan = await apply(target);
            results.push({ name: target.name, plan });
            await onApplied?.(target.name);
            if (plan.conflicts.length > 0) {
              setReviewPlan({ target, plan, takeTheirs: [] });
              break;
            }
          } catch (e) {
            const msg = e instanceof Error ? e.message : String(e);
            results.push({ name: target.name, error: msg });
            setError(msg);
            break;
          }
        }
      } finally {
        setBusy(false);
        setPreviewName(null);
      }
      return results;
    },
    [apply, onApplied],
  );

  const modals = useMemo(
    () => (
      <>
        {confirm && (
          <ConfirmUpdateModal
            target={confirm.target}
            plan={confirm.plan}
            busy={busy}
            onConfirm={() => void applyOne(confirm.target)}
            onCancel={() => {
              if (!busy) setConfirm(null);
            }}
          />
        )}
        {reviewPlan && (
          <UpdatePlanModal
            target={reviewPlan.target}
            plan={reviewPlan.plan}
            busy={busy}
            onApply={() =>
              void applyOne(reviewPlan.target, reviewPlan.takeTheirs)
            }
            onTakeUpstream={(paths) =>
              void resolveConflicts(
                reviewPlan.target,
                Array.from(new Set([...reviewPlan.takeTheirs, ...paths])),
              )
            }
            onClose={() => {
              // Ignore close while an apply is in flight: the pending request
              // resolves into setReviewPlan(...) and would otherwise race the UI
              // by reopening the modal after the user dismissed it.
              if (!busy) setReviewPlan(null);
            }}
          />
        )}
      </>
    ),
    [confirm, reviewPlan, busy, applyOne, resolveConflicts],
  );

  return {
    startUpdate,
    applyBatch,
    previewName,
    busy,
    error,
    clearError,
    modals,
  };
}

/// The lightweight Continue/Cancel confirm for a clean plan (changes, no
/// conflicts), so the user gets a one-tap confirmation instead of the full diff
/// modal. A pre-update snapshot is always taken server-side before anything is
/// written.
export function ConfirmUpdateModal({
  target,
  plan,
  busy,
  onConfirm,
  onCancel,
}: {
  target: UpdateTarget;
  plan: UpdatePlan;
  busy: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const title = targetTitle(target);
  const changes = planChangeCount(plan);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) onCancel();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onCancel, busy]);
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onCancel}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="confirm-update-title"
        className="w-full max-w-md rounded-lg border border-edge bg-panel p-5 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id="confirm-update-title" className="text-sm font-semibold text-fg">
          Update “{title}”?
        </h2>
        <p className="mt-2 text-sm text-fg-faint">
          This will update your local project to the latest extension ({changes}{" "}
          file{changes === 1 ? "" : "s"}
          {plan.toVersion ? `, v${plan.toVersion}` : ""}). A snapshot of your
          current project is saved first as a restore point (note: rolling back
          restores changed files but won't remove newly-added ones).
        </p>
        <div className="mt-4 flex items-center justify-end gap-2">
          <Button variant="ghost" onClick={onCancel} disabled={busy}>
            Cancel
          </Button>
          <Button onClick={onConfirm} disabled={busy}>
            {busy ? "Updating…" : "Continue"}
          </Button>
        </div>
      </div>
    </div>
  );
}

/// The review modal for "Update from template": shows the overlay plan grouped
/// by outcome (new / updated / merged / conflicts / kept), and applies the safe
/// subset on confirm. Conflicts can be resolved *take-upstream* (discard local,
/// write the incoming pack file) individually or in bulk via `onTakeUpstream` —
/// which re-runs the dry run so the resolved plan re-renders before applying.
export function UpdatePlanModal({
  target,
  plan,
  busy,
  onApply,
  onTakeUpstream,
  onClose,
}: {
  target: UpdateTarget;
  plan: UpdatePlan;
  busy: boolean;
  onApply: () => void;
  onTakeUpstream?: (paths: string[]) => void;
  onClose: () => void;
}) {
  const title = targetTitle(target);
  const changes = planChangeCount(plan);
  const nothingToDo = planNothingToDo(plan);
  // Conflicts can be resolved take-upstream only before applying and only when
  // the flow wired a resolver (the batch/observe callers may not).
  const canResolve = !plan.applied && !!onTakeUpstream;
  // Close on Escape for keyboard users, but not while an apply is in flight.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, busy]);
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="update-plan-title"
        className="flex max-h-[85vh] w-full max-w-2xl flex-col overflow-hidden rounded-lg border border-edge bg-panel shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-edge px-4 py-2.5">
          <h2 id="update-plan-title" className="text-sm font-semibold text-fg">
            Update “{title}” from template
          </h2>
          <button
            type="button"
            onClick={onClose}
            disabled={busy}
            className="rounded p-1 text-fg-faint hover:bg-hover disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-transparent"
            title="Close"
            aria-label="Close"
          >
            ✕
          </button>
        </div>
        <div className="min-h-0 flex-1 space-y-3 overflow-auto p-4 text-sm">
          <p className="text-fg-faint">
            {plan.pack}
            {plan.fromVersion ? ` v${plan.fromVersion}` : ""} →
            {plan.toVersion ? ` v${plan.toVersion}` : " latest"}
            {plan.applied && (
              <span className="ml-2 font-semibold text-ok">
                {plan.versionBumped ? "✓ applied" : "✓ applied (version kept)"}
              </span>
            )}
          </p>
          {plan.conflicts.length > 0 && (
            <div className="space-y-2 rounded-md border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
              <p>
                {plan.conflicts.length} file
                {plan.conflicts.length === 1 ? "" : "s"} changed both upstream
                and locally. Keep them and they stay unwritten (and the version
                stays pinned), or <strong>take upstream</strong> to discard your
                local changes and match the pack.
              </p>
              {!plan.applied && canResolve && (
                <Button
                  variant="ghost"
                  onClick={() => onTakeUpstream?.(plan.conflicts)}
                  disabled={busy}
                  className="!border-danger/50 !text-danger"
                >
                  {busy
                    ? "Resolving…"
                    : `Take upstream for all ${plan.conflicts.length} conflict${
                        plan.conflicts.length === 1 ? "" : "s"
                      }`}
                </Button>
              )}
            </div>
          )}
          {plan.applied && plan.checkpoint && (
            <p className="rounded-md border border-edge bg-bg-subtle px-3 py-2 text-xs text-fg-faint">
              A snapshot of your project was saved to{" "}
              <code className="text-fg-muted">{plan.checkpoint}</code> before
              this update. To undo, copy its contents back over the project —
              this restores changed files but won't remove any files the update
              newly added, so delete those by hand for a full revert.
            </p>
          )}
          {plan.applied && plan.checkpointWarning && (
            <p className="rounded-md border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
              ⚠ {plan.checkpointWarning}
            </p>
          )}
          <PlanGroup label="New files" tone="ok" items={plan.create} />
          <PlanGroup label="Updated" tone="accent" items={plan.overwrite} />
          <PlanGroup
            label="Merged (your edits kept)"
            tone="accent"
            items={plan.merged}
          />
          <PlanGroup
            label={canResolve ? "Conflicts" : "Conflicts (skipped)"}
            tone="danger"
            items={plan.conflicts}
            onTakeItem={
              canResolve ? (path) => onTakeUpstream?.([path]) : undefined
            }
            actionBusy={busy}
          />
          <PlanGroup label="Preserved" tone="muted" items={plan.preserved} />
          <PlanGroup
            label="Kept (local only)"
            tone="muted"
            items={plan.orphans}
          />
          {plan.postUpdate && (
            <div className="rounded-md border border-edge bg-bg-subtle px-3 py-2 text-xs">
              <p className="font-semibold text-fg">
                {plan.postUpdate.installedDeps || plan.postUpdate.generated
                  ? "✓ Project refreshed"
                  : "Project refresh"}
              </p>
              <ul className="mt-1 space-y-0.5 text-fg-faint">
                {plan.postUpdate.installedDeps && (
                  <li>Reinstalled npm dependencies.</li>
                )}
                {plan.postUpdate.generated && (
                  <li>Regenerated app artifacts (urban gen).</li>
                )}
                {!plan.postUpdate.installedDeps &&
                  !plan.postUpdate.generated &&
                  (plan.postUpdate.warnings?.length ?? 0) === 0 && (
                    <li>
                      Already up to date — nothing to reinstall or regenerate.
                    </li>
                  )}
              </ul>
              {(plan.postUpdate.warnings?.length ?? 0) > 0 && (
                <ul className="mt-1 space-y-0.5 text-danger">
                  {plan.postUpdate.warnings?.map((w, i) => (
                    <li key={`${i}-${w}`}>⚠ {w}</li>
                  ))}
                </ul>
              )}
            </div>
          )}
          {nothingToDo && (
            <p className="text-fg-faint">
              This project is already up to date with the template.
            </p>
          )}
        </div>
        <div className="flex items-center justify-end gap-2 border-t border-edge px-4 py-2.5">
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            {plan.applied ? "Close" : "Cancel"}
          </Button>
          {!plan.applied && (
            <Button onClick={onApply} disabled={busy || changes === 0}>
              {busy
                ? "Applying…"
                : plan.conflicts.length > 0
                  ? `Apply ${changes} safe change${changes === 1 ? "" : "s"}`
                  : "Apply update"}
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}

/// One labelled bucket of the update plan; renders nothing when empty. When
/// `onTakeItem` is supplied (the Conflicts bucket, pre-apply), each file gets a
/// "Take upstream" action that resolves just that file take-upstream.
function PlanGroup({
  label,
  items,
  tone,
  onTakeItem,
  actionBusy,
}: {
  label: string;
  items: string[];
  tone: "ok" | "accent" | "danger" | "muted";
  onTakeItem?: (path: string) => void;
  actionBusy?: boolean;
}) {
  if (items.length === 0) return null;
  const toneClass = {
    ok: "text-ok",
    accent: "text-accent",
    danger: "text-danger",
    muted: "text-fg-muted",
  }[tone];
  return (
    <div>
      <div
        className={`mb-1 text-xs font-semibold uppercase tracking-wider ${toneClass}`}
      >
        {label} ({items.length})
      </div>
      <ul className="space-y-0.5 font-mono text-xs text-fg-faint">
        {items.map((f) => (
          <li
            key={f}
            className="flex items-center justify-between gap-2"
            title={f}
          >
            <span className="truncate">{f}</span>
            {onTakeItem && (
              <button
                type="button"
                onClick={() => onTakeItem(f)}
                disabled={actionBusy}
                className="shrink-0 rounded px-1.5 py-0.5 font-sans text-[10px] font-medium text-danger hover:bg-danger/10 disabled:cursor-not-allowed disabled:opacity-50"
                title="Discard your local changes and take the upstream version"
              >
                Take upstream
              </button>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
