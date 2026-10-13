import { useCallback, useEffect, useState } from "react";

import BpmnViewer from "./BpmnViewer";
import { Button } from "./ui";
import { projectDerivedModels, type DerivedModel } from "../lib/api";

// The Derived Model panel (ADR 0045). Code-first workflow projects have no
// authored `.bpmn`; the executable BPMN is DERIVED from `workflows/*.ts` via
// `@nanobpm/workflow`'s `toBpmn`. The server overlays each model with the
// on-disk auto-laid-out `resources/processes/<id>.bpmn` (ADR 0048), so this
// read-only view renders the real diagram the code produces — the maker sees
// (and screenshots) the model without a separate modeller, code stays the
// single source of truth.

function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  return typeof e === "string" ? e : String(e);
}

export default function DerivedModelPanel({ name }: { name: string }) {
  const [models, setModels] = useState<DerivedModel[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [active, setActive] = useState(0);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const derived = await projectDerivedModels(name);
      setModels(derived);
      setActive(0);
    } catch (e) {
      setError(errMsg(e));
      setModels(null);
    } finally {
      setLoading(false);
    }
  }, [name]);

  useEffect(() => {
    void load();
  }, [load]);

  const current = models && models.length > 0 ? models[active] : null;

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex flex-wrap items-center gap-2 border-b border-edge bg-panel px-4 py-2">
        <span className="text-sm font-semibold text-fg">Derived model</span>
        {models && models.length > 1 && (
          <select
            aria-label="Select derived workflow model"
            className="rounded border border-edge bg-input px-2 py-1 text-sm text-fg"
            value={active}
            onChange={(e) => setActive(Number(e.target.value))}
          >
            {models.map((m, i) => (
              <option key={m.id} value={i}>
                {m.id} ({m.kind})
              </option>
            ))}
          </select>
        )}
        {current && models && models.length === 1 && (
          <span className="text-xs text-fg-faint">
            {current.id} · {current.kind}
          </span>
        )}
        <div className="ml-auto flex items-center gap-2">
          {loading && <span className="text-xs text-fg-faint">Deriving…</span>}
          <Button onClick={() => void load()} disabled={loading}>
            Refresh
          </Button>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-hidden">
        {error ? (
          <div className="p-6 text-sm text-danger">
            <p className="font-medium">Could not derive the model.</p>
            <p className="mt-1 whitespace-pre-wrap text-fg-muted">{error}</p>
            <p className="mt-3 text-xs text-fg-faint">
              The model is derived by running this project&apos;s{" "}
              <code>workflows/*.ts</code> through <code>@nanobpm/workflow</code>{" "}
              with Node. Make sure the Node toolchain is installed and the
              workflows type-check.
            </p>
          </div>
        ) : !models ? (
          <div className="flex h-full items-center justify-center text-sm text-fg-faint">
            {loading ? "Deriving the model…" : "No model yet."}
          </div>
        ) : models.length === 0 ? (
          <div className="flex h-full items-center justify-center px-6 text-center text-sm text-fg-faint">
            No workflows found. Export a workflow from{" "}
            <code className="mx-1">workflows/*.ts</code> (e.g.{" "}
            <code>export const prReview = defineFlow(…)</code>).
          </div>
        ) : (
          <BpmnViewer key={current?.id ?? active} xml={current?.xml ?? null} />
        )}
      </div>
    </div>
  );
}
