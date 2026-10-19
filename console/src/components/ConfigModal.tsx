import { useState } from "react";
import type { ProjectConfig } from "../gen";
import { Button, inputClass, Modal } from "./ui";

const inputCls = `mt-1 w-full ${inputClass}`;

/// "Configure project" modal (extracted from ProjectWorkspace so the
/// configuration-save gate is directly testable — issue #1350). The caller
/// injects the actual write (`onSave`) so the modal carries no API dependency:
/// ProjectWorkspace passes a closure around the generated `saveProjectConfig`
/// client, and the unit test passes a spy.
export function ConfigModal({
  config,
  appRunning,
  onClose,
  onSaved,
  onSave,
}: {
  config: ProjectConfig;
  /** Mirrors the server's 409 `app_running` guard: config is read-only while
   * the app runs (issue #1350, same gate as the file editor's Save). */
  appRunning: boolean;
  onClose: () => void;
  onSaved: (cfg: ProjectConfig) => void;
  /** Performs the configuration write; resolves with the saved config. */
  onSave: (cfg: ProjectConfig) => Promise<ProjectConfig>;
}) {
  const [deployTarget, setDeployTarget] = useState(config.deployTarget);
  const [main, setMain] = useState(config.main);
  const [desc, setDesc] = useState(config.description);
  // Env editor state: array of {key,value} rows, kept ordered so that adding
  // a fresh empty row doesn't reshuffle the user's typing. We serialise back
  // to an object on save, dropping rows with an empty key.
  const [envRows, setEnvRows] = useState<Array<{ key: string; value: string }>>(
    () =>
      Object.entries(config.env ?? {})
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, value]) => ({ key, value })),
  );
  const [busy, setBusy] = useState(false);

  const addEnvRow = () =>
    setEnvRows((rows) => [...rows, { key: "", value: "" }]);
  const removeEnvRow = (idx: number) =>
    setEnvRows((rows) => rows.filter((_, i) => i !== idx));
  const setEnvRow = (
    idx: number,
    patch: Partial<{ key: string; value: string }>,
  ) =>
    setEnvRows((rows) =>
      rows.map((r, i) => (i === idx ? { ...r, ...patch } : r)),
    );

  const save = async () => {
    if (appRunning) return;
    setBusy(true);
    try {
      const env: Record<string, string> = {};
      for (const { key, value } of envRows) {
        const k = key.trim();
        if (k) env[k] = value;
      }
      const cfg = await onSave({
        ...config,
        description: desc,
        deployTarget,
        main,
        env,
      });
      onSaved(cfg);
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  return (
    <Modal title="Configure project" onClose={onClose}>
      <label className="block text-xs uppercase tracking-wider text-fg-faint">
        Description
      </label>
      <input
        value={desc}
        onChange={(e) => setDesc(e.target.value)}
        className={inputCls}
      />
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">
        Deploy target
      </label>
      <input
        value={deployTarget}
        onChange={(e) => setDeployTarget(e.target.value)}
        className={inputCls}
        placeholder="http://localhost:8080"
      />
      <p className="mt-1 text-[11px] text-fg-faint">
        REST API at &lt;target&gt;/v2; the Falcon protocol is dialled here too.
      </p>
      <label className="mt-3 block text-xs uppercase tracking-wider text-fg-faint">
        Entry point
      </label>
      <input
        value={main}
        onChange={(e) => setMain(e.target.value)}
        className={inputCls}
        placeholder="main.ts"
      />
      <div className="mt-3 flex items-center justify-between">
        <label className="block text-xs uppercase tracking-wider text-fg-faint">
          Environment variables
        </label>
        <button
          type="button"
          onClick={addEnvRow}
          className="text-[11px] text-accent hover:underline"
        >
          + Add
        </button>
      </div>
      <p className="mt-1 text-[11px] text-fg-faint">
        Passed to every Run/Compile spawn. Run-config env overrides these.
        Example:{" "}
        <span className="font-mono">
          CAMUNDA_REST_ADDRESS=http://localhost:8081
        </span>
        .
      </p>
      {envRows.length === 0 ? (
        <p className="mt-2 text-[11px] text-fg-faint italic">
          No env vars set.
        </p>
      ) : (
        <div className="mt-2 space-y-1">
          {envRows.map((row, i) => (
            <div key={i} className="flex gap-2">
              <input
                value={row.key}
                onChange={(e) => setEnvRow(i, { key: e.target.value })}
                placeholder="KEY"
                className={`${inputClass} font-mono w-2/5`}
              />
              <input
                value={row.value}
                onChange={(e) => setEnvRow(i, { value: e.target.value })}
                placeholder="value"
                className={`${inputClass} font-mono min-w-0 flex-1`}
              />
              <button
                type="button"
                onClick={() => removeEnvRow(i)}
                aria-label="Remove"
                className="px-2 text-fg-faint hover:text-fg"
              >
                ×
              </button>
            </div>
          ))}
        </div>
      )}
      {appRunning && (
        <p className="mt-4 text-xs text-fg-faint" role="status">
          Stop the app before changing its configuration. The app reads its
          environment when it starts.
        </p>
      )}
      <div className="mt-5 flex justify-end gap-2">
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="primary"
          onClick={() => void save()}
          disabled={busy || appRunning}
          title={appRunning ? "Stop the app before saving changes" : undefined}
        >
          {busy ? "Saving…" : "Save"}
        </Button>
      </div>
    </Modal>
  );
}
