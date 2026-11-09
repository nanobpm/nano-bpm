import { useCallback, useEffect, useState } from "react";
import { browseFilesystem, type BrowseResult } from "../lib/api";
import { Badge, Button } from "@nanobpm/process-explorer/ui";

/// A modal directory browser for the Import-by-reference flow. It walks the
/// host filesystem via the loopback-only `/console/api/fs/browse` endpoint and
/// calls `onPick` with the absolute path of the chosen folder. Only rendered
/// when the console is viewed over localhost (see `isLocalhost`).
export default function DirectoryPicker({
  initialPath,
  onPick,
  onClose,
}: {
  initialPath?: string;
  onPick: (path: string) => void;
  onClose: () => void;
}) {
  const [data, setData] = useState<BrowseResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async (path?: string) => {
    setLoading(true);
    setError(null);
    try {
      setData(await browseFilesystem(path));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load(initialPath);
  }, [load, initialPath]);

  // Close on Escape for keyboard users.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="directory-picker-title"
        className="flex max-h-[80vh] w-full max-w-2xl flex-col overflow-hidden rounded-lg border border-edge-strong bg-raised shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-edge px-4 py-3">
          <h2
            id="directory-picker-title"
            className="text-sm font-semibold text-fg"
          >
            Choose an app folder
          </h2>
          <button
            className="rounded p-1 text-fg-muted hover:bg-hover hover:text-fg"
            aria-label="Close"
            onClick={onClose}
          >
            ✕
          </button>
        </div>

        {/* Current path + up-a-level */}
        <div className="flex items-center gap-2 border-b border-edge bg-inset px-4 py-2">
          <Button
            variant="secondary"
            size="sm"
            disabled={loading || !data?.parent}
            onClick={() => data?.parent && void load(data.parent)}
            title="Up one level"
          >
            ↑ Up
          </Button>
          <code
            className="min-w-0 flex-1 truncate text-xs text-fg-muted"
            title={data?.path}
          >
            {data?.path ?? "…"}
          </code>
        </div>

        {error && (
          <div className="border-b border-danger/40 bg-danger/10 px-4 py-2 text-sm text-danger">
            {error}
          </div>
        )}

        {/* Directory listing */}
        <div className="min-h-[12rem] flex-1 overflow-y-auto px-2 py-2">
          {loading ? (
            <div className="py-10 text-center text-sm text-fg-faint">
              Loading…
            </div>
          ) : data && data.entries.length === 0 ? (
            <div className="py-10 text-center text-sm text-fg-faint">
              No sub-folders here.
            </div>
          ) : (
            <ul className="flex flex-col">
              {data?.entries.map((e) => (
                <li key={e.path}>
                  <button
                    className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm text-fg hover:bg-hover"
                    onClick={() => void load(e.path)}
                    title={`Open ${e.path}`}
                  >
                    <span className="text-fg-faint">📁</span>
                    <span className="min-w-0 flex-1 truncate">{e.name}</span>
                    {e.isNanoApp && <Badge tone="accent">Nano app</Badge>}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        {/* Footer: select the current directory */}
        <div className="flex items-center justify-between gap-3 border-t border-edge px-4 py-3">
          <span className="text-xs text-fg-faint">
            {data?.isNanoApp
              ? "This folder is a Nano app — ready to import."
              : "Open a folder, or select one that contains nano.app.json."}
          </span>
          <div className="flex gap-2">
            <Button variant="secondary" onClick={onClose}>
              Cancel
            </Button>
            <Button
              variant="primary"
              disabled={loading || !data?.isNanoApp}
              onClick={() => data?.isNanoApp && onPick(data.path)}
            >
              Select this folder
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
