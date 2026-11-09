import {
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";

import { Badge, Button, Input, inputClass } from "@nanobpm/process-explorer/ui";
import {
  addConnector,
  type ConnectorInfo,
  type ConnectorKindInfo,
  type ConnectorsResponse,
  getConnectors,
} from "../gen";

// The Connectors panel — ADR 0050, the outbound mirror of the Triggers panel.
// A *connector* is a pack that ships both an element-template component (the
// design-time face) and a long-lived worker keyed by the component's
// `zeebe:taskDefinition:type`. Enabling one appends a pack-backed worker to the
// App manifest's `workers[]`; the host then supervises it. This panel lists the
// App's enabled connectors (tagged against the installable registry from
// `GET /connectors`) and drives the "Add connector" form — no hand-editing
// `nano.app.json`.

function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (typeof e === "string") return e;
  if (e && typeof e === "object") {
    const o = e as { error?: unknown; detail?: unknown };
    if (typeof o.error === "string") return o.error;
    if (typeof o.detail === "string") return o.detail;
  }
  return String(e);
}

export default function ConnectorsPanel({
  name,
  appRunning,
}: {
  name: string;
  appRunning: boolean;
}) {
  const [data, setData] = useState<ConnectorsResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [showAdd, setShowAdd] = useState(false);

  const load = useCallback(async () => {
    setLoadError(null);
    try {
      const r = await getConnectors({ path: { name }, throwOnError: true });
      setData(r.data);
    } catch (e) {
      setLoadError(errMsg(e));
    } finally {
      setLoading(false);
    }
  }, [name]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-3 border-b border-edge bg-panel px-4 py-2">
        <span className="text-xs font-semibold uppercase tracking-wider text-fg-faint">
          Connectors
        </span>
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        {loading ? (
          <div className="p-8 text-sm text-fg-faint">Loading connectors…</div>
        ) : loadError ? (
          <div className="p-8 text-sm text-danger">
            Couldn’t load connectors: {loadError}
          </div>
        ) : !data ? null : (
          <ConnectorsTab
            name={name}
            data={data}
            onReload={() => void load()}
            showAdd={showAdd}
            setShowAdd={setShowAdd}
            appRunning={appRunning}
          />
        )}
      </div>
    </div>
  );
}

function ConnectorsTab({
  name,
  data,
  onReload,
  showAdd,
  setShowAdd,
  appRunning,
}: {
  name: string;
  data: ConnectorsResponse;
  onReload: () => void;
  showAdd: boolean;
  setShowAdd: (v: boolean) => void;
  appRunning: boolean;
}) {
  const enabledTypes = useMemo(
    () => data.connectors.map((c) => c.taskType),
    [data.connectors],
  );

  return (
    <div className="flex flex-col">
      <div className="flex items-center gap-2 px-4 pt-4">
        <span className="text-xs text-fg-faint">
          {data.connectors.length} connector
          {data.connectors.length === 1 ? "" : "s"} enabled
        </span>
        <div className="flex-1" />
        <Button
          size="sm"
          variant="primary"
          disabled={data.available.length === 0 || appRunning}
          title={appRunning ? "Stop the app to add a connector" : undefined}
          onClick={() => setShowAdd(true)}
        >
          ＋ Add connector
        </Button>
      </div>

      {data.connectors.length === 0 ? (
        <div className="flex items-center justify-center p-8 text-center text-sm text-fg-faint">
          <div>
            <p className="font-medium text-fg-muted">No connectors enabled.</p>
            <p className="mt-1">
              Click{" "}
              <span className="font-medium text-fg-muted">Add connector</span>{" "}
              to enable a pack-supplied output (e.g. Slack) — its worker is
              added to <code className="text-fg-muted">nano.app.json</code> and
              supervised when the App runs (ADR 0050).
            </p>
          </div>
        </div>
      ) : (
        <div className="flex flex-col gap-4 p-4">
          {data.errors.length > 0 && (
            <div className="rounded-md border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger">
              <p className="font-semibold">Connector configuration errors</p>
              <ul className="mt-1 list-disc pl-4">
                {data.errors.map((e, i) => (
                  <li key={i}>{e}</li>
                ))}
              </ul>
            </div>
          )}

          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="border-b border-edge text-left text-xs uppercase tracking-wider text-fg-faint">
                <th className="px-2 py-1.5 font-medium">Connector</th>
                <th className="px-2 py-1.5 font-medium">Pack</th>
                <th className="px-2 py-1.5 font-medium">Connection</th>
                <th className="px-2 py-1.5 font-medium text-right">Status</th>
              </tr>
            </thead>
            <tbody>
              {data.connectors.map((c) => (
                <ConnectorRow key={c.taskType} connector={c} />
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="px-4 pb-4">
        <ConnectorRegistry available={data.available} onRefresh={onReload} />
      </div>

      {showAdd && (
        <AddConnectorDialog
          name={name}
          available={data.available}
          enabledTypes={enabledTypes}
          onClose={() => setShowAdd(false)}
          onAdded={() => {
            setShowAdd(false);
            onReload();
          }}
        />
      )}
    </div>
  );
}

function ConnectorRow({ connector }: { connector: ConnectorInfo }) {
  return (
    <tr className="border-b border-edge/50">
      <td className="px-2 py-2">
        <span className="font-mono text-xs text-fg">{connector.taskType}</span>
        {connector.displayName && (
          <span className="ml-2 text-xs text-fg-faint">
            {connector.displayName}
          </span>
        )}
      </td>
      <td className="px-2 py-2 text-xs text-fg-muted">
        {connector.connector ?? "—"}
      </td>
      <td className="px-2 py-2 font-mono text-xs text-fg-muted">
        {connector.connection ?? "—"}
      </td>
      <td className="px-2 py-2 text-right">
        {!connector.recognized ? (
          <span title="Unknown connector — a stale enablement or a not-yet-installed pack.">
            <Badge tone="danger">unrecognized</Badge>
          </span>
        ) : connector.backed ? (
          <Badge tone="info">backed</Badge>
        ) : (
          <span title="No launchable worker — the pack is uninstalled or declaration-only.">
            <Badge tone="danger">no worker</Badge>
          </span>
        )}
      </td>
    </tr>
  );
}

function ConnectorRegistry({
  available,
  onRefresh,
}: {
  available: ConnectorKindInfo[];
  onRefresh: () => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-md border border-edge">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 px-3 py-2 text-left text-xs font-medium text-fg-muted hover:bg-hover"
      >
        <span className="text-fg-faint">{open ? "▾" : "▸"}</span>
        Connector registry ({available.length})
        <span className="ml-1 font-normal text-fg-faint">
          — every installed pack's declared worker types
        </span>
        <div className="flex-1" />
        <span
          role="button"
          tabIndex={0}
          onClick={(e) => {
            e.stopPropagation();
            onRefresh();
          }}
          className="rounded px-1.5 py-0.5 text-fg-faint hover:bg-hover hover:text-fg-muted"
        >
          Refresh
        </span>
      </button>
      {open && (
        <div className="flex flex-wrap gap-2 border-t border-edge px-3 py-2">
          {available.length === 0 && (
            <span className="text-xs text-fg-faint">
              No connector packs installed — add one from the marketplace.
            </span>
          )}
          {available.map((k) => (
            <span
              key={k.taskType}
              className="inline-flex items-center gap-1.5 rounded-md border border-edge px-2 py-1 text-xs"
              title={
                k.hasComponent
                  ? "Ships a matching element-template component (design→runtime seam)"
                  : "Worker only — wire it to a hand-authored task"
              }
            >
              <span className="font-mono text-fg">{k.taskType}</span>
              {k.displayName && (
                <span className="text-fg-faint">{k.displayName}</span>
              )}
              <Badge tone={k.hasComponent ? "accent" : "info"}>
                {k.hasComponent ? "component" : "worker"}
              </Badge>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

// --- Add connector dialog ---------------------------------------------------

/** A lightweight centered modal (mirrors the Triggers panel's Add dialog). */
function Modal({
  title,
  onClose,
  children,
  footer,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  footer: ReactNode;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <div
        className="flex max-h-[85vh] w-full max-w-lg flex-col overflow-hidden rounded-lg border border-edge bg-panel shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-edge px-4 py-2.5">
          <h2 className="text-sm font-semibold text-fg">{title}</h2>
          <button
            onClick={onClose}
            className="rounded p-1 text-fg-faint hover:bg-hover"
            title="Close"
          >
            ✕
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto p-4">{children}</div>
        <div className="flex items-center justify-end gap-2 border-t border-edge px-4 py-2.5">
          {footer}
        </div>
      </div>
    </div>
  );
}

/**
 * Enable a connector as a form (the Delphi-style affordance): pick an
 * installable connector, fill its declared config fields (env-pointer
 * credentials — never inline secrets, ADR 0027 §5), name the shared connection,
 * and POST to the manifest. The server (`add_connector`) appends the pack-backed
 * worker to `workers[]` and writes the connection's env pointers.
 */
function AddConnectorDialog({
  name,
  available,
  enabledTypes,
  onClose,
  onAdded,
}: {
  name: string;
  available: ConnectorKindInfo[];
  enabledTypes: string[];
  onClose: () => void;
  onAdded: () => void;
}) {
  // Only connectors not already enabled can be added.
  const addable = useMemo(
    () => available.filter((k) => !enabledTypes.includes(k.taskType)),
    [available, enabledTypes],
  );
  const [taskType, setTaskType] = useState(addable[0]?.taskType ?? "");
  const [config, setConfig] = useState<Record<string, string>>({});
  const [connection, setConnection] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const kind = useMemo(
    () => addable.find((k) => k.taskType === taskType),
    [addable, taskType],
  );
  const fields = kind?.configFields ?? [];

  const setField = (key: string, value: string) =>
    setConfig((c) => ({ ...c, [key]: value }));

  const missingRequired = fields.some(
    (f) => f.required && !(config[f.key] ?? "").trim(),
  );
  // A connection name is required whenever config values are supplied — the
  // server writes them as env pointers onto the named connection.
  const hasConfig = fields.some((f) => (config[f.key] ?? "").trim() !== "");
  const needsConnection = hasConfig && connection.trim() === "";
  const canSubmit = taskType !== "" && !missingRequired && !needsConnection;

  const submit = useCallback(async () => {
    setBusy(true);
    setError(null);
    const cfg: Record<string, string> = {};
    for (const f of fields) {
      const v = (config[f.key] ?? "").trim();
      if (v !== "") cfg[f.key] = v;
    }
    try {
      await addConnector({
        path: { name },
        body: {
          type: taskType,
          config: Object.keys(cfg).length ? cfg : undefined,
          connection: connection.trim() || undefined,
        },
        throwOnError: true,
      });
      onAdded();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }, [fields, config, name, taskType, connection, onAdded]);

  return (
    <Modal
      title="Add connector"
      onClose={onClose}
      footer={
        <>
          {error && (
            <span className="mr-auto text-xs text-danger">{error}</span>
          )}
          <Button size="sm" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button
            size="sm"
            variant="primary"
            disabled={!canSubmit || busy}
            onClick={() => void submit()}
          >
            {busy ? "Adding…" : "Add connector"}
          </Button>
        </>
      }
    >
      {addable.length === 0 ? (
        <p className="text-sm text-fg-faint">
          Every installed connector is already enabled. Install a connector pack
          from the marketplace to add more.
        </p>
      ) : (
        <div className="flex flex-col gap-3">
          <label className="flex flex-col gap-1 text-xs text-fg-faint">
            Connector
            <select
              value={taskType}
              onChange={(e) => {
                setTaskType(e.target.value);
                setConfig({});
              }}
              className={inputClass}
            >
              {addable.map((k) => (
                <option key={k.taskType} value={k.taskType}>
                  {k.taskType}
                  {k.displayName ? ` — ${k.displayName}` : ""}
                  {k.hasComponent ? "" : " (worker only)"}
                </option>
              ))}
            </select>
          </label>

          {fields.map((f) => (
            <label
              key={f.key}
              className="flex flex-col gap-1 text-xs text-fg-faint"
            >
              <span>
                {f.label}
                {f.required && <span className="text-danger"> *</span>}
              </span>
              <Input
                value={config[f.key] ?? ""}
                onChange={(e) => setField(f.key, e.target.value)}
                placeholder={f.default ?? ""}
                spellCheck={false}
              />
              {f.description && (
                <span className="text-fg-faint/80">{f.description}</span>
              )}
            </label>
          ))}

          <label className="flex flex-col gap-1 text-xs text-fg-faint">
            Connection{fields.length > 0 ? "" : " (optional)"}
            <Input
              value={connection}
              onChange={(e) => setConnection(e.target.value)}
              placeholder="named connection reference (e.g. slack)"
              spellCheck={false}
            />
            {needsConnection ? (
              <span className="text-danger">
                Name a connection to store the config values against.
              </span>
            ) : (
              <span className="text-fg-faint/80">
                Config values are stored as env pointers on this named
                connection — never inline secrets (ADR 0027 §5).
              </span>
            )}
          </label>
        </div>
      )}
    </Modal>
  );
}
