import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import CodeEditor from "./CodeEditor";
import { Badge, Button, SectionLabel } from "@nanobpm/process-explorer/ui";
import { projectFileEx } from "../lib/api";
import { setManifestSource } from "../lib/manifestIntellisense";
import { saveProjectFile, type FileNode } from "../gen";
import {
  buildSymbolIndex,
  modelKindOf,
  resolveDomainTypes,
  validateManifest,
  type Diagnostic,
  type SymbolIndex,
  type ModelFile,
} from "@nanobpm/nano-app-schema";

/** True for a project's Urban App manifest (`nano.app.json`, at the root). */
export function isAppManifestPath(path: string): boolean {
  return path === "nano.app.json" || path.endsWith("/nano.app.json");
}

/** Stable virtual URI for the manifest's Monaco model, so the manifest
 * IntelliSense provider can scope itself to exactly this document. */
const MANIFEST_URI = "file:///nano.app.json";

function flattenFiles(nodes: FileNode[]): string[] {
  const out: string[] = [];
  const walk = (ns: FileNode[]) => {
    for (const n of ns) {
      if (n.kind === "file") out.push(n.path);
      if (n.children) walk(n.children);
    }
  };
  walk(nodes);
  return out;
}

type IndexState =
  | { status: "building" }
  | { status: "ready"; index: SymbolIndex }
  | { status: "error"; message: string };

/// The Urban App manifest editor (ADR 0027 §6 / 0029). Opened when the user
/// selects a project's `nano.app.json`. It edits the manifest (the App's source
/// of truth) as JSON while building the project **symbol index** client-side
/// from the same models the graphical editors use, then runs the fail-closed
/// `validateManifest` gate and surfaces its diagnostics + the index's
/// enumeration inline — the single source the reference pickers will bind to.
export default function AppManifestEditor({
  name,
  path,
  files,
}: {
  name: string;
  path: string;
  files: FileNode[];
}) {
  const [content, setContent] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [idx, setIdx] = useState<IndexState>({ status: "building" });

  // Load the manifest text.
  useEffect(() => {
    let alive = true;
    setContent(null);
    setDirty(false);
    setLoadError(null);
    projectFileEx(name, path)
      .then((f) => alive && setContent(f.binary ? "" : f.text))
      .catch(
        (e) =>
          alive && setLoadError(e instanceof Error ? e.message : String(e)),
      );
    return () => {
      alive = false;
    };
  }, [name, path]);

  // Build the symbol index from the project's model files. Parsing uses the same
  // moddle/form-js model the editors use, so the index can't disagree with what
  // the maker actually drew (ADR 0029). Rebuilt when the file set changes.
  const modelPaths = useMemo(
    () => flattenFiles(files).filter((p) => modelKindOf(p) !== undefined),
    [files],
  );
  const buildIndex = useCallback(async () => {
    setIdx({ status: "building" });
    try {
      const models: ModelFile[] = [];
      for (const p of modelPaths) {
        const kind = modelKindOf(p);
        if (!kind) continue;
        const f = await projectFileEx(name, p);
        if (f.binary) continue;
        models.push({ path: p, kind, text: f.text });
      }
      const index = await buildSymbolIndex(models);
      setIdx({ status: "ready", index });
    } catch (e) {
      setIdx({
        status: "error",
        message: e instanceof Error ? e.message : String(e),
      });
    }
  }, [name, modelPaths]);

  useEffect(() => {
    void buildIndex();
  }, [buildIndex]);

  // Validate: shape-first, then cross-reference against the index once it's
  // ready. A JSON parse failure is reported as its own diagnostic (the manifest
  // can't be linted until it parses).
  const { diagnostics, parsed } = useMemo<{
    diagnostics: Diagnostic[];
    parsed: unknown;
  }>(() => {
    if (content == null) return { diagnostics: [], parsed: undefined };
    let value: unknown;
    try {
      value = JSON.parse(content);
    } catch (e) {
      return {
        diagnostics: [
          {
            severity: "error",
            pointer: "/",
            message: `not valid JSON: ${e instanceof Error ? e.message : String(e)}`,
            code: "json",
          },
        ],
        parsed: undefined,
      };
    }
    const index = idx.status === "ready" ? idx.index : undefined;
    return {
      diagnostics: validateManifest(value, index).diagnostics,
      parsed: value,
    };
  }, [content, idx]);

  // Keep the manifest IntelliSense provider (ADR 0029 §2) fed with the latest
  // parsed manifest + symbol index. A ref-cell means the provider reads fresh
  // state on each completion request without re-registering per keystroke.
  const sourceRef = useRef<{
    manifest: unknown;
    index: SymbolIndex | undefined;
  }>({
    manifest: undefined,
    index: undefined,
  });
  sourceRef.current = {
    manifest: parsed,
    index: idx.status === "ready" ? idx.index : undefined,
  };
  useEffect(() => setManifestSource(MANIFEST_URI, () => sourceRef.current), []);

  const save = useCallback(async () => {
    if (content == null) return;
    setSaving(true);
    try {
      await saveProjectFile({
        path: { name },
        query: { path },
        body: content,
        throwOnError: true,
      });
      setDirty(false);
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [content, name, path]);

  if (loadError) {
    return (
      <div className="flex h-full items-center justify-center p-6 text-sm text-danger">
        {loadError}
      </div>
    );
  }
  if (content == null) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-fg-faint">
        Loading manifest…
      </div>
    );
  }

  const ok = diagnostics.length === 0 && idx.status === "ready";

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center gap-2 border-b border-edge bg-panel px-4 py-2">
        <span className="font-semibold text-fg">nano.app.json</span>
        {idx.status === "building" ? (
          <Badge tone="neutral">indexing…</Badge>
        ) : ok ? (
          <Badge tone="ok">valid</Badge>
        ) : (
          <Badge tone="danger">
            {diagnostics.length}{" "}
            {diagnostics.length === 1 ? "problem" : "problems"}
          </Badge>
        )}
        <div className="ml-auto flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => void buildIndex()}
            title="Re-read the project models and rebuild the symbol index"
          >
            Rescan models
          </Button>
          <Button
            variant="primary"
            size="sm"
            disabled={!dirty || saving}
            onClick={() => void save()}
          >
            {saving ? "Saving…" : "Save"}
          </Button>
        </div>
      </div>

      <div className="flex min-h-0 flex-1">
        {/* Manifest JSON editor */}
        <div className="min-w-0 flex-1 border-r border-edge">
          <CodeEditor
            value={content}
            language="json"
            path={MANIFEST_URI}
            onChange={(v) => {
              setContent(v);
              setDirty(true);
            }}
            onSave={() => void save()}
          />
        </div>

        {/* App inspector: validation + the symbol enumeration (picker source). */}
        <aside className="w-80 shrink-0 overflow-y-auto bg-panel p-4">
          <section className="mb-5">
            <SectionLabel>Validation</SectionLabel>
            {diagnostics.length === 0 ? (
              <p className="text-sm text-fg-muted">
                {idx.status === "ready"
                  ? "No problems — the manifest is valid against the schema and every reference resolves."
                  : "Shape valid. Cross-reference checks run once the model index is ready."}
              </p>
            ) : (
              <ul className="space-y-2">
                {diagnostics.map((d, i) => (
                  <li
                    key={`${d.pointer}-${i}`}
                    className="rounded-md border border-danger/30 bg-danger/5 p-2 text-sm"
                  >
                    <div className="text-danger">{d.message}</div>
                    <div className="mt-0.5 flex items-center gap-2 text-xs text-fg-faint">
                      <code>{d.pointer}</code>
                      <Badge tone="neutral">{d.code}</Badge>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>

          <ManifestInspector idx={idx} parsed={parsed} />
        </aside>
      </div>
    </div>
  );
}

/// Read-only enumeration of the project's models — the ids a maker can reference
/// from the manifest. This is the same data the reference pickers (ADR 0029
/// phase 2) bind to; surfacing it here proves the index and gives makers the
/// exact ids to type today.
function ManifestInspector({
  idx,
  parsed,
}: {
  idx: IndexState;
  parsed: unknown;
}) {
  if (idx.status === "building") {
    return <p className="text-sm text-fg-faint">Building the model index…</p>;
  }
  if (idx.status === "error") {
    return <p className="text-sm text-danger">Index error: {idx.message}</p>;
  }
  const { index } = idx;
  const dataSources = Object.keys(
    (parsed as { data?: { sources?: Record<string, unknown> } } | undefined)
      ?.data?.sources ?? {},
  );
  const domain = resolveDomainTypes(parsed, index);
  return (
    <div className="space-y-5">
      {dataSources.length > 0 && (
        <section>
          <SectionLabel>Data sources</SectionLabel>
          <ul className="space-y-1 text-sm">
            {dataSources.map((s) => (
              <li key={s}>
                <code className="text-accent">{s}</code>
              </li>
            ))}
          </ul>
        </section>
      )}

      {(domain.declared.length > 0 || domain.inferred.length > 0) && (
        <section>
          <SectionLabel>
            Domain types ({domain.declared.length}
            {domain.inferred.length > 0
              ? ` +${domain.inferred.length} inferred`
              : ""}
            )
          </SectionLabel>
          {domain.declared.length === 0 && (
            <p className="text-sm text-fg-faint">
              No types declared yet. The candidates below are inferred from
              forms — promote one into <code>types</code> to reference it.
            </p>
          )}
          <ul className="space-y-2 text-sm">
            {domain.declared.map((t) => (
              <li key={t.id}>
                <code className="text-accent">{t.id}</code>
                {t.name ? (
                  <span className="text-fg-muted"> — {t.name}</span>
                ) : null}
                {t.table ? (
                  <span className="text-fg-faint"> · table {t.table}</span>
                ) : null}
                {t.fields.length > 0 && (
                  <div className="ml-3 text-xs text-fg-faint">
                    {t.fields
                      .map(
                        (f) =>
                          `${f.key}: ${f.type}${f.list ? "[]" : ""}${f.optional ? "?" : ""}`,
                      )
                      .join(", ")}
                  </div>
                )}
              </li>
            ))}
            {domain.inferred.map((r) => (
              <li key={`inferred-${r.id}`} className="text-fg-muted">
                <code className="text-fg-muted">{r.id}</code>
                <Badge>inferred from form</Badge>
                <div className="ml-3 text-xs text-fg-faint">
                  {r.fields.map((f) => `${f.key}: ${f.type}`).join(", ")}
                </div>
              </li>
            ))}
          </ul>
        </section>
      )}

      <section>
        <SectionLabel>Processes ({index.processes.length})</SectionLabel>
        {index.processes.length === 0 ? (
          <p className="text-sm text-fg-faint">No BPMN processes found.</p>
        ) : (
          <ul className="space-y-2 text-sm">
            {index.processes.map((p) => (
              <li key={p.id}>
                <code className="text-accent">{p.id}</code>
                {p.name ? (
                  <span className="text-fg-muted"> — {p.name}</span>
                ) : null}
                {p.messageStartEvents.length > 0 && (
                  <div className="ml-3 text-xs text-fg-faint">
                    starts on: {p.messageStartEvents.join(", ")}
                  </div>
                )}
                {p.userTasks.length > 0 && (
                  <div className="ml-3 text-xs text-fg-faint">
                    user tasks: {p.userTasks.map((t) => t.id).join(", ")}
                  </div>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      {index.messages.length > 0 && (
        <section>
          <SectionLabel>Messages ({index.messages.length})</SectionLabel>
          <ul className="space-y-1 text-sm">
            {index.messages.map((m) => (
              <li key={m}>
                <code className="text-accent">{m}</code>
              </li>
            ))}
          </ul>
        </section>
      )}

      {index.decisions.length > 0 && (
        <section>
          <SectionLabel>Decisions ({index.decisions.length})</SectionLabel>
          <ul className="space-y-1 text-sm">
            {index.decisions.map((d) => (
              <li key={d.id}>
                <code className="text-accent">{d.id}</code>
                {d.name ? (
                  <span className="text-fg-muted"> — {d.name}</span>
                ) : null}
              </li>
            ))}
          </ul>
        </section>
      )}

      {index.forms.length > 0 && (
        <section>
          <SectionLabel>Forms ({index.forms.length})</SectionLabel>
          <ul className="space-y-1 text-sm">
            {index.forms.map((f) => (
              <li key={f.id}>
                <code className="text-accent">{f.id}</code>
                {f.fields.length > 0 && (
                  <div className="ml-3 text-xs text-fg-faint">
                    fields: {f.fields.map((fld) => fld.key).join(", ")}
                  </div>
                )}
              </li>
            ))}
          </ul>
        </section>
      )}

      {index.parseErrors.length > 0 && (
        <section>
          <SectionLabel>Parse warnings</SectionLabel>
          <ul className="space-y-1 text-sm">
            {index.parseErrors.map((e, i) => (
              <li key={`${e.path}-${i}`} className="text-warn">
                <code>{e.path}</code>: {e.message}
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}
