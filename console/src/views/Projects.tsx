import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  createProject,
  deleteProject,
  importProject,
  listProjects,
  renameProject,
  runProject,
  stopProject,
  type ProjectSummary,
  type ProjectTemplate,
  type TemplateOption,
} from "../gen";
import {
  Button,
  Card,
  EmptyState,
  Input,
  PageHeader,
  Spinner,
  useIsNarrow,
} from "../components/ui";
import { useTemplateUpdate } from "../components/TemplateUpdate";
import { toUpdateTarget } from "../lib/templateUpdate";
import JourneyPicker from "../components/JourneyPicker";
import DirectoryPicker from "../components/DirectoryPicker";
import { isLocalhost } from "../lib/api";
import { copyText, selectElementText } from "../lib/clipboard";
import { CONSOLE_PROFILE } from "../lib/profile";
import { TOUR_ANCHOR, templateAnchor } from "../lib/tour/tourAnchors";
import { useTour } from "../lib/tour/tourContext";
import { pickerJourneys } from "../lib/tour/picker";
import {
  slugifyProjectName,
  validateProjectName,
  validateSafeName,
} from "../lib/projectName";

/// Resolved language-pack presentation for a project card: the pack's icon
/// (inline SVG markup or a data:/http: URL) and its human-facing name.
type LangMeta = { icon?: string; displayName: string };

/// Turn a pack `icon` (raw SVG markup, or an already-usable data:/http: URL)
/// into an `<img src>`. Inline SVG is wrapped in a `data:` URI so it renders
/// sandboxed (no script execution), which also means it can't inherit theme
/// colours — pack icons are authored as self-coloured tiles for that reason.
function iconSrc(icon: string): string {
  const s = icon.trim();
  if (s.startsWith("<")) return `data:image/svg+xml,${encodeURIComponent(s)}`;
  return s;
}

/// Home view of the RAD environment: every project as a tile (like the LLM
/// profile tiles). "New project" swaps to a template gallery — every scaffold
/// template as a card (title, language, description) — plus the name form.
/// Opening a project tile routes to its workspace.
export default function Projects() {
  const navigate = useNavigate();
  const tour = useTour();
  // Below the canonical mobile breakpoint the desktop IDE (ProjectWorkspace) is
  // unreachable by design (issue #1005 A2): the card surfaces lifecycle actions
  // inline instead of drilling into it. Drives the responsive presentation of
  // the SAME route set — no forked mobile route tree.
  const narrow = useIsNarrow();
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [nodeAvailable, setNodeAvailable] = useState(true);
  const [urbanAvailable, setUrbanAvailable] = useState(true);
  const [templates, setTemplates] = useState<ProjectTemplate[]>([]);
  const [langMeta, setLangMeta] = useState<Record<string, LangMeta>>({});
  const [newTemplate, setNewTemplate] = useState("starter");
  // Chosen values for each template's declared creation options, keyed
  // templateId → optionId → value. Missing entries fall back to the option's
  // `default` (then its first choice). Echoed back in the createProject body.
  const [optionSel, setOptionSel] = useState<
    Record<string, Record<string, string>>
  >({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Name of the project whose start/stop lifecycle call is in flight, so the
  // inline mobile controls can show progress and block a double-tap. One at a
  // time is enough — the cards are independent and calls are quick.
  const [lifecycleBusy, setLifecycleBusy] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [importing, setImporting] = useState(false);
  const [importName, setImportName] = useState("");
  const [importPath, setImportPath] = useState("");
  const [browsing, setBrowsing] = useState(false);
  const canBrowse = useMemo(() => isLocalhost(), []);
  const [busy, setBusy] = useState(false);
  // "Update from template" flow — the shared dry-run → review → apply hook, so
  // the projects list, the IDE workspace and the extensions reverse-index drive
  // identical review/confirm modals. `previewName`/`updateBusy` gate the card
  // affordance; `updateError` surfaces in the page error banner below.
  const {
    startUpdate,
    previewName,
    busy: updateBusy,
    error: updateError,
    modals: updateModals,
  } = useTemplateUpdate({ onApplied: () => reload() });
  // "Point your agent here" affordance (ADR 0051): reveals the /agent brief URL
  // an external coding agent can be aimed at to author an app and link it in.
  const [agentHint, setAgentHint] = useState(false);
  const [agentCopied, setAgentCopied] = useState(false);
  const agentUrl = useMemo(
    () => `${globalThis.location?.origin ?? ""}/agent`,
    [],
  );
  const agentUrlRef = useRef<HTMLElement>(null);

  const reload = useCallback(async () => {
    try {
      const res = (await listProjects({ throwOnError: true })).data;
      setProjects(res.projects);
      setNodeAvailable(res.nodeAvailable);
      setUrbanAvailable(res.urbanAvailable);
      setTemplates(res.templates ?? []);
      const meta: Record<string, LangMeta> = {};
      for (const e of res.extensions?.extensions ?? []) {
        if (e.kind === "lang")
          meta[e.id] = { icon: e.icon, displayName: e.displayName };
      }
      setLangMeta(meta);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  // The selected template card. Falls back to the first template so the
  // gallery never renders with nothing selected (e.g. a pack shadowed away
  // the remembered id between visits).
  const selectedTemplate =
    templates.find((t) => t.id === newTemplate) ?? templates[0];

  // Whether a host capability named by a choice's `requires` is present.
  // Unknown/absent capability = always selectable.
  const capabilityOk = useCallback(
    (requires: string | null | undefined): boolean => {
      if (!requires) return true;
      if (requires === "node") return nodeAvailable;
      return true;
    },
    [nodeAvailable],
  );

  // Effective value for one of a template's options: the user's pick, else the
  // declared default, else the first choice.
  const optionValue = useCallback(
    (templateId: string, opt: TemplateOption): string =>
      optionSel[templateId]?.[opt.id] ??
      opt.default ??
      opt.choices[0]?.value ??
      "",
    [optionSel],
  );

  // The chosen values for the selected template's options as a flat map, for
  // the createProject request body. Empty when the template declares none.
  const selectedOptions = useMemo<Record<string, string>>(() => {
    const t = selectedTemplate;
    if (!t?.options?.length) return {};
    const out: Record<string, string> = {};
    for (const opt of t.options) out[opt.id] = optionValue(t.id, opt);
    return out;
  }, [selectedTemplate, optionValue]);

  const create = async () => {
    const name = newName.trim();
    if (!name || nameError) return;
    setBusy(true);
    try {
      const res = await createProject({
        body: {
          name,
          description: newDesc.trim(),
          template: selectedTemplate?.id ?? "starter",
          ...(Object.keys(selectedOptions).length
            ? { options: selectedOptions }
            : {}),
        },
        throwOnError: true,
      });
      // Route by the server's directory-safe slug — for a display name with
      // spaces ("Home Heating") the project lives at /projects/home-heating.
      // On mobile `openWorkspace` keeps us on the list (the IDE is gated).
      if (narrow) {
        setCreating(false);
        setBusy(false);
      }
      openWorkspace(res.data.name);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  const nameError = useMemo(
    () => validateProjectName(newName, projects),
    [newName, projects],
  );
  const nameValid = newName.trim().length > 0 && !nameError;
  const nameSlug = slugifyProjectName(newName);

  const importNameError = useMemo(
    () => validateSafeName(importName, projects),
    [importName, projects],
  );
  const importValid =
    importName.trim().length > 0 &&
    importPath.trim().length > 0 &&
    !importNameError;

  /// Import an existing checked-out Urban app by reference (ADR 0041): point a
  /// name at an external directory, read live. The server validates the path is
  /// a Nano app/project and that the name is free.
  const importRef = async () => {
    const name = importName.trim();
    const path = importPath.trim();
    if (!importValid) return;
    setBusy(true);
    try {
      await importProject({ body: { name, path }, throwOnError: true });
      // Success: collapse the import panel and clear its inputs so a narrow
      // viewport (which stays on the list via openWorkspace) isn't left with a
      // stale, populated panel open after the action completes.
      setImporting(false);
      setImportName("");
      setImportPath("");
      if (narrow) setBusy(false);
      openWorkspace(name);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  };

  const remove = async (project: ProjectSummary) => {
    const { name, source } = project;
    const label = project.displayName ?? name;
    // A linked (imported-by-reference) project owns only a pointer file — the
    // backend deletes the reference and leaves the external checkout on disk
    // untouched (ADR 0041). Only a workspace project's files are actually
    // removed, so don't warn about deleting files we won't touch.
    const message =
      source === "path"
        ? `Remove the link to “${label}”? The files on disk won’t be deleted.`
        : `Delete project “${label}” and all its files? This cannot be undone.`;
    if (!confirm(message)) return;
    try {
      await deleteProject({ path: { name }, throwOnError: true });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const rename = async (project: ProjectSummary) => {
    const current = project.displayName ?? project.name;
    const next = prompt(`Rename “${current}” to:`, current)?.trim();
    if (!next || next === current) return;
    try {
      await renameProject({
        path: { name: project.name },
        body: { newName: next },
        throwOnError: true,
      });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  // The single gate that keeps the desktop IDE off mobile (issue #1005 A2):
  // ProjectWorkspace is unreachable below the mobile breakpoint, so EVERY path
  // that would land there — the card drill-in and the post-create / post-import
  // hand-off — routes through here. On a narrow viewport we stay on the project
  // list (reloading so a freshly created card appears) instead of navigating
  // into an editor that can't be used on a phone.
  const openWorkspace = useCallback(
    (name: string) => {
      if (narrow) {
        void reload();
        return;
      }
      navigate(`/projects/${encodeURIComponent(name)}`);
    },
    [narrow, navigate, reload],
  );

  // Project lifecycle: Start (Run) and Stop are the real, project-level actions
  // (runProject / stopProject) — surfaced inline on each mobile card so they are
  // reachable without the (mobile-gated) IDE. Update lives in the shared
  // template-update flow (`onUpdate` below).
  const start = async (project: ProjectSummary) => {
    // Guard against overlapping lifecycle ops: a second call while one is
    // in-flight would let the first call's `finally` clear `lifecycleBusy`
    // out from under the second, re-enabling controls mid-request.
    if (lifecycleBusy) return;
    setLifecycleBusy(project.name);
    setError(null);
    try {
      await runProject({ path: { name: project.name }, throwOnError: true });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLifecycleBusy(null);
    }
  };

  const stop = async (project: ProjectSummary) => {
    if (lifecycleBusy) return;
    setLifecycleBusy(project.name);
    setError(null);
    try {
      await stopProject({ path: { name: project.name }, throwOnError: true });
      await reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLifecycleBusy(null);
    }
  };

  // The page error banner surfaces both this view's own operations and the
  // shared update flow's errors. It has no dismiss control — each is cleared
  // when its source starts a fresh operation (this view's handlers reset
  // `error`; the update hook clears `updateError` on the next update).
  const bannerError = error ?? updateError;
  const errorBanner = bannerError && (
    <div className="mb-4 rounded-md border border-danger/40 bg-danger/10 px-4 py-2 text-sm text-danger">
      {bannerError}
    </div>
  );

  // ── New Project: template gallery ─────────────────────────────────────────
  if (creating) {
    return (
      <div className="mx-auto max-w-6xl p-8">
        <PageHeader
          title="New project"
          subtitle="Pick a template and name your project — a runnable app is scaffolded for you."
          actions={
            <Button variant="secondary" onClick={() => setCreating(false)}>
              ← Back to projects
            </Button>
          }
        />

        {errorBanner}

        <Card className="mb-6 p-4">
          <div className="grid gap-3 sm:grid-cols-[1fr_2fr]">
            <div>
              <input
                autoFocus
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                placeholder="Project name"
                aria-invalid={!!nameError}
                className={`w-full rounded-md border bg-inset px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none ${
                  nameError
                    ? "border-danger/70 focus:border-danger"
                    : "border-edge-strong focus:border-accent"
                }`}
                onKeyDown={(e) =>
                  e.key === "Enter" && nameValid && void create()
                }
              />
              <p
                className={`mt-1 text-xs ${
                  nameError ? "text-danger" : "text-fg-faint"
                }`}
              >
                {nameError ??
                  (nameSlug && nameSlug !== newName.trim()
                    ? `Files will live in “${nameSlug}”.`
                    : "Spaces are OK — files use a URL-safe slug.")}
              </p>
            </div>
            <Input
              value={newDesc}
              onChange={(e) => setNewDesc(e.target.value)}
              placeholder="Short description (optional)"
              className="h-fit"
              onKeyDown={(e) => e.key === "Enter" && nameValid && void create()}
            />
          </div>
          <div className="mt-3 flex items-center gap-3">
            <Button
              variant="primary"
              onClick={() => void create()}
              disabled={busy || !nameValid}
            >
              {busy ? "Creating…" : "Create project"}
            </Button>
            {selectedTemplate && (
              <span className="text-xs text-fg-faint">
                Template:{" "}
                <span className="text-fg-muted">{selectedTemplate.label}</span>
              </span>
            )}
          </div>
          {selectedTemplate?.options?.map((opt) => (
            <div key={opt.id} className="mt-4">
              <label className="mb-1 block text-xs font-medium text-fg-muted">
                {opt.label}
              </label>
              <div className="inline-flex rounded-md border border-edge p-0.5">
                {opt.choices.map((choice) => {
                  const enabled = capabilityOk(choice.requires);
                  const active =
                    optionValue(selectedTemplate.id, opt) === choice.value;
                  return (
                    <button
                      key={choice.value}
                      type="button"
                      disabled={!enabled}
                      title={
                        enabled
                          ? undefined
                          : `${choice.label} needs ${choice.requires} — not detected on this host`
                      }
                      onClick={() =>
                        setOptionSel((prev) => ({
                          ...prev,
                          [selectedTemplate.id]: {
                            ...prev[selectedTemplate.id],
                            [opt.id]: choice.value,
                          },
                        }))
                      }
                      className={[
                        "rounded px-3 py-1 text-sm transition-colors",
                        active
                          ? "bg-accent text-accent-fg"
                          : "text-fg-muted hover:bg-subtle",
                        enabled ? "" : "cursor-not-allowed opacity-40",
                      ].join(" ")}
                    >
                      {choice.label}
                    </button>
                  );
                })}
              </div>
            </div>
          ))}
          {selectedTemplate?.needsUrban && !urbanAvailable && (
            <p className="mt-4 rounded-md border border-edge bg-subtle px-3 py-2 text-xs text-fg-muted">
              This is an Urban app. The{" "}
              <span className="font-medium text-fg">@nanobpm/urban</span>{" "}
              toolkit scaffolds it, and isn’t installed yet — Studio will set it
              up for you as part of creating the project.
            </p>
          )}
        </Card>

        {loading ? (
          <div className="py-16 text-center text-sm text-fg-faint">
            Loading…
          </div>
        ) : templates.length === 0 ? (
          <EmptyState
            title="No templates available."
            hint="The server reported no scaffold templates — check the gateway connection."
          />
        ) : (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {templates.map((t) => (
              <TemplateTile
                key={t.id}
                template={t}
                lang={langMeta[t.lang]}
                selected={t.id === selectedTemplate?.id}
                onSelect={() => setNewTemplate(t.id)}
              />
            ))}
          </div>
        )}
      </div>
    );
  }

  // ── Projects home ──────────────────────────────────────────────────────────
  return (
    <div className="mx-auto max-w-6xl p-8">
      <PageHeader
        title="Studio"
        subtitle="Self-contained applications — processes, decisions, forms, workers and shared libraries. Author, run, compile and export from one place."
        actions={
          <div className="flex gap-2">
            <Button
              variant="secondary"
              onClick={() => {
                setAgentHint((v) => !v);
              }}
            >
              {agentHint ? "Hide agent" : "Build with an agent"}
            </Button>
            <Button
              variant="secondary"
              onClick={() => {
                setImporting((v) => !v);
              }}
            >
              {importing ? "Cancel" : "Import by reference"}
            </Button>
            <Button
              variant="primary"
              data-tour={TOUR_ANCHOR.newProject}
              onClick={() => {
                setCreating(true);
                setImporting(false);
              }}
            >
              + New project
            </Button>
          </div>
        }
      />

      {!nodeAvailable && (
        <div className="mb-4 rounded-md border border-warn/40 bg-warn/10 px-4 py-2 text-sm text-warn">
          No JavaScript runtime detected — you can author projects, but Run
          needs Node ≥ 22.6 (the npm launcher provides one).
        </div>
      )}

      {errorBanner}

      {agentHint && (
        <Card className="mb-6 p-4">
          <h2 className="text-sm font-semibold text-fg">
            Build with a coding agent
          </h2>
          <p className="mt-1 text-sm text-fg-muted">
            Point your coding agent (Claude Code, Copilot CLI, or any MCP-driven
            assistant) at the URL below. It serves a live brief that teaches the
            agent how to author a Nano app on disk and link it into this node —
            and how Nano works, so it can explain it to you.
          </p>
          <div className="mt-3 flex items-center gap-2">
            <code
              ref={agentUrlRef}
              className="flex-1 truncate rounded-md border border-edge bg-inset px-3 py-2 text-sm text-fg"
            >
              {agentUrl}
            </code>
            <Button
              variant="secondary"
              onClick={() => {
                void copyText(agentUrl).then((ok) => {
                  if (!ok) {
                    // Insecure context with no clipboard access — select the URL
                    // so the user can copy it by hand.
                    if (agentUrlRef.current)
                      selectElementText(agentUrlRef.current);
                    return;
                  }
                  setAgentCopied(true);
                  setTimeout(() => setAgentCopied(false), 1500);
                });
              }}
            >
              {agentCopied ? "Copied" : "Copy"}
            </Button>
            <a
              href={agentUrl}
              target="_blank"
              rel="noreferrer"
              className="text-sm text-accent hover:underline"
            >
              Open
            </a>
          </div>
          <p className="mt-2 text-xs text-fg-faint">
            Tell the agent: <em>“Read {agentUrl} and build me an app.”</em> When
            it finishes, the app appears here in your Projects gallery.
          </p>
        </Card>
      )}

      {importing && (
        <Card className="mb-6 p-4">
          <div className="grid gap-3 sm:grid-cols-[1fr_2fr]">
            <div>
              <input
                autoFocus
                value={importName}
                onChange={(e) => setImportName(e.target.value)}
                placeholder="project-name"
                aria-invalid={!!importNameError}
                className={`w-full rounded-md border bg-inset px-3 py-2 text-sm text-fg placeholder:text-fg-faint outline-none ${
                  importNameError
                    ? "border-danger/70 focus:border-danger"
                    : "border-edge-strong focus:border-accent"
                }`}
                onKeyDown={(e) =>
                  e.key === "Enter" && importValid && void importRef()
                }
              />
              <p
                className={`mt-1 text-xs ${
                  importNameError ? "text-danger" : "text-fg-faint"
                }`}
              >
                {importNameError ?? "The name this app is registered under."}
              </p>
            </div>
            <div>
              <div className="flex gap-2">
                <Input
                  value={importPath}
                  onChange={(e) => setImportPath(e.target.value)}
                  placeholder="/absolute/path/to/checked-out/app"
                  className="h-fit flex-1"
                  onKeyDown={(e) =>
                    e.key === "Enter" && importValid && void importRef()
                  }
                />
                {canBrowse && (
                  <Button
                    variant="secondary"
                    onClick={() => setBrowsing(true)}
                    title="Browse the server's filesystem"
                  >
                    Browse…
                  </Button>
                )}
              </div>
              <p className="mt-1 text-xs text-fg-faint">
                Absolute path to a checked-out Urban app directory (contains{" "}
                <code>nano.app.json</code> or <code>nanobpm.project.json</code>
                ). Read live — no copy.
              </p>
            </div>
          </div>
          <div className="mt-3 flex items-center gap-3">
            <Button
              variant="primary"
              onClick={() => void importRef()}
              disabled={busy || !importValid}
            >
              {busy ? "Importing…" : "Import project"}
            </Button>
            <span className="text-xs text-fg-faint">
              The app runs from its source directory, so edits show up on the
              next Run.
            </span>
          </div>
        </Card>
      )}

      {browsing && (
        <DirectoryPicker
          initialPath={importPath.trim() || undefined}
          onPick={(path) => {
            setImportPath(path);
            setBrowsing(false);
          }}
          onClose={() => setBrowsing(false)}
        />
      )}

      {loading ? (
        <div className="py-16 text-center text-sm text-fg-faint">Loading…</div>
      ) : projects.length === 0 ? (
        // The empty state IS the journey picker (#411, ADR 0049 §2): a
        // first-timer with no projects chooses an outcome-shaped journey (or the
        // quiet overview) instead of hitting a dead-end "Create one to get
        // started." Cards are derived from the registry, so a new journey needs
        // no change here, and the whole block stops rendering once projects
        // exist — no dismissal state to track.
        <JourneyPicker
          title="No projects yet."
          subtitle="Pick a guided journey to build your first one — or explore on your own."
          journeys={
            tour ? pickerJourneys(tour.availableJourneys, CONSOLE_PROFILE) : []
          }
          onPick={(id) => tour?.startJourney(id)}
          onOverview={tour ? () => tour.startTour() : undefined}
          fallback={
            <EmptyState
              title="No projects yet."
              hint="Create one to get started."
            />
          }
        />
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {projects.map((p) => (
            <ProjectTile
              key={p.name}
              project={p}
              lang={langMeta[p.lang]}
              narrow={narrow}
              onOpen={() => openWorkspace(p.name)}
              onDelete={() => void remove(p)}
              onRename={() => void rename(p)}
              onStart={() => void start(p)}
              onStop={() => void stop(p)}
              lifecycleBusy={lifecycleBusy === p.name}
              lifecycleLocked={lifecycleBusy !== null}
              onUpdate={
                p.updateAvailable && p.source !== "path"
                  ? () => void startUpdate(toUpdateTarget(p))
                  : undefined
              }
              updating={previewName === p.name}
              updateDisabled={updateBusy}
            />
          ))}
        </div>
      )}
      {updateModals}
    </div>
  );
}

/// The language badge shared by project and template cards: the lang pack's
/// icon when installed, else a two-letter fallback tile.
function LangIcon({ langId, lang }: { langId: string; lang?: LangMeta }) {
  const langLabel = lang?.displayName ?? langId;
  return lang?.icon ? (
    <img
      src={iconSrc(lang.icon)}
      alt={langLabel}
      title={langLabel}
      className="h-5 w-5 shrink-0 rounded-sm"
    />
  ) : (
    <span
      role="img"
      aria-label={langLabel}
      title={langLabel}
      className="inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-sm bg-inset text-[9px] font-bold uppercase text-fg-faint"
    >
      {(langId || "?").slice(0, 2)}
    </span>
  );
}

/// One scaffold template as a selectable card in the New Project gallery:
/// language icon + title, the language's name, the template's one-line
/// description, and its provenance (built-in or contributing pack).
function TemplateTile({
  template,
  lang,
  selected,
  onSelect,
}: {
  template: ProjectTemplate;
  lang?: LangMeta;
  selected: boolean;
  onSelect: () => void;
}) {
  const langLabel = lang?.displayName ?? template.lang;
  return (
    <Card
      data-tour={templateAnchor(template.id)}
      className={`flex flex-col p-4 transition-colors ${
        selected
          ? "border-accent ring-1 ring-accent"
          : "hover:border-edge-strong"
      }`}
    >
      <button
        type="button"
        onClick={onSelect}
        aria-pressed={selected}
        className="flex flex-1 flex-col text-left"
      >
        <div className="flex w-full items-center gap-2">
          <LangIcon langId={template.lang} lang={lang} />
          <span className="truncate text-base font-semibold text-fg">
            {template.label}
          </span>
          {selected && (
            <span
              aria-hidden
              className="ml-auto inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-accent text-[11px] font-bold text-on-accent"
            >
              ✓
            </span>
          )}
        </div>
        <div className="mt-1 text-xs text-fg-muted">{langLabel}</div>
        <p className="mt-1 line-clamp-3 min-h-[3.75rem] text-sm text-fg-faint">
          {template.description || "No description"}
        </p>
        <div
          className="mt-2 flex items-center gap-1 truncate text-[11px] text-fg-faint"
          title={
            template.source === "pack"
              ? `Contributed by the “${template.pack ?? "extension"}” extension pack`
              : "Built-in scaffold — works offline"
          }
        >
          {template.source === "pack" ? (
            <>
              <span aria-hidden>⧉</span>
              <span className="truncate">
                {template.pack ?? "Extension pack"}
              </span>
            </>
          ) : (
            <span>Built-in</span>
          )}
        </div>
      </button>
    </Card>
  );
}

function ProjectTile({
  project,
  lang,
  narrow = false,
  onOpen,
  onDelete,
  onRename,
  onStart,
  onStop,
  onUpdate,
  lifecycleBusy = false,
  lifecycleLocked = false,
  updating = false,
  updateDisabled = false,
}: {
  project: ProjectSummary;
  lang?: LangMeta;
  narrow?: boolean;
  onOpen: () => void;
  onDelete: () => void;
  onRename: () => void;
  onStart: () => void;
  onStop: () => void;
  onUpdate?: () => void;
  lifecycleBusy?: boolean;
  lifecycleLocked?: boolean;
  updating?: boolean;
  updateDisabled?: boolean;
}) {
  const title = project.displayName ?? project.name;
  if (narrow) {
    return (
      <MobileProjectTile
        project={project}
        lang={lang}
        title={title}
        onDelete={onDelete}
        onRename={onRename}
        onStart={onStart}
        onStop={onStop}
        onUpdate={onUpdate}
        lifecycleBusy={lifecycleBusy}
        lifecycleLocked={lifecycleLocked}
        updating={updating}
        updateDisabled={updateDisabled}
      />
    );
  }
  return (
    <Card className="group relative flex flex-col p-4 transition-colors hover:border-edge-strong">
      <div
        className={`absolute right-3 top-3 gap-1 group-hover:flex group-focus-within:flex [@media(hover:none)]:flex ${updating ? "flex" : "hidden"}`}
      >
        {onUpdate && (
          <button
            type="button"
            onClick={onUpdate}
            disabled={updateDisabled}
            title={
              updating
                ? "Checking for template changes…"
                : `Update from template${project.latestVersion ? ` (v${project.latestVersion} available)` : ""}`
            }
            aria-label={
              updating
                ? `Checking ${title} for template changes`
                : `Update ${title} from template`
            }
            aria-busy={updating}
            className="inline-flex items-center rounded px-1.5 py-0.5 text-xs text-accent hover:bg-accent/10 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {updating ? (
              <>
                <Spinner />
                <span className="sr-only">Checking for template changes…</span>
              </>
            ) : (
              "↑"
            )}
          </button>
        )}
        <button
          type="button"
          onClick={onRename}
          title="Rename project"
          aria-label={`Rename project ${title}`}
          className="rounded px-1.5 py-0.5 text-xs text-fg-faint hover:bg-hover hover:text-fg"
        >
          ✎
        </button>
        <button
          type="button"
          onClick={onDelete}
          title={project.source === "path" ? "Remove link" : "Delete project"}
          aria-label={
            project.source === "path"
              ? `Remove link to ${title}`
              : `Delete project ${title}`
          }
          className="rounded px-1.5 py-0.5 text-xs text-fg-faint hover:bg-danger/10 hover:text-danger"
        >
          ✕
        </button>
      </div>
      <div
        role="button"
        tabIndex={0}
        onClick={onOpen}
        onKeyDown={(e) => {
          // Only the tile itself opens on Enter/Space. Key events bubbling up
          // from nested controls (e.g. the "↑ update" badge) must not also
          // trigger onOpen — guard on the event originating on this element.
          if (
            e.target === e.currentTarget &&
            (e.key === "Enter" || e.key === " ")
          ) {
            e.preventDefault();
            onOpen();
          }
        }}
        className="flex flex-1 cursor-pointer flex-col text-left"
      >
        <div className="flex items-center gap-2">
          <LangIcon langId={project.lang} lang={lang} />
          <span
            className="truncate text-base font-semibold text-fg"
            title={
              project.displayName
                ? `${project.displayName} (${project.name})`
                : project.name
            }
          >
            {title}
          </span>
          {project.source === "path" && (
            <span
              title="Imported by reference — runs live from an external checked-out directory (ADR 0041)"
              className="inline-flex items-center gap-1 rounded-full bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent"
            >
              ⧉ linked
            </span>
          )}
          {onUpdate && project.updateAvailable && project.source !== "path" && (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onUpdate();
              }}
              disabled={updateDisabled}
              title={`A newer template is available${project.latestVersion ? ` (v${project.latestVersion})` : ""} — click to review and apply the update`}
              aria-label={`Update ${title} from template`}
              className="inline-flex items-center gap-1 rounded-full bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent hover:bg-accent/20 disabled:cursor-not-allowed disabled:opacity-50"
            >
              ↑ update
            </button>
          )}
          {project.running && (
            <span className="inline-flex items-center gap-1 rounded-full bg-ok/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-ok">
              <span className="h-1.5 w-1.5 rounded-full bg-ok" /> running
            </span>
          )}
        </div>
        <p className="mt-1 line-clamp-2 min-h-[2.5rem] text-sm text-fg-faint">
          {project.description || "No description"}
        </p>
        <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs text-fg-muted">
          <Stat label="processes" value={project.processes} />
          <Stat label="decisions" value={project.decisions} />
          <Stat label="forms" value={project.forms} />
          <Stat label="workers" value={project.workers} />
        </div>
        <TemplateProvenance project={project} />
        <div className="mt-2 truncate text-[11px] text-fg-faint">
          → {project.deployTarget}
        </div>
      </div>
    </Card>
  );
}

/// The mobile presentation of a project card (issue #1005 A2). The desktop tile
/// is a drill-in into the ProjectWorkspace IDE; that editor is unreachable on a
/// phone by design, so this variant NEVER navigates there. Instead the whole
/// card is static content and the project's lifecycle actions — Start / Stop and
/// (when applicable) Update, Rename, Delete — are surfaced inline as 44px touch
/// targets (`.nano-touch`, WCAG 2.5.5). A single-column layout keeps the card
/// within a 375px viewport with no horizontal scroll.
function MobileProjectTile({
  project,
  lang,
  title,
  onDelete,
  onRename,
  onStart,
  onStop,
  onUpdate,
  lifecycleBusy,
  lifecycleLocked,
  updating,
  updateDisabled,
}: {
  project: ProjectSummary;
  lang?: LangMeta;
  title: string;
  onDelete: () => void;
  onRename: () => void;
  onStart: () => void;
  onStop: () => void;
  onUpdate?: () => void;
  lifecycleBusy: boolean;
  lifecycleLocked: boolean;
  updating: boolean;
  updateDisabled: boolean;
}) {
  const canUpdate = Boolean(
    onUpdate && project.updateAvailable && project.source !== "path",
  );
  return (
    <Card className="flex flex-col gap-3 p-4">
      <div className="flex min-w-0 items-center gap-2">
        <LangIcon langId={project.lang} lang={lang} />
        <span
          className="min-w-0 flex-1 truncate text-base font-semibold text-fg"
          title={
            project.displayName
              ? `${project.displayName} (${project.name})`
              : project.name
          }
        >
          {title}
        </span>
        {project.source === "path" && (
          <span
            title="Imported by reference — runs live from an external checked-out directory (ADR 0041)"
            className="inline-flex shrink-0 items-center gap-1 rounded-full bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent"
          >
            ⧉ linked
          </span>
        )}
        {project.running && (
          <span className="inline-flex shrink-0 items-center gap-1 rounded-full bg-ok/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-ok">
            <span className="h-1.5 w-1.5 rounded-full bg-ok" /> running
          </span>
        )}
      </div>

      <p className="line-clamp-2 text-sm text-fg-faint">
        {project.description || "No description"}
      </p>

      <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-fg-muted">
        <Stat label="processes" value={project.processes} />
        <Stat label="decisions" value={project.decisions} />
        <Stat label="forms" value={project.forms} />
        <Stat label="workers" value={project.workers} />
      </div>

      <TemplateProvenance project={project} />
      <div className="truncate text-[11px] text-fg-faint">
        → {project.deployTarget}
      </div>

      {/* Lifecycle actions — reachable inline, no drill-in required. */}
      <div className="flex flex-col gap-2">
        {project.running ? (
          <Button
            variant="danger"
            className="nano-touch w-full"
            onClick={onStop}
            disabled={lifecycleLocked}
            aria-busy={lifecycleBusy}
            aria-label={`Stop ${title}`}
          >
            {lifecycleBusy ? <Spinner /> : "■"} Stop
          </Button>
        ) : (
          <Button
            variant="primary"
            className="nano-touch w-full"
            onClick={onStart}
            disabled={lifecycleLocked}
            aria-busy={lifecycleBusy}
            aria-label={`Start ${title}`}
          >
            {lifecycleBusy ? <Spinner /> : "▶"} Start
          </Button>
        )}
        <div className="grid grid-cols-2 gap-2">
          {canUpdate && (
            <Button
              variant="secondary"
              className="nano-touch col-span-2"
              onClick={onUpdate}
              disabled={updateDisabled}
              aria-busy={updating}
              aria-label={`Update ${title} from template`}
              title={`A newer template is available${project.latestVersion ? ` (v${project.latestVersion})` : ""} — review and apply the update`}
            >
              {updating ? <Spinner /> : "↑"} Update
            </Button>
          )}
          <Button
            variant="secondary"
            className="nano-touch"
            onClick={onRename}
            aria-label={`Rename project ${title}`}
          >
            ✎ Rename
          </Button>
          <Button
            variant="secondary"
            className="nano-touch text-danger"
            onClick={onDelete}
            aria-label={
              project.source === "path"
                ? `Remove link to ${title}`
                : `Delete project ${title}`
            }
          >
            ✕ {project.source === "path" ? "Remove" : "Delete"}
          </Button>
        </div>
      </div>

      {/* The IDE is desktop-only (issue #1005 A2): say so plainly rather than
          offering a drill-in that opens a broken editor on a phone. */}
      <p
        className="rounded-md border border-edge bg-inset px-3 py-2 text-xs text-fg-faint"
        role="note"
      >
        The project editor is unavailable on this screen — open the console on a
        larger display to edit this project.
      </p>
    </Card>
  );
}

/// Where this project came from: the scaffold template id plus its origin —
/// a contributing pack (with version when known) or a built-in. Lets a bug in
/// generated project scaffolding be traced straight to the pack or built-in
/// template that produced it. Renders nothing for projects predating the
/// breadcrumb.
function TemplateProvenance({ project }: { project: ProjectSummary }) {
  if (!project.template) return null;
  const from = project.scaffoldedFrom;
  const origin = from
    ? `${from.pack}${from.version ? ` v${from.version}` : ""}`
    : "built-in";
  return (
    <div
      className="mt-2 flex items-center gap-1 truncate text-[11px] text-fg-faint"
      title={`Scaffolded from the “${project.template}” template (${origin})`}
    >
      <span aria-hidden>⧉</span>
      <span className="truncate">
        <span className="text-fg-muted">{project.template}</span>
        <span className="text-fg-faint"> · {origin}</span>
      </span>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <span>
      <span className="font-semibold text-fg">{value}</span> {label}
    </span>
  );
}
