import { useEffect, useMemo, useRef, useState } from "react";
import {
  getIdeConfig,
  getServerConfig,
  setSlaMode,
  type ConfigDependency,
  type IdeConfig,
  type LangPackConfig,
  type ServerConfig,
  type SlaModeConfig,
} from "../gen";
import { useTheme } from "../theme/ThemeProvider";
import { TOKEN_KEYS, type ThemeSpec } from "../theme/themes";
import {
  getTerminalConfig,
  setTerminalConfig,
  type TerminalConfig,
} from "../lib/api";
import {
  Button,
  Card,
  ErrorText,
  PageHeader,
  SectionLabel,
} from "@nanobpm/process-explorer/ui";

type Tab = "server" | "ide" | "appearance";

export default function Config() {
  const [tab, setTab] = useState<Tab>("server");
  return (
    <div className="mx-auto max-w-4xl p-6">
      <PageHeader
        title="Configuration"
        subtitle="Server runtime behaviour, IDE toolchains, and the console's appearance."
      />

      <div className="mb-6 inline-flex rounded-lg border border-edge bg-panel p-1">
        {(
          [
            ["server", "Server"],
            ["ide", "IDE"],
            ["appearance", "Appearance"],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`rounded-md px-4 py-1.5 text-sm transition-colors ${
              tab === id
                ? "bg-accent/10 font-medium text-accent-strong"
                : "text-fg-muted hover:text-fg"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === "server" ? (
        <ServerPane />
      ) : tab === "ide" ? (
        <IdePane />
      ) : (
        <AppearancePane />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Appearance pane — light/dark/system modes, theme packs, custom theme import
// ---------------------------------------------------------------------------

/** Render a theme's key colours as chips so the gallery previews without applying. */
function ThemeSwatch({ spec }: { spec: ThemeSpec }) {
  const base =
    spec.appearance === "dark"
      ? {
          app: "#0b0b10",
          raised: "#16161f",
          text: "#f2f2f7",
          accent: "#8b5cf6",
        }
      : {
          app: "#f5f5f9",
          raised: "#ffffff",
          text: "#1a1a22",
          accent: "#7c3aed",
        };
  const t = { ...base, ...spec.tokens };
  return (
    <span
      className="inline-flex items-center gap-1 rounded-md border border-edge p-1"
      style={{ background: t.app }}
      aria-hidden="true"
    >
      {[t.raised, t.text, t.accent].map((c, i) => (
        <span
          key={i}
          className="h-3.5 w-3.5 rounded-full"
          style={{ background: c }}
        />
      ))}
    </span>
  );
}

const THEME_JSON_EXAMPLE = `{
  "id": "my-theme",
  "label": "My Theme",
  "appearance": "dark",
  "tokens": { "accent": "#f472b6", "app": "#0c0a12" }
}`;

function AppearancePane() {
  const {
    selection,
    select,
    packThemes,
    importedThemes,
    importTheme,
    removeImportedTheme,
  } = useTheme();
  const [json, setJson] = useState("");
  const [importErr, setImportErr] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement | null>(null);

  const modes = [
    { mode: "light", label: "Light", blurb: "Bright surfaces, dark text." },
    { mode: "dark", label: "Dark", blurb: "The classic console look." },
    { mode: "system", label: "System", blurb: "Follow the OS appearance." },
  ] as const;

  const doImport = (text: string) => {
    const err = importTheme(text);
    setImportErr(err);
    if (!err) setJson("");
  };

  const themeCard = (t: ThemeSpec, removable: boolean) => {
    const active = selection.mode === "theme" && selection.id === t.id;
    return (
      <Card
        key={t.id}
        className={`flex items-center justify-between p-3 ${active ? "border-accent/60 ring-1 ring-accent/40" : ""}`}
      >
        <button
          onClick={() => select({ mode: "theme", id: t.id })}
          className="flex min-w-0 items-center gap-3 text-left"
        >
          <ThemeSwatch spec={t} />
          <span>
            <span className="block text-sm font-medium text-fg">{t.label}</span>
            <span className="block text-xs text-fg-faint">
              {t.appearance} base · {Object.keys(t.tokens).length} token
              {Object.keys(t.tokens).length === 1 ? "" : "s"}
            </span>
          </span>
        </button>
        <div className="flex shrink-0 items-center gap-2">
          {active && (
            <span className="text-xs font-medium text-accent-strong">
              active
            </span>
          )}
          {removable && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => removeImportedTheme(t.id)}
            >
              remove
            </Button>
          )}
        </div>
      </Card>
    );
  };

  return (
    <div className="space-y-8">
      <section>
        <SectionLabel>Mode</SectionLabel>
        <div className="grid gap-3 sm:grid-cols-3">
          {modes.map((m) => {
            const active = selection.mode === m.mode;
            return (
              <button
                key={m.mode}
                onClick={() => select({ mode: m.mode })}
                className={`rounded-xl border p-4 text-left transition-colors ${
                  active
                    ? "border-accent/60 bg-accent/10"
                    : "border-edge bg-raised hover:bg-hover"
                }`}
              >
                <div
                  className={`text-sm font-medium ${active ? "text-accent-strong" : "text-fg"}`}
                >
                  {m.label}
                </div>
                <div className="mt-0.5 text-xs text-fg-faint">{m.blurb}</div>
              </button>
            );
          })}
        </div>
      </section>

      <section>
        <SectionLabel>Theme packs</SectionLabel>
        {packThemes.length > 0 ? (
          <div className="grid gap-2">
            {packThemes.map((t) => themeCard(t, false))}
          </div>
        ) : (
          <p className="text-sm text-fg-faint">
            No theme packs installed. Browse the{" "}
            <a
              href="/console/extensions"
              className="text-accent-strong hover:underline"
            >
              extension marketplace
            </a>{" "}
            for <code className="font-mono text-xs">nano-ide-theme-*</code>{" "}
            packs, or import a theme below.
          </p>
        )}
      </section>

      <section>
        <SectionLabel>Imported themes</SectionLabel>
        {importedThemes.length > 0 && (
          <div className="mb-3 grid gap-2">
            {importedThemes.map((t) => themeCard(t, true))}
          </div>
        )}
        <Card className="p-3">
          <div className="mb-2 text-xs text-fg-muted">
            Paste a theme JSON (or load a{" "}
            <code className="font-mono">.json</code> file). Tokens:{" "}
            <code className="font-mono text-[11px] text-fg-faint">
              {TOKEN_KEYS.join(" ")}
            </code>
          </div>
          <textarea
            value={json}
            onChange={(e) => setJson(e.target.value)}
            placeholder={THEME_JSON_EXAMPLE}
            rows={5}
            spellCheck={false}
            className="mb-2 w-full rounded-md border border-edge bg-inset px-3 py-2 font-mono text-xs text-fg placeholder:text-fg-faint outline-none focus:border-accent"
          />
          {importErr && (
            <div className="mb-2">
              <ErrorText>{importErr}</ErrorText>
            </div>
          )}
          <div className="flex items-center gap-2">
            <Button
              variant="primary"
              size="sm"
              disabled={!json.trim()}
              onClick={() => doImport(json)}
            >
              Import & apply
            </Button>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => fileRef.current?.click()}
            >
              Load file…
            </Button>
            <input
              ref={fileRef}
              type="file"
              accept=".json,application/json"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0];
                if (!f) return;
                void f.text().then(doImport);
                e.target.value = "";
              }}
            />
          </div>
        </Card>
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Server pane — basic (guitar pedal) / advanced (env params)
// ---------------------------------------------------------------------------

function ServerPane() {
  const [cfg, setCfg] = useState<ServerConfig | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [mode, setMode] = useState<"basic" | "advanced">("basic");

  useEffect(() => {
    getServerConfig({ throwOnError: true })
      .then(({ data }) => setCfg(data))
      .catch((e) => setErr(String(e)));
  }, []);

  if (err) return <ErrorText>{err}</ErrorText>;
  if (!cfg) return <div className="text-sm text-fg-faint">Loading…</div>;

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div className="text-sm text-fg-muted">
          The SLA mode is switchable live below; other parameters are set on
          startup via the environment.
        </div>
        <div className="inline-flex rounded-md border border-edge bg-panel p-0.5 text-xs">
          {(["basic", "advanced"] as const).map((m) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={`rounded px-3 py-1 capitalize transition-colors ${
                mode === m
                  ? "bg-accent/10 font-medium text-accent-strong"
                  : "text-fg-muted hover:text-fg"
              }`}
            >
              {m}
            </button>
          ))}
        </div>
      </div>

      {mode === "basic" ? (
        <>
          <SlaPedal sla={cfg.slaMode} onApplied={setCfg} />
          <TerminalToggle />
        </>
      ) : (
        <AdvancedParams cfg={cfg} />
      )}
    </div>
  );
}

// The integrated-terminal enablement toggle (issue #500). A shell is arbitrary
// code execution, so it is off by default and gated server-side; this persists
// the console-managed setting. When `NANO_CONSOLE_TERMINAL` has locked it off,
// the control is disabled and explains why.
function TerminalToggle() {
  const [cfg, setCfg] = useState<TerminalConfig | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    getTerminalConfig()
      .then(setCfg)
      .catch((e) => setErr(String(e)));
  }, []);

  const toggle = (next: boolean) => {
    setBusy(true);
    setErr(null);
    setTerminalConfig(next)
      .then(setCfg)
      .catch((e) => setErr(e instanceof Error ? e.message : String(e)))
      .finally(() => setBusy(false));
  };

  return (
    <div className="mt-6 rounded-md border border-edge bg-panel p-4">
      <div className="flex items-center justify-between gap-4">
        <div>
          <SectionLabel>Integrated terminal</SectionLabel>
          <p className="mt-1 max-w-prose text-sm text-fg-muted">
            Adds a shell to the IDE bottom pane on this machine. It's arbitrary
            code execution, so it's off by default and only reachable from
            localhost.
          </p>
        </div>
        <button
          role="switch"
          aria-checked={cfg?.enabled ?? false}
          aria-label="Integrated terminal"
          disabled={!cfg || cfg.locked || busy}
          onClick={() => cfg && toggle(!cfg.enabled)}
          className={`relative h-6 w-11 shrink-0 rounded-full transition-colors disabled:opacity-40 ${
            cfg?.enabled ? "bg-accent" : "bg-edge"
          }`}
        >
          <span
            className={`absolute top-0.5 h-5 w-5 rounded-full bg-white transition-transform ${
              cfg?.enabled ? "translate-x-[22px]" : "translate-x-0.5"
            }`}
          />
        </button>
      </div>
      {cfg?.locked && (
        <p className="mt-2 text-xs text-fg-faint">
          Locked off by <code>NANO_CONSOLE_TERMINAL</code>; it can't be enabled
          from the console.
        </p>
      )}
      {err && <ErrorText>{err}</ErrorText>}
    </div>
  );
}

// The single-knob guitar-effects pedal for the SLA mode. The knob reflects the
// server's current mode. When the server reports `switchable`, turning the knob
// applies the mode live (and the server propagates it cluster-wide); otherwise
// it previews the alternative and shows how to apply it on startup.
// The pedal chassis is deliberately skeuomorphic "hardware" — its metals and
// LED colours are fixed rather than theme-token driven.
function SlaPedal({
  sla,
  onApplied,
}: {
  sla: SlaModeConfig;
  onApplied: (cfg: ServerConfig) => void;
}) {
  const [preview, setPreview] = useState(sla.current);
  const [applying, setApplying] = useState(false);
  const [applyErr, setApplyErr] = useState<string | null>(null);
  const dragging = useRef(false);
  const knobRef = useRef<HTMLDivElement | null>(null);

  // Keep the knob in sync with the server's live mode (e.g. after a
  // cluster-wide switch initiated elsewhere refetches, or on apply).
  useEffect(() => setPreview(sla.current), [sla.current]);

  const idx = Math.max(
    0,
    sla.options.findIndex((o) => o.id === preview),
  );
  const selected = sla.options[idx] ?? sla.options[0];
  // Two detents: left = option 0, right = option 1. Map to a rotation.
  const angle = idx === 0 ? -48 : 48;
  const changed = preview !== sla.current;

  // Move the knob to `mode`. When the server allows live switching, apply it
  // immediately (optimistic knob move, revert on failure); otherwise just
  // preview so the restart hint shows the target env value.
  const select = (mode: string) => {
    if (mode === preview) return;
    setPreview(mode);
    if (!sla.switchable) return;
    setApplyErr(null);
    setApplying(true);
    setSlaMode({ body: { mode }, throwOnError: true })
      .then(({ data }) => onApplied(data))
      .catch((e) => {
        setApplyErr(String(e));
        setPreview(sla.current);
      })
      .finally(() => setApplying(false));
  };

  const setFromPointer = (clientX: number) => {
    const el = knobRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    // Left half selects option 0, right half option 1.
    const left = clientX < r.left + r.width / 2;
    select(sla.options[left ? 0 : 1].id);
  };

  return (
    <div className="flex flex-col items-center gap-6 sm:flex-row sm:items-start">
      {/* Pedal chassis */}
      <div
        className="relative w-64 shrink-0 select-none rounded-2xl p-5 shadow-2xl"
        style={{
          background:
            "linear-gradient(160deg,#3f3f46 0%,#27272a 45%,#18181b 100%)",
          boxShadow:
            "0 10px 30px rgba(0,0,0,.6), inset 0 1px 0 rgba(255,255,255,.08)",
          border: "1px solid #000",
        }}
      >
        {/* corner screws */}
        {[
          "left-2 top-2",
          "right-2 top-2",
          "left-2 bottom-2",
          "right-2 bottom-2",
        ].map((pos) => (
          <div
            key={pos}
            className={`absolute ${pos} h-3 w-3 rounded-full`}
            style={{
              background: "radial-gradient(circle at 35% 35%,#a1a1aa,#52525b)",
              boxShadow: "inset 0 0 0 1px rgba(0,0,0,.6)",
            }}
          >
            <div className="absolute left-1/2 top-1/2 h-[1px] w-2 -translate-x-1/2 -translate-y-1/2 rotate-45 bg-black/60" />
          </div>
        ))}

        <div className="mb-1 text-center text-[10px] font-bold uppercase tracking-[0.25em] text-violet-300/80">
          nano SLA
        </div>
        <div className="mb-4 flex items-center justify-center gap-2">
          <span
            className="inline-block h-2 w-2 rounded-full"
            style={{
              background: idx === 0 ? "#f59e0b" : "#22d3ee",
              boxShadow: `0 0 8px ${idx === 0 ? "#f59e0b" : "#22d3ee"}`,
            }}
          />
          <span className="text-[9px] uppercase tracking-widest text-[#a1a1aa]">
            {idx === 0 ? "latency" : "admission"}
          </span>
        </div>

        {/* Knob */}
        <div className="flex justify-center py-2">
          <div
            ref={knobRef}
            role="slider"
            aria-label="SLA mode"
            aria-valuetext={selected.label}
            tabIndex={0}
            onPointerDown={(e) => {
              dragging.current = true;
              (e.target as HTMLElement).setPointerCapture?.(e.pointerId);
              setFromPointer(e.clientX);
            }}
            onPointerMove={(e) => {
              if (dragging.current) setFromPointer(e.clientX);
            }}
            onPointerUp={() => (dragging.current = false)}
            onKeyDown={(e) => {
              if (e.key === "ArrowLeft") select(sla.options[0].id);
              if (e.key === "ArrowRight") select(sla.options[1].id);
            }}
            className="relative h-28 w-28 cursor-pointer rounded-full"
            style={{
              background:
                "radial-gradient(circle at 50% 30%,#52525b 0%,#27272a 60%,#09090b 100%)",
              boxShadow:
                "0 6px 14px rgba(0,0,0,.7), inset 0 2px 4px rgba(255,255,255,.12), inset 0 -3px 6px rgba(0,0,0,.6)",
            }}
          >
            {/* knurled ring */}
            <div className="absolute inset-2 rounded-full border border-black/40" />
            {/* pointer that rotates with the value — the wrapper fills the
                knob (inset-0) so it pivots around the knob's exact centre */}
            <div
              className="absolute inset-0 transition-transform duration-300"
              style={{
                transform: `rotate(${angle}deg)`,
                transformOrigin: "50% 50%",
              }}
            >
              <div
                className="absolute left-1/2 top-2 h-4 w-1 -translate-x-1/2 rounded-full"
                style={{
                  background: idx === 0 ? "#f59e0b" : "#22d3ee",
                  boxShadow: `0 0 6px ${idx === 0 ? "#f59e0b" : "#22d3ee"}`,
                }}
              />
            </div>
          </div>
        </div>

        {/* detent labels */}
        <div className="mt-2 flex justify-between text-[9px] uppercase tracking-wider text-[#a1a1aa]">
          <button
            className="hover:text-amber-300"
            onClick={() => select(sla.options[0].id)}
          >
            ◄ reject
          </button>
          <button
            className="hover:text-cyan-300"
            onClick={() => select(sla.options[1].id)}
          >
            accept ►
          </button>
        </div>
      </div>

      {/* Explanation + apply hint */}
      <div className="flex-1">
        <div className="mb-1 text-sm font-semibold text-fg">
          {selected.label}
        </div>
        <div className="mb-3 text-xs uppercase tracking-wider text-accent-strong/80">
          {selected.tagline}
        </div>
        <p className="mb-4 text-sm leading-relaxed text-fg-muted">
          {selected.description}
        </p>

        <div className="rounded-lg border border-edge bg-raised p-3 text-xs text-fg-muted">
          <div className="mb-1">
            Current: <span className="font-mono text-fg">{sla.current}</span>{" "}
            <span className="text-fg-faint">
              (source: {sla.source === "default" ? "default" : "environment"})
            </span>
          </div>
          {sla.switchable ? (
            applyErr ? (
              <div className="text-danger">Switch failed: {applyErr}</div>
            ) : applying ? (
              <div className="text-info">
                Applying {preview} across the cluster…
              </div>
            ) : (
              <div className="text-ok">
                Live. Turning the knob switches the mode immediately and
                propagates it cluster-wide. On restart it reseeds from{" "}
                <code className="rounded bg-inset px-1 py-0.5 font-mono text-ok">
                  NANOBPMN_SLA_MODE
                </code>
                .
              </div>
            )
          ) : changed ? (
            <div className="text-warn">
              Preview only. To apply, restart with{" "}
              <code className="rounded bg-inset px-1 py-0.5 font-mono text-warn">
                NANOBPMN_SLA_MODE={preview}
              </code>
              .
            </div>
          ) : (
            <div className="text-fg-faint">
              Turn the knob to preview the other mode; the mode is set on
              startup.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function AdvancedParams({ cfg }: { cfg: ServerConfig }) {
  const groups = useMemo(() => {
    const m = new Map<string, typeof cfg.params>();
    for (const p of cfg.params) {
      if (!m.has(p.category)) m.set(p.category, []);
      m.get(p.category)!.push(p);
    }
    return [...m.entries()];
  }, [cfg]);

  return (
    <div className="space-y-6">
      {groups.map(([cat, params]) => (
        <section key={cat}>
          <SectionLabel>{cat}</SectionLabel>
          <div className="overflow-hidden rounded-lg border border-edge">
            <table className="w-full text-sm">
              <tbody>
                {params.map((p, i) => (
                  <tr
                    key={p.key}
                    className={i % 2 ? "bg-raised/60" : "bg-raised/20"}
                  >
                    <td className="w-1/3 border-b border-edge/60 px-3 py-2 align-top">
                      <div className="text-fg">{p.label}</div>
                      <div className="font-mono text-[11px] text-fg-faint">
                        {p.key}
                      </div>
                    </td>
                    <td className="border-b border-edge/60 px-3 py-2 align-top">
                      <div className="mb-0.5 flex items-center gap-2">
                        {p.value !== null ? (
                          <span className="rounded bg-ok/10 px-1.5 py-0.5 font-mono text-xs text-ok">
                            {p.value}
                          </span>
                        ) : (
                          <span className="font-mono text-xs text-fg-faint">
                            default: {p.default}
                          </span>
                        )}
                      </div>
                      <div className="text-xs text-fg-faint">
                        {p.description}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// IDE pane — toolchain dependencies + language packs
// ---------------------------------------------------------------------------

function IdePane() {
  const [cfg, setCfg] = useState<IdeConfig | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    getIdeConfig({ throwOnError: true })
      .then(({ data }) => setCfg(data))
      .catch((e) => setErr(String(e)));
  }, []);

  if (err) return <ErrorText>{err}</ErrorText>;
  if (!cfg) return <div className="text-sm text-fg-faint">Loading…</div>;

  const missing = cfg.dependencies.filter((d) => !d.present);

  return (
    <div className="space-y-6">
      {missing.length > 0 && (
        <section>
          <SectionLabel>Missing dependencies</SectionLabel>
          <div className="space-y-2">
            {missing.map((d) => (
              <DepCard key={d.id} d={d} />
            ))}
          </div>
        </section>
      )}

      <section>
        <SectionLabel>Toolchains</SectionLabel>
        <div className="space-y-2">
          {cfg.dependencies.map((d) => (
            <ToolchainRow key={d.id} d={d} />
          ))}
        </div>
      </section>

      <section>
        <SectionLabel>Language packs</SectionLabel>
        <div className="space-y-3">
          {cfg.langPacks.map((p) => (
            <LangPackCard key={p.id} p={p} />
          ))}
        </div>
      </section>
    </div>
  );
}

function DepCard({ d }: { d: ConfigDependency }) {
  return (
    <div className="rounded-lg border border-warn/30 bg-warn/5 p-3">
      <div className="text-sm">
        <span className="font-semibold text-warn">
          {d.name} is not installed.
        </span>{" "}
        <span className="text-fg-muted">{d.purpose}</span>
      </div>
      <div className="mt-1 text-xs text-fg-muted">{d.hint}</div>
      <a
        href={d.installUrl}
        target="_blank"
        rel="noopener noreferrer"
        className="mt-2 inline-block text-xs font-medium text-accent-strong hover:underline"
      >
        Install instructions →
      </a>
    </div>
  );
}

function ToolchainRow({ d }: { d: ConfigDependency }) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-edge bg-raised px-3 py-2">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span
            className={`inline-block h-2 w-2 rounded-full ${
              d.present ? "bg-ok" : "bg-fg-faint/50"
            }`}
          />
          <span className="text-sm text-fg">{d.name}</span>
        </div>
        <div className="truncate font-mono text-[11px] text-fg-faint">
          {d.present ? (d.version ?? d.bin) : d.bin}
        </div>
      </div>
      {d.present ? (
        <span className="rounded bg-ok/10 px-2 py-0.5 text-xs text-ok">
          installed
        </span>
      ) : (
        <a
          href={d.installUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs font-medium text-accent-strong hover:underline"
        >
          Install →
        </a>
      )}
    </div>
  );
}

function LangPackCard({ p }: { p: LangPackConfig }) {
  return (
    <div className="rounded-lg border border-edge bg-raised p-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-fg">{p.displayName}</span>
          {p.builtin && (
            <span className="rounded bg-hover px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-fg-muted">
              built-in
            </span>
          )}
        </div>
        <span
          className={`rounded px-2 py-0.5 text-xs ${
            p.available ? "bg-ok/10 text-ok" : "bg-warn/10 text-warn"
          }`}
        >
          {p.available ? "ready" : "toolchain missing"}
        </span>
      </div>

      {p.detect.length > 0 && (
        <div className="mt-1 font-mono text-[11px] text-fg-faint">
          probe: {p.detect.join(" ")}
        </div>
      )}

      {p.configFields.length > 0 && (
        <div className="mt-3 space-y-2 border-t border-edge pt-2">
          {p.configFields.map((f) => (
            <div key={f.key} className="text-xs">
              <div className="flex items-center gap-2">
                <span className="text-fg">{f.label}</span>
                {f.value !== null ? (
                  <span className="rounded bg-ok/10 px-1.5 py-0.5 font-mono text-ok">
                    {f.value}
                  </span>
                ) : (
                  <span className="font-mono text-fg-faint">
                    default: {f.default ?? "—"}
                  </span>
                )}
                {f.env && (
                  <span className="font-mono text-[10px] text-fg-faint">
                    {f.env}
                  </span>
                )}
              </div>
              {f.description && (
                <div className="text-fg-faint">{f.description}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
