import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  getProject,
  runProject,
  stopProject,
  type AppUi,
  type RunState,
} from "../gen";
import { projectLogs, type ProjectLogLine } from "../lib/api";
import { isAssetIcon } from "../lib/appRailIcon";
import { AppIcon } from "../components/AppIcon";
import {
  CardGrid,
  NavCard,
  SectionLabel,
  useIsNarrow,
} from "@nanobpm/process-explorer/ui";
import { cssVar, TOKEN_KEYS } from "../theme/themes";
import { decideAppViewMessage } from "../lib/appViewMessage";

// The console-integrated control surface for a supervised app (ADR 0057, issue
// #638 — Slice 5). Reached from the left-rail running-apps entry. Headless apps
// get a control-only body (status + ports + Start/Stop/Restart + live logs). A
// *UI* app additionally gets an [App] tab: its own web UI embedded in a
// sandboxed iframe over the same-origin reverse proxy at
// `/console/app-view/<name>/…` (posture A — the app self-authenticates; the
// console injects no credentials and never proxies its WebSocket stream).
//
// Rendered by PersistentAppViews (issue #1040), which keeps ONE instance per
// visited app mounted for the console session and hides the inactive ones, so
// the iframe below survives navigation away and back. `name` is therefore fixed
// per instance, and `active` is the visibility signal for effects that must not
// run from a hidden view.

function statusTone(status: RunState["status"]): {
  label: string;
  dot: string;
  text: string;
} {
  switch (status) {
    case "running":
      return { label: "Running", dot: "bg-success", text: "text-success" };
    case "starting":
      return { label: "Starting", dot: "bg-accent", text: "text-accent" };
    case "stopping":
      return { label: "Stopping", dot: "bg-warn", text: "text-warn" };
    case "error":
      return { label: "Crashed", dot: "bg-danger", text: "text-danger" };
    default:
      return { label: "Stopped", dot: "bg-fg-muted", text: "text-fg-muted" };
  }
}

function streamTone(stream: ProjectLogLine["stream"]): string {
  if (stream === "err") return "text-danger";
  if (stream === "sys") return "text-fg-muted";
  return "text-fg";
}

// A log line tagged with a stable, monotonically-assigned React key so the
// capped buffer can trim its front without reshuffling existing rows' keys.
type KeyedLogLine = ProjectLogLine & { _key: number };

export default function AppView({
  name,
  active,
}: {
  name: string;
  /** Whether this instance is the active route. PersistentAppViews keeps one
   * instance per visited app mounted (hidden, not unmounted) across navigation,
   * so effects that must not act from a hidden view gate on this. */
  active: boolean;
}) {
  const navigate = useNavigate();
  const [runState, setRunState] = useState<RunState | null>(null);
  const [appUi, setAppUi] = useState<AppUi | null>(null);
  const [displayName, setDisplayName] = useState<string>(name);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [busy, setBusy] = useState(false);
  const [logs, setLogs] = useState<KeyedLogLine[]>([]);
  const [tab, setTab] = useState<"app" | "logs">("app");
  // On a phone the App/Logs tab strip is presented as drill-in cards that open
  // a view full-screen; `drilled` tracks whether one is open (false = show the
  // card chooser). The desktop presentation ignores this and keeps the tab
  // strip. Each visited app gets its own kept-alive instance (see
  // PersistentAppViews), so `name` never changes within an instance — the reset
  // below fires once on mount, leaving the chooser as the initial mobile view.
  const [drilled, setDrilled] = useState(false);
  const isNarrow = useIsNarrow();
  // Hide the header icon when the server 404s a missing/oversized/wrong-type
  // asset, mirroring the rail's fallback. Reset when the icon hint or the app
  // route changes so a fixed/renamed icon — or a different app that reuses the
  // same icon string — recovers without a remount.
  const [iconFailed, setIconFailed] = useState(false);
  useEffect(() => setIconFailed(false), [name, appUi?.icon]);
  useEffect(() => setDrilled(false), [name]);
  const logRef = useRef<HTMLDivElement | null>(null);
  const iframeRef = useRef<HTMLIFrameElement | null>(null);
  // Monotonic id source for stable React keys: assigning a per-line id on
  // ingest means trimming the front of the capped buffer never shifts an
  // existing line's key, so React re-renders only the changed rows instead of
  // re-mounting the whole list.
  const logKeyRef = useRef(0);

  const refresh = useCallback(async () => {
    // Use the non-throwing client so we can inspect the HTTP status: the
    // generated client throws the raw response body (a string) on error, which
    // makes 404 (deleted project) indistinguishable from a transport failure.
    const { data, error: err, response } = await getProject({ path: { name } });
    if (response?.status === 404) {
      setNotFound(true);
      return;
    }
    if (err || !data) {
      setError(
        typeof err === "string" ? err : "Failed to load the app's status.",
      );
      // A non-404 failure (offline/500) tells us nothing about whether the app
      // still exists, so clear any stale not-found state from a prior 404 —
      // otherwise a transient error after a real 404 would keep rendering the
      // "no longer exists" screen indefinitely.
      setNotFound(false);
      return;
    }
    setRunState(data.runState);
    setAppUi(data.appUi ?? null);
    setDisplayName(data.config.displayName || data.config.name || name);
    setNotFound(false);
    setError(null);
  }, [name]);

  // Coalesced authoritative re-read. Several push notifications can land in a
  // burst (e.g. `stop requested` → `application stopped` → the app re-binding),
  // so debounce them into a single `getProject` fetch on the trailing edge.
  const refreshTimer = useRef<number | null>(null);
  const scheduleRefresh = useCallback(() => {
    // Trailing-edge debounce: clear and re-arm on each call so a burst of
    // `sys` lines collapses into a single trailing `getProject`, rather than
    // firing on the leading edge and ignoring the rest of the burst.
    if (refreshTimer.current != null) {
      window.clearTimeout(refreshTimer.current);
    }
    refreshTimer.current = window.setTimeout(() => {
      refreshTimer.current = null;
      void refresh();
    }, 150);
  }, [refresh]);
  // Cancel any pending debounce on unmount (or if `refresh` ever changed). With
  // the keep-alive host an instance lives for the whole session, so this
  // cleanup effectively only runs when the console session ends.
  useEffect(
    () => () => {
      if (refreshTimer.current != null) {
        window.clearTimeout(refreshTimer.current);
        refreshTimer.current = null;
      }
    },
    [refresh],
  );

  // Initial load on mount (each visited app gets its own instance).
  useEffect(() => {
    setLogs([]);
    setError(null);
    void refresh();
  }, [refresh]);

  // Re-read the authoritative run state whenever this instance becomes the
  // active route again. Before the keep-alive host, returning to an app REMOUNTED
  // the view — a full refresh. Now the instance survives (hidden), and its live
  // SSE keeps state current while hidden, but a re-activation refresh restores
  // the old freshness guarantee for the paths the stream can't see (e.g. a
  // deleted-then-recreated same-name app, which clears a stale notFound).
  const didMountRef = useRef(false);
  // Mirror `active` into a ref, updated during render so it is current on the
  // very next commit. The message handler below reads the ref instead of the
  // closed-over `active`, so a just-hidden iframe can't slip a navigation
  // through on a stale `active=true` closure before the effect re-runs.
  const activeRef = useRef(active);
  activeRef.current = active;
  useEffect(() => {
    if (!didMountRef.current) {
      didMountRef.current = true;
      return;
    }
    if (active) void refresh();
  }, [active, refresh]);

  // Live logs for the life of this view (history replays first, then tail).
  // The same per-project SSE doubles as our lifecycle *notification* channel:
  // the supervisor emits a `sys` line at every state transition — including
  // `app listening on port N` the instant the ADR 0057 boot handshake re-detects
  // the port after a (re)start, plus stop/exit/compile notes. Re-reading the
  // authoritative run state on each `sys` line (and on stream (re)connect) keeps
  // the badge, ports and headless/embedded state fresh with zero polling — and,
  // crucially, recovers from an *out-of-band* restart (an agent restarting the
  // app while this view is open) that a self-disabling poller used to miss,
  // leaving the app stuck showing "running headless" until a manual remount.
  useEffect(() => {
    if (!name || notFound) return;
    const src = projectLogs(name, (line) => {
      setLogs((prev) => {
        // Cap the buffer at 2000 lines *after* appending: keep the last 1999
        // before the push so the array settles at exactly 2000, not 2001.
        const next =
          prev.length >= 2000 ? prev.slice(prev.length - 1999) : prev.slice();
        next.push({ ...line, _key: logKeyRef.current++ });
        return next;
      });
      if (line.stream === "sys") scheduleRefresh();
    });
    // On (re)connect — including EventSource's automatic reconnect after the
    // host restarts or the stream drops — reconcile any state changed while we
    // were disconnected.
    src.addEventListener("open", scheduleRefresh);
    return () => {
      src.removeEventListener("open", scheduleRefresh);
      src.close();
    };
  }, [name, notFound, scheduleRefresh]);

  // Theme bridge for the embedded app. The app renders in a sandboxed,
  // cross-document iframe, so the console's `--nano-*` custom properties and
  // `data-appearance` don't cascade in. We read the *resolved* token values and
  // appearance straight off <html> — which ThemeProvider keeps current with the
  // built-in palette plus any theme-pack / imported overrides — and postMessage
  // them to the frame. Same-origin proxy (posture A), so we target the console
  // origin. The Urban runtime mirrors these onto its own :root (@nanobpm/urban).
  const postTheme = useCallback(() => {
    const frame = iframeRef.current?.contentWindow;
    if (!frame) return;
    const html = document.documentElement;
    const computed = getComputedStyle(html);
    const vars: Record<string, string> = {};
    for (const key of TOKEN_KEYS) {
      const prop = cssVar(key);
      const value = computed.getPropertyValue(prop).trim();
      if (value) vars[prop] = value;
    }
    const appearance = html.dataset.appearance === "light" ? "light" : "dark";
    frame.postMessage(
      { type: "nano-theme", appearance, vars },
      window.location.origin,
    );
  }, []);

  // The app announces `nano-app-ready` once its runtime installs the listener
  // (→ reply with the current theme), and posts `nano-navigate` when an embedded
  // link should route the console in-host instead of opening a new window (→
  // navigate). Guard on the framing iframe's own window and our own origin so a
  // message from any other frame can't drive a post or a navigation; the routing
  // decision (incl. target whitelist + host-side path construction) lives in the
  // pure `decideAppViewMessage` helper. Navigate actions additionally require
  // `active`: the keep-alive host keeps a hidden instance's iframe alive and
  // posting, and a background app must not yank the console off the route the
  // user is actually on (theme replies stay allowed — they're side-effect-free
  // and keep the hidden frame correctly themed for when it's shown again).
  useEffect(() => {
    const onMessage = (ev: MessageEvent) => {
      if (
        ev.origin !== window.location.origin ||
        ev.source !== iframeRef.current?.contentWindow
      ) {
        return;
      }
      const action = decideAppViewMessage(ev.data);
      if (!action) return;
      if (action.kind === "theme") postTheme();
      else {
        if (!activeRef.current) return;
        if ("stash" in action && action.stash) {
          try {
            sessionStorage.setItem(action.stash.key, action.stash.value);
          } catch {
            // sessionStorage may be full/unavailable. Drop any prior stash so a
            // failed write can't leave a STALE handoff behind for the preview
            // view to render — it must degrade cleanly to the empty state.
            try {
              sessionStorage.removeItem(action.stash.key);
            } catch {
              // Storage wholly unavailable — nothing to clear; empty state anyway.
            }
          }
        }
        navigate(action.path);
      }
    };
    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, [postTheme, navigate]);

  // Re-push whenever the resolved theme on <html> changes — its inline token
  // overrides (theme packs / imports, which ThemeProvider may apply *async* as
  // extension packs load, without any change to the user's selection) or
  // `data-appearance`. Observing the DOM (as TerminalPane does) is robust to
  // React effect ordering: ThemeProvider writes the tokens onto <html> and we
  // read them straight back, so we never post a stale palette.
  useEffect(() => {
    const obs = new MutationObserver(() => postTheme());
    obs.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["style", "data-appearance"],
    });
    return () => obs.disconnect();
  }, [postTheme]);

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight });
  }, [logs]);

  const start = async () => {
    setBusy(true);
    setError(null);
    setLogs([]);
    try {
      // Use the non-throwing client so a 404 (project deleted mid-session)
      // flips the view into `notFound` instead of surfacing as an opaque
      // error while polling/streaming keep running against a gone project.
      const {
        data,
        error: err,
        response,
      } = await runProject({ path: { name } });
      if (response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (err || !data) {
        setError(typeof err === "string" ? err : "Failed to start the app.");
        return;
      }
      setRunState(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stop = async () => {
    setBusy(true);
    setError(null);
    try {
      const {
        data,
        error: err,
        response,
      } = await stopProject({
        path: { name },
      });
      if (response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (err || !data) {
        setError(typeof err === "string" ? err : "Failed to stop the app.");
        return;
      }
      setRunState(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const restart = async () => {
    setBusy(true);
    setError(null);
    try {
      const stopRes = await stopProject({ path: { name } });
      if (stopRes.response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (stopRes.error) {
        setError(
          typeof stopRes.error === "string"
            ? stopRes.error
            : "Failed to stop the app.",
        );
        return;
      }
      // `stop` only *signals* termination; `run` no-ops while the phase is still
      // Starting/Running. Without waiting for the child to actually exit, the
      // run is dropped and the app is left stopped. Poll (bounded ~10s) until the
      // supervisor reports a terminal phase, then start.
      let reachedTerminal = false;
      for (let i = 0; i < 40; i++) {
        const { data, response } = await getProject({ path: { name } });
        if (response?.status === 404) {
          setNotFound(true);
          return;
        }
        const s = data?.runState.status;
        // Only a genuine terminal phase ends the poll. A transient failure
        // (network error or missing `data`) leaves `s` undefined — sleep and
        // keep polling within the bounded window rather than treating it as
        // terminal, which would reintroduce the stop→run race.
        if (s === "stopped" || s === "error") {
          reachedTerminal = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 250));
      }
      // If the bound elapsed without ever observing a terminal phase, the child
      // is still shutting down. Starting now would let `run` no-op against the
      // live process and reintroduce the stop→run race, leaving the app stopped.
      // Surface an error and bail rather than issue a run that may be dropped.
      if (!reachedTerminal) {
        setError("The app did not stop in time to restart. Please try again.");
        return;
      }
      setLogs([]);
      const runRes = await runProject({ path: { name } });
      if (runRes.response?.status === 404) {
        setNotFound(true);
        return;
      }
      if (runRes.error || !runRes.data) {
        setError(
          typeof runRes.error === "string"
            ? runRes.error
            : "Failed to start the app.",
        );
        return;
      }
      setRunState(runRes.data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  if (notFound) {
    return (
      <div className="p-8">
        <p className="text-fg-muted">
          App <span className="font-medium text-fg">{name}</span> no longer
          exists.{" "}
          <Link to="/projects" className="text-accent underline">
            Back to Studio
          </Link>
        </p>
      </div>
    );
  }

  const status = runState?.status ?? "stopped";
  const tone = statusTone(status);
  // Treat every non-terminal phase as "in flight" so the Stop/Restart controls
  // (not Start) show while the app is starting or stopping. Showing Start during
  // a `stopping` transition invites an invalid action that races the shutdown.
  const running =
    status === "running" || status === "starting" || status === "stopping";
  const headless = !appUi?.enabled || appUi?.port == null;
  // The embedded UI can only be served while the app's own server is up.
  const canEmbed = !headless && status === "running";
  // Headless apps have no App tab; coerce the active tab to logs for them.
  const activeTab: "app" | "logs" = headless ? "logs" : tab;
  // The app's declared landing path is resolved *within* its own app-view
  // namespace, and may not escape it. A manifest `path` like `../../console/api`
  // would otherwise resolve (in the browser) to an arbitrary same-origin console
  // route and point the iframe there, so we resolve the path against the
  // app-view base and keep it only when the result still lives under that base.
  const appViewBase = `/console/app-view/${encodeURIComponent(name)}/`;
  const appSrc = (() => {
    const rawPath = (appUi?.path ?? "/").replace(/^\/+/, "");
    try {
      const resolved = new URL(
        rawPath,
        `${window.location.origin}${appViewBase}`,
      );
      if (
        resolved.origin === window.location.origin &&
        resolved.pathname.startsWith(appViewBase)
      ) {
        return resolved.pathname + resolved.search + resolved.hash;
      }
    } catch {
      // Malformed path ⇒ fall back to the app-view root below.
    }
    return appViewBase;
  })();
  // An app-shipped image icon is served path-guarded from the icon route;
  // bundled glyph names are a rail-only concern. Classification + themed
  // rendering are shared with the rail via `isAssetIcon` / `AppIcon` (single
  // source of truth — no drift).
  const iconIsAsset = isAssetIcon(appUi?.icon);
  const btn =
    "rounded-md px-3 py-1.5 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50";

  // Start/Stop/Restart cluster, shared by the desktop header and the mobile
  // chooser so lifecycle controls stay reachable in either presentation.
  const controls = running ? (
    <>
      <button
        type="button"
        onClick={restart}
        disabled={busy}
        className={`${btn} border border-edge text-fg hover:bg-hover`}
      >
        Restart
      </button>
      <button
        type="button"
        onClick={stop}
        disabled={busy}
        className={`${btn} bg-danger text-white hover:opacity-90`}
      >
        Stop
      </button>
    </>
  ) : (
    <button
      type="button"
      onClick={start}
      disabled={busy}
      className={`${btn} bg-accent text-white hover:opacity-90`}
    >
      Start
    </button>
  );

  // The app's active view — the embedded UI iframe (or its start-prompt
  // placeholder) or the live log stream. Rendered verbatim by the desktop tab
  // body and the mobile full-screen drill-in: the iframe sandbox flags, the
  // theme bridge (`onLoad`/`iframeRef`) and the log stream are identical in both,
  // so the mobile branch changes layout only — never the bridge. `flex-1` /
  // `min-h-0` make it full-bleed under whichever header is above it, so the
  // embedded app gets full-bleed height on mobile.
  const body =
    activeTab === "app" && !headless ? (
      canEmbed ? (
        <iframe
          // Sandboxed embed of the app's own UI. `allow-same-origin` is
          // required because the app is proxied same-origin (posture A) so
          // its fetches/cookies work; `referrerpolicy=no-referrer` avoids
          // leaking console URLs. No `allow-top-navigation` — a framed app
          // can't navigate the studio away.
          //
          // `allow-popups-to-escape-sandbox` pairs with `allow-popups`: an
          // app's external links (a grid `linkField` PR URL, the "API docs"
          // badge — plain `target=_blank rel=noopener noreferrer` anchors)
          // must open as a normal new tab. With only `allow-popups`, the
          // popup would INHERIT this sandbox, and Safari then refuses to open
          // it on a trusted left-click (right-click "open in new tab" bypasses
          // the frame sandbox, which is why that still worked). The escape
          // flag lets the new tab drop the sandbox; `rel=noopener noreferrer`
          // already severs any back-reference to the opener. In-host targets
          // (processExplorer) don't rely on this — they route via the
          // nano-navigate postMessage bridge, no popup.
          key={appSrc}
          ref={iframeRef}
          title={`${appUi?.label || displayName} UI`}
          src={appSrc}
          onLoad={postTheme}
          className="min-h-0 flex-1 border-0 bg-app"
          sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-same-origin allow-downloads"
          referrerPolicy="no-referrer"
        />
      ) : (
        <div className="flex min-h-0 flex-1 items-center justify-center bg-inset px-6 text-center text-sm text-fg-muted">
          <p>
            Start the app to view its UI.
            <br />
            The embedded view loads once the app is running.
          </p>
        </div>
      )
    ) : (
      <div
        ref={logRef}
        className="min-h-0 flex-1 overflow-auto bg-inset px-6 py-3 font-mono text-xs leading-relaxed"
      >
        {logs.length === 0 ? (
          <p className="text-fg-muted">
            No output yet. Start the app to see logs.
          </p>
        ) : (
          logs.map((l) => (
            <div key={l._key} className={streamTone(l.stream)}>
              {l.text}
            </div>
          ))
        )}
      </div>
    );

  // Mobile drill-in cards: one card per available view. A UI app offers the
  // embedded App and the Logs; a headless app has no server to embed, so it
  // stays Logs-only (no UI card). Tapping a card opens that view full-screen.
  const mobileViews: {
    key: "app" | "logs";
    label: string;
    description: string;
  }[] = [
    ...(headless
      ? []
      : [
          {
            key: "app" as const,
            label: "App UI",
            description: canEmbed
              ? "Open the embedded app"
              : "Start the app to view its UI",
          },
        ]),
    {
      key: "logs" as const,
      label: "Logs",
      description: "Live output from the app",
    },
  ];

  if (isNarrow) {
    return (
      <div className="flex h-full flex-col">
        {drilled ? (
          <>
            <div className="nano-safe-x flex items-center gap-2 border-b border-edge px-2 py-2">
              <button
                type="button"
                onClick={() => setDrilled(false)}
                className="nano-touch flex items-center gap-1 rounded-md px-2 text-sm font-medium text-fg-muted hover:bg-hover hover:text-fg"
              >
                <span aria-hidden>←</span> Back
              </button>
              <span className="min-w-0 flex-1 truncate text-sm font-medium text-fg">
                {appUi?.label || displayName}
              </span>
              <span className="shrink-0 text-xs font-medium uppercase tracking-wide text-fg-muted">
                {activeTab === "app" ? "App" : "Logs"}
              </span>
            </div>
            {body}
          </>
        ) : (
          <>
            <header className="nano-safe-x border-b border-edge px-4 py-3">
              <div className="flex items-center gap-2">
                {iconIsAsset && !iconFailed && (
                  <AppIcon
                    name={name}
                    icon={appUi?.icon}
                    sizeClass="h-6 w-6 rounded"
                    onError={() => setIconFailed(true)}
                  />
                )}
                <span
                  className={`inline-block h-2.5 w-2.5 rounded-full ${tone.dot}`}
                  aria-hidden="true"
                />
                <h1 className="min-w-0 flex-1 truncate text-base font-semibold text-fg">
                  {appUi?.label || displayName}
                </h1>
                <span className={`shrink-0 text-xs ${tone.text}`}>
                  {tone.label}
                </span>
              </div>
              <dl className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-fg-muted">
                <div className="flex gap-1.5">
                  <dt>Mode</dt>
                  <dd className="text-fg">{headless ? "Headless" : "UI"}</dd>
                </div>
                {appUi?.port != null && (
                  <div className="flex gap-1.5">
                    <dt>UI port</dt>
                    <dd className="font-mono text-fg">{appUi.port}</dd>
                  </div>
                )}
              </dl>
              <div className="mt-3 flex flex-wrap gap-2">{controls}</div>
              {runState?.lastError && (
                <p className="mt-2 text-xs text-danger">{runState.lastError}</p>
              )}
              {error && <p className="mt-2 text-xs text-danger">{error}</p>}
            </header>
            <div className="nano-safe-x min-h-0 flex-1 overflow-auto p-4">
              <SectionLabel>Open</SectionLabel>
              <div className="mt-2">
                <CardGrid>
                  {mobileViews.map((v) => (
                    <NavCard
                      key={v.key}
                      label={v.label}
                      description={v.description}
                      onClick={() => {
                        setTab(v.key);
                        setDrilled(true);
                      }}
                    />
                  ))}
                </CardGrid>
              </div>
            </div>
          </>
        )}
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-edge px-6 py-4">
        <div className="flex items-center gap-3">
          {iconIsAsset && !iconFailed && (
            <AppIcon
              name={name}
              icon={appUi?.icon}
              sizeClass="h-6 w-6 rounded"
              onError={() => setIconFailed(true)}
            />
          )}
          <span
            className={`inline-block h-2.5 w-2.5 rounded-full ${tone.dot}`}
            aria-hidden="true"
          />
          <h1 className="text-lg font-semibold text-fg">
            {appUi?.label || displayName}
          </h1>
          <span className={`text-sm ${tone.text}`}>{tone.label}</span>
          <span className="ml-auto flex items-center gap-2">{controls}</span>
        </div>
        <dl className="mt-3 flex flex-wrap gap-x-6 gap-y-1 text-xs text-fg-muted">
          <div className="flex gap-1.5">
            <dt>Project</dt>
            <dd className="font-mono text-fg">{name}</dd>
          </div>
          <div className="flex gap-1.5">
            <dt>Mode</dt>
            <dd className="text-fg">{headless ? "Headless" : "UI"}</dd>
          </div>
          {appUi?.port != null && (
            <div className="flex gap-1.5">
              <dt>UI port</dt>
              <dd className="font-mono text-fg">{appUi.port}</dd>
            </div>
          )}
          {runState?.pid != null && (
            <div className="flex gap-1.5">
              <dt>PID</dt>
              <dd className="font-mono text-fg">{runState.pid}</dd>
            </div>
          )}
        </dl>
        {!headless && appUi?.port != null && (
          <p className="mt-2 text-xs text-fg-muted">
            Embedded UI proxied from port{" "}
            <span className="font-mono">{appUi.port}</span> on this host.
          </p>
        )}
        {runState?.lastError && (
          <p className="mt-2 text-xs text-danger">{runState.lastError}</p>
        )}
        {error && <p className="mt-2 text-xs text-danger">{error}</p>}
      </header>

      <section className="flex min-h-0 flex-1 flex-col">
        <div className="flex border-b border-edge px-6 text-xs font-medium uppercase tracking-wide">
          {!headless && (
            <button
              type="button"
              onClick={() => setTab("app")}
              className={`-mb-px border-b-2 px-2 py-2 ${
                activeTab === "app"
                  ? "border-accent text-fg"
                  : "border-transparent text-fg-muted hover:text-fg"
              }`}
            >
              App
            </button>
          )}
          <button
            type="button"
            onClick={() => setTab("logs")}
            className={`-mb-px border-b-2 px-2 py-2 ${
              activeTab === "logs"
                ? "border-accent text-fg"
                : "border-transparent text-fg-muted hover:text-fg"
            }`}
          >
            Logs
          </button>
        </div>

        {body}
      </section>
    </div>
  );
}
