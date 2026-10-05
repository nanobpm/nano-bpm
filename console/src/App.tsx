import {
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  NavLink,
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
  useSearchParams,
} from "react-router-dom";
import Topology from "./views/Topology";
import { useTheme } from "./theme/ThemeProvider";
import {
  BottomSheet,
  CardGrid,
  NavCard,
  SectionLabel,
  useIsNarrow,
} from "./components/ui";
import {
  getExtensions,
  getMarketplace,
  getTopology,
  listProjects,
  type ProjectSummary,
} from "./gen";
import { registerFileTypesFromOverview } from "./lib/editorLang";
import { isAssetIcon } from "./lib/appRailIcon";
import { AppIcon } from "./components/AppIcon";
import { setIntellisenseFromOverview } from "./lib/langIntellisense";
import { IS_STUDIO, CONSOLE_PROFILE } from "./lib/profile";
import { MARKETPLACE_POLL_MS } from "./lib/marketplace";
import { useProductTour } from "./lib/tour/useProductTour";
import { TourContext } from "./lib/tour/tourContext";
import { navAnchor, TOUR_ANCHOR } from "./lib/tour/tourAnchors";
import { pickerJourneys } from "./lib/tour/picker";
import StartupJourneyPanel from "./components/StartupJourneyPanel";
import ChangelogPanel from "./components/ChangelogPanel";
import type { ChangelogDoc } from "./lib/changelog";
import { hasUnseenSince, gatewayLabel } from "./lib/changelog";
import { RouteErrorBoundary } from "./components/RouteErrorBoundary";
import { lazyImport } from "./lib/lazyWithReload";
import { PersistentAppViews } from "./views/PersistentAppViews";

// Route views are code-split so heavy editors (bpmn-js modeler + properties
// panel, monaco) stay out of the initial bundle and load on navigation.
//
// The studio-only views are additionally guarded by the compile-time
// `__STUDIO__` literal (ADR 0034): in an `observe` build it folds to `false`, so
// esbuild drops these `import()` anchors during transform and the IDE chunks
// (Monaco's ts.worker/typescript, the bpmn/dmn/form modeler bundle) — and the
// orphan `?worker` bundles Vite's worker plugin would otherwise emit — are never
// produced. The anchors must guard on the raw `__STUDIO__` define, not the
// imported `IS_STUDIO` const: an imported binding only tree-shakes after
// transform, too late to stop the worker emit (see profile.ts).
const Projects = __STUDIO__
  ? lazyImport(() => import("./views/Projects"))
  : null;
const ProjectWorkspace = __STUDIO__
  ? lazyImport(() => import("./views/ProjectWorkspace"))
  : null;
const Extensions = __STUDIO__
  ? lazyImport(() => import("./views/Extensions"))
  : null;
// AppView is anchored in ./views/PersistentAppViews (the keep-alive host renders
// it outside <Routes>) so this import stays tree-shakeable with the profile.
// Operator surface — always present in both profiles.
const Workers = lazyImport(() => import("./views/Workers"));
const Metrics = lazyImport(() => import("./views/Metrics"));
const Traces = lazyImport(() => import("./views/Traces"));
const Explorer = lazyImport(() => import("./views/Explorer"));
const DefinitionPreview = lazyImport(() => import("./views/DefinitionPreview"));

/// The `/explorer` route serves two views off one path: the normal process
/// instance explorer, and — with `?preview=1` — a read-only preview of a
/// not-yet-deployed BPMN definition (a staged delivery-graph proposal's DI).
/// Branch on the query param here so the heavy instance-list hooks in Explorer
/// never run in preview mode.
function ExplorerRoute() {
  const [params] = useSearchParams();
  // Match the bridge contract exactly (`?preview=1`): checking only presence
  // would let an unrelated `preview=0` (or any value) force preview mode.
  return params.get("preview") === "1" ? <DefinitionPreview /> : <Explorer />;
}
const Config = lazyImport(() => import("./views/Config"));
const Credits = lazyImport(() => import("./views/Credits"));

// 24×24 stroke icons, drawn to lucide-style metrics so the rail reads as one
// family.
function Icon({ d, children }: { d?: string; children?: ReactNode }) {
  return (
    <svg
      className="h-4 w-4 shrink-0"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {d ? <path d={d} /> : children}
    </svg>
  );
}

const icons = {
  projects: (
    <Icon d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" />
  ),
  extensions: (
    <Icon d="M14 4a2 2 0 1 1 4 0v2h2a2 2 0 0 1 2 2v3h-2a2.5 2.5 0 0 0 0 5h2v3a2 2 0 0 1-2 2h-3v-2a2.5 2.5 0 0 0-5 0v2H9a2 2 0 0 1-2-2v-3H5a2.5 2.5 0 0 1 0-5h2V8a2 2 0 0 1 2-2h5z" />
  ),
  topology: (
    <Icon>
      <circle cx="12" cy="5" r="2.5" />
      <circle cx="5" cy="19" r="2.5" />
      <circle cx="19" cy="19" r="2.5" />
      <path d="M12 7.5 6.3 17M12 7.5l5.7 9.5M7.5 19h9" />
    </Icon>
  ),
  metrics: <Icon d="M3 21h18M7 16v-5M12 16V8M17 16v-8" />,
  explorer: (
    <Icon>
      <circle cx="11" cy="11" r="7" />
      <path d="m21 21-4.3-4.3" />
    </Icon>
  ),
  traces: <Icon d="M22 12h-4l-3 8L9 4l-3 8H2" />,
  workers: (
    <Icon>
      <rect x="4" y="4" width="16" height="16" rx="2" />
      <rect x="9" y="9" width="6" height="6" />
      <path d="M9 1v3M15 1v3M9 20v3M15 20v3M1 9h3M1 15h3M20 9h3M20 15h3" />
    </Icon>
  ),
  docs: (
    <Icon>
      <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
      <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z" />
    </Icon>
  ),
  whitepaper: (
    <Icon>
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <path d="M14 2v6h6M16 13H8M16 17H8M10 9H8" />
    </Icon>
  ),
  credits: (
    <Icon>
      <rect x="2" y="4" width="20" height="16" rx="2" />
      <path d="M7 4v16M17 4v16M2 8h5M2 12h5M2 16h5M17 8h5M17 12h5M17 16h5" />
    </Icon>
  ),
  feedback: (
    <Icon>
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </Icon>
  ),
  config: (
    <Icon>
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </Icon>
  ),
  sun: (
    <Icon>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </Icon>
  ),
  moon: <Icon d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" />,
  system: (
    <Icon>
      <rect x="2" y="3" width="20" height="14" rx="2" />
      <path d="M8 21h8M12 17v4" />
    </Icon>
  ),
  // Chevrons-left: points left to "collapse"; rotated 180° to point right for
  // "expand" when the rail is already collapsed.
  collapse: <Icon d="M11 17l-5-5 5-5M18 17l-5-5 5-5" />,
  // Hamburger — opens the mobile menu bottom-sheet (the lower rail off-canvas).
  menu: <Icon d="M4 6h16M4 12h16M4 18h16" />,
  // Default glyph for a supervised running app that declares no icon (issue
  // #638): an app window. The rail keys identity on the project name, so this
  // fallback is shared by every un-iconed app.
  appDefault: (
    <Icon>
      <rect x="3" y="4" width="18" height="16" rx="2" />
      <path d="M3 9h18M7 6.5h.01M10 6.5h.01" />
    </Icon>
  ),
} as const;

// A running app's left-rail `icon` hint is either a *bundled glyph name*
// (resolved against the console's icon set) or a *project asset path* the app
// ships itself. Classification helpers (`isAssetIcon`, `isSvgIcon`) live in
// `./lib/appRailIcon` so they can be unit-tested without a DOM.

// Resolve a running app's left-rail glyph from its manifest `icon` hint,
// falling back to the default app glyph when the hint is absent or names an
// icon the console doesn't bundle (per the AppUi.icon contract). Keys like
// theme/nav glyphs are all fair game as bundled names.
function appRailIcon(icon: string | null | undefined): ReactNode {
  if (icon && Object.prototype.hasOwnProperty.call(icons, icon)) {
    return icons[icon as keyof typeof icons];
  }
  return icons.appDefault;
}

// The rail glyph for one running app: the shared `AppIcon` renderer (which
// themes SVG icons via a CSS mask so they don't vanish on the dark rail) for an
// app-shipped asset, else the resolved bundled glyph. A failed image load
// (missing/oversized/wrong type ⇒ the server 404s) falls back to the default
// glyph.
function AppRailGlyph({
  name,
  icon,
}: {
  name: string;
  icon: string | null | undefined;
}) {
  const [failed, setFailed] = useState(false);
  // Reset the fallback when the icon hint changes, so fixing a broken/renamed
  // icon recovers without a full remount.
  useEffect(() => setFailed(false), [icon]);
  if (isAssetIcon(icon) && !failed) {
    return (
      <AppIcon
        name={name}
        icon={icon}
        sizeClass="h-4 w-4 rounded-sm"
        onError={() => setFailed(true)}
      />
    );
  }
  return appRailIcon(icon);
}

const navItems: {
  to: string;
  label: string;
  icon: ReactNode;
  studio?: boolean;
}[] = [
  { to: "/projects", label: "Studio", icon: icons.projects, studio: true },
  {
    to: "/extensions",
    label: "Extensions",
    icon: icons.extensions,
    studio: true,
  },
  { to: "/topology", label: "Topology", icon: icons.topology },
  { to: "/metrics", label: "Metrics", icon: icons.metrics },
  { to: "/explorer", label: "Explorer", icon: icons.explorer },
  { to: "/traces", label: "Traces", icon: icons.traces },
  { to: "/workers", label: "Workers", icon: icons.workers },
].filter((i) => IS_STUDIO || !i.studio);

// Where "home" lands: the maker starts in Studio; the operator ("observe"
// build, no Studio route) starts on Topology.
const HOME_ROUTE = IS_STUDIO ? "/projects" : "/topology";

// Cheap structural equality on the running-apps set so the 5s poll only
// re-renders the rail when the set actually changes (name + display fields).
function sameApps(a: ProjectSummary[], b: ProjectSummary[]): boolean {
  if (a.length !== b.length) return false;
  return a.every((p, i) => {
    const q = b[i];
    return (
      p.name === q.name &&
      p.displayName === q.displayName &&
      p.appUi?.label === q.appUi?.label &&
      p.appUi?.icon === q.appUi?.icon &&
      p.appUi?.enabled === q.appUi?.enabled &&
      p.appUi?.port === q.appUi?.port
    );
  });
}

/** A running app's presence in the navigation surface, normalised once so every
 * consumer renders the same identity/label/target. The desktop rail, the mobile
 * card home and the hamburger sheet all bind to these — and unit A5 (embedded
 * app cards) feeds off the same shape rather than re-deriving the rail's
 * disambiguation. Identity is the stable project `name`; `label`/`icon` are
 * display hints only (same-template apps share a manifest), so a label
 * collision is disambiguated with the unique name. */
export type RunningAppRailEntry = {
  /** Stable project name — the identity the route and React key are built on. */
  name: string;
  /** Display label; suffixed with the project name when two apps collide. */
  label: string;
  /** Title/aria hover text always exposing the unique project name. */
  hover: string;
  /** SPA route for the app's UI/Logs view (`/apps/<encoded name>`). */
  href: string;
  /** Manifest icon hint (bundled glyph name or app-shipped asset path). */
  icon: string | null | undefined;
};

/** Build the running-app rail entries from the raw running-app set: resolves the
 * display label, disambiguates collisions against the unique project name, and
 * computes the SPA target. The one place the rail's identity rules live, shared
 * by the desktop rail, the mobile presentation and unit A5. */
export function buildRunningAppRailEntries(
  apps: ProjectSummary[],
): RunningAppRailEntry[] {
  const labelCounts = new Map<string, number>();
  for (const app of apps) {
    const base = app.appUi?.label || app.displayName || app.name;
    labelCounts.set(base, (labelCounts.get(base) ?? 0) + 1);
  }
  return apps.map((app) => {
    const base = app.appUi?.label || app.displayName || app.name;
    const collides = (labelCounts.get(base) ?? 0) > 1;
    return {
      name: app.name,
      label: collides ? `${base} · ${app.name}` : base,
      hover: base === app.name ? app.name : `${base} (${app.name})`,
      href: `/apps/${encodeURIComponent(app.name)}`,
      icon: app.appUi?.icon,
    };
  });
}

function railItemClass(active: boolean, collapsed = false): string {
  return `relative flex items-center ${
    collapsed ? "justify-center px-2" : "gap-2.5 px-3"
  } rounded-md py-2 text-sm no-underline transition-colors ${
    active
      ? "bg-accent/10 font-medium text-accent-strong"
      : "text-fg-muted hover:bg-hover hover:text-fg"
  }`;
}

/** Accent bar marking the active rail item. */
function ActiveBar({ show }: { show: boolean }) {
  if (!show) return null;
  return (
    <span className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-gradient-to-b from-accent to-accent-2" />
  );
}

/** Sidebar segmented control cycling the appearance: light / dark / system.
 * Theme packs and imports are picked in Config → Appearance. */
function ThemeToggle({ collapsed = false }: { collapsed?: boolean }) {
  const { selection, select } = useTheme();
  const modes = [
    { mode: "light", icon: icons.sun, title: "Light" },
    { mode: "dark", icon: icons.moon, title: "Dark" },
    { mode: "system", icon: icons.system, title: "Follow system" },
  ] as const;
  return (
    <div
      className={`mb-3 flex rounded-lg border border-edge bg-inset p-0.5 ${
        collapsed ? "mx-2 flex-col gap-0.5" : "mx-3"
      }`}
    >
      {modes.map((m) => {
        const active = selection.mode === m.mode;
        return (
          <button
            key={m.mode}
            title={
              selection.mode === "theme"
                ? `${m.title} (a theme pack is active — this switches back)`
                : m.title
            }
            onClick={() => select({ mode: m.mode })}
            className={`flex flex-1 items-center justify-center rounded-md py-1.5 transition-colors ${
              active
                ? "bg-raised text-accent-strong shadow-sm"
                : "text-fg-faint hover:text-fg"
            }`}
          >
            {m.icon}
          </button>
        );
      })}
    </div>
  );
}

/**
 * Delay before the startup persona panel (#464) opens, letting the initial
 * route render and the journey list settle before the modal appears.
 */
const STARTUP_PANEL_DELAY_MS = 500;

export default function App() {
  const location = useLocation();
  const navigate = useNavigate();
  // Below `MOBILE_MAX_WIDTH` the persistent left rail can't fit, so the SAME
  // routes are presented mobile-first: the primary nav becomes a card home and
  // the lower rail collapses into a hamburger bottom-sheet. This is a
  // presentation switch only — the route tree and every `/console/...` deep
  // link are unchanged (single targets), so a wide/narrow toggle never changes
  // where a link lands.
  const isNarrow = useIsNarrow();
  const [menuOpen, setMenuOpen] = useState(false);
  // The card home renders on the profile's landing route (`/projects` in studio,
  // `/topology` in observe) — never a hardcoded `studio` assumption.
  const isHome = location.pathname === HOME_ROUTE;
  // Any client navigation dismisses the menu sheet so it never lingers over the
  // destination.
  useEffect(() => setMenuOpen(false), [location.pathname]);
  // The one product-tour instance for the whole app. Published via TourContext
  // so the rail button here AND the empty-state journey pickers (#411) drive the
  // same runner and journey state.
  //
  // `autoStart` is now false: ADR 0049 replaces first-run auto-start with the
  // journey picker on the Projects/Topology empty state (#411, landed), so a
  // first-timer chooses one of the real outcome-shaped journeys instead of being
  // dropped into the demoted overview. The overview stays reachable from each
  // picker's "just show me around" link and the rail's "Take a tour".
  const tour = useProductTour({ autoStart: false });
  const { startTour, resumeJourney, activeJourney, isRunning } = tour;
  // Offer "Resume" only when there is an unfinished journey that is not already
  // on screen — otherwise the label would invite the user to resume the tour
  // they are looking at.
  const canResume = !!activeJourney && !isRunning;

  // The startup persona panel (#464): the front door that replaces the CLI's
  // `?tour=` link-spray. Personas are the offerable, outcome-shaped journeys
  // (everything but the zero-commitment overview), derived from the registry.
  const personaJourneys = pickerJourneys(
    tour.availableJourneys,
    CONSOLE_PROFILE,
  );
  const [startupOpen, setStartupOpen] = useState(false);
  // One-shot decision, deferred a beat so the initial route renders first and
  // the journey list settles against real context. Never interrupts a journey
  // already running or resumable (e.g. a `?tour=` deep link that still fires).
  const startupDecided = useRef(false);
  const personaJourneysRef = useRef(personaJourneys);
  personaJourneysRef.current = personaJourneys;
  useEffect(() => {
    if (startupDecided.current) return;
    if (!tour.showStartupPanel) {
      startupDecided.current = true;
      return;
    }
    const id = window.setTimeout(() => {
      startupDecided.current = true;
      if (isRunning || activeJourney) return;
      if (personaJourneysRef.current.length === 0) return;
      setStartupOpen(true);
    }, STARTUP_PANEL_DELAY_MS);
    return () => window.clearTimeout(id);
  }, [tour.showStartupPanel, isRunning, activeJourney]);
  // Remember the last place the user was within the Studio section (the
  // project list or a specific workspace) so the rail's "Studio" item returns
  // them there after a detour through Metrics/Traces/etc. — instead of always
  // dropping back at the root list.
  const projectsRoute = useRef(
    localStorage.getItem("nano.projectsRoute") || "/projects",
  );
  useEffect(() => {
    if (location.pathname.startsWith("/projects")) {
      projectsRoute.current = location.pathname;
      localStorage.setItem("nano.projectsRoute", location.pathname);
    }
  }, [location.pathname]);

  // Collapsible left rail (#511). Persist the collapsed/expanded choice in
  // localStorage — same convention as `nano.projectsRoute` above — so it
  // survives reloads.
  const [railCollapsed, setRailCollapsed] = useState<boolean>(
    () => localStorage.getItem("nano.railCollapsed") === "1",
  );
  const toggleRail = useCallback(() => {
    setRailCollapsed((prev) => {
      const next = !prev;
      localStorage.setItem("nano.railCollapsed", next ? "1" : "0");
      return next;
    });
  }, []);

  // The running gateway's version, shown in the sidebar chrome so it's visible
  // on every page — useful when bouncing between dev builds and staged releases
  // to confirm which binary is actually serving the console. Reads
  // `/console/api/topology`'s `gateway_version` (the same field surfaced on the
  // Topology page); silently absent if the probe fails.
  const [serverVersion, setServerVersion] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    getTopology({ throwOnError: true })
      .then(({ data }) => {
        if (!cancelled) setServerVersion(data.gateway_version);
      })
      .catch(() => {
        /* leave hidden — sidebar is not the place to surface a probe error */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // "What's new" changelog. The document is a static asset generated at build
  // time from the git history (console/scripts/build-changelog.mjs) and served
  // at `${BASE_URL}changelog.json`; we fetch it once and keep an unobtrusive
  // "new" dot on the version chrome until the user opens the panel. The last
  // acknowledged version is persisted so the dot only reappears after a genuine
  // upgrade. Offline-soft: a missing/failed asset simply hides the affordance's
  // badge and shows a graceful message if the panel is opened.
  const [changelog, setChangelog] = useState<ChangelogDoc | null>(null);
  const [changelogError, setChangelogError] = useState(false);
  const [changelogOpen, setChangelogOpen] = useState(false);
  const [lastSeenChangelog, setLastSeenChangelog] = useState<string | null>(
    () => localStorage.getItem("nano.changelog.lastSeen"),
  );
  useEffect(() => {
    let cancelled = false;
    fetch(`${import.meta.env.BASE_URL}changelog.json`, {
      headers: { accept: "application/json" },
    })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("not ok"))))
      .then((doc: ChangelogDoc) => {
        if (!cancelled) setChangelog(doc);
      })
      .catch(() => {
        if (!cancelled) setChangelogError(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const changelogHasUnseen = hasUnseenSince(changelog, lastSeenChangelog);
  const openChangelog = () => setChangelogOpen(true);
  const closeChangelog = () => {
    setChangelogOpen(false);
    // Closing acknowledges the newest version, clearing the dot. Persist on
    // close (not open) so an open that happened before changelog.json finished
    // loading still records the release once its data is present at close time,
    // and update state so the dot clears without a reload.
    const newest = changelog?.versions[0]?.version;
    if (newest) {
      localStorage.setItem("nano.changelog.lastSeen", newest);
      setLastSeenChangelog(newest);
    }
  };

  // Marketplace update poll so the Extensions rail item can wear a badge with
  // the current available-updates count on every page — the user doesn't have
  // to open Extensions to notice a freshly-published fix. Skipped while the tab
  // is hidden (background tabs shouldn't hammer npm). Polls on a several-minute
  // cadence (not 30 s) and never starts a poll while the previous one is still
  // running: the server-side marketplace() shells out to npm per installed pack,
  // and a 30 s cadence across several tabs swap-thrashed a small host (#1330).
  const [updateCount, setUpdateCount] = useState(0);
  useEffect(() => {
    // Studio-only: the operator ("observe") build has no Extensions view, so
    // there's no badge to feed and no reason to poll npm.
    if (!IS_STUDIO) return;
    let cancelled = false;
    let inFlight = false;
    const poll = () => {
      if (typeof document !== "undefined" && document.hidden) return;
      // Don't stack a new poll on top of an unfinished one — under memory
      // pressure a poll can outlast the interval, and overlapping fan-outs are
      // exactly the feedback loop that collapsed merlin (#1330).
      if (inFlight) return;
      inFlight = true;
      getMarketplace({ throwOnError: true })
        .then(({ data }) => {
          if (cancelled) return;
          setUpdateCount(data.entries.filter((e) => e.updateAvailable).length);
        })
        .catch(() => {
          /* offline or npm missing — leave the badge as-is */
        })
        .finally(() => {
          inFlight = false;
        });
    };
    poll();
    const id = window.setInterval(poll, MARKETPLACE_POLL_MS);
    const onVis = () => {
      if (!document.hidden) poll();
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      cancelled = true;
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, []);

  // Running supervised apps contributed to the left rail (issue #638). The rail
  // doubles as the running-apps control surface: every app the supervisor is
  // running gets an entry (UI or headless), keyed on the project name — manifest
  // icon/label are display hints only, since same-template apps share a manifest.
  const [runningApps, setRunningApps] = useState<ProjectSummary[]>([]);
  useEffect(() => {
    if (!IS_STUDIO) return;
    let cancelled = false;
    const poll = () => {
      if (typeof document !== "undefined" && document.hidden) return;
      listProjects({ throwOnError: true })
        .then(({ data }) => {
          if (cancelled) return;
          // Sort by the stable `name` key so a reshuffle of the server's
          // `updatedMs`-then-name ordering doesn't churn the rail when the
          // running-app set and its display fields are unchanged. Storing the
          // sorted list also keeps the rendered rail order stable.
          const running = data.projects
            .filter((p) => p.running)
            .sort((a, b) => a.name.localeCompare(b.name));
          setRunningApps((prev) => (sameApps(prev, running) ? prev : running));
        })
        .catch(() => {
          /* offline or supervisor unavailable — keep the last known set */
        });
    };
    poll();
    const id = window.setInterval(poll, 5_000);
    const onVis = () => {
      if (!document.hidden) poll();
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      cancelled = true;
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, []);

  // Running apps normalised to their shared rail entries (label disambiguation +
  // SPA target). One source of truth for the desktop rail, the mobile card home
  // / hamburger sheet, and unit A5's embedded app cards.
  const runningAppEntries = useMemo(
    () => buildRunningAppRailEntries(runningApps),
    [runningApps],
  );
  // ext→language map at boot, and again whenever the user navigates — so a
  // pack installed via the Extensions view during this session takes effect
  // as soon as they open a project, without a hard reload. Extensions.tsx
  // also calls the same helper right after install/remove, which is the
  // fast path; this navigation-triggered refetch is the safety net for any
  // other codepath that might mutate the extension set.
  useEffect(() => {
    // The ext→language + IntelliSense maps only matter to the Monaco editors,
    // which the operator ("observe") build doesn't ship.
    if (!IS_STUDIO) return;
    getExtensions({ throwOnError: true })
      .then(({ data }) => {
        registerFileTypesFromOverview(data);
        setIntellisenseFromOverview(data);
      })
      .catch(() => {
        /* ignore — the static fallback table still covers the common cases */
      });
  }, [location.pathname]);

  // Primary navigation resolved to card models — the mobile card home and the
  // hamburger sheet both render these (the rail turned into a card grid). Mirrors
  // the desktop rail: Studio returns to the last-visited project route, and the
  // Extensions card carries the update-count badge.
  const primaryNavCards = navItems.map((item) => {
    const isProjects = item.to === "/projects";
    const to = isProjects ? projectsRoute.current : item.to;
    const active = isProjects
      ? location.pathname.startsWith("/projects")
      : location.pathname === item.to;
    const badge =
      item.to === "/extensions" && updateCount > 0 ? updateCount : 0;
    return {
      key: item.to,
      to,
      label: item.label,
      icon: item.icon,
      active,
      badge,
    };
  });

  // A card tap navigates within the SPA and dismisses the menu sheet.
  const goMobile = (to: string) => {
    setMenuOpen(false);
    navigate(to);
  };

  const renderNavCard = (m: (typeof primaryNavCards)[number]) => (
    <NavCard
      key={m.key}
      active={m.active}
      icon={m.icon}
      onClick={() => goMobile(m.to)}
      label={
        m.badge > 0 ? (
          <span className="flex items-center gap-2">
            {m.label}
            <span
              className="inline-flex min-w-[18px] items-center justify-center rounded-full bg-danger px-1.5 text-[10px] font-bold leading-none text-white"
              style={{ height: "18px" }}
              aria-label={`${m.badge} extension update${m.badge === 1 ? "" : "s"} available`}
            >
              {m.badge > 99 ? "99+" : m.badge}
            </span>
          </span>
        ) : (
          m.label
        )
      }
    />
  );

  const renderAppCard = (entry: RunningAppRailEntry) => (
    <NavCard
      key={entry.name}
      active={location.pathname === entry.href}
      icon={<AppRailGlyph name={entry.name} icon={entry.icon} />}
      label={entry.label}
      title={entry.hover}
      onClick={() => goMobile(entry.href)}
    />
  );

  // The mobile card home: the primary rail as a card grid plus the running-app
  // cards, shown on the profile's landing route. The routed home view renders
  // below it — this is navigation chrome, not a route of its own.
  const cardHome = (
    <section className="nano-safe-x border-b border-edge p-4">
      <SectionLabel>Navigate</SectionLabel>
      <div className="mt-2">
        <CardGrid>{primaryNavCards.map(renderNavCard)}</CardGrid>
      </div>
      {IS_STUDIO && runningAppEntries.length > 0 && (
        <div className="mt-4">
          <SectionLabel>Running apps</SectionLabel>
          <div className="mt-2">
            <CardGrid>{runningAppEntries.map(renderAppCard)}</CardGrid>
          </div>
        </div>
      )}
    </section>
  );

  // The version chrome text mirrors the desktop sidebar's "What's new" line.
  const versionLabel = serverVersion
    ? `gateway ${gatewayLabel(serverVersion) ?? serverVersion}`
    : undefined;

  // The lower rail (theme, tour, changelog, version, Config, Credits, Feedback,
  // Documentation, Whitepaper) collapses into this hamburger bottom-sheet.
  const menuSheet = (
    <BottomSheet
      open={menuOpen}
      onClose={() => setMenuOpen(false)}
      title="Menu"
    >
      <div className="flex flex-col gap-5">
        <section>
          <SectionLabel>Navigate</SectionLabel>
          <div className="mt-2">
            <CardGrid>{primaryNavCards.map(renderNavCard)}</CardGrid>
          </div>
        </section>

        {IS_STUDIO && runningAppEntries.length > 0 && (
          <section>
            <SectionLabel>Running apps</SectionLabel>
            <div className="mt-2">
              <CardGrid>{runningAppEntries.map(renderAppCard)}</CardGrid>
            </div>
          </section>
        )}

        <section>
          <SectionLabel>Appearance</SectionLabel>
          <div className="mt-2">
            <ThemeToggle />
          </div>
        </section>

        <section>
          <SectionLabel>More</SectionLabel>
          <div className="mt-2 flex flex-col gap-2">
            <NavCard
              icon={
                <Icon>
                  <circle cx="12" cy="12" r="9" />
                  <path d="M9.1 9a3 3 0 1 1 4.3 3.2c-.8.5-1.4 1-1.4 1.9" />
                  <path d="M12 17h.01" />
                </Icon>
              }
              label={canResume ? "Resume tour" : "Take a tour"}
              onClick={() => {
                setMenuOpen(false);
                if (canResume) resumeJourney();
                else startTour();
              }}
            />

            {(serverVersion || changelog || changelogError) && (
              <NavCard
                icon={icons.whitepaper}
                label={
                  <span className="flex items-center gap-2">
                    What's new
                    {changelogHasUnseen && (
                      <>
                        <span className="sr-only">New changes available</span>
                        <span
                          className="inline-block h-1.5 w-1.5 rounded-full bg-accent"
                          aria-hidden="true"
                        />
                      </>
                    )}
                  </span>
                }
                description={versionLabel}
                onClick={() => {
                  setMenuOpen(false);
                  openChangelog();
                }}
              />
            )}

            <NavCard
              icon={icons.config}
              label="Config"
              active={location.pathname.startsWith("/config")}
              onClick={() => goMobile("/config")}
            />
            <NavCard
              icon={icons.credits}
              label="Credits"
              active={location.pathname.startsWith("/credits")}
              onClick={() => goMobile("/credits")}
            />
            <NavCard
              icon={icons.feedback}
              label="Feedback"
              href="https://github.com/nanobpm/nano-ide/issues/new/choose"
              target="_blank"
              rel="noopener noreferrer"
              onClick={() => setMenuOpen(false)}
            />
            <NavCard icon={icons.docs} label="Documentation" href="/docs" />
            <NavCard
              icon={icons.whitepaper}
              label="Whitepaper"
              href="/whitepaper"
            />
          </div>
        </section>
      </div>
    </BottomSheet>
  );

  return (
    <TourContext.Provider value={tour}>
      <div
        className={`flex h-full bg-app text-fg ${isNarrow ? "flex-col" : ""}`}
      >
        {isNarrow ? (
          <header className="nano-safe-top nano-safe-x flex shrink-0 items-center gap-2 border-b border-edge bg-panel px-3 py-2">
            <button
              type="button"
              onClick={() => setMenuOpen(true)}
              className="nano-touch relative flex items-center justify-center rounded-md px-2 text-fg-muted outline-none transition-colors hover:bg-hover hover:text-fg focus-visible:ring-2 focus-visible:ring-accent"
              aria-label="Open menu"
              aria-haspopup="dialog"
              aria-expanded={menuOpen}
            >
              {icons.menu}
              {changelogHasUnseen && (
                <>
                  <span className="sr-only">New changes available</span>
                  <span
                    className="absolute right-1 top-1 inline-block h-1.5 w-1.5 rounded-full bg-accent"
                    aria-hidden="true"
                  />
                </>
              )}
            </button>
            <a
              href="/"
              className="min-w-0 flex-1 truncate bg-gradient-to-r from-accent to-accent-2 bg-clip-text text-base font-bold tracking-tight text-transparent no-underline"
              title="nano BPM — single-node console"
            >
              nano BPM
            </a>
          </header>
        ) : (
          <aside
            className={`flex shrink-0 flex-col border-r border-edge bg-panel transition-[width] duration-150 ${
              railCollapsed ? "w-14" : "w-56"
            }`}
          >
            <div className={railCollapsed ? "px-2 py-4" : "px-5 py-4"}>
              <a
                href="/"
                className="block no-underline"
                title="nano BPM — single-node console"
              >
                {railCollapsed ? (
                  <div className="bg-gradient-to-r from-accent to-accent-2 bg-clip-text text-center text-xl font-bold tracking-tight text-transparent">
                    n
                  </div>
                ) : (
                  <>
                    <div className="bg-gradient-to-r from-accent to-accent-2 bg-clip-text text-lg font-bold tracking-tight text-transparent">
                      nano BPM
                    </div>
                    <div className="text-xs text-fg-faint">
                      single-node console
                    </div>
                    <div className="mt-2 inline-block rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent-strong">
                      Advanced Research Prototype
                    </div>
                    <div className="mt-1.5 text-[10px] text-fg-faint">
                      Free for personal or evaluation use
                    </div>
                  </>
                )}
              </a>
              {/* Version chrome doubles as the "What's new" entry point. It sits
                outside the home <a> (a button can't nest in an anchor) and wears
                a dot until the newest release has been opened. Hidden while the
                rail is collapsed (no room in the narrow rail). */}
              {!railCollapsed &&
                (serverVersion || changelog || changelogError) && (
                  <button
                    type="button"
                    onClick={openChangelog}
                    title="See what's new in Nano"
                    className="mt-1 flex items-center gap-1.5 rounded font-mono text-[10px] text-fg-faint outline-none transition-colors hover:text-fg focus-visible:ring-2 focus-visible:ring-accent"
                  >
                    <span title={serverVersion ?? undefined}>
                      {serverVersion
                        ? `gateway ${gatewayLabel(serverVersion) ?? serverVersion}`
                        : "What's new"}
                    </span>
                    {serverVersion && (
                      <span className="underline decoration-dotted underline-offset-2">
                        What's new
                      </span>
                    )}
                    {changelogHasUnseen && (
                      <>
                        <span className="sr-only">New changes available</span>
                        <span
                          className="inline-block h-1.5 w-1.5 rounded-full bg-accent"
                          aria-hidden="true"
                        />
                      </>
                    )}
                  </button>
                )}
            </div>
            <nav className="flex flex-col gap-1 px-3">
              {navItems.map((item) => {
                // The Studio item is special: it links back to wherever the user
                // last was in that section and stays highlighted across all
                // /projects/* routes.
                const isProjects = item.to === "/projects";
                const to = isProjects ? projectsRoute.current : item.to;
                const active = isProjects
                  ? location.pathname.startsWith("/projects")
                  : location.pathname === item.to;
                return (
                  <NavLink
                    key={item.to}
                    to={to}
                    data-tour={navAnchor(item.to)}
                    className={railItemClass(active, railCollapsed)}
                    title={railCollapsed ? item.label : undefined}
                    aria-label={railCollapsed ? item.label : undefined}
                  >
                    <ActiveBar show={active} />
                    {item.icon}
                    {!railCollapsed && item.label}
                    {item.to === "/extensions" && updateCount > 0 && (
                      <span
                        className={`inline-flex min-w-[18px] items-center justify-center rounded-full bg-danger px-1.5 text-[10px] font-bold leading-none text-white ${
                          railCollapsed
                            ? "absolute -right-0.5 -top-0.5"
                            : "ml-auto"
                        }`}
                        style={{ height: "18px" }}
                        title={`${updateCount} extension update${updateCount === 1 ? "" : "s"} available`}
                        aria-label={`${updateCount} extension updates available`}
                      >
                        {updateCount > 99 ? "99+" : updateCount}
                      </span>
                    )}
                  </NavLink>
                );
              })}
            </nav>

            {IS_STUDIO && runningAppEntries.length > 0 && (
              <nav className="mt-2 flex flex-col gap-1 border-t border-edge px-3 pt-2">
                {!railCollapsed && (
                  <div className="px-3 pb-1 text-[10px] font-semibold uppercase tracking-wide text-fg-muted">
                    Running apps
                  </div>
                )}
                {runningAppEntries.map((entry) => {
                  const active = location.pathname === entry.href;
                  return (
                    <NavLink
                      key={entry.name}
                      to={entry.href}
                      className={railItemClass(active, railCollapsed)}
                      title={entry.hover}
                      aria-label={railCollapsed ? entry.hover : undefined}
                    >
                      <ActiveBar show={active} />
                      <AppRailGlyph name={entry.name} icon={entry.icon} />
                      {!railCollapsed && (
                        <span className="truncate">{entry.label}</span>
                      )}
                      <span
                        className={`inline-block h-1.5 w-1.5 shrink-0 rounded-full bg-success ${
                          railCollapsed
                            ? "absolute -right-0.5 -top-0.5"
                            : "ml-auto"
                        }`}
                        title="Running"
                        aria-hidden="true"
                      />
                    </NavLink>
                  );
                })}
              </nav>
            )}

            <button
              type="button"
              onClick={canResume ? resumeJourney : startTour}
              data-tour={TOUR_ANCHOR.takeATour}
              className={`mt-auto mx-3 ${railItemClass(false, railCollapsed)}`}
              title={
                canResume
                  ? `Pick up “${activeJourney.title}” where you left off`
                  : "Replay the product tour"
              }
              aria-label={
                railCollapsed
                  ? canResume
                    ? "Resume tour"
                    : "Take a tour"
                  : undefined
              }
            >
              <Icon>
                <circle cx="12" cy="12" r="9" />
                <path d="M9.1 9a3 3 0 1 1 4.3 3.2c-.8.5-1.4 1-1.4 1.9" />
                <path d="M12 17h.01" />
              </Icon>
              {!railCollapsed && (canResume ? "Resume tour" : "Take a tour")}
            </button>

            <a
              href="https://github.com/nanobpm/nano-ide/issues/new/choose"
              target="_blank"
              rel="noopener noreferrer"
              className={`mx-3 ${railItemClass(false, railCollapsed)}`}
              title="Send feedback or report an issue"
              aria-label={railCollapsed ? "Feedback" : undefined}
            >
              {icons.feedback}
              {!railCollapsed && "Feedback"}
            </a>

            <a
              href="/docs"
              className={`mx-3 ${railItemClass(false, railCollapsed)}`}
              title="Documentation"
              aria-label={railCollapsed ? "Documentation" : undefined}
            >
              {icons.docs}
              {!railCollapsed && "Documentation"}
            </a>

            <a
              href="/whitepaper"
              className={`mx-3 ${railItemClass(false, railCollapsed)}`}
              title="Whitepaper"
              aria-label={railCollapsed ? "Whitepaper" : undefined}
            >
              {icons.whitepaper}
              {!railCollapsed && "Whitepaper"}
            </a>

            <NavLink
              to="/credits"
              className={`mx-3 ${railItemClass(location.pathname.startsWith("/credits"), railCollapsed)}`}
              title="Credits"
              aria-label={railCollapsed ? "Credits" : undefined}
            >
              <ActiveBar show={location.pathname.startsWith("/credits")} />
              {icons.credits}
              {!railCollapsed && "Credits"}
            </NavLink>

            <NavLink
              to="/config"
              className={`mx-3 mb-3 ${railItemClass(location.pathname.startsWith("/config"), railCollapsed)}`}
              title="Configuration"
              aria-label={railCollapsed ? "Config" : undefined}
            >
              <ActiveBar show={location.pathname.startsWith("/config")} />
              {icons.config}
              {!railCollapsed && "Config"}
            </NavLink>

            <button
              type="button"
              onClick={toggleRail}
              className={`mx-3 mb-1 ${railItemClass(false, railCollapsed)}`}
              title={railCollapsed ? "Expand sidebar" : "Collapse sidebar"}
              aria-label={railCollapsed ? "Expand sidebar" : "Collapse sidebar"}
              aria-expanded={!railCollapsed}
            >
              <span
                className={`inline-flex ${railCollapsed ? "rotate-180" : ""}`}
              >
                {icons.collapse}
              </span>
              {!railCollapsed && "Collapse"}
            </button>

            <ThemeToggle collapsed={railCollapsed} />
          </aside>
        )}

        <main className="min-w-0 flex-1 overflow-auto">
          {isNarrow && isHome && cardHome}
          <Suspense
            fallback={
              <div className="flex h-full items-center justify-center text-sm text-fg-faint">
                Loading…
              </div>
            }
          >
            <RouteErrorBoundary resetKey={location.pathname}>
              <Routes>
                <Route
                  path="/"
                  element={<Navigate to={HOME_ROUTE} replace />}
                />
                {/* Studio-only routes — absent (and tree-shaken) in observe builds.
                  RR6 ignores falsy children, so a null component drops the route. */}
                {Projects && <Route path="/projects" element={<Projects />} />}
                {ProjectWorkspace && (
                  <Route
                    path="/projects/:name"
                    element={<ProjectWorkspace />}
                  />
                )}
                {Extensions && (
                  <Route path="/extensions" element={<Extensions />} />
                )}
                {/* Stub: keeps `/apps/:name` matched so the catch-all redirect
                    below doesn't fire. The kept-alive view itself is rendered by
                    PersistentAppViews OUTSIDE <Routes> (issue #1040) — mounting
                    it here would unmount the embedded app's iframe on every
                    navigation away. */}
                {IS_STUDIO && <Route path="/apps/:name" element={null} />}
                <Route path="/config" element={<Config />} />
                <Route path="/credits" element={<Credits />} />
                <Route path="/topology" element={<Topology />} />
                <Route path="/metrics" element={<Metrics />} />
                <Route
                  path="/modeler"
                  element={<Navigate to={HOME_ROUTE} replace />}
                />
                <Route path="/explorer" element={<ExplorerRoute />} />
                <Route path="/traces" element={<Traces />} />
                <Route path="/workers" element={<Workers />} />
                <Route
                  path="*"
                  element={<Navigate to={HOME_ROUTE} replace />}
                />
              </Routes>
            </RouteErrorBoundary>
            {/* Keep-alive host for the embedded app views (issue #1040): mounts
                one AppView per visited app for the session and hides the
                inactive ones, so leaving `/apps/:name` no longer destroys the
                app's iframe.

                Own <Suspense> boundary: PersistentAppViews lazily imports the
                AppView chunk, and a *hidden* kept-alive view can suspend in the
                background (e.g. its chunk is still loading when you navigate
                away). Sharing the outer boundary would raise the global
                "Loading…" over whatever route is actually visible (Topology,
                etc.). Isolating it here keeps that background suspend invisible,
                while still showing a fallback when `/apps/:name` is the active
                route (where the AppView *is* the visible content). */}
            <Suspense
              fallback={
                location.pathname.startsWith("/apps/") ? (
                  <div className="flex h-full items-center justify-center text-sm text-fg-faint">
                    Loading…
                  </div>
                ) : null
              }
            >
              <PersistentAppViews />
            </Suspense>
          </Suspense>
        </main>
      </div>
      {isNarrow && menuSheet}
      {startupOpen && (
        <StartupJourneyPanel
          journeys={personaJourneys}
          showAtStartup={tour.showStartupPanel}
          onToggleShowAtStartup={tour.setShowStartupPanel}
          onPick={(journeyId) => {
            setStartupOpen(false);
            tour.startJourney(journeyId);
          }}
          onOverview={() => {
            setStartupOpen(false);
            startTour();
          }}
          onClose={() => setStartupOpen(false)}
        />
      )}
      {changelogOpen && (
        <ChangelogPanel
          doc={changelog}
          loadError={changelogError}
          onClose={closeChangelog}
        />
      )}
    </TourContext.Provider>
  );
}
