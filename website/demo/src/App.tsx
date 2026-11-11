import { useEffect, useMemo, useRef, useState } from "react";
import { BpmnRuntimeView, useBojtos } from "@nanobpm/bojtos-react";
import { ParticleField } from "./ParticleField";
import { Cancelled, SCENARIOS, initialScenario, type LogKind, type Scenario } from "./scenarios";

/** Pace of the animation, ms per token beat. */
const BEAT = 1400;
const RESTART_DELAY = 4200;
/** Activity lines kept (the panel scrolls; this just bounds memory). */
const LOG_LINES = 20;

interface LogEntry {
  id: number;
  kind: LogKind;
  text: string;
}

export function App() {
  const [scenario, setScenario] = useState<Scenario>(() =>
    initialScenario(typeof window === "undefined" ? "" : window.location.search),
  );
  const bojtos = useBojtos({ bpmn: scenario.bpmn });
  const { phase, error, processIds, snapshot, reset } = bojtos;

  const [running, setRunning] = useState(true);
  const [log, setLog] = useState<LogEntry[]>([]);

  // Embed mode (`/demo/?embed=1`): render just the live demo — no topbar, hero
  // or footer — so the landing page can iframe the running demo section alone.
  const embed = useMemo(
    () =>
      typeof window !== "undefined" &&
      new URLSearchParams(window.location.search).has("embed"),
    [],
  );

  // An embed that names its scenario (the home page's per-example tabs) shows
  // that flow alone; the host page does the picking.
  const pinned = useMemo(() => {
    if (typeof window === "undefined") return false;
    const q = new URLSearchParams(window.location.search);
    return q.has("embed") && q.has("scenario");
  }, []);

  const runningRef = useRef(running);
  runningRef.current = running;
  const bojtosRef = useRef(bojtos);
  bojtosRef.current = bojtos;
  const processIdRef = useRef<string>(scenario.id);
  processIdRef.current = processIds[0] ?? scenario.id;

  const logIdRef = useRef(0);
  // Last variables shown; see the render below and DriveCtx.noteVars.
  const lastVarsRef = useRef<Record<string, unknown>>({});
  const logRef = useRef<HTMLUListElement>(null);

  // Keep the newest activity line in view as the (scrollable, fixed-height) log
  // fills, so the closing entry is always visible.
  useEffect(() => {
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [log]);

  const pushLog = (kind: LogKind, text: string) =>
    setLog((prev) => {
      const next = [...prev, { id: logIdRef.current++, kind, text }];
      return next.slice(-LOG_LINES);
    });

  // The auto-driver: replays the selected scenario on the wasm engine, forever.
  // Keyed on the scenario too, so switching tears down the old run (the hook
  // re-deploys the new model on a fresh engine and phase cycles to "ready").
  useEffect(() => {
    if (phase !== "ready") return;
    let cancelled = false;
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    const waitWhilePaused = async () => {
      while (!runningRef.current && !cancelled) await sleep(120);
    };
    const beat = async () => {
      await sleep(BEAT);
      await waitWhilePaused();
      if (cancelled) throw new Cancelled();
    };

    (async () => {
      while (!cancelled) {
        await waitWhilePaused();
        if (cancelled) break;
        setLog([]);
        // A new instance starts with no variables: drop the previous run's held
        // ones here, not via the render's `!snapshot` branch — reset() and
        // createInstance() land in one batched render, so that branch never runs.
        lastVarsRef.current = {};
        // Wipe the engine before each run so `completedInstances` starts at 0 —
        // otherwise a prior run's completed instance makes the next one "finish" at once.
        reset();
        try {
          await scenario.drive({
            // The hook's commands are stable callbacks over its session ref.
            bojtos: bojtosRef.current,
            processId: processIdRef.current,
            log: (kind, text) => {
              if (!cancelled) pushLog(kind, text);
            },
            noteVars: (v) => {
              if (!cancelled) lastVarsRef.current = { ...lastVarsRef.current, ...v };
            },
            beat,
            cancelled: () => cancelled,
          });
        } catch (e) {
          if (e instanceof Cancelled) break;
          throw e;
        }
        await sleep(RESTART_DELAY);
      }
    })();

    return () => {
      cancelled = true;
    };
    // Intentionally keyed on phase + scenario only: the driver reads live values via refs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [phase, scenario]);

  const activeIds = snapshot?.activeElementIds ?? [];
  const incidentIds = snapshot?.incidentElementIds ?? [];
  // A completed instance's variables are dropped from the snapshot, so hold the
  // last ones seen until the next run starts rather than blanking at the finish.
  const liveVars = snapshot?.instances?.[0]?.variables;
  if (liveVars && Object.keys(liveVars).length > 0) lastVarsRef.current = { ...lastVarsRef.current, ...liveVars };
  else if (!snapshot) lastVarsRef.current = {};
  const vars = lastVarsRef.current;

  return (
    <div className={embed ? "page embed" : "page"}>
      <ParticleField />

      {!embed && (
        <>
          <header className="topbar">
            <a className="brand" href="/">
              <span className="logo" aria-hidden>
                ◆
              </span>
              nanobpm<span className="brand-io">.io</span>
            </a>
            <nav className="topnav">
              <a href="/">Home</a>
              <a href="/schemas/">Schemas</a>
            </nav>
          </header>

          <section className="hero">
            <h1>
              The engine,
              <br />
              <span className="grad">running in your browser.</span>
            </h1>
            <p className="sub">
              nanobpm is the load-bearing runtime for agent systems — a from-scratch process
              orchestration engine in a single Rust binary. Pick a flow below: two Nano Agent Graph
              Engine flows (an adversarial branch review, and publishing a video), or an Urban app's
              PR review loop. Each runs live on the WebAssembly build of the very same engine — no
              server, no mocks in the runtime. Agents and people are scripted.
            </p>
          </section>
        </>
      )}

      <section className="demo">
        {!pinned && (
        <div className="scenario-picker" role="tablist" aria-label="Example flow">
          {SCENARIOS.map((s) => (
            <button
              key={s.id}
              role="tab"
              aria-selected={s.id === scenario.id}
              className={s.id === scenario.id ? "scenario on" : "scenario"}
              onClick={() => {
                if (s.id !== scenario.id) {
                  setLog([]);
                  setScenario(s);
                }
              }}
            >
              {s.label}
            </button>
          ))}
        </div>
        )}
        <div className="demo-head">
          <div className="demo-title">
            <span className={`pulse ${running ? "on" : ""}`} aria-hidden />
            Live: {scenario.title}
          </div>
          <div className="controls">
            <button className="btn ghost" onClick={() => setRunning((r) => !r)}>
              {running ? "Pause" : "Play"}
            </button>
          </div>
        </div>

        <div className={scenario.wide ? "demo-grid wide" : "demo-grid"}>
          <div className="diagram-wrap">
            {phase === "error" ? (
              <div className="fallback">Failed to load the engine: {error}</div>
            ) : phase === "loading" ? (
              <div className="fallback">Booting the WebAssembly engine…</div>
            ) : (
              <BpmnRuntimeView
                xml={scenario.bpmn}
                activeIds={activeIds}
                incidentIds={incidentIds}
                className="diagram"
              />
            )}
          </div>

          <aside className="side">
            <div className="panel">
              <div className="panel-h">Process variables</div>
              <dl className="vars">
                {Object.entries(vars).map(([k, v]) => (
                  <div className="var" key={k}>
                    <dt>{k}</dt>
                    <dd>{format(v)}</dd>
                  </div>
                ))}
                {Object.keys(vars).length === 0 && (
                  <div className="var muted">
                    <dt>—</dt>
                    <dd>waiting for the first instance…</dd>
                  </div>
                )}
              </dl>
            </div>

            <div className="panel grow">
              <div className="panel-h">Activity</div>
              <ul className="log" ref={logRef}>
                {log.map((e) => (
                  <li key={e.id} className={`log-${e.kind}`}>
                    {e.text}
                  </li>
                ))}
              </ul>
            </div>
          </aside>
        </div>
      </section>

      {!embed && (
        <footer className="foot">
          Running <code>@nanobpm/engine-wasm</code> via the Bojtos in-browser framework.
          The agents and humans here are scripted stand-ins; every token move, gateway, message,
          user task and variable update is the actual engine.
        </footer>
      )}
    </div>
  );
}

function format(v: unknown): string {
  if (typeof v === "string") return v;
  return JSON.stringify(v);
}
