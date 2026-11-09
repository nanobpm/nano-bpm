import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Button,
  Explorer,
  ExplorerDataProvider,
} from "@nanobpm/process-explorer";
import { consoleExplorerSource } from "../lib/explorerSource.ts";
import { TOUR_ANCHOR } from "../lib/tour/tourAnchors";
import {
  markBaseUrlCopied,
  markExplorerReached,
  swaggerUrl,
  v2BaseUrl,
} from "../lib/tour/journeys/localdev";

/**
 * The console's Process Explorer route: the shared `@nanobpm/process-explorer`
 * fed by the console API, with the URL in react-router and the console-only
 * extras (the v2 base URL affordance, the guided-tour anchor).
 */
export default function ExplorerRoute() {
  const [searchParams, setSearchParams] = useSearchParams();

  // Reaching Explorer is half of the headless-local-dev journey's outcome (the
  // other half is taking the v2 base URL below). Record it on mount so the
  // journey's successEvent can tell "walked the steps" from "actually debugged
  // here". Harmless outside a tour.
  useEffect(() => {
    markExplorerReached();
  }, []);

  return (
    <ExplorerDataProvider source={consoleExplorerSource}>
      <Explorer
        searchParams={searchParams}
        setSearchParams={setSearchParams}
        tourAnchor={TOUR_ANCHOR.explorerInspect}
        headerExtra={<BaseUrlAffordance />}
      />
    </ExplorerDataProvider>
  );
}

/**
 * The v2 base URL affordance — a durable feature, not a tour-only element.
 *
 * A headless user's whole integration is "point my Camunda 8 client at one URL",
 * so the console surfaces that URL where they land to debug, with a copy button
 * and a link to the offline Swagger UI. The URL is derived from the live origin
 * (never a hardcoded port), and copy falls back to selecting the text because
 * `navigator.clipboard` is unavailable over the plain-HTTP LAN origins where a
 * local engine is typically reached.
 */
function BaseUrlAffordance() {
  const origin =
    typeof window !== "undefined" && window.location?.origin
      ? window.location.origin
      : "http://127.0.0.1:8080";
  const base = v2BaseUrl(origin);
  const inputRef = useRef<HTMLInputElement>(null);
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);

  const onCopy = async () => {
    const ok = await copyToClipboard(base);
    if (ok) {
      markBaseUrlCopied();
      setCopied(true);
      setFailed(false);
      window.setTimeout(() => setCopied(false), 1500);
    } else {
      // Could not write the clipboard (insecure context): actually select the
      // text so the user can copy it by hand, and still count it as taken.
      // Clear `copied` too, in case this was a re-click inside the 1.5s "Copied"
      // window — the label must reflect the fallback, not a stale success.
      markBaseUrlCopied();
      setCopied(false);
      setFailed(true);
      inputRef.current?.focus();
      inputRef.current?.select();
    }
  };

  return (
    <div className="mt-3 flex items-center gap-2">
      <input
        ref={inputRef}
        readOnly
        value={base}
        aria-label="Camunda 8 v2 base URL"
        onFocus={(e) => e.currentTarget.select()}
        className={`min-w-0 flex-1 rounded-md border bg-inset px-2 py-1 font-mono text-xs text-fg outline-none ${
          failed ? "border-accent" : "border-edge"
        }`}
      />
      <Button size="sm" onClick={onCopy} title="Copy the Camunda 8 v2 base URL">
        {copied ? "Copied" : failed ? "Select & copy" : "Copy v2 URL"}
      </Button>
      <a
        href={swaggerUrl(origin)}
        target="_blank"
        rel="noreferrer noopener"
        className="text-xs text-accent-strong hover:underline"
      >
        Swagger
      </a>
    </div>
  );
}

/**
 * Copy text, falling back when the async clipboard API is unavailable.
 * `navigator.clipboard` needs a secure context, which a LAN/plain-HTTP engine
 * origin is not — return false so the caller can select the text instead.
 */
async function copyToClipboard(text: string): Promise<boolean> {
  try {
    const clipboard = globalThis.navigator?.clipboard;
    if (clipboard && typeof clipboard.writeText === "function") {
      await clipboard.writeText(text);
      return true;
    }
  } catch {
    // Fall through to the legacy path.
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}
