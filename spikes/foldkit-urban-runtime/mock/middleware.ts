import type { Connect, Plugin } from "vite";
import { PAGE, RUNS } from "./fixture";

// Stands in for the App-side ADR 0026 action API so the Foldkit renderer runs
// end-to-end in dev/preview. Not part of the shipped runtime.
const json = (res: Parameters<Connect.NextHandleFunction>[1], body: unknown) => {
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(body));
};

const handle: Connect.NextHandleFunction = (req, res, next) => {
  const url = new URL(req.url ?? "/", "http://localhost");
  const p = url.pathname;
  if (p.startsWith("/app/pages/")) return json(res, PAGE);
  if (p.startsWith("/app/data/")) {
    const where = url.searchParams.get("where") ?? "";
    let rows = RUNS;
    const m = where.match(/^state:(in:(.+)|(.+))$/);
    if (m) {
      const set = m[2] ? m[2].split(",") : [m[3]];
      rows = RUNS.filter((r) => set.includes(r.state));
    }
    return json(res, { rows });
  }
  if (p.startsWith("/app/actions/start/")) {
    return json(res, { processInstanceKey: Math.floor(Math.random() * 1e6) });
  }
  next();
};

export const mockUrbanApi = (): Plugin => ({
  name: "mock-urban-api",
  configureServer(server) {
    server.middlewares.use(handle);
  },
  configurePreviewServer(server) {
    server.middlewares.use(handle);
  },
});
