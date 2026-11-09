import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement, type ReactNode } from "react";
import { renderToString } from "react-dom/server";
import {
  QueryClient,
  QueryClientProvider,
  useQueryClient,
} from "@tanstack/react-query";
import {
  ExplorerDataProvider,
  createSourceQueryClient,
  type ExplorerDataSource,
} from "./dataSource.tsx";

function source(): ExplorerDataSource {
  return {
    listInstances: async () => ({ items: [], total: 0 }) as never,
    getInstance: async () => ({}) as never,
    getTrace: async () => undefined,
    getProcessXml: async () => null,
  };
}

/** The QueryClient the Explorer's components see under a provider for `src`. */
function clientUnder(outer: QueryClient, src: ExplorerDataSource): QueryClient {
  let seen: QueryClient | undefined;
  function Probe(): ReactNode {
    seen = useQueryClient();
    return null;
  }
  renderToString(
    createElement(
      QueryClientProvider,
      { client: outer },
      createElement(ExplorerDataProvider, {
        source: src,
        children: createElement(Probe),
      }),
    ),
  );
  assert.ok(seen);
  return seen;
}

test("each data source gets a query cache isolated from the host and other sources", () => {
  const outer = new QueryClient();
  const a = clientUnder(outer, source());
  const b = clientUnder(outer, source());
  assert.notEqual(a, outer);
  assert.notEqual(a, b);
  // The same key in one source's cache is invisible to another's and the host's.
  a.setQueryData(["process-xml", "1"], "<a/>");
  assert.equal(b.getQueryData(["process-xml", "1"]), undefined);
  assert.equal(outer.getQueryData(["process-xml", "1"]), undefined);
});

test("a source's cache inherits the host's default query options", () => {
  const outer = new QueryClient({
    defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } },
  });
  const c = createSourceQueryClient(outer);
  assert.equal(c.getDefaultOptions().queries?.retry, 1);
  assert.equal(c.getDefaultOptions().queries?.refetchOnWindowFocus, false);
});
