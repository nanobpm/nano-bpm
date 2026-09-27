// The Urban App runtime as a Foldkit program (ADR 0042 §3 spike). It consumes the
// SAME page.json contract and the SAME /app/pages, /app/data and /app/actions/*
// API as the vanilla runtime — only the renderer implementation changes.
//
// Everything the screen does flows through one Model, a fact-named Message union,
// an exhaustive `update`, and explicit Commands for I/O. Compare with the vanilla
// runtime's imperative DOM + untyped `pc:refresh` CustomEvent bus.
import { Duration, Effect, Match as M, Option, Schema as S, Stream } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { AsyncData, Command, Http, Runtime, Subscription } from "foldkit";
import type { Document, Html, HtmlBuilder } from "foldkit/html";
import { m } from "foldkit/message";
import { evo } from "foldkit/struct";

import {
  ActionFormProps,
  DataGridProps,
  DataResponse,
  PageDoc,
  TextProps,
  type PageNode,
} from "./schema";

// Effect 4.0-rc has no `opt`; this is the spike's local shim.
const opt = <T>(x: T | null | undefined): Option.Option<NonNullable<T>> =>
  x == null ? Option.none() : Option.some(x as NonNullable<T>);

// MODEL -----------------------------------------------------------------------

const RowSchema = S.Record(S.String, S.Unknown);
type Row = typeof RowSchema.Type;

const PageAsync = AsyncData.Schema(PageDoc, S.String);
const RowsAsync = AsyncData.Schema(S.Array(RowSchema), S.String);

const GridState = S.Struct({ tabIndex: S.Number, rows: RowsAsync.schema });
const FormState = S.Struct({
  values: S.Record(S.String, S.String),
  status: S.String,
  busy: S.Boolean,
});

export const Model = S.Struct({
  home: S.String,
  page: PageAsync.schema,
  grids: S.Record(S.String, GridState),
  forms: S.Record(S.String, FormState),
});
export type Model = typeof Model.Type;

// MESSAGE ---------------------------------------------------------------------

const LoadedPage = m("LoadedPage", { doc: PageDoc });
const FailedPage = m("FailedPage", { error: S.String });
const ChangedField = m("ChangedField", {
  formId: S.String,
  key: S.String,
  value: S.String,
});
const SubmittedForm = m("SubmittedForm", {
  formId: S.String,
  process: S.String,
});
const FormSucceeded = m("FormSucceeded", {
  formId: S.String,
  instanceKey: S.String,
});
const FormFailed = m("FormFailed", { formId: S.String, error: S.String });
const SelectedTab = m("SelectedTab", { gridId: S.String, tabIndex: S.Number });
const LoadedGrid = m("LoadedGrid", {
  gridId: S.String,
  rows: S.Array(RowSchema),
});
const FailedGrid = m("FailedGrid", { gridId: S.String, error: S.String });
const TickedRefresh = m("TickedRefresh");

export const Message = S.Union([
  LoadedPage,
  FailedPage,
  ChangedField,
  SubmittedForm,
  FormSucceeded,
  FormFailed,
  SelectedTab,
  LoadedGrid,
  FailedGrid,
  TickedRefresh,
]);
export type Message = typeof Message.Type;

type Cmds = ReadonlyArray<Command.Command<Message>>;
type Step = readonly [Model, Cmds];

// DECODE HELPERS --------------------------------------------------------------

const decodeText = S.decodeUnknownOption(TextProps);
const decodeForm = S.decodeUnknownOption(ActionFormProps);
const decodeGrid = S.decodeUnknownOption(DataGridProps);

const pageNodes = (model: Model): ReadonlyArray<PageNode> =>
  AsyncData.getData(model.page).pipe(
    Option.map((doc) => doc.nodes ?? []),
    Option.getOrElse(() => [] as ReadonlyArray<PageNode>),
  );

// The data URL builder — byte-for-byte the same query grammar the vanilla
// runtime emits (`where=field:eq` / `where=field:in:a,b`, `order=field:dir`).
const dataUrl = (grid: typeof DataGridProps.Type, tabIndex: number): string => {
  const filters =
    grid.tabs && grid.tabs.length
      ? grid.tabs[tabIndex]?.filter ?? []
      : grid.data.filter ?? [];
  let u =
    "/app/data/" +
    encodeURIComponent(grid.data.source) +
    "/" +
    encodeURIComponent(grid.data.table);
  const qs: string[] = [];
  for (const f of filters) {
    if (f.in && f.in.length)
      qs.push("where=" + encodeURIComponent(f.field + ":in:" + f.in.join(",")));
    else if (f.eq != null)
      qs.push("where=" + encodeURIComponent(f.field + ":" + f.eq));
  }
  const order = grid.data.orderBy;
  if (order?.field)
    qs.push(
      "order=" + encodeURIComponent(order.field + ":" + (order.dir ?? "asc")),
    );
  return qs.length ? u + "?" + qs.join("&") : u;
};

// gridId → its decoded props (view + update both need this).
const gridPropsAt = (
  model: Model,
  gridId: string,
): Option.Option<typeof DataGridProps.Type> => {
  const node = pageNodes(model)[Number(gridId)];
  return node && node.type === "dataGrid"
    ? decodeGrid(node.props)
    : Option.none();
};

const loadGridCmd = (
  model: Model,
  gridId: string,
): ReadonlyArray<Command.Command<Message>> =>
  gridPropsAt(model, gridId).pipe(
    Option.map((grid) => {
      const tabIndex = model.grids[gridId]?.tabIndex ?? 0;
      return [FetchGrid({ gridId, url: dataUrl(grid, tabIndex) })];
    }),
    Option.getOrElse(() => [] as ReadonlyArray<Command.Command<Message>>),
  );

const allGridIds = (model: Model): string[] =>
  pageNodes(model)
    .map((n, i) => [n, i] as const)
    .filter(([n]) => n.type === "dataGrid")
    .map(([, i]) => String(i));

// UPDATE ----------------------------------------------------------------------

export const update = (model: Model, message: Message): Step =>
  M.value(message).pipe(
    M.withReturnType<Step>(),
    M.tagsExhaustive({
      LoadedPage: ({ doc }) => {
        const withPage = evo(model, { page: () => PageAsync.Success({ data: doc }) });
        const nodes = doc.nodes ?? [];
        const grids: Record<string, typeof GridState.Type> = {};
        const forms: Record<string, typeof FormState.Type> = {};
        const cmds: Command.Command<Message>[] = [];
        nodes.forEach((node, i) => {
          if (node.type === "dataGrid") {
            grids[String(i)] = { tabIndex: 0, rows: RowsAsync.Loading() };
          } else if (node.type === "actionForm") {
            forms[String(i)] = { values: {}, status: "", busy: false };
          }
        });
        const seeded = evo(withPage, { grids: () => grids, forms: () => forms });
        for (const id of allGridIds(seeded)) cmds.push(...loadGridCmd(seeded, id));
        return [seeded, cmds];
      },

      FailedPage: ({ error }) => [
        evo(model, { page: () => PageAsync.Failure({ error }) }),
        [],
      ],

      ChangedField: ({ formId, key, value }) => [
        evo(model, {
          forms: (forms) => ({
            ...forms,
            [formId]: {
              ...forms[formId]!,
              values: { ...forms[formId]!.values, [key]: value },
            },
          }),
        }),
        [],
      ],

      SubmittedForm: ({ formId, process }) => {
        const form = model.forms[formId];
        if (!form || form.busy) return [model, []];
        return [
          evo(model, {
            forms: (forms) => ({
              ...forms,
              [formId]: { ...form, busy: true, status: "Submitting…" },
            }),
          }),
          [StartProcess({ formId, process, variables: form.values })],
        ];
      },

      FormSucceeded: ({ formId, instanceKey }) => {
        const cmds = allGridIds(model).flatMap((id) => loadGridCmd(model, id));
        return [
          evo(model, {
            forms: (forms) => ({
              ...forms,
              [formId]: {
                values: {},
                busy: false,
                status: "Started (instance " + instanceKey + ")",
              },
            }),
          }),
          cmds,
        ];
      },

      FormFailed: ({ formId, error }) => [
        evo(model, {
          forms: (forms) => ({
            ...forms,
            [formId]: { ...forms[formId]!, busy: false, status: error },
          }),
        }),
        [],
      ],

      SelectedTab: ({ gridId, tabIndex }) => {
        const next = evo(model, {
          grids: (grids) => ({
            ...grids,
            [gridId]: { tabIndex, rows: RowsAsync.Loading() },
          }),
        });
        return [next, loadGridCmd(next, gridId)];
      },

      LoadedGrid: ({ gridId, rows }) => [
        evo(model, {
          grids: (grids) => ({
            ...grids,
            [gridId]: { ...grids[gridId]!, rows: RowsAsync.Success({ data: rows }) },
          }),
        }),
        [],
      ],

      FailedGrid: ({ gridId, error }) => [
        evo(model, {
          grids: (grids) => ({
            ...grids,
            [gridId]: { ...grids[gridId]!, rows: RowsAsync.Failure({ error }) },
          }),
        }),
        [],
      ],

      // Periodic refresh keeps existing rows on screen (no Loading flicker).
      TickedRefresh: () => [
        model,
        allGridIds(model).flatMap((id) => loadGridCmd(model, id)),
      ],
    }),
  );

// INIT ------------------------------------------------------------------------

export const init: Runtime.ApplicationInit<Model, Message> = () => {
  const home =
    document.getElementById("page")?.dataset.home?.trim() || "home";
  return [
    { home, page: PageAsync.Loading(), grids: {}, forms: {} },
    [FetchPage({ home })],
  ];
};

// COMMANDS --------------------------------------------------------------------

const getJson = (url: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const res = yield* client.execute(HttpClientRequest.get(url));
    const body = yield* res.json;
    if (res.status < 200 || res.status >= 300) {
      return yield* Effect.fail(new Error("HTTP " + res.status));
    }
    return body;
  });

export const FetchPage = Command.define("FetchPage", {
  args: { home: S.String },
  messages: [LoadedPage, FailedPage],
  execute: ({ home }) =>
    Effect.gen(function* () {
      const raw = yield* getJson("/app/pages/" + encodeURIComponent(home));
      const doc = yield* S.decodeUnknownEffect(PageDoc)(raw);
      return LoadedPage({ doc });
    }).pipe(
      Effect.provide(Http.layer),
      Effect.catch((e) =>
        Effect.succeed(FailedPage({ error: String((e as Error).message ?? e) })),
      ),
    ),
});

export const FetchGrid = Command.define("FetchGrid", {
  args: { gridId: S.String, url: S.String },
  messages: [LoadedGrid, FailedGrid],
  execute: ({ gridId, url }) =>
    Effect.gen(function* () {
      const raw = yield* getJson(url);
      const parsed = yield* S.decodeUnknownEffect(DataResponse)(raw);
      return LoadedGrid({ gridId, rows: parsed.rows ?? [] });
    }).pipe(
      Effect.provide(Http.layer),
      Effect.catch((e) =>
        Effect.succeed(
          FailedGrid({ gridId, error: String((e as Error).message ?? e) }),
        ),
      ),
    ),
});

export const StartProcess = Command.define("StartProcess", {
  args: {
    formId: S.String,
    process: S.String,
    variables: S.Record(S.String, S.String),
  },
  messages: [FormSucceeded, FormFailed],
  execute: ({ formId, process, variables }) =>
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const req = yield* HttpClientRequest.bodyJson(
        HttpClientRequest.post("/app/actions/start/" + encodeURIComponent(process)),
        { variables },
      );
      const res = yield* client.execute(req);
      const body = (yield* res.json) as { processInstanceKey?: unknown; error?: string };
      if (res.status < 200 || res.status >= 300) {
        return yield* Effect.fail(new Error(body.error ?? "HTTP " + res.status));
      }
      return FormSucceeded({
        formId,
        instanceKey: String(body.processInstanceKey ?? "?"),
      });
    }).pipe(
      Effect.provide(Http.layer),
      Effect.catch((e) =>
        Effect.succeed(
          FormFailed({ formId, error: String((e as Error).message ?? e) }),
        ),
      ),
    ),
});

// SUBSCRIPTION ----------------------------------------------------------------
// The dataGrid `refreshMs` polling, as one declarative Subscription instead of a
// scattered `setInterval`. It ticks only while some grid asks for refresh.

const minRefreshMs = (model: Model): number => {
  const vals = pageNodes(model)
    .filter((n) => n.type === "dataGrid")
    .flatMap((n) =>
      decodeGrid(n.props).pipe(
        Option.flatMap((g) => opt(g.refreshMs)),
        Option.filter((ms) => ms > 0),
        Option.match({ onNone: () => [] as number[], onSome: (ms) => [ms] }),
      ),
    );
  return vals.length ? Math.max(250, Math.min(...vals)) : 0;
};

export const subscriptions = Subscription.make<Model, Message>()((entry) => ({
  gridRefresh: entry(
    { everyMs: S.Number },
    {
      modelToDependencies: (model) => ({ everyMs: minRefreshMs(model) }),
      dependenciesToStream: ({ everyMs }) =>
        Stream.when(
          Stream.tick(Duration.millis(everyMs || 1000)).pipe(
            Stream.drop(1),
            Stream.map(() => TickedRefresh()),
          ),
          Effect.sync(() => everyMs > 0),
        ),
    },
  ),
}));

// VIEW ------------------------------------------------------------------------

const cell = (h: HtmlBuilder<Message>, v: unknown): Html =>
  h.td([], [v == null ? "" : String(v)]);

const gridView = (
  h: HtmlBuilder<Message>,
  model: Model,
  gridId: string,
  grid: typeof DataGridProps.Type,
): Html => {
  const cols = grid.columns ?? [];
  const tabs = grid.tabs ?? [];
  const state = model.grids[gridId];
  const activeTab = state?.tabIndex ?? 0;

  const tabBar = tabs.length
    ? [
        h.div(
          [h.Class("pc-tabs")],
          tabs.map((t, i) =>
            h.button(
              [
                h.Class("pc-tab" + (i === activeTab ? " active" : "")),
                h.OnClick(SelectedTab({ gridId, tabIndex: i })),
              ],
              [t.label],
            ),
          ),
        ),
      ]
    : [];

  const head = h.thead(
    [],
    [h.tr([], cols.map((c) => h.th([], [c.header ?? c.field])))],
  );

  const bodyRows = state
    ? AsyncData.matchDataSplitEmpty(state.rows, {
        onIdle: () => [rowSpan(h, cols.length, "…")],
        onLoading: () => [rowSpan(h, cols.length, "Loading…")],
        onFailure: (error) => [rowSpan(h, cols.length, error)],
        onData: (rows) =>
          rows.length
            ? rows.map((row) =>
                h.tr([], cols.map((c) => cell(h, (row as Row)[c.field]))),
              )
            : [rowSpan(h, cols.length, "No rows")],
      })
    : [rowSpan(h, cols.length, "…")];

  return h.section(
    [h.Class("pc-card")],
    [
      ...(grid.title ? [h.h2([], [grid.title])] : []),
      ...tabBar,
      h.table([h.Class("pc-grid")], [head, h.tbody([], bodyRows)]),
    ],
  );
};

const rowSpan = (h: HtmlBuilder<Message>, cols: number, text: string): Html =>
  h.tr([], [h.td([h.Attribute("colspan", String(cols || 1))], [text])]);

const formView = (
  h: HtmlBuilder<Message>,
  model: Model,
  formId: string,
  props: typeof ActionFormProps.Type,
): Html => {
  const state = model.forms[formId];
  const fields = props.fields ?? [];
  return h.section(
    [h.Class("pc-card")],
    [
      ...(props.title ? [h.h2([], [props.title])] : []),
      ...fields.map((f) =>
        h.div(
          [h.Class("pc-field")],
          [
            h.label([], [f.label ?? f.key]),
            h.input([
              h.Type("text"),
              h.Value(state?.values[f.key] ?? ""),
              h.OnInput((value) =>
                ChangedField({ formId, key: f.key, value }),
              ),
            ]),
          ],
        ),
      ),
      h.button(
        [
          h.Class("pc-btn"),
          h.OnClick(
            SubmittedForm({ formId, process: props.action.process }),
          ),
        ],
        [props.submitLabel ?? "Submit"],
      ),
      h.p([h.Class("pc-msg")], [state?.status ?? ""]),
    ],
  );
};

const nodeView = (
  h: HtmlBuilder<Message>,
  model: Model,
  node: PageNode,
  index: number,
): Html => {
  const id = String(index);
  switch (node.type) {
    case "text":
      return decodeText(node.props).pipe(
        Option.map((p) =>
          p.variant === "heading"
            ? h.h1([h.Class("pc-heading")], [p.text ?? ""])
            : h.p([h.Class(p.variant === "sub" ? "pc-sub" : "pc-body")], [p.text ?? ""]),
        ),
        Option.getOrElse(() => h.empty),
      );
    case "actionForm":
      return decodeForm(node.props).pipe(
        Option.map((p) => formView(h, model, id, p)),
        Option.getOrElse(() => h.empty),
      );
    case "dataGrid":
      return decodeGrid(node.props).pipe(
        Option.map((p) => gridView(h, model, id, p)),
        Option.getOrElse(() => h.empty),
      );
    default:
      return h.empty;
  }
};

export const view = (model: Model, h: HtmlBuilder<Message>): Document => {
  const body = AsyncData.matchDataSplitEmpty(model.page, {
    onIdle: () => h.p([h.Class("pc-msg")], ["…"]),
    onLoading: () => h.p([h.Class("pc-msg")], ["Loading…"]),
    onFailure: (error) =>
      h.p([h.Class("pc-msg err")], ["Failed to load page: " + error]),
    onData: (doc) =>
      h.div(
        [h.Class("pc-page")],
        (doc.nodes ?? []).map((n, i) => nodeView(h, model, n, i)),
      ),
  });
  const title = AsyncData.getData(model.page).pipe(
    Option.flatMap((d) => opt(d.title)),
    Option.getOrElse(() => "Urban App"),
  );
  return { title, body };
};
