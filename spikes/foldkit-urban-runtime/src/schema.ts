// The page.json contract (ADR 0042 §1), expressed as an Effect Schema so the
// runtime validates every page it renders instead of trusting `any`. This is the
// single biggest architectural difference from the vanilla runtime, which reads
// `node.props.*` untyped and fails silently on a shape mismatch.
//
// Spike scope: the core vocabulary (text / actionForm / dataGrid with columns,
// tabs and interval refresh). rowActions / detail / childGrid nesting are noted
// in the findings but not modelled here.
import { Schema as S } from "effect";

export const ColumnFilter = S.Struct({
  field: S.String,
  eq: S.optional(S.String),
  in: S.optional(S.Array(S.String)),
});

export const OrderBy = S.Struct({
  field: S.String,
  dir: S.optional(S.Literals(["asc", "desc"])),
});

export const DataBinding = S.Struct({
  source: S.String,
  table: S.String,
  filter: S.optional(S.Array(ColumnFilter)),
  orderBy: S.optional(OrderBy),
});

export const Column = S.Struct({
  field: S.String,
  header: S.optional(S.String),
});

export const Tab = S.Struct({
  label: S.String,
  filter: S.optional(S.Array(ColumnFilter)),
});

// --- node props ---------------------------------------------------------------

export const TextProps = S.Struct({
  variant: S.optional(S.Literals(["heading", "sub", "body"])),
  text: S.optional(S.String),
});

export const ActionFormField = S.Struct({
  key: S.String,
  label: S.optional(S.String),
});

export const ActionFormProps = S.Struct({
  title: S.optional(S.String),
  submitLabel: S.optional(S.String),
  fields: S.optional(S.Array(ActionFormField)),
  action: S.Struct({ process: S.String }),
});

export const DataGridProps = S.Struct({
  title: S.optional(S.String),
  columns: S.optional(S.Array(Column)),
  tabs: S.optional(S.Array(Tab)),
  data: DataBinding,
  refreshMs: S.optional(S.Number),
});

// A node is `{ type, props }`; props are decoded per branch in the view. Keeping
// props as Unknown here mirrors the wire shape and lets an unknown node type be
// rendered inertly instead of crashing the whole page.
export const PageNode = S.Struct({
  type: S.String,
  props: S.Unknown,
});
export type PageNode = typeof PageNode.Type;

export const PageDoc = S.Struct({
  title: S.optional(S.String),
  nodes: S.optional(S.Array(PageNode)),
});
export type PageDoc = typeof PageDoc.Type;

export type Row = Record<string, unknown>;
export const DataResponse = S.Struct({
  rows: S.optional(S.Array(S.Record(S.String, S.Unknown))),
});
