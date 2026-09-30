/** The datasource binding attached to a choice field's `dataSource` property. */
export interface FormFieldDataBinding {
	/** Datasource alias (a key of manifest `data.sources`). */
	source: string;
	/** A read query (SELECT …) whose rows become the field's options. */
	query: string;
	/** Row column mapped to each option's `value` (default `"value"`). */
	value?: string;
	/** Row column mapped to each option's `label` (default `"label"`). */
	label?: string;
}
/** A binding located within a form schema, with the field it belongs to. */
export interface CollectedFormBinding {
	/** The bound field's `key` (its data path), when it has one. */
	fieldKey?: string;
	/** The field component's `id` (form-js always assigns one). */
	fieldId?: string;
	/** JSON-pointer-ish path to the field within the schema, for diagnostics. */
	path: string;
	binding: FormFieldDataBinding;
}
/** A single resolved option, matching form-js's static `values` entry shape. */
export interface FormOption {
	value: string;
	label: string;
}
/** The form-js component `type`s that render an option list. */
export declare const CHOICE_FIELD_TYPES: ReadonlySet<string>;
/**
 * Reads a well-formed `dataSource` binding off a component, or `undefined`.
 * Deliberately strict: `source` and `query` must be non-empty strings, so a
 * half-typed or malformed binding is simply ignored (and separately flagged by
 * the validator) rather than throwing at render time.
 */
export declare function readFieldDataBinding(component: unknown): FormFieldDataBinding | undefined;
/**
 * Walks a form-js schema (recursing into layout components: groups, dynamic
 * lists) and returns every field carrying a datasource binding. Order is
 * document order so diagnostics are stable.
 */
export declare function collectFormDataBindings(schema: unknown): CollectedFormBinding[];
/**
 * Maps datasource query rows to form-js static options. Uses the binding's
 * `value`/`label` columns, defaulting to `"value"`/`"label"`; when only one
 * mapping resolves, it doubles as the other so a `SELECT name` still renders.
 */
export declare function rowsToOptions(rows: ReadonlyArray<Record<string, unknown>>, binding: FormFieldDataBinding): FormOption[];
/**
 * Returns a deep clone of `schema` with each bound field's options replaced by
 * the resolved list (keyed by the field's `id`, falling back to `key`). The
 * field is switched to form-js's static source (`values` set, `valuesKey`
 * cleared) so a plain viewer renders the live options with no extra wiring. The
 * input schema is never mutated.
 */
export declare function applyDataSourceOptions(schema: unknown, resolved: ReadonlyMap<string, ReadonlyArray<FormOption>>): unknown;
/** A model file to index. `kind` selects the parser; `text` is the file body. */
export interface ModelFile {
	path: string;
	kind: "bpmn" | "dmn" | "form";
	text: string;
}
export interface UserTaskSymbol {
	id: string;
	name?: string;
	/** zeebe:formDefinition formId, if the task references a form. */
	formId?: string;
}
export interface ProcessSymbol {
	id: string;
	name?: string;
	executable: boolean;
	/** Message names on message start events (targets of action.message that start an instance). */
	messageStartEvents: string[];
	userTasks: UserTaskSymbol[];
	/** zeebe:taskDefinition types of service tasks (worker taskTypes). */
	serviceTaskTypes: string[];
}
export interface DecisionSymbol {
	id: string;
	name?: string;
}
export interface FormFieldSymbol {
	key: string;
	type: string;
	/** Datasource binding (ADR 0024 §5), when the field declares one. */
	dataSource?: FormFieldDataBinding;
}
export interface FormSymbol {
	id: string;
	fields: FormFieldSymbol[];
}
/** The primitive field types a domain type (ADR 0029 §4 / ADR 0031) may use. */
export type DomainPrimitive = "string" | "number" | "integer" | "boolean" | "date" | "datetime" | "json";
export declare const DOMAIN_PRIMITIVES: readonly DomainPrimitive[];
export interface InferredField {
	key: string;
	type: DomainPrimitive;
}
/**
 * A candidate domain record inferred from a form's fields — the ADR 0029 §4
 * on-ramp: the maker either promotes it into the `types` registry or binds it
 * to a datasource table. Inference is heuristic (form keys are free strings),
 * so it is a suggestion, never a silently-invented schema.
 */
export interface InferredRecord {
	/** Candidate type id — the source form's id. */
	id: string;
	source: "form";
	sourcePath: string;
	fields: InferredField[];
}
export interface SymbolIndex {
	processes: ProcessSymbol[];
	/** All declared bpmn:message names (targets of action.message). */
	messages: string[];
	decisions: DecisionSymbol[];
	forms: FormSymbol[];
	/** Candidate domain records inferred from forms (ADR 0029 §4 promotion on-ramp). */
	inferredRecords: InferredRecord[];
	/** Non-fatal problems encountered while parsing a model file. */
	parseErrors: {
		path: string;
		message: string;
	}[];
}
/**
 * Map a form-js component `type` to a domain primitive (ADR 0029 §4). Heuristic
 * and deliberately conservative — anything not clearly numeric/boolean/temporal
 * falls back to `string`, and the maker confirms on promotion.
 */
export declare function formTypeToPrimitive(formType: string): DomainPrimitive;
/**
 * Build the symbol index from a project's model files. Parse failures are
 * collected in `parseErrors` rather than thrown, so one malformed model does not
 * blind the index to the rest of the project.
 */
export declare function buildSymbolIndex(models: ModelFile[]): Promise<SymbolIndex>;
/** Classify a model file by extension (helper for callers listing a project dir). */
export declare function modelKindOf(path: string): ModelFile["kind"] | undefined;
export interface ResolvedField {
	key: string;
	/** A primitive, or the id of another declared type (nominal). */
	type: string;
	optional: boolean;
	list: boolean;
}
export interface ResolvedDomainType {
	id: string;
	name?: string;
	/** Identity discipline; "nominal" today (the structural escape hatch is reserved). */
	match: "nominal" | "structural";
	/** Datasource table this type binds to as its rest projection, if any. */
	table?: string;
	fields: ResolvedField[];
}
export interface DomainTypeResolution {
	/** Types declared in the manifest `types` registry. */
	declared: ResolvedDomainType[];
	/** Form-inferred candidates not already declared — a maker may promote these. */
	inferred: InferredRecord[];
}
/**
 * Resolve the domain types a maker can reference. Pass the project `index` to
 * include form-inferred candidates; omit it for the declared registry alone.
 */
export declare function resolveDomainTypes(manifest: unknown, index?: SymbolIndex): DomainTypeResolution;
export interface FeelFieldDef {
	type?: string;
	list?: boolean;
}
/** Whether `id` names a declared domain type (not a primitive / unknown). */
export declare function isDeclaredType(manifest: unknown, id: string | undefined): boolean;
/** Declared fields of a domain type id (empty when the id is unknown/absent). */
export declare function fieldsOf(manifest: unknown, typeId: string | undefined): Record<string, FeelFieldDef>;
/** Outcome of resolving a dotted `body`-rooted path against the scope type. */
export type PathResolution = 
/** The full path resolves to a declared field. */
{
	kind: "ok";
	type?: string;
	list?: boolean;
}
/** Just `body` — the root, no segments to resolve. */
 | {
	kind: "root";
}
/** A segment is not a field of the (known) type at that point. */
 | {
	kind: "unknown";
	segment: string;
}
/** The walk passed through a primitive `json`/unknown type — can't verify. */
 | {
	kind: "indeterminate";
};
/**
 * Resolve `segs` (the path after `body`) against `bodyType`, walking nested
 * declared types. Descending into a list of a declared type follows FEEL's list
 * projection (`body.items.name`). The walk is deliberately conservative: it only
 * reports `unknown` when a segment is definitively absent from a *declared* type
 * at that point, and reports `indeterminate` (never a false error) once it hits a
 * `json` field or an undeclared type whose shape it cannot know.
 */
export declare function resolveBodyPath(manifest: unknown, bodyType: string | undefined, segs: string[]): PathResolution;
/**
 * Extract the `body`-rooted dotted paths in a FEEL expression, as segment arrays
 * *excluding* the leading `body`. Conservative on purpose: it matches only a
 * standalone `body` identifier followed by one or more `.field` accessors, and
 * skips anything followed by `[` (indexing) or `(` (a call) — so complex FEEL
 * never yields a spurious path to flag.
 */
export declare function bodyPaths(feel: string): string[][];
/**
 * A neutral variable-scope node (ADR 0029 §5). An editor-agnostic tree the
 * console maps onto its FEEL editor's variable shape (e.g. dmn-js / feel-editor
 * `Variable`), so the type-in-scope logic stays here (tested) and the editor
 * wiring stays thin. `entries` are the fields of a nested declared type.
 */
export interface ScopeVar {
	name: string;
	/** The field's declared type or primitive — a short hint for the editor. */
	type?: string;
	list?: boolean;
	entries?: ScopeVar[];
}
/**
 * The fields of `typeId` as a scope tree, recursing into nested declared types
 * (lists included — FEEL projects a list of records). Cycles in the nominal type
 * graph are broken by tracking the types on the current path, so a self- or
 * mutually-recursive type resolves one level deep without looping.
 */
export declare function scopeVarsForType(manifest: unknown, typeId: string | undefined): ScopeVar[];
/**
 * The variable scope for a decision's input-expression FEEL: the fields of the
 * domain type bound to `decisionId` in `bindings[]` (ADR 0029 §5). Returns
 * `undefined` when the decision has no binding, or the binding's type is not a
 * declared type — callers then contribute no domain variables (never a wrong scope).
 */
export declare function decisionScope(manifest: unknown, decisionId: string | undefined): ScopeVar[] | undefined;
/**
 * The variable scope for a process's FEEL (component/service-task inputs, gateway
 * conditions): the fields of the domain type bound to `processId` in `bindings[]`
 * — the process as the motion of a typed domain object (ADR 0030). Returns
 * `undefined` when the process has no binding or the bound type is not declared.
 */
export declare function processScope(manifest: unknown, processId: string | undefined): ScopeVar[] | undefined;
/**
 * A component output mapping: a service task's `taskType` (the worker seam,
 * ADR 0022) and the process variable one of its output mappings writes into.
 * The console extracts these from the diagram; `componentOutputScope` types them.
 */
export interface ComponentOutput {
	taskType: string;
	target: string;
}
/**
 * The declared domain type a worker writes as its result (ADR 0033 §3), resolved
 * by a component's `taskType`. Returns `undefined` when no worker matches, the
 * worker declares no `outputType`, or that type is not declared — callers then
 * leave the output variable untyped (never a wrong scope).
 */
export declare function outputTypeForTaskType(manifest: unknown, taskType: string | undefined): string | undefined;
/**
 * The declared domain type id bound to a form in `bindings[]` (ADR 0029 §5), or
 * `undefined` when the form has no binding or the bound type is not declared. A
 * user task's data envelope (ADR 0033 §6) defaults to this: a user task whose
 * linked form (`zeebe:formDefinition:formId`) is typed inherits that type as its
 * envelope, so the form binding stays the single source of truth (no second
 * per-task binding is authored unless the maker overrides it in the model).
 */
export declare function formTypeId(manifest: unknown, formId: string | undefined): string | undefined;
/**
 * The variable scope contributed by a process's component outputs (ADR 0033 §3):
 * each output-mapped process variable typed by the domain type its worker
 * declares (`workers[].outputType`). This is the "component output → typed
 * process variable → next component input" continuity — a task placed after a
 * component autocompletes on the result's fields. Outputs whose worker declares
 * no (declared) `outputType` are skipped; a variable written by more than one
 * component keeps the first typed occurrence.
 */
export declare function componentOutputScope(manifest: unknown, outputs: readonly ComponentOutput[]): ScopeVar[];
/** The `data.query` builtin's editor-facing signature descriptor. */
export interface FeelFunctionSignature {
	/** The callable name as written in FEEL. */
	name: string;
	/** One-line human hint for autocomplete/detail. */
	detail: string;
	/** The accepted call forms, most-specific first. */
	forms: string[];
	/** A longer description for signature/documentation surfaces. */
	doc: string;
}
/**
 * The `data.query` builtin contract (ADR 0024 §5). Read-only: it reads from a
 * datasource, never writes. Two forms — an explicit alias, or the default
 * source (`data.default`) when the alias is omitted.
 */
export declare const DATA_QUERY: FeelFunctionSignature;
/** A `data.query(...)` call site found in a FEEL expression. */
export interface DataQueryCall {
	/**
	 * The datasource alias named as the first argument in the two-argument form
	 * `data.query("alias", "SELECT …")`. `null` for the single-argument form
	 * `data.query("SELECT …")` (which uses `data.default`) or when the first
	 * argument is not a plain string literal (dynamic — unverifiable, never a
	 * false error), mirroring the conservative stance of `bodyPaths`.
	 */
	source: string | null;
	/** Offset of the `data.query` occurrence in the expression. */
	index: number;
}
/**
 * Extract the `data.query(...)` call sites in a FEEL expression, reporting the
 * datasource alias of each (see {@link DataQueryCall}). Used by the validator to
 * cross-check the alias against `data.sources`. Delegates to the shared scanner
 * in `data-query.ts` so parsing and pre-resolution can never disagree.
 */
export declare function dataQueryCalls(feel: string): DataQueryCall[];
/** The kinds of reference a manifest string value can be. */
export type ReferenceSite = "process" | "message" | "decision" | "field-type" | "body-type" | "binding-type" | "input-type" | "output-type" | "form-ref" | "process-ref" | "datasource" | "agent";
export type CandidateKind = "process" | "message" | "decision" | "primitive" | "type" | "form" | "datasource" | "agent" | "function" | "variable";
export interface CompletionCandidate {
	/** The literal id/name to insert (unquoted). */
	value: string;
	kind: CandidateKind;
	/** Short human hint (e.g. a process name or "primitive"). */
	detail?: string;
}
export interface ManifestCompletion {
	site: ReferenceSite | "feel";
	/** Offset span of the string *content* (between the quotes) to replace. */
	range: {
		start: number;
		end: number;
	};
	candidates: CompletionCandidate[];
}
/** Just the parts of the index this engine reads (keeps callers flexible). */
export type CompletionIndex = Pick<SymbolIndex, "processes" | "messages" | "decisions" | "forms">;
/**
 * The public entry point: what completions apply at `offset`, or null when the
 * cursor is not inside a recognized reference value or FEEL expression.
 */
export declare function manifestCompletionAt(text: string, offset: number, manifest: unknown, index?: CompletionIndex): ManifestCompletion | null;
export interface Diagnostic {
	severity: "error";
	/** JSON Pointer (RFC 6901) to the offending node. */
	pointer: string;
	message: string;
	/** Stable code: "schema" for shape errors, else the cross-reference rule id. */
	code: string;
}
export interface ValidationResult {
	ok: boolean;
	diagnostics: Diagnostic[];
}
/**
 * Validate a manifest fail-closed. Pass the project `index` to enable the
 * model-resolving rules (start/message/decision); omit it for a manifest-only
 * lint (schema + intra-manifest references). Returns `ok:false` with the
 * diagnostics whenever anything fails.
 */
export declare function validateManifest(manifest: unknown, index?: SymbolIndex): ValidationResult;
/** The prefix for a generated binding variable a `data.query(...)` call becomes. */
export declare const DATA_QUERY_BINDING_PREFIX = "__dq";
/** A `data.query(...)` call site located in a FEEL expression. */
export interface DataQueryCallSpan {
	/** `[start, end)` offsets of the whole `data.query(...)` call in the source. */
	span: [
		number,
		number
	];
	/**
	 * The datasource alias — the first argument's string literal in the
	 * two-argument form `data.query("alias", "SELECT …")`. `null` for the
	 * single-argument (default-source) form or a dynamic first argument.
	 */
	source: string | null;
	/**
	 * The SQL — the last argument's string literal, when it is statically
	 * resolvable. `null` when the SQL argument is dynamic (a non-literal FEEL
	 * expression) and so cannot be pre-resolved.
	 */
	sql: string | null;
	/** Whether the call can be pre-resolved up-front (`sql` is a literal). */
	static: boolean;
}
/**
 * Locate the `data.query(...)` call sites in a FEEL expression, with their full
 * span and statically-resolvable arguments. Conservative: an argument that is
 * not a plain string literal yields `null` for that slot (never a guess).
 */
export declare function scanDataQueryCalls(feel: string): DataQueryCallSpan[];
/** A row set, as returned by the datasource gateway (ADR 0024 phase 2). */
export type DataQueryRows = ReadonlyArray<Record<string, unknown>>;
/**
 * Runs one `data.query(...)` read against the datasource gateway. `source` is
 * the named alias, or `null` for the default-source form (the caller maps it to
 * `data.default`). Read-only by contract (ADR 0024 §5).
 */
export type DataQueryResolver = (source: string | null, sql: string) => Promise<DataQueryRows>;
/** A pre-resolved expression: the rewritten FEEL plus the bound row sets. */
export interface PreResolvedExpr {
	/** The expression with each resolved `data.query(...)` call replaced by its binding. */
	expr: string;
	/** The binding variables (`__dq0`, …) → resolved rows to add to the FEEL context. */
	context: Record<string, DataQueryRows>;
}
/**
 * Pre-resolve the statically-resolvable `data.query(...)` calls in one FEEL
 * expression: run each distinct `(source, sql)` once through `resolve`, bind the
 * rows to a fresh `__dq<n>` variable, and rewrite the calls to reference it. The
 * returned `expr` evaluates synchronously once `context` is merged into the FEEL
 * data. `nextIndex` seeds the binding counter so a caller can keep names unique
 * across many expressions (see `preResolveFormSchema`).
 */
export declare function preResolveDataQuery(feel: string, resolve: DataQueryResolver, nextIndex?: number): Promise<PreResolvedExpr & {
	nextIndex: number;
}>;
/** A per-expression pre-resolution failure, located by a schema JSON path. */
export interface DataQueryError {
	/** Dotted/indexed path to the offending schema property (e.g. `components.0.conditional.hide`). */
	path: string;
	message: string;
}
/** The result of pre-resolving a whole form schema's `data.query(...)` calls. */
export interface PreResolvedForm {
	/** A deep copy of the schema with resolved calls rewritten to their bindings. */
	schema: unknown;
	/** The bound row sets to seed as the form's initial data (the FEEL context). */
	data: Record<string, DataQueryRows>;
	/** Per-expression resolution errors; the call is bound to `[]` so the form still renders. */
	errors: DataQueryError[];
}
/**
 * Pre-resolve every `=`-prefixed FEEL expression in a form-js schema that
 * contains a `data.query(...)` call (ADR 0024 §5). Walks the schema, rewrites
 * each such expression in place (a deep copy — the input is untouched), and
 * returns the bound row sets to seed as the form's initial data so form-js's
 * synchronous evaluation resolves the bindings from context. Distinct queries
 * across the whole form share one binding and one gateway call. A failing query
 * is bound to an empty list and reported in `errors`, so an unbound field never
 * breaks the whole preview.
 */
export declare function preResolveFormSchema(schema: unknown, resolve: DataQueryResolver): Promise<PreResolvedForm>;
export declare const PAGE_NODE_TYPES: readonly [
	"text",
	"nav",
	"actionForm",
	"dataGrid",
	"prose",
	"button"
];
/** A node type known to a composed page — one of {@link PAGE_NODE_TYPES}. */
export type PageNodeType = (typeof PAGE_NODE_TYPES)[number];
/** Narrowing guard: is `value` a known page node type? */
export declare function isPageNodeType(value: unknown): value is PageNodeType;
/**
 * The single canonical mobile breakpoint: pages render their mobile presentation
 * at or below this viewport width. This is the ONE breakpoint constant in the
 * system — consumers (the console's `useIsNarrow`, the Urban runtime's
 * `isNarrow()`) MUST import it rather than restating a literal `640px`/`640`, so
 * the breakpoint can never fork.
 */
export declare const MOBILE_MAX_WIDTH: "640px";
/** The type of {@link MOBILE_MAX_WIDTH} — the canonical breakpoint literal. */
export type MobileMaxWidth = typeof MOBILE_MAX_WIDTH;
/**
 * How a grid column is presented on mobile (`column.mobile.priority`):
 *   - `"primary"` — shown as the card's primary line;
 *   - `"chip"` — shown as a secondary chip/badge on the card;
 *   - `"hidden"` — dropped from the mobile card entirely.
 */
export declare const COLUMN_MOBILE_PRIORITIES: readonly [
	"primary",
	"chip",
	"hidden"
];
/** A column's mobile priority — one of {@link COLUMN_MOBILE_PRIORITIES}. */
export type ColumnMobilePriority = (typeof COLUMN_MOBILE_PRIORITIES)[number];
/**
 * The mobile presentation hints on a grid column (`column.mobile`) — the shape
 * the Urban runtime already reads (`column.mobile.priority`,
 * `column.mobile.label`).
 */
export interface ColumnMobile {
	/** How this column is surfaced on the mobile card. */
	priority: ColumnMobilePriority;
	/** An optional short label to use in place of the column header on mobile. */
	label?: string;
}
/** Narrowing guard: is `value` a known column mobile priority? */
export declare function isColumnMobilePriority(value: unknown): value is ColumnMobilePriority;
/**
 * A `nav` node's layout variant (`nav.props.variant`):
 *   - `"bar"` — a horizontal top bar;
 *   - `"rail"` — a vertical side rail;
 *   - `"cards"` — a launcher grid of cards (the mobile-first home).
 */
export declare const NAV_VARIANTS: readonly [
	"bar",
	"rail",
	"cards"
];
/** A nav variant — one of {@link NAV_VARIANTS}. */
export type NavVariant = (typeof NAV_VARIANTS)[number];
/** Narrowing guard: is `value` a known nav variant? */
export declare function isNavVariant(value: unknown): value is NavVariant;
/**
 * How a `nav` node collapses items that do not fit (`nav.props.overflow`):
 *   - `"menu"` — overflowing items move into a hamburger/overflow menu.
 * Omitting `overflow` keeps the default (no collapse; items wrap/scroll).
 */
export declare const NAV_OVERFLOW_MODES: readonly [
	"menu"
];
/** A nav overflow mode — one of {@link NAV_OVERFLOW_MODES}. */
export type NavOverflow = (typeof NAV_OVERFLOW_MODES)[number];
/** Narrowing guard: is `value` a known nav overflow mode? */
export declare function isNavOverflow(value: unknown): value is NavOverflow;
/**
 * Which group a nav item belongs to (`nav.items[].group`):
 *   - `"primary"` — rendered as a first-class card / top-level item;
 *   - `"secondary"` — demoted (e.g. into the overflow menu).
 */
export declare const NAV_ITEM_GROUPS: readonly [
	"primary",
	"secondary"
];
/** A nav item group — one of {@link NAV_ITEM_GROUPS}. */
export type NavItemGroup = (typeof NAV_ITEM_GROUPS)[number];
/**
 * The default nav item group. An item that annotates no `group` is `"primary"`,
 * so an app that annotates nothing gets every item rendered as a card.
 */
export declare const NAV_ITEM_GROUP_DEFAULT: NavItemGroup;
/** Narrowing guard: is `value` a known nav item group? */
export declare function isNavItemGroup(value: unknown): value is NavItemGroup;
/**
 * How a `dataGrid` presents itself on mobile
 * (`dataGrid.props.mobile.presentation`):
 *   - `"cards"` — one card per row (the mobile-first default);
 *   - `"table"` — stay tabular (escape hatch for a grid that must be a table).
 */
export declare const DATA_GRID_MOBILE_PRESENTATIONS: readonly [
	"cards",
	"table"
];
/** A grid's mobile presentation — one of {@link DATA_GRID_MOBILE_PRESENTATIONS}. */
export type DataGridMobilePresentation = (typeof DATA_GRID_MOBILE_PRESENTATIONS)[number];
/**
 * The default grid mobile presentation. A grid that annotates no
 * `mobile.presentation` renders as `"cards"` on mobile.
 */
export declare const DATA_GRID_MOBILE_PRESENTATION_DEFAULT: DataGridMobilePresentation;
/** The mobile presentation hints on a grid (`dataGrid.props.mobile`). */
export interface DataGridMobile {
	/**
	 * How the grid renders below {@link MOBILE_MAX_WIDTH}. Omitting it defaults to
	 * {@link DATA_GRID_MOBILE_PRESENTATION_DEFAULT} (`"cards"`).
	 */
	presentation?: DataGridMobilePresentation;
}
/** Narrowing guard: is `value` a known grid mobile presentation? */
export declare function isDataGridMobilePresentation(value: unknown): value is DataGridMobilePresentation;
/**
 * The page-level mobile layout variant (`layout.mobile`) — the Tier-2 switch the
 * runtime's `isNarrow()` anticipates (Tier-1 is the pure CSS reflow at
 * {@link MOBILE_MAX_WIDTH}; Tier-2 is this explicit, authored variant):
 *   - `"stack"` — the natural vertical stack of the page's nodes;
 *   - `"cards"` — a launcher/card mobile layout.
 */
export declare const LAYOUT_MOBILE_VARIANTS: readonly [
	"stack",
	"cards"
];
/** A page-level mobile layout variant — one of {@link LAYOUT_MOBILE_VARIANTS}. */
export type LayoutMobileVariant = (typeof LAYOUT_MOBILE_VARIANTS)[number];
/**
 * The default page mobile layout variant. A page that declares `layout` but no
 * `layout.mobile` reflows as `"stack"`.
 */
export declare const LAYOUT_MOBILE_VARIANT_DEFAULT: LayoutMobileVariant;
/** Narrowing guard: is `value` a known page mobile layout variant? */
export declare function isLayoutMobileVariant(value: unknown): value is LayoutMobileVariant;
/**
 * The page-level layout hints (`page.layout`). Its `mobile` field is the Tier-2
 * variant hook; omitting it leaves the runtime on the Tier-1 CSS reflow.
 */
export interface PageLayout {
	/** The Tier-2 mobile layout variant (see {@link LAYOUT_MOBILE_VARIANTS}). */
	mobile?: LayoutMobileVariant;
}
/**
 * GENERATED — do not edit by hand.
 *
 * TypeScript types for the Urban App manifest (nano.app.json), generated from
 * spec-app/nano-app.schema.json (ADR 0027). Regenerate with:  npm run gen
 * (from spec-app/) or  make generate-app-manifest  (from the repo root).
 */
/**
 * BPMN process globs. Overrides the convention default (`resources/**\/*.bpmn`).
 */
export type GlobList = string[];
/**
 * DMN decision globs. Overrides the convention default (`resources/**\/*.dmn`).
 */
export type GlobList1 = string[];
/**
 * form-js form globs. Overrides the convention default (form files under `resources/`).
 */
export type GlobList2 = string[];
/**
 * A ${VAR} or ${VAR:-default} boot-time substitution reference (ADR 0027 §5). Resolved at App boot / IDE Run, never persisted. The validator checks the reference shape, not the resolved value.
 */
export type EnvTemplate = string;
/**
 * Lowercase kebab-case slug.
 */
export type Slug = string;
/**
 * Maps an event to exactly one engine call (ADR 0025 §1): start a process, or publish a CorrelateMessage.
 */
export type TriggerAction = {
	/**
	 * Process id/name to start; variables seeded by a FEEL expression over the event body.
	 */
	start?: string;
	/**
	 * FEEL over the event body producing the started instance's variables.
	 */
	variables?: string;
	/**
	 * messageName to publish as a CorrelateMessage (correlates to a message-start subscription to start a new instance, or to a running-instance catch to feed a token — ADR 0025 §5).
	 */
	message?: string;
	/**
	 * FEEL over the event body producing the correlationKey (message actions).
	 */
	correlationKey?: string;
} & TriggerAction1;
export type TriggerAction1 = {
	[k: string]: unknown;
};
/**
 * Binds one model — a form, a decision, OR a process — to the domain type in scope for its FEEL (ADR 0029 §5, ADR 0030). Exactly one of form/decision/process.
 */
export type Binding = {
	/**
	 * form-js form id (schema.id) whose default-value FEEL is scoped to `type`.
	 */
	form?: string;
	/**
	 * DMN decision id whose input-expression FEEL is scoped to `type`.
	 */
	decision?: string;
	/**
	 * BPMN process id whose FEEL (component/service-task inputs, conditions) is scoped to `type` — the process as the motion of a typed domain object (ADR 0030).
	 */
	process?: string;
	/**
	 * Lowercase kebab-case slug.
	 */
	type: string;
} & Binding1;
export type Binding1 = {
	[k: string]: unknown;
};
/**
 * A service-task worker: a referenced handler file, an llm binding, or a connector supplied by an installed pack (ADR 0022 §E, ADR 0050).
 */
export type Worker = {
	taskType: string;
	/**
	 * Path to a handler file (language via ADR 0008 packs).
	 */
	handler?: string;
	/**
	 * Name of an llm[] binding used as the worker (LLM-as-worker).
	 */
	llm?: string;
	/**
	 * Id of the installed pack (`nano-ide.ext.json` `id`) that supplies this worker's out-of-process handler, enabled into the project rather than authored in it (ADR 0050 — the outbound I/O edge). The host resolves + supervises the pack's worker `entry` by `taskType`; the pack's element template (its `zeebe:taskDefinition:type` = this `taskType`) is the design-time face. Mutually exclusive with `handler`/`llm`.
	 */
	connector?: string;
	/**
	 * Optional named `connections[]` entry supplying this worker's shared credential/endpoint (ADR 0025 §1), symmetric to `trigger.connection`. Its secrets stay env pointers (ADR 0027 §5).
	 */
	connection?: string;
	/**
	 * Lowercase kebab-case slug.
	 */
	inputType?: string;
	/**
	 * Lowercase kebab-case slug.
	 */
	outputType?: string;
} & Worker1;
export type Worker1 = {
	[k: string]: unknown;
};
export type SecurityMode = "none" | "local" | "oidc";
/**
 * Urban App manifest (nano.app.json) — the declared-data binding of an Urban RAD application (ADR 0027). Owns the envelope + cross-reference rules; each block's detail is owned by its ADR (data=0024, triggers=0025, surfaces=0026, security=0028, workers/llm=0022 §E). This file is the source of truth: TypeScript types are generated from it (scripts/generate-app-manifest.sh) and it doubles as the $schema editors use for nano.app.json autocompletion.
 */
export interface AppManifest {
	/**
	 * Optional editor hint pointing at this schema for autocompletion.
	 */
	$schema?: string;
	/**
	 * Manifest schema version for forward-compat. Currently always 1.
	 */
	schemaVersion: 1;
	/**
	 * Stable App identifier (slug). Required.
	 */
	id: string;
	/**
	 * Human-readable App name. Required.
	 */
	name: string;
	/**
	 * Informational codename, surfaced as App.CODENAME (ADR 0015). Optional.
	 */
	codename?: string;
	/**
	 * App-declared entrypoint module the Studio supervisor runs/compiles, resolved relative to the app root (issue #957). Overrides the default `main.ts` convention so an app whose real entrypoint lives elsewhere (e.g. `src/main.ts`) is self-describing and runnable without a placeholder root shim. Precedence: an explicit project-config `main` (nanobpm.project.json) > this `entrypoint` > the `main.ts` default. Omit to keep the `main.ts` convention (zero regression).
	 */
	entrypoint?: string;
	runtime?: Runtime;
	models?: Models;
	data?: Data;
	/**
	 * The domain type registry (ADR 0029 §4, ADR 0031). Named record types keyed by a stable id — the *nominal* identity every reference resolves against. A type's fields project onto three shapes: form field (face), process variable (motion) and datasource row (rest); the Process-Relational Mapper (ADR 0031) generates the mapping. Types here are the transient/declared source; a datasource table is the other (ADR 0029 §4).
	 */
	types?: {
		[k: string]: DomainType;
	};
	/**
	 * Event sources bound to engine actions (ADR 0025).
	 */
	triggers?: Trigger[];
	/**
	 * Declares the domain type in scope for a model's FEEL (ADR 0029 §5): a form's default-value expressions and a decision's input expressions autocomplete + validate against the bound type's fields. The same 'typed reference replaces a free-string id' move as trigger.bodyType, applied to forms and decisions.
	 */
	bindings?: Binding[];
	/**
	 * Declarative process-instance lifecycle bindings. Each entry names a datasource table whose rows track a process instance by a key column; the runtime polls the engine and, when an instance reaches the TERMINATED state while its row is still marked active, applies the declared `onTerminated` patch. Reconciliation is scoped to TERMINATED only — never COMPLETED, whose terminal row-write is owned by the app's own completion (finalize) worker, so reconciling it here would race that worker and could clobber a legitimately-completed row. This closes rows whose instance ended with no completion worker running — an operator cancel/termination, a crash, or a fire-and-forget row-cancel — so a UI derived from the read model reflects the engine's real state.
	 */
	instanceTracking?: InstanceTracking[];
	/**
	 * Named connections (credentials/endpoint) referenced by triggers/workers by id, so configs carry no inline secrets (ADR 0025 §1).
	 */
	connections?: {
		[k: string]: Connection;
	};
	surfaces?: Surfaces;
	/**
	 * App-authored action handler overrides (ADR 0055 §3): each binds a route to a handler module that wraps the generic pages start/cancel/message actions. Mounted before the generic routes, so an exact override shadows the generic one.
	 */
	actions?: ActionDecl[];
	api?: ApiBinding;
	/**
	 * Service-task handlers: referenced files, an llm binding, or a connector supplied by an installed pack (ADR 0022 §E, ADR 0050).
	 */
	workers?: Worker[];
	/**
	 * Service-task types that are serviced by an EXTERNAL worker (a coding-agent harness, an out-of-process fleet, another deployment) and are deliberately NOT hosted by this app. `urban gen` skips these when scaffolding write-once worker stubs, so they never appear as missing-stub drift or get auto-wired into `workers[]`. Declaring one is an explicit assertion that the app owns the model but not the handler (ADR 0056).
	 */
	externalTaskTypes?: string[];
	/**
	 * Named LLM bindings usable as workers or as a chat surface agent (ADR 0022 §E).
	 */
	llm?: {
		[k: string]: LlmBinding;
	};
	network?: Network;
	security?: Security;
	ui?: AppUi;
}
/**
 * The shipping topology of the compiled App (ADR 0005). Distinct from the IDE dev-loop deployTarget, which lives in nanobpm.project.json (ADR 0027 §1).
 */
export interface Runtime {
	/**
	 * How the App reaches the engine at runtime.
	 */
	engine?: "embedded" | "remote" | "cluster";
	node?: "single" | "cluster";
}
/**
 * OPTIONAL override of the deploy-by-convention layout (ADR 0062). By convention an app keeps its deployables under `resources/` (the deploy root): Urban walks `resources/` and infers content-types by extension, and codegen defaults its BPMN scan to `resources/**\/*.bpmn` (and `.dmn`). Docs live outside `resources/`; the tree is one level deep by convention. Declare `models` only for advanced/non-standard layouts where the convention does not fit — each subkey then overrides the corresponding convention default. Each glob must resolve to at least one file (cross-reference rule, ADR 0027 §4).
 */
export interface Models {
	processes?: GlobList;
	decisions?: GlobList1;
	forms?: GlobList2;
}
/**
 * Named datasources — the BDE-alias abstraction (ADR 0024). Consumers bind by name, never by driver, so the same bundle runs on SQLite in the IDE and Postgres in production by flipping env only.
 */
export interface Data {
	/**
	 * Name of the datasource used when a consumer names none.
	 */
	default?: string;
	sources: {
		[k: string]: DataSource;
	};
}
export interface DataSource {
	/**
	 * Driver id. May be an env template so deployment flips SQLite to Postgres without a source change (ADR 0024 §1).
	 */
	driver: ("sqlite" | "postgres") | EnvTemplate;
	/**
	 * Connection URL, typically an env template (e.g. file:./app.db or ${NANO_APP_DB_URL:-file:./app.db}).
	 */
	url: string;
	/**
	 * Path to a migrations directory.
	 */
	migrations?: string;
}
/**
 * A named domain record type (ADR 0029 §4, ADR 0031). Its map key is the stable id; matching is nominal (by id), consistent with model reference pickers.
 */
export interface DomainType {
	/**
	 * Human-readable label. The map key remains the stable id every reference uses.
	 */
	name?: string;
	/**
	 * Identity discipline. `nominal` (default): references resolve by this type's id. `structural` is a reserved escape hatch (match by field shape) — declared here but not yet honoured by the validator/mapper.
	 */
	match?: "nominal" | "structural";
	/**
	 * Optional datasource table this type binds to as its rest projection (ADR 0031 rest bank). Absent = transient / non-persisted (ADR 0029 §4.2). Table existence is validated once the datasource schema() runtime (ADR 0024) lands; the shape is checked now.
	 */
	table?: string;
	/**
	 * Field name → field definition. Field names are the keys the form field, the variable path and the datasource column share (ADR 0029 §4).
	 */
	fields: {
		[k: string]: DomainField;
	};
}
/**
 * A single field of a domain type.
 */
export interface DomainField {
	/**
	 * A primitive type, or the id of another domain type in the registry (nominal reference). Primitive ids take precedence over an identically named type.
	 */
	type: ("string" | "number" | "integer" | "boolean" | "date" | "datetime" | "json") | Slug;
	/**
	 * Whether the field may be absent.
	 */
	optional?: boolean;
	/**
	 * Whether the field is a list of `type` rather than a single value.
	 */
	list?: boolean;
}
export interface Trigger {
	id: Slug;
	/**
	 * Source kind. Core (in-binary): cron | webhook | file. Pack sources add imap, mqtt, cloud, … (ADR 0025 §1).
	 */
	type: string;
	/**
	 * cron: the crontab spec (e.g. '0 6 * * *'). 5 fields, evaluated in UTC (ADR 0025 §2).
	 */
	spec?: string;
	/**
	 * cron catch-up policy for fires missed while the App was down (ADR 0025 §Open questions): skip them, fire once for the whole span, or enqueue every missed instant (dedup keys keep it idempotent).
	 */
	onMissed?: "skip" | "once" | "all";
	/**
	 * Source-kind-specific settings. Core: file may set { pollMs }. Pack sources (nano-ide-trigger-*, ADR 0025 §6) read their declared config fields from here.
	 */
	config?: {
		[k: string]: unknown;
	};
	/**
	 * webhook: the HTTP path served on the App backend (e.g. /hooks/temp).
	 */
	path?: string;
	/**
	 * Name of a connections[] entry supplying this source's credentials (e.g. imap mailbox).
	 */
	connection?: string;
	/**
	 * Inbound auth policy for a webhook, e.g. 'hmac:sensors' referencing a connection (ADR 0025).
	 */
	auth?: string;
	/**
	 * Lowercase kebab-case slug.
	 */
	bodyType?: string;
	action: TriggerAction;
}
/**
 * One process-instance lifecycle binding: reconcile rows of `table` when their tracked instance reaches the TERMINATED engine state (never COMPLETED — see the top-level `instanceTracking` description).
 */
export interface InstanceTracking {
	/**
	 * Datasource table whose rows each track one process instance.
	 */
	table: string;
	/**
	 * Column holding the tracked process instance key.
	 */
	keyField: string;
	/**
	 * Column governing whether a row is still active. Combined with `activeStatuses` (allow-list) or `terminalStatuses` (exclusion list) to select the rows worth polling; when omitted, every row of `table` is polled.
	 */
	statusField?: string;
	/**
	 * Values of `statusField` considered still-open. Only rows in one of these states are polled; a row already in a terminal status is skipped. FAIL-CLOSED: a non-terminal status omitted here is silently dropped from reconciliation — prefer `terminalStatuses` for a fail-open selector. When neither is set, every row is polled (use with care on large tables). Requires `statusField`; mutually exclusive with `terminalStatuses`.
	 *
	 * @minItems 1
	 */
	activeStatuses?: [
		string,
		...string[]
	];
	/**
	 * Values of `statusField` considered FINISHED. Fail-open alternative to `activeStatuses`: every row whose `statusField` is NOT one of these is polled, so a newly-added non-terminal status is reconciled by default instead of being silently dropped. Prefer this and mirror the app's terminal-status enum. Requires `statusField`; mutually exclusive with `activeStatuses`.
	 *
	 * @minItems 1
	 */
	terminalStatuses?: [
		string,
		...string[]
	];
	/**
	 * The reconciliation applied to a row whose instance has reached the TERMINATED engine state.
	 */
	onTerminated: {
		/**
		 * Column → literal value patch written to the row (e.g. set the status to an abandoned/terminal value and clear open-task pointers).
		 */
		set: {
			[k: string]: string | number | boolean | null;
		};
	};
	/**
	 * Poll interval in milliseconds. Default 15000.
	 */
	pollMs?: number;
}
/**
 * A named connection (credentials/endpoint). Shape is source-specific; secrets should be env templates, never inline literals (ADR 0025 §1).
 */
export interface Connection {
	/**
	 * Connection kind (e.g. imap, mqtt, hmac).
	 */
	type: string;
}
/**
 * Batteries-included human surfaces generated from the manifest (ADR 0026).
 */
export interface Surfaces {
	taskInbox?: TaskInboxSurface;
	chat?: ChatSurface;
	pages?: PagesSurface;
}
/**
 * Generic task inbox: lists open user tasks and renders their .form to claim/complete (ADR 0026).
 */
export interface TaskInboxSurface {
	enabled?: boolean;
	path?: string;
}
/**
 * Conversational surface whose LLM agent drives the action API via its tools (ADR 0026).
 */
export interface ChatSurface {
	enabled?: boolean;
	path?: string;
	/**
	 * Name of an llm[] binding backing this chat (cross-reference rule, ADR 0027 §4).
	 */
	agent?: string;
}
/**
 * Schema-driven page runtime (ADR 0042): serves pages/<homePage>.page.json at / and the generic /app/actions + /app/data routes over the named datasource, with no hand-written frontend.
 */
export interface PagesSurface {
	enabled?: boolean;
	/**
	 * Directory of *.page.json composed pages, relative to the app root.
	 */
	pagesDir?: string;
	/**
	 * Id of the page served at / (loaded as <pagesDir>/<homePage>.page.json).
	 */
	homePage?: string;
	/**
	 * Maximum rows a dataGrid fetch returns.
	 */
	rowLimit?: number;
	/**
	 * Name of the data[] source the page runtime reads (cross-reference rule, ADR 0027 §4).
	 */
	sourceName?: string;
	/**
	 * How every dataGrid lays out (nanobpm/nano-ide#572). "auto": a table, flipping to a stacked card list below the mobile breakpoint. "cards": the card list at every width, so wide grids are never clipped by a fixed-max-width shell.
	 */
	gridLayout?: "auto" | "cards";
}
/**
 * An app-authored action handler override (ADR 0055 §3): binds a route to a handler module that default-exports an ActionHandler.
 */
export interface ActionDecl {
	/**
	 * Route path to serve, e.g. "/app/actions/cancel" or "/app/actions/start/convergence-loop".
	 */
	path: string;
	/**
	 * Handler module path relative to the app root; default-exports an ActionHandler (or a named `handler`).
	 */
	module: string;
	/**
	 * HTTP method to match.
	 */
	method?: string;
	/**
	 * Match `path` as a prefix rather than exactly.
	 */
	prefix?: boolean;
}
/**
 * The OpenAPI endpoint surface (ADR 0058): a contract-first API where the toolkit derives the controller layer (typed request/response contracts + runtime validators + route table) from an OpenAPI document and the author writes only the delegated implementation per `operationId`. Coexists with `actions[]` (both mount together, first-match-wins) and is ejectable — `eject` (whole surface) or an `x-urban-eject: true` vendor extension on an operation skips generated validation and hands the delegate the raw request. Every operation MUST carry a unique `operationId` (the delegate module key + type stem); `urban check` fails closed otherwise.
 */
export interface ApiBinding {
	/**
	 * OpenAPI 3.x document, app-root-relative (e.g. "openapi.json"). JSON is supported first; YAML is a fast-follow (ADR 0058 open questions).
	 */
	spec: string;
	/**
	 * Directory (app-root-relative) holding the per-`operationId` delegate modules; each default-exports an operation handler (or a named `handler`).
	 */
	dir?: string;
	/**
	 * Route prefix the derived operation paths mount under.
	 */
	base?: string;
	/**
	 * When to run the derived response validators: "dev" (IDE/Run only), "always", or "never". Response validation is off in production by default for perf.
	 */
	validateResponses?: "dev" | "always" | "never";
	/**
	 * Opt the whole surface out of generated request validation: routes + docs are still mounted, but every delegate receives the raw request. Per-operation opt-out uses the `x-urban-eject: true` OpenAPI vendor extension instead.
	 */
	eject?: boolean;
}
export interface LlmBinding {
	/**
	 * LLM provider selector (e.g. 'env' to resolve from environment).
	 */
	provider: string;
	/**
	 * Model id, typically an env template (e.g. ${NANO_APP_LLM_MODEL}).
	 */
	model: string;
	/**
	 * Constrains the model's structured output (e.g. to a DMN decision).
	 */
	output?: {
		/**
		 * DMN decision id constraining the output shape.
		 */
		decision?: string;
	};
	/**
	 * Action-API tools the agent may call (e.g. start-process, complete-task, query-data).
	 */
	tools?: string[];
}
/**
 * App-level network settings (nanobpm/nano-ide#235). The loopback default applies when this block is absent, when it is `{}`, or when `bind` is omitted.
 */
export interface Network {
	/**
	 * Which interface the app's embedded HTTP server binds to. Default "loopback" (127.0.0.1 — secure by default, refuses off-box connections); "all" (0.0.0.0) exposes it on the LAN for a distributed worker fleet. The URBAN_BIND env var overrides this at runtime.
	 */
	bind?: "loopback" | "all";
}
/**
 * App-user auth/identity/authorization policy (ADR 0028). Default (block absent) is single-user, unsecured. Secrets are env templates resolved at boot, never persisted.
 */
export interface Security {
	/**
	 * Enabled auth tier(s): none (default), local (username/password), oidc (social). A list combines them.
	 */
	mode?: SecurityMode | SecurityMode[];
	providers?: SecurityProvider[];
	/**
	 * Role names; the maker may add domain roles beyond admin/user.
	 */
	roles?: string[];
	/**
	 * Role-based authorization: which roles may reach each action/surface/datasource (ADR 0028).
	 */
	rules?: {
		actions?: RoleMap;
		surfaces?: RoleMap;
		data?: {
			[k: string]: {
				[k: string]: string[];
			};
		};
	};
}
export interface SecurityProvider {
	id: string;
	/**
	 * oidc: social/generic OIDC (Authorization Code + PKCE). password: local username/password.
	 */
	type: "oidc" | "password";
	/**
	 * OIDC preset shorthand (e.g. google, github, auth0).
	 */
	preset?: string;
	clientId?: string;
	clientSecret?: string;
	/**
	 * password providers: self sign-up policy.
	 */
	signup?: "open" | "invite" | "closed";
}
/**
 * Maps a pattern (e.g. 'start/*') to the list of roles permitted.
 */
export interface RoleMap {
	[k: string]: string[];
}
/**
 * Console-integrated app view (ADR 0057, issue #638). When the app runs under the studio supervisor it appears in the left rail as a running app; a UI app embeds its own webview in the right pane, a headless app shows a control (status/logs/stop) pane. Omitting this block still lists the app (headless).
 */
export interface AppUi {
	/**
	 * Opt in to an embedded UI. false ⇒ headless (control-only), but the app is still listed in the running-apps rail.
	 */
	enabled?: boolean;
	/**
	 * The integrated-UI port. Takes precedence over portEnv. Apps that expose multiple ports use this to name which one is the UI.
	 */
	port?: number;
	/**
	 * Name of the env var the app reads its UI port from (e.g. "PORT"). The studio resolves it from the project run config so it can discover the port without allocating one.
	 */
	portEnv?: string;
	/**
	 * Path the embedded webview opens (default "/").
	 */
	path?: string;
	/**
	 * Left-rail icon: either a bundled glyph name (e.g. "workers") resolved by the console, or a project-relative asset path the app ships itself (e.g. "assets/icon.svg"), served path-guarded and image-only from /console/app-view-icon/<project>. Display hint only; the console falls back to a default glyph when absent/invalid/unresolvable.
	 */
	icon?: string;
	/**
	 * Left-rail label hint. Display only; the console disambiguates on the project name, since same-template apps share a manifest.
	 */
	label?: string;
}

export {};
