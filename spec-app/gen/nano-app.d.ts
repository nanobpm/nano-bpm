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
  activeStatuses?: [string, ...string[]];
  /**
   * Values of `statusField` considered FINISHED. Fail-open alternative to `activeStatuses`: every row whose `statusField` is NOT one of these is polled, so a newly-added non-terminal status is reconciled by default instead of being silently dropped. Prefer this and mirror the app's terminal-status enum. Requires `statusField`; mutually exclusive with `activeStatuses`.
   *
   * @minItems 1
   */
  terminalStatuses?: [string, ...string[]];
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
