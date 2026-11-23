import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useRef,
  useState,
} from "react";
import Modeler from "bpmn-js/lib/Modeler";
import BaseRenderer from "diagram-js/lib/draw/BaseRenderer";
import {
  append as svgAppend,
  create as svgCreate,
  attr as svgAttr,
} from "tiny-svg";
import {
  BpmnPropertiesPanelModule,
  BpmnPropertiesProviderModule,
  ZeebePropertiesProviderModule,
  useService,
} from "bpmn-js-properties-panel";
import {
  SelectEntry,
  TextFieldEntry,
  TextAreaEntry,
  ToggleSwitchEntry,
  Group,
  isSelectEntryEdited,
  isTextFieldEntryEdited,
  isTextAreaEntryEdited,
  isToggleSwitchEntryEdited,
} from "@bpmn-io/properties-panel";
import { zeebeModdleWithAgent } from "../moddle/zeebeAgent";
import { nanoShapesModdle } from "../moddle/nanoShapes";
import {
  CloudElementTemplatesCoreModule,
  CloudElementTemplatesPropertiesProviderModule,
  type ElementTemplatesService,
} from "bpmn-js-element-templates";
import CloudBehaviorsModule from "camunda-bpmn-js-behaviors/lib/camunda-cloud";
import { getVariablesForElement as extractZeebeVariables } from "@bpmn-io/extract-process-variables/zeebe";
import type { ElementTemplate } from "../lib/urbanComponents";
import type { FeelVariable } from "../lib/feelVariables";
import type { ComponentOutput } from "../lib/bpmnDomainVariables";
import {
  SERVICE_TASK_TYPES,
  CREATE_ENVELOPE,
  EDIT_ENVELOPE,
  envelopeContext,
  readEnvelope,
  writeEnvelope,
  collectEnvelopeTypeRefs,
  type EnvelopeField,
} from "../lib/dataEnvelope";
import {
  readMeta,
  readShapes,
  writeMeta,
  writeShapes,
  envelopeEditableFields,
  type MetaEntry,
  type ShapeDecl,
  type ShapeModdle,
  type ShapeModdleElement,
  type ShapeModeling,
} from "../lib/shapeCarrier";
import {
  PROMPT_BINDING_TYPES,
  PROMPT_DEFAULT_BINDING_TYPE,
  PROMPT_LINK_NAME,
  PROMPT_RESOURCE_TYPE,
  AGENT_TASK_ELEMENT_TYPE,
  AGENT_TYPE_EXTERNAL,
  AUTO_SUBSCRIBE_PROPERTY,
  isAgentTask,
  hasPromptBinding,
  hasExternalAgentMarker,
  readAutoSubscribeOptOut,
  readPromptBinding,
  writePromptLink,
  writeAppendPrompt,
  removePromptBinding,
  writeExternalAgentMarker,
  removeExternalAgentMarker,
  writeAutoSubscribeOptOut,
  type AgentModdle,
  type AgentModdleElement,
  type AgentModeling,
} from "../lib/agentTask";
import { DocumentLoadGate, type LoadAttempt } from "../lib/documentLoadGate";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";
import "bpmn-js/dist/assets/bpmn-js.css";
import "@bpmn-io/properties-panel/dist/assets/properties-panel.css";

interface RootElement {
  id: string;
  businessObject: { id: string; name?: string };
}
interface Canvas {
  zoom(mode: string): void;
  getRootElement(): RootElement;
}
interface Modeling {
  updateProperties(element: unknown, props: Record<string, unknown>): void;
  updateModdleProperties(
    element: unknown,
    moddleElement: unknown,
    props: Record<string, unknown>,
  ): void;
}
interface Moddle {
  create(type: string, attrs?: Record<string, unknown>): ModdleElement;
}

// --- composed motion-shapes (ADR 0040 §9) -----------------------------------
// Shapes live in a `nano:shapes` container on a `bpmn:process`'s extension
// elements; the composer edits the *primary* process's set, and the Data envelope
// pickers offer shapes from *any* process (all shapes fold into one global
// registry). These helpers locate the process business object(s) in the model.

/** The local (namespace-stripped) name of a moddle `$type`. */
const shapeLocalType = (t: string | undefined): string =>
  (t ?? "").split(":").pop() ?? "";

interface DefinitionsBo extends ShapeModdleElement {
  rootElements?: ShapeModdleElement[];
}

/** Every `bpmn:process` business object in the open definitions. */
function allProcessBos(modeler: Modeler): ShapeModdleElement[] {
  try {
    const rootBo = modeler.get<Canvas>("canvas").getRootElement()
      .businessObject as unknown as ShapeModdleElement;
    const defs = (
      shapeLocalType(rootBo?.$type) === "Definitions"
        ? rootBo
        : (rootBo?.$parent as DefinitionsBo | undefined)
    ) as DefinitionsBo | undefined;
    return (defs?.rootElements ?? []).filter(
      (e) => shapeLocalType(e?.$type) === "Process",
    );
  } catch {
    return [];
  }
}

/** The process the composer edits (the root process, or the first participant's
 * process in a collaboration) plus the diagram element the undoable command hangs
 * on. Null when no process is loaded yet. */
function primaryProcess(
  modeler: Modeler,
): { element: unknown; processBo: ShapeModdleElement } | null {
  try {
    const rootEl = modeler.get<Canvas>("canvas").getRootElement();
    const rootBo = rootEl.businessObject as unknown as ShapeModdleElement;
    if (shapeLocalType(rootBo?.$type) === "Process")
      return { element: rootEl, processBo: rootBo };
    const procs = allProcessBos(modeler);
    return procs.length ? { element: rootEl, processBo: procs[0] } : null;
  } catch {
    return null;
  }
}

// --- Component output extraction (ADR 0033 §3) -------------------------------
// Walks the diagram's service tasks to pair each output-mapped process variable
// with the `taskType` of the component that writes it. The schema package joins
// these `{ taskType, target }` pairs against `workers[].outputType` to type the
// variables, so a task placed after a component autocompletes on its result's
// fields — component output → typed process variable → next component input.
interface ModdleElement {
  $type?: string;
  type?: string;
  target?: string;
  id?: string;
  name?: string;
  value?: string;
  values?: ModdleElement[];
  properties?: ModdleElement[];
  outputParameters?: ModdleElement[];
  eventDefinitions?: ModdleElement[];
  messageRef?: ModdleElement;
  extensionElements?: ModdleElement;
  $parent?: unknown;
}
interface RegistryElement {
  type?: string;
  businessObject?: ModdleElement;
}
interface ElementRegistry {
  getAll(): RegistryElement[];
}

function collectComponentOutputs(registry: ElementRegistry): ComponentOutput[] {
  const outputs: ComponentOutput[] = [];
  for (const el of registry.getAll()) {
    if (!el.type || !SERVICE_TASK_TYPES.has(el.type)) continue;
    const ext = el.businessObject?.extensionElements?.values ?? [];
    const taskDef = ext.find((v) => v.$type === "zeebe:TaskDefinition");
    const taskType =
      typeof taskDef?.type === "string" ? taskDef.type : undefined;
    if (!taskType) continue;
    const io = ext.find((v) => v.$type === "zeebe:IoMapping");
    for (const p of io?.outputParameters ?? []) {
      if (typeof p.target === "string" && p.target)
        outputs.push({ taskType, target: p.target });
    }
  }
  return outputs;
}

// --- Agent-task rendering (issue #950) ---------------------------------------
// An agent task is a service task carrying a `linkName="prompt"` linked resource
// (the `@nanobpm/agentic` signal — see `lib/agentTask.ts`, the single source of
// truth for the shape). A plain `bpmn-js` service task renders it identically to
// any other, so this custom renderer decorates prompt-bearing service tasks with
// a distinguishing badge plus a caption of the bound `resourceId` / `bindingType`
// — making the binding a first-class, visible citizen on the canvas. It carries
// DI (the model already has it; the renderer only augments the shape graphics).
const AGENT_BADGE_FILL = "#6d28d9";
const AGENT_CAPTION_FILL = "#6d28d9";
// A distinct amber for the opt-out badge so a "manual subscription only" task
// reads differently from a plain agent task at a glance.
const AGENT_OPTOUT_FILL = "#b45309";

interface BpmnShapeRenderer {
  drawShape(parentNode: SVGElement, element: DiagramElement): SVGElement;
  getShapePath(shape: DiagramElement): string;
}
interface DiagramElement {
  type?: string;
  width?: number;
  height?: number;
  businessObject?: AgentModdleElement;
  labelTarget?: unknown;
}

// A high render priority so this decorator wins over the default BpmnRenderer for
// agent tasks; the default rendering is still produced (delegated to) and then
// augmented, so the task shell can never drift from bpmn-js.
const AGENT_RENDER_PRIORITY = 1500;

class AgentTaskRenderer extends BaseRenderer {
  // Explicit annotation so didi injection survives Vite minification.
  static $inject = ["eventBus", "bpmnRenderer"];
  private readonly bpmnRenderer: BpmnShapeRenderer;
  constructor(eventBus: unknown, bpmnRenderer: BpmnShapeRenderer) {
    super(
      eventBus as ConstructorParameters<typeof BaseRenderer>[0],
      AGENT_RENDER_PRIORITY,
    );
    this.bpmnRenderer = bpmnRenderer;
  }
  canRender(element: DiagramElement): boolean {
    // Only the task shape itself, never its external label.
    return !element.labelTarget && isAgentTask(element);
  }
  drawShape(parentNode: SVGElement, element: DiagramElement): SVGElement {
    const shape = this.bpmnRenderer.drawShape(parentNode, element);
    const width = typeof element.width === "number" ? element.width : 100;
    const height = typeof element.height === "number" ? element.height : 80;
    const binding = readPromptBinding(element.businessObject);

    // Corner badge (top-right) marking the task as agentic.
    const badgeW = 40;
    const badgeH = 15;
    const badgeX = width - badgeW - 4;
    const badge = svgCreate("rect");
    svgAttr(badge, {
      x: badgeX,
      y: 4,
      width: badgeW,
      height: badgeH,
      rx: 3,
      ry: 3,
      fill: AGENT_BADGE_FILL,
    });
    svgAppend(parentNode, badge);
    const badgeText = svgCreate("text");
    svgAttr(badgeText, {
      x: badgeX + badgeW / 2,
      y: 4 + badgeH / 2,
      "text-anchor": "middle",
      "dominant-baseline": "central",
      "font-size": "9px",
      "font-family": "Arial, sans-serif",
      "font-weight": "bold",
      fill: "#ffffff",
    });
    badgeText.textContent = "AGENT";
    svgAppend(parentNode, badgeText);

    // Second badge (below the AGENT badge) when the task opts OUT of the
    // harness `--auto` subscription set (`autoSubscribe="false"`) — so a
    // manual-subscription-only agent task is distinguishable on the canvas.
    // Gated on the external marker (not merely the opt-out property): the
    // `--auto` contract is marker-only, so a prompt-only task carrying a stray
    // `autoSubscribe="false"` is not in the `--auto` set at all and must not be
    // badged as opting out of it (mirrors the panel provider's opt-out gating).
    if (
      hasExternalAgentMarker(element.businessObject) &&
      readAutoSubscribeOptOut(element.businessObject)
    ) {
      const optW = 56;
      const optH = 15;
      const optX = width - optW - 4;
      const optY = 4 + badgeH + 3;
      const optBadge = svgCreate("rect");
      svgAttr(optBadge, {
        x: optX,
        y: optY,
        width: optW,
        height: optH,
        rx: 3,
        ry: 3,
        fill: AGENT_OPTOUT_FILL,
      });
      svgAppend(parentNode, optBadge);
      const optText = svgCreate("text");
      svgAttr(optText, {
        x: optX + optW / 2,
        y: optY + optH / 2,
        "text-anchor": "middle",
        "dominant-baseline": "central",
        "font-size": "9px",
        "font-family": "Arial, sans-serif",
        "font-weight": "bold",
        fill: "#ffffff",
      });
      optText.textContent = "NO --auto";
      svgAppend(parentNode, optText);
    }

    // Caption below the shape: the bound prompt resource + binding type (or the
    // external-agent marker when there is no prompt link), so the agentic shape
    // is inspectable at a glance without opening the properties panel.
    const caption = svgCreate("text");
    svgAttr(caption, {
      x: width / 2,
      y: height + 14,
      "text-anchor": "middle",
      "font-size": "11px",
      "font-family": "Arial, sans-serif",
      fill: AGENT_CAPTION_FILL,
    });
    if (binding) {
      const resource = binding.resourceId ? binding.resourceId : "(no prompt)";
      caption.textContent = `${PROMPT_LINK_NAME}: ${resource} · ${
        binding.bindingType ?? PROMPT_DEFAULT_BINDING_TYPE
      }`;
    } else {
      // Marker-only agent task (external marker, no prompt link).
      caption.textContent = `agent: ${AGENT_TYPE_EXTERNAL}`;
    }
    svgAppend(parentNode, caption);

    return shape;
  }
  getShapePath(shape: DiagramElement): string {
    return this.bpmnRenderer.getShapePath(shape);
  }
}

const agentTaskRendererModule = {
  __init__: ["agentTaskRenderer"],
  agentTaskRenderer: ["type", AgentTaskRenderer],
};

// --- Urban components palette (ADR 0033) -------------------------------------
// A diagram-js palette provider that adds an entry per installed Urban component
// (an element template). Dragging one onto the canvas stamps a service task
// pre-bound to the template's task type + input/output mappings — the "drag a
// Delphi component off the palette" loop. Runtime behaviour is the matching
// `workers[].taskType` (ADR 0022); the properties panel is the Object Inspector.
// The component set comes from the open project (ADR 0033 increment 2,
// `loadProjectComponents`), read live through a ref so a newly-added component
// file surfaces without recreating the modeler; the provider class is defined
// inside the mount effect so it can close over that ref.
interface PaletteService {
  registerProvider(provider: unknown): void;
  /** diagram-js internal palette re-render; re-invokes every provider's
   *  `getPaletteEntries`. Used to refresh entries when the component set loads
   *  after mount. Optional/guarded — absence just means no live refresh. */
  _update?(): void;
}
interface CreateService {
  start(event: Event, shape: unknown): void;
}

/// Imperative handle the Modeler view drives. Keeps the live bpmn document inside
/// this component and exposes just the operations the toolbar needs.
export interface BpmnModelerHandle {
  /// Serializes the current document to formatted BPMN XML.
  getXml(): Promise<string>;
  /// Replaces the document with `xml` (used to open a model or pull a deployed one).
  importXml(xml: string): Promise<void>;
  /// Loads a blank diagram (New).
  createBlank(): Promise<void>;
  /// The id of the root process element, or null when none is loaded.
  getProcessId(): string | null;
  /// Renames the root process element's id (the BPMN deploy identity).
  setProcessId(id: string): void;
  /// The composed motion-shapes on the primary process (ADR 0040 §9), or `[]`
  /// when none are declared or no process is loaded yet.
  getShapes(): ShapeDecl[];
  /// Replaces the primary process's composed shapes as one undoable command;
  /// no-ops when no process is loaded.
  setShapes(shapes: ShapeDecl[]): void;
  /// The model-level metadata (`nano:meta`) on the primary process (ADR 0040 §5),
  /// or `[]` when none are declared or no process is loaded yet.
  getMeta(): MetaEntry[];
  /// Replaces the primary process's model-level metadata as one undoable command;
  /// no-ops when no process is loaded.
  setMeta(meta: MetaEntry[]): void;
}

interface BpmnModelerProps {
  /// Called whenever the document changes (after the first import/createDiagram),
  /// so the parent can drive dirty state. The initial load does not mark dirty.
  onChange?: () => void;
  /// Called once after the initial blank diagram has loaded, so the parent can
  /// read the starting process id.
  onReady?: () => void;
  /// Supplies the domain variables in scope for the open diagram's process,
  /// given the component outputs the modeler extracted. Combines the process's
  /// bound-type `body` scope (ADR 0030) with the component-output scope (ADR
  /// 0033 §3: each output-mapped variable typed by its worker's `outputType`),
  /// so component-input / gateway FEEL autocomplete offers both. Empty/absent →
  /// only bpmn-js's extracted process variables are offered. Read live, so
  /// binding/output edits reflect without a remount.
  getVariables?: (ctx: { taskOutputs: ComponentOutput[] }) => FeelVariable[];
  /// The component element templates installed for the open project (ADR 0033
  /// increment 2, loaded by `loadProjectComponents`). Feeds both the palette
  /// (one draggable entry per component) and the template chooser. Read live, so
  /// adding a component file refreshes the palette without a remount. Empty/absent
  /// → an empty component palette (nothing installed).
  components?: ElementTemplate[];
  /// The manifest domain-type binding for the **Data envelope** group (ADR 0033
  /// §6). When `enabled` (the open project is an Urban App with a `nano.app.json`),
  /// the properties panel shows a Data envelope group on every element with a
  /// typed data boundary — service-ish tasks, user tasks, and message-bearing
  /// elements — letting the modeler pick the input/output domain type. The
  /// reference is written into the model; `typeIds` are the declared registry
  /// type ids (the dropdown options), `createType` declares a new one, and `set`
  /// projects a service task's choice onto its `workers[]` entry so `defineWorker`
  /// stays typed. Read live via a ref, so edits reflect without recreating the
  /// modeler. Absent/`enabled:false` → no Data envelope group (non-App projects).
  domainTypeBinding?: DomainTypeBinding;
}

/// The manifest domain-type binding surfaced in the BPMN properties panel's
/// **Data envelope** group (ADR 0033 §6). The envelope reference itself lives in
/// the model (a reserved `zeebe:property`); this binding supplies the picker's
/// options (`typeIds`), the create-a-new-type action (`createType`), and the
/// service-task projection (`set`) that keeps the manifest `workers[]` entry —
/// and thus `defineWorker` typing — in sync with the model.
export interface DomainTypeBinding {
  /// Whether the Data envelope group is offered (true only for Urban App projects).
  enabled: boolean;
  /// Declared registry type ids (manifest `types`), the dropdown options.
  typeIds: string[];
  /// Projects a service task's chosen envelope onto its `workers[]` entry
  /// (`inputType`/`outputType`, keyed by task type), creating it if absent and
  /// clearing on "". A cache for the reifier until server-side derivation lands.
  set(taskType: string, field: "inputType" | "outputType", value: string): void;
  /// Opens the envelope field-authoring surface so a maker can declare a new
  /// (transient) domain type — an id plus its fields — in the manifest `types`
  /// registry, then pick it in one gesture (the "Create new envelope…"
  /// affordance). Resolves with the new type id, or `undefined` if the maker
  /// cancelled. Absent → no create option.
  createType?(): Promise<string | undefined>;
  /// Opens the field-authoring surface pre-filled for an existing model-shape
  /// type (`nano:shape`) so a maker can edit its fields in place — the "Edit
  /// fields…" affordance offered on the envelope picker when the selected type is
  /// an editable model shape (a flat scalar field list). Resolves with the type
  /// id once saved, or `undefined` if the maker cancelled or the type isn't
  /// editable through the flat editor (composition/list shapes stay in the shape
  /// composer). Absent → no edit option.
  editType?(id: string): Promise<string | undefined>;
  /// The declared domain type bound to a form in the manifest `bindings[]`
  /// (ADR 0029 §5), or undefined. A user task whose linked form is typed defaults
  /// its envelope to this (ADR 0033 §6) — the form binding stays the single source
  /// of truth unless the maker sets an explicit override on the task in the model.
  formType?(formId: string): string | undefined;
}

const BpmnModeler = forwardRef<BpmnModelerHandle, BpmnModelerProps>(
  function BpmnModeler(
    { onChange, onReady, getVariables, components, domainTypeBinding },
    ref,
  ) {
    const containerRef = useRef<HTMLDivElement>(null);
    const panelRef = useRef<HTMLDivElement>(null);
    const modelerRef = useRef<Modeler | null>(null);
    // Collapsible properties panel. Persist the collapsed/expanded choice in
    // localStorage — same convention as `nano.railCollapsed` in App.tsx — so it
    // survives reloads. The panel div stays mounted while collapsed (bpmn-js
    // keeps its `propertiesPanel.parent`); only its width is clipped to 0.
    const [panelCollapsed, setPanelCollapsed] = useState<boolean>(
      () => localStorage.getItem("nano.bpmnPropertiesCollapsed") === "1",
    );
    const togglePanel = useCallback(() => {
      setPanelCollapsed((prev) => {
        const next = !prev;
        localStorage.setItem("nano.bpmnPropertiesCollapsed", next ? "1" : "0");
        return next;
      });
    }, []);
    // Set once the modeler has been destroyed, so async work that was already
    // in flight (an import, the initial createDiagram) doesn't touch a dead
    // instance — diagram-js throws "reading 'root-0'" when operated on after
    // destroy or while a load is mid-flight.
    const disposedRef = useRef(false);
    // Serializes document loads. importXML/createDiagram must never overlap;
    // every load is chained after the previous one so they run one at a time.
    const opChainRef = useRef<Promise<unknown>>(Promise.resolve());
    // Makes re-delivering the document the modeler already holds a no-op
    // (issue #1048): a duplicate import would wipe the user's selection.
    const loadGateRef = useRef(new DocumentLoadGate<Modeler>());
    // Loads queued but not yet settled; mirrored onto the canvas container as
    // `data-load-state` ("loading" | "idle") so callers (and e2e tests) can wait
    // on the real readiness signal instead of a wall-clock budget.
    const pendingLoadsRef = useRef(0);
    const publishLoadState = () => {
      containerRef.current?.setAttribute(
        "data-load-state",
        pendingLoadsRef.current > 0 ? "loading" : "idle",
      );
    };
    // Suppress the change callback for programmatic loads (import/createDiagram),
    // so opening a model doesn't immediately look dirty.
    const suppressChange = useRef(false);
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    const onReadyRef = useRef(onReady);
    onReadyRef.current = onReady;
    const getVariablesRef = useRef(getVariables);
    getVariablesRef.current = getVariables;
    // The installed component set, read live by the palette provider and the
    // template-sync effect so a newly-loaded/edited component surfaces without
    // recreating the modeler.
    const componentsRef = useRef<ElementTemplate[]>(components ?? []);
    componentsRef.current = components ?? [];
    // The manifest domain-type binding, read live by the domain-type properties
    // provider so type edits + manifest reloads reflect without recreating the
    // modeler.
    const domainTypeBindingRef = useRef<DomainTypeBinding | undefined>(
      domainTypeBinding,
    );
    domainTypeBindingRef.current = domainTypeBinding;

    // Queues a load on a single chain so imports can't race each other or the
    // initial blank diagram. Each op captures the modeler instance it was
    // enqueued for and bails if that instance was destroyed or swapped out
    // (e.g. StrictMode's mount→unmount→mount) before it ran. Returns a promise
    // that rejects on a genuine load failure (e.g. invalid XML) so callers can
    // surface it, while the internal chain keeps going regardless.
    const runLoad = (
      loader: (m: Modeler) => Promise<unknown>,
      after?: () => void,
    ): Promise<void> => {
      const modeler = modelerRef.current;
      if (!modeler) return Promise.resolve();
      pendingLoadsRef.current += 1;
      publishLoadState();
      const run = opChainRef.current.then(async () => {
        if (disposedRef.current || modelerRef.current !== modeler) return;
        suppressChange.current = true;
        try {
          await loader(modeler);
          if (disposedRef.current || modelerRef.current !== modeler) return;
          modeler.get<Canvas>("canvas").zoom("fit-viewport");
          after?.();
        } finally {
          suppressChange.current = false;
        }
      });
      void run
        .catch(() => {})
        .finally(() => {
          pendingLoadsRef.current -= 1;
          publishLoadState();
        });
      // Keep the chain alive even when this op fails, so one bad import doesn't
      // wedge every later load.
      opChainRef.current = run.catch(() => {});
      return run;
    };

    useEffect(() => {
      if (!containerRef.current || !panelRef.current) return;
      disposedRef.current = false;
      // A `variableResolver` service the bpmn-js FEEL fields consult
      // (`useServiceIfAvailable('variableResolver', …)` in properties-panel and
      // element-templates). We keep bpmn-js's own extracted process variables
      // (output mappings written upstream) and append the domain scope: the type
      // bound to this diagram's process (ADR 0030, `body`) plus each component
      // output typed by its worker's `outputType` (ADR 0033 §3). Typed domain
      // variables override the plain extracted ones of the same name so their
      // fields autocomplete. Read live via the ref so binding/output edits
      // reflect without recreating the modeler.
      class DomainVariableResolver {
        static $inject = ["elementRegistry"];
        private readonly elementRegistry: ElementRegistry;
        constructor(elementRegistry: ElementRegistry) {
          this.elementRegistry = elementRegistry;
        }
        // `@bpmn-io/extract-process-variables` >=2 made zeebe variable
        // extraction async, and bpmn-js-properties-panel awaits this method
        // (`await variableResolver.getVariablesForElement(...)`), so we await the
        // extraction rather than iterating its Promise (which threw "not
        // iterable" and crashed the whole panel render on every selection).
        // Both sources are coerced to arrays so a future signature change can
        // never wedge the panel again.
        async getVariablesForElement(bo: unknown): Promise<FeelVariable[]> {
          let base: FeelVariable[] = [];
          try {
            const extracted = await extractZeebeVariables(bo);
            base = Array.isArray(extracted)
              ? (extracted as FeelVariable[])
              : [];
          } catch {
            base = [];
          }
          let taskOutputs: ComponentOutput[] = [];
          try {
            taskOutputs = collectComponentOutputs(this.elementRegistry);
          } catch {
            taskOutputs = [];
          }
          const resolved = getVariablesRef.current?.({ taskOutputs });
          const domain = Array.isArray(resolved) ? resolved : [];
          // Dedupe by name; a typed domain variable supersedes its plain
          // extracted counterpart so nested-field completion wins.
          const byName = new Map<string, FeelVariable>();
          for (const v of base) if (v?.name) byName.set(v.name, v);
          for (const v of domain) if (v?.name) byName.set(v.name, v);
          return [...byName.values()];
        }
      }
      const domainVariableResolverModule = {
        variableResolver: ["type", DomainVariableResolver],
      };
      // Palette provider: one draggable entry per installed component. Defined
      // here so it can read the live `componentsRef` — the set loads from the
      // project after mount and can change as component files are added.
      class UrbanComponentsPaletteProvider {
        // Explicit annotation so didi injection survives Vite minification.
        static $inject = ["palette", "create", "elementTemplates"];
        private readonly create: CreateService;
        private readonly elementTemplates: ElementTemplatesService;
        constructor(
          palette: PaletteService,
          create: CreateService,
          elementTemplates: ElementTemplatesService,
        ) {
          this.create = create;
          this.elementTemplates = elementTemplates;
          palette.registerProvider(this);
        }
        getPaletteEntries(): Record<string, unknown> {
          const entries: Record<string, unknown> = {};
          for (const tpl of componentsRef.current) {
            const start = (event: Event) => {
              const shape = this.elementTemplates.createElement(tpl);
              this.create.start(event, shape);
            };
            entries[`create.urban-${tpl.id}`] = {
              group: "urban-components",
              className:
                tpl.elementType?.value === "bpmn:StartEvent"
                  ? "bpmn-icon-start-event-message"
                  : "bpmn-icon-service-task",
              title: tpl.name,
              action: { dragstart: start, click: start },
            };
          }
          return entries;
        }
      }
      const urbanComponentsPaletteModule = {
        __init__: ["urbanComponentsPaletteProvider"],
        urbanComponentsPaletteProvider: [
          "type",
          UrbanComponentsPaletteProvider,
        ],
      };
      // Properties provider (the Object Inspector): a "Data envelope" group on
      // every element with a typed data boundary — service-ish tasks (job I/O),
      // user tasks (form I/O) and message-bearing elements (correlation payload,
      // carried on the referenced `bpmn:Message`). Input/Output envelope pickers
      // bound to the manifest's declared registry types (ADR 0033 §6). Picking a
      // type writes a reserved `zeebe:property` *into the model* as a normal,
      // undoable modeling command (so the data contract travels in the `.bpmn`);
      // for service tasks it is also projected onto the manifest `workers[]`
      // entry so the reifier keeps `defineWorker` typed. Gated on the binding
      // being enabled (Urban App projects only).
      const createEnvelope = async (apply: (value: string) => void) => {
        const binding = domainTypeBindingRef.current;
        if (!binding?.createType) return;
        let id: string | undefined;
        try {
          // Opens the field-authoring modal in the parent; resolves with the new
          // type id once declared + persisted, or undefined if cancelled.
          id = await binding.createType();
        } catch {
          // Surfaced by the parent's manifest-save error handling.
          return;
        }
        if (id) apply(id);
      };
      const DataEnvelopeEntry = (props: {
        element: { type?: string; businessObject?: ModdleElement };
        field: EnvelopeField;
      }) => {
        const { element, field } = props;
        const ctx = envelopeContext(element);
        // A user task whose linked form is typed defaults its envelope to the
        // form's bound type (ADR 0033 §6): the form binding stays the single
        // source of truth; an explicit ref on the task overrides it.
        const formDefault = ctx?.formId
          ? domainTypeBindingRef.current?.formType?.(ctx.formId)
          : undefined;
        const getValue = () => (ctx ? readEnvelope(ctx.target, field) : "");
        const applyValue = (value: string) => {
          const modeler = modelerRef.current;
          if (!modeler || !ctx) return;
          const v = value ?? "";
          writeEnvelope(
            modeler.get<Moddle>("moddle"),
            modeler.get<Modeling>("modeling"),
            element,
            ctx.target,
            field,
            v,
          );
          // Projection (service tasks only): keep the worker-IO map that types
          // `defineWorker` in sync with the model (ADR 0033 §6, until the
          // server-side derivation of increment 12 retires this cache).
          if (ctx.taskType)
            domainTypeBindingRef.current?.set(ctx.taskType, field, v);
        };
        const getOptions = () => {
          const modeler = modelerRef.current;
          const typeIds = domainTypeBindingRef.current?.typeIds ?? [];
          const known = new Set(typeIds);
          // Composed shapes are first-class registry entries (ADR 0040 §9), so a
          // shape id is a selectable envelope type too — gathered from every
          // process (shapes fold into one global registry) and de-duped against
          // the manifest types. Keep the full decls (not just ids) so we can tell
          // which are editable through the flat field modal below.
          const allShapes = modeler
            ? allProcessBos(modeler).flatMap((p) => readShapes(p))
            : [];
          const shapeIds = [
            ...new Set(
              allShapes
                .map((s) => s.id)
                .filter((id): id is string => !!id && !known.has(id)),
            ),
          ];
          // Types the *model* already references via an envelope but that are
          // neither a declared manifest type nor a composed shape — e.g. a
          // hand-authored or drifted `.bpmn`. The model is the authoritative
          // carrier of the data contract (ADR 0033 §6 / ADR 0040), so surface
          // these as selectable options; otherwise a set envelope renders blank
          // (its id is absent from the authored registry) and looks unset. Always
          // include the currently-set value so the picker reflects the model.
          const shapeKnown = new Set([...known, ...shapeIds]);
          const undeclaredIds = modeler
            ? collectEnvelopeTypeRefs(
                modeler.get<ElementRegistry>("elementRegistry").getAll(),
              ).filter((id) => !shapeKnown.has(id))
            : [];
          const current = getValue();
          if (
            current &&
            !shapeKnown.has(current) &&
            !undeclaredIds.includes(current)
          )
            undeclaredIds.push(current);
          // "Edit fields…" is offered only when the selected type is a model shape
          // this flat editor can round-trip losslessly (a non-empty list of pure
          // scalar `extend` fields). It must also live on the **primary** process,
          // since that's the only process the edit write path (`getShapes()`/
          // `writeModelEnvelopeType`) touches — gating on `allShapes` here would
          // offer the option for a non-primary shape and then no-op on click.
          // Composition/list shapes and manifest `types` stay in their own
          // surfaces (shape composer / `nano.app.json`).
          const primaryShapes = modeler
            ? readShapes(primaryProcess(modeler)?.processBo)
            : [];
          const currentShape = primaryShapes.find((s) => s.id === current);
          const currentEditable = envelopeEditableFields(currentShape) != null;
          return [
            // With a form default, clearing (this option) reverts to the inherited
            // type rather than "no type", so name it accordingly.
            {
              value: "",
              label: formDefault
                ? `Inherit from form (${formDefault})`
                : "<none>",
            },
            ...typeIds.map((id) => ({ value: id, label: id })),
            ...shapeIds.map((id) => ({ value: id, label: `${id} (shape)` })),
            // Flagged so the maker sees the type is referenced but not declared
            // in the registry (define its fields via "Create new envelope…").
            ...undeclaredIds.map((id) => ({
              value: id,
              label: `${id} (undeclared)`,
            })),
            ...(current &&
            currentEditable &&
            domainTypeBindingRef.current?.editType
              ? [{ value: EDIT_ENVELOPE, label: "✎ Edit fields…" }]
              : []),
            ...(domainTypeBindingRef.current?.createType
              ? [{ value: CREATE_ENVELOPE, label: "➕ Create new envelope…" }]
              : []),
          ];
        };
        const baseDescription =
          field === "inputType"
            ? "Domain type of the data this element receives — travels in the model."
            : "Domain type of the data this element produces — travels in the model.";
        return SelectEntry({
          element,
          id: `urban-envelope-${field}`,
          label: field === "inputType" ? "Input envelope" : "Output envelope",
          getValue,
          setValue: (value: string) => {
            if (value === CREATE_ENVELOPE) {
              void createEnvelope(applyValue);
              return;
            }
            if (value === EDIT_ENVELOPE) {
              // Edit the currently-set type's fields in place; the envelope ref
              // itself is unchanged, so nothing to re-apply on the element.
              const cur = getValue();
              if (cur) void domainTypeBindingRef.current?.editType?.(cur);
              return;
            }
            applyValue(value);
          },
          getOptions,
          description: formDefault
            ? `${baseDescription} Defaults to the linked form's type (${formDefault}).`
            : baseDescription,
        });
      };
      interface PropertiesPanelService {
        registerProvider(priority: number, provider: unknown): void;
      }
      class UrbanDomainTypePropertiesProvider {
        // Explicit annotation so didi injection survives Vite minification.
        static $inject = ["propertiesPanel"];
        constructor(propertiesPanel: PropertiesPanelService) {
          propertiesPanel.registerProvider(500, this);
        }
        getGroups(element: { type?: string; businessObject?: ModdleElement }) {
          return (groups: unknown[]): unknown[] => {
            const binding = domainTypeBindingRef.current;
            if (!binding?.enabled) return groups;
            const ctx = envelopeContext(element);
            if (!ctx) return groups;
            const entries: unknown[] = [
              {
                id: "urban-envelope-in",
                component: DataEnvelopeEntry,
                isEdited: isSelectEntryEdited,
                field: "inputType",
              },
            ];
            // An input-only carrier (a none start event) produces the process's
            // initial variables, so it has no output envelope.
            if (!ctx.inputOnly)
              entries.push({
                id: "urban-envelope-out",
                component: DataEnvelopeEntry,
                isEdited: isSelectEntryEdited,
                field: "outputType",
              });
            groups.push({
              id: "urban-data-envelope",
              label: "Data envelope",
              component: Group,
              entries,
            });
            return groups;
          };
        }
      }
      const urbanDomainTypePropertiesModule = {
        __init__: ["urbanDomainTypePropertiesProvider"],
        urbanDomainTypePropertiesProvider: [
          "type",
          UrbanDomainTypePropertiesProvider,
        ],
      };
      // Agent-task properties (issue #950): an "Agent task" group on every
      // service task. A toggle adds/removes the prompt binding; when bound, the
      // group surfaces (and edits) the `zeebe:linkedResource` fields — resourceId,
      // bindingType, the fixed resourceType/linkName — plus the optional
      // `appendPrompt` ioMapping addendum. Every read/write derives the emitted
      // shape from `lib/agentTask.ts` (the single source of truth), so the
      // modeler can never drift from the toolchain. Writes run through the live
      // modeler services so they are ordinary undoable modeling commands.
      const agentServices = (): {
        moddle: AgentModdle;
        modeling: AgentModeling;
      } | null => {
        const m = modelerRef.current;
        if (!m || disposedRef.current) return null;
        return {
          moddle: m.get<AgentModdle>("moddle"),
          modeling: m.get<AgentModeling>("modeling"),
        };
      };
      type AgentElement = {
        type?: string;
        businessObject?: AgentModdleElement;
      };
      const AgentExternalToggleEntry = (props: { element: AgentElement }) => {
        const { element } = props;
        return ToggleSwitchEntry({
          element,
          id: "nano-agent-external-toggle",
          label: "Agent task",
          switcherLabel: "External agent worker",
          getValue: () => hasExternalAgentMarker(element.businessObject),
          setValue: (value: boolean) => {
            const svc = agentServices();
            const bo = element.businessObject;
            if (!svc || !bo) return;
            if (value)
              writeExternalAgentMarker(svc.moddle, svc.modeling, element, bo);
            else
              removeExternalAgentMarker(svc.moddle, svc.modeling, element, bo);
          },
          description: `Mark this task agentic with the canonical <zeebe:agentDefinition agentType="${AGENT_TYPE_EXTERNAL}"/> marker (the harness --auto scan keys on it).`,
        });
      };
      const AgentOptOutEntry = (props: { element: AgentElement }) => {
        const { element } = props;
        return ToggleSwitchEntry({
          element,
          id: "nano-agent-optout",
          label: "Exclude from --auto",
          switcherLabel: "Manual subscription only",
          getValue: () => readAutoSubscribeOptOut(element.businessObject),
          setValue: (value: boolean) => {
            const svc = agentServices();
            const bo = element.businessObject;
            if (!svc || !bo) return;
            writeAutoSubscribeOptOut(
              svc.moddle,
              svc.modeling,
              element,
              bo,
              value,
            );
          },
          description: `Set ${AUTO_SUBSCRIBE_PROPERTY}="false" so the harness --auto scan skips this task; explicit --job-type/profile targeting still serves it.`,
        });
      };
      const AgentToggleEntry = (props: { element: AgentElement }) => {
        const { element } = props;
        return ToggleSwitchEntry({
          element,
          id: "nano-agent-toggle",
          label: "Prompt binding",
          switcherLabel: "Prompt-bound worker",
          getValue: () => hasPromptBinding(element.businessObject),
          setValue: (value: boolean) => {
            const svc = agentServices();
            const bo = element.businessObject;
            if (!svc || !bo) return;
            if (value)
              writePromptLink(
                svc.moddle,
                svc.modeling,
                element,
                bo,
                "",
                PROMPT_DEFAULT_BINDING_TYPE,
              );
            else removePromptBinding(svc.moddle, svc.modeling, element, bo);
          },
          description:
            "Bind an LLM prompt resource so an agent worker services this task.",
        });
      };
      const AgentResourceIdEntry = (props: { element: AgentElement }) => {
        const { element } = props;
        const debounce = useService("debounceInput");
        return TextFieldEntry({
          element,
          id: "nano-agent-resourceId",
          label: "Prompt resource",
          debounce,
          getValue: () =>
            readPromptBinding(element.businessObject)?.resourceId ?? "",
          setValue: (value?: string) => {
            const svc = agentServices();
            const bo = element.businessObject;
            if (!svc || !bo) return;
            const current = readPromptBinding(bo);
            writePromptLink(
              svc.moddle,
              svc.modeling,
              element,
              bo,
              value ?? "",
              current?.bindingType ?? PROMPT_DEFAULT_BINDING_TYPE,
            );
          },
          description: `The ${PROMPT_RESOURCE_TYPE} resource bound as the agent's prompt (e.g. "feature.md").`,
        });
      };
      const AgentBindingTypeEntry = (props: { element: AgentElement }) => {
        const { element } = props;
        return SelectEntry({
          element,
          id: "nano-agent-bindingType",
          label: "Binding type",
          getValue: () =>
            readPromptBinding(element.businessObject)?.bindingType ??
            PROMPT_DEFAULT_BINDING_TYPE,
          setValue: (value: string) => {
            const svc = agentServices();
            const bo = element.businessObject;
            if (!svc || !bo) return;
            const current = readPromptBinding(bo);
            writePromptLink(
              svc.moddle,
              svc.modeling,
              element,
              bo,
              current?.resourceId ?? "",
              value || PROMPT_DEFAULT_BINDING_TYPE,
            );
          },
          getOptions: () =>
            PROMPT_BINDING_TYPES.map((id) => ({ value: id, label: id })),
          description:
            "How the engine resolves the prompt resource version at deploy time.",
        });
      };
      // The two fixed fields are disabled, but `@bpmn-io/properties-panel`'s
      // `TextField` still evaluates `debounce(onInput)` in a `useMemo` on every
      // render regardless of `disabled`, so a `TextFieldEntry` without a
      // `debounce` throws "is not a function" and — the console has no error
      // boundary — unmounts the whole properties panel. Supply the service like
      // the editable entries above so a bound agent task inspects cleanly.
      const AgentResourceTypeEntry = (props: { element: AgentElement }) => {
        const debounce = useService("debounceInput");
        return TextFieldEntry({
          element: props.element,
          id: "nano-agent-resourceType",
          label: "Resource type",
          debounce,
          disabled: true,
          getValue: () => PROMPT_RESOURCE_TYPE,
          setValue: () => {},
          description: "Fixed — the agentic prompt side-car marker.",
        });
      };
      const AgentLinkNameEntry = (props: { element: AgentElement }) => {
        const debounce = useService("debounceInput");
        return TextFieldEntry({
          element: props.element,
          id: "nano-agent-linkName",
          label: "Link name",
          debounce,
          disabled: true,
          getValue: () => PROMPT_LINK_NAME,
          setValue: () => {},
          description: "Fixed — the agentic signal consumers detect.",
        });
      };
      const AgentAppendEntry = (props: { element: AgentElement }) => {
        const { element } = props;
        const debounce = useService("debounceInput");
        return TextAreaEntry({
          element,
          id: "nano-agent-append",
          label: "Append prompt (FEEL)",
          debounce,
          getValue: () =>
            readPromptBinding(element.businessObject)?.append ?? "",
          setValue: (value?: string) => {
            const svc = agentServices();
            const bo = element.businessObject;
            if (!svc || !bo) return;
            writeAppendPrompt(
              svc.moddle,
              svc.modeling,
              element,
              bo,
              value ?? "",
            );
          },
          description:
            "Optional FEEL expression appended to the bound prompt at runtime (appendPrompt input).",
        });
      };
      class AgentTaskPropertiesProvider {
        // Explicit annotation so didi injection survives Vite minification.
        static $inject = ["propertiesPanel"];
        constructor(propertiesPanel: PropertiesPanelService) {
          propertiesPanel.registerProvider(500, this);
        }
        getGroups(element: AgentElement) {
          return (groups: unknown[]): unknown[] => {
            if (element.type !== AGENT_TASK_ELEMENT_TYPE) return groups;
            // The external-agent marker toggle leads — it is the canonical
            // single-convention agentic signal (#1180).
            const entries: unknown[] = [
              {
                id: "nano-agent-external-toggle",
                component: AgentExternalToggleEntry,
                isEdited: isToggleSwitchEntryEdited,
              },
            ];
            // The `--auto` opt-out only makes sense for a marked agent task.
            if (hasExternalAgentMarker(element.businessObject)) {
              entries.push({
                id: "nano-agent-optout",
                component: AgentOptOutEntry,
                isEdited: isToggleSwitchEntryEdited,
              });
            }
            // The prompt binding remains authorable alongside the marker.
            entries.push({
              id: "nano-agent-toggle",
              component: AgentToggleEntry,
              isEdited: isToggleSwitchEntryEdited,
            });
            if (hasPromptBinding(element.businessObject)) {
              entries.push(
                {
                  id: "nano-agent-resourceId",
                  component: AgentResourceIdEntry,
                  isEdited: isTextFieldEntryEdited,
                },
                {
                  id: "nano-agent-bindingType",
                  component: AgentBindingTypeEntry,
                  isEdited: isSelectEntryEdited,
                },
                {
                  id: "nano-agent-resourceType",
                  component: AgentResourceTypeEntry,
                },
                {
                  id: "nano-agent-linkName",
                  component: AgentLinkNameEntry,
                },
                {
                  id: "nano-agent-append",
                  component: AgentAppendEntry,
                  isEdited: isTextAreaEntryEdited,
                },
              );
            }
            groups.push({
              id: "nano-agent-task",
              label: "Agent task",
              component: Group,
              entries,
            });
            return groups;
          };
        }
      }
      const agentTaskPropertiesModule = {
        __init__: ["agentTaskPropertiesProvider"],
        agentTaskPropertiesProvider: ["type", AgentTaskPropertiesProvider],
      };
      const modeler = new Modeler({
        container: containerRef.current,
        propertiesPanel: { parent: panelRef.current },
        additionalModules: [
          BpmnPropertiesPanelModule,
          BpmnPropertiesProviderModule,
          ZeebePropertiesProviderModule,
          // Element templates (ADR 0033): the component model. Core registers the
          // `elementTemplates` service + create-append behaviour; the provider
          // renders the "Template" group (the component's Object Inspector); the
          // cloud behaviours keep Zeebe extension elements consistent on edits.
          CloudElementTemplatesCoreModule,
          CloudElementTemplatesPropertiesProviderModule,
          CloudBehaviorsModule,
          urbanComponentsPaletteModule,
          urbanDomainTypePropertiesModule,
          agentTaskRendererModule,
          agentTaskPropertiesModule,
          domainVariableResolverModule,
        ],
        moddleExtensions: {
          zeebe: zeebeModdleWithAgent,
          nano: nanoShapesModdle,
        },
      });
      modelerRef.current = modeler;
      // Install the project's components so the palette + template chooser
      // surface them (ADR 0033 increment 2). The set may still be loading at
      // mount; the `[components]` effect below re-installs + refreshes the
      // palette once it arrives or changes.
      try {
        modeler
          .get<ElementTemplatesService>("elementTemplates")
          .set(componentsRef.current);
      } catch {
        // Non-fatal: without the core module the palette simply shows nothing.
      }
      const handleChanged = () => {
        if (suppressChange.current) return;
        // The canvas now diverges from the last imported document.
        loadGateRef.current.invalidate();
        onChangeRef.current?.();
      };
      modeler.on("commandStack.changed", handleChanged);
      // Start on a blank diagram so the canvas is never empty. Queued like any
      // other load so a quick open() serializes after it instead of racing it.
      void runLoad(
        (m) => m.createDiagram(),
        () => onReadyRef.current?.(),
      );
      return () => {
        disposedRef.current = true;
        modeler.off("commandStack.changed", handleChanged);
        modeler.destroy();
        modelerRef.current = null;
      };
    }, []);

    // Re-install the component set + refresh the palette when it changes. The
    // project loads components asynchronously (and can add more), so the mount
    // effect's `set()` may have run against an empty/stale set. Reads live via
    // `componentsRef`; the palette re-render re-invokes `getPaletteEntries`.
    useEffect(() => {
      const modeler = modelerRef.current;
      if (!modeler || disposedRef.current) return;
      try {
        modeler
          .get<ElementTemplatesService>("elementTemplates")
          .set(componentsRef.current);
        modeler.get<PaletteService>("palette")._update?.();
      } catch {
        // Non-fatal: core/palette absent, or modeler torn down mid-flight.
      }
    }, [components]);

    useImperativeHandle(ref, () => ({
      async getXml() {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return "";
        const { xml } = await modeler.saveXML({ format: true });
        return xml;
      },
      importXml(xml: string) {
        const modeler = modelerRef.current;
        if (!modeler) return Promise.resolve();
        // The attempt token keys the gate record to THIS load, so a stale
        // failure/settle from a superseded attempt (e.g. the first A in an
        // A → B → A sequence) cannot clear or capture into a newer record.
        const attempt: { current?: LoadAttempt } = {};
        const duplicate = loadGateRef.current.shouldLoad(modeler, xml, attempt);
        // A duplicate request awaits the ORIGINAL in-flight import (rejection
        // included) instead of resolving immediately — resolving early would
        // let a repeated XML→Visual click clear the XML-dirty flag and show
        // the canvas even when the import later fails.
        if (duplicate) return duplicate;
        const run = runLoad((m) =>
          m.importXML(xml).catch((e: unknown) => {
            // Only re-arm when the gate still records THIS import — a newer
            // document may have been delivered while this one was in flight.
            loadGateRef.current.invalidateIf(modeler, xml, attempt.current);
            throw e;
          }),
        );
        loadGateRef.current.settle(modeler, xml, run, attempt.current);
        return run;
      },
      createBlank() {
        loadGateRef.current.invalidate();
        return runLoad((m) => m.createDiagram());
      },
      getProcessId() {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return null;
        try {
          return modeler.get<Canvas>("canvas").getRootElement().businessObject
            .id;
        } catch {
          return null;
        }
      },
      setProcessId(id: string) {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return;
        try {
          const canvas = modeler.get<Canvas>("canvas");
          const modeling = modeler.get<Modeling>("modeling");
          modeling.updateProperties(canvas.getRootElement(), { id });
        } catch {
          // Root not ready (e.g. a load is still settling) — ignore.
        }
      },
      getShapes() {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return [];
        return readShapes(primaryProcess(modeler)?.processBo);
      },
      setShapes(shapes: ShapeDecl[]) {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return;
        const p = primaryProcess(modeler);
        if (!p) return;
        try {
          writeShapes(
            modeler.get<ShapeModdle>("moddle"),
            modeler.get<ShapeModeling>("modeling"),
            p.element,
            p.processBo,
            shapes,
          );
        } catch {
          // Root not ready (e.g. a load is still settling) — ignore.
        }
      },
      getMeta() {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return [];
        return readMeta(primaryProcess(modeler)?.processBo);
      },
      setMeta(meta: MetaEntry[]) {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return;
        const p = primaryProcess(modeler);
        if (!p) return;
        try {
          writeMeta(
            modeler.get<ShapeModdle>("moddle"),
            modeler.get<ShapeModeling>("modeling"),
            p.element,
            p.processBo,
            meta,
          );
        } catch {
          // Root not ready (e.g. a load is still settling) — ignore.
        }
      },
    }));

    return (
      // The bpmn-js canvas + properties panel are light-themed third-party
      // widgets, so both surfaces stay physically white (with a matching light
      // divider) regardless of the console theme to keep diagrams legible.
      <div className="flex h-full w-full">
        <div ref={containerRef} className="h-full min-w-0 flex-1 bg-white" />
        <div className="relative flex h-full shrink-0">
          <button
            type="button"
            onClick={togglePanel}
            title={
              panelCollapsed ? "Show properties panel" : "Hide properties panel"
            }
            aria-label={
              panelCollapsed ? "Show properties panel" : "Hide properties panel"
            }
            aria-expanded={!panelCollapsed}
            className="absolute left-0 top-2 z-10 -translate-x-full rounded-l border border-r-0 border-[#d4d4d8] bg-white p-1 text-[#52525b] shadow-sm hover:bg-[#f4f4f5]"
          >
            <svg
              className={`h-4 w-4 ${panelCollapsed ? "rotate-180" : ""}`}
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <path d="M9 6l6 6-6 6" />
            </svg>
          </button>
          <div
            ref={panelRef}
            className={`bpmn-properties h-full overflow-auto bg-white ${
              panelCollapsed
                ? "w-0 overflow-hidden border-l-0"
                : "w-80 border-l border-[#d4d4d8]"
            }`}
          />
        </div>
      </div>
    );
  },
);

export default BpmnModeler;
