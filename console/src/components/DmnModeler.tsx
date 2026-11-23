import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import Modeler from "dmn-js/lib/Modeler";
import type { FeelVariable } from "../lib/dmnDomainVariables";
import { DocumentLoadGate, type LoadAttempt } from "../lib/documentLoadGate";
import "dmn-js/dist/assets/diagram-js.css";
import "dmn-js/dist/assets/dmn-js-shared.css";
import "dmn-js/dist/assets/dmn-js-drd.css";
import "dmn-js/dist/assets/dmn-js-decision-table.css";
import "dmn-js/dist/assets/dmn-js-decision-table-controls.css";
import "dmn-js/dist/assets/dmn-js-literal-expression.css";
import "dmn-js/dist/assets/dmn-font/css/dmn.css";

interface EventBus {
  on(event: string, callback: (...args: unknown[]) => void): void;
  off(event: string, callback: (...args: unknown[]) => void): void;
}

interface Viewer {
  get<T = unknown>(service: string): T;
}

/** dmn-js/didi injector — used to reach the optional `variableResolver`. */
interface Injector {
  get<T = unknown>(name: string, strict: false): T | null;
}

/** The subset of `@bpmn-io/dmn-variable-resolver`'s VariableResolver we use. */
interface VariableResolver {
  registerProvider(provider: {
    getVariables(variables: FeelVariable[], element: unknown): FeelVariable[];
  }): void;
}

/// Imperative handle the DMN editor view drives. Keeps the live DMN document
/// inside this component and exposes just the operations the toolbar needs.
export interface DmnModelerHandle {
  /// Serializes the current document to formatted DMN XML.
  getXml(): Promise<string>;
  /// Replaces the document with `xml`.
  importXml(xml: string): Promise<void>;
  /// Loads a blank DMN model.
  createBlank(): Promise<void>;
}

interface DmnModelerProps {
  /// Called whenever the document changes (after the first import). The initial
  /// blank model does not mark dirty.
  onChange?: () => void;
  /// FEEL variables in scope for a decision's input expressions, from the App
  /// manifest's domain-type binding (ADR 0029 §5). Called with the active
  /// decision id; returns the variables to add to dmn-js's own inferred set.
  getVariables?: (decisionId: string | undefined) => FeelVariable[];
  /// Called once the modeler has mounted and finished its initial (blank) load,
  /// so a parent that fetched a document before the lazy chunk mounted can retry
  /// the import (mirrors BpmnModeler.onReady).
  onReady?: () => void;
}

const EMPTY_DMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" xmlns:dmndi="https://www.omg.org/spec/DMN/20191111/DMNDI/" xmlns:dc="http://www.omg.org/spec/DMN/20180521/DC/" id="Definitions_1" name="Definitions" namespace="http://nanobpmn/dmn">
  <decision id="Decision_1" name="Decision">
    <decisionTable id="DecisionTable_1">
      <input id="Input_1">
        <inputExpression id="InputExpression_1" typeRef="string">
          <text></text>
        </inputExpression>
      </input>
      <output id="Output_1" typeRef="string" />
    </decisionTable>
  </decision>
  <dmndi:DMNDI>
    <dmndi:DMNDiagram id="DMNDiagram_1">
      <dmndi:DMNShape id="DMNShape_Decision_1" dmnElementRef="Decision_1">
        <dc:Bounds x="160" y="100" width="180" height="80" />
      </dmndi:DMNShape>
    </dmndi:DMNDiagram>
  </dmndi:DMNDI>
</definitions>`;

const DmnModeler = forwardRef<DmnModelerHandle, DmnModelerProps>(
  function DmnModeler({ onChange, getVariables, onReady }, ref) {
    const containerRef = useRef<HTMLDivElement>(null);
    const modelerRef = useRef<Modeler | null>(null);
    // Set once the modeler has been destroyed, so async work already in flight
    // doesn't touch a dead instance.
    const disposedRef = useRef(false);
    // Serializes document loads. importXML must never overlap.
    const opChainRef = useRef<Promise<unknown>>(Promise.resolve());
    // Re-delivering the document the editor already holds is a no-op
    // (issue #1048): a duplicate import would wipe the user's selection.
    const loadGateRef = useRef(new DocumentLoadGate<Modeler>());
    // Suppress the change callback for programmatic loads (import/createBlank).
    const suppressChange = useRef(false);
    const suppressTimerRef = useRef<number | null>(null);
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    const onReadyRef = useRef(onReady);
    onReadyRef.current = onReady;
    // The bound-type FEEL variable source and the id of the decision currently
    // shown, read live by the variable-resolver provider (ADR 0029 §5).
    const getVariablesRef = useRef(getVariables);
    getVariablesRef.current = getVariables;
    const activeDecisionIdRef = useRef<string | undefined>(undefined);
    const activeEventBusRef = useRef<EventBus | null>(null);
    const activeHandlerRef = useRef<((...args: unknown[]) => void) | null>(
      null,
    );

    const clearSuppressTimer = () => {
      if (suppressTimerRef.current === null) return;
      window.clearTimeout(suppressTimerRef.current);
      suppressTimerRef.current = null;
    };

    const runLoad = (
      loader: (m: Modeler) => Promise<unknown>,
    ): Promise<void> => {
      const modeler = modelerRef.current;
      if (!modeler) return Promise.resolve();
      const run = opChainRef.current.then(async () => {
        if (disposedRef.current || modelerRef.current !== modeler) return;
        clearSuppressTimer();
        suppressChange.current = true;
        try {
          await loader(modeler);
        } finally {
          suppressTimerRef.current = window.setTimeout(() => {
            if (!disposedRef.current && modelerRef.current === modeler) {
              suppressChange.current = false;
            }
            suppressTimerRef.current = null;
          }, 0);
        }
      });
      // Keep the chain alive even when this op fails, so one bad import doesn't
      // wedge every later load.
      opChainRef.current = run.catch(() => {});
      return run;
    };

    useEffect(() => {
      if (!containerRef.current) return;
      disposedRef.current = false;

      // A didi module that registers a variable provider on any viewer carrying a
      // `variableResolver` (the decision-table / expression editors). The provider
      // appends the bound domain type's fields (ADR 0029 §5) to dmn-js's own
      // inferred variables, reading the live source + active decision each call.
      function DomainVariableProvider(injector: Injector) {
        const variableResolver = injector.get<VariableResolver>(
          "variableResolver",
          false,
        );
        if (!variableResolver) return;
        variableResolver.registerProvider({
          getVariables(variables) {
            const extra =
              getVariablesRef.current?.(activeDecisionIdRef.current) ?? [];
            return extra.length > 0 ? [...variables, ...extra] : variables;
          },
        });
      }
      DomainVariableProvider.$inject = ["injector"];
      const domainModule = {
        __init__: ["nanoDomainVariables"],
        nanoDomainVariables: ["type", DomainVariableProvider],
      };
      const viewerModules = { additionalModules: [domainModule] };

      // dmn-js merges per-viewer `additionalModules` (dmn-js-shared Manager
      // `_createViewer`), but its bundled types don't declare the viewer keys —
      // pass a variable so structural typing admits the extra properties.
      const modelerOptions = {
        container: containerRef.current,
        decisionTable: viewerModules,
        literalExpression: viewerModules,
        boxedExpression: viewerModules,
      };
      const modeler = new Modeler(modelerOptions);
      modelerRef.current = modeler;

      const handleChanged = () => {
        if (suppressChange.current) return;
        // The editor now diverges from the last imported document.
        loadGateRef.current.invalidate();
        onChangeRef.current?.();
      };

      const attachActiveViewerChangeListener = () => {
        if (activeEventBusRef.current && activeHandlerRef.current) {
          activeEventBusRef.current.off(
            "commandStack.changed",
            activeHandlerRef.current,
          );
          activeEventBusRef.current = null;
          activeHandlerRef.current = null;
        }
        if (disposedRef.current || modelerRef.current !== modeler) return;
        try {
          const activeViewer = modeler.getActiveViewer() as Viewer | null;
          const eventBus = activeViewer?.get<EventBus>("eventBus");
          if (!eventBus) return;
          eventBus.on("commandStack.changed", handleChanged);
          activeEventBusRef.current = eventBus;
          activeHandlerRef.current = handleChanged;
        } catch {
          // Active view not ready yet — views.changed will fire again once it is.
        }
      };

      const handleViewsChanged = () => {
        // Track the decision shown in the active view so the variable provider
        // scopes to its binding. A DRD view exposes the DRG id (not a decision),
        // which simply resolves to no bound type — no wrong variables.
        const view = (
          modeler as unknown as {
            getActiveView?: () => { element?: { id?: string } } | undefined;
          }
        ).getActiveView?.();
        activeDecisionIdRef.current = view?.element?.id;
        attachActiveViewerChangeListener();
        // Switching views (DRD <-> decision table) is navigation, not an edit:
        // the document is untouched, so the load gate must NOT be invalidated —
        // otherwise a duplicate XML delivered after a view switch re-imports and
        // resets the active view + selection (issue #1048). Only surface the
        // change notification, suppression-guarded like handleChanged.
        if (suppressChange.current) return;
        onChangeRef.current?.();
      };

      modeler.on("views.changed", handleViewsChanged);
      // Fire `onReady` after the initial blank load settles (success OR failure)
      // so a parent waiting to import a fetched document is never left waiting —
      // but not if the modeler was torn down mid-load (e.g. a rapid file switch),
      // which would signal readiness for a destroyed modeler and setState on an
      // unmounted parent.
      void runLoad((m) => m.importXML(EMPTY_DMN)).finally(() => {
        if (!disposedRef.current) onReadyRef.current?.();
      });

      return () => {
        disposedRef.current = true;
        clearSuppressTimer();
        if (activeEventBusRef.current && activeHandlerRef.current) {
          activeEventBusRef.current.off(
            "commandStack.changed",
            activeHandlerRef.current,
          );
        }
        activeEventBusRef.current = null;
        activeHandlerRef.current = null;
        modeler.off("views.changed", handleViewsChanged);
        modeler.destroy();
        modelerRef.current = null;
      };
    }, []);

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
        // let a caller treat a still-pending, possibly-failing import as done.
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
        return runLoad((m) => m.importXML(EMPTY_DMN));
      },
    }));

    return <div ref={containerRef} className="h-full w-full bg-white" />;
  },
);

export default DmnModeler;
