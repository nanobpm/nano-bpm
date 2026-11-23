import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import {
  FormEditor as FormJsEditor,
  schemaVersion,
} from "@bpmn-io/form-js-editor";
import "@bpmn-io/form-js/dist/assets/form-js.css";
import "@bpmn-io/form-js/dist/assets/form-js-editor.css";
import { createFormDataBindingModule } from "../lib/formDataBindingProperties";
import { DocumentLoadGate, type LoadAttempt } from "../lib/documentLoadGate";

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | { [key: string]: JsonValue } | JsonValue[];

interface FormSchema {
  type: "default";
  components: JsonValue[];
  schemaVersion: number;
  [key: string]: JsonValue;
}

/// Imperative handle the form editor view drives. Keeps the live form schema
/// inside this component and exposes just the operations the toolbar needs.
export interface FormEditorHandle {
  /// Serializes the current schema to pretty-printed JSON.
  getSchema(): Promise<string>;
  /// Replaces the schema with parsed JSON.
  importSchema(json: string): Promise<void>;
  /// Loads a blank form schema.
  createBlank(): Promise<void>;
}

interface FormEditorProps {
  /// Called whenever the schema changes (after the first import). The initial
  /// blank schema does not mark dirty.
  onChange?: () => void;
  /// Returns the datasource aliases (`data.sources`) declared in the current
  /// project's `nano.app.json`, driving the "Data source" binding inspector's
  /// dropdown (ADR 0024 §5). Read lazily so a project switch reflects the right
  /// list. Absent → no datasources (the inspector still shows, just empty).
  getDataSources?: () => string[];
  /// Called once the editor has mounted and finished its initial (blank) load,
  /// so a parent that fetched a schema before the lazy chunk mounted can retry
  /// the import (mirrors BpmnModeler.onReady).
  onReady?: () => void;
}

const createEmptySchema = (): FormSchema => ({
  type: "default",
  components: [],
  schemaVersion,
});

const parseSchema = (json: string): FormSchema => {
  const parsed: unknown = JSON.parse(json);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Form schema must be a JSON object");
  }
  return parsed as FormSchema;
};

const FormEditor = forwardRef<FormEditorHandle, FormEditorProps>(
  function FormEditor({ onChange, getDataSources, onReady }, ref) {
    const containerRef = useRef<HTMLDivElement>(null);
    const editorRef = useRef<FormJsEditor | null>(null);
    // Keep the datasource accessor in a ref so the editor (built once) always
    // reads the current project's list without needing a remount.
    const getDataSourcesRef = useRef(getDataSources);
    getDataSourcesRef.current = getDataSources;
    // Set once the editor has been destroyed, so async work already in flight
    // doesn't touch a dead instance.
    const disposedRef = useRef(false);
    // Serializes schema loads. importSchema must never overlap.
    const opChainRef = useRef<Promise<unknown>>(Promise.resolve());
    // Re-delivering the document the editor already holds is a no-op
    // (issue #1048): a duplicate import would wipe the user's selection.
    const loadGateRef = useRef(new DocumentLoadGate<FormJsEditor>());
    // Suppress the change callback for programmatic loads (import/createBlank).
    const suppressChange = useRef(false);
    const suppressTimerRef = useRef<number | null>(null);
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    const onReadyRef = useRef(onReady);
    onReadyRef.current = onReady;

    const clearSuppressTimer = () => {
      if (suppressTimerRef.current === null) return;
      window.clearTimeout(suppressTimerRef.current);
      suppressTimerRef.current = null;
    };

    const runLoad = (
      loader: (editor: FormJsEditor) => Promise<unknown>,
    ): Promise<void> => {
      const editor = editorRef.current;
      if (!editor) return Promise.resolve();
      const run = opChainRef.current.then(async () => {
        if (disposedRef.current || editorRef.current !== editor) return;
        clearSuppressTimer();
        suppressChange.current = true;
        try {
          await loader(editor);
        } finally {
          suppressTimerRef.current = window.setTimeout(() => {
            if (!disposedRef.current && editorRef.current === editor) {
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
      const editor = new FormJsEditor({
        container: containerRef.current,
        additionalModules: [
          createFormDataBindingModule(
            () => getDataSourcesRef.current?.() ?? [],
          ),
        ],
      });
      editorRef.current = editor;

      const handleChanged = () => {
        if (suppressChange.current) return;
        // The editor now diverges from the last imported document.
        loadGateRef.current.invalidate();
        onChangeRef.current?.();
      };

      editor.on("changed", handleChanged);
      // Fire `onReady` after the initial load settles (success OR failure) so a
      // parent waiting to import a fetched schema is never left waiting — but not
      // if the editor was torn down mid-load (e.g. a rapid file switch), which
      // would signal readiness for a destroyed editor and setState on an
      // unmounted parent.
      void runLoad((e) => e.importSchema(createEmptySchema())).finally(() => {
        if (!disposedRef.current) onReadyRef.current?.();
      });

      return () => {
        disposedRef.current = true;
        clearSuppressTimer();
        editor.off("changed", handleChanged);
        editor.destroy();
        editorRef.current = null;
      };
    }, []);

    useImperativeHandle(ref, () => ({
      async getSchema() {
        const editor = editorRef.current;
        if (!editor || disposedRef.current) return "";
        return JSON.stringify(editor.getSchema() as unknown, null, 2);
      },
      importSchema(json: string) {
        const editor = editorRef.current;
        if (!editor) return Promise.resolve();
        // The attempt token keys the gate record to THIS load, so a stale
        // failure/settle from a superseded attempt (e.g. the first A in an
        // A → B → A sequence) cannot clear or capture into a newer record.
        const attempt: { current?: LoadAttempt } = {};
        const duplicate = loadGateRef.current.shouldLoad(editor, json, attempt);
        // A duplicate request awaits the ORIGINAL in-flight import (rejection
        // included) instead of resolving immediately — resolving early would
        // let a caller treat a still-pending, possibly-failing import as done.
        if (duplicate) return duplicate;
        const run = runLoad(async (e) => {
          try {
            await e.importSchema(parseSchema(json));
          } catch (err) {
            // Only re-arm when the gate still records THIS import — a newer
            // document may have been delivered while this one was in flight.
            loadGateRef.current.invalidateIf(editor, json, attempt.current);
            throw err;
          }
        });
        loadGateRef.current.settle(editor, json, run, attempt.current);
        return run;
      },
      createBlank() {
        loadGateRef.current.invalidate();
        return runLoad((editor) => editor.importSchema(createEmptySchema()));
      },
    }));

    return <div ref={containerRef} className="h-full w-full bg-white" />;
  },
);

export default FormEditor;
