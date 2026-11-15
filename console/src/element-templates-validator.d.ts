// `@bpmn-io/element-templates-validator` ships no types; declare the entry the
// element-template unit tests use (`builtinComponents.test.ts`).
declare module "@bpmn-io/element-templates-validator" {
  export function validateZeebe(template: unknown): {
    valid: boolean;
    errors: unknown[] | null;
  };
}
