// Installs the error-normalising interceptor (see `apiError.ts`, issue #1350) on
// EVERY generated API client the console uses. Called once from `main.tsx`.
//
// Adding a new generated client? Register it in `GENERATED_CLIENTS`; the guard
// in `apiError.test.ts` fails if a `src/gen*/client.gen.ts` exists that is not
// listed here.
import { client as consoleClient } from "../gen/client.gen";
import { client as camundaClient } from "../gen-c8/client.gen";
import { installApiErrorInterceptor } from "./apiError";

export const GENERATED_CLIENTS = {
  gen: consoleClient,
  "gen-c8": camundaClient,
} as const;

export function installApiErrorInterceptors(): void {
  for (const client of Object.values(GENERATED_CLIENTS)) {
    installApiErrorInterceptor(client);
  }
}
