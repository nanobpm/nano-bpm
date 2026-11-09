import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { subscribeToReadModel } from "./liveStream.ts";

/// Invalidates the given query keys whenever the read model advances (and on
/// every SSE (re)connect) — see `subscribeToReadModel`. The Process Explorer
/// gets the same signal through its data source (`lib/explorerSource.ts`).
export function useLiveInvalidation(queryKeys: string[]) {
  const queryClient = useQueryClient();

  useEffect(
    () =>
      subscribeToReadModel(() => {
        for (const key of queryKeys) {
          queryClient.invalidateQueries({ queryKey: [key] });
        }
      }),
    // queryKeys is a stable literal at the call sites; intentionally not a dep.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [queryClient],
  );
}
