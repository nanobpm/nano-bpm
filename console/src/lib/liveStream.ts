/// Subscribes to the gateway's `/console/api/stream` SSE feed and calls
/// `onChange` whenever the read model advances. The server emits a cheap change
/// signal (the exported position); the caller decides what to refetch — keeping
/// the wire protocol trivial and the UI always fresh without busy polling.
///
/// The change signal is edge-triggered, so a client that misses an edge stays
/// stale until the next one. That is exactly what a gateway restart causes: the
/// SSE connection drops, the browser silently auto-reconnects, but any state
/// that changed during the gap (or the pre-restart cache itself) is never
/// refetched — e.g. an instance the console still shows as ACTIVE after it was
/// cancelled/terminated across the bounce. So `onChange` also fires on every
/// `open`: the initial connect and every auto-reconnect both trigger a
/// catch-up refetch, closing that window. (The mount-time refetch React Query
/// already does makes the first `open` a cheap dedupe.)
///
/// Returns the unsubscribe function.
export function subscribeToReadModel(onChange: () => void): () => void {
  const source = new EventSource("/console/api/stream");
  source.addEventListener("open", onChange);
  source.addEventListener("instances", onChange);
  return () => {
    source.removeEventListener("open", onChange);
    source.removeEventListener("instances", onChange);
    source.close();
  };
}
