// Shared cadence for the extension-marketplace update polls (the left-rail
// badge in App.tsx and the Extensions view). The server caches the marketplace
// listing for several minutes and shells out to npm once per installed pack on
// a miss, so a tight 30 s cadence across several tabs fanned concurrent npm
// bursts at the OS and swap-thrashed a small host (#1330). Poll on a
// several-minute cadence instead — update discovery doesn't need second-level
// freshness — and never overlap a poll with an unfinished one.
export const MARKETPLACE_POLL_MS = 5 * 60_000;
