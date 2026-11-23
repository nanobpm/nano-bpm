// Idempotent document delivery for a graphical editor (issue #1048).
//
// ProjectWorkspace hands the fetched file to the editor from an effect that also
// re-runs when the lazy editor reports ready. If the content landed after the
// editor mounted but before its initial blank load finished, the same document
// was imported twice. The second import cleared the canvas and reset the
// properties panel to the process root, dropping whatever the user (or an e2e
// test) had selected in between: the intermittent "Agent task group missing".
//
// The gate records which document was last delivered to which editor instance.
// A repeat request for the same pair is a no-op — and while that document's
// import is still in flight, the repeat request returns the ORIGINAL in-flight
// promise (rejection included) instead of resolving immediately, so a caller
// awaiting the duplicate (e.g. a repeated XML→Visual click clearing the
// XML-dirty flag) cannot observe a successful load that has not happened and
// may still fail. Anything that makes the editor diverge from that document (a
// user edit, a blank load) must call `invalidate()` so the next request for the
// document loads again; a failed import calls `invalidateIf()` so a stale
// failure cannot re-arm a newer document delivered while the failed one was in
// flight.
//
// The record is keyed by load ATTEMPT, not by document identity: in an
// A → B → A sequence both A attempts name the same document, so comparing the
// doc alone would let the FIRST A's late failure (or late `settle`) clear or
// capture into the SECOND A's record. `shouldLoad` hands back an opaque
// attempt token; `settle`/`invalidateIf` only touch the record while it still
// belongs to that attempt.

/** Opaque identity of one load attempt, returned by `shouldLoad`. */
export type LoadAttempt = object;

export class DocumentLoadGate<I> {
  private current: {
    instance: I;
    doc: string;
    /** Identity of the load attempt that created this record. */
    attempt: LoadAttempt;
    /** The in-flight import of `doc`, if the caller registered one. */
    promise: Promise<void> | null;
  } | null = null;

  /** Whether `doc` must be loaded into `instance`, and what a duplicate caller
   *  should await. Records the delivery on a new pair.
   *
   *  Returns `null` when the caller must start the load itself, and sets
   *  `attempt` (when provided) to the token identifying the new record — the
   *  caller SHOULD pass it back to `settle()`/`invalidateIf()` so a superseded
   *  attempt cannot touch a newer record, and SHOULD pass the load's promise
   *  to `settle()` so duplicates share it. Otherwise returns the promise a
   *  duplicate must return: the recorded in-flight promise while the import is
   *  still running (preserving its rejection), or an already-resolved promise
   *  once it has settled (the import is a no-op). */
  shouldLoad(
    instance: I,
    doc: string,
    attempt?: { current?: LoadAttempt },
  ): Promise<void> | null {
    const cur = this.current;
    if (cur && cur.instance === instance && cur.doc === doc) {
      return cur.promise ?? Promise.resolve();
    }
    const token: LoadAttempt = {};
    this.current = { instance, doc, attempt: token, promise: null };
    if (attempt) attempt.current = token;
    return null;
  }

  /** Record the promise of the load started after `shouldLoad()` returned
   *  `null`, so a duplicate request for the same pair awaits the same import.
   *  No-op when the gate no longer records that pair (an edit, blank load, or
   *  a newer document superseded it) — the load then belongs to no record and
   *  duplicates re-load as usual. When `attempt` is given, the registration is
   *  also a no-op unless the record still belongs to THAT attempt: in an
   *  A → B → A sequence the first A's late `settle` must not capture its
   *  promise into the second A's record. */
  settle(
    instance: I,
    doc: string,
    promise: Promise<void>,
    attempt?: LoadAttempt,
  ): void {
    const cur = this.current;
    if (
      cur &&
      cur.instance === instance &&
      cur.doc === doc &&
      (attempt === undefined || cur.attempt === attempt)
    ) {
      cur.promise = promise;
    }
  }

  /** The editor no longer holds the recorded document unchanged. */
  invalidate(): void {
    this.current = null;
  }

  /** The load of `doc` into `instance` failed, so the editor no longer holds
   *  it — but only clear the record when it still names that pair AND still
   *  belongs to the failing attempt. A stale failure (an earlier queued import
   *  rejecting after a newer document — or a newer attempt at the SAME
   *  document, as in A → B → A — was recorded) must not re-arm the newer
   *  record: that would re-import it and wipe the selection, the double-import
   *  this gate exists to prevent. */
  invalidateIf(instance: I, doc: string, attempt?: LoadAttempt): void {
    const cur = this.current;
    if (
      cur &&
      cur.instance === instance &&
      cur.doc === doc &&
      (attempt === undefined || cur.attempt === attempt)
    ) {
      this.current = null;
    }
  }
}

/** The one canonical gated-import wiring shared by every graphical editor
 *  (BpmnModeler, DmnModeler, FormEditor). Deliver `doc` into `instance`
 *  through `gate`: a duplicate delivery awaits the original in-flight import
 *  (rejection included); a fresh delivery enqueues `importDoc` through the
 *  editor's serial `runLoad` queue, re-arms the gate only if THIS attempt
 *  fails, and registers the run so later duplicates share it. */
export function gatedImport<I>(
  gate: DocumentLoadGate<I>,
  instance: I,
  doc: string,
  runLoad: (loader: (i: I) => Promise<unknown>) => Promise<void>,
  importDoc: (i: I) => Promise<unknown>,
): Promise<void> {
  const attempt: { current?: LoadAttempt } = {};
  const duplicate = gate.shouldLoad(instance, doc, attempt);
  if (duplicate) return duplicate;
  const run = runLoad(async (i) => {
    try {
      await importDoc(i);
    } catch (e) {
      // Only re-arm when the gate still records THIS attempt — a newer
      // document may have been delivered while this one was in flight.
      gate.invalidateIf(instance, doc, attempt.current);
      throw e;
    }
  });
  gate.settle(instance, doc, run, attempt.current);
  return run;
}
