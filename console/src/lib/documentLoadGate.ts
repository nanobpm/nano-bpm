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

export class DocumentLoadGate<I> {
  private current: {
    instance: I;
    doc: string;
    /** The in-flight import of `doc`, if the caller registered one. */
    promise: Promise<void> | null;
  } | null = null;

  /** Whether `doc` must be loaded into `instance`, and what a duplicate caller
   *  should await. Records the delivery on a new pair.
   *
   *  Returns `null` when the caller must start the load itself (and SHOULD then
   *  pass the load's promise to `settle()` so duplicates share it). Otherwise
   *  returns the promise a duplicate must return: the recorded in-flight
   *  promise while the import is still running (preserving its rejection), or
   *  an already-resolved promise once it has settled (the import is a no-op). */
  shouldLoad(instance: I, doc: string): Promise<void> | null {
    const cur = this.current;
    if (cur && cur.instance === instance && cur.doc === doc) {
      return cur.promise ?? Promise.resolve();
    }
    this.current = { instance, doc, promise: null };
    return null;
  }

  /** Record the promise of the load started after `shouldLoad()` returned
   *  `null`, so a duplicate request for the same pair awaits the same import.
   *  No-op when the gate no longer records that pair (an edit, blank load, or
   *  a newer document superseded it) — the load then belongs to no record and
   *  duplicates re-load as usual. */
  settle(instance: I, doc: string, promise: Promise<void>): void {
    const cur = this.current;
    if (cur && cur.instance === instance && cur.doc === doc) {
      cur.promise = promise;
    }
  }

  /** The editor no longer holds the recorded document unchanged. */
  invalidate(): void {
    this.current = null;
  }

  /** The load of `doc` into `instance` failed, so the editor no longer holds
   *  it — but only clear the record when it still names that pair. A stale
   *  failure (an earlier queued import rejecting after a newer document was
   *  recorded) must not re-arm the newer document: that would re-import it and
   *  wipe the selection, the double-import this gate exists to prevent. */
  invalidateIf(instance: I, doc: string): void {
    const cur = this.current;
    if (cur && cur.instance === instance && cur.doc === doc) {
      this.current = null;
    }
  }
}
