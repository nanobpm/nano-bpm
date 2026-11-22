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
// A repeat request for the same pair is a no-op. Anything that makes the editor
// diverge from that document (a user edit, a blank load) must call
// `invalidate()` so the next request for the document loads again; a failed
// import calls `invalidateIf()` so a stale failure cannot re-arm a newer
// document delivered while the failed one was in flight.

export class DocumentLoadGate<I> {
  private current: { instance: I; doc: string } | null = null;

  /** True when `doc` must be loaded into `instance`. Records the delivery, so
   *  an identical follow-up request returns false. */
  shouldLoad(instance: I, doc: string): boolean {
    const cur = this.current;
    if (cur && cur.instance === instance && cur.doc === doc) return false;
    this.current = { instance, doc };
    return true;
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
