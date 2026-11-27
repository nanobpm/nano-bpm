#!/usr/bin/env bash
# fetch_pinned: fetch a pinned, immutable artifact into a local cache and
# SHA-256 verify it (#1770, #2079). Sourced by zeebe-coverage.sh; tested by
# fetch-pinned.test.mjs.
#
#   fetch_pinned <name> <dest> <sha256> <url> [<mirror-url>...]
#
# - A cached <dest> that verifies is reused: NO network request is made.
# - Otherwise each URL is tried in turn. A 429 / 5xx / connection failure is
#   retried with exponential backoff (capped at 120 s), up to
#   FETCH_PINNED_BUDGET_S seconds of wall-clock time per URL (default 300),
#   request time included; each attempt's curl --max-time is bounded by the
#   remaining budget (max 300 s), and no request starts once the budget is
#   spent. A numeric Retry-After (header name matched case-insensitively) is
#   never shortened: if it leaves no budget for a further request the source
#   is abandoned at once for the next mirror, otherwise it is slept in full. Any other
#   status (e.g. 404) moves on to the next mirror. This is a retry of the
#   FETCH of an immutable artifact, not a test retry.
# - A download enters the cache only after it matches the pinned SHA-256, so
#   a bad mirror can never poison it. Fails closed with a message naming the
#   artifact and the last HTTP status.
#
# FETCH_PINNED_SLEEP overrides the sleep command and FETCH_PINNED_NOW the
# clock command (prints epoch seconds; default `date +%s`) for tests.

fetch_pinned_sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

fetch_pinned() {
  local name="$1" dest="$2" want="$3"
  shift 3
  if [[ -f "$dest" && "$(fetch_pinned_sha256 "$dest")" == "$want" ]]; then
    echo "$name: using cached $dest"
    return 0
  fi
  rm -f "$dest"
  mkdir -p "$(dirname "$dest")"
  local budget="${FETCH_PINNED_BUDGET_S:-300}" sleeper="${FETCH_PINNED_SLEEP:-sleep}"
  local clock="${FETCH_PINNED_NOW:-date +%s}"
  local tmp="$dest.tmp" hdr="$dest.hdr" url status="none" start elapsed remaining delay ra maxt wait_s
  for url in "$@"; do
    start="$($clock)" delay=5
    while :; do
      elapsed=$(( $($clock) - start )); remaining=$(( budget - elapsed ))
      # Never start a request once the source's budget is spent (the backoff
      # below only sleeps when time is left for at least one more request).
      (( remaining < 1 )) && break
      maxt=$remaining; (( maxt > 300 )) && maxt=300
      echo "$name: fetching $url"
      status="$(curl -sS -L --connect-timeout 20 --max-time "$maxt" -D "$hdr" -o "$tmp" -w '%{http_code}' "$url" 2>/dev/null)" || true
      status="${status:-000}"
      if [[ "$status" == 200 ]]; then
        if [[ "$(fetch_pinned_sha256 "$tmp")" == "$want" ]]; then
          mv "$tmp" "$dest"
          rm -f "$hdr"
          echo "$name: fetched and verified (sha256 $want)"
          return 0
        fi
        echo "$name: $url does not match the pinned SHA-256 — trying next source" >&2
        status="sha256-mismatch"
        break
      fi
      if [[ "$status" != 429 && "$status" != 000 && "$status" != 5?? ]]; then
        echo "$name: $url returned HTTP $status — trying next source" >&2
        break
      fi
      ra="$(tr -d '\r' < "$hdr" 2>/dev/null | tr '[:upper:]' '[:lower:]' | sed -n 's/^retry-after:[[:space:]]*\([0-9][0-9]*\)[[:space:]]*$/\1/p' | tail -1)"
      elapsed=$(( $($clock) - start )); remaining=$(( budget - elapsed ))
      if [[ -n "$ra" ]]; then
        if (( ra >= remaining )); then
          echo "$name: $url returned HTTP $status with Retry-After ${ra}s, beyond the ${remaining}s left of its budget — trying next source" >&2
          break
        fi
        wait_s="$ra"
      else
        wait_s="$delay"
        (( wait_s > 120 )) && wait_s=120
        if (( wait_s >= remaining )); then
          echo "$name: $url still failing (HTTP $status) after ${elapsed}s — trying next source" >&2
          break
        fi
      fi
      echo "$name: $url returned HTTP $status; retrying in ${wait_s}s" >&2
      "$sleeper" "$wait_s"
      delay=$(( delay * 2 ))
    done
  done
  rm -f "$tmp" "$hdr"
  echo "error: could not fetch $name (last HTTP status: $status) from: $*" >&2
  return 1
}
