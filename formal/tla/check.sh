#!/usr/bin/env bash
# Multi-spec TLA+ model-checking harness (#1226).
#
#   formal/tla/check.sh              # check every model of every registered spec
#   formal/tla/check.sh MCFoo ...    # check the named models only (any spec)
#
# Each spec family registers itself with a self-contained descriptor under
# `formal/tla/specs/<Name>.spec` (see specs/TokenFlow.spec for the reference
# example and the descriptor contract). A descriptor declares that spec's OWN
# CONSTANTS mapping, INVARIANTS set, PROPERTIES set, model directory + glob, and
# per-model EXPECTED verdict table. A new spec is added by CREATING a new
# descriptor file in its own slice — never by editing this script or another
# spec's descriptor. Discovery, the generated .cfg, and the drift guard are all
# scoped PER SPEC, so one spec's models are never forced into another's table.
#
# The EXPECTED table in each descriptor is the single record of what each model
# should do. `pass` means TLC finds no error: every invariant and property
# holds, and no state deadlocks. `violates:<P1>,<P2>,...` names the EXACT set of
# invariants and properties TLC must report as violated; every property not
# listed is thereby proven to hold. It records one of two things, and the row's
# comment must say which:
#   - a known engine defect the model reproduces (cite its issue). The fix PR
#     updates the spec to model the fixed engine, TLC stops reporting the
#     violation, and this script fails until the entry is flipped. That is the
#     ratchet: a known bug cannot be forgotten, and the fixed behaviour stays
#     guarded.
#   - a deliberately unsound process graph, where the violation is the correct
#     verdict on the graph (for example a BPMN lack of synchronization, which
#     Zeebe also leaves stuck).
# Trace validation (#1226, Deliverable B; formal/tla/gen-traces.sh) closes the
# spec-vs-Rust gap by replaying TLC behaviours against the engine.
#
# Set FORMAL_LOG_DIR to keep each model's generated .cfg and full TLC log
# (including counterexample traces).
#
# TLC is pinned by version and SHA-256. Set TLA2TOOLS_JAR to use a pre-fetched
# jar; it must match the pinned hash.
set -euo pipefail

TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

specs_dir="$here/specs"
[[ -d "$specs_dir" ]] || { echo "error: no spec descriptors in $specs_dir" >&2; exit 1; }

sha256() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

fetch_tla2tools() {
  mkdir -p "$(dirname "$1")"
  echo "fetching tla2tools $TLA_VERSION -> $1"
  curl -fsSL -o "$1.tmp" \
    "https://github.com/tlaplus/tlaplus/releases/download/v$TLA_VERSION/tla2tools.jar"
  # Verify before the jar enters the cache, so a bad download is never kept.
  if [[ "$(sha256 "$1.tmp")" != "$TLA_SHA256" ]]; then
    rm -f "$1.tmp"
    echo "error: downloaded tla2tools does not match the pinned SHA-256 for $TLA_VERSION" >&2
    exit 1
  fi
  mv "$1.tmp" "$1"
}

if [[ -n "${TLA2TOOLS_JAR:-}" ]]; then
  jar="$TLA2TOOLS_JAR"
  if [[ ! -f "$jar" || "$(sha256 "$jar")" != "$TLA_SHA256" ]]; then
    echo "error: TLA2TOOLS_JAR=$jar is missing or does not match the pinned SHA-256 for $TLA_VERSION" >&2
    exit 1
  fi
else
  jar="${XDG_CACHE_HOME:-$HOME/.cache}/nanobpm-formal/tla2tools-$TLA_VERSION.jar"
  # A cached jar that no longer matches (for example, corrupted) is replaced
  # instead of failing every run.
  if [[ -f "$jar" && "$(sha256 "$jar")" != "$TLA_SHA256" ]]; then
    echo "cached $jar does not match the pinned SHA-256; refetching"
    rm -f "$jar"
  fi
  [[ -f "$jar" ]] || fetch_tla2tools "$jar"
fi
export TLA2TOOLS_JAR="$jar"

# Descriptors, sorted for stable output.
specs=()
for s in "$specs_dir"/*.spec; do
  [[ -e "$s" ]] || { echo "error: no *.spec descriptors in $specs_dir" >&2; exit 1; }
  specs+=("$s")
done

# ---------------------------------------------------------------------------
# Cross-spec guard: a model file must be claimed by exactly one spec, and no
# committed model may EXTEND a registered spec base without being claimed. This
# is the global safety net that keeps a spec's `.spec` descriptor the ONLY place
# its models are registered, so a stray or double-claimed model can never be
# silently unchecked.
declare_names() { # spec_file -> prints "SPEC_NAME|SPEC_MODELS_DIR|SPEC_MODELS_GLOB"
  ( set -euo pipefail
    SPEC_NAME="" SPEC_MODELS_DIR="." SPEC_MODELS_GLOB=""
    # shellcheck disable=SC1090
    source "$1"
    printf '%s|%s|%s\n' "$SPEC_NAME" "$SPEC_MODELS_DIR" "$SPEC_MODELS_GLOB" )
}

resolve_models() { # dir glob -> prints basenames (no .tla), one per line
  ( shopt -s nullglob; cd "$here/$1" 2>/dev/null || exit 0
    for f in $2; do [[ -f "$f" ]] && echo "${f%.tla}"; done )
}

status=0
registered_names=" "
claimed=" "          # "name<space>" for each claimed model basename
for s in "${specs[@]}"; do
  IFS='|' read -r sname sdir sglob <<<"$(declare_names "$s")"
  [[ -n "$sname" ]] || { echo "error: $s does not set SPEC_NAME" >&2; status=1; continue; }
  registered_names="$registered_names$sname "
  while IFS= read -r m; do
    [[ -n "$m" ]] || continue
    if [[ "$claimed" == *" $m "* ]]; then
      echo "error: model $m is claimed by more than one spec descriptor" >&2
      status=1
    fi
    claimed="$claimed$m "
  done < <(resolve_models "$sdir" "$sglob")
done

# Orphan guard: any committed *.tla that EXTENDS a registered spec base, is not
# itself a registered base, and is not claimed, is an unregistered model.
while IFS= read -r tla; do
  base="$(basename "${tla%.tla}")"
  [[ " $registered_names " == *" $base "* ]] && continue   # a spec base module
  # Collect the full EXTENDS clause, which TLA+ allows to span continuation
  # lines (`EXTENDS` alone, or a trailing comma, continues onto the next line).
  # A same-line-only match silently misses multiline declarations, letting an
  # unclaimed model bypass the guard.
  ext="$(awk '
    collecting {
      buf = buf " " $0
      t = buf; sub(/[[:space:]]+$/, "", t)
      if (t ~ /,$/) next
      print buf; exit
    }
    /^[[:space:]]*EXTENDS([[:space:]]|,|$)/ {
      line = $0; sub(/^[[:space:]]*EXTENDS/, "", line); buf = line
      t = buf; sub(/[[:space:]]+$/, "", t)
      if (t == "" || t ~ /,$/) { collecting = 1; next }
      print buf; exit
    }
  ' "$tla" | tr ',' ' ')"
  for e in $ext; do
    if [[ "$registered_names" == *" $e "* ]]; then
      if [[ "$claimed" != *" $base "* ]]; then
        echo "error: $tla EXTENDS $e but is claimed by no spec descriptor (add it to that spec's SPEC_MODELS_GLOB/EXPECTED)" >&2
        status=1
      fi
      break
    fi
  done
done < <(find "$here" -name '*.tla' -not -path '*/specs/*')
[[ $status -eq 0 ]] || exit $status

# ---------------------------------------------------------------------------
# Per-spec model checking. Runs each spec in a fresh subshell so its SPEC_*
# variables are isolated. Returns non-zero if any model of the spec fails.
run_spec() { # spec_file [model...]
  set -euo pipefail
  local spec_file="$1"; shift
  SPEC_NAME="" SPEC_MODELS_DIR="." SPEC_MODELS_GLOB="" SPEC_TRACE_MODELS=()
  SPEC_CONSTANTS=() SPEC_INVARIANTS=() SPEC_PROPERTIES=() SPEC_EXPECTED=()
  # shellcheck disable=SC1090
  source "$spec_file"
  local modeldir="$here/$SPEC_MODELS_DIR"

  # TLC reports a temporal violation without naming the property, so a verdict
  # can only attribute it while there is exactly one.
  [[ ${#SPEC_PROPERTIES[@]} -le 1 ]] || {
    echo "error: ${SPEC_NAME}: check.sh attributes temporal violations to a single property" >&2; return 1; }

  expected_outcome() { # model -> outcome ("" if none)
    local row model outcome
    for row in "${SPEC_EXPECTED[@]}"; do
      read -r model outcome _ <<<"$row"
      if [[ "$model" == "$1" ]]; then echo "$outcome"; return 0; fi
    done
  }

  write_cfg() { # out
    {
      echo "SPECIFICATION Spec"
      echo "CONSTANTS"
      printf '    %s\n' "${SPEC_CONSTANTS[@]}"
      if [[ ${#SPEC_INVARIANTS[@]} -gt 0 ]]; then
        echo "INVARIANTS"; printf '    %s\n' "${SPEC_INVARIANTS[@]}"
      fi
      if [[ ${#SPEC_PROPERTIES[@]} -gt 0 ]]; then
        echo "PROPERTIES"; printf '    %s\n' "${SPEC_PROPERTIES[@]}"
      fi
    } >"$1"
  }

  # Per-spec, two-way drift guard between the model files and the EXPECTED table.
  # `globbed` is the set of model basenames SPEC_MODELS_GLOB actually selects —
  # the same set the global cross-spec `claimed` registry is built from — so it,
  # not mere file existence, is the authority on which models belong to this
  # spec's registered scope.
  local st=0 m globbed=" "
  while IFS= read -r m; do
    [[ -n "$m" ]] || continue
    globbed="$globbed$m "
    [[ -n "$(expected_outcome "$m")" ]] || { echo "error: ${SPEC_NAME}: $m.tla has no entry in EXPECTED" >&2; st=1; }
  done < <( cd "$modeldir"; shopt -s nullglob; for f in $SPEC_MODELS_GLOB; do echo "${f%.tla}"; done )
  local seen=" " row outcome p
  for row in "${SPEC_EXPECTED[@]}"; do
    read -r m _ <<<"$row"
    if [[ "$seen" == *" $m "* ]]; then echo "error: ${SPEC_NAME}: EXPECTED lists $m more than once" >&2; st=1; fi
    seen="$seen$m "
    # An EXPECTED model must be selected by SPEC_MODELS_GLOB, not merely exist on
    # disk: a file present but outside the glob is absent from `claimed`, cannot
    # be picked by the model-filter path, yet the full-run loop below would still
    # run it — silently checking a model outside this spec's registered scope.
    if [[ "$globbed" != *" $m "* ]]; then
      if [[ -f "$modeldir/$m.tla" ]]; then
        echo "error: ${SPEC_NAME}: EXPECTED lists $m but $m.tla is not selected by SPEC_MODELS_GLOB ($SPEC_MODELS_GLOB); it would run outside the spec's registered scope" >&2
      else
        echo "error: ${SPEC_NAME}: EXPECTED lists $m but $m.tla does not exist" >&2
      fi
      st=1
    fi
    outcome="$(expected_outcome "$m")"
    if [[ "$outcome" == violates:* ]]; then
      for p in $(tr ',' ' ' <<<"${outcome#violates:}"); do
        [[ " ${SPEC_INVARIANTS[*]} ${SPEC_PROPERTIES[*]} " == *" $p "* ]] ||
          { echo "error: ${SPEC_NAME}: EXPECTED $m names $p, which is not a checked invariant or property" >&2; st=1; }
      done
    elif [[ "$outcome" != pass ]]; then
      echo "error: ${SPEC_NAME}: EXPECTED $m has outcome $outcome; want pass or violates:<P1>,<P2>,..." >&2; st=1
    fi
  done
  [[ $st -eq 0 ]] || return $st

  # The models to run: an explicit filter (intersected with this spec) or all.
  local models=()
  if [[ $# -gt 0 ]]; then
    for m in "$@"; do [[ -n "$(expected_outcome "$m")" ]] && models+=("$m"); done
    [[ ${#models[@]} -gt 0 ]] || return 0   # none of the filter belongs here
  else
    for row in "${SPEC_EXPECTED[@]}"; do read -r m _ <<<"$row"; models+=("$m"); done
  fi

  local metadir; metadir="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '$metadir'" RETURN

  for m in "${models[@]}"; do
    local want log cfg code violated want_set got matches states
    local continue_flag=()
    want="$(expected_outcome "$m")"
    log="$metadir/$m.log"; cfg="$metadir/$m.cfg"
    write_cfg "$cfg"
    if [[ "$want" == violates:* ]]; then continue_flag=(-continue -deadlock); fi
    set +e
    ( cd "$modeldir" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -cleanup \
        ${continue_flag[@]+"${continue_flag[@]}"} \
        -metadir "$metadir/$m" -config "$cfg" "$m.tla" ) >"$log" 2>&1
    code=$?
    set -e

    violated="$(
      {
        grep -oE 'Invariant [A-Za-z0-9_]+ is violated' "$log" | awk '{print $2}'
        if grep -q 'Temporal properties were violated' "$log"; then echo "${SPEC_PROPERTIES[0]}"; fi
        if grep -q 'Deadlock reached' "$log"; then echo Deadlock; fi
      } | sort -u | paste -sd, - || true
    )"
    want_set="$(tr ',' '\n' <<<"${want#violates:}" | sort -u | paste -sd, -)"
    if [[ $code -eq 0 && -z "$violated" ]] && grep -q "Model checking completed. No error has been found." "$log"; then
      got="pass"
    elif [[ -n "$violated" ]]; then
      got="violates:$violated"
    else
      got="error(exit $code)"
    fi

    matches=false
    if [[ "$want" == pass ]]; then
      if [[ "$got" == pass ]]; then matches=true; fi
    elif [[ "$got" == "violates:$want_set" ]] && grep -q ' 0 states left on queue' "$log"; then
      matches=true
    fi

    if [[ -n "${FORMAL_LOG_DIR:-}" ]]; then
      mkdir -p "$FORMAL_LOG_DIR"; cp "$cfg" "$log" "$FORMAL_LOG_DIR/"
    fi

    states="$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 || true)"
    if grep -q '^Warning' "$log"; then
      echo "FAIL  ${SPEC_NAME}/$m  TLC emitted a warning" >&2
      grep -A1 '^Warning' "$log" >&2
      st=1
    elif $matches; then
      echo "ok    ${SPEC_NAME}/$m  $got  ($states)"
    else
      echo "FAIL  ${SPEC_NAME}/$m  expected $want, got $got" >&2
      cat "$log" >&2
      st=1
    fi
  done
  return $st
}

# Remember whether a model filter was given, before "$@" is consumed: the trace
# drift guard runs only on a full check (below).
full_run=true
[[ $# -eq 0 ]] || full_run=false

# Global filter validation: a requested model that belongs to NO spec descriptor
# is an error, not a silent no-op. Without this, `check.sh DoesNotExist` returns
# 0 (every run_spec skips it), so a typo looks like a verified model. `claimed`
# holds every model basename registered by any descriptor (built above).
if [[ "$full_run" != true ]]; then
  for arg in "$@"; do
    if [[ "$claimed" != *" $arg "* ]]; then
      echo "error: no registered spec descriptor declares model '$arg'" >&2
      status=1
    fi
  done
  [[ $status -eq 0 ]] || exit $status
fi

for s in "${specs[@]}"; do
  # Each spec runs in a fresh subshell (matching run_spec's isolation contract
  # and gen-traces.sh) so a descriptor's sourced fragment cannot leak `cd`, shell
  # options, functions, or traps into a later spec's run.
  ( run_spec "$s" "$@" ) || status=1
done

# Trace-validation fixture drift guard (#1226, Deliverable B). The committed
# formal/tla/traces/<Spec>/<Model>.json fixtures anchor the specs to the engine
# (engine-core/tests/trace_validation); they are a derived artifact. On a full
# run, regenerate them and fail on drift, so a spec change that alters a
# behaviour must refresh + commit the fixtures. gen-traces.sh needs node (to
# parse TLC output): run the guard only when node is present — GitHub runners
# ship it, so the formal CI job (which runs this script) enforces it, while a
# local model-check without node simply skips it. It reuses the jar this script
# already fetched (TLA2TOOLS_JAR is exported above).
if [[ $status -eq 0 && "$full_run" == true ]]; then
  if command -v node >/dev/null 2>&1; then
    "$here/gen-traces.sh" --check || status=1
  else
    echo "note: node not found; skipping trace-validation fixture drift guard (formal/tla/gen-traces.sh --check)"
  fi
fi

# Single-source corpus drift guard (#1258, #1240 slice 3). The MC*/ZMC*.tla
# models above, and the paired BPMN(+DI) and scenario artifacts under
# formal/corpus/, are GENERATED from one graph source per model
# (formal/corpus/graphs/<Id>.json). They are a derived artifact: on a full run,
# regenerate them into memory and fail on drift, so a forgotten regeneration
# (an edited graph, or a hand-edited generated .tla) fails CI instead of
# shipping a silent MC/ZMC twin divergence. The generator's own structural
# tests run here too. Needs node (the generator is node-only) — same
# skip-when-absent discipline as the trace guard above.
if [[ $status -eq 0 && "$full_run" == true ]]; then
  if command -v node >/dev/null 2>&1; then
    node "$here/../corpus/generate.mjs" --check || status=1
    node --test "$here"/../corpus/*.test.mjs || status=1
  else
    echo "note: node not found; skipping single-source corpus drift guard (formal/corpus/generate.mjs --check)"
  fi
fi

exit $status
