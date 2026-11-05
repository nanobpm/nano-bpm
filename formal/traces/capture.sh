#!/usr/bin/env bash
# Zeebe trace capture for the nano differential harness (formal/traces, #1391).
#
# Runs every scenario under formal/traces/scenarios/ on the Zeebe engine at the
# commit pinned in formal/parity/zeebe-pin.json (the ONE pin — nothing here
# duplicates the version) and rewrites formal/traces/goldens/. Offline/manual:
# NOT part of required CI (the nano side, engine-core/tests/zeebe_traces.rs, is).
#
#   formal/traces/capture.sh            # capture all scenarios (refresh goldens)
#   formal/traces/capture.sh <name>...  # capture only these scenarios
#   formal/traces/capture.sh test       # unit-test the harness (site -> cell ids)
#
# After a pin bump, run it and commit the goldens diff: every changed Zeebe
# trace shows up as a reviewable diff of a *.trace.json / *.cells.json file.
#
# Needs: git, node (reads the pin), JDK 21 (ZEEBE_TRACES_JAVA_HOME, else
# JAVA_HOME). The camunda checkout + build is cached in
# ${NANO_TRACES_CACHE:-$HOME/.cache/nano-zeebe-traces}; that path must not
# contain spaces (the camunda build breaks on them).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
pin_file="$here/../parity/zeebe-pin.json"
sha="$(node -p 'require(process.argv[1]).sha' "$pin_file")"
repo="$(node -p 'require(process.argv[1]).repository' "$pin_file")"
cache="${NANO_TRACES_CACHE:-$HOME/.cache/nano-zeebe-traces}"
case "$cache" in *" "*) echo "capture.sh: NANO_TRACES_CACHE must not contain spaces: $cache" >&2; exit 2 ;; esac
src="$cache/camunda-$sha"
export JAVA_HOME="${ZEEBE_TRACES_JAVA_HOME:-${JAVA_HOME:-}}"
if [ -z "$JAVA_HOME" ]; then echo "capture.sh: set ZEEBE_TRACES_JAVA_HOME to a JDK 21" >&2; exit 2; fi
java="$JAVA_HOME/bin/java"

mkdir -p "$cache"
if [ ! -d "$src/.git" ]; then
  git init -q "$src"
  git -C "$src" remote add origin "$repo"
  git -C "$src" fetch -q --depth 1 origin "$sha"
  git -C "$src" checkout -q FETCH_HEAD
fi
if [ ! -f "$src/.nano-traces-built" ]; then
  (cd "$src" && ./mvnw -q -B install -Dquickly -T1C -pl zeebe/engine -am)
  touch "$src/.nano-traces-built"
fi
cp_file="$src/.nano-traces-classpath"
if [ ! -f "$cp_file" ]; then
  (cd "$src" && ./mvnw -q -B -o dependency:build-classpath -Dquickly -pl zeebe/engine \
    -Dmdep.includeScope=test -Dmdep.outputFile="$cp_file")
fi
cp="$src/zeebe/engine/target/classes:$src/zeebe/engine/target/test-classes:$(cat "$cp_file")"

# A PRIVATE per-run classes dir: the cache is shared by every worktree on the
# machine, so a fixed path let two concurrent captures (parallel agents, maybe on
# different JDKs) `rm -rf` and recompile each other's classes mid-run — a
# capture then died with UnsupportedClassVersionError or a startup NPE.
classes="$(mktemp -d "$cache/.nano-traces-classes.XXXXXX")"
trap 'rm -rf "$classes" "$classes.sources" "$classes.javac.log"' EXIT
# An @argfile with quoted paths: the repo checkout path may contain spaces.
find "$here/capture/src/main/java" "$here/capture/src/test/java" -name '*.java' \
  | sed 's/.*/"&"/' > "$classes.sources"
# javac must fail the script (no `|| true` swallowing it): `-Xstdout` routes its
# non-diagnostic notes to a file so the diagnostics left on stderr stay visible.
if ! "$JAVA_HOME/bin/javac" -Xlint:none -nowarn -Xstdout "$classes.javac.log" \
  -d "$classes" -cp "$cp" "@$classes.sources"; then
  echo "capture.sh: compile failed" >&2
  exit 1
fi

if [ "${1:-}" = "test" ]; then
  # Every *Test class under capture/src/test/java is a harness unit test.
  tests="$(cd "$here/capture/src/test/java" && find . -name '*Test.java' \
    | sed -e 's|^\./||' -e 's|\.java$||' -e 's|/|.|g' | sort | tr '\n' ' ')"
  "$java" -cp "$classes:$cp" org.junit.runner.JUnitCore $tests
  exit 0
fi

jvm_flags=(-XX:+EnableDynamicAgentLoading -Djdk.attach.allowAttachSelf=true
  --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
  --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED --enable-native-access=ALL-UNNAMED)

if [ "${1:-}" = "harvest" ]; then
  # Dynamic harvest (#1391 step 2): run the pinned Zeebe engine tests of the
  # packages in harvest/harvest.json and turn each passing test's deployment +
  # client commands into scenarios/h-*/. processos lays out models whose
  # builder output lacks DI.
  shift
  if [ -z "${PROCESSOS_BIN:-}" ]; then
    (cd "$here/../../processos" && cargo build -q --bin processos)
    export PROCESSOS_BIN="$here/../../processos/target/debug/processos"
  fi
  "$java" "${jvm_flags[@]}" -cp "$classes:$cp" io.nanobpm.traces.Harvest \
    "$here" "$src/zeebe/engine/target/test-classes" "$@"
  # Capture every scenario (hand-written + harvested) from the committed XML.
  "$java" "${jvm_flags[@]}" -cp "$classes:$cp" io.nanobpm.traces.Capture "$here" "$sha"
  exit 0
fi

"$java" -XX:+EnableDynamicAgentLoading -Djdk.attach.allowAttachSelf=true \
  --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED --enable-native-access=ALL-UNNAMED \
  -cp "$classes:$cp" io.nanobpm.traces.Capture "$here" "$sha" "$@"
