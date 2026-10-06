#!/usr/bin/env bash
# JaCoCo agent feasibility probe for spike #1280, task 3 (feeds #1247's
# dual-engine JVM branch coverage).
#
# The full #1247 story attaches the JaCoCo agent to `zeebe/engine` running in the
# live parity runtime. That build is heavy; this probe answers the *environment*
# question that gates it — can a CI runner (a) fetch the JaCoCo agent, (b) attach
# it to a JVM via -javaagent, and (c) produce a coverage exec/report artifact? —
# with a trivial throwaway JVM class, so a NO-GO shows up in seconds, not after a
# full Zeebe build.
#
# Records: agent jar size, exec dump size, report presence. Emits a
# ::JACOCO_FINDINGS:: line the workflow scrapes into the go/no-go summary.
set -euo pipefail

# Pin to the JaCoCo line current with the 8.10 Zeebe toolchain. A spike may bump
# this; it is not a merge-gated artifact.
JACOCO_VERSION="${JACOCO_VERSION:-0.8.12}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "[jacoco] work dir: $WORK (JaCoCo $JACOCO_VERSION)"

base="https://repo1.maven.org/maven2/org/jacoco"
curl -fsSL "$base/org.jacoco.agent/${JACOCO_VERSION}/org.jacoco.agent-${JACOCO_VERSION}-runtime.jar" \
  -o "$WORK/jacocoagent.jar"
curl -fsSL "$base/org.jacoco.cli/${JACOCO_VERSION}/org.jacoco.cli-${JACOCO_VERSION}-nodeps.jar" \
  -o "$WORK/jacococli.jar"

agent_size=$(wc -c < "$WORK/jacocoagent.jar")
echo "[jacoco] agent jar: ${agent_size} bytes"

# Trivial class to exercise the agent's instrumentation + dump path.
cat > "$WORK/Probe.java" <<'JAVA'
public class Probe {
  static int fib(int n) { return n < 2 ? n : fib(n - 1) + fib(n - 2); }
  public static void main(String[] a) { System.out.println("fib(10)=" + fib(10)); }
}
JAVA

javac -d "$WORK/classes" "$WORK/Probe.java"

java -javaagent:"$WORK/jacocoagent.jar=destfile=$WORK/jacoco.exec,output=file" \
  -cp "$WORK/classes" Probe

if [[ ! -s "$WORK/jacoco.exec" ]]; then
  echo "[jacoco] FAIL — no exec dump produced" >&2
  exit 1
fi
exec_size=$(wc -c < "$WORK/jacoco.exec")
echo "[jacoco] exec dump: ${exec_size} bytes"

# Render an XML report to confirm the CLI end of the pipeline works too.
java -jar "$WORK/jacococli.jar" report "$WORK/jacoco.exec" \
  --classfiles "$WORK/classes" \
  --xml "$WORK/report.xml" >/dev/null
report_ok=false
if [[ -s "$WORK/report.xml" ]]; then report_ok=true; fi
echo "[jacoco] report generated: ${report_ok}"

echo "[jacoco] PASS — runner can attach the JaCoCo agent and collect coverage"
printf '::JACOCO_FINDINGS::{"agentJarBytes":%s,"execBytes":%s,"reportGenerated":%s}\n' \
  "$agent_size" "$exec_size" "$report_ok"
