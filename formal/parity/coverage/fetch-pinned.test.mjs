// Behaviour of fetch-pinned.sh (#2079): a verified cached jar makes no network
// request; 429/5xx back off (honouring Retry-After) then succeed; a dead source
// falls through to the mirror; a SHA mismatch never enters the cache; a total
// failure names the artifact and the HTTP status. `curl` and `sleep` are stubs.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const lib = fileURLToPath(new URL('./fetch-pinned.sh', import.meta.url));
const BODY = 'jar-bytes';
const SHA = createHash('sha256').update(BODY).digest('hex');

function run(plan, { cached = false, corrupt = false, budget } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'fetch-pinned-'));
  writeFileSync(join(dir, 'plan'), plan.join('\n') + '\n');
  const curl = join(dir, 'curl');
  writeFileSync(curl, `#!/usr/bin/env bash
n=$(cat "$STUB_DIR/n" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$STUB_DIR/n"
echo "\${@: -1}" >> "$STUB_DIR/calls"
read -r code ra cost < <(sed -n "\${n}p" "$STUB_DIR/plan")
[[ "$ra" == - ]] && ra=
echo $(( $(cat "$STUB_DIR/clock" 2>/dev/null || echo 0) + \${cost:-0} )) > "$STUB_DIR/clock"
while [[ $# -gt 0 ]]; do
  [[ "$1" == --max-time ]] && echo "$2" >> "$STUB_DIR/maxtimes"
  case "$1" in -D) hdr="$2"; shift;; -o) out="$2"; shift;; esac; shift
done
{ echo "HTTP/1.1 $code"; [[ -n "$ra" ]] && echo "Retry-After: $ra"; } | sed 's/$/\\r/' > "$hdr"
if [[ "$code" == bad ]]; then printf bad > "$out"; printf 200; exit 0; fi
[[ "$code" == 200 ]] && printf '${BODY}' > "$out"
[[ "$code" == 000 ]] && exit 7
printf '%s' "$code"
`);
  chmodSync(curl, 0o755);
  const sleep = join(dir, 'fake-sleep');
  writeFileSync(sleep, '#!/usr/bin/env bash\necho "$1" >> "$STUB_DIR/sleeps"\necho $(( $(cat "$STUB_DIR/clock" 2>/dev/null || echo 0) + $1 )) > "$STUB_DIR/clock"\n');
  chmodSync(sleep, 0o755);
  const clock = join(dir, 'fake-clock');
  writeFileSync(clock, '#!/usr/bin/env bash\ncat "$STUB_DIR/clock" 2>/dev/null || echo 0\n');
  chmodSync(clock, 0o755);
  const dest = join(dir, 'cache', 'a.jar');
  if (cached || corrupt) { spawnSync('mkdir', ['-p', join(dir, 'cache')]); writeFileSync(dest, corrupt ? 'stale' : BODY); }
  const r = spawnSync('bash', ['-c', `source "${lib}"; fetch_pinned a.jar "$DEST" ${SHA} https://one/a.jar https://two/a.jar`], {
    encoding: 'utf8',
    env: { ...process.env, PATH: `${dir}:${process.env.PATH}`, STUB_DIR: dir, DEST: dest,
      FETCH_PINNED_SLEEP: sleep, FETCH_PINNED_NOW: clock, ...(budget ? { FETCH_PINNED_BUDGET_S: String(budget) } : {}) },
  });
  const read = (f) => (existsSync(join(dir, f)) ? readFileSync(join(dir, f), 'utf8').trim().split('\n') : []);
  return { ...r, calls: read('calls'), maxtimes: read('maxtimes').map(Number), sleeps: read('sleeps').map(Number), dest };
}

test('a verified cached jar skips the network fetch', () => {
  const r = run([], { cached: true });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(r.calls, []);
});

test('a corrupt cached jar is replaced by a verified download', () => {
  const r = run(['200'], { corrupt: true });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(readFileSync(r.dest, 'utf8'), BODY);
});

test('429 honours Retry-After, 5xx/connection errors back off exponentially', () => {
  const r = run(['429 30', '503', '000', '200']);
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(r.sleeps, [30, 10, 20]);
  assert.match(r.stderr, /a\.jar: https:\/\/one\/a\.jar returned HTTP 429/);
  assert.deepEqual(r.calls, Array(4).fill('https://one/a.jar'));
});

test('a 404 or exhausted backoff falls through to the mirror', () => {
  const r = run(['404', '200']);
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(r.calls, ['https://one/a.jar', 'https://two/a.jar']);
  const b = run(['429', '429', '200'], { budget: 5 });
  assert.equal(b.status, 0, b.stderr);
  assert.deepEqual(b.calls, ['https://one/a.jar', 'https://one/a.jar', 'https://two/a.jar']);
});

test('a SHA mismatch never enters the cache; total failure names artifact + status', () => {
  const r = run(['bad', '429 999'], { budget: 1 });
  assert.notEqual(r.status, 0);
  assert.equal(existsSync(r.dest), false);
  assert.match(r.stderr, /does not match the pinned SHA-256/);
  assert.match(r.stderr, /error: could not fetch a\.jar \(last HTTP status: 429\)/);
});

test('a Retry-After beyond the budget is never shortened: the source is abandoned for the mirror', () => {
  const r = run(['429 600', '200'], { budget: 300 });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(r.sleeps, []);
  assert.deepEqual(r.calls, ['https://one/a.jar', 'https://two/a.jar']);
  assert.match(r.stderr, /a\.jar: https:\/\/one\/a\.jar returned HTTP 429 with Retry-After 600s/);
});

test('the per-source budget counts request time, and bounds each attempt', () => {
  // Each failing request burns 100s of (fake) wall clock; the backoff sleeps add 5, then 10.
  const r = run(['503 - 100', '503 - 100', '503 - 100', '200'], { budget: 300 });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(r.calls, ['https://one/a.jar', 'https://one/a.jar', 'https://one/a.jar', 'https://two/a.jar']);
  // elapsed before each attempt: 0, 105, 215 -> --max-time 300, 195, 85; the mirror starts afresh.
  assert.deepEqual(r.maxtimes, [300, 195, 85, 300]);
  assert.deepEqual(r.sleeps, [5, 10]);
});
