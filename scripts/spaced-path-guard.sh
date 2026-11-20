#!/usr/bin/env bash
#
# spaced-path-guard.sh — guard for issue #1716: a checkout path containing a
# space must not break the build entry points agents run.
#
# The defect class: Makefile recipes expand path-derived variables
# ($(PROJECT_ROOT), $(FMT_CRATES), …) UNQUOTED into shell words, so a checkout
# under a spaced path (macOS agent workspaces live under
# `~/Library/Application Support/c8ctl-nano/…`) word-splits at the space and
# `make fmt` / `make fmt-check` / `make console-wasm` fail with misleading
# "no such file or directory" errors. tikv-jemalloc-sys's autoconf configure
# splits the same way on a spaced CARGO_TARGET_DIR, which is why the Makefile
# and scripts/new-worktree.sh redirect the target dir to a space-free path.
#
# This script fails (red) when:
#   1. the Makefile still contains an unquoted path-derived shell word in a
#      recipe (the textual defect class — word-splitting waiting to happen), or
#   2. `make -C <dir with a space> fmt-check` does not work end-to-end
#      (functional probe; skipped unless NANOBPMN_SPACED_PROBE=1, because it
#      needs the pinned rustfmt toolchain and the codegen stubs).
#
# Run from anywhere; it locates the repo from its own path.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MAKEFILE="${ROOT}/Makefile"

fail() { echo "spaced-path-guard: FAIL: $*" >&2; exit 1; }

[ -f "${MAKEFILE}" ] || fail "no Makefile at ${MAKEFILE}"

# --- 1. Textual guard: no unquoted path-derived shell word in a recipe -------
#
# Recipe lines (start with a TAB) must never expand a path-carrying make
# variable bare. The variables below all derive from $(PROJECT_ROOT) — the
# checkout directory — so a space in it word-splits the shell command. Inside
# double quotes ("$(VAR)") or make-only contexts (prerequisite lists, $(shell),
# $(wildcard), echo text) the expansion is safe; those are not matched.
#
# Matched (bad):
# 	cd $(PROJECT_ROOT)/server && cargo build
# 	(cd $$d && …)                      — with $$d iterating over $(FMT_CRATES)
# 	node $(ENGINE_DIR)/scripts/x.mjs
# Not matched (safe):
# 	cd "$(PROJECT_ROOT)/server" && cargo build
# 	VAR := $(PROJECT_ROOT)/x            (make-level assignment, not a shell line)
# 	target: $(GENERATED_DIR)/Cargo.toml (prerequisite list, not a shell line)
# 	@echo "built: $(PROJECT_ROOT)/…"    (inside quotes)
bad_vars='PROJECT_ROOT|GENERATED_DIR|CONSOLE_GENERATED_DIR|STUB_IMPLS|RESPONSE_CONTRACT|ENGINE_DIR|CONSOLE_DIR|PROCESSOS_DIR|WASM_DIR|READ_MODEL_DIR'
# Recipe lines only: TAB-indented, but not a make-level assignment (those are
# often TAB-continued variable definitions, e.g. REST_SPEC_SRCS) and not a
# make directive (ifeq/$(if)/… at line start are make-expanded, not shell).
in_recipe=$(grep -nP '^\t' "${MAKEFILE}" \
	| grep -vP '^\d+:\t\S+\s*[:+?]?=' \
	| grep -vP '^\d+:\t(\S+\s+)*\$\((if|else|endif|wildcard|shell|patsubst|lastword|abspath|dir|filter|call|foreach|subst|findstring)\b' \
	| grep -vP '^\d+:\t\S+/\S+\.(yaml|yml|sh|py|mjs|json)\b' \
	|| true)
# A $(VAR) is unsafe only when some shell word character follows it before the
# closing quote of its string. Scanning char-by-char: track quote state; flag a
# bare expansion only when the next `"` on the line opens a string (i.e. the
# expansion sits outside quotes).
offenders=$(printf '%s\n' "${in_recipe}" | perl -ne '
		my $line = $_; chomp $line;
		my $in_dq = 0; my $bad = 0;
		my @chars = split //, $line;
		for (my $i = 0; $i < @chars; $i++) {
			my $c = $chars[$i];
			if ($c eq "\"") { $in_dq = !$in_dq; next; }
			if (!$in_dq && $c eq "\$" && $i + 1 < @chars && $chars[$i+1] eq "(") {
				my $rest = substr($line, $i);
				if ($rest =~ /^\$\((PROJECT_ROOT|GENERATED_DIR|CONSOLE_GENERATED_DIR|STUB_IMPLS|RESPONSE_CONTRACT|ENGINE_DIR|CONSOLE_DIR|PROCESSOS_DIR|WASM_DIR|READ_MODEL_DIR)\)/) { $bad = 1; last; }
			}
		}
		print "$line\n" if $bad;
	' || true)
if [ -n "${offenders}" ]; then
	echo "spaced-path-guard: unquoted path-derived shell words in Makefile recipes:" >&2
	echo "${offenders}" >&2
	fail 'quote every path expansion ("$(PROJECT_ROOT)/…", cd "$$d") — a checkout path with a space word-splits these (#1716)'
fi

# The fmt/fmt-check loops iterate over $(FMT_CRATES) into $$d and cd into it;
# that cd must be quoted too.
if grep -nP '^\t' "${MAKEFILE}" | grep -qP '\bcd \$\$d\b'; then
	fail "fmt/fmt-check loop uses 'cd \$\$d' unquoted — quote it (cd \"\$\$d\") so a spaced FMT_CRATES entry survives (#1716)"
fi

# PROJECT_ROOT must be resolved whitespace-safely via $(CURDIR) (set by make
# with the spaced path intact). The whitespace-sensitive make functions
# $(abspath), $(dir …), and $(lastword $(MAKEFILE_LIST)) word-split a spaced
# checkout path — guard against a regression to any of them.
project_root_line=$(grep -nP '^PROJECT_ROOT\s*:?=' "${MAKEFILE}" || true)
if printf '%s' "${project_root_line}" | grep -qP 'abspath|MAKEFILE_LIST|\$\(dir\b'; then
	fail "PROJECT_ROOT derives from \$(abspath)/\$(dir)/\$(MAKEFILE_LIST), which word-split a spaced checkout path (#1716) — use PROJECT_ROOT := \$(CURDIR) (make sets it with the spaced path intact)"
fi
if ! printf '%s' "${project_root_line}" | grep -qP 'CURDIR'; then
	fail "PROJECT_ROOT is not \$(CURDIR) (#1716) — use PROJECT_ROOT := \$(CURDIR) so a spaced checkout path survives"
fi

# new-worktree.sh must pin the space-free target dir via .cargo/config.toml
# (read by every direct cargo invocation), not a direnv .envrc that nothing
# sources without a configured direnv hook. Match an actual .envrc WRITE
# (> …/.envrc), not a comment mentioning the word.
NEW_WORKTREE="${ROOT}/scripts/new-worktree.sh"
if [ -f "${NEW_WORKTREE}" ] && grep -qP '>\s*\S*\.envrc' "${NEW_WORKTREE}"; then
	fail "scripts/new-worktree.sh writes a direnv .envrc for CARGO_TARGET_DIR — nothing sources it, so direct cargo runs still build under the spaced worktree (#1716). Pin [build] target-dir in the worktree's .cargo/config.toml instead"
fi

# --- 2. Functional probe: run fmt-check from a directory whose path has a ----
# --- space. Opt-in: needs the pinned nightly rustfmt + codegen stubs.      ---
if [ "${NANOBPMN_SPACED_PROBE:-0}" = 1 ]; then
	command -v git >/dev/null 2>&1 || fail "probe needs git"
	probe_parent="$(mktemp -d "${TMPDIR:-/tmp}/nanobpmn spaced probe.XXXXXX")"
	trap 'rm -rf "${probe_parent}"' EXIT
	probe="${probe_parent}/checkout with space"
	mkdir -p "${probe}"
	# A functional fmt-check needs the real crate sources (rustfmt parses the
	# module trees) and the Makefile. Copy the formatted crates + rustfmt.toml;
	# the codegen inputs are stubbed below exactly as the CI fmt job does.
	cp "${ROOT}/rustfmt.toml" "${probe}/"
	cp "${ROOT}/Makefile" "${probe}/"
	mkdir -p "${probe}/scripts"
	cp "${ROOT}/scripts/fmt-stub-codegen.sh" "${probe}/scripts/"
	for d in engine-core server processos engine-wasm read-model; do
		mkdir -p "${probe}/${d}"
		cp "${ROOT}/${d}/Cargo.toml" "${probe}/${d}/"
		[ -f "${ROOT}/${d}/Cargo.lock" ] && cp "${ROOT}/${d}/Cargo.lock" "${probe}/${d}/" || true
		[ -d "${ROOT}/${d}/src" ] && cp -R "${ROOT}/${d}/src" "${probe}/${d}/"
		[ -d "${ROOT}/${d}/crates" ] && cp -R "${ROOT}/${d}/crates" "${probe}/${d}/"
		[ -d "${ROOT}/${d}/tests" ] && cp -R "${ROOT}/${d}/tests" "${probe}/${d}/"
		[ -d "${ROOT}/${d}/examples" ] && cp -R "${ROOT}/${d}/examples" "${probe}/${d}/"
		[ -d "${ROOT}/${d}/benches" ] && cp -R "${ROOT}/${d}/benches" "${probe}/${d}/"
	done
	# fmt-stub-codegen.sh locates the repo root via git, so the probe dir must
	# be a git work tree.
	git -C "${probe}" init -q
	if [ "${NANOBPMN_SPACED_DEBUG:-0}" = 1 ]; then
		echo "--- probe Makefile fmt-check recipe ---" >&2
		make -C "${probe}" -n fmt-check 2>/dev/null | grep -A3 'for d in' >&2 || true
		echo "--- end ---" >&2
	fi
	(
		cd "${probe}"
		./scripts/fmt-stub-codegen.sh
		# Exercise the supported invocation against the spaced path (#1716):
		# `make -C <spaced dir>` (make chdirs, so $(CURDIR) is the spaced
		# project root). `cd "${probe}" && make fmt-check` is the same thing.
		make -C "${probe}" fmt-check
	)
	echo "spaced-path-guard: fmt-check passed from '${probe}' (make -C)"
else
	echo "spaced-path-guard: textual guard passed (set NANOBPMN_SPACED_PROBE=1 for the functional fmt-check probe)"
fi
