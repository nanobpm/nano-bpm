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
	| grep -vP '^\d+:\t(\S+\s+)*\$\((if|else|endif|wildcard|shell|patsubst|lastword|abspath|dir|filter)\b' \
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

# --- 2. Functional probe: run fmt-check from a directory whose path has a ----
# --- space. Opt-in: needs the pinned nightly rustfmt + codegen stubs.      ---
if [ "${NANOBPMN_SPACED_PROBE:-0}" = 1 ]; then
	probe_parent="$(mktemp -d "${TMPDIR:-/tmp}/nanobpmn spaced probe.XXXXXX")"
	trap 'rm -rf "${probe_parent}"' EXIT
	probe="${probe_parent}/checkout with space"
	mkdir -p "${probe}"
	# A functional fmt-check needs the real crate sources (rustfmt parses the
	# module trees). Copy the formatted crates + rustfmt.toml; stub the codegen
	# inputs exactly as the CI fmt job does.
	cp "${ROOT}/rustfmt.toml" "${probe}/"
	for d in engine-core server processos engine-wasm read-model; do
		mkdir -p "${probe}/${d}"
		cp "${ROOT}/${d}/Cargo.toml" "${probe}/${d}/"
		# src trees: needed so rustfmt resolves the module tree.
		[ -d "${ROOT}/${d}/src" ] && cp -R "${ROOT}/${d}/src" "${probe}/${d}/"
	done
	cp "${ROOT}/Makefile" "${probe}/"
	mkdir -p "${probe}/scripts"
	cp "${ROOT}/scripts/fmt-stub-codegen.sh" "${probe}/scripts/"
	(
		cd "${probe}"
		./scripts/fmt-stub-codegen.sh
		make fmt-check
	)
	echo "spaced-path-guard: fmt-check passed from '${probe}'"
else
	echo "spaced-path-guard: textual guard passed (set NANOBPMN_SPACED_PROBE=1 for the functional fmt-check probe)"
fi
