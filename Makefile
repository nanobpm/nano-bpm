# nanobpmn — Orchestration Cluster REST layer code generation (Rust / rust-axum).
#
# Quick start:
#   make setup       — check the toolchain (rust, uv, java), provision the Python
#                      build env via `uv sync`, then generate the REST stubs.
#   make all         — build nano top-to-bottom: generate the REST stubs, then
#                      compile the gateway (server) and ProcessOS (debug).
#   make all-release — same, but build optimized release binaries of the gateway
#                      (nano) and ProcessOS.
#   make release     — the optimized self-contained distribution (gateway with the
#                      embedded web console + Swagger).
#   make debug       — the same full self-contained stack as `make release`, but an
#                      unoptimized debug build — far faster to compile, for a tight
#                      local iteration loop.
#
# `make generate` produces the Rust REST layer + server stubs from spec/; the
# output under generated/ and server/src/stub_impls.rs is git-ignored.

SHELL := /usr/bin/env bash
PROJECT_ROOT := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
GENERATED_DIR := $(PROJECT_ROOT)/generated
CONSOLE_GENERATED_DIR := $(PROJECT_ROOT)/generated-console
STUB_IMPLS := $(PROJECT_ROOT)/server/src/stub_impls.rs
# Spec-generated response-contract bundle (issue #1011); git-ignored like the
# stub, regenerated from spec/ by scripts/gen-response-contract.py.
RESPONSE_CONTRACT := $(PROJECT_ROOT)/server/src/response_contract_schema.json
ENGINE_DIR := $(PROJECT_ROOT)/engine-core
CONSOLE_DIR := $(PROJECT_ROOT)/console
PROCESSOS_DIR := $(PROJECT_ROOT)/processos
WASM_DIR := $(PROJECT_ROOT)/engine-wasm
READ_MODEL_DIR := $(PROJECT_ROOT)/read-model
UV := uv

# Spec inputs for the generated crates. Listing these as prerequisites of the
# codegen targets makes `make` regenerate when the spec (or its generator
# config/patches/script) changes — otherwise, once generated/ exists, make
# treats it as up to date forever and a spec change silently leaves the crate
# stale (e.g. new endpoints added to spec-console/console-api.yaml fail the
# release build with "cannot find type ...PathParams").
REST_SPEC_SRCS := $(wildcard $(PROJECT_ROOT)/spec/*.yaml) \
	$(wildcard $(PROJECT_ROOT)/spec-patches/*.yaml) \
	$(PROJECT_ROOT)/openapi-generator-config.yaml \
	$(PROJECT_ROOT)/scripts/generate.sh
CONSOLE_SPEC_SRCS := $(PROJECT_ROOT)/spec-console/console-api.yaml \
	$(PROJECT_ROOT)/openapi-generator-config-console.yaml \
	$(PROJECT_ROOT)/scripts/generate-console.sh

# Canonical formatter: pinned nightly rustfmt. The repo's rustfmt.toml uses
# unstable options (group_imports), which stable `cargo fmt` silently ignores —
# producing import-ordering drift. Always format via this toolchain so the tree
# matches the `make fmt-check` CI gate. Bump deliberately (single style commit).
FMT_TOOLCHAIN := nightly-2026-06-26
FMT_CRATES := $(ENGINE_DIR) $(PROJECT_ROOT)/server $(PROCESSOS_DIR) $(WASM_DIR) $(READ_MODEL_DIR)

.DEFAULT_GOAL := build

.PHONY: setup
setup: check-deps git-merge-drivers ## Verify the toolchain, provision the Python build env (uv sync), then generate the REST stubs — run this first
	$(UV) sync
	$(MAKE) generate
	@echo
	@echo "Setup complete. Build everything with: make all"

.PHONY: git-merge-drivers
git-merge-drivers: ## Register the `keep-ours` merge driver used by .gitattributes for checked-in derived artifacts (engine-wasm/pkg, processos/assets/ir.gbnf). Idempotent; no-op outside a git work tree.
	@if [ "$$(git rev-parse --is-inside-work-tree 2>/dev/null)" = true ]; then \
	  git config merge.keep-ours.driver true && \
	  git config merge.keep-ours.name "keep the current side (derived artifact; regenerate + drift-guard is the source of truth)" && \
	  echo "  registered git merge driver 'keep-ours' (see .gitattributes)"; \
	fi

.PHONY: check-deps
check-deps: ## Check that the required toolchain is installed (rust, uv, java) and report optional tools
	@missing=0; \
	chk() { \
	  if command -v "$$1" >/dev/null 2>&1; then \
	    printf '  \033[32m✓\033[0m %-10s %s\n' "$$1" "$$($$2 2>&1 | head -n1)"; \
	  else \
	    printf '  \033[31m✗\033[0m %-10s MISSING — %s\n' "$$1" "$$3"; \
	    [ "$$4" = required ] && missing=1 || true; \
	  fi; \
	}; \
	echo "Required:"; \
	chk cargo "cargo --version" "install Rust via https://rustup.rs" required; \
	chk uv "uv --version" "install uv: https://docs.astral.sh/uv/getting-started/installation/" required; \
	chk java "java -version" "install a JRE/JDK 11+ for the OpenAPI generator" required; \
	echo "Optional (web console / wasm):"; \
	chk node "node --version" "install Node.js to build the console SPA" optional; \
	chk npm "npm --version" "bundled with Node.js" optional; \
	chk wasm-pack "wasm-pack --version" "cargo install wasm-pack (else committed wasm artifacts are used)" optional; \
	if [ "$$missing" -ne 0 ]; then \
	  echo; echo "error: required tools are missing (see ✗ above). Install them and re-run 'make setup'."; \
	  exit 1; \
	fi

.PHONY: all
all: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) ## Build nano top-to-bottom: generate REST stubs, then compile the gateway (nano) and ProcessOS
	cd $(GENERATED_DIR) && cargo build
	cd $(PROJECT_ROOT)/server && cargo build
	cd $(PROCESSOS_DIR) && cargo build
	@echo
	@echo "Built nano top-to-bottom: generated REST crate + gateway (server) + ProcessOS."

.PHONY: all-release
all-release: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) ## Build optimized release binaries of nano (gateway) and ProcessOS top-to-bottom
	cd $(GENERATED_DIR) && cargo build --release
	cd $(PROJECT_ROOT)/server && cargo build --release
	cd $(PROCESSOS_DIR) && cargo build --release
	@echo
	@echo "Built release binaries:"
	@echo "  gateway (nano): $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"
	@echo "  ProcessOS:      $(PROCESSOS_DIR)/target/release/processos"

.PHONY: generate
generate: ## Generate the Rust REST layer + server stub impls from spec/ (needs local Java)
	./scripts/generate.sh
	./scripts/generate-console.sh

.PHONY: generate-app-manifest
generate-app-manifest: ## Generate the Urban App manifest TypeScript types from spec-app/nano-app.schema.json (ADR 0027; needs Node)
	./scripts/generate-app-manifest.sh

$(GENERATED_DIR)/Cargo.toml: $(REST_SPEC_SRCS)
	$(MAKE) generate

$(CONSOLE_GENERATED_DIR)/Cargo.toml: $(CONSOLE_SPEC_SRCS)
	./scripts/generate-console.sh

# stub_impls.rs is generated (git-ignored) and wires each REST trait method to
# its hand-written `*_impl` on ServerImpl via the delegation map in
# scripts/gen-stub-server.py. Depend on that script so a changed delegation map
# regenerates the stub: otherwise a stale local copy leaves newly wired `*_impl`
# methods reachable only from tests, and since `make release` builds the bin
# alone (no --all-targets), `warnings = "deny"` rejects them as dead code — a
# failure CI never sees because it regenerates the stub and compiles tests.
# When the generated apis already exist this is a fast, Java-free re-run of just
# the Python stub step; otherwise fall back to a full `make generate`.
#
# Placeholder guard (FORCE idiom). A leaked `// fmt stub` one-liner placeholder
# defeats a plain mtime rule: that placeholder is written deliberately by the CI
# fmt job and the pre-push hook so rustfmt can resolve the `mod stub_impls` tree
# without full codegen, and if it is left behind (e.g. a push interrupted
# mid-hook) it carries a FRESH mtime — newer than both prerequisites. Make would
# then treat stub_impls.rs as up to date and skip regeneration, so the build
# compiles the placeholder and fails with "the trait `apis::*` is not implemented
# for `ServerImpl`" for every REST tag (a failure CI never sees: its build jobs
# regenerate from a clean checkout).
#
# An order-only prune (`| prune`) does NOT fix this: Make decides the target is up
# to date from the placeholder's fresh mtime and never runs the recipe, so a prune
# that deletes the file mid-pass just leaves it missing and unregenerated. Instead
# the recipe ALWAYS runs (via the phony FORCE-stub-check prerequisite) and decides
# for itself whether to regenerate — on a missing target, a placeholder (no
# `for ServerImpl` impl), or a genuinely newer prerequisite (a changed delegation
# map / spec must still take effect). A real, current stub is left untouched, so
# its mtime is preserved and dependent server objects are not needlessly recompiled.
$(STUB_IMPLS): scripts/gen-stub-server.py $(GENERATED_DIR)/Cargo.toml FORCE-stub-check
	@need=0; \
	if [ ! -f "$(STUB_IMPLS)" ]; then need=1; \
	elif ! grep -q "for ServerImpl" "$(STUB_IMPLS)" 2>/dev/null; then need=1; \
	elif [ scripts/gen-stub-server.py -nt "$(STUB_IMPLS)" ]; then need=1; \
	elif [ "$(GENERATED_DIR)/Cargo.toml" -nt "$(STUB_IMPLS)" ]; then need=1; \
	fi; \
	if [ $$need -eq 1 ]; then \
		if [ -d "$(GENERATED_DIR)/src/apis" ]; then \
			echo "Regenerating $(STUB_IMPLS) from $(GENERATED_DIR)/src/apis"; \
			python3 scripts/gen-stub-server.py "$(GENERATED_DIR)/src/apis" "$(STUB_IMPLS)"; \
			command -v rustfmt >/dev/null 2>&1 && rustfmt --edition 2024 "$(STUB_IMPLS)" || true; \
		else \
			$(MAKE) generate; \
		fi; \
	fi

# Empty phony that forces the $(STUB_IMPLS) recipe to run every build so it can
# content-check the target (see the FORCE idiom explanation on the rule above).
# It updates no file, so it does not by itself trigger any downstream rebuild.
.PHONY: FORCE-stub-check
FORCE-stub-check:

# The response-contract bundle (issue #1011) is derived from the sanitized spec
# under build/spec/ and from gen-stub-server.py's OVERRIDES (the implemented-op
# set). Regenerate when either generator script, the spec, or the generated
# crate is newer — or when the bundle is missing. When build/spec/ isn't present
# yet (never generated) fall back to a full `make generate`, which produces it.
# Uses the FORCE idiom (like $(STUB_IMPLS)) so a missing target always
# regenerates even if a stale mtime would otherwise mark it up to date.
$(RESPONSE_CONTRACT): scripts/gen-response-contract.py scripts/gen-stub-server.py $(REST_SPEC_SRCS) $(GENERATED_DIR)/Cargo.toml FORCE-contract-check
	@need=0; \
	if [ ! -f "$(RESPONSE_CONTRACT)" ]; then need=1; \
	elif [ scripts/gen-response-contract.py -nt "$(RESPONSE_CONTRACT)" ]; then need=1; \
	elif [ scripts/gen-stub-server.py -nt "$(RESPONSE_CONTRACT)" ]; then need=1; \
	elif [ "$(GENERATED_DIR)/Cargo.toml" -nt "$(RESPONSE_CONTRACT)" ]; then need=1; \
	else \
		for spec in $(REST_SPEC_SRCS); do \
			if [ "$$spec" -nt "$(RESPONSE_CONTRACT)" ]; then need=1; break; fi; \
		done; \
	fi; \
	if [ $$need -eq 1 ]; then \
		if [ -d "$(PROJECT_ROOT)/build/spec" ]; then \
			echo "Regenerating $(RESPONSE_CONTRACT) from build/spec"; \
			$(UV) run --project "$(PROJECT_ROOT)" python scripts/gen-response-contract.py \
				"$(PROJECT_ROOT)/build/spec" rest-api.yaml "$(RESPONSE_CONTRACT)"; \
		else \
			$(MAKE) generate; \
		fi; \
	fi

.PHONY: FORCE-contract-check
FORCE-contract-check:

.PHONY: build
build: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) ## Compile the generated crate and the stub server
	cd $(GENERATED_DIR) && cargo build
	cd $(PROJECT_ROOT)/server && cargo build

.PHONY: release
release: $(GENERATED_DIR)/Cargo.toml $(CONSOLE_GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) console-frontend ## Build the optimized self-contained distribution (gateway + embedded console + Swagger)
	@# Force the RustEmbed derive to re-run so the just-built console/dist (which
	@# release builds bake in at compile time) is embedded, even if the gateway
	@# sources are otherwise unchanged.
	touch $(PROJECT_ROOT)/server/crates/nano-server-console/src/lib.rs
	cd $(PROJECT_ROOT)/server && cargo build --release --features console
	@echo "Built self-contained distribution: $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"
	@echo "  landing /  ·  console /console  ·  API docs /swagger  ·  REST /v2"

.PHONY: debug
debug: $(GENERATED_DIR)/Cargo.toml $(CONSOLE_GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) console-frontend ## Full self-contained stack (gateway + embedded console + Swagger) as an UNOPTIMIZED debug build — same as `release` but far faster to compile, for a tight local loop
	@# Force the RustEmbed derive to re-run so the just-built console/dist is
	@# embedded, even if the gateway sources are otherwise unchanged (mirrors
	@# `release`; the embed is a compile-time bake regardless of profile).
	touch $(PROJECT_ROOT)/server/crates/nano-server-console/src/lib.rs
	cd $(PROJECT_ROOT)/server && cargo build --features console
	@echo "Built self-contained debug distribution: $(PROJECT_ROOT)/server/target/debug/nanobpm-gateway-rest-server"
	@echo "  landing /  ·  console /console  ·  API docs /swagger  ·  REST /v2"

.PHONY: release-gateway
release-gateway: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) ## Build the optimized API-only gateway (no web console)
	cd $(PROJECT_ROOT)/server && cargo build --release
	@echo "Built API-only gateway: $(PROJECT_ROOT)/server/target/release/nanobpm-gateway-rest-server"

# ---------------------------------------------------------------------------
# Local cross-compilation of the gateway for Linux and Windows targets — the same
# recipe CI uses in .github/workflows/publish-c8ctl-binaries.yml. Linux uses
# cargo-zigbuild + a glibc floor; Windows uses cargo-xwin (the msvc ABI CI ships,
# which CI builds natively on a windows-2022 runner). So you can produce dev builds
# for a Linux x86-64 box, a Raspberry Pi (ARMv7), or Windows x64 from any host
# without Docker. Needs zig + cargo-zigbuild (Linux) or cargo-xwin (Windows); the
# script checks and prints install hints. Output lands in dist/ (git-ignored).
#
#   make cross-linux-x64      -> Linux x86-64  (x86_64-unknown-linux-gnu)
#   make cross-linux-armv7    -> Raspberry Pi  (armv7-unknown-linux-gnueabihf)
#   make cross-linux-arm64    -> Linux ARM64   (aarch64-unknown-linux-gnu)
#   make cross-linux-armv6    -> ARMv6 (Pi 1/Zero)  (arm-unknown-linux-gnueabihf)
#   make cross-linux          -> x64 + armv7 (the two you test on)
#   make cross-windows        -> Windows x64   (x86_64-pc-windows-msvc)
#   make cross TARGET=<triple> [GLIBC=2.31] [CROSS_ARGS=--no-console]  -> generic
#
# These share `release`'s prerequisites, so the REST layer + embedded console are
# (re)generated first; pass CROSS_ARGS=--no-console for a faster API-only binary
# (which also drops the console prerequisites below, so nothing console is built).
GLIBC ?= 2.31
CROSS_ARGS ?=
# The console prerequisites (built web SPA + generated-console crate) are only
# needed for the default `--features console` build. When CROSS_ARGS opts out
# with --no-console, drop them so an API-only cross-build doesn't build the SPA.
CROSS_CONSOLE_PREREQS := $(CONSOLE_GENERATED_DIR)/Cargo.toml console-frontend
CROSS_PREREQS := $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) \
	$(if $(filter --no-console,$(CROSS_ARGS)),,$(CROSS_CONSOLE_PREREQS))

.PHONY: cross
cross: $(CROSS_PREREQS) ## Cross-compile the gateway for TARGET=<rust-triple> (GLIBC floor default 2.31; extra flags via CROSS_ARGS, e.g. --no-console)
	@test -n "$(TARGET)" || { echo "Set TARGET=<rust-triple>, e.g. make cross TARGET=armv7-unknown-linux-gnueabihf" >&2; exit 2; }
	$(PROJECT_ROOT)/scripts/cross-build.sh "$(TARGET)" --glibc "$(GLIBC)" $(CROSS_ARGS)

.PHONY: cross-linux-x64
cross-linux-x64: $(CROSS_PREREQS) ## Cross-compile the gateway (release; embedded console unless CROSS_ARGS=--no-console) for Linux x86-64 -> dist/
	$(PROJECT_ROOT)/scripts/cross-build.sh x86_64-unknown-linux-gnu --glibc "$(GLIBC)" $(CROSS_ARGS)

.PHONY: cross-linux-armv7
cross-linux-armv7: $(CROSS_PREREQS) ## Cross-compile the gateway (release; embedded console unless CROSS_ARGS=--no-console) for Raspberry Pi ARMv7 -> dist/
	$(PROJECT_ROOT)/scripts/cross-build.sh armv7-unknown-linux-gnueabihf --glibc "$(GLIBC)" $(CROSS_ARGS)

.PHONY: cross-linux-arm64
cross-linux-arm64: $(CROSS_PREREQS) ## Cross-compile the gateway (release; embedded console unless CROSS_ARGS=--no-console) for Linux ARM64 -> dist/
	$(PROJECT_ROOT)/scripts/cross-build.sh aarch64-unknown-linux-gnu --glibc "$(GLIBC)" $(CROSS_ARGS)

.PHONY: cross-linux-armv6
cross-linux-armv6: $(CROSS_PREREQS) ## Cross-compile the gateway (release; embedded console unless CROSS_ARGS=--no-console) for ARMv6 (Pi 1/Zero) -> dist/
	$(PROJECT_ROOT)/scripts/cross-build.sh arm-unknown-linux-gnueabihf --glibc "$(GLIBC)" $(CROSS_ARGS)

.PHONY: cross-linux
cross-linux: cross-linux-x64 cross-linux-armv7 ## Cross-compile the gateway for both Linux x86-64 and Raspberry Pi ARMv7 -> dist/

.PHONY: cross-windows
cross-windows: $(CROSS_PREREQS) ## Cross-compile the gateway (release; embedded console unless CROSS_ARGS=--no-console) for Windows x64 (msvc) -> dist/
	$(PROJECT_ROOT)/scripts/cross-build.sh x86_64-pc-windows-msvc $(CROSS_ARGS)

.PHONY: console-frontend
console-frontend: console-wasm ## Build the web console SPA (console/ -> console/dist)
	cd $(CONSOLE_DIR) && npm install && npm run build

.PHONY: console-wasm
console-wasm: ## Regenerate the in-browser engine package (engine-wasm -> engine-wasm/pkg, the @nanobpm/engine-wasm package). Builds BOTH subpath variants: lean (pkg/lean) and read-model (pkg/readmodel). Needs wasm-pack; the read-model build also needs an LLVM clang with a wasm backend (see below). Falls back to the committed artifacts if wasm-pack is absent.
	@set -e; \
	if command -v wasm-pack >/dev/null 2>&1; then \
		echo "Regenerating engine-wasm/pkg (@nanobpm/engine-wasm) via wasm-pack (lean + read-model)..."; \
		echo "  -> lean  (pkg/lean)"; \
		cd $(PROJECT_ROOT)/engine-wasm; \
		wasm-pack build --target web --release --no-pack --out-dir pkg/lean --out-name nanobpmn_engine; \
		rm -f $(PROJECT_ROOT)/engine-wasm/pkg/lean/.gitignore; \
		echo "  -> read-model (pkg/readmodel, --features read-model)"; \
		$(WASM_READ_MODEL_ENV) wasm-pack build --target web --release --no-pack --out-dir pkg/readmodel --out-name nanobpmn_engine -- --features read-model; \
		rm -f $(PROJECT_ROOT)/engine-wasm/pkg/readmodel/.gitignore; \
		echo "  -> use-after-free DX guard (scripts/inject-free-guard.mjs)"; \
		node $(PROJECT_ROOT)/engine-wasm/scripts/inject-free-guard.mjs $(PROJECT_ROOT)/engine-wasm/pkg/lean/nanobpmn_engine.js $(PROJECT_ROOT)/engine-wasm/pkg/readmodel/nanobpmn_engine.js; \
		cp $(PROJECT_ROOT)/engine-wasm/pkg.package.json $(PROJECT_ROOT)/engine-wasm/pkg/package.json; \
		cp $(PROJECT_ROOT)/engine-wasm/README.md $(PROJECT_ROOT)/engine-wasm/pkg/README.md; \
		echo "  -> readmodel-types (pkg/readmodel-types, derived DTO types)"; \
		mkdir -p $(PROJECT_ROOT)/engine-wasm/pkg/readmodel-types; \
		cp $(PROJECT_ROOT)/engine-wasm/readmodel-types/types.gen.ts $(PROJECT_ROOT)/engine-wasm/pkg/readmodel-types/types.gen.d.ts; \
		cp $(PROJECT_ROOT)/engine-wasm/readmodel-types/index.d.ts $(PROJECT_ROOT)/engine-wasm/pkg/readmodel-types/index.d.ts; \
		cp $(PROJECT_ROOT)/engine-wasm/readmodel-types/index.js $(PROJECT_ROOT)/engine-wasm/pkg/readmodel-types/index.js; \
	else \
		echo "wasm-pack not found; using the committed engine-wasm/pkg artifacts (run 'cargo install wasm-pack' to regenerate)."; \
	fi

# The read-model variant compiles a trimmed SQLite C amalgamation (via the `cc`
# crate + read-model/build.rs) to wasm32. Apple clang and GCC have no wasm
# backend; upstream LLVM clang does. The `cc` crate reads these per-target knobs
# to pick the compiler + archiver. On macOS, default to Homebrew LLVM when the
# caller has not already exported CC_wasm32_unknown_unknown/AR_wasm32_unknown_unknown.
WASM_READ_MODEL_ENV =
ifeq ($(origin CC_wasm32_unknown_unknown),undefined)
ifneq ($(wildcard /opt/homebrew/opt/llvm/bin/clang),)
WASM_READ_MODEL_ENV += CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
endif
endif

.PHONY: console
console: release ## Alias for `release` (the self-contained single-node distribution)

.PHONY: console-dev
console-dev: ## Run the console frontend dev server (Vite); proxies /console/api to a gateway on :8080
	cd $(CONSOLE_DIR) && npm run dev

.PHONY: run
run: $(STUB_IMPLS) $(RESPONSE_CONTRACT) ## Run the stub server (PORT overrides the default 8080)
	cd $(PROJECT_ROOT)/server && cargo run

.PHONY: server-test
server-test: $(GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) ## Test the stub server (incl. journal-replay e2e tests)
	cd $(PROJECT_ROOT)/server && cargo test --workspace

.PHONY: server-test-release
server-test-release: $(GENERATED_DIR)/Cargo.toml $(CONSOLE_GENERATED_DIR)/Cargo.toml $(STUB_IMPLS) $(RESPONSE_CONTRACT) ## Test the stub server with release optimizations (uses the release-test profile to avoid the panic=abort double-compile)
	cd $(PROJECT_ROOT)/server && cargo test --workspace --profile release-test --features nanobpm-gateway-rest-server/console

.PHONY: engine-build
engine-build: ## Build the embeddable BPMN engine-core crate (no Docker, no codegen)
	cd $(ENGINE_DIR) && cargo build

.PHONY: engine-test
engine-test: ## Test the engine-core crate (unit + integration + doctests)
	cd $(ENGINE_DIR) && cargo test

.PHONY: engine-wasm
engine-wasm: ## Build engine-core for wasm32 (portability check; needs the wasm32 target)
	cd $(ENGINE_DIR) && cargo build --target wasm32-unknown-unknown

.PHONY: engine-wasm-ffi
engine-wasm-ffi: ## Build the FFI cdylib for wasm32 and verify its exports (needs the wasm32 target + node)
	cd $(ENGINE_DIR) && cargo build --release --features ffi --target wasm32-unknown-unknown
	node $(ENGINE_DIR)/scripts/verify-wasm-ffi.mjs

.PHONY: engine-wasm-ffi-dist
engine-wasm-ffi-dist: engine-wasm-ffi engine-wasm-agent-instance-e2e ## Emit the release FFI wasm + manifest into dist/engine-wasm-ffi/ (needs wasm-opt/binaryen). Also runs the AgentInstance/AgentHistory end-to-end probe against the committed engine-wasm/pkg (needs node).
	node $(ENGINE_DIR)/scripts/emit-dist.mjs

.PHONY: engine-wasm-agent-instance-e2e
engine-wasm-agent-instance-e2e: ## Run the AgentInstance/AgentHistory end-to-end probe against the committed engine-wasm/pkg read-model TestEngine (needs node). Guards the AgentInstance driver + read methods on the exported wasm surface and their REST parity (status/producedAt/object shapes).
	cd $(WASM_DIR)/tests/agent-instance-e2e && npm install && npm test

.PHONY: engine-wasm-check
engine-wasm-check: ## Type-check the console wasm-bindgen crate for wasm32 (guards the `make release` console-wasm build; needs the wasm32 target)
	cd $(WASM_DIR) && cargo check --target wasm32-unknown-unknown

.PHONY: engine-wasm-check-read-model
engine-wasm-check-read-model: ## Type-check engine-wasm with the off-by-default `read-model` feature for wasm32. Compiles a trimmed SQLite C amalgamation, so it needs the wasm32 target AND an LLVM clang with a wasm backend (Apple/GCC clang have none): point CC_wasm32_unknown_unknown/AR_wasm32_unknown_unknown at llvm clang/llvm-ar (e.g. Homebrew LLVM on macOS, the distro `llvm`/`clang` on CI).
	cd $(WASM_DIR) && $(WASM_READ_MODEL_ENV) cargo check --features read-model --target wasm32-unknown-unknown

.PHONY: release-tags
release-tags: ## Cut release tags for every train whose version is bumped but untagged (dry run; PUSH=1 to tag origin/main HEAD and push). One tag per push + confirms each release workflow started — see RELEASE.md.
	node $(PROJECT_ROOT)/scripts/release-tags.mjs $(if $(PUSH),--push)

.PHONY: release-engine-wasm
release-engine-wasm: ## Cut an @nanobpm/engine-wasm npm release: tag bojtos-npm-v<pkg version> on origin/main HEAD and push it (CI OIDC-publishes). Thin wrapper over scripts/release-tags.mjs.
	node $(PROJECT_ROOT)/scripts/release-tags.mjs --only engine-wasm --push

.PHONY: processos-build
processos-build: ## Build ProcessOS, the separate optimization-plane server (Stage T1: Insights)
	cd processos && cargo build

.PHONY: processos-build-release
processos-build-release: ## Build ProcessOS in release (opt-level=s, lto) -> processos/target/release/processos
	cd processos && cargo build --release

.PHONY: processos-test
processos-test: ## Test the ProcessOS crate
	cd processos && cargo test

.PHONY: processos-run
processos-run: ## Run ProcessOS (PROCESSOS_PORT=8090, NANO_BASE_URL=http://localhost:8080)
	cd processos && cargo run

.PHONY: fmt
fmt: ## Format every hand-written crate with the pinned nightly rustfmt (see FMT_TOOLCHAIN)
	@for d in $(FMT_CRATES); do \
		echo "fmt $$d"; \
		(cd $$d && rustup run $(FMT_TOOLCHAIN) cargo fmt) || exit 1; \
	done

.PHONY: fmt-check
fmt-check: ## Verify formatting with the pinned nightly rustfmt (CI gate; fails on drift)
	@for d in $(FMT_CRATES); do \
		echo "fmt-check $$d"; \
		(cd $$d && rustup run $(FMT_TOOLCHAIN) cargo fmt -- --check) || exit 1; \
	done

.PHONY: console-fmt
console-fmt: ## Format the web console TypeScript/CSS with Prettier (console/)
	cd $(CONSOLE_DIR) && npm run format

.PHONY: console-fmt-check
console-fmt-check: ## Verify console Prettier formatting (CI gate; fails on drift)
	cd $(CONSOLE_DIR) && npm run format:check

.PHONY: install-hooks
install-hooks: ## Activate the tracked git hooks (.githooks) — adds a pre-push rustfmt gate
	git config core.hooksPath .githooks
	@chmod +x .githooks/* 2>/dev/null || true
	@echo "git hooks installed: core.hooksPath -> .githooks (pre-push runs 'make fmt-check')"

.PHONY: clippy
clippy: $(GENERATED_DIR)/Cargo.toml ## Lint the generated crate, the stub server and engine-core
	cd $(GENERATED_DIR) && cargo clippy
	cd $(PROJECT_ROOT)/server && cargo clippy --workspace
	cd $(ENGINE_DIR) && cargo clippy --all-targets -- -D warnings

.PHONY: clean
clean: ## Remove all generated artifacts
	rm -rf $(PROJECT_ROOT)/build $(GENERATED_DIR) $(STUB_IMPLS) $(RESPONSE_CONTRACT) $(PROJECT_ROOT)/server/target $(ENGINE_DIR)/target

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
