#!/usr/bin/env bash
#
# bootstrap-build-host.sh — idempotently provision a Linux build host with the
# full nanobpmn build toolchain, so a bare `git clone` can produce a deployable
# gateway binary (generated REST crate + embedded console) with no manual setup.
#
# Idempotent: every step is guarded by a capability check, so re-running is safe
# and fast. Designed for Debian/Ubuntu (apt) on x86_64 — e.g. the GCP soak nodes.
#
# Installs (skipping anything already present):
#   * apt build deps  — build-essential, pkg-config, libssl-dev, git, curl, unzip, ca-certificates
#   * Java (JRE 17)   — runs the pinned openapi-generator-cli JAR (needs Java 11+)
#   * Rust (rustup)   — cargo/rustc toolchain for the gateway + generated crate
#   * uv              — locked Python env (PyYAML) for the spec-preprocessing helpers
#   * Node.js 20 + npm — builds the console SPA (console/ -> console/dist via Vite)
#   * wasm-pack       — builds the in-browser engine, engine-wasm/pkg (needed by `make console-wasm`)
#
# After this, `make setup && make release` (or scripts/build-tagged.sh) works from a clean clone.
#
# Usage: scripts/bootstrap-build-host.sh [--no-node] [--no-wasm-pack]
set -euo pipefail

NODE_MAJOR=20
WANT_NODE=1
WANT_WASM_PACK=1
while [ $# -gt 0 ]; do
  case "$1" in
    --no-node)      WANT_NODE=0; shift ;;
    --no-wasm-pack) WANT_WASM_PACK=0; shift ;;
    -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  have sudo || { echo "error: need root or sudo" >&2; exit 1; }
  SUDO="sudo"
fi

if ! have apt-get; then
  echo "error: this bootstrap targets Debian/Ubuntu (apt-get not found)." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 1. apt build dependencies
# ---------------------------------------------------------------------------
APT_PKGS="build-essential pkg-config libssl-dev git curl unzip ca-certificates"
missing_apt=""
for p in $APT_PKGS; do dpkg -s "$p" >/dev/null 2>&1 || missing_apt="$missing_apt $p"; done
if [ -n "$missing_apt" ]; then
  log "installing apt packages:$missing_apt"
  $SUDO apt-get update -qq
  # shellcheck disable=SC2086
  $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y $missing_apt >/dev/null
else
  log "apt build deps already present"
fi

# ---------------------------------------------------------------------------
# 2. Java (JRE 17, headless) — for openapi-generator-cli
# ---------------------------------------------------------------------------
if have java; then
  log "java present: $(java -version 2>&1 | head -1)"
else
  log "installing default-jre-headless"
  $SUDO apt-get update -qq
  $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y default-jre-headless >/dev/null
fi

# ---------------------------------------------------------------------------
# 3. Rust toolchain (rustup)
# ---------------------------------------------------------------------------
if [ -f "$HOME/.cargo/env" ]; then
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi
if have cargo; then
  log "rust present: $(cargo --version)"
else
  log "installing rust via rustup"
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi

# ---------------------------------------------------------------------------
# 4. uv (Python env for build helpers)
# ---------------------------------------------------------------------------
export PATH="$HOME/.local/bin:$PATH"
if have uv; then
  log "uv present: $(uv --version)"
else
  log "installing uv"
  curl -fsSL https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

# ---------------------------------------------------------------------------
# 5. Node.js + npm (console SPA)
# ---------------------------------------------------------------------------
if [ "$WANT_NODE" = 1 ]; then
  node_ok=0
  if have node; then
    cur=$(node --version | sed 's/^v//; s/\..*//')
    [ "${cur:-0}" -ge 18 ] && node_ok=1
  fi
  if [ "$node_ok" = 1 ]; then
    log "node present: $(node --version) (npm $(npm --version 2>/dev/null))"
  else
    log "installing Node.js ${NODE_MAJOR}.x via NodeSource"
    curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | $SUDO -E bash - >/dev/null
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs >/dev/null
    log "node installed: $(node --version) (npm $(npm --version))"
  fi
else
  log "skipping Node.js (--no-node); console SPA cannot be rebuilt on this host"
fi

# ---------------------------------------------------------------------------
# 6. wasm-pack (needed by `make console-wasm`; engine-wasm/pkg is not committed, #668)
# ---------------------------------------------------------------------------
if [ "$WANT_WASM_PACK" = 1 ]; then
  if have wasm-pack; then
    log "wasm-pack present: $(wasm-pack --version)"
  else
    log "installing wasm-pack (make install-wasm-pack: the pinned WASM_PACK_VERSION)"
    make -C "$(cd "$(dirname "$0")/.." && pwd)" install-wasm-pack >/dev/null 2>&1 || echo "warning: wasm-pack install failed; 'make console-wasm' (engine-wasm/pkg) will not build"
  fi
else
  log "skipping wasm-pack (--no-wasm-pack); 'make console-wasm' (engine-wasm/pkg) will not build"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log "toolchain summary:"
printf '    cargo     %s\n' "$(cargo --version 2>/dev/null || echo MISSING)"
printf '    rustc     %s\n' "$(rustc --version 2>/dev/null || echo MISSING)"
printf '    java      %s\n' "$(java -version 2>&1 | head -1 || echo MISSING)"
printf '    uv        %s\n' "$(uv --version 2>/dev/null || echo MISSING)"
printf '    node      %s\n' "$(node --version 2>/dev/null || echo 'skipped/MISSING')"
printf '    npm       %s\n' "$(npm --version 2>/dev/null || echo 'skipped/MISSING')"
printf '    wasm-pack %s\n' "$(wasm-pack --version 2>/dev/null || echo 'skipped/MISSING')"
log "build host ready. Next: make setup && make release  (or scripts/build-tagged.sh <ref>)"
