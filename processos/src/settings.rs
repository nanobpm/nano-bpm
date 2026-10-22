//! **Operator settings** — named LLM connection *profiles* plus the Python interpreter,
//! editable from the console instead of only via environment variables.
//!
//! A consultant works across several models/providers (a local llama.cpp at
//! `localhost:8888`, a cloud Anthropic key, a teammate's vLLM, …). Rather than a single
//! flat config, settings hold a list of **[`LlmProfile`]s** and an *active* one; the
//! console can add/edit/delete profiles, switch the active one, and even query an
//! endpoint's `/models` route to pick the model id. The active profile is layered **over**
//! the environment at request time:
//!
//! ```text
//! provider/base_url/model/api_key/...  =  built-in default
//!                                          → PROCESSOS_LLM_* env
//!                                          → active profile (this file)
//!                                          → per-request override
//! ```
//!
//! Persistence lives at `${PROCESSOS_CONFIG_DIR:-${XDG_CONFIG_HOME:-$HOME/.config}}/processos/settings.json`,
//! created `0600` where supported (it may hold an API key, like `~/.aws/credentials`).
//! A legacy single-LLM file (pre-profiles) is migrated into one `default` profile on load.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::harness::{LlmConfig, LlmOverride, ThinkingLevel};
use crate::pyrunner::PyConfig;

/// A named LLM connection. Every connection field is optional: an unset field falls back
/// to the environment / built-in default for that provider.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LlmProfile {
    /// Stable identifier (slug); referenced by `active_profile`.
    pub id: String,
    /// Human-friendly display name.
    pub name: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    /// The model's advertised context window in tokens, learned by querying the endpoint
    /// (`/models` → `meta.n_ctx`/`n_ctx_train`/`context_length`) or, for a running sidecar, the
    /// live `n_ctx` it was launched with. Metadata only — distinct from [`max_tokens`] (the
    /// per-request budget, which may be smaller). Shown on the LLM tile so the operator knows how
    /// much context the model affords.
    #[serde(default)]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub temperature: Option<f32>,
    /// When true, this profile is served by ProcessOS's **local llama.cpp `llama-server`
    /// sidecar**: the supervisor launches `llama-server` for [`model_file`] and serves it at
    /// [`base_url`]'s port. When false it is a plain remote/external endpoint (the original
    /// behaviour).
    #[serde(default)]
    pub sidecar: bool,
    /// The model the sidecar loads: either a Hugging Face `repo[:quant]` spec (e.g.
    /// `unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M`, downloaded/cached under the models dir) or a
    /// local `.gguf` path (absolute, or relative to the models directory).
    #[serde(default)]
    pub model_file: Option<String>,
    /// Extra `llama-server` startup arguments (e.g. `-ngl 99 -c 32768`). The supervisor always
    /// supplies `--host`, `--port` and the model flag; these are appended verbatim.
    #[serde(default)]
    pub sidecar_args: Option<String>,
    /// Enable **MTP (multi-token prediction)** when launching this sidecar: llama-server gets
    /// `--mtp`, using the model's built-in NextN prediction heads for self-speculative decoding.
    /// Only valid for models whose GGUF actually carries those heads — the start handler scans
    /// the GGUF ([`crate::gguf`]) and refuses to launch an MTP config on a non-MTP model.
    #[serde(default)]
    pub mtp: bool,
    /// Coarse reasoning budget (Fast/Medium/Max) for this profile, applied to **both** the
    /// investigative primary and any pairing/partner role this profile fills. `None` leaves the
    /// model's thinking unconstrained.
    #[serde(default)]
    pub thinking_level: Option<ThinkingLevel>,
    /// Starred models float to the top of the cockpit sidecar card layout.
    #[serde(default)]
    pub starred: bool,
}

impl LlmProfile {
    /// Project this profile into a per-request-style [`LlmOverride`] for layering onto
    /// [`LlmConfig`].
    pub fn as_llm_override(&self) -> LlmOverride {
        LlmOverride {
            provider: self.provider.clone(),
            base_url: self.base_url.clone(),
            model: self.model.clone(),
            api_key: self.api_key.clone(),
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            thinking_level: self.thinking_level,
            // Profiles do not carry a grammar today — grammar is per-*turn*, not per-*profile*,
            // because a profile-wide grammar would break tool-call and prose turns. The write
            // path opts in explicitly via a separate override.
            grammar: None,
        }
    }

    /// Apply a partial update. Per field: `None` keeps it, `Some("")`/`Some(0)`/negative
    /// clears it to the env default, any other value sets it. (`name` cannot be cleared.)
    fn apply(&mut self, patch: ProfilePatch) {
        if let Some(v) = patch.name.and_then(non_empty) {
            self.name = v;
        }
        if let Some(v) = patch.provider {
            self.provider = non_empty(v);
        }
        if let Some(v) = patch.base_url {
            self.base_url = non_empty(v);
        }
        if let Some(v) = patch.model {
            self.model = non_empty(v);
        }
        if let Some(v) = patch.api_key {
            self.api_key = non_empty(v);
        }
        if let Some(v) = patch.max_tokens {
            self.max_tokens = (v != 0).then_some(v);
        }
        if let Some(v) = patch.context_window {
            self.context_window = (v != 0).then_some(v);
        }
        if let Some(v) = patch.temperature {
            self.temperature = (v >= 0.0).then_some(v);
        }
        if let Some(v) = patch.sidecar {
            self.sidecar = v;
        }
        if let Some(v) = patch.model_file {
            self.model_file = non_empty(v);
        }
        if let Some(v) = patch.sidecar_args {
            self.sidecar_args = non_empty(v);
        }
        if let Some(v) = patch.mtp {
            self.mtp = v;
        }
        if let Some(v) = patch.thinking_level {
            // Empty/unknown string clears the level (back to unconstrained thinking).
            self.thinking_level = ThinkingLevel::parse(&v);
        }
        if let Some(v) = patch.starred {
            self.starred = v;
        }
    }

    /// The operator's *previously assigned* sidecar port, if any. `None` means "no preference"
    /// (a fresh sidecar profile), in which case ProcessOS picks any free port. Used so a restart
    /// reuses the same port when it is still free, keeping `base_url` stable between runs.
    pub fn preferred_port(&self) -> Option<u16> {
        self.base_url.as_deref().and_then(parse_port)
    }
}

/// Pull the port out of a base URL like `http://host:PORT/v1`.
fn parse_port(url: &str) -> Option<u16> {
    let after_scheme = url.split("//").nth(1).unwrap_or(url);
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    authority.rsplit(':').next().and_then(|p| p.parse().ok())
}

fn non_empty(s: String) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// The persisted settings: the profile list, the active profile id, and the (global)
/// Python interpreter for the analysis escape hatch.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(default)]
    pub profiles: Vec<LlmProfile>,
    #[serde(default)]
    pub active_profile: Option<String>,
    #[serde(default)]
    pub python_bin: Option<String>,
    /// Directory the local llama.cpp sidecar uses for GGUF models / Hugging Face downloads
    /// (exported as `LLAMA_CACHE` to the child). Unset ⇒ llama.cpp's own default cache, so models
    /// are shared with a separately-run llama.cpp. See [`default_models_dir`].
    #[serde(default)]
    pub models_dir: Option<String>,
    /// Path to the `llama-server` binary for the sidecar. Unset ⇒ found on `PATH`.
    #[serde(default)]
    pub llama_bin: Option<String>,
}

impl Settings {
    /// The active profile: the one named by `active_profile`, else the first, else none.
    pub fn active(&self) -> Option<&LlmProfile> {
        match &self.active_profile {
            Some(id) => self
                .profiles
                .iter()
                .find(|p| &p.id == id)
                .or_else(|| self.profiles.first()),
            None => self.profiles.first(),
        }
    }

    /// The active profile's override (empty if there are no profiles).
    pub fn as_llm_override(&self) -> LlmOverride {
        self.active()
            .map(LlmProfile::as_llm_override)
            .unwrap_or_default()
    }

    /// Build the effective [`PyConfig`]: env (timeouts/caps) with the operator's chosen
    /// interpreter when set.
    pub fn py_config(&self) -> PyConfig {
        let mut cfg = PyConfig::from_env();
        if let Some(bin) = self.python_bin.as_deref().filter(|s| !s.trim().is_empty()) {
            cfg.python_bin = bin.to_string();
        }
        cfg
    }

    /// The models directory the sidecar should use: the operator's choice, else llama.cpp's
    /// own default cache (so models are shared with a separately-run llama.cpp).
    pub fn effective_models_dir(&self) -> PathBuf {
        self.models_dir
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(default_models_dir)
    }

    fn profile_mut(&mut self, id: &str) -> Option<&mut LlmProfile> {
        self.profiles.iter_mut().find(|p| p.id == id)
    }

    /// Allocate a unique slug id from a display name.
    fn fresh_id(&self, name: &str) -> String {
        let base: String = name
            .trim()
            .to_ascii_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>()
            .trim_matches('-')
            .to_string();
        let base = if base.is_empty() {
            "profile".to_string()
        } else {
            base
        };
        if !self.profiles.iter().any(|p| p.id == base) {
            return base;
        }
        // `base-2`, `base-3`, … — the first id not already taken. Finite profile
        // set over an unbounded counter, so this loop always terminates.
        let mut n = 2u32;
        loop {
            let candidate = format!("{base}-{n}");
            if !self.profiles.iter().any(|p| p.id == candidate) {
                return candidate;
            }
            n += 1;
        }
    }
}

/// The seeded default when no settings file exists. Ships ready-to-run **local llama.cpp
/// sidecar** profiles spanning a size spread for two model families — **Gemma 4** (E2B / E4B /
/// 12B / 26B-A4B / 31B) and **Qwen** (Qwen3 4B / 8B / 32B / Coder 30B-A3B and Qwen 3.6 35B-A3B) —
/// plus **Ornith 1.0** (9B / 35B, coding-specialised) and **Nemotron 3.5 Lightning** (30B-A3B,
/// tool-heavy agent execution) — from a ~4 GB monitor up to a ~64 GB
/// flagship, plus a coding-tuned variant (a RAM hint is in
/// each name). Each is given its **own port** (8888–8900) so two can run side by side — a primary
/// plus a sparring-partner / monitor.
/// The operator picks one as active and presses Start. Models download/cache to the shared models
/// directory ([`default_models_dir`]).
fn seeded() -> Settings {
    fn local(id: &str, name: &str, model: &str, port: u16, max_tokens: u32) -> LlmProfile {
        LlmProfile {
            id: id.to_string(),
            name: name.to_string(),
            provider: Some("openai".to_string()),
            base_url: Some(format!("http://127.0.0.1:{port}/v1")),
            model: Some(model.to_string()),
            api_key: None,
            max_tokens: Some(max_tokens),
            context_window: None,
            temperature: None,
            sidecar: true,
            model_file: Some(model.to_string()),
            sidecar_args: Some("-ngl 99 -c 32768 --jinja".to_string()),
            mtp: false,
            thinking_level: None,
            starred: false,
        }
    }
    Settings {
        profiles: vec![
            local(
                "gemma-4-local",
                "Gemma 4 · local (needs 48GB)",
                "unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M",
                8888,
                16000,
            ),
            local(
                "qwen-36-local",
                "Qwen 3.6 · local (needs 64GB)",
                "unsloth/Qwen3.6-35B-A3B-GGUF:UD-Q6_K",
                8889,
                32768,
            ),
            local(
                "qwen3-8b-local",
                "Qwen3 8B · local (needs 16GB)",
                "unsloth/Qwen3-8B-GGUF:UD-Q4_K_XL",
                8890,
                16000,
            ),
            local(
                "qwen3-4b-local",
                "Qwen3 4B · local (needs 8GB)",
                "unsloth/Qwen3-4B-GGUF:UD-Q4_K_XL",
                8891,
                8192,
            ),
            // A 48GB-class dense Qwen, filling the gap between Qwen3 8B (16GB) and
            // Qwen 3.6 35B-A3B (64GB).
            local(
                "qwen3-32b-local",
                "Qwen3 32B · local (needs 48GB)",
                "unsloth/Qwen3-32B-GGUF:UD-Q4_K_XL",
                8892,
                32768,
            ),
            // Gemma 4 across the same size spread as Qwen (8 / 16 / 48 / 64 GB): the
            // existing 26B-A4B is the 48GB tier; these add the small, mid and top tiers.
            local(
                "gemma-4-e4b-local",
                "Gemma 4 E4B · local (needs 8GB)",
                "unsloth/gemma-4-E4B-it-GGUF:UD-Q4_K_XL",
                8893,
                8192,
            ),
            local(
                "gemma-4-12b-local",
                "Gemma 4 12B · local (needs 16GB)",
                "unsloth/gemma-4-12b-it-GGUF:UD-Q4_K_XL",
                8894,
                16000,
            ),
            local(
                "gemma-4-31b-local",
                "Gemma 4 31B · local (needs 64GB)",
                "unsloth/gemma-4-31B-it-GGUF:UD-Q4_K_XL",
                8895,
                32768,
            ),
            // A tiny, fast model (~4GB) — handy as a cheap loop-**monitor** watching a
            // larger primary, or for very low-resource machines.
            local(
                "gemma-4-e2b-local",
                "Gemma 4 E2B · local (needs 4GB)",
                "unsloth/gemma-4-E2B-it-GGUF:UD-Q4_K_XL",
                8896,
                8192,
            ),
            // A coding-tuned MoE (~24GB at Q4) — BPMN/XML authoring is code-shaped, so a
            // coder model can produce cleaner experiment variants.
            local(
                "qwen3-coder-local",
                "Qwen3 Coder 30B-A3B · local (needs 24GB)",
                "unsloth/Qwen3-Coder-30B-A3B-Instruct-GGUF:UD-Q4_K_XL",
                8897,
                32768,
            ),
            // Ornith 1.0 — a coding-specialised family. The 9B is a 16GB-class
            // primary; the 35B (Q4) fills the 48GB coder tier, fitting in 64GB.
            local(
                "ornith-9b-local",
                "Ornith 1.0 9B · local (needs 16GB)",
                "deepreinforce-ai/Ornith-1.0-9B-GGUF:Q6_K",
                8898,
                32768,
            ),
            local(
                "ornith-35b-local",
                "Ornith 1.0 35B · local (needs 48GB)",
                "deepreinforce-ai/Ornith-1.0-35B-GGUF:Q4_K_M",
                8899,
                32768,
            ),
            // Nemotron 3.5 Lightning — NVIDIA's 30B-A3B hybrid-reasoning MoE built for
            // high-volume, tool-heavy agent execution (~20GB at UD-Q4). Ships MTP
            // prediction heads, so it runs with `--mtp` self-speculative decoding for
            // the fast repeated calls it is designed for.
            LlmProfile {
                mtp: true,
                ..local(
                    "nemotron-35-lightning-local",
                    "Nemotron 3.5 Lightning 30B-A3B · local (needs 24GB)",
                    "unsloth/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-GGUF:UD-Q4_K_XL",
                    8900,
                    32768,
                )
            },
        ],
        active_profile: Some("gemma-4-local".to_string()),
        python_bin: None,
        models_dir: None,
        llama_bin: None,
    }
}

/// llama.cpp's own default model/download cache, so ProcessOS's sidecar shares models with a
/// separately-run llama.cpp: `$LLAMA_CACHE`, else the platform cache (`~/Library/Caches/llama.cpp`
/// on macOS, `${XDG_CACHE_HOME:-~/.cache}/llama.cpp` elsewhere).
pub fn default_models_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("LLAMA_CACHE").filter(|s| !s.is_empty()) {
        return PathBuf::from(d);
    }
    let home = std::env::var_os("HOME").map(PathBuf::from);
    #[cfg(target_os = "macos")]
    {
        if let Some(h) = home {
            return h.join("Library/Caches/llama.cpp");
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        if let Some(x) = std::env::var_os("XDG_CACHE_HOME").filter(|s| !s.is_empty()) {
            return PathBuf::from(x).join("llama.cpp");
        }
        if let Some(h) = home {
            return h.join(".cache/llama.cpp");
        }
    }
    PathBuf::from(".")
}

/// Partial update for a single profile (the `name` plus connection fields).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProfilePatch {
    pub name: Option<String>,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub max_tokens: Option<u32>,
    pub context_window: Option<u64>,
    pub temperature: Option<f32>,
    pub sidecar: Option<bool>,
    pub model_file: Option<String>,
    pub sidecar_args: Option<String>,
    pub mtp: Option<bool>,
    /// Coarse reasoning budget label (`fast`/`medium`/`max`); empty/unknown clears it.
    pub thinking_level: Option<String>,
    pub starred: Option<bool>,
}

/// Partial update for the global (non-profile) settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GlobalsPatch {
    pub active_profile: Option<String>,
    pub python_bin: Option<String>,
    pub models_dir: Option<String>,
    pub llama_bin: Option<String>,
}

/// An ad-hoc endpoint descriptor for listing models — either a saved profile (by id, so
/// its stored key is used) and/or inline connection overrides (so the console can probe
/// an endpoint it is still editing).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProbeRequest {
    pub profile_id: Option<String>,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
}

/// A redacted profile safe to return over the API: the API key itself is never echoed,
/// only whether one is set.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileView {
    pub id: String,
    pub name: String,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key_set: bool,
    pub max_tokens: Option<u32>,
    pub context_window: Option<u64>,
    pub temperature: Option<f32>,
    pub sidecar: bool,
    pub model_file: Option<String>,
    pub sidecar_args: Option<String>,
    pub mtp: bool,
    pub thinking_level: Option<ThinkingLevel>,
    pub starred: bool,
}

impl ProfileView {
    fn of(p: &LlmProfile) -> Self {
        Self {
            id: p.id.clone(),
            name: p.name.clone(),
            provider: p.provider.clone(),
            base_url: p.base_url.clone(),
            model: p.model.clone(),
            api_key_set: p.api_key.as_deref().is_some_and(|k| !k.is_empty()),
            max_tokens: p.max_tokens,
            context_window: p.context_window,
            temperature: p.temperature,
            sidecar: p.sidecar,
            model_file: p.model_file.clone(),
            sidecar_args: p.sidecar_args.clone(),
            mtp: p.mtp,
            thinking_level: p.thinking_level,
            starred: p.starred,
        }
    }
}

/// The full redacted settings view returned to the console.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsView {
    pub profiles: Vec<ProfileView>,
    pub active_profile: Option<String>,
    pub python_bin: Option<String>,
    pub models_dir: Option<String>,
    pub llama_bin: Option<String>,
    /// llama.cpp's default cache dir, shown as the placeholder/prefill when `models_dir` is unset.
    pub default_models_dir: String,
    pub path: String,
}

impl SettingsView {
    fn of(s: &Settings, path: &std::path::Path) -> Self {
        Self {
            profiles: s.profiles.iter().map(ProfileView::of).collect(),
            active_profile: s.active().map(|p| p.id.clone()),
            python_bin: s.python_bin.clone(),
            models_dir: s.models_dir.clone(),
            llama_bin: s.llama_bin.clone(),
            default_models_dir: default_models_dir().display().to_string(),
            path: path.display().to_string(),
        }
    }
}

/// Thread-safe, file-backed settings store. Cloneable (shares the inner lock) so it can
/// live in the axum `AppState`.
#[derive(Clone)]
pub struct SettingsStore {
    inner: Arc<RwLock<Settings>>,
    path: Arc<PathBuf>,
}

impl SettingsStore {
    /// Open the store, loading + migrating the persisted file if present, else seeding a
    /// default local profile. A malformed file degrades to the seeded default.
    pub fn open() -> Self {
        let path = settings_path();
        let settings = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| migrate(&s))
            .unwrap_or_else(seeded);
        Self {
            inner: Arc::new(RwLock::new(settings)),
            path: Arc::new(path),
        }
    }

    /// A snapshot of the current settings (cheap clone).
    pub fn snapshot(&self) -> Settings {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The redacted view for the API/console.
    pub fn view(&self) -> SettingsView {
        SettingsView::of(
            &self.inner.read().unwrap_or_else(|e| e.into_inner()),
            &self.path,
        )
    }

    /// Update the globals (active profile + python interpreter), persist, return the view.
    pub fn update_globals(&self, patch: GlobalsPatch) -> Result<SettingsView, String> {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(id) = patch.active_profile {
            let id = id.trim();
            if id.is_empty() {
                g.active_profile = None;
            } else if g.profiles.iter().any(|p| p.id == id) {
                g.active_profile = Some(id.to_string());
            } else {
                return Err(format!("no such profile: {id}"));
            }
        }
        if let Some(v) = patch.python_bin {
            g.python_bin = non_empty(v);
        }
        if let Some(v) = patch.models_dir {
            g.models_dir = non_empty(v);
        }
        if let Some(v) = patch.llama_bin {
            g.llama_bin = non_empty(v);
        }
        self.persist(&g)?;
        Ok(SettingsView::of(&g, &self.path))
    }

    /// Create a new profile (optionally seeded from a patch); make it active if it is the
    /// first. Returns the new profile id plus the full view.
    pub fn add_profile(&self, patch: ProfilePatch) -> Result<(String, SettingsView), String> {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let name = patch
            .name
            .clone()
            .and_then(non_empty)
            .unwrap_or_else(|| "New profile".to_string());
        let id = g.fresh_id(&name);
        let mut profile = LlmProfile {
            id: id.clone(),
            name,
            provider: None,
            base_url: None,
            model: None,
            api_key: None,
            max_tokens: None,
            context_window: None,
            temperature: None,
            sidecar: false,
            model_file: None,
            sidecar_args: None,
            mtp: false,
            thinking_level: None,
            starred: false,
        };
        profile.apply(ProfilePatch {
            name: None,
            ..patch
        });
        g.profiles.push(profile);
        if g.active_profile.is_none() {
            g.active_profile = Some(id.clone());
        }
        self.persist(&g)?;
        Ok((id, SettingsView::of(&g, &self.path)))
    }

    /// Apply a patch to an existing profile, persist, return the view.
    pub fn update_profile(&self, id: &str, patch: ProfilePatch) -> Result<SettingsView, String> {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let p = g
            .profile_mut(id)
            .ok_or_else(|| format!("no such profile: {id}"))?;
        p.apply(patch);
        self.persist(&g)?;
        Ok(SettingsView::of(&g, &self.path))
    }

    /// Delete a profile; if it was active, the active falls back to the first remaining.
    pub fn delete_profile(&self, id: &str) -> Result<SettingsView, String> {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let before = g.profiles.len();
        g.profiles.retain(|p| p.id != id);
        if g.profiles.len() == before {
            return Err(format!("no such profile: {id}"));
        }
        if g.active_profile.as_deref() == Some(id) {
            g.active_profile = g.profiles.first().map(|p| p.id.clone());
        }
        self.persist(&g)?;
        Ok(SettingsView::of(&g, &self.path))
    }

    /// Resolve the [`LlmConfig`] to use for a model-listing probe: start from the env, layer
    /// the saved profile (if `profile_id` is given) so its stored key is used, then layer
    /// the inline overrides (so the console can probe an endpoint it is still editing).
    pub fn probe_config(&self, req: &ProbeRequest) -> Result<LlmConfig, String> {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let mut cfg = LlmConfig::from_env();
        if let Some(id) = req.profile_id.as_deref() {
            let p = g
                .profiles
                .iter()
                .find(|p| p.id == id)
                .ok_or_else(|| format!("no such profile: {id}"))?;
            cfg = cfg.with_override(&p.as_llm_override());
        }
        cfg = cfg.with_override(&LlmOverride {
            provider: req.provider.clone(),
            base_url: req.base_url.clone(),
            api_key: req.api_key.clone(),
            ..Default::default()
        });
        Ok(cfg)
    }

    fn persist(&self, s: &Settings) -> Result<(), String> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let json = serde_json::to_string_pretty(s).map_err(|e| e.to_string())?;
        std::fs::write(&*self.path, json)
            .map_err(|e| format!("write {}: {e}", self.path.display()))?;
        restrict_permissions(&self.path);
        Ok(())
    }
}

/// Parse a stored settings file, migrating the legacy single-LLM shape (flat `llm*`
/// fields) into one `default` profile. Returns `None` if the JSON is unusable.
fn migrate(s: &str) -> Option<Settings> {
    #[derive(Deserialize, Default)]
    #[serde(rename_all = "camelCase", default)]
    struct Stored {
        profiles: Vec<LlmProfile>,
        active_profile: Option<String>,
        python_bin: Option<String>,
        models_dir: Option<String>,
        llama_bin: Option<String>,
        // legacy flat fields (pre-profiles)
        llm_provider: Option<String>,
        llm_base_url: Option<String>,
        llm_model: Option<String>,
        llm_api_key: Option<String>,
        llm_max_tokens: Option<u32>,
        llm_temperature: Option<f32>,
    }
    let st: Stored = serde_json::from_str(s).ok()?;
    let mut settings = Settings {
        profiles: st.profiles,
        active_profile: st.active_profile,
        python_bin: st.python_bin,
        models_dir: st.models_dir,
        llama_bin: st.llama_bin,
    };
    if settings.profiles.is_empty() {
        let has_legacy = st.llm_provider.is_some()
            || st.llm_base_url.is_some()
            || st.llm_model.is_some()
            || st.llm_api_key.is_some()
            || st.llm_max_tokens.is_some()
            || st.llm_temperature.is_some();
        if has_legacy {
            settings.profiles.push(LlmProfile {
                id: "default".to_string(),
                name: "Default".to_string(),
                provider: st.llm_provider,
                base_url: st.llm_base_url,
                model: st.llm_model,
                api_key: st.llm_api_key,
                max_tokens: st.llm_max_tokens,
                context_window: None,
                temperature: st.llm_temperature,
                sidecar: false,
                model_file: None,
                sidecar_args: None,
                mtp: false,
                thinking_level: None,
                starred: false,
            });
            settings
                .active_profile
                .get_or_insert_with(|| "default".to_string());
        } else {
            // Empty/unknown file → fall back to the seeded local profile.
            return Some(seeded());
        }
    }
    Some(settings)
}

/// Resolve the ProcessOS config directory. Honours `PROCESSOS_CONFIG_DIR` (mainly for
/// tests), then `XDG_CONFIG_HOME`/processos, then `$HOME/.config`/processos, falling back
/// to `./processos`.
pub fn config_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("PROCESSOS_CONFIG_DIR").filter(|s| !s.is_empty()) {
        return PathBuf::from(dir);
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("processos")
}

/// Resolve the settings file path (the `settings.json` inside [`config_dir`]).
pub fn settings_path() -> PathBuf {
    config_dir().join("settings.json")
}

#[cfg(unix)]
fn restrict_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &std::path::Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests mutate the process-global PROCESSOS_CONFIG_DIR env var, so they must not run
    // concurrently. Serialize them through one lock (recovering from a poisoned guard).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn store_in(dir: &std::path::Path) -> SettingsStore {
        std::env::set_var("PROCESSOS_CONFIG_DIR", dir);
        SettingsStore::open()
    }

    #[test]
    fn ships_with_local_sidecar_profiles() {
        let _g = env_guard();
        let tmp = std::env::temp_dir().join(format!("processos-set-seed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = store_in(&tmp);
        let v = store.view();
        // Ships ready-to-run local sidecar models across a size spread, with RAM hints in the name.
        assert_eq!(v.profiles.len(), 13);
        assert_eq!(v.active_profile.as_deref(), Some("gemma-4-local"));
        assert!(v.profiles.iter().all(|p| p.sidecar));
        // Every sidecar gets its own port so several can run side by side.
        let ports: std::collections::HashSet<u16> = v
            .profiles
            .iter()
            .filter_map(|p| p.base_url.as_deref().and_then(parse_port))
            .collect();
        assert_eq!(ports.len(), v.profiles.len());
        let gemma = v.profiles.iter().find(|p| p.id == "gemma-4-local").unwrap();
        assert!(gemma.name.contains("48GB"));
        assert_eq!(gemma.base_url.as_deref(), Some("http://127.0.0.1:8888/v1"));
        assert!(gemma.model_file.as_deref().unwrap().contains("gemma-4"));
        assert!(v.profiles.iter().any(|p| p.name.contains("64GB")));
        // Nemotron 3.5 Lightning ships with MTP heads, so its sidecar enables `--mtp`.
        let nemotron = v
            .profiles
            .iter()
            .find(|p| p.id == "nemotron-35-lightning-local")
            .unwrap();
        assert!(nemotron.mtp);
        assert!(nemotron
            .model_file
            .as_deref()
            .unwrap()
            .contains("NVIDIA-Nemotron-3.5-Lightning-30B-A3B"));
        // A non-empty default models dir is surfaced for the UI to prefill.
        assert!(!v.default_models_dir.is_empty());
        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }

    #[test]
    fn parses_preferred_port_from_base_url() {
        assert_eq!(parse_port("http://127.0.0.1:8888/v1"), Some(8888));
        assert_eq!(parse_port("http://localhost:1234"), Some(1234));
        assert_eq!(parse_port("http://localhost/v1"), None);
        let p = LlmProfile {
            id: "x".into(),
            name: "x".into(),
            provider: None,
            base_url: Some("http://127.0.0.1:9090/v1".into()),
            model: None,
            api_key: None,
            max_tokens: None,
            context_window: None,
            temperature: None,
            sidecar: true,
            model_file: None,
            sidecar_args: None,
            mtp: false,
            thinking_level: None,
            starred: false,
        };
        assert_eq!(p.preferred_port(), Some(9090));
    }

    #[test]
    fn globals_patch_sets_models_dir_and_llama_bin() {
        let _g = env_guard();
        let tmp = std::env::temp_dir().join(format!("processos-set-md-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = store_in(&tmp);
        let v = store
            .update_globals(GlobalsPatch {
                models_dir: Some("/models".into()),
                llama_bin: Some("/usr/bin/llama-server".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(v.models_dir.as_deref(), Some("/models"));
        assert_eq!(v.llama_bin.as_deref(), Some("/usr/bin/llama-server"));
        assert_eq!(
            store.snapshot().effective_models_dir(),
            std::path::PathBuf::from("/models")
        );
        // Clearing models_dir falls back to the llama.cpp default.
        let v = store
            .update_globals(GlobalsPatch {
                models_dir: Some("".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(v.models_dir, None);
        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }

    #[test]
    fn add_switch_and_redact_profiles() {
        let _g = env_guard();
        let tmp = std::env::temp_dir().join(format!("processos-set-multi-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = store_in(&tmp);

        let (id, _) = store
            .add_profile(ProfilePatch {
                name: Some("Cloud".into()),
                provider: Some("anthropic".into()),
                model: Some("claude".into()),
                api_key: Some("sk-secret".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(id, "cloud");

        // Switch active to the new profile and confirm the override follows it.
        store
            .update_globals(GlobalsPatch {
                active_profile: Some(id.clone()),
                python_bin: None,
                ..Default::default()
            })
            .unwrap();
        let ovr = store.snapshot().as_llm_override();
        assert_eq!(ovr.model.as_deref(), Some("claude"));
        assert_eq!(ovr.api_key.as_deref(), Some("sk-secret"));

        // The view redacts the key but flags it set.
        let v = store.view();
        let cloud = v.profiles.iter().find(|p| p.id == "cloud").unwrap();
        assert!(cloud.api_key_set);
        assert!(!serde_json::to_string(&v).unwrap().contains("sk-secret"));

        // Switching to an unknown profile is rejected.
        assert!(store
            .update_globals(GlobalsPatch {
                active_profile: Some("nope".into()),
                python_bin: None,
                ..Default::default()
            })
            .is_err());

        // Delete falls back the active to the first remaining.
        store.delete_profile("cloud").unwrap();
        assert_eq!(
            store.view().active_profile.as_deref(),
            Some("gemma-4-local")
        );

        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }

    #[test]
    fn migrates_legacy_flat_file() {
        let _g = env_guard();
        let tmp = std::env::temp_dir().join(format!("processos-set-legacy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(
            tmp.join("settings.json"),
            r#"{"llmProvider":"openai","llmBaseUrl":"http://x/v1","llmModel":"m","llmApiKey":"k","pythonBin":"/p"}"#,
        )
        .unwrap();
        let store = store_in(&tmp);
        let v = store.view();
        assert_eq!(v.profiles.len(), 1);
        assert_eq!(v.profiles[0].id, "default");
        assert_eq!(v.profiles[0].model.as_deref(), Some("m"));
        assert_eq!(v.python_bin.as_deref(), Some("/p"));
        assert!(v.profiles[0].api_key_set);
        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }
}
