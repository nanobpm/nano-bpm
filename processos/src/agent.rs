//! **Agentic tool-calling loop** — the substrate that lets the cockpit droid *act*
//! on the data, not just be prompted with a summary.
//!
//! [`run_agent`] is provider-agnostic orchestration: it offers the model a set of
//! [`ToolSpec`]s, and whenever the model emits tool calls it dispatches them to a
//! [`ToolBox`], feeds the results back, and loops until the model returns a final
//! answer (or a round budget is hit). The transport is abstracted behind
//! [`AgentStep`] so the loop is unit-testable with a deterministic mock — no network.
//!
//! The concrete transport here speaks the **OpenAI chat-completions** tool-calling
//! wire shape (what a local `llama.cpp`/vLLM/Ollama server and OpenAI itself all
//! accept); Anthropic tool-use can slot in as a second `AgentStep` impl later.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::harness::llm::LlmConfig;

/// A tool the model may call.
#[derive(Debug, Clone, Serialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    /// JSON-Schema object describing the call arguments.
    pub parameters: Value,
}

/// One tool invocation the model requested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// What the model decided on a step.
pub enum Turn {
    /// The model wants to run these tools before answering.
    ToolCalls(Vec<ToolCall>),
    /// The model produced its final answer.
    Final(String),
}

/// A neutral conversation message the transport serialises per provider.
///
/// `Serialize`/`Deserialize` let a multi-turn chat persist the *full* transcript
/// (including tool calls and their results) so the model keeps its working memory
/// across operator messages and server restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    System(String),
    User(String),
    /// An assistant turn that requested tools (text optional).
    Assistant {
        text: Option<String>,
        tool_calls: Vec<ToolCall>,
    },
    /// The result of one tool call, fed back to the model.
    Tool {
        call_id: String,
        content: String,
    },
}

/// Optional sink for persisting the *in-progress* transcript at agent round boundaries.
///
/// When supplied, [`run_agent_streaming`] invokes it with the running messages so the caller
/// can checkpoint the conversation incrementally (e.g. throttled to disk every couple of
/// seconds), leaving a debuggable transcript even when a turn times out, errors, or is
/// interrupted — instead of only persisting on clean completion.
pub type Checkpoint<'a> = Option<&'a mut dyn FnMut(&[Msg])>;

/// A set of callable tools.
pub trait ToolBox {
    fn specs(&self) -> Vec<ToolSpec>;
    /// Execute a tool by name; the returned string is fed back verbatim to the model.
    fn call(&self, name: &str, args: &Value) -> Result<String, String>;
}

/// A streamed fragment of one model step, surfaced live to the operator.
pub enum Delta {
    /// A chunk of the model's chain-of-thought (`reasoning_content`).
    Reasoning(String),
    /// A chunk of the model's user-facing answer (`content`).
    Answer(String),
    /// The OpenAI completion id (`chatcmpl-…`) for this streamed turn, surfaced once as soon as
    /// the first chunk carrying it arrives — so the caller can target the live turn with the
    /// reasoning-control endpoint (end its thinking mid-generation).
    Completion(String),
}

/// An event emitted by the streaming agent loop, for live cockpit feedback.
pub enum AgentEvent {
    /// A new round of the tool-calling loop began.
    Round(usize),
    /// A provenance boundary: the agent now producing events changed (Pair AI). All events
    /// after this — until the next `Agent` — belong to `name` acting in `role` ("primary" or
    /// "pair"). The cockpit starts a fresh, attributed bubble on each boundary.
    Agent {
        id: String,
        name: String,
        role: String,
    },
    /// The exact request body about to be sent to the model this round (for the debug view).
    Request { round: usize, body: Value },
    /// A chunk of the droid's thinking.
    Reasoning(String),
    /// A chunk of the droid's answer prose.
    Answer(String),
    /// The OpenAI completion id (`chatcmpl-…`) of the in-flight turn, so the host can target it
    /// with the reasoning-control endpoint (wrap-up / loop monitor). Emitted once per turn.
    Completion { id: String },
    /// The droid asked to run a tool (emitted before execution).
    ToolCall { tool: String, arguments: Value },
    /// A tool finished; `result` is the raw string fed back to the model.
    ToolResult { tool: String, result: String },
}

/// The model transport: one request/response step given the running messages.
#[allow(async_fn_in_trait)]
pub trait AgentStep {
    async fn step(&self, msgs: &[Msg], tools: &[ToolSpec]) -> Result<Turn, String>;

    /// The exact request body this transport would send for `msgs`/`tools`, surfaced to the
    /// cockpit's per-session debug view so the operator can see *everything* the model receives
    /// (system prompt, tool specs, full transcript) — not just their latest message. The default
    /// is `None` (mock transports make no HTTP request and have nothing to show).
    fn debug_body(&self, _msgs: &[Msg], _tools: &[ToolSpec]) -> Option<Value> {
        None
    }

    /// Streaming variant: same contract as [`AgentStep::step`], but `on_delta` is invoked with
    /// each token fragment as it arrives so the caller can surface live progress. `cancel`, when
    /// set mid-stream, asks the transport to STOP reading (which cancels generation) and return
    /// what it has — so an operator can interrupt a model caught in a loop while still generating,
    /// not only between rounds. The default implementation is non-streaming (emits no deltas,
    /// ignores `cancel`) so mock transports need not implement it.
    async fn step_streaming(
        &self,
        msgs: &[Msg],
        tools: &[ToolSpec],
        _on_delta: &mut dyn FnMut(Delta),
        _cancel: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Turn, String> {
        self.step(msgs, tools).await
    }
}

/// One recorded tool call + its result (the "lab notebook" of an investigation).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentStepRecord {
    pub tool: String,
    pub arguments: Value,
    pub result: String,
}

/// The outcome of an agent run.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRun {
    pub answer: String,
    pub steps: Vec<AgentStepRecord>,
    pub rounds: usize,
}

/// Drive the model→tools→model loop until a final answer or `max_rounds` is reached.
pub async fn run_agent<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    system: &str,
    user: &str,
    max_rounds: usize,
) -> Result<AgentRun, String> {
    let mut msgs = vec![Msg::System(system.to_string()), Msg::User(user.to_string())];
    run_agent_resumable(model, tools, &mut msgs, max_rounds).await
}

/// Drive the loop over an **existing transcript**, appending the assistant/tool turns
/// (and finally the assistant's answer) to `msgs` in place. This is the multi-turn
/// substrate: the caller seeds `msgs` with the persisted history plus the new user
/// message, runs a turn, and persists the mutated `msgs` so the next turn resumes
/// with full context. `run_agent` is the single-shot special case.
pub async fn run_agent_resumable<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    msgs: &mut Vec<Msg>,
    max_rounds: usize,
) -> Result<AgentRun, String> {
    run_agent_resumable_cancellable(model, tools, msgs, max_rounds, None).await
}

/// As [`run_agent_resumable`], but a shared `cancel` flag lets the caller ask the agent to
/// **wrap up early**: when it is set, the loop stops issuing tool calls, instructs the model
/// to report its findings so far (with tools withheld so it must answer in prose), and
/// returns that. Used by the cockpit's "wrap it up" control for long investigations.
pub async fn run_agent_resumable_cancellable<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    msgs: &mut Vec<Msg>,
    max_rounds: usize,
    cancel: Option<&std::sync::atomic::AtomicBool>,
) -> Result<AgentRun, String> {
    let mut sink = |_ev: AgentEvent| {};
    run_agent_streaming(
        model, tools, msgs, max_rounds, cancel, None, &mut sink, None,
    )
    .await
}

/// As [`run_agent_resumable_cancellable`], but emits [`AgentEvent`]s through `sink` as the run
/// progresses — token fragments of the droid's thinking and answer, plus tool-call boundaries —
/// so the cockpit can render the investigation live instead of waiting for the whole turn. This
/// is the canonical loop; the non-streaming entry points delegate here with a no-op sink.
///
/// `checkpoint`, when supplied, is invoked with the *running* transcript at each round boundary
/// and after each batch of tool results, so the caller can PERSIST the in-progress conversation
/// incrementally (rather than only when the whole turn completes). This is what lets a turn that
/// times out, errors, or is interrupted still leave a debuggable transcript on disk.
#[allow(clippy::too_many_arguments)]
pub async fn run_agent_streaming<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    msgs: &mut Vec<Msg>,
    max_rounds: usize,
    cancel: Option<&std::sync::atomic::AtomicBool>,
    steer: Option<&std::sync::Mutex<Vec<String>>>,
    sink: &mut dyn FnMut(AgentEvent),
    mut checkpoint: Checkpoint<'_>,
) -> Result<AgentRun, String> {
    use std::sync::atomic::Ordering;
    let specs = tools.specs();
    let mut steps: Vec<AgentStepRecord> = Vec::new();
    // Bounded safety net: how many times the loop may nudge a model that ended a
    // turn by *naming* a next action without performing it (see Turn::Final below).
    const MAX_AUTO_CONTINUES: usize = 2;
    let mut auto_continues = 0usize;
    // Bounded safety net for a *different* spin: a model that keeps re-issuing the SAME
    // tool call(s) it already ran (e.g. running an identical query_traces SQL every round
    // instead of acting on the result). We remember the last few call signatures; on a
    // repeat we steer once, and after MAX_REPEAT_NUDGES repeats we force a wrap-up so the
    // run always terminates with whatever findings it has instead of grinding to max_rounds.
    const MAX_REPEAT_NUDGES: usize = 2;
    let mut repeat_nudges = 0usize;
    let mut recent_sigs: std::collections::VecDeque<String> = std::collections::VecDeque::new();

    for round in 1..=max_rounds {
        // Operator asked to wrap up: stop investigating and force a prose summary now.
        if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
            return wrap_up(model, msgs, steps, round, sink).await;
        }
        // Operator sent steering instruction(s) mid-investigation: inject them as user
        // turns so the next round reasons with the new direction in context. Drained at
        // the round boundary — the model finishes its current generation, then the steer
        // lands before it decides the next action. Each becomes part of the persisted
        // transcript (the cockpit redraws it on `done`).
        if let Some(q) = steer {
            let pending: Vec<String> = q
                .lock()
                .map(|mut v| std::mem::take(&mut *v))
                .unwrap_or_default();
            for s in pending {
                let s = s.trim();
                if !s.is_empty() {
                    msgs.push(Msg::User(s.to_string()));
                }
            }
        }
        sink(AgentEvent::Round(round));
        // Persist the running transcript at the round boundary (the user/steer messages are now
        // in `msgs`). On round 1 this lands the operator's message immediately, so even a turn
        // that dies in its first generation leaves something on disk to debug.
        if let Some(cp) = checkpoint.as_deref_mut() {
            cp(msgs);
        }
        // Surface the exact payload this round sends to the model, so the debug view can show
        // everything it receives (system prompt + tool specs + full transcript), not just the
        // operator's latest message.
        if let Some(body) = model.debug_body(msgs, &specs) {
            sink(AgentEvent::Request { round, body });
        }
        // Capture this round's chain-of-thought so a tool-calling turn can persist it (the
        // model often does its real thinking in the round where it decides to call a tool;
        // without this it would vanish from the saved transcript, leaving the droid bubble
        // with no "Thinking" disclosure after the live stream ends).
        let mut round_reasoning = String::new();
        let turn = {
            // Forward token fragments live; scoped so `sink` is free again after the call.
            let mut on_delta = |d: Delta| match d {
                Delta::Reasoning(t) => {
                    round_reasoning.push_str(&t);
                    sink(AgentEvent::Reasoning(t));
                }
                Delta::Answer(t) => sink(AgentEvent::Answer(t)),
                Delta::Completion(id) => sink(AgentEvent::Completion { id }),
            };
            model
                .step_streaming(msgs, &specs, &mut on_delta, cancel)
                .await?
        };
        // If the operator interrupted while the model was still generating (cancel flipped
        // mid-stream, so step_streaming returned early), don't process the partial turn —
        // stop and summarise from what we have, immediately, rather than nudging on a
        // truncated runaway. (Round-top also checks cancel, but only between rounds.)
        if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
            return wrap_up(model, msgs, steps, round, sink).await;
        }
        match turn {
            Turn::Final(answer) => {
                // Two failure modes end a turn without progress: (a) the model NAMES a
                // next action ("I will now author the variant") but doesn't perform it,
                // and (b) the model only *thinks* — a reasoning-only turn with no visible
                // answer and no tool call, often because it exhausted its output budget
                // mid-thought (renders as a blank "cut off while thinking" bubble). Both
                // would silently end the investigation. Nudge once (bounded), never when
                // the operator asked to wrap up, using the same mid-conversation System
                // steering pattern as wrap_up.
                let wrapping_up = cancel.is_some_and(|c| c.load(Ordering::Relaxed));
                let thinking_only = is_thinking_only(&answer);
                let runaway = is_runaway_repetition(&answer);
                let needs_nudge = thinking_only || runaway || signals_deferred_action(&answer);
                if !wrapping_up
                    && auto_continues < MAX_AUTO_CONTINUES
                    && round < max_rounds
                    && needs_nudge
                {
                    msgs.push(Msg::Assistant {
                        text: Some(answer.clone()),
                        tool_calls: Vec::new(),
                    });
                    let nudge = if runaway {
                        "Your previous turn ran on, repeating the same text many times without \
                         finishing. Writing SQL in your answer (e.g. in backticks) does NOTHING \
                         — the ONLY way to run a query is to emit a query_traces tool call. Stop \
                         repeating yourself. Prefer feedback from tool calls to pure reasoning: \
                         you can get it wrong and iterate on the tool feedback. Now do exactly \
                         one thing: emit a single query_traces tool call, run a simulation, or \
                         give a concise final answer."
                    } else if thinking_only {
                        "Your previous turn was all reasoning and produced no answer or \
                         tool call — you likely ran out of room mid-thought. Do NOT author \
                         long content (such as full BPMN XML) inside your reasoning; keep \
                         thinking brief and put the XML directly in the tool-call argument. \
                         Now, concisely: either call the appropriate tool, or give your \
                         final answer."
                    } else {
                        "You ended your turn by naming a next step but did not carry it \
                         out. Do not stop here: perform that step NOW by calling the \
                         appropriate tool (e.g. simulate with the full BPMN XML of your \
                         variant). Only stop to report evidence-backed findings or to ask \
                         the operator a genuine decision."
                    };
                    msgs.push(Msg::User(nudge.to_string()));
                    auto_continues += 1;
                    continue;
                }
                // Record the answer in the transcript so a resumed conversation
                // remembers what the droid concluded last turn.
                msgs.push(Msg::Assistant {
                    text: Some(answer.clone()),
                    tool_calls: Vec::new(),
                });
                return Ok(AgentRun {
                    answer,
                    steps,
                    rounds: round,
                });
            }
            Turn::ToolCalls(calls) if calls.is_empty() => {
                // Defensive: a tool-call turn with nothing to call — treat as done.
                return Ok(AgentRun {
                    answer: String::new(),
                    steps,
                    rounds: round,
                });
            }
            Turn::ToolCalls(calls) => {
                // Detect a model spinning on identical tool call(s) it already ran this
                // session. We still execute and return the (cheap, read-only) result so the
                // model has the data, but steer it to act on what it has; if it keeps
                // repeating past the bound, force a wrap-up so the loop terminates.
                let sig = tool_call_signature(&calls);
                let is_repeat = recent_sigs.contains(&sig);
                recent_sigs.push_back(sig);
                while recent_sigs.len() > 3 {
                    recent_sigs.pop_front();
                }
                // Persist this round's thinking (as a `<think>` block) on the tool-calling
                // turn so it survives into the rendered transcript even though the visible
                // answer comes from a later round.
                let text = {
                    let r = round_reasoning.trim();
                    (!r.is_empty()).then(|| format!("<think>{r}</think>"))
                };
                msgs.push(Msg::Assistant {
                    text,
                    tool_calls: calls.clone(),
                });
                for call in calls {
                    sink(AgentEvent::ToolCall {
                        tool: call.name.clone(),
                        arguments: call.arguments.clone(),
                    });
                    let result = tools
                        .call(&call.name, &call.arguments)
                        .unwrap_or_else(|e| json!({ "error": e }).to_string());
                    sink(AgentEvent::ToolResult {
                        tool: call.name.clone(),
                        result: result.clone(),
                    });
                    steps.push(AgentStepRecord {
                        tool: call.name.clone(),
                        arguments: call.arguments.clone(),
                        result: result.clone(),
                    });
                    msgs.push(Msg::Tool {
                        call_id: call.id,
                        content: result,
                    });
                }
                // The assistant's tool calls and their results are now in `msgs`; persist this
                // progress so a turn that later stalls/times out still shows the tools it ran.
                if let Some(cp) = checkpoint.as_deref_mut() {
                    cp(msgs);
                }
                if is_repeat {
                    repeat_nudges += 1;
                    if repeat_nudges > MAX_REPEAT_NUDGES {
                        // The model is stuck re-running the same call; stop the spin and
                        // summarise from the evidence gathered so far.
                        return wrap_up(model, msgs, steps, round, sink).await;
                    }
                    msgs.push(Msg::User(
                        "You just re-issued a tool call you already ran this session and got \
                         the same result — you are repeating yourself, not making progress. Do \
                         NOT run that query again. You already have this data: act on it. Call a \
                         DIFFERENT tool that moves the investigation forward (e.g. simulate a \
                         variant), or, if you have enough evidence, give your final answer now."
                            .to_string(),
                    ));
                }
            }
        }
    }
    // Budget exhausted: rather than erroring, ask the model to summarise what it has so the
    // operator still gets findings from a long run.
    wrap_up(model, msgs, steps, max_rounds, sink).await
}

/// Force a final prose answer from the model with no tools available, recording it in the
/// transcript. Used both when the operator wraps up early and when the round budget is hit.
async fn wrap_up<M: AgentStep>(
    model: &M,
    msgs: &mut Vec<Msg>,
    steps: Vec<AgentStepRecord>,
    round: usize,
    sink: &mut dyn FnMut(AgentEvent),
) -> Result<AgentRun, String> {
    msgs.push(Msg::User(
        "Stop investigating now and report your findings so far, based only on what you \
         have already gathered. Do not call any more tools — answer in clear prose, stating \
         what you found, your confidence, and what you'd recommend or check next."
            .to_string(),
    ));
    let answer = {
        let mut on_delta = |d: Delta| match d {
            Delta::Reasoning(t) => sink(AgentEvent::Reasoning(t)),
            Delta::Answer(t) => sink(AgentEvent::Answer(t)),
            // The wrap-up summary turn is already terminal; no need to surface its completion id.
            Delta::Completion(_) => {}
        };
        // Pass no cancel: wrap-up is the *consequence* of an interrupt/budget stop, so the
        // summary generation must be allowed to run even though `cancel` is (often) still set.
        match model.step_streaming(msgs, &[], &mut on_delta, None).await? {
            Turn::Final(a) => a,
            Turn::ToolCalls(_) => String::new(),
        }
    };
    // The wrap-up turn forbids tools, but a Hermes-style model may still emit a `<tool_call>`
    // block as content (its grammar isn't applied when no tools are offered). Strip that markup
    // so the conversation doesn't end on a wall of raw XML; fall back to a brief note if nothing
    // intelligible remains.
    let answer = {
        let stripped = strip_leaked_tool_markup(&answer);
        if stripped.is_empty() && !answer.trim().is_empty() {
            "I reached the step budget while drafting a candidate model and did not finish a \
             prose summary. Based on the work so far, re-run with a concrete next step (validate \
             the candidate, then simulate it on a small sample) to continue."
                .to_string()
        } else {
            stripped
        }
    };
    msgs.push(Msg::Assistant {
        text: Some(answer.clone()),
        tool_calls: Vec::new(),
    });
    Ok(AgentRun {
        answer,
        steps,
        rounds: round,
    })
}

// ─── OpenAI chat-completions tool-calling transport ──────────────────────────

/// An [`AgentStep`] backed by an OpenAI-compatible `chat/completions` endpoint.
pub struct OpenAiAgent {
    pub cfg: LlmConfig,
}

impl OpenAiAgent {
    /// Build the `chat/completions` request body shared by streaming and non-streaming calls.
    pub(crate) fn request_body(&self, msgs: &[Msg], tools: &[ToolSpec], stream: bool) -> Value {
        let tool_defs: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect();
        let mut body = json!({
            "model": self.cfg.model,
            "temperature": self.cfg.temperature,
            "max_tokens": self.cfg.max_tokens,
            "frequency_penalty": self.cfg.frequency_penalty,
            "tools": tool_defs,
            "stream": stream,
            "messages": wire_messages(msgs),
            // Arm the reasoning-control budget sampler so the loop monitor / "wrap it up" can end
            // this turn's thinking mid-generation via POST /v1/chat/completions/control. Servers
            // without the feature (older llama.cpp) ignore the unknown field — harmless.
            "reasoning_control": true,
        });
        crate::harness::llm::apply_thinking_budget(&mut body, &self.cfg);
        crate::harness::llm::apply_grammar(&mut body, &self.cfg);
        body
    }

    fn endpoint(&self) -> String {
        format!(
            "{}/chat/completions",
            self.cfg.base_url.trim_end_matches('/')
        )
    }
}

/// Accumulates one streamed tool call whose `arguments` (and sometimes name) arrive in fragments.
#[derive(Default)]
struct ToolCallAccum {
    id: String,
    name: String,
    args: String,
}

impl AgentStep for OpenAiAgent {
    fn debug_body(&self, msgs: &[Msg], tools: &[ToolSpec]) -> Option<Value> {
        Some(self.request_body(msgs, tools, true))
    }

    async fn step(&self, msgs: &[Msg], tools: &[ToolSpec]) -> Result<Turn, String> {
        if !self.cfg.is_ready() {
            return Err("no LLM model configured (set PROCESSOS_LLM_MODEL)".into());
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        let url = self.endpoint();
        let body = self.request_body(msgs, tools, false);

        let mut req = client.post(&url).json(&body);
        if let Some(key) = &self.cfg.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("LLM request to {url} failed: {e}"))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("reading LLM response: {e}"))?;
        if !status.is_success() {
            return Err(format!("LLM returned {status}: {}", truncate(&text, 500)));
        }
        let v: Value =
            serde_json::from_str(&text).map_err(|e| format!("LLM response not JSON: {e}"))?;
        parse_openai_turn(&v["choices"][0]["message"])
    }

    async fn step_streaming(
        &self,
        msgs: &[Msg],
        tools: &[ToolSpec],
        on_delta: &mut dyn FnMut(Delta),
        cancel: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Turn, String> {
        use std::sync::atomic::Ordering;

        use futures_util::StreamExt;
        if !self.cfg.is_ready() {
            return Err("no LLM model configured (set PROCESSOS_LLM_MODEL)".into());
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        let url = self.endpoint();
        let body = self.request_body(msgs, tools, true);

        let mut req = client.post(&url).json(&body);
        if let Some(key) = &self.cfg.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("LLM request to {url} failed: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("LLM returned {status}: {}", truncate(&text, 500)));
        }

        let mut content = String::new();
        let mut reasoning = String::new();
        let mut tool_accum: Vec<ToolCallAccum> = Vec::new();
        let mut buf = String::new();
        // Surface the OpenAI completion id (`chatcmpl-…`) the first time a chunk carries it, so the
        // host can target THIS live turn with the reasoning-control endpoint.
        let mut emitted_cmpl_id = false;
        // Mid-stream circuit-breaker: a small local model can fall into a repetition
        // attractor and emit the same line forever until it exhausts the (now larger)
        // token budget — the operator watches a wall of identical text. Once a channel's
        // tail shows that runaway, stop reading the stream (which cancels generation) so
        // the harness can nudge the model on the next round instead of waiting it out.
        // The first check only fires past RUNAWAY_TAIL_MIN so the accumulated text is also
        // long enough for the harness-level `is_runaway_repetition` to catch and nudge it.
        let mut next_check = RUNAWAY_TAIL_MIN;
        let mut stream = resp.bytes_stream();
        'outer: while let Some(chunk) = stream.next().await {
            // Operator interrupted (wrap-up/steer) while the model is still generating: stop
            // reading the stream — which cancels generation upstream — and return what we have
            // so the harness can summarise immediately instead of waiting out a runaway.
            if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
                break 'outer;
            }
            let bytes = chunk.map_err(|e| format!("LLM stream error: {e}"))?;
            buf.push_str(&String::from_utf8_lossy(&bytes));
            // Process complete SSE lines; keep the trailing partial line in `buf`.
            while let Some(pos) = buf.find('\n') {
                let line = buf[..pos].trim().to_string();
                buf.drain(..=pos);
                let data = match line.strip_prefix("data:") {
                    Some(d) => d.trim(),
                    None => continue, // comments / blank lines
                };
                if data == "[DONE]" {
                    break 'outer;
                }
                let v: Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if !emitted_cmpl_id {
                    if let Some(id) = v["id"].as_str() {
                        if !id.is_empty() {
                            emitted_cmpl_id = true;
                            on_delta(Delta::Completion(id.to_string()));
                        }
                    }
                }
                let delta = &v["choices"][0]["delta"];
                if let Some(r) = delta["reasoning_content"].as_str() {
                    if !r.is_empty() {
                        reasoning.push_str(r);
                        on_delta(Delta::Reasoning(r.to_string()));
                    }
                }
                if let Some(c) = delta["content"].as_str() {
                    if !c.is_empty() {
                        content.push_str(c);
                        on_delta(Delta::Answer(c.to_string()));
                    }
                }
                if let Some(tcs) = delta["tool_calls"].as_array() {
                    for tc in tcs {
                        let idx = tc["index"].as_u64().unwrap_or(0) as usize;
                        while tool_accum.len() <= idx {
                            tool_accum.push(ToolCallAccum::default());
                        }
                        let acc = &mut tool_accum[idx];
                        if let Some(id) = tc["id"].as_str() {
                            if !id.is_empty() {
                                acc.id = id.to_string();
                            }
                        }
                        if let Some(n) = tc["function"]["name"].as_str() {
                            if !n.is_empty() {
                                acc.name.push_str(n);
                            }
                        }
                        if let Some(a) = tc["function"]["arguments"].as_str() {
                            acc.args.push_str(a);
                        }
                    }
                }
            }
            // Only watch free-form generation (a tool call streaming its arguments is not a
            // runaway). Check the tail periodically to keep this cheap.
            if tool_accum.is_empty() {
                let total = content.len() + reasoning.len();
                if total >= next_check {
                    next_check = total + RUNAWAY_TAIL_STEP;
                    if runaway_tail(&content) || runaway_tail(&reasoning) {
                        break 'outer;
                    }
                }
            }
        }

        if !tool_accum.is_empty() {
            let calls = tool_accum
                .into_iter()
                .enumerate()
                .map(|(i, a)| ToolCall {
                    id: if a.id.is_empty() {
                        format!("call_{i}")
                    } else {
                        a.id
                    },
                    name: a.name,
                    arguments: serde_json::from_str(&a.args).unwrap_or(json!({})),
                })
                .collect();
            return Ok(Turn::ToolCalls(calls));
        }
        // Fold any separate reasoning stream back into a `<think>` block so the persisted
        // transcript (and non-streaming render) keeps the droid's chain-of-thought.
        let reasoning = reasoning.trim();
        let final_text = if reasoning.is_empty() {
            content
        } else {
            format!("<think>{reasoning}</think>\n{content}")
        };
        // Fallback: recover a tool call the model leaked into the streamed `content`
        // as literal template tokens, so a leaked call doesn't end the loop early.
        let leaked = parse_leaked_tool_calls(&final_text);
        if !leaked.is_empty() {
            return Ok(Turn::ToolCalls(leaked));
        }
        Ok(Turn::Final(final_text))
    }
}

fn openai_message(m: &Msg) -> Value {
    match m {
        Msg::System(s) => json!({ "role": "system", "content": s }),
        Msg::User(s) => json!({ "role": "user", "content": s }),
        Msg::Assistant { text, tool_calls } => {
            let calls: Vec<Value> = tool_calls
                .iter()
                .map(|c| {
                    json!({
                        "id": c.id,
                        "type": "function",
                        "function": {
                            "name": c.name,
                            "arguments": c.arguments.to_string(),
                        }
                    })
                })
                .collect();
            json!({
                "role": "assistant",
                "content": text.clone().unwrap_or_default(),
                "tool_calls": calls,
            })
        }
        Msg::Tool { call_id, content } => json!({
            "role": "tool",
            "tool_call_id": call_id,
            "content": content,
        }),
    }
}

/// Build the wire `messages` array, shrinking the resent context so long investigations don't
/// blow past the model's context window (the agent transcript is otherwise never trimmed — it
/// is re-sent in full every round, and the Experiment persona's read_model XML, simulate
/// scorecards and authored variants accumulate fast). Two reductions, neither of which touches
/// the persisted transcript the cockpit renders:
///   1. The model's own `<think>…</think>` reasoning is NEVER resent — a chat API does not need
///      a model's past chain-of-thought, and on a local model that drafts BPMN XML in its head
///      it is the single biggest amplifier. The `<think>` block stays in the saved transcript so
///      the UI's "Thinking" disclosure is unaffected.
///   2. Large tool results from EARLIER rounds are truncated to a cap; the most recent messages
///      are kept verbatim so the model still reasons over fresh data in full.
fn wire_messages(msgs: &[Msg]) -> Vec<Value> {
    // Trailing messages kept fully verbatim (covers roughly the last couple of rounds).
    const KEEP_RECENT: usize = 6;
    // Earlier tool results longer than this (chars) are clipped — generous enough to keep a
    // full small model intact, bounded enough to stop unbounded growth.
    const TOOL_RESULT_CAP: usize = 6000;
    let n = msgs.len();
    msgs.iter()
        .enumerate()
        .map(|(i, m)| {
            let recent = i + KEEP_RECENT >= n;
            match m {
                // A `system` message is only valid as the VERY FIRST message for strict chat
                // templates (llama.cpp's Jinja templates for some models — e.g. Ornith — raise
                // "System message must be at the beginning"). Any system directive injected
                // mid-conversation (steer nudges, a forced wrap-up) is resent as a user turn so
                // those templates accept it.
                Msg::System(s) if i > 0 => json!({ "role": "user", "content": s }),
                // Strip the model's own reasoning from every assistant turn we resend.
                Msg::Assistant { text, tool_calls } => {
                    let stripped = text
                        .as_deref()
                        .map(|t| strip_think_blocks(t).trim().to_string());
                    openai_message(&Msg::Assistant {
                        text: stripped.filter(|s| !s.is_empty()),
                        tool_calls: tool_calls.clone(),
                    })
                }
                // Clip big tool results from earlier rounds; keep recent ones whole.
                Msg::Tool { call_id, content } if !recent && content.len() > TOOL_RESULT_CAP => {
                    openai_message(&Msg::Tool {
                        call_id: call_id.clone(),
                        content: clip(content, TOOL_RESULT_CAP),
                    })
                }
                other => openai_message(other),
            }
        })
        .collect()
}

/// Truncate a tool result on a char boundary, leaving a note so the model knows content was
/// dropped to fit the context window (rather than silently seeing a half result).
fn clip(s: &str, cap: usize) -> String {
    if s.len() <= cap {
        return s.to_string();
    }
    let mut end = cap;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n…[truncated {} chars of an earlier tool result to fit the context window — \
         re-run the tool if you need the full output]",
        &s[..end],
        s.len() - end
    )
}

fn parse_openai_turn(message: &Value) -> Result<Turn, String> {
    let calls = message.get("tool_calls").and_then(|c| c.as_array());
    if let Some(calls) = calls {
        if !calls.is_empty() {
            let mut out = Vec::with_capacity(calls.len());
            for (i, c) in calls.iter().enumerate() {
                let id = c["id"]
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("call_{i}"));
                let name = c["function"]["name"]
                    .as_str()
                    .ok_or("tool call missing function.name")?
                    .to_string();
                let raw = c["function"]["arguments"].as_str().unwrap_or("{}");
                let arguments: Value = serde_json::from_str(raw).unwrap_or(json!({}));
                out.push(ToolCall {
                    id,
                    name,
                    arguments,
                });
            }
            return Ok(Turn::ToolCalls(out));
        }
    }
    let content = message["content"].as_str().unwrap_or_default().to_string();
    // Some backends (e.g. llama.cpp serving Gemma/Qwen) return the model's chain-of-thought in a
    // separate `reasoning_content` field rather than inline `<think>` tags. Fold it back in as a
    // `<think>` block so the cockpit can show it as a collapsed "Thinking" disclosure.
    let reasoning = message
        .get("reasoning_content")
        .and_then(|r| r.as_str())
        .unwrap_or_default()
        .trim();
    let final_text = if reasoning.is_empty() {
        content
    } else {
        format!("<think>{reasoning}</think>\n{content}")
    };
    // Fallback: a model that leaked its tool call into `content` as literal template
    // tokens (no structured `tool_calls`) would otherwise end the loop prematurely.
    let leaked = parse_leaked_tool_calls(&final_text);
    if !leaked.is_empty() {
        return Ok(Turn::ToolCalls(leaked));
    }
    Ok(Turn::Final(final_text))
}

/// Recover tool calls that a local model leaked into its `content` as literal
/// template tokens instead of the structured `tool_calls` field.
///
/// Some llama.cpp-served models (observed: Gemma-4, Qwen3.x) intermittently emit
/// their tool call as plain text — even with `--jinja` — when the server's chat
/// template doesn't recognise the model's tool-call syntax. The OpenAI parse path
/// then sees no tool call and the agent loop ends, so a multi-step investigation
/// appears to "abort" mid-analysis. This is a defensive fallback: when no
/// structured call is present we scan the text for the mangled shape and rebuild
/// the call so the loop can continue.
///
/// Recognised (mangled) shape, e.g.:
/// ```text
/// <|tool_call>call:query_traces{sql:<|"|>SELECT 1<|"|>}<tool_call|>
/// ```
/// String argument values are delimited by the `<|"|>` quote marker; bare
/// (unquoted) values are parsed as JSON scalars when possible, else as strings.
fn parse_leaked_tool_calls(content: &str) -> Vec<ToolCall> {
    const OPEN: &str = "<|tool_call>";
    const CLOSE: &str = "<tool_call|>";
    const QUOTE: &str = "<|\"|>";

    let mut calls = Vec::new();
    let mut rest = content;
    let mut idx = 0usize;
    while let Some(start) = rest.find(OPEN) {
        let after_open = &rest[start + OPEN.len()..];
        // Body runs to the closing wrapper if present, else to the end of the text
        // (some models drop the closing token).
        let (segment, consumed) = match after_open.find(CLOSE) {
            Some(end) => (&after_open[..end], start + OPEN.len() + end + CLOSE.len()),
            None => (after_open, rest.len()),
        };
        if let Some(call) = parse_leaked_segment(segment, QUOTE, idx) {
            calls.push(call);
            idx += 1;
        }
        rest = &rest[consumed..];
    }
    // Also recover the Hermes/Qwen XML tool-call form, which some llama.cpp-served models
    // (observed: Qwen3.x) emit as content — notably on a no-tools wrap-up turn, where the
    // server's tool-call grammar is not applied. e.g.:
    //   <tool_call><function=simulate><parameter=limit>25</parameter>...</function></tool_call>
    calls.extend(parse_hermes_tool_calls(content));
    calls
}

/// Recover Hermes/Qwen-style XML tool calls leaked into `content`:
/// `<tool_call><function=NAME><parameter=KEY>VALUE</parameter>…</function></tool_call>`.
/// Also accepts a JSON body (`<tool_call>{"name":…,"arguments":{…}}</tool_call>`).
///
/// Qwen3-Coder-30B intermittently drops the opening `<tool_call>` wrapper, emitting a
/// bare `<function=NAME>…</function>` (often trailed by a stray `</tool_call>`). So the
/// XML form is anchored directly on `<function=` rather than on the wrapper, which makes
/// recovery robust to a missing (or duplicated) `<tool_call>` tag.
fn parse_hermes_tool_calls(content: &str) -> Vec<ToolCall> {
    const OPEN: &str = "<tool_call>";
    const CLOSE: &str = "</tool_call>";
    const FN_OPEN: &str = "<function=";
    const FN_CLOSE: &str = "</function>";
    let mut calls = Vec::new();
    let mut idx = 0usize;

    // 1) JSON-body calls wrapped in <tool_call>{…}</tool_call>. (XML-body calls are
    //    handled by the <function=-anchored pass below, so only JSON segments here.)
    let mut rest = content;
    while let Some(start) = rest.find(OPEN) {
        let after_open = &rest[start + OPEN.len()..];
        let (segment, consumed) = match after_open.find(CLOSE) {
            Some(end) => (&after_open[..end], start + OPEN.len() + end + CLOSE.len()),
            None => (after_open, rest.len()),
        };
        let seg = segment.trim();
        if seg.starts_with('{') {
            if let Some(call) = parse_hermes_segment(seg, idx) {
                calls.push(call);
                idx += 1;
            }
        }
        rest = &rest[consumed..];
    }

    // 2) XML `<function=NAME>…</function>` blocks, with or without the <tool_call> wrapper.
    let mut rest = content;
    while let Some(start) = rest.find(FN_OPEN) {
        let region = &rest[start..];
        let (segment, consumed) = match region.find(FN_CLOSE) {
            Some(end) => (
                &region[..end + FN_CLOSE.len()],
                start + end + FN_CLOSE.len(),
            ),
            None => (region, rest.len()),
        };
        if let Some(call) = parse_hermes_segment(segment.trim(), idx) {
            calls.push(call);
            idx += 1;
        }
        rest = &rest[consumed..];
    }

    calls
}

/// Parse one Hermes `<function=NAME>…</function>` (or JSON) tool-call body.
fn parse_hermes_segment(segment: &str, idx: usize) -> Option<ToolCall> {
    // JSON body form: {"name": "...", "arguments": {...}}
    if segment.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<Value>(segment) {
            let name = v.get("name").and_then(|n| n.as_str())?.to_string();
            if name.is_empty() {
                return None;
            }
            let arguments = v.get("arguments").cloned().unwrap_or_else(|| json!({}));
            return Some(ToolCall {
                id: format!("hermes_call_{idx}"),
                name,
                arguments,
            });
        }
        return None;
    }
    // XML body form: <function=NAME> <parameter=KEY>VALUE</parameter> … </function>
    const FN_OPEN: &str = "<function=";
    let fn_start = segment.find(FN_OPEN)?;
    let after_fn = &segment[fn_start + FN_OPEN.len()..];
    let name_end = after_fn.find('>')?;
    let name = after_fn[..name_end].trim().to_string();
    if name.is_empty() {
        return None;
    }
    let mut body = &after_fn[name_end + 1..];
    let mut map = serde_json::Map::new();
    const P_OPEN: &str = "<parameter=";
    const P_CLOSE: &str = "</parameter>";
    while let Some(p) = body.find(P_OPEN) {
        let after_p = &body[p + P_OPEN.len()..];
        let Some(key_end) = after_p.find('>') else {
            break;
        };
        let key = after_p[..key_end].trim().to_string();
        let val_region = &after_p[key_end + 1..];
        let (raw_val, advance) = match val_region.find(P_CLOSE) {
            Some(end) => (&val_region[..end], end + P_CLOSE.len()),
            None => (val_region, val_region.len()),
        };
        let val = raw_val.trim();
        if !key.is_empty() {
            // Keep XML/multiline values as strings; coerce bare scalars (e.g. 25, true) to JSON.
            let parsed = serde_json::from_str::<Value>(val)
                .ok()
                .filter(|p| p.is_number() || p.is_boolean())
                .unwrap_or_else(|| Value::String(val.to_string()));
            map.insert(key, parsed);
        }
        body = &val_region[advance..];
    }
    Some(ToolCall {
        id: format!("hermes_call_{idx}"),
        name,
        arguments: Value::Object(map),
    })
}

/// Remove any leaked tool-call markup spans (the `<|tool_call>…<tool_call|>` template form,
/// the Hermes `<tool_call>…</tool_call>` XML form, and bare `<function=…></function>` blocks
/// that Qwen3-Coder leaks without the wrapper) from a final answer, returning the trimmed
/// remainder. Used to keep a forced wrap-up from ending on a wall of raw markup when the model
/// emits a tool call despite being told to answer in prose.
fn strip_leaked_tool_markup(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    loop {
        let tmpl = rest.find("<|tool_call>");
        let hermes = rest.find("<tool_call>");
        let bare_fn = rest.find("<function=");
        let Some(start) = [tmpl, hermes, bare_fn].into_iter().flatten().min() else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..start]);
        let tail = &rest[start..];
        let consumed = if tail.starts_with("<|tool_call>") {
            tail.find("<tool_call|>")
                .map(|e| e + "<tool_call|>".len())
                .unwrap_or(tail.len())
        } else if tail.starts_with("<tool_call>") {
            tail.find("</tool_call>")
                .map(|e| e + "</tool_call>".len())
                .unwrap_or(tail.len())
        } else {
            // Bare `<function=…></function>` (no wrapper). Consume through the closing
            // `</function>`, then also swallow a trailing stray `</tool_call>` if present.
            let mut end = tail
                .find("</function>")
                .map(|e| e + "</function>".len())
                .unwrap_or(tail.len());
            let after = tail[end..].trim_start();
            if let Some(stray) = after.strip_prefix("</tool_call>") {
                end = tail.len() - stray.len();
            }
            end
        };
        rest = &tail[consumed..];
    }
    out.trim().to_string()
}

/// Parse one `call:NAME{ ... }` segment into a [`ToolCall`].
fn parse_leaked_segment(segment: &str, quote: &str, idx: usize) -> Option<ToolCall> {
    let seg = segment.trim();
    let seg = seg.strip_prefix("call:").unwrap_or(seg).trim_start();
    let brace = seg.find('{')?;
    let name = seg[..brace].trim().trim_matches(|c| c == ':' || c == ' ');
    if name.is_empty() {
        return None;
    }
    let close = seg.rfind('}')?;
    if close < brace {
        return None;
    }
    let body = &seg[brace + 1..close];
    let arguments = parse_leaked_args(body, quote);
    Some(ToolCall {
        id: format!("leaked_call_{idx}"),
        name: name.to_string(),
        arguments,
    })
}

/// Parse a `key:<|"|>value<|"|>, key2:bare` argument body into a JSON object.
fn parse_leaked_args(body: &str, quote: &str) -> Value {
    let mut map = serde_json::Map::new();
    let mut rest = body;
    while !rest.trim().is_empty() {
        // Advance to the next key, skipping separators left by the previous value.
        rest = rest.trim_start_matches([',', ' ', '\n', '\t', '\r']);
        let colon = match rest.find(':') {
            Some(c) => c,
            None => break,
        };
        let key = rest[..colon].trim().trim_matches('"').to_string();
        let mut after = rest[colon + 1..].trim_start();
        if key.is_empty() {
            break;
        }
        if let Some(stripped) = after.strip_prefix(quote) {
            // Quoted string value: read until the closing quote marker.
            match stripped.find(quote) {
                Some(endq) => {
                    let val = &stripped[..endq];
                    map.insert(key, Value::String(val.to_string()));
                    after = &stripped[endq + quote.len()..];
                }
                None => {
                    // Unterminated quote: take the remainder as the value and stop.
                    map.insert(key, Value::String(stripped.to_string()));
                    break;
                }
            }
        } else {
            // Bare value up to the next comma; parse as a JSON scalar when possible.
            let end = after.find(',').unwrap_or(after.len());
            let raw = after[..end].trim();
            let val = serde_json::from_str::<Value>(raw)
                .unwrap_or_else(|_| Value::String(raw.to_string()));
            map.insert(key, val);
            after = &after[end..];
        }
        rest = after;
    }
    Value::Object(map)
}

/// Heuristic: did the model end a turn by *promising* a concrete next action
/// (author/run a variant, call a tool) without actually performing it? Such an
/// "I will now ..." turn is a genuine final answer with no tool call, so it would
/// silently end the agent loop mid-investigation. Conservative by design — it only
/// fires on an explicit self-commitment adjacent to an action verb, so ordinary
/// recommendation prose ("I would not change the logic") never trips it.
fn signals_deferred_action(answer: &str) -> bool {
    let t = answer.to_lowercase();
    // Direct commitments to author/simulate a variant — unambiguous on their own.
    const DIRECT: [&str; 8] = [
        "i will author",
        "i'll author",
        "let me author",
        "i will simulate",
        "i'll simulate",
        "let me simulate",
        "i will now author",
        "i will now simulate",
    ];
    if DIRECT.iter().any(|p| t.contains(p)) {
        return true;
    }
    // Generic commitment markers must sit next to an action verb to count.
    const COMMIT: [&str; 6] = [
        "i will now",
        "i'll now",
        "let me now",
        "next step: i will",
        "next step: i'll",
        "i am going to",
    ];
    const ACTION: [&str; 8] = [
        "author", "simulate", "fork", "variant", "model", "build", "create", "run ",
    ];
    for c in COMMIT {
        if let Some(pos) = t.find(c) {
            let end = (pos + 120).min(t.len());
            let window = &t[pos..end];
            if ACTION.iter().any(|a| window.contains(a)) {
                return true;
            }
        }
    }
    false
}

/// True when a "final" answer carries chain-of-thought but no actual user-facing
/// content — the model only *thought* and produced neither an answer nor a tool
/// call (commonly because it exhausted its output-token budget mid-reasoning).
/// Such a turn renders as a blank droid bubble ("cut off while thinking").
fn is_thinking_only(answer: &str) -> bool {
    answer.contains("<think>") && strip_think_blocks(answer).trim().is_empty()
}

/// Remove `<think>…</think>` reasoning blocks, returning the user-facing remainder.
/// An unterminated `<think>` (a turn truncated mid-thought) drops everything after
/// the opening tag, since none of it is user-facing content.
fn strip_think_blocks(s: &str) -> String {
    const OPEN: &str = "<think>";
    const CLOSE: &str = "</think>";
    let mut out = String::new();
    let mut rest = s;
    while let Some(start) = rest.find(OPEN) {
        out.push_str(&rest[..start]);
        match rest[start + OPEN.len()..].find(CLOSE) {
            Some(end) => rest = &rest[start + OPEN.len() + end + CLOSE.len()..],
            None => {
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Detect a *runaway* final turn: the model rambles, repeating the same line many times
/// (often writing SQL as backtick prose instead of calling `query_traces`), filling the
/// output budget without ever finishing or calling a tool. Conservative: only fires on a
/// long answer where some non-trivial line recurs many times, so ordinary prose — which
/// does not repeat a 12+-char line six times — is never misclassified.
fn is_runaway_repetition(answer: &str) -> bool {
    const MIN_LEN: usize = 3000;
    answer.len() >= MIN_LEN && has_repeated_line(answer, RUNAWAY_MIN_LINE, RUNAWAY_MAX_REPEATS)
}

/// First accumulated length at which `step_streaming` starts watching for a runaway, and
/// the gap between subsequent checks. The minimum is kept above `is_runaway_repetition`'s
/// own length floor so that when we break early the partial text still trips the
/// harness-level nudge.
const RUNAWAY_TAIL_MIN: usize = 3500;
const RUNAWAY_TAIL_STEP: usize = 1000;
const RUNAWAY_MIN_LINE: usize = 12;
const RUNAWAY_MAX_REPEATS: usize = 6;

/// True if any single trimmed line of at least `min_line` chars occurs `max_repeats`+ times.
fn has_repeated_line(s: &str, min_line: usize, max_repeats: usize) -> bool {
    let mut counts: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for line in s.lines() {
        let l = line.trim();
        if l.len() >= min_line {
            let c = counts.entry(l).or_insert(0);
            *c += 1;
            if *c >= max_repeats {
                return true;
            }
        }
    }
    false
}

/// Streaming guard: does the *tail* of an in-flight generation already show a runaway
/// (the same line repeating)? Only the last window is scanned so the per-chunk cost stays
/// bounded regardless of how much has streamed.
fn runaway_tail(s: &str) -> bool {
    const TAIL: usize = 2500;
    let mut start = s.len().saturating_sub(TAIL);
    while start < s.len() && !s.is_char_boundary(start) {
        start += 1;
    }
    has_repeated_line(&s[start..], RUNAWAY_MIN_LINE, RUNAWAY_MAX_REPEATS)
}

/// SQL over and over instead of acting on the result). Tool name plus normalised
/// arguments: JSON object keys are sorted and string values whitespace-collapsed and
/// lower-cased, so trivially-reformatted repeats (re-indented SQL, case changes) still
/// collide. Multiple calls in one round are joined in order.
fn tool_call_signature(calls: &[ToolCall]) -> String {
    fn norm(v: &Value) -> String {
        match v {
            Value::String(s) => s
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase(),
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                keys.into_iter()
                    .map(|k| format!("{k}={}", norm(&map[k])))
                    .collect::<Vec<_>>()
                    .join(",")
            }
            Value::Array(items) => items.iter().map(norm).collect::<Vec<_>>().join(";"),
            other => other.to_string(),
        }
    }
    calls
        .iter()
        .map(|c| format!("{}({})", c.name, norm(&c.arguments)))
        .collect::<Vec<_>>()
        .join("\u{1}")
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    /// A deterministic mock model: replays a scripted list of turns, one per step.
    struct ScriptedModel {
        turns: Vec<Vec<ToolCall>>, // empty vec => final answer
        idx: Cell<usize>,
        final_answer: String,
    }

    impl AgentStep for ScriptedModel {
        async fn step(&self, _msgs: &[Msg], _tools: &[ToolSpec]) -> Result<Turn, String> {
            let i = self.idx.get();
            self.idx.set(i + 1);
            match self.turns.get(i) {
                Some(calls) if !calls.is_empty() => Ok(Turn::ToolCalls(calls.clone())),
                _ => Ok(Turn::Final(self.final_answer.clone())),
            }
        }
    }

    struct EchoTools;
    impl ToolBox for EchoTools {
        fn specs(&self) -> Vec<ToolSpec> {
            vec![ToolSpec {
                name: "echo".into(),
                description: "echo the input".into(),
                parameters: json!({"type":"object","properties":{"x":{"type":"string"}}}),
            }]
        }
        fn call(&self, name: &str, args: &Value) -> Result<String, String> {
            if name != "echo" {
                return Err(format!("unknown tool {name}"));
            }
            Ok(format!("echoed:{}", args["x"].as_str().unwrap_or("")))
        }
    }

    #[tokio::test]
    async fn loop_dispatches_tools_then_returns_final_answer() {
        let model = ScriptedModel {
            turns: vec![vec![ToolCall {
                id: "c1".into(),
                name: "echo".into(),
                arguments: json!({"x": "hi"}),
            }]],
            idx: Cell::new(0),
            final_answer: "done".into(),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 5).await.unwrap();
        assert_eq!(run.answer, "done");
        assert_eq!(run.steps.len(), 1);
        assert_eq!(run.steps[0].tool, "echo");
        assert_eq!(run.steps[0].result, "echoed:hi");
        assert_eq!(run.rounds, 2);
    }

    #[tokio::test]
    async fn budget_exhaustion_wraps_up_instead_of_erroring() {
        // A model that keeps asking for tools hits the round budget; rather than erroring,
        // the loop withholds tools and forces a prose wrap-up so the operator still gets
        // findings. The scripted model returns its final answer once tools are withheld.
        let model = ScriptedModel {
            turns: vec![
                vec![ToolCall {
                    id: "c".into(),
                    name: "echo".into(),
                    arguments: json!({"x":"a"}),
                }],
                vec![ToolCall {
                    id: "c".into(),
                    name: "echo".into(),
                    arguments: json!({"x":"b"}),
                }],
            ],
            idx: Cell::new(0),
            final_answer: "wrapped up".into(),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 2).await.unwrap();
        assert_eq!(run.answer, "wrapped up");
        assert_eq!(run.rounds, 2);
        // Both tool calls were executed before the budget hit.
        assert_eq!(run.steps.len(), 2);
    }

    #[tokio::test]
    async fn cancel_flag_forces_an_early_wrap_up() {
        use std::sync::atomic::{AtomicBool, Ordering};
        // The cancel flag is already set before the loop starts, so round 1 short-circuits
        // straight to the wrap-up (no tools), returning the model's prose answer.
        let model = ScriptedModel {
            turns: vec![],
            idx: Cell::new(0),
            final_answer: "early findings".into(),
        };
        let cancel = AtomicBool::new(true);
        let mut msgs = vec![Msg::System("sys".into()), Msg::User("go".into())];
        let run = run_agent_resumable_cancellable(&model, &EchoTools, &mut msgs, 5, Some(&cancel))
            .await
            .unwrap();
        assert_eq!(run.answer, "early findings");
        // No tools were dispatched — the very first scripted tool turn was never reached.
        assert!(run.steps.is_empty());
        // The wrap-up was triggered on round 1.
        assert_eq!(run.rounds, 1);
        // Flag untouched by the loop.
        assert!(cancel.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn a_queued_steer_is_injected_as_a_user_turn_mid_loop() {
        use std::sync::Mutex;
        // Round 1 calls a tool, round 2 gives the final answer. A steer queued before the
        // run is drained at the round-1 boundary and appended as a user turn, so it lands in
        // the transcript and is in context for the model's subsequent rounds.
        let model = ScriptedModel {
            turns: vec![vec![ToolCall {
                id: "c".into(),
                name: "echo".into(),
                arguments: json!({"x":"a"}),
            }]],
            idx: Cell::new(0),
            final_answer: "done".into(),
        };
        let steer = Mutex::new(vec!["focus on credit-check".to_string()]);
        let mut msgs = vec![Msg::System("sys".into()), Msg::User("go".into())];
        let mut sink = |_ev: AgentEvent| {};
        let run = run_agent_streaming(
            &model,
            &EchoTools,
            &mut msgs,
            5,
            None,
            Some(&steer),
            &mut sink,
            None,
        )
        .await
        .unwrap();
        assert_eq!(run.answer, "done");
        // The steer was consumed from the queue …
        assert!(steer.lock().unwrap().is_empty());
        // … and appears as a user message in the transcript.
        assert!(
            msgs.iter()
                .any(|m| matches!(m, Msg::User(t) if t == "focus on credit-check")),
            "steer must be injected as a user turn: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn the_completion_id_delta_is_forwarded_to_the_sink() {
        // A model whose streaming step surfaces the OpenAI completion id (as the real transport
        // does from the first SSE chunk). run_agent_streaming must forward it as
        // AgentEvent::Completion so the host can target the live turn with reasoning control.
        struct IdModel;
        impl AgentStep for IdModel {
            async fn step(&self, _m: &[Msg], _t: &[ToolSpec]) -> Result<Turn, String> {
                Ok(Turn::Final("done".into()))
            }
            async fn step_streaming(
                &self,
                _m: &[Msg],
                _t: &[ToolSpec],
                on_delta: &mut dyn FnMut(Delta),
                _c: Option<&std::sync::atomic::AtomicBool>,
            ) -> Result<Turn, String> {
                on_delta(Delta::Completion("chatcmpl-xyz".into()));
                on_delta(Delta::Answer("done".into()));
                Ok(Turn::Final("done".into()))
            }
        }
        let mut msgs = vec![Msg::System("sys".into()), Msg::User("go".into())];
        let mut seen: Vec<String> = Vec::new();
        let mut sink = |ev: AgentEvent| {
            if let AgentEvent::Completion { id } = ev {
                seen.push(id);
            }
        };
        run_agent_streaming(
            &IdModel, &EchoTools, &mut msgs, 3, None, None, &mut sink, None,
        )
        .await
        .unwrap();
        assert_eq!(seen, vec!["chatcmpl-xyz".to_string()]);
    }

    #[test]
    fn the_request_body_arms_reasoning_control() {
        // The agent's generation request must carry reasoning_control:true so the loop monitor /
        // wrap-up can end the turn's thinking mid-generation via the control endpoint.
        let cfg = LlmConfig {
            provider: crate::harness::llm::Provider::Openai,
            base_url: "http://127.0.0.1:8080/v1".into(),
            model: "m".into(),
            api_key: None,
            max_tokens: 256,
            temperature: 0.2,
            frequency_penalty: 0.0,
            thinking_level: None,
            grammar: None,
        };
        let agent = OpenAiAgent { cfg };
        let body = agent.request_body(&[Msg::User("hi".into())], &[], true);
        assert_eq!(body["reasoning_control"], serde_json::json!(true));
    }

    #[tokio::test]
    async fn checkpoint_is_invoked_with_the_running_transcript() {
        use std::cell::RefCell;
        // The checkpoint must fire at the round-1 boundary (capturing system+user before the
        // model is even called) and again after the tool results land — so a turn that later
        // dies still leaves the user message and any tool work on disk.
        let model = ScriptedModel {
            turns: vec![vec![ToolCall {
                id: "c".into(),
                name: "echo".into(),
                arguments: json!({"x":"a"}),
            }]],
            idx: Cell::new(0),
            final_answer: "done".into(),
        };
        let mut msgs = vec![Msg::System("sys".into()), Msg::User("go".into())];
        let mut sink = |_ev: AgentEvent| {};
        // Record the transcript length at each checkpoint call.
        let snapshots: RefCell<Vec<usize>> = RefCell::new(Vec::new());
        let mut cp = |m: &[Msg]| snapshots.borrow_mut().push(m.len());
        let run = run_agent_streaming(
            &model,
            &EchoTools,
            &mut msgs,
            5,
            None,
            None,
            &mut sink,
            Some(&mut cp),
        )
        .await
        .unwrap();
        assert_eq!(run.answer, "done");
        let snaps = snapshots.into_inner();
        // First checkpoint (round 1 top) sees exactly system+user …
        assert_eq!(
            snaps.first(),
            Some(&2),
            "round-1 checkpoint snapshots: {snaps:?}"
        );
        // … and a later checkpoint (after tool results) sees a longer transcript.
        assert!(
            snaps.iter().any(|&n| n > 2),
            "expected a post-tool checkpoint with more messages: {snaps:?}"
        );
    }

    #[test]
    fn parses_openai_tool_call_message() {
        let msg = json!({
            "content": null,
            "tool_calls": [{
                "id": "abc",
                "type": "function",
                "function": { "name": "query_traces", "arguments": "{\"sql\":\"SELECT 1\"}" }
            }]
        });
        match parse_openai_turn(&msg).unwrap() {
            Turn::ToolCalls(calls) => {
                assert_eq!(calls[0].name, "query_traces");
                assert_eq!(calls[0].arguments["sql"], "SELECT 1");
            }
            Turn::Final(_) => panic!("expected tool calls"),
        }
    }

    #[test]
    fn parses_openai_final_message() {
        let msg = json!({ "content": "the answer" });
        match parse_openai_turn(&msg).unwrap() {
            Turn::Final(s) => assert_eq!(s, "the answer"),
            Turn::ToolCalls(_) => panic!("expected final"),
        }
    }

    #[test]
    fn folds_reasoning_content_into_a_think_block() {
        let msg = json!({ "content": "the answer", "reasoning_content": "  step one\nstep two  " });
        match parse_openai_turn(&msg).unwrap() {
            Turn::Final(s) => assert_eq!(s, "<think>step one\nstep two</think>\nthe answer"),
            Turn::ToolCalls(_) => panic!("expected final"),
        }
    }

    #[test]
    fn recovers_a_tool_call_leaked_into_content() {
        // Verbatim shape observed from Gemma-4 in Investigation 5: the model emitted
        // its tool call as literal template tokens in `content` with empty tool_calls.
        let leaked = "</think><|tool_call>call:query_traces{sql:<|\"|>SELECT element_id, count(*) as count FROM incidents GROUP BY element_id<|\"|>}<tool_call|></think>\n";
        let msg = json!({ "content": leaked, "tool_calls": [] });
        match parse_openai_turn(&msg).unwrap() {
            Turn::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].name, "query_traces");
                assert_eq!(
                    calls[0].arguments["sql"],
                    "SELECT element_id, count(*) as count FROM incidents GROUP BY element_id"
                );
            }
            Turn::Final(_) => panic!("expected the leaked tool call to be recovered"),
        }
    }

    #[test]
    fn structured_tool_calls_take_precedence_over_leak_scan() {
        // A normal structured call must never be re-parsed by the fallback.
        let msg = json!({
            "content": "irrelevant prose with no markers",
            "tool_calls": [{
                "id": "abc",
                "type": "function",
                "function": { "name": "read_model", "arguments": "{}" }
            }]
        });
        match parse_openai_turn(&msg).unwrap() {
            Turn::ToolCalls(calls) => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id, "abc");
                assert_eq!(calls[0].name, "read_model");
            }
            Turn::Final(_) => panic!("expected structured tool call"),
        }
    }

    #[test]
    fn plain_prose_is_never_misread_as_a_tool_call() {
        let msg = json!({ "content": "The bottleneck is credit-check; no action token here." });
        match parse_openai_turn(&msg).unwrap() {
            Turn::Final(s) => assert!(s.contains("bottleneck")),
            Turn::ToolCalls(_) => panic!("plain prose must stay a final answer"),
        }
    }

    #[test]
    fn parses_leaked_call_with_missing_close_token_and_bare_arg() {
        // Defensive: closing wrapper dropped, plus a bare (unquoted) scalar arg.
        let leaked = "<|tool_call>call:simulate{limit:500}";
        let calls = parse_leaked_tool_calls(leaked);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "simulate");
        assert_eq!(calls[0].arguments["limit"], 500);
    }

    #[test]
    fn recovers_a_hermes_xml_tool_call_leaked_into_content() {
        // Verbatim shape observed from Qwen in Investigation 3: on the no-tools wrap-up turn the
        // model emitted a Hermes `<tool_call><function=…>` block as content. It must be recovered
        // as a structured call (so a non-wrap-up turn keeps iterating instead of stalling).
        let leaked = "<tool_call>\n<function=simulate>\n<parameter=limit>\n25\n</parameter>\n<parameter=model>\n<bpmn:definitions><bpmn:process id=\"loan\"/></bpmn:definitions>\n</parameter>\n<parameter=name>\nbaseline\n</parameter>\n</function>\n</tool_call>";
        let calls = parse_leaked_tool_calls(leaked);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "simulate");
        assert_eq!(calls[0].arguments["limit"], 25);
        assert_eq!(calls[0].arguments["name"], "baseline");
        assert!(calls[0].arguments["model"]
            .as_str()
            .unwrap()
            .contains("bpmn:process"));
    }

    #[test]
    fn recovers_a_hermes_json_body_tool_call() {
        let leaked = "<tool_call>{\"name\": \"read_model\", \"arguments\": {}}</tool_call>";
        let calls = parse_leaked_tool_calls(leaked);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read_model");
    }

    #[test]
    fn recovers_a_bare_function_call_without_the_tool_call_wrapper() {
        // Verbatim shape observed from Qwen3-Coder-30B in Investigation 2: the model dropped the
        // opening `<tool_call>` tag and emitted a bare `<function=…></function>` (with a stray
        // closing `</tool_call>`). Earlier this leaked as content and the loop stalled, so the
        // operator saw the model "unable to use the tools".
        let leaked = "I'll try again to read the model structure.\n\n<function=read_model>\n</function>\n</tool_call>";
        let calls = parse_leaked_tool_calls(leaked);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read_model");
        assert!(calls[0].arguments.as_object().unwrap().is_empty());
    }

    #[test]
    fn recovers_a_bare_function_call_with_parameters() {
        let leaked =
            "<function=simulate>\n<parameter=limit>\n25\n</parameter>\n<parameter=name>\nbaseline\n</parameter>\n</function>";
        let calls = parse_leaked_tool_calls(leaked);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "simulate");
        assert_eq!(calls[0].arguments["limit"], 25);
        assert_eq!(calls[0].arguments["name"], "baseline");
    }

    #[test]
    fn does_not_double_count_a_wrapped_xml_call() {
        // A properly wrapped XML call must still yield exactly one call (the JSON pass skips it,
        // the <function=-anchored pass claims it once).
        let leaked = "<tool_call><function=read_model></function></tool_call>";
        let calls = parse_leaked_tool_calls(leaked);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read_model");
    }

    #[test]
    fn strip_leaked_tool_markup_clears_a_hermes_block_but_keeps_prose() {
        let answer = "Here is my conclusion.\n<tool_call>\n<function=simulate>\n<parameter=limit>\n25\n</parameter>\n</function>\n</tool_call>";
        assert_eq!(strip_leaked_tool_markup(answer), "Here is my conclusion.");
        // A pure-markup answer collapses to empty (the wrap-up fallback then kicks in).
        let only =
            "<tool_call><function=simulate><parameter=limit>25</parameter></function></tool_call>";
        assert_eq!(strip_leaked_tool_markup(only), "");
        // The bare (wrapper-less) Qwen3-Coder form is also stripped, stray </tool_call> included.
        let bare = "Done.\n<function=read_model>\n</function>\n</tool_call>";
        assert_eq!(strip_leaked_tool_markup(bare), "Done.");
        // Plain prose is untouched.
        assert_eq!(strip_leaked_tool_markup("just prose"), "just prose");
    }

    #[test]
    fn deferred_action_detector_fires_on_commitment_not_on_recommendation() {
        // Verbatim tail from Investigation 5 message [18] — the droid promised to act.
        assert!(signals_deferred_action(
            "Next Step: I will now author the Retry Logic variant to see if we can recover those instances."
        ));
        assert!(signals_deferred_action(
            "Let me author a variant that parallelises the two tasks."
        ));
        // Plain recommendation / refusal prose must NOT trip the detector.
        assert!(!signals_deferred_action(
            "I would not change the logic; this is a provisioning problem, not a design one."
        ));
        assert!(!signals_deferred_action(
            "The bottleneck is credit-check. My recommendation is to scale the worker pool."
        ));
    }

    /// A mock that replays a scripted sequence of turns (deferral text, tool call, or done).
    enum Scripted {
        Defer(String),
        Call(ToolCall),
        Done(String),
    }
    struct SequencedModel {
        script: Vec<Scripted>,
        idx: Cell<usize>,
    }
    impl AgentStep for SequencedModel {
        async fn step(&self, _msgs: &[Msg], _tools: &[ToolSpec]) -> Result<Turn, String> {
            let i = self.idx.get();
            self.idx.set(i + 1);
            match self.script.get(i) {
                Some(Scripted::Call(c)) => Ok(Turn::ToolCalls(vec![c.clone()])),
                Some(Scripted::Defer(s)) | Some(Scripted::Done(s)) => Ok(Turn::Final(s.clone())),
                None => Ok(Turn::Final("(exhausted)".into())),
            }
        }
    }

    #[tokio::test]
    async fn deferred_final_is_nudged_to_actually_call_the_tool() {
        // Round 1: the model defers ("I will now author..."). The loop must NOT end —
        // it nudges, and round 2 the model issues the tool call, then round 3 finishes.
        let model = SequencedModel {
            script: vec![
                Scripted::Defer("Next step: I will now author and simulate the variant.".into()),
                Scripted::Call(ToolCall {
                    id: "c1".into(),
                    name: "echo".into(),
                    arguments: json!({"x": "variant"}),
                }),
                Scripted::Done("Here is the evidence-backed result.".into()),
            ],
            idx: Cell::new(0),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 8).await.unwrap();
        assert_eq!(run.answer, "Here is the evidence-backed result.");
        // The tool actually ran (the deferral did not end the investigation).
        assert_eq!(run.steps.len(), 1);
        assert_eq!(run.steps[0].tool, "echo");
    }

    #[tokio::test]
    async fn persistent_deferral_is_bounded_and_still_terminates() {
        // A model that ALWAYS defers must not loop forever: after MAX_AUTO_CONTINUES
        // nudges the loop accepts the final answer and returns.
        let model = SequencedModel {
            script: vec![
                Scripted::Defer("I will now author the variant.".into()),
                Scripted::Defer("I will now author the variant.".into()),
                Scripted::Defer("I will now author the variant.".into()),
                Scripted::Defer("I will now author the variant.".into()),
            ],
            idx: Cell::new(0),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 8).await.unwrap();
        assert_eq!(run.answer, "I will now author the variant.");
        assert!(run.steps.is_empty());
    }

    #[test]
    fn thinking_only_detector_distinguishes_blank_reasoning_from_real_answers() {
        // Reasoning with no visible content (incl. a truncated, unterminated block).
        assert!(is_thinking_only("<think>lots of reasoning here</think>\n"));
        assert!(is_thinking_only(
            "<think>authored XML then ran out of tokens"
        ));
        assert!(is_thinking_only("<think>a</think>   \n  "));
        // A real answer (with or without a preceding think block) is not thinking-only.
        assert!(!is_thinking_only(
            "<think>reasoned</think>\nThe bottleneck is credit-check."
        ));
        assert!(!is_thinking_only("Plain final answer, no thinking."));
        assert!(!is_thinking_only(""));
    }

    #[tokio::test]
    async fn reasoning_only_turn_is_nudged_to_finish_instead_of_returning_blank() {
        // Round 1: the model produces only a (truncated) think block — no answer, no
        // tool call. The loop must nudge rather than return a blank answer; round 2 the
        // model recovers with a real final answer.
        let model = SequencedModel {
            script: vec![
                Scripted::Defer("<think>I authored a huge BPMN variant and ran out of room".into()),
                Scripted::Done("Variant simulated: conservedRate 1.0, P99 down 38x.".into()),
            ],
            idx: Cell::new(0),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 8).await.unwrap();
        assert_eq!(
            run.answer,
            "Variant simulated: conservedRate 1.0, P99 down 38x."
        );
    }

    #[test]
    fn tool_call_signature_collides_on_trivial_reformatting_only() {
        let a = vec![ToolCall {
            id: "1".into(),
            name: "query_traces".into(),
            arguments: json!({"sql": "SELECT * FROM jobs WHERE x = 1"}),
        }];
        let b = vec![ToolCall {
            id: "2".into(), // different id, reindented + recased SQL
            name: "query_traces".into(),
            arguments: json!({"sql": "  select   *\n  from JOBS\n  where x = 1  "}),
        }];
        let c = vec![ToolCall {
            id: "3".into(),
            name: "query_traces".into(),
            arguments: json!({"sql": "SELECT * FROM incidents"}),
        }];
        assert_eq!(tool_call_signature(&a), tool_call_signature(&b));
        assert_ne!(tool_call_signature(&a), tool_call_signature(&c));
    }

    #[test]
    fn wire_messages_strips_reasoning_and_clips_old_tool_results() {
        let big = "X".repeat(9000);
        let mut msgs = vec![
            Msg::System("sys".into()),
            Msg::User("go".into()),
            // An old, oversized tool result (e.g. a full read_model XML) — should be clipped.
            Msg::Tool {
                call_id: "t0".into(),
                content: big.clone(),
            },
            // An assistant turn whose text is pure chain-of-thought — should not be resent.
            Msg::Assistant {
                text: Some("<think>I will author a big BPMN variant…</think>".into()),
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "simulate".into(),
                    arguments: json!({"model":"<x/>"}),
                }],
            },
        ];
        let old_tool_idx = 2;
        let old_asst_idx = 3;
        // Pad with later rounds so the result above is well outside KEEP_RECENT of the end.
        for k in 0..8 {
            msgs.push(Msg::User(format!("round {k}")));
        }
        // Recent messages (within KEEP_RECENT of the end) — kept verbatim.
        msgs.push(Msg::Tool {
            call_id: "c9".into(),
            content: big.clone(),
        });
        let recent_tool_idx = msgs.len() - 1;
        msgs.push(Msg::Assistant {
            text: Some("<think>done</think>The bottleneck is credit-check.".into()),
            tool_calls: vec![],
        });
        let final_asst_idx = msgs.len() - 1;
        let wire = wire_messages(&msgs);

        // Reasoning is gone from BOTH assistant turns; the user-facing answer survives.
        assert_eq!(wire[old_asst_idx]["content"], "");
        assert_eq!(
            wire[old_asst_idx]["tool_calls"][0]["function"]["name"],
            "simulate"
        );
        let final_content = wire[final_asst_idx]["content"].as_str().unwrap();
        assert!(!final_content.contains("<think>"));
        assert!(final_content.contains("credit-check"));

        // The OLD oversized tool result is clipped; the RECENT one is kept whole.
        let old_tool = wire[old_tool_idx]["content"].as_str().unwrap();
        assert!(old_tool.len() < big.len());
        assert!(old_tool.contains("truncated"));
        assert_eq!(
            wire[recent_tool_idx]["content"].as_str().unwrap().len(),
            big.len()
        );
    }

    #[test]
    fn wire_messages_downgrades_mid_conversation_system_to_user() {
        // A leading system prompt must stay `system`; any system directive injected later
        // (a steer nudge or a forced wrap-up) must be resent as a `user` turn so strict chat
        // templates (e.g. llama.cpp / Ornith) don't raise "System message must be at the beginning".
        let msgs = vec![
            Msg::System("persona system prompt".into()),
            Msg::User("investigate".into()),
            Msg::Assistant {
                text: Some("looking…".into()),
                tool_calls: vec![],
            },
            // Mid-conversation directive — historically pushed as Msg::System.
            Msg::System("Stop investigating now and report your findings so far.".into()),
        ];
        let wire = wire_messages(&msgs);
        assert_eq!(
            wire[0]["role"], "system",
            "leading system prompt is preserved"
        );
        assert_eq!(wire[0]["content"], "persona system prompt");
        assert_eq!(
            wire[3]["role"], "user",
            "a mid-conversation system directive must be resent as a user turn"
        );
        assert_eq!(
            wire[3]["content"],
            "Stop investigating now and report your findings so far."
        );
    }

    struct AlwaysSameCall {
        call: ToolCall,
        summary: String,
    }
    impl AgentStep for AlwaysSameCall {
        async fn step(&self, _msgs: &[Msg], tools: &[ToolSpec]) -> Result<Turn, String> {
            if tools.is_empty() {
                Ok(Turn::Final(self.summary.clone()))
            } else {
                Ok(Turn::ToolCalls(vec![self.call.clone()]))
            }
        }
    }

    #[tokio::test]
    async fn repeated_identical_tool_call_is_broken_before_max_rounds() {
        // A model stuck re-running the same query_traces must not grind to max_rounds:
        // after MAX_REPEAT_NUDGES repeats the loop forces a wrap-up summary.
        let model = AlwaysSameCall {
            call: ToolCall {
                id: "q".into(),
                name: "echo".into(),
                arguments: json!({"x": "same"}),
            },
            summary: "Stuck — summarising what I have.".into(),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 20)
            .await
            .unwrap();
        assert_eq!(run.answer, "Stuck — summarising what I have.");
        // Terminated well before the 20-round budget (1 first call + 3 repeats).
        assert_eq!(run.rounds, 4);
        assert_eq!(run.steps.len(), 4);
    }

    #[test]
    fn runaway_tail_detects_a_loop_only_at_the_end_of_a_stream() {
        // A clean prefix followed by a repeating tail (the Investigation 8 symptom).
        let cycle = "Actually, I'll just do the Retry.\nWait, I'll parallelize credit-check.\n";
        let mut s = "Reasoning about the bottleneck in detail.\n".repeat(40);
        s.push_str(&cycle.repeat(50));
        assert!(runaway_tail(&s));
        // A long but non-repeating tail must not trip it.
        let varied: String = (0..200)
            .map(|i| format!("Distinct analysis line number {i}.\n"))
            .collect();
        assert!(!runaway_tail(&varied));
    }

    #[test]
    fn runaway_tail_is_utf8_boundary_safe() {
        // Multibyte chars near the tail window must not panic the slice.
        let s = "✓ data looks consistent ✓\n".repeat(300);
        let _ = runaway_tail(&s); // must not panic
    }

    #[tokio::test]
    async fn distinct_tool_calls_are_not_treated_as_a_spin() {
        // Genuine progress (different args each round) must never trip the repeat breaker.
        let model = ScriptedModel {
            turns: vec![
                vec![ToolCall {
                    id: "a".into(),
                    name: "echo".into(),
                    arguments: json!({"x": "one"}),
                }],
                vec![ToolCall {
                    id: "b".into(),
                    name: "echo".into(),
                    arguments: json!({"x": "two"}),
                }],
            ],
            idx: Cell::new(0),
            final_answer: "found it".into(),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 20)
            .await
            .unwrap();
        assert_eq!(run.answer, "found it");
        assert_eq!(run.steps.len(), 2);
    }

    #[test]
    fn runaway_repetition_detector_fires_only_on_a_long_repetitive_ramble() {
        // The Investigation 7 failure: a long answer that repeats the same SQL line.
        let line = "`SELECT job_type, SUM(failures) FROM jobs GROUP BY job_type`\n";
        let runaway = format!("Let me think about the bottleneck.\n{}", line.repeat(80));
        assert!(is_runaway_repetition(&runaway));
        // A normal (even fairly long) answer with varied content must not trip.
        let normal = "The credit-check task is the bottleneck: P99 queue 91m, 189 incidents. \
                      Recommend a retry boundary plus +4 workers. I verified this against the \
                      trace data and the simulate scorecard."
            .repeat(20); // long but every line is distinct after repeat (single line)
        assert!(!is_runaway_repetition(&normal));
        // Short answers are never runaway, however repetitive.
        assert!(!is_runaway_repetition(&line.repeat(3)));
    }

    #[tokio::test]
    async fn runaway_final_turn_is_nudged_to_call_the_tool_instead_of_writing_sql() {
        // Round 1: the model rambles, repeating SQL as prose with no tool call. The loop
        // must nudge rather than accept the runaway; round 2 it recovers with a real answer.
        let line = "Actually, I'll just do: `SELECT * FROM jobs`\n";
        let model = SequencedModel {
            script: vec![
                Scripted::Defer(format!("Thinking...\n{}", line.repeat(100))),
                Scripted::Done("Bottleneck is credit-check; recommend retry + workers.".into()),
            ],
            idx: Cell::new(0),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 8).await.unwrap();
        assert_eq!(
            run.answer,
            "Bottleneck is credit-check; recommend retry + workers."
        );
    }
}
