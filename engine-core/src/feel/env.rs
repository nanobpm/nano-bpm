//! The replicated, deterministic evaluation environment for FEEL's
//! nondeterministic builtins (`now()`, `today()`, `random number()`, `uuid()`).
//!
//! Every engine-side FEEL evaluation (gateway/sequence-flow conditions,
//! conditional event re-evaluation, conditional starts, input/output mappings,
//! …) runs while the engine processes a command. Raft replicas apply the same
//! command independently, so those builtins must not read the process-local
//! wall clock or an unseeded RNG — replicas would take different branches and
//! diverge (#1642). Instead the engine installs an [`EvalEnv`] — the command's
//! replicated timestamp plus an RNG seeded from replicated engine state — for
//! the duration of command processing via [`with_env`]. Because it is installed
//! at the engine's processing seam rather than per call site, every present and
//! future `feel::eval*` call made while processing a command is covered.
//!
//! Outside an installed environment the builtins **fail closed** (#1663, ADR
//! 0066 audit #2): they return a typed [`FeelError`] — surfaced by the engine
//! as an incident — rather than silently reading the host clock or entropy.
//! Callers that are genuinely off the replicated apply path (ad-hoc evaluation
//! APIs, host-side trigger expressions, tests) must opt in explicitly with
//! [`with_host_env`], the single documented host-clock/entropy seam.

use std::cell::Cell;

use super::error::FeelError;

/// A deterministic evaluation environment: a fixed clock and a seeded RNG.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvalEnv {
    /// The clock reading `now()`/`today()` observe, in Unix epoch milliseconds.
    pub now_ms: u64,
    /// SplitMix64 state; advanced by every random draw.
    pub rng_state: u64,
}

impl EvalEnv {
    /// An environment for a command stamped `now_ms`, with its RNG seeded from
    /// `seed` (replicated engine state, so every replica derives the same one).
    pub fn new(now_ms: u64, seed: u64) -> Self {
        let mut state = now_ms ^ seed.wrapping_mul(0xD6E8_FEB8_6659_FD93);
        // Mix once so near-identical inputs do not yield correlated streams.
        let rng_state = splitmix64(&mut state);
        EvalEnv { now_ms, rng_state }
    }
}

thread_local! {
    static ENV: Cell<Option<EvalEnv>> = const { Cell::new(None) };
}

/// Restores the previously installed environment on drop (also on unwind) and
/// writes the advanced RNG state back to the caller's `EvalEnv`.
struct Restore<'a> {
    prev: Option<EvalEnv>,
    out: &'a mut EvalEnv,
}

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        if let Some(cur) = ENV.with(|e| e.replace(self.prev)) {
            *self.out = cur;
        }
    }
}

/// Runs `f` with `env` installed as the FEEL evaluation environment of this
/// thread. On return `env` holds the advanced RNG state, so a caller resuming
/// work later (e.g. a stepping debugger) continues the same random stream.
pub fn with_env<R>(env: &mut EvalEnv, f: impl FnOnce() -> R) -> R {
    let prev = ENV.with(|e| e.replace(Some(*env)));
    let _restore = Restore { prev, out: env };
    f()
}

/// Runs `f` with an environment drawn from the **host** wall clock and fresh
/// per-call entropy installed — the one explicit opt-in for evaluation that is
/// off the replicated apply path (an ad-hoc `POST /expression/evaluation`, a
/// host-side trigger, a test). Never call this from engine command processing:
/// that path installs the replicated environment via [`with_env`], and a
/// host-clock read there would let replicas diverge (#1642, #1663).
pub fn with_host_env<R>(f: impl FnOnce() -> R) -> R {
    let mut env = host_env();
    with_env(&mut env, f)
}

/// The host clock + entropy environment behind [`with_host_env`]. This is the
/// only place in the apply crate allowed to read the wall clock (see the
/// `disallowed-methods` purity lint in `engine-core/clippy.toml`).
#[allow(
    clippy::disallowed_methods,
    reason = "documented off-apply host seam (#1663): only reached via with_host_env"
)]
fn host_env() -> EvalEnv {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = (since.as_nanos() as u64).wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    EvalEnv::new(since.as_millis() as u64, seed)
}

/// The typed fail-closed error for a nondeterministic builtin evaluated with
/// no environment installed (#1663).
pub(crate) fn missing_env_error(builtin: &str) -> FeelError {
    FeelError(format!(
        "{builtin}() requires a replicated evaluation environment; refusing to read the \
         host clock/entropy (evaluate inside engine command processing, or opt in \
         explicitly with feel::with_host_env)"
    ))
}

/// The installed clock reading, if any.
pub(crate) fn now_ms() -> Option<u64> {
    ENV.with(|e| e.get().map(|env| env.now_ms))
}

/// The next draw from the installed RNG, if any.
pub(crate) fn next_u64() -> Option<u64> {
    ENV.with(|e| {
        let mut env = e.get()?;
        let v = splitmix64(&mut env.rng_state);
        e.set(Some(env));
        Some(v)
    })
}

/// One SplitMix64 step.
pub(crate) fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Value;

    #[test]
    fn installed_env_pins_now_and_seeds_random() {
        let ctx = std::collections::HashMap::new();
        let run = || {
            let mut env = EvalEnv::new(1_700_000_000_000, 42);
            with_env(&mut env, || {
                (
                    crate::feel::eval("string(today())", &ctx).unwrap(),
                    crate::feel::eval("random number()", &ctx).unwrap(),
                    crate::feel::eval("uuid()", &ctx).unwrap(),
                )
            })
        };
        let (a, b) = (run(), run());
        assert_eq!(a, b);
        assert_eq!(a.0, Value::Str("2023-11-14".into()));
        // Uninstalled again afterwards.
        assert!(now_ms().is_none());
    }

    /// #1663 red test: with no environment installed the nondeterministic
    /// builtins fail closed instead of reading the host clock / entropy.
    #[test]
    fn missing_env_fails_closed() {
        let ctx = std::collections::HashMap::new();
        for expr in ["now()", "today()", "random number()", "uuid()"] {
            let err = crate::feel::eval(expr, &ctx).expect_err(expr);
            assert!(
                err.0
                    .contains("requires a replicated evaluation environment"),
                "{expr}: {err}"
            );
        }
    }

    #[test]
    fn host_env_is_an_explicit_opt_in() {
        let ctx = std::collections::HashMap::new();
        let v = with_host_env(|| crate::feel::eval("now()", &ctx)).unwrap();
        assert!(matches!(v, Value::Str(_)), "{v:?}");
        let a = with_host_env(|| crate::feel::eval("uuid()", &ctx)).unwrap();
        let b = with_host_env(|| crate::feel::eval("uuid()", &ctx)).unwrap();
        assert_ne!(a, b, "fresh entropy per host-env call");
        assert!(now_ms().is_none());
    }

    #[test]
    fn with_env_writes_back_the_advanced_stream() {
        let mut env = EvalEnv::new(0, 1);
        let start = env;
        let first = with_env(&mut env, || next_u64().unwrap());
        assert_ne!(env, start);
        let second = with_env(&mut env, || next_u64().unwrap());
        assert_ne!(first, second);
    }
}
