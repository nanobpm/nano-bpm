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
//! Outside an installed environment (ad-hoc evaluation APIs, unit tests) the
//! builtins fall back to the wall clock, as before.

use std::cell::Cell;

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
        // Uninstalled again afterwards: falls back to the wall clock.
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
