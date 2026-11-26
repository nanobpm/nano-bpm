//! Kani harnesses for the split-gateway route kernel (#1983).
//!
//! Compiled only under `cargo kani` (`cfg(kani)`). See `formal/kani/README.md`
//! (`formal/kani/check.sh gateway_route_exclusive_total_and_sound`,
//! `gateway_route_inclusive_total_and_sound`).

use super::{select_exclusive, select_inclusive, Guard, Route};

const FLOWS: usize = 3;

fn any_guard() -> Guard {
    match kani::any::<u8>() % 5 {
        0 => Guard::Default,
        1 => Guard::Unconditional,
        2 => Guard::Holds,
        3 => Guard::Fails,
        _ => Guard::Errors,
    }
}

fn any_guards() -> [Guard; FLOWS] {
    [any_guard(), any_guard(), any_guard()]
}

#[kani::proof]
#[kani::unwind(4)]
fn gateway_route_exclusive_total_and_sound() {
    let guards = any_guards();
    let len: usize = kani::any();
    kani::assume(len <= FLOWS);
    let g = &guards[..len];
    match select_exclusive(g) {
        Route::Take(i) => {
            assert!(i < len);
            // Never takes a failing or erroring flow.
            assert!(g[i] != Guard::Fails && g[i] != Guard::Errors);
            // A holding condition wins over every fallback; no earlier
            // condition errored or held.
            let mut j = 0;
            while j < len {
                if g[j] == Guard::Holds || (j < i && g[j] == Guard::Errors) {
                    assert!(g[i] == Guard::Holds && j >= i);
                }
                // A fallback (non-holding) take scanned the whole slice, so
                // no condition anywhere errored. (A holding take stops the
                // scan, so a later `Errors` is legitimately never looked at.)
                if g[i] != Guard::Holds {
                    assert!(g[j] != Guard::Errors);
                }
                j += 1;
            }
            // The default is taken only when nothing else could be.
            if g[i] == Guard::Default {
                let mut k = 0;
                while k < len {
                    assert!(g[k] == Guard::Default || g[k] == Guard::Fails);
                    k += 1;
                }
            }
            kani::cover!(g[i] == Guard::Default);
        }
        Route::ConditionError(i) => {
            assert!(i < len && g[i] == Guard::Errors);
        }
        Route::NoMatch => {
            let mut k = 0;
            while k < len {
                assert!(g[k] == Guard::Fails);
                k += 1;
            }
            kani::cover!(len > 0);
        }
    }
}

#[kani::proof]
#[kani::unwind(4)]
fn gateway_route_inclusive_total_and_sound() {
    let guards = any_guards();
    let len: usize = kani::any();
    let mask_len: usize = kani::any();
    kani::assume(len <= FLOWS && mask_len <= FLOWS);
    let g = &guards[..len];
    let mut mask = [false; FLOWS];
    let got = select_inclusive(g, &mut mask[..mask_len]);
    if mask_len < len {
        // A short mask is tolerated (no panic); nothing more to check.
        return;
    }
    match got {
        Route::Take(count) => {
            assert!(count >= 1 && count <= len);
            let mut marked = 0;
            let mut any_taken = false;
            let mut k = 0;
            while k < len {
                if g[k] == Guard::Holds || g[k] == Guard::Unconditional {
                    any_taken = true;
                    assert!(mask[k]);
                }
                if mask[k] {
                    marked += 1;
                    assert!(g[k] != Guard::Fails && g[k] != Guard::Errors);
                }
                k += 1;
            }
            assert!(marked == count);
            // The default is marked only when no other flow is taken.
            let mut d = 0;
            while d < len {
                if mask[d] && g[d] == Guard::Default {
                    assert!(!any_taken);
                }
                d += 1;
            }
            kani::cover!(count == 2);
        }
        Route::ConditionError(i) => assert!(i < len && g[i] == Guard::Errors),
        Route::NoMatch => {
            let mut k = 0;
            while k < len {
                assert!(g[k] == Guard::Fails);
                k += 1;
            }
        }
    }
}
