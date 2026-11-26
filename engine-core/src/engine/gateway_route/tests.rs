//! Equivalence of the gateway-route kernel with the pre-#1983 engine loops,
//! exhaustively over every guard sequence up to 5 flows.

use super::{select_exclusive, select_inclusive, Guard, Route};

const ALL: [Guard; 5] = [
    Guard::Default,
    Guard::Unconditional,
    Guard::Holds,
    Guard::Fails,
    Guard::Errors,
];

/// The old `select_exclusive_flow` loop, verbatim in shape: `Ok(pos)`, or
/// `Err(Some(pos))` for a condition error, `Err(None)` for no match.
fn old_exclusive(guards: &[Guard]) -> Result<usize, Option<usize>> {
    let mut default_flow = None;
    let mut unconditional = None;
    for (index, g) in guards.iter().enumerate() {
        if *g == Guard::Default {
            default_flow = Some(index);
            continue;
        }
        match g {
            Guard::Unconditional => unconditional = Some(index),
            Guard::Holds => return Ok(index),
            Guard::Fails => continue,
            Guard::Errors => return Err(Some(index)),
            Guard::Default => unreachable!(),
        }
    }
    unconditional.or(default_flow).ok_or(None)
}

/// The old `select_inclusive_flows` loop.
fn old_inclusive(guards: &[Guard]) -> Result<Vec<usize>, Option<usize>> {
    let mut default_flow = None;
    let mut selected = Vec::new();
    for (index, g) in guards.iter().enumerate() {
        match g {
            Guard::Default => default_flow = Some(index),
            Guard::Unconditional | Guard::Holds => selected.push(index),
            Guard::Fails => {}
            Guard::Errors => return Err(Some(index)),
        }
    }
    if selected.is_empty() {
        match default_flow {
            Some(d) => selected.push(d),
            None => return Err(None),
        }
    }
    Ok(selected)
}

fn for_each_sequence(max: usize, mut f: impl FnMut(&[Guard])) {
    for len in 0..=max {
        let total = ALL.len().pow(len as u32);
        for mut code in 0..total {
            let mut seq = Vec::with_capacity(len);
            for _ in 0..len {
                seq.push(ALL[code % ALL.len()]);
                code /= ALL.len();
            }
            f(&seq);
        }
    }
}

#[test]
fn exclusive_kernel_matches_old_loop() {
    for_each_sequence(5, |guards| {
        let expected = match old_exclusive(guards) {
            Ok(i) => Route::Take(i),
            Err(Some(i)) => Route::ConditionError(i),
            Err(None) => Route::NoMatch,
        };
        assert_eq!(select_exclusive(guards), expected, "{guards:?}");
    });
}

#[test]
fn inclusive_kernel_matches_old_loop() {
    for_each_sequence(5, |guards| {
        let mut mask = vec![false; guards.len()];
        let got = select_inclusive(guards, &mut mask);
        match old_inclusive(guards) {
            Ok(flows) => {
                assert_eq!(got, Route::Take(flows.len()), "{guards:?}");
                let marked: Vec<usize> = (0..mask.len()).filter(|&i| mask[i]).collect();
                assert_eq!(marked, flows, "{guards:?}");
            }
            Err(Some(i)) => assert_eq!(got, Route::ConditionError(i), "{guards:?}"),
            Err(None) => assert_eq!(got, Route::NoMatch, "{guards:?}"),
        }
    });
}

#[test]
fn inclusive_short_mask_is_total() {
    let mut mask = [false; 1];
    let got = select_inclusive(&[Guard::Fails, Guard::Holds, Guard::Default], &mut mask);
    assert_eq!(got, Route::Take(1));
}
