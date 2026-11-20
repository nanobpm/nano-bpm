//! #1663 (ADR 0066 audit #7): FEEL float transcendentals are platform-independent.
//!
//! `log`, `exp` and `**` route through the pinned pure-Rust `libm` crate rather
//! than the platform libm (glibc vs Apple libm can differ by one ulp, which
//! would let Raft replicas on different OS/arch take different branches). These
//! golden vectors pin the exact IEEE-754 bit patterns: they must hold on every
//! CI runner (Linux and macOS), and a `libm` bump that changes a result fails
//! here instead of silently forking replicas.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples)"
)]

use std::collections::BTreeMap;

use nanobpmn_engine_core::{feel, Value};

/// (expression, expected f64 bit pattern).
const GOLDEN: &[(&str, u64)] = &[
    ("log(2)", 0x3fe6_2e42_fefa_39ef),
    ("log(10)", 0x4002_6bb1_bbb5_5516),
    ("log(0.1)", 0xc002_6bb1_bbb5_5515),
    ("exp(1)", 0x4005_bf0a_8b14_576a),
    ("exp(0.5)", 0x3ffa_6129_8e1e_069c),
    ("exp(-3.3)", 0x3fa2_e259_bb85_be85),
    ("2 ** 0.5", 0x3ff6_a09e_667f_3bcd),
    ("1.1 ** 3.7", 0x3ff6_c3e9_1ed0_825a),
    ("10 ** -3", 0x3f50_624d_d2f1_a9fc),
    ("7.3 ** 2.9", 0x4073_ee2f_9631_2c0e),
    ("round half up(2.675, 2)", 0x4005_70a3_d70a_3d71),
    ("floor(1.23456, 3)", 0x3ff3_be76_c8b4_3958),
];

fn bits(expr: &str) -> u64 {
    match feel::eval(expr, &BTreeMap::new()).unwrap() {
        Value::Double(d) => d.to_bits(),
        Value::Int(i) => (i as f64).to_bits(),
        other => panic!("{expr}: non-numeric {other:?}"),
    }
}

#[test]
fn transcendentals_match_golden_bits() {
    for (expr, want) in GOLDEN {
        let got = bits(expr);
        assert_eq!(
            got,
            *want,
            "{expr}: got {} ({got:#018x}), want {} ({want:#018x})",
            f64::from_bits(got),
            f64::from_bits(*want)
        );
    }
}

/// The FEEL results equal the pure-Rust reference, so a regression back to the
/// platform intrinsic is caught even on a runner where both happen to agree.
#[test]
fn transcendentals_match_pure_rust_reference() {
    assert_eq!(bits("log(10)"), libm::log(10.0).to_bits());
    assert_eq!(bits("exp(0.5)"), libm::exp(0.5).to_bits());
    assert_eq!(bits("1.1 ** 3.7"), libm::pow(1.1, 3.7).to_bits());
}
