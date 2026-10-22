//! Build script for `nanobpmn-read-model`.
//!
//! It has two jobs, both scoped tightly to the `wasm` backend on a `wasm32`
//! target — the default `native` (server) build hits neither and is left
//! completely untouched:
//!
//! 1. **Make the `wasm` backend link.** On `wasm32-unknown-unknown` the read
//!    model is built with `rusqlite`'s `libsqlite3-sys` in *non-bundled* mode
//!    (no C SQLite of its own): the actual `sqlite3_*` C symbols are supplied by
//!    the `sqlite-wasm-rs` crate instead (its compiled `libwsqlite3.a`, plus an
//!    in-memory `MemoryVFS`). `libsqlite3-sys`'s build script does not know that
//!    and still emits a bare `cargo:rustc-link-lib=dylib=sqlite3`, so the final
//!    `wasm-ld` link of any dependent fails with `error: unable to find library
//!    -lsqlite3`. We drop an **empty** `libsqlite3.a` archive on the link search
//!    path: it satisfies the `-lsqlite3` file lookup while contributing zero
//!    symbols, so it cannot shadow the real `sqlite3_*` implementations.
//!
//! 2. **Trim the wasm SQLite footprint (epic #796 size follow-up).** By default
//!    `sqlite-wasm-rs` bakes a FULL-featured SQLite (FTS5, RTREE, SESSION,
//!    geopoly, math functions, extra vtabs …). None of those are used by the read
//!    model — its schema and queries are plain core SQL — yet they cannot be
//!    stripped by `wasm-opt` DCE (they self-register through SQLite's init
//!    tables) and cannot be removed with extra `CFLAGS` (you can't *un*-define the
//!    crate's baked `-DSQLITE_ENABLE_*` flags). So instead of shipping the
//!    crate's default archive we compile our OWN minimal `libwsqlite3.a` here —
//!    the same amalgamation and the same musl/printf shim `sqlite-wasm-rs` uses,
//!    but with the extensions *not enabled at compile time* — and link that.
//!    `sqlite-wasm-rs`'s own C build is suppressed via a Cargo build-script
//!    override on its `links = "wsqlite3"` key (see `.cargo/config.toml`), so only
//!    this trimmed archive is ever produced or linked. The Rust bindings and
//!    `MemoryVFS` in `sqlite-wasm-rs` speak only core SQLite, so nothing they
//!    reference is dropped.
//!
//! A `rustc-link-search` emitted here propagates to the final link of every crate
//! that depends on `read-model` (its own artifacts and the downstream
//! `engine-wasm` cdylib), which is exactly where both `-lsqlite3` and `-lwsqlite3`
//! are resolved.

// Epic #1357: a build script reports unrecoverable configuration failures by
// panicking — that is the idiomatic, Cargo-sanctioned error path (it aborts the
// build with the message). None of these sites are runtime-fallible production
// code, so the crate-wide `unwrap_used`/`expect_used` deny is relaxed here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "build-script failures abort the build; panic is the idiomatic error path"
)]

use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SQLITE_WASM_RS_SRC_DIR");
    // Dependency resolution feeds `locate_sqlite_wasm_rs_src` (it makes a
    // best-effort read of the locked `sqlite-wasm-rs` version from *this crate's
    // own* `Cargo.lock`), so a change to either manifest must re-run this script —
    // otherwise Cargo reuses a stale build-script output and keeps linking a
    // `libwsqlite3.a` compiled against the previous checkout. Note this local read
    // is authoritative only when `read-model` is built standalone: when it is a
    // path dependency (e.g. from `engine-wasm`) Cargo resolves `sqlite-wasm-rs`
    // from the *downstream* crate's lockfile and no `read-model/Cargo.lock` need
    // exist, so the read may miss — which is exactly why the version is only a
    // disambiguation hint and the locator fails closed on ambiguity rather than
    // trusting it (see `locked_sqlite_wasm_rs_version`).
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=Cargo.toml");

    let wasm_feature = std::env::var_os("CARGO_FEATURE_WASM").is_some();
    let native_feature = std::env::var_os("CARGO_FEATURE_NATIVE").is_some();
    // Match the EXACT target triple, not just `target_arch == "wasm32"`. The
    // `wsqlite3` build-script override in `.cargo/config.toml` is scoped to
    // `[target.wasm32-unknown-unknown.wsqlite3]`, so on any other wasm32 triple
    // (e.g. `wasm32-wasip1`) that override would NOT apply: `sqlite-wasm-rs`'s own
    // build script would compile its full `wsqlite3` while this script also
    // compiled a minimal one, colliding with duplicate link directives/symbols.
    // Gating on the full triple keeps this script and the override in lockstep.
    let target = std::env::var("TARGET").unwrap_or_default();

    // Only the wasm-backend-on-`wasm32-unknown-unknown` configuration needs any of
    // this. When `native` is also enabled the backend seam selects `native` (real
    // bundled SQLite), so neither the `-lsqlite3` stub nor the `wsqlite3` override
    // apply.
    if !(wasm_feature && !native_feature && target == "wasm32-unknown-unknown") {
        return;
    }

    let out_dir = PathBuf::from(
        std::env::var("OUT_DIR").expect("cargo always sets OUT_DIR for a build script"),
    );

    // (1) Empty `libsqlite3.a` stub so `libsqlite3-sys`'s bare `-lsqlite3`
    //     resolves without shadowing the real `sqlite3_*` symbols.
    write_empty_archive(&out_dir.join("libsqlite3.a"));

    // (2) Minimal, core-SQL-only `libwsqlite3.a` in place of `sqlite-wasm-rs`'s
    //     FULL-featured default (whose own C build is suppressed via the empty
    //     build-script override in `.cargo/config.toml`). `cc` emits the
    //     `rustc-link-lib=static=wsqlite3` + `rustc-link-search` for it.
    compile_minimal_wsqlite3(&out_dir);

    // Search path for the empty `libsqlite3.a` stub above; it propagates to the
    // final link. (`cc` emits its own search path for `libwsqlite3.a`.)
    println!("cargo:rustc-link-search=native={}", out_dir.display());
}

/// Writes a valid, empty `ar` archive (the classic Unix `!<arch>\n` magic with no
/// members). `wasm-ld` accepts it as satisfying `-lsqlite3` yet it defines no
/// symbols, so the real `sqlite3_*` symbols still resolve against the trimmed
/// `libwsqlite3.a`.
fn write_empty_archive(path: &Path) {
    std::fs::write(path, b"!<arch>\n").expect("write empty libsqlite3.a stub");
}

/// SQLite compile flags for the trimmed wasm build.
///
/// This is `sqlite-wasm-rs`'s own base configuration (kept identical so the
/// runtime environment — OS shim, threading, page/cache sizing, URI handling — is
/// byte-for-byte the same) MINUS every `-DSQLITE_ENABLE_*` extension the read
/// model does not use. Dropping the enables at *compile time* is what actually
/// removes the code: the extensions register themselves through SQLite's static
/// init tables, so they survive `wasm-opt` DCE if compiled in.
///
/// Deliberately **kept** (cheap and potentially referenced by the `rusqlite` /
/// `libsqlite3-sys` bindings): `API_ARMOR` (defensive misuse checks) and
/// `COLUMN_METADATA` (`sqlite3_column_*_name` / `sqlite3_table_column_metadata`).
///
/// Deliberately **dropped** (large, self-registering, unused by the core-SQL
/// projection): FTS5, RTREE (+geopoly), SESSION/PREUPDATE_HOOK, MATH_FUNCTIONS,
/// the BYTECODE/DBPAGE/DBSTAT/STMT vtabs, OFFSET_SQL_FUNC, UNKNOWN_SQL_FUNCTION
/// and UNLOCK_NOTIFY. No `-DSQLITE_OMIT_*` beyond `sqlite-wasm-rs`'s own are added,
/// so no core SQL behaviour (JSON, string/date funcs, DQS handling, foreign keys)
/// changes — the projection answers exactly as on the full build.
const MINIMAL_FEATURED: [&str; 11] = [
    "-DSQLITE_OS_OTHER",
    "-DSQLITE_USE_URI",
    "-DSQLITE_THREADSAFE=0",
    "-DSQLITE_TEMP_STORE=2",
    "-DSQLITE_DEFAULT_CACHE_SIZE=-16384",
    "-DSQLITE_DEFAULT_PAGE_SIZE=8192",
    "-DSQLITE_OMIT_DEPRECATED",
    "-DSQLITE_OMIT_LOAD_EXTENSION",
    "-DSQLITE_OMIT_SHARED_CACHE",
    "-DSQLITE_ENABLE_API_ARMOR",
    "-DSQLITE_ENABLE_COLUMN_METADATA",
];

/// The musl/printf shim `sqlite-wasm-rs` compiles alongside the amalgamation to
/// provide the libc surface `wasm32-unknown-unknown` lacks. Mirrors the
/// `C_SOURCE` list in `sqlite-wasm-rs` 0.5.x's `build.rs` (paths relative to
/// `<src>/shim/musl/`). Compiling the identical shim is what keeps our trimmed
/// archive drop-in-compatible with the crate's Rust bindings and `MemoryVFS`.
const MUSL_SHIM_SOURCES: [&str; 36] = [
    "string/memchr.c",
    "string/memrchr.c",
    "string/stpcpy.c",
    "string/stpncpy.c",
    "string/strcat.c",
    "string/strchr.c",
    "string/strchrnul.c",
    "string/strcmp.c",
    "string/strcpy.c",
    "string/strcspn.c",
    "string/strlen.c",
    "string/strncat.c",
    "string/strncmp.c",
    "string/strncpy.c",
    "string/strrchr.c",
    "string/strspn.c",
    "stdlib/atoi.c",
    "stdlib/bsearch.c",
    "stdlib/qsort.c",
    "stdlib/qsort_nr.c",
    "stdlib/strtod.c",
    "stdlib/strtol.c",
    "math/__fpclassifyl.c",
    "math/acosh.c",
    "math/asinh.c",
    "math/atanh.c",
    "math/fmodl.c",
    "math/scalbn.c",
    "math/scalbnl.c",
    "math/sqrt.c",
    "math/trunc.c",
    "errno/__errno_location.c",
    "stdio/__toread.c",
    "stdio/__uflow.c",
    "internal/floatscan.c",
    "internal/shgetc.c",
];

/// Compiles the trimmed `libwsqlite3.a` into `out_dir` from `sqlite-wasm-rs`'s
/// vendored amalgamation + shim, replacing the crate's FULL-featured default.
///
/// `cc` emits `rustc-link-lib=static=wsqlite3` and a `rustc-link-search` for
/// `out_dir` itself, so the trimmed archive is picked up at `read-model`'s own
/// compile and at the downstream final link. `sqlite-wasm-rs`'s own (suppressed)
/// build script would normally have emitted the same `wsqlite3` link directives;
/// here `read-model` supplies them for the minimal archive instead.
fn compile_minimal_wsqlite3(out_dir: &Path) {
    let src = locate_sqlite_wasm_rs_src();
    let shim = src.join("shim");
    let wasm_shim_h = shim.join("wasm-shim.h");
    let printf_c = shim.join("printf/printf.c");
    let sqlite3_c = src.join("sqlite3/sqlite3.c");

    for required in [&wasm_shim_h, &printf_c, &sqlite3_c] {
        assert!(
            required.exists(),
            "expected sqlite-wasm-rs source at {} — set SQLITE_WASM_RS_SRC_DIR to the crate root \
             if it is not in the cargo registry",
            required.display()
        );
    }

    // Every compiled input (not just the amalgamation) must trigger a rebuild —
    // otherwise an edit to the header/printf/musl shim under a
    // `SQLITE_WASM_RS_SRC_DIR` override leaves a stale `libwsqlite3.a` linked.
    let musl_sources: Vec<PathBuf> = MUSL_SHIM_SOURCES
        .iter()
        .map(|s| shim.join("musl").join(s))
        .collect();
    for musl_source in &musl_sources {
        assert!(
            musl_source.exists(),
            "expected sqlite-wasm-rs musl shim source at {} — the vendored shim layout may have \
             changed; set SQLITE_WASM_RS_SRC_DIR to the crate root or update MUSL_SHIM_SOURCES",
            musl_source.display()
        );
    }
    for input in [&wasm_shim_h, &printf_c, &sqlite3_c]
        .into_iter()
        .chain(musl_sources.iter())
    {
        println!("cargo:rerun-if-changed={}", input.display());
    }

    let mut cc = cc::Build::new();
    cc.warnings(false)
        // `-Wno-macro-redefined` is a Clang-specific flag; a non-Clang
        // `CC_wasm32_unknown_unknown` may treat an unknown `-Wno-*` as a hard
        // error, so probe support rather than forcing it (`warnings(false)`
        // already suppresses the diagnostics where it is unavailable).
        .flag_if_supported("-Wno-macro-redefined")
        .include(&shim)
        .include(shim.join("musl/arch/generic"))
        .include(shim.join("musl/include"))
        .file(&printf_c)
        .file(&sqlite3_c)
        .files(&musl_sources)
        .flag("-DPRINTF_ALIAS_STANDARD_FUNCTION_NAMES_HARD")
        .flag("-include")
        // Pass the shim header as an `OsStr` (cc's `flag` accepts `AsRef<OsStr>`)
        // rather than `.to_str().expect(...)`, so a non-UTF-8 registry path can't
        // panic the build script.
        .flag(&wasm_shim_h);

    for flag in MINIMAL_FEATURED {
        cc.flag(flag);
    }

    // Produces `<out_dir>/libwsqlite3.a`.
    cc.out_dir(out_dir).compile("wsqlite3");
}

/// Finds the vendored `sqlite-wasm-rs` crate source directory (which ships the
/// SQLite amalgamation + shim we recompile). Honours an explicit
/// `SQLITE_WASM_RS_SRC_DIR` override first, otherwise searches the cargo registry
/// checkout. The crate is a normal registry dependency (target-gated to wasm32),
/// so its sources are present here whenever this wasm-backend build runs.
fn locate_sqlite_wasm_rs_src() -> PathBuf {
    if let Some(dir) = std::env::var_os("SQLITE_WASM_RS_SRC_DIR") {
        return PathBuf::from(dir);
    }

    let want_version = locked_sqlite_wasm_rs_version();

    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cargo")))
        .or_else(|| std::env::var_os("USERPROFILE").map(|h| PathBuf::from(h).join(".cargo")))
        .expect("cannot locate CARGO_HOME/HOME to find the sqlite-wasm-rs registry checkout");

    let registry_src = cargo_home.join("registry").join("src");
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(indexes) = std::fs::read_dir(&registry_src) {
        for index in indexes.flatten() {
            let Ok(entries) = std::fs::read_dir(index.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(ver) = name.strip_prefix("sqlite-wasm-rs-")
                    && entry.path().join("sqlite3/sqlite3.c").exists()
                {
                    // Prefer the exact locked version when we could read it.
                    if want_version.as_deref() == Some(ver) {
                        return entry.path();
                    }
                    candidates.push(entry.path());
                }
            }
        }
    }

    // No exact match. Do NOT guess by lexicographic "highest": `candidates.sort()`
    // orders paths as strings, so `sqlite-wasm-rs-0.5.9` sorts *after* `0.5.10` and
    // we would silently compile against a mismatched amalgamation/shim. Fail closed
    // instead — fall back only when there is exactly one checkout (no ambiguity).
    match candidates.len() {
        1 => candidates.pop().expect("len checked to be 1"),
        0 => panic!(
            "could not find a sqlite-wasm-rs source checkout under {} — run the build with \
             the wasm32 target (which fetches it) or set SQLITE_WASM_RS_SRC_DIR",
            registry_src.display()
        ),
        n => panic!(
            "found {n} sqlite-wasm-rs checkouts under {} but none match the locked version {:?}; \
             refusing to guess which to compile — set SQLITE_WASM_RS_SRC_DIR to the intended \
             crate root",
            registry_src.display(),
            want_version,
        ),
    }
}

/// Best-effort read of the `sqlite-wasm-rs` version pinned in this crate's
/// `Cargo.lock`, used only to disambiguate multiple registry checkouts. A miss is
/// non-fatal only when a single checkout is present: with more than one, the
/// caller fails closed rather than guess (see `locate_sqlite_wasm_rs_src`).
fn locked_sqlite_wasm_rs_version() -> Option<String> {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")?;
    let lock = PathBuf::from(manifest_dir).join("Cargo.lock");
    let text = std::fs::read_to_string(lock).ok()?;
    let mut in_pkg = false;
    for line in text.lines() {
        let line = line.trim();
        if line == "[[package]]" {
            in_pkg = false;
            continue;
        }
        if line == "name = \"sqlite-wasm-rs\"" {
            in_pkg = true;
            continue;
        }
        if in_pkg && let Some(rest) = line.strip_prefix("version = \"") {
            return rest.strip_suffix('"').map(str::to_string);
        }
    }
    None
}
