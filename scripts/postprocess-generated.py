#!/usr/bin/env python3
"""Patch known bugs in the rust-axum generated code so the crate compiles.

rust-axum is a beta generator. A few constructs in the Orchestration Cluster
spec make it emit invalid Rust. Rather than editing the shared spec (the API
contract) this script rewrites the *generated* output deterministically, so the
fixes are reapplied automatically on every regeneration.

Each fix is narrowly targeted and documented below. Run after generation:

    postprocess-generated.py <generated-crate-dir>
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def fix_oneof_datetime_variant(models: str) -> tuple[str, int]:
    """Fix the invalid enum variant `DateTime::OfUtc`.

    For a `oneOf` whose first member is an inline `string`/`date-time`, rust-axum
    derives the variant identifier from the Rust type `chrono::DateTime<Utc>` and
    emits `DateTime::OfUtc`, which is not a legal variant name (it contains `::`).
    The enum is `#[serde(untagged)]`, so the variant name is not part of the wire
    format and can be safely renamed to a valid identifier.
    """
    new = models.replace("DateTime::OfUtc", "DateTimeUtc")
    return new, models.count("DateTime::OfUtc")


def fix_optional_discriminators(models: str) -> tuple[str, list[str]]:
    """Fix discriminator helpers for structs whose discriminator field is
    `Option<String>`.

    For a model with a discriminator property (e.g. `type`), rust-axum emits
    helper code that assumes the field is a required `String`:

      * `#[serde(default = "Struct::_name_for_<prop>")]` (returns `String`)
      * `#[serde(serialize_with = "Struct::_serialize_<prop>")]` taking `&String`
      * `new()` initialising the field with `Self::_name_for_<prop>()`

    When the property is optional the field is `Option<String>`, so each of these
    fails to compile. We detect only the optional discriminators (required ones
    compile fine and are left untouched) and adapt the helpers to `Option<String>`.
    """
    # A discriminator field is one carrying a `default = "X::_name_for_<prop>"`
    # serde attribute. Capture the property name and whether the backing field is
    # optional by looking at the `pub <prop>:` declaration that follows.
    discriminator_field = re.compile(
        r'#\[serde\(default = "\w+::_name_for_(?P<prop>\w+)"\)\][\s\S]*?'
        r'pub (?P=prop): (?P<ty>Option<String>|String),'
    )

    optional_props: set[str] = set()
    for match in discriminator_field.finditer(models):
        if match.group("ty") == "Option<String>":
            optional_props.add(match.group("prop"))

    applied: list[str] = []
    for prop in sorted(optional_props):
        # 1. serialize_with helper must accept the optional reference.
        models = models.replace(
            f"fn _serialize_{prop}<S>(_: &String,",
            f"fn _serialize_{prop}<S>(_: &Option<String>,",
        )

        # 2. Point the serde default at a wrapper returning Option<String>.
        models = re.sub(
            rf'(#\[serde\(default = ")(\w+)(::)_name_for_{prop}("\)\])',
            rf"\1\2\3_default_for_{prop}\4",
            models,
        )

        # 3. Inject the `_default_for_<prop>` wrapper next to `_name_for_<prop>`.
        models = re.sub(
            rf"(    fn _name_for_{prop}\(\) -> String \{{[^}}]*\}}\n)",
            rf"\1"
            rf"    fn _default_for_{prop}() -> Option<String> {{\n"
            rf"        Some(Self::_name_for_{prop}())\n"
            rf"    }}\n",
            models,
        )

        # 4. Wrap the constructor initialisation in Some(..).
        models = models.replace(
            f"{prop}: Self::_name_for_{prop}(),",
            f"{prop}: Some(Self::_name_for_{prop}()),",
        )

        applied.append(prop)

    return models, applied


def fix_pagination_disambiguation(models: str) -> tuple[str, list[str]]:
    """Add `#[serde(deny_unknown_fields)]` to the four pagination structs.

    `SearchQueryPageRequest` is an untagged `oneOf` of `LimitPagination`,
    `OffsetPagination`, `CursorForwardPagination`, and `CursorBackwardPagination`,
    all of which have only optional fields. Without `deny_unknown_fields`, serde's
    untagged deserialization matches the first variant (`LimitPagination`) for any
    object that merely contains `limit`, silently dropping `after`/`before`/
    `from`. That makes cursor and offset pagination unreachable through the typed
    model. Denying unknown fields lets each request disambiguate to the variant
    whose exact field set it matches.
    """
    structs = [
        "LimitPagination",
        "OffsetPagination",
        "CursorForwardPagination",
        "CursorBackwardPagination",
    ]
    applied: list[str] = []
    for name in structs:
        needle = f"pub struct {name} {{"
        idx = models.find(needle)
        if idx == -1:
            continue
        if models[:idx].rstrip().endswith("deny_unknown_fields)]"):
            continue
        models = models[:idx] + "#[serde(deny_unknown_fields)]\n" + models[idx:]
        applied.append(name)
    return models, applied


def fix_html_escaped_regex_patterns(models: str) -> tuple[str, int]:
    """Unescape HTML entities the generator emits inside regex patterns.

    rust-axum's templates HTML-escape the OpenAPI `pattern` keyword, so a spec
    pattern such as `^(<default>|[\\w\\.\\-]{1,31})$` is emitted as the literal
    Rust string `^(&lt;default&gt;|[\\w\\.\\-]{1,31})$`. The compiled
    `regex::Regex` then matches the seven characters `&lt;` instead of `<`, so
    legitimately valid values fail validation with a 400 — most visibly the
    `tenantId` value `<default>`, which is sent on nearly every request.

    Unescape the entities back to their literal characters, but *only* inside
    `regex::Regex::new("...")` string literals so nothing else is touched. The
    handled entities (`&lt;` `&gt;` `&#x3D;` `&amp;`) never expand to a `"` or
    `\\`, so they cannot break the surrounding Rust string literal.
    """
    entities = {"&lt;": "<", "&gt;": ">", "&#x3D;": "=", "&amp;": "&"}
    count = 0

    def unescape(match: re.Match) -> str:
        nonlocal count
        literal = match.group(0)
        new = literal
        for entity, char in entities.items():
            new = new.replace(entity, char)
        if new != literal:
            count += 1
        return new

    # Match a `regex::Regex::new("...")` call, allowing escaped chars (e.g. `\\w`)
    # inside the string literal.
    pattern = re.compile(r'regex::Regex::new\("(?:[^"\\]|\\.)*"\)')
    models = pattern.sub(unescape, models)
    return models, count


def fix_default_tenant_xss_false_positive(models: str) -> tuple[str, int]:
    """Allow the `<default>` tenant sentinel past the generated XSS guard.

    rust-axum decorates string fields with a custom validator,
    `check_xss_string`, whose body is `if ammonia::is_html(v) { Err } else
    { Ok }`. Camunda uses the literal alias `<default>` as the default tenant
    id, and `ammonia::is_html("<default>")` is `true` (it parses as a stray
    HTML tag). Every request carrying `tenantId: "<default>"` — the common
    case — therefore fails XSS validation with a 400.

    Inject an early `Ok` for the exact `<default>` sentinel so the otherwise
    desirable XSS guard still applies to all real input. The match is anchored
    on the unique function signature, so it patches the one definition only.
    """
    needle = (
        "pub fn check_xss_string(v: &str) "
        "-> std::result::Result<(), validator::ValidationError> {\n"
    )
    guard = (
        '    if v == "<default>" {\n'
        "        return std::result::Result::Ok(());\n"
        "    }\n"
    )
    if needle not in models or guard in models:
        return models, 0
    return models.replace(needle, needle + guard, 1), 1


def fix_discriminator_mapping_values(models: str) -> tuple[str, list[str]]:
    """Make each discriminated-`oneOf` member emit its discriminator *mapping key*.

    For a `oneOf` with `discriminator.mapping` (e.g. `WaitStateDetails`, keyed
    `JOB` → `JobWaitStateDetails`), rust-axum seeds every member struct's tag
    helper `_name_for_<prop>()` with the member's *schema name*
    (`"JobWaitStateDetails"`) and forces that value onto the wire through
    `serialize_with`. The spec's wire value is the mapping key (`"JOB"`), so
    every serialized member carried a wrong discriminator.

    The correct key is derived from the generated code itself (no second source
    of truth): the tagged enum lists each member as
    `#[serde(alias = "<key>")] <Variant>(models::<Struct>)`. Rewrite each member's
    `_name_for_<prop>` body to return that key. Fails closed when a member struct
    is claimed under two different keys, or when its helper cannot be found (a
    generator change must be looked at, not silently skipped).
    """
    enum_re = re.compile(
        r'#\[serde\(tag = "(?P<prop>[^"]+)"\)\]\n(?:#\[[^\n]*\]\n)*'
        r"pub enum (?P<enum>\w+) \{(?P<body>.*?)\n\}",
        re.S,
    )
    member_re = re.compile(
        r'#\[serde\(alias = "(?P<key>[^"]+)"\)\]\s*\w+\(models::(?P<struct>\w+)\),'
    )
    keys: dict[str, str] = {}
    for enum in enum_re.finditer(models):
        for member in member_re.finditer(enum.group("body")):
            struct, key = member.group("struct"), member.group("key")
            if keys.get(struct, key) != key:
                raise SystemExit(
                    f"discriminator: {struct} is mapped to both "
                    f"{keys[struct]!r} and {key!r}; cannot pick one wire tag"
                )
            keys[struct] = key

    applied: list[str] = []
    for struct, key in sorted(keys.items()):
        helper = re.compile(
            rf"(impl {struct} \{{\n    fn _name_for_\w+\(\) -> String \{{\n"
            rf'        String::from\(")[^"]*("\))'
        )
        models, n = helper.subn(rf"\g<1>{key}\g<2>", models)
        if n != 1:
            raise SystemExit(
                f"discriminator: expected one _name_for_ helper on {struct}, found {n}"
            )
        applied.append(f"{struct}={key}")
    return models, applied


OPTIONAL_JSON_EXTRACTOR_MARKER = "pub struct OptionalJson<T>"

OPTIONAL_JSON_EXTRACTOR = """

/// Extractor for an OPTIONAL JSON request body (injected by
/// `scripts/postprocess-generated.py`, issue #1531).
///
/// rust-axum emits `Json<Option<T>>` for a `requestBody` that is not
/// `required: true`, but axum's `Json` extractor rejects a request without a
/// `Content-Type: application/json` header with 415 *before* the `Option` is
/// considered, so a bodyless request -- which Camunda 8 accepts -- never
/// reached the handler. An empty body (with or without a content type) is
/// "no body" (`None`); any non-empty body is decoded exactly as `Json` would,
/// including its content-type check.
#[derive(Debug, Clone, Default)]
pub struct OptionalJson<T>(pub Option<T>);

impl<T, S> FromRequest<S> for OptionalJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        use axum::response::IntoResponse;

        // Buffer on the ORIGINAL request: `Bytes::from_request` reads the
        // router's `DefaultBodyLimit` extension from `req.extensions()`, so a
        // router that raises the limit is respected here.
        let headers = req.headers().clone();
        let bytes = <Bytes as FromRequest<S>>::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        if bytes.is_empty() {
            return Ok(OptionalJson(None));
        }
        // Rebuild with the original headers for the `Json` decode. The body is
        // already fully buffered, so disable the body limit on the rebuilt
        // request: otherwise the inner `Json` extraction re-applies the default
        // 2 MiB limit and 413s a body the router explicitly permitted.
        let mut rebuilt = Request::new(Body::from(bytes));
        *rebuilt.headers_mut() = headers;
        DefaultBodyLimit::disable().apply(&mut rebuilt);
        let Json(value) = <Json<Option<T>> as FromRequest<S>>::from_request(rebuilt, state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(OptionalJson(value))
    }
}
"""

_OPTIONAL_JSON_PARAM = re.compile(r"Json\(body\): Json<Option<(models::\w+)>>")


def fix_optional_json_bodies(server: str) -> tuple[str, int]:
    """Accept bodyless requests on optional-body operations (issue #1531).

    Rewrites every `Json(body): Json<Option<models::T>>` handler parameter to
    the `OptionalJson<T>` extractor and injects its definition once. The
    handler body is untouched: `OptionalJson(body)` binds the same
    `Option<models::T>` the generator expected.
    """
    server, count = _OPTIONAL_JSON_PARAM.subn(r"OptionalJson(body): OptionalJson<\1>", server)
    if count and OPTIONAL_JSON_EXTRACTOR_MARKER not in server:
        server += OPTIONAL_JSON_EXTRACTOR
    if re.search(r": Json<Option<", server):
        raise SystemExit("optional JSON body: unrecognised Json<Option<..>> parameter left behind")
    return server, count


# Dependencies only the axum server surface (`apis` + `server` modules) needs.
# They become optional and are pulled in by the `server` feature, so a
# `default-features = false` build is a MODELS-ONLY crate (serde models + types)
# that compiles for wasm32 (#1883): `engine-wasm` deserialises the generated
# `*SearchQuery` / serialises `*SearchQueryResult` through it instead of
# hand-writing parsers. `tokio` (rt-multi-thread) is what cannot target wasm32.
SERVER_ONLY_DEPS = ("async-trait", "axum", "axum-extra", "headers", "tokio", "tracing")


def make_models_only_buildable(cargo: str, lib: str) -> tuple[str, str]:
    """Gate the server-only surface behind the `server` feature (fail-closed)."""
    if "\nserver = []\n" not in cargo:
        raise SystemExit("postprocess: expected `server = []` feature in generated Cargo.toml")
    deps = ", ".join(f'"dep:{d}"' for d in SERVER_ONLY_DEPS)
    cargo = cargo.replace("\nserver = []\n", f"\nserver = [{deps}]\n", 1)
    for dep in SERVER_ONLY_DEPS:
        pat = re.compile(rf"^{re.escape(dep)} = (\{{ (.*?) \}}|\"([^\"]*)\")$", re.M | re.S)
        m = pat.search(cargo)
        if m is None:
            raise SystemExit(f"postprocess: dependency `{dep}` not found in generated Cargo.toml")
        if m.group(2) is not None:
            new = f"{dep} = {{ {m.group(2)}, optional = true }}"
        else:
            new = f'{dep} = {{ version = "{m.group(3)}", optional = true }}'
        cargo = cargo[: m.start()] + new + cargo[m.end():]
    if "\npub mod apis;\n" not in lib:
        raise SystemExit("postprocess: expected `pub mod apis;` in generated lib.rs")
    lib = lib.replace("\npub mod apis;\n", '\n#[cfg(feature = "server")]\npub mod apis;\n', 1)
    return cargo, lib


# Request bodies decoded STRICTLY: a property the spec does not define is
# rejected (400), never silently dropped by serde. These are the operations
# migrated onto the shared `nanobpmn-search` layer (#1883), whose WASM surface
# decodes through the same `strict::decode_strict`; the server/WASM parity guard
# (engine-wasm/src/search_parity_tests.rs) fails if the two ever differ. Extend
# this list as operations migrate (#1898).
STRICT_BODY_MODELS = (
    "AgentInstanceSearchQuery",
    "AgentInstanceHistorySearchQuery",
    # The WASM `search*` reads decode these strictly too (#1898); the gateway
    # must reject the same unknown fields or the two surfaces diverge.
    "UserTaskSearchQuery",
    "ProcessInstanceSearchQuery",
    "VariableSearchQuery",
)

STRICT_MODULE = """//! Strict request-body decoding (injected by
//! `scripts/postprocess-generated.py`, #1883).
//!
//! serde ignores unknown fields, so a misspelt or spec-foreign property (a
//! filter operator a property does not support, a filter property at the top
//! level instead of under `filter`) would otherwise decode to an EMPTY
//! criterion and run as an unfiltered search — the wrong rows with no error.
//! [`decode_strict`] is the single implementation shared by the gateway's
//! `StrictOptionalJson` extractor and the in-browser engine (`nanobpmn-search`).

/// Decode `json` into `T`, rejecting every non-null property that does not
/// survive the round trip through the generated model. An empty/whitespace or
/// `null` body is an absent body (`None`).
pub fn decode_strict<T>(json: &[u8]) -> Result<Option<T>, String>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    if json.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let raw: serde_json::Value = serde_json::from_slice(json).map_err(|e| e.to_string())?;
    let value: Option<T> = serde_json::from_value(raw.clone()).map_err(|e| e.to_string())?;
    if let Some(v) = &value {
        let canonical = serde_json::to_value(v).map_err(|e| e.to_string())?;
        reject_unknown_properties(&raw, &canonical, "")?;
    }
    Ok(value)
}

fn reject_unknown_properties(
    raw: &serde_json::Value,
    canonical: &serde_json::Value,
    path: &str,
) -> Result<(), String> {
    use serde_json::Value;
    match (raw, canonical) {
        (Value::Object(r), Value::Object(c)) => {
            for (key, value) in r {
                if value.is_null() {
                    continue;
                }
                let here = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match c.get(key) {
                    Some(cv) => reject_unknown_properties(value, cv, &here)?,
                    None => return Err(format!("unknown property `{here}` (not in the spec)")),
                }
            }
            Ok(())
        }
        (Value::Array(r), Value::Array(c)) if r.len() == c.len() => {
            r.iter().zip(c).enumerate().try_for_each(|(i, (rv, cv))| {
                reject_unknown_properties(rv, cv, &format!("{path}[{i}]"))
            })
        }
        _ => Ok(()),
    }
}
"""

STRICT_JSON_EXTRACTOR = """

/// Extractor for an optional JSON body decoded STRICTLY (injected by
/// `scripts/postprocess-generated.py`, #1883): like `OptionalJson`, but a
/// property the spec does not define is a 400, via the same
/// `crate::strict::decode_strict` the in-browser engine uses.
///
/// Body buffering, the empty-body acceptance and the JSON `Content-Type`
/// check (415) are DELEGATED to `OptionalJson`, so the two extractors cannot
/// drift on those rules. A strict rejection answers with the spec's error
/// contract: an `application/problem+json` `ProblemDetail`.
#[derive(Debug, Clone, Default)]
pub struct StrictOptionalJson<T>(pub Option<T>);

impl<T, S> FromRequest<S> for StrictOptionalJson<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let OptionalJson(raw) =
            <OptionalJson<serde_json::Value> as FromRequest<S>>::from_request(req, state).await?;
        let Some(raw) = raw else {
            return Ok(StrictOptionalJson(None));
        };
        let bytes = serde_json::to_vec(&raw).map_err(|e| strict_rejection(e.to_string()))?;
        crate::strict::decode_strict(&bytes)
            .map(StrictOptionalJson)
            .map_err(strict_rejection)
    }
}

/// A strict-decode rejection as the spec's 400 `ProblemDetail`.
fn strict_rejection(detail: String) -> Response {
    use axum::response::IntoResponse;

    let problem = models::ProblemDetail::new(
        "Bad Request".to_string(),
        400,
        detail,
        String::new(),
    );
    (
        StatusCode::BAD_REQUEST,
        [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
        axum::Json(problem),
    )
        .into_response()
}
"""


def apply_strict_bodies(server: str, models: str) -> tuple[str, int]:
    """Decode the STRICT_BODY_MODELS bodies with `StrictOptionalJson` (fail-closed)."""
    count = 0
    for model in STRICT_BODY_MODELS:
        if f"pub struct {model} " not in models:
            continue
        server, n = re.subn(
            rf"OptionalJson\(body\): OptionalJson<models::{model}>",
            f"StrictOptionalJson(body): StrictOptionalJson<models::{model}>",
            server,
        )
        if n != 1:
            raise SystemExit(f"postprocess: expected one OptionalJson<models::{model}> body, found {n}")
        count += n
    if count:
        server += STRICT_JSON_EXTRACTOR
    return server, count


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <generated-crate-dir>", file=sys.stderr)
        return 2

    crate_dir = Path(argv[1]).resolve()
    models_path = crate_dir / "src" / "models.rs"
    if not models_path.is_file():
        print(f"generated models.rs not found: {models_path}", file=sys.stderr)
        return 1

    models = models_path.read_text(encoding="utf-8")

    models, datetime_count = fix_oneof_datetime_variant(models)
    models, optional_props = fix_optional_discriminators(models)
    models, discriminator_keys = fix_discriminator_mapping_values(models)
    models, pagination_structs = fix_pagination_disambiguation(models)
    models, regex_count = fix_html_escaped_regex_patterns(models)
    models, xss_count = fix_default_tenant_xss_false_positive(models)

    models_path.write_text(models, encoding="utf-8")

    server_path = crate_dir / "src" / "server" / "mod.rs"
    optional_json_count = 0
    strict_count = 0
    if server_path.is_file():
        server = server_path.read_text(encoding="utf-8")
        server, optional_json_count = fix_optional_json_bodies(server)
        server, strict_count = apply_strict_bodies(server, models)
        server_path.write_text(server, encoding="utf-8")

    cargo_path = crate_dir / "Cargo.toml"
    lib_path = crate_dir / "src" / "lib.rs"
    cargo, lib = make_models_only_buildable(
        cargo_path.read_text(encoding="utf-8"), lib_path.read_text(encoding="utf-8")
    )
    if "\npub mod models;\n" not in lib:
        raise SystemExit("postprocess: expected `pub mod models;` in generated lib.rs")
    lib = lib.replace("\npub mod models;\n", "\npub mod models;\npub mod strict;\n", 1)
    (crate_dir / "src" / "strict.rs").write_text(STRICT_MODULE, encoding="utf-8")
    cargo_path.write_text(cargo, encoding="utf-8")
    lib_path.write_text(lib, encoding="utf-8")

    print("Post-processed generated code:")
    print(f"  oneOf date-time variant occurrences fixed: {datetime_count}")
    if optional_props:
        print(f"  optional discriminators fixed: {', '.join(optional_props)}")
    else:
        print("  optional discriminators fixed: none")
    if pagination_structs:
        print(f"  pagination structs disambiguated: {', '.join(pagination_structs)}")
    else:
        print("  pagination structs disambiguated: none")
    print(f"  discriminator mapping keys applied: {len(discriminator_keys)}")
    print(f"  HTML-escaped regex patterns unescaped: {regex_count}")
    print(f"  default-tenant XSS false positive fixed: {xss_count}")
    print(f"  optional JSON bodies made bodyless-tolerant: {optional_json_count}")
    print(f"  strictly-decoded request bodies: {strict_count}")
    print("  models-only build gated behind the `server` feature: yes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
