//! A coarse C-ABI surface over the engine, for embedding via FFI.
//!
//! This is the boundary a non-Rust host (a Swift/Kotlin app on mobile, or
//! JavaScript driving the `wasm32-unknown-unknown` build in a browser) calls
//! through. It is deliberately **coarse** — submit one command, read back a
//! scalar summary of what happened — rather than chatty, because every call
//! across an FFI boundary has a cost and the engine is a single owned state
//! machine.
//!
//! It stays **dependency-free and `std`-only**, like the rest of the crate, so
//! the same source compiles for `aarch64-apple-ios`, `aarch64-linux-android`
//! and `wasm32-unknown-unknown`. The crate must be built as a `cdylib` for the
//! exports below to appear in the artifact (`crate-type = ["lib", "cdylib"]`),
//! and this module is gated behind the off-by-default `ffi` feature so a pure
//! library embedder pays nothing for it.
//!
//! # Memory ownership
//!
//! The host allocates an input buffer with [`nbpmn_alloc`], writes its bytes
//! (BPMN XML, a process id, a message name…) into wasm linear memory, passes
//! the `(ptr, len)` to a call, and frees it with [`nbpmn_free`]. The engine
//! handle from [`nbpmn_engine_new`] must be released with [`nbpmn_engine_free`].
//! All pointers are borrowed for the duration of a call only.
//!
//! # Safety
//!
//! Every function is `unsafe` to call: the host must pass either null or a
//! pointer/length pair it actually owns, and a valid engine handle. Calls null-
//! check and validate UTF-8, returning a sentinel (`0` keys, negative counts)
//! rather than unwinding across the boundary.

use core::slice;

use crate::bpmn::parse_bpmn;
use crate::model::Value;
use crate::{Command, Engine};

/// Allocates `len` bytes inside the module and returns a pointer the host can
/// write to. Returns null for a zero-length request. Pair every call with
/// [`nbpmn_free`] using the **same** `len`.
///
/// # Safety
/// The returned pointer is valid for `len` bytes until passed to [`nbpmn_free`].
#[no_mangle]
pub unsafe extern "C" fn nbpmn_alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return core::ptr::null_mut();
    }
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    core::mem::forget(buf);
    ptr
}

/// Frees a buffer previously returned by [`nbpmn_alloc`]. `len` must match the
/// original allocation. A null pointer or zero length is a no-op.
///
/// # Safety
/// `ptr`/`len` must come from a prior [`nbpmn_alloc`] and not be used after.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    drop(Vec::from_raw_parts(ptr, 0, len));
}

/// Creates a fresh engine and returns an opaque handle. Release it with
/// [`nbpmn_engine_free`].
#[no_mangle]
pub extern "C" fn nbpmn_engine_new() -> *mut Engine {
    Box::into_raw(Box::new(Engine::new()))
}

/// Releases an engine handle from [`nbpmn_engine_new`]. Null is a no-op.
///
/// # Safety
/// `engine` must be a handle from [`nbpmn_engine_new`], not yet freed.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_engine_free(engine: *mut Engine) {
    if !engine.is_null() {
        drop(Box::from_raw(engine));
    }
}

/// Borrows the engine and the `(ptr, len)` bytes as a `&str`, or returns `None`
/// if anything is invalid (null engine/ptr, or non-UTF-8 bytes).
unsafe fn engine_and_str<'a>(
    engine: *mut Engine,
    ptr: *const u8,
    len: usize,
) -> Option<(&'a mut Engine, &'a str)> {
    if engine.is_null() || ptr.is_null() {
        return None;
    }
    let bytes = slice::from_raw_parts(ptr, len);
    let text = core::str::from_utf8(bytes).ok()?;
    Some((&mut *engine, text))
}

/// Parses BPMN 2.0 XML and deploys every process in it as one deployment.
/// Returns the number of processes deployed, or a negative error code:
/// `-1` invalid arguments (null/handle/UTF-8), `-2` the XML failed to parse,
/// `-3` the deployment was rejected (e.g. a process with no start event).
///
/// # Safety
/// `engine` must be a valid handle and `xml`/`xml_len` a UTF-8 buffer the
/// caller owns (or null/0).
#[no_mangle]
pub unsafe extern "C" fn nbpmn_deploy_bpmn(
    engine: *mut Engine,
    xml: *const u8,
    xml_len: usize,
) -> i64 {
    let Some((engine, xml)) = engine_and_str(engine, xml, xml_len) else {
        return -1;
    };
    let processes = match parse_bpmn(xml) {
        Ok(p) => p,
        Err(_) => return -2,
    };
    let count = processes.len() as i64;
    match engine.apply_command(Command::DeployResources(processes)) {
        Ok(_) => count,
        Err(_) => -3,
    }
}

/// Creates an instance of the process with id `(id, id_len)` at clock `now`.
/// Returns the new instance key, or `0` on any error (null/handle/UTF-8, or no
/// such process).
///
/// # Safety
/// `engine` must be a valid handle and `id`/`id_len` a UTF-8 buffer (or null/0).
#[no_mangle]
pub unsafe extern "C" fn nbpmn_create_instance(
    engine: *mut Engine,
    id: *const u8,
    id_len: usize,
    now: u64,
) -> u64 {
    let Some((engine, id)) = engine_and_str(engine, id, id_len) else {
        return 0;
    };
    match engine.apply_command_at(Command::create_instance(id), now) {
        Ok(events) => events.iter().find_map(|e| e.instance_key()).unwrap_or(0),
        Err(_) => 0,
    }
}

/// Correlates a message named `(name, name_len)` with correlation key
/// `(key, key_len)` at clock `now`. Returns the number of events produced (a
/// published message always yields at least one), or a negative code for
/// invalid arguments (`-1`).
///
/// # Safety
/// `engine` must be a valid handle and the name/key buffers UTF-8 (or null/0).
#[no_mangle]
pub unsafe extern "C" fn nbpmn_correlate_message(
    engine: *mut Engine,
    name: *const u8,
    name_len: usize,
    key: *const u8,
    key_len: usize,
    now: u64,
) -> i64 {
    if engine.is_null() || name.is_null() {
        return -1;
    }
    let Ok(name) = core::str::from_utf8(slice::from_raw_parts(name, name_len)) else {
        return -1;
    };
    let correlation_key = if key.is_null() {
        ""
    } else {
        match core::str::from_utf8(slice::from_raw_parts(key, key_len)) {
            Ok(k) => k,
            Err(_) => return -1,
        }
    };
    let engine = &mut *engine;
    engine
        .correlate_message(name, correlation_key, Default::default(), now)
        .len() as i64
}

/// Fires every timer due at clock `now` (timer catch events, boundary timers and
/// timer start events). Returns the number of events produced.
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_trigger_timers(engine: *mut Engine, now: u64) -> i64 {
    if engine.is_null() {
        return -1;
    }
    (*engine).trigger_timers(now).len() as i64
}

/// Returns `1` if the instance with `instance_key` has completed, `0` if it is
/// still active or unknown, `-1` for a null handle.
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_is_completed(engine: *mut Engine, instance_key: u64) -> i32 {
    if engine.is_null() {
        return -1;
    }
    i32::from((*engine).is_completed(instance_key))
}

/// Returns the number of process instances the engine is tracking (active and
/// completed), or `-1` for a null handle.
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_instance_count(engine: *mut Engine) -> i64 {
    if engine.is_null() {
        return -1;
    }
    (*engine).state().instances.len() as i64
}

// ─── Job worker surface (ABI v2) ────────────────────────────────────────────
//
// Job activation returns a variable-length list of ActivatedJob records; the
// coarse C-ABI passes that back as a single caller-freeable buffer of JSON.
// Rationale:
//
//   * A row-by-row cursor API would require the engine to hold per-caller
//     iteration state (breaking single-writer determinism) or force a
//     round-trip per job (fatal on the wasm boundary at any real rate).
//   * A hand-rolled binary framing (length-prefixed tuples) buys nothing
//     over JSON here — the payload is dominated by variables, which are a
//     tree, and every host language already has a fast JSON parser.
//   * `engine-core` stays dependency-free: `write_jobs_json` and `write_value_json`
//     below are a small hand-written serializer (~60 lines) covering just the
//     `Value` variants the engine emits. This preserves the "one crate, three
//     targets, zero deps" property that makes the wasm build small.
//
// The engine allocates the JSON buffer with `nbpmn_alloc`, then writes the
// pointer and length into caller-provided out-params (`out_ptr`, `out_len`).
// The host reads `*out_len` bytes at `*out_ptr`, then frees with
// `nbpmn_free(*out_ptr, *out_len)` using the same length. Portable across
// 32- and 64-bit hosts (no ptr-into-u64 packing), so `cargo test` on the
// native host exercises the same code path as the wasm build.

/// Serialize a `Value` into `out` as JSON. Standard escape rules; matches
/// the shape `engine-wasm`'s `vars_to_json` produces so hosts can share a
/// parser between the wasm-bindgen and FFI builds.
fn write_value_json(out: &mut String, v: &Value) {
    use core::fmt::Write as _;
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => {
            let _ = write!(out, "{}", i);
        }
        Value::Double(d) => {
            if d.is_finite() {
                let _ = write!(out, "{}", d);
            } else {
                out.push_str("null");
            }
        }
        Value::Str(s) => write_json_string(out, s),
        Value::List(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value_json(out, item);
            }
            out.push(']');
        }
        Value::Map(entries) => {
            out.push('{');
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(out, k);
                out.push(':');
                write_value_json(out, v);
            }
            out.push('}');
        }
    }
}

/// Emit a JSON string literal with the escapes required by RFC 8259 §7.
fn write_json_string(out: &mut String, s: &str) {
    crate::json::write_json_string(out, s);
}

/// Copy `bytes` into a freshly-allocated wasm-side buffer and write its
/// pointer and length into the caller-provided out-params. The host owns
/// the buffer and must call `nbpmn_free(*out_ptr, *out_len)` to release it.
/// A zero-length payload writes `null`/`0` into the out-params (freeing null
/// is a no-op, so callers can uniformly `nbpmn_free` unconditionally).
///
/// # Safety
/// `out_ptr` and `out_len` must be non-null and writable.
unsafe fn emit_owned(bytes: &[u8], out_ptr: *mut *mut u8, out_len: *mut usize) {
    if bytes.is_empty() {
        *out_ptr = core::ptr::null_mut();
        *out_len = 0;
        return;
    }
    let ptr = nbpmn_alloc(bytes.len());
    if ptr.is_null() {
        *out_ptr = core::ptr::null_mut();
        *out_len = 0;
        return;
    }
    core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
    *out_ptr = ptr;
    *out_len = bytes.len();
}

/// Activate up to `max_jobs` `Created` jobs of `job_type`, locking each to
/// `worker` until `now + timeout_ms`. Writes a caller-owned JSON blob into
/// `*out_ptr` / `*out_len` (free with `nbpmn_free(*out_ptr, *out_len)`),
/// shaped as:
///
/// ```json
/// [
///   { "key": "…", "type": "…", "instanceKey": "…", "elementInstanceKey": "…",
///     "elementId": "…", "worker": "…", "deadline": 12345, "retries": 3,
///     "jobLeaseToken": "…" (only for leased jobs, #1106/#1283),
///     "variables": { … } }
/// ]
/// ```
///
/// Returns the number of activated jobs (`0` when nothing matched — the
/// out-buffer is still a valid `[]`), or a negative error code (`-1` null
/// engine / bad UTF-8, `-2` null out-params).
///
/// # Safety
/// `engine` must be a valid handle; `job_type`/`worker` buffers must be UTF-8
/// (or null/0); `out_ptr` and `out_len` must be non-null.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_activate_jobs(
    engine: *mut Engine,
    job_type: *const u8,
    job_type_len: usize,
    worker: *const u8,
    worker_len: usize,
    max_jobs: u32,
    timeout_ms: u64,
    now: u64,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if out_ptr.is_null() || out_len.is_null() {
        return -2;
    }
    // Default the out-params so callers can always uniformly free the result.
    *out_ptr = core::ptr::null_mut();
    *out_len = 0;
    if engine.is_null() || job_type.is_null() || worker.is_null() {
        return -1;
    }
    let Ok(job_type) = core::str::from_utf8(slice::from_raw_parts(job_type, job_type_len)) else {
        return -1;
    };
    let Ok(worker) = core::str::from_utf8(slice::from_raw_parts(worker, worker_len)) else {
        return -1;
    };
    let engine = &mut *engine;
    let activated = engine.activate_jobs(
        job_type,
        worker,
        (max_jobs.max(1)) as usize,
        timeout_ms,
        now,
    );
    let mut json = String::from("[");
    for (i, job) in activated.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        json.push_str("{\"key\":\"");
        push_u64(&mut json, job.key);
        json.push_str("\",\"type\":");
        write_json_string(&mut json, &job.job_type);
        json.push_str(",\"instanceKey\":\"");
        push_u64(&mut json, job.instance_key);
        json.push_str("\",\"elementInstanceKey\":\"");
        push_u64(&mut json, job.element_instance_key);
        json.push_str("\",\"elementId\":");
        write_json_string(&mut json, &job.element_id);
        json.push_str(",\"worker\":");
        write_json_string(&mut json, &job.worker);
        json.push_str(",\"deadline\":");
        {
            use core::fmt::Write as _;
            let _ = write!(json, "{}", job.deadline);
        }
        if let Some(lease) = &job.lease_token {
            json.push_str(",\"jobLeaseToken\":");
            write_json_string(&mut json, lease);
        } else {
            json.push_str(",\"jobLeaseToken\":null");
        }
        json.push_str(",\"retries\":");
        {
            use core::fmt::Write as _;
            let _ = write!(json, "{}", job.retries);
        }
        json.push_str(",\"bpmnProcessId\":");
        write_json_string(&mut json, &job.bpmn_process_id);
        json.push_str(",\"processDefinitionKey\":\"");
        push_u64(&mut json, job.process_definition_key);
        json.push_str("\",\"processDefinitionVersion\":");
        {
            use core::fmt::Write as _;
            let _ = write!(json, "{}", job.process_definition_version);
        }
        json.push_str(",\"priority\":");
        {
            use core::fmt::Write as _;
            let _ = write!(json, "{}", job.priority);
        }
        json.push_str(",\"customHeaders\":{");
        for (j, (k, v)) in job.custom_headers.iter().enumerate() {
            if j > 0 {
                json.push(',');
            }
            write_json_string(&mut json, k);
            json.push(':');
            write_json_string(&mut json, v);
        }
        json.push_str("},\"tags\":[");
        for (j, tag) in job.tags.iter().enumerate() {
            if j > 0 {
                json.push(',');
            }
            write_json_string(&mut json, tag);
        }
        json.push(']');
        // Always emit `businessId` (using `null` when absent) so the C-ABI
        // activation surface matches the wasm `TestEngine` shape, which
        // serializes `Option<String>` as `null` rather than dropping the key.
        json.push_str(",\"businessId\":");
        match &job.business_id {
            Some(business_id) => write_json_string(&mut json, business_id),
            None => json.push_str("null"),
        }
        json.push_str(",\"variables\":{");
        for (j, (k, v)) in job.variables.iter().enumerate() {
            if j > 0 {
                json.push(',');
            }
            write_json_string(&mut json, k);
            json.push(':');
            write_value_json(&mut json, v);
        }
        json.push_str("}}");
    }
    json.push(']');
    emit_owned(json.as_bytes(), out_ptr, out_len);
    activated.len() as i32
}

/// u64→decimal without `format!`/`ryu` — keeps the dep-free promise. Handles 0
/// specially; otherwise writes digits into a stack buffer big enough for `u64::MAX`
/// (20 digits) and appends in order.
fn push_u64(out: &mut String, n: u64) {
    if n == 0 {
        out.push('0');
        return;
    }
    let mut buf = [0u8; 20];
    let mut i = 0;
    let mut x = n;
    while x > 0 {
        buf[i] = b'0' + (x % 10) as u8;
        x /= 10;
        i += 1;
    }
    while i > 0 {
        i -= 1;
        out.push(buf[i] as char);
    }
}

/// Complete an activated job. Returns `0` on success, or a negative error code:
/// `-1` null engine, `-2` no such job, `-3` job is not in a completable state,
/// `-4` completion was otherwise rejected. Variables-on-complete are deferred
/// to a future ABI bump (a JSON parser in `engine-core` would break the
/// dep-free promise; the host workaround for now is to `nbpmn_set_variables`
/// on the instance before completing — a v3 API).
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_complete_job(engine: *mut Engine, job_key: u64) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = &mut *engine;
    if engine.job(job_key).is_none() {
        return -2;
    }
    let cmd = Command::CompleteJob {
        job_key,
        lease_token: None,
        variables: Default::default(),
        adhoc_result: None,
        task_listener_result: None,
    };
    match engine.apply_command(cmd) {
        Ok(_) => 0,
        Err(_) => -4,
    }
}

/// Fail an activated job. `retries` is the *remaining* retry count (a value of
/// `0` parks the job with an incident). Returns `0` on success, negative on
/// error (`-1` null engine, `-2` no such job, `-3` invalid message UTF-8, `-4`
/// otherwise rejected).
///
/// # Safety
/// `engine` must be a valid handle; message bytes (if any) must be UTF-8.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_fail_job(
    engine: *mut Engine,
    job_key: u64,
    retries: i32,
    message: *const u8,
    message_len: usize,
) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = &mut *engine;
    if engine.job(job_key).is_none() {
        return -2;
    }
    let error_message = if message.is_null() || message_len == 0 {
        String::new()
    } else {
        match core::str::from_utf8(slice::from_raw_parts(message, message_len)) {
            Ok(s) => s.to_string(),
            Err(_) => return -3,
        }
    };
    let cmd = Command::FailJob {
        job_key,
        lease_token: None,
        retries,
        error_message,
    };
    match engine.apply_command(cmd) {
        Ok(_) => 0,
        Err(_) => -4,
    }
}

/// Release the activation lock of every job whose `deadline` is at or before
/// `now`. Callers pair this with [`nbpmn_trigger_timers`] to implement the
/// wall-clock tick loop the host owns. Returns the number of events emitted,
/// or `-1` for a null handle.
///
/// # Safety
/// `engine` must be a valid handle.
#[no_mangle]
pub unsafe extern "C" fn nbpmn_expire_jobs(engine: *mut Engine, now: u64) -> i64 {
    if engine.is_null() {
        return -1;
    }
    let engine = &mut *engine;
    match engine.apply_command_at(Command::ExpireJobs { now }, now) {
        Ok(events) => events.len() as i64,
        Err(_) => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drives a full deploy → create → complete cycle entirely through the
    /// C-ABI surface, mirroring how a host would call it.
    #[test]
    fn should_run_a_process_through_the_ffi() {
        const XML: &[u8] = br#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        unsafe {
            let engine = nbpmn_engine_new();
            assert!(!engine.is_null());

            // Deploy one process through the boundary.
            let deployed = nbpmn_deploy_bpmn(engine, XML.as_ptr(), XML.len());
            assert_eq!(deployed, 1);

            // Create and run an instance; it completes immediately (start->end).
            let id = b"p";
            let instance_key = nbpmn_create_instance(engine, id.as_ptr(), id.len(), 0);
            assert_ne!(instance_key, 0);
            assert_eq!(nbpmn_is_completed(engine, instance_key), 1);
            assert_eq!(nbpmn_instance_count(engine), 1);

            // An unknown process id is rejected with the 0 sentinel.
            let bad = b"nope";
            assert_eq!(nbpmn_create_instance(engine, bad.as_ptr(), bad.len(), 0), 0);

            nbpmn_engine_free(engine);
        }
    }

    #[test]
    fn should_round_trip_an_alloc() {
        unsafe {
            let ptr = nbpmn_alloc(8);
            assert!(!ptr.is_null());
            nbpmn_free(ptr, 8);
            // A zero-length alloc is null and freeing null is a no-op.
            assert!(nbpmn_alloc(0).is_null());
            nbpmn_free(core::ptr::null_mut(), 0);
        }
    }

    #[test]
    fn should_reject_bad_arguments_without_unwinding() {
        unsafe {
            assert_eq!(
                nbpmn_deploy_bpmn(core::ptr::null_mut(), core::ptr::null(), 0),
                -1
            );
            assert_eq!(nbpmn_is_completed(core::ptr::null_mut(), 1), -1);
            assert_eq!(nbpmn_instance_count(core::ptr::null_mut()), -1);

            let engine = nbpmn_engine_new();
            // Invalid UTF-8 in the XML buffer is a parse-stage rejection.
            let bytes = [0xff, 0xfe];
            assert_eq!(nbpmn_deploy_bpmn(engine, bytes.as_ptr(), bytes.len()), -1);
            // Well-formed bytes that aren't BPMN fail to parse (-2).
            let not_bpmn = b"<x/>";
            assert_eq!(
                nbpmn_deploy_bpmn(engine, not_bpmn.as_ptr(), not_bpmn.len()),
                -2
            );
            nbpmn_engine_free(engine);
        }
    }

    /// Full deploy → create → activate → complete cycle through the job
    /// surface. Verifies the JSON blob shape and that completion drains the
    /// activation.
    #[test]
    fn should_activate_and_complete_a_job() {
        const XML: &[u8] = br#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="t">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="work" />
                  <zeebe:taskHeaders>
                    <zeebe:header key="channel" value="card" />
                  </zeebe:taskHeaders>
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
              <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        unsafe {
            let engine = nbpmn_engine_new();
            assert_eq!(nbpmn_deploy_bpmn(engine, XML.as_ptr(), XML.len()), 1);
            let instance = nbpmn_create_instance(engine, b"p".as_ptr(), 1, 0);
            assert_ne!(instance, 0);
            assert_eq!(nbpmn_is_completed(engine, instance), 0);

            let job_type = b"work";
            let worker = b"w1";
            let mut buf_ptr: *mut u8 = core::ptr::null_mut();
            let mut buf_len: usize = 0;
            let count = nbpmn_activate_jobs(
                engine,
                job_type.as_ptr(),
                job_type.len(),
                worker.as_ptr(),
                worker.len(),
                10,
                30_000,
                0,
                &mut buf_ptr,
                &mut buf_len,
            );
            assert_eq!(count, 1, "expected exactly one activated job");
            assert!(!buf_ptr.is_null() && buf_len > 0);
            let json = core::str::from_utf8(slice::from_raw_parts(buf_ptr, buf_len)).unwrap();
            assert!(json.starts_with('['), "not a JSON array: {json}");
            assert!(json.contains("\"type\":\"work\""), "no type: {json}");
            assert!(json.contains("\"worker\":\"w1\""), "no worker: {json}");
            assert!(json.contains("\"retries\":"), "no retries: {json}");
            assert!(json.contains("\"variables\":"), "no variables: {json}");
            assert!(
                json.contains("\"customHeaders\":{\"channel\":\"card\"}"),
                "no custom headers: {json}"
            );
            assert!(
                json.contains("\"bpmnProcessId\":\"p\""),
                "no bpmnProcessId: {json}"
            );
            assert!(
                json.contains("\"processDefinitionVersion\":1"),
                "no processDefinitionVersion: {json}"
            );
            assert!(json.contains("\"priority\":50"), "no priority: {json}");
            assert!(json.contains("\"tags\":["), "no tags: {json}");
            // `businessId` is always emitted (null when absent) so this surface
            // matches the wasm `TestEngine` job shape.
            assert!(
                json.contains("\"businessId\":null"),
                "businessId should be present as null: {json}"
            );
            // extract the job key: `"key":"<digits>"`
            let k_start = json.find("\"key\":\"").unwrap() + 7;
            let k_end = k_start + json[k_start..].find('"').unwrap();
            let job_key: u64 = json[k_start..k_end].parse().unwrap();
            nbpmn_free(buf_ptr, buf_len);

            // Completing the job advances the token and ends the instance.
            assert_eq!(nbpmn_complete_job(engine, job_key), 0);
            assert_eq!(nbpmn_is_completed(engine, instance), 1);

            // Completing again is rejected — the job still exists in state
            // but is no longer in a completable state, so apply_command
            // returns the "otherwise rejected" (-4) sentinel.
            assert_eq!(nbpmn_complete_job(engine, job_key), -4);

            nbpmn_engine_free(engine);
        }
    }

    /// Activation with no matching jobs returns a valid empty `[]` blob, not
    /// an error. This keeps host code simple ("always parse the result").
    #[test]
    fn should_return_empty_array_when_no_jobs_match() {
        unsafe {
            let engine = nbpmn_engine_new();
            let mut buf_ptr: *mut u8 = core::ptr::null_mut();
            let mut buf_len: usize = 0;
            let count = nbpmn_activate_jobs(
                engine,
                b"nothing".as_ptr(),
                7,
                b"w".as_ptr(),
                1,
                10,
                30_000,
                0,
                &mut buf_ptr,
                &mut buf_len,
            );
            assert_eq!(count, 0);
            let json = core::str::from_utf8(slice::from_raw_parts(buf_ptr, buf_len)).unwrap();
            assert_eq!(json, "[]");
            nbpmn_free(buf_ptr, buf_len);
            nbpmn_engine_free(engine);
        }
    }

    /// Failing a job with retries left returns it to the activatable pool;
    /// failing with 0 retries parks it. The FFI mirrors both paths.
    #[test]
    fn should_fail_a_job() {
        const XML: &[u8] = br#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                            xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
            <bpmn:process id="p" isExecutable="true">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="t">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="work" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
              <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;
        unsafe {
            let engine = nbpmn_engine_new();
            assert_eq!(nbpmn_deploy_bpmn(engine, XML.as_ptr(), XML.len()), 1);
            nbpmn_create_instance(engine, b"p".as_ptr(), 1, 0);
            let mut buf_ptr: *mut u8 = core::ptr::null_mut();
            let mut buf_len: usize = 0;
            let count = nbpmn_activate_jobs(
                engine,
                b"work".as_ptr(),
                4,
                b"w1".as_ptr(),
                2,
                10,
                30_000,
                0,
                &mut buf_ptr,
                &mut buf_len,
            );
            assert_eq!(count, 1);
            let json = core::str::from_utf8(slice::from_raw_parts(buf_ptr, buf_len))
                .unwrap()
                .to_string();
            nbpmn_free(buf_ptr, buf_len);
            let k_start = json.find("\"key\":\"").unwrap() + 7;
            let k_end = k_start + json[k_start..].find('"').unwrap();
            let job_key: u64 = json[k_start..k_end].parse().unwrap();

            let msg = b"boom";
            assert_eq!(
                nbpmn_fail_job(engine, job_key, 2, msg.as_ptr(), msg.len()),
                0
            );
            // A null message is accepted.
            assert_eq!(nbpmn_fail_job(engine, job_key, 1, core::ptr::null(), 0), 0);
            // Bad key rejected.
            assert_eq!(
                nbpmn_fail_job(engine, 9_999_999, 0, core::ptr::null(), 0),
                -2
            );
            nbpmn_engine_free(engine);
        }
    }

    #[test]
    fn should_write_json_escapes() {
        let mut out = String::new();
        write_json_string(&mut out, "a\"b\\c\nd\t");
        assert_eq!(out, "\"a\\\"b\\\\c\\nd\\t\"");
        out.clear();
        write_value_json(&mut out, &Value::Null);
        assert_eq!(out, "null");
        out.clear();
        write_value_json(&mut out, &Value::Int(42));
        assert_eq!(out, "42");
        out.clear();
        write_value_json(&mut out, &Value::Bool(true));
        assert_eq!(out, "true");
    }
}
