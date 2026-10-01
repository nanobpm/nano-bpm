/*
 * Weak ARM RTABI unaligned-access helpers (IHI0043, section 4.3.3), linked into
 * armv6 zigbuild binaries via scripts/zig-arm-shim-env.sh (#1322). libgcc and
 * Rust's compiler_builtins provide these, but cargo-zigbuild drops
 * compiler_builtins on ARM and zig's compiler-rt lacks them.
 *
 * Verbatim from cargo-zigbuild v0.23.4 (src/zig/mod.rs, AEABI_UNALIGNED_C;
 * MIT, https://github.com/rust-cross/cargo-zigbuild, commit 236ff1da).
 * Compiled with zig's strict_align -mcpu, the memcpy lowers to byte loads and
 * stores, so these never recurse into themselves.
 */
#ifdef __cplusplus
extern "C" {
#endif
__attribute__((weak)) int __aeabi_uread4(void *address) {
    int value;
    __builtin_memcpy(&value, address, 4);
    return value;
}
__attribute__((weak)) int __aeabi_uwrite4(int value, void *address) {
    __builtin_memcpy(address, &value, 4);
    return value;
}
__attribute__((weak)) long long __aeabi_uread8(void *address) {
    long long value;
    __builtin_memcpy(&value, address, 8);
    return value;
}
__attribute__((weak)) long long __aeabi_uwrite8(long long value, void *address) {
    __builtin_memcpy(address, &value, 8);
    return value;
}
#ifdef __cplusplus
}
#endif
