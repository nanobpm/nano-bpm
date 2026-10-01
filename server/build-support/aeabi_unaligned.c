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
