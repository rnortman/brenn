"""Which platforms a target is buildable for.

Two constants, used by every rule that straddles the host/wasm32 split. A
target that states neither is buildable under both, which is right for the
crates that compile either way and wrong for everything else: a wasm32-only
crate requested on the host fails to compile, and a host test requested under
wasm32 fails to resolve a test toolchain. Declaring the fact makes both cases
skip instead.
"""

# Crates whose only artifact form is a WASM module: the WIT guests, and the
# browser cdylibs whose DOM glue is behind `cfg(target_arch = "wasm32")`.
WASM32_ONLY = ["@platforms//cpu:wasm32"]

# Targets that run, or are exercised, on the host: tests, gates, and the host
# libraries beside a browser cdylib.
HOST_ONLY = select({
    "//bazel/platforms:is_wasm32": ["@platforms//:incompatible"],
    "//conditions:default": [],
})
