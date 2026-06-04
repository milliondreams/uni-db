# `uni-plugin-wasm` WIT worlds

Component Model contracts for the uni-db plugin framework's WASM
loader. Per the M6 plan, one package (`uni:plugin@0.1.0`) carries
four worlds: `scalar-plugin`, `aggregate-plugin`, `procedure-plugin`,
`lua-host`.

## Binding generation

Bindings are generated at compile time by wasmtime's `bindgen!` macro
— no pre-committed `bindings/*.rs` files, no separate `wit-bindgen`
CLI dependency. See `src/bindings.rs` for the macro invocations.

This trades a build-time generation cost (negligible in `cargo check`
times) for zero drift risk: the bindings *cannot* go stale relative
to the WIT files.

## Adding a new world

1. Add the world block to `world.wit` (or split into a new file in
   this directory — `wasmtime::component::bindgen!` resolves the
   whole `wit/` tree as one package).
2. Add a matching `wasmtime::component::bindgen!` invocation to
   `src/bindings.rs`.
3. Wire up host-side adapters and `WasmLoader::load` dispatch.

## Versioning

The package is at `0.1.0`. ABI-breaking changes bump the minor
version (and the host carries the major in `Linker` selection — per
the proposal's per-major linker pattern).
