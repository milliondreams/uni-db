# example-wasm-geo

Example Component Model plugin implementing
`ai.example.geo.haversine`. Ships the full Arrow-IPC `invoke-scalar`
export — not just `manifest` + `register` — exercising the typed
WIT contract end-to-end through `Uni::load_wasm_component`.

## Build

```bash
rustup target add wasm32-wasip2   # one-time
cd examples/example-wasm-geo
cargo build --target wasm32-wasip2 --release
```

Output: `target/wasm32-wasip2/release/example_wasm_geo.wasm`
(~1.4 MB after `lto + opt-level=s + strip`). Or use the top-level
helper: `./scripts/build-wasm-fixtures.sh`.

**Note:** the current Rust `wasm32-wasip2` toolchain produces a
**Component Model binary directly** — no `wasm-tools component new`
post-processing required. The output file is already a CM
component (verifiable via the magic header `\0asm 0d 00 01 00`).

## Load from Rust

```rust
use uni_db::Uni;
use uni_plugin::{Capability, CapabilitySet};
use uni_plugin_wasm::WasmLoader;

let uni = Uni::in_memory().build().await?;
let loader = WasmLoader::new();
let bytes = std::fs::read("target/wasm32-wasip2/release/example_wasm_geo.wasm")?;
let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
let outcome = uni.load_wasm_component(&loader, &bytes, &[], &caps)?;
assert_eq!(outcome.plugin_id, "ai.example.geo");
assert!(outcome.scalars_registered.contains(&"ai.example.geo.haversine".to_owned()));
```

Once loaded, `ai.example.geo.haversine(lat1, lon1, lat2, lon2)` is
callable from any Cypher query against this `Uni` instance.

## Scope

The plugin ships:
- `manifest` export — canonical-JSON plugin manifest.
- `register` export — declares the scalar `ai.example.geo.haversine`.
- `invoke-scalar` export — typed WIT function taking `(qname,
  ipc-bytes)`, decoding the 4-column `Float64` `RecordBatch`,
  computing haversine row-wise, returning a 1-column `Float64` IPC
  stream.

Wasm size: ~1.4 MB. The 500 KB target from the original M6 plan was
aspirational; this follow-up accepts the size for full ABI
exercise.

## Workspace status

Standalone `[workspace]` declaration in `Cargo.toml` — `cargo build
--workspace` from the repo root does not pick this up.

## End-to-end test

`crates/uni-plugin-wasm/tests/example_wasm_geo_e2e.rs` loads this
prebuilt artifact and exercises the full path: load → manifest →
register → registry lookup → `invoke-scalar` with single-row and
multi-row batches. Hard-fails if the artifact is missing; run
`./scripts/build-wasm-fixtures.sh` first.
