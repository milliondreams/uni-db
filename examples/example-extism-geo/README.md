# example-extism-geo

Example Extism plugin implementing `ai.example.geo.haversine` for the
M6 acceptance criterion. Ships the full Arrow-IPC `invoke` export —
not just `manifest` + `register` — so the host-to-wasm-and-back path
is exercised end-to-end.

## Build

```bash
rustup target add wasm32-unknown-unknown   # one-time setup
cd examples/example-extism-geo
cargo build --target wasm32-unknown-unknown --release
```

Output: `target/wasm32-unknown-unknown/release/example_extism_geo.wasm`
(~1.4 MB after `lto + opt-level=s + strip`). Or use the top-level
helper: `./scripts/build-wasm-fixtures.sh`.

## Load from Rust

```rust
use uni_db::{Uni, UniBuilder};
use uni_plugin::{Capability, CapabilitySet};
use uni_plugin_extism::ExtismLoader;

let uni = UniBuilder::new("memory://test").build().await?;
let loader = ExtismLoader::new();
let bytes = std::fs::read("target/wasm32-unknown-unknown/release/example_extism_geo.wasm")?;
let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
let outcome = uni.load_wasm_extism(&loader, &bytes, &[], &caps)?;
assert_eq!(outcome.plugin_id, "ai.example.geo");
assert!(outcome.scalars_registered.contains(&"ai.example.geo.haversine".to_owned()));
```

Once loaded, `ai.example.geo.haversine(lat1, lon1, lat2, lon2)` is
callable from any Cypher query against this `Uni` instance.

## Scope

The plugin ships:
- `manifest` export — canonical-JSON plugin manifest.
- `register` export — declares the scalar `ai.example.geo.haversine`.
- `invoke_ai_example_geo_haversine` export — Arrow-IPC in, Arrow-IPC
  out. Decodes a 4-column `Float64` `RecordBatch`, computes the
  haversine row-wise, returns a 1-column `Float64` batch.

Pulling in `arrow-ipc` on `wasm32-unknown-unknown` adds ~1 MB to the
wasm. The 500 KB target from the original M6 plan was aspirational;
the M6 deferred-followup completion accepts the size in exchange for
exercising the same wire format as the host.

## Workspace status

This crate declares `[workspace]` in its own `Cargo.toml`, opting it
out of the outer `uni` workspace so `cargo build --workspace` from
the repo root does not try to build it for the host target.

## End-to-end test

`crates/uni-plugin-extism/tests/example_extism_geo_e2e.rs` loads
this prebuilt artifact and exercises the full path: load → manifest
→ register → registry lookup → `invoke` with single-row and
multi-row batches. Hard-fails if the artifact is missing; run
`./scripts/build-wasm-fixtures.sh` first.
