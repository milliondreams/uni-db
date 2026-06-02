#!/usr/bin/env bash
# Build the example wasm plugins used by the e2e tests in
# `crates/uni-plugin-extism/tests/example_extism_geo_e2e.rs` and
# `crates/uni-plugin-wasm/tests/example_wasm_geo_e2e.rs`.
#
# Both example crates declare their own `[workspace]`, so they're
# built independently of the outer uni workspace. The output paths
# are referenced verbatim from the test files (`include_bytes!` /
# `std::fs::read`).
#
# Prerequisites (one-time):
#   rustup target add wasm32-unknown-unknown wasm32-wasip2
#   cargo install wasm-tools --locked
#
# Usage:
#   ./scripts/build-wasm-fixtures.sh
#
# After running, you can:
#   cargo nextest run -p uni-plugin-extism --test example_extism_geo_e2e
#   cargo nextest run -p uni-plugin-wasm   --test example_wasm_geo_e2e

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Building example-extism-geo (wasm32-unknown-unknown)"
(
    cd examples/example-extism-geo
    cargo build --target wasm32-unknown-unknown --release
)

EXTISM_WASM="examples/example-extism-geo/target/wasm32-unknown-unknown/release/example_extism_geo.wasm"
if [[ ! -f "$EXTISM_WASM" ]]; then
    echo "ERROR: expected $EXTISM_WASM after build" >&2
    exit 1
fi
echo "    extism plugin size: $(du -h "$EXTISM_WASM" | cut -f1)"

echo "==> Building example-wasm-geo (wasm32-wasip2)"
(
    cd examples/example-wasm-geo
    cargo build --target wasm32-wasip2 --release
)

WASM_COMPONENT="examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm"

if [[ ! -f "$WASM_COMPONENT" ]]; then
    echo "ERROR: expected $WASM_COMPONENT after build" >&2
    exit 1
fi
echo "    wasm-geo component size: $(du -h "$WASM_COMPONENT" | cut -f1)"
echo "    (wasm32-wasip2 produces a Component Model binary directly; no wasm-tools wrap needed)"

echo "==> Building example-wasm-net (wasm32-wasip2)"
(
    cd examples/example-wasm-net
    cargo build --target wasm32-wasip2 --release
)

WASM_NET_COMPONENT="examples/example-wasm-net/target/wasm32-wasip2/release/example_wasm_net.wasm"
if [[ ! -f "$WASM_NET_COMPONENT" ]]; then
    echo "ERROR: expected $WASM_NET_COMPONENT after build" >&2
    exit 1
fi
echo "    wasm-net component size: $(du -h "$WASM_NET_COMPONENT" | cut -f1)"
echo "    (imports the capability-gated uni:plugin/host-net interface)"

echo "==> Done. Run tests with:"
echo "    cargo nextest run -p uni-plugin-extism --test example_extism_geo_e2e"
echo "    cargo nextest run -p uni-plugin-wasm   --test example_wasm_geo_e2e"
echo "    cargo nextest run -p uni-plugin-wasm   --test example_wasm_net_e2e"
