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

echo "==> Building example-extism-stateful (wasm32-unknown-unknown)"
(
    cd examples/example-extism-stateful
    cargo build --target wasm32-unknown-unknown --release
)

EXTISM_STATEFUL_WASM="examples/example-extism-stateful/target/wasm32-unknown-unknown/release/example_extism_stateful.wasm"
if [[ ! -f "$EXTISM_STATEFUL_WASM" ]]; then
    echo "ERROR: expected $EXTISM_STATEFUL_WASM after build" >&2
    exit 1
fi
echo "    extism-stateful plugin size: $(du -h "$EXTISM_STATEFUL_WASM" | cut -f1)"
echo "    (mutable global; proves a fresh extism::Plugin per invoke)"

echo "==> Building example-extism-net (wasm32-unknown-unknown)"
(
    cd examples/example-extism-net
    cargo build --target wasm32-unknown-unknown --release
)

EXTISM_NET_WASM="examples/example-extism-net/target/wasm32-unknown-unknown/release/example_extism_net.wasm"
if [[ ! -f "$EXTISM_NET_WASM" ]]; then
    echo "ERROR: expected $EXTISM_NET_WASM after build" >&2
    exit 1
fi
echo "    extism-net plugin size: $(du -h "$EXTISM_NET_WASM" | cut -f1)"
echo "    (imports + calls the capability-gated uni_http_get host fn)"

echo "==> Building example-extism-graph (wasm32-unknown-unknown)"
(
    cd examples/example-extism-graph
    cargo build --target wasm32-unknown-unknown --release
)

EXTISM_GRAPH_WASM="examples/example-extism-graph/target/wasm32-unknown-unknown/release/example_extism_graph.wasm"
if [[ ! -f "$EXTISM_GRAPH_WASM" ]]; then
    echo "ERROR: expected $EXTISM_GRAPH_WASM after build" >&2
    exit 1
fi
echo "    extism-graph plugin size: $(du -h "$EXTISM_GRAPH_WASM" | cut -f1)"
echo "    (GraphCompute Personalized PageRank via the uni_graph_call host fn)"

echo "==> Building example-extism-manifest-callout (wasm32-unknown-unknown)"
(
    cd examples/example-extism-manifest-callout
    cargo build --target wasm32-unknown-unknown --release
)

EXTISM_MANIFEST_CALLOUT_WASM="examples/example-extism-manifest-callout/target/wasm32-unknown-unknown/release/example_extism_manifest_callout.wasm"
if [[ ! -f "$EXTISM_MANIFEST_CALLOUT_WASM" ]]; then
    echo "ERROR: expected $EXTISM_MANIFEST_CALLOUT_WASM after build" >&2
    exit 1
fi
echo "    extism-manifest-callout plugin size: $(du -h "$EXTISM_MANIFEST_CALLOUT_WASM" | cut -f1)"
echo "    (manifest-declared exports fire host callouts under uninteresting grants)"

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

echo "==> Building example-wasm-stateful (wasm32-wasip2)"
(
    cd examples/example-wasm-stateful
    cargo build --target wasm32-wasip2 --release
)

WASM_STATEFUL_COMPONENT="examples/example-wasm-stateful/target/wasm32-wasip2/release/example_wasm_stateful.wasm"
if [[ ! -f "$WASM_STATEFUL_COMPONENT" ]]; then
    echo "ERROR: expected $WASM_STATEFUL_COMPONENT after build" >&2
    exit 1
fi
echo "    wasm-stateful component size: $(du -h "$WASM_STATEFUL_COMPONENT" | cut -f1)"
echo "    (mutable global; drives the per-invoke isolation repros)"

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

echo "==> Building example-wasm-graph (wasm32-wasip2)"
(
    cd examples/example-wasm-graph
    cargo build --target wasm32-wasip2 --release
)

WASM_GRAPH_COMPONENT="examples/example-wasm-graph/target/wasm32-wasip2/release/example_wasm_graph.wasm"
if [[ ! -f "$WASM_GRAPH_COMPONENT" ]]; then
    echo "ERROR: expected $WASM_GRAPH_COMPONENT after build" >&2
    exit 1
fi
echo "    wasm-graph component size: $(du -h "$WASM_GRAPH_COMPONENT" | cut -f1)"
echo "    (GraphCompute Personalized PageRank via the host-graph interface)"

echo "==> Building example-wasm-scratch (wasm32-wasip2)"
(
    cd examples/example-wasm-scratch
    cargo build --target wasm32-wasip2 --release
)

WASM_SCRATCH_COMPONENT="examples/example-wasm-scratch/target/wasm32-wasip2/release/example_wasm_scratch.wasm"
if [[ ! -f "$WASM_SCRATCH_COMPONENT" ]]; then
    echo "ERROR: expected $WASM_SCRATCH_COMPONENT after build" >&2
    exit 1
fi
echo "    wasm-scratch component size: $(du -h "$WASM_SCRATCH_COMPONENT" | cut -f1)"
echo "    (Mode B-seq mutable scratch graph via the host-graph interface)"

echo "==> Done. Run tests with:"
echo "    cargo nextest run -p uni-plugin-extism --test example_extism_geo_e2e"
echo "    cargo nextest run -p uni-plugin-extism --test example_extism_net_e2e"
echo "    cargo nextest run -p uni-plugin-wasm   --test example_wasm_geo_e2e"
echo "    cargo nextest run -p uni-plugin-wasm   --test example_wasm_net_e2e"
