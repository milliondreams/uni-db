#!/usr/bin/env bash
# Build a manylinux_2_28 wheel locally, replicating the CI release build.
#
# Usage:
#   scripts/build-manylinux-wheel.sh             # x86_64 (default)
#   scripts/build-manylinux-wheel.sh aarch64     # cross-compile (needs QEMU)
#
# Output: dist/uni_db-*.whl (manylinux_2_28 tagged)
set -euo pipefail

ARCH="${1:-x86_64}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$REPO_ROOT/dist"
MATURIN_VERSION="1.12.4"
PROTOC_VERSION="28.3"

IMAGE="quay.io/pypa/manylinux_2_28_${ARCH}"

# :z relabels the volume for SELinux — only needed when SELinux is active
if command -v getenforce >/dev/null 2>&1 && [ "$(getenforce)" != "Disabled" ]; then
  Z=":z"
else
  Z=""
fi

mkdir -p "$DIST_DIR"

echo "==> Pulling ${IMAGE}..."
docker pull "$IMAGE"

echo "==> Building manylinux_2_28 wheel (${ARCH})..."
docker run --rm \
  -v "$REPO_ROOT:/io${Z}" \
  -v "$HOME/.cargo/registry:/root/.cargo/registry${Z}" \
  -v "$HOME/.cargo/git:/root/.cargo/git${Z}" \
  -w /io \
  -e RUST_MIN_STACK=8388608 \
  -e RUSTFLAGS="" \
  -e MATURIN_VERSION="$MATURIN_VERSION" \
  -e PROTOC_VERSION="$PROTOC_VERSION" \
  -e BUILD_ARCH="$ARCH" \
  "$IMAGE" \
  bash -c '
    set -euo pipefail

    echo "--- Installing system dependencies..."
    yum install -y openssl-devel unzip

    echo "--- Installing protoc ${PROTOC_VERSION} (yum version is too old: 3.5.0)..."
    if [ "$(uname -m)" = "aarch64" ]; then
      PROTOC_ARCH="linux-aarch_64"
    else
      PROTOC_ARCH="linux-x86_64"
    fi
    curl -sSLO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip"
    unzip -q "protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip" -d /usr/local
    rm "protoc-${PROTOC_VERSION}-${PROTOC_ARCH}.zip"
    protoc --version

    echo "--- Installing Rust (stable)..."
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain stable --profile minimal
    source "$HOME/.cargo/env"

    echo "--- Installing maturin ${MATURIN_VERSION}..."
    curl -sSL \
      "https://github.com/PyO3/maturin/releases/download/v${MATURIN_VERSION}/maturin-${BUILD_ARCH}-unknown-linux-musl.tar.gz" \
      | tar -xz -C /usr/local/bin
    maturin --version

    echo "--- Building wheel..."
    maturin build --release --out /io/dist -m /io/bindings/uni-db/Cargo.toml

    echo "--- Wheel contents:"
    ls -lh /io/dist/
  '

echo ""
echo "Done. Built wheel(s):"
ls -lh "$DIST_DIR"/*.whl
