# Installation

This guide covers all methods for installing Uni.

## System Requirements

| Component | Requirement |
|-----------|-------------|
| **OS** | Linux (x86_64, aarch64), macOS (x86_64, Apple Silicon) |
| **Memory** | 4 GB RAM minimum, 16 GB recommended for large graphs |
| **Disk** | SSD recommended for optimal performance |

---

## Rust Library

Add `uni-db` to your project from [crates.io](https://crates.io/crates/uni-db):

```bash
cargo add uni-db
```

Or add it to your `Cargo.toml` directly:

```toml
[dependencies]
uni-db = "*"
```

---

## Python Package

Install from [PyPI](https://pypi.org/project/uni-db/):

```bash
pip install uni-db
```

For the Pydantic OGM layer:

```bash
pip install uni-pydantic
```

---

## CLI (Build from Source)

The CLI is not published as a standalone binary. Build it from source:

### Step 1: Install Rust and System Dependencies

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**System dependencies** (needed for Lance native code):

=== "Ubuntu / Debian"

    ```bash
    sudo apt update
    sudo apt install -y build-essential pkg-config libssl-dev protobuf-compiler clang llvm
    ```

=== "Fedora / RHEL"

    ```bash
    sudo dnf install -y gcc gcc-c++ pkg-config openssl-devel protobuf-compiler clang llvm
    ```

=== "macOS"

    ```bash
    brew install protobuf llvm pkg-config openssl@3
    # Apple Silicon: add LLVM to PATH
    echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
    source ~/.zshrc
    ```

=== "Arch Linux"

    ```bash
    sudo pacman -S base-devel pkg-config openssl protobuf clang llvm
    ```

### Step 2: Clone and Build

```bash
git clone https://github.com/rustic-ai/uni-db.git
cd uni
cargo build --release
```

### Step 3: Install to PATH (Optional)

```bash
# Option A: Copy to /usr/local/bin
sudo cp target/release/uni /usr/local/bin/

# Option B: Use cargo install
cargo install --path crates/uni-cli
```

---

## Verification

After installation, verify Uni is working correctly:

### Check Version

```bash
uni --version
# Output: uni 1.0.0
```

### Display Help

```bash
uni --help
```

Expected output:
```
Uni - Reasoning and Memory Infrastructure for Intelligent Systems

Usage: uni <COMMAND>

Commands:
  import    Import data from JSONL
  query     Execute a Cypher query
  repl      Start the interactive REPL
  snapshot  Manage snapshots
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### Run a Simple Query

```bash
# Create a test directory
mkdir -p /tmp/uni-test

# Run a query (will create empty storage)
uni query "RETURN 1 + 1 AS result" --path /tmp/uni-test

# Expected output:
# ┌────────┐
# │ result │
# ├────────┤
# │ 2      │
# └────────┘
```

---

## Troubleshooting Installation

### Common Issues

#### "protoc not found"

```bash
# Ubuntu/Debian
sudo apt install protobuf-compiler

# macOS
brew install protobuf

# Verify
protoc --version
```

#### "failed to run custom build command for `ring`"

This usually indicates missing C compiler or LLVM:

```bash
# Ubuntu/Debian
sudo apt install build-essential clang

# macOS
xcode-select --install
```

#### "openssl not found"

```bash
# Ubuntu/Debian
sudo apt install libssl-dev pkg-config

# macOS
brew install openssl@3
export OPENSSL_DIR=$(brew --prefix openssl@3)
```

#### Slow Compilation

Enable parallel compilation and use the `mold` linker:

```bash
# Install mold (Linux)
sudo apt install mold

# Configure Cargo to use mold
cat >> ~/.cargo/config.toml << EOF
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
EOF

# Rebuild
cargo build --release
```

---

## Feature Flags

Uni supports optional features that can be enabled during compilation:

| Feature | Description | Default |
|---------|-------------|---------|
| `candle-text` | Native Rust embedding models (Candle) | Enabled |
| `fastembed` | ONNX-based embedding models (legacy) | Disabled |
| `s3` | Amazon S3 object store | Enabled |
| `gcs` | Google Cloud Storage | Disabled |
| `azure` | Azure Blob Storage | Disabled |

### Custom Build Example

```bash
# Minimal build (local filesystem only)
cargo build --release --no-default-features

# Build with all cloud providers
cargo build --release --features "s3,gcs,azure"

# Build without embedding support (smaller binary)
cargo build --release --no-default-features --features "s3"
```

---

## Development Setup

For contributing to Uni, set up the full development environment:

```bash
# Clone with submodules
git clone --recursive https://github.com/rustic-ai/uni-db.git
cd uni

# Install development tools
cargo install cargo-watch cargo-nextest

# Run tests
cargo nextest run

# Run with hot-reload during development
cargo watch -x "run -- query 'RETURN 1'"

# Check code quality
cargo fmt --check
cargo clippy -- -D warnings
```

---

## Next Steps

Now that Uni is installed:

1. **[Quick Start](quickstart.md)** — Import data and run your first queries
2. **[CLI Reference](cli-reference.md)** — Learn all available commands
3. **[Data Model](../concepts/data-model.md)** — Understand vertices, edges, and properties
