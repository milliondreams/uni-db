# Local CI Runbook — replicating the full GitHub CI locally

This document lists every CI job from `.github/workflows/pr.yml` and `.github/workflows/ci.yml`
with the **exact command** to run it locally, plus prerequisites, ordering, and the local-only
gotchas that bite.

> **Source of truth = the workflow YAML.** This runbook mirrors the workflows as of 2026-06-29.
> If a command here disagrees with `.github/workflows/{pr,ci}.yml`, the YAML wins — update this doc.
> Run commands from the repo root unless a "working dir" is noted. **Do not substitute workarounds**
> for the documented commands; if a command fails, that is a real signal — report it.

## 0. Prerequisites (one-time)

```bash
# Rust toolchain targets used by the wasm plugin fixtures
rustup target add wasm32-unknown-unknown wasm32-wasip2

# Test runner + wasm tooling
cargo install cargo-nextest --locked
cargo install wasm-tools --locked

# Python tooling (bindings)
#   install uv:  https://docs.astral.sh/uv/   (CI uses python 3.12)

# System deps CI installs (Debian/Ubuntu names; install equivalents on Fedora)
#   mold, protobuf-compiler

# Docker — only needed for the Cloud/LocalStack job
```

Network access is required for: HuggingFace model pulls (reranker real-ONNX tests), the ONNX Runtime
tarball (reranker load-dynamic), and the LocalStack image (cloud).

### Environment normalization

```bash
# CI runs with NO rustc wrapper. If you have a global sccache/RUSTC_WRAPPER configured locally,
# unset it for every cargo/maturin command or the build can fail. Prefix commands with:
export RUSTC_WRAPPER=""
```

### Ordering / contention

- **All `cargo`/`maturin` commands serialize on the build-dir lock** — run them one at a time.
- Docker (LocalStack) and pure-Python (`uv`) steps do **not** contend with a cargo build, so they can
  warm up in parallel (e.g. start LocalStack while a build runs).
- Heavy builds (provider-onnx static link, release wheel) are best run in the background while you
  watch a log.

---

## 1. Quick path — a Rust-only change

The jobs a Rust change can actually move. Run these first:

```bash
export RUSTC_WRAPPER=""
cargo fmt --all -- --check
cargo clippy --workspace \
  --exclude uni-tck --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal \
  --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace \
  --exclude uni-python --exclude uni-tck --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
./scripts/build-wasm-fixtures.sh
cargo nextest run --workspace \
  --exclude uni-tck --exclude uni-python --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
```

The full job list follows.

---

## 2. `pr.yml` — PR checks

### Lint
```bash
cargo fmt --all -- --check
cargo clippy --workspace \
  --exclude uni-tck --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal \
  --all-targets -- -D warnings
```

### Rust Tests (workspace suite)
```bash
./scripts/build-wasm-fixtures.sh      # builds the geo/net example wasm plugin fixtures first
cargo nextest run --workspace \
  --exclude uni-tck --exclude uni-python --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
```

### Concurrency Model Check (loom smoke)
```bash
# MUST set the preemption bound, or the exhaustive search blows past the nextest timeout.
LOOM_MAX_PREEMPTIONS=2 cargo nextest run -p uni-store --features loom --test occ_model
```

### Metamorphic Query Oracles (smoke)
```bash
METAMORPHIC_CASES=64 cargo nextest run -p uni-db --test integration \
  -E 'test(/metamorphic::/) and not test(soak)'
```

### openCypher TCK (schemaless)
```bash
cargo nextest run -p uni-tck --test tck
```

### Python Tests
```bash
# uni-db  (working dir: bindings/uni-db)
( cd bindings/uni-db
  uv sync --group dev
  uv run maturin develop
  uv run ruff format --check .
  uv run ruff check .
  uv run pytest tests/ -v -n auto )

# uni-pydantic  (working dir: bindings/uni-pydantic) — imports the uni-db .so via editable path dep
( cd bindings/uni-pydantic
  uv sync --group dev
  uv run ruff format --check .
  uv run ruff check .
  uv run pytest tests/ -v -n auto )
```

---

## 3. `ci.yml` — main-push thorough suite (everything above, plus)

### TCK sidecar + Locy TCK (both lanes)
```bash
UNI_TCK_SCHEMA_MODE=sidecar cargo nextest run -p uni-tck --test tck
cargo nextest run -p uni-locy-tck --test locy_tck
UNI_LOCY_TCK_SCHEMA_MODE=sidecar cargo nextest run -p uni-locy-tck --test locy_tck
```

### Reranker Integration (ONNX)
```bash
# Bundled CPU ONNX (statically links libonnxruntime.a; pulls real models from HF).
# One test is filtered out — its model is only served via an unsupported xet-bridge redirect.
cargo nextest run -p uni-db --features provider-onnx --test reranker_integration --run-ignored all \
  -E 'not test(=test_real_onnx_cross_encoder_reranks_by_relevance)'

# Load-dynamic ONNX — needs the ORT shared lib at runtime; --no-default-features is required
# (default `provider-onnx` and `provider-onnx-dynamic` are mutually exclusive at the `ort` level).
curl -sSL -o /tmp/ort.tgz \
  https://github.com/microsoft/onnxruntime/releases/download/v1.20.1/onnxruntime-linux-x64-1.20.1.tgz
tar xzf /tmp/ort.tgz -C /tmp
export ORT_DYLIB_PATH=/tmp/onnxruntime-linux-x64-1.20.1/lib/libonnxruntime.so
cargo nextest run -p uni-db --no-default-features --features provider-onnx-dynamic \
  --test reranker_integration
unset ORT_DYLIB_PATH
```

### Cloud Integration (LocalStack)
```bash
docker run -d --name uni-localstack -p 4566:4566 \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 \
  localstack/localstack:4.13.1
timeout 120 bash -c 'until curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; do sleep 2; done'

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ALLOW_HTTP=true \
  cargo nextest run -p uni-store --test integration --run-ignored all \
    -E 'test(/^cloud_integration_test::/)'
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ALLOW_HTTP=true \
  cargo nextest run -p uni-db --test integration --run-ignored all \
    -E 'test(/^hybrid_localstack_e2e::/)'

docker rm -f uni-localstack          # teardown
```

### Documentation
```bash
# Generated-notebook freshness (no compile)
python3 website/scripts/generate_locy_notebooks.py --check
python3 website/scripts/generate_semiconductor_flagship_notebook.py --check
python3 website/scripts/generate_pharma_flagship_notebook.py --check
python3 website/scripts/generate_cyber_flagship_notebook.py --check

# rustdoc gate
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace \
  --exclude uni-python --exclude uni-tck --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
```

### Flagship Notebooks (heaviest; release wheel + neural execution)
```bash
( cd bindings/uni-db
  uv sync --group dev --extra notebook-runtime
  uv run maturin build --out dist          # RELEASE wheel — long build
  uv pip install --force-reinstall dist/*.whl )

# Run the 6 notebooks SERIALLY (they fail spuriously under concurrent CPU/GIL load).
for nb in semiconductor pharma cyber predictive_maintenance adverse_drug_reaction drug_drug_interaction; do
  uv run --project bindings/uni-db python website/scripts/verify_${nb}_flagship_notebook.py
done
```

### Release Guards
```bash
python3 scripts/ci/check_wheel_variant_features.py
python3 scripts/ci/check_publish_list.py
cargo check -p uni-python-onnx          # slim default-features=false wheel compile guard
```

### CUDA wheel-graph smoke
```bash
cargo metadata --format-version=1 --manifest-path bindings/uni-db-cuda/Cargo.toml > /dev/null
```

---

## 4. Not run locally

- **`gate`** (`ci.yml`) — an aggregator that just depends on the jobs above; nothing to execute.
- **`release-wheels.yml`, `deploy-docs.yml`, `publish-pydantic.yml`** — tag/push-triggered artifact
  publishing; no local validation value.

---

## 5. Local-only gotchas

- **`RUSTC_WRAPPER=""`** — see §0. Unset any global wrapper for cargo/maturin.
- **loom timeout** — always pass `LOOM_MAX_PREEMPTIONS=2`; without it the exhaustive model runs past
  the nextest `terminate-after` and reports a false TIMEOUT.
- **Python static-TLS (glibc)** — on some boxes `uv run pytest` can fail with
  `ImportError: ... cannot allocate memory in static TLS block` (a large debug `.so` exhausting
  glibc's static-TLS surplus; CI runners have more surplus so they never hit it). If it triggers,
  preload the built lib via the venv interpreter directly — do **not** `export LD_PRELOAD` globally
  (it poisons `uv`/non-python subprocesses):
  ```bash
  SO=bindings/uni-db/uni_db/_uni_db.abi3.so
  PY=bindings/uni-db/.venv/bin/python3
  LD_PRELOAD="$SO" "$PY" -m pytest tests/ -v -n auto
  ```
  This is a last-resort local fix, not part of the workflow.
- **Notebooks** — run serially (see §3); concurrent runs fail spuriously, not from a code bug.
- **Cloud** — always `docker rm -f uni-localstack` when done so the port/container doesn't linger.
- **Reranker real-ONNX tests** need network (HF). A flaky download is an infra failure, not a code
  failure — re-run before concluding.
