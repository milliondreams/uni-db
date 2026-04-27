# Python wheel matrix for `uni-db`

**Status:** Draft
**Created:** 2026-04-26
**Audience:** anyone deciding which `uni-db` wheel to install, or working on the publish pipeline
**Scope:** the maturin-built Python package at `bindings/uni-db/` (PyPI name `uni-db`, import `uni_db`)

---

## Context

Pip cannot pick compile-time Cargo features at install time — wheels are pre-compiled binaries. Today the `uni-db` wheel is built once with the cargo defaults (`lance-backend`, `provider-gemini`, `provider-openai`) and ships the same content to every user. ONNX, mistralrs, candle, fastembed, and all GPU features are unreachable from Python.

The ML ecosystem has converged on a few patterns for shipping cargo-feature-style variants through Python:

1. **Multiple PyPI package names** (`tensorflow` vs `tensorflow-cpu`, `onnxruntime` vs `onnxruntime-gpu`).
2. **Custom index per variant** (PyTorch — `--index-url https://download.pytorch.org/whl/cu124`).
3. **Extras that trigger source builds** (only works for users with full toolchains).

For `uni-db` we adopt **(1)**: a small set of separately-named wheels along the hardware axis, each with the same Python API and the same set of providers compiled in. The user picks one based on their platform and accelerator.

---

## The matrix

| PyPI package | Wheel platforms | Cargo features (Maturin `--features`) | Use case |
|---|---|---|---|
| **`uni-db`** | manylinux2014-x86_64, win-x86_64, macos-x86_64, macos-arm64 | `lance-backend`, all `provider-*` (incl. `provider-onnx`, `provider-mistralrs`, `provider-fastembed`, `provider-candle`) | Default. CPU-only, works everywhere. |
| **`uni-db-cuda`** | manylinux2014-x86_64, win-x86_64 | same as above, swapping `provider-onnx` for `gpu-cuda` | NVIDIA users. ORT CUDA EP + candle/mistralrs CUDA kernels. |
| **`uni-db-metal`** | macos-arm64 | same as above, plus `gpu-coreml` and `gpu-metal` | Apple Silicon users. ORT CoreML EP for ONNX models + candle/mistralrs Metal kernels. |

The Python API and the model-catalog spec format are identical across all three wheels. A script that says

```python
spec = {
    "alias": "rerank/minilm",
    "task": "rerank",
    "provider_id": "local/onnx",
    "model_id": "cross-encoder/ms-marco-MiniLM-L6-v2",
    "options": {"execution_providers": ["cuda", "cpu"]},
}
```

works unchanged across `uni-db` (would fall back to `cpu` since `cuda` isn't compiled in — actually returns a `CapabilityMismatch` because the `cuda` EP isn't built; see open question #1) and `uni-db-cuda` (uses `cuda`, falls back to `cpu` if the GPU isn't available at runtime).

---

## Per-wheel breakdown

### `uni-db` (CPU)

**Cargo features:** `provider-onnx`, `provider-fastembed`, `provider-mistralrs`, `provider-candle`, `provider-openai`, `provider-gemini`, `provider-vertexai`, `provider-mistral`, `provider-anthropic`, `provider-voyageai`, `provider-cohere`, `provider-azure-openai`, `lance-backend`.

**What's bundled:**
- `_uni_db.so` (or `.pyd` / `.dylib`) — single cdylib containing all uni-db / uni-locy / uni-store / uni-xervo Rust code, plus statically linked ORT (`libonnxruntime.a`, ~70 MB after strip).
- mistralrs + candle-core / candle-nn / candle-transformers compiled in CPU mode (Accelerate on macOS, OpenBLAS or pure-Rust SIMD on Linux/Windows).

**Approximate compressed wheel size:** 120–180 MB on Linux x86_64. Smaller on macOS arm64.

**Host requirements:** none. Self-contained.

### `uni-db-cuda`

**Cargo features:** all of the above except `provider-onnx`, plus `gpu-cuda`. The `provider-onnx` and `gpu-cuda` features are mutually exclusive (enforced by uni-xervo's `build.rs` since `0.6.0`); `gpu-cuda` activates the dynamic-linkage path that loads ORT EP sidecars rather than statically linking ORT.

**What's bundled:**
- `_uni_db.so` — main cdylib, smaller than the CPU variant because ORT is no longer statically linked into it.
- `libonnxruntime.so.X` (~50 MB) — ORT's main shared lib, fetched at build time by `ort/download-binaries`.
- `libonnxruntime_providers_cuda.so` (~180 MB) and `libonnxruntime_providers_shared.so` (~10 MB) — CUDA EP sidecars, fetched at build time by `ort/download-binaries`, copied into `target/<profile>/` by `ort/copy-dylibs`. Maturin's `[tool.maturin] include` field needs to pull these into the wheel alongside `_uni_db.so`. They're loaded lazily via `dlopen` at first GPU use.
- candle-cuda kernels — PTX-compiled for the supported SM targets, baked into the candle-kernels rlib at Rust build time (~20–30 MB).

**Approximate compressed wheel size:** 330–450 MB on Linux x86_64.

**Host requirements:**
- NVIDIA driver supporting the CUDA toolkit version that PTX was compiled with (currently CUDA 13.0 → driver 580.x; if the build moves to 13.1, requires driver 580.142+ or 585.x).
- cuDNN ≥ 9 on the loader path (typically `/usr/local/cuda-X.X/targets/<arch>/lib/`).
- `libcudart` — typically comes with the NVIDIA toolkit install; not bundled.
- These are NOT bundled in the wheel (combined ~700 MB; licensing concerns). Same convention as `onnxruntime-gpu`.

**Failure mode if host deps missing:** clear error at first model load — `libcudnn.so.9: cannot open shared object file`. Should be caught by a Python-side post-install probe (see open question #4).

**CI notes:**
- Linux build needs CUDA 13.x toolkit in the manylinux container. Custom base image likely required.
- candle's CUDA kernels go through `nvcc` for every supported SM target → CI build time **~15–25 minutes** end-to-end for one Linux x86_64 wheel.
- `auditwheel repair --include-libs` on Linux to bundle the `libonnxruntime_providers_*.so` files into `uni_db.libs/` with correct rpath. `delvewheel` equivalent on Windows.

### `uni-db-metal`

**Cargo features:** all CPU providers + `gpu-coreml` (ORT CoreML EP) + `gpu-metal` (candle/mistralrs Metal kernels).

The two GPU features are **orthogonal**: `gpu-coreml` accelerates ONNX models, `gpu-metal` accelerates candle/mistralrs models. Both make sense on Apple Silicon and they don't conflict.

**What's bundled:**
- `_uni_db.dylib` (renamed `.so` for Python).
- ORT statically linked (CoreML EP is built into the same lib in pyke's macOS bundle — no separate sidecar).
- candle Metal kernels — `.metallib` baked into the binary at build time (~5–10 MB).

**Approximate compressed wheel size:** 150–200 MB on macOS arm64.

**Host requirements:**
- macOS with Metal framework (every supported macOS already has it).
- macOS arm64 only — the x86_64 Apple platform isn't a target because the Metal/CoreML stacks aren't worth supporting separately for retired hardware.

---

## What is **not** bundled in any wheel

| Component | Why not bundled |
|---|---|
| **cuDNN ≥ 9** | ~600 MB, NVIDIA licensing. Convention is "system cuDNN; set `LD_LIBRARY_PATH` if needed." |
| **CUDA runtime** (`libcudart.so.X`) | Comes with the NVIDIA driver/toolkit install. Bundling the wrong minor breaks compatibility. |
| **NVIDIA driver + `libcuda.so.1`** | Kernel-mode component; never bundled in any user-space artifact. |
| **Apple Metal framework** | macOS system component; always present on supported macOS. |
| **MKL** (for max-perf x86 BLAS) | If `candle/mkl` is enabled, the lib is system-installed. We default to OpenBLAS or pure-Rust SIMD. |
| **Models** (HuggingFace weights) | All providers download to a configurable cache (`~/.uni_cache/...` by default) on first use. Wheels never contain model weights. |

---

## Build sketch

For each wheel, maturin produces one binary using a directory-per-package layout (the same pattern Microsoft uses for `onnxruntime` vs `onnxruntime-gpu`):

```
bindings/
  uni-db/                    # uni_db (CPU)
    pyproject.toml           # name = "uni-db"
    Cargo.toml               # _uni_db cdylib, default features
  uni-db-cuda/               # uni_db on CUDA hosts
    pyproject.toml           # name = "uni-db-cuda"
    Cargo.toml               # _uni_db cdylib, gpu-cuda feature
    src/                     # symlink to ../uni-db/src
  uni-db-metal/              # uni_db on Apple Silicon
    pyproject.toml           # name = "uni-db-metal"
    Cargo.toml               # _uni_db cdylib, gpu-coreml + gpu-metal features
    src/                     # symlink to ../uni-db/src
```

All three sibling Cargo crates compile the same `_uni_db` cdylib from the same source, just with different cargo features. Each ships a wheel with a different PyPI name but the same Python module name (`uni_db`), so user code is portable across them.

CI: `cibuildwheel` (or `maturin-action` with platform matrices) drives one job per `(package, platform, python_version)` cell. Only sensible cells are built — `uni-db-cuda` is not built for macOS, `uni-db-metal` is not built for Linux, etc.

---

## Open questions

1. **EP fallback semantics on the CPU wheel.** When a script with `execution_providers: ["cuda", "cpu"]` runs on a wheel built without `gpu-cuda`, today uni-xervo returns a `CapabilityMismatch` error citing "CUDA requested without `gpu-cuda` enabled". Is that the right UX for a Python user who just wants their script to work cross-platform? Possibilities: (a) keep strict, document loudly, (b) have the CPU wheel silently filter unsupported EPs from the list before passing to ORT, falling back to whatever's available. (b) is more user-friendly but hides misconfigurations.

2. **`auto` execution-provider keyword.** Many ORT-based Python libraries accept `execution_providers: "auto"` meaning "pick the best one this build supports." A natural extension: the catalog spec interprets `"auto"` as "the first EP from this build's bundled list that loads successfully." Less coupling between the script and the wheel variant.

3. **Where to host wheels.** Initially PyPI for all three. If wheel sizes hit PyPI limits (currently 100 MB per file, with exemptions on request), the CUDA wheel may need to live on a custom index or split across multiple `bdist_wheel` files. PyTorch went the custom-index route for this reason.

4. **Post-install runtime probe.** A small `uni_db._probe()` Python function that the wheel can run post-install (or that a CLI can call) to verify host deps:
   - For `uni-db-cuda`: dlopen `libcudnn.so.9` and `libcudart.so` and report their versions; check the NVIDIA driver supports the toolkit version.
   - For `uni-db-metal`: report Metal framework presence (always true on supported macOS but a sanity check).
   - For `uni-db` (CPU): report ORT version, optional MKL/Accelerate.

   Should print actionable messages if any required dependency is missing — e.g. *"`uni-db-cuda` requires cuDNN ≥ 9 on the loader path. Install via `dnf install cuda-cudnn` (Fedora) / `apt install libcudnn9` (Ubuntu) / etc., then re-run."*

5. **mistralrs feature subset on the CPU wheel.** mistralrs's CPU path is functional but slow for real LLMs. Is it worth shipping mistralrs in the CPU wheel at all, or should it only appear in the GPU variants? Tradeoff: smaller CPU wheel vs. portable script (same `provider-mistralrs` works on every wheel even if slow).

6. **Versioning across wheels.** When `uni-db` bumps from 1.1.1 to 1.2.0, do `uni-db-cuda` and `uni-db-metal` follow lock-step? If yes (recommended), the publish pipeline must release all three before announcing the version. If asymmetric, users can end up with mismatched API versions across packages — fragile. Lock-step adds release coordination but is the only honest model.

7. **Do we add `uni-db-rocm`, `uni-db-directml`?** Hardware variants for AMD and Windows. Mechanical to add once the 3-wheel pattern is in place; defer until users ask. Same for `uni-db-onnx-dynamic` (BYO ORT, useful for RHEL container images that ship their own ORT).

---

## Sequencing

1. **Add cargo feature passthroughs to `bindings/uni-db/Cargo.toml`** mirroring `crates/uni/Cargo.toml`'s feature list. Pure plumbing — no behavior change.
2. **Stand up the CPU wheel CI** with the full provider feature list. Verify it builds for all four target platforms (Linux x86_64, win x86_64, macOS x86_64, macOS arm64). Publish as `uni-db` 1.2.0 (or whichever version makes sense).
3. **Add the `uni-db-cuda` sibling crate** under `bindings/uni-db-cuda/`. Build CI on a Linux + CUDA 13.0 image. Verify auditwheel correctly bundles the EP sidecars. Publish.
4. **Add the `uni-db-metal` sibling crate** under `bindings/uni-db-metal/`. Build CI on a macOS arm64 runner. Publish.
5. **Implement the post-install probe** (`uni_db._probe()` + CLI `uni-db check`) once at least two wheels exist, so the diagnostic message can guide users between them.

Each step is independent and shippable on its own — start with the CPU wheel, GPU wheels follow once the basic pipeline is proven.

---

## What this does *not* solve

- **Source builds for users with custom hardware** (e.g. Jetson, embedded ARM with NVIDIA GPU). Those users build from source. The provided wheels target the common cases.
- **Multi-version cuDNN matrix.** We pin one cuDNN major version per CUDA wheel release. Users who need cuDNN 8 stay on an older `uni-db-cuda` release. Same model as `onnxruntime-gpu`.
- **Single-import "auto-detect" wheel.** A wheel that detects at import-time whether the host has CUDA and switches behavior accordingly is technically possible but would require shipping all kernels in one wheel (~700 MB). Not worth it.
