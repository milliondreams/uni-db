# Python wheel matrix for `uni-db`

> **Historical record (preserved 2026-04-29).** This document describes the
> 12-wheel matrix designed against uni-xervo 0.6.x's feature surface. That
> design was superseded by the 6-wheel collapse in `uni-db 1.2.0`, driven
> by uni-xervo 0.9.0's three-axis model (provider × linking × acceleration).
> See `docs/migrations/0.9.0-wheel-matrix-collapse.md` for the current
> design, the deletion rationale, and the upgrade mapping for users on the
> old wheels (`uni-db-fastembed*`, `uni-db-mistralrs*`, `uni-db-all*`).
>
> The body below remains as it was authored — useful as context for *why*
> the original 12 variants were chosen, but no longer authoritative. Do not
> refer to this document when picking a wheel today.

**Status:** Superseded (was Draft)
**Created:** 2026-04-26
**Superseded:** 2026-04-29 by `docs/migrations/0.9.0-wheel-matrix-collapse.md`
**Audience:** anyone deciding which `uni-db` wheel to install, or working on the publish pipeline
**Scope:** the maturin-built Python package at `bindings/uni-db/` (PyPI name `uni-db`, import `uni_db`)

---

## Context

Pip cannot pick compile-time Cargo features at install time — wheels are pre-compiled binaries. Today the `uni-db` wheel is built once with the cargo defaults (`lance-backend`, `provider-gemini`, `provider-openai`) and ships the same content to every user. ONNX, mistralrs, candle, fastembed, and all GPU features are unreachable from Python.

The ML ecosystem has converged on a few patterns for shipping cargo-feature-style variants through Python:

1. **Multiple PyPI package names** (`tensorflow` vs `tensorflow-cpu`, `onnxruntime` vs `onnxruntime-gpu`).
2. **Custom index per variant** (PyTorch — `--index-url https://download.pytorch.org/whl/cu124`).
3. **Extras that trigger source builds** (only works for users with full toolchains).

For `uni-db` we adopt **(1)**: a small set of separately-named wheels along *two* axes — provider stack and hardware accelerator — each with the same Python API. The user picks one wheel based on which providers they need and which accelerator their host has.

---

## The two axes

**Provider axis** — which model backends are compiled in:

| Variant | What it adds (vs base) | Why someone wants this |
|---|---|---|
| **base** (`uni-db`) | nothing — only `lance-backend` + remote providers (`openai`, `gemini`, `vertexai`, `mistral`, `anthropic`, `voyageai`, `cohere`, `azure-openai`) | Hosted-only workflows; smallest wheel; no native ML stack at all |
| **`-onnx`** | `provider-onnx` (raw ONNX execution + cross-encoder rerank) | Local rerankers via ONNX; bring-your-own-model raw tensor execution |
| **`-fastembed`** | `provider-fastembed` (which transitively pulls ORT, so it includes ONNX too — `provider-fastembed` requires an ORT linking mode per `uni-xervo` `build.rs`) | Local embeddings (BGE, E5, Jina, …) via fastembed-rs's curated registry, *plus* everything `-onnx` gives you |
| **`-mistralrs`** | `provider-mistralrs` (LLM gen, vision, diffusion, speech) + `provider-candle` (BERT-family local embeddings) | Self-hosted LLMs and multimodal generation. No ONNX. |
| **`-all`** | everything (all of the above + remote providers) | One-stop wheel. Largest, but no need to think about what to install. |

**Hardware axis** — which kernels are precompiled:

| Suffix | Cargo feature(s) | Hardware target |
|---|---|---|
| (none) | `provider-onnx` if ONNX is in the wheel; otherwise CPU defaults of each provider | CPU. Works everywhere. |
| **`-cuda`** | `gpu-cuda` (replaces `provider-onnx`; activates ORT dynamic-linkage CUDA EP and candle CUDA kernels) | NVIDIA, Linux + Windows |
| **`-metal`** | `gpu-coreml` for ORT-using wheels, `gpu-metal` for candle/mistralrs-using wheels, both for `-all-metal` | Apple Silicon |

Some hardware suffixes don't make sense for some provider variants — the base `uni-db` only has remote providers, so a `uni-db-cuda` wheel would have no kernels to compile. Those cells are dropped.

---

## The matrix

| | CPU (none) | `-cuda` | `-metal` |
|---|---|---|---|
| **`uni-db`** (base) | `uni-db` | — (no GPU consumers) | — (no GPU consumers) |
| **`uni-db-onnx`** | `uni-db-onnx` | `uni-db-onnx-cuda` | `uni-db-onnx-metal` (CoreML) |
| **`uni-db-fastembed`** (incl. ONNX) | `uni-db-fastembed` | `uni-db-fastembed-cuda` | `uni-db-fastembed-metal` (CoreML) |
| **`uni-db-mistralrs`** (incl. candle) | `uni-db-mistralrs` | `uni-db-mistralrs-cuda` | `uni-db-mistralrs-metal` (Metal) |
| **`uni-db-all`** | `uni-db-all` | `uni-db-all-cuda` | `uni-db-all-metal` (CoreML + Metal) |

**13 wheel-package names total.** The Python API and model-catalog spec format are identical across all of them. A script that says

```python
spec = {
    "alias": "rerank/minilm",
    "task": "rerank",
    "provider_id": "local/onnx",
    "model_id": "cross-encoder/ms-marco-MiniLM-L6-v2",
    "options": {"execution_providers": ["cuda", "cpu"]},
}
```

works on `uni-db-onnx-cuda`, `uni-db-fastembed-cuda`, and `uni-db-all-cuda` (uses CUDA EP, falls back to CPU at runtime if the GPU isn't available). On the CPU variants of those same packages the `cuda` EP isn't compiled in — see [open question #1](#open-questions) for whether that should be a silent filter or a loud error.

---

## Per-wheel breakdown

The breakdown below describes what's bundled, the approximate compressed wheel size, and the host requirements. Wheel-size estimates assume Linux x86_64; macOS arm64 is typically 10–30% smaller because Apple's compiler emits more compact code and ORT's CoreML path doesn't ship sidecar `.so` files.

### CPU row — no `nvcc`, no Metal kernels at build time

#### `uni-db` (base)

- **Cargo features:** `lance-backend`, `provider-openai`, `provider-gemini`, `provider-vertexai`, `provider-mistral`, `provider-anthropic`, `provider-voyageai`, `provider-cohere`, `provider-azure-openai`.
- **Bundled:** the cdylib only. No native ML stack. Just HTTP clients.
- **Approx size:** 25–40 MB.
- **Host reqs:** none.
- **Use case:** hosted-only deployments; minimal-footprint installs; CI runners where local model inference isn't needed.

#### `uni-db-onnx`

- **Cargo features:** base + `provider-onnx` (CPU bundled ORT, static linkage).
- **Bundled:** cdylib with ORT statically linked (`libonnxruntime.a` ~70 MB after strip baked into the .so).
- **Approx size:** 90–120 MB.
- **Host reqs:** none.
- **Use case:** local cross-encoder reranking and raw ONNX tensor execution. No local embeddings, no local LLMs.

#### `uni-db-fastembed`

- **Cargo features:** base + `provider-fastembed` + `provider-onnx` (fastembed *requires* an ORT linking mode, enforced by `uni-xervo`'s `build.rs` since `0.6.1`).
- **Bundled:** cdylib with ORT statically linked (shared between fastembed and the ONNX raw/rerank tasks — single static lib, no double-counting).
- **Approx size:** 95–125 MB. Marginal increase over `uni-db-onnx` because fastembed itself is a thin wrapper plus a model registry.
- **Host reqs:** none.
- **Use case:** local embeddings (BGE/E5/Jina/Nomic/…) *and* local rerankers, all via the same ORT static lib. The most common deployment shape for hybrid retrieval.

#### `uni-db-mistralrs`

- **Cargo features:** base + `provider-mistralrs` + `provider-candle`.
- **Bundled:** cdylib with mistralrs + candle-core/nn/transformers compiled in CPU mode (Accelerate on macOS, OpenBLAS or pure-Rust SIMD on Linux/Windows). No ORT.
- **Approx size:** 60–100 MB. Smaller than `uni-db-onnx` because mistralrs's CPU code is more compact than ORT's static lib, but heavier than the base because the LLM/vision/diffusion/speech model code is non-trivial.
- **Host reqs:** none.
- **Use case:** self-hosted LLMs, vision, image generation, speech — but *no ONNX path*. Useful when your pipeline is "remote embeddings + local generation."

#### `uni-db-all`

- **Cargo features:** all of the above. `provider-onnx` + `provider-fastembed` + `provider-mistralrs` + `provider-candle` + all remotes.
- **Bundled:** cdylib with both ORT (static) and candle/mistralrs compiled in. Both stacks present, chosen at runtime.
- **Approx size:** 130–180 MB.
- **Host reqs:** none.
- **Use case:** "I don't want to think about which wheel to pick." Slightly larger than `uni-db-fastembed` + `uni-db-mistralrs` summed (because deduped), but wheel = single artifact.

### CUDA row — `gpu-cuda`, dynamic ORT linkage, candle CUDA kernels

For CUDA wheels, `gpu-cuda` is **mutually exclusive** with `provider-onnx` (enforced by `uni-xervo`'s `build.rs` since `0.6.0`). `gpu-cuda` activates the dynamic-linkage path: ORT becomes a separate `libonnxruntime.so` plus EP sidecars (`libonnxruntime_providers_cuda.so`, `libonnxruntime_providers_shared.so`) that need to be packaged alongside the cdylib in the wheel. `auditwheel repair --include-libs` (Linux) or `delvewheel` (Windows) handles this.

Sizes below include the EP sidecars where applicable.

#### `uni-db-onnx-cuda`

- **Cargo features:** base + `gpu-cuda` (no `provider-onnx`; `gpu-cuda` provides the ORT linkage).
- **Bundled:** cdylib + `libonnxruntime.so` (~50 MB) + `libonnxruntime_providers_cuda.so` (~180 MB) + `libonnxruntime_providers_shared.so` (~10 MB).
- **Approx size:** 230–280 MB.
- **Host reqs:** NVIDIA driver supporting the toolkit version PTX was compiled with; cuDNN ≥ 9 on the loader path. See [host requirements](#host-requirements-not-bundled-in-any-cuda-wheel) below.

#### `uni-db-fastembed-cuda`

- **Cargo features:** base + `provider-fastembed` + `gpu-cuda`.
- **Bundled:** same as `uni-db-onnx-cuda` — fastembed adds nothing new on the native side; it just adds Rust glue and the model registry.
- **Approx size:** 235–285 MB.
- **Host reqs:** same as `uni-db-onnx-cuda`.
- **Use case:** GPU-accelerated local embeddings + rerankers via ORT CUDA EP. The most common GPU deployment for retrieval.

#### `uni-db-mistralrs-cuda`

- **Cargo features:** base + `provider-mistralrs` + `provider-candle` + `gpu-cuda`. Note that `gpu-cuda` activates *both* the ORT CUDA path *and* the candle CUDA kernel path — but ORT isn't in this wheel, so only candle's CUDA kernels matter.
- **Bundled:** cdylib + candle CUDA kernels (PTX baked into the .rlib at build time, ~20–30 MB per supported SM target).
- **Approx size:** 100–150 MB. Smaller than `uni-db-onnx-cuda` because there's no ORT EP sidecar.
- **Host reqs:** NVIDIA driver supporting the toolkit version. cuDNN is *not* required for candle's CUDA path (cuDNN is an ORT requirement, not a candle one).
- **Use case:** GPU-accelerated LLM generation, VLMs, diffusion, speech. No ONNX rerankers.

#### `uni-db-all-cuda`

- **Cargo features:** all providers + `gpu-cuda`.
- **Bundled:** ORT EP sidecars + candle CUDA kernels.
- **Approx size:** 280–350 MB.
- **Host reqs:** NVIDIA driver + cuDNN ≥ 9 (cuDNN required because ORT is in this wheel).
- **Use case:** GPU-accelerated everything in one wheel.

#### Host requirements (not bundled in any CUDA wheel)

| Component | Why not bundled |
|---|---|
| **NVIDIA driver + `libcuda.so.1`** | Kernel-mode component; never bundled in any user-space artifact. |
| **CUDA runtime** (`libcudart.so.X`) | Comes with the NVIDIA driver/toolkit install. Bundling the wrong minor breaks compatibility. |
| **cuDNN ≥ 9** (only required when ORT is in the wheel) | ~600 MB. Licensing concerns. Convention is "system cuDNN; set `LD_LIBRARY_PATH` if needed." Same as `onnxruntime-gpu`. |

If a host doesn't have these, the failure mode is a clear runtime error at first model load — `libcudnn.so.9: cannot open shared object file` or similar. Caught by the post-install probe (see [open question #4](#open-questions)).

### Metal row — `gpu-coreml` and/or `gpu-metal`, macOS-arm64-only

The "metal" suffix is **overloaded** by which providers are present:

| Wheel | Cargo accel feature(s) | Why |
|---|---|---|
| `uni-db-onnx-metal` | `gpu-coreml` | ORT on macOS accelerates via CoreML, not Metal directly. |
| `uni-db-fastembed-metal` | `gpu-coreml` | Same — fastembed uses ORT. |
| `uni-db-mistralrs-metal` | `gpu-metal` | candle/mistralrs use Metal kernels directly, not via CoreML. |
| `uni-db-all-metal` | `gpu-coreml` + `gpu-metal` | Both — ORT path for ONNX, Metal kernels for candle. They're orthogonal and both make sense. |

ORT statically links the CoreML EP into pyke's macOS bundle (no separate sidecar `.dylib`). candle's Metal kernels compile to a `.metallib` baked into the binary at build time (~5–10 MB).

#### Approx sizes (macOS arm64, compressed)

| Wheel | Size |
|---|---|
| `uni-db-onnx-metal` | 80–110 MB |
| `uni-db-fastembed-metal` | 85–115 MB |
| `uni-db-mistralrs-metal` | 70–110 MB |
| `uni-db-all-metal` | 110–160 MB |

#### Host requirements

- macOS with Metal framework (every supported macOS already has it).
- macOS arm64 only — Apple Silicon. The x86_64 macOS platform isn't a target because the Metal/CoreML stacks aren't worth supporting separately for retired hardware.

---

## Build sketch

For each wheel, maturin produces one `_uni_db` cdylib using a directory-per-package layout (the same pattern Microsoft uses for `onnxruntime` vs `onnxruntime-gpu`):

```
bindings/
  uni-db/                          # base (CPU, remotes only)
    pyproject.toml                 # name = "uni-db"
    Cargo.toml                     # _uni_db cdylib, default features only
    src/
  uni-db-onnx/                     # ONNX, CPU
    pyproject.toml                 # name = "uni-db-onnx"
    Cargo.toml                     # adds provider-onnx
    src/                           # symlink to ../uni-db/src
  uni-db-onnx-cuda/                # ONNX, CUDA
    pyproject.toml                 # name = "uni-db-onnx-cuda"
    Cargo.toml                     # gpu-cuda (replaces provider-onnx)
    src/                           # symlink
  uni-db-onnx-metal/               # ONNX, CoreML
  uni-db-fastembed/                # fastembed + ONNX, CPU
  uni-db-fastembed-cuda/
  uni-db-fastembed-metal/
  uni-db-mistralrs/                # mistralrs + candle, CPU
  uni-db-mistralrs-cuda/
  uni-db-mistralrs-metal/
  uni-db-all/                      # everything, CPU
  uni-db-all-cuda/
  uni-db-all-metal/
```

All 13 sibling Cargo crates compile the **same `_uni_db` cdylib from the same source**, just with different cargo features. Each ships a wheel with a different PyPI name but the same Python module name (`uni_db`), so user code is portable across them.

`src/` (and any other shared input — README, type stubs, tests) is symlinked from each variant directory back to `bindings/uni-db/`. Single source of truth, 13 packaging wrappers.

CI: `cibuildwheel` (or `maturin-action` with platform matrices) drives one job per `(package, platform, python_version)` cell. Only sensible cells are built — `uni-db-mistralrs-cuda` is not built for macOS, `uni-db-all-metal` is not built for Linux, etc.

### CI build-time budget (rough)

| Wheel | Linux x86_64 wall-clock |
|---|---|
| Base, `-onnx`, `-fastembed` (CPU) | 3–6 min each |
| `-mistralrs`, `-all` (CPU) | 8–15 min (mistralrs + candle dominate) |
| Any `-cuda` | +10–15 min on top of the CPU build (candle's `nvcc` per-SM compilation) |
| Any `-metal` (macOS arm64) | 6–12 min |

Total CI time for one full release across 13 wheels × 3 Python versions × applicable platforms is on the order of **2–4 hours of wall-clock time**, parallelized across runners. Worth budgeting for.

---

## Decision rules for which wheel to install

A simple flowchart for users:

1. **Just remote APIs?** → `uni-db`.
2. **Local rerankers only (no embeddings, no LLMs)?** → `uni-db-onnx`. Add `-cuda`/`-metal` if you have a GPU.
3. **Local embeddings (and rerankers)?** → `uni-db-fastembed`. Add `-cuda`/`-metal` for GPU.
4. **Local LLMs / vision / image-gen / speech?** → `uni-db-mistralrs`. Add `-cuda`/`-metal` for GPU.
5. **Mix of all of the above, or unsure?** → `uni-db-all`. Add `-cuda`/`-metal` for GPU.

Worth shipping this as a one-page table in user-facing docs. Possibly with a `pip install uni-db[recommend]` quick command that runs `uni_db._recommend()` on the user's host and prints the suggested wheel — see [open question #4](#open-questions).

---

## What is **not** bundled in any wheel

| Component | Why not bundled |
|---|---|
| **cuDNN ≥ 9** (CUDA wheels with ORT only) | ~600 MB, NVIDIA licensing. |
| **CUDA runtime** (`libcudart.so.X`) | Comes with the NVIDIA driver/toolkit install. |
| **NVIDIA driver + `libcuda.so.1`** | Kernel-mode component; never bundled. |
| **Apple Metal framework** | macOS system component. |
| **MKL** | If `candle/mkl` is enabled, system-installed; default is OpenBLAS or pure-Rust SIMD. |
| **Models** (HuggingFace weights) | Downloaded on first use to a configurable cache (`~/.uni_cache/...` by default). Wheels never contain model weights. |

---

## Open questions

1. **EP fallback semantics on CPU wheels.** When a script with `execution_providers: ["cuda", "cpu"]` runs on a wheel without `gpu-cuda`, today `uni-xervo` returns a `CapabilityMismatch` error. Should the CPU wheel silently filter unsupported EPs from the list before passing to ORT, falling back to whatever's available? Or fail loudly so misconfigurations don't hide?

2. **`auto` execution-provider keyword.** Many ORT-based Python libraries accept `execution_providers: "auto"`. Natural extension: catalog spec interprets `"auto"` as "the first EP from this build's bundled list that loads successfully." Loosens the coupling between the script and the wheel variant.

3. **Where to host wheels.** Initial plan is PyPI for all 13. The CUDA wheels (especially `uni-db-onnx-cuda`, `uni-db-fastembed-cuda`, `uni-db-all-cuda`) may exceed PyPI's 100 MB per-file soft limit and need an exemption request. Alternative: custom index for CUDA wheels (PyTorch's model). Decide before publishing the first CUDA wheel.

4. **Post-install probe.** **Resolved.** Implemented in `bindings/uni-db/uni_db/_probe.py` (Step 9). `python -m uni_db check` prints a host report; `python -m uni_db recommend` suggests the best wheel for the host. `bindings/uni-db/uni_db/_variant.py` carries a `VARIANT` constant that the bootstrap script overrides per-variant — the probe reads this to decide which checks apply (NVIDIA driver + cuDNN for `*-cuda` wheels, Metal-on-macOS for `*-metal` wheels, extension-load for all). Tested in `bindings/uni-db/tests/test_probe.py` (16 cases).

5. **Lock-step versioning across wheels.** When `uni-db` bumps from 1.1.1 to 1.2.0, do all 13 sibling wheels follow lock-step? If yes (recommended), publish pipeline must release all before announcing. If asymmetric, users hit version drift. Lock-step is the only honest model.

6. **Asymmetric provider combinations.** What about a user who wants ONNX *and* mistralrs but *not* fastembed? Do we ship `uni-db-onnx-mistralrs`? My take: no — they pick `uni-db-all` (fastembed adds ~5 MB on top of ONNX, so the marginal cost is negligible). The current 13-wheel matrix deliberately has no "two of three" cells. If users complain we add them; until then the simple rule "one provider, all providers, or remote-only" is enough.

7. **Future hardware variants.** `uni-db-*-rocm` (AMD), `uni-db-*-directml` (Windows any-vendor), `uni-db-*-tensorrt`. Mechanically easy to add once the 13-wheel pattern is in place. Defer until users ask for them.

8. **Wheel-variant deprecation policy.** When a CUDA toolkit version goes EOL (e.g. CUDA 13.0 → CUDA 14.0 transition), do we keep building the old `-cuda` wheel for a release cycle, or cut over immediately? Convention from `onnxruntime-gpu` is to ship the latest only and let users on old hosts pin to old `uni-db-*-cuda` versions.

---

## Sequencing

The 13-wheel matrix is too much to land at once. Sensible rollout:

1. **Add cargo feature passthroughs to `bindings/uni-db/Cargo.toml`** mirroring the workspace passthroughs. Pure plumbing — no behavior change. Done as one commit.

2. **Stand up the base wheel CI** (no native ML, just remotes) — verify the pipeline works for `uni-db` with no special features. This proves the maturin + cibuildwheel skeleton.

3. **Add `uni-db-fastembed`** as the first non-trivial wheel. Most-requested feature combination for the typical retrieval use case (embeddings + rerank). Verify auditwheel/delvewheel work for the static-ORT case.

4. **Add `uni-db-fastembed-cuda`** — the first GPU wheel. Tests the dynamic-ORT + EP-sidecar path that all other CUDA wheels reuse.

5. **Add `uni-db-mistralrs` and `uni-db-mistralrs-cuda`** — independent stack (no ORT involved), exercises a different code path. Validate the per-SM PTX compilation timing budget on real CI runners.

6. **Add `uni-db-all` and `uni-db-all-cuda`** — once both single-provider stacks work, the all-in-one wheel is mostly a cargo-feature-list change.

7. **Add the Metal row** (`-metal` suffixes for `-onnx`, `-fastembed`, `-mistralrs`, `-all`). Needs a macOS arm64 CI runner. Last because it's mostly mechanical once the cargo features are in place.

8. **Implement the post-install probe** (`uni_db._probe()` + `uni-db check` CLI). Add once at least 4–5 wheels exist so the diagnostic message can guide users between them.

9. **Add `uni-db-onnx` and `uni-db-onnx-{cuda,metal}`** — the "ONNX only, no fastembed" wheels. Lower priority than `-fastembed` because most users who want ONNX also want embeddings, but worth shipping for the bring-your-own-model use case.

Each step is independently shippable. The CPU wheels can launch before any GPU wheel exists; the GPU wheels can land asymmetrically (CUDA before Metal or vice versa).

---

## What this does *not* solve

- **Source builds for users with custom hardware** (e.g. Jetson, embedded ARM with NVIDIA GPU). They build from source. The provided wheels cover the common cases.
- **Multi-version cuDNN matrix.** We pin one cuDNN major per CUDA wheel release. Users who need cuDNN 8 stay on an older `uni-db-*-cuda` release. Same model as `onnxruntime-gpu`.
- **Single-import "auto-detect" wheel.** A wheel that detects at import-time whether the host has CUDA and switches behavior accordingly is technically possible but would require shipping all kernels in one wheel (~500–700 MB) and is not worth it. The post-install probe accomplishes the same goal at install time without the size penalty.
- **Compile-time provider selection from a single PyPI install.** That's the whole reason this proposal exists — pip can't do it.
