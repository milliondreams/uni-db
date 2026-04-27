# uni-db-mistralrs-cuda

Python bindings for Uni Graph Database with **mistralrs + candle on CUDA**.

GPU-accelerated LLM generation, vision, image generation, speech. Requires an NVIDIA driver supporting the bundled CUDA toolkit version. cuDNN is not required for this wheel (cuDNN is only needed when ORT is in the wheel).

If you don't have CUDA, install plain `uni-db-mistralrs` instead — same Python API, runs on CPU.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-mistralrs-cuda
```
