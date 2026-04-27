# uni-db-all-cuda

Python bindings for Uni Graph Database with **every provider + CUDA**.

GPU-accelerated everything: ORT CUDA EP for ONNX/fastembed/reranking, candle CUDA kernels for mistralrs LLMs/vision/diffusion/speech. Requires:

- NVIDIA driver supporting the bundled CUDA toolkit version.
- cuDNN ≥ 9 on the host loader path. Not bundled.

If you don't have these, install `uni-db-all` instead — same Python API, runs on CPU.

For the full wheel matrix, see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-all-cuda
```
