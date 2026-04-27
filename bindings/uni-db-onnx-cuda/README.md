# uni-db-onnx-cuda

Python bindings for Uni Graph Database with the **ONNX provider on CUDA**.

This wheel adds GPU-accelerated cross-encoder reranking and raw ONNX tensor execution. Requires:

- NVIDIA driver supporting the bundled CUDA toolkit version.
- cuDNN ≥ 9 on the host loader path (typically `/usr/local/cuda-X.X/...`). Not bundled.

If you don't have these, install plain `uni-db-onnx` instead — same Python API, runs on CPU.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-onnx-cuda
```
