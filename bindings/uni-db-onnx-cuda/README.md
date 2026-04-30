# uni-db-onnx-cuda

Python bindings for Uni Graph Database — **slim ONNX + remote APIs + NVIDIA CUDA**. Adds GPU-accelerated cross-encoder reranking, raw ONNX tensor execution, and ONNX-based dense embeddings on top of the slim `uni-db-onnx`.

Requires:

- NVIDIA driver supporting the bundled CUDA toolkit version.
- cuDNN ≥ 9 on the host loader path (typically `/usr/local/cuda-X.X/...`). Not bundled.

If you don't have these, install plain `uni-db-onnx` instead — same Python API, runs on CPU.

```
pip install uni-db-onnx-cuda
```

For the full wheel matrix (default-everything, slim, CUDA, Apple GPU/ANE), see the [migration guide](https://github.com/rustic-ai/uni-db/blob/main/docs/migrations/0.9.0-wheel-matrix-collapse.md).
