# uni-db-fastembed-metal

Python bindings for Uni Graph Database with **fastembed + ONNX on Apple CoreML**.

Apple Silicon only (macOS arm64). Local embeddings + reranking accelerated via the CoreML execution provider; no host setup required.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-fastembed-metal
```
