# uni-db-onnx-metal

Python bindings for Uni Graph Database with the **ONNX provider on Apple CoreML**.

Apple Silicon only (macOS arm64). ORT models accelerate via the CoreML execution provider; no host setup required beyond a supported macOS.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-onnx-metal
```
