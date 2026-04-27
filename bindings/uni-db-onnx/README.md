# uni-db-onnx

Python bindings for Uni Graph Database with the **ONNX provider** compiled in.

This wheel adds local cross-encoder reranking and raw ONNX tensor execution on top of the base remote-only `uni-db`. ORT is statically linked (CPU); no `ORT_DYLIB_PATH` setup required.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-onnx
```

The Python API is identical across all `uni-db` wheel variants — pick the wheel that matches the providers and accelerator your deployment needs.
