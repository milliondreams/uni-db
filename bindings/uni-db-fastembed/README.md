# uni-db-fastembed

Python bindings for Uni Graph Database with **fastembed-rs + ONNX**.

Adds local text/image embeddings (BGE, E5, Jina, Nomic, …) plus cross-encoder reranking and raw ONNX tensor execution. ORT is statically linked (CPU). The most common deployment shape for hybrid retrieval.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-fastembed
```

The Python API is identical across all `uni-db` wheel variants.
