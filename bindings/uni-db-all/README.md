# uni-db-all

Python bindings for Uni Graph Database with **every provider compiled in** (CPU).

Includes ONNX (raw + rerank), fastembed (local embeddings), mistralrs + candle (local LLMs / vision / diffusion / speech), plus all remote providers. Largest wheel — installs everything in one shot.

For GPU acceleration use `uni-db-all-cuda` (NVIDIA) or `uni-db-all-metal` (Apple Silicon). For smaller wheels with subsets of providers see `uni-db-onnx`, `uni-db-fastembed`, or `uni-db-mistralrs`.

For the full wheel matrix, see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-all
```
