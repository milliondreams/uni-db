# uni-db-mistralrs-metal

Python bindings for Uni Graph Database with **mistralrs + candle on Apple Metal**.

Apple Silicon only (macOS arm64). Local LLM / vision / image-gen / speech accelerated via candle's Metal kernels. No host setup required beyond a supported macOS.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-mistralrs-metal
```
