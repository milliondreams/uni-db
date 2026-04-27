# uni-db-mistralrs

Python bindings for Uni Graph Database with **mistralrs + candle**.

Adds self-hosted LLM generation, vision (VLMs), image generation (FLUX), and speech (Whisper ASR, TTS). CPU-only in this wheel; for GPU acceleration use `uni-db-mistralrs-cuda` (NVIDIA) or `uni-db-mistralrs-metal` (Apple Silicon).

This wheel does **not** include ONNX — reranking and ONNX-based embeddings aren't available. Use `uni-db-all` if you need both stacks.

For the full wheel matrix (CPU / CUDA / Metal variants, other provider combos), see [the wheel matrix doc](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/python-wheel-matrix.md). For full documentation, see https://rustic-ai.github.io/uni-db.

```
pip install uni-db-mistralrs
```
