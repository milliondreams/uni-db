# uni-db-metal

Python bindings for Uni Graph Database with **all 11 providers** (candle, mistralrs, ONNX local + 8 remote APIs) and **Apple GPU/ANE acceleration**: candle/mistralrs run on Metal, the ONNX runtime runs on the CoreML execution provider.

This is the recommended wheel for Apple Silicon Macs.

```
pip install uni-db-metal
```

For the full wheel matrix (slim, CPU, NVIDIA CUDA variants), see the [migration guide](https://github.com/rustic-ai/uni-db/blob/main/docs/migrations/0.9.0-wheel-matrix-collapse.md). For full documentation, see https://rustic-ai.github.io/uni-db.

The Python API is identical across all `uni-db` wheel variants — pick the wheel that matches the providers and accelerator your deployment needs.
