# Python API — retired

> **Status: RETIRED — do not edit.**
>
> This file was one of **two** hand-maintained references for the same surface.
> Together they ran to ~10,000 lines, neither generated, and they drifted
> independently — which is how each ended up documenting methods the other
> lacked and that never existed (`xervo.rerank`, `session.profile_with`).
> Maintaining both was the defect, so this copy is retired rather than merged:
> merging two drifting hand-written sources propagates the fiction in each.

The published, narrative reference is
[`website/docs/reference/python-api.md`](../website/docs/reference/python-api.md).

The **complete symbol surface** is now **generated** from
`bindings/uni-db/uni_db/__init__.pyi` into
`website/docs/reference/python-api-symbols.md` — regenerate with
`python3 scripts/gen_python_api_reference.py`, and CI fails if it is stale.
