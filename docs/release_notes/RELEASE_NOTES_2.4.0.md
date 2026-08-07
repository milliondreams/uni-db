# Release Notes — 2.4.0

**Tag:** `v2.4.0` · **Range:** `v2.3.0..v2.4.0` (30 commits)

Written retrospectively from the commit range: this release shipped tagged but
without notes. Grouped by theme; see `git log v2.3.0..v2.4.0` for the full list.

## Learned-sparse vectors (#95)

- Sparse (SPLADE-style) vector columns end to end: schema type, index, and
  `uni.sparse.query`.
- Text auto-embed on the sparse path for write, query and hybrid, including
  re-embedding targets on `SET`.
- `CREATE VECTOR INDEX ... type:'sparse'` routed to the sparse path, with
  backfill after flush.
- 8-bit weight quantization (default) plus a retrieval benchmark; P2 pruning
  deferred.
- Fork-local sparse index via brute-force branch scan.

## Retrieval correctness

- Dense search now unions committed-but-unflushed L0 rows, with a dense-index
  parity suite to pin it.
- `RETURN`-projection type fidelity for dense and multi-vector columns.
- Hardened sparse + multi-vector retrieval following the #95/#96 review.

## Durability and storage

- A committed-but-unflushed write survives a crash *during* flush.
- Corrected the `VertexDataset` `.lance` path; closed an `open_raw` fail-open
  and a silent index-skip.

## Locy

- Typed-value boundary fixes (#111 / #112 / #113) plus property type inference.

## Forks

- Adjacency CSR is warmed per-direction rather than gated on the dual-write
  overlay.
