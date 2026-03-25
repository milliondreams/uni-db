# Python API Priorities

This document lists only the Python-facing API work that is worth prioritizing next. It is not a parity checklist for parity's sake. The goal is to expose the smallest set of additional APIs that unlock valid, high-value user workflows.

## Scope

There are two Python layers:

- `uni_db`: the direct database binding
- `uni_pydantic`: the Pydantic OGM built on top of `uni_db`

They should not converge on the same surface area.

- `uni_db` should expose database, runtime, admin, and reasoning features that Python users need directly.
- `uni_pydantic` should stay model- and session-oriented, with clean escape hatches to `uni_db` when users need lower-level control.

## Prioritization Rules

A Python API addition should be prioritized only if it meets at least one of these:

- It unblocks a real notebook, service, or ETL workflow that Python users are likely to own.
- It exposes an already-stable engine capability whose absence forces Python users into Rust for normal work.
- It fits naturally into the existing abstraction boundary of the target package.

An API should not be prioritized if it is:

- internal-only engine plumbing
- mostly useful for tests or implementation internals
- a duplicate admin surface better owned by `uni_db` than `uni_pydantic`
- parity theater without a clear workflow behind it

---

## Priority 1: `uni_db` Xervo Runtime Access

### Why This Is Worth Doing

This is the single highest-value missing Python surface.

Valid use cases:

- RAG notebooks that need direct embedding generation for query preparation
- local/offline semantic search workflows
- agent workflows that need text generation from the same runtime used by Uni
- debugging and validating vector-index auto-embedding behavior from Python
- multimodal demos where Python is the natural orchestration layer

Today Rust users can call `db.xervo()`, but Python users cannot access the same runtime directly.

### Proposed API

```python
import uni_db

db = uni_db.DatabaseBuilder.open("./graph") \
    .xervo_catalog_from_file("./models.json") \
    .build()

xervo = db.xervo()

vectors = xervo.embed("embed/default", ["graph databases", "vector search"])

result = xervo.generate(
    "llm/default",
    [
        uni_db.Message.system("You are a concise technical assistant."),
        uni_db.Message.user("Explain snapshot isolation in Uni."),
    ],
    uni_db.GenerationOptions(max_tokens=256, temperature=0.2),
)

quick = xervo.generate_text(
    "llm/default",
    ["List three reasons to use hybrid search."],
    uni_db.GenerationOptions(),
)
```

Async:

```python
adb = await uni_db.AsyncDatabaseBuilder.open("./graph") \
    .xervo_catalog_from_file("./models.json") \
    .build()

xervo = adb.xervo()
vectors = await xervo.embed("embed/default", ["search query"])
```

### Proposed Public Types

```python
class Xervo:
    def embed(self, alias: str, texts: list[str]) -> list[list[float]]: ...
    def generate(
        self,
        alias: str,
        messages: list[Message],
        options: GenerationOptions,
    ) -> GenerationResult: ...
    def generate_text(
        self,
        alias: str,
        messages: list[str],
        options: GenerationOptions,
    ) -> GenerationResult: ...

class AsyncXervo:
    async def embed(self, alias: str, texts: list[str]) -> list[list[float]]: ...
    async def generate(
        self,
        alias: str,
        messages: list[Message],
        options: GenerationOptions,
    ) -> GenerationResult: ...
    async def generate_text(
        self,
        alias: str,
        messages: list[str],
        options: GenerationOptions,
    ) -> GenerationResult: ...
```

Re-export the same conceptual message/result types as Rust:

- `Message`
- `MessageRole`
- `ContentBlock`
- `ImageInput`
- `GenerationOptions`
- `GenerationResult`

### Design Notes

- Keep this on `uni_db`, not `uni_pydantic`.
- Do not invent an OGM-specific generation abstraction.
- Match Rust naming closely so examples translate cleanly across languages.

---

## Priority 2: Full Locy Parity in `uni_db`

### Why This Is Worth Doing

Python already exposes `locy_evaluate`, which means Python users are explicitly invited to use Locy. The current binding is incomplete exactly where advanced Locy becomes operationally important.

Valid use cases:

- probabilistic reasoning notebooks using MNOR / MPROD
- explainable remediation workflows that need runtime warnings
- experimentation with exact probability vs. independence mode
- CI or analyst tooling that needs to fail on out-of-domain probabilities

Current gap:

- config does not expose `strict_probability_domain`, `probability_epsilon`, `exact_probability`, `max_bdd_variables`
- result payload does not expose `warnings` or `approximate_groups`

### Proposed API

Keep the existing entry point:

```python
out = db.locy_evaluate(
    program,
    config={
        "max_iterations": 500,
        "timeout": 60.0,
        "max_derived_bytes": 64 * 1024 * 1024,
        "strict_probability_domain": True,
        "probability_epsilon": 1e-15,
        "exact_probability": True,
        "max_bdd_variables": 1000,
    },
)
```

Expand the return contract:

```python
{
    "derived": {...},
    "stats": LocyStats(...),
    "command_results": [...],
    "warnings": [
        {
            "code": "SharedProbabilisticDependency",
            "message": "...",
            "rule_name": "supplier_risk",
            "variable_count": None,
            "key_group": None,
        }
    ],
    "approximate_groups": {
        "supplier_risk": ["supplier=acme-east"]
    },
}
```

### Proposed Public Types

```python
class LocyWarning:
    code: str
    message: str
    rule_name: str
    variable_count: int | None
    key_group: str | None
```

Optional future step:

```python
class LocyResult(TypedDict):
    derived: dict[str, list[dict[str, Any]]]
    stats: LocyStats
    command_results: list[dict[str, Any]]
    warnings: list[LocyWarning]
    approximate_groups: dict[str, list[str]]
```

### Design Notes

- Do not create a second Locy API shape for Python.
- Extend the existing dict-based API first; typed wrappers can come later.
- This is a high-value parity target because the engine behavior is already present.

---

## Priority 3: Snapshot Management in `uni_db`

### Why This Is Worth Doing

Snapshots are a normal Python workflow feature, not a Rust-only systems feature.

Valid use cases:

- notebooks that need “reset to known baseline”
- data science experiments that checkpoint graph states
- ETL pipelines that take restore points before bulk updates
- demo environments that need deterministic rollback

### Proposed API

Sync:

```python
snapshot_id = db.create_snapshot()
snapshot_id = db.create_snapshot("baseline-import")

snapshots = db.list_snapshots()

db.restore_snapshot(snapshot_id)
```

Async:

```python
snapshot_id = await adb.create_snapshot("baseline-import")
snapshots = await adb.list_snapshots()
await adb.restore_snapshot(snapshot_id)
```

### Proposed Public Types

```python
class SnapshotInfo:
    snapshot_id: str
    name: str | None
    created_at: str
    version_hwm: int
```

### Design Notes

- This belongs on `uni_db`, not `uni_pydantic`.
- Keep the API small and direct.
- Python users should not need to go through Cypher procedures for this.

---

## Priority 4: Index Rebuild and Index Status APIs in `uni_db`

### Why This Is Worth Doing

Python is a natural place for ingestion and maintenance orchestration. If index rebuilds are async or fail, users need observability and control.

Valid use cases:

- ETL jobs that bulk-load data and then wait for indexes to be ready
- CI scripts that assert no rebuilds are stuck
- admin notebooks that inspect stale or failed index tasks
- repair workflows that retry failed index builds

### Proposed API

```python
tasks = db.index_rebuild_status()

retry_ids = db.retry_index_rebuilds()

task_id = db.rebuild_indexes("Document", async_=True)
ready = db.is_index_building("Document")

indexes = db.list_indexes("Document")
all_indexes = db.list_all_indexes()
```

Async:

```python
tasks = await adb.index_rebuild_status()
retry_ids = await adb.retry_index_rebuilds()
task_id = await adb.rebuild_indexes("Document", async_=True)
building = await adb.is_index_building("Document")
```

### Proposed Public Types

```python
class IndexRebuildTask:
    id: str
    label: str
    status: str
    retry_count: int
    error: str | None

class IndexDefinition:
    name: str
    index_type: str
    properties: list[str]
    state: str
```

### Design Notes

- Expose the same workflow Rust already supports.
- Avoid over-design; Python mainly needs status inspection and control hooks.

---

## Priority 5: Richer `uni_db` Builder and Config Surface

### Why This Is Worth Doing

Python can open the database, but not fully configure it like Rust can. That blocks non-trivial deployments.

Valid use cases:

- hybrid/cloud deployments from Python services
- programmatic tuning for write-heavy or memory-sensitive jobs
- xervo-enabled databases configured entirely from Python
- loading schema files at open time for reproducible setups

### Proposed API

```python
builder = (
    uni_db.DatabaseBuilder.open("./graph")
    .schema_file("./schema.json")
    .xervo_catalog_from_file("./models.json")
    .cache_size(512 * 1024 * 1024)
    .parallelism(8)
    .config(
        {
            "query_timeout": 30.0,
            "max_query_memory": 512 * 1024 * 1024,
            "max_transaction_memory": 512 * 1024 * 1024,
            "auto_flush_threshold": 50_000,
        }
    )
)

db = builder.build()
```

Cloud and hybrid:

```python
db = (
    uni_db.DatabaseBuilder.open("./local-meta")
    .hybrid("./local-meta", "s3://bucket/graph")
    .cloud_config(
        {
            "backend": "s3",
            "bucket": "bucket",
            "region": "us-east-1",
            "endpoint": None,
        }
    )
    .build()
)
```

### Design Notes

- Python does not need a 1:1 typed mirror of every Rust struct on day one.
- A dict-based config surface is acceptable if validation is strict and documented.
- `schema_file(...)`, `xervo_catalog_from_str(...)`, and `xervo_catalog_from_file(...)` are especially valuable.

---

## Priority 6: Query Streaming / Cursor API in `uni_db`

### Why This Is Worth Doing

Materializing all results into Python lists is fine for demos and bad for serious workloads.

Valid use cases:

- exporting large result sets
- analytics jobs over large scans
- service endpoints that stream rows
- notebook users inspecting large datasets incrementally

### Proposed API

Sync:

```python
cursor = db.query_cursor(
    "MATCH (d:Document) RETURN d.title AS title, d.score AS score"
)

for row in cursor:
    ...

batch = cursor.fetch_many(1000)
all_rows = cursor.fetch_all()
cursor.close()
```

Async:

```python
cursor = await adb.query_cursor(
    "MATCH (d:Document) RETURN d.title AS title, d.score AS score"
)

async for row in cursor:
    ...
```

### Proposed Public Types

```python
class QueryCursor:
    def fetch_one(self) -> dict[str, Any] | None: ...
    def fetch_many(self, n: int) -> list[dict[str, Any]]: ...
    def fetch_all(self) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...

class AsyncQueryCursor:
    async def fetch_one(self) -> dict[str, Any] | None: ...
    async def fetch_many(self, n: int) -> list[dict[str, Any]]: ...
    async def fetch_all(self) -> list[dict[str, Any]]: ...
    async def close(self) -> None: ...
```

### Design Notes

- This should be on `uni_db`, not `uni_pydantic`.
- Keep the existing `query()` behavior for convenience; cursor is the opt-in scalable path.

---

## Priority 7: `uni_pydantic` Escape Hatch and Interop Improvements

### Why This Is Worth Doing

`uni_pydantic` should remain focused, but it must not trap users inside a narrowed abstraction.

Valid use cases:

- use OGM models for writes and raw Cypher for advanced reads
- run Locy or snapshot operations from an OGM-centric application
- mix typed session logic with lower-level vector search or admin APIs

### Proposed API

Expose the underlying database explicitly:

```python
session = UniSession(db)

raw_db = session.db
snapshots = raw_db.list_snapshots()
out = raw_db.locy_evaluate(program, config={"exact_probability": True})
```

Add explicit raw query helpers that preserve the session as the unit of work:

```python
rows = session.cypher(
    "MATCH (n:Person) WHERE n.age > $age RETURN n.name AS name",
    {"age": 30},
)

out = session.locy(program, config={"max_iterations": 500})
```

Async:

```python
rows = await async_session.cypher(...)
out = await async_session.locy(...)
```

### Design Notes

- Do not duplicate snapshot, compaction, or xervo APIs on `uni_pydantic`.
- Provide access to `uni_db`; let `uni_db` own engine/admin features.
- OGM convenience is valuable only if the escape hatch is first-class.

---

## Explicit Non-Priorities

These should not be near-term goals.

### Internal Engine Manager Exposure

Do not prioritize Python APIs for:

- storage manager
- schema manager
- procedure registry
- raw runtime internals unrelated to user workflows

These are implementation-facing, not user-facing.

### Full Rust-to-Python Surface Mirroring

Do not make “every Rust method exists in Python” the goal. That will create low-value maintenance cost and a confusing Python API.

### Duplicate Admin APIs in `uni_pydantic`

Do not add snapshot, compaction, index-admin, or xervo management separately to `uni_pydantic`. Those belong in `uni_db`.

### OGM Wrappers for Every Engine Feature

Do not build an OGM-flavored abstraction for:

- snapshots
- compaction
- query cursors
- model runtime access
- index rebuild administration

The correct design is a good escape hatch, not a second full control plane.

---

## Recommended Delivery Order

If implemented incrementally, the sequence should be:

1. `uni_db` Xervo runtime access
2. `uni_db` full Locy parity
3. `uni_db` snapshot management
4. `uni_db` index rebuild/status APIs
5. `uni_db` richer builder/config surface
6. `uni_db` query cursor / streaming
7. `uni_pydantic` escape hatch and interop cleanup

This order follows user value, not implementation neatness.
