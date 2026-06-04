# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Sync/async binding parity guard + functional tests.

The sync (`uni_db.Uni` & friends) and async (`uni_db.AsyncUni` & friends)
surfaces are meant to be 1:1 mirrors. `test_sync_async_method_parity` is the
regression guard against drift; the functional tests exercise the async
methods added to close known gaps: `AsyncUni.load_rhai_plugin`,
`AsyncTransaction.appender` / `appender_builder`
(`AsyncStreamingAppender` / `AsyncTxAppenderBuilder`), and the async
session-template classes.
"""

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Parity guard
# ---------------------------------------------------------------------------

# Each sync pyclass and its async counterpart must expose the same public
# (non-dunder) method surface. Context-manager / iterator dunders differ by
# design (`__enter__` vs `__aenter__`, `__next__` vs `__anext__`) and are
# intentionally excluded by the `_public_methods` filter.
SYNC_ASYNC_PAIRS = [
    ("Uni", "AsyncUni"),
    ("Session", "AsyncSession"),
    ("Transaction", "AsyncTransaction"),
    ("UniBuilder", "AsyncUniBuilder"),
    ("TransactionBuilder", "AsyncTransactionBuilder"),
    ("SessionQueryBuilder", "AsyncSessionQueryBuilder"),
    ("SessionLocyBuilder", "AsyncSessionLocyBuilder"),
    ("TxQueryBuilder", "AsyncTxQueryBuilder"),
    ("TxExecuteBuilder", "AsyncTxExecuteBuilder"),
    ("TxLocyBuilder", "AsyncTxLocyBuilder"),
    ("ApplyBuilder", "AsyncApplyBuilder"),
    ("ForkBuilder", "AsyncForkBuilder"),
    ("ForkSchemaBuilder", "AsyncForkSchemaBuilder"),
    ("Compaction", "AsyncCompaction"),
    ("Indexes", "AsyncIndexes"),
    ("TxBulkWriterBuilder", "AsyncTxBulkWriterBuilder"),
    ("TxAppenderBuilder", "AsyncTxAppenderBuilder"),
    ("StreamingAppender", "AsyncStreamingAppender"),
    ("SessionTemplateBuilder", "AsyncSessionTemplateBuilder"),
    ("SessionTemplate", "AsyncSessionTemplate"),
    ("BulkWriter", "AsyncBulkWriter"),
    ("QueryCursor", "AsyncQueryCursor"),
    ("SchemaBuilder", "AsyncSchemaBuilder"),
    ("LabelBuilder", "AsyncLabelBuilder"),
    ("EdgeTypeBuilder", "AsyncEdgeTypeBuilder"),
]


def _public_methods(cls):
    return {
        n for n in dir(cls) if not n.startswith("_") and callable(getattr(cls, n, None))
    }


@pytest.mark.parametrize("sync_name,async_name", SYNC_ASYNC_PAIRS)
def test_sync_async_method_parity(sync_name, async_name):
    sync_cls = getattr(uni_db, sync_name, None)
    async_cls = getattr(uni_db, async_name, None)
    assert sync_cls is not None, f"missing sync class uni_db.{sync_name}"
    assert async_cls is not None, f"missing async class uni_db.{async_name}"
    sync_methods = _public_methods(sync_cls)
    async_methods = _public_methods(async_cls)
    assert sync_methods == async_methods, (
        f"{sync_name} vs {async_name} method mismatch: "
        f"sync-only={sorted(sync_methods - async_methods)}, "
        f"async-only={sorted(async_methods - sync_methods)}"
    )


def test_async_query_builder_removed():
    """The dead, unreachable `AsyncQueryBuilder` was removed."""
    assert not hasattr(uni_db, "AsyncQueryBuilder")


# ---------------------------------------------------------------------------
# Functional: AsyncUni.load_rhai_plugin
# ---------------------------------------------------------------------------

RHAI_SCRIPT = """
fn uni_manifest() {
    #{
        id: "ai.example.score",
        version: "0.1.0",
        determinism: "pure",
        scalar_fns: [
            #{ name: "score", args: ["float","float"], returns: "float" },
        ],
    }
}
fn score(x, y) { x * 0.7 + y * 0.3 }
"""


async def test_async_load_rhai_plugin_returns_metadata():
    db = await uni_db.AsyncUni.temporary()
    outcome = await db.load_rhai_plugin(RHAI_SCRIPT)
    assert outcome["plugin_id"] == "ai.example.score"
    assert outcome["version"] == "0.1.0"
    assert "ai.example.score.score" in outcome["scalars_registered"]
    assert outcome["aggregates_registered"] == []
    assert outcome["procedures_registered"] == []


async def test_async_load_rhai_plugin_explicit_grants():
    db = await uni_db.AsyncUni.temporary()
    outcome = await db.load_rhai_plugin(RHAI_SCRIPT, grants=["ScalarFn"])
    assert outcome["plugin_id"] == "ai.example.score"


async def test_async_load_rhai_plugin_rejects_bad_grant():
    db = await uni_db.AsyncUni.temporary()
    # Mirrors the sync behavior: unknown grants raise ValueError.
    with pytest.raises(ValueError):
        await db.load_rhai_plugin(RHAI_SCRIPT, grants=["NotARealCapability"])


async def test_async_load_rhai_plugin_rejects_bad_script():
    db = await uni_db.AsyncUni.temporary()
    with pytest.raises(Exception):
        await db.load_rhai_plugin("@@@ this is not rhai @@@")


# ---------------------------------------------------------------------------
# Functional: async streaming appender
# ---------------------------------------------------------------------------


async def test_async_appender_appends_and_persists():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").apply()
    session = db.session()
    tx = await session.tx()
    app = await tx.appender("Person")
    assert type(app).__name__ == "AsyncStreamingAppender"
    await app.append({"name": "Alice"})
    await app.append({"name": "Bob"})
    stats = await app.finish()
    assert stats.vertices_inserted == 2
    await tx.commit()

    results = await db.session().query("MATCH (n:Person) RETURN count(n) AS c")
    assert results[0]["c"] == 2


async def test_async_appender_builder_configures_and_builds():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Item").property("sku", "string").apply()
    session = db.session()
    tx = await session.tx()
    builder = tx.appender_builder("Item")
    assert type(builder).__name__ == "AsyncTxAppenderBuilder"
    builder = builder.batch_size(64).max_buffer_size_bytes(1 << 20)
    app = await builder.build()
    assert type(app).__name__ == "AsyncStreamingAppender"
    await app.append({"sku": "A1"})
    stats = await app.finish()
    assert stats.vertices_inserted == 1
    await tx.commit()


# ---------------------------------------------------------------------------
# Functional: async session template
# ---------------------------------------------------------------------------


async def test_async_session_template_builds_async_session():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").apply()

    template = db.session_template().param("tenant", 1).build()
    assert type(template).__name__ == "AsyncSessionTemplate"

    session = template.create()
    assert type(session).__name__ == "AsyncSession"

    tx = await session.tx()
    await tx.execute("CREATE (n:Person {name: 'Zoe'})")
    await tx.commit()

    results = await session.query("MATCH (n:Person) RETURN n.name AS name")
    assert any(r["name"] == "Zoe" for r in results)
