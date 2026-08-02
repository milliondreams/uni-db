"""Every teardown path must leave no scratch directory behind.

An ``in_memory()`` database is only in-memory from the caller's side: it is
backed by a ``uni_mem_*`` directory. ``shutdown()`` used to call ``flush()`` and
return, leaving real teardown to ``Drop`` at garbage-collection time -- which
*signals* the background tasks without awaiting them, so the directory was
removed while writers were still finishing and survived a few percent of the
time.

The survivors never expire. A suite opening thousands of databases strands tens
per run, and on a tmpfs ``TMPDIR`` that exhausts *inodes* rather than space --
presenting as unrelated "failed to create temporary directory" errors while
``df -h`` shows the disk nearly empty.

All four paths are covered because they were not all fixed together: the async
context manager open-coded a flush of its own and kept leaking after the two
explicit ``shutdown()`` calls were corrected.
"""

import pathlib

import pytest

import uni_db

CYCLES = 60


def _stranded(root: pathlib.Path) -> list[str]:
    return sorted(p.name for p in root.iterdir() if p.name.startswith("uni_mem_"))


@pytest.fixture
def scratch_root(tmp_path, monkeypatch):
    """A private TMPDIR, so a concurrent test's databases are not miscounted."""
    root = tmp_path / "scratch"
    root.mkdir()
    monkeypatch.setenv("TMPDIR", str(root))
    return root


def test_sync_shutdown_leaves_nothing(scratch_root):
    for _ in range(CYCLES):
        uni_db.Uni.in_memory().shutdown()
    assert _stranded(scratch_root) == []


def test_sync_context_manager_leaves_nothing(scratch_root):
    for _ in range(CYCLES):
        with uni_db.Uni.in_memory():
            pass
    assert _stranded(scratch_root) == []


@pytest.mark.asyncio
async def test_async_shutdown_leaves_nothing(scratch_root):
    for _ in range(CYCLES):
        db = await uni_db.AsyncUni.in_memory()
        await db.shutdown()
    assert _stranded(scratch_root) == []


@pytest.mark.asyncio
async def test_async_context_manager_leaves_nothing(scratch_root):
    # The path that stayed broken after the explicit `shutdown()` calls were
    # fixed: `__aexit__` open-coded a flush instead of delegating to shutdown.
    for _ in range(CYCLES):
        async with await uni_db.AsyncUni.in_memory():
            pass
    assert _stranded(scratch_root) == []
