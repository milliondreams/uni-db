# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Pytest configuration and fixtures for uni-pydantic tests."""

import pytest


@pytest.fixture
def db():
    """Create a temporary database instance.

    Uses ``UniBuilder.temporary()`` (a Rust-owned temp dir) rather than opening
    at an external ``tempfile.TemporaryDirectory``. The latter races the
    database's background flush threads, which keep writing after the test body
    returns, so the directory's ``rmtree`` can fail with
    ``OSError: Directory not empty``. With ``temporary()`` the Rust side stops
    those threads on drop and removes its own temp dir, so teardown is race-free
    by construction. (Mirrors ``async_db`` below, which already uses
    ``AsyncUni.temporary()``.)
    """
    try:
        import uni_db

        return uni_db.UniBuilder.temporary().build()
    except ImportError:
        pytest.skip("uni_db not available")


@pytest.fixture
def session(db):
    """Create a UniSession with a temporary database."""
    from uni_pydantic import UniSession

    with UniSession(db) as s:
        yield s


@pytest.fixture
async def async_db():
    """Create an async temporary database instance."""
    try:
        import uni_db

        db = await uni_db.AsyncUni.temporary()
        yield db
    except ImportError:
        pytest.skip("uni_db not available")


@pytest.fixture
async def async_session(async_db):
    """Create an AsyncUniSession with a temporary database."""
    from uni_pydantic import AsyncUniSession

    async with AsyncUniSession(async_db) as s:
        yield s
