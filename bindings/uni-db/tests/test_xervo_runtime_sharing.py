# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Issue #155 — sharing one ModelRuntime across Uni instances.

Before this API existed, N databases built from the same catalog meant N
resident copies of the same weights, with no workaround available from Python.

These tests prove sharing *without downloading a model*. `ModelRuntime`
compares by identity (`Arc::ptr_eq` under the hood), so "both databases are
backed by the same runtime" is directly assertable; a regression that rebuilt
the runtime per database flips the equality deterministically. The catalog uses
a lazily-warmed remote provider, which is validated at build time but never
loaded, and `remote/openai` is compiled into every wheel variant.
"""

import json

import pytest

import uni_db

CATALOG = json.dumps(
    [
        {
            "alias": "embed/shared",
            "task": "embed",
            "provider_id": "remote/openai",
            "model_id": "text-embedding-3-small",
            "warmup": "lazy",
            "required": False,
        }
    ]
)


def test_runtime_is_constructible_standalone():
    """No throwaway bootstrap database is needed to mint a runtime."""
    rt = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    assert rt.contains_alias("embed/shared")
    assert not rt.contains_alias("embed/absent")


def test_one_runtime_backs_two_databases():
    rt = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    a = uni_db.UniBuilder.in_memory().xervo_runtime(rt).build()
    b = uni_db.UniBuilder.in_memory().xervo_runtime(rt).build()

    assert a.xervo().is_available()
    assert b.xervo().is_available()

    # The load-bearing assertion: the SAME runtime object backs both, so the
    # models resident in it are shared rather than duplicated.
    assert a.xervo().raw_runtime() == b.xervo().raw_runtime()
    assert a.xervo().raw_runtime() == rt


def test_a_separately_built_runtime_is_a_distinct_object():
    """Guards the assertion above from being vacuously true."""
    rt1 = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    rt2 = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    assert rt1 != rt2, "identical catalogs must still yield distinct runtimes"

    a = uni_db.UniBuilder.in_memory().xervo_runtime(rt1).build()
    b = uni_db.UniBuilder.in_memory().xervo_runtime(rt2).build()
    assert a.xervo().raw_runtime() != b.xervo().raw_runtime()


def test_handles_are_hashable_by_identity():
    rt = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    db = uni_db.UniBuilder.in_memory().xervo_runtime(rt).build()
    assert len({rt, db.xervo().raw_runtime()}) == 1


def test_raw_runtime_is_none_without_xervo():
    db = uni_db.UniBuilder.in_memory().build()
    assert db.xervo().is_available() is False
    assert db.xervo().raw_runtime() is None


def test_round_trip_from_a_catalog_built_database():
    """The other direction: hand a catalog-built database's runtime onward."""
    boot = uni_db.UniBuilder.in_memory().xervo_catalog_from_str(CATALOG).build()
    rt = boot.xervo().raw_runtime()
    assert rt is not None

    db = uni_db.UniBuilder.in_memory().xervo_runtime(rt).build()
    assert db.xervo().raw_runtime() == rt


def test_runtime_supersedes_a_previously_set_catalog():
    """The three Xervo sources are mutually exclusive, last setter wins."""
    rt = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    db = (
        uni_db.UniBuilder.in_memory()
        .xervo_catalog_from_str(CATALOG)
        .xervo_runtime(rt)
        .build()
    )
    assert db.xervo().raw_runtime() == rt


def test_catalog_supersedes_a_previously_set_runtime():
    rt = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    db = (
        uni_db.UniBuilder.in_memory()
        .xervo_runtime(rt)
        .xervo_catalog_from_str(CATALOG)
        .build()
    )
    got = db.xervo().raw_runtime()
    assert got is not None
    assert got != rt, "the later catalog must build its own runtime"


def test_an_invalid_catalog_raises():
    with pytest.raises(Exception):
        uni_db.ModelRuntime.from_catalog_str('[{"alias": "bad"}]')


async def test_async_constructor_and_builder():
    rt = await uni_db.ModelRuntime.from_catalog_str_async(CATALOG)
    assert rt.contains_alias("embed/shared")

    db = await uni_db.AsyncUniBuilder.in_memory().xervo_runtime(rt).build()
    assert db.xervo().raw_runtime() == rt


async def test_sync_and_async_share_one_handle_type():
    """One handle feeds both builders — no separate AsyncModelRuntime."""
    rt = uni_db.ModelRuntime.from_catalog_str(CATALOG)
    sync_db = uni_db.UniBuilder.in_memory().xervo_runtime(rt).build()
    async_db = await uni_db.AsyncUniBuilder.in_memory().xervo_runtime(rt).build()
    assert sync_db.xervo().raw_runtime() == async_db.xervo().raw_runtime()
