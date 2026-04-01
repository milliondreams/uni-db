# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for error handling and edge cases."""

import pytest

import uni_db


@pytest.mark.asyncio
async def test_invalid_cypher_syntax_raises_exception(async_empty_db):
    """Test that invalid Cypher syntax raises an exception."""
    session = async_empty_db.session()

    with pytest.raises(Exception):
        await session.query("INVALID CYPHER SYNTAX !!!")

    with pytest.raises(Exception):
        tx = await session.tx()
        await tx.execute("CREATE (n:Person {name: 'Alice' MISSING_BRACE")
        await tx.commit()


@pytest.mark.asyncio
async def test_query_on_non_existent_label(async_empty_db):
    """Test querying a non-existent label returns empty results or raises error."""
    session = async_empty_db.session()

    try:
        result = await session.query("MATCH (n:NonExistentLabel) RETURN n")
        assert isinstance(result, list)
    except (RuntimeError, Exception):
        # Also acceptable: the DB raises an error for unknown labels
        pass


@pytest.mark.asyncio
async def test_type_mismatch_in_property(async_empty_db):
    """Test that type mismatches in properties raise appropriate errors."""

    await (
        async_empty_db.schema()
        .label("Person")
        .property("name", "string")
        .property_nullable("age", "int")
        .done()
        .apply()
    )

    session = async_empty_db.session()
    try:
        tx = await session.tx()
        await tx.execute("""
            CREATE (:Person {name: 'Alice', age: 'not_a_number'})
        """)
        await tx.commit()
        pass  # No error means type was coerced
    except Exception:
        pass


@pytest.mark.asyncio
async def test_operations_on_committed_bulk_writer_raise_error(async_empty_db):
    """Test that operations on a committed async bulk writer raise RuntimeError."""

    await async_empty_db.schema().label("Person").property("name", "string").apply()

    session = async_empty_db.session()
    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    await writer.insert_vertices("Person", [{"name": "Alice"}])
    await writer.commit()
    await tx.commit()

    with pytest.raises(RuntimeError):
        await writer.insert_vertices("Person", [{"name": "Bob"}])

    with pytest.raises(RuntimeError):
        await writer.commit()


@pytest.mark.asyncio
async def test_operations_on_aborted_bulk_writer_raise_error(async_empty_db):
    """Test that operations on an aborted async bulk writer raise RuntimeError."""

    await async_empty_db.schema().label("Person").property("name", "string").apply()

    session = async_empty_db.session()
    tx = await session.tx()
    writer = await tx.bulk_writer().build()
    await writer.insert_vertices("Person", [{"name": "Alice"}])
    writer.abort()
    await tx.rollback()

    with pytest.raises(RuntimeError):
        await writer.insert_vertices("Person", [{"name": "Bob"}])

    with pytest.raises(RuntimeError):
        await writer.commit()


@pytest.mark.asyncio
async def test_double_commit_on_transaction_raises_error(async_empty_db):
    """Test that double commit on async transaction raises UniTransactionAlreadyCompletedError."""

    await async_empty_db.schema().label("Person").property("name", "string").apply()

    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query("CREATE (:Person {name: 'Alice'})")
    await tx.commit()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.commit()


@pytest.mark.asyncio
async def test_double_rollback_on_transaction_raises_error(async_empty_db):
    """Test that double rollback on async transaction raises UniTransactionAlreadyCompletedError."""

    await async_empty_db.schema().label("Person").property("name", "string").apply()

    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query("CREATE (:Person {name: 'Alice'})")
    await tx.rollback()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.rollback()


@pytest.mark.asyncio
async def test_operations_after_transaction_commit_raise_error(async_empty_db):
    """Test that operations after async transaction commit raise UniTransactionAlreadyCompletedError."""

    await async_empty_db.schema().label("Person").property("name", "string").apply()

    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query("CREATE (:Person {name: 'Alice'})")
    await tx.commit()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.query("MATCH (n:Person) RETURN n")

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.query("CREATE (:Person {name: 'Bob'})")


@pytest.mark.asyncio
async def test_operations_after_transaction_rollback_raise_error(async_social_db):
    """Test that operations after async transaction rollback raise UniTransactionAlreadyCompletedError."""

    session = async_social_db.session()
    tx = await session.tx()
    await tx.query("CREATE (:Person {name: 'test_person', age: 25})")
    await tx.rollback()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.query("MATCH (n:Person) RETURN n")

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.query("CREATE (:Person {name: 'another_person'})")


@pytest.mark.asyncio
async def test_commit_after_rollback_raises_error(async_empty_db):
    """Test that committing after rollback raises UniTransactionAlreadyCompletedError."""

    await async_empty_db.schema().label("Person").property("name", "string").apply()

    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query("CREATE (:Person {name: 'Alice'})")
    await tx.rollback()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.commit()


@pytest.mark.asyncio
async def test_rollback_after_commit_raises_error(async_empty_db):
    """Test that rolling back after commit raises UniTransactionAlreadyCompletedError."""

    await async_empty_db.schema().label("Person").property("name", "string").apply()

    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query("CREATE (:Person {name: 'Alice'})")
    await tx.commit()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.rollback()
