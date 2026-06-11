"""Conflict-retry helpers mirroring Rust's ``Session::transact_with_retry``.

Under default-on SSI, conflicting concurrent commits abort with a retriable
error instead of silently losing writes. These helpers re-run a transaction
body with jittered exponential backoff, exactly like the Rust API
(``RetryOptions``: 5 attempts, 200 µs base, 50 ms cap, ±50 % jitter).

The helper owns the transaction lifecycle: it creates the transaction,
invokes the user callable with it, and commits. The callable must NOT
commit or roll back itself — returning normally means "commit me".
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from ._uni_db import (  # type: ignore[attr-defined]
    UniCommitTimeoutError,
    UniConstraintConflictError,
    UniLockTimeoutError,
    UniTransactionConflictError,
)

T = TypeVar("T")

#: Exception types classified as transient contention — safe to retry with a
#: fresh transaction. Mirrors Rust's ``UniError::is_retriable``.
RETRIABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    UniTransactionConflictError,
    UniConstraintConflictError,
    UniLockTimeoutError,
    UniCommitTimeoutError,
)

#: Defaults mirroring Rust's ``RetryOptions::default()``.
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BASE_BACKOFF = 0.0002  # 200 µs
DEFAULT_MAX_BACKOFF = 0.05  # 50 ms
DEFAULT_JITTER = 0.5  # ±50 %


def _backoff_delay(attempt: int, base: float, cap: float, jitter: float) -> float:
    """Exponential backoff with fractional jitter, clamped to ``cap``."""
    delay = min(base * (2 ** (attempt - 1)), cap)
    if jitter > 0.0:
        spread = delay * jitter
        delay = max(0.0, delay + random.uniform(-spread, spread))
    return delay


def transact_with_retry(
    session: Any,
    fn: Callable[[Any], T],
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_backoff: float = DEFAULT_BASE_BACKOFF,
    max_backoff: float = DEFAULT_MAX_BACKOFF,
    jitter: float = DEFAULT_JITTER,
) -> T:
    """Run ``fn(tx)`` in a fresh transaction, retrying on conflict.

    Creates a transaction from ``session``, calls ``fn`` with it, then
    commits. If the body or the commit raises one of
    :data:`RETRIABLE_EXCEPTIONS`, the transaction is rolled back and the
    whole body re-runs in a NEW transaction after a jittered exponential
    backoff — up to ``max_attempts`` total attempts. Non-retriable
    exceptions roll back and propagate immediately.

    Returns whatever ``fn`` returned on the successful attempt.
    """
    attempt = 1
    while True:
        tx = session.tx()
        try:
            result = fn(tx)
            tx.commit()
            return result
        except RETRIABLE_EXCEPTIONS:
            _rollback_quietly(tx)
            if attempt >= max_attempts:
                raise
            time.sleep(_backoff_delay(attempt, base_backoff, max_backoff, jitter))
            attempt += 1
        except BaseException:
            _rollback_quietly(tx)
            raise


def execute_with_retry(
    session: Any,
    cypher: str,
    params: dict[str, Any] | None = None,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_backoff: float = DEFAULT_BASE_BACKOFF,
    max_backoff: float = DEFAULT_MAX_BACKOFF,
    jitter: float = DEFAULT_JITTER,
) -> Any:
    """Run a single mutation with conflict retry.

    Convenience over :func:`transact_with_retry` for a self-contained
    statement such as an atomic read-modify-write ``SET``.
    """

    def body(tx: Any) -> Any:
        if params:
            return tx.execute(cypher, params)
        return tx.execute(cypher)

    return transact_with_retry(
        session,
        body,
        max_attempts=max_attempts,
        base_backoff=base_backoff,
        max_backoff=max_backoff,
        jitter=jitter,
    )


async def async_transact_with_retry(
    session: Any,
    fn: Callable[[Any], Awaitable[T]],
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_backoff: float = DEFAULT_BASE_BACKOFF,
    max_backoff: float = DEFAULT_MAX_BACKOFF,
    jitter: float = DEFAULT_JITTER,
) -> T:
    """Async variant of :func:`transact_with_retry`.

    ``fn`` is an async callable receiving the transaction; the helper
    awaits it, commits, and retries on retriable conflicts with
    ``asyncio.sleep`` backoff.
    """
    attempt = 1
    while True:
        tx = await session.tx()
        try:
            result = await fn(tx)
            await tx.commit()
            return result
        except RETRIABLE_EXCEPTIONS:
            await _async_rollback_quietly(tx)
            if attempt >= max_attempts:
                raise
            await asyncio.sleep(
                _backoff_delay(attempt, base_backoff, max_backoff, jitter)
            )
            attempt += 1
        except BaseException:
            await _async_rollback_quietly(tx)
            raise


async def async_execute_with_retry(
    session: Any,
    cypher: str,
    params: dict[str, Any] | None = None,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_backoff: float = DEFAULT_BASE_BACKOFF,
    max_backoff: float = DEFAULT_MAX_BACKOFF,
    jitter: float = DEFAULT_JITTER,
) -> Any:
    """Async variant of :func:`execute_with_retry`."""

    async def body(tx: Any) -> Any:
        if params:
            return await tx.execute(cypher, params)
        return await tx.execute(cypher)

    return await async_transact_with_retry(
        session,
        body,
        max_attempts=max_attempts,
        base_backoff=base_backoff,
        max_backoff=max_backoff,
        jitter=jitter,
    )


def _rollback_quietly(tx: Any) -> None:
    """Roll back, swallowing already-completed errors (commit may have
    consumed the transaction before failing)."""
    try:
        tx.rollback()
    except Exception:
        pass


async def _async_rollback_quietly(tx: Any) -> None:
    try:
        await tx.rollback()
    except Exception:
        pass
