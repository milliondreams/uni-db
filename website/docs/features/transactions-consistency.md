# Transactions & Consistency

Uni provides ACID transactions with Serializable Snapshot Isolation (SSI). Reads see a consistent snapshot, and writes are applied atomically on commit. Concurrent read-write transactions run optimistically: Uni detects conflicting write sets (including write skew) at commit time and aborts the loser with a serialization conflict instead of silently overwriting, so the committed history is always serializable.

## What It Provides

- Serializable Snapshot Isolation with optimistic concurrency control.
- Concurrent read-write transactions with write-skew prevention.
- `UniError::SerializationConflict` raised on a conflicting commit, with automatic abort + retry helpers.
- WAL-backed durability for committed changes.
- `CommitResult` type returned on successful commit with metadata.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:User {email: 'a@b.com'})").await?;
    tx.commit().await?;
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:User {email: 'a@b.com'})")
    tx.commit()
    ```

## Use Cases

- Multi-step writes that must commit atomically.
- Consistent reads during complex queries.
- Predictable concurrency without distributed locking — conflicting writers abort and retry via `transact_with_retry` instead of taking locks.

## When To Use

Use transactions for any workflow where partial writes are unacceptable or where multiple updates must be consistent.

## Concurrency, Conflicts, and Retries

SSI is on by default (`UniConfig.ssi_enabled = true`). Under SSI, concurrent read-write transactions execute against their own snapshot and are validated at commit. If two transactions write conflicting data (or create a write skew), one commits and the other aborts with `UniError::SerializationConflict`.

- **Retry helpers.** `Session::transact_with_retry` re-runs a transaction closure when the commit fails with a retriable error, and `Session::execute_with_retry` is the single-statement convenience over it. The `is_retriable` classifier decides which errors trigger a retry — `SerializationConflict` is retriable, plain timeouts are not.
- **`ssi_enabled` toggle.** Setting `UniConfig.ssi_enabled = false` reverts to the legacy single-writer last-write-wins (LWW) behavior, where concurrent writers silently overwrite each other rather than aborting. Leave it on unless you specifically need the old semantics.
- **`commit_timeout`.** `UniConfig.commit_timeout` (default 5s) bounds how long a commit waits before failing, guarding against a likely deadlock or long-held lock.

```rust
use uni_db::{RetryOptions, Uni};

# async fn demo() -> Result<(), uni_db::UniError> {
let db = Uni::open("./my_db").build().await?;
let session = db.session();

// Conflicting writers abort + retry transparently instead of locking.
session
    .execute_with_retry("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
    .await?;

// Or wrap a multi-step transaction:
session
    .transact_with_retry(RetryOptions::default(), |tx| {
        Box::pin(async move {
            tx.execute("MATCH (a:Account {id: 1}) SET a.balance = a.balance - 10").await?;
            tx.execute("MATCH (b:Account {id: 2}) SET b.balance = b.balance + 10").await?;
            Ok(())
        })
    })
    .await?;
# Ok(())
# }
```
