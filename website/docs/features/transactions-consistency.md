# Transactions & Consistency

Uni provides ACID transactions with snapshot isolation. Reads see a consistent snapshot, and writes are applied atomically on commit. Uni uses commit-time serialization for simplicity and predictable performance.

## What It Provides

- Snapshot isolation for consistent reads.
- Commit-time serialization with concurrent transaction preparation.
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
- Predictable concurrency without distributed locking.

## When To Use

Use transactions for any workflow where partial writes are unacceptable or where multiple updates must be consistent.
