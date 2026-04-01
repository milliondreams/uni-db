# Snapshots & Time Travel

Uni supports point-in-time snapshots and time-travel queries. Snapshots are durable checkpoints; time travel lets you query historical versions without mutating state.

## What It Provides

- Create, list, and restore snapshots.
- `VERSION AS OF` and `TIMESTAMP AS OF` queries for historical reads.
- Read-only time-travel safety checks.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let snap_id = db.create_snapshot(Some("daily")).await?;
    let rows = session.query(&format!(
        "MATCH (n) RETURN count(n) AS c VERSION AS OF '{}'",
        snap_id
    )).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()

    # Snapshot via procedure
    rows = session.query("CALL uni.admin.snapshot.create('daily') YIELD snapshot_id RETURN snapshot_id")
    snap_id = rows[0]["snapshot_id"]

    rows = session.query(
        f"MATCH (n) RETURN count(n) AS c VERSION AS OF '{snap_id}'"
    )
    print(rows)
    ```

## Use Cases

- Auditing and reproducible analytics.
- Debugging or regression analysis.
- Checkpointing before bulk operations.

## When To Use

Use snapshots and time travel when you need historical reads or safe rollbacks without exporting or duplicating data.
