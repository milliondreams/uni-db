# OpenCypher Querying

Uni uses OpenCypher as its primary query language, with extensions for vector search, time travel, and admin procedures. This keeps graph queries expressive and readable.

## What It Provides

- Pattern matching for nodes and relationships, including `OPTIONAL MATCH` for left-outer-join semantics.
- Aggregations, ordering, filtering, and window functions (`OVER` clause).
- `MERGE` for upsert operations with `ON CREATE SET` / `ON MATCH SET`.
- `SET` / `REMOVE` for updating properties and labels on existing nodes and relationships.
- Procedure calls for algorithms, snapshots, and indexes.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let results = db
        .query_with("MATCH (p:Person) WHERE p.age > $min RETURN p.name")
        .param("min", 30)
        .fetch_all()
        .await?;

    println!("{:?}", results);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    results = db.query_with(
        "MATCH (p:Person) WHERE p.age > $min RETURN p.name"
    ).param("min", 30).fetch_all()

    print(results)
    ```

## Use Cases

- Relationship queries and graph traversals.
- Pattern matching with filters and aggregations.
- Admin and metadata procedures.

## When To Use

OpenCypher is the best choice when your data model is graph-shaped and you need expressive relationship queries, not just key-value lookups or SQL tables.

## Locy Integration

When you need recursive rule-based reasoning, hypothetical analysis, or abductive remediation workflows, use Locy on top of Cypher.

- Start at [Locy Overview](../locy/index.md)
- Learn syntax in [Locy Language Guide](../locy/language-guide.md)
- See advanced workflows in [DERIVE / ASSUME / ABDUCE](../locy/advanced/derive-assume-abduce.md)
