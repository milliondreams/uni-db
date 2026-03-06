# Locy Quickstart

This walkthrough shows non-recursive and recursive reasoning, then explains one inferred result.

## Rust Example

```rust
use uni_db::Uni;

# async fn run() -> Result<(), uni_db::UniError> {
let db = Uni::in_memory().build().await?;

db.execute("CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})").await?;
db.execute("MATCH (b:Node {name:'B'}), (c:Node {name:'C'}) CREATE (b)-[:EDGE]->(c)").await?;

let program = r#"
CREATE RULE reachable AS
MATCH (a:Node)-[:EDGE]->(b:Node)
YIELD KEY a, KEY b

CREATE RULE reachable AS
MATCH (a:Node)-[:EDGE]->(mid:Node)
WHERE mid IS reachable TO b
YIELD KEY a, KEY b

QUERY reachable WHERE a.name = 'A' RETURN b.name AS target
"#;

let result = db.locy().evaluate(program).await?;
println!("rows = {:?}", result.rows());
# Ok(())
# }
```

## Python Example

```python
import uni_db

db = uni_db.Database(":memory:")

program = """
CREATE RULE adults AS
MATCH (p:Person)
WHERE p.age >= 18
YIELD KEY p, p.name AS name

QUERY adults RETURN name
"""

out = db.locy_evaluate(program)
print(out["derived"].keys())
print(out["stats"]) 
```

## Explain a Derivation

```cypher
EXPLAIN RULE reachable WHERE a.name = 'A', b.name = 'C'
```

Use this when you need proof-style traceability for compliance or debugging.

## Next

- [Language Guide](language-guide.md)
- [Rule Semantics](rule-semantics.md)
