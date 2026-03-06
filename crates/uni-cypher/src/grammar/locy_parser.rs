use pest_derive::Parser;

/// Locy parser: stacks `locy.pest` on top of `cypher.pest`.
///
/// This generates a `Rule` enum that is a superset of CypherParser's Rule,
/// containing both Cypher and Locy grammar rules. The two `Rule` enums are
/// distinct Rust types — locy_walker bridges between them by re-parsing
/// embedded Cypher text spans with CypherParser when needed.
#[derive(Parser)]
#[grammar = "grammar/cypher.pest"]
#[grammar = "grammar/locy.pest"]
pub struct LocyParser;
