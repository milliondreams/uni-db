//! Deterministic seed graph for the metamorphic oracles.
//!
//! [`build_seed`] builds a small, fixed-schema in-memory database. The schema
//! deliberately includes **nullable** properties carrying **real NULLs**: the
//! TLP `WHERE p IS NULL` partition (three-valued logic) is the whole point of
//! the oracle, and it is only exercised when comparisons can actually evaluate
//! to NULL.
//!
//! The graph is rebuilt fresh for every metamorphic comparison so that the
//! sub-queries of one comparison run against identical state (stable node/edge
//! ids — see `diff`) and so cases never leak into each other.
//!
//! # Shape
//!
//! - `Person { name (NOT NULL), age?, score?, city? }` — 12 rows, with `age`
//!   NULL on 2, `score` NULL on 3, `city` NULL on 2; `age = 30` repeats four
//!   times and `score = 0.5` repeats, so bags carry multiplicity > 1.
//! - `Company { name (NOT NULL), founded? }` — 4 rows, `founded` NULL on 1.
//! - `(:Person)-[:WORKS_AT]->(:Company)` — 10 edges; two Persons are edge-less
//!   so relationship patterns drop them (exercised, not a bug).

use uni_db::{DataType, Uni};

/// Boundary value for `age` reused by generated predicates (`age > 30` etc.).
///
/// Several Persons sit exactly on it so off-by-one partitioning (`<` vs `<=`)
/// is caught.
pub const AGE_BOUNDARY: i64 = 30;

/// Write statements seeding the fixed graph, executed in one transaction.
///
/// Omitting a nullable property in `CREATE` leaves it genuinely NULL — that is
/// how the `IS NULL` partition gets non-empty input.
const SEED_STATEMENTS: &[&str] = &[
    // Persons: 12 rows, mixing present and NULL nullable props plus duplicates.
    "CREATE (:Person {name: 'p1', age: 30, score: 0.5, city: 'NYC'})",
    "CREATE (:Person {name: 'p2', age: 25, score: 0.0, city: 'SF'})",
    "CREATE (:Person {name: 'p3', age: 30, score: 0.5, city: 'NYC'})",
    "CREATE (:Person {name: 'p4', age: 40, city: 'SF'})", // score NULL
    "CREATE (:Person {name: 'p5', score: 0.9, city: 'NYC'})", // age NULL
    "CREATE (:Person {name: 'p6', age: 18, score: 0.2})", // city NULL
    "CREATE (:Person {name: 'p7', age: 50, score: 1.0, city: 'SF'})",
    "CREATE (:Person {name: 'p8'})", // age, score, city all NULL
    "CREATE (:Person {name: 'p9', age: 30, score: 0.7, city: 'LA'})",
    "CREATE (:Person {name: 'p10', age: 22, score: 0.5, city: 'NYC'})",
    "CREATE (:Person {name: 'p11', age: 65, city: 'SF'})", // score NULL
    "CREATE (:Person {name: 'p12', age: 30, score: 0.5, city: 'NYC'})",
    // Companies: 4 rows, one with NULL `founded`.
    "CREATE (:Company {name: 'c1', founded: 1999})",
    "CREATE (:Company {name: 'c2', founded: 2010})",
    "CREATE (:Company {name: 'c3'})", // founded NULL
    "CREATE (:Company {name: 'c4', founded: 1999})",
    // WORKS_AT edges: 10. Persons p8 and p12 are intentionally edge-less.
    "MATCH (p:Person {name: 'p1'}), (c:Company {name: 'c1'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p2'}), (c:Company {name: 'c2'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p3'}), (c:Company {name: 'c1'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p4'}), (c:Company {name: 'c3'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p5'}), (c:Company {name: 'c2'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p6'}), (c:Company {name: 'c1'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p7'}), (c:Company {name: 'c4'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p9'}), (c:Company {name: 'c2'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p10'}), (c:Company {name: 'c1'}) CREATE (p)-[:WORKS_AT]->(c)",
    "MATCH (p:Person {name: 'p11'}), (c:Company {name: 'c4'}) CREATE (p)-[:WORKS_AT]->(c)",
];

/// Builds the deterministic seed database.
///
/// # Errors
///
/// Returns any error from database construction, schema application, or the seed
/// writes.
pub async fn build_seed() -> anyhow::Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int)
        .property_nullable("score", DataType::Float)
        .property_nullable("city", DataType::String)
        .done()
        .label("Company")
        .property("name", DataType::String)
        .property_nullable("founded", DataType::Int)
        .done()
        .edge_type("WORKS_AT", &["Person"], &["Company"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for stmt in SEED_STATEMENTS {
        tx.execute(stmt).await?;
    }
    tx.commit().await?;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::build_seed;

    async fn scalar_count(db: &uni_db::Uni, query: &str) -> anyhow::Result<i64> {
        let r = db.session().query(query).await?;
        Ok(r.rows()[0].get::<i64>("c")?)
    }

    #[tokio::test]
    async fn seed_has_expected_cardinalities_and_real_nulls() -> anyhow::Result<()> {
        let db = build_seed().await?;
        assert_eq!(
            scalar_count(&db, "MATCH (p:Person) RETURN count(p) AS c").await?,
            12,
            "12 persons"
        );
        assert_eq!(
            scalar_count(&db, "MATCH (c:Company) RETURN count(c) AS c").await?,
            4,
            "4 companies"
        );
        assert_eq!(
            scalar_count(
                &db,
                "MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) AS c"
            )
            .await?,
            10,
            "10 WORKS_AT edges"
        );
        // The reason the fixture exists: nullable props must carry real NULLs so
        // the TLP `IS NULL` partition is non-vacuous.
        assert_eq!(
            scalar_count(
                &db,
                "MATCH (p:Person) WHERE p.age IS NULL RETURN count(p) AS c"
            )
            .await?,
            2,
            "age NULL on 2 persons"
        );
        assert_eq!(
            scalar_count(
                &db,
                "MATCH (p:Person) WHERE p.score IS NULL RETURN count(p) AS c"
            )
            .await?,
            3,
            "score NULL on 3 persons"
        );
        assert_eq!(
            scalar_count(
                &db,
                "MATCH (p:Person) WHERE p.city IS NULL RETURN count(p) AS c"
            )
            .await?,
            2,
            "city NULL on 2 persons"
        );
        Ok(())
    }
}
