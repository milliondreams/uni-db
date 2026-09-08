// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Comparing two entities is an identity test, and costs identity (#215).
//!
//! IC9's root scan projected every declared column plus `_all_props` and
//! `overflow_json` for a query that reads nothing off `root` — the only use is
//! `WHERE NOT friend = root`. The property collector widened both operands
//! because a bare variable means "the whole entity".
//!
//! Removing that widening alone is *not* safe, and the issue said so: confirm
//! the comparison really lowers to an identity test first. Measured, it lowers
//! on the traversal path and did **not** on a cross join, where the equi-join
//! classifier splits `a = b` into one key per side and compiles each against a
//! context holding only its own variable — so neither compiler could see that
//! both sides were entities, and each asked for a whole-entity column. With the
//! widening gone that query failed outright:
//!
//! ```text
//! Schema error: No field named a. Valid fields are "a._vid", "a._labels".
//! ```
//!
//! So the join key is rewritten to identity too, and the widening is dropped on
//! top of that. These tests pin the answers, because the failure mode the issue
//! warned about is a wrong one, not a loud one.

use uni_db::{DataType, Uni, Value};

/// Two `:P` nodes with **identical properties** and different identity.
///
/// The fixture is the experiment. Under identity semantics `a = b` over the 2x2
/// product matches each node with itself and yields 2; under structural
/// comparison every pair matches and it yields 4. A fixture whose nodes differ
/// cannot separate those — both hypotheses predict 2.
async fn twins() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property("bio", DataType::String)
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:P {id: 1, bio: 'same'}), (:P {id: 1, bio: 'same'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

async fn count_of(db: &Uni, cypher: &str) -> i64 {
    let r = db
        .session()
        .query(cypher)
        .await
        .unwrap_or_else(|e| panic!("{cypher}: {e}"));
    match r.rows()[0].values()[0] {
        Value::Int(i) => i,
        ref other => panic!("expected a count, got {other:?}"),
    }
}

/// Equality over a cross join compares identity, not properties.
///
/// The shape that broke when the widening was removed without the join-key
/// rewrite, and the one where identity and structure disagree.
#[tokio::test]
async fn entity_equality_over_a_cross_join_is_identity() {
    let db = twins().await;
    assert_eq!(
        count_of(&db, "MATCH (a:P), (b:P) WHERE a = b RETURN count(*) AS c").await,
        2,
        "two nodes with identical properties must equal only themselves; 4 \
         would mean the join compares structure, and the widening this issue \
         removes would have been load-bearing"
    );
}

/// `<>` is the complement over the same product.
#[tokio::test]
async fn entity_inequality_over_a_cross_join_is_the_complement() {
    let db = twins().await;
    assert_eq!(
        count_of(
            &db,
            "MATCH (a:P), (b:P) WHERE NOT a = b RETURN count(*) AS c"
        )
        .await,
        2
    );
    assert_eq!(
        count_of(&db, "MATCH (a:P), (b:P) WHERE a <> b RETURN count(*) AS c").await,
        2
    );
}

/// An operand needed elsewhere still hydrates.
///
/// The skip applies to the comparison, not to the variable: `a` is also
/// returned whole here and must come back as a node with its properties.
#[tokio::test]
async fn an_operand_returned_whole_still_hydrates() {
    let db = twins().await;
    let r = db
        .session()
        .query("MATCH (a:P), (b:P) WHERE a = b RETURN a.bio AS bio, a AS node")
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 2);
    for row in r.rows() {
        assert_eq!(row.values()[0], Value::String("same".to_string()));
        assert!(matches!(row.values()[1], Value::Node(_)));
    }
}

/// IC9's own shape: a traversal with `WHERE NOT friend = root`.
#[tokio::test]
async fn the_ic9_shape_answers_correctly() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {id: 1}), (:P {id: 2}), (:P {id: 3})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {id:1}), (y:P {id:2}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {id:2}), (y:P {id:3}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Two hops from id:1 reaches id:2 and id:3; `root` itself is excluded by
    // the comparison, which is the only thing the query does with it.
    assert_eq!(
        count_of(
            &db,
            "MATCH (root:P {id:1})-[:KNOWS*1..2]-(friend:P) \
             WHERE NOT friend = root RETURN count(DISTINCT friend) AS c"
        )
        .await,
        2
    );
}

/// A node compared to a relationship: the pair the identity rewrite declines.
///
/// Different kinds, so neither the join-key rewrite nor
/// `compile_binary_op_dispatch` applies and the generic path answers. It must
/// still be false, and must not need either side's properties.
#[tokio::test]
async fn a_node_compared_to_a_relationship_is_never_equal() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE EDGE TYPE R FROM P TO P").await.unwrap();
    tx.execute("CREATE (:P {id: 1}), (:P {id: 2})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {id:1}), (y:P {id:2}) CREATE (x)-[:R]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        count_of(
            &db,
            "MATCH (a:P)-[r:R]->(b:P) WHERE a = r RETURN count(*) AS c"
        )
        .await,
        0
    );
    assert_eq!(
        count_of(
            &db,
            "MATCH (a:P)-[r:R]->(b:P) WHERE NOT a = r RETURN count(*) AS c"
        )
        .await,
        1
    );
}

/// An entity compared to a non-entity is a planning error, before and after.
///
/// Not a regression this introduced: `a = 1` fails the same way with the whole
/// schema materialised. Pinned so the skip is never blamed for it, and so the
/// day someone makes it return `false` they come here first.
#[tokio::test]
async fn an_entity_compared_to_a_literal_is_refused() {
    let db = twins().await;
    let err = db
        .session()
        .query("MATCH (a:P) WHERE a = 1 RETURN count(*) AS c")
        .await
        .err()
        .expect("comparing a node to an integer is a planning error");
    assert!(
        err.to_string().contains("common argument type"),
        "unexpected failure: {err}"
    );
}
