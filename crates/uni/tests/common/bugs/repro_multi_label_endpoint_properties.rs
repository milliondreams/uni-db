// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for Tier 0 item 0.15 — `properties()` returns `{}` for an unlabelled
//! traversal target when the edge type's declared endpoints span more than one
//! label.
//!
//! Two things compose:
//!
//! 1. `planner.rs::plan_traverse_with_source` collapses a multi-label
//!    destination set to `None` (`if unique_dsts.len() == 1 { .. } else { None }`)
//!    rather than to a union, so the traverse runs its label-agnostic branch.
//! 2. That branch, in `traverse.rs::build_target_property_columns`, filters
//!    `_all_props` out of the requested property list and then — with nothing
//!    left to ask for — **skips the storage read entirely** and returns an empty
//!    map. `build_all_props_column` then appends a null.
//!
//! `_all_props` is not an internal name to be stripped: it is the wildcard
//! `PropertyManager::get_batch_vertex_props` understands, reading declared
//! columns, the overflow blob and the L0 overlay. The sibling that was fixed
//! for issue #135 (`build_edge_adjacency_and_target_props`) passes the sentinel
//! straight through and says so in a comment; that fix reached
//! `GraphTraverseMainStream` and not `GraphTraverseStream`.
//!
//! The result is a **silent wrong answer**: the row is found, `labels(b)` is
//! correct — it comes from the VidLabelsIndex, not the planner's label guess —
//! and only the properties are missing.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Build a graph whose edge type declares both labels on both endpoints.
///
/// Multi-label endpoints are legal and useful — a `TAGGED` edge from several
/// kinds of node — and the declaration is what pushes the traverse onto its
/// label-agnostic path.
async fn seeded_multi_label(db: &Uni) -> Result<()> {
    db.schema()
        .label("Author")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("Book")
        .property("title", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("WROTE", &["Author", "Book"], &["Author", "Book"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Author {name: 'Ursula'})-[:WROTE]->(:Book {title: 'Earthsea'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

#[tokio::test]
async fn properties_of_an_unlabelled_multi_label_endpoint_are_returned() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seeded_multi_label(&db).await?;

    let res = db
        .session()
        .query("MATCH (a:Author)-[:WROTE]->(b) RETURN properties(b) AS p")
        .await?;

    assert_eq!(res.len(), 1, "the row itself was always found");
    let props = res.rows()[0].value("p").cloned();
    let map = match &props {
        Some(Value::Map(m)) => m.clone(),
        other => panic!("expected a property map, got {other:?}"),
    };
    assert_eq!(
        map.get("title").and_then(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        }),
        Some("Earthsea"),
        "properties() dropped the target's properties for an unlabelled \
         endpoint of a multi-label edge type; got {map:?}"
    );

    Ok(())
}

/// The same row's `labels()` was always right — which is what made this silent.
///
/// Label resolution goes through the VidLabelsIndex, so it is immune to the
/// planner collapsing the candidate set. A caller therefore sees a row that is
/// correctly identified and merely appears to have no properties.
#[tokio::test]
async fn labels_of_the_same_row_were_never_wrong() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seeded_multi_label(&db).await?;

    let res = db
        .session()
        .query("MATCH (a:Author)-[:WROTE]->(b) RETURN labels(b) AS l")
        .await?;
    let labels = res.rows()[0].value("l").cloned();
    assert!(
        format!("{labels:?}").contains("Book"),
        "labels(b) must name the target, got {labels:?}"
    );

    Ok(())
}

/// Control: naming the label in the pattern always worked, because that takes
/// the labelled branch.
#[tokio::test]
async fn naming_the_label_still_works() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seeded_multi_label(&db).await?;

    let res = db
        .session()
        .query("MATCH (a:Author)-[:WROTE]->(b:Book) RETURN properties(b) AS p")
        .await?;
    assert!(
        format!("{:?}", res.rows()[0].value("p")).contains("Earthsea"),
        "the labelled form was never affected"
    );

    Ok(())
}

/// Control: single-label endpoints always worked, because the planner keeps the
/// label and the labelled branch is taken.
#[tokio::test]
async fn single_label_endpoints_still_work() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Author")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("Book")
        .property("title", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("WROTE", &["Author"], &["Book"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Author {name: 'Ursula'})-[:WROTE]->(:Book {title: 'Earthsea'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (a:Author)-[:WROTE]->(b) RETURN properties(b) AS p")
        .await?;
    assert!(
        format!("{:?}", res.rows()[0].value("p")).contains("Earthsea"),
        "precisely-declared endpoints were never affected"
    );

    Ok(())
}

/// The same defect existed a second time, in the variable-length-path
/// hydration (`hydrate_vlp_target_properties`).
///
/// Both sites stripped `_all_props` and then skipped the storage read. Fixing
/// only the single-hop one would have left `MATCH (a)-[:R*1..2]->(b)` still
/// returning propertyless targets, so this pins the VLP path separately.
#[tokio::test]
async fn properties_survive_a_variable_length_path_to_an_unlabelled_target() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seeded_multi_label(&db).await?;

    let res = db
        .session()
        .query("MATCH (a:Author)-[:WROTE*1..2]->(b) RETURN properties(b) AS p")
        .await?;

    assert_eq!(res.len(), 1, "the row itself is found");
    assert!(
        format!("{:?}", res.rows()[0].value("p")).contains("Earthsea"),
        "a variable-length path to an unlabelled target must carry properties too; \
         got {:?}",
        res.rows()[0].value("p")
    );

    Ok(())
}
