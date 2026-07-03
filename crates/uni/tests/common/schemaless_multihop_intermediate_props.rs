//! Regression: in SCHEMALESS mode, a multi-hop traversal dropped the properties
//! of the *intermediate* node (the hop-1 target that becomes hop-2's source),
//! returning an empty/`{}` node. Single-hop targets were unaffected (issue #135
//! fixed that for the typed path), so the failure only surfaced for `(x)->(y)->(z)`.
//!
//! Root cause: the schemaless `_all_props` wildcard collapsed to an empty property
//! name list (the code enumerated schema-declared names, and schemaless declares
//! none), so `get_batch_vertex_props` fetched nothing for the wildcard target.
//! Mirrors the openCypher TCK `MatchWhere6 [7]/[8]` scenarios, plus an explicit
//! `flush()` to exercise the main-table (`props_json`) fallback in addition to
//! the in-memory L0 overlay.

use uni_db::{Uni, Value};

const CREATE: &str = "CREATE \
    (:X {val: 1})-[:E1]->(:Y {val: 2})-[:E2]->(:Z {val: 3}), \
    (:X {val: 4})-[:E1]->(:Y {val: 5}), \
    (:X {val: 6})";

// Two-hop through the intermediate `y`, projected as a WHOLE NODE (`RETURN y`),
// which drives the `_all_props` wildcard end to end. `y` is a hop-1 target and
// hop-2 source. Only the X{1}->Y{2}->Z{3} chain satisfies `x.val < z.val`.
const TWO_HOP: &str = "MATCH (x:X) \
    OPTIONAL MATCH (x)-[:E1]->(y:Y)-[:E2]->(z:Z) \
    WHERE x.val < z.val \
    RETURN y AS y";

async fn build() -> anyhow::Result<Uni> {
    // No schema declared -> schemaless storage (props ride in the main table's
    // props_json / L0, not in typed per-label columns).
    let db = Uni::temporary().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(CREATE).await?;
    tx.commit().await?;
    Ok(db)
}

/// Collect the `val` property of each returned intermediate node `y` (ignoring
/// the NULL rows for chains without a matching two-hop path).
async fn intermediate_vals(db: &Uni) -> anyhow::Result<Vec<Option<i64>>> {
    let rows = db.session().query(TWO_HOP).await?;
    Ok(rows
        .rows()
        .iter()
        .filter_map(|r| match r.value("y") {
            Some(Value::Node(n)) => Some(match n.properties.get("val") {
                Some(Value::Int(v)) => Some(*v),
                _ => None,
            }),
            _ => None,
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn schemaless_multihop_intermediate_props_survive_flush() -> anyhow::Result<()> {
    let db = build().await?;

    // Pre-flush: the intermediate node's props are served from the L0 overlay.
    let pre = intermediate_vals(&db).await?;
    assert!(
        pre.contains(&Some(2)),
        "pre-flush: intermediate node y must carry val=2, got {pre:?}"
    );

    // Force L0 -> Lance so the props now live in the main table's props_json.
    db.flush().await?;

    let post = intermediate_vals(&db).await?;
    assert!(
        post.contains(&Some(2)),
        "post-flush: intermediate node y must survive the flush, got {post:?}"
    );
    Ok(())
}
