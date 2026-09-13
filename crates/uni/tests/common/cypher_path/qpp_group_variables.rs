//! GQL group variables in quantified path patterns.
//!
//! Every variable declared inside a quantified pattern binds a *list* with one
//! element per iteration of the quantifier — `x`, `y`, `r` and `s` in
//! `((x)-[r:E]->(y)-[s:L]->(z)){2}` each hold two elements. The pattern's
//! endpoints are the adjacent outer nodes, never the inner variables.
//!
//! The fixture gives every node and every edge a distinct value so that an
//! off-by-one in a stride, or two same-typed group columns transposed, changes
//! the answer rather than passing silently.

use anyhow::Result;
use uni_db::{DataType, Uni};
use uni_query::Value;

/// Chain `1 -[e1]-> 2 -[l1]-> 3 -[e2]-> 4 -[l2]-> 5`, alternating edge types so
/// a two-hop sub-pattern `(-[:E]->()-[:L]->())` iterates cleanly twice.
async fn chain_fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .apply()
        .await?;
    db.schema()
        .edge_type("E", &["N"], &["N"])
        .property("tag", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("L", &["N"], &["N"])
        .property("tag", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n1:N {id: 1}), (n2:N {id: 2}), (n3:N {id: 3}), (n4:N {id: 4}), (n5:N {id: 5}) \
         CREATE (n1)-[:E {tag: 'e1'}]->(n2), (n2)-[:L {tag: 'l1'}]->(n3), \
                (n3)-[:E {tag: 'e2'}]->(n4), (n4)-[:L {tag: 'l2'}]->(n5)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

fn ints(val: &Value, what: &str) -> Vec<i64> {
    let Value::List(items) = val else {
        panic!("expected a list for {what}, got {val:?}");
    };
    items
        .iter()
        .map(|v| match v {
            Value::Int(i) => *i,
            other => panic!("expected an int in {what}, got {other:?}"),
        })
        .collect()
}

fn strings(val: &Value, what: &str) -> Vec<String> {
    let Value::List(items) = val else {
        panic!("expected a list for {what}, got {val:?}");
    };
    items
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => panic!("expected a string in {what}, got {other:?}"),
        })
        .collect()
}

/// The load-bearing test for the offset arithmetic. With `hops_per_iter = 2`,
/// the sub-pattern's last node position of one iteration is its first node
/// position of the next, so `x` and `z` legitimately overlap by one element.
/// An implementation that collects "one node per hop" gets this wrong.
#[tokio::test]
async fn qpp_node_group_variables_stride_and_overlap() -> Result<()> {
    let db = chain_fixture().await?;

    let result = db
        .session()
        .query(
            "MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) \
             RETURN [n IN x | n.id] AS xs, [n IN y | n.id] AS ys, [n IN z | n.id] AS zs",
        )
        .await?;

    assert_eq!(result.rows().len(), 1);
    let row = &result.rows()[0];
    let xs = ints(row.value("xs").unwrap(), "xs");
    let ys = ints(row.value("ys").unwrap(), "ys");
    let zs = ints(row.value("zs").unwrap(), "zs");

    assert_eq!(xs, vec![1, 3], "iteration sources");
    assert_eq!(ys, vec![2, 4], "mid-iteration nodes");
    assert_eq!(zs, vec![3, 5], "iteration targets");
    assert_eq!(
        zs[0], xs[1],
        "one iteration's target is the next one's source"
    );

    Ok(())
}

/// Each relationship position is its own group variable, so the two must not
/// carry the same values — this is what an off-by-one in the edge stride, or a
/// transposition of the two same-typed columns, would break.
#[tokio::test]
async fn qpp_edge_group_variables_are_per_position() -> Result<()> {
    let db = chain_fixture().await?;

    let result = db
        .session()
        .query(
            "MATCH (s:N {id: 1})((x)-[r:E]->(y)-[q:L]->(z)){2}(t:N) \
             RETURN [e IN r | e.tag] AS rs, [e IN q | e.tag] AS qs",
        )
        .await?;

    assert_eq!(result.rows().len(), 1);
    let row = &result.rows()[0];
    assert_eq!(strings(row.value("rs").unwrap(), "rs"), vec!["e1", "e2"]);
    assert_eq!(strings(row.value("qs").unwrap(), "qs"), vec!["l1", "l2"]);

    Ok(())
}

/// Every group variable holds exactly one element per iteration, whatever the
/// position — and the count tracks the quantifier.
#[tokio::test]
async fn qpp_group_variable_size_tracks_iteration_count() -> Result<()> {
    let db = chain_fixture().await?;

    let result = db
        .session()
        .query(
            "MATCH (s:N {id: 1})((x)-[r:E]->(y)-[q:L]->(z)){1,2}(t:N) \
             RETURN size(x) AS sx, size(y) AS sy, size(z) AS sz, \
                    size(r) AS sr, size(q) AS sq, t.id AS target \
             ORDER BY target",
        )
        .await?;

    assert_eq!(
        result.rows().len(),
        2,
        "one row per admissible iteration count"
    );
    for (row, want) in result.rows().iter().zip([1i64, 2]) {
        for col in ["sx", "sy", "sz", "sr", "sq"] {
            assert_eq!(
                row.get::<i64>(col)?,
                want,
                "{col} for target {:?}",
                row.get::<i64>("target")?
            );
        }
    }
    assert_eq!(result.rows()[0].get::<i64>("target")?, 3);
    assert_eq!(result.rows()[1].get::<i64>("target")?, 5);

    Ok(())
}

/// Returning the group variable whole goes through the result normalizer, which
/// must recognise the elements as nodes and decode their properties. Asserted
/// after a flush, where the properties come from storage rather than L0.
#[tokio::test]
async fn qpp_group_variable_returns_nodes_after_flush() -> Result<()> {
    let db = chain_fixture().await?;
    db.flush().await?;

    let result = db
        .session()
        .query("MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) RETURN y AS ys")
        .await?;

    assert_eq!(result.rows().len(), 1);
    let Value::List(items) = result.rows()[0].value("ys").unwrap() else {
        panic!("expected a list");
    };
    assert_eq!(items.len(), 2);
    let ids: Vec<i64> = items
        .iter()
        .map(|v| match v {
            Value::Node(n) => match n.properties.get("id") {
                Some(Value::Int(i)) => *i,
                other => panic!("expected an int id, got {other:?}"),
            },
            other => panic!("expected a Node, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![2, 4]);

    Ok(())
}

/// The pattern's endpoints are the adjacent outer nodes. The inner source's
/// label still constrains the anchor scan even though it no longer names it.
#[tokio::test]
async fn qpp_endpoints_come_from_the_outer_nodes() -> Result<()> {
    let db = chain_fixture().await?;

    let result = db
        .session()
        .query(
            "MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) \
             RETURN s.id AS start, t.id AS end",
        )
        .await?;

    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("start")?, 1);
    assert_eq!(result.rows()[0].get::<i64>("end")?, 5);

    Ok(())
}

/// A quantified pattern with no adjacent outer nodes still matches — the
/// endpoints simply are not nameable. The inner source's label continues to
/// gate the anchor scan.
#[tokio::test]
async fn qpp_without_outer_nodes_still_applies_inner_constraints() -> Result<()> {
    let db = chain_fixture().await?;

    let matched = db
        .session()
        .query("MATCH ((x:N)-[:E]->(y)-[:L]->(z)){1,1} RETURN count(*) AS c")
        .await?;
    assert_eq!(
        matched.rows()[0].get::<i64>("c")?,
        2,
        "two E->L pairs exist"
    );

    // A label no node carries must reduce that to zero, proving the inner
    // source's label is still load-bearing for the anchor scan.
    let unmatched = db
        .session()
        .query("MATCH ((x:Missing)-[:E]->(y)-[:L]->(z)){1,1} RETURN count(*) AS c")
        .await;
    match unmatched {
        Ok(r) => assert_eq!(r.rows()[0].get::<i64>("c")?, 0),
        Err(e) => assert!(
            e.to_string().contains("Missing"),
            "expected an unknown-label error, got {e}"
        ),
    }

    Ok(())
}

/// The diagnostic for property access on a group variable names two
/// replacements. Both must actually work — an error message that suggests a
/// broken workaround is worse than no suggestion.
#[tokio::test]
async fn qpp_group_variable_property_access_is_a_clear_error() -> Result<()> {
    let db = chain_fixture().await?;

    let err = db
        .session()
        .query("MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) RETURN y.id AS ids")
        .await
        .expect_err("property access on a group variable must be refused");
    let msg = err.to_string();
    assert!(msg.contains("group variable"), "unhelpful message: {msg}");
    assert!(
        msg.contains("[item IN y | item.id]"),
        "no workaround named: {msg}"
    );
    assert!(msg.contains("last(y).id"), "no workaround named: {msg}");

    // Workaround 1, as named by the message.
    let comprehension = db
        .session()
        .query("MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) RETURN [item IN y | item.id] AS ids")
        .await?;
    assert_eq!(
        ints(comprehension.rows()[0].value("ids").unwrap(), "ids"),
        vec![2, 4]
    );

    // Workaround 2, as named by the message.
    let last = db
        .session()
        .query("MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) RETURN last(y).id AS id")
        .await?;
    assert_eq!(last.rows()[0].get::<i64>("id")?, 4);

    Ok(())
}

/// Every way of misusing a group variable must be a planner error that names
/// the variable and says what it is — not a silent null, and not a confusing
/// error from deeper in the stack.
#[tokio::test]
async fn qpp_group_variable_misuse_is_diagnosed() -> Result<()> {
    let db = chain_fixture().await?;

    let cases: &[(&str, &str)] = &[
        // Property access on a group node variable.
        (
            "MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) RETURN y.id",
            "group variable",
        ),
        // ...and on a group relationship variable.
        (
            "MATCH (s:N {id: 1})((x)-[r:E]->(y)-[:L]->(z)){2}(t:N) RETURN r.tag",
            "group variable",
        ),
        // Reusing a group variable as a pattern endpoint.
        (
            "MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) MATCH (y)-[:E]->(q) RETURN q.id",
            "y",
        ),
        // Reusing a group relationship variable as a single relationship.
        (
            "MATCH (s:N {id: 1})((x)-[r:E]->(y)-[:L]->(z)){2}(t:N) MATCH (a)-[r]->(b) RETURN a.id",
            "r",
        ),
        // A group variable is not a path.
        (
            "MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(z)){2}(t:N) RETURN nodes(y)",
            "path",
        ),
        // An inner name colliding with an already-bound outer variable.
        (
            "MATCH (s:N {id: 1})((s)-[:E]->(y)-[:L]->(z)){2}(t:N) RETURN t.id",
            "already",
        ),
        // The same name at two inner positions would need two columns with one name.
        (
            "MATCH (s:N {id: 1})((x)-[:E]->(y)-[:L]->(y)){2}(t:N) RETURN t.id",
            "more than one position",
        ),
    ];

    for (query, expected) in cases {
        let err = db
            .session()
            .query(query)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected an error for: {query}"));
        let msg = err.to_string();
        assert!(
            msg.contains(expected),
            "message for `{query}` should mention {expected:?}, got: {msg}"
        );
    }

    Ok(())
}

/// Legitimate uses must all remain legal.
///
/// This ran nine shapes and asserted only that none errored, which cannot tell
/// "supported and working" from "supported and silently empty" (#259). Measured
/// at the time: `y` binds `[2, 4]`, so nothing is empty today and no defect was
/// hiding behind the weak assertion — this is hygiene against a future
/// regression, not a repro.
///
/// The execution loop is kept: it is the cheapest way to hold nine shapes to
/// "still legal", and most of them have exact-value equivalents in the sibling
/// tests above. What is added is the piece with no equivalent anywhere — the
/// negative `all()` twin — plus the two values this fixture fully determines.
#[tokio::test]
async fn qpp_group_variable_supported_uses() -> Result<()> {
    let db = chain_fixture().await?;
    let prefix = "MATCH (s:N {id: 1})((x)-[r:E]->(y)-[:L]->(z)){2}(t:N) ";

    for suffix in [
        "RETURN y",
        "RETURN size(y), size(r)",
        "RETURN [n IN y | n.id] AS ids",
        "RETURN y[0].id AS first",
        "RETURN head(y).id AS first, last(y).id AS lastid",
        "WHERE all(n IN y WHERE n.id > 0) RETURN t.id",
        "WHERE size(y) = 2 RETURN t.id",
        "UNWIND y AS n RETURN n.id ORDER BY n.id",
        "WITH y AS ys RETURN size(ys) AS n",
    ] {
        let query = format!("{prefix}{suffix}");
        db.session()
            .query(&query)
            .await
            .unwrap_or_else(|e| panic!("supported use rejected: {query}\n{e}"));
    }

    // The group variable actually binds, in iteration order.
    let ids = db
        .session()
        .query(&format!("{prefix}RETURN [n IN y | n.id] AS ids"))
        .await?;
    assert_eq!(
        ids.rows()[0].values()[0],
        Value::List(vec![Value::Int(2), Value::Int(4)]),
        "`y` must bind both iterations' nodes in order"
    );

    // Both group variables have one entry per iteration.
    let sizes = db
        .session()
        .query(&format!("{prefix}RETURN size(y) AS sy, size(r) AS sr"))
        .await?;
    assert_eq!(sizes.rows()[0].values()[0], Value::Int(2), "size(y)");
    assert_eq!(sizes.rows()[0].values()[1], Value::Int(2), "size(r)");

    // The satisfiable twin, so the negative below cannot pass for an unrelated
    // reason. Without it, a shape that returned no rows for any cause — a
    // broken prefix, a fixture that never matched — would satisfy the
    // assertion and prove nothing.
    let some = db
        .session()
        .query(&format!(
            "{prefix}WHERE all(n IN y WHERE n.id > 0) RETURN t.id"
        ))
        .await?;
    assert!(
        !some.rows().is_empty(),
        "the satisfiable `all()` shape returned no rows, so the impossible one \
         below would pass whatever the group variable contained"
    );

    // The decisive control, and the one assertion with no equivalent in the
    // sibling tests. `all()` over an empty list is vacuously true, so an empty
    // `y` would make this return rows — the same reason the positive
    // `n.id > 0` shape above cannot distinguish success from emptiness. It is
    // the *negative* twin that carries the claim.
    let none = db
        .session()
        .query(&format!(
            "{prefix}WHERE all(n IN y WHERE n.id > 999) RETURN t.id"
        ))
        .await?;
    assert!(
        none.rows().is_empty(),
        "an impossible predicate over `y` returned {} rows; `all()` is vacuously \
         true over an empty list, so rows here mean the group variable bound \
         nothing and every positive shape above is passing for the wrong reason",
        none.rows().len()
    );

    Ok(())
}

/// Pruning unreferenced group variables changes the BFS strategy — from
/// endpoint-only to full path enumeration — so it must not change the rows.
/// A diamond makes the two strategies disagree if the pruning is unsound:
/// there are two distinct paths between the same endpoints.
#[tokio::test]
async fn qpp_pruning_group_variables_does_not_change_the_row_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .apply()
        .await?;
    db.schema().edge_type("E", &["N"], &["N"]).apply().await?;

    // 1 -> {2, 3} -> 4: two distinct one-iteration-per-hop routes to the same end.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}), (d:N {id: 4}) \
         CREATE (a)-[:E]->(b), (a)-[:E]->(c), (b)-[:E]->(d), (c)-[:E]->(d)",
    )
    .await?;
    tx.commit().await?;

    let ids = |rows: uni_db::QueryResult| -> Vec<i64> {
        let mut v: Vec<i64> = rows
            .rows()
            .iter()
            .map(|r| r.get::<i64>("end").unwrap())
            .collect();
        v.sort_unstable();
        v
    };

    let anonymous = db
        .session()
        .query("MATCH (s:N {id: 1})(()-[:E]->()){2}(t:N) RETURN t.id AS end")
        .await?;
    let named_unused = db
        .session()
        .query("MATCH (s:N {id: 1})((x)-[r:E]->(y)){2}(t:N) RETURN t.id AS end")
        .await?;
    let materialized = db
        .session()
        .query("MATCH (s:N {id: 1})((x)-[r:E]->(y)){2}(t:N) RETURN t.id AS end, size(y) AS n")
        .await?;

    assert_eq!(
        ids(anonymous),
        ids(named_unused),
        "merely naming the inner positions must not change the rows"
    );

    // Projecting a group variable does change the cardinality, and this is the
    // reason the pruning above matters. A quantified pattern's endpoint-only
    // plan collapses distinct paths that share endpoints, so the diamond yields
    // one row; enumerating paths to fill the group column yields the two rows
    // GQL actually specifies (one per distinct binding). That collapse is a
    // pre-existing gap in QPP cardinality, not something group variables
    // introduce — pruning is what keeps it from leaking into queries that do
    // not ask for a group variable.
    assert_eq!(ids(materialized), vec![4, 4], "one row per distinct path");

    Ok(())
}
