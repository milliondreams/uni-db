use std::collections::HashMap;
use uni_query::{Edge, Node, Path, QueryResult, Value};

const FLOAT_EPSILON: f64 = 1e-10;

/// Value equality comparator: `fn(actual, expected) -> bool`.
type Comparator = fn(&Value, &Value) -> bool;

/// Match query result against expected rows in row order, using `eq` to
/// compare cell values.
fn match_result_ordered_with(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
    eq: Comparator,
) -> Result<(), String> {
    if actual.len() != expected_rows.len() {
        return Err(format!(
            "Row count mismatch: expected {}, got {}",
            expected_rows.len(),
            actual.len()
        ));
    }

    if actual.is_empty() {
        return Ok(());
    }

    let expected_cols = validate_columns(actual, expected_rows)?;

    for (i, (actual_row, expected_row)) in
        actual.rows().iter().zip(expected_rows.iter()).enumerate()
    {
        for col in &expected_cols {
            let Some(expected_val) = expected_row.get(col) else {
                return Err(format!("Expected row {} missing column {}", i, col));
            };
            let Some(actual_val) = actual_row.value(col) else {
                return Err(format!("Actual row {} missing column {}", i, col));
            };
            if !eq(actual_val, expected_val) {
                return Err(format!(
                    "Row {} column {} mismatch: expected {:?}, got {:?}",
                    i, col, expected_val, actual_val
                ));
            }
        }
    }

    Ok(())
}

/// Match query result against expected rows in any order, using `eq` to
/// compare cell values.
fn match_result_unordered_with(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
    eq: Comparator,
) -> Result<(), String> {
    if actual.len() != expected_rows.len() {
        return Err(format!(
            "Row count mismatch: expected {}, got {}",
            expected_rows.len(),
            actual.len()
        ));
    }

    if actual.is_empty() {
        return Ok(());
    }

    let expected_cols = validate_columns(actual, expected_rows)?;

    let mut unmatched: Vec<&HashMap<String, Value>> = expected_rows.iter().collect();

    for (i, actual_row) in actual.rows().iter().enumerate() {
        let match_idx = unmatched.iter().position(|expected_row| {
            expected_cols.iter().all(|col| {
                let Some(expected_val) = expected_row.get(col) else {
                    return false;
                };
                let Some(actual_val) = actual_row.value(col) else {
                    return false;
                };
                eq(actual_val, expected_val)
            })
        });

        match match_idx {
            Some(idx) => {
                unmatched.remove(idx);
            }
            None => {
                let actual_vals: Vec<_> = expected_cols
                    .iter()
                    .map(|col| (col.clone(), actual_row.value(col).cloned()))
                    .collect();
                return Err(format!(
                    "No match found for actual row {}. Actual values: {:?}. Expected: {:?}",
                    i, actual_vals, unmatched
                ));
            }
        }
    }

    if !unmatched.is_empty() {
        return Err(format!(
            "{} expected rows were not matched",
            unmatched.len()
        ));
    }

    Ok(())
}

/// Match query result against expected rows (order-sensitive).
pub fn match_result(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    match_result_ordered_with(actual, expected_rows, values_equal)
}

/// Match query result against expected rows (order-agnostic).
pub fn match_result_unordered(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    match_result_unordered_with(actual, expected_rows, values_equal)
}

/// Match query result against expected rows (row-order-sensitive),
/// ignoring the order of elements within each list value.
///
/// "List order" in the name refers to ordering of elements *within* list
/// cells, not the order of rows. Row order is preserved (zip-and-compare
/// against `expected_rows`); only equality of [`Value::List`] cells is
/// relaxed via [`values_equal_ignoring_list_order`].
pub fn match_result_ignoring_list_order(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    match_result_ordered_with(actual, expected_rows, values_equal_ignoring_list_order)
}

/// Match query result against expected rows (order-agnostic), ignoring list element order.
pub fn match_result_unordered_ignoring_list_order(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<(), String> {
    match_result_unordered_with(actual, expected_rows, values_equal_ignoring_list_order)
}

/// Validate that actual and expected column sets match, returning the expected column names.
fn validate_columns(
    actual: &QueryResult,
    expected_rows: &[HashMap<String, Value>],
) -> Result<Vec<String>, String> {
    let expected_cols: Vec<String> = expected_rows[0].keys().cloned().collect();
    let actual_cols: Vec<String> = actual.columns().to_vec();

    for col in &expected_cols {
        if !actual_cols.contains(col) {
            return Err(format!("Expected column '{}' not found in result", col));
        }
    }
    for col in &actual_cols {
        if !expected_cols.contains(col) {
            return Err(format!("Unexpected column '{}' in result", col));
        }
    }

    Ok(expected_cols)
}

/// Compare two values for equality with special handling for floats and graph types.
fn values_equal(a: &Value, b: &Value) -> bool {
    values_equal_inner(a, b, false)
}

/// Compare two values for equality with special handling for floats, graph types,
/// and ignoring element order within lists.
fn values_equal_ignoring_list_order(a: &Value, b: &Value) -> bool {
    values_equal_inner(a, b, true)
}

/// Shared value-equality core. When `ignore_list_order` is set, [`Value::List`]
/// cells are sorted before element-wise comparison; otherwise list order is
/// significant. The flag is threaded recursively so it applies at every depth.
fn values_equal_inner(a: &Value, b: &Value, ignore_list_order: bool) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => floats_equal(*a, *b),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Temporal(a), Value::Temporal(b)) => a == b,
        (Value::List(a), Value::List(b)) => {
            if a.len() != b.len() {
                return false;
            }
            if ignore_list_order {
                let mut a_sorted: Vec<&Value> = a.iter().collect();
                let mut b_sorted: Vec<&Value> = b.iter().collect();
                a_sorted.sort_by_key(|v| value_sort_key(v));
                b_sorted.sort_by_key(|v| value_sort_key(v));
                a_sorted
                    .iter()
                    .zip(b_sorted.iter())
                    .all(|(av, bv)| values_equal_inner(av, bv, ignore_list_order))
            } else {
                a.iter()
                    .zip(b.iter())
                    .all(|(av, bv)| values_equal_inner(av, bv, ignore_list_order))
            }
        }
        (Value::Map(a), Value::Map(b)) => maps_equal(a, b, ignore_list_order),
        (Value::Node(a), Value::Node(b)) => nodes_equal(a, b, ignore_list_order),
        (Value::Edge(a), Value::Edge(b)) => edges_equal(a, b, ignore_list_order),
        (Value::Path(a), Value::Path(b)) => paths_equal(a, b, ignore_list_order),
        (Value::Vector(a), Value::Vector(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(av, bv)| (av - bv).abs() < FLOAT_EPSILON as f32)
        }
        // openCypher result tables render temporal/BTIC values as quoted strings
        // (e.g. `'PT22H'`, `'1984-10-11'`) that are byte-identical to genuine string
        // results such as `toString(d)`. The reference comparison is therefore
        // rendering-based for temporals: a temporal matches its canonical string
        // rendering (and vice versa). Numeric types stay strict above — `1` and
        // `1.0` are syntactically distinct, typed cells in the corpus.
        (Value::Temporal(t), Value::String(s)) | (Value::String(s), Value::Temporal(t)) => {
            t.to_string() == *s
        }
        _ => false,
    }
}

fn floats_equal(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a.is_infinite() && b.is_infinite() {
        return a.is_sign_positive() == b.is_sign_positive();
    }
    (a - b).abs() < FLOAT_EPSILON
}

/// Generate a content-distinguishing sort key for a value.
///
/// This key drives the order-independent list comparison in
/// [`values_equal_inner`]: both lists are sorted by this key and then zipped
/// element-wise, so two values that are equal under [`values_equal_inner`] MUST
/// produce the same key (otherwise permuted-but-equal elements fail to align).
///
/// The key encodes structural content, not identity: node/edge/path keys omit
/// vids, eids and label ordering (mirroring `nodes_equal`/`edges_equal`), and
/// list/map keys are built order-independently so that permutations collapse to
/// the same key — consistent with the recursive `ignore_list_order` policy under
/// which this key is used.
fn value_sort_key(v: &Value) -> String {
    match v {
        Value::Null => "0:null".to_string(),
        Value::Bool(b) => format!("1:{}", b),
        Value::Int(i) => format!("2:{:020}", i),
        Value::Float(f) => format!("3:{:020.10}", f),
        Value::String(s) => format!("4:{}", s),
        Value::Bytes(b) => format!("5:{:?}", b),
        Value::List(l) => {
            // Order-independent: sort child keys so permuted lists collapse.
            let mut inner: Vec<String> = l.iter().map(value_sort_key).collect();
            inner.sort();
            format!("6:[{}]", inner.join("\u{1f}"))
        }
        Value::Map(m) => format!("7:{{{}}}", map_sort_key(m)),
        Value::Node(n) => format!("8:{}", node_sort_key(n)),
        Value::Edge(e) => format!("9:{}", edge_sort_key(e)),
        Value::Path(p) => format!("A:{}", path_sort_key(p)),
        Value::Temporal(t) => format!("C:{}", t),
        Value::Vector(v) => {
            // Vectors compare element-wise, so preserve element order.
            let inner: Vec<String> = v.iter().map(|f| format!("{:020.10}", f)).collect();
            format!("B:[{}]", inner.join("\u{1f}"))
        }
        _ => "Z:unknown".to_string(),
    }
}

/// Order-independent sort key for a property map: sorted `key=value` entries.
fn map_sort_key(m: &HashMap<String, Value>) -> String {
    let mut entries: Vec<String> = m
        .iter()
        .map(|(k, val)| format!("{}={}", k, value_sort_key(val)))
        .collect();
    entries.sort();
    entries.join("\u{1e}")
}

/// Sort key for a node: sorted labels plus its property map, excluding vid.
fn node_sort_key(n: &Node) -> String {
    let mut labels = n.labels.clone();
    labels.sort();
    format!(
        "labels=[{}];props={{{}}}",
        labels.join(","),
        map_sort_key(&n.properties)
    )
}

/// Sort key for an edge: its type plus property map, excluding eid/src/dst.
fn edge_sort_key(e: &Edge) -> String {
    format!(
        "type={};props={{{}}}",
        e.edge_type,
        map_sort_key(&e.properties)
    )
}

/// Sort key for a path: the ordered node and edge content keys.
fn path_sort_key(p: &Path) -> String {
    let nodes: Vec<String> = p.nodes.iter().map(node_sort_key).collect();
    let edges: Vec<String> = p.edges.iter().map(edge_sort_key).collect();
    format!("nodes=[{}];edges=[{}]", nodes.join("|"), edges.join("|"))
}

/// Compare two property maps for equality (order-agnostic), recursing with the
/// same `ignore_list_order` policy for nested values.
fn maps_equal(
    a: &HashMap<String, Value>,
    b: &HashMap<String, Value>,
    ignore_list_order: bool,
) -> bool {
    a.len() == b.len()
        && a.iter().all(|(key, a_val)| {
            b.get(key)
                .is_some_and(|b_val| values_equal_inner(a_val, b_val, ignore_list_order))
        })
}

fn nodes_equal(a: &Node, b: &Node, ignore_list_order: bool) -> bool {
    let labels_match = if a.labels.is_empty() && b.labels.is_empty() {
        true
    } else {
        a.labels.len() == b.labels.len() && a.labels.iter().all(|l| b.labels.contains(l))
    };

    labels_match && maps_equal(&a.properties, &b.properties, ignore_list_order)
}

fn edges_equal(a: &Edge, b: &Edge, ignore_list_order: bool) -> bool {
    a.edge_type == b.edge_type && maps_equal(&a.properties, &b.properties, ignore_list_order)
}

/// Orientation of `edge` relative to the path step from `from` to `to`.
///
/// Returns `Some(true)` when the edge points forward (`from -> to`), `Some(false)`
/// when it points backward (`to -> from`), and `None` when the endpoints do not
/// line up with this step (a malformed path). openCypher paths are directed, so
/// orientation is part of path identity.
fn edge_orientation(edge: &Edge, from: &Node, to: &Node) -> Option<bool> {
    if edge.src == from.vid && edge.dst == to.vid {
        Some(true)
    } else if edge.src == to.vid && edge.dst == from.vid {
        Some(false)
    } else {
        None
    }
}

fn paths_equal(a: &Path, b: &Path, ignore_list_order: bool) -> bool {
    if a.nodes.len() != b.nodes.len() || a.edges.len() != b.edges.len() {
        return false;
    }

    let nodes_match = a
        .nodes
        .iter()
        .zip(&b.nodes)
        .all(|(a, b)| nodes_equal(a, b, ignore_list_order));
    if !nodes_match {
        return false;
    }

    // openCypher paths are directed: each edge's orientation relative to the
    // node sequence is significant, so a reversed path must NOT compare equal.
    // Absolute vids differ between actual (real graph IDs) and expected (parsed
    // positional IDs), so compare the *relative* orientation of each edge against
    // its surrounding nodes rather than raw src/dst values.
    a.edges
        .iter()
        .zip(&b.edges)
        .enumerate()
        .all(|(i, (ea, eb))| {
            if !edges_equal(ea, eb, ignore_list_order) {
                return false;
            }
            let (a_from, a_to) = (&a.nodes[i], &a.nodes[i + 1]);
            let (b_from, b_to) = (&b.nodes[i], &b.nodes[i + 1]);
            match (
                edge_orientation(ea, a_from, a_to),
                edge_orientation(eb, b_from, b_to),
            ) {
                (Some(oa), Some(ob)) => oa == ob,
                // If either side's endpoints are unrecoverable, fall back to
                // type+property equality only (already checked above).
                _ => true,
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_equal_scalars() {
        assert!(values_equal(&Value::Null, &Value::Null));
        assert!(values_equal(&Value::Bool(true), &Value::Bool(true)));
        assert!(!values_equal(&Value::Bool(true), &Value::Bool(false)));
        assert!(values_equal(&Value::Int(42), &Value::Int(42)));
        assert!(!values_equal(&Value::Int(42), &Value::Int(43)));
    }

    #[test]
    fn test_values_equal_floats() {
        assert!(values_equal(&Value::Float(3.15), &Value::Float(3.15)));
        assert!(values_equal(&Value::Float(1.0 + 1e-11), &Value::Float(1.0)));
        assert!(!values_equal(&Value::Float(1.0), &Value::Float(2.0)));
    }

    #[test]
    fn test_values_equal_numeric_type_strict() {
        // openCypher result tables are typed: Integer `1` and Float `1.0` are
        // DISTINCT values, so the runner must NOT cross-type equate them.
        assert!(!values_equal(&Value::Int(1), &Value::Float(1.0)));
        assert!(!values_equal(&Value::Float(1.0), &Value::Int(1)));
        // Same-type numeric equality still holds.
        assert!(values_equal(&Value::Int(1), &Value::Int(1)));
        assert!(values_equal(&Value::Float(1.0), &Value::Float(1.0)));
        // Float/Float approximate equality (tolerance) is preserved.
        assert!(values_equal(&Value::Float(1.0), &Value::Float(1.0 + 1e-11)));
    }

    #[test]
    fn test_values_equal_temporal_matches_rendering() {
        use uni_common::TemporalValue;
        // `Date { days_since_epoch: 0 }` renders as the string "1970-01-01".
        let date = Value::Temporal(TemporalValue::Date {
            days_since_epoch: 0,
        });
        let rendered = Value::String(date.to_string());
        assert_eq!(date.to_string(), "1970-01-01");
        // openCypher renders temporals as quoted strings identical to genuine
        // string results (e.g. `toString(d)`), so the runner matches a temporal
        // against its canonical string rendering in either order.
        assert!(values_equal(&date, &rendered));
        assert!(values_equal(&rendered, &date));
        // A temporal does NOT match a string that renders differently.
        assert!(!values_equal(
            &date,
            &Value::String("1999-12-31".to_string())
        ));
        // Temporal-vs-Temporal structural equality still holds.
        let date_again = Value::Temporal(TemporalValue::Date {
            days_since_epoch: 0,
        });
        assert!(values_equal(&date, &date_again));
        let other_date = Value::Temporal(TemporalValue::Date {
            days_since_epoch: 1,
        });
        assert!(!values_equal(&date, &other_date));
    }

    #[test]
    fn test_values_equal_nan() {
        assert!(values_equal(
            &Value::Float(f64::NAN),
            &Value::Float(f64::NAN)
        ));
    }

    #[test]
    fn test_values_equal_list() {
        assert!(values_equal(
            &Value::List(vec![Value::Int(1), Value::Int(2)]),
            &Value::List(vec![Value::Int(1), Value::Int(2)])
        ));
        assert!(!values_equal(
            &Value::List(vec![Value::Int(1), Value::Int(2)]),
            &Value::List(vec![Value::Int(2), Value::Int(1)])
        ));
    }

    #[test]
    fn test_nodes_equal_single_label() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["Person".to_string()],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2, false));
    }

    #[test]
    fn test_nodes_equal_multi_label_same_order() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2, false));
    }

    #[test]
    fn test_nodes_equal_multi_label_different_order() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["A".to_string(), "B".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["C".to_string(), "B".to_string(), "A".to_string()],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2, false));
    }

    #[test]
    fn test_nodes_not_equal_different_labels() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec!["A".to_string(), "B".to_string()],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec!["A".to_string(), "C".to_string()],
            properties: HashMap::new(),
        };
        assert!(!nodes_equal(&node1, &node2, false));
    }

    #[test]
    fn test_paths_equal_respects_edge_direction() {
        use uni_common::core::id::{Eid, Vid};

        fn node(vid: u64) -> Node {
            Node {
                vid: Vid::from(vid),
                labels: vec![],
                properties: HashMap::new(),
            }
        }
        // Build a 2-node path with one edge of the given orientation.
        // `forward` => a -> b ; otherwise b -> a (reversed).
        fn two_node_path(forward: bool) -> Path {
            let a = node(0);
            let b = node(1);
            let edge = if forward {
                Edge {
                    eid: Eid::from(0),
                    edge_type: "T".to_string(),
                    src: a.vid,
                    dst: b.vid,
                    properties: HashMap::new(),
                }
            } else {
                Edge {
                    eid: Eid::from(0),
                    edge_type: "T".to_string(),
                    src: b.vid,
                    dst: a.vid,
                    properties: HashMap::new(),
                }
            };
            Path {
                nodes: vec![a, b],
                edges: vec![edge],
            }
        }

        let forward = two_node_path(true);
        let forward2 = two_node_path(true);
        let reversed = two_node_path(false);

        // Identical directed paths match.
        assert!(
            paths_equal(&forward, &forward2, false),
            "identical directed paths must be equal"
        );
        // A reversed path must NOT match: (a)-[:T]->(b) != (a)<-[:T]-(b).
        assert!(
            !paths_equal(&forward, &reversed, false),
            "reversed path must NOT be equal to forward path (paths are directed)"
        );
        assert!(
            !paths_equal(&reversed, &forward, false),
            "direction mismatch must be symmetric"
        );
    }

    #[test]
    fn test_nodes_equal_empty_labels() {
        use uni_common::core::id::Vid;
        let node1 = Node {
            vid: Vid::from(1),
            labels: vec![],
            properties: HashMap::new(),
        };
        let node2 = Node {
            vid: Vid::from(2),
            labels: vec![],
            properties: HashMap::new(),
        };
        assert!(nodes_equal(&node1, &node2, false));
    }
}
