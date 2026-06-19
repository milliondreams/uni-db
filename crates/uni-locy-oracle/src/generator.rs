//! Program generator — the single source of truth for differential cases.
//!
//! Each builder emits, from one set of parameters, the triple consumed by the
//! differential test: the Cypher that seeds the base graph, the equivalent Locy
//! program text (run by the engine), and the oracle IR (run by this crate's
//! evaluator). Because the same parameters drive all three, the engine and
//! oracle never share evaluation code, only inputs.

// Rust guideline compliant

use std::collections::HashMap;
use std::ops::Range;

use proptest::prelude::*;

use crate::ir::{Generated, IsRef, OracleClause, OracleProgram, OracleRule, Tuple};

/// Closed-form transitive-closure cardinality of [`build_layered_dag`].
///
/// In a layered DAG with `stages` fully-bipartite layers of `width` nodes, every
/// node in layer `L` reaches every node in each later layer `L' > L`. So the
/// closure has `width²` pairs for each of the `C(stages, 2)` ordered layer pairs:
/// `width² · stages · (stages − 1) / 2`.
///
/// # Examples
/// ```
/// use uni_locy_oracle::generator::expected_closure_size;
/// assert_eq!(expected_closure_size(4, 15), 1350); // the historical IS-NOT-bug shape
/// assert_eq!(expected_closure_size(1, 9), 0); // a single layer has no edges
/// ```
#[must_use]
pub fn expected_closure_size(stages: usize, width: usize) -> usize {
    width * width * stages * stages.saturating_sub(1) / 2
}

/// Edges of a layered fully-bipartite DAG, as oracle base tuples `[src_id, dst_id]`.
///
/// Node ids are `layer * width + index`. Consecutive layers are fully connected:
/// every node in layer `L` has an `EDGE` to every node in layer `L + 1`.
fn layered_dag_edges(stages: usize, width: usize) -> Vec<Tuple> {
    let mut edges = Vec::new();
    for layer in 0..stages.saturating_sub(1) {
        for i in 0..width {
            for j in 0..width {
                let src = (layer * width + i) as i64;
                let dst = ((layer + 1) * width + j) as i64;
                edges.push(vec![src, dst]);
            }
        }
    }
    edges
}

/// Renders the base-graph Cypher: one `CREATE` binding every node, then every edge.
///
/// Uses only basic `CREATE` with bound variables (`n{id}`) — the most widely
/// supported Cypher — so seeding never depends on `UNWIND`/`range`/list features.
/// Each `(edge_type, edges)` group is emitted under its own relationship type.
fn render_cypher(node_count: usize, edge_groups: &[(&str, &[Tuple])]) -> String {
    let total_edges: usize = edge_groups.iter().map(|(_, e)| e.len()).sum();
    let mut parts: Vec<String> = Vec::with_capacity(node_count + total_edges);
    for id in 0..node_count {
        parts.push(format!("(n{id}:Node {{id: {id}}})"));
    }
    for (edge_type, edges) in edge_groups {
        for e in *edges {
            parts.push(format!("(n{})-[:{edge_type}]->(n{})", e[0], e[1]));
        }
    }
    format!("CREATE {}", parts.join(", "))
}

/// Builds a `var -> column` binding map from `(name, index)` pairs.
fn var_cols(pairs: &[(&str, usize)]) -> HashMap<String, usize> {
    pairs.iter().map(|(v, i)| ((*v).to_string(), *i)).collect()
}

/// The two `reaches` clauses (base + recursive transitive closure) over `EDGE`.
const REACHES_PROGRAM: &str = concat!(
    "CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b\n",
    "CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(mid:Node) ",
    "WHERE mid IS reaches TO b YIELD KEY a, KEY b",
);

/// The `reaches` rule IR: `reaches(a,b) :- EDGE(a,b)` ∪ `EDGE(a,mid), reaches(mid,b)`.
fn reaches_rule(edges: &[Tuple]) -> OracleRule {
    OracleRule {
        name: "reaches".to_string(),
        clauses: vec![
            OracleClause {
                base: edges.to_vec(),
                var_cols: var_cols(&[("a", 0), ("b", 1)]),
                pos_refs: Vec::new(),
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "b".to_string()],
            },
            OracleClause {
                base: edges.to_vec(),
                var_cols: var_cols(&[("a", 0), ("mid", 1)]),
                pos_refs: vec![IsRef {
                    rule: "reaches".to_string(),
                    subjects: vec!["mid".to_string()],
                    target: Some("b".to_string()),
                }],
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "b".to_string()],
            },
        ],
    }
}

/// All ordered pairs of node ids `[a, b]` — the `MATCH (a:Node), (b:Node)` relation.
fn all_pairs(node_count: usize) -> Vec<Tuple> {
    let n = node_count as i64;
    (0..n)
        .flat_map(|a| (0..n).map(move |b| vec![a, b]))
        .collect()
}

/// Builds a transitive-closure program over a layered fully-bipartite DAG.
///
/// This is the workhorse family for the threshold-straddling property test: the
/// closure size [`expected_closure_size(stages, width)`](expected_closure_size)
/// can be tuned to land on either side of the engine's 300-fact dedup-strategy
/// switch. The recursive rule is right-recursive transitive closure:
///
/// ```text
/// CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
/// CREATE RULE reaches AS MATCH (a:Node)-[:EDGE]->(mid:Node)
///   WHERE mid IS reaches TO b YIELD KEY a, KEY b
/// ```
///
/// Degenerate inputs are handled, not rejected: `stages < 2` or `width == 0`
/// yields an edgeless graph and an empty closure.
#[must_use]
pub fn build_layered_dag(stages: usize, width: usize) -> Generated {
    let node_count = stages * width;
    let edges = layered_dag_edges(stages, width);

    Generated {
        base_graph_cypher: render_cypher(node_count, &[("EDGE", &edges)]),
        program_text: REACHES_PROGRAM.to_string(),
        oracle_rules: OracleProgram {
            strata: vec![vec![reaches_rule(&edges)]],
        },
        key_schema: HashMap::from([(
            "reaches".to_string(),
            vec!["a".to_string(), "b".to_string()],
        )]),
    }
}

/// Builds a program whose second stratum is the stratified `IS NOT` complement
/// of the transitive closure: `unreached(a,b) :- Node(a), Node(b), NOT reaches(a,b)`.
///
/// This is the headline family — it exercises the anti-join path that carried the
/// historical `IS NOT` complement bug. Choose `stages`/`width` so the closure
/// exceeds 300 (e.g. `(4, 12)` → 864) to drive the over-threshold dedup strategy;
/// the oracle catches a divergence regardless of that trigger.
///
/// The closed-form complement size is `(stages·width)² − expected_closure_size`.
/// Keep `stages · width` modest (≤ ~80) so the all-pairs base stays tractable.
#[must_use]
pub fn build_complement(stages: usize, width: usize) -> Generated {
    let node_count = stages * width;
    let edges = layered_dag_edges(stages, width);

    let program_text = format!(
        "{REACHES_PROGRAM}\n\
         CREATE RULE unreached AS MATCH (a:Node), (b:Node) \
         WHERE a IS NOT reaches TO b YIELD KEY a, KEY b"
    );

    // unreached(a, b) :- all-pairs(a, b), NOT reaches(a, b). Negation keys on the
    // full (a, b) tuple, so subjects carries both bound variables.
    let unreached = OracleRule {
        name: "unreached".to_string(),
        clauses: vec![OracleClause {
            base: all_pairs(node_count),
            var_cols: var_cols(&[("a", 0), ("b", 1)]),
            pos_refs: Vec::new(),
            neg_refs: vec![IsRef {
                rule: "reaches".to_string(),
                subjects: vec!["a".to_string(), "b".to_string()],
                target: None,
            }],
            yield_vars: vec!["a".to_string(), "b".to_string()],
        }],
    };

    Generated {
        base_graph_cypher: render_cypher(node_count, &[("EDGE", &edges)]),
        program_text,
        oracle_rules: OracleProgram {
            strata: vec![vec![reaches_rule(&edges)], vec![unreached]],
        },
        key_schema: HashMap::from([
            (
                "reaches".to_string(),
                vec!["a".to_string(), "b".to_string()],
            ),
            (
                "unreached".to_string(),
                vec!["a".to_string(), "b".to_string()],
            ),
        ]),
    }
}

/// Builds a non-recursive union: `linked` is the same head from two base
/// patterns, `EDGE` and its reverse `EDGE2`.
///
/// Exercises multi-clause union semantics — the derived relation must equal the
/// set union of the two edge relations. `EDGE` and `EDGE2` are disjoint (forward
/// vs. backward), so the union has exactly `2 · |EDGE|` pairs.
#[must_use]
pub fn build_union(stages: usize, width: usize) -> Generated {
    let node_count = stages * width;
    let forward = layered_dag_edges(stages, width);
    let reverse: Vec<Tuple> = forward.iter().map(|e| vec![e[1], e[0]]).collect();

    let program_text = concat!(
        "CREATE RULE linked AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b\n",
        "CREATE RULE linked AS MATCH (a:Node)-[:EDGE2]->(b:Node) YIELD KEY a, KEY b",
    )
    .to_string();

    let clause = |base: Vec<Tuple>| OracleClause {
        base,
        var_cols: var_cols(&[("a", 0), ("b", 1)]),
        pos_refs: Vec::new(),
        neg_refs: Vec::new(),
        yield_vars: vec!["a".to_string(), "b".to_string()],
    };

    Generated {
        base_graph_cypher: render_cypher(node_count, &[("EDGE", &forward), ("EDGE2", &reverse)]),
        program_text,
        oracle_rules: OracleProgram {
            strata: vec![vec![OracleRule {
                name: "linked".to_string(),
                clauses: vec![clause(forward), clause(reverse)],
            }]],
        },
        key_schema: HashMap::from([("linked".to_string(), vec!["a".to_string(), "b".to_string()])]),
    }
}

/// A proptest strategy yielding [`build_layered_dag`] instances over given bounds.
///
/// Exposed from the library (rather than inlined in the test) so downstream test
/// crates can reuse the same generator. Pick `width`/`stages` ranges whose
/// closure size straddles the 300-fact threshold.
///
/// # Examples
/// ```
/// use uni_locy_oracle::generator::layered_dag_strategy;
/// let _strategy = layered_dag_strategy(2..6, 5..30);
/// ```
pub fn layered_dag_strategy(
    stages: Range<usize>,
    width: Range<usize>,
) -> impl Strategy<Value = Generated> {
    (stages, width).prop_map(|(s, w)| build_layered_dag(s, w))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Independent transitive closure (distinct from the oracle) for cross-checking.
    fn brute_closure(edges: &[Tuple]) -> HashSet<(i64, i64)> {
        let mut set: HashSet<(i64, i64)> = edges.iter().map(|t| (t[0], t[1])).collect();
        loop {
            let snapshot: Vec<(i64, i64)> = set.iter().copied().collect();
            let mut added = false;
            for &(a, b) in &snapshot {
                for &(c, d) in &snapshot {
                    if b == c && set.insert((a, d)) {
                        added = true;
                    }
                }
            }
            if !added {
                break;
            }
        }
        set
    }

    fn edges_of(g: &Generated) -> &Vec<Tuple> {
        &g.oracle_rules.strata[0][0].clauses[0].base
    }

    #[test]
    fn edge_count_matches_formula() {
        for stages in 1..6 {
            for width in 1..8 {
                let g = build_layered_dag(stages, width);
                assert_eq!(
                    edges_of(&g).len(),
                    width * width * stages.saturating_sub(1),
                    "stages={stages} width={width}"
                );
            }
        }
    }

    #[test]
    fn closure_size_matches_closed_form_and_brute_force() {
        for stages in 1..6 {
            for width in 1..8 {
                let g = build_layered_dag(stages, width);
                let bf = brute_closure(edges_of(&g));
                assert_eq!(
                    bf.len(),
                    expected_closure_size(stages, width),
                    "stages={stages} width={width}"
                );
            }
        }
    }

    #[test]
    fn program_text_has_both_clauses() {
        let g = build_layered_dag(3, 5);
        assert_eq!(g.program_text.matches("CREATE RULE reaches AS").count(), 2);
        assert!(g.program_text.contains("mid IS reaches TO b"));
    }

    #[test]
    fn cypher_has_right_node_and_edge_counts() {
        let g = build_layered_dag(3, 4);
        assert_eq!(g.base_graph_cypher.matches(":Node {id:").count(), 3 * 4);
        assert_eq!(
            g.base_graph_cypher.matches("[:EDGE]").count(),
            4 * 4 * (3 - 1)
        );
        assert!(g.base_graph_cypher.starts_with("CREATE "));
    }

    #[test]
    fn oracle_rules_structure() {
        let g = build_layered_dag(2, 3);
        assert_eq!(g.oracle_rules.strata.len(), 1);
        let rule = &g.oracle_rules.strata[0][0];
        assert_eq!(rule.name, "reaches");
        assert_eq!(rule.clauses.len(), 2);
        let rec = &rule.clauses[1];
        assert_eq!(rec.pos_refs.len(), 1);
        assert_eq!(rec.pos_refs[0].rule, "reaches");
        assert_eq!(rec.pos_refs[0].subjects, vec!["mid".to_string()]);
        assert_eq!(rec.pos_refs[0].target.as_deref(), Some("b"));
        assert_eq!(
            g.key_schema["reaches"],
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
