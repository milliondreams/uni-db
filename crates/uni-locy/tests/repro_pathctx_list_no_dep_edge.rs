//! Repro for compiler/dependency.rs:148 — PathContextWalker::walk_expr does NOT
//! recurse into Expr::List / Expr::Map (catch-all `_ => {}`), while the
//! InvocationLifter DOES lift model calls out of those shapes. So a
//! path-context model invocation nested inside a list/map literal runs WITHOUT
//! the stratifier adding the `rule -> source_rule` ordering edge — the model
//! may then evaluate against incomplete/absent path-context data.

use uni_locy::compile_with_config;
use uni_locy::{CompiledProgram, LocyConfig};
use uni_cypher::parse_locy;

fn stratum_of<'a>(prog: &'a CompiledProgram, rule: &str) -> &'a uni_locy::types::Stratum {
    prog.strata
        .iter()
        .find(|s| s.rules.iter().any(|r| r.name == rule))
        .unwrap_or_else(|| panic!("no stratum for rule {rule}"))
}

const MODEL_AND_SOURCE: &str = "CREATE MODEL scorer AS \
     INPUT (s) \
     FEATURES (s, dist) FROM reach \
     OUTPUT SCORE sc \
     USING xervo('score/x') \
     CREATE RULE reach AS MATCH (a)-[:E]->(b) YIELD a, b ";

#[test]
fn pathctx_model_in_list_literal_drops_dependency_edge() {
    let cfg = LocyConfig::default();

    // Control: top-level YIELD invocation. walk_expr sees the FunctionCall and
    // adds edge s -> reach, so reach's stratum precedes s's.
    let top_src = format!(
        "{MODEL_AND_SOURCE} \
         CREATE RULE s AS MATCH (n)-[:E]->(m) YIELD n, scorer(n) AS out"
    );
    let top = compile_with_config(&parse_locy(&top_src).unwrap(), &cfg)
        .expect("top-level program should compile");
    let s_strat = stratum_of(&top, "s");
    let reach_id = stratum_of(&top, "reach").id;
    let control_has_edge = s_strat.depends_on.contains(&reach_id);
    assert!(
        control_has_edge,
        "control: top-level path-context call must create s -> reach edge \
         (s.depends_on={:?}, reach_id={reach_id})",
        s_strat.depends_on
    );

    // Buggy case: same call nested inside a list literal `[scorer(n)]`.
    let list_src = format!(
        "{MODEL_AND_SOURCE} \
         CREATE RULE s AS MATCH (n)-[:E]->(m) YIELD n, [scorer(n)] AS out"
    );
    let list = compile_with_config(&parse_locy(&list_src).unwrap(), &cfg)
        .expect("list-literal program should compile");
    let s_strat_l = stratum_of(&list, "s");
    let reach_id_l = stratum_of(&list, "reach").id;
    let list_has_edge = s_strat_l.depends_on.contains(&reach_id_l);

    // FIXED (dependency.rs): walk_expr now recurses into Expr::List, so the
    // list-nested path-context invocation forces s after reach — same edge as
    // the top-level control.
    assert!(
        list_has_edge,
        "list-nested path-context call must create the s -> reach dep edge \
         (s.depends_on={:?}, reach_id={reach_id_l})",
        s_strat_l.depends_on
    );
}
