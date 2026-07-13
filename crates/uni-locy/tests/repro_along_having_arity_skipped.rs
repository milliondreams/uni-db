//! Repro for compiler/typecheck.rs:696 — check_model_invocations only visits
//! WHERE, FOLD, and YIELD expressions; ALONG and HAVING positions are skipped.
//! Arity for model calls is therefore never validated in ALONG/HAVING, and the
//! InvocationLifter explicitly relies on that check ("Arity already validated
//! by check_model_invocations"), passing wrong-arity calls through unlifted.
//! Net effect: a wrong-arity model call that IS rejected in YIELD is silently
//! accepted in ALONG.

use uni_cypher::parse_locy;
use uni_locy::LocyConfig;
use uni_locy::compile_with_config;
use uni_locy::compiler::errors::LocyCompileError;

// `scorer` has arity 1 (INPUT (s)). Both rules below invoke it with 2 args.
const MODEL: &str = "CREATE MODEL scorer AS INPUT (s) OUTPUT SCORE sc \
     USING xervo('score/x') ";

#[test]
fn model_arity_validated_in_yield_but_not_along() {
    let cfg = LocyConfig::default();

    // Control: wrong-arity call in YIELD → ModelArityMismatch (validated).
    let yield_src =
        format!("{MODEL} CREATE RULE ry AS MATCH (a)-[:E]->(b) YIELD a, b, scorer(a, b) AS s");
    let yield_res = compile_with_config(&parse_locy(&yield_src).unwrap(), &cfg);
    assert!(
        matches!(yield_res, Err(LocyCompileError::ModelArityMismatch { .. })),
        "control: wrong-arity model call in YIELD must be rejected, got {yield_res:?}"
    );

    // Buggy: identical wrong-arity call in ALONG → NOT rejected, because
    // check_model_invocations never visits ALONG. (repro for typecheck.rs:696)
    let along_src = format!(
        "{MODEL} CREATE RULE ra AS MATCH (a)-[:E]->(b) ALONG s = scorer(a, b) YIELD a, b, s"
    );
    let along_res = compile_with_config(&parse_locy(&along_src).unwrap(), &cfg);

    // FIXED (typecheck.rs): check_model_invocations now visits ALONG, so the
    // wrong-arity call is rejected exactly like the YIELD control.
    assert!(
        matches!(along_res, Err(LocyCompileError::ModelArityMismatch { .. })),
        "ALONG model-call arity must be validated, got {along_res:?}"
    );
}
