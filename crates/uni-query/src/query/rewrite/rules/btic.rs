/// BTIC temporal rewrite rules
///
/// Decomposes opaque BTIC function calls into range predicates that
/// the existing pushdown infrastructure can evaluate.
use crate::query::rewrite::context::RewriteContext;
use crate::query::rewrite::error::RewriteError;
use crate::query::rewrite::rule::{Arity, RewriteRule};
use uni_cypher::ast::{BinaryOp, Expr};

/// Rewrite rule for `btic_contains_point`
///
/// Transforms: `btic_contains_point(expr, point)`
/// Into:       `btic_lo(expr) <= point AND btic_hi(expr) > point`
///
/// This decomposes the opaque function call into two range predicates
/// on the lo/hi accessors, enabling downstream predicate analysis and
/// potential pushdown.
pub struct BticContainsPointRule;

impl RewriteRule for BticContainsPointRule {
    fn function_name(&self) -> &str {
        "btic_contains_point"
    }

    fn validate_args(&self, args: &[Expr]) -> Result<(), RewriteError> {
        Arity::Exact(2).check(args.len())
    }

    fn rewrite(&self, args: Vec<Expr>, _ctx: &RewriteContext) -> Result<Expr, RewriteError> {
        let btic_expr = args[0].clone();
        let point = args[1].clone();

        // btic_lo(expr) <= point
        let lo_check = Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "btic_lo".to_string(),
                args: vec![btic_expr.clone()],
                distinct: false,
                window_spec: None,
            }),
            op: BinaryOp::LtEq,
            right: Box::new(point.clone()),
        };

        // btic_hi(expr) > point
        let hi_check = Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "btic_hi".to_string(),
                args: vec![btic_expr],
                distinct: false,
                window_spec: None,
            }),
            op: BinaryOp::Gt,
            right: Box::new(point),
        };

        // lo_check AND hi_check
        Ok(Expr::BinaryOp {
            left: Box::new(lo_check),
            op: BinaryOp::And,
            right: Box::new(hi_check),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::rewrite::context::RewriteContext;
    use uni_cypher::ast::CypherLiteral;

    #[test]
    fn test_btic_contains_point_rewrite() {
        let rule = BticContainsPointRule;
        let ctx = RewriteContext::default();

        let args = vec![
            Expr::Property(Box::new(Expr::Variable("n".into())), "valid_at".into()),
            Expr::Literal(CypherLiteral::Integer(489_024_000_000)),
        ];

        assert!(rule.validate_args(&args).is_ok());

        let result = rule.rewrite(args, &ctx).unwrap();

        // Should produce: btic_lo(n.valid_at) <= 489024000000 AND btic_hi(n.valid_at) > 489024000000
        match result {
            Expr::BinaryOp {
                op: BinaryOp::And, ..
            } => {} // Correct shape
            other => panic!("expected AND expression, got: {other:?}"),
        }
    }

    #[test]
    fn test_btic_contains_point_wrong_arity() {
        let rule = BticContainsPointRule;
        let args = vec![Expr::Variable("x".into())];
        assert!(rule.validate_args(&args).is_err());
    }
}
