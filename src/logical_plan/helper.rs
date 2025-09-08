use std::sync::Arc;

use crate::logical_plan::{
    AggregateExpr,
    expr::{Expr, ExprRef, LiteralExpression, NumericExpression},
    expression::Column,
    macro_utils::{AggregateCount, AggregateMax, AggregateMin},
};

/*Conveniece method for Aggregates */
// Macros might be helpful

pub fn min(name: &str) -> AggregateExpr {
    AggregateExpr::Min(AggregateMin::new(column(name)))
}

pub fn max(name: &str) -> AggregateExpr {
    AggregateExpr::Max(AggregateMax::new(column(name)))
}

pub fn count(name: &str) -> AggregateExpr {
    AggregateExpr::Count(AggregateCount::new(column(name)))
}

// Convenience method for creating a column Expr Enum struct
pub fn column(name: &str) -> ExprRef {
    ExprRef {
        state: Arc::new(Expr::ColumnExpr(Column {
            name: name.to_string(),
        })),
    }
}

// Convenience method for matching a literal numeric expression to a usize
pub fn numeric_lit_expr_to_usize(state: &Expr) -> usize {
    if let Expr::LiteralExpr(lit) = state {
        match lit {
            LiteralExpression::Numeric(numeric_expression) => match numeric_expression {
                NumericExpression::Integer64Expr(value) => value.value as usize,
                NumericExpression::Integer32Expr(value) => value.value as usize,
                NumericExpression::Integer16Expr(value) => value.value as usize,
                NumericExpression::Integer8Expr(value) => value.value as usize,
                NumericExpression::DoubleExpr(value) => value.value as usize,
                NumericExpression::FloatExpr(value) => value.value as usize,
                NumericExpression::UInteger64Expr(value) => value.value as usize,
                NumericExpression::UInteger32Expr(value) => value.value as usize,
                NumericExpression::UInteger16Expr(value) => value.value as usize,
                NumericExpression::UInteger8Expr(value) => value.value as usize,
            },
            _ => panic!("Limit expression must be a literal numeric expression"),
        }
    } else {
        panic!("Limit expression must be a literal expression")
    }
}
