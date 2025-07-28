use std::sync::Arc;

use crate::logical_plan::{
    AggregateExpr,
    expr::{Expr, ExprRef},
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
