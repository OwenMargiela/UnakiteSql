use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::{
    datatypes::schema::Field,
    logical_plan::{LogicalExpr, LogicalPlan, expr::ExprRef},
};

/*Logical expression representing a reference to a column by name. */
pub struct Column {
    pub name: String,
}

impl LogicalExpr for Column {
    fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        let schema = input.schema();
        let found_field = schema
            .fields
            .iter()
            .find(|&it| it.name == self.name)
            .unwrap();
        found_field.clone()
    }
}

/*Logical expression representing a reference to a column by index. */
pub struct ColumnIndex {
    i: usize,
}

impl LogicalExpr for ColumnIndex {
    fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        let schema = input.schema();
        let found_field = &schema.fields[self.i];

        found_field.clone()
    }
}

pub struct Alias {
    pub expr: Arc<ExprRef>,
    pub alias: String,
}

impl LogicalExpr for Alias {
    fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        Field {
            name: self.alias.clone(),
            data_type: self.expr.state.to_field(input).data_type,
        }
    }
}

/* Scalar function */
pub struct ScalarFunction {
    name: String,
    _args: Vec<Arc<dyn LogicalExpr>>,
    return_type: DataType,
}

impl LogicalExpr for ScalarFunction {
    fn to_field(&self, _input: Arc<LogicalPlan>) -> Field {
        Field {
            name: self.name.clone(),
            data_type: self.return_type.clone(),
        }
    }
}

pub struct UnaryExpr {
    name: String,
    op: String,
    expr: Arc<dyn LogicalExpr>,
}

impl LogicalExpr for UnaryExpr {
    fn to_field(&self, _input: Arc<LogicalPlan>) -> Field {
        Field {
            name: self.name.clone(),
            data_type: DataType::Binary,
        }
    }
}

pub struct CastExpr {
    pub expr: Arc<dyn LogicalExpr>,
    pub data_type: DataType,
}

impl LogicalExpr for CastExpr {
    fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        Field {
            name: self.expr.to_field(input).name,
            data_type: self.data_type.clone(),
        }
    }
}

// Implementing Display and Debug traits for various structs
macro_rules! impl_fmt {
    ($t:ty, $body:expr) => {
        impl std::fmt::Display for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                $body(self, f)
            }
        }

        impl std::fmt::Debug for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                $body(self, f)
            }
        }
    };
}

impl_fmt!(
    Column,
    |s: &Column, f: &mut std::fmt::Formatter<'_>| write!(f, "{}", s.name)
);

impl_fmt!(
    ColumnIndex,
    |s: &ColumnIndex, f: &mut std::fmt::Formatter<'_>| write!(f, "{}", s.i)
);
impl_fmt!(Alias, |s: &Alias, f: &mut std::fmt::Formatter<'_>| write!(
    f,
    "{} as {}",
    s.expr.state, s.alias
));
impl_fmt!(
    UnaryExpr,
    |s: &UnaryExpr, f: &mut std::fmt::Formatter<'_>| write!(f, "{} {}", s.op, s.expr)
);
impl_fmt!(
    CastExpr,
    |s: &CastExpr, f: &mut std::fmt::Formatter<'_>| write!(
        f,
        "CAST(#{} AS {})",
        s.expr, s.data_type
    )
);

impl_fmt!(
    ScalarFunction,
    |s: &ScalarFunction, f: &mut std::fmt::Formatter<'_>| {
        let args_str = s
            ._args
            .iter()
            .map(|arg| format!("{}", arg))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}({})", s.name, args_str)
    }
);
