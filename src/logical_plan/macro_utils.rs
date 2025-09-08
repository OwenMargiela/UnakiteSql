macro_rules! impl_literal_helper {
    ($func_name:ident, &str, $variant:ident, $struct:ident) => {
        pub fn $func_name(value: &str) -> crate::logical_plan::expr::ExprRef {
            crate::logical_plan::expr::ExprRef {
                state: crate::logical_plan::Arc::new(crate::logical_plan::expr::Expr::LiteralExpr(
                    crate::logical_plan::expr::LiteralExpression::$variant($struct {
                        value: value.to_string(),
                    }),
                )),
            }
        }
    };

    ($func_name:ident, $ty:ty, $variant:ident, $struct:ident) => {
        pub fn $func_name(value: $ty) -> crate::logical_plan::expr::ExprRef {
            crate::logical_plan::expr::ExprRef {
                state: crate::logical_plan::Arc::new(crate::logical_plan::expr::Expr::LiteralExpr(
                    crate::logical_plan::expr::LiteralExpression::Numeric(
                        crate::logical_plan::expr::NumericExpression::$variant($struct { value }),
                    ),
                )),
            }
        }
    };
}

macro_rules! impl_literal {
    ($name:ident, $ty:ty, $dt:ident) => {
        pub struct $name {
            pub value: $ty,
        }

        impl super::LogicalExpr for $name {
            fn to_field(
                &self,
                _input: crate::logical_plan::Arc<crate::logical_plan::LogicalPlan>,
            ) -> crate::datatypes::schema::Field {
                crate::datatypes::schema::Field {
                    name: format!("{}", self),
                    data_type: arrow::datatypes::DataType::$dt,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.value)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.value)
            }
        }
    };
}

macro_rules! impl_math_expr_op {
    ($name:ident, $trait:ident, $method:ident, $variant:ident, $constructor:ident) => {
        impl std::ops::$trait for $name {
            type Output = crate::logical_plan::Arc<crate::logical_plan::expr::MathExpression>;

            fn $method(self, rhs: Self) -> Self::Output {
                return crate::logical_plan::Arc::new(
                    crate::logical_plan::expr::MathExpression::$variant($constructor::new(
                        crate::logical_plan::Arc::new(self),
                        crate::logical_plan::Arc::new(rhs),
                    )),
                );
            }
        }
    };
}

macro_rules! impl_math_expr {
    ($type_name: ident) => {
        impl_math_expr_op!($type_name, Add, add, AddExpr, MathAdd);
        impl_math_expr_op!($type_name, Sub, sub, SubExpr, MathSubtract);
        impl_math_expr_op!($type_name, Mul, mul, MulExpr, MathMultiply);
        impl_math_expr_op!($type_name, Div, div, DivExpr, MathDivide);
        impl_math_expr_op!($type_name, Rem, rem, ModExpr, MathMod);
    };
}

macro_rules! impl_non_string_literals {
    (
        $(
            ($type_name:ident, $rust_type:ty, $arrow_type:ident, $variant:ident, $fn_name:ident)
        ),* $(,)?
    ) => {
        $(
            impl_literal!($type_name, $rust_type, $arrow_type);
            impl_math_expr!($type_name);
            impl_literal_helper!($fn_name, $rust_type, $variant, $type_name);
        )*
    };
}

impl_non_string_literals!(
    /* Logical expression representing a literal small int value. */
    (LiteralInt8, i8, Int8, Integer8Expr, literal_i8),
    /* Logical expression representing a literal short int value. */
    (LiteralInt16, i16, Int16, Integer16Expr, literal_i16),
    /* Logical expression representing a literal int value. */
    (LiteralInt32, i32, Int32, Integer32Expr, literal_i32),
    /* Logical expression representing a literal long int value. */
    (LiteralInt64, i64, Int64, Integer64Expr, literal_i64),
    /* Logical expression representing a literal small uint value. */
    (LiteralUInt8, u8, UInt8, UInteger8Expr, literal_u8),
    /* Logical expression representing a literal short uint value. */
    (LiteralUInt16, u16, UInt16, UInteger16Expr, literal_u16),
    /* Logical expression representing a literal  uint value. */
    (LiteralUInt32, u32, UInt32, UInteger32Expr, literal_u32),
    /* Logical expression representing a literal long uint value. */
    (LiteralUInt64, u64, UInt64, UInteger64Expr, literal_u64),
    /* Logical expression representing a literal float value. */
    (LiteralFloat, f32, Float32, FloatExpr, literal_float),
    /* Logical expression representing a literal double value. */
    (LiteralDouble, f64, Float64, DoubleExpr, literal_double),
);

/* Logical expression representing a literal string value. */
impl_literal!(LiteralString, String, Utf8);
impl_math_expr!(LiteralString);
impl_literal_helper!(literal_string, &str, StringExpr, LiteralString);

macro_rules! impl_binary_expr {
    ($name:ident, $op:expr) => {
        /// Documentation
        pub struct $name {
            pub name: String,
            op: String,
            l: crate::logical_plan::Arc<dyn crate::logical_plan::LogicalExpr>,
            r: crate::logical_plan::Arc<dyn crate::logical_plan::LogicalExpr>,
        }
        impl $name {
            pub fn new(
                l: crate::logical_plan::Arc<dyn crate::logical_plan::LogicalExpr>,
                r: crate::logical_plan::Arc<dyn crate::logical_plan::LogicalExpr>,
            ) -> $name {
                $name {
                    name: $op.to_lowercase(),
                    op: $op,
                    l,
                    r,
                }
            }
        }

        impl crate::logical_plan::LogicalExpr for $name {
            fn to_field(
                &self,
                _input: crate::logical_plan::Arc<crate::logical_plan::LogicalPlan>,
            ) -> crate::datatypes::schema::Field {
                crate::datatypes::schema::Field {
                    name: format!("{}", self),
                    data_type: arrow::datatypes::DataType::Boolean,
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} {:?} {}", self.l, self.op, self.r)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} {:?} {}", self.l, self.op, self.r)
            }
        }
    };
}

macro_rules! impl_comparison_expr_helper {
    ($fn_name:ident, $variant:ident, $struct:ident) => {
        pub fn $fn_name(
            l: crate::logical_plan::expr::ExprRef,
            r: crate::logical_plan::expr::ExprRef,
        ) -> crate::logical_plan::expr::ExprRef {
            crate::logical_plan::expr::ExprRef {
                state: crate::logical_plan::Arc::new(crate::logical_plan::expr::Expr::$variant(
                    $struct::new(l.state, r.state),
                )),
            }
        }
    };
}

/* Logical expression representing a logical AND */
impl_binary_expr!(And, "AND".to_string());

/* Logical expression representing a logical OR */
impl_binary_expr!(Or, "OR".to_string());
impl_comparison_expr_helper!(or, OrExpr, Or);

/* Logical expression representing an equality (`=`) comparison */
impl_binary_expr!(EqOp, "=".to_string());
impl_comparison_expr_helper!(eq, EqOpExpr, EqOp);

/* Logical expression representing an inequality (`!=`) comparison */
impl_binary_expr!(Neq, "!=".to_string());
impl_comparison_expr_helper!(neq, NeqExpr, Neq);
/* Logical expression representing a greater than (`>`) comparison */
impl_binary_expr!(Gt, ">".to_string());
impl_comparison_expr_helper!(gt, GtExpr, Gt);

/* Logical expression representing a greater than or equals (`>=`) comparison */
impl_binary_expr!(Gteq, ">=".to_string());
impl_comparison_expr_helper!(gteq, GtEqExpr, Gteq);

/* Logical expression representing a less than (`<`) comparison */
impl_binary_expr!(Lt, "<".to_string());
impl_comparison_expr_helper!(lt, LtExpr, Lt);

/* Logical expression representing a less than or equals (`<=`) comparison */
impl_binary_expr!(Lteq, "<=".to_string());
impl_comparison_expr_helper!(lteq, LtEqExpr, Lteq);

/* Logical expression representing binary math exspression*/
impl_binary_expr!(MathAdd, "+".to_string());
impl_binary_expr!(MathSubtract, "-".to_string());
impl_binary_expr!(MathMultiply, "*".to_string());
impl_binary_expr!(MathDivide, "/".to_string());
impl_binary_expr!(MathMod, "%".to_string());

// Helper macro to implement both Display and Debug using the same formatting logic.
// Helper macro: handles both one-field and two-field display formatting
macro_rules! impl_fmt {
    // Two fields (e.g. "NAME(EXPR)")
    ($type:ident, "{}({})", $field1:ident, $field2:ident) => {
        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.$field2)
            }
        }
        impl std::fmt::Debug for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.$field2)
            }
        }
    };

    // One field (e.g. "COUNT(EXPR)")
    ($type:ident, $fmt_str:expr, $field:ident) => {
        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, $fmt_str, self.$field)
            }
        }
        impl std::fmt::Debug for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, $fmt_str, self.$field)
            }
        }
    };
}

// Main macro to generate aggregate logical expressions.
// Handles:
// 1. COUNT DISTINCT — special logic and fixed return type
// 2. COUNT — fixed return type
// 3. Generic aggregates — use inner expression's type
macro_rules! impl_aggregate_expr {
    // COUNT DISTINCT is special: uses Boxed expr and always returns Int32
    (AggregateCountDistinct, $op_name:expr) => {
        pub struct AggregateCountDistinct {
            _name: String,
            expr: Box<dyn crate::logical_plan::LogicalExpr>,
        }

        impl AggregateCountDistinct {
            pub fn new(input: Box<dyn crate::logical_plan::LogicalExpr>) -> Self {
                Self {
                    _name: $op_name,
                    expr: input,
                }
            }
        }

        impl crate::logical_plan::LogicalExpr for AggregateCountDistinct {
            fn to_field(
                &self,
                _: crate::logical_plan::Arc<crate::logical_plan::LogicalPlan>,
            ) -> crate::datatypes::schema::Field {
                crate::datatypes::schema::Field {
                    name: "COUNT DISTINCT".into(),
                    data_type: arrow::datatypes::DataType::Int32,
                }
            }
        }

        // Reuse formatting logic
        impl_fmt!(AggregateCountDistinct, "COUNT DISTINCT({})", expr);
    };

    // COUNT is also special: returns Int32 but uses Arc expr
    (AggregateCount, $op_name:expr) => {
        pub struct AggregateCount {
            _name: String,
            expr: crate::logical_plan::expr::ExprRef,
        }

        impl AggregateCount {
            pub fn new(expr: crate::logical_plan::expr::ExprRef) -> Self {
                Self {
                    _name: $op_name,
                    expr,
                }
            }
        }

        impl crate::logical_plan::LogicalExpr for AggregateCount {
            fn to_field(
                &self,
                _: crate::logical_plan::Arc<crate::logical_plan::LogicalPlan>,
            ) -> crate::datatypes::schema::Field {
                crate::datatypes::schema::Field {
                    name: "COUNT".into(),
                    data_type: arrow::datatypes::DataType::Int32,
                }
            }
        }

        impl_fmt!(AggregateCount, "{:?}", expr);
    };

    // Generic aggregate case: return same data type as inner expression
    ($name:ident, $op_name:expr) => {
        pub struct $name {
            name: String,
            expr: crate::logical_plan::expr::ExprRef,
        }

        impl $name {
            pub fn new(expr: crate::logical_plan::expr::ExprRef) -> Self {
                Self {
                    name: $op_name,
                    expr,
                }
            }
        }

        impl crate::logical_plan::LogicalExpr for $name {
            fn to_field(
                &self,
                input: crate::logical_plan::Arc<crate::logical_plan::LogicalPlan>,
            ) -> crate::datatypes::schema::Field {
                let mut field = self.expr.to_field(input);
                field.name = self.name.clone();
                field
            }
        }

        // name(expr)
        impl_fmt!($name, "{}({})", name, expr);
    };
}

/* Logical expression representing the SUM aggregate expression. */
impl_aggregate_expr!(AggregateSum, String::from("Sum"));

/* Logical expression representing the MIN aggregate expression. */
impl_aggregate_expr!(AggregateMin, String::from("Min"));
/* Logical expression representing the MAX aggregate expression. */
impl_aggregate_expr!(AggregateMax, String::from("Max"));

/* Logical expression representing the AVG aggregate expression. */
impl_aggregate_expr!(AggregateAvg, String::from("Avg"));

/* Logical expression representing the COUNT aggregate expression. */
impl_aggregate_expr!(AggregateCount, String::from("Count"));

/* Logical expression representing the COUNT DISTINCT aggregate expression. */
impl_aggregate_expr!(AggregateCountDistinct, String::from("Count Distinct"));
