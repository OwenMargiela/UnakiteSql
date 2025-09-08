use std::{fmt, sync::Arc};

use crate::{
    datatypes::schema::Field,
    logical_plan::{
        LogicalExpr, LogicalPlan,
        expression::{Alias, Column},
        macro_utils::{
            AggregateAvg, AggregateCount, AggregateCountDistinct, AggregateMax, AggregateMin,
            AggregateSum, And, EqOp, Gt, Gteq, LiteralDouble, LiteralFloat, LiteralInt8,
            LiteralInt16, LiteralInt32, LiteralInt64, LiteralString, LiteralUInt8, LiteralUInt16,
            LiteralUInt32, LiteralUInt64, Lt, Lteq, MathAdd, MathDivide, MathMod, MathMultiply,
            MathSubtract, Neq, Or,
        },
    },
};

#[derive(Debug)]
pub enum MathExpression {
    AddExpr(MathAdd),
    SubExpr(MathSubtract),
    MulExpr(MathMultiply),
    DivExpr(MathDivide),
    ModExpr(MathMod),
}

#[derive(Debug)]
pub enum NumericExpression {
    Integer8Expr(LiteralInt8),
    Integer16Expr(LiteralInt16),
    Integer32Expr(LiteralInt32),
    Integer64Expr(LiteralInt64),
    UInteger8Expr(LiteralUInt8),
    UInteger16Expr(LiteralUInt16),
    UInteger32Expr(LiteralUInt32),
    UInteger64Expr(LiteralUInt64),
    FloatExpr(LiteralFloat),
    DoubleExpr(LiteralDouble),
}

#[derive(Debug)]
pub enum LiteralExpression {
    StringExpr(LiteralString),
    Numeric(NumericExpression),
}

impl NumericExpression {
    pub fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        match self {
            NumericExpression::Integer8Expr(literal_int8) => literal_int8.to_field(input),
            NumericExpression::Integer16Expr(literal_int16) => literal_int16.to_field(input),
            NumericExpression::Integer32Expr(literal_int32) => literal_int32.to_field(input),
            NumericExpression::Integer64Expr(literal_int64) => literal_int64.to_field(input),
            NumericExpression::UInteger8Expr(literal_uint8) => literal_uint8.to_field(input),
            NumericExpression::UInteger16Expr(literal_uint16) => literal_uint16.to_field(input),
            NumericExpression::UInteger32Expr(literal_uint32) => literal_uint32.to_field(input),
            NumericExpression::UInteger64Expr(literal_uint64) => literal_uint64.to_field(input),
            NumericExpression::FloatExpr(literal_float) => literal_float.to_field(input),
            NumericExpression::DoubleExpr(literal_double) => literal_double.to_field(input),
        }
    }
}

impl LiteralExpression {
    pub fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        match self {
            LiteralExpression::StringExpr(literal_string) => literal_string.to_field(input),
            LiteralExpression::Numeric(numeric_expression) => numeric_expression.to_field(input),
        }
    }
}

impl MathExpression {
    pub fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        match self {
            MathExpression::AddExpr(math_add) => math_add.to_field(input),
            MathExpression::SubExpr(math_subtract) => math_subtract.to_field(input),
            MathExpression::MulExpr(math_multiply) => math_multiply.to_field(input),
            MathExpression::DivExpr(math_divide) => math_divide.to_field(input),
            MathExpression::ModExpr(math_mod) => math_mod.to_field(input),
        }
    }
}

#[derive(Debug)]
pub enum Expr {
    // Binary Expression
    EqOpExpr(EqOp),
    NeqExpr(Neq),
    GtExpr(Gt),
    GtEqExpr(Gteq),
    LtExpr(Lt),
    LtEqExpr(Lteq),
    AndExpr(And),
    OrExpr(Or),

    // Column
    ColumnExpr(Column),

    // Literal
    LiteralExpr(LiteralExpression),
    // Math Expression
    MathExpr(MathExpression),

    // aggregations
    MaxExpr(AggregateMax),
    MinExpr(AggregateMin),
    SumExpr(AggregateSum),
    AvgExpr(AggregateAvg),
    CountExpr(AggregateCount),
    CountDistinctExpr(AggregateCountDistinct),

    // alias
    AliasExpr(Alias),
}

impl LogicalExpr for Expr {
    /**
     * Return meta-data about the value that will be produced by this expression when evaluated
     * against a particular input.
     */
    fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        match self {
            Expr::MaxExpr(aggregate_max) => aggregate_max.to_field(input),
            Expr::MinExpr(aggregate_min) => aggregate_min.to_field(input),
            Expr::SumExpr(aggregate_sum) => aggregate_sum.to_field(input),
            Expr::AvgExpr(aggregate_avg) => aggregate_avg.to_field(input),
            Expr::CountExpr(aggregate_count) => aggregate_count.to_field(input),
            Expr::CountDistinctExpr(aggregate_count_distinct) => {
                aggregate_count_distinct.to_field(input)
            }
            Expr::MathExpr(math_expression) => math_expression.to_field(input),
            Expr::ColumnExpr(column) => column.to_field(input),
            Expr::LiteralExpr(literal_expression) => literal_expression.to_field(input),
            Expr::AliasExpr(alias) => alias.to_field(input),

            Expr::EqOpExpr(eq_op) => eq_op.to_field(input),
            Expr::NeqExpr(neq) => neq.to_field(input),
            Expr::GtExpr(gt) => gt.to_field(input),
            Expr::GtEqExpr(gteq) => gteq.to_field(input),
            Expr::LtExpr(lt) => lt.to_field(input),
            Expr::LtEqExpr(lteq) => lteq.to_field(input),
            Expr::AndExpr(and) => and.to_field(input),
            Expr::OrExpr(or) => or.to_field(input),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::ColumnExpr(column) => write!(f, "{}", column),
            Expr::MathExpr(math_expression) => write!(f, "{:?}", math_expression),
            Expr::MaxExpr(aggregate_max) => write!(f, "{}", aggregate_max),
            Expr::MinExpr(aggregate_min) => write!(f, "{}", aggregate_min),
            Expr::SumExpr(aggregate_sum) => write!(f, "{}", aggregate_sum),
            Expr::AvgExpr(aggregate_avg) => write!(f, "{}", aggregate_avg),
            Expr::CountExpr(aggregate_count) => write!(f, "{}", aggregate_count),
            Expr::CountDistinctExpr(aggregate_count_distinct) => {
                write!(f, "{}", aggregate_count_distinct)
            }
            Expr::LiteralExpr(literal_expression) => write!(f, "{:?}", literal_expression),
            Expr::AliasExpr(alias) => write!(f, "{:?}", alias),

            Expr::EqOpExpr(eq_op) => write!(f, "{}", eq_op),
            Expr::NeqExpr(neq) => write!(f, "{}", neq),
            Expr::GtExpr(gt) => write!(f, "{}", gt),
            Expr::GtEqExpr(gteq) => write!(f, "{}", gteq),
            Expr::LtExpr(lt) => write!(f, "{}", lt),
            Expr::LtEqExpr(lteq) => write!(f, "{}", lteq),
            Expr::AndExpr(and) => write!(f, "{}", and),
            Expr::OrExpr(or) => write!(f, "{}", or),
        }
    }
}

#[derive(Debug)]
pub struct ExprRef {
    pub state: Arc<Expr>,
}

impl ExprRef {
    pub fn new(state: Arc<Expr>) -> Self {
        ExprRef { state }
    }
    pub fn to_field(&self, input: Arc<LogicalPlan>) -> Field {
        self.state.to_field(input)
    }
}

pub trait AsAlias {
    fn alias(self, alias: &str) -> ExprRef;
}
impl AsAlias for ExprRef {
    fn alias(self, alias: &str) -> ExprRef {
        ExprRef {
            state: Arc::new(Expr::AliasExpr(Alias {
                expr: Arc::new(self),
                alias: alias.to_string(),
            })),
        }
    }
}

macro_rules! impl_exprref_binop {
    ($(
        $fn_name:ident => $expr_variant:ident, $struct_name:ident
    ),* $(,)?) => {
        $(
            pub fn $fn_name(self, other: Self) -> ExprRef {
                ExprRef {
                    state: std::sync::Arc::new(Expr::$expr_variant($struct_name::new(self.state, other.state))),
                }
            }
        )*
    };
}

macro_rules! impl_exprref_math_op {
    ($trait:ident, $method:ident, $variant:ident, $expr_struct:ident) => {
        impl std::ops::$trait for ExprRef {
            type Output = ExprRef;

            fn $method(self, other: Self) -> Self::Output {
                ExprRef {
                    state: std::sync::Arc::new(Expr::MathExpr(MathExpression::$variant(
                        $expr_struct::new(self.state, other.state),
                    ))),
                }
            }
        }
    };
}

impl_exprref_math_op!(Add, add, AddExpr, MathAdd);
impl_exprref_math_op!(Sub, sub, SubExpr, MathSubtract);
impl_exprref_math_op!(Mul, mul, MulExpr, MathMultiply);
impl_exprref_math_op!(Div, div, DivExpr, MathDivide);

impl ExprRef {
    impl_exprref_binop! {
        eq => EqOpExpr, EqOp,
        neq => NeqExpr, Neq,
        gt => GtExpr, Gt,
        gteq => GtEqExpr, Gteq,
        lt => LtExpr, Lt,
        lteq => LtEqExpr, Lteq,
        and => AndExpr, And,
    }
}
