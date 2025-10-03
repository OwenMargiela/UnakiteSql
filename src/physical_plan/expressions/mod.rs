pub mod aggregates;
pub mod booleans;
pub mod cast_expression;
pub mod column_expressions;
pub mod literal_expressions;
pub mod math;
pub mod unary_expression;

use crate::{
    datatypes::{
        column_vector::ColumnVector, concrete_type::ConcreteType, record_batch::RecordBatch,
    },
    physical_plan::expressions::{
        booleans::BooleanExpression, column_expressions::ColumnExpression, literal_expressions::*,
    },
};
use std::{fmt::Debug, sync::Arc};



// Row oriented bs use arrow compute for simd physical operations unless you are willing to do simd from scratch..... 



/// Physical representation of an expression in the query plan
/// 
/// 
/*

    pub struct PhysicalExpression {
        expression: Box<dyn Expression>
    }

    physicalExpression.evaluate

*/
#[derive(Debug, Clone)]
pub enum Expression {
    Literal(LiteralExpression),
    Boolean(Arc<BooleanExpression>),
    Column(ColumnExpression),
    // Aggregations(Arc<dyn AggregateExpression>),
    Cast,
    Unary,
}




#[derive(Debug, Clone)]
pub enum LiteralExpression {
    Int8(LiteralSmallExpression),
    Int16(LiteralShortExpression),
    Int32(LiteralIntExpression),
    Int64(LiteralULongExpression),
    UInt8(LiteralUSmallExpression),
    UInt16(LiteralUShortExpression),
    UInt32(LiteralUIntExpression),
    UInt64(LiteralULongExpression),
    Float32(LiteralFloatExpression),
    Float64(LiteralDoubleExpression),
    String(LiteralStringExpression),
}

impl LiteralExpression {
    pub fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        use LiteralExpression::*;
        match self {
            Int8(expr) => expr.evaluate(input),
            Int16(expr) => expr.evaluate(input),
            Int32(expr) => expr.evaluate(input),
            Int64(expr) => expr.evaluate(input),
            UInt8(expr) => expr.evaluate(input),
            UInt16(expr) => expr.evaluate(input),
            UInt32(expr) => expr.evaluate(input),
            UInt64(expr) => expr.evaluate(input),
            Float32(expr) => expr.evaluate(input),
            Float64(expr) => expr.evaluate(input),
            String(expr) => expr.evaluate(input),
        }
    }
}

impl Expression {
    /// Evaluate the expression against an input record batch and produce a column of data as output
    pub fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        use Expression::*;
        match self {
            Boolean(expr) => expr.evaluate(input),
            Column(expr) => expr.evaluate(input),
            Literal(expr) => expr.evaluate(input),
            // Aggregations(expr) => expr.input_expression().evaluate(input),
            Unary => todo!("Unary expressions not yet implemented"),
            Cast => todo!("Cast expressions not yet implemented"),
        }
    }
}

impl ConcreteType for Expression {
    fn get_conc_type(&self) -> arrow::datatypes::DataType {
        use Expression::*;
        use LiteralExpression::*;
        use arrow::datatypes::DataType;

        match self {
            Boolean(_) => DataType::Boolean,
            Unary => DataType::Float64, // TODO: This should depend on the actual unary operation. The only unary operations to be be added will yield the double/float64 type
            Literal(literal) => match literal {
                Int8(_) => DataType::Int8,
                Int16(_) => DataType::Int16,
                Int32(_) => DataType::Int32,
                Int64(_) => DataType::Int64,
                UInt8(_) => DataType::UInt8,
                UInt16(_) => DataType::UInt16,
                UInt32(_) => DataType::UInt32,
                UInt64(_) => DataType::UInt64,
                Float32(_) => DataType::Float32,
                Float64(_) => DataType::Float64,
                String(_) => DataType::Utf8,
            },

            // Aggregations(expr) => expr.input_expression().get_conc_type(),
            _ => panic!("Expression does not yeild a constant type"),
        }
    }
}
