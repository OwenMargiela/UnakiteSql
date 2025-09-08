use std::sync::Arc;

use crate::datatypes::{
    column_vector::ColumnVector, literal_value_vector::LiteralValueVector,
    record_batch::RecordBatch,
};

#[derive(Debug,Clone)]
pub struct LiteralStringExpression {
    value: String,
}
impl LiteralStringExpression {
    pub fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        return ColumnVector::Literal(Arc::new(LiteralValueVector {
            arrow_type: arrow::datatypes::DataType::Utf8,
            value: Some(crate::datatypes::value::ArrowValue::StringType(
                self.value.clone(),
            )),
            size: input.row_count(),
        }));
    }
}

// Implementing Literal Expressions
macro_rules! impl_literal_expression {


    ( $( ($dt:ty, $struct_name:ident, $data_type_variant:ident, $arrow_value_variant:ident)), * $(,)? ) => {

        $(

            #[derive(Debug,Clone,Copy)]
            pub struct $struct_name {
                value: $dt,
            }

            impl $struct_name {
                pub fn evaluate(&self, input: RecordBatch) -> ColumnVector {
                    return ColumnVector::Literal(Arc::new(LiteralValueVector {
                        arrow_type: arrow::datatypes::DataType::$data_type_variant,
                        value: Some(crate::datatypes::value::ArrowValue::$arrow_value_variant(self.value)),
                        size: input.row_count(),
                    }));
                }
            }
        )*
    };
}

// Physical expressions for literal expressions of each data type.

impl_literal_expression!(
    (i8, LiteralSmallExpression, Int8, Int8Type),
    (i16, LiteralShortExpression, Int16, Int16Type),
    (i32, LiteralIntExpression, Int32, Int32Type),
    (i64, LiteralLongExpression, Int64, Int64Type),
    (u8, LiteralUSmallExpression, UInt8, UInt8Type),
    (u16, LiteralUShortExpression, UInt16, UInt16Type),
    (u32, LiteralUIntExpression, UInt32, UInt32Type),
    (u64, LiteralULongExpression, UInt64, UInt64Type),
    (f32, LiteralFloatExpression, Float32, FloatType),
    (f64, LiteralDoubleExpression, Float64, DoubleType),
);
