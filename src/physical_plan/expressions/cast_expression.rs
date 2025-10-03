use core::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::{
    datatypes::{
        arrow_field_vector::ArrowFieldVector, column_vector::ColumnVector,
        record_batch::RecordBatch,
    },
    physical_plan::expressions::{Expression, column_expressions::ColumnExpression},
};

pub struct CastExpression {
    expr: Expression,
    data_type: DataType,
}

impl fmt::Display for CastExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST({:?} as{}", self.expr, self.data_type)
    }
}

macro_rules! builded_cast_array {
    (  $data_type: ident, $value:expr, $(($variant:ident, $rust_type:ty)),* $(,)?  ) => {

        match $data_type {

            $(
                DataType::$variant => {

                    let vec = &$value.get_vector().field;
                    let casted = arrow::compute::kernels::cast(vec, &$data_type).unwrap();

                    return ColumnVector::ArrowVector(ArrowFieldVector {
                                field: Arc::new(casted)
                    });


                }
            )*,
            DataType::Utf8 => {

                    let vec = &$value.get_vector().field;
                    let casted = arrow::compute::kernels::cast(vec, &DataType::Utf8).unwrap();

                    return ColumnVector::ArrowVector(ArrowFieldVector {
                        field: Arc::new(casted)
                    });

                }

            _ => todo!(),
        }


    };
}

impl CastExpression {
    pub fn new(expr: ColumnExpression, data_type: DataType) -> CastExpression {
        CastExpression {
            expr: Expression::Column(expr),
            data_type,
        }
    }
    pub fn evaluate(&self, input: RecordBatch) -> ColumnVector {
        let value = self.expr.evaluate(input);

        let data_type = self.data_type.clone();

        builded_cast_array!(
            data_type,
            value,
            (Int8, i8),
            (Int16, i16),
            (Int32, i32),
            (Int64, i64),
            (UInt8, u8),
            (UInt16, u16),
            (UInt32, u32),
            (UInt64, u64),
            (Float32, f32),
            (Float64, f64),
        )
    }
}
