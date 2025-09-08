use core::fmt;

use arrow::datatypes::DataType;

use crate::{
    datatypes::{
        arrow_vector_builder::ArrowVectorBuilder,
        column_vector::{ColumnVector, ColumnVectorTrait},
        record_batch::RecordBatch,
        value::ArrowValue,
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
    ( $builder:expr, $data_type: ident, $value:expr, $(($variant:ident, $rust_type:ty)),* $(,)?  ) => {

        match $data_type {

            $(
                DataType::$variant => {
                    use crate::datatypes::cast_accross::CastAcross;
                    for i in 0..$value.size() {
                    let vv = $value.get_value_inner(i);
                    if let Some(vv) = vv {

                        let cast_value: ArrowValue = vv.cast_to(DataType::$variant);

                        $builder.set(i, Some(cast_value));
                    } else {
                        $builder.set(i, None);
                    }
                }

                }
            )*,
              DataType::Utf8 => {
                    for i in 0..$value.size() {
                    use crate::datatypes::cast_accross::CastAcross;
                    let vv = $value.get_value_inner(i);
                    if let Some(vv) = vv {

                        let cast_value: ArrowValue = vv.cast_to(DataType::Utf8);

                        $builder.set(i, Some(cast_value));
                    } else {
                        $builder.set(i, None);
                    }
                }

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

        let mut builder = ArrowVectorBuilder::new(&data_type);

        builded_cast_array!(
            builder,
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
        );

        builder.build()
    }
}
