use std::fmt::{self, Debug, Display};

use arrow::{
    array::{ArrayRef, PrimitiveArray},
    compute,
    datatypes::{
        DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
        UInt16Type, UInt32Type, UInt64Type,
    },
};

use crate::{
    datatypes::{column_vector::ColumnVector, value::ArrowValue},
    physical_plan::expressions::{Expression, column_expressions::ColumnExpression},
};

pub trait AggregateExpression: Display + Debug {
    fn input_expression(&self) -> Expression;
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator {
    fn update(&mut self, values: &ColumnVector) -> anyhow::Result<()>;
    fn final_value(&self) -> ArrowValue;
}

#[macro_export]
macro_rules! match_primitive_op {
    ($array:expr, $op:ident,
        $( $datatype:pat => $variant:ident => $ty:ty ),* $(,)?
    ) => {
        match $array.data_type() {
            $(
                $datatype => {
                    let arr = $array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<$ty>>()
                        .ok_or_else(|| anyhow::anyhow!(concat!("Failed to downcast to ", stringify!($ty))))?;
                    Ok(ArrowValue::$variant(compute::$op::<$ty>(arr).unwrap_or_default()))
                }
            )*
            other => Err(anyhow::anyhow!("Unsupported data type: {:?}", other)),
        }
    };
}

/// Creates a primitive aggregator function like min_primitive, max_primitive, etc.
macro_rules! define_primitive_agg_fn {
    ($func_name:ident, $kernel_fn:ident) => {
        fn $func_name(array: &ArrayRef) -> anyhow::Result<ArrowValue> {
            match_primitive_op!(array, $kernel_fn,
                DataType::Int8 => Int8Type => Int8Type,
                DataType::Int16 => Int16Type => Int16Type,
                DataType::Int32 => Int32Type => Int32Type,
                DataType::Int64 => Int64Type => Int64Type,
                DataType::UInt8 => UInt8Type => UInt8Type,
                DataType::UInt16 => UInt16Type => UInt16Type,
                DataType::UInt32 => UInt32Type => UInt32Type,
                DataType::UInt64 => UInt64Type => UInt64Type,
                DataType::Float32 => FloatType => Float32Type,
                DataType::Float64 => DoubleType => Float64Type,
            )
        }
    };
}

define_primitive_agg_fn!(min_primitive, min);
define_primitive_agg_fn!(max_primitive, max);
define_primitive_agg_fn!(sum_primitive, sum);

macro_rules! impl_aggregator {
    ($( ($aggregate_name:ident, $accumulator_name:ident, $op_func:ident) ),* $(,)?) => {
        $(
            pub struct $aggregate_name {
                pub expr: Expression,
            }

            impl Display for $aggregate_name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}({:?})", stringify!($aggregate_name), self.expr)
                }
            }

            impl Debug for $aggregate_name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}({:?})", stringify!($aggregate_name), self.expr)
                }
            }

            impl AggregateExpression for $aggregate_name {
                fn create_accumulator(&self) -> Box<dyn Accumulator> {
                    Box::new($accumulator_name { value: None })
                }

                fn input_expression(&self) -> Expression {
                    self.expr.clone()
                }
            }

            pub struct $accumulator_name {
                value: Option<ArrowValue>,
            }

            impl Accumulator for $accumulator_name {
                fn update(&mut self, values: &ColumnVector) -> anyhow::Result<()> {
                    let vec = values.get_vector();
                    let value = $op_func(&vec.field)?;

                    self.value = Some(match &self.value {
                    Some(current) => match value {
                        ArrowValue::Int8Type(v) => ArrowValue::Int8Type(v + i8::from(current.clone())),
                        ArrowValue::Int16Type(v) => ArrowValue::Int16Type(v + i16::from(current.clone())),
                        ArrowValue::Int32Type(v) => ArrowValue::Int32Type(v + i32::from(current.clone())),
                        ArrowValue::Int64Type(v) => ArrowValue::Int64Type(v + i64::from(current.clone())),
                        ArrowValue::UInt8Type(v) => ArrowValue::UInt8Type(v + u8::from(current.clone())),
                        ArrowValue::UInt16Type(v) => ArrowValue::UInt16Type(v + u16::from(current.clone())),
                        ArrowValue::UInt32Type(v) => ArrowValue::UInt32Type(v + u32::from(current.clone())),
                        ArrowValue::UInt64Type(v) => ArrowValue::UInt64Type(v + u64::from(current.clone())),
                        ArrowValue::FloatType(v) => ArrowValue::FloatType(v + f32::from(current.clone())),
                        ArrowValue::DoubleType(v) => ArrowValue::DoubleType(v + f64::from(current.clone())),
                        _ => panic!("MAX is not implemented for data type"),
                        },
                        None => value,
                    });
                Ok(())
                }

                fn final_value(&self) -> ArrowValue {
                    self.value.clone().expect("Accumulator has no value")
                }
            }

        )*
    };
}

impl_aggregator!(
    (MaxExpression, MaxAccumulator, max_primitive),
    (MinExpression, MinAccumulator, min_primitive),
    (SumExpression, SumAccumulator, sum_primitive)
);

// Helper
pub fn sum_expression() -> SumExpression {
    SumExpression {
        expr: Expression::Column(ColumnExpression { i: 0 }),
    }
}

pub fn min_expression() -> MinExpression {
    let a = MinExpression {
        expr: Expression::Column(ColumnExpression { i: 0 }),
    };

    a
}

pub fn max_expression() -> MaxExpression {
    let a = MaxExpression {
        expr: Expression::Column(ColumnExpression { i: 0 }),
    };

    a
}
