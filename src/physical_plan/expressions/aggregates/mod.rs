use std::fmt::{self, Debug, Display};

use crate::{
    datatypes::value::ArrowValue,
    physical_plan::expressions::{Expression, column_expressions::ColumnExpression},
};

pub trait AggregateExpression: Display + Debug {
    fn input_expression(&self) -> Expression;
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator {
    fn accumulate(&mut self, value: Option<ArrowValue>);
    fn final_value(&self) -> Option<ArrowValue>;
}

macro_rules! impl_aggregator {
    ($( ($aggregate_name:ident,$accumulator_name:ident,$string_expr:expr,$infix:tt)),* $(,)? )=> {

        $(
            pub struct $aggregate_name {
                pub expr: Expression,
            }

            impl fmt::Display for $aggregate_name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}({:?}{}",stringify!($string_expr), self.expr, ")")
                }
            }

            impl AggregateExpression for $aggregate_name {
                fn create_accumulator(&self) -> Box<dyn Accumulator> {
                    return Box::new($accumulator_name { value: None });
                }

                fn input_expression(&self) -> Expression {
                    self.expr.clone()
                }
            }

            impl fmt::Debug for $aggregate_name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}({:?}{}",stringify!($string_expr), self.expr, ")")
                }
            }

            pub struct $accumulator_name {
                value: Option<ArrowValue>,
            }

            impl Accumulator for $accumulator_name {
                fn accumulate(&mut self, value: Option<ArrowValue>) {

                if let Some(value) = value {
                    if let Some(_) = &self.value {
                        let bool_res: bool = match value {
                        ArrowValue::Int8Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::Int16Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::Int32Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::Int64Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::UInt8Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::UInt16Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::UInt32Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::UInt64Type(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::FloatType(val) => val $infix self.value.clone().unwrap().into(),
                        ArrowValue::DoubleType(val) => val $infix self.value.clone().unwrap().into(),
                        _ => panic!("MAX is not implemented for data type"),
                    };

                    if bool_res {
                        self.value = Some(value)
                    }

                    } else {
                        self.value = Some(value)
                    }
                }

                }
                fn final_value(&self) -> Option<ArrowValue> {
                    self.value.clone()
                }
        }

        )*


    };
}

impl_aggregator!(
    (MaxExpression,MaxAccumulator,Max, >),
    (MinExpression,MinAccumulator,Min, <)

);

pub struct SumExpression {
    pub expr: Expression,
}
impl fmt::Display for SumExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SUM({:?}{}", self.expr, ")")
    }
}

pub struct SumAccumulator {
    value: Option<ArrowValue>,
}

impl AggregateExpression for SumExpression {
    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        return Box::new(SumAccumulator { value: None });
    }

    fn input_expression(&self) -> Expression {
        self.expr.clone()
    }
}

macro_rules! accumulate_numeric {
    ($self:expr, $current_value:expr, $value:expr, $(($variant:path, $type:ty)),*) => {
        match $current_value {
            $(
                $variant(_) => {
                    let cur_val: $type = $current_value.into();
                    let incoming_val: $type = $value.into();
                    $self.value = Some((cur_val + incoming_val).into());
                }
            )*
            _ => panic!("SUM is not implemented for this data type"),
        }
    };
}

impl fmt::Debug for SumExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sum({:?})", self.expr)
    }
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: Option<ArrowValue>) {
        if let Some(value) = value {
            if let Some(current_value) = self.value.clone() {
                accumulate_numeric!(
                    self,
                    current_value,
                    value,
                    (ArrowValue::Int8Type, i8),
                    (ArrowValue::Int16Type, i16),
                    (ArrowValue::Int32Type, i32),
                    (ArrowValue::Int64Type, i64),
                    (ArrowValue::UInt8Type, u8),
                    (ArrowValue::UInt16Type, u16),
                    (ArrowValue::UInt32Type, u32),
                    (ArrowValue::UInt64Type, u64),
                    (ArrowValue::FloatType, f32),
                    (ArrowValue::DoubleType, f64)
                );
            } else {
                self.value = Some(value)
            }
        }
    }
    fn final_value(&self) -> Option<ArrowValue> {
        self.value.clone()
    }
}

// Helprer
pub fn sum_expression() -> SumExpression {
    let a = SumExpression {
        expr: Expression::Column(ColumnExpression { i: 0 }),
    };

    a
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
