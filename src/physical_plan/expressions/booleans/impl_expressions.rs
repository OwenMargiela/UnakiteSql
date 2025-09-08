
use crate::{
    datatypes::{concrete_type::ConcreteType, value::ArrowValue}, physical_plan::expressions::booleans::BooleanPair,

};

pub struct AndPlan;

impl BooleanPair for AndPlan {
    fn evaluate_pair(
        &self,
        _l: Option<ArrowValue>,
        _r: Option<ArrowValue>,
        _arrow_type: arrow::datatypes::DataType,
    ) -> bool {
        _l.unwrap().into() && _r.unwrap().into()
    }
}

impl std::fmt::Display for AndPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "&&")
    }
}

impl std::fmt::Debug for AndPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "&& ")
    }
}

pub struct OrPlan;

impl BooleanPair for OrPlan {
    fn evaluate_pair(
        &self,
        _l: Option<ArrowValue>,
        _r: Option<ArrowValue>,
        _arrow_type: arrow::datatypes::DataType,
    ) -> bool {
        _l.unwrap().into() || _r.unwrap().into()
    }
}

impl std::fmt::Display for OrPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "||")
    }
}

impl std::fmt::Debug for OrPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|| ")
    }
}

macro_rules! impl_binary_infix_op {
    ($(($struct:ident, $infix:tt)),* $(,)?) => {
        $(
            pub struct $struct ;

            impl BooleanPair for $struct {
                fn evaluate_pair(
                    &self,
                    _l: Option<ArrowValue>,
                    _r: Option<ArrowValue>,
                    _arrow_type: arrow::datatypes::DataType,
                ) -> bool {
                    let (l, r) = match (_l, _r) {
                        (Some(l), Some(r)) => (l, r),
                        _ => panic!("Both operands must be Some"),
                    };

                    if l.get_conc_type() == r.get_conc_type() && r.get_conc_type() == _arrow_type {
                        match _arrow_type {
                            arrow::datatypes::DataType::Int8 => i8::from(l) $infix i8::from(r),
                            arrow::datatypes::DataType::Int16 => i16::from(l) $infix i16::from(r),
                            arrow::datatypes::DataType::Int32 => i32::from(l) $infix i32::from(r),
                            arrow::datatypes::DataType::Int64 => i64::from(l) $infix i64::from(r),
                            arrow::datatypes::DataType::UInt8 => u8::from(l) $infix u8::from(r),
                            arrow::datatypes::DataType::UInt16 => u16::from(l) $infix u16::from(r),
                            arrow::datatypes::DataType::UInt32 => u32::from(l) $infix u32::from(r),
                            arrow::datatypes::DataType::UInt64 => u64::from(l) $infix u64::from(r),
                            arrow::datatypes::DataType::Float32 => f32::from(l) $infix f32::from(r),
                            arrow::datatypes::DataType::Float64 => f64::from(l) $infix f64::from(r),

                            arrow::datatypes::DataType::Utf8 => String::from(l) $infix String::from(r),
                            _ => panic!("Invalid Arrow datatype for this operation"),
                        }
                    } else {
                        panic!("Invalid type combination or mismatched Arrow types")
                    }
                }


            }


            impl std::fmt::Display for $struct {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} ", stringify!($infix))

            }
            }


                impl std::fmt::Debug for $struct {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} ", stringify!($infix))

            }
            }
        )*
    };
}

// Implements binary infix operations

impl_binary_infix_op!(
    (EqPlan, ==),
    (NeqPlan, !=),

    (LtPlan, <),
    (LteqPlan, <=),
    (GtPlan, >),
    (GteqPlan, >=),

);
